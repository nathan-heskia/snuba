from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from threading import Event, Lock
from typing import Callable, Generic, Mapping, MutableMapping, Optional, Set, Sequence

from snuba.utils.concurrent import execute
from snuba.utils.streams.consumers.backends.abstract import ConsumerBackend
from snuba.utils.streams.consumers.types import Message, TOffset, TStream, TValue


ConsumerGroup = str


@dataclass
class CommitData(Generic[TStream, TOffset]):
    consumer_group: ConsumerGroup
    stream: TStream
    offset: TOffset


# TODO: This should be moved into the abstract consumer backend.
class Topic(ABC, Generic[TStream]):
    @abstractmethod
    def __contains__(self, stream: TStream) -> bool:
        raise NotImplementedError


@dataclass
class Subscription(Generic[TStream, TOffset]):
    topics: Sequence[Topic[TStream]]
    remote_consumer_group_offsets: MutableMapping[
        TStream, MutableMapping[ConsumerGroup, TOffset]
    ] = field(default_factory=lambda: defaultdict(dict))


# TODO: TOffset needs to be bound as some sort of comparable type.


class SynchronizedConsumerBackend(ConsumerBackend[TStream, TOffset, TValue]):
    def __init__(
        self,
        backend: ConsumerBackend[TStream, TOffset, TValue],
        commit_log_consumer_backend: ConsumerBackend[
            # NOTE: These TStream and TOffset values for the commit log
            # consumer do not need to match, but this simplifies things a bit
            # so that we don't have to declare two additional type variables.
            TStream,
            TOffset,
            CommitData[TStream, TOffset],
        ],
        remote_consumer_groups: Set[ConsumerGroup],
        get_commit_log_streams_for_topic: Callable[[Topic[TStream]], Sequence[TStream]],
    ) -> None:
        self.__backend = backend
        self.__commit_log_consumer_backend = commit_log_consumer_backend
        self.__remote_consumer_groups = remote_consumer_groups
        self.__get_commit_log_streams_for_topic = get_commit_log_streams_for_topic

        self.__lock = Lock()
        self.__subscription: Subscription[TStream, TOffset] = Subscription([])
        self.__shutdown_requested = Event()

        # TODO: If the commit log consumer crashes, it should cause all method
        # calls on this object to raise (probably a RuntimeError raised from
        # the commit log consumer exception.)
        self.__commit_log_consumer_future = execute(self.__run_commit_log_consumer)

    def __run_commit_log_consumer(self) -> None:
        # TODO: How does the commit log consumer ensure that it is the only
        # consumer within it's consumer group (or that it otherwise has the
        # comprehensive set of partitions assigned for the commit log topic?)
        # TODO: How do we ensure that the commit log consumer is not committing
        # it's offsets (or is subject to automatic offset resets?)

        # XXX: This reference to the subscription object is only maintained so
        # that it can be referenced checked during the commit log consumer main
        # loop. It should not be used otherwise!
        current_subscription: Optional[Subscription[TStream, TOffset]] = None

        while not self.__shutdown_requested.is_set():
            with self.__lock:
                # If the subscription for the consumer has changed, we need to
                # update the assignment for the commit log consumer.
                if self.__subscription is not current_subscription:
                    # Convert the subscription topics into the TStream
                    # instances where we can expect to find their commit log
                    # data.
                    assignment: Set[TStream] = set()
                    for topic in self.__subscription.topics:
                        assignment.update(
                            self.__get_commit_log_streams_for_topic(topic)
                        )

                    # TODO: We need to be able to use managed assignment to
                    # assign these topics to the commit log consumer. The
                    # starting offset for these streams should be the first
                    # available offset. (This API doesn't exist yet.)
                    self.__commit_log_consumer_backend.assign(assignment)

                    # Once the assignment has been updated, we can safely
                    # replace the current subscription reference with the new
                    # subscription.
                    current_subscription = self.__subscription

            # TODO: This timeout was a totally arbitrary descision and may or
            # may not make sense.
            # TODO: This may need to be protected from EndOfStream errors,
            # depending on how that configuration has been set.
            message = self.__commit_log_consumer_backend.poll(0.1)
            if message is None:
                continue

            # If this update matches our subscription (whether or not it is
            # part of the current assignment does not matter -- it may be part
            # of a later assignment) and was from a remote consumer group that
            # we are observing, we need to update the offset state. First, we
            # check the consumer groups reference (because we can do so without
            # taking the lock):
            if message.value.consumer_group in self.__remote_consumer_groups:
                with self.__lock:
                    # If the current subscription has changed while we were
                    # waiting for a message, we need to discard this message.
                    # The next loop iteration will update the consumer
                    # assignment, and we will begin to rebuild state there.
                    if self.__subscription is not current_subscription:
                        continue

                    # If the stream for this message matches the subscription
                    # topics, apply the update to the subscription offsets.
                    if any(
                        (message.stream in topic)
                        for topic in self.__subscription.topics
                    ):
                        self.__subscription.remote_consumer_group_offsets[
                            message.value.stream
                        ][message.value.consumer_group] = message.value.offset

    def __get_effective_remote_offset(self, stream: TStream) -> Optional[TOffset]:
        # XXX: This assumes that the lock has already been acquired before
        # calling this method.
        offsets = self.__subscription.remote_consumer_group_offsets[stream]
        return min(
            filter(
                lambda offset: offset is not None,
                [offsets.get(group) for group in self.__remote_consumer_groups],
            ),
            default=None,
        )

    def subscribe(
        self,
        topics: Sequence[
            Topic[TStream]
        ],  # TODO: This will have to apply to the superclass as well.
        on_assign: Optional[Callable[[Mapping[TStream, TOffset]], None]] = None,
        on_revoke: Optional[Callable[[Sequence[TStream]], None]] = None,
    ) -> None:
        # Update the subscription, so that the commit log consumer can starts
        # to consume the commit log data for the subscribed (and potentially
        # assigned) topics.
        with self.__lock:
            self.__subscription = Subscription(topics)

        def assignment_callback(streams: Mapping[TStream, TOffset]) -> None:
            with self.__lock:
                for stream, offset in streams.items():
                    effective_remote_offset = self.__get_effective_remote_offset(stream)
                    # TODO: This will have to be more intelligent about the way
                    # that pause and resume are handled as soon as those are
                    # exposed via the public interface: for example, it's
                    # possible that the caller wants a stream to be paused, but
                    # it is allowed to be consumed from by the offset
                    # invariant, or vice versa -- in both cases, the stream
                    # should be paused, and only resumed when the caller would
                    # like it to be and the offset invariant can be maintained.
                    if (
                        effective_remote_offset is None
                        or offset >= effective_remote_offset
                    ):
                        self.__backend.pause([stream])
                    else:
                        self.__backend.resume([stream])

            if on_assign is not None:
                on_assign(streams)

        return self.__backend.subscribe(
            topics, on_assign=assignment_callback, on_revoke=on_revoke
        )

    def unsubscribe(self) -> None:
        # Update the subscription, so that the commit log consumer can stop
        # consuming the commit log data for the previous (and no longer
        # potentially assigned) topics.
        with self.__lock:
            self.__subscription = Subscription([])

        return self.__backend.unsubscribe()

    def poll(
        self, timeout: Optional[float] = None
    ) -> Optional[Message[TStream, TOffset, TValue]]:
        # Check to see if any streams are paused that are able to be resumed
        # based on the progression of the remote offset. (This could go the
        # other way as well -- remote offsets may be rolled back -- but those
        # will be caught later during normal consumption. The only benefit to
        # pausing them here would be to avoid buffering messages that will
        # likely be discarded. On the other hand, by the time we get to consume
        # these messages, the offsets may have been synchronized at that point.
        # It's hard to predict which scenario is more likely -- in the interest
        # of buffering and low latency, it probably makes sense to avoid
        # pausing here?)
        with self.__lock:
            # TODO: This should be limited to only streams that are currently
            # paused due to the remote offsets -- we should even be able to get
            # those streams without acquiring the lock here.
            for stream, offset in self.__backend.tell().items():
                effective_remote_offset = self.__get_effective_remote_offset(stream)
                if (
                    effective_remote_offset is not None
                    and effective_remote_offset > offset
                ):
                    # TODO: This should be more intelligent (see comment in the
                    # assignment callback in ``subscribe`` for more details.)
                    self.__backend.resume([stream])

        message = self.__backend.poll(timeout)
        if message is None:
            return None

        # Check to ensure that consuming this message will not cause the local
        # offset to exeed the remote offset(s).
        with self.__lock:
            effective_remote_offset = self.__get_effective_remote_offset(message.stream)
            if (
                effective_remote_offset is None
                or message.offset >= effective_remote_offset
            ):
                # If this message does exceed the remote offset(s), it should be
                # dropped, the stream should be paused, and the offset should be
                # reset so that this message is consumed again when the stream is
                # resumed.
                self.__backend.pause([message.stream])
                self.__backend.seek({message.stream: message.offset})

        return message
