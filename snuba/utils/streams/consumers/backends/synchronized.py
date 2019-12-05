from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from threading import Event
from typing import (
    Callable,
    Generic,
    Iterable,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Set,
)

from snuba.utils.concurrent import Synchronized, execute
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

    def get_effective_remote_consumer_group_offset(
        self, stream: TStream, groups: Iterable[ConsumerGroup]
    ) -> Optional[TOffset]:
        offsets = self.remote_consumer_group_offsets[stream]
        return min(
            filter(
                lambda offset: offset is not None,
                [offsets.get(group) for group in groups],
            ),
            default=None,
        )


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

        self.__subscription: Synchronized[
            Subscription[TStream, TOffset]
        ] = Synchronized(Subscription([]))

        self.__paused_streams: Set[TStream] = set()

        self.__shutdown_requested = Event()

        # TODO: If the commit log consumer crashes, it should cause all method
        # calls on this object to raise (probably a RuntimeError raised from
        # the commit log consumer exception.)
        self.__commit_log_consumer_future = execute(self.__run_commit_log_consumer)

    def __run_commit_log_consumer(self) -> None:
        # TODO: How do we ensure that the commit log consumer is not committing
        # it's offsets (or is subject to automatic offset resets?)

        # XXX: This reference to the subscription object is only maintained so
        # that it can be referenced checked during the commit log consumer main
        # loop. It should not be used otherwise!
        current_subscription: Optional[Subscription[TStream, TOffset]] = None

        while not self.__shutdown_requested.is_set():
            with self.__subscription.get() as subscription:
                # If the subscription for the consumer has changed, we need to
                # update the assignment for the commit log consumer.
                if subscription is not current_subscription:
                    # Convert the subscription topics into the TStream
                    # instances where we can expect to find their commit log
                    # data.
                    assignment: Set[TStream] = set()
                    for topic in subscription.topics:
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
                    current_subscription = subscription

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
                with self.__subscription.get() as subscription:
                    # If the current subscription has changed while we were
                    # waiting for a message, we need to discard this message.
                    # The next loop iteration will update the consumer
                    # assignment, and we will begin to rebuild state there.
                    if subscription is not current_subscription:
                        continue

                    # If the stream for this message matches the subscription
                    # topics, apply the update to the subscription offsets.
                    if any((message.stream in topic) for topic in subscription.topics):
                        subscription.remote_consumer_group_offsets[
                            message.value.stream
                        ][message.value.consumer_group] = message.value.offset

    def __update_stream_states(self) -> None:
        with self.__subscription.get() as subscription:
            for stream, offset in self.__backend.tell().items():
                remote_offset = subscription.get_effective_remote_consumer_group_offset(
                    stream, self.__remote_consumer_groups
                )
                if remote_offset is None or offset >= remote_offset:
                    self.__backend.pause([stream])
                elif stream not in self.__paused_streams:
                    # TODO: It should be safe to call resume on a stream
                    # that is already consuming, but may have some
                    # performance drawbacks (I vaguely recall that this may
                    # flush the consume buffer in the Confluent consumer),
                    # so it may be worth caching which streams are active
                    # or paused here, or in the KafkaConsumerBackend.
                    self.__backend.resume([stream])

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
        self.__subscription.set(Subscription(topics))

        # TODO: How does the Kafka consumer handle paused streams? My
        # assumption here is that the new subscription should cause those
        # streams to be resumed again, but not sure that's accurate.
        self.__paused_streams.clear()

        def assignment_callback(streams: Mapping[TStream, TOffset]) -> None:
            # Pause and resume the streams in the new assignment as
            # necessitated by their remote offset.
            self.__update_stream_state()

            if on_assign is not None:
                on_assign(streams)

        return self.__backend.subscribe(
            topics, on_assign=assignment_callback, on_revoke=on_revoke
        )

    def unsubscribe(self) -> None:
        # Update the subscription, so that the commit log consumer can stop
        # consuming the commit log data for the previous (and no longer
        # potentially assigned) topics.
        self.__subscription.set(Subscription([]))

        # TODO: How does the Kafka consumer handle paused streams? My
        # assumption here is that the new subscription should cause those
        # streams to be resumed again, but not sure that's accurate.
        self.__paused_streams.clear()

        return self.__backend.unsubscribe()

    def poll(
        self, timeout: Optional[float] = None
    ) -> Optional[Message[TStream, TOffset, TValue]]:
        # Check to see if any streams are paused that are able to be resumed
        # based on the progression of the remote offset that may have occurred
        # between calls to ``poll``.
        # TODO: In this specific case, we could likely limit this state check
        # to only those streams that are currently paused due to the remote
        # offsets (enabling their messages to be returned from ``poll`` calls
        # to the backend.) We don't need to be concerned with streams that are
        # not paused, as we'll catch those below if they start to return
        # messages that exceed the remote offset (and the only benefit to
        # pausing them here would be to reclaim memory used by buffered
        # messages.) We should even be able to identify if there are streams
        # paused to maintain the offset invariant without acquiring the lock
        # here, potentially skipping lock acquisition entirely.
        self.__update_stream_states()

        message = self.__backend.poll(timeout)
        if message is None:
            return None

        # Check to ensure that consuming this message will not cause the local
        # offset to exeed the remote offset(s).
        with self.__subscription.get() as subscription:
            remote_offset = subscription.get_effective_remote_consumer_group_offset(
                message.stream, self.__remote_consumer_groups
            )
            if remote_offset is None or message.offset >= remote_offset:
                # If this message does exceed the remote offset(s), it should be
                # dropped, the stream should be paused, and the offset should be
                # reset so that this message is consumed again when the stream is
                # resumed.
                self.__backend.pause([message.stream])
                self.__backend.seek({message.stream: message.offset})

        return message

    def tell(self) -> Mapping[TStream, TOffset]:
        return self.__backend.tell()

    def seek(self, offsets: Mapping[TStream, TOffset]) -> None:
        # TODO: How should this handle the scenario where the stream(s) is not
        # part of the assignment set? (This is probably, or maybe should be,
        # already be handled by the backend itself.)
        self.__backend.seek(offsets)

        # Moving the stream offsets may require the stream to be paused, so
        # check for that now.
        self.__update_stream_states()

    def pause(self, streams: Sequence[TStream]) -> None:
        # TODO: How should this handle the scenario where the stream(s) is not
        # part of the assignment set?
        for stream in streams:
            self.__paused_streams.add(stream)

        return self.__backend.pause(streams)

    def resume(self, streams: Sequence[TStream]) -> None:
        # TODO: How should this handle the scenario where the stream(s) is not
        # part of the assignment set?
        for stream in streams:
            self.__paused_streams.discard(stream)

        # This method doesn't need to update the stream set after it is called:
        # any streams that are eligible to be resumed will be resumed with the
        # next ``poll`` call.

    def commit(self) -> Mapping[TStream, TOffset]:
        return self.__backend.commit()

    def close(self, timeout: Optional[float] = None) -> None:
        self.__shutdown_requested.set()
        try:
            return self.__backend.close(timeout)
        finally:
            self.__commit_log_consumer_future.result(timeout=0)
