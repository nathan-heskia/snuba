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
            with self.__subscription.get() as subscription:
                for stream, offset in streams.items():
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
        # based on the progression of the remote offset. (This could go the
        # other way as well -- remote offsets may be rolled back -- but those
        # will be caught later during normal consumption. The only benefit to
        # pausing them here would be to avoid buffering messages that will
        # likely be discarded. On the other hand, by the time we get to consume
        # these messages, the offsets may have been synchronized at that point.
        # It's hard to predict which scenario is more likely -- in the interest
        # of buffering and low latency, it probably makes sense to avoid
        # pausing here?)
        with self.__subscription.get() as subscription:
            # TODO: This could be limited to only streams that are currently
            # paused due to the remote offsets -- we should even be able to get
            # those streams without acquiring the lock here. (See similar note
            # about lock-free implementation in resume.)
            for stream, offset in self.__backend.tell().items():
                # If a stream has been explicitly paused by the user of this
                # class, we don't need to check the offsets: just leave it
                # paused.
                if stream in self.__paused_streams:
                    continue

                remote_offset = subscription.get_effective_remote_consumer_group_offset(
                    stream, self.__remote_consumer_groups
                )
                if remote_offset is not None and remote_offset > offset:
                    # TODO: It should be safe to call resume on a stream that
                    # is already consuming, but may have some performance
                    # drawbacks (I vaguely recall that this may flush the
                    # consume buffer in the Confluent consumer), so it may be
                    # worth caching which streams are active or paused here, or
                    # in the KafkaConsumerBackend.
                    self.__backend.resume([stream])

        message = self.__backend.poll(timeout)
        if message is None:
            return None

        # Check to ensure that consuming this message will not cause the local
        # offset to exeed the remote offset(s).
        with self.__subscription.get() as subscription:
            remote_offset = subscription.get_effective_remote_consumer_group_offset(
                stream, self.__remote_consumer_groups
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

        with self.__subscription.get() as subscription:
            for stream, offset in offsets.items():
                remote_offset = subscription.get_effective_remote_consumer_group_offset(
                    stream, self.__remote_consumer_groups
                )
                if remote_offset is None or offset >= remote_offset:
                    # If this message does exceed the remote offset(s), it should be
                    # dropped, the stream should be paused, and the offset should be
                    # reset so that this message is consumed again when the stream is
                    # resumed.
                    self.__backend.pause([stream])
                elif stream not in self.__paused_streams:
                    # TODO: It should be safe to call resume on a stream that
                    # is already consuming, but may have some performance
                    # drawbacks (I vaguely recall that this may flush the
                    # consume buffer in the Confluent consumer), so it may be
                    # worth caching which streams are active or paused here, or
                    # in the KafkaConsumerBackend.
                    self.__backend.resume([stream])

    def pause(self, streams: Sequence[TStream]) -> None:
        # TODO: How should this handle the scenario where the stream(s) is not
        # part of the assignment set?
        for stream in streams:
            self.__paused_streams.add(stream)

        return self.__backend.pause(streams)

    def resume(self, streams: Sequence[TStream]) -> None:
        # TODO: How should this handle the scenario where the stream(s) is not
        # part of the assignment set?

        offsets = self.tell()

        # TODO: This could be performed lock-free if the pause/resume state
        # derived from the remote offset value was cached on this object: this
        # doesn't necessarily need to resume immediately (it could be handled
        # by the next run of the poll loop.)
        resumable = []

        with self.__subscription.get() as subscription:
            for stream in streams:
                self.__paused_streams.discard(stream)

                remote_offset = subscription.get_effective_remote_consumer_group_offset(
                    stream, self.__remote_consumer_groups
                )
                # NOTE: offsets[stream] should not throw a KeyError, assuming
                # that the input value has been validated against the current
                # stream assignment. (See the TODO at method start. Remove this
                # comment after that has been implemented, or protect against
                # KeyError here.)
                if remote_offset is not None and remote_offset > offsets[stream]:
                    resumable.append(stream)

        return self.__backend.resume(resumable)

    def commit(self) -> Mapping[TStream, TOffset]:
        return self.__backend.commit()

    def close(self, timeout: Optional[float] = None) -> None:
        self.__shutdown_requested.set()
        try:
            return self.__backend.close(timeout)
        finally:
            self.__commit_log_consumer_future.result(timeout=0)
