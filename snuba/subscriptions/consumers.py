from __future__ import annotations

from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass
from threading import Lock
from typing import (
    Generic,
    Iterator,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Set,
    TypeVar,
)

from confluent_kafka import OFFSET_BEGINNING
from confluent_kafka import Consumer as ConfluentConsumer
from confluent_kafka import KafkaMessage
from confluent_kafka import TopicPartition as ConfluentTopicPartition

from snuba.utils.streams.consumers.backends.kafka import TopicPartition
from snuba.utils.streams.consumers.types import TOffset, TStream

TStreamData = TypeVar("TStreamData")


@dataclass(frozen=True)
class Assignment(Generic[TStream, TStreamData]):
    generation: int
    streams: Mapping[TStream, TStreamData]


class AssignmentManager(Generic[TStream, TStreamData]):
    def __init__(self) -> None:
        self.__assignment: Assignment[TStream, TStreamData] = Assignment(0, {})
        self.__lock = Lock()

    def assign(self, streams: Mapping[TStream, TStreamData]) -> None:
        with self.__lock:
            self.__assignment = Assignment(self.__assignment.generation + 1, streams)

    @contextmanager
    def assignment(self) -> Iterator[Assignment[TStream, TStreamData]]:
        with self.__lock:
            yield self.__assignment


class OffsetOutOfRange(Exception):
    pass


@dataclass
class Offsets(Generic[TOffset]):
    local: TOffset
    remote: MutableMapping[str, Optional[TOffset]]


@dataclass
class State(Generic[TOffset]):
    offsets: Offsets[TOffset]


ConsumerGroup = str


@dataclass
class Commit(Generic[TStream, TOffset]):
    stream: TStream
    group: ConsumerGroup
    offset: TOffset


class CommitLogConsumer:
    def __init__(
        self,
        assignment_manager: AssignmentManager[TopicPartition, State[int]],
        topic: str,
        remote_consumer_groups: Set[ConsumerGroup],
    ):
        self.__assignment_manager = assignment_manager
        self.__topic = topic
        self.__remote_consumer_groups = remote_consumer_groups

    def run(self) -> None:
        consumer = ConfluentConsumer()

        generation: Optional[int] = None

        shutdown_requested = False
        while not shutdown_requested:
            with self.__assignment_manager.assignment() as assignment:
                # If our assignment generation is not equal to the current
                # assignment generation, we need to update our assignment.
                if generation != assignment.generation:
                    # Replace the existing assignment with the new assignment.
                    # TODO: Ideally, streams that were part of the previous
                    # generation would retain their previous offsets, while new
                    # streams will consume from their earliest available
                    # offset. This requires preserving the remote offset state
                    # outside of the assignment manager, which I'm not sure is
                    # a very good idea. We'll probably need to figure out
                    # something here at some point, though -- having to
                    # completely reinitialize the offsets (and subscriptions)
                    # after every rebalance is going to probably be pretty
                    # slow.
                    consumer.assign(
                        [
                            ConfluentTopicPartition(
                                stream.topic, stream.partition, OFFSET_BEGINNING,
                            )
                            for stream in assignment.streams.keys()
                        ]
                    )

                    generation = assignment.generation

            def decode(message: KafkaMessage) -> Commit:
                # TODO: This should be encapsulated somewhere other than here.
                topic, partition, group = message.key().decode("utf-8").split(":")
                return Commit(
                    TopicPartition(topic, int(partition)),
                    group,
                    int(message.value().decode("utf-8")),
                )

            updates: MutableMapping[
                TopicPartition, MutableMapping[ConsumerGroup, int]
            ] = defaultdict(dict)

            # TODO: These message/timeout numbers are totally arbitrary and
            # could use some more thought. Larger numbers means less lock
            # contention, but more latency when resuming paused partitions and
            # a greater potential for having to discard work due to rebalances.
            for commit in map(decode, consumer.consume(max_messages=1000, timeout=1)):
                if commit.group in self.__remote_consumer_groups:
                    updates[commit.stream][commit.group] = commit.offset

            with self.__assignment_manager.assignment() as assignment:
                # If the generation has changed while we've been processing
                # messages, we need to discard our local updates.
                if generation != assignment.generation:
                    continue

                for stream, remote_offsets in updates.items():
                    assignment.streams[stream].offsets.remote.update(remote_offsets)


class SubscribedQueryExecutor:
    def __init__(
        self,
        assignment_manager: AssignmentManager[TopicPartition, State[int]],
        topic: str,
        remote_consumer_groups: Set[ConsumerGroup],
    ):
        self.__assignment_manager = assignment_manager
        self.__topic = topic
        self.__remote_consumer_groups = remote_consumer_groups

    def run(self) -> None:
        consumer = ConfluentConsumer()

        def on_assign(consumer, assignment: Sequence[ConfluentTopicPartition]):
            # TODO: This needs to handle initial offset reset if no committed offsets are available.
            offsets: Mapping[TopicPartition, int] = {
                TopicPartition(partition.topic, partition.partition): partition.offset
                for partition in consumer.committed(assignment)
            }

            consumer.assign(
                [
                    ConfluentTopicPartition(stream.topic, stream.partition, offset)
                    for stream, offset in offsets.items()
                ]
            )

            # TODO: Resetting the stream state after every rebalance like this
            # is going to be pretty bad for performance. We'll need to
            # reevaluate this -- splitting local and remote offsets would
            # probably make sense here, and allowing the assignment manager to
            # deal with initializing the state data.
            self.__assignment_manager.assign(
                {
                    stream: State(
                        offsets=Offsets(
                            local=offset,
                            remote={
                                group: None for group in self.__remote_consumer_groups
                            },
                        )
                    )
                    for stream, offset in offsets.items()
                }
            )

        consumer.subscribe([self.__topic], on_assign)

        while True:
            # TODO: Check to see if there are any streams that are paused that
            # have transitioned into a state where they can resume consuming.

            message = consumer.poll(0.1)
            if message is None:
                continue

            error = message.error()
            if error is not None:
                raise Exception(error)

            stream = TopicPartition(message.topic(), message.partition())

            # Check the assignment state to ensure that this message can be
            # accepted without exceeding the remote offset. If it does, we need
            # to pause the partition and seek to the previous offset.
            # NOTE: Ideally this would be encapsulated in the synchronized
            # consumer instead of here.
            with self.__assignment_manager.assignment() as assignment:
                try:
                    # TODO: Update the local offset.
                    assignment.streams[stream]
                except OffsetOutOfRange:
                    partition = ConfluentTopicPartition(
                        stream.topic, stream.partition, message.offset()
                    )
                    consumer.pause(partition)
                    consumer.seek(
                        partition
                    )  # XXX: seek must happen after pause, because confluent-kafka
