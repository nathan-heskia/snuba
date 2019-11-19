from __future__ import annotations

from dataclasses import dataclass
from threading import Lock
from typing import Generic, Set, Sequence

from confluent_kafka import (
    Consumer as ConfluentConsumer,
    TopicPartition as ConfluentTopicPartition,
)

from snuba.utils.streams.consumers.backends.kafka import TopicPartition
from snuba.utils.streams.consumers.types import TStream


@dataclass(frozen=True)
class Assignment(Generic[TStream]):
    generation: int
    streams: Set[TStream]


class AssignmentManager(Generic[TStream]):
    def __init__(self) -> None:
        self.__assignment: Assignment[TStream] = Assignment(0, {})
        self.__lock = Lock()

    def __enter__(self) -> Assignment[TStream]:
        self.__lock.acquire()
        return self.__assignment

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.__lock.release()

    def assign(self, streams: Set[TStream]) -> None:
        with self.__lock:
            self.__assignment = Assignment(self.__assignment.generation + 1, streams)


class OffsetOutOfRange(Exception):
    pass


class SubscribedQueryExecutor:
    def __init__(self, assignment_manager: AssignmentManager[TStream], topic: str):
        self.__assignment_manager = assignment_manager
        self.__topic = topic

    def run(self) -> None:
        # TODO: This should use the generic consumer once things have settled
        # down a little bit.
        consumer = ConfluentConsumer()

        def on_assign(consumer, assignment: Sequence[ConfluentTopicPartition]):
            self.__assignment_manager.assign(
                set(
                    TopicPartition(partition.topic, partition.partition)
                    for partition in assignment
                )
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
            with self.__assignment_manager as assignment:
                try:
                    # TODO: Update the local offset.
                    assignment.streams[stream]
                except OffsetOutOfRange:
                    partition = TopicPartition(
                        stream.topic, stream.partition, message.offset()
                    )
                    consumer.pause(partition)
                    consumer.seek(
                        partition
                    )  # XXX: seek must happen after pause, because confluent-kafka
                    continue
