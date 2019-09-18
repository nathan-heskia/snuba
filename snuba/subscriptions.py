from __future__ import annotations

from abc import ABC, abstractmethod
from concurrent.futures import Future, as_completed
from typing import (
    Any,
    Generic,
    Iterable,
    Iterator,
    Mapping,
    MutableMapping,
    NamedTuple,
    Optional,
    Sequence,
    TypeVar,
)

from confluent_kafka import TIMESTAMP_LOG_APPEND_TIME, Consumer, TopicPartition


# TODO: Synchronize these types with the Reader interface.
class Query:
    pass


class Result:
    pass


class Subcription:
    pass


class Task(NamedTuple):
    subscription: Subcription
    query: Query


class TaskSet(ABC):
    @abstractmethod
    def __bool__(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def __iter__(self) -> Iterator[Task]:
        raise NotImplementedError


TTaskSet = TypeVar("TTaskSet", bound=TaskSet)


class Scheduler(Generic[TTaskSet], ABC):
    @abstractmethod
    def poll(self, timeout: Optional[float] = None) -> Optional[TTaskSet]:
        """
        Poll to see if any tasks are ready to execute.
        """
        raise NotImplementedError

    @abstractmethod
    def done(self, tasks: TTaskSet) -> None:
        """
        Mark all tasks within the task set as completed.
        """
        raise NotImplementedError


class Position(NamedTuple):
    offset: int
    timestamp: float


class Interval(NamedTuple):
    lower: Optional[Position]
    upper: Position

    def shift(self, upper: Position) -> Interval:
        # TODO: It probably makes sense to warn or throw if lower > upper.
        return Interval(self.upper, upper)


class KafkaTaskSet(TaskSet):
    def __init__(self, partition: int, interval: Interval, tasks: Sequence[Task]):
        self.__partition = partition
        self.__interval = interval
        self.__tasks = tasks

    @property
    def partition(self) -> int:
        return self.__partition

    @property
    def interval(self) -> Interval:
        return self.__interval

    def __bool__(self) -> bool:
        return bool(self.__tasks)

    def __iter__(self) -> Iterator[Task]:
        return iter(self.__tasks)


class KafkaScheduler(Scheduler[KafkaTaskSet]):
    def __init__(self, configuration: Mapping[str, Any], topic: str) -> None:
        self.__configuration = configuration
        self.__topic = topic

        # There are three valid states for a partition in this mapping:
        # 1. Partitions that have been assigned but have not yet had any messages
        # consumed from them will have a value of ``None``.
        # 2. Partitions that have had a single message consumed will have a
        # value of ``(None, Position of Message A)``.
        # 3. Partitions that have had more than one message consumed will have
        # a value of ``(Position of Message A, Position of Message B)``.
        #
        # Take this example, where a partition contains three messages (MA, MB,
        # MC) and a scheduled task (T1-TN).
        #
        #    Messages:           MA        MB        MC
        #    Timeline: +---------+---------+---------+---------
        #    Tasks:    ^    ^    ^    ^    ^    ^    ^    ^
        #              T1   T2   T3   T4   T5   T6   T7   T8
        #
        # In this example, when we are assigned the partition, the state is set
        # to ``None``. After consuming Message A ("MA"), the partition state
        # becomes ``(None, MA)``. No tasks will have yet been executed.
        #
        # When Message B is consumed, the partition state becomes be ``(MA,
        # MB)``. At this point, T4 and T5 (the tasks that are scheduled between
        # the timestamps of messages "MA" and "MB") will be included in the
        # ``TaskSet`` returned by the ``poll`` call. T3 will not be included,
        # since it was presumably contained within a ``TaskSet`` instance
        # returned by a previous ``poll`` call. The lower bound ("MA" in this
        # case) is exclusive, while the upper bound ("MB") is inclusive.
        #
        # When all tasks in the ``TaskSet`` have been successfully evaluated,
        # committing the task set will commit the *lower bound* offset of this
        # task set. The lower bound is selected so that on consumer restart or
        # rebalance, the message that has an offset greater than the lower
        # bound (in our case, "MB") will be the first message consumed. The
        # next tasks to be executed will be those that are scheduled between
        # the timestamps of "MB" and "MC" (again: lower bound exclusive, upper
        # bound inclusive): T6 and T7.
        self.__partitions: MutableMapping[int, Optional[Interval]] = {}

        self.__consumer = Consumer(configuration)
        self.__consumer.subscribe(
            [topic], on_assign=self.__on_assign, on_revoke=self.__on_revoke
        )

    def __on_assign(
        self, consumer: Consumer, assignment: Sequence[TopicPartition]
    ) -> None:
        for tp in assignment:
            if tp.partition not in self.__partitions:
                self.__partitions[tp.partition] = None

    def __on_revoke(
        self, consumer: Consumer, assignment: Sequence[TopicPartition]
    ) -> None:
        for tp in assignment:
            del self.__partitions[tp.partition]

    def poll(self, timeout: Optional[float] = None) -> Optional[KafkaTaskSet]:
        # TODO: If all partitions are paused, this should probably raise an
        # error? Or will commit be able to occur via a separate thread, in
        # which cause this should then set an event/condition variable?
        message = self.__consumer.poll(timeout)
        if message is None:
            return None

        error = message.error()
        if error is not None:
            raise error

        timestamp_type, timestamp = message.timestamp()
        assert timestamp_type == TIMESTAMP_LOG_APPEND_TIME

        position = Position(message.offset(), timestamp / 1000.0)

        interval = self.__partitions[message.partition()]

        if interval is None:
            interval = Interval(None, position)
        else:
            interval = interval.shift(position)

        self.__partitions[message.partition()] = interval

        if interval.lower is None:
            return None

        # TODO: Get tasks that are scheduled between the interval.
        return KafkaTaskSet(message.partition(), interval, [])

    def done(self, tasks: KafkaTaskSet) -> None:
        assert self.__partitions[tasks.partition] == tasks.interval

        # N.B.: ``store_offset``'s handling of offset values differs from that
        # of ``commit``! The offset passed to ``store_offset`` will be the
        # offset of the first message read when the consumer restarts.
        self.__consumer.store_offsets(
            offsets=[
                TopicPartition(self.__topic, tasks.partition, tasks.interval.upper)
            ]
        )


class Executor:
    def submit(self, queries: Iterable[Query]) -> Iterator["Future[Result]"]:
        """
        Run a collection of queries as efficiently as possible.
        """
        # TODO: Somewhere in here we're going to need to figure out where the
        # correct place is to apply the replacement exclusion set and/or FINAL
        # modifiers.
        # TODO: Coalescing eventually will happen here. For now, this will
        # probably just loop through the subscriptions and submit them to a
        # thread poll that will run through independent Reader instances.
        raise NotImplementedError


class Producer:
    def produce(self, task: Task, result: Result) -> "Future[None]":
        """
        Produce the result of a task.
        """
        raise NotImplementedError


if __name__ == "__main__":
    scheduler = Scheduler()
    executor = Executor()
    producer = Producer()

    while True:
        tasks = scheduler.poll()
        if tasks is None:
            continue

        # Submit the queries for execution, maintaining a mapping of futures to
        # the task that initiated the query so that we can accomodate results
        # being retrieved in any order.
        futures = dict(zip(executor.submit(task.query for task in tasks), tasks))

        # Iterate through the futures in the order that they are completed,
        # mapping them back to the originating task and publishing the results.
        for future in as_completed(futures.keys()):
            task = futures[future]

            try:
                result = future.result()
            except Exception:
                # TODO: Need to figure out what to do with the error here. This
                # probably should make a distinction between ephemeral errors
                # that can be retried (connection resets, etc.) and other
                # errors that indicate that the query cannot succeed
                # (referencing columns that no longer exist, etc.)
                pass
            else:
                # TODO: What should we do on a failure to publish results?
                producer.produce(task, result).result()

        scheduler.done(tasks)
