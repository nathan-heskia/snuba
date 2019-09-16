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
    Tuple,
    TypeVar,
    Union,
)

from confluent_kafka import (
    TIMESTAMP_LOG_APPEND_TIME,
    Consumer,
    KafkaConsumer,
    TopicPartition,
)


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


class TaskSet:
    def __iter__(self) -> Iterator[Task]:
        raise NotImplementedError


TTaskSet = TypeVar('TTaskSet', bound=TaskSet)


class Scheduler(Generic[TTaskSet]):
    def poll(self, timeout: Optional[float] = None) -> Optional[TTaskSet]:
        """
        Poll to see if any tasks are ready to execute.
        """
        raise NotImplementedError

    def commit(self, tasks: TTaskSet):
        """
        Mark all tasks within the task set as completed.
        """
        raise NotImplementedError


class KafkaTaskSet(TaskSet):
    pass


class Position(NamedTuple):
    offset: int
    timestamp: float


class PartitionState(NamedTuple):
    previous: Optional[Position]
    current: Position


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
        self.__partitions: MutableMapping[int, Optional[PartitionState]] = {}
        self.__consumer = KafkaConsumer(configuration)
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
        # TODO: Actually respect the `timeout` parameter.
        while True:
            message = self.__consumer.poll()
            if message is None:
                continue

            error = message.error()
            if error is not None:
                raise error

            timestamp_type, timestamp = message.timestamp()
            assert timestamp_type == TIMESTAMP_LOG_APPEND_TIME

            partition = message.partition()
            assert partition in self.__partitions

            state = self.__partitions[partition]
            position = Position(message.offset(), timestamp / 1000.0)
            if state is None:
                state = PartitionState(None, position)
            else:
                state = PartitionState(state.current, position)
                # TODO: Create ``TaskSet`` with tasks between timestamps.
                # TODO: Pause partition consumption.

            self.__partitions[partition] = state

    def commit(self, tasks: KafkaTaskSet):
        # TODO: This should also unlock the partition.
        # If the scheduler is stateful, this enables marking a collection of
        # tasks as done (for example, if we are using a Kafka partition
        # timestamp advancing as a "clock" rather than the system/wall clock,
        # this allows for committing that offset after all of the tasks
        # scheduled for that point have been completed.)
        # TODO: This probably could use a better return type.
        raise NotImplementedError


class Executor:
    def submit(self, queries: Iterable[Query]) -> Iterator[Future[Result]]:
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
    def produce(self, task: Task, result: Result) -> Future[None]:
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

        scheduler.commit(tasks)
