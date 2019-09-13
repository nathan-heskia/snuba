import functools
from concurrent.futures import Future, as_completed
from typing import Any, Iterable, Iterator, NamedTuple, Optional


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

    def commit(self) -> Future[None]:
        """Mark all tasks within the set as completed."""
        # If the scheduler is stateful, this enables marking a collection of
        # tasks as done (for example, if we are using a Kafka partition
        # timestamp advancing as a "clock" rather than the system/wall clock,
        # this allows for committing that offset after all of the tasks
        # scheduled for that point have been completed.)
        # TODO: This probably could use a better return type.
        raise NotImplementedError


class Scheduler:
    def poll(self, timeout: Optional[float] = None) -> Optional[TaskSet]:
        """
        Poll to see if any tasks are ready to execute.
        """
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

        tasks.commit().result()
