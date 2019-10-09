import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import (
    Any,
    Generic,
    MutableMapping,
    MutableSequence,
    Optional,
    Sequence,
    Type,
    TypeVar,
)

from confluent_kafka import (
    OFFSET_BEGINNING,
    OFFSET_END,
    OFFSET_INVALID,
    OFFSET_STORED,
    KafkaError,
    KafkaException,
    Producer,
)
from confluent_kafka import Message as ConfluentMessage

from snuba.utils.metrics.backends.abstract import MetricsBackend
from snuba.utils.streams.abstract import ConsumerError
from snuba.utils.streams.kafka import KafkaConsumer, KafkaMessage, TopicPartition


logger = logging.getLogger("batching-kafka-consumer")

TMessage = TypeVar("TMessage")


class AbstractBatchWorker(ABC, Generic[TMessage]):
    """The `BatchingKafkaConsumer` requires an instance of this class to
    handle user provided work such as processing raw messages and flushing
    processed batches to a custom backend."""

    @abstractmethod
    def process_message(self, message: TMessage) -> Optional[Any]:
        """Called with each (raw) Kafka message, allowing the worker to do
        incremental (preferablly local!) work on events. The object returned
        is put into the batch maintained by the `BatchingKafkaConsumer`.

        If this method returns `None` it is not added to the batch.

        A simple example would be decoding the JSON value and extracting a few
        fields.
        """
        pass

    @abstractmethod
    def flush_batch(self, batch: Sequence[Any]) -> None:
        """Called with a list of pre-processed (by `process_message`) objects.
        The worker should write the batch of processed messages into whatever
        store(s) it is maintaining. Afterwards the Kafka offsets are committed.

        A simple example would be writing the batch to another Kafka topic.
        """
        pass


@dataclass
class Offsets:
    __slots__ = ["lo", "hi"]
    lo: int
    hi: int


class BatchingKafkaConsumer:
    """The `BatchingKafkaConsumer` is an abstraction over most Kafka consumer's main event
    loops. For this reason it uses inversion of control: the user provides an implementation
    for the `AbstractBatchWorker` and then the `BatchingKafkaConsumer` handles the rest.

    Main differences from the default KafkaConsumer are as follows:
    * Messages are processed locally (e.g. not written to an external datastore!) as they are
      read from Kafka, then added to an in-memory batch
    * Batches are flushed based on the batch size or time sent since the first message
      in the batch was recieved (e.g. "500 items or 1000ms")
    * Kafka offsets are not automatically committed! If they were, offsets might be committed
      for messages that are still sitting in an in-memory batch, or they might *not* be committed
      when messages are sent to an external datastore right before the consumer process dies
    * Instead, when a batch of items is flushed they are written to the external datastore and
      then Kafka offsets are immediately committed (in the same thread/loop)
    * Users need only provide an implementation of what it means to process a raw message
      and flush a batch of events
    * Supports an optional "dead letter topic" where messages that raise an exception during
      `process_message` are sent so as not to block the pipeline.

    NOTE: This does not eliminate the possibility of duplicates if the consumer process
    crashes between writing to its backend and commiting Kafka offsets. This should eliminate
    the possibility of *losing* data though. An "exactly once" consumer would need to store
    offsets in the external datastore and reconcile them on any partition rebalance.
    """

    # Set of logical (not literal) offsets to not publish to the commit log.
    # https://github.com/confluentinc/confluent-kafka-python/blob/443177e1c83d9b66ce30f5eb8775e062453a738b/tests/test_enums.py#L22-L25
    LOGICAL_OFFSETS = frozenset(
        [OFFSET_BEGINNING, OFFSET_END, OFFSET_STORED, OFFSET_INVALID]
    )

    def __init__(
        self,
        consumer: KafkaConsumer,
        topic: str,
        worker: AbstractBatchWorker[KafkaMessage],
        max_batch_size: int,
        max_batch_time: int,
        group_id: str,
        metrics: MetricsBackend,
        producer: Optional[Producer] = None,
        commit_log_topic: Optional[str] = None,
        recoverable_errors: Optional[Sequence[Type[ConsumerError]]] = None,
    ) -> None:
        self.consumer = consumer

        assert isinstance(worker, AbstractBatchWorker)
        self.worker = worker

        self.max_batch_size = max_batch_size
        self.max_batch_time = max_batch_time  # in milliseconds
        self.__metrics = metrics
        self.group_id = group_id

        self.shutdown = False

        self.__batch_results: MutableSequence[Any] = []
        self.__batch_offsets: MutableMapping[TopicPartition, Offsets] = {}
        self.__batch_deadline: Optional[float] = None
        self.__batch_messages_processed_count: int = 0
        # the total amount of time, in milliseconds, that it took to process
        # the messages in this batch (does not included time spent waiting for
        # new messages)
        self.__batch_processing_time_ms: float = 0.0

        self.producer = producer
        self.commit_log_topic = commit_log_topic

        # The types passed to the `except` clause must be a tuple, not a Sequence.
        self.__recoverable_errors = tuple(recoverable_errors or [])

        def on_partitions_assigned(partitions: Sequence[TopicPartition]) -> None:
            logger.info("New partitions assigned: %r", partitions)

        def on_partitions_revoked(partitions: Sequence[TopicPartition]) -> None:
            "Reset the current in-memory batch, letting the next consumer take over where we left off."
            logger.info("Partitions revoked: %r", partitions)
            self._flush(force=True)

        self.consumer.subscribe(
            [topic], on_assign=on_partitions_assigned, on_revoke=on_partitions_revoked
        )

    def run(self) -> None:
        "The main run loop, see class docstring for more information."

        logger.debug("Starting")
        while not self.shutdown:
            self._run_once()

        self._shutdown()

    def _run_once(self) -> None:
        self._flush()

        if self.producer:
            self.producer.poll(0.0)

        try:
            msg = self.consumer.poll(timeout=1.0)
        except self.__recoverable_errors:
            return

        if msg is None:
            return

        self._handle_message(msg)

    def signal_shutdown(self) -> None:
        """Tells the `BatchingKafkaConsumer` to shutdown on the next run loop iteration.
        Typically called from a signal handler."""
        logger.debug("Shutdown signalled")

        self.shutdown = True

    def _handle_message(self, msg: KafkaMessage) -> None:
        start = time.time()

        # set the deadline only after the first message for this batch is seen
        if not self.__batch_deadline:
            self.__batch_deadline = self.max_batch_time / 1000.0 + start

        result = self.worker.process_message(msg)
        if result is not None:
            self.__batch_results.append(result)

        duration = (time.time() - start) * 1000
        self.__batch_messages_processed_count += 1
        self.__batch_processing_time_ms += duration
        self.__metrics.timing("process_message", duration)

        if msg.stream in self.__batch_offsets:
            self.__batch_offsets[msg.stream].hi = msg.offset
        else:
            self.__batch_offsets[msg.stream] = Offsets(msg.offset, msg.offset)

    def _shutdown(self) -> None:
        logger.debug("Stopping")

        # drop in-memory events, letting the next consumer take over where we left off
        self._reset_batch()

        # close the consumer
        logger.debug("Stopping consumer")
        self.consumer.close()
        logger.debug("Stopped")

    def _reset_batch(self) -> None:
        logger.debug("Resetting in-memory batch")
        self.__batch_results = []
        self.__batch_offsets = {}
        self.__batch_deadline = None
        self.__batch_messages_processed_count = 0
        self.__batch_processing_time_ms = 0.0

    def _flush(self, force: bool = False) -> None:
        """Decides whether the `BatchingKafkaConsumer` should flush because of either
        batch size or time. If so, delegate to the worker, clear the current batch,
        and commit offsets to Kafka."""
        if not self.__batch_messages_processed_count > 0:
            return  # No messages were processed, so there's nothing to do.

        batch_by_size = len(self.__batch_results) >= self.max_batch_size
        batch_by_time = self.__batch_deadline and time.time() > self.__batch_deadline
        if not (force or batch_by_size or batch_by_time):
            return

        logger.info(
            "Flushing %s items (from %r): forced:%s size:%s time:%s",
            len(self.__batch_results),
            self.__batch_offsets,
            force,
            batch_by_size,
            batch_by_time,
        )

        self.__metrics.timing(
            "process_message.normalized",
            self.__batch_processing_time_ms / self.__batch_messages_processed_count,
        )

        batch_results_length = len(self.__batch_results)
        if batch_results_length > 0:
            logger.debug("Flushing batch via worker")
            flush_start = time.time()
            self.worker.flush_batch(self.__batch_results)
            flush_duration = (time.time() - flush_start) * 1000
            logger.info("Worker flush took %dms", flush_duration)
            self.__metrics.timing("batch.flush", flush_duration)
            self.__metrics.timing(
                "batch.flush.normalized", flush_duration / batch_results_length
            )

        logger.debug("Committing Kafka offsets")
        commit_start = time.time()
        self._commit()
        commit_duration = (time.time() - commit_start) * 1000
        logger.debug("Kafka offset commit took %dms", commit_duration)

        self._reset_batch()

    def _commit_message_delivery_callback(
        self, error: Optional[KafkaError], message: ConfluentMessage
    ) -> None:
        if error is not None:
            raise Exception(error.str())

    def _commit(self) -> None:
        retries = 3
        while True:
            try:
                offsets = self.consumer.commit()
                logger.debug("Committed offsets: %s", offsets)
                break  # success
            except KafkaException as e:
                if e.args[0].code() in (
                    KafkaError.REQUEST_TIMED_OUT,
                    KafkaError.NOT_COORDINATOR_FOR_GROUP,
                    KafkaError._WAIT_COORD,
                ):
                    logger.warning("Commit failed: %s (%d retries)", str(e), retries)
                    if retries <= 0:
                        raise
                    retries -= 1
                    time.sleep(1)
                    continue
                else:
                    raise

        if self.commit_log_topic:
            for stream, offset in offsets.items():
                # XXX: This is an abstraction leak from the Kafka consumer.
                if offset in self.LOGICAL_OFFSETS:
                    logger.debug(
                        "Skipped publishing logical offset (%r) to commit log for %s",
                        offset,
                        stream,
                    )
                    continue
                elif offset < 0:
                    logger.warning(
                        "Found unexpected negative offset (%r) after commit for %s",
                        offset,
                        stream,
                    )

                assert self.producer is not None  # XXX: Hack to ensure non-Optional
                self.producer.produce(
                    self.commit_log_topic,
                    key="{}:{}:{}".format(
                        stream.topic, stream.partition, self.group_id
                    ).encode("utf-8"),
                    value="{}".format(offset).encode("utf-8"),
                    on_delivery=self._commit_message_delivery_callback,
                )
