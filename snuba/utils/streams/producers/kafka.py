from __future__ import annotations

from concurrent.futures import Future
from dataclasses import dataclass
from functools import partial
from queue import Empty, Queue
from typing import Any, Mapping, MutableMapping, NamedTuple, Optional, Union

from confluent_kafka import KafkaError as ConfluentKafkaError
from confluent_kafka import Producer as ConfluentKafkaProducer

from snuba.utils.concurrent import execute
from snuba.utils.streams.producers.abstract import Producer


class TopicPartition(NamedTuple):
    topic: str
    partition: Optional[int] = None


@dataclass
class KafkaMessage:
    value: Optional[bytes]
    key: Optional[bytes] = None


@dataclass
class ProduceRequest:
    stream: TopicPartition
    message: KafkaMessage
    future: Future[None]


@dataclass
class CloseRequest:
    future: Future[None]


class KafkaProducer(Producer[TopicPartition, KafkaMessage]):
    def __init__(self, configuration: Mapping[str, Any]):
        self.__configuration = configuration

        # TODO: Need to decide if this queue should be bound.
        self.__queue: Queue[Union[ProduceRequest, CloseRequest]] = Queue()
        self.__closed: bool = False  # XXX: Not thread safe

        self.__worker = execute(self.__run)

    def __run(self) -> None:
        producer = ConfluentKafkaProducer(self.__configuration)

        def delivery_callback(
            future: Future[None], error: ConfluentKafkaError, message: Any
        ) -> None:
            if error:
                future.set_exception(Exception(error))
            else:
                future.set_result(None)

        while True:
            producer.poll(0.0)

            try:
                request = self.__queue.get(block=False)
            except Empty:
                continue

            if isinstance(request, ProduceRequest):
                if request.future.set_running_or_notify_cancel():
                    kwargs: MutableMapping[str, Any] = {}
                    if request.message.value is not None:
                        kwargs["value"] = request.message.value
                    if request.message.key is not None:
                        kwargs["key"] = request.message.key
                    if request.stream.partition is not None:
                        kwargs["partition"] = request.stream.partition
                    try:
                        producer.produce(
                            request.stream.topic,
                            on_delivery=partial(delivery_callback, request.future),
                            **kwargs,
                        )
                    except Exception as e:
                        request.future.set_exception(e)
            elif isinstance(request, CloseRequest):
                # TODO: Need to ensure that the close method is always the
                # last message added to the queue.
                if request.future.set_running_or_notify_cancel():
                    try:
                        producer.flush()
                    except Exception as e:
                        request.future.set_exception(e)
                    else:
                        request.future.set_result(None)
                    finally:
                        break
            else:
                raise ValueError

    def produce(self, stream: TopicPartition, message: KafkaMessage) -> Future[None]:
        if self.__closed:
            raise RuntimeError("closed")

        if not self.__worker.running():
            raise RuntimeError("worker thread not running")

        future: Future[None] = Future()
        # TODO: Need to decide if this blocking or not.
        self.__queue.put(ProduceRequest(stream, message, future))
        return future

    def close(self) -> Future[None]:
        if self.__closed:
            raise RuntimeError("closed")

        if not self.__worker.running():
            raise RuntimeError("worker thread not running")

        self.__closed = True

        future: Future[None] = Future()
        # TODO: Need to decide if this blocking or not.
        self.__queue.put(CloseRequest(future))
        return future
