from typing import Any, Mapping, NamedTuple, Optional

from confluent_kafka import Producer

from snuba.utils.streams.producers.backends.abstract import ProducerBackend


class TopicPartition(NamedTuple):
    topic: str
    partition: Optional[int]


class KafkaProducerBackend(ProducerBackend[TopicPartition, bytes]):
    def __init__(self, configuration: Mapping[str, Any]) -> None:
        self.__producer = Producer(configuration)

    def poll(self, timeout: Optional[float] = None) -> None:
        self.__producer.poll(*[timeout] if timeout is not None else [])
        return None

    def produce(self, stream: TopicPartition, value: bytes) -> None:
        kwargs = {}
        if stream.partition is not None:
            kwargs["partition"] = stream.partition
        self.__producer.produce(stream.topic, value, **kwargs)
        return None

    def flush(self, timeout: Optional[float] = None) -> int:
        size: int = self.__producer.flush(*[timeout] if timeout is not None else [])
        return size
