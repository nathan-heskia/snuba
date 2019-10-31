from typing import Optional, Generic

from snuba.utils.streams.producers.backends.abstract import ProducerBackend
from snuba.utils.streams.producers.types import TStream, TValue


class Producer(Generic[TStream, TValue]):
    def __init__(self, backend: ProducerBackend[TStream, TValue]):
        self.__backend = backend

    def produce(self, stream: TStream, value: TValue) -> None:
        return self.__backend.produce(stream, value)

    def flush(self, timeout: Optional[float] = None) -> int:
        return self.__backend.flush(timeout)
