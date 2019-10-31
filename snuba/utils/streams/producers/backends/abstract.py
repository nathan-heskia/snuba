from abc import ABC, abstractmethod
from typing import Generic, Optional

from snuba.utils.streams.producers.types import TStream, TValue


class ProducerBackend(ABC, Generic[TStream, TValue]):

    @abstractmethod
    def produce(self, stream: TStream, value: TValue) -> None:
        raise NotImplementedError

    @abstractmethod
    def flush(self, timeout: Optional[float] = None) -> int:
        raise NotImplementedError
