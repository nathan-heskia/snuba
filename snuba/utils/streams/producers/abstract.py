from __future__ import annotations

from abc import ABC, abstractmethod
from concurrent.futures import Future
from typing import Generic

from snuba.utils.streams.producers.types import TMessage, TStream


class Producer(ABC, Generic[TStream, TMessage]):
    @abstractmethod
    def produce(self, stream: TStream, message: TMessage) -> Future[None]:
        raise NotImplementedError

    @abstractmethod
    def close(self) -> Future[None]:
        raise NotImplementedError
