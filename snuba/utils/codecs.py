from abc import ABC, abstractmethod
from typing import Generic, TypeVar


TInput = TypeVar('TInput')

TOutput = TypeVar('TOutput')


class Decoder(ABC, Generic[TInput, TOutput]):
    @abstractmethod
    def decode(self, input: TInput) -> TOutput:
        raise NotImplementedError
