from typing import TypeVar
from typing_extensions import Protocol


T = TypeVar("T", contravariant=True)


class Comparable(Protocol[T]):
    def __lt__(self, other: T) -> bool:
        raise NotImplementedError

    def __le__(self, other: T) -> bool:
        raise NotImplementedError

    def __gt__(self, other: T) -> bool:
        raise NotImplementedError

    def __ge__(self, other: T) -> bool:
        raise NotImplementedError

    def __eq__(self, other: object) -> bool:
        raise NotImplementedError

    def __ne__(self, other: object) -> bool:
        raise NotImplementedError
