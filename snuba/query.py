from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import NewType, Optional, Sequence, Set, Union

from snuba.datasets import DataSet


Alias = NewType("Alias", str)


Literal = Union[None, bool, float, int, str]


@dataclass
class Column:
    name: str


@dataclass
class Function:
    name: str
    arguments: Optional[Sequence[Expression]] = None


Expression = Union[Alias, Column, Function, Literal]


@dataclass
class AliasedExpression:
    expression: Expression
    alias: Optional[str] = None


@dataclass
class From:
    dataset: DataSet
    # TODO: Support sampling.


@dataclass
class GroupBy:
    expressions: Sequence[Expression]
    totals: bool = False


class Direction(Enum):
    ASC = "asc"
    DESC = "desc"


@dataclass
class OrderByExpression:
    expression: Expression
    direction: Direction = Direction.ASC


@dataclass
class OrderBy:
    expressions: Sequence[OrderByExpression]


@dataclass
class Limit:
    rows: int
    offset: int = 0


@dataclass
class LimitBy:
    rows: int
    expressions: Sequence[Expression]
    offset: int = 0


@dataclass
class Query:
    select: Sequence[Union[Expression, AliasedExpression]]
    from_: From
    where: Optional[Expression] = None
    groupby: Optional[GroupBy] = None
    having: Optional[Expression] = None
    orderby: Optional[Sequence[Expression]] = None
    limit: Optional[Limit] = None
    limit_by: Optional[LimitBy] = None


from snuba.datasets.factory import get_dataset

query = Query(
    select=[Column("group_id"), AliasedExpression(Function("count"), alias="c")],
    from_=From(get_dataset("events")),
    where=Function("equals", [Column("project_id"), 1]),
    groupby=GroupBy([Column("group_id")]),
    having=Function("greater", [Alias("c"), 10]),
)
