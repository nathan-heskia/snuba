from __future__ import annotations
from typing import (
    TYPE_CHECKING,
    Any,
    NamedTuple,
    NewType,
    Optional,
    Sequence,
    Tuple,
    Union,
)

Column = NewType("Column", str)


class Condition(NamedTuple):
    lhs: Any
    op: str
    rhs: Any


class Aggregation(NamedTuple):
    function: str
    column: Optional[Column]
    alias: Optional[str]


if TYPE_CHECKING:
    from mypy_extensions import TypedDict

    Query = TypedDict(
        "Query",
        {
            "selected_columns": Any,
            "aggregations": Sequence[Aggregation],
            "arrayjoin": Optional[Column],
            "sample": Optional[Union[int, float]],
            "conditions": Any,
            "groupby": Any,
            "totals": bool,
            "having": Any,
            "orderby": Any,
            "limit": int,
            "offset": Optional[int],
            "limitby": Optional[Tuple[int, Column]],
        },
    )


def to_column(value) -> Column:
    if isinstance(value, str):
        return Column(value)
    else:
        raise ValueError(f"cannot convert {value!r} to column")


def to_column_list(value):  # TODO
    # each value is either a column name or a nested expression
    return value


def to_aggregation(value) -> Aggregation:
    function, column, alias = value

    if not isinstance(function, str):
        raise ValueError(f"cannot convert {function!r} to aggregation function name")

    if column == "":
        column = None

    if column is not None:
        column = to_column(column)

    if alias == "":
        alias = None

    if alias is not None and not isinstance(alias, str):
        raise ValueError(f"cannot convert {alias!r} to aggregation function alias")

    return Aggregation(function, column, alias)


def to_aggregation_list(value) -> Sequence[Aggregation]:
    if not isinstance(value, list):
        raise ValueError(f"cannot convert {value!r} to aggregation list")
    return [to_aggregation(val) for val in value]


def to_condition(value) -> Condition:
    lhs, operator, rhs = value  # TODO
    return Condition(lhs, operator, rhs)


def to_bool(value) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"cannot convert {value!r} to boolean")
    return value


def to_int(value) -> int:
    if not isinstance(value, int):
        raise ValueError(f"cannot convert {value!r} to int")
    return value


def to_number(value) -> Union[int, float]:
    if not isinstance(value, (int, float)):
        raise ValueError(f"cannot convert {value!r} to number")
    return value


def to_limitby(value) -> Tuple[int, Column]:
    n, col = value
    return to_int(n), to_column(col)


def normalize_query(value) -> Query:
    return {
        "selected_columns": (
            to_column_list(value["selected_columns"])
            if isinstance(value["selected_columns"], list)
            else [to_column(value["selected_columns"])]
        ),
        "aggregations": to_aggregation_list(value["aggregations"]),
        "arrayjoin": to_column(value["arrayjoin"]) if "arrayjoin" in value else None,
        "sample": to_number(value["sample"]) if "sample" in value else None,
        "conditions": value["conditions"],  # TODO
        "groupby": (
            to_column_list(value["groupby"])
            if isinstance(value["groupby"], list)
            else [to_column(value["groupby"])]
        ),
        "totals": bool(value["totals"]),
        "having": [to_condition(val) for val in value["having"]],
        "orderby": value["orderby"] if "orderby" in value else None,  # TODO
        "limit": int(value["limit"]),
        "offset": int(value["offset"]) if "offset" in value else None,
        "limitby": to_limitby(value["limitby"]) if "limitby" in value else None,
    }
