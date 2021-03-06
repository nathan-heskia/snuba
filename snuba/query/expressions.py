from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, replace
from typing import Callable, Generic, Iterator, Optional, Sequence, TypeVar, Union

TVisited = TypeVar("TVisited")


@dataclass(frozen=True)
class Expression(ABC):
    """
    A node in the Query AST. This can be a leaf or an intermediate node.
    It represents an expression that can be resolved to a value. This
    includes column names, function calls and boolean conditions (which are
    function calls themselves in the AST), literals, etc.

    All expressions can have an optional alias.
    """

    alias: Optional[str]

    @abstractmethod
    def transform(self, func: Callable[[Expression], Expression]) -> Expression:
        """
        Transforms this expression through the function passed in input.
        This works almost like a map function over sequences though, contrarily to
        sequences, this acts on a subtree. The semantics of transform can be different
        between intermediate nodes and leaves, so each node class can implement it
        its own way.

        All expressions are frozen dataclasses. This means they are immutable and
        format will either return self or a new instance. It cannot transform the
        expression in place.
        """
        raise NotImplementedError

    @abstractmethod
    def __iter__(self) -> Iterator[Expression]:
        """
        Used to iterate over this expression and its children. The exact
        semantics depends on the structure of the expression.
        See the implementations for more details.
        """
        raise NotImplementedError

    @abstractmethod
    def accept(self, visitor: ExpressionVisitor[TVisited]) -> TVisited:
        """
        Accepts a visitor class to traverse the tree. The only role of this method is to
        call the right visit method on the visitor object. Requiring the implementation
        to call the method with the right type forces us to keep the visitor interface
        up to date every time we create a new subclass of Expression.
        """
        raise NotImplementedError


class ExpressionVisitor(ABC, Generic[TVisited]):
    """
    Implementation of a Visitor pattern to simplify traversal of the AST while preserving
    the structure and delegating the control of the traversal algorithm to the client.
    This pattern is generally used for evaluation or formatting. While the iteration
    defined above is for stateless use cases where the order of the nodes is not important.

    The original Visitor pattern does not foresee a return type for visit and accept
    methods, instead it relies on having the Visitor class stateful (any side effect a visit method
    could produce has to make changes to the state of the visitor object). This implementation
    allows the Visitor to define a return type which is generic.
    """

    @abstractmethod
    def visitLiteral(self, exp: Literal) -> TVisited:
        raise NotImplementedError

    @abstractmethod
    def visitColumn(self, exp: Column) -> TVisited:
        raise NotImplementedError

    @abstractmethod
    def visitFunctionCall(self, exp: FunctionCall) -> TVisited:
        raise NotImplementedError

    @abstractmethod
    def visitCurriedFunctionCall(self, exp: CurriedFunctionCall) -> TVisited:
        raise NotImplementedError


@dataclass(frozen=True)
class Literal(Expression):
    """
    A literal in the SQL expression
    """

    value: Union[None, bool, str, float, int]

    def transform(self, func: Callable[[Expression], Expression]) -> Expression:
        return func(self)

    def __iter__(self) -> Iterator[Expression]:
        yield self

    def accept(self, visitor: ExpressionVisitor[TVisited]) -> TVisited:
        return visitor.visitLiteral(self)


@dataclass(frozen=True)
class Column(Expression):
    """
    Represent a column in the schema of the dataset.
    """

    column_name: str
    table_name: Optional[str]

    def transform(self, func: Callable[[Expression], Expression]) -> Expression:
        return func(self)

    def __iter__(self) -> Iterator[Expression]:
        yield self

    def accept(self, visitor: ExpressionVisitor[TVisited]) -> TVisited:
        return visitor.visitColumn(self)


@dataclass(frozen=True)
class FunctionCall(Expression):
    """
    Represents an expression that resolves to a function call on Clickhouse.
    This class also represent conditions. Since Clickhouse supports both the conventional
    infix notation for condition and the functional one, we converge into one
    representation only in the AST to make query processing easier.
    A query processor would not have to care of processing both functional conditions
    and infix conditions.
    """

    function_name: str
    parameters: Sequence[Expression]

    def transform(self, func: Callable[[Expression], Expression]) -> Expression:
        """
        Transforms the subtree starting from the children and then applying
        the transformation function to the root.
        This order is chosen to make the semantics of transform more meaningful,
        the transform operation will be performed on the children first (think
        about the parameters of a function call) and then to the node itself.

        The consequence of this is that, if the transformation function replaces
        the root with something else, with different children, we trust the
        transformation function and we do not run that same function over the
        new children.
        """
        transformed = replace(
            self,
            parameters=list(map(lambda child: child.transform(func), self.parameters)),
        )
        return func(transformed)

    def __iter__(self) -> Iterator[Expression]:
        """
        Traverse the subtree in a postfix order.
        The order here is arbitrary, postfix is chosen to follow the same
        order we have in the transform method.
        """
        for child in self.parameters:
            for sub in child:
                yield sub
        yield self

    def accept(self, visitor: ExpressionVisitor[TVisited]) -> TVisited:
        return visitor.visitFunctionCall(self)


@dataclass(frozen=True)
class CurriedFunctionCall(Expression):
    """
    This function call represent a function with currying: f(x)(y).
    it means applying the function returned by f(x) to y.
    Clickhouse has a few of these functions, like topK(5)(col).

    We intentionally support only two groups of parameters to avoid an infinite
    number of parameters groups recursively.
    """

    # The function on left side of the expression.
    # for topK this would be topK(5)
    internal_function: FunctionCall
    # The list of parameters to apply to the result of internal_function.
    parameters: Sequence[Expression]

    def transform(self, func: Callable[[Expression], Expression]) -> Expression:
        """
        Applies the transformation function to this expression following
        the same policy of FunctionCall. The only difference is that this
        one transforms the internal function before applying the function to the
        parameters.
        """
        transformed = replace(
            self,
            internal_function=self.internal_function.transform(func),
            parameters=list(map(lambda child: child.transform(func), self.parameters)),
        )
        return func(transformed)

    def __iter__(self) -> Iterator[Expression]:
        """
        Traverse the subtree in a postfix order.
        """
        for child in self.internal_function:
            yield child
        for child in self.parameters:
            for sub in child:
                yield sub
        yield self

    def accept(self, visitor: ExpressionVisitor[TVisited]) -> TVisited:
        return visitor.visitCurriedFunctionCall(self)
