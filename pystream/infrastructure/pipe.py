from functools import reduce, partial
from typing import Callable, List, Iterable, Any, TypeVar, Generic, Tuple, Union, Type, Optional

_AT = TypeVar("_AT")
_RT = TypeVar("_RT")
_AT1 = TypeVar("_AT1")
_RT1 = TypeVar("_RT1")


class _Empty:
    pass


class MaybeEmpty(Generic[_AT]):
    __x: _AT
    __empty: bool

    def __init__(self, x: _AT, empty=False):
        self.__x = x
        self.__empty = empty

    def filter(self, predicate: Callable[[_AT], bool]):
        if self.__empty:
            return self
        if predicate(self.__x):
            return self
        self.__empty = True
        return self

    def map(self, mapper: Callable[[_AT], _RT]) -> Union['MaybeEmpty[_RT]', 'MaybeEmpty[_AT]']:
        if self.__empty:
            return self
        else:
            return MaybeEmpty(mapper(self.__x))

    def is_empty(self) -> bool:
        return self.__empty

    def get_x(self) -> _AT:
        return self.__x


def _init_operation(x: _AT) -> [MaybeEmpty[_AT]]:
    return MaybeEmpty(x)


def _apply_chain_operations(
        x: MaybeEmpty[_AT],
        /,
        op1: Callable[[MaybeEmpty[_AT]], MaybeEmpty[_RT]],
        op2: Callable[[MaybeEmpty[_RT]], MaybeEmpty[_RT1]]
) -> MaybeEmpty[_RT1]:
    return op2(op1(x))


def _identity(x):
    return x


def _filter(x: MaybeEmpty[_AT], /, predicate: Callable[[_AT], bool]) -> MaybeEmpty[_AT]:
    return x.filter(predicate)


def _map(x: MaybeEmpty[_AT], /, mapper: Callable[[_AT], _RT]) -> Union['MaybeEmpty[_RT]', 'MaybeEmpty[_AT]']:
    return x.map(mapper)


class Pipe(Generic[_RT]):
    __operation: Callable[[Any], MaybeEmpty[_RT]]

    def __init__(self, operation: Callable[[Any], MaybeEmpty[_RT]] = _init_operation):
        # noinspection Mypy
        self.__operation = operation

    def map(self, mapper: Callable[[_RT], _RT1]) -> "Pipe[Union['MaybeEmpty[_RT1]', 'MaybeEmpty[_RT]']]":
        # noinspection Mypy
        return Pipe(partial(
            _apply_chain_operations,
            op1=self.__operation,
            op2=partial(_map, mapper=mapper)
        ))

    # noinspection PyMethodMayBeStatic
    def filter(self, predicate: Callable[[_RT], bool]) -> 'Pipe[MaybeEmpty[_RT]]':
        # noinspection Mypy
        return Pipe(partial(
            _apply_chain_operations,
            op1=self.__operation,
            op2=partial(_filter, predicate=predicate)
        ))

    def get_operation(self) -> Callable[[Any], MaybeEmpty[_RT]]:
        # noinspection Mypy
        return self.__operation
