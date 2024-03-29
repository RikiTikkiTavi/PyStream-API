from functools import partial
from itertools import chain
from multiprocessing.pool import Pool
from multiprocessing import cpu_count
from typing import (
    Generic,
    Iterator,
    TypeVar,
    Callable,
    Iterable,
    Tuple,
    Any,
    Generator,
    cast,
)
import pystream.core.utils as utils
import pystream.sequential_stream as stream
import pystream.core.pipe as core_pipe
import pystream.collectors as collectors

import pystream.types

_AT = TypeVar("_AT")
_RT = TypeVar("_RT")

_NAT = TypeVar("_NAT", bound=pystream.types.SupportsAddAndCompare)


def _reducer(pair: Tuple[_AT, ...], /, reducer: Callable[[_AT, _AT], _AT]) -> _AT:
    return reducer(*pair) if len(pair) == 2 else pair[0]


def _order_reducer(*args: _AT, selector: Callable[[Tuple[_AT, ...]], _AT]) -> _AT:
    return selector(args)


def _with_action(x: _AT, /, action: Callable[[_AT], Any]) -> _AT:
    action(x)
    return x


class ParallelStream(Generic[_AT]):
    __n_processes: int
    __pipe: core_pipe.Pipe[_AT]
    __iterable: Iterator[_AT]

    def __init__(
        self,
        *iterables: Iterable[_AT],
        n_processes: int = cpu_count(),
        chunk_size: int = 1,
    ):
        self.__iterable = chain(*iterables)
        self.__n_processes = n_processes
        self.__pipe = core_pipe.Pipe()
        self.__chunk_size = chunk_size

    def __iterator_pipe(self, pool: Pool) -> Iterator[_AT]:
        return core_pipe.filter_out_empty(
            pool.imap(
                self.__pipe.get_operation(),
                self.__iterable,
                chunksize=self.__chunk_size,
            )
        )

    def iterator(self) -> Generator[_AT, None, None]:
        """
        Creates iterator from stream.
        This is terminal operation.

        :returns: Iterator over stream elements
        """
        with Pool(processes=self.__n_processes) as pool:
            for element in self.__iterator_pipe(pool):
                yield element

    def partition_iterator(
        self, partition_size: int
    ) -> Generator[list[_AT], None, None]:
        """
        Creates iterator over partitions of stream. This is terminal operation.

        :param partition_size: Length of partition
        :returns: Iterator over partitions of stream.
        """
        return utils.partition_generator(self.iterator(), partition_size)

    def map(self, mapper: Callable[[_AT], _RT]) -> "ParallelStream[_RT]":
        """
        Returns a stream consisting of the results of applying the given function to the elements of this stream.
        This is an intermediate operation.

        :param mapper: Mapper function
        :return: Stream with mapper operation lazily applied
        """
        self.__pipe = self.__pipe.map(mapper)
        return cast("ParallelStream[_RT]", self)

    def filter(self, predicate: Callable[[_AT], bool]) -> "ParallelStream[_AT]":
        """
        Returns a stream consisting of the elements of this stream that match the given predicate.
        This is an intermediate operation.

        :param predicate: Predicate to apply to each element to determine if it should be included
        :return: The new stream
        """
        self.__pipe = self.__pipe.filter(predicate)
        return self

    def peek(self, action: Callable[[_AT], Any]) -> "ParallelStream[_AT]":
        """
        Returns a stream consisting of the elements of this stream, additionally performing the provided action on each
        element as elements are consumed from the resulting stream.
        This is an intermediate operation.

        :param action: An action to perform on the elements as they are consumed from the stream
        :return: the new stream
        """
        return self.map(mapper=partial(_with_action, action=action))

    def reduce(self, reducer: Callable[[_AT, _AT], _AT]) -> _AT:
        """
        Performs a reduction on the elements of this stream, using provided associative
        accumulation function, and returns the reduced value.

        :param reducer: Function for combining two values
        :return: The result of the reduction
        """
        with Pool(processes=self.__n_processes) as pool:
            return utils.fold(
                self.__iterator_pipe(pool),
                partial(_reducer, reducer=reducer),
                pool,
                self.__chunk_size,
            )

    def for_each(self, action: Callable[[_AT], Any]) -> None:
        """
        Performs an action for each element of this stream.
        This is terminal operation.

        :param action: An action to perform on the elements
        """
        for _ in self.map(action).iterator():
            pass

    def sequential(self) -> "stream.SequentialStream[_AT]":
        """
        :return: Sequential Stream. All ops applied on parallel stream still parallel.
        """
        return stream.SequentialStream(self.iterator())

    def collect(self, collector: "collectors.Collector[_AT, _RT]") -> _RT:
        """
        Collects the stream using supplied collector.
        This is terminal operation.

        :param collector:  Collector instance
        :return: The result of collector.collect(...)
        """
        with Pool(processes=self.__n_processes) as pool:
            return collector.collect(
                stream.SequentialStream(self.__iterator_pipe(pool))
            )


class ParallelNumberLikeStream(ParallelStream[_NAT]):
    def max(self) -> _NAT:
        """
        :return: Returns a Nullable describing the maximum element of this stream, or an empty Nullable if this stream is empty.
        """
        reducer = cast(
            Callable[[_NAT, _NAT], _NAT], partial(_order_reducer, selector=max)
        )
        return self.reduce(reducer)

    def min(self) -> _NAT:
        """
        :return: Returns a Nullable describing the minimum element of this stream, or an empty Nullable if this stream is empty.
        """
        reducer = cast(
            Callable[[_NAT, _NAT], _NAT], partial(_order_reducer, selector=min)
        )
        return self.reduce(reducer)
