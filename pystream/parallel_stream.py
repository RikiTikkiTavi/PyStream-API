from functools import partial, reduce
from itertools import chain, islice, tee
from multiprocessing.pool import Pool
from multiprocessing import cpu_count
from typing import Generic, TypeVar, Callable, List, Iterable, Tuple, Any
import pystream.infrastructure.utils as utils
import pystream.stream as stream
import pystream.infrastructure.pipe as pipe

_AT = TypeVar('_AT')
_RT = TypeVar('_RT')


def _reducer(pair: Tuple[_AT, ...], /, reducer: Callable[[_AT, _AT], _AT]) -> _AT:
    return reducer(*pair) if len(pair) == 2 else pair[0]


def _order_reducer(*args: Tuple[_AT, ...], selector: Callable[[Tuple[_AT, ...]], _AT]) -> _AT:
    return selector(args)


def _with_action(x: _AT, action: Callable[[_AT], Any]):
    action(x)
    return x


class ParallelStream(Generic[_AT]):
    __n_processes: int
    __pipe: pipe.Pipe[_AT]
    __iterable: Iterable[_AT]

    def __init__(self, *iterables: Iterable[_AT], n_processes: int = cpu_count()):
        self.__iterable = chain(*iterables)
        self.__n_processes = n_processes
        self.__pipe = pipe.Pipe()

    # ------------------------------------------------------------------------------
    # Backends
    # ------------------------------------------------------------------------------

    def __reduce(
            self,
            iterable: Iterable[_AT],
            /,
            reducer: Callable[[_AT, _AT], _AT],
            pool: Pool,
            chunk_size: int = 1
    ) -> _AT:
        """
        Lazy reduce implementation
        """
        while True:
            iterable = pool.imap(
                func=partial(_reducer, reducer=reducer),
                iterable=utils.reduction_pairs_generator(iterable)
            )
            first_pair = tuple(islice(iterable, 2))
            if len(first_pair) == 1: return first_pair[0]
            iterable = chain(first_pair, iterable)

    def __unwrap_pipe(self, iterable: Iterable[pipe.MaybeEmpty]) -> Iterable:
        for m in iterable:
            if m.is_empty():
                continue
            yield m.get_x()

    def __iterator_pipe(self, pool: Pool, chunk_size: int = 1):
        return self.__unwrap_pipe(pool.imap(self.__pipe.get_operation(), self.__iterable, chunksize=chunk_size))

    # ------------------------------------------------------------------------------
    # Frontends
    # ------------------------------------------------------------------------------

    def map(
            self,
            mapper: Callable[[_AT], _RT]
    ) -> 'ParallelStream[_RT]':
        self.__pipe = self.__pipe.map(mapper)
        return self

    def filter(
            self,
            predicate: Callable[[_AT], bool]
    ) -> 'ParallelStream[_AT]':
        self.__pipe = self.__pipe.filter(predicate)
        return self

    def reduce(self, reducer: Callable[[_AT, _AT], _AT], chunk_size: int = 1) -> _AT:
        with Pool(processes=self.__n_processes) as pool:
            return self.__reduce(self.__iterator_pipe(pool, chunk_size), reducer, pool, chunk_size)

    def max(self):
        return self.reduce(partial(_order_reducer, selector=max))

    def min(self):
        return self.reduce(partial(_order_reducer, selector=min))

    def for_each(self, action: Callable[[_AT], Any]) -> None:
        for _ in self.map(action).iterator(): pass

    def peek(self, action: Callable[[_AT], Any], lazy: bool = True, chunk_size: int = 1) -> "ParallelStream[_AT]":
        return self.map(mapper=partial(_with_action, action=action))

    def iterator(self) -> Iterable[_AT]:
        with Pool(processes=self.__n_processes) as pool:
            for element in self.__iterator_pipe(pool): yield element

    def sequential(self) -> 'stream.Stream[_AT]':
        return stream.Stream(self.iterator())

    def collect(self, collector: 'Collector[_AT, _RT]', chunk_size: int = 1) -> _RT:
        with Pool(processes=self.__n_processes) as pool:
            return collector.collect(self.__iterator_pipe(pool, chunk_size))
