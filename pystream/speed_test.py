from time import time

from pystream.infrastructure import pipe
from pystream.infrastructure.collectors import to_collection
from pystream.parallel_stream import ParallelStream


def _filter(x):
    return x % 3 == 0


def _map(x):
    return x ** 2


if __name__ == '__main__':
    collection = tuple(range(100_000))
    times = []
    for i in range(100):
        t_s = time()
        ParallelStream(collection) \
            .filter(_filter) \
            .map(_map) \
            .collect(collector=to_collection(tuple), chunk_size=2500)
        t_elapsed = time() - t_s
        times.append(t_elapsed)
        print(f"Time elapsed on {i}'ths experiment:", t_elapsed)
    print("Average time: ", sum(times) / len(times))
