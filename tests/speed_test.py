import math
from time import time

from pystream.infrastructure.collectors import to_collection
from pystream import ParallelStream
from pystream import SequentialStream


def _filter(x) -> bool:
    return x % 3 == 0


if __name__ == '__main__':
    collection = tuple(range(10_000, 10_100))
    times = []

    print("Parallel:")
    for i in range(100):
        t_s = time()
        ParallelStream(collection) \
            .filter(_filter) \
            .map(math.factorial) \
            .collect(collector=to_collection(tuple))
        t_elapsed = time() - t_s
        times.append(t_elapsed)
        print(f"Time elapsed on {i}'ths experiment:", t_elapsed)
    t_parallel = sum(times) / len(times)

    times = []
    print("Sequential:")
    for i in range(100):
        t_s = time()
        SequentialStream(collection) \
            .filter(_filter) \
            .map(math.factorial) \
            .collect(collector=to_collection(tuple))
        t_elapsed = time() - t_s
        times.append(t_elapsed)
        print(f"Time elapsed on {i}'ths experiment:", t_elapsed)
    t_seq = sum(times) / len(times)

    print("Avg parallel: ", t_parallel)
    print("Avg sequential: ", t_seq)
    print("Time elapsed Sequential/Parallel", t_seq / t_parallel)
