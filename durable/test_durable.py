import functools
from concurrent.futures import Future, ThreadPoolExecutor

import cachetools
import pytest


def donothing():
    pass

def donothing_singlearg(n):
    pass

def add(x, y):
    return x + y

def multiply(x, y):
    return x * y

def fibonacci(n):
    """Generate Fibonacci sequence up to n"""
    a, b = 0, 1
    sequence = []
    while a < n:
        sequence.append(a)
        a, b = b, a + b
    return sequence


def is_prime(num):
    if num <= 1:
        return False
    for i in range(2, int(num**0.5) + 1):
        if num % i == 0:
            return False
    return True


def compute_heavy_task(n):
    """ A function that performs a computationally heavy task and returns an integer.
        This function calculates the sum of the first n prime numbers.
    """

    sum_of_primes, count, current_num = 0, 0, 2

    while count < n:
        if is_prime(current_num):
            sum_of_primes += current_num
            count += 1
        current_num += 1

    return sum_of_primes


@pytest.fixture
def durable(tmp_path):
    from . import durable
    durable.DEFAULT_CACHE_STORE_ID = str(tmp_path / "test_cache.db")
    durable.DEFAULT_CALL_STORE_ID = str(tmp_path / "test_call.db")
    yield durable

@pytest.mark.parametrize("args,expected", [((1, 2), 3), ((3, 4), 7), ((5, 6), 11)])
def test_caching(durable, args, expected):
    cached_func = durable.cache(add)

    # First call, result should be computed
    assert cached_func(*args) == expected

    # Second call with same arguments, should return cached result
    assert cached_func(*args) == expected


@pytest.mark.parametrize("kwargs,expected", [({'x': 7, 'y': 8}, 15), ({'y': 8, 'x': 7}, 15)])
def test_cache_key_sensitivity(durable, kwargs, expected):
    cached_func = durable.cache(add)

    # Test with different order of keyword arguments
    assert cached_func(**kwargs) == expected

@pytest.mark.parametrize("cache_type", ['durable', 'functools', 'none', 'cachetools'])
@pytest.mark.parametrize("test_func, args", [(compute_heavy_task, (100,)),
                                             (donothing, ()),
                                             (donothing_singlearg, (100,)),
                                             (fibonacci, (100,)),
                                             ])
def test_benchmark_functions(benchmark, durable, test_func, args, cache_type):
    if cache_type == 'durable':
        func_to_benchmark = durable.cache(test_func)
    elif cache_type == 'functools':
        func_to_benchmark = functools.cache(test_func)
    elif cache_type == 'cachetools':
        func_to_benchmark = cachetools.cached(cache={})(test_func)
    else:
        func_to_benchmark = test_func

    # Benchmarking the function call
    result = benchmark(func_to_benchmark, *args)

    # Verifying the result is as expected
    assert result == test_func(*args)
    if hasattr(func_to_benchmark, "cache_info") and func_to_benchmark.cache_info is not None:
        print(cache_type)
        print(func_to_benchmark.cache_info())