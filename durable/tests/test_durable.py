import functools
import logging
import time
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any
from unittest.mock import create_autospec

import cachetools
import dask.distributed
import pytest
import ray

from ..durable import FutureProtocol


def donothing() -> None:
    pass

def donothing_singlearg(n) -> None:
    pass

def add(x, y) -> Any:
    return x + y

def multiply(x, y) -> Any:
    return x * y

def fibonacci(n) -> int:
    """Generate Fibonacci sequence up to n"""
    a, b = 0, 1
    sequence = []
    while a < n:
        sequence.append(a)
        a, b = b, a + b
    return sequence


def is_prime(num) -> bool:
    if num <= 1:
        return False
    for i in range(2, int(num**0.5) + 1):
        if num % i == 0:
            return False
    return True


def compute_heavy_task(n) -> int:
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
def durable_cache(tmp_path):
    from .. import durable
    durable.DEFAULT_CACHE_STORE_ID = str(tmp_path / "test_cache.db")
    durable.DEFAULT_CALL_STORE_ID = str(tmp_path / "test_call.db")
    yield durable.cache

@pytest.fixture
def functools_cache(tmp_path):
    yield functools.cache

@pytest.mark.parametrize("args,expected", [((1, 2), 3), ((3, 4), 7), ((5, 6), 11)])
def test_caching(durable_cache, args, expected):
    cached_func = durable_cache(add)

    # First call, result should be computed
    assert cached_func(*args) == expected

    # Second call with same arguments, should return cached result
    assert cached_func(*args) == expected


@pytest.mark.parametrize("kwargs,expected", [({'x': 7, 'y': 8}, 15), ({'y': 8, 'x': 7}, 15)])
def test_cache_key_sensitivity(durable_cache, kwargs, expected):
    cached_func = durable_cache(add)

    # Test with different order of keyword arguments
    assert cached_func(**kwargs) == expected


def async_add_fake(x, y) -> Future:
    future = Future()
    future.set_result(add(x, y))
    return future

def async_add_in_thread(x, y) -> Future:
    with ThreadPoolExecutor() as executor:
        future = executor.submit(add, x, y)
        return future

def longer_add(x, y):
    time.sleep(0.1)
    return add(x, y)

def async_longer_add_in_thread(x, y) -> Future:
    with ThreadPoolExecutor() as executor:
        future = executor.submit(longer_add, x, y)
        return future

@pytest.fixture(scope="module", autouse=True)
def dask_client():
    client = dask.distributed.Client()

def async_add_with_dask(x, y) -> dask.distributed.Future:
    client = dask.distributed.get_client()
    future = client.submit(add, x, y)
    return future

def async_add_with_ray(x, y) -> Future:
    ray.init(ignore_reinit_error=True, logging_level=logging.WARNING)
    return ray.remote(add).remote(x, y).future()

@pytest.mark.parametrize("cache_fixture", [
    "durable_cache",
    "functools_cache",
])
@pytest.mark.parametrize("func, args, expected", [
    (add, (10, 4), 14),
    (async_add_fake, (10, 3), 13),
    (async_add_in_thread, (2, 5), 7),
    (async_longer_add_in_thread, (2, 5), 7),
    (async_add_with_dask, (33, 1), 34),
    (async_add_with_ray, (3, 1), 4),
])
def test_cached_with_future(cache_fixture, func, args, expected, request):
    # Mock the original function
    mocked_func = create_autospec(func, side_effect=func)

    # Apply the 'cache' decorator defined by the 'cache_fixture'
    cache = request.getfixturevalue(cache_fixture)
    cached_func = cache(mocked_func)

    result = cached_func(*args)
    future = None

    if isinstance(result, FutureProtocol):
        future = result
        result = future.result()

    assert result == expected
    assert mocked_func.call_count == 1, "Function should be called once"

    if future is not None:
        # Wait for the cache to be filled
        # Some frameworks execute callbacks passed to Future.add_done_callback asynchronously
        # FIXME: do it better
        time.sleep(0.1)

    # Test the cache
    # Call the function again with the same arguments and ensure the cached result is returned
    cached_result = cached_func(*args)

    if isinstance(cached_result, FutureProtocol):
        cached_result = cached_result.result()

    assert cached_result == expected, f"The cached result should be {expected}"
    assert mocked_func.call_count == 1, "Function should not be called again, result should be from cache"


@pytest.mark.parametrize("cache_type", ['durable', 'functools', 'none', 'cachetools'])
@pytest.mark.parametrize("test_func, args", [(compute_heavy_task, (100,)),
                                             (donothing, ()),
                                             (donothing_singlearg, (100,)),
                                             (fibonacci, (100,)),
                                             ])
def test_benchmark_functions(benchmark, durable_cache, test_func, args, cache_type):
    if cache_type == 'durable':
        func_to_benchmark = durable_cache(test_func)
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