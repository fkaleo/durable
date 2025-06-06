import functools
from inspect import isclass
import logging
import time
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any
from unittest.mock import create_autospec

import cachetools
import pytest

from ..future import FutureProtocol


def donothing() -> None:
    pass

def donothing_singlearg(n) -> None:
    pass

def add(x, y) -> Any:
    return x + y

def multiply(x, y) -> Any:
    return x * y

def add_without_return_hint(x, y):
    return x + y

class NoStringRepresentation:
    __str__ = None
    __repr__ = None

    def __init__(self, value) -> None:
        self.value = value

    def __hash__(self) -> int:
        return hash(self.value)

    def __eq__(self, other: "NoStringRepresentation") -> bool:
        return self.value == other.value

def no_string_representation(arg: NoStringRepresentation) -> NoStringRepresentation:
    return NoStringRepresentation(arg.value)

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

@pytest.fixture(scope="module", autouse=False)
def dask_client():
    import dask.distributed
    client = dask.distributed.Client()

def async_add_with_dask(x, y) -> 'dask.distributed.Future':
    import dask.distributed
    client = dask.distributed.get_client()
    future = client.submit(add, x, y)
    return future

def async_add_with_ray(x, y) -> Future:
    import ray
    ray.init(ignore_reinit_error=True, logging_level=logging.WARNING)
    return ray.remote(add).remote(x, y).future()


@pytest.fixture
def cache_rocksdb(tmp_path):
    from .. import cache_rocksdb
    cache_rocksdb.DEFAULT_CACHE_STORE_ID = str(tmp_path / "test_cache.db")
    cache_rocksdb.DEFAULT_CALL_STORE_ID = str(tmp_path / "test_call.db")
    yield cache_rocksdb.cache

@pytest.fixture
def functools_cache():
    yield functools.cache

@pytest.fixture
def connection_string(tmp_path):
    connection_string = "sqlite:///" + str(tmp_path / "test.db")
    yield connection_string

@pytest.fixture
def cache_sql(connection_string):
    from ..cache_sql import cache
    yield cache(connection_string)


@pytest.fixture(params=[
    "cache_rocksdb",
    # "functools_cache",
    "cache_sql",
])
def cache(request):
    cache = request.getfixturevalue(request.param)
    yield cache


@pytest.fixture
def observe_rocksdb(tmp_path):
    from .. import cache_rocksdb
    cache_rocksdb.DEFAULT_CALL_STORE_ID = str(tmp_path / "test_observe.db")
    yield cache_rocksdb.observe


@pytest.fixture(params=[
    "observe_rocksdb",
    # Add other observe implementations here in the future
])
def observe(request):
    observe_func = request.getfixturevalue(request.param)
    yield observe_func

@pytest.mark.parametrize("args,expected", [((1, 2), 3), ((3, 4), 7), ((5, 6), 11)])
def test_caching(cache, args, expected):
    cached_func = cache(add)

    # First call, result should be computed
    assert cached_func(*args) == expected

    # Second call with same arguments, should return cached result
    assert cached_func(*args) == expected


@pytest.mark.parametrize("kwargs,expected", [({'x': 7, 'y': 8}, 15), ({'y': 8, 'x': 7}, 15)])
def test_cache_key_sensitivity(cache, kwargs, expected):
    cached_func = cache(add)

    # Test with different order of keyword arguments
    assert cached_func(**kwargs) == expected


@pytest.mark.parametrize("args,expected", [((10, 20), 30), ((30, 40), 70)])
def test_observe_decorator(observe, cache, args, expected):
    """Test that the observe decorator works correctly after the fix.
    
    This test verifies that the observe decorator can be applied without errors
    and properly marks function calls as pending in the store.
    """
    # Store the call count
    call_count = 0
    
    # Define a function to be decorated
    @cache
    @observe
    def observed_function(x, y) -> int:
        nonlocal call_count
        call_count += 1
        return x + y
    
    # First call with these arguments
    result = observed_function(*args)
    assert result == expected
    assert call_count == 1
    
    # Call again with the same arguments - should use cache
    result = observed_function(*args)
    assert result == expected
    # Call count should not increase because of caching
    assert call_count == 1


@pytest.mark.parametrize("func, args, expected", [
    (add, (10, 4), 14),
    (no_string_representation, (NoStringRepresentation(42),), NoStringRepresentation(42)),
    (add_without_return_hint, (10, 4), TypeError),
    (async_add_fake, (10, 3), 13),
    (async_add_in_thread, (2, 5), 7),
    (async_longer_add_in_thread, (2, 5), 7),
    # (async_add_with_dask, (33, 1), 34),
    (async_add_with_ray, (3, 1), 4),
])
def test_cached_with_future(cache, func, args, expected):
    # Mock the original function
    mocked_func = create_autospec(func, side_effect=func)

    if isclass(expected) and issubclass(expected, Exception):
        with pytest.raises(expected):
            cached_func = cache(mocked_func)
        return

    # Apply the 'cache' decorator
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


@pytest.mark.parametrize("cache_type", ['cache_rocksdb', 'functools', 'none', 'cachetools'])
@pytest.mark.parametrize("test_func, args", [(compute_heavy_task, (100,)),
                                             (donothing, ()),
                                             (donothing_singlearg, (100,)),
                                             (fibonacci, (100,)),
                                             ])
def test_benchmark_functions(benchmark, cache_rocksdb, test_func, args, cache_type):
    if cache_type == 'cache_rocksdb':
        func_to_benchmark = cache_rocksdb(test_func)
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