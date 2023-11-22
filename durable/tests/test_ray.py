import pytest

import ray

from ..ray import remote_and_cached, submit
from ..future import FutureProtocol


@pytest.fixture(scope="session", autouse=True)
def ray_setup():
    ray.init()
    yield
    ray.shutdown()


def test_simple_remote_function():

    @submit
    @ray.remote
    def simple_function():
        return 42

    future = simple_function()
    assert isinstance(future, FutureProtocol)
    assert future.result() == 42

def test_function_with_arguments():
    @submit
    @ray.remote
    def add(a, b):
        return a + b

    future = add(10, 32)
    assert isinstance(future, FutureProtocol)
    assert future.result() == 42

def test_error_handling():
    @submit
    @ray.remote
    def failing_function():
        raise ValueError("Expected error")

    future = failing_function()
    with pytest.raises(ValueError, match="Expected error"):
        future.result()

@pytest.fixture
def temp_sqlite_db_url(tmp_path):
    db_path = tmp_path / "sql_cache.db"
    db_url = f"sqlite:///{db_path}"
    yield db_url

def multiply(x: int) -> int:
    return x * x

def test_remote_and_cached_basic(temp_sqlite_db_url):
    multiply_remote = remote_and_cached(num_cpus=0.1, memory=64, cache_db=temp_sqlite_db_url)(multiply)


    assert multiply_remote.__name__ == multiply.__name__
    assert multiply_remote.__doc__ == multiply.__doc__

    result = multiply_remote(4)
    assert result == 16
