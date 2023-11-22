import pytest
import concurrent.futures
from ..future import result_from_future

# Mock functions to test the decorator
def future_returning_function():
    future = concurrent.futures.Future()
    future.set_result(42)
    return future

def non_future_returning_function():
    return 42

def test_future_returning_function():
    decorated_function = result_from_future(future_returning_function)
    assert decorated_function() == 42

def test_non_future_returning_function():
    decorated_function = result_from_future(non_future_returning_function)
    with pytest.raises(TypeError) as exc_info:
        decorated_function()
    assert str(exc_info.value) == "The wrapped function must return a concurrent.futures.Future object"

def test_exception_in_future():
    def exception_raising_function():
        future = concurrent.futures.Future()
        future.set_exception(ValueError("An error occurred"))
        return future

    decorated_function = result_from_future(exception_raising_function)
    with pytest.raises(ValueError) as exc_info:
        decorated_function()
    assert str(exc_info.value) == "An error occurred"

def test_metadata_preservation():
    @result_from_future
    def sample_function():
        """Sample docstring"""
        future = concurrent.futures.Future()
        future.set_result(None)
        return future

    assert sample_function.__name__ == "sample_function"
    assert sample_function.__doc__ == "Sample docstring"
