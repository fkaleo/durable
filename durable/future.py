import concurrent.futures
import functools


def result_from_future(func):
    """
    A decorator that wraps a function returning a concurrent.futures.Future object and returns its result.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        future = func(*args, **kwargs)
        if not isinstance(future, concurrent.futures.Future):
            raise TypeError("The wrapped function must return a concurrent.futures.Future object")
        return future.result()

    return wrapper