import concurrent.futures
import functools

from typing import Protocol, Type, runtime_checkable


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


@runtime_checkable
class FutureProtocol(Protocol):

    def add_done_callback(self, fn):
        ...

    def result(self, timeout=None):
        ...

    # def set_result(self, result: Any) -> None:
    #     ...


def is_future_type(type_: Type) -> bool:
    return isinstance(type_, FutureProtocol)