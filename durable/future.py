import concurrent.futures
from concurrent.futures import Future
import functools

from typing import Any, Protocol, Type, Union, runtime_checkable


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


def _wrap_in_future(return_type: object, return_value: Any) -> Union[FutureProtocol, Any]:
    # If the function is supposed to return a Future, wrap the cached value
    if is_future_type(return_type):
        # FIXME: Unfortunately we cannot always reconstruct the original future type
        # For example dask's distributed.Future cannot be instantiated simply
        # We use concurrent.futures.Future instead
        future = Future()
        future.set_result(return_value)
        return future
    else:
        return return_value