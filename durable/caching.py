import functools
import inspect
import logging
from concurrent.futures import Future
from typing import Any, Callable, Union

from .function_store import FunctionCall, ResultStore
from .future import FutureProtocol, is_future_type


def _get_return_type(func: Callable) -> object:
    return_type = inspect.signature(func).return_annotation

    if return_type is inspect._empty:
        raise TypeError("Function must have a declared return type")

    return return_type


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


def caching_decorator(func: Callable, store: ResultStore) -> Callable:
    """
    Decorator that caches the results of function calls using a specified result store.

    This decorator wraps a function and caches its return values in the provided
    ResultStore. On subsequent calls with the same arguments, the cached result
    is returned instead of executing the function again. The decorator handles 
    both regular and asynchronous (future-based) function calls.

    Parameters
    ----------
    func : Callable
        The function to be wrapped and whose results are to be cached.
    store : ResultStore
        An instance of ResultStore or its implementation, used to store and 
        retrieve cached results.

    Returns
    -------
    Callable
        A wrapped version of `func` that implements caching of its return values.

    Notes
    -----
    - The caching logic uses the arguments of the function call to determine uniqueness.
    - For asynchronous functions (returning a future), the result is stored once
      the future is resolved. Exceptions during execution are also stored.
    - The decorator assumes that `store` implements the methods specified in the 
      ResultStore protocol.

    Examples
    --------
    >>> class MyStore(ResultStore):
    ...     # Implement the required methods
    ...     pass
    ...
    >>> @caching_decorator
    ... def my_function(x):
    ...     return x * x
    ...
    >>> store = MyStore()
    >>> cached_func = caching_decorator(my_function, store)
    >>> cached_func(4)
    16
    >>> # Subsequent calls with 4 will return the cached result 16
    """
    return_type = _get_return_type(func)

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        call = FunctionCall(func, args, kwargs)
        try:
            cached_value = store.get_result(call)
            return _wrap_in_future(return_type, cached_value)
        except KeyError:
            pass

        result = func(*args, **kwargs)

        def on_future_done(future):
            try:
                store.store_result(call, future.result())
            except Exception as exception:
                logging.warning("Exception: %s", exception)
                store.store_exception(call, exception)

        if is_future_type(result):
            # FIXME: on_future_done might be called from a different thread
            # this is the case with Ray
            result.add_done_callback(on_future_done)
        else:
            store.store_result(call, result)

        return result

    return wrapper
