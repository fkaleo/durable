import functools
import inspect
import logging
from typing import Callable

from .function_store import FunctionCall, ResultStore
from .future import _wrap_in_future, is_future_type


def get_return_type(func: Callable) -> object:
    return_type = inspect.signature(func).return_annotation

    if return_type is inspect._empty:
        raise TypeError("Function must have a declared return type")

    return return_type


def caching_decorator(func: Callable, store: ResultStore) -> Callable:
    return_type = get_return_type(func)

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