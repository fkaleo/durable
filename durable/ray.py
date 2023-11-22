from functools import wraps
from typing import Callable

from .durable import FutureProtocol


def submit(func: Callable) -> Callable[..., FutureProtocol]:

    @wraps(func)
    def wrapper(*args, **kwargs) -> FutureProtocol:
        object_ref = func.remote(*args, **kwargs)

        # note that .future() comes from https://github.com/ray-project/ray/pull/15425
        return object_ref.future()

    return wrapper
