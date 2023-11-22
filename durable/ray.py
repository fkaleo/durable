import functools
from functools import wraps
from typing import Callable

import ray

from .cache_sql import cache
from .future import result_from_future, FutureProtocol
from .unique import ensure_unique


def submit(func: Callable) -> Callable[..., FutureProtocol]:
    """
    Decorator to convert a Ray remote function to return a FutureProtocol object.
    
    Args:
        func (Callable): The Ray remote function to be wrapped.

    Returns:
        Callable[..., FutureProtocol]: A function that, when called, returns a FutureProtocol object.
    """

    @wraps(func)
    def wrapper(*args, **kwargs) -> FutureProtocol:
        object_ref = func.remote(*args, **kwargs)

        # note that .future() comes from https://github.com/ray-project/ray/pull/15425
        return object_ref.future()

    return wrapper


def remote_and_cached(num_cpus: float, memory: float, cache_db: str, num_gpus: float = 0.0) -> Callable:
    """
    A decorator factory for modifying a function to enable remote execution and caching.

    Parameters
    ----------
    num_cpus : float
        Number of CPUs to allocate for the remote function.
    memory : float
        Amount of memory (in bytes) to allocate for the remote function.
    cache_db : str
        Connection string for the database to use for caching.
    num_gpus : float, optional
        Number of GPUs to allocate for the remote function (default is 0.0).

    Returns
    -------
    Callable
        A decorator that, when applied to a function, modifies it for remote execution and caching.

    """

    def wrapper(func):

        @ensure_unique
        @result_from_future
        @cache(cache_db)
        @submit
        @functools.wraps(func)
        @ray.remote(num_cpus=num_cpus, memory=memory, num_gpus=num_gpus)
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        return wrapper

    return wrapper
