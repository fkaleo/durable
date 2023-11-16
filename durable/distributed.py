from concurrent.futures import Future
import ray
from typing import Any, Callable, Optional
from functools import wraps
from dask.distributed import get_client, fire_and_forget


def distributed(func: Callable) -> Callable[..., Future]:
    """
    Decorator to convert a function into its asynchronous version using Ray.
    """

    @wraps(func)
    def wrapper(*args, **kwargs) -> Future:
        # Initialize Ray if not already done
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True)

        # Convert the function to a Ray remote function
        remote_func = ray.remote(func)

        # Execute the function asynchronously and return a future
        return remote_func.remote(*args, **kwargs).future()

    return wrapper

def ray_to_future(func: Callable[..., ray.ObjectRef]) -> Callable[..., Future]:
    def wrapper(*args, **kwargs) -> Future:
        return func.remote(*args, **kwargs).future()

    return wrapper


def dask_submit(_func: Optional[Callable] = None, **submit_kwargs: Any) -> Callable[[Callable], Callable[..., Future]]:
    def decorator(func) -> Callable[..., Future]:
        @wraps(func)
        def wrapper(*args, **kwargs):
            client = get_client()
            future = client.submit(func, *args, **submit_kwargs, **kwargs)
            fire_and_forget(future)
            return future
        return wrapper

    if _func:
        return decorator(_func)

    return decorator