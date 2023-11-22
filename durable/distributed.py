from functools import wraps
from typing import Any, Callable, Optional, Union

import ray
from dask.distributed import fire_and_forget, get_client

from .durable import FutureProtocol


def ray_to_future(func: Callable[..., ray.ObjectRef]) -> Callable[..., FutureProtocol]:
    def wrapper(*args, **kwargs) -> FutureProtocol:
        object_ref = func.remote(*args, **kwargs)

        # make object_ref compatible with FutureProtocol
        # note that .future() comes from https://github.com/ray-project/ray/pull/15425
        future = object_ref.future()
        object_ref.add_done_callback = future.add_done_callback
        object_ref.result = future.result
        return object_ref

    return wrapper

# same idea as https://github.com/dask/distributed/pull/7936
def dask_submit(_func: Optional[Callable] = None, **submit_kwargs: Any) -> Union[Callable[[Callable[..., Any]], Callable[..., FutureProtocol]],
                                                                                 Callable[..., FutureProtocol]]:
    def decorator(func: Callable[..., Any]) -> Callable[..., FutureProtocol]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> FutureProtocol:
            client = get_client()
            future = client.submit(func, *args, **submit_kwargs, **kwargs)
            fire_and_forget(future)
            return future
        return wrapper

    if _func:
        return decorator(_func)

    return decorator