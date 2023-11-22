from functools import wraps
from typing import Any, Callable, Optional, Union

from .future import FutureProtocol


# same idea as https://github.com/dask/distributed/pull/7936
def submit(_func: Optional[Callable] = None, **submit_kwargs: Any) -> Union[Callable[[Callable[..., Any]], Callable[..., FutureProtocol]],
                                                                                 Callable[..., FutureProtocol]]:
    from dask.distributed import fire_and_forget, get_client

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