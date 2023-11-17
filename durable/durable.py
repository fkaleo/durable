# from speedict import Rdict
import functools
import hashlib
import inspect
from concurrent.futures import Future
import pickle
from typing import (KT, Any, Callable, Dict, ItemsView, Mapping, MutableMapping, Optional,
                    Protocol, Tuple, Type, Union, VT_co, runtime_checkable)

from rocksdict import AccessType, Rdict

class _HashedSeq(list):
    """ This class guarantees that hash() will be called no more than once
        per element.  This is important because the lru_cache() will hash
        the key multiple times on a cache miss.

    """

    __slots__ = 'hashvalue'

    def __init__(self, tup, hash=hash):
        self[:] = tup
        self.hashvalue = hash(tup)

    def __hash__(self):
        return self.hashvalue

def _make_key(args, kwds, typed,
             kwd_mark = (object(),),
             fasttypes = {int, str},
             tuple=tuple, type=type, len=len):
    """Make a cache key from optionally typed positional and keyword arguments

    The key is constructed in a way that is flat as possible rather than
    as a nested structure that would take more memory.

    If there is only a single argument and its data type is known to cache
    its hash value, then that argument is returned without a wrapper.  This
    saves space and improves lookup speed.

    """
    # All of code below relies on kwds preserving the order input by the user.
    # Formerly, we sorted() the kwds before looping.  The new way is *much*
    # faster; however, it means that f(x=1, y=2) will now be treated as a
    # distinct call from f(y=2, x=1) which will be cached separately.
    key = args
    if kwds:
        key += kwd_mark
        for item in kwds.items():
            key += item
    if typed:
        key += tuple(type(v) for v in args)
        if kwds:
            key += tuple(type(v) for v in kwds.values())
    elif len(key) == 1 and type(key[0]) in fasttypes:
        return key[0]
    return _HashedSeq(key)


class SortedItems(Protocol[KT, VT_co]):
    """
    A protocol representing a sorted collection of key-value pairs.

    This protocol expects implementing classes to provide an `items` method
    that returns an ItemsView, allowing iteration over key-value pairs
    starting from a specified key.

    
    Methods
    -------
    items(from_key: str) -> ItemsView[KT, VT_co]
        Returns an ItemsView of key-value pairs, starting from `from_key`.
    """
    def items(from_key: str) -> ItemsView[KT, VT_co]: ...

def keys_with_prefix(store: SortedItems, prefix: str):
    """
    Generate key-value pairs from the store where keys start with the given prefix.

    This function iterates over key-value pairs in the store, starting from
    the key that matches the given prefix. Iteration stops when a key is encountered
    that does not start with the prefix.

    Parameters
    ----------
    store : SortedItems
        An instance of SortedItems to retrieve key-value pairs from.
    prefix : str
        The prefix to match keys against.

    Yields
    ------
    Iterator[tuple[KT, VT_co]]
        An iterator over key-value pairs where the keys start with `prefix`.

    Examples
    --------
    >>> class MySortedItems(SortedItems):
    ...     def items(self, from_key: str):
    ...         # example implementation
    ...         yield from [("apple", 1), ("banana", 2), ("cherry", 3)]
    ...
    >>> store = MySortedItems()
    >>> list(keys_with_prefix(store, "b"))
    [('banana', 2)]
    """
    for key, value in store.items(from_key=prefix):
        if key.startswith(prefix):
            yield key, value
        else:
            break

# Protocol @observe requires:
# add_call_pending(func, *args, **kwargs)
# is_call_pending(func, *args, **kwargs)

# Protocol @cache requires:
# set_call_result(func, *args, **kwargs)

@runtime_checkable
class FutureProtocol(Protocol):

    def add_done_callback(self, fn):
        ...

    def result(self, timeout=None):
        ...

    # def set_result(self, result: Any) -> None:
    #     ...

PENDING = None

def is_future_type(type_: Type) -> bool:
    return isinstance(type_, FutureProtocol)

def return_value_for_func(func: Callable, return_value: Any) -> Union[FutureProtocol, Any]:
    # Determine if the function is supposed to return a Future
    return_type = inspect.signature(func).return_annotation

    # Check if the return type is declared
    if return_type is inspect._empty:
        raise TypeError("Function must have a declared return type")

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


class ResultStore(Protocol):
    def get_result(key: str) -> Any:
        ...

    def store_result(key: str, result: Any) -> None:
        ...
    
    def store_exception(key: str, exception: Exception) -> None:
        ...

def caching_decorator(func: Callable, key_func: Callable, store: ResultStore) -> Callable:
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        key = key_func(func, args, kwargs)
        try:
            cached_value = store.get_result(key)
            return return_value_for_func(func, cached_value)
        except KeyError:
            pass

        result = func(*args, **kwargs)

        def on_future_done(future):
            try:
                store.store_result(key, future.result())
            except Exception as exception:
                store.store_exception(key, exception)

        if is_future_type(result):
            result.add_done_callback(on_future_done)
        else:
            store.store_result(key, result)

        return result

    return wrapper

def cached(cache: MutableMapping, key_func: Optional[Callable[[Callable, Tuple, Dict], str]] = None) -> Callable:
    class DictResultStore(ResultStore):
        def __init__(self, cache: MutableMapping):
            self.cache = cache

        def get_result(self, key):
            return self.cache[key]

        def store_result(self, key, result):
            self.cache[key] = result

        def store_exception(self, key, exception):
            pass

    return functools.partial(caching_decorator, key_func=key_func, store=DictResultStore(cache))

def observed(cache: MutableMapping, key: Callable[..., Any]) -> Callable:
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            k = key(*args, **kwargs)
            # FIXME: should do that transactionally
            cache[k] = PENDING
            return func(*args, **kwargs)

        return wrapper

    return decorator

_stores: Mapping[str, MutableMapping] = {}

DEFAULT_CACHE_STORE_ID = "cache.db"
DEFAULT_CALL_STORE_ID = "calls.db"

def get_store(store_id, access_type: AccessType = AccessType.read_only()):
    store = _stores.get(store_id)
    if not store:
        store = Rdict(store_id, access_type=access_type)
        _stores[store_id] = store

    # FIXME: should close RDict upon deletion; make it a context manager?
    return store

def path_from_func(func: Callable):
    return f"/{func.__name__}/"

def _make_key_hash(args, kwds, typed=False):
    key_data = (args, frozenset(kwds.items()))
    key = hashlib.md5(pickle.dumps(key_data)).hexdigest()
    return key

def key_for_function_call(func: Callable, args: Tuple, kwargs: Dict):
    argskey = _make_key(args, kwds=kwargs, typed=False)
    # argskey = _make_key_hash(args, kwds=kwargs, typed=False)
    # argskey = f"{str(args)}_{str(kwargs)}"
    return f"{path_from_func(func)}{argskey}"

def cache(func: Callable, store_id: str = None) -> Callable:
    if not store_id:
        store_id = DEFAULT_CACHE_STORE_ID
    store = get_store(store_id, AccessType.read_write())
    return cached(cache=store, key_func=key_for_function_call)(func)

def observe(func: Callable, store_id: str = None) -> Callable:
    if not store_id:
        store_id = DEFAULT_CALL_STORE_ID
    store = get_store(store_id, AccessType.read_write())
    return observed(cache=store, key=key_for_function_call)(func)
