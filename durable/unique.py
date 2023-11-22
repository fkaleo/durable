import functools
from threading import Lock, Event
from typing import Callable, Dict, Tuple, Any

def ensure_unique(func: Callable) -> Callable:
    """
    Decorator that ensures a function with given arguments runs only once concurrently.
    
    This decorator wraps a function to ensure that if it's called multiple times 
    with the same arguments, only the first call will actually execute the function. 
    Subsequent calls with the same arguments will wait for the first call to complete 
    and return its result. This behavior is particularly useful for avoiding redundant 
    computations or operations, especially in a multithreaded environment.

    Parameters
    ----------
    func : Callable
        The function to be wrapped by the decorator. This function can be any 
        callable with any set of arguments.

    Returns
    -------
    Callable
        A wrapped version of `func` that ensures uniqueness of execution based 
        on its arguments.

    Examples
    --------
    >>> @ensure_unique
    ... def example_function(x):
    ...     # some expensive computation or I/O operation
    ...     return x * x
    ...
    >>> example_function(2)  # This call will execute the function.
    4
    >>> example_function(2)  # This call will return the result from the first call.
    4
    """
    running_functions: Dict[Tuple[Callable, Tuple], Tuple[Event, Any]] = {}
    lock = Lock()

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        key = (func, args + tuple(sorted(kwargs.items())))
        first_call = False

        with lock:
            if key not in running_functions:
                # Create a new event and placeholder for the result
                event = Event()
                running_functions[key] = (event, None)
                first_call = True
            else:
                # FIXME: maybe the result is already available?
                event, _ = running_functions[key]

        if first_call:
            # This is the first thread to call the function with these arguments
            result = func(*args, **kwargs)
            # Update the result and signal completion
            running_functions[key] = (event, result)
            event.set()
        else:                
            event.wait()
            with lock:
                event, result = running_functions[key]

        return result

    return wrapper