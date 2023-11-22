import functools
from threading import Lock, Event
from typing import Callable, Dict, Tuple, Any

def ensure_unique(func: Callable) -> Callable:
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