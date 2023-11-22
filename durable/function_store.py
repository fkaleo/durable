from typing import Any, Callable, Dict, List, Protocol, Tuple


class FunctionCall:
    """
    Represents a single call to a function, including its arguments and keyword arguments.

    This class encapsulates a function call by storing the function, its positional
    arguments, and keyword arguments. It is designed to be used where a record of
    function invocations is needed, such as in caching or logging mechanisms.

    Parameters
    ----------
    func : Callable
        The function that is being called.
    args : Tuple
        The positional arguments with which the function is called.
    kwargs : Dict
        The keyword arguments with which the function is called.

    Attributes
    ----------
    func : Callable
        The function to be called.
    args : Tuple
        The positional arguments for the function.
    kwargs : Dict
        The keyword arguments for the function.
    """
    def __init__(self, func: Callable, args: Tuple, kwargs: Dict):
        self.func = func
        self.args = args
        self.kwargs = kwargs


class ResultStore(Protocol):
    """
    A protocol defining a storage mechanism for function call results and exceptions.

    This protocol specifies methods for storing and retrieving results or exceptions 
    from function calls. Implementations of this protocol can be used in caching, 
    logging, or other systems where tracking the outcomes of function calls is necessary.

    Methods
    -------
    get_function_calls(function_name: str) -> List[Any]
        Retrieve a list of all recorded calls to a specified function.

    get_result(call: FunctionCall) -> Any
        Retrieve the result of a given function call.

    store_result(call: FunctionCall, result: Any) -> None
        Store the result of a function call.

    store_exception(call: FunctionCall, exception: Exception) -> None
        Store an exception raised during a function call.
    """
    def get_function_calls(self, function_name: str) -> List[Any]:
        ...

    def get_result(self, call: FunctionCall) -> Any:
        ...

    def store_result(self, call: FunctionCall, result: Any) -> None:
        ...

    def store_exception(self, call: FunctionCall, exception: Exception) -> None:
        ...