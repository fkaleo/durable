from typing import Any, Callable, Dict, List, Protocol, Tuple


class FunctionCall:
    def __init__(self, func: Callable, args: Tuple, kwargs: Dict):
        self.func = func
        self.args = args
        self.kwargs = kwargs


class ResultStore(Protocol):
    def get_function_calls(self, function_name: str) -> List[Any]:
        ...

    def get_result(self, call: FunctionCall) -> Any:
        ...

    def store_result(self, call: FunctionCall, result: Any) -> None:
        ...

    def store_exception(self, call: FunctionCall, exception: Exception) -> None:
        ...