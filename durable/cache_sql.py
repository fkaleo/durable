import functools
import pickle
import sqlite3
from typing import Any, Callable, Dict, List, Optional, Tuple

from .durable import (FunctionCall, ResultStore, _make_key, caching_decorator,
                      key_for_function_call)


class SQLResultStore(ResultStore):
    def __init__(self, connection_string: str,
                        create_table_sql: Optional[str] = None, 
                        select_sql: Optional[str] = None, 
                        insert_sql: Optional[str] = None,
                        key_func: Optional[Callable[[Callable, Tuple, Dict], str]] = None):
        self.connection = sqlite3.connect(connection_string)

        if create_table_sql is None:
            create_table_sql = """
                CREATE TABLE IF NOT EXISTS cache (
                    function TEXT,
                    args TEXT,
                    result BLOB,
                    PRIMARY KEY (function, args)
                )
            """

        if select_sql is None:
            select_sql = "SELECT result FROM cache WHERE function = ? AND args = ?"

        if insert_sql is None:
            insert_sql = "INSERT INTO cache (function, args, result) VALUES (?, ?, ?)"

        if key_func is None:
            key_func = key_for_function_call

        self.create_table_sql = create_table_sql
        self._create_table()
        self.select_sql = select_sql
        self.insert_sql = insert_sql
        self.key_func = key_func

    def __del__(self):
        self.connection.close()

    def _create_table(self):
        cursor = self.connection.cursor()
        cursor.execute(self.create_table_sql)
        self.connection.commit()

    def get_results(self, function_name: str) -> List[Any]:
        cursor = self.connection.cursor()
        select_sql = "SELECT function, args, result FROM cache WHERE function = ?"
        cursor.execute(select_sql, (function_name,))
        for result in cursor.fetchall():
            yield result[0], result[1], pickle.loads(result[2])


    def get_result(self, call: FunctionCall) -> Any:
        function_name = call.func.__name__
        args_key = str(_make_key(call.args, kwds=call.kwargs, typed=False))

        cursor = self.connection.cursor()
        cursor.execute(self.select_sql + " LIMIT 1", (function_name, args_key))
        cached_result = cursor.fetchone()

        if cached_result is not None:
            # Deserialize and return the cached result
            return pickle.loads(cached_result[0])
        else:
            raise KeyError()

    def store_result(self, call: FunctionCall, result: Any) -> None:
        function_name = call.func.__name__
        args_key = str(_make_key(call.args, kwds=call.kwargs, typed=False))

        serialized_result = pickle.dumps(result)
        cursor = self.connection.cursor()
        cursor.execute(self.insert_sql, (function_name, args_key, serialized_result))
        self.connection.commit()

    def store_exception(self, call: FunctionCall, exception: Exception) -> None:
        pass


def sql_cached(connection_string: str,
               create_table_sql: Optional[str] = None, 
               select_sql: Optional[str] = None, 
               insert_sql: Optional[str] = None,
               key_func: Optional[Callable[[Callable, Tuple, Dict], str]] = None) -> Callable:
    """
    A decorator that caches the results of a function in a database.
    Allows customization of the SQL statements and the key generation logic.
    """
    store = SQLResultStore(connection_string, create_table_sql, select_sql, insert_sql, key_func)
    return functools.partial(caching_decorator, store=store)