import functools
import pickle
from typing import Any, Callable, List, Optional

from sqlalchemy import create_engine, text

from .cache_rocksdb import caching_decorator # FIXME
from .function_store import FunctionCall, ResultStore


class SQLAlchemyResultStore(ResultStore):
    def __init__(self, connection_string: str,
                 create_table_sql: Optional[str] = None,
                 select_sql: Optional[str] = None,
                insert_sql: Optional[str] = None):
        self.connection_string = connection_string
        self.engine = None

        if create_table_sql is None:
            create_table_sql = """
                CREATE TABLE IF NOT EXISTS function_calls (
                    function TEXT,
                    args BLOB,
                    result BLOB,
                    PRIMARY KEY (function, args)
                )
            """

        if select_sql is None:
            select_sql = "SELECT result FROM function_calls WHERE function = :function AND args = :args"

        if insert_sql is None:
            insert_sql = "INSERT INTO function_calls (function, args, result) VALUES (:function, :args, :result)"

        self.create_table_sql = create_table_sql
        self._create_table()
        self.select_sql = select_sql
        self.insert_sql = insert_sql

    def _ensure_engine(self):
        if self.engine is None:
            self.engine = create_engine(self.connection_string, echo=False, connect_args={"check_same_thread": False})

    def _create_table(self):
        self._ensure_engine()
        with self.engine.connect() as connection:
            connection.execute(text(self.create_table_sql))
            connection.commit()

    def get_function_calls(self, function_name: str) -> List[Any]:
        self._ensure_engine()
        with self.engine.connect() as connection:
            select_sql = "SELECT function, args, result FROM function_calls WHERE function = :function"
            result = connection.execute(text(select_sql), {"function": function_name})
            for row in result:
                yield row[0], pickle.loads(row[1]), pickle.loads(row[2])

    def get_result(self, call: FunctionCall) -> Any:
        self._ensure_engine()
        function_name = call.func.__name__
        serialized_args = pickle.dumps({"args": call.args, "kwargs": call.kwargs})

        with self.engine.connect() as connection:
            result = connection.execute(text(self.select_sql), {"function": function_name, "args": serialized_args}).fetchone()
            if result is not None:
                return pickle.loads(result[0])
            else:
                raise KeyError()

    def store_result(self, call: FunctionCall, result: Any) -> None:
        self._ensure_engine()
        function_name = call.func.__name__
        serialized_args = pickle.dumps({"args": call.args, "kwargs": call.kwargs})
        serialized_result = pickle.dumps(result)

        with self.engine.connect() as connection:
            connection.execute(text(self.insert_sql), {"function": function_name, "args": serialized_args, "result": serialized_result})
            connection.commit()

    def store_exception(self, call: FunctionCall, exception: Exception) -> None:
        pass


def sql_cached(connection_string: str,
               create_table_sql: Optional[str] = None, 
               select_sql: Optional[str] = None, 
               insert_sql: Optional[str] = None) -> Callable:
    """
    A decorator that caches the results of a function in a database.
    Allows customization of the SQL statements and the key generation logic.
    """
    store = SQLAlchemyResultStore(connection_string, create_table_sql, select_sql, insert_sql)
    return functools.partial(caching_decorator, store=store)