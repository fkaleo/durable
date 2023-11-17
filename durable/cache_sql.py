import functools
import pickle
import sqlite3
from typing import Any, Callable, List, Optional

from sqlalchemy import create_engine, text

from .durable import FunctionCall, ResultStore, caching_decorator


class SQLiteResultStore(ResultStore):
    def __init__(self, connection_string: str,
                        create_table_sql: Optional[str] = None, 
                        select_sql: Optional[str] = None, 
                        insert_sql: Optional[str] = None):
        self.connection = sqlite3.connect(connection_string)

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
            select_sql = "SELECT result FROM function_calls WHERE function = ? AND args = ?"

        if insert_sql is None:
            insert_sql = "INSERT INTO function_calls (function, args, result) VALUES (?, ?, ?)"

        self.create_table_sql = create_table_sql
        self._create_table()
        self.select_sql = select_sql
        self.insert_sql = insert_sql

    def __del__(self):
        self.connection.close()

    def _create_table(self):
        cursor = self.connection.cursor()
        cursor.execute(self.create_table_sql)
        self.connection.commit()

    def get_function_calls(self, function_name: str) -> List[Any]:
        cursor = self.connection.cursor()
        select_sql = "SELECT function, args, result FROM function_calls WHERE function = ?"
        cursor.execute(select_sql, (function_name,))
        for result in cursor.fetchall():
            yield result[0], pickle.loads(result[1]), pickle.loads(result[2])

    def get_result(self, call: FunctionCall) -> Any:
        function_name = call.func.__name__
        serialized_args = pickle.dumps({"args": call.args, "kwargs": call.kwargs})

        cursor = self.connection.cursor()
        cursor.execute(self.select_sql, (function_name, serialized_args))
        cached_result = cursor.fetchone()
        if cached_result is not None:
            # Deserialize and return the cached result
            return pickle.loads(cached_result[0])
        else:
            raise KeyError()

    def store_result(self, call: FunctionCall, result: Any) -> None:
        function_name = call.func.__name__
        serialized_args = pickle.dumps({"args": call.args, "kwargs": call.kwargs})

        serialized_result = pickle.dumps(result)
        cursor = self.connection.cursor()
        cursor.execute(self.insert_sql, (function_name, serialized_args, serialized_result))
        self.connection.commit()

    def store_exception(self, call: FunctionCall, exception: Exception) -> None:
        pass


class SQLAlchemyResultStore(ResultStore):
    def __init__(self, connection_string: str,
                 create_table_sql: Optional[str] = None,
                 select_sql: Optional[str] = None,
                insert_sql: Optional[str] = None):
        self.engine = create_engine(connection_string, echo=False, connect_args={"check_same_thread": False})

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

    def _create_table(self):
        with self.engine.connect() as connection:
            connection.execute(text(self.create_table_sql))
            connection.commit()

    def get_function_calls(self, function_name: str) -> List[Any]:
        with self.engine.connect() as connection:
            select_sql = "SELECT function, args, result FROM function_calls WHERE function = :function"
            result = connection.execute(text(select_sql), {"function": function_name})
            for row in result:
                yield row[0], pickle.loads(row[1]), pickle.loads(row[2])

    def get_result(self, call: FunctionCall) -> Any:
        function_name = call.func.__name__
        serialized_args = pickle.dumps({"args": call.args, "kwargs": call.kwargs})

        with self.engine.connect() as connection:
            result = connection.execute(text(self.select_sql), {"function": function_name, "args": serialized_args}).fetchone()
            if result is not None:
                return pickle.loads(result[0])
            else:
                raise KeyError()

    def store_result(self, call: FunctionCall, result: Any) -> None:
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