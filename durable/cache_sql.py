import functools
import pickle
import sqlite3
from typing import Callable, Dict, Optional, Tuple

from .durable import ResultStore, caching_decorator, key_for_function_call


class SQLResultStore(ResultStore):
    def __init__(self, connection_string, create_table_sql, select_sql, insert_sql):
        self.connection = sqlite3.connect(connection_string)
        self.create_table_sql = create_table_sql
        self.select_sql = select_sql
        self.insert_sql = insert_sql

    def __del__(self):
        self.connection.close()

    def get_result(self, key):
        cursor = self.connection.cursor()
        cursor.execute(self.select_sql, (key,))
        cached_result = cursor.fetchone()

        if cached_result is not None:
            # Deserialize and return the cached result
            return pickle.loads(cached_result[0])
        else:
            raise KeyError()

    def store_result(self, key, result):
        serialized_result = pickle.dumps(result)
        cursor = self.connection.cursor()
        cursor.execute(self.insert_sql, (key, serialized_result))
        self.connection.commit()

    def store_exception(self, key, exception):
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
    if create_table_sql is None:
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS cache (
                key TEXT PRIMARY KEY,
                result BLOB
            )
        """

    if select_sql is None:
        select_sql = "SELECT result FROM cache WHERE key = ? LIMIT 1"

    if insert_sql is None:
        insert_sql = "INSERT INTO cache (key, result) VALUES (?, ?)"

    if key_func is None:
        key_func = key_for_function_call

    store = SQLResultStore(connection_string, create_table_sql, select_sql, insert_sql)
    return functools.partial(caching_decorator, key_func=key_func, store=store)