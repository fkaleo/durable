import functools
import sqlite3
import pickle
import hashlib
from typing import Callable, Any, Optional, Dict, Tuple

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
        select_sql = "SELECT result FROM cache WHERE key = ?"

    if insert_sql is None:
        insert_sql = "INSERT INTO cache (key, result) VALUES (?, ?)"

    if key_func is None:
        def default_key_func(func: Callable, args: Tuple, kwargs: Dict) -> str:
            key_data = (func.__name__, args, frozenset(kwargs.items()))
            return hashlib.md5(pickle.dumps(key_data)).hexdigest()

        key_func = default_key_func

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            conn = sqlite3.connect(connection_string)
            cursor = conn.cursor()

            # Execute the provided CREATE TABLE SQL
            cursor.execute(create_table_sql)

            # Generate a unique key using the provided key generation function
            key = key_func(func, args, kwargs)

            # Check if the result is in cache using the provided SELECT SQL
            cursor.execute(select_sql, (key,))
            cached_result = cursor.fetchone()

            if cached_result is not None:
                # Deserialize and return the cached result
                result = pickle.loads(cached_result[0])
            else:
                # Execute the function and cache the result
                result = func(*args, **kwargs)
                serialized_result = pickle.dumps(result)
                cursor.execute(insert_sql, (key, serialized_result))
                conn.commit()

            conn.close()
            return result

        return wrapper

    return decorator