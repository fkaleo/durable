import pytest
import sqlite3
import os

from ..cache_sql import sql_cached

# A simple function to be decorated for testing
@sql_cached()
def double(param: int) -> int:
    return param * 2

# Tests
def setup_module(module):
    """ Setup for the entire module """
    # Create a new database for testing
    conn = sqlite3.connect("test.db")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cache (
            key TEXT PRIMARY KEY,
            result BLOB
        )
    """)
    conn.commit()
    conn.close()

def teardown_module(module):
    """ Teardown for the entire module """
    # Remove the test database file
    os.remove("test.db")

def test_caching_of_function():
    """ Test if the function result is cached """
    first_call_result = double(5)
    second_call_result = double(5)

    assert first_call_result == second_call_result
    assert first_call_result == 10

def test_custom_key_generator():
    """ Test the custom key generator functionality """

    def custom_key_gen(func, args, kwargs):
        return f"custom_{func.__name__}_{args[0]}"

    custom_cached_double = sql_cached(key_gen_func=custom_key_gen)(double)
    
    # Call the function to ensure the result is cached
    custom_cached_double(3)

    conn = sqlite3.connect("test.db")
    cursor = conn.cursor()
    cursor.execute("SELECT key FROM cache")
    keys = cursor.fetchall()

    # Debugging output
    print("Generated keys in cache:", keys)
    print("Expected key:", custom_key_gen(double, (3,), {}))

    assert len(keys) > 0
    assert "custom_double_3" in keys[0]


def test_no_cache_on_different_args():
    """ Test that different arguments do not hit the cache """
    result_for_3 = double(3)
    result_for_4 = double(4)

    assert result_for_3 != result_for_4
    assert result_for_3 == 6
    assert result_for_4 == 8

if __name__ == "__main__":
    pytest.main()
