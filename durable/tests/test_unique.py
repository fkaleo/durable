import threading
from unittest.mock import create_autospec

from ..unique import ensure_unique


start_event = threading.Event()

def add_numbers(a, b):
    start_event.wait()  # Wait for the signal to start inside the function
    return a + b

def test_concurrent_duplicate_calls():
    """ Test that concurrent duplicate calls execute the function only once. """
    mocked_func = create_autospec(add_numbers, side_effect=add_numbers)
    uniqued_func = ensure_unique(mocked_func)

    # Create threads for concurrent execution
    threads = [
        threading.Thread(target=lambda: uniqued_func(50, 60))
        for _ in range(2)
    ]

    # Start all threads
    for thread in threads:
        thread.start()

    # Signal threads to proceed with the function call
    start_event.set()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    # Check that the function was called only once
    assert mocked_func.call_count == 1