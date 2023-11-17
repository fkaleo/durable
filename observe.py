from durable.durable import DictResultStore
from durable.cache_sql import SQLAlchemyResultStore

result_store = SQLAlchemyResultStore("sqlite:///sql_cache.db")
# result_store = DictResultStore(durable.get_store(durable.DEFAULT_CACHE_STORE_ID))

for v in result_store.get_function_calls("is_speaker_in_video"):
    print(v)