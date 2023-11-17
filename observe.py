from durable import durable
from durable.cache_sql import SQLResultStore

result_store = SQLResultStore("sql_cache.db")
# result_store = durable.DictResultStore(durable.get_store(durable.DEFAULT_CACHE_STORE_ID))

# prefix = "/my_long_op/http://1"
for v in result_store.get_results("is_speaker_in_video"):
    print(v)