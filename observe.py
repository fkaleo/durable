from durable import durable

store = durable.get_store(durable.DEFAULT_CACHE_STORE_ID)

# print(store["/my_long_op/http://1"])
prefix = "/"
# for k, v in store.items(from_key=prefix):
for k, v in durable.keys_with_prefix(store, prefix):
    print(k, v)