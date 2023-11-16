from durable import durable
from tqdm import tqdm



store = durable.get_store()
# print(store["/my_long_op/http://1"])
prefix = "/my_long_op/"
# for k, v in store.items(from_key=prefix):
for k, v in durable.keys_with_prefix(store, prefix):
    print(k, v)