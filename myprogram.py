from durable.cache_rocksdb import cache
from tqdm import tqdm
import time

@cache
def my_long_op(url: str) -> str:
    time.sleep(1)
    return f"{len(url)} {url}"


prefix = "http://"
urls = [f"{prefix}{i}" for i in range(100)]

for url in tqdm(urls, mininterval=0.05, miniters=1, dynamic_ncols=True):
    my_long_op(url)