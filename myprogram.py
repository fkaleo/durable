from durable.cache_rocksdb import cache, observe
from tqdm import tqdm
import time

@cache
@observe
def my_long_op(url: str) -> str:
    time.sleep(1)
    return f"{len(url)} {url}"

@cache
@observe
def my_very_long_op(url: str) -> str:
    time.sleep(1)
    return url.split("://")[1]

prefix = "http://"
urls = [f"{prefix}{i}" for i in range(1000000)]

for url in tqdm(urls, mininterval=0.05, miniters=1, dynamic_ncols=True):
    # my_very_long_op(url)
    my_long_op(url)