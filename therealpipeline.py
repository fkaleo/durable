import pandas as pd
import ray

from durable.cache_sql import sql_cached
from durable.distributed import ray_to_future
from durable.durable import cache
from functions import media_service_search_person, \
                        get_diarized_transcript as real_get_diarized_transcript, \
                        is_speaker_in_video as real_is_speaker_in_video


# media_service_search_person = ray_to_future(ray.remote(num_cpus=0.1)(media_service_search_person))
# @ray_to_future
# @ray.remote(num_cpus=0.1)
# @cache
@sql_cached("sql_cache.db")
def is_speaker_in_video(*args, **kwargs) -> float:
    return real_is_speaker_in_video(*args, **kwargs)

@sql_cached("sql_cache.db")
def get_diarized_transcript(*args, **kwargs) -> pd.DataFrame:
    return real_get_diarized_transcript(*args, **kwargs)

people = [
    {"first_name": "John", "last_name": "Doe"},
    {"first_name": "Jane", "last_name": "Doe"},
    {"first_name": "Bob", "last_name": "Smith"},
    {"first_name": "Alice", "last_name": "Johnson"}
]

for person in people:
    videos = media_service_search_person('YouTube', person['first_name'], person['last_name'], 0, 1)
    for i, video in videos.iterrows():
        print(video['title'], video['url'])
        if is_speaker_in_video(f"{person['first_name']} {person['last_name']}", video['title']):
            transcript = get_diarized_transcript('YouTube', video['url'])
            print(transcript)
            # print(transcript.result())
            print()
