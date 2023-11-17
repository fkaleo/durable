import hashlib
import random
import time
from typing import List

import pandas as pd
from pandas import DataFrame


# Adjusting the MediaSchemaWithNames to include a url field
class MediaSchemaWithNames:
    title: str
    release_date: str
    keywords: List[str]
    raw_metadata: str
    url: str

def media_service_search_person(
    media_service: str, first_name: str, last_name: str, start: int, max_results: int
) -> DataFrame:

    id = hashlib.sha256(f"{first_name}{last_name}".encode()).hexdigest()[:10]
    # Mock data generation
    mock_data = [
        {
            "title": f"Mock Title {i}",
            "release_date": "2023-01-01",
            "keywords": ["mock", "data"],
            "raw_metadata": "{'mock': 'data'}",
            "url": f"https://www.youtube.com/watch?v={id}_{i}",
            "person_first_name": first_name,
            "person_last_name": last_name
        }
        for i in range(start, start + max_results)
    ]

    # Create DataFrame
    df = pd.DataFrame(mock_data)

    # Adjust data types as in the original function
    df["keywords"] = df["keywords"].astype(pd.StringDtype())
    df["raw_metadata"] = df["raw_metadata"].astype(str)

    return df



def is_speaker_in_video(person_name: str, description: str, model: str = "gpt-4") -> float:
    """
    Determine if a given person is mentioned as a speaker in a video description.

    Parameters
    ----------
    person_name : str
        Name of the person to check if they are mentioned as a speaker.
    description : str
        Description of the video.
    model : str, optional
        Name of the model to use for determination, by default "gpt-4".

    Returns
    -------
    float
        A probability value between 0.0 and 1.0 indicating the likelihood that
        the person is mentioned as a speaker in the video description.

    Raises
    ------
    ValueError
        If the returned probability value is not between 0.0 and 1.0.

    Example
    -------
    >>> is_speaker_in_video("John Doe", "This video features talks by John Doe and Jane Smith.")
    1.0

    >>> is_speaker_in_video("John Doe", "In this documentary, we discuss the theories of John Doe.")
    0.0
    """
    time.sleep(2)
    probability = 0.5

    if probability >= 0.0 and probability <= 1.0:
        return probability
    else:
        raise ValueError(f"probability returned is {probability} but should be between 0.0 and 1.0")


def get_diarized_transcript(media_service: str, url: str) -> pd.DataFrame:
    NUM_ROWS = 5
    
    # Set random seed based on hash of url
    random.seed(int(hashlib.sha256(url.encode()).hexdigest(), 16))

    sentences = [
        "The quick brown fox jumps over the lazy dog.",
        "She sells seashells by the seashore.",
        "Peter Piper picked a peck of pickled peppers.",
        "How much wood would a woodchuck chuck, if a woodchuck could chuck wood?",
        "I scream, you scream, we all scream for ice cream!",
        "The rain in Spain stays mainly in the plain.",
        "To be or not to be, that is the question.",
        "All work and no play makes Jack a dull boy.",
        "A picture is worth a thousand words.",
        "You can't handle the truth!"
    ]


    # Mock data generation
    speakers = ["John", "Jane", "Bob", "Alice"]
    mock_data = {
        "speaker": random.choices(speakers, k=NUM_ROWS),
        "start": [random.uniform(0, 10) for _ in range(NUM_ROWS)],
        "end": [random.uniform(0, 10) for _ in range(NUM_ROWS)],
        "text": [random.choice(sentences) for _ in range(NUM_ROWS)]
    }

    # Create DataFrame
    df = pd.DataFrame(mock_data)

    # Reinitialize random seed
    random.seed()
    time.sleep(10)

    return df