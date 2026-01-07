from enum import Enum


class RunnerType(Enum):
    INGEST = 1
    SEARCH = 2
    SEARCH_WITH_RECALL = 3
    UNKNOWN = 1000