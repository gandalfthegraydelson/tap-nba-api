from singer_sdk.streams.core import Stream
from singer_sdk.tap_base import Tap


class NBAStatsStream(Stream):
    def __init__(self, tap: Tap):
        super().__init__(tap)
