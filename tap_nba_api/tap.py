from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th

from tap_nba_api.streams import LeagueGameLogStream

STREAM_TYPES = [
    LeagueGameLogStream,
]


class TapNBAStats(Tap):
    """NBAStats tap class."""

    name = "tap-nba-api"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList().to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
