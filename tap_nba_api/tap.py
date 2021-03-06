from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th

from tap_nba_api.streams import (
    LeagueGameLogStream,
    PlayByPlayLiveStream,
    PlayByPlayV2Stream,
)

STREAM_TYPES = [
    LeagueGameLogStream,
    PlayByPlayV2Stream,
    PlayByPlayLiveStream,
]


class TapNBAStats(Tap):
    """NBAStats tap class."""

    name = "tap-nba-api"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "season",
            th.IntegerType,
            required=True,
            description="2021 for 2021-2022 season",
        )
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
