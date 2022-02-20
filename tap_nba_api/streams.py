from typing import Optional, Iterable

from singer_sdk import typing as th

from tap_nba_api.client import NBAStatsStream

from nba_api.stats.endpoints import leaguegamelog


class LeagueGameLogStream(NBAStatsStream):

    name = "leaguegamelog"
    primary_keys = ["GAME_ID", "TEAM_ID"]
    replication_key = "GAME_DATE"

    schema = th.PropertiesList(
        th.Property("SEASON_ID", th.StringType),
        th.Property("TEAM_ID", th.IntegerType),
        th.Property("TEAM_ABBREVIATION", th.StringType),
        th.Property("TEAM_NAME", th.StringType),
        th.Property("GAME_ID", th.StringType),
        th.Property("GAME_DATE", th.StringType),
        th.Property("MATCHUP", th.StringType),
        th.Property("WL", th.StringType),
        th.Property("MIN", th.IntegerType),
        th.Property("FGM", th.IntegerType),
        th.Property("FGA", th.IntegerType),
        th.Property("FG_PCT", th.NumberType),
        th.Property("FG3M", th.IntegerType),
        th.Property("FG3A", th.IntegerType),
        th.Property("FG3_PCT", th.NumberType),
        th.Property("FTM", th.IntegerType),
        th.Property("FTA", th.IntegerType),
        th.Property("FT_PCT", th.NumberType),
        th.Property("OREB", th.IntegerType),
        th.Property("DREB", th.IntegerType),
        th.Property("REB", th.IntegerType),
        th.Property("AST", th.IntegerType),
        th.Property("STL", th.IntegerType),
        th.Property("BLK", th.IntegerType),
        th.Property("TOV", th.IntegerType),
        th.Property("PF", th.IntegerType),
        th.Property("PTS", th.IntegerType),
        th.Property("PLUS_MINUS", th.IntegerType),
        th.Property("VIDEO_AVAILABLE", th.IntegerType),
    ).to_dict()

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        records = leaguegamelog.LeagueGameLog().get_normalized_dict()["LeagueGameLog"]
        for record in records:
            yield record
