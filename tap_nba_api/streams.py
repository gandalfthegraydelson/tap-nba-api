from typing import Optional, Iterable

from singer_sdk import typing as th

from tap_nba_api.client import NBAStatsStream

from nba_api.stats.endpoints import leaguegamelog, playbyplayv2


def home_or_away(record):
    return "HOME" if " vs. " in record["MATCHUP"] else "AWAY"


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

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        return {"GAME_ID": record["GAME_ID"]} if home_or_away(record) == "HOME" else {}

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        date_from = self.get_starting_replication_key_value(context)
        records = leaguegamelog.LeagueGameLog(
            season=self.season, date_from_nullable=date_from
        ).get_normalized_dict()["LeagueGameLog"]
        for record in records:
            yield record


class PlayByPlayV2Stream(NBAStatsStream):
    name = "playbyplayv2"
    primary_keys = ["GAME_ID", "EVENTNUM"]
    replication_key = None
    parent_stream_type = LeagueGameLogStream

    schema = th.PropertiesList(
        th.Property("GAME_ID", th.StringType),
        th.Property("EVENTNUM", th.IntegerType),
        th.Property("EVENTMSGTYPE", th.IntegerType),
        th.Property("EVENTMSGACTIONTYPE", th.IntegerType),
        th.Property("PERIOD", th.IntegerType),
        th.Property("WCTIMESTRING", th.StringType),
        th.Property("PCTIMESTRING", th.StringType),
        th.Property("HOMEDESCRIPTION", th.StringType),
        th.Property("NEUTRALDESCRIPTION", th.StringType),
        th.Property("VISITORDESCRIPTION", th.StringType),
        th.Property("SCORE", th.StringType),
        th.Property("SCOREMARGIN", th.StringType),
        th.Property("PERSON1TYPE", th.IntegerType),
        th.Property("PLAYER1_ID", th.IntegerType),
        th.Property("PLAYER1_NAME", th.StringType),
        th.Property("PLAYER1_TEAM_ID", th.IntegerType),
        th.Property("PLAYER1_TEAM_CITY", th.StringType),
        th.Property("PLAYER1_TEAM_NICKNAME", th.StringType),
        th.Property("PLAYER1_TEAM_ABBREVIATION", th.StringType),
        th.Property("PERSON2TYPE", th.IntegerType),
        th.Property("PLAYER2_ID", th.IntegerType),
        th.Property("PLAYER2_NAME", th.StringType),
        th.Property("PLAYER2_TEAM_ID", th.IntegerType),
        th.Property("PLAYER2_TEAM_CITY", th.StringType),
        th.Property("PLAYER2_TEAM_NICKNAME", th.StringType),
        th.Property("PLAYER2_TEAM_ABBREVIATION", th.StringType),
        th.Property("PERSON3TYPE", th.IntegerType),
        th.Property("PLAYER3_ID", th.IntegerType),
        th.Property("PLAYER3_NAME", th.StringType),
        th.Property("PLAYER3_TEAM_ID", th.IntegerType),
        th.Property("PLAYER3_TEAM_CITY", th.StringType),
        th.Property("PLAYER3_TEAM_NICKNAME", th.StringType),
        th.Property("PLAYER3_TEAM_ABBREVIATION", th.StringType),
        th.Property("VIDEO_AVAILABLE_FLAG", th.IntegerType),
    ).to_dict()

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        if context:
            game_id = context.get("GAME_ID")
            records = playbyplayv2.PlayByPlayV2(game_id=game_id).get_normalized_dict()[
                "PlayByPlay"
            ]
            for record in records:
                yield record
