from typing import Optional, Iterable

from singer_sdk import typing as th

from tap_nba_api.client import NBAStatsStream

from nba_api.stats.endpoints import leaguegamelog, playbyplayv2
from nba_api.live.nba.endpoints import playbyplay


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


class PlayByPlayLiveStream(NBAStatsStream):
    name = "playbyplaylive"
    primary_keys = ["GAME_ID", "actionNumber"]
    replication_key = None
    parent_stream_type = LeagueGameLogStream

    schema = th.PropertiesList(
        th.Property("GAME_ID", th.StringType),
        th.Property("actionNumber", th.IntegerType),
        th.Property("clock", th.StringType),
        th.Property("timeActual", th.StringType),
        th.Property("period", th.IntegerType),
        th.Property("periodType", th.StringType),
        th.Property("teamId", th.IntegerType),
        th.Property("teamTricode", th.StringType),
        th.Property("actionType", th.StringType),
        th.Property("subType", th.StringType),
        th.Property("descriptor", th.StringType),
        th.Property("personId", th.IntegerType),
        th.Property("x", th.NumberType),
        th.Property("y", th.NumberType),
        th.Property("possession", th.IntegerType),
        th.Property("scoreHome", th.StringType),
        th.Property("scoreAway", th.StringType),
        th.Property("edited", th.StringType),
        th.Property("orderNumber", th.IntegerType),
        th.Property("xLegacy", th.IntegerType),
        th.Property("yLegacy", th.IntegerType),
        th.Property("isFieldGoal", th.IntegerType),
        th.Property("jumpBallRecoveredName", th.StringType),
        th.Property("jumpBallRecoverdPersonId", th.IntegerType),
        th.Property("side", th.StringType),
        th.Property("playerName", th.StringType),
        th.Property("playerNameI", th.StringType),
        th.Property("jumpBallWonPlayerName", th.StringType),
        th.Property("jumpBallWonPersonId", th.IntegerType),
        th.Property("jumpBallLostPlayerName", th.StringType),
        th.Property("jumpBallLostPersonId", th.IntegerType),
        th.Property("shotResult", th.StringType),
        th.Property("stealPlayerName", th.StringType),
        th.Property("foulPersonalTotal", th.IntegerType),
        th.Property("foulDrawnPlayerName", th.StringType),
        th.Property("assistPlayerNameInitial", th.StringType),
        th.Property("officialId", th.IntegerType),
        th.Property("assistTotal", th.IntegerType),
        th.Property("blockPlayerName", th.StringType),
        th.Property("assistPersonId", th.IntegerType),
        th.Property("stealPersonId", th.IntegerType),
        th.Property("shotDistance", th.NumberType),
        th.Property("shotActionNumber", th.IntegerType),
        th.Property("reboundDefensiveTotal", th.IntegerType),
        th.Property("foulTechnicalTotal", th.IntegerType),
        th.Property("pointsTotal", th.IntegerType),
        th.Property("value", th.StringType),
        th.Property("reboundTotal", th.IntegerType),
        th.Property("blockPersonId", th.IntegerType),
        th.Property("reboundOffensiveTotal", th.IntegerType),
        th.Property("foulDrawnPersonId", th.IntegerType),
        th.Property("turnoverTotal", th.IntegerType),
    ).to_dict()

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        if context:
            game_id = context.get("GAME_ID")
            records = playbyplay.PlayByPlay(game_id=game_id).get_dict()["game"][
                "actions"
            ]
            for record in records:
                record = {
                    k: v
                    for k, v in record.items()
                    if k not in ["qualifiers", "personIdsFilter"]
                }
                record["GAME_ID"] = game_id
                yield record
