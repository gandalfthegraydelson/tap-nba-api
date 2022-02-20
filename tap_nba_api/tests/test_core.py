import datetime

from singer_sdk.testing import get_standard_tap_tests

from tap_nba_api.tap import TapNBAStats

SAMPLE_CONFIG = {}


def test_standard_tap_tests():
    tests = get_standard_tap_tests(TapNBAStats, config=SAMPLE_CONFIG)
    for test in tests:
        test()
