# -*- coding: utf-8 -*-
"""Top Price 풀 조건의 클래스 코드 → 시즌 id.

못 찾은 코드는 unresolved_codes 로 남아 서버가 그 풀이 닿는 상품째 숨긴다("DB 에 없는 클래스").

    python -m unittest discover -s tests
"""
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

import iteminfo_job as job  # noqa: E402


class _Conn:
    """_SEASON_KEYS_SQL 결과(시즌 id, 정규화 키)만 돌려주는 가짜 연결."""

    def __init__(self, rows):
        self.rows = rows

    def execute(self, _sql):
        return iter(self.rows)


SEASONS = [(519, "26PLA"), (519, "26PREMIUMLIVE"), (517, "25PL"), (878, "26UP")]


class SeasonIdsTest(unittest.TestCase):
    def test_26pl_is_26pla(self):
        # 26UP UPGRADE 패키지 구성품 '26PL Top Price 5500 (1~11강)'.
        self.assertEqual(job._season_ids(_Conn(SEASONS), ["26PL"]), ([519], []))

    def test_plain_codes_and_unresolved(self):
        ids, unresolved = job._season_ids(_Conn(SEASONS), ["26UP", "25 PL", "EVO-CU"])
        self.assertEqual(ids, [517, 878])
        self.assertEqual(unresolved, ["EVO-CU"])


if __name__ == "__main__":
    unittest.main()
