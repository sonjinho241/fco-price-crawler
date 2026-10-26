# -*- coding: utf-8 -*-
"""Top Price 후보 풀·기준가 수집 대상(REACH_CTE).

대상은 콘솔에 등록된 상품·회차 전부다. 끔·미리보기·종료일 지남과 상관없이 넣고, 콘솔에서 삭제한
행만 빠진다(2026-10-26 사용자 결정). 범용 회차(flow)의 보상 표도 따라가야 미리보기 회차가 열린다.

    python -m unittest discover -s tests
"""
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from iteminfo_job import REACH_CTE  # noqa: E402


class ReachTest(unittest.TestCase):
    def _source(self, name: str) -> str:
        start = REACH_CTE.index(f"{name} AS (")
        return REACH_CTE[start:REACH_CTE.index("), ", start)]

    def test_registered_rows_regardless_of_state(self):
        for name in ("live", "live_event"):
            source = self._source(name)
            self.assertNotIn("is_active", source)
            self.assertNotIn("ends_at", source)
            self.assertNotIn("is_preview", source)

    def test_event_kinds_include_flow_but_not_old_helper_rows(self):
        source = self._source("live_event")
        for kind in ("crystal_shop", "key_shop", "sss_board", "flow"):
            self.assertIn(f"'{kind}'", source)

    def test_flow_reward_tables(self):
        self.assertIn("config->'games'", REACH_CTE)
        self.assertIn("g->'cells'", REACH_CTE)
        self.assertIn("(g->>'sn')::int", REACH_CTE)
        self.assertIn("t->'outcomes'", REACH_CTE)


if __name__ == "__main__":
    unittest.main()
