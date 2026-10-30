# -*- coding: utf-8 -*-
"""메타 크롤러의 오류 페이지 카드 판정과 데이터센터 시드 분할. 넥슨·DB 에 묻지 않는다.

2026-10-30 넥슨이 지난 시즌 LIVE 카드 5,614장을 300 에서 325 로 옮기자, 300 쪽 상세가
오류 페이지로 넘어가 금요일 풀크롤이 "크롤 실패 5614건" 을 냈다.

    python -m unittest discover -s tests
"""
import pathlib
import sys
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

import meta_crawler as m  # noqa: E402
import notify  # noqa: E402


class Res:
    def __init__(self, url, history=(), status=200, text=""):
        self.url, self.history, self.status_code, self.text = url, list(history), status, text

    def raise_for_status(self):
        if self.status_code >= 400:
            raise m.requests.HTTPError(str(self.status_code))


class FakeSession:
    def __init__(self, res):
        self.res = res
        self.calls = 0

    def get(self, url, timeout=None):
        self.calls += 1
        return self.res


ERROR_URL = "https://bulletin.nexon.com/nxk/error.html?aspxerrorpath=/datacenter/PlayerAbility"


class CardMissingTest(unittest.TestCase):
    def test_error_redirect_is_card_missing(self):
        s = FakeSession(Res(ERROR_URL, history=[object(), object()]))
        with self.assertRaises(m.CardMissing):
            m.fetch_player_html(s, 300052679)

    def test_normal_page_passes(self):
        s = FakeSession(Res(m.ABILITY_URL + "?spid=325052679", text="<html/>"))
        self.assertEqual(m.fetch_player_html(s, 325052679), "<html/>")

    def test_missing_is_not_retried(self):
        s = FakeSession(Res(ERROR_URL, history=[object()]))
        spid, parsed, err = m._fetch_and_parse(s, 300052679, max_retries=3, delay=0)
        self.assertEqual((spid, parsed, err), (300052679, None, m.MISSING))
        self.assertEqual(s.calls, 1)


class SplitMissingTest(unittest.TestCase):
    def test_card_move_is_hidden(self):
        # 실제 이동: 샤드당 약 6%
        hide, outage = m.split_missing([3, 1, 2], 50)
        self.assertEqual((hide, outage), ([1, 2, 3], 0))

    def test_mass_error_page_is_outage(self):
        hide, outage = m.split_missing(list(range(20)), 100)
        self.assertEqual((hide, outage), ([], 20))

    def test_nothing_missing(self):
        self.assertEqual(m.split_missing([], 0), ([], 0))


class LastMissingTest(unittest.TestCase):
    def test_union_of_shard_rows(self):
        conn = mock.Mock()
        conn.execute.return_value = [({"missing": [1, 2]},), ({"missing": [3]},), (None,)]
        self.assertEqual(m.read_last_missing_spids(conn), {1, 2, 3})


class SeedSplitTest(unittest.TestCase):
    """급여·OVR 한 값에서도 200장 상한에 닿으면 키 구간을 나눠 끝까지 긁는다."""

    def _run(self, max_cards):
        def fake_fetch(session, season_id, page, timeout=20, salary=None, ovr=None, height=None):
            return (salary, ovr, height)

        def fake_parse(key, season_id):
            salary, ovr, height = key
            # 급여 5·OVR 51 에만 카드가 몰려 있다(키 없이 물으면 200장으로 잘린다).
            if not (salary[0] <= 5 <= salary[1]):
                return {}
            lo, hi = ovr or (m._OVR_MIN, m._OVR_MAX)
            if not (lo <= 51 <= hi):
                return {}
            cards = {325_000_000 + i: f"p{i}" for i in range(246)}
            if height is not None:
                h_lo, h_hi = height
                cards = {sp: n for sp, n in cards.items()
                         if h_lo <= (170 if sp % 2 else 190) <= h_hi}
            return dict(list(cards.items())[:m._DATACENTER_LIST_CAP])

        with mock.patch.object(m, "fetch_player_list_page", fake_fetch), \
                mock.patch.object(m, "parse_player_list", fake_parse), \
                mock.patch.object(m.time, "sleep"):
            return m.fetch_season_spids_from_datacenter(None, 325, max_cards)

    def test_height_split_collects_all(self):
        self.assertEqual(len(self._run(None)), 246)

    def test_default_cap_stops_early(self):
        with mock.patch.object(m, "_SEED_MAX_CARDS", 100):
            self.assertLessEqual(len(self._run(100)), m._DATACENTER_LIST_CAP)


class GrowingSeasonTest(unittest.TestCase):
    """ICON 계열은 기존 시즌에 카드가 추가된다(2026-10-30 ICON TM 8명 → 100·101·110 에 8장씩)."""

    def test_default_seasons(self):
        with mock.patch.dict(m.os.environ, {}, clear=True):
            self.assertEqual(m.growing_season_ids(), [100, 101, 110])

    def test_env_override(self):
        with mock.patch.dict(m.os.environ, {"META_SEED_GROWING_SEASONS": "100"}):
            self.assertEqual(m.growing_season_ids(), [100])

    def test_fetch_skips_oversized_season(self):
        lists = {100: {100082092: "마리오 켐페스"}, 101: {101000000 + i: "" for i in range(1001)}}
        with mock.patch.object(m, "fetch_season_spids_from_datacenter",
                               lambda session, sid: lists.get(sid, {})):
            self.assertEqual(m.fetch_growing_season_spids(None, [100, 101, 110]),
                             {100082092: "마리오 켐페스"})

    def test_growing_cards_are_not_gone(self):
        """spid.json 이 늦어도 데이터센터에 있는 추가 카드는 사라진 카드로 숨기지 않는다."""
        conn = mock.MagicMock()
        conn.execute.side_effect = lambda sql, *a: {
            id(m._ALL_CRAWLED_SPIDS_SQL): [mock.Mock(spid=100082092, crawled_at=None),
                                           mock.Mock(spid=100000001, crawled_at=None)],
            id(m._HIDDEN_SPIDS_SQL): [],
            id(m._LAST_MISSING_SQL): [],
        }.get(id(sql), mock.Mock(rowcount=0))
        engine = mock.Mock()
        engine.connect.return_value.__enter__ = lambda s: conn
        engine.connect.return_value.__exit__ = lambda *a: False
        m._log_gone_spids(engine, "r", [{"id": 100000001}], {100082092: "마리오 켐페스"})
        hides = [c for c in conn.execute.call_args_list if c.args[0] is m._HIDE_SPIDS_SQL]
        self.assertEqual(hides, [])


class EnvListTest(unittest.TestCase):
    def test_parse(self):
        with mock.patch.dict(m.os.environ, {"META_SEED_LARGE_SEASONS": "325, 326,x"}):
            self.assertEqual(m._env_int_list("META_SEED_LARGE_SEASONS"), [325, 326])

    def test_unset(self):
        with mock.patch.dict(m.os.environ, {}, clear=True):
            self.assertEqual(m._env_int_list("META_SEED_LARGE_SEASONS"), [])


class NotifyMissingTest(unittest.TestCase):
    def _summary(self, **kw):
        s = {
            "run_id": "41028386512", "new_players": 0, "new_spid_count": 0, "changed": 0,
            "failures": 0, "new_traits": {}, "new_team_colors": {}, "new_seasons": [],
            "top_new": [], "top_changed": [], "shards_logged": 4, "shards_expected": 4,
            "missing_shards": 0, "career": None, "gone_total": 0, "newly_gone_count": 0,
            "top_gone": [], "missing_total": 0, "newly_missing_count": 0, "top_missing": [],
            "career_expected": False,
        }
        s.update(kw)
        return s

    def test_newly_missing_is_reported_not_failure(self):
        msg = notify.build_message(self._summary(
            missing_total=5614, newly_missing_count=5614, top_missing=[("LIVE", "글렌 모리스")]))
        self.assertIn("넥슨 오류 페이지(검색 숨김): LIVE 글렌 모리스 외 5613장", msg)
        self.assertNotIn("크롤 실패", msg)
        self.assertNotIn("변동 없음", msg)

    def test_already_hidden_is_quiet(self):
        msg = notify.build_message(self._summary(missing_total=5614))
        self.assertIn("변동 없음", msg)


if __name__ == "__main__":
    unittest.main()
