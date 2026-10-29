# -*- coding: utf-8 -*-
"""시세 배치 두 묶음(hot/tail) 버킷 배정·밀린 버킷 계산·요청 문.

    python -m unittest discover -s tests
"""
import pathlib
import sys
import unittest
from collections import Counter
from datetime import datetime, timedelta, timezone

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

import crawler_job as cj  # noqa: E402

KST = timezone(timedelta(hours=9))

# 실제 spid 모양(시즌 번호×100만 + 선수 번호)으로 여러 시즌에 걸친 카드 묶음.
SPIDS = [season * 1_000_000 + pid
         for season in (101, 214, 253, 300, 801, 836, 867)
         for pid in range(190_000, 260_000, 7)]


class BucketOfTest(unittest.TestCase):
    def test_deterministic(self):
        # 파이썬 hash() 와 달리 실행이 바뀌어도 같은 값(고정 기대값으로 확인).
        self.assertEqual(cj.bucket_of(101190000, 168), cj.bucket_of(101190000, 168))
        self.assertEqual([cj.bucket_of(s, 168) for s in SPIDS[:50]],
                         [cj.bucket_of(s, 168) for s in SPIDS[:50]])
        import zlib
        self.assertEqual(cj.bucket_of(867231443, 168), zlib.crc32(b"867231443") % 168)

    def _assert_even(self, n: int):
        counts = Counter(cj.bucket_of(s, n) for s in SPIDS)
        self.assertEqual(set(counts), set(range(n)))
        mean = len(SPIDS) / n
        # 7만 장에서 버킷마다 평균의 ±25% 안(실제 8.9만 장은 168 버킷 466~592, 평균 532).
        self.assertLess(max(counts.values()), mean * 1.25)
        self.assertGreater(min(counts.values()), mean * 0.75)

    def test_even_over_168(self):
        self._assert_even(168)

    def test_even_over_24(self):
        self._assert_even(24)


class SlotTest(unittest.TestCase):
    def test_week_starts_monday_midnight_kst(self):
        monday = datetime(2026, 10, 26, 0, 0, tzinfo=KST)  # 월요일
        self.assertEqual(cj.tail_slot(monday), 0)
        self.assertEqual(cj.tail_slot(monday + timedelta(days=6, hours=23)), 167)
        self.assertEqual(cj.hot_slot(monday + timedelta(hours=5)), 5)

    def test_utc_input_uses_kst(self):
        # UTC 일요일 15시 = KST 월요일 00시.
        at = datetime(2026, 10, 25, 15, 0, tzinfo=timezone.utc)
        self.assertEqual(cj.tail_slot(at), 0)
        self.assertEqual(cj.hot_slot(at), 0)

    def test_full_week_covers_every_card_once(self):
        start = datetime(2026, 10, 26, 0, 0, tzinfo=KST)
        seen = Counter()
        for h in range(168):
            _, tail = cj.bucket_spids(start + timedelta(hours=h), SPIDS, hot=set())
            seen.update(tail)
        self.assertEqual(set(seen), set(SPIDS))
        self.assertEqual(set(seen.values()), {1})


class PendingBucketsTest(unittest.TestCase):
    now = datetime(2026, 10, 30, 14, 7, tzinfo=KST)
    current = datetime(2026, 10, 30, 14, 0, tzinfo=KST)

    def test_no_state_starts_now(self):
        self.assertEqual(cj.pending_buckets(None, self.now), [self.current])

    def test_on_time(self):
        last = self.current - timedelta(hours=1)
        self.assertEqual(cj.pending_buckets(last, self.now), [self.current])

    def test_already_done(self):
        self.assertEqual(cj.pending_buckets(self.current, self.now), [])

    def test_catch_up_capped_at_three_oldest_first(self):
        last = self.current - timedelta(hours=6)
        self.assertEqual(
            cj.pending_buckets(last, self.now),
            [self.current - timedelta(hours=5), self.current - timedelta(hours=4),
             self.current - timedelta(hours=3)],
        )

    def test_exactly_seven_days_still_catches_up(self):
        last = self.current - timedelta(days=7)
        self.assertEqual(cj.pending_buckets(last, self.now)[0], last + timedelta(hours=1))

    def test_more_than_seven_days_resets(self):
        last = self.current - timedelta(days=7, hours=1)
        self.assertEqual(cj.pending_buckets(last, self.now), [self.current])

    def test_state_stored_in_utc(self):
        last = (self.current - timedelta(hours=1)).astimezone(timezone.utc)
        self.assertEqual(cj.pending_buckets(last, self.now), [self.current])


class BucketSpidsTest(unittest.TestCase):
    start = datetime(2026, 10, 26, 0, 0, tzinfo=KST)

    def test_hot_daily_tail_weekly_without_duplicates(self):
        hot = set(SPIDS[::20])
        seen = Counter()
        for h in range(168):
            hot_part, tail_part = cj.bucket_spids(self.start + timedelta(hours=h), SPIDS, hot)
            self.assertFalse(set(hot_part) & set(tail_part))
            self.assertTrue(set(hot_part) <= hot)
            self.assertFalse(set(tail_part) & hot)
            seen.update(hot_part + tail_part)
        self.assertEqual({seen[s] for s in hot}, {7})            # 하루 한 번 × 7일
        self.assertEqual({seen[s] for s in SPIDS if s not in hot}, {1})

    def test_hot_outside_players_is_dropped(self):
        ghost = 999_999_999
        hits = [cj.bucket_spids(self.start + timedelta(hours=h), SPIDS, {ghost})[0] for h in range(24)]
        self.assertEqual(sum(hits, []), [])

    def test_skip_removes_already_fetched(self):
        hot = set(SPIDS[::20])
        for h in range(24):
            at = self.start + timedelta(hours=h)
            full_hot, full_tail = cj.bucket_spids(at, SPIDS, hot)
            skip = set(full_hot[:1] + full_tail[:1])
            hot_part, tail_part = cj.bucket_spids(at, SPIDS, hot, skip=skip)
            self.assertFalse(skip & set(hot_part + tail_part))


class RateGateTest(unittest.TestCase):
    def _gate(self, rate=4.0):
        self.t = 0.0
        self.slept = []

        def sleep(s):
            self.slept.append(s)
            self.t += s

        return cj.RateGate(rate, sleep=sleep, clock=lambda: self.t)

    def test_rate_never_above_five(self):
        self.assertAlmostEqual(self._gate(50).min_interval, 0.2)
        self.assertAlmostEqual(self._gate(0).min_interval, 0.2)

    def test_spacing(self):
        gate = self._gate(4.0)
        for _ in range(5):
            gate.wait()
        self.assertAlmostEqual(self.t, 1.0)

    def test_backoff_then_block(self):
        gate = self._gate()
        gate.throttle(429, None)
        gate.wait()
        self.assertAlmostEqual(self.t, 30)
        gate.throttle(429, None)           # 같은 파도(쉬는 중)가 아니라 다음 단계
        gate.wait()
        self.assertAlmostEqual(self.t, 90)
        gate.throttle(403, "5")            # Retry-After 를 따른다
        gate.wait()
        self.assertAlmostEqual(self.t, 95)
        self.assertFalse(gate.blocked)
        gate.throttle(429, None)
        self.assertTrue(gate.blocked)

    def test_in_flight_responses_count_once(self):
        gate = self._gate()
        for _ in range(5):
            gate.throttle(429, None)
        gate.wait()
        self.assertAlmostEqual(self.t, 30)
        self.assertEqual(gate.throttled, 5)
        self.assertFalse(gate.blocked)

    def test_success_resets(self):
        gate = self._gate()
        for _ in range(3):
            gate.throttle(429, None)
            gate.wait()
            gate.ok()
        gate.throttle(429, None)
        self.assertFalse(gate.blocked)


class FetchOneTest(unittest.TestCase):
    class _Resp:
        def __init__(self, status, text="", headers=None):
            self.status_code, self.text, self.headers = status, text, headers or {}

        def raise_for_status(self):
            if self.status_code >= 400:
                import requests
                raise requests.HTTPError(response=self)

    class _Session:
        def __init__(self, responses):
            self.responses = list(responses)

        def post(self, *a, **k):
            return self.responses.pop(0)

    def test_429_waits_then_succeeds(self):
        ok = self._Resp(200, '<strong class="x" alt="1,000">1,000</strong> var json1 = {}')
        session = self._Session([self._Resp(429, headers={"Retry-After": "1"}), ok])
        gate = cj.RateGate(5, sleep=lambda s: None)
        row = cj._fetch_one(session, gate, 101190000, 1, max_retries=1, delay=0)
        self.assertEqual(row["price"], 1000)
        self.assertEqual(gate.throttled, 1)

    def test_blocked_returns_none(self):
        gate = cj.RateGate(5, sleep=lambda s: None)
        gate.blocked = True
        self.assertIsNone(cj._fetch_one(self._Session([]), gate, 1, 1, 3, 0))


if __name__ == "__main__":
    unittest.main()
