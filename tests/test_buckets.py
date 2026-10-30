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

    def test_catch_up_one_at_a_time_oldest_first(self):
        last = self.current - timedelta(hours=6)
        self.assertEqual(cj.pending_buckets(last, self.now), [self.current - timedelta(hours=5)])
        self.assertEqual(
            cj.pending_buckets(last, self.now, limit=3),
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


class NextActionTest(unittest.TestCase):
    now = datetime(2026, 10, 30, 14, 7, tzinfo=KST)
    current = datetime(2026, 10, 30, 14, 0, tzinfo=KST)
    budget = 330 * 60
    bucket = 45 * 60

    def test_behind_crawls_oldest(self):
        last = self.current - timedelta(hours=12)
        self.assertEqual(cj.next_action(last, self.now, 0, self.budget, self.bucket),
                         ("crawl", last + timedelta(hours=1), 0.0))

    def test_behind_but_out_of_budget_stops(self):
        last = self.current - timedelta(hours=12)
        self.assertEqual(cj.next_action(last, self.now, 290 * 60, self.budget, self.bucket)[0], "stop")

    def test_caught_up_sleeps_until_next_hour(self):
        action, bucket, wait = cj.next_action(self.current, self.now, 60, self.budget, self.bucket)
        self.assertEqual((action, bucket), ("sleep", None))
        self.assertAlmostEqual(wait, (53 * 60) + 90)

    def test_caught_up_near_end_stops(self):
        # 자고 일어나 받으면 예산을 넘는다 → 멈추고 다음 실행에 넘긴다.
        self.assertEqual(cj.next_action(self.current, self.now, 250 * 60, self.budget, self.bucket)[0], "stop")

    def test_simulated_catch_up(self):
        """3.3 req/s·버킷 9,000건이면 12버킷 밀림을 따라잡고, 그 뒤엔 매시 한 버킷씩 받는다."""
        bucket_sec = 9000 / 3.3
        clock = datetime(2026, 10, 30, 21, 20, tzinfo=KST)
        last = datetime(2026, 10, 30, 9, 0, tzinfo=KST)
        caught_up_at = None
        run_elapsed = 0.0
        for _ in range(500):
            action, bucket, wait = cj.next_action(last, clock, run_elapsed, self.budget, bucket_sec)
            if action == "crawl":
                clock += timedelta(seconds=bucket_sec)
                run_elapsed += bucket_sec
                last = bucket
            elif action == "sleep":
                if caught_up_at is None:
                    caught_up_at = clock
                clock += timedelta(seconds=wait)
                run_elapsed += wait
            else:  # 다음 실행이 2분 뒤 이어 받는다
                clock += timedelta(minutes=2)
                run_elapsed = 0.0
            if clock > datetime(2026, 11, 3, tzinfo=KST):
                break
        self.assertIsNotNone(caught_up_at)
        hours = (caught_up_at - datetime(2026, 10, 30, 21, 20, tzinfo=KST)).total_seconds() / 3600
        self.assertLess(hours, 48)
        self.assertGreaterEqual(cj.floor_hour(last), cj.floor_hour(clock) - timedelta(hours=1))


class RunBucketsTest(unittest.TestCase):
    """긴 실행 루프: 밀린 버킷을 하나씩 받고 상태를 버킷마다 적고, 따라잡으면 자고, 예산이 차면 멈춘다."""

    def setUp(self):
        import contextlib
        from unittest import mock

        self.state = {"last": datetime(2026, 10, 30, 9, 0, tzinfo=KST)}
        self.now = [datetime(2026, 10, 30, 10, 30, tzinfo=KST)]
        self.mono = [0.0]
        self.written, self.crawled, self.slept, self.vacuumed = [], [], [], []
        test = self

        class Engine:
            @contextlib.contextmanager
            def connect(self):
                yield None

        class FakeDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                return test.now[0].astimezone(tz)

        class Crawler:
            strongs = list(range(1, 14))
            gate = cj.RateGate(4)

            def __init__(self):
                self.stats = cj.CrawlStats()

            def crawl(self, label, spids):
                test.crawled.append(label)
                secs = 2700.0  # 버킷 하나 45분
                test.now[0] += timedelta(seconds=secs)
                test.mono[0] += secs
                self.stats.done += len(spids) * 13
                self.stats.busy += secs
                self.stats.changed += 1
                return True

        def sleep(s):
            test.slept.append(s)
            test.now[0] += timedelta(seconds=s)
            test.mono[0] += s

        patches = [
            mock.patch.object(cj, "read_last_bucket", lambda conn: test.state["last"]),
            mock.patch.object(cj, "write_last_bucket", lambda e, at: (test.written.append(at), test.state.update(last=at))),
            mock.patch.object(cj, "load_bucket", lambda conn, at, cap: ([], [1] * 170, [2] * 520)),
            mock.patch.object(cj, "_vacuum", lambda e: test.vacuumed.append(test.state["last"])),
            mock.patch.object(cj, "datetime", FakeDatetime),
            mock.patch.object(cj.time, "monotonic", lambda: test.mono[0]),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        self.engine, self.crawler, self.sleep = Engine(), Crawler(), sleep

    def test_catches_up_then_sleeps_then_stops_within_budget(self):
        ok = cj.run_buckets(self.engine, self.crawler, 330 * 60, 100, sleep=self.sleep)
        self.assertTrue(ok)
        # 10시부터 한 시간씩 빠짐없이, 순서대로 적었다.
        self.assertEqual(self.written, [datetime(2026, 10, 30, 10, tzinfo=KST) + timedelta(hours=i)
                                        for i in range(len(self.written))])
        self.assertGreaterEqual(len(self.written), 6)
        self.assertTrue(self.slept)                      # 따라잡은 뒤 다음 정시까지 잤다
        self.assertLessEqual(self.mono[0], 330 * 60)     # 예산을 넘지 않았다
        # VACUUM 은 KST 10·16시 같은 6시간 버킷에서만.
        self.assertTrue(all(cj.hot_slot(at) % 6 == 4 for at in self.vacuumed))
        self.assertIn(datetime(2026, 10, 30, 10, tzinfo=KST), self.vacuumed)

    def test_blocked_stops_without_writing(self):
        self.crawler.crawl = lambda label, spids: False
        self.assertFalse(cj.run_buckets(self.engine, self.crawler, 330 * 60, 100, sleep=self.sleep))
        self.assertEqual(self.written, [])


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
