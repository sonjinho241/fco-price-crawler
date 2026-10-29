# -*- coding: utf-8 -*-
"""확률표 수집 순서·속도(plan_targets · Fetcher · fetch_pages). 넥슨에 묻지 않는다.

2026-10-29 넥슨이 GitHub 러너의 4개 동시 요청에 429 를 줘서 297개 중 21개만 받았다. 그 뒤로
새 표·등록 상품에서 닿는 표를 먼저, 한 번에 하나씩 받고 429 면 오래 쉬다 멈춘다.

    python -m unittest discover -s tests
"""
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

import iteminfo_job as job  # noqa: E402


class Res:
    def __init__(self, code, text="", headers=None):
        self.status_code, self.text, self.headers = code, text, headers or {}


class FakeSession:
    def __init__(self, answers):
        self.answers = list(answers)
        self.urls = []

    def get(self, url, timeout=None):
        self.urls.append(url)
        a = self.answers.pop(0)
        if isinstance(a, Exception):
            raise a
        return a


def fetcher(answers, budget=10_000, clock=None):
    waits = []
    f = job.Fetcher(session=FakeSession(answers), delay=1.0, budget=budget,
                    sleep=waits.append, clock=clock or (lambda: 0.0), rand=lambda: 0.5)
    return f, waits


class PlanTest(unittest.TestCase):
    INDEX = [(105, "새 표"), (104, "닿는 표"), (103, "옛 표 c"), (102, "옛 표 b"), (101, "옛 표 a")]
    SEEN = {104: "2026-10-29", 103: "2026-10-28", 102: "2026-10-20", 101: "2026-10-25"}

    def test_new_and_reached_first_then_oldest(self):
        must, extra = job.plan_targets(self.INDEX, self.SEEN, {104, 999}, rotate=2)
        self.assertEqual(must, [105, 104])
        self.assertEqual(extra, [102, 101])  # 가장 오래전에 받은 순

    def test_rotate_zero_and_reach_outside_index(self):
        must, extra = job.plan_targets(self.INDEX, self.SEEN, {999}, rotate=0)
        self.assertEqual(must, [105])  # 목록에 없는 닿는 표(999)는 묻지 않는다
        self.assertEqual(extra, [])


class FetcherTest(unittest.TestCase):
    def test_one_at_a_time_with_pause_between(self):
        f, waits = fetcher([Res(200, "a"), Res(200, "b")])
        self.assertEqual(f.get("u1"), "a")
        self.assertEqual(f.get("u2"), "b")
        self.assertEqual(waits, [1.5])  # 첫 요청 앞에는 쉬지 않는다, 그 뒤 delay×(1+rand)

    def test_429_waits_retry_after_then_backoff(self):
        f, waits = fetcher([Res(429, headers={"Retry-After": "7"}), Res(429), Res(200, "ok")])
        self.assertEqual(f.get("u"), "ok")
        self.assertEqual(waits, [7, 60])
        self.assertEqual(f.throttled, 2)

    def test_429_keeps_coming_then_blocked(self):
        f, waits = fetcher([Res(429)] * 4)
        with self.assertRaises(job.Blocked):
            f.get("u")
        self.assertEqual(waits, [30, 60, 120])

    def test_other_errors_retry_three_times(self):
        f, waits = fetcher([Res(500), Res(503), Res(502)])
        with self.assertRaises(RuntimeError) as ctx:
            f.get("u")
        self.assertNotIsInstance(ctx.exception, job.Blocked)
        self.assertEqual(waits, [2, 4])

    def test_retry_after_is_capped(self):
        self.assertEqual(job._retry_after("9999", 30), 300)
        self.assertEqual(job._retry_after("Wed, 21 Oct 2026 07:28:00 GMT", 30), 30)


class FetchPagesTest(unittest.TestCase):
    def test_blocked_stops_and_rest_count_as_missed(self):
        f, _ = fetcher([Res(200, "p1")] + [Res(429)] * 4)
        pages, failed, blocked = job.fetch_pages(f, [1, 2, 3])
        self.assertEqual(pages, {1: "p1"})
        self.assertEqual(failed, [2, 3])
        self.assertTrue(blocked)

    def test_one_bad_table_does_not_stop_others(self):
        f, _ = fetcher([Res(404), Res(404), Res(404), Res(200, "p2")])
        pages, failed, blocked = job.fetch_pages(f, [1, 2])
        self.assertEqual(pages, {2: "p2"})
        self.assertEqual(failed, [1])
        self.assertFalse(blocked)

    def test_time_budget_leaves_rest_for_next_run(self):
        ticks = iter([0, 0, 2000, 2000])  # 시작 · 1번 앞 확인 · 2번 앞 확인(예산 넘김)
        f, _ = fetcher([Res(200, "p1"), Res(200, "p2")], budget=1500, clock=lambda: next(ticks))
        pages, failed, blocked = job.fetch_pages(f, [1, 2, 3])
        self.assertEqual(sorted(pages), [1])
        self.assertEqual(failed, [2, 3])
        self.assertTrue(blocked)


if __name__ == "__main__":
    unittest.main()
