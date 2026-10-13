# -*- coding: utf-8 -*-
"""개봉일 0~1시 기준가 수집 + '일별 추이 값 = 0~1시 기준가' 검증. (현질 도우미 0단계)

넥슨 Top Price 팩은 "개봉일 0~1시 사이 기준가" 순위로 카드를 정한다. 데이터센터 일별 추이
(PlayerPriceGraph 의 json1)의 D 일 값이 바로 그 값이라면, 다음 날 추이 한 번으로 D 일 기준가를
얻을 수 있어 0시 수집이 필요 없다. 그걸 확인하는 작업이다.

MIDNIGHT_MODE=record (00:20 KST): 후보 풀 카드 중 표본 N 장의 현재가를 오늘 날짜로 저장한다.
    실제 수집 시각이 0~1시를 벗어나면(크론 지연) 저장하지 않는다 — 다른 시각 값을 0~1시 값으로
    착각하면 검증이 무의미하다.
MIDNIGHT_MODE=compare (12:20 KST): 어제(D) 0시대 기록을 지금 받은 추이의 D 일 값과 맞대어
    일치 비율을 출력한다. 추이의 마지막 칸은 '어제'다(2026-10-13 11시 실측: 마지막 칸 10.12).
    0시 직후에는 그 칸이 아직 안 붙었을 수 있어 낮에 비교한다.

환경변수: DATABASE_URL(필수), MIDNIGHT_MODE(record|compare), MIDNIGHT_SAMPLE(표본 수, 기본 200),
          MIDNIGHT_RETENTION_DAYS(기본 14)
"""
from __future__ import annotations

import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import List, Tuple

from sqlalchemy import text

from crawler_job import fetch_price_snapshot, make_engine, make_session

KST = timezone(timedelta(hours=9))


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        return default


def sample_cards(engine, limit: int) -> List[Tuple[int, int]]:
    """후보 풀에 든 카드에서 표본. 풀이 비었으면(상품 미등록) 빈 목록."""
    with engine.connect() as conn:
        return [tuple(r) for r in conn.execute(text(
            # spid 순으로 자르면 한 시즌 저강화 카드로 몰린다. 해시 순서로 고루 섞되,
            # 날마다 같은 표본이 뽑혀 이틀 치를 맞대어 볼 수 있게 한다.
            "SELECT spid, strong FROM (SELECT DISTINCT spid, strong FROM spend.pack_pool_card) c "
            "ORDER BY md5(spid::text || '/' || strong::text) LIMIT :n"
        ), {"n": limit})]


def compare_yesterday(engine, session, today) -> None:
    day = today - timedelta(days=1)
    with engine.connect() as conn:
        rows = [tuple(r) for r in conn.execute(text(
            "SELECT spid, strong, price FROM spend.price_snapshot "
            "WHERE price_date = :d AND source = 'midnight'"
        ), {"d": day})]
    if not rows:
        print(f"[midnight] {day} 0시 기록 없음 — 비교 생략")
        return
    same = diff = missing = 0
    samples = []
    for spid, strong, midnight in rows:
        snap = fetch_price_snapshot(session, spid, strong, end=day)
        graph = dict(snap.history).get(day)
        if graph is None:
            missing += 1
        elif graph == midnight:
            same += 1
        else:
            diff += 1
            if len(samples) < 10:
                samples.append(f"{spid}/{strong}강 0시 {midnight:,} vs 추이 {graph:,}")
        time.sleep(0.1)
    total = same + diff
    ratio = f"{same / total:.1%}" if total else "-"
    print(f"[midnight] {day} 비교: 일치 {same} · 불일치 {diff} · 추이 없음 {missing} → 일치율 {ratio}")
    for line in samples:
        print(f"[midnight]   불일치 예: {line}")


def record_today(engine, session, today, cards: List[Tuple[int, int]]) -> None:
    started = datetime.now(KST)
    if started.hour != 0:
        print(f"[midnight] 지금 {started:%H:%M} KST — 0~1시가 아니라 기록하지 않음")
        return
    rows = []
    for spid, strong in cards:
        if datetime.now(KST).hour != 0:
            print("[midnight] 1시를 넘겨 기록 중단")
            break
        snap = fetch_price_snapshot(session, spid, strong)
        if snap.current_price:
            rows.append({"spid": spid, "strong": strong, "price": snap.current_price})
        time.sleep(0.1)
    if not rows:
        print("[midnight] 기록할 값 없음")
        return
    import json

    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO spend.price_snapshot (price_date, spid, strong, price, source) "
            "SELECT :d, (e->>'spid')::bigint, (e->>'strong')::smallint, (e->>'price')::bigint, 'midnight' "
            "FROM jsonb_array_elements(CAST(:payload AS jsonb)) AS e "
            "ON CONFLICT (price_date, spid, strong) DO NOTHING"
        ), {"d": today, "payload": json.dumps(rows)})
    print(f"[midnight] {today} 0시대 기준가 {len(rows)}장 기록 ({started:%H:%M}~{datetime.now(KST):%H:%M} KST)")


def prune(engine, today, keep_days: int) -> None:
    with engine.begin() as conn:
        removed = conn.execute(text(
            "DELETE FROM spend.price_snapshot WHERE source = 'midnight' AND price_date < :cut"
        ), {"cut": today - timedelta(days=keep_days)}).rowcount
    if removed:
        print(f"[midnight] 보존 기한 지난 {removed}행 삭제")


def run() -> None:
    engine = make_engine()
    session = make_session()
    today = datetime.now(KST).date()
    if os.environ.get("MIDNIGHT_MODE") == "compare":
        compare_yesterday(engine, session, today)
        return
    cards = sample_cards(engine, _env_int("MIDNIGHT_SAMPLE", 200))
    if not cards:
        print("[midnight] 후보 풀 카드가 없습니다 — 콘솔에서 상품을 등록하면 다음 날부터 표본이 생깁니다")
        sys.exit(0)
    record_today(engine, session, today, cards)
    prune(engine, today, _env_int("MIDNIGHT_RETENTION_DAYS", 14))


if __name__ == "__main__":
    run()
