# -*- coding: utf-8 -*-
"""현질 도우미 개봉일 기준가 → spend.price_snapshot (source='datacenter_daily').

넥슨 Top Price 팩은 "개봉일 0~1시 기준가" 순위로 카드를 정한다. 데이터센터 일별 추이의
D 일 값이 그 기준가와 같다면(0단계 — midnight_price_job.py 로 검증 중), 다음 날 추이 한 번으로
D 일 기준가를 얻는다. 이 작업은 그 가정 위에서 어제(D) 값을 모은다.
⚠️ 0단계 일치율이 확인되기 전에는 스케줄을 켜지 않는다(워크플로 price-snapshot.yml 참고).

대상 = 활성 상품에서 닿는 표의 카드 전부:
  - Top Price 후보 풀 카드(spend.pack_pool_card, 확정 여부와 무관 — 순위를 매기려면 다 필요)
  - 카드 확정 행(spend.prob_row row_kind='card', spid 를 찾은 것)

환경변수: DATABASE_URL(필수), SNAPSHOT_WORKERS(기본 8), SNAPSHOT_LIMIT(테스트용, 0=전체),
          SNAPSHOT_RETENTION_DAYS(기본 7)
"""
from __future__ import annotations

import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

from sqlalchemy import text

from crawler_job import fetch_price_snapshot, make_engine, make_session
from iteminfo_job import REACH_CTE

KST = timezone(timedelta(hours=9))
CHUNK = 5000


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        return default


def target_cards(engine) -> List[Tuple[int, int]]:
    with engine.connect() as conn:
        return [tuple(r) for r in conn.execute(text(
            REACH_CTE
            + "SELECT spid, strong FROM spend.pack_pool_card c JOIN reach ON reach.sn = c.sn "
            "UNION SELECT spid, strong FROM spend.prob_row r JOIN reach ON reach.sn = r.sn "
            "  WHERE r.row_kind = 'card' AND r.spid IS NOT NULL "
            "ORDER BY 1, 2"
        ))]


def _one(card: Tuple[int, int], day, retries: int = 3) -> Optional[dict]:
    spid, strong = card
    session = make_session()
    for attempt in range(retries):
        try:
            snap = fetch_price_snapshot(session, spid, strong, end=day)
            price = dict(snap.history).get(day)
            # 0 = 거래 불가. 기준가가 없는 카드로 둔다(0 으로 넣으면 순위가 틀린다).
            return {"spid": spid, "strong": strong, "price": price} if price else None
        except Exception:  # noqa: BLE001 — 재시도 후 포기, 그 카드는 기준가 없음으로 남는다
            time.sleep(1.5 * (attempt + 1))
    return None


def write(engine, day, rows: List[dict]) -> None:
    for start in range(0, len(rows), CHUNK):
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO spend.price_snapshot (price_date, spid, strong, price, source) "
                "SELECT :d, (e->>'spid')::bigint, (e->>'strong')::smallint, (e->>'price')::bigint, "
                "'datacenter_daily' FROM jsonb_array_elements(CAST(:payload AS jsonb)) AS e "
                # 같은 날 다시 돌면 먼저 저장한 값을 둔다(크롤러 롤에 UPDATE 권한이 없다).
                "ON CONFLICT (price_date, spid, strong) DO NOTHING"
            ), {"d": day, "payload": json.dumps(rows[start:start + CHUNK])})


def run() -> None:
    engine = make_engine()
    day = datetime.now(KST).date() - timedelta(days=1)
    cards = target_cards(engine)
    limit = _env_int("SNAPSHOT_LIMIT", 0)
    if limit:
        cards = cards[:limit]
    if not cards:
        print("[snapshot] 대상 카드가 없습니다 — 콘솔에 상품이 등록돼 있는지 확인하세요")
        return
    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=_env_int("SNAPSHOT_WORKERS", 8)) as pool:
        rows = [r for r in pool.map(lambda c: _one(c, day), cards) if r]
    write(engine, day, rows)
    with engine.begin() as conn:
        removed = conn.execute(text(
            "DELETE FROM spend.price_snapshot WHERE source = 'datacenter_daily' AND price_date < :cut"
        ), {"cut": day - timedelta(days=_env_int("SNAPSHOT_RETENTION_DAYS", 7))}).rowcount
    missing = len(cards) - len(rows)
    print(f"[snapshot] {day} 기준가 {len(rows)}/{len(cards)}장 저장 · 없음 {missing} · "
          f"오래된 {removed}행 삭제 · {time.monotonic() - started:.0f}초")
    # 절반 넘게 못 받았으면 넥슨 차단·구조 변경일 수 있다 — 실패로 알린다.
    if len(rows) < len(cards) // 2:
        sys.exit(1)


if __name__ == "__main__":
    run()
