# -*- coding: utf-8 -*-
"""넥슨 확률표 수집 → spend.prob_table / prob_row, Top Price 후보 풀 초안 → spend.pack_pool.

FCO 매니저 '현질 도우미'의 원천 데이터. 하루 1회 돈다(넥슨이 목요일 점검 때 상품을 바꾼다).

1. 목록(iteminfo.nexon.com/probability/fco)과 표 301개 안팎을 받는다.
   Cloudflare 가 브라우저 헤더 없는 요청에 403 을 준다 → BROWSER_HEADERS 필수.
2. 표마다 해시를 비교해 **바뀐 표만** 행을 갈아 쓴다. 전량 16만 행을 매일 다시 쓰면
   Disk IO 예산을 태운다(2026-07 사고). 행은 JSON 문자열 하나로 바인드한다.
3. 카드 확정 행은 (시즌 코드, 선수명)으로 spid 를 찾는다. 이름이 유일하게 맞을 때만 채운다.
4. 운영자가 콘솔에 등록한 상품(spend.product)·이벤트 회차(spend.event)에서 닿는 Top Price 표만 후보 풀
   초안을 만든다. 등록돼 있으면 끔·미리보기·종료일 지남과 상관없이 대상이고, 콘솔에서 삭제하면 다음
   실행부터 대상에서 빠진다(2026-10-26 사용자 결정 — 판매를 내리는 건 운영자가 삭제로만 한다).
   확률표 자체(1~3)는 등록과 상관없이 넥슨 목록 전체를 받는다 — 콘솔이 그 표에서 sn 을 골라 등록하기 때문.
   조건 = 등급칸 클래스 · 제목의 강화 범위 · 최종 OVR 하한. 최종 OVR = 기본 OVR + 강화 보너스
   (2026-10-13 넥슨 최종 OVR 표 1,142장과 대조해 전부 일치). 카드 목록이 바뀌면 확정을 푼다 —
   운영자가 확정한 풀만 앱에 기댓값이 공개된다.

환경변수: DATABASE_URL(필수), ITEMINFO_LIMIT(테스트용 표 수 상한, 0=전체),
          ITEMINFO_WORKERS(기본 4), ITEMINFO_FORCE=1(해시 무시하고 전부 다시 쓰기),
          ITEMINFO_LOCAL_DIR(넥슨 대신 저장해 둔 index.html·s<sn>.html 폴더에서 읽기 — 오프라인 검증용)
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Sequence, Tuple

import requests
from sqlalchemy import text

import iteminfo as ii
from crawler_job import make_engine

log = logging.getLogger("iteminfo")

BASE = "https://iteminfo.nexon.com/probability/fco"
BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9",
}
ROW_CHUNK = 5000

# 백엔드 players/crud.py ENHANCEMENT_OVR_BONUS 와 같아야 한다.
ENHANCEMENT_OVR_BONUS = {1: 0, 2: 1, 3: 2, 4: 4, 5: 6, 6: 8, 7: 11, 8: 15, 9: 17, 10: 19, 11: 21, 12: 24, 13: 27}


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        return default


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(BROWSER_HEADERS)
    return s


def fetch(session: requests.Session, url: str, retries: int = 3) -> str:
    last: Optional[Exception] = None
    for attempt in range(retries):
        try:
            res = session.get(url, timeout=60)
            if res.status_code == 200:
                return res.text
            last = RuntimeError(f"HTTP {res.status_code}")
        except requests.RequestException as exc:
            last = exc
        time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"{url}: {last}")


def read_local(folder: str, limit: int) -> Tuple[List[Tuple[int, str]], Dict[int, str], List[int]]:
    import pathlib

    root = pathlib.Path(folder)
    index = ii.parse_index((root / "index.html").read_text(encoding="utf-8"))
    targets = index[:limit] if limit else index
    pages = {sn: (root / f"s{sn}.html").read_text(encoding="utf-8")
             for sn, _ in targets if (root / f"s{sn}.html").exists()}
    return index, pages, [sn for sn, _ in targets if sn not in pages]


def fetch_all(limit: int, workers: int) -> Tuple[List[Tuple[int, str]], Dict[int, str], List[int]]:
    session = make_session()
    index = ii.parse_index(fetch(session, BASE))
    if not index:
        raise SystemExit("확률표 목록이 비었습니다 — 넥슨 페이지 구조가 바뀌었는지 확인하세요.")
    targets = index[:limit] if limit else index
    pages: Dict[int, str] = {}
    failed: List[int] = []

    def one(sn: int):
        try:
            return sn, fetch(make_session(), f"{BASE}?sn={sn}")
        except Exception as exc:  # noqa: BLE001 — 실패 sn 은 다음 날 다시 받는다
            log.warning("sn=%s 실패: %s", sn, exc)
            return sn, None

    with ThreadPoolExecutor(max_workers=workers) as pool:
        for sn, page in pool.map(lambda item: one(item[0]), targets):
            if page is None:
                failed.append(sn)
            else:
                pages[sn] = page
    return index, pages, failed


# ---- DB 쓰기 ---------------------------------------------------------------


def _stored_hashes(conn) -> Dict[int, str]:
    return {sn: h for sn, h in conn.execute(text("SELECT sn, content_hash FROM spend.prob_table"))}


_ROW_COLUMNS = (
    "row_no", "raw_grade", "raw_item", "raw_probability", "probability", "quantity", "row_kind",
    "season_code", "player_name", "strong", "pool_key", "top_rank", "sub_sn", "bp_amount",
)
_ROW_RECORD = (
    "row_no integer, raw_grade text, raw_item text, raw_probability text, probability numeric, "
    "quantity integer, row_kind text, season_code text, player_name text, strong smallint, "
    "pool_key text, top_rank integer, sub_sn integer, bp_amount bigint"
)


def write_table(engine, table: ii.ParsedTable) -> None:
    """표 하나를 한 트랜잭션으로 갈아 쓴다. 커넥션은 표마다 새로 꺼낸다(유휴 SSL 끊김 대비)."""
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO spend.prob_table (sn, title, table_kind, draw_mode, period, row_count, "
            "content_hash, fetched_at, changed_at) "
            "VALUES (:sn, :title, :kind, :mode, :period, :n, :hash, now(), now()) "
            "ON CONFLICT (sn) DO UPDATE SET title = EXCLUDED.title, table_kind = EXCLUDED.table_kind, "
            "draw_mode = EXCLUDED.draw_mode, period = EXCLUDED.period, row_count = EXCLUDED.row_count, "
            "content_hash = EXCLUDED.content_hash, fetched_at = now(), changed_at = now()"
        ), {"sn": table.sn, "title": table.title, "kind": table.table_kind, "mode": table.draw_mode,
            "period": table.period, "n": len(table.rows), "hash": table.content_hash})
        conn.execute(text("DELETE FROM spend.prob_row WHERE sn = :sn"), {"sn": table.sn})
        for start in range(0, len(table.rows), ROW_CHUNK):
            chunk = table.rows[start:start + ROW_CHUNK]
            payload = json.dumps([{c: getattr(r, c) for c in _ROW_COLUMNS} for r in chunk], ensure_ascii=False)
            conn.execute(text(
                f"INSERT INTO spend.prob_row (sn, {', '.join(_ROW_COLUMNS)}) "
                f"SELECT :sn, {', '.join(_ROW_COLUMNS)} "
                f"FROM jsonb_to_recordset(CAST(:payload AS jsonb)) AS r({_ROW_RECORD})"
            ), {"sn": table.sn, "payload": payload})


def touch_unchanged(engine, sns: Sequence[int]) -> None:
    if not sns:
        return
    with engine.begin() as conn:
        conn.execute(text(
            "UPDATE spend.prob_table SET fetched_at = now() WHERE sn = ANY(CAST(:sns AS integer[]))"
        ), {"sns": ii_array(sns)})


def ii_array(values: Sequence[int]) -> str:
    return "{" + ",".join(str(int(v)) for v in values) + "}"


# 시즌 코드 매칭: seasonid.class_name = 'CODE (정식 이름)'. 넥슨 등급칸은 코드('25UCL')나
# 정식 이름 압축형('26HEROES' ← '26HR (26 Heroes)')을 쓴다. 공백·점을 지우고 대문자로 비교한다.
# ⚠️ '[[:space:]]' 는 쓰지 말 것 — sqlalchemy text() 가 ':space' 를 바인드 변수로 읽는다.
_NORM_SQL = r"upper(regexp_replace({}, '[\s.]', '', 'g'))"
_SEASON_KEYS_SQL = (
    "SELECT season_id, " + _NORM_SQL.format("split_part(class_name, ' (', 1)") + " AS k FROM player.seasonid "
    "UNION SELECT season_id, " + _NORM_SQL.format("substring(class_name from '\\((.*)\\)$')") + " FROM player.seasonid"
)


def resolve_card_spids(engine, sns: Sequence[int]) -> Tuple[int, int]:
    """바뀐 표의 카드 행에 spid 를 채운다. (시즌, 이름)이 정확히 한 장일 때만."""
    if not sns:
        return 0, 0
    with engine.begin() as conn:
        updated = conn.execute(text(
            f"WITH seasons AS ({_SEASON_KEYS_SQL}), "
            "cand AS ("
            "  SELECT r.sn, r.row_no, min(p.spid) AS spid, count(DISTINCT p.spid) AS n "
            "  FROM spend.prob_row r "
            "  JOIN seasons s ON s.k = " + _NORM_SQL.format("r.season_code") + " "
            "  JOIN player.players p ON p.season_id = s.season_id AND p.name = r.player_name "
            "  WHERE r.sn = ANY(CAST(:sns AS integer[])) AND r.row_kind = 'card' "
            "  GROUP BY r.sn, r.row_no) "
            "UPDATE spend.prob_row r SET spid = cand.spid FROM cand "
            "WHERE r.sn = cand.sn AND r.row_no = cand.row_no AND cand.n = 1"
        ), {"sns": ii_array(sns)}).rowcount
        missing = conn.execute(text(
            "SELECT count(*) FROM spend.prob_row WHERE sn = ANY(CAST(:sns AS integer[])) "
            "AND row_kind = 'card' AND spid IS NULL"
        ), {"sns": ii_array(sns)}).scalar_one()
    return updated, missing


# ---- Top Price 후보 풀 --------------------------------------------------------


# 활성 상품에서 닿는 표 전체(reach). 시작점 = 상품의 확률표(sn) + 보상 표들
# (SSS 절구 칸 choice_sns, 교환소 exchange[].sn). config 쪽을 빼면 절구·교환소 보상
# 풀이 안 만들어진다. price_snapshot_job 도 같은 범위의 카드 기준가를 모은다.
# 2026-10-19 부터 규칙은 이벤트 회차(spend.event, event_kind = crystal_shop/key_shop/sss_board)
# config 에 있다(절구 칸은 special/normal.cells). 옛 상품 config 경로도 남겨 둔다(지금은 비어 있음).
# 2026-10-26 범용 회차 flow: 보상 표는 games[].cells[].choice_sns(판)·games[].sn(확률표 뽑기)·
# games[].tiers[].outcomes[].sn(등급 사다리)에 있다. 미리보기 회차(is_preview)도 켜져 있으면 포함한다 —
# 운영자가 공개 전에 폰에서 시험하려면 풀이 먼저 있어야 한다.
# 대상 = 콘솔에 등록된 행 전부(끔·미리보기·종료일 지남 포함). 콘솔에서 삭제하면 행이 없어져 빠진다
# (2026-10-26 사용자 결정). 꺼 둔 회차도 넣는 건, 복제한 회차를 끈 채 sn 을 고치는 동안 풀이 먼저
# 만들어져 있어야 미리보기로 켜자마자 열리기 때문이다. 옛 현질 도우미 event 행(쓸돌·쿠폰)은 뺀다.
REACH_CTE = (
    "WITH RECURSIVE live AS ("
    "  SELECT * FROM spend.product), "
    "live_event AS ("
    "  SELECT * FROM spend.event "
    "  WHERE event_kind IN ('crystal_shop', 'key_shop', 'sss_board', 'flow')), "
    "reach(sn) AS ("
    "  SELECT sn FROM live WHERE sn IS NOT NULL "
    "  UNION SELECT (x->>'sn')::int FROM live_event, "
    "    jsonb_array_elements(coalesce(live_event.config->'exchange', '[]'::jsonb)) AS x "
    "  UNION SELECT c.sn::int FROM live_event, "
    "    jsonb_array_elements(coalesce(live_event.config->'special'->'cells', '[]'::jsonb) "
    "      || coalesce(live_event.config->'normal'->'cells', '[]'::jsonb)) AS cell, "
    "    jsonb_array_elements_text(cell->'choice_sns') AS c(sn) "
    "  UNION SELECT c.sn::int FROM live_event, "
    "    jsonb_array_elements(coalesce(live_event.config->'games', '[]'::jsonb)) AS g, "
    "    jsonb_array_elements(coalesce(g->'cells', '[]'::jsonb)) AS cell, "
    "    jsonb_array_elements_text(cell->'choice_sns') AS c(sn) "
    "  UNION SELECT (g->>'sn')::int FROM live_event, "
    "    jsonb_array_elements(coalesce(live_event.config->'games', '[]'::jsonb)) AS g "
    "  UNION SELECT (o->>'sn')::int FROM live_event, "
    "    jsonb_array_elements(coalesce(live_event.config->'games', '[]'::jsonb)) AS g, "
    "    jsonb_array_elements(coalesce(g->'tiers', '[]'::jsonb)) AS t, "
    "    jsonb_array_elements(coalesce(t->'outcomes', '[]'::jsonb)) AS o "
    # script 게임: 줄 수 있는 상자(sns)와 턴 안에서 여는 확률표(tables). 2026-10-29 열쇠 상점에서 빠져 있던 것.
    "  UNION SELECT s.sn::int FROM live_event, "
    "    jsonb_array_elements(coalesce(live_event.config->'games', '[]'::jsonb)) AS g, "
    "    jsonb_array_elements_text(coalesce(g->'sns', '[]'::jsonb) || coalesce(g->'tables', '[]'::jsonb)) AS s(sn) "
    "  UNION SELECT c.sn::int FROM live, "
    "    jsonb_array_elements(coalesce(live.config->'cells', '[]'::jsonb)) AS cell, "
    "    jsonb_array_elements_text(cell->'choice_sns') AS c(sn) "
    "  UNION SELECT (x->>'sn')::int FROM live, "
    "    jsonb_array_elements(coalesce(live.config->'exchange', '[]'::jsonb)) AS x "
    "  UNION SELECT r.sub_sn FROM spend.prob_row r JOIN reach ON r.sn = reach.sn "
    "    WHERE r.sub_sn IS NOT NULL) "
)


def reachable_top_price(engine) -> List[int]:
    """등록된 상품·회차에서 하위 상자를 따라 닿는 Top Price 표."""
    with engine.connect() as conn:
        return [sn for (sn,) in conn.execute(text(
            REACH_CTE + "SELECT t.sn FROM reach JOIN spend.prob_table t ON t.sn = reach.sn "
            "WHERE t.table_kind = 'top_price' ORDER BY t.sn"
        ))]


def _season_ids(conn, codes: Sequence[str]) -> Tuple[List[int], List[str]]:
    keys = {ii.norm_code(c): c for c in codes}
    found: Dict[str, List[int]] = {}
    for season_id, k in conn.execute(text(_SEASON_KEYS_SQL)):
        if k in keys:
            found.setdefault(k, []).append(season_id)
    ids = sorted({sid for v in found.values() for sid in v})
    return ids, [keys[k] for k in keys if k not in found]


def sync_pool(engine, sn: int, spec: ii.PoolSpec) -> dict:
    """조건으로 카드 목록을 만들어, 저장된 것과 다르면 갈아 쓰고 확정을 푼다."""
    with engine.begin() as conn:
        season_ids, unresolved = _season_ids(conn, spec.class_codes)
        bonus = " ".join(f"WHEN {s} THEN {b}" for s, b in ENHANCEMENT_OVR_BONUS.items())
        cards = [tuple(r) for r in conn.execute(text(
            "SELECT p.spid, s.strong FROM player.players p "
            "CROSS JOIN unnest(CAST(:strongs AS smallint[])) AS s(strong) "
            "WHERE p.season_id = ANY(CAST(:seasons AS smallint[])) AND NOT p.hidden "
            f"AND (CAST(:min_ovr AS integer) IS NULL OR p.main_ovr + (CASE s.strong {bonus} END) >= CAST(:min_ovr AS integer)) "
            "ORDER BY 1, 2"
        ), {"strongs": ii_array(spec.strongs), "seasons": ii_array(season_ids), "min_ovr": spec.min_ovr})]
        # 조건으로 만든 목록의 해시. 운영자가 콘솔에서 카드를 뺀 풀은 저장 목록이 이것과
        # 달라도 정상이다 — 지난번 자동 목록(auto_hash)과 비교해 **조건 쪽이 바뀌었을 때만**
        # (신규 카드 출시·숨김 등) 갈아 쓰고 확정을 푼다.
        auto_hash = hashlib.sha256(json.dumps(cards).encode()).hexdigest()
        rule = {
            "class_codes": spec.class_codes, "season_ids": season_ids, "unresolved_codes": unresolved,
            "strongs": spec.strongs, "min_ovr": spec.min_ovr, "top_n": spec.top_n,
            "eligible": len(cards), "auto_hash": auto_hash,
        }
        previous = conn.execute(text(
            "SELECT rule->>'auto_hash' FROM spend.pack_pool WHERE sn = :sn AND pool_key = ''"
        ), {"sn": sn}).scalar()
        changed = previous != auto_hash
        # rule 은 병합한다(||) — 운영자 메모 등 콘솔이 넣은 키를 지우지 않는다.
        conn.execute(text(
            "INSERT INTO spend.pack_pool (sn, pool_key, rule) VALUES (:sn, '', CAST(:rule AS jsonb)) "
            "ON CONFLICT (sn, pool_key) DO UPDATE SET rule = spend.pack_pool.rule || EXCLUDED.rule, "
            "updated_at = now(), "
            "confirmed_at = CASE WHEN :changed THEN NULL ELSE spend.pack_pool.confirmed_at END"
        ), {"sn": sn, "rule": json.dumps(rule, ensure_ascii=False), "changed": changed})
        if changed:
            conn.execute(text("DELETE FROM spend.pack_pool_card WHERE sn = :sn AND pool_key = ''"), {"sn": sn})
            for start in range(0, len(cards), ROW_CHUNK):
                conn.execute(text(
                    "INSERT INTO spend.pack_pool_card (sn, pool_key, spid, strong) "
                    "SELECT :sn, '', (e->>0)::bigint, (e->>1)::smallint "
                    "FROM jsonb_array_elements(CAST(:payload AS jsonb)) AS e"
                ), {"sn": sn, "payload": json.dumps(cards[start:start + ROW_CHUNK])})
    return {"sn": sn, "changed": changed, **{k: rule[k] for k in ("eligible", "top_n", "unresolved_codes")}}


def run() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    limit = _env_int("ITEMINFO_LIMIT", 0)
    workers = _env_int("ITEMINFO_WORKERS", 4)
    force = os.environ.get("ITEMINFO_FORCE") == "1"

    local = os.environ.get("ITEMINFO_LOCAL_DIR")
    index, pages, failed = read_local(local, limit) if local else fetch_all(limit, workers)
    titles = ii.TitleIndex(index)
    engine = make_engine()
    with engine.connect() as conn:
        stored = _stored_hashes(conn)

    changed: List[int] = []
    unchanged: List[int] = []
    parse_failed: List[Tuple[int, str]] = []
    ambiguous: List[str] = []
    for sn, page in sorted(pages.items()):
        try:
            table = ii.classify(ii.parse_page(sn, page), titles)
        except Exception as exc:  # noqa: BLE001 — 한 표가 깨져도 나머지는 쓴다
            parse_failed.append((sn, str(exc)))
            continue
        ambiguous.extend(f"{sn}: {a}" for a in table.ambiguous_refs)
        if not force and stored.get(sn) == table.content_hash:
            unchanged.append(sn)
            continue
        write_table(engine, table)
        changed.append(sn)
    touch_unchanged(engine, unchanged)
    resolved, unresolved_cards = resolve_card_spids(engine, changed)

    pools = []
    with engine.connect() as conn:
        specs = {}
        for sn in reachable_top_price(engine):
            title = conn.execute(text("SELECT title FROM spend.prob_table WHERE sn = :sn"), {"sn": sn}).scalar_one()
            grades = [g for (g,) in conn.execute(text(
                "SELECT DISTINCT raw_grade FROM spend.prob_row WHERE sn = :sn"), {"sn": sn})]
            rows = conn.execute(text("SELECT count(*) FROM spend.prob_row WHERE sn = :sn"), {"sn": sn}).scalar_one()
            spec = ii.pool_spec(title, sorted(grades), rows)
            if spec:
                specs[sn] = spec
    for sn, spec in specs.items():
        pools.append(sync_pool(engine, sn, spec))

    print(f"[iteminfo] 목록 {len(index)} · 받음 {len(pages)} · 실패 {failed or '-'}")
    print(f"[iteminfo] 바뀐 표 {len(changed)} · 그대로 {len(unchanged)} · 해석 실패 {parse_failed or '-'}")
    print(f"[iteminfo] 카드 spid 채움 {resolved} · 못 찾음 {unresolved_cards}")
    for line in ambiguous:
        print(f"[iteminfo] 같은 제목 → 형제 번호로 고름: {line}")
    for p in pools:
        flag = []
        if p["changed"]:
            flag.append("카드 바뀜 → 확정 해제")
        if p["eligible"] < p["top_n"]:
            flag.append(f"후보 {p['eligible']} < TOP {p['top_n']}")
        if p["unresolved_codes"]:
            flag.append(f"모르는 클래스 {p['unresolved_codes']}")
        print(f"[iteminfo] 풀 {p['sn']}: 후보 {p['eligible']}장 / TOP {p['top_n']} {' · '.join(flag)}")
    if parse_failed or len(pages) < len(index[:limit] if limit else index) // 2:
        sys.exit(1)


if __name__ == "__main__":
    run()
