# -*- coding: utf-8 -*-
"""넥슨 확률표 수집 → spend.prob_table / prob_row, Top Price 후보 풀 초안 → spend.pack_pool.

FCO 매니저 '현질 도우미'의 원천 데이터. 하루 1회 돈다(넥슨이 목요일 점검 때 상품을 바꾼다).

1. 목록(iteminfo.nexon.com/probability/fco)을 받고, 표는 새 표·등록 상품에서 닿는 표 + 오래된 표 일부만
   한 번에 하나씩 받는다(Fetcher·plan_targets — 2026-10-29 넥슨 429 이후). 전부 받으려면 ITEMINFO_FORCE=1.
   Cloudflare 가 브라우저 헤더 없는 요청에 403 을 준다 → BROWSER_HEADERS 필수.
2. 표마다 해시를 비교해 **바뀐 표만** 행을 갈아 쓴다. 전량 16만 행을 매일 다시 쓰면
   Disk IO 예산을 태운다(2026-07 사고). 행은 JSON 문자열 하나로 바인드한다.
3. 카드 확정 행은 (시즌 코드, 선수명)으로 spid 를 찾는다. 이름이 유일하게 맞을 때만 채운다.
4. 운영자가 콘솔에 등록한 상품(spend.product)·이벤트 회차(spend.event)에서 닿는 Top Price 표만 후보 풀
   초안을 만든다. 등록돼 있으면 끔·미리보기·종료일 지남과 상관없이 대상이고, 콘솔에서 삭제하면 다음
   실행부터 대상에서 빠진다(2026-10-26 사용자 결정 — 판매를 내리는 건 운영자가 삭제로만 한다).
   확률표 자체(1~3)는 등록과 상관없이 새 표를 모두 받는다 — 콘솔이 그 표에서 sn 을 골라 등록하기 때문.
   조건 = 등급칸 클래스 · 제목의 강화 범위 · 최종 OVR 하한. 최종 OVR = 기본 OVR + 강화 보너스
   (2026-10-13 넥슨 최종 OVR 표 1,142장과 대조해 전부 일치). 카드 목록이 바뀌면 확정을 푼다 —
   운영자가 확정한 풀만 앱에 기댓값이 공개된다.

환경변수: DATABASE_URL(필수), ITEMINFO_LIMIT(테스트용 표 수 상한, 0=전체),
          ITEMINFO_ROTATE(새 표·등록 상품 표 말고 돌아가며 받을 옛 표 수, 기본 50),
          ITEMINFO_DELAY(요청 간격 최소 초, 기본 1 — 1~2배 무작위), ITEMINFO_BUDGET_SEC(수집 시간 상한, 기본 1500),
          ITEMINFO_FORCE=1(목록 전체를 받고 해시 무시하고 전부 다시 쓰기),
          ITEMINFO_LOCAL_DIR(넥슨 대신 저장해 둔 index.html·s<sn>.html 폴더에서 읽기 — 오프라인 검증용)
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import random
import sys
import time
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


class Blocked(RuntimeError):
    """넥슨이 429 를 계속 준다 — 이번 실행의 수집을 멈춘다."""


class Fetcher:
    """넥슨에 한 번에 하나씩, 요청 사이 [delay]~2×[delay] 초를 쉬며 묻는다.

    2026-10-29 부터 넥슨(Cloudflare)이 GitHub 러너의 4개 동시 요청에 429 를 줬다(297개 중 21개만 받음).
    429 는 Retry-After(없으면 30·60·120초)만큼 쉬고 다시 묻고, 그래도 막히면 [Blocked] 로 멈춘다 —
    막힌 채 계속 두드리면 더 오래 막힌다. 그 밖의 오류는 2·4초 쉬고 세 번까지 묻는다.
    [budget] 초가 지나면 남은 표는 다음 실행에 넘긴다.
    """

    BACKOFF = (30, 60, 120)

    def __init__(self, session=None, delay: float = 1.0, budget: float = 1500,
                 sleep=time.sleep, clock=time.monotonic, rand=random.random):
        self.session = session or make_session()
        self.delay, self.budget = delay, budget
        self.sleep, self.clock, self.rand = sleep, clock, rand
        self.started = clock()
        self.first = True
        self.throttled = 0  # 429 를 받은 횟수(로그용)

    def out_of_time(self) -> bool:
        return self.clock() - self.started > self.budget

    def _pause(self) -> None:
        if not self.first:
            self.sleep(self.delay * (1 + self.rand()))
        self.first = False

    def get(self, url: str) -> str:
        self._pause()
        backoff = 0
        last: Optional[str] = None
        attempt = 0
        while True:
            try:
                res = self.session.get(url, timeout=60)
            except requests.RequestException as exc:
                res, last = None, str(exc)
            if res is not None and res.status_code == 200:
                return res.text
            if res is not None and res.status_code == 429:
                self.throttled += 1
                if backoff >= len(self.BACKOFF):
                    raise Blocked(f"{url}: HTTP 429 가 계속됩니다")
                wait = _retry_after(res.headers.get("Retry-After"), self.BACKOFF[backoff])
                backoff += 1
                log.warning("429 — %s초 쉬고 다시 묻습니다: %s", wait, url)
                self.sleep(wait)
                continue
            if res is not None:
                last = f"HTTP {res.status_code}"
            attempt += 1
            if attempt >= 3:
                raise RuntimeError(f"{url}: {last}")
            self.sleep(2 * attempt)


def _retry_after(raw: Optional[str], default: int) -> int:
    """Retry-After 초(숫자만 믿는다). 없거나 이상하면 [default], 너무 길면 5분으로 자른다."""
    try:
        return max(1, min(300, int(raw)))
    except (TypeError, ValueError):
        return default


def plan_targets(index: Sequence[Tuple[int, str]], fetched_at: Dict[int, object], reach: set,
                 rotate: int) -> Tuple[List[int], List[int]]:
    """받을 표를 고른다 → (꼭 받을 표, 돌아가며 받을 표).

    꼭 받을 표 = 목록에 새로 생긴 표(DB 에 없음) + 콘솔에 등록된 상품·회차에서 닿는 표(앱이 쓰는 표).
    나머지는 가장 오래전에 받은 [rotate] 개만 받는다 — 평소엔 거의 안 바뀌는데 매번 300개 가까이
    물으면 넥슨이 막는다. 목록에서 빠진 표는 묻지 않는다.
    """
    sns = [sn for sn, _ in index]
    new = [sn for sn in sns if sn not in fetched_at]
    reached = [sn for sn in sns if sn in reach and sn in fetched_at]
    must = set(new) | set(reached)
    rest = sorted((sn for sn in sns if sn not in must), key=lambda sn: (str(fetched_at[sn]), sn))
    return new + reached, rest[:max(0, rotate)]


def reachable_sns(engine) -> set:
    """콘솔에 등록된 상품·회차에서 하위 상자를 따라 닿는 표 전부(종류 무관)."""
    with engine.connect() as conn:
        return {int(sn) for (sn,) in conn.execute(text(REACH_CTE + "SELECT sn FROM reach WHERE sn IS NOT NULL"))}


def fetched_times(engine) -> Dict[int, object]:
    with engine.connect() as conn:
        return {int(sn): at for sn, at in conn.execute(text("SELECT sn, fetched_at FROM spend.prob_table"))}


def read_local(folder: str, limit: int) -> Tuple[List[Tuple[int, str]], Dict[int, str], List[int]]:
    import pathlib

    root = pathlib.Path(folder)
    index = ii.parse_index((root / "index.html").read_text(encoding="utf-8"))
    targets = index[:limit] if limit else index
    pages = {sn: (root / f"s{sn}.html").read_text(encoding="utf-8")
             for sn, _ in targets if (root / f"s{sn}.html").exists()}
    return index, pages, [sn for sn, _ in targets if sn not in pages]


def fetch_pages(fetcher: Fetcher, sns: Sequence[int]) -> Tuple[Dict[int, str], List[int], bool]:
    """[sns] 를 차례로 받는다 → (받은 페이지, 못 받은 sn, 막혀서 멈췄는지). 멈춘 뒤의 sn 도 못 받은 쪽에 넣는다."""
    pages: Dict[int, str] = {}
    failed: List[int] = []
    for i, sn in enumerate(sns):
        if fetcher.out_of_time():
            log.warning("시간 예산을 넘겨 남은 표 %s개는 다음 실행에 받습니다", len(sns) - i)
            return pages, failed + list(sns[i:]), True
        try:
            pages[sn] = fetcher.get(f"{BASE}?sn={sn}")
        except Blocked as exc:
            log.warning("넥슨이 계속 막아 수집을 멈춥니다(남은 %s개): %s", len(sns) - i, exc)
            return pages, failed + list(sns[i:]), True
        except Exception as exc:  # noqa: BLE001 — 실패 sn 은 다음 실행에 다시 받는다
            log.warning("sn=%s 실패: %s", sn, exc)
            failed.append(sn)
    return pages, failed, False


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
    rotate = _env_int("ITEMINFO_ROTATE", 50)
    delay = float(os.environ.get("ITEMINFO_DELAY") or 1.0)
    budget = _env_int("ITEMINFO_BUDGET_SEC", 1500)
    force = os.environ.get("ITEMINFO_FORCE") == "1"

    engine = make_engine()
    local = os.environ.get("ITEMINFO_LOCAL_DIR")
    must: List[int] = []
    blocked = False
    if local:
        index, pages, failed = read_local(local, limit)
        must = [sn for sn, _ in (index[:limit] if limit else index)]
    else:
        fetcher = Fetcher(delay=delay, budget=budget)
        try:
            index = ii.parse_index(fetcher.get(BASE))
        except Blocked as exc:
            raise SystemExit(f"확률표 목록부터 막혔습니다 — 다음 실행에 다시 받습니다: {exc}")
        if not index:
            raise SystemExit("확률표 목록이 비었습니다 — 넥슨 페이지 구조가 바뀌었는지 확인하세요.")
        if force:
            must, extra = [sn for sn, _ in index], []
        else:
            must, extra = plan_targets(index, fetched_times(engine), reachable_sns(engine), rotate)
        if limit:
            must, extra = must[:limit], extra[:max(0, limit - len(must))]
        print(f"[iteminfo] 받을 표: 새 표·등록 상품에서 닿는 표 {len(must)} + 돌아가며 {len(extra)} "
              f"(목록 {len(index)}, 동시 1개·간격 {delay:g}~{2 * delay:g}초)")
        pages, failed, blocked = fetch_pages(fetcher, must + extra)
        if fetcher.throttled:
            print(f"[iteminfo] 429 {fetcher.throttled}번")
    titles = ii.TitleIndex(index)
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

    missed = [sn for sn in must if sn not in pages]
    print(f"[iteminfo] 목록 {len(index)} · 받음 {len(pages)} · 실패 {failed or '-'}"
          f"{' · 막혀서 멈춤' if blocked else ''}")
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
    # 실패 = 꼭 받을 표(새 표·앱이 쓰는 표)를 못 받았거나 해석이 깨졌을 때만. 돌아가며 받는 옛 표는
    # 다음 실행이 이어받으므로 실패로 치지 않는다 — 빨간 표시가 "새 상품을 놓쳤다"는 신호가 되게.
    if missed:
        print(f"[iteminfo] 꼭 받을 표 중 못 받음 {len(missed)}개: {missed}")
    if parse_failed or missed:
        sys.exit(1)


if __name__ == "__main__":
    run()
