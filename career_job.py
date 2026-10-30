"""
FC Online 선수 "클럽 경력" 주간 갱신 배치 — 공개 저장소용 자기완결형 스크립트.

앱의 선수 디테일 "클럽 경력" 탭은 백엔드 GET /players/{id} 가 player.player_career 를
그대로 읽어 내려준다(앱·서버 어디에도 캐시 없음). 그래서 이 배치가 그 테이블만 갱신하면
앱/서버 배포 없이 다음 화면 진입부터 바로 반영된다. 리그/팀/현역 소속 필터가 쓰는 파생
테이블 player.player_career_clubs 도 **바뀐 선수만** 같은 트랜잭션에서 다시 만든다.

넥슨은 클럽 경력을 목요일 정기점검에 묶어 일괄 반영한다(대략 월 1회, 불규칙 — 예:
2025-07-24 1,821명, 2026-08-20 4,496명, 사전 안내 공지가 1주 전쯤 뜬다). 공지 형식에
기대지 않고 매주 금요일 전량을 다시 긁어 비교한다. 바뀐 선수만 write 하므로 갱신이
없는 주는 write 0 이다.

클럽 경력은 카드(spid)가 아니라 실존 선수(pid = spid % 1000000)의 속성이다(시즌이
달라도 같음 — 2026-10 이적 선수 36명 최소/최대 시즌 카드 교차 확인). 그래서 pid 당
대표 spid(MIN) 1장만 요청한다(~5.1만 요청, 카드 수의 약 절반). 대표는 숨긴 카드(hidden)를
빼고 고른다 — 2026-10-30 넥슨이 지난 시즌 LIVE 카드를 300 에서 325 로 옮기자 MIN 이 오류
페이지로 넘어가는 300 카드를 골라 실패율 상한에 걸려 중단됐다. 숨김은 메타 풀크롤이 먼저 한다.

모드(CAREER_MODE):
  full    : 모든 pid 재크롤 → 바뀐 것만 write. meta-friday.yml 에서 능력치 풀크롤 뒤에 돈다.
  pending : player_career 에 아직 없는 pid 만. meta-thursday.yml 에서 신규 카드 크롤 직후.
  probe   : DB 없이 CAREER_PROBE_SPIDS 의 파싱 결과만 출력(넥슨 마크업 변경 점검용).

⚠️ 보호 장치 — 점검 페이지·차단 응답을 "경력 없음"으로 덮어쓰지 않기 위해:
  1) 진짜 빈 경력 페이지에도 경력 표(div.data_detail_club)는 있고 <ul> 만 비어 있다.
     표 자체가 없으면(점검·없는 spid — 77바이트짜리 응답) 실패로 세고 write 하지 않는다.
     백엔드 players/career_crawler.py 는 둘 다 [] 로 돌려주므로 그대로 가져오면 안 된다.
  2) 경력이 있던 선수가 빈 경력으로 바뀌는 건 write 하지 않고 건수만 보고한다.
  3) 한 청크에서 실패율이 CAREER_MAX_FAIL_RATE 를 넘으면 그 청크부터 write 없이 중단한다.

DB 커넥션은 크롤 동안 붙들지 않는다. 시작 시 읽기 1회, 이후 배치마다 새로 체크아웃한다
(pool_pre_ping 은 체크아웃 때만 돌아서, 30분 붙든 커넥션은 SSL 끊김으로 죽는다).

결과는 player.meta_crawl_log 에 phase='career' 1행(같은 META_RUN_ID)으로 남기고,
notify.py 가 요약 알림 본문에 한 줄로 합친다. 컬럼은 기존 것을 재사용한다:
  new_players = 처음 저장한 pid 수, changed = 경력이 바뀐 pid 수, failures = 실패 수,
  new_spids   = 세부 dict(mode/targets/done/emptied_skipped/aborted).
전용 컬럼을 새로 만들면 백엔드 BOOTSTRAP_VERSION 을 올려 배포해야 해서 JSONB 칸을 빌린다.

⚠️ 비밀정보(DATABASE_URL)는 절대 이 파일/저장소에 두지 않는다. GitHub Secret 으로만 주입.

환경변수
    DATABASE_URL            (full/pending 필수) Postgres 접속 문자열. 로그에 출력 안 함.
    CAREER_MODE             'full'(기본) | 'pending' | 'probe'
    META_RUN_ID             알림 집계 키(워크플로가 github.run_id 주입). 없으면 날짜.
    CAREER_WORKERS          동시 요청 스레드 수 (기본 8)
    CAREER_REQUEST_DELAY    각 요청 후 sleep 초 (기본 0.1)
    CAREER_MAX_RETRIES      넥슨 호출 재시도 횟수 (기본 3)
    CAREER_CHUNK_SIZE       한 번에 요청하는 pid 수 = 실패율 판정 단위 (기본 2000)
    CAREER_DB_BATCH         한 트랜잭션에 쓰는 pid 수 (기본 200)
    CAREER_MAX_FAIL_RATE    청크 실패율 상한, 넘으면 중단 (기본 0.2)
    CAREER_LIMIT            처리할 pid 수 상한 (기본 0=전체, 테스트용)
    CAREER_PROBE_SPIDS      probe 모드에서 볼 spid 목록 (콤마 구분)

로컬 테스트:
    CAREER_MODE=probe CAREER_PROBE_SPIDS=866200104,321166853 python career_job.py
    DATABASE_URL=postgresql://... CAREER_LIMIT=30 python career_job.py
"""

import json
import logging
import os
import re
import sys
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import requests
from sqlalchemy import text

from meta_crawler import _env_int, _pg_array_literal, make_engine, make_session

logger = logging.getLogger("career_job")

PLAYER_SCHEMA = "player"
PID_MOD = 1000000
INFO_URL = "https://fconline.nexon.com/DataCenter/PlayerInfo"

# 청크가 이보다 작으면 실패율 판정을 건너뛴다(목요일 신규 몇십 명에서 한두 건 실패로
# 전부 버리지 않게). 실패분은 pending 이면 다음 실행이, full 이면 다음 주가 다시 긁는다.
_MIN_CHUNK_FOR_RATE = 100


# =============================================================================
# PARSE — 넥슨 PlayerInfo HTML → 클럽 경력 리스트
#   정규식은 백엔드 players/career_crawler.py 와 같다(바꾸면 양쪽 다).
#   다른 점은 하나: 경력 표가 없으면 [] 가 아니라 None(=실패)을 돌려준다.
# =============================================================================
_CLUB_BLOCK_RE = re.compile(
    r'<div class="content data_detail_club">(.*?)</div>\s*</div>\s*</div>',
    re.S,
)
_ROW_RE = re.compile(
    r'<div class="td year">(.*?)</div>\s*'
    r'<div class="td club">(.*?)</div>\s*'
    r'<div class="td rent">(.*?)</div>',
    re.S,
)
_TAG_RE = re.compile(r"<[^>]+>")


def _clean(value: str) -> str:
    return _TAG_RE.sub("", value).strip()


def parse_club_career(html: str) -> Optional[List[dict]]:
    """[{"year": "2025 ~", "club": "LA FC", "loan": False}, ...] (최신→과거).

    경력 표가 있는데 비어 있으면 [](넥슨에 데이터 없는 선수 — 전체의 약 절반, 정상).
    경력 표 자체가 없으면 None(점검 페이지·없는 spid·마크업 변경 — 실패로 센다).
    """
    block = _CLUB_BLOCK_RE.search(html)
    if not block:
        return None

    career = []
    for year, club, rent in _ROW_RE.findall(block.group(1)):
        club_name = _clean(club)
        if not club_name:
            continue
        career.append({
            "year": _clean(year),
            "club": club_name,
            "loan": _clean(rent) == "임대",
        })
    return career


def fetch_career(session: requests.Session, spid: int, timeout: int = 12) -> Optional[List[dict]]:
    res = session.get(
        INFO_URL,
        params={"spid": spid},
        headers={"Referer": f"{INFO_URL}?spid={spid}"},
        timeout=timeout,
    )
    res.raise_for_status()
    return parse_club_career(res.text)


def _fetch_one(session, pid: int, spid: int, max_retries: int,
               delay: float) -> Tuple[int, Optional[List[dict]]]:
    """(pid, career) — 끝까지 실패하면 career=None."""
    reason = ""
    for attempt in range(max_retries):
        try:
            career = fetch_career(session, spid)
            reason = "경력 표 없음"
        except requests.RequestException as exc:
            career = None
            reason = str(exc)
        if delay:
            time.sleep(delay)
        if career is not None:
            return pid, career
        if attempt < max_retries - 1:
            time.sleep(1.5 * (attempt + 1))  # 백오프
    logger.warning("실패 pid=%s spid=%s: %s", pid, spid, reason)
    return pid, None


# =============================================================================
# 소속 필터 파생(player_career_clubs) — 백엔드 scripts/build_player_career_clubs.py 와
# 같은 규칙. 한쪽을 바꾸면 다른 쪽도 바꿀 것. 저쪽은 전량 TRUNCATE 후 재빌드라 그동안
# 필터가 비어 보이므로, 자동 배치는 바뀐 pid 만 DELETE+INSERT 한다(한 트랜잭션).
# =============================================================================
# "2020 ~" 처럼 끝 연도가 없는(열린) 구간 = 현재 소속. "2015 ~ 2018" 은 과거.
_OPEN_YEAR_RE = re.compile(r"^\s*\d{3,4}\s*~\s*$")


def _norm(name) -> str:
    return unicodedata.normalize("NFC", (name or "").strip())


def club_rows_for_career(career: List[dict],
                         clubs_by_name: Dict[str, Tuple[int, int]]) -> Dict[int, Tuple[int, bool]]:
    """한 선수 경력 → {club_id: (league_id, is_current)}.

    클럽명이 데이터센터 클럽 목록(clubs)에 없으면 건너뛴다(하위리그·국가대표 등).
    같은 클럽 재입단은 (pid, club_id) 한 행으로 합쳐지고 is_current 는 OR.
    """
    if not career:
        return {}

    top = career[0]
    current_name = (
        _norm(top.get("club"))
        if _OPEN_YEAR_RE.match(top.get("year") or "")
        else None
    )

    resolved: Dict[int, Tuple[int, bool]] = {}
    for entry in career:
        name = _norm(entry.get("club"))
        club = clubs_by_name.get(name)
        if club is None:
            continue
        club_id, league_id = club
        is_current = current_name is not None and name == current_name
        prev = resolved.get(club_id)
        resolved[club_id] = (league_id, is_current or (prev[1] if prev else False))
    return resolved


# =============================================================================
# DB
#   ⚠️ 배열은 전부 _pg_array_literal() 문자열 1개로 바인드한다(파이썬 리스트 바인드 금지 —
#      pg_stat_statements 텍스트 비대 → Supabase Disk IO 경고, meta_crawler 와 같은 규칙).
# =============================================================================
_TARGETS_FULL_SQL = text(f"""
    SELECT (spid % {PID_MOD}) AS pid, MIN(spid) AS rep_spid
    FROM {PLAYER_SCHEMA}.players
    WHERE NOT hidden
    GROUP BY 1
    ORDER BY 1
""")

_TARGETS_PENDING_SQL = text(f"""
    SELECT (p.spid % {PID_MOD}) AS pid, MIN(p.spid) AS rep_spid
    FROM {PLAYER_SCHEMA}.players AS p
    WHERE NOT p.hidden AND NOT EXISTS (
        SELECT 1 FROM {PLAYER_SCHEMA}.player_career AS c
        WHERE c.pid = (p.spid % {PID_MOD})
    )
    GROUP BY 1
    ORDER BY 1
""")

_EXISTING_SQL = text(f"SELECT pid, career FROM {PLAYER_SCHEMA}.player_career")

_CLUBS_SQL = text(f"SELECT id, name, league_id FROM {PLAYER_SCHEMA}.clubs")

# 파이썬에서 이미 비교했지만, 같은 pid 를 다른 실행이 먼저 썼을 때를 대비해 SQL 쪽에서도
# 같은 값이면 건드리지 않는다(crawled_at = 경력이 마지막으로 바뀐 시각이 된다).
_UPSERT_CAREER_SQL = text(f"""
    INSERT INTO {PLAYER_SCHEMA}.player_career AS c (pid, career)
    SELECT * FROM unnest(
        CAST(:pids AS integer[]),
        CAST(:careers AS jsonb[])
    )
    ON CONFLICT (pid) DO UPDATE SET
        career = EXCLUDED.career,
        crawled_at = now()
    WHERE c.career IS DISTINCT FROM EXCLUDED.career
""")

_DELETE_CLUBS_SQL = text(f"""
    DELETE FROM {PLAYER_SCHEMA}.player_career_clubs
    WHERE pid = ANY(CAST(:pids AS integer[]))
""")

_INSERT_CLUBS_SQL = text(f"""
    INSERT INTO {PLAYER_SCHEMA}.player_career_clubs
        (pid, club_id, league_id, is_current)
    SELECT * FROM unnest(
        CAST(:pids AS integer[]),
        CAST(:club_ids AS integer[]),
        CAST(:league_ids AS integer[]),
        CAST(:currents AS boolean[])
    )
    ON CONFLICT (pid, club_id) DO UPDATE SET
        league_id = EXCLUDED.league_id,
        is_current = EXCLUDED.is_current
""")

_LOG_SQL = text(f"""
    INSERT INTO {PLAYER_SCHEMA}.meta_crawl_log
        (run_id, phase, new_players, changed, failures, new_spids)
    VALUES (:rid, 'career', :np, :ch, :fa, CAST(:detail AS jsonb))
""")


def _write_batch(engine, rows: List[Tuple[int, List[dict]]],
                 clubs_by_name: Dict[str, Tuple[int, int]]) -> None:
    """경력 upsert + 그 pid 들의 소속 파생 재생성을 한 트랜잭션으로."""
    pids = [pid for pid, _ in rows]
    club_rows = []
    for pid, career in rows:
        for club_id, (league_id, is_current) in club_rows_for_career(career, clubs_by_name).items():
            club_rows.append((pid, club_id, league_id, is_current))

    with engine.begin() as conn:
        conn.execute(_UPSERT_CAREER_SQL, {
            "pids": _pg_array_literal(pids),
            # JSON 문자열들을 배열 리터럴 1개로 묶고 서버가 jsonb[] 로 캐스팅(백엔드와 같은 방식).
            "careers": _pg_array_literal(
                json.dumps(career, ensure_ascii=False) for _, career in rows
            ),
        })
        conn.execute(_DELETE_CLUBS_SQL, {"pids": _pg_array_literal(pids)})
        if club_rows:
            conn.execute(_INSERT_CLUBS_SQL, {
                "pids": _pg_array_literal(r[0] for r in club_rows),
                "club_ids": _pg_array_literal(r[1] for r in club_rows),
                "league_ids": _pg_array_literal(r[2] for r in club_rows),
                "currents": _pg_array_literal(r[3] for r in club_rows),
            })


def _log_result(engine, run_id: str, mode: str, stats: dict) -> None:
    detail = {
        "mode": mode,
        "targets": stats["targets"],
        "done": stats["done"],
        "emptied_skipped": stats["emptied_skipped"],
        "aborted": stats["aborted"],
    }
    with engine.begin() as conn:
        conn.execute(_LOG_SQL, {
            "rid": run_id,
            "np": stats["new"],
            "ch": stats["changed"],
            "fa": stats["failures"],
            "detail": json.dumps(detail),
        })


def _chunked(items, size: int):
    for i in range(0, len(items), size):
        yield items[i:i + size]


# =============================================================================
# RUN
# =============================================================================
def crawl_careers(engine, run_id: str, mode: str) -> dict:
    workers = _env_int("CAREER_WORKERS", 8)
    delay = float(os.getenv("CAREER_REQUEST_DELAY", "0.1") or 0)
    max_retries = _env_int("CAREER_MAX_RETRIES", 3)
    chunk_size = _env_int("CAREER_CHUNK_SIZE", 2000)
    db_batch = _env_int("CAREER_DB_BATCH", 200)
    max_fail_rate = float(os.getenv("CAREER_MAX_FAIL_RATE", "0.2") or 0.2)
    pid_limit = _env_int("CAREER_LIMIT", 0)

    # 시작 시 한 번만 읽고 커넥션을 반납한다(크롤 동안 붙들지 않음).
    with engine.connect() as conn:
        sql = _TARGETS_FULL_SQL if mode == "full" else _TARGETS_PENDING_SQL
        targets = [(int(r.pid), int(r.rep_spid)) for r in conn.execute(sql).all()]
        # pending 대상은 정의상 player_career 에 없으므로 비교할 기존 값이 없다.
        existing: Dict[int, list] = (
            {int(r.pid): r.career for r in conn.execute(_EXISTING_SQL).all()}
            if mode == "full" else {}
        )
        clubs_by_name = {
            _norm(r.name): (int(r.id), int(r.league_id))
            for r in conn.execute(_CLUBS_SQL).all()
        }
    if not clubs_by_name:
        # 클럽 목록이 비면 파생 DELETE 만 일어나 소속 필터가 통째로 비게 된다.
        raise SystemExit("player.clubs 가 비었습니다. 백엔드 scripts.crawl_leagues_clubs 를 먼저 실행하세요.")
    if pid_limit > 0:
        targets = targets[:pid_limit]

    stats = {"targets": len(targets), "done": 0, "new": 0, "changed": 0,
             "failures": 0, "emptied_skipped": 0, "aborted": False}
    logger.info("career %s: 대상 pid=%d 기존=%d 클럽=%d (workers=%d, delay=%.2fs)",
                mode, len(targets), len(existing), len(clubs_by_name), workers, delay)

    session = make_session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=workers, pool_maxsize=workers, max_retries=0
    )
    session.mount("https://", adapter)
    started = time.monotonic()

    try:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            for chunk in _chunked(targets, chunk_size):
                results = list(executor.map(
                    lambda t: _fetch_one(session, t[0], t[1], max_retries, delay), chunk
                ))
                chunk_fail = sum(1 for _, career in results if career is None)
                stats["done"] += len(chunk)
                stats["failures"] += chunk_fail

                if len(chunk) >= _MIN_CHUNK_FOR_RATE and chunk_fail / len(chunk) > max_fail_rate:
                    stats["aborted"] = True
                    logger.error("청크 실패율 %d/%d 이 상한 %.0f%% 초과 — 점검/차단 의심, 중단(이 청크 write 안 함)",
                                 chunk_fail, len(chunk), max_fail_rate * 100)
                    break

                writes: List[Tuple[int, List[dict]]] = []
                for pid, career in results:
                    if career is None:
                        continue
                    if pid not in existing:
                        stats["new"] += 1
                    else:
                        old = existing[pid]
                        if old == career:
                            continue
                        if old and not career:
                            stats["emptied_skipped"] += 1
                            logger.warning("경력 있던 pid=%s 가 빈 경력으로 응답 — write 보류", pid)
                            continue
                        stats["changed"] += 1
                    writes.append((pid, career))

                for batch in _chunked(writes, db_batch):
                    _write_batch(engine, batch, clubs_by_name)

                elapsed = time.monotonic() - started
                logger.info("진행 %d/%d 신규=%d 변경=%d 실패=%d 빈경력보류=%d %.1f req/s",
                            stats["done"], stats["targets"], stats["new"], stats["changed"],
                            stats["failures"], stats["emptied_skipped"],
                            stats["done"] / elapsed if elapsed else 0)
    except Exception:
        stats["aborted"] = True
        _log_result(engine, run_id, mode, stats)
        raise

    _log_result(engine, run_id, mode, stats)
    logger.info("career %s 완료: %s (%.0fs)", mode, stats, time.monotonic() - started)
    return stats


def probe() -> None:
    """DB 없이 넥슨 마크업 점검. 경력 있는 선수·빈 선수·없는 spid 를 섞어 보면 된다."""
    raw = os.getenv("CAREER_PROBE_SPIDS", "866200104,321166853,100999999")
    session = make_session()
    for spid in [int(s) for s in raw.replace(" ", "").split(",") if s]:
        try:
            career = fetch_career(session, spid)
        except requests.RequestException as exc:
            print(f"{spid}\t요청 실패: {exc}")
            continue
        if career is None:
            print(f"{spid}\t실패(경력 표 없음)")
        else:
            print(f"{spid}\t{len(career)}건\t{json.dumps(career[:3], ensure_ascii=False)}")


def run() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    mode = os.getenv("CAREER_MODE", "full").strip().lower()
    if mode == "probe":
        probe()
        return
    if mode not in ("full", "pending"):
        raise SystemExit(f"알 수 없는 CAREER_MODE: {mode}")

    run_id = os.getenv("META_RUN_ID") or datetime.now(timezone.utc).strftime("%Y%m%d")
    stats = crawl_careers(make_engine(), run_id, mode)
    if stats["aborted"]:
        sys.exit(1)   # 잡을 빨간불로 — 요약 알림은 notify 가 따로 보낸다


if __name__ == "__main__":
    run()
