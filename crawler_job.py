"""
FC Online 선수 "최신 시세만" 일일 크롤러 — 공개 저장소용 자기완결형 스크립트.

이 파일 하나로 끝난다(메인 비공개 앱 저장소에 의존하지 않는다). GitHub Actions
(공개 저장소 = Actions 분 무제한)에서 하루 1회 실행한다. 넥슨에서 선수×강화단계당
'가장 최근 1줄'만 받아 player.player_price_latest 에 upsert 한다.
멱등(idempotent)하므로 중간에 죽어도 다시 돌리면 이어서 채워진다.

⚠️ 비밀정보(DATABASE_URL = DB 주소 + 비밀번호)는 절대 이 파일/저장소에 두지 않는다.
   GitHub repo Settings → Secrets and variables → Actions → DATABASE_URL 로만 주입한다.
   이 스크립트는 DATABASE_URL 을 로그에 절대 출력하지 않는다.

넥슨 엔드포인트: POST https://fconline.nexon.com/datacenter/PlayerPriceGraph
  body: spid, n1strong(강화단계 1~13)
  응답: HTML 안에 `var json1 = {time:[...], value:[...]}` (쿠키/CSRF 불필요)

환경변수
    DATABASE_URL                (필수) Postgres 접속 문자열. 로그에 절대 출력 안 함.
    SHARD_INDEX / SHARD_COUNT   GitHub matrix 샤딩. 각 샤드가 spids[INDEX::COUNT] 담당 (기본 0/1)
    PRICE_LATEST_STRONGS        크롤할 강화단계. "1-13"(기본) / "1,5,8" / "1-5,8" 형식
    PRICE_LATEST_WORKERS        동시 요청 스레드 수 (기본 10)
    PRICE_LATEST_CHUNK_SIZE     한 번에 제출/처리하는 (spid,strong) 작업 수 (기본 4000, 메모리 상한)
    PRICE_LATEST_DB_BATCH       DB 에 한 번에 upsert 하는 행 수 (기본 500)
    PRICE_LATEST_MAX_RETRIES    넥슨 호출 재시도 횟수 (기본 3)
    PRICE_LATEST_REQUEST_DELAY  각 요청 후 sleep 초 (기본 0.05, 레이트리밋 완화용)
    PRICE_LATEST_LIMIT          처리할 spid 수 상한 (기본 0=전체; 샤딩 전에 적용, 테스트용)

로컬 테스트:
    DATABASE_URL=postgresql://... PRICE_LATEST_LIMIT=20 PRICE_LATEST_STRONGS=1 python crawler_job.py
"""

import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from typing import List, Optional

import requests
from sqlalchemy import (
    BigInteger,
    Column,
    Date,
    DateTime,
    MetaData,
    SmallInteger,
    Table,
    create_engine,
    text,
)
from sqlalchemy.sql import func

logger = logging.getLogger("price_latest_crawler")

PLAYER_SCHEMA = "player"

# 검색 가능한 선수 목록을 읽어올 테이블(읽기 전용). 이 spid 들에 대해서만 시세를 채운다.
PLAYERS_TABLE = f'{PLAYER_SCHEMA}.players'

# upsert 대상 테이블. 메인 앱의 prices/models.py / alembic 마이그레이션과 동일 스키마.
_metadata = MetaData()
player_price_latest = Table(
    "player_price_latest",
    _metadata,
    Column("spid", BigInteger, primary_key=True),
    Column("strong", SmallInteger, primary_key=True),
    Column("price", BigInteger, nullable=False),
    Column("price_date", Date, nullable=True),
    Column("updated_at", DateTime(timezone=True), server_default=func.now()),
    schema=PLAYER_SCHEMA,
)


# =============================================================================
# 넥슨 시세 크롤러 코어 (메인 저장소 prices/crawler.py 와 동일 로직)
# =============================================================================
BASE_URL = "https://fconline.nexon.com/datacenter/PlayerPriceGraph"

_TIME_RE = re.compile(r'"(\d+\.\d+)"')   # "6.23"
_NUM_RE = re.compile(r'"(\d+)"')          # "442777777800"


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0",
        "X-Requested-With": "XMLHttpRequest",
    })
    return s


def _parse_json1(html: str):
    """HTML에서 time/value 배열을 추출. (times, values) 반환."""
    start = html.find("var json1")
    if start == -1:
        return [], []
    block = html[start:]
    v_idx = block.find('"value"')
    if v_idx == -1:
        return [], []

    time_part = block[:v_idx]
    value_part = block[v_idx:block.find("]", v_idx)]

    times = _TIME_RE.findall(time_part)
    values = [int(v) for v in _NUM_RE.findall(value_part)]
    return times, values


def _to_dates(times, end: date):
    """월.일 문자열 리스트를 date 객체로. 끝에서부터 역산해 연 경계 처리."""
    n = len(times)
    return [end - timedelta(days=n - 1 - i) for i in range(n)]


def fetch_price_history(session, spid, strong, end=None, recent_days=None, timeout=15):
    """특정 선수/강화단계 시세를 [(date, price), ...] 로 반환. 빈 값이면 []."""
    end = end or (date.today() - timedelta(days=1))
    headers = {"Referer": f"https://fconline.nexon.com/DataCenter/PlayerInfo?spid={spid}"}

    res = session.post(
        BASE_URL,
        data={"spid": spid, "n1strong": strong},
        headers=headers,
        timeout=timeout,
    )
    res.raise_for_status()  # 429/5xx 는 여기서 예외 → 호출부에서 백오프 재시도

    times, values = _parse_json1(res.text)
    if not times:
        return []

    dates = _to_dates(times, end)
    rows = list(zip(dates, values))
    if recent_days:
        rows = rows[-recent_days:]
    return rows


# =============================================================================
# DB
# =============================================================================
def make_engine():
    """DATABASE_URL 로 엔진 생성. 비밀번호가 들어있으므로 절대 로그에 찍지 않는다."""
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise SystemExit(
            "DATABASE_URL 환경변수가 없습니다. "
            "(GitHub Secret 으로 주입하거나 로컬 테스트 시 export 하세요)"
        )
    # postgres:// → postgresql:// 정규화 후 psycopg2 드라이버 명시.
    url = url.replace("postgres://", "postgresql://", 1)
    if url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    # pool_pre_ping: 유휴 커넥션이 끊겨도 재연결.
    return create_engine(url, pool_pre_ping=True)


def read_spids(conn) -> List[int]:
    """검색 가능한 선수(players 테이블)의 spid 전체를 정렬해 반환."""
    rows = conn.execute(text(f"SELECT DISTINCT spid FROM {PLAYERS_TABLE}")).all()
    return sorted({row[0] for row in rows})


# 배치 행 수와 무관하게 SQL 텍스트가 항상 같아야 한다. insert().values(rows) 는
# 행 수만큼 placeholder 가 늘어나 배치 크기별로 pg_stat_statements 에 "서로 다른
# 쿼리"로 등록됐고(수천 엔트리 × 최대 17KB 텍스트), Supabase 모니터링이
# pg_stat_statements 를 읽을 때마다 그 텍스트 전체(~66MB)가 temp 파일로 쏟아져
# Disk IO 예산을 태웠다(2026-07 재발 원인). UNNEST(배열) 은 항상 한 가지 텍스트.
_UPSERT_LATEST_SQL = text(f"""
    INSERT INTO {PLAYER_SCHEMA}.player_price_latest (spid, strong, price, price_date)
    SELECT * FROM unnest(
        CAST(:spids AS bigint[]),
        CAST(:strongs AS smallint[]),
        CAST(:prices AS bigint[]),
        CAST(:price_dates AS date[])
    )
    ON CONFLICT (spid, strong) DO UPDATE SET
        price = EXCLUDED.price,
        price_date = EXCLUDED.price_date,
        updated_at = now()
    WHERE player_price_latest.price IS DISTINCT FROM EXCLUDED.price
""")


def bulk_upsert_latest(conn, rows: List[dict]) -> int:
    """최신 시세들을 player_price_latest 에 한 번에 upsert. 멱등. 반환=시도 행 수.

    price 가 기존 값과 같으면 ON CONFLICT 시 UPDATE 를 건너뛴다
    (WHERE price IS DISTINCT FROM EXCLUDED.price). 시세는 대부분 날마다 안 바뀌는데,
    안 바뀐 행까지 매번 다시 쓰면 dead tuple + WAL 이 매일 쌓여 Supabase 의
    Disk IO 예산을 갉아먹는다(이 크롤러가 하루 6번 도므로 누적이 크다). 그래서
    값이 실제로 변한 행만 쓴다. 이 때문에 updated_at 의 의미는 "마지막 확인 시각"이
    아니라 "마지막으로 price 가 바뀐 시각"이 된다.
    """
    if not rows:
        return 0
    conn.execute(
        _UPSERT_LATEST_SQL,
        {
            "spids": [r["spid"] for r in rows],
            "strongs": [r["strong"] for r in rows],
            "prices": [r["price"] for r in rows],
            "price_dates": [r["price_date"] for r in rows],
        },
    )
    conn.commit()
    return len(rows)


def vacuum_analyze(engine) -> None:
    """이번 샤드가 만든 dead tuple 을 배치 직후 즉시 회수한다(재발 방지의 본체).

    no-op skip 으로 dead 생성은 이미 줄였지만, 바뀐 가격만큼은 매 회차 dead 가
    생긴다. autovacuum 데몬을 기다리지 않고 쓰기 직후 한 번 VACUUM 해서, 비운 공간을
    테이블 내부에서 곧장 재사용하게 만든다 → high-water mark(파일 크기) 가 다시
    부풀지 않는다. ANALYZE 까지 묶어 (strong, price) 인덱스 통계도 최신으로 유지.
    dead 가 적으면 visibility map 으로 깨끗한 페이지를 건너뛰므로 매우 가볍다.

    ⚠️ VACUUM 은 트랜잭션 블록 안에서 못 돈다. AUTOCOMMIT 커넥션으로 실행한다.
    ⚠️ VACUUM FULL 이 아니다(배타 락 없음). 일상 회수용이라 검색/배치를 막지 않는다.
    """
    sql = f"VACUUM (ANALYZE) {PLAYER_SCHEMA}.player_price_latest"
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text(sql))


# =============================================================================
# 배치 본체
# =============================================================================
def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _parse_strongs(spec: str) -> List[int]:
    """"1-13" / "1,5,8" / "1-5,8,10" 형식을 강화단계 리스트로 파싱(1~13 범위)."""
    out: List[int] = []
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            start, end = part.split("-", 1)
            out.extend(range(int(start), int(end) + 1))
        else:
            out.append(int(part))
    return sorted({s for s in out if 1 <= s <= 13})


def _shard(items: List[int]) -> List[int]:
    """GitHub Actions matrix 샤딩: 자기 몫(spids[INDEX::COUNT])만 떼어낸다."""
    index = _env_int("SHARD_INDEX", 0)
    count = _env_int("SHARD_COUNT", 1)
    if count <= 1:
        return items
    sharded = items[index::count]
    logger.info("shard %d/%d → %d spids", index, count, len(sharded))
    return sharded


def _chunked(items, size: int):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def _fetch_one(session, spid: int, strong: int, max_retries: int, delay: float) -> Optional[dict]:
    """(spid, strong) 의 최신 시세 1줄을 받아온다. 빈 값/실패면 None."""
    for attempt in range(max_retries):
        try:
            rows = fetch_price_history(session, spid, strong, recent_days=1)
            if delay:
                time.sleep(delay)
            if not rows:
                return None
            last_date, last_price = rows[-1]
            return {
                "spid": spid,
                "strong": strong,
                "price": int(last_price),
                "price_date": last_date,
            }
        except requests.RequestException as exc:
            if attempt == max_retries - 1:
                logger.warning("fetch 실패 spid=%s strong=%s: %s", spid, strong, exc)
                return None
            time.sleep(1.5 * (attempt + 1))  # 백오프
    return None


def run() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    strongs = _parse_strongs(os.getenv("PRICE_LATEST_STRONGS", "1-13"))
    workers = _env_int("PRICE_LATEST_WORKERS", 10)
    chunk_size = _env_int("PRICE_LATEST_CHUNK_SIZE", 4000)
    db_batch = _env_int("PRICE_LATEST_DB_BATCH", 500)
    max_retries = _env_int("PRICE_LATEST_MAX_RETRIES", 3)
    delay = float(os.getenv("PRICE_LATEST_REQUEST_DELAY", "0.05") or 0)
    spid_limit = _env_int("PRICE_LATEST_LIMIT", 0)

    # 동시 요청 수만큼 커넥션 풀을 키운다(HTTP 는 워커 스레드에서만 일어난다).
    session = make_session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=workers, pool_maxsize=workers, max_retries=0
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    engine = make_engine()
    with engine.connect() as conn:
        all_spids = read_spids(conn)
        if spid_limit > 0:                       # 테스트용 상한은 샤딩 전에 적용
            all_spids = all_spids[:spid_limit]
        spids = _shard(all_spids)

        tasks = [(spid, strong) for spid in spids for strong in strongs]
        total = len(tasks)
        logger.info(
            "배치 시작: spids=%d × strongs=%s → tasks=%d (workers=%d, delay=%.3fs)",
            len(spids), strongs, total, workers, delay,
        )

        done = 0
        written = 0
        started = time.monotonic()

        # 작업을 청크 단위로 처리해 미해결 future 수(=메모리)를 상한선 안에 둔다.
        with ThreadPoolExecutor(max_workers=workers) as executor:
            for chunk in _chunked(tasks, chunk_size):
                futures = [
                    executor.submit(_fetch_one, session, spid, strong, max_retries, delay)
                    for spid, strong in chunk
                ]
                buffer: List[dict] = []
                for future in as_completed(futures):
                    done += 1
                    row = future.result()
                    if row is not None:
                        buffer.append(row)
                        if len(buffer) >= db_batch:
                            written += bulk_upsert_latest(conn, buffer)
                            buffer = []
                if buffer:
                    written += bulk_upsert_latest(conn, buffer)

                elapsed = time.monotonic() - started
                rate = done / elapsed if elapsed else 0
                logger.info(
                    "진행 %d/%d (%.1f%%) written=%d %.1f req/s",
                    done, total, 100 * done / total if total else 100, written, rate,
                )

        logger.info(
            "배치 완료: tasks=%d written=%d elapsed=%.0fs",
            total, written, time.monotonic() - started,
        )

    # 이번 샤드가 실제로 뭔가 썼을 때만 청소한다(안 바뀐 날 = dead 없음 = VACUUM 불필요).
    if written:
        try:
            vacuum_analyze(engine)
            logger.info("VACUUM (ANALYZE) 완료: %s.player_price_latest", PLAYER_SCHEMA)
        except Exception as exc:  # 청소 실패가 배치 성공을 뒤집지 않게.
            logger.warning("VACUUM 스킵(실패): %s", exc)


if __name__ == "__main__":
    run()
