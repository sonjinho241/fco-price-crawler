"""
FC Online 선수 "최신 시세만" 크롤러 — 공개 저장소용 자기완결형 스크립트.

GitHub Actions(공개 저장소 = Actions 분 무제한)에서 한 실행이 5시간 30분 머물며 시각 버킷을 차례로
받고, 끝날 때 다음 실행을 스스로 띄운다(crawl.yml). 넥슨에서 선수×강화단계당
'가장 최근 1줄'만 받아 player.player_price_latest 와 player.player_price_strongs 에 upsert 한다.
멱등(idempotent)하므로 중간에 죽어도 다시 돌리면 이어서 채워진다.

⚠️ 2026-10-30 부터 카드를 두 묶음으로 나눠 돈다(이전: 전 카드를 하루 6토막, 토막당 35 req/s 로
   몰아서 하루 116만 건). 2026-10-29 넥슨(Cloudflare)이 GitHub 러너의 iteminfo 수집을 429 로
   막았고, 이 배치가 넥슨 요청의 98% 라 같은 규칙에 가장 먼저 걸린다. 설계는 메인 저장소
   docs/price-crawl-two-tier-plan.md.

     hot  = 최근 7일 조회 + 서버 동기화 스쿼드 + 콘솔 상품에서 닿는 팩 후보 카드 → 24시간에 한 바퀴
     tail = 나머지 전부                                                     → 168시간에 한 바퀴

   카드마다 크롤 시각을 두지 않는다(그러면 player_price_strongs 의 "값이 같으면 UPDATE 스킵"이
   무력화돼 전 행이 매주 다시 쓰인다). 대신 spid 의 고정 해시로 시각 버킷을 정한다(bucket_of).
   마지막으로 끝낸 버킷만 player.price_crawl_state 한 행에 적는다.

⚠️ 2026-10-30 매시 cron 하나에 기대던 방식은 cron 이 하루 3번 남짓밖에 안 와서(19시간에 3번) 버킷이
   밀렸다. 그래서 실행 하나가 길게 머문다(run_buckets): 밀린 버킷을 가장 오래된 것부터 **하나씩**
   받고(동시에 여러 버킷을 몰아 받지 않는다, 속도도 그대로), 다 따라잡으면 다음 정시까지 잔다.
   예산(기본 330분) 안에 다음 버킷을 못 끝내면 멈추고, 워크플로가 다음 실행을 바로 이어 띄운다.
   cron 은 사슬이 끊겼을 때(넥슨 차단으로 실패한 실행은 잇지 않는다) 다시 잇는 예비다.

⚠️ 2026-10-27 부터 두 표에 함께 쓴다(전환 기간). player_price_strongs 는 카드당 한 행에
   강화 1~13단계를 배열로 담는 새 표(115만 행·159MB → 8.8만 행·22MB)이고, 앱 서버가 그 표로
   옮겨 가면 player_price_latest 쓰기를 지운다. 표 정의는 메인 저장소
   Backend/FCOmanager_server/scripts/player_price_strongs.sql.

⚠️ 비밀정보(DATABASE_URL = DB 주소 + 비밀번호)는 절대 이 파일/저장소에 두지 않는다.
   GitHub repo Settings → Secrets and variables → Actions → DATABASE_URL 로만 주입한다.
   이 스크립트는 DATABASE_URL 을 로그에 절대 출력하지 않는다.

넥슨 엔드포인트: POST https://fconline.nexon.com/datacenter/PlayerPriceGraph
  body: spid, n1strong(강화단계 1~13)
  응답: 하나의 HTML 안에 **두 가지 값**이 들어 있다 (쿠키/CSRF 불필요)

    1. 현재가  — `var json1` 앞 마크업의 <strong alt="1,210,000,000">.
       넥슨이 2시간 주기(홀수시 5분)로 갱신하는 기준가.
    2. 일별 추이 — `var json1 = {time:[...], value:[...]}`. 하루 한 칸이고
       **오늘 칸은 없다**(마지막 칸 = 어제).

  player_price_latest 에 넣을 값은 **1번**이다. 2번의 마지막 칸을 쓰면 하루 늦은
  값이 되어 앱의 검색·가격 필터·구단가치가 실제와 20~30%까지 어긋난다
  (2026-09-28 규명·수정). 메인 저장소 prices/crawler.py 와 동일 로직을 유지한다.

환경변수
    DATABASE_URL                (필수) Postgres 접속 문자열. 로그에 절대 출력 안 함.
    PRICE_LATEST_STRONGS        크롤할 강화단계. "1-13"(기본) / "1,5,8" / "1-5,8" 형식
    PRICE_LATEST_WORKERS        동시 요청 스레드 수 (기본 3)
    PRICE_LATEST_REQUEST_DELAY  워커가 요청마다 쉬는 최소 초. 실제로는 [값, 2×값] 사이 무작위 (기본 0.4)
    PRICE_LATEST_MAX_RATE       모든 워커를 합친 초당 요청 상한 (기본 4, 5 를 넘기면 5 로 자른다)
    PRICE_LATEST_BUDGET_MIN     한 실행의 시간 예산(분). 이 안에 끝날 버킷만 시작한다 (기본 330)
    PRICE_LATEST_NEW_CAP        버킷마다 먼저 받는 신규 카드(시세 표에 없는 spid) 상한 (기본 100)
    PRICE_LATEST_CHUNK_SIZE     한 번에 제출/처리하는 (spid,strong) 작업 수 (기본 1300 = 100카드)
    PRICE_LATEST_DB_BATCH       DB 에 한 번에 upsert 하는 행 수 (기본 500)
    PRICE_LATEST_MAX_RETRIES    넥슨 호출 재시도 횟수(429·403 제외) (기본 3)
    PRICE_LATEST_LIMIT          처리할 spid 수 상한 (기본 0=전체, 스모크 테스트용). 0 이 아니면 상태 표를 안 건드린다
    PRICE_LATEST_ONLY_NEW       1 이면 신규 카드만, 상한 없이 (기본 0). 상태 표를 안 건드린다
    PRICE_LATEST_FULL           1 이면 전 카드 한 바퀴 (기본 0, 신규 시즌 같은 비상용). 상태 표를 안 건드린다

로컬 테스트:
    DATABASE_URL=postgresql://... PRICE_LATEST_LIMIT=20 PRICE_LATEST_STRONGS=1 python crawler_job.py
"""

import logging
import os
import random
import re
import threading
import time
import zlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from typing import Iterable, List, NamedTuple, Optional, Sequence, Set, Tuple

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
from sqlalchemy.exc import OperationalError
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
# 현재가: json1 앞 마크업의 <strong alt="1,210,000,000">.
_CURRENT_PRICE_RE = re.compile(r'<strong[^>]*\salt="([\d,]+)"')

# 배치는 UTC(GitHub Actions)로 돌므로 수집 날짜는 KST 로 고정한다.
_KST = timezone(timedelta(hours=9))


def _today_kst() -> date:
    return datetime.now(_KST).date()


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0",
        "X-Requested-With": "XMLHttpRequest",
    })
    return s


class PriceSnapshot(NamedTuple):
    """한 번의 넥슨 호출로 얻는 두 가지 값."""

    current_price: Optional[int]        # 지금 기준가. 거래 불가(0)면 None
    history: List[Tuple[date, int]]     # 일별 추이 (마지막 칸 = 어제)


def _parse_current_price(html: str) -> Optional[int]:
    """json1 앞 마크업에서 현재가를 읽는다. 없거나 0(거래 불가)이면 None."""
    head = html[:html.find("var json1")] if "var json1" in html else html
    match = _CURRENT_PRICE_RE.search(head)
    if match is None:
        return None
    price = int(match.group(1).replace(",", ""))
    return price or None


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


def fetch_price_snapshot(
    session, spid, strong, end=None, recent_days=None, timeout=15
) -> PriceSnapshot:
    """특정 선수/강화단계의 현재가 + 일별 추이를 한 번의 호출로 받는다."""
    end = end or (date.today() - timedelta(days=1))
    headers = {"Referer": f"https://fconline.nexon.com/DataCenter/PlayerInfo?spid={spid}"}

    res = session.post(
        BASE_URL,
        data={"spid": spid, "n1strong": strong},
        headers=headers,
        timeout=timeout,
    )
    res.raise_for_status()  # 429/5xx 는 여기서 예외 → 호출부에서 백오프 재시도

    current_price = _parse_current_price(res.text)

    times, values = _parse_json1(res.text)
    if not times:
        return PriceSnapshot(current_price=current_price, history=[])

    dates = _to_dates(times, end)
    rows = list(zip(dates, values))
    if recent_days:
        rows = rows[-recent_days:]
    return PriceSnapshot(current_price=current_price, history=rows)


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
    # pool_pre_ping: 체크아웃할 때마다 죽은 커넥션을 걸러낸다. 단 이건 커넥션을
    # 풀에서 "꺼낼 때"만 도므로, 커넥션 하나를 붙들고 있으면 아무 일도 하지 않는다
    # (2026-07-16 실패 원인). 쓰기는 반드시 flush 마다 새로 체크아웃해야 한다.
    # pool_recycle: 풀러가 조용히 끊는 시간보다 먼저 우리가 커넥션을 버린다.
    return create_engine(url, pool_pre_ping=True, pool_recycle=600)


def read_spids(conn) -> List[int]:
    """검색 가능한 선수(players 테이블)의 spid 전체를 정렬해 반환."""
    rows = conn.execute(text(f"SELECT DISTINCT spid FROM {PLAYERS_TABLE}")).all()
    return sorted({row[0] for row in rows})


def read_new_spids(conn) -> List[int]:
    """시세 표(player_price_strongs)에 아직 한 줄도 없는 spid. 매시 실행이 버킷보다 먼저 받는다
    (2026-10-29 26UP 출시 주 — 새 카드 시세가 없으면 개봉 시뮬레이터 상품이 통째로 숨겨진다)."""
    rows = conn.execute(text(
        f"SELECT DISTINCT p.spid FROM {PLAYERS_TABLE} p "
        f"WHERE NOT EXISTS (SELECT 1 FROM {PLAYER_SCHEMA}.player_price_strongs s WHERE s.spid = p.spid)"
    )).all()
    return sorted({row[0] for row in rows})


# 매일 받는 카드(hot)의 출처. 실행할 때마다 다시 센다(조회·스쿼드·상품이 날마다 바뀐다).
#   - 최근 7일(KST) 상세 조회가 있는 선수. player_view_daily 의 키는 players.id 다.
#   - 서버에 동기화된 팀 시뮬 스쿼드의 선수(squads/policy.normalize_members 의 "s").
#     비로그인 팀은 기기에만 있어 잡히지 않는다(감수).
#   - 콘솔에 등록된 상품·회차에서 닿는 팩 후보 풀·확률표 카드(개봉 시뮬레이터 순위가 이 시세로 매겨진다).
# 2026-10-30 실측: 조회 205 + 스쿼드 145 + 팩 4,453 → 겹침 빼고 4,479장(전체 89,383장의 5.0%).
_HOT_SOURCES_SQL = (
    "SELECT p.spid FROM player.player_view_daily v JOIN player.players p ON p.id = v.player_id "
    "  WHERE v.view_date >= (now() AT TIME ZONE 'Asia/Seoul')::date - 6 "
    "UNION SELECT (m->>'s')::bigint FROM app_user.squads s, jsonb_array_elements(s.members) AS m "
    "  WHERE s.deleted_at IS NULL AND jsonb_typeof(s.members) = 'array' AND (m->>'s') ~ '^[0-9]+$' "
    "UNION SELECT c.spid FROM spend.pack_pool_card c JOIN reach ON reach.sn = c.sn WHERE c.spid IS NOT NULL "
    "UNION SELECT r.spid FROM spend.prob_row r JOIN reach ON reach.sn = r.sn "
    "  WHERE r.row_kind = 'card' AND r.spid IS NOT NULL"
)


def read_hot_spids(conn) -> Set[int]:
    # iteminfo_job 이 이 모듈을 import 하므로 여기서 늦게 가져온다(순환 import 방지).
    from iteminfo_job import REACH_CTE

    return {int(row[0]) for row in conn.execute(text(REACH_CTE + _HOT_SOURCES_SQL))}


# 마지막으로 끝낸 시각 버킷(정시). 표 정의는 메인 저장소 scripts/price_crawl_state.sql.
# 버킷마다 한 행만 바꾸므로 IO 는 무시해도 된다.
_STATE_ID = 1


def read_last_bucket(conn) -> Optional[datetime]:
    return conn.execute(
        text(f"SELECT last_bucket_at FROM {PLAYER_SCHEMA}.price_crawl_state WHERE id = :id"),
        {"id": _STATE_ID},
    ).scalar_one_or_none()


def write_last_bucket(engine, bucket_at: datetime) -> None:
    with engine.begin() as conn:
        conn.execute(text(
            f"INSERT INTO {PLAYER_SCHEMA}.price_crawl_state (id, last_bucket_at, updated_at) "
            "VALUES (:id, :at, now()) "
            "ON CONFLICT (id) DO UPDATE SET last_bucket_at = EXCLUDED.last_bucket_at, updated_at = now()"
        ), {"id": _STATE_ID, "at": bucket_at})


# 배치 행 수와 무관하게 SQL 텍스트가 항상 같아야 한다. insert().values(rows) 는
# 행 수만큼 placeholder 가 늘어나 배치 크기별로 pg_stat_statements 에 "서로 다른
# 쿼리"로 등록됐고(수천 엔트리 × 최대 17KB 텍스트), Supabase 모니터링이
# pg_stat_statements 를 읽을 때마다 그 텍스트 전체(~66MB)가 temp 파일로 쏟아져
# Disk IO 예산을 태웠다(2026-07 재발 원인). UNNEST(배열) 은 항상 한 가지 텍스트.
#
# ⚠️ 단, 배열을 "파이썬 리스트"로 바인드하면 안 된다(2026-07-13 재재발 원인).
# psycopg2 는 리스트를 ARRAY[v1, v2, ...] 로 인라인하므로 원소 수만큼 상수가 생기고,
# pg_stat_statements 가 배치 크기별로 또 별개 엔트리를 만든다(4,283 엔트리/59MB).
# 반드시 _pg_array_literal() 로 '{...}' 문자열 하나로 직렬화해 넘겨야
# 상수가 항상 1개($1)가 된다.
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

# 카드당 한 행(player_price_strongs)에 강화단계별 값을 끼워 넣는다. 들어온 (spid, strong) 만
# 바꾸고 나머지 단계는 기존 값을 둔다(넥슨 호출이 실패한 단계는 아예 안 온다). 날짜는 값이
# 실제로 바뀐 단계만 새 날짜로 바뀐다. 배열 전체가 같으면 UPDATE 를 건너뛴다(옛 표와 같은
# no-op skip). 메인 저장소 prices/crud.py 의 _UPSERT_LATEST_SQL 과 같은 텍스트를 유지한다.
_UPSERT_STRONGS_SQL = text(f"""
    WITH incoming AS (
        SELECT * FROM unnest(
            CAST(:spids AS bigint[]),
            CAST(:strongs AS smallint[]),
            CAST(:prices AS bigint[]),
            CAST(:price_dates AS date[])
        ) AS u(spid, strong, price, price_date)
    ),
    merged AS (
        SELECT
            k.spid,
            array_agg(COALESCE(i.price, o.prices[g.strong]) ORDER BY g.strong) AS prices,
            array_agg(
                CASE WHEN i.price IS NOT NULL AND i.price IS DISTINCT FROM o.prices[g.strong]
                     THEN i.price_date ELSE o.price_dates[g.strong] END
                ORDER BY g.strong
            ) AS price_dates
        FROM (SELECT DISTINCT spid FROM incoming) k
        CROSS JOIN generate_series(1, 13) AS g(strong)
        LEFT JOIN incoming i ON i.spid = k.spid AND i.strong = g.strong
        LEFT JOIN {PLAYER_SCHEMA}.player_price_strongs o ON o.spid = k.spid
        GROUP BY k.spid
    )
    INSERT INTO {PLAYER_SCHEMA}.player_price_strongs (spid, prices, price_dates)
    SELECT spid, prices, price_dates FROM merged
    ON CONFLICT (spid) DO UPDATE SET
        prices = EXCLUDED.prices,
        price_dates = EXCLUDED.price_dates
    WHERE player_price_strongs.prices IS DISTINCT FROM EXCLUDED.prices
""")


def _pg_array_literal(values) -> str:
    """리스트를 Postgres 배열 리터럴 문자열('{...}') 하나로 직렬화한다.

    psycopg2 의 리스트 어댑터(ARRAY[...] 인라인)를 우회해, 서버에는 문자열 상수
    1개만 보내고 SQL 쪽 CAST 가 배열로 파싱하게 한다. 모든 원소를 "..." 로 감싸므로
    숫자/날짜 어느 타입 배열로도 캐스팅된다. None 은 NULL.
    """
    parts = []
    for v in values:
        if v is None:
            parts.append("NULL")
        else:
            s = str(v).replace("\\", "\\\\").replace('"', '\\"')
            parts.append('"' + s + '"')
    return "{" + ",".join(parts) + "}"


_DB_MAX_RETRIES = 4


def bulk_upsert_latest(engine, rows: List[dict]) -> Tuple[int, int]:
    """최신 시세들을 player_price_latest·player_price_strongs 에 한 번에 upsert. 멱등.
    반환=(시도 행 수, player_price_latest 에서 실제로 바뀐 행 수).

    rows 안에 같은 (spid, strong) 가 두 번 들어오면 안 된다(새 표의 배열 길이 CHECK 에 걸린다).

    price 가 기존 값과 같으면 ON CONFLICT 시 UPDATE 를 건너뛴다
    (WHERE price IS DISTINCT FROM EXCLUDED.price). 시세는 대부분 날마다 안 바뀌는데,
    안 바뀐 행까지 매번 다시 쓰면 dead tuple + WAL 이 쌓여 Supabase 의
    Disk IO 예산을 갉아먹는다. 그래서 값이 실제로 변한 행만 쓴다. 이 때문에 updated_at 의
    의미는 "마지막 확인 시각"이 아니라 "마지막으로 price 가 바뀐 시각"이 된다.

    ⚠️ 2026-09-28 부터 price 가 일별 추이 마지막 칸이 아니라 넥슨 "현재가"다.
    현재가는 2시간마다 움직이므로 하루 1회만 바뀌던 예전보다 **이 조건을 통과하는
    행이 늘어난다** = dead tuple/WAL 도 는다. 거래가 없는 대다수 선수는 여전히 스킵된다.

    ⚠️ 커넥션은 호출마다 새로 체크아웃한다(엔진을 받는 이유). 이 배치는 한 실행이
    한 시간 가까이 도는데, 그동안 커넥션 하나를 붙들고 있으면 flush 사이 수십 초씩
    유휴 상태가 된다. Supabase 트랜잭션 모드 풀러(:6543)는 그런 커넥션을 조용히
    닫고, 다음 쓰기가 "SSL connection has been closed unexpectedly" 로 죽는다
    (2026-07-16 shard 4/6, 73% 지점에서 실패). 매번 체크아웃해야 pool_pre_ping 이
    죽은 커넥션을 걸러낼 기회를 갖는다.

    그래도 뚫고 들어오는 끊김(체크아웃 직후 닫히는 경우)에 대비해 재시도한다.
    upsert 가 멱등이므로 같은 버퍼를 다시 써도 안전하다.
    """
    if not rows:
        return 0, 0
    params = {
        "spids": _pg_array_literal(r["spid"] for r in rows),
        "strongs": _pg_array_literal(r["strong"] for r in rows),
        "prices": _pg_array_literal(r["price"] for r in rows),
        "price_dates": _pg_array_literal(r["price_date"] for r in rows),
    }
    for attempt in range(_DB_MAX_RETRIES):
        try:
            # 두 표를 한 트랜잭션에 쓴다. 재시도해도 둘 다 멱등이라 안전하다.
            with engine.connect() as conn:
                changed = conn.execute(_UPSERT_LATEST_SQL, params).rowcount
                conn.execute(_UPSERT_STRONGS_SQL, params)
                conn.commit()
            return len(rows), max(0, changed or 0)
        except OperationalError as exc:
            if attempt == _DB_MAX_RETRIES - 1:
                raise
            # 끊긴 커넥션이 풀에 남아 다음 시도까지 오염시키지 않게 통째로 버린다.
            engine.dispose()
            wait = 2.0 * (attempt + 1)
            logger.warning(
                "DB 쓰기 실패(%d/%d), %.1fs 후 재시도: %s",
                attempt + 1, _DB_MAX_RETRIES, wait, exc.orig or exc,
            )
            time.sleep(wait)
    raise AssertionError("unreachable")  # 루프는 항상 return 또는 raise 로 끝난다


def vacuum_analyze(engine) -> None:
    """이번 실행이 만든 dead tuple 을 배치 직후 즉시 회수한다(재발 방지의 본체).

    no-op skip 으로 dead 생성은 이미 줄였지만, 바뀐 가격만큼은 매 회차 dead 가
    생긴다. autovacuum 데몬을 기다리지 않고 쓰기 직후 한 번 VACUUM 해서, 비운 공간을
    테이블 내부에서 곧장 재사용하게 만든다 → high-water mark(파일 크기) 가 다시
    부풀지 않는다. ANALYZE 까지 묶어 (strong, price) 인덱스 통계도 최신으로 유지.
    dead 가 적으면 visibility map 으로 깨끗한 페이지를 건너뛰므로 매우 가볍다.

    ⚠️ VACUUM 은 트랜잭션 블록 안에서 못 돈다. AUTOCOMMIT 커넥션으로 실행한다.
    ⚠️ VACUUM FULL 이 아니다(배타 락 없음). 일상 회수용이라 검색/배치를 막지 않는다.
    """
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        for table in ("player_price_latest", "player_price_strongs"):
            conn.execute(text(f"VACUUM (ANALYZE) {PLAYER_SCHEMA}.{table}"))


# =============================================================================
# 배치 본체
# =============================================================================
def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
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


# ---- 시각 버킷 ----------------------------------------------------------------
HOT_BUCKETS = 24             # 하루 시각(KST 0~23시)
TAIL_BUCKETS = 168           # 주간 시각(KST 월 00시 = 0 … 일 23시 = 167)
MAX_LAG = timedelta(days=7)  # 이보다 밀리면 지금 버킷부터 새로 시작한다
# 버킷 하나의 평균 요청 수(89,383장×13÷168 + 4,479장×13÷24 ≈ 9,000, 2026-10-30 실측). 준비 전 예산 판단용.
BUCKET_TASKS_GUESS = 9000
VACUUM_EVERY_HOURS = 6       # KST 04·10·16·22시 버킷이 끝나면 VACUUM (이전 6토막과 같은 하루 4~6회 수준)


def bucket_of(spid: int, n: int) -> int:
    """spid 의 고정 버킷(0..n-1). 파이썬 hash() 는 실행마다 달라지므로 crc32 를 쓴다.

    spid 는 시즌×100만 + 선수 번호라 spid % n 은 시즌 경계에 따라 몰릴 수 있다. 해시로 섞는다.
    """
    return zlib.crc32(str(int(spid)).encode("ascii")) % n


def floor_hour(at: datetime) -> datetime:
    return at.astimezone(_KST).replace(minute=0, second=0, microsecond=0)


def hot_slot(bucket_at: datetime) -> int:
    return bucket_at.astimezone(_KST).hour


def tail_slot(bucket_at: datetime) -> int:
    kst = bucket_at.astimezone(_KST)
    return kst.weekday() * 24 + kst.hour


def pending_buckets(last: Optional[datetime], now: datetime,
                    limit: int = 1, max_lag: timedelta = MAX_LAG) -> List[datetime]:
    """이번 실행이 처리할 시각 버킷(정시, 오래된 순).

    마지막으로 끝낸 버킷 다음 정시부터 지금 정시까지가 밀린 몫이고, 앞에서 [limit] 개만 한다.
    상태가 없거나 [max_lag] 넘게 밀렸으면 지금 버킷 하나부터 새로 시작한다(밀린 tail 은 다음 주기에 돈다).
    """
    current = floor_hour(now)
    if last is None or current - floor_hour(last) > max_lag:
        return [current]
    out: List[datetime] = []
    at = floor_hour(last) + timedelta(hours=1)
    while at <= current and len(out) < limit:
        out.append(at)
        at += timedelta(hours=1)
    return out


def bucket_spids(bucket_at: datetime, all_spids: Iterable[int], hot: Set[int],
                 skip: Set[int] = frozenset()) -> Tuple[List[int], List[int]]:
    """버킷 하나에서 받을 카드 → (hot, tail). tail 버킷에 걸린 hot 카드는 건너뛴다(hot 쪽에서 받는다).

    hot 도 [all_spids] 안에 있는 것만 받는다(지워진 카드·잘못된 스쿼드 값 제외). [skip] 은 이번
    실행에서 이미 받은 카드(신규).
    """
    h, t = hot_slot(bucket_at), tail_slot(bucket_at)
    hot_out: List[int] = []
    tail_out: List[int] = []
    for spid in sorted(set(all_spids)):
        if spid in skip:
            continue
        if spid in hot:
            if bucket_of(spid, HOT_BUCKETS) == h:
                hot_out.append(spid)
        elif bucket_of(spid, TAIL_BUCKETS) == t:
            tail_out.append(spid)
    return hot_out, tail_out


# ---- 넥슨 요청 속도 ------------------------------------------------------------
HARD_MAX_RATE = 5.0  # 어떤 설정으로도 넘기지 않는 초당 요청 수


def _retry_after(raw: Optional[str], default: int) -> int:
    """Retry-After 초(숫자만 믿는다). 없거나 이상하면 [default], 너무 길면 5분으로 자른다."""
    try:
        return max(1, min(300, int(raw)))
    except (TypeError, ValueError):
        return default


class RateGate:
    """모든 워커가 함께 지나는 넥슨 요청 문(iteminfo_job.Fetcher 와 같은 규칙).

    - 요청 사이 간격을 1/[max_rate] 초 이상으로 벌린다(워커 수와 무관한 전체 상한, 최대 5 req/s).
    - 429·403 을 받으면 Retry-After(없으면 30·60·120초)만큼 모든 워커가 함께 쉰다. 쉬는 동안
      이미 나가 있던 요청이 돌려준 429 는 같은 파도로 보고 더 세지 않는다.
    - 그래도 막히면 [blocked] 를 세워 수집을 멈춘다 — 막힌 채 계속 두드리면 더 오래 막힌다.
    - 성공 응답이 오면 단계를 처음으로 되돌린다.
    """

    BACKOFF = (30, 60, 120)

    def __init__(self, max_rate: float, sleep=time.sleep, clock=time.monotonic):
        rate = min(max_rate, HARD_MAX_RATE) if max_rate > 0 else HARD_MAX_RATE
        self.min_interval = 1.0 / rate
        self.sleep, self.clock = sleep, clock
        self._lock = threading.Lock()
        self._next_at = 0.0
        self._paused_until = 0.0
        self._strikes = 0
        self.blocked = False
        self.throttled = 0  # 429·403 을 받은 횟수(로그용)

    def wait(self) -> None:
        with self._lock:
            now = self.clock()
            at = max(now, self._next_at, self._paused_until)
            self._next_at = at + self.min_interval
        if at > now:
            self.sleep(at - now)

    def ok(self) -> None:
        with self._lock:
            self._strikes = 0

    def throttle(self, status: int, retry_after: Optional[str]) -> None:
        with self._lock:
            self.throttled += 1
            now = self.clock()
            if now < self._paused_until:
                return
            if self._strikes >= len(self.BACKOFF):
                self.blocked = True
                return
            wait = _retry_after(retry_after, self.BACKOFF[self._strikes])
            self._strikes += 1
            self._paused_until = now + wait
        logger.warning("넥슨 HTTP %s — 모든 워커가 %s초 쉽니다", status, wait)


def _fetch_one(session, gate: RateGate, spid: int, strong: int, max_retries: int,
               delay: float) -> Optional[dict]:
    """(spid, strong) 의 최신 시세 1줄을 받아온다. 빈 값/실패/막힘이면 None.

    price 는 넥슨 "현재가"(2시간 주기 기준가)다. 일별 추이의 마지막 칸은 어제 값이라
    앱의 검색·가격 필터·구단가치가 하루 늦은 값으로 계산됐다(2026-09-28 수정).
    현재가가 없는 거래 불가 선수만 예전처럼 추이 마지막 칸으로 폴백한다 —
    그래야 그 선수들이 가격 필터에서 통째로 사라지지 않는다.

    429·403 은 재시도 횟수에 넣지 않고 [gate] 가 정한 만큼 쉬고 다시 묻는다. 요청마다 이 워커는
    [delay]~2×[delay] 초를 더 쉰다(일정한 간격으로 두드리지 않게).
    """
    attempt = 0
    while not gate.blocked:
        gate.wait()
        if gate.blocked:
            return None
        error: Optional[Exception] = None
        snapshot: Optional[PriceSnapshot] = None
        try:
            snapshot = fetch_price_snapshot(session, spid, strong, recent_days=1)
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else 0
            if status in (403, 429):
                gate.throttle(status, exc.response.headers.get("Retry-After"))
                continue
            error = exc
        except requests.RequestException as exc:
            error = exc
        finally:
            if delay:
                time.sleep(delay * (1 + random.random()))
        if snapshot is None:
            attempt += 1
            if attempt >= max_retries:
                logger.warning("fetch 실패 spid=%s strong=%s: %s", spid, strong, error)
                return None
            time.sleep(1.5 * attempt)  # 백오프
            continue
        gate.ok()
        if snapshot.current_price is not None:
            return {
                "spid": spid,
                "strong": strong,
                "price": snapshot.current_price,
                "price_date": _today_kst(),
            }
        if not snapshot.history:
            return None
        last_date, last_price = snapshot.history[-1]
        return {
            "spid": spid,
            "strong": strong,
            "price": int(last_price),
            "price_date": last_date,
        }
    return None


def _chunked(items, size: int):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def _batches_by_spid(rows: List[dict], size: int):
    """spid 순으로 정렬해 size 행 안팎으로 자르되 한 spid 의 단계들은 쪼개지 않는다.

    새 표는 카드당 한 행이라 같은 카드가 여러 배치에 흩어지면 그 행을 배치 수만큼
    다시 쓴다(dead tuple·WAL 이 그만큼 는다).
    """
    batch: List[dict] = []
    for row in sorted(rows, key=lambda r: (r["spid"], r["strong"])):
        if len(batch) >= size and batch[-1]["spid"] != row["spid"]:
            yield batch
            batch = []
        batch.append(row)
    if batch:
        yield batch


class CrawlStats:
    def __init__(self) -> None:
        self.done = 0
        self.written = 0
        self.changed = 0
        self.busy = 0.0  # crawl() 안에서 보낸 시간(쉬는 시간 제외)
        self.started = time.monotonic()

    def elapsed(self) -> float:
        return time.monotonic() - self.started

    def busy_rate(self) -> float:
        return self.done / self.busy if self.busy else 0.0


class Crawler:
    def __init__(self, engine, session, gate: RateGate, strongs: List[int], workers: int,
                 delay: float, max_retries: int, chunk_size: int, db_batch: int):
        self.engine, self.session, self.gate = engine, session, gate
        self.strongs, self.workers, self.delay = strongs, workers, delay
        self.max_retries, self.db_batch = max_retries, db_batch
        # 청크를 카드 경계에 맞춘다(한 카드의 단계들이 한 청크 = 한 쓰기에 들어가게).
        self.spids_per_chunk = max(1, chunk_size // max(1, len(strongs)))
        self.stats = CrawlStats()

    def crawl(self, label: str, spids: Sequence[int]) -> bool:
        """[spids] 를 받아 쓴다. 다 받았으면 True, 넥슨이 막아 중간에 멈췄으면 False."""
        total = len(spids) * len(self.strongs)
        logger.info("%s 시작: spids=%d × strongs=%d → tasks=%d", label, len(spids), len(self.strongs), total)
        done = 0
        started = time.monotonic()
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            for spid_chunk in _chunked(list(spids), self.spids_per_chunk):
                futures = [
                    executor.submit(_fetch_one, self.session, self.gate, spid, strong,
                                    self.max_retries, self.delay)
                    for spid in spid_chunk
                    for strong in self.strongs
                ]
                buffer: List[dict] = []
                for future in as_completed(futures):
                    row = future.result()
                    if row is not None:
                        buffer.append(row)
                # 막혀도 받은 값은 쓴다(멱등). 버킷은 끝난 것으로 치지 않는다.
                for batch in _batches_by_spid(buffer, self.db_batch):
                    written, changed = bulk_upsert_latest(self.engine, batch)
                    self.stats.written += written
                    self.stats.changed += changed
                done += len(futures)
                self.stats.done += len(futures)
                elapsed = time.monotonic() - started
                logger.info(
                    "%s 진행 %d/%d (%.1f%%) written=%d changed=%d %.2f req/s",
                    label, done, total, 100 * done / total if total else 100,
                    self.stats.written, self.stats.changed, done / elapsed if elapsed else 0,
                )
                if self.gate.blocked:
                    logger.warning("%s: 넥슨이 계속 막아 멈춥니다(남은 작업 %d)", label, total - done)
                    self.stats.busy += time.monotonic() - started
                    return False
        self.stats.busy += time.monotonic() - started
        return True


def load_bucket(conn, bucket_at: datetime, new_cap: int) -> Tuple[List[int], List[int], List[int]]:
    """버킷 하나를 돌 준비 → (먼저 받을 신규 카드, hot 몫, tail 몫). hot 집합은 버킷마다 다시 센다."""
    new = read_new_spids(conn)[:max(0, new_cap)]
    all_spids = read_spids(conn)
    hot = read_hot_spids(conn) & set(all_spids)
    hot_part, tail_part = bucket_spids(bucket_at, all_spids, hot, skip=set(new))
    logger.info(
        "버킷 %s 준비: 전 카드 %d, hot %d (%.1f%%) → 신규 %d + hot %d + tail %d",
        f"{bucket_at.astimezone(_KST):%m-%d %H}시", len(all_spids), len(hot),
        100 * len(hot) / max(1, len(all_spids)), len(new), len(hot_part), len(tail_part),
    )
    return new, hot_part, tail_part


# 따라잡은 뒤 다음 버킷을 기다릴 때 정시보다 조금 늦게 깬다(시계 오차·정시 혼잡 회피).
WAKE_DELAY = timedelta(seconds=90)
# 아직 속도를 못 쟀을 때 버킷 소요 시간 추정에 쓰는 속도(스모크·첫 실행 실측 3.1~3.3 req/s).
ASSUMED_RATE = 3.0


def next_action(last: Optional[datetime], now: datetime, elapsed: float, budget: float,
                bucket_estimate: float) -> Tuple[str, Optional[datetime], float]:
    """긴 실행이 다음에 할 일 → ("crawl", 버킷, 0) / ("sleep", None, 초) / ("stop", None, 0).

    밀린 버킷이 있으면 가장 오래된 하나를 받는다(한 번에 하나씩, 몰아 받지 않는다). 없으면 다음 정시까지
    잔다. 받는 데([bucket_estimate] 초)나 기다렸다 받는 데 실행 예산([budget] 초)을 넘기면 멈춘다 —
    워크플로가 다음 실행을 바로 이어 띄운다.
    """
    pending = pending_buckets(last, now, limit=1)
    if pending:
        if elapsed + bucket_estimate > budget:
            return "stop", None, 0.0
        return "crawl", pending[0], 0.0
    wait = (floor_hour(now) + timedelta(hours=1) + WAKE_DELAY - now).total_seconds()
    if elapsed + wait + bucket_estimate > budget:
        return "stop", None, 0.0
    return "sleep", None, wait


def run_buckets(engine, crawler: Crawler, budget: float, new_cap: int, sleep=time.sleep) -> bool:
    """매시 방식의 긴 실행. 상태 표를 버킷마다 다시 읽고, 끝낸 버킷만 적는다. 막히면 False."""
    started = time.monotonic()
    strongs = len(crawler.strongs)
    while True:
        with engine.connect() as conn:
            last = read_last_bucket(conn)
        elapsed = time.monotonic() - started
        rate = crawler.stats.busy_rate() or ASSUMED_RATE
        # 버킷 크기는 준비해 봐야 알므로, 먼저 평균 크기(전 카드×13÷168 + hot×13÷24 ≈ 9,000건)로 가늠한다.
        action, bucket_at, wait = next_action(
            last, datetime.now(timezone.utc), elapsed, budget, BUCKET_TASKS_GUESS / rate)
        if action == "stop":
            logger.info("실행 예산(%.0f분) 안에 다음 버킷을 끝낼 수 없어 멈춥니다(경과 %.0f분, 마지막 버킷 %s)",
                        budget / 60, elapsed / 60,
                        last.astimezone(_KST).isoformat() if last else "없음")
            return True
        if action == "sleep":
            logger.info("밀린 버킷이 없습니다. %.0f초 쉬고 다음 정시 버킷을 받습니다.", wait)
            sleep(wait)
            continue

        with engine.connect() as conn:
            new, hot_part, tail_part = load_bucket(conn, bucket_at, new_cap)
        estimate = (len(new) + len(hot_part) + len(tail_part)) * strongs / rate
        if elapsed + estimate > budget:
            logger.info("버킷 %s 은 예산 안에 못 끝나(예상 %.0f분) 다음 실행에 넘깁니다.",
                        bucket_at.astimezone(_KST).isoformat(), estimate / 60)
            return True
        if new and not crawler.crawl("신규 카드", new):
            return False
        label = f"버킷 {bucket_at.astimezone(_KST):%m-%d %H}시 (hot {len(hot_part)} + tail {len(tail_part)})"
        changed_before = crawler.stats.changed
        if not crawler.crawl(label, hot_part + tail_part):
            return False
        write_last_bucket(engine, bucket_at)
        logger.info("상태 표: 마지막 버킷 = %s (누적 %.2f req/s, 429·403=%d)",
                    bucket_at.astimezone(_KST).isoformat(), crawler.stats.busy_rate(), crawler.gate.throttled)
        # 6시간에 한 번(KST 04·10·16·22시 버킷)만 청소한다. 받은 행의 66% 가 값이 바뀌어(현재가는 2시간마다
        # 움직인다) 행 수 임계로는 매시 돌게 된다 — 매시 VACUUM 은 그 자체가 IO 다.
        if hot_slot(bucket_at) % VACUUM_EVERY_HOURS == 4 and crawler.stats.changed > changed_before:
            _vacuum(engine)


def _vacuum(engine) -> None:
    try:
        vacuum_analyze(engine)
        logger.info("VACUUM (ANALYZE) 완료: %s.player_price_latest·player_price_strongs", PLAYER_SCHEMA)
    except Exception as exc:  # 청소 실패가 배치 성공을 뒤집지 않게.
        logger.warning("VACUUM 스킵(실패): %s", exc)


def run() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    strongs = _parse_strongs(os.getenv("PRICE_LATEST_STRONGS", "1-13"))
    workers = max(1, _env_int("PRICE_LATEST_WORKERS", 3))
    chunk_size = _env_int("PRICE_LATEST_CHUNK_SIZE", 1300)
    db_batch = _env_int("PRICE_LATEST_DB_BATCH", 500)
    max_retries = _env_int("PRICE_LATEST_MAX_RETRIES", 3)
    delay = _env_float("PRICE_LATEST_REQUEST_DELAY", 0.4)
    max_rate = _env_float("PRICE_LATEST_MAX_RATE", 4.0)
    budget = _env_float("PRICE_LATEST_BUDGET_MIN", 330) * 60
    new_cap = _env_int("PRICE_LATEST_NEW_CAP", 100)
    spid_limit = _env_int("PRICE_LATEST_LIMIT", 0)
    only_new = os.getenv("PRICE_LATEST_ONLY_NEW", "0") == "1"
    full = os.getenv("PRICE_LATEST_FULL", "0") == "1"

    # 동시 요청 수만큼 커넥션 풀을 키운다(HTTP 는 워커 스레드에서만 일어난다).
    session = make_session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=workers, pool_maxsize=workers, max_retries=0
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    engine = make_engine()
    gate = RateGate(max_rate)
    crawler = Crawler(engine, session, gate, strongs, workers, delay, max_retries, chunk_size, db_batch)
    logger.info(
        "설정: workers=%d delay=%.2f~%.2fs 전체 상한=%.1f req/s 예산=%.0f분 strongs=%s",
        workers, delay, 2 * delay, 1 / gate.min_interval, budget / 60, strongs,
    )

    # 수동 실행(신규만·전 카드·스모크 limit)은 한 번 돌고 끝낸다. 상태 표는 건드리지 않는다.
    one_shot: Optional[Tuple[str, List[int]]] = None
    # spid 목록만 읽고 커넥션을 바로 돌려준다. 이후 쓰기는 flush 마다 새로 체크아웃한다
    # (bulk_upsert_latest 주석 참고).
    with engine.connect() as conn:
        if only_new:
            one_shot = ("신규 카드", read_new_spids(conn))
        elif full:
            one_shot = ("전 카드", read_spids(conn))
        elif spid_limit > 0:
            bucket_at = floor_hour(datetime.now(timezone.utc))
            new, hot_part, tail_part = load_bucket(conn, bucket_at, new_cap)
            one_shot = (f"버킷 {bucket_at.astimezone(_KST):%m-%d %H}시 [limit]", new + hot_part + tail_part)
    if one_shot is not None:
        label, spids = one_shot
        if spid_limit > 0:
            spids = spids[:spid_limit]
        ok = crawler.crawl(label, spids)
        if ok and crawler.stats.changed and (full or only_new):
            _vacuum(engine)
    else:
        ok = run_buckets(engine, crawler, budget, new_cap)

    stats = crawler.stats
    logger.info(
        "배치 완료: tasks=%d written=%d changed=%d elapsed=%.0fs %.2f req/s 429·403=%d",
        stats.done, stats.written, stats.changed, stats.elapsed(), stats.busy_rate(), gate.throttled,
    )
    if not ok:
        raise SystemExit("넥슨이 429·403 으로 계속 막았습니다 — 끝내지 못한 버킷은 다음 실행이 다시 받습니다.")


if __name__ == "__main__":
    run()
