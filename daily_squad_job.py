"""
FC Online 팀컬러 이용률 일일 크롤러 — 공개 저장소용 자기완결형 스크립트.

넥슨 데이터센터 데일리 차트는 상위 1만 랭커의 **전일 공식경기(1 ON 1) 스쿼드**를
집계해 팀컬러 이용률 30위를 공개한다. 우리가 랭커를 순회해 팀컬러를 역산할 필요가
없으므로, 하루 한 번 이 페이지만 받아 player.team_color_usage 를 갈아끼운다.

과거분은 보관하지 않는다. 같은 자리 (match_mode, rank) 를 덮어써 항상 30행만 남는다.

⚠️ 비밀정보(DATABASE_URL)는 절대 이 파일/저장소에 두지 않는다. GitHub Secret 으로만
   주입하고, 이 스크립트는 DATABASE_URL 을 로그에 출력하지 않는다.

넥슨 엔드포인트: GET https://fconline.nexon.com/datacenter/dailysquad
  응답: SSR HTML. `#divTeamColorInfo` 안에 30개 항목, `#strDate` 에 집계 기준일.
  (쿠키/CSRF 불필요)

⚠️ 실패 시 DB 를 건드리지 않고 종료 코드 1 로 끝낸다. 목록이 비었는데 조용히 넘어가면
   앱이 몇 주 전 순위를 계속 보여줘도 아무도 모른다 — 반드시 배치를 빨간불로 만든다.

⚠️ 팀컬러 식별자: onclick 의 `GetTeamColorVsInfo('1018')` 은 넥슨 데이터센터 내부
   번호이고 우리 player.team_colors.id 와 **다른 체계**다(2026-09-17 실측: 30건 전부
   불일치). 그래서 매칭은 이름으로 한다 — 같은 날 30건 전부 유일 매칭을 확인했다.

환경변수
    DATABASE_URL            (필수) Postgres 접속 문자열. 로그에 절대 출력 안 함.
    DAILY_SQUAD_TIMEOUT     넥슨 요청 타임아웃 초 (기본 30)

로컬 테스트:
    DATABASE_URL=postgresql://... python daily_squad_job.py
"""

import logging
import os
import re
import sys
from dataclasses import dataclass
from datetime import date
from typing import Dict, List, Optional

import requests
from sqlalchemy import create_engine, text

BASE_URL = "https://fconline.nexon.com/datacenter/dailysquad"

PLAYER_SCHEMA = "player"
USAGE_TABLE = f"{PLAYER_SCHEMA}.team_color_usage"
TEAM_COLORS_TABLE = f"{PLAYER_SCHEMA}.team_colors"

MATCH_MODE_OFFICIAL_1ON1 = "official_1on1"

# 집계 기준일(전일). 페이지 상단 datepicker 의 value 가 곧 기준일이다.
_STAT_DATE_RE = re.compile(r'value="(\d{4})\.(\d{2})\.(\d{2})"\s+id="strDate"')

_LIST_ANCHOR = 'id="divTeamColorInfo"'
_ITEM_SEPARATOR = "teamcolor_list__item"

_NAME_RE = re.compile(r'<div class="txt">(.*?)</div>', re.S)
_PER_RE = re.compile(r'<div class="per">\s*([\d,]+)명\s*\(\s*([\d.]+)%\s*\)')
_NEXON_ID_RE = re.compile(r"GetTeamColorVsInfo\(\s*'(\d+)'\s*\)")
_TIER_RE = re.compile(r'<div class="level lv(\d+)"')
_TAG_RE = re.compile(r"<[^>]+>")

# 항목 하나의 마크업 길이 상한. 다음 항목의 값을 잘못 집어오는 것을 막는다.
_ITEM_WINDOW = 3000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("daily_squad")


@dataclass(frozen=True)
class TeamColorUsage:
    rank: int
    name: str
    user_count: int
    usage_rate: float
    tier: Optional[int]
    nexon_team_color_id: Optional[int]


class DailySquadUnavailable(RuntimeError):
    """데일리 차트를 읽지 못했다. 기존 적재분은 그대로 두고 배치만 실패시킨다."""


# ── 넥슨 ────────────────────────────────────────────────────────────────────
def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})
    return session


def fetch_html(session: requests.Session) -> str:
    timeout = int(os.getenv("DAILY_SQUAD_TIMEOUT", "30"))
    response = session.get(BASE_URL, timeout=timeout)
    response.raise_for_status()
    # 넥슨은 charset 을 헤더에 싣지 않는 경우가 있어 requests 가 latin-1 로 추측한다.
    response.encoding = "utf-8"
    return response.text


def parse_stat_date(html: str) -> Optional[date]:
    match = _STAT_DATE_RE.search(html)
    if not match:
        return None
    year, month, day = (int(part) for part in match.groups())
    return date(year, month, day)


def _clean(value: str) -> str:
    return _TAG_RE.sub("", value).strip()


def parse_team_color_usage(html: str) -> List[TeamColorUsage]:
    anchor = html.find(_LIST_ANCHOR)
    if anchor == -1:
        return []

    rows: List[TeamColorUsage] = []
    for chunk in html[anchor:].split(_ITEM_SEPARATOR)[1:]:
        window = chunk[:_ITEM_WINDOW]

        name_match = _NAME_RE.search(window)
        per_match = _PER_RE.search(window)
        if not name_match or not per_match:
            continue

        name = _clean(name_match.group(1))
        if not name:
            continue

        tier_match = _TIER_RE.search(window)
        nexon_id_match = _NEXON_ID_RE.search(window)

        rows.append(
            TeamColorUsage(
                rank=len(rows) + 1,
                name=name,
                user_count=int(per_match.group(1).replace(",", "")),
                usage_rate=float(per_match.group(2)),
                tier=int(tier_match.group(1)) if tier_match else None,
                nexon_team_color_id=(
                    int(nexon_id_match.group(1)) if nexon_id_match else None
                ),
            )
        )
    return rows


def estimate_sample_size(rows: List[TeamColorUsage]) -> Optional[int]:
    """집계 모수(전일 공식경기를 치른 랭커 수) 추정.

    넥슨은 모수를 직접 노출하지 않는다. 인원수/비율로 역산하되 비율이 소수점 한
    자리라 오차가 크므로, 인원수가 가장 많은(=상대오차가 가장 작은) 항목을 쓴다.
    """
    usable = [row for row in rows if row.usage_rate > 0]
    if not usable:
        return None
    top = max(usable, key=lambda row: row.user_count)
    return round(top.user_count * 100 / top.usage_rate)


# ── DB ──────────────────────────────────────────────────────────────────────
def make_engine():
    """DATABASE_URL 로 엔진 생성. 비밀번호가 들어있으므로 절대 로그에 찍지 않는다."""
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise SystemExit(
            "DATABASE_URL 환경변수가 없습니다. "
            "(GitHub Secret 으로 주입하거나 로컬 테스트 시 export 하세요)"
        )
    url = url.replace("postgres://", "postgresql://", 1)
    if url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    return create_engine(url, pool_pre_ping=True, pool_recycle=600)


def _normalized_key(value: str) -> str:
    return re.sub(r"\s+", "", value).upper()


def read_team_color_ids(conn) -> Dict[str, int]:
    """이름 → team_colors.id. 같은 이름이 둘 이상이면 모호하므로 제외한다.

    데일리 차트 목록은 소속 팀컬러(클럽·국가)라 club 종류로 한정하면 관계/강화
    컬러의 동명 이의와 부딪히지 않는다.
    """
    rows = conn.execute(
        text(
            f"SELECT id, name FROM {TEAM_COLORS_TABLE} "
            "WHERE team_color_type = 'club'"
        )
    ).all()

    candidates: Dict[str, List[int]] = {}
    for team_color_id, name in rows:
        candidates.setdefault(_normalized_key(name), []).append(team_color_id)
    return {
        name_key: ids[0]
        for name_key, ids in candidates.items()
        if len(set(ids)) == 1
    }


# 행 수와 무관하게 SQL 텍스트가 항상 같아야 한다(executemany 로 바인드만 반복).
# 여러 행을 multi-VALUES 로 펼치면 행 수마다 다른 쿼리가 되어 pg_stat_statements
# 텍스트가 비대해진다 — 2026-07 Disk IO 경고의 원인.
_UPSERT_SQL = text(f"""
    INSERT INTO {USAGE_TABLE} (
        match_mode, rank, stat_date, team_color_id, team_color_name,
        user_count, usage_rate, tier, nexon_team_color_id, sample_size
    ) VALUES (
        :match_mode, :rank, :stat_date, :team_color_id, :team_color_name,
        :user_count, :usage_rate, :tier, :nexon_team_color_id, :sample_size
    )
    ON CONFLICT (match_mode, rank) DO UPDATE SET
        stat_date = EXCLUDED.stat_date,
        team_color_id = EXCLUDED.team_color_id,
        team_color_name = EXCLUDED.team_color_name,
        user_count = EXCLUDED.user_count,
        usage_rate = EXCLUDED.usage_rate,
        tier = EXCLUDED.tier,
        nexon_team_color_id = EXCLUDED.nexon_team_color_id,
        sample_size = EXCLUDED.sample_size,
        updated_at = now()
""")

# 넥슨이 30위보다 짧은 목록을 준 날, 지난 적재분의 꼬리가 남지 않게 지운다.
_TRIM_SQL = text(
    f"DELETE FROM {USAGE_TABLE} WHERE match_mode = :match_mode AND rank > :max_rank"
)


def build_records(
    rows: List[TeamColorUsage],
    *,
    stat_date: date,
    team_color_ids: Dict[str, int],
    sample_size: Optional[int],
) -> List[dict]:
    records = []
    for row in rows:
        team_color_id = team_color_ids.get(_normalized_key(row.name))
        if team_color_id is None:
            # 조용히 버리지 않는다. 신규 팀컬러가 우리 DB 에 아직 없다는 신호다.
            logger.warning(
                "팀컬러 이름 매칭 실패: %s (%s %d위) — 메타 크롤이 밀렸을 수 있습니다",
                row.name,
                stat_date,
                row.rank,
            )
        records.append(
            {
                "match_mode": MATCH_MODE_OFFICIAL_1ON1,
                "rank": row.rank,
                "stat_date": stat_date,
                "team_color_id": team_color_id,
                "team_color_name": row.name,
                "user_count": row.user_count,
                "usage_rate": row.usage_rate,
                "tier": row.tier,
                "nexon_team_color_id": row.nexon_team_color_id,
                "sample_size": sample_size,
            }
        )
    return records


def main() -> None:
    session = make_session()
    html = fetch_html(session)

    # 아래 두 경우 모두 DB 에는 손대지 않는다. 화면이 빈 표로 바뀌는 것보다 어제 값이
    # 남는 편이 낫고, 낡았다는 사실은 앱이 stat_date 로 드러낸다.
    rows = parse_team_color_usage(html)
    if not rows:
        raise DailySquadUnavailable(
            "팀컬러 이용률 목록이 비어 있습니다 — 넥슨 마크업이 바뀌었을 수 있습니다"
        )

    stat_date = parse_stat_date(html)
    if stat_date is None:
        raise DailySquadUnavailable("집계 기준일(#strDate)을 찾지 못했습니다")

    sample_size = estimate_sample_size(rows)
    logger.info(
        "파싱 완료: %s 기준 %d행 (모수 추정 %s명)", stat_date, len(rows), sample_size
    )

    engine = make_engine()
    with engine.begin() as conn:
        records = build_records(
            rows,
            stat_date=stat_date,
            team_color_ids=read_team_color_ids(conn),
            sample_size=sample_size,
        )
        conn.execute(_UPSERT_SQL, records)
        conn.execute(
            _TRIM_SQL,
            {"match_mode": MATCH_MODE_OFFICIAL_1ON1, "max_rank": len(records)},
        )

    matched = sum(1 for record in records if record["team_color_id"] is not None)
    logger.info("적재 완료: %d행 (이름 매칭 %d행)", len(records), matched)


if __name__ == "__main__":
    main()
