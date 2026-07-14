"""
FC Online 선수 "메타(능력치·특성·주발 등)" 주간 크롤러 — 공개 저장소용 자기완결형 스크립트.

GitHub Actions 에서 두 워크플로가 이 스크립트를 돌린다:
  · meta-thursday.yml — 목요일 패치 완료 시각이 매주 달라서, 목 10~23시 KST 매시
    sync 로 폴링하다가 신규 spid/시즌이 감지된 시각에만 신규 카드를 크롤(META_ONLY_NEW=1).
  · meta-friday.yml  — 라이브 퍼포먼스 반영(금 11:00 KST) 직후 전체 선수를 재크롤.
선수 카드 내용을 콘텐츠 해시로 떠서 **바뀐 카드만 DB write** 한다(안 바뀐 ~8.8만 장을
매번 덮어쓰지 않음 → Disk IO/bloat 절감, 시세 크롤러의 no-op skip 과 같은 취지).

크롤 도중 (1)처음 보는 spid = 신규 선수, (2)처음 보는 특성 = 신규 특성, (3)해시가
바뀐 기존 선수 = 라이브 퍼포먼스 변동 을 감지해 player.meta_crawl_log 에 적는다.
모든 샤드가 끝나면 notify 잡이 이 로그를 모아 카카오톡 요약 1통을 보낸다.

⚠️ 비밀정보(DATABASE_URL)는 절대 이 파일/저장소에 두지 않는다. GitHub Secret 으로만 주입.
   이 스크립트는 DATABASE_URL 을 로그에 절대 출력하지 않는다.

────────────────────────────────────────────────────────────────────────────
두 가지 모드(META_MODE 환경변수):

  sync   : spid.json / seasonid.json 을 받아 player.spid / player.seasonid 에 upsert.
           이때 처음 등장한 spid(=신규 선수)와 신규 시즌을 phase='sync' 로그로 남긴다.
           ⚠️ 병렬 crawl 샤드들보다 *먼저* 1회만 돈다(workflow 의 needs 로 보장).

  crawl  : (기본) player.spid 전체를 SHARD 로 나눠 자기 몫만 크롤. 카드 해시 비교 후
           신규/변경만 write. 샤드별 결과를 phase='crawl' 로그로 남긴다.
           META_ONLY_NEW=1 이면 crawled=FALSE 인 spid(=sync 가 방금 넣은 신규 +
           과거 크롤 실패분)만 크롤한다(목요일 폴링용 경량 모드).

⚠️ 이 크롤러의 샤딩은 시세 크롤러(crawl.yml)와 정반대다. 시세는 "6토막 시간분산
   sequential(동시 1, 버스트 회피)"이고, 이건 "matrix 4샤드 병렬(주1회라 빨리 끝내려는
   의도적 burst)". 둘을 같은 방식이라 여기지 말 것.

환경변수
    DATABASE_URL            (필수) Postgres 접속 문자열. 로그에 절대 출력 안 함.
    META_MODE               'sync' | 'crawl'(기본)
    META_RUN_ID             이 주(週) 실행을 묶는 ID(workflow 가 주입). 없으면 날짜로 생성.
    SHARD_INDEX/SHARD_COUNT  crawl 샤딩. 각 샤드가 spids[INDEX::COUNT] 담당 (기본 0/1)
    META_WORKERS            HTML fetch 동시 스레드 수 (기본 8)
    META_REQUEST_DELAY      각 요청 후 sleep 초 (기본 0.2)
    META_MAX_RETRIES        넥슨 호출 재시도 횟수 (기본 3)
    META_LIMIT              처리할 spid 수 상한 (기본 0=전체; 샤딩 전 적용, 테스트용)
    META_ONLY_NEW           '1' 이면 crawl 대상을 crawled=FALSE 인 spid 로 한정 (기본 0)

로컬 테스트:
    DATABASE_URL=postgresql://... META_MODE=sync python meta_crawler.py
    DATABASE_URL=postgresql://... META_LIMIT=30 python meta_crawler.py
"""

import hashlib
import json
import logging
import os
import re
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text

logger = logging.getLogger("meta_crawler")

PLAYER_SCHEMA = "player"

PLAYER_URL = "https://open.api.nexon.com/static/fconline/meta/spid.json"
SEASON_URL = "https://open.api.nexon.com/static/fconline/meta/seasonid.json"
ABILITY_URL = "https://fconline.nexon.com/datacenter/PlayerAbility"


# =============================================================================
# STAT 매핑 (넥슨 한글 라벨 → DB 컬럼)
# =============================================================================
def _normalize(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", s)
    s = re.sub(r"[\s ​‌‍]+", "", s)
    return s.strip()


STAT_MAP = {
    _normalize("속력"): "pace",
    _normalize("가속력"): "acceleration",
    _normalize("골 결정력"): "finishing",
    _normalize("슛 파워"): "shot_power",
    _normalize("중거리 슛"): "long_shot",
    _normalize("위치 선정"): "positioning",
    _normalize("발리슛"): "volley",
    _normalize("페널티 킥"): "penalty_kick",
    _normalize("짧은 패스"): "short_pass",
    _normalize("시야"): "vision",
    _normalize("크로스"): "crossing",
    _normalize("긴 패스"): "long_pass",
    _normalize("프리킥"): "free_kick",
    _normalize("커브"): "curve",
    _normalize("드리블"): "dribble",
    _normalize("볼 컨트롤"): "ball_control",
    _normalize("민첩성"): "agility",
    _normalize("밸런스"): "balance",
    _normalize("반응 속도"): "reactions",
    _normalize("대인 수비"): "marking",
    _normalize("태클"): "tackle",
    _normalize("가로채기"): "interception",
    _normalize("슬라이딩 태클"): "sliding_tackle",
    _normalize("헤더"): "heading",
    _normalize("몸싸움"): "strength",
    _normalize("스태미너"): "stamina",
    _normalize("적극성"): "aggression",
    _normalize("점프"): "jumping",
    _normalize("침착성"): "composure",
    _normalize("GK 다이빙"): "gk_diving",
    _normalize("GK 핸들링"): "gk_handling",
    _normalize("GK 킥"): "gk_kick",
    _normalize("GK 반응속도"): "gk_reflexes",
    _normalize("GK 위치 선정"): "gk_positioning",
}

# players 테이블에 쓰는 스탯 컬럼 전체(해시 정규화에 사용).
STAT_COLUMNS = sorted(set(STAT_MAP.values()))


def _clean_nation_name(name: str) -> str:
    if not name:
        return name
    return re.sub(r",?\s*국가대표", "", name).strip()


# =============================================================================
# HTTP
# =============================================================================
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36"
        )
    })
    return s


def fetch_player_html(session: requests.Session, spid: int, timeout: int = 12) -> str:
    res = session.get(f"{ABILITY_URL}?spid={spid}&n1Strong=1", timeout=timeout)
    res.raise_for_status()
    return res.text


# =============================================================================
# PARSE — 넥슨 PlayerAbility HTML → 선수 dict
# =============================================================================
def parse_player_ability(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    result: dict = {}

    name_el = soup.select_one(".name")
    result["name"] = name_el.text.strip() if name_el else None
    if not result["name"]:
        raise ValueError("player name not found")

    positions = []
    for pos in soup.select(".info_line.info_ab .position"):
        txt_el = pos.select_one(".txt")
        value_el = pos.select_one(".value")
        if not txt_el or not value_el:
            continue
        positions.append({"position": txt_el.text.strip(), "ovr": int(value_el.text.strip())})
    if not positions:
        raise ValueError("positions not found")
    result["main_position"] = positions[0]
    result["sub_positions"] = positions[1:]

    pay_el = soup.select_one(".pay span")
    result["pay"] = int(pay_el.text.strip()) if pay_el else None

    def safe_text(selector: str) -> Optional[str]:
        el = soup.select_one(selector)
        return el.get_text(" ", strip=True) if el else None

    height_text = safe_text(".etc.height")
    weight_text = safe_text(".etc.weight")
    result["height"] = int(re.search(r"\d+", height_text).group()) if height_text else None
    result["weight"] = int(re.search(r"\d+", weight_text).group()) if weight_text else None
    result["body_type"] = safe_text(".etc.physical")

    skill_text = safe_text(".etc.skill")
    result["skill_moves"] = skill_text.count("★") if skill_text else None

    # ── 주발/약발 ────────────────────────────────────────────────────────
    # ⚠️ 넥슨은 주발을 <strong> 으로 표시한다: <span class="etc foot"><strong>L5</strong> – R4</span>
    #    주발 = <strong> 안의 발, weak_foot = 나머지 발의 등급.
    #    (옛 크롤러는 이를 무시하고 L/R 숫자 크기로 추론 → 양발(둘 다 5)인 왼발잡이를
    #     전부 R 로 잘못 저장하는 버그가 있었다. 숫자 비교는 동점에서 틀린다.)
    foot_el = soup.select_one(".etc.foot")
    preferred_foot = None
    weak_foot = None
    if foot_el:
        full_text = foot_el.get_text(" ", strip=True)
        ratings = {side: int(val) for side, val in re.findall(r"([LR])\s*(\d)", full_text)}
        strong_el = foot_el.select_one("strong")
        m = re.match(r"\s*([LR])", strong_el.get_text(strip=True)) if strong_el else None
        if m and ratings:
            preferred_foot = m.group(1)                       # 'L' or 'R'
            other = "R" if preferred_foot == "L" else "L"
            weak_foot = ratings.get(other)                    # 비주발 발의 등급
    result["preferred_foot"] = preferred_foot
    result["weak_foot"] = weak_foot

    nation_name = safe_text(".etc.nation .txt")
    nation_img_el = soup.select_one(".etc.nation img")
    result["nation"] = {
        "name": nation_name,
        "image_url": nation_img_el["src"] if nation_img_el else None,
    }

    traits = []
    for trait in soup.select(".skill_wrap > span"):
        img_el = trait.select_one("img")
        desc_el = trait.select_one(".desc")
        if not img_el or not desc_el:
            continue
        traits.append({"name": desc_el.text.strip(), "image_url": img_el["src"]})
    result["traits"] = traits

    team_colors = []
    for item in soup.select(".tdefault_wrap .selector_item"):
        t = item.get_text(strip=True)
        if t and t != "소속 팀컬러":
            team_colors.append({"name": t, "type": "club"})
    for item in soup.select(".tspecial_wrap .selector_item"):
        t = item.get_text(strip=True)
        if t and t != "관계 팀컬러":
            team_colors.append({"name": t, "type": "relation"})
    result["team_colors"] = team_colors

    stats: Dict[str, int] = {}
    for item in soup.select(".data_wrap_playerinfo li.ab"):
        txt_el = item.select_one(".txt")
        value_el = item.select_one(".value")
        if not txt_el or not value_el:
            continue
        col = STAT_MAP.get(_normalize(txt_el.text))
        if not col:
            continue
        match = re.search(r"\d+", value_el.get_text(" ", strip=True))
        if match:
            stats[col] = int(match.group())
    result["stats"] = stats

    return result


# =============================================================================
# DB 엔진
# =============================================================================
def make_engine():
    """DATABASE_URL 로 엔진 생성. 비밀번호가 들어있으므로 절대 로그에 찍지 않는다."""
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise SystemExit("DATABASE_URL 환경변수가 없습니다. (GitHub Secret 또는 로컬 export)")
    url = url.replace("postgres://", "postgresql://", 1)
    if url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    return create_engine(url, pool_pre_ping=True)


def _pg_array_literal(values) -> str:
    """리스트를 Postgres 배열 리터럴 문자열('{...}') 하나로 직렬화한다.

    ⚠️ 배열을 "파이썬 리스트"로 바인드하면 안 된다(시세 크롤러 2026-07-13 재발 원인).
    psycopg2 는 리스트를 ARRAY[v1, v2, ...] 로 인라인하므로 원소 수만큼 상수가 생기고,
    pg_stat_statements 가 배열 길이별로 별개 엔트리를 만들어 텍스트가 비대해진다
    (Supabase Disk IO 예산을 태운 그 패턴). 문자열 상수 1개($1)로 보내고 SQL 쪽
    CAST 가 배열로 파싱하게 한다. 모든 원소를 "..." 로 감싸므로 숫자/텍스트 어느
    타입 배열로도 캐스팅된다. None 은 NULL.
    """
    parts = []
    for v in values:
        if v is None:
            parts.append("NULL")
        else:
            s = str(v).replace("\\", "\\\\").replace('"', '\\"')
            parts.append('"' + s + '"')
    return "{" + ",".join(parts) + "}"


# =============================================================================
# 레퍼런스(nation/trait/team_color) get-or-create
#   ⚠️ 병렬 샤드가 같은 신규 이름을 동시에 insert 하면 중복 행이 생긴다.
#      unique 제약을 새로 걸면 기존 중복 데이터에서 마이그레이션이 깨질 수 있으므로,
#      트랜잭션 advisory lock(이름 해시 기준)으로 같은 이름 생성만 직렬화한다.
#   process 내에서는 캐시로 같은 이름을 두 번 조회하지 않는다.
# =============================================================================
class RefResolver:
    def __init__(self, engine):
        # ⚠️ 선수-쓰기 트랜잭션과 분리된 AUTOCOMMIT 커넥션. 레퍼런스 생성은 즉시 커밋되어,
        #    이후 선수 INSERT 가 롤백돼도 캐시의 id 가 무효가 되지 않는다.
        self.conn = engine.connect().execution_options(isolation_level="AUTOCOMMIT")
        self._nation: Dict[str, int] = {}
        self._trait: Dict[str, int] = {}
        self._team_color: Dict[str, int] = {}

    def close(self):
        self.conn.close()

    def _get_or_create(self, cache, table, name, insert_cols, lock_ns):
        if name in cache:
            return cache[name]
        # 같은 이름 생성 경합을 직렬화(다른 이름끼리는 안 막힘). AUTOCOMMIT 라 xact lock 은
        # 즉시 풀리므로 SELECT+INSERT 를 감싸는 세션 lock 을 쓰고 finally 에서 반드시 unlock.
        key = f"{lock_ns}:{name}"
        self.conn.execute(text("SELECT pg_advisory_lock(hashtext(:k))"), {"k": key})
        try:
            row = self.conn.execute(
                text(f"SELECT id FROM {PLAYER_SCHEMA}.{table} WHERE name = :name"),
                {"name": name},
            ).first()
            if row:
                new_id = row[0]
            else:
                cols = ", ".join(insert_cols.keys())
                binds = ", ".join(f":{k}" for k in insert_cols)
                new_id = self.conn.execute(
                    text(f"INSERT INTO {PLAYER_SCHEMA}.{table} ({cols}) VALUES ({binds}) RETURNING id"),
                    insert_cols,
                ).scalar()
        finally:
            self.conn.execute(text("SELECT pg_advisory_unlock(hashtext(:k))"), {"k": key})
        cache[name] = new_id
        return new_id

    def nation(self, name, image_url):
        return self._get_or_create(
            self._nation, "nations", name,
            {"name": name, "image_url": image_url}, "nation",
        )

    def team_color(self, name, ttype):
        return self._get_or_create(
            self._team_color, "team_colors", name,
            {"name": name, "team_color_type": ttype}, "team_color",
        )

    def trait(self, name, image_url):
        # 신규 특성 감지는 crawl_shard 의 known_trait_ids 스냅샷이 담당한다(여기선 id 만 해석).
        return self._get_or_create(
            self._trait, "traits", name,
            {"name": name, "image_url": image_url}, "trait",
        )


# =============================================================================
# 콘텐츠 해시
# =============================================================================
def content_hash(parsed: dict, season_id: int, nation_id: int,
                 trait_ids: List[int], team_color_ids: List[int]) -> str:
    """선수 카드의 '내용'을 정규화해 sha256. 같은 내용이면 같은 해시 → write 스킵 판단용."""
    payload = {
        "name": parsed["name"],
        "season_id": season_id,
        "nation_id": nation_id,
        "main_position": parsed["main_position"]["position"],
        "main_ovr": parsed["main_position"]["ovr"],
        "sub_positions": [x["position"] for x in parsed["sub_positions"]],
        "pay": parsed["pay"],
        "height": parsed["height"],
        "weight": parsed["weight"],
        "body_type": parsed["body_type"],
        "skill_moves": parsed["skill_moves"],
        "weak_foot": parsed["weak_foot"],
        "preferred_foot": parsed["preferred_foot"],
        "trait_ids": sorted(trait_ids),
        "team_color_ids": sorted(team_color_ids),
        "stats": {k: parsed["stats"].get(k) for k in STAT_COLUMNS},
    }
    blob = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


# =============================================================================
# SYNC 모드 — spid.json / seasonid.json upsert + 신규 감지
# =============================================================================
_SYNC_CHUNK = 10_000

# ⚠️ 8.8만 spid 를 한 행씩 보내면 GitHub Actions 러너(미국)↔서울 DB 왕복(~150ms)
#    만으로 3시간을 넘겨 잡 타임아웃에 걸린다. 시세 크롤러와 같은 unnest(배열 리터럴)
#    청크 업서트로 왕복을 청크당 1회로 줄인다(쿼리 텍스트도 항상 동일 → pgss 1엔트리).
#    RETURNING 은 실제로 INSERT/UPDATE 된 행만 반환하므로(no-op 스킵 행 제외)
#    "xmax=0 인 행 = 신규 spid" 감지 방식이 행별 업서트 때와 동일하게 유지된다.
_SPID_UPSERT_SQL = text(f"""
    INSERT INTO {PLAYER_SCHEMA}.spid (spid, name, crawled)
    SELECT t.spid, t.name, FALSE
    FROM unnest(CAST(:spids AS bigint[]), CAST(:names AS text[])) AS t(spid, name)
    ON CONFLICT (spid) DO UPDATE SET name = EXCLUDED.name
    WHERE spid.name IS DISTINCT FROM EXCLUDED.name
    RETURNING spid, (xmax = 0) AS inserted
""")


def _write_github_output(**kwargs) -> None:
    """GitHub Actions step output 기록(워크플로가 crawl 실행 여부를 게이트하는 데 사용).
    로컬 실행(GITHUB_OUTPUT 없음)에선 아무것도 안 한다."""
    path = os.getenv("GITHUB_OUTPUT")
    if not path:
        return
    with open(path, "a", encoding="utf-8") as f:
        for k, v in kwargs.items():
            f.write(f"{k}={v}\n")


def sync_meta(engine, run_id: str) -> None:
    session = make_session()
    player_data = session.get(PLAYER_URL, timeout=30).json()
    season_data = session.get(SEASON_URL, timeout=30).json()

    spid_limit = _env_int("META_LIMIT", 0)   # 테스트용 상한(0=전체)
    if spid_limit > 0:
        player_data = player_data[:spid_limit]

    with engine.begin() as conn:
        # 시즌 upsert + 신규 시즌 감지(xmax=0 → 이번에 INSERT 된 행). ~60행이라 행별로 충분.
        new_seasons = []
        for s in season_data:
            row = conn.execute(
                text(f"""
                    INSERT INTO {PLAYER_SCHEMA}.seasonid (season_id, class_name, season_img)
                    VALUES (:sid, :cn, :img)
                    ON CONFLICT (season_id) DO UPDATE
                      SET class_name = EXCLUDED.class_name, season_img = EXCLUDED.season_img
                    WHERE seasonid.class_name IS DISTINCT FROM EXCLUDED.class_name
                       OR seasonid.season_img IS DISTINCT FROM EXCLUDED.season_img
                    RETURNING season_id, (xmax = 0) AS inserted
                """),
                {"sid": s["seasonId"], "cn": s["className"], "img": s["seasonImg"]},
            ).first()
            if row and row.inserted:
                new_seasons.append({"season_id": s["seasonId"], "class_name": s["className"]})

        # spid upsert + 신규 spid 감지. 신규 행은 crawled=FALSE 로 들어가 crawl 단계가 채운다.
        new_spids = []
        for i in range(0, len(player_data), _SYNC_CHUNK):
            chunk = player_data[i:i + _SYNC_CHUNK]
            rows = conn.execute(
                _SPID_UPSERT_SQL,
                {
                    "spids": _pg_array_literal(p["id"] for p in chunk),
                    "names": _pg_array_literal(p["name"] for p in chunk),
                },
            ).all()
            new_spids.extend(r.spid for r in rows if r.inserted)

        conn.execute(
            text(f"""
                INSERT INTO {PLAYER_SCHEMA}.meta_crawl_log
                    (run_id, phase, new_players, new_seasons, new_spids)
                VALUES (:rid, 'sync', :np, :ns, :nsp)
            """),
            {
                "rid": run_id,
                "np": len(new_spids),
                "ns": json.dumps(new_seasons, ensure_ascii=False),
                "nsp": json.dumps(new_spids),
            },
        )

    logger.info("sync 완료: 신규 spid=%d, 신규 시즌=%d", len(new_spids), len(new_seasons))
    _write_github_output(new_spids=len(new_spids), new_seasons=len(new_seasons))


# =============================================================================
# CRAWL 모드 — 샤드 단위 선수 크롤 + 해시 diff
# =============================================================================
def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _shard(items: List[int]) -> Tuple[List[int], int, int]:
    index = _env_int("SHARD_INDEX", 0)
    count = _env_int("SHARD_COUNT", 1)
    if count <= 1:
        return items, 0, 1
    return items[index::count], index, count


def read_target_spids(conn, only_new: bool) -> List[int]:
    """크롤 대상 spid. only_new=True 면 crawled=FALSE(=sync 가 방금 넣은 신규 +
    과거 크롤 실패분)만 — 목요일 폴링의 경량 크롤 모드."""
    if only_new:
        sql = f"SELECT spid FROM {PLAYER_SCHEMA}.spid WHERE crawled = FALSE ORDER BY spid"
    else:
        sql = f"SELECT spid FROM {PLAYER_SCHEMA}.spid ORDER BY spid"
    return [r[0] for r in conn.execute(text(sql)).all()]


# 카드마다 SELECT 1회(왕복 ~150ms)를 없애기 위해, 샤드가 맡은 spid 들의
# (id, content_hash, main_ovr) 를 시작 시 한 번에 메모리로 프리로드한다.
# main_ovr 은 변경 카드의 OVR 변동량(delta = 새 OVR − 옛 OVR) 계산에 쓴다.
_EXISTING_SQL = text(f"""
    SELECT spid, id, content_hash, main_ovr FROM {PLAYER_SCHEMA}.players
    WHERE spid = ANY(CAST(:spids AS bigint[]))
""")


def _load_existing(conn, spids: List[int]) -> Dict[int, Tuple[int, Optional[str], Optional[int]]]:
    existing: Dict[int, Tuple[int, Optional[str], Optional[int]]] = {}
    for i in range(0, len(spids), _SYNC_CHUNK):
        chunk = spids[i:i + _SYNC_CHUNK]
        for r in conn.execute(_EXISTING_SQL, {"spids": _pg_array_literal(chunk)}).all():
            existing[r.spid] = (r.id, r.content_hash, r.main_ovr)
    return existing


# crawled=TRUE 마킹도 카드마다 UPDATE+commit(왕복 2회) 대신 버퍼에 모아 배치로 민다.
# 플러시 전에 잡이 죽으면 해당 spid 는 FALSE 로 남아 다음 실행이 재크롤하는데,
# 해시가 같으면 write 스킵 후 마킹만 하므로 멱등하다.
_MARK_CHUNK = 500
_MARK_CRAWLED_SQL = text(f"""
    UPDATE {PLAYER_SCHEMA}.spid SET crawled = TRUE, crawled_at = now()
    WHERE spid = ANY(CAST(:spids AS bigint[]))
""")


def _flush_marks(conn, buf: List[int]) -> None:
    if not buf:
        return
    conn.execute(_MARK_CRAWLED_SQL, {"spids": _pg_array_literal(buf)})
    conn.commit()
    buf.clear()


# players 의 배열 컬럼과 SQL 타입(백엔드 players/models.py 와 일치해야 함).
# 이 컬럼들은 _pg_array_literal() 문자열로 바인드하고 SQL 에서 CAST 한다 —
# 파이썬 리스트 바인드는 배열 길이별로 쿼리 텍스트가 갈라져 pg_stat_statements 를
# 비대하게 만든다(금지, _pg_array_literal docstring 참고).
_ARRAY_COLUMN_TYPES = {
    "sub_positions": "text[]",
    "trait_ids": "smallint[]",
    "team_color_ids": "smallint[]",
}


def _bind_expr(col: str) -> str:
    cast = _ARRAY_COLUMN_TYPES.get(col)
    return f"CAST(:{col} AS {cast})" if cast else f":{col}"


def _upsert_player(conn, spid: int, parsed: dict, season_id: int, nation_id: int,
                   trait_ids: List[int], team_color_ids: List[int], chash: str,
                   existing_id: Optional[int], ovr_change: Optional[int] = None) -> None:
    base = {
        "spid": spid,
        "name": parsed["name"],
        "season_id": season_id,
        "nation_id": nation_id,
        "main_position": parsed["main_position"]["position"],
        "sub_positions": _pg_array_literal(x["position"] for x in parsed["sub_positions"]),
        "main_ovr": parsed["main_position"]["ovr"],
        "pay": parsed["pay"],
        "height": parsed["height"],
        "weight": parsed["weight"],
        "body_type": parsed["body_type"],
        "skill_moves": parsed["skill_moves"],
        "weak_foot": parsed["weak_foot"],
        "preferred_foot": parsed["preferred_foot"],
        "trait_ids": _pg_array_literal(trait_ids),
        "team_color_ids": _pg_array_literal(team_color_ids),
        "content_hash": chash,
    }
    for col in STAT_COLUMNS:
        base[col] = parsed["stats"].get(col)

    if existing_id is None:
        cols = list(base.keys())
        col_sql = ", ".join(cols) + ", created_at, updated_at"
        bind_sql = ", ".join(_bind_expr(c) for c in cols) + ", now(), now()"
        conn.execute(
            text(f"INSERT INTO {PLAYER_SCHEMA}.players ({col_sql}) VALUES ({bind_sql})"),
            base,
        )
    else:
        set_sql = ", ".join(f"{c} = {_bind_expr(c)}" for c in base) + ", updated_at = now()"
        params = dict(base, _id=existing_id)
        # OVR 이 실제로 바뀐 카드에만 변동량+시각을 기록한다(delta==0 이면 두 컬럼 미변경 →
        # 해시만 바뀐 재기록에 거짓 인디케이터가 안 생김). 쿼리 텍스트는 with/without 두
        # 형태로만 갈라져(길이 가변 아님) pg_stat_statements 비대 우려 없음.
        if ovr_change is not None and ovr_change != 0:
            set_sql += ", ovr_change = :ovr_change, ovr_changed_at = now()"
            params["ovr_change"] = ovr_change
        conn.execute(
            text(f"UPDATE {PLAYER_SCHEMA}.players SET {set_sql} WHERE id = :_id"),
            params,
        )


def _fetch_and_parse(session, spid, max_retries, delay):
    """네트워크/파싱만(스레드에서). (spid, parsed|None, error|None).

    넥슨은 간헐적으로 200 응답에 불완전한 HTML(이름/포지션 누락)을 준다 → 파싱 실패도
    일시적일 수 있으므로 재시도한다. 진짜 구조 변화면 max_retries 회 후에도 실패로 보고된다.
    """
    last_err = "unknown"
    for attempt in range(max_retries):
        try:
            html = fetch_player_html(session, spid)
            parsed = parse_player_ability(html)
            if delay:
                time.sleep(delay)
            return spid, parsed, None
        except requests.RequestException as exc:
            last_err = f"net:{exc}"
        except ValueError as exc:        # 불완전 HTML(이름/포지션 없음) → 일시적일 수 있음
            last_err = f"parse:{exc}"
        if attempt < max_retries - 1:
            time.sleep(1.5 * (attempt + 1))
    return spid, None, last_err


def crawl_shard(engine, run_id: str) -> None:
    workers = _env_int("META_WORKERS", 8)
    delay = float(os.getenv("META_REQUEST_DELAY", "0.2") or 0)
    max_retries = _env_int("META_MAX_RETRIES", 3)
    spid_limit = _env_int("META_LIMIT", 0)
    only_new = _env_int("META_ONLY_NEW", 0) == 1

    session = make_session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=workers, pool_maxsize=workers, max_retries=0
    )
    session.mount("https://", adapter)

    engine_conn = engine.connect()
    with engine_conn:
        all_spids = read_target_spids(engine_conn, only_new)
        if spid_limit > 0:
            all_spids = all_spids[:spid_limit]
        spids, index, count = _shard(all_spids)
        existing_map = _load_existing(engine_conn, spids)
        engine_conn.commit()   # 프리로드 SELECT 가 연 트랜잭션/스냅샷을 닫아둔다
        logger.info("crawl 샤드 %d/%d → %d spids (only_new=%s, workers=%d, delay=%.2fs)",
                    index, count, len(spids), only_new, workers, delay)

        resolver = RefResolver(engine)
        # 이번 run 시작 전 trait id 스냅샷(신규 특성 정확 감지용).
        known_trait_ids = {
            r[0] for r in engine_conn.execute(
                text(f"SELECT id FROM {PLAYER_SCHEMA}.traits")
            ).all()
        }

        new_players = 0
        changed = 0
        failures = 0
        seen_new_traits: Dict[int, str] = {}
        done = 0
        started = time.monotonic()
        mark_buf: List[int] = []

        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = [
                ex.submit(_fetch_and_parse, session, spid, max_retries, delay)
                for spid in spids
            ]
            for fut in as_completed(futures):
                spid, parsed, err = fut.result()
                done += 1
                if err is not None:
                    failures += 1
                    logger.warning("실패 spid=%s: %s", spid, err)
                    continue
                try:
                    # 레퍼런스 해석(메인 스레드, advisory lock + 캐시)
                    nation_id = resolver.nation(
                        _clean_nation_name(parsed["nation"]["name"]),
                        parsed["nation"]["image_url"],
                    ) if parsed["nation"]["name"] else None
                    trait_ids = [resolver.trait(t["name"], t["image_url"]) for t in parsed["traits"]]
                    team_color_ids = [resolver.team_color(tc["name"], tc["type"]) for tc in parsed["team_colors"]]
                    season_id = int(str(spid)[:3])

                    chash = content_hash(parsed, season_id, nation_id, trait_ids, team_color_ids)
                    existing = existing_map.get(spid)

                    if existing is None:
                        _upsert_player(engine_conn, spid, parsed, season_id, nation_id,
                                       trait_ids, team_color_ids, chash, None)
                        new_players += 1
                        engine_conn.commit()   # 변경 카드만 개별 커밋(실패 격리)
                    elif existing[1] != chash:
                        # 라이브 퍼포먼스로 바뀐 카드 → 새 OVR − 옛 OVR = delta(선수 디테일
                        # ▲/▼ 인디케이터용). 옛 OVR 이 없으면(있을 리 없지만) 0 취급.
                        old_ovr = existing[2]
                        delta = (parsed["main_position"]["ovr"] - old_ovr) if old_ovr is not None else 0
                        _upsert_player(engine_conn, spid, parsed, season_id, nation_id,
                                       trait_ids, team_color_ids, chash, existing[0],
                                       ovr_change=delta)
                        changed += 1
                        engine_conn.commit()
                    # 같으면 write 스킵(커밋할 것도 없음)

                    # 신규 특성: 스냅샷에 없던 id 가 새로 잡힌 경우.
                    for tid, t in zip(trait_ids, parsed["traits"]):
                        if tid not in known_trait_ids:
                            seen_new_traits[tid] = t["name"]
                            known_trait_ids.add(tid)

                    mark_buf.append(spid)
                    if len(mark_buf) >= _MARK_CHUNK:
                        _flush_marks(engine_conn, mark_buf)
                except Exception as exc:
                    engine_conn.rollback()
                    failures += 1
                    logger.warning("저장 실패 spid=%s: %s", spid, exc)

                if done % 500 == 0:
                    elapsed = time.monotonic() - started
                    logger.info("진행 %d/%d 신규=%d 변경=%d 실패=%d %.1f req/s",
                                done, len(spids), new_players, changed, failures,
                                done / elapsed if elapsed else 0)

        _flush_marks(engine_conn, mark_buf)
        resolver.close()
        new_traits_payload = [{"id": tid, "name": nm} for tid, nm in seen_new_traits.items()]
        engine_conn.execute(
            text(f"""
                INSERT INTO {PLAYER_SCHEMA}.meta_crawl_log
                    (run_id, phase, shard, new_players, changed, failures, new_traits)
                VALUES (:rid, 'crawl', :shard, :np, :ch, :fa, :nt)
            """),
            {
                "rid": run_id, "shard": index, "np": new_players, "ch": changed,
                "fa": failures, "nt": json.dumps(new_traits_payload, ensure_ascii=False),
            },
        )
        engine_conn.commit()
        logger.info("crawl 샤드 %d 완료: 신규=%d 변경=%d 신규특성=%d 실패=%d",
                    index, new_players, changed, len(new_traits_payload), failures)


# =============================================================================
# MAIN
# =============================================================================
def run() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run_id = os.getenv("META_RUN_ID") or datetime.now(timezone.utc).strftime("%Y%m%d")
    mode = os.getenv("META_MODE", "crawl").strip().lower()

    engine = make_engine()
    if mode == "sync":
        sync_meta(engine, run_id)
    else:
        crawl_shard(engine, run_id)


if __name__ == "__main__":
    run()
