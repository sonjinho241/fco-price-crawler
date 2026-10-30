"""
FC Online 선수 "메타(능력치·특성·주발 등)" 주간 크롤러 — 공개 저장소용 자기완결형 스크립트.

GitHub Actions 에서 두 워크플로가 이 스크립트를 돌린다:
  · meta-thursday.yml — 목요일 패치 완료 시각이 매주 달라서, 목 10~23시 KST 매시
    sync 로 폴링하다가 신규 spid/시즌이 감지된 시각에만 신규 카드를 크롤(META_ONLY_NEW=1).
  · meta-friday.yml  — 라이브 퍼포먼스 반영(금 11:00 KST) 직후 전체 선수를 재크롤.
선수 카드 내용을 콘텐츠 해시로 떠서 **바뀐 카드만 DB write** 한다(안 바뀐 ~8.8만 장을
매번 덮어쓰지 않음 → Disk IO/bloat 절감, 시세 크롤러의 no-op skip 과 같은 취지).

크롤 도중 (1)처음 보는 spid = 신규 선수, (2)처음 보는 특성 = 신규 특성, (3)처음 보는
팀컬러 = 신규 팀컬러, (4)해시가 바뀐 기존 선수 = 라이브 퍼포먼스 변동 을 감지해
player.meta_crawl_log 에 적는다.
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
           같은 run 의 sync 가 phase='gone' 로그를 남겼으면 그 spid(넥슨이 지운 카드)는 뺀다.
           META_ONLY_NEW=1 이면 crawled=FALSE 인 spid(=sync 가 방금 넣은 신규 +
           과거 크롤 실패분)만 크롤한다(목요일 폴링용 경량 모드).

  seed-probe : DB 없이 데이터센터 PlayerList 시드 파서만 점검(META_SEED_SEASON=879).
           페이지 1~2 의 spid 수와 앞 몇 개를 찍는다. 넥슨 마크업이 바뀌었는지 확인용.

⚠️ 출시 당일의 반쪽 감지(2026-09-10 LKI2 에서 실제로 겪음):
   넥슨 static 메타는 seasonid.json 이 먼저, spid.json 이 하루쯤 늦게 갱신된다. 그래서
   목요일 sync 가 "신규 시즌"은 잡아도 그 시즌의 spid 가 0개라 crawl 이 할 일이 없고,
   앱에는 선수 0명인 클래스 카드만 뜬 채 금요일 풀크롤까지 기다리게 됐다.
   → sync 는 spid 가 하나도 없는 최근 시즌을 데이터센터 PlayerList(출시 즉시 노출)에서
     긁어 crawled=FALSE 로 시드한다(_seed_spids_from_datacenter). 이름은 spid.json 이
     갱신되면 기존 upsert 가 공식 값으로 덮어쓴다.

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
    META_SEED_SEASON        seed-probe 모드에서 점검할 시즌 ID (예 879)
    META_SEED_DISABLE       '1' 이면 sync 의 데이터센터 시드를 끈다 (기본 0)
    META_DETECT_GONE        '1' 이면 sync 가 spid.json 에서 빠진 spid 를 'gone' 로그로 남기고
                            players.hidden 으로 앱 검색에서 뺀다. 돌아온 카드는 다시 띄운다
                            (금요일 풀크롤 전용, 기본 0)
    META_SEED_LARGE_SEASONS 사람이 확인하고 시드를 허락한 큰 시즌 ID(콤마 구분, 예 325). 이 시즌은
                            _SEED_MAX_CARDS 상한 없이 끝까지 긁어 시드한다 (기본 없음)
    META_SEED_GROWING_SEASONS 새 시즌 번호 없이 카드가 추가되는 기존 시즌(콤마 구분). sync 마다
                            데이터센터와 대조해 DB 에 없는 spid 만 넣는다 (기본 100,101,110 — ICON 계열)
    META_RECHECK_STALE_HOURS crawl 대상을 이 시간 안에 성공한 적 없는 크롤 완료 spid 로 한정한다
                            — 풀크롤 실패분만 다시 묻는 수동 재확인 (기본 0=끔)

풀크롤은 넥슨이 오류 페이지로 돌리는 카드(CardMissing)를 실패로 세지 않고 phase='missing'
로그로 남겨 players.hidden 으로 숨긴다. 다시 열리면 다음 풀크롤이 숨김을 푼다.

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
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError

logger = logging.getLogger("meta_crawler")

PLAYER_SCHEMA = "player"

PLAYER_URL = "https://open.api.nexon.com/static/fconline/meta/spid.json"
SEASON_URL = "https://open.api.nexon.com/static/fconline/meta/seasonid.json"
ABILITY_URL = "https://fconline.nexon.com/datacenter/PlayerAbility"
DATACENTER_URL = "https://fconline.nexon.com/datacenter"
PLAYER_LIST_URL = "https://fconline.nexon.com/datacenter/PlayerList"


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


class CardMissing(Exception):
    """넥슨이 카드 상세를 오류 페이지로 돌린다 = 그 spid 의 카드가 넥슨에 없다.

    2026-10-30 넥슨이 지난 시즌 LIVE 카드 5,614장을 300 에서 325 로 옮기자, 300 쪽 상세가
    302 → bulletin.nexon.com/nxk/error.html 로 넘어갔다. spid.json 은 그 카드를 여전히 300 으로
    싣고 있어 'gone' 판정으로는 잡히지 않는다. 재시도해도 같으므로 바로 포기한다.
    """


_ERROR_PAGE_HOST = "bulletin.nexon.com"


def fetch_player_html(session: requests.Session, spid: int, timeout: int = 12) -> str:
    res = session.get(f"{ABILITY_URL}?spid={spid}&n1Strong=1", timeout=timeout)
    if res.history and _ERROR_PAGE_HOST in res.url:
        raise CardMissing(spid)
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
_SEQ_CATCHUP_ATTEMPTS = 200


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
            new_id = self._select_or_insert(table, name, insert_cols)
        finally:
            self.conn.execute(text("SELECT pg_advisory_unlock(hashtext(:k))"), {"k": key})
        cache[name] = new_id
        return new_id

    def _select_or_insert(self, table, name, insert_cols):
        """SELECT 후 없으면 INSERT. pkey 중복이면 시퀀스가 따라잡을 때까지 재시도한다.

        ⚠️ 레퍼런스 테이블 id 는 시퀀스 기본값인데, 외부 스크립트가 `max(id)+1` 로 직접
           채번하고 setval 을 안 하면 시퀀스가 그만큼 뒤처진다. 그 상태로 여기가 INSERT 하면
           nextval 이 이미 쓰인 id 를 돌려줘 pkey 중복으로 터지고, 레퍼런스 하나 때문에
           그 선수 카드 저장 전체가 실패한다(2026-08-21 team_colors 44장).
           실패한 INSERT 도 nextval 을 한 칸 진행시키므로 **그냥 재시도하면 따라잡는다.**
           setval 로 한 번에 맞추지 않는 이유는 시퀀스 UPDATE 권한이 필요한데 크롤러 롤은
           최소권한(USAGE)만 갖기 때문이다(sql/crawler_role.sql).
        """
        cols = ", ".join(insert_cols.keys())
        binds = ", ".join(f":{k}" for k in insert_cols)
        for attempt in range(_SEQ_CATCHUP_ATTEMPTS):
            row = self.conn.execute(
                text(f"SELECT id FROM {PLAYER_SCHEMA}.{table} WHERE name = :name"),
                {"name": name},
            ).first()
            if row:
                return row[0]
            try:
                return self.conn.execute(
                    text(f"INSERT INTO {PLAYER_SCHEMA}.{table} ({cols}) VALUES ({binds}) RETURNING id"),
                    insert_cols,
                ).scalar()
            except IntegrityError:
                if attempt == 0:
                    logger.warning(
                        "%s.id 시퀀스가 뒤처졌다(pkey 중복) — 재시도로 따라잡는 중. "
                        "수동 채번 스크립트가 setval 을 빠뜨렸는지 확인할 것. name=%s",
                        table, name,
                    )
        raise RuntimeError(
            f"{PLAYER_SCHEMA}.{table} id 시퀀스가 {_SEQ_CATCHUP_ATTEMPTS}회 재시도로도 "
            f"따라잡히지 않았다. setval 로 max(id) 에 맞춰야 한다(name={name})"
        )

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


# ── 데이터센터 PlayerList 시드(출시 당일 spid.json 지연 우회) ─────────────────
# 시즌 필터는 strSeason 이고 값이 ",879," 처럼 앞뒤 콤마다(DataCenter.SetCheckSearch 가
# 그렇게 만든다. "879" 만 보내면 0건). 페이지는 n4PageNo, 행 식별자는 area_playerunit_{spid}.
_SEED_RECENT_SEASONS = 10        # 시드 대상은 seasonid 상위 N개 중 spid 0개인 시즌만(옛 빈 시즌 제외)
_DATACENTER_LIST_CAP = 200       # PlayerList 한 번에 오는 최대 장수(페이지 넘김 없음)
_SALARY_MIN, _SALARY_MAX = 0, 99  # 상한에 닿으면 이 급여 구간을 반씩 나눠 다시 묻는다
_OVR_MIN, _OVR_MAX = 1, 199        # 급여 한 값에서도 닿으면 OVR 구간을 반씩 나눈다
_HEIGHT_MIN, _HEIGHT_MAX = 100, 250  # 급여·OVR 한 값에서도 닿으면 키 구간을 반씩 나눈다
_SEED_MAX_CARDS = 1000            # 이보다 큰 시즌은 시드하지 않는다(재발행된 LIVE 등, 사람이 판단)
                                  # 사람이 판단했으면 META_SEED_LARGE_SEASONS 로 그 시즌만 연다
_PLAYERUNIT_RE = re.compile(r"area_playerunit_(\d+)")

# 시드 후보: 최근 시즌 중 player.spid 에 한 건도 없는 시즌. 8.8만 행 정수 나눗셈 1회라 가볍다.
_SEASONS_WITHOUT_SPIDS_SQL = text(f"""
    SELECT s.season_id, s.class_name
    FROM (
        (SELECT season_id, class_name FROM {PLAYER_SCHEMA}.seasonid
         ORDER BY season_id DESC LIMIT :recent)
        UNION
        SELECT season_id, class_name FROM {PLAYER_SCHEMA}.seasonid
        WHERE season_id = ANY(CAST(:extra AS integer[]))
    ) s
    WHERE NOT EXISTS (
        SELECT 1 FROM {PLAYER_SCHEMA}.spid p
        WHERE p.spid / 1000000 = s.season_id
    )
    ORDER BY s.season_id DESC
""")


def fetch_player_list_page(session: requests.Session, season_id: int, page: int,
                           timeout: int = 20,
                           salary: Optional[Tuple[int, int]] = None,
                           ovr: Optional[Tuple[int, int]] = None,
                           height: Optional[Tuple[int, int]] = None) -> str:
    data = {"strSeason": f",{season_id},", "n4PageNo": page}
    if salary is not None:
        data["n4SalaryMin"], data["n4SalaryMax"] = salary
    if ovr is not None:
        data["n4OvrMin"], data["n4OvrMax"] = ovr
    if height is not None:
        data["n4HeightMin"], data["n4HeightMax"] = height
    res = session.post(
        PLAYER_LIST_URL,
        data=data,
        headers={"Referer": DATACENTER_URL, "X-Requested-With": "XMLHttpRequest"},
        timeout=timeout,
    )
    res.raise_for_status()
    return res.text


def parse_player_list(html: str, season_id: int) -> Dict[int, str]:
    """PlayerList 한 페이지 → {spid: name}. 이름을 못 찾으면 ''(spid.json 이 나중에 채움).
    다른 시즌 spid 가 섞여 오면(필터 무시·마크업 변경) 걸러 낸다."""
    result: Dict[int, str] = {}
    soup = BeautifulSoup(html, "html.parser")
    for el in soup.find_all(id=_PLAYERUNIT_RE):
        m = _PLAYERUNIT_RE.search(el.get("id", ""))
        if not m:
            continue
        spid = int(m.group(1))
        if spid // 1_000_000 != season_id:
            continue
        name_el = el.select_one(".name")
        name = name_el.get_text(" ", strip=True) if name_el else ""
        result[spid] = name
    if not result:
        # 마크업이 달라 id 매칭에 실패한 경우를 위한 정규식 폴백(이름 없이 spid 만).
        for m in _PLAYERUNIT_RE.finditer(html):
            spid = int(m.group(1))
            if spid // 1_000_000 == season_id:
                result.setdefault(spid, "")
    return result


class _TooManyCards(Exception):
    pass


def fetch_season_spids_from_datacenter(session: requests.Session, season_id: int,
                                       max_cards: Optional[int] = _SEED_MAX_CARDS) -> Dict[int, str]:
    """시즌 하나의 spid 전부를 긁는다. 요청이 하나라도 실패하면 빈 dict(반쪽 시드 방지 —
    spid 가 한 건이라도 들어가면 그 시즌은 다음 폴링의 시드 후보에서 빠진다).

    데이터센터는 검색 한 번에 최대 _DATACENTER_LIST_CAP 장만 주고 n4PageNo 는 무시한다
    (2026-10-29 실측: 26UP 205장이 1·2·3페이지 모두 같은 200장). 상한에 닿으면 급여
    구간을 반으로 나눠 다시 묻고, 급여 한 값에서도 닿으면(LIVE 시즌은 수천 장) OVR 구간을,
    OVR 한 값에서도 닿으면 키 구간을 나눈다(2026-10-30 25 LIVE 325: 급여 5·OVR 51 에 200장 →
    키 ≤180 117장 + 181~ 129장). max_cards=None 이면 상한 없이 끝까지 긁는다.
    """
    found: Dict[int, str] = {}

    def collect(salary: Tuple[int, int], ovr: Optional[Tuple[int, int]],
                height: Optional[Tuple[int, int]] = None) -> None:
        html = fetch_player_list_page(session, season_id, 1, salary=salary, ovr=ovr, height=height)
        items = parse_player_list(html, season_id)
        time.sleep(0.3)
        if len(items) >= _DATACENTER_LIST_CAP:
            if salary[0] < salary[1]:
                mid = (salary[0] + salary[1]) // 2
                collect((salary[0], mid), ovr)
                collect((mid + 1, salary[1]), ovr)
                return
            lo, hi = ovr or (_OVR_MIN, _OVR_MAX)
            if lo < hi:
                mid = (lo + hi) // 2
                collect(salary, (lo, mid))
                collect(salary, (mid + 1, hi))
                return
            h_lo, h_hi = height or (_HEIGHT_MIN, _HEIGHT_MAX)
            if h_lo < h_hi:
                mid = (h_lo + h_hi) // 2
                collect(salary, ovr, (h_lo, mid))
                collect(salary, ovr, (mid + 1, h_hi))
                return
            logger.warning("시드 시즌=%s 급여=%d OVR=%d 키=%d 한 값에 %d장 — 일부 누락 가능(금요일 풀크롤이 채움)",
                           season_id, salary[0], lo, h_lo, len(items))
        found.update(items)
        if max_cards is not None and len(found) > max_cards:
            raise _TooManyCards()

    try:
        collect((_SALARY_MIN, _SALARY_MAX), None)
    except _TooManyCards:
        pass   # 호출부가 상한 초과로 건너뛴다. 끝까지 긁지 않아 매시 폴링 비용이 작다
    except requests.RequestException as exc:
        logger.warning("시드 시즌=%s 실패, 다음 폴링에서 재시도: %s", season_id, exc)
        return {}
    return found


def discover_seasons_from_datacenter(session: requests.Session) -> List[dict]:
    """데이터센터 시즌 필터의 체크박스에서 시즌 목록을 읽는다.

    seasonid.json 도 spid.json 처럼 출시 당일 갱신이 늦을 수 있다(2026-10-29: 26UP·EVO-EP·
    EVO-CE 가 데이터센터엔 있는데 메타 파일은 10/7 그대로). 그런 시즌은 sync 가 여기서
    찾아 seasonid 에 넣고, 아래 시드가 spid 를 채운다. class_name 은 임시값이라 넥슨이
    seasonid.json 을 갱신하면 sync 의 upsert 가 공식 표기로 덮어쓴다.
    """
    html = session.get(DATACENTER_URL + "/", timeout=30).text
    soup = BeautifulSoup(html, "html.parser")
    seasons: Dict[int, dict] = {}
    for el in soup.select("input.season_check[data-no]"):
        try:
            sid = int(el["data-no"])
        except (KeyError, ValueError):
            continue
        label = soup.find("label", attrs={"for": el.get("id")}) if el.get("id") else None
        title = (label.get("title") or "").strip() if label else ""
        img_el = label.find("img") if label else None
        img = (img_el.get("src") or "").strip() if img_el else ""
        if not title or not img:
            continue
        abbr = img.rsplit("/", 1)[-1].rsplit(".", 1)[0].upper()
        seasons[sid] = {"seasonId": sid, "className": f"{abbr} ({title})", "seasonImg": img}
    return list(seasons.values())


def _seed_spids_from_datacenter(engine, session: requests.Session, run_id: str,
                                extra_season_ids: List[int] = ()) -> List[int]:
    """spid 가 0개인 최근 시즌을 데이터센터에서 긁어 crawled=FALSE 로 upsert. 신규 spid 목록 반환.

    DB 커넥션은 (1)후보 조회, (2)시즌별 upsert 에서만 짧게 잡는다. 페이지 수백 장을 받는
    HTTP 구간에는 커넥션을 들고 있지 않는다(idle in transaction 회피).
    결과는 phase='sync' 로그 행으로 따로 남긴다 — notify 가 sync 행들을 합산하므로
    시드분이 카톡 요약의 신규 spid/시즌에 그대로 반영된다.
    """
    with engine.begin() as conn:
        candidates = conn.execute(
            _SEASONS_WITHOUT_SPIDS_SQL,
            {"recent": _SEED_RECENT_SEASONS, "extra": _pg_array_literal(extra_season_ids)},
        ).all()

    large_ok = set(_env_int_list("META_SEED_LARGE_SEASONS"))
    all_inserted: List[int] = []
    seeded_seasons: List[dict] = []
    for season_id, class_name in candidates:
        items = fetch_season_spids_from_datacenter(   # 커넥션 없이
            session, season_id, None if season_id in large_ok else _SEED_MAX_CARDS)
        if not items:
            logger.info("시드 시즌=%s(%s): 데이터센터에도 아직 0건", season_id, class_name)
            continue
        if len(items) > _SEED_MAX_CARDS and season_id not in large_ok:
            # 2026-10-29 데이터센터에 25 LIVE 가 325 번으로 다시 올라왔다. 300 은 26 LIVE 가
            # 되고, 지난 시즌 LIVE 카드 일부(5,614장)는 300 에서 325 로 옮겨졌다 — 300 쪽 상세는
            # 넥슨 오류 페이지로 넘어간다. 출시 당일 신규 시즌은 수백 장이라, 이만큼 크면 사람이
            # 판단할 일이다(판단했으면 META_SEED_LARGE_SEASONS 로 연다).
            logger.warning("시드 시즌=%s(%s): 데이터센터 %d건 — 상한 %d 초과라 건너뜀",
                           season_id, class_name, len(items), _SEED_MAX_CARDS)
            continue
        spids = sorted(items)
        inserted: List[int] = []
        with engine.begin() as conn:
            for i in range(0, len(spids), _SYNC_CHUNK):
                chunk = spids[i:i + _SYNC_CHUNK]
                res = conn.execute(
                    _SPID_UPSERT_SQL,
                    {
                        "spids": _pg_array_literal(chunk),
                        "names": _pg_array_literal(items[sp] for sp in chunk),
                    },
                ).all()
                inserted.extend(r.spid for r in res if r.inserted)
        logger.info("시드 시즌=%s(%s): 데이터센터 %d건 → 신규 spid %d건",
                    season_id, class_name, len(spids), len(inserted))
        if inserted:
            all_inserted.extend(inserted)
            seeded_seasons.append({
                "season_id": season_id,
                "class_name": class_name,
                "seeded_from_datacenter": len(inserted),
            })

    if all_inserted:
        with engine.begin() as conn:
            conn.execute(
                text(f"""
                    INSERT INTO {PLAYER_SCHEMA}.meta_crawl_log
                        (run_id, phase, new_players, new_seasons, new_spids)
                    VALUES (:rid, 'sync', :np, :ns, :nsp)
                """),
                {
                    "rid": run_id,
                    "np": len(all_inserted),
                    "ns": json.dumps(seeded_seasons, ensure_ascii=False),
                    "nsp": json.dumps(all_inserted),
                },
            )
    return all_inserted


# ── 기존 시즌에 추가된 카드(ICON 계열) ────────────────────────────────────────
# 위 시드는 spid 가 0개인 시즌만 본다. 그런데 ICON·ICON The Moment(귀속 포함)는 새 시즌 번호
# 없이 기존 시즌에 선수를 더한다 — 2026-10-30 ICON TM 신규 8명이 데이터센터에는 100·110·101
# 에 8장씩 떴는데 spid.json 은 그대로라 앱에 24장이 안 보였다. 그래서 이 시즌들은 매 sync 마다
# 데이터센터 목록(시즌당 1~2요청)과 대조해 DB 에 없는 spid 만 넣는다.
_SEED_GROWING_SEASONS = (100, 101, 110)

# 이미 있는 spid 는 건드리지 않는다(이름을 데이터센터 표기로 덮으면 spid.json upsert 와 매번
# 번갈아 바뀐다).
_SPID_INSERT_NEW_SQL = text(f"""
    INSERT INTO {PLAYER_SCHEMA}.spid (spid, name, crawled)
    SELECT t.spid, t.name, FALSE
    FROM unnest(CAST(:spids AS bigint[]), CAST(:names AS text[])) AS t(spid, name)
    ON CONFLICT (spid) DO NOTHING
    RETURNING spid
""")


def growing_season_ids() -> List[int]:
    return _env_int_list("META_SEED_GROWING_SEASONS") or list(_SEED_GROWING_SEASONS)


def fetch_growing_season_spids(session: requests.Session, season_ids: List[int]) -> Dict[int, str]:
    """기존 시즌들의 데이터센터 카드 전부. 실패·상한 초과 시즌은 빼고 나머지만 돌려준다."""
    found: Dict[int, str] = {}
    for season_id in season_ids:
        items = fetch_season_spids_from_datacenter(session, season_id)
        if len(items) > _SEED_MAX_CARDS:
            logger.warning("추가 카드 대조 시즌=%s: 데이터센터 %d건 — 상한 초과라 건너뜀",
                           season_id, len(items))
            continue
        found.update(items)
    return found


def _seed_growing_spids(engine, run_id: str, items: Dict[int, str]) -> List[int]:
    """데이터센터에만 있는 spid 를 crawled=FALSE 로 넣는다. 신규 spid 목록 반환.
    sync 로그는 new_seasons 를 비워 남긴다(notify 가 이걸 신규 시즌으로 세지 않게)."""
    if not items:
        return []
    spids = sorted(items)
    with engine.begin() as conn:
        inserted = sorted(
            r.spid for r in conn.execute(
                _SPID_INSERT_NEW_SQL,
                {"spids": _pg_array_literal(spids),
                 "names": _pg_array_literal(items[sp] for sp in spids)},
            ).all()
        )
        if inserted:
            conn.execute(
                text(f"""
                    INSERT INTO {PLAYER_SCHEMA}.meta_crawl_log
                        (run_id, phase, new_players, new_seasons, new_spids)
                    VALUES (:rid, 'sync', :np, '[]', :nsp)
                """),
                {"rid": run_id, "np": len(inserted), "nsp": json.dumps(inserted)},
            )
    logger.info("기존 시즌 추가 카드: 데이터센터 %d건 → 신규 spid %d건 %s",
                len(spids), len(inserted), inserted[:20])
    return inserted


def seed_probe() -> None:
    """DB 없이 파서만 점검: META_SEED_SEASON 시즌의 1~2페이지를 받아 spid 수를 찍는다."""
    season_id = _env_int("META_SEED_SEASON", 0)
    if season_id <= 0:
        raise SystemExit("META_SEED_SEASON=<시즌ID> 를 지정해주세요 (예 879)")
    session = make_session()
    total: Dict[int, str] = {}
    for page in (1, 2):
        html = fetch_player_list_page(session, season_id, page)
        items = parse_player_list(html, season_id)
        logger.info("페이지 %d: %d건 (HTML %d바이트, area_playerunit 매치 %d)",
                    page, len(items), len(html), len(_PLAYERUNIT_RE.findall(html)))
        total.update(items)
    logger.info("합계 %d건, 예시 %s", len(total), list(total.items())[:5])
    if not total:
        logger.warning("0건 — strSeason 콤마 형식/마크업(area_playerunit_) 변경 여부를 확인해주세요")


# ── 넥슨이 지운 카드(spid.json 에서 빠진 spid) ─────────────────────────────────
# sync 는 player.spid 에 넣기만 하고 지우지 않는다. 그래서 넥슨이 내린 카드(2026-08 의
# ICON TM 야신 1장, HRT 70짜리 4장)가 금요일 풀크롤마다 상세 페이지 파싱에 실패해
# 매주 "크롤 실패 5건" 을 만들었다. sync 가 이런 spid 를 phase='gone' 로그로 남기면
# 같은 run 의 crawl 샤드들이 그 목록을 읽어 풀크롤 대상에서 뺀다(샤드마다 따로
# spid.json 을 받으면 받는 시각 차로 샤드 분할이 어긋날 수 있어 sync 한 곳에서 정한다).
#
# 판정 = crawled=TRUE 이고, spid.json 에 없고, **그 시즌은 spid.json 에 있는** spid.
#   · crawled=FALSE 는 방금 시드/sync 된 대기분이라 제외(목요일 크롤 몫).
#   · 시즌째 빠졌으면 spid.json 갱신 지연(9/10 LKI2 처럼 하루 늦는 경우)일 수 있어 제외.
# 행은 지우지 않고 players.hidden=TRUE 로 앱 검색에서만 뺀다(서버가 목록성 조회에서 거른다.
# 상세·저장된 스쿼드 복원은 그대로 보인다). 넥슨이 되살리면 다음 금요일 sync 가 hidden 을
# 풀고, 자동으로 다시 크롤 대상이 된다.
# ⚠️ 그래서 hidden 은 이 판정과 아래 'missing' 판정 둘이 소유한다 — spid.json 에 있는 카드를
#    손으로 숨겨도 금요일에 풀린다. 다른 이유로 숨길 일이 생기면 별도 컬럼을 둘 것.
#
# ── 넥슨이 오류 페이지로 돌리는 카드(spid.json 에는 남아 있는 spid) ─────────────────
# 풀크롤이 CardMissing 을 받은 spid 를 샤드마다 phase='missing' 로그로 남기고 숨긴다. 크롤
# 대상에서는 빼지 않는다 — 다음 풀크롤도 한 번씩(재시도 없이) 물어보고, 열리면 숨김을 푼다.
# sync 의 되살림은 가장 최근 풀크롤의 missing 목록을 빼고 판단한다(spid.json 에 있다고 풀면
# 같은 카드가 매주 sync~crawl 사이 1시간씩 검색에 다시 뜬다).
# 넥슨 점검처럼 한꺼번에 오류 페이지가 뜨면 숨기지 않고 실패로 센다(_MISSING_HIDE_MAX_RATIO).
_ALL_CRAWLED_SPIDS_SQL = text(f"""
    SELECT spid, crawled_at FROM {PLAYER_SCHEMA}.spid WHERE crawled = TRUE
""")

# 이 기간 안에 정상 크롤된 적이 있으면 "이번 주에 새로 사라진" 카드로 본다(알림용).
# 금요일 풀크롤 주기 7일 + cron 지연 여유 1일.
_GONE_FRESH_DAYS = 8

# 한 번에 이보다 많이 사라졌으면 넥슨이 카드를 내린 게 아니라 spid.json 이 잘려 온 것으로
# 본다. 그때는 숨기지 않는다 — 크롤 제외는 다음 주에 저절로 풀리지만, 대량 숨김은 앱 검색을
# 통째로 비운다. 지금까지 실제로 내려간 카드는 한 번에 5장이었다.
_GONE_HIDE_MAX = 100

_HIDE_SPIDS_SQL = text(f"""
    UPDATE {PLAYER_SCHEMA}.players SET hidden = TRUE
    WHERE spid = ANY(CAST(:spids AS bigint[])) AND hidden = FALSE
""")

_HIDDEN_SPIDS_SQL = text(f"""
    SELECT spid FROM {PLAYER_SCHEMA}.players WHERE hidden = TRUE
""")

_UNHIDE_SPIDS_SQL = text(f"""
    UPDATE {PLAYER_SCHEMA}.players SET hidden = FALSE
    WHERE spid = ANY(CAST(:spids AS bigint[])) AND hidden = TRUE
""")

MISSING = "missing"   # _fetch_and_parse 의 오류 표식(CardMissing)

# 한 샤드에서 이보다 큰 비율이 오류 페이지면 카드가 옮겨진 게 아니라 넥슨 점검으로 본다.
# 2026-10-30 실제 이동은 샤드당 약 6%(1,400/22,345)였다.
_MISSING_HIDE_MAX_RATIO = 0.15

# 가장 최근에 missing 을 남긴 풀크롤 run 의 목록(샤드 행 전부). 다음 풀크롤 전까지 유효하다.
_LAST_MISSING_SQL = text(f"""
    SELECT new_spids FROM {PLAYER_SCHEMA}.meta_crawl_log
    WHERE phase = 'missing' AND run_id = (
        SELECT run_id FROM {PLAYER_SCHEMA}.meta_crawl_log
        WHERE phase = 'missing' ORDER BY created_at DESC LIMIT 1
    )
""")


def read_last_missing_spids(conn) -> set:
    missing: set = set()
    for (detail,) in conn.execute(_LAST_MISSING_SQL):
        missing.update((detail or {}).get("missing") or [])
    return missing


def _log_gone_spids(engine, run_id: str, player_data: list, also_listed=()) -> None:
    # also_listed = 데이터센터에서 확인한 기존 시즌 추가 카드. spid.json 이 늦어도 시즌은
    # spid.json 에 있으므로, 이걸 넣지 않으면 크롤된 다음 금요일에 사라진 카드로 숨겨진다.
    listed = {int(p["id"]) for p in player_data} | set(also_listed)
    listed_seasons = {spid // 1_000_000 for spid in listed}
    fresh_since = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=_GONE_FRESH_DAYS)

    gone: List[int] = []
    newly_gone: List[int] = []
    with engine.connect() as conn:
        for r in conn.execute(_ALL_CRAWLED_SPIDS_SQL):
            if r.spid in listed or r.spid // 1_000_000 not in listed_seasons:
                continue
            gone.append(r.spid)
            # crawled_at 은 naive UTC(now() 를 timestamp without tz 에 적재).
            if r.crawled_at is not None and r.crawled_at >= fresh_since:
                newly_gone.append(r.spid)
        gone.sort()
        newly_gone.sort()

        # 앱 검색에서 빼고, spid.json 에 돌아온 카드는 다시 띄운다. 숨긴 카드는 수천 장 이하라
        # 되살아남 판정은 파이썬에서 한다(8.8만 spid 를 배열로 넘기지 않는다).
        # 오류 페이지로 숨긴 카드는 spid.json 에 계속 있으므로 여기서 풀지 않는다(풀크롤 몫).
        still_missing = read_last_missing_spids(conn)
        hidden_now = 0
        if len(gone) > _GONE_HIDE_MAX:
            logger.warning(
                "사라진 spid 가 %d개로 상한(%d)을 넘어 숨기지 않습니다 — spid.json 이 잘려 왔는지 확인",
                len(gone), _GONE_HIDE_MAX,
            )
        elif gone:
            hidden_now = conn.execute(
                _HIDE_SPIDS_SQL, {"spids": _pg_array_literal(gone)}
            ).rowcount
        revived = sorted(
            r.spid for r in conn.execute(_HIDDEN_SPIDS_SQL)
            if r.spid in listed and r.spid not in still_missing
        )
        if revived:
            conn.execute(_UNHIDE_SPIDS_SQL, {"spids": _pg_array_literal(revived)})

        conn.execute(
            text(f"""
                INSERT INTO {PLAYER_SCHEMA}.meta_crawl_log
                    (run_id, phase, new_players, new_spids)
                VALUES (:rid, 'gone', :n, CAST(:detail AS jsonb))
            """),
            {
                "rid": run_id,
                "n": len(newly_gone),
                "detail": json.dumps(
                    {"gone": gone, "newly_gone": newly_gone, "revived": revived}
                ),
            },
        )
        conn.commit()
    logger.info("넥슨에서 사라진 spid=%d (이번 주 신규 %d): %s", len(gone), len(newly_gone), gone[:20])
    logger.info("앱 검색에서 새로 숨김 %d장, 되살아나 다시 노출 %d장: %s", hidden_now, len(revived), revived[:20])


_GONE_SPIDS_SQL = text(f"""
    SELECT new_spids FROM {PLAYER_SCHEMA}.meta_crawl_log
    WHERE run_id = :rid AND phase = 'gone'
    ORDER BY created_at DESC LIMIT 1
""")


def read_gone_spids(conn, run_id: str) -> set:
    """같은 run 의 sync 가 남긴 사라진 spid. 로그가 없으면(목요일·수동 실행) 빈 집합."""
    detail = conn.execute(_GONE_SPIDS_SQL, {"rid": run_id}).scalar()
    return set((detail or {}).get("gone") or [])


def _insert_datacenter_seasons(engine, session: requests.Session, season_data: list) -> dict:
    """seasonid.json 에 아직 없는 시즌을 데이터센터에서 찾아 seasonid 에 넣는다(있으면 그대로).

    반환: inserted = 이번에 새로 넣은 시즌, missing_ids = seasonid.json 에 없는 시즌 전부
    (시드가 상위 N개 밖의 번호 — 예 519 26PL — 도 후보로 보게).
    """
    known = {s["seasonId"] for s in season_data}
    try:
        found = [s for s in discover_seasons_from_datacenter(session) if s["seasonId"] not in known]
    except requests.RequestException as exc:
        logger.warning("데이터센터 시즌 목록 실패, 건너뜀: %s", exc)
        return {"inserted": [], "missing_ids": []}
    inserted = []
    with engine.begin() as conn:
        for s in found:
            row = conn.execute(
                text(f"""
                    INSERT INTO {PLAYER_SCHEMA}.seasonid (season_id, class_name, season_img)
                    VALUES (:sid, :cn, :img)
                    ON CONFLICT (season_id) DO NOTHING
                    RETURNING season_id
                """),
                {"sid": s["seasonId"], "cn": s["className"], "img": s["seasonImg"]},
            ).first()
            if row:
                inserted.append({"season_id": s["seasonId"], "class_name": s["className"]})
    if found:
        logger.info("seasonid.json 에 없는 데이터센터 시즌 %s (이번에 추가 %d)",
                    [s["seasonId"] for s in found], len(inserted))
    return {"inserted": inserted, "missing_ids": [s["seasonId"] for s in found]}


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

    seed_enabled = _env_int("META_SEED_DISABLE", 0) != 1 and spid_limit == 0
    # 기존 시즌 추가 카드는 HTTP 만 먼저(커넥션 없이) — 사라진 카드 판정에도 쓴다.
    growing = fetch_growing_season_spids(session, growing_season_ids()) if seed_enabled else {}

    # 넥슨이 지운 카드 감지(금요일 풀크롤 전용). 시드보다 먼저 — 시드분은 아직 spid.json 에
    # 없어서 섞이면 사라진 카드로 오판한다.
    if _env_int("META_DETECT_GONE", 0) == 1 and spid_limit == 0:
        _log_gone_spids(engine, run_id, player_data, growing)

    # 출시 당일 폴백: 시즌은 들어왔는데 spid 가 하나도 없는 최근 시즌 → 데이터센터 시드.
    # (spid.json 이 seasonid.json 보다 늦어 목요일 crawl 이 빈손이 되는 것을 막는다.)
    # ⚠️ 반드시 위 트랜잭션 **밖**에서. 시드는 수백 페이지 HTTP 라 트랜잭션 안에서 돌면
    #    커넥션을 분 단위로 붙들어(idle in transaction) SSL 끊김·풀 고갈을 부른다.
    #    시드된 spid 는 new_spids 에 합산돼 워크플로 게이트가 같은 시각에 crawl 을 연다.
    if seed_enabled:
        dc_seasons = _insert_datacenter_seasons(engine, session, season_data)
        new_seasons.extend(dc_seasons["inserted"])
        new_spids.extend(_seed_spids_from_datacenter(engine, session, run_id,
                                                     dc_seasons["missing_ids"]))
        new_spids.extend(_seed_growing_spids(engine, run_id, growing))

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


def _env_int_list(name: str) -> List[int]:
    """'325, 326' → [325, 326]. 숫자가 아닌 조각은 버린다."""
    raw = os.getenv(name) or ""
    return [int(x) for x in raw.replace(" ", "").split(",") if x.isdigit()]


def _shard(items: List[int]) -> Tuple[List[int], int, int]:
    index = _env_int("SHARD_INDEX", 0)
    count = _env_int("SHARD_COUNT", 1)
    if count <= 1:
        return items, 0, 1
    return items[index::count], index, count


def read_target_spids(conn, only_new: bool, stale_hours: int = 0) -> List[int]:
    """크롤 대상 spid. only_new=True 면 crawled=FALSE(=sync 가 방금 넣은 신규 +
    과거 크롤 실패분)만 — 목요일 폴링의 경량 크롤 모드.

    stale_hours>0 이면 그 시간 안에 한 번도 성공하지 못한 크롤 완료 spid 만 — 풀크롤의
    실패분(오류 페이지 포함)만 다시 묻는 수동 재확인. 성공해야 crawled_at 이 갱신된다."""
    if only_new:
        sql = f"SELECT spid FROM {PLAYER_SCHEMA}.spid WHERE crawled = FALSE ORDER BY spid"
    elif stale_hours > 0:
        sql = (f"SELECT spid FROM {PLAYER_SCHEMA}.spid WHERE crawled = TRUE "
               f"AND crawled_at < now() - make_interval(hours => :h) ORDER BY spid")
    else:
        sql = f"SELECT spid FROM {PLAYER_SCHEMA}.spid ORDER BY spid"
    return [r[0] for r in conn.execute(text(sql), {"h": stale_hours}).all()]


def split_missing(missing: List[int], population: int, site_up: bool) -> Tuple[List[int], int]:
    """(숨길 spid, 실패로 셀 수). 넥슨 점검으로 보면 하나도 숨기지 않고 전부 실패로 센다:
      · 정상 카드가 열리지 않았거나(site_up=False — 샤드 성공 0장이면 대조군으로 확인),
      · 오류 페이지가 샤드 몫의 전체 카드(population) 대비 상한 비율을 넘을 때.
    population 은 풀크롤이면 샤드 대상 수, 실패분 재확인이면 전체 카드 ÷ 샤드 수다 —
    재확인 대상은 원래 실패한 카드라 전부 오류 페이지일 수도 있다(2026-10-30 1360/1360)."""
    if missing and (not site_up or len(missing) / max(population, 1) > _MISSING_HIDE_MAX_RATIO):
        return [], len(missing)
    return sorted(missing), 0


# 샤드에서 한 장도 안 열렸을 때 넥슨이 살아 있는지 볼 대조군: 최근에 정상 크롤된 카드.
_CONTROL_SPIDS_SQL = text(f"""
    SELECT s.spid FROM {PLAYER_SCHEMA}.spid s
    JOIN {PLAYER_SCHEMA}.players p ON p.spid = s.spid AND NOT p.hidden
    WHERE s.crawled = TRUE
    ORDER BY s.crawled_at DESC NULLS LAST LIMIT 3
""")


def site_is_up(session, control_spids: List[int], max_retries: int) -> bool:
    """대조군 카드가 하나라도 열리면 넥슨은 정상이다."""
    for spid in control_spids:
        if _fetch_and_parse(session, spid, max_retries, 0)[1] is not None:
            return True
    return False


def _record_missing(conn, run_id: str, shard: int, missing: List[int],
                    hidden_before: set, revived: List[int]) -> int:
    """오류 페이지 카드를 숨기고, 다시 열린 카드는 풀고, phase='missing' 로그를 남긴다.
    newly_missing(이번에 처음 숨긴 카드)만 알림에 쓴다 — 같은 카드를 매주 알리지 않게."""
    newly = [s for s in missing if s not in hidden_before]
    if missing:
        conn.execute(_HIDE_SPIDS_SQL, {"spids": _pg_array_literal(missing)})
    if revived:
        conn.execute(_UNHIDE_SPIDS_SQL, {"spids": _pg_array_literal(revived)})
    conn.execute(
        text(f"""
            INSERT INTO {PLAYER_SCHEMA}.meta_crawl_log
                (run_id, phase, shard, new_players, new_spids)
            VALUES (:rid, 'missing', :shard, :n, CAST(:detail AS jsonb))
        """),
        {
            "rid": run_id, "shard": shard, "n": len(newly),
            "detail": json.dumps({"missing": missing, "newly_missing": newly, "revived": revived}),
        },
    )
    conn.commit()
    return len(newly)


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
        except CardMissing:
            if delay:
                time.sleep(delay)
            return spid, None, MISSING
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
    stale_hours = _env_int("META_RECHECK_STALE_HOURS", 0)
    # 오류 페이지 숨김/되살림은 풀크롤(또는 그 실패분 재확인)만 판단한다. 목요일 신규 크롤과
    # META_LIMIT 스모크는 일부만 보므로 missing 목록을 남기면 sync 의 되살림 판단이 틀어진다.
    track_missing = not only_new and spid_limit == 0

    session = make_session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=workers, pool_maxsize=workers, max_retries=0
    )
    session.mount("https://", adapter)

    engine_conn = engine.connect()
    with engine_conn:
        all_spids = read_target_spids(engine_conn, only_new, 0 if only_new else stale_hours)
        gone = set() if only_new else read_gone_spids(engine_conn, run_id)
        if gone:
            # 샤드 분할 전에 빼야 모든 샤드가 같은 목록을 같은 방식으로 나눈다.
            all_spids = [s for s in all_spids if s not in gone]
            logger.info("넥슨에서 사라진 spid %d개 제외", len(gone))
        if spid_limit > 0:
            all_spids = all_spids[:spid_limit]
        spids, index, count = _shard(all_spids)
        population = len(spids)
        if stale_hours > 0:
            total = engine_conn.execute(text(f"SELECT count(*) FROM {PLAYER_SCHEMA}.spid")).scalar()
            population = max(population, (total or 0) // count)
        existing_map = _load_existing(engine_conn, spids)
        shard_set = set(spids)
        hidden_before = {
            r.spid for r in engine_conn.execute(_HIDDEN_SPIDS_SQL) if r.spid in shard_set
        }
        engine_conn.commit()   # 프리로드 SELECT 가 연 트랜잭션/스냅샷을 닫아둔다
        logger.info("crawl 샤드 %d/%d → %d spids (only_new=%s, stale_hours=%d, workers=%d, delay=%.2fs)",
                    index, count, len(spids), only_new, stale_hours, workers, delay)

        resolver = RefResolver(engine)
        # 이번 run 시작 전 trait id 스냅샷(신규 특성 정확 감지용).
        known_trait_ids = {
            r[0] for r in engine_conn.execute(
                text(f"SELECT id FROM {PLAYER_SCHEMA}.traits")
            ).all()
        }
        # 팀컬러도 같은 방식으로 스냅샷을 떠서 신규를 감지한다(넥슨이 관계 팀컬러를
        # 대량으로 추가하는 일이 있는데, 예전엔 알림에 아무 흔적도 남지 않았다).
        known_team_color_ids = {
            r[0] for r in engine_conn.execute(
                text(f"SELECT id FROM {PLAYER_SCHEMA}.team_colors")
            ).all()
        }

        new_players = 0
        changed = 0
        failures = 0
        seen_new_traits: Dict[int, str] = {}
        seen_new_team_colors: Dict[int, str] = {}
        done = 0
        started = time.monotonic()
        mark_buf: List[int] = []
        missing: List[int] = []
        revived: List[int] = []
        succeeded = 0

        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = [
                ex.submit(_fetch_and_parse, session, spid, max_retries, delay)
                for spid in spids
            ]
            for fut in as_completed(futures):
                spid, parsed, err = fut.result()
                done += 1
                if err == MISSING and track_missing:
                    missing.append(spid)   # 샤드 끝에서 한 번에 숨긴다(줄마다 경고를 찍지 않는다)
                    continue
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

                    # 신규 팀컬러도 같은 방식으로.
                    for cid, tc in zip(team_color_ids, parsed["team_colors"]):
                        if cid not in known_team_color_ids:
                            seen_new_team_colors[cid] = tc["name"]
                            known_team_color_ids.add(cid)

                    succeeded += 1
                    if spid in hidden_before:
                        revived.append(spid)   # 오류 페이지였다가 다시 열린 카드
                    mark_buf.append(spid)
                    if len(mark_buf) >= _MARK_CHUNK:
                        _flush_marks(engine_conn, mark_buf)
                except Exception as exc:
                    engine_conn.rollback()
                    failures += 1
                    logger.warning("저장 실패 spid=%s: %s", spid, exc)

                if done % 500 == 0:
                    elapsed = time.monotonic() - started
                    logger.info("진행 %d/%d 신규=%d 변경=%d 실패=%d 오류페이지=%d %.1f req/s",
                                done, len(spids), new_players, changed, failures, len(missing),
                                done / elapsed if elapsed else 0)

        _flush_marks(engine_conn, mark_buf)
        resolver.close()
        newly_missing = 0
        if track_missing:
            site_up = succeeded > 0
            if missing and not site_up:
                controls = [r[0] for r in engine_conn.execute(_CONTROL_SPIDS_SQL).all()]
                engine_conn.commit()
                site_up = site_is_up(session, controls, max_retries)
                logger.info("성공 0장 — 대조군 %s 확인: 넥슨 %s", controls, "정상" if site_up else "응답 없음")
            to_hide, outage = split_missing(missing, population, site_up)
            if outage:
                logger.warning("오류 페이지 %d장(샤드 몫 %d장, 성공 %d장) — 점검으로 보고 숨기지 않습니다",
                               outage, population, succeeded)
                failures += outage
            newly_missing = _record_missing(engine_conn, run_id, index, to_hide,
                                            hidden_before, revived)
            logger.info("오류 페이지 카드 %d장 숨김(이번에 새로 %d), 다시 열려 노출 %d장: %s",
                        len(to_hide), newly_missing, len(revived), to_hide[:20])
        new_traits_payload = [{"id": tid, "name": nm} for tid, nm in seen_new_traits.items()]
        new_team_colors_payload = [
            {"id": cid, "name": nm} for cid, nm in seen_new_team_colors.items()
        ]
        engine_conn.execute(
            text(f"""
                INSERT INTO {PLAYER_SCHEMA}.meta_crawl_log
                    (run_id, phase, shard, new_players, changed, failures,
                     new_traits, new_team_colors)
                VALUES (:rid, 'crawl', :shard, :np, :ch, :fa,
                        CAST(:nt AS jsonb), CAST(:ntc AS jsonb))
            """),
            {
                "rid": run_id, "shard": index, "np": new_players, "ch": changed,
                "fa": failures, "nt": json.dumps(new_traits_payload, ensure_ascii=False),
                "ntc": json.dumps(new_team_colors_payload, ensure_ascii=False),
            },
        )
        engine_conn.commit()
        logger.info("crawl 샤드 %d 완료: 신규=%d 변경=%d 신규특성=%d 신규팀컬러=%d 실패=%d 새오류페이지=%d",
                    index, new_players, changed, len(new_traits_payload),
                    len(new_team_colors_payload), failures, newly_missing)


# =============================================================================
# MAIN
# =============================================================================
def run() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run_id = os.getenv("META_RUN_ID") or datetime.now(timezone.utc).strftime("%Y%m%d")
    mode = os.getenv("META_MODE", "crawl").strip().lower()

    if mode == "seed-probe":
        seed_probe()
        return

    engine = make_engine()
    if mode == "sync":
        sync_meta(engine, run_id)
    else:
        crawl_shard(engine, run_id)


if __name__ == "__main__":
    run()
