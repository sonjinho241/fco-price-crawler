"""
FC Online 랭커 스쿼드 크롤러 — "팀컬러별 포지션별 인기 선수" 교차표용 자기완결형 스크립트.

공식경기 1 ON 1 랭킹 상위 10,000명의 **대표팀 스쿼드 선발 11명**을 모아
(팀컬러 × 포지션 × 선수) 교차표의 원재료를 만든다. 넥슨은 팀컬러 순위와
포지션별 선수 순위를 각각 별개 표로만 주고 이 교차표는 주지 않는다.

이 파일 하나로 끝난다(메인 비공개 앱 저장소에 의존하지 않는다).

⚠️ 비밀정보(DATABASE_URL)는 절대 이 파일/저장소에 두지 않는다.
   GitHub repo Settings → Secrets and variables → Actions → DATABASE_URL 로만 주입한다.


넥슨 엔드포인트 (2026-09-17 실측, 모두 로그인 불필요)
--------------------------------------------------------------------------
1) 랭커 목록  GET /datacenter/rank_inner?rt=1vs1&n4seasonno=0&n4pageno={1..500}
   20행/페이지, 500페이지에서 정확히 10,000명(501은 500을 그대로 되돌려준다).
   행마다 순위 · 구단주명 · data-sn(넥슨SN) · **팀컬러 이름+인원수** · 포메이션.
   여기의 팀컬러가 "그 랭커가 랭크 경기에서 실제로 쓴 덱"이다.

2) 캐릭터 ID  GET /profile/squad/popup/{nexon_sn}
   HTML 안의 SetSquadInfo("<teamType>", "<squad>", "<sn>", "<characterId>") 에서
   characterId(24 hex)와 그 유저의 "대표 스쿼드" 좌표를 얻는다.

3) 스쿼드     GET /datacenter/SquadGetUserInfo
              ?strTeamType={1=대표팀,0=클럽팀}&n1Type={1,2,3 = A,B,C 스쿼드}
              &n8NexonSN={sn}&strCharacterID={cid}
   → JSON. **X-Requested-With: XMLHttpRequest 헤더가 없으면 302로 튕긴다.**
   쿠키는 필요 없다(cid 만 맞으면 된다).

   players[] 18명 중 state==0 이 선발 11명, state==1 이 후보 7명.
   쓰는 필드: spid(9자리) · buildUp(=강화단계) · role(포메이션 슬롯) · name.
   totalTeamColor.affiliation 에 **넥슨이 직접 판정한 소속 팀컬러**가 들어있다
   (우리가 요구인원/카테고리 규칙을 역산할 필요가 없다).


어느 스쿼드를 쓰는가 (중요)
--------------------------------------------------------------------------
유저는 대표팀 A/B/C 3개를 저장해두고, 랭크에서 그중 무엇을 썼는지는 프로필에
표시되지 않는다. 게다가 경기 후 스쿼드를 바꾼 유저는 A/B/C 어디에도 그 덱이 없다
(2026-09-17 표본 13명 중 3명이 불일치, 그중 1명은 A/B/C 전부 불일치).

그래서 **랭킹 표의 팀컬러 이름과 일치하는 스쿼드만 채택**한다:
  대표 스쿼드 먼저 확인 → 불일치하면 대표팀 A/B/C 를 훑어 일치하는 것 채택
  → 끝내 없으면 그 랭커는 버린다(unresolved 로 집계만 남긴다).
이렇게 해야 "랭크에서 실제로 쓴 덱"이라는 말이 정확해진다.


환경변수
    DATABASE_URL                (선택) 없으면 DB 에 쓰지 않고 --out 으로만 덤프한다.
    RANKER_SQUAD_LIMIT          처리할 랭커 수 상한 (기본 0=전체 10,000)
    RANKER_SQUAD_WORKERS        동시 요청 스레드 수 (기본 6)
    RANKER_SQUAD_REQUEST_DELAY  각 요청 후 sleep 초 (기본 0.15)
    RANKER_SQUAD_MAX_RETRIES    넥슨 호출 재시도 횟수 (기본 3)
    RANKER_SQUAD_DB_BATCH       DB 에 한 번에 쓰는 행 수 (기본 2000)

로컬 테스트(DB 없이):
    RANKER_SQUAD_LIMIT=40 python ranker_squad_job.py --out sample.json
"""

import argparse
import html
import json
import logging
import os
import random
import re
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from typing import Dict, Iterable, List, Optional, Tuple

import requests
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

logger = logging.getLogger("ranker_squad")

PLAYER_SCHEMA = "player"

BASE = "https://fconline.nexon.com"
RANK_INNER_URL = f"{BASE}/datacenter/rank_inner"
SQUAD_POPUP_URL = f"{BASE}/profile/squad/popup"
SQUAD_INFO_URL = f"{BASE}/datacenter/SquadGetUserInfo"

# 랭킹은 최대 10,000명(=500페이지)만 제공된다. 501페이지는 500과 같은 내용이 온다.
RANK_PAGE_SIZE = 20
RANK_MAX_PAGE = 500

# 대표팀. 클럽팀(0)은 이 통계의 대상이 아니다.
TEAM_TYPE_NATIONAL = 1
# 대표팀 A / B / C 스쿼드.
SQUAD_SLOTS = (1, 2, 3)

STATE_STARTER = 0


# =============================================================================
# 넥슨 파싱
# =============================================================================
# rank_inner 한 행에서 넥슨SN·구단주명·팀컬러 블록까지. 팀컬러가 비어있는 행
# (팀컬러 없이 등록된 스쿼드)도 있으므로 팀컬러는 뒤에서 따로 느슨하게 뽑는다.
_RANK_ROW_RE = re.compile(
    r'<span class="td rank_no">([\d,]+)</span>.*?'
    r'data-sn="(\d+)">([^<]*)</span>.*?'
    r'<span class="td team_color">(.*?)<span class="td formation">',
    re.S,
)
# 이름과 인원수를 한 패턴으로 묶으면 (.*?) 가 <small> 까지 먹는다(인원수가 없는 행도
# 있어서 선택 그룹이라 non-greedy 가 멈추지 않는다). 둘을 따로 뽑는다.
_RANK_TC_NAME_RE = re.compile(r'class="inner">\s*([^<]*)')
_RANK_TC_COUNT_RE = re.compile(r"<small>\((\d+)명\)")
_SET_SQUAD_INFO_RE = re.compile(
    r'SetSquadInfo\(\s*"(\d+)"\s*,\s*"(\d+)"\s*,\s*"(\d+)"\s*,\s*"([0-9a-f]+)"'
)


class RankRow:
    """랭킹 표 한 줄. team_color_name 이 "랭크에서 실제로 쓴 덱"의 정답지다."""

    __slots__ = ("rank", "nexon_sn", "nickname", "team_color_name", "team_color_count")

    def __init__(self, rank, nexon_sn, nickname, team_color_name, team_color_count):
        self.rank = rank
        self.nexon_sn = nexon_sn
        self.nickname = nickname
        self.team_color_name = team_color_name
        self.team_color_count = team_color_count

    def __repr__(self):
        return (
            f"RankRow(rank={self.rank}, sn={self.nexon_sn}, "
            f"tc={self.team_color_name!r}x{self.team_color_count})"
        )


def parse_rank_page(markup: str) -> List[RankRow]:
    rows = []
    for m in _RANK_ROW_RE.finditer(markup):
        rank_text, sn, nickname, tc_block = m.groups()
        name, count = None, None
        tc = _RANK_TC_NAME_RE.search(tc_block)
        if tc:
            name = html.unescape(tc.group(1)).strip() or None
        tc_count = _RANK_TC_COUNT_RE.search(tc_block)
        if tc_count:
            count = int(tc_count.group(1))
        rows.append(
            RankRow(
                rank=int(rank_text.replace(",", "")),
                nexon_sn=int(sn),
                nickname=html.unescape(nickname).strip(),
                team_color_name=name,
                team_color_count=count,
            )
        )
    return rows


def parse_character_id(markup: str) -> Optional[Tuple[int, int, str]]:
    """popup HTML → (기본 teamType, 기본 squad, characterId). 없으면 None."""
    m = _SET_SQUAD_INFO_RE.search(markup)
    if not m:
        return None
    team_type, squad, _sn, cid = m.groups()
    return int(team_type), int(squad), cid


# 포메이션 슬롯(role) → 집계용 포지션 그룹 8종.
# 포메이션마다 슬롯 이름이 갈라져서(ls/st/rs 가 다 같은 최전방) 슬롯 단위로 세면
# 같은 자리가 여러 줄로 쪼개진다. 실제로 경쟁하는 자리끼리 묶는다.
#
#   FW   톱      ST LS RS CF RF LF
#   CAM  공격형MF CAM
#   WING 윙      LM RM LW RW LAM RAM   (LAM/RAM 은 중앙이 아니라 측면으로 본다)
#   CM   중앙MF  LCM CM RCM
#   CDM  수비형MF LDM CDM RDM
#   CB   센터백  LCB CB RCB SW
#   FB   풀백    LWB LB RB RWB
#   GK   골키퍼  GK
POSITION_GROUPS = ("FW", "CAM", "WING", "CM", "CDM", "CB", "FB", "GK")

POSITION_GROUP_LABELS = {
    "FW": "톱", "CAM": "공미", "WING": "윙", "CM": "중미",
    "CDM": "수미", "CB": "센백", "FB": "풀백", "GK": "골키퍼",
}

_ROLE_TO_GROUP = {}
for _group, _roles in {
    "FW": ("st", "ls", "rs", "cf", "rf", "lf"),
    "CAM": ("cam",),
    "WING": ("lm", "rm", "lw", "rw", "lam", "ram"),
    "CM": ("rcm", "lcm", "cm"),
    "CDM": ("ldm", "cdm", "rdm"),
    "CB": ("lcb", "cb", "rcb", "sw"),
    "FB": ("lwb", "lb", "rb", "rwb"),
    "GK": ("gk",),
}.items():
    for _role in _roles:
        _ROLE_TO_GROUP[_role] = _group


def normalize_role(role: str) -> str:
    """슬롯 이름을 포지션 그룹으로. 모르는 슬롯은 대문자 그대로 둔다(집계에서 눈에 띄게)."""
    key = (role or "").strip().lower()
    if not key:
        # role 이 비어 오는 스쿼드가 실제로 있다. 자리를 모르면 집계에서 걸러내야 하니
        # 빈 문자열 대신 눈에 띄는 값으로 둔다.
        return "UNKNOWN"
    return _ROLE_TO_GROUP.get(key, key.upper())


def affiliation_team_colors(squad: dict) -> List[Tuple[str, int]]:
    """스쿼드 JSON 에서 넥슨이 판정한 '소속' 팀컬러 [(이름, 적용 인원), ...]."""
    total = squad.get("totalTeamColor") or {}
    affiliation = total.get("affiliation") or {}
    out = []
    for value in affiliation.values():
        name = (value.get("name") or "").strip()
        if name:
            out.append((name, int(value.get("playercnt") or 0)))
    out.sort(key=lambda item: -item[1])
    return out


def starters(squad: dict) -> List[dict]:
    """선발 11명만. 후보(state==1)는 이 통계에서 쓰지 않으므로 아예 버린다."""
    rows = []
    for player in squad.get("players") or []:
        if int(player.get("state", -1)) != STATE_STARTER:
            continue
        spid = player.get("spid")
        if not spid:
            continue
        role = (player.get("role") or "").strip().lower()
        rows.append(
            {
                # slot_index 는 스쿼드 안 순번(0..10)이다. slot_role 은 키로 못 쓴다 —
                # role 이 빈 문자열인 스쿼드가 실제로 있어서(1만명 중 1건) 한 스쿼드에
                # 같은 값이 두 번 나오면 PK 가 충돌한다.
                "slot_index": len(rows),
                "spid": int(spid),
                "build_up": int(player.get("buildUp") or 0),
                "slot_role": role,
                "position": normalize_role(role),
                "player_name": (player.get("name") or "").strip(),
            }
        )
    return rows


# =============================================================================
# HTTP
# =============================================================================
class Nexon:
    """넥슨 호출 담당. 스레드마다 별도 세션을 쓴다(requests.Session 은 스레드 공유 비권장)."""

    def __init__(self, delay: float, max_retries: int, timeout: int = 20):
        self.delay = delay
        self.max_retries = max_retries
        self.timeout = timeout
        self._local = threading.local()

    @property
    def session(self) -> requests.Session:
        session = getattr(self._local, "session", None)
        if session is None:
            session = requests.Session()
            # Accept-Encoding: gzip 이 핵심이다. 스쿼드 JSON 은 원문 80KB 인데
            # gzip 으로 12.5KB 로 줄어든다(10,000명이면 800MB → 125MB).
            session.headers.update(
                {"User-Agent": "Mozilla/5.0", "Accept-Encoding": "gzip, deflate"}
            )
            self._local.session = session
        return session

    def get(self, url: str, params=None, *, xhr: bool = False, referer: str = None) -> str:
        headers = {}
        if xhr:
            # ⚠️ 이 헤더가 없으면 SquadGetUserInfo 는 302 로 튕긴다.
            headers["X-Requested-With"] = "XMLHttpRequest"
        if referer:
            headers["Referer"] = referer

        last = None
        for attempt in range(self.max_retries):
            try:
                res = self.session.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=self.timeout,
                    allow_redirects=False,
                )
                if res.status_code in (301, 302, 303, 307, 308):
                    raise requests.HTTPError(f"{res.status_code} redirect (차단/파라미터 오류)")
                res.raise_for_status()
                if self.delay:
                    time.sleep(self.delay)
                return res.text
            except Exception as exc:  # noqa: BLE001 - 어떤 실패든 백오프 후 재시도
                last = exc
                if attempt == self.max_retries - 1:
                    break
                time.sleep((2.0 ** attempt) + random.random())
        raise RuntimeError(f"{url} 호출 실패: {last}")

    def rank_page(self, page: int) -> List[RankRow]:
        markup = self.get(
            RANK_INNER_URL,
            params={"rt": "1vs1", "n4seasonno": 0, "n4pageno": page},
            xhr=True,
            referer=f"{BASE}/datacenter/rank",
        )
        return parse_rank_page(markup)

    def character_id(self, nexon_sn: int) -> Optional[Tuple[int, int, str]]:
        markup = self.get(f"{SQUAD_POPUP_URL}/{nexon_sn}")
        return parse_character_id(markup)

    def squad(self, nexon_sn: int, character_id: str, team_type: int, slot: int) -> Optional[dict]:
        body = self.get(
            SQUAD_INFO_URL,
            params={
                "strTeamType": team_type,
                "n1Type": slot,
                "n8NexonSN": nexon_sn,
                "strCharacterID": character_id,
            },
            xhr=True,
            referer=f"{SQUAD_POPUP_URL}/{nexon_sn}",
        )
        body = body.strip()
        if not body:
            return None
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            return None


# =============================================================================
# 수집
# =============================================================================
def fetch_rank_rows(nexon: Nexon, limit: int = 0) -> List[RankRow]:
    """랭킹 1위부터 순서대로 limit 명(0=전체 10,000)."""
    pages = RANK_MAX_PAGE
    if limit:
        pages = min(RANK_MAX_PAGE, -(-limit // RANK_PAGE_SIZE))

    rows: List[RankRow] = []
    for page in range(1, pages + 1):
        page_rows = nexon.rank_page(page)
        if not page_rows:
            logger.warning("랭킹 %d페이지가 비어있습니다. 중단합니다.", page)
            break
        rows.extend(page_rows)
        if page % 50 == 0:
            logger.info("랭킹 %d/%d 페이지 (%d명)", page, pages, len(rows))

    # 같은 랭커가 두 번 잡히는 일은 없어야 하지만, 페이지 경계에서 순위가 밀리면
    # 중복이 생길 수 있다. 순위가 앞선 쪽을 남긴다.
    seen = {}
    for row in rows:
        if row.nexon_sn not in seen:
            seen[row.nexon_sn] = row
    unique = sorted(seen.values(), key=lambda r: r.rank)
    return unique[:limit] if limit else unique


class Resolution:
    """랭커 1명의 처리 결과."""

    __slots__ = ("row", "status", "team_type", "slot", "matched_count", "players",
                 "probes", "character_id")

    def __init__(self, row, status, team_type=None, slot=None, matched_count=None,
                 players=None, probes=0, character_id=None):
        self.row = row
        self.status = status  # matched | unresolved | no_team_color | no_character_id | error
        self.team_type = team_type
        self.slot = slot
        self.matched_count = matched_count
        self.players = players or []
        self.probes = probes
        self.character_id = character_id


def resolve_ranker(nexon: Nexon, row: RankRow, character_id: Optional[str] = None) -> Resolution:
    """랭킹 표의 팀컬러와 일치하는 대표팀 스쿼드를 찾아 선발 11명을 돌려준다.

    character_id 를 넘기면 popup 요청을 건너뛴다(이전 회차에 캐시해둔 값).
    """
    if not row.team_color_name:
        # 팀컬러가 적용되지 않은 스쿼드는 교차표의 축이 없으므로 대상이 아니다.
        return Resolution(row, "no_team_color")

    default_slot = None
    if character_id is None:
        info = nexon.character_id(row.nexon_sn)
        if not info:
            return Resolution(row, "no_character_id")
        _default_team_type, default_slot, character_id = info

    # 대표 스쿼드를 먼저 본다(표본상 4명 중 3명은 여기서 끝난다).
    order = [default_slot] if default_slot in SQUAD_SLOTS else []
    order += [slot for slot in SQUAD_SLOTS if slot != default_slot]

    probes = 0
    for slot in order:
        squad = nexon.squad(row.nexon_sn, character_id, TEAM_TYPE_NATIONAL, slot)
        probes += 1
        if not squad:
            continue
        for name, count in affiliation_team_colors(squad):
            if name != row.team_color_name:
                continue
            # 인원수는 일치하지 않을 수 있다. 랭킹 표는 경기 시점 값이고 프로필은
            # 현재 값이라, 같은 덱이어도 한두 명 교체로 숫자가 갈린다(밥상추 10 vs 11).
            # 이름이 같으면 같은 덱으로 본다.
            return Resolution(
                row, "matched", TEAM_TYPE_NATIONAL, slot, count, starters(squad), probes,
                character_id=character_id,
            )

    return Resolution(row, "unresolved", probes=probes, character_id=character_id)


def collect(nexon: Nexon, rows: List[RankRow], workers: int,
            character_ids: Dict[int, str] = None) -> Tuple[List[Resolution], Counter]:
    character_ids = character_ids or {}
    results: List[Resolution] = []
    stats = Counter()

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(resolve_ranker, nexon, row, character_ids.get(row.nexon_sn)): row
            for row in rows
        }
        done = 0
        for future in as_completed(futures):
            row = futures[future]
            try:
                resolution = future.result()
            except Exception as exc:  # noqa: BLE001 - 한 명이 죽어도 배치는 계속
                logger.warning("랭커 %s 처리 실패: %s", row.nexon_sn, exc)
                resolution = Resolution(row, "error")
            results.append(resolution)
            stats[resolution.status] += 1
            stats["probes"] += resolution.probes
            done += 1
            if done % 200 == 0:
                logger.info(
                    "스쿼드 %d/%d (matched=%d unresolved=%d)",
                    done, len(rows), stats["matched"], stats["unresolved"],
                )

    results.sort(key=lambda r: r.row.rank)
    return results, stats


# =============================================================================
# DB
# =============================================================================
def make_engine():
    """DATABASE_URL 로 엔진 생성. 비밀번호가 들어있으므로 절대 로그에 찍지 않는다."""
    url = os.environ.get("DATABASE_URL")
    if not url:
        return None
    url = url.replace("postgres://", "postgresql://", 1)
    if url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    # pool_pre_ping 은 체크아웃할 때만 돈다. 이 배치는 한 시간 넘게 도니까 쓰기는
    # 매번 새로 체크아웃해야 Supabase 트랜잭션 모드 풀러(:6543)가 조용히 끊어둔
    # 커넥션을 걸러낼 기회가 생긴다(2026-07-16 크롤러 실패 원인).
    return create_engine(url, pool_pre_ping=True, pool_recycle=600)


def _pg_array_literal(values: Iterable) -> str:
    """리스트를 Postgres 배열 리터럴 '{...}' 문자열 하나로 직렬화한다.

    ⚠️ 배열을 파이썬 리스트로 바인드하면 psycopg2 가 ARRAY[v1, v2, ...] 로
    인라인해서 배치 크기마다 다른 쿼리 텍스트가 되고, pg_stat_statements 가
    비대해져 Supabase Disk IO 예산을 태운다(2026-07 재발). 문자열 하나로 보내면
    상수가 항상 $1 한 개다.
    """
    parts = []
    for value in values:
        if value is None:
            parts.append("NULL")
        else:
            escaped = str(value).replace("\\", "\\\\").replace('"', '\\"')
            parts.append('"' + escaped + '"')
    return "{" + ",".join(parts) + "}"


_INSERT_SNAPSHOT_SQL = text(f"""
    INSERT INTO {PLAYER_SCHEMA}.ranker_squad_snapshot (
        collected_on, nexon_sn, rank, team_color_name, squad_slot,
        slot_index, slot_role, position_group, spid, build_up, player_name
    )
    SELECT * FROM unnest(
        CAST(:collected_on AS date[]),
        CAST(:nexon_sn AS bigint[]),
        CAST(:rank AS integer[]),
        CAST(:team_color_name AS text[]),
        CAST(:squad_slot AS smallint[]),
        CAST(:slot_index AS smallint[]),
        CAST(:slot_role AS text[]),
        CAST(:position_group AS text[]),
        CAST(:spid AS bigint[]),
        CAST(:build_up AS smallint[]),
        CAST(:player_name AS text[])
    )
    ON CONFLICT (collected_on, nexon_sn, slot_index) DO UPDATE SET
        rank = EXCLUDED.rank,
        team_color_name = EXCLUDED.team_color_name,
        squad_slot = EXCLUDED.squad_slot,
        slot_role = EXCLUDED.slot_role,
        position_group = EXCLUDED.position_group,
        spid = EXCLUDED.spid,
        build_up = EXCLUDED.build_up,
        player_name = EXCLUDED.player_name
""")

_UPSERT_CHARACTER_ID_SQL = text(f"""
    INSERT INTO {PLAYER_SCHEMA}.ranker_character_id (nexon_sn, character_id)
    SELECT * FROM unnest(
        CAST(:nexon_sn AS bigint[]),
        CAST(:character_id AS text[])
    )
    ON CONFLICT (nexon_sn) DO UPDATE SET
        character_id = EXCLUDED.character_id,
        updated_at = now()
    WHERE ranker_character_id.character_id IS DISTINCT FROM EXCLUDED.character_id
""")

_UPSERT_RUN_SQL = text(f"""
    INSERT INTO {PLAYER_SCHEMA}.ranker_squad_run
        (collected_on, ranker_cnt, matched_cnt, unresolved_cnt)
    VALUES (:collected_on, :ranker_cnt, :matched_cnt, :unresolved_cnt)
    ON CONFLICT (collected_on) DO UPDATE SET
        ranker_cnt = EXCLUDED.ranker_cnt,
        matched_cnt = EXCLUDED.matched_cnt,
        unresolved_cnt = EXCLUDED.unresolved_cnt,
        created_at = now()
""")

# 집계는 스냅샷에서 서버가 직접 만든다(행을 파이썬으로 왕복시키지 않는다).
# position_user_cnt = 그 팀컬러에서 그 자리를 채운 랭커 수 = 점유율의 분모.
_AGGREGATE_SQL = text(f"""
    WITH picks AS (
        SELECT team_color_name, position_group, spid, build_up
        FROM {PLAYER_SCHEMA}.ranker_squad_snapshot
        WHERE collected_on = :collected_on
          -- 자리를 모르는 선수(role 이 빈 값으로 온 스쿼드)는 포지션 축이 없으니 뺀다.
          AND position_group <> 'UNKNOWN'
    ),
    per_player AS (
        SELECT
            team_color_name,
            position_group,
            spid,
            COUNT(*) AS user_cnt,
            AVG(build_up) AS avg_build_up
        FROM picks
        GROUP BY team_color_name, position_group, spid
    ),
    per_position AS (
        SELECT team_color_name, position_group, COUNT(*) AS position_user_cnt
        FROM picks
        GROUP BY team_color_name, position_group
    )
    INSERT INTO {PLAYER_SCHEMA}.team_color_player_usage (
        collected_on, team_color_name, team_color_id, position_group,
        spid, user_cnt, position_user_cnt, avg_build_up
    )
    SELECT
        :collected_on,
        p.team_color_name,
        tc.id,
        p.position_group,
        p.spid,
        p.user_cnt,
        q.position_user_cnt,
        ROUND(p.avg_build_up, 2)
    FROM per_player p
    JOIN per_position q
      ON q.team_color_name = p.team_color_name
     AND q.position_group = p.position_group
    LEFT JOIN LATERAL (
        SELECT id FROM {PLAYER_SCHEMA}.team_colors
        WHERE name = p.team_color_name
        ORDER BY id
        LIMIT 1
    ) tc ON TRUE
    ON CONFLICT (collected_on, team_color_name, position_group, spid) DO UPDATE SET
        team_color_id = EXCLUDED.team_color_id,
        user_cnt = EXCLUDED.user_cnt,
        position_user_cnt = EXCLUDED.position_user_cnt,
        avg_build_up = EXCLUDED.avg_build_up
""")

_DB_MAX_RETRIES = 4


def _execute_with_retry(engine, statement, params) -> None:
    """커넥션을 매번 새로 체크아웃하고, 끊긴 커넥션은 풀째로 버리고 재시도한다."""
    for attempt in range(_DB_MAX_RETRIES):
        try:
            with engine.connect() as conn:
                conn.execute(statement, params)
                conn.commit()
            return
        except OperationalError as exc:
            if attempt == _DB_MAX_RETRIES - 1:
                raise
            engine.dispose()
            wait = 2.0 * (attempt + 1)
            logger.warning(
                "DB 쓰기 실패(%d/%d), %.1fs 후 재시도: %s",
                attempt + 1, _DB_MAX_RETRIES, wait, exc.orig or exc,
            )
            time.sleep(wait)


def read_character_ids(engine) -> Dict[int, str]:
    """이전 회차에 받아둔 characterId. 이게 있으면 popup 요청을 건너뛴다."""
    if engine is None:
        return {}
    sql = text(f"SELECT nexon_sn, character_id FROM {PLAYER_SCHEMA}.ranker_character_id")
    with engine.connect() as conn:
        return {row[0]: row[1] for row in conn.execute(sql)}


def save_character_ids(engine, pairs: Dict[int, str], batch: int) -> None:
    items = list(pairs.items())
    for start in range(0, len(items), batch):
        chunk = items[start:start + batch]
        _execute_with_retry(engine, _UPSERT_CHARACTER_ID_SQL, {
            "nexon_sn": _pg_array_literal(sn for sn, _ in chunk),
            "character_id": _pg_array_literal(cid for _, cid in chunk),
        })


def save_snapshot(engine, collected_on: date, results: List["Resolution"], batch: int) -> int:
    """이번 회차 스냅샷을 통째로 다시 쓴다(멱등: 같은 날 다시 돌리면 덮어쓴다)."""
    rows = []
    for resolution in results:
        if resolution.status != "matched":
            continue
        for index, player in enumerate(resolution.players):
            rows.append({
                "collected_on": collected_on,
                "nexon_sn": resolution.row.nexon_sn,
                "rank": resolution.row.rank,
                "team_color_name": resolution.row.team_color_name,
                "squad_slot": resolution.slot,
                # 덤프(--from-json)에는 slot_index 가 없을 수 있으니 순번으로 메운다.
                "slot_index": player.get("slot_index", index),
                "slot_role": player["slot_role"],
                "position_group": player["position"],
                "spid": player["spid"],
                "build_up": player["build_up"],
                "player_name": player["player_name"],
            })

    for start in range(0, len(rows), batch):
        chunk = rows[start:start + batch]
        _execute_with_retry(engine, _INSERT_SNAPSHOT_SQL, {
            key: _pg_array_literal(row[key] for row in chunk)
            for key in (
                "collected_on", "nexon_sn", "rank", "team_color_name", "squad_slot",
                "slot_index", "slot_role", "position_group", "spid", "build_up",
                "player_name",
            )
        })
    return len(rows)


def aggregate(engine, collected_on: date) -> None:
    _execute_with_retry(engine, _AGGREGATE_SQL, {"collected_on": collected_on})


# =============================================================================
# 보고 (DB 없이 눈으로 확인할 때)
# =============================================================================
def build_cross_tab(results: List["Resolution"]):
    """matched 결과 → {팀컬러: {"rankers": set, "positions": {그룹: Counter(spid)}}}"""
    table: Dict[str, dict] = {}
    names: Dict[int, str] = {}
    for resolution in results:
        if resolution.status != "matched":
            continue
        bucket = table.setdefault(
            resolution.row.team_color_name,
            {"rankers": set(), "positions": {}, "build_up": {}},
        )
        bucket["rankers"].add(resolution.row.nexon_sn)
        for player in resolution.players:
            group = player["position"]
            bucket["positions"].setdefault(group, Counter())[player["spid"]] += 1
            bucket["build_up"].setdefault((group, player["spid"]), []).append(
                player["build_up"]
            )
            if player["player_name"]:
                names[player["spid"]] = player["player_name"]
    return table, names


def print_report(results: List["Resolution"], team_color: Optional[str] = None,
                 groups: Iterable[str] = None, top: int = 10) -> None:
    table, names = build_cross_tab(results)
    if not table:
        print("집계할 스쿼드가 없습니다.")
        return

    ranking = sorted(table.items(), key=lambda kv: -len(kv[1]["rankers"]))
    print("\n=== 랭커가 많이 쓴 팀컬러 ===")
    total = sum(len(v["rankers"]) for v in table.values())
    for idx, (name, data) in enumerate(ranking[:15], 1):
        cnt = len(data["rankers"])
        print(f"{idx:2}. {name:22} {cnt:5}명  ({cnt / total * 100:4.1f}%)")

    target = team_color or ranking[0][0]
    data = table.get(target)
    if not data:
        print(f"\n'{target}' 팀컬러 집계가 없습니다.")
        return

    ranker_cnt = len(data["rankers"])
    for group in (groups or POSITION_GROUPS):
        counter = data["positions"].get(group)
        if not counter:
            continue
        label = POSITION_GROUP_LABELS.get(group, group)
        slots = sum(counter.values())
        print(f"\n=== {target} ({ranker_cnt}명) — {label} 인기 순위 ===")
        print(f"    (이 자리 연인원 {slots}명 기준)")
        for idx, (spid, cnt) in enumerate(counter.most_common(top), 1):
            builds = data["build_up"].get((group, spid), [])
            avg = sum(builds) / len(builds) if builds else 0
            name = names.get(spid, "?")
            print(
                f"{idx:2}. {name:18} {cnt:5}회  {cnt / slots * 100:5.1f}%"
                f"  평균 +{avg:.1f}  spid={spid}"
            )


# =============================================================================
# 메인
# =============================================================================
def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except ValueError:
        return default


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="랭커 대표팀 스쿼드 수집")
    parser.add_argument("--limit", type=int, default=_env_int("RANKER_SQUAD_LIMIT", 0),
                        help="처리할 랭커 수 (0=전체 10,000)")
    parser.add_argument("--workers", type=int, default=_env_int("RANKER_SQUAD_WORKERS", 6))
    parser.add_argument("--from-json", default=None, metavar="경로",
                        help="크롤링 대신 --out 으로 떠둔 JSON 을 읽어 보고만 한다")
    parser.add_argument("--report", nargs="?", const="", default=None,
                        metavar="팀컬러",
                        help="수집 결과를 표로 출력한다. 값을 생략하면 가장 인기 있는 팀컬러")
    parser.add_argument("--report-position", default=None, metavar="그룹",
                        help="--report 에서 볼 포지션 그룹 (FW/CAM/WING/CM/CDM/CB/FB/GK)")
    parser.add_argument("--no-db", action="store_true",
                        help="DATABASE_URL 이 있어도 DB 에 쓰지 않는다")
    parser.add_argument("--out", default=None,
                        help="결과를 JSON 파일로 덤프한다(DB 없이 확인할 때)")
    return parser


def main(argv=None) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    args = build_arg_parser().parse_args(argv)

    if args.from_json:
        # 이미 떠둔 덤프로 표를 다시 그리거나 DB 에 적재한다. 같은 수집을 두 번
        # 돌리지 않으려는 것(1만명 크롤은 35분 + 넥슨 요청 1.2만 건이다).
        with open(args.from_json, encoding="utf-8") as fp:
            payload = json.load(fp)
        # 덤프는 그때의 포지션 분류를 담고 있다. 분류 규칙이 바뀌었을 수 있으므로
        # 슬롯 원문에서 지금 규칙으로 다시 매긴다.
        for item in payload["rankers"]:
            for index, player in enumerate(item["players"]):
                player.setdefault("slot_index", index)
                player["position"] = normalize_role(player["slot_role"])
        results = [
            Resolution(
                RankRow(
                    item["rank"], item["nexon_sn"], item["nickname"],
                    item["team_color_name"], None,
                ),
                "matched", TEAM_TYPE_NATIONAL, item.get("squad_slot"),
                None, item["players"],
            )
            for item in payload["rankers"]
        ]
        engine = None if args.no_db else make_engine()
        if engine is not None:
            collected_on = date.fromisoformat(payload["collected_on"])
            batch = _env_int("RANKER_SQUAD_DB_BATCH", 2000)
            written = save_snapshot(engine, collected_on, results, batch)
            logger.info("스냅샷 %d행 저장 (%s)", written, collected_on)
            aggregate(engine, collected_on)
            stats = payload.get("stats") or {}
            _execute_with_retry(engine, _UPSERT_RUN_SQL, {
                "collected_on": collected_on,
                "ranker_cnt": sum(
                    int(stats.get(key, 0))
                    for key in ("matched", "unresolved", "no_team_color",
                                "no_character_id", "error")
                ) or len(results),
                "matched_cnt": int(stats.get("matched", len(results))),
                "unresolved_cnt": int(stats.get("unresolved", 0)),
            })
            logger.info("집계 완료")

        if args.report is not None:
            print_report(
                results,
                team_color=(args.report or None),
                groups=[args.report_position] if args.report_position else None,
            )
        return 0

    nexon = Nexon(
        delay=_env_float("RANKER_SQUAD_REQUEST_DELAY", 0.15),
        max_retries=_env_int("RANKER_SQUAD_MAX_RETRIES", 3),
    )

    engine = None if args.no_db else make_engine()
    if engine is None and not args.out:
        logger.warning("DATABASE_URL 이 없습니다. 수집만 하고 아무 데도 쓰지 않습니다.")

    started = time.time()
    rows = fetch_rank_rows(nexon, args.limit)
    logger.info("랭커 %d명 수집 (%.1fs)", len(rows), time.time() - started)

    # 이전 회차의 characterId 가 있으면 랭커당 popup 요청 1건씩을 통째로 아낀다.
    character_ids = read_character_ids(engine) if engine is not None else {}
    if character_ids:
        reused = sum(1 for row in rows if row.nexon_sn in character_ids)
        logger.info("characterId 캐시 %d건 중 %d명 재사용", len(character_ids), reused)

    results, stats = collect(nexon, rows, args.workers, character_ids)
    matched = [r for r in results if r.status == "matched"]
    logger.info(
        "완료: matched=%d unresolved=%d no_team_color=%d no_character_id=%d error=%d "
        "(스쿼드 요청 %d건, %.1fs)",
        stats["matched"], stats["unresolved"], stats["no_team_color"],
        stats["no_character_id"], stats["error"], stats["probes"], time.time() - started,
    )

    collected_on = date.today()
    if engine is not None:
        batch = _env_int("RANKER_SQUAD_DB_BATCH", 2000)
        fresh = {
            r.character_id: r.row.nexon_sn
            for r in results
            if r.character_id and character_ids.get(r.row.nexon_sn) != r.character_id
        }
        if fresh:
            save_character_ids(engine, {sn: cid for cid, sn in fresh.items()}, batch)
            logger.info("characterId %d건 저장", len(fresh))

        written = save_snapshot(engine, collected_on, results, batch)
        logger.info("스냅샷 %d행 저장 (%s)", written, collected_on)

        aggregate(engine, collected_on)
        _execute_with_retry(engine, _UPSERT_RUN_SQL, {
            "collected_on": collected_on,
            "ranker_cnt": len(rows),
            "matched_cnt": stats["matched"],
            "unresolved_cnt": stats["unresolved"],
        })
        logger.info("집계 완료")

    if args.out:
        payload = {
            "collected_on": date.today().isoformat(),
            "stats": dict(stats),
            "rankers": [
                {
                    "rank": r.row.rank,
                    "nexon_sn": r.row.nexon_sn,
                    "nickname": r.row.nickname,
                    "team_color_name": r.row.team_color_name,
                    "squad_slot": r.slot,
                    "players": r.players,
                }
                for r in matched
            ],
        }
        with open(args.out, "w", encoding="utf-8") as fp:
            json.dump(payload, fp, ensure_ascii=False, indent=2)
        logger.info("%s 에 덤프했습니다.", args.out)

    if args.report is not None:
        print_report(
            results,
            team_color=args.report or None,
            groups=[args.report_position] if args.report_position else None,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
