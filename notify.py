"""
목요일 메타 크롤 결과 요약을 카카오톡("나에게 보내기")으로 보낸다. 실패 시 이메일 폴백.

meta-thursday 워크플로의 notify 잡에서 crawl 4샤드가 끝난 뒤 호출된다. 같은 실행의
모든 단계가 META_RUN_ID(=github.run_id)로 player.meta_crawl_log 에 적어둔 결과를
run_id 로 모아 집계한다.

⚠️ 비밀정보는 전부 GitHub Secret 으로만 주입한다(아래 환경변수). 로그에 토큰/DB 출력 금지.

환경변수
    DATABASE_URL          (필수) Postgres 접속 문자열
    META_RUN_ID           (필수) 이 실행 ID. meta_crawl_log 집계 키
    EXPECTED_SHARDS       crawl 샤드 수(기본 4). 로그된 샤드가 이보다 적으면 "샤드 누락" 경고
    NOTIFY_TOP_N          신규 선수 미리보기 인원(기본 5, 0=미리보기 끔)
    EXPECT_CAREER         '1' 이면 이 실행에 career_job.py 가 돈다는 뜻. 그 로그가 없으면 경고 줄
    KAKAO_REST_KEY        카카오 디벨로퍼스 REST API 키
    KAKAO_REFRESH_TOKEN   카카오 OAuth refresh token(1회 발급해 Secret 저장)
    SMTP_HOST/PORT/USER/PASS, NOTIFY_EMAIL_TO   (선택) 카톡 실패 시 이메일 폴백

카톡 셋업은 같은 폴더 KAKAO_SETUP.md 참고.
"""

import json
import os
import re
import smtplib
import sys
from email.mime.text import MIMEText
from typing import Optional

import requests
from sqlalchemy import create_engine, text

from meta_crawler import _pg_array_literal

KAUTH_TOKEN_URL = "https://kauth.kakao.com/oauth/token"
KAKAO_MEMO_URL = "https://kapi.kakao.com/v2/api/talk/memo/default/send"


def make_engine():
    url = os.environ["DATABASE_URL"]
    url = url.replace("postgres://", "postgresql://", 1)
    if url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    return create_engine(url, pool_pre_ping=True)


# 신규 spid 를 players/seasonid 와 조인해 상위 N명만 뽑는다(알림 미리보기용).
# ⚠️ spid 배열은 _pg_array_literal() 문자열 1개로 바인드한다 — 파이썬 리스트 바인드는
#    원소 수별로 pg_stat_statements 엔트리가 갈라진다(meta_crawler 의 같은 규칙).
#
# 정렬은 앱 검색 결과의 기본 정렬(main_ovr DESC, spid ASC)과 일부러 똑같이 맞춘다.
# 백엔드 players/crud.py 의 normalize_search_sort() 기본값(sort_by='overall',
# sort_order='desc') + tie-breaker asc(Player.spid) 와 대응 — 알림에서 본 순서와
# 앱에서 본 순서가 어긋나지 않게 하기 위함이다. 저쪽을 바꾸면 여기도 같이 바꿀 것.
# NULLS LAST 는 백엔드의 coalesce(main_ovr, -1) 와 같은 결과(내림차순에서 NULL 뒤로).
# 성장형 시즌(예: 596 SH)은 전원 같은 OVR 이라 사실상 spid 순으로 나온다.
_TOP_NEW_SQL = text("""
    SELECT p.name, s.class_name
    FROM player.players p
    LEFT JOIN player.seasonid s ON s.season_id = p.season_id
    WHERE p.spid = ANY(CAST(:spids AS bigint[]))
    ORDER BY p.main_ovr DESC NULLS LAST, p.spid ASC
    LIMIT :lim
""")


def _season_abbrev(class_name: str) -> str:
    """'SH (Step Higher)' → 'SH'. class_name 은 전부 '약칭 (English Full Name)' 꼴이다.

    ⚠️ **마지막** 괄호에서 자른다. 첫 괄호로 자르면 시즌 110 'ICON TM(제한) (ICON The
    Moment Bound)' 이 'ICON TM' 이 돼 시즌 100 'ICON TM (ICON The Moment)' 과 구별이
    안 된다. 마지막 괄호 기준이면 149개 시즌 약칭이 전부 고유하다(2026-07 전수 확인).
    """
    if not class_name:
        return ""
    return class_name.rsplit("(", 1)[0].strip() or class_name.strip()


def _top_new_players(engine, spids: list, limit: int) -> list:
    """crawl 이 방금 넣은 신규 카드 중 상위 limit 명. [(시즌약칭, 이름), ...]"""
    if not spids or limit <= 0:
        return []
    with engine.connect() as c:
        rows = c.execute(
            _TOP_NEW_SQL, {"spids": _pg_array_literal(spids), "lim": limit}
        ).all()
    return [(_season_abbrev(r.class_name), r.name) for r in rows]


# ── 변동 카드 미리보기 ────────────────────────────────────────────────────────
# meta_crawl_log 는 변동 "개수"만 남기고 어떤 카드가 바뀌었는지는 안 남긴다
# (신규는 sync 가 new_spids 를 남겨서 알 수 있지만 변동은 대응물이 없다).
# 그래서 시각으로 역산한다: 이 run 의 첫 로그(=sync 트랜잭션 시작) 이후에 updated_at
# 이 갱신됐고, 그 전에 이미 존재했던(created_at < since) 카드 = 이번에 바뀐 카드.
# meta_crawler._upsert_player 가 INSERT/UPDATE 양쪽에서 updated_at=now() 를 찍고,
# 해시가 같으면 아예 write 를 건너뛰므로 이 조건이 changed 집합과 정확히 일치한다.
# 신규 카드는 created_at >= since 라 자동으로 빠진다.
#
# ⚠️ players.created_at/updated_at 은 timestamp WITHOUT tz(naive UTC 로 적재)인데
#    meta_crawl_log.created_at 은 timestamptz 다. 세션 타임존에 의존하지 않도록
#    AT TIME ZONE 'UTC' 로 명시 변환해서 비교한다.
# ⚠️ 이 방식은 crawl 중 players 를 건드리는 다른 writer 가 없다는 전제에 기댄다.
#    (시세 크롤러는 player_price_latest 만 쓴다. 이 전제가 깨지면 changed_spids 를
#     meta_crawl_log 에 명시적으로 남기는 쪽으로 바꿔야 한다.)
_RUN_SINCE_SQL = text("""
    SELECT min(created_at) AT TIME ZONE 'UTC'
    FROM player.meta_crawl_log
    WHERE run_id = :rid
""")

_CHANGED_PREVIEW_SQL = text("""
    SELECT p.name, s.class_name
    FROM player.players p
    LEFT JOIN player.seasonid s ON s.season_id = p.season_id
    WHERE p.updated_at >= :since AND p.created_at < :since
    ORDER BY p.main_ovr DESC NULLS LAST, p.spid ASC
    LIMIT :lim
""")


def _top_changed_players(engine, run_id: str, limit: int) -> list:
    """이번 run 에서 내용이 바뀐 카드 중 상위 limit 명. [(시즌약칭, 이름), ...]"""
    if limit <= 0:
        return []
    with engine.connect() as c:
        since = c.execute(_RUN_SINCE_SQL, {"rid": run_id}).scalar()
        if since is None:
            return []
        rows = c.execute(
            _CHANGED_PREVIEW_SQL, {"since": since, "lim": limit}
        ).all()
    return [(_season_abbrev(r.class_name), r.name) for r in rows]


def aggregate(engine, run_id: str) -> dict:
    """meta_crawl_log 를 run_id 로 모아 요약 dict 를 만든다."""
    with engine.connect() as c:
        rows = c.execute(
            text("""
                SELECT phase, shard, new_players, changed, failures,
                       new_traits, new_team_colors, new_seasons, new_spids
                FROM player.meta_crawl_log
                WHERE run_id = :rid
            """),
            {"rid": run_id},
        ).mappings().all()

    new_players = changed = failures = 0
    crawl_shards = set()
    new_traits = {}       # id -> name (중복 제거)
    new_team_colors = {}  # id -> name (샤드마다 겹칠 수 있어 id 로 합친다)
    new_seasons = {}      # season_id -> dict (sync 행이 여러 개일 수 있어 id 로 합친다:
                          # static 메타로 들어온 신규 시즌 + 데이터센터 시드 행)
    new_spids = []
    career = None         # career_job.py 결과(phase='career'). 잡이 안 돌았으면 None.
    gone_total = 0        # 넥슨이 지워 크롤에서 뺀 spid 수(phase='gone', 금요일만)
    newly_gone = []       # 그중 이번 주에 새로 사라진 것
    missing_total = 0     # 넥슨이 오류 페이지로 돌려 숨긴 카드 수(phase='missing', 풀크롤 샤드별)
    newly_missing = []    # 그중 이번에 처음 숨긴 것

    for r in rows:
        if r["phase"] == "gone":
            # gone 행도 new_spids 칸에 세부 dict 를 담는다(meta_crawler._log_gone_spids).
            detail = r["new_spids"] or {}
            gone_total = len(detail.get("gone") or [])
            newly_gone = list(detail.get("newly_gone") or [])
        elif r["phase"] == "missing":
            # missing 행도 new_spids 칸에 세부 dict 를 담는다(meta_crawler._record_missing).
            detail = r["new_spids"] or {}
            missing_total += len(detail.get("missing") or [])
            newly_missing.extend(detail.get("newly_missing") or [])
        elif r["phase"] == "career":
            # career 행은 new_spids 칸에 세부 dict 를 담는다(career_job._log_result).
            detail = r["new_spids"] or {}
            career = career or {"new": 0, "changed": 0, "failures": 0,
                                "emptied_skipped": 0, "aborted": False, "mode": ""}
            career["new"] += r["new_players"] or 0
            career["changed"] += r["changed"] or 0
            career["failures"] += r["failures"] or 0
            career["emptied_skipped"] += detail.get("emptied_skipped") or 0
            career["aborted"] = career["aborted"] or bool(detail.get("aborted"))
            career["mode"] = detail.get("mode") or career["mode"]
        elif r["phase"] == "crawl":
            new_players += r["new_players"] or 0
            changed += r["changed"] or 0
            failures += r["failures"] or 0
            if r["shard"] is not None:
                crawl_shards.add(r["shard"])
            for t in (r["new_traits"] or []):
                new_traits[t["id"]] = t["name"]
            for tc in (r["new_team_colors"] or []):
                new_team_colors[tc["id"]] = tc["name"]
        elif r["phase"] == "sync":
            for s in (r["new_seasons"] or []):
                new_seasons.setdefault(s.get("season_id"), {}).update(s)
            new_spids.extend(r["new_spids"] or [])

    expected = int(os.getenv("EXPECTED_SHARDS", "4"))
    top_n = int(os.getenv("NOTIFY_TOP_N", "5"))
    # sync 가 신규로 잡은 spid 를 crawl 이 채운 뒤라 이 시점엔 players 에서 읽을 수 있다.
    # 크롤 실패분은 players 에 없어 조용히 빠진다(미리보기라 실패는 🔴 줄이 따로 알린다).
    top_new = _top_new_players(engine, new_spids, top_n)
    top_changed = _top_changed_players(engine, run_id, top_n) if changed else []
    # 사라진 카드도 players 행은 남아 있어 신규 미리보기와 같은 조회로 이름을 얻는다.
    top_gone = _top_new_players(engine, newly_gone, top_n)
    top_missing = _top_new_players(engine, newly_missing, top_n)

    return {
        "run_id": run_id,
        "new_players": new_players,
        "new_spid_count": len(new_spids),
        "changed": changed,
        "failures": failures,
        "new_traits": new_traits,
        "new_team_colors": new_team_colors,
        "new_seasons": list(new_seasons.values()),
        "top_new": top_new,
        "top_changed": top_changed,
        "shards_logged": len(crawl_shards),
        "shards_expected": expected,
        "missing_shards": expected - len(crawl_shards),
        "career": career,
        "gone_total": gone_total,
        "newly_gone_count": len(newly_gone),
        "top_gone": top_gone,
        "missing_total": missing_total,
        "newly_missing_count": len(newly_missing),
        "top_missing": top_missing,
        # 워크플로가 career 잡을 돌렸는데 로그가 없으면(잡이 로그 전에 죽음) 경고 줄을 띄운다.
        "career_expected": os.getenv("EXPECT_CAREER", "0") == "1",
    }


def _card_preview(cards: list, total: int, unit: str = "명") -> str:
    """[(시즌약칭, 이름), ...] → 'SH 크리스티아누 호날두, SH 리오넬 메시 외 32명'"""
    body = ", ".join(f"{abbrev} {name}".strip() for abbrev, name in cards)
    more = total - len(cards)
    return body + (f" 외 {more}{unit}" if more > 0 else "")


def _career_line(s: dict) -> Optional[str]:
    """클럽 경력 결과 한 줄. 경력 잡을 안 도는 실행이면 None.

    목요일(pending)은 새 선수만 긁고 기존 경력은 보지 않으므로 '변경'을 빼고 '신규'만 쓴다.
    """
    c = s.get("career")
    if c is None:
        return "⚠️ 클럽경력 결과 없음 (잡 실패 의심)" if s.get("career_expected") else None
    parts = []
    if c["mode"] != "pending":
        parts.append(f"변경 {c['changed']:,}")
    parts.append(f"신규 {c['new']:,}")
    if c["failures"]:
        parts.append(f"실패 {c['failures']:,}")
    if c["emptied_skipped"]:
        parts.append(f"빈경력 보류 {c['emptied_skipped']:,}")
    head = "⚠️ 클럽경력 중단" if c["aborted"] else "⚪ 클럽경력"
    return head + " " + "  ".join(parts)


def build_message(s: dict, max_len: Optional[int] = None) -> str:
    """요약 텍스트. max_len 을 주면 그 길이로 잘라낸다(카카오 text 템플릿 200자 제한용).

    이메일 폴백은 길이 제한이 없으므로 max_len 없이 호출해 전문을 보낸다.
    """
    short_run = str(s["run_id"])[-6:]
    label = os.getenv("NOTIFY_LABEL", "메타 크롤")   # 워크플로가 요일별 라벨 주입
    lines = [f"[FCO {label} #{short_run}]"]

    # 이상신호 먼저(수상한 것만 보면 되게)
    if s["missing_shards"] > 0:
        lines.append(f"⚠️ 샤드 누락 {s['missing_shards']}/{s['shards_expected']} (잡 실패 의심)")
    if s["failures"] > 0:
        lines.append(f"🔴 크롤 실패 {s['failures']}건")

    lines.append(f"🟢 신규선수 {s['new_players']}  🔵 변동 {s['changed']}장")
    # 팀컬러는 집계 줄에 얹는다(카카오 200자 제한 때문에 줄을 더 늘리지 않는다).
    lines.append(
        f"🟡 신규특성 {len(s['new_traits'])}  🟣 신규시즌 {len(s['new_seasons'])}"
        f"  🟠 신규팀컬러 {len(s.get('new_team_colors') or {})}"
    )
    # 클럽 경력도 집계 줄 묶음에 둔다(아래 미리보기보다 위 — 길이 제한에 잘려도 남게).
    career_line = _career_line(s)
    if career_line:
        lines.append(career_line)

    # 미리보기는 집계 아래로 — 길이 초과로 잘려도 위의 경고/집계는 살아남는다.
    if s["new_traits"]:
        names = list(s["new_traits"].values())
        shown = ", ".join(names[:5])
        more = len(names) - 5
        lines.append(f"• 특성: {shown}" + (f" 외 {more}개" if more > 0 else ""))
    if s.get("new_team_colors"):
        names = list(s["new_team_colors"].values())
        shown = ", ".join(names[:3])
        more = len(names) - 3
        lines.append(f"• 팀컬러: {shown}" + (f" 외 {more}개" if more > 0 else ""))
    if s["new_seasons"]:
        # 데이터센터 시드로 spid 를 채운 시즌은 표시해 둔다(출시 당일 폴백이 돌았다는 신호).
        names = [
            x.get("class_name", "?") + (
                f"(시드 {x['seeded_from_datacenter']}장)"
                if x.get("seeded_from_datacenter") else ""
            )
            for x in s["new_seasons"]
        ]
        shown = ", ".join(names[:3])
        more = len(names) - 3
        lines.append(f"• 시즌: {shown}" + (f" 외 {more}개" if more > 0 else ""))

    if s["top_new"]:
        lines.append("• 신규: " + _card_preview(s["top_new"], s["new_spid_count"]))
    if s["top_changed"]:
        lines.append("• 변동: " + _card_preview(s["top_changed"], s["changed"], unit="장"))
    # 넥슨이 지운 카드는 크롤에서 빼므로 실패로 세지 않는다. 이번 주에 새로 빠진 것만
    # 알린다(이미 알린 카드를 매주 반복하면 예전 "실패 5건" 과 같은 소음이 된다).
    if s.get("newly_gone_count"):
        lines.append("• 사라진 카드(크롤 제외): "
                     + _card_preview(s["top_gone"], s["newly_gone_count"], unit="장"))
    # 넥슨이 오류 페이지로 돌리는 카드(시즌 번호 이동 등)도 실패가 아니라 숨김으로 알린다.
    if s.get("newly_missing_count"):
        lines.append("• 넥슨 오류 페이지(검색 숨김): "
                     + _card_preview(s["top_missing"], s["newly_missing_count"], unit="장"))

    career = s.get("career")
    career_quiet = (
        (career is None and not s.get("career_expected"))
        or (career is not None and career["new"] == 0 and career["changed"] == 0
            and career["failures"] == 0 and career["emptied_skipped"] == 0
            and not career["aborted"])
    )
    if (s["new_players"] == 0 and s["changed"] == 0 and not s["new_traits"]
            and not s.get("new_team_colors") and not s["new_seasons"]
            and s["failures"] == 0 and s["missing_shards"] == 0 and career_quiet
            and not s.get("newly_gone_count") and not s.get("newly_missing_count")):
        lines.append("변동 없음 (정상 동작)")

    msg = "\n".join(lines)
    if max_len is not None and len(msg) > max_len:
        return msg[:max_len - 1] + "…"
    return msg


def run_link() -> str:
    server = os.getenv("GITHUB_SERVER_URL", "https://github.com")
    repo = os.getenv("GITHUB_REPOSITORY", "")
    rid = os.getenv("META_RUN_ID", "")
    if repo and rid:
        return f"{server}/{repo}/actions/runs/{rid}"
    return "https://github.com"


# ── 카카오톡 "나에게 보내기" ───────────────────────────────────────────────
def kakao_access_token() -> str:
    res = requests.post(
        KAUTH_TOKEN_URL,
        data={
            "grant_type": "refresh_token",
            "client_id": os.environ["KAKAO_REST_KEY"],
            "refresh_token": os.environ["KAKAO_REFRESH_TOKEN"],
        },
        timeout=15,
    )
    res.raise_for_status()
    body = res.json()
    # 카카오는 refresh_token 만료가 가까우면 새 refresh_token 을 함께 준다 → 수동으로
    # Secret 갱신 필요(자동 갱신은 repo secret 쓰기 권한이 있어야 해 생략).
    if body.get("refresh_token"):
        print("⚠️ 카카오가 새 refresh_token 을 반환했습니다. "
              "GitHub Secret KAKAO_REFRESH_TOKEN 을 갱신하세요(만료 임박 신호).")
    return body["access_token"]


def send_kakao(message: str, link: str) -> None:
    token = kakao_access_token()
    template = {
        "object_type": "text",
        "text": message,
        "link": {"web_url": link, "mobile_web_url": link},
        "button_title": "실행 로그 보기",
    }
    res = requests.post(
        KAKAO_MEMO_URL,
        headers={"Authorization": f"Bearer {token}"},
        data={"template_object": json.dumps(template, ensure_ascii=False)},
        timeout=15,
    )
    res.raise_for_status()
    print("카카오톡 전송 성공")


# ── 이메일 폴백 ─────────────────────────────────────────────────────────────
def send_email(message: str, link: str) -> None:
    host = os.getenv("SMTP_HOST")
    to = os.getenv("NOTIFY_EMAIL_TO")
    if not host or not to:
        raise RuntimeError("SMTP 설정 없음(이메일 폴백 불가)")
    msg = MIMEText(f"{message}\n\n{link}", _charset="utf-8")
    msg["Subject"] = f"FCO {os.getenv('NOTIFY_LABEL', '메타 크롤')} 요약"
    msg["From"] = os.getenv("SMTP_USER", to)
    msg["To"] = to
    port = int(os.getenv("SMTP_PORT", "587"))
    with smtplib.SMTP(host, port, timeout=20) as srv:
        srv.starttls()
        srv.login(os.environ["SMTP_USER"], os.environ["SMTP_PASS"])
        srv.send_message(msg)
    print("이메일 전송 성공")


def main() -> None:
    run_id = os.environ["META_RUN_ID"]
    engine = make_engine()
    summary = aggregate(engine, run_id)
    link = run_link()
    # 카톡은 text 템플릿 200자 제한이 있어 잘라 보내고, 이메일은 제한이 없어 전문을 보낸다.
    full_message = build_message(summary)
    kakao_message = build_message(summary, max_len=196)
    print("요약:\n" + full_message)

    # 카톡 우선, 실패하면 이메일 폴백. 둘 다 실패해도 잡은 죽이지 않는다(요약은 로그에 있음).
    try:
        send_kakao(kakao_message, link)
        return
    except Exception as exc:
        print(f"카카오톡 전송 실패: {exc}")
    try:
        send_email(full_message, link)
    except Exception as exc:
        print(f"이메일 폴백도 실패: {exc}", file=sys.stderr)


if __name__ == "__main__":
    main()
