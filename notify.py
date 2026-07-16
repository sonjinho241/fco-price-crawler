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


def aggregate(engine, run_id: str) -> dict:
    """meta_crawl_log 를 run_id 로 모아 요약 dict 를 만든다."""
    with engine.connect() as c:
        rows = c.execute(
            text("""
                SELECT phase, shard, new_players, changed, failures,
                       new_traits, new_seasons, new_spids
                FROM player.meta_crawl_log
                WHERE run_id = :rid
            """),
            {"rid": run_id},
        ).mappings().all()

    new_players = changed = failures = 0
    crawl_shards = set()
    new_traits = {}     # id -> name (중복 제거)
    new_seasons = []
    new_spids = []

    for r in rows:
        if r["phase"] == "crawl":
            new_players += r["new_players"] or 0
            changed += r["changed"] or 0
            failures += r["failures"] or 0
            if r["shard"] is not None:
                crawl_shards.add(r["shard"])
            for t in (r["new_traits"] or []):
                new_traits[t["id"]] = t["name"]
        elif r["phase"] == "sync":
            for s in (r["new_seasons"] or []):
                new_seasons.append(s)
            new_spids.extend(r["new_spids"] or [])

    expected = int(os.getenv("EXPECTED_SHARDS", "4"))
    # sync 가 신규로 잡은 spid 를 crawl 이 채운 뒤라 이 시점엔 players 에서 읽을 수 있다.
    # 크롤 실패분은 players 에 없어 조용히 빠진다(미리보기라 실패는 🔴 줄이 따로 알린다).
    top_new = _top_new_players(engine, new_spids, int(os.getenv("NOTIFY_TOP_N", "5")))

    return {
        "run_id": run_id,
        "new_players": new_players,
        "new_spid_count": len(new_spids),
        "changed": changed,
        "failures": failures,
        "new_traits": new_traits,
        "new_seasons": new_seasons,
        "top_new": top_new,
        "shards_logged": len(crawl_shards),
        "shards_expected": expected,
        "missing_shards": expected - len(crawl_shards),
    }


def build_message(s: dict) -> str:
    """카카오 text 템플릿용 요약(200자 제한 고려, 넘으면 잘라낸다)."""
    short_run = str(s["run_id"])[-6:]
    label = os.getenv("NOTIFY_LABEL", "메타 크롤")   # 워크플로가 요일별 라벨 주입
    lines = [f"[FCO {label} #{short_run}]"]

    # 이상신호 먼저(수상한 것만 보면 되게)
    if s["missing_shards"] > 0:
        lines.append(f"⚠️ 샤드 누락 {s['missing_shards']}/{s['shards_expected']} (잡 실패 의심)")
    if s["failures"] > 0:
        lines.append(f"🔴 크롤 실패 {s['failures']}건")

    lines.append(f"🟢 신규선수 {s['new_players']}  🔵 변동 {s['changed']}장")
    lines.append(f"🟡 신규특성 {len(s['new_traits'])}  🟣 신규시즌 {len(s['new_seasons'])}")

    if s["new_traits"]:
        names = ", ".join(list(s["new_traits"].values())[:5])
        lines.append(f"• 특성: {names}")
    if s["new_seasons"]:
        names = ", ".join(x.get("class_name", "?") for x in s["new_seasons"][:3])
        lines.append(f"• 시즌: {names}")

    # 신규 선수 미리보기는 맨 끝 — 200자를 넘겨 잘리더라도 위의 경고/집계는 살아남는다.
    if s["top_new"]:
        preview = ", ".join(
            f"{abbrev} {name}".strip() for abbrev, name in s["top_new"]
        )
        more = s["new_spid_count"] - len(s["top_new"])
        lines.append(f"• 신규: {preview}" + (f" 외 {more}명" if more > 0 else ""))

    if (s["new_players"] == 0 and s["changed"] == 0 and not s["new_traits"]
            and not s["new_seasons"] and s["failures"] == 0 and s["missing_shards"] == 0):
        lines.append("변동 없음 (정상 동작)")

    msg = "\n".join(lines)
    return msg[:195] + "…" if len(msg) > 196 else msg


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
    message = build_message(summary)
    link = run_link()
    print("요약:\n" + message)

    # 카톡 우선, 실패하면 이메일 폴백. 둘 다 실패해도 잡은 죽이지 않는다(요약은 로그에 있음).
    try:
        send_kakao(message, link)
        return
    except Exception as exc:
        print(f"카카오톡 전송 실패: {exc}")
    try:
        send_email(message, link)
    except Exception as exc:
        print(f"이메일 폴백도 실패: {exc}", file=sys.stderr)


if __name__ == "__main__":
    main()
