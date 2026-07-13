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

KAUTH_TOKEN_URL = "https://kauth.kakao.com/oauth/token"
KAKAO_MEMO_URL = "https://kapi.kakao.com/v2/api/talk/memo/default/send"


def make_engine():
    url = os.environ["DATABASE_URL"]
    url = url.replace("postgres://", "postgresql://", 1)
    if url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    return create_engine(url, pool_pre_ping=True)


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
    new_spid_count = 0

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
            new_spid_count += len(r["new_spids"] or [])

    expected = int(os.getenv("EXPECTED_SHARDS", "4"))
    return {
        "run_id": run_id,
        "new_players": new_players,
        "new_spid_count": new_spid_count,
        "changed": changed,
        "failures": failures,
        "new_traits": new_traits,
        "new_seasons": new_seasons,
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
