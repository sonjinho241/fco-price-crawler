"""배치 실패 알림 — 실패한 워크플로에서 `if: failure()` 스텝으로 부른다.

notify.py 는 meta_crawl_log 를 run_id 로 집계하는 "요약" 전용이라, 집계할 로그가
없는 배치(팀컬러 이용률 등)의 실패에는 쓸 수 없다. 여기서는 전송 경로(카카오톡 →
이메일 폴백)만 재사용하고 본문은 워크플로가 넘긴 한 줄로 만든다.

환경변수
    NOTIFY_LABEL        알림에 찍을 배치 이름 (예: "팀컬러 이용률")
    GITHUB_RUN_ID       실행 로그 링크용 (Actions 가 자동 주입)
    KAKAO_REST_KEY / KAKAO_REFRESH_TOKEN    카카오톡 "나에게 보내기"
    SMTP_* / NOTIFY_EMAIL_TO                이메일 폴백(선택)

⚠️ 이 스크립트는 실패해도 워크플로를 더 망가뜨리지 않는다. 알림이 안 갔다고 해서
   이미 실패한 배치의 결론이 달라지지 않으므로, 전송 오류는 로그만 남기고 삼킨다.
"""

import os
import sys

from notify import send_email, send_kakao


def run_link() -> str:
    server = os.getenv("GITHUB_SERVER_URL", "https://github.com")
    repo = os.getenv("GITHUB_REPOSITORY", "")
    run_id = os.getenv("GITHUB_RUN_ID", "")
    if repo and run_id:
        return f"{server}/{repo}/actions/runs/{run_id}"
    return "https://github.com"


def main() -> None:
    label = os.getenv("NOTIFY_LABEL", "배치")
    message = (
        f"❌ {label} 배치가 실패했습니다.\n"
        "DB 는 건드리지 않았으니 앱에는 직전 성공분이 그대로 보입니다.\n"
        "실행 로그에서 원인을 확인해 주세요."
    )
    link = run_link()
    print(message)

    try:
        send_kakao(message, link)
        return
    except Exception as exc:  # 카톡 실패는 이메일로 물러선다.
        print(f"카카오톡 전송 실패: {exc}", file=sys.stderr)

    try:
        send_email(message, link)
    except Exception as exc:
        print(f"이메일 전송도 실패: {exc}", file=sys.stderr)


if __name__ == "__main__":
    main()
