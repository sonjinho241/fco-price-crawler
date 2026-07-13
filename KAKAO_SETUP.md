# 카카오톡 "나에게 보내기" 알림 셋업 (1회)

메타 크롤 워크플로(`meta-thursday` 신규 시즌 감지 / `meta-friday` 라이브 퍼포먼스 풀크롤)의
`notify` 잡이 결과 요약을 **내 카카오톡**으로 보낸다. 카카오는 webhook 이 없어
OAuth refresh token 을 1회 발급해 GitHub Secret 에 넣어야 한다.
아래는 한 번만 하면 되고, 그 뒤로는 매주 자동이다.

> 토큰/키는 절대 코드·커밋에 넣지 말 것. 전부 GitHub Secret 으로만.

---

## 1. 카카오 디벨로퍼스 앱 만들기
1. https://developers.kakao.com → 로그인 → **내 애플리케이션** → **애플리케이션 추가하기**.
2. 만든 앱 → **앱 키** 에서 **REST API 키** 를 복사해둔다. → 이게 `KAKAO_REST_KEY`.

## 2. 카카오 로그인 + 권한 설정
1. **제품 설정 → 카카오 로그인** → **활성화 ON**.
2. **Redirect URI** 등록: 아무 값이나 되지만 그대로 다시 써야 한다. 예) `https://localhost:3000/oauth`
3. **제품 설정 → 카카오 로그인 → 동의항목** → **카카오톡 메시지 전송(`talk_message`)** 을 사용 설정.

## 3. 인가 코드 받기 (브라우저)
아래 URL 의 `REST_KEY`, `REDIRECT` 를 본인 값으로 바꿔 브라우저에 붙여넣고 동의한다:

```
https://kauth.kakao.com/oauth/authorize?client_id=REST_KEY&redirect_uri=REDIRECT&response_type=code&scope=talk_message
```

동의하면 `REDIRECT?code=XXXXXXXX` 로 리다이렉트된다. 주소창의 **`code=` 뒤 값**을 복사한다.
(이 코드는 몇 분 안에 만료되니 바로 다음 단계로.)

## 4. refresh token 발급 (터미널)
`REST_KEY`, `REDIRECT`, `CODE` 를 채워 실행:

```bash
curl -s -X POST "https://kauth.kakao.com/oauth/token" \
  -d "grant_type=authorization_code" \
  -d "client_id=REST_KEY" \
  -d "redirect_uri=REDIRECT" \
  -d "code=CODE"
```

응답 JSON 의 **`refresh_token`** 값을 복사한다. → 이게 `KAKAO_REFRESH_TOKEN`.
(`access_token` 은 6시간이면 만료되므로 저장 안 한다. notify.py 가 매번 refresh_token 으로 새로 발급한다.)

## 5. GitHub Secret 등록
레포 **Settings → Secrets and variables → Actions → New repository secret** 에 추가:

| Secret 이름 | 값 |
|---|---|
| `KAKAO_REST_KEY` | 2단계 REST API 키 |
| `KAKAO_REFRESH_TOKEN` | 4단계 refresh_token |
| `DATABASE_URL` | (이미 있음 — 시세 크롤러와 공용) |

## 6. 동작 확인
레포 **Actions → meta-thursday → Run workflow** 로 수동 실행하되, **limit** 에 `30` 을 넣어
테스트한다(30명만 크롤). 끝나면 카톡으로 요약이 오는지 확인.

---

## 이메일 폴백 (선택)
카톡 전송이 실패하면 이메일로 보내고 싶을 때만. Gmail 이면 **앱 비밀번호**를 발급해서:

| Secret | 값 |
|---|---|
| `SMTP_HOST` | `smtp.gmail.com` |
| `SMTP_PORT` | `587` |
| `SMTP_USER` | 보내는 Gmail 주소 |
| `SMTP_PASS` | Gmail 앱 비밀번호 |
| `NOTIFY_EMAIL_TO` | 받을 주소 (sohnjinho241@gmail.com) |

## refresh token 만료
refresh_token 은 약 2개월 유효하고, notify.py 가 매주 쓰면서 만료가 임박하면 카카오가 새
토큰을 돌려준다. 그때 로그에 `⚠️ 새 refresh_token 반환` 이 찍히면 **5단계처럼 Secret 을 갱신**한다.
(자동 갱신은 repo secret 쓰기 권한이 필요해 의도적으로 생략했다.)
