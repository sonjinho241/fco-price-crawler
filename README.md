# fco-price-crawler

FC Online 선수 **"최신 시세만"** 일일 크롤러. 공개 저장소 + GitHub Actions(무료)에서
하루 1회 새벽에 돌아가며, 넥슨에서 선수×강화단계당 *가장 최근 1줄*을 받아
DB의 `player.player_price_latest` 테이블에 upsert 한다. 이 표 덕분에 메인 앱이
`WHERE strong = ? AND price BETWEEN ? AND ?` 검색을 인덱스로 빠르게 태운다.

- **자기완결형**: `crawler_job.py` 하나로 끝. 메인(비공개) 앱 저장소를 체크아웃하지 않는다.
- **멱등**: 중간에 죽어도 다시 돌리면 이어서 채워진다.
- **비용 0원**: 공개 저장소는 Actions 분(分) 무제한.
- **하루 1회만**: 넥슨 시세 자체가 하루 1회 갱신이라 1일 1회가 의미 있는 최대치.

> ⚠️ 이 저장소에는 **크롤링 로직만** 공개된다(민감정보 아님). DB 접속문자열
> (`DATABASE_URL`)은 **절대 코드/파일에 두지 않고** GitHub Secret 으로만 주입한다.

---

## 구조

```
crawler_job.py                 # 자기완결형 크롤러 (이 파일 하나가 전부)
requirements.txt               # requests, SQLAlchemy, psycopg2-binary
.github/workflows/crawl.yml    # 매일 1회 cron + 수동 실행, 4-샤드 병렬
sql/crawler_role.sql           # 최소권한 전용 DB 롤 생성 SQL (Supabase에서 1회 실행)
.env.example                   # 로컬 테스트용 예시 (.env 는 커밋 금지)
```

---

## 셋업 (한 번만)

### 1) 최소권한 DB 롤 만들기 (권장, 보안)
Supabase → **SQL Editor** 에서 [`sql/crawler_role.sql`](sql/crawler_role.sql) 를 열고
비밀번호 자리를 강력한 값으로 바꿔 실행한다. 이 롤은 `player.players` 읽기 +
`player.player_price_latest` 읽기/쓰기만 가능하다(슈퍼유저 아님).

접속문자열 형태:
```
postgresql://fco_crawler:비밀번호@db.<프로젝트ref>.supabase.co:5432/postgres
```

### 2) GitHub Secret 등록
이 저장소 → **Settings → Secrets and variables → Actions → New repository secret**
- Name: `DATABASE_URL`
- Value: 위 1)의 접속문자열 (암호화되어 저장됨)

> 🔒 `DATABASE_URL` 을 코드, 커밋, 이슈, PR, 로그 어디에도 남기지 말 것.
> 외부 fork 의 PR 은 기본적으로 Secret 에 접근할 수 없다(이 워크플로에 `pull_request`
> 트리거를 **추가하지 말 것**).

### 3) 대상 테이블이 먼저 존재해야 함
크롤러가 쓰기 전에 메인 앱 쪽에서 `player.player_price_latest` 테이블이 생성돼 있어야 한다
(백엔드 재배포 시 `create_all` 또는 `alembic upgrade head`).

---

## 실행

### 수동 실행 (소량 테스트부터)
GitHub → **Actions → crawl-prices → Run workflow** 에서 입력값을 주고 실행:
- `limit` = `20`, `strongs` = `1` 로 먼저 동작 확인 (선수 20명 × 1강만).
- 잘 되면 `limit` = `0`(전체), `strongs` = `1-13` 로 전체 1회 실행 → 소요시간 측정.

### 자동 실행 (cron)
[`.github/workflows/crawl.yml`](.github/workflows/crawl.yml) 의 cron `0 20 * * *`
= 매일 **20:00 UTC = 한국시간 다음날 05:00**. 기본 브랜치에서만 동작한다.

### 로컬 실행
```bash
pip install -r requirements.txt
export DATABASE_URL='postgresql://fco_crawler:...@db.xxxxx.supabase.co:5432/postgres'
PRICE_LATEST_LIMIT=20 PRICE_LATEST_STRONGS=1 python crawler_job.py
```

---

## 샤딩 (6시간 작업 제한 회피)

전체(약 8.7만 선수 × 13강)를 한 job 으로 돌리면 GitHub 의 단일 job 6시간 제한에 걸릴 수
있다. 그래서 `matrix` 로 여러 샤드를 병렬 실행하고, 각 샤드는 `spids[SHARD_INDEX::SHARD_COUNT]`
만 담당한다. 처음엔 **보수적으로 4 샤드**로 시작한다.

**샤드 수를 바꾸려면** `crawl.yml` 의 두 곳을 **함께** 맞춰야 한다:
```yaml
matrix:
  shard: [0, 1, 2, 3]   # ← 목록 길이
...
SHARD_COUNT: "4"          # ← 이 숫자 = 위 목록 길이
```
예) 8 샤드로 늘리려면 `shard: [0,1,2,3,4,5,6,7]` + `SHARD_COUNT: "8"`.

> ⚠️ 넥슨 차단 위험: 동시 부하 ≈ **샤드 수 × WORKERS**. 처음엔 샤드 4 / `WORKERS` 10 /
> `REQUEST_DELAY` 0.05 로 1회 돌려 소요시간·차단 여부를 측정한 뒤 조절하라. 하루 1회만.

---

## 환경변수

| 변수 | 기본 | 설명 |
|---|---|---|
| `DATABASE_URL` | (필수) | Postgres 접속 문자열. **로그에 절대 출력 안 함.** |
| `SHARD_INDEX` / `SHARD_COUNT` | `0` / `1` | matrix 샤딩. 각 샤드가 `spids[INDEX::COUNT]` 담당 |
| `PRICE_LATEST_STRONGS` | `1-13` | 크롤할 강화단계. `1-13` / `1,5,8` / `1-5,8` 형식 |
| `PRICE_LATEST_WORKERS` | `10` | 동시 요청 스레드 수(샤드당) |
| `PRICE_LATEST_REQUEST_DELAY` | `0.05` | 각 요청 후 sleep 초(레이트리밋 완화) |
| `PRICE_LATEST_LIMIT` | `0` | 처리할 spid 수 상한(0=전체). **샤딩 전에** 적용, 테스트용 |
| `PRICE_LATEST_CHUNK_SIZE` | `4000` | 한 번에 처리하는 (spid,strong) 작업 수(메모리 상한) |
| `PRICE_LATEST_DB_BATCH` | `500` | DB 에 한 번에 upsert 하는 행 수 |
| `PRICE_LATEST_MAX_RETRIES` | `3` | 넥슨 호출 재시도 횟수 |

---

## 검증 순서
1. 수동 실행 + `limit=20`, `strongs=1` → 동작 확인.
2. 전체 1회 실행 → 소요시간 측정 → 샤드/워커 수 튜닝, 차단 없는지 확인.
3. DB 에 `player.player_price_latest` 행이 쌓였는지 확인 → 앱에서 강화단계+가격 필터 검색 결과 확인.
4. cron 으로 매일 자동 실행 확인.

> 표가 채워지기 전에는 가격필터 검색 결과 0건이 정상이다.
