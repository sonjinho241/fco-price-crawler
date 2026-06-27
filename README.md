# fco-price-crawler

FC Online 선수 **"최신 시세만"** 일일 크롤러. 공개 저장소 + GitHub Actions(무료)에서
하루 1회 새벽에 돌아가며, 넥슨에서 선수×강화단계당 *가장 최근 1줄*을 받아
DB의 `player.player_price_latest` 테이블에 upsert 한다. 이 표 덕분에 메인 앱이
`WHERE strong = ? AND price BETWEEN ? AND ?` 검색을 인덱스로 빠르게 태운다.

- **자기완결형**: `crawler_job.py` 하나로 끝. 메인(비공개) 앱 저장소를 체크아웃하지 않는다.
- **멱등**: 중간에 죽어도 다시 돌리면 이어서 채워진다.
- **비용 0원**: 공개 저장소는 Actions 분(分) 무제한.
- **하루 6번 샤딩**: 넥슨 시세는 하루 1회 갱신이라 신선도는 1일 1바퀴면 충분하지만,
  Supabase Disk IO 예산을 아끼려고 부하를 하루 6토막(4시간 간격)으로 펴서 각 토막이
  전체 선수의 1/6만 담당한다. → 24시간에 한 바퀴.

> ⚠️ 이 저장소에는 **크롤링 로직만** 공개된다(민감정보 아님). DB 접속문자열
> (`DATABASE_URL`)은 **절대 코드/파일에 두지 않고** GitHub Secret 으로만 주입한다.

---

## 🤖 운영 요약 — 시세 배치가 "어디서·언제·어떻게" 도는가 (사람/AI 필독)

> 이 표가 메인 앱(`fco_manager`)에서 "시세 배치 어디서 도냐"로 헤매지 않게 하는 단일 진실원본이다.

- **어디서**: **이 저장소(`sonjinho241/fco-price-crawler`)의 GitHub Actions.** 메인 앱 Cloud Run 도,
  Cloud Scheduler 도, 로컬 PC 도 **아니다.** 메인 레포 `prices/latest_job.py`·`prices/crud.py` 는
  같은 로직의 **참고용 복사본일 뿐, 매일 실제로 DB 에 쓰는 건 여기다.**
- **언제**: `.github/workflows/crawl.yml` 의 cron **6개**(UTC 20·0·4·8·12·16시 = KST 05·09·13·17·21·01시).
  각 cron 이 샤드 인덱스 0~5 중 하나를 맡아 전체 spid 의 1/6 을 크롤한다.
- **무엇을**: 넥슨에서 선수×강화단계당 최신 1줄을 받아 `player.player_price_latest` 에 upsert.
  앱의 "강화단계+시세범위" **검색 필터 전용** 표다. (앱의 *시세 상세 그래프*는 이 표가 아니라
  메인 앱 `prices/router.py` 의 넥슨 **실시간 프록시**+12h 캐시에서 온다 — 별개 경로.)
- **Disk IO 보호 2종**: ① 하루 6토막 샤딩으로 **순간 폭주(burst) 제거**, ② `bulk_upsert_latest` 의
  **no-op skip**(`where=price.is_distinct_from(excluded.price)`)으로 **안 바뀐 행은 안 씀**
  → dead tuple/WAL 누적을 줄여 예산 소진을 막는다. (②가 없으면 매일 ~114만 행을 통째로
  다시 써서 예산을 갉아먹는다 — 2026-06 Supabase 경고의 원인이었다.)
- **변경 시 주의**: cron 개수를 바꾸면 `crawl.yml` 의 cron 목록·`Resolve shard` case 매핑·`SHARD_COUNT`
  를 **셋 다 함께** 맞춰야 한다.

---

## 구조

```
crawler_job.py                 # 자기완결형 크롤러 (이 파일 하나가 전부)
requirements.txt               # requests, SQLAlchemy, psycopg2-binary
.github/workflows/crawl.yml    # 하루 6회 cron(4시간 간격 샤딩) + 수동 실행
sql/crawler_role.sql           # 최소권한 전용 DB 롤 생성 SQL (Supabase에서 1회 실행)
.env.example                   # 로컬 테스트용 예시 (.env 는 커밋 금지)
```

---

## 셋업 (한 번만)

### 1) 최소권한 DB 롤 만들기 (권장, 보안)
Supabase → **SQL Editor** 에서 [`sql/crawler_role.sql`](sql/crawler_role.sql) 를 열고
비밀번호 자리를 강력한 값으로 바꿔 실행한다. 이 롤은 `player.players` 읽기 +
`player.player_price_latest` 읽기/쓰기만 가능하다(슈퍼유저 아님).

접속문자열 형태 ⚠️ **반드시 "Session pooler"(IPv4) 주소를 쓸 것** — GitHub Actions 는
IPv6 를 지원하지 않는데, Supabase 의 "직접 연결"(`db.<ref>.supabase.co`)은 IPv6 전용이라
`Network is unreachable` 로 실패한다. Supabase → **Connect → Session pooler** 탭의 주소를 쓰고,
유저명은 `<role>.<프로젝트ref>` 형식(점+ref)이라는 점에 주의:
```
postgresql://fco_crawler.<프로젝트ref>:비밀번호@aws-1-<region>.pooler.supabase.com:5432/postgres
```
(직접 연결 `postgresql://fco_crawler:비밀번호@db.<ref>.supabase.co:5432/postgres` 는 로컬 테스트용으로만)

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
[`.github/workflows/crawl.yml`](.github/workflows/crawl.yml) 에 cron 이 **6개**(UTC
20·0·4·8·12·16시 = KST 05·09·13·17·21·01시) 있고, 각 cron 이 샤드 0~5 중 하나를 맡아
전체 spid 의 1/6 만 크롤한다 → 24시간에 한 바퀴. 기본 브랜치에서만 동작한다.
(부하를 하루 종일 펴서 Supabase Disk IO 의 순간 폭주를 막기 위한 구조.)

### 로컬 실행
```bash
pip install -r requirements.txt
export DATABASE_URL='postgresql://fco_crawler:...@db.xxxxx.supabase.co:5432/postgres'
PRICE_LATEST_LIMIT=20 PRICE_LATEST_STRONGS=1 python crawler_job.py
```

---

## 샤딩 (시간 분산 + Disk IO 보호)

전체(약 8.7만 선수 × 13강)를 한 번에 몰아 돌리면 ① GitHub 단일 job 6시간 제한에 걸릴 수 있고
② Supabase Disk IO 예산을 순간에 몰아 태운다(과거 새벽 한 창에 4샤드 동시 실행 → 2h 폭주로
크레딧 소진). 그래서 지금은 **시간(matrix 병렬) 대신 하루를 6토막으로 펴는 방식**을 쓴다.
각 cron 슬롯이 샤드 인덱스 1개를 맡아 `spids[SHARD_INDEX::SHARD_COUNT]`(전체의 1/6)만 담당하고,
4시간 간격이라 동시 커넥션이 1로 깔려 순간 IO 가 baseline 밑에 머문다.

**슬롯(샤드) 수를 바꾸려면** [`crawl.yml`](.github/workflows/crawl.yml) 의 **세 곳을 함께** 맞춰야 한다:
```yaml
on:
  schedule:
    - cron: "0 20 * * *"   # ← cron 1줄 = 샤드 1개 (줄 수 = 샤드 수)
    ...
# Resolve shard 스텝의 case 매핑 ("0 20 * * *") idx=0 ...  ← cron↔인덱스 대응
count=6                     # ← 이 숫자 = 위 cron 줄 수
```

> ⚠️ 넥슨 차단 위험: 한 슬롯의 동시 부하 ≈ **WORKERS**(샤드당). `WORKERS` 10 / `REQUEST_DELAY`
> 0.05 가 기본. 한 슬롯은 전체의 1/6 분량이라 단일 job 6시간 제한에 한참 못 미친다.

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
