-- =============================================================================
-- 크롤러 전용 "최소권한" DB 롤 만들기  (Supabase → SQL Editor 에서 1회 실행)
-- =============================================================================
-- 이유: DATABASE_URL 은 공개 저장소의 GitHub Secret 에 들어간다(암호화되지만,
--       만에 하나 유출돼도 피해를 최소화하려면 슈퍼유저/postgres URL 이 아니라
--       딱 필요한 권한만 가진 전용 롤의 접속문자열을 써야 한다).
--
-- 이 롤이 할 수 있는 것:
--   시세 크롤러: player.players 읽기 + player.player_price_latest 읽기/쓰기
--   메타 크롤러: player 스키마의 선수/시즌/spid/레퍼런스 테이블 읽기/쓰기 (아래 4번)
--
-- ⚠️ 아래 비밀번호 자리는 실행할 때만 강력한 값으로 바꾸고, 이 파일에 평문으로
--    저장하거나 커밋하지 마세요. 실행 후 접속문자열은 GitHub Secret 에만 보관.

-- 1) 로그인 가능한 롤 생성
create role fco_crawler with login password 'CHANGE-ME-강력한-비밀번호';

-- 2) DB 접속 + 스키마 사용 권한
grant connect on database postgres to fco_crawler;   -- Supabase 기본 DB명 = postgres
grant usage   on schema   player   to fco_crawler;

-- 3) 딱 필요한 테이블 권한만 (player_price_latest 의 upsert = INSERT + UPDATE)
grant select               on player.players              to fco_crawler;
grant select, insert, update on player.player_price_latest to fco_crawler;

-- 4) 메타 크롤러(meta_crawler.py) 권한 — 목/금 메타 자동화(2026-07)에서 추가.
--    upsert(ON CONFLICT DO UPDATE)는 INSERT+UPDATE, RETURNING/WHERE 는 SELECT 가 필요.
--    nations/traits/team_colors 는 get-or-create 라 INSERT 까지만(UPDATE 불필요).
grant select, insert, update on player.players        to fco_crawler;
grant select, insert, update on player.spid           to fco_crawler;
grant select, insert, update on player.seasonid       to fco_crawler;
grant select, insert         on player.nations        to fco_crawler;
grant select, insert         on player.traits         to fco_crawler;
grant select, insert         on player.team_colors    to fco_crawler;
grant select, insert         on player.meta_crawl_log to fco_crawler;
-- id 컬럼 default nextval() 용
grant usage, select on all sequences in schema player to fco_crawler;

-- (참고) 권한 회수가 필요하면:
--   revoke all on player.player_price_latest from fco_crawler;
--   revoke all on player.players              from fco_crawler;
--   drop role fco_crawler;
--
-- 접속문자열(직접 연결): postgresql://fco_crawler:비밀번호@db.<프로젝트ref>.supabase.co:5432/postgres
-- 이 문자열을 repo Settings → Secrets and variables → Actions → DATABASE_URL 로 저장.

-- 5) 랭커 스쿼드 크롤러(ranker_squad_job.py) 권한 — 2026-09 추가.
--    테이블 정의와 동일한 grant 가 sql/ranker_squad.sql 에도 있다. 그 파일을
--    실행했다면 여기 것은 다시 돌려도 무해하다(grant 는 멱등).
grant select, insert, update, delete on player.ranker_squad_snapshot   to fco_crawler;
grant select, insert, update, delete on player.team_color_player_usage to fco_crawler;
grant select, insert, update         on player.ranker_squad_run        to fco_crawler;
grant select, insert, update         on player.ranker_character_id     to fco_crawler;
grant select, insert, update         on player.ranker_ouid             to fco_crawler;
grant select                         on player.team_colors             to fco_crawler;

-- 6) 클럽 경력 배치(career_job.py) 권한 — 2026-10 추가.
--    player_career 는 바뀐 선수만 upsert, player_career_clubs(소속 필터 파생)는 바뀐 선수만
--    DELETE 후 다시 INSERT 한다. clubs 는 클럽명→id 매칭용 읽기 전용.
--    로그는 위 4번의 meta_crawl_log 권한을 그대로 쓴다.
grant select, insert, update         on player.player_career       to fco_crawler;
grant select, insert, update, delete on player.player_career_clubs to fco_crawler;
grant select                         on player.clubs               to fco_crawler;

-- 7) 현질 도우미(iteminfo_job.py · midnight_price_job.py) 권한 — 2026-10 추가.
--    spend 스키마 테이블은 백엔드 부트스트랩(BOOTSTRAP_VERSION 24)이 만든다. 서버 배포 뒤 실행할 것.
--    확률표는 바뀐 표만 DELETE 후 다시 INSERT, 후보 풀은 크롤러가 초안을 쓰고 운영자가 확정한다.
grant usage on schema spend to fco_crawler;
grant select, insert, update, delete on spend.prob_table      to fco_crawler;
grant select, insert, update, delete on spend.prob_row        to fco_crawler;
grant select, insert, update, delete on spend.pack_pool       to fco_crawler;
grant select, insert, update, delete on spend.pack_pool_card  to fco_crawler;
grant select, insert, delete         on spend.price_snapshot  to fco_crawler;
grant select                         on spend.product         to fco_crawler;
-- 2026-10-19: 교환·절구 보상 규칙이 회차(spend.event) config 로 옮겨 갔다. 앱 서버 BOOTSTRAP 27 이 적용한다.
grant select                         on spend.event           to fco_crawler;
grant select                         on player.seasonid       to fco_crawler;
