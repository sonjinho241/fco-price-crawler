-- =============================================================================
-- 크롤러 전용 "최소권한" DB 롤 만들기  (Supabase → SQL Editor 에서 1회 실행)
-- =============================================================================
-- 이유: DATABASE_URL 은 공개 저장소의 GitHub Secret 에 들어간다(암호화되지만,
--       만에 하나 유출돼도 피해를 최소화하려면 슈퍼유저/postgres URL 이 아니라
--       딱 필요한 권한만 가진 전용 롤의 접속문자열을 써야 한다).
--
-- 이 롤이 할 수 있는 것: player.players 읽기 + player.player_price_latest 읽기/쓰기. 그게 전부.
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

-- (참고) 권한 회수가 필요하면:
--   revoke all on player.player_price_latest from fco_crawler;
--   revoke all on player.players              from fco_crawler;
--   drop role fco_crawler;
--
-- 접속문자열(직접 연결): postgresql://fco_crawler:비밀번호@db.<프로젝트ref>.supabase.co:5432/postgres
-- 이 문자열을 repo Settings → Secrets and variables → Actions → DATABASE_URL 로 저장.
