-- =============================================================================
-- 랭커 스쿼드 수집(ranker_squad_job.py) 스키마 + 크롤러 롤 권한
-- Supabase → SQL Editor 에서 1회 실행. 모두 멱등(IF NOT EXISTS).
-- =============================================================================
-- ⚠️ 프로덕션 alembic_version 은 b3f1c2a4d5e6 에서 멈춰 있다. alembic upgrade 를
--    쓰지 말고 이 파일을 직접 실행한다. 백엔드 쪽 콜드스타트 DDL 은
--    database_bootstrap.py 가 BOOTSTRAP_VERSION 마커로 관리한다.

-- 1) 회차 로그 ---------------------------------------------------------------
create table if not exists player.ranker_squad_run (
    collected_on    date        primary key,
    ranker_cnt      integer     not null,
    matched_cnt     integer     not null,
    unresolved_cnt  integer     not null,
    created_at      timestamptz not null default now()
);

-- 2) 원본 스냅샷 — 랭커 1명 × 선발 11명 = 11행 ------------------------------
--    랭킹 표의 팀컬러와 일치한 대표팀 스쿼드만 들어온다(= 랭크에서 실제로 쓴 덱).
--    후보 7명은 저장하지 않는다.
create table if not exists player.ranker_squad_snapshot (
    collected_on    date        not null,
    nexon_sn        bigint      not null,
    rank            integer     not null,
    team_color_name text        not null,
    squad_slot      smallint    not null,   -- 대표팀 A/B/C = 1/2/3
    slot_index      smallint    not null,   -- 스쿼드 안 순번 0..10
    slot_role       text        not null,   -- 포메이션 슬롯 원문(ls, rcb, ...). 빈 값이 올 수 있어 키로 못 쓴다
    position_group  text        not null,   -- FW/CAM/WING/CM/CDM/CB/FB/GK
    spid            bigint      not null,
    build_up        smallint    not null,   -- 강화단계
    player_name     text,
    primary key (collected_on, nexon_sn, slot_index)
);

create index if not exists ix_ranker_squad_snapshot_tc
    on player.ranker_squad_snapshot (collected_on, team_color_name, position_group);

-- 3) 집계 — 팀컬러 × 포지션 × 선수 ------------------------------------------
--    team_color_id 는 이름으로 player.team_colors 에 매칭한 결과다. 넥슨
--    데이터센터의 팀컬러 번호는 우리 team_colors.id 와 체계가 달라서 번호로는
--    못 잇는다(2026-09 실측). 매칭 실패(신규/개명 팀컬러)면 NULL 로 둔다.
create table if not exists player.team_color_player_usage (
    collected_on        date     not null,
    team_color_name     text     not null,
    team_color_id       integer,
    position_group      text     not null,
    spid                bigint   not null,
    user_cnt            integer  not null,  -- 이 선수를 이 자리에 쓴 랭커 수
    position_user_cnt   integer  not null,  -- 분모: 그 팀컬러에서 이 자리를 채운 랭커 수
    avg_build_up        numeric(4, 2) not null,
    primary key (collected_on, team_color_name, position_group, spid)
);

create index if not exists ix_team_color_player_usage_lookup
    on player.team_color_player_usage (collected_on, team_color_id, position_group, user_cnt desc);

-- 4) characterId 캐시 -------------------------------------------------------
--    SquadGetUserInfo 는 characterId 가 있어야 답한다. 이 값은 계정마다 고정이라
--    한 번 받아두면 다음 회차엔 popup 요청(랭커당 1건, gzip 9KB)을 통째로 건너뛴다.
create table if not exists player.ranker_character_id (
    nexon_sn     bigint      primary key,
    character_id text        not null,
    updated_at   timestamptz not null default now()
);

-- 5) 크롤러 롤 권한 ---------------------------------------------------------
--    sql/crawler_role.sql 과 이 파일, 그리고 실제 DB 양쪽에 반영되어야 한다.
grant select, insert, update, delete on player.ranker_squad_snapshot   to fco_crawler;
grant select, insert, update, delete on player.team_color_player_usage to fco_crawler;
grant select, insert, update         on player.ranker_squad_run        to fco_crawler;
grant select, insert, update         on player.ranker_character_id     to fco_crawler;
grant select                         on player.team_colors             to fco_crawler;
