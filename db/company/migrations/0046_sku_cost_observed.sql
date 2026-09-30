-- 0046 観測の原価 core.sku_cost_observed (D7b-2。2026-09-30)
--
-- 設計の正本 = AI_reference『システム設計/CompanyDB構想/13_Amazon利益のmart_設計_20260930.md』§3.3・§3.4・D-57。
--   core.sku_costs (夜間ロードが書く原価) は有効期間の始まりが全部 2026-09-10 以降 = それより前の Amazon の利益に原価が付かない。
--   → miniPC の SQLite の原価の履歴 m_products_history (2026-05-05 の最初の写しから・日次の処理が変化に気づいた時刻) から、SKU ごとの原価の期間を作って別の表に持つ。
--   🚨 core.sku_costs とは混ぜない (マスタ正本切替 (10) の持ち主・監査・版の外 = 観測した値の写し)。
--
--   core.sku_cost_observed_loads = 受領の見出し (会社 × 送り元 × 世代 = 1 行。追記だけ = 古い世代の見出しも監査の履歴として残す。今の世代 = 最大の generation)。
--                                  manifest = checksum・行の数・結びつかない商品コードの数・曖昧な商品コードの数 (送り手が数えた値)
--   core.sku_cost_observed       = SKU × 期間 [valid_from, valid_to] (両端を含む・valid_to null = 今も) = 1 行。行は今の世代の見出しのものだけ
--   mart.v_sku_cost_observed_effective = 読む口。**SKU ごとに core.sku_costs の最初の valid_from より前だけ** に切った観測の原価 (§3.3「SKU ごとの境目」)
--
-- 決め (設計 §3.4):
--   ・世代 = 送り手 (miniPC) の台帳の連番。受け口は 古い世代 → stale (書かない) / 同じ世代で manifest が全部同じ → same / 違えば 409 /
--     新しい世代 → その会社 × 送り元の行を全部消して入れ替える (見出しの追加・行の削除・挿入を 1 取引で。apps/company-db/ingest/sku-cost-observed.mjs)
--   ・行の値 = 夜間ロードと同じ costForLoad (整数円に丸め・数でない / 負は原価不明 = 行を作らない)・状態は COMPLETE / OVERRIDDEN だけ (PARTIAL / MISSING / 削除は原価不明の期間 = 行が無い)
--   ・backfill_method: observed_daily_diff = 履歴の変化 (changed_at の JST の日の翌日から。最初の写しの日だけはその日から) /
--                      estimated_before_first_snapshot = 最初の写し (5/5) より前を同じ値で推定 (2026-01-01 から写しの日の前日まで。必ず終わりがある)
--   ・同じ SKU の期間は重ならない (受け口が確かめる)。商品コード → SKU は core.norm_code で (正規化で同じ SKU になる履歴のコードが 2 つ以上 = 送り手が入れない)
--   ・🚨 読むときは mart.v_sku_cost_observed_effective (sku_costs が始まった日から先は sku_costs が正。境目を送り手で切らない = sku_costs が後から始まる SKU でも読むときに正しく切れる)
-- 🚨 個人情報なし (商品コード・原価・日付だけ)。

create table core.sku_cost_observed_loads (
  observed_load_id      bigint generated always as identity primary key,
  company_id            smallint not null references core.companies,
  source                text not null check (source in ('warehouse_sqlite')),
  generation            bigint not null check (generation > 0),
  checksum              text not null check (checksum ~ '^[0-9a-f]{64}$'),
  row_count             integer not null check (row_count >= 0),
  unresolved_code_count integer not null check (unresolved_code_count >= 0),
  ambiguous_code_count  integer not null check (ambiguous_code_count >= 0),
  ingest_run_id         text not null,
  sent_at               timestamptz not null default now(),
  unique (company_id, source, generation),
  unique (observed_load_id, company_id, generation)   -- 行の複合 FK の参照先 (行の会社・世代 = 見出しの会社・世代)
);
comment on table core.sku_cost_observed_loads is '観測の原価の受領の見出し (0046)。追記だけ (古い世代の見出しは監査の履歴として残す)。会社 × 送り元 × 世代 = 1 行。今の世代 = 最大の generation。manifest = checksum・row_count・unresolved_code_count・ambiguous_code_count (送り手が数えた値)';
select core.make_append_only('core', 'sku_cost_observed_loads');   -- 見出しは追記だけ (UPDATE / DELETE / TRUNCATE を拒む。R8 M4)

create table core.sku_cost_observed (
  sku_cost_observed_id  bigint generated always as identity primary key,   -- 選び方の最後の順 (§3.3)
  observed_load_id      bigint not null,
  company_id            smallint not null,
  generation            bigint not null,
  sku_id                bigint not null,
  product_code          text not null check (product_code <> ''),          -- 履歴の商品コードの原文 (checksum の鍵。SKU は core.norm_code で結ぶ)
  cost_jpy              bigint not null check (cost_jpy >= 0),
  cost_status           text not null check (cost_status in ('COMPLETE','OVERRIDDEN')),
  valid_from            date not null,
  valid_to              date,                                               -- 両端を含む。null = 今も
  backfill_method       text not null check (backfill_method in ('observed_daily_diff','estimated_before_first_snapshot')),
  first_observed_at     timestamptz not null,                               -- その値を最初に観測した履歴の行の changed_at (UTC)
  source_history_id     bigint not null check (source_history_id > 0),     -- m_products_history.history_id
  constraint ck_sku_cost_observed_period check (valid_to is null or valid_to >= valid_from),
  constraint ck_sku_cost_observed_estimated_closed check (backfill_method <> 'estimated_before_first_snapshot' or valid_to is not null),
  foreign key (observed_load_id, company_id, generation) references core.sku_cost_observed_loads (observed_load_id, company_id, generation),
  foreign key (company_id, sku_id) references core.skus (company_id, sku_id),
  unique (observed_load_id, sku_id, valid_from)
);
create index ix_sku_cost_observed_sku on core.sku_cost_observed (company_id, sku_id, valid_from);
-- 行は UPDATE させない (入れ替えは受け口の DELETE + INSERT だけ = 行の数が同じまま中身だけ変わって「変わりなし」に見えるのを防ぐ。Codex #1549 R3 M3)
create function core.sku_cost_observed_no_update() returns trigger language plpgsql as $$
begin
  raise exception 'core.sku_cost_observed は UPDATE できない (入れ替えは新しい世代の DELETE + INSERT)';
end
$$;
create trigger trg_sku_cost_observed_no_update before update on core.sku_cost_observed for each statement execute function core.sku_cost_observed_no_update();
comment on table core.sku_cost_observed is '観測の原価 (0046・D-57)。SQLite の m_products_history から作った SKU × 期間 [valid_from, valid_to] (両端を含む)。core.sku_costs とは別 = 読むときは mart.v_sku_cost_observed_effective (sku_costs の最初の日より前だけ)';

-- 読む口: SKU ごとに core.sku_costs の最初の valid_from より前だけ (その日から先は sku_costs が正。状態によらず最初の行 = PARTIAL / MISSING の行でも sku_costs の側で「原価不明」)。
--   valid_to を境目の前日で切る。境目より後に始まる行は出さない。sku_costs が 1 行も無い SKU は観測のまま
create or replace view mart.v_sku_cost_observed_effective as
  select o.sku_cost_observed_id, o.company_id, o.sku_id, o.product_code, o.cost_jpy, o.cost_status, o.valid_from,
         case when f.first_from is null then o.valid_to
              when o.valid_to is null or o.valid_to >= f.first_from then f.first_from - 1
              else o.valid_to end as valid_to,
         o.valid_to as observed_valid_to,
         f.first_from as sku_costs_first_from,
         case when o.backfill_method = 'estimated_before_first_snapshot' then 'estimated' else 'observed' end as cost_basis,
         o.backfill_method, o.first_observed_at, o.source_history_id, o.observed_load_id, o.generation
    from core.sku_cost_observed o
    left join (select company_id, sku_id, min(valid_from) as first_from from core.sku_costs group by company_id, sku_id) f
      on f.company_id = o.company_id and f.sku_id = o.sku_id
   where f.first_from is null or o.valid_from < f.first_from;
comment on view mart.v_sku_cost_observed_effective is '観測の原価の読む口 (0046)。SKU ごとに core.sku_costs の最初の valid_from より前だけ (valid_to を前日で切る)。cost_basis = observed / estimated (§3.3)';

-- ─── 権限 (0043 と同じ形: ロールがあれば付ける) ───
do $$ begin
  if exists (select 1 from pg_roles where rolname = 'watcher') then
    execute 'grant select on core.sku_cost_observed_loads, core.sku_cost_observed, mart.v_sku_cost_observed_effective to watcher';
  end if;
end $$;
