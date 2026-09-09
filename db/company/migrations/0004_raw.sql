-- 0004 raw 層: 1 ソース 2 表 (06 §11-1)
--
--   raw.<src>_contents     = 中身の重複排除 (content_hash が主キー。同じ内容は 1 回だけ持つ)
--   raw.<src>_observations = 毎回の観測 (この取得でこの business_key がどの内容だったか / 取れなかったか)
--   → A→B→A の 3 回目も、「変化なしの日」も、「取れなかった日」も残る。
--   🚨 1 表 unique(business_key, content_hash) だけでは表せない (PW-15 が誤検知する)。
--   content_hash は volatile 列 (取得時刻・run_id) を含めない (P-4)。
--
-- ops.ingest_runs = 取得 1 回の記録 (完走したか・何頁・件数・失敗範囲・版・checksum)。「前回比 80%」ではなく
--   complete=true の同一 scope だけを削除・停止判定の根拠にする (06 §11-2)。

create table ops.ingest_runs (
  ingest_run_id  text primary key,                      -- ISO ms + 乱数 (既存規約。expected-profit の run_id をそのまま使ってよい)
  source_system  text not null,                        -- 'amazon','rakuten','yahoo','aupay','qoo10','linegift','ne','logizard'
  entity         text not null,                        -- 'listing_report','catalog_items','items','item_list','inventory',...
  scope_key      text not null,                        -- 店舗・アカウント・marketplace (取得範囲の識別)
  host           text,
  started_at     timestamptz not null,
  finished_at    timestamptz,
  status         text not null check (status in ('running','success','failed','partial')),
  complete       boolean,                              -- 取得範囲を完走したか (status とは別)
  pages          integer,
  truncated      boolean,
  deadline_hit   boolean,
  rows_seen      integer,
  rows_inserted  integer,
  rows_skipped   integer,
  failed_ranges  jsonb,
  api_version    text,
  format_version text,
  source_tz      text not null default 'UTC',          -- D-13: 取込元の時刻の tz を必ず書く
  checksum       text,
  error          text,
  created_at     timestamptz not null default now()
);
create index ix_ingest_runs_source_time on ops.ingest_runs (source_system, entity, started_at desc);

-- ソースごとに同じ 2 表を作る (列を揃えるため DO で生成)
do $$
declare
  src text;
  srcs text[] := array[
    'ne_products', 'ne_set_products',
    'amazon_listing_report', 'amazon_listings_items', 'amazon_catalog_items',
    'rakuten_items', 'rakuten_inventory',
    'yahoo_item_list', 'yahoo_items',
    'aupay_items', 'qoo10_items', 'linegift_products',
    'logizard_products'
  ];
begin
  foreach src in array srcs loop
    execute format($f$
      create table raw.%1$I_contents (
        content_hash   text primary key,
        payload        jsonb not null,
        payload_bytes  integer,
        first_seen_at  timestamptz not null default now()
      )$f$, src);
    execute format($f$
      create table raw.%1$I_observations (
        observation_id bigint generated always as identity primary key,
        ingest_run_id  text not null references ops.ingest_runs,
        scope_key      text not null,
        business_key   text not null,
        content_hash   text references raw.%1$I_contents,
        fetch_status   text not null check (fetch_status in ('ok','not_found','error','skipped')),
        observed_at    timestamptz not null,
        created_at     timestamptz not null default now(),
        unique (ingest_run_id, scope_key, business_key),
        constraint ck_%1$s_obs_content check ((fetch_status = 'ok') = (content_hash is not null))
      )$f$, src);
    execute format('create index ix_%1$s_obs_key_time on raw.%1$I_observations (scope_key, business_key, observed_at desc)', src);
  end loop;
end
$$;
