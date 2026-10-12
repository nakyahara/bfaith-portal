-- 0076 (仮の番号・マージのときに付け替える): Amazon の出品一覧の今の姿 (出品レポート GET_MERCHANT_LISTINGS_ALL_DATA) と、「まだつないでいない Amazon の SKU」の隠す印 (2026-10-12 中原さん)
-- 🚨 前提 = 0054 (core.amazon_jp_shop_code・core.amazon_sku_maps)・0013 (core.orders / order_lines)・0004 (ops.ingest_runs)・0001 (core.norm_code・core.make_append_only)
--
-- なぜ: Amazon の SKU の対応を作る画面で「Amazon の登録の商品名」を見たい。FBA に登録したのに社内の商品コードとつないでいない SKU を一覧で拾いたい
--   (今の「未登録」= 直近 7 日に売れた SKU だけ = 売れていない SKU は出ない)。
--   出品レポートは想定利益の夜間 (miniPC 23:30) が毎晩全件取って TSV のまま残している (DATA_DIR/mall-items-history/amazon/)。Company DB には入っていない
--   (夜間ロードの出品は SKU マスタ・fba.db・手数料の写しから = 商品名・出品日・停止中の SKU が無い)。
--   → 毎朝の daily-sync の 1 工程 (apps/company-db/push/amazon-listings.mjs) が前の夜の TSV を読み、1 要求で Render の受け口
--     (POST /apps/company-db/sync/amazon-listings・apps/company-db/ingest/amazon-listings.mjs) へ送る。SP-API は呼ばない (2 回取らない)。
-- なにを:
--   1. ops.amazon_listing_report_loads = 受け取ったレポートの見出し (取得の時刻 = 世代・想定利益の run_id・指紋・行の数・送り手が外した行の数)。
--      今の姿 = 店ごとに世代のいちばん大きい行。中身が同じ新しい取得 = 見出しの世代・時刻だけ進める (行は触らない)
--   2. ops.amazon_listing_report = 今の姿の行 (seller SKU ごと)。新しい中身が来たら店ごとに丸ごと入れ替える (1 取引)。履歴は持たない (TSV が miniPC に毎晩残る)
--   3. ops.amazon_sku_hidden (今「隠す」の SKU) + ops.amazon_sku_hidden_events (隠す・戻す の記録・追記だけ) と、書く関数 ops.set_amazon_sku_hidden (security definer)。
--      隠すのは「まだつないでいない SKU」の一覧の表示だけ。Amazon にも対応 (core.amazon_sku_maps)・出品 (core.listings) にも触らない。鍵 = core.norm_code(seller SKU)
--   4. ops.amazon_sku_last_sold(norms, since) = seller SKU (正規化) ごとの最後に売れた日・個数 (Amazon 日本の注文・取り消しを除く)。画面のロールに注文の表は読ませない (0054 と同じ)
-- 🚨 この migration は今のデータを何も変えない (新しい表は空・新しい関数だけ)。
-- 🚨 security definer の関数 = 一時の表を使わない・search_path = pg_catalog, pg_temp (名前は全部 schema つき)・public の実行権を外す (0034 の約束)。
--    画面のロール (master_edit) への権限は scripts/company-db/create-master-edit-roles.mjs (表の select と関数の実行・表が無い DB では付けない)

-- 1. 受け取ったレポートの見出し
create table ops.amazon_listing_report_loads (
  load_id        bigint generated always as identity primary key,
  company_id     smallint not null default 1 references core.companies,
  shop_code      text not null,
  generation     bigint not null check (generation > 0),             -- 取得の時刻 (ms・想定利益の夜間がレポートを取り終えた時刻)。新しいほど大きい
  report_run_id  text not null check (length(report_run_id) between 1 and 100),   -- 想定利益の price_fetch_run.run_id (= TSV のファイル名にも入る)
  snapshot_at    timestamptz not null,
  checksum       text not null check (checksum ~ '^[0-9a-f]{64}$'),
  row_count      integer not null check (row_count >= 0),
  skipped_count  integer not null default 0 check (skipped_count >= 0),   -- 送り手が外した行 (SKU が空・同じ SKU が 2 行・形が読めない)
  ingest_run_id  text not null references ops.ingest_runs,
  received_at    timestamptz not null default now(),
  refreshed_at   timestamptz,                                         -- 中身が同じ新しい取得で世代だけ進めた時刻
  unique (company_id, shop_code, generation)
);
create index ix_amazon_listing_report_loads_cur on ops.amazon_listing_report_loads (company_id, shop_code, generation desc);
comment on table ops.amazon_listing_report_loads is 'Amazon の出品レポート (GET_MERCHANT_LISTINGS_ALL_DATA) の受け取りの見出し (0076)。今の姿 = 店ごとに generation が最大の行。書くのは受け口 apps/company-db/ingest/amazon-listings.mjs だけ';

-- 2. 今の姿の行
create table ops.amazon_listing_report (
  company_id     smallint not null default 1 references core.companies,
  shop_code      text not null,
  seller_sku     text not null check (length(seller_sku) between 1 and 255),
  sku_norm       text not null generated always as (core.norm_code(seller_sku)) stored,
  load_id        bigint not null references ops.amazon_listing_report_loads,
  item_name      text check (item_name is null or length(item_name) <= 2000),
  asin           text check (asin is null or asin ~ '^[A-Z0-9]{10}$'),
  open_date      date,                                                -- 出品日 (レポートの表記の日付の部分。読めなければ null で open_date_raw に原文)
  open_date_raw  text check (open_date_raw is null or length(open_date_raw) <= 60),
  fulfillment    text check (fulfillment in ('FBA', 'FBM')),          -- null = 列が無い・知らない値 (FBM に倒さない)
  channel_raw    text check (channel_raw is null or length(channel_raw) <= 60),
  status         text not null check (status in ('active', 'inactive', 'incomplete', 'unknown')),
  status_raw     text check (status_raw is null or length(status_raw) <= 60),
  quantity       integer check (quantity >= 0),                       -- FBA は空 (数量の列で在庫は判定できない)
  price_jpy      numeric(14, 2) check (price_jpy >= 0),               -- 空 = null (バリエーションの親・停止中に多い)
  primary key (company_id, shop_code, seller_sku)
);
create index ix_amazon_listing_report_norm on ops.amazon_listing_report (company_id, shop_code, sku_norm);
create index ix_amazon_listing_report_load on ops.amazon_listing_report (load_id);
comment on table ops.amazon_listing_report is 'Amazon の出品レポートの今の姿 (0076)。1 行 = 1 つの seller SKU (原文)。新しい中身が来たら店ごとに丸ごと入れ替える。書くのは受け口だけ';

-- 3. 隠す印 (「まだつないでいない Amazon の SKU」の一覧の表示だけ)
create table ops.amazon_sku_hidden (
  company_id     smallint not null default 1 references core.companies,
  shop_code      text not null,
  sku_norm       text not null check (sku_norm <> ''),
  seller_sku     text not null,                                       -- 隠したときのレポートの原文 (見せる用)
  reason         text not null check (reason in ('variation_parent', 'unused', 'other')),
  note           text check (note is null or length(note) between 1 and 200),
  hidden_by      text not null,
  hidden_at      timestamptz not null default now(),
  primary key (company_id, shop_code, sku_norm),
  constraint ck_amazon_sku_hidden_other_note check (reason <> 'other' or note is not null)
);
comment on table ops.amazon_sku_hidden is '「まだつないでいない Amazon の SKU」の一覧で隠す SKU (0076)。表示だけ = Amazon・対応・出品には触らない。書くのは ops.set_amazon_sku_hidden だけ (戻す = 行を消す・記録は ops.amazon_sku_hidden_events)';
create table ops.amazon_sku_hidden_events (
  event_id       bigint generated always as identity primary key,
  company_id     smallint not null,
  shop_code      text not null,
  sku_norm       text not null,
  seller_sku     text not null,
  action         text not null check (action in ('hide', 'show')),
  reason         text,
  note           text,
  actor          text not null,
  at             timestamptz not null default now()
);
create index ix_amazon_sku_hidden_events_key on ops.amazon_sku_hidden_events (company_id, shop_code, sku_norm, event_id desc);
select core.make_append_only('ops', 'amazon_sku_hidden_events');
comment on table ops.amazon_sku_hidden_events is '隠す・戻す の記録 (0076・追記だけ)。誰が・いつ・なぜ';

-- 隠す / 戻す (1 件ずつ)。戻り値 = { status: 'hidden' | 'shown' | 'same', seller_sku, sku_norm, reason, note, by, at }
--   形が外れる = 22023 (画面は 400)・隠す SKU が今の出品一覧に無い = P0002 (画面は 404)。同じ状態への操作 = same (何も書かない)
create function ops.set_amazon_sku_hidden(p_actor text, p_seller_sku text, p_hide boolean, p_reason text default null, p_note text default null)
  returns jsonb language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_shop text := core.amazon_jp_shop_code();
  v_actor text := pg_catalog.lower(pg_catalog.btrim(coalesce(p_actor, '')));
  v_note text := nullif(pg_catalog.btrim(coalesce(p_note, '')), '');
  v_norm text;
  v_cur record;
  v_now timestamptz := pg_catalog.now();
begin
  if v_actor = '' or pg_catalog.length(v_actor) > 200 then raise exception 'invalid_value: 操作した人が無い' using errcode = '22023'; end if;
  if p_seller_sku is null or pg_catalog.btrim(p_seller_sku) = '' or pg_catalog.length(p_seller_sku) > 255 or p_seller_sku ~ '[\u0001-\u001f\u007f]' then
    raise exception 'invalid_value: seller SKU の形が不正 (空・255 文字超・制御文字)' using errcode = '22023';
  end if;
  if p_hide is null then raise exception 'invalid_value: 隠すか戻すかが無い' using errcode = '22023'; end if;
  v_norm := core.norm_code(p_seller_sku);
  if v_norm is null or v_norm = '' then raise exception 'invalid_value: seller SKU が空' using errcode = '22023'; end if;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('company-db:amazon-sku-hidden:' || v_shop || ':' || v_norm));
  select h.seller_sku, h.reason, h.note, h.hidden_by, h.hidden_at into v_cur from ops.amazon_sku_hidden h
   where h.company_id = 1 and h.shop_code = v_shop and h.sku_norm = v_norm for update;
  if p_hide then
    if p_reason is null or p_reason not in ('variation_parent', 'unused', 'other') then raise exception 'invalid_value: 理由は variation_parent / unused / other' using errcode = '22023'; end if;
    if v_note is not null and pg_catalog.length(v_note) > 200 then raise exception 'invalid_value: 理由の文は 200 文字まで' using errcode = '22023'; end if;
    if p_reason = 'other' and v_note is null then raise exception 'invalid_value: 「その他」は理由の文が要る' using errcode = '22023'; end if;
    if v_cur.seller_sku is not null then
      return pg_catalog.jsonb_build_object('status', 'same', 'seller_sku', v_cur.seller_sku, 'sku_norm', v_norm, 'reason', v_cur.reason, 'note', v_cur.note, 'by', v_cur.hidden_by, 'at', v_cur.hidden_at);
    end if;
    -- 隠すのは今の出品一覧にある SKU だけ (打ち間違いの印を残さない)。原文は出品一覧の行のもの (正規化で同じ行が 2 つなら小さい方)
    select r.seller_sku into v_cur from ops.amazon_listing_report r
     where r.company_id = 1 and r.shop_code = v_shop and r.sku_norm = v_norm order by r.seller_sku limit 1;
    if v_cur.seller_sku is null then raise exception 'not_found: Amazon の出品一覧 (今の姿) に無い seller SKU' using errcode = 'P0002'; end if;
    insert into ops.amazon_sku_hidden (company_id, shop_code, sku_norm, seller_sku, reason, note, hidden_by, hidden_at)
      values (1, v_shop, v_norm, v_cur.seller_sku, p_reason, v_note, v_actor, v_now);
    insert into ops.amazon_sku_hidden_events (company_id, shop_code, sku_norm, seller_sku, action, reason, note, actor, at)
      values (1, v_shop, v_norm, v_cur.seller_sku, 'hide', p_reason, v_note, v_actor, v_now);
    return pg_catalog.jsonb_build_object('status', 'hidden', 'seller_sku', v_cur.seller_sku, 'sku_norm', v_norm, 'reason', p_reason, 'note', v_note, 'by', v_actor, 'at', v_now);
  end if;
  if v_cur.seller_sku is null then
    return pg_catalog.jsonb_build_object('status', 'same', 'seller_sku', p_seller_sku, 'sku_norm', v_norm, 'reason', null, 'note', null, 'by', null, 'at', null);
  end if;
  delete from ops.amazon_sku_hidden where company_id = 1 and shop_code = v_shop and sku_norm = v_norm;
  insert into ops.amazon_sku_hidden_events (company_id, shop_code, sku_norm, seller_sku, action, reason, note, actor, at)
    values (1, v_shop, v_norm, v_cur.seller_sku, 'show', v_cur.reason, v_note, v_actor, v_now);
  return pg_catalog.jsonb_build_object('status', 'shown', 'seller_sku', v_cur.seller_sku, 'sku_norm', v_norm, 'reason', v_cur.reason, 'note', v_note, 'by', v_actor, 'at', v_now);
end
$$;
revoke all on function ops.set_amazon_sku_hidden(text, text, boolean, text, text) from public;

-- 4. 最後に売れた日 (seller SKU の正規化ごと)。Amazon (日本) の注文・取り消した注文と全部取り消した明細は数えない (0054 の未登録と同じ数え方)。
--    注文の明細の出品 (listing_norm) か、出品に当たらなかった元のコード (unresolved_code の正規化) で結ぶ。p_norms は 20,000 個まで
create function ops.amazon_sku_last_sold(p_norms text[], p_since date)
  returns table (sku_norm text, last_date date, units bigint)
  language plpgsql stable security definer set search_path = pg_catalog, pg_temp as $$
begin
  if p_norms is null or p_since is null then raise exception 'invalid_value: norms と since が要る' using errcode = '22023'; end if;
  if pg_catalog.cardinality(p_norms) > 20000 then raise exception 'invalid_value: norms は 20,000 個まで' using errcode = '22023'; end if;
  return query
  select x.n, pg_catalog.max(x.d), pg_catalog.sum(x.u)::bigint
    from (select coalesce(l.listing_norm, core.norm_code(ol.unresolved_code)) as n, o.order_date_jst as d, (ol.qty - ol.cancelled_qty) as u
            from core.orders o
            join core.order_lines ol on ol.order_id = o.order_id and ol.removed_at is null
            left join core.listings l on l.listing_id = ol.listing_id
           where o.company_id = 1 and o.mall = 'amazon' and o.scope_key = 'jp' and o.order_date_jst >= p_since
             and not o.is_cancelled and ol.qty > ol.cancelled_qty
             and (l.listing_id is null or (l.mall = 'amazon' and l.shop_code = core.amazon_jp_shop_code()))) as x
   where x.n = any(p_norms)
   group by x.n;
end
$$;
revoke all on function ops.amazon_sku_last_sold(text[], date) from public;
