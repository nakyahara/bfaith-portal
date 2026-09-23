-- 0024: 出品に当たらなかった注文明細 (unresolved_code) を、出品が増えた後に解き直す (2026-09-23)
--
-- なぜ: 注文明細の listing_id は受け口 (core.upsert_order) が取込のときに 1 回だけ解決する。後から core.listings に出品が増えても
--   (例: 自社発送 (FBM) の seller SKU = NE 商品コード の規則を初期ロードに足した #1409)、既に取り込んだ明細は unresolved_code のまま
--   = 売上日次で SKU に展開できず、見張り W6 の対象からも抜ける。
-- なにを: 会社 × モールの、現行 (removed_at is null) で listing も sku も無い明細について core.resolve_listing_id で解き直し、当たれば listing_id を入れて
--   unresolved_code を消す。当たった明細の注文の updated_at を進める = 翌朝の売上日次の作り直し (watermark より後に動いた注文の日) に乗る。
--   🚨 sku_id は解かない (mall を見ない NE コード一致は、FBA なのに NE コードと偶然同じ seller SKU を誤って結ぶ。sku は出品の構成 (listing_components) から売上日次が展開する)
-- 呼び出し = 初期ロード / 夜間ロード (apps/company-db/load/engine.mjs) が出品を入れた後。人が流すなら select * from core.reresolve_order_lines(1, 'amazon');
--
-- 🚨 注文の updated_at は trigger (touch_updated_at_unless_seq_only) が「内容が変わっていなければ元に戻す」ので、素の update では進まない。
--   保守経路として set_config('core.touch_force', 'on', true) (取引の中だけ) を見て進める形に trigger 関数を直す (0013 の関数の置き換え。ふだんの動きは変えない)
create or replace function core.touch_updated_at_unless_seq_only() returns trigger language plpgsql as $$
begin
  if coalesce(current_setting('core.touch_force', true), '') = 'on'
     or (to_jsonb(new) - 'received_batch_seq' - 'last_ingest_run_id' - 'updated_at') is distinct from (to_jsonb(old) - 'received_batch_seq' - 'last_ingest_run_id' - 'updated_at') then
    new.updated_at := now();
  else
    new.updated_at := old.updated_at;
  end if;
  return new;
end $$;

create or replace function core.reresolve_order_lines(p_company smallint, p_mall text)
returns table (candidates integer, resolved integer, orders_touched integer) language plpgsql as $$
declare
  v_candidates integer;
  v_resolved integer;
  v_orders integer;
begin
  create temp table _rr_cand on commit drop as
    select l.order_line_id, l.order_id, core.resolve_listing_id(p_company, o.mall, l.unresolved_code) as lid
      from core.order_lines l
      join core.orders o on o.order_id = l.order_id
     where o.company_id = p_company and o.mall = p_mall
       and l.removed_at is null and l.listing_id is null and l.sku_id is null and l.unresolved_code is not null;
  select count(*) into v_candidates from _rr_cand;
  with upd as (
    update core.order_lines l
       set listing_id = c.lid, unresolved_code = null
      from _rr_cand c
     where c.order_line_id = l.order_line_id and c.lid is not null
    returning l.order_id
  )
  select count(*) into v_resolved from upd;
  -- 当たった注文の updated_at を進める (trigger は内容が同じなら戻すので、この取引の中だけ touch_force を立てる)
  perform set_config('core.touch_force', 'on', true);
  with touched as (
    update core.orders o set updated_at = now()
     where o.order_id in (select distinct c.order_id from _rr_cand c where c.lid is not null)
    returning 1
  )
  select count(*) into v_orders from touched;
  perform set_config('core.touch_force', 'off', true);
  drop table _rr_cand;
  return query select v_candidates, v_resolved, v_orders;
end $$;
comment on function core.reresolve_order_lines(smallint, text) is '出品に当たらなかった注文明細を解き直す (出品が増えた後)。当たった注文の updated_at を進めて売上日次の作り直しに乗せる';
