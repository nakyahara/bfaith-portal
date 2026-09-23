-- 0024: 出品に当たらなかった注文明細 (unresolved_code) を、出品が増えた後に解き直す (2026-09-23)
--
-- なぜ: 注文明細の listing_id は受け口 (core.upsert_order) が取込のときに 1 回だけ解決する。後から core.listings に出品が増えても
--   (例: 自社発送 (FBM) の seller SKU = NE 商品コード の規則を初期ロードに足した #1410)、既に取り込んだ明細は unresolved_code のまま
--   = 売上日次で SKU に展開できず、見張り W6 の対象からも抜ける。
-- なにを: 会社 × モールの、現行 (removed_at is null) で listing も sku も無い明細 (注文日が p_since 以降。null = 全部) について core.resolve_listing_id で解き直し、
--   当たれば listing_id を入れて unresolved_code を消す。当たった明細の注文の updated_at を進める = 翌朝の売上日次の作り直し (watermark より後に動いた注文の日) に乗る。
--   🚨 sku_id は解かない (mall を見ない NE コード一致は、FBA なのに NE コードと偶然同じ seller SKU を誤って結ぶ。sku は出品の構成 (listing_components) から売上日次が展開する)
--   🚨 ロックは 注文 → 明細 の順 (受け口 core.apply_order_batch と同じ)。ただし受け口は chunk の中の複数の注文を入力順にロックしたまま commit まで進むので、
--      こちらが order_id 順に「待つ」と循環 (deadlock) になりうる → 注文は **skip locked で「いま取れるものだけ」** 取り、取れなかった注文は次の回に回す (数を返す)。
--      候補はロックを取ってから確定し、update のときにも「まだ未解決で元のコードが同じ」を再確認する (push が同じ明細を変えて commit した後に、古い候補で上書きしない。Codex #1410 R1 #3・R2 #1)
--   🚨 注文の updated_at は trigger (touch_updated_at_unless_seq_only) が「内容が同じなら戻す」ので、保守経路 set_config('core.touch_force', 'on', true) (取引の中だけ) を
--      見て進める形に trigger 関数を直す (0013 の関数の置き換え。ふだんの動き = 世代だけの更新では動かさない、は変えない)
-- 呼び出し = 初期ロード / 夜間ロード (apps/company-db/load/engine.mjs 8b。直近 35 日) が出品を入れた後。
--   全履歴を解き直すなら人が: select * from core.reresolve_order_lines(1, 'amazon', null); → その後 node apps/company-db/push/mall-orders.mjs --mall amazon --refresh-sales --all
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

create or replace function core.reresolve_order_lines(p_company smallint, p_mall text, p_since date default null)
returns table (candidates integer, resolved integer, orders_touched integer, orders_skipped_locked integer) language plpgsql as $$
declare
  v_candidates integer;
  v_resolved integer;
  v_orders integer;
  v_total_orders integer;
  v_locked_orders integer;
  v_rec record;
begin
  -- ① 対象の注文を先にロック (注文 → 明細 の順)。🚨 いま他 (受け口の chunk) がロックしている注文は待たずに飛ばす (skip locked) = deadlock にしない。飛ばした数は返す (次の回で拾う)
  create temp table _rr_orders (order_id bigint primary key) on commit drop;
  select count(*) into v_total_orders from core.orders o
   where o.company_id = p_company and o.mall = p_mall
     and (p_since is null or o.order_date_jst >= p_since)
     and exists (select 1 from core.order_lines l where l.order_id = o.order_id and l.removed_at is null and l.listing_id is null and l.sku_id is null and l.unresolved_code is not null);
  for v_rec in
    select o.order_id from core.orders o
     where o.company_id = p_company and o.mall = p_mall
       and (p_since is null or o.order_date_jst >= p_since)
       and exists (select 1 from core.order_lines l where l.order_id = o.order_id and l.removed_at is null and l.listing_id is null and l.sku_id is null and l.unresolved_code is not null)
     order by o.order_id
       for update of o skip locked
  loop
    insert into _rr_orders (order_id) values (v_rec.order_id);
  end loop;
  select count(*) into v_locked_orders from _rr_orders;
  -- ② 明細をロックしてから候補を確定 (注文のロックを持っている = 受け口はこの注文の明細に触れない。ロックの後に読む)
  perform 1 from core.order_lines l join _rr_orders r on r.order_id = l.order_id where l.removed_at is null and l.listing_id is null and l.sku_id is null and l.unresolved_code is not null order by l.order_line_id for update of l;
  create temp table _rr_cand on commit drop as
    select l.order_line_id, l.order_id, l.unresolved_code, core.resolve_listing_id(p_company, p_mall, l.unresolved_code) as lid
      from core.order_lines l join _rr_orders r on r.order_id = l.order_id
     where l.removed_at is null and l.listing_id is null and l.sku_id is null and l.unresolved_code is not null;
  select count(*) into v_candidates from _rr_cand;
  -- ③ 当たった明細だけ更新 (再確認つき)。実際に更新した明細の注文だけを touch
  create temp table _rr_done (order_id bigint) on commit drop;
  with upd as (
    update core.order_lines l
       set listing_id = c.lid, unresolved_code = null
      from _rr_cand c
     where c.order_line_id = l.order_line_id and c.lid is not null
       and l.removed_at is null and l.listing_id is null and l.sku_id is null and l.unresolved_code = c.unresolved_code
    returning l.order_id
  )
  insert into _rr_done (order_id) select order_id from upd;
  select count(*) into v_resolved from _rr_done;
  perform set_config('core.touch_force', 'on', true);
  with touched as (
    update core.orders o set updated_at = now()
     where o.order_id in (select distinct d.order_id from _rr_done d)
    returning 1
  )
  select count(*) into v_orders from touched;
  perform set_config('core.touch_force', 'off', true);
  drop table _rr_done; drop table _rr_cand; drop table _rr_orders;
  return query select v_candidates, v_resolved, v_orders, v_total_orders - v_locked_orders;
end $$;
comment on function core.reresolve_order_lines(smallint, text, date) is '出品に当たらなかった注文明細を解き直す (出品が増えた後・注文日が p_since 以降)。当たった注文の updated_at を進めて売上日次の作り直しに乗せる。ロックは注文 (skip locked) → 明細';
