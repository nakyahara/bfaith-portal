-- 0016: 注文の push (Company DB構想 08 §4.1 / §4.7 / §9 D5b) の受け皿の補い。D5b-1 = 楽天
--   ① 楽天の注文状態 (orderProgress) を core.order_status_map に (100〜900。800 / 900 = キャンセル系)
--   ② core.resolve_listing_id: 出品コードの原文 (listing_code) に当たらなければ別名 (core.external_ids の listing の system = mall) でも探す。
--      楽天の raw (raw_rakuten_orders.item_number) は 商品番号 (W) で、AM (システム連携用 SKU 番号) を持つ出品は listing_code = AM・W は別名にしかいない (初期ロードの束ね方)。
--      1 件に決まるときだけ返す (色違いが同じ W を共有する = 複数に当たる → null = unresolved_code に原文を残す。
--      🚨 楽天の raw に SKU 単位のコード (variantId / SKU 管理番号) が無いので色違いは解決できない = 宿題: rakuten-orders.js の allowlist に足して取り直す)
--   ③ core.relink_shipments_bulk: 伝票 → 注文の結び直しを集合で (0013 の relink_shipments は 1 伝票ずつ = 50 万伝票では遅い)。
--      shipment_id の順に p_limit 件ずつ (for update skip locked)、注文が見つかった伝票だけ order_id を入れる。呼ぶ側は last_id を渡して続きを取る
-- 🚨 0004〜0015 の表には触らない (関数の差し替えとデータの追加だけ)。

insert into core.order_status_map (source_system, source_value, status, note) values
  ('rakuten', '100', 'new',       '注文確認待ち'),
  ('rakuten', '200', 'new',       '楽天処理中'),
  ('rakuten', '300', 'confirmed', '発送待ち'),
  ('rakuten', '400', 'on_hold',   '変更確認待ち'),
  ('rakuten', '500', 'ready',     '発送前'),
  ('rakuten', '600', 'shipped',   '発送後'),
  ('rakuten', '700', 'delivered', '完了'),
  ('rakuten', '800', 'cancelled', 'キャンセル確定待ち (キャンセル系)'),
  ('rakuten', '900', 'cancelled', 'キャンセル確定')
on conflict (source_system, source_value) do nothing;

create or replace function core.resolve_listing_id(p_company_id smallint, p_mall text, p_code text) returns bigint language sql stable as $$
  with direct as (
    select l.listing_id from core.listings l
     where l.company_id = p_company_id and l.mall = p_mall and p_code is not null and l.listing_norm = core.norm_code(p_code)
  ), alias as (
    select e.entity_id as listing_id from core.external_ids e
      join core.listings l on l.listing_id = e.entity_id and l.company_id = p_company_id and l.mall = p_mall
     where e.company_id = p_company_id and e.entity_type = 'listing' and e.system = p_mall and e.valid_to is null
       and p_code is not null and e.external_norm = core.norm_code(p_code)
  ), cand as (
    select listing_id from direct union select listing_id from alias
  )
  select min(listing_id) from cand having count(*) = 1;
$$;

create or replace function core.relink_shipments_bulk(p_company_id smallint, p_after bigint default 0, p_limit integer default 20000)
returns table (linked integer, examined integer, last_id bigint) language plpgsql as $$
declare v_linked integer := 0; v_examined integer := 0; v_last bigint := null;
begin
  if p_limit is null or p_limit <= 0 or p_limit > 100000 then raise exception 'p_limit must be 1..100000'; end if;
  create temp table if not exists _relink_cand (shipment_id bigint primary key) on commit drop;
  delete from _relink_cand;
  insert into _relink_cand
    select s.shipment_id from core.shipments s
     where s.company_id = p_company_id and s.order_id is null and s.ne_order_no is not null and s.shop_code is not null and s.shipment_id > coalesce(p_after, 0)
     order by s.shipment_id limit p_limit
     for update of s skip locked;
  select count(*), max(shipment_id) into v_examined, v_last from _relink_cand;
  update core.shipments s set order_id = o.order_id
    from _relink_cand c
    join core.shipments s2 on s2.shipment_id = c.shipment_id
    join core.ne_shops n on n.company_id = s2.company_id and n.shop_code = s2.shop_code and n.mall is not null
    join core.orders o on o.company_id = s2.company_id and o.mall = n.mall and o.scope_key = n.scope_key and o.mall_order_no = n.order_no_prefix || s2.ne_order_no
   where s.shipment_id = c.shipment_id;
  get diagnostics v_linked = row_count;
  linked := v_linked; examined := v_examined; last_id := v_last;
  return next;
end
$$;
