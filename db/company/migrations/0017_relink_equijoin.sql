-- 0017: 伝票 → 注文の結び直し (core.relink_shipments_bulk) を件数に依らず速くする (D5b-1 の本番投入で見つかった。9/16)
--   症状: limit 2,000 は 0.3 秒だが limit 20,000 は statement_timeout 60 秒に当たる (非線形)。
--   原因: 0016 の UPDATE は `o.mall_order_no = n.order_no_prefix || s2.ne_order_no` = **2 つの表 (ne_shops と shipments) の列を組んだ式**で orders と結合していた。
--         候補が少ないと planner は orders の一意索引を引く nested loop を選ぶが、2 万件では ne_shops との Hash Join に切り替え、
--         この式を「Join Filter」として 全注文 (27 万) × 全候補 (2 万) で評価していた (explain で確認)。
--   直し: 候補の temp table に **照合用の (mall, scope_key, mall_order_no) を列として先に作り**、orders とは列同士の等結合にする
--         (= 一意索引 (company_id, mall, scope_key, mall_order_no) がそのまま使える)。temp table は統計が無いので analyze で件数を planner に教える。
--   🚨 表には触らない (関数の差し替えだけ)。temp table の名前は 0016 の _relink_cand と変える (同じ取引の中に古い形が残っていても衝突しない)
-- 🚨 教訓: temp table 越しの「式」での結合は件数で実行計画が反転する → 結合鍵は列にしてから結合する

create or replace function core.relink_shipments_bulk(p_company_id smallint, p_after bigint default 0, p_limit integer default 20000)
returns table (linked integer, examined integer, last_id bigint) language plpgsql as $$
declare v_linked integer := 0; v_examined integer := 0; v_last bigint := null;
begin
  if p_limit is null or p_limit <= 0 or p_limit > 100000 then raise exception 'p_limit must be 1..100000'; end if;
  create temp table if not exists _relink_cand2 (shipment_id bigint primary key, mall text, scope_key text, mall_order_no text) on commit drop;
  delete from _relink_cand2;
  -- 候補 = 未結合の伝票を shipment_id の順に p_limit 件 (for update。skip locked にしない = 飛ばした伝票を「完了」にしない)。
  -- 店舗 (ne_shops) が無い / mall が null (対象外の店) の伝票も候補に数える (examined) が、照合用の鍵が null なので結ばれない (0016 と同じ)
  insert into _relink_cand2 (shipment_id, mall, scope_key, mall_order_no)
    select s.shipment_id, n.mall, n.scope_key, n.order_no_prefix || s.ne_order_no
      from core.shipments s
      left join core.ne_shops n on n.company_id = s.company_id and n.shop_code = s.shop_code and n.mall is not null
     where s.company_id = p_company_id and s.order_id is null and s.ne_order_no is not null and s.shop_code is not null and s.shipment_id > coalesce(p_after, 0)
     order by s.shipment_id limit p_limit
     for update of s;
  analyze _relink_cand2;
  select count(*), max(shipment_id) into v_examined, v_last from _relink_cand2;
  update core.shipments s set order_id = o.order_id
    from _relink_cand2 c
    join core.orders o on o.company_id = p_company_id and o.mall = c.mall and o.scope_key = c.scope_key and o.mall_order_no = c.mall_order_no
   where s.shipment_id = c.shipment_id and c.mall is not null;
  get diagnostics v_linked = row_count;
  linked := v_linked; examined := v_examined; last_id := v_last;
  return next;
end
$$;
