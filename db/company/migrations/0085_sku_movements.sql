-- 0085: 商品ごとの入出庫履歴と月別のまとめ (CompanyDB構想 26 の PR-1・PR-2) = マスタの入力の画面 (master_edit) が読むだけの関数 4 つ
--
--   番号: 0083 = #1734 (今朝の NE の値)・0084 = #1739 (メーカー) の次 = 0085 (マージの順に付け替えた。欠番は migrate が断る)。
--
--   画面のロール master_edit は core.shipments / shipment_lines / ne_shops と ops.ingest_runs を読めない (create-master-edit-roles.mjs)。
--   表の select を渡すと送り状番号なども読めてしまう・行の数の上限を決められない = 読むだけの関数 (security definer) だけを渡す (0070 と同じ形):
--     1. ops.sku_stock_anchors        ロジザードの毎日の在庫 (締めた日) とその取得時刻・その SKU の数 (在庫数を合わせる錨)
--     2. ops.sku_shipment_totals      出荷の数を、時刻の区切り (錨の取得時刻) ごとに足したもの (調整の行を全部の区間で作る)
--     3. ops.sku_movement_shipments   出荷の行 (伝票ごと・時刻・NE の伝票番号・店舗・数)。新しい順・上限つき
--     4. ops.sku_shipments_monthly    月ごとの受注数 (NE の受注日) と売上数 (出荷確定日)
--   数え方 (26 §2・§3): 取消の伝票・取消の明細・外れた明細 (removed_at) は数えない。出荷 = 出荷確定 (shipped_at) のある伝票。店舗では絞らない (FBA 納品・卸も入る = NE と同じ)。
--   金額は返さない (出荷の明細に金額の列が無い = PR-3)。送り状番号・宛先・受注番号は返さない。
--   🚨 security definer の関数 = 一時の表を使わない・search_path = pg_catalog, pg_temp (名前は全部 schema つき)・public の実行権を外す。
--      master_edit への実行権は scripts/company-db/create-master-edit-roles.mjs の流し直しで付ける (表の権限は足さない)

-- ─── 1. ロジザードの毎日の在庫 (錨) ───
--   日 = その日の最後に完走した取得 (0011)。取得時刻 = stock_capture_days.captured_at (ロジザードは空) か、取得の回の世代 (ops.ingest_runs.checksum = ISO の時刻・logizard.mjs)。
--   数 = その日の SKU の行の合計 (ロケ・品質区分を足した sku_stock_daily)。complete の日に行が無い = 0 (その日の倉庫に無い)。complete でない日は時刻と数を返さない
create function ops.sku_stock_anchors(p_sku_id bigint, p_from date)
  returns table (snapshot_date date, status text, captured_at timestamptz, qty integer)
  language sql stable security definer set search_path = pg_catalog, pg_temp as $$
  select d.snapshot_date, d.status,
         case when d.status = 'complete' then coalesce(d.captured_at, case when r.checksum ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T' then r.checksum::timestamptz end) end,
         case when d.status = 'complete' then coalesce((select pg_catalog.sum(x.qty) from snapshots.sku_stock_daily x
                                                         where x.sku_id = p_sku_id and x.snapshot_date = d.snapshot_date and x.source = d.source and x.scope_key = d.scope_key), 0)::integer end
    from snapshots.stock_capture_days d left join ops.ingest_runs r on r.ingest_run_id = d.ingest_run_id
   where d.company_id = 1 and d.source = 'logizard' and d.scope_key = 'main' and d.snapshot_date >= p_from and p_sku_id is not null
   order by d.snapshot_date
$$;
revoke all on function ops.sku_stock_anchors(bigint, date) from public;

-- ─── 2. 区切りごとの出荷の数 ───
--   p_bounds = 昇順の時刻の区切り (2〜1,000 個)。区間 i = (p_bounds[i], p_bounds[i + 1]] (取得の時刻ちょうどの出荷はその日の区間)。
--   width_bucket は左を含む = 1 マイクロ秒引いて右を含む形にする (timestamptz の精度 = 1 マイクロ秒)
create function ops.sku_shipment_totals(p_sku_id bigint, p_bounds timestamptz[])
  returns table (bucket integer, qty bigint)
  language sql stable security definer set search_path = pg_catalog, pg_temp as $$
  select pg_catalog.width_bucket(s.shipped_at - interval '1 microsecond', p_bounds), pg_catalog.sum(l.qty)::bigint
    from core.shipment_lines l join core.shipments s on s.shipment_id = l.shipment_id
   where l.sku_id = p_sku_id and l.removed_at is null and not l.is_cancelled and not s.is_cancelled
     and pg_catalog.cardinality(p_bounds) between 2 and 1000
     and s.shipped_at > p_bounds[1] and s.shipped_at <= p_bounds[pg_catalog.cardinality(p_bounds)]
   group by 1
$$;
revoke all on function ops.sku_shipment_totals(bigint, timestamptz[]) from public;

-- ─── 3. 出荷の行 (伝票ごと) ───
--   新しい順 (出荷確定の時刻・伝票番号 (C の並び) の降順)。(p_after, 続きの位置] の出荷確定。p_limit は 1〜5,000 (既定 200)。同じ伝票に同じ商品の明細が 2 つあれば足す。
--   続きの位置 = p_before (時刻) と p_before_slip (伝票番号)。p_before_slip が null = その時刻まで全部 (時刻ちょうどを含む)・あり = 同じ時刻ならその伝票番号より前だけ
--   (同じ時刻に伝票が多くて上限で切れたとき、同じ時刻の残りを次のページで読む = 取りこぼさない。Codex R1)
create function ops.sku_movement_shipments(p_sku_id bigint, p_after timestamptz, p_before timestamptz, p_before_slip text, p_limit integer)
  returns table (shipped_at timestamptz, ship_date_jst date, ne_slip_no text, shop_code text, shop_name text, qty bigint)
  language sql stable security definer set search_path = pg_catalog, pg_temp as $$
  select s.shipped_at, s.ship_date_jst, s.ne_slip_no, s.shop_code, n.shop_name, pg_catalog.sum(l.qty)::bigint
    from core.shipment_lines l join core.shipments s on s.shipment_id = l.shipment_id
    left join core.ne_shops n on n.company_id = s.company_id and n.shop_code = s.shop_code
   where l.sku_id = p_sku_id and l.removed_at is null and not l.is_cancelled and not s.is_cancelled and s.shipped_at is not null
     and s.shipped_at > coalesce(p_after, '-infinity'::timestamptz)
     and (s.shipped_at < coalesce(p_before, 'infinity'::timestamptz)
          or (s.shipped_at = p_before and (p_before_slip is null or s.ne_slip_no collate "C" < p_before_slip collate "C")))
   group by s.shipment_id, s.shipped_at, s.ship_date_jst, s.ne_slip_no, s.shop_code, n.shop_name
  having pg_catalog.sum(l.qty) > 0
   order by s.shipped_at desc, s.ne_slip_no collate "C" desc
   limit least(greatest(coalesce(p_limit, 200), 1), 5000)
$$;
revoke all on function ops.sku_movement_shipments(bigint, timestamptz, timestamptz, text, integer) from public;

-- ─── 4. 月ごとの受注数・売上数 ───
--   受注数 = NE の受注日 (order_date_jst) の月・出荷していない伝票も入る。売上数 = 出荷確定日 (ship_date_jst) の月。p_from の月から
create function ops.sku_shipments_monthly(p_sku_id bigint, p_from date)
  returns table (month date, order_qty bigint, ship_qty bigint)
  language sql stable security definer set search_path = pg_catalog, pg_temp as $$
  with x as (
    select s.order_date_jst, s.ship_date_jst, l.qty
      from core.shipment_lines l join core.shipments s on s.shipment_id = l.shipment_id
     where l.sku_id = p_sku_id and l.removed_at is null and not l.is_cancelled and not s.is_cancelled
       and (s.order_date_jst >= p_from or s.ship_date_jst >= p_from)),
  m as (
    select pg_catalog.date_trunc('month', order_date_jst::timestamp)::date as month, qty as o, 0 as sh from x where order_date_jst >= p_from
    union all
    select pg_catalog.date_trunc('month', ship_date_jst::timestamp)::date, 0, qty from x where ship_date_jst >= p_from)
  select month, pg_catalog.sum(o)::bigint, pg_catalog.sum(sh)::bigint from m group by month order by month
$$;
revoke all on function ops.sku_shipments_monthly(bigint, date) from public;
