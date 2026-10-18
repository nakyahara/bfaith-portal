-- 0084 (仮番号): 商品の画面の「販売数のグラフ」(2026-10-17 中原さん「販売数量のグラフが見れるようになるといいかも」) = マスタの入力の画面 (master_edit) が読むだけの関数 1 つ
--
--   🚨 番号は仮。0083 (入出庫の履歴・feat/sku-movements) の後に積んだ作業ブランチ = 0083 のマージの後にマージする。ほかの作業ブランチ (feat/ne-update-csv-pr1 = メーカー) も 0084 を使う
--      = マージの順で空いている番号に付け替える (欠番は migrate が断る)。0083 に足さない理由 = 0083 を先に本番に当てた後でこの PR がマージされると、
--      当てたファイルの checksum が変わって migrate が止まる (適用済みファイルは書き換えない)。別のファイルなら 0083 とは別の日に当てられる
--
--   画面のロール master_edit は core.orders / order_lines を読めない (create-master-edit-roles.mjs)。表の select を渡すと注文番号・金額まで読めてしまう
--   = 数量だけを返す読むだけの関数 (security definer・0083 と同じ形) だけを渡す。金額は返さない
--
--   数え方 (mart.sales_daily (0021) と mart.sales_expanded_to_skus (0042・見張り W6) と同じ規則):
--     ・材料 = core.orders + 現行の明細 (order_lines.removed_at is null)。日 = 注文日 (JST)。数 = 正味 = 数量 − 取消 (取り消された注文は全数・明細の取消は cancelled_qty)
--     ・SKU の分かる明細はその SKU / 出品だけの明細 (sku_id が空) は出品の構成 (listing_components) × 数量 / NE のセット商品 (sku_kind = 'set') は構成品まで (入れ子 5 段・循環は止める)
--       = 0042 は売れた行から末端へ下る。ここは 1 つの SKU から「それを含むセット・出品」へ上る (同じ道を逆にたどる = 同じ数。会社の全部の行を展開しない = 速い)
--     ・🚨 mart.v_sales_daily (公開の行) でなく core を直に読む: ① mart の売上日次はメルカリを作らない (数量は core にだけある) ② 出品だけの明細を出品で引く索引が
--       mart.sales_daily に無い (core.order_lines には ix_order_lines_listing) ③ 公開の前の注文 (朝の作り直しの前) も入る。数え方は上の規則で同じ
--     ・p_sku_id がセットのとき = そのセットとして売れた数 (そのセットを含む大きいセットの分も)。via_set = NE のセット商品を通って届いた数 (まとめ売りの出品 = 出品の構成だけ、はセット経由に入れない)。
--       数 (qty) は 0042 と同じ。via_set は道ごとに数える = 同じ出品に SKU そのものとそれを含むセットがある行だけ 0042 (行 × SKU に 1 つの印 = 全部をセット経由) と違う (こちらが正しい分け方)
--   返す行 = 日 × モール (正味が 0 の日も、注文があれば返す)。期間 = p_from (2025-01-01 より古くできない = Company DB の注文が日ごとにそろう始まり) 〜 今日 (JST・先の日付の注文は入れない)。
--     決まった日付で切る (今の時刻からの「N 日前」にしない = 試験と本番で同じ範囲)。行の数の上限 = 2025-01-01 から今日までの日数 × 8 モール (年 2,920 行ずつ増える・画面は 24 か月だけ頼む)。
--     明細はその SKU (と、それを含むセット・出品) の全部を索引で引く = Company DB の注文が 2025-01 からなので、日付で切るのと読む量はほぼ同じ (Codex R1 Medium 1)
--   🚨 security definer の関数 = 一時の表を使わない・search_path = pg_catalog, pg_temp (名前は全部 schema つき)・public の実行権を外す。
--      master_edit への実行権は scripts/company-db/create-master-edit-roles.mjs の流し直しで付ける (表の権限は足さない)

create function ops.sku_sales_daily(p_sku_id bigint, p_from date)
  returns table (date_jst date, mall text, qty bigint, via_set bigint)
  language sql stable security definer set search_path = pg_catalog, pg_temp as $$
  with recursive
  -- その SKU と、それを含むセット (上へ 5 段まで・同じ道をもう一度通らない)。mult = その SKU 1 つに何個入るか (道ごと)
  up as (
    select p_sku_id as sku_id, 1::bigint as mult, 0 as depth, array[p_sku_id] as path
     where p_sku_id is not null
    union all
    select sc.parent_sku_id, up.mult * sc.qty, up.depth + 1, up.path || sc.parent_sku_id
      from up join core.sku_components sc on sc.company_id = 1 and sc.child_sku_id = up.sku_id
              join core.skus k on k.company_id = 1 and k.sku_id = sc.parent_sku_id and k.sku_kind = 'set'
     where up.depth < 5 and not (sc.parent_sku_id = any(up.path))
  ),
  src as (
    -- SKU の分かる明細 (その SKU か、それを含むセット)
    select l.order_id, l.qty, l.cancelled_qty, up.mult, up.depth > 0 as via_set
      from up join core.order_lines l on l.sku_id = up.sku_id and l.company_id = 1 and l.removed_at is null
    union all
    -- 出品だけの明細 (出品の構成がその SKU か、それを含むセット)
    select l.order_id, l.qty, l.cancelled_qty, up.mult * c.qty, up.depth > 0
      from up join core.listing_components c on c.company_id = 1 and c.sku_id = up.sku_id
              join core.order_lines l on l.listing_id = c.listing_id and l.company_id = 1 and l.sku_id is null and l.removed_at is null
  ),
  net as (
    select o.order_date_jst as d, o.mall, (s.qty - case when o.is_cancelled then s.qty else s.cancelled_qty end)::bigint * s.mult as n, s.via_set
      from src s join core.orders o on o.order_id = s.order_id
     where o.company_id = 1 and o.order_date_jst >= greatest(p_from, date '2025-01-01')
       and o.order_date_jst <= (pg_catalog.now() at time zone 'Asia/Tokyo')::date
  )
  select d, mall, pg_catalog.sum(n)::bigint, coalesce(pg_catalog.sum(n) filter (where via_set), 0)::bigint
    from net group by d, mall order by d, mall
$$;
revoke all on function ops.sku_sales_daily(bigint, date) from public;
