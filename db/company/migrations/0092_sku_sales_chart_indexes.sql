-- 0092: 販売数のグラフ (0086 の ops.sku_sales_daily) を速くする = 索引 3 本 + 関数の読む順 + 注文の 2 表の autovacuum の設定 (2026-10-19・よく売れる SKU でグラフが 8 秒の期限に当たる)
--
--   なぜ: 本番で読むだけの EXPLAIN ANALYZE (いちばん売れる 0726-001931・2,068 行) = 11.0 秒 (2 回目も 11 秒 = DB のメモリが小さく、読んだ頁が残らない)。
--     ① 出品だけの明細 (出品 7 つ・31,005 行) = ix_order_lines_listing の後に明細の表の頁を 1 行ずつ読む = 4.9 秒 (読み 24,071 頁)
--     ② 注文 33,646 件 = orders_pkey の後に注文の表の頁 (注文日・モール・取消の印) を 1 件ずつ読む = 約 6 秒 (読み 26,882 頁)
--     = 遅いのは表の頁を 1 行ずつ読むこと (合計 読み 51k 頁)。数え方は変えずに、要る列を全部持つ索引 (covering) で「索引だけで読む」(Index Only Scan) にする
--   索引 (関数の where と同じ条件の部分索引・include = 関数が読む列の全部 = 表の頁を読まない):
--     ・ix_order_lines_listing_only_chart = 出品だけの明細 (sku_id が空・現行) を出品で引く。1 つの出品の行が索引の中で並ぶ = 3 万行でも 約 200 頁
--     ・ix_order_lines_sku_chart          = SKU の分かる明細 (現行) を SKU で引く
--     ・ix_orders_chart                   = 注文を注文の番号で引いて 会社・注文日・モール・取消の印 を索引だけで読む (表の 1 行 約 350 バイト → 索引の 1 行 約 40 バイト)
--   関数 = 0086 の写しに 2 か所だけ足した (数え方・返す形・権限は同じ): 明細の集まり (src) を materialized にして注文の番号の順に並べる。
--     よく売れる SKU は注文を 3 万件引く = ix_orders_chart の葉の頁のほとんどに当たる。出品ごとに 7 回の行き来で引くと頁を飛び飛びに読む →
--     番号の順に 1 回で引くと、索引の頁を前から順に読む (索引は番号の順に並んで作られる = OS の先読みが効く向き・同じ頁を 2 回読まない)。並べ替えは 3 万行で数 ms
--     create or replace = 持ち主・master_edit の実行権・public から外した実行権はそのまま (create-master-edit-roles.mjs の流し直しは要らない)。revoke は念のためもう一度
--   🚨 lock: ふつうの create index (concurrently でない) = 作っている間 core.orders / core.order_lines への書き込み (insert・update・delete) が待つ (読むのは止めない = 画面・グラフ・見張りは動く)。
--     3 本は 1 つの取引 = order_lines は 1 本目から・orders は 3 本目から commit まで書き込みが待つ。runner がこの取引に lock_timeout 10 秒・statement_timeout 10 分を入れる
--     (書いている取引があれば lock を 10 秒待って巻き戻して失敗 = 少し待ってもう一度)。🚨 statement_timeout は 1 文ごと (取引の全体ではない・Codex 0092 R1 High) =
--     このファイルの中で 1 文 3 分に縮める = 書き込みが待つのは 索引 3 文の合計で長くて 約 9 分 (目安・ほかの文は一瞬) (手元の 注文 400 万・明細 500 万 で 3 本 約 60 秒)。超えたら全部巻き戻す = 何も残らない
--     (索引・設定・関数のどれも残らない。concurrently の失敗と違って invalid の索引も残らない)。
--     → 注文の push (daily-sync)・夜間ロード (明細の解き直し) が動いていない昼に当てる (README「販売数のグラフを速くする (0092)」)
--     concurrent-index の migration (D-60 PR 3a-i の runner) にしない理由 = 本番では今は流れない (容量の読み手が Render の host の対応の回答を待って fail-closed =
--     RENDER_PG_HOST_MAPPING.confirmed = false)。ここで作る 3 本は後から作り直さなくてよい
--   索引だけで読めるのは「全部の行が見える」印 (visibility map) の付いた頁だけ (印の無い頁は表を読みに行く = 数は同じ・遅くなるだけ)。
--     印を付けるのは vacuum。この 2 表は大きい (注文 約 400 万) = 既定の autovacuum (変わった行・足した行が 20% で動く) では 数か月に 1 回 = 直近の頁に印が無い。
--     → この 2 表だけ 2% で動くようにする (目安 = 足した行・変わった行が 約 8 万 = 1〜2 週ごと。PG 18 の足した行の閾値は凍っていない頁の割合も掛かる = もっと早く動くこともある。
--     印の付いた頁は飛ばす = 1 回は前より軽いはず・負荷は当てた後の実績で見る)。設定は vacuum を流す命令ではない = 当てた後に閾値に届いていなければ人が 1 回 vacuum (analyze) を流す
--     (README の手順・取引の中では流せない)。alter table … set (autovacuum の設定) の lock は SHARE UPDATE EXCLUSIVE (読み書きを止めない・ただし同じ取引の create index の lock は commit まで残る)
--   INCLUDE の列 (orders.is_cancelled・order_lines.qty / cancelled_qty) が変わる update は HOT にならなくなる (取消のときだけ・ほかの列は前から索引にある) = 書き込みが少し増えるだけ
--   作る時間・大きさの見込み = README の 0092 の節 (当てる前に scripts/company-db/sales-chart-measure.mjs で読むだけで確かめる)

set local statement_timeout = '3min';   -- 1 文ごと (上の 🚨・runner の set local 10min の後なのでこちらが効く)。取引の終わりで session の値に戻る

create index if not exists ix_order_lines_listing_only_chart on core.order_lines (listing_id) include (company_id, order_id, qty, cancelled_qty)
  where listing_id is not null and sku_id is null and removed_at is null;
create index if not exists ix_order_lines_sku_chart on core.order_lines (sku_id) include (company_id, order_id, qty, cancelled_qty)
  where sku_id is not null and removed_at is null;
create index if not exists ix_orders_chart on core.orders (order_id) include (company_id, order_date_jst, mall, is_cancelled);

alter table core.orders set (autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_insert_scale_factor = 0.02);
alter table core.order_lines set (autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_insert_scale_factor = 0.02);

-- 0086 の写し。🆕 0092 = src を materialized にして order by order_id (この 2 か所だけ)
create or replace function ops.sku_sales_daily(p_sku_id bigint, p_from date)
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
  src as materialized (
    -- SKU の分かる明細 (その SKU か、それを含むセット)
    select l.order_id, l.qty, l.cancelled_qty, up.mult, up.depth > 0 as via_set
      from up join core.order_lines l on l.sku_id = up.sku_id and l.company_id = 1 and l.removed_at is null
    union all
    -- 出品だけの明細 (出品の構成がその SKU か、それを含むセット)
    select l.order_id, l.qty, l.cancelled_qty, up.mult * c.qty, up.depth > 0
      from up join core.listing_components c on c.company_id = 1 and c.sku_id = up.sku_id
              join core.order_lines l on l.listing_id = c.listing_id and l.company_id = 1 and l.sku_id is null and l.removed_at is null
     order by order_id   -- 🆕 0092: 注文を番号の順に 1 回で引く (速さだけ・数は変わらない)
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
