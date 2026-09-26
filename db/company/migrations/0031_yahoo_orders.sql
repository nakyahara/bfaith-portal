-- 0031: Yahoo!ショッピングの注文を Company DB に入れる (D5b-5。Company DB構想 08 §9 D5 / D-32)
--   D-32 (Yahoo の受注データを持ってよいか) は 0013 で「b) 確認できるまで入れない」= core.mall_order_policy の yahoo を false にしていた。
--   2026-09-26 に中原さんが「Yahoo の注文を Company DB に入れてよい」と決めた = a) に変える。
--   入れるのは raw_yahoo_orders の列だけ (注文番号・日時・状態・金額・商品コード・数量。氏名・住所・電話・メールなどの個人情報の列は raw にも無い)
--
--   状態 = OrderStatus (1 予約中 / 2 処理中 / 3 保留 / 4 キャンセル / 5 完了) - PayStatus (0 未入金 / 1 入金済) - ShipStatus (0 出荷不可 / 1 出荷可 / 2 出荷処理中 / 3 出荷完了 / 4 着荷済)
--   を送り手が '5-1-3' の形の 1 つの原文にして送る。2026-09-26 の miniPC の raw (90,854 注文) で出ていた組み合わせ =
--     5-1-3 89,666 / 4-1-1 652 / 4-0-0 279 / 2-1-1 155 / 2-0-0 69 / 4-1-3 15 / 5-1-4 9 / 2-1-3 6 / 2-0-3 2 / 5-0-0 1
--   ・実測で出ていない組み合わせは入れない (出たら core.map_order_status が 'unknown' を返す = DQ に出る)
--   ・5-0-0 (完了なのに未入金・出荷不可。1 件) は意味が分からないので入れない = unknown
-- 🚨 表・関数には触らない (データの変更だけ)。

update core.mall_order_policy
   set orders_enabled = true,
       note = 'D-32 = a: 2026-09-26 中原さん「Yahoo の注文を Company DB に入れてよい」(0031)'
 where company_id = 1 and mall = 'yahoo' and scope_key = 'main' and orders_enabled = false;

insert into core.order_status_map (source_system, source_value, status, note) values
  ('yahoo', '5-1-3', 'shipped',   '完了・入金済・出荷完了'),
  ('yahoo', '5-1-4', 'delivered', '完了・入金済・着荷済'),
  ('yahoo', '2-0-0', 'new',       '処理中・未入金・出荷不可 (入金待ち)'),
  ('yahoo', '2-1-1', 'confirmed', '処理中・入金済・出荷可'),
  ('yahoo', '2-1-3', 'shipped',   '処理中・入金済・出荷完了'),
  ('yahoo', '2-0-3', 'shipped',   '処理中・未入金・出荷完了 (後払いなど)'),
  ('yahoo', '4-0-0', 'cancelled', 'キャンセル・未入金'),
  ('yahoo', '4-1-1', 'cancelled', 'キャンセル・入金済・出荷可'),
  ('yahoo', '4-1-3', 'cancelled', 'キャンセル・入金済・出荷完了')
on conflict (source_system, source_value) do nothing;
