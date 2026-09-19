-- 0020: Qoo10 の注文状態を core.order_status_map に (D5b-4 = Qoo10 の注文の push。Company DB構想 08 §9 D5)
--   元 = ShippingBasic.GetShippingInfo_v2 の shippingStatus の原文。2026-09-19 に miniPC の raw_qoo10_orders の API の行 (1,994 行) で出ていた値 =
--     Delivered(5) 1,970 / Seller confirm(3) 14 / Awaiting shipping(1) 8 / On delivery(4) 2。意味は apps/qoo10-unshipped/service.js (実測にもとづく):
--       Awaiting shipping(1) = 入金待ち (全件コンビニ決済 / 決済方法なし) → new / Seller confirm(3) = 販売者確認済み = 発送できる状態 → confirmed /
--       On delivery(4) = 配送中 → shipped / Delivered(5) = 配送完了 (モールが配達完了と言っている) → delivered
--   ・状態 (2) は実測で出ていない = 原文の表記が分からないので入れない (出たら core.map_order_status が 'unknown' を返す = DQ に出る)
--   ・🚨 取消は API に出てこない (取込は状態 1〜5 だけ) = 取り消された注文は最後に見えた状態のまま残る (raw 側の限界。cancelled の対応は無い)
-- 🚨 表・関数には触らない (データの追加だけ)。

insert into core.order_status_map (source_system, source_value, status, note) values
  ('qoo10', 'Awaiting shipping(1)', 'new',       '入金待ち (コンビニ決済など。発送できなくて当然)'),
  ('qoo10', 'Seller confirm(3)',    'confirmed', '販売者確認済み = 発送できる状態'),
  ('qoo10', 'On delivery(4)',       'shipped',   '配送中'),
  ('qoo10', 'Delivered(5)',         'delivered', '配送完了 (モールの表示)')
on conflict (source_system, source_value) do nothing;
