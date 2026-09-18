-- 0018: Amazon の注文状態を core.order_status_map に (D5b-2 = Amazon の注文の push。Company DB構想 08 §9 D5)
--   元 = 注文レポート (GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL) の order-status の原文。2026-09-18 に miniPC の raw_sp_orders (2025-01-01 以降 128.6 万注文) で出ていた値:
--     Shipped 1,126,797 / Shipped - Delivered to Buyer 77,743 / Cancelled 77,295 / Pending 2,823 / Shipped - Picked Up 488 / Shipped - Lost in Transit 451 /
--     Shipped - Returned to Seller 75 / Shipped - Returning to Seller 74 / Shipped - Undeliverable 65 / Shipped - Rejected by Buyer 64 / Shipped - Out for Delivery 22 /
--     Unfulfillable 5 / Pending - Waiting for Pick Up 1
--   ・「配達完了」の根拠がある 2 つ (Delivered to Buyer / Picked Up) だけ delivered。ただの Shipped は shipped (楽天と同じ考え方 = 根拠の無い delivered を作らない)
--   ・戻ってくる・戻ってきた (Returning / Returned / Undeliverable / Rejected) は returned。紛失 (Lost in Transit) は発送後なので shipped (注に残す)
--   ・表に無い値は core.map_order_status が 'unknown' を返す (DQ に出る)。出ていないが仕様にある値 (Unshipped / PartiallyShipped 等) も先に入れておく
-- 🚨 表・関数には触らない (データの追加だけ)。

insert into core.order_status_map (source_system, source_value, status, note) values
  ('amazon', 'Pending',                        'new',       '支払い確認中 (金額・数量が空のことがある)'),
  ('amazon', 'PendingAvailability',            'new',       '予約注文 (実測では未出現)'),
  ('amazon', 'Unshipped',                      'confirmed', '出荷待ち (実測では未出現。注文レポートでは Pending の次が Shipped)'),
  ('amazon', 'PartiallyShipped',               'confirmed', '一部出荷 (実測では未出現)'),
  ('amazon', 'InvoiceUnconfirmed',             'confirmed', '請求書未確認 (実測では未出現)'),
  ('amazon', 'Shipping',                       'ready',     '出荷作業中 (実測では未出現)'),
  ('amazon', 'Shipped',                        'shipped',   '出荷済み (配達完了の根拠ではない)'),
  ('amazon', 'Shipped - Out for Delivery',     'shipped',   '配達中'),
  ('amazon', 'Shipped - Lost in Transit',      'shipped',   '輸送中に紛失 (発送後。補填は財務側)'),
  ('amazon', 'Pending - Waiting for Pick Up',  'shipped',   '受取スポットで受け取り待ち (発送後)'),
  ('amazon', 'Shipped - Delivered to Buyer',   'delivered', '配達完了'),
  ('amazon', 'Shipped - Picked Up',            'delivered', '受取スポットで受け取り済み'),
  ('amazon', 'Shipped - Returning to Seller',  'returned',  '返送中'),
  ('amazon', 'Shipped - Returned to Seller',   'returned',  '返送済み'),
  ('amazon', 'Shipped - Undeliverable',        'returned',  '配達不能 (返送になる)'),
  ('amazon', 'Shipped - Rejected by Buyer',    'returned',  '受取拒否'),
  ('amazon', 'Unfulfillable',                  'on_hold',   '出荷できない (FBA の在庫なし等)'),
  ('amazon', 'Cancelled',                      'cancelled', 'キャンセル')
on conflict (source_system, source_value) do nothing;
