-- 0019: au PAY マーケット / LINE ギフトの注文状態を core.order_status_map に (D5b-3 = au PAY・LINE ギフトの注文の push。Company DB構想 08 §9 D5)
--   au PAY: 受注 API の orderStatus の原文。2026-09-18 に miniPC の raw_aupay_orders (全期間 16,029 注文) で出ていた値 = 完了 15,745 (ship_status = Y) / キャンセル 222 / 発送前入金待ち 47 / 発送待ち 15。
--     「完了」は発送後の状態 (apps/aupay-unshipped/service.js: 発送待ちが発送後に変わる) = shipped (配達完了の根拠は無い)。出ていないが仕様にある値も先に入れておく。
--     🚨 raw は注文日 7 日の窓でしか更新されない → 古い注文は発送済みでも「発送待ち」のまま残り得る (raw 側の限界。Company DB はそのまま写す)
--   LINE ギフト: 注文 API の status の原文。raw_linegift_orders (5,809 注文) で出ていた値 = received 5,389 / cancel 350 / payment 36 / gift_message_send 30 / gift_message_wait 3 / cvs 1。
--     🚨 'received' は「届いた」ではない: received の全件に発送時刻 (delivered_on) と送り状番号があり、delivered_on = NE の出荷確定日、received_on は delivered_on とほぼ同時刻
--       = 店が発送した後の終端の状態 → shipped (根拠の無い delivered を作らない)。payment = 支払い済みで出荷前 → confirmed。gift_message_* = 贈り主・受取人の手続き待ち → new。cvs = コンビニ支払い待ち → new
--   ・表に無い値は core.map_order_status が 'unknown' を返す (DQ に出る)
-- 🚨 表・関数には触らない (データの追加だけ)。

insert into core.order_status_map (source_system, source_value, status, note) values
  ('aupay', '新規受付',         'new',       '実測では未出現'),
  ('aupay', '発送前入金待ち',   'new',       '前払いの入金待ち (発送できなくて当然)'),
  ('aupay', '与信待ち',         'new',       '実測では未出現'),
  ('aupay', '発送待ち',         'confirmed', '発送できる状態 (楽天の 300 相当)'),
  ('aupay', '保留',             'on_hold',   '実測では未出現'),
  ('aupay', '発送後入金待ち',   'shipped',   '後払いの入金待ち (発送後)。実測では未出現'),
  ('aupay', '完了',             'shipped',   '発送後 (配達完了の根拠ではない)'),
  ('aupay', 'キャンセル',       'cancelled', 'キャンセル'),
  ('linegift', 'cvs',               'new',       'コンビニ支払い待ち'),
  ('linegift', 'gift_message_wait', 'new',       '贈り主のメッセージ入力待ち'),
  ('linegift', 'gift_message_send', 'new',       '受取人の住所入力待ち'),
  ('linegift', 'payment',           'confirmed', '支払い済み・出荷前'),
  ('linegift', 'received',          'shipped',   '店が発送した後の終端の状態 (発送時刻・送り状番号あり。配達完了の根拠ではない)'),
  ('linegift', 'cancel',            'cancelled', 'キャンセル')
on conflict (source_system, source_value) do nothing;
