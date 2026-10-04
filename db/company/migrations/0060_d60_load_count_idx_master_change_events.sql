-- migrate:concurrent-index
-- D-60 PR 3a-i の index (負荷の数え上げの段が index だけで上限 + 1 行目で止まるため)・5 つの file の 4/5 = events.master_change_events (⑨a・⑨b)
--   1 file = 1 表 (設計 §3.10 の推し)。番号の付け替え・runner の契約・マージの前提は 1/5 (d60_load_count_idx_listings) の頭の注記と同じ
--   設計の正本 = AI_reference『システム設計/CompanyDB構想/13_Amazon利益のmart_設計_20260930.md』(v40・D-60 v3.15) の付録 B と §3.10
-- 🚨 番号は仮 (今の次の空きからの連番)。付け替え = この .sql と横の .expect.json を git mv するだけ (本文と expect.json は番号を書かない)
-- 🚨 途中で落ちた = この file は記録しない (前の file は記録済みのまま) → もう一度流して完成させる (作り済みの valid は飛ばし、invalid は作り直す)。
--    この file だけ 2 文 (同じ表) = 1 本目が valid・2 本目で落ちたら、再実行の file の合計の判定は 2 本の予想 (作り済みの分も入れる = 止まる向き)
--
-- ⑨a 監査 (構成・今の側) = events.master_change_events の CASE の式 (entity_type = 'listing_component' の entity_key ->> 'listing_id')
-- ⑨b 監査 (構成・旧い側) = events.master_change_events の CASE の式 (listing_component の UPDATE で attribute = 'listing_id' の old_value #>> '{}')
--    … ⑨a / ⑨b は文に entity_type の等号を書かない = ix_master_change_events_entity (entity_type, …) + Filter を選べない (設計 v3.8)。
--      式は付録 B の本文の式と同じ (比べるのは catalog の pg_get_expr)・部分 index の条件 (<式> is not null) は本文の = any(…) (strict) から導ける
-- 試験 = scripts/test-company-db-d60-load-count-indexes-pg.mjs

create index concurrently if not exists ix_master_change_events_component_listing
  on events.master_change_events ((case when entity_type = 'listing_component' then entity_key ->> 'listing_id' end))
  where (case when entity_type = 'listing_component' then entity_key ->> 'listing_id' end) is not null;

create index concurrently if not exists ix_master_change_events_component_old_listing
  on events.master_change_events ((case when entity_type = 'listing_component' and operation = 'UPDATE' and attribute = 'listing_id' then old_value #>> '{}' end))
  where (case when entity_type = 'listing_component' and operation = 'UPDATE' and attribute = 'listing_id' then old_value #>> '{}' end) is not null;
