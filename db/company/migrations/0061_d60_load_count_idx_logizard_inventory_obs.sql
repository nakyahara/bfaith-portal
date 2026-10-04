-- migrate:concurrent-index
-- D-60 PR 3a-i の index (負荷の数え上げの段が index だけで上限 + 1 行目で止まるため)・5 つの file の 5/5 = raw.logizard_inventory_observations ((b)-2)
--   1 file = 1 表 (設計 §3.10 の推し)。番号の付け替え・runner の契約・マージの前提は 1/5 (d60_load_count_idx_listings) の頭の注記と同じ
--   設計の正本 = AI_reference『システム設計/CompanyDB構想/13_Amazon利益のmart_設計_20260930.md』(v40・D-60 v3.15) の付録 B と §3.10
-- 🚨 番号は仮 (今の次の空きからの連番)。付け替え = この .sql と横の .expect.json を git mv するだけ (本文と expect.json は番号を書かない)
-- 🚨 途中で落ちた = この file は記録しない (前の file は記録済みのまま) → もう一度流して完成させる (作り済みの valid は飛ばし、invalid は作り直す)
--
-- (b)-2 raw.purge_superseded_observations の count = raw.logizard_inventory_observations (observed_at)
--    … これが無いと PG 18 は ix_logizard_inventory_obs_key_time を skip scan で使う (Filter なし) = 試験は Index Name の固定で落とす
-- 試験 = scripts/test-company-db-d60-load-count-indexes-pg.mjs

create index concurrently if not exists ix_logizard_inventory_obs_observed_at
  on raw.logizard_inventory_observations (observed_at);
