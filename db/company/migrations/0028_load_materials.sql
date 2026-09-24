-- 0028: 夜間ロードが読んだ「材料」の世代を残す (2026-09-25。Company DB構想 10 §6 / PR ③a-1。Codex ③ 設計レビュー)
--
-- なぜ: 毎朝の照合 (③a-2) は 2 つに分ける = ①ロードの検証 (Company DB ↔ 実際に読んだ材料) ②外との照合 (Company DB ↔ 今朝の NE・ロジザード)。
--   ①には「Company DB がどの材料を読んだか」が要る。ops.ingest_runs.checksum は report の summary のハッシュで、材料の同一性は示さない。
-- なにを: 1 回のロード × 材料の種類 (products / set_components) ごとに 1 行。
--   generation_id・content_hash・row_count = miniPC の sync-to-render が付けた世代 (apps/warehouse/material-lineage.js。Render の mirror_material_generations 経由)。
--     控えは miniPC の DATA_DIR/cdb-material/<generation_id>.json.gz (新しい 14 世代)
--   source_complete_at = その世代の元になった NE の取得が最後まで終わった時刻 (sync_meta.ne_api_*_complete_at)
--   rule_version = 夜間ロードの規則の版 / ownership_hash = その回の持ち主の設定 (config/master-ownership.mjs) のハッシュ
--   材料の世代が分からない回 (古い送り手・mirror に記録なし) は generation_id = null の行を残す (= 照合は「判定できない」)
-- 書くのは夜間ロード (apps/company-db/load/engine.mjs) の取引の中 (ロードが巻き戻れば行も残らない)。読むのは照合 (watcher は select できる)

create table ops.load_materials (
  ingest_run_id      text not null,
  entity             text not null check (entity in ('products', 'set_components')),
  generation_id      text,
  content_hash       text,
  row_count          integer check (row_count >= 0),
  source_complete_at text,
  generation_created_at text,
  mirror_received_at text,
  rule_version       text not null,
  ownership_hash     text not null,
  recorded_at        timestamptz not null default now(),
  primary key (ingest_run_id, entity),
  constraint ck_load_materials_known check (generation_id is null or (content_hash is not null and row_count is not null))
);
create index ix_load_materials_generation on ops.load_materials (generation_id) where generation_id is not null;
