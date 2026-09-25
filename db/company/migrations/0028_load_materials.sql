-- 0028: 夜間ロードが読んだ「材料」の世代を残す (2026-09-25。Company DB構想 10 §6 / PR ③a-1。Codex ③ 設計レビュー・PR #1447 R1)
--
-- なぜ: 毎朝の照合 (③a-2) は 2 つに分ける = ①ロードの検証 (Company DB ↔ 実際に読んだ材料) ②外との照合 (Company DB ↔ 今朝の NE・ロジザード)。
--   ①には「Company DB がどの材料を読んだか」が要る。ops.ingest_runs.checksum は report の summary のハッシュで、材料の同一性は示さない。
-- なにを: 1 回のロード × 材料の種類 (products / set_components) ごとに 1 行。
--   content_hash・row_count = この回が **実際に読んだ** mirror の中身 (apps/warehouse/material-lineage.js の規則でそろえたハッシュ)
--   status:
--     matched       = 中身が mirror の世代 (miniPC の sync-to-render が付け、Render の受け手が確かめて残した) と同じ。
--                     generation_id の控え (miniPC の DATA_DIR/cdb-material/<generation_id>.json.gz) = この回が読んだ中身
--     mismatch      = mirror が世代を名乗っているが中身が違う (受信のあと Render 側で書き換えられた = 会計アプリの税率・売上分類 / 原価の例外 など)
--     no_generation = mirror に世代の記録が無い (古い送り手・記録できなかった受信)
--   generation_id・source_complete_at・generation_created_at は matched のときだけ (照合に使ってよいのは matched だけ)。
--   mirror_generation_id・mirror_received_at = mirror が名乗っていた世代 (mismatch の調べ用)
--   source_complete_at = その世代の元になった NE の取得が最後まで終わった時刻 (sync_meta.ne_api_*_complete_at。途中で失敗した回は null)
--   rule_version = 夜間ロードの規則の版 / ownership_hash = その回の持ち主の設定 (config/master-ownership.mjs) のハッシュ
-- 書くのは夜間ロード (apps/company-db/load/engine.mjs) の取引の中 (ロードが巻き戻れば行も残らない)。読むのは照合 (watcher は select できる)

create table ops.load_materials (
  ingest_run_id         text not null,
  entity                text not null check (entity in ('products', 'set_components')),
  status                text not null check (status in ('matched', 'mismatch', 'no_generation')),
  content_hash          text not null check (content_hash ~ '^[0-9a-f]{64}$'),
  row_count             integer not null check (row_count >= 0),
  generation_id         text,
  source_complete_at    text,
  generation_created_at text,
  mirror_generation_id  text,
  mirror_received_at    text,
  rule_version          text not null,
  ownership_hash        text not null,
  recorded_at           timestamptz not null default now(),
  primary key (ingest_run_id, entity),
  constraint ck_load_materials_matched check ((status = 'matched') = (generation_id is not null)),
  constraint ck_load_materials_matched_only check (status = 'matched' or (source_complete_at is null and generation_created_at is null)),
  constraint ck_load_materials_no_generation check (status <> 'no_generation' or mirror_generation_id is null)
);
create index ix_load_materials_generation on ops.load_materials (generation_id) where generation_id is not null;
