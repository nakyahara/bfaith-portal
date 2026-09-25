-- 0030: 夜間ロードの「判断」を残す (2026-09-25。Company DB構想 10 §6.1.1 B1。Codex ③a-2 R1 H2・B-R0 #1)
--
-- なぜ: 毎朝の照合の ①ロードの検証は「ロードの後にあるべき値」を材料 (miniPC の控え) から作り直して Company DB と比べる。
--   ロードの判断の一部は**ロードの時の DB の状態**に依る (セット構成の manual の行・代表の仕入先の (仕入先, SKU) の行があったか)。
--   朝の Company DB から決め直すと、ロードの後に人が直した行で偽の異常になる・ロードの誤りを隠す。
-- なにを: 1 回のロード × section ごとに 1 行 (jsonb)。自由文ではなく理由コードと、書こうとした対象そのもの:
--   skus              = 採用した件数・飛ばした SKU (code・reason_code: empty_code / norm_collision)
--   sku_costs         = 持ち主が load か・飛ばした SKU (reason_code: invalid_cost)
--   set_components    = 持ち主が load か・削除まで行った親・書こうとした行と同じだった行 (manual で数量が同じ行を含む)・
--                       manual と数量が違って飛ばした行・削除で manual を残した行・飛ばした行 (reason_code)
--   primary_suppliers = 付けたか (0027・持ち主)・確かめた後の対象 (SKU → 期待する仕入先。もう正しかったものも)・触らなかった SKU (reason_code)
-- 書くのは夜間ロード (apps/company-db/load/engine.mjs) の取引の中 (巻き戻れば残らない)。0030 が未適用なら書かない (ロードは止めない = 照合は blocked)。
-- 60 日より古い行はロードが消す

create table ops.load_decisions (
  ingest_run_id text not null,
  section       text not null check (section in ('skus', 'sku_costs', 'set_components', 'primary_suppliers')),
  format        text not null,
  payload       jsonb not null,
  recorded_at   timestamptz not null default now(),
  primary key (ingest_run_id, section)
);
create index ix_load_decisions_recorded on ops.load_decisions (recorded_at);
