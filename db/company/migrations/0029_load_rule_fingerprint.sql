-- 0029: 夜間ロードの「規則の指紋・持ち主の設定・実行の条件」を ops.load_materials に残す (2026-09-25。Company DB構想 10 §6.1.1 A4。Codex ③a-2 R0 #4 / R1 M2)
--
-- なぜ: 毎朝の照合の ①ロードの検証は、夜間ロードが読んだ材料 (miniPC の控え) から「ロードの後にあるべき値」を作り直して Company DB と比べる。
--   ロードの後に変換のコードや持ち主の設定が変わると、同じ材料でも作り直した値が変わり、偽の「ロードの誤り」になる。
--   rule_version ('v1') は手で上げる版で、コードの同一性は示さない。
-- なにを (どれも null を許す = 0029 の前の行・指紋を計算できなかった回):
--   rule_fingerprint = 夜間ロードの変換コード (sources.mjs・engine.mjs・material-lineage.js・lib/sku-norm.js・config/master-ownership.mjs) の中身を
--                      決まった順・改行を LF にそろえて sha256 (engine.mjs の起動時に計算 = 動いているコード)。照合は同じ指紋のコードでしか判定しない
--   ownership        = その回の持ち主の設定そのもの (ownership_hash の元)。照合はロードした回の持ち主で対象の列を決める
--   load_conditions  = ロードの分岐に効く条件 (適用済み migration の最新の版・0027 の有無 など)
-- 書くのは夜間ロード (apps/company-db/load/engine.mjs) の取引の中。0029 が未適用の DB では書かない (ロードは止めない)

alter table ops.load_materials
  add column rule_fingerprint text check (rule_fingerprint is null or rule_fingerprint ~ '^[0-9a-f]{64}$'),
  add column ownership        jsonb,
  add column load_conditions  jsonb;
