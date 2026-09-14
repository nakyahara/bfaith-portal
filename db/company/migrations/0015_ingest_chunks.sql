-- 0015: 取込 chunk の受領記録 = ops.ingest_chunks (Company DB構想 08 §4.7 / §9 D5a。PR #1336 Codex R1 #4)
--
-- miniPC → Render の push は 1 run (ops.ingest_runs) を chunk に分けて送る。応答を失った送り手が同じ chunk を再送したとき、
-- 伝票の適用は冪等 (apply_* が 'same' を返す) でも run の集計 (rows_seen 等) は二重に数えてしまう。そこで
-- (run_id, chunk_index) ごとに「受け取った内容の指紋 (Render が受け取った rows から計算) と送り返した応答」を残し、
--   - 同じ chunk_index に同じ指紋 → 適用せず保存した応答をそのまま返す (replay)
--   - 同じ chunk_index に違う指紋 → 拒む (送り手のバグ = 黙って上書きしない)
-- run を閉じる (success / partial) のは「最後の chunk (last=true) が届き、0〜最後までの chunk が全部そろった」ときだけ。
-- 🚨 append-only (訂正は新しい run で)。0004〜0014 の表には触らない。
create table ops.ingest_chunks (
  ingest_run_id    text not null references ops.ingest_runs,
  chunk_index      integer not null check (chunk_index >= 0),
  payload_checksum text not null,                      -- Render が受け取った rows の sha256 (送り手の値は使わない)
  rows_seen        integer not null check (rows_seen >= 0),
  rows_applied     integer not null default 0 check (rows_applied >= 0),
  rows_same        integer not null default 0 check (rows_same >= 0),
  rows_stale       integer not null default 0 check (rows_stale >= 0),
  rows_failed      integer not null default 0 check (rows_failed >= 0),
  result           jsonb not null,                     -- 送り返した応答 (再送にはこれを返す)
  received_at      timestamptz not null default now(),
  primary key (ingest_run_id, chunk_index),
  constraint ck_ingest_chunks_sum check (rows_seen = rows_applied + rows_same + rows_stale + rows_failed)
);
select core.make_append_only('ops', 'ingest_chunks');
