// H-4 staged sync E2E テスト (一時 DATA_DIR、本番に触れない)
// シナリオ:
//  0. mirror に旧データ (date=2026-07-01, OLD) を single-chunk sync で投入
//  1. multi-chunk run (3 chunks) の chunk 0,1 だけ送信 → live 表は旧データ無傷のはず (staging)
//  2. chunk 2 送信 → 原子的 apply → 旧データ消え新データ 3 行
//  3. chunk 再送 (replay) → 二重 apply しない
//  4. 途中放棄 run の残骸が次 run の is_first で掃除される
import crypto from 'node:crypto';

const BASE = `http://127.0.0.1:${process.env.TEST_PORT || 3999}`;
const ENTITY = 'amazon_finance_sku_daily';

function makeRow(dateJst, sku, units) {
  return {
    date_jst: dateJst, seller_sku: sku, asin: 'B000TEST01',
    units_ordered: units, units_refunded: 0, units_net: units,
    sales_amount_jpy: 1000 * units, refund_amount_jpy: 0,
    net_sales_jpy: 1000 * units, cogs_amount_jpy: 500 * units,
    fee_amount_jpy: 100, ad_spend_jpy: 0,
    gross_margin_jpy: 400 * units,
    cost_status: 'complete',
    source_run_id: 'test-run', source_row_hash: `hash-${sku}-${dateJst}`,
    synced_at: new Date().toISOString(),
  };
}

async function sendChunk(runId, chunkIndex, chunkCount, rows, clearDates) {
  const payload = { rows };
  const body = {
    sync_run_id: runId, contract_version: 1,
    scope_from: '2026-07-01', scope_to: '2026-07-01',
    chunk_index: chunkIndex, chunk_count: chunkCount,
    is_first: chunkIndex === 0, is_last: chunkIndex === chunkCount - 1,
    row_count: rows.length,
    payload_checksum: crypto.createHash('sha256').update(JSON.stringify(payload)).digest('hex'),
    meta: chunkIndex === 0 ? { clear_amazon_finance_dates: clearDates } : {},
    payload,
  };
  const res = await fetch(`${BASE}/apps/mirror/api/sync/${ENTITY}/chunk`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  const json = await res.json().catch(() => ({}));
  return { status: res.status, json };
}

async function countRows() {
  // mirror テーブル直接参照用の確認エンドポイントは無いので better-sqlite3 で直読み
  const { default: Database } = await import('better-sqlite3');
  const path = await import('node:path');
  const db = new Database(path.join(process.env.DATA_DIR, 'warehouse-mirror.db'), { readonly: true });
  const rows = db.prepare(`SELECT seller_sku, units_ordered FROM mirror_amazon_finance_sku_daily WHERE date_jst = '2026-07-01' ORDER BY seller_sku`).all();
  const stage = db.prepare(`SELECT COUNT(*) AS c FROM sync_stage_rows`).get().c;
  const applied = db.prepare(`SELECT COUNT(*) AS c FROM sync_run_applied`).get().c;
  db.close();
  return { rows, stage, applied };
}

let failures = 0;
function check(label, cond, detail) {
  if (cond) console.log(`  ✅ ${label}`);
  else { failures++; console.error(`  ❌ ${label}: ${JSON.stringify(detail)}`); }
}

// ── 0. 旧データ投入 (single-chunk = 従来経路)
let r = await sendChunk('run-old-1', 0, 1, [makeRow('2026-07-01', 'OLD-SKU', 1)], ['2026-07-01']);
check('旧データ single-chunk 投入 200', r.status === 200, r);
let s = await countRows();
check('旧データが live に存在', s.rows.length === 1 && s.rows[0].seller_sku === 'OLD-SKU', s);

// ── 1. multi-chunk run: chunk 0,1 のみ (途中失敗シミュレート)
r = await sendChunk('run-new-1', 0, 3, [makeRow('2026-07-01', 'NEW-A', 2)], ['2026-07-01']);
check('chunk 0 受信 200 (staged)', r.status === 200, r);
r = await sendChunk('run-new-1', 1, 3, [makeRow('2026-07-01', 'NEW-B', 3)]);
check('chunk 1 受信 200 (staged)', r.status === 200, r);
s = await countRows();
check('★途中状態: live は旧データ無傷 (clear されていない)', s.rows.length === 1 && s.rows[0].seller_sku === 'OLD-SKU', s);
check('stage に 2 行溜まっている', s.stage === 2, s);

// ── 2. chunk 2 (最終) → 原子的 apply
const chunk2Rows = [makeRow('2026-07-01', 'NEW-C', 4)];
r = await sendChunk('run-new-1', 2, 3, chunk2Rows);
check('chunk 2 受信 200 completed + staged_apply 報告', r.status === 200 && r.json.status === 'completed' && r.json.staged_apply && r.json.staged_apply.appliedRows === 3, r);
s = await countRows();
check('★apply 後: 旧データ消え新 3 行', s.rows.length === 3 && s.rows.map(x => x.seller_sku).join(',') === 'NEW-A,NEW-B,NEW-C', s);
check('stage が空', s.stage === 0, s);
check('applied マーカー 1 件', s.applied === 1, s);

// ── 3. replay (chunk 2 を同一 payload で再送) → 二重 apply しない
r = await sendChunk('run-new-1', 2, 3, chunk2Rows);
check('replay 200 + replayed', r.status === 200 && r.json.replayed === true, r);
s = await countRows();
check('replay 後も 3 行のまま (二重 apply なし)', s.rows.length === 3, s);

// ── 4. 途中放棄 run が居ても並行 run は壊れない (24h 以内の stage は掃除されない)
r = await sendChunk('run-abandoned', 0, 3, [makeRow('2026-07-01', 'ABANDON-A', 1)], ['2026-07-01']);
check('放棄 run chunk 0 受信', r.status === 200, r);
s = await countRows();
check('放棄 run が stage に 1 行', s.stage === 1, s);
r = await sendChunk('run-new-2', 0, 2, [makeRow('2026-07-01', 'NEW2-A', 5)], ['2026-07-01']);
check('次 run chunk 0 受信', r.status === 200, r);
s = await countRows();
check('★24h 以内の他 run stage は保持される (TTL 掃除)', s.stage === 2, s);
check('live はまだ前 run の 3 行 (次 run 未完のため)', s.rows.length === 3, s);
r = await sendChunk('run-new-2', 1, 2, [makeRow('2026-07-01', 'NEW2-B', 6)]);
check('次 run 完了 → live が 2 行に置換', r.status === 200, r);
s = await countRows();
check('★置換確認', s.rows.length === 2 && s.rows.map(x => x.seller_sku).join(',') === 'NEW2-A,NEW2-B', s);

// ── 5. row_count=0 の空 chunk を含む run も apply できる (staged フラグがカバレッジの正)
r = await sendChunk('run-empty-mid', 0, 3, [makeRow('2026-07-01', 'EM-A', 1)], ['2026-07-01']);
check('空chunk run: chunk 0 受信', r.status === 200, r);
r = await sendChunk('run-empty-mid', 1, 3, []);
check('空chunk run: chunk 1 (0行) 受信', r.status === 200, r);
r = await sendChunk('run-empty-mid', 2, 3, [makeRow('2026-07-01', 'EM-B', 2)]);
check('★空chunk run: 完了 + apply (2行)', r.status === 200 && r.json.status === 'completed' && r.json.staged_apply && r.json.staged_apply.appliedRows === 2, r);
s = await countRows();
check('★空chunk run 置換確認', s.rows.length === 2 && s.rows.map(x => x.seller_sku).join(',') === 'EM-A,EM-B', s);

console.log(failures === 0 ? '\n=== H-4 E2E ALL PASS ===' : `\n=== ${failures} FAILURES ===`);
process.exit(failures === 0 ? 0 : 1);
