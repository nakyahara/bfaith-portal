/** test-miss-stats.mjs — ピッキングミス集計 (2026-08-21) の検証 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'picking-miss-test-'));
process.env.PICKING_STATS_MIN_LINES = '10';
process.env.PICKING_STATS_MIN_DATE = '2026-01-01';   // 窓の検証で床止めに当たらないように
const { initPickingDB, getDB, jstToday } = await import('../db.js');
const { getMissStats } = await import('../service.js');
initPickingDB();
const db = getDB();
let passed = 0;
const t = (name, fn) => { fn(); passed++; console.log(`  ok: ${name}`); };

t('packing未初期化 (表なし) でも空で返る', () => {
  const m = getMissStats({});
  assert.equal(m.total.total, 0);
});

// packing所有の pk_pack_incidents を試験用に用意
db.exec(`CREATE TABLE pk_pack_incidents (
  id INTEGER PRIMARY KEY, batch_id INTEGER, slip_seq INTEGER, kind TEXT, sku TEXT, actual_sku TEXT,
  qty INTEGER, status TEXT, attributed_worker TEXT, detected_by TEXT, confirmed_by TEXT,
  created_at TEXT, updated_at TEXT)`);
const now = new Date().toISOString().slice(0, 19) + 'Z';
const old = new Date(Date.now() - 40 * 864e5).toISOString().slice(0, 19) + 'Z';
const ins = db.prepare(`INSERT INTO pk_pack_incidents (kind, sku, qty, status, attributed_worker, detected_by, created_at, updated_at)
  VALUES (?, 'sku1', ?, ?, ?, '梱包A', ?, ?)`);
ins.run('shortage', 1, 'confirmed', '星', now, now);
ins.run('shortage', 2, 'confirmed', '星', now, now);
ins.run('wrong_item', 1, 'confirmed', '星', now, now);
ins.run('excess', 1, 'confirmed', '倉田', now, now);
ins.run('shortage', 1, 'withdrawn', '星', now, now);       // 取下げ=数えない
ins.run('shortage', 1, 'candidate', '星', now, now);        // 未確定=数えない
ins.run('shortage', 1, 'confirmed', '星', old, old);        // 窓外=数えない
ins.run('shortage', 1, 'confirmed', null, now, now);        // 帰責不明

t('確定分のみ・作業者×種別で集計 (取下げ/未確定/窓外は除外)', () => {
  const m = getMissStats({ until: jstToday(), days: 30 });
  const hoshi = m.byWorker.find((w) => w.worker === '星');
  assert.deepEqual([hoshi.total, hoshi.shortage, hoshi.wrong_item, hoshi.qty], [3, 2, 1, 4]);
  const kurata = m.byWorker.find((w) => w.worker === '倉田');
  assert.deepEqual([kurata.total, kurata.excess], [1, 1]);
  assert.ok(m.byWorker.find((w) => w.worker === '(担当不明)'), '帰責不明も見える');
  assert.equal(m.total.total, 5);
  assert.equal(m.byWorker[0].worker, '星', '分母が無いときは件数降順');
  assert.equal(hoshi.per1000, null, '分母ゼロは比率なし');
});

// ── 比率 (分母 = 期間内にピッキングした明細数) と 欠品由来の除外 (2026-08-31) ──
const today = jstToday();
const utcNowStr = () => new Date().toISOString().slice(0, 19) + 'Z';
const mkPick = (id, tb, worker) => {
  db.prepare(`INSERT INTO pk_batches (id, tb_no, hikiate_class, folder_name, work_date, composition, line_count, slip_count, total_qty,
      status, validity, worker, csv_sha256, imported_by, created_at, updated_at, started_at, finished_at)
    VALUES (?, ?, 'AES《単品》', '出荷_9', ?, '単品', 0, 0, 0, 'done', 'valid', ?, ?, 'test', ?, ?, ?, ?)`)
    .run(id, tb, today, worker, `sha${id}`, now, now, now, now);
};
const addLine = (batchId, seq, sku, status = 'done', remaining = null) => {
  db.prepare(`INSERT INTO pk_lines (batch_id, seq, location, block, sku, product_name, barcode, qty, status, shown_at, done_at, remaining_qty)
    VALUES (?, ?, '001-001-01', 'P3FA', ?, 'x', NULL, 1, ?, ?, ?, ?)`).run(batchId, seq, sku, status, now, now, remaining);
};
mkPick(101, 'TB-M1', '星');   // 星: 明細 20 (うち1件は欠品 later)
for (let i = 1; i <= 19; i++) addLine(101, i, `s${i}`);
addLine(101, 20, 'kekpin', 'shortage', 1);
mkPick(102, 'TB-M2', '倉田'); // 倉田: 明細 5 (参考値)
for (let i = 1; i <= 5; i++) addLine(102, i, `k${i}`);
// packing 側 (バッチ・伝票) を試験用に用意して、欠品由来の不足を1件作る
db.exec(`CREATE TABLE pk_pack_batches (id INTEGER PRIMARY KEY, tb_key TEXT, pk_batch_id INTEGER)`);
db.exec(`CREATE TABLE pk_pack_slips (id INTEGER PRIMARY KEY, batch_id INTEGER, seq INTEGER, ne_slip_no TEXT)`);
db.prepare('INSERT INTO pk_pack_batches (id, tb_key) VALUES (501, ?)').run('TB-M1');
db.prepare("INSERT INTO pk_pack_slips (batch_id, seq, ne_slip_no) VALUES (501, 7, 'NE-7'), (501, 8, 'NE-8'), (501, 9, 'NE-9')").run();
const insFull = db.prepare(`INSERT INTO pk_pack_incidents (batch_id, slip_seq, kind, sku, qty, status, attributed_worker, detected_by, created_at, updated_at)
  VALUES (?, ?, ?, ?, 1, 'confirmed', ?, '梱包A', ?, ?)`);
const n2 = utcNowStr();
insFull.run(501, 7, 'shortage', 'kekpin', '星', n2, n2);   // ① 配賦あり → 欠品由来
db.prepare(`INSERT INTO pk_shortage_allocations (batch_id, line_seq, sku, ne_slip_no, qty, kind, created_at) VALUES (101, 20, 'kekpin', 'NE-7', 1, 'later', ?)`).run(n2);
insFull.run(501, 8, 'shortage', 'kekpin', '星', n2, n2);   // ② 配賦記録のあるバッチで、配賦されていない受注の不足 → 本当のミス (取り忘れ)
insFull.run(501, 9, 'shortage', 's3', '星', n2, n2);       // ③ 欠品記録なし → ミス
// 配賦記録を持たない古いバッチ (2026-08-31 以前): 同バッチ同SKUの欠品 (残りあり) があれば欠品由来とみなす (規則 b)
mkPick(105, 'TB-M5', '星');
for (let i = 1; i <= 4; i++) addLine(105, i, `o${i}`);
addLine(105, 5, 'oldkp', 'shortage', 1);
db.prepare('INSERT INTO pk_pack_batches (id, tb_key) VALUES (505, ?)').run('TB-M5');
db.prepare("INSERT INTO pk_pack_slips (batch_id, seq, ne_slip_no) VALUES (505, 1, 'NE-505-1')").run();
insFull.run(505, 1, 'shortage', 'oldkp', '星', n2, n2);   // ④ 旧バッチ・同SKU欠品あり → 欠品由来

t('比率 = 1,000明細あたり・欠品由来の不足はミスから除外して stockout に', () => {
  const m = getMissStats({ until: jstToday(), days: 30 });
  const hoshi = m.byWorker.find((w) => w.worker === '星');
  assert.equal(hoshi.lines, 25, '分母 = ピッキングした明細数 (欠品で止めた明細も含む)');
  assert.equal(hoshi.stockout, 2, '欠品由来 2件 (①配賦一致 + ④旧バッチの同SKU欠品)');
  assert.equal(hoshi.total, 3 + 2, 'ミス = 既存3 + ②③ (欠品由来は数えない)');
  assert.equal(hoshi.per1000, 200, '5件 / 25明細 = 200 /1000明細');
  assert.equal(hoshi.provisional, false, '分母が minLines 以上なら本値');
  const kurata = m.byWorker.find((w) => w.worker === '倉田');
  assert.equal(kurata.lines, 5);
  assert.equal(kurata.per1000, 200, '1件 / 5明細');
  assert.equal(kurata.provisional, true, '分母不足は参考値');
  assert.equal(m.total.stockout, 2);
  assert.equal(m.total.lines, 30);
  assert.equal(m.byWorker[0].per1000, 200, '比率の高い順');
});

// ── 帰属 = 明細を実際にピッキングした人 / 日付 = ピッキングバッチの作業日 (Codex R1 High×2) ──
t('交代バッチ: ミスは attributed_worker (最終担当) ではなく、その明細を取った人に付く', () => {
  mkPick(103, 'TB-M3', '倉田');                       // 最終担当 = 倉田 (交代後)
  for (let i = 1; i <= 12; i++) addLine(103, i, `m${i}`);
  // 明細 3 (SKU m3) は交代前の 星 が取った (pk_events の最後の next の担当)
  db.prepare(`INSERT INTO pk_events (op_id, batch_id, worker, event, line_seq, result_json, at) VALUES ('opx1', 103, '星', 'next', 3, '{}', ?)`).run(now);
  db.prepare('INSERT INTO pk_pack_batches (id, tb_key) VALUES (503, ?)').run('TB-M3');
  db.prepare("INSERT INTO pk_pack_slips (batch_id, seq, ne_slip_no) VALUES (503, 1, 'NE-503-1')").run();
  const before = getMissStats({ until: jstToday(), days: 30 });
  const h0 = before.byWorker.find((w) => w.worker === '星').total;
  const k0 = before.byWorker.find((w) => w.worker === '倉田').total;
  insFull.run(503, 1, 'shortage', 'm3', '倉田', n2, n2);  // 確定時の帰責は倉田だが…
  const m = getMissStats({ until: jstToday(), days: 30 });
  assert.equal(m.byWorker.find((w) => w.worker === '星').total, h0 + 1, '取った人 (星) に付く');
  assert.equal(m.byWorker.find((w) => w.worker === '倉田').total, k0, '倉田には付かない');
});

t('時間軸: 梱包で見つかった日ではなく、ピッキングバッチの作業日で期間に入れる', () => {
  const oldDay = new Date(Date.now() - 40 * 864e5).toISOString().slice(0, 10);
  db.prepare(`INSERT INTO pk_batches (id, tb_no, hikiate_class, folder_name, work_date, composition, line_count, slip_count, total_qty,
      status, validity, worker, csv_sha256, imported_by, created_at, updated_at, started_at, finished_at)
    VALUES (104, 'TB-M4', 'AES《単品》', '出荷_9', ?, '単品', 0, 0, 0, 'done', 'valid', '星', 'sha104', 'test', ?, ?, ?, ?)`)
    .run(oldDay, old, old, old, old);
  addLine(104, 1, 'z1');
  db.prepare('INSERT INTO pk_pack_batches (id, tb_key) VALUES (504, ?)').run('TB-M4');
  db.prepare("INSERT INTO pk_pack_slips (batch_id, seq, ne_slip_no) VALUES (504, 1, 'NE-504-1')").run();
  const before30 = getMissStats({ until: jstToday(), days: 30 }).total.total;
  const before60 = getMissStats({ until: jstToday(), days: 60 }).total.total;   // (最初の節の 40 日前の incident を含む)
  insFull.run(504, 1, 'shortage', 'z1', '星', n2, n2);   // 今日確定したが、ピッキングは 40 日前
  assert.equal(getMissStats({ until: jstToday(), days: 30 }).total.total, before30, '40日前のバッチのミスは 30 日窓に入らない (分母も入っていない)');
  assert.equal(getMissStats({ until: jstToday(), days: 60 }).total.total, before60 + 1, '60 日窓なら入る');
});

t('梱包バッチが突合した pk_batch_id を優先する (tb_key が tb_no と一致しなくても引ける — Codex R4)', () => {
  // pk_batches.tb_no は UNIQUE なので同一 tb_no の重複は起きない。tb_key が複数TBの連結などで
  // tb_no 検索に掛からないケースで、取込時の突合 (pk_batch_id) から引けることを確認する
  mkPick(106, 'TB-ONLYID', '星');
  addLine(106, 1, 'd1');
  db.prepare('INSERT INTO pk_pack_batches (id, tb_key, pk_batch_id) VALUES (506, ?, 106)').run('TB-ONLYID,TB-OTHER');
  db.prepare("INSERT INTO pk_pack_slips (batch_id, seq, ne_slip_no) VALUES (506, 1, 'NE-506-1')").run();
  const h0 = getMissStats({ until: jstToday(), days: 30 }).byWorker.find((w) => w.worker === '星').total;
  insFull.run(506, 1, 'shortage', 'd1', '倉田', n2, n2);   // 確定時の帰責は倉田だが、明細を取ったのは星
  const m = getMissStats({ until: jstToday(), days: 30 });
  assert.equal(m.byWorker.find((w) => w.worker === '星').total, h0 + 1, 'pk_batch_id (106=星) から明細の担当を引く');
});

t('分母には表示/完了時刻の無い明細も入る (Codex R5)', () => {
  const before = getMissStats({ until: jstToday(), days: 30 }).byWorker.find((w) => w.worker === '星').lines;
  db.prepare(`INSERT INTO pk_lines (batch_id, seq, location, block, sku, product_name, barcode, qty, status, shown_at, done_at)
    VALUES (101, 99, '001-001-01', 'P3FA', 'notime', 'x', NULL, 1, 'done', NULL, NULL)`).run();
  const after = getMissStats({ until: jstToday(), days: 30 }).byWorker.find((w) => w.worker === '星').lines;
  assert.equal(after, before + 1, '時刻が無い完了明細も分母に数える');
});

try { fs.rmSync(process.env.DATA_DIR, { recursive: true, force: true }); } catch { /* 無視 */ }
console.log(`\ntest-miss-stats: ${passed} 件 pass`);
