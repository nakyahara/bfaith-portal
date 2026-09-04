/**
 * 取りこぼしの見張りのテスト (2026-09-04 障害の再発防止)。
 *   node apps/packing/tests/test-miss-watch.mjs
 *
 * 見張るのは「ピッキングにあるのに梱包に無い」と「引当分類が推定値のまま確定」の2つ。
 * どちらも当日は誰も気づけず、現場の申告まで約2時間かかった。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'packing-miss-'));

const { initPackingDB, getDB, utcNow, jstToday } = await import('../db.js');
const { initPickingDB } = await import('../../picking/db.js');
const {
  findMisses, recordMisses, pendingAlerts, markNotified, buildMissText, missWatchStep,
} = await import('../miss-watch.js');

initPickingDB();
initPackingDB();

let passed = 0;
function t(name, fn) {
  try { fn(); console.log(`✅ ${name}`); passed++; } catch (e) { console.error(`❌ ${name}\n   ${e.message}`); process.exitCode = 1; }
}
async function ta(name, fn) {
  try { await fn(); console.log(`✅ ${name}`); passed++; } catch (e) { console.error(`❌ ${name}\n   ${e.message}`); process.exitCode = 1; }
}

const TODAY = jstToday();
const minutesAgo = (m) => new Date(Date.now() - m * 60000).toISOString().replace(/\.\d+Z$/, 'Z');
let seq = 0;

/** picking 側にバッチを1つ置く (取込経路を通さず直接入れる = 見張りの入力を作るため) */
function addPickBatch({ folder, cls = 'ネコポス手動複数SKU複数個', source = 'txt', ageMin = 60,
  slips = 5, workDate = TODAY, validity = 'valid', status = 'ready' } = {}) {
  const at = minutesAgo(ageMin);
  const tbNo = `TB-${folder}-${++seq}`;
  const info = getDB().prepare(`
    INSERT INTO pk_batches (tb_no, hikiate_class, class_source, folder_name, work_date, composition,
      line_count, slip_count, total_qty, status, validity, csv_sha256, imported_by, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, '単品', ?, ?, ?, ?, ?, ?, 'test', ?, ?)
  `).run(tbNo, cls, source, folder, workDate, slips, slips, slips, status, validity, `sha-${tbNo}`, at, at);
  return { id: Number(info.lastInsertRowid), tbNo, folder };
}

/** packing 側に「取り込まれた」印を置く (突合は pk_batch_id / tb_key) */
function addPackBatch(pick, { validity = 'valid' } = {}) {
  getDB().prepare(`
    INSERT INTO pk_pack_batches (tb_key, pk_batch_id, folder_name, work_date, sagyo_date, slip_count,
      line_count, total_qty, match_status, status, validity, csv_sha256, imported_by, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, 1, 1, 1, 'ok', 'ready', ?, ?, 'test', ?, ?)
  `).run(pick.tbNo, pick.id, pick.folder, TODAY, TODAY, validity, `psha-${pick.tbNo}`, utcNow(), utcNow());
}

const forFolder = (f, opts) => findMisses(opts).filter((m) => m.folderName === f);

// ─── 1. ピッキングにあるのに梱包に無い ───

t('梱包に来ていない出荷グループを見つける', () => {
  addPickBatch({ folder: '出荷_07', ageMin: 60, slips: 33 });
  const misses = forFolder('出荷_07');
  assert.equal(misses.length, 1);
  assert.equal(misses[0].kind, 'not_imported');
  assert.match(misses[0].detail, /33伝票/);
});

t('梱包にも来ていれば鳴らさない', () => {
  addPackBatch(addPickBatch({ folder: '出荷_08', ageMin: 60 }));
  assert.equal(forFolder('出荷_08').length, 0);
});

t('取り込まれたばかりは待つ (猶予内は鳴らさない)', () => {
  addPickBatch({ folder: '出荷_09', ageMin: 5 });
  assert.equal(forFolder('出荷_09').filter((m) => m.kind === 'not_imported').length, 0,
    '5分前のバッチは「まだ来ていないだけ」');
  assert.equal(forFolder('出荷_09', { graceMin: 1 }).filter((m) => m.kind === 'not_imported').length, 1,
    '猶予を跨いだら鳴る');
});

t('取消・無効化されたバッチは鳴らさない', () => {
  addPickBatch({ folder: '出荷_40', ageMin: 60, status: 'cancelled' });
  addPickBatch({ folder: '出荷_41', ageMin: 60, validity: 'invalid' });
  assert.equal(forFolder('出荷_40').length, 0, '取消済みは対象外');
  assert.equal(forFolder('出荷_41').length, 0, '差し替えで無効になった行は対象外');
});

t('同名フォルダの別バッチを「来ている」と見なさない', () => {
  // フォルダ名は日をまたいで使い回される表示用の名前。名前だけで突合すると欠落を見逃す
  const yesterday = addPickBatch({ folder: '出荷_42', ageMin: 60, workDate: '2020-01-01' });
  addPackBatch(yesterday);                                  // 昔の 出荷_42 は梱包済み
  addPickBatch({ folder: '出荷_42', ageMin: 60 });           // 今日の 出荷_42 は未取込
  assert.equal(forFolder('出荷_42').filter((m) => m.kind === 'not_imported').length, 1,
    '今日のぶんは別バッチとして鳴る');
});

t('梱包側が無効化されていたら「来ている」と見なさない', () => {
  addPackBatch(addPickBatch({ folder: '出荷_43', ageMin: 60 }), { validity: 'invalid' });
  assert.equal(forFolder('出荷_43').filter((m) => m.kind === 'not_imported').length, 1);
});

// ─── 2. 引当分類が推定値のまま ───

t('分類が推定値のまま確定していたら見つける', () => {
  const p = addPickBatch({ folder: '出荷_17', cls: 'ネコポス【梱包機PAS-LINE《3つ折り》】複数SKU複数個', source: 'suggested', ageMin: 60 });
  addPackBatch(p);   // 梱包には来ている = not_imported では鳴らない
  const misses = forFolder('出荷_17');
  assert.equal(misses.length, 1);
  assert.equal(misses[0].kind, 'class_suggested');
  assert.match(misses[0].detail, /3つ折り/);
});

t('txt から取れた分類は鳴らさない', () => {
  addPackBatch(addPickBatch({ folder: '出荷_18', source: 'txt', ageMin: 60 }));
  assert.equal(forFolder('出荷_18').length, 0);
});

t('古い行 (class_source が NULL) は鳴らさない', () => {
  addPackBatch(addPickBatch({ folder: '出荷_19', source: null, ageMin: 60 }));
  assert.equal(forFolder('出荷_19').filter((m) => m.kind === 'class_suggested').length, 0,
    'v13 より前に取り込んだ行を「推定」と決めつけない');
});

// ─── 3. 鳴らし方 (outbox) ───

t('検知した時点で outbox に残る (送信前でも消えない)', () => {
  const before = pendingAlerts().length;
  recordMisses(forFolder('出荷_07'));
  const rows = pendingAlerts().filter((r) => r.folder_name === '出荷_07');
  assert.equal(rows.length, 1, '未送信行として残る');
  assert.ok(pendingAlerts().length > before);
});

t('同じ件を二度は鳴らさない', () => {
  const key = pendingAlerts().find((r) => r.folder_name === '出荷_07').alert_key;
  markNotified(key);
  assert.equal(pendingAlerts().filter((r) => r.folder_name === '出荷_07').length, 0);
  recordMisses(forFolder('出荷_07'));   // もう一度検知しても
  assert.equal(pendingAlerts().filter((r) => r.folder_name === '出荷_07').length, 0, '鳴らし直さない');
});

t('本文に何をすればよいかが入る', () => {
  const text = buildMissText('not_imported', [{ folderName: '出荷_07', workDate: TODAY, detail: '33伝票' }]);
  assert.match(text, /出荷_07/);
  assert.match(text, /33伝票/);
  assert.match(text, /管理/, '確認先が書いてある');
  const text2 = buildMissText('class_suggested', [{ folderName: '出荷_17', workDate: TODAY, detail: 'ネコポス…' }]);
  assert.match(text2, /推定/);
  assert.match(text2, /違う/, '現物と違いうることが書いてある');
  assert.equal(buildMissText('not_imported', []), null);
});

await ta('送れなかったら「鳴らした」ことにしない (次の周期で再送する)', async () => {
  addPickBatch({ folder: '出荷_20', ageMin: 60 });
  await missWatchStep(async () => { throw new Error('webhook down'); });
  const still = pendingAlerts().filter((r) => r.folder_name === '出荷_20');
  assert.equal(still.length, 1, '送信に失敗した件は未送信のまま残る');
  assert.ok(still[0].attempts >= 1, '試行回数が記録される');
  assert.match(still[0].last_error ?? '', /webhook down/);

  const sent = [];
  await missWatchStep(async (text) => { sent.push(text); return true; });
  assert.ok(sent.some((x) => x.includes('出荷_20')), '次の周期で送られる');
  assert.equal(pendingAlerts().filter((r) => r.folder_name === '出荷_20').length, 0);
});

await ta('通知先が無くても異常は残る (取込を止めない)', async () => {
  addPickBatch({ folder: '出荷_21', ageMin: 60 });
  const r = await missWatchStep(null);
  assert.equal(r.notified, 0);
  assert.equal(pendingAlerts().filter((x) => x.folder_name === '出荷_21').length, 1,
    '鳴らせなくても outbox には残る');
});

await ta('日付を跨いだ未送信も拾い直す', async () => {
  // webhook が落ちたまま日を跨ぐと、当日分しか探さない実装では永久に見失う
  getDB().prepare(`INSERT INTO pk_pack_miss_alerts (alert_key, kind, work_date, folder_name, detail, attempts, created_at)
    VALUES ('old:not_imported:出荷_99', 'not_imported', date('now','-1 day'), '出荷_99', '昨日の未送信', 3, ?)`).run(utcNow());
  const sent = [];
  await missWatchStep(async (text) => { sent.push(text); return true; });
  assert.ok(sent.some((x) => x.includes('出荷_99')), '前日の未送信が送られる');
  assert.equal(pendingAlerts().filter((x) => x.folder_name === '出荷_99').length, 0);
});

await ta('種類ごとに1通にまとめる', async () => {
  addPickBatch({ folder: '出荷_22', ageMin: 60 });
  addPickBatch({ folder: '出荷_23', ageMin: 60 });
  const texts = [];
  await missWatchStep(async (text) => { texts.push(text); return true; });
  const notImported = texts.find((x) => x.includes('来ていない'));
  assert.ok(notImported, 'not_imported の通知がある');
  assert.ok(notImported.includes('出荷_22') && notImported.includes('出荷_23'), '2件が1通にまとまる');
});

console.log(`test-miss-watch: ${passed} 件 pass`);
