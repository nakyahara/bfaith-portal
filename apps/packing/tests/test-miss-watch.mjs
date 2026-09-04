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
const { findMisses, pendingMisses, buildMissText, missWatchStep, markNotified } = await import('../miss-watch.js');

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

/** picking 側にバッチを1つ置く (取込経路を通さず直接入れる = 見張りの入力を作るため) */
function addPickBatch({ folder, cls = 'ネコポス手動複数SKU複数個', source = 'txt', ageMin = 60, slips = 5 }) {
  const at = minutesAgo(ageMin);
  const info = getDB().prepare(`
    INSERT INTO pk_batches (tb_no, hikiate_class, class_source, folder_name, work_date, composition,
      line_count, slip_count, total_qty, status, csv_sha256, imported_by, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, '単品', ?, ?, ?, 'ready', ?, 'test', ?, ?)
  `).run(`TB-${folder}-${Math.random()}`, cls, source, folder, TODAY, slips, slips, slips, `sha-${folder}`, at, at);
  return Number(info.lastInsertRowid);
}

/** packing 側に「取り込まれた」印を置く */
function addPackBatch(folder) {
  getDB().prepare(`
    INSERT INTO pk_pack_batches (tb_key, folder_name, work_date, sagyo_date, slip_count, line_count,
      total_qty, match_status, status, csv_sha256, imported_by, created_at, updated_at)
    VALUES (?, ?, ?, ?, 1, 1, 1, 'ok', 'ready', ?, 'test', ?, ?)
  `).run(`PK-${folder}-${Math.random()}`, folder, TODAY, TODAY, `psha-${folder}`, utcNow(), utcNow());
}

// ─── 1. ピッキングにあるのに梱包に無い ───

t('梱包に来ていない出荷グループを見つける', () => {
  addPickBatch({ folder: '出荷_07', ageMin: 60, slips: 33 });
  const misses = findMisses().filter((m) => m.folderName === '出荷_07');
  assert.equal(misses.length, 1);
  assert.equal(misses[0].kind, 'not_imported');
  assert.match(misses[0].detail, /33伝票/);
});

t('梱包にも来ていれば鳴らさない', () => {
  addPickBatch({ folder: '出荷_08', ageMin: 60 });
  addPackBatch('出荷_08');
  assert.equal(findMisses().filter((m) => m.folderName === '出荷_08').length, 0);
});

t('取り込まれたばかりは待つ (猶予内は鳴らさない)', () => {
  addPickBatch({ folder: '出荷_09', ageMin: 5 });
  assert.equal(findMisses().filter((m) => m.folderName === '出荷_09' && m.kind === 'not_imported').length, 0,
    '5分前のバッチは「まだ来ていないだけ」');
  // 猶予を跨いだら鳴る
  assert.equal(findMisses({ graceMin: 1 }).filter((m) => m.folderName === '出荷_09' && m.kind === 'not_imported').length, 1);
});

// ─── 2. 引当分類が推定値のまま ───

t('分類が推定値のまま確定していたら見つける', () => {
  addPickBatch({ folder: '出荷_17', cls: 'ネコポス【梱包機PAS-LINE《3つ折り》】複数SKU複数個', source: 'suggested', ageMin: 60 });
  addPackBatch('出荷_17');   // 梱包には来ている = not_imported では鳴らない
  const misses = findMisses().filter((m) => m.folderName === '出荷_17');
  assert.equal(misses.length, 1);
  assert.equal(misses[0].kind, 'class_suggested');
  assert.match(misses[0].detail, /3つ折り/);
});

t('txt から取れた分類は鳴らさない', () => {
  addPickBatch({ folder: '出荷_18', source: 'txt', ageMin: 60 });
  addPackBatch('出荷_18');
  assert.equal(findMisses().filter((m) => m.folderName === '出荷_18').length, 0);
});

t('古い行 (class_source が NULL) は鳴らさない', () => {
  addPickBatch({ folder: '出荷_19', source: null, ageMin: 60 });
  addPackBatch('出荷_19');
  assert.equal(findMisses().filter((m) => m.folderName === '出荷_19' && m.kind === 'class_suggested').length, 0,
    'v13 より前に取り込んだ行を「推定」と決めつけない');
});

// ─── 3. 鳴らし方 ───

t('同じ件を二度は鳴らさない', () => {
  const before = findMisses().filter((m) => m.folderName === '出荷_07');
  assert.equal(pendingMisses(before).length, 1);
  markNotified(before[0]);
  assert.equal(pendingMisses(findMisses().filter((m) => m.folderName === '出荷_07')).length, 0);
});

t('本文に何をすればよいかが入る', () => {
  const text = buildMissText('not_imported', [{ folderName: '出荷_07', detail: '33伝票' }]);
  assert.match(text, /出荷_07/);
  assert.match(text, /33伝票/);
  assert.match(text, /管理/, '確認先が書いてある');
  const text2 = buildMissText('class_suggested', [{ folderName: '出荷_17', detail: 'ネコポス…' }]);
  assert.match(text2, /推定/);
  assert.match(text2, /違う/, '現物と違いうることが書いてある');
  assert.equal(buildMissText('not_imported', []), null);
});

await ta('送れなかったら「鳴らした」ことにしない (次の周期で再送する)', async () => {
  addPickBatch({ folder: '出荷_20', ageMin: 60 });
  const fail = async () => { throw new Error('webhook down'); };
  await missWatchStep(fail);
  const still = pendingMisses(findMisses().filter((m) => m.folderName === '出荷_20'));
  assert.equal(still.length, 1, '送信に失敗した件は未通知のまま残る');

  const sent = [];   // 種類ごとに1通ずつ来るので配列で受ける
  await missWatchStep(async (text) => { sent.push(text); return true; });
  assert.ok(sent.some((x) => x.includes('出荷_20')), '次の周期で送られる');
  assert.equal(pendingMisses(findMisses().filter((m) => m.folderName === '出荷_20')).length, 0);
});

await ta('通知先が無くても落ちない (取込を止めない)', async () => {
  addPickBatch({ folder: '出荷_21', ageMin: 60 });
  const r = await missWatchStep(null);
  assert.equal(r.notified, 0);
  assert.equal(pendingMisses(findMisses().filter((m) => m.folderName === '出荷_21')).length, 1,
    '鳴らせなかった件は未通知のまま残る');
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
