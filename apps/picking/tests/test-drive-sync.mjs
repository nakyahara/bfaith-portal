/**
 * picking — Drive自動ポーリング (pollOnce) のテスト。Drive API は deps 注入で差し替え。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import iconv from 'iconv-lite';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'picking-test-'));

const { pollOnce, pickPollCandidates, patternFromTxtName } = await import('../drive-sync.js');
const { initPickingDB, getDB, listBatches, jstToday } = await import('../db.js');

initPickingDB();

let passed = 0;
function t(name, fn) { fn(); passed++; console.log(`  ok: ${name}`); }

// ─── 純関数 ───

t('patternFromTxtName: 引当パターンtxtからパターン名を抽出', () => {
  assert.equal(patternFromTxtName('引当パターン_ネコポス手動単品.txt'), 'ネコポス手動単品');
  assert.equal(patternFromTxtName('引当パターン_AES《単品》.txt'), 'AES《単品》');
  assert.equal(patternFromTxtName('ピッキングリストデータ_1.csv'), null);
  assert.equal(patternFromTxtName(null), null);
});

t('pickPollCandidates: 安定した3日以内のピッキングリストCSVだけ', () => {
  const now = Date.now();
  const iso = (msAgo) => new Date(now - msAgo).toISOString();
  const files = [
    { filename: 'ピッキングリストデータ_1.csv', modified_time: iso(120_000) },   // OK
    { filename: 'ピッキングリストデータ_2.csv', modified_time: iso(10_000) },    // 書き込み直後 → 待つ
    { filename: 'ピッキングリスト_3.pdf', modified_time: iso(120_000) },         // PDFは対象外 (Phase 2)
    { filename: 'okurijo_nekoposu_1.csv', modified_time: iso(120_000) },         // 送り状CSVは対象外
    { filename: 'ピッキングリストデータ_4.csv', modified_time: iso(4 * 24 * 3600 * 1000) }, // 3日超
    { filename: 'ピッキングリストデータ_5.csv', modified_time: null },           // 時刻不明
  ];
  assert.deepEqual(pickPollCandidates(files, now).map((f) => f.filename), ['ピッキングリストデータ_1.csv']);
});

// ─── pollOnce (Drive注入) ───

const HEADERS = [
  '出荷指示日', 'ブロック略称', 'ロケーション', '商品ID', '商品名', '出荷指示数', '出荷引当数',
  'ピッキングNO', '出荷伝票NO', '荷主出荷NO', 'バーコード',
  '送り状発行ソフト名', '配送方法名', 'トータルピッキングバッチ番号',
];
function makeCsv(rows) {
  const q = (v) => `"${String(v ?? '').replace(/"/g, '""')}"`;
  const lines = [HEADERS.map(q).join(',')];
  for (const r of rows) lines.push(HEADERS.map((h) => q(r[h] ?? '')).join(','));
  return iconv.encode(lines.join('\r\n') + '\r\n', 'Shift_JIS');
}
const csvBuf = makeCsv([{
  '出荷指示日': '20260813', 'ブロック略称': 'P3FB', 'ロケーション': '00201604',
  '商品ID': 'sku-a', '商品名': '商品A', '出荷指示数': '1', '出荷引当数': '1',
  'ピッキングNO': 'PC1', '出荷伝票NO': 'SP1', '荷主出荷NO': '100',
  'バーコード': 'X1', '送り状発行ソフト名': 'B2(Ver6.0)',
  '配送方法名': 'ネコポス 陸便 元払い 営業所止めなし', 'トータルピッキングバッチ番号': 'TBPOLL1',
}]);

const stableTime = new Date(Date.now() - 300_000).toISOString();
const deps = {
  getFolders: async () => [{ folder_id: 'root', name: '(出荷_no直下)' }, { folder_id: 'f30', name: '出荷_30' }],
  listFiles: async ({ nameContains }) => {
    if (nameContains === '引当パターン_') {
      return [{ file_id: 'txt1', filename: '引当パターン_ネコポス手動単品.txt', parent_name: '出荷_30', modified_time: stableTime }];
    }
    return [{ file_id: 'csv1', filename: 'ピッキングリストデータ_30.csv', parent_name: '出荷_30', modified_time: stableTime }];
  },
  download: async () => ({ buffer: csvBuf, filename: 'ピッキングリストデータ_30.csv' }),
};

{
  const stats = await pollOnce(deps);
  assert.equal(stats.imported, 1);
  assert.equal(stats.failed, 0);
  const batch = listBatches(jstToday()).find((b) => b.tb_no === 'TBPOLL1');
  assert.ok(batch, 'バッチが作られる');
  assert.equal(batch.folder_name, '出荷_30');
  assert.equal(batch.hikiate_class, 'ネコポス手動単品', 'パターンtxtから分類が入る');
  assert.equal(batch.imported_by, 'drive-poller');
  const ledger = getDB().prepare("SELECT * FROM pk_drive_imports WHERE drive_file_id='csv1'").get();
  assert.equal(ledger.status, 'imported');
  console.log('  ok: pollOnce: CSV取込+パターンtxt分類+台帳記録 (async)');
}

{
  // 同じ版の再ポーリングはダウンロードすら走らない (台帳スキップ)
  let downloads = 0;
  const stats = await pollOnce({ ...deps, download: async () => { downloads++; return { buffer: csvBuf }; } });
  assert.equal(stats.imported + stats.replayed + stats.failed, 0);
  assert.equal(downloads, 0);
  console.log('  ok: pollOnce: 取込済みの版はスキップ (台帳冪等) (async)');
}

{
  // 作業開始後にファイルが更新されても上書きしない (skipped)
  getDB().prepare("UPDATE pk_batches SET status='picking', worker='星' WHERE tb_no='TBPOLL1'").run();
  const newTime = new Date(Date.now() - 200_000).toISOString();
  const changedCsv = makeCsv([{
    '出荷指示日': '20260813', 'ブロック略称': 'P3FB', 'ロケーション': '00201604',
    '商品ID': 'sku-a', '商品名': '商品A変更', '出荷指示数': '2', '出荷引当数': '2',
    'ピッキングNO': 'PC1', '出荷伝票NO': 'SP1', '荷主出荷NO': '100',
    'バーコード': 'X1', '送り状発行ソフト名': 'B2(Ver6.0)',
    '配送方法名': 'ネコポス 陸便 元払い 営業所止めなし', 'トータルピッキングバッチ番号': 'TBPOLL1',
  }]);
  const stats = await pollOnce({
    ...deps,
    listFiles: async ({ nameContains }) => nameContains === '引当パターン_' ? [] : [
      { file_id: 'csv1', filename: 'ピッキングリストデータ_30.csv', parent_name: '出荷_30', modified_time: newTime },
    ],
    download: async () => ({ buffer: changedCsv }),
  });
  assert.equal(stats.skipped, 1);
  assert.equal(stats.failed, 0);
  const ledger = getDB().prepare("SELECT status FROM pk_drive_imports WHERE drive_file_id='csv1' AND modified_time=?").get(newTime);
  assert.equal(ledger.status, 'skipped');
  console.log('  ok: pollOnce: 作業開始後の内容変更は上書きせずskipped (async)');
}

{
  // 壊れたCSVは failed 台帳に残り、他の取込は続く
  const stats = await pollOnce({
    ...deps,
    listFiles: async ({ nameContains }) => nameContains === '引当パターン_' ? [] : [
      { file_id: 'bad1', filename: 'ピッキングリストデータ_31.csv', parent_name: '出荷_31', modified_time: stableTime },
    ],
    download: async () => ({ buffer: Buffer.from('broken') }),
  });
  assert.equal(stats.failed, 1);
  const ledger = getDB().prepare("SELECT * FROM pk_drive_imports WHERE drive_file_id='bad1'").get();
  assert.equal(ledger.status, 'failed');
  assert.ok(ledger.error);
  console.log('  ok: pollOnce: 壊れたCSVはfailed台帳 (黙殺しない) (async)');
}

console.log(`\ntest-drive-sync: ${passed + 4} 件 pass`);
