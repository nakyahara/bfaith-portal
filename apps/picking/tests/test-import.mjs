/**
 * picking PR1 — CS03002 解析・集約・分類推定・取込冪等のテスト。
 * fixture は実データ (2026-08-11 採取の CS03002) の構造を必須列だけで再現する
 * (個人情報列はそもそも解析対象外のため fixture に含めない)。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import iconv from 'iconv-lite';

// DB を一時ディレクトリへ (モジュール読み込み前に設定する)
process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'picking-test-'));

const { parseCs03002, importBatch, classifyComposition, formatLocation, PkError,
  deriveFolderName, isStaleInstructDate } = await import('../service.js');
const { suggestPatterns } = await import('../patterns.js');
const { initPickingDB, getDB, listBatches, listLines, jstToday } = await import('../db.js');

initPickingDB();

// ─── fixture ───

const HEADERS = [
  '出荷指示日', 'ブロック略称', 'ロケーション', '商品ID', '商品名', '出荷指示数',
  'ピッキングNO', '出荷伝票NO', '荷主出荷NO', 'バーコード',
  '送り状発行ソフト名', '配送方法名', 'トータルピッキングバッチ番号',
];

function makeCsv(rows, { headers = HEADERS } = {}) {
  const q = (v) => `"${String(v ?? '').replace(/"/g, '""')}"`;
  const lines = [headers.map(q).join(',')];
  for (const r of rows) lines.push(headers.map((h) => q(r[h] ?? '')).join(','));
  return iconv.encode(lines.join('\r\n') + '\r\n', 'Shift_JIS');
}

let slipSeq = 0;
function row({ loc, sku, qty = 1, slip, name = '商品', block = 'P3FB', tb = 'TB00110023349', barcode = 'X000TEST01' }) {
  return {
    '出荷指示日': '20260811', 'ブロック略称': block, 'ロケーション': loc,
    '商品ID': sku, '商品名': name, '出荷指示数': String(qty),
    'ピッキングNO': `PC${slip}`, '出荷伝票NO': `SP${slip}`, '荷主出荷NO': String(1500000 + Number(slip)),
    'バーコード': barcode, '送り状発行ソフト名': 'B2(Ver6.0)',
    '配送方法名': 'ネコポス 陸便 元払い 営業所止めなし', 'トータルピッキングバッチ番号': tb,
  };
}

function expectPkError(fn, code) {
  try {
    fn();
    assert.fail(`PkError(${code}) が投げられるはず`);
  } catch (e) {
    assert.ok(e instanceof PkError, `PkError のはずが ${e.constructor.name}: ${e.message}`);
    assert.equal(e.code, code, `code=${code} のはずが ${e.code} (${e.message})`);
  }
}

let passed = 0;
function t(name, fn) {
  fn();
  passed++;
  console.log(`  ok: ${name}`);
}

// ─── 解析・集約 ───

t('実データ相当のCSVが解析できる (集約・ロケ昇順・合算)', () => {
  // 同一SKU×同一ロケが2伝票に分かれるケース + ロケ順が入力順と逆のケース
  const csv = makeCsv([
    row({ loc: '00700803', sku: 'sheet-cm', qty: 3, slip: '0001' }),
    row({ loc: '00201604', sku: 'rosemary10', qty: 2, slip: '0002' }),
    row({ loc: '00201604', sku: 'rosemary10', qty: 1, slip: '0003' }),
    row({ loc: '00400304', sku: 'as-sa5', qty: 1, slip: '0004' }),
  ]);
  const p = parseCs03002(csv);
  assert.equal(p.tbNo, 'TB00110023349');
  assert.equal(p.instructDate, '2026-08-11');
  assert.equal(p.lines.length, 3);                       // 4行 → 3明細に集約
  assert.deepEqual(p.lines.map((l) => l.location), ['00201604', '00400304', '00700803']);
  assert.equal(p.lines[0].qty, 3);                       // 2+1 合算
  assert.equal(p.slipCount, 4);
  assert.equal(p.totalQty, 7);
});

t('並び順はブロック優先 (紙PDFと同順: P3FA→P3FB→P3FD→P3FF、ブロック内はロケ昇順)', () => {
  // 実データの並び (2026-08-12 出荷_01 で紙PDFと突合): P3FFの小さいロケ番号は最後に来る
  const csv = makeCsv([
    row({ loc: '00100303', sku: 'ana', qty: 1, slip: '0001', block: 'P3FF' }),
    row({ loc: '00200904', sku: 'dotta', qty: 1, slip: '0002', block: 'P3FD' }),
    row({ loc: '00100118', sku: 'reed', qty: 1, slip: '0003', block: 'P3FA' }),
    row({ loc: '00202305', sku: 'jas', qty: 1, slip: '0004', block: 'P3FB' }),
  ]);
  const p = parseCs03002(csv);
  assert.deepEqual(p.lines.map((l) => l.sku), ['reed', 'jas', 'dotta', 'ana']);
});

t('同一ロケの別SKUは別明細のまま (SKU昇順)', () => {
  const csv = makeCsv([
    row({ loc: '00201604', sku: 'bbb', qty: 1, slip: '0001' }),
    row({ loc: '00201604', sku: 'aaa', qty: 1, slip: '0002' }),
  ]);
  const p = parseCs03002(csv);
  assert.deepEqual(p.lines.map((l) => l.sku), ['aaa', 'bbb']);
});

// ─── 数量構成の判定 ───

t('構成判定: 全て1明細1個 → 単品', () => {
  const csv = makeCsv([
    row({ loc: '00201604', sku: 'a', qty: 1, slip: '0001' }),
    row({ loc: '00201703', sku: 'b', qty: 1, slip: '0002' }),
  ]);
  assert.equal(parseCs03002(csv).composition, '単品');
});

t('構成判定: 1SKUで数量2以上 → 1SKU複数個', () => {
  const csv = makeCsv([
    row({ loc: '00201604', sku: 'a', qty: 4, slip: '0001' }),
    row({ loc: '00201703', sku: 'b', qty: 2, slip: '0002' }),
  ]);
  assert.equal(parseCs03002(csv).composition, '1SKU複数個');
});

t('構成判定: 複数SKUの伝票 → アソート / 混ざれば混在', () => {
  const assort = makeCsv([
    row({ loc: '00400304', sku: 'a', qty: 1, slip: '0001' }),
    row({ loc: '00400402', sku: 'b', qty: 1, slip: '0001' }),
  ]);
  assert.equal(parseCs03002(assort).composition, 'アソート');
  const mixed = makeCsv([
    row({ loc: '00400304', sku: 'a', qty: 1, slip: '0001' }),
    row({ loc: '00400402', sku: 'b', qty: 1, slip: '0001' }),
    row({ loc: '00201604', sku: 'c', qty: 1, slip: '0002' }),
  ]);
  assert.equal(parseCs03002(mixed).composition, '混在');
});

t('classifyComposition: 同一伝票に同一SKUが2行 (qty合算で1SKU複数個)', () => {
  assert.equal(classifyComposition([
    { slipNo: 'SP1', sku: 'a', qty: 1 },
    { slipNo: 'SP1', sku: 'a', qty: 1 },
  ]), '1SKU複数個');
});

// ─── fail-closed ───

t('必須列欠落は missing_columns', () => {
  const headers = HEADERS.filter((h) => h !== 'ロケーション');
  const csv = makeCsv([], { headers });
  expectPkError(() => parseCs03002(csv), 'missing_columns');
});

t('データ行なしは no_rows', () => {
  expectPkError(() => parseCs03002(makeCsv([])), 'no_rows');
});

t('空ファイルは empty_file', () => {
  expectPkError(() => parseCs03002(Buffer.alloc(0)), 'empty_file');
});

t('数量が不正なら bad_qty', () => {
  const csv = makeCsv([row({ loc: '00201604', sku: 'a', qty: 'x', slip: '0001' })]);
  expectPkError(() => parseCs03002(csv), 'bad_qty');
  const zero = makeCsv([row({ loc: '00201604', sku: 'a', qty: 0, slip: '0001' })]);
  expectPkError(() => parseCs03002(zero), 'bad_qty');
});

t('複数TBは正常 (1引当で複数TBが振られる実仕様)。キーはソート済みTB一覧の組', () => {
  const csv = makeCsv([
    row({ loc: '00201604', sku: 'a', qty: 1, slip: '0001', tb: 'TB002' }),
    row({ loc: '00201703', sku: 'b', qty: 1, slip: '0002', tb: 'TB001' }),
  ]);
  const p = parseCs03002(csv);
  assert.equal(p.tbNo, 'TB001,TB002');   // 行順に依らずソートで正規化
  assert.equal(p.tbCount, 2);
  // 行順を入れ替えた同じ引当のCSVでも同一キーになる (冪等の前提)
  const csv2 = makeCsv([
    row({ loc: '00201703', sku: 'b', qty: 1, slip: '0002', tb: 'TB001' }),
    row({ loc: '00201604', sku: 'a', qty: 1, slip: '0001', tb: 'TB002' }),
  ]);
  assert.equal(parseCs03002(csv2).tbNo, 'TB001,TB002');
});

t('引用符が閉じていない壊れCSVは broken_csv', () => {
  const buf = iconv.encode(`"${HEADERS.join('","')}"\r\n"20260811","P3FB`, 'Shift_JIS');
  expectPkError(() => parseCs03002(buf), 'broken_csv');
});

t('列数が揃わない行は ragged_rows', () => {
  const good = makeCsv([row({ loc: '00201604', sku: 'a', qty: 1, slip: '0001' })]);
  const text = iconv.decode(good, 'Shift_JIS') + '"only","two"\r\n';
  expectPkError(() => parseCs03002(iconv.encode(text, 'Shift_JIS')), 'ragged_rows');
});

// ─── 表示形式・パターン推定 ───

t('formatLocation: 確定形式 P3FB-002-016-04', () => {
  assert.equal(formatLocation('P3FB', '00201604'), 'P3FB-002-016-04');
  assert.equal(formatLocation('', '00201604'), '002-016-04');
  assert.equal(formatLocation('P3FB', 'ABC'), 'P3FB-ABC');   // 8桁数値以外はそのまま
});

const NEKOPOS = { invoiceSofts: ['B2(Ver6.0)'], deliveryMethods: ['ネコポス 陸便 元払い 営業所止めなし'] };

t('suggestPatterns: ネコポスB2単品は単品パターンが先頭', () => {
  const s = suggestPatterns({ ...NEKOPOS, composition: '単品' });
  assert.ok(s.length > 0);
  assert.ok(s[0].includes('単品'), `先頭が単品パターンのはず: ${s[0]}`);
  assert.ok(!s[0].includes('全て'), '単品専用が全て系より先');
});

t('suggestPatterns: 混在は全て系のみ適合候補', () => {
  const s = suggestPatterns({ ...NEKOPOS, composition: '混在' });
  assert.ok(s[0].includes('全て'), `先頭が全て系のはず: ${s[0]}`);
});

t('suggestPatterns: ワイルドカード (LINEギフト) は明示一致より後ろ', () => {
  const s = suggestPatterns({ ...NEKOPOS, composition: '混在' });
  assert.ok(!s[0].includes('LINEギフト'), `先頭がLINEギフトではないはず: ${s[0]}`);
});

t('suggestPatterns: 複数配送方法の混在 (LINEギフト型) でも候補が出る', () => {
  const s = suggestPatterns({
    invoiceSofts: ['B2(Ver6.0)', 'ゆうプリR'],
    deliveryMethods: ['ネコポス 陸便 元払い 営業所止めなし', 'ゆうパケット 陸便 元払い 営業所止めなし'],
    composition: '混在',
  });
  assert.ok(s.some((n) => n.includes('LINEギフト')));
});

t('suggestPatterns: 未知の配送方法でも空配列で落ちない', () => {
  const s = suggestPatterns({ invoiceSofts: ['X'], deliveryMethods: ['Y'], composition: '単品' });
  // LINEギフト (soft/methods 不問) だけは常に候補に残る
  assert.ok(Array.isArray(s));
});

// ─── 取込 (冪等・上書き) ───

t('importBatch: 新規作成 → 明細とseqが保存される', () => {
  const csv = makeCsv([
    row({ loc: '00700803', sku: 'sheet-cm', qty: 3, slip: '0001' }),
    row({ loc: '00201604', sku: 'rosemary10', qty: 2, slip: '0002' }),
  ]);
  const p = parseCs03002(csv);
  const { batchId, replaced } = importBatch(p,
    { hikiateClass: 'ネコポス手動単品', folderName: '出荷_03' }, 'test@b-faith.biz');
  assert.equal(replaced, false);
  const lines = listLines(batchId);
  assert.deepEqual(lines.map((l) => [l.seq, l.location, l.qty]),
    [[1, '00201604', 2], [2, '00700803', 3]]);
  const slips = getDB().prepare('SELECT COUNT(*) c FROM pk_slip_lines WHERE batch_id=?').get(batchId).c;
  assert.equal(slips, 2);
  const batches = listBatches(jstToday());
  assert.equal(batches.length, 1);
  assert.equal(batches[0].status, 'ready');
  assert.equal(batches[0].composition, '1SKU複数個');   // qty3 と qty2 の2伝票 = どちらも1SKU複数個
});

t('importBatch: 同一CSV+同一分類の再confirmは再送として成功 (replayed)', () => {
  const csv = makeCsv([row({ loc: '00201604', sku: 'a', qty: 1, slip: '0001', tb: 'TB_REPLAY' })]);
  const p = parseCs03002(csv);
  const r1 = importBatch(p, { hikiateClass: 'ネコポス手動単品' }, 'test@b-faith.biz');
  const r2 = importBatch(p, { hikiateClass: 'ネコポス手動単品' }, 'test@b-faith.biz');
  assert.equal(r2.replayed, true);
  assert.equal(r2.batchId, r1.batchId);
  assert.equal(listLines(r1.batchId).length, 1, '再送で明細が二重にならない');
});

t('importBatch: 内容が異なる再取込は duplicate、overwrite=true で入れ替え', () => {
  const csv1 = makeCsv([row({ loc: '00201604', sku: 'a', qty: 1, slip: '0001', tb: 'TB_DUP' })]);
  const p1 = parseCs03002(csv1);
  const { batchId } = importBatch(p1, { hikiateClass: 'ネコポス手動単品' }, 'test@b-faith.biz');

  // 同一CSVでも分類が違えば再送ではない → duplicate
  expectPkError(() => importBatch(p1, { hikiateClass: 'ネコポス手動1SKU複数個' }, 'test@b-faith.biz'), 'duplicate');

  const csv2 = makeCsv([
    row({ loc: '00201604', sku: 'a', qty: 1, slip: '0001', tb: 'TB_DUP' }),
    row({ loc: '00201703', sku: 'b', qty: 2, slip: '0002', tb: 'TB_DUP' }),
  ]);
  const p2 = parseCs03002(csv2);
  expectPkError(() => importBatch(p2, { hikiateClass: 'ネコポス手動単品' }, 'test@b-faith.biz'), 'duplicate');
  const r2 = importBatch(p2, { hikiateClass: 'ネコポス手動1SKU複数個', overwrite: true }, 'test@b-faith.biz');
  assert.equal(r2.replaced, true);
  assert.equal(r2.batchId, batchId, 'バッチIDは維持される');
  assert.equal(listLines(batchId).length, 2);
  const b = getDB().prepare('SELECT * FROM pk_batches WHERE id=?').get(batchId);
  assert.equal(b.hikiate_class, 'ネコポス手動1SKU複数個');
  assert.equal(b.total_qty, 3);
  // 監査ログ: create + overwrite が追記され、before に変更前の集計が残る
  const logs = getDB().prepare('SELECT * FROM pk_import_logs WHERE batch_id=? ORDER BY id').all(batchId);
  assert.deepEqual(logs.map((l) => l.action), ['create', 'overwrite']);
  const before = JSON.parse(logs[1].before_json);
  assert.equal(before.line_count, 1);
  assert.equal(before.hikiate_class, 'ネコポス手動単品');
});

t('importBatch: 同一CSVでもフォルダ名が違えば再送扱いしない (duplicate)', () => {
  const csv = makeCsv([row({ loc: '00201604', sku: 'a', qty: 1, slip: '0001', tb: 'TB_FOLDER' })]);
  const p = parseCs03002(csv);
  const { batchId } = importBatch(p, { hikiateClass: 'ネコポス手動単品', folderName: '出荷_01' }, 'test@b-faith.biz');
  expectPkError(() => importBatch(p, { hikiateClass: 'ネコポス手動単品', folderName: '出荷_02' }, 'test@b-faith.biz'), 'duplicate');
  // overwrite でフォルダ変更でき、監査ログに folder_name が残る
  const r = importBatch(p, { hikiateClass: 'ネコポス手動単品', folderName: '出荷_02', overwrite: true }, 'test@b-faith.biz');
  assert.equal(r.replaced, true);
  const b = getDB().prepare('SELECT folder_name FROM pk_batches WHERE id=?').get(batchId);
  assert.equal(b.folder_name, '出荷_02');
  const log = getDB().prepare('SELECT * FROM pk_import_logs WHERE batch_id=? ORDER BY id DESC').get(batchId);
  assert.equal(log.folder_name, '出荷_02');
  assert.equal(JSON.parse(log.before_json).folder_name, '出荷_01');
});

t('importBatch: 作業開始後の別内容は already_started (同一内容の再送は成功)', () => {
  const csv = makeCsv([row({ loc: '00201604', sku: 'a', qty: 1, slip: '0001', tb: 'TB_STARTED' })]);
  const p = parseCs03002(csv);
  const { batchId } = importBatch(p, { hikiateClass: 'ネコポス手動単品' }, 'test@b-faith.biz');
  getDB().prepare("UPDATE pk_batches SET status='picking' WHERE id=?").run(batchId);
  // 同一CSV+同一分類 = 応答喪失の再送 → 開始後でも成功済み結果を返す (明細は触らない)
  const r = importBatch(p, { hikiateClass: 'ネコポス手動単品', overwrite: true }, 'test@b-faith.biz');
  assert.equal(r.replayed, true);
  // 内容が違えば開始後は overwrite でも不可
  const csv2 = makeCsv([
    row({ loc: '00201604', sku: 'a', qty: 1, slip: '0001', tb: 'TB_STARTED' }),
    row({ loc: '00201703', sku: 'b', qty: 1, slip: '0002', tb: 'TB_STARTED' }),
  ]);
  expectPkError(() => importBatch(parseCs03002(csv2), { hikiateClass: 'ネコポス手動単品', overwrite: true }, 'test@b-faith.biz'),
    'already_started');
});

t('importBatch: 引当分類が空なら no_class・長すぎる入力は拒否', () => {
  const csv = makeCsv([row({ loc: '00201604', sku: 'a', qty: 1, slip: '0001', tb: 'TB_NOCLASS' })]);
  const p = parseCs03002(csv);
  expectPkError(() => importBatch(p, { hikiateClass: '  ' }, 'test@b-faith.biz'), 'no_class');
  expectPkError(() => importBatch(p, { hikiateClass: 'x'.repeat(101) }, 'test@b-faith.biz'), 'class_too_long');
  expectPkError(() => importBatch(p, { hikiateClass: 'ネコポス手動単品', folderName: 'y'.repeat(51) }, 'test@b-faith.biz'), 'folder_too_long');
});

t('TBが空の行があれば行番号つきで no_tb_no', () => {
  const csv = makeCsv([
    row({ loc: '00201604', sku: 'a', qty: 1, slip: '0001', tb: 'TB001' }),
    row({ loc: '00201703', sku: 'b', qty: 1, slip: '0002', tb: '' }),
  ]);
  try {
    parseCs03002(csv);
    assert.fail('no_tb_no のはず');
  } catch (e) {
    assert.equal(e.code, 'no_tb_no');
    assert.ok(e.message.includes('行3'), `行番号が入るはず: ${e.message}`);
  }
});

t('必須列名が重複していたら duplicate_columns', () => {
  const headers = [...HEADERS, 'ロケーション'];
  const csv = makeCsv([{ ...row({ loc: '00201604', sku: 'a', qty: 1, slip: '0001' }) }], { headers });
  expectPkError(() => parseCs03002(csv), 'duplicate_columns');
});

t('配送方法が混在するCSVは distinct を保持して表示・推定に使う', () => {
  const r1 = row({ loc: '00201604', sku: 'a', qty: 1, slip: '0001' });
  const r2 = row({ loc: '00201703', sku: 'b', qty: 1, slip: '0002' });
  r2['送り状発行ソフト名'] = 'ゆうプリR';
  r2['配送方法名'] = 'ゆうパケット 陸便 元払い 営業所止めなし';
  const p = parseCs03002(makeCsv([r1, r2]));
  assert.ok(p.deliveryMethod.includes(' / '), `混在が表示に残るはず: ${p.deliveryMethod}`);
  assert.ok(p.suggestions.some((n) => n.includes('LINEギフト')), 'ソフト混在=LINEギフトが候補に出る');
});

t('deriveFolderName: Driveファイル名から出荷フォルダ名を導出', () => {
  assert.equal(deriveFolderName('ピッキングリストデータ_出荷03.csv'), '出荷_03');
  assert.equal(deriveFolderName('ピッキングリストデータ_出荷3.csv'), '出荷_03');
  assert.equal(deriveFolderName('ピッキングリストデータ_出荷_12.csv'), '出荷_12');
  assert.equal(deriveFolderName('CS03002_9eb48e3d.csv'), null);
  assert.equal(deriveFolderName(''), null);
});

t('isStaleInstructDate: 出荷指示日が今日を含まなければ警告', () => {
  assert.equal(isStaleInstructDate('2000-01-01', '2026-08-12'), true);
  assert.equal(isStaleInstructDate('2026-08-12', '2026-08-12'), false);
  assert.equal(isStaleInstructDate('2000-01-01 / 2026-08-12', '2026-08-12'), false);   // 混在で今日を含む
  assert.equal(isStaleInstructDate('', '2026-08-12'), false);                          // 不明はブロックしない
});

t('listBatches: 前日以前の未完了は持ち越し表示、完了は当日のみ', () => {
  const db = getDB();
  const mk = (tb, workDate, status) => {
    db.prepare(`INSERT INTO pk_batches
      (tb_no, hikiate_class, work_date, composition, line_count, slip_count, total_qty,
       status, csv_sha256, imported_by, created_at, updated_at)
      VALUES (?, 'テスト', ?, '単品', 1, 1, 1, ?, 'x', 't@b', '2026-08-10T00:00:00Z', '2026-08-10T00:00:00Z')
    `).run(tb, workDate, status);
  };
  mk('TB_Y1', '2000-01-01', 'ready');   // 持ち越し
  mk('TB_Y2', '2000-01-01', 'done');    // 過去の完了 → 出ない
  const list = listBatches(jstToday());
  const tbs = list.map((b) => b.tb_no);
  assert.ok(tbs.includes('TB_Y1'));
  assert.ok(!tbs.includes('TB_Y2'));
});

console.log(`\ntest-import: ${passed} 件 pass`);
