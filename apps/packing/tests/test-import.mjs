/**
 * packing PR1 — CS03003 解析・警告バッジ・CS03002突合・取込冪等のテスト。
 * fixture は実データ (2026-08-16 採取の 納品書_出荷_XX.csv・237列) の構造を
 * 必須列だけで再現する (個人情報列はそもそも解析対象外のため fixture に含めない)。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import iconv from 'iconv-lite';

// DB を一時ディレクトリへ (モジュール読み込み前に設定する)
process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'packing-test-'));

const {
  parseCs03003, importPackBatch, checkPickingMatch, slipWarns, PackError,
  deriveFolderName, isStaleSagyoDate,
} = await import('../service.js');
const {
  initPackingDB, getDB, listPackBatches, getPackBatch, listPackSlips,
  listPackLinesBySlip, jstToday,
} = await import('../db.js');
// picking 側 (突合相手)。同じ picking.db に同居する
const { parseCs03002, importBatch: importPickingBatch } = await import('../../picking/service.js');
const { initPickingDB } = await import('../../picking/db.js');

initPickingDB();
initPackingDB();

// ─── fixture ───

const HEADERS = [
  '荷主出荷NO', '出荷伝票NO', 'ピッキングNO', 'トータルピッキングバッチ番号',
  '出荷予定行NO', 'マテハン用BC', '出荷作業日', '取引先名',
  '配送方法ID', '配送方法名', '引当抽出グループ1',
  '商品ID', '商品名', '出荷数', 'バーコード', '印字商品名', '送り状備考1',
  'ギフトフラグ', 'ギフトメッセージ', 'のし',
  '納品書ヘッダコメント', '納品書フッタコメント', '倉庫連絡事項', '顧客コメント',
  '配達指定日', '配達時間帯', '箱数', '有効期限', 'ロット',
];

function makeCsv(rows, { headers = HEADERS } = {}) {
  const q = (v) => `"${String(v ?? '').replace(/"/g, '""')}"`;
  const lines = [headers.map(q).join(',')];
  for (const r of rows) lines.push(headers.map((h) => q(r[h] ?? '')).join(','));
  return iconv.encode(lines.join('\r\n') + '\r\n', 'Shift_JIS');
}

const TODAY8 = jstToday().replace(/-/g, '');
function row({
  slip, lineNo = 1, sku, qty = 1, name = '商品', tb = 'TB00110023900',
  gift = '0', giftMsg = '', noshi = '', box = '1', comment = '', expiry = '', lot = '',
  material = 'pasline3つ折り', sagyoDate = TODAY8,
}) {
  return {
    '荷主出荷NO': String(1527000 + Number(slip)),
    '出荷伝票NO': `SP${slip}`, 'ピッキングNO': `PC${slip}`,
    'トータルピッキングバッチ番号': tb,
    '出荷予定行NO': String(lineNo),
    'マテハン用BC': `0010011035${slip}R`, '出荷作業日': sagyoDate,
    '取引先名': '雑貨イズム楽天市場店',
    '配送方法ID': '4', '配送方法名': 'ネコポス 営業所止めなし',
    '引当抽出グループ1': material,
    '商品ID': sku, '商品名': name, '出荷数': String(qty),
    'バーコード': 'X000TEST01', '印字商品名': `${name} (印字)`,
    '送り状備考1': `${sku}/${qty}`,
    'ギフトフラグ': gift, 'ギフトメッセージ': giftMsg, 'のし': noshi,
    '納品書ヘッダコメント': '', '納品書フッタコメント': '',
    '倉庫連絡事項': comment, '顧客コメント': '',
    '配達指定日': '', '配達時間帯': '指定なし', '箱数': box,
    '有効期限': expiry, 'ロット': lot,
  };
}

// picking 側 fixture (CS03002)
const PICK_HEADERS = [
  '出荷指示日', 'ブロック略称', 'ロケーション', '商品ID', '商品名', '出荷指示数',
  'ピッキングNO', '出荷伝票NO', '荷主出荷NO', 'バーコード',
  '送り状発行ソフト名', '配送方法名', 'トータルピッキングバッチ番号',
];
function pickRow({ slip, sku, qty = 1, tb = 'TB00110023900', loc = '00201604' }) {
  return {
    '出荷指示日': TODAY8, 'ブロック略称': 'P3FB', 'ロケーション': loc,
    '商品ID': sku, '商品名': '商品', '出荷指示数': String(qty),
    'ピッキングNO': `PC${slip}`, '出荷伝票NO': `SP${slip}`,
    '荷主出荷NO': String(1527000 + Number(slip)), 'バーコード': 'X000TEST01',
    '送り状発行ソフト名': 'B2(Ver6.0)', '配送方法名': 'ネコポス 営業所止めなし',
    'トータルピッキングバッチ番号': tb,
  };
}
function makePickCsv(rows) {
  const q = (v) => `"${String(v ?? '').replace(/"/g, '""')}"`;
  const lines = [PICK_HEADERS.map(q).join(',')];
  for (const r of rows) lines.push(PICK_HEADERS.map((h) => q(r[h] ?? '')).join(','));
  return iconv.encode(lines.join('\r\n') + '\r\n', 'Shift_JIS');
}

function expectPackError(fn, code) {
  try {
    fn();
    assert.fail(`PackError(${code}) が投げられるはず`);
  } catch (e) {
    assert.ok(e instanceof PackError, `PackError のはずが ${e.constructor.name}: ${e.message}`);
    assert.equal(e.code, code, `code=${code} のはずが ${e.code} (${e.message})`);
  }
}

let passed = 0;
function t(name, fn) {
  fn();
  passed++;
  console.log(`  ok: ${name}`);
}

// ─── 解析 ───

t('実データ相当のCSVが解析できる (伝票の出現順・明細・数量)', () => {
  const csv = makeCsv([
    row({ slip: '0002', lineNo: 1, sku: 'hinokimat5l', qty: 1 }),      // ファイル順が伝票順
    row({ slip: '0001', lineNo: 1, sku: 'fbalm-musk', qty: 2 }),
    row({ slip: '0001', lineNo: 2, sku: 'tartstone500', qty: 1 }),
  ]);
  const p = parseCs03003(csv);
  assert.equal(p.tbKey, 'TB00110023900');
  assert.equal(p.slipCount, 2);
  assert.equal(p.lineCount, 3);
  assert.equal(p.totalQty, 4);
  // 並びはCSV出現順のまま (ソートしない = 納品書PDFの束の順)
  assert.deepEqual(p.slips.map((s) => s.slipNo), ['SP0002', 'SP0001']);
  assert.equal(p.slips[0].seq, 1);
  assert.equal(p.slips[1].lines.length, 2);
  assert.equal(p.slips[1].lines[0].sku, 'fbalm-musk');
  assert.equal(p.slips[1].lines[0].qty, 2);
  assert.equal(p.slips[1].neSlipNo, '1527001');
  assert.equal(p.sagyoDate, jstToday());
});

t('複数TBはソート済みカンマ結合が tbKey になる (picking と同じ正規化)', () => {
  const csv = makeCsv([
    row({ slip: '0001', sku: 'a', tb: 'TB00110023902' }),
    row({ slip: '0002', sku: 'b', tb: 'TB00110023901' }),
  ]);
  const p = parseCs03003(csv);
  assert.equal(p.tbKey, 'TB00110023901,TB00110023902');
  assert.equal(p.tbCount, 2);
});

t('警告バッジ: ギフト・のし・コメント・複数箱・複数個・アソート・期限', () => {
  const csv = makeCsv([
    row({ slip: '0001', sku: 'a', qty: 2 }),                                    // multi_qty
    row({ slip: '0002', lineNo: 1, sku: 'a' }),
    row({ slip: '0002', lineNo: 2, sku: 'b' }),                                 // assort
    row({ slip: '0003', sku: 'a', gift: '1', giftMsg: 'おめでとう', noshi: '御祝' }),  // gift+noshi
    row({ slip: '0004', sku: 'a', box: '2', comment: 'ワレモノ注意' }),           // multi_box+comment
    row({ slip: '0005', sku: 'a', expiry: '20270101', lot: 'L01' }),            // expiry_lot
  ]);
  const p = parseCs03003(csv);
  const bySlip = new Map(p.slips.map((s) => [s.slipNo, s.warns]));
  assert.deepEqual(bySlip.get('SP0001'), ['multi_qty']);
  assert.deepEqual(bySlip.get('SP0002'), ['assort']);
  assert.deepEqual(bySlip.get('SP0003'), ['gift', 'noshi']);
  assert.deepEqual(bySlip.get('SP0004'), ['comment', 'multi_box']);
  assert.deepEqual(bySlip.get('SP0005'), ['expiry_lot']);
  assert.equal(p.slips.find((s) => s.slipNo === 'SP0004').comments.warehouse, 'ワレモノ注意');
});

t('アソート伝票は multi_qty を重複表示しない (assort が優先)', () => {
  const slip = {
    giftFlag: '0', giftMessage: '', noshi: '', comments: {}, boxCount: 1,
    lines: [{ sku: 'a', qty: 2 }, { sku: 'b', qty: 1 }],
  };
  assert.deepEqual(slipWarns(slip), ['assort']);
});

t('フッタコメント (定型のサイト受注番号) だけでは comment バッジを出さない', () => {
  const base = { giftFlag: '0', giftMessage: '', noshi: '', boxCount: 1, lines: [{ sku: 'a', qty: 1 }] };
  assert.deepEqual(slipWarns({ ...base, comments: { footer: '746421459' } }), []);
  assert.deepEqual(slipWarns({ ...base, comments: { footer: 'x', warehouse: 'ワレモノ' } }), ['comment']);
});

// ─── fail-closed ───

t('必須列の欠落は missing_columns', () => {
  const headers = HEADERS.filter((h) => h !== 'マテハン用BC');
  const csv = makeCsv([row({ slip: '0001', sku: 'a' })], { headers });
  expectPackError(() => parseCs03003(csv), 'missing_columns');
});

t('出荷数が不正なら bad_qty', () => {
  const csv = makeCsv([row({ slip: '0001', sku: 'a', qty: '0' })]);
  expectPackError(() => parseCs03003(csv), 'bad_qty');
});

t('TB空行は no_tb_no', () => {
  const csv = makeCsv([row({ slip: '0001', sku: 'a', tb: '' })]);
  expectPackError(() => parseCs03003(csv), 'no_tb_no');
});

t('同一伝票の行が離れていたら noncontiguous_slip (行順=納品書順の前提が崩れている)', () => {
  const csv = makeCsv([
    row({ slip: '0001', lineNo: 1, sku: 'a' }),
    row({ slip: '0002', lineNo: 1, sku: 'b' }),
    row({ slip: '0001', lineNo: 2, sku: 'c' }),
  ]);
  expectPackError(() => parseCs03003(csv), 'noncontiguous_slip');
});

t('箱数が不正なら bad_box_count (空は1扱い)', () => {
  expectPackError(() => parseCs03003(makeCsv([row({ slip: '0001', sku: 'a', box: 'x' })])), 'bad_box_count');
  const p = parseCs03003(makeCsv([row({ slip: '0001', sku: 'a', box: '' })]));
  assert.equal(p.slips[0].boxCount, 1);
});

t('同一伝票内でTB・荷主出荷NO等が食い違ったら inconsistent_slip (Codex R1 high)', () => {
  const r1 = row({ slip: '0001', lineNo: 1, sku: 'a' });
  const r2 = { ...row({ slip: '0001', lineNo: 2, sku: 'b' }), 'トータルピッキングバッチ番号': 'TB99999999999' };
  expectPackError(() => parseCs03003(makeCsv([r1, r2])), 'inconsistent_slip');
  const r3 = { ...row({ slip: '0001', lineNo: 2, sku: 'b' }), '荷主出荷NO': '9999999' };
  expectPackError(() => parseCs03003(makeCsv([r1, r3])), 'inconsistent_slip');
});

t('2行目だけに「のし」やコメントが入ったCSVも inconsistent_slip (警告欠落を防ぐ)', () => {
  const r1 = row({ slip: '0001', lineNo: 1, sku: 'a' });
  const r2 = { ...row({ slip: '0001', lineNo: 2, sku: 'b' }), 'のし': '御祝' };
  expectPackError(() => parseCs03003(makeCsv([r1, r2])), 'inconsistent_slip');
  const r3 = { ...row({ slip: '0001', lineNo: 2, sku: 'b' }), '倉庫連絡事項': 'ワレモノ' };
  expectPackError(() => parseCs03003(makeCsv([r1, r3])), 'inconsistent_slip');
});

t('出荷作業日の混在は mixed_sagyo_date (別の日のファイルが連結された兆候)', () => {
  const csv = makeCsv([
    row({ slip: '0001', sku: 'a', sagyoDate: '20260816' }),
    row({ slip: '0002', sku: 'b', sagyoDate: '20260815' }),
  ]);
  expectPackError(() => parseCs03003(csv), 'mixed_sagyo_date');
});

t('空ファイル・データ行なしは弾く', () => {
  expectPackError(() => parseCs03003(Buffer.alloc(0)), 'empty_file');
  expectPackError(() => parseCs03003(makeCsv([])), 'no_rows');
});

// ─── ヘルパー ───

t('deriveFolderName: 納品書_出荷_01.csv → 出荷_01', () => {
  assert.equal(deriveFolderName('納品書_出荷_01.csv'), '出荷_01');
  assert.equal(deriveFolderName('納品書_出荷_19.csv'), '出荷_19');
  assert.equal(deriveFolderName('納品書_1.pdf'), null);
});

t('isStaleSagyoDate: 今日を含まなければ警告', () => {
  assert.equal(isStaleSagyoDate(jstToday()), false);
  assert.equal(isStaleSagyoDate('2026-08-01'), true);
  assert.equal(isStaleSagyoDate(''), false);
});

// ─── CS03002 突合 ───

// picking 側にバッチを作る (TB00110023900: SP0001=a×2+b×1 / SP0002=a×1)
const pickPreview = parseCs03002(makePickCsv([
  pickRow({ slip: '0001', sku: 'a', qty: 2 }),
  pickRow({ slip: '0001', sku: 'b', qty: 1, loc: '00300101' }),
  pickRow({ slip: '0002', sku: 'a', qty: 1 }),
]));
importPickingBatch(pickPreview, { hikiateClass: 'テスト分類', folderName: '出荷_01' }, 'test');

t('突合: 完全一致は ok (SKUは大文字小文字を無視)', () => {
  const p = parseCs03003(makeCsv([
    row({ slip: '0001', lineNo: 1, sku: 'A', qty: 2 }),   // 大文字でも一致する
    row({ slip: '0001', lineNo: 2, sku: 'b', qty: 1 }),
    row({ slip: '0002', lineNo: 1, sku: 'a', qty: 1 }),
  ]));
  const m = checkPickingMatch(p);
  assert.equal(m.status, 'ok');
  assert.ok(m.pkBatchId > 0);
  assert.deepEqual(m.diffs, []);
});

t('突合: 数量差・伝票欠落・余剰を検出する', () => {
  const p = parseCs03003(makeCsv([
    row({ slip: '0001', lineNo: 1, sku: 'a', qty: 1 }),   // picking は 2
    row({ slip: '0003', lineNo: 1, sku: 'c', qty: 1 }),   // picking に無い伝票
  ]));
  const m = checkPickingMatch(p);
  assert.equal(m.status, 'mismatch');
  assert.equal(m.diffs.length, 4);   // qty差1 + 納品書のみ1 + ピッキングのみ2 (SP0001×b, SP0002×a)
});

t('突合: pickingにTBが無ければ no_picking', () => {
  const p = parseCs03003(makeCsv([row({ slip: '0001', sku: 'a', tb: 'TB99999999999' })]));
  assert.equal(checkPickingMatch(p).status, 'no_picking');
});

// ─── 取込 ───

t('取込→一覧・伝票・明細が引ける (突合ok)', () => {
  const p = parseCs03003(makeCsv([
    row({ slip: '0001', lineNo: 1, sku: 'a', qty: 2 }),
    row({ slip: '0001', lineNo: 2, sku: 'b', qty: 1 }),
    row({ slip: '0002', lineNo: 1, sku: 'a', qty: 1 }),
  ]));
  const r = importPackBatch(p, { folderName: '出荷_01' }, 'test');
  assert.ok(r.batchId > 0);
  assert.equal(r.replaced, false);
  assert.equal(r.match.status, 'ok');

  const batch = getPackBatch(r.batchId);
  assert.equal(batch.tb_key, 'TB00110023900');
  assert.equal(batch.slip_count, 2);
  assert.equal(batch.match_status, 'ok');
  assert.ok(batch.pk_batch_id > 0);

  const slips = listPackSlips(r.batchId);
  assert.deepEqual(slips.map((s) => s.seq), [1, 2]);
  assert.equal(slips[0].ne_slip_no, '1527001');
  const lines = listPackLinesBySlip(r.batchId);
  assert.equal(lines.get(slips[0].id).length, 2);
  assert.equal(listPackBatches(jstToday()).length, 1);
});

t('同一CSVの再confirmは replayed (二重バッチにならない)', () => {
  const p = parseCs03003(makeCsv([
    row({ slip: '0001', lineNo: 1, sku: 'a', qty: 2 }),
    row({ slip: '0001', lineNo: 2, sku: 'b', qty: 1 }),
    row({ slip: '0002', lineNo: 1, sku: 'a', qty: 1 }),
  ]));
  const r = importPackBatch(p, { folderName: '出荷_01' }, 'test');
  assert.equal(r.replayed, true);
  assert.equal(listPackBatches(jstToday()).length, 1);
});

t('内容が変わった再取込は overwrite が必要 → 上書きで伝票が入れ替わる', () => {
  const p = parseCs03003(makeCsv([
    row({ slip: '0001', lineNo: 1, sku: 'a', qty: 2 }),
    row({ slip: '0001', lineNo: 2, sku: 'b', qty: 1 }),
    // SP0002 が消えた新版 → picking と差分が出るため matchAck も必要
  ]));
  expectPackError(() => importPackBatch(p, { folderName: '出荷_01' }, 'test'), 'match_mismatch');
  expectPackError(() => importPackBatch(p, { folderName: '出荷_01', matchAck: true }, 'test'), 'duplicate');
  const r = importPackBatch(p, { folderName: '出荷_01', matchAck: true, overwrite: true }, 'test');
  assert.equal(r.replaced, true);
  const batch = getPackBatch(r.batchId);
  assert.equal(batch.slip_count, 1);
  assert.equal(batch.match_status, 'mismatch');
  assert.ok(JSON.parse(batch.match_json).length > 0);
  // 旧版の伝票・明細が残っていない
  assert.equal(listPackSlips(r.batchId).length, 1);
  // 監査ログに overwrite + ack が残る
  const log = getDB().prepare(
    'SELECT * FROM pk_pack_import_logs WHERE batch_id = ? ORDER BY id DESC LIMIT 1'
  ).get(r.batchId);
  assert.equal(log.action, 'overwrite');
  assert.equal(log.match_acked, 1);
  assert.ok(log.before_json);
});

t('作業開始後の差し替えは already_started', () => {
  const batch = listPackBatches(jstToday())[0];
  getDB().prepare("UPDATE pk_pack_batches SET status='packing' WHERE id=?").run(batch.id);
  const p = parseCs03003(makeCsv([
    row({ slip: '0009', lineNo: 1, sku: 'z', qty: 1 }),
  ]));
  // 同じ tb_key にするため TB を合わせた新版
  const p2 = parseCs03003(makeCsv([
    row({ slip: '0009', lineNo: 1, sku: 'z', qty: 1, tb: 'TB00110023900' }),
  ]));
  expectPackError(() => importPackBatch(p2, { folderName: '出荷_01', matchAck: true }, 'test'), 'already_started');
  getDB().prepare("UPDATE pk_pack_batches SET status='ready' WHERE id=?").run(batch.id);
  void p;
});

t('no_picking は matchAck がなければ拒否・ackで取り込める', () => {
  const p = parseCs03003(makeCsv([
    row({ slip: '0050', sku: 'x', tb: 'TB88888888888' }),
  ]));
  expectPackError(() => importPackBatch(p, {}, 'test'), 'match_no_picking');
  const r = importPackBatch(p, { matchAck: true }, 'test');
  assert.equal(getPackBatch(r.batchId).match_status, 'no_picking');
});

t('replayで突合結果が最新化される (no_picking承認後にCS03002が入ったら ok へ)', () => {
  const p = parseCs03003(makeCsv([
    row({ slip: '0050', sku: 'x', tb: 'TB88888888888' }),
  ]));
  // 後から picking 側に同じバッチが取り込まれた
  importPickingBatch(parseCs03002(makePickCsv([
    pickRow({ slip: '0050', sku: 'x', qty: 1, tb: 'TB88888888888' }),
  ])), { hikiateClass: 'テスト分類', folderName: '出荷_09' }, 'test');
  // 同一CSVの再confirm (ackなし) = replay。突合が ok に更新される
  const r = importPackBatch(p, {}, 'test');
  assert.equal(r.replayed, true);
  const b = getPackBatch(r.batchId);
  assert.equal(b.match_status, 'ok');
  assert.ok(b.pk_batch_id > 0);
});

t('replayで突合が悪化する場合 (ok→no_picking) は matchAck が必要 (勝手に承認済みにしない)', () => {
  const p = parseCs03003(makeCsv([
    row({ slip: '0050', sku: 'x', tb: 'TB88888888888' }),
  ]));
  // picking 側が後から無効化された → 突合が ok から no_picking へ悪化
  getDB().prepare("UPDATE pk_batches SET validity='invalid' WHERE tb_no='TB88888888888'").run();
  expectPackError(() => importPackBatch(p, {}, 'test'), 'match_no_picking');
  const r = importPackBatch(p, { matchAck: true }, 'test');
  assert.equal(r.replayed, true);
  assert.equal(getPackBatch(r.batchId).match_status, 'no_picking');
  // 承認つきの状態更新は監査ログ (match_update) に残る (Codex R3 medium)
  const log = getDB().prepare(
    "SELECT * FROM pk_pack_import_logs WHERE batch_id = ? AND action = 'match_update' ORDER BY id DESC LIMIT 1"
  ).get(r.batchId);
  assert.equal(log.match_acked, 1);
  assert.equal(JSON.parse(log.before_json).match_status, 'ok');
});

t('出荷作業日が今日でないファイルは dateAck が必要', () => {
  const p = parseCs03003(makeCsv([
    row({ slip: '0060', sku: 'y', tb: 'TB77777777777', sagyoDate: '20260801' }),
  ]));
  expectPackError(() => importPackBatch(p, { matchAck: true }, 'test'), 'stale_sagyo_date');
  const r = importPackBatch(p, { matchAck: true, dateAck: true }, 'test');
  assert.ok(r.batchId > 0);
});

t('上書きの監査ログに旧版の伝票+明細スナップショットが残る', () => {
  const mk = (sku) => parseCs03003(makeCsv([
    row({ slip: '0070', sku, tb: 'TB66666666666' }),
  ]));
  const r1 = importPackBatch(mk('old-sku'), { matchAck: true }, 'test');
  const r2 = importPackBatch(mk('new-sku'), { matchAck: true, overwrite: true }, 'test');
  assert.equal(r2.batchId, r1.batchId);
  const log = getDB().prepare(
    "SELECT * FROM pk_pack_import_logs WHERE batch_id = ? AND action = 'overwrite' ORDER BY id DESC LIMIT 1"
  ).get(r1.batchId);
  const before = JSON.parse(log.before_json);
  assert.equal(before.slips.length, 1);
  assert.equal(before.slips[0].lines[0].sku, 'old-sku');
  // バッチ側も全列スナップショット (復元用 — Codex R2 medium)
  assert.equal(before.batch.tb_key, 'TB66666666666');
  assert.ok(before.batch.csv_sha256);
  assert.ok(before.batch.created_at);
});

console.log(`test-import: ${passed} 件 pass`);
