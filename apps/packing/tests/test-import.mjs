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
  deriveFolderName, isStaleSagyoDate, getWorkState, applyEvent,
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
  '配送先名', '配送先郵便番号', '配送先都道府県', '配送先住所1', '配送先住所2', '配送先住所3',
  'サイト受注№', '注文日', '納品書印字ヘッダ1',
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
    '配送先名': `テスト太郎${slip}`, '配送先郵便番号': '5640038', '配送先都道府県': '大阪府',
    '配送先住所1': '吹田市テスト町1-1', '配送先住所2': `建物${slip}号室`, '配送先住所3': '',
    'サイト受注№': `ORD-${slip}`, '注文日': TODAY8, '納品書印字ヘッダ1': '',
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

// ─── 送り先・受注情報 (v2・納品書PDF同等表示) ───

t('送り先 (名前・〒・住所連結) と受注番号・注文日が取り込まれる', () => {
  const p = parseCs03003(makeCsv([row({ slip: '0080', sku: 'a', tb: 'TB55555555555' })]));
  const s = p.slips[0];
  assert.equal(s.recipientName, 'テスト太郎0080');
  assert.equal(s.recipientZip, '5640038');
  assert.equal(s.recipientPref, '大阪府');
  assert.equal(s.recipientAddr, '吹田市テスト町1-1 建物0080号室');   // 住所1〜3を空白連結 (空は落ちる)
  assert.equal(s.siteOrderNo, 'ORD-0080');
  assert.equal(s.orderDate, jstToday());
  const r = importPackBatch(p, { matchAck: true }, 'test');
  const dbSlip = getDB().prepare('SELECT * FROM pk_pack_slips WHERE batch_id=?').get(r.batchId);
  assert.equal(dbSlip.recipient_name, 'テスト太郎0080');
  assert.equal(dbSlip.site_order_no, 'ORD-0080');
});

// ─── 作業イベント (start / next / takeover) ───

const wp = parseCs03003(makeCsv([
  row({ slip: '0090', lineNo: 1, sku: 'w1', tb: 'TB44444444444' }),
  row({ slip: '0091', lineNo: 1, sku: 'w2', tb: 'TB44444444444' }),
  row({ slip: '0092', lineNo: 1, sku: 'w3', tb: 'TB44444444444' }),
]));
const wr = importPackBatch(wp, { matchAck: true }, 'test');

t('start で packing になり先頭伝票に shown_at が付く', () => {
  const r = applyEvent(wr.batchId, { opId: 'op-s1', event: 'start' }, '星');
  assert.equal(r.batchStatus, 'packing');
  assert.equal(r.currentSeq, 1);
  const s = getWorkState(wr.batchId);
  assert.ok(s.slips[0].shown_at);
  assert.equal(s.batch.worker, '星');
});

t('同一op_idの再送は replayed', () => {
  const r = applyEvent(wr.batchId, { opId: 'op-s1', event: 'start' }, '星');
  assert.equal(r.replayed, true);
});

t('別作業者の start は taken (takeover で交代できる)', () => {
  expectPackError(() => applyEvent(wr.batchId, { opId: 'op-s2', event: 'start' }, '月'), 'taken');
  applyEvent(wr.batchId, { opId: 'op-t1', event: 'takeover' }, '月');
  assert.equal(getWorkState(wr.batchId).batch.worker, '月');
  applyEvent(wr.batchId, { opId: 'op-t2', event: 'takeover' }, '星');   // 戻す
});

t('next は現在の伝票のみ・順に完了して最後で batch done (計測タイムスタンプ分離)', () => {
  expectPackError(() => applyEvent(wr.batchId, { opId: 'op-x1', event: 'next', slipSeq: 2 }, '星'), 'out_of_order');
  let r = applyEvent(wr.batchId, { opId: 'op-n1', event: 'next', slipSeq: 1 }, '星');
  assert.equal(r.currentSeq, 2);
  assert.equal(r.doneCount, 1);
  const mid = getWorkState(wr.batchId);
  assert.ok(mid.slips[0].done_at);      // packing_completed
  assert.ok(mid.slips[1].shown_at);     // slip_opened
  r = applyEvent(wr.batchId, { opId: 'op-n2', event: 'next', slipSeq: 2 }, '星');
  r = applyEvent(wr.batchId, { opId: 'op-n3', event: 'next', slipSeq: 3 }, '星');
  assert.equal(r.batchStatus, 'done');
  assert.equal(r.currentSeq, null);
  assert.ok(getWorkState(wr.batchId).batch.finished_at);
});

t('完了後の next / start は拒否される', () => {
  expectPackError(() => applyEvent(wr.batchId, { opId: 'op-n9', event: 'next', slipSeq: 3 }, '星'), 'not_packing');
  expectPackError(() => applyEvent(wr.batchId, { opId: 'op-s9', event: 'start' }, '星'), 'already_done');
});

// ─── 例外操作: 中断/再開・完了取消・ズレ回復ジャンプ・バッチ取消 ───

const ep = parseCs03003(makeCsv([
  row({ slip: '0110', lineNo: 1, sku: 'e1', tb: 'TB11111111111' }),
  row({ slip: '0111', lineNo: 1, sku: 'e2', tb: 'TB11111111111' }),
  row({ slip: '0112', lineNo: 1, sku: 'e3', tb: 'TB11111111111' }),
]));
const er = importPackBatch(ep, { matchAck: true }, 'test');
applyEvent(er.batchId, { opId: 'e-s1', event: 'start' }, '星');

t('pause/resume: 中断時間が paused_total_sec に積まれる (clientAtクランプ)', () => {
  const t0 = new Date(Date.now() - 300_000).toISOString();   // 5分前に中断
  applyEvent(er.batchId, { opId: 'e-p1', event: 'pause', reason: '休憩', clientAt: t0 }, '星');
  assert.equal(getPackBatch(er.batchId).status, 'paused');
  expectPackError(() => applyEvent(er.batchId, { opId: 'e-n0', event: 'next', slipSeq: 1 }, '星'), 'not_packing');
  applyEvent(er.batchId, { opId: 'e-r1', event: 'resume' }, '星');
  const b = getPackBatch(er.batchId);
  assert.equal(b.status, 'packing');
  assert.ok(b.paused_total_sec >= 290 && b.paused_total_sec <= 310, `5分前後のはず: ${b.paused_total_sec}`);
});

t('ズレ回復ジャンプ: jumped=trueで順序外の完了ができ、飛ばした伝票が残ると完了しない', () => {
  // 現在は seq1 だが、紙の束が seq3 から始まっていた想定
  applyEvent(er.batchId, { opId: 'e-j1', event: 'jump', slipSeq: 3 }, '星');
  expectPackError(() => applyEvent(er.batchId, { opId: 'e-x1', event: 'next', slipSeq: 3 }, '星'), 'out_of_order');
  const r = applyEvent(er.batchId, { opId: 'e-n1', event: 'next', slipSeq: 3, jumped: true }, '星');
  assert.equal(r.batchStatus, 'packing');           // 1・2が未処理なので完了しない (完了ガード)
  assert.deepEqual(r.doneSeqs, [3]);
  assert.equal(r.currentSeq, 1);
  assert.equal(r.lastDoneSeq, 3);
});

t('undo: 最後に完了した伝票 (done_at順) だけ理由つきで取り消せる', () => {
  applyEvent(er.batchId, { opId: 'e-n2', event: 'next', slipSeq: 1 }, '星');
  // 最後に完了したのは seq1 (done_at順)。seq3 は取り消せない
  expectPackError(() => applyEvent(er.batchId, { opId: 'e-u0', event: 'undo', slipSeq: 3, reason: '誤タップ' }, '星'), 'out_of_order');
  expectPackError(() => applyEvent(er.batchId, { opId: 'e-u1', event: 'undo', slipSeq: 1 }, '星'), 'bad_reason');
  const r = applyEvent(er.batchId, { opId: 'e-u2', event: 'undo', slipSeq: 1, reason: '誤タップ' }, '星');
  assert.deepEqual(r.doneSeqs, [3]);
  assert.equal(r.currentSeq, 1);
});

t('undo: バッチ完了直後も取り消して再開できる', () => {
  applyEvent(er.batchId, { opId: 'e-n3', event: 'next', slipSeq: 1 }, '星');
  applyEvent(er.batchId, { opId: 'e-n4', event: 'next', slipSeq: 2 }, '星');
  assert.equal(getPackBatch(er.batchId).status, 'done');
  const r = applyEvent(er.batchId, { opId: 'e-u3', event: 'undo', slipSeq: 2, reason: 'その他' }, '星');
  assert.equal(r.batchStatus, 'packing');
  assert.equal(r.currentSeq, 2);
});

t('cancel: 未着手に戻り伝票進捗が初期化される', () => {
  applyEvent(er.batchId, { opId: 'e-c1', event: 'cancel' }, '星');
  const b = getPackBatch(er.batchId);
  assert.equal(b.status, 'ready');
  assert.equal(b.worker, null);
  assert.equal(b.paused_total_sec, 0);
  const slips = listPackSlips(er.batchId);
  assert.ok(slips.every((s) => s.status === 'pending' && !s.done_at));
});

// ─── Drive自動ポーラー (deps注入・v3) ───

const { pollOnce, pickPollCandidates } = await import('../drive-sync.js');
const { createDevice, verifyDevice, revokeDevice } = await import('../db.js');

t('pickPollCandidates: 納品書CSVのみ・60秒安定待ち・3日窓', () => {
  const now = Date.parse('2026-08-16T03:00:00Z');
  const files = [
    { filename: '納品書_出荷_01.csv', modified_time: '2026-08-16T02:58:00Z' },   // ok
    { filename: '納品書_出荷_02.csv', modified_time: '2026-08-16T02:59:30Z' },   // 30秒前 → 待つ
    { filename: 'ピッキングリストデータ_出荷_01.csv', modified_time: '2026-08-16T02:00:00Z' },  // 対象外
    { filename: '納品書_1.pdf', modified_time: '2026-08-16T02:00:00Z' },         // 対象外
    { filename: '納品書_出荷_03.csv', modified_time: '2026-08-10T00:00:00Z' },   // 3日超
  ];
  assert.deepEqual(pickPollCandidates(files, now).map((f) => f.filename), ['納品書_出荷_01.csv']);
});

async function runPoll(csvBuf, { fileId = 'F1', mtime = '2026-08-16T00:00:00Z', filename = '納品書_出荷_77.csv', parent = '出荷_77' } = {}) {
  return pollOnce({
    listFiles: async () => [{ file_id: fileId, filename, parent_name: parent, modified_time: mtime }],
    download: async () => ({ buffer: csvBuf, modified_time: mtime, filename }),
    nowMs: Date.parse(mtime) + 120_000,
  });
}

await (async () => {
  // 突合ok → 自動取込
  const okCsv = makeCsv([
    { ...row({ slip: '0100', sku: 'pz', tb: 'TB33333333333' }) },
  ]);
  importPickingBatch(parseCs03002(makePickCsv([
    pickRow({ slip: '0100', sku: 'pz', qty: 1, tb: 'TB33333333333' }),
  ])), { hikiateClass: 'テスト', folderName: '出荷_77' }, 'test');
  let s = await runPoll(okCsv);
  assert.equal(s.imported, 1, `imported=1 のはずが ${JSON.stringify(s)}`);
  // 同じ版はスキップ (台帳冪等)
  s = await runPoll(okCsv);
  assert.equal(s.checked + s.imported + s.failed, 1);   // checked=1, imported=0
  assert.equal(s.imported, 0);
  passed++; console.log('  ok: ポーラー: 突合okは自動取込・同一版は台帳で冪等 (async)');

  // no_picking → failed (再試行対象)
  const npCsv = makeCsv([
    { ...row({ slip: '0101', sku: 'nq', tb: 'TB22222222222' }) },
  ]);
  s = await runPoll(npCsv, { fileId: 'F2', filename: '納品書_出荷_78.csv', parent: '出荷_78' });
  assert.equal(s.failed, 1);
  const led = getDB().prepare("SELECT * FROM pk_pack_drive_imports WHERE drive_file_id='F2'").get();
  assert.equal(led.status, 'failed');
  assert.ok(/no_picking/.test(led.error));
  // picking 側が届いたら次周期で取り込まれる
  importPickingBatch(parseCs03002(makePickCsv([
    pickRow({ slip: '0101', sku: 'nq', qty: 1, tb: 'TB22222222222' }),
  ])), { hikiateClass: 'テスト', folderName: '出荷_78' }, 'test');
  s = await runPoll(npCsv, { fileId: 'F2', filename: '納品書_出荷_78.csv', parent: '出荷_78' });
  assert.equal(s.imported, 1);
  passed++; console.log('  ok: ポーラー: no_pickingは再試行→CS03002到着後に自動取込 (async)');

  // mismatch → skipped (人の承認へ)
  const mmCsv = makeCsv([
    { ...row({ slip: '0100', sku: 'pz', qty: 3, tb: 'TB33333333333' }) },   // pickingは1個
  ]);
  s = await runPoll(mmCsv, { fileId: 'F3', mtime: '2026-08-16T01:00:00Z', filename: '納品書_出荷_77.csv', parent: '出荷_77' });
  assert.equal(s.skipped, 1);
  assert.ok(/mismatch/.test(getDB().prepare("SELECT error FROM pk_pack_drive_imports WHERE drive_file_id='F3'").get().error));
  passed++; console.log('  ok: ポーラー: mismatchは取り込まずskipped=承認待ち (async)');
})();

// ─── 登録端末 (v3) ───

t('端末登録→検証→失効', () => {
  const { token, id } = createDevice('梱包iPad1', 'test@example.com');
  const d = verifyDevice(token);
  assert.equal(d.label, '梱包iPad1');
  assert.equal(d.id, id);
  assert.equal(verifyDevice('bogus'), null);
  assert.ok(revokeDevice(id));
  assert.equal(verifyDevice(token), null);
});

console.log(`test-import: ${passed} 件 pass`);
