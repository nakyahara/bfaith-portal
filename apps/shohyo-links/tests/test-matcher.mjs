/**
 * matcher.js (証憑↔明細の突合ルール) と inbox.js (受け箱) のテスト
 * 実行: node apps/shohyo-links/tests/test-matcher.mjs
 */
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'shohyo-matcher-'));
const { matchVoucher, parseVoucherFileName } = await import('../matcher.js');
const { addToInbox, listInbox, updateInboxMeta, setMatch, markAttached, countByStatus, takenTransactionIds, autoAttachEnabled, setSetting } = await import('../inbox.js');

let failed = 0;
const check = (label, cond) => { console.log(`${cond ? 'OK ' : 'NG '} ${label}`); if (!cond) failed++; };

const vendors = [
  { id: 1, name: 'ﾛｼﾞﾏｰﾄ（LOGIMART）' },
  { id: 2, name: 'ｴﾈｵｽ（ENEOS-SS）' },
  { id: 3, name: 'CANVA' },
];
const txs = [
  { id: 't1', date: '2026-08-08', value: 21092, side: 'EXPENSE', content: 'ﾛｼﾞﾏｰﾄ（LOGIMART）', journalizing_status: 'registered' },
  { id: 't2', date: '2026-08-01', value: 10249, side: 'EXPENSE', content: 'ｴﾈｵｽｰｴｽｴｽ（ENEOS-SS）', journalizing_status: 'none' },
  { id: 't3', date: '2026-08-08', value: 9713, side: 'EXPENSE', content: 'ｴﾈｵｽｰｴｽｴｽ（ENEOS-SS）', journalizing_status: 'none' },
  { id: 't4', date: '2026-08-17', value: 9713, side: 'EXPENSE', content: 'ｴﾈｵｽｰｴｽｴｽ（ENEOS-SS）', journalizing_status: 'none' },
  { id: 't5', date: '2026-08-06', value: 3760, side: 'EXPENSE', content: 'CANVA* I04965-6620563', journalizing_status: 'none' },
  { id: 't6', date: '2026-08-06', value: 3760, side: 'INCOME', content: 'CANVA REFUND', journalizing_status: 'none' },
  { id: 't7', date: '2026-08-10', value: 21092, side: 'EXPENSE', content: 'ﾗｸｽﾙ', journalizing_status: 'none' },
];

// 3キー一致 → strong
let m = matchVoucher({ vendor_id: 1, amount: 21092, doc_date: '2026-08-07' }, txs, vendors);
check('支払先+金額+日付±3日 で一意 strong', m.kind === 'unique' && m.strength === 'strong' && m.candidates[0].tx_id === 't1');
// 同額でも支払先が違えば合わせない (t7 ラクスル 21092 は候補に入らない)
check('同額の別支払先は候補にしない', m.candidates.length === 1);
// 同支払先・同額が2件 (ENEOS 9713 が 8/8 と 8/17) → 日付窓で1件に絞れる
m = matchVoucher({ vendor_id: 2, amount: 9713, doc_date: '2026-08-16' }, txs, vendors);
check('同額2件でも日付窓で1件に絞れる', m.kind === 'unique' && m.candidates[0].tx_id === 't4');
// 日付が無いと ENEOS 9713 は2件 → ambiguous (貼らない)
m = matchVoucher({ vendor_id: 2, amount: 9713, doc_date: '' }, txs, vendors);
check('日付なしで候補複数は ambiguous', m.kind === 'ambiguous' && m.reason === 'multiple_candidates' && m.candidates.length === 2);
// 支払先不明 (vendor_id も名前も無い) → 金額+日付だけ → weak
m = matchVoucher({ amount: 3760, doc_date: '2026-08-06' }, txs, vendors);
check('支払先不明は weak (INCOME は除外)', m.kind === 'unique' && m.strength === 'weak' && m.candidates[0].tx_id === 't5');
// 抽出した支払先名 (マスタ未登録) でも突合できる
m = matchVoucher({ vendor_name: 'CANVA', amount: 3760, doc_date: '2026-08-06' }, txs, vendors);
check('vendor_name からのキーで strong', m.kind === 'unique' && m.strength === 'strong');
// 金額1円違い → none
m = matchVoucher({ vendor_id: 1, amount: 21093, doc_date: '2026-08-08' }, txs, vendors);
check('金額1円違いは none', m.kind === 'none');
// 日付窓の外 → none
m = matchVoucher({ vendor_id: 1, amount: 21092, doc_date: '2026-08-20' }, txs, vendors);
check('日付±3日の外は none', m.kind === 'none');
// 相手が既に他の証憑に取られている → ambiguous (all_candidates_taken)
m = matchVoucher({ vendor_id: 1, amount: 21092, doc_date: '2026-08-08' }, txs, vendors, { taken: new Set(['t1']) });
check('既に取られた明細には貼らない', m.kind === 'ambiguous' && m.reason === 'all_candidates_taken');
// 金額なし
m = matchVoucher({ vendor_id: 1, doc_date: '2026-08-08' }, txs, vendors);
check('金額なしは none', m.kind === 'none' && m.reason === 'amount_missing');

// ファイル名規約
let p = parseVoucherFileName('2026-08-08_21092_ロジマート_a1b2c3d4.pdf');
check('規約ファイル名 (ハッシュ付き) を読む', p && p.doc_date === '2026-08-08' && p.amount === 21092 && p.vendor_name === 'ロジマート');
p = parseVoucherFileName('2026-08-08_21092_ロジマート.PDF');
check('規約ファイル名 (ハッシュ無し・大文字拡張子)', p && p.vendor_name === 'ロジマート');
check('規約外は null', parseVoucherFileName('IMG_1234.jpg') === null);

// 受け箱
const buf = Buffer.from('%PDF-1.4 dummy');
const a = addToInbox(buf, { file_name: '2026-08-08_21092_ロジマート.pdf', mime: 'application/pdf', vendor_id: 1, doc_date: '2026-08-08', amount: 21092 });
check('受け箱に入る (status=new)', !a.duplicate && a.row.status === 'new' && fs.existsSync(a.row.stored_path));
const b = addToInbox(buf, { file_name: 'same.pdf', mime: 'application/pdf' });
check('同じ内容は二重登録しない', b.duplicate && b.row.id === a.row.id);
check('既定は提案モード (auto_attach OFF)', autoAttachEnabled() === false);
setSetting('auto_attach', '1');
check('auto_attach を ON にできる', autoAttachEnabled() === true);
setMatch(a.row.id, { status: 'proposed', tx_id: 't1', journal_id: 'j1', journal_number: 480, strength: 'strong', reason: 'vendor+amount+date', candidates: [{ tx_id: 't1' }] });
check('taken に proposed の明細が入る', takenTransactionIds().has('t1'));
const upd = updateInboxMeta(a.row.id, { amount: 21000 });
check('メタ修正で new に戻り突合結果が消える', upd.status === 'new' && upd.amount === 21000 && upd.match_tx_id === '');
markAttached(a.row.id, { journal_id: 'j1', journal_number: 480, tx_id: 't1', mf_file_id: 'f1', mode: 'manual', actor: 'test' });
check('添付済みに遷移', listInbox({ status: 'attached' }).length === 1 && countByStatus().attached === 1);
let threw = false;
try { updateInboxMeta(a.row.id, { amount: 1 }); } catch (e) { threw = e.message === 'already_attached'; }
check('添付済みは直せない', threw);

console.log(failed ? `\n${failed}件NG` : '\n全件パス');
process.exit(failed ? 1 : 0);
