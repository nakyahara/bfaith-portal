/**
 * matcher.js (証憑↔明細の突合ルール) と inbox.js (受け箱・添付の確保) のテスト
 * 実行: node apps/shohyo-links/tests/test-matcher.mjs
 */
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'shohyo-matcher-'));
const { matchVoucher, matchBatch, parseVoucherFileName, isValidDate } = await import('../matcher.js');
const {
  addToInbox, listInbox, updateInboxMeta, setMatch, countByStatus, transactionOwners, autoAttachEnabled, setSetting,
  claimForAttach, releaseClaim, markAttached, recoverStaleClaims, readFile, sniffKind, decodeBase64Strict, getInbox,
} = await import('../inbox.js');
const { periodFor } = await import('../attach-job.js');

let failed = 0;
const check = (label, cond) => { console.log(`${cond ? 'OK ' : 'NG '} ${label}`); if (!cond) failed++; };

const vendors = [
  { id: 1, name: 'ﾛｼﾞﾏｰﾄ（LOGIMART）' },
  { id: 2, name: 'ｴﾈｵｽ（ENEOS-SS）' },
  { id: 3, name: 'CANVA' },
  { id: 4, name: 'AMAZON' },
];
const txs = [
  { id: 't1', date: '2026-08-08', value: 21092, side: 'EXPENSE', content: 'ﾛｼﾞﾏｰﾄ（LOGIMART）', journalizing_status: 'registered' },
  { id: 't2', date: '2026-08-01', value: 10249, side: 'EXPENSE', content: 'ｴﾈｵｽｰｴｽｴｽ（ENEOS-SS）', journalizing_status: 'none' },
  { id: 't3', date: '2026-08-08', value: 9713, side: 'EXPENSE', content: 'ｴﾈｵｽｰｴｽｴｽ（ENEOS-SS）', journalizing_status: 'none' },
  { id: 't4', date: '2026-08-17', value: 9713, side: 'EXPENSE', content: 'ｴﾈｵｽｰｴｽｴｽ（ENEOS-SS）', journalizing_status: 'none' },
  { id: 't5', date: '2026-08-06', value: 3760, side: 'EXPENSE', content: 'CANVA* I04965-6620563', journalizing_status: 'none' },
  { id: 't6', date: '2026-08-06', value: 3760, side: 'INCOME', content: 'CANVA REFUND', journalizing_status: 'none' },
  { id: 't7', date: '2026-08-10', value: 21092, side: 'EXPENSE', content: 'ﾗｸｽﾙ', journalizing_status: 'none' },
  { id: 't8', date: '2026-08-12', value: 5000, side: 'EXPENSE', content: 'ABC STORE', memo: 'ロジマート分', journalizing_status: 'none' },
  { id: 't9', date: '2026-08-13', value: 8000, side: 'EXPENSE', content: 'AMAZON WEB SERVICES', journalizing_status: 'none' },
];

// 3キー一致 → strong
let m = matchVoucher({ vendor_id: 1, amount: 21092, doc_date: '2026-08-07' }, txs, vendors);
check('支払先+金額+日付±3日 で一意 strong', m.kind === 'unique' && m.strength === 'strong' && m.candidates[0].tx_id === 't1');
check('同額の別支払先は候補にしない', m.candidates.length === 1);
m = matchVoucher({ vendor_id: 2, amount: 9713, doc_date: '2026-08-16' }, txs, vendors);
check('同額2件でも日付窓で1件に絞れる', m.kind === 'unique' && m.candidates[0].tx_id === 't4');
m = matchVoucher({ vendor_id: 2, amount: 9713, doc_date: '' }, txs, vendors);
check('日付なしで候補複数は ambiguous', m.kind === 'ambiguous' && m.reason === 'multiple_candidates' && m.candidates.length === 2);
m = matchVoucher({ amount: 3760, doc_date: '2026-08-06' }, txs, vendors);
check('支払先不明は weak (INCOME は除外)', m.kind === 'unique' && m.strength === 'weak' && m.candidates[0].tx_id === 't5');
m = matchVoucher({ vendor_name: 'CANVA', amount: 3760, doc_date: '2026-08-06' }, txs, vendors);
check('vendor_name (5文字) は短いキー → weak (自動添付しない)', m.kind === 'unique' && m.strength === 'weak');
m = matchVoucher({ vendor_name: 'LOGIMART', amount: 21092, doc_date: '2026-08-08' }, txs, vendors);
check('vendor_name が英字だけで加盟店名のカタカナ部分を説明できない → weak (人が確認)', m.kind === 'unique' && m.strength === 'weak');
m = matchVoucher({ vendor_name: 'ﾛｼﾞﾏｰﾄ（LOGIMART）', amount: 21092, doc_date: '2026-08-08' }, txs, vendors);
check('vendor_name が加盟店名を丸ごと説明できる → strong', m.kind === 'unique' && m.strength === 'strong');
m = matchVoucher({ vendor_id: 1, amount: 5000, doc_date: '2026-08-12' }, txs, vendors);
check('memo にしか一致しないときは weak', m.kind === 'unique' && m.strength === 'weak' && m.candidates[0].vendor_hit === 'weak');
m = matchVoucher({ vendor_id: 4, amount: 8000, doc_date: '2026-08-13' }, txs, vendors);
check('AMAZON は AMAZON WEB SERVICES に内包されるが WEB/SERVICES を説明できない → weak', m.kind === 'unique' && m.strength === 'weak');
m = matchVoucher({ vendor_id: 4, amount: 5986, doc_date: '2026-08-01' }, txs.concat([{ id: 't10', date: '2026-08-01', value: 5986, side: 'EXPENSE', content: 'AMAZON.CO.JP', journalizing_status: 'registered' }]), vendors);
check('AMAZON.CO.JP は CO/JP が雑音語 → strong', m.kind === 'unique' && m.strength === 'strong');
m = matchVoucher({ vendor_name: 'ﾗｸｽﾙ（DP4*RAKSUL）', amount: 11422, doc_date: '2026-08-14' }, [{ id: 't11', date: '2026-08-14', value: 11422, side: 'EXPENSE', content: 'ﾗｸｽﾙｶﾌﾞｼｷｶﾞｲｼｬ（DP4*RAKSUL）', journalizing_status: 'none' }], vendors);
check('ラクスル株式会社(DP4*RAKSUL) は語が全部説明できる → strong', m.kind === 'unique' && m.strength === 'strong');
m = matchVoucher({ vendor_id: 1, amount: 21093, doc_date: '2026-08-08' }, txs, vendors);
check('金額1円違いは none', m.kind === 'none');
m = matchVoucher({ vendor_id: 1, amount: 21092, doc_date: '2026-08-20' }, txs, vendors);
check('日付±3日の外は none', m.kind === 'none');
m = matchVoucher({ vendor_id: 1, amount: 21092, doc_date: '2026-08-08' }, txs, vendors, { taken: new Set(['t1']) });
check('既に取られた明細には貼らない', m.kind === 'ambiguous' && m.reason === 'all_candidates_taken');
m = matchVoucher({ vendor_id: 1, doc_date: '2026-08-08' }, txs, vendors);
check('金額なしは none', m.kind === 'none' && m.reason === 'amount_missing');
m = matchVoucher({ vendor_id: 1, amount: 21092, doc_date: '2026-99-99' }, txs, vendors);
check('存在しない日付は「日付なし」扱い (weak)', m.kind === 'unique' && m.strength === 'weak');

// 双方向の一意性 (matchBatch)
let b = matchBatch([
  { id: 101, vendor_id: 1, amount: 21092, doc_date: '2026-08-08' },
  { id: 102, vendor_name: 'LOGIMART', amount: 21092, doc_date: '2026-08-09' },
], txs, vendors);
check('同じ明細を2つの証憑が一意に指す → 両方 ambiguous (先着で奪わない)',
  b.get(101).kind === 'ambiguous' && b.get(102).kind === 'ambiguous' && b.get(101).reason === 'multiple_vouchers_for_transaction');
b = matchBatch([
  { id: 101, vendor_id: 1, amount: 21092, doc_date: '2026-08-08' },
  { id: 103, vendor_id: 3, amount: 3760, doc_date: '2026-08-06' },
], txs, vendors);
check('別々の明細なら両方 unique', b.get(101).kind === 'unique' && b.get(103).kind === 'unique');
b = matchBatch([{ id: 101, vendor_id: 1, amount: 21092, doc_date: '2026-08-08' }], txs, vendors, { owners: new Map([['t1', new Set([999])]]) });
check('他の証憑が所有する明細は taken', b.get(101).kind === 'ambiguous' && b.get(101).reason === 'all_candidates_taken');
b = matchBatch([{ id: 101, vendor_id: 1, amount: 21092, doc_date: '2026-08-08' }], txs, vendors, { owners: new Map([['t1', new Set([101])]]) });
check('自分が所有する明細は taken にならない', b.get(101).kind === 'unique');

// ファイル名規約・日付
let p = parseVoucherFileName('2026-08-08_21092_ロジマート_a1b2c3d4.pdf');
check('規約ファイル名 (ハッシュ付き) を読む', p && p.doc_date === '2026-08-08' && p.amount === 21092 && p.vendor_name === 'ロジマート');
check('規約ファイル名 (大文字拡張子)', parseVoucherFileName('2026-08-08_21092_ロジマート.PDF')?.vendor_name === 'ロジマート');
check('規約外は null', parseVoucherFileName('IMG_1234.jpg') === null);
check('存在しない日付のファイル名は null', parseVoucherFileName('2026-02-30_100_x.pdf') === null);
check('isValidDate', isValidDate('2026-02-28') && !isValidDate('2026-02-30') && !isValidDate('2026-8-1'));

// 種別判定・base64
const pdf = Buffer.from('%PDF-1.4 dummy');
check('sniff: PDF/JPEG/PNG/その他', sniffKind(pdf)?.ext === 'pdf' && sniffKind(Buffer.from([0xFF, 0xD8, 0xFF, 0xE0]))?.ext === 'jpg'
  && sniffKind(Buffer.from([0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0]))?.ext === 'png' && sniffKind(Buffer.from('hello')) === null);
check('base64 厳密復号 (壊れた文字列は null)', decodeBase64Strict(pdf.toString('base64'))?.equals(pdf) && decodeBase64Strict('!!!') === null && decodeBase64Strict('') === null);

// 受け箱
const a = addToInbox(pdf, { file_name: '2026-08-08_21092_ロジマート.pdf', mime: 'text/html', vendor_id: 1, doc_date: '2026-08-08', amount: 21092 });
check('受け箱に入る (status=new・mimeは中身から決まる)', !a.duplicate && a.row.status === 'new' && a.row.mime === 'application/pdf' && a.row.ext === 'pdf');
check('読み出し時に sha256 を検証して返す', readFile(a.row).equals(pdf));
const dupe = addToInbox(pdf, { file_name: 'same.pdf' });
check('同じ内容は二重登録しない', dupe.duplicate && dupe.row.id === a.row.id);
let threw = false;
try { addToInbox(Buffer.from('<html>'), { file_name: 'x.pdf' }); } catch (e) { threw = e.message === 'unsupported_file'; }
check('PDF/JPEG/PNG 以外は拒否', threw);
check('金額の上限・非整数は弾く', addToInbox(Buffer.from('%PDF-1.4 b'), { file_name: 'b.pdf', amount: 1e12 }).row.amount === null
  && addToInbox(Buffer.from('%PDF-1.4 c'), { file_name: 'c.pdf', amount: '1234.6' }).row.amount === 1235);
check('既定は提案モード (auto_attach OFF)', autoAttachEnabled() === false);
setSetting('auto_attach', '1');
check('auto_attach を ON にできる', autoAttachEnabled() === true);
setMatch(a.row.id, { status: 'proposed', tx_id: 't1', journal_id: 'j1', journal_number: 480, strength: 'strong', reason: 'vendor+amount+date', candidates: [{ tx_id: 't1' }] });
check('owners に proposed の明細と所有者が入る', transactionOwners().get('t1')?.has(a.row.id));
threw = false;
try { updateInboxMeta(a.row.id, { doc_date: '2026-13-01' }); } catch (e) { threw = e.message === 'bad_date'; }
check('存在しない日付への修正は bad_date', threw);
const upd = updateInboxMeta(a.row.id, { amount: 21000 });
check('メタ修正で new に戻り突合結果が消える', upd.status === 'new' && upd.amount === 21000 && upd.match_tx_id === '');

// 添付の確保 (リース) — 二重添付防止
const token = claimForAttach(a.row.id);
check('確保できる (attaching)', token && getInbox(a.row.id).status === 'attaching');
check('二重に確保できない', claimForAttach(a.row.id) === null);
check('確保中は突合結果で上書きされない', (setMatch(a.row.id, { status: 'no_match' }), getInbox(a.row.id).status === 'attaching'));
check('確保中はメタを直せない', (() => { try { updateInboxMeta(a.row.id, { amount: 1 }); return false; } catch (e) { return e.message === 'already_attached'; } })());
check('偽トークンでは attached にできない', markAttached(a.row.id, 'bogus', { journal_id: 'j1', mf_file_id: 'f', mode: 'auto' }) === false);
releaseClaim(a.row.id, token, 'mf_api_500');
check('失敗時は error に戻る', getInbox(a.row.id).status === 'error' && getInbox(a.row.id).error === 'mf_api_500');
const token2 = claimForAttach(a.row.id);
releaseClaim(a.row.id, token2, '');
check('エラーなしの解放は元の状態 (error) に戻る', getInbox(a.row.id).status === 'error');
const token3 = claimForAttach(a.row.id);
check('正しいトークンで attached', markAttached(a.row.id, token3, { journal_id: 'j1', journal_number: 480, tx_id: 't1', mf_file_id: 'f1', mode: 'manual', actor: 'test' }) === true
  && listInbox({ status: 'attached' }).length === 1 && countByStatus().attached === 1);
threw = false;
try { updateInboxMeta(a.row.id, { amount: 1 }); } catch (e) { threw = e.message === 'already_attached'; }
check('添付済みは直せない', threw);
// 添付済み明細の一意制約 (同じ明細に2つ目を attached にできない)
const c2 = addToInbox(Buffer.from('%PDF-1.4 second'), { file_name: 'second.pdf' });
const tk = claimForAttach(c2.row.id);
threw = false;
try { markAttached(c2.row.id, tk, { journal_id: 'j1', tx_id: 't1', mf_file_id: 'f2', mode: 'manual' }); } catch (e) { threw = /UNIQUE/i.test(e.message); }
check('同じ明細への2件目の attached は DB が拒否', threw);
// 途中で落ちた確保の回収
check('起動時に attaching を error に戻す', recoverStaleClaims() === 1 && getInbox(c2.row.id).status === 'error');

// 期間計算
const [ps, pe] = periodFor([{ doc_date: '2026-99-99', created_at: '2026-08-20T00:00:00Z' }, { doc_date: '2026-08-01', created_at: '2026-08-21T00:00:00Z' }], Date.parse('2026-08-28T00:00:00Z'));
check('periodFor: 無効日付は登録日にフォールバック・前後7日', ps === '2026-07-25' && pe === '2026-08-27');
check('periodFor: 空でも落ちない', Array.isArray(periodFor([], Date.parse('2026-08-28T00:00:00Z'))));

console.log(failed ? `\n${failed}件NG` : '\n全件パス');
process.exit(failed ? 1 : 0);
