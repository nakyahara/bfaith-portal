/**
 * test-reprint.mjs — 🖨伝票再印刷依頼 (2026-08-21) の検証
 * 実行: node apps/packing/tests/test-reprint.mjs
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'packing-reprint-test-'));
process.env.DATA_DIR = tmpDir;
delete process.env.PACKING_REPRINT_WEBHOOK;

const { initPackingDB, getDB, utcNow } = await import('../db.js');
const { applyEvent, PackError } = await import('../service.js');
const { findLabelPageAcross, decideLabelPage } = await import('../reprint-pdf.js');
const { notifyReprint } = await import('../notify.js');

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };
const eq = (a, b, label) => ok(JSON.stringify(a) === JSON.stringify(b), `${label} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);
const throws = (fn, code, label) => {
  try { fn(); ok(false, `${label} (エラーにならなかった)`); }
  catch (e) { ok(e instanceof PackError && e.code === code, `${label} (${e.code || e.message})`); }
};

initPackingDB();
const db = getDB();
const now = utcNow();
db.prepare(`INSERT INTO pk_pack_batches (id, tb_key, folder_name, work_date, slip_count, line_count, total_qty,
  match_status, status, worker, csv_sha256, imported_by, created_at, updated_at)
  VALUES (1, 'TB1', '出荷_02', '2026-08-21', 1, 1, 1, 'ok', 'packing', '大場', 'x', 't', ?, ?)`).run(now, now);
db.prepare(`INSERT INTO pk_pack_slips (batch_id, seq, ne_slip_no, slip_no, recipient_name, site_order_no, status)
  VALUES (1, 1, '1507800', 'SP1', '山田 太郎', 'R123-456', 'pending')`).run();

let op = 0;
const ev = (event, extra = {}, worker = '大場') =>
  applyEvent(1, { opId: `t${++op}`, event, slipSeq: 1, ...extra }, worker);

console.log('── reprint イベント ──');
{
  const r = ev('reprint');
  ok(Number.isInteger(r.reprintId), '記録される (reprintId返却)');
  const row = db.prepare('SELECT * FROM pk_pack_reprints WHERE id=?').get(r.reprintId);
  eq([row.ne_slip_no, row.site_order_no, row.folder_name, row.recipient_name, row.requested_by],
    ['1507800', 'R123-456', '出荷_02', '山田 太郎', '大場'], 'NE伝票/モール伝票/出荷NO/送り先/依頼者を保存');
  ok(db.prepare('SELECT status FROM pk_pack_slips WHERE batch_id=1 AND seq=1').get().status === 'pending',
    '伝票状態は変えない');
  throws(() => ev('reprint', { slipSeq: 99 }), 'slip_not_found', '存在しない伝票は404');
  throws(() => ev('reprint', {}, '別人'), 'taken', '担当者以外は不可');
}

console.log('\n── findLabelPageAcross (全ファイル横断・全体で一意のみ採用) ──');
{
  const sets = [
    { pages: ['ヤマト 1507799 佐藤 花子', 'ヤマト 1507800 山田 太郎'] },
    { pages: ['ヤマト 1507801 鈴木 一郎'] },
  ];
  eq(findLabelPageAcross(sets, { neSlipNo: '1507800', recipientName: '山田 太郎' }),
    { file: 0, page: 1, by: 'slip_no' }, '伝票番号で一意特定 (ファイル横断)');
  eq(findLabelPageAcross(sets, { neSlipNo: '9999999', recipientName: '鈴木一郎' }),
    { file: 1, page: 0, by: 'name' }, '番号が無ければ氏名 (空白ゆれ無視) で特定');
  eq(findLabelPageAcross([{ pages: ['A 1507800'] }, { pages: ['B 1507800'] }],
    { neSlipNo: '1507800', recipientName: '' }), null, '別ファイルにも同番号=複数一致は採用しない');
  eq(findLabelPageAcross([{ pages: ['番号 11507800 続き'] }], { neSlipNo: '1507800', recipientName: '' }),
    null, '数字境界: 部分文字列一致では拾わない');
  eq(findLabelPageAcross([{ pages: ['山田 太郎', '山田 太郎'] }], { neSlipNo: '', recipientName: '山田太郎' }),
    null, '氏名の複数一致も特定しない (誤添付防止)');
  eq(findLabelPageAcross([{ pages: ['', ''] }], { neSlipNo: '1234', recipientName: '山田' }),
    null, 'テキスト層なしは特定しない');
}

console.log('\n── 通知 (webhook未設定はfalse) ──');
{
  const sent = await notifyReprint({ folderName: '出荷_02', slipSeq: 1, neSlipNo: '1507800', worker: '大場' });
  eq(sent, false, 'PACKING_REPRINT_WEBHOOK未設定は送らずfalse');
}

console.log('\n[decideLabelPage: 位置対応フォールバック (AES=テキスト層なし)]');
{
  const imgOnly = [{ filename: 'AES送り状_並び替え済.pdf', pages: ['', '', '', '', '', '', '', '', ''] }];
  eq(decideLabelPage(imgOnly, { neSlipNo: '1532160', recipientName: '永寿', slipSeq: 8, slipCount: 9 }),
    { file: 0, page: 7, by: 'position' }, '並び替え済+件数一致 → 伝票seq=ページ位置');
  eq(decideLabelPage(imgOnly, { neSlipNo: '1532160', recipientName: '永寿', slipSeq: 8, slipCount: 10 }),
    null, 'ページ数≠伝票数 (欠けあり) は位置対応しない');
  eq(decideLabelPage([{ filename: 'ネコポス送り状.pdf', pages: ['', '', ''] }],
    { neSlipNo: '1', recipientName: 'x', slipSeq: 2, slipCount: 3 }),
    null, '並び替え済以外 (順序保証なし) は位置対応しない');
  eq(decideLabelPage(imgOnly, { neSlipNo: '1532160', recipientName: '永寿', slipSeq: null, slipCount: 9 }),
    null, 'slipSeq無しは不発動');
  // テキスト照合が効く場合はそちらが優先
  eq(decideLabelPage([{ filename: 'AES送り状_並び替え済.pdf', pages: ['a 1532160 b', '', ''] }],
    { neSlipNo: '1532160', recipientName: '', slipSeq: 3, slipCount: 3 }),
    { file: 0, page: 0, by: 'slip_no' }, 'テキスト一致が優先');
}

db.close();
try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* 無視 */ }
console.log(`\n${failed === 0 ? '✅ 全テスト PASS' : `❌ ${failed} 件失敗`}`);
process.exitCode = failed === 0 ? 0 : 1;
