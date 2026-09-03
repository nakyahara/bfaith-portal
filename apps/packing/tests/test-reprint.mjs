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
const { findLabelPageAcross, decideLabelPage, fitToLabel, labelFitBox, pageHasContent } = await import('../reprint-pdf.js');
const { PDFDocument, rgb } = await import('pdf-lib');
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
  eq(row.kind, 'reprint', 'kind=reprint');
}

console.log('── label_missing イベント (📭送り状がない・2026-08-26) ──');
{
  const r = ev('label_missing');
  ok(Number.isInteger(r.reprintId), '記録される (reprintId返却・再印刷と同じ表)');
  const row = db.prepare('SELECT * FROM pk_pack_reprints WHERE id=?').get(r.reprintId);
  eq([row.kind, row.ne_slip_no, row.requested_by], ['label_missing', '1507800', '大場'], 'kind=label_missing で保存');
  ok(db.prepare('SELECT status FROM pk_pack_slips WHERE batch_id=1 AND seq=1').get().status === 'pending',
    '伝票状態は変えない');
  throws(() => ev('label_missing', {}, '別人'), 'taken', '担当者以外は不可');
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

console.log('\n[labelFitBox: ラベルより大きいページだけラベル実寸に収める]');
{
  const MM = 72 / 25.4;
  const mm = (pt) => Math.round((pt / MM) * 10) / 10;
  const box = (wMm, hMm, label, margin) => {
    const b = labelFitBox(wMm * MM, hMm * MM, label, margin);
    return b && { label: [mm(b.labelW), mm(b.labelH)], size: [mm(b.w), mm(b.h)], at: [mm(b.x), mm(b.y)] };
  };

  // A4 (210×297) → 幅で決まる倍率 (96/210) で 96×135.8mm・左右余白2mm・上下は中央寄せ
  eq(box(210, 297), { label: [100, 150], size: [96, 135.8], at: [2, 7.1] },
    'A4の送り状は 100×150mm ラベルの内側に収まる (幅で決まる倍率・縦横比は保つ)');
  // 縦長ページは高さで倍率が決まる = 幅いっぱいまで広げない
  eq(box(200, 600), { label: [100, 150], size: [48.7, 146], at: [25.7, 2] },
    '縦長ページは高さで倍率が決まる');

  eq(box(100, 150), null, 'ラベル実寸ちょうどのページは触らない (等倍が正しい)');
  eq(box(60, 100), null, 'ラベルより小さいページは触らない');
  // 1mm以内の超過は倍率1.0のままページ枠だけ実寸にする (バーコードを縮めない)
  const near = box(100.5, 150.5);
  eq([near.label, near.size], [[100, 150], [100.5, 150.5]],
    '1mm以内の超過は等倍のままページ枠だけラベル実寸にする');
  ok(near.at[0] < 0 && near.at[1] < 0, '等倍のはみ出しは中央寄せ (上下左右へ均等に落ちる)');
  ok(!!box(105, 150), '幅だけ超えていても収め直す');
  ok(!!box(100, 160), '高さだけ超えていても収め直す');
  // ラベル実寸・余白は env で変えられる = 別サイズのロールに載せ替えても追随する
  eq(box(210, 297, { w: 60, h: 90 }, 1), { label: [60, 90], size: [58, 82], at: [1, 4] },
    '別のラベル実寸・余白を渡せばそのサイズに収まる');
}

console.log('\n[fitToLabel: 実際のPDFページを収め直す]');
{
  const MM = 72 / 25.4;
  const mm = (pt) => Math.round((pt / MM) * 10) / 10;
  /** 中身入りの1ページPDF (幅×高さ mm・回転・CropBox) を作る */
  const makePdf = async (wMm, hMm, { rotate = 0, crop = null } = {}) => {
    const d = await PDFDocument.create();
    const pg = d.addPage([wMm * MM, hMm * MM]);
    pg.drawRectangle({ x: 10 * MM, y: 20 * MM, width: (wMm - 20) * MM, height: (hMm - 40) * MM, color: rgb(0, 0, 0) });
    if (rotate) pg.setRotation({ type: 'degrees', angle: rotate });
    if (crop) pg.setCropBox(crop[0] * MM, crop[1] * MM, crop[2] * MM, crop[3] * MM);
    return PDFDocument.load(await d.save());
  };
  // 保存 → 読み直して確かめる (書き出したPDFが壊れていないこと自体もここで見る)
  const saved = async (doc) => PDFDocument.load(await doc.save());
  const size = (doc) => { const s = doc.getPage(0).getSize(); return [mm(s.width), mm(s.height)]; };

  const fitted = await saved(await fitToLabel(await makePdf(210, 297)));
  eq(size(fitted), [100, 150], 'A4の送り状はラベル実寸 100×150mm のページになる');
  eq(fitted.getPage(0).getRotation().angle, 0, '出来上がりに /Rotate は残さない (等倍で刷れる形)');
  ok(pageHasContent(fitted.getPage(0)), '中身 (Form XObject) が入っている');

  eq(await fitToLabel(await makePdf(100, 150)), null, 'ラベル実寸のページはそのまま渡す');

  // /Rotate 付き = 見たままの向きで収める。90/270 は縦横が入れ替わる
  for (const angle of [90, 180, 270]) {
    const r = await saved(await fitToLabel(await makePdf(210, 297, { rotate: angle })));
    eq(size(r), [100, 150], `回転${angle}° のA4もラベル実寸に収める`);
    eq(r.getPage(0).getRotation().angle, 0, `回転${angle}° の出来上がりに /Rotate は残さない`);
  }

  // CropBox がある = そこが印字領域。MediaBox (トンボ込みA4) 基準で縮めない
  const cropped = await fitToLabel(await makePdf(210, 297, { crop: [5, 5, 105, 155] }));
  eq(size(await saved(cropped)), [100, 150], 'CropBox付きでもページはラベル実寸');
  // CropBox は 100×150mm = ラベル実寸ちょうど → 1mm以内なので等倍のまま
  eq(await fitToLabel(await makePdf(210, 297, { crop: [5, 5, 100, 150] })), null,
    'CropBoxがラベルに収まっていれば触らない (MediaBoxのA4に引っ張られない)');
}

db.close();
try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* 無視 */ }
console.log(`\n${failed === 0 ? '✅ 全テスト PASS' : `❌ ${failed} 件失敗`}`);
process.exitCode = failed === 0 ? 0 : 1;
