/**
 * shohyo-links — 証憑を受け箱に入れる共通入口 (画面アップロード / Gドライブ / ロボット / メール)
 *
 * addToInbox (保存・重複防止) → 足りない項目を extract で読む (ファイル名規約 → PDFルール → AI)。
 * 人が入れた値は上書きしない。読めなくても受け入れは成功する (人が「直す」で入れる)。
 */
import { addToInbox, getInbox, updateInboxMeta, setExtractNote } from './inbox.js';
import { parseVoucherFileName, resolveVendorId } from './matcher.js';
import { extractVoucher } from './extract.js';
import { listLinks } from './db.js';

/** 中身を読んで、空の項目だけ埋める。読み取り結果 (applied=埋めた項目) を返す */
export async function applyExtraction(row, buffer) {
  try {
    const ex = await extractVoucher(buffer, row.ext, { fileName: row.file_name });
    const patch = {};
    if (!row.doc_date && ex.doc_date) patch.doc_date = ex.doc_date;
    if (!row.amount && ex.amount) patch.amount = ex.amount;
    if (!row.vendor_id && !row.vendor_name && ex.vendor_name) patch.vendor_name = ex.vendor_name;
    // 支払先名 (AI/ファイル名) をマスタに結びつける。マスタ名はカード明細の表記なので突合が強くなる
    const nameForResolve = patch.vendor_name || row.vendor_name;
    if (!row.vendor_id && nameForResolve) {
      const vid = resolveVendorId(nameForResolve, listLinks());
      if (vid) { patch.vendor_id = vid; ex.notes = [...(ex.notes || []), `マスタ#${vid} に結びつけ`]; }
    }
    if (Object.keys(patch).length) updateInboxMeta(row.id, patch);
    setExtractNote(row.id, (ex.notes || []).join(' / '));
    console.log(`[shohyo-extract] inbox#${row.id} ${row.file_name}: ${(ex.notes || []).join(' / ')} → ${Object.keys(patch).join(',') || 'なし'}`);
    return { ...ex, applied: Object.keys(patch) };
  } catch (e) {
    console.warn('[shohyo-links] extract failed', row.id, e.message);
    setExtractNote(row.id, `読み取りエラー: ${String(e.message).slice(0, 200)}`);
    return { source: 'none', error: String(e.message).slice(0, 200), applied: [] };
  }
}

/**
 * @param {Buffer} buffer
 * @param {{ file_name, source, vendor_id?, vendor_name?, doc_date?, amount?, note? }} meta
 * @returns {{ row, duplicate, extracted }}
 */
export async function ingestVoucher(buffer, meta) {
  const parsed = parseVoucherFileName(meta.file_name) || {};
  let { row, duplicate } = addToInbox(buffer, {
    ...meta,
    vendor_name: meta.vendor_name ?? parsed.vendor_name,
    doc_date: meta.doc_date ?? parsed.doc_date,
    amount: meta.amount ?? parsed.amount,
  });
  let extracted = null;
  if (!duplicate && (!row.doc_date || !row.amount || !(row.vendor_id || row.vendor_name))) {
    extracted = await applyExtraction(row, buffer);
    row = getInbox(row.id);
  }
  return { row, duplicate, extracted };
}
