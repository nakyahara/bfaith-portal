/**
 * 🖨 伝票再印刷 — 送り状PDFから該当ページだけを抜き出す (fail-soft・誤添付はfail-closed)。
 *
 * 出荷_XX フォルダの `*送り状*.pdf` (AES送り状_並び替え済.pdf / ネコポス / 宅急便) を対象に、
 * **全候補ファイルの全ページ**からテキスト層で照合し、「全体でちょうど1ページ」一致した
 * ときだけ抜き出す (Codexレビュー high: ファイル毎の early-return は別ファイルの同番号を
 * 見落として誤添付し得る)。NE伝票番号は数字境界つきの完全一致相当で判定する。
 *
 * SA は閲覧者共有のため Drive へは保存できない — DATA_DIR/reprints/<token>.pdf に置き、
 * 推測不能トークンつきURL (/apps/packing/reprints/:token.pdf) で事務が開く。
 */
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { PDFDocument } from 'pdf-lib';
import { DATA_DIR } from './db.js';
import { getShippingFolders, driveCall } from './drive.js';
import { listDriveFilesAcross, downloadDriveFileById } from '../../lib/drive-csv.js';

export const REPRINTS_DIR = path.join(DATA_DIR, 'reprints');

const norm = (s) => String(s || '').replace(/[\s　]/g, '').toLowerCase();
const escapeRe = (s) => String(s).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

/** ページごとのテキストを抽出 (pdf-parse の pagerender)。テキスト層なしは空文字。 */
export async function extractPageTexts(buffer) {
  const { default: pdfParse } = await import('pdf-parse/lib/pdf-parse.js');
  const pages = [];
  await pdfParse(buffer, {
    pagerender: (pageData) => pageData.getTextContent().then((tc) => {
      const text = tc.items.map((it) => it.str).join(' ');
      pages.push(text);
      return text;
    }),
  });
  return pages;
}

/**
 * 全候補ファイル横断で該当ページを特定 (純関数・テスト対象)。
 * @param fileSets [{pages: string[]}] — ファイルごとのページテキスト
 * @returns {{file: number, page: number, by: 'slip_no'|'name'}|null} 全体で一意のときだけ返す
 */
export function findLabelPageAcross(fileSets, { neSlipNo, recipientName }) {
  const collect = (test) => {
    const hits = [];
    fileSets.forEach((f, fi) => f.pages.forEach((t, pi) => { if (test(t)) hits.push({ file: fi, page: pi }); }));
    return hits;
  };
  const slipDigits = String(neSlipNo || '').replace(/\D/g, '');
  if (slipDigits.length >= 4) {
    // 数字境界つき (前後が数字でない) — 「1507800」が「11507800…」に部分一致しないように
    const re = new RegExp(`(^|[^0-9])${escapeRe(slipDigits)}([^0-9]|$)`);
    const hits = collect((t) => re.test(norm(t)));
    if (hits.length === 1) return { ...hits[0], by: 'slip_no' };
    if (hits.length > 1) return null;   // 複数一致 = 誤添付リスク → 氏名にも進まない
  }
  const nameKey = norm(recipientName);
  if (nameKey.length >= 2) {
    const hits = collect((t) => norm(t).includes(nameKey));
    if (hits.length === 1) return { ...hits[0], by: 'name' };
  }
  return null;
}

/**
 * 該当ページの決定 (純関数・テスト対象)。テキスト照合 → 位置対応フォールバックの順。
 * 位置対応は「AES送り状_並び替え済」(並び替えツールが**納品書順**で出力・照合できたページのみ)
 * かつ **ページ数=バッチ伝票数が完全一致** のときだけ (欠けがあると位置がズレるため fail-closed)。
 * AESの送り状は画像のみでテキスト層が無い (実測 2026-08-21) — 位置対応が唯一の手段
 */
export function decideLabelPage(fileSets, { neSlipNo, recipientName, slipSeq = null, slipCount = null }) {
  const hit = findLabelPageAcross(fileSets, { neSlipNo, recipientName });
  if (hit) return hit;
  const si = fileSets.findIndex((f) => String(f.filename || '').includes('並び替え済'));
  if (si >= 0 && Number.isInteger(slipSeq) && Number.isInteger(slipCount)
    && fileSets[si].pages.length === slipCount && slipSeq >= 1 && slipSeq <= slipCount) {
    return { file: si, page: slipSeq - 1, by: 'position' };
  }
  return null;
}

/**
 * 出荷フォルダの送り状PDFから該当ページを抜き出して配信トークンを返す。
 * 特定できない・失敗は throw (呼び出し側が握って「手動で印刷して」通知にする)。
 */
export async function extractReprintPdf({ folderName, neSlipNo, recipientName, slipSeq = null, slipCount = null }) {
  const folders = (await getShippingFolders()).filter((f) => f.name === folderName);
  if (folders.length === 0) throw new Error(`Driveフォルダ ${folderName} が見つかりません`);
  const files = (await driveCall(() => listDriveFilesAcross({ folders, nameContains: '送り状' })))
    .filter((f) => /\.pdf$/i.test(f.filename))
    .sort((a, b) => String(b.modified_time || '').localeCompare(String(a.modified_time || '')))
    .slice(0, 3);   // 新しい順に最大3ファイル
  if (files.length === 0) throw new Error('送り状PDFがフォルダにありません');
  const fileSets = [];
  for (const f of files) {
    try {
      // downloadDriveFileById は {buffer, filename, ...} を返す (生Bufferではない — 実機バグ 2026-08-21)。
      // 送り状PDFは繁忙日に大きくなるため上限も CSV既定20MB から引き上げる
      const dl = await driveCall(() => downloadDriveFileById({
        fileId: f.file_id, folderIds: folders.map((x) => x.folder_id), maxBytes: 60 * 1024 * 1024,
      }));
      fileSets.push({ filename: f.filename, buf: dl.buffer, pages: await extractPageTexts(dl.buffer) });
    } catch (e) {
      console.warn(`[packing-reprint] ${f.filename} の読込失敗: ${e.message}`);
    }
  }
  if (fileSets.length === 0) throw new Error('送り状PDFを読み込めませんでした');
  const hit = decideLabelPage(fileSets, { neSlipNo, recipientName, slipSeq, slipCount });
  if (!hit) throw new Error('該当ページを一意に特定できません (テキスト0件/複数一致・位置対応も条件不成立)');
  const src = await PDFDocument.load(fileSets[hit.file].buf);
  const out = await PDFDocument.create();
  const [page] = await out.copyPages(src, [hit.page]);
  out.addPage(page);
  const bytes = await out.save();
  const token = crypto.randomBytes(16).toString('base64url');
  fs.mkdirSync(REPRINTS_DIR, { recursive: true });
  fs.writeFileSync(path.join(REPRINTS_DIR, `${token}.pdf`), bytes);
  return { token, by: hit.by, file: fileSets[hit.file].filename };
}

/** 古い抜き出しPDFの掃除 (7日超)。ポーラーから呼ばれる (fail-soft)。 */
export function cleanupReprintPdfs(maxAgeDays = 7) {
  try {
    if (!fs.existsSync(REPRINTS_DIR)) return 0;
    const limit = Date.now() - maxAgeDays * 24 * 3600 * 1000;
    let n = 0;
    for (const f of fs.readdirSync(REPRINTS_DIR)) {
      const p = path.join(REPRINTS_DIR, f);
      try {
        if (fs.statSync(p).mtimeMs < limit) { fs.unlinkSync(p); n++; }
      } catch { /* 個別失敗は無視 */ }
    }
    return n;
  } catch {
    return 0;
  }
}
