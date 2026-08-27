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
import { PDFDocument, PDFName, PDFDict } from 'pdf-lib';
import { DATA_DIR } from './db.js';
import { getShippingFolders, driveCall } from './drive.js';
import { listDriveFilesAcross, downloadDriveFileById } from '../../lib/drive-csv.js';

export const REPRINTS_DIR = path.join(DATA_DIR, 'reprints');

// AES並び替えツール (apps/aes-pdf-sorter) が出力ページ↔注文番号の対応表を書き出す。
// 位置推定 (ページ数=伝票数なら page=seq-1) は、照合できなかった注文がページごと落ちると
// 1ページずつズレて**別人の送り状**を掴むため、自動印刷の根拠には使えない
export const MANIFEST_NAME = 'AES送り状_並び替え済_manifest.json';
export const SORTED_PDF_NAME = 'AES送り状_並び替え済.pdf';
export const SORTER_ERROR_TXT = 'AES送り状_エラー.txt';

// 白紙判定のしきい値 (manifest の ink_ratio = 低解像度描画の非白ピクセル率)。
// 実データを見て調整できるよう env で上書き可能にするが、envの打ち間違いで安全境界が
// 壊れないよう (0,1] の有限数に限る。範囲外・非数は既定値に戻す (Codexレビュー指摘1)
const DEFAULT_MIN_INK_RATIO = 0.005;
export const MIN_INK_RATIO = (() => {
  const raw = process.env.PACKING_MIN_INK_RATIO;
  if (raw == null || raw === '') return DEFAULT_MIN_INK_RATIO;
  const v = Number(raw);
  if (!Number.isFinite(v) || v <= 0 || v > 1) {
    console.warn(`[packing-reprint] PACKING_MIN_INK_RATIO=${raw} は不正 (0<x<=1) → 既定 ${DEFAULT_MIN_INK_RATIO} を使います`);
    return DEFAULT_MIN_INK_RATIO;
  }
  return v;
})();

/** 送り状が白紙・対応表が使えない等で「印刷してはいけない」と判定したときのエラー */
export class LabelUnusableError extends Error {
  constructor(reason, detail) {
    super(detail || reason);
    this.name = 'LabelUnusableError';
    this.reason = reason;      // 'blank' | その他の検証失敗コード
  }
}

const norm = (s) => String(s || '').replace(/[\s　]/g, '').toLowerCase();
const escapeRe = (s) => String(s).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
const sha256 = (buf) => crypto.createHash('sha256').update(buf).digest('hex');
const ms = (v) => { const t = Date.parse(v || ''); return Number.isFinite(t) ? t : null; };

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
 * manifest (出力ページ→注文番号の対応表) を検証して該当ページを決める (純関数・テスト対象)。
 *
 * ⭐ここを通ったページだけが「自動印刷してよい」。1つでも欠けたら印刷しない (fail-closed)。
 * Drive障害で「旧PDFの失効」と「旧manifestの失効」が**両方**失敗すると、ハッシュの整合が
 * 取れた古い成果物が対で残り得るため、sha256 だけでなく世代も見る:
 *   - エラーtxtがmanifestより新しい = 並び替えが失敗した回
 *   - manifestが記録している納品書 (name+更新時刻) が、いまフォルダにある納品書と一致するか
 *     → 「この対応表はいまの納品書から作られたか」を順序に依存せず確かめられる。
 *       梱包バッチの取込時刻との前後比較は、並び替えと取込のどちらが先かが運用で変わるため
 *       使わない (常に古い判定になって新経路が一度も有効にならない事故を避ける・Codex指摘5)
 *
 * @returns {{ok: true, page: number, inkRatio: number}|{ok: false, reason: string}}
 *          page は0始まり (PDFの何ページ目か)
 */
export function verifyManifest(manifest, {
  folderName, siteOrderNo, pdfSha256, pdfPageCount = null,
  manifestModifiedMs = null, errorTxt = null, invoiceFiles = null,
  minInkRatio = MIN_INK_RATIO,
} = {}) {
  const bad = (reason) => ({ ok: false, reason });
  if (!Number.isFinite(minInkRatio) || minInkRatio <= 0 || minInkRatio > 1) return bad('白紙しきい値の設定が不正です');
  if (!manifest || typeof manifest !== 'object') return bad('manifestなし');
  if (manifest.invalid === true) return bad('manifestが失効しています (並び替えが失敗した回)');
  // 型は変換せずそのまま見る ("1" や true を数値として受理しない・Codexレビュー2巡目)
  if (manifest.version !== 1) return bad(`manifestのversionが未知 (${manifest.version})`);
  if (!Array.isArray(manifest.pages) || manifest.pages.length === 0) return bad('manifestにpagesがありません');
  if (manifest.pages.some((p) => !p || typeof p !== 'object')) return bad('manifestのpagesが壊れています');
  if (typeof pdfSha256 !== 'string' || manifest.output_pdf_sha256 !== pdfSha256) {
    return bad('manifestが送り状PDFと一致しません (作り直された可能性)');
  }
  // 欠けているメタは「検証をスキップ」ではなく不合格にする (Codexレビュー指摘3)
  if (!manifest.folder_name) return bad('manifestに出荷フォルダ名がありません');
  if (manifest.folder_name !== folderName) return bad(`manifestの出荷フォルダが違います (${manifest.folder_name})`);
  if (ms(manifest.generated_at) == null) return bad('manifestの生成時刻が不正です');

  // 並び替えの失敗記録との突き合わせ。
  // ⚠ 並び替えツールは**成功時にもエラーtxtを「解消済み」で上書きする**ため、
  //   時刻の前後だけで判定すると一度でも失敗したフォルダは永久に不合格になる
  //   (= 新経路が一度も有効にならない)。中身で失敗/解消を見分ける (Codexレビュー2巡目)
  if (errorTxt) {
    if (manifestModifiedMs == null || errorTxt.modifiedMs == null) return bad('並び替え記録の更新時刻が取れません');
    if (errorTxt.isFailure == null) return bad('並び替え記録の内容を判別できません');
    if (errorTxt.isFailure && errorTxt.modifiedMs > manifestModifiedMs) {
      return bad('並び替えエラーの記録がmanifestより新しい (失敗した回の可能性)');
    }
  }

  // いまフォルダにある納品書と、manifestが作られたときの納品書が同じか。
  // 欠損を空文字で埋めて一致させないよう、両側とも要素を厳格に検証してから比較する
  const normalizeInvoices = (list) => {
    if (!Array.isArray(list) || list.length === 0) return null;
    const rows = [];
    for (const f of list) {
      if (!f || typeof f !== 'object') return null;
      const name = f.name ?? f.filename;
      const mtime = f.modified_time ?? f.modifiedTime;
      if (typeof name !== 'string' || name === '') return null;
      if (typeof mtime !== 'string' || ms(mtime) == null) return null;
      rows.push([name, ms(mtime)]);
    }
    rows.sort((a, b) => (a[0] === b[0] ? a[1] - b[1] : (a[0] < b[0] ? -1 : 1)));
    return JSON.stringify(rows);
  };
  const nowInvoices = normalizeInvoices(invoiceFiles);
  if (nowInvoices == null) return bad('納品書が見つからず世代を確認できません');
  const manifestInvoices = normalizeInvoices(manifest.invoice_files);
  if (manifestInvoices == null) return bad('manifestに納品書の記録がありません (古い形式・壊れている)');
  if (manifestInvoices !== nowInvoices) return bad('manifestが今の納品書から作られていません (作り直しが必要)');

  // ページ構成の整合 (壊れた対応表を自動印刷の根拠にしない・Codexレビュー指摘4)
  const total = manifest.page_count;
  if (!Number.isInteger(total) || total < 1) return bad(`manifestのpage_countが不正 (${manifest.page_count})`);
  if (manifest.pages.length !== total) return bad(`manifestのページ数が合いません (${manifest.pages.length}/${total})`);
  if (pdfPageCount != null && pdfPageCount !== total) return bad(`送り状PDFのページ数と合いません (${pdfPageCount}/${total})`);
  const nums = manifest.pages.map((p) => p.page);
  if (nums.some((n) => !Number.isInteger(n) || n < 1 || n > total)) return bad('manifestに範囲外のページ番号があります');
  if (new Set(nums).size !== total) return bad('manifestにページ番号の重複があります');

  const key = norm(siteOrderNo);
  if (!key) return bad('モール伝票番号が空です');
  const hits = manifest.pages.filter((p) => norm(p.order_number) === key);
  if (hits.length === 0) return bad('この注文番号がmanifestにありません');
  if (hits.length > 1) return bad(`この注文番号がmanifestに${hits.length}件あります`);
  const hit = hits[0];

  // ink_ratio は「そのページを低解像度で描画した非白ピクセル率」。null は判定不能であって
  // 白紙の証明ではないが、確かめられない以上は自動印刷しない。
  // NaN/Infinity/範囲外は「比較が常に false になって素通り」するので明示的に弾く (Codex指摘1)
  if (hit.ink_ratio == null) return bad('白紙かどうか判定できません (ink_ratioなし)');
  const ink = hit.ink_ratio;
  if (typeof ink !== 'number' || !Number.isFinite(ink) || ink < 0 || ink > 1) {
    return bad(`ink_ratioが不正です (${hit.ink_ratio})`);
  }
  if (ink < minInkRatio) return { ok: false, reason: 'blank', inkRatio: ink };
  return { ok: true, page: hit.page - 1, inkRatio: ink };
}

/**
 * 並び替えツールのエラーtxtが「失敗の記録」か「解消済み」かを見分ける (純関数・テスト対象)。
 * 成功時にも同じファイル名で「解消済み」が書かれるため、存在と時刻だけでは判定できない。
 * @returns {boolean|null} true=失敗の記録 / false=解消済み / null=判別不能 (fail-closed側で扱う)
 */
export function isSorterFailureText(text) {
  const t = String(text || '').replace(/^﻿/, '');
  if (t === '') return null;
  if (t.includes('解消済み')) return false;
  if (t.includes('失敗しました') || t.includes('❌')) return true;
  return null;
}

/**
 * 抜き出した1ページに中身があるか (pdf-lib の構造チェック・テスト対象)。
 * 画像XObject (Form XObject 経由も再帰で辿る) かフォントか、十分な長さの content stream が
 * あれば「中身あり」。「画像がある=正しい送り状」ではないので、これは**明白な空を弾く**用途。
 */
export function pageHasContent(page) {
  try {
    const node = page.node;
    const contents = node.Contents();
    let len = 0;
    if (contents) {
      const arr = typeof contents.asArray === 'function' ? contents.asArray() : [contents];
      for (const c of arr) len += (c?.getContentsSize?.() ?? c?.contents?.length ?? 0);
    }
    const seen = new Set();
    const hasXObject = (res, depth = 0) => {
      if (!res || depth > 4) return false;
      const xo = res.lookupMaybe ? res.lookupMaybe(PDFName.of('XObject'), PDFDict) : null;
      if (!xo) return false;
      for (const key of xo.keys()) {
        const k = key.asString();
        if (seen.has(k)) continue;
        seen.add(k);
        const obj = xo.lookup(key);
        const subtype = obj?.dict?.get?.(PDFName.of('Subtype'))?.asString?.();
        if (subtype === '/Image') return true;
        if (subtype === '/Form') {
          const sub = obj?.dict?.lookupMaybe?.(PDFName.of('Resources'), PDFDict);
          if (hasXObject(sub, depth + 1)) return true;
        }
      }
      return false;
    };
    const res = node.Resources?.();
    const hasFont = !!res?.lookupMaybe?.(PDFName.of('Font'), PDFDict);
    return hasXObject(res) || hasFont || len > 200;
  } catch {
    return true;   // 判定できないときは構造チェックでは弾かない (ink_ratio 側で見る)
  }
}

/**
 * 出荷フォルダの送り状PDFから該当ページを抜き出して配信トークンを返す。
 * 特定できない・失敗は throw (呼び出し側が握って「手動で印刷して」通知にする)。
 */
export async function extractReprintPdf({
  folderName, neSlipNo, recipientName, siteOrderNo = null,
  slipSeq = null, slipCount = null,
}) {
  const folders = (await getShippingFolders()).filter((f) => f.name === folderName);
  if (folders.length === 0) throw new Error(`Driveフォルダ ${folderName} が見つかりません`);
  const folderIds = folders.map((x) => x.folder_id);
  // 「送り状」で1回だけ一覧する — 並び替え済PDF・manifest.json・エラーtxt が全部これに乗る
  const listed = await driveCall(() => listDriveFilesAcross({ folders, nameContains: '送り状' }));
  const newest = (arr) => arr.slice().sort((a, b) =>
    String(b.modified_time || '').localeCompare(String(a.modified_time || '')))[0] || null;
  const download = (f, maxBytes) => driveCall(() => downloadDriveFileById({
    // downloadDriveFileById は {buffer, filename, ...} を返す (生Bufferではない — 実機バグ 2026-08-21)
    fileId: f.file_id, folderIds, maxBytes,
  }));

  // ── ① manifest による完全一致 (自動印刷を許可できる唯一の経路) ──
  // Driveは同名ファイルを許すため、**重複があったら自動印刷しない** (どれが正か確定できない。
  // 並び替えツール側も同名重複は競合として停止する設計。Codexレビュー指摘2)
  const manifestDup = listed.filter((f) => f.filename === MANIFEST_NAME);
  const sortedPdfDup = listed.filter((f) => f.filename === SORTED_PDF_NAME);
  const errorTxtDup = listed.filter((f) => f.filename === SORTER_ERROR_TXT);
  const manifestFile = manifestDup.length === 1 ? manifestDup[0] : null;
  const sortedPdfFile = sortedPdfDup.length === 1 ? sortedPdfDup[0] : null;
  let manifestReason = manifestDup.length > 1 || sortedPdfDup.length > 1 || errorTxtDup.length > 1
    ? '同名の出力ファイルが複数あります (Driveを整理してください)'
    : (folders.length > 1 ? `出荷フォルダ ${folderName} が複数あります` : 'manifestなし');
  if (manifestFile && sortedPdfFile && siteOrderNo && folders.length === 1
      && manifestDup.length === 1 && sortedPdfDup.length === 1 && errorTxtDup.length <= 1) {
    try {
      const pdf = await download(sortedPdfFile, 60 * 1024 * 1024);
      const mf = await download(manifestFile, 8 * 1024 * 1024);
      const manifest = JSON.parse(mf.buffer.toString('utf8'));
      const invoices = await driveCall(() => listDriveFilesAcross({ folders, nameContains: '納品書' }));
      // エラーtxtは成功時にも「解消済み」で上書きされるので、中身まで見て失敗か解消かを判別する
      let errorTxt = null;
      if (errorTxtDup.length === 1) {
        errorTxt = { modifiedMs: ms(errorTxtDup[0].modified_time), isFailure: null };
        try {
          const txt = await download(errorTxtDup[0], 1024 * 1024);
          errorTxt.isFailure = isSorterFailureText(txt.buffer.toString('utf8'));
        } catch (e) {
          console.warn(`[packing-reprint] ${SORTER_ERROR_TXT} の読込失敗: ${e.message}`);
        }
      }
      const v = verifyManifest(manifest, {
        folderName, siteOrderNo,
        pdfSha256: sha256(pdf.buffer),
        pdfPageCount: (await PDFDocument.load(pdf.buffer)).getPageCount(),
        manifestModifiedMs: ms(manifestFile.modified_time),
        errorTxt,
        invoiceFiles: invoices.map((f) => ({ name: f.filename, modified_time: f.modified_time })),
      });
      if (v.ok) {
        const r = await cutPage(pdf.buffer, v.page, sortedPdfFile.filename);
        return { ...r, by: 'manifest', printable: true, inkRatio: v.inkRatio };
      }
      // 白紙は「探し方が悪い」のではなく**元が白紙**。位置推定に落ちても同じ紙しか出ないので止める
      if (v.reason === 'blank') {
        throw new LabelUnusableError('blank',
          `送り状のページが白紙です (ink_ratio=${v.inkRatio}・並び替え元のPDFを確認してください)`);
      }
      manifestReason = v.reason;
    } catch (e) {
      if (e instanceof LabelUnusableError) throw e;
      manifestReason = `manifest読込エラー: ${e.message}`;
    }
  }
  console.warn(`[packing-reprint] ${folderName}/${neSlipNo}: manifest経路を使いません (${manifestReason})`);

  // ── ② 従来経路 (テキスト照合→位置対応)。人が目で見て印刷する前提なので残すが、
  //       printable=false = 自動印刷はしない ──
  const files = listed
    .filter((f) => /\.pdf$/i.test(f.filename))
    .sort((a, b) => String(b.modified_time || '').localeCompare(String(a.modified_time || '')))
    .slice(0, 3);   // 新しい順に最大3ファイル
  if (files.length === 0) throw new Error('送り状PDFがフォルダにありません');
  const fileSets = [];
  for (const f of files) {
    try {
      // 送り状PDFは繁忙日に大きくなるため上限も CSV既定20MB から引き上げる
      const dl = await download(f, 60 * 1024 * 1024);
      fileSets.push({ filename: f.filename, buf: dl.buffer, pages: await extractPageTexts(dl.buffer) });
    } catch (e) {
      console.warn(`[packing-reprint] ${f.filename} の読込失敗: ${e.message}`);
    }
  }
  if (fileSets.length === 0) throw new Error('送り状PDFを読み込めませんでした');
  const hit = decideLabelPage(fileSets, { neSlipNo, recipientName, slipSeq, slipCount });
  if (!hit) throw new Error('該当ページを一意に特定できません (テキスト0件/複数一致・位置対応も条件不成立)');
  const r = await cutPage(fileSets[hit.file].buf, hit.page, fileSets[hit.file].filename);
  return { ...r, by: hit.by, printable: false, inkRatio: null };
}

/** 1ページだけの新しいPDFを作って保存し、配信トークンを返す。明白な空ページは拒否する。 */
async function cutPage(srcBuffer, pageIndex, filename) {
  const src = await PDFDocument.load(srcBuffer);
  const out = await PDFDocument.create();
  const [page] = await out.copyPages(src, [pageIndex]);
  out.addPage(page);
  if (!pageHasContent(out.getPage(0))) {
    throw new LabelUnusableError('blank', `抜き出したページに中身がありません (${filename} の ${pageIndex + 1}ページ目)`);
  }
  const bytes = await out.save();
  const token = crypto.randomBytes(16).toString('base64url');
  fs.mkdirSync(REPRINTS_DIR, { recursive: true });
  fs.writeFileSync(path.join(REPRINTS_DIR, `${token}.pdf`), bytes);
  return { token, file: filename };
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
