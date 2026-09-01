/**
 * 入荷受付チェック — ロジザード「入荷状況照会 [FA04_01]」CSV (CA04001_*.csv) のパース
 *
 * 入力: Shift-JIS (CP932) / ヘッダあり / 全項目引用符あり / 58列 (2026-08-31 実測)。
 * 明細キー = 入荷管理番号 (AR番号) + 入荷管理行番号 + 入荷管理詳細行番号。
 * 商品IDはキーにしない (同じ商品が複数行・複数伝票に載り得る)。
 *
 * 方針 (要件定義 v1.1 §7):
 *  - fail-closed: 必須列が無い / 列数不一致 / 数値でない予定数 / AR空 は **ファイル全体を拒否**
 *  - 0件は正常 (入荷が無い日は本当に0件)。ヘッダだけの CSV は rows=[] で ok
 *  - 行数の前回比ガードは置かない
 */
import iconv from 'iconv-lite';

export const REQUIRED_COLS = [
  '入荷管理番号', '入荷管理行番号', 'ステータス', '入荷予定日', '入荷受付日',
  '商品ID', '商品名', '予定数', '受付数', 'バーコード',
];
const OPTIONAL_COLS = ['入荷管理詳細行番号', '作成日時', '更新日時', '取引先ID', '取引先名', '業務区分名', '品質区分名'];

/** Shift-JIS/UTF-8 自動判別 (厳密UTF-8で読めた時だけUTF-8。Windows由来なので CP932 明示) */
export function decodeCsvBuffer(buf) {
  try {
    return new TextDecoder('utf-8', { fatal: true }).decode(buf);
  } catch {
    // iconv-lite は不正バイトを U+FFFD に置換して続行する。正しい CP932 ファイルに U+FFFD は現れないので、
    // 含まれていたら「壊れた Shift-JIS」としてファイル全体を拒否する (Codex R3 Medium)
    const text = iconv.decode(buf, 'cp932');
    if (text.includes('�')) throw new Error('Shift-JIS として解釈できないバイトが含まれます (ファイル破損・別エンコーディングの可能性)');
    return text;
  }
}

/** 厳格な CSV パーサ (引用符はフィールド先頭のみ・閉じ引用符の後は区切り/改行/EOF のみ) */
export function parseCsv(text) {
  if (text.charCodeAt(0) === 0xFEFF) text = text.slice(1);
  const rows = [];
  let row = [], field = '', q = false, closed = false;
  const pushField = () => { row.push(field); field = ''; closed = false; };
  const pushRow = () => { pushField(); if (row.some(v => v !== '')) rows.push(row); row = []; };
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (q) {
      if (c === '"') {
        if (text[i + 1] === '"') { field += '"'; i++; } else { q = false; closed = true; }
      } else field += c;
    } else if (c === '"') {
      if (field !== '' || closed) throw new Error(`CSVの引用符の位置が不正です (行${rows.length + 2}付近)`);
      q = true;
    } else if (c === ',') pushField();
    else if (c === '\n' || c === '\r') {
      if (c === '\r' && text[i + 1] === '\n') i++;
      pushRow();
    } else {
      if (closed) throw new Error(`CSVの閉じ引用符の後に文字があります (行${rows.length + 2}付近)`);
      field += c;
    }
  }
  if (q) throw new Error('CSVの引用符が閉じていません (ファイル破損の可能性)');
  pushRow();
  return rows;
}

const trimS = v => String(v == null ? '' : v).trim();

function intField(v, label, lineNo, { allowEmpty = false } = {}) {
  const s = trimS(v).replace(/,/g, '');
  if (s === '') {
    if (allowEmpty) return null;
    throw new Error(`${lineNo}行目: 「${label}」が空です`);
  }
  if (!/^-?\d+$/.test(s)) throw new Error(`${lineNo}行目: 「${label}」が整数ではありません (${s})`);
  const n = Number(s);
  if (!Number.isSafeInteger(n)) throw new Error(`${lineNo}行目: 「${label}」が大きすぎます (${s})`);
  if (n < 0) throw new Error(`${lineNo}行目: 「${label}」が負数です (${s})`);
  return n;
}

/** 実在する暦日か (2026-13-99 のような値を弾く。Date.UTC は繰り上げるので往復で照合) */
function isCalendarDate(y, m, d) {
  const dt = new Date(Date.UTC(y, m - 1, d));
  return dt.getUTCFullYear() === y && dt.getUTCMonth() === m - 1 && dt.getUTCDate() === d;
}

/** 20260831 → 2026-08-31 (空は null。8桁でない・実在しない日付はエラー) */
export function ymd8ToIso(v, label, lineNo) {
  const s = trimS(v);
  if (s === '') return null;
  const m = /^(\d{4})(\d{2})(\d{2})$/.exec(s);
  if (!m || !isCalendarDate(Number(m[1]), Number(m[2]), Number(m[3]))) {
    throw new Error(`${lineNo}行目: 「${label}」が日付 (YYYYMMDD) ではありません (${s})`);
  }
  return `${m[1]}-${m[2]}-${m[3]}`;
}

/** 20260831182105 → 2026-08-31T18:21:05+09:00 (JST 表記のまま保持。空は null) */
export function ts14ToIso(v) {
  const s = trimS(v);
  const m = /^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})$/.exec(s);
  if (!m) return null;
  return `${m[1]}-${m[2]}-${m[3]}T${m[4]}:${m[5]}:${m[6]}+09:00`;
}

/**
 * CSV バッファ → { header, rows: [{ar_no, line_no, detail_no, line_key, status, planned_date, received_date,
 *   product_id, code_key, product_name, barcode, planned_qty, received_qty, created_at, updated_at, seq}] }
 * 例外 = ファイル全体を拒否する理由 (メッセージは画面にそのまま出す)
 */
export function parseInboundCsv(buffer) {
  if (!buffer || buffer.length === 0) throw new Error('CSVが空です (0バイト)');
  const text = decodeCsvBuffer(buffer);
  const table = parseCsv(text);
  if (table.length === 0) throw new Error('CSVにヘッダ行がありません');
  const header = table[0].map(trimS);
  const idx = {};
  header.forEach((h, i) => {
    // 空ヘッダ・重複ヘッダは「どの列を読んだか」が定まらないのでファイル全体を拒否 (Codex R3 Medium)
    if (h === '') throw new Error(`CSVのヘッダ ${i + 1} 列目が空です`);
    if (h in idx) throw new Error(`CSVのヘッダに重複があります: 「${h}」`);
    idx[h] = i;
  });
  const missing = REQUIRED_COLS.filter(c => !(c in idx));
  if (missing.length) {
    throw new Error(`CSVのヘッダに必須列がありません: ${missing.join(', ')} (入荷状況照会 [FA04_01] のCSVですか?)`);
  }
  const col = (r, name) => (name in idx ? r[idx[name]] : '');
  const rows = [];
  const seen = new Set();
  for (let i = 1; i < table.length; i++) {
    const r = table[i];
    const lineNo = i + 1;
    if (r.length !== header.length) {
      throw new Error(`${lineNo}行目: 列数がヘッダと一致しません (${r.length} 列 / ヘッダ ${header.length} 列)`);
    }
    const arNo = trimS(col(r, '入荷管理番号'));
    if (!arNo) throw new Error(`${lineNo}行目: 「入荷管理番号」が空です`);
    const lineNoV = intField(col(r, '入荷管理行番号'), '入荷管理行番号', lineNo);
    const detailNo = intField(col(r, '入荷管理詳細行番号'), '入荷管理詳細行番号', lineNo, { allowEmpty: true }) ?? 1;
    const productId = trimS(col(r, '商品ID'));
    if (!productId) throw new Error(`${lineNo}行目: 「商品ID」が空です`);
    const lineKey = `${arNo}|${lineNoV}|${detailNo}`;
    if (seen.has(lineKey)) throw new Error(`${lineNo}行目: 明細キーが重複しています (${lineKey})`);
    seen.add(lineKey);
    rows.push({
      ar_no: arNo,
      line_no: lineNoV,
      detail_no: detailNo,
      line_key: lineKey,
      status: trimS(col(r, 'ステータス')),
      planned_date: ymd8ToIso(col(r, '入荷予定日'), '入荷予定日', lineNo),
      received_date: ymd8ToIso(col(r, '入荷受付日'), '入荷受付日', lineNo),
      product_id: productId,
      code_key: productId.toLowerCase(),
      product_name: trimS(col(r, '商品名')),
      barcode: trimS(col(r, 'バーコード')),
      planned_qty: intField(col(r, '予定数'), '予定数', lineNo),
      received_qty: intField(col(r, '受付数'), '受付数', lineNo, { allowEmpty: true }),
      created_at: ts14ToIso(col(r, '作成日時')),
      updated_at: ts14ToIso(col(r, '更新日時')),
      seq: rows.length + 1,
    });
  }
  return { header, rows, optionalPresent: OPTIONAL_COLS.filter(c => c in idx) };
}
