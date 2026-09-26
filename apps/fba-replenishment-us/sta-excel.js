/**
 * 米国の STA (Send to Amazon) に取り込む納品 Excel を作る。
 *
 * 土台 = 中原さんが 2026-09-26 に米国セラーセントラルから落としたテンプレートそのもの (templates/sta-us-template.xlsx)。
 *   日本の出力 (apps/fba-replenishment/router.js /api/export-manifest) とは列が違う:
 *   - 米国は 10 列。行ごとの Prep owner / Labeling owner の列が無く、Manufacturing lot code がある
 *   - 箱の単位は in / lb (日本は cm / kg)
 *   - 見出しの「Manufacturing lot code 」「Units per box 」は末尾に空白がある
 *   → 列を自分で並べず、テンプレートの「Create workflow – template」シートの 9 行目から SKU・数量 (・期限) を書くだけにする。
 *     見出しがテンプレートと違っていたら (Amazon がテンプレートを変えた・ファイルが壊れた) 作らずに止める。
 * Default prep owner / labeling owner は Seller (米国は 2026-01 に Amazon の prep・商品ラベルのサービスが終了)。
 */
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const TEMPLATE_FILE = path.join(__dirname, 'templates', 'sta-us-template.xlsx');
export const SHEET = 'Create workflow – template';
export const HEADER_ROW = 8;
export const FIRST_DATA_ROW = 9;
// テンプレートの 8 行目 (末尾の空白も含めてそのまま)
export const EXPECTED_HEADERS = ['Merchant SKU', 'Quantity', 'Expiration date (MM/DD/YYYY)', 'Manufacturing lot code ', 'Units per box ', 'Number of boxes', 'Box length (in)', 'Box width (in)', 'Box height (in)', 'Box weight (lb)'];
export const MAX_QTY = 100000;

/** YYYY-MM-DD / YYYYMMDD / YYYY/MM/DD → MM/DD/YYYY。空は null。それ以外の形は例外 (推測で書かない) */
export function toUsDate(v) {
  if (v == null || String(v).trim() === '') return null;
  const raw = String(v).trim().replace(/[/-]/g, '');
  if (!/^\d{8}$/.test(raw)) throw new Error(`期限の形が分からない: ${v}`);
  const y = raw.slice(0, 4), m = raw.slice(4, 6), d = raw.slice(6, 8);
  const dt = new Date(`${y}-${m}-${d}T00:00:00Z`);
  if (Number.isNaN(dt.getTime()) || dt.toISOString().slice(0, 10) !== `${y}-${m}-${d}`) throw new Error(`期限の日付がおかしい: ${v}`);
  return `${m}/${d}/${y}`;
}

/**
 * 画面から来た行を確かめる。SKU は米国 RESTOCK にある SKU だけ (表記は RESTOCK のものに直す)。
 * @param {{sku: string, qty: number, expiry?: string}[]} items
 * @param {Map<string, string>} knownSkus  小文字の SKU → RESTOCK の表記
 * @returns {{ rows: {sku, qty, expiry}[], errors: string[] }}
 */
export function validateStaItems(items, knownSkus) {
  const errors = [];
  const rows = [];
  if (!Array.isArray(items) || items.length === 0) return { rows, errors: ['送る SKU がありません'] };
  if (items.length > 200) return { rows, errors: ['一度に 200 行まで'] };
  const seen = new Set();
  for (const [i, it] of items.entries()) {
    const key = String(it && it.sku != null ? it.sku : '').trim().toLowerCase();
    const label = `${i + 1} 行目 (${String(it && it.sku != null ? it.sku : '').slice(0, 60)})`;
    if (!key) { errors.push(`${label}: SKU が空`); continue; }
    if (!knownSkus.has(key)) { errors.push(`${label}: 米国の RESTOCK に無い SKU`); continue; }
    if (seen.has(key)) { errors.push(`${label}: 同じ SKU が 2 行 (1 行にまとめてください)`); continue; }
    seen.add(key);
    // テンプレートの Data definitions C4: 「80 文字以内・英字・数字・特殊文字 (英語以外の文字は使用できません)」
    //   = RESTOCK の表記で ASCII の印字可能文字だけ・80 文字まで。違えば STA の取り込みで弾かれるので作らない (Codex #1473 R1 Medium 2)
    const official = knownSkus.get(key);
    if (official.length > 80 || !/^[\x20-\x7e]+$/.test(official)) { errors.push(`${label}: STA のテンプレートは SKU に 80 文字以内の英数字・記号しか使えない (${official})`); continue; }
    // 数量は整数の数字か、10 進の数字だけの文字列。true・[12]・"0x10" などを数に直して通さない (Codex #1473 R1 Low)
    const q = it.qty;
    const qty = typeof q === 'number' ? q : (typeof q === 'string' && /^\d+$/.test(q.trim()) ? Number(q.trim()) : NaN);
    if (!Number.isSafeInteger(qty) || qty < 1 || qty > MAX_QTY) { errors.push(`${label}: 数量は 1〜${MAX_QTY} の整数 (${typeof q === 'string' ? q : JSON.stringify(q)})`); continue; }
    let expiry = null;
    try { expiry = toUsDate(it.expiry); } catch (e) { errors.push(`${label}: ${e.message}`); continue; }
    rows.push({ sku: official, qty, expiry });
  }
  return { rows, errors };
}

/**
 * テンプレートに行を書いた xlsx の Buffer を返す。見出しがテンプレートと違えば例外 (作らない)。
 * @param {{sku: string, qty: number, expiry: string|null}[]} rows  validateStaItems を通したもの
 */
export async function buildStaUsWorkbook(rows, { templateFile = TEMPLATE_FILE } = {}) {
  const ExcelJS = (await import('exceljs')).default;
  const wb = new ExcelJS.Workbook();
  await wb.xlsx.readFile(templateFile);
  const ws = wb.getWorksheet(SHEET);
  if (!ws) throw new Error(`テンプレートに「${SHEET}」シートが無い`);
  const got = EXPECTED_HEADERS.map((_, i) => ws.getCell(HEADER_ROW, i + 1).value);
  const diff = EXPECTED_HEADERS.map((h, i) => (got[i] === h ? null : `${i + 1} 列目: 期待「${h}」/ 実物「${got[i]}」`)).filter(Boolean);
  if (diff.length) throw new Error(`テンプレートの見出しが想定と違う (Amazon がテンプレートを変えた?): ${diff.join(' / ')}`);
  // 既に書かれている行があれば作らない (テンプレートは 9 行目以降が空のはず)
  for (let r = FIRST_DATA_ROW; r <= ws.rowCount; r++) {
    if (ws.getRow(r).values.some((v) => v != null && v !== '')) throw new Error(`テンプレートの ${r} 行目が空でない`);
  }
  ws.getCell('B3').value = 'Seller';
  ws.getCell('B4').value = 'Seller';
  rows.forEach((row, i) => {
    const r = FIRST_DATA_ROW + i;
    ws.getCell(r, 1).value = row.sku;
    ws.getCell(r, 2).value = row.qty;
    if (row.expiry) ws.getCell(r, 3).value = row.expiry;
  });
  return Buffer.from(await wb.xlsx.writeBuffer());
}
