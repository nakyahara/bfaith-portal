/**
 * list-csv.mjs — 一覧 (絞った全件) の CSV (10/5 中原さん「絞った一覧を CSV で出す」)
 *
 * 形: UTF-8 の BOM 付き (Excel で文字化けしない)・行の終わり CRLF・全部の欄を "…" で囲む (中の " は "")。
 * 式の注入の対策: 文字の欄で、先頭の空白・制御文字を除いた最初の文字が = + - @ タブ CR なら先頭に ' を付ける (Excel・スプレッドシートが式として動かさない)。
 *   数の欄 (売価・原価・在庫など) は数のまま (こちらで作る数 = 式にならない)
 * 列 = 画面の一覧の列 (コード・区分・名前・状態・登録日・売価・原価・税率・売上分類・在庫・FBA (JP)・売れた数・注文残) + 画面の詳細にあるもの
 *   (構成品の数・登録の状態・代表の仕入先・JAN・送料コード・推奨月数・対応が必要)。参考の値の見出しに時点 (例「在庫 (ロジザード 10/5 18:01 時点)」)。
 *   読めない参考の値 = 空 + 見出しに「読めない」。注文残 = 発注アプリの利用権がある人だけ (無い人には列ごと出さない)
 */
import { fmtJst, fmtDay } from './ui-format.mjs';

export const KIND_LABELS = Object.freeze({ single: '単品', set: 'セット', exception: '例外' });
const SALES_LABELS = Object.freeze({ 1: '1 自社', 2: '2 取引先限定', 3: '3 仕入', 4: '4 輸出' });

/** 1 つの欄。v = 文字 / 数 / null。文字は式の注入を止める */
export function csvCell(v) {
  if (v == null) return '""';
  let s = typeof v === 'number' ? (Number.isFinite(v) ? String(v) : '') : String(v);
  if (typeof v !== 'number') {
    const first = s.replace(/^[\s\u0000-\u001f\u007f-\u009f\u200b\ufeff]+/, '').charAt(0);
    if (/^[=+\-@\t\r]$/.test(first) || /^[\t\r]/.test(s)) s = `'${s}`;
  }
  return `"${s.replace(/"/g, '""')}"`;
}

/** ファイルの名前 (日本の日時)。例 master-list_20261005-1830.csv */
export function csvFileName(nowMs = Date.now()) {
  const d = new Date(nowMs + 9 * 3600e3).toISOString();
  return `master-list_${d.slice(0, 10).replace(/-/g, '')}-${d.slice(11, 13)}${d.slice(14, 16)}.csv`;
}

const hm = (iso, nowMs) => fmtJst(iso, { nowMs }).replace(/ \(.\) /, ' ');
const dd = (ymd) => fmtDay(ymd).replace(/ \(.\)$/, '');

/**
 * CSV の中身 (BOM + 見出し + 行・CRLF)。data = listSkus(…, { mode: 'all' }) の結果。extras = 一覧と同じ参考の値 (stock / fba / sales / backorders)。
 * poOk = 注文残を出してよい人
 */
export function buildListCsv(data, extras, { poOk = false, nowMs = Date.now(), regStates = {} } = {}) {
  const st = extras && extras.stock; const fb = extras && extras.fba; const sa = extras && extras.sales; const bo = extras && extras.backorders;
  const stH = !st || !st.ok ? '在庫 (ロジザード 読めない)' : `在庫 (ロジザード ${hm(st.asOf, nowMs)} 時点${st.stale ? '・古い' : ''}・セットは作れる数)`;
  const fbH = !fb || !fb.ok ? 'FBA (JP) 販売可能 (読めない)' : `FBA (JP) 販売可能 (${hm(fb.asOf, nowMs)} 時点${fb.stale ? '・古い' : ''})`;
  const saWhen = !sa || !sa.ok ? '読めない' : `${dd(sa.asOf)} まで${sa.stale ? '・古い' : ''}`;
  const cols = [
    ['商品コード', (r) => r.code],
    ['区分', (r) => KIND_LABELS[r.kind] || r.kind],
    ['構成品の数', (r) => (r.kind === 'set' ? r.comp_count : null)],
    ['名前', (r) => r.name],
    ['状態', (r) => (r.state === 'discontinued' ? '中止' : '取扱中')],
    ['登録の状態', (r) => (r.reg_state && r.reg_state !== 'none' ? regStates[r.reg_state] || r.reg_state : '')],
    ['登録日', (r) => (r.registered_on ? r.registered_on.replace(/-/g, '/') : '')],
    ['売価', (r) => r.standard_price],
    ['原価', (r) => r.cost],
    ['原価は構成品から計算', (r) => (r.cost != null && r.cost_derived ? '計算' : '')],
    ['税率 (%)', (r) => (r.tax_rate == null ? null : Math.round(r.tax_rate * 100))],
    ['売上分類', (r) => (r.sales_class == null ? '' : SALES_LABELS[r.sales_class] || String(r.sales_class))],
    ['代表の仕入先コード', (r) => r.primary_supplier || ''],
    ['代表の仕入先', (r) => r.primary_supplier_name || ''],
    ['JAN', (r) => r.jan || ''],
    ['送料コード', (r) => r.shipping_code || ''],
    ['推奨月数', (r) => r.reorder_months],
    [stH, (r) => (r.kind === 'set' ? r.buildable : r.stock)],
    [fbH, (r) => r.fba],
    [`売れた 7 日 (${saWhen})`, (r) => salesValue(r, 'd7')],
    [`売れた 30 日 (${saWhen})`, (r) => salesValue(r, 'd30')],
    ...(poOk ? [[!bo || !bo.ok ? '注文残 (発注アプリ 読めない)' : '注文残 (発注アプリ)', (r) => (r.backorder == null ? null : r.backorder)]] : []),
    ['対応が必要', (r) => (r.flags || []).join('・')],
  ];
  const lines = [cols.map(([h]) => csvCell(h)).join(',')];
  for (const r of data.rows) lines.push(cols.map(([, f]) => csvCell(f(r))).join(','));
  return `\ufeff${lines.join('\r\n')}\r\n`;
}
/** 売れた数: 読めない・商品管理リストに無い・セット (値に関係なく = 構成品に入る。構成の欠けたセットだけ上流がセットのコードに数えることがある) = 空 (画面の「—」と同じ) */
function salesValue(r, k) {
  const s = r.sales;
  if (!s || s.missing || r.kind === 'set') return null;
  return s[k];
}
