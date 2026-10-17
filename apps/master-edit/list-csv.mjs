/**
 * list-csv.mjs — 一覧 (絞った全件) の CSV (10/5 中原さん「絞った一覧を CSV で出す」)
 *
 * 形: UTF-8 の BOM 付き (Excel で文字化けしない)・行の終わり CRLF・全部の欄を "…" で囲む (中の " は "")。
 * 式の注入の対策: 文字の欄で、先頭の空白・制御文字を除いた最初の文字が = + - @ タブ CR なら先頭に ' を付ける (Excel・スプレッドシートが式として動かさない)。
 *   数の欄 (売価・原価・在庫など) は数のまま (こちらで作る数 = 式にならない)
 * 列 = 画面の一覧の列 (コード・区分・名前・状態・登録日・売価・原価・税率・売上分類・在庫・FBA (JP)・売れた数・注文残) + 画面の詳細にあるもの
 *   (構成品の数・登録の状態・代表の仕入先・JAN・送料コード・推奨月数・対応が必要)。利益・利益率 (10/6・1 個あたりの参考 = 一覧と同じ lib/profit-estimate.js。計算できない = 空)。参考の値の見出しに時点 (例「在庫 (ロジザード 10/5 18:01 時点)」)。
 *   読めない参考の値 = 空 + 見出しに「読めない」。注文残 = 発注アプリの利用権がある人だけ (無い人には列ごと出さない)
 * 商品コード (10/16 中原さん): NE から来た商品 = NE の書き方 (NE の元のコードの記録が ok のとき)・決まらない = 今のコード + 最後の列「商品コードの注意」に理由 /
 *   新商品の入口の商品 = 打った書き方 (ne-spelling.mjs)。新しい列は最後に足す (前の列の並びは変えない)
 */
import { fmtJst, fmtDay } from './ui-format.mjs';
import { PLATFORM_FEE_RATES, ratePct1 } from '../../lib/profit-estimate.js';

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
    ['商品コード', (r) => r.export_code ?? r.code],   // NE から来た商品 = NE の書き方 (read.mjs の mode 'all' が入れる)
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
    [`利益 (1 個あたり・参考・手数料 ${Math.round(PLATFORM_FEE_RATES.standard * 1000) / 10}%・円)`, (r) => (r.profit == null ? null : r.profit)],
    ['利益率 (%)', (r) => ratePct1(r.profit_rate)],   // 画面と同じ丸め (lib の ratePct1。Codex #1632 R3 L: 独自の Math.round だと 0.1 ずれた)
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
    // NE の書き方が決まらない商品 = 今のコードのまま出して理由を添える (確かめずに NE・ロジザードに上げないように。10/16 中原さん)
    ['商品コードの注意', (r) => r.code_note || ''],
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

/** Amazon SKU の対応の CSV のファイル名 (日本の日時)。例 amazon-sku_20261016-1830.csv */
export function amazonCsvFileName(nowMs = Date.now()) {
  return csvFileName(nowMs).replace(/^master-list_/, 'amazon-sku_');
}
/** ISO (UTC) → 日本時間の 'YYYY/MM/DD HH:MM' (Excel で日時として読める・年も付ける)。読めない = 元の文字 */
function jstFull(iso) {
  const t = Date.parse(String(iso ?? ''));
  if (Number.isNaN(t)) return iso == null ? '' : String(iso);
  const d = new Date(t + 9 * 3600e3).toISOString();
  return `${d.slice(0, 10).replace(/-/g, '/')} ${d.slice(11, 16)}`;
}
/** 日付 'YYYY-MM-DD' → 'YYYY/MM/DD' (Excel で日付として読める)。無い = 空 */
const ymdSlash = (v) => (v ? String(v).slice(0, 10).replace(/-/g, '/') : '');
/** 行を CSV に (BOM + 見出し + 行・CRLF・式の注入の対策 = csvCell) */
function csvOf(cols, rows) {
  const lines = [cols.map(([h]) => csvCell(h)).join(',')];
  for (const r of rows) lines.push(cols.map(([, f]) => csvCell(f(r))).join(','));
  return `﻿${lines.join('\r\n')}\r\n`;
}

/** 未登録 (売れたのに対応が無い SKU) の CSV のファイル名。例 amazon-unmapped_20261017-0930.csv */
export const unmappedCsvFileName = (nowMs = Date.now()) => csvFileName(nowMs).replace(/^master-list_/, 'amazon-unmapped_');
/**
 * 未登録の CSV (10/17 中原さん「未登録一覧の CSV ダウンロード」)。data = amazonUnmapped の結果 (画面と同じ期間)。
 *   行 = 画面の一覧 (FBA) + 自社発送で seller SKU が NE コードでない SKU (withFbm = 画面でその一覧を開いているとき) + 区分が分からない SKU (画面の下の一言の SKU)。
 *   「一覧」の列で分ける。期間・今日も列に (Excel で開いた後も、いつのどの範囲か分かる)
 */
export function buildUnmappedCsv(data, { withFbm = false, mapStates = {}, channels = {}, periodLabel = '' } = {}) {
  const FROM = { report: '出品一覧', fees: '手数料', conflict: '食い違い', stale: '古い', none: '無い' };
  const list = [
    ...data.rows.map((r) => ({ ...r, group: 'FBA' })),
    ...(withFbm ? (data.fbm || []).map((r) => ({ ...r, group: '自社発送 (seller SKU が NE コードでない)' })) : []),
    ...(data.unknown || []).map((r) => ({ ...r, group: '区分が分からない' })),
  ];
  const cols = [
    ['一覧', (r) => r.group],
    ['seller SKU', (r) => r.code],
    ['Amazon の商品名', (r) => r.title || ''],
    ['区分', (r) => (r.channel ? channels[r.channel] || r.channel : '')],
    ['区分の出どころ', (r) => FROM[r.channel_from] || ''],
    ['個数', (r) => Number(r.units)],
    ['注文', (r) => Number(r.orders)],
    ['最後に売れた日', (r) => ymdSlash(r.last_date)],
    ['いまの対応', (r) => (r.map_state ? mapStates[r.map_state] || r.map_state : r.listing_id ? '出品はある (構成なし)' : '出品なし')],
    ['期間', () => `${periodLabel}${data.from ? ` (${ymdSlash(data.from)} 〜 ${ymdSlash(data.today)})` : ''}`],
    ['未判定の日 (売上の日次が未公開)', () => (data.undecided || []).length],
  ];
  return csvOf(cols, list);
}

/** まだつないでいない SKU の CSV のファイル名。例 amazon-unlinked_20261017-0930.csv */
export const unlinkedCsvFileName = (nowMs = Date.now()) => csvFileName(nowMs).replace(/^master-list_/, 'amazon-unlinked_');
/** まだつないでいない SKU の CSV (10/17)。data = listUnlinked(…, { all: true }) の結果 (画面と同じ絞り込み・ページに分けない) */
export function buildUnlinkedCsv(data, { statusWords = {}, hideReasons = {} } = {}) {
  const mapWords = (r) => (r.map_state === 'deleted' ? '対応を削除済み (墓標)' : r.listing_id ? '出品はある (構成なし)' : '対応なし');
  const cols = [
    ['seller SKU', (r) => r.seller_sku],
    ['Amazon の商品名', (r) => r.item_name || ''],
    ['ASIN', (r) => r.asin || ''],
    ['出品日', (r) => (r.open_date ? ymdSlash(r.open_date) : r.open_date_raw || '')],
    ['状態', (r) => statusWords[r.status] || r.status || ''],
    ['いまの対応', mapWords],
    [`FBA 在庫 (販売可能${data.fba && data.fba.ok ? `・${ymdSlash(data.fba.date)}` : '・読めない'})`, (r) => (!data.fba || !data.fba.ok || !r.fba_row ? null : r.fba_available)],
    [`最後に売れた日${data.sold && data.sold.ok ? '' : ' (読めない)'}`, (r) => ymdSlash(r.last_sold)],
    ['価格 (円)', (r) => (r.price == null ? null : Number(r.price))],
    ['隠した理由', (r) => (r.hidden_reason ? hideReasons[r.hidden_reason] || r.hidden_reason : '')],
    ['隠した理由の文', (r) => r.hidden_note || ''],
    ['隠した人', (r) => r.hidden_by || ''],
    ['隠した日時 (日本時間)', (r) => (r.hidden_at ? jstFull(r.hidden_at) : '')],
    ['出品一覧の取得 (日本時間)', () => (data.load ? jstFull(data.load.snapshot_at) : '')],
  ];
  return csvOf(cols, data.rows);
}

/**
 * Amazon SKU の対応 (絞った全件) の CSV (BOM + 見出し + 行・CRLF・形は上の一覧の CSV と同じ)。data = listAmazonMapsAll の結果。
 * 列 = 画面の一覧の列 (seller SKU・名前・構成・区分・ASIN・状態・変えた) + 構成の行の数・移行で入ったか・変えた人
 */
export function buildAmazonCsv(data, { mapStates = {}, channels = {} } = {}) {
  const cols = [
    ['seller SKU', (r) => r.seller_sku],
    ['名前', (r) => r.name],
    ['構成 (NE の商品 × 数)', (r) => r.comps || ''],
    ['構成の行の数', (r) => (r.comps ? String(r.comps).split(', ').filter(Boolean).length : 0)],
    ['区分', (r) => (r.channel ? channels[r.channel] || r.channel : '')],
    ['ASIN', (r) => r.asin || ''],
    ['状態', (r) => mapStates[r.state] || r.state],
    ['移行で入った', (r) => (r.origin === 'legacy' ? '移行' : '')],
    ['変えた日時 (日本時間)', (r) => jstFull(r.changed_at)],
    ['変えた人', (r) => r.changed_by || ''],
    // ASIN・区分が出品 (夜間ロード)・手数料の写しに無く、Amazon の出品一覧 (前の夜の出品レポート) で埋めた行 = 「出品一覧」(10/16 中原さん)。前の列の並びは変えない (後ろに足す)
    ['ASIN の出どころ', (r) => (r.asin_from === 'report' ? '出品一覧' : r.asin_from === 'listing' ? '出品' : '')],
    ['区分の出どころ', (r) => (r.channel_from === 'report' ? '出品一覧' : r.channel_from === 'fees' ? '手数料' : '')],
    // 構成の商品コード = NE から来た商品なら NE の書き方 (amazon-read.mjs の setExportComps)。決まらない構成品 = 今のコード + ここに理由 (10/16 中原さん。後ろに足す)
    ['構成の商品コードの注意', (r) => r.comp_notes || ''],
  ];
  const lines = [cols.map(([h]) => csvCell(h)).join(',')];
  for (const r of data.rows) lines.push(cols.map(([, f]) => csvCell(f(r))).join(','));
  return `﻿${lines.join('\r\n')}\r\n`;
}
