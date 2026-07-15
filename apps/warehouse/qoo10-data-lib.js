/**
 * qoo10-data-lib.js — Qoo10 Analytics (seller.qoo10.jp) xlsx の取込ライブラリ (mall-csv-fetcher P1-Q R1)
 *
 * 対象 (実ファイルで仕様確定 2026-07-14):
 *   1. トラフィック/CV 店舗版 Qoo10_CVR_<from>_<to>.xlsx — シート'data'、日付行×
 *      「上位のチャネル(PV) : <ラベル>」52列 + PV合計/訪問者数/商品カート数/注文完了/コンバージョン率(%)
 *      → fact_qoo10_traffic_channel_daily (縦持ち) + fact_qoo10_cvr_daily
 *   2. 日付・商品別版 Qoo10_CVR_DateGoods_*.xlsx — 日付×商品 (商品番号/販売者商品コード/商品名/
 *      ブランド名) ×同系チャネル + PV合計。KPI列は無い (PVのみ)
 *      → fact_qoo10_item_traffic_daily (sparse縦持ち: pv>0 + '(合計)'行) + m_qoo10_items
 *
 * 設計判断 (Codex R1設計レビュー反映):
 *   - チャネルはヘッダ生ラベルのまま保存 ('_'でグループ分割しない — ラベル自体に'_'があり曖昧。
 *     グループ集計はVIEW/分類マスタ側で)
 *   - 商品キー=商品番号 (安定キー)。販売者商品コード (SKU) は属性 (raw+normSku)。
 *     商品名/ブランドはfact行に重複保存せず m_qoo10_items へ (mirror転送量削減)
 *   - CVR%はセルの表示形式 (numFmt に %) を見て単位判定。数式セルは拒否 (cached result不信)
 *   - PV合計とチャネル列合計の差は許容して記録 (未分類PVがあり得る)、恒常監視用
 *   - scope置換 (DELETE→INSERT) は実データの min〜max 日付。冪等=(sha256, scopeFrom)
 */
import crypto from 'node:crypto';
import ExcelJS from 'exceljs';
import { normSku } from '../../lib/sku-norm.js';
import { normalizeDate } from './rakuten-ads-rpp-lib.js';

const trimS = v => String(v == null ? '' : v).trim();

// ─── DDL ───
export function ensureQoo10DataTables(db) {
  const metaCols = `source_file TEXT, import_id INTEGER, imported_at TEXT NOT NULL, updated_at TEXT NOT NULL`;
  const dateCheck = `CHECK (date_jst GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]')`;

  db.exec(`CREATE TABLE IF NOT EXISTS fact_qoo10_traffic_channel_daily (
    date_jst TEXT NOT NULL ${dateCheck},
    channel  TEXT NOT NULL,
    pv INTEGER,
    ${metaCols},
    PRIMARY KEY (date_jst, channel)
  )`);

  db.exec(`CREATE TABLE IF NOT EXISTS fact_qoo10_cvr_daily (
    date_jst TEXT NOT NULL ${dateCheck},
    pv_total INTEGER, visitors INTEGER, cart_adds INTEGER, orders INTEGER, cvr_pct REAL,
    channel_pv_diff INTEGER,
    ${metaCols},
    PRIMARY KEY (date_jst)
  )`);

  // sparse縦持ち: pv>0 のチャネル行 + '(合計)' 行 (商品が現れた日は必ず合計行がある)
  db.exec(`CREATE TABLE IF NOT EXISTS fact_qoo10_item_traffic_daily (
    date_jst TEXT NOT NULL ${dateCheck},
    item_no  TEXT NOT NULL,
    channel  TEXT NOT NULL,
    pv INTEGER,
    ${metaCols},
    PRIMARY KEY (date_jst, item_no, channel)
  )`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_fqitd_item ON fact_qoo10_item_traffic_daily(item_no, date_jst)`);

  // 商品マスタ (最新値で上書き。first_seen保持)
  db.exec(`CREATE TABLE IF NOT EXISTS m_qoo10_items (
    item_no TEXT PRIMARY KEY,
    seller_code_raw TEXT, seller_code TEXT,
    item_name TEXT, brand TEXT,
    attr_date_jst TEXT,
    first_seen_at TEXT NOT NULL,
    ${metaCols}
  )`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_mqi_seller ON m_qoo10_items(seller_code)`);

  db.exec(`CREATE TABLE IF NOT EXISTS raw_qoo10_data_import_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    imported_at  TEXT NOT NULL,
    shop_code    TEXT NOT NULL DEFAULT 'b-faith',
    source       TEXT NOT NULL,
    file_name    TEXT NOT NULL,
    file_sha256  TEXT NOT NULL,
    file_type    TEXT NOT NULL,
    date_from    TEXT,
    date_to      TEXT,
    row_count    INTEGER NOT NULL DEFAULT 0,
    inserted     INTEGER NOT NULL DEFAULT 0,
    updated      INTEGER NOT NULL DEFAULT 0,
    status       TEXT NOT NULL CHECK (status IN ('ok','error','skipped','duplicate')),
    message      TEXT NOT NULL DEFAULT ''
  )`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_rqdil_sha ON raw_qoo10_data_import_log(file_sha256, status)`);
}

// ─── ヘルパ ───
const CHANNEL_RE = /^上位のチャネル\(PV\)\s*[:：]\s*(.+)$/;
const TOTAL_RE = /^上位のチャネル\(PV\)_合計/;

function dateFromFileName(name) {
  const m = name.match(/_d(\d{4}-\d{2}-\d{2})_/);
  if (!m) return null;
  return normalizeDate(m[1]);
}

/** ファイル名の要求期間マーカー `_d<from>_<to>_` (fetcherの命名契約)。
 *  scope置換の範囲は「要求期間」が正 — 実データのmin/maxをscopeにすると
 *  中抜けファイルで既存日を巻き添え削除する (Codex R1 High) */
function rangeFromFileName(name) {
  const m = name.match(/_d(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})_/);
  if (!m) return null;
  const from = normalizeDate(m[1]);
  const to = normalizeDate(m[2]);
  return (from && to && from <= to) ? { from, to } : null;
}

function enumerateDates(from, to) {
  const out = [];
  let cur = from;
  while (cur <= to) {
    out.push(cur);
    const d = new Date(cur + 'T00:00:00Z');
    d.setUTCDate(d.getUTCDate() + 1);
    cur = d.toISOString().slice(0, 10);
  }
  return out;
}

/** セル値 → 検証済みプリミティブ。数式セルは拒否 (Codex: cached resultを無条件に信用しない) */
function cellVal(cell, ctx) {
  const v = cell.value;
  if (v && typeof v === 'object' && !(v instanceof Date)) {
    if ('formula' in v || 'sharedFormula' in v) {
      throw new Error(`数式セルは取込不可 (${ctx}: ${cell.address ?? '?'})`);
    }
    if ('richText' in v) return v.richText.map(t => t.text).join('');
    if ('text' in v) return v.text;
    if ('result' in v) throw new Error(`数式セルは取込不可 (${ctx}: ${cell.address ?? '?'})`);
    return String(v);
  }
  return v;
}

/** 日付セル → YYYY-MM-DD (Date object / '2026-07-13' / Excelシリアルは exceljs がDate化) */
function dateVal(cell, ctx) {
  const v = cellVal(cell, ctx);
  if (v instanceof Date) {
    return `${v.getUTCFullYear()}-${String(v.getUTCMonth() + 1).padStart(2, '0')}-${String(v.getUTCDate()).padStart(2, '0')}`;
  }
  const d = normalizeDate(trimS(v));
  if (!d) throw new Error(`日付を解釈できない (${ctx}: 「${trimS(v)}」)`);
  return d;
}

/** 非負整数 (空欄=null と 0 を区別) */
function pvVal(cell, ctx) {
  const v = cellVal(cell, ctx);
  if (v === null || v === undefined || v === '') return null;
  const n = Number(String(v).replace(/[,\s]/g, ''));
  if (!Number.isFinite(n) || n < 0 || !Number.isInteger(n)) {
    throw new Error(`非負整数でない値 (${ctx}: 「${v}」)`);
  }
  return n;
}

/** CVR% — セルのnumFmtに%があれば比率→%換算。値域0〜100で fail-closed
 *  (「百分率の値+%書式」への仕様変更で250%等になる二重換算を検出 — Codex Medium) */
function cvrVal(cell, ctx) {
  const v = cellVal(cell, ctx);
  if (v === null || v === undefined || v === '') return null;
  const n = Number(v);
  if (!Number.isFinite(n)) throw new Error(`CVRが数値でない (${ctx}: 「${v}」)`);
  const fmt = String(cell.numFmt || '');
  const pct = fmt.includes('%') ? n * 100 : n;
  if (pct < 0 || pct > 100) throw new Error(`CVRが値域外 (${ctx}: ${pct})。単位判定/仕様変更の疑い`);
  return pct;
}

/** ヘッダ行を探す (先頭5行から「開始日」を含む行)。列マップを返す */
function mapHeader(ws) {
  for (let r = 1; r <= Math.min(ws.rowCount, 5); r++) {
    const row = ws.getRow(r);
    const cells = [];
    for (let c = 1; c <= ws.columnCount; c++) cells.push(trimS(cellVal(row.getCell(c), `header r${r}c${c}`)));
    if (!cells.includes('開始日')) continue;
    // 正規化後ヘッダの重複検出 (Codex)
    const seen = new Set();
    for (const h of cells.filter(Boolean)) {
      if (seen.has(h)) throw new Error(`ヘッダ重複: 「${h}」`);
      seen.add(h);
    }
    return { headerRow: r, cells };
  }
  return null;
}

// ─── パース: 1ファイル → preps[] (店舗版=2 preps / 商品版=2 preps) ───
export async function prepareQoo10File(name, buffer) {
  // xlsx署名 (PK) を確認 (HTML偽装/破損の検出 — Codex)
  if (!(buffer.length > 4 && buffer[0] === 0x50 && buffer[1] === 0x4b)) {
    return { name, ok: false, type: 'unknown', error: `xlsxでないファイル (magic=${buffer.slice(0, 4).toString('hex')})。HTMLエラーページ/破損の疑い` };
  }
  const wb = new ExcelJS.Workbook();
  try {
    await wb.xlsx.load(buffer);
  } catch (e) {
    return { name, ok: false, type: 'unknown', error: `xlsx読込失敗: ${e.message}` };
  }
  // シートは名前でなく必須ヘッダで識別 (可視シートを走査)
  let ws = null, hdr = null;
  const candidates = [];
  for (const sheet of wb.worksheets) {
    if (sheet.state && sheet.state !== 'visible') continue;
    try {
      const h = mapHeader(sheet);
      if (h) candidates.push([sheet, h]);
    } catch (e) {
      return { name, ok: false, type: 'unknown', error: e.message };
    }
  }
  if (candidates.length === 0) return { name, ok: false, type: 'unknown', error: '「開始日」ヘッダを持つシートが見つからない (形式変更の疑い)' };
  if (candidates.length > 1) return { name, ok: false, type: 'unknown', error: `候補シートが複数 (${candidates.map(([s]) => s.name).join(',')})` };
  [ws, hdr] = candidates[0];

  const isItem = hdr.cells.includes('商品番号');
  try {
    return isItem ? parseItemSheet(name, ws, hdr) : parseStoreSheet(name, ws, hdr);
  } catch (e) {
    return { name, ok: false, type: isItem ? 'qoo10_item_traffic_daily' : 'qoo10_cvr_daily', error: e.message };
  }
}

function channelCols(hdr) {
  const cols = [];
  let totalIdx = -1;
  hdr.cells.forEach((h, i) => {
    const m = h.match(CHANNEL_RE);
    if (m) cols.push({ i: i + 1, channel: trimS(m[1]) });
    else if (TOTAL_RE.test(h)) totalIdx = i + 1;
  });
  // 既知チャネルの激減=形式変更の疑い (Codex)。実測52 → 20未満でエラー
  if (cols.length < 20) throw new Error(`チャネル列が少なすぎる (${cols.length}列)。ヘッダ形式変更の疑い`);
  if (totalIdx < 0) throw new Error('PV合計列が見つからない');
  return { cols, totalIdx };
}

function colOf(hdr, label) {
  const i = hdr.cells.indexOf(label);
  if (i < 0) throw new Error(`列が見つかりません: ${label}`);
  return i + 1;
}

/** 店舗版: fact_qoo10_traffic_channel_daily + fact_qoo10_cvr_daily */
function parseStoreSheet(name, ws, hdr) {
  const { cols, totalIdx } = channelCols(hdr);
  const c = {
    start: colOf(hdr, '開始日'), end: colOf(hdr, '終了日'),
    visitors: colOf(hdr, '訪問者数'), cart: colOf(hdr, '商品カート数'),
    orders: colOf(hdr, '注文完了'), cvr: colOf(hdr, 'コンバージョン率(%)'),
  };
  const chRecords = [], cvrRecords = [];
  const warnings = [];
  const seenDates = new Set();
  for (let r = hdr.headerRow + 1; r <= ws.rowCount; r++) {
    const row = ws.getRow(r);
    const rawStart = cellVal(row.getCell(c.start), `r${r}`);
    if (rawStart === null || rawStart === undefined || trimS(rawStart) === '') continue;
    const d = dateVal(row.getCell(c.start), `r${r} 開始日`);
    const dEnd = dateVal(row.getCell(c.end), `r${r} 終了日`);
    if (d !== dEnd) throw new Error(`日別でない行 (r${r}: ${d}〜${dEnd})。週別/月別ファイルは取込対象外`);
    if (seenDates.has(d)) throw new Error(`日付が重複 (r${r}: ${d})`);
    seenDates.add(d);
    let chSum = 0;
    for (const { i, channel } of cols) {
      const pv = pvVal(row.getCell(i), `r${r} ${channel}`) ?? 0;
      // 「<グループ>_全体」列はグループ小計 (実測: 内訳列と両方あるため単純合算は二重計上)
      if (!/_全体$/.test(channel)) chSum += pv;
      chRecords.push({ date_jst: d, channel, pv });
    }
    const pvTotal = pvVal(row.getCell(totalIdx), `r${r} PV合計`);
    const visitors = pvVal(row.getCell(c.visitors), `r${r} 訪問者数`);
    const orders = pvVal(row.getCell(c.orders), `r${r} 注文完了`);
    const cvr = cvrVal(row.getCell(c.cvr), `r${r} CVR`);
    // 検算: PV合計 vs チャネル合計 (_全体小計は除外済み)。実サンプルで差0を確認済みのため、
    // 小差=warning+記録 (未分類PVの可能性) / 大差=エラー (チャネル欠落・列誤認の fail-closed — Codex Medium)
    const diff = pvTotal === null ? null : pvTotal - chSum;
    if (diff !== null && diff !== 0) {
      const tol = Math.max(10, Math.round((pvTotal ?? 0) * 0.1));
      if (Math.abs(diff) > tol) {
        throw new Error(`PV合計とチャネル計の大差 (${d}: 合計${pvTotal} vs 計${chSum}、差${diff} > 許容${tol})。チャネル列欠落/列誤認の疑い`);
      }
      warnings.push(`${d}: PV合計${pvTotal}≠チャネル計${chSum} (差${diff})`);
    }
    // 検算: CVR ≈ orders/visitors*100 (丸め差1.0pt許容)
    if (cvr !== null && visitors > 0 && orders !== null) {
      const calc = (orders / visitors) * 100;
      if (Math.abs(calc - cvr) > 1.0) throw new Error(`CVR不整合 (${d}: 表記${cvr} vs 計算${calc.toFixed(2)})。単位判定ミスの疑い`);
    }
    cvrRecords.push({
      date_jst: d, pv_total: pvTotal, visitors, cart_adds: pvVal(row.getCell(c.cart), `r${r} カート`),
      orders, cvr_pct: cvr, channel_pv_diff: diff,
    });
  }
  if (cvrRecords.length === 0) return { name, ok: false, type: 'qoo10_cvr_daily', error: 'データ行が0件' };
  const dates = [...seenDates].sort();
  // scope = ファイル名の要求期間 (無ければ実データ範囲)。中抜け検出 (Codex R1 High):
  // 店舗版はゼロの日も必ず行が出る (実測) → scope内の全日が揃っていなければエラー
  // (揃っていないファイルでscope置換すると既存の正常日を巻き添え削除する)
  const range = rangeFromFileName(name) || { from: dates[0], to: dates[dates.length - 1] };
  const missing = enumerateDates(range.from, range.to).filter((d) => !seenDates.has(d));
  const outside = dates.filter((d) => d < range.from || d > range.to);
  if (outside.length > 0) return { name, ok: false, type: 'qoo10_cvr_daily', error: `要求期間 ${range.from}〜${range.to} 外の日付: ${outside.slice(0, 3).join(', ')}` };
  if (missing.length > 0) return { name, ok: false, type: 'qoo10_cvr_daily', error: `期間内に欠落日: ${missing.slice(0, 5).join(', ')} (中抜けファイルはscope置換不可)` };
  const scope = { scopeFrom: range.from, scopeTo: range.to };
  return {
    name, ok: true,
    label: `Analytics店舗CVR (${dates.length}日, チャネル${cols.length}種)`,
    warnings: warnings.slice(0, 5),
    dateFrom: scope.scopeFrom, dateTo: scope.scopeTo,
    preps: [
      { type: 'qoo10_traffic_channel_daily', records: chRecords, ...scope },
      { type: 'qoo10_cvr_daily', records: cvrRecords, ...scope },
    ],
  };
}

/** 商品版: fact_qoo10_item_traffic_daily (sparse) + m_qoo10_items */
function parseItemSheet(name, ws, hdr) {
  const { cols, totalIdx } = channelCols(hdr);
  const c = {
    start: colOf(hdr, '開始日'), end: colOf(hdr, '終了日'),
    itemNo: colOf(hdr, '商品番号'), seller: colOf(hdr, '販売者商品コード'),
    name: colOf(hdr, '商品名'), brand: colOf(hdr, 'ブランド名'),
  };
  const records = [];
  const items = new Map();
  const seen = new Set();
  const dates = new Set();
  for (let r = hdr.headerRow + 1; r <= ws.rowCount; r++) {
    const row = ws.getRow(r);
    const rawStart = cellVal(row.getCell(c.start), `r${r}`);
    if (rawStart === null || rawStart === undefined || trimS(rawStart) === '') continue;
    const d = dateVal(row.getCell(c.start), `r${r} 開始日`);
    if (d !== dateVal(row.getCell(c.end), `r${r} 終了日`)) throw new Error(`日別でない行 (r${r})`);
    const itemNo = trimS(cellVal(row.getCell(c.itemNo), `r${r} 商品番号`));
    if (!itemNo) continue;
    if (/e\+|E\+/.test(itemNo)) throw new Error(`商品番号が指数表記 (r${r}: ${itemNo})。数値化事故の疑い`);
    const key = `${d}|${itemNo}`;
    if (seen.has(key)) throw new Error(`(日付,商品番号) が重複 (r${r}: ${key})`);
    seen.add(key);
    dates.add(d);
    const sellerRaw = trimS(cellVal(row.getCell(c.seller), `r${r} SKU`));
    // マスタは「最新の日付の行」の属性を採用 (最初の行固定だと期間途中の名称/SKU変更を取り逃す — Codex Medium)
    const prev = items.get(itemNo);
    if (!prev || d >= prev._d) {
      items.set(itemNo, {
        _d: d,
        item_no: itemNo, seller_code_raw: sellerRaw || null,
        seller_code: sellerRaw ? normSku(sellerRaw) : null,
        item_name: trimS(cellVal(row.getCell(c.name), `r${r} 商品名`)).slice(0, 300) || null,
        brand: trimS(cellVal(row.getCell(c.brand), `r${r} ブランド`)).slice(0, 120) || null,
      });
    }
    let anyPv = false;
    let chSum = 0;
    for (const { i, channel } of cols) {
      const pv = pvVal(row.getCell(i), `r${r} ${channel}`) ?? 0;
      if (!/_全体$/.test(channel)) chSum += pv;
      if (pv > 0) { records.push({ date_jst: d, item_no: itemNo, channel, pv }); anyPv = true; }
    }
    const total = pvVal(row.getCell(totalIdx), `r${r} PV合計`) ?? 0;
    // 合計行は常に格納 (0でも「その日その商品が出現した」ことの記録。sparse検証の基準)
    records.push({ date_jst: d, item_no: itemNo, channel: '(合計)', pv: total });
    if (total === 0 && anyPv) throw new Error(`PV合計0なのにチャネルPVあり (r${r}: ${key})`);
    // 行検算: チャネル欠落/列誤認の検出 (店舗版と同じ許容 — Codex R2 Medium)
    const diff = total - chSum;
    if (diff !== 0 && Math.abs(diff) > Math.max(10, Math.round(total * 0.1))) {
      throw new Error(`商品行のPV合計とチャネル計の大差 (${key}: 合計${total} vs 計${chSum})。チャネル列欠落の疑い`);
    }
  }
  if (records.length === 0) return { name, ok: false, type: 'qoo10_item_traffic_daily', error: 'データ行が0件' };
  const ds = [...dates].sort();
  // 商品版は (日×商品) がsparse (活動のない日は行が無い) → 中抜けは正常。
  // scope = ファイル名の要求期間 (fetcher契約)。マーカー無し (手動DL等) は実データ日付のみ置換
  // (min〜maxをscopeにすると中抜け日を巻き添え削除する — Codex R1 High)
  const range = rangeFromFileName(name);
  const outside = range ? ds.filter((d) => d < range.from || d > range.to) : [];
  if (outside.length > 0) return { name, ok: false, type: 'qoo10_item_traffic_daily', error: `要求期間 ${range.from}〜${range.to} 外の日付: ${outside.slice(0, 3).join(', ')}` };
  const scope = range
    ? { scopeFrom: range.from, scopeTo: range.to }
    : { scopeFrom: ds[0], scopeTo: ds[ds.length - 1], scopeDatesOnly: ds };
  const masterRecords = [...items.values()].map(({ _d, ...rest }) => ({ ...rest, attr_date_jst: _d }));
  return {
    name, ok: true,
    label: `Analytics商品別トラフィック (${ds.length}日×${items.size}商品, sparse ${records.length}行)`,
    warnings: range ? [] : ['ファイル名に期間マーカーなし → 実データ日付のみ置換 (期間全体の置換はfetcher命名 qoo10_cvritem_d<from>_<to>_ で)'],
    dateFrom: scope.scopeFrom, dateTo: scope.scopeTo,
    preps: [
      { type: 'qoo10_item_traffic_daily', records, ...scope },
      { type: 'qoo10_items', records: masterRecords, ...scope },
    ],
  };
}

// ─── UPSERT specs ───
const Q_SPECS = {
  qoo10_traffic_channel_daily: {
    table: 'fact_qoo10_traffic_channel_daily', scopeReplace: true,
    cols: ['date_jst', 'channel', 'pv'],
  },
  qoo10_cvr_daily: {
    table: 'fact_qoo10_cvr_daily', scopeReplace: true,
    cols: ['date_jst', 'pv_total', 'visitors', 'cart_adds', 'orders', 'cvr_pct', 'channel_pv_diff'],
  },
  qoo10_item_traffic_daily: {
    table: 'fact_qoo10_item_traffic_daily', scopeReplace: true,
    cols: ['date_jst', 'item_no', 'channel', 'pv'],
  },
  qoo10_items: {
    table: 'm_qoo10_items', scopeReplace: false, pk: ['item_no'],
    cols: ['item_no', 'seller_code_raw', 'seller_code', 'item_name', 'brand', 'attr_date_jst'],
  },
};
export const QOO10_DATA_TYPES = new Set(Object.keys(Q_SPECS));

function getLogInsertStmt(db) {
  return db.prepare(`INSERT INTO raw_qoo10_data_import_log
    (imported_at, source, file_name, file_sha256, file_type, date_from, date_to, row_count, inserted, updated, status, message)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
}

function commitPrep(db, logStmt, p, ctx, extraMsg) {
  const spec = Q_SPECS[p.type];
  let replaced = 0;
  const logInfo = logStmt.run(ctx.importedAt, ctx.source, ctx.fileName, ctx.fileSha256, p.type,
    p.scopeFrom, p.scopeTo, p.records.length, 0, 0, 'ok', '');
  const importId = logInfo.lastInsertRowid;
  const meta = { source_file: ctx.fileName.slice(0, 200), import_id: importId, imported_at: ctx.importedAt, updated_at: ctx.importedAt };
  if (spec.scopeReplace) {
    if (p.scopeDatesOnly) {
      // マーカー無しファイル: 実データ日付のみ置換 (中抜け日の巻き添え削除防止)
      const del = db.prepare(`DELETE FROM ${spec.table} WHERE date_jst = ?`);
      for (const d of p.scopeDatesOnly) replaced += del.run(d).changes;
    } else {
      replaced = db.prepare(`DELETE FROM ${spec.table} WHERE date_jst >= ? AND date_jst <= ?`).run(p.scopeFrom, p.scopeTo).changes;
    }
    const ins = db.prepare(`INSERT INTO ${spec.table}
      (${spec.cols.join(', ')}, source_file, import_id, imported_at, updated_at)
      VALUES (${spec.cols.map(k => '@' + k).join(', ')}, @source_file, @import_id, @imported_at, @updated_at)`);
    for (const r of p.records) ins.run({ ...r, ...meta });
  } else {
    // マスタ: item_no upsert (first_seen_at 保持)
    const up = db.prepare(`INSERT INTO ${spec.table}
      (${spec.cols.join(', ')}, first_seen_at, source_file, import_id, imported_at, updated_at)
      VALUES (${spec.cols.map(k => '@' + k).join(', ')}, @imported_at, @source_file, @import_id, @imported_at, @updated_at)
      ON CONFLICT (item_no) DO UPDATE SET
        ${spec.cols.filter(k => !spec.pk.includes(k)).map(k => `${k}=excluded.${k}`).join(', ')},
        source_file=excluded.source_file, import_id=excluded.import_id,
        imported_at=excluded.imported_at, updated_at=excluded.updated_at
      WHERE excluded.attr_date_jst >= COALESCE(${spec.table}.attr_date_jst, '')`);
    for (const r of p.records) up.run({ ...r, ...meta });
  }
  const msg = [`scope置換 ${p.scopeFrom}〜${p.scopeTo}${spec.scopeReplace ? ` (旧${replaced}行削除)` : ' (master upsert)'}`, ...(extraMsg || [])]
    .join(' / ').slice(0, 500);
  db.prepare(`UPDATE raw_qoo10_data_import_log SET inserted=?, updated=?, message=? WHERE id=?`)
    .run(p.records.length, replaced, msg, importId);
  return { type: p.type, rows: p.records.length, replaced };
}

/** 入口: xlsx 1ファイルを1トランザクションで取込 (パースは非同期で先に済ませる) */
export async function importQoo10File(db, { name, buffer, sha256, source = 'incoming' }) {
  const importedAt = new Date().toISOString();
  const logStmt = getLogInsertStmt(db);
  const logOutcome = (status, message, fileType = 'unknown') => {
    try {
      logStmt.run(importedAt, source, name, sha256, fileType, null, null, 0, 0, 0, status, String(message || '').slice(0, 500));
    } catch (e) { console.error(`[qoo10-data-import] ログ記録失敗: ${e.message}`); }
  };

  // 冪等キー: sha256 + 要求期間 (from AND to)。sparse商品版は「同一内容で期間だけ広い」再取得が
  // あり得る (7/1-14 と 7/1-31 が同一SHA) ため、date_from だけだと後者が duplicate 扱いになり
  // 広がった期間の scope 置換がスキップされる (Codex R2 High)
  const range = rangeFromFileName(name);
  const dup = range
    ? db.prepare(`SELECT id FROM raw_qoo10_data_import_log WHERE file_sha256 = ? AND status = 'ok' AND date_from = ? AND date_to = ? LIMIT 1`).get(sha256, range.from, range.to)
    : db.prepare(`SELECT id FROM raw_qoo10_data_import_log WHERE file_sha256 = ? AND status = 'ok' LIMIT 1`).get(sha256);
  if (dup) {
    logOutcome('duplicate', `同一内容のファイルを取込済み (import log id=${dup.id})`);
    return { status: 'duplicate', results: [{ file: name, ok: false, duplicate: true, error: '取込済み (同一sha256)' }] };
  }

  const parsed = await prepareQoo10File(name, buffer);
  if (!parsed.ok) {
    logOutcome('error', parsed.error, parsed.type);
    return { status: 'error', results: [{ file: name, ok: false, type: parsed.type, error: parsed.error }] };
  }

  const ctx = { importedAt, source, fileName: name, fileSha256: sha256 };
  const collected = [];
  try {
    db.transaction(() => {
      for (const p of parsed.preps) collected.push(commitPrep(db, logStmt, p, ctx, parsed.warnings));
    })();
  } catch (e) {
    logOutcome('error', e.message);
    return { status: 'error', results: [{ file: name, ok: false, type: 'unknown', error: e.message }] };
  }
  return {
    status: 'ok',
    results: [{
      file: name, ok: true, label: parsed.label, warnings: parsed.warnings || [],
      date_from: parsed.dateFrom, date_to: parsed.dateTo,
      detail: collected.map(cRes => `${cRes.type}=${cRes.rows}行(旧${cRes.replaced})`).join(' / '),
      rows: collected.reduce((a, cRes) => a + cRes.rows, 0),
    }],
  };
}
