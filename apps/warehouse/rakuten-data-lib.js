/**
 * rakuten-data-lib.js — 楽天RMS「データ分析」CSV の取込ライブラリ (mall-csv-fetcher P1-R2)
 *
 * 対象レポート (実ファイルで仕様確定 2026-07-10):
 *   1. 商品分析 (item_list): SKU×日次 — 売上/アクセス/UU/CVR/新規・リピート/レビュー/
 *      滞在・直帰・離脱/お気に入り/在庫数。UTF-8 BOM、ヘッダ前に 商品分析/表示期間/キーワード/端末 の前置行。
 *      → fact_rakuten_item_daily (PK: date_jst × item_manage_number) + m_rakuten_items (最新名/ジャンル)
 *   2. 日次_分析用レポート: 店舗×日次 70列 — 売上/件数/アクセス/転換率/客単価 ×端末4種、
 *      商圏ベンチマーク (サブジャンルTOP10平均・月商別平均値)、
 *      金額内訳 (税額/送料/クーポン店舗/クーポン楽天/送料無料クーポン/のし・ラッピング/決済手数料)。CP932。
 *      → fact_rakuten_store_daily (PK: date_jst)
 *
 * 設計判断:
 *   - ベンチマークは「すべて」端末のみ保存 (端末別ベンチは列爆発の割に用途薄。原本は processed/ に残る)
 *   - 商品名/ジャンルは fact に持たず m_rakuten_items へ最新値 UPSERT (fact肥大防止)
 *   - 商品分析は 表示期間 from==to (1日単位) を必須化。範囲DLは日次粒度が壊れるためエラーで弾く
 *   - キーワード絞り込み・端末絞り込みされたDLは部分データ → エラーで弾く (黙って混ぜない)
 *   - 金額=税込円 INTEGER / 率=REAL / SKU=normSku (全社規約)。ベンチマークは平均値のため REAL
 *   - 過去分は再DLで変動しうる → 同一 (キー) は UPSERT。sha256 冪等 (同一ファイル再投入はスキップ)
 *
 * CSV解析・zip展開・数値正規化は rakuten-ads-rpp-lib.js の実装を再利用する。
 */
import { normSku } from '../../lib/sku-norm.js';
import {
  parseCsvBuffer, numOrNull, normalizeDate, expandFile, GroupAbortError,
} from './rakuten-ads-rpp-lib.js';

const trimS = v => String(v == null ? '' : v).trim();
const yenOrNull = v => { const n = numOrNull(v); return n === null ? null : Math.round(n); };
const intOrNull = v => { const n = numOrNull(v); return n === null ? null : Math.round(n); };

// ─── DDL ───
export function ensureRakutenDataTables(db) {
  db.exec(`CREATE TABLE IF NOT EXISTS fact_rakuten_item_daily (
    date_jst            TEXT NOT NULL CHECK (date_jst GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'),
    item_manage_number  TEXT NOT NULL,
    raw_sku_code        TEXT,
    sales_yen           INTEGER,
    orders              INTEGER,
    units               INTEGER,
    access_users        INTEGER,
    unique_users        INTEGER,
    cvr_pct             REAL,
    aov_yen             INTEGER,
    buyers_total        INTEGER,
    buyers_new          INTEGER,
    buyers_repeat       INTEGER,
    nonbuyer_access     INTEGER,
    review_posts        INTEGER,
    review_avg          REAL,
    review_total        INTEGER,
    stay_seconds        INTEGER,
    bounce_count        INTEGER,
    exit_count          INTEGER,
    exit_rate_pct       REAL,
    favorites_added     INTEGER,
    favorites_total     INTEGER,
    stock_qty           INTEGER,
    is_tax_included     INTEGER NOT NULL DEFAULT 1,
    source_file         TEXT,
    import_id           INTEGER,
    imported_at         TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    PRIMARY KEY (date_jst, item_manage_number)
  )`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_frid_item ON fact_rakuten_item_daily(item_manage_number, date_jst)`);

  // 商品マスタ (最新値のみ。名称・ジャンルの time-travel はしない — 原本CSVがアーカイブ)
  db.exec(`CREATE TABLE IF NOT EXISTS m_rakuten_items (
    item_manage_number  TEXT PRIMARY KEY,
    raw_sku_code        TEXT,
    item_id             INTEGER,
    catalog_id          TEXT,
    item_number         TEXT,
    item_name           TEXT,
    genre_path          TEXT,
    updated_at          TEXT NOT NULL
  )`);
  // item_number は後付け列 (Codex R2 high: 旧スキーマで作成済みの環境向け冪等 migration)
  const miCols = db.prepare(`PRAGMA table_info(m_rakuten_items)`).all().map(x => x.name);
  if (!miCols.includes('item_number')) db.exec(`ALTER TABLE m_rakuten_items ADD COLUMN item_number TEXT`);

  db.exec(`CREATE TABLE IF NOT EXISTS fact_rakuten_store_daily (
    date_jst              TEXT PRIMARY KEY CHECK (date_jst GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'),
    sales_all_yen         INTEGER,
    sales_pc_yen          INTEGER,
    sales_app_yen         INTEGER,
    sales_sp_yen          INTEGER,
    orders_all            INTEGER,
    orders_pc             INTEGER,
    orders_app            INTEGER,
    orders_sp             INTEGER,
    access_all            INTEGER,
    access_pc             INTEGER,
    access_app            INTEGER,
    access_sp             INTEGER,
    cvr_all_pct           REAL,
    cvr_pc_pct            REAL,
    cvr_app_pct           REAL,
    cvr_sp_pct            REAL,
    aov_all_yen           INTEGER,
    aov_pc_yen            INTEGER,
    aov_app_yen           INTEGER,
    aov_sp_yen            INTEGER,
    bench_top10_sales_yen REAL,
    bench_top10_orders    REAL,
    bench_top10_access    REAL,
    bench_top10_cvr_pct   REAL,
    bench_top10_aov_yen   REAL,
    bench_class_label     TEXT,
    bench_class_sales_yen REAL,
    bench_class_orders    REAL,
    bench_class_access    REAL,
    bench_class_cvr_pct   REAL,
    bench_class_aov_yen   REAL,
    tax_out_yen           INTEGER,
    shipping_yen          INTEGER,
    coupon_store_yen      INTEGER,
    coupon_rakuten_yen    INTEGER,
    free_ship_coupon_yen  INTEGER,
    wrapping_yen          INTEGER,
    settlement_fee_yen    INTEGER,
    is_tax_included       INTEGER NOT NULL DEFAULT 1,
    source_file           TEXT,
    import_id             INTEGER,
    imported_at           TEXT NOT NULL,
    updated_at            TEXT NOT NULL
  )`);

  // 取込ログ (raw_rakuten_ads_import_log と同型。sha256 冪等の根拠)
  db.exec(`CREATE TABLE IF NOT EXISTS raw_rakuten_data_import_log (
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
  db.exec(`CREATE INDEX IF NOT EXISTS idx_rrdil_sha ON raw_rakuten_data_import_log(file_sha256, status)`);
}

// ─── ヘッダ探索ヘルパ ───
function findHeaderRow(rows, requiredCols, maxScan = 12) {
  for (let i = 0; i < Math.min(rows.length, maxScan); i++) {
    const cells = rows[i].map(trimS);
    if (requiredCols.every(c => cells.includes(c))) return i;
  }
  return -1;
}
function colIndex(header, name) {
  const i = header.indexOf(name);
  if (i < 0) throw new Error(`列が見つかりません: ${name}`);
  return i;
}
function colIndexRe(header, re) {
  const i = header.findIndex(h => re.test(h));
  return i; // 見つからなければ -1 (ベンチマーク等、任意列)
}

// ─── レシピ1: 商品分析 (item_list) — SKU×日次 ───
function prepareItemDaily(name, rows, headerIdx) {
  const header = rows[headerIdx].map(trimS);

  // 前置行ガード: 表示期間は1日単位必須 / キーワード絞り込み禁止 / 端末=すべて必須
  let dateJst = null;
  for (let i = 0; i < headerIdx; i++) {
    const line = rows[i].map(trimS);
    const joined = line.join(',');
    const m = joined.match(/(\d{4})年(\d{1,2})月(\d{1,2})日から(\d{4})年(\d{1,2})月(\d{1,2})日/);
    if (m) {
      const from = `${m[1]}-${String(+m[2]).padStart(2, '0')}-${String(+m[3]).padStart(2, '0')}`;
      const to = `${m[4]}-${String(+m[5]).padStart(2, '0')}-${String(+m[6]).padStart(2, '0')}`;
      if (from !== to) {
        return { name, ok: false, type: 'rakuten_item_daily', error: `表示期間が複数日 (${from}〜${to})。商品分析は「1日単位」でDLしてください (日次粒度が壊れるため)` };
      }
      dateJst = from;
    }
    if (line[0] === 'キーワード' && trimS(line[1] || '') !== '') {
      return { name, ok: false, type: 'rakuten_item_daily', error: `キーワード絞り込み (${line[1]}) されたDLは部分データのため取込不可。絞り込みなしでDLし直してください` };
    }
    if (line[0] === '端末' && line[1] && line[1] !== 'すべて') {
      return { name, ok: false, type: 'rakuten_item_daily', error: `端末=${line[1]} で絞り込まれたDLは取込不可。「すべて」でDLし直してください` };
    }
  }
  if (!dateJst) {
    return { name, ok: false, type: 'rakuten_item_daily', error: '前置行から表示期間 (YYYY年MM月DD日からYYYY年MM月DD日) を特定できません' };
  }

  const c = {
    genre: colIndex(header, 'ジャンル'), catalog: colIndex(header, 'カタログID'),
    itemId: colIndex(header, '商品ID'), name: colIndex(header, '商品名'),
    manage: colIndex(header, '商品管理番号'), itemNumber: colIndex(header, '商品番号'),
    sales: colIndex(header, '売上'), orders: colIndex(header, '売上件数'), units: colIndex(header, '売上個数'),
    access: colIndex(header, 'アクセス人数'), uu: colIndex(header, 'ユニークユーザー数'),
    cvr: colIndex(header, '転換率'), aov: colIndex(header, '客単価'),
    buyersTotal: colIndex(header, '総購入件数'), buyersNew: colIndex(header, '新規購入件数'),
    buyersRepeat: colIndex(header, 'リピート購入件数'), nonbuyer: colIndex(header, '未購入アクセス人数'),
    revPosts: colIndex(header, 'レビュー投稿数'), revAvg: colIndex(header, 'レビュー総合評価（点）'),
    revTotal: colIndex(header, '総レビュー数'), stay: colIndex(header, '滞在時間（秒）'),
    bounce: colIndex(header, '直帰数'), exits: colIndex(header, '離脱数'), exitRate: colIndex(header, '離脱率'),
    favAdd: colIndex(header, 'お気に入り登録ユーザ数'), favTotal: colIndex(header, 'お気に入り総ユーザ数'),
    stock: colIndex(header, '在庫数'),
  };

  const records = [];
  const dims = [];
  for (let i = headerIdx + 1; i < rows.length; i++) {
    const r = rows[i];
    // 全セル空行はスキップ、非空の短い行は列ズレ/破損CSVの兆候 → ファイル単位でエラー (Codex R1 low / R2 low)
    // 実CSV検証 2026-07-10: 商品分析1,372行・店舗日次32行とも全行 header と同列数 → 完全一致を要求
    if (r.every(v => trimS(v) === '')) continue;
    if (r.length !== header.length) {
      return { name, ok: false, type: 'rakuten_item_daily', error: `列数不一致の行 (row ${i + 1}: ${r.length}列/${header.length}列)。CSVが破損している可能性` };
    }
    const rawManage = trimS(r[c.manage]);
    if (!rawManage) continue;
    const sku = normSku(rawManage);
    records.push({
      date_jst: dateJst, item_manage_number: sku, raw_sku_code: rawManage,
      sales_yen: yenOrNull(r[c.sales]), orders: intOrNull(r[c.orders]), units: intOrNull(r[c.units]),
      access_users: intOrNull(r[c.access]), unique_users: intOrNull(r[c.uu]),
      cvr_pct: numOrNull(r[c.cvr]), aov_yen: yenOrNull(r[c.aov]),
      buyers_total: intOrNull(r[c.buyersTotal]), buyers_new: intOrNull(r[c.buyersNew]),
      buyers_repeat: intOrNull(r[c.buyersRepeat]), nonbuyer_access: intOrNull(r[c.nonbuyer]),
      review_posts: intOrNull(r[c.revPosts]), review_avg: numOrNull(r[c.revAvg]),
      review_total: intOrNull(r[c.revTotal]), stay_seconds: intOrNull(r[c.stay]),
      bounce_count: intOrNull(r[c.bounce]), exit_count: intOrNull(r[c.exits]),
      exit_rate_pct: numOrNull(r[c.exitRate]),
      favorites_added: intOrNull(r[c.favAdd]), favorites_total: intOrNull(r[c.favTotal]),
      stock_qty: intOrNull(r[c.stock]),
    });
    dims.push({
      item_manage_number: sku, raw_sku_code: rawManage,
      item_id: intOrNull(r[c.itemId]), catalog_id: trimS(r[c.catalog]) || null,
      item_number: trimS(r[c.itemNumber]) || null,
      item_name: trimS(r[c.name]).slice(0, 300) || null, genre_path: trimS(r[c.genre]).slice(0, 300) || null,
    });
  }
  if (records.length === 0) {
    return { name, ok: false, type: 'rakuten_item_daily', error: 'データ行が0件 (DL条件を確認してください)' };
  }
  // ファイル内 PK 重複は黙って統合しない (ads lib と同方針)
  const seen = new Set(); const dups = [];
  for (const r of records) {
    if (seen.has(r.item_manage_number)) dups.push(r.item_manage_number); else seen.add(r.item_manage_number);
  }
  if (dups.length > 0) {
    return { name, ok: false, type: 'rakuten_item_daily', error: `ファイル内で商品管理番号が重複: ${[...new Set(dups)].slice(0, 5).join(', ')} 等 ${dups.length}件` };
  }
  return { name, ok: true, type: 'rakuten_item_daily', label: '商品分析 (SKU×日次)', records, dims, dateFrom: dateJst, dateTo: dateJst };
}

// ─── レシピ2: 日次_分析用レポート — 店舗×日次 ───
function prepareStoreDaily(name, rows, headerIdx) {
  const header = rows[headerIdx].map(trimS);
  const dev = { all: 'すべて', pc: 'PC', app: '楽天市場アプリ', sp: 'スマートフォン' };
  const metric = (m, d) => colIndex(header, `${m}（${dev[d]}）`);
  const benchTop10 = (m) => colIndexRe(header, new RegExp(`^サブジャンルTOP10平均 ${m}（すべて）$`));
  const benchClass = (m) => colIndexRe(header, new RegExp(`^月商別平均値（.+） ${m}（すべて）$`));

  // 月商クラスのラベル (ヘッダから抽出。店舗の月商が変わるとラベルも変わる)
  let benchClassLabel = null;
  const bcHeader = header.find(h => /^月商別平均値（(.+)） 売上金額（すべて）$/.test(h));
  // (.+)） の貪欲マッチだと「月商1,000万～2,999万） 売上金額（すべて」まで食う → 末尾まで固定した regex で抽出
  if (bcHeader) benchClassLabel = bcHeader.match(/^月商別平均値（(.+)） 売上金額（すべて）$/)[1];

  const c = {
    date: colIndex(header, '日付'),
    sales: { all: metric('売上金額', 'all'), pc: metric('売上金額', 'pc'), app: metric('売上金額', 'app'), sp: metric('売上金額', 'sp') },
    orders: { all: metric('売上件数', 'all'), pc: metric('売上件数', 'pc'), app: metric('売上件数', 'app'), sp: metric('売上件数', 'sp') },
    access: { all: metric('アクセス人数', 'all'), pc: metric('アクセス人数', 'pc'), app: metric('アクセス人数', 'app'), sp: metric('アクセス人数', 'sp') },
    cvr: { all: metric('転換率', 'all'), pc: metric('転換率', 'pc'), app: metric('転換率', 'app'), sp: metric('転換率', 'sp') },
    aov: { all: metric('客単価', 'all'), pc: metric('客単価', 'pc'), app: metric('客単価', 'app'), sp: metric('客単価', 'sp') },
    bt: { sales: benchTop10('売上金額'), orders: benchTop10('売上件数'), access: benchTop10('アクセス人数'), cvr: benchTop10('転換率'), aov: benchTop10('客単価') },
    bc: { sales: benchClass('売上金額'), orders: benchClass('売上件数'), access: benchClass('アクセス人数'), cvr: benchClass('転換率'), aov: benchClass('客単価') },
    taxOut: header.indexOf('税額（外税額）'),
    shipping: header.indexOf('送料額'),
    couponStore: header.indexOf('クーポン値引額（店舗）'),
    couponRakuten: header.indexOf('クーポン値引額（楽天）'),
    freeShipCoupon: header.indexOf('送料無料クーポン'),
    wrapping: header.indexOf('のし・ラッピング代金'),
    settlementFee: header.indexOf('決済手数料'),
  };
  const at = (r, i) => (i >= 0 ? r[i] : null);

  const records = [];
  let unaggregatedSkipped = 0;
  for (let i = headerIdx + 1; i < rows.length; i++) {
    const r = rows[i];
    if (r.every(v => trimS(v) === '')) continue;
    const rawDate = trimS(r[c.date]);
    const date = normalizeDate(rawDate);
    if (!date) {
      if (/実績|合計/.test(rawDate)) continue; // 「通算実績」等の集計行はスキップ
      return { name, ok: false, type: 'rakuten_store_daily', error: `日付を解釈できない行: 「${rawDate}」` };
    }
    // 非空の短い行は列ズレ/破損CSVの兆候 → ファイル単位でエラー (Codex R1 low / R2 low、実CSVは全行 header と同列数)
    if (r.length !== header.length) {
      return { name, ok: false, type: 'rakuten_store_daily', error: `列数不一致の行 (${date}: ${r.length}列/${header.length}列)。CSVが破損している可能性` };
    }
    // 未集計行 (未来日、早朝DL時の当日等): 自店指標=0 かつ ベンチマーク空 → スキップ。
    // 0で埋めると実績と区別がつかない。稼働店舗で真のゼロ日は access=0 になり得ず、
    // 仮に休店でもベンチ (商圏平均) は埋まるため、この複合条件が偽陽性になることは実質ない。
    // 早朝境界で前日行が未集計でも、翌日の再DL+scope clear 同期で自己修復する (RPP #467 と同じ構え)
    const salesAll = yenOrNull(r[c.sales.all]);
    const accessAll = intOrNull(r[c.access.all]);
    const btSalesRaw = trimS(at(r, c.bt.sales) ?? '');
    if ((salesAll ?? 0) === 0 && (accessAll ?? 0) === 0 && btSalesRaw === '') { unaggregatedSkipped++; continue; }

    records.push({
      date_jst: date,
      sales_all_yen: salesAll, sales_pc_yen: yenOrNull(r[c.sales.pc]), sales_app_yen: yenOrNull(r[c.sales.app]), sales_sp_yen: yenOrNull(r[c.sales.sp]),
      orders_all: intOrNull(r[c.orders.all]), orders_pc: intOrNull(r[c.orders.pc]), orders_app: intOrNull(r[c.orders.app]), orders_sp: intOrNull(r[c.orders.sp]),
      access_all: accessAll, access_pc: intOrNull(r[c.access.pc]), access_app: intOrNull(r[c.access.app]), access_sp: intOrNull(r[c.access.sp]),
      cvr_all_pct: numOrNull(r[c.cvr.all]), cvr_pc_pct: numOrNull(r[c.cvr.pc]), cvr_app_pct: numOrNull(r[c.cvr.app]), cvr_sp_pct: numOrNull(r[c.cvr.sp]),
      aov_all_yen: yenOrNull(r[c.aov.all]), aov_pc_yen: yenOrNull(r[c.aov.pc]), aov_app_yen: yenOrNull(r[c.aov.app]), aov_sp_yen: yenOrNull(r[c.aov.sp]),
      bench_top10_sales_yen: numOrNull(at(r, c.bt.sales)), bench_top10_orders: numOrNull(at(r, c.bt.orders)),
      bench_top10_access: numOrNull(at(r, c.bt.access)), bench_top10_cvr_pct: numOrNull(at(r, c.bt.cvr)),
      bench_top10_aov_yen: numOrNull(at(r, c.bt.aov)),
      bench_class_label: benchClassLabel,
      bench_class_sales_yen: numOrNull(at(r, c.bc.sales)), bench_class_orders: numOrNull(at(r, c.bc.orders)),
      bench_class_access: numOrNull(at(r, c.bc.access)), bench_class_cvr_pct: numOrNull(at(r, c.bc.cvr)),
      bench_class_aov_yen: numOrNull(at(r, c.bc.aov)),
      tax_out_yen: yenOrNull(at(r, c.taxOut)), shipping_yen: yenOrNull(at(r, c.shipping)),
      coupon_store_yen: yenOrNull(at(r, c.couponStore)), coupon_rakuten_yen: yenOrNull(at(r, c.couponRakuten)),
      free_ship_coupon_yen: yenOrNull(at(r, c.freeShipCoupon)), wrapping_yen: yenOrNull(at(r, c.wrapping)),
      settlement_fee_yen: yenOrNull(at(r, c.settlementFee)),
    });
  }
  if (records.length === 0) {
    return { name, ok: false, type: 'rakuten_store_daily', error: '実績のあるデータ行が0件 (未来期間のみのDL?)' };
  }
  const seen = new Set(); const dups = [];
  for (const r of records) { if (seen.has(r.date_jst)) dups.push(r.date_jst); else seen.add(r.date_jst); }
  if (dups.length > 0) {
    return { name, ok: false, type: 'rakuten_store_daily', error: `ファイル内で日付が重複: ${[...new Set(dups)].join(', ')}` };
  }
  const dates = records.map(r => r.date_jst).sort();
  const label = unaggregatedSkipped > 0
    ? `店舗日次 (分析用レポート、未集計${unaggregatedSkipped}日スキップ)`
    : '店舗日次 (分析用レポート)';
  return { name, ok: true, type: 'rakuten_store_daily', label, records, dateFrom: dates[0], dateTo: dates[dates.length - 1] };
}

// ─── ファイル種別判別 + パース ───
export function prepareDataFile(name, buffer) {
  const rows = parseCsvBuffer(buffer);
  if (rows.length === 0) return { name, ok: false, type: 'unknown', error: '空ファイル' };

  const itemIdx = findHeaderRow(rows, ['商品管理番号', 'アクセス人数', '在庫数']);
  if (itemIdx >= 0) return prepareItemDaily(name, rows, itemIdx);

  const storeIdx = findHeaderRow(rows, ['日付', '曜日', '売上金額（すべて）']);
  if (storeIdx >= 0) return prepareStoreDaily(name, rows, storeIdx);

  // RPP系のファイルが誤ってこちらに投入されたケースへの案内
  const rppIdx = findHeaderRow(rows, ['コントロールカラム']);
  if (rppIdx >= 0) {
    return { name, ok: false, type: 'unknown', error: 'RPP広告レポートです。incoming/rakuten-ads/ に置いてください' };
  }
  return { name, ok: false, type: 'unknown', error: '種類を判別できません (商品分析 / 日次_分析用レポート のどちらでもない)' };
}

// ─── 確定: 検証済み1ファイル分を UPSERT (呼び出し元 transaction 内) ───
function commitOne(db, logStmt, p, ctx) {
  const { importedAt, source, fileSha256 } = ctx;
  let inserted = 0, updated = 0;
  const logInfo = logStmt.run(importedAt, source, p.name, fileSha256, p.type, p.dateFrom, p.dateTo, p.records.length, 0, 0, 'ok', '');
  const importId = logInfo.lastInsertRowid;
  const meta = { source_file: p.name.slice(0, 200), import_id: importId, imported_at: importedAt, updated_at: importedAt };

  if (p.type === 'rakuten_item_daily') {
    const existsStmt = db.prepare(`SELECT 1 FROM fact_rakuten_item_daily WHERE date_jst=? AND item_manage_number=?`);
    const cols = ['date_jst', 'item_manage_number', 'raw_sku_code', 'sales_yen', 'orders', 'units',
      'access_users', 'unique_users', 'cvr_pct', 'aov_yen', 'buyers_total', 'buyers_new', 'buyers_repeat',
      'nonbuyer_access', 'review_posts', 'review_avg', 'review_total', 'stay_seconds', 'bounce_count',
      'exit_count', 'exit_rate_pct', 'favorites_added', 'favorites_total', 'stock_qty'];
    const upsert = db.prepare(`INSERT INTO fact_rakuten_item_daily
      (${cols.join(', ')}, source_file, import_id, imported_at, updated_at)
      VALUES (${cols.map(k => '@' + k).join(', ')}, @source_file, @import_id, @imported_at, @updated_at)
      ON CONFLICT (date_jst, item_manage_number) DO UPDATE SET
        ${cols.filter(k => k !== 'date_jst' && k !== 'item_manage_number').map(k => `${k}=excluded.${k}`).join(', ')},
        source_file=excluded.source_file, import_id=excluded.import_id,
        imported_at=excluded.imported_at, updated_at=excluded.updated_at`);
    const dimUpsert = db.prepare(`INSERT INTO m_rakuten_items
      (item_manage_number, raw_sku_code, item_id, catalog_id, item_number, item_name, genre_path, updated_at)
      VALUES (@item_manage_number, @raw_sku_code, @item_id, @catalog_id, @item_number, @item_name, @genre_path, @updated_at)
      ON CONFLICT (item_manage_number) DO UPDATE SET
        raw_sku_code=excluded.raw_sku_code, item_id=excluded.item_id, catalog_id=excluded.catalog_id,
        item_number=excluded.item_number, item_name=excluded.item_name, genre_path=excluded.genre_path,
        updated_at=excluded.updated_at`);
    for (const r of p.records) {
      const exists = existsStmt.get(r.date_jst, r.item_manage_number);
      upsert.run({ ...r, ...meta });
      if (exists) updated++; else inserted++;
    }
    for (const d of p.dims) dimUpsert.run({ ...d, updated_at: importedAt });
  } else {
    const existsStmt = db.prepare(`SELECT 1 FROM fact_rakuten_store_daily WHERE date_jst=?`);
    const cols = ['date_jst',
      'sales_all_yen', 'sales_pc_yen', 'sales_app_yen', 'sales_sp_yen',
      'orders_all', 'orders_pc', 'orders_app', 'orders_sp',
      'access_all', 'access_pc', 'access_app', 'access_sp',
      'cvr_all_pct', 'cvr_pc_pct', 'cvr_app_pct', 'cvr_sp_pct',
      'aov_all_yen', 'aov_pc_yen', 'aov_app_yen', 'aov_sp_yen',
      'bench_top10_sales_yen', 'bench_top10_orders', 'bench_top10_access', 'bench_top10_cvr_pct', 'bench_top10_aov_yen',
      'bench_class_label', 'bench_class_sales_yen', 'bench_class_orders', 'bench_class_access', 'bench_class_cvr_pct', 'bench_class_aov_yen',
      'tax_out_yen', 'shipping_yen', 'coupon_store_yen', 'coupon_rakuten_yen',
      'free_ship_coupon_yen', 'wrapping_yen', 'settlement_fee_yen'];
    const upsert = db.prepare(`INSERT INTO fact_rakuten_store_daily
      (${cols.join(', ')}, source_file, import_id, imported_at, updated_at)
      VALUES (${cols.map(k => '@' + k).join(', ')}, @source_file, @import_id, @imported_at, @updated_at)
      ON CONFLICT (date_jst) DO UPDATE SET
        ${cols.filter(k => k !== 'date_jst').map(k => `${k}=excluded.${k}`).join(', ')},
        source_file=excluded.source_file, import_id=excluded.import_id,
        imported_at=excluded.imported_at, updated_at=excluded.updated_at`);
    for (const r of p.records) {
      const exists = existsStmt.get(r.date_jst);
      upsert.run({ ...r, ...meta });
      if (exists) updated++; else inserted++;
    }
  }

  db.prepare(`UPDATE raw_rakuten_data_import_log SET inserted=?, updated=? WHERE id=?`).run(inserted, updated, importId);
  return {
    file: p.name, ok: true, type: p.type, label: p.label,
    date_from: p.dateFrom, date_to: p.dateTo, rows: p.records.length, inserted, updated,
  };
}

function getLogInsertStmt(db) {
  return db.prepare(`INSERT INTO raw_rakuten_data_import_log
    (imported_at, source, file_name, file_sha256, file_type, date_from, date_to, row_count, inserted, updated, status, message)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
}

// ─── 入口: 物理ファイル1個 (zip or CSV) を1トランザクションで取込 ───
export function importDataFile(db, { name, buffer, sha256, source = 'incoming' }) {
  const importedAt = new Date().toISOString();
  const logStmt = getLogInsertStmt(db);
  const logOutcome = (fileName, fileType, status, message) => {
    try {
      logStmt.run(importedAt, source, fileName, sha256, fileType, null, null, 0, 0, 0, status, String(message || '').slice(0, 500));
    } catch (e) { console.error(`[rakuten-data-import] ログ記録失敗: ${e.message}`); }
  };

  const dup = db.prepare(`SELECT id FROM raw_rakuten_data_import_log WHERE file_sha256 = ? AND status = 'ok' LIMIT 1`).get(sha256);
  if (dup) {
    logOutcome(name, 'unknown', 'duplicate', `同一内容のファイルを取込済み (import log id=${dup.id})`);
    return { status: 'duplicate', results: [{ file: name, ok: false, duplicate: true, type: 'unknown', error: '取込済み (同一sha256)' }] };
  }

  const g = expandFile(name, buffer);
  if (g.error) {
    logOutcome(g.name, 'unknown', 'error', g.error);
    return { status: 'error', results: [{ file: g.name, ok: false, type: 'unknown', error: g.error }] };
  }

  const fileNames = g.files.map(f => f.name);
  const collected = [];
  const ctx = { importedAt, source, fileSha256: sha256 };
  const tx = db.transaction(() => {
    for (const f of g.files) {
      const p = prepareDataFile(f.name, f.buffer);
      if (!p.ok) throw new GroupAbortError(p);
      collected.push(commitOne(db, logStmt, p, ctx));
    }
  });
  try {
    tx();
    return { status: 'ok', results: collected };
  } catch (e) {
    if (e instanceof GroupAbortError) {
      const p = e.prep;
      const results = [];
      logOutcome(p.name, p.type, p.skipped ? 'skipped' : 'error', p.error);
      results.push({ file: p.name, ok: false, skipped: !!p.skipped, type: p.type, error: p.error });
      const others = fileNames.filter(n => n !== p.name);
      for (const otherName of others) {
        const msg = '同じzip内の他ファイルにエラーがあるため取込を中止しました (修正後にzipごと再投入してください)';
        logOutcome(otherName, 'unknown', 'skipped', msg);
        results.push({ file: otherName, ok: false, skipped: true, type: 'unknown', error: msg });
      }
      return { status: p.skipped ? 'skipped' : 'error', results };
    }
    console.error(`[rakuten-data-import] group ${g.name}: ${e.stack || e.message}`);
    const results = fileNames.map(fileName => {
      logOutcome(fileName, 'unknown', 'error', e.message);
      return { file: fileName, ok: false, type: 'unknown', error: e.message };
    });
    return { status: 'error', results };
  }
}
