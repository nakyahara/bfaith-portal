/**
 * amazon-unlinked.mjs — マスタ入力画面の「Amazon の出品一覧」を読むもの (2026-10-12 中原さん):
 *   ① seller SKU の画面に「Amazon の出品一覧では」(Amazon の商品名・ASIN・出品日・FBA / 自社・状態) を出す (readAmazonReportFor)
 *   ② 新しい入口「まだつないでいない Amazon の SKU」= Amazon の出品一覧にあるのに、Company DB の出品に構成 (NE の商品 × 数) が無い **FBA の** seller SKU (listUnlinked)。
 *      🚨 自社発送 (FBM) は出さない・数えない (中原さん 10/12「自社発送はそもそも 商品コード = Amazon SKU だから不要」)。区分の切り替えも無い (URL の ch は捨てる)。
 *      区分が分からない行 (列が無い・知らない値) も FBA と言い切れないので出さない。① の seller SKU の画面は自社発送も出す
 *   ③ いらない SKU (バリエーションの親・使っていない古い SKU) を一覧から「隠す」/ 元に戻す (setHidden = ops.set_amazon_sku_hidden。表示だけ・Amazon にも対応にも触らない)
 *
 * 読み元 = Company DB の ops.amazon_listing_report (migration 0074)。毎朝の daily-sync の「Company DB 出品一覧 (Amazon)」が、
 *   前の夜に想定利益の夜間 (miniPC 23:30) が取った出品レポート (GET_MERCHANT_LISTINGS_ALL_DATA) を送る = 画面の値は「前の夜 23:30 ごろの Amazon」。取得の時刻を必ず出す。
 * 「無い」と「読めない」を混ぜない:
 *   - 読めない (表が無い = 0074 の前・画面のロールに権限が無い・まだ 1 回も届いていない・DB の誤り) = state 'unreadable' + 理由 (画面は「読めない」と出す)
 *   - 届いた出品一覧にその SKU が無い = state 'absent' (seller SKU の画面は何も出さない)
 *   - 取得が古い (36 時間より前) = stale (値は出すが「古い」の札)
 * 「つないでいる」= Company DB の Amazon (日本) の出品 (core.listings・正規化した seller SKU) に構成 (core.listing_components) が 1 行以上ある。
 *   切替の前は対応の表 (core.amazon_sku_maps) が空でも、夜間ロードが SKU マスタから入れた構成があれば「つないでいる」= 今の「未登録」(amazon-read.mjs) と同じ決まり。
 *   削除済み (墓標) の対応 = 構成 0 行 = 「つないでいない」に出す (札「削除済み (墓標)」)
 * 権限 (scripts/company-db/create-master-edit-roles.mjs): 画面のロール master_edit に 3 つの表の select と、最後に売れた日 (ops.amazon_sku_last_sold)・隠す (ops.set_amazon_sku_hidden) の実行
 */
import { normSku } from '../../lib/sku-norm.js';
import { COMPANY_ID, jstDate, MasterWriteError } from '../../lib/master-write.mjs';
import { sellerSkuIn, AMAZON_JP_SHOP_CODE } from '../../lib/amazon-map-write.mjs';
import { foldSearch } from './search-fold.mjs';
import { readFbaDay, FBA_SOURCE } from './fba-stock.mjs';
import { buildUnlinkedCsv, unlinkedCsvFileName } from './list-csv.mjs';

export const UNLINKED_PAGE = 100;
export const SOLD_LOOKBACK_DAYS = 365;
export const REPORT_STALE_MS = 36 * 3600e3;
export const HIDE_REASONS = Object.freeze({ variation_parent: 'バリエーションの親', unused: '使っていない・古い SKU', other: 'その他' });
export const LISTING_STATUS_WORDS = Object.freeze({ active: '出品中', inactive: '停止中', incomplete: '情報が足りない', unknown: '分からない' });
export const SORTS = Object.freeze({ open_desc: '出品日の新しい順', open_asc: '出品日の古い順', sold_desc: '最後に売れた日の新しい順', sku: 'seller SKU の順' });
export const NOTE_MAX = 200;
const TS = (col) => `to_char((${col}) at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')`;
const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
const realDate = (v) => typeof v === 'string' && DATE_RE.test(v) && new Date(`${v}T00:00:00Z`).toISOString().slice(0, 10) === v;
const addDays = (d, n) => new Date(Date.parse(`${d}T00:00:00Z`) + n * 864e5).toISOString().slice(0, 10);
const errCodeOf = (e) => (e && typeof e.code === 'string' ? e.code : '');

/** 画面から開く seller SKU (保存と同じ形 = 前後の空白を除いて小文字)。形が合わない (制御文字など) = null = リンクを出さない */
export function openSkuOf(sellerSku) {
  try { return sellerSkuIn(String(sellerSku ?? '')); } catch { return null; }
}

/**
 * 今の出品一覧の見出し。{ state: 'ok', load: { load_id, snapshot_at, row_count, skipped_count, received_at }, stale } /
 *   { state: 'unreadable', reason: 'not_migrated' | 'no_privilege' | 'empty' | 'error', message }
 */
export async function readReportLoad(db, { nowMs = Date.now() } = {}) {
  if (!db) return { state: 'unreadable', reason: 'error', message: 'Company DB につながらない' };
  try {
    const t = (await db.query(`select to_regclass('ops.amazon_listing_report') is not null and to_regclass('ops.amazon_listing_report_loads') is not null as ok`)).rows[0];
    if (!t || !t.ok) return { state: 'unreadable', reason: 'not_migrated', message: 'Company DB に Amazon の出品一覧の表がまだありません (migration 0074 の前)' };
    const p = (await db.query(`select has_table_privilege('ops.amazon_listing_report', 'select') and has_table_privilege('ops.amazon_listing_report_loads', 'select') as ok`)).rows[0];
    if (!p || !p.ok) return { state: 'unreadable', reason: 'no_privilege', message: '画面のロールに Amazon の出品一覧を読む権限がまだありません (create-master-edit-roles.mjs の流し直しが要る)' };
    const x = (await db.query(`select load_id::text as load_id, ${TS('snapshot_at')} as snapshot_at, row_count, skipped_count, ${TS('received_at')} as received_at, ${TS('coalesce(refreshed_at, received_at)')} as arrived_at
        from ops.amazon_listing_report_loads where company_id = $1 and shop_code = $2 order by generation desc limit 1`, [COMPANY_ID, AMAZON_JP_SHOP_CODE])).rows[0];
    if (!x) return { state: 'unreadable', reason: 'empty', message: 'Amazon の出品一覧がまだ 1 回も届いていません (毎朝の daily-sync の「Company DB 出品一覧 (Amazon)」の後に出ます)' };
    const t2 = Date.parse(x.snapshot_at);
    return { state: 'ok', load: { ...x, row_count: Number(x.row_count), skipped_count: Number(x.skipped_count) }, stale: !Number.isFinite(t2) || nowMs - t2 > REPORT_STALE_MS };
  } catch (e) {
    console.error(`[master-edit] Amazon の出品一覧を読めない: ${e && e.message}`);
    return { state: 'unreadable', reason: errCodeOf(e) === '42501' ? 'no_privilege' : 'error', message: errCodeOf(e) === '42501' ? '画面のロールに Amazon の出品一覧を読む権限がまだありません' : 'Amazon の出品一覧を読めません' };
  }
}

const REPORT_COLS = `r.seller_sku, r.sku_norm, r.item_name, r.asin, r.open_date::text as open_date, r.open_date_raw, r.fulfillment, r.channel_raw, r.status, r.status_raw,
  r.quantity, r.price_jpy::text as price`;

/**
 * seller SKU の画面の「Amazon の出品一覧では」。{ state: 'ok', load, stale, rows: [行 (正規化で同じ SKU が 2 つあれば両方)] } / { state: 'absent', load, stale } / { state: 'unreadable', reason, message }
 */
export async function readAmazonReportFor(db, sellerSku, { nowMs = Date.now() } = {}) {
  const head = await readReportLoad(db, { nowMs });
  if (head.state !== 'ok') return head;
  try {
    const rows = (await db.query(`select ${REPORT_COLS} from ops.amazon_listing_report r
       where r.company_id = $1 and r.shop_code = $2 and r.load_id = $3::bigint and r.sku_norm = core.norm_code($4) order by r.seller_sku limit 5`,
    [COMPANY_ID, AMAZON_JP_SHOP_CODE, head.load.load_id, String(sellerSku ?? '')])).rows;
    return rows.length ? { state: 'ok', load: head.load, stale: head.stale, rows: rows.map(rowOut) } : { state: 'absent', load: head.load, stale: head.stale };
  } catch (e) {
    console.error(`[master-edit] Amazon の出品一覧の行を読めない: ${e && e.message}`);
    return { state: 'unreadable', reason: 'error', message: 'Amazon の出品一覧を読めません' };
  }
}
const rowOut = (r) => ({ ...r, quantity: r.quantity == null ? null : Number(r.quantity) });

/** 一覧の絞り込み (知らない値は捨てる・状態は全部・出品日の新しい順が既定)。区分 (ch) は無い = いつも FBA だけ */
export function normalizeUnlinkedFilters(q = {}, { today = null } = {}) {
  const str = (v) => (typeof v === 'string' ? v : '');
  const st = ['all', 'active', 'inactive'].includes(str(q.st)) ? str(q.st) : 'all';
  const sort = Object.hasOwn(SORTS, str(q.sort)) ? str(q.sort) : 'open_desc';
  let from = realDate(str(q.from)) ? str(q.from) : '';
  let to = realDate(str(q.to)) ? str(q.to) : '';
  // 「直近 N 日」の札 (days) は from に直す (today が要る)
  const days = Number.parseInt(str(q.days), 10);
  if (!from && !to && today && [30, 90, 365].includes(days)) from = addDays(today, -(days - 1));
  if (from && to && from > to) [from, to] = [to, from];
  const offset = Math.max(0, Math.min(1e6, Number.parseInt(str(q.offset), 10) || 0));
  return { q: str(q.q).trim().slice(0, 100), st, sort, from, to, hidden: str(q.hidden) === '1' ? '1' : '', offset };
}

const cmpStr = (a, b) => (a < b ? -1 : a > b ? 1 : 0);
/** 並べ方 (値の無い行 = 出品日・売れた日が無い = 後ろ)。同じなら seller SKU の順 */
export function sortUnlinked(rows, sort) {
  const key = sort === 'sold_desc' ? 'last_sold' : 'open_date';
  const dir = sort === 'open_asc' ? 1 : -1;
  return rows.sort((a, b) => {
    if (sort !== 'sku') {
      const x = a[key], y = b[key];
      if (x && !y) return -1;
      if (!x && y) return 1;
      if (x && y && x !== y) return dir * cmpStr(x, y);
    }
    return cmpStr(a.seller_sku, b.seller_sku);
  });
}

/**
 * 「まだつないでいない Amazon の SKU」。filters = normalizeUnlinkedFilters の形。
 * 戻り値 = { state: 'ok', load, stale, rows (このページ), total (絞り込みの後), offset, limit, filters, counts: { report (FBA の行), linked, unlinked, hidden }, sold: { ok, since }, fba: readFbaDay の答え }
 *   / { state: 'unreadable', reason, message, filters }
 *   all = true (CSV・10/17): ページに分けず、絞った全部の行 (FBA の在庫も全部の行の分)
 */
export async function listUnlinked(db, filters, { nowMs = Date.now(), all: wantAll = false } = {}) {
  const head = await readReportLoad(db, { nowMs });
  if (head.state !== 'ok') return { ...head, filters };
  const today = jstDate(new Date(nowMs));
  const all = (await db.query(`select ${REPORT_COLS}, l.listing_id::text as listing_id, m.state as map_state,
        h.reason as hidden_reason, h.note as hidden_note, h.hidden_by, ${TS('h.hidden_at')} as hidden_at
      from ops.amazon_listing_report r
      left join core.listings l on l.company_id = r.company_id and l.mall = 'amazon' and l.shop_code = r.shop_code and l.listing_norm = r.sku_norm
      left join core.amazon_sku_maps m on m.listing_id = l.listing_id
      left join ops.amazon_sku_hidden h on h.company_id = r.company_id and h.shop_code = r.shop_code and h.sku_norm = r.sku_norm
     where r.company_id = $1 and r.shop_code = $2 and r.load_id = $3::bigint and r.fulfillment = 'FBA'
       and not exists (select 1 from core.listing_components c where c.listing_id = l.listing_id)`,
  [COMPANY_ID, AMAZON_JP_SHOP_CODE, head.load.load_id])).rows.map(rowOut);
  const hiddenCount = all.filter((r) => r.hidden_reason).length;
  // 数も FBA だけ (自社発送は出品一覧の件数にも入れない)
  const fbaRows = Number((await db.query(`select count(*)::int as n from ops.amazon_listing_report where company_id = $1 and shop_code = $2 and load_id = $3::bigint and fulfillment = 'FBA'`, [COMPANY_ID, AMAZON_JP_SHOP_CODE, head.load.load_id])).rows[0].n);
  const counts = { report: fbaRows, unlinked: all.length - hiddenCount, hidden: hiddenCount, linked: Math.max(0, fbaRows - all.length) };
  const base = all.filter((r) => (filters.hidden ? !!r.hidden_reason : !r.hidden_reason));
  const skuQ = filters.q ? normSku(filters.q) : '';
  const nameQ = filters.q ? foldSearch(filters.q) : '';
  const asinQ = filters.q ? filters.q.toUpperCase() : '';
  let rows = base.filter((r) => {
    if (filters.st === 'active' && r.status !== 'active') return false;
    if (filters.st === 'inactive' && r.status === 'active') return false;
    if ((filters.from || filters.to) && !r.open_date) return false;
    if (filters.from && r.open_date < filters.from) return false;
    if (filters.to && r.open_date > filters.to) return false;
    if (filters.q && !((skuQ && r.sku_norm.includes(skuQ)) || (nameQ && r.item_name && foldSearch(r.item_name).includes(nameQ)) || (r.asin && r.asin.includes(asinQ)))) return false;
    return true;
  });
  // 最後に売れた日 (絞り込んだ行の分だけ・1 回)。読めない = 列に「読めない」(0 件・売れていないとは言わない)
  const since = addDays(today, -(SOLD_LOOKBACK_DAYS - 1));
  const sold = { ok: false, since };
  if (rows.length) {
    try {
      const norms = [...new Set(rows.map((r) => r.sku_norm))];
      const m = new Map((await db.query('select sku_norm, last_date::text as last_date, units::text as units from ops.amazon_sku_last_sold($1::text[], $2::date)', [norms, since])).rows.map((x) => [x.sku_norm, x]));
      for (const r of rows) { const s = m.get(r.sku_norm); r.last_sold = s ? s.last_date : null; r.sold_units = s ? Number(s.units) : 0; }
      sold.ok = true;
    } catch (e) {
      console.error(`[master-edit] 最後に売れた日を読めない: ${e && e.message}`);
      sold.error = errCodeOf(e) === '42501' ? '画面のロールに最後に売れた日を読む権限がまだありません' : '最後に売れた日を読めません';
    }
  } else sold.ok = true;
  if (!sold.ok && filters.sort === 'sold_desc') filters = { ...filters, sort: 'open_desc' };
  sortUnlinked(rows, filters.sort);
  const total = rows.length;
  const page = wantAll ? rows : rows.slice(filters.offset, filters.offset + UNLINKED_PAGE);
  // FBA の在庫 (販売可能・このページの FBA の行だけ)。出品 SKU (source_code) の正規化で結ぶ = 構成が無くても数は出る
  const fba = await readFbaDay(db, { now: nowMs });
  if (fba.ok) {
    const norms = [...new Set(page.filter((r) => r.fulfillment === 'FBA').map((r) => r.sku_norm))];
    if (norms.length) {
      try {
        const m = new Map((await db.query(`select core.norm_code(source_code) as n, sum(fba_available)::bigint::text as avail, count(*)::int as rows
            from snapshots.sku_stock_daily where company_id = $1 and source = $2 and scope_key = $3 and snapshot_date = $4::date and core.norm_code(source_code) = any($5::text[]) group by 1`,
        [COMPANY_ID, FBA_SOURCE.source, FBA_SOURCE.scope, fba.date, norms])).rows.map((x) => [x.n, x]));
        for (const r of page) if (r.fulfillment === 'FBA') { const s = m.get(r.sku_norm); r.fba_available = s && s.avail != null ? Number(s.avail) : null; r.fba_row = !!s; }
      } catch (e) {
        console.error(`[master-edit] FBA の在庫を読めない (まだつないでいない SKU): ${e && e.message}`);
        fba.ok = false; fba.error = 'FBA の在庫を読めません';
      }
    }
  }
  for (const r of page) r.open_sku = openSkuOf(r.seller_sku);
  return { state: 'ok', load: head.load, stale: head.stale, today, rows: page, total, offset: filters.offset, limit: UNLINKED_PAGE, filters, counts, sold, fba };
}

/** 隠す / 元に戻す の入力の検査 (DB の関数も同じことを確かめる)。外れたら MasterWriteError 400 */
export function hideInput(b = {}) {
  const sellerSku = typeof b.seller_sku === 'string' ? b.seller_sku : '';
  if (!sellerSku.trim() || sellerSku.length > 255) throw new MasterWriteError(400, 'invalid_value', 'seller SKU が無い・長すぎる');
  if (typeof b.hide !== 'boolean') throw new MasterWriteError(400, 'invalid_value', '隠すか戻すか (hide) が無い');
  const note = typeof b.note === 'string' && b.note.trim() ? b.note.trim() : null;
  if (note && note.length > NOTE_MAX) throw new MasterWriteError(400, 'invalid_value', `理由の文は ${NOTE_MAX} 文字まで`);
  if (!b.hide) return { sellerSku, hide: false, reason: null, note };
  const reason = typeof b.reason === 'string' && Object.hasOwn(HIDE_REASONS, b.reason) ? b.reason : null;
  if (!reason) throw new MasterWriteError(400, 'invalid_value', `理由を選んでください (${Object.values(HIDE_REASONS).join(' / ')})`);
  if (reason === 'other' && !note) throw new MasterWriteError(400, 'invalid_value', '「その他」は理由の文を書いてください');
  return { sellerSku, hide: true, reason, note };
}

/** 隠す / 元に戻す (ops.set_amazon_sku_hidden)。戻り値 = { ok: true, status: 'hidden' | 'shown' | 'same', seller_sku, reason, note, by, at } */
export async function setHidden(db, { actor, ...input }) {
  const v = hideInput(input);
  try {
    const r = (await db.query('select ops.set_amazon_sku_hidden($1, $2, $3, $4, $5) as r', [actor, v.sellerSku, v.hide, v.reason, v.note])).rows[0].r;
    const out = typeof r === 'string' ? JSON.parse(r) : r;
    return { ok: true, ...out };
  } catch (e) {
    const code = errCodeOf(e);
    const msg = String((e && e.message) || '').replace(/^(invalid_value|not_found|linked): /, '');
    if (code === 'P0001' && /^linked: /.test(String(e.message))) throw new MasterWriteError(409, 'linked', `${msg}。何もしていません (一覧を開き直してください)`);
    if (code === '22023') throw new MasterWriteError(400, 'invalid_value', msg);
    if (code === 'P0002') throw new MasterWriteError(404, 'not_in_report', `${msg} (毎朝の取り込みの後に、もう一度開いてください)`);
    if (code === '42501') throw new MasterWriteError(503, 'no_privilege', '画面のロールに「隠す」の権限がまだありません (create-master-edit-roles.mjs の流し直しが要る)。何もしていません');
    if (code === '42883' || code === '42P01') throw new MasterWriteError(503, 'not_migrated', 'Company DB に「隠す」の仕組みがまだありません (migration 0074 の前)。何もしていません');
    throw e;
  }
}

/**
 * 画面の道 (apps/master-edit/router.mjs から呼ぶ・router の中の部品を h で受け取る = router.mjs の変更を小さく)。
 *   GET  /amazon/unlinked               まだつないでいない Amazon の SKU (読むだけ)
 *   GET  /amazon/unlinked.csv           同じ絞り込みのまま全部を CSV で (10/17・読むだけ)
 *   POST /api/amazon/unlinked/hide      隠す / 元に戻す { seller_sku, hide, reason?, note? } (名簿 MASTER_EDITORS の人・この画面だけのロール)
 * h = { withPgPage, withPgApi, pageLocals, editorGate, view, readPhaseView, clock }
 */
export function registerAmazonUnlinkedRoutes(router, h) {
  router.get('/amazon/unlinked', (req, res) => h.withPgPage(req, res, async (db, dbError) => {
    const nowMs = h.clock();
    const filters = normalizeUnlinkedFilters(req.query, { today: jstDate(new Date(nowMs)) });
    const data = db ? await listUnlinked(db, filters, { nowMs }) : null;
    const phase = await h.readPhaseView(db);
    const locals = h.pageLocals(req, phase);
    res.render(h.view('amazon-unlinked.ejs'), {
      ...locals, nav: 'amazon', dbError, data, filters: data && data.filters ? data.filters : filters,
      canHide: locals.canEdit && !dbError, HIDE_REASONS, LISTING_STATUS_WORDS, SORTS, SOLD_LOOKBACK_DAYS, NOTE_MAX,
      // その場で登録 (10/17) = 保存できる人・保存を開いているときだけ (保存は POST /api/amazon/save = lib の門が確かめ直す)
      inline: locals.canEdit && !locals.amazonClosed && !dbError, ...(h.inlineLocals || {}),
    });
  }));
  // 🆕 10/17 中原さん「未登録一覧の CSV ダウンロード」: 今の絞り込み (?q=&st=&from=&to=&sort=&hidden=) のまま、ページに分けずに全部 (読むだけ・見られる人 = この画面と同じ)
  router.get('/amazon/unlinked.csv', (req, res) => h.withPgPage(req, res, async (db, dbError) => {
    const text = (status, msg) => res.status(status).type('text/plain; charset=utf-8').send(msg);
    if (!db) return text(503, dbError || 'Company DB につながりません');
    const nowMs = h.clock();
    const filters = normalizeUnlinkedFilters(req.query, { today: jstDate(new Date(nowMs)) });
    const data = await listUnlinked(db, { ...filters, offset: 0 }, { nowMs, all: true });
    if (data.state !== 'ok') return text(503, `${data.message}。CSV を出せません (0 件ではありません)`);
    const name = unlinkedCsvFileName(nowMs);
    res.set('Content-Type', 'text/csv; charset=utf-8');
    res.set('Content-Disposition', `attachment; filename="${name}"; filename*=UTF-8''${encodeURIComponent(name)}`);
    res.set('Cache-Control', 'no-store');
    res.send(buildUnlinkedCsv(data, { statusWords: LISTING_STATUS_WORDS, hideReasons: HIDE_REASONS }));
  }));
  router.post('/api/amazon/unlinked/hide', (req, res) => {
    const gate = h.editorGate(req);
    if (!gate.ok) return res.status(403).json({ ok: false, error: gate.message, reason: 'not_editor' });
    const b = req.body || {};
    return h.withPgApi(res, async (db) => {
      res.json(await setHidden(db, { actor: String(req.session.email).trim().toLowerCase(), seller_sku: b.seller_sku, hide: b.hide, reason: b.reason ?? null, note: b.note ?? null }));
    }, 'write');
  });
}
