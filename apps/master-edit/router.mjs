/**
 * router.mjs — マスタ入力画面 (商品・セットを Company DB で直す。Company DB構想 14「マスタ入力画面」§2・§6 ⑤-1)
 *
 * 載せ方 (server.js): env MASTER_EDIT_ENABLED = 1 かつ Render (PORTAL_VARIANT = render) のときだけ
 *   app.use('/apps/master-edit', requireAppAccess('master-edit'), router)
 *   miniPC は同じ server.js を動かすが載せない (Company DB に人が書く口を 1 つに)
 * 見る = アプリの利用権がある人 / 書く = env MASTER_EDITORS の名簿のメールだけ (空 = 誰も書けない。admin でも名簿に無ければ不可。画面で隠すだけでなく API で止める)
 * 🚨 保存を開く = 切替の段階が new_open (ops.master_cutover_state・読めない = 閉) **かつ** 持ち主表 (config/master-ownership.mjs) の列が 'company'
 *    **かつ** env MASTER_EDIT_OPEN = 1 (Codex ⑤-R0 High 1・R1 H1)。どれかが欠ければ、名簿の人でも保存は 409「切替前」(lib/master-write.mjs)。
 *    MASTER_EDIT_ENABLED は画面を載せるだけ (見るだけ)
 *   GET  /                   一覧 (画面 A)。?q=&kind=&state=&missing=&diff=1&offset=
 *   GET  /manual             つかいかた
 *   GET  /sku/:code          1 つの商品 (単品 = 画面 B / セット = 画面 C)
 *   GET  /sku/:code/history  変更の記録
 *   GET  /api/lookup?code=   構成品の引き当て
 *   POST /api/sku/:code      保存 { request_id, reason?, seen: { token (編集の印), event_id? }, values: {...} }
 *   GET  /new?kind=single|set 新商品の登録 (画面 D・⑤-2a)
 *   GET  /api/code-check?code= 新しい商品コードを確かめる (形 + Company DB・NE・使ったことがあるか)
 *   POST /api/new            新商品の登録 { request_id, kind, code, reason?, values: {...}, card: {...} } (lib/master-register.mjs)。
 *                            保存が成功したら、同じ要求の中で product-hub のカードの取り込みを 1 回試す (うまくいかなくても登録は成功のまま)
 *   POST /api/sku/:code/card-retry  カードの取り込みをもう一度 (名簿の人・衝突 / 失敗の知らせも試す)
 *   POST /api/sku/:code/card-link   衝突を解く = 既存のカード (同じ商品コード) をこの商品に結ぶ (名簿の人。PR #1566 R1 M6)
 *   ── Amazon SKU の対応 (⑦-1・0054・Company DB構想 16。保存を開く門は上と同じ + 持ち主 listing_components.amazon が 'company') ──
 *   GET  /amazon/            一覧 (Company DB の対応・墓標も)。?q=&state=&offset=
 *   GET  /amazon/sku?sku=    1 つの seller SKU (見る・直す・墓標にする・墓標から戻す)。対応が無ければ「新しい対応」(今の構成は夜間ロードの写しとして見せる)
 *   GET  /amazon/sku/history?sku=  変更の記録 (対応・構成・出品)
 *   GET  /amazon/unmapped    未登録 = 直近 7 日に売れたのに構成が無い seller SKU (FBA / FBM で分ける・売上の公開が欠けた日は「未判定」)
 *   POST /api/amazon/save    保存 { request_id, seller_sku, name, components: [{ code, qty }], reason?, seen: { versions } } (lib/amazon-map-write.mjs)
 *   POST /api/amazon/delete  墓標にする { request_id, seller_sku, reason, seen: { versions } }
 *   NE 登録の CSV (⑤-2b・0053・lib/master-reg-csv.mjs。作る・配る・申告・使わない・実機の確かめは MASTER_DECISION_APPROVERS の名簿の人だけ = マスタの判断の CSV と同じ):
 *   GET  /reg-csv                   画面
 *   GET  /api/reg-csv/summary       今日の照合の回・形の確かめ・候補と止まる理由・ファイル
 *   POST /api/reg-csv/exports       作る { kind: products | sets, codes: [...], request_id }
 *   POST /api/reg-csv/exports/:id/issue     配る (built → issued)。この後に /file でダウンロード
 *   GET  /api/reg-csv/exports/:id/file      byte 列 (配った・申告したファイルだけ)
 *   POST /api/reg-csv/exports/:id/declare   取り込んだと申告 { sha256, result: ok | partial | rejected_all, ne_message?, imported_at?, note? }
 *   POST /api/reg-csv/exports/:id/supersede 使わない { reason, correction, confirm: true }
 *   POST /api/reg-csv/verified      実機で確かめた { kind, result: ok | ng, export_id?, note? }
 * Company DB に届かない = 画面は「つながらない (保存できない)」の帯・保存は 503 (何も書かない)。SQLite と NE には書かない
 * env: COMPANY_DB_MASTER_EDIT_URL = この画面だけのロール master_edit (scripts/company-db/create-master-edit-roles.mjs が作る。読む・この画面の書き込みだけ・
 *        切替の段階を進める関数・構成の依頼を上げる / 観測を書く権限は無い)。🚨 保存はこの接続だけ = 無ければ見るだけ (保存は 503・#1563 R1 M8)
 *      COMPANY_DB_URL = 表の持ち主のロール。COMPANY_DB_MASTER_EDIT_URL が無いときの読むだけの接続 (書き込みには使わない)
 */
import express from 'express';
import path from 'node:path';
import fs from 'node:fs';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { openPgClient, pgAdapter } from '../../scripts/company-db/migrate.mjs';
import { MASTER_OWNERSHIP, validateOwnership } from '../../config/master-ownership.mjs';
import { saveSku, MasterWriteError, MAX_COMPONENTS, fieldsOf, REG_CSV_FIELDS } from '../../lib/master-write.mjs';
import { registerNewSku, checkNewCodeInDb, KINDS_NEW, SET_PLAN_CHOICES, MAX_REFERENCE_URLS, NEW_ENTRY_KEYS } from '../../lib/master-register.mjs';
import { runCardOutbox, linkCardToExisting, CARD_STATUS_LABELS } from '../../lib/product-hub-outbox.mjs';
import { SET_DECISION_REASONS } from '../product-hub/lib/set-decision.js';
import { SHIPPING_METHOD_GROUPS } from '../product-hub/lib/shipping-groups.js';
import { listSkus, listCounts, readSkuPage, lookupSku, skuHistory, normalizeFilters, readNewPage, KINDS, MISSING, STATES, REG_STATES, CARD_FILTERS, ADV_KEYS, TAX_FILTERS, SALES_FILTERS, MULTI_MAX } from './read.mjs';
import { readBackorders, readBackorderLines, readWarehouseStock, stockOf, buildableOf } from './extras.mjs';
import { ui } from './ui-format.mjs';
import { readCutoverPhase, newEntryWritable, PHASE_LABELS } from '../../lib/master-cutover.mjs';
import { saveAmazonMap, deleteAmazonMap, sellerSkuIn, AMAZON_MAP_OWNER_KEY, MAP_STATES, MAX_MAP_COMPONENTS, MAX_MAP_QTY } from '../../lib/amazon-map-write.mjs';
import { listAmazonMaps, readAmazonPage, amazonHistory, amazonUnmapped, normalizeAmazonFilters, CHANNELS, UNMAPPED_DAYS } from './amazon-read.mjs';
import { normSku } from '../../lib/sku-norm.js';
import { approverGate } from '../master-decisions/router.mjs';
import {
  regSummary, buildRegExport, issueRegExport, regExportFile, declareRegExport, supersedeRegExport, recordRegVerified,
  REG_ITEM_STATES, REG_RESULTS,
} from '../../lib/master-reg-csv.mjs';

/** NE 登録の CSV の画面の言葉 */
const REG_EXPORT_STATES = Object.freeze({ built: '作った (まだ配っていない)', issued: '配った (取り込み待ち)', declared: '取り込んだと申告', closed: '閉じた' });
const REG_CLOSE_REASONS = Object.freeze({ superseded: '使わない', rejected_all: '全部だめ', finished: '全部の商品が終わった' });
const REG_CHECK_OUTCOMES = Object.freeze({ verified: 'NE で確かめた', partial: '違う列がある', failed: '取り込めなかった', waiting: '待ち', in_ne_undeclared: 'NE にある (申告がまだ)' });

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const view = (name) => path.join(__dirname, 'views', name);
const router = express.Router();

/**
 * 画面の見た目の部品 (新しいデザイン = 一覧・1 つの商品・つかいかた・誤り。CSS 1 つ + 画面の JS)。public/ の中だけを配る (読むだけ)。
 * 版 (assetV) = 中身のハッシュ = 配り直した日に古い CSS / JS が 1 時間残らない
 */
const PUBLIC_DIR = path.join(__dirname, 'public');
const assetV = (() => {
  const h = crypto.createHash('sha256');
  try { for (const f of fs.readdirSync(PUBLIC_DIR).sort()) h.update(f).update(fs.readFileSync(path.join(PUBLIC_DIR, f))); } catch { /* 無ければ空の版 */ }
  return h.digest('hex').slice(0, 12);
})();
router.use('/public', express.static(PUBLIC_DIR, { maxAge: '1h', index: false }));

/** Postgres の接続の作り方 (試験は PGlite に差し替える。本番では触らない) */
let pgClientFactory = openPgClient;
export function __setPgClientFactory(fn) { pgClientFactory = fn || openPgClient; }
/** 今の時刻 (試験は日を固定する)。本番の保存の「今日」は DB の now() (取引の初めの東京の日付) */
let clock = () => Date.now();
let clockOverridden = false;
export function __setClock(fn) { clock = fn || (() => Date.now()); clockOverridden = !!fn; }
/** 列の持ち主 (試験は 'company' にした表に差し替える。本番は config/master-ownership.mjs のまま) */
let ownershipOverride = null;
export function __setOwnership(o) { ownershipOverride = o ? validateOwnership(o) : null; }
const ownershipNow = () => ownershipOverride || MASTER_OWNERSHIP;
/** 保存を開いているか (env MASTER_EDIT_OPEN = 1。切替日に持ち主表と一緒に開ける。要求ごとに読む = 再起動なしで閉じられる) */
const isOpen = () => process.env.MASTER_EDIT_OPEN === '1';

/**
 * 送料の表 (送料コード → 配送方法・配送関係費合計)。Render の warehouse-mirror.db の mirror_shipping_rates (読むだけ)。
 * 読めない = null (送料の保存は 503・ほかの項目は保存できる)
 */
async function defaultShippingRates() {
  try {
    const { getMirrorDB } = await import('../warehouse-mirror/db.js');
    const rows = getMirrorDB().prepare('select shipping_code, 小分類区分名称 as method, 配送関係費合計 as cost from mirror_shipping_rates order by shipping_code').all();
    return rows.length ? new Map(rows.map((r) => [String(r.shipping_code), { method: r.method ?? null, cost: r.cost ?? null }])) : null;
  } catch {
    return null;
  }
}
let shippingRatesProvider = defaultShippingRates;
export function __setShippingRatesProvider(fn) { shippingRatesProvider = fn || defaultShippingRates; }

/**
 * FBA / FBM (seller SKU の正規化 → 'FBA' / 'FBM')。Render の warehouse-mirror.db の mirror_amazon_sku_fees.fulfillment_channel (読むだけ・16 §3 #9 = Company DB に列を作らない)。
 * 読めない = null (画面は「分からない」と出す・「未登録」は全部を出す)
 */
async function defaultAmazonChannels() {
  try {
    const { getMirrorDB } = await import('../warehouse-mirror/db.js');
    const rows = getMirrorDB().prepare('select seller_sku, fulfillment_channel from mirror_amazon_sku_fees').all();
    const out = new Map();
    for (const r of rows) {
      const ch = String(r.fulfillment_channel || '').trim().toUpperCase();
      if (ch === 'FBA' || ch === 'FBM') out.set(normSku(r.seller_sku), ch);
    }
    return out;
  } catch {
    return null;
  }
}
let amazonChannelsProvider = defaultAmazonChannels;
export function __setAmazonChannelsProvider(fn) { amazonChannelsProvider = fn || defaultAmazonChannels; }

/**
 * product-hub のカードの取り込み (SQLite)。本番 = apps/product-hub/services/cdb-card-intake.js (使うときに読む = この画面の試験は SQLite 無しで動く)。
 * 試験は差し替える
 */
let cardApplier = null;
export function __setCardApplier(fn) { cardApplier = fn || null; }
async function applyCard(ev) {
  if (cardApplier) return cardApplier(ev);
  const m = await import('../product-hub/services/cdb-card-intake.js');
  return m.applyCdbCardEvent(ev);
}
/** 衝突を解く (既存のカードに結ぶ)。本番 = cdb-card-intake.js の linkCdbCardToExisting。試験は差し替える */
let cardLinker = null;
export function __setCardLinker(fn) { cardLinker = fn || null; }
async function linkCard(ev, opts) {
  if (cardLinker) return cardLinker(ev, opts);
  const m = await import('../product-hub/services/cdb-card-intake.js');
  return m.linkCdbCardToExisting(ev, opts);
}
/** 知らせを 1 つ取り込む (保存の直後・「もう一度」)。誤りは投げない (画面に「カード作成待ち」を出す) */
async function tryCard(db, { eventId = null, skuId = null, manual = false }) {
  try {
    const r = await runCardOutbox(db, applyCard, { eventId, skuId, manual, limit: 1 });
    return r[0] || null;
  } catch (e) {
    console.error(`[master-edit] カードの取り込みの失敗: ${e && e.message}`);
    return { status: 'pending', error: String(e && e.message || e) };
  }
}

// ─── CSRF 二段ガード (マスタの判断と同じ): 書く API は Origin 必須で Host と一致・Content-Type は JSON ───
router.use('/api/', (req, res, next) => {
  if (['GET', 'HEAD', 'OPTIONS'].includes(req.method)) return next();
  const origin = req.headers.origin;
  let host = null;
  try { host = origin ? new URL(origin).host : null; } catch { /* 壊れた Origin は不一致 */ }
  if (!host || host !== req.headers.host) return res.status(403).json({ ok: false, error: 'origin_mismatch', message: 'ブラウザから操作してください (Origin ヘッダが必要です)' });
  if (!/^application\/json\b/i.test(String(req.headers['content-type'] || ''))) return res.status(415).json({ ok: false, error: 'Content-Type は application/json にしてください' });
  next();
});
router.use(express.json({ limit: '256kb' }));

/** 書ける人か。env MASTER_EDITORS にメールをカンマ区切りで。名簿がすべて (admin でも名簿に無ければ不可・空なら誰も書けない) */
export function editorGate(req) {
  const list = String(process.env.MASTER_EDITORS || '').split(',').map((s) => s.trim().toLowerCase()).filter(Boolean);
  if (!list.length) return { ok: false, message: '書ける人がまだ設定されていません (環境変数 MASTER_EDITORS)。設定されるまで誰も保存できません (見るのはできます)' };
  const email = String(req.session?.email || '').trim().toLowerCase();
  if (!email || !list.includes(email)) return { ok: false, message: '保存できるのは名簿の人だけです。見るのはできます' };
  return { ok: true, message: null };
}

/** 書き込み用の接続 (この画面だけのロール) が設定されているか */
const writeConfigured = () => !!process.env.COMPANY_DB_MASTER_EDIT_URL;
/** kind = 'read' (この画面のロール・無ければ持ち主のロールで読むだけ) / 'write' (この画面のロールだけ) */
async function connect(kind = 'read') {
  const url = kind === 'write' ? process.env.COMPANY_DB_MASTER_EDIT_URL : (process.env.COMPANY_DB_MASTER_EDIT_URL || process.env.COMPANY_DB_URL);
  if (!url && kind === 'write') return { error: '書き込み用の接続 (COMPANY_DB_MASTER_EDIT_URL・この画面だけのロール) が設定されていないので、保存できません (見るだけ)', reason: 'no_write_role' };
  if (!url) return { error: 'Company DB の接続先が設定されていません (COMPANY_DB_URL)。いまは見ることも保存もできません' };
  try {
    const client = await pgClientFactory(url, { application_name: 'master-edit' });
    if (client.on) client.on('error', (e) => console.error(`[master-edit] 接続のエラー: ${e.message}`));   // 切れた接続でプロセスを落とさない
    await client.query(`set statement_timeout = '20s'`);
    await client.query(`set lock_timeout = '10s'`);
    await client.query(`set idle_in_transaction_session_timeout = '60s'`);
    return { client };
  } catch (e) {
    console.error(`[master-edit] Company DB につながらない: ${e && e.message}`);
    return { error: 'Company DB につながりません。いまは保存できません (つながったら画面を開き直してください)' };
  }
}

/** 画面: つながらないときも画面は出す (帯で知らせる・保存のボタンは出さない) */
async function withPgPage(req, res, fn) {
  const c = await connect();
  try {
    await fn(c.client ? pgAdapter(c.client) : null, c.error || null);
  } catch (e) {
    console.error(`[master-edit] ${e && e.stack || e}`);
    if (!res.headersSent) res.status(500).render(view('error.ejs'), { ...pageLocals(req), ui2: true, message: 'サーバーエラーが発生しました' });
  } finally { if (c.client) { try { await c.client.end(); } catch { /* */ } } }
}
/** API: つながらない = 503 (書き込み用の接続が無い = no_write_role) */
async function withPgApi(res, fn, kind = 'read') {
  const c = await connect(kind);
  if (!c.client) return res.status(503).json({ ok: false, error: c.error, reason: c.reason || 'db_unreachable' });
  try {
    await fn(pgAdapter(c.client));
  } catch (e) {
    if (e instanceof MasterWriteError) return res.status(e.status).json({ ok: false, error: e.message, reason: e.reason, ...e.extra });
    if (e && e.code === '55P03') return res.status(409).json({ ok: false, error: 'ほかの処理 (夜間の処理・朝の照合・CSV など) が同じ商品を使っています。何も保存していません。少し待ってからもう一度', reason: 'locked' });
    if (e && (e.code === '40P01' || e.code === '40001')) return res.status(409).json({ ok: false, error: 'ほかの処理とぶつかりました。何も保存していません。もう一度保存してください', reason: 'retry' });
    console.error(`[master-edit] ${e && e.stack || e}`);
    if (!res.headersSent) res.status(500).json({ ok: false, error: 'サーバーエラーが発生しました (何も保存していません)' });
  } finally { try { await c.client.end(); } catch { /* */ } }
}

/** 画面の共通の値。phase = 切替の段階 (読めない・渡されない = 閉じている扱い)。closed = 保存を開いていない (画面の保存のボタンも出さない = #1563 R1 M5) */
const pageLocals = (req, phase = null) => {
  const gate = editorGate(req);
  const own = ownershipNow();
  const phaseText = phase && phase.readable ? PHASE_LABELS[phase.phase] : '読めない (閉じている扱い)';
  const why = !phase || !phase.readable || phase.phase !== 'new_open' ? `段階: ${phaseText}`
    : !newEntryWritable(phase, own) ? '持ち主表が切替のときの記録と違う'
      : !isOpen() ? '保存を開くスイッチ (MASTER_EDIT_OPEN) が入っていない'
        : Object.values(own).every((v) => v === 'load') ? '持ち主表の列が全部 NE・/register'
          : !writeConfigured() ? '書き込み用の接続 (COMPANY_DB_MASTER_EDIT_URL) が無い' : '';
  // Amazon SKU の対応 (⑦-1): 上の門 + 持ち主 listing_components.amazon が company
  const amazonWhy = why || (own[AMAZON_MAP_OWNER_KEY] !== 'company' ? 'Amazon SKU の対応の持ち主がまだ miniPC の SKU マスタ (listing_components.amazon = load)' : '');
  return {
    title: 'マスタの入力 (商品・セット)', username: req.session?.email || '', displayName: req.session?.displayName || '',
    canEdit: gate.ok, gateMessage: gate.message || '', base: req.baseUrl || '',
    open: isOpen(), phaseText,
    closed: !!why, closedWhy: why,
    amazonClosed: !!amazonWhy, amazonClosedWhy: amazonWhy,
    // 新しいデザインの画面 (ui2) だけが使う: 見せ方の道具・部品の版・左の列でいまどこか
    ui, assetV, ui2: false, nav: '', nowMs: clock(),
  };
};
const fmt = {
  yen: (v) => (v == null ? '' : Number(v).toLocaleString('ja-JP')),
  tax: (v) => (v == null ? '' : String(Math.round(Number(v) * 100))),
};

router.get('/', (req, res) => {
  // 画面の中のリンクは相対 (sku/… ・manual) = 末尾の / が無いと 1 つ上を指す
  if (!String(req.originalUrl || '').split('?')[0].endsWith('/')) return res.redirect(301, `${req.baseUrl}/`);
  return withPgPage(req, res, async (db, dbError) => {
    const filters = normalizeFilters(req.query);
    // 参考の値 (注文残 = 発注アプリ・在庫 = ロジザード)。読めなくても一覧は出す (その欄だけ「読めない」)
    const extras = { backorders: readBackorders(), stock: await readWarehouseStock({ now: clock() }) };
    const data = db ? await listSkus(db, filters, { now: new Date(clock()), extras }) : { rows: [], total: 0, offset: 0, limit: 0, filters, latestRun: null, diffAvailable: false, notFound: [], multiCut: false };
    const phase = db ? await readCutoverPhase(db) : null;
    const counts = db ? await listCounts(db, { now: new Date(clock()) }) : null;
    res.render(view('index.ejs'), { ...pageLocals(req, phase), ui2: true, nav: 'list', listPage: true, dbError, data, counts, filters, KINDS, MISSING, STATES, REG_STATES, CARD_FILTERS, ADV_KEYS, TAX_FILTERS, SALES_FILTERS, MULTI_MAX, extras, fmt });
  });
});
router.get('/manual', (req, res) => res.render(view('manual.ejs'), { ...pageLocals(req), ui2: true, nav: 'manual', MAX_COMPONENTS }));

// 新商品の登録 (画面 D)。つながらないときも画面は出す (帯・保存のボタンは出さない)
router.get('/new', (req, res) => withPgPage(req, res, async (db, dbError) => {
  const kind = Object.prototype.hasOwnProperty.call(KINDS_NEW, String(req.query.kind || '')) ? String(req.query.kind) : 'single';
  const page = db ? await readNewPage(db) : null;
  const shipping = await shippingRatesProvider();
  const locals = pageLocals(req, page ? page.phase : null);
  const own = ownershipNow();
  res.render(view('new.ejs'), {
    ...locals, dbError, kind, page, fmt, KINDS_NEW, MAX_COMPONENTS, MAX_REFERENCE_URLS,
    // 登録を開いているか = 段階 new_open・持ち主表のハッシュが段階の記録と同じ・MASTER_EDIT_OPEN・この種類で書く列の持ち主が全部 company・
    //   書き込み用の接続 (画面だけのロール)・backfill 済み (lib/master-register.mjs と同じ)
    entryClosed: !newEntryWritable(page ? page.phase : null, own) || !isOpen() || NEW_ENTRY_KEYS[kind].some((k) => own[k] !== 'company') || !writeConfigured() || !(page && page.backfillDone),
    entryWhy: locals.closedWhy
      || (NEW_ENTRY_KEYS[kind].some((k) => own[k] !== 'company') ? `${KINDS_NEW[kind]}で書く項目の持ち主がまだ NE・/register` : '')
      || (!(page && page.backfillDone) ? '切替の手順の「既存の商品の登録の状態 (backfill)」がまだ' : ''),
    shippingRates: shipping ? [...shipping.entries()].map(([code, r]) => ({ code, method: r.method, cost: r.cost })) : null,
    yahooDeliveries: Object.values(SHIPPING_METHOD_GROUPS), setPlanChoices: SET_PLAN_CHOICES, setDecisionReasons: SET_DECISION_REASONS,
  });
}));

router.get('/sku/:code', (req, res) => withPgPage(req, res, async (db, dbError) => {
  const now = new Date(clock());
  const page = db ? await readSkuPage(db, req.params.code, { now, ownership: ownershipNow(), open: isOpen() }) : null;
  if (db && !page) return res.status(404).render(view('error.ejs'), { ...pageLocals(req), ui2: true, nav: 'list', message: `商品コード ${req.params.code} は Company DB にありません` });
  const shipping = page ? await shippingRatesProvider() : null;
  // 参考の値: 注文残の内訳 (発注アプリ)・在庫 (ロジザード。セットは構成品から作れる数)
  const stock = page ? await readWarehouseStock({ now: clock() }) : null;
  const extras = page ? {
    backorder: page.cur.sku_kind === 'set' ? null : readBackorderLines(page.cur.code),
    stock,
    qty: page.cur.sku_kind === 'set' ? null : stockOf(stock, page.cur.code_norm),
    buildable: page.cur.sku_kind === 'set' ? buildableOf(stock, (page.cur.components || []).map((x) => ({ code_norm: normSku(x.code), qty: x.qty }))) : null,
  } : null;
  res.render(view('sku.ejs'), {
    ...pageLocals(req, page ? page.phase : null), ui2: true, nav: 'list', dbError, page, FIELD_DEFS: page ? fieldsOf(page.cur.sku_kind) : {}, REG_FIELDS: page ? (REG_CSV_FIELDS[page.cur.sku_kind] || []) : [], code: req.params.code, fmt, KINDS, STATES, REG_STATES, MAX_COMPONENTS, CARD_STATUS_LABELS, REG_ITEM_STATES,
    shippingRates: shipping ? [...shipping.entries()].map(([code, r]) => ({ code, method: r.method, cost: r.cost })) : null, extras,
  });
}));
router.get('/sku/:code/history', (req, res) => withPgPage(req, res, async (db, dbError) => {
  const h = db ? await skuHistory(db, req.params.code) : null;
  if (db && !h) return res.status(404).render(view('error.ejs'), { ...pageLocals(req), message: `商品コード ${req.params.code} は Company DB にありません` });
  res.render(view('history.ejs'), { ...pageLocals(req), dbError, h, code: req.params.code });
}));

router.get('/api/lookup', (req, res) => withPgApi(res, async (db) => {
  const code = String(req.query.code || '').trim();
  if (!code || code.length > 60) return res.status(400).json({ ok: false, error: 'コードを入れてください' });
  const item = await lookupSku(db, code, { now: new Date(clock()) });
  if (!item) return res.status(404).json({ ok: false, error: `${code} は Company DB にありません` });
  res.json({ ok: true, item });
}));

router.get('/api/code-check', (req, res) => withPgApi(res, async (db) => {
  const code = String(req.query.code ?? '');
  if (code.length > 60) return res.status(400).json({ ok: false, error: '長すぎます' });
  res.json(await checkNewCodeInDb(db, code));
}));

router.post('/api/new', (req, res) => {
  const gate = editorGate(req);
  if (!gate.ok) return res.status(403).json({ ok: false, error: gate.message, reason: 'not_editor' });
  const b = req.body || {};
  return withPgApi(res, async (db) => {
    const r = await registerNewSku(db, {
      actor: String(req.session.email).trim().toLowerCase(), requestId: b.request_id, kind: b.kind, code: b.code, reason: b.reason ?? null, values: b.values, card: b.card,
    }, { open: isOpen(), ownership: ownershipNow(), shippingRates: await shippingRatesProvider(), now: clockOverridden ? new Date(clock()) : undefined });
    // カードは保存の後で 1 回だけ試す (同じ取引ではない = 失敗しても登録はできている。ボードを開いたとき・「もう一度」で続きを)
    let card = r.card || null;
    if (card && card.status !== 'done') {
      const t = await tryCard(db, { eventId: card.event_id });
      if (t) card = { ...card, status: t.status, draft_id: t.result?.draft_id ?? null, error: t.error ?? null };
    }
    res.json({ ...r, card, card_label: card ? CARD_STATUS_LABELS[card.status] || card.status : null });
  }, 'write');
});

router.post('/api/sku/:code/card-retry', (req, res) => {
  const gate = editorGate(req);
  if (!gate.ok) return res.status(403).json({ ok: false, error: gate.message, reason: 'not_editor' });
  return withPgApi(res, async (db) => {
    const sku = (await db.query('select sku_id::text as id from core.skus where company_id = 1 and code_norm = core.norm_code($1)', [String(req.params.code || '')])).rows[0];
    if (!sku) return res.status(404).json({ ok: false, error: `商品コード ${req.params.code} は Company DB にありません` });
    const t = await tryCard(db, { skuId: sku.id, manual: true });
    if (!t) return res.status(409).json({ ok: false, error: 'もう一度試せる知らせがありません (作成済み・カードを作らない登録・ほかの処理が取り込み中)', reason: 'nothing_to_retry' });
    res.json({ ok: t.status === 'done', status: t.status, label: CARD_STATUS_LABELS[t.status] || t.status, error: t.error ?? null, draft_id: t.result?.draft_id ?? null });
  }, 'write');
});

router.post('/api/sku/:code/card-link', (req, res) => {
  const gate = editorGate(req);
  if (!gate.ok) return res.status(403).json({ ok: false, error: gate.message, reason: 'not_editor' });
  return withPgApi(res, async (db) => {
    const sku = (await db.query('select sku_id::text as id from core.skus where company_id = 1 and code_norm = core.norm_code($1)', [String(req.params.code || '')])).rows[0];
    if (!sku) return res.status(404).json({ ok: false, error: `商品コード ${req.params.code} は Company DB にありません` });
    const expected = req.body?.draft_id;
    if (expected == null || !/^\d{1,12}$/.test(String(expected))) return res.status(400).json({ ok: false, error: '結ぶカードの番号 (draft_id) が要る。画面を開き直してください', reason: 'invalid_input' });
    let r;
    try {
      r = await linkCardToExisting(db, linkCard, { skuId: sku.id, actor: String(req.session.email).trim().toLowerCase(), expectedDraftId: String(expected) });
    } catch (e) {
      if (e && e.code === 'CDB_CARD_INVALID') return res.status(409).json({ ok: false, error: `結べませんでした: ${e.message}`, reason: 'link_refused' });
      throw e;
    }
    if (r.ok) return res.json({ ok: true, draft_id: r.draft_id, already: !!r.already, label: CARD_STATUS_LABELS.done, applied: r.applied || [], not_applied: r.not_applied || [] });
    const msg = { no_event: 'この商品にはカードの知らせがありません', not_conflict: '衝突の知らせではありません (「カードをもう一度作る」を使ってください)', leased: 'ほかの処理がちょうど取り込み中です。少し待ってからもう一度',
      already_done: 'カードはもう作ってあります', hash_mismatch: '知らせの中身が壊れています',
      draft_mismatch: `衝突しているカードが画面を開いたときと違います (今は #${r.draft_id})。何も結んでいません。画面を開き直してください`,
      ambiguous: `同じ商品コードのカードが product-hub に ${(r.draft_ids || []).length} 枚 (${(r.draft_ids || []).map((x) => '#' + x).join('・')}) あります。どれに結ぶか決められないので結んでいません。product-hub で 1 枚に片付けてから「カードをもう一度作る」` }[r.reason] || r.reason;
    res.status(409).json({ ok: false, error: msg, reason: r.reason, draft_id: r.draft_id ?? null, draft_ids: r.draft_ids ?? null });
  }, 'write');
});

// ─── Amazon SKU の対応 (⑦-1) ───
router.get('/amazon', (req, res) => {
  // 画面の中のリンクは相対 = 末尾の / が無いと 1 つ上を指す
  if (!String(req.originalUrl || '').split('?')[0].endsWith('/')) return res.redirect(301, `${req.baseUrl}/amazon/`);
  return withPgPage(req, res, async (db, dbError) => {
    const filters = normalizeAmazonFilters(req.query);
    const channels = db ? await amazonChannelsProvider() : null;
    const data = db ? await listAmazonMaps(db, filters, { channels }) : { rows: [], total: 0, offset: 0, limit: 0, filters, tableMissing: false };
    const phase = db ? await readCutoverPhase(db) : null;
    res.render(view('amazon-index.ejs'), { ...pageLocals(req, phase), dbError, data, filters, MAP_STATES, CHANNELS });
  });
});
router.get('/amazon/unmapped', (req, res) => withPgPage(req, res, async (db, dbError) => {
  const want = String(req.query.channel ?? 'FBA');
  const channel = ['FBA', 'FBM'].includes(want) ? want : '';
  const channels = db ? await amazonChannelsProvider() : null;
  const data = db ? await amazonUnmapped(db, { now: new Date(clock()), channel, channels }) : null;
  const phase = db ? await readCutoverPhase(db) : null;
  res.render(view('amazon-unmapped.ejs'), { ...pageLocals(req, phase), dbError, data, channel, CHANNELS, UNMAPPED_DAYS, MAP_STATES });
}));
router.get('/amazon/sku/history', (req, res) => withPgPage(req, res, async (db, dbError) => {
  let sku;
  try { sku = sellerSkuIn(String(req.query.sku ?? '')); } catch (e) { return res.status(400).render(view('error.ejs'), { ...pageLocals(req), message: e.message }); }
  const h = db ? await amazonHistory(db, sku) : null;
  if (db && !h) return res.status(404).render(view('error.ejs'), { ...pageLocals(req), message: `seller SKU ${sku} の出品は Company DB にありません` });
  res.render(view('amazon-history.ejs'), { ...pageLocals(req), dbError, h, sku, MAP_STATES });
}));
router.get('/amazon/sku', (req, res) => withPgPage(req, res, async (db, dbError) => {
  let sku;
  try { sku = sellerSkuIn(String(req.query.sku ?? '')); } catch (e) { return res.status(400).render(view('error.ejs'), { ...pageLocals(req), message: e.message }); }
  const channels = db ? await amazonChannelsProvider() : null;
  const page = db ? await readAmazonPage(db, sku, { channels }) : null;
  res.render(view('amazon-sku.ejs'), { ...pageLocals(req, page ? page.phase : null), dbError, page, sku, MAP_STATES, CHANNELS, MAX_MAP_COMPONENTS, MAX_MAP_QTY });
}));
router.post('/api/amazon/save', (req, res) => {
  const gate = editorGate(req);
  if (!gate.ok) return res.status(403).json({ ok: false, error: gate.message, reason: 'not_editor' });
  const b = req.body || {};
  return withPgApi(res, async (db) => {
    const r = await saveAmazonMap(db, {
      actor: String(req.session.email).trim().toLowerCase(), requestId: b.request_id, sellerSku: b.seller_sku, name: b.name, components: b.components, reason: b.reason ?? null, seen: b.seen,
    }, { open: isOpen(), ownership: ownershipNow() });
    res.json(r);
  }, 'write');
});
router.post('/api/amazon/delete', (req, res) => {
  const gate = editorGate(req);
  if (!gate.ok) return res.status(403).json({ ok: false, error: gate.message, reason: 'not_editor' });
  const b = req.body || {};
  return withPgApi(res, async (db) => {
    const r = await deleteAmazonMap(db, {
      actor: String(req.session.email).trim().toLowerCase(), requestId: b.request_id, sellerSku: b.seller_sku, reason: b.reason ?? null, seen: b.seen,
    }, { open: isOpen(), ownership: ownershipNow() });
    res.json(r);
  }, 'write');
});

// ─── NE 登録の CSV (⑤-2b) ───
router.get('/reg-csv', (req, res) => withPgPage(req, res, async (db, dbError) => {
  const summary = db ? await regSummary(db, { nowMs: clock() }) : null;
  const phase = db ? await readCutoverPhase(db) : null;
  const gate = approverGate(req);
  res.render(view('reg-csv.ejs'), {
    ...pageLocals(req, phase), dbError, summary, canApprove: gate.ok, approveMessage: gate.message || '',
    REG_STATES, ITEM_STATES: REG_ITEM_STATES, RESULTS: REG_RESULTS, EXPORT_STATES: REG_EXPORT_STATES, CLOSE_REASONS: REG_CLOSE_REASONS, CHECK_OUTCOMES: REG_CHECK_OUTCOMES,
  });
}));
router.get('/api/reg-csv/summary', (req, res) => withPgApi(res, async (db) => res.json({ ok: true, ...(await regSummary(db, { nowMs: clock() })) })));
/** NE 登録の CSV の書き込み = 名簿 (MASTER_DECISION_APPROVERS) の人だけ・この画面だけのロールで */
function regWrite(req, res, fn) {
  const gate = approverGate(req);
  if (!gate.ok) return res.status(403).json({ ok: false, error: gate.message, reason: 'not_approver' });
  const actor = String(req.session.email).trim().toLowerCase();
  const opts = { open: isOpen(), ownership: ownershipNow(), nowMs: clock() };
  return withPgApi(res, async (db) => res.json({ ok: true, ...(await fn(db, actor, req.body || {}, opts)) }), 'write');
}
router.post('/api/reg-csv/exports', (req, res) => regWrite(req, res, (db, actor, b, o) => buildRegExport(db, { actor, kind: b.kind, codes: b.codes, requestId: b.request_id }, o)));
router.post('/api/reg-csv/exports/:id/issue', (req, res) => regWrite(req, res, (db, actor, b, o) => issueRegExport(db, { actor, exportId: req.params.id, requestId: b.request_id }, o)));
router.post('/api/reg-csv/exports/:id/declare', (req, res) => regWrite(req, res, (db, actor, b, o) => declareRegExport(db, {
  actor, exportId: req.params.id, sha256: b.sha256, result: b.result, neMessage: b.ne_message ?? null, importedAt: b.imported_at ?? null, note: b.note ?? null, requestId: b.request_id,
}, o)));
router.post('/api/reg-csv/exports/:id/supersede', (req, res) => regWrite(req, res, (db, actor, b, o) => supersedeRegExport(db, {
  actor, exportId: req.params.id, reason: b.reason, correction: b.correction, confirm: b.confirm === true, requestId: b.request_id,
}, o)));
router.post('/api/reg-csv/verified', (req, res) => regWrite(req, res, (db, actor, b, o) => recordRegVerified(db, { actor, kind: b.kind, result: b.result, note: b.note ?? null, exportId: b.export_id ?? null, requestId: b.request_id }, o)));
router.get('/api/reg-csv/exports/:id/file', (req, res) => {
  const gate = approverGate(req);
  if (!gate.ok) return res.status(403).json({ ok: false, error: gate.message, reason: 'not_approver' });
  return withPgApi(res, async (db) => {
    const f = await regExportFile(db, req.params.id);
    if (!f) return res.status(404).json({ ok: false, error: 'ファイルがありません' });
    if (!f.bytes) return res.status(409).json({ ok: false, error: f.state === 'built' ? '先に「配る」を押してください' : '閉じたファイルは配りません', reason: f.state });
    res.set('Content-Type', 'text/csv; charset=utf-8');
    res.set('Content-Disposition', `attachment; filename="${f.file_name}"`);
    res.set('X-Content-SHA256', f.sha256);
    res.send(f.bytes);
  });
});

router.post('/api/sku/:code', (req, res) => {
  const gate = editorGate(req);
  if (!gate.ok) return res.status(403).json({ ok: false, error: gate.message, reason: 'not_editor' });
  const b = req.body || {};
  return withPgApi(res, async (db) => {
    const shippingRates = b.values && Object.prototype.hasOwnProperty.call(b.values, 'shipping_code') ? await shippingRatesProvider() : null;
    const r = await saveSku(db, {
      actor: String(req.session.email).trim().toLowerCase(), requestId: b.request_id, code: req.params.code, reason: b.reason ?? null, seen: b.seen, values: b.values,
    }, { open: isOpen(), ownership: ownershipNow(), shippingRates, now: clockOverridden ? new Date(clock()) : undefined });
    res.json(r);
  }, 'write');
});

export default router;
