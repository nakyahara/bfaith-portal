/**
 * amazon-map-write.mjs — Amazon SKU の対応 (seller SKU ↔ NE コード) を Company DB で直す (マスタ入力画面の「Amazon SKU」= apps/master-edit/amazon/*)
 * (Company DB構想 16「Amazon SKU の対応の編集」§2・§3・§7 v2 H5 / M7 / M10・§8 契約 v3。PR ⑦-1・0054)
 *
 * 保存 1 回 = 1 つの Postgres の取引で、専用の security definer の関数を 1 回呼ぶ (0052 の新商品の登録と同じ形):
 *   ops.save_amazon_sku_map   = 登録・直す・墓標から戻す (出品が無ければ作る → 対応 → 構成 → 保存の記録 done)
 *   ops.delete_amazon_sku_map = 墓標にする (1 件ずつ・理由が要る・構成を全部消す。行は消さない)
 *   画面のロール master_edit には表の書き込みを渡さない (関数の実行だけ)。DB が段階・持ち主表・版・構成品・seller SKU の形を確かめ直し、結果も DB が作る
 * 閉じている (⑤-1 と同じ門): 切替の段階が new_open **かつ** 持ち主表のハッシュが段階の記録と同じ **かつ** listing_components.amazon が 'company'
 *   **かつ** env MASTER_EDIT_OPEN = 1。どれかが欠ける = 409 before_cutover (ほかの鍵を取る前・何も書かない・失敗の記録は残す)
 * 鍵の順 (⑤-1 の saveSku と同じ): request_id の鍵 → 段階の共有の鍵 → (門) → マスタの書き込みの鍵 (共有・夜間ロード / 切替の日の移行の最中は 3 秒で 409 nightly_load) →
 *   関数の中で 出品ごとの鍵 (正規化したコード) → 出品と対応の行 → 版 → 構成品の SKU ごとの鍵 (sku_id の順) → 行
 * 編集の印 = 版 { listing: '出品:version', map: '出品:version:state' } (出品の version は構成・対応の変更でも上がる = 0026)。画面が読んだ版と DB の版が違う = 409
 * 同じ request_id = 残した結果・誤りを返す (中身が違う = 409)
 * 🚨 SQLite と Render には書かない (古い表への写しは ⑦-2)。前からの未解決の注文が結びつくのは次の夜の取り込みの後 (L12)
 */
import { normSku } from './sku-norm.js';
import { baseGateInTx, lockCutoverSharedInTx } from './master-owner-gate.mjs';
import { MasterWriteError, ownerGateError, textIn, intIn, stable, sha256, REQUEST_LOCK_SQL, lockMasterWriteShared, COMPANY_ID, REASON_MAX } from './master-write.mjs';
import { skuMapKeyProblem, SKU_MAP_EDGE_SPACE_CHARS } from './sku-map-canonical.js';

export const AMAZON_MAP_OWNER_KEY = 'listing_components.amazon';
export const AMAZON_MAP_SOURCE = 'portal_amazon_map';
/** Amazon (日本) の出品の店舗キー (sources.mjs の SHOP_CODES.amazon・0054 の core.amazon_jp_shop_code()) */
export const AMAZON_JP_SHOP_CODE = 'main@A1VC38T7YXB528';
export const MAX_MAP_COMPONENTS = 20;
export const MAX_MAP_QTY = 999;
export const MAX_MAP_NAME = 255;
/** 構成品にしてよい登録の状態 (NE 確認済み以降。0054 の ops.amazon_map_components と同じ) */
export const USABLE_REG_STATES = Object.freeze(['ne_confirmed', 'distributable', 'available']);
export const MAP_STATES = Object.freeze({ active: '有効', deleted: '削除済み (墓標)' });
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const CTRL_RE = new RegExp(`[\\x00-\\x1f\\x7f${String.fromCharCode(0x2028, 0x2029)}]`);
const VERSION_RE = /^\d{1,19}:\d{1,19}(:(active|deleted))?$/;
const EDGE = new Set(SKU_MAP_EDGE_SPACE_CHARS);
const bad = (message, field, reason = 'invalid_input', extra = {}) => new MasterWriteError(400, reason, message, { field, ...extra });

// ─── 入力の形 (DB を読まない。ここで落ちた保存は記録しない) ───

/** seller SKU を決まった形に (前後の空白を除いて小文字)。写しの受け手と同じ決まり (skuMapKeyProblem) に合わなければ投げる */
export function sellerSkuIn(raw, field = 'seller_sku') {
  if (typeof raw !== 'string') throw bad('seller SKU を入れてください', field);
  const s = raw.trim().toLowerCase();
  if (!s) throw bad('seller SKU を入れてください', field);
  const p = skuMapKeyProblem(s);
  if (p) throw bad(`seller SKU が使えない形です (${p})`, field);
  return s;
}
/** 名前が空 (空白だけ = 全角の空白だけも) か */
export const nameBlank = (s) => [...String(s ?? '')].every((ch) => EDGE.has(ch));
function nameIn(v) {
  const s = textIn(v, { label: '名前', field: 'name', max: MAX_MAP_NAME });
  if (s == null || nameBlank(s)) throw bad('名前を入れてください', 'name');
  if ([...s].some((ch, i, a) => (i === 0 || i === a.length - 1) && EDGE.has(ch))) throw bad('名前の前後に空白があります', 'name');
  return s;
}
function mapComponentsIn(v) {
  if (!Array.isArray(v)) throw bad('構成の入れ方が違う', 'components');
  if (v.length < 1 || v.length > MAX_MAP_COMPONENTS) throw bad(`構成品は 1〜${MAX_MAP_COMPONENTS} 行です`, 'components');
  const seen = new Set();
  return v.map((r, i) => {
    const code = textIn(r?.code, { label: `構成品 ${i + 1} 行目のコード`, field: 'components', max: 60 });
    if (!code) throw bad(`構成品 ${i + 1} 行目のコードが空です`, 'components');
    const qty = intIn(r?.qty, 1, MAX_MAP_QTY, `構成品 ${code} の数量`, 'components');
    const k = normSku(code);
    if (seen.has(k)) throw bad(`構成品 ${code} が 2 回あります (同じ品は 1 行にして数量で)`, 'components');
    seen.add(k);
    return { code, qty };
  });
}
function versionsIn(v) {
  if (!v || typeof v !== 'object' || Array.isArray(v)) throw bad('画面が読んだ「版」が無い。画面を開き直してください', 'seen');
  const out = {};
  for (const k of ['listing', 'map']) {
    const x = v[k] ?? null;
    if (x !== null && (typeof x !== 'string' || !VERSION_RE.test(x))) throw bad('画面が読んだ「版」の形が違う。画面を開き直してください', 'seen');
    out[k] = x;
  }
  return out;
}
function common(input) {
  const actor = String(input?.actor ?? '').trim().toLowerCase();
  if (!actor || actor.length > 320 || CTRL_RE.test(actor)) throw bad('保存する人 (ログインのメール) が分からない', 'actor');
  const requestId = String(input?.requestId ?? '').trim().toLowerCase();
  if (!UUID_RE.test(requestId)) throw bad('保存の番号 (request_id) の形が違う。画面を開き直してください', 'request_id');
  return { actor, requestId, sellerSku: sellerSkuIn(input?.sellerSku), versions: versionsIn(input?.seen?.versions) };
}

/** 保存 (登録・直す・墓標から戻す) の中身。input = { actor, requestId, sellerSku, name, components: [{ code, qty }], reason?, seen: { versions } } */
export function parseAmazonMapSave(input) {
  const c = common(input);
  return { ...c, op: 'save', name: nameIn(input?.name), components: mapComponentsIn(input?.components), reason: textIn(input?.reason, { label: '理由', field: 'reason', max: REASON_MAX }) };
}
/** 墓標にする中身。理由が要る。input = { actor, requestId, sellerSku, reason, seen: { versions } } */
export function parseAmazonMapDelete(input) {
  const c = common(input);
  const reason = textIn(input?.reason, { label: '理由', field: 'reason', max: REASON_MAX });
  if (!reason) throw bad('削除 (墓標にする) の理由を入れてください', 'reason');
  return { ...c, op: 'delete', reason };
}
/** 画面の要求のハッシュ (同じ request_id の押し直しを見分ける) */
export const amazonMapPayloadHashOf = (req) => sha256(stable({ op: req.op, seller_sku: req.sellerSku, name: req.name ?? null, components: req.components ?? null, reason: req.reason ?? null, versions: req.versions }));

// ─── 読む ───

const TS = (col) => `to_char((${col}) at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')`;

/** 版 (0054 の ops.amazon_map_versions と同じ形) */
export function amazonMapVersionsOf(cur) {
  return {
    listing: cur.listing ? `${cur.listing.listing_id}:${cur.listing.version}` : null,
    map: cur.map ? `${cur.map.listing_id}:${cur.map.version}:${cur.map.state}` : null,
  };
}

/**
 * 1 つの seller SKU の今の値 (出品・対応・構成・ASIN・FNSKU)。出品も対応も無ければ { listing: null, map: null, components: [] }。
 * 引くのは正規化したコード (出品の listing_norm)
 */
export async function readAmazonMap(db, sellerSku) {
  const listing = (await db.query(`select l.listing_id::text as listing_id, l.listing_code, l.title, l.status, l.version::text as version, ci.asin,
        ${TS('l.created_at')} as created_at
      from core.listings l left join core.catalog_items ci on ci.catalog_item_id = l.catalog_item_id
     where l.company_id = $1 and l.mall = 'amazon' and l.shop_code = $2 and l.listing_norm = core.norm_code($3)`, [COMPANY_ID, AMAZON_JP_SHOP_CODE, sellerSku])).rows[0] || null;
  const cur = { seller_sku: sellerSku, listing, map: null, components: [], fnsku: null };
  if (!listing) { cur.versions = amazonMapVersionsOf(cur); return cur; }
  cur.map = (await db.query(`select m.listing_id::text as listing_id, m.seller_sku, m.name, m.state, m.origin, m.version::text as version,
        ${TS('m.registered_at')} as registered_at, m.registered_by, ${TS('m.changed_at')} as changed_at, m.changed_by,
        ${TS('m.deleted_at')} as deleted_at, m.deleted_by, m.deleted_reason
      from core.amazon_sku_maps m where m.listing_id = $1`, [listing.listing_id])).rows[0] || null;
  cur.components = (await db.query(`select c.sku_id::text as sku_id, k.code, k.name, k.sku_kind, c.qty, c.sort_order, c.resolution, reg.state as reg_state,
        ${TS('c.created_at')} as created_at, ${TS('coalesce(c.updated_at, c.created_at)')} as updated_at
      from core.listing_components c join core.skus k on k.sku_id = c.sku_id left join ops.master_registrations reg on reg.sku_id = c.sku_id
     where c.listing_id = $1 order by c.sort_order, k.code_norm`, [listing.listing_id])).rows.map((r) => ({ ...r, qty: Number(r.qty), sort_order: Number(r.sort_order) }));
  cur.fnsku = (await db.query(`select external_value from core.external_ids
     where entity_type = 'listing' and entity_id = $1 and system = 'amazon' and id_kind = 'fnsku' and valid_to is null order by external_id_row desc limit 1`, [listing.listing_id])).rows[0]?.external_value ?? null;
  cur.versions = amazonMapVersionsOf(cur);
  return cur;
}

/**
 * Company DB の有効な対応 → 写しの決まった形の行 (lib/sku-map-canonical.js の { master, components })。⑦-2 の写しと切替の日の移行の照らしが使う。
 *   親 = seller_sku・名前・registered_at (created_at)・changed_at (updated_at) / 構成 = seller_sku・NE コード (SKU のコードの前後の空白を除いて ASCII だけ小文字)・
 *   数量・並び・created_at・updated_at (null = created_at)。時刻は UTC のミリ秒 (session の時間帯によらない・M6)。sellerSkus を渡すとその SKU だけ
 *   🆕 0064 (#1667 Codex R1 Low): 構成の NE コード = norm の内部の鍵 (miniPC の raw_ne_products.商品コード = NE の取得の小文字と結ぶ・m_sku_components の CHECK も小文字だけ)。
 *   大文字の新しいコードも小文字で写す (原文は Company DB の core.skus.code と 0041 にある。NE のコードとして外へ出す値ではない)
 */
export async function readCompanyAmazonMapCanon(db, { sellerSkus = null } = {}) {
  const only = sellerSkus ? 'and m.seller_sku = any($1::text[])' : '';
  const params = sellerSkus ? [sellerSkus] : [];
  const master = (await db.query(`select m.seller_sku, m.name, ${TS('m.registered_at')} as created_at, ${TS('m.changed_at')} as updated_at
      from core.amazon_sku_maps m where m.state = 'active' ${only}`, params)).rows;
  const components = (await db.query(`select m.seller_sku, translate(btrim(k.code), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz') as ne_code,
        c.qty as quantity, c.sort_order::int as sort_order, ${TS('c.created_at')} as created_at, ${TS('coalesce(c.updated_at, c.created_at)')} as updated_at
      from core.amazon_sku_maps m join core.listing_components c on c.listing_id = m.listing_id join core.skus k on k.sku_id = c.sku_id
     where m.state = 'active' ${only}`, params)).rows.map((r) => ({ ...r, quantity: Number(r.quantity), sort_order: Number(r.sort_order) }));
  return { master, components };
}

// ─── 保存 ───

/**
 * 保存を開いているか (段階 new_open・持ち主表のハッシュ・listing_components.amazon = company・MASTER_EDIT_OPEN)。段階の共有の鍵を持った取引の中で。
 * 🆕 広げる道 PR-2: 持ち主表 = DB の active (土台の門 lib/master-owner-gate.mjs・読めない = 503・code_behind = 409)。ctx.ownership にここで入れる
 */
async function assertOpen(db, ctx) {
  const gate = await baseGateInTx(db, { open: ctx.open, closedWhy: '保存はまだ開いていません' });
  const phase = gate.phase;
  const refuse = (why) => new MasterWriteError(409, 'before_cutover', `切替前です: Amazon SKU の対応はまだ miniPC の SKU マスタ (/apps/warehouse) が正です (${why})。何も保存していません`,
    { open: ctx.open, phase: phase.readable ? phase.phase : null });
  if (!gate.ok) throw ownerGateError(gate, refuse);
  if (gate.ownership[AMAZON_MAP_OWNER_KEY] !== 'company') throw refuse('Amazon SKU の対応の持ち主がまだ miniPC の SKU マスタの側');
  ctx.ownership = gate.ownership;
}

/** 構成品のコード → SKU (DB で確かめる前に画面で分かる誤りを先に返す。関数も確かめ直す) */
async function resolveComponents(db, list) {
  const found = (await db.query(`select x as raw, k.sku_id::text as sku_id, k.code, k.sku_kind, reg.state
      from unnest($1::text[]) as t(x) left join core.skus k on k.company_id = $2 and k.code_norm = core.norm_code(x)
      left join ops.master_registrations reg on reg.sku_id = k.sku_id`, [list.map((r) => r.code), COMPANY_ID])).rows;
  const byRaw = new Map(found.map((f) => [f.raw, f]));
  const ids = new Set();
  return list.map((r) => {
    const k = byRaw.get(r.code);
    if (!k || !k.sku_id) throw bad(`構成品 ${r.code} は Company DB にありません`, 'components');
    if (!['single', 'set'].includes(k.sku_kind)) throw bad(`${k.code} は例外の SKU (NE に無い商品) なので構成品にできません`, 'components');
    if (!USABLE_REG_STATES.includes(k.state)) throw bad(`${k.code} はまだ NE で確かめた商品ではありません (登録の状態: ${k.state || 'なし'})`, 'components');
    if (ids.has(k.sku_id)) throw bad(`構成品 ${r.code} が 2 回あります (同じ品は 1 行にして数量で)`, 'components');
    ids.add(k.sku_id);
    return { sku_id: Number(k.sku_id), code: k.code, qty: r.qty };
  });
}

/** 関数の誤り → 画面の誤り (それ以外は投げ直す = 500) */
function mapDbError(e, req) {
  const msg = String((e && e.message) || '');
  const m = /^([a-z_]+):\s*(.*)$/s.exec(msg);
  const key = m ? m[1] : '';
  const rest = m ? m[2] : '';
  if (key === 'before_cutover') return new MasterWriteError(409, 'before_cutover', `切替前です (DB が断った: ${rest})。何も保存していません`);
  if (key === 'version_conflict') return new MasterWriteError(409, 'version_conflict', '画面を開いた後に、この Amazon SKU (構成・名前・削除) が変わりました。何も保存していません。画面を開き直してから、もう一度入れてください');
  if (key === 'norm_collision') return new MasterWriteError(409, 'norm_collision', `${rest}。何も保存していません`, { field: 'seller_sku' });
  if (key === 'not_found') return new MasterWriteError(404, 'not_found', `seller SKU ${req.sellerSku} の対応がありません`);
  if (key === 'already_deleted') return new MasterWriteError(409, 'already_deleted', `seller SKU ${req.sellerSku} はもう削除 (墓標) になっています`);
  if (key === 'component_unusable') return bad(`構成品に使えない商品があります (DB が断った: ${rest.replace(/^構成品に使えない商品がある: /, '')})。何も保存していません`, 'components');
  if (key === 'invalid_value' || key === 'invalid_input') return bad(`保存の値が DB の決まりに合いません (${rest})。何も保存していません`, 'values');
  return null;
}

/**
 * 保存 (op = save) / 墓標にする (op = delete)。opts = { open (MASTER_EDIT_OPEN)・ownership (試験で差し替え)・beforeCommit (試験) }
 * 戻り値 = 結果 (保存の記録にも同じもの)。誤りは MasterWriteError
 */
export async function saveAmazonMap(db, input, opts = {}) { return run(db, parseAmazonMapSave(input), opts); }
export async function deleteAmazonMap(db, input, opts = {}) { return run(db, parseAmazonMapDelete(input), opts); }

const OPERATION = { save: 'amazon_map_save', delete: 'amazon_map_delete' };

async function run(db, req, opts) {
  // 持ち主表 = 取引の中で読む DB の active (広げる道 PR-2)。opts.ownership (配った config の持ち主表) はもう使わない
  const ctx = { ownership: null, open: opts.open === true, payloadHash: amazonMapPayloadHashOf(req), startedAt: null, operation: OPERATION[req.op] };
  await db.query('begin');
  try {
    ctx.startedAt = (await db.query('select now()::text as t')).rows[0].t;
    const out = await inTx(db, req, ctx);
    if (out.replay) { await db.query('rollback'); return out.replay; }
    if (opts.beforeCommit) await opts.beforeCommit();
    await db.query('commit');
    return out.result;
  } catch (e0) {
    let e = e0;
    try { await db.query('rollback'); } catch { /* 接続が切れていれば rollback も失敗する */ }
    // 同じ request_id を別の取引が先に書いた = 残った記録を返す (⑤-1 と同じ)
    if (e && e.code === '23505' && /master_edit_requests_pkey/.test(String(e.constraint || e.message || ''))) {
      const prev = (await db.query('select actor_id, operation, payload_hash, status, result, error from ops.master_edit_requests where request_id = $1', [req.requestId])).rows[0];
      return replayOrThrow(prev, req, ctx);
    }
    // seller SKU が正規化で重なる・同じ seller SKU をほかの保存がちょうど作った (一意の誤り) = 409
    if (e && e.code === '23505' && /amazon_sku_maps_seller_sku_key|listings_mall_shop_code_listing_norm_key/.test(String(e.constraint || e.message || ''))) {
      e = new MasterWriteError(409, 'retry', `seller SKU ${req.sellerSku} をちょうどほかの処理が作りました。何も保存していません。画面を開き直してください`);
    }
    if (ctx.startedAt && !(e instanceof MasterWriteError && (e.reason === 'request_id_reused' || e.extra?.replayed))) {
      const locked = e && e.code === '55P03';
      const err = e instanceof MasterWriteError
        ? { status: e.status, reason: e.reason, message: e.message, extra: e.extra }
        : { status: locked ? 409 : 500, reason: locked ? 'locked' : 'error', message: locked ? 'ほかの処理が同じ Amazon SKU を使っていました' : 'サーバーエラー', pg_code: e && e.code ? String(e.code) : null };
      const prev = await recordFailure(db, req, ctx, err);
      if (prev) return replayOrThrow(prev, req, ctx);   // 待っている間に同じ request_id の保存が終わっていた = その結果
    }
    throw e;
  }
}

async function inTx(db, req, ctx) {
  // 記録用の設定 (画面のロールの書き込みでは、0051 の変更の記録は約束の行から取る。持ち主のロールで動かす試験ではこの設定)
  await db.query(`select set_config('core.actor_type', 'human', true), set_config('core.actor_id', $1, true), set_config('core.source_system', $2, true),
      set_config('core.request_id', $3, true), set_config('core.reason', $4, true), set_config('core.run_id', '', true)`,
  [req.actor, AMAZON_MAP_SOURCE, req.requestId, req.reason || '']);
  // 1. request_id の鍵 → 同じ request_id
  await db.query(REQUEST_LOCK_SQL, [req.requestId]);
  const prev = (await db.query('select actor_id, operation, payload_hash, status, result, error from ops.master_edit_requests where request_id = $1', [req.requestId])).rows[0];
  if (prev) return { replay: replayOrThrow(prev, req, ctx) };
  // 2. 段階の共有の鍵 → 門 (閉じていれば、ほかの鍵を取らずに 409)
  await lockCutoverSharedInTx(db);   // 55P03 は 1 回だけ取り直す (設計 M8)
  await assertOpen(db, ctx);
  // 3. マスタの書き込みの鍵 (共有・夜間ロード / 切替の日の移行が持っていれば短く待って 409 nightly_load)
  await lockMasterWriteShared(db);
  // 4. 構成品 (保存だけ) → 関数 (中で出品ごとの鍵 → 行 → 版 → 構成品の鍵と確かめ → 書く → done)
  const entry = { seller_sku: req.sellerSku, versions: req.versions, started_at: ctx.startedAt };
  if (req.op === 'save') {
    entry.name = req.name;
    entry.components = await resolveComponents(db, req.components);
  }
  const fn = req.op === 'save' ? 'ops.save_amazon_sku_map' : 'ops.delete_amazon_sku_map';
  try {
    const r = (await db.query(`select ${fn}($1::uuid, $2, $3, $4::jsonb, $5, $6::jsonb) as r`,
      [req.requestId, req.actor, req.reason || null, JSON.stringify(ctx.ownership), ctx.payloadHash, JSON.stringify(entry)])).rows[0].r;
    return { result: r };
  } catch (e) {
    throw mapDbError(e, req) || e;
  }
}

/** 失敗の記録を新しい取引で (request_id の鍵を取り、もう結果があれば書かない = その結果を返す)。書けなければ null */
async function recordFailure(db, req, ctx, err) {
  try {
    await db.query('begin');
    await db.query(REQUEST_LOCK_SQL, [req.requestId]);
    const prev = (await db.query('select actor_id, operation, payload_hash, status, result, error from ops.master_edit_requests where request_id = $1', [req.requestId])).rows[0];
    if (!prev) {
      await db.query(`insert into ops.master_edit_requests (request_id, company_id, operation, target_code, sku_id, actor_id, payload_hash, status, error, started_at)
         values ($1, $2, $3, $4, null, $5, $6, 'failed', $7::jsonb, $8::timestamptz)`,
      [req.requestId, COMPANY_ID, ctx.operation, req.sellerSku.slice(0, 60), req.actor, ctx.payloadHash, JSON.stringify(err), ctx.startedAt]);
    }
    await db.query('commit');
    return prev || null;
  } catch (e2) {
    try { await db.query('rollback'); } catch { /* */ }
    console.error(`[amazon-map] 失敗の記録を残せなかった: ${e2 && e2.message}`);
    return null;
  }
}

/** 残っている同じ request_id の記録 → 結果 / 同じ誤り / 中身が違えば 409 (done の payload_hash は DB が作った値 = 画面の要求のハッシュは結果の request_payload_hash) */
function replayOrThrow(prev, req, ctx) {
  const reqHash = prev && prev.status === 'done' && prev.result && prev.result.request_payload_hash ? prev.result.request_payload_hash : prev && prev.payload_hash;
  if (!prev || prev.actor_id !== req.actor || prev.operation !== ctx.operation || reqHash !== ctx.payloadHash) {
    throw new MasterWriteError(409, 'request_id_reused', '同じ保存の番号 (request_id) で違う中身が来ました。画面を開き直してください');
  }
  if (prev.status === 'done') return { ...prev.result, replayed: true };
  const x = prev.error || {};
  throw new MasterWriteError(x.status || 500, x.reason || 'error', x.message || '前の保存は失敗しました', { ...(x.extra || {}), replayed: true });
}
