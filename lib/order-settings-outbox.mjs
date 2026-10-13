/**
 * order-settings-outbox.mjs — 新商品の登録の「発注の設定の知らせ」(Company DB → 発注アプリ) (AI_reference CompanyDB構想/21 v4 の C1・0075)
 *
 * なぜ: 新商品の登録は ① Company DB (Postgres) に登録 → ② 発注アプリ (Render の SQLite の po_*) に発注の設定を書く の 2 段。
 *   Postgres と SQLite は 1 つの取引にできない = ② だけ失敗すると発注の設定だけ消える。
 *   → ① と同じ Postgres の取引に知らせ (ops.order_settings_outbox) を積み (enqueueOrderSettingsInTx)、届くまで何度でも届け直す。
 *   正本は発注アプリのまま (D-44・中原さんの決定 1 = a)。届ける = 発注アプリの 1 つの部品 (apps/purchase-orders/order-settings.js の
 *   writeRegistrationOrderSettings・届けの ID = request_id で 1 回だけ = 2 回届けても 1 回分・その後に人が直した値を戻さない)。
 * いつ届けるか (🚨 新しい定期実行は作らない):
 *   - 登録の保存が成功した直後 (同じ要求の中・届かなくても登録は成功のまま)
 *   - マスタの入力・発注アプリの画面を開いたとき (画面の上の「届いていない件数」を読むついで・60 秒に 1 回まで)
 *   - 人の「もう一度届ける」(マスタの入力の「発注の設定の届け待ち」の画面)
 *   - 既存の夜間ロード (apps/company-db/nightly.mjs = company-db-nightly-load) の後ろの 1 段 (runOrderSettingsNightlyStep)。
 *     24 時間を超えて届いていない知らせが 1 つでもあれば段の失敗 = 夜間ロードの ping が fail = jobs-monitor が知らせる
 * 🚨 届けるのは Render の中だけ (lib/is-render.js)。miniPC も同じ server.js を動かすが、miniPC の SQLite は発注アプリの正本ではない
 * 状態: pending → done / failed (自動は 5 回まで・夜間の段は毎晩) / conflict (発注アプリにほかの画面がもう行を作っていた = 上書きしない) / dismissed (人が閉じた)
 * 決定 2 = b (中原さん 10/13): 届いていない間も発注アプリのすすめる数は止めない (今の発注アプリの設定で出す)。画面に「発注の設定がまだ届いていない」の印だけ
 */
import crypto from 'node:crypto';
import { openPgClient, pgAdapter } from '../scripts/company-db/migrate.mjs';
import { stable, sha256 } from './master-write.mjs';
import { isRender } from './is-render.js';
import { withCompanyDb } from './product-hub-outbox.mjs';
// 🚨 apps/purchase-orders/order-settings.js は import しない (その先の warehouse-mirror/db.js が import のときに DATA_DIR を読む = 登録の lib から読むと
//    試験の DATA_DIR より前に決まってしまう)。届ける部品は使うときに読む (defaultApply)・形の確かめは apps/master-edit/order-settings.mjs の orderBaseOf

/** 知らせの中身の形の版 (形を変えたら上げる。DB の ops.order_settings_payload_problem も同じ版だけ受ける) */
export const ORDER_SCHEMA_VERSION = 'po-settings-v1';
export const ORDER_LEASE_SECONDS = 120;
/** 自動 (登録の直後・画面を開いたとき) で試す回数の上限。夜間の段 (sweep) と人の「もう一度」は上限なし */
export const ORDER_MAX_AUTO_ATTEMPTS = 5;
/** これより古い届いていない知らせ = 夜間の段の失敗 (jobs-monitor に知らせる) */
export const ORDER_OVERDUE_HOURS = 24;
export const ORDER_OPEN_STATUSES = Object.freeze(['pending', 'failed', 'conflict']);
export const ORDER_STATUS_LABELS = Object.freeze({
  pending: '届け待ち', failed: '届け待ち (失敗・もう一度届けます)', conflict: '要確認 (発注アプリにもう設定がある)', done: '届いた', dismissed: '閉じた (人)',
});
const PATCH_KEYS = Object.freeze(['order_lot', 'condition_id', 'material_group_id', 'capacity_per_unit', 'case_group', 'case_lot']);
const NC_KEYS = Object.freeze(['condition_id', 'condition_type', 'condition_value', 'display_name', 'maker_name', 'unit']);
const NM_KEYS = Object.freeze(['group_id', 'min_order_qty', 'name', 'unit']);
const PAYLOAD_KEYS = Object.freeze(['code', 'new_condition', 'new_material', 'patch', 'schema', 'supplier_code']);

const str = (v) => (v == null ? null : String(v));
const isObj = (v) => v !== null && typeof v === 'object' && !Array.isArray(v);
const sameKeys = (o, keys) => isObj(o) && Object.keys(o).sort().join(',') === [...keys].sort().join(',');
const strOrNull = (v) => v === null || typeof v === 'string';

/** 1 つの商品の知らせの中身 */
export const orderPayloadOf = (code, base) => ({ schema: ORDER_SCHEMA_VERSION, code: String(code), ...base });
export const orderPayloadHash = (payload) => sha256(stable(payload));

/** 中身の形 (表を読まない)。問題が無ければ null。🚨 DB の ops.order_settings_payload_problem (0075) と同じ決まり・同じ順 */
export function orderPayloadShapeProblem(p) {
  if (!isObj(p)) return 'payload_not_object';
  if (!sameKeys(p, PAYLOAD_KEYS)) return 'payload_keys';
  if (p.schema !== ORDER_SCHEMA_VERSION) return 'schema';
  if (typeof p.code !== 'string' || [...p.code].length < 1 || [...p.code].length > 255) return 'code';
  if (!strOrNull(p.supplier_code)) return 'supplier_code';
  if (!isObj(p.patch)) return 'patch';
  for (const [k, v] of Object.entries(p.patch)) {
    if (!PATCH_KEYS.includes(k)) return 'patch_key';
    if (!strOrNull(v)) return 'patch_value';
  }
  if (isObj(p.new_condition)) {
    if (!sameKeys(p.new_condition, NC_KEYS)) return 'new_condition_keys';
    if (Object.values(p.new_condition).some((v) => !strOrNull(v))) return 'new_condition_value';
  } else if (p.new_condition !== null) return 'new_condition';
  if (isObj(p.new_material)) {
    if (!sameKeys(p.new_material, NM_KEYS)) return 'new_material_keys';
    if (Object.values(p.new_material).some((v) => !strOrNull(v))) return 'new_material_value';
  } else if (p.new_material !== null) return 'new_material';
  return null;
}

const OUTBOX_FUNCS = Object.freeze(['ops.enqueue_order_settings(uuid, text, jsonb)', 'ops.claim_order_settings_events(text, text, uuid, uuid, integer, integer, integer, uuid[])',
  'ops.finish_order_settings_event(uuid, text, text, jsonb, text)']);
/**
 * 0075 の様子 (今の接続のロールで): absent (表・関数が無い = 0075 の前) / no_privilege (入ったが、このロールに表の select・関数の実行が無い =
 *   migration の後・ロールの流し直しの前) / ready。🚨 ready の時だけ知らせを使う。ほか = 今までの「② をすぐ書く」(登録は止めない・Codex #1703 R1 L1)
 */
export async function orderOutboxState(db) {
  const has = (await db.query(`select to_regclass('ops.order_settings_outbox') is not null as t, array(select to_regprocedure(x) is not null from unnest($1::text[]) x) as f`, [OUTBOX_FUNCS])).rows[0];
  if (!has.t || !has.f.every(Boolean)) return 'absent';
  const pr = (await db.query(`select has_table_privilege(current_user, 'ops.order_settings_outbox', 'select') as t,
       array(select has_function_privilege(current_user, to_regprocedure(x), 'execute') from unnest($1::text[]) x) as f`, [OUTBOX_FUNCS])).rows[0];
  return pr.t && pr.f.every(Boolean) ? 'ready' : 'no_privilege';
}
/** 知らせを使ってよいか (= orderOutboxState が ready) */
export async function orderOutboxReady(db) {
  return (await orderOutboxState(db)) === 'ready';
}

/**
 * 登録の取引の中で知らせを積む (lib/master-register.mjs・lib/master-variation.mjs が登録の関数の後・commit の前に呼ぶ)。
 * items = [{ code }]・base = orderBaseOf の答え。戻り値 = [{ event_id, code, sku_id, delivery_request_id }]
 * 🚨 DB (ops.enqueue_order_settings) が「この取引で書いた同じ人の登録の SKU」だけを受ける = 取引の外・ほかの人・ほかの商品は断る
 */
export async function enqueueOrderSettingsInTx(db, { registrationRequestId, actor, items, base }) {
  const list = items.map((it) => ({ code: String(it.code), payload: orderPayloadOf(it.code, base) }));
  return (await db.query('select ops.enqueue_order_settings($1::uuid, $2, $3::jsonb) as r', [registrationRequestId, actor, JSON.stringify(list)])).rows[0].r;
}

/** 知らせ → 発注アプリの部品の入力 (writeRegistrationOrderSettings)。届けの ID = 部品の request_id (1 回だけ) */
export const deliveryInputOf = (ev) => ({
  code: ev.payload.code, patch: ev.payload.patch, newCondition: ev.payload.new_condition, newMaterial: ev.payload.new_material,
  supplierCode: ev.payload.supplier_code, actor: ev.created_by, requestId: ev.delivery_request_id,
});
async function defaultApply(ev) {
  const { writeRegistrationOrderSettings } = await import('../apps/purchase-orders/order-settings.js');
  return writeRegistrationOrderSettings(deliveryInputOf(ev));
}

/**
 * 知らせを届ける。apply(ev) = 発注アプリに書く (既定 = writeRegistrationOrderSettings。答え { changed, created, replayed }・誤りは throw)。
 * opts: mode (auto / sweep / manual)・registrationRequestId / eventId (その登録 / 知らせだけ)・limit・owner (借りる人)
 * 戻り値 = [{ event_id, code, sku_id, status, result, error, recorded, attempts }]
 * 🚨 借りる (ops.claim_order_settings_events・期限つき) → 書く (取引の外・SQLite の 1 つの取引) → 結果 (ops.finish_order_settings_event・借りた人だけ)。
 *    結果を書けなかった = 借りの期限の後に次の回がもう一度 (部品は届けの ID で 1 回だけ = 前の結果を返す = 2 回分にならない)
 */
export async function runOrderSettingsOutbox(db, { mode = 'auto', registrationRequestId = null, eventId = null, limit = 20, owner = null, apply = null, exclude = null } = {}) {
  const me = owner || `po-set:${process.pid}:${crypto.randomUUID().slice(0, 8)}`;
  const claimed = (await db.query(`select event_id::text as event_id, sku_id::text as sku_id, code, registration_kind, registration_request_id::text as registration_request_id,
       delivery_request_id::text as delivery_request_id, schema_version, payload, payload_hash, created_by, attempts, status, reg_state
     from ops.claim_order_settings_events($1, $2, $3::uuid, $4::uuid, $5, $6, $7, $8::uuid[])`,
  [me, mode, registrationRequestId, eventId, Math.max(1, Math.min(200, Number(limit) || 20)), ORDER_LEASE_SECONDS, ORDER_MAX_AUTO_ATTEMPTS,
    exclude && exclude.length ? exclude : null])).rows;
  const out = [];
  for (const ev of claimed) {
    let status; let result = null; let error = null;
    const prob = ev.schema_version !== ORDER_SCHEMA_VERSION ? `schema: ${ev.schema_version}`
      : orderPayloadHash(ev.payload) !== ev.payload_hash ? 'payload_hash_mismatch'
        : orderPayloadShapeProblem(ev.payload) || (ev.payload.code !== ev.code ? 'code_mismatch' : null);
    if (prob) {
      status = 'failed'; error = `${prob}: 知らせの中身が書いたときと違う / 形が違う (届けない)`;
    } else if (ev.reg_state === 'cancelled') {
      // 登録をやめた下書き (0071) = 届けない (発注アプリはやめた商品の設定を要らない)。知らせは閉じる
      status = 'done'; result = { outcome: 'skipped_cancelled' };
    } else {
      try {
        const w = await (apply || defaultApply)(ev);
        status = 'done';
        result = { outcome: w && w.replayed ? 'replayed' : w && w.changed ? 'written' : 'unchanged', changed: !!(w && w.changed), created: (w && w.created) || null, replayed: !!(w && w.replayed) };
      } catch (e) {
        const reason = e && e.name === 'OrderSettingsError' ? e.reason : null;
        if (reason === 'stale' || reason === 'request_id_reused') {
          // 発注アプリにほかの画面 (マスタ管理・商品の画面) がもうこの商品の行を作っていた = 上書きしない (正本は発注アプリ)。人が確かめて閉じる
          status = 'conflict';
          error = reason === 'stale' ? 'この商品の発注の設定は発注アプリにもう作られていました (ほかの画面)。上書きしていません。商品の画面で確かめてから閉じてください'
            : '同じ届けの ID で違う発注の設定が発注アプリに残っています。上書きしていません。商品の画面で確かめてから閉じてください';
          result = { outcome: 'conflict', reason };
        } else {
          status = 'failed';
          error = String((e && e.message) || e).slice(0, 2000);
          result = reason ? { outcome: 'failed', reason, field: (e.extra && e.extra.field) || null } : null;
        }
      }
    }
    let recorded = false;
    try {
      recorded = (await db.query('select ops.finish_order_settings_event($1::uuid, $2, $3, $4::jsonb, $5) as ok',
        [ev.event_id, me, status, result ? JSON.stringify(result) : null, error])).rows[0].ok === true;
    } catch (e) {
      console.error(`[order-settings-outbox] 結果を書けなかった ${ev.event_id}: ${e && e.message}`);
    }
    out.push({ event_id: ev.event_id, code: ev.code, sku_id: ev.sku_id, status, result, error, recorded, attempts: Number(ev.attempts) });
  }
  return out;
}

/** 1 つの登録の知らせ (画面の応答)。[{ event_id, code, status, attempts, last_error, result, created_at }] */
export async function readOrderSettingsForRegistration(db, registrationRequestId) {
  return (await db.query(`select event_id::text as event_id, code, sku_id::text as sku_id, status, attempts, last_error, result, created_at::text as created_at, payload_hash
     from ops.order_settings_outbox where registration_request_id = $1::uuid order by code`, [registrationRequestId])).rows;
}

/**
 * 届いていない知らせのうち、渡したコード (発注アプリの画面に出ている商品) のもの = 正規化したコードの集合 (Codex #1703 R1 L2 = 件数の上限で印が欠けない)。
 * 0075 の前 = null
 */
export async function readOrderSettingsPendingFor(db, codes) {
  if (!(await db.query(`select to_regclass('ops.order_settings_outbox') is not null as ok`)).rows[0].ok) return null;
  const keys = [...new Set((codes || []).map((c) => String(c ?? '').trim().toLowerCase()).filter(Boolean))];
  if (!keys.length) return new Set();
  const rows = (await db.query(`select distinct lower(btrim(o.code)) as k from ops.order_settings_outbox o
     where o.status in ('pending', 'failed', 'conflict') and lower(btrim(o.code)) = any($1::text[])`, [keys])).rows;
  return new Set(rows.map((r) => r.k));
}

/**
 * 届いていない知らせ (画面の上の件数・届け待ちの画面・夜間の段)。0075 の前 = { ready: false }。
 * 戻り値 { ready, open, overdue, oldest_at, by_status: { pending, failed, conflict }, items: [{ event_id, code, sku_id, status, attempts, last_error, created_at, overdue, registration_kind }] (古い順・limit まで) }
 */
export async function readOrderSettingsOutbox(db, { limit = 50, closedDays = 0 } = {}) {
  if (!(await db.query(`select to_regclass('ops.order_settings_outbox') is not null as ok`)).rows[0].ok) return { ready: false, open: 0, overdue: 0, oldest_at: null, by_status: {}, items: [], closed: [] };
  const sum = (await db.query(`select o.status, count(*)::int as n, min(o.created_at)::text as oldest_at,
       count(*) filter (where o.created_at < now() - make_interval(hours => $1))::int as overdue
     from ops.order_settings_outbox o where o.status in ('pending', 'failed', 'conflict') group by o.status`, [ORDER_OVERDUE_HOURS])).rows;
  const by = Object.fromEntries(ORDER_OPEN_STATUSES.map((s) => [s, 0]));
  let open = 0; let overdue = 0; let oldest = null;
  for (const r of sum) { by[r.status] = r.n; open += r.n; overdue += r.overdue; if (!oldest || r.oldest_at < oldest) oldest = r.oldest_at; }
  const items = (await db.query(`select o.event_id::text as event_id, o.code, o.sku_id::text as sku_id, o.status, o.attempts, o.last_error, o.created_at::text as created_at,
       o.registration_kind, o.created_by, o.created_at < now() - make_interval(hours => $1) as overdue,
       o.leased_until is not null and o.leased_until > now() as leased
     from ops.order_settings_outbox o where o.status in ('pending', 'failed', 'conflict') order by o.created_at, o.event_id limit $2`,
  [ORDER_OVERDUE_HOURS, Math.max(1, Math.min(500, Number(limit) || 50))])).rows;
  const closed = closedDays > 0 ? (await db.query(`select o.event_id::text as event_id, o.code, o.status, o.attempts, o.result, o.closed_at::text as closed_at, o.closed_by, o.close_reason
     from ops.order_settings_outbox o where o.status in ('done', 'dismissed') and o.closed_at > now() - make_interval(days => $1) order by o.closed_at desc limit 100`,
  [Math.min(90, Number(closedDays))])).rows : [];
  return { ready: true, open, overdue, oldest_at: oldest, by_status: by, items, closed };
}

/** 人が閉じる (failed / conflict だけ・理由が要る)。閉じた = true / もう閉じていた・借りの最中・届け待ち (pending) = false */
export async function dismissOrderSettingsEvent(db, { eventId, actor, reason }) {
  return (await db.query('select ops.dismiss_order_settings_event($1::uuid, $2, $3) as ok', [eventId, actor, reason])).rows[0].ok === true;
}

// ─── 届けるのは Render の中だけ (試験は差し替える) ───
let deliveryForced = null;
export function __setOrderDeliveryEnabledForTest(v) { deliveryForced = v == null ? null : !!v; }
/** 届けてよいか = Render の中 (miniPC の SQLite は発注アプリの正本ではない) */
export const orderDeliveryEnabled = () => (deliveryForced != null ? deliveryForced : isRender());

// ─── 画面の上の件数 (マスタの入力・発注アプリ) = 開いたときに届け直してから数える (60 秒に 1 回まで・読むのは 15 秒の使い回し) ───
let lastSweepAt = 0;
let statusCache = null;
const SWEEP_EVERY_MS = 60 * 1000;
const STATUS_TTL_MS = 15 * 1000;
export function __resetOrderOutboxCacheForTest() { lastSweepAt = 0; statusCache = null; }

/**
 * 画面の上の件数。届けてよい (Render) かつ前の届け直しから 60 秒過ぎた = 先に届け直す (auto・20 件まで・画面のロール)。失敗しても投げない。
 * 戻り値 { ok, ready, open, overdue, by_status, items, error? }
 */
export async function orderOutboxStatus({ apply = null, sweep = true, limit = 50 } = {}) {
  const now = Date.now();
  if (sweep && orderDeliveryEnabled() && now - lastSweepAt >= SWEEP_EVERY_MS) {
    lastSweepAt = now;
    try {
      const r = await withCompanyDb(async (db) => ((await orderOutboxReady(db)) ? runOrderSettingsOutbox(db, { mode: 'auto', limit: 20, apply }) : []),
        { applicationName: 'po-settings-sweep', role: 'edit' });
      if (r.ok && r.value.length) statusCache = null;
      if (!r.ok && r.error !== 'no_edit_url') console.error(`[order-settings-outbox] 画面を開いたときの届け直しの失敗: ${r.error}`);
    } catch (e) { console.error(`[order-settings-outbox] 画面を開いたときの届け直しの失敗: ${e && e.message}`); }
  }
  if (statusCache && now - statusCache.at < STATUS_TTL_MS && statusCache.limit >= limit) return statusCache.value;
  try {
    const r = await withCompanyDb((db) => readOrderSettingsOutbox(db, { limit }), { applicationName: 'po-settings-status', statementTimeout: '5s' });
    const value = r.ok ? { ok: true, ...r.value } : { ok: false, ready: false, open: 0, overdue: 0, by_status: {}, items: [], error: r.error };
    if (r.ok) statusCache = { at: now, limit, value };
    return value;
  } catch (e) {
    return { ok: false, ready: false, open: 0, overdue: 0, by_status: {}, items: [], error: String((e && e.message) || e) };
  }
}

/**
 * 夜間ロードの後ろの 1 段 (apps/company-db/nightly.mjs)。本体のロードとは別の接続・別の取引。
 * 届け直す (sweep = 失敗の回数を問わない・200 件ずつ・10 回まで) → 24 時間を超えて届いていない知らせを数える。
 * 戻り値 { ok, note, delivered, failed, conflict, open, overdue }。ok = 24 時間を超えた知らせが 0 (届け直しの失敗だけなら翌晩・画面を開いたときにもう一度)
 */
export async function runOrderSettingsNightlyStep({ url, connect = openPgClient, apply = null, log = () => {}, batch = 200, maxRounds = 50 } = {}) {
  if (!orderDeliveryEnabled()) return { ok: true, skipped: true, note: 'Render の外なので届けない' };
  if (!url) return { ok: false, note: 'COMPANY_DB_URL が無い' };
  let client;
  try {
    client = await connect(url, { application_name: 'company-db-nightly-po-settings' });
    if (client.on) client.on('error', (e) => log(`接続のエラー: ${e.message}`));
    const db = client.query && client.exec ? client : pgAdapter(client);
    await db.query(`set statement_timeout = '60s'`);
    if (!(await orderOutboxReady(db))) return { ok: true, skipped: true, note: '0075 の前 (知らせの表が無い)' };
    const tally = { done: 0, failed: 0, conflict: 0 };
    // この回にもう借りた知らせは次の周で借りない (いつも失敗する行が何周も同じ席を取らない・Codex #1703 R1 M1)
    const seen = [];
    for (let round = 0; round < maxRounds; round++) {
      const r = await runOrderSettingsOutbox(db, { mode: 'sweep', limit: batch, owner: `nightly:${process.pid}:${round}`, apply, exclude: seen });
      for (const x of r) { tally[x.status] = (tally[x.status] || 0) + 1; seen.push(x.event_id); }
      if (r.length < batch) break;
    }
    const s = await readOrderSettingsOutbox(db, { limit: 5 });
    const late = s.items.filter((i) => i.overdue).map((i) => i.code);
    const note = `届けた ${tally.done}・失敗 ${tally.failed}・要確認 ${tally.conflict} / 届いていない ${s.open}`
      + (s.overdue ? ` (24時間超 ${s.overdue}: ${late.slice(0, 3).join(',')}${s.overdue > 3 ? ' ほか' : ''})` : '');
    log(`発注の設定の知らせ: ${note}`);
    return { ok: s.overdue === 0, note, delivered: tally.done, failed: tally.failed, conflict: tally.conflict, open: s.open, overdue: s.overdue };
  } catch (e) {
    return { ok: false, note: `想定外: ${String((e && e.message) || e).slice(0, 120)}` };
  } finally {
    try { if (client && client.end) await client.end(); } catch { /* */ }
  }
}
