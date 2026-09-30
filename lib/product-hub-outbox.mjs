/**
 * product-hub-outbox.mjs — 新商品の登録 (Company DB) から product-hub (SQLite) の出品カードを作る知らせ (outbox)
 * (Company DB構想 14 §9 H7・§10 契約 v3 Medium 1・§11 の「product-hub のカード」)
 *
 * なぜ outbox: Postgres と SQLite は 1 つの取引にできない。登録 (lib/master-register.mjs) と同じ Postgres の取引で
 *   ops.product_hub_outbox (0051) に知らせを書き、あとで product-hub の側 (apps/product-hub/services/cdb-card-intake.js) が
 *   cdb_sku_id の一意で冪等に取り込む。知らせの中身 (event_id・版・payload・hash) は変えない。
 * いつ取り込むか (🚨 新しい定期実行は作らない):
 *   - 登録の保存が成功した直後 (同じ要求の中・うまくいかなくても登録は成功のまま)
 *   - product-hub のボードを開いたとき (sweep。MASTER_EDIT_OPEN = 1 の Render だけ)
 *   - マスタの入力の商品の画面の「カードをもう一度作る」(人が押す = conflict・回数を使い切った failed も試す)
 * 状態: pending (まだ) → done (作った・もう結んであった) / failed (失敗 = 自動で CARD_MAX_AUTO_ATTEMPTS 回まで・その後は人が押す) /
 *        conflict (同じ商品コードのカードが product-hub にもうある = 増やさない・人が決める)
 * 取るとき: 期限つきの借り (lease)。借りたまま落ちても期限が過ぎればほかが取れる (取り込みは冪等なので 2 回走っても 1 枚)
 */
import crypto from 'node:crypto';
import { openPgClient, pgAdapter } from '../scripts/company-db/migrate.mjs';
import { stable, sha256 } from './master-write.mjs';
import { readCutoverPhase, newEntryWritable } from './master-cutover.mjs';

/** payload の形の版 (中身の形を変えたら上げる。product-hub の側は知らない版を取り込まない) */
export const CARD_SCHEMA_VERSION = 'ph-card-v1';
export const CARD_KIND = 'card_create';
/** 自動 (保存の直後・ボードを開いたとき) で試す回数の上限。超えたら人が「もう一度」を押す */
export const CARD_MAX_AUTO_ATTEMPTS = 5;
export const CARD_LEASE_SECONDS = 120;
export const CARD_STATUS_LABELS = Object.freeze({ pending: 'カード作成待ち', failed: 'カード作成待ち (失敗)', done: 'カード作成済み', conflict: 'カードの衝突 (要確認)' });

export const cardPayloadHash = (payload) => sha256(stable(payload));

/**
 * カードの payload (product-hub に渡す値)。Company DB の値と、画面 D の product-hub の欄 (§11 の表) を分けて持つ。
 * 送料コード → 楽天の配送方法グループの対応は product-hub の側 (ph_shipping_method_map) で引く
 */
export function buildCardPayload({ skuId, code, kind, name, price, shipping, card, components = [], actor }) {
  return {
    schema: CARD_SCHEMA_VERSION,
    cdb_sku_id: String(skuId),
    code, kind, name, price,
    shipping: { code: shipping.code, method: shipping.method ?? null, cost_jpy: shipping.cost_jpy ?? null },
    amazon_url: card.amazon_url ?? null,
    asin: card.asin ?? null,
    official_url: card.official_url ?? null,
    reference_urls: card.reference_urls ?? [],
    set_decision: card.set_decision ?? null,
    yahoo: card.yahoo ?? null,
    components: kind === 'set' ? components.map((c) => ({ code: c.code, qty: c.qty })) : [],
    created_by: actor,
  };
}

/** 登録と同じ取引の中で知らせを書く。戻り値 = event_id */
export async function writeCardEvent(db, { companyId = 1, skuId, requestId, actor, payload }) {
  const r = (await db.query(`insert into ops.product_hub_outbox (company_id, sku_id, kind, schema_version, payload, payload_hash, request_id, created_by)
     values ($1, $2, $3, $4, $5::jsonb, $6, $7::uuid, $8) returning event_id::text as event_id`,
  [companyId, skuId, CARD_KIND, CARD_SCHEMA_VERSION, JSON.stringify(payload), cardPayloadHash(payload), requestId, actor])).rows[0];
  return r.event_id;
}

/** その SKU の知らせ (画面の「カード作成待ち」)。無ければ null (カードを作らない登録・0051 の前) */
export async function readCardEvent(db, skuId) {
  if (!(await db.query(`select to_regclass('ops.product_hub_outbox') is not null as ok`)).rows[0].ok) return null;
  return (await db.query(`select event_id::text as event_id, status, attempts, last_error, result, created_at::text as created_at, done_at::text as done_at,
       leased_until is not null and leased_until > now() as leased
     from ops.product_hub_outbox where sku_id = $1 and kind = $2`, [skuId, CARD_KIND])).rows[0] ?? null;
}

/**
 * 知らせを取り込む。apply = product-hub の取り込み (event → { outcome: created | linked | conflict, draft_id?, conflict_draft_id?, message? }・失敗は throw)。
 * opts: eventId / skuId (その知らせだけ)・manual (人が押した = conflict と回数を使い切った failed も試す)・limit・owner (借りる人)
 * 戻り値 = [{ event_id, sku_id, status, result, error, recorded }]
 * 🚨 取る (短い取引・for update skip locked・借り) → 取り込む (取引の外) → 結果を書く (借りた人のときだけ・done は二度と変えない)
 */
export async function runCardOutbox(db, apply, { eventId = null, skuId = null, manual = false, limit = 20, owner = null } = {}) {
  const me = owner || `portal:${process.pid}:${crypto.randomUUID().slice(0, 8)}`;
  const statusCond = manual
    ? `o.status in ('pending', 'failed', 'conflict')`
    : `(o.status = 'pending' or (o.status = 'failed' and o.attempts < ${Number(CARD_MAX_AUTO_ATTEMPTS)}))`;
  let claimed;
  await db.query('begin');
  try {
    claimed = (await db.query(`with c as (
        select o.event_id from ops.product_hub_outbox o
         where ${statusCond} and (o.leased_until is null or o.leased_until < now())
           and ($1::uuid is null or o.event_id = $1::uuid) and ($2::bigint is null or o.sku_id = $2::bigint)
         order by o.created_at, o.event_id limit $3 for update skip locked)
      update ops.product_hub_outbox o set lease_owner = $4, leased_until = now() + ($5::int * interval '1 second'), attempts = o.attempts + 1, updated_at = now()
        from c where o.event_id = c.event_id
      returning o.event_id::text as event_id, o.sku_id::text as sku_id, o.schema_version, o.payload, o.payload_hash, o.attempts`,
    [eventId, skuId, Math.max(1, Math.min(200, Number(limit) || 20)), me, CARD_LEASE_SECONDS])).rows;
    await db.query('commit');
  } catch (e) {
    try { await db.query('rollback'); } catch { /* */ }
    throw e;
  }
  const out = [];
  for (const ev of claimed) {
    let status; let result = null; let error = null;
    if (cardPayloadHash(ev.payload) !== ev.payload_hash) {
      status = 'failed'; error = 'payload_hash_mismatch: 知らせの中身が書いたときと違う (取り込まない)';
    } else {
      try {
        const r = await apply({ event_id: ev.event_id, sku_id: ev.sku_id, schema_version: ev.schema_version, payload: ev.payload });
        if (!r || !['created', 'linked', 'conflict'].includes(r.outcome)) throw new Error(`取り込みの答えが分からない: ${JSON.stringify(r)}`);
        status = r.outcome === 'conflict' ? 'conflict' : 'done';
        result = r;
        if (status === 'conflict') error = String(r.message || '同じ商品コードのカードが product-hub にもうある').slice(0, 2000);
      } catch (e) {
        status = 'failed'; error = String(e && e.message || e).slice(0, 2000);
      }
    }
    let recorded = false;
    try {
      const fin = await db.query(`update ops.product_hub_outbox set status = $3, result = $4::jsonb, last_error = $5,
           done_at = case when $3 = 'done' then now() end, lease_owner = null, leased_until = null, updated_at = now()
         where event_id = $1::uuid and lease_owner = $2 and status <> 'done'`, [ev.event_id, me, status, result ? JSON.stringify(result) : null, error]);
      recorded = (fin.rowCount ?? 0) === 1;
    } catch (e) {
      // 結果を書けなかった = 借りの期限が過ぎたら次の回がもう一度取り込む (product-hub の側は冪等 = 2 枚にはならない)
      console.error(`[product-hub-outbox] 結果を書けなかった ${ev.event_id}: ${e && e.message}`);
    }
    out.push({ event_id: ev.event_id, sku_id: ev.sku_id, status, result, error, recorded, attempts: Number(ev.attempts) });
  }
  return out;
}

/**
 * 衝突 (conflict) を人が解く = 既存のカードをこの SKU に結ぶ (PR #1566 R1 M6)。link = product-hub の linkCdbCardToExisting (SQLite の 1 つの取引で衝突を確かめ直して結ぶ)。
 * 順番: 知らせの行を for update (ほかの取り込みと並ぶ) → SQLite で結ぶ → 知らせを done (結果 = linked・誰が) → commit。
 * SQLite で結んだ後に commit できなかった = 次に押したとき / 取り込みが「もう結んである」(linked) で done にする (冪等)
 * 戻り値 { ok, reason?, draft_id?, already? }。reason = no_event / not_conflict / leased / already_done / hash_mismatch
 */
export async function linkCardToExisting(db, link, { skuId, actor }) {
  await db.query('begin');
  try {
    const ev = (await db.query(`select event_id::text as event_id, sku_id::text as sku_id, status, schema_version, payload, payload_hash, result,
         (leased_until is not null and leased_until > now()) as leased
       from ops.product_hub_outbox where sku_id = $1 and kind = $2 for update`, [skuId, CARD_KIND])).rows[0];
    const stop = async (out) => { await db.query('rollback'); return out; };
    if (!ev) return stop({ ok: false, reason: 'no_event' });
    if (ev.status === 'done') return stop({ ok: ev.result?.outcome === 'linked', reason: 'already_done', draft_id: ev.result?.draft_id ?? null, already: true });
    if (ev.status !== 'conflict' || !ev.result?.conflict_draft_id) return stop({ ok: false, reason: 'not_conflict', status: ev.status });
    if (ev.leased) return stop({ ok: false, reason: 'leased' });
    if (cardPayloadHash(ev.payload) !== ev.payload_hash) return stop({ ok: false, reason: 'hash_mismatch' });
    const r = await link({ event_id: ev.event_id, sku_id: ev.sku_id, schema_version: ev.schema_version, payload: ev.payload }, { draftId: ev.result.conflict_draft_id, actor });
    if (!r || r.outcome !== 'linked') throw new Error(`結ぶ処理の答えが分からない: ${JSON.stringify(r)}`);
    const result = { outcome: 'linked', draft_id: r.draft_id, linked_by: actor, conflict_draft_id: ev.result.conflict_draft_id };
    await db.query(`update ops.product_hub_outbox set status = 'done', result = $2::jsonb, last_error = null, done_at = now(), lease_owner = null, leased_until = null, updated_at = now()
       where event_id = $1::uuid and status <> 'done'`, [ev.event_id, JSON.stringify(result)]);
    await db.query('commit');
    return { ok: true, draft_id: r.draft_id, already: !!r.already };
  } catch (e) {
    try { await db.query('rollback'); } catch { /* */ }
    throw e;
  }
}

// ─── Company DB へのつなぎ (product-hub のボード・/new から。試験は差し替える) ───
let clientFactory = openPgClient;
export function __setCompanyDbClientFactory(fn) { clientFactory = fn || openPgClient; }

/**
 * Company DB につないで fn(db) を呼ぶ。つながらない・URL が無い = { ok: false, error } (投げない)
 * TODO (⑤-1 の rebase のとき): ⑤-1 R1 の画面は画面だけのロール (COMPANY_DB_MASTER_EDIT_URL = master_edit) でつなぐ。ここも同じロールにそろえる
 *   (権限 = scripts/company-db/master-register-grants.mjs の master_edit: 知らせの読み・取り込みの結果の列の更新・段階の読み)
 */
export async function withCompanyDb(fn, { applicationName = 'portal', statementTimeout = '10s' } = {}) {
  const url = process.env.COMPANY_DB_URL;
  if (!url) return { ok: false, error: 'no_url' };
  let client;
  try {
    client = await clientFactory(url, { application_name: applicationName });
    if (client.on) client.on('error', (e) => console.error(`[company-db] 接続のエラー: ${e.message}`));
    await client.query(`set statement_timeout = '${/^\d+m?s$/.test(statementTimeout) ? statementTimeout : '10s'}'`);
    await client.query(`set lock_timeout = '5s'`);
    await client.query(`set idle_in_transaction_session_timeout = '60s'`);
  } catch (e) {
    try { if (client) await client.end(); } catch { /* */ }
    return { ok: false, error: `unreachable: ${e && e.message}` };
  }
  try {
    return { ok: true, value: await fn(pgAdapter(client)) };
  } finally {
    try { await client.end(); } catch { /* */ }
  }
}

/** ボードを開いたときの取り込み (sweep) をするか。MASTER_EDIT_OPEN = 1 の Render だけ (切替の前は Company DB につながない = 今の動きのまま) */
export const cardSweepEnabled = () => process.env.MASTER_EDIT_OPEN === '1' && !!process.env.COMPANY_DB_URL && (process.env.PORTAL_VARIANT || 'render').toLowerCase() === 'render';

/** ボードを開いたときの取り込み。apply は product-hub の取り込み。失敗しても投げない (ボードは出す) */
export async function sweepCardOutbox(apply, { limit = 20 } = {}) {
  if (!cardSweepEnabled()) return { ok: true, skipped: 'disabled', results: [] };
  try {
    const r = await withCompanyDb((db) => runCardOutbox(db, apply, { limit }), { applicationName: 'product-hub-card-sweep' });
    if (!r.ok) return { ok: false, error: r.error, results: [] };
    return { ok: true, results: r.value };
  } catch (e) {
    console.error(`[product-hub-outbox] sweep の失敗: ${e && e.message}`);
    return { ok: false, error: String(e && e.message || e), results: [] };
  }
}

/**
 * product-hub の「新規作成」をどう出すか (§11 の 2 = 新商品の入口を 1 つに)。
 *   legacy = 今までの新規作成の画面 (MASTER_EDIT_OPEN が無い = Company DB につながない・段階が legacy_open)
 *   guide  = 新しい「新商品の登録」へ案内するだけ (段階 new_open かつ MASTER_EDIT_OPEN = 1)
 *   paused = 切替の途中 (frozen / company_owner) か、段階が読めない = 登録は止めている (古い入口も開けない = fail-closed)
 */
export async function newEntryGate() {
  if (process.env.MASTER_EDIT_OPEN !== '1') return { mode: 'legacy' };
  const r = await withCompanyDb((db) => readCutoverPhase(db), { applicationName: 'product-hub-new-gate', statementTimeout: '5s' });
  if (!r.ok) return { mode: 'paused', phase: null, error: r.error };
  if (newEntryWritable(r.value)) return { mode: 'guide', phase: r.value.phase };
  if (r.value.readable && r.value.phase === 'legacy_open') return { mode: 'legacy' };
  return { mode: 'paused', phase: r.value.readable ? r.value.phase : null, error: r.value.error || null };
}
