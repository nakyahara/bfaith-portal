/**
 * product-hub-outbox.mjs — 新商品の登録 (Company DB) から product-hub (SQLite) の出品カードを作る知らせ (outbox)
 * (Company DB構想 14 §9 H7・§10 契約 v3 Medium 1・§11 の「product-hub のカード」)
 *
 * なぜ outbox: Postgres と SQLite は 1 つの取引にできない。登録 (lib/master-register.mjs → 0052 の ops.register_new_sku) と同じ Postgres の取引で
 *   ops.product_hub_outbox (0052) に知らせを書き、あとで product-hub の側 (apps/product-hub/services/cdb-card-intake.js) が
 *   cdb_sku_id の一意で冪等に取り込む。知らせの中身 (event_id・版・payload・hash) は変えない。
 *   SKU は知らせの行の sku_id (DB が振った番号) で結ぶ = payload には入れない (登録の関数が番号を振る前に、画面が payload と hash を作る)
 * いつ取り込むか (🚨 新しい定期実行は作らない):
 *   - 登録の保存が成功した直後 (同じ要求の中・うまくいかなくても登録は成功のまま)
 *   - product-hub のボードを開いたとき (sweep。MASTER_EDIT_OPEN = 1 かつ 画面だけのロール COMPANY_DB_MASTER_EDIT_URL のある Render だけ)
 *   - マスタの入力の商品の画面の「カードをもう一度作る」(人が押す = conflict・回数を使い切った failed も試す)
 * 状態: pending (まだ) → done (作った・もう結んであった) / failed (失敗 = 自動で CARD_MAX_AUTO_ATTEMPTS 回まで・その後は人が押す) /
 *        conflict (同じ商品コードのカードが product-hub にもうある = 増やさない・人が決める)
 * 取るとき: 期限つきの借り (lease)。借りたまま落ちても期限が過ぎればほかが取れる (取り込みは冪等なので 2 回走っても 1 枚)
 */
import crypto from 'node:crypto';
import { openPgClient, pgAdapter } from '../scripts/company-db/migrate.mjs';
import { stable, sha256 } from './master-write.mjs';
import { readCutoverPhase } from './master-cutover.mjs';
import { readScreenOwnership, screenOwnerWritable } from './master-owner-gate.mjs';
import { checkLegacyGate } from './master-legacy-gate.mjs';

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
export function buildCardPayload({ code, kind, name, price, shipping, card, components = [], actor }) {
  return {
    schema: CARD_SCHEMA_VERSION,
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

/** 登録の関数 (ops.register_new_sku) に渡すカードの知らせ (書くのは関数 = 画面のロールに知らせの insert を渡さない)。カードを作らない = null */
export const cardEventOf = (payload) => ({ schema_version: CARD_SCHEMA_VERSION, payload, payload_hash: cardPayloadHash(payload) });

/** その SKU の知らせ (画面の「カード作成待ち」)。無ければ null (カードを作らない登録・0052 の前) */
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
 * 🚨 借りる (ops.claim_card_events = security definer・for update skip locked・期限つき) → 取り込む (取引の外) →
 *    結果を書く (ops.finish_card_event = 借りた人のときだけ・done は二度と変えない)。画面のロールは知らせの状態の列を直接書けない (仮レビュー L7)
 */
export async function runCardOutbox(db, apply, { eventId = null, skuId = null, manual = false, limit = 20, owner = null } = {}) {
  const me = owner || `portal:${process.pid}:${crypto.randomUUID().slice(0, 8)}`;
  const claimed = (await db.query(`select event_id::text as event_id, sku_id::text as sku_id, schema_version, payload, payload_hash, attempts
       from ops.claim_card_events($1, $2, $3::uuid, $4::bigint, $5, $6, $7)`,
  [me, manual ? 'manual' : 'auto', eventId, skuId, Math.max(1, Math.min(200, Number(limit) || 20)), CARD_LEASE_SECONDS, CARD_MAX_AUTO_ATTEMPTS])).rows;
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
      recorded = (await db.query('select ops.finish_card_event($1::uuid, $2, $3, $4::jsonb, $5) as ok',
        [ev.event_id, me, status, result ? JSON.stringify(result) : null, error])).rows[0].ok === true;
    } catch (e) {
      // 結果を書けなかった = 借りの期限が過ぎたら次の回がもう一度取り込む (product-hub の側は冪等 = 2 枚にはならない)
      console.error(`[product-hub-outbox] 結果を書けなかった ${ev.event_id}: ${e && e.message}`);
    }
    out.push({ event_id: ev.event_id, sku_id: ev.sku_id, status, result, error, recorded, attempts: Number(ev.attempts) });
  }
  return out;
}

/**
 * 衝突 (conflict) を人が解く = 既存のカードをこの SKU に結ぶ (PR #1566 R1 M6・仮レビュー L1 / L2)。
 * link = product-hub の linkCdbCardToExisting (SQLite の 1 つの取引で衝突を確かめ直して結ぶ・カードの空の欄だけ画面 D の値で埋める)。
 * expectedDraftId = 画面が見ていたカードの番号 (知らせの conflict_draft_id と違えば結ばない = 画面を開き直してもらう)
 * 順番: 知らせを借りる (link = 衝突の知らせだけ) → SQLite で結ぶ → 知らせを done (結果 = linked・誰が・埋めた欄 / 埋めなかった欄)。
 *   SQLite で落ちた = 知らせは conflict のまま (借りを返す)。SQLite で結んだ後に done を書けなかった = 次に押したとき / 取り込みが「もう結んである」(linked) で done
 * 戻り値 { ok, reason?, draft_id?, draft_ids?, already?, applied?, not_applied? }。reason = no_event / not_conflict / leased / already_done / hash_mismatch / draft_mismatch /
 *   ambiguous (同じコードのカードが 2 枚以上 = どれか決められない。知らせは conflict のまま・PR #1566 Codex R2 Medium)
 */
export async function linkCardToExisting(db, link, { skuId, actor, expectedDraftId, owner = null }) {
  const me = owner || `link:${process.pid}:${crypto.randomUUID().slice(0, 8)}`;
  const ev = (await db.query(`select event_id::text as event_id, sku_id::text as sku_id, schema_version, payload, payload_hash, result
       from ops.claim_card_events($1, 'link', null, $2::bigint, 1, $3, $4)`, [me, skuId, CARD_LEASE_SECONDS, CARD_MAX_AUTO_ATTEMPTS])).rows[0];
  if (!ev) {
    const cur = await readCardEvent(db, skuId);
    if (!cur) return { ok: false, reason: 'no_event' };
    if (cur.status === 'done') return { ok: cur.result?.outcome === 'linked', reason: 'already_done', draft_id: cur.result?.draft_id ?? null, already: true };
    if (cur.status === 'conflict' && cur.leased) return { ok: false, reason: 'leased' };
    return { ok: false, reason: 'not_conflict', status: cur.status };
  }
  const release = async (error = `同じ商品コードのカード (#${ev.result?.conflict_draft_id ?? '?'}) が product-hub にもうあります`, result = ev.result || {}) => {
    try { await db.query('select ops.finish_card_event($1::uuid, $2, $3, $4::jsonb, $5)', [ev.event_id, me, 'conflict', JSON.stringify(result), error]); } catch (e) { console.error(`[product-hub-outbox] 借りを返せなかった ${ev.event_id}: ${e && e.message}`); }
  };
  if (ev.result?.ambiguous) {
    await release(ev.result.message || '同じ商品コードのカードが 2 枚以上ある');
    return { ok: false, reason: 'ambiguous', draft_ids: Array.isArray(ev.result.conflict_draft_ids) ? ev.result.conflict_draft_ids : [] };
  }
  const conflictDraft = ev.result?.conflict_draft_id ?? null;
  if (!conflictDraft) { await release('衝突のカードの番号が無い'); return { ok: false, reason: 'not_conflict', status: 'conflict' }; }
  if (expectedDraftId == null || String(expectedDraftId) !== String(conflictDraft)) {
    await release();
    return { ok: false, reason: 'draft_mismatch', draft_id: conflictDraft };
  }
  if (cardPayloadHash(ev.payload) !== ev.payload_hash) { await release('payload_hash_mismatch'); return { ok: false, reason: 'hash_mismatch' }; }
  let r;
  try {
    r = await link({ event_id: ev.event_id, sku_id: ev.sku_id, schema_version: ev.schema_version, payload: ev.payload }, { draftId: conflictDraft, actor });
    if (!r || r.outcome !== 'linked') throw new Error(`結ぶ処理の答えが分からない: ${JSON.stringify(r)}`);
  } catch (e) {
    if (e && e.ambiguous) {
      // 結ぶまでの間に同じコードのカードが増えた = 結ばない。知らせは「どれか決められない衝突」にする (画面は「結ぶ」を出さない)
      const amb = { ...(ev.result || {}), outcome: 'conflict', draft_id: null, conflict_draft_id: null, conflict_draft_ids: e.draft_ids || [], ambiguous: true,
        message: String(e.message).slice(0, 2000) };
      await release(amb.message, amb);
      return { ok: false, reason: 'ambiguous', draft_ids: amb.conflict_draft_ids };
    }
    await release(String(e && e.message || e).slice(0, 2000));
    throw e;
  }
  const result = { outcome: 'linked', draft_id: r.draft_id, linked_by: actor, conflict_draft_id: conflictDraft, applied: r.applied || [], not_applied: r.not_applied || [] };
  const done = (await db.query('select ops.finish_card_event($1::uuid, $2, $3, $4::jsonb, $5) as ok', [ev.event_id, me, 'done', JSON.stringify(result), null])).rows[0].ok === true;
  return { ok: true, draft_id: r.draft_id, already: !!r.already, applied: result.applied, not_applied: result.not_applied, recorded: done };
}

// ─── Company DB へのつなぎ (product-hub のボード・/new から。試験は差し替える) ───
let clientFactory = openPgClient;
export function __setCompanyDbClientFactory(fn) { clientFactory = fn || openPgClient; }

/**
 * Company DB につないで fn(db) を呼ぶ。つながらない・URL が無い = { ok: false, error } (投げない)
 * role = 'edit' = 画面だけのロール master_edit (COMPANY_DB_MASTER_EDIT_URL) だけ (カードの知らせの取り込み = 書く) /
 *        'read' = master_edit か、無ければ持ち主のロール (段階を読むだけ。⑤-1 の画面の読むだけの接続と同じ)
 */
export async function withCompanyDb(fn, { applicationName = 'portal', statementTimeout = '10s', role = 'read' } = {}) {
  const url = role === 'edit' ? process.env.COMPANY_DB_MASTER_EDIT_URL : (process.env.COMPANY_DB_MASTER_EDIT_URL || process.env.COMPANY_DB_URL);
  if (!url) return { ok: false, error: role === 'edit' ? 'no_edit_url' : 'no_url' };
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

/** ボードを開いたときの取り込み (sweep) をするか。MASTER_EDIT_OPEN = 1 かつ 画面だけのロールの接続がある Render だけ (切替の前は Company DB につながない = 今の動きのまま) */
export const cardSweepEnabled = () => process.env.MASTER_EDIT_OPEN === '1' && !!process.env.COMPANY_DB_MASTER_EDIT_URL && (process.env.PORTAL_VARIANT || 'render').toLowerCase() === 'render';

/** ボードを開いたときの取り込み。apply は product-hub の取り込み。失敗しても投げない (ボードは出す) */
export async function sweepCardOutbox(apply, { limit = 20 } = {}) {
  if (!cardSweepEnabled()) return { ok: true, skipped: 'disabled', results: [] };
  try {
    const r = await withCompanyDb((db) => runCardOutbox(db, apply, { limit }), { applicationName: 'product-hub-card-sweep', role: 'edit' });
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
 *   guide  = 新しい「新商品の登録」へ案内するだけ (段階 new_open かつ 持ち主表 (🆕 広げる道 PR-2 = DB の active・code_behind でない) のハッシュが段階の記録と同じ かつ MASTER_EDIT_OPEN = 1
 *            かつ 🆕 ⑤-3b: 新商品の登録の列 (NEW_ENTRY_KEYS の単品 + セット) が全部 C = 古い新商品の作り方の門が閉じている)
 *   legacy (切替の後) = 🆕 ⑤-3b: 新商品の登録の列にまだ load がある (10/5 は sku_kind・sku_components) = 新しい登録では作れない =
 *            古い作り方の門 (config/master-legacy-entries.mjs の product-hub:screen:/new・owner_match 'all') が開いている = 今までの画面
 *   paused = 切替の途中 (frozen / company_owner で列が全部 C)・持ち主表が記録と違う・段階か持ち主が読めない = 登録は止めている (古い入口も開けない = fail-closed)
 */
let legacyGateFn = null;
/** 試験だけ: 古い新商品の作り方の門の読み方を差し替える (本番は lib/master-legacy-gate.mjs の checkLegacyGate) */
export function __setNewEntryLegacyGate(fn) { legacyGateFn = fn || null; }
/** 古い新商品の作り方の門 (画面の入口 product-hub:screen:/new の列で決める。画面と同じ読み方 = 30 秒の使い回し) */
async function oldCreationGate() {
  try {
    return await (legacyGateFn || checkLegacyGate)({ purpose: 'screen', entry: 'product-hub:screen:/new' });
  } catch (e) { return { readable: false, writable: false, error: String((e && e.message) || e) }; }
}
/**
 * 🆕 広げる道 PR-2: 持ち主表は配った config ではなく DB の active (同じ接続で段階の後に読む・読めない / code_behind = paused)
 */
export async function newEntryGate() {
  if (process.env.MASTER_EDIT_OPEN !== '1') return { mode: 'legacy' };
  const r = await withCompanyDb(async (db) => {
    const phase = await readCutoverPhase(db);
    const owner = phase.readable && phase.phase !== 'legacy_open' ? await readScreenOwnership(db) : null;
    return { phase, owner };
  }, { applicationName: 'product-hub-new-gate', statementTimeout: '5s' });
  if (!r.ok) return { mode: 'paused', phase: null, error: r.error };
  const { phase, owner } = r.value;
  if (phase.readable && phase.phase === 'legacy_open') return { mode: 'legacy' };
  // ⑤-3b: 古い作り方の門と同じ答え (列が全部 C でなければ開いている = 今までの画面)
  const g = await oldCreationGate();
  if (g.readable === true && g.writable === true) return { mode: 'legacy', phase: phase.phase };
  if (g.readable === true && screenOwnerWritable(phase, owner)) return { mode: 'guide', phase: phase.phase };
  return { mode: 'paused', phase: phase.readable ? phase.phase : null, error: phase.error || (owner && !owner.readable ? owner.error : null)
    || (owner && owner.code_behind.length ? `code_behind: ${owner.code_behind.join('・')}` : null) };
}
