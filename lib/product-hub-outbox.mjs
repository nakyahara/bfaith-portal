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

// ─── 名前空間 (0066・Company DB構想 20 §⑤ / §⑩ の PR-3) ───
// 知らせの相手 = entity_kind (sku = SKU のカード / variation_group = まとまりのカード)・entity_id (sku_id / group_product_id)・revision。
// 一意 = (entity_kind, entity_id, kind, revision)。SKU の知らせは revision 1 だけ (今の card_create のまま)。
// 今の取り込み (runCardOutbox / linkCardToExisting = ops.claim_card_events) は SKU の知らせだけを借りる (まとまりの知らせは PR-4 の product-hub が ops.claim_outbox_events で)
export const ENTITY_KINDS = Object.freeze(['sku', 'variation_group']);
/** まとまりの知らせ = 毎回「今のまとまりの全部」の完全なスナップショット (差分の知らせは作らない・設計 R3 High 3) */
export const GROUP_SCHEMA_VERSION = 'ph-group-v1';
export const GROUP_SNAPSHOT_KIND = 'group_snapshot';
export const GROUP_SNAPSHOT_MAX_CHILDREN = 1000;
/** hash は DB (ops.js_stable_sha256) と同じ形 = カードの知らせと同じ */
export const groupPayloadHash = (payload) => sha256(stable(payload));

const isObj = (v) => v !== null && typeof v === 'object' && !Array.isArray(v);
const jsonType = (v) => (v === null ? 'null' : Array.isArray(v) ? 'array' : typeof v === 'object' ? 'object' : typeof v === 'number' ? 'number' : typeof v === 'string' ? 'string' : typeof v === 'boolean' ? 'boolean' : 'undefined');
const keysExactly = (o, keys) => isObj(o) && Object.keys(o).length === keys.length && Object.keys(o).every((k) => keys.includes(k));
const CNTRL = /[\u0000-\u001f\u007f-\u009f]/;
const chars = (s) => [...s].length;
/** PostgreSQL の btrim (既定 = 半角の空白だけ) と同じ */
const btrim = (s) => s.replace(/^ +| +$/g, '');
const ID_RE = /^[1-9][0-9]{0,17}$/;
const codeOk = (s) => typeof s === 'string' && chars(s) >= 1 && chars(s) <= 255 && !CNTRL.test(s) && s === btrim(s);
const nameOk = (s) => typeof s === 'string' && chars(s) >= 1 && chars(s) <= 255 && !CNTRL.test(s);
const labelOk = (s) => typeof s === 'string' && chars(btrim(s)) >= 1 && chars(btrim(s)) <= 100 && !CNTRL.test(s);
const intIn = (v, lo, hi) => typeof v === 'number' && Number.isInteger(v) && v >= lo && v <= hi;

/**
 * ph-group-v1 の形の確かめ (表を読まない)。問題が無ければ null・あれば最初の 1 つの名前。
 * 🚨 DB の ops.group_snapshot_shape_problem (0066) と同じ決まり・同じ順 (試験で同じ見本を両方に通して答えが同じかを見る)。
 * 今の Company DB の値と同じか (子の全部・コード・名前・売価・JAN) は DB だけが見る (ops.group_snapshot_problem = 知らせの insert の trigger)
 */
export function groupSnapshotShapeProblem(p) {
  if (!isObj(p)) return 'payload_not_object';
  if (!keysExactly(p, ['schema', 'revision', 'created_by', 'group', 'axes', 'options', 'children', 'cancelled_children', 'common'])) return 'payload_keys';
  if (p.schema !== GROUP_SCHEMA_VERSION) return 'schema';
  if (!intIn(p.revision, 1, 2147483647)) return 'revision';
  if (typeof p.created_by !== 'string' || chars(p.created_by) < 1 || chars(p.created_by) > 320 || CNTRL.test(p.created_by)) return 'created_by';
  const g = p.group;
  if (!keysExactly(g, ['product_id', 'sku_id', 'code', 'name', 'kind'])) return 'group_keys';
  if (typeof g.product_id !== 'string' || !ID_RE.test(g.product_id)) return 'group_product_id';
  if (g.kind !== 'tag' && g.kind !== 'single') return 'group_kind';
  if (g.kind === 'tag' && g.sku_id !== null) return 'group_sku_id';
  if (g.kind === 'single' && (typeof g.sku_id !== 'string' || !ID_RE.test(g.sku_id))) return 'group_sku_id';
  if (!codeOk(g.code)) return 'group_code';
  if (!nameOk(g.name)) return 'group_name';
  const gcode = g.code.toLowerCase();

  if (!Array.isArray(p.axes) || p.axes.length > 2) return 'axes';
  const axes = [];
  for (const a of p.axes) {
    if (!keysExactly(a, ['axis', 'name'])) return 'axis_keys';
    if (a.axis !== 1 && a.axis !== 2) return 'axis_no';
    if (!labelOk(a.name)) return 'axis_name';
    axes.push(a.axis);
  }
  if (axes.join(',') !== [1, 2].slice(0, axes.length).join(',')) return 'axes_order';

  if (!Array.isArray(p.options)) return 'options';
  const ocodes = new Set(); const oexact = new Set(); const onames = new Set();
  for (const o of p.options) {
    if (!keysExactly(o, ['axis', 'code', 'name', 'sort'])) return 'option_keys';
    if ((o.axis !== 1 && o.axis !== 2) || !axes.includes(o.axis)) return 'option_axis';
    if (typeof o.code !== 'string' || !/^-[A-Za-z0-9]{1,10}$/.test(o.code)) return 'option_code';
    if (!labelOk(o.name)) return 'option_name';
    if (!intIn(o.sort, 0, 999999999)) return 'option_sort';
    const kc = `${o.axis}:${o.code.toLowerCase()}`;
    if (ocodes.has(kc)) return 'option_code_dup';
    ocodes.add(kc); oexact.add(`${o.axis}:${o.code}`);
    const kn = `${o.axis}:${btrim(o.name.normalize('NFKC'))}`;
    if (onames.has(kn)) return 'option_name_dup';
    onames.add(kn);
  }

  if (!Array.isArray(p.children) || p.children.length > GROUP_SNAPSHOT_MAX_CHILDREN) return 'children';
  const ids = new Set(); const codes = new Set(); const combos = new Set();
  for (const c of p.children) {
    if (!keysExactly(c, ['sku_id', 'code', 'name', 'price', 'choices', 'jans'])) return 'child_keys';
    if (typeof c.sku_id !== 'string' || !ID_RE.test(c.sku_id)) return 'child_sku_id';
    if (ids.has(c.sku_id)) return 'child_dup';
    ids.add(c.sku_id);
    if (!codeOk(c.code)) return 'child_code';
    const code = c.code.toLowerCase();
    if (codes.has(code)) return 'child_dup';
    codes.add(code);
    if (!nameOk(c.name)) return 'child_name';
    if (c.price !== null && !intIn(c.price, 0, 999999999999)) return 'child_price';
    if (!Array.isArray(c.jans) || c.jans.length > 5 || c.jans.some((j) => typeof j !== 'string' || !/^([0-9]{8}|[0-9]{13})$/.test(j)) || new Set(c.jans).size !== c.jans.length) return 'child_jans';
    if (!isObj(c.choices)) return 'child_choices';
    const ck = Object.keys(c.choices);
    if (ck.length > 0) {
      if (ck.some((k) => k !== '1' && k !== '2') || ck.length !== axes.length || axes.some((a) => typeof c.choices[String(a)] !== 'string')) return 'child_choices';
      const c1 = c.choices['1']; const c2 = Object.hasOwn(c.choices, '2') ? c.choices['2'] : null;
      if (!oexact.has(`1:${c1}`) || (c2 !== null && !oexact.has(`2:${c2}`))) return 'child_choice_unknown';
      if (code !== gcode + c1.toLowerCase() + (c2 ?? '').toLowerCase()) return 'child_code_not_group_plus_choices';
      const kc = `${c1.toLowerCase()}|${(c2 ?? '').toLowerCase()}`;
      if (combos.has(kc)) return 'child_choice_dup';
      combos.add(kc);
    }
  }

  if (!Array.isArray(p.cancelled_children) || p.cancelled_children.length > GROUP_SNAPSHOT_MAX_CHILDREN) return 'cancelled_children';
  for (const c of p.cancelled_children) {
    if (!keysExactly(c, ['sku_id', 'code'])) return 'cancelled_keys';
    if (typeof c.sku_id !== 'string' || !ID_RE.test(c.sku_id)) return 'cancelled_sku_id';
    if (!codeOk(c.code)) return 'cancelled_code';
    if (ids.has(c.sku_id) || codes.has(c.code.toLowerCase())) return 'cancelled_dup';
    ids.add(c.sku_id); codes.add(c.code.toLowerCase());
  }

  const m = p.common;
  if (!keysExactly(m, ['shipping', 'amazon_url', 'asin', 'official_url', 'reference_urls', 'yahoo'])) return 'common_keys';
  if (!(m.shipping === null || keysExactly(m.shipping, ['code', 'method', 'cost_jpy']))) return 'common_shipping';
  if (['amazon_url', 'asin', 'official_url'].some((f) => !['null', 'string'].includes(jsonType(m[f])))) return 'common_urls';
  if (!Array.isArray(m.reference_urls) || m.reference_urls.some((u) => typeof u !== 'string')) return 'common_reference_urls';
  if (!(m.yahoo === null || (isObj(m.yahoo) && Object.keys(m.yahoo).every((k) => ['price', 'price_sagawa', 'delivery_label', 'category_id', 'path'].includes(k))))) return 'common_yahoo';
  return null;
}

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

/** まとまりの知らせの取り込みの答え → 知らせの状態 (stale = もっと新しい revision をもう取り込んだ = 何もしないで済み) */
const GROUP_OUTCOME_STATUS = Object.freeze({ created: 'done', replaced: 'done', linked: 'done', stale: 'done', conflict: 'conflict' });

/**
 * まとまりの知らせ (entity_kind = variation_group・group_snapshot / ph-group-v1) を取り込む (Company DB構想 20 v7 §⑤・§⑩ の PR-4)。
 * apply = product-hub の取り込み (apps/product-hub/services/cdb-group-intake.js の applyCdbGroupEvent)。
 *   event → { outcome: created | replaced | linked | stale | conflict, draft_id?, conflict_draft_id?, message? }・失敗は throw
 * 借りる = ops.claim_outbox_events (0066・entity_kind を必ず渡す = SKU の知らせを混ぜない)・結果を書く = ops.finish_card_event (借りた人だけ・done は変えない)。
 * 🚨 取り込む前に: hash (DB と同じ stable の sha256)・形 (groupSnapshotShapeProblem = DB と同じ決まり)・知らせの列 (まとまりの番号・revision) と payload が同じか。
 *    違えば取り込まない (failed)。revision の大小は product-hub が決める (今より大きいときだけ全部置き換え・小さい / 同じ = stale で済み)
 * opts: eventId / groupProductId (その知らせだけ)・manual (人が押した = conflict と回数を使い切った failed も試す)・limit・owner
 * 戻り値 = [{ event_id, group_product_id, revision, status, result, error, recorded, attempts }]
 */
export async function runGroupOutbox(db, apply, { eventId = null, groupProductId = null, manual = false, limit = 20, owner = null } = {}) {
  const me = owner || `portal-g:${process.pid}:${crypto.randomUUID().slice(0, 8)}`;
  const claimed = (await db.query(`select event_id::text as event_id, entity_kind, entity_id::text as entity_id, revision, kind, group_product_id::text as group_product_id,
         schema_version, payload, payload_hash, attempts
       from ops.claim_outbox_events($1, $2, 'variation_group', $3::uuid, $4::bigint, $5, $6, $7)`,
  [me, manual ? 'manual' : 'auto', eventId, groupProductId, Math.max(1, Math.min(200, Number(limit) || 20)), CARD_LEASE_SECONDS, CARD_MAX_AUTO_ATTEMPTS])).rows;
  const out = [];
  for (const ev of claimed) {
    let status; let result = null; let error = null;
    const p = ev.payload;
    const prob = ev.kind !== GROUP_SNAPSHOT_KIND || ev.schema_version !== GROUP_SCHEMA_VERSION ? `kind_or_schema: ${ev.kind} / ${ev.schema_version}`
      : groupPayloadHash(p) !== ev.payload_hash ? 'payload_hash_mismatch'
        : groupSnapshotShapeProblem(p)
          || (String(p.group.product_id) !== String(ev.group_product_id) ? 'group_mismatch' : null)
          || (Number(p.revision) !== Number(ev.revision) ? 'revision_mismatch' : null);
    if (prob) {
      status = 'failed'; error = `${prob}: まとまりの知らせの中身が書いたときと違う / 形が違う (取り込まない)`;
    } else {
      try {
        const r = await apply({ event_id: ev.event_id, entity_kind: ev.entity_kind, kind: ev.kind, group_product_id: ev.group_product_id, revision: Number(ev.revision),
          schema_version: ev.schema_version, payload: p });
        if (!r || !GROUP_OUTCOME_STATUS[r.outcome]) throw new Error(`取り込みの答えが分からない: ${JSON.stringify(r)}`);
        status = GROUP_OUTCOME_STATUS[r.outcome];
        result = r;
        if (status === 'conflict') error = String(r.message || '同じコードのカードが product-hub にもうある').slice(0, 2000);
      } catch (e) {
        status = 'failed'; error = String(e && e.message || e).slice(0, 2000);
      }
    }
    let recorded = false;
    try {
      recorded = (await db.query('select ops.finish_card_event($1::uuid, $2, $3, $4::jsonb, $5) as ok',
        [ev.event_id, me, status, result ? JSON.stringify(result) : null, error])).rows[0].ok === true;
    } catch (e) {
      // 結果を書けなかった = 借りの期限の後に次の回がもう一度 (product-hub の側は revision と event_id で冪等)
      console.error(`[product-hub-outbox] まとまりの知らせの結果を書けなかった ${ev.event_id}: ${e && e.message}`);
    }
    out.push({ event_id: ev.event_id, group_product_id: ev.group_product_id, revision: Number(ev.revision), status, result, error, recorded, attempts: Number(ev.attempts) });
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

/**
 * ボードを開いたときの取り込み。apply は product-hub の取り込み。失敗しても投げない (ボードは出す)
 * 🆕 PR-4: applyGroup (まとまりの取り込み) を渡すと、SKU の知らせの後に同じ接続でまとまりの知らせも取り込む。
 *   まとまりの側の失敗 (ロールの流し直しの前 = claim_outbox_events の実行権が無い など) は groupError に返すだけ = SKU の取り込みは今までどおり
 */
export async function sweepCardOutbox(apply, { limit = 20, applyGroup = null } = {}) {
  if (!cardSweepEnabled()) return { ok: true, skipped: 'disabled', results: [], groups: [] };
  try {
    const r = await withCompanyDb(async (db) => {
      const results = await runCardOutbox(db, apply, { limit });
      let groups = []; let groupError = null;
      if (applyGroup) {
        try { groups = await runGroupOutbox(db, applyGroup, { limit }); } catch (e) {
          groupError = String(e && e.message || e);
          console.error(`[product-hub-outbox] まとまりの知らせの sweep の失敗: ${groupError}`);
        }
      }
      return { results, groups, groupError };
    }, { applicationName: 'product-hub-card-sweep', role: 'edit' });
    if (!r.ok) return { ok: false, error: r.error, results: [], groups: [] };
    return { ok: true, results: r.value.results, groups: r.value.groups, ...(r.value.groupError ? { groupError: r.value.groupError } : {}) };
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
