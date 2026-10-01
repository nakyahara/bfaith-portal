/**
 * sku-map-generation.js — Render の受け手: Amazon SKU の対 (mirror_sku_master + mirror_sku_resolved) の世代 (PR ⑦-0)
 * 設計 = AI_reference システム設計/CompanyDB構想/16 §7 v2 H1・M7 / §8 契約 v3 (Codex R1 High 1)。
 * 手順書 (有効にする・間違えて有効にしたとき) = db/company/README.md「Amazon SKU の対の世代の受け口」。
 *
 * なにを:
 *   - 世代の状態 (mirror_sku_map_state・1 行だけ) = 有効になった印 (戻せない)・有効になった時刻・今の世代 (bigint > 0)・ハッシュ・行数
 *   - 有効になる前: 今までの送り方 (世代なし) は今までどおり (router.js の旧い分岐)。
 *     世代つきが届いたら確かめ、**有効にしてよいと 2 つそろったときだけ** 有効にして入れる
 *     (Render の env SKU_MAP_ACTIVATION_ALLOWED=1 と、世代の印の activate: true。どちらかが無ければ 409 = 何も書かない・有効にならない)
 *   - 有効になった後: 世代なし・今より小さい世代・同じ世代で違うハッシュ・両方を空に (clear)・片方だけ を断る (409 / 422・何も書かない)。
 *     同じ世代で同じハッシュ = 何もしない (replayed)
 *   - SKU_MAP_REQUIRE_GENERATION=1 (切替の後に Render に置く) = 状態の行が無くても有効とみなす (DB ファイルを戻した・DATA_DIR が変わった
 *     ときに、世代なしの対を黙って受けない)。行が無いときの世代つきは、上の 2 つがそろったときだけ有効にして記録する
 *     (バックアップから戻した後は、送り手を止め、max(Company DB・miniPC・Render) より大きい世代を決め、許しを一時的に置いて有効にし直す = 手順書)
 *   - 世代つきの body は **SKU の対だけの単独の POST** (sku_master・sku_resolved・sku_map_generation・meta だけ)。
 *     ほかの表が同じ body にあれば 422 (何も書かない)。= 対を断ったときにマスタの部 (products など) を道連れにしない・
 *     「対は入ったのに後の表で 500」も作らない
 *   - 2 表と状態と同期の印 (mirror_sync_status) は SQLite の 1 つの取引 (BEGIN IMMEDIATE) で入れる。入れた後に表から読み直してハッシュを出し直し、違えば巻き戻す
 *   - 初期化 (表・trigger・列) が途中で落ちたら、確かめる口は 503 (capability を出さない)・世代つきの body も 503 (何も書かない)
 * 🚨 状態の行は消さない・世代を下げない (DB の trigger でも止める)。材料の世代 (mirror_material_generations) の
 *    「合わなければ記録を消す」やり方はここでは使わない (Codex R1 High 1)
 * 🚨 世代は bigint で比べる (文字列で比べない)。応答には 10 進の文字列で書く
 * 🚨 trigger は CREATE TRIGGER IF NOT EXISTS なので、同じ名前のまま定義を変えても今ある DB には入らない。
 *    定義を変えるときは名前の版を上げ (_v1 → _v2)、前の名前を RETIRED_STATE_TRIGGERS に足す (新しいのを作った後に DROP する)。
 *    初期化は名前ごとに sqlite_master の定義と照らし、違えば初期化の失敗にする (受け口は 503)。表と trigger の定義 = sku-map-state-schema.js
 */
import {
  SKU_MAP_CANON_FORMAT, SKU_MAP_HASH_RE, parseSkuMapGeneration, skuMapDigest, validateSkuMap, fromMirrorWireRows,
} from '../../lib/sku-map-canonical.js';
import { createSkuMapGenerationTables, verifySkuMapGenerationSchema, STATE_TRIGGERS, SKU_MAP_STATE_TABLE } from './sku-map-state-schema.js';

// 表と trigger を作る・確かめる部品は sku-map-state-schema.js (import を持たない = db.js から読んでも他を連れてこない)。ここからも使えるように出す
export { createSkuMapGenerationTables, verifySkuMapGenerationSchema, STATE_TRIGGERS, SKU_MAP_STATE_TABLE };

/** 送り手が「世代つきで送ってよいか」を確かめる印 (GET /api/sync/sku-map/state の 200・応答の sku_map)。初期化が落ちたときは出さない */
export const SKU_MAP_RECEIVER_CAPABILITY = Object.freeze({ sku_map_generations: 1, format: SKU_MAP_CANON_FORMAT, hash: 'sha256' });

/** 世代つきの body に置いてよい鍵 (SKU の対だけの単独の POST)。meta は同期の印 (mirror_sync_status) に残る */
export const SKU_MAP_BODY_KEYS = Object.freeze(['sku_master', 'sku_resolved', 'sku_map_generation', 'meta']);

/**
 * 受け手の設定 (Render の Environment)。要求のたびに読む (Render は env を変えると再起動するので、効くのは次の起動から)。
 *   activation_allowed = SKU_MAP_ACTIVATION_ALLOWED=1: まだ有効でないときに、activate: true の世代つきで有効にしてよい
 *   require_generation = SKU_MAP_REQUIRE_GENERATION=1: 状態の行が無くても有効とみなす (世代なしの対を断る)
 * どちらも '1' のときだけ (ほかの値は無いのと同じ)
 */
export function skuMapReceiverSettings(env = process.env) {
  return {
    activation_allowed: env.SKU_MAP_ACTIVATION_ALLOWED === '1',
    require_generation: env.SKU_MAP_REQUIRE_GENERATION === '1',
  };
}

/** 断るときの印 (HTTP の番号・理由の記号) */
export class SkuMapReject extends Error {
  constructor(status, code, message, extra = {}) { super(message); this.status = status; this.code = code; this.extra = extra; }
}

/**
 * 今の状態。表が無い (作れなかった) = 一度も有効になっていない (有効にするには表が要る) → { available: false, activated: false }。
 * 読めない (表はあるのに失敗) は投げる = 呼び手は断る (世代なしの対も 503 = 有効かどうか分からないまま入れない)。
 */
export function readSkuMapState(db) {
  let row;
  try {
    row = db.prepare('SELECT * FROM mirror_sku_map_state WHERE id = 1').safeIntegers(true).get();
  } catch (e) {
    if (/no such table/i.test(String(e.message))) return { available: false, activated: false };
    throw e;
  }
  if (!row) return { available: true, activated: false };
  return {
    available: true,
    activated: true,
    activated_at: row.activated_at,
    activation_generation: row.activation_generation,   // bigint
    generation: row.generation,                         // bigint
    format: row.format,
    content_hash: row.content_hash,
    master_rows: Number(row.master_rows),
    component_rows: Number(row.component_rows),
    applied_at: row.applied_at,
  };
}

/** 状態を応答に書く形 (世代は 10 進の文字列) */
export function stateForResponse(s) {
  if (!s || !s.activated) return { activated: false };
  return {
    activated: true,
    activated_at: s.activated_at,
    activation_generation: s.activation_generation.toString(),
    generation: s.generation.toString(),
    format: s.format,
    content_hash: s.content_hash,
    master_rows: s.master_rows,
    component_rows: s.component_rows,
    applied_at: s.applied_at,
  };
}

const reject = (status, code, message, extra) => ({ mode: 'reject', status, code, message, extra: extra || {} });
const plainObject = (v) => v !== null && typeof v === 'object' && !Array.isArray(v);

/**
 * /api/sync の body の SKU の対をどう扱うか決める。**書き込みはしない** (router は他の表より前に呼ぶ = 断るときは何も書かない)。
 * @param {{ initError?: object|null, env?: object }} opts initError = db.js の skuMapGenerationInitError / env = 受け手の設定 (既定は process.env)
 * @returns {{mode:'none'} | {mode:'legacy'} | {mode:'reject', status, code, message, extra} |
 *   {mode:'generation', generation: bigint, claimed, canon, digest, activationPermitted: boolean}}
 *   none = 対に触れない / legacy = 有効になる前の世代なし (router の旧い分岐がそのまま扱う) / generation = applySkuMapGeneration に渡す
 */
export function planSkuMapPair(db, body, { initError = null, env = process.env } = {}) {
  const b = plainObject(body) ? body : {};
  // 鍵があれば世代つき (null も = 形がおかしい 422。「null なので世代なし」と読まない)
  const hasGen = Object.hasOwn(b, 'sku_map_generation');
  const touches = b.sku_master !== undefined || b.sku_resolved !== undefined || hasGen;
  if (!touches) return { mode: 'none' };
  const settings = skuMapReceiverSettings(env);

  // 初期化が途中で落ちた = 状態の表・trigger・列を信じられない → 世代つきは受けない (世代なしは下の決まりのまま)
  if (hasGen && initError) {
    return reject(503, 'sku_map_init_failed', `世代の表の初期化に失敗している → 世代つきは受けられない (再起動で直らなければ手順書): ${initError.message || initError}`, { init_error: initError });
  }
  let state;
  try { state = readSkuMapState(db); } catch (e) {
    return reject(503, 'sku_map_state_unavailable', `世代の状態が読めない: ${e.message}`);
  }
  // 有効になった後 (または SKU_MAP_REQUIRE_GENERATION=1 = 行が無くても有効とみなす) だけ世代なしを断る
  const enforced = state.activated || settings.require_generation;
  if (!enforced && !hasGen) return { mode: 'legacy' };   // 今までどおり
  if (hasGen && !state.available) return reject(503, 'sku_map_state_unavailable', '世代の状態の表が無い (初期化の失敗) → 世代つきは受けられない');

  const current = stateForResponse(state);
  const masterRows = b.sku_master, resolvedRows = b.sku_resolved;
  // 世代つきは SKU の対だけの単独の POST (他の表と相乗りしない = 対を断ってもマスタの部を道連れにしない・対だけ入って後で 500 を作らない)
  if (hasGen) {
    const extraKeys = Object.keys(b).filter((k) => !SKU_MAP_BODY_KEYS.includes(k));
    if (extraKeys.length || !(b.meta === undefined || b.meta === null || plainObject(b.meta))) {
      return reject(422, 'sku_map_body_not_standalone',
        `世代つきの SKU の対は単独で送る (置いてよい鍵 = ${SKU_MAP_BODY_KEYS.join('・')}。meta はオブジェクト)${extraKeys.length ? `: 余計な鍵 ${extraKeys.join(', ')}` : ': meta がオブジェクトでない'}`,
        { current, extra_keys: extraKeys });
    }
  }
  const meta = plainObject(b.meta) ? b.meta : {};
  // 両方を空に (clear の印・空の配列) は有効になった後・世代つきでは受けない (0 件の世代は入れない。16 §3 #8)
  if (meta.clear_sku_master === true || meta.clear_sku_resolved === true
    || (Array.isArray(masterRows) && masterRows.length === 0) || (Array.isArray(resolvedRows) && resolvedRows.length === 0)) {
    return reject(422, 'sku_map_clear_forbidden', 'SKU の対を空にする送り方は受けない (有効になった後・世代つき)', { current });
  }
  if (!hasGen) {
    const why = state.activated ? '有効になった後は' : 'SKU_MAP_REQUIRE_GENERATION=1 なので (状態の行は無いが有効とみなす)';
    return reject(409, 'sku_map_generation_required', `${why}世代 (sku_map_generation) の無い SKU の対を受けない`, { current, require_generation: settings.require_generation });
  }

  const genRaw = b.sku_map_generation;
  if (!plainObject(genRaw)) return reject(422, 'sku_map_generation_invalid', 'sku_map_generation がオブジェクトでない', { current });
  const generation = parseSkuMapGeneration(genRaw.generation);
  if (generation === null) return reject(422, 'sku_map_generation_invalid', `世代が 1 以上の整数 (bigint の範囲) でない: ${JSON.stringify(genRaw.generation ?? null)}`, { current });
  if (genRaw.format !== SKU_MAP_CANON_FORMAT) return reject(422, 'sku_map_format_unsupported', `並べ方の版が違う: ${JSON.stringify(genRaw.format ?? null)} (受けるのは ${SKU_MAP_CANON_FORMAT})`, { current });
  if (typeof genRaw.content_hash !== 'string' || !SKU_MAP_HASH_RE.test(genRaw.content_hash)) return reject(422, 'sku_map_generation_invalid', 'content_hash が 16 進の小文字 64 文字でない', { current });
  if (!Number.isSafeInteger(genRaw.master_rows) || !Number.isSafeInteger(genRaw.component_rows)) return reject(422, 'sku_map_generation_invalid', 'master_rows / component_rows が整数でない', { current });
  const claimed = { generation, format: genRaw.format, content_hash: genRaw.content_hash, master_rows: genRaw.master_rows, component_rows: genRaw.component_rows };
  const received = { generation: generation.toString(), content_hash: claimed.content_hash };

  if (!Array.isArray(masterRows) || !Array.isArray(resolvedRows)) {
    return reject(422, 'sku_map_pair_incomplete', 'sku_master と sku_resolved はいつも対で送る (片方だけ・配列でない は受けない)', { current, received });
  }

  const wire = fromMirrorWireRows({ sku_master: masterRows, sku_resolved: resolvedRows });
  const issues = [...wire.issues, ...validateSkuMap(wire)].slice(0, 20);
  if (issues.length) return reject(422, 'sku_map_rows_invalid', `行が受ける決まりに合わない (${issues.length}${issues.length >= 20 ? ' 件以上' : ' 件'})`, { current, received, issues });
  if (claimed.master_rows !== wire.master.length || claimed.component_rows !== wire.components.length) {
    return reject(422, 'sku_map_count_mismatch', `行数が世代の印と違う (親 ${wire.master.length} / 印 ${claimed.master_rows}・構成 ${wire.components.length} / 印 ${claimed.component_rows})`, { current, received });
  }
  let digest;
  try { digest = skuMapDigest(wire); } catch (e) {
    return reject(422, 'sku_map_rows_invalid', `決まった並べ方にできない: ${e.message}`, { current, received });
  }
  if (digest.content_hash !== claimed.content_hash) {
    return reject(422, 'sku_map_hash_mismatch', '届いた行から出し直したハッシュが世代の印と違う', { current, received, recomputed_content_hash: digest.content_hash });
  }
  // 有効にする許し (まだ有効でないときだけ見る。有効になった後の activate は見ない)。
  //   形・ハッシュを全部確かめた後に見る = この 409 は「中身は受けられる形だった・何も書いていない」の意味 (影運転が間違えて本番に送っても有効にならない)
  const activationPermitted = settings.activation_allowed && genRaw.activate === true;
  if (!state.activated && !activationPermitted) {
    const missing = [];
    if (!settings.activation_allowed) missing.push('env SKU_MAP_ACTIVATION_ALLOWED=1 (Render)');
    if (genRaw.activate !== true) missing.push('sku_map_generation.activate = true (送り手)');
    return reject(409, 'sku_map_activation_not_allowed', `まだ有効になっていない受け口を有効にする許しが無い (足りない: ${missing.join('・')}) → 何も書かない・有効にしない`, { current, received, validated: true, missing });
  }
  return { mode: 'generation', generation, claimed, canon: { master: wire.master, components: wire.components }, digest, activationPermitted };
}

/** mirror の 2 表から読み直した決まった形の行 */
function readStoredCanon(db) {
  const master = db.prepare(`SELECT seller_sku, 商品名 AS name, source_created_at AS created_at, source_updated_at AS updated_at
    FROM mirror_sku_master`).all();
  const components = db.prepare(`SELECT seller_sku, ne_code, quantity, sort_order, component_created_at AS created_at, component_updated_at AS updated_at
    FROM mirror_sku_resolved`).all();
  return { master, components };
}

/** 今 mirror に入っている中身のハッシュ (形が決まりに合わなければ null) */
export function storedSkuMapHash(db) {
  try { return skuMapDigest(readStoredCanon(db)).content_hash; } catch { return null; }
}

/** 2 表を入れ替えて、表から読み直したハッシュが世代と同じか確かめる (取引の中で呼ぶ。違えば投げる = 巻き戻る) */
function replaceTables(db, plan, syncedAt) {
  db.exec('DELETE FROM mirror_sku_resolved');
  db.exec('DELETE FROM mirror_sku_master');
  const insMaster = db.prepare(`INSERT INTO mirror_sku_master (seller_sku, 商品名, source_created_at, source_updated_at, synced_at)
    VALUES (?,?,?,?,?)`);
  const byMaster = new Map();
  for (const m of plan.canon.master) {
    insMaster.run(m.seller_sku, m.name, m.created_at, m.updated_at, syncedAt);
    byMaster.set(m.seller_sku, m);
  }
  const insComp = db.prepare(`INSERT INTO mirror_sku_resolved (
    seller_sku, ne_code, quantity, source, 商品名, source_updated_at, sort_order, component_created_at, component_updated_at, synced_at
  ) VALUES (?,?,?,?,?,?,?,?,?,?)`);
  for (const c of plan.canon.components) {
    const m = byMaster.get(c.seller_sku);
    insComp.run(c.seller_sku, c.ne_code, c.quantity, 'master', m.name, m.updated_at, c.sort_order, c.created_at, c.updated_at, syncedAt);
  }
  const stored = storedSkuMapHash(db);
  if (stored !== plan.claimed.content_hash) {
    throw new SkuMapReject(500, 'sku_map_stored_hash_mismatch', `入れた後の表から出し直したハッシュが世代と違う → 巻き戻した (${stored ?? '形が合わない'})`);
  }
}

/**
 * 世代つきの対を入れる。SQLite の 1 つの取引 (BEGIN IMMEDIATE) で: 今の状態を読み直す (CAS) → 断る / 何もしない / 2 表を入れ替えて状態を進める
 * → recordInSameTx (同期の印) を呼ぶ。断るときは SkuMapReject を投げる (取引は巻き戻る = 何も書かない)。
 * @param {object} plan planSkuMapPair の mode = 'generation'
 * @param {{ syncedAt: string, nowIso?: string, recordInSameTx?: (db) => void }} opts
 *   syncedAt = mirror の synced_at (router の now) / nowIso = 状態の時刻 / recordInSameTx = 同じ取引で書くもの (router の last_sync・meta。落ちれば全部巻き戻る)
 * @returns 応答の sku_map (part・result・世代・ハッシュ・行数)
 */
export function applySkuMapGeneration(db, plan, { syncedAt, nowIso = new Date().toISOString(), recordInSameTx = null }) {
  const run = db.transaction(() => {
    const s = readSkuMapState(db);
    if (!s.available) throw new SkuMapReject(503, 'sku_map_state_unavailable', '世代の状態の表が無い');
    const g = plan.generation;
    let out;
    if (!s.activated) {
      // plan の後に状態が変わっていても (手順書の戻しの直後など)、許しが無ければ有効にしない
      if (plan.activationPermitted !== true) {
        throw new SkuMapReject(409, 'sku_map_activation_not_allowed', 'まだ有効になっていない受け口を有効にする許しが無い → 何も書かない・有効にしない', { current: stateForResponse(s) });
      }
      replaceTables(db, plan, syncedAt);
      db.prepare(`INSERT INTO mirror_sku_map_state
        (id, activated, activated_at, activation_generation, generation, format, content_hash, master_rows, component_rows, applied_at)
        VALUES (1, 1, ?, ?, ?, ?, ?, ?, ?, ?)`)
        .run(nowIso, g, g, plan.claimed.format, plan.claimed.content_hash, plan.claimed.master_rows, plan.claimed.component_rows, nowIso);
      out = { result: 'activated', state: readSkuMapState(db) };
    } else {
      const current = stateForResponse(s);
      const received = { generation: g.toString(), content_hash: plan.claimed.content_hash };
      if (g < s.generation) {
        throw new SkuMapReject(409, 'sku_map_generation_stale', `今の世代 ${s.generation} より古い世代 ${g} は受けない`, { current, received });
      }
      if (g === s.generation) {
        if (plan.claimed.content_hash !== s.content_hash || plan.claimed.format !== s.format
          || plan.claimed.master_rows !== s.master_rows || plan.claimed.component_rows !== s.component_rows) {
          throw new SkuMapReject(409, 'sku_map_generation_conflict', `同じ世代 ${g} で中身 (ハッシュ) が違う`, { current, received });
        }
        // 同じ世代・同じハッシュ = 何もしない。ただし表が世代の中身と違っていたら (手で書き換えた等)、同じ中身を入れ直す (状態は変えない)
        if (storedSkuMapHash(db) === s.content_hash) out = { result: 'replayed', state: s };
        else {
          replaceTables(db, plan, syncedAt);
          out = { result: 'repaired', state: s };
        }
      } else {
        replaceTables(db, plan, syncedAt);
        const r = db.prepare(`UPDATE mirror_sku_map_state
          SET generation = ?, format = ?, content_hash = ?, master_rows = ?, component_rows = ?, applied_at = ?
          WHERE id = 1 AND generation = ?`)
          .run(g, plan.claimed.format, plan.claimed.content_hash, plan.claimed.master_rows, plan.claimed.component_rows, nowIso, s.generation);
        if (r.changes !== 1) throw new SkuMapReject(409, 'sku_map_generation_stale', '状態が途中で変わった (CAS)', { current, received });
        out = { result: 'applied', state: readSkuMapState(db) };
      }
    }
    if (recordInSameTx) recordInSameTx(db);
    return out;
  });
  // 応答に書く状態は取引の中で読んだもの (取引の後に読み直さない)
  const { result, state: s } = run.immediate();
  return {
    part: 'sku_map',
    result,
    capability: SKU_MAP_RECEIVER_CAPABILITY,
    format: s.format,
    generation: s.generation.toString(),
    content_hash: s.content_hash,
    master_rows: s.master_rows,
    component_rows: s.component_rows,
    activated: true,
    activated_at: s.activated_at,
  };
}

/** 断ったときの応答の body。503 (受け手が世代を扱えない) のときは capability を載せない (送り手は capability を見て世代つきで送る) */
export function skuMapRejectBody(r) {
  return {
    error: r.code,
    message: r.message,
    sku_map: {
      part: 'sku_map', result: 'rejected', code: r.code,
      ...(r.status === 503 ? {} : { capability: SKU_MAP_RECEIVER_CAPABILITY }),
      ...(r.extra || {}),
    },
  };
}
