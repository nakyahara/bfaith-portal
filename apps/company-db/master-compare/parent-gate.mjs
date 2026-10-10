/**
 * parent-gate.mjs — 代表 (親) の生の数えと門 (0068・AI_reference CompanyDB構想/20_代表の正本を自社DBへ_設計 v7 §②・§⑥・§⑩ の PR-6)
 *
 * 何をするか:
 *   1. parentObservations = 照合 ② が読んだ NE の完全な取得 (compare-ne の nModelOf) から、単品の代表の観測を作る (DB は NE を読めない = 観測は渡す)
 *   2. recordParentGate = 照合 ② の最後 (結果の JSON を書いた後 = 封をした回) に watch_writer で ops.record_parent_gate を呼ぶ。数えは DB が自分の今の値で数える
 *      (6 つ: parent_mismatch / parent_incomparable / parent_ambiguous / parent_missing / parent_two_level / parent_loop・単品だけ・登録の状態 × 品目の状態の判定表 = §⑥)
 *   3. parentTrouble = 朝の要約の先頭の ⚠️ (daily-sync の isWarnSummary は先頭の ⚠️ だけを見る = #1667 R2 の教訓)。
 *      ロードの完了 (① の行)・照合の失敗 (② の行) とは別に「代表のずれ」と「新しい NE 登録の CSV を閉じたか」を出す
 * 門 (DB の ops.parent_gate_state / 品目の表の trigger):
 *   products.parent の持ち主 (DB の active) が company のときだけ、数えが 0 でない朝は新しい一般の NE 登録の CSV (作る・配る) を閉じる。
 *   🚨 load の間 (今) は数えて知らせるだけ = 今の単品の登録を止めない (設計 v7 §⑩ PR-6「持ち主が load の間は知らせだけ・門は company の後」)。
 *   夜間ロードは止めない。配ったファイルの再取得・申告・照合・取り込めなかった商品だけの作り直し・廃止は門が閉じていても通す (自己デッドロックを防ぐ)
 * 差を残す承認 (accept_difference) では減らない (承認の台帳を読まない・DB の承認の trigger も代表には断る)
 */

import { checkNeFetchCounts, NE_FETCH_KINDS } from '../../warehouse/ne-fetch-counts.js';

export const PARENT_OBS_FORMAT = 'parent-obs-v1';
/** 6 つの数え (DB の ops.parent_raw_gate の counts の鍵と同じ) */
export const PARENT_COUNT_KEYS = Object.freeze(['parent_mismatch', 'parent_incomparable', 'parent_ambiguous', 'parent_missing', 'parent_two_level', 'parent_loop']);
export const PARENT_COUNT_JA = Object.freeze({ parent_mismatch: 'NE と違う', parent_incomparable: '比べられない', parent_ambiguous: '当たる親が 2 つ以上',
  parent_missing: '社内に親が無い', parent_two_level: '2 段', parent_loop: '循環' });
const MAX_CODE = 200;

/** 取得の件数の記録の完了の時刻 (sync_meta = UTC の 'YYYY-MM-DD HH:MM:SS') → RFC 3339 ('…Z')。読めない = null */
export function fetchTimeRfc3339(t) {
  return typeof t === 'string' && /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(t) ? `${t.replace(' ', 'T')}Z` : null;
}

/**
 * NE の完全な取得 (nModelOf の m = norm → { code, kind, cols.parent = repState, repRaw }) → DB に渡す観測。
 *   rows = [code_norm, 'single', 'ok' | 'unknown', 代表の norm (親なし・自分自身 = null), 代表の原文] / セット = [code_norm, 'set', null, null, null]
 *   untrusted = 正規化の衝突・取込の整合で保持した商品 (DB は「比べられない」に数える)
 *   complete・trust = 🆕 #1676 Codex R4: parentObsTrust (許可の一覧) の答え。trust = { 確かめの名前: 'ok' | 理由 }・complete = 全部 'ok' のときだけ true。
 *     trust を渡さない = 確かめていない = complete:false (既定は閉じる)
 *   rep_collided = 🆕 #1676 Codex R1 High: NE のコードの元の書き方 (raw_ne_code_spellings の代表の名前空間) で書き方が 2 つ以上の代表 (DB は「曖昧」に数える)。
 *     代表の原文 (rows の 5 つめ) は compare-ne の repRawOf = 代表商品コード_src から戻した元の書き方 (保存の値は小文字)
 *   rep_spellings = 🆕 #1676 Codex R2 High: その台帳を読めたか ({ state: 'ok' } / { state: 'unavailable', reason })。読めない回の rep_collided の空は「衝突なし」ではない
 *     = 照合は記録しない (DB の ops.record_parent_gate も断る)・drift-list は止まる。渡されない = 読めない (not_read) に倒す
 *   🚨 長すぎるコード (200 字超) は送らない (= DB では「取得に無い」= 比べられない)・長すぎる代表は unknown (比べられない)
 */
export function parentObservations(nm, { untrusted = [], trust = null, repSpellings = { state: 'unavailable', reason: 'not_read', collided: [] } } = {}) {
  const rows = [];
  for (const [norm, n] of [...nm].sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0))) {
    if (!norm || norm.length > MAX_CODE) continue;
    if (n.kind !== 'single') { rows.push([norm, 'set', null, null, null]); continue; }
    const st = n.cols && n.cols.parent;
    const ok = !!st && st.raw === 'value' && st.validity === 'ok' && (st.value == null || (typeof st.value === 'string' && st.value.length <= MAX_CODE));
    const rep = ok ? (st.value ?? null) : null;
    const raw = rep == null ? null : (typeof n.repRaw === 'string' && n.repRaw && n.repRaw.length <= MAX_CODE ? n.repRaw : rep);
    rows.push([norm, 'single', ok ? 'ok' : 'unknown', rep, raw]);
  }
  const u = [...new Set([...untrusted].filter((x) => typeof x === 'string' && x && x.length <= MAX_CODE))].sort();
  const okSp = repSpellingsOk(repSpellings);   // 🆕 #1676 Codex R5: 形が完全に正しい ok だけ (許可の一覧と同じ確かめ)
  const rc = okSp ? [...new Set(repSpellings.collided.filter((x) => x.length <= MAX_CODE))].sort() : [];
  const sp = okSp ? { state: 'ok' } : { state: 'unavailable', reason: String((repSpellings && repSpellings.reason) || 'not_read').slice(0, 100) };
  // 🆕 #1676 Codex R4: 信用 = 許可の一覧の全部が明示的に ok のときだけ complete (渡さない = 確かめていない = 閉じる)
  const tr = trust && typeof trust === 'object' && trust.checks ? trust : parentObsTrust({ rep_spellings: repSpellings });
  const complete = tr.complete === true;
  const inc = complete ? {} : { incomplete_reason: tr.reasons.join(',').slice(0, 100) || 'not_checked' };
  return { format: PARENT_OBS_FORMAT, complete, ...inc, trust: { ...tr.checks }, untrusted: u, rep_collided: rc, rep_spellings: sp, rows };
}

/**
 * 🆕 #1676 Codex R4: 親の観測の信用 = 許可の一覧 (allowlist)。全部の確かめが**明示的に ok** のときだけ complete。どれか 1 つでも false / 未定義 / 読めない = 閉じる。
 *   知らない印 (一覧に無い入力の鍵) が渡された = 一覧に足すまでは閉じる (既定は閉じる)。DB の ops.record_parent_gate も同じ一覧 (trust の鍵の集合 = 全部 'ok') で断る。
 *   入力 (signals):
 *     fetch_counts = 今朝の取得の件数 (readNeFetchCounts の答え): 単品・セットとも ok・同じ取得 (fingerprint 一致)・落とした行 0・重なり 0
 *     integrity    = 取込の整合 (compare-ne の neIntegrity の答え): 読める・行が落ちていない (absenceUntrusted / C1 のセットの行の欠けが無い)
 *     kind_gate    = 照合 ② の区分のゲートの数 (integrity_untrusted = 0 = 新商品の許可と同じ厳しさ)
 *     rep_spellings = NE のコードの元の書き方 (代表) の台帳 (repSpellingsOf の答え) が ok
 * 戻り値 { complete, checks: { fetch_counts, integrity, kind_gate_integrity, rep_spellings, (知らない印) }, reasons: [ok でない確かめの理由] }
 * 🆕 #1676 Codex R5 High: 4 つの信号それぞれ、**形が完全に正しいときだけ** ok (型・範囲・形の不正 = どれも ok にしない):
 *   fetch_counts = 2 つの種類の記録が取得の契約の確かめ (checkNeFetchCounts = 書く時と読む時と同じ部品) に通る・版は 64 桁の 16 進で 3 つとも同じ
 *   integrity    = neIntegrity の答えの形 (intBlocked = Map・c2Form = 真偽・2 つの落ちの印が false そのもの。数え・配列の中身は neIntegrity が厳しく確かめる)
 *   kind_gate    = 0 そのもの (負でない safe integer の 0。文字の '0'・小数・負・無い = ok にしない)
 *   rep_spellings = repSpellingsOk (state 'ok' + collided が文字の配列)
 */
export const PARENT_TRUST_INPUTS = Object.freeze(['fetch_counts', 'integrity', 'kind_gate', 'rep_spellings']);
export const PARENT_TRUST_KEYS = Object.freeze(['fetch_counts', 'integrity', 'kind_gate_integrity', 'rep_spellings']);
const isObj = (v) => !!v && typeof v === 'object' && !Array.isArray(v);
const FP_RE = /^[0-9a-f]{64}$/;
const why = (v, d = 'invalid') => (typeof v === 'string' && v ? v : typeof v === 'number' && Number.isFinite(v) ? String(v) : d);
/** 代表の名前空間の書き方の台帳の答え (repSpellingsOf) が「読めた」の形そのものか: state 'ok'・collided = 空でない文字の配列 */
export function repSpellingsOk(sp) {
  return isObj(sp) && sp.state === 'ok' && Array.isArray(sp.collided) && sp.collided.every((x) => typeof x === 'string' && x.length > 0);
}
const TRUST_CHECKS = Object.freeze({
  fetch_counts: (s) => {
    const fc = s.fetch_counts;
    if (!isObj(fc)) return 'fetch_counts_unreadable';
    if (fc.fetch_fingerprint_mismatch !== undefined) return 'fetch_counts_fingerprint_mismatch';
    for (const kind of NE_FETCH_KINDS) {
      const r = fc[kind];
      if (!isObj(r) || r.ok !== true) return `fetch_counts_unavailable:${kind}:${isObj(r) ? why(r.reason, 'unreadable') : 'unreadable'}`;
      // 取得の記録の契約 (版・種類・時刻・件数が負でない safe integer・式・ページ) を読む時と同じ部品でもう一度 (渡された ok を信じない)
      if (!isObj(r.counts) || checkNeFetchCounts(kind, r.counts).length) return `fetch_counts_invalid:${kind}`;
      const c = r.counts;
      if (c.dropped_no_code + c.dropped_missing_fields !== 0) return `fetch_rows_dropped:${kind}`;
      if (c.write_attempts !== c.stored_rows) return `fetch_rows_overwritten:${kind}`;
    }
    if (fc.ok !== true) return 'fetch_counts_unavailable';
    if (typeof fc.fetch_fingerprint !== 'string' || !FP_RE.test(fc.fetch_fingerprint)
      || fc.products.counts.fetch_fingerprint !== fc.fetch_fingerprint || fc.setproducts.counts.fetch_fingerprint !== fc.fetch_fingerprint) {
      return 'fetch_counts_fingerprint_mismatch';
    }
    return 'ok';
  },
  integrity: (s) => {
    const i = s.integrity;
    if (!isObj(i) || !(i.intBlocked instanceof Map) || typeof i.c2Form !== 'boolean') return 'integrity_unreadable';
    if (i.componentsUntrusted !== false) return 'c1_set_rows_dropped';
    if (i.absenceUntrusted !== false) return 'ne_rows_dropped';
    return 'ok';
  },
  kind_gate_integrity: (s) => {
    const k = s.kind_gate;
    if (!isObj(k)) return 'kind_gate_integrity_untrusted:none';
    const v = k.integrity_untrusted;
    return Number.isSafeInteger(v) && v === 0 ? 'ok' : `kind_gate_integrity_untrusted:${typeof v === 'number' && Number.isFinite(v) ? v : 'invalid'}`;
  },
  rep_spellings: (s) => (repSpellingsOk(s.rep_spellings) ? 'ok' : `rep_spellings_unavailable:${!isObj(s.rep_spellings) ? 'none' : s.rep_spellings.state === 'ok' ? 'invalid_shape' : why(s.rep_spellings.reason, 'none')}`),
});
export function parentObsTrust(signals = {}) {
  const checks = {};
  for (const k of PARENT_TRUST_KEYS) {
    let v; try { v = TRUST_CHECKS[k](signals || {}); } catch { v = `${k}_unreadable`; }
    checks[k] = typeof v === 'string' && v ? v.slice(0, 100) : `${k}_unreadable`;
  }
  for (const k of Object.keys(signals || {})) if (!PARENT_TRUST_INPUTS.includes(k)) checks[`unknown:${k}`.slice(0, 100)] = 'unknown_signal';   // 一覧に無い印 = 閉じる
  const reasons = Object.entries(checks).filter(([, v]) => v !== 'ok').map(([k, v]) => (k.startsWith('unknown:') ? k : v));
  return { complete: reasons.length === 0, checks, reasons };
}
/** 観測の信用の確かめ (DB の ops.record_parent_gate と同じ決まり): complete = true・trust の鍵 = 許可の一覧と同じ・全部 'ok'。問題の一覧 (空 = ok) */
export function trustProblems(obs) {
  const t = obs && obs.trust;
  if (!obs || obs.complete !== true) return [String((obs && obs.incomplete_reason) || 'not_complete')];
  if (!t || typeof t !== 'object') return ['trust_missing'];
  const keys = Object.keys(t).sort();
  if (keys.join(',') !== [...PARENT_TRUST_KEYS].sort().join(',')) return [`trust_keys:${keys.join('|')}`.slice(0, 100)];
  return keys.filter((k) => t[k] !== 'ok').map((k) => String(t[k]));
}

/**
 * compare-ne の resolveNeCodes の答え → 代表の名前空間の書き方の台帳の状態 (#1676 Codex R2 High)。
 *   読めた = { state: 'ok', collided: [書き方が 2 つ以上の代表の norm] } / 読めない (未収集・行の数が違う・知らない版 ほか) = { state: 'unavailable', reason }
 *   🆕 #1676 Codex R3 High 1: 代表が 1 件でも invalid (壊れた記録・全角・正規化の不一致 = 書き方を確かめられない) = 台帳全体を unavailable (理由 invalid_rep_spellings:件数)
 *     = その回は記録しない (保守的。次の正常な回で開く)
 *   🆕 #1676 Codex R5 High 2: 壊れた行 (resolveNeCodes の damaged = 空・正規化できない code_norm・配列でない / 空の配列 / 文字でない / code_norm と合わない書き方・知らない種類) が
 *     1 行でもある = 台帳全体を unavailable (理由 damaged_spellings:件数。商品の側の行でも = 台帳そのものが壊れている)。
 *     damaged を持たない答え・entries の形が違う答え = resolveNeCodes の答えではない = unavailable (明示的に ok のときだけ)
 */
const ENTRY_KINDS = new Set(['product', 'rep']);
const ENTRY_STATES = new Set(['ok', 'collided', 'invalid']);
export function repSpellingsOf(neCodes) {
  if (!neCodes || neCodes.ok !== true) return { state: 'unavailable', reason: why(neCodes && neCodes.reason, 'not_read'), collided: [] };
  if (!Number.isSafeInteger(neCodes.damaged) || neCodes.damaged < 0) return { state: 'unavailable', reason: 'damaged_unknown', collided: [] };
  if (neCodes.damaged > 0) return { state: 'unavailable', reason: `damaged_spellings:${neCodes.damaged}`, collided: [] };
  if (!Array.isArray(neCodes.entries) || !neCodes.entries.every((e) => isObj(e) && ENTRY_KINDS.has(e.kind) && ENTRY_STATES.has(e.state) && typeof e.code_norm === 'string' && e.code_norm.length > 0)) {
    return { state: 'unavailable', reason: 'entries_invalid', collided: [] };
  }
  const invalid = neCodes.entries.filter((e) => e.kind === 'rep' && e.state === 'invalid').length;
  if (invalid > 0) return { state: 'unavailable', reason: `invalid_rep_spellings:${invalid}`, collided: [] };
  return { state: 'ok', collided: neCodes.entries.filter((e) => e.kind === 'rep' && e.state === 'collided').map((e) => e.code_norm) };
}

const msg = (e) => String((e && e.message) || e).replace(/\s+/g, ' ').slice(0, 200);
const FN_RECORD = "select to_regprocedure('ops.record_parent_gate(text, jsonb, text, text, jsonb)') is not null as ok";

/**
 * 読むだけで数える (書く接続が無い・記録が落ちた朝の要約のため / drift-list)。読み取りだけの取引。戻り値 = { counts, counted, excluded, samples, owner, gate, items? }
 * @param {{ query: Function }} db  watcher の接続 (pg の adapter / PGlite)
 */
export async function readParentCounts(db, obs, { detail = false } = {}) {
  const g = await readGateState(db);
  await db.query('begin transaction read only');
  try {
    const r = (await db.query('select ops.parent_raw_gate(1, $1::jsonb, $2) as r', [JSON.stringify(obs), !!detail])).rows[0];
    return { ...r.r, owner: ownerOf(g), gate: g };
  } finally { try { await db.query('rollback'); } catch { /* */ } }
}
/** 門の状態だけ (軽い・重い数えと別に読む = 数えが落ちても持ち主は分かる。#1676 Codex R1 Medium) */
export async function readGateState(db) {
  return (await db.query('select ops.parent_gate_state() as g')).rows[0].g;
}
const ownerOf = (g) => (g && typeof g.enforced === 'boolean' ? (g.enforced ? 'company' : 'load') : undefined);

/**
 * 照合 ② の最後に 1 回 (結果の JSON を書いた後 = evidenceSha256)。照合そのものは失敗にしない (状態を返す)。
 *   ok = 記録した / not_applied = 関数が無いと確かめた (0068 の前) / not_configured = 書く接続が無い / failed = 落ちた / no_fetch = 取得の世代・時刻・材料の世代が無い
 *   ok 以外でも readDb があれば読むだけで数える (要約に数を出す = 記録はしない = 持ち主が company なら門は閉じたまま)
 * @param {(() => Promise<object>)|null} getWriter  書く接続 (watch_writer)
 * @param {{ compareRunId: string, parentObs: { obs, fetch, material_generation_id }, evidenceSha256: string, readDb?: object|null }} p
 */
export async function recordParentGate(getWriter, { compareRunId, parentObs, evidenceSha256, readDb = null }) {
  if (!parentObs || !parentObs.obs) return { state: 'no_obs' };
  // 🆕 #1676 Codex R2 High: 書き方の台帳を読めない回 = 記録しない (0 件の記録で門・widen を開けない = 一番新しい記録は前の回のまま = company なら閉じたまま)。照合そのものは止めない
  const sp = parentObs.obs.rep_spellings;
  const spOk = !!sp && sp.state === 'ok';
  const withCounts = async (out) => {
    if (!readDb || out.state === 'not_applied') return out;
    // 門の状態 (持ち主) を先に別に読む = 重い数えの読み直しが落ちても「持ち主は夜間ロード」と取り違えない (#1676 Codex R1 Medium)
    let base = out;
    try { const g = await readGateState(readDb); base = { ...out, owner: ownerOf(g), gate: g }; } catch (e) { base = { ...out, state_error: msg(e) }; }
    try { return { ...base, ...(await readParentCounts(readDb, parentObs.obs)) }; } catch (e) { return { ...base, read_error: msg(e) }; }
  };
  const f = parentObs.fetch || {};
  if (!parentObs.material_generation_id || !f.generation_id || !f.raw_hash || !f.products_complete_at || !f.setproducts_complete_at) {
    return withCounts({ state: 'no_fetch' });
  }
  // 🆕 #1676 Codex R3 High 2・R4: 観測の信用 = 許可の一覧の全部が ok のときだけ記録する (取得の件数・取込の整合・区分のゲート・台帳。DB も同じ一覧で断る)
  const tp = trustProblems(parentObs.obs);
  if (!spOk || tp.length) {
    if (readDb) { try { if (!(await readDb.query(FN_RECORD)).rows[0].ok) return { state: 'not_applied' }; } catch { /* 有無が分からない = 記録しないことは同じ */ } }
    if (spOk) return withCounts({ state: 'ne_untrusted', reason: tp.join(',').slice(0, 100) });
    return withCounts({ state: 'no_spellings', reason: (sp && sp.reason) || 'not_read' });
  }
  if (!getWriter) {
    if (!readDb) return { state: 'not_configured' };
    try { if (!(await readDb.query(FN_RECORD)).rows[0].ok) return { state: 'not_applied' }; } catch (e) { return { state: 'failed', stage: 'presence', error: msg(e) }; }
    return withCounts({ state: 'not_configured' });
  }
  let w;
  try {
    w = await getWriter();
    if (!(await w.query(FN_RECORD)).rows[0].ok) return { state: 'not_applied' };
  } catch (e) { return withCounts({ state: 'failed', stage: 'presence', error: msg(e) }); }
  try {
    const r = (await w.query('select ops.record_parent_gate($1, $2::jsonb, $3, $4, $5::jsonb) as r',
      [compareRunId, JSON.stringify(f), parentObs.material_generation_id, evidenceSha256, JSON.stringify(parentObs.obs)])).rows[0].r;
    return { state: 'ok', result_id: r.result_id, counts: r.counts, counted: r.counted, excluded: r.excluded, samples: r.samples, owner: r.owner, gate: r.gate };
  } catch (e) { return withCounts({ state: 'failed', stage: 'record', error: msg(e) }); }
}

/**
 * 🆕 #1676 Codex R4 Medium: 照合 ② が代表の観測を作る前に blocked / error になった回 = 記録しない。0068 の有無と門の状態 (持ち主) を読んで no_obs に付ける
 *   (company = ⚠️「記録できない・新しい CSV は閉じた」/ load = ℹ️ / 0068 の前 = not_applied = 黙る / 読めない = 持ち主が分からない = company と同じ ⚠️)
 */
export async function parentGateUnrecorded(readDb, reason) {
  const why = String(reason || 'no_obs').slice(0, 100);
  if (!readDb) return { state: 'no_obs', reason: why };
  try { if (!(await readDb.query(FN_RECORD)).rows[0].ok) return { state: 'not_applied' }; } catch (e) { return { state: 'no_obs', reason: why, state_error: msg(e) }; }
  try { const g = await readGateState(readDb); return { state: 'no_obs', reason: why, owner: ownerOf(g), gate: g }; } catch (e) { return { state: 'no_obs', reason: why, state_error: msg(e) }; }
}
const sumOf = (counts) => (counts && typeof counts === 'object' ? PARENT_COUNT_KEYS.reduce((a, k) => a + (Number(counts[k]) || 0), 0) : null);
const detailOf = (counts) => PARENT_COUNT_KEYS.filter((k) => Number(counts[k]) > 0).map((k) => `${PARENT_COUNT_JA[k]} ${counts[k]}`).join('・');
const codesOf = (samples) => {
  const all = [];
  for (const k of PARENT_COUNT_KEYS) for (const c of (samples && Array.isArray(samples[k]) ? samples[k] : [])) all.push(c);
  return all.length ? `: ${all.slice(0, 5).join(', ')}${all.length > 5 ? ' ほか' : ''}` : '';
};
const STATE_JA = { not_configured: '書く接続が無い (COMPANY_DB_WATCH_WRITER_URL)', failed: '書けない', no_fetch: '取得の世代・時刻・材料の世代が無い',
  no_spellings: 'NE のコードの元の書き方 (代表) を読めない', ne_untrusted: 'NE の取得を確かめられない (取得の件数・取込の整合・区分のゲートの許可の一覧)',
  no_obs: '照合 ② が代表の観測を作る前に止まった' };

/**
 * 代表の持ち主が company (DB の active) = 門が閉じる側 (記録 / 読み直しの答えの owner・門の状態)。
 *   🆕 #1676 Codex R1 Medium: 持ち主が分からない (記録も門の状態の読み直しも落ちた) = company と同じに倒す (「持ち主は夜間ロード」と言わない)
 */
export function parentEnforced(ne) {
  const p = ne && ne.parent_gate;
  if (!p) return false;
  if (p.owner === 'company' || (p.gate && p.gate.enforced === true)) return true;
  return !ownerKnown(p);
}
const ownerKnown = (p) => p.owner === 'load' || p.owner === 'company' || (!!p.gate && typeof p.gate.enforced === 'boolean');
const CLOSED_JA = '新しい NE 登録の CSV (作る・配る) は閉じた (夜間ロードは済んだ・配ったファイルの申告・照合・取り込めなかった商品だけの作り直し・廃止はできる)';
const recordWhy = (p) => `${STATE_JA[p.state] || p.state}${p.error ? `: ${String(p.error).slice(0, 80)}` : p.reason ? `: ${String(p.reason).slice(0, 80)}` : ''}`;

/**
 * 朝の要約に ⚠️ で出す代表 (親) の知らせ (問題なし = null。run.mjs の summaryLine が先頭が ⚠️ になるように置く)。
 *   ずれ (6 つの数えが 0 でない) = どの持ち主でも ⚠️ (company = 新しい NE 登録の CSV は閉じた / load = 知らせだけ・広げる前に 0 にする)
 *   記録できない = 持ち主 company のときだけ ⚠️ (前の回の記録は使わない = 門は閉じたまま)。load の間は parentNote の ℹ️
 */
export function parentTrouble(ne) {
  const p = ne && ne.parent_gate;
  if (!p || p.state === 'not_applied') return null;   // 🆕 #1676 Codex R4 Medium: 黙るのは 0068 の前だけ (no_obs = ② が観測を作る前に止まった朝も知らせる)
  const enforced = parentEnforced(ne);
  const total = sumOf(p.counts);
  const drift = total > 0 ? `代表 (親) が NE とずれた単品 ${total} 件 (${detailOf(p.counts)}${codesOf(p.samples)})` : null;
  if (p.state !== 'ok') {
    if (enforced) return `代表 (親) の数えを記録できない (${recordWhy(p)}${ownerKnown(p) ? '' : '・持ち主が分からない = company と同じに扱う'})${drift ? `・${drift}` : ''} → ${CLOSED_JA}`;
    return drift ? `${drift}・知らせだけ (代表の持ち主は夜間ロード = CSV は閉じない・広げる前に 0 にする)・一覧 = drift-list.mjs` : null;
  }
  if (!drift) return null;
  return `${drift} → ${enforced ? CLOSED_JA : '知らせだけ (代表の持ち主は夜間ロード = CSV は閉じない・広げる前に 0 にする)'}・一覧 = drift-list.mjs`;
}

/** 持ち主が load の間に数えを記録できなかった朝の一言 (ℹ️・要約の後ろ)。書く接続の無い朝 (試験・設定の前) は黙る */
export function parentNote(ne) {
  const p = ne && ne.parent_gate;
  if (!p || parentEnforced(ne) || ['ok', 'not_applied', 'not_configured'].includes(p.state)) return null;
  return `ℹ️ 代表 (親) の数えを記録できない (${recordWhy(p)}・持ち主は夜間ロード = CSV は閉じない)`;
}
