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
 *   untrusted = 正規化の衝突・取込の整合で保持した商品 (DB は「比べられない」に数える)・complete = 行が落ちていない取得
 *   rep_collided = 🆕 #1676 Codex R1 High: NE のコードの元の書き方 (raw_ne_code_spellings の代表の名前空間) で書き方が 2 つ以上の代表 (DB は「曖昧」に数える)。
 *     代表の原文 (rows の 5 つめ) は compare-ne の repRawOf = 代表商品コード_src から戻した元の書き方 (保存の値は小文字)
 *   🚨 長すぎるコード (200 字超) は送らない (= DB では「取得に無い」= 比べられない)・長すぎる代表は unknown (比べられない)
 */
export function parentObservations(nm, { untrusted = [], complete = false, repCollided = [] } = {}) {
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
  const rc = [...new Set([...repCollided].filter((x) => typeof x === 'string' && x && x.length <= MAX_CODE))].sort();
  return { format: PARENT_OBS_FORMAT, complete: !!complete, untrusted: u, rep_collided: rc, rows };
}

/** compare-ne の resolveNeCodes の答え → 代表の名前空間で書き方が 2 つ以上の norm (読めない回 = 空 = rows の原文だけで見る) */
export function repCollisionsOf(neCodes) {
  return neCodes && neCodes.ok ? neCodes.entries.filter((e) => e.kind === 'rep' && e.state === 'collided').map((e) => e.code_norm) : [];
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

const sumOf = (counts) => (counts && typeof counts === 'object' ? PARENT_COUNT_KEYS.reduce((a, k) => a + (Number(counts[k]) || 0), 0) : null);
const detailOf = (counts) => PARENT_COUNT_KEYS.filter((k) => Number(counts[k]) > 0).map((k) => `${PARENT_COUNT_JA[k]} ${counts[k]}`).join('・');
const codesOf = (samples) => {
  const all = [];
  for (const k of PARENT_COUNT_KEYS) for (const c of (samples && Array.isArray(samples[k]) ? samples[k] : [])) all.push(c);
  return all.length ? `: ${all.slice(0, 5).join(', ')}${all.length > 5 ? ' ほか' : ''}` : '';
};
const STATE_JA = { not_configured: '書く接続が無い (COMPANY_DB_WATCH_WRITER_URL)', failed: '書けない', no_fetch: '取得の世代・時刻・材料の世代が無い' };

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
const recordWhy = (p) => `${STATE_JA[p.state] || p.state}${p.error ? `: ${String(p.error).slice(0, 80)}` : ''}`;

/**
 * 朝の要約に ⚠️ で出す代表 (親) の知らせ (問題なし = null。run.mjs の summaryLine が先頭が ⚠️ になるように置く)。
 *   ずれ (6 つの数えが 0 でない) = どの持ち主でも ⚠️ (company = 新しい NE 登録の CSV は閉じた / load = 知らせだけ・広げる前に 0 にする)
 *   記録できない = 持ち主 company のときだけ ⚠️ (前の回の記録は使わない = 門は閉じたまま)。load の間は parentNote の ℹ️
 */
export function parentTrouble(ne) {
  const p = ne && ne.parent_gate;
  if (!p || p.state === 'not_applied' || p.state === 'no_obs') return null;
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
  if (!p || parentEnforced(ne) || ['ok', 'not_applied', 'no_obs', 'not_configured'].includes(p.state)) return null;
  return `ℹ️ 代表 (親) の数えを記録できない (${recordWhy(p)}・持ち主は夜間ロード = CSV は閉じない)`;
}
