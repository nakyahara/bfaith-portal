/**
 * master-cutover.mjs — 商品マスタの切替の段階を読む・進める (Company DB構想 10 §8・14 §5。Codex ⑤-R1 H1。0050 の ops.master_cutover_state)
 *
 * 段階 (一方向・1 段ずつ): legacy_open (古い入口 = NE・/register が正) → frozen (古い入口を止めた) → company_owner (持ち主が Company DB) → new_open (新しい画面で保存できる)
 * 使うところ:
 *   - 新しい画面の保存 (lib/master-write.mjs): 段階が new_open のときだけ (+ 持ち主表が company + env MASTER_EDIT_OPEN = 1)
 *   - 古い入口 (⑤-3 で塞ぐ /register・会計アプリの POST など。Render と miniPC の両方): legacyWritable が false なら 410 + 新しい画面へ
 *   - 手の操作 (scripts/company-db/master-cutover.mjs): 見る・1 段進める
 * 🚨 読めない (表が無い・つながらない・壊れた値) = 閉じている (fail-closed)。新しい画面も古い入口も「書けない」側に倒す
 */
export const CUTOVER_PHASES = Object.freeze(['legacy_open', 'frozen', 'company_owner', 'new_open']);
export const PHASE_LABELS = Object.freeze({ legacy_open: '切替前 (NE・/register が正)', frozen: '切替中 (古い入口を止めた)', company_owner: '切替中 (持ち主が Company DB)', new_open: '切替後 (新しい画面で保存できる)' });

/**
 * 今の段階を読む。db = { query } (node-postgres の adapter / PGlite)。forShare = 取引の中で段階の行に共有の鍵 (段階を変える取引と並ばせる)
 * 戻り値 { readable, phase, changed_at, changed_by, error }。読めなければ readable = false・phase = null
 */
export async function readCutoverPhase(db, { forShare = false } = {}) {
  try {
    const r = (await db.query(`select phase, changed_at::text as changed_at, changed_by from ops.master_cutover_state where id = 1${forShare ? ' for share' : ''}`)).rows[0];
    if (!r || !CUTOVER_PHASES.includes(r.phase)) return { readable: false, phase: null, changed_at: null, changed_by: null, error: r ? `知らない段階 ${r.phase}` : '段階の行が無い' };
    return { readable: true, phase: r.phase, changed_at: r.changed_at, changed_by: r.changed_by, error: null };
  } catch (e) {
    if (forShare) throw e;   // 取引の中 (保存) では誤りをそのまま上げる = 取引ごと止める
    return { readable: false, phase: null, changed_at: null, changed_by: null, error: String(e && e.message || e) };
  }
}

/** 新しい画面で保存してよい段階か (読めない = いいえ) */
export const newEntryWritable = (s) => !!s && s.readable === true && s.phase === 'new_open';
/** 古い入口で書いてよい段階か (読めない = いいえ = 止める。⑤-3 の塞ぎ方が使う) */
export const legacyWritable = (s) => !!s && s.readable === true && s.phase === 'legacy_open';

/** 1 段進める (人の手の操作だけ。DB の関数が一方向・1 段ずつを守る) */
export async function advanceCutoverPhase(db, { to, actor, note = null }) {
  if (!CUTOVER_PHASES.includes(to)) throw Object.assign(new Error(`知らない段階: ${to}`), { code: 'CUTOVER_INVALID' });
  if (!actor) throw Object.assign(new Error('誰が進めたか (actor) が要る'), { code: 'CUTOVER_INVALID' });
  return (await db.query('select ops.set_master_cutover_phase($1, $2, $3) as r', [to, actor, note])).rows[0].r;
}
