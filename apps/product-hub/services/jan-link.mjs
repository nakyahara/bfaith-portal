/**
 * jan-link.mjs — product-hub の今ある JAN を、商品マスタ (Company DB) の有効な JAN に結ぶ (Company DB構想 21 §5 B2 の段 2)
 *
 * なぜ: 楽天に出す JAN は「Company DB の有効な JAN から選んだ物」にする (中原さんの決定 3 = a)。選んだ物は値と外部 ID の行番号で持つ (db.js の jan_* の列)。
 *   段 3 (選ぶ画面・出品の前に止める) を有効にする前に、今ある値を「同じ商品・同じ JAN・今も有効」で照らして、1 つに決まる物だけ結ぶ。
 *   0 件・複数は「要確認」(人が段 3 の画面で選ぶ)。
 * 流し方 (中原さんの答え 10/13 = a): product-hub の管理画面のボタン。先に「数える」= 何件結べるか・何件が要確認か を見せる → 「移す」で書く。
 *   🚨 「数える」と「移す」は同じ関数 (buildJanLinkPlan) で数える。「移す」は数え直して、見せた計画の印 (hash) と同じときだけ書く (違えば書かずに 409)。
 *   書くのは product-hub の SQLite の 1 つの取引だけ (途中で失敗 = 全部 戻る)。書く直前に、取引の中で今の値が数えたときと同じかを確かめる。
 *   同じ行を書き直さない (同じ = 何もしない) = 二度押しても 2 回目は「もう済んだ」(計画が変わる = 409)。同時に押す = プロセスの中の鍵で 1 つだけ。
 *   Company DB は読むだけ (cdb-jan-compare.mjs の読むだけの取引・期限つき)。読めない = 何も書かない。
 * 対象 (楽天に送る値と同じ決め方 = cdb-jan-compare.mjs の janUnitsForDraft):
 *   単品 = product_drafts.jan_code (その行の jan_* に書く) / まとまり = 子の SKU ごとの draft_sku_jans (その行の jan_* に書く)。
 *   まとまりの子で JAN の行が無い子・ページ代表の jan_code・まとまりに居ない子の行は対象外 (段 3 の画面で選ぶ)。
 *   JAN が空になった行に前の印が残っていれば消す (clear)。
 */
import crypto from 'node:crypto';
import { janUnitsForDraft, readCdbJans, JAN_READ_DEADLINE_MS } from './cdb-jan-compare.mjs';
import { logEvent } from '../db.js';

/** 要確認の理由 (画面の言葉) */
export const JAN_LINK_REASONS = Object.freeze({
  cdb_other_jan: '商品マスタにはこの商品の別の JAN がある (product-hub の JAN はそのどれでもない)',
  cdb_no_jan: '商品マスタのこの商品に JAN が無い',
  cdb_set: 'セット・例外の商品 (商品マスタでは JAN を持たない)',
  cdb_no_sku: '商品マスタに商品コードが無い',
  cdb_multiple: '同じ JAN が商品マスタに 2 つ以上ある (1 つに決まらない)',
});
export const JAN_LINK_ACTIONS = Object.freeze(['link', 'review', 'clear', 'same', 'empty']);

const str = (v) => String(v ?? '').trim();
const pairKey = (code, jan) => `${code.toLowerCase()}\u0000${jan}`;
const curOf = (r) => ({
  state: r.jan_link_state ?? null,
  row: r.jan_cdb_row == null ? null : Number(r.jan_cdb_row),
  value: r.jan_cdb_value ?? null,
  reason: r.jan_link_reason ?? null,
});

/**
 * 結ぶ対象の単位を集める (product-hub の SQLite を読むだけ・同期)。
 * 戻り値 [{ draftId, draftName, neCode, table: 'product_drafts' | 'draft_sku_jans', skuKey (draft_sku_jans の sku_code・単品は null), code, phJan, cur }]
 */
export function collectJanLinkUnits(db) {
  const drafts = db.prepare('SELECT * FROM product_drafts ORDER BY id').all();
  const skuRow = db.prepare('SELECT * FROM draft_sku_jans WHERE draft_id = ? AND sku_code = ?');
  const out = [];
  for (const d of drafts) {
    const { mode, units } = janUnitsForDraft(db, d);
    if (mode === 'variation') {
      for (const u of units) {
        const r = skuRow.get(d.id, u.code.toLowerCase());
        if (!r) continue;   // JAN の行が無い子 = 結ぶ物が無い (段 3 の画面で選ぶ)
        out.push({ draftId: d.id, draftName: d.name, neCode: d.ne_code, table: 'draft_sku_jans', skuKey: r.sku_code, code: u.code, phJan: str(r.jan_code), cur: curOf(r) });
      }
    } else if (units.length) {
      out.push({ draftId: d.id, draftName: d.name, neCode: d.ne_code, table: 'product_drafts', skuKey: null, code: units[0].code, phJan: str(d.jan_code), cur: curOf(d) });
    }
  }
  return out;
}

/** 1 つの単位の行き先を決める (読んだ Company DB の行から)。戻り値 { state, row, value, reason } (全部 null = 印なし) */
export function decideJanLink(unit, read) {
  const none = { state: null, row: null, value: null, reason: null };
  if (!unit.phJan) return none;
  const review = (reason) => ({ state: 'needs_review', row: null, value: null, reason });
  const r = read.byPair.get(pairKey(unit.code, unit.phJan));
  if (!r) throw new Error(`Company DB の答えに ${unit.code} が無い`);
  let jans = r.jans;
  if (typeof jans === 'string') jans = JSON.parse(jans);
  if (!Array.isArray(jans)) throw new Error(`Company DB の JAN の形が違う (${unit.code})`);
  if (r.sku_id == null) return review('cdb_no_sku');
  if (r.product_id == null) return review('cdb_set');
  const norm = r.jan_norm == null ? null : String(r.jan_norm);
  const hits = norm ? jans.filter((x) => String(x.norm) === norm) : [];
  if (hits.length === 1) return { state: 'linked', row: Number(hits[0].row), value: String(hits[0].value), reason: null };
  if (hits.length > 1) return review('cdb_multiple');
  return review(jans.length ? 'cdb_other_jan' : 'cdb_no_jan');
}

const sameTarget = (a, b) => a.state === b.state && a.row === b.row && a.value === b.value && a.reason === b.reason;
const actionOf = (unit, target) => {
  if (!unit.phJan) return unit.cur.state || unit.cur.row != null ? 'clear' : 'empty';
  if (sameTarget(unit.cur, target)) return 'same';
  return target.state === 'linked' ? 'link' : 'review';
};
/** 今の SQLite の値の印 (書く直前に取引の中で同じかを確かめる) */
const snapshotOf = (units) => sha(units.map((u) => [u.draftId, u.table, u.skuKey, u.phJan, u.cur.state, u.cur.row, u.cur.value, u.cur.reason]));
function sha(v) { return crypto.createHash('sha256').update(JSON.stringify(v)).digest('hex'); }

/**
 * 計画を作る (「数える」と「移す」の両方がこれを使う = 同じ数え方)。
 * 戻り値 { ok: true, hash, snapshot, counts, items: [...unit, target, action] } / { ok: false, reason } (Company DB を読めない = 計画なし)
 *   hash = 書く物 (link / review / clear) の印。0 件なら 'none'
 */
export async function buildJanLinkPlan(db, { env = process.env, deadlineMs = JAN_READ_DEADLINE_MS } = {}) {
  const units = collectJanLinkUnits(db);
  const snapshot = snapshotOf(units);
  const read = await readCdbJans(units.filter((u) => u.phJan).map((u) => ({ code: u.code, jan: u.phJan })), { env, deadlineMs });
  if (!read.ok) return { ok: false, reason: read.reason };
  let items;
  try {
    items = units.map((u) => { const target = decideJanLink(u, read); return { ...u, target, action: actionOf(u, target) }; });
  } catch (e) {
    return { ok: false, reason: String((e && e.message) || e).slice(0, 160) };
  }
  const writes = items.filter((i) => i.action === 'link' || i.action === 'review' || i.action === 'clear');
  const counts = { units: items.length, link: 0, review: 0, clear: 0, same: 0, empty: 0, reviewByReason: {}, drafts: new Set(writes.map((i) => i.draftId)).size };
  for (const i of items) {
    counts[i.action]++;
    if (i.action === 'review') counts.reviewByReason[i.target.reason] = (counts.reviewByReason[i.target.reason] || 0) + 1;
  }
  // 要確認のまま (same で needs_review) も「人が選ぶ物」として数える
  counts.needsReviewTotal = items.filter((i) => i.target.state === 'needs_review').length;
  counts.linkedTotal = items.filter((i) => i.target.state === 'linked').length;
  const hash = writes.length ? sha(writes.map((i) => [i.draftId, i.table, i.skuKey, i.phJan, i.action, i.target.state, i.target.row, i.target.value, i.target.reason])) : 'none';
  return { ok: true, hash, snapshot, counts, items };
}

export class JanLinkConflict extends Error {
  constructor(message) { super(message); this.code = 'jan_link_conflict'; }
}

/**
 * 計画を書く (product-hub の SQLite の 1 つの取引)。取引の中で今の値が計画を作ったときと同じかを確かめる (違えば何も書かずに JanLinkConflict)。
 * 1 行でも思ったとおりに書けなければ全部 戻す。戻り値 { written: { link, review, clear }, drafts }
 */
export function applyJanLinkPlan(db, plan, { actor, now = () => new Date().toISOString(), failAfter = null } = {}) {
  if (!actor) throw new Error('だれが押したかが無い');
  const writes = plan.items.filter((i) => i.action === 'link' || i.action === 'review' || i.action === 'clear');
  const at = now();
  const run = db.transaction(() => {
    if (snapshotOf(collectJanLinkUnits(db)) !== plan.snapshot) throw new JanLinkConflict('数えた後に product-hub の JAN が変わりました。もう一度「数える」を押してください');
    const written = { link: 0, review: 0, clear: 0 };
    const byDraft = new Map();
    let n = 0;
    for (const i of writes) {
      if (failAfter != null && n >= failAfter) throw new Error('試験: 途中で失敗');
      const t = i.action === 'clear' ? { state: null, row: null, value: null, reason: null } : i.target;
      const who = i.action === 'clear' ? null : actor;
      const when = i.action === 'clear' ? null : at;
      const info = i.table === 'product_drafts'
        ? db.prepare(`UPDATE product_drafts SET jan_cdb_row = ?, jan_cdb_value = ?, jan_link_state = ?, jan_link_reason = ?, jan_linked_at = ?, jan_linked_by = ?
            WHERE id = ? AND TRIM(COALESCE(jan_code, '')) = ?`).run(t.row, t.value, t.state, t.reason, when, who, i.draftId, i.phJan)
        : db.prepare(`UPDATE draft_sku_jans SET jan_cdb_row = ?, jan_cdb_value = ?, jan_link_state = ?, jan_link_reason = ?, jan_linked_at = ?, jan_linked_by = ?
            WHERE draft_id = ? AND sku_code = ? AND TRIM(jan_code) = ?`).run(t.row, t.value, t.state, t.reason, when, who, i.draftId, i.skuKey, i.phJan);
      if (info.changes !== 1) throw new JanLinkConflict(`書けなかった行があります (${i.code})。もう一度「数える」を押してください`);
      written[i.action]++;
      n++;
      if (!byDraft.has(i.draftId)) byDraft.set(i.draftId, []);
      byDraft.get(i.draftId).push(i.action === 'link' ? `${i.code} = ${i.phJan} を商品マスタの JAN に結んだ`
        : i.action === 'review' ? `${i.code} = ${i.phJan} は要確認 (${JAN_LINK_REASONS[i.target.reason] || i.target.reason})`
          : `${i.code} の JAN が空なので結びを外した`);
    }
    for (const [draftId, lines] of byDraft) logEvent(db, draftId, 'jan_linked', lines.join(' / ').slice(0, 1000), actor);
    return { written, drafts: byDraft.size };
  });
  return run();
}

/** 記録 (だれがいつ・数えた / 移した・結果) */
export function recordJanLinkRun(db, { kind, outcome, actor, hash = null, counts = null, error = null }) {
  db.prepare('INSERT INTO ph_jan_link_runs (kind, outcome, actor, plan_hash, counts_json, error) VALUES (?, ?, ?, ?, ?, ?)')
    .run(kind, outcome, actor || '(不明)', hash, counts ? JSON.stringify(counts) : null, error ? String(error).slice(0, 500) : null);
}
export function recentJanLinkRuns(db, limit = 20) {
  return db.prepare('SELECT * FROM ph_jan_link_runs ORDER BY id DESC LIMIT ?').all(limit)
    .map((r) => ({ ...r, counts: r.counts_json ? JSON.parse(r.counts_json) : null }));
}

/** 画面に返す形 (一覧は上限つき) */
export function planView(plan, { limit = 300 } = {}) {
  const pick = (a) => plan.items.filter((i) => i.action === a).slice(0, limit)
    .map((i) => ({ draftId: i.draftId, draftName: i.draftName, neCode: i.neCode, code: i.code, phJan: i.phJan, reason: i.target.reason, reasonText: i.target.reason ? JAN_LINK_REASONS[i.target.reason] : null, cdbJan: i.target.value }));
  const stillReview = plan.items.filter((i) => i.action === 'same' && i.target.state === 'needs_review').slice(0, limit)
    .map((i) => ({ draftId: i.draftId, draftName: i.draftName, neCode: i.neCode, code: i.code, phJan: i.phJan, reason: i.target.reason, reasonText: JAN_LINK_REASONS[i.target.reason] || i.target.reason }));
  return { hash: plan.hash, counts: plan.counts, link: pick('link'), review: pick('review'), clear: pick('clear'), stillReview };
}

// ─── 同時に押す = プロセスの中の鍵で 1 つだけ ───
let applying = false;
export function tryLockJanLink() { if (applying) return false; applying = true; return true; }
export function unlockJanLink() { applying = false; }
