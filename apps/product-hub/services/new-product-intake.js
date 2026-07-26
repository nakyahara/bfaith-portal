/**
 * NE の新商品を product-hub に自動取込する (2026-07-25 中原さん指示)。
 *
 * 背景: Notion 側で「新商品が入ってきたらカードを作成する処理」を止めたため、
 *       今後は新商品がこのアプリに入ってくるようにする。
 *
 * 設計:
 *   - 検知元は `mirror_products` (NE 商品マスタのミラー)。inbound-info の syncNewProducts と同じ土台
 *   - 対象は **商品区分='単品' かつ 取扱区分='取扱中'** (中原さん決定。inbound-info と同じ範囲)
 *   - ⚠️ 「mirror にあって product_drafts に無い」を条件にすると、既に楽天へ出ている
 *     取扱中 3,723 件が全部流れ込む。そこで `ph_ne_seen_codes` に初出を記録し、
 *     **初回実行は全件をシードするだけでドラフトを作らない** (カットオフ)。
 *     2回目以降に現れた未知コードだけが「新商品」。
 *   - **バリエーションはまとめる**: 楽天は1ページ=代表商品コード単位なので、
 *     同じグループの子SKUが何件来ても代表コードのドラフト1件に集約する (§4)
 *   - **Notion カードは作らない** (中原さん決定: Notion を切る)。
 *     ⚠️ RYS は Notion を読んで Yahoo に出しているため、Yahoo 展開には
 *        要件定義 §12 のアダプタ (draft_yahoo → RYS へ供給) が別途必要になる
 *   - fail-safe: mirror が空 (miniPC 同期前/失敗) のときは何もしない
 */
import { getDB, logEvent, upsertDraftYahoo, isNeCodeUniqueEnforced } from '../db.js';
import { resolveVariationGroup, resolveNeDefaults, mirrorReady } from '../lib/variation.js';

const norm = (v) => String(v == null ? '' : v).trim().toLowerCase();

/**
 * シード時の「単品・取扱中」最低件数。
 * mirror が truncate→再投入型の同期中に走ると、数件しか無いスナップショットで
 * カットオフが確定し、後から戻った既存商品が全部「新商品」になる (Codex critical)。
 * 2026-07-25 実測で 3,723 件なので、その半分を大きく下回る状態はシードしない。
 */
export const MIN_SEED_SINGLES = 2000;

function seedCompletedAt(db) {
  try {
    return db.prepare(`SELECT value FROM ph_intake_state WHERE key = 'seed_completed_at'`).get()?.value || null;
  } catch (_) {
    return null;
  }
}

/** 1回の実行で作るドラフトの上限。異常時に大量生成しないためのブレーキ */
export const MAX_CREATE_PER_RUN = 200;

/**
 * 1 商品コードぶんのドラフトを用意する (無ければ作る)。自動取込と一括登録で共有。
 * - 楽天1ページ単位にまとめる: バリエーションなら代表商品コードをキーにする
 * - 商品名・税率は NE から初期値を入れる (§5)
 * - **Notion カードは作らない** (2026-07-25 中原さん決定)
 * @returns {{outcome:'created'|'merged'|'not_in_ne', draftId?:number, groupKey?:string}}
 */
export function ensureDraftForCode(db, code, { actor = null, requireNe = true } = {}) {
  const raw = String(code == null ? '' : code).trim();
  if (!raw) return { outcome: 'not_in_ne' };

  const v = resolveVariationGroup(db, raw, { withMembers: false });
  if (requireNe && v.kind === 'unknown') return { outcome: 'not_in_ne', groupKey: raw };
  const groupKey = v.kind === 'variation' ? v.groupKey : raw;

  const findDraft = db.prepare('SELECT id FROM product_drafts WHERE LOWER(TRIM(ne_code)) = ?');
  const existing = findDraft.get(norm(groupKey));
  if (existing) {
    markSeenStmt(db).run(norm(raw), raw, existing.id);
    return { outcome: 'merged', draftId: existing.id, groupKey };
  }

  const defaults = resolveNeDefaults(db, raw);
  const name = defaults?.name || raw;
  let draftId;
  try {
    draftId = Number(db.prepare(`
      INSERT INTO product_drafts (ne_code, name, source, created_by) VALUES (?, ?, 'portal', ?)
    `).run(groupKey, name, actor || 'auto:ne-intake').lastInsertRowid);
  } catch (e) {
    // 同時実行で先に入った場合は既存に寄せる (冪等)
    if (!/UNIQUE/i.test(String(e && e.message))) throw e;
    const raced = findDraft.get(norm(groupKey));
    if (!raced) throw e;
    markSeenStmt(db).run(norm(raw), raw, raced.id);
    return { outcome: 'merged', draftId: raced.id, groupKey };
  }
  logEvent(db, draftId, 'created', groupKey, actor);
  logEvent(db, draftId, 'auto_intake_from_ne', raw === groupKey ? raw : `${raw} → ${groupKey}`, actor);
  if (defaults?.taxPercent) upsertDraftYahoo(db, draftId, { tax_rate: `${defaults.taxPercent}%` });
  markSeenStmt(db).run(norm(raw), raw, draftId);
  return { outcome: 'created', draftId, groupKey };
}

function markSeenStmt(db) {
  return db.prepare(`
    INSERT INTO ph_ne_seen_codes (code_key, ne_code, draft_id) VALUES (?, ?, ?)
    ON CONFLICT(code_key) DO NOTHING
  `);
}

/** 1回の一括登録で受け付けるコード数の上限 */
export const MAX_REGISTER_CODES = 200;

/**
 * 商品コードを貼り付けての一括登録 (人が明示的に実行する)。
 * 自動取込のカットオフ (ph_ne_seen_codes) とは無関係に、指定されたコードを必ず対象にする。
 */
export function registerByCodes(codes, { actor = null, dryRun = false } = {}) {
  const db = getDB();
  if (!mirrorReady(db)) return { ok: false, error: 'mirror_unavailable' };
  // 既存の新規登録APIと同じ fail-closed (Codex medium): 正規化UNIQUEが張れていない環境では
  // 'ABC' と 'abc' の重複ドラフトを作れてしまうため止める
  if (!isNeCodeUniqueEnforced()) return { ok: false, error: 'ne_unique_not_enforced' };
  const list = [...new Set((codes || []).map((c) => String(c == null ? '' : c).trim()).filter(Boolean))];
  if (list.length === 0) return { ok: false, error: 'no_codes' };
  if (list.length > MAX_REGISTER_CODES) return { ok: false, error: 'too_many_codes' };

  const results = [];
  const summary = { created: 0, merged: 0, not_in_ne: 0 };
  const plannedKeys = new Set(); // dryRun 用: 同じグループを二重に数えない

  for (const code of list) {
    if (dryRun) {
      const v = resolveVariationGroup(db, code, { withMembers: false });
      if (v.kind === 'unknown') {
        summary.not_in_ne += 1;
        results.push({ code, outcome: 'not_in_ne' });
        continue;
      }
      const groupKey = v.kind === 'variation' ? v.groupKey : code;
      const existing = db.prepare('SELECT id FROM product_drafts WHERE LOWER(TRIM(ne_code)) = ?').get(norm(groupKey));
      if (existing || plannedKeys.has(norm(groupKey))) {
        summary.merged += 1;
        results.push({ code, outcome: 'merged', groupKey, draftId: existing?.id });
      } else {
        plannedKeys.add(norm(groupKey));
        summary.created += 1;
        const d = resolveNeDefaults(db, code);
        results.push({ code, outcome: 'created', groupKey, name: d?.name || null, taxPercent: d?.taxPercent ?? null });
      }
      continue;
    }

    const r = db.transaction(() => ensureDraftForCode(db, code, { actor }))();
    summary[r.outcome] += 1;
    results.push({ code, ...r });
  }
  return { ok: true, dryRun, summary, results };
}

/** 取込対象の候補 (単品・取扱中・未知コード) */
function selectCandidates(db) {
  return db.prepare(`
    SELECT p.商品コード AS code, p.商品名 AS name
      FROM mirror_products p
     WHERE p.商品区分 = '単品'
       AND p.取扱区分 = '取扱中'
       AND LOWER(TRIM(p.商品コード)) NOT IN (SELECT code_key FROM ph_ne_seen_codes)
     ORDER BY p.商品コード
  `).all();
}

export function intakeStatus() {
  const db = getDB();
  const seededAt = seedCompletedAt(db);
  const seen = db.prepare('SELECT COUNT(*) AS c FROM ph_ne_seen_codes').get().c;
  const mirror = mirrorReady(db)
    ? db.prepare(`SELECT COUNT(*) AS c FROM mirror_products WHERE 商品区分='単品' AND 取扱区分='取扱中'`).get().c
    : 0;
  return {
    initialized: !!seededAt,
    seededAt,
    seen,
    mirrorSingles: mirror,
    // シード未了のときの pending は「シードで登録される件数」であって新商品ではないので 0 扱い
    pending: seededAt && mirrorReady(db) ? selectCandidates(db).length : 0,
    autoCreated: db.prepare(`SELECT COUNT(*) AS c FROM product_drafts WHERE created_by = 'auto:ne-intake'`).get().c,
  };
}

/**
 * @param {object} opts
 * @param {boolean} [opts.dryRun] 書き込まず件数だけ返す
 * @returns {{ok: boolean, mode?: 'seed'|'intake', error?: string, ...}}
 */
export function syncNewProducts({ dryRun = false, actor = null } = {}) {
  const db = getDB();
  if (!mirrorReady(db)) return { ok: false, error: 'mirror_unavailable', created: 0 };
  if (!isNeCodeUniqueEnforced()) return { ok: false, error: 'ne_unique_not_enforced', created: 0 };
  const mirrorCount = db.prepare('SELECT COUNT(*) AS c FROM mirror_products').get().c;
  if (mirrorCount === 0) return { ok: false, error: 'mirror_empty', created: 0 };

  // シード済み判定は専用の状態キーで行う (seen 件数だと一括登録1件で intake モードに入ってしまう)
  const alreadySeeded = !!seedCompletedAt(db);
  const candidates = selectCandidates(db);

  const markSeen = db.prepare(`
    INSERT INTO ph_ne_seen_codes (code_key, ne_code, draft_id) VALUES (?, ?, ?)
    ON CONFLICT(code_key) DO NOTHING
  `);

  // ── 初回: カットオフを打つだけ (既存商品をドラフト化しない) ──
  if (!alreadySeeded) {
    // 同期途中の欠けたスナップショットでカットオフを確定させない (Codex critical)。
    // 単品・取扱中の件数が実測 (3,723件) の半分程度を下回る状態はシードを見送る
    const singles = db.prepare(`SELECT COUNT(*) AS c FROM mirror_products WHERE 商品区分='単品' AND 取扱区分='取扱中'`).get().c;
    if (singles < MIN_SEED_SINGLES) {
      return { ok: false, error: 'mirror_too_small', singles, minRequired: MIN_SEED_SINGLES, created: 0 };
    }
    if (dryRun) return { ok: true, mode: 'seed', dryRun: true, wouldSeed: candidates.length, created: 0 };
    db.transaction(() => {
      for (const c of candidates) markSeen.run(norm(c.code), String(c.code).trim(), null);
      db.prepare(`INSERT INTO ph_intake_state (key, value) VALUES ('seed_completed_at', strftime('%Y-%m-%dT%H:%M:%fZ','now'))
                  ON CONFLICT(key) DO NOTHING`).run();
    })();
    return { ok: true, mode: 'seed', seeded: candidates.length, created: 0 };
  }

  // intake モードでも、mirror が痩せているときは走らせない (truncate→再投入の最中など)
  const singles = db.prepare(`SELECT COUNT(*) AS c FROM mirror_products WHERE 商品区分='単品' AND 取扱区分='取扱中'`).get().c;
  if (singles < MIN_SEED_SINGLES) {
    return { ok: false, error: 'mirror_too_small', singles, minRequired: MIN_SEED_SINGLES, created: 0 };
  }

  // ── 2回目以降: 未知コード = 今日以降の新商品 ──
  const result = { ok: true, mode: 'intake', dryRun, candidates: candidates.length, created: 0, merged: 0, not_in_ne: 0, capped: false, drafts: [] };

  for (const c of candidates) {
    if (result.created >= MAX_CREATE_PER_RUN) { result.capped = true; break; }
    if (dryRun) {
      const v = resolveVariationGroup(db, c.code, { withMembers: false });
      const groupKey = v.kind === 'variation' ? v.groupKey : String(c.code).trim();
      const existing = db.prepare('SELECT id FROM product_drafts WHERE LOWER(TRIM(ne_code)) = ?').get(norm(groupKey));
      if (existing) result.merged += 1;
      else { result.created += 1; result.drafts.push({ ne_code: groupKey, from: c.code, name: c.name }); }
      continue;
    }
    const r = db.transaction(() => ensureDraftForCode(db, c.code, { actor: actor || 'auto:ne-intake' }))();
    if (r.outcome === 'created') { result.created += 1; result.drafts.push({ id: r.draftId, ne_code: r.groupKey, from: c.code }); }
    else if (r.outcome === 'merged') result.merged += 1;
    else result.not_in_ne += 1;
  }
  return result;
}
