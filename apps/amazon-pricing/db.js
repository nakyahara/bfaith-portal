/**
 * db.js — Amazon 価格管理 (自社プライスター) の表 ap_* と、その読み書き。
 *
 * 置き場所: warehouse-mirror.db の中 (price-update の pu_* / purchase-orders の po_* と同じ方式)。
 *   ・mirror_ 接頭辞の表だけが同期で作り直される対象なので、ap_* は同期・再初期化から触られない
 *   ・render-backup は mirror_/mart_/sync_ 以外を全部エクスポートするので、ap_* は自動で退避される
 *   ・同じファイルにあるので、AI 用の view (v_ap_listing_360) が mirror の表と JOIN できる
 *
 * 表は Company DB 構想 (03_内部ID設計+DDL草案 §4 events / §5 ai) の形を SQLite で先取りしている:
 *   ap_policies            … 出品ごとの値付け方針の「現在値」 (core.listings の属性に相当)
 *   ap_policy_events       … 方針の変更履歴 (append-only。誰が・いつ・何を・なぜ)  = events 層
 *   ap_evaluation_runs     … 判定を作った回 (ops.job_runs)
 *   ap_evaluations         … 判定 = 提案 (append-only)                              = ai.decisions
 *   ap_evaluation_reviews  … 人の採点 (append-only)                                  = ai.decision_reviews
 *   v_ap_listing_360       … AI と人が「1出品の全部」を 1 行で読む view              = mart.v_sku_360 の価格版
 *
 * ★append-only は DB のトリガで強制する (pu_* と同じ。「UPDATE を書かない」という規約だけでは、
 *   保守スクリプトや SQL コンソールから監査根拠を書き換えられる)。
 * ★ap_evaluations.autonomy_level は CHECK で 0 (提案のみ) に固定。このバージョンでは
 *   「人が承認したら実行」「自動実行」の段階を表す値を入れられない = 実行段階が存在しないことを表で示す。
 * ★時刻は UTC ISO (末尾 Z) で保存し、画面で JST にする (price-update と同じ)。
 */
import { getMirrorDB } from '../warehouse-mirror/db.js';
import { MODE_KEYS } from './engine.js';
import { LISTING_360_SQL, REQUIRED_MIRROR_TABLES } from './read-model.js';

let initialized = false;

/** 方針の列 (変更履歴に残す対象)。順番は画面の表示順 */
export const POLICY_FIELDS = ['mode', 'floor_price', 'ceiling_price', 'offset_jpy', 'min_margin_rate', 'note'];
/** 方針が無い出品の扱い (= 追従しない・ストッパー無し) */
export const POLICY_DEFAULTS = { mode: 'off', floor_price: null, ceiling_price: null, offset_jpy: 0, min_margin_rate: null, note: null };

/** 変更理由 (方針を変えるときに必ず 1 つ選ぶ) */
export const REASON_CODES = {
  initial: 'はじめて設定した',
  cost_change: '原価が変わった',
  competitor: '競合・カートの状況が変わった',
  margin: '粗利を見直した',
  mistake: '入力ミスの修正',
  stop: '追従を止めたい',
  other: 'その他 (理由を書く)',
};

export const REVIEW_VERDICTS = { agree: '妥当', disagree: '違う', unsure: 'わからない' };

export function initAmazonPricing() {
  const db = getMirrorDB();
  createTables(db);
  initialized = true;
  return db;
}

export function getDB() {
  if (!initialized) initAmazonPricing();
  return getMirrorDB();
}

/** DDL 単体 (テストが自前の DB に対して呼べるように分けてある) */
export function createTables(db) {
  db.exec(`CREATE TABLE IF NOT EXISTS ap_policies (
    seller_sku      TEXT PRIMARY KEY,
    mode            TEXT NOT NULL DEFAULT 'off' CHECK(mode IN ('off','buybox','fba_lowest','lowest')),
    floor_price     INTEGER CHECK(floor_price IS NULL OR floor_price > 0),
    ceiling_price   INTEGER CHECK(ceiling_price IS NULL OR ceiling_price > 0),
    offset_jpy      INTEGER NOT NULL DEFAULT 0,
    min_margin_rate REAL CHECK(min_margin_rate IS NULL OR (min_margin_rate >= 0 AND min_margin_rate < 0.9)),
    note            TEXT,
    updated_at      TEXT NOT NULL,
    updated_by      TEXT NOT NULL,
    CHECK(ceiling_price IS NULL OR floor_price IS NULL OR ceiling_price >= floor_price)
  )`);

  db.exec(`CREATE TABLE IF NOT EXISTS ap_policy_events (
    event_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    seller_sku   TEXT NOT NULL,
    at           TEXT NOT NULL,
    actor_type   TEXT NOT NULL CHECK(actor_type IN ('human','ai','system')),
    actor_id     TEXT NOT NULL,
    field        TEXT NOT NULL,
    old_value    TEXT,
    new_value    TEXT,
    reason_code  TEXT NOT NULL,
    reason_text  TEXT,
    source       TEXT NOT NULL DEFAULT 'ui' CHECK(source IN ('ui','csv','api','migration','test')),
    change_group TEXT NOT NULL
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_ap_pe_sku ON ap_policy_events(seller_sku, event_id)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_ap_pe_at ON ap_policy_events(at)');

  db.exec(`CREATE TABLE IF NOT EXISTS ap_evaluation_runs (
    run_id            TEXT PRIMARY KEY,
    started_at        TEXT NOT NULL,
    finished_at       TEXT,
    trigger           TEXT NOT NULL CHECK(trigger IN ('manual','page_open','test')),
    actor_id          TEXT NOT NULL,
    snapshot_date_jst TEXT,
    rule_version      TEXT NOT NULL,
    status            TEXT NOT NULL CHECK(status IN ('running','success','failed')),
    listings_total    INTEGER,
    summary_json      TEXT,
    error             TEXT
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_ap_runs_snap ON ap_evaluation_runs(snapshot_date_jst, rule_version, status)');

  db.exec(`CREATE TABLE IF NOT EXISTS ap_evaluations (
    decision_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id            TEXT NOT NULL REFERENCES ap_evaluation_runs(run_id),
    seller_sku        TEXT NOT NULL,
    asin              TEXT,
    evaluated_at      TEXT NOT NULL,
    snapshot_date_jst TEXT,
    domain            TEXT NOT NULL DEFAULT 'price',
    decision_kind     TEXT NOT NULL DEFAULT 'proposal',
    generated_by      TEXT NOT NULL DEFAULT 'rule' CHECK(generated_by IN ('rule','llm','hybrid')),
    rule_version      TEXT NOT NULL,
    model             TEXT,
    autonomy_level    INTEGER NOT NULL DEFAULT 0 CHECK(autonomy_level = 0),
    action            TEXT NOT NULL CHECK(action IN ('raise','lower','keep','hold')),
    proposed_price    INTEGER CHECK(proposed_price IS NULL OR proposed_price > 0),
    current_price     INTEGER,
    reason_code       TEXT NOT NULL,
    reason_text       TEXT NOT NULL,
    confidence        REAL CHECK(confidence IS NULL OR (confidence >= 0 AND confidence <= 1)),
    flags_json        TEXT NOT NULL DEFAULT '[]',
    inputs_json       TEXT NOT NULL,
    UNIQUE(run_id, seller_sku)
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_ap_ev_sku ON ap_evaluations(seller_sku, decision_id)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_ap_ev_run_action ON ap_evaluations(run_id, action)');

  db.exec(`CREATE TABLE IF NOT EXISTS ap_evaluation_reviews (
    review_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    decision_id  INTEGER NOT NULL REFERENCES ap_evaluations(decision_id),
    reviewer_id  TEXT NOT NULL,
    verdict      TEXT NOT NULL CHECK(verdict IN ('agree','disagree','unsure')),
    comment      TEXT,
    reviewed_at  TEXT NOT NULL
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_ap_rev_decision ON ap_evaluation_reviews(decision_id, review_id)');

  // ★append-only を DB 側で強制 (訂正は新しい行の追記で表す)
  for (const t of ['ap_policy_events', 'ap_evaluations', 'ap_evaluation_reviews']) {
    db.exec(`CREATE TRIGGER IF NOT EXISTS ${t}_no_update BEFORE UPDATE ON ${t}
      BEGIN SELECT RAISE(ABORT, '${t} は追記のみ (UPDATE 禁止)。訂正は行を足してください'); END`);
    db.exec(`CREATE TRIGGER IF NOT EXISTS ${t}_no_delete BEFORE DELETE ON ${t}
      BEGIN SELECT RAISE(ABORT, '${t} は追記のみ (DELETE 禁止)。訂正は行を足してください'); END`);
  }
  // run は running → success/failed の 1 回だけ状態が変わる。それ以外の書き換えは禁止
  db.exec(`CREATE TRIGGER IF NOT EXISTS ap_evaluation_runs_finish_once BEFORE UPDATE ON ap_evaluation_runs
    WHEN OLD.status <> 'running' OR NEW.run_id <> OLD.run_id OR NEW.started_at <> OLD.started_at
      OR NEW.trigger <> OLD.trigger OR NEW.actor_id <> OLD.actor_id OR NEW.rule_version <> OLD.rule_version
    BEGIN SELECT RAISE(ABORT, 'ap_evaluation_runs は終了の記録以外は書き換えられません'); END`);
  db.exec(`CREATE TRIGGER IF NOT EXISTS ap_evaluation_runs_no_delete BEFORE DELETE ON ap_evaluation_runs
    BEGIN SELECT RAISE(ABORT, 'ap_evaluation_runs は削除できません'); END`);

  ensureReadModelView(db);
  return db;
}

/**
 * AI 用 view。★参照先の mirror 表が全部そろっている時だけ作る。
 * 無い表を参照する view が残ると、その DB の ALTER TABLE / CREATE TABLE が全部失敗する
 * (docs/incidents/2026-05-15-mf-view-fix.md の再発防止)。そろっていなければ view を消して警告だけ出す。
 * @returns {{created:boolean, missing:string[]}}
 */
export function ensureReadModelView(db) {
  const have = new Set(db.prepare(`SELECT name FROM sqlite_master WHERE type='table'`).all().map((r) => r.name));
  const missing = REQUIRED_MIRROR_TABLES.filter((t) => !have.has(t));
  if (missing.length > 0) {
    db.exec('DROP VIEW IF EXISTS v_ap_listing_360');
    console.warn(`[amazon-pricing] v_ap_listing_360 は作りません (無い表: ${missing.join(', ')})`);
    return { created: false, missing };
  }
  // 定義が変わったときに追随できるよう、作り直す (view はデータを持たないので安全)
  db.exec('DROP VIEW IF EXISTS v_ap_listing_360');
  db.exec(`CREATE VIEW v_ap_listing_360 AS ${LISTING_360_SQL}`);
  return { created: true, missing: [] };
}

/** 一意ID (ms + 乱数)。時刻だけだと同一 ms の衝突がありうる */
export function newId(prefix) {
  const rnd = Math.random().toString(36).slice(2, 8);
  return `${prefix}-${Date.now().toString(36)}-${rnd}`;
}

const nowIso = () => new Date().toISOString();

// ─── 方針 (policies) ─────────────────────────────────────

export function getPolicy(db, sku) {
  return db.prepare('SELECT * FROM ap_policies WHERE seller_sku = ?').get(sku) || null;
}

export function listPolicies(db) {
  return new Map(db.prepare('SELECT * FROM ap_policies').all().map((p) => [p.seller_sku, p]));
}

/** 値の正規化 (画面から来た文字列を、表に入る型に)。不正なら {error} */
export function normalizePolicyPatch(patch) {
  const out = {};
  const errors = [];
  const intOrNull = (v, label, { allowZero = false, allowNegative = false } = {}) => {
    if (v === null || v === undefined || v === '') return null;
    const n = typeof v === 'number' ? v : Number(String(v).trim().replace(/,/g, ''));
    if (!Number.isInteger(n)) { errors.push(`${label} は整数円で入力してください`); return null; }
    if (!allowNegative && n < 0) { errors.push(`${label} は 0 以上で入力してください`); return null; }
    if (!allowZero && !allowNegative && n === 0) { errors.push(`${label} に 0 は入れられません (未設定にするなら空欄)`); return null; }
    if (Math.abs(n) > 9_999_999) { errors.push(`${label} が大きすぎます`); return null; }
    return n;
  };
  if ('mode' in patch) {
    if (!MODE_KEYS.includes(patch.mode)) errors.push('追従モードの値が不正です');
    else out.mode = patch.mode;
  }
  if ('floor_price' in patch) out.floor_price = intOrNull(patch.floor_price, '赤字ストッパー');
  if ('ceiling_price' in patch) out.ceiling_price = intOrNull(patch.ceiling_price, '高値ストッパー');
  if ('offset_jpy' in patch) out.offset_jpy = intOrNull(patch.offset_jpy, '上乗せ', { allowZero: true, allowNegative: true }) ?? 0;
  if ('min_margin_rate' in patch) {
    const v = patch.min_margin_rate;
    if (v === null || v === undefined || v === '') out.min_margin_rate = null;
    else {
      // 画面は % で入れる (10 = 10%)。小数 (0.1) で来ても受ける
      let n = Number(String(v).trim().replace(/%/g, ''));
      if (!Number.isFinite(n)) errors.push('最低粗利率は数値で入力してください');
      else {
        if (n >= 1) n = n / 100;
        if (n < 0 || n >= 0.9) errors.push('最低粗利率は 0〜89% の範囲で入力してください');
        else out.min_margin_rate = Math.round(n * 10000) / 10000;
      }
    }
  }
  if ('note' in patch) {
    const s = patch.note == null ? '' : String(patch.note).trim();
    if (s.length > 500) errors.push('メモは 500 文字までです');
    else out.note = s === '' ? null : s;
  }
  if (out.floor_price != null && out.ceiling_price != null && out.ceiling_price < out.floor_price) {
    errors.push('高値ストッパーは赤字ストッパー以上にしてください');
  }
  return { patch: out, errors };
}

const asText = (v) => (v === null || v === undefined ? null : String(v));

/**
 * 方針を保存し、変わった列ごとに履歴を残す (1 トランザクション)。
 * 何も変わらなければ履歴は増えない (押し直しで同じ内容を送っても二重に記録しない)。
 * @returns {{changed:string[], changeGroup:string|null, policy:object}}
 */
export function savePolicy(db, { sku, patch, actorId, actorType = 'human', reasonCode, reasonText = null, source = 'ui' }) {
  if (!sku || typeof sku !== 'string') throw validation('SKU が不正です');
  if (!REASON_CODES[reasonCode]) throw validation('変更理由を選んでください');
  if (reasonCode === 'other' && !(reasonText && reasonText.trim())) throw validation('「その他」のときは理由を書いてください');
  const norm = normalizePolicyPatch(patch);
  if (norm.errors.length) throw validation(norm.errors.join(' / '));

  const tx = db.transaction(() => {
    const before = getPolicy(db, sku);
    const next = { ...POLICY_DEFAULTS, ...(before || {}), ...norm.patch };
    // ストッパーの前後関係は「保存後の姿」で見る (片方だけ変えた時も守る)
    if (next.floor_price != null && next.ceiling_price != null && next.ceiling_price < next.floor_price) {
      throw validation('高値ストッパーは赤字ストッパー以上にしてください');
    }
    // 変わった列だけ履歴に残す。初回は「既定値と違う列」だけ (メモ (なし)→(なし) のような行で履歴を汚さない)
    const changed = POLICY_FIELDS.filter((f) => asText((before ?? POLICY_DEFAULTS)[f]) !== asText(next[f]));
    if (changed.length === 0 && before) return { changed: [], changeGroup: null, policy: before };

    const at = nowIso();
    const changeGroup = newId('apc');
    db.prepare(`INSERT INTO ap_policies (seller_sku, mode, floor_price, ceiling_price, offset_jpy, min_margin_rate, note, updated_at, updated_by)
      VALUES (@seller_sku, @mode, @floor_price, @ceiling_price, @offset_jpy, @min_margin_rate, @note, @updated_at, @updated_by)
      ON CONFLICT(seller_sku) DO UPDATE SET
        mode = excluded.mode, floor_price = excluded.floor_price, ceiling_price = excluded.ceiling_price,
        offset_jpy = excluded.offset_jpy, min_margin_rate = excluded.min_margin_rate, note = excluded.note,
        updated_at = excluded.updated_at, updated_by = excluded.updated_by`)
      .run({ seller_sku: sku, ...pick(next, POLICY_FIELDS), updated_at: at, updated_by: actorId });
    const ins = db.prepare(`INSERT INTO ap_policy_events
      (seller_sku, at, actor_type, actor_id, field, old_value, new_value, reason_code, reason_text, source, change_group)
      VALUES (?,?,?,?,?,?,?,?,?,?,?)`);
    // 初回で既定値のまま保存 (何も入れずに「記録する」) でも、設定したこと自体は mode の 1 行で見えるようにする
    const fields = changed.length > 0 ? changed : ['mode'];
    for (const f of fields) {
      ins.run(sku, at, actorType, actorId, f, before ? asText(before[f]) : null, asText(next[f]),
        reasonCode, reasonText ? String(reasonText).trim() : null, source, changeGroup);
    }
    return { changed: fields, changeGroup, policy: getPolicy(db, sku) };
  });
  return tx.immediate();
}

export function listPolicyEvents(db, { sku = null, limit = 200 } = {}) {
  if (sku) {
    return db.prepare('SELECT * FROM ap_policy_events WHERE seller_sku = ? ORDER BY event_id DESC LIMIT ?').all(sku, limit);
  }
  return db.prepare('SELECT * FROM ap_policy_events ORDER BY event_id DESC LIMIT ?').all(limit);
}

export function countPolicyEvents(db) {
  return db.prepare('SELECT COUNT(*) AS c FROM ap_policy_events').get().c;
}

// ─── 判定 (evaluations) ──────────────────────────────────

export function insertRun(db, { trigger, actorId, snapshotDate, ruleVersion }) {
  const runId = newId('apr');
  db.prepare(`INSERT INTO ap_evaluation_runs (run_id, started_at, trigger, actor_id, snapshot_date_jst, rule_version, status)
    VALUES (?,?,?,?,?,?,'running')`).run(runId, nowIso(), trigger, actorId, snapshotDate ?? null, ruleVersion);
  return runId;
}

export function finishRun(db, runId, { status, listingsTotal = null, summary = null, error = null }) {
  db.prepare(`UPDATE ap_evaluation_runs SET status = ?, finished_at = ?, listings_total = ?, summary_json = ?, error = ?
    WHERE run_id = ? AND status = 'running'`)
    .run(status, nowIso(), listingsTotal, summary ? JSON.stringify(summary) : null, error, runId);
}

export function getRun(db, runId) {
  const r = db.prepare('SELECT * FROM ap_evaluation_runs WHERE run_id = ?').get(runId);
  if (!r) return null;
  return { ...r, summary: r.summary_json ? JSON.parse(r.summary_json) : null };
}

/** その日のスナップショット・そのルール版で成功した run (あれば)。null 日付は IS で比較する */
export function runForSnapshot(db, snapshotDate, ruleVersion) {
  const r = db.prepare(`SELECT * FROM ap_evaluation_runs
    WHERE snapshot_date_jst IS ? AND rule_version = ? AND status = 'success'
    ORDER BY started_at DESC LIMIT 1`).get(snapshotDate ?? null, ruleVersion);
  return r ? { ...r, summary: r.summary_json ? JSON.parse(r.summary_json) : null } : null;
}

export function latestSuccessRun(db) {
  const r = db.prepare(`SELECT * FROM ap_evaluation_runs WHERE status = 'success' ORDER BY started_at DESC LIMIT 1`).get();
  return r ? { ...r, summary: r.summary_json ? JSON.parse(r.summary_json) : null } : null;
}

export function listRuns(db, limit = 20) {
  return db.prepare('SELECT * FROM ap_evaluation_runs ORDER BY started_at DESC LIMIT ?').all(limit)
    .map((r) => ({ ...r, summary: r.summary_json ? JSON.parse(r.summary_json) : null }));
}

export function insertEvaluations(db, runId, rows, { ruleVersion, snapshotDate }) {
  const at = nowIso();
  const ins = db.prepare(`INSERT INTO ap_evaluations
    (run_id, seller_sku, asin, evaluated_at, snapshot_date_jst, rule_version, action, proposed_price, current_price,
     reason_code, reason_text, confidence, flags_json, inputs_json)
    VALUES (@run_id, @seller_sku, @asin, @evaluated_at, @snapshot_date_jst, @rule_version, @action, @proposed_price,
     @current_price, @reason_code, @reason_text, @confidence, @flags_json, @inputs_json)`);
  for (const r of rows) {
    ins.run({
      run_id: runId, seller_sku: r.seller_sku, asin: r.asin ?? null, evaluated_at: at, snapshot_date_jst: snapshotDate ?? null,
      rule_version: ruleVersion, action: r.action, proposed_price: r.proposedPrice ?? null, current_price: r.currentPrice ?? null,
      reason_code: r.reasonCode, reason_text: r.reasonText, confidence: r.confidence ?? null,
      flags_json: JSON.stringify(r.flags || []), inputs_json: JSON.stringify(r.inputs || {}),
    });
  }
}

const parseEval = (e) => ({
  ...e,
  flags: e.flags_json ? JSON.parse(e.flags_json) : [],
  review_verdict: e.review_verdict ?? null,
});

/** run の判定一覧。各判定に「最後の採点」を付ける (採点は追記のみなので最後の行が現在の見方) */
const EVAL_WITH_REVIEW_SQL = `
  SELECT e.*, rv.verdict AS review_verdict, rv.reviewer_id AS review_by, rv.reviewed_at AS review_at, rv.comment AS review_comment
    FROM ap_evaluations e
    LEFT JOIN ap_evaluation_reviews rv ON rv.review_id = (
      SELECT MAX(review_id) FROM ap_evaluation_reviews WHERE decision_id = e.decision_id)`;

export function evaluationsOfRun(db, runId) {
  return db.prepare(`${EVAL_WITH_REVIEW_SQL} WHERE e.run_id = ? ORDER BY e.decision_id`).all(runId).map(parseEval);
}

export function evaluationsForSku(db, sku, limit = 30) {
  return db.prepare(`${EVAL_WITH_REVIEW_SQL} WHERE e.seller_sku = ? ORDER BY e.decision_id DESC LIMIT ?`).all(sku, limit).map(parseEval);
}

export function getEvaluation(db, decisionId) {
  const e = db.prepare(`${EVAL_WITH_REVIEW_SQL} WHERE e.decision_id = ?`).get(decisionId);
  return e ? parseEval(e) : null;
}

export function addReview(db, { decisionId, reviewerId, verdict, comment = null }) {
  if (!REVIEW_VERDICTS[verdict]) throw validation('採点の値が不正です');
  const ev = db.prepare('SELECT decision_id FROM ap_evaluations WHERE decision_id = ?').get(decisionId);
  if (!ev) throw validation('その判定はありません');
  const c = comment == null ? null : String(comment).trim().slice(0, 500) || null;
  const info = db.prepare(`INSERT INTO ap_evaluation_reviews (decision_id, reviewer_id, verdict, comment, reviewed_at)
    VALUES (?,?,?,?,?)`).run(decisionId, reviewerId, verdict, c, nowIso());
  return info.lastInsertRowid;
}

/** 採点の集計 (run 単位)。「AI (ルール) の判定に人がどれだけ同意したか」の材料 */
export function reviewStats(db, runId) {
  return db.prepare(`
    SELECT rv.verdict, COUNT(*) AS c
      FROM ap_evaluations e
      JOIN ap_evaluation_reviews rv ON rv.review_id = (
        SELECT MAX(review_id) FROM ap_evaluation_reviews WHERE decision_id = e.decision_id)
     WHERE e.run_id = ?
     GROUP BY rv.verdict`).all(runId)
    .reduce((acc, r) => { acc[r.verdict] = r.c; return acc; }, { agree: 0, disagree: 0, unsure: 0 });
}

// ─── 共通 ────────────────────────────────────────────────

function pick(obj, keys) {
  const o = {};
  for (const k of keys) o[k] = obj[k] === undefined ? null : obj[k];
  return o;
}

export function validation(message) {
  const e = new Error(message);
  e.code = 'VALIDATION';
  return e;
}
