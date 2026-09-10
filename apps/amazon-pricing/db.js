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
 *   ap_custom_types        … カスタムの型 (プライスターの「オリジナルボタン」)。方針は mode='custom' + custom_type_id で参照
 *   ap_custom_type_events  … 型の変更履歴 (append-only)。型を直すと、その型を使う出品全部に効くので履歴は必須
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
import { MODE_KEYS, CUSTOM_OPTIONS, CUSTOM_DEFAULTS, OFFSET_PCT_MIN, OFFSET_PCT_MAX, MAX_CHANGE_AMOUNT } from './engine.js';
import { LISTING_360_SQL, REQUIRED_MIRROR_TABLES } from './read-model.js';

let initialized = false;

/** 方針の列 (変更履歴に残す対象)。順番は画面の表示順 */
export const POLICY_FIELDS = ['mode', 'custom_type_id', 'floor_price', 'ceiling_price', 'offset_jpy', 'min_margin_rate', 'note'];
/** 方針が無い出品の扱い (= 追従しない・ストッパー無し) */
export const POLICY_DEFAULTS = { mode: 'off', custom_type_id: null, floor_price: null, ceiling_price: null, offset_jpy: 0, min_margin_rate: null, note: null };
/** カスタムの型の列 (変更履歴に残す対象)。engine.js の CUSTOM_OPTIONS と同じ名前 */
export const CUSTOM_TYPE_FIELDS = ['name', 'basis', 'rival_scope', 'direction', 'offset_kind', 'offset_value', 'amazon_seller', 'prime_as', 'points', 'solo_raise', 'note', 'archived_at'];
export const CUSTOM_NAME_MAX = 40;
export const CUSTOM_NOTE_MAX = 300;

/** 変更理由 (方針を変えるときに必ず 1 つ選ぶ) */
export const REASON_CODES = {
  initial: 'はじめて設定した',
  cost_change: '原価が変わった',
  competitor: '競合・カートの状況が変わった',
  margin: '粗利を見直した',
  mistake: '入力ミスの修正',
  stop: '追従を止めたい',
  other: 'その他 (理由を書く)',
  inline: '一覧で直接変えた (プライスター風の操作)',
  bulk: 'チェックして一括で記録した',
};

export const REVIEW_VERDICTS = { agree: '妥当', disagree: '違う', unsure: 'わからない' };
/** 自由記述の上限 (画面の maxlength と同じ値をサーバ側でも強制する) */
export const REASON_TEXT_MAX = 300;
export const REVIEW_COMMENT_MAX = 500;

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

/**
 * DDL 単体 (テストが自前の DB に対して呼べるように分けてある)。
 *
 * ★脅威モデル (Codex R1 で明文化): ここのトリガ・CHECK が防ぐのは **普通のアプリ接続からの DML**
 *   (UPDATE / DELETE / INSERT OR REPLACE / autonomy_level ≠ 0 / 履歴を伴わない方針の書き換え)。
 *   DB ファイルの DDL 権限を持つ人 (DROP TRIGGER・表の作り直し・PRAGMA ignore_check_constraints) からは防げない。
 *   それは render-backup の日次退避と、Company DB (Postgres) 移行後のロール分離で扱う。
 */
export function createTables(db) {
  // PRAGMA は接続単位。REPLACE の内部 DELETE で DELETE トリガを発火させるには recursive_triggers が要る
  // (それとは別に、既存主キーへの INSERT を BEFORE INSERT で止めるので、PRAGMA の無い別接続でも REPLACE は通らない)
  // ★PRAGMA はトランザクションの中では効かないので外で
  db.pragma('foreign_keys = ON');
  db.pragma('recursive_triggers = ON');
  // ★DDL は全部 1 つの immediate トランザクションの中で (表の作り直し・トリガの DROP → CREATE・view の作り直しを含む)。
  //   途中で止まっても「トリガの無い表」「view の無い DB」が残らない (Codex R2 P1: 作り直しの後のトリガ再作成が外にあった)
  db.transaction(() => createTablesInTx(db)).immediate();
  return db;
}

function createTablesInTx(db) {
  // カスタムの型 (方針より先に作る: ap_policies が参照する)
  db.exec(`CREATE TABLE IF NOT EXISTS ap_custom_types (
    type_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    name          TEXT NOT NULL UNIQUE,
    basis         TEXT NOT NULL DEFAULT 'buybox' CHECK(basis IN ('buybox','lowest')),
    rival_scope   TEXT NOT NULL DEFAULT 'all' CHECK(rival_scope IN ('all','fba','fbm','same')),
    direction     TEXT NOT NULL DEFAULT 'both' CHECK(direction IN ('both','up_only','down_only')),
    offset_kind   TEXT NOT NULL DEFAULT 'jpy' CHECK(offset_kind IN ('jpy','pct')),
    offset_value  INTEGER NOT NULL DEFAULT 0,
    amazon_seller TEXT NOT NULL DEFAULT 'include' CHECK(amazon_seller IN ('include','ignore')),
    prime_as      TEXT NOT NULL DEFAULT 'fba' CHECK(prime_as IN ('fba','fbm')),
    points        TEXT NOT NULL DEFAULT 'price_only' CHECK(points IN ('price_only','effective')),
    solo_raise    TEXT NOT NULL DEFAULT 'none' CHECK(solo_raise IN ('none','to_ceiling')),
    note          TEXT,
    archived_at   TEXT,
    created_at    TEXT NOT NULL,
    created_by    TEXT NOT NULL,
    updated_at    TEXT NOT NULL,
    updated_by    TEXT NOT NULL,
    CHECK(offset_kind <> 'pct' OR (offset_value >= ${OFFSET_PCT_MIN} AND offset_value <= ${OFFSET_PCT_MAX})),
    CHECK(offset_kind <> 'jpy' OR (offset_value >= -${MAX_CHANGE_AMOUNT} AND offset_value <= ${MAX_CHANGE_AMOUNT}))
  )`);
  db.exec(`CREATE TABLE IF NOT EXISTS ap_custom_type_events (
    event_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    type_id      INTEGER NOT NULL,
    at           TEXT NOT NULL,
    actor_type   TEXT NOT NULL CHECK(actor_type IN ('human','ai','system')),
    actor_id     TEXT NOT NULL,
    field        TEXT NOT NULL,
    old_value    TEXT,
    new_value    TEXT,
    reason_text  TEXT,
    change_group TEXT NOT NULL
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_ap_cte_type ON ap_custom_type_events(type_id, event_id)');

  // 方針の履歴は方針の表より先に作る (旧表の作り直しの中で、履歴を要求するトリガを復元するため)
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

  db.exec(policiesDDL('ap_policies'));
  // 2026-09-10 より前に作られた ap_policies (mode 4 種・custom_type_id 無し) は作り直す (SQLite は CHECK を ALTER できない)
  migratePoliciesForCustom(db);

  db.exec(`CREATE TABLE IF NOT EXISTS ap_evaluation_runs (
    run_id            TEXT PRIMARY KEY,
    started_at        TEXT NOT NULL,
    finished_at       TEXT,
    trigger           TEXT NOT NULL CHECK(trigger IN ('manual','page_open','test')),
    actor_id          TEXT NOT NULL,
    snapshot_date_jst TEXT,
    input_fingerprint TEXT NOT NULL,
    rule_version      TEXT NOT NULL,
    status            TEXT NOT NULL CHECK(status IN ('running','success','failed')),
    listings_total    INTEGER,
    summary_json      TEXT,
    error             TEXT
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_ap_runs_snap ON ap_evaluation_runs(snapshot_date_jst, rule_version, status)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_ap_runs_fp ON ap_evaluation_runs(input_fingerprint, rule_version, status)');

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
  const APPEND_ONLY = { ap_policy_events: 'event_id', ap_custom_type_events: 'event_id', ap_evaluations: 'decision_id', ap_evaluation_reviews: 'review_id' };
  for (const [t, pk] of Object.entries(APPEND_ONLY)) {
    db.exec(`CREATE TRIGGER IF NOT EXISTS ${t}_no_update BEFORE UPDATE ON ${t}
      BEGIN SELECT RAISE(ABORT, '${t} は追記のみ (UPDATE 禁止)。訂正は行を足してください'); END`);
    db.exec(`CREATE TRIGGER IF NOT EXISTS ${t}_no_delete BEFORE DELETE ON ${t}
      BEGIN SELECT RAISE(ABORT, '${t} は追記のみ (DELETE 禁止)。訂正は行を足してください'); END`);
    // INSERT OR REPLACE は recursive_triggers が OFF の接続だと DELETE トリガを通らずに行を置き換える (Codex R1 High)。
    // 既にある主キーへの INSERT 自体を BEFORE INSERT で止める (REPLACE も同じ経路を通る)
    db.exec(`CREATE TRIGGER IF NOT EXISTS ${t}_no_replace BEFORE INSERT ON ${t}
      WHEN EXISTS (SELECT 1 FROM ${t} WHERE ${pk} = NEW.${pk})
      BEGIN SELECT RAISE(ABORT, '${t} は追記のみ (既にある行の置き換え禁止)'); END`);
  }
  // ap_evaluations は UNIQUE(run_id, seller_sku) 経由の REPLACE もある (Codex R2 High)。主キー以外の一意制約も同じく止める
  db.exec(`CREATE TRIGGER IF NOT EXISTS ap_evaluations_no_replace_unique BEFORE INSERT ON ap_evaluations
    WHEN EXISTS (SELECT 1 FROM ap_evaluations WHERE run_id = NEW.run_id AND seller_sku = NEW.seller_sku)
    BEGIN SELECT RAISE(ABORT, 'ap_evaluations は追記のみ (同じ run の同じ SKU の置き換え禁止)'); END`);
  // run は running → success/failed の 1 回だけ状態が変わる。変えてよい列も終了の記録だけ (ホワイトリスト)。
  // 定義を変えたので古いトリガは作り直す (CREATE IF NOT EXISTS は既存を更新しない)
  db.exec('DROP TRIGGER IF EXISTS ap_evaluation_runs_finish_once');
  db.exec(`CREATE TRIGGER ap_evaluation_runs_finish_once BEFORE UPDATE ON ap_evaluation_runs
    WHEN NOT (
      OLD.status = 'running' AND NEW.status IN ('success', 'failed')
      AND NEW.run_id IS OLD.run_id AND NEW.started_at IS OLD.started_at AND NEW.trigger IS OLD.trigger
      AND NEW.actor_id IS OLD.actor_id AND NEW.rule_version IS OLD.rule_version
      AND NEW.snapshot_date_jst IS OLD.snapshot_date_jst AND NEW.input_fingerprint IS OLD.input_fingerprint
    )
    BEGIN SELECT RAISE(ABORT, 'ap_evaluation_runs は running → success/failed の終了記録 (finished_at / listings_total / summary_json / error) 以外は書き換えられません'); END`);
  db.exec(`CREATE TRIGGER IF NOT EXISTS ap_evaluation_runs_no_delete BEFORE DELETE ON ap_evaluation_runs
    BEGIN SELECT RAISE(ABORT, 'ap_evaluation_runs は削除できません'); END`);
  db.exec(`CREATE TRIGGER IF NOT EXISTS ap_evaluation_runs_no_replace BEFORE INSERT ON ap_evaluation_runs
    WHEN EXISTS (SELECT 1 FROM ap_evaluation_runs WHERE run_id = NEW.run_id)
    BEGIN SELECT RAISE(ABORT, 'ap_evaluation_runs は既にある run の置き換え禁止'); END`);
  // 方針の現在値は「その変更を説明する履歴行」が無いと書けない (履歴を残さない直接 UPDATE を構造で止める)。
  //   ・列ごとに「同じ SKU・同じ時刻・同じ列・前の値・後の値・同じ actor」の履歴行を要求する (Codex R2 Medium:
  //     「同時刻に何か 1 件」では、note の履歴だけ書いて mode を直接変えられた)
  //   ・UPDATE は updated_at を必ず進めること (進めない UPDATE は古い履歴行で条件を満たしてしまう)
  //   ・既にある seller_sku への INSERT (= INSERT OR REPLACE) は拒否。savePolicy は初回 INSERT / 2 回目以降 UPDATE に分ける
  // 定義を変えたので古いトリガは作り直す (requireHistoryTriggers が DROP → CREATE する)
  policyHistoryTriggers(db);
  // 型も同じ規則: 履歴 (ap_custom_type_events) を先に書かないと作れない・変えられない・消せない。
  // 作成者・作成日時は履歴の対象外なので、代わりに「初回は更新者と同じ・以後は変えられない」を不変条件にする (Codex R1 P2)
  requireHistoryTriggers(db, {
    table: 'ap_custom_types', key: 'type_id', events: 'ap_custom_type_events', fields: CUSTOM_TYPE_FIELDS,
    textFields: CUSTOM_TYPE_FIELDS.filter((f) => f !== 'offset_value'),
    defaults: { basis: "'buybox'", rival_scope: "'all'", direction: "'both'", offset_kind: "'jpy'", offset_value: "'0'", amazon_seller: "'include'", prime_as: "'fba'", points: "'price_only'", solo_raise: "'none'" },
    immutable: ['created_at', 'created_by'], insertEquals: [['created_at', 'updated_at'], ['created_by', 'updated_by']],
    noDeleteMessage: 'ap_custom_types は削除できません (使わないなら archived_at を入れる)',
  });

  ensureReadModelView(db);
}

/** ap_policies の定義 (新規作成と、旧表の作り直しで同じものを使う) */
function policiesDDL(name) {
  return `CREATE TABLE IF NOT EXISTS ${name} (
    seller_sku      TEXT PRIMARY KEY,
    mode            TEXT NOT NULL DEFAULT 'off' CHECK(mode IN ('off','buybox','fba_lowest','lowest','custom')),
    custom_type_id  INTEGER REFERENCES ap_custom_types(type_id),
    floor_price     INTEGER CHECK(floor_price IS NULL OR floor_price > 0),
    ceiling_price   INTEGER CHECK(ceiling_price IS NULL OR ceiling_price > 0),
    offset_jpy      INTEGER NOT NULL DEFAULT 0,
    min_margin_rate REAL CHECK(min_margin_rate IS NULL OR (min_margin_rate >= 0 AND min_margin_rate < 0.9)),
    note            TEXT,
    updated_at      TEXT NOT NULL,
    updated_by      TEXT NOT NULL,
    CHECK(ceiling_price IS NULL OR floor_price IS NULL OR ceiling_price >= floor_price),
    CHECK((mode = 'custom') = (custom_type_id IS NOT NULL))
  )`;
}

const OLD_POLICY_COLS = 'seller_sku, mode, floor_price, ceiling_price, offset_jpy, min_margin_rate, note, updated_at, updated_by';

/**
 * 2026-09-10 より前の ap_policies (mode の CHECK が 4 種・custom_type_id 無し) を、行を 1 つも失わずに作り直す。
 * SQLite は CHECK 制約を ALTER できないので、新しい表に写して入れ替える。
 *   ・1 つの immediate トランザクションの中で: 写す → 件数と中身 (EXCEPT 両方向) が一致することを確かめる → 旧表を消す → 改名
 *   ・★無い表を参照する view が残ると、以後の ALTER / CREATE が全部失敗する (docs/incidents/2026-05-15) → 先に view を消す。
 *     view は createTables の最後 (ensureReadModelView) が作り直す
 *   ・旧表のトリガは表と一緒に消える。新しい表のトリガは createTables の続きで作る
 *   ・foreign_keys=ON の DROP TABLE は暗黙の DELETE を行うがトリガは発火しない (SQLite の仕様)
 * @returns {{migrated:boolean, rows:number}}
 */
export function migratePoliciesForCustom(db) {
  const needsMigration = () => {
    const cols = db.prepare('PRAGMA table_info(ap_policies)').all().map((c) => c.name);
    return cols.length > 0 && !cols.includes('custom_type_id');
  };
  if (!needsMigration()) return { migrated: false, rows: 0 };
  const tx = db.transaction(() => {
    // ★ロック (BEGIN IMMEDIATE) を取ってから要否を見直す。別の接続が先に作り直していれば何もしない (Codex R1 P1)
    if (!needsMigration()) return null;
    const before = db.prepare('SELECT COUNT(*) AS c FROM ap_policies').get().c;
    db.exec('DROP VIEW IF EXISTS v_ap_listing_360');
    db.exec('DROP TABLE IF EXISTS ap_policies__new');
    db.exec(policiesDDL('ap_policies__new'));
    db.exec(`INSERT INTO ap_policies__new (${OLD_POLICY_COLS}) SELECT ${OLD_POLICY_COLS} FROM ap_policies`);
    const after = db.prepare('SELECT COUNT(*) AS c FROM ap_policies__new').get().c;
    const lost = db.prepare(`SELECT COUNT(*) AS c FROM (SELECT ${OLD_POLICY_COLS} FROM ap_policies EXCEPT SELECT ${OLD_POLICY_COLS} FROM ap_policies__new)`).get().c;
    const extra = db.prepare(`SELECT COUNT(*) AS c FROM (SELECT ${OLD_POLICY_COLS} FROM ap_policies__new EXCEPT SELECT ${OLD_POLICY_COLS} FROM ap_policies)`).get().c;
    if (after !== before || lost !== 0 || extra !== 0) {
      throw new Error(`ap_policies の作り直しで行が一致しません (前 ${before} / 後 ${after} / 失われた ${lost} / 増えた ${extra})。何も変えずに止めます`);
    }
    db.exec('DROP TABLE ap_policies');
    db.exec('ALTER TABLE ap_policies__new RENAME TO ap_policies');
    // ★トリガと view の復元まで同じトランザクションの中で。ここで止まっても「トリガの無い新表」だけが残ることはない (Codex R1 P1)
    policyHistoryTriggers(db);
    ensureReadModelView(db);
    return before;
  });
  const rows = tx.immediate();
  if (rows == null) return { migrated: false, rows: 0 };
  console.log(`[amazon-pricing] ap_policies を作り直しました (custom_type_id を追加・${rows} 行そのまま)`);
  return { migrated: true, rows };
}

/** 方針の表の「履歴を先に書かないと変えられない」トリガ (createTables と作り直しの両方から呼ぶ。DROP → CREATE) */
function policyHistoryTriggers(db) {
  requireHistoryTriggers(db, {
    table: 'ap_policies', key: 'seller_sku', events: 'ap_policy_events', fields: POLICY_FIELDS,
    textFields: ['mode', 'note'], defaults: { mode: "'off'", offset_jpy: "'0'" },
    noDeleteMessage: 'ap_policies は削除できません (追従を止めるなら mode を off に)',
  });
}

/**
 * 「現在値の表」は「その変更を説明する履歴行」が無いと書けない、をトリガで強制する (方針と型で共通)。
 *   ・列ごとに「同じ鍵・同じ時刻・同じ列・前の値・後の値・同じ actor」の履歴行を要求する (Codex R2 Medium:
 *     「同時刻に何か 1 件」では、note の履歴だけ書いて mode を直接変えられた)
 *   ・UPDATE は updated_at を必ず進めること (進めない UPDATE は古い履歴行で条件を満たしてしまう)
 *   ・既にある鍵への INSERT (= INSERT OR REPLACE) は拒否。保存側は初回 INSERT / 2 回目以降 UPDATE に分ける
 *   ・初回は「既定値と違う列」だけ履歴を要求する (既定のままの列で履歴を汚さない)。余分な履歴行があるのは構わない
 *   ・DELETE は禁止
 *   ・immutable: 一度入れたら変えられない列 (作成者・作成日時)。insertEquals: 初回に等しくなければならない列の組 (作成者 = 更新者)
 * @param {{table:string, key:string, events:string, fields:string[], textFields:string[], defaults:Record<string,string>, noDeleteMessage:string, immutable?:string[], insertEquals?:string[][]}} spec
 */
function requireHistoryTriggers(db, { table, key, events, fields, textFields, defaults, noDeleteMessage, immutable = [], insertEquals = [] }) {
  const cast = (side, f) => (textFields.includes(f) ? `${side}.${f}` : `CAST(${side}.${f} AS TEXT)`);
  const eventFor = (f, oldExpr) => `EXISTS (SELECT 1 FROM ${events} e WHERE e.${key} = NEW.${key} AND e.at = NEW.updated_at
        AND e.actor_id = NEW.updated_by AND e.field = '${f}' AND e.old_value IS ${oldExpr} AND e.new_value IS ${cast('NEW', f)})`;
  const defaultOf = (f) => (Object.hasOwn(defaults, f) ? defaults[f] : 'NULL');
  const insertNeeds = fields.map((f) => `(${cast('NEW', f)} IS NOT ${defaultOf(f)} AND NOT ${eventFor(f, 'NULL')})`).join('\n      OR ');
  const updateNeeds = fields.map((f) => `(${cast('NEW', f)} IS NOT ${cast('OLD', f)} AND NOT ${eventFor(f, cast('OLD', f))})`).join('\n      OR ');
  const insertEq = insertEquals.map(([a, b]) => `OR NEW.${a} IS NOT NEW.${b}`).join(' ');
  const immutableChanged = immutable.map((f) => `OR NEW.${f} IS NOT OLD.${f}`).join(' ');
  db.exec(`DROP TRIGGER IF EXISTS ${table}_requires_event_insert`);
  db.exec(`CREATE TRIGGER ${table}_requires_event_insert BEFORE INSERT ON ${table}
    WHEN EXISTS (SELECT 1 FROM ${table} WHERE ${key} = NEW.${key})
      OR NOT EXISTS (SELECT 1 FROM ${events} e WHERE e.${key} = NEW.${key} AND e.at = NEW.updated_at AND e.actor_id = NEW.updated_by)
      ${insertEq}
      OR ${insertNeeds}
    BEGIN SELECT RAISE(ABORT, '${table} は変更履歴 (${events}) に変更内容を先に書いてからでないと作れません (既にある行の置き換えも不可${insertEquals.length ? '・作成者は更新者と同じ' : ''})'); END`);
  db.exec(`DROP TRIGGER IF EXISTS ${table}_requires_event_update`);
  db.exec(`CREATE TRIGGER ${table}_requires_event_update BEFORE UPDATE ON ${table}
    WHEN NEW.${key} IS NOT OLD.${key} OR NEW.updated_at IS OLD.updated_at OR NEW.updated_at < OLD.updated_at
      ${immutableChanged}
      OR NOT EXISTS (SELECT 1 FROM ${events} e WHERE e.${key} = NEW.${key} AND e.at = NEW.updated_at AND e.actor_id = NEW.updated_by)
      OR ${updateNeeds}
    BEGIN SELECT RAISE(ABORT, '${table} は変更履歴 (${events}) に変更内容 (列・前後の値・誰が) を先に書き、updated_at を進めてからでないと書き換えられません${immutable.length ? ' (作成者・作成日時は変えられません)' : ''}'); END`);
  db.exec(`CREATE TRIGGER IF NOT EXISTS ${table}_no_delete BEFORE DELETE ON ${table}
    BEGIN SELECT RAISE(ABORT, '${noDeleteMessage}'); END`);
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
  if ('custom_type_id' in patch) out.custom_type_id = intOrNull(patch.custom_type_id, 'カスタムの型');
  if ('floor_price' in patch) out.floor_price = intOrNull(patch.floor_price, '赤字ストッパー');
  if ('ceiling_price' in patch) out.ceiling_price = intOrNull(patch.ceiling_price, '高値ストッパー');
  if ('offset_jpy' in patch) out.offset_jpy = intOrNull(patch.offset_jpy, '上乗せ', { allowZero: true, allowNegative: true }) ?? 0;
  if ('min_margin_rate' in patch) {
    const v = patch.min_margin_rate;
    if (v === null || v === undefined || v === '') out.min_margin_rate = null;
    else {
      // ★画面・API の境界では **常に % で受ける** (10 = 10%、0.5 = 0.5%)。1 未満を比率と解釈しない
      //   (Codex R3 Medium: 単位が入力値で変わると 0.5 が 50% になる)。DB には比率 (0.10) で入れる
      const n = Number(String(v).trim().replace(/%/g, ''));
      if (!Number.isFinite(n)) errors.push('最低粗利率は % の数値で入力してください (例: 10)');
      else if (n < 0 || n >= 90) errors.push('最低粗利率は 0〜89% の範囲で入力してください');
      else out.min_margin_rate = Math.round(n * 100) / 10000;
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
  const reason = reasonText == null ? null : String(reasonText).trim();
  if (reasonCode === 'other' && !reason) throw validation('「その他」のときは理由を書いてください');
  if (reason && reason.length > REASON_TEXT_MAX) throw validation(`理由のメモは ${REASON_TEXT_MAX} 文字までです`);
  const norm = normalizePolicyPatch(patch);
  if (norm.errors.length) throw validation(norm.errors.join(' / '));

  const tx = db.transaction(() => {
    const before = getPolicy(db, sku);
    const next = { ...POLICY_DEFAULTS, ...(before || {}), ...norm.patch };
    // カスタムは型が必須 (実在し、使わない設定でないもの)。カスタム以外にしたら型は外す (表の CHECK と同じ規則)
    if (next.mode === 'custom') {
      if (next.custom_type_id == null) throw validation('カスタムにするときは型を選んでください');
      const t = getCustomType(db, next.custom_type_id);
      if (!t) throw validation('その型はありません (消されたか、番号が違います)');
      if (t.archived_at) throw validation(`型「${t.name}」は「使わない」になっています。別の型を選ぶか、型の画面で戻してください`);
    } else {
      next.custom_type_id = null;
    }
    // ストッパーの前後関係は「保存後の姿」で見る (片方だけ変えた時も守る)
    if (next.floor_price != null && next.ceiling_price != null && next.ceiling_price < next.floor_price) {
      throw validation('高値ストッパーは赤字ストッパー以上にしてください');
    }
    // 変わった列だけ履歴に残す。初回は「既定値と違う列」だけ (メモ (なし)→(なし) のような行で履歴を汚さない)
    const changed = POLICY_FIELDS.filter((f) => asText((before ?? POLICY_DEFAULTS)[f]) !== asText(next[f]));
    if (changed.length === 0 && before) return { changed: [], changeGroup: null, policy: before };

    // 時刻は前回より必ず進める (同一 ms に 2 回保存されると、トリガが「updated_at が進んでいない」で拒否するため)
    let at = nowIso();
    if (before?.updated_at && at <= before.updated_at) at = new Date(Date.parse(before.updated_at) + 1).toISOString();
    const changeGroup = newId('apc');
    // ★履歴を先に書く。ap_policies のトリガは「同じ時刻の履歴行」が無い書き換えを拒否する
    const ins = db.prepare(`INSERT INTO ap_policy_events
      (seller_sku, at, actor_type, actor_id, field, old_value, new_value, reason_code, reason_text, source, change_group)
      VALUES (?,?,?,?,?,?,?,?,?,?,?)`);
    // 初回で既定値のまま保存 (何も入れずに「記録する」) でも、設定したこと自体は mode の 1 行で見えるようにする
    const fields = changed.length > 0 ? changed : ['mode'];
    for (const f of fields) {
      ins.run(sku, at, actorType, actorId, f, before ? asText(before[f]) : null, asText(next[f]),
        reasonCode, reason || null, source, changeGroup);
    }
    // 初回は INSERT、2 回目以降は UPDATE (UPSERT にしない: 既にある SKU への INSERT はトリガで拒否している)
    const values = { seller_sku: sku, ...pick(next, POLICY_FIELDS), updated_at: at, updated_by: actorId };
    if (before) {
      db.prepare(`UPDATE ap_policies SET mode = @mode, custom_type_id = @custom_type_id, floor_price = @floor_price, ceiling_price = @ceiling_price,
        offset_jpy = @offset_jpy, min_margin_rate = @min_margin_rate, note = @note, updated_at = @updated_at, updated_by = @updated_by
        WHERE seller_sku = @seller_sku`).run(values);
    } else {
      db.prepare(`INSERT INTO ap_policies (seller_sku, mode, custom_type_id, floor_price, ceiling_price, offset_jpy, min_margin_rate, note, updated_at, updated_by)
        VALUES (@seller_sku, @mode, @custom_type_id, @floor_price, @ceiling_price, @offset_jpy, @min_margin_rate, @note, @updated_at, @updated_by)`).run(values);
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

// ─── カスタムの型 (custom types) ─────────────────────────

export function getCustomType(db, typeId) {
  return db.prepare('SELECT * FROM ap_custom_types WHERE type_id = ?').get(typeId) || null;
}

/** 型の一覧 (名前順)。既定は「使う」型だけ */
export function listCustomTypes(db, { includeArchived = false } = {}) {
  return db.prepare(`SELECT * FROM ap_custom_types ${includeArchived ? '' : 'WHERE archived_at IS NULL'} ORDER BY (archived_at IS NOT NULL), name`).all();
}

/** 型ごとに、それを使っている出品の数 (Map: type_id → 件数) */
export function customTypeUsage(db) {
  return new Map(db.prepare(`SELECT custom_type_id AS id, COUNT(*) AS c FROM ap_policies WHERE mode = 'custom' AND custom_type_id IS NOT NULL GROUP BY custom_type_id`).all().map((r) => [r.id, r.c]));
}

export function listCustomTypeEvents(db, { typeId = null, limit = 200 } = {}) {
  if (typeId != null) return db.prepare('SELECT * FROM ap_custom_type_events WHERE type_id = ? ORDER BY event_id DESC LIMIT ?').all(typeId, limit);
  return db.prepare('SELECT * FROM ap_custom_type_events ORDER BY event_id DESC LIMIT ?').all(limit);
}

/** 型の値の正規化 (画面から来た文字列を表に入る型に)。不正なら errors */
export function normalizeCustomTypePatch(patch) {
  const out = {};
  const errors = [];
  if ('name' in patch) {
    const v = patch.name == null ? '' : String(patch.name).trim();
    if (!v) errors.push('型の名前を入れてください');
    else if (v.length > CUSTOM_NAME_MAX) errors.push(`型の名前は ${CUSTOM_NAME_MAX} 文字までです`);
    else out.name = v;
  }
  for (const k of ['basis', 'rival_scope', 'direction', 'offset_kind', 'amazon_seller', 'prime_as', 'points', 'solo_raise']) {
    if (!(k in patch)) continue;
    const v = patch[k];
    if (!Object.hasOwn(CUSTOM_OPTIONS[k], v)) errors.push(`${k} の値が不正です`);
    else out[k] = v;
  }
  if ('offset_value' in patch) {
    const v = patch.offset_value;
    if (v === null || v === undefined || v === '') out.offset_value = 0;
    else {
      const n = typeof v === 'number' ? v : Number(String(v).trim().replace(/,/g, ''));
      if (!Number.isInteger(n)) errors.push('上乗せは整数 (円、または整数 %) で入力してください');
      else out.offset_value = n;
    }
  }
  if ('note' in patch) {
    const v = patch.note == null ? '' : String(patch.note).trim();
    if (v.length > CUSTOM_NOTE_MAX) errors.push(`メモは ${CUSTOM_NOTE_MAX} 文字までです`);
    else out.note = v === '' ? null : v;
  }
  return { patch: out, errors };
}

/** 上乗せの範囲は単位が決まってから見る (保存後の姿で) */
function checkOffsetRange(next) {
  if (next.offset_kind === 'pct' && (next.offset_value < OFFSET_PCT_MIN || next.offset_value > OFFSET_PCT_MAX)) {
    throw validation(`上乗せ (%) は ${OFFSET_PCT_MIN}〜${OFFSET_PCT_MAX} の範囲で入力してください`);
  }
  if (next.offset_kind === 'jpy' && Math.abs(next.offset_value) > MAX_CHANGE_AMOUNT) {
    throw validation(`上乗せ (円) は ±${MAX_CHANGE_AMOUNT.toLocaleString()} 円までです`);
  }
}

function checkReason(reasonText) {
  const r = reasonText == null ? '' : String(reasonText).trim();
  if (!r) throw validation('変更の理由を書いてください (この型を使っている出品全部に効きます)');
  if (r.length > REASON_TEXT_MAX) throw validation(`理由は ${REASON_TEXT_MAX} 文字までです`);
  return r;
}

const CTE_INSERT = `INSERT INTO ap_custom_type_events (type_id, at, actor_type, actor_id, field, old_value, new_value, reason_text, change_group)
  VALUES (?,?,?,?,?,?,?,?,?)`;

/**
 * 型を作る。type_id は履歴を先に書くために自分で採番する (表は AUTOINCREMENT だが、消せないので MAX+1 で衝突しない。immediate トランザクションの中)
 * @returns {object} 作った型
 */
export function createCustomType(db, { patch, actorId, actorType = 'human', reasonText = null }) {
  if (!actorId) throw validation('誰が作ったか (actorId) が必要です');
  const norm = normalizeCustomTypePatch({ ...patch, name: patch?.name });
  if (norm.errors.length) throw validation(norm.errors.join(' / '));
  if (!norm.patch.name) throw validation('型の名前を入れてください');
  const reason = reasonText == null ? null : (String(reasonText).trim() || null);
  if (reason && reason.length > REASON_TEXT_MAX) throw validation(`理由は ${REASON_TEXT_MAX} 文字までです`);
  const tx = db.transaction(() => {
    const next = { ...CUSTOM_DEFAULTS, ...norm.patch };
    checkOffsetRange(next);
    if (db.prepare('SELECT 1 FROM ap_custom_types WHERE name = ?').get(next.name)) throw validation(`型「${next.name}」は既にあります`);
    const typeId = (db.prepare('SELECT COALESCE(MAX(type_id), 0) AS m FROM ap_custom_types').get().m) + 1;
    const at = nowIso();
    const changeGroup = newId('apt');
    const ins = db.prepare(CTE_INSERT);
    // 作ったときは、値のある列を全部履歴に残す (「どういう型として生まれたか」が後から読める)
    for (const f of CUSTOM_TYPE_FIELDS) {
      if (next[f] == null) continue;
      ins.run(typeId, at, actorType, actorId, f, null, asText(next[f]), reason, changeGroup);
    }
    db.prepare(`INSERT INTO ap_custom_types (type_id, name, basis, rival_scope, direction, offset_kind, offset_value, amazon_seller, prime_as, points, solo_raise, note, archived_at, created_at, created_by, updated_at, updated_by)
      VALUES (@type_id, @name, @basis, @rival_scope, @direction, @offset_kind, @offset_value, @amazon_seller, @prime_as, @points, @solo_raise, @note, NULL, @at, @actor, @at, @actor)`)
      .run({ type_id: typeId, ...pick(next, CUSTOM_TYPE_FIELDS.filter((f) => f !== 'archived_at')), at, actor: actorId });
    return getCustomType(db, typeId);
  });
  return tx.immediate();
}

/**
 * 型を直す。理由は必須 (その型を使う出品全部に効くため)。変わった列だけ履歴に残す
 * @returns {{changed:string[], type:object}}
 */
export function updateCustomType(db, { typeId, patch, actorId, actorType = 'human', reasonText, expectedUpdatedAt = null }) {
  if (!actorId) throw validation('誰が変えたか (actorId) が必要です');
  const reason = checkReason(reasonText);
  const norm = normalizeCustomTypePatch(patch || {});
  if (norm.errors.length) throw validation(norm.errors.join(' / '));
  const tx = db.transaction(() => {
    const before = getCustomType(db, typeId);
    if (!before) throw validation('その型はありません');
    checkNotStale(before, expectedUpdatedAt);
    const next = { ...before, ...norm.patch };
    checkOffsetRange(next);
    if (next.name !== before.name && db.prepare('SELECT 1 FROM ap_custom_types WHERE name = ? AND type_id <> ?').get(next.name, typeId)) {
      throw validation(`型「${next.name}」は既にあります`);
    }
    return writeCustomTypeChange(db, { before, next, actorId, actorType, reason });
  });
  return tx.immediate();
}

/**
 * 型を「使わない」にする / 戻す。使っている出品が 1 件でもあれば「使わない」にできない
 * (割り当て先が無い方針を作らない。先に別の型か「しない」に変えてもらう)
 */
export function setCustomTypeArchived(db, { typeId, archived, actorId, actorType = 'human', reasonText, expectedUpdatedAt = null }) {
  if (!actorId) throw validation('誰が変えたか (actorId) が必要です');
  if (typeof archived !== 'boolean') throw validation('archived は true か false で指定してください');
  const reason = checkReason(reasonText);
  const tx = db.transaction(() => {
    const before = getCustomType(db, typeId);
    if (!before) throw validation('その型はありません');
    checkNotStale(before, expectedUpdatedAt);
    if (archived) {
      const used = db.prepare(`SELECT COUNT(*) AS c FROM ap_policies WHERE mode = 'custom' AND custom_type_id = ?`).get(typeId).c;
      if (used > 0) throw validation(`型「${before.name}」は ${used} 件の出品が使っています。先に別の型か「しない」に変えてください`);
    }
    const next = { ...before, archived_at: archived ? nowIso() : null };
    return writeCustomTypeChange(db, { before, next, actorId, actorType, reason });
  });
  return tx.immediate();
}

/**
 * 画面が表示した時点の updated_at と今の値が違えば、別の人が先に変えている (古い画面からの無言の上書きを防ぐ — Codex R1 P2)。
 * expectedUpdatedAt を渡さない呼び出し (スクリプト等) は照合しない
 */
function checkNotStale(before, expectedUpdatedAt) {
  if (expectedUpdatedAt == null || expectedUpdatedAt === '') return;
  if (String(expectedUpdatedAt) !== before.updated_at) {
    throw conflict(`型「${before.name}」は表示したあとに別の人が変えています (${before.updated_by})。画面を読み直してから直してください`);
  }
}

function writeCustomTypeChange(db, { before, next, actorId, actorType, reason }) {
  const changed = CUSTOM_TYPE_FIELDS.filter((f) => asText(before[f]) !== asText(next[f]));
  if (changed.length === 0) return { changed: [], type: before };
  let at = nowIso();
  if (before.updated_at && at <= before.updated_at) at = new Date(Date.parse(before.updated_at) + 1).toISOString();
  const changeGroup = newId('apt');
  const ins = db.prepare(CTE_INSERT);
  for (const f of changed) ins.run(before.type_id, at, actorType, actorId, f, asText(before[f]), asText(next[f]), reason, changeGroup);
  db.prepare(`UPDATE ap_custom_types SET name = @name, basis = @basis, rival_scope = @rival_scope, direction = @direction, offset_kind = @offset_kind,
    offset_value = @offset_value, amazon_seller = @amazon_seller, prime_as = @prime_as, points = @points, solo_raise = @solo_raise, note = @note,
    archived_at = @archived_at, updated_at = @updated_at, updated_by = @updated_by WHERE type_id = @type_id`)
    .run({ type_id: before.type_id, ...pick(next, CUSTOM_TYPE_FIELDS), updated_at: at, updated_by: actorId });
  return { changed, type: getCustomType(db, before.type_id) };
}

// ─── 判定 (evaluations) ──────────────────────────────────

export function insertRun(db, { trigger, actorId, snapshotDate, fingerprint, ruleVersion }) {
  const runId = newId('apr');
  db.prepare(`INSERT INTO ap_evaluation_runs (run_id, started_at, trigger, actor_id, snapshot_date_jst, input_fingerprint, rule_version, status)
    VALUES (?,?,?,?,?,?,?,'running')`).run(runId, nowIso(), trigger, actorId, snapshotDate ?? null, fingerprint ?? '-', ruleVersion);
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

/** 同じ入力 (指紋)・そのルール版で成功した run (あれば) */
export function runForInputs(db, fingerprint, ruleVersion) {
  const r = db.prepare(`SELECT * FROM ap_evaluation_runs
    WHERE input_fingerprint = ? AND rule_version = ? AND status = 'success'
    ORDER BY started_at DESC LIMIT 1`).get(fingerprint, ruleVersion);
  return r ? { ...r, summary: r.summary_json ? JSON.parse(r.summary_json) : null } : null;
}

/** その日のスナップショット・そのルール版で最後に成功した run (画面の表示用) */
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
  const c = comment == null ? null : (String(comment).trim() || null);
  if (c && c.length > REVIEW_COMMENT_MAX) throw validation(`ひとことは ${REVIEW_COMMENT_MAX} 文字までです`);
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

/** 競合 (表示が古い)。API は 409 で返す */
export function conflict(message) {
  const e = new Error(message);
  e.code = 'CONFLICT';
  return e;
}
