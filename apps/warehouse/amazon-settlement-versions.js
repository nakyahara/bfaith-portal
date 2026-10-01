/**
 * amazon-settlement-versions.js — 決済の文書の版 (D-66)・生の表の版 source_revision・読み直す注文・coverage の lease (D7b-1b-3)
 *
 * 設計 = AI_reference システム設計/CompanyDB構想/13_Amazon利益のmart_設計_20260930.md v26 §3.1・D-65・D-66
 *
 * ① 文書の版 (D-66)
 *   - 決済のレポート 1 本 (= 1 つの決済の全部の行) を「文書の版」として amazon_settlement_document_versions に 1 行で持つ。
 *     document_version_id = 正規の JSON {source_layer, report_type, report_id, report_document_id, file_hash, normalization_version} の SHA-256
 *     (canonical-hash.mjs・null は null。manual CSV は report_id / report_document_id を null にして file_hash で区別)
 *   - 生の表 (headers / lines) は document_version_seq (版の表の整数の鍵) で版を参照する。
 *     🚨 設計書は「document_version_id を参照する」。64 文字の hash を 440 万行に持つと約 +600MB (索引を含む) = 整数の鍵で参照する (版の表で 1 対 1)
 *   - 決済ごとに採る版を 1 つ (selectDocumentVersions = SQL の view v_amazon_settlement_selected_documents と同じ規則):
 *       **中身の確かな版 (detail_valid = 1) が先** → **見出しがちょうど 1 行の版が先** (#1567 R1 Medium 3・R2 High 1・Medium 1) →
 *       層の順 (sp_api_v1 と sp_api_v2 が同じ 1 → manual_csv 2 → ほか 3) → ingested_at の新しい順 → document_version_id の UTF-8 のバイトの順
 *     detail_valid = 見出しがちょうど 1 行・明細の部品の合計 = 見出しの total・明細の決済 ID が 1 つで見出しと同じ・通貨 JPY
 *       (例外 = 過去の行の backfill の版で見出しが 0 行 = 明細の決済 ID が 1 つなら 1。ただし並びで見出し 1 行の版より後 = ほかに候補が無いときだけ採る。
 *        coverage は見出し 1 行を要る = complete にはならない)
 *     🚨 良い版 (detail_valid = 1) が 1 つも無い決済は、中身の悪い版の中で一番の版を **仮に採る** (PR の前もその行で build していた = 値は動かない・
 *       1 つの決済の違反で全部を止めない)。朝の報告と coverage の理由に「🚨 仮に採った壊れた版」(provisional_broken_version・人が直す) を出し、
 *       正式な値は見出しの検算と frontier で null のまま (fail-closed)。その決済を除いて続ける (Render に墓石を送る) はしない
 *     財務 (SQLite の build・送り手の変換)・見出しの検算・manifest は **その版の行だけ** から作る (版をまたいで行ごとに最新を選ばない)
 *   - 版の明細の要約 detail_digest = (business_line_key, 出現順, 9 つの金額, 数量, 取引の種類, 注文番号, SKU, 計上日) を
 *     business_line_key の UTF-8 のバイトの順 → 出現順で並べた正規の JSON の SHA-256。出現順 = 同じ鍵の中の source_line_no の DENSE_RANK
 *     (同じ文書の同じ行番号 = 過去の膨張の残骸 = 1 つにまとめる)
 *   - 過去の行 (reportDocumentId を保存していない) = report_document_id null で版を作る (backfillDocumentVersions)
 *
 * ② 生の表の版 source_revision と「読み直す注文」(§3.1 R10 H2・R11 M1・R13 M1・R14 M1)
 *   - amazon_settlement_source_revision の 1 行。lines / headers の INSERT / UPDATE / DELETE の trigger で 1 ずつ増える
 *   - lines の trigger は変わった注文番号 (無ければ疑似注文 '-:計上日') を amazon_settlement_dirty_orders に記録 (UPDATE は OLD と NEW の両方)
 *   - 記録を消すのは送り手 = その回の読み取りの版 R 以下の記録だけ (clearDirtyOrders)
 *   - 🚨 例外 = 版の鍵を null → 値にする UPDATE (過去の行の backfill) は数えない (中身は変わらない。trigger の WHEN)。backfill は最後に版を 1 つ進める
 *
 * ③ coverage の lease (§3.1「1 回の coverage の回が全部を持つ」)
 *   - warehouse.db の 1 行 (amazon_finance_coverage_lease)。持ち主は coordinator の親だけ。
 *     持ち主の判定 = retry-lock.js と同じ (pid が生きている node で、そのプロセスの開始が lease の開始より後でない)・心拍の期限では奪わない
 *   - 生の表の取引は書く直前に「lease が自分の世代・token のまま」を同じ取引の中で確かめる (assertLease)
 */
import crypto from 'node:crypto';
import { canonicalJsonStrict, canonicalSha256 } from '../company-db/canonical-hash.mjs';

export const REPORT_TYPES = Object.freeze({
  v1: 'GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE',
  v2: 'GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2',
});
/** 層 → レポートの種類 (過去の行の backfill で使う。manual_csv は形式が分からない = null) */
export const REPORT_TYPE_OF_LAYER = Object.freeze({ sp_api_v1: REPORT_TYPES.v1, sp_api_v2: REPORT_TYPES.v2 });
/** 採る版の層の順 (SQL の view と同じ) */
export const layerRank = (l) => (l === 'sp_api_v1' || l === 'sp_api_v2' ? 1 : l === 'manual_csv' ? 2 : 3);
const LAYER_RANK_SQL = `CASE v.source_layer WHEN 'sp_api_v1' THEN 1 WHEN 'sp_api_v2' THEN 1 WHEN 'manual_csv' THEN 2 ELSE 3 END`;
export const cmpUtf8 = (a, b) => Buffer.compare(Buffer.from(String(a), 'utf8'), Buffer.from(String(b), 'utf8'));
const nowSql = (d = new Date()) => d.toISOString().replace('T', ' ').slice(0, 19);

/** 文書の版の ID (D-66)。鍵は 6 つだけ (null は null) */
export function documentVersionId({ source_layer, report_type = null, report_id = null, report_document_id = null, file_hash = null, normalization_version }) {
  if (typeof source_layer !== 'string' || !source_layer) throw new Error('documentVersionId: source_layer が無い');
  if (typeof normalization_version !== 'string' || !normalization_version) throw new Error('documentVersionId: normalization_version が無い');
  const s = (v) => (v == null || v === '' ? null : String(v));
  return canonicalSha256({ source_layer, report_type: s(report_type), report_id: s(report_id), report_document_id: s(report_document_id), file_hash: s(file_hash), normalization_version });
}

/** 採る版の並び (a が b より先 = 負)。SQL の view の ORDER BY と同じ: 中身の確かな版が先 → 見出しがちょうど 1 行の版が先 → 層 → 新しい順 → ID */
export const validRank = (v) => (Number(v.detail_valid) === 1 ? 0 : 1);
export const headerRank = (v) => (Number(v.header_count) === 1 ? 0 : 1);
export function compareVersionOrder(a, b) {
  const va = validRank(a), vb = validRank(b);
  if (va !== vb) return va - vb;
  const ha = headerRank(a), hb = headerRank(b);
  if (ha !== hb) return ha - hb;
  const la = layerRank(a.source_layer), lb = layerRank(b.source_layer);
  if (la !== lb) return la - lb;
  if (a.ingested_at !== b.ingested_at) return String(a.ingested_at) > String(b.ingested_at) ? -1 : 1;   // 新しい順 ('YYYY-MM-DD HH:MM:SS' = 文字の順)
  return cmpUtf8(a.document_version_id, b.document_version_id);
}

/**
 * 採る版を決めるのに要る列 (compareVersionOrder と SQL の view が見る列 + 戻りで使う seq)。
 * 🚨 版の一覧を読む所は **どこもこの列を全部** 渡す (#1567 Codex R2 High 1: 送り手の SQL に header_count が無く、
 *    見出し 0 行の新しい版を採って SQLite の build・manifest と別の版を Render に送っていた)。足りなければ selectDocumentVersions が throw
 */
export const VERSION_SELECT_COLUMNS = Object.freeze(['seq', 'document_version_id', 'settlement_id', 'source_layer', 'ingested_at', 'detail_valid', 'header_count']);
/** 採る版を決める最小の SQL (送り手が使う。ほかは SELECT * = 全部の列) */
export const VERSION_SELECT_SQL = `SELECT ${VERSION_SELECT_COLUMNS.join(', ')} FROM amazon_settlement_document_versions`;

/** 決済ごとに採る版 (JS の実装。SQL の view v_amazon_settlement_selected_documents と試験で一致を確かめる)。戻り = Map<settlement_id, 版の行> */
export function selectDocumentVersions(versions) {
  const out = new Map();
  for (const v of versions) {
    for (const c of VERSION_SELECT_COLUMNS) {
      if (!v || typeof v !== 'object' || !Object.hasOwn(v, c)) {
        throw new Error(`selectDocumentVersions: 版の行に列 ${c} が無い (渡された列 = ${v && typeof v === 'object' ? Object.keys(v).join(', ') : typeof v}) = 採る版が SQLite の view と変わりうる → VERSION_SELECT_COLUMNS を全部読む`);
      }
    }
    if (v.settlement_id == null) continue;   // 決済 ID の決まらない版はどの決済にも採られない (SQL の view と同じ)
    const cur = out.get(v.settlement_id);
    if (!cur || compareVersionOrder(v, cur) < 0) out.set(v.settlement_id, v);
  }
  return out;
}

// ─── 表・trigger・view (db.js の createTables から呼ぶ。冪等) ───
const DIRTY_KEY = (r) => `CASE WHEN ${r}.amazon_order_id IS NULL OR ${r}.amazon_order_id = '' THEN '-:' || ${r}.economic_date ELSE ${r}.amazon_order_id END`;
const BUMP = `UPDATE amazon_settlement_source_revision SET revision = revision + 1 WHERE id = 1;`;
const DIRTY = (r) => `INSERT INTO amazon_settlement_dirty_orders (mall_order_no, revision, first_revision, updated_at)
      VALUES (${DIRTY_KEY(r)}, (SELECT revision FROM amazon_settlement_source_revision WHERE id = 1), (SELECT revision FROM amazon_settlement_source_revision WHERE id = 1), strftime('%Y-%m-%d %H:%M:%S', 'now'))
      ON CONFLICT (mall_order_no) DO UPDATE SET revision = excluded.revision, updated_at = excluded.updated_at;`;
const STALE = (r) => `UPDATE amazon_settlement_document_versions SET detail_stale = 1 WHERE seq = ${r}.document_version_seq AND detail_stale = 0;`;
/** backfill (版の鍵を null → 値) の UPDATE は数えない */
const NOT_BACKFILL = `NOT (OLD.document_version_seq IS NULL AND NEW.document_version_seq IS NOT NULL)`;

/** 索引が無ければ作り、かかった時間 (= 書き込みの lock の長さ) をログに出す。あれば何もしない (#1567 Codex R4 Medium 2) */
function createIndexTimed(db, name, on) {
  if (db.prepare(`SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = ?`).get(name)) return null;
  const t0 = Date.now();
  db.exec(`CREATE INDEX IF NOT EXISTS ${name} ON ${on}`);
  const msTaken = Date.now() - t0;
  console.log(`[versions] 索引 ${name} を作った: ${msTaken} ms (この間 warehouse.db は書き込みの lock = 初回の schema の準備。daily-sync・retry と重ねない)`);
  return msTaken;
}

export function createSettlementVersionSchema(db) {
  const cols = (t) => db.prepare(`PRAGMA table_info(${t})`).all().map((c) => c.name);
  const addCol = (t, c, type) => { if (!cols(t).includes(c)) db.exec(`ALTER TABLE ${t} ADD COLUMN ${c} ${type}`); };
  for (const t of ['raw_amazon_settlement_headers', 'raw_amazon_settlement_lines']) {
    addCol(t, 'report_document_id', 'TEXT');        // 取込が落とした文書 ID (過去の行 = null)
    addCol(t, 'normalization_version', 'TEXT');     // parser の版 (過去の行 = parser_version を backfill)
    addCol(t, 'document_version_seq', 'INTEGER');   // 文書の版 (amazon_settlement_document_versions.seq)
    addCol(t, 'currency_raw', 'TEXT');              // 原文の通貨 (空なら null。過去の行 = null = 初期の印で代える。R16 M1)
  }
  addCol('amazon_settlement_report_inventory', 'document_version_seq', 'INTEGER');   // 取込が入れた (or 既にあった) 文書の版
  // 🚨 初回 (pull の後に最初に initDB した処理) は生の表 (本番 約 440 万行) に索引を作る = その間 warehouse.db は書き込みの lock。
  //    版付けの「1 取引の最長」には入らないので、ここで個別に測ってログに出す (#1567 Codex R4 Medium 2)。初回の schema の準備は daily-sync・retry と重ねない別の作業
  createIndexTimed(db, 'idx_settle_lines_docver', 'raw_amazon_settlement_lines(document_version_seq)');
  createIndexTimed(db, 'idx_settle_headers_docver', 'raw_amazon_settlement_headers(document_version_seq)');

  db.exec(`CREATE TABLE IF NOT EXISTS amazon_settlement_document_versions (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    document_version_id   TEXT NOT NULL UNIQUE,   -- 正規の JSON の SHA-256 (D-66)
    source_layer          TEXT NOT NULL,
    report_type           TEXT,
    report_id             TEXT,                   -- API の report ID (manual = null)
    report_document_id    TEXT,                   -- API の文書 ID (過去の行・manual = null)
    file_hash             TEXT,                   -- 元のファイルの SHA-256 (V2 は並べ直す前)
    normalization_version TEXT NOT NULL,          -- parser の版 (同じファイルを新しい parser で読み直すと別の版)
    source_document_id    TEXT NOT NULL,          -- 生の行の source_document_id (API = report ID / manual = 'manual:<file_hash>')
    settlement_id         TEXT,                   -- その文書の決済 ID (見出し・明細から 1 つ。決まらない = null = どの決済にも採られない)
    ingested_at           TEXT NOT NULL,          -- 版を最初に入れた時刻 (UTC 'YYYY-MM-DD HH:MM:SS'。過去の行 = 行の ingested_at の最小)
    registered_by         TEXT NOT NULL CHECK (registered_by IN ('ingest', 'backfill')),
    created_at            TEXT NOT NULL,
    -- ↓ 明細の要約 (refreshVersionDetails が作る。行が変わると trigger が detail_stale = 1)
    detail_stale          INTEGER NOT NULL DEFAULT 1 CHECK (detail_stale IN (0, 1)),
    detail_revision       INTEGER,                -- 要約を作ったときの source_revision
    raw_line_count        INTEGER,                -- 生の行の数 (残骸を含む)
    line_count            INTEGER,                -- 出現順でまとめた後の行の数 (= detail_digest の要素の数)
    detail_digest         TEXT,
    components_sum_micro  INTEGER,                -- まとめた後の 9 つの金額の部品の符号つきの合計 (= 見出しの total と比べる)
    line_settlement_count INTEGER,                -- 明細の決済 ID の種類の数 (1 でなければ壊れた文書)
    line_settlement_id    TEXT,
    line_currency_bad     INTEGER,                -- 明細の原文の通貨が JPY でも空でもない行の数
    header_count          INTEGER,                -- 見出しの数 (物理の行の数。1 でなければ壊れた文書・#1567 Codex R3 High 2)
    header_id             INTEGER,                -- 見出しの行の ID (最小)
    header_settlement_id  TEXT,
    header_start          TEXT,                   -- 見出しの settlement-start-date (原文)
    header_end            TEXT,
    header_total_micro    INTEGER,
    header_currency       TEXT,
    header_currency_raw   TEXT,
    detail_valid          INTEGER                 -- 1 = 採る版の候補 (中身が確か) / 0 = 候補にしない (見出しが 1 行でない・部品の合計 ≠ total など)。要約を作るまで null
  )`);
  addCol('amazon_settlement_document_versions', 'detail_valid', 'INTEGER');
  db.exec(`CREATE INDEX IF NOT EXISTS idx_settle_docver_settlement ON amazon_settlement_document_versions(settlement_id)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_settle_docver_report ON amazon_settlement_document_versions(report_id, report_document_id)`);

  db.exec(`CREATE TABLE IF NOT EXISTS amazon_settlement_source_revision (id INTEGER PRIMARY KEY CHECK (id = 1), revision INTEGER NOT NULL DEFAULT 0)`);
  db.exec(`INSERT OR IGNORE INTO amazon_settlement_source_revision (id, revision) VALUES (1, 0)`);
  db.exec(`CREATE TABLE IF NOT EXISTS amazon_settlement_dirty_orders (
    mall_order_no  TEXT PRIMARY KEY,   -- 注文番号 / 疑似注文 '-:YYYY-MM-DD'
    revision       INTEGER NOT NULL,   -- 最後に変わったときの source_revision (送り手は読み取りの版 R 以下だけ消す)
    first_revision INTEGER NOT NULL,
    updated_at     TEXT NOT NULL
  ) WITHOUT ROWID`);

  // trigger (lines = 版 + 読み直す注文 + 版の要約を古い印に / headers = 版 + 要約を古い印に)
  db.exec(`CREATE TRIGGER IF NOT EXISTS trg_settle_lines_rev_ins AFTER INSERT ON raw_amazon_settlement_lines BEGIN
      ${BUMP} ${DIRTY('NEW')} ${STALE('NEW')}
    END`);
  db.exec(`CREATE TRIGGER IF NOT EXISTS trg_settle_lines_rev_upd AFTER UPDATE ON raw_amazon_settlement_lines WHEN ${NOT_BACKFILL} BEGIN
      ${BUMP} ${DIRTY('OLD')} ${DIRTY('NEW')} ${STALE('OLD')} ${STALE('NEW')}
    END`);
  db.exec(`CREATE TRIGGER IF NOT EXISTS trg_settle_lines_rev_del AFTER DELETE ON raw_amazon_settlement_lines BEGIN
      ${BUMP} ${DIRTY('OLD')} ${STALE('OLD')}
    END`);
  db.exec(`CREATE TRIGGER IF NOT EXISTS trg_settle_headers_rev_ins AFTER INSERT ON raw_amazon_settlement_headers BEGIN
      ${BUMP} ${STALE('NEW')}
    END`);
  db.exec(`CREATE TRIGGER IF NOT EXISTS trg_settle_headers_rev_upd AFTER UPDATE ON raw_amazon_settlement_headers WHEN ${NOT_BACKFILL} BEGIN
      ${BUMP} ${STALE('OLD')} ${STALE('NEW')}
    END`);
  db.exec(`CREATE TRIGGER IF NOT EXISTS trg_settle_headers_rev_del AFTER DELETE ON raw_amazon_settlement_headers BEGIN
      ${BUMP} ${STALE('OLD')}
    END`);

  // 決済ごとに採る版 (JS の selectDocumentVersions と同じ規則)
  db.exec(`DROP VIEW IF EXISTS v_amazon_settlement_selected_documents`);
  db.exec(`CREATE VIEW v_amazon_settlement_selected_documents AS
    SELECT settlement_id, seq AS document_version_seq, document_version_id, source_layer, report_id, report_document_id, file_hash, ingested_at
    FROM (
      SELECT v.*, ROW_NUMBER() OVER (PARTITION BY v.settlement_id ORDER BY CASE WHEN v.detail_valid = 1 THEN 0 ELSE 1 END, CASE WHEN v.header_count = 1 THEN 0 ELSE 1 END,
        ${LAYER_RANK_SQL}, v.ingested_at DESC, v.document_version_id) AS rn
      FROM amazon_settlement_document_versions v
      WHERE v.settlement_id IS NOT NULL
    ) WHERE rn = 1`);

  // coverage の lease (1 行・持ち主は coordinator の親だけ)
  db.exec(`CREATE TABLE IF NOT EXISTS amazon_finance_coverage_lease (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    run_token           TEXT,
    coverage_generation INTEGER,   -- 回の途中で入る (Render の今の世代を読んだ後・updating の前)。null = 世代を採らない回 (取込だけ)
    owner_pid           INTEGER,
    started_at          TEXT,      -- UTC ISO
    released_at         TEXT,
    updated_at          TEXT
  )`);

  // 初期の印 (D-65 案 a・追記だけの 2 層)。中身 = Seller Central の「過去の決済情報」(apps/warehouse/amazon-finance-initial-marker.js で取り込む)
  db.exec(`CREATE TABLE IF NOT EXISTS initial_marker_headers (
    marker_id        TEXT PRIMARY KEY,
    evidence_epoch   INTEGER NOT NULL UNIQUE,   -- 印を作るたびに採番。一覧の回はその時の最新の epoch を持ち、積み上げはその epoch の回だけ
    evidence_kind    TEXT NOT NULL,             -- 例 seller_central_payments_export
    verified_from    TEXT NOT NULL,             -- UTC 'YYYY-MM-DDTHH:MM:SSZ' (半開区間 [from, through))
    verified_through TEXT NOT NULL,
    captured_at      TEXT NOT NULL,             -- 書き出した (撮った) 時刻 (UTC)
    source_file_name TEXT,
    source_file_hash TEXT NOT NULL,             -- 取り込んだファイルの SHA-256
    settlement_count INTEGER NOT NULL,
    detail_digest    TEXT NOT NULL,             -- 明細 (決済 ID の UTF-8 のバイトの順) の正規の JSON の SHA-256
    marker_digest    TEXT NOT NULL,             -- 見出し + 明細の digest (coverage の initial_marker_digest)
    created_at       TEXT NOT NULL,
    note             TEXT
  )`);
  db.exec(`CREATE TABLE IF NOT EXISTS initial_marker_settlements (
    marker_id          TEXT NOT NULL REFERENCES initial_marker_headers(marker_id),
    settlement_id      TEXT NOT NULL,
    period_start       TEXT NOT NULL,           -- precision = time: UTC 'YYYY-MM-DDTHH:MM:SSZ' / jst_date: 'YYYY-MM-DD' (JST の日)
    period_end         TEXT NOT NULL,
    period_precision   TEXT NOT NULL CHECK (period_precision IN ('time', 'jst_date')),
    total_amount_micro INTEGER NOT NULL,
    currency           TEXT NOT NULL,
    report_id          TEXT,                    -- 保持期間の中で対応を確かめられたものだけ (R21 H1)
    PRIMARY KEY (marker_id, settlement_id)
  )`);
  for (const t of ['initial_marker_headers', 'initial_marker_settlements']) {
    db.exec(`CREATE TRIGGER IF NOT EXISTS trg_${t}_no_update BEFORE UPDATE ON ${t} BEGIN SELECT RAISE(ABORT, '${t} は追記だけ (印を直すときは新しい印を作る)'); END`);
    db.exec(`CREATE TRIGGER IF NOT EXISTS trg_${t}_no_delete BEFORE DELETE ON ${t} BEGIN SELECT RAISE(ABORT, '${t} は追記だけ (印を直すときは新しい印を作る)'); END`);
  }
  // 初期の印の順番待ち (#1567 Codex R1 High 2)。CLI は積むだけ = 印の表に書くのは coordinator の回の中だけ
  //   (Render の coverage を updating にした後・同じ lease / token の下。手のファイルと同じ = 印を変える前に古い complete を無効にする)
  db.exec(`CREATE TABLE IF NOT EXISTS initial_marker_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_key          TEXT NOT NULL,          -- 正規化した印 + 元のファイルの hash の SHA-256 (順番待ちの中で同じ印は 1 回だけ。入れた後に同じ印を積む = 印の作り直し = 新しい epoch)
    source_file_name   TEXT,
    source_file_hash   TEXT NOT NULL,
    norm_json          TEXT NOT NULL,          -- 正規化した印 (amazon-finance-initial-marker.js の normalizeMarker の戻り)
    detail_digest      TEXT NOT NULL,          -- 積んだときの明細の digest (入れるときに作り直して同じか確かめる)
    queued_at          TEXT NOT NULL,
    applied_at         TEXT,                   -- 印の表に入れた時刻 (null = 順番待ち)
    applied_generation INTEGER,                -- 入れた回の coverage の世代 (= coverage で回った証拠にもなる)
    marker_id          TEXT,
    evidence_epoch     INTEGER,
    failed_at          TEXT,                   -- 入れられなかった (中身が積んだときと違う など) = 順番待ちから外す・朝の報告
    apply_note         TEXT
  )`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_initial_marker_queue_key ON initial_marker_queue(queue_key)`);

  // 手で取り込む決済のファイル (Seller Central から落とした V1 / V2) の順番待ち。書くのは coordinator の回の中だけ (apps/warehouse/amazon-settlement-manual-file.js が積む)
  db.exec(`CREATE TABLE IF NOT EXISTS amazon_settlement_manual_files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_hash      TEXT NOT NULL UNIQUE,
    file_name      TEXT,
    stored_path    TEXT NOT NULL,
    format         TEXT NOT NULL CHECK (format IN ('v1', 'v2')),
    settlement_id  TEXT NOT NULL,
    queued_at      TEXT NOT NULL,
    ingested_at    TEXT,
    ingest_generation INTEGER,
    ingest_note    TEXT
  )`);
}

// ─── source_revision・読み直す注文 ───
export const readSourceRevision = (db) => Number(db.prepare(`SELECT revision FROM amazon_settlement_source_revision WHERE id = 1`).get()?.revision ?? 0);
export function bumpSourceRevision(db) { db.prepare(`UPDATE amazon_settlement_source_revision SET revision = revision + 1 WHERE id = 1`).run(); return readSourceRevision(db); }
/** 読み直す注文 (mall_order_no → revision) */
export const readDirtyOrders = (db) => db.prepare(`SELECT mall_order_no, revision FROM amazon_settlement_dirty_orders`).all();
/** 読み取りの版 R 以下の記録だけ消す (後から入った記録を消さない)。1 取引・check = 同じ取引の中の確かめ (lease)。戻り = 消した数 */
export function clearDirtyOrders(db, orderNos, maxRevision, { check = null } = {}) {
  const del = db.prepare(`DELETE FROM amazon_settlement_dirty_orders WHERE mall_order_no = ? AND revision <= ?`);
  return db.transaction(() => {
    if (check) check(db);
    let n = 0;
    for (const no of orderNos) n += del.run(no, maxRevision).changes;
    return n;
  }).immediate();
}
/** その版の全部の注文番号・疑似注文の日を「読み直す注文」に記録 (採る版が変わったとき = 旧い版にだけある注文を墓石で消せるように。R22 H3) */
export function dirtyVersionOrders(db, seq) {
  bumpSourceRevision(db);
  return db.prepare(`INSERT INTO amazon_settlement_dirty_orders (mall_order_no, revision, first_revision, updated_at)
      SELECT DISTINCT ${DIRTY_KEY('l')}, (SELECT revision FROM amazon_settlement_source_revision WHERE id = 1), (SELECT revision FROM amazon_settlement_source_revision WHERE id = 1), ?
        FROM raw_amazon_settlement_lines l INDEXED BY idx_settle_lines_docver WHERE l.document_version_seq = ?
      ON CONFLICT (mall_order_no) DO UPDATE SET revision = excluded.revision, updated_at = excluded.updated_at`).run(nowSql(), seq).changes;
}

// ─── 版の明細の要約 ───
export const AMOUNT_COLUMNS_MICRO = ['price_amount_micro', 'item_related_fee_amount_micro', 'promotion_amount_micro', 'shipment_fee_amount_micro', 'order_fee_amount_micro',
  'misc_fee_amount_micro', 'other_fee_amount_micro', 'direct_payment_amount_micro', 'other_amount_micro'];
const DETAIL_COLS = ['business_line_key', 'source_line_no', ...AMOUNT_COLUMNS_MICRO, 'quantity_purchased', 'transaction_type', 'amazon_order_id', 'seller_sku_normalized', 'economic_date'];
const numOrNull = (v) => (v == null ? null : typeof v === 'bigint' ? Number(v) : v);

/**
 * 1 つの版の明細の行 (business_line_key → source_line_no → ingested_at 新しい順 → id の順に並んだもの) から要約を作る (1 行ずつ)。
 *   同じ (鍵, 行番号) の 2 行目以降 = 過去の膨張の残骸 = 数えない。出現順 = 鍵の中の行番号の DENSE_RANK
 */
export function createDetailDigester() {
  const h = crypto.createHash('sha256');
  h.update('[', 'utf8');
  let n = 0, prevKey = null, prevLine = undefined, occ = 0, sum = 0n;
  return {
    add(r) {
      if (prevKey !== null) {
        const c = cmpUtf8(prevKey, r.business_line_key);
        if (c > 0) throw new Error(`明細の並びが business_line_key の UTF-8 のバイトの順でない (${prevKey} の後に ${r.business_line_key})`);
        if (c === 0 && prevLine === (r.source_line_no ?? null)) return;   // 残骸
        if (c === 0 && prevLine !== null && r.source_line_no != null && r.source_line_no < prevLine) throw new Error('明細の並びが行番号の順でない');
        occ = c === 0 ? occ + 1 : 1;
      } else occ = 1;
      prevKey = r.business_line_key; prevLine = r.source_line_no ?? null;
      const item = { business_line_key: r.business_line_key, occurrence: occ };
      for (const c of AMOUNT_COLUMNS_MICRO) { item[c] = numOrNull(r[c]); if (r[c] != null) sum += BigInt(r[c]); }
      item.quantity_purchased = numOrNull(r.quantity_purchased);
      item.transaction_type = r.transaction_type ?? null;
      item.amazon_order_id = r.amazon_order_id ?? null;
      item.seller_sku_normalized = r.seller_sku_normalized ?? null;
      item.economic_date = r.economic_date ?? null;
      h.update((n ? ',' : '') + canonicalJsonStrict(item), 'utf8');
      n++;
    },
    finish() { h.update(']', 'utf8'); return { lineCount: n, detailDigest: h.digest('hex'), componentsSumMicro: sum }; },
  };
}
/** 行の配列から要約 (試験・V2 を V1 の形にした行の比較)。並べ直してから */
export function detailDigestOfRows(rows) {
  const list = rows.slice().sort((a, b) => cmpUtf8(a.business_line_key, b.business_line_key) || ((a.source_line_no ?? -1) - (b.source_line_no ?? -1)));
  const d = createDetailDigester();
  for (const r of list) d.add(r);
  return d.finish();
}

/** 1 つの版の要約を DB から作り直して保存する (呼び手の取引の中で)。戻り = 保存した値 */
export function refreshVersionDetail(db, seq) {
  const d = createDetailDigester();
  let raw = 0, badCur = 0;
  const settlements = new Set();
  const it = db.prepare(`SELECT ${DETAIL_COLS.join(', ')}, source_settlement_id, currency_raw FROM raw_amazon_settlement_lines INDEXED BY idx_settle_lines_docver
     WHERE document_version_seq = ? ORDER BY business_line_key, source_line_no, ingested_at DESC, id`).safeIntegers(true).iterate(seq);
  for (const r of it) {
    raw++;
    settlements.add(r.source_settlement_id);
    if (r.currency_raw != null && r.currency_raw !== 'JPY') badCur++;
    d.add({ ...r, source_line_no: r.source_line_no == null ? null : Number(r.source_line_no) });
  }
  const det = d.finish();
  const hs = db.prepare(`SELECT id, business_line_key, source_settlement_id, settlement_start_date, settlement_end_date, total_amount_micro, currency, currency_raw
     FROM raw_amazon_settlement_headers INDEXED BY idx_settle_headers_docver WHERE document_version_seq = ? ORDER BY id`).all(seq);
  // 🚨 見出しの数 = **物理の行の数** (前は business_line_key の種類の数 = 同じ鍵の見出しが 2 行あっても 1 と数えた。#1567 Codex R3 High 2)
  const h0 = hs[0] || null;
  const sOne = settlements.size === 1 ? [...settlements][0] : null;
  const out = {
    detail_stale: 0, detail_revision: readSourceRevision(db), raw_line_count: raw, line_count: det.lineCount, detail_digest: det.detailDigest,
    components_sum_micro: det.componentsSumMicro, line_settlement_count: settlements.size, line_settlement_id: sOne, line_currency_bad: badCur,
    header_count: hs.length, header_id: h0 ? h0.id : null, header_settlement_id: h0 ? h0.source_settlement_id : null,
    header_start: h0 ? h0.settlement_start_date : null, header_end: h0 ? h0.settlement_end_date : null, header_total_micro: h0 ? h0.total_amount_micro : null,
    header_currency: h0 ? h0.currency : null, header_currency_raw: h0 ? h0.currency_raw : null,
  };
  // 決済 ID = 見出しと明細の決済 ID の **和集合** が 1 種類のときだけ (#1567 R3 Medium 1: 見出しの 1 行目で決めると、明細の別の決済の行が build から黙って落ちる)
  const union = new Set([...settlements, ...hs.map((h) => h.source_settlement_id)].filter((x) => x != null));
  const settle = union.size === 1 ? [...union][0] : null;
  const reg = db.prepare(`SELECT registered_by, settlement_id FROM amazon_settlement_document_versions WHERE seq = ?`).get(seq) || {};
  out.detail_valid = versionDetailValid({ ...out, registered_by: reg.registered_by, settlement_id: reg.settlement_id ?? settle }) ? 1 : 0;
  db.prepare(`UPDATE amazon_settlement_document_versions SET ${Object.keys(out).map((k) => `${k} = @${k}`).join(', ')},
      settlement_id = COALESCE(settlement_id, @settle) WHERE seq = @seq`).run({ ...out, settle, seq });
  return out;
}
/**
 * 版の中身が確かか (採る版の候補にしてよいか・R1 Medium 3)。
 *   見出し 1 行・明細の部品の合計 = 見出しの total・明細の決済 ID が 1 つで見出しと同じ・通貨 JPY (原文が JPY か空) = 候補
 *   過去の行の backfill の版で見出しが 0 行 = 明細の決済 ID が 1 つなら候補 (昔の取込は見出しを入れていないことがある・build は今までどおり作る)
 */
export function versionDetailValid(d) {
  const settle = d.settlement_id ?? null;
  if (settle == null) return false;
  if ((d.line_settlement_count ?? 0) > 1) return false;
  if (d.line_settlement_count === 1 && d.line_settlement_id !== settle) return false;
  if ((d.line_currency_bad ?? 0) > 0) return false;
  if (d.header_count === 0) return d.registered_by === 'backfill' && d.line_settlement_count === 1;
  if (d.header_count !== 1) return false;
  if (d.header_settlement_id !== settle) return false;
  if (d.header_currency !== 'JPY' || (d.header_currency_raw != null && d.header_currency_raw !== 'JPY')) return false;
  return String(d.components_sum_micro ?? 0) === String(d.header_total_micro ?? 'x');
}

/**
 * 書き込みの取引 (BEGIN IMMEDIATE) の時間 = 書き込みの lock を持った時間を測る。最長とその取引の名前を持つ (#1567 Codex R5 Medium 1)。
 *   版付けの「1 取引の最長」= 版の登録・行の版の UPDATE・版の +1・要約の作り直し (1 版の全行を読み並べ digest を作る) の **全部** の取引の最長
 */
export function newTxTimer() {
  return {
    maxMs: 0, maxName: null, count: 0,
    record(name, ms) { this.count++; if (this.maxName == null || ms > this.maxMs) { this.maxMs = ms; this.maxName = name; } },
  };
}
/** 測りながら BEGIN IMMEDIATE の取引を回す (例外でも測る) */
function timedImmediate(db, timer, name, fn) {
  const t0 = Date.now();
  try { return db.transaction(fn).immediate(); } finally { if (timer) timer.record(name, Date.now() - t0); }
}

/** 古い印の版を全部作り直す (1 版 = 1 取引)。check = 取引の中の確かめ (lease)。txTimer = 取引の時間を測る (newTxTimer)。戻り = 作り直した数 */
export function refreshStaleVersionDetails(db, { check = null, log = () => {}, txTimer = null } = {}) {
  const seqs = db.prepare(`SELECT seq FROM amazon_settlement_document_versions WHERE detail_stale = 1 ORDER BY seq`).all().map((r) => r.seq);
  for (const seq of seqs) {
    timedImmediate(db, txTimer, `要約の作り直し (版 #${seq})`, () => { if (check) check(db); refreshVersionDetail(db, seq); });
  }
  if (seqs.length) log(`[versions] 版の要約を作り直した: ${seqs.length} 版`);
  return seqs.length;
}

export const readVersions = (db) => db.prepare(`SELECT * FROM amazon_settlement_document_versions ORDER BY seq`).all();
/** その決済の今採っている版 (無ければ null) */
export function selectedVersionOf(db, settlementId) {
  const vs = db.prepare(`SELECT * FROM amazon_settlement_document_versions WHERE settlement_id = ?`).all(settlementId);
  return selectDocumentVersions(vs).get(settlementId) ?? null;
}

/** 版を登録する (既にあれば今の行)。meta = { document_version_id, source_layer, report_type, report_id, report_document_id, file_hash, normalization_version, source_document_id, settlement_id, ingested_at } */
export function registerDocumentVersion(db, meta, { registeredBy = 'ingest', now = new Date() } = {}) {
  db.prepare(`INSERT OR IGNORE INTO amazon_settlement_document_versions (document_version_id, source_layer, report_type, report_id, report_document_id, file_hash,
      normalization_version, source_document_id, settlement_id, ingested_at, registered_by, created_at)
    VALUES (@document_version_id, @source_layer, @report_type, @report_id, @report_document_id, @file_hash, @normalization_version, @source_document_id, @settlement_id, @ingested_at, @registered_by, @created_at)`)
    .run({ report_type: null, report_id: null, report_document_id: null, file_hash: null, settlement_id: null, ...meta, registered_by: registeredBy, created_at: nowSql(now) });
  return db.prepare(`SELECT * FROM amazon_settlement_document_versions WHERE document_version_id = ?`).get(meta.document_version_id);
}

// ─── 過去の行の backfill (D-66: report_document_id = null で版を作る) ───
const HAS_ROWS = (v) => `(EXISTS (SELECT 1 FROM raw_amazon_settlement_lines l INDEXED BY idx_settle_lines_docver WHERE l.document_version_seq = ${v}.seq)
  OR EXISTS (SELECT 1 FROM raw_amazon_settlement_headers h INDEXED BY idx_settle_headers_docver WHERE h.document_version_seq = ${v}.seq))`;
/**
 * build・送り手が決済の行を読んでよいかの問題の一覧 (空 = 読んでよい)。**次の回で直る一時の状態だけ** (R1 High 1・R2 Medium 1):
 *   ① 版の無い行がある (過去の行の backfill の前・途中)
 *   ② 行のある版の要約が古い (detail_stale = 1。backfill や取込が要約を作る前に止まった・行を手で直した)
 *   人が直すまで直らないもの (決済 ID の決まらない版・仮に採った壊れた版) は止めない = documentVersionWarnings (朝の報告と coverage の理由)
 */
export function documentVersionProblems(db) {
  const out = [];
  if (db.prepare(`SELECT 1 FROM raw_amazon_settlement_lines INDEXED BY idx_settle_lines_docver WHERE document_version_seq IS NULL LIMIT 1`).get()
    || db.prepare(`SELECT 1 FROM raw_amazon_settlement_headers INDEXED BY idx_settle_headers_docver WHERE document_version_seq IS NULL LIMIT 1`).get()) {
    out.push({ code: 'rows_without_version', detail: '決済の生の行に文書の版 (document_version_seq) の無い行がある = 過去の行の版付けがまだ・途中 → 夜に手で node apps/warehouse/migrate-settlement-document-versions.js --commit (coordinator は重い版付けを流さない・#1567 Codex R1 Medium)' });
  }
  const bad = db.prepare(`SELECT v.seq, v.settlement_id FROM amazon_settlement_document_versions v
     WHERE v.detail_stale = 1 AND ${HAS_ROWS('v')} ORDER BY v.seq LIMIT 5`).all();
  if (bad.length) out.push({ code: 'version_incomplete', detail: `行のある文書の版の要約が古い (${bad.map((b) => `#${b.seq} ${b.settlement_id ?? '決済なし'}`).join(' / ')}) = 版付け・取込が途中で止まった・行を手で直した → migrate-settlement-document-versions.js --commit か coordinator の次の回で作り直す` });
  return out;
}
/**
 * 人が直すまで直らない版の問題 (止めない・朝の報告と coverage の理由に出す・retry しない。#1567 R2 Medium 1 / 2):
 *   version_unresolved_settlement = 行のある版の決済 ID が決まらない (見出し・明細の決済 ID が 2 つ以上) = その版の行はどの決済にも採られない
 *   provisional_broken_version    = 良い版が無い決済で、中身の悪い版を仮に採っている (見出しの数・部品の合計・通貨を人が確かめる)
 */
export function documentVersionWarnings(db) {
  const out = [];
  const unresolved = db.prepare(`SELECT v.seq, v.report_id, v.source_document_id FROM amazon_settlement_document_versions v
     WHERE v.settlement_id IS NULL AND ${HAS_ROWS('v')} ORDER BY v.seq`).all();
  if (unresolved.length) out.push({ code: 'version_unresolved_settlement', human: true, count: unresolved.length,
    detail: `🚨 決済 ID の決まらない文書の版 ${unresolved.length} (${unresolved.slice(0, 5).map((u) => `#${u.seq} ${u.report_id ?? u.source_document_id}`).join(' / ')}) = 見出し・明細の決済 ID が 2 つ以上 = その版の行はどの決済にも採られない → 文書を確かめる (次の回では直らない)` });
  const prov = provisionalBrokenSettlements(db);
  if (prov.length) out.push({ code: 'provisional_broken_version', human: true, count: prov.length,
    detail: `🚨 仮に採った壊れた版 ${prov.length} 決済 (${prov.slice(0, 5).map((p) => `${p.settlement_id} #${p.seq} 見出し ${p.header_count}`).join(' / ')}) = 良い版が無いので中身の悪い版で build・送信している (正式な値は null) → 見出しの数・部品の合計・通貨を確かめる` });
  return out;
}
/** 良い版 (detail_valid = 1) が無く、中身の悪い版を仮に採っている決済 (行のあるもの) */
export function provisionalBrokenSettlements(db) {
  return db.prepare(`SELECT s.settlement_id, v.seq, v.header_count, v.detail_valid FROM v_amazon_settlement_selected_documents s
     JOIN amazon_settlement_document_versions v ON v.seq = s.document_version_seq
     WHERE COALESCE(v.detail_valid, 0) <> 1 AND ${HAS_ROWS('v')} ORDER BY s.settlement_id`).all();
}
/** 見出しが 0 行の版がある決済で、ほかの版もあるもの (#1567 R2 High 1 = 本番のコピーで 0 件を確かめてから初回を流す) */
export function header0MultiVersionSettlements(db) {
  return db.prepare(`SELECT settlement_id, COUNT(*) AS versions, SUM(header_count = 0) AS header0 FROM amazon_settlement_document_versions
     WHERE settlement_id IS NOT NULL GROUP BY 1 HAVING SUM(header_count = 0) > 0 AND COUNT(*) > 1 ORDER BY 1`).all();
}
/** 版の無い行・要約の古い版が無いか (一時の状態だけ) */
export function documentVersionsReady(db) { return documentVersionProblems(db).length === 0; }
/** build・送り手の前の確かめ (問題があれば止める = 黙って行を落とさない) */
export function assertDocumentVersionsReady(db) {
  const p = documentVersionProblems(db);
  if (p.length) throw Object.assign(new Error(p.map((x) => x.detail).join(' ／ ')), { code: 'SETTLEMENT_VERSIONS_NOT_READY', problems: p });
}

/**
 * 版の無い行 (過去の行) に版を付ける。規則 = (source_layer, source_document_id (= report ID), source_file_hash, parser_version) ごとに 1 つの版
 *   (report_id = API の層なら source_document_id・manual は null / report_document_id = null / normalization_version = parser_version)。
 *   行の更新は id の範囲ごとの取引 (batchIds)。trigger は版の鍵の null → 値を数えない = 最後に source_revision を 1 つ進める。
 *   check = 各取引の中の確かめ (lease)。戻り = { groups, versions, lines, headers }
 */
/** 決済 ID が 2 つ以上ある過去の文書を見つけた (版を付けずに止まる = その行は版の無いまま = build と送り手も止まる。#1567 R3 Medium 1) */
export class UnresolvedSettlementDocsError extends Error {
  constructor(groups) {
    super(`🚨 決済 ID が 2 つ以上ある過去の文書 ${groups.length} (${groups.slice(0, 5).map((g) => `${g.source_layer} ${g.source_document_id} = 決済 ${g.u_n} 個`).join(' / ')}) = どの決済の行か決まらない = 版を付けずに止めた `
      + '(build と送り手も「版の無い行」で止まる = 行を黙って落とさない・墓石を送らない)。文書を確かめ、分けて入れ直すか、確かめた上で migrate-settlement-document-versions.js --commit --allow-unresolved (🚨 その文書の行は SQLite の build から外れ、Render の仮の財務からも消える (墓石)。正しく分けて入れ直せば戻る)');
    this.code = 'UNRESOLVED_SETTLEMENT_DOCS';
    this.groups = groups;
  }
}
/** 見出しの行が 2 行以上ある過去の文書を見つけた (版を付けずに止まる = build と送り手も止まる。#1567 Codex R3 High 2) */
export class MultiHeaderDocsError extends Error {
  constructor(groups) {
    super(`🚨 見出しの行が 2 行以上ある過去の文書 ${groups.length} (${groups.slice(0, 5).map((g) => `${g.source_layer} ${g.source_document_id} = 見出し ${g.h_rows} 行`).join(' / ')}) = 連結・壊れた文書 = 版を付けずに止めた `
      + '(版を付けると見出しの数で採らない版になり、ほかに版が無い決済では「仮に採った壊れた版」になる)。文書を確かめ、見出しが 1 行の正しい文書で入れ直してから migrate-settlement-document-versions.js --commit を流し直す');
    this.code = 'MULTI_HEADER_DOCS';
    this.groups = groups;
  }
}
export function backfillDocumentVersions(db, { batchIds = 200000, check = null, log = () => {}, now = new Date(), allowUnresolved = false, txTimer = null } = {}) {
  // 🚨 全部の書き込みの取引を測る (版の登録・行の版の UPDATE・版の +1・要約の作り直し) = 「1 取引の最長」(#1567 Codex R5 Medium 1)
  const timer = txTimer || newTxTimer();
  const tx = (name, fn) => timedImmediate(db, timer, name, () => { if (check) check(db); return fn(); });
  const done = (o) => Object.assign(o, { maxTxMs: timer.maxMs, maxTxName: timer.maxName, txCount: timer.count });
  // 決済 ID も group で取る (R1 High 1: 版の登録と同じ取引で入れる = 行の UPDATE の後に止まっても決済 ID の無い版を残さない)。
  //   見出しの決済 ID が 1 つならそれ・見出しが無ければ明細の決済 ID が 1 つならそれ・どちらでもなければ null (壊れた文書 = assertDocumentVersionsReady が止める)
  const groups = db.prepare(`
    SELECT source_layer, source_document_id, source_file_hash, parser_version, MIN(ingested_at) AS ingested_at,
           COUNT(DISTINCT CASE WHEN is_header = 1 THEN source_settlement_id END) AS h_n, MAX(CASE WHEN is_header = 1 THEN source_settlement_id END) AS h_id,
           COUNT(DISTINCT CASE WHEN is_header = 0 THEN source_settlement_id END) AS l_n, MAX(CASE WHEN is_header = 0 THEN source_settlement_id END) AS l_id,
           COUNT(DISTINCT source_settlement_id) AS u_n, MAX(source_settlement_id) AS u_id,
           SUM(is_header) AS h_rows
    FROM (
      SELECT source_layer, source_document_id, source_file_hash, parser_version, ingested_at, source_settlement_id, 0 AS is_header FROM raw_amazon_settlement_lines INDEXED BY idx_settle_lines_docver WHERE document_version_seq IS NULL
      UNION ALL
      SELECT source_layer, source_document_id, source_file_hash, parser_version, ingested_at, source_settlement_id, 1 AS is_header FROM raw_amazon_settlement_headers INDEXED BY idx_settle_headers_docver WHERE document_version_seq IS NULL
    ) GROUP BY 1, 2, 3, 4 ORDER BY 1, 2, 3, 4`).all();
  if (!groups.length) return done({ groups: 0, versions: 0, lines: 0, headers: 0, refreshed: refreshStaleVersionDetails(db, { check, log, txTimer: timer }) });
  // 見出しと明細の決済 ID の和集合が 2 つ以上 = 決まらない → 版を付けずに止まる (明示の allowUnresolved だけ進める = 決済 ID が null の版・version_unresolved_settlement)
  const unresolved = groups.filter((g) => g.u_n > 1);
  if (unresolved.length && !allowUnresolved) throw new UnresolvedSettlementDocsError(unresolved);
  // 見出しの行 (物理の行) が 2 行以上 = 連結・壊れた文書 → 版を付けずに止まる (#1567 Codex R3 High 2・--allow-unresolved でも進めない)
  //   決済 ID の決まらない文書 (和集合が 2 つ以上) は上で止まる / allowUnresolved の後は決済 ID が null の版 = どの決済にも採られない = ここでは数えない
  const multiHeader = groups.filter((g) => Number(g.h_rows) > 1 && Number(g.u_n) <= 1);
  if (multiHeader.length) throw new MultiHeaderDocsError(multiHeader);
  log(`[versions] 版の無い文書 ${groups.length} 個に版を付ける`);
  const seqs = [];
  tx(`版の登録 (文書 ${groups.length})`, () => {
    db.exec(`CREATE TEMP TABLE IF NOT EXISTS _asdv_map (source_layer TEXT, source_document_id TEXT, source_file_hash TEXT, parser_version TEXT, seq INTEGER)`);
    db.exec(`DELETE FROM temp._asdv_map`);
    const ins = db.prepare(`INSERT INTO temp._asdv_map VALUES (?, ?, ?, ?, ?)`);
    for (const g of groups) {
      const api = Object.hasOwn(REPORT_TYPE_OF_LAYER, g.source_layer);
      const meta = {
        source_layer: g.source_layer, report_type: api ? REPORT_TYPE_OF_LAYER[g.source_layer] : null, report_id: api ? g.source_document_id : null,
        report_document_id: null, file_hash: g.source_file_hash ?? null, normalization_version: g.parser_version,
      };
      const settlementId = g.u_n === 1 ? g.u_id : null;   // 見出しと明細の決済 ID の和集合が 1 種類のときだけ (#1567 R3 Medium 1)
      const v = registerDocumentVersion(db, { ...meta, document_version_id: documentVersionId(meta), source_document_id: g.source_document_id, settlement_id: settlementId, ingested_at: g.ingested_at || nowSql(now) }, { registeredBy: 'backfill', now });
      if (v.settlement_id == null && settlementId != null) db.prepare(`UPDATE amazon_settlement_document_versions SET settlement_id = ? WHERE seq = ?`).run(settlementId, v.seq);
      ins.run(g.source_layer, g.source_document_id, g.source_file_hash ?? null, g.parser_version, v.seq);
      seqs.push(v.seq);
    }
  });
  const setSql = (t) => `UPDATE ${t} SET
      document_version_seq = (SELECT m.seq FROM temp._asdv_map m WHERE m.source_layer = ${t}.source_layer AND m.source_document_id = ${t}.source_document_id
                                AND m.source_file_hash IS ${t}.source_file_hash AND m.parser_version = ${t}.parser_version),
      normalization_version = COALESCE(normalization_version, parser_version)
    WHERE document_version_seq IS NULL AND id BETWEEN ? AND ?`;
  const out = { groups: groups.length, versions: new Set(seqs).size, lines: 0, headers: 0 };
  for (const [t, key] of [['raw_amazon_settlement_headers', 'headers'], ['raw_amazon_settlement_lines', 'lines']]) {
    const mm = db.prepare(`SELECT MIN(id) a, MAX(id) b FROM ${t} INDEXED BY ${t === 'raw_amazon_settlement_lines' ? 'idx_settle_lines_docver' : 'idx_settle_headers_docver'} WHERE document_version_seq IS NULL`).get();
    if (mm.a == null) continue;
    const upd = db.prepare(setSql(t));
    for (let a = mm.a; a <= mm.b; a += batchIds) {
      const b = Math.min(a + batchIds - 1, mm.b);
      const t0 = Date.now();
      out[key] += tx(`${key === 'lines' ? '明細' : '見出し'}の行の版 (id ${a}〜${b})`, () => upd.run(a, b).changes);
      if (key === 'lines') log(`[versions]   明細 id ${a}〜${b}: 累計 ${out.lines} 行 (この取引 ${Date.now() - t0} ms = 書き込みの lock を持った時間)`);
    }
  }
  tx('版の +1', () => { bumpSourceRevision(db); db.exec(`DROP TABLE IF EXISTS temp._asdv_map`); });
  // 要約 (1 版 = 1 取引)。今回の版だけでなく、古い印の版を全部 (前の回が途中で止まった版・行を手で直した版も)
  out.refreshed = refreshStaleVersionDetails(db, { check, log, txTimer: timer });
  done(out);
  log(`[versions] 版を付けた: 文書 ${out.groups} / 版 ${out.versions} / 明細 ${out.lines} 行 / 見出し ${out.headers} 行 / 1 取引の最長 ${out.maxTxMs} ms (${out.maxTxName ?? '-'}・取引 ${out.txCount})`);
  return out;
}

// ─── coverage の lease ───
export class LeaseLostError extends Error {
  constructor(m = 'coverage の lease が自分の世代・token でない (別の回が lease を取った) ので書かない') { super(m); this.code = 'LEASE_LOST'; }
}
export const newRunToken = () => `cov-${Date.now().toString(36)}-${crypto.randomBytes(8).toString('hex')}`;
const readLease = (db) => db.prepare(`SELECT * FROM amazon_finance_coverage_lease WHERE id = 1`).get() || null;
export { readLease };

/**
 * lease を取る (BEGIN IMMEDIATE の 1 取引)。前の持ち主が生きていれば { ok: false, held }。
 *   isAlive(pid, started_at) = retry-lock.js の isAliveNodeSince (試験は差し替え)。心拍の期限では奪わない (R18 M3)
 *   戻り = { ok: true, lease: { runToken, generation: null, pid, startedAt }, recovered }
 */
export function acquireCoverageLease(db, { isAlive, pid = process.pid, now = new Date(), runToken = newRunToken() }) {
  return db.transaction(() => {
    const cur = readLease(db);
    let recovered = null;
    if (cur && cur.run_token && !cur.released_at) {
      if (isAlive(cur.owner_pid, cur.started_at)) return { ok: false, held: { pid: cur.owner_pid, started_at: cur.started_at, generation: cur.coverage_generation } };
      recovered = { pid: cur.owner_pid, started_at: cur.started_at, generation: cur.coverage_generation };
    }
    db.prepare(`INSERT INTO amazon_finance_coverage_lease (id, run_token, coverage_generation, owner_pid, started_at, released_at, updated_at) VALUES (1, ?, NULL, ?, ?, NULL, ?)
      ON CONFLICT (id) DO UPDATE SET run_token = excluded.run_token, coverage_generation = NULL, owner_pid = excluded.owner_pid, started_at = excluded.started_at, released_at = NULL, updated_at = excluded.updated_at`)
      .run(runToken, pid, now.toISOString(), now.toISOString());
    return { ok: true, lease: { runToken, generation: null, pid, startedAt: now.toISOString() }, recovered };
  }).immediate();
}
/** lease が自分のもの (token と世代) か。取引の中で呼ぶ。違えば LeaseLostError */
export function assertLease(db, lease) {
  const cur = readLease(db);
  if (!lease || !cur || cur.released_at || cur.run_token !== lease.runToken || (cur.coverage_generation ?? null) !== (lease.generation ?? null)) {
    throw new LeaseLostError(`coverage の lease が自分のもの (token ${String(lease && lease.runToken).slice(0, 16)}… 世代 ${lease && lease.generation}) でない`
      + ` (今の lease = token ${String(cur && cur.run_token).slice(0, 16)}… 世代 ${cur && cur.coverage_generation}${cur && cur.released_at ? '・放した' : ''}) = 書かない`);
  }
  return cur;
}
/** 世代を lease に書く (同じ token のときだけ)。lease.generation も更新 */
export function setLeaseGeneration(db, lease, generation, now = new Date()) {
  db.transaction(() => {
    const cur = readLease(db);
    if (!cur || cur.released_at || cur.run_token !== lease.runToken) throw new LeaseLostError();
    db.prepare(`UPDATE amazon_finance_coverage_lease SET coverage_generation = ?, updated_at = ? WHERE id = 1`).run(generation, now.toISOString());
  }).immediate();
  lease.generation = generation;
}
/**
 * 生きている coverage の lease (放していない・持ち主が生きている)。無ければ null。
 *   人が積む入口 (初期の印・手のファイルの --queue) が、coordinator の回の途中に積まないために使う (#1567 Codex R2 High 2)。
 *   取引の中で呼ぶ (BEGIN IMMEDIATE = lease の取り直しと直列)
 */
export function liveCoverageLease(db, { isAlive }) {
  const cur = readLease(db);
  if (!cur || !cur.run_token || cur.released_at) return null;
  return isAlive(cur.owner_pid, cur.started_at) ? cur : null;
}
/** 生きている lease があれば積まずに throw (what = 積むもの) */
export function assertNoLiveCoverageLease(db, { isAlive, what }) {
  const cur = liveCoverageLease(db, { isAlive });
  if (cur) {
    throw new Error(`coordinator の回が動いている (lease の持ち主 pid ${cur.owner_pid}・開始 ${cur.started_at}・世代 ${cur.coverage_generation ?? '-'}) = ${what}を積まない`
      + ` (回の途中に積むと古い中身で complete になりうる・#1567 Codex R2 High 2)。回が終わってから積み直す`);
  }
}
/** 放す (自分の token のときだけ)。戻り = 放したか */
export function releaseCoverageLease(db, lease, now = new Date()) {
  if (!lease) return false;
  return db.transaction(() => {
    const cur = readLease(db);
    if (!cur || cur.run_token !== lease.runToken || cur.released_at) return false;
    db.prepare(`UPDATE amazon_finance_coverage_lease SET released_at = ?, updated_at = ? WHERE id = 1`).run(now.toISOString(), now.toISOString());
    return true;
  }).immediate();
}
