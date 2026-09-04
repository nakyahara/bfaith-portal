/**
 * いろは在庫化作業アプリ — Notion → f_iroha_tasks の移行ツール (要件定義 v1.1 §F / Codex 設計相談 R3 / レビュー A1 R1)
 *
 * 手順 (全て管理画面から。本取込以外は Notion もローカル DB も書き換えない):
 *   1. survey  : Notion の全ページを取得して実態を集計 (ステータス別件数・未知値・台帳キー/destination の欠落と重複・
 *                作業時間/写真の孤立)。**Notion の生レスポンス**を DATA_DIR/iroha-migration/survey-raw-*.json に保存
 *                (parser に不具合があっても再解析できる証跡)。スキーマは checkCardSchema で検証だけ (PATCH しない)
 *   2. plan    : 各ページの写像 (mapLegacyStatus) と取込可否・要確認・警告を行にする = dry-run。既存 tasks との
 *                destination 衝突もここで分類 (衝突は destination を外して要確認)。CSV でも出せる
 *   3. apply   : 取込可の行を 1 トランザクションで upsert (notion_page_id で冪等) → sessions/media/events の task_id を
 *                バックフィル。途中で一意制約や不変条件に当たれば全部戻す
 *   4. reconcile: 全件照合 (mode=full: 欠けも余りも 0 で balanced) と差分照合 (mode=delta: 欠け 0 のみ)
 *   差分取込 = since〜cutoff (取得開始時刻) の窓で 1〜3 を回し、次回は cutoff を since にする。取得は last_edited_time 昇順で固定
 *
 * ⚠ 完了カード (棚入完了/対象外/取消) も closed として取り込む (作業時間・写真の履歴のため。一覧には出ない — 中原さん 9/3)
 */
import fs from 'fs';
import path from 'path';
import { getDB, setMetaValue } from './db.js';
import { queryPages, parsePage } from './notion-read.js';
import { checkCardSchema, isNotionConfigured } from '../inbound-check/notion.js';
import { mapLegacyStatus, OPEN_STATUSES } from './tasks.js';
import { upsertTaskFromImport, backfillTaskIds, listOrphans, countTasksByStatus } from './tasks-db.js';

const MIG_DIR = process.env.DATA_DIR ? path.join(process.env.DATA_DIR, 'iroha-migration') : 'data/iroha-migration';
const MAX_PAGES = 100;   // 100 × 100 = 10,000 枚まで (完了カードが溜まっている想定)
const STATUS_PROP = 'ステータス';
const SORTS = [{ timestamp: 'last_edited_time', direction: 'ascending' }];

const utcNow = () => new Date().toISOString();
const numOrNull = (v) => (v == null || v === '' || Number.isNaN(Number(v)) ? null : Number(v));
const tsName = (iso) => iso.replace(/[:.]/g, '-');

/**
 * Notion の全ページ (アーカイブ除く) を取得してパース。since を渡すと since〜cutoff (取得開始時刻) の窓だけ (差分取込)。
 * スキーマは検証だけで書き換えない (Codex A1 R1 #1)
 * @returns {{pages: Array, rawPages: Array, truncated: boolean, fetchedAt: string, since: string|null, cutoff: string}}
 */
export async function fetchAllPages({ since = null } = {}) {
  if (!isNotionConfigured()) throw new Error('Notion 連携が未設定です (NOTION_TOKEN / INBOUND_CHECK_NOTION_DB_ID)');
  await checkCardSchema();
  const cutoff = utcNow();   // 窓の上限を取得開始時点で固定 (走査中の編集は次回の差分で拾う — Codex A1 R1 #6)
  let filter = null;
  if (since) {
    filter = { and: [
      { timestamp: 'last_edited_time', last_edited_time: { on_or_after: new Date(since).toISOString() } },
      { timestamp: 'last_edited_time', last_edited_time: { on_or_before: cutoff } },
    ] };
  }
  const r = await queryPages(filter, MAX_PAGES, { sorts: SORTS });
  const rawPages = r.results;
  const pages = rawPages.map((p) => ({ ...parsePage(p), createdTime: p.created_time || null, archived: !!p.archived }));
  return { pages, rawPages, truncated: r.truncated, fetchedAt: cutoff, since, cutoff };
}

/** 実態調査 (Notion を読むだけ。生レスポンスと集計を保存) */
export async function surveyNotion({ since = null, save = true } = {}) {
  const { pages, rawPages, truncated, fetchedAt, cutoff } = await fetchAllPages({ since });
  const db = getDB();
  const byStatus = {};
  const issues = { unknownStatus: [], noProductCode: [], noDedupeKey: [], noDestination: [], dupDestination: [], dupDedupe: [] };
  const seenDest = new Map(); const seenDedupe = new Map();
  for (const p of pages) {
    const st = p.props[STATUS_PROP] || '(未設定)';
    byStatus[st] = (byStatus[st] || 0) + 1;
    if (mapLegacyStatus(p.props[STATUS_PROP]).confidence === 'rejected') issues.unknownStatus.push({ pageId: p.pageId, status: st, title: p.title });
    if (!p.productCode) issues.noProductCode.push({ pageId: p.pageId, title: p.title });
    if (!p.dedupeKey) issues.noDedupeKey.push({ pageId: p.pageId, title: p.title });
    const dest = numOrNull(p.props.destination_id);
    if (dest == null) issues.noDestination.push({ pageId: p.pageId, title: p.title });
    else if (seenDest.has(dest)) issues.dupDestination.push({ destinationId: dest, pageIds: [seenDest.get(dest), p.pageId] });
    else seenDest.set(dest, p.pageId);
    if (p.dedupeKey) {
      if (seenDedupe.has(p.dedupeKey)) issues.dupDedupe.push({ dedupeKey: p.dedupeKey, pageIds: [seenDedupe.get(p.dedupeKey), p.pageId] });
      else seenDedupe.set(p.dedupeKey, p.pageId);
    }
  }
  // 孤立 = DB 基準 (task に紐づいていない page_id)。全件調査のときだけ「Notion にも無い」も出す
  // (差分調査では未更新の正常ページが取得に入らないため — Codex A1 R1 #15)
  const unlinked = (table) => db.prepare(`SELECT DISTINCT page_id FROM ${table} WHERE task_id IS NULL AND page_id IS NOT NULL`).all().map((r) => r.page_id);
  const pageIds = new Set(pages.map((p) => p.pageId));
  const orphans = { unlinked: { sessions: unlinked('f_iroha_work_sessions'), media: unlinked('f_iroha_card_media') } };
  if (!since) {
    orphans.missingInNotion = {
      sessions: orphans.unlinked.sessions.filter((id) => !pageIds.has(id)),
      media: orphans.unlinked.media.filter((id) => !pageIds.has(id)),
    };
  }
  let file = null, rawFile = null;
  if (save) {
    fs.mkdirSync(MIG_DIR, { recursive: true });
    rawFile = path.join(MIG_DIR, `survey-raw-${tsName(fetchedAt)}.json`);
    fs.writeFileSync(rawFile, JSON.stringify({ fetchedAt, since, cutoff, truncated, count: rawPages.length, pages: rawPages }));
    file = path.join(MIG_DIR, `survey-${tsName(fetchedAt)}.json`);
    fs.writeFileSync(file, JSON.stringify({ fetchedAt, since, cutoff, truncated, count: pages.length, byStatus, issues, orphans, pages }, null, 1));
  }
  return { fetchedAt, since, cutoff, truncated, count: pages.length, byStatus, issues, orphans, file, rawFile, pages };
}

/**
 * 写像 (dry-run)。1 ページ = 1 行。will_import=false の行は apply で飛ばす。
 * destination_id の衝突 (今回の取得内の重複 / 既存 tasks で別ページが持っている) は destination_id を外して取り込み、要確認にする
 * — apply が一意制約で丸ごと戻らないようにここで分類する (Codex A1 R1 #3)
 */
export function planImport(pages, { existingByDestination = null } = {}) {
  const existing = existingByDestination || new Map(getDB().prepare('SELECT id, destination_id, notion_page_id FROM f_iroha_tasks WHERE destination_id IS NOT NULL').all()
    .map((r) => [r.destination_id, r]));
  const rows = [];
  const seenDest = new Map();
  for (const p of pages) {
    const legacy = p.props[STATUS_PROP] || '';
    const m = mapLegacyStatus(legacy);
    const warnings = [];
    let review = m.confidence === 'needs_review';
    let destinationId = numOrNull(p.props.destination_id);
    if (destinationId == null) warnings.push('no_destination');
    else if (seenDest.has(destinationId)) { warnings.push(`dup_destination:${seenDest.get(destinationId)}`); destinationId = null; review = true; }
    else {
      const ex = existing.get(destinationId);
      if (ex && ex.notion_page_id !== p.pageId) { warnings.push(`dup_destination_db:task${ex.id}`); destinationId = null; review = true; }
      else seenDest.set(destinationId, p.pageId);
    }
    if (!p.productCode) warnings.push('no_product_code');
    if (p.props['数量'] == null) warnings.push('no_qty');
    if (m.confidence === 'needs_review') warnings.push('needs_review');
    const status = m.status;
    // 時刻は Notion から正確に復元できない (作成日時・最終更新で近似 — 警告)
    let startedAt = null, readyAt = null, closedAt = null;
    if (status && status !== 'not_started') { startedAt = p.createdTime || null; if (!p.createdTime) warnings.push('started_at_unknown'); }
    if (status === 'ready_for_stocking') readyAt = p.lastEditedTime || null;
    if (status === 'closed') { closedAt = p.lastEditedTime || null; warnings.push('closed_at_approx'); }
    const props = p.props || {};
    rows.push({
      notion_page_id: p.pageId, title: p.title, legacy_status: legacy || null,
      mapped_status: status, close_reason: m.close_reason || null, hold_reason_code: m.hold_reason || null,
      facility_code: m.facility || null,   // ⭐Notion のステータスに施設名が無ければ「未定」(要件 §W-2)
      destination_id: destinationId,
      product_code: p.productCode || null, product_name: p.title || null, qty: numOrNull(props['数量']),
      arrival_date: props['入庫日'] || null, ar_no: props['入荷管理番号'] || null, barcode: props['バーコード'] || null,
      expiry: props['有効期限'] || null, supplier: props['取引先'] || null, handling: props['取扱区分'] || null,
      master_snapshot: {
        material_code: props['資材セットID'] || null, storage_container: props['収納容器'] || null,
        units_per_container: numOrNull(props['入数']), process_count: numOrNull(props['工程数']), note: props['備考'] || null,
      },
      payload: props,
      started_at: startedAt, ready_at: readyAt, closed_at: closedAt,
      mapping_confidence: m.confidence, mapping_note: m.note || null, warnings,
      migration_review: review ? 1 : 0,
      will_import: m.confidence !== 'rejected',
      skip_reason: m.confidence === 'rejected' ? m.note : null,
      url: p.url || null, last_edited_time: p.lastEditedTime || null,
    });
  }
  const summary = { total: rows.length, willImport: 0, rejected: 0, needsReview: 0, byMapped: {}, warnings: {} };
  for (const r of rows) {
    if (r.will_import) summary.willImport++; else summary.rejected++;
    if (r.migration_review) summary.needsReview++;
    const k = r.mapped_status ? `${r.mapped_status}${r.close_reason ? ':' + r.close_reason : ''}${r.hold_reason_code ? ':' + r.hold_reason_code : ''}` : '(rejected)';
    summary.byMapped[k] = (summary.byMapped[k] || 0) + 1;
    for (const w of r.warnings) { const key = w.split(':')[0]; summary.warnings[key] = (summary.warnings[key] || 0) + 1; }
  }
  return { rows, summary };
}

const CSV_COLS = ['notion_page_id', 'title', 'legacy_status', 'mapped_status', 'close_reason', 'hold_reason_code', 'facility_code', 'destination_id',
  'product_code', 'qty', 'arrival_date', 'mapping_confidence', 'migration_review', 'warnings', 'will_import', 'skip_reason', 'url'];
export function planToCsv(rows) {
  const esc = (v) => { const s = v == null ? '' : (Array.isArray(v) ? v.join('|') : String(v)); return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s; };
  return '﻿' + [CSV_COLS.join(','), ...rows.map((r) => CSV_COLS.map((c) => esc(r[c])).join(','))].join('\n');
}

/**
 * 本取込。will_import の行だけ、1 トランザクションで upsert (冪等) → task_id バックフィル。
 * 途中で一意制約・不変条件に当たったら全部戻す (半端な状態を残さない)。
 * 証跡ファイルの書き込みは DB の commit 後 — 失敗しても取込は成功として返し、journal に理由を載せる (Codex A1 R1 #13)
 */
export function applyImport(rows, { batchId = null, actor = null } = {}) {
  const db = getDB();
  const id = batchId || `mig-${tsName(utcNow())}`;
  const now = utcNow();
  const out = { batchId: id, inserted: 0, updated: 0, kept: 0, skipped: 0, backfill: null, at: now, actor, journal: null };
  db.transaction(() => {
    for (const r of rows) {
      if (!r.will_import) { out.skipped++; continue; }
      const res = upsertTaskFromImport({
        notion_page_id: r.notion_page_id, legacy_status: r.legacy_status, status: r.mapped_status, close_reason: r.close_reason,
        facility_code: r.facility_code, hold_reason_code: r.hold_reason_code, hold_reason_note: null,
        destination_id: r.destination_id, product_code: r.product_code, product_name: r.product_name, qty: r.qty,
        arrival_date: r.arrival_date, ar_no: r.ar_no, barcode: r.barcode, expiry: r.expiry, supplier: r.supplier, handling: r.handling,
        master_snapshot: r.master_snapshot, payload: r.payload, started_at: r.started_at, ready_at: r.ready_at, closed_at: r.closed_at,
        migration_review: r.migration_review, migration_note: r.mapping_note,
      }, { batchId: id, now });
      out[res.action]++;
    }
    out.backfill = backfillTaskIds();
    // 「Notion の状態をいつ取り込んだか」— 下見の詳細で API 取得時刻と区別して出す (Codex R4 Q5)
    setMetaValue('last_import_at', now);
    setMetaValue('last_import_batch', id);
  })();
  try {
    fs.mkdirSync(MIG_DIR, { recursive: true });
    out.journal = 'saved';   // 証跡ファイル自身にも saved と残す (応答と食い違わない — Codex A1 R2 #3)
    fs.writeFileSync(path.join(MIG_DIR, `apply-${id}.json`),
      JSON.stringify({ ...out, rows: rows.map((r) => ({ page: r.notion_page_id, status: r.mapped_status, will: r.will_import, warnings: r.warnings })) }, null, 1));
  } catch (e) {
    // パスや OS エラーの文言は画面に出さない (ログに詳細・応答は参照番号 — Codex A1 R2 #2)
    const ref = Date.now().toString(36);
    out.journal = `failed (参照 ${ref})`;
    console.error(`[iroha-work migration ${ref}] 取込は完了したが証跡ファイルを書けませんでした`, e);
  }
  return out;
}

/**
 * 照合: Notion の page ID 集合と tasks の集合差分。
 *   mode=full  : 全件調査の結果に対して。missing (取込対象なのに無い) も extra (tasks にあるが Notion に無い = 削除/アーカイブ) も 0 で balanced
 *   mode=delta : 差分調査の結果に対して。rows は窓の中だけなので extra は評価しない (missing 0 で balanced) — Codex A1 R1 #4
 */
export function reconcile(rows, { mode = 'full' } = {}) {
  const db = getDB();
  const taskPages = new Set(db.prepare('SELECT notion_page_id FROM f_iroha_tasks WHERE notion_page_id IS NOT NULL').all().map((r) => r.notion_page_id));
  const notionPages = new Set(rows.map((r) => r.notion_page_id));
  const missing = rows.filter((r) => r.will_import && !taskPages.has(r.notion_page_id)).map((r) => r.notion_page_id);
  const rejected = rows.filter((r) => !r.will_import).map((r) => r.notion_page_id);
  const extra = mode === 'full' ? [...taskPages].filter((id) => !notionPages.has(id)) : null;
  const counts = countTasksByStatus();
  const notionOpen = rows.filter((r) => r.will_import && OPEN_STATUSES.includes(r.mapped_status)).length;
  return {
    mode, notionTotal: rows.length, notionOpen, willImport: rows.length - rejected.length,
    tasksTotal: counts.total, tasksByStatus: counts.byStatus, tasksWithPage: taskPages.size,
    missing, rejected, extra, unlinked: listOrphans(50),
    balanced: missing.length === 0 && (mode !== 'full' || extra.length === 0),
  };
}

/** 管理画面用: 直近の調査/取込ファイル一覧 */
export function listMigrationFiles(limit = 20) {
  if (!fs.existsSync(MIG_DIR)) return [];
  return fs.readdirSync(MIG_DIR).filter((f) => f.endsWith('.json')).sort().reverse().slice(0, limit)
    .map((f) => ({ name: f, size: fs.statSync(path.join(MIG_DIR, f)).size }));
}
