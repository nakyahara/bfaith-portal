/**
 * いろは在庫化作業アプリ — Notion → f_iroha_tasks の移行ツール (要件定義 v1.1 §F / Codex 設計相談 R3)
 *
 * 手順 (全て管理画面から。本取込以外は DB を書き換えない):
 *   1. survey  : Notion の全ページを取得して実態を集計 (ステータス別件数・未知値・台帳キー/destination の欠落と重複・
 *                作業時間/写真の孤立)。原本 JSON を DATA_DIR/iroha-migration/ に保存 (移行事故時の証跡)
 *   2. plan    : 各ページの写像 (mapLegacyStatus) と取込可否・要確認・警告を行にする = dry-run。CSV でも出せる
 *   3. apply   : 取込可の行を 1 トランザクションで upsert (notion_page_id で冪等) → sessions/media/events の task_id をバックフィル
 *   4. reconcile: Notion の page ID 集合と tasks の集合差分 (取込済 + 明示除外 + 要確認 = 全件 になるか)
 *   差分取込 = since (last_edited_time) 付きで 1〜3 を回す (切替直前に実行)。同時刻境界の取りこぼしを避けるため数分前から
 *
 * ⚠ 完了カード (棚入完了/対象外/取消) も closed として取り込む (作業時間・写真の履歴のため。一覧には出ない — 中原さん 9/3)
 */
import fs from 'fs';
import path from 'path';
import { getDB } from './db.js';
import { queryPages, parsePage } from './notion-read.js';
import { ensureCardSchema, isNotionConfigured } from '../inbound-check/notion.js';
import { mapLegacyStatus, DEFAULT_FACILITY, OPEN_STATUSES } from './tasks.js';
import { upsertTaskFromImport, backfillTaskIds, listOrphans, countTasksByStatus } from './tasks-db.js';

const MIG_DIR = process.env.DATA_DIR ? path.join(process.env.DATA_DIR, 'iroha-migration') : 'data/iroha-migration';
const MAX_PAGES = 100;   // 100 × 100 = 10,000 枚まで (完了カードが溜まっている想定)
const STATUS_PROP = 'ステータス';

const utcNow = () => new Date().toISOString();
const numOrNull = (v) => (v == null || v === '' || Number.isNaN(Number(v)) ? null : Number(v));

/**
 * Notion の全ページ (アーカイブ除く) を取得してパース。since を渡すと last_edited_time 以降だけ (差分取込)
 * @returns {{pages: Array, truncated: boolean, fetchedAt: string}}
 */
export async function fetchAllPages({ since = null } = {}) {
  if (!isNotionConfigured()) throw new Error('Notion 連携が未設定です (NOTION_TOKEN / INBOUND_CHECK_NOTION_DB_ID)');
  await ensureCardSchema();
  const filter = since ? { timestamp: 'last_edited_time', last_edited_time: { on_or_after: new Date(since).toISOString() } } : null;
  const r = await queryPages(filter, MAX_PAGES);
  const pages = r.results.map((p) => ({ ...parsePage(p), createdTime: p.created_time || null, archived: !!p.archived }));
  return { pages, truncated: r.truncated, fetchedAt: utcNow() };
}

/** 実態調査 (Notion を読むだけ。原本 JSON を保存) */
export async function surveyNotion({ since = null, save = true } = {}) {
  const { pages, truncated, fetchedAt } = await fetchAllPages({ since });
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
    else { if (seenDest.has(dest)) issues.dupDestination.push({ destinationId: dest, pageIds: [seenDest.get(dest), p.pageId] }); else seenDest.set(dest, p.pageId); }
    if (p.dedupeKey) { if (seenDedupe.has(p.dedupeKey)) issues.dupDedupe.push({ dedupeKey: p.dedupeKey, pageIds: [seenDedupe.get(p.dedupeKey), p.pageId] }); else seenDedupe.set(p.dedupeKey, p.pageId); }
  }
  // 作業時間・写真・履歴が参照している page_id のうち、今回の取得に無いもの (= 削除/アーカイブ済み or 別 DB)
  const pageIds = new Set(pages.map((p) => p.pageId));
  const orphan = (table) => db.prepare(`SELECT DISTINCT page_id FROM ${table} WHERE page_id IS NOT NULL`).all().map((r) => r.page_id).filter((id) => !pageIds.has(id));
  const orphans = { sessions: orphan('f_iroha_work_sessions'), media: orphan('f_iroha_card_media') };
  let file = null;
  if (save) {
    fs.mkdirSync(MIG_DIR, { recursive: true });
    file = path.join(MIG_DIR, `survey-${fetchedAt.replace(/[:.]/g, '-')}.json`);
    fs.writeFileSync(file, JSON.stringify({ fetchedAt, since, truncated, count: pages.length, byStatus, issues, orphans, pages }, null, 1));
  }
  return { fetchedAt, since, truncated, count: pages.length, byStatus, issues, orphans, file, pages };
}

/**
 * 写像 (dry-run)。1 ページ = 1 行。will_import=false の行は apply で飛ばす (rejected / destination 重複の 2 枚目以降は
 * destination_id を外して取り込む)。
 */
export function planImport(pages) {
  const rows = [];
  const seenDest = new Map();
  for (const p of pages) {
    const legacy = p.props[STATUS_PROP] || '';
    const m = mapLegacyStatus(legacy);
    const warnings = [];
    let destinationId = numOrNull(p.props.destination_id);
    if (destinationId == null) warnings.push('no_destination');
    else if (seenDest.has(destinationId)) { warnings.push(`dup_destination:${seenDest.get(destinationId)}`); destinationId = null; }
    else seenDest.set(destinationId, p.pageId);
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
      facility_code: m.facility || DEFAULT_FACILITY,
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
      migration_review: m.confidence === 'needs_review' ? 1 : 0,
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
 * 途中で不変条件違反が出たら全部戻す (半端な状態を残さない)
 */
export function applyImport(rows, { batchId = null, actor = null } = {}) {
  const db = getDB();
  const id = batchId || `mig-${utcNow().replace(/[:.]/g, '-')}`;
  const now = utcNow();
  const out = { batchId: id, inserted: 0, updated: 0, kept: 0, skipped: 0, backfill: null, at: now, actor };
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
  })();
  fs.mkdirSync(MIG_DIR, { recursive: true });
  fs.writeFileSync(path.join(MIG_DIR, `apply-${id}.json`), JSON.stringify({ ...out, rows: rows.map((r) => ({ page: r.notion_page_id, status: r.mapped_status, will: r.will_import, warnings: r.warnings })) }, null, 1));
  return out;
}

/**
 * 照合: Notion の page ID 集合と tasks の集合差分。
 *   missing = 取込対象なのに tasks に無い / extra = tasks にあるが Notion に無い (削除・アーカイブ) / rejected = 取り込まない
 */
export function reconcile(rows) {
  const db = getDB();
  const taskPages = new Set(db.prepare('SELECT notion_page_id FROM f_iroha_tasks WHERE notion_page_id IS NOT NULL').all().map((r) => r.notion_page_id));
  const notionPages = new Set(rows.map((r) => r.notion_page_id));
  const missing = rows.filter((r) => r.will_import && !taskPages.has(r.notion_page_id)).map((r) => r.notion_page_id);
  const rejected = rows.filter((r) => !r.will_import).map((r) => r.notion_page_id);
  const extra = [...taskPages].filter((id) => !notionPages.has(id));
  const counts = countTasksByStatus();
  const notionOpen = rows.filter((r) => r.will_import && OPEN_STATUSES.includes(r.mapped_status)).length;
  return {
    notionTotal: rows.length, notionOpen, willImport: rows.length - rejected.length,
    tasksTotal: counts.total, tasksByStatus: counts.byStatus, tasksWithPage: taskPages.size,
    missing, rejected, extra, orphans: listOrphans(50),
    balanced: missing.length === 0 && (rows.length === (rows.length - rejected.length) + rejected.length),
  };
}

/** 管理画面用: 直近の調査/取込ファイル一覧 */
export function listMigrationFiles(limit = 20) {
  if (!fs.existsSync(MIG_DIR)) return [];
  return fs.readdirSync(MIG_DIR).filter((f) => f.endsWith('.json')).sort().reverse().slice(0, limit)
    .map((f) => ({ name: f, size: fs.statSync(path.join(MIG_DIR, f)).size }));
}
