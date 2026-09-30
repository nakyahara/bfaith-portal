#!/usr/bin/env node
/**
 * amazon-settlement-manual-file.js — Seller Central から落とした決済のファイル (V1 / V2) を手で取り込む口 (D7b-1b-3)
 *
 * なぜ: API は 90 日より前の決済のレポートを返さない。SQLite に無い決済 (例: 2025/12/29〜2026/1/12・2026/1/12〜1/26 の 2 回分) は
 *   中原さんが Seller Central の「過去の決済情報」から V2 (や V1) のファイルを落とし、ここで順番待ちに積む。
 * 🚨 生の表に書くのは coordinator (amazon-finance-coverage-run.js) の回の中だけ (lease の世代・token を確かめる・書く前に coverage を updating)。
 *   ここは **読んで確かめて順番待ちに積むだけ** (DATA_DIR/amazon-settlement-manual/<hash>.txt と amazon_settlement_manual_files の 1 行)。
 *   次の coordinator の回が、API の取込の前に順番待ちのファイルを source_layer = manual_csv の文書の版として入れる
 *   (report ID・文書 ID は null・file hash で区別。層の順は API の版より後 = 同じ決済に API の版があればそちらを採る)。
 *
 * 使い方:
 *   node apps/warehouse/amazon-settlement-manual-file.js --file 2025-12-29_settlement.txt           → 読んで確かめるだけ (dry-run)
 *   node apps/warehouse/amazon-settlement-manual-file.js --file 2025-12-29_settlement.txt --queue   → 順番待ちに積む (次の coordinator の回で入る)
 *   node apps/warehouse/amazon-settlement-manual-file.js --list                                     → 順番待ちの一覧
 *   形式は 1 行目の列で決める (amount-type があれば V2)。--format v1|v2 で指定もできる
 * env: DATA_DIR (必須)
 */
import 'dotenv/config';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { pathToFileURL } from 'node:url';
import { prepareReportTsv, prepareV2ReportTsv, ingestSettlement } from './fetch-amazon-settlements.js';
import { assertLease } from './amazon-settlement-versions.js';

export const MANUAL_DIR = 'amazon-settlement-manual';
const sha = (s) => crypto.createHash('sha256').update(s, 'utf8').digest('hex');

export function detectFormat(tsv) {
  const head = tsv.replace(/^﻿/, '').split(/\r?\n/, 1)[0].split('\t');
  if (!head.includes('settlement-id')) throw new Error('決済のファイルでない (1 行目に settlement-id が無い)');
  return head.includes('amount-type') ? 'v2' : 'v1';
}

/** 読んで正規化する (書かない)。戻り = { format, prepared, settlementId, fileHash, header, lineCount, componentsSumMicro, totalMicro, ok, problems[] } */
export function inspectManualFile(tsv, { fileName = null, format = null } = {}) {
  const text = tsv.replace(/^﻿/, '');
  const fmt = format || detectFormat(text);
  const fileHash = sha(text);
  const problems = [];
  let prepared;
  if (fmt === 'v2') {
    prepared = prepareV2ReportTsv(text, null, 'manual-inspect', { layer: 'manual_csv', fileName });
    if (prepared.unknown.length) problems.push(`並べ直しの規則に無い組み合わせ ${JSON.stringify(prepared.unknown)}`);
    if (prepared.itemCodeUnresolved) problems.push(`品物の番号を補えないポイントの行 ${prepared.itemCodeUnresolved}`);
  } else if (fmt === 'v1') {
    prepared = prepareReportTsv(text, null, 'manual-inspect', { source: 'v1', layer: 'manual_csv', fileName, sourceFileHash: fileHash });
  } else throw new Error(`--format は v1 か v2: ${fmt}`);
  const h = prepared.headerRow;
  if (!h) problems.push('見出しの行が無い');
  const ids = new Set(prepared.lineRows.map((l) => l.source_settlement_id));
  if (h) ids.add(h.source_settlement_id);
  if (ids.size !== 1) problems.push(`決済 ID が 1 つでない (${[...ids].join(', ')})`);
  let sum = 0n;
  for (const l of prepared.lineRows) for (const c of ['price_amount_micro', 'item_related_fee_amount_micro', 'promotion_amount_micro', 'shipment_fee_amount_micro', 'order_fee_amount_micro', 'misc_fee_amount_micro', 'other_fee_amount_micro', 'direct_payment_amount_micro', 'other_amount_micro']) if (l[c] != null) sum += BigInt(l[c]);
  const total = h && h.total_amount_micro != null ? BigInt(h.total_amount_micro) : null;
  if (total != null && sum !== total) problems.push(`明細の部品の合計 (${sum}) ≠ 見出しの total (${total}) = ファイルが途中で切れている / 規則が違う`);
  if (h && h.currency_raw !== 'JPY') problems.push(`見出しの通貨が JPY でない (${h.currency_raw})`);
  return { format: fmt, prepared, settlementId: ids.size === 1 ? [...ids][0] : null, fileHash: fmt === 'v2' ? prepared.sourceFileHash : fileHash, header: h, lineCount: prepared.lineRows.length, componentsSumMicro: sum, totalMicro: total, ok: problems.length === 0, problems };
}

/**
 * その決済に API の版 (sp_api_v1 / v2) が既にあるか (R1 Medium 1)。あれば手のファイルは採られない (層の順が後) = 積んでも値は変わらない。
 *   手のファイルにだけある注文は「読み直す注文」には入るが、送るものが無い = 消える (無駄な回・紛らわしい) → 積む前に警告する
 */
export function apiVersionsOf(db, settlementId) {
  if (!settlementId) return [];
  return db.prepare(`SELECT seq, source_layer, report_id FROM amazon_settlement_document_versions WHERE settlement_id = ? AND source_layer IN ('sp_api_v1', 'sp_api_v2') ORDER BY seq`).all(settlementId);
}

/** 順番待ちに積む (ファイルを DATA_DIR に写す + 表に 1 行)。同じ file hash は 1 回だけ。戻りの apiVersions があれば呼び手は警告を出す */
export function queueManualFile(db, dataDir, tsv, { fileName, format = null, now = new Date() }) {
  const x = inspectManualFile(tsv, { fileName, format });
  if (!x.ok) throw new Error(`取り込めない: ${x.problems.join(' / ')}`);
  x.apiVersions = apiVersionsOf(db, x.settlementId);
  const dir = path.join(dataDir, MANUAL_DIR);
  fs.mkdirSync(dir, { recursive: true });
  const stored = path.join(dir, `${x.fileHash}.txt`);
  fs.writeFileSync(stored, tsv.replace(/^﻿/, ''));
  const r = db.prepare(`INSERT OR IGNORE INTO amazon_settlement_manual_files (file_hash, file_name, stored_path, format, settlement_id, queued_at) VALUES (?, ?, ?, ?, ?, ?)`)
    .run(x.fileHash, fileName, stored, x.format, x.settlementId, now.toISOString());
  return { ...x, queued: r.changes === 1, storedPath: stored };
}

/**
 * coordinator の回の中で順番待ちのファイルを入れる (lease を渡す = 生の表の取引の中で確かめる)。
 * 戻り = [{ id, settlementId, status: 'ingested' | 'failed', note, documentVersionSeq }]
 */
export function ingestQueuedManualFiles(db, { lease, generation = null, log = () => {}, now = null }) {
  const out = [];
  for (const q of db.prepare(`SELECT * FROM amazon_settlement_manual_files WHERE ingested_at IS NULL ORDER BY id`).all()) {
    let status = 'failed', note = null, seq = null;
    try {
      const text = fs.readFileSync(q.stored_path, 'utf8');
      const x = inspectManualFile(text, { fileName: q.file_name, format: q.format });
      if (x.fileHash !== q.file_hash) throw new Error(`ファイルの hash が積んだときと違う (${x.fileHash.slice(0, 12)}… / ${q.file_hash.slice(0, 12)}…)`);
      if (!x.ok) throw new Error(x.problems.join(' / '));
      const r = ingestSettlement(db, x.prepared.headerRow, x.prepared.lineRows, x.prepared.ctx, { lease, now });
      status = 'ingested'; seq = r.documentVersionSeq;
      note = `明細 ${r.lineInserted} 行を入れた (版 #${r.documentVersionSeq}${r.selectedChanged ? '・採る版が変わった' : ''})`;
    } catch (e) {
      if (e && e.code === 'LEASE_LOST') throw e;
      note = String(e && e.message).slice(0, 300);
    }
    if (status === 'ingested') {
      db.transaction(() => { assertLease(db, lease); db.prepare(`UPDATE amazon_settlement_manual_files SET ingested_at = ?, ingest_generation = ?, ingest_note = ? WHERE id = ?`).run(new Date().toISOString(), generation, note, q.id); }).immediate();
    } else {
      db.prepare(`UPDATE amazon_settlement_manual_files SET ingest_note = ? WHERE id = ?`).run(note, q.id);
    }
    log(`[manual] ${status === 'ingested' ? '✅' : '❌'} 手の決済のファイル #${q.id} (決済 ${q.settlement_id}): ${note}`);
    out.push({ id: q.id, settlementId: q.settlement_id, status, note, documentVersionSeq: seq });
  }
  return out;
}

const isDirectRun = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isDirectRun) {
  (async () => {
    const argv = process.argv.slice(2);
    const a = { file: null, queue: false, list: false, format: null };
    for (let i = 0; i < argv.length; i++) {
      if (argv[i] === '--file') a.file = argv[++i];
      else if (argv[i] === '--queue') a.queue = true;
      else if (argv[i] === '--list') a.list = true;
      else if (argv[i] === '--format') a.format = argv[++i];
      else throw new Error(`知らない引数: ${argv[i]}`);
    }
    if (!process.env.DATA_DIR) throw new Error('DATA_DIR が無い');
    const { initDB, getDB } = await import('./db.js');
    await initDB();
    const db = getDB();
    if (a.list) {
      for (const q of db.prepare(`SELECT * FROM amazon_settlement_manual_files ORDER BY id`).all()) console.log(`  #${q.id} 決済 ${q.settlement_id} ${q.format} ${q.file_name}: ${q.ingested_at ? `入れた ${q.ingested_at}` : '順番待ち'}${q.ingest_note ? ` (${q.ingest_note})` : ''}`);
      return;
    }
    if (!a.file) throw new Error('--file か --list');
    const tsv = fs.readFileSync(a.file, 'utf8');
    const x = inspectManualFile(tsv, { fileName: path.basename(a.file), format: a.format });
    console.log(`[manual] ${path.basename(a.file)}: 形式 ${x.format}・決済 ${x.settlementId}・期間 ${x.header?.settlement_start_date} 〜 ${x.header?.settlement_end_date}・total ${x.totalMicro}・明細 ${x.lineCount} 行・部品の合計 ${x.componentsSumMicro}`);
    if (!x.ok) { console.log(`[manual] ❌ 取り込めない: ${x.problems.join(' / ')}`); process.exitCode = 1; return; }
    const api = apiVersionsOf(db, x.settlementId);
    if (api.length) console.log(`[manual] ⚠️ 決済 ${x.settlementId} には API の版が既にある (${api.map((v) => `#${v.seq} ${v.source_layer} ${v.report_id}`).join(' / ')}) = 手のファイルは採られない (API の版が先)。積む必要があるか確かめる`);
    if (!a.queue) { console.log('[manual] dry-run (積まない)。積むなら --queue (次の coordinator の回で入る)'); return; }
    const q = queueManualFile(db, process.env.DATA_DIR, tsv, { fileName: path.basename(a.file), format: a.format });
    console.log(q.queued ? `[manual] ✅ 順番待ちに積んだ (${q.storedPath})。次の coordinator の回で入る` : '[manual] 同じファイルは積んである');
  })().catch((e) => { console.error(`❌ 手の決済のファイル: ${e.message}`); process.exitCode = 1; });
}
