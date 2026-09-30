#!/usr/bin/env node
/**
 * check-settlement-v1-v2.js — 各決済の「今採っている版 (過去の V1)」と「V2 のレポート」の明細の要約 (detail_digest) が一致するかを **読むだけ** で確かめる (#1567 R1 Medium 3)
 *
 * なぜ: coordinator の初回の回で、直近 90 日の決済の V2 が版として入り、採る版が V1 から V2 に替わる。
 *   中身が同じなら値は変わらない (satisfied_by_selected_settlement)。違えば coverage は complete にならない・値が動く = 替わる前に知っておく。
 *
 * 🚨 書かない: warehouse.db は読むだけで開く (readonly)。SP-API は一覧とダウンロードだけ (読む)。V2 の中身はメモリで正規化して要約を作る。
 *   過去の行に版が付いている (migrate-settlement-document-versions.js --commit の後) DB で流す = **本番の DB のコピー** で:
 *     1. 本番の warehouse.db をコピー (例 C:\tmp\wh-copy\warehouse.db)
 *     2. $env:DATA_DIR = 'C:\tmp\wh-copy'; node apps/warehouse/migrate-settlement-document-versions.js --commit   (コピーに版を付ける)
 *     3. $env:DATA_DIR = 'C:\tmp\wh-copy'; node apps/warehouse/check-settlement-v1-v2.js   (SP-API の V2 を読んで比べる)
 * 出力: 決済ごとに 一致 / 違う (行の数・digest・部品の合計・見出しの total) / 版が無い。違うものがあれば exit 1
 */
import 'dotenv/config';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import Database from 'better-sqlite3';
import { prepareV2ReportTsv, listSettlementReports, downloadReportTsv, spClients, SOURCES } from './fetch-amazon-settlements.js';
import { detailDigestOfRows, selectDocumentVersions, versionDetailValid, header0MultiVersionSettlements } from './amazon-settlement-versions.js';

/** 1 本の V2 の TSV と DB の採った版を比べる (試験から呼ぶ) */
export function compareV2WithSelected(db, v2Tsv, reportId) {
  const p = prepareV2ReportTsv(v2Tsv, reportId, 'check');
  const sid = p.headerRow ? p.headerRow.source_settlement_id : (p.lineRows[0] && p.lineRows[0].source_settlement_id);
  const d = detailDigestOfRows(p.lineRows);
  const total = p.headerRow && p.headerRow.total_amount_micro != null ? BigInt(p.headerRow.total_amount_micro) : null;
  const v2Valid = p.unknown.length === 0 && !p.itemCodeUnresolved && total != null && total === d.componentsSumMicro;
  const vs = db.prepare(`SELECT * FROM amazon_settlement_document_versions WHERE settlement_id = ?`).all(sid);
  const sel = selectDocumentVersions(vs).get(sid) || null;
  const out = { reportId, settlementId: sid, v2: { lineCount: d.lineCount, digest: d.detailDigest, sum: String(d.componentsSumMicro), total: total == null ? null : String(total), valid: v2Valid, unknown: p.unknown.length },
    selected: sel ? { seq: sel.seq, layer: sel.source_layer, reportId: sel.report_id, lineCount: sel.line_count, digest: sel.detail_digest, valid: versionDetailValid(sel), stale: sel.detail_stale, headerCount: sel.header_count } : null,
    versions: vs.length, header0Versions: vs.filter((v) => Number(v.header_count) === 0).length };
  // no_version = SQLite にその決済の版が無い (V2 だけで入る) / blocked = 版はあるが採れない / provisional = 良い版が無く壊れた版を仮に採っている (#1567 R2 L1)
  out.status = !vs.length ? 'no_version' : !sel ? 'blocked' : sel.detail_stale ? 'selected_stale' : Number(sel.detail_valid) !== 1 ? 'provisional'
    : (sel.line_count === d.lineCount && sel.detail_digest === d.detailDigest ? 'match' : 'differs');
  return out;
}

async function main() {
  const dataDir = (process.env.DATA_DIR || '').trim();
  if (!dataDir) throw new Error('DATA_DIR が無い (本番の DB のコピーを指す)');
  const db = new Database(path.join(dataDir, 'warehouse.db'), { readonly: true, fileMustExist: true });
  try {
    const { sp } = spClients({ withInventory: false });
    const reports = (await listSettlementReports(sp, SOURCES.v2.reportType)).filter((r) => r.processingStatus === 'DONE' && r.reportDocumentId);
    console.log(`[check] V2 のレポート ${reports.length} 本 (DONE)`);
    const h0 = header0MultiVersionSettlements(db);
    console.log(h0.length ? `[check] 🚨 見出し 0 行の版があり、ほかの版もある決済 ${h0.length}: ${h0.map((x) => `${x.settlement_id} (版 ${x.versions}・見出し 0 行 ${x.header0})`).join(' / ')} = どの版を採るか人が確かめる` : '[check] 見出し 0 行の版があり、ほかの版もある決済 0 (✅)');
    let bad = 0;
    for (const r of reports) {
      const tsv = await downloadReportTsv(sp, r.reportDocumentId);
      const c = compareV2WithSelected(db, tsv, r.reportId);
      if (c.status !== 'match' && c.status !== 'no_version') bad++;
      const s = c.selected;
      console.log(`  ${c.status === 'match' ? '✅ 一致' : c.status === 'no_version' ? '・ SQLite に版が無い (V2 だけで入る)' : '❌ ' + c.status} 決済 ${c.settlementId} (report ${r.reportId}): V2 ${c.v2.lineCount} 行 ${c.v2.digest.slice(0, 12)}… 合計 ${c.v2.sum} / total ${c.v2.total}${c.v2.valid ? '' : ' ⚠️V2 の中身が確かでない'}`
        + (s ? ` | 採っている版 #${s.seq} ${s.layer} ${s.reportId} 見出し ${s.headerCount} 行 ${s.lineCount} 行 ${String(s.digest).slice(0, 12)}…${s.valid ? '' : ' ⚠️'}` : '')
        + ` | この決済の版 ${c.versions}${c.header0Versions ? ` (🚨 見出し 0 行の版 ${c.header0Versions})` : ''}`);
    }
    console.log(bad ? `❌ 違う決済 ${bad} = coordinator の初回で採る版が替わると値が動く / coverage は complete にならない (report_selected_differs)。中身を確かめてから流す` : '✅ V2 と採っている版の明細は全部一致 (または SQLite に無い決済)');
    process.exitCode = bad ? 1 : 0;
  } finally { db.close(); }
}

const isDirectRun = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isDirectRun) main().catch((e) => { console.error(`❌ V1 / V2 の確かめ: ${e.message}`); process.exitCode = 1; });
