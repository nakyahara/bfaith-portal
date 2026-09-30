#!/usr/bin/env node
/**
 * migrate-settlement-document-versions.js — 決済の生の行 (過去の行) に文書の版を付ける (D-66・D7b-1b-3)
 *
 * 設計 = AI_reference システム設計/CompanyDB構想/13_Amazon利益のmart_設計_20260930.md D-66
 *   過去の行 (reportDocumentId を保存していない) = report_document_id null で版を作る。規則 = (source_layer, source_document_id (= report ID),
 *   source_file_hash, parser_version) ごとに 1 つの版 (amazon-settlement-versions.js の backfillDocumentVersions)。
 *   🚨 版の無い行がある間、build (日次の財務・月の手数料・月の mart) と Company DB の送り手は止まる (黙って行を落とさない)。
 *   coordinator (amazon-finance-coverage-run.js) の回の始めでも自動で流れる = ふつうは手で流さなくてよい (夜の daily-sync の最初の回が付ける)。
 *   手で先に流すときはこれ (本番の 440 万行で数分・WAL が大きくなる = daily-sync と重ならない時間に)。
 *   明細の UPDATE は id の範囲 20 万行ごとの取引 = 1 取引ずつ書き込みの lock を持つ (ログに ms)。最後に「build と送り手が読める状態か」の問題を出す
 *
 * 使い方:
 *   node apps/warehouse/migrate-settlement-document-versions.js            → 数えるだけ (dry-run・書かない)
 *   node apps/warehouse/migrate-settlement-document-versions.js --commit   → coverage の lease を取って版を付ける (coordinator が動いていれば止まる)
 * env: DATA_DIR (必須)
 */
import 'dotenv/config';
import { initDB, getDB } from './db.js';
import { backfillDocumentVersions, refreshStaleVersionDetails, acquireCoverageLease, releaseCoverageLease, assertLease, documentVersionsReady, documentVersionProblems } from './amazon-settlement-versions.js';
import { isAliveNodeSince } from './retry-lock.js';
import { pathToFileURL } from 'node:url';

export async function runMigrate({ commit = false, log = console.log, isAlive = isAliveNodeSince } = {}) {
  if (!process.env.DATA_DIR) throw new Error('DATA_DIR が無い (cwd の data に作らない)');
  await initDB();
  const db = getDB();
  const nullLines = db.prepare(`SELECT COUNT(*) n FROM raw_amazon_settlement_lines INDEXED BY idx_settle_lines_docver WHERE document_version_seq IS NULL`).get().n;
  const nullHeaders = db.prepare(`SELECT COUNT(*) n FROM raw_amazon_settlement_headers INDEXED BY idx_settle_headers_docver WHERE document_version_seq IS NULL`).get().n;
  const stale = db.prepare(`SELECT COUNT(*) n FROM amazon_settlement_document_versions WHERE detail_stale = 1`).get().n;
  log(`[versions] 版の無い行: 明細 ${nullLines} / 見出し ${nullHeaders}・要約の古い版 ${stale}`);
  if (!commit) {
    const groups = db.prepare(`SELECT source_layer, source_document_id, COUNT(*) n FROM raw_amazon_settlement_lines INDEXED BY idx_settle_lines_docver WHERE document_version_seq IS NULL GROUP BY 1, 2 ORDER BY 1, 2`).all();
    for (const g of groups.slice(0, 50)) log(`  ${g.source_layer} ${g.source_document_id}: ${g.n} 行`);
    log(`[versions] dry-run (書かない)。付けるなら --commit`);
    return { ready: documentVersionsReady(db), nullLines, nullHeaders, stale, committed: false };
  }
  const got = acquireCoverageLease(db, { isAlive });
  if (!got.ok) throw new Error(`coverage の lease を別の回が持っている (pid ${got.held.pid}・開始 ${got.held.started_at}) = coordinator が動いている。終わってから流す`);
  const lease = got.lease;
  try {
    const check = (dbx) => assertLease(dbx, lease);
    const out = backfillDocumentVersions(db, { check, log });
    const refreshed = refreshStaleVersionDetails(db, { check, log });
    const problems = documentVersionProblems(db);
    log(`[versions] ${problems.length ? '⚠️' : '✅'} 版を付けた: 文書 ${out.groups}・版 ${out.versions}・明細 ${out.lines} 行・見出し ${out.headers} 行 / 要約を作り直した版 ${refreshed + (out.refreshed || 0)} / 1 取引の最長 ${out.maxTxMs || 0} ms`);
    for (const p of problems) log(`[versions] ❌ ${p.code}: ${p.detail}`);
    if (!problems.length) log('[versions] build と送り手が読める状態 (版の無い行・途中の版・壊れた決済なし)');
    return { ready: problems.length === 0, problems, ...out, refreshed, committed: true };
  } finally { releaseCoverageLease(db, lease); }
}

const isDirectRun = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isDirectRun) {
  const args = process.argv.slice(2);
  for (const a of args) if (a !== '--commit') { console.error(`知らない引数: ${a}`); process.exit(2); }
  runMigrate({ commit: args.includes('--commit') }).catch((e) => { console.error(`❌ 決済の文書の版: ${e.message}`); process.exitCode = 1; });
}
