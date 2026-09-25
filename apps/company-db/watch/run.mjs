/**
 * run.mjs — Company DB の見張り (毎朝 1 回。daily-sync の最後の 1 ステップ。設計 = AI_reference CompanyDB構想/09)
 *
 * 使い方 (miniPC):
 *   node apps/company-db/watch/run.mjs [--data-dir D] [--as-of YYYY-MM-DD] [--dry-run] [--json] [--sync-run-id ID]
 *   記録する回 (dry-run でない) は as-of = 今日 (JST) だけ・実行 ID (env DAILY_SYNC_RUN_ID = daily-sync が発行。手動なら --sync-run-id) が要る。過去の日を見るなら --dry-run
 *
 * env:
 *   COMPANY_DB_WATCH_URL         照会用 (ロール watcher = select だけ)。scripts/company-db/create-watch-roles.mjs が作る
 *   COMPANY_DB_WATCH_WRITER_URL  記録用 (ロール watch_writer = ops.watch_* だけ)
 *   どちらも無ければ「⏭️ 未設定」で exit 0 (Dark Launch。マージ → デプロイ → ロール作成 → .env の順で安全に始める)
 *   --dry-run のときだけ COMPANY_DB_URL でも動く (人が手で確かめる用。書かない)
 *
 * 終わり方: 業務の異常 (breach) を見つけて記録・通知まで済めば **exit 0** (⚠️)。異常のたびに再実行させない。
 *   exit 1 は見張り自身の失敗 (評価できない・DB に届かない・期限超過) だけ。最後の 1 行は複数行にしない。
 */
import 'dotenv/config';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { openPgClient, pgAdapter } from '../../../scripts/company-db/migrate.mjs';
import { jstDateStr } from '../../../lib/jst-date.js';
import { readEvidence, EVIDENCE_KEEP_DAYS } from '../push/evidence.mjs';
import * as config from '../../../config/watch-checks.mjs';
import { runWatch } from './engine.mjs';

export function parseArgs(argv) {
  const out = { dataDir: null, asOf: null, dryRun: false, json: false, syncRunId: null };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--data-dir') out.dataDir = argv[++i];
    else if (a === '--as-of') out.asOf = argv[++i];
    else if (a === '--dry-run') out.dryRun = true;
    else if (a === '--json') out.json = true;
    else if (a === '--sync-run-id') out.syncRunId = argv[++i];
    else if (a === '7') { /* daily-sync の runScript は引数が無いと '7' を足す */ }
    else throw new Error(`知らない引数: ${a}`);
  }
  if (out.asOf && !/^\d{4}-\d{2}-\d{2}$/.test(out.asOf)) throw new Error('--as-of は YYYY-MM-DD');
  return out;
}

/** 接続を開いて、ロール・DB・statement_timeout を確かめる (SET で変えられるので保険) */
async function openChecked(url, label, { readOnly }) {
  const client = await openPgClient(url);
  const who = (await client.query(`select current_user as u, current_database() as d, current_setting('statement_timeout') as st`)).rows[0];
  await client.query(`set statement_timeout = '10s'`);
  if (readOnly) await client.query(`set default_transaction_read_only = on`);
  return { client, db: pgAdapter(client), who, label };
}

const fold = (x) => (process.platform === 'win32' ? x.toLowerCase() : x);
const isMain = (() => { try { return !!process.argv[1] && fold(fs.realpathSync.native(process.argv[1])) === fold(fs.realpathSync.native(fileURLToPath(import.meta.url))); } catch { return false; } })();
if (isMain) {
  let code = 1, last = '';
  const conns = [];
  try {
    const a = parseArgs(process.argv.slice(2));
    const dataDir = (a.dataDir || process.env.DATA_DIR || '').trim();
    if (!dataDir) throw new Error('DATA_DIR が無い (--data-dir でも可)');
    const asOf = a.asOf || jstDateStr(new Date());
    // 記録する回は今日だけ (過去の日を評価して今の案件を回復させない。engine でも守る = ここは接続する前に分かりやすく止めるだけ)
    if (!a.dryRun && asOf !== jstDateStr(new Date())) throw new Error(`記録する回の as-of は今日 (${jstDateStr(new Date())}) だけ (${asOf} を見るなら --dry-run)`);
    const watchUrl = (process.env.COMPANY_DB_WATCH_URL || '').trim();
    const writerUrl = (process.env.COMPANY_DB_WATCH_WRITER_URL || '').trim();
    if (!watchUrl && !(a.dryRun && process.env.COMPANY_DB_URL)) {
      last = '⏭️ Company DB 見張り: 未設定 (COMPANY_DB_WATCH_URL / COMPANY_DB_WATCH_WRITER_URL を .env に。作り方 = db/company/README.md「AI が見張る」)';
      code = 0;
    } else {
      const reader = await openChecked(watchUrl || process.env.COMPANY_DB_URL, 'watcher', { readOnly: true });
      conns.push(reader);
      let writer = null;
      if (!a.dryRun) {
        if (!writerUrl) throw new Error('COMPANY_DB_WATCH_WRITER_URL が無い (dry-run でなければ記録用の接続が要る)');
        writer = await openChecked(writerUrl, 'watch_writer', { readOnly: false });
        conns.push(writer);
        if (writer.who.d !== reader.who.d) throw new Error(`照会用と記録用の接続が別の DB を指している (${reader.who.d} / ${writer.who.d})`);
        if (writer.who.u === reader.who.u) console.log(`[company-db watch] ⚠️ 照会用と記録用が同じロール (${reader.who.u}) = 分けるのが設計 (09 §6)`);
      }
      const evidence = readEvidence(dataDir, asOf);
      // 過去の日の証跡 (W10 の「信頼できる世代」= 自動 retry が送り直した世代も、見張りが記録していなくても拾う。Codex #1417 R3)
      const evidenceHistory = {};
      for (let i = 1; i < EVIDENCE_KEEP_DAYS; i++) { const d = new Date(Date.parse(`${asOf}T00:00:00Z`) - i * 86400000).toISOString().slice(0, 10); const e = readEvidence(dataDir, d); if (Object.keys(e).length) evidenceHistory[d] = e; }
      const syncRunId = (a.syncRunId || process.env.DAILY_SYNC_RUN_ID || '').trim() || null;
      const r = await runWatch({ db: reader.db, writer: writer ? writer.db : null, config, asOf, evidence, evidenceHistory, dataDir, now: new Date(), host: process.env.COMPUTERNAME || 'minipc', syncRunId, log: (m) => console.log(`[company-db watch] ${m}`) });
      if (a.json) console.log(JSON.stringify({ runId: r.runId, counts: r.counts, notes: r.notes, persisted: r.persisted }, null, 1));
      last = r.lastLine + (a.dryRun ? ' [dry-run = 記録していない]' : '');
      code = r.exitCode;
    }
  } catch (e) {
    last = `❌ Company DB 見張り: ${String(e && e.message).replace(/\s+/g, ' ').slice(0, 400)}`;
    code = 1;
  } finally {
    for (const c of conns) { try { await c.client.end(); } catch { /* */ } }
  }
  console.log(String(last).replace(/\s+/g, ' '));
  // fetch / pg の直後に process.exit() しない (Windows の Node は libuv の assertion で 127 になる。#1386)
  process.exitCode = code;
  setTimeout(() => process.exit(code), 10000).unref();
}
