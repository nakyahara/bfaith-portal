#!/usr/bin/env node
/**
 * backup-cli.mjs — Company DB のバックアップ・検証・復元 (どこからでも同じ手順で)
 *
 *   node scripts/company-db/backup-cli.mjs dump [--out <file.gz>]     ダンプを取る (既定: DATA_DIR/backup-company-db/company-db_<日時>.dump.gz)
 *   node scripts/company-db/backup-cli.mjs verify <file.gz>            ダンプが壊れていないか・何行入っているかを見る (DB に触らない)
 *   node scripts/company-db/backup-cli.mjs restore <file.gz> --yes     復元する (対象の表を入れ替える。--yes が無ければ何もしない)
 *
 * 接続先は COMPANY_DB_URL (--url で上書き)。🚨 復元は今の中身を消して入れ替える。復元先の URL を必ず確かめること
 * 終了コード: 0 = ok / 1 = 失敗 / 2 = 引数・環境不足
 */
import 'dotenv/config';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { openPgClient, pgAdapter } from './migrate.mjs';
import { dumpToFile, restoreFromFile, verifyDumpFile } from '../../apps/company-db/backup/dump.mjs';

export function defaultDumpPath(dataDir, now = new Date()) {
  const stamp = now.toISOString().replace(/[-:T]/g, '').slice(0, 15);   // YYYYMMDDhhmmss
  return path.join(dataDir, 'backup-company-db', `company-db_${stamp}.dump.gz`);
}
const mb = (n) => `${(n / 1024 / 1024).toFixed(2)}MB`;

const isMain = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) {
  const args = process.argv.slice(2);
  const cmd = args[0];
  const getArg = (f) => { const i = args.indexOf(f); return i >= 0 && i < args.length - 1 ? args[i + 1] : null; };
  const log = (m) => console.log(`[company-db backup] ${m}`);
  let code = 0;
  try {
    if (cmd === 'verify') {
      const file = args[1];
      if (!file) { console.error('ファイルを指定する'); process.exit(2); }
      const v = await verifyDumpFile(file);
      const nonEmpty = v.tables.filter((t) => t.rows > 0);
      console.log(JSON.stringify({ ok: v.ok, generated_at: v.generatedAt, migrations: v.migrations.length, tables: v.tables.length, tables_with_rows: nonEmpty.length, total_rows: v.totalRows, declared_total: v.declaredTotal, top: nonEmpty.sort((a, b) => b.rows - a.rows).slice(0, 10) }, null, 2));
      code = v.ok ? 0 : 1;
    } else if (cmd === 'dump' || cmd === 'restore') {
      const url = getArg('--url') || process.env.COMPANY_DB_URL;
      if (!url) { console.error('COMPANY_DB_URL (または --url) が要る'); process.exit(2); }
      const client = await openPgClient(url);
      const db = pgAdapter(client);
      try {
        if (cmd === 'dump') {
          const dataDir = getArg('--data-dir') || process.env.DATA_DIR || process.cwd();
          const out = getArg('--out') || defaultDumpPath(dataDir);
          fs.mkdirSync(path.dirname(out), { recursive: true });
          const t0 = Date.now();
          const r = await dumpToFile(db, out, { log });
          log(`ok ${out} (${r.totalRows} 行 / ${mb(r.bytes)} / ${Math.round((Date.now() - t0) / 1000)} 秒)`);
          console.log(JSON.stringify({ ok: true, file: out, total_rows: r.totalRows, bytes: r.bytes, sha256: r.sha256, tables: r.tables.filter((x) => x.rows > 0).length }, null, 2));
        } else {
          const file = args[1];
          if (!file) { console.error('ファイルを指定する'); process.exit(2); }
          if (!args.includes('--yes')) {
            const v = await verifyDumpFile(file);
            console.error(`🚨 復元は今の中身を消して入れ替える。実行するなら --yes を付ける\n   ファイル: ${file} (${v.totalRows} 行、${v.generatedAt})\n   復元先: ${String(url).replace(/:\/\/[^@]*@/, '://***@')}`);
            process.exit(2);
          }
          const t0 = Date.now();
          const r = await restoreFromFile(db, file, { log });
          log(`ok 復元 ${r.totalRows} 行 (${Math.round((Date.now() - t0) / 1000)} 秒、ダンプ ${r.generatedAt})`);
          console.log(JSON.stringify({ ok: true, total_rows: r.totalRows, generated_at: r.generatedAt }, null, 2));
        }
      } finally { await client.end(); }
    } else {
      console.error('使い方: dump [--out <file>] | verify <file> | restore <file> --yes');
      code = 2;
    }
  } catch (e) {
    console.error(`[company-db backup] FAILED: ${e.message}${e.code ? ` (${e.code})` : ''}`);
    code = 1;
  }
  process.exit(code);
}
