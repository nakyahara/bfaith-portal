#!/usr/bin/env node
/**
 * amazon-map-migrate.mjs — miniPC の SKU マスタ → Company DB の Amazon SKU の対応 (影運転 / 切替の日の移行。Company DB構想 16 §4・§5 の 4・PR ⑦-1・lib/amazon-map-migrate.mjs)
 *
 * 使い方 (miniPC の PowerShell・リポジトリ直下):
 *   # 影運転 (切替の前に毎日・T-7 から)。🚨 試し用の DB だけ (本番の Company DB = env COMPANY_DB_URL と同じ URL は断る)。1 つの取引で移して照らし、必ず巻き戻す
 *   node -r dotenv/config scripts/company-db/amazon-map-migrate.mjs --shadow --db-url <試し用の DB の URL> --legacy <warehouse.db> [--fba-db <fba.db>] [--json out.json]
 *   # 古い表のハッシュ (H0) だけ出す (DB に触らない)
 *   node -r dotenv/config scripts/company-db/amazon-map-migrate.mjs --legacy-hash --legacy <warehouse.db>
 *   # 切替の日 ③ (段階 frozen の間だけ・手順書の順番でだけ)。H0 と照らして同じときだけ commit
 *   node -r dotenv/config scripts/company-db/amazon-map-migrate.mjs --apply --expect-hash <H0> --legacy <warehouse.db> --actor <人のメール> --yes
 * 出すもの: 古い表のハッシュ (H0 の候補)・移せた SKU の古い表 / Company DB のハッシュと一致・切替を止める項目 (blockers・目標は全部 0)・気をつけること・書いた行の数
 * 🚨 古い表 (warehouse.db・fba.db) は読むだけで開く。Render と miniPC の SQLite には書かない (16 §7 M8)
 * 終了コード: 0 = 成功 (影運転は止める項目が 0 件かつハッシュが一致) / 1 = 失敗・止める項目あり・不一致 / 2 = 引数不正
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { openPgClient, pgAdapter } from './migrate.mjs';
import { readLegacyAmazonMaps, readSheetOnlySkus, legacyDigest, runAmazonMapMigration } from '../../lib/amazon-map-migrate.mjs';

/** 影運転の接続先が本番 (env COMPANY_DB_URL) と同じか (ユーザー・ホスト・ポート・DB 名で比べる。パスワードは見ない) */
export function sameDatabaseUrl(a, b) {
  if (!a || !b) return false;
  try {
    const x = new URL(a); const y = new URL(b);
    return x.hostname.toLowerCase() === y.hostname.toLowerCase() && (x.port || '5432') === (y.port || '5432')
      && decodeURIComponent(x.pathname) === decodeURIComponent(y.pathname) && decodeURIComponent(x.username) === decodeURIComponent(y.username);
  } catch { return a === b; }
}

const isMain = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) {
  const args = process.argv.slice(2);
  const getArg = (f) => { const i = args.indexOf(f); return i >= 0 && i < args.length - 1 ? args[i + 1] : null; };
  const legacyFile = getArg('--legacy');
  if (!legacyFile || !fs.existsSync(legacyFile)) { console.error('--legacy <miniPC の warehouse.db> が要る'); process.exit(2); }
  const legacy = readLegacyAmazonMaps(legacyFile);
  if (args.includes('--legacy-hash')) {
    const d = legacyDigest(legacy);
    console.log(d.content_hash ? `古い表のハッシュ (H0 の候補): ${d.content_hash} (親 ${d.master_rows}・構成 ${d.component_rows})` : `古い表の形が写しの決まりに合わない: ${d.error}`);
    process.exit(d.content_hash ? 0 : 1);
  }
  const mode = args.includes('--apply') ? 'apply' : args.includes('--shadow') ? 'shadow' : null;
  if (!mode) { console.error('--shadow か --apply か --legacy-hash を付ける'); process.exit(2); }
  let url;
  if (mode === 'shadow') {
    url = getArg('--db-url');
    if (!url) { console.error('影運転は --db-url <試し用の DB> が要る (本番の Company DB には流さない)'); process.exit(2); }
    if (sameDatabaseUrl(url, process.env.COMPANY_DB_URL)) { console.error('--db-url が本番の Company DB (COMPANY_DB_URL) と同じ。影運転は試し用の DB だけ'); process.exit(2); }
  } else {
    url = process.env.COMPANY_DB_URL;
    if (!url) { console.error('COMPANY_DB_URL が要る'); process.exit(2); }
    if (!getArg('--actor') || !getArg('--expect-hash')) { console.error('apply は --actor と --expect-hash <H0> が要る'); process.exit(2); }
    if (!args.includes('--yes')) { console.log('--yes が無いので移さない (切替の日の手順書の順番でだけ使う)'); process.exit(0); }
  }
  const fbaFile = getArg('--fba-db');
  const sheetOnly = fbaFile ? readSheetOnlySkus(fbaFile, legacy) : [];
  const client = await openPgClient(url, { application_name: `amazon-map-${mode}` });
  try {
    const r = await runAmazonMapMigration(pgAdapter(client), legacy, { mode, actor: getArg('--actor') || 'amazon_map_shadow', expectHash: getArg('--expect-hash'), sheetOnly, log: (m) => console.log(m) });
    const out = getArg('--json');
    if (out) fs.writeFileSync(out, JSON.stringify(r, null, 2));
    console.log(`古い表のハッシュ: ${r.legacy_digest.content_hash || r.legacy_digest.error} (親 ${r.legacy_digest.master_rows}・構成 ${r.legacy_digest.component_rows})`);
    console.log(`移せた SKU ${r.subset.skus}: 古い表 ${r.subset.legacy.content_hash || r.subset.legacy.error} / Company DB ${r.subset.company.content_hash || r.subset.company.error} → ${r.subset.match ? '一致' : '不一致'}`);
    console.log(`切替を止める項目: ${r.blocker_total} 件 (止まる SKU ${r.blocked_skus})${Object.entries(r.blockers).map(([k, v]) => `\n  ${k}: ${v.count} 件 例 ${JSON.stringify(v.samples.slice(0, 3))}`).join('')}`);
    for (const [k, v] of Object.entries(r.warnings)) console.log(`気をつける: ${k} ${v.count} 件 例 ${JSON.stringify(v.samples.slice(0, 3))}`);
    console.log(r.committed ? '✅ 移した (commit)' : '巻き戻した (影運転)');
    if (!(r.subset.match && r.blocker_total === 0)) process.exitCode = 1;
  } catch (e) {
    console.error(`失敗: ${e.message}`);
    process.exitCode = 1;
  } finally { await client.end(); }
}
