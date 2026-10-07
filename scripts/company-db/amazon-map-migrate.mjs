#!/usr/bin/env node
/**
 * amazon-map-migrate.mjs — miniPC の SKU マスタ → Company DB の Amazon SKU の対応 (影運転 / 切替の日の移行。Company DB構想 16 §4・§5 の 4・PR ⑦-1・lib/amazon-map-migrate.mjs)
 *
 * 使い方 (miniPC の PowerShell・リポジトリ直下):
 *   # 影運転 (切替の前に毎日・T-7 から)。🚨 試し用の DB だけ。1 つの取引で移して照らし、必ず巻き戻す
 *   node -r dotenv/config scripts/company-db/amazon-map-migrate.mjs --shadow --db-url <試し用の DB の URL> --legacy <warehouse.db> --fba-db <fba.db> [--json out.json]
 *   # 古い表のハッシュ (H0) だけ出す (DB に触らない)
 *   node -r dotenv/config scripts/company-db/amazon-map-migrate.mjs --legacy-hash --legacy <warehouse.db>
 *   # 切替の日 ③ (段階 frozen の間 か、🆕 段階 new_open で listing_components.amazon を足す広げる道の試みが開いていて手の入口
 *   #   gas:logizard-sheet-and-sku-map を止めた記録がある間だけ (0059・PR-B)・手順書の順番でだけ)。H0 と照らして同じときだけ commit
 *   node -r dotenv/config scripts/company-db/amazon-map-migrate.mjs --apply --expect-hash <H0> --legacy <warehouse.db> --fba-db <fba.db> --actor <人のメール> --yes
 * 影運転の先が本番でないことの確かめ (Codex #1586 R1 M2): 本番の URL (env COMPANY_DB_URL) が要る (無ければ断る)。
 *   ① URL のホスト・ポート・DB 名が本番と同じ = 断る (ユーザー・パスワードは見ない = 別のユーザーでも同じ DB は断る)
 *   ② 両方につないで、DB 名が同じ かつ DB の識別 (pg_control_system() の system_identifier) が同じか読めない = 断る (識別が読めない所では、試し用の DB は本番と違う DB 名にする)
 *   本番に届かない = 断る (確かめられない)。本番には読むだけの 1 文 (current_database など) しか流さない
 * --fba-db は影運転と apply の両方で要る (Sheet にだけある SKU = 切替を止める項目を数える。Codex #1586 R1 M3)。無い・読めない = すぐ断る
 * 出すもの: 古い表のハッシュ (H0 の候補)・移せた SKU の古い表 / Company DB のハッシュと一致・切替を止める項目 (blockers・目標は全部 0)・気をつけること・書いた行の数
 * 🚨 古い表 (warehouse.db・fba.db) は読むだけで開く。Render と miniPC の SQLite には書かない (16 §7 M8)
 * 終了コード: 0 = 成功 (影運転は止める項目が 0 件かつハッシュが一致) / 1 = 失敗・止める項目あり・不一致 / 2 = 引数不正・本番を指している
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { openPgClient, pgAdapter } from './migrate.mjs';
import { readLegacyAmazonMaps, readSheetOnlySkus, legacyDigest, runAmazonMapMigration } from '../../lib/amazon-map-migrate.mjs';

const fail = (message, code = 'AMAZON_MAP_MIGRATE_ARGS') => Object.assign(new Error(message), { code });

/** URL のホスト・ポート・DB 名が同じか (ユーザー・パスワードは見ない)。読めない URL は同じとみなす (断る側) */
export function sameDatabaseUrl(a, b) {
  if (!a || !b) return false;
  try {
    const x = new URL(a); const y = new URL(b);
    return x.hostname.toLowerCase() === y.hostname.toLowerCase() && (x.port || '5432') === (y.port || '5432')
      && decodeURIComponent(x.pathname).replace(/^\/+/, '') === decodeURIComponent(y.pathname).replace(/^\/+/, '');
  } catch { return true; }
}

/** つないだ DB の識別: DB 名・サーバーのアドレスとポート・system_identifier (pg_control_system() が読めなければ null) */
export async function databaseIdentity(client) {
  const r = (await client.query('select current_database() as db, inet_server_addr()::text as addr, inet_server_port() as port')).rows[0];
  let sys = null;
  try { sys = String((await client.query('select system_identifier::text as s from pg_control_system()')).rows[0].s); } catch { sys = null; }
  return { db: r.db, addr: r.addr ?? null, port: r.port == null ? null : Number(r.port), system_identifier: sys };
}

/**
 * 同じ DB か: DB 名が同じ かつ (system_identifier が両方読めればそれが同じ / 片方でも読めなければ同じとみなす = 断る側)。
 * アドレス・ポートでは決めない (同じサーバーでも localhost / 127.0.0.1 / ::1 で違って見える)。識別が読めない所では、試し用の DB は本番と違う DB 名にする
 */
export function sameIdentity(a, b) {
  if (a.db !== b.db) return false;
  if (a.system_identifier && b.system_identifier) return a.system_identifier === b.system_identifier;
  return true;
}

/**
 * 影運転の先が本番でないことを確かめる (本番を指していれば投げる)。openClient(url) = 接続を開く (試験は差し替える)
 * @returns {Promise<{ target: object, production: object }>} 両方の識別
 */
export async function assertShadowTarget({ targetUrl, productionUrl, openClient = (u) => openPgClient(u, { application_name: 'amazon-map-shadow-check' }) }) {
  if (!targetUrl) throw fail('影運転は --db-url <試し用の DB> が要る (本番の Company DB には流さない)');
  if (!productionUrl) throw fail('本番の URL (COMPANY_DB_URL) が無いので、影運転の先が本番でないことを確かめられない (.env を読んで流す)');
  if (sameDatabaseUrl(targetUrl, productionUrl)) throw fail('--db-url が本番の Company DB (COMPANY_DB_URL) と同じホスト・ポート・DB 名。影運転は試し用の DB だけ', 'AMAZON_MAP_MIGRATE_PRODUCTION');
  const clients = [];
  try {
    let target; let production;
    try { const t = await openClient(targetUrl); clients.push(t); target = await databaseIdentity(t); } catch (e) { throw fail(`試し用の DB につながらない: ${e.message}`); }
    try { const p = await openClient(productionUrl); clients.push(p); production = await databaseIdentity(p); } catch (e) { throw fail(`本番の DB の識別を読めない = 影運転の先が本番でないことを確かめられない: ${e.message}`); }
    if (sameIdentity(target, production)) {
      throw fail(`--db-url が本番と同じ DB (DB 名 ${target.db}・${target.system_identifier ? `system_identifier ${target.system_identifier}` : `サーバー ${target.addr}:${target.port}`})。影運転は試し用の DB だけ`, 'AMAZON_MAP_MIGRATE_PRODUCTION');
    }
    return { target, production };
  } finally {
    for (const c of clients) { try { await c.end(); } catch { /* */ } }
  }
}

/** --fba-db (要る・読めること)。Sheet にだけある SKU の一覧 */
export function sheetOnlyFrom(fbaFile, legacy) {
  if (!fbaFile) throw fail('--fba-db <fba.db> が要る (Sheet にだけある SKU = 切替を止める項目を数える)');
  if (!fs.existsSync(fbaFile)) throw fail(`--fba-db のファイルが無い: ${fbaFile}`);
  try { return readSheetOnlySkus(fbaFile, legacy); } catch (e) { throw fail(`--fba-db が読めない (${fbaFile}): ${e.message}`); }
}

const isMain = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) {
  const args = process.argv.slice(2);
  const getArg = (f) => { const i = args.indexOf(f); return i >= 0 && i < args.length - 1 ? args[i + 1] : null; };
  try {
    const legacyFile = getArg('--legacy');
    if (!legacyFile || !fs.existsSync(legacyFile)) throw fail('--legacy <miniPC の warehouse.db> が要る');
    const legacy = readLegacyAmazonMaps(legacyFile);
    if (args.includes('--legacy-hash')) {
      const d = legacyDigest(legacy);
      console.log(d.content_hash ? `古い表のハッシュ (H0 の候補): ${d.content_hash} (親 ${d.master_rows}・構成 ${d.component_rows})` : `古い表の形が写しの決まりに合わない: ${d.error}`);
      process.exit(d.content_hash ? 0 : 1);
    }
    const mode = args.includes('--apply') ? 'apply' : args.includes('--shadow') ? 'shadow' : null;
    if (!mode) throw fail('--shadow か --apply か --legacy-hash を付ける');
    const sheetOnly = sheetOnlyFrom(getArg('--fba-db'), legacy);
    let url;
    if (mode === 'shadow') {
      url = getArg('--db-url');
      await assertShadowTarget({ targetUrl: url, productionUrl: process.env.COMPANY_DB_URL });
    } else {
      url = process.env.COMPANY_DB_URL;
      if (!url) throw fail('COMPANY_DB_URL が要る');
      if (!getArg('--actor') || !getArg('--expect-hash')) throw fail('apply は --actor と --expect-hash <H0> が要る');
      if (!args.includes('--yes')) { console.log('--yes が無いので移さない (切替の日の手順書の順番でだけ使う)'); process.exit(0); }
    }
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
    } finally { await client.end(); }
  } catch (e) {
    console.error(`失敗: ${e.message}`);
    process.exitCode = e && (e.code === 'AMAZON_MAP_MIGRATE_ARGS' || e.code === 'AMAZON_MAP_MIGRATE_PRODUCTION') ? 2 : 1;
  }
}
