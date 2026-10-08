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
 *   node -r dotenv/config scripts/company-db/amazon-map-migrate.mjs --apply --expect-hash <H0> --legacy <warehouse.db> --fba-db <fba.db> --actor <人のメール> --yes [--attempt <widen_prepare_id>]
 *   🆕 --attempt は段階 new_open の apply で必須 (#1648 Codex R1 Medium 1・指した試みの窓でだけ通る = lib の migrationWidenWindow が DB で照らす)。frozen の apply は今までどおり要らない
 *   # 🆕 移行の後に試みを cancel して古い表が変わったときのやり直し (#1648 Codex R1 Medium 2・中原さんの決定 b)。段階 new_open の Amazon を足す試みの窓だけ (--attempt 必須)
 *   #   今の Company DB の写しのハッシュを出す (読むだけ) → 古い表のハッシュと一緒に渡す。origin = legacy の対応を古い表に合わせ直す (消えた対応は墓標)・合わせた後 = 古い表のハッシュのときだけ commit
 *   node -r dotenv/config scripts/company-db/amazon-map-migrate.mjs --cdb-hash
 *   node -r dotenv/config scripts/company-db/amazon-map-migrate.mjs --reconcile --attempt <widen_prepare_id> --expect-hash <古い表のハッシュ> --expect-cdb-hash <今の Company DB のハッシュ> --legacy <warehouse.db> --fba-db <fba.db> --actor <人> --yes
 * 影運転の先が本番でないことの確かめ (Codex #1586 R1 M2): 本番の URL (env COMPANY_DB_URL) が要る (無ければ断る)。
 *   ① URL のホスト・ポート・DB 名が本番と同じ = 断る (ユーザー・パスワードは見ない = 別のユーザーでも同じ DB は断る)
 *   ② 両方につないで、DB 名が同じ かつ DB の識別 (pg_control_system() の system_identifier) が同じか読めない = 断る (識別が読めない所では、試し用の DB は本番と違う DB 名にする)
 *   本番に届かない = 断る (確かめられない)。本番には読むだけの 1 文 (current_database など) しか流さない
 * --fba-db は影運転と apply (と reconcile) で要る (Sheet にだけある SKU を数える。Codex #1586 R1 M3)。無い・読めない = すぐ断る。
 *   PR-D (10/8 中原さん「スプレッドシートは使用していないので無視」): Sheet にだけある SKU (sheet_only) は止める項目でなく「気をつける」(数・例は出す・終了コードに効かない)。
 *   ただしその出品 (対応なし) に Company DB の構成が 1 行でもあれば止める項目 sheet_only_has_components (出どころによらない・何も消さない = 人が見て決める・#1651)
 * 出すもの: 古い表のハッシュ (H0 の候補)・移せた SKU の古い表 / Company DB のハッシュと一致・切替を止める項目 (blockers・目標は全部 0)・気をつけること・書いた行の数
 * 🚨 古い表 (warehouse.db・fba.db) は読むだけで開く。Render と miniPC の SQLite には書かない (16 §7 M8)
 * 終了コード: 0 = 成功 (影運転は止める項目が 0 件かつハッシュが一致) / 1 = 失敗・止める項目あり・不一致 / 2 = 引数不正・本番を指している
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { openPgClient, pgAdapter } from './migrate.mjs';
import { readLegacyAmazonMaps, readSheetOnlySkus, legacyDigest, runAmazonMapMigration } from '../../lib/amazon-map-migrate.mjs';
import { readCompanyAmazonMapCanon } from '../../lib/amazon-map-write.mjs';
import { skuMapDigest } from '../../lib/sku-map-canonical.js';

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

/** --fba-db (要る・読めること)。Sheet にだけある SKU の一覧 (PR-D から気をつける項目 = 止めない) */
export function sheetOnlyFrom(fbaFile, legacy) {
  if (!fbaFile) throw fail('--fba-db <fba.db> が要る (Sheet にだけある SKU を数える = 気をつける項目)');
  if (!fs.existsSync(fbaFile)) throw fail(`--fba-db のファイルが無い: ${fbaFile}`);
  try { return readSheetOnlySkus(fbaFile, legacy); } catch (e) { throw fail(`--fba-db が読めない (${fbaFile}): ${e.message}`); }
}

const isMain = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) {
  const args = process.argv.slice(2);
  const getArg = (f) => { const i = args.indexOf(f); return i >= 0 && i < args.length - 1 ? args[i + 1] : null; };
  try {
    if (args.includes('--cdb-hash')) {
      // 今の Company DB の写し (active の対応) のハッシュ = reconcile の --expect-cdb-hash (読むだけ・古い表は要らない)
      const u = process.env.COMPANY_DB_URL || process.env.COMPANY_DB_WATCH_URL;
      if (!u) throw fail('COMPANY_DB_URL (または COMPANY_DB_WATCH_URL) が要る');
      const client = await openPgClient(u, { application_name: 'amazon-map-cdb-hash' });
      try {
        const d = skuMapDigest(await readCompanyAmazonMapCanon(pgAdapter(client)));
        console.log(`今の Company DB の写しのハッシュ: ${d.content_hash} (対応 ${d.master_rows}・構成 ${d.component_rows})`);
      } finally { await client.end(); }
      process.exit(0);
    }
    const legacyFile = getArg('--legacy');
    if (!legacyFile || !fs.existsSync(legacyFile)) throw fail('--legacy <miniPC の warehouse.db> が要る');
    const legacy = readLegacyAmazonMaps(legacyFile);
    if (args.includes('--legacy-hash')) {
      const d = legacyDigest(legacy);
      console.log(d.content_hash ? `古い表のハッシュ (H0 の候補): ${d.content_hash} (親 ${d.master_rows}・構成 ${d.component_rows})` : `古い表の形が写しの決まりに合わない: ${d.error}`);
      process.exit(d.content_hash ? 0 : 1);
    }
    const mode = args.includes('--reconcile') ? 'reconcile' : args.includes('--apply') ? 'apply' : args.includes('--shadow') ? 'shadow' : null;
    if (!mode) throw fail('--shadow か --apply か --reconcile か --legacy-hash か --cdb-hash を付ける');
    if (args.includes('--reconcile') && args.includes('--apply')) throw fail('--apply と --reconcile は一緒に付けない');
    const sheetOnly = sheetOnlyFrom(getArg('--fba-db'), legacy);
    let url;
    if (mode === 'shadow') {
      url = getArg('--db-url');
      await assertShadowTarget({ targetUrl: url, productionUrl: process.env.COMPANY_DB_URL });
    } else {
      url = process.env.COMPANY_DB_URL;
      if (!url) throw fail('COMPANY_DB_URL が要る');
      if (!getArg('--actor') || !getArg('--expect-hash')) throw fail(`${mode} は --actor と --expect-hash <古い表のハッシュ> が要る`);
      if (mode === 'reconcile' && (!getArg('--expect-cdb-hash') || !getArg('--attempt'))) throw fail('reconcile は --expect-cdb-hash <今の Company DB のハッシュ (--cdb-hash)> と --attempt <widen_prepare_id> が要る');
      if (!args.includes('--yes')) { console.log('--yes が無いので移さない (切替の日の手順書の順番でだけ使う)'); process.exit(0); }
    }
    const client = await openPgClient(url, { application_name: `amazon-map-${mode}` });
    try {
      const r = await runAmazonMapMigration(pgAdapter(client), legacy, { mode, actor: getArg('--actor') || 'amazon_map_shadow', expectHash: getArg('--expect-hash'), expectCdbHash: getArg('--expect-cdb-hash'), attemptId: getArg('--attempt'), sheetOnly, log: (m) => console.log(m) });
      const out = getArg('--json');
      if (out) fs.writeFileSync(out, JSON.stringify(r, null, 2));
      console.log(`古い表のハッシュ: ${r.legacy_digest.content_hash || r.legacy_digest.error} (親 ${r.legacy_digest.master_rows}・構成 ${r.legacy_digest.component_rows})`);
      console.log(`移せた SKU ${r.subset.skus}: 古い表 ${r.subset.legacy.content_hash || r.subset.legacy.error} / Company DB ${r.subset.company.content_hash || r.subset.company.error} → ${r.subset.match ? '一致' : '不一致'}`);
      console.log(`切替を止める項目: ${r.blocker_total} 件 (止まる SKU ${r.blocked_skus})${Object.entries(r.blockers).map(([k, v]) => `\n  ${k}: ${v.count} 件 例 ${JSON.stringify(v.samples.slice(0, 3))}`).join('')}`);
      for (const [k, v] of Object.entries(r.warnings)) console.log(`気をつける: ${k} ${v.count} 件 例 ${JSON.stringify(v.samples.slice(0, 3))}`);
      if (r.cdb_before) console.log(`合わせ直す前の Company DB のハッシュ: ${r.cdb_before.content_hash} (対応 ${r.cdb_before.master_rows}・構成 ${r.cdb_before.component_rows})`);
      console.log(r.committed ? (mode === 'reconcile' ? '✅ 合わせ直した (commit)' : '✅ 移した (commit)') : '巻き戻した (影運転)');
      if (!(r.subset.match && r.blocker_total === 0)) process.exitCode = 1;
    } finally { await client.end(); }
  } catch (e) {
    console.error(`失敗: ${e.message}`);
    process.exitCode = e && (e.code === 'AMAZON_MAP_MIGRATE_ARGS' || e.code === 'AMAZON_MAP_MIGRATE_PRODUCTION') ? 2 : 1;
  }
}
