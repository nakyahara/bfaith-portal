#!/usr/bin/env node
/**
 * master-gone-day-override.mjs — 「NE から消えた → やめる」(0070・0072) の「完全な NE の取得に 3 日続けて無い」を、中原さんが決めた SKU だけ
 *   期限つきで短くする許可を足す (0076・中原さんの決定 10/13 夜 B = 色違いの登録 (widen) を 1 日前倒し)
 *
 * 許可は Company DB の表 ops.sku_gone_from_ne_day_overrides に追記だけで残る (だれ・理由・日数・期限・request_id・DB のロール)。期限が過ぎると効かない (= 3 日に戻る)。
 * 許可で変わるのは「続けて無い日数の下限」だけ。ほかの決まり (完全で信用できる取得・一番新しい取得が新しい・その取得にも無い・NE のコードの台帳にも無い・
 *   単品かセット・生きた NE 登録の CSV なし・構成品 / 構成の依頼 / 出品の構成 / 親に使われていない) は全部そのまま = やめるのは今まで通り master-gone-from-ne.mjs で
 *
 * 使い方 (miniPC の PowerShell・リポジトリ直下。接続 = env COMPANY_DB_MASTER_OPS_URL (運用のロール master_ops) か --url):
 *   # 確かめ (読むだけ・既定) = 今の答え (NE に無い日数・止まる理由・今効いている許可) と、許可を足したら やめてよい になるか
 *   node -r dotenv/config scripts/company-db/master-gone-day-override.mjs --codes-file drift18.txt --days 1
 *   # 許可を足す (🚨 確かめの答えを中原さんが見て OK の後だけ・全部か何も無いか)
 *   node -r dotenv/config scripts/company-db/master-gone-day-override.mjs --apply --codes-file drift18.txt --days 1 --valid-days 7 --actor <人のメール> --reason "<理由>" --yes
 * 終了コード: 0 = 確かめを出した / 許可を足した・1 = 断られた・つながらない・2 = 引数が違う
 */
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { openPgClient, pgAdapter } from './migrate.mjs';
import { listGoneChecks, goneReasonOf, GONE_REASON_WORDS } from '../../lib/master-gone.mjs';
import { readCodesFile } from './master-gone-from-ne.mjs';

const fail = (message, code = 'GONE_DAY_ARGS') => Object.assign(new Error(message), { code });
export const ADD_FN = 'ops.add_sku_gone_day_overrides(uuid, text, text, text[], integer, integer)';

/** 引数を読む。誤り = GONE_DAY_ARGS を投げる */
export function parseDayArgs(argv) {
  const o = { apply: false, yes: false, codes: null, codesFile: null, days: 1, validDays: 7, actor: null, reason: null, json: null, url: null, requestId: null };
  const need = (i, name) => { const v = argv[i + 1]; if (v == null || v.startsWith('--')) throw fail(`${name} の後に値が要る`); return v; };
  const int = (v, name, lo, hi) => { if (!/^\d+$/.test(v) || Number(v) < lo || Number(v) > hi) throw fail(`${name} は ${lo}〜${hi} の整数`); return Number(v); };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--apply') o.apply = true;
    else if (a === '--yes') o.yes = true;
    else if (a === '--codes') { o.codes = need(i, a).split(',').map((s) => s.trim()).filter(Boolean); i++; }
    else if (a === '--codes-file') { o.codesFile = need(i, a); i++; }
    else if (a === '--days') { o.days = int(need(i, a), a, 1, 3); i++; }
    else if (a === '--valid-days') { o.validDays = int(need(i, a), a, 1, 14); i++; }
    else if (a === '--actor') { o.actor = need(i, a); i++; }
    else if (a === '--reason') { o.reason = need(i, a); i++; }
    else if (a === '--request-id') { const v = need(i, a); if (!/^[0-9a-f-]{36}$/.test(v)) throw fail('--request-id は UUID'); o.requestId = v; i++; }
    else if (a === '--json') { o.json = need(i, a); i++; }
    else if (a === '--url') { o.url = need(i, a); i++; }
    else throw fail(`知らない引数: ${a}`);
  }
  if (o.codes && o.codesFile) throw fail('--codes と --codes-file はどちらか 1 つ');
  if (!o.codes && !o.codesFile) throw fail('コード (--codes か --codes-file) が要る (候補の全部に黙って許可を足さない)');
  if (o.apply) {
    if (!o.actor || !/^[^\s@]+@[^\s@]+$/.test(o.actor)) throw fail('--apply は --actor <人のメール> が要る');
    try { o.reason = goneReasonOf(o.reason); } catch (e) { throw fail(`--reason: ${e.message}`); }
    if (!o.yes) throw fail('--apply は --yes が要る (確かめの答えを見て OK をもらった後だけ)');
  }
  return o;
}

/** 許可 (days 日) を足したら やめてよい になるか = 止まる理由が「日数が足りない」だけで、続けて無い日数 >= days */
export const okWithDays = (r, days) => {
  const ps = (r.problems || []).map((p) => p.reason);
  return r.ok === true || (ps.length > 0 && ps.every((x) => x === 'absent_short') && Number(r.streak || 0) >= days);
};

/** 本体 (試験は db を渡す)。戻り値 = { mode, checks, added, exitCode, error? } */
export async function runDayOverride(db, opts, { log = console.log } = {}) {
  const codes = opts.codesFile ? readCodesFile(opts.codesFile) : opts.codes;
  if (!codes || !codes.length) throw fail('コードが 1 つも無い');
  const checks = await listGoneChecks(db, codes);
  const willOk = checks.filter((r) => okWithDays(r, opts.days)).length;
  log(`確かめ (読むだけ・今の ops.sku_gone_from_ne_check): ${checks.length} 件・許可 ${opts.days} 日なら やめてよい ${willOk} 件`);
  for (const r of checks) {
    const ov = r.day_override ? ` [今の許可 ${r.day_override.required_days} 日・${r.day_override.actor_id}・期限 ${r.day_override.expires_at}]` : '';
    log(`${okWithDays(r, opts.days) ? '✅' : '⛔'} ${r.code ?? '(?)'}  NE に無い ${r.streak ?? 0}/${r.required_runs ?? '?'} 日${ov}`);
    for (const p of r.problems || []) log(`      - ${GONE_REASON_WORDS[p.reason] || p.reason}: ${p.text}`);
  }
  if (!opts.apply) return { mode: 'dry-run', checks, added: null, exitCode: 0 };
  const requestId = opts.requestId || crypto.randomUUID();
  try {
    const r = (await db.query(`select ops.add_sku_gone_day_overrides($1::uuid, $2, $3, $4::text[], $5::integer, $6::integer) as r`,
      [requestId, opts.actor.trim().toLowerCase(), opts.reason, codes, opts.days, opts.validDays])).rows[0].r;
    const x = typeof r === 'string' ? JSON.parse(r) : r;
    log(`${x.replayed ? 'もう足してあった' : '許可を足した'} ${x.overrides.length} 件 (日数 ${x.required_days}・期限 ${x.expires_at}・request_id ${requestId})`);
    return { mode: 'apply', checks, added: x, exitCode: 0 };
  } catch (e) {
    log(`⛔ 許可を足せなかった (1 件も足していない): ${e.message}`);
    return { mode: 'apply', checks, added: null, exitCode: 1, error: { code: e.code, message: String(e.message || e) } };
  }
}

async function main() {
  let opts;
  try { opts = parseDayArgs(process.argv.slice(2)); } catch (e) { console.error(`❌ ${e.message}`); process.exitCode = 2; return; }
  const url = opts.url || process.env.COMPANY_DB_MASTER_OPS_URL;
  if (!url) { console.error('❌ COMPANY_DB_MASTER_OPS_URL (運用のロール master_ops) か --url が要る'); process.exitCode = 2; return; }
  const client = await openPgClient(url, { application_name: 'master-gone-day-override' });
  try {
    await client.query(`set statement_timeout = '20s'`);
    await client.query(`set lock_timeout = '10s'`);
    const r = await runDayOverride(pgAdapter(client), opts);
    if (opts.json) fs.writeFileSync(opts.json, JSON.stringify({ mode: r.mode, checks: r.checks, added: r.added, error: r.error ?? null }, null, 2));
    process.exitCode = r.exitCode;
  } catch (e) {
    console.error(`❌ ${e.message}`);
    process.exitCode = e.code === 'GONE_DAY_ARGS' ? 2 : 1;
  } finally { try { await client.end(); } catch { /* */ } }
}

const isMain = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) main();
