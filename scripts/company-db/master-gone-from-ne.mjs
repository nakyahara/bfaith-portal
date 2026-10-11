#!/usr/bin/env node
/**
 * master-gone-from-ne.mjs — 業務で使える商品 (登録の状態 available) を「NE から消えた」でまとめてやめる (cancelled)。0070・中原さんの決定 10/11 Q1 = a
 *
 * やめてよいかは Company DB が自分で確かめる (ops.sku_gone_from_ne_check = 完全で信用できる NE の取得 (照合 ② の記録の回) に 3 回続けて無い・
 * 一番新しい記録が新しい・今の NE のコードの台帳にも無い・単品・生きた NE 登録の CSV が無い・ほかのセット / 構成の依頼 / 出品の構成に使われていない・ほかの商品の親でない)。
 * このスクリプトは確かめの答えを出す (dry-run・既定) と、人の OK の後に 1 件ずつ別の取引で ops.cancel_sku_gone_from_ne を呼ぶ (--apply) だけ。
 *   1 件が断られても残りは続ける (断られた件は理由を出す・終了コード 1)。記録 = 1 件ごとの保存の記録 (ops.master_edit_requests・request_id) と登録の状態の履歴
 *
 * 使い方 (miniPC の PowerShell・リポジトリ直下。接続 = env COMPANY_DB_MASTER_OPS_URL (運用のロール master_ops) か --url):
 *   # 候補 (読むだけ) = 業務で使える商品で、一番新しい完全な NE の取得に無い SKU と、やめてよいかの答え
 *   node -r dotenv/config scripts/company-db/master-gone-from-ne.mjs
 *   # コードを渡して確かめる (読むだけ)。--codes はカンマ区切り / --codes-file は 1 行 1 コード (# から後は読まない)
 *   node -r dotenv/config scripts/company-db/master-gone-from-ne.mjs --codes-file gone.txt [--json out.json]
 *   # やめる (🚨 dry-run の答えを中原さんが見て OK の後だけ)。--expect = dry-run で「やめてよい」の数 (違えば 1 件も動かさない)
 *   node -r dotenv/config scripts/company-db/master-gone-from-ne.mjs --apply --codes-file gone.txt --expect 18 --actor <人のメール> --reason "<理由>" --yes [--json out.json]
 * 終了コード: 0 = dry-run を出した / apply で全部やめた・1 = apply で断られた件がある・数が --expect と違う・つながらない・2 = 引数が違う
 */
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { openPgClient, pgAdapter } from './migrate.mjs';
import { listGoneChecks, cancelSkuGoneFromNe, goneReasonOf, GONE_REASON_WORDS } from '../../lib/master-gone.mjs';

const fail = (message, code = 'GONE_ARGS') => Object.assign(new Error(message), { code });

/** 引数を読む。誤り = GONE_ARGS を投げる */
export function parseGoneArgs(argv) {
  const o = { apply: false, yes: false, codes: null, codesFile: null, expect: null, actor: null, reason: null, json: null, url: null };
  const need = (i, name) => { const v = argv[i + 1]; if (v == null || v.startsWith('--')) throw fail(`${name} の後に値が要る`); return v; };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--apply') o.apply = true;
    else if (a === '--yes') o.yes = true;
    else if (a === '--codes') { o.codes = need(i, a).split(',').map((s) => s.trim()).filter(Boolean); i++; }
    else if (a === '--codes-file') { o.codesFile = need(i, a); i++; }
    else if (a === '--expect') { const v = need(i, a); if (!/^\d{1,4}$/.test(v)) throw fail('--expect は 0 以上の整数'); o.expect = Number(v); i++; }
    else if (a === '--actor') { o.actor = need(i, a); i++; }
    else if (a === '--reason') { o.reason = need(i, a); i++; }
    else if (a === '--json') { o.json = need(i, a); i++; }
    else if (a === '--url') { o.url = need(i, a); i++; }
    else throw fail(`知らない引数: ${a}`);
  }
  if (o.codes && o.codesFile) throw fail('--codes と --codes-file はどちらか 1 つ');
  if (o.apply) {
    if (!o.codes && !o.codesFile) throw fail('--apply はコード (--codes か --codes-file) が要る (候補の全部を黙ってやめない)');
    if (o.expect == null) throw fail('--apply は --expect <dry-run で「やめてよい」の数> が要る');
    if (!o.actor || !/^[^\s@]+@[^\s@]+$/.test(o.actor)) throw fail('--apply は --actor <人のメール> が要る');
    try { o.reason = goneReasonOf(o.reason); } catch (e) { throw fail(`--reason: ${e.message}`); }
    if (!o.yes) throw fail('--apply は --yes が要る (dry-run の答えを見て OK をもらった後だけ)');
  }
  return o;
}

/** 1 行 1 コード (空行・# から後は読まない) */
export function readCodesFile(file) {
  const text = fs.readFileSync(file, 'utf8').replace(/^﻿/, '');
  return text.split(/\r?\n/).map((l) => l.replace(/#.*$/, '').trim()).filter(Boolean);
}

/** 確かめの答えを 1 行ずつの文に */
export function checkLines(rows) {
  const out = [];
  for (const r of rows) {
    const head = `${r.ok ? '✅ やめてよい' : '⛔ やめない'}  ${r.code ?? '(?)'}  NE に無い ${r.streak ?? 0}/${r.required_runs ?? '?'} 回`;
    out.push(head);
    for (const p of r.problems || []) out.push(`      - ${GONE_REASON_WORDS[p.reason] || p.reason}: ${p.text}`);
  }
  return out;
}

/**
 * 本体 (試験は db を渡す)。戻り値 = { mode, checks, applied: [{ code, ok, request_id, event_id?, replayed?, error?, reason? }], exitCode }
 */
export async function runGone(db, opts, { log = console.log } = {}) {
  const codes = opts.codesFile ? readCodesFile(opts.codesFile) : opts.codes;
  if (codes && !codes.length) throw fail('コードが 1 つも無い');
  const checks = await listGoneChecks(db, codes ?? null);
  const okRows = checks.filter((r) => r.ok);
  log(`確かめ (読むだけ・Company DB の ops.sku_gone_from_ne_check): ${checks.length} 件・やめてよい ${okRows.length} 件・やめない ${checks.length - okRows.length} 件`);
  for (const l of checkLines(checks)) log(l);
  if (!opts.apply) return { mode: 'dry-run', checks, applied: [], exitCode: 0 };
  if (okRows.length !== opts.expect) {
    log(`⛔ やめてよいのは ${okRows.length} 件で、--expect ${opts.expect} と違う = 1 件も動かさない (dry-run をもう一度見てから)`);
    return { mode: 'apply', checks, applied: [], exitCode: 1 };
  }
  const applied = [];
  for (const r of okRows) {
    const requestId = crypto.randomUUID();
    try {
      const x = await cancelSkuGoneFromNe(db, { actor: opts.actor, skuId: r.sku_id, reason: opts.reason, requestId }, { open: true, requireConfirm: false });
      applied.push({ code: r.code, ok: true, request_id: requestId, event_id: x.event_id ?? null, replayed: !!x.replayed });
      log(`  ✅ ${r.code} → やめた (request_id ${requestId}・履歴 #${x.event_id})`);
    } catch (e) {
      applied.push({ code: r.code, ok: false, request_id: requestId, reason: e.reason || e.code || 'error', error: String(e.message || e) });
      log(`  ⛔ ${r.code} → 断られた (${e.reason || e.code || 'error'}): ${e.message}`);
    }
  }
  const ng = applied.filter((x) => !x.ok).length;
  log(`やめた ${applied.length - ng} 件・断られた ${ng} 件`);
  return { mode: 'apply', checks, applied, exitCode: ng ? 1 : 0 };
}

async function main() {
  let opts;
  try { opts = parseGoneArgs(process.argv.slice(2)); } catch (e) { console.error(`❌ ${e.message}`); process.exitCode = 2; return; }
  const url = opts.url || process.env.COMPANY_DB_MASTER_OPS_URL;
  if (!url) { console.error('❌ COMPANY_DB_MASTER_OPS_URL (運用のロール master_ops) か --url が要る'); process.exitCode = 2; return; }
  const client = await openPgClient(url, { application_name: 'master-gone-from-ne' });
  try {
    await client.query(`set statement_timeout = '20s'`);
    await client.query(`set lock_timeout = '10s'`);
    const r = await runGone(pgAdapter(client), opts);
    if (opts.json) fs.writeFileSync(opts.json, JSON.stringify({ mode: r.mode, checks: r.checks, applied: r.applied }, null, 2));
    process.exitCode = r.exitCode;
  } catch (e) {
    console.error(`❌ ${e.message}`);
    process.exitCode = e.code === 'GONE_ARGS' ? 2 : 1;
  } finally { try { await client.end(); } catch { /* */ } }
}

const isMain = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) main();
