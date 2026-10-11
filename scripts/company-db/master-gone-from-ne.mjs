#!/usr/bin/env node
/**
 * master-gone-from-ne.mjs — 業務で使える商品 (登録の状態 available) を「NE から消えた」でまとめてやめる (cancelled)。0070・中原さんの決定 10/11 Q1 = a
 *
 * やめてよいかは Company DB が自分で確かめる (ops.sku_gone_from_ne_check = 完全で信用できる NE の取得 (照合 ② の記録の回) に 3 日続けて無い・
 * 数えた一番新しい取得が新しい・今の NE のコードの台帳にも無い・単品・生きた NE 登録の CSV が無い・ほかのセット / 構成の依頼 / 出品の構成に使われていない・ほかの商品の親でない)。
 * このスクリプトは確かめの答えと「計画のハッシュ」を出す (dry-run・既定) と、人の OK の後に ops.cancel_sku_gone_from_ne を 1 件ずつ呼ぶ (--apply) だけ。
 *   🆕 #1683 Codex R1 Medium: --apply は dry-run の計画のハッシュ (--expect-hash = やめてよい件の sku_id・最初 / 最後に無かった記録の回・証拠の全部) を受け取り、
 *   1 つの取引の中で確かめ直したハッシュと、1 件ずつ DB の関数が鍵の後に返した証拠が、全部その計画と同じときだけ commit する
 *   (1 つでも違う・断られた = 取引ごと巻き戻す = 1 件も変えない・終了コード 1)。記録 = 1 件ごとの保存の記録 (ops.master_edit_requests・request_id) と登録の状態の履歴
 *
 * 使い方 (miniPC の PowerShell・リポジトリ直下。接続 = env COMPANY_DB_MASTER_OPS_URL (運用のロール master_ops) か --url):
 *   # 候補 (読むだけ) = 業務で使える商品で、数えた一番新しい完全な NE の取得に無い SKU と、やめてよいかの答え
 *   node -r dotenv/config scripts/company-db/master-gone-from-ne.mjs
 *   # コードを渡して確かめる (読むだけ)。--codes はカンマ区切り / --codes-file は 1 行 1 コード (# から後は読まない)
 *   node -r dotenv/config scripts/company-db/master-gone-from-ne.mjs --codes-file gone.txt [--json out.json]
 *   # やめる (🚨 dry-run の答えを中原さんが見て OK の後だけ)。--expect-hash = dry-run が出した計画のハッシュ (対象と証拠が 1 つでも違えば 1 件も動かさない)
 *   node -r dotenv/config scripts/company-db/master-gone-from-ne.mjs --apply --codes-file gone.txt --expect-hash <64 桁> --actor <人のメール> --reason "<理由>" --yes [--json out.json]
 * 終了コード: 0 = dry-run を出した / apply で全部やめた・1 = apply を巻き戻した (計画と違う・断られた)・つながらない・2 = 引数が違う
 */
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { openPgClient, pgAdapter } from './migrate.mjs';
import { listGoneChecks, cancelSkuGoneFromNe, goneReasonOf, gonePlanHash, goneEvidenceLine, GONE_REASON_WORDS } from '../../lib/master-gone.mjs';

const fail = (message, code = 'GONE_ARGS') => Object.assign(new Error(message), { code });

/** 引数を読む。誤り = GONE_ARGS を投げる */
export function parseGoneArgs(argv) {
  const o = { apply: false, yes: false, codes: null, codesFile: null, expectHash: null, actor: null, reason: null, json: null, url: null };
  const need = (i, name) => { const v = argv[i + 1]; if (v == null || v.startsWith('--')) throw fail(`${name} の後に値が要る`); return v; };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--apply') o.apply = true;
    else if (a === '--yes') o.yes = true;
    else if (a === '--codes') { o.codes = need(i, a).split(',').map((s) => s.trim()).filter(Boolean); i++; }
    else if (a === '--codes-file') { o.codesFile = need(i, a); i++; }
    else if (a === '--expect-hash') { const v = need(i, a); if (!/^[0-9a-f]{64}$/.test(v)) throw fail('--expect-hash は dry-run が出した 64 桁の 16 進'); o.expectHash = v; i++; }
    else if (a === '--actor') { o.actor = need(i, a); i++; }
    else if (a === '--reason') { o.reason = need(i, a); i++; }
    else if (a === '--json') { o.json = need(i, a); i++; }
    else if (a === '--url') { o.url = need(i, a); i++; }
    else throw fail(`知らない引数: ${a}`);
  }
  if (o.codes && o.codesFile) throw fail('--codes と --codes-file はどちらか 1 つ');
  if (o.apply) {
    if (!o.codes && !o.codesFile) throw fail('--apply はコード (--codes か --codes-file) が要る (候補の全部を黙ってやめない)');
    if (o.expectHash == null) throw fail('--apply は --expect-hash <dry-run が出した計画のハッシュ> が要る');
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
    out.push(`${r.ok ? '✅ やめてよい' : '⛔ やめない'}  ${r.code ?? '(?)'}  NE に無い ${r.streak ?? 0}/${r.required_runs ?? '?'} 日`);
    for (const p of r.problems || []) out.push(`      - ${GONE_REASON_WORDS[p.reason] || p.reason}: ${p.text}`);
  }
  return out;
}

/**
 * 本体 (試験は db を渡す)。戻り値 = { mode, checks, planHash, applied: [{ code, ok, request_id, event_id? }], exitCode, error? }
 */
export async function runGone(db, opts, { log = console.log } = {}) {
  const codes = opts.codesFile ? readCodesFile(opts.codesFile) : opts.codes;
  if (codes && !codes.length) throw fail('コードが 1 つも無い');
  if (!opts.apply) {
    const checks = await listGoneChecks(db, codes ?? null);
    const planHash = gonePlanHash(checks);
    const okN = checks.filter((r) => r.ok).length;
    log(`確かめ (読むだけ・Company DB の ops.sku_gone_from_ne_check): ${checks.length} 件・やめてよい ${okN} 件・やめない ${checks.length - okN} 件`);
    for (const l of checkLines(checks)) log(l);
    log(`計画のハッシュ (--apply に --expect-hash で渡す・やめてよい ${okN} 件の対象と証拠): ${planHash}`);
    return { mode: 'dry-run', checks, planHash, applied: [], exitCode: 0 };
  }
  // apply = 1 つの取引: 確かめ直す → 計画のハッシュが同じか → 1 件ずつやめる (DB の関数が鍵の後に確かめ直した証拠が計画と同じか) → 全部そろったら commit
  const applied = [];
  await db.query('begin');
  try {
    const checks = await listGoneChecks(db, codes);
    const planHash = gonePlanHash(checks);
    const okRows = checks.filter((r) => r.ok);
    log(`確かめ直し: やめてよい ${okRows.length} 件・計画のハッシュ ${planHash}`);
    if (planHash !== opts.expectHash) {
      throw Object.assign(new Error(`計画のハッシュが dry-run と違う (今 ${planHash}・渡された ${opts.expectHash}) = 対象か証拠が変わった。dry-run をもう一度見てから`), { reason: 'plan_mismatch' });
    }
    for (const r of okRows) {
      const requestId = crypto.randomUUID();
      let x;
      try {
        x = await cancelSkuGoneFromNe(db, { actor: opts.actor, skuId: r.sku_id, reason: opts.reason, requestId }, { open: true, requireConfirm: false, inTx: true });
      } catch (e) {
        applied.push({ code: r.code, ok: false, request_id: requestId, reason: e.reason || e.code || 'error', error: String(e.message || e) });
        throw Object.assign(new Error(`${r.code} が断られた (${e.reason || e.code || 'error'}): ${e.message}`), { reason: e.reason || 'refused' });
      }
      if (JSON.stringify(goneEvidenceLine(x.sku_id, x.evidence)) !== JSON.stringify(goneEvidenceLine(r.sku_id, r.evidence))) {
        applied.push({ code: r.code, ok: false, request_id: requestId, reason: 'evidence_mismatch' });
        throw Object.assign(new Error(`${r.code} の証拠が確かめ直しの時と違う (やめる間に照合 ② の記録が入った) = 巻き戻す`), { reason: 'evidence_mismatch' });
      }
      applied.push({ code: r.code, ok: true, request_id: requestId, event_id: x.event_id ?? null });
      log(`  ✅ ${r.code} → やめた (request_id ${requestId}・履歴 #${x.event_id})`);
    }
    await db.query('commit');
    log(`やめた ${applied.length} 件 (計画どおり・1 つの取引で commit)`);
    return { mode: 'apply', checks, planHash, applied, exitCode: 0 };
  } catch (e) {
    try { await db.query('rollback'); } catch { /* */ }
    log(`⛔ 巻き戻した = 1 件も変えていない: ${e.message}`);
    return { mode: 'apply', checks: null, planHash: null, applied: applied.map((a) => ({ ...a, ok: false, rolled_back: true })), exitCode: 1,
      error: { reason: e.reason || e.code || 'error', message: String(e.message || e) } };
  }
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
    if (opts.json) fs.writeFileSync(opts.json, JSON.stringify({ mode: r.mode, plan_hash: r.planHash, checks: r.checks, applied: r.applied, error: r.error ?? null }, null, 2));
    process.exitCode = r.exitCode;
  } catch (e) {
    console.error(`❌ ${e.message}`);
    process.exitCode = e.code === 'GONE_ARGS' ? 2 : 1;
  } finally { try { await client.end(); } catch { /* */ } }
}

const isMain = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) main();
