#!/usr/bin/env node
/**
 * master-legacy-latency.mjs — 古い入口の門が段階を読むのにかかる時間を測る (読むだけ。何も書かない。PR #1565 中間レビュー 2 回目 M-A)
 *
 * なぜ: 門は段階を毎回読む。miniPC → Render の PostgreSQL は、つなぎ直すと TLS と認証で時間がかかる。
 *   画面は 1 秒で諦める (5 分前までの結果を使う)・書き込みは つなぐ 3 秒 + 読む 5 秒 を 2 回まで。
 *   配る前に、この場所で「つなぎ直し + 読む」と「つないだままで読む」の時間 (p50 / p95 / いちばん遅い) を見ておく。
 * 測るもの (どちらも select phase from ops.master_cutover_state だけ):
 *   cold = 毎回つなぎ直す (つなぐ + 読む + 閉じる)。門のプールは 10 分つないだままなので、ふだんは起きない (起動の直後・10 分使わなかった後・切られた後)
 *   warm = 1 本をつないだまま読む (ふだんの門)
 * 使い方 (miniPC で。接続先は門と同じ = COMPANY_DB_MASTER_GATE_MINIPC_URL → 無ければ COMPANY_DB_URL):
 *   node -r dotenv/config scripts/company-db/master-legacy-latency.mjs --host minipc            # 20 回ずつ・0.5 秒おき
 *   node -r dotenv/config scripts/company-db/master-legacy-latency.mjs --host minipc --n 50 --gap 1000
 * 終了コード 0 = 測れた (目安を超えたら ⚠️ を出す) / 1 = つながらない・読めない / 2 = 引数
 * 🚨 接続文字列・パスワードは出さない (ログインの役の名前だけ)。同時には 1 本だけつなぐ (接続の上限を食わない)
 */
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { openPgClient } from './migrate.mjs';
import { gateUrlFor, GATE_URL_ENV, SCREEN_READ_TIMEOUT_MS } from '../../lib/master-legacy-gate.mjs';

const PHASE_SQL = 'select phase from ops.master_cutover_state where id = 1';

export function percentile(values, p) {
  if (!values.length) return null;
  const a = [...values].sort((x, y) => x - y);
  return a[Math.min(a.length - 1, Math.max(0, Math.ceil((p / 100) * a.length) - 1))];
}
const summary = (xs) => ({ n: xs.length, p50: percentile(xs, 50), p95: percentile(xs, 95), max: xs.length ? Math.max(...xs) : null });

/** 測る。open(url) = pg の Client を返す (試験で差し替え)。戻り値 { role, phase, cold: {connect, total}, warm, errors } (ms) */
export async function measureLatency({ url, n = 20, gapMs = 500, open = (u) => openPgClient(u, { application_name: 'master-legacy-latency', connectionTimeoutMillis: 5000, statement_timeout: 5000 }) } = {}) {
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  const coldConnect = [], coldTotal = [], warm = [], errors = [];
  let role = null, phase = null;
  for (let i = 0; i < n; i++) {
    const t0 = performance.now();
    let c = null;
    try {
      c = await open(url);
      const t1 = performance.now();
      const r = (await c.query(`${PHASE_SQL}`)).rows[0];
      phase = r ? r.phase : null;
      coldConnect.push(Math.round(t1 - t0));
      coldTotal.push(Math.round(performance.now() - t0));
    } catch (e) { errors.push(`cold ${i + 1}: ${String((e && e.message) || e).slice(0, 200)}`); } finally { if (c) { try { await c.end(); } catch { /* */ } } }
    if (gapMs) await sleep(gapMs);
  }
  let c = null;
  try {
    c = await open(url);
    role = (await c.query('select session_user::text as u')).rows[0].u;
    for (let i = 0; i < n; i++) {
      const t0 = performance.now();
      try { await c.query(PHASE_SQL); warm.push(Math.round(performance.now() - t0)); } catch (e) { errors.push(`warm ${i + 1}: ${String((e && e.message) || e).slice(0, 200)}`); }
      if (gapMs) await sleep(gapMs);
    }
  } catch (e) { errors.push(`warm: ${String((e && e.message) || e).slice(0, 200)}`); } finally { if (c) { try { await c.end(); } catch { /* */ } } }
  return { role, phase, cold: { connect: summary(coldConnect), total: summary(coldTotal) }, warm: summary(warm), errors };
}

const isMain = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) {
  const args = process.argv.slice(2);
  const getArg = (f) => { const i = args.indexOf(f); return i >= 0 && i < args.length - 1 ? args[i + 1] : null; };
  const host = getArg('--host');
  const n = Number(getArg('--n') || 20);
  const gapMs = Number(getArg('--gap') || 500);
  if (!['render', 'minipc'].includes(host) || !Number.isInteger(n) || n < 1 || n > 100 || !Number.isFinite(gapMs) || gapMs < 0) {
    console.error('--host render か --host minipc を付ける (--n 1〜100・--gap ミリ秒)'); process.exitCode = 2;
  } else {
    const url = gateUrlFor(host, process.env) || String(process.env.COMPANY_DB_URL || '').trim();
    if (!url) { console.error(`${GATE_URL_ENV[host]} も COMPANY_DB_URL も無い`); process.exitCode = 2; } else {
      const r = await measureLatency({ url, n, gapMs });
      const f = (s) => (s.n ? `p50 ${s.p50}ms / p95 ${s.p95}ms / いちばん遅い ${s.max}ms (${s.n} 回)` : '測れない');
      console.log(`段階を読む時間 (${host}・ログインの役 ${r.role ?? '?'}・今の段階 ${r.phase ?? '?'}):`);
      console.log(`  つなぎ直し (つなぐだけ)   : ${f(r.cold.connect)}`);
      console.log(`  つなぎ直し + 読む         : ${f(r.cold.total)}`);
      console.log(`  つないだまま読む          : ${f(r.warm)}`);
      if (r.cold.total.n && r.cold.total.p95 > SCREEN_READ_TIMEOUT_MS) console.log(`  ⚠️ つなぎ直しの p95 が画面の待ち ${SCREEN_READ_TIMEOUT_MS}ms を超える = 起動の直後などは画面が 5 分前の結果か帯になる (書き込みは待つので止まらない)`);
      if (r.warm.n && r.warm.p95 > 300) console.log('  ⚠️ つないだままでも p95 が 300ms を超える = 書き込みのたびにこれだけ待つ');
      for (const e of r.errors) console.log(`  ✗ ${e}`);
      process.exitCode = r.errors.length && !r.warm.n ? 1 : 0;
    }
  }
}
