#!/usr/bin/env node
/**
 * migrate-watched.mjs — 45 分の見張りつきで migrate を流す 1 つのコマンド (本番の Company DB の migrate はこれから)
 *   (設計 = AI_reference『システム設計/CompanyDB構想/13_Amazon利益のmart_設計_20260930.md』§3.10 v3.9「見張り」・v3.12「45 分の見張り」・
 *    PR #1638 Codex R1 High = 見張りなしで migrate を始められない形)
 *
 * 1 つのコマンドの中で起きること:
 *   ⓪ run ごとの nonce (16 桁の hex) を作り、見張りと migrate の子のプロセスに env CDB_MIGRATE_WATCH_NONCE で渡す
 *   ① 見張り (migrate-lock-watch.mjs --supervised) を子のプロセスで起動する (見張りは見回りのたびに heartbeat = application_name に nonce と時刻)
 *   ② 見張りが「最初の見回りで lock が無い」と「GChat の起動の知らせが本当に届いた (sendJobsChat === true)」を確かめて ready を返す
 *      (返さない・起動の知らせが届かない・3 分で返らない・始める前から別の migrate が lock を持っている = **migrate を始めない** exit 1)
 *   ③ migrate (migrate.mjs・接続は env COMPANY_DB_URL) を子のプロセスで始める
 *      (migrate.mjs は本物の PG の concurrent-index の各文の前に、同じ nonce の 120 秒以内の heartbeat を必ず確かめ、無ければ流さない = LOCK_WATCH_REQUIRED)
 *   ④ migrate の間に見張りが死んだら GChat に知らせ、migrate を始めた時刻から数える見張りを起動し直す (3 回まで。migrate は止めない =
 *      見張りが無ければ次の CIC の文の前で migrate.mjs が止まる)
 *   ⑤ migrate が終わったら見張りに done を送る → 見張りは lock が無いのを見て終わる (lock が残っていれば外れるまで見張る)
 *   見張りが exit 5 (ready から 10 分 migrate の lock が現れない) で終わったら起動し直さない (見張りが知らせた・CIC は heartbeat が無いので止まる) = exit 3
 *   子のプロセスを起動できない (error event) = 見張りなら migrate を始めない (exit 1)・migrate なら exit 1
 *   このコマンド自身が死ぬと (窓を閉じる・Ctrl+C・kill)、migrate (detached でない子) は一緒に終わる = 接続が切れて lock が外れる。見張りは detached で残り、
 *   IPC の切れを見て、lock が無ければ知らせて終わる・あれば外れるまで見張る。🚨 見張りも死ぬと誰も知らせない = 朝の見張り W15 が lock の残りを拾う
 *
 * 使い方 (miniPC の PowerShell 5.1・リポジトリ直下):
 *   node scripts\company-db\migrate-watched.mjs            # 未適用を全部
 *   node scripts\company-db\migrate-watched.mjs --to 0061  # その番号まで
 *   env (リポジトリ直下の .env): COMPANY_DB_URL (migrate)・COMPANY_DB_WATCH_URL (見張り = watcher の役割)・GCHAT_WEBHOOK_JOBS (要対応スペース)
 *   (--list / --dry-run / --index-expect は今までどおり migrate.mjs を直接 = lock を長く持たない・CIC を流さない)
 *
 * 終了コード: 0 = migrate も見張りも済んだ / 1 = migrate が失敗・migrate を始めなかった / 2 = 引数・設定の誤り /
 *   3 = migrate は済んだが見張りが途中で止まった・終わりの知らせが届かなかった (人が GChat と log を見る)
 */
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fork, spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { jobsHook, sendJobsChat } from '../logizard-import/notify-jobs.mjs';
import { DEFAULTS as WATCH_DEFAULTS, EXIT as WATCH_EXIT } from './migrate-lock-watch.mjs';
import { LOCK_WATCH_NONCE_ENV } from './migrate.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const WATCH_CLI = path.join(__dirname, 'migrate-lock-watch.mjs');
export const MIGRATE_CLI = path.join(__dirname, 'migrate.mjs');
export const WATCHED_DEFAULTS = Object.freeze({ readyTimeoutMs: 3 * 60000, maxRestarts: 3 });

export function parseArgs(argv) {
  const out = { to: null };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--to') { out.to = argv[++i]; if (!/^\d{4}$/.test(String(out.to || ''))) throw new Error('--to は 4 桁の番号'); }
    else if (a === '--url') throw new Error('--url は受けない (接続文字列をプロセスの引数に出さない) = env COMPANY_DB_URL / COMPANY_DB_WATCH_URL');
    else if (['--list', '--dry-run', '--index-expect', '--dir'].includes(a)) throw new Error(`${a} は migrate.mjs を直接 (このコマンドは本番に流すときだけ)`);
    else throw new Error(`知らない引数: ${a}`);
  }
  return out;
}

/** run ごとの nonce (application_name は 63 バイトまで = 16 桁の hex) */
export const newRunNonce = () => crypto.randomBytes(8).toString('hex');

/** exit と error のどちらか先の 1 回だけ (起動できない = error だけで exit が来ないことがある) */
function exitOf(child, label, log) {
  return new Promise((r) => {
    let done = false;
    const once = (code) => { if (!done) { done = true; r(code); } };
    child.on('exit', (code, sig) => once(code ?? 1));
    child.on('error', (e) => { log(`❌ ${label} のプロセスを起動できない・誤り: ${e && e.message}`); once(1); });
  });
}

/** 子のプロセスの見張り (IPC つき)。戻り = { ready: Promise<{type,...}>, exited: Promise<number>, done(), kill() }。execPath は試験 (起動できない形) だけ */
export function forkWatcher({ since = null, nonce, extraArgs = [], env = process.env, stdio = 'inherit', execPath = process.execPath, log = (m) => console.error(`[migrate-watched] ${m}`) } = {}) {
  const child = fork(WATCH_CLI, ['--supervised', ...(since != null ? ['--since', String(since)] : []), ...extraArgs], { env: { ...env, [LOCK_WATCH_NONCE_ENV]: nonce }, execPath, stdio: stdio === 'pipe' ? ['ignore', 'pipe', 'pipe', 'ipc'] : 'inherit',
    // 🆕 Codex R2 Medium 2: 親 (このコマンド) が死んでも見張りは残って知らせる。Windows の libuv は detached でない子を親と同じ job に入れ、親が殺されると一緒に終わる
    //   (試験 E3 の前の実験で確かめた) = detached (別の process group・job の外 = 親が kill されても残る = 試験 E3。窓を閉じた時・Ctrl+C は試せていない)・windowsHide (別の窓を出さない)。migrate は detached にしない (親と一緒に終わる = 接続が切れて lock が外れる)
    detached: true, windowsHide: true });
  const exited = exitOf(child, '見張り', log);
  const ready = new Promise((r) => child.on('message', (m) => { if (m && (m.type === 'ready' || m.type === 'held')) r(m); }));
  return {
    pid: child.pid, child, ready, exited,
    done: () => { try { if (child.connected) child.send({ type: 'done' }); } catch { /* 先に終わった */ } },
    kill: () => { try { child.kill(); } catch { /* */ } },
  };
}
/** 子のプロセスの migrate (接続は env COMPANY_DB_URL = 引数に出さない・nonce は env)。戻り = { exited: Promise<number> }。execPath は試験だけ */
export function spawnMigrate({ args = [], nonce, env = process.env, stdio = 'inherit', execPath = process.execPath, log = (m) => console.error(`[migrate-watched] ${m}`) } = {}) {
  const child = spawn(execPath, [MIGRATE_CLI, ...args], { env: { ...env, [LOCK_WATCH_NONCE_ENV]: nonce }, stdio: stdio === 'pipe' ? ['ignore', 'pipe', 'pipe'] : 'inherit' });
  return { pid: child.pid, child, exited: exitOf(child, 'migrate', log) };
}

/** 待ちの時間切れ (終わったら cancel で外す = 残った timer でプロセスを生かさない) */
const timeout = (ms, v) => { let tm; const p = new Promise((r) => { tm = setTimeout(() => r(v), ms); }); return { p, cancel: () => clearTimeout(tm) }; };

/**
 * 本体 (子のプロセスの作り方・送り・時計は差し替えられる = 試験)。
 * @param {{ startWatcher: ({ since, nonce }) => handle, startMigrate: ({ nonce }) => { exited }, notify: (text) => Promise<boolean>, log?, now?, readyTimeoutMs?, maxRestarts?, nonce? }} o
 * @returns {Promise<{ code: number, reason: string, migrateCode: number|null, watcherCode: number|null, restarts: number }>}
 */
export async function runWatchedMigrate(o) {
  const log = o.log || (() => {});
  const now = o.now || (() => Date.now());
  const readyTimeoutMs = o.readyTimeoutMs ?? WATCHED_DEFAULTS.readyTimeoutMs;
  const maxRestarts = o.maxRestarts ?? WATCHED_DEFAULTS.maxRestarts;
  const nonce = o.nonce || newRunNonce();
  /** 見張りを起動して ready を待つ。戻り = { ok, w, why } */
  const startAndWait = async (since) => {
    let w;
    try { w = o.startWatcher({ since, nonce }); } catch (e) { return { ok: false, w: null, why: `見張りを起動できない (${e && e.message})` }; }
    const to = timeout(readyTimeoutMs, { timeout: true });
    const r = await Promise.race([w.ready.then((m) => ({ m })), w.exited.then((code) => ({ exited: code })), to.p]);
    to.cancel();
    if (r.m && r.m.type === 'ready') return { ok: true, w };
    if (r.m && r.m.type === 'held') { const t2 = timeout(15000); await Promise.race([w.exited, t2.p]); t2.cancel(); w.kill(); return { ok: false, w, why: `始める前から別の migrate が lock を持っている (${r.m.holder || `pid ${r.m.pid}`})` }; }
    if (r.timeout) { w.kill(); return { ok: false, w, why: `見張りが ${Math.round(readyTimeoutMs / 1000)} 秒で ready を返さない` }; }
    return { ok: false, w, why: `見張りが ready の前に終わった (exit ${r.exited}${r.exited === 4 ? ' = 起動の知らせが GChat に届かない' : ''})` };
  };

  // ①② 見張り → ready
  const first = await startAndWait(null);
  if (!first.ok) {
    log(`❌ ${first.why} = migrate を始めない`);
    return { code: 1, reason: 'WATCH_NOT_READY', why: first.why, migrateCode: null, watcherCode: null, restarts: 0 };
  }
  let w = first.w;
  // ③ migrate
  const migStartMs = now();
  log('見張りが動いている (GChat に起動の知らせが届いた) = migrate を始める');
  let m;
  try { m = o.startMigrate({ nonce }); } catch (e) {
    log(`❌ migrate を起動できない (${e && e.message})`);
    m = { exited: Promise.resolve(1) };
  }
  let migrateCode = null, restarts = 0, watcherLost = false;
  // ④ migrate の間に見張りが死んだら起動し直す
  for (;;) {
    const r = await Promise.race([m.exited.then((code) => ({ migrate: code })), w.exited.then((code) => ({ watcher: code }))]);
    if ('migrate' in r) { migrateCode = r.migrate; break; }
    if (r.watcher === WATCH_EXIT.NO_START) {
      // 見張りが「migrate が始まらない」と知らせて終わった = 起動し直さない (CIC は heartbeat が無いので止まる)
      log(`❌ 見張りが ready から ${WATCH_DEFAULTS.noStartMin} 分 migrate の lock を見なかった (exit ${r.watcher}) = 起動し直さない・migrate の終わりを待つ`);
      watcherLost = true;
      migrateCode = await m.exited;
      break;
    }
    log(`❌ migrate の間に見張りが終わった (exit ${r.watcher}) = 起動し直す`);
    let restarted = false;
    while (restarts < maxRestarts) {
      restarts++;
      await o.notify(`❌ Company DB の migrate の lock の見張りが migrate の間に止まった (exit ${r.watcher}) = 起動し直す (${restarts}/${maxRestarts})・migrate は流れたまま (見張りが無ければ次の concurrent-index の文の前で止まる)`).catch(() => false);
      const again = await startAndWait(migStartMs);
      if (again.ok) { w = again.w; restarted = true; break; }
      log(`❌ 見張りを起動し直せない: ${again.why}`);
    }
    if (!restarted) {
      watcherLost = true;
      await o.notify(`❌ Company DB の migrate の lock の見張りを ${maxRestarts} 回起動し直せなかった = 45 分の見張りが効いていない。migrate は流れたまま (次の concurrent-index の文の前で LOCK_WATCH_REQUIRED で止まる)・人が migrate の窓と pg_stat_activity を見る`).catch(() => false);
      migrateCode = await m.exited;
      break;
    }
  }
  // ⑤ 見張りに done → lock が無ければ終わる (残っていれば外れるまで見張る = 待つ)
  let watcherCode = null;
  if (!watcherLost) {
    w.done();
    watcherCode = await w.exited;
  }
  log(`migrate exit ${migrateCode}・見張り ${watcherLost ? '途中で止まった (起動し直せなかった)' : `exit ${watcherCode}`}${restarts ? `・起動し直し ${restarts} 回` : ''}`);
  if (migrateCode !== 0) return { code: 1, reason: 'MIGRATE_FAILED', migrateCode, watcherCode, restarts };
  if (watcherLost || watcherCode !== 0) return { code: 3, reason: watcherLost ? 'WATCH_LOST' : 'WATCH_FAILED', migrateCode, watcherCode, restarts };
  return { code: 0, reason: 'OK', migrateCode, watcherCode, restarts };
}

const fold = (x) => (process.platform === 'win32' ? x.toLowerCase() : x);
const isMain = (() => { try { return !!process.argv[1] && fold(fs.realpathSync.native(process.argv[1])) === fold(fs.realpathSync.native(fileURLToPath(import.meta.url))); } catch { return false; } })();
if (isMain) {
  const say = (m) => console.log(`[migrate-watched] ${m}`);
  let code = 1;
  try {
    await import('dotenv/config');
    let a;
    try { a = parseArgs(process.argv.slice(2)); } catch (e) { console.error(`[migrate-watched] ${e.message}`); code = 2; throw null; }
    const missing = ['COMPANY_DB_URL', 'COMPANY_DB_WATCH_URL'].filter((k) => !String(process.env[k] || '').trim());
    if (missing.length) { console.error(`[migrate-watched] env ${missing.join('・')} が無い`); code = 2; throw null; }
    if (!jobsHook(process.env)) { console.error('[migrate-watched] GChat の送り先 (env GCHAT_WEBHOOK_JOBS) が無い・壊れている = 見張りにならないので migrate を始めない'); code = 2; throw null; }
    say(`見張り (${WATCH_DEFAULTS.intervalSec} 秒ごと・${WATCH_DEFAULTS.alertMin} 分で GChat) を起動してから migrate を流す${a.to ? ` (--to ${a.to})` : ''}`);
    const r = await runWatchedMigrate({
      startWatcher: ({ since, nonce }) => forkWatcher({ since, nonce }),
      startMigrate: ({ nonce }) => spawnMigrate({ args: a.to ? ['--to', a.to] : [], nonce }),
      notify: (text) => sendJobsChat(text),
      log: say,
    });
    say(`終わり: ${r.reason} (exit ${r.code})`);
    code = r.code;
  } catch (e) {
    if (e !== null) { console.error(`[migrate-watched] ❌ ${String(e && e.message).replace(/\s+/g, ' ').slice(0, 400)}`); code = 1; }
  }
  process.exitCode = code;
  setTimeout(() => process.exit(code), 10000).unref();
}
