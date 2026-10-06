#!/usr/bin/env node
/**
 * migrate-lock-watch.mjs — migrate の lock (company_db_migrate) を 45 分を超えて持っていたら GChat に知らせる見張り
 *   (D-60 PR 3a-i の後・設計 = AI_reference『システム設計/CompanyDB構想/13_Amazon利益のmart_設計_20260930.md』§3.10
 *    「migrate の runner の契約 (3a-i)」v3.9 の「見張り」・v3.12 の「45 分の見張り」・§5 の 0b-3 の (o))
 *
 * 契約 (設計):
 *   - migrate の lock (`company_db_migrate`) を持つ時間が **45 分** (migrate.mjs の MIGRATE_LOCK_ALERT_MINUTES) を超えたら GChat。
 *     CIC 1 文は statement_timeout = 30min で先に切れるはず = 鳴るのは止まっている印
 *   - 見張りの役割 (watcher) からは、ほかの役割の session の backend_start は見えない
 *     = 「lock を持つ pid を覚えて、見えた時刻から数える」(設計 v3.12)。見えれば (同じ役割・pg_read_all_stats) backend_start から数える
 *     (runner は接続の直後に lock を取る = --list と同じ物差し)
 *
 * 形 (設計に無い所の決め・PR の「迷った所」):
 *   - 毎朝 1 回の見張り (W・daily-sync) では 45 分を測れない (次の朝まで鳴らない) = 本番の migrate を流す間だけ、人が別の窓で起動する道具にした。
 *     定期実行ではない (自分で終わる) = 台帳 (config/jobs-registry.mjs) には載せない
 *   - 見回りは既定 60 秒ごと。pid が見えなかった直前の見回りの時刻から数える (lock を取ったのはその後 = 長めに数える = 早めに鳴る向き)。
 *     最初の見回りでもう持たれていて backend_start も見えないときは、見張りを始めた時刻から数え、知らせに「実際はもっと長い」と書く
 *   - 終わり方: lock が外れた (見えた後で無くなった) = exit 0 (知らせた後なら「外れた」も送る) /
 *     起動から --wait-start-min (既定 30 分) の間 lock が一度も現れない = exit 0 (見張る物が無い) /
 *     --max-hours (既定 8 時間) を超えてもまだ持たれている = 「見張りを打ち切る」を送って exit 1 /
 *     引数・設定の誤り = exit 2 (GChat の送り先が無ければ起動しない = 見張りにならない)
 *   - 知らせた後も持たれていれば --repeat-min (既定 60 分) ごとにもう一度。送れなかった知らせは次の見回りで送り直す
 *   - DB を 3 回続けて読めなければ「見張れていない」を 1 回送る (接続は見回りごとに作り直す)
 *   - 書かない: 接続は default_transaction_read_only・statement_timeout 10s。lock を取らない・backend を止めない (止めるのは人)
 *
 * 使い方 (miniPC の PowerShell 5.1・リポジトリ直下・本番の migrate を流す **前に** 別の窓で):
 *   node scripts\company-db\migrate-lock-watch.mjs
 *   (接続 = env COMPANY_DB_WATCH_URL (照会用の watcher の役割) か --url。送り先 = env GCHAT_WEBHOOK_JOBS (要対応スペース))
 *   --interval-sec N (既定 60) / --alert-min N (既定 45) / --repeat-min N (既定 60) / --wait-start-min N (既定 30) / --max-hours N (既定 8)
 *   --dry-run = GChat に送らず、送る文を画面に出すだけ (試しの用)
 */
import fs from 'node:fs';
import { fileURLToPath } from 'node:url';
import { openPgClient, MIGRATE_LOCK_NAME, MIGRATE_LOCK_ALERT_MINUTES } from './migrate.mjs';
import { jobsHook, sendJobsChat } from '../logizard-import/notify-jobs.mjs';

export const WATCH_APPLICATION_NAME = 'company-db-migrate-lock-watch';
export const DEFAULTS = Object.freeze({ intervalSec: 60, alertMin: MIGRATE_LOCK_ALERT_MINUTES, repeatMin: 60, waitStartMin: 30, maxHours: 8, readFailAlertCount: 3 });
const MIN = 60000;

export function parseArgs(argv) {
  const out = { url: null, dryRun: false, ...DEFAULTS };
  const num = (a, v, { min, int = false }) => {
    const n = Number(v);
    if (v === undefined || !Number.isFinite(n) || n < min || (int && !Number.isInteger(n))) throw new Error(`${a} は ${min} 以上の${int ? '整' : ''}数`);
    return n;
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    const v = () => argv[++i];
    if (a === '--url') { out.url = v(); if (!out.url) throw new Error('--url の値が無い'); }
    else if (a === '--dry-run') out.dryRun = true;
    else if (a === '--interval-sec') out.intervalSec = num(a, v(), { min: 1 });
    else if (a === '--alert-min') out.alertMin = num(a, v(), { min: 0.01 });
    else if (a === '--repeat-min') out.repeatMin = num(a, v(), { min: 0.01 });
    else if (a === '--wait-start-min') out.waitStartMin = num(a, v(), { min: 0.01 });
    else if (a === '--max-hours') out.maxHours = num(a, v(), { min: 0.001 });
    else throw new Error(`知らない引数: ${a}`);
  }
  if (out.url && !/^postgres(ql)?:\/\//.test(out.url)) throw new Error('--url は postgres:// で始まる接続文字列');
  return out;
}

/**
 * lock を持っている backend を読む (書かない)。条件は migrate.mjs の describeLockHolder と同じ (同じ DB・advisory・granted・objsubid 1・hashtextextended)。
 * 戻り = null (誰も持っていない) | { pid, applicationName, usename, startVisible, heldMs, phase, relation }
 *   startVisible = backend_start が見える (同じ役割・pg_read_all_stats)。見えれば heldMs = server の時計で接続からの時間
 */
export async function readLockHolder(client) {
  await client.query('begin isolation level read committed read only');
  try {
    await client.query('set local search_path = pg_catalog, pg_temp');
    const { rows } = await client.query(`
      select l.pid, a.application_name::pg_catalog.text as application_name, a.usename::pg_catalog.text as usename,
             (a.backend_start is not null) as start_visible,
             (extract(epoch from (pg_catalog.clock_timestamp() - a.backend_start)) * 1000)::pg_catalog.float8 as held_ms,
             p.phase::pg_catalog.text as phase, p.relid::pg_catalog.regclass::pg_catalog.text as relation
        from pg_catalog.pg_locks l
        left join pg_catalog.pg_stat_activity a on a.pid = l.pid
        left join pg_catalog.pg_stat_progress_create_index p on p.pid = l.pid
       where l.locktype = 'advisory' and l.granted and l.objsubid = 1
         and l.database = (select oid from pg_catalog.pg_database where datname = pg_catalog.current_database())
         and ((l.classid::pg_catalog.int8 << 32) | l.objid::pg_catalog.int8) = pg_catalog.hashtextextended($1, 0)
       order by l.pid`, [MIGRATE_LOCK_NAME]);
    await client.query('commit');
    if (!rows.length) return null;
    const r = rows[0];   // advisory の排他の lock = 持てるのは 1 つの backend だけ
    return { pid: Number(r.pid), applicationName: r.application_name || null, usename: r.usename || null, startVisible: r.start_visible === true, heldMs: r.held_ms == null ? null : Number(r.held_ms), phase: r.phase || null, relation: r.relation || null };
  } catch (e) {
    try { await client.query('rollback'); } catch { /* 接続が死んでいれば呼び手が作り直す */ }
    throw e;
  }
}

const minText = (ms) => `${Math.floor(ms / MIN)} 分`;
const holderLine = (h) => `pid ${h.pid}${h.applicationName ? ` (${h.applicationName}${h.usename ? `・${h.usename}` : ''})` : ''}`;
const COUNT_TEXT = { backend_start: '接続から (backend_start)', prev_poll: '前の見回りで lock が無かった時から (長めに数える)', watch_start: '見張りを始めた時から (始めた時にはもう持たれていた = 実際はもっと長い)' };
const HINT = [
  'CIC 1 文は statement_timeout = 30 分で切れるはず = 止まっている印 (ただし file・文が多い回は全体で 45 分を超えうる)',
  '見ること (読むだけ): pg_stat_progress_create_index と pg_stat_activity (application_name = \'company-db-migrate\')・migrate の窓の log',
  '止めるなら人が pg_cancel_backend (この見張りは止めない)。手で DROP INDEX (CONCURRENTLY なし) はしない。回収の手順 = db/company/README.md「concurrent-index の migration が途中で止まったとき」',
].map((x) => `・${x}`).join('\n');

export function alertText({ h, elapsedMs, countFrom, dbName, alertMin, repeat }) {
  return [
    `⚠️ Company DB の migrate の lock (${MIGRATE_LOCK_NAME}) を ${minText(elapsedMs)}持っている (${alertMin} 分を超えた${repeat ? '・まだ外れていない' : ''})`,
    `・DB ${dbName}・持っている接続 ${holderLine(h)}`,
    `・数え方 = ${COUNT_TEXT[countFrom]}`,
    ...(h.phase ? [`・作っている index = ${h.relation || '(表は見えない)'} の ${h.phase}`] : []),
    HINT,
  ].join('\n');
}

/**
 * 見張りの本体 (時計・読み・送りは差し替えられる = 試験)。
 * @param {{ readHolder: () => Promise<object|null>, send: (text: string) => Promise<boolean>, now?: () => number, sleep?: (ms: number) => Promise<void>,
 *           log?: (m: string) => void, dbName?: string, intervalSec?: number, alertMin?: number, repeatMin?: number, waitStartMin?: number, maxHours?: number, readFailAlertCount?: number }} o
 * @returns {Promise<{ code: number, outcome: 'released'|'never_seen'|'max_hours', sent: string[] }>}
 */
export async function runLockWatch(o) {
  const now = o.now || (() => Date.now());
  const sleep = o.sleep || ((ms) => new Promise((r) => setTimeout(r, ms)));
  const log = o.log || (() => {});
  const c = { ...DEFAULTS, ...Object.fromEntries(Object.entries(o).filter(([k, v]) => k in DEFAULTS && v != null)) };
  const dbName = o.dbName || '(不明)';
  const alertMs = c.alertMin * MIN, repeatMs = c.repeatMin * MIN, waitMs = c.waitStartMin * MIN, maxMs = c.maxHours * 60 * MIN, intervalMs = c.intervalSec * 1000;
  const sent = [];
  const send = async (text) => { const ok = await o.send(text).catch(() => false); if (ok) sent.push(text); else log(`❌ GChat に送れなかった (次の見回りで送り直す): ${text.split('\n')[0]}`); return ok; };

  const startMs = now();
  let prevPollMs = null;      // 直前の見回りの時刻
  let prevPid = null;         // 直前の見回りで lock を持っていた pid (読めなかった見回りは数えない)
  let cur = null;             // 今見ている持ち主 { pid, firstMs, countFrom, alertedAtMs, lastH }
  let everSeen = false;
  let readFails = 0, readFailAlerted = false;

  for (;;) {
    const t = now();
    let h, readOk = true;
    try { h = await o.readHolder(); } catch (e) {
      readOk = false; readFails++;
      log(`DB を読めない (${readFails} 回続けて): ${String(e && e.message).replace(/\s+/g, ' ').slice(0, 200)}`);
      if (readFails >= c.readFailAlertCount && !readFailAlerted) {
        readFailAlerted = await send(`❌ Company DB の migrate の lock の見張りが DB を ${readFails} 回続けて読めない = 45 分の見張りが効いていない\n・DB ${dbName}${cur ? `・最後に見た持ち主 ${holderLine(cur.lastH)} (${minText(t - cur.firstMs)}前から)` : ''}\n・migrate の窓の log を人が見る (見張りは読み直しを続ける)`);
      }
    }
    if (readOk) {
      if (readFails >= c.readFailAlertCount) log(`DB をまた読めるようになった (${readFails} 回続けて読めなかった)`);
      readFails = 0; readFailAlerted = false;
      // 持ち主が外れた・替わった
      if (cur && (!h || h.pid !== cur.pid)) {
        const heldMs = t - cur.firstMs;
        log(`${holderLine(cur.lastH)} の lock が外れた (おおよそ ${minText(heldMs)})`);
        if (cur.alertedAtMs != null) await send(`✅ Company DB の migrate の lock (${MIGRATE_LOCK_NAME}) が外れた: ${holderLine(cur.lastH)}・DB ${dbName}・おおよそ ${minText(heldMs)}持っていた\n・結果は migrate の窓の log と \`migrate.mjs --list\` で確かめる (記録されていない file があれば README の回収の手順)`);
        cur = null;
        if (!h) return { code: 0, outcome: 'released', sent };
      }
      if (h) {
        everSeen = true;
        if (!cur) {
          // 数え始め: backend_start が見えれば server の時計の値を使う。見えなければ lock が無かった直前の見回りから (長めに数える = 早めに鳴る)
          let countFrom, firstMs;
          if (h.startVisible && h.heldMs != null) { countFrom = 'backend_start'; firstMs = t - h.heldMs; }
          else if (prevPollMs != null && prevPid !== h.pid) { countFrom = 'prev_poll'; firstMs = prevPollMs; }
          else { countFrom = 'watch_start'; firstMs = startMs; }
          cur = { pid: h.pid, firstMs, countFrom, alertedAtMs: null, lastH: h };
          log(`lock を持っている: ${holderLine(h)}・数え方 = ${COUNT_TEXT[countFrom]}`);
          if (countFrom === 'watch_start') log('⚠️ 見張りを始めた時にはもう lock が持たれていて、接続の時刻も見えない = 見張りを始めた時から数える (実際はもっと長い)。次からは migrate の前に見張りを起動する');
        } else if (h.startVisible && h.heldMs != null && cur.countFrom !== 'backend_start') {
          cur.countFrom = 'backend_start'; cur.firstMs = t - h.heldMs;
        }
        cur.lastH = h;
        const elapsed = t - cur.firstMs;
        const due = cur.alertedAtMs == null ? elapsed >= alertMs : t - cur.alertedAtMs >= repeatMs;
        if (due) {
          const repeat = cur.alertedAtMs != null;
          if (await send(alertText({ h, elapsedMs: elapsed, countFrom: cur.countFrom, dbName, alertMin: c.alertMin, repeat }))) cur.alertedAtMs = t;
        }
      } else if (!everSeen && t - startMs >= waitMs) {
        log(`起動から ${minText(t - startMs)}、lock は一度も現れなかった = 見張りを終える (migrate を流したなら、もう終わっている・流す前に起動し直す)`);
        return { code: 0, outcome: 'never_seen', sent };
      }
      prevPid = h ? h.pid : null;
      prevPollMs = t;
    }
    if (t - startMs >= maxMs && (cur || !readOk)) {
      await send(`❌ Company DB の migrate の lock の見張りを打ち切る (${c.maxHours} 時間)${cur ? `: lock はまだ ${holderLine(cur.lastH)} が持っている (${minText(t - cur.firstMs)})` : ': DB を読めないまま'}・DB ${dbName}\n・人が pg_stat_activity と migrate の窓を見る・見張りを続けるなら起動し直す`);
      return { code: 1, outcome: 'max_hours', sent };
    }
    await sleep(intervalMs);
  }
}

/** 本物の接続で読む (見回りごとに読めなければ接続を作り直す) */
export function pgHolderReader(url) {
  let client = null;
  const open = async () => {
    const c = await openPgClient(url, { application_name: WATCH_APPLICATION_NAME, connectionTimeoutMillis: 15000 });
    c.on('error', () => {});
    await c.query("set statement_timeout = '10s'");
    await c.query('set default_transaction_read_only = on');
    return c;
  };
  return {
    async read() {
      if (!client) client = await open();
      try { return await readLockHolder(client); } catch (e) {
        const dead = client; client = null;
        try { await dead.end(); } catch { /* */ }
        throw e;
      }
    },
    async dbName() {
      if (!client) client = await open();
      return (await client.query('select pg_catalog.current_database()::pg_catalog.text as d')).rows[0].d;
    },
    async close() { if (client) { const c = client; client = null; try { await c.end(); } catch { /* */ } } },
  };
}

const fold = (x) => (process.platform === 'win32' ? x.toLowerCase() : x);
const isMain = (() => { try { return !!process.argv[1] && fold(fs.realpathSync.native(process.argv[1])) === fold(fs.realpathSync.native(fileURLToPath(import.meta.url))); } catch { return false; } })();
if (isMain) {
  const say = (m) => console.log(`[migrate-lock-watch] ${new Date(Date.now() + 9 * 3600000).toISOString().slice(11, 19)} ${m}`);
  let code = 1, reader = null;
  try {
    await import('dotenv/config');
    let a;
    try { a = parseArgs(process.argv.slice(2)); } catch (e) { console.error(`[migrate-lock-watch] ${e.message}`); code = 2; throw null; }
    const url = (a.url || process.env.COMPANY_DB_WATCH_URL || '').trim();
    if (!url) { console.error('[migrate-lock-watch] 接続先が無い (env COMPANY_DB_WATCH_URL か --url)'); code = 2; throw null; }
    if (!a.dryRun && !jobsHook(process.env)) { console.error('[migrate-lock-watch] GChat の送り先 (env GCHAT_WEBHOOK_JOBS) が無い・壊れている = 見張りにならないので起動しない (試しなら --dry-run)'); code = 2; throw null; }
    const send = a.dryRun ? async (text) => { console.log(`[migrate-lock-watch] (dry-run・送らない)\n${text}`); return true; } : (text) => sendJobsChat(text);
    reader = pgHolderReader(url);
    const dbName = await reader.dbName();
    say(`見張りを始める: DB ${dbName}・lock ${MIGRATE_LOCK_NAME}・${a.intervalSec} 秒ごと・${a.alertMin} 分で知らせる${a.dryRun ? ' (dry-run = GChat に送らない)' : ''}・Ctrl+C で止める`);
    const r = await runLockWatch({ readHolder: () => reader.read(), send, log: say, dbName, intervalSec: a.intervalSec, alertMin: a.alertMin, repeatMin: a.repeatMin, waitStartMin: a.waitStartMin, maxHours: a.maxHours });
    say(`終わり: ${r.outcome} (知らせた ${r.sent.length} 件)`);
    code = r.code;
  } catch (e) {
    if (e !== null) { console.error(`[migrate-lock-watch] ❌ ${String(e && e.message).replace(/\s+/g, ' ').slice(0, 400)}`); code = 1; }
  } finally {
    if (reader) await reader.close();
  }
  // fetch / pg の直後に process.exit() しない (Windows の Node は libuv の assertion で 127 になる。#1386)
  process.exitCode = code;
  setTimeout(() => process.exit(code), 10000).unref();
}
