#!/usr/bin/env node
/**
 * migrate-lock-watch.mjs — migrate の lock (migrate.mjs の MIGRATE_LOCK_NAME) を 45 分を超えて持っていたら GChat に知らせる見張り
 *   (D-60 PR 3a-i の後・設計 = AI_reference『システム設計/CompanyDB構想/13_Amazon利益のmart_設計_20260930.md』§3.10
 *    「migrate の runner の契約 (3a-i)」v3.9 の「見張り」・v3.12 の「45 分の見張り」・§5 の 0b-3 の (o))
 *
 * 契約 (設計):
 *   - migrate の lock (MIGRATE_LOCK_NAME) を持つ時間が **45 分** (migrate.mjs の MIGRATE_LOCK_ALERT_MINUTES) を超えたら GChat。
 *     CIC 1 文は statement_timeout = 30min で先に切れるはず = 鳴るのは止まっている印
 *   - 見張りの役割 (watcher) からは、ほかの役割の session の backend_start は見えない
 *     = 「lock を持つ pid を覚えて、見えた時刻から数える」(設計 v3.12)。見えれば (同じ役割・pg_read_all_stats) backend_start から数える
 *
 * 形 (PR #1638 Codex R1 の後):
 *   - **人が直接起動しない**。1 つのコマンド scripts/company-db/migrate-watched.mjs が子のプロセス (IPC つき = --supervised) として起動し、
 *     ① 最初の見回り (lock が無いことを見る) ② GChat の起動の知らせが本当に送れた (sendJobsChat === true) の後に「ready」を返す
 *     → それから migrate を始める。= lock を取る前に見張りが動いている = 「起動した時にもう持たれていた」は起きない
 *   - さらに migrate.mjs の CLI は concurrent-index の各文の前に、この見張りの接続 (application_name = company-db-migrate-lock-watch) が
 *     あることを確かめ、無ければ流さない (LOCK_WATCH_REQUIRED) = 見張りなしで CIC を始められない・見張りが途中で死んでも次の文で止まる
 *   - 朝の保険 = 見張り W15 (apps/company-db/watch・毎朝) が「朝の時点で lock が持たれていない」を見る (起動忘れ・見張りの消失)
 *   - 数え方 = backend_start が見えれば接続から。見えなければ lock が無かった直前の見回りの時刻から (長めに数える = 早めに鳴る)。
 *     起動し直した見張り (--since) は、最初に見た lock を migrate を始めた時刻から数える (長めに数える)
 *   - 知らせ: ⚠️ 45 分を超えた最初の見回りで 1 回・その後 60 分ごと・送れなければ次の見回りで送り直す /
 *     ✅ 鳴った後に外れた・❌ 8 時間の打ち切り = 終わりの知らせは数回 (既定 5 回・30 秒おき) 送り直し、届かなければ exit 3 /
 *     ❌ DB を 3 回続けて読めない = 「見張れていない」を 1 回 (届くまで次の読めない見回りで送り直す)
 *   - 終わり方 (exit): 0 = lock が外れた・migrate が終わった (親の done) 後に lock が無い・親が死んで lock が無い / 1 = 8 時間の打ち切り・見張り自身の失敗 /
 *     2 = 引数・設定の誤り / 3 = 終わりの知らせ (持ち主が替わった時の「外れた」も) が届かなかった / 4 = 起動の知らせが届かなかった (= migrate を始めない) /
 *     5 = ready から 10 分 migrate の lock が現れない
 *   - 書かない: 接続は default_transaction_read_only・statement_timeout 10s。lock を取らない・backend を止めない (止めるのは人)
 *   - 🆕 Codex R2 High: heartbeat = 見回りが通るたびに自分の接続の application_name を `company-db-migrate-lock-watch:<nonce>:<server の epoch 秒>` に変える
 *     (nonce = 親が run ごとに作り env CDB_MIGRATE_WATCH_NONCE で渡す)。runner は本物の PG の CIC の各文の前に、同じ nonce で 120 秒以内の heartbeat を確かめる
 *   - 🆕 Codex R2 Medium 2 (親が死んだとき): IPC が切れたら、lock が無ければ知らせて終わる (exit 0)・lock があれば外れるまで見張る (45 分の知らせも出す)。
 *     ready から 10 分 lock が一度も現れない (migrate が始まらない) = 知らせて終わる (exit 5)。lock の無い状態にも 8 時間の上限。
 *     見張りは detached で起動される (親が死んでも残る・Windows の job から外れる)。親と一緒に migrate は終わる = 接続が切れて lock が外れる
 *     (CIC は invalid が残りうる = 次に migrate-watched で流すと回収)。親が死んだ後に見張りも死ぬと知らせる手が無い = 朝の見張り W15 が lock の残りを拾う
 *
 * 引数 (親 = migrate-watched.mjs が付ける): --supervised [--since <epoch ms>]
 *   --dry-run = GChat に送らず、送る文を画面に出すだけ (試しの用)。--interval-sec・--alert-min は --dry-run の時だけ変えられる (本番は 60 秒・45 分)
 *   接続 = env COMPANY_DB_WATCH_URL だけ (接続文字列を引数に出さない)・送り先 = env GCHAT_WEBHOOK_JOBS (要対応スペース)
 */
import fs from 'node:fs';
import { fileURLToPath } from 'node:url';
import { openPgClient, MIGRATE_LOCK_NAME, MIGRATE_LOCK_ALERT_MINUTES, MIGRATE_LOCK_WATCH_APPLICATION_NAME, LOCK_WATCH_NONCE_ENV, LOCK_WATCH_NONCE_RE } from './migrate.mjs';
import { jobsHook, sendJobsChat } from '../logizard-import/notify-jobs.mjs';

export const WATCH_APPLICATION_NAME = MIGRATE_LOCK_WATCH_APPLICATION_NAME;
export const DEFAULTS = Object.freeze({ intervalSec: 60, alertMin: MIGRATE_LOCK_ALERT_MINUTES, repeatMin: 60, waitStartMin: 30, noStartMin: 10, maxHours: 8, readFailAlertCount: 3, terminalRetries: 5, terminalRetrySec: 30 });
/** 本番 (dry-run でない) の上限 = 見回りは 60 秒より間をあけない・知らせは 45 分より遅くしない (Codex R1 Low) */
export const PROD_LIMITS = Object.freeze({ maxIntervalSec: 60, maxAlertMin: MIGRATE_LOCK_ALERT_MINUTES });
export const EXIT = Object.freeze({ OK: 0, FAIL: 1, ARGS: 2, NOTIFY_END_FAILED: 3, NOTIFY_START_FAILED: 4, NO_START: 5 });
const MIN = 60000;

export function parseArgs(argv) {
  const out = { dryRun: false, supervised: false, since: null, ...DEFAULTS };
  const given = new Set();
  const num = (a, v, { min }) => {
    const n = Number(v);
    if (v === undefined || !Number.isFinite(n) || n < min) throw new Error(`${a} は ${min} 以上の数`);
    return n;
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    const v = () => argv[++i];
    if (a === '--dry-run') out.dryRun = true;
    else if (a === '--supervised') out.supervised = true;
    else if (a === '--since') { const x = v(); if (!/^\d{10,16}$/.test(String(x || ''))) throw new Error('--since は epoch の ms'); out.since = Number(x); }
    else if (a === '--interval-sec') { out.intervalSec = num(a, v(), { min: 1 }); given.add(a); }
    else if (a === '--alert-min') { out.alertMin = num(a, v(), { min: 0.01 }); given.add(a); }
    else if (a === '--url') throw new Error('--url は受けない (接続文字列をプロセスの引数に出さない) = env COMPANY_DB_WATCH_URL');
    else throw new Error(`知らない引数: ${a}`);
  }
  // 🆕 Codex R1 Low: 本番で 45 分の契約を弱めない = 値を変えられるのは --dry-run の時だけ
  if (!out.dryRun && given.size) throw new Error(`${[...given].join('・')} は --dry-run の時だけ (本番は ${DEFAULTS.intervalSec} 秒ごと・${DEFAULTS.alertMin} 分)`);
  if (!out.dryRun && (out.intervalSec > PROD_LIMITS.maxIntervalSec || out.alertMin > PROD_LIMITS.maxAlertMin)) throw new Error('本番の上限を超えている');
  return out;
}

/**
 * lock を持っている backend を読む (書かない)。条件は migrate.mjs の describeLockHolder と同じ (同じ DB・advisory・granted・objsubid 1・hashtextextended)。
 * 戻り = null (誰も持っていない) | { pid, applicationName, usename, startVisible, heldMs, phase, relation }
 *   startVisible = backend_start が見える (同じ役割・pg_read_all_stats)。見えれば heldMs = server の時計で接続からの時間
 */
export const LOCK_HOLDER_SQL = `
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
       order by l.pid`;
export const holderOfRows = (rows) => {
  if (!rows.length) return null;
  const r = rows[0];   // advisory の排他の lock = 持てるのは 1 つの backend だけ
  return { pid: Number(r.pid), applicationName: r.application_name || null, usename: r.usename || null, startVisible: r.start_visible === true, heldMs: r.held_ms == null ? null : Number(r.held_ms), phase: r.phase || null, relation: r.relation || null };
};
export async function readLockHolder(client) {
  await client.query('begin isolation level read committed read only');
  try {
    await client.query('set local search_path = pg_catalog, pg_temp');
    const { rows } = await client.query(LOCK_HOLDER_SQL, [MIGRATE_LOCK_NAME]);
    await client.query('commit');
    return holderOfRows(rows);
  } catch (e) {
    try { await client.query('rollback'); } catch { /* 接続が死んでいれば呼び手が作り直す */ }
    throw e;
  }
}

/**
 * 🆕 Codex R2 High: heartbeat = 自分の接続の application_name を `company-db-migrate-lock-watch:<nonce>:<server の epoch 秒>` に (取引の外・読むだけの session でも SET はできる)。
 * runner (migrate.mjs の assertLockWatchFresh) が同じ形を読む。見回りが通った時だけ呼ぶ (読めない・止まった見張りは古くなる)
 */
export async function writeLockWatchHeartbeat(client, nonce) {
  if (!LOCK_WATCH_NONCE_RE.test(String(nonce || ''))) throw new Error('heartbeat の nonce の形が違う');
  await client.query("select pg_catalog.set_config('application_name', $1::pg_catalog.text || ':' || $2::pg_catalog.text || ':' || pg_catalog.floor(extract(epoch from pg_catalog.clock_timestamp()))::pg_catalog.int8::pg_catalog.text, false)", [MIGRATE_LOCK_WATCH_APPLICATION_NAME, nonce]);
}

const minText = (ms) => `${Math.floor(ms / MIN)} 分`;
export const holderLine = (h) => `pid ${h.pid}${h.applicationName ? ` (${h.applicationName}${h.usename ? `・${h.usename}` : ''})` : ''}`;
const COUNT_TEXT = {
  backend_start: '接続から (backend_start)',
  prev_poll: '前の見回りで lock が無かった時から (長めに数える)',
  since: 'migrate を始めた時から (見張りを起動し直した・長めに数える)',
  watch_start: '見張りを始めた時から (始めた時にはもう持たれていた = 実際はもっと長い)',
};
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
export const startText = ({ dbName, restart }) => restart
  ? `🟡 Company DB の migrate の lock の見張りを起動し直した (前の見張りが止まった)・DB ${dbName}・migrate を始めた時刻から数え、${MIGRATE_LOCK_ALERT_MINUTES} 分を超えたら知らせる`
  : `🟢 Company DB の migrate の lock の見張りを始めた (これから migrate を流す)・DB ${dbName}・${MIGRATE_LOCK_ALERT_MINUTES} 分を超えて lock が持たれたら知らせる (終わって鳴らなければ知らせは来ない)`;

/** 届くまで数回送る (起動の知らせ・終わりの知らせ)。戻り = 届いたか */
export async function sendWithRetry(send, text, { retries, retrySec, sleep, log = () => {} }) {
  for (let i = 0; i < retries; i++) {
    if (await send(text).catch(() => false) === true) return true;
    log(`❌ GChat に送れなかった (${i + 1}/${retries}): ${text.split('\n')[0]}`);
    if (i < retries - 1) await sleep(retrySec * 1000);
  }
  return false;
}

/**
 * 見張りの本体 (時計・読み・送りは差し替えられる = 試験)。
 * @param {{ readHolder, send, now?, sleep?, log?, dbName?, isParentDone?: () => boolean, supervised?: boolean,
 *           priorPoll?: { atMs: number, pid: number|null }, sinceMs?: number|null, intervalSec?, alertMin?, repeatMin?, waitStartMin?, maxHours?,
 *           readFailAlertCount?, terminalRetries?, terminalRetrySec? }} o
 *   priorPoll = 起動の時の見回り (lock が無いのを見た時刻 = 数え始めの上限)。sinceMs = 起動し直した見張りの「migrate を始めた時刻」
 *   isParentDone = 親 (migrate-watched) が「migrate が終わった」と言った。その後 lock が無ければ終わる
 *   supervised = 親の下で動く (起動の待ち (waitStartMin) の代わりに noStartMin = ready から lock が一度も現れなければ exit 5)
 *   isParentGone = 親との IPC が切れた (親が死んだ)。lock が無ければ知らせて終わる・あれば外れるまで見張る
 * @returns {Promise<{ code: number, outcome: 'released'|'never_seen'|'max_hours'|'parent_done'|'parent_gone'|'no_start', sent: string[], notifyFailed: boolean }>}
 */
export async function runLockWatch(o) {
  const now = o.now || (() => Date.now());
  const sleep = o.sleep || ((ms) => new Promise((r) => setTimeout(r, ms)));
  const log = o.log || (() => {});
  const isParentDone = o.isParentDone || (() => false);
  const isParentGone = o.isParentGone || (() => false);
  const c = { ...DEFAULTS, ...Object.fromEntries(Object.entries(o).filter(([k, v]) => k in DEFAULTS && v != null)) };
  const dbName = o.dbName || '(不明)';
  const alertMs = c.alertMin * MIN, repeatMs = c.repeatMin * MIN, waitMs = c.waitStartMin * MIN, noStartMs = c.noStartMin * MIN, maxMs = c.maxHours * 60 * MIN, intervalMs = c.intervalSec * 1000;
  const sent = [];
  const send = async (text) => { const ok = (await o.send(text).catch(() => false)) === true; if (ok) sent.push(text); else log(`❌ GChat に送れなかった (次の見回りで送り直す): ${text.split('\n')[0]}`); return ok; };
  // 🆕 Codex R1 Medium 1: 終わりの知らせは届くまで数回送り、届かなければ exit 3 (黙って終わらない)
  const sendTerminal = async (text) => {
    const ok = await sendWithRetry(o.send, text, { retries: c.terminalRetries, retrySec: c.terminalRetrySec, sleep, log });
    if (ok) sent.push(text);
    return ok;
  };
  let earlierNotifyFailed = false;   // 🆕 Codex R2 Medium 1: 持ち主が替わった時の A の「外れた」が届かなかった = 最後に exit 3
  const finish = async (outcome, code, terminalText) => {
    let notifyFailed = earlierNotifyFailed;
    if (terminalText && !(await sendTerminal(terminalText))) { notifyFailed = true; log(`❌ 終わりの知らせを ${c.terminalRetries} 回送っても届かなかった = exit ${EXIT.NOTIFY_END_FAILED}`); }
    return { code: notifyFailed ? EXIT.NOTIFY_END_FAILED : code, outcome, sent, notifyFailed };
  };

  const startMs = now();
  let prevPollMs = o.priorPoll ? o.priorPoll.atMs : null;   // 直前の見回りの時刻
  let prevPid = o.priorPoll ? o.priorPoll.pid : null;       // 直前の見回りで lock を持っていた pid (読めなかった見回りは数えない)
  let cur = null;             // 今見ている持ち主 { pid, firstMs, countFrom, alertedAtMs, lastH }
  let everSeen = false;
  let readFails = 0, readFailAlerted = false;
  let parentGoneNoticed = false;

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
        const text = `✅ Company DB の migrate の lock (${MIGRATE_LOCK_NAME}) が外れた: ${holderLine(cur.lastH)}・DB ${dbName}・おおよそ ${minText(heldMs)}持っていた\n・結果は migrate の窓の log と \`migrate.mjs --list\` で確かめる (記録されていない file があれば README の回収の手順)`;
        const alerted = cur.alertedAtMs != null;
        cur = null;
        if (!h) return finish('released', EXIT.OK, alerted ? text : null);
        if (alerted && !(await sendTerminal(text))) { earlierNotifyFailed = true; log(`❌ 「外れた」が届かなかった (持ち主が替わった = 見張りは続け、最後に exit ${EXIT.NOTIFY_END_FAILED})`); }
      }
      if (h && isParentGone() && !parentGoneNoticed) {
        parentGoneNoticed = true;
        log(`親 (migrate-watched) が終わった・lock は ${holderLine(h)} が持っている = 外れるまで見張る`);
        await send(`⚠️ Company DB の migrate の見張りの親 (migrate-watched) が途中で終わった・DB ${dbName}・migrate の lock は ${holderLine(h)} が持っている = 外れるまで見張る (${c.alertMin} 分の知らせも出す)\n・migrate の窓を人が見る`);
      }
      if (h) {
        everSeen = true;
        if (!cur) {
          // 数え始め: backend_start が見えれば server の時計の値。見えなければ lock が無かった直前の見回りから (長めに数える = 早めに鳴る)
          let countFrom, firstMs;
          if (h.startVisible && h.heldMs != null) { countFrom = 'backend_start'; firstMs = t - h.heldMs; }
          else if (prevPollMs != null && prevPid !== h.pid) { countFrom = 'prev_poll'; firstMs = prevPollMs; }
          else if (o.sinceMs != null) { countFrom = 'since'; firstMs = Math.min(o.sinceMs, t); }
          else { countFrom = 'watch_start'; firstMs = startMs; }
          cur = { pid: h.pid, firstMs, countFrom, alertedAtMs: null, lastH: h };
          log(`lock を持っている: ${holderLine(h)}・数え方 = ${COUNT_TEXT[countFrom]}`);
          if (countFrom === 'watch_start') log('⚠️ 見張りを始めた時にはもう lock が持たれていて、接続の時刻も見えない = 見張りを始めた時から数える (実際はもっと長い)。migrate-watched.mjs から流せば起きない');
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
      } else if (isParentDone()) {
        log('migrate が終わり、lock も無い = 見張りを終える');
        return finish(everSeen ? 'released' : 'parent_done', EXIT.OK, null);
      } else if (isParentGone()) {
        log('親 (migrate-watched) が終わり、lock も無い = 知らせて見張りを終える');
        return finish('parent_gone', EXIT.OK, `⚠️ Company DB の migrate の見張りの親 (migrate-watched) が途中で終わった・DB ${dbName}・migrate の lock は今は無い = 見張りを終える\n・migrate の窓の log と \`migrate.mjs --list\` で、記録されていない file が無いかを確かめる (README の回収の手順)`);
      } else if (o.supervised && !everSeen && t - startMs >= noStartMs) {
        log(`ready から ${minText(t - startMs)}、migrate の lock が一度も現れない = 知らせて見張りを終える (exit ${EXIT.NO_START})`);
        return finish('no_start', EXIT.NO_START, `⚠️ Company DB の migrate が始まらない: 見張りの ready から ${minText(t - startMs)} lock が現れない・DB ${dbName} = 見張りを終える (この後に始まる concurrent-index は heartbeat が無いので止まる)\n・migrate の窓を人が見る`);
      } else if (!o.supervised && !everSeen && t - startMs >= waitMs) {
        log(`起動から ${minText(t - startMs)}、lock は一度も現れなかった = 見張りを終える`);
        return finish('never_seen', EXIT.OK, null);
      }
      prevPid = h ? h.pid : null;
      prevPollMs = t;
    }
    // 🆕 Codex R2 Medium 2: lock の無い健康な状態にも上限 (孤児の見張りの接続を残さない)
    if (t - startMs >= maxMs) {
      return finish('max_hours', EXIT.FAIL, `❌ Company DB の migrate の lock の見張りを打ち切る (${c.maxHours} 時間)${cur ? `: lock はまだ ${holderLine(cur.lastH)} が持っている (${minText(t - cur.firstMs)})` : !readOk ? ': DB を読めないまま' : ': lock は無い (migrate が終わったか始まっていない)'}・DB ${dbName}\n・人が pg_stat_activity と migrate の窓を見る`);
    }
    await sleep(intervalMs);
  }
}

/** 本物の接続で読む (見回りごとに読めなければ接続を作り直す)。🆕 nonce があれば、読めた見回りのたびに heartbeat を書く (runner が CIC の各文の前に探す) */
export function pgHolderReader(url, { nonce = null } = {}) {
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
      try {
        const h = await readLockHolder(client);
        if (nonce) await writeLockWatchHeartbeat(client, nonce);
        return h;
      } catch (e) {
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

/**
 * 親の下で動く見張り (CLI の --supervised の本体・試験は読み・送り・IPC を差し替える)。
 * ① DB 名と最初の見回り ② 起動の知らせ (届くまで数回・届かなければ exit 4 = ready を返さない = 親は migrate を始めない) ③ ready ④ 見張り
 * @param {{ reader: { read, dbName }, send, ipcSend: (m) => void, isParentDone, isParentGone?, sinceMs?, log?, now?, sleep?, watchOpts? }} o
 */
export async function runSupervised(o) {
  const now = o.now || (() => Date.now());
  const sleep = o.sleep || ((ms) => new Promise((r) => setTimeout(r, ms)));
  const log = o.log || (() => {});
  const w = { ...DEFAULTS, ...(o.watchOpts || {}) };
  const dbName = await o.reader.dbName();
  const atMs = now();
  const first = await o.reader.read();
  if (first && o.sinceMs == null) {
    // 始める前から別の migrate が lock を持っている = 親は migrate を始めない (流しても MIGRATE_LOCKED)。知らせは送らない
    log(`⚠️ 始める前から lock を ${holderLine(first)} が持っている (別の migrate が動いている) = migrate を始めない`);
    o.ipcSend({ type: 'held', pid: first.pid, holder: holderLine(first) });
    return { code: EXIT.FAIL, outcome: 'held_at_start', sent: [] };
  }
  // 🆕 Codex R1 Medium 2: GChat に本当に送れた (=== true) ことを確かめてから ready (送れない見張りで migrate を始めない)
  const ok = await sendWithRetry(o.send, startText({ dbName, restart: o.sinceMs != null }), { retries: 3, retrySec: 5, sleep, log });
  if (!ok) { log(`❌ 起動の知らせが GChat に届かない (GCHAT_WEBHOOK_JOBS) = 見張りにならない = exit ${EXIT.NOTIFY_START_FAILED}`); return { code: EXIT.NOTIFY_START_FAILED, outcome: 'start_notify_failed', sent: [] }; }
  o.ipcSend({ type: 'ready', dbName });
  log(`見張りを始める: DB ${dbName}・lock ${MIGRATE_LOCK_NAME}・${w.intervalSec} 秒ごと・${w.alertMin} 分で知らせる`);
  return runLockWatch({ ...w, readHolder: () => o.reader.read(), send: o.send, log, now, sleep, dbName, isParentDone: o.isParentDone, isParentGone: o.isParentGone, supervised: true, priorPoll: { atMs, pid: first ? first.pid : null }, sinceMs: o.sinceMs ?? null });
}

const fold = (x) => (process.platform === 'win32' ? x.toLowerCase() : x);
const isMain = (() => { try { return !!process.argv[1] && fold(fs.realpathSync.native(process.argv[1])) === fold(fs.realpathSync.native(fileURLToPath(import.meta.url))); } catch { return false; } })();
if (isMain) {
  // 🆕 Codex R2 Medium 2: 親が死んだ後は画面の pipe が閉じていることがある = 書けなくても落ちない (知らせは GChat)
  process.stdout.on('error', () => {}); process.stderr.on('error', () => {});
  const say = (m) => console.log(`[migrate-lock-watch] ${new Date(Date.now() + 9 * 3600000).toISOString().slice(11, 19)} ${m}`);
  let code = EXIT.FAIL, reader = null;
  try {
    await import('dotenv/config');
    let a;
    try { a = parseArgs(process.argv.slice(2)); } catch (e) { console.error(`[migrate-lock-watch] ${e.message}`); code = EXIT.ARGS; throw null; }
    // 🆕 Codex R1 High: 人が別の窓で起動しない = 親 (migrate-watched.mjs) の IPC が無ければ起動しない (試しの --dry-run だけ例外)
    if (!a.supervised || typeof process.send !== 'function') {
      if (!a.dryRun) { console.error('[migrate-lock-watch] 単独では起動しない = node scripts/company-db/migrate-watched.mjs から (見張りを起動 → GChat に送れたのを確かめてから migrate)'); code = EXIT.ARGS; throw null; }
    }
    const url = (process.env.COMPANY_DB_WATCH_URL || '').trim();
    // 🆕 Codex R2 High: 親の下では run の nonce が要る (heartbeat = runner が CIC の各文の前に探す)。単独の --dry-run では無くてよい (heartbeat を出さない)
    const nonce = (process.env[LOCK_WATCH_NONCE_ENV] || '').trim() || null;
    if (a.supervised && !LOCK_WATCH_NONCE_RE.test(nonce || '')) { console.error(`[migrate-lock-watch] run の nonce (env ${LOCK_WATCH_NONCE_ENV}) が無い・形が違う = migrate-watched.mjs から起動する`); code = EXIT.ARGS; throw null; }
    if (nonce && !LOCK_WATCH_NONCE_RE.test(nonce)) { console.error(`[migrate-lock-watch] env ${LOCK_WATCH_NONCE_ENV} の形が違う`); code = EXIT.ARGS; throw null; }
    if (!url) { console.error('[migrate-lock-watch] 接続先が無い (env COMPANY_DB_WATCH_URL)'); code = EXIT.ARGS; throw null; }
    if (!a.dryRun && !jobsHook(process.env)) { console.error('[migrate-lock-watch] GChat の送り先 (env GCHAT_WEBHOOK_JOBS) が無い・壊れている = 見張りにならないので起動しない'); code = EXIT.ARGS; throw null; }
    const send = a.dryRun ? async (text) => { console.log(`[migrate-lock-watch] (dry-run・送らない)\n${text}`); return true; } : (text) => sendJobsChat(text);
    reader = pgHolderReader(url, { nonce });
    // 親の「migrate が終わった」(done) = 次の見回りを待たずに起こす
    let parentDone = false, parentGone = false;
    const wakers = new Set();
    const sleep = (ms) => new Promise((r) => { const tm = setTimeout(() => { wakers.delete(wake); r(); }, ms); const wake = () => { clearTimeout(tm); wakers.delete(wake); r(); }; wakers.add(wake); });
    const ipc = typeof process.send === 'function' && a.supervised;
    if (ipc) {
      process.on('message', (m) => { if (m && m.type === 'done') { parentDone = true; for (const w of [...wakers]) w(); } });
      process.on('disconnect', () => { parentGone = true; say('親 (migrate-watched) との接続が切れた = lock が無ければ知らせて終わる・あれば外れるまで見張る'); for (const w of [...wakers]) w(); });
    }
    const watchOpts = { intervalSec: a.intervalSec, alertMin: a.alertMin };
    let r;
    if (ipc) {
      r = await runSupervised({ reader, send, ipcSend: (m) => { try { process.send(m); } catch { /* 親が先に終わった */ } }, isParentDone: () => parentDone, isParentGone: () => parentGone, sinceMs: a.since, log: say, sleep, watchOpts });
    } else {
      const dbName = await reader.dbName();
      say(`(dry-run・単独) 見張りを始める: DB ${dbName}`);
      r = await runLockWatch({ ...watchOpts, readHolder: () => reader.read(), send, log: say, sleep, dbName });
    }
    say(`終わり: ${r.outcome} (知らせた ${r.sent.length} 件)`);
    code = r.code;
  } catch (e) {
    if (e !== null) { console.error(`[migrate-lock-watch] ❌ ${String(e && e.message).replace(/\s+/g, ' ').slice(0, 400)}`); code = EXIT.FAIL; }
  } finally {
    if (reader) await reader.close();
  }
  // fetch / pg の直後に process.exit() しない (Windows の Node は libuv の assertion で 127 になる。#1386)
  process.exitCode = code;
  if (typeof process.disconnect === 'function' && process.connected) { try { process.disconnect(); } catch { /* */ } }
  setTimeout(() => process.exit(code), 10000).unref();
}
