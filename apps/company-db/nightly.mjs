/**
 * nightly.mjs — Company DB を毎晩そっくり合わせ直す (Company DB構想 07 §7 の 6)
 *
 * なぜ要るか:
 *   初期ロードは 2026-09-10 に 1 回流しただけ。放っておくと Company DB は「その日の写し」のまま古びる。
 *   ロードは冪等 (同じ材料なら何も変わらない) なので、毎晩流せば **昨日 SQLite で起きたことが翌朝には入っている**。
 *
 * いつ動くか:
 *   既定は 02:00 JST。夜間の取り込み (Step 0 は 23:30 JST) が終わったあと、03:30 JST より前。
 *   Company DB を Drive へ送る仕組み (PR #1292) が入れば、その晩の控えに新しいロードの結果が入る。
 *
 * 約束:
 *   - **Render の中でだけ動く** (`lib/is-render.js` の `isRender()`)。miniPC も同じ server.js を動かすので、
 *     これが無いと二重実行になる。🚨 材料 (warehouse-mirror.db) の有無では見分けられない
 *     (miniPC でも mirror の初期化が同じファイルを作る)
 *   - 材料が無ければ、始めずに失敗として ping する (Render の中で材料が消えていたら、それは異常)
 *   - 単一飛行。同じプロセスの `POST /apps/company-db/sync/load` が走っていたら、この回は見送る (二重に流さない)。
 *     🚨 見送りが長引いている (前の回が `SKIP_ALERT_HOURS` より前に始まったまま) ときは **失敗として ping する**。
 *     黙って見送り続けると「動いているのか止まっているのか分からない」時間ができる
 *   - 台帳 = config/jobs-registry.mjs の `company-db-nightly-load`。成功・失敗の両方を jobs-monitor に ping する
 *     (dead-man 方式なので、**動かなくなったら「締切超過」で催促が出る**)
 *   - Dark Launch: env `COMPANY_DB_LOAD_CRON_ENABLED` が '1' / 'true' のときだけ起動する
 *   - 🚨 **待つのをやめる上限**はあるが、それでロード本体は止まらない (Postgres の 1 トランザクションを
 *     外から切る手段を持っていない)。上限に達したら失敗として ping し、あとは次の回の「見送りが長引いている」
 *     判定と dead-man に任せる。「打ち切ったので次はきれいに始まる」ではない
 *
 * env:
 *   COMPANY_DB_LOAD_CRON_ENABLED  '1'|'true' で有効 (既定 OFF)
 *   COMPANY_DB_LOAD_CRON          cron 式 UTC (既定 '0 17 * * *' = JST 02:00)
 *   COMPANY_DB_LOAD_TIMEOUT_MS    待つのをやめるまで (既定 1800000 = 30 分。実測は 6〜10 秒)
 *   COMPANY_DB_LOAD_SKIP_ALERT_H  見送りが何時間続いたら失敗として ping するか (既定 2)
 *   DATA_DIR / COMPANY_DB_URL     どちらか無ければ動かない (設定漏れとして失敗を ping する)
 *
 * 🚨 手で動かすときは **HTTP の口を使う** (miniPC から `node scripts/company-db/remote-load.mjs load --apply --wait`)。
 *    このファイルを別プロセスで直接動かすと、Web 側の単一飛行の見張りを迂回して二重に流れる
 */
import fs from 'node:fs';
import path from 'node:path';
import cron from 'node-cron';
import { startLoad } from './router.mjs';
import { pingJob } from '../jobs-monitor/ping-local.js';
// 🚨 miniPC も同じ server.js を動かすので、Render 専用の定期実行はこの gate を通す。
//    2026-08-05 に FBA同期・healthcheck・inbound-info が二重実行になった実績がある
import { isRender } from '../../lib/is-render.js';

/** config/jobs-registry.mjs の id */
export const JOB_ID = 'company-db-nightly-load';
const DEFAULT_CRON = '0 17 * * *';        // UTC 17:00 = JST 02:00
const DEFAULT_TIMEOUT_MS = 30 * 60 * 1000;

function envInt(name, def, min, max) {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return def;
  const n = Number(raw);
  if (!Number.isInteger(n) || n < min || n > max) throw new Error(`env ${name} が不正です: "${raw}" (整数 ${min}〜${max})`);
  return n;
}

/** 何が入って何が変わったかを 1 行にする (ping の note・ログ用。180 字で切られる) */
export function summarize(cur) {
  const s = cur.summary || {};
  const applied = Object.entries(s)
    .filter(([, v]) => v && typeof v === 'object' && Number(v.applied) > 0)
    .map(([k, v]) => `${k}+${v.applied}`);
  const parts = [`run=${cur.run_id}`];
  if (applied.length) parts.push(`変化 ${applied.join(' ')}`);
  else parts.push('変化なし');
  if (cur.conflicts) parts.push(`不一致 ${cur.conflicts}`);
  const un = Object.entries(cur.unresolved || {}).filter(([, n]) => Number(n) > 0);
  if (un.length) parts.push(`未解決 ${un.map(([k, n]) => `${k}:${n}`).join(',')}`);
  return parts.join(' / ');
}

/**
 * 1 回流す。戻り値 = { ok, skipped, note }
 * 🚨 例外は投げない (cron の中で投げても誰も拾わない)。失敗は ok=false と ping で表す
 * @param start 差し替え用 (試験)。既定は router.mjs の startLoad
 * @param ping  差し替え用 (試験)。既定は jobs-monitor への ping
 */
export async function runNightlyLoad(opts = {}) {
  // 🚨 何があっても投げない (cron の中で投げても誰も拾わない)。想定外も失敗として ping する
  try {
    return await runNightlyLoadInner(opts);
  } catch (e) {
    const note = `想定外: ${e && e.message ? e.message : e}`;
    (opts.log || ((m) => console.log(`[company-db nightly] ${m}`)))(note);
    (opts.ping || pingJob)(JOB_ID, 'fail', note);
    return { ok: false, skipped: false, note };
  }
}

async function runNightlyLoadInner({
  log = (m) => console.log(`[company-db nightly] ${m}`),
  start = startLoad,
  ping = pingJob,
} = {}) {
  if (!isRender()) {
    // 🚨 miniPC でも env がそろえば動いてしまう。Render 以外では **何も言わずに何もしない**
    //    (ping もしない。miniPC は担当ではないので、締切超過は Render 側の話として扱う)
    log('Render の中ではないので動かさない');
    return { ok: true, skipped: true, note: 'not-render' };
  }
  const dataDir = process.env.DATA_DIR;
  const url = process.env.COMPANY_DB_URL;
  if (!dataDir || !url) {
    const note = 'DATA_DIR / COMPANY_DB_URL が無い (Render の env を確かめる)';
    log(note);
    ping(JOB_ID, 'fail', note);
    return { ok: false, skipped: false, note };
  }
  // 材料が無ければ始めない (HTTP の口が 409 で断るのと同じ条件)。
  // 🚨 これが「Render の中かどうか」の実質的な見分け。手元や miniPC には mirror が無い
  if (!fs.existsSync(path.join(dataDir, 'warehouse-mirror.db'))) {
    const note = `材料が無い: ${path.join(dataDir, 'warehouse-mirror.db')} (Render の中で動かす)`;
    log(note);
    ping(JOB_ID, 'fail', note);
    return { ok: false, skipped: false, note };
  }
  let timeoutMs; let alertHours;
  try {
    timeoutMs = envInt('COMPANY_DB_LOAD_TIMEOUT_MS', DEFAULT_TIMEOUT_MS, 1000, 6 * 60 * 60 * 1000);
    alertHours = envInt('COMPANY_DB_LOAD_SKIP_ALERT_H', 2, 1, 240);
  } catch (e) {
    log(e.message);
    ping(JOB_ID, 'fail', e.message);
    return { ok: false, skipped: false, note: e.message };
  }

  const started = Date.now();
  const r = start({ dataDir, url, apply: true, host: 'render-nightly', log });
  if (!r.started) {
    // 別のロードが走っている。二重に流さず、この回は見送る。
    // 短い見送りは正常 (手で流している最中など) なので ping しない。
    // 🚨 ただし長引いているなら、それは「前の回が終わっていない」= 異常。失敗として ping して人を呼ぶ
    const startedAt = Date.parse(r.current?.started_at || '') || null;
    const hours = startedAt ? (Date.now() - startedAt) / 3600000 : null;
    const note = `別のロードが走っているので見送った (run=${r.current?.run_id}${hours == null ? '' : `, ${hours.toFixed(1)}時間前から`})`;
    log(note);
    if (hours != null && hours >= alertHours) {
      const bad = `前の回が終わっていない: ${note}`;
      ping(JOB_ID, 'fail', bad);
      return { ok: false, skipped: true, note: bad };
    }
    return { ok: true, skipped: true, note };
  }

  let timer = null;
  const cur = await Promise.race([
    r.done,
    new Promise((resolve) => { timer = setTimeout(() => resolve(null), timeoutMs); }),
  ]);
  if (timer) clearTimeout(timer);
  const secs = Math.round((Date.now() - started) / 1000);

  if (!cur) {
    // 🚨 待つのをやめただけ。ロード本体は走り続ける (Postgres の 1 トランザクションを外から切る手段がない)。
    //    次の回は「見送り」になり、長引けば上の判定で失敗として ping される
    const note = `${Math.round(timeoutMs / 60000)} 分待っても終わらない (ロードは走り続けている。run=${r.current.run_id})`;
    log(note);
    ping(JOB_ID, 'fail', note);
    return { ok: false, skipped: false, note };
  }
  if (cur.status !== 'done') {
    const note = `失敗: ${cur.error || cur.error_code || '理由不明'} (run=${cur.run_id})`;
    log(note);
    ping(JOB_ID, 'fail', note);
    return { ok: false, skipped: false, note };
  }
  const note = `${summarize(cur)} / ${secs}秒`;
  log(`成功: ${note}`);
  ping(JOB_ID, 'ok', note);
  return { ok: true, skipped: false, note };
}

/** server.js から呼ぶ。env で有効にしていなければ何もしない (Dark Launch) */
export function startCompanyDbNightlyLoadCron() {
  if (!isRender()) {
    console.log('[company-db nightly] Render の中ではないので cron 起動せず (miniPC との二重実行を防ぐ)');
    return null;
  }
  const enabled = process.env.COMPANY_DB_LOAD_CRON_ENABLED;
  if (enabled !== '1' && enabled !== 'true') {
    console.log('[company-db nightly] COMPANY_DB_LOAD_CRON_ENABLED 未設定のため cron 起動せず (Dark Launch)');
    return null;
  }
  const expr = process.env.COMPANY_DB_LOAD_CRON || DEFAULT_CRON;
  if (!cron.validate(expr)) {
    console.error(`[company-db nightly] COMPANY_DB_LOAD_CRON が cron 式として不正: "${expr}" → 起動しない`);
    return null;
  }
  const task = cron.schedule(expr, () => {
    runNightlyLoad().catch((e) => console.error(`[company-db nightly] 想定外: ${e.message}`));
  }, { timezone: 'UTC' });
  console.log(`[company-db nightly] cron 起動 (${expr} UTC)`);
  return task;
}

// 🚨 ここに「手で動かす」入口は作らない。別プロセスから呼ぶと Web 側の単一飛行の見張り (state.current) を
//    共有しないので、cron と同時に走って running.json / latest.json を取り合う (Codex 2026-09-10)。
//    手で流すときは HTTP の口を使う: node scripts/company-db/remote-load.mjs load --apply --wait
