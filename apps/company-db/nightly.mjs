/**
 * nightly.mjs — Company DB を毎晩そっくり合わせ直す (Company DB構想 07 §7 の 6)
 *
 * なぜ要るか:
 *   初期ロードは 2026-09-10 に 1 回流しただけ。放っておくと Company DB は「その日の写し」のまま古びる。
 *   ロードは冪等 (同じ材料なら何も変わらない) なので、毎晩流せば **昨日 SQLite で起きたことが翌朝には入っている**。
 *
 * いつ動くか:
 *   既定は 02:00 JST。夜間の取り込み (Step 0 は 23:30 JST) が終わったあと、
 *   Render 外バックアップ (03:30 JST) の **前** に置く。こうすると、その晩の控えに新しいロードの結果が入る。
 *
 * 約束:
 *   - Render の中でだけ動く (読み込み元の SQLite が Render の DATA_DIR にある)。miniPC では材料が無いので何もしない
 *   - 単一飛行。手で叩いた `POST /apps/company-db/sync/load` が走っていたら、この回は見送る (二重に流さない)
 *   - 台帳 = config/jobs-registry.mjs の `company-db-nightly-load`。成功・失敗の両方を jobs-monitor に ping する
 *     (dead-man 方式なので、**動かなくなったら「締切超過」で催促が出る**)
 *   - Dark Launch: env `COMPANY_DB_LOAD_CRON_ENABLED` が '1' / 'true' のときだけ起動する
 *   - 上限つき。終わらないときは打ち切って失敗にする (次の晩に持ち越さない)
 *
 * env:
 *   COMPANY_DB_LOAD_CRON_ENABLED  '1'|'true' で有効 (既定 OFF)
 *   COMPANY_DB_LOAD_CRON          cron 式 UTC (既定 '0 17 * * *' = JST 02:00)
 *   COMPANY_DB_LOAD_TIMEOUT_MS    1 回の上限 (既定 1800000 = 30 分。実測は 6〜10 秒)
 *   DATA_DIR / COMPANY_DB_URL     どちらか無ければ動かない (設定漏れとして失敗を ping する)
 *
 * 手で動かす: Render Shell で `node apps/company-db/nightly.mjs run`
 */
import cron from 'node-cron';
import { startLoad } from './router.mjs';
import { pingJob } from '../jobs-monitor/ping-local.js';

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
export async function runNightlyLoad({
  log = (m) => console.log(`[company-db nightly] ${m}`),
  start = startLoad,
  ping = pingJob,
} = {}) {
  const dataDir = process.env.DATA_DIR;
  const url = process.env.COMPANY_DB_URL;
  if (!dataDir || !url) {
    const note = 'DATA_DIR / COMPANY_DB_URL が無い (Render の env を確かめる)';
    log(note);
    ping(JOB_ID, 'fail', note);
    return { ok: false, skipped: false, note };
  }
  let timeoutMs;
  try {
    timeoutMs = envInt('COMPANY_DB_LOAD_TIMEOUT_MS', DEFAULT_TIMEOUT_MS, 1000, 6 * 60 * 60 * 1000);
  } catch (e) {
    log(e.message);
    ping(JOB_ID, 'fail', e.message);
    return { ok: false, skipped: false, note: e.message };
  }

  const started = Date.now();
  const r = start({ dataDir, url, apply: true, host: 'render-nightly', log });
  if (!r.started) {
    // 手で叩いたぶんが走っている。二重に流さず、この回は見送る。
    // 🚨 ping は打たない (動いていない証拠にはならないが、成功でもない)。締切を過ぎれば催促が出る
    const note = `別のロードが走っているので見送った (run=${r.current?.run_id})`;
    log(note);
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
    // 打ち切り。走っているものは止められない (Postgres の 1 トランザクションなので、
    // 途中で切ると巻き戻る。放っておいて次の朝に /status の interrupted で見る)
    const note = `${Math.round(timeoutMs / 60000)} 分で終わらないので打ち切った (run=${r.current.run_id})`;
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

// 手で動かす: node apps/company-db/nightly.mjs run
if (process.argv[1] && process.argv[1].endsWith('nightly.mjs') && process.argv[2] === 'run') {
  const r = await runNightlyLoad();
  process.exit(r.ok ? 0 : 1);
}
