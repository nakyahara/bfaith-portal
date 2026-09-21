/**
 * inventory-hourly.mjs — ロジザード在庫を毎時 Company DB に写す定期実行 (Company DB構想 08 §3.3。D2)
 *
 * 何をするか (本体は inventory/logizard.mjs):
 *   ① mirror_logizard_stock (Render の warehouse-mirror.db) を読み、前の世代と比べて変わった行だけを raw に書く (同じ世代なら skipped)
 *   ② 日付 (JST) が変わっていたら、前日までの未締めの日を締める (日次 2 表 → complete。取得が無い日は missing)
 *   ②' 締めた日どうしの差 (前日 → 当日・SKU 単位) を events.inventory_events に inferred で追記する (inventory/stock-diff.mjs。0022 が未適用の間は何もしない)
 *   ③ 締めた日には raw の整理 (30 日) と DB の大きさの記録
 *   → 08 §3.3 では ② ③ を夜間の再ロード (#1296) の step にする案だったが、毎時ジョブの「日付が変わった最初の回」に載せた
 *     (入口を増やさない = CLAUDE.md の規則。夜間の再ロードは商品・出品の写しで、在庫とは材料も失敗の仕方も別)
 *
 * いつ動くか: 毎時 :35 (mirror への push は毎時 :00 台 = miniPC の LogizardZaikoHourly 09〜18 時)。夜間は世代が変わらないので skipped
 *
 * 約束 (nightly.mjs と同じ流儀):
 *   - **Render の中でだけ動く** (lib/is-render.js の isRender())。miniPC も同じ server.js を動かすので、これが無いと二重実行になる
 *   - 材料 (warehouse-mirror.db / mirror_logizard_stock) が無ければ失敗として ping する (Render の中なのに材料が無い = 異常)
 *   - 単一飛行 (プロセス内)。前の回が終わっていなければ見送る。見送りが SKIP_ALERT_HOURS より長引いたら失敗として ping する
 *   - ping: 取り込んだ (success) / 日を締めた / 在庫の差を作った → ok。世代が同じで締める日も無い回 → **ping しない** (夜間の 14 回で partial を積まない)。
 *     失敗 → fail。台帳 = config/jobs-registry.mjs の company-db-inventory-hourly (dead-man: 09:35 JST + 猶予 3 時間 = 日中の最初の取込で満たす)
 *   - Dark Launch: env COMPANY_DB_INVENTORY_CRON_ENABLED が '1' / 'true' のときだけ起動する
 *
 * env:
 *   COMPANY_DB_INVENTORY_CRON_ENABLED  '1'|'true' で有効 (既定 OFF)
 *   COMPANY_DB_INVENTORY_CRON          cron 式 UTC (既定 '35 * * * *' = 毎時 35 分)
 *   COMPANY_DB_INVENTORY_SKIP_ALERT_H  見送りが何時間続いたら失敗として ping するか (既定 2)
 *   DATA_DIR / COMPANY_DB_URL          どちらか無ければ動かない (設定漏れとして失敗を ping する)
 *
 * 手で流す: Render Shell から `node -e "import('./apps/company-db/inventory-hourly.mjs').then(m => m.runInventoryHourly({ force: true }))"`
 *   (force = 単一飛行の見張りは共有されないので、cron が動いている時間帯 (:35 前後) は避ける)
 */
import fs from 'node:fs';
import path from 'node:path';
import cron from 'node-cron';
import { pingJob } from '../jobs-monitor/ping-local.js';
import { isRender } from '../../lib/is-render.js';
import { openPgClient, pgAdapter } from '../../scripts/company-db/migrate.mjs';
import { jstDateStr } from '../../lib/jst-date.js';
import { captureLogizardInventory, closeStockDays, maintainInventory, readMirrorLogizardStock, JOB_ID } from './inventory/logizard.mjs';
import { inferStockDiffs } from './inventory/stock-diff.mjs';

export { JOB_ID };
const DEFAULT_CRON = '35 * * * *';

function envInt(name, def, min, max) {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return def;
  const n = Number(raw);
  if (!Number.isInteger(n) || n < min || n > max) throw new Error(`env ${name} が不正です: "${raw}" (整数 ${min}〜${max})`);
  return n;
}

/** 実行中の 1 本 (プロセス内の単一飛行) */
const state = { current: null, last: null };
export const getInventoryHourlyState = () => ({ current: state.current, last: state.last });

/** ping の note・ログ用の 1 行 (180 字で切られる) */
export function summarize({ cap, closed, diff, maint }) {
  const parts = [];
  if (cap) {
    if (cap.status === 'success') parts.push(`世代 ${cap.generation.slice(0, 16)} 取込 +${cap.added} ~${cap.changed} -${cap.removed} (行 ${cap.seen}, ロケ +${cap.locationsAdded})`);
    else parts.push(`世代 ${cap.generation.slice(0, 16)} skipped`);
  }
  if (closed && closed.closed.length) {
    parts.push(`締め ${closed.closed.map((c) => `${c.day.slice(5)}:${c.status === 'complete' ? `ok(${c.skus}${c.badDates ? `,日付NG${c.badDates}` : ''})` : c.status}`).join(' ')}${closed.locked ? ' (別の取込が走っていて途中で見送り)' : closed.backlog ? ' (まだ残りあり)' : ''}`);
  } else if (closed && closed.locked) parts.push('締め見送り (別の取込が走っている)');
  if (diff && diff.days.length) {
    parts.push(`差 ${diff.days.map((d) => `${d.day.slice(5)}:${d.status === 'done' ? `${d.events}件${d.unresolvedChanged ? `(SKU不明${d.unresolvedChanged})` : ''}` : d.skipReason}`).join(' ')}${diff.locked ? ' (別の回が走っていて途中で見送り)' : diff.backlog ? ' (まだ残りあり)' : ''}`);
  }
  if (maint) parts.push(`整理 -${maint.purged} / DB ${Math.round(maint.dbBytes / 1048576)}MB`);
  return parts.join(' / ') || '何もなし';
}

/**
 * 1 回流す。戻り値 = { ok, skipped, note }
 * 🚨 例外は投げない (cron の中で投げても誰も拾わない)。失敗は ok=false と ping で表す
 * 差し替え (試験): readMirror / connect / ping / now / force (Render 以外でも動かす)
 */
export async function runInventoryHourly(opts = {}) {
  const log = opts.log || ((m) => console.log(`[company-db inventory] ${m}`));
  try {
    return await runInner({ ...opts, log });
  } catch (e) {
    const note = `想定外: ${e && e.message ? e.message : e}`;
    log(note);
    (opts.ping || pingJob)(JOB_ID, 'fail', note);
    return { ok: false, skipped: false, note };
  }
}

async function runInner({ log, ping = pingJob, readMirror = readMirrorLogizardStock, connect = openPgClient, now = () => new Date(), force = false, maxDays = 60,
  capture = captureLogizardInventory, close = closeStockDays, inferDiffs = inferStockDiffs, maintain = maintainInventory }) {
  if (!force && !isRender()) {
    // 🚨 miniPC でも env がそろえば動いてしまう。Render 以外では何も言わずに何もしない (ping もしない)
    log('Render の中ではないので動かさない');
    return { ok: true, skipped: true, note: 'not-render' };
  }
  const dataDir = process.env.DATA_DIR;
  const url = process.env.COMPANY_DB_URL;
  if (!dataDir || !url) {
    const note = 'DATA_DIR / COMPANY_DB_URL が無い (Render の env を確かめる)';
    log(note); ping(JOB_ID, 'fail', note);
    return { ok: false, skipped: false, note };
  }
  let alertHours;
  try { alertHours = envInt('COMPANY_DB_INVENTORY_SKIP_ALERT_H', 2, 1, 240); }
  catch (e) { log(e.message); ping(JOB_ID, 'fail', e.message); return { ok: false, skipped: false, note: e.message }; }

  if (state.current) {
    const hours = (now().getTime() - state.current.startedMs) / 3600000;
    const note = `前の回が終わっていないので見送った (${hours.toFixed(1)} 時間前から)`;
    log(note);
    if (hours >= alertHours) { ping(JOB_ID, 'fail', note); return { ok: false, skipped: true, note }; }
    return { ok: true, skipped: true, note };
  }
  const cur = { startedMs: now().getTime(), started_at: now().toISOString() };
  state.current = cur;
  try {
    if (!fs.existsSync(path.join(dataDir, 'warehouse-mirror.db'))) {
      const note = `材料が無い: ${path.join(dataDir, 'warehouse-mirror.db')} (Render の中なのに mirror が無い)`;
      log(note); ping(JOB_ID, 'fail', note);
      return { ok: false, skipped: false, note };
    }
    const mirror = await readMirror(dataDir);
    if (!mirror) {
      const note = '材料が無い: mirror_logizard_stock が無い・空 (miniPC の LogizardZaikoHourly が一度も届いていない)';
      log(note); ping(JOB_ID, 'fail', note);
      return { ok: false, skipped: false, note };
    }
    const client = await connect(url);
    const db = pgAdapter(client);
    let cap = null, closed = null, diff = null, maint = null;
    try {
      cap = await capture(db, { rows: mirror.rows, capturedAt: mirror.capturedAt, host: 'render', log });
      if (cap.status === 'skipped' && cap.reasonCode === 'locked') {
        // 🚨 別の取込が走っている = その世代をまだ見ていない。締めも整理もこの回は見送る (未取込の世代を待たずに日を確定しない。Codex R2 #1)
        const note = `別の取込が走っているので見送った (${cap.reason})`;
        log(note);
        return { ok: true, skipped: true, note };
      }
      closed = await close(db, { todayJst: jstDateStr(now()), maxDays, log });
      // 締めた日どうしの差 → 在庫のイベント (inferred)。まだ作っていない complete の日を古い順に (0022 が未適用なら何もしない)。
      // 🚨 「締めた回だけ」にしない: 前の回で差だけ失敗していたら、次の回が追いつく (印の無い日を毎回探す。小さい表 2 つの突き合わせ)
      // 🚨 差が失敗しても、この後の整理は走らせる (差は 1 日ごとの取引で、失敗した日は何も残っていない。あふれなど、やり直しても直らない日があると毎時ここで落ちる
      //    → 整理と容量の記録まで止めない。Codex #1396 R1)。失敗は握りつぶさない = この回の結果と ping は fail
      let diffError = null;
      try { diff = await inferDiffs(db, { maxDays, log }); }
      catch (e) { diffError = e; diff = { status: 'failed', days: [], backlog: true, locked: false }; }
      if (diff.status === 'not_migrated') log('在庫の差は見送り: 0022 (snapshots.stock_diff_days) が未適用');
      // 🚨 整理は締めが追いついているときだけ (未締めの日が残る間は、その復元材料 = 古い観測を消さない。Codex R1 #2)
      if (closed.closed.length && !closed.backlog) maint = await maintain(db, { host: 'render', note: `closed ${closed.closed.map((c) => c.day).join(',')}` });
      if (diffError) throw Object.assign(new Error(`在庫の差を作れない: ${diffError.message} (取込・締め・整理は済み: ${summarize({ cap, closed, diff: null, maint })})`), { code: diffError.code || 'STOCK_DIFF_FAILED' });
    } finally {
      try { await client.end(); } catch { /* 閉じられなくても結果は変わらない */ }
    }
    const note = summarize({ cap, closed, diff, maint });
    const didSomething = cap.status === 'success' || closed.closed.length > 0 || diff.days.length > 0;
    if (didSomething) { log(`成功: ${note}`); ping(JOB_ID, 'ok', note); return { ok: true, skipped: false, note }; }
    log(`変化なし: ${note}`);   // 世代が同じで締める日も無い = 夜間の通常。ping しない
    return { ok: true, skipped: true, note };
  } catch (e) {
    const note = `失敗: ${e.message}${e.code ? ` [${e.code}]` : ''}`;
    log(note); ping(JOB_ID, 'fail', note);
    return { ok: false, skipped: false, note };
  } finally {
    state.last = { ...cur, finished_at: now().toISOString() };
    state.current = null;
  }
}

/** server.js から呼ぶ。env で有効にしていなければ何もしない (Dark Launch) */
export function startCompanyDbInventoryHourlyCron() {
  if (!isRender()) {
    console.log('[company-db inventory] Render の中ではないので cron 起動せず (miniPC との二重実行を防ぐ)');
    return null;
  }
  const enabled = process.env.COMPANY_DB_INVENTORY_CRON_ENABLED;
  if (enabled !== '1' && enabled !== 'true') {
    console.log('[company-db inventory] COMPANY_DB_INVENTORY_CRON_ENABLED 未設定のため cron 起動せず (Dark Launch)');
    return null;
  }
  const expr = process.env.COMPANY_DB_INVENTORY_CRON || DEFAULT_CRON;
  if (!cron.validate(expr)) {
    console.error(`[company-db inventory] COMPANY_DB_INVENTORY_CRON が cron 式として不正: "${expr}" → 起動しない`);
    return null;
  }
  const task = cron.schedule(expr, () => {
    runInventoryHourly().catch((e) => console.error(`[company-db inventory] 想定外: ${e.message}`));
  }, { timezone: 'UTC' });
  console.log(`[company-db inventory] cron 起動 (${expr} UTC)`);
  return task;
}
