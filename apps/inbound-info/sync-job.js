/**
 * 入庫情報管理 — 日次自動処理 (新商品追加 → 入荷予定 nefuda.csv 取得 → 入荷予定リストPDFをDrive保存)
 *
 * 1) mirror_products (単品・取扱中) のうち f_inbound_info 未登録のコードを INSERT
 * 2) Drive の nefuda.csv (入荷予定) を取得して f_inbound_schedule を full-replace
 * 3) 入荷予定リスト (CSV順) の PDF を作って nefuda.csv と同じフォルダに上書き保存
 * 各段は独立の try/catch で、前段の失敗が後段を巻き込まない (ただし 3 は 2 の結果を使うので
 * 2 が失敗した場合は「前回のスナップショット」で PDF を作る = 中身が古いだけで壊れない)。
 *
 * 実行時刻: 画面 (入庫情報管理 → ⚙️自動実行) で設定した時刻 (既定 09:00 JST)。
 *   設定変更時に applyInboundInfoSchedule() が呼ばれて cron を張り替える。
 *   ミラー同期 (miniPC daily-sync 07:00 JST 起点) の完了後になる時刻にすること。
 *
 * 環境変数:
 *   INBOUND_INFO_SYNC_ENABLED=false … 自動実行を止める (既定は有効。false/0/off/no)
 *   INBOUND_INFO_SYNC_CRON          … cron 式で直接指定する上級者向け上書き (JST 基準)。
 *                                     設定されている場合、画面の時刻設定より優先される
 *   INBOUND_NEFUDA_FOLDER_ID / INBOUND_NEFUDA_FILE … nefuda.csv の場所 (nefuda-fetch.js 参照)
 *   INBOUND_SCHEDULE_PDF_FILE       … 保存するPDFのファイル名 (既定 入荷予定リスト.pdf)
 *
 * 自動実行を止めている間も UI の「新商品を今すぐ取込」「最新の入荷予定を取得」
 * 「今すぐPDFを作成してDriveに保存」で同じ処理を手動実行できる。
 */
import cron from 'node-cron';
import { syncNewProducts, getJobSettings, parseHhmm } from './db.js';
import { refreshNefudaSchedule } from './nefuda-fetch.js';
import { exportSchedulePdfToDrive } from './schedule-pdf.js';

// 既定 ON なので、止めたい意図の書き方 (false / 0 / off / no) を全部拾う (Codex R4 Medium)
const OFF = new Set(['false', '0', 'off', 'no']);

function isDisabled() {
  return OFF.has(String(process.env.INBOUND_INFO_SYNC_ENABLED ?? '').trim().toLowerCase());
}

/** 実効の cron 式と表示用の時刻。env 上書きがあればそれを使う */
export function effectiveSchedule() {
  const envExpr = (process.env.INBOUND_INFO_SYNC_CRON || '').trim();
  if (envExpr) return { expr: envExpr, source: 'env', time: null };
  const s = getJobSettings();
  const t = parseHhmm(s.time) || parseHhmm('09:00');
  return { expr: `${t.minute} ${t.hour} * * *`, source: 'settings', time: t.text };
}

async function runDailyJob() {
  try {
    const r = syncNewProducts();
    if (!r.ok) console.warn(`[inbound-info] sync skipped: ${r.error}`);
    else console.log(`[inbound-info] sync done: inserted=${r.inserted} (mirror_products=${r.mirror_products})`);
  } catch (e) {
    console.error('[inbound-info] sync failed:', e.message);
  }
  try {
    const s = await refreshNefudaSchedule('cron');
    if (!s.ok) console.warn(`[inbound-info] nefuda skipped: ${s.error} (反映済みの方が新しい)`);
    else console.log(`[inbound-info] nefuda done: rows=${s.schedule_rows} added_to_master=${s.added_to_master}`
      + (s.not_in_master.length ? ` not_in_master=${s.not_in_master.join(',')}` : ''));
  } catch (e) {
    console.error('[inbound-info] nefuda fetch failed:', e.message);
  }
  // PDF は CSV 取得の「その後」。取得が失敗しても前回スナップショットで出す (中身が古いだけ)
  if (getJobSettings().pdf_enabled) {
    try {
      const p = await exportSchedulePdfToDrive('cron');
      console.log(`[inbound-info] pdf ${p.action}: ${p.filename} rows=${p.rows} bytes=${p.bytes}`);
    } catch (e) {
      console.error('[inbound-info] pdf export failed:', e.message);
    }
  } else {
    console.log('[inbound-info] pdf export skipped (画面設定で無効)');
  }
}

let task = null;

/**
 * cron を (再)登録する。起動時と、画面で時刻を変更した時に呼ぶ。
 * @returns {{started:boolean, expr:string|null, source:string|null, reason?:string}}
 */
export function applyInboundInfoSchedule() {
  if (task) {
    task.stop();
    task = null;
  }
  if (isDisabled()) {
    console.log('[inbound-info] cron disabled (INBOUND_INFO_SYNC_ENABLED)');
    return { started: false, expr: null, source: null, reason: 'disabled_by_env' };
  }
  const { expr, source, time } = effectiveSchedule();
  if (!cron.validate(expr)) {
    console.error(`[inbound-info] invalid cron expr: ${expr} — cron not started`);
    return { started: false, expr, source, reason: 'invalid_expr' };
  }
  // timezone を明示 (未指定だとプロセスのローカル TZ 依存になり、実行環境が変わると
  // ミラー同期完了前に走り得る)
  task = cron.schedule(expr, runDailyJob, { timezone: 'Asia/Tokyo' });
  console.log(`[inbound-info] cron started (${expr} JST${time ? ` = ${time}` : ''}, source=${source})`);
  return { started: true, expr, source };
}

// 既存の呼び出し名 (server.js) を維持
export function startInboundInfoCron() {
  return applyInboundInfoSchedule();
}
