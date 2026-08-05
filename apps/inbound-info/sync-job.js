/**
 * 入庫情報管理 — 日次自動処理 (新商品追加 → 入荷予定 nefuda.csv 取得 → 値札印刷用CSV → 入荷予定リストPDF)
 *
 * 1) mirror_products (単品・取扱中) のうち f_inbound_info 未登録のコードを INSERT
 * 2) Drive の nefuda.csv (入荷予定) を取得して f_inbound_schedule を full-replace
 * 3) 値札印刷用CSV (nefudaprint.csv) を作って nefuda.csv と同じフォルダに上書き保存
 *    (旧 GAS processNefudaCsv の置き換え。入数は f_inbound_info が正 — nefuda-print.js 参照)
 * 4) 入荷予定リスト (CSV順) の PDF を作って nefuda.csv と同じフォルダに上書き保存
 * 各段は独立の try/catch で、前段の失敗が後段を巻き込まない (ただし 4 は 2 の結果を使うので
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
import { syncNewProducts, getJobSettings, parseHhmm, savePdfState } from './db.js';
import { refreshNefudaSchedule } from './nefuda-fetch.js';
import { exportSchedulePdfToDrive, PDF_FILENAME } from './schedule-pdf.js';
import { pingJob } from '../jobs-monitor/ping-local.js';

// 既定 ON なので、止めたい意図の書き方 (false / 0 / off / no) を全部拾う (Codex R4 Medium)
const OFF = new Set(['false', '0', 'off', 'no']);
// 非Render環境であえて動かす意図の書き方 (既定は Render のみ)
const FORCE_ON = new Set(['true', '1', 'on', 'yes']);

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

// export はテスト (scripts/test-inbound-info.mjs) 用。cron からは下の schedule 経由で呼ぶ
// 戻り値 { ok, note } は dead-man 監視の ping 用 (runDailyJobWithPing が使う)。
// ok の基準は「入荷予定 CSV を取得できたか」— 現場が使う値札CSV/PDFの前提になるステップ。
export async function runDailyJob() {
  const notes = [];
  try {
    const r = syncNewProducts();
    if (!r.ok) {
      console.warn(`[inbound-info] sync skipped: ${r.error}`);
      notes.push(`新商品skip(${r.error})`);
    } else {
      console.log(`[inbound-info] sync done: inserted=${r.inserted} (mirror_products=${r.mirror_products})`);
      notes.push(`新商品=${r.inserted}`);
    }
  } catch (e) {
    console.error('[inbound-info] sync failed:', e.message);
    notes.push(`新商品失敗(${e.message})`);
  }
  // ②③ 入荷予定 (nefuda.csv) の取得 + 値札印刷用CSV (nefudaprint.csv) の出力。
  // ③ は ② と同じダウンロード結果から同一ロック内で作られる (nefuda-fetch.js)。
  // この結果で ④ を出すかどうかが変わる。
  let csvError = null;
  try {
    const s = await refreshNefudaSchedule('cron');
    // stale_file = 別経路がより新しいCSVを反映済み = DBの中身は最新なので PDF は出してよい
    if (!s.ok) {
      console.warn(`[inbound-info] nefuda skipped: ${s.error} (反映済みの方が新しい)`);
      notes.push(`nefuda skip(${s.error})`);
    } else {
      console.log(`[inbound-info] nefuda done: rows=${s.schedule_rows} added_to_master=${s.added_to_master}`
        + (s.not_in_master.length ? ` not_in_master=${s.not_in_master.join(',')}` : ''));
      notes.push(`nefuda=${s.schedule_rows}行`);
    }
    const np = s.nefuda_print;
    if (np?.skipped) console.log(`[inbound-info] nefudaprint skipped (${np.skipped})`);
    else if (np?.ok) console.log(`[inbound-info] nefudaprint ${np.action}: ${np.filename}`
      + ` rows=${np.rows} missing_irisu=${np.missing_irisu}`);
    else if (np) console.error(`[inbound-info] nefudaprint failed: ${np.error}`);
  } catch (e) {
    csvError = e.message;
    console.error('[inbound-info] nefuda fetch failed:', e.message);
    // 値札CSVの「更新していない」記録は refreshNefudaSchedule 側で残している
  }

  // ④ 入荷予定リストPDF。
  // ⚠ CSV取得が失敗した日は **Drive のPDFを上書きしない** (Codex pdf-R1 High)。
  //   前回スナップショットで作った古いPDFで上書きすると、現場は「今日の入荷予定」として
  //   古い紙を刷ってしまい、しかも保存状態が「成功」に見えて失敗を見逃す。
  //   代わりに失敗として記録し、画面に理由を出す (手動ボタンなら意図的に出せる)。
  if (!getJobSettings().pdf_enabled) {
    console.log('[inbound-info] pdf export skipped (画面設定で無効)');
    notes.push('PDF無効');
    return { ok: !csvError, note: notes.join(' ') };
  }
  if (csvError) {
    const msg = `入荷予定(nefuda.csv)の取得に失敗したため、PDFは更新していません (Drive上のPDFは前回のまま): ${csvError}`;
    savePdfState({ ok: false, error: msg, filename: PDF_FILENAME(), user: 'cron' });
    console.warn(`[inbound-info] pdf export skipped: ${msg}`);
    return { ok: false, note: notes.join(' ') };
  }
  try {
    const p = await exportSchedulePdfToDrive('cron');
    console.log(`[inbound-info] pdf ${p.action}: ${p.filename} rows=${p.rows} bytes=${p.bytes}`);
    notes.push(`PDF=${p.rows}行`);
  } catch (e) {
    console.error('[inbound-info] pdf export failed:', e.message);
    notes.push(`PDF失敗(${e.message})`);
    // PDF は現場が印刷する成果物なので、ここで失敗した日は ok にしない
    return { ok: false, note: notes.join(' ') };
  }
  return { ok: true, note: notes.join(' ') };
}

/** cron から呼ぶ入口。本体の結果を dead-man 監視へ報告する (jobs-registry: inbound-info-daily) */
async function runDailyJobWithPing() {
  try {
    const r = await runDailyJob();
    pingJob('inbound-info-daily', r?.ok ? 'ok' : 'fail', r?.note);
  } catch (e) {
    // runDailyJob は内部で握り潰す作りだが、想定外の例外でも ping だけは残す
    console.error('[inbound-info] daily job failed:', e.message);
    pingJob('inbound-info-daily', 'fail', e.message);
  }
}

let task = null;

/**
 * cron を (再)登録する。起動時と、画面で時刻を変更した時に呼ぶ。
 * @returns {{started:boolean, expr:string|null, source:string|null, reason?:string}}
 */
export function applyInboundInfoSchedule() {
  const disposeOld = () => {
    if (!task) return;
    // stop() だけだとタスクが登録簿に残り得るので destroy() で完全に破棄する
    // (設定保存を繰り返してもリスナー/登録が積み上がらない — Codex pdf-R1 Medium)
    try {
      if (typeof task.destroy === 'function') task.destroy();
      else task.stop();
    } catch (e) {
      console.error('[inbound-info] 旧cronの破棄に失敗:', e.message);
    }
    task = null;
  };

  if (isDisabled()) {
    disposeOld();
    console.log('[inbound-info] cron disabled (INBOUND_INFO_SYNC_ENABLED)');
    return { started: false, expr: null, source: null, reason: 'disabled_by_env' };
  }
  // ⚠ Render 限定。miniPC も同じ server.js を動かすため、無条件だと同じ日次処理
  //   (nefuda.csv 取得 → 値札CSV → 入荷予定PDF を Drive へ) が2箇所から走る。
  //   miniPC 側は mirror_products も持たないので、書き込む内容も本番と揃わない。
  //   意図的に miniPC で動かす場合だけ INBOUND_INFO_SYNC_ENABLED=true を明示する。
  if (!process.env.RENDER && !FORCE_ON.has(String(process.env.INBOUND_INFO_SYNC_ENABLED ?? '').trim().toLowerCase())) {
    disposeOld();
    console.log('[inbound-info] cron skipped (非Render環境。動かすなら INBOUND_INFO_SYNC_ENABLED=true)');
    return { started: false, expr: null, source: null, reason: 'not_render' };
  }
  // 先に式を検証してから旧タスクを破棄する。env に不正な cron 式が入っていた場合に、
  // 稼働中の予約まで失って「何も動かない」状態になるのを避ける (Codex pdf-R1 Medium)
  const { expr, source, time } = effectiveSchedule();
  if (!cron.validate(expr)) {
    console.error(`[inbound-info] invalid cron expr: ${expr} — 既存の予約を維持します`);
    return { started: !!task, expr, source, reason: 'invalid_expr' };
  }
  // 新タスクの生成に成功してから旧タスクを破棄する。逆順だと schedule() が例外を投げた時に
  // 「予約が1つも無い」状態で残る (Codex pdf-R2 Low)。
  // timezone を明示 (未指定だとプロセスのローカル TZ 依存になり、実行環境が変わると
  // ミラー同期完了前に走り得る)
  let next;
  try {
    next = cron.schedule(expr, runDailyJobWithPing, { timezone: 'Asia/Tokyo' });
  } catch (e) {
    console.error(`[inbound-info] cron の登録に失敗 (${expr}): ${e.message} — 既存の予約を維持します`);
    return { started: !!task, expr, source, reason: 'schedule_failed' };
  }
  disposeOld();
  task = next;
  console.log(`[inbound-info] cron started (${expr} JST${time ? ` = ${time}` : ''}, source=${source})`);
  return { started: true, expr, source };
}

// 既存の呼び出し名 (server.js) を維持
export function startInboundInfoCron() {
  return applyInboundInfoSchedule();
}
