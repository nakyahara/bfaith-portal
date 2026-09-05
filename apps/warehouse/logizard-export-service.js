/**
 * ロジザードCSVの**オンデマンド取得** (Render の画面ボタン → miniPC)
 *
 * 入荷受付チェック (iPad) は miniPC の定時タスク `Logizard-NyukaCSV` (08:40 / 11:45) が出した
 * CSV を Drive 経由で受け取っている。**予定していない納品が来た日**は次の定時まで iPad に出ないので、
 * 現場が「いま取りに行く」ボタンを押せるようにするための入口。
 *
 * 設計:
 *  - ロジザードは miniPC からしか触れない (Playwright + 認証情報)。**新しい取得を増やすのではなく、
 *    既に毎日動いている `auto-nyuka-csv.js` を人の操作で1回走らせるだけ** (取得先も条件も定時と同じ)
 *  - 実処理は子プロセス。サービスプロセスの中でブラウザを動かさない (rankcheck-service と同じ考え方)
 *  - 長い処理なので job-manager に載せ、呼び出し側は `/service-api/jobs/:jobId` で追う
 *  - 🚨 **jobs-monitor へ ping しない**。台帳 `logizard-nyuka-csv` の dead-man は「定時が動いているか」を
 *    見張るもので、人が押した成功で ok を打つと**朝の定時が壊れていても無音になる**
 *  - 🚨 **bat (`run-nyuka-csv-scheduled.bat`) は呼ばない**。あれは商品マスタの取得と ping まで含む定時用。
 *    ここは入荷受付CSVだけを走らせる
 *  - ロジザードのログインセッションは在庫CSV (毎時00分) / 値札CSV (8:30) と**同じロックを取り合う**。
 *    掴めない間は待ち、それでも空かなければ「いま別の取得中」として返す (定時 bat の10分待ちは画面には長すぎる)
 */
import { Router } from 'express';
import { spawn } from 'child_process';
import fs from 'fs';
import path from 'path';
import { createJob, getJob } from './job-manager.js';
import { okResponse, errorResponse } from './error-handler.js';

const router = Router();

/** ロジザード自動化の置き場 (git 管理外。PC ごとに違う可能性があるので env で差し替え可) */
const AUTOMATION_DIR = process.env.LOGIZARD_AUTOMATION_DIR || 'C:\\tools\\logizard-automation';
/** 入荷受付CSV (入荷状況照会[FA04_01] / 受付済 / 当日〜7日前) を出すスクリプト */
const SCRIPT = 'auto-nyuka-csv.js';
/** ロジザードのセッションロック (在庫CSV・値札CSV と共有) */
const LOCK_PATH = path.join(AUTOMATION_DIR, 'logs', 'logizard-session.lock');
/** 連打防止。ロジザードへのログインを短時間に何度も行わない */
const COOLDOWN_MS = 60_000;
/** ロックが空くのを待つ上限。超えたら「いま別の取得中」で諦める (画面を待たせ続けない) */
const LOCK_WAIT_MS = 90_000;
const LOCK_POLL_MS = 3_000;
/** 子プロセスの上限。実測は約30秒 (2026-09-05: 11:45:01 開始 → 11:45:31 Drive 反映) */
const RUN_TIMEOUT_MS = 5 * 60 * 1000;
/** ジョブに残す出力の上限 (エラーの手掛かりだけ。全文はスクリプト側のログにある) */
const MAX_OUTPUT_CHARS = 4000;

let _lastStartAt = 0;
let _runningJobId = null;

/** いま走っているジョブ (無ければ null)。プロセス再起動でジョブは消えるので getJob で確かめ直す */
function runningJob() {
  if (!_runningJobId) return null;
  const j = getJob(_runningJobId);
  if (!j || j.status !== 'running') { _runningJobId = null; return null; }
  return j;
}

/** ロックが誰かに握られているか (ファイルの有無だけを見る。stale 判定はスクリプト側が持つ) */
function lockHeld() {
  try { return fs.existsSync(LOCK_PATH); } catch { return false; }
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** 子プロセスの出力の末尾だけ残す (メモリを食わせない) */
function tail(s) {
  const t = String(s || '');
  return t.length <= MAX_OUTPUT_CHARS ? t : `…(前略)\n${t.slice(-MAX_OUTPUT_CHARS)}`;
}

/**
 * `node auto-nyuka-csv.js` を1回走らせる。
 * @returns {Promise<{code:number|null, output:string, timedOut:boolean}>}
 */
function runScript(scriptPath) {
  return new Promise((resolve, reject) => {
    let child;
    try {
      child = spawn(process.execPath, [scriptPath], {
        cwd: AUTOMATION_DIR,
        // ⭐スクリプト自身の loadEnv() が同じフォルダの .env を読んで process.env を**上書きする**ので、
        //   こちらの env が混ざってロジザードの接続先が変わることはない (定時タスクと同じ条件で走る)
        env: process.env,
        windowsHide: true,
      });
    } catch (e) {
      return reject(new Error(`取得スクリプトを起動できませんでした: ${e.message}`));
    }
    let out = '';
    const add = (b) => {
      out += b.toString('utf8');
      // 途中でも上限を超えたら前を捨てる (長時間走っても膨らませない)
      if (out.length > MAX_OUTPUT_CHARS * 4) out = out.slice(-MAX_OUTPUT_CHARS * 2);
    };
    child.stdout?.on('data', add);
    child.stderr?.on('data', add);
    let timedOut = false;
    const timer = setTimeout(() => {
      timedOut = true;
      try { child.kill(); } catch { /* 既に終わっている */ }
    }, RUN_TIMEOUT_MS);
    child.on('error', (e) => { clearTimeout(timer); reject(new Error(`取得スクリプトの起動に失敗: ${e.message}`)); });
    child.on('close', (code) => { clearTimeout(timer); resolve({ code, output: tail(out), timedOut }); });
  });
}

/**
 * POST /service-api/logizard/nyuka-refresh
 *   入荷受付CSVをいま取り直す。すぐ jobId を返し、進捗は /service-api/jobs/:jobId で見る。
 *   200 { jobId, status:'running' }          … 走り出した
 *   200 { jobId, status:'already_running' }  … 既に走っている (同じジョブを見てもらう)
 *   429 COOLDOWN                             … 直前に押されたばかり
 *   503 NOT_AVAILABLE                        … この PC にロジザード自動化が無い
 */
router.post('/nyuka-refresh', (req, res) => {
  const already = runningJob();
  if (already) {
    return okResponse(res, { jobId: already.jobId, status: 'already_running', message: '既に取得中です' });
  }
  const scriptPath = path.join(AUTOMATION_DIR, SCRIPT);
  if (!fs.existsSync(scriptPath)) {
    // fail-closed: 自動化が入っていない PC で「成功した」と言わない
    return errorResponse(res, {
      status: 503, error: 'NOT_AVAILABLE', requestId: req.requestId,
      message: `この PC にロジザード自動化がありません (${scriptPath})`,
    });
  }
  const now = Date.now();
  if (now - _lastStartAt < COOLDOWN_MS) {
    const wait = Math.ceil((COOLDOWN_MS - (now - _lastStartAt)) / 1000);
    return errorResponse(res, {
      status: 429, error: 'COOLDOWN', requestId: req.requestId,
      message: `さっき取得したばかりです。${wait}秒あけてからもう一度お試しください`,
    });
  }
  _lastStartAt = now;

  const job = createJob('logizard-nyuka-refresh', async (updateProgress) => {
    // ① ロジザードのセッションロックが空くのを待つ (在庫CSVは毎時00分に走る)
    const until = Date.now() + LOCK_WAIT_MS;
    let waited = false;
    while (lockHeld()) {
      if (Date.now() >= until) {
        const e = new Error('ロジザードの別の取得 (在庫CSV等) が終わりませんでした。1〜2分おいてからもう一度お試しください');
        e.code = 'LOCK_BUSY';
        throw e;
      }
      waited = true;
      updateProgress({ phase: 'waiting_lock', message: 'ロジザードの別の取得が終わるのを待っています' });
      await sleep(LOCK_POLL_MS);
    }
    updateProgress({ phase: 'running', message: 'ロジザードから入荷受付CSVを取得しています', waitedForLock: waited });

    // ② 取得 (ログイン → 検索 → CSV出力 → Drive へ転送まで、定時と同じスクリプト)
    const r = await runScript(scriptPath);
    if (r.timedOut) {
      const e = new Error(`取得が ${Math.round(RUN_TIMEOUT_MS / 1000)} 秒を超えたため中止しました`);
      e.code = 'TIMEOUT';
      throw e;
    }
    if (r.code !== 0) {
      // ロック待ちを抜けた直後に別プロセスが取った場合もここに来る (スクリプト側が即終了する)
      const lockish = /ロック|lock/i.test(r.output);
      const e = new Error(lockish
        ? 'ロジザードの別の取得と重なりました。少しおいてからもう一度お試しください'
        : `取得に失敗しました (終了コード ${r.code})`);
      e.code = lockish ? 'LOCK_BUSY' : 'SCRIPT_FAILED';
      e.output = r.output;
      throw e;
    }
    return { ok: true, output: r.output, waitedForLock: waited };
  });
  _runningJobId = job.jobId;
  okResponse(res, { jobId: job.jobId, status: 'running' }, 202);
});

/** GET /service-api/logizard/status — 押す前に「いま取得中か」を見たいとき (画面の初期表示用) */
router.get('/status', (req, res) => {
  const j = runningJob();
  okResponse(res, {
    available: fs.existsSync(path.join(AUTOMATION_DIR, SCRIPT)),
    running: !!j,
    jobId: j ? j.jobId : null,
    lockHeld: lockHeld(),
    cooldownRemainSec: Math.max(0, Math.ceil((COOLDOWN_MS - (Date.now() - _lastStartAt)) / 1000)),
  });
});

/** テスト用: 連打防止と実行中の記録を初期化する */
export function _resetForTest() {
  _lastStartAt = 0;
  _runningJobId = null;
}

export default router;
