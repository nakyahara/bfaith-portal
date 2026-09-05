/**
 * 🚚 「いま入荷を取りに行く」 — 予定していない納品を iPad にすぐ出す (2026-09-05 中原さん)
 *
 * 通常は miniPC の定時 (08:40 / 11:45) がロジザードから CSV を出し、Render が30分おきに拾う。
 * 予定外の納品をロジザードの入荷受付に入れた直後は次の定時まで iPad に出ないので、
 * **画面のボタンで「ロジザードから出し直す → Drive → 取り込む」を1回分だけ人が起こせる**ようにする。
 *
 * ```
 * iPad「🚚 いま取りに行く」 → Render → miniPC /service-api/logizard/nyuka-refresh (ジョブ開始)
 *                                        ↓ 約30秒 (ログイン→検索→CSV出力→Drive転送)
 *                            Render が /service-api/jobs/:id を見て完了を待つ
 *                                        ↓
 *                            Drive の更新日時が動くのを待って取り込む → iPad の一覧が入れ替わる
 * ```
 *
 * 設計:
 *  - **HTTP は即返す**。全体で30〜60秒かかるので、押した瞬間に「取りに行っています」を返し、
 *    進捗は `/api/state` の `refresh` を5秒ポーリングで見る (iPad の送信は4秒で打ち切る作りのため)
 *  - **同時に1本だけ**。複数の iPad が押しても走るのは1回 (ロジザードへのログインを増やさない)
 *  - 取り込みは既存の `fetchAndImportFromDrive` をそのまま使う (fail-closed の判定も確認状態の
 *    引き継ぎも既存のまま。同じ日のうちは ✅ が消えない)
 *  - 🚨 miniPC の env が無い環境 (ローカル開発) では**はっきり失敗させる**。半分動いて
 *    「押したのに何も起きない」を作らない
 */
import { getDriveInfo, fetchAndImportFromDrive } from './drive-fetch.js';

const WAREHOUSE_URL = process.env.WAREHOUSE_URL || 'https://wh.bfaith-wh.uk';

/**
 * 時間の既定値。
 *  - job: miniPC のジョブを待つ上限 (実測 約30秒。ロック待ちが入ると延びる)
 *  - drive: rclone 転送後に Drive API から新しい世代が見えるまでの遅れを待つ上限
 *  - cooldown: 終わった直後の連打を止める (miniPC 側にも同じ 60 秒がある)
 * テストからは短い値を渡す (待ち時間の実測ではなく、遷移を確かめたいので)
 */
export const DEFAULT_TIMING = Object.freeze({
  // 🚨 miniPC 側の上限 (ロック待ち60秒 + 取得180秒 = 最大240秒) より**長く**取る。
  //    短いと、正常に走り続けている取得をこちらが先に「失敗」と表示して再試行を誘う
  jobTimeoutMs: 300_000, jobPollMs: 2_000,
  driveWaitMs: 30_000, drivePollMs: 2_500,
  cooldownMs: 60_000,
});

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const nowIso = () => new Date().toISOString();

/** 直近1回分の実行状態 (画面が見るのはこれだけ)。プロセス内メモリで足りる */
let _run = null;
let _lastFinishedAt = 0;

/** miniPC を呼ぶための資格情報が揃っているか (揃っていなければボタン自体を出さない) */
export function refreshConfigured() {
  return !!(process.env.WAREHOUSE_SERVICE_TOKEN && process.env.CF_ACCESS_CLIENT_ID && process.env.CF_ACCESS_CLIENT_SECRET);
}

function serviceHeaders() {
  return {
    'CF-Access-Client-Id': process.env.CF_ACCESS_CLIENT_ID || '',
    'CF-Access-Client-Secret': process.env.CF_ACCESS_CLIENT_SECRET || '',
    Authorization: `Bearer ${process.env.WAREHOUSE_SERVICE_TOKEN || ''}`,
    'Content-Type': 'application/json',
  };
}

/**
 * miniPC の service-api を叩く。CF Access のリダイレクトは「認証の構成がおかしい」なので
 * 追わずに落とす (ログイン画面の HTML を JSON として読もうとしない)
 */
async function callWarehouse(path, { method = 'GET', timeout = 20_000, fetchFn = fetch } = {}) {
  const res = await fetchFn(`${WAREHOUSE_URL}${path}`, {
    method, headers: serviceHeaders(), redirect: 'manual', signal: AbortSignal.timeout(timeout),
  });
  if (res.status === 302 || res.status === 303) throw new Error('miniPC の認証構成が異常です (CF Access)');
  const ct = res.headers.get('content-type') || '';
  const body = ct.includes('application/json') ? await res.json().catch(() => ({})) : null;
  if (!res.ok) {
    const msg = (body && (body.message || body.error)) || `HTTP ${res.status}`;
    const e = new Error(msg);
    e.status = res.status;
    e.code = body && body.error ? body.error : null;
    throw e;
  }
  if (!body) throw new Error(`miniPC の応答が JSON ではありません (HTTP ${res.status})`);
  return body;
}

/** 画面に出す現在の状態 */
export function refreshState() {
  if (!_run) return { state: 'idle', configured: refreshConfigured() };
  return { ..._run, configured: true };
}

function setRun(patch) {
  _run = { ..._run, ...patch, updatedAt: nowIso() };
}

/**
 * 取りに行って取り込む本体。**例外を投げない** (状態に書いて終わる)。
 */
async function runRefresh({ actor, fetchFn, drive, timing }) {
  const T = { ...DEFAULT_TIMING, ...(timing || {}) };
  try {
    setRun({ phase: 'requesting', message: 'ロジザードに取りに行っています' });
    // 出し直す前の Drive の世代を控える。キャッシュ越しだと自分が起こした更新を見落とす
    let before = null;
    try { before = await drive.getDriveInfo({ force: true }); } catch { /* 取れなくても続行 (取込側が判定する) */ }

    const start = await callWarehouse('/service-api/logizard/nyuka-refresh', { method: 'POST', fetchFn });
    const jobId = start.jobId;
    if (!jobId) throw new Error('miniPC がジョブIDを返しませんでした');

    // ① miniPC のジョブ完了を待つ。**先に1回聞いてから待つ** (すぐ終わるジョブを無駄に待たない)
    const until = Date.now() + T.jobTimeoutMs;
    let job = null;
    for (;;) {
      let r = null;
      try {
        r = await callWarehouse(`/service-api/jobs/${encodeURIComponent(jobId)}`, { fetchFn });
      } catch (e) {
        // 一時的な通信エラーで諦めない (取得自体は miniPC で走り続けている)
        if (Date.now() > until) throw e;
      }
      if (r) {
        job = r.job || null;
        if (!job) throw new Error('miniPC がジョブを見失いました');
        if (job.status !== 'running') break;
        if (job.progress && job.progress.message) setRun({ message: job.progress.message });
      }
      if (Date.now() > until) throw new Error('ロジザードからの取得が時間内に終わりませんでした');
      await sleep(T.jobPollMs);
    }
    if (job.status !== 'completed') {
      const e = new Error((job.error && job.error.message) || 'ロジザードからの取得に失敗しました');
      e.code = job.error && job.error.code;
      throw e;
    }

    // ② Drive に反映されるのを待つ (rclone 転送の直後は Drive API にまだ見えないことがある)
    setRun({ phase: 'importing', message: '取り込んでいます' });
    const driveUntil = Date.now() + T.driveWaitMs;
    let driveAdvanced = false;
    // 🚨「違う値になった」ではなく「**進んだ**」で見る。古いファイルに差し替わった (時刻が戻った) ものを
    //    「新しい世代が届いた」と読まない。読めない時刻は進んでいない扱い (fail-closed)
    const advanced = (a, b) => {
      const t1 = Date.parse(a || ''); const t2 = Date.parse(b || '');
      return Number.isFinite(t1) && Number.isFinite(t2) && t1 > t2;
    };
    if (before && before.modified_time) {
      for (;;) {
        let now2 = null;
        try { now2 = await drive.getDriveInfo({ force: true }); } catch { /* 次の周回で見る */ }
        if (now2 && advanced(now2.modified_time, before.modified_time)) { driveAdvanced = true; break; }
        if (Date.now() > driveUntil) break;
        await sleep(T.drivePollMs);
      }
    }
    /**
     * 🚨「取り直した結果が本当に届いたか」を確かめてからでないと、`duplicate_file` を
     *    「新しい入荷受付はありませんでした」と言ってはいけない。転送漏れ・Drive 障害・反映遅れでも
     *    同じ見た目になり、現場は「ロジザードに登録できていないのか」と誤解する。
     *  裏取りは2つ。どちらかが取れれば「確かめられた」とする:
     *    - Drive の更新日時が**進んだ**
     *    - miniPC が**今回の実行で CSV を書き直し** (csvWritten)、その中身が**前回と同じ** (csvSameContent)
     *      = 増えていないことが miniPC 側で確定している (rclone は中身が同じなら転送を省くので Drive が動かないのが正しい)。
     *      🚨 「前後で同じ」だけでは足りない — 古い CSV を残したまま終了コード0で終わった場合と区別できない
     */
    const res = job.result || {};
    const verified = driveAdvanced || (res.csvWritten === true && res.csvSameContent === true);

    // ③ 取り込む (fail-closed の判定・確認状態の引き継ぎは既存のまま)
    const r = await drive.fetchAndImportFromDrive({ actor, source: 'drive_retry' });
    if (r.ok) {
      // 新しいバッチができた = 新しい世代が確かに届いている (裏取りは不要)
      setRun({
        state: 'done', phase: 'done', finishedAt: nowIso(), ok: true, verified: true,
        message: `取り込みました (${r.slipCount}伝票 / ${r.rowCount}行)`,
        slipCount: r.slipCount, rowCount: r.rowCount, batchId: r.batch ? r.batch.id : null,
      });
    } else if (r.error === 'duplicate_file' && verified) {
      // ロジザード側に増えていなかった。失敗ではないので、そう言う
      setRun({
        state: 'done', phase: 'done', finishedAt: nowIso(), ok: true, unchanged: true, verified: true,
        message: '一覧に追加される新しい受付はありませんでした (一覧は最新です)',
      });
    } else if (r.error === 'duplicate_file') {
      // 取り直したのに新しい世代を確認できない = 転送漏れ / Drive 障害 / 反映遅れ。
      // 「新規なし」と言い切らず、確かめられなかったことをそのまま伝える
      setRun({
        state: 'failed', phase: 'done', finishedAt: nowIso(), ok: false, error: 'not_verified',
        message: 'ロジザードからは取り直しましたが、共有ドライブへの反映を確認できませんでした。'
          + '1〜2分おいてからもう一度押すか、管理画面の取込履歴を確認してください',
      });
    } else {
      setRun({
        state: 'failed', phase: 'done', finishedAt: nowIso(), ok: false,
        message: r.message || '取り込めませんでした', error: r.error || 'import_failed',
      });
    }
  } catch (e) {
    setRun({
      state: 'failed', phase: 'done', finishedAt: nowIso(), ok: false,
      error: e.code || 'error',
      message: e.message || '取りに行けませんでした',
    });
  } finally {
    _lastFinishedAt = Date.now();
  }
}

/**
 * ボタンから呼ぶ。**すぐ返る** (走らせて状態を返すだけ)。
 * @returns {{ok:boolean, error?:string, message?:string, run:object}}
 */
export function startRefresh({ actor = null, fetchFn = fetch, drive = null, timing = null } = {}) {
  if (!refreshConfigured()) {
    return { ok: false, error: 'not_configured',
      message: 'この環境から miniPC を呼べません (WAREHOUSE_SERVICE_TOKEN / CF_ACCESS_* が未設定)', run: refreshState() };
  }
  if (_run && _run.state === 'running') {
    return { ok: true, already: true, message: 'いま取りに行っています', run: refreshState() };
  }
  const T = { ...DEFAULT_TIMING, ...(timing || {}) };
  const wait = Math.ceil((T.cooldownMs - (Date.now() - _lastFinishedAt)) / 1000);
  if (_lastFinishedAt && wait > 0) {
    return { ok: false, error: 'cooldown',
      message: `さっき取りに行ったばかりです。${wait}秒あけてからもう一度押してください`, run: refreshState() };
  }
  _run = {
    state: 'running', phase: 'requesting', startedAt: nowIso(), updatedAt: nowIso(),
    by: actor, message: 'ロジザードに取りに行っています',
  };
  // 走らせっぱなしにする (呼び出し元は待たない)。中で例外は握って状態に落とす
  runRefresh({ actor, fetchFn, drive: drive || { getDriveInfo, fetchAndImportFromDrive }, timing })
    .catch(() => { /* runRefresh 内で処理済み */ });
  return { ok: true, run: refreshState() };
}

/** テスト用: 状態を初期化する */
export function _resetForTest() {
  _run = null;
  _lastFinishedAt = 0;
}

/** テスト用: 実行が終わるまで待つ */
export async function _waitIdleForTest(timeoutMs = 10_000) {
  const until = Date.now() + timeoutMs;
  while (_run && _run.state === 'running' && Date.now() < until) await sleep(10);
  return refreshState();
}
