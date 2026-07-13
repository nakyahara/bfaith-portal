/**
 * fetch-all.mjs — モール横断 CSV自動取得ランナー (mall-csv-fetcher)
 *
 * Task Scheduler はこれを起動する (モール個別スクリプトを直接登録しない)。
 * 各モールの取得スクリプトを子プロセスで順次実行し、**失敗しても次のモールへ続行**する
 * (1モールの障害で他モールのデータ取得を道連れにしない)。
 *
 * 通知の分担:
 *   - 取得スクリプト自身: 業務エラー (FORM_VERIFY/2FA/DL失敗等) の詳細を GChat 通知 (exit 1)
 *   - このランナー: 子が通知できない異常終了のみ通知 — env不備(exit 2)/クラッシュ/timeout/signal
 *     (exit 1 は子が通知済みなので二重通知しない。ランナーはログに記録のみ)
 *
 * 将来のモール追加: FETCHERS に1エントリ追加するだけ (例: Yahoo は 2FA検証が前提)。
 *
 * 実行: node scripts/mall-csv-fetcher/fetch-all.mjs
 *   env: MALL_FETCH_ONLY=rakuten でモール絞り込み (カンマ区切り)
 *
 * exit code: 0=全モール成功 / 1=いずれか失敗 (全モール実行は完了)
 */
import fs from 'node:fs';
import { spawnSync } from 'node:child_process';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { initRunLog, sendGChat, buildErrorReport, LOG_DIR } from './lib-notify.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = join(__dirname, '..', '..');

// ─── 多重起動ロック (Task Scheduler + 手動実行の同時起動で同一ブラウザプロファイルを
// 2プロセスが触るとプロファイル破損/2FA誤検知の恐れ — Codex R1 Medium) ───
const LOCK_PATH = join(LOG_DIR, 'fetch-all.lock');
const LOCK_STALE_MS = 3 * 60 * 60 * 1000; // 全モール合計timeoutより長め。超過は前回クラッシュの残骸とみなす
function acquireLock() {
  fs.mkdirSync(LOG_DIR, { recursive: true });
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      fs.writeFileSync(LOCK_PATH, `pid=${process.pid} at=${new Date().toISOString()}`, { flag: 'wx' });
      return true;
    } catch (e) {
      if (e.code !== 'EEXIST') throw e;
      const age = Date.now() - fs.statSync(LOCK_PATH).mtimeMs;
      if (age < LOCK_STALE_MS) return false; // 実行中の別プロセスあり
      console.warn(`[lock] stale lock (${Math.round(age / 60000)}分前) を破棄して続行 (前回クラッシュの残骸とみなす)`);
      try { fs.unlinkSync(LOCK_PATH); } catch { /* 競合で消えていたら次のwxで決着 */ }
    }
  }
  return false;
}
function releaseLock() { try { fs.unlinkSync(LOCK_PATH); } catch { /* noop */ } }

// ─── 古いローカル出力の自動掃除 (logs/ と downloads/ の30日超。放置disk full事故の教訓) ───
// incoming/ と processed/ は取込の正規経路・原本バックアップなので触らない
function cleanupOldFiles() {
  const KEEP_MS = 30 * 24 * 3600 * 1000;
  const targets = [
    { dir: LOG_DIR, pattern: /\.log$/ },
    { dir: join(__dirname, 'downloads'), pattern: /^(rpp|rdata|yahoo|aupay)_/ },
  ];
  for (const t of targets) {
    let removed = 0;
    try {
      for (const name of fs.readdirSync(t.dir)) {
        if (!t.pattern.test(name)) continue;
        const p = join(t.dir, name);
        try {
          if (Date.now() - fs.statSync(p).mtimeMs > KEEP_MS) { fs.unlinkSync(p); removed++; }
        } catch { /* 個別失敗は無視 */ }
      }
    } catch { continue; } // dir無しは初回など正常
    if (removed > 0) console.log(`[cleanup] ${t.dir}: 30日超の ${removed} ファイルを削除`);
  }
}

// ─── モール取得スクリプト一覧 (追加はここに1行) ───
const DEFAULT_FETCHERS = [
  {
    mall: 'rakuten',
    script: join(__dirname, 'rakuten-rpp-download.mjs'),
    timeoutMs: 40 * 60 * 1000, // 履歴生成ポーリング最大15分×2レポートに余裕
  },
  {
    mall: 'rakuten-data', // RMSデータ分析 (商品分析SKU日次+店舗日次)。同期DLなので短め
    script: join(__dirname, 'rakuten-data-download.mjs'),
    timeoutMs: 20 * 60 * 1000,
  },
  {
    mall: 'yahoo', // ストクリ統計6種 (mall-csv-fetcher P1-Y)。全て同期DLなので短め
    script: join(__dirname, 'yahoo-data-download.mjs'),
    timeoutMs: 20 * 60 * 1000,
  },
  {
    mall: 'aupay', // WOW!マネージャー分析CSV (mall-csv-fetcher P1-A)。非同期ジョブのポーリング最大15分に余裕
    script: join(__dirname, 'aupay-data-download.mjs'),
    timeoutMs: 30 * 60 * 1000,
  },
];
// テスト用オーバーライド: MALL_FETCHERS_JSON='[{"mall":"x","script":"...","timeoutMs":1000}]'
const FETCHERS = process.env.MALL_FETCHERS_JSON
  ? JSON.parse(process.env.MALL_FETCHERS_JSON)
  : DEFAULT_FETCHERS;

async function main() {
  const runLog = initRunLog('fetch-all');
  if (!acquireLock()) {
    // 別プロセス実行中のスキップは異常ではない (Task Scheduler二重発火/手動実行との重複)
    console.log('[lock] 別の fetch-all が実行中のためスキップ (logs/fetch-all.lock)');
    return; // exitCode 0 (自然終了)
  }
  process.on('exit', releaseLock); // 正常/異常どちらの終了でもロック解放
  cleanupOldFiles();
  const only = (process.env.MALL_FETCH_ONLY || '').split(',').map((s) => s.trim()).filter(Boolean);
  const targets = only.length ? FETCHERS.filter((f) => only.includes(f.mall)) : FETCHERS;
  if (targets.length === 0) {
    console.error(`FATAL: 対象モールなし (MALL_FETCH_ONLY=${process.env.MALL_FETCH_ONLY})`);
    process.exitCode = 2;
    return;
  }

  console.log(`=== モールCSV自動取得 一括実行 (${targets.map((t) => t.mall).join(', ')}) ===`);
  const results = [];
  for (const f of targets) {
    console.log(`\n>>> ${f.mall}: ${f.script}`);
    const started = Date.now();
    // stdio inherit: 子の出力はコンソールへ (子自身が logs/ に tee 保存する)。
    // shell:false (feedback_execfile_vs_execsync: cmd.exe 経由の孫プロセス残留を避ける)
    const r = spawnSync(process.execPath, [f.script], {
      cwd: REPO_ROOT,
      stdio: 'inherit',
      shell: false,
      timeout: f.timeoutMs,
      killSignal: 'SIGKILL',
      env: process.env,
    });
    const secs = Math.round((Date.now() - started) / 1000);
    const outcome = {
      mall: f.mall,
      code: r.status,       // null = signal/timeout kill
      signal: r.signal || null,
      spawnError: r.error ? r.error.message : null,
      timedOut: !!(r.error && r.error.code === 'ETIMEDOUT'),
      secs,
    };
    results.push(outcome);
    console.log(`<<< ${f.mall}: exit=${outcome.code} signal=${outcome.signal || '-'} ${outcome.timedOut ? 'TIMEOUT ' : ''}(${secs}s)`);
    // 失敗しても続行 — 次のモールへ
  }

  const failed = results.filter((r) => r.code !== 0);
  console.log(`\n=== summary: ${results.map((r) => `${r.mall}=${r.code === 0 ? 'ok' : (r.timedOut ? 'timeout' : `exit${r.code ?? 'kill'}`)}`).join(' / ')} ===`);

  // 子が自力通知できないケースだけランナーが通知 (exit 1=業務エラー / exit 2=env不備 は
  // 子が詳細通知済み → 二重通知しない。実機 2026-07-09 で exit2 の二重通知を確認して除外追加)
  const unreported = failed.filter((r) => r.code !== 1 && r.code !== 2);
  if (unreported.length > 0) {
    await sendGChat(buildErrorReport({
      mall: unreported.map((r) => r.mall).join(','),
      outcomes: results.map((r) => ({ spec: r.mall, ym: '-', status: r.code === 0 ? 'ok' : (r.timedOut ? 'timeout' : `exit${r.code ?? 'kill'}`) })),
      failures: unreported.map((r) => ({
        reportType: `${r.mall} (取得スクリプト自体の異常終了)`,
        error: r.spawnError ? `spawn失敗: ${r.spawnError}`
          : r.timedOut ? `タイムアウト (${Math.round(r.secs / 60)}分)。ブラウザ/履歴ポーリングのハング疑い`
          : r.signal ? `signal ${r.signal} で強制終了`
          : `exit ${r.code} (exit2=env不備: scripts/mall-csv-fetcher/.env を確認)`,
      })),
      logPath: runLog.logPath,
      repro: `node scripts/mall-csv-fetcher/fetch-all.mjs (絞り込み: MALL_FETCH_ONLY=${unreported.map((r) => r.mall).join(',')})`,
    }), 'fetch-all');
  }

  // fetch (undici keep-alive) 直後の process.exit() は Windows で libuv assertion crash を
  // 起こし終了コードが化ける (実機 2026-07-09: exit2 が 0xC0000409 に化けた) → exitCode で自然終了
  process.exitCode = failed.length > 0 ? 1 : 0;
}

main().catch(async (e) => {
  console.error('[fetch-all FATAL]', e.stack || e.message);
  await sendGChat(`⚠️ *モールCSV取得ランナー自体が異常終了*\n${String(e.message).slice(0, 500)}`, 'fetch-all');
  process.exitCode = 1;
});
