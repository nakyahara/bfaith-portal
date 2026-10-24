/**
 * lib-fetch-status.mjs — モールCSV自動取得 (fetch-all.mjs) の「最後の回の結果」を 1 つのファイルに残す・読む (2026-10-24)
 *
 * なぜ要るか (2026-10-23 に調べた): Yahoo・au PAY・Qoo10 のログインが切れて、取得スクリプトが毎朝 exit 3 で終わっていた。
 *   exit 3 (手で直すもの) は fetch-all が再試行せず、子が GChat に 1 回知らせるだけ。daily-sync の取込は「置かれたファイル 0 件 = 正常」で ✅ になり、
 *   毎朝のまとめでは何週間も見えなかった (データ: Yahoo統計 10/13・au PAY分析 10/9・Qoo10分析 10/3 で止まっていた)。
 * 決め:
 *   - fetch-all は回の終わりに、モールごとの終了コードをこのファイルに書く (別名で書いてから名前を替える = 書きかけを読ませない)。
 *     🚨 書けなくても fetch-all の終了コード・通知は変えない (残すのは「見える化」のためだけ)
 *   - 読むのは daily-sync の「データの新しさ」(apps/warehouse/monitor-data-freshness.js)。exit 3 を「ログイン切れなど手で直すもの」として毎朝の要約に出す
 *   - 場所 = scripts/mall-csv-fetcher/logs/fetch-all-last.json (logs/ は git の外)。試験は関数に file を渡す (このファイルは環境変数を読まない = lib-env.mjs を要らない)
 */
import fs from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
/** 置き場所 (env では変えない = このファイルは環境変数を読まない。試験は関数に file を渡す) */
export const FETCH_STATUS_FILE = join(__dirname, 'logs', 'fetch-all-last.json');

/** 子の終了の意味 (fetch-all.mjs の「子スクリプトの exit code 契約」と同じ) */
export function describeFetchExit(r) {
  if (!r) return '記録なし';
  if (r.code === 0) return '成功';
  if (r.timedOut) return 'タイムアウト';
  if (r.code === 3) return 'ログイン切れなど手で直すもの (exit 3・再試行しない)';
  if (r.code === 2) return 'env 不備 (exit 2)';
  if (r.code === 1) return '取得の失敗 (exit 1・再試行でも直らなかった)';
  if (r.code === null || r.code === undefined) return `強制終了 (${r.signal || 'kill'})`;
  return `exit ${r.code}`;
}

/** 書く中身 (純粋関数)。results = fetch-all の最終結果 (再試行の後) */
export function buildFetchStatus(results, { only = [], now = new Date() } = {}) {
  return {
    version: 1,
    finished_at: now.toISOString(),
    only: only.length ? [...only] : null,
    results: (results || []).map((r) => ({ mall: r.mall, code: r.code ?? null, signal: r.signal || null, timedOut: !!r.timedOut, secs: r.secs ?? null })),
  };
}

/** 書く (別名で書いてから名前を替える)。@returns {{ ok: boolean, file: string, error: string|null }} */
export function writeFetchStatus(status, file = FETCH_STATUS_FILE) {
  const tmp = `${file}.${process.pid}.tmp`;
  try {
    fs.mkdirSync(dirname(file), { recursive: true });
    fs.writeFileSync(tmp, JSON.stringify(status, null, 2));
    fs.renameSync(tmp, file);
    return { ok: true, file, error: null };
  } catch (e) {
    try { fs.rmSync(tmp, { force: true }); } catch { /* */ }
    return { ok: false, file, error: String(e && e.message).slice(0, 200) };
  }
}

/** 読む。無い = { status: null, problem: 'missing' } / 読めない・形が違う = { status: null, problem: 'unreadable: …' } */
export function readFetchStatus(file = FETCH_STATUS_FILE) {
  if (!fs.existsSync(file)) return { status: null, problem: 'missing' };
  try {
    const j = JSON.parse(fs.readFileSync(file, 'utf8'));
    if (!j || typeof j.finished_at !== 'string' || !Array.isArray(j.results)) return { status: null, problem: 'unreadable: 形が違う' };
    return { status: j, problem: null };
  } catch (e) {
    return { status: null, problem: `unreadable: ${String(e && e.message).slice(0, 120)}` };
  }
}
