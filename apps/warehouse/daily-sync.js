/**
 * daily-sync.js — 日次データ同期 + Google Chat通知
 *
 * 毎朝タスクスケジューラから実行。
 * SP-API（Amazon 7日分）と楽天RMS API（7日分）のデータを取得し、
 * 結果をGoogle Chatに通知する。
 */
import 'dotenv/config';
import { execFileSync } from 'child_process';
import crypto from 'node:crypto';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { isLibuvTransientCrash } from '../../lib/libuv-transient-crash.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PROJECT_DIR = path.resolve(__dirname, '..', '..');

// retry-failed-jobs.js が読む state ファイル。リトライ対象失敗ジョブをここに記録する
const RETRY_STATE_FILE = path.join(PROJECT_DIR, 'data', 'daily-sync-retry-state.json');
// 実行中マーカー兼多重起動ガード (構造監査 H-3)。通知に到達する全終了経路で削除し、
// プロセス abort (libuv assertion 等、JS の catch に落ちない死に方) の時だけ残る。
// 残骸は ①翌朝の起動時に「前回異常終了」として通知 ②bat 側の notify-crash.js が即時通知、の2段で拾う。
const LOCK_FILE = path.join(PROJECT_DIR, 'data', 'daily-sync.lock.json');

// この run が取得した lock の run_id。releaseLock は自分の lock だけ削除する
// (stale 上書き後に旧 run が完走して新 run の lock を消す事故の防止 — Codex Medium #1)
let myLockRunId = null;

// lock 削除の共通プロトコル (Codex R4): read→compare→unlink は比較後に差し替えられる TOCTOU があるため、
// ①固有 claim パスへ rename (原子的な排他取得) ②claim の中身を検証 ③自分のものなら削除、
// 他人の生存 lock なら返却 (返却先が塞がっていれば claim を破棄) — どの分岐でも他 run の lock を消さない。
function claimAndDeleteLock(matchFn) {
  const claim = `${LOCK_FILE}.claim-${process.pid}-${crypto.randomUUID()}`;
  try { fs.renameSync(LOCK_FILE, claim); } catch { return 'gone'; }
  let raw = null;
  try { raw = fs.readFileSync(claim, 'utf-8'); } catch { /* 読めない claim は下で破棄判定へ */ }
  let mine = false;
  try { mine = raw !== null && matchFn(raw); } catch { mine = false; }
  if (mine) {
    try { fs.unlinkSync(claim); } catch { /* 残置しても実害なし */ }
    return 'deleted';
  }
  try { fs.renameSync(claim, LOCK_FILE); return 'restored'; }
  catch {
    try { fs.unlinkSync(claim); } catch { /* 残置しても実害なし */ }
    return 'conflict';
  }
}

function releaseLock() {
  if (!myLockRunId) return;
  claimAndDeleteLock(raw => JSON.parse(raw).run_id === myLockRunId);
}

function isPidAlive(pid) {
  try { process.kill(pid, 0); return true; } catch { return false; }
}

// pid が「生きている node プロセス」か。単なる pid 生存だと Windows の pid 再利用で
// 無関係プロセスを先行 run と誤認して毎朝スキップし続けるため、プロセス名まで照合する。
// (tasklist 失敗時は保守的に「node である」扱い = 多重起動側に倒す)
function isAliveNodeProcess(pid) {
  if (!isPidAlive(pid)) return false;
  try {
    const out = execFileSync('tasklist', ['/FI', `PID eq ${pid}`, '/FO', 'CSV', '/NH'], { encoding: 'utf-8', timeout: 10000 });
    return /^"node(\.exe)?"/i.test(out.trim());
  } catch {
    return true;
  }
}
// retry-failed-jobs.js でリトライ可能なジョブ (idempotent + 一時的失敗想定)
// Amazon Ads / Settlement は SP-API throttle / レポート polling timeout / DL 失敗等の一過性失敗が多いため retry 対象
// Codex Round 1 #2: 'Amazon finance sync' は RETRYABLE_JOBS に入れない。
//   sync 単独 retry すると build やり直さずに古い fact を sync する事故になる。
//   build を retryable に残し、sync は build 成功時のみ実行 (依存連鎖は cron 単位で完結)。
// 'Amazon手数料' (fetch-amazon-fees.js) を追加 (2026-07-16 障害対応):
//   07:00 の SP-API 混雑帯で一過性失敗しても同日 retry されず、TTL(168h) 超過した高額 SKU が
//   数日累積して手数料カバー率が 69.84% まで崩壊した (incident_amazon_fee_coverage_no_retry)。
//   amazon_sku_fees への INSERT OR REPLACE + TTL/差分フィルタで再実行安全 (成功済み SKU は次 run で skip)。
// '楽天未発送アラート' も retry 対象: RMS API の一時障害で落ちた日でも、
// 8:30/10:00/11:30 の retry で当日中に通知が出る (失敗時のみ再実行 = 重複通知にはならない)
const RETRYABLE_JOBS = ['f_sales', 'sales_velocity', 'pml_snapshot', '楽天sku_map', 'Render同期', 'Amazon Ads (campaign)', 'Amazon Ads (SKU)', 'Amazon Settlement', 'Amazon finance build', 'Amazon手数料', 'ABA検索ワード', 'DBバックアップ', '楽天未発送アラート', 'Yahoo未発送アラート', 'auPAY未発送アラート', 'Qoo10未発送アラート', 'Yahoo問い合わせ対応漏れ', 'Qoo10'];

const GCHAT_WEBHOOK = process.env.GCHAT_WEBHOOK;

async function notify(text) {
  if (!GCHAT_WEBHOOK) {
    console.warn('[daily-sync] [NOTIFY:status=skipped] GCHAT_WEBHOOK未設定のため通知スキップ');
    return false;
  }
  try {
    const res = await fetch(GCHAT_WEBHOOK, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text }),
    });
    if (!res.ok) {
      console.error(`[daily-sync] [NOTIFY:status=failed] GChat HTTP ${res.status}`);
      return false;
    }
    console.log('[daily-sync] [NOTIFY:status=sent]');
    return true;
  } catch (e) {
    console.error('[daily-sync] [NOTIFY:status=failed]', e.message);
    return false;
  }
}

/**
 * Issue #85 対応: GChat 通知用 error summary 抽出
 * 単純 slice(0, 200) だと "Command failed: node ..." だけで FATAL 本体が切れる事故あり (5/10 朝)
 * → FATAL/Error/ERR 行を優先抽出 + 末尾数行 (stack trace の意味行) を残す形に
 * @param {string} errMsg - error.message (execFileSync の Command failed message + stdout/stderr)
 * @param {number} maxLen - GChat への送信最大長 (デフォルト 1024、GChat は 4096 まで OK)
 */
function extractErrorSummary(errMsg, maxLen = 1024) {
  if (!errMsg) return '(empty error message)';
  const lines = errMsg.split('\n').map(l => l.trim()).filter(Boolean);
  // 優先: FATAL / Error / ERR で始まる行 (root cause)
  const fatalLines = lines.filter(l => /^(FATAL|Error|ERR|TypeError|RangeError|SqliteError|SyntaxError)/.test(l));
  if (fatalLines.length > 0) {
    // root cause + 最後 3 行 (stack trace 末尾の意味行)
    const tail = lines.slice(-3).filter(l => !fatalLines.includes(l));
    const combined = [...fatalLines, ...(tail.length ? ['---', ...tail] : [])].join('\n');
    return combined.length > maxLen ? combined.slice(0, maxLen - 12) + '... [trunc]' : combined;
  }
  // フォールバック: 最後 5 行 (stack trace 末尾)
  const fallback = lines.slice(-5).join('\n');
  return fallback.length > maxLen ? fallback.slice(0, maxLen - 12) + '... [trunc]' : fallback;
}

/** 一過性 libuv クラッシュ後、再実行するまでの待機 (孫プロセス・ファイルロック・
 *  進行中リクエストの後始末が終わるのを待つ — Codex Medium 指摘)。
 *  runScript は同期関数なので Atomics.wait で同期スリープする */
const LIBUV_RETRY_WAIT_MS = Number(process.env.LIBUV_RETRY_WAIT_MS || 3000);
function sleepSync(ms) {
  if (!(ms > 0)) return;
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}

/**
 * 子スクリプトを実行する。
 *
 * 一過性 libuv クラッシュ ([[lib/libuv-transient-crash.js]] 参照) の保険として、
 * `{ retryLibuvCrash: true }` を渡した呼び出しだけ 1 回自動再実行する。
 *
 * 🚨 **既定は false (opt-in)。** このクラッシュには「本処理を全部終えてから終了時に abort する」型が
 *   あるため、再実行すると "完了済みの副作用" がもう一度走る。成功通知を直接送るジョブ
 *   (sync-to-render.js の `✅ Render同期完了` など) で有効化すると通知が二重に飛ぶ (Codex R3 High)。
 *   有効化してよいのは **再実行が完全に no-op になると確認済みのジョブだけ**
 *   (sha256 冪等 / run_id publish / 通知はキュー方式 = 送信済み分は消えている、等)。
 *
 * なお本命の対策は各スクリプト側で「通知 fetch 直後の process.exit() をやめる」こと
 * (#439 monitor-fee-coverage.js / #464 sync-rakuten-review-daily.js / 今回 import-rakuten-review.js)。
 * こちらは取りこぼし用の保険。
 *
 * @param {string} scriptPath - PROJECT_DIR からの相対パス (+ スペース区切り引数)
 * @param {string} label - ログ・GChat レポート上の表示名
 * @param {number} [timeoutMs=600000]
 * @param {object} [opts]
 * @param {boolean} [opts.retryLibuvCrash=false] - true で一過性 libuv クラッシュ時のみ1回再実行
 */
function runScript(scriptPath, label, timeoutMs = 600000, { retryLibuvCrash = false } = {}) {
  const parts = scriptPath.split(' ').filter(Boolean);
  const filePath = path.join(PROJECT_DIR, parts[0]);
  const scriptArgs = parts.slice(1);
  // 旧仕様互換: 引数なしなら '7' を強制 (一部スクリプトが days 指定として受け取る)
  if (scriptArgs.length === 0) scriptArgs.push('7');
  console.log(`\n=== ${label} ===`);
  // libuv 一過性クラッシュ時のみ 1 回だけ再実行 (それ以外は従来どおり即失敗)
  for (let attempt = 1; ; attempt++) {
    const startedAt = Date.now();
    try {
      // execFileSync(node, [...]) で直接 node を起動 (cmd.exe を経由しない)
      // → timeout 時に確実に node 本体が SIGTERM で殺される
      // (Codex 朝レビュー指摘: execSync("node ...") は Windows で cmd.exe 経由となり、
      //  shell だけ殺されて孫の node.exe が残り SQLite WAL lock が残存する事故あり)
      const output = execFileSync(process.execPath, [filePath, ...scriptArgs], {
        cwd: PROJECT_DIR,
        timeout: timeoutMs,
        encoding: 'utf-8',
        shell: false,
        windowsHide: true,
        env: {
          ...process.env,
          PATH: process.env.PATH,
          // 子プロセス側のSQLite busy_timeout を 60秒 に上書き（バッチ用）。
          // db.js の initDB() がこの env を参照する。
          WAREHOUSE_DB_BUSY_TIMEOUT_MS: process.env.WAREHOUSE_DB_BUSY_TIMEOUT_MS || '60000',
        },
        // 大きな stdout でも握りつぶさず確保 (デフォルト 1MB は settlement で超える)
        maxBuffer: 64 * 1024 * 1024,
      });
      console.log(output);
      const lines = output.trim().split('\n');
      const lastLine = lines[lines.length - 1] || '';
      // 再実行で通った場合は朝レポートに痕跡を残す (握り潰すと再発に気づけない)
      return { success: true, summary: attempt > 1 ? `♻️再実行で成功 | ${lastLine}` : lastLine };
    } catch (e) {
      // 観測性 (2026-07-16 障害対応、Codex 指摘 Critical):
      //   execFileSync 失敗時 e.message は "Command failed: node ..." のみ。子の stdout/stderr・
      //   exit status・signal は e に別プロパティで残る (捨てられていない)。これらを残さないと
      //   「timeout kill (signal=SIGTERM) / 子の非ゼロ終了 (status=1) / spawn 失敗 / maxBuffer 超過」
      //   を後から判別できず、単日の失敗原因が特定不能になる (今回まさにこれで確定できなかった)。
      const elapsedMs = Date.now() - startedAt;
      // 子出力は末尾 tail のみ扱う (全量 64MiB を再連結すると失敗処理中にメモリ増幅、Codex R2 #4)。
      const tail = (s, n) => String(s ?? '').trim().split('\n').slice(-n).join('\n');
      const errTail = tail(e.stderr, 20);
      const outTail = tail(e.stdout, 20);
      const meta = `status=${e.status ?? 'null'} signal=${e.signal ?? 'null'} code=${e.code ?? 'null'} elapsed=${Math.round(elapsedMs / 1000)}s${attempt > 1 ? ` attempt=${attempt}` : ''}`;
      const diagLines = [
        `[${label}] エラー: ${e.message} (${meta})`,
        errTail && `--- ${label} stderr(tail) ---\n${errTail}`,
        outTail && `--- ${label} stdout(tail) ---\n${outTail}`,
      ].filter(Boolean);
      console.error(diagLines.join('\n'));  // ローカルログには full-ish な tail を残す
      // 一過性 libuv クラッシュなら 1 回だけ再実行。ここで診断出力を先に出しているので、
      // 「1回目で実際に何をやり終えていたか」(取込件数・通知送信など) はログに残る
      if (attempt === 1 && retryLibuvCrash && isLibuvTransientCrash(e)) {
        console.error(`[${label}] ↑は Node/Windows の一過性 libuv クラッシュ (UV_HANDLE_CLOSING)。${Math.round(LIBUV_RETRY_WAIT_MS / 1000)}秒待って1回だけ自動再実行します`);
        sleepSync(LIBUV_RETRY_WAIT_MS);
        continue;
      }
      // GChat summary は message + meta + 子ログ tail を明示構成し ~400 字で cap。
      //   extractErrorSummary の「Error 始まりの行を優先」に依存すると、子ログが
      //   `[3/10] Error after 3 retries:` 形式で拾えず meta が落ちる (Codex R2 #5)。→ meta を先頭固定。
      //   per-job summary を抑えることで、多重失敗時の集約メッセージ肥大 (Codex R2 High #1) も緩和。
      //   meta を本当に先頭に置く (Codex R3 High): e.message が長い場合でも 400字cap で
      //   signal/status が落ちないよう、診断上最重要な meta を先頭固定にする。
      const rootish = errTail || outTail || '';
      const head = `${meta} | ${e.message}`;
      let summary = rootish ? `${head}\n${tail(rootish, 6)}` : head;
      if (summary.length > 400) summary = summary.slice(0, 388) + '… [trunc]';
      // exitCode は呼び出し側が失敗の種類で分岐するため (例: 楽天未発送アラートの exit 2 =
      // 「通知は出たが結果が不完全」→ retry しても同じなので blocked 扱いにする)。
      // 既存の呼び出し側は無視するので後方互換
      return { success: false, summary, exitCode: e.status ?? null };
    }
  }
}

/** Date を JST (UTC+9) の YYYY-MM-DD に変換 */
function toJstDate(d) {
  const jst = new Date(d.getTime() + 9 * 60 * 60 * 1000);
  return jst.toISOString().slice(0, 10);
}

// ─── ディスク保守 (再発防止: 2026-06-01 障害) ───
// 障害: warehouse.db.bak_pre_* (migration 直前の手動バックアップ) が 10 個・約 150GB 蓄積し
//   C: ドライブが残り 1.5GB に → Amazon Settlement / FBA snapshot が
//   "database or disk is full" で失敗、月末確定値まで連鎖 skip。
// 根本原因: これらの .bak は各 migration スクリプトが「実行前に手動 backup 必須」と
//   注記しているだけで、作成側に cleanup 機構が無く溜まり続ける。
// 対策: 毎朝この冒頭 (重い処理の前) で
//   (1) 古い .bak を自動掃除 — 最新 1 個は常に温存し、retention 日数より古いものだけ削除。
//   (2) 空き容量を取得し warn/crit 閾値で通知 → 溢れる前に気づける。
const DB_BAK_RETENTION_DAYS = Number(process.env.DB_BAK_RETENTION_DAYS || 7);
const DISK_FREE_WARN_GB = Number(process.env.DISK_FREE_WARN_GB || 20);
const DISK_FREE_CRIT_GB = Number(process.env.DISK_FREE_CRIT_GB || 10);

function diskMaintenance() {
  const dataDir = process.env.DATA_DIR || path.join(PROJECT_DIR, 'data');
  const warnings = [];
  const deleted = [];
  let freedBytes = 0;
  let keptCount = 0;

  // (1) 古い warehouse.db.bak* (= migration 直前バックアップ) の掃除
  try {
    const baks = fs.readdirSync(dataDir)
      .filter((n) => n.startsWith('warehouse.db.bak'))
      .map((n) => {
        const fp = path.join(dataDir, n);
        try {
          const st = fs.statSync(fp);
          return { name: n, fp, mtime: st.mtimeMs, size: st.size };
        } catch {
          return null;
        }
      })
      .filter(Boolean)
      .sort((a, b) => b.mtime - a.mtime); // 新しい順

    keptCount = baks.length; // 後で削除数を引く
    const cutoff = Date.now() - DB_BAK_RETENTION_DAYS * 86400000;
    // index 0 (最新) は age に関わらず常に温存 (直前 migration の唯一の復元点を守る)。
    // 残りのうち retention 日数より古いものだけ削除 (新しい .bak は手元に残す)。
    for (let i = 1; i < baks.length; i++) {
      const b = baks[i];
      if (b.mtime < cutoff) {
        try {
          fs.unlinkSync(b.fp);
          deleted.push(b.name);
          freedBytes += b.size;
        } catch (e) {
          warnings.push(`🟡 .bak 削除失敗 ${b.name} (${e.message})`);
        }
      }
    }
    keptCount -= deleted.length;
  } catch (e) {
    warnings.push(`🟡 .bak 掃除スキップ (${e.message})`);
  }

  // (2) 空き容量チェック (fs.statfsSync は Node v18.15+、miniPC は v24)
  let freeGB = null;
  try {
    const s = fs.statfsSync(dataDir);
    freeGB = Math.round((s.bavail * s.bsize) / 1e9 * 10) / 10;
  } catch (e) {
    warnings.push(`🟡 空き容量取得失敗 (${e.message})`);
  }

  if (freeGB !== null) {
    if (freeGB < DISK_FREE_CRIT_GB) {
      warnings.unshift(`🔴 ディスク残り ${freeGB}GB → 逼迫 (warehouse.db ~35GB の書き込みが失敗する恐れ、不要 .bak / クラッシュダンプ削除を)`);
    } else if (freeGB < DISK_FREE_WARN_GB) {
      warnings.unshift(`🟡 ディスク残り ${freeGB}GB`);
    }
  }

  const freedGB = Math.round((freedBytes / 1e9) * 10) / 10;
  const summary =
    `${freeGB !== null ? `残り ${freeGB}GB` : '残り不明'}` +
    ` / .bak ${deleted.length}個削除` +
    `${deleted.length ? `(${freedGB}GB解放)` : ''}, 保持${keptCount}個`;
  // critical 未満のみ success:false (= 全体通知を ⚠️ 化)。warn / 取得失敗では落とさない。
  const ok = !(freeGB !== null && freeGB < DISK_FREE_CRIT_GB);
  return { ok, summary, warnings, freeGB };
}

async function main() {
  const startTime = new Date();
  // JST 固定の業務日付。子プロセスへ env で引き回す (UTC癖回避)
  const businessDate = toJstDate(startTime);
  process.env.WAREHOUSE_BUSINESS_DATE = businessDate;
  const dateStr = businessDate;
  console.log(`[DailySync] 開始: ${startTime.toISOString()} (business_date=${businessDate})`);

  // state 操作の失敗を集約して通知に出す配列 (握り潰しを排除)。
  // 起動時 cleanup / 書き込み失敗時の旧 state 削除 / 全成功時 cleanup の3経路で使う。
  const stateOpWarnings = [];

  // 多重起動ガード + 前回異常終了検知 (構造監査 H-3)
  try {
    if (fs.existsSync(LOCK_FILE)) {
      // read と parse を分離: 破損 lock も raw 一致で回収できるようにする
      // (破損 lock を回収不能にすると wx が永久 EEXIST → 毎朝中止が続く)
      let prevRaw = null;
      let prev = null;
      try { prevRaw = fs.readFileSync(LOCK_FILE, 'utf-8'); } catch { /* 消えた直後なら wx 取得で決着 */ }
      try { prev = prevRaw !== null ? JSON.parse(prevRaw) : null; } catch { /* 破損 = prev null のまま残骸扱い */ }
      // 先行 run が「生きている node プロセス」なら常に中止 (ハング中でも並走は SQLite 直列書き込み前提を壊すので不可)。
      // pid 再利用の誤検知はプロセス名照合で排除。それでも残る場合は通知の手動対応案内で回収する
      if (prev && prev.pid && isAliveNodeProcess(prev.pid)) {
        const msg = `⚠️ *Warehouse日次同期 多重起動を中止* (先行 run: pid=${prev.pid}, started_at=${prev.started_at})\n先行 run がハングしている場合はプロセス終了後に手動削除を: ${LOCK_FILE}`;
        console.error(`[DailySync] ${msg}`);
        await notify(msg);
        process.exit(1);  // 先行 run の lock は触らない
      }
      // 先行 run 死亡 or 破損 lock = 残骸。回収は claimAndDeleteLock で排他化し、
      // 「自分が観測した raw とバイト一致」する実体だけ削除する (Codex R5: run_id 有無に依存しない同一性判定)
      if (prevRaw !== null) {
        const recovery = claimAndDeleteLock(raw => raw === prevRaw);
        if (recovery === 'deleted') {
          const info = prev ? `started_at=${prev.started_at}, pid=${prev.pid}` : '(lock 破損)';
          stateOpWarnings.push(`🔴 前回の daily-sync が完了通知なしで異常終了した形跡 (${info})。当該朝のジョブは途中までしか実行されていない`);
        } else if (recovery === 'restored' || recovery === 'conflict') {
          // 観測後に別プロセスが回収→新 lock 作成済み = 並行起動レース → 中止
          const msg = `⚠️ *Warehouse日次同期 lock 回収レースを検知、今回の起動を中止* (先行 run が並行起動中)`;
          console.error(`[DailySync] ${msg}`);
          await notify(msg);
          process.exit(1);
        }
        // 'gone' = 回収レース負け。直後の 'wx' 取得で決着する
      }
    }
  } catch (e) {
    stateOpWarnings.push(`🔴 lock ファイル検査失敗 (${e.message})。多重起動ガード無効のまま続行: ${LOCK_FILE}`);
  }
  // flag 'wx' = 排他的作成。check-then-write の TOCTOU で 2 プロセスが並走するのを防ぐ (Codex High #1)
  try {
    const runId = `${process.pid}-${crypto.randomUUID()}`;
    fs.writeFileSync(LOCK_FILE, JSON.stringify({ run_id: runId, pid: process.pid, started_at: startTime.toISOString(), business_date: businessDate }), { flag: 'wx' });
    myLockRunId = runId;
  } catch (e) {
    if (e.code === 'EEXIST') {
      // 直前の残骸回収と自分の取得の間に別プロセスが lock を取った = 多重起動レース負け
      const msg = `⚠️ *Warehouse日次同期 多重起動を中止* (lock 取得レースで先行 run を検知)`;
      console.error(`[DailySync] ${msg}`);
      await notify(msg);
      process.exit(1);
    }
    // EEXIST 以外 (EACCES/ENOSPC 等) = data ディレクトリ自体の異常。多重起動ガード不能のまま
    // 続行しても後続の DB/state 書き込みも壊れるので fail-closed (Codex R6 High)
    const msg = `🔴 *Warehouse日次同期 起動中止* lock 書き込み失敗 (${e.message})。data ディレクトリの異常を確認してください: ${LOCK_FILE}`;
    console.error(`[DailySync] ${msg}`);
    await notify(msg);
    process.exit(1);
  }

  // retry-state の前処理:
  //   - 別日 or 破損 → 即削除 (古い backlog を引きずらない)
  //   - 同日 → 一旦残す。今回 run の結果に応じて終了時に上書き or 削除する
  //     (失敗あり → 上書き / 全成功 → 削除)
  try {
    if (fs.existsSync(RETRY_STATE_FILE)) {
      let removeStale = false;
      try {
        const existing = JSON.parse(fs.readFileSync(RETRY_STATE_FILE, 'utf-8'));
        if (existing.run_date !== businessDate) removeStale = true;
        else console.log(`[DailySync] 同日の retry-state を検出 (run_date=${existing.run_date})。終了時に上書き or 削除予定`);
      } catch {
        // 破損 state は削除対象
        removeStale = true;
      }
      if (removeStale) {
        try {
          fs.unlinkSync(RETRY_STATE_FILE);
          console.log('[DailySync] 別日 or 破損した retry-state を削除');
        } catch (delErr) {
          // 削除失敗 = 古い backlog が残る = retry-failed-jobs が stale 内容で誤実行の恐れ
          stateOpWarnings.push(`🔴 起動時 retry-state cleanup 失敗 (${delErr.message})、retry が古い内容で誤実行する恐れあり。手動削除を: ${RETRY_STATE_FILE}`);
          console.error('[DailySync] retry-state 削除失敗:', delErr.message);
        }
      }
    }
  } catch (e) {
    stateOpWarnings.push(`🔴 起動時 retry-state チェック失敗 (${e.message})、retry 状態が不明。手動確認を: ${RETRY_STATE_FILE}`);
    console.error('[DailySync] retry-state チェック失敗:', e.message);
  }

  const results = [];

  // ディスク保守を最初に実行: 古い .bak 掃除で当日の空きを確保し、空き容量を監視。
  // 後続の重い処理 (Amazon Settlement / FBA snapshot 等、warehouse.db 書き込み) の前に走らせる。
  const diskInfo = diskMaintenance();
  console.log(`[DailySync] ディスク保守: ${diskInfo.summary}`);
  results.push({ name: '💾 ディスク保守', success: diskInfo.ok, summary: diskInfo.summary });

  // VPS環境変数同期 (au PAYキーがミニPC .env で更新されていればVPSへ反映)
  const vpsEnvResult = runScript('apps/warehouse/sync-vps-env.js', 'VPS env同期');
  results.push({ name: 'VPS env同期', ...vpsEnvResult });

  // Settlement 冪等性 smoke テスト (監査PR-13/INV-27。一時DB相手に数秒で完了、本番DBに触れない)
  // hash設計が崩れると PR #229 (31.8M行膨張) が再発するため、Settlement 取得より前に毎朝検証。
  // 失敗しても以降のジョブは止めない (❌通知で気付く)。
  const idemResult = runScript('apps/warehouse/test-settlement-idempotency.js', 'Settlement冪等性テスト', 120000);
  results.push({ name: 'Settlement冪等性テスト', ...idemResult });

  // raw_*_orders_log 3本のローテ (監査PR-12(b)。保持60日+月次gzアーカイブ。
  // 実測2.3GB/4.5M行の純無限成長を停止。定常時は前日分のみで数秒)
  const rotateResult = runScript('apps/warehouse/rotate-order-logs.js', 'ログローテ', 1800000);
  results.push({ name: 'ログローテ', ...rotateResult });

  // NE API（商品マスタ + セット商品 + 受注7日分 + 受注ベース14日分）
  const neResult = runScript('apps/warehouse/ne-api.js sync', 'NE API');
  results.push({ name: 'NE', ...neResult });

  // NE 取得成功直後に raw_ne_products → ne_stock_daily_snapshot へ複製 (履歴化)
  // ここで失敗しても後続は続ける (snapshot は付加的、欠損日は翌日また取れる)
  if (neResult.success) {
    const snapResult = runScript('apps/warehouse/snapshot-ne-stock.js', 'NE在庫スナップショット', 60000);
    results.push({ name: 'NE在庫snapshot', ...snapResult });
  } else {
    console.log('[DailySync] NE API 失敗のため在庫スナップショットをスキップ');
  }

  // 日次出荷サマリ (出荷日 × モール × 配送方法 の伝票件数) を再構築。
  // raw_ne_order_base からの純粋な再集計なので数秒。NE 失敗時は前日の値を残す
  // (部分取得の raw から作り直すと当日の件数が過少に見える)。
  // --all で毎回全期間を作り直す: 出荷日が訂正されて再集計窓の外へ動くと、窓を切った
  // 再構築では移動先の日が更新されず件数がズレる (Codex R1 medium)。全期間でも数秒。
  if (neResult.success) {
    const shipDailyResult = runScript('apps/warehouse/rebuild-shipments-daily.js --all', '日次出荷サマリ', 120000);
    results.push({ name: '出荷サマリ', ...shipDailyResult });
  } else {
    console.log('[DailySync] NE API 失敗のため出荷サマリ再構築をスキップ');
  }

  // SP-API
  const spResult = runScript('apps/warehouse/sp-api-orders.js', 'Amazon SP-API');
  results.push({ name: 'Amazon', ...spResult });

  // Amazon Settlement Report raw 取得 (Phase 3.1.1)
  // SP-API getReports で直近 14 日の Settlement を DL → raw_amazon_settlement_lines に append
  // settlement_refresh_queue へ dirty month 追加 → 後段の mart rebuild が拾う
  // 14日 = 1〜2 settlements、日次の差分捕捉に十分。timeout は 60 分余裕
  // (2026-05-07 朝の cron で --days 30 default + 30分 timeout で ETIMEDOUT、過去 90 日分は手動 fetch 済)
  const settlementResult = runScript('apps/warehouse/fetch-amazon-settlements.js --days 14', 'Amazon Settlement', 3600000);
  results.push({ name: 'Amazon Settlement', ...settlementResult });

  // ABA「Amazon検索用語」週次取込 (セラースプライト置換、aba.db 別建て)
  // 取込済み週は即skip・レポート未公開 (集計中) は正常skip の冪等設計なので毎朝呼んでよい。
  // 週次レポートの公開日が不定 (週明け〜数日後) なため、毎朝叩いて公開当日に自動取込する。
  const abaResult = runScript('apps/aba-keywords/fetch-aba-search-terms.js', 'ABA検索ワード', 3600000);
  results.push({ name: 'ABA検索ワード', ...abaResult });

  // Amazon Ads spCampaigns (campaign 全広告費、Phase 3.4)
  // 直近 30 日。fact_ad_spend_campaign に PK(日付,モール,キャンペーンID,広告タイプ) で UPSERT
  // Auto-Targeting Campaign の SKU 別漏れ (47%) を補完するための campaign 単位データ
  const adsCampaignResult = runScript('apps/warehouse/fetch-amazon-ads-campaign.js', 'Amazon Ads (campaign)', 1800000);
  results.push({ name: 'Amazon Ads (campaign)', ...adsCampaignResult });

  // Amazon Ads spAdvertisedProduct (SKU 別広告費)
  // 直近 30 日。fact_ad_spend に SKU 別の広告費を UPSERT
  // v_amazon_sku_profit_actual_v4 view が SKU 別 contribution margin 計算に使う
  const adsProductResult = runScript('apps/warehouse/fetch-amazon-ads.js', 'Amazon Ads (SKU)', 1800000);
  results.push({ name: 'Amazon Ads (SKU)', ...adsProductResult });

  // Amazon SKU手数料取得 (v2: batch API + TTL/差分更新)
  // SP-API getMyFeesEstimates (20件/call、0.5 RPS) で fetch、未キャッシュ + TTL>7日 + 価格乖離大 のみ refresh
  // 旧版の単発APIで37分タイムアウトしてた問題を解消、通常2-3分で完了 (fetch-amazon-fees.js v2 に rewrite 済)
  // SP-API 失敗時はスキップ (依存関係)、後続は継続
  if (spResult.success) {
    const feeResult = runScript(
      'apps/warehouse/fetch-amazon-fees.js --recent 30',
      'Amazon手数料 (--recent 30)',
      600000  // 10分 (通常 2-3分 + マージン、batch API + TTL で 30分も要らない)
    );
    results.push({ name: 'Amazon手数料', ...feeResult });
  } else {
    console.log('[DailySync] SP-API 失敗のため Amazon手数料取得をスキップ');
    results.push({ name: 'Amazon手数料', success: false, summary: '⏭️ skipped (SP-API失敗のため)' });
  }

  // Amazon カート(Buy Box)価格スナップショット (amazon-dashboard PR-D)
  // Product Pricing v0 batch (0.5 RPS)。~2,500 SKU で 2 パス ≈ 9 分想定、30 分余裕。
  // 対象 SKU は amazon_sku_fees キャッシュ流用のため、SP-API 失敗時はスキップ。
  if (spResult.success) {
    const pricesResult = runScript('apps/warehouse/fetch-amazon-prices.js', 'Amazonカート価格', 1800000);
    results.push({ name: 'Amazonカート価格', ...pricesResult });
  } else {
    console.log('[DailySync] SP-API 失敗のため Amazonカート価格取得をスキップ');
    results.push({ name: 'Amazonカート価格', success: false, summary: '⏭️ skipped (SP-API失敗のため)' });
  }

  // FBA 在庫スナップショット (RESTOCK + PLANNING) — daily_snapshots に履歴蓄積
  // 手動 /fetch-reports と lockfile で排他、既に走ってればスキップ (失敗扱いにしない)
  // SP-API レポート polling のため最大 15 分余裕
  const fbaSnapResult = runScript('apps/warehouse/snapshot-fba-stock.js', 'FBA在庫スナップショット', 900000);
  results.push({ name: 'FBA在庫snapshot', ...fbaSnapResult });

  // 在庫スナップショット集計 (FBA + 自社倉庫の金額算出 → inv_daily_summary)
  // 上の NE/FBA スナップショットの後に必ず走る (失敗時も結果は no_source で記録)
  const invAggResult = runScript('apps/warehouse/snapshot-inventory-aggregate.js', '在庫スナップショット集計', 120000);
  results.push({ name: '在庫集計', ...invAggResult });

  // 楽天
  const rkResult = runScript('apps/warehouse/rakuten-orders.js', '楽天 RMS API');
  results.push({ name: '楽天', ...rkResult });

  // Yahoo!ショッピング（VPSプロキシ経由で遅延しやすいため60分）
  const yahooResult = runScript('apps/warehouse/yahoo-orders.js 7', 'Yahoo!ショッピング', 3600000);
  // 受注取込の窓 (7日) から漏れた ship_date を毎日少しずつ埋める (P2-Y PR-Y-C2)。
  // 出荷完了なのに発送日が無い注文 → レビューメールのフォロー予定が立たないため。上限 60 件/日 (VPS 側 1.1 秒間隔)
  const yahooShipDateFill = runScript(
    'apps/warehouse/backfill-yahoo-ship-date.js --days 60 --limit 60', 'Yahoo発送日 穴埋め', 300000);
  results.push({ name: 'Yahoo発送日 穴埋め', ...yahooShipDateFill });
  results.push({ name: 'Yahoo', ...yahooResult });

  // au PAY マーケット (Wow!manager API、VPS proxy 経由で遅延しやすいため 60 分)
  // Phase 1: aupay-orders.js (受注 API 全フィールド + fail-closed) に移行
  const aupayResult = runScript('apps/warehouse/aupay-orders.js 7', 'au PAY マーケット', 3600000);
  results.push({ name: 'au PAY', ...aupayResult });

  // Qoo10 Phase 1 A-1 (2026-05-18、設計書 v0.11)
  // mall-orders.js fetchQoo10 (packNo を PK 使用、grain 崩壊バグ持ち、~17,252 行) を廃止
  // → qoo10-orders.js (新規) に置換、orderNo を PK + 90日 frozen horizon + 30列保持
  // 旧 raw は migrate_legacy_raw_qoo10_orders.sql で 'legacy:%' prefix + legacy_fields_missing=1 にマイグレ済
  const qoo10Result = runScript('apps/warehouse/qoo10-orders.js 90', 'Qoo10');
  results.push({ name: 'Qoo10', ...qoo10Result });

  // LINEギフト Phase 1 A-1 (2026-05-15、設計書 v0.5)
  // mall-orders.js fetchLineGift (バグ持ち、item_code 等空文字) を廃止 → linegift-orders.js (新規) に置換
  // OAuth refresh atomic (file lock + secure staging + atomic .env write) + 90日境界 frozen horizon
  const linegiftResult = runScript('apps/warehouse/linegift-orders.js', 'LINEギフト');
  results.push({ name: 'LINEギフト', ...linegiftResult });

  // 統合商品マスタ再構築
  // NE 失敗時はスキップ: raw_ne_* が部分状態の可能性がある中で rebuild すると、
  // セット構成の欠落等が staging 件数ゲートを素通りして m_products に固定される (構造監査 H-1)。
  // スキップ時は前日の m_products が残る (stale but consistent)。
  let mProductResult;
  if (neResult.success) {
    mProductResult = runScript('apps/warehouse/rebuild-m-products.js', 'm_products 再構築');
  } else {
    console.log('[DailySync] NE 失敗のため m_products 再構築をスキップ (前日データ維持)');
    mProductResult = { success: false, summary: '⏭️ skipped (NE失敗のため、前日データ維持)' };
  }
  results.push({ name: 'm_products', ...mProductResult });

  // m_products 変更差分を history に記録 (trigger 廃止 → 差分バッチ化)
  // rebuild-m-products.js の直後に実行 (m_products 確定後の比較)
  const historyResult = runScript('apps/warehouse/record-m-products-history.js', 'm_products 履歴記録');
  results.push({ name: 'm_products_history', ...historyResult });

  // 販売集計テーブル再構築
  const fSalesResult = runScript('apps/warehouse/rebuild-f-sales.js', 'f_sales 再構築');
  results.push({ name: 'f_sales', ...fSalesResult });

  // 販売速度サマリ再構築 (商品管理リスト用: FBA/FBA以外 × 7d/30d)
  // m_products / v_sku_resolved / raw受注 が揃った後に実行。
  const velocityResult = runScript('apps/warehouse/rebuild-sales-velocity.js', 'sales velocity 再構築');
  results.push({ name: 'sales_velocity', ...velocityResult });

  // 商品管理リスト スナップショット生成 (在庫集計 + velocity の後)
  // m_products 起点で在庫/販売/利益/発注パラメータを1表に確定し published_run_id を切替。
  const pmlSnapResult = runScript('apps/warehouse/build-product-management-snapshot.js', '商品管理リスト snapshot');
  results.push({ name: 'pml_snapshot', ...pmlSnapResult });

  // Amazon Settlement mart 再構築 (Phase 3.5)
  // dirty queue から rebuild。Refund qty 推定込みで long+wide 生成。
  // queue 空なら即終了するので低コスト。
  const settlementMartResult = runScript('apps/warehouse/rebuild-amazon-settlement-mart.js', 'Settlement mart 再構築', 900000);
  results.push({ name: 'Settlement mart', ...settlementMartResult });

  // === Amazon finance daily fact (Phase 1 #1-7、#1-1 + #1-4a 統合) ===
  // 1. f_amazon_finance_sku_daily_v1 を当月分 build (snapshot UPSERT)
  //    → raw_amazon_settlement_lines から economic_date × seller_sku_normalized で集約、
  //       snapshot 原価 + cost_status 4 値、UPSERT で snapshot 不変
  // 2. mirror_amazon_finance_sku_daily へ sync (chunk POST + ledger 記録)
  //    → entity-driven contract sync、CHUNK_SIZE=3000 (5000 で connection terminated 対策)
  //    → sync 成功時に Render rebuild trigger を内部で POST (現状 noop)
  // 月初は前月確定値も sync 必要だが MVP は当月のみ (前月処理は将来)。
  const currentMonth = businessDate.slice(0, 7); // 'YYYY-MM'
  // DATA_DIR は env 必須 (memory: feedback_db_path_cwd_dependency.md、cwd fallback で
  // worktree の stray DB 事故を起こした履歴あり、Codex Round 1 #1 対応で fail-fast 化)
  if (!process.env.DATA_DIR) {
    // 設定不備系の blocked は success:false (失敗扱いで通知に載る) かつ blocked:true で
    // retry 対象外マーキング (Codex Round 3 #medium 対応)。
    //   - 旧 success:true は green 集計に隠れるリスク (Codex 指摘)
    //   - 単純 success:false は RETRYABLE_JOBS の永久 retry ノイズ
    //   - blocked:true 別軸で「失敗だが retry しない」を明示、下の retry 判定 filter で除外
    //   - 真の解決は winsw config 修正
    console.error('[DailySync] FATAL: DATA_DIR env が未設定です。Amazon finance build/sync は実行できません。winsw config に <env name="DATA_DIR" value="C:\\Users\\bfaith\\bfaith-portal\\data"/> を設定してください。');
    const blockedSummary = '🔴 blocked: DATA_DIR env 未設定 (winsw config 要修正、retry 対象外)';
    results.push({ name: 'Amazon finance build', success: false, blocked: true, summary: blockedSummary });
  } else {
    // runScript は scriptPath.split(' ') で parse するため、DATA_DIR_ARG は空白なし前提
    // (引用符 `"${DATA_DIR_ARG}"` を script string に含めると literal のまま args に渡り
    //  `"C:/.../data"` で fs.existsSync false → FATAL: warehouse.db not found 事故あり、5/10 朝発覚)
    const DATA_DIR_ARG = process.env.DATA_DIR.replace(/\\/g, '/');
    if (DATA_DIR_ARG.includes(' ')) {
      console.error(`[DailySync] FATAL: DATA_DIR に空白が含まれています (${DATA_DIR_ARG})。runScript の split(' ') 仕様で分解されます`);
    }
    const amazonFinanceBuildResult = runScript(
      `scripts/amazon-finance/build-daily-fact.js --data-dir ${DATA_DIR_ARG} --month ${currentMonth}`,
      'Amazon finance build', 600000
    );
    results.push({ name: 'Amazon finance build', ...amazonFinanceBuildResult });

    if (amazonFinanceBuildResult.success) {
      // CHUNK_SIZE は 3000 推奨 (issue #72)、env 経由で override 可
      if (!process.env.CHUNK_SIZE) process.env.CHUNK_SIZE = '3000';
      const amazonFinanceSyncResult = runScript(
        `apps/warehouse/sync-amazon-finance-daily.js --data-dir ${DATA_DIR_ARG} --month ${currentMonth}`,
        'Amazon finance sync', 600000
      );
      results.push({ name: 'Amazon finance sync', ...amazonFinanceSyncResult });
    } else {
      // build 失敗 → sync は記録自体しない (Codex Round 1 #2 対応)。
      // sync を success:false で push + RETRYABLE_JOBS にあると retry-failed-jobs が
      // build を再実行せずに sync 単独 retry してしまい、古い fact を sync する事故。
      // build を retryable に残し、sync は build 成功時のみ実行 = sync を retry 対象外にする。
      console.log(`[DailySync] Amazon finance sync は build 失敗のため記録せず (build retry 後に翌 cron で sync 実行)`);
    }

    // === Amazon finance 前月分 build+sync (月初の settlement 追い込み反映) ===
    // settlement は約14日周期で確定するため、月初〜中旬は前月 economic_date の行が
    // 新規 settlement で増え続ける。従来は当月のみ (「前月処理は将来」TODO) だったため、
    // 月初に amazon_finance_sku_daily の sync が最大2週間止まって見えていた
    // (2026-07-07 発覚: 6/29 を最後に 1 週間 no rows in range)。
    // 毎月 20 日までは前月分も build+sync する (snapshot 原価は UPSERT 不変なので安全)。
    const dayOfMonth = parseInt(businessDate.slice(8, 10), 10);
    if (dayOfMonth <= 20) {
      const prevMonthDate = new Date(Date.UTC(
        parseInt(currentMonth.slice(0, 4), 10),
        parseInt(currentMonth.slice(5, 7), 10) - 2, 1
      ));
      const prevMonth = `${prevMonthDate.getUTCFullYear()}-${String(prevMonthDate.getUTCMonth() + 1).padStart(2, '0')}`;
      const prevBuildResult = runScript(
        `scripts/amazon-finance/build-daily-fact.js --data-dir ${DATA_DIR_ARG} --month ${prevMonth}`,
        'Amazon finance build (前月)', 600000
      );
      results.push({ name: 'Amazon finance build (前月)', ...prevBuildResult });
      if (prevBuildResult.success) {
        const prevSyncResult = runScript(
          `apps/warehouse/sync-amazon-finance-daily.js --data-dir ${DATA_DIR_ARG} --month ${prevMonth}`,
          'Amazon finance sync (前月)', 600000
        );
        results.push({ name: 'Amazon finance sync (前月)', ...prevSyncResult });
      } else {
        console.log(`[DailySync] Amazon finance sync (前月) は build 失敗のためスキップ`);
      }
    }

    // === Amazon Ads mirror sync (amazon-dashboard PR-A) ===
    // fact_ad_spend / fact_ad_spend_campaign を Render mirror へ sync。
    // 取得ジョブ (上の Amazon Ads (campaign)/(SKU)) は直近 30 日窓なので、
    // attribution 遡及の取りこぼし余裕を見て 35 日窓で scope clear + 再投入。
    // campaign と SKU の鮮度がズレると dashboard の未配賦率/TACOS が歪むため、
    // **両方成功時のみ** sync する (Codex R1 Medium #2)。片方失敗は fetch retry 後の翌 cron に委ねる。
    if (adsCampaignResult.success && adsProductResult.success) {
      const adsSyncResult = runScript(
        `apps/warehouse/sync-amazon-ads-daily.js --data-dir ${DATA_DIR_ARG} --days 35`,
        'Amazon Ads sync', 600000
      );
      results.push({ name: 'Amazon Ads sync', ...adsSyncResult });
    } else {
      console.log('[DailySync] Amazon Ads sync は fetch 失敗 (campaign/SKU いずれか) のためスキップ (fetch retry 後に翌 cron で sync 実行)');
    }

    // === Amazon カート価格 mirror sync (amazon-dashboard PR-D) ===
    // 日次スナップショットなので直近 3 日窓で十分 (取り漏れ日の再送用)。
    const pricesFetchOk = results.some(r => r.name === 'Amazonカート価格' && r.success);
    if (pricesFetchOk) {
      const priceSyncResult = runScript(
        `apps/warehouse/sync-amazon-price-snapshot.js --data-dir ${DATA_DIR_ARG} --days 3`,
        'Amazonカート価格 sync', 300000
      );
      results.push({ name: 'Amazonカート価格 sync', ...priceSyncResult });
    } else {
      console.log('[DailySync] Amazonカート価格 sync は fetch 失敗のためスキップ');
    }

    // === Amazon アカウント単位フィー月次 (amazon-dashboard PR-C) ===
    // 保管料/長期在庫追加手数料/返送等の SKU 無しフィーを月次集計して mirror へ。
    // raw settlement は上の fetch-amazon-settlements.js で更新済みの前提 (失敗時も前回分で再集計、冪等)。
    const accountFeesBuildResult = runScript(
      `apps/warehouse/rebuild-amazon-account-fees.js --data-dir ${DATA_DIR_ARG} --months 14`,
      'Amazonアカウントフィー build', 300000
    );
    results.push({ name: 'Amazonアカウントフィー build', ...accountFeesBuildResult });
    if (accountFeesBuildResult.success) {
      const accountFeesSyncResult = runScript(
        `apps/warehouse/sync-amazon-account-fees.js --data-dir ${DATA_DIR_ARG} --months 14`,
        'Amazonアカウントフィー sync', 300000
      );
      results.push({ name: 'Amazonアカウントフィー sync', ...accountFeesSyncResult });
    } else {
      console.log('[DailySync] Amazonアカウントフィー sync は build 失敗のためスキップ');
    }

    // === 楽天 finance daily fact (Phase 1a #R-3c、#R-1 + #R-2 + #R-3a 統合) ===
    // 1. f_rakuten_finance_sku_daily_v1 build (status IN (500,600,700) D 案、snapshot UPSERT)
    // 2. DQ gate (6 check、severity error で exit 1)
    // 3. mirror_rakuten_finance_sku_daily へ sync (chunk POST + ledger 記録、Render side で受信)
    // build 失敗で DQ/sync スキップ、DQ gate failure (exit 1) でも sync スキップ
    const rakutenFinanceBuildResult = runScript(
      `scripts/rakuten-finance/build-rakuten-daily-fact.js --data-dir ${DATA_DIR_ARG} --month ${currentMonth}`,
      '楽天 finance build', 600000
    );
    results.push({ name: '楽天 finance build', ...rakutenFinanceBuildResult });

    if (rakutenFinanceBuildResult.success) {
      const rakutenFinanceDqResult = runScript(
        `apps/warehouse/run-rakuten-finance-dq.js --data-dir ${DATA_DIR_ARG} --month ${currentMonth}`,
        '楽天 finance DQ', 300000
      );
      results.push({ name: '楽天 finance DQ', ...rakutenFinanceDqResult });

      if (rakutenFinanceDqResult.success) {
        // CHUNK_SIZE は Amazon と共有 (上で 3000 set 済)
        const rakutenFinanceSyncResult = runScript(
          `apps/warehouse/sync-rakuten-finance-daily.js --data-dir ${DATA_DIR_ARG} --month ${currentMonth}`,
          '楽天 finance sync', 600000
        );
        results.push({ name: '楽天 finance sync', ...rakutenFinanceSyncResult });
      } else {
        // DQ gate failure (severity='error' breach) → sync スキップ
        // 不正なデータを Render mirror に押し付ける事故防止 (Amazon Phase 1 と同方針)
        console.log(`[DailySync] 楽天 finance sync は DQ gate failure のため記録せず (品質不正データの mirror 投入防止)`);
      }
    } else {
      // build 失敗 → DQ/sync 記録しない (Amazon と同方針)
      console.log(`[DailySync] 楽天 finance DQ/sync は build 失敗のため記録せず (build retry 後に翌 cron で実行)`);
    }

    // === 楽天RPP広告費 取込 + mirror sync (mall-csv-fetcher P1) ===
    // 自動DL (rakuten-rpp-download.mjs、別 Task Scheduler スロット) と手動バックアップが
    // incoming/rakuten-ads/ に置いた CSV/zip を取り込む。対象0件は正常 (exit 0)。
    // 取込成功時のみ sync (--days 70 = 今月+先月+月初の前々月をカバー、720h遡及)
    const rakutenAdsImportResult = runScript(
      `apps/warehouse/import-rakuten-ads-rpp.js --data-dir ${DATA_DIR_ARG}`,
      '楽天RPP広告 取込', 600000
    );
    results.push({ name: '楽天RPP広告 取込', ...rakutenAdsImportResult });
    if (rakutenAdsImportResult.success) {
      const rakutenAdsSyncResult = runScript(
        `apps/warehouse/sync-rakuten-ads-daily.js --data-dir ${DATA_DIR_ARG} --days 70`,
        '楽天RPP広告 sync', 600000
      );
      results.push({ name: '楽天RPP広告 sync', ...rakutenAdsSyncResult });
    } else {
      console.log('[DailySync] 楽天RPP広告 sync は取込失敗のためスキップ (failed/ を確認、修正後に再投入)');
    }

    // === 楽天RMSデータ分析 (商品分析SKU日次/店舗日次) 取込 + mirror sync (mall-csv-fetcher P1-R2) ===
    // 手動DL (将来は自動DL) が incoming/rakuten-data/ に置いた CSV/zip を取り込む。
    // 対象0件は正常 (exit 0)。取込成功時のみ sync (--days 70、RPP と同 scope)
    const rakutenDataImportResult = runScript(
      `apps/warehouse/import-rakuten-data.js --data-dir ${DATA_DIR_ARG}`,
      '楽天データ分析 取込', 600000
    );
    results.push({ name: '楽天データ分析 取込', ...rakutenDataImportResult });
    if (rakutenDataImportResult.success) {
      const rakutenDataSyncResult = runScript(
        `apps/warehouse/sync-rakuten-data-daily.js --data-dir ${DATA_DIR_ARG} --days 70`,
        '楽天データ分析 sync', 600000
      );
      results.push({ name: '楽天データ分析 sync', ...rakutenDataSyncResult });
    } else {
      console.log('[DailySync] 楽天データ分析 sync は取込失敗のためスキップ (failed/ を確認、修正後に再投入)');
    }

    // === 楽天レビューCSV 取込 + mirror sync (mall-csv-fetcher P2 PR-A、らくらくーぽん置換) ===
    // 自動DL (rakuten-review-download.mjs、fetch-all 05:30) が incoming/rakuten-review/ に置いた
    // CSVを取り込む (新規★1-2は取込内でGChat通知)。対象0件は正常。
    // --days 35 = downloader の直近30日窓+余裕。mirror へは非PII日次集計のみ
    // retryLibuvCrash: 取込は sha256 冪等 (処理済CSVは processed/ へ移動)、低評価通知はキュー方式
    // (送信済み分は消えている) で再実行が完全に no-op → 一過性 libuv クラッシュの保険を有効化。
    // 2026-07-25 までに 4 回踏んでおり、失敗扱いになると直後の mirror sync が丸ごと落ちる
    const rakutenReviewImportResult = runScript(
      `apps/warehouse/import-rakuten-review.js --data-dir ${DATA_DIR_ARG}`,
      '楽天レビュー 取込', 600000, { retryLibuvCrash: true }
    );
    results.push({ name: '楽天レビュー 取込', ...rakutenReviewImportResult });
    if (rakutenReviewImportResult.success) {
      // retryLibuvCrash: run_id 単位 publish で冪等、GChat 通知なし → 再実行安全
      const rakutenReviewSyncResult = runScript(
        `apps/warehouse/sync-rakuten-review-daily.js --data-dir ${DATA_DIR_ARG} --days 35`,
        '楽天レビュー sync', 600000, { retryLibuvCrash: true }
      );
      results.push({ name: '楽天レビュー sync', ...rakutenReviewSyncResult });
    } else {
      console.log('[DailySync] 楽天レビュー sync は取込失敗のためスキップ (failed/ を確認、修正後に再投入)');
    }

    // === Yahoo レビュー ZIP 取込 (P2-Y PR-Y-A、らくらくーぽん Yahoo版置換) ===
    // 自動DL (yahoo-review-download.mjs、fetch-all) が incoming/yahoo-review/ に置いた 90日全量 ZIP を取り込む。
    // 低評価・identity衝突はキュー方式で GChat。mirror は PR-Y-D。楽天版と同じ理由で retryLibuvCrash
    const yahooReviewImportResult = runScript(
      `apps/warehouse/import-yahoo-review.js --data-dir ${DATA_DIR_ARG}`,
      'Yahooレビュー 取込', 600000, { retryLibuvCrash: true }
    );
    results.push({ name: 'Yahooレビュー 取込', ...yahooReviewImportResult });

    // === Yahoo レビュー campaign planner shadow (P2-Y PR-Y-C1) ===
    // 母集合 = raw_yahoo_orders (上の Yahoo 受注取込で ship_date 入り) の VIEW。送信ゼロ (ownership は vendor)。
    // レビュー取込が失敗しても plan は安全 (材料が減るだけ) なので実行する
    const yahooPlanResult = runScript(
      `apps/warehouse/plan-rakuten-review-campaigns.js plan --mall yahoo --data-dir ${DATA_DIR_ARG}`,
      'Yahooレビュー campaign plan (shadow)', 600000
    );
    results.push({ name: 'Yahooレビュー campaign plan (shadow)', ...yahooPlanResult });

    // === 楽天レビュー contacts 取得 (mall-csv-fetcher P2 PR-B、らくらくーぽん置換) ===
    // 発送日直近25日の注文の「マスクメールアドレス+最終発送日」を暗号化保存 (フォローメールPR-Cの材料)。
    // miniPC内で完結し mirror へは送らない。終了時に期限超過分の purge も実行
    const reviewContactsResult = runScript(
      `apps/warehouse/fetch-rakuten-review-contacts.js fetch --data-dir ${DATA_DIR_ARG} --days 25`,
      '楽天レビューcontacts 取得', 900000
    );
    results.push({ name: '楽天レビューcontacts 取得', ...reviewContactsResult });

    // === 楽天レビュー campaign planner shadow (mall-csv-fetcher P2 PR-C1、らくらくーぽん置換) ===
    // 「らくらくーぽんなら何をいつ送るか」を rakuten_campaign_actions に記録するだけ (送信ゼロ)。
    // contacts が古くても plan 自体は安全なため取得失敗時も実行する (材料が減るだけ)
    const campaignPlanResult = runScript(
      `apps/warehouse/plan-rakuten-review-campaigns.js plan --data-dir ${DATA_DIR_ARG}`,
      '楽天レビュー campaign plan (shadow)', 600000
    );
    results.push({ name: '楽天レビュー campaign plan (shadow)', ...campaignPlanResult });

    // === Yahoo!ストクリ統計CSV 取込 + mirror sync (mall-csv-fetcher P1-Y) ===
    // 自動DL (yahoo-data-download.mjs、fetch-all 05:30) が incoming/yahoo-data/ に置いた
    // CSVを取り込む。対象0件は正常。--days 110 = 全体分析の一括100日レンジをカバー
    const yahooDataImportResult = runScript(
      `apps/warehouse/import-yahoo-data.js --data-dir ${DATA_DIR_ARG}`,
      'Yahoo統計 取込', 600000
    );
    results.push({ name: 'Yahoo統計 取込', ...yahooDataImportResult });
    if (yahooDataImportResult.success) {
      const yahooDataSyncResult = runScript(
        `apps/warehouse/sync-yahoo-data-daily.js --data-dir ${DATA_DIR_ARG} --days 110`,
        'Yahoo統計 sync', 600000
      );
      results.push({ name: 'Yahoo統計 sync', ...yahooDataSyncResult });
    } else {
      console.log('[DailySync] Yahoo統計 sync は取込失敗のためスキップ (failed/ を確認、修正後に再投入)');
    }

    // === au PAYマーケット分析CSV 取込 + mirror sync (mall-csv-fetcher P1-A) ===
    // 自動DL (aupay-data-download.mjs、fetch-all 05:30) が incoming/aupay-data/ に置いた
    // CSV/JSONを取り込む。対象0件は正常。--days 110 = 日次35日再取得+月次前月+PM35日をカバー
    const aupayDataImportResult = runScript(
      `apps/warehouse/import-aupay-data.js --data-dir ${DATA_DIR_ARG}`,
      'auPAY分析 取込', 600000
    );
    results.push({ name: 'auPAY分析 取込', ...aupayDataImportResult });
    if (aupayDataImportResult.success) {
      const aupayDataSyncResult = runScript(
        `apps/warehouse/sync-aupay-data-daily.js --data-dir ${DATA_DIR_ARG} --days 110`,
        'auPAY分析 sync', 600000
      );
      results.push({ name: 'auPAY分析 sync', ...aupayDataSyncResult });
    } else {
      console.log('[DailySync] auPAY分析 sync は取込失敗のためスキップ (failed/ を確認、修正後に再投入)');
    }

    // === Qoo10 Analytics取込 + mirror sync (mall-csv-fetcher P1-Q R1) ===
    // 自動DL (qoo10-data-download.mjs、fetch-all 05:30) が incoming/qoo10-data/ に置いた
    // xlsxを取り込む。対象0件は正常。--days 110 = 月1の90日再取得をカバー
    const qoo10DataImportResult = runScript(
      `apps/warehouse/import-qoo10-data.js --data-dir ${DATA_DIR_ARG}`,
      'Qoo10分析 取込', 600000
    );
    results.push({ name: 'Qoo10分析 取込', ...qoo10DataImportResult });
    if (qoo10DataImportResult.success) {
      const qoo10DataSyncResult = runScript(
        `apps/warehouse/sync-qoo10-data-daily.js --data-dir ${DATA_DIR_ARG} --days 110`,
        'Qoo10分析 sync', 600000
      );
      results.push({ name: 'Qoo10分析 sync', ...qoo10DataSyncResult });
    } else {
      console.log('[DailySync] Qoo10分析 sync は取込失敗のためスキップ (failed/ を確認、修正後に再投入)');
    }

    // === Yahoo finance daily fact (Yahoo Phase 1 Y-3c、Y-1 + Y-2 + Y-3a 統合) ===
    // 1. f_yahoo_finance_sku_daily_v1 build (whitelist order=5/pay=1/ship=3、partial margin)
    // 2. DQ gate (6 check、severity error で exit 1)
    // 3. mirror_yahoo_finance_sku_daily へ sync (chunk POST + ledger)
    // build 失敗で DQ/sync スキップ、DQ gate failure でも sync スキップ (楽天と同方針)
    const yahooFinanceBuildResult = runScript(
      `scripts/yahoo-finance/build-yahoo-daily-fact.js --data-dir ${DATA_DIR_ARG} --month ${currentMonth}`,
      'Yahoo finance build', 600000
    );
    results.push({ name: 'Yahoo finance build', ...yahooFinanceBuildResult });

    if (yahooFinanceBuildResult.success) {
      const yahooFinanceDqResult = runScript(
        `apps/warehouse/run-yahoo-finance-dq.js --data-dir ${DATA_DIR_ARG} --month ${currentMonth}`,
        'Yahoo finance DQ', 300000
      );
      results.push({ name: 'Yahoo finance DQ', ...yahooFinanceDqResult });

      if (yahooFinanceDqResult.success) {
        const yahooFinanceSyncResult = runScript(
          `apps/warehouse/sync-yahoo-finance-daily.js --data-dir ${DATA_DIR_ARG} --month ${currentMonth}`,
          'Yahoo finance sync', 600000
        );
        results.push({ name: 'Yahoo finance sync', ...yahooFinanceSyncResult });
      } else {
        console.log(`[DailySync] Yahoo finance sync は DQ gate failure のため記録せず (品質不正データの mirror 投入防止)`);
      }
    } else {
      console.log(`[DailySync] Yahoo finance DQ/sync は build 失敗のため記録せず (build retry 後に翌 cron で実行)`);
    }

    // === au PAY マーケット finance daily fact (Phase 1 A-2、A-1 build + DQ + A-2 sync 統合) ===
    // 1. f_aupay_finance_sku_daily_v1 build (rolling: 当月 + 前2か月、whitelist orderStatus='完了'+itemCancelStatus='N'、partial margin 5要素)
    //    mall_fee は config/aupay_mall_fee_rates.json 由来の率推計 (現状 13% 暫定)
    // 2. DQ gate (11 check、severity error で exit 1) — build が rolling 3か月なので DQ/sync も同じ 3か月 window で回す
    // 3. mirror_aupay_finance_sku_daily へ sync (chunk POST + ledger) — 月単位、各月独立に DQ→sync
    // build 失敗で DQ/sync スキップ。各月の DQ gate failure でその月の sync のみスキップ (楽天/Yahoo と同方針)
    const aupayFinanceBuildResult = runScript(
      `scripts/aupay-finance/build-aupay-daily-fact.js --data-dir ${DATA_DIR_ARG} --month ${currentMonth}`,
      'au PAY finance build', 600000
    );
    results.push({ name: 'au PAY finance build', ...aupayFinanceBuildResult });

    if (aupayFinanceBuildResult.success) {
      // build-aupay-daily-fact.js の rolling 既定 (当月+前2か月) と同じ window を生成
      const aupayRollingMonths = [0, 1, 2].map((n) => {
        const [y, m] = currentMonth.split('-').map(Number);
        const d = new Date(Date.UTC(y, m - 1 - n, 1));
        return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}`;
      });
      for (const ym of aupayRollingMonths) {
        const aupayFinanceDqResult = runScript(
          `apps/warehouse/run-aupay-finance-dq.js --data-dir ${DATA_DIR_ARG} --month ${ym}`,
          `au PAY finance DQ ${ym}`, 300000
        );
        results.push({ name: `au PAY finance DQ ${ym}`, ...aupayFinanceDqResult });

        if (aupayFinanceDqResult.success) {
          const aupayFinanceSyncResult = runScript(
            `apps/warehouse/sync-aupay-finance-daily.js --data-dir ${DATA_DIR_ARG} --month ${ym}`,
            `au PAY finance sync ${ym}`, 600000
          );
          results.push({ name: `au PAY finance sync ${ym}`, ...aupayFinanceSyncResult });
        } else {
          console.log(`[DailySync] au PAY finance sync ${ym} は DQ gate failure のため記録せず (品質不正データの mirror 投入防止)`);
        }
      }
    } else {
      console.log(`[DailySync] au PAY finance DQ/sync は build 失敗のため記録せず (build retry 後に翌 cron で実行)`);
    }

    // ─── LINEギフト Phase 1 A-3 (Codex R1 critical 反映: DATA_DIR_ARG スコープ内に配置) ───
    // 1. f_linegift_finance_sku_daily_v1 build (rolling: 当月 + 前2か月、whitelist status='received'、3 段 confidence
    //    の Phase A 'provisional_full_candidate'、按分なし、mall_fee は API 実額)
    // 2. DQ gate (13 check、severity error で exit 1) — build と同じ 3か月 window で回す
    // 3. mirror_linegift_finance_sku_daily へ sync (chunk POST + ledger) — 月単位、各月独立に DQ→sync
    // build 失敗で DQ/sync スキップ。各月の DQ gate failure でその月の sync のみスキップ (au PAY と同方針)
    const linegiftFinanceBuildResult = runScript(
      `scripts/linegift-finance/build-linegift-daily-fact.js --data-dir ${DATA_DIR_ARG} --month ${currentMonth}`,
      'LINEギフト finance build', 600000
    );
    results.push({ name: 'LINEギフト finance build', ...linegiftFinanceBuildResult });

    if (linegiftFinanceBuildResult.success) {
      // build-linegift-daily-fact.js の rolling 既定 (当月+前2か月) と同じ window を生成
      const linegiftRollingMonths = [0, 1, 2].map((n) => {
        const [y, m] = currentMonth.split('-').map(Number);
        const d = new Date(Date.UTC(y, m - 1 - n, 1));
        return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}`;
      });
      for (const ym of linegiftRollingMonths) {
        const linegiftFinanceDqResult = runScript(
          `apps/warehouse/run-linegift-finance-dq.js --data-dir ${DATA_DIR_ARG} --month ${ym}`,
          `LINEギフト finance DQ ${ym}`, 300000
        );
        results.push({ name: `LINEギフト finance DQ ${ym}`, ...linegiftFinanceDqResult });

        if (linegiftFinanceDqResult.success) {
          const linegiftFinanceSyncResult = runScript(
            `apps/warehouse/sync-linegift-finance-daily.js --data-dir ${DATA_DIR_ARG} --month ${ym}`,
            `LINEギフト finance sync ${ym}`, 600000
          );
          results.push({ name: `LINEギフト finance sync ${ym}`, ...linegiftFinanceSyncResult });
        } else {
          // Codex R3 medium + R4 medium + R5 medium 反映: DQ failure 時 skipped 行を results に push
          //   skipped:true → GChat 表示が ⏸️ (失敗 ❌ ではなく意図的未実行と明示)
          //   blocked:true → retry filter で除外 (DQ pass までは sync 不可、memory: feedback_blocked_field_for_retry_exclusion.md)
          results.push({
            name: `LINEギフト finance sync ${ym}`,
            success: false,
            skipped: true,
            blocked: true,
            summary: `⏸️ skipped: DQ gate failure ${ym} (品質不正データの mirror 投入防止)`,
          });
          console.log(`[DailySync] LINEギフト finance sync ${ym} は DQ gate failure のため skipped 記録`);
        }
      }
    } else {
      // Codex R3 medium + R4 medium + R5 medium 反映: build failure 時も 3 か月分の DQ/sync の skipped 行を push
      const linegiftRollingMonths = [0, 1, 2].map((n) => {
        const [y, m] = currentMonth.split('-').map(Number);
        const d = new Date(Date.UTC(y, m - 1 - n, 1));
        return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}`;
      });
      for (const ym of linegiftRollingMonths) {
        results.push({ name: `LINEギフト finance DQ ${ym}`, success: false, skipped: true, blocked: true, summary: `⏸️ skipped: build failure (${currentMonth} の build が失敗、翌 cron で retry)` });
        results.push({ name: `LINEギフト finance sync ${ym}`, success: false, skipped: true, blocked: true, summary: `⏸️ skipped: build failure (${currentMonth} の build が失敗、翌 cron で retry)` });
      }
      console.log(`[DailySync] LINEギフト finance DQ/sync 3か月分は build 失敗のため skipped 記録 (build retry 後に翌 cron で実行)`);
    }

    // ─── Qoo10 Phase 1 A-3 (LINEギフト同型、2026-05-19) ───
    // 1. f_qoo10_finance_sku_daily_v1 build (rolling: 当月 + 前2か月、whitelist shipping_status='Delivered(5)')
    // 2. DQ gate (16 check、severity error で exit 1)
    // 3. mirror_qoo10_finance_sku_daily へ sync (chunk POST + ledger)
    // build 失敗で DQ/sync スキップ、各月の DQ failure でその月の sync のみスキップ
    const qoo10FinanceBuildResult = runScript(
      `scripts/qoo10-finance/build-qoo10-daily-fact.js --data-dir ${DATA_DIR_ARG} --month ${currentMonth}`,
      'Qoo10 finance build', 600000
    );
    results.push({ name: 'Qoo10 finance build', ...qoo10FinanceBuildResult });

    if (qoo10FinanceBuildResult.success) {
      const qoo10RollingMonths = [0, 1, 2].map((n) => {
        const [y, m] = currentMonth.split('-').map(Number);
        const d = new Date(Date.UTC(y, m - 1 - n, 1));
        return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}`;
      });
      for (const ym of qoo10RollingMonths) {
        const qoo10FinanceDqResult = runScript(
          `apps/warehouse/run-qoo10-finance-dq.js --data-dir ${DATA_DIR_ARG} --month ${ym}`,
          `Qoo10 finance DQ ${ym}`, 300000
        );
        results.push({ name: `Qoo10 finance DQ ${ym}`, ...qoo10FinanceDqResult });

        if (qoo10FinanceDqResult.success) {
          const qoo10FinanceSyncResult = runScript(
            `apps/warehouse/sync-qoo10-finance-daily.js --data-dir ${DATA_DIR_ARG} --month ${ym}`,
            `Qoo10 finance sync ${ym}`, 600000
          );
          results.push({ name: `Qoo10 finance sync ${ym}`, ...qoo10FinanceSyncResult });
        } else {
          // DQ failure 時: blocked:true (retry 除外) + dq_fail:true (exit 1 トリガー、設計書 v0.11 §9-1)
          results.push({
            name: `Qoo10 finance sync ${ym}`,
            success: false,
            skipped: true,
            blocked: true,
            dq_fail: true,
            summary: `⏸️ skipped: DQ gate failure ${ym} (品質不正データの mirror 投入防止)`,
          });
          console.log(`[DailySync] Qoo10 finance sync ${ym} は DQ gate failure のため skipped 記録 (dq_fail=true、daily-sync は exit 1)`);
        }
      }
    } else {
      // build failure 時も 3 か月分の DQ/sync を skipped 記録 (LINEギフト同型)
      const qoo10RollingMonths = [0, 1, 2].map((n) => {
        const [y, m] = currentMonth.split('-').map(Number);
        const d = new Date(Date.UTC(y, m - 1 - n, 1));
        return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}`;
      });
      for (const ym of qoo10RollingMonths) {
        results.push({ name: `Qoo10 finance DQ ${ym}`, success: false, skipped: true, blocked: true, summary: `⏸️ skipped: build failure (${currentMonth} の build が失敗、翌 cron で retry)` });
        results.push({ name: `Qoo10 finance sync ${ym}`, success: false, skipped: true, blocked: true, summary: `⏸️ skipped: build failure (${currentMonth} の build が失敗、翌 cron で retry)` });
      }
      console.log(`[DailySync] Qoo10 finance DQ/sync 3か月分は build 失敗のため skipped 記録 (build retry 後に翌 cron で実行)`);
    }

    // ─── biz-ops-overview f_sales_by_listing → mirror sync (2026-05-19 patch v2、PR #155 系) ───
    // f_sales_by_listing は f_sales レイヤー (各モール raw API 由来) を rebuild-f-sales.js で集約済。
    // ここで mirror に sync して Render ダッシュボードと GChat 通知 (in-server cron) が同じデータを参照する。
    // 直近 90 日 sync (scope_clear_per_run、当月+前2か月の rolling 範囲)
    // Codex R1 L1 反映: fromDate を分解して明示計算 (= currentMonth から 2 月前の月初)
    const [_fsblY, _fsblM] = currentMonth.split('-').map(Number);
    const _fsblD = new Date(Date.UTC(_fsblY, _fsblM - 1 - 2, 1));
    const fSalesByListingFromDate = `${_fsblD.getUTCFullYear()}-${String(_fsblD.getUTCMonth() + 1).padStart(2, '0')}-01`;
    const fSalesByListingToDate = new Date(Date.now() + 9 * 3600 * 1000).toISOString().slice(0, 10);
    const fSalesByListingResult = runScript(
      `apps/warehouse/sync-f-sales-by-listing.js --data-dir ${DATA_DIR_ARG} --from ${fSalesByListingFromDate} --to ${fSalesByListingToDate}`,
      'f_sales_by_listing sync', 600000
    );
    results.push({ name: 'f_sales_by_listing sync', ...fSalesByListingResult });
  }

  // 楽天 AM/AL/W → NE商品コード sku_map 再構築 (f_sales と独立)
  const rakutenSkuMapResult = runScript('apps/warehouse/rebuild-rakuten-sku-map.js', '楽天 sku_map 再構築');
  results.push({ name: '楽天sku_map', ...rakutenSkuMapResult });

  // Renderにミラーデータ送信。
  // sync-to-render.js は f_sales_by_listing/by_product と f_rakuten_sku_map の両方を Render に送るため、
  // どちらかが失敗していると古い表が Render に押し付けられる → 両方成功時のみ実行
  // → retry-failed-jobs で復旧後に再試行
  let syncResult;
  const fSalesOk = fSalesResult.success;
  const skuMapOk = rakutenSkuMapResult.success;
  if (fSalesOk && skuMapOk) {
    syncResult = runScript('apps/warehouse/sync-to-render.js', 'Render同期');
  } else {
    const reasons = [];
    if (!fSalesOk) reasons.push('f_sales 失敗');
    if (!skuMapOk) reasons.push('楽天sku_map 失敗');
    console.log(`[DailySync] Render同期 スキップ (${reasons.join(', ')}、retry で復旧予定)`);
    syncResult = { success: false, summary: `⏸️ skipped (${reasons.join(', ')})` };
  }
  results.push({ name: 'Render同期', ...syncResult });

  // Amazon手数料カバー率の監視 (SLO 監視、Codex+Claude 議論結果 2026-05-15)
  // 売上加重カバー率 (件数じゃなく金額) を主指標、warn 30日<98%、critical <95% or 売上TOP20未カバー
  // critical で exit 1 → daily-sync 赤、warn/ok は exit 0 (cron 緑)
  // GChat 通知は warn/critical のみ (env GCHAT_WEBHOOK)
  const monitorFeeResult = runScript('apps/warehouse/monitor-fee-coverage.js', '手数料カバー率監視', 60000);
  results.push({ name: '手数料カバー率監視', ...monitorFeeResult });

  // 売上分類別粗利集計 (mgmt-accounting) の売上自動同期は Render 完結（server.js の常駐
  // スケジューラが in-process で実行）に移行したため、daily-sync(miniPC)からの起動は撤去。

  // 月初の自動 月末確定値保存
  // 条件: 今日が月初1〜3日 (JST) かつ アップストリームのデータ取得が全部成功してる
  // (ハンガリ式: FBA snapshot / 在庫集計 / Render同期 が全て成功 = mirror に最新データ揃ってる)
  // 失敗してれば skip + retry 待ちにすることで 「壊れた前月末値が履歴に残る」事故を防ぐ
  // [Codex 設計相談 2026-08-01] 1日のみだと当日失敗で月末snapshotが欠落したままになるため、
  // 1〜3日は毎朝試す自己修復方式に変更。保存済みなら server 側が skip を返すので冪等。
  // (source断面は snapshot_date+1日 = 月初1日朝で固定。2〜3日の実行でも同じ断面を読む)
  const todayJstDay = Number(businessDate.slice(8, 10)); // 'YYYY-MM-DD' の dd
  if (todayJstDay >= 1 && todayJstDay <= 3) {
    const fbaSnapOk = fbaSnapResult.success;
    const invAggOk = invAggResult.success;
    const renderOk = syncResult.success;
    const blockingFails = [];
    if (!fbaSnapOk) blockingFails.push('FBA在庫snapshot');
    if (!invAggOk) blockingFails.push('在庫集計');
    if (!renderOk) blockingFails.push('Render同期');

    if (blockingFails.length === 0) {
      console.log('\n=== 月末確定値の自動保存 (前月末日) ===');
      try {
        const url = (process.env.RENDER_MIRROR_URL || 'https://bfaith-portal.onrender.com/apps/mirror').replace(/\/apps\/mirror\/?$/, '') + '/apps/inventory-monthly/api/save-month-end';
        const syncKey = process.env.MIRROR_SYNC_KEY;
        if (!syncKey) {
          // [Codex R2 #3] MIRROR_SYNC_KEY 未設定は「設定不備の隠蔽」になるので skipped 扱いにしない。
          // 全体通知を ❌ 化させて運用者に気づかせる (本物のスキップは上流失敗時 / 既存snapshotあり時のみ)
          results.push({ name: '月末確定値', success: false, summary: '❌ MIRROR_SYNC_KEY 未設定 (.env に追加してください)' });
        } else {
          const resp = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'x-sync-key': syncKey },
            body: JSON.stringify({}), // 省略時は前月末日を server 側で計算
            signal: AbortSignal.timeout(60000),
          });
          const data = await resp.json().catch(() => ({}));
          if (resp.ok && data.ok && data.skipped) {
            // 既存 snapshot あり (手動入力 pending 等を保護するため上書きしない)
            console.log(`[月末確定値] スキップ: ${data.reason}`);
            results.push({ name: '月末確定値', success: true, skipped: true, summary: `⏸️ ${data.snapshot_date} 既存あり (id=${data.snapshot_id})` });
          } else if (resp.ok && data.ok) {
            const { snapshot_date, source_business_date, totals, partial_categories } = data;
            const partialNote = (partial_categories && partial_categories.length > 0) ? ` (partial: ${partial_categories.join(',')})` : '';
            const sourceNote = source_business_date ? ` (source=${source_business_date}朝)` : '';
            console.log(`[月末確定値] 保存成功: ${snapshot_date} 合計=¥${totals.total.toLocaleString('ja-JP')}${sourceNote}${partialNote}`);
            results.push({ name: '月末確定値', success: true, summary: `${snapshot_date} 合計=¥${totals.total.toLocaleString('ja-JP')}${sourceNote}${partialNote}` });
          } else {
            const err = data.error || `HTTP ${resp.status}`;
            console.error(`[月末確定値] 保存失敗:`, err);
            results.push({ name: '月末確定値', success: false, summary: extractErrorSummary(err) });
          }
        }
      } catch (e) {
        console.error('[月末確定値] 例外:', e.message);
        results.push({ name: '月末確定値', success: false, summary: extractErrorSummary(e.message) });
      }
    } else {
      const msg = `⏸️ skipped (上流失敗: ${blockingFails.join(',')})`;
      console.log(`[DailySync] 月末確定値スキップ: ${msg}`);
      // 上流失敗側は別行で既に ❌ になってるので、ここを失敗扱いにすると同じ事故を二重通知する
      // 意図的スキップ (success=true, skipped=true) として記録
      results.push({ name: '月末確定値', success: true, skipped: true, summary: msg });
    }
  }

  // ─── Yahoo refresh token 期限アラート (残り5日から毎日1通、#958 の GChat 再認可へ誘導) ───
  // 失効すると Yahoo 系 (受注取込・未発送アラート・レビューメール) が全滅するため、切れる前に声をかける。
  // 1日1通の抑止は script 側 (yahoo_token_notify_state)。retry で再実行されても二重送信しない
  // 通知の失敗 (webhook 障害等) では exit 0 を返す実装 = daily-sync 全体を落とさない (Codex R1 High)。
  // RETRYABLE_JOBS にも入れない (翌朝の実行で送れば足りる)
  const yahooTokenAlert = runScript('apps/warehouse/notify-yahoo-token-expiry.js', 'Yahooトークン期限アラート', 120000);
  results.push({ name: 'Yahooトークン期限アラート', ...yahooTokenAlert });

  // ─── 楽天 未発送アラート (出荷漏れの通知) ───
  // 前日12:00 (出荷の締め) までに入金確認できていたのに、まだ発送されていない注文を GChat へ。
  // ・warehouse.db を見ず RMS API を直接読むので、他ステップの成否に影響されない
  // ・楽天RMSは「同じキーで受注変更もできる」= API集約方針によりミニPC経由必須。
  //   Render の node-cron ではなくこの日次ランナーの1ステップとして動かしている
  // ・DBバックアップ (最大6h) より前に置く: バックアップ待ちで朝の通知が遅れないように
  // exit 0 = 正常 / exit 1 = 取得も通知もできず (retry する価値あり) /
  // exit 2 = 通知は送れたが結果が不完全 (検索打ち切り・日時不正・明細欠落)。
  // exit 2 は同じ条件で retry しても結果が変わらず通知だけ重複するので blocked にして
  // 再試行対象から外す (サマリには ❌ で出るので見落とさない)
  const unshippedResult = runScript('apps/rakuten-unshipped/notify-job.js --once', '楽天未発送アラート', 600000);
  results.push({
    name: '楽天未発送アラート',
    ...unshippedResult,
    ...(unshippedResult.exitCode === 2 ? { blocked: true } : {}),
  });

  // ─── Yahoo! 未発送アラート ───
  // 楽天と違い Yahoo受注APIは1件1秒 (VPSプロキシのレート制御) なので、
  // 上の Yahoo 同期で入った warehouse.db から候補を絞り、候補だけAPIで最新確認する。
  // ⚠️DBだけで判定してはいけない (同期は7日窓 = 古い注文の ship_status は更新されない。
  //   実測では候補9件のうち7件が出荷済み・キャンセル済みだった)。終了コードは楽天版と同じ規約
  const yahooUnshippedResult = runScript('apps/yahoo-unshipped/notify-job.js --once', 'Yahoo未発送アラート', 600000);
  results.push({
    name: 'Yahoo未発送アラート',
    ...yahooUnshippedResult,
    ...(yahooUnshippedResult.exitCode === 2 ? { blocked: true } : {}),
  });

  // ─── au PAY 未発送アラート ───
  // Yahoo と同じ二段構え (DBで候補を絞る → APIで最新確認)。auPAY の受注APIは注文ID指定が
  // できないので、候補の**注文日**をピンポイントで検索して引き当てる。
  // ⚠️auPAY の payment_status は「入金済み」ではなく発送後に Y になる値なので判定に使わない。
  //   発送できる状態かどうかは order_status='発送待ち' が正 (service.js 参照)
  const aupayUnshippedResult = runScript('apps/aupay-unshipped/notify-job.js --once', 'auPAY未発送アラート', 900000);
  results.push({
    name: 'auPAY未発送アラート',
    ...aupayUnshippedResult,
    ...(aupayUnshippedResult.exitCode === 2 ? { blocked: true } : {}),
  });

  // ─── Qoo10 未発送アラート ───
  // Qoo10 だけは API 再確認をしない。上の Qoo10 同期が直近90日を毎回取り直しており、
  // さらに**キャンセル済みは受注APIから消える (= last_seen_at が古くなる)** ので、
  // 「今日の同期で確認できた注文」に絞るだけで最新状態を判定できる (service.js 参照)。
  // 同期が失敗した日は判定を見送って exit 1 = retry で拾い直す
  // ⚠️Qoo10同期が失敗した日は判定できない (DBが今日の状態になっていない)。
  //   実行せず失敗として積み、retry (Qoo10同期 → このジョブの順) で拾い直す
  if (qoo10Result.success) {
    const qoo10UnshippedResult = runScript('apps/qoo10-unshipped/notify-job.js --once', 'Qoo10未発送アラート', 300000);
    results.push({ name: 'Qoo10未発送アラート', ...qoo10UnshippedResult });
  } else {
    console.log('[DailySync] Qoo10未発送アラートは Qoo10受注同期の失敗によりスキップ (retryで拾う)');
    results.push({
      name: 'Qoo10未発送アラート',
      success: false,
      summary: '⏸️ Qoo10受注同期の失敗により未実行 (retryで Qoo10同期→判定 の順に再実行)',
    });
  }

  // ─── Yahoo! 問い合わせ対応漏れチェック ───
  // ① 未返信 (出店者回答待ち) ② 返信済みだが「完了する」処理がされていない問い合わせ
  // (通常 + オークション落札前) を検知し、**1件以上あるときだけ**専用スペースへ通知する。
  // 問い合わせ管理API (VPSプロキシの read-only passthrough) を直接読むだけで
  // warehouse.db には依存しない → 他ステップの成否に影響されない。照会は4リクエストのみ。
  // 該当ゼロ=無通知 の仕様なので、実行失敗はジョブ自身が同じ webhook に ❌ を送る
  // (静かな停止と平和な0件を受け手が区別できるように)
  const yahooInquiryResult = runScript('apps/yahoo-inquiry-alert/notify-job.js --once', 'Yahoo問い合わせ対応漏れ', 300000);
  results.push({ name: 'Yahoo問い合わせ対応漏れ', ...yahooInquiryResult });

  // ─── warehouse.db 日次バックアップ (VACUUM INTO → gzip → rclone offsite → 世代管理) ───
  // 全 build/sync の後に実行 (WarehouseServer の並行書き込み分は前後しうる = 厳密断面ではない)。
  // 冪等 (当日分完成済み+元DB変化なしなら再利用して offsite 以降だけやり直す) なので retry 対象。
  // timeout 6h: 月初最悪ケース = rclone 内訳上限合計 約4h (upload90+manifest5+prune10+monthly copy30
  // +prune10+restore DL90分) + VACUUM/gzip/検証/復元展開 約1.5h に余裕を乗せた値 (Codex R2 High#2)
  const backupResult = runScript('apps/warehouse/backup-warehouse.js', 'DBバックアップ', 21600000);
  results.push({ name: 'DBバックアップ', ...backupResult });

  const endTime = new Date();
  const duration = Math.round((endTime - startTime) / 1000);

  // ─── APIトークン・認証情報 期限チェック ───
  const tokenWarnings = [];

  // Yahoo — VPSプロキシのhealthから確認
  try {
    const yahooProxyUrl = process.env.YAHOO_PROXY_URL || 'http://133.167.122.198:8080';
    const yahooProxySecret = process.env.YAHOO_PROXY_SECRET || process.env.AUPAY_PROXY_SECRET || '';
    const healthRes = await fetch(`${yahooProxyUrl}/yahoo/health`, {
      headers: { 'X-Proxy-Secret': yahooProxySecret },
    });
    const health = await healthRes.json();
    if (health.refreshTokenExpiresAt) {
      const expiry = new Date(health.refreshTokenExpiresAt);
      const daysLeft = Math.floor((expiry - new Date()) / 86400000);
      if (daysLeft <= 7) {
        // 再認可は在庫検索ボットへ「yahoo再認可」と送れば URL と手順が出る (apps/stock-bot/yahoo-reauth.js)。
        // ここでも認可 URL を直接添えて 1 タップで始められるようにする
        let authLine = '';
        try {
          const r = await fetch(`${yahooProxyUrl}/yahoo/auth-url`, { headers: { 'X-Proxy-Secret': yahooProxySecret }, signal: AbortSignal.timeout(10000) });
          const j = r.ok ? await r.json() : null;
          if (j?.url) authLine = `\n　→ 再認可: 在庫検索ボットに「yahoo再認可」と送る / または ${j.url} を開いて戻り先 URL をボットに貼る`;
        } catch { /* URL 取得失敗は警告本体だけ出す */ }
        tokenWarnings.push(`${daysLeft <= 3 ? '🔴' : '🟡'} Yahoo refresh token 残り${daysLeft}日（${expiry.toISOString().slice(0, 10)}）${daysLeft <= 3 ? '→ 再認可が必要！' : ''}${authLine}`);
      }
    } else if (health.tokenExpiry) {
      tokenWarnings.push('🟡 Yahoo refresh token 期限不明（次回認可時に記録されます）');
    }
    if (!health.hasTokens) {
      tokenWarnings.push('🔴 Yahoo トークン未初期化 → 認可が必要！');
    }
  } catch (e) {
    // プロキシ死は「トークン期限を確認できない」ではなく「Yahoo取得系が全滅+期限警告も沈黙」なので urgent (🔴) 扱い
    tokenWarnings.push(`🔴 Yahoo プロキシ接続失敗 (取得系全滅・期限確認不能): ${e.message.slice(0, 100)}`);
  }

  // NE（ネクストエンジン） — ne-tokens.jsonの更新日から判定（2日以内に更新必要）
  try {
    const neTokenPath = path.join(PROJECT_DIR, 'data', 'ne-tokens.json');
    if (fs.existsSync(neTokenPath)) {
      const stat = fs.statSync(neTokenPath);
      const hoursSinceUpdate = (Date.now() - stat.mtimeMs) / 3600000;
      if (hoursSinceUpdate > 36) {
        tokenWarnings.push(`🔴 NE トークン ${Math.floor(hoursSinceUpdate)}時間未更新 → 期限切れの恐れ`);
      }
    } else {
      tokenWarnings.push('🔴 NE トークンファイルなし（data/ne-tokens.json）');
    }
  } catch {}

  // 楽天 — ライセンスキー（.envに設定、1年有効、手動で期限管理）
  if (!process.env.RAKUTEN_SERVICE_SECRET || !process.env.RAKUTEN_LICENSE_KEY) {
    tokenWarnings.push('🔴 楽天 SERVICE_SECRET / LICENSE_KEY 未設定');
  }

  // LINEギフト — トークンリフレッシュ可否で判定
  if (!process.env.LINEGIFT_ACCESS_TOKEN) {
    tokenWarnings.push('🔴 LINEギフト アクセストークン未設定');
  } else if (!process.env.LINEGIFT_REFRESH_TOKEN) {
    tokenWarnings.push('🟡 LINEギフト refresh token 未設定（自動更新不可）');
  }

  // Amazon SP-API — refresh tokenは無期限だが設定有無チェック
  if (!process.env.SP_API_REFRESH_TOKEN) {
    tokenWarnings.push('🔴 Amazon SP-API refresh token 未設定');
  }

  // au PAY — APIキー設定チェック
  if (!process.env.AUPAY_API_KEY) {
    tokenWarnings.push('🟡 au PAY APIキー未設定');
  }

  // Qoo10 — APIキー設定チェック
  if (!process.env.QOO10_CERT_KEY) {
    tokenWarnings.push('🟡 Qoo10 CERT_KEY 未設定');
  }

  // メルカリ — トークン設定チェック
  if (!process.env.MERCARI_API_TOKEN) {
    tokenWarnings.push('🟡 メルカリ APIトークン未設定');
  }

  // ─── retry-state 書き込み (リトライ対象の失敗があれば) ───

  const retryableFailed = results
    // blocked:true は「失敗だが構成不備等で retry しても無駄」なので除外 (Codex Round 3 #medium)
    .filter(r => RETRYABLE_JOBS.includes(r.name) && !r.success && !r.blocked)
    .map(r => r.name);

  let retryStateWritten = false;
  let retryStateError = null;
  if (retryableFailed.length > 0) {
    // 失敗あり → state 書き出し (retry_count=0 でリセット、retry-failed-jobs が拾う)。
    // 直接 writeFileSync。中断で破損しても retry-failed-jobs.loadState() が JSON
    // parse 失敗時に検出 + deleteState を呼ぶ self-healing 機構があるので tmp+rename 不要。
    try {
      fs.writeFileSync(RETRY_STATE_FILE, JSON.stringify({
        run_date: dateStr,
        started_at: startTime.toISOString(),
        remaining_jobs: retryableFailed,
        retry_count: 0,
        last_attempt_at: null,
      }, null, 2));
      retryStateWritten = true;
      console.log(`[DailySync] retry-state 書き込み: ${retryableFailed.join(', ')}`);
    } catch (e) {
      retryStateError = e.message;
      console.error('[DailySync] retry-state 書き込み失敗:', e.message);
      // 旧 same-day state を明示削除 (整合性優先、retry されない方がマシ)
      try {
        if (fs.existsSync(RETRY_STATE_FILE)) {
          fs.unlinkSync(RETRY_STATE_FILE);
          console.warn('[DailySync] 整合性確保のため旧 retry-state も削除');
        }
      } catch (delErr) {
        // 書き込み失敗 + 旧 state 削除も失敗 = stale state が残る危険な状態
        stateOpWarnings.push(`🔴 retry-state 書き込み失敗 + 旧 state 削除も失敗 (write: ${e.message}, del: ${delErr.message})、stale 内容で retry が誤実行する恐れあり。手動削除を: ${RETRY_STATE_FILE}`);
        console.error('[DailySync] 旧 retry-state 削除も失敗:', delErr.message);
      }
    }
  } else {
    // 全成功 → 同日に残っている可能性がある古い state を削除
    // (例: 7:00 で失敗→state作成、同日中に手動再実行→全成功 のケース)
    try {
      if (fs.existsSync(RETRY_STATE_FILE)) {
        fs.unlinkSync(RETRY_STATE_FILE);
        console.log('[DailySync] 全成功のため残存していた retry-state を削除');
      }
    } catch (e) {
      // 削除失敗時、古い state が残って 8:30 retry が stale 内容で誤実行するリスクあり。
      stateOpWarnings.push(`🔴 全成功時の retry-state クリーンアップ失敗 (${e.message})、stale 内容で retry が誤実行する恐れあり。手動削除を: ${RETRY_STATE_FILE}`);
      console.error('[DailySync] retry-state クリーンアップ失敗:', e.message);
    }
  }

  // 通知メッセージ作成
  // 期限切れ系（🔴🟡）のみ通知に含め、設定チェック系は同期失敗時のみ表示
  const urgentWarnings = tokenWarnings.filter(w => w.startsWith('🔴') || w.startsWith('🟡'));
  const diskWarnings = (diskInfo && diskInfo.warnings) ? diskInfo.warnings : [];
  const allOk = results.every(r => r.success) && urgentWarnings.length === 0 && diskWarnings.length === 0;
  const icon = allOk ? '✅' : '⚠️';
  let msg = `${icon} *Warehouse日次同期 ${dateStr}* (${duration}秒)\n`;
  for (const r of results) {
    const icon = r.skipped ? '⏸️' : (r.success ? '✅' : '❌');
    msg += `${icon} ${r.name}: ${r.summary}\n`;
  }
  if (retryableFailed.length > 0) {
    if (retryStateWritten) {
      msg += `\n🔄 自動再試行予定: ${retryableFailed.join(', ')} を本日 8:30 / 10:00 / 11:30 JST に再実行\n`;
    } else {
      msg += `\n⚠️ retry-state 書き込み失敗 (${retryStateError})、自動再試行されません。手動対応必要\n`;
    }
  }
  // state 操作の失敗 (起動時 cleanup / 書き込み失敗時旧削除 / 全成功時 cleanup) を通知
  for (const w of stateOpWarnings) {
    msg += `\n${w}\n`;
  }
  if (urgentWarnings.length > 0) {
    msg += `\n*⏰ 認証情報アラート:*\n`;
    for (const w of urgentWarnings) msg += `${w}\n`;
  }
  if (diskWarnings.length > 0) {
    msg += `\n*💾 ディスク容量アラート:*\n`;
    for (const w of diskWarnings) msg += `${w}\n`;
  }

  console.log('\n' + msg);  // ローカルログには全文を残す
  // GChat は 4096 字上限。多重失敗で本文が超えると HTTP 400 で最重要通知そのものが落ちる
  // (Codex R2 High #1)。上限手前で切り、末尾に切り詰めマーカー + 完全版はログ参照を明示。
  const GCHAT_MAX = 3900;
  let notifyMsg = msg;
  if (notifyMsg.length > GCHAT_MAX) {
    notifyMsg = notifyMsg.slice(0, GCHAT_MAX) + `\n…[本文を切り詰めました。全文は daily-sync ログ参照]`;
  }
  const notifyOk = await notify(notifyMsg);

  console.log(`[DailySync] 完了: ${endTime.toISOString()}`);

  // Codex (Qoo10 A-3) Critical #1 + R2 High #1 + R3 Low #1 反映: DQ gate failure 時のみ exit non-zero。
  //   設計書 v0.11 §9-1: 「DQ error 時 mirror sync 中止 + exit non-zero」
  //   ただし既存 blocked:true は retry-failed-jobs.js の除外用途 (memory: feedback_blocked_field_for_retry_exclusion.md)
  //   = build failure (一時障害、翌 cron で build retry → 連鎖回復) も blocked:true で積んでる用途混在。
  //   → 新フラグ dq_fail:true を「DQ gate failure (恒久ブロック)」のみに立てて exit 1 はそれに紐付ける。
  //   blocked セマンティクスは既存 (retry 除外) のまま維持、retry filter 挙動への影響なし。
  // ⚠️ TODO (Codex R3 Low #1): 現状 dq_fail を立ててるのは Qoo10 のみ。LINEギフト/au PAY/楽天/Yahoo の
  //   DQ failure 経路にも同フラグ立てを横展開する別 PR を切ること。pushDqFailureResult() ヘルパー化推奨。
  const hasDqFail = results.some(r => r.dq_fail === true);
  // lock 解放は「通知が届いた」or「全成功かつ届けるべき警告が無い」時のみ (Codex R7/R8 High)。
  // 失敗・stateOpWarnings (前回異常終了の未達警告を含む) ありの朝に通知が届かなかった場合は lock を残す →
  // DQ-fail (exit 1) なら bat の notify-crash.js が、通常時 (exit 0) でも翌朝の起動時検知が 🔴 でフォールバック通知する。
  // 全成功 + 警告なし + 通知失敗は lock を解放する (run 自体は正常完了、残すと翌朝「異常終了」の誤検知になる)
  if (notifyOk || (allOk && stateOpWarnings.length === 0)) releaseLock();
  if (hasDqFail) {
    const dqFailNames = results.filter(r => r.dq_fail === true).map(r => r.name).join(', ');
    console.error(`[DailySync] exit 1: DQ gate failure あり (設計書 v0.11 §9-1、mirror sync 中止): ${dqFailNames}`);
    process.exit(1);
  }
}

main().catch(async (e) => {
  console.error('[DailySync] 致命的エラー:', e.message);
  const notified = await notify(`❌ *Warehouse日次同期 失敗*\n${e.message}`);
  // 通知が実際に届いた時だけ lock を解放 (Codex R2 High #3)。
  // 届かなかった場合は lock を残す → bat の notify-crash.js / 翌朝の起動時検知がフォールバックで拾う
  if (notified) releaseLock();
  process.exit(1);
});
