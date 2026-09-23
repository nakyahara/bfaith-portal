/**
 * ABA「Amazon検索用語 (Top Search Terms)」週次レポート取込
 *
 * SP-API GET_BRAND_ANALYTICS_SEARCH_TERMS_REPORT (要: ブランド登録 + Brand Analytics ロール)
 * を取得する。セラースプライト「注文ワード」の置換 (2026-07-27)。
 *
 * 蓄積しない設計 (中原さん方針):
 *   - 既定 (ABA_INGEST_MODE=watched): 監視ASIN (aba_watch_asins = 拡張で照会した/企画候補
 *     として登録したASIN) を含む検索語グループだけ DB に保存し、残りは捨てる。
 *     DBは数MB規模。レポート本体は最新週のファイル1本だけ data/aba-reports/ に残し、
 *     未知ASINの初回照会時に router がその場でスキャンする
 *   - ABA_INGEST_MODE=full にすると全検索語を保存 (任意ASINが即答になる代わりにGB級)。
 *     その場合の保持は ABA_KEEP_WEEKS 週 (監視ASINを含む語は無期限)
 *
 * 実行: node apps/aba-keywords/fetch-aba-search-terms.js [--week YYYY-MM-DD] [--backfill N] [--dry-run] [--force] [--budget-min M]
 *   - 引数なし: 直近の「完了した週 (日曜〜土曜, **UTC**)」を既定 3 週 (--backfill) 対象にする。
 *     🚨 ABA の週境界は UTC (createReport の期間も UTC の日曜 00:00:00Z〜土曜 23:59:59Z。JST で送ると FATAL。2026-09-23)
 *   - 処理済みの週は即スキップ (冪等)。daily-sync から毎朝呼んでも新しい週だけ処理
 *   - DONE にならない週はレポート文書の理由文を読んで分類: 引数/設定/権限の誤り → 即ハード失敗 / 週末から 10 日過ぎ → ハード失敗 / それ以外 (集計中) → 正常 skip
 *   - 🚨 監視ASINを後から増やしても、取込済みの週は台帳で skip されるので --backfill では埋まらない。
 *     埋め直すには `--week <日曜> --force` (その週の行と台帳を消してから取り直す。レポートは Amazon 側に残っている)
 *   - 1 回の実行は --budget-min (既定 50 分) の中で週を処理し、残りは次回に持ち越す (daily-sync の 60 分制限の内側)
 *
 * メモリ注意: レポートは数百MB級。全体を JSON.parse せず streaming で流し込む
 * (miniPC は AES sorter OOM の前科があるため常に省メモリ側に倒す)。
 */
import 'dotenv/config';
import fs from 'fs';
import path from 'path';
import zlib from 'zlib';
import { fileURLToPath } from 'url';
import SellingPartner from 'amazon-sp-api';
import { initAbaDB, closeAbaDB } from './db.js';
import { streamTermGroups } from './aba-report-parser.js';
import { downloadReportToFile, openReportStream, cleanupReportFiles } from './report-store.js';

const REPORT_TYPE = 'GET_BRAND_ANALYTICS_SEARCH_TERMS_REPORT';
const MODE = process.env.ABA_INGEST_MODE === 'full' ? 'full' : 'watched';
// fullモード時のみ: 全検索語の保持週数 (監視ASINを含む語は無期限)
const KEEP_WEEKS = Math.max(4, parseInt(process.env.ABA_KEEP_WEEKS || '8', 10) || 8);
// 商品タイトルは1行あたりの容量を数倍にする割に拡張UIでは未使用のため既定で保存しない
const STORE_TITLES = process.env.ABA_STORE_TITLES === '1';
const POLL_INTERVAL_MS = 15000;
const POLL_MAX = 40;            // 15s × 40 = 10 分 (実測 = 1.5 分で DONE。30 分待つと 3 週で親の 60 分制限に収まらない。Codex #1411)
// 1 週に「普通」かかる時間 + 余裕 (polling 1.5 分・DL 数分・解析 数分)。着手の見積もりに使う。
// 🚨 予算の本体は deadline: polling・DL・解析の途中でもこの時刻を過ぎたら打ち切って次回へ (見積もりが外れても親の 60 分は越えない)
const PER_WEEK_RESERVE_MS = 12 * 60 * 1000;

// ---- 引数 ----
const args = process.argv.slice(2);
const DRY_RUN = args.includes('--dry-run');
function argValue(name) {
  const i = args.indexOf(name);
  return i !== -1 && args[i + 1] ? args[i + 1] : null;
}
const WEEK_ARG = argValue('--week');
// 🚨 既定 3 週 (1 だと「直近の完了週が未公開 → 日曜に対象が次週へ回転 → 前の週は二度と試されない」で永遠に 0 週。2026-09-23)。
//    取込済みの週は台帳で即 skip なので、余分に見る分の負担は createReport 1 回だけ
const BACKFILL = Math.min(12, Math.max(1, parseInt(argValue('--backfill') || '3', 10) || 3));
const FORCE = args.includes('--force');
// 1 回の実行の時間予算。daily-sync は 60 分で子プロセスを切るので、その内側で週を処理し、残りは次回へ持ち越す
const RUN_BUDGET_MS = Math.max(5, parseInt(argValue('--budget-min') || '50', 10) || 50) * 60 * 1000;

/** 残りの週に着手してよいか。予算の残りが「1 週に普通かかる時間 + 余裕」より少なければ持ち越す (見積もり。本体は deadline による打ち切り) */
export function shouldDefer(elapsedMs, budgetMs = RUN_BUDGET_MS, reserveMs = PER_WEEK_RESERVE_MS) {
  return budgetMs - elapsedMs < reserveMs;
}
/** 予算が 1 週分の予約時間より短いと毎回何もせず持ち越すので、その予算は受け付けない */
export function budgetIsUsable(budgetMs, reserveMs = PER_WEEK_RESERVE_MS) {
  return budgetMs >= reserveMs;
}
/**
 * この週に着手してよいか。**最初の未処理週は必ず着手する** (起動時に予算を検証済み。台帳照会までの数 ms で
 * 残りが予約時間を割っても止まらない — Codex #1411 R3)。2 週目以降は残りが予約時間以上のときだけ
 */
export function canStartWeek({ mustStart, remainingMs, reserveMs = PER_WEEK_RESERVE_MS }) {
  return mustStart || remainingMs >= reserveMs;
}

// ---- 週計算。🚨 ABA の週は UTC の日曜〜土曜 (createReport の期間も UTC)。「完了した週」も UTC で判定する
//      (JST で判定すると日曜 09:00 JST より前は UTC ではまだ土曜 = 未完了の週を要求してしまう。Codex #1411)
function fmt(dateUtcMs) {
  const t = new Date(dateUtcMs);
  const p = (n) => String(n).padStart(2, '0');
  return `${t.getUTCFullYear()}-${p(t.getUTCMonth() + 1)}-${p(t.getUTCDate())}`;
}
/** 直近の完了済み週 (日曜開始) を新しい順に n 件返す */
export function recentCompletedWeeks(n, nowMs = Date.now()) {
  const t = new Date(nowMs);
  const todayMs = Date.UTC(t.getUTCFullYear(), t.getUTCMonth(), t.getUTCDate());   // UTC の今日
  // 直近の「終わった土曜 (UTC)」: 今日から遡って最初の土曜 (今日が土曜でも当週は 23:59:59Z まで未完了なので昨日から遡る)
  let sat = todayMs - 86400000;
  while (new Date(sat).getUTCDay() !== 6) sat -= 86400000;
  const weeks = [];
  for (let i = 0; i < n; i++) {
    const end = sat - i * 7 * 86400000;
    const start = end - 6 * 86400000;
    weeks.push({ weekStart: fmt(start), weekEnd: fmt(end) });
  }
  return weeks;
}
function weekFromStart(weekStart) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(weekStart)) throw new Error(`--week は YYYY-MM-DD 形式: ${weekStart}`);
  const [y, m, d] = weekStart.split('-').map(Number);
  const ms = Date.UTC(y, m - 1, d);
  // Date.UTC は 2026-02-29 等を翌月へ繰り上げるため、往復一致で実在日を検証 (Codex R1 low)
  if (fmt(ms) !== weekStart) throw new Error(`--week が実在しない日付: ${weekStart}`);
  if (new Date(ms).getUTCDay() !== 0) throw new Error(`--week は日曜日を指定: ${weekStart} は ${'日月火水木金土'[new Date(ms).getUTCDay()]}曜`);
  return { weekStart, weekEnd: fmt(ms + 6 * 86400000) };
}

// ---- SP-API ----
let spClient = null;
function getClient() {
  if (!process.env.SP_API_REFRESH_TOKEN || !process.env.SP_API_CLIENT_ID || !process.env.SP_API_CLIENT_SECRET) {
    throw new Error('SP-API 認証情報 (SP_API_REFRESH_TOKEN / SP_API_CLIENT_ID / SP_API_CLIENT_SECRET) が未設定');
  }
  if (!spClient) {
    spClient = new SellingPartner({
      region: 'fe',
      refresh_token: process.env.SP_API_REFRESH_TOKEN,
      credentials: {
        SELLING_PARTNER_APP_CLIENT_ID: process.env.SP_API_CLIENT_ID,
        SELLING_PARTNER_APP_CLIENT_SECRET: process.env.SP_API_CLIENT_SECRET,
        AWS_ACCESS_KEY_ID: process.env.AWS_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY: process.env.AWS_SECRET_ACCESS_KEY,
      },
    });
  }
  return spClient;
}
const MARKETPLACE_ID = process.env.SP_API_MARKETPLACE_ID || 'A1VC38T7YXB528';

/**
 * createReport に渡す期間。🚨 **UTC の日曜 00:00:00Z 〜 土曜 23:59:59Z**。
 * JST (+09:00) で送ると Amazon は UTC の土曜 15:00 と解釈し「dataStartTime must be a Sunday when reportPeriod=WEEK」で
 * FATAL になる (2026-09-23 に実レポートの理由文で確認。JP マーケットプレイスでも境界は UTC)
 */
export function reportPeriodBody(weekStart, weekEnd) {
  return { dataStartTime: `${weekStart}T00:00:00Z`, dataEndTime: `${weekEnd}T23:59:59Z` };
}

/**
 * DONE にならなかったレポートの理由文 (reportDocument の errorDetails)。読めなければ null
 */
const FAILURE_DOC_MAX_BYTES = 64 * 1024;   // 理由文の文書は数十バイト。想定外に大きいものは読まない (メモリを守る)
async function readReportFailureReason(sp, report) {
  if (!report?.reportDocumentId) return null;
  try {
    const doc = await withTimeout(sp.callAPI({
      operation: 'getReportDocument', endpoint: 'reports',
      path: { reportDocumentId: report.reportDocumentId },
      options: { version: '2021-06-30' },
    }), 30000, 'getReportDocument(failure)');
    const res = await fetch(doc.url, { signal: AbortSignal.timeout(30000) });
    if (!res.ok) return null;   // 配信側 (S3) の HTTP エラー本文を Amazon の理由文として扱わない
    // 受信は上限つき (全体を読み切ってから切らない)
    const reader = res.body.getReader();
    const chunks = []; let total = 0;
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > FAILURE_DOC_MAX_BYTES) { try { await reader.cancel(); } catch (_) { /* 打ち切り */ } return '(理由文の文書が大きすぎて読まなかった)'; }
      chunks.push(Buffer.from(value));
    }
    let buf = Buffer.concat(chunks);
    if (doc.compressionAlgorithm === 'GZIP') buf = zlib.gunzipSync(buf, { maxOutputLength: FAILURE_DOC_MAX_BYTES });   // 解凍後も上限
    const text = buf.toString('utf8').slice(0, 2000);
    try { const j = JSON.parse(text); if (j && typeof j.errorDetails === 'string') return j.errorDetails; } catch (_) { /* JSON でない */ }
    return text.replace(/\s+/g, ' ').trim() || null;
  } catch (e) {
    return null;
  }
}

/**
 * 失敗の分類: 'config' = 引数/設定/権限の誤り (即ハード失敗) / 'stale' = 週末から 10 日過ぎても未生成 (恒久障害を疑う) / 'unpublished' = 集計中 (正常 skip)。
 * 'config' の語は実際に観測した理由文 (「dataStartTime must be a Sunday」) と権限系に絞る。未観測の文言は 10 日ルールに任せる (Codex #1411)
 */
export function classifyReportFailure(reason, daysSinceWeekEnd) {
  if (reason && /must be a (sunday|saturday)|invalid|not allowed|not permitted|not supported|unsupported|forbidden|denied|unauthorized|not authorized|malformed/i.test(reason)) return 'config';
  if (daysSinceWeekEnd > 10) return 'stale';
  return 'unpublished';
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function withTimeout(promise, ms, label = '') {
  return Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error(`タイムアウト (${ms / 1000}秒): ${label}`)), ms)),
  ]);
}

/**
 * 1週分を取得して処理。
 * @returns {'ingested'|'already'|'unavailable'} 結果種別
 */
/** 時間予算で打ち切ったときの合図 (失敗ではない。台帳に書かず次回に持ち越す) */
class DeferError extends Error { constructor(where) { super(`時間予算の期限に達したため ${where} で打ち切り (次回に持ち越す)`); this.name = 'DeferError'; } }
const pastDeadline = (deadline) => Number.isFinite(deadline) && Date.now() >= deadline;

async function ingestWeek(db, { weekStart, weekEnd }, { deadline = Infinity, mustStart = true } = {}) {
  // 台帳に行がある = 処理完了済み (台帳は処理がすべて成功した後にだけ書くので、中断時は残らない)
  const existing = db.prepare('SELECT row_count, parsed_count FROM aba_weeks WHERE week_start = ?').get(weekStart);
  if (existing && !FORCE) {
    console.log(`[ABA] ${weekStart}〜${weekEnd}: 処理済み (全${existing.parsed_count}行/保存${existing.row_count}行) → skip`);
    return 'already';
  }
  // 時間予算: 処理済みの判定のあとで見る (skip は予算を食わない)。最初の未処理週は必ず着手し、2 週目以降は残りが 1 週分に足りなければ持ち越す。
  // 🚨 予算の本体はこの見積もりではなく、polling・DL・解析の途中でも deadline で打ち切ること (Codex #1411 R2/R3)
  if (Number.isFinite(deadline) && !canStartWeek({ mustStart, remainingMs: deadline - Date.now() })) return 'deferred';
  if (existing && FORCE) {
    // --force = 監視 ASIN を増やしたあと等に、その週を取り直す。🚨 消すのは新しい結果を書くトランザクションの中
    //    (取得・解析が失敗したら既存の行と台帳はそのまま。Codex #1411 R2)
    console.log(`[ABA] ${weekStart}〜${weekEnd}: 処理済み (保存${existing.row_count}行) だが --force → 取り直して置き換える`);
  }

  const sp = getClient();
  console.log(`[ABA] ${weekStart}〜${weekEnd}: createReport ...`);
  const created = await withTimeout(sp.callAPI({
    operation: 'createReport', endpoint: 'reports',
    body: {
      reportType: REPORT_TYPE,
      marketplaceIds: [MARKETPLACE_ID],
      reportOptions: { reportPeriod: 'WEEK' },
      ...reportPeriodBody(weekStart, weekEnd),
    },
    options: { version: '2021-06-30' },
  }), 60000, 'createReport');

  let report;
  for (let i = 0; i < POLL_MAX; i++) {
    if (pastDeadline(deadline)) { console.log(`[ABA] ${weekStart}: 時間予算の期限 → polling を打ち切り (次回に持ち越す)`); return 'deferred'; }
    await sleep(POLL_INTERVAL_MS);
    report = await withTimeout(sp.callAPI({
      operation: 'getReport', endpoint: 'reports',
      path: { reportId: created.reportId },
      options: { version: '2021-06-30' },
    }), 15000, 'getReport');
    if (i % 4 === 0 || ['DONE', 'FATAL', 'CANCELLED'].includes(report.processingStatus)) {
      console.log(`[ABA] ポーリング ${i + 1}/${POLL_MAX}: ${report.processingStatus}`);
    }
    if (['DONE', 'FATAL', 'CANCELLED'].includes(report.processingStatus)) break;
  }
  if (report.processingStatus !== 'DONE') {
    // 🚨 FATAL/CANCELLED を「未公開」と決めつけない (2026-09-23: JST 境界の送り方が原因の FATAL を 8 週間「未公開」として
    //    緑で skip し続けた)。レポート文書に理由文 (errorDetails) が入るので、まず読んで分類する:
    //    - 設定・権限・引数の誤り (「must be a Sunday」等) → 即ハード失敗 (顕在化)
    //    - 週末から 10 日過ぎても DONE にならない → 恒久障害を疑ってハード失敗
    //    - それ以外 (直近週の集計中) → 正常 skip。理由文はログに残す
    const reason = await readReportFailureReason(sp, report);
    const [ey, em, ed] = weekEnd.split('-').map(Number);
    const daysSinceWeekEnd = Math.floor((Date.now() + 9 * 3600 * 1000 - Date.UTC(ey, em - 1, ed)) / 86400000);
    const kind = classifyReportFailure(reason, daysSinceWeekEnd);
    if (kind === 'config') {
      throw new Error(`${weekStart}週のレポートが ${report.processingStatus} — 引数/設定/権限の誤り: ${reason}`);
    }
    if (kind === 'stale') {
      throw new Error(`${weekStart}週のレポートが週末から${daysSinceWeekEnd}日経っても ${report.processingStatus} (${reason || '理由文なし'}) — 未公開ではなく設定/権限問題を疑う`);
    }
    console.log(`[ABA] ${weekStart}: レポート未生成 (${report.processingStatus}${reason ? `: ${reason}` : ''}) → skip (公開後に自動取込)`);
    return 'unavailable';
  }

  const doc = await withTimeout(sp.callAPI({
    operation: 'getReportDocument', endpoint: 'reports',
    path: { reportDocumentId: report.reportDocumentId },
    options: { version: '2021-06-30' },
  }), 30000, 'getReportDocument');

  // ファイルへ保存してから解析 (初回照会スキャンで再利用するため + ネットワーク切断と
  // DB トランザクションを分離するため)
  console.log(`[ABA] ダウンロード開始 (compression=${doc.compressionAlgorithm || 'none'})`);
  if (pastDeadline(deadline)) { console.log(`[ABA] ${weekStart}: 時間予算の期限 → DL 前に打ち切り (次回に持ち越す)`); return 'deferred'; }
  let filePath;
  try {
    filePath = await downloadReportToFile(doc.url, doc.compressionAlgorithm === 'GZIP', weekStart,
      { timeoutMs: Number.isFinite(deadline) ? deadline - Date.now() : undefined });
  } catch (e) {
    if (e && e.name === 'TimeoutError' && pastDeadline(deadline)) { console.log(`[ABA] ${weekStart}: 時間予算の期限 → DL を打ち切り (次回に持ち越す)`); return 'deferred'; }
    throw e;
  }
  const fileMB = Math.round(fs.statSync(filePath).size / 1e6);
  console.log(`[ABA] 保存完了: ${filePath} (${fileMB}MB) → 解析開始 (mode=${MODE})`);

  if (DRY_RUN) {
    let groups = 0, rows = 0;
    const { skipped } = await streamTermGroups(openReportStream(filePath), (g) => { groups++; rows += g.length; });
    console.log(`[ABA] dry-run: ${groups}検索語 / ${rows}行 (skip=${skipped}、書き込みなし)`);
    return 'ingested';
  }

  // 取込開始時点の監視ASINスナップショット (両モードで使用)
  const watchAsins = db.prepare('SELECT asin FROM aba_watch_asins').all().map(r => r.asin);
  const watchSet = new Set(watchAsins);

  const insert = db.prepare(`
    INSERT OR REPLACE INTO aba_search_terms
      (week_start, department, search_term, search_frequency_rank, click_position,
       asin, product_title, click_share, conversion_share)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);
  const insertLedger = db.prepare(`
    INSERT OR REPLACE INTO aba_weeks (week_start, week_end, ingested_at, term_count, row_count, parsed_count)
    VALUES (?, ?, datetime('now'), ?, ?, ?)
  `);
  // ⚠ スナップショットのASINだけを走査済みにする。全体UPDATEにすると解析中に
  // router 側で登録されたASIN (このパスでは拾っていない) まで「出現なし」で確定し、
  // 次週まで欠落する (Codex R6 high)。取込中登録分は初回照会スキャンが新ファイルを見る
  const markScanned = db.prepare(`
    UPDATE aba_watch_asins SET last_scanned_week = ?
    WHERE asin = ? AND (last_scanned_week IS NULL OR last_scanned_week < ?)
  `);
  const zeroParsedError = (skippedRows) => new Error(skippedRows > 0
    ? `全${skippedRows}行が必須フィールド欠落で skip — レポート形式が想定と異なる (normalizeAbaItem 要確認)`
    : '解析0行 (空レポート) — ABA週次が空になることは想定外のため要確認');

  let parsed = 0, kept = 0, termsKept = 0, skippedRows = 0;

  try {
    if (MODE === 'watched') {
      // watched: 該当行だけメモリに集め、書き込みは短い1トランザクションで確定する。
      // 解析中 (数分〜数十分) に書込トランザクションを張りっぱなしにすると、WAL でも
      // writer は単一のため router 側の watch 登録/キャッシュ書込が busy で失敗し続ける
      // (Codex R6 medium)。監視分は高々数万行なのでメモリで安全
      const keptRows = [];
      let checked = 0;
      const { skipped } = await streamTermGroups(openReportStream(filePath), (group) => {
        parsed += group.length;
        if ((++checked & 1023) === 0 && pastDeadline(deadline)) throw new DeferError('解析');   // 解析の途中でも期限で打ち切る
        if (!group.some(r => watchSet.has(r.asin))) return;
        keptRows.push(...group);
        termsKept++;
      });
      skippedRows = skipped;
      // 全行 parse 不能は台帳に書かずハード失敗 (書くと「処理済み」でサイレント欠落が確定する)
      if (parsed === 0) throw zeroParsedError(skippedRows);

      db.transaction(() => {
        // ⚠ watched では週の先頭 DELETE をしない (Codex R7 high):
        // この週の既存行は「router の初回照会スキャンが同じレポートファイルから拾った行」
        // だけであり (台帳存在チェックで再取込は無い)、DELETE すると解析中に完了した
        // 並走スキャンの成果を消して last_scanned_week だけ残る = 次週まで欠落する。
        // 同一ファイル由来なので INSERT OR REPLACE で同一PKに収束し、消す理由がない。
        // 例外 = --force (人が手で流す取り直し): 新しい結果がそろったこのトランザクションの中で置き換える。
        // 🚨 消すのは**スナップショットの監視 ASIN の行だけ** (週全体ではない)。解析中に登録された ASIN を router が並走スキャンで
        //    保存した行は keptRows に無いので、週全体を消すと復元されず last_scanned_week だけ残って欠落が続く (Codex #1411 R3)。
        //    スナップショット分は keptRows で INSERT OR REPLACE され、レポートから消えた語だけが減る
        if (FORCE && existing) {
          const delMine = db.prepare('DELETE FROM aba_search_terms WHERE week_start = ? AND asin = ?');
          for (const a of watchAsins) delMine.run(weekStart, a);
          db.prepare('DELETE FROM aba_weeks WHERE week_start = ?').run(weekStart);
        }
        for (const r of keptRows) {
          insert.run(weekStart, r.department, r.search_term, r.search_frequency_rank,
            r.click_position, r.asin, STORE_TITLES ? r.product_title : null,
            r.click_share, r.conversion_share);
        }
        kept = keptRows.length;
        insertLedger.run(weekStart, weekEnd, termsKept, kept, parsed);
        for (const a of watchAsins) markScanned.run(weekStart, a, weekStart);
      })();
    } else {
      // full: 数百万行はメモリに置けないため streaming insert + 明示 BEGIN/COMMIT
      // (.transaction() は async を跨げない)。行データと台帳を単一txnで確定 (Codex R1 medium)。
      // この間 router の書き込みは busy になり得るが、full は opt-in かつ朝バッチ帯のみ
      db.exec('BEGIN IMMEDIATE');
      try {
        db.prepare('DELETE FROM aba_search_terms WHERE week_start = ?').run(weekStart);
        if (FORCE && existing) db.prepare('DELETE FROM aba_weeks WHERE week_start = ?').run(weekStart);   // --force: 台帳もこの txn の中で置き換える
        let checked = 0;
        const { skipped } = await streamTermGroups(openReportStream(filePath), (group) => {
          parsed += group.length;
          if ((++checked & 1023) === 0 && pastDeadline(deadline)) throw new DeferError('解析');   // 期限で打ち切り → ROLLBACK (何も書かない)
          for (const r of group) {
            insert.run(weekStart, r.department, r.search_term, r.search_frequency_rank,
              r.click_position, r.asin, STORE_TITLES ? r.product_title : null,
              r.click_share, r.conversion_share);
            kept++;
          }
          termsKept++;
        });
        skippedRows = skipped;
        if (parsed === 0) throw zeroParsedError(skippedRows);
        insertLedger.run(weekStart, weekEnd, termsKept, kept, parsed);
        for (const a of watchAsins) markScanned.run(weekStart, a, weekStart);
        db.exec('COMMIT');
      } catch (e) {
        try { db.exec('ROLLBACK'); } catch { /* BEGIN前の失敗等は無視 */ }
        throw e;
      }
    }
  } catch (e) {
    if (e instanceof DeferError) {
      // 時間予算で打ち切った = 失敗ではない。ファイルは次回そのまま使えるので残す (台帳は書いていないので次回取り直す)
      console.log(`[ABA] ${weekStart}: ${e.message}`);
      return 'deferred';
    }
    // 解析に失敗したファイルはスキャンにも使えないため残さない
    try { fs.unlinkSync(filePath); } catch { /* 無ければ無視 */ }
    throw e;
  }

  console.log(`[ABA] ${weekStart}: 処理完了 全${parsed}行 → 保存${kept}行/${termsKept}検索語 (mode=${MODE}, 不正行skip=${skippedRows})`);
  return 'ingested';
}

/** fullモードのみ: 保持期間を過ぎた週から、監視ASINを含まない検索語を削除 */
function pruneOldWeeks(db, anchorWeekStart) {
  const [y, m, d] = anchorWeekStart.split('-').map(Number);
  const cutoff = fmt(Date.UTC(y, m - 1, d) - KEEP_WEEKS * 7 * 86400000);
  const result = db.prepare(`
    DELETE FROM aba_search_terms
    WHERE week_start < ?
      AND NOT EXISTS (
        SELECT 1 FROM aba_search_terms t2
        WHERE t2.week_start = aba_search_terms.week_start
          AND t2.department = aba_search_terms.department
          AND t2.search_term = aba_search_terms.search_term
          AND t2.asin IN (SELECT asin FROM aba_watch_asins)
      )
  `).run(cutoff);
  if (result.changes > 0) {
    console.log(`[ABA] prune: ${cutoff} より古い非監視 ${result.changes}行を削除`);
    db.pragma('wal_checkpoint(TRUNCATE)');
  }
}

async function main() {
  const db = initAbaDB();
  const targets = WEEK_ARG ? [weekFromStart(WEEK_ARG)] : recentCompletedWeeks(BACKFILL);

  if (!budgetIsUsable(RUN_BUDGET_MS)) {
    closeAbaDB();
    throw new Error(`--budget-min は ${Math.ceil(PER_WEEK_RESERVE_MS / 60000)} 分以上にしてください (1 週分の予約時間より短いと毎回何もせず持ち越す)`);
  }
  const startedAt = Date.now();
  const deadline = startedAt + RUN_BUDGET_MS;   // polling・DL・解析の途中でも、この時刻で打ち切って次回へ
  let ingested = 0, already = 0, unavailable = 0, deferred = 0;
  let attempted = 0;   // 実際に取りに行った (skip でない) 週の数。最初の 1 週は予算の残りに関わらず着手する
  for (let i = 0; i < targets.length; i++) {
    const week = targets[i];
    const r = await ingestWeek(db, week, { deadline, mustStart: attempted === 0 });
    if (r !== 'already' && r !== 'deferred') attempted++;
    if (r === 'ingested') ingested++;
    else if (r === 'already') already++;
    else if (r === 'deferred') {
      deferred = targets.length - i;   // この週と、それより古い週は次回に
      console.log(`[ABA] 時間予算 (${RUN_BUDGET_MS / 60000} 分) の期限 → ${deferred} 週を次回に持ち越す (${week.weekStart} から)`);
      break;
    } else unavailable++;
  }
  if (!DRY_RUN) {
    if (MODE === 'full' && ingested > 0) pruneOldWeeks(db, targets[0].weekStart);
    // レポートファイルは最新1本だけ残す (初回照会スキャン用)。--backfill 中の過去週分もここで掃除
    cleanupReportFiles(1);
  }

  // 鮮度アラーム: 過去に処理実績があるのに3週間以上新しい週が入らない場合はハード失敗。
  // 対象週は毎週転がるため「直近週の未公開skip」だけでは恒久障害 (権限剥奪・仕様変更) が
  // 永遠にサイレントになる。ここが最後の検知線 (初回セットアップ中 = 台帳空なら鳴らさない)
  if (ingested === 0) {
    const latest = db.prepare('SELECT MAX(week_end) AS we FROM aba_weeks').get();
    if (latest && latest.we) {
      const [ly, lm, ld] = latest.we.split('-').map(Number);
      const ageDays = Math.floor((Date.now() + 9 * 3600 * 1000 - Date.UTC(ly, lm - 1, ld)) / 86400000);
      if (ageDays > 21) {
        closeAbaDB();
        throw new Error(`最終処理週 (〜${latest.we}) から${ageDays}日間 新しい週が処理できていない — 権限/仕様変更を疑う`);
      }
    }
  }

  const stats = db.prepare('SELECT COUNT(*) AS weeks FROM aba_weeks WHERE parsed_count > 0').get();
  const watch = db.prepare('SELECT COUNT(*) AS c FROM aba_watch_asins').get();
  closeAbaDB();
  // 最終行 = daily-sync が拾うサマリ。保有 0 週のままなら人の目に付くように印を付ける (緑で無音にしない)
  const flag = stats.weeks === 0 ? '🚨まだ1週も保有していない ' : '';
  const carry = deferred > 0 ? ` / 持ち越し${deferred}週` : '';
  console.log(`ABA検索ワード: ${flag}新規${ingested}週 / 処理済${already}週 / 未公開${unavailable}週${carry} (DB保有${stats.weeks}週・監視${watch.c}ASIN, mode=${MODE})`);
}

// 試験から import しても走らないように (直接起動のときだけ main)。実体パスで比べる (apps/warehouse/retry-failed-jobs.js と同じ理由)
const realPath = (p) => { try { return fs.realpathSync.native(p); } catch { return path.resolve(p); } };
const foldCase = (p) => (process.platform === 'win32' ? p.toLowerCase() : p);
export const isDirectRun = (argv1, selfUrl) => !!argv1 && foldCase(realPath(argv1)) === foldCase(realPath(fileURLToPath(selfUrl)));
if (isDirectRun(process.argv[1], import.meta.url)) {
  main().catch((err) => {
    console.error('[ABA] エラー:', err.message || err);
    closeAbaDB();
    process.exit(1);
  });
}
