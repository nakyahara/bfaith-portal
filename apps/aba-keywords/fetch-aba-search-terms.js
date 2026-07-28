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
 * 実行: node apps/aba-keywords/fetch-aba-search-terms.js [--week YYYY-MM-DD] [--backfill N] [--dry-run]
 *   - 引数なし: 直近の「完了した週 (日曜〜土曜, JST)」を対象
 *   - 処理済みの週は即スキップ (冪等)。daily-sync から毎朝呼んでも新しい週だけ処理
 *   - レポート未公開 (直近週のFATAL/CANCELLED) は正常終了扱いでスキップ
 *   - 監視ASINを増やした後に --backfill 4 を回すと過去週の履歴も埋まる
 *
 * メモリ注意: レポートは数百MB級。全体を JSON.parse せず streaming で流し込む
 * (miniPC は AES sorter OOM の前科があるため常に省メモリ側に倒す)。
 */
import 'dotenv/config';
import fs from 'fs';
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
const POLL_MAX = 120;           // 15s × 120 = 30分

// ---- 引数 ----
const args = process.argv.slice(2);
const DRY_RUN = args.includes('--dry-run');
function argValue(name) {
  const i = args.indexOf(name);
  return i !== -1 && args[i + 1] ? args[i + 1] : null;
}
const WEEK_ARG = argValue('--week');
const BACKFILL = Math.min(12, Math.max(1, parseInt(argValue('--backfill') || '1', 10) || 1));

// ---- JST 週計算 (toISOString の UTC ズレ罠を避けるため UTC+9 を明示加算) ----
function jstToday() {
  const t = new Date(Date.now() + 9 * 3600 * 1000);
  return { y: t.getUTCFullYear(), m: t.getUTCMonth(), d: t.getUTCDate() };
}
function fmt(dateUtcMs) {
  const t = new Date(dateUtcMs);
  const p = (n) => String(n).padStart(2, '0');
  return `${t.getUTCFullYear()}-${p(t.getUTCMonth() + 1)}-${p(t.getUTCDate())}`;
}
/** 直近の完了済み週 (日曜開始) を新しい順に n 件返す */
function recentCompletedWeeks(n) {
  const { y, m, d } = jstToday();
  const todayMs = Date.UTC(y, m, d);
  // 直近の「終わった土曜」: 今日から遡って最初の土曜 (今日が土曜でも当週は未完了なので昨日から遡る)
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
async function ingestWeek(db, { weekStart, weekEnd }) {
  // 台帳に行がある = 処理完了済み (台帳は処理がすべて成功した後にだけ書くので、中断時は残らない)
  const existing = db.prepare('SELECT row_count, parsed_count FROM aba_weeks WHERE week_start = ?').get(weekStart);
  if (existing) {
    console.log(`[ABA] ${weekStart}〜${weekEnd}: 処理済み (全${existing.parsed_count}行/保存${existing.row_count}行) → skip`);
    return 'already';
  }

  const sp = getClient();
  console.log(`[ABA] ${weekStart}〜${weekEnd}: createReport ...`);
  const created = await withTimeout(sp.callAPI({
    operation: 'createReport', endpoint: 'reports',
    body: {
      reportType: REPORT_TYPE,
      marketplaceIds: [MARKETPLACE_ID],
      reportOptions: { reportPeriod: 'WEEK' },
      // ABA週 = 日曜〜土曜。JP マーケットプレイスなので JST 境界を明示する
      // (Amazon が日付部分だけ見るなら同値。⚠ 初回実取得時に週の同定を要確認)
      dataStartTime: `${weekStart}T00:00:00+09:00`,
      dataEndTime: `${weekEnd}T23:59:59+09:00`,
    },
    options: { version: '2021-06-30' },
  }), 60000, 'createReport');

  let report;
  for (let i = 0; i < POLL_MAX; i++) {
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
    // 未公開週 (週明け〜数日は集計中) は FATAL/CANCELLED で返るため、直近週に限り正常スキップ
    // (ハード失敗にすると公開までの数日間 毎朝 🔴 通知が出続ける)。
    // ただし週末から10日過ぎても FATAL のままなら「未公開」ではなく恒久障害
    // (ロール不足・reportOptions不正等) の可能性が高いのでハード失敗させて顕在化 (Codex R1 medium)。
    const [ey, em, ed] = weekEnd.split('-').map(Number);
    const daysSinceWeekEnd = Math.floor((Date.now() + 9 * 3600 * 1000 - Date.UTC(ey, em - 1, ed)) / 86400000);
    if (daysSinceWeekEnd > 10) {
      throw new Error(`${weekStart}週のレポートが週末から${daysSinceWeekEnd}日経っても ${report.processingStatus} — 未公開ではなく設定/権限問題を疑う`);
    }
    console.log(`[ABA] ${weekStart}: レポート未生成 (${report.processingStatus}) → skip (公開後に自動取込)`);
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
  const filePath = await downloadReportToFile(doc.url, doc.compressionAlgorithm === 'GZIP', weekStart);
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
      const { skipped } = await streamTermGroups(openReportStream(filePath), (group) => {
        parsed += group.length;
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
        // (手動で台帳行を消して強制再取込した場合のみ、旧レポート版の残骸が残り得る)
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
        const { skipped } = await streamTermGroups(openReportStream(filePath), (group) => {
          parsed += group.length;
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

  let ingested = 0, already = 0, unavailable = 0;
  for (const week of targets) {
    const r = await ingestWeek(db, week);
    if (r === 'ingested') ingested++;
    else if (r === 'already') already++;
    else unavailable++;
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
  // 最終行 = daily-sync が拾うサマリ
  console.log(`ABA検索ワード: 新規${ingested}週 / 処理済${already}週 / 未公開${unavailable}週 (DB保有${stats.weeks}週・監視${watch.c}ASIN, mode=${MODE})`);
}

main().catch((err) => {
  console.error('[ABA] エラー:', err.message || err);
  closeAbaDB();
  process.exit(1);
});
