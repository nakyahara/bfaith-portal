import { temporaryTestRoot } from '../../scripts/test-temp-dir.mjs';
await temporaryTestRoot(import.meta.url);
/**
 * test-settlement-inventory.js — 決済のレポートの一覧 (inventory) の記録の試験 (D7b-1b の下ごしらえ・2026-09-30)
 *
 * 設計 = AI_reference CompanyDB構想/13 §3.1。SP-API は差し替え (fake の callAPI / download)。本番 DB には触れない (一時 DATA_DIR)。
 *   - 一覧の窓の固定 (最初の要求に createdSince = createdUntil − 85 日・createdUntil = 回の開始の時刻。2 ページ目からは nextToken だけ)
 *   - 取込の一覧の要求は今までと同じ形 (日時の境なし) = 取込む report は変わらない
 *   - 最後のページまで取れた回 / 取れなかった回 (nextToken が残ったまま上限 = last_page_reached 0・取込は続ける)
 *   - 各分岐の取込の結果 (skipped_not_done / imported / skipped_v1 / failed / not_processed)・同じ report ID を 2 回見たら最後の状態
 *   - dry-run は表に書かない / --report-id は一覧の回にしない / 一覧の失敗でも取込は続ける / 例外で落ちた回は completed_at が null
 *   - 取込む行は変わらない (正規化した行の physical_line_hash の集まりと一致・一覧の有無で生の表が同じ)
 *   - digest の再現性 (並びに依らない・UTF-8 のバイトの順・保存した行から作り直して一致・式の固定)
 *
 * 実行: node apps/warehouse/test-settlement-inventory.js
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';

const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'settlement-inventory-test-'));
process.env.DATA_DIR = tmpDir;

const { initDB, getDB } = await import('./db.js');
const { SOURCES, runSettlementFetch, prepareReportTsv, prepareV2ReportTsv, ingestSettlement } = await import('./fetch-amazon-settlements.js');
const { V1_COLUMNS, V2_COLUMNS } = await import('./amazon-settlement-v2.js');
const inv = await import('./amazon-settlement-inventory.js');
const { canonicalSha256 } = await import('../company-db/canonical-hash.mjs');

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };
const eq = (a, b, label) => ok(JSON.stringify(a) === JSON.stringify(b), `${label}${JSON.stringify(a) === JSON.stringify(b) ? '' : ` (実際 ${JSON.stringify(a)} / 期待 ${JSON.stringify(b)})`}`);
const sha256 = (s) => crypto.createHash('sha256').update(s, 'utf8').digest('hex');
const tsvOf = (cols, rows) => [cols.join('\t'), ...rows.map((r) => cols.map((c) => r[c] ?? '').join('\t'))].join('\n') + '\n';

// 試験の中の console.log を拾う (取込のログは多い)
async function captured(fn) {
  const logs = [];
  const orig = console.log;
  console.log = (...a) => logs.push(a.join(' '));
  try { return { value: await fn(), logs, error: null }; }
  catch (e) { return { value: null, logs, error: e }; }
  finally { console.log = orig; }
}

// ── 作り物のレポート ──
const V2T = SOURCES.v2.reportType;
const MKT = 'A1VC38T7YXB528';
const T1 = '2099/01/05 01:00:00 UTC';
function v2Tsv(sid, { bad = false, amount = '300.00' } = {}) {
  const hdr = { 'settlement-id': sid, 'settlement-start-date': '2099/01/01 00:20:09 UTC', 'settlement-end-date': '2099/01/15 00:20:09 UTC', 'deposit-date': '2099/01/17 00:20:09 UTC', 'total-amount': amount, currency: 'JPY' };
  const line = { 'settlement-id': sid, currency: '', 'marketplace-name': 'Amazon.co.jp', 'posted-date': T1.slice(0, 10), 'posted-date-time': T1, 'transaction-type': 'Order', 'order-id': `O-${sid}`, 'merchant-order-id': `O-${sid}`, 'order-item-code': `OI-${sid}`, sku: 'SKU-X', 'quantity-purchased': '1', 'amount-type': 'ItemPrice', 'amount-description': 'Principal', amount };
  const rows = [hdr, line];
  if (bad) rows.push({ ...line, 'transaction-type': 'NewThing', 'amount-type': 'Mystery', 'amount-description': 'x', amount: '-7.00' });
  return tsvOf(V2_COLUMNS, rows);
}
const rep = (id, status, doc, over = {}) => ({
  reportId: id, reportType: V2T, processingStatus: status, marketplaceIds: [MKT],
  createdTime: '2026-09-20T03:04:05+00:00', dataStartTime: '2026-09-01T00:00:00+00:00', dataEndTime: '2026-09-15T00:00:00+09:00',
  ...(doc ? { reportDocumentId: doc } : {}), ...over,
});

const DOCS = {
  'D-IMP': v2Tsv('S-IMP'),
  'D-V1': v2Tsv('S-V1'),
  'D-BAD': v2Tsv('S-BAD', { bad: true }),
  'D-ONLYINV': v2Tsv('S-ONLYINV'),
  'D-ONLYING': v2Tsv('S-ONLYING'),
};
const R = {
  notDone: rep('R-NOTDONE', 'IN_PROGRESS', null, { dataStartTime: undefined, dataEndTime: undefined }),
  imp: rep('R-IMP', 'DONE', 'D-IMP'),
  v1: rep('R-V1', 'DONE', 'D-V1'),
  bad: rep('R-BAD', 'DONE', 'D-BAD'),
  onlyInv: rep('R-ONLYINV', 'DONE', 'D-ONLYINV'),          // 一覧にだけ居る (取込の一覧に無い)
  onlyIng: rep('R-ONLYING', 'DONE', 'D-ONLYING', { createdTime: '2026-07-03T00:00:00+00:00' }),   // 取込の一覧にだけ居る (85〜90 日前に作られた)
};

/**
 * fake の SP-API。getReports の最初の要求は createdSince の有無で「一覧の記録用」か「取込用」かを分ける。
 * pages = { inv: [page, ...] | (idx) => page, ing: [...] }・page = { reports, next }
 */
// opts: failInventory = 一覧の要求が失敗 / hangInventory = 一覧の要求が返らない (長い retry) / invalidInventory = 一覧のページの応答 (そのまま返す)
//       tokens = getReports の共有のレートの枠 (無くなると QuotaExceeded) / clock = { ms, stepMs } = 呼ぶたびに時計が進む (page 関数に呼んだ時刻を渡す)
// events = 呼んだ順の記録 (list:ing = 取込の一覧 / list:inv = 一覧の記録 / next:* = 2 ページ目から / dl:文書 = ダウンロード)
const events = [];
function fakeSp(pages, { failInventory = false, hangInventory = false, invalidInventory, tokens = Infinity, clock = null, reportById = {} } = {}) {
  const calls = [];
  let left = tokens;
  events.length = 0;
  const page = (kind, idx, at) => {
    const src = pages[kind];
    const p = typeof src === 'function' ? src(idx, at) : src[idx];
    return { reports: p.reports, ...(p.next ? { nextToken: `${kind}:${idx + 1}` } : {}) };
  };
  return {
    calls,
    async callAPI(req) {
      calls.push(JSON.parse(JSON.stringify(req)));
      if (req.operation === 'getReports') {
        const q = req.query;
        events.push(q.nextToken ? `next:${q.nextToken.split(':')[0]}` : q.createdSince ? 'list:inv' : 'list:ing');
      } else events.push(req.operation);
      const at = clock ? clock.ms : null;
      if (clock) clock.ms += clock.stepMs;
      if (req.operation === 'getReports') {
        if (left <= 0) throw new Error('QuotaExceeded (作り物のレートの枠)');
        left--;
        const q = req.query;
        if (q.nextToken) { const [kind, idx] = q.nextToken.split(':'); return page(kind, Number(idx), at); }
        const kind = q.createdSince ? 'inv' : 'ing';
        if (kind === 'inv' && failInventory) throw new Error('QuotaExceeded (作り物)');
        if (kind === 'inv' && hangInventory) return new Promise(() => {});
        if (kind === 'inv' && invalidInventory !== undefined) return invalidInventory;
        return page(kind, 0, at);
      }
      if (req.operation === 'getReport') return reportById[req.path.reportId];
      throw new Error(`想定外の呼び出し ${req.operation}`);
    },
  };
}
const downloaded = [];
const fakeDownload = (throwFor = null) => async (docId) => {
  downloaded.push(docId);
  events.push(`dl:${docId}`);
  if (docId === throwFor) throw new Error(`ダウンロードの失敗 (作り物) ${docId}`);
  if (!Object.hasOwn(DOCS, docId)) throw new Error(`文書が無い ${docId}`);
  return DOCS[docId];
};
const ARGS = { reportId: null, dryRun: false, source: 'v2' };
const NOW = new Date('2026-09-30T00:00:05.500Z');
let tick = 0;
const nowFn = () => new Date(NOW.getTime() + (tick++) * 1000);

await initDB();
const db = getDB();

// 前提: S-V1 は V1 で取込済み (V2 では入れない = skipped_v1)
const v1Prep = prepareReportTsv(tsvOf(V1_COLUMNS, [
  { 'settlement-id': 'S-V1', 'settlement-start-date': '2099-01-01T00:20:09+00:00', 'settlement-end-date': '2099-01-15T00:20:09+00:00', 'deposit-date': '2099-01-17T00:20:09+00:00', 'total-amount': '300.00', currency: 'JPY' },
  { 'settlement-id': 'S-V1', 'marketplace-name': 'Amazon.co.jp', 'posted-date': '2099-01-05T01:00:00+00:00', 'transaction-type': 'Order', 'order-id': 'O9', sku: 'SKU-Y', 'price-type': 'Principal', 'price-amount': '300.00' },
]), 'R-V1-OLD', 'seed');
ingestSettlement(db, v1Prep.headerRow, v1Prep.lineRows, v1Prep.ctx);

const runs = () => db.prepare(`SELECT * FROM amazon_settlement_report_inventory_runs ORDER BY id`).all();
const rows = (runId) => db.prepare(`SELECT * FROM amazon_settlement_report_inventory WHERE inventory_run_id = ? ORDER BY report_id`).all(runId);
const rowOf = (runId, reportId) => db.prepare(`SELECT * FROM amazon_settlement_report_inventory WHERE inventory_run_id = ? AND report_id = ?`).get(runId, reportId);
const rawLines = () => db.prepare(`SELECT physical_line_hash FROM raw_amazon_settlement_lines WHERE source_layer = 'sp_api_v2' ORDER BY physical_line_hash`).all().map((r) => r.physical_line_hash);
const rawCounts = () => db.prepare(`SELECT (SELECT COUNT(*) FROM raw_amazon_settlement_lines) l, (SELECT COUNT(*) FROM raw_amazon_settlement_headers) h`).get();

// ════ 1. ふつうの回 (一覧 2 ページ・最後まで取れた) ════
const PAGES_NORMAL = {
  // R-V1 を 1 ページ目で IN_QUEUE・2 ページ目で DONE に = 同じ report ID を 2 回見たら最後の状態
  inv: [
    { reports: [R.notDone, R.imp, { ...R.v1, processingStatus: 'IN_QUEUE', reportDocumentId: undefined }], next: true },
    { reports: [R.bad, R.onlyInv, R.v1] },
  ],
  ing: [{ reports: [R.notDone, R.imp, R.v1, R.bad, R.onlyIng] }],
};
let sp = fakeSp(PAGES_NORMAL);
let res = await captured(() => runSettlementFetch(ARGS, { db, sp, runId: 'run-1', downloadTsv: fakeDownload(), now: nowFn }));
ok(!res.error, `ふつうの回は落ちない ${res.error ? res.error.message : ''}`);

// 呼ぶ順: 取込の一覧の要求が先 (今と同じ位置・同じ形) → 一覧の記録の要求 (窓を明示・2 ページ目は nextToken だけ)
const reportsCalls = sp.calls.filter((c) => c.operation === 'getReports');
eq(reportsCalls[0].query, { reportTypes: [V2T], marketplaceIds: [MKT], pageSize: 100 }, '🚨 最初の getReports = 取込の一覧の要求 (今までと同じ形・日時の境なし) = 取込む report は変わらない');
eq(reportsCalls[1].query, { reportTypes: [V2T], marketplaceIds: [MKT], pageSize: 100, createdSince: '2026-07-07T00:00:05Z', createdUntil: '2026-09-30T00:00:05Z' }, '一覧の記録の要求は取込の一覧の後 = createdUntil は回の開始の時刻 (秒に切り捨て)・createdSince はその 85 日前');
eq(reportsCalls[2].query, { nextToken: 'inv:1' }, '一覧の 2 ページ目は nextToken だけ (窓を付け直さない)');
ok(reportsCalls.length === 3, `getReports は取込 1 + 一覧 2 = 3 回 (${reportsCalls.length})`);
ok(sp.calls.findIndex((c) => c.operation === 'getReports' && c.query.createdSince) > sp.calls.findIndex((c) => c.operation === 'getReports' && !c.query.createdSince && !c.query.nextToken), '一覧の記録の要求は取込の一覧の要求より後');
const lastDl = (ev) => ev.reduce((m, e, i) => (e.startsWith('dl:') ? i : m), -1);
eq(events, ['list:ing', 'dl:D-IMP', 'dl:D-V1', 'dl:D-BAD', 'dl:D-ONLYING', 'list:inv', 'next:inv'], '🚨 呼ぶ順 = 取込の一覧 → ダウンロード全部 → 一覧の記録 (Codex #1555 R2 High)');

let [run1] = runs();
ok(run1 && run1.report_type === V2T && run1.marketplace_id === MKT, '回の見出し: report type・marketplace');
eq([run1.query_created_since, run1.query_created_until, run1.started_at], ['2026-07-07T00:00:05Z', '2026-09-30T00:00:05Z', '2026-09-30T00:00:05Z'], '回の見出し: 窓 (UTC) と開始の時刻');
eq([run1.last_page_reached, run1.page_count, run1.report_count], [1, 2, 5], '最後のページまで取れた = last_page_reached 1・2 ページ・5 本 (重複を除く)');
ok(run1.completed_at && run1.list_completed_at && run1.list_error === null, '完了の時刻がある・一覧の失敗なし');
ok(run1.coverage_generation === null && run1.run_token === null, 'coverage_generation / run_token は null (後の coordinator が入れる)');
eq([run1.company_id, run1.mall, run1.scope_key, run1.evidence_epoch], [1, 'amazon', 'jp', null], '所属 = 1 / amazon / jp・evidence_epoch は null (どの印の鎖にも属さない)');
ok(run1.inventory_run_seq === 1 && run1.ingest_run_id === 'run-1', '回の連番 1・取込の回の ID');

// 各分岐の取込の結果
eq(rows(run1.id).map((r) => [r.report_id, r.import_result]), [['R-BAD', 'failed'], ['R-IMP', 'imported'], ['R-NOTDONE', 'skipped_not_done'], ['R-ONLYINV', 'not_processed'], ['R-V1', 'skipped_v1']], '各分岐の取込の結果 (取込の一覧にだけ居る R-ONLYING は一覧の行にしない)');
const imp = rowOf(run1.id, 'R-IMP');
ok(imp.source_file_hash === sha256(DOCS['D-IMP']) && imp.settlement_id === 'S-IMP' && imp.header_inserted === 1 && imp.lines_inserted === 2 && imp.imported_report_document_id === 'D-IMP', 'imported: file hash・決済 ID・入れた行の数・落とした文書 ID');
const rawHash = db.prepare(`SELECT DISTINCT source_file_hash h FROM raw_amazon_settlement_lines WHERE source_document_id = 'R-IMP'`).all().map((r) => r.h);
eq(rawHash, [imp.source_file_hash], 'imported の file hash = 生の表の source_file_hash と同じ式');
const v1row = rowOf(run1.id, 'R-V1');
ok(v1row.settlement_id === 'S-V1' && v1row.source_file_hash === sha256(DOCS['D-V1']) && v1row.processing_status === 'DONE' && v1row.report_document_id === 'D-V1' && v1row.last_seen_ordinal === 6, `skipped_v1: 決済 ID・file hash / 同じ report ID を 2 回見たら最後の状態と位置 (last_seen_ordinal ${v1row.last_seen_ordinal})`);
const badRow = rowOf(run1.id, 'R-BAD');
ok(/^blocked: /.test(badRow.import_note) && badRow.source_file_hash === sha256(DOCS['D-BAD']), `failed (blocked): 理由と file hash (${badRow.import_note})`);
const nd = rowOf(run1.id, 'R-NOTDONE');
ok(nd.processing_status === 'IN_PROGRESS' && nd.data_start_time === null && nd.data_end_time === null && nd.report_document_id === null && /IN_PROGRESS/.test(nd.import_note), 'skipped_not_done: 期間の無い report も行にする (null のまま)');
eq([imp.created_time, imp.data_start_time, imp.data_end_time], ['2026-09-20T03:04:05Z', '2026-09-01T00:00:00Z', '2026-09-14T15:00:00Z'], '日時は UTC の YYYY-MM-DDTHH:MM:SSZ に (+09:00 も UTC に直す)');
ok(res.value.blocked.length === 1 && res.value.blocked[0].reportId === 'R-BAD', '取り込まなかった V2 は今までどおり blocked で返る (main が終了コード 3)');
ok(/決済の一覧 5 本を記録$/.test(res.logs.find((l) => l.includes('[settlements] 完了')) || ''), '完了の行の末尾 = 一覧の本数 (daily-sync の朝の報告)');

// 取込む行は変わらない = 正規化した行の集まりとちょうど同じ
const expected = [...prepareV2ReportTsv(DOCS['D-IMP'], 'R-IMP', 'x').lineRows, ...prepareV2ReportTsv(DOCS['D-ONLYING'], 'R-ONLYING', 'x').lineRows].map((r) => r.physical_line_hash).sort();
eq(rawLines(), expected, '🚨 生の表 (V2) = 取込む report (R-IMP・R-ONLYING) の正規化した行とちょうど同じ (一覧にだけ居る R-ONLYINV は入れない)');
const afterRun1 = rawCounts();

// digest = 保存した行から作り直して一致
const dbDigest = inv.inventorySnapshotDigest(rows(run1.id));
ok(run1.snapshot_digest === dbDigest && /^[0-9a-f]{64}$/.test(dbDigest), 'snapshot の digest = 保存した行から作り直した digest と一致 (hex 64 桁)');

// ════ 2. dry-run は表に書かない ════
sp = fakeSp(PAGES_NORMAL);
res = await captured(() => runSettlementFetch({ ...ARGS, dryRun: true }, { db, sp, runId: 'run-dry', downloadTsv: fakeDownload(), now: nowFn }));
ok(!res.error && runs().length === 1 && db.prepare(`SELECT COUNT(*) n FROM amazon_settlement_report_inventory`).get().n === 5, 'dry-run は一覧の表に書かない');
eq(rawCounts(), afterRun1, 'dry-run は生の表にも書かない');
ok(res.logs.some((l) => /\[inventory\] \[dry-run\] 一覧は記録しない/.test(l)) && /決済の一覧 5 本 \(dry-run = 記録しない\)$/.test(res.logs.find((l) => l.includes('[settlements] 完了')) || ''), 'dry-run は一覧を取って本数を出すが「記録しない」');

// ════ 3. 最後のページまで取れなかった回 (nextToken が残ったまま上限) = last_page_reached 0・取込は続ける ════
sp = fakeSp({ inv: (idx) => ({ reports: idx === 0 ? [R.imp] : [], next: true }), ing: PAGES_NORMAL.ing });
res = await captured(() => runSettlementFetch(ARGS, { db, sp, runId: 'run-3', downloadTsv: fakeDownload(), now: nowFn }));
const run3 = runs().at(-1);
ok(!res.error && run3.last_page_reached === 0 && run3.page_count === inv.MAX_LIST_PAGES && run3.ingest_run_id === 'run-3', `nextToken が残ったまま上限 = last_page_reached 0 (${run3.page_count} ページ)`);
ok(run3.completed_at !== null && rowOf(run3.id, 'R-IMP').import_result === 'imported', '取込は今までどおり続ける (取込の結果も記録)');
ok(res.logs.some((l) => /⚠️ 21 ページで打ち切り/.test(l)) && /⚠️ 決済の一覧が途中まで/.test(res.logs.find((l) => l.includes('[settlements] 完了')) || ''), '黙って打ち切らない = ⚠️ をログと完了の行に');
ok(run3.inventory_run_seq === 2, '回の連番は増える (dry-run は数えない)');
eq(rawCounts(), afterRun1, '同じレポートの取り直しで生の表は増えない (今までどおり冪等)');

// ════ 4. --report-id の 1 本だけの回は一覧の回にしない ════
sp = fakeSp({ inv: [], ing: [] }, { reportById: { 'R-IMP': R.imp } });
const before4 = runs().length;
res = await captured(() => runSettlementFetch({ ...ARGS, reportId: 'R-IMP' }, { db, sp, runId: 'run-4', downloadTsv: fakeDownload(), now: nowFn }));
ok(!res.error && runs().length === before4 && !sp.calls.some((c) => c.operation === 'getReports'), '--report-id は一覧を取らない・一覧の回にしない');
ok(sp.calls.some((c) => c.operation === 'getReport'), '--report-id は今までどおり getReport で 1 本');

// ════ 5. 一覧の要求が失敗しても取込は続ける (回は失敗として残す) ════
sp = fakeSp(PAGES_NORMAL, { failInventory: true });
res = await captured(() => runSettlementFetch(ARGS, { db, sp, runId: 'run-5', downloadTsv: fakeDownload(), now: nowFn }));
const run5 = runs().at(-1);
ok(!res.error && run5.ingest_run_id === 'run-5' && run5.last_page_reached === 0 && run5.report_count === 0 && run5.snapshot_digest === null && /QuotaExceeded/.test(run5.list_error), `一覧の失敗 = last_page_reached 0・digest null・list_error (${run5.list_error})`);
ok(res.value?.blocked.length === 1 && /⚠️ 決済の一覧を取れなかった/.test(res.logs.find((l) => l.includes('[settlements] 完了')) || ''), '取込は最後まで回る (⚠️ は完了の行に)');
eq(rawCounts(), afterRun1, '一覧が無くても取込む行は同じ (一覧の有無で生の表は変わらない)');

// ════ 6. ダウンロードで例外 = 今までどおり回ごと止める・その report は failed・回は completed_at null ════
sp = fakeSp(PAGES_NORMAL);
res = await captured(() => runSettlementFetch(ARGS, { db, sp, runId: 'run-6', downloadTsv: fakeDownload('D-V1'), now: nowFn }));
const run6 = runs().at(-1);
ok(res.error && /ダウンロードの失敗/.test(res.error.message), '例外は今までどおり投げる (FATAL・終了コード 1)');
ok(run6.completed_at === null && run6.last_page_reached === 1 && /ダウンロードの失敗/.test(run6.ingest_error || ''), `例外で止まった回も一覧は記録する = completed_at null・ingest_error (${run6.ingest_error})`);
ok(events.at(-2) === 'list:inv' && events.indexOf('list:inv') > lastDl(events), '例外で止まった回も、一覧の記録の要求は取込のループの後');
const r6 = rowOf(run6.id, 'R-V1');
ok(r6.import_result === 'failed' && /^例外: /.test(r6.import_note) && r6.imported_report_document_id === 'D-V1', `落ちた report = failed + 理由 (${r6.import_note})`);
ok(rowOf(run6.id, 'R-IMP').import_result === 'imported' && rowOf(run6.id, 'R-BAD').import_result === 'not_processed', '落ちる前の report は結果あり・後の report は not_processed');

// ════ H. 取込の一覧を先に確定する = 一覧の記録の要求が取込の対象を変えない (Codex #1555 R1 High) ════
const DAY = 86400000;
const clock = { ms: Date.parse('2026-10-01T00:00:00Z'), stepMs: 30000 };   // SP-API を呼ぶたびに 30 秒進む
DOCS['D-BORDER'] = v2Tsv('S-BORDER');
const BORDER = rep('R-BORDER', 'DONE', 'D-BORDER', { createdTime: new Date(clock.ms - 90 * DAY + 10000).toISOString() });   // 既定の窓の境の 10 秒内側
// 取込の一覧 = Amazon の既定の窓 = 呼んだ時刻の 90 日前より後に作られた report だけ
const defaultWindow = (idx, at) => ({ reports: [BORDER, R.imp].filter((x) => Date.parse(x.createdTime) >= at - 90 * DAY) });
sp = fakeSp({ inv: [{ reports: [R.imp] }], ing: defaultWindow }, { clock });
downloaded.length = 0;
res = await captured(() => runSettlementFetch(ARGS, { db, sp, runId: 'run-h1', downloadTsv: fakeDownload(), now: () => new Date(clock.ms) }));
ok(!res.error && downloaded.includes('D-BORDER') && rowOf(runs().at(-1).id, 'R-IMP')?.import_result === 'imported', '🚨 90 日の境の report が今と同じく取込まれる (一覧の記録の要求が先に「今」を進めない)');

// 基準 = 一覧の記録が何も邪魔しない回で、取込が落とした文書と止まった report
const ingestOutcome = async (opts, runId, deps = {}) => {
  downloaded.length = 0;
  const s = fakeSp(PAGES_NORMAL, opts);
  const out = await captured(() => runSettlementFetch(ARGS, { db, sp: s, runId, downloadTsv: fakeDownload(), now: nowFn, ...deps }));
  return { out, sp: s, downloads: [...downloaded], blocked: (out.value?.blocked || []).map((b) => b.reportId), run: runs().at(-1) };
};
const base = await ingestOutcome({}, 'run-h-base');
eq(base.out.error ? 'error' : base.downloads, ['D-IMP', 'D-V1', 'D-BAD', 'D-ONLYING'], '前提: 基準の回は DONE の 4 本を落とす (IN_PROGRESS は落とさない)');
// 共有のレートの枠が 1 回分だけ = 取込の一覧が先に使う → 一覧の記録の要求は枠切れ
const rate = await ingestOutcome({ tokens: 1 }, 'run-h-rate');
ok(!rate.out.error && JSON.stringify(rate.downloads) === JSON.stringify(base.downloads) && JSON.stringify(rate.blocked) === JSON.stringify(base.blocked), '🚨 レートの枠が 1 回分だけでも取込の結果は同じ (取込の一覧が先に枠を使う)');
ok(rate.run.ingest_run_id === 'run-h-rate' && rate.run.last_page_reached === 0 && /QuotaExceeded/.test(rate.run.list_error), `枠切れの一覧は失敗として残す (${rate.run.list_error})`);
// 一覧の記録の要求が返らない (長い retry) = 時間の上限で打ち切る・取込は同じ
const t0 = Date.now();
const hang = await ingestOutcome({ hangInventory: true }, 'run-h-hang', { inventoryTimeoutMs: 50 });
ok(!hang.out.error && JSON.stringify(hang.downloads) === JSON.stringify(base.downloads) && JSON.stringify(hang.blocked) === JSON.stringify(base.blocked) && Date.now() - t0 < 10000, '🚨 一覧の要求が返らなくても時間の上限で打ち切り、取込の結果は同じ');
ok(hang.run.last_page_reached === 0 && /時間切れ/.test(hang.run.list_error) && hang.run.completed_at !== null, `時間切れの一覧は失敗として残す (${hang.run.list_error})`);
ok(events.indexOf('list:inv') > lastDl(events) && lastDl(events) === 4, '🚨 一覧の要求が返らなくても、取込のダウンロードは全部その前に済んでいる');

// ════ M (R2). 記録の失敗 = completed_at を入れない・record_error・取込の結果は同じ ════
const recFail = async (triggerSql, runId) => {
  db.exec(triggerSql);
  try {
    const before = rawCounts();
    const o = await ingestOutcome({}, runId);
    return { ...o, before, after: rawCounts() };
  } finally { db.exec(`DROP TRIGGER IF EXISTS trg_test_inventory_fail`); }
};
const rf1 = await recFail(`CREATE TRIGGER trg_test_inventory_fail BEFORE INSERT ON amazon_settlement_report_inventory WHEN NEW.report_id = 'R-BAD' BEGIN SELECT RAISE(ABORT, 'わざとの一覧の行の INSERT の失敗'); END`, 'run-rf-ins');
ok(!rf1.out.error && rf1.run.ingest_run_id === 'run-rf-ins' && rf1.run.completed_at === null && /INSERT の失敗/.test(rf1.run.record_error || '') && rf1.run.report_count === 0 && rf1.run.last_page_reached === 0 && rows(rf1.run.id).length === 0,
  `一覧の行の INSERT の失敗 = 見出しだけ・completed_at null・record_error (${rf1.run.record_error})`);
ok(JSON.stringify(rf1.downloads) === JSON.stringify(base.downloads) && JSON.stringify(rf1.blocked) === JSON.stringify(base.blocked) && JSON.stringify(rf1.after) === JSON.stringify(rf1.before) && /⚠️ 決済の一覧を記録できなかった/.test(rf1.out.logs.find((l) => l.includes('[settlements] 完了')) || ''),
  '記録の失敗でも取込の結果・生の行は同じ (⚠️ だけ)');
const rf2 = await recFail(`CREATE TRIGGER trg_test_inventory_fail BEFORE UPDATE ON amazon_settlement_report_inventory WHEN NEW.import_result = 'skipped_v1' BEGIN SELECT RAISE(ABORT, 'わざとの取込の結果の UPDATE の失敗'); END`, 'run-rf-upd');
ok(!rf2.out.error && rf2.run.ingest_run_id === 'run-rf-upd' && rf2.run.completed_at === null && /UPDATE の失敗/.test(rf2.run.record_error || '') && rows(rf2.run.id).length === 0 && JSON.stringify(rf2.after) === JSON.stringify(rf2.before),
  `取込の結果の UPDATE の失敗 = 取引ごと戻して見出しだけ・completed_at null・record_error (${rf2.run.record_error})`);
const runsBefore = runs().length;
const rf3 = await recFail(`CREATE TRIGGER trg_test_inventory_fail BEFORE INSERT ON amazon_settlement_report_inventory_runs BEGIN SELECT RAISE(ABORT, 'わざとの見出しの失敗'); END`, 'run-rf-hdr');
ok(!rf3.out.error && runs().length === runsBefore && rf3.out.value?.inventory?.id === null && /見出しも書けない/.test(rf3.out.value?.inventory?.recordError || '') && JSON.stringify(rf3.downloads) === JSON.stringify(base.downloads),
  '見出しも書けない = 回は残らない (coverage に使えない = 安全側)・取込は同じ');

// ════ M1. 不正なページの応答を「最後まで取れた空のページ」にしない ════
for (const [bad, label] of [[null, 'null'], [{ reports: 'x' }, 'reports が文字'], [{}, 'reports が無い'], [{ reports: [], nextToken: 5 }, 'nextToken が数']]) {
  const o = await ingestOutcome({ invalidInventory: bad }, `run-m1-${label}`);
  ok(!o.out.error && o.run.last_page_reached === 0 && o.run.report_count === 0 && o.run.snapshot_digest === null && /形が違う|文字でない/.test(o.run.list_error || '') && JSON.stringify(o.downloads) === JSON.stringify(base.downloads),
    `不正なページ (${label}) = 一覧の失敗 (last_page_reached 0・list_error)・取込は同じ (${o.run.list_error})`);
}

// ════ M2. ダウンロードの後の例外でも file hash を failed の行に残す ════
DOCS['D-TRIG'] = v2Tsv('S-TRIG');
const TRIG = rep('R-TRIG', 'DONE', 'D-TRIG');
db.exec(`CREATE TRIGGER trg_test_settlement_fail BEFORE INSERT ON raw_amazon_settlement_headers WHEN NEW.source_settlement_id = 'S-TRIG' BEGIN SELECT RAISE(ABORT, 'わざとの DB の失敗'); END`);
sp = fakeSp({ inv: [{ reports: [TRIG] }], ing: [{ reports: [TRIG] }] });
res = await captured(() => runSettlementFetch(ARGS, { db, sp, runId: 'run-m2', downloadTsv: fakeDownload(), now: nowFn }));
db.exec(`DROP TRIGGER trg_test_settlement_fail`);
const rTrig = rowOf(runs().at(-1).id, 'R-TRIG');
ok(res.error && /わざとの DB の失敗/.test(res.error.message), 'DB の投入の例外は今までどおり投げる');
ok(rTrig.import_result === 'failed' && rTrig.source_file_hash === sha256(DOCS['D-TRIG']) && rTrig.imported_report_document_id === 'D-TRIG' && /わざとの DB の失敗/.test(rTrig.import_note), 'ダウンロードの後の例外 = failed の行に file hash と理由を残す');
ok(db.prepare(`SELECT COUNT(*) n FROM raw_amazon_settlement_lines WHERE source_settlement_id = 'S-TRIG'`).get().n === 0, '失敗した決済の行は入らない (取引ごと戻る)');

// ════ Low. CANCELLED の report = 保存・ダウンロードしない・skipped_not_done ════
const CAN = rep('R-CANCEL', 'CANCELLED', 'D-CANCEL', { dataStartTime: undefined, dataEndTime: undefined });
sp = fakeSp({ inv: [{ reports: [CAN] }], ing: [{ reports: [CAN] }] });
downloaded.length = 0;
res = await captured(() => runSettlementFetch(ARGS, { db, sp, runId: 'run-cancel', downloadTsv: fakeDownload(), now: nowFn }));
const rCan = rowOf(runs().at(-1).id, 'R-CANCEL');
ok(!res.error && rCan && rCan.processing_status === 'CANCELLED' && rCan.report_document_id === 'D-CANCEL' && rCan.import_result === 'skipped_not_done' && rCan.import_note === 'status=CANCELLED' && !downloaded.includes('D-CANCEL'),
  `CANCELLED = 一覧に保存・ダウンロードしない・skipped_not_done (${rCan?.import_note})`);

// ════ R3. 一覧の記録用の要求は専用の接続 = 時間の上限つき・自動の再試行なし・期限で要求そのものを止める (Codex #1555 R3) ════
eq(inv.inventoryClientOptions(5000), { auto_request_throttled: false, retry_remote_timeout: false, timeouts: { response: 5000, idle: 5000, deadline: 5000 } }, '専用の接続の options = 429 / 通信の失敗で再試行しない・時間の上限');
{
  // 本物の amazon-sp-api (1.2.0) に専用の options を渡し、通信の所だけ差し替える (_request.api・_validateAccessToken)
  const SellingPartner = (await import('amazon-sp-api')).default;
  const client = new SellingPartner({ region: 'fe', refresh_token: 'dummy', credentials: { SELLING_PARTNER_APP_CLIENT_ID: 'dummy', SELLING_PARTNER_APP_CLIENT_SECRET: 'dummy' }, options: inv.inventoryClientOptions(5000) });
  client._validateAccessToken = async () => {};
  const seen = [];
  client._request.api = async (token, reqParams) => {
    seen.push(reqParams.timeouts);
    // 1 回目は 429 (再試行するなら 10 ms 待って 2 回目 = 成功してしまう)
    if (seen.length === 1) return { statusCode: 429, headers: { 'x-amzn-ratelimit-limit': '100' }, body: JSON.stringify({ errors: [{ code: 'QuotaExceeded', message: '作り物の枠切れ' }] }) };
    return { statusCode: 200, headers: {}, body: JSON.stringify({ reports: [] }) };
  };
  let err = null;
  try { await inv.listInventoryReports(client, { reportType: V2T, marketplaceId: MKT, startedAt: NOW, timeoutMs: 5000 }); } catch (e) { err = e; }
  ok(err && err.code === 'QuotaExceeded' && seen.length === 1, `本物のライブラリ: 429 で待って再試行しない = 一覧の失敗 (${err?.code} / 呼んだ回数 ${seen.length})`);
  ok(seen[0] && seen[0].deadline > 0 && seen[0].deadline <= 5000 && seen[0].response === seen[0].deadline && seen[0].idle === seen[0].deadline, `本物のライブラリ: 要求に残り時間の timeouts が付く (${JSON.stringify(seen[0])})`);
}
{
  // 子プロセス: 一覧の記録用の fake の要求は、ライブラリと同じく timeouts.deadline があればその時刻で timer を消して失敗 (socket の破棄)、
  // 無ければ 60 秒の timer (= 取消されない再試行の待ち・active handle) を持つ。取込が済んだら子が期限の内に自分で終わること (process.exit で無理に終わらせない)
  const { spawnSync } = await import('node:child_process');
  const { pathToFileURL } = await import('node:url');
  const childDir = fs.mkdtempSync(path.join(os.tmpdir(), 'settlement-inventory-child-'));
  const here = path.dirname(new URL(import.meta.url).pathname.replace(/^\/([A-Za-z]:)/, '$1'));
  const code = [
    `process.env.DATA_DIR = ${JSON.stringify(childDir)};`,
    `const { initDB, getDB } = await import(${JSON.stringify(pathToFileURL(path.join(here, 'db.js')).href)});`,
    `const { runSettlementFetch } = await import(${JSON.stringify(pathToFileURL(path.join(here, 'fetch-amazon-settlements.js')).href)});`,
    'await initDB();',
    `const TSV = ${JSON.stringify(DOCS['D-IMP'])};`,
    `const REP = ${JSON.stringify(R.imp)};`,
    'const ingestSp = { async callAPI(req) { return { reports: [REP] }; } };',
    'const invSp = { callAPI(req) { const d = req.options && req.options.timeouts && req.options.timeouts.deadline;',
    '  return new Promise((resolve, reject) => { if (d) setTimeout(() => reject(new Error("API_DEADLINE_TIMEOUT (作り物)")), d); else setTimeout(() => resolve({ reports: [] }), 60000); }); } };',
    'const r = await runSettlementFetch({ reportId: null, dryRun: false, source: "v2" }, { db: getDB(), sp: ingestSp, inventorySp: invSp, runId: "child", downloadTsv: async () => TSV, inventoryTimeoutMs: 300 });',
    'const lines = getDB().prepare("SELECT COUNT(*) n FROM raw_amazon_settlement_lines").get().n;',
    'console.log("CHILD_RESULT " + JSON.stringify({ blocked: r.blocked.length, lines, listError: (r.inventory && r.inventory.listError) || null }));',
  ].join('\n');
  const t0c = Date.now();
  const child = spawnSync(process.execPath, ['--input-type=module', '-e', code], { cwd: childDir, encoding: 'utf8', timeout: 20000, env: { ...process.env, DATA_DIR: childDir } });
  const tookMs = Date.now() - t0c;
  const line = (child.stdout || '').split(/\r?\n/).find((l) => l.startsWith('CHILD_RESULT ')) || '';
  const out = line ? JSON.parse(line.slice('CHILD_RESULT '.length)) : null;
  ok(child.status === 0 && tookMs < 15000, `🚨 一覧の要求が timer を持っていても、子プロセスは期限の内に自分で終わる (exit ${child.status}・${tookMs} ms${child.error ? `・${child.error.message}` : ''})`);
  ok(out && out.blocked === 0 && out.lines === 2 && /時間切れ|DEADLINE/.test(out.listError || ''), `子プロセスの取込の結果は同じ (生の行 ${out?.lines}・一覧 ${out?.listError})`);
}

// ════ 7. digest の再現性 ════
const E = (id, over = {}) => ({ report_id: id, processing_status: 'DONE', created_time: '2026-09-01T00:00:00Z', data_start_time: '2026-08-01T00:00:00Z', data_end_time: null, report_document_id: `amzn1.doc.${id}`, ...over });
const d1 = inv.inventorySnapshotDigest([E('2'), E('10'), E('9')]);
ok(d1 === inv.inventorySnapshotDigest([E('9'), E('2'), E('10')]), '並びに依らない (report ID の順に並べ直す)');
ok(d1 === canonicalSha256([E('10'), E('2'), E('9')].map(({ report_id, processing_status, created_time, data_start_time, data_end_time, report_document_id }) => ({ report_id, processing_status, created_time, data_start_time, data_end_time, report_document_id }))), '並びは UTF-8 のバイトの順 ("10" < "2" < "9"・数の順ではない)');
ok(d1 !== inv.inventorySnapshotDigest([E('2', { processing_status: 'CANCELLED' }), E('10'), E('9')]), 'status が変われば digest が変わる');
ok(d1 !== inv.inventorySnapshotDigest([E('2', { data_end_time: '2026-08-15T00:00:00Z' }), E('10'), E('9')]), '期間が変われば digest が変わる');
ok(d1 === inv.inventorySnapshotDigest([{ ...E('2'), last_seen_ordinal: 3, import_result: 'imported' }, E('10'), E('9')]), '取込の結果・位置は digest に入れない (一覧の中身だけ)');
// 期待値 = 手で書いた正規の JSON '[{"created_time":…,"report_id":"10"},{…"2"},{…"9"}]' (鍵は名前の順・null は null) の SHA-256 (canonical-hash.mjs を通さずに計算)
eq(d1, '1af95e644f6383a90cd7c9b20dbe2007445c844a86354f19b34457ba701a8f32', '式の固定 (手で書いた正規の JSON の SHA-256 と一致・変えるときは版を足す)');
// UTF-16 の順と UTF-8 のバイトの順が違う例 (U+FF61 と U+1F600): UTF-8 では U+FF61 (EF..) が先
const d2 = inv.inventorySnapshotDigest([E('😀'), E('｡')]);
ok(d2 === canonicalSha256([E('｡'), E('😀')]), 'UTF-16 の順でなく UTF-8 のバイトの順 (U+FF61 が U+1F600 より先)');
let threw = false;
try { inv.inventorySnapshotDigest([E('1'), E('1')]); } catch { threw = true; }
ok(threw, '同じ report ID が 2 つある並びは例外 (黙って digest にしない)');
eq(inv.inventoryEntries([{ reportId: '5', processingStatus: 'IN_QUEUE' }, { processingStatus: 'DONE' }, { reportId: '5', processingStatus: 'DONE', reportDocumentId: 'd5' }], V2T), { entries: [{ report_id: '5', report_type: V2T, processing_status: 'DONE', created_time: null, data_start_time: null, data_end_time: null, report_document_id: 'd5', last_seen_ordinal: 3 }], missingId: 1 }, '同じ report ID は最後の状態・reportId の無いものは数える');

// 日時の正規化
eq(['2026-09-20T03:04:05+00:00', '2026-09-20T12:04:05+09:00', '2026-09-20T03:04:05.789Z', null, ''].map(inv.normalizeApiTime), ['2026-09-20T03:04:05Z', '2026-09-20T03:04:05Z', '2026-09-20T03:04:05Z', null, null], '日時 → UTC の YYYY-MM-DDTHH:MM:SSZ');
eq(['2026-02-30T00:00:00Z', '2026-09-20T03:04:05', '2026-09-20', 'x'].map(inv.normalizeApiTime), ['2026-02-30T00:00:00Z', '2026-09-20T03:04:05', '2026-09-20', 'x'], '読めない日時 (暦に無い・時差が無い・日付だけ) は元の文字のまま (null にしない = 期間が無いと区別)');
eq(inv.inventoryWindow(new Date('2026-01-01T00:00:00.999Z')), { createdSince: '2025-10-08T00:00:00Z', createdUntil: '2026-01-01T00:00:00Z' }, '窓 = 開始の時刻 (秒に切り捨て) から 85 日前');

// 表の決まり: 取込の結果は固定の集合
let chk = false;
try { db.prepare(`UPDATE amazon_settlement_report_inventory SET import_result = 'skipped_already' WHERE inventory_run_id = ?`).run(run1.id); } catch { chk = true; }
ok(chk, '取込の結果は固定の集合 (表の CHECK)');
let chk2 = false;
try { inv.recordImportResult(db, run1.id, 'R-IMP', 'satisfied'); } catch { chk2 = true; }
ok(chk2, '取込の結果は固定の集合 (記録の関数)');

// reportId の無い report は落とさず list_error に数える (完了の行も ⚠️)
sp = fakeSp({ inv: [{ reports: [R.imp, { processingStatus: 'DONE' }] }], ing: [{ reports: [] }] });
res = await captured(() => runSettlementFetch(ARGS, { db, sp, runId: 'run-8', downloadTsv: fakeDownload(), now: nowFn }));
const run8 = runs().at(-1);
ok(!res.error && run8.report_count === 1 && /reportId の無い report 1 件/.test(run8.list_error) && /⚠️ 決済の一覧 1 本/.test(res.logs.find((l) => l.includes('[settlements] 完了')) || ''), `reportId の無い report = list_error に数える・⚠️ (${run8.list_error})`);

console.log(failed ? `\n❌ ${failed} 件 失敗` : '\n=== 決済の一覧 (inventory) テスト ALL PASS ===');
process.exit(failed ? 1 : 0);
