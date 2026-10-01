import { temporaryTestRoot } from '../../scripts/test-temp-dir.mjs';
await temporaryTestRoot(import.meta.url);
/**
 * test-settlement-document-versions.js — 決済の文書の版 (D-66)・生の表の版 source_revision・読み直す注文・coverage の lease (D7b-1b-3) の試験
 *
 * 設計 = AI_reference システム設計/CompanyDB構想/13_Amazon利益のmart_設計_20260930.md §3.1・D-66
 *   ① 版の ID の形 (6 つの鍵・null は null・manual は file hash で区別) / 取込が版を登録し行が版を参照する / 要約 (件数・detail_digest・見出し・部品の合計)
 *   ② 採る版の規則 = JS (selectDocumentVersions) と SQL の view が一致 (層 → ingested_at の新しい順 → document_version_id のバイトの順・乱数 300 版)
 *   ③ V1 と V2 (並べ直した後) の中身が同じ = detail_digest と行の数が一致 / 相殺の +100 −100 だけ違う = 合計は同じでも digest が違う
 *   ④ 採る版が変わった取引 = 旧い版と新しい版の全部の注文を「読み直す注文」に (旧い版にだけある注文も) / 同じ report ID で file hash が変わる = 別の版
 *   ⑤ trigger: INSERT / UPDATE (OLD と NEW の両方) / DELETE で source_revision が増え、読み直す注文を記録 / backfill (版の鍵の null → 値) は数えない
 *   ⑥ R 以下だけ消す (後から入った記録を消さない)
 *   ⑦ 過去の行の backfill (report_document_id null で版) = 冪等・build が止まる / 動く
 *   ⑧ lease: 取る・生きている持ち主からは奪わない・死んだら奪う (token が変わる) = 古い token の子の取引は書けない (生の表・一覧)
 *   ⑨ build (SQLite の日次の財務) と送り手の変換が、版が 2 つある決済で「新しい版から消えた相殺の行」を両方とも数えない
 *
 * 実行: node apps/warehouse/test-settlement-document-versions.js (daily-sync 冒頭でも実行)。本番 DB には触れない (一時 DATA_DIR)
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'settlement-docver-test-'));
process.env.DATA_DIR = tmpDir;
const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..');
const { initDB, getDB } = await import('./db.js');
const { prepareReportTsv, prepareV2ReportTsv, ingestSettlement, runSettlementFetch } = await import('./fetch-amazon-settlements.js');
const { V1_COLUMNS, V2_COLUMNS } = await import('./amazon-settlement-v2.js');
const V = await import('./amazon-settlement-versions.js');
const { canonicalSha256 } = await import('../company-db/canonical-hash.mjs');
const { aggregateOrderFinance, filterSelectedRows, RAW_COLUMNS } = await import('../company-db/push/amazon-finance-transform.mjs');

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };
const throws = (fn, re, label) => { try { fn(); ok(false, `${label} (投げなかった)`); } catch (e) { ok(re.test(String(e && e.message)), `${label} (${String(e && e.message).slice(0, 100)})`); } };
const tsvOf = (cols, rows) => [cols.join('\t'), ...rows.map((r) => cols.map((c) => r[c] ?? '').join('\t'))].join('\n') + '\n';
const sha = (s) => crypto.createHash('sha256').update(s, 'utf8').digest('hex');

await initDB();
const db = getDB();
const rev = () => V.readSourceRevision(db);
const dirty = () => Object.fromEntries(V.readDirtyOrders(db).map((r) => [r.mall_order_no, r.revision]));
const nowJst = new Date(Date.now() + 9 * 3600 * 1000);
const YM = `${nowJst.getUTCFullYear()}-${String(nowJst.getUTCMonth() + 1).padStart(2, '0')}`;
const P = `${YM}-02T01:00:00+00:00`;

// ─── ① 版の ID ───
{
  const m = { source_layer: 'sp_api_v2', report_type: 'T', report_id: '1', report_document_id: 'D', file_hash: 'h', normalization_version: 'v2.0.0' };
  ok(V.documentVersionId(m) === canonicalSha256(m), '版の ID = 6 つの鍵の正規の JSON の SHA-256');
  ok(V.documentVersionId({ ...m, report_document_id: null }) !== V.documentVersionId(m), '文書 ID が違えば別の版');
  ok(V.documentVersionId({ ...m, normalization_version: 'v2.0.1' }) !== V.documentVersionId(m), 'parser の版が違えば別の版 (R24 M3)');
  ok(V.documentVersionId({ ...m, report_document_id: '' }) === V.documentVersionId({ ...m, report_document_id: null }), '空の文字は null');
  throws(() => V.documentVersionId({ ...m, normalization_version: '' }), /normalization_version/, 'parser の版が無ければ例外');
}

// ─── 取込: 版の登録・行の参照・要約 ───
const S1 = 'S-DV-1';
const v1line = (o) => ({ 'settlement-id': S1, 'transaction-type': 'Order', 'order-id': 'OA', 'merchant-order-id': 'OA', 'shipment-id': 'SH', 'marketplace-name': 'Amazon.co.jp', 'fulfillment-id': 'AFN', 'posted-date': P, 'order-item-code': 'I1', sku: 'SKU-A', ...o });
const HDR1 = { 'settlement-id': S1, 'settlement-start-date': `${YM}-01T00:00:00+00:00`, 'settlement-end-date': `${YM}-15T00:00:00+00:00`, 'deposit-date': `${YM}-17T00:00:00+00:00`, 'total-amount': '1300.00', currency: 'JPY' };
const V1A = tsvOf(V1_COLUMNS, [HDR1,
  v1line({ 'price-type': 'Principal', 'price-amount': '1000.00' }), v1line({ 'quantity-purchased': '1' }),
  v1line({ 'order-id': 'OB', 'merchant-order-id': 'OB', 'order-item-code': 'I2', 'price-type': 'Principal', 'price-amount': '300.00' }),
  v1line({ 'order-id': 'OB', 'merchant-order-id': 'OB', 'order-item-code': 'I2', 'transaction-type': 'Other', 'price-type': 'Something', 'other-amount': '100.00' }),
  v1line({ 'order-id': 'OB', 'merchant-order-id': 'OB', 'order-item-code': 'I2', 'transaction-type': 'Other', 'price-type': 'Something', 'other-amount': '-100.00' }),
]);
const pA = prepareReportTsv(V1A, 'R-A', 'run-a', { reportDocumentId: 'DOC-A' });
const r0 = rev();
const ra = ingestSettlement(db, pA.headerRow, pA.lineRows, pA.ctx);
const verA = db.prepare(`SELECT * FROM amazon_settlement_document_versions WHERE seq = ?`).get(ra.documentVersionSeq);
ok(verA && verA.document_version_id === pA.ctx.documentVersionId && verA.report_id === 'R-A' && verA.report_document_id === 'DOC-A' && verA.settlement_id === S1 && verA.registered_by === 'ingest', '取込が版を登録 (report ID・文書 ID・決済 ID)');
ok(db.prepare(`SELECT COUNT(*) n FROM raw_amazon_settlement_lines WHERE document_version_seq = ?`).get(verA.seq).n === 5 && db.prepare(`SELECT document_version_seq s, currency_raw c, report_document_id d, normalization_version nv FROM raw_amazon_settlement_headers WHERE source_settlement_id = ?`).get(S1).s === verA.seq, '明細 5 行・見出しが版を参照');
const hA = db.prepare(`SELECT currency_raw c, report_document_id d, normalization_version nv FROM raw_amazon_settlement_headers WHERE source_settlement_id = ?`).get(S1);
ok(hA.c === 'JPY' && hA.d === 'DOC-A' && hA.nv === 'v1.0.0', '見出し: 原文の通貨・文書 ID・parser の版');
ok(verA.detail_stale === 0 && verA.line_count === 5 && verA.header_count === 1 && Number(verA.components_sum_micro) === 1300e6 && Number(verA.header_total_micro) === 1300e6 && verA.line_settlement_count === 1, `要約: 5 行・見出し 1・部品の合計 = 見出しの total (${verA.components_sum_micro})`);
ok(rev() === r0 + 6, `🚨 INSERT の trigger で版が 1 行ずつ増える (見出し 1 + 明細 5 = +6: ${r0} → ${rev()})`);
ok(JSON.stringify(Object.keys(dirty()).sort()) === JSON.stringify(['OA', 'OB']), `読み直す注文 = OA・OB (${Object.keys(dirty())})`);
ok(ingestSettlement(db, pA.headerRow, pA.lineRows, pA.ctx).lineInserted === 0 && rev() === r0 + 6, '同じ版を入れ直しても行も版も増えない (冪等)');

// ─── ③ V1 と V2 の同じ中身 = digest 一致 / 相殺の行だけ違う = 合計は同じでも digest が違う ───
const V2D = (s) => s.replace(/-/g, '/').replace('T', ' ').replace('+00:00', ' UTC');
const v2line = (o) => ({ 'settlement-id': S1, 'transaction-type': 'Order', 'order-id': 'OA', 'merchant-order-id': 'OA', 'shipment-id': 'SH', 'marketplace-name': 'Amazon.co.jp', 'fulfillment-id': 'AFN',
  'posted-date': V2D(P).slice(0, 10), 'posted-date-time': V2D(P), 'order-item-code': 'I1', sku: 'SKU-A', 'quantity-purchased': '1', 'amount-type': 'ItemPrice', 'amount-description': 'Principal', ...o });
const HDR2 = { 'settlement-id': S1, 'settlement-start-date': V2D(HDR1['settlement-start-date']), 'settlement-end-date': V2D(HDR1['settlement-end-date']), 'deposit-date': V2D(HDR1['deposit-date']), 'total-amount': '1300.00', currency: 'JPY' };
const v1Digest = V.detailDigestOfRows(pA.lineRows);
// V2 は相殺の 2 行を持たない (V1 の「Other / Something」の行は V2 の並べ直しの規則に無い = ここでは本体 2 行だけの別の決済で確かめる)
{
  const S9 = 'S-DV-9';
  const a = prepareReportTsv(tsvOf(V1_COLUMNS, [{ ...HDR1, 'settlement-id': S9, 'total-amount': '1000.00' }, v1line({ 'settlement-id': S9, 'price-type': 'Principal', 'price-amount': '1000.00' }), v1line({ 'settlement-id': S9, 'quantity-purchased': '1' })]), 'R-9', 'x');
  const b = prepareV2ReportTsv(tsvOf(V2_COLUMNS, [{ ...HDR2, 'settlement-id': S9, 'total-amount': '1000.00' }, v2line({ 'settlement-id': S9, amount: '1000.00' })]), 'R-9v2', 'x');
  const da = V.detailDigestOfRows(a.lineRows), dbb = V.detailDigestOfRows(b.lineRows);
  ok(da.lineCount === dbb.lineCount && da.detailDigest === dbb.detailDigest, `V1 と V2 (並べ直した後) の中身が同じ = 行の数と detail_digest が一致 (${da.lineCount} 行)`);
  ok(a.ctx.documentVersionId !== b.ctx.documentVersionId, 'V1 と V2 は別の版');
}
const withoutOffset = V.detailDigestOfRows(pA.lineRows.filter((r) => r.other_amount_micro == null));
ok(withoutOffset.componentsSumMicro === v1Digest.componentsSumMicro && withoutOffset.detailDigest !== v1Digest.detailDigest, '🚨 相殺の +100 / −100 だけ違う = 部品の合計は同じでも detail_digest は違う (合計の一致では満たさない)');

// ─── ④ 採る版が変わる: 新しい版 (同じ report ID・別の file = 別の版) から OB が消えた ───
const V1B = tsvOf(V1_COLUMNS, [{ ...HDR1, 'total-amount': '1000.00' }, v1line({ 'price-type': 'Principal', 'price-amount': '1000.00' }), v1line({ 'quantity-purchased': '1' })]);
V.clearDirtyOrders(db, ['OA', 'OB'], rev());
ok(Object.keys(dirty()).length === 0, '前提: 読み直す注文を消した');
const pB = prepareReportTsv(V1B, 'R-A', 'run-b', { reportDocumentId: 'DOC-A' });   // 同じ report ID・同じ文書 ID・別の file hash
ok(pB.ctx.documentVersionId !== pA.ctx.documentVersionId, '同じ report ID で file hash が変わる = 別の版');
const rb = ingestSettlement(db, pB.headerRow, pB.lineRows, pB.ctx);
ok(rb.selectedChanged === true && V.selectedVersionOf(db, S1).seq === rb.documentVersionSeq, '採る版 = 新しい版 (ingested_at の新しい順)');
ok(Object.hasOwn(dirty(), 'OB') && Object.hasOwn(dirty(), 'OA'), `🚨 旧い版にだけある注文 OB も読み直す注文に (新しい版に INSERT が無くても・R22 H3) (${Object.keys(dirty())})`);
// 送り手の変換: 採った版の行だけ = OB は 0 行 (墓石) / OA は新しい版の 1,000 円
const rowsOf = (no) => db.prepare(`SELECT ${RAW_COLUMNS.join(', ')} FROM raw_amazon_settlement_lines WHERE amazon_order_id = ?`).safeIntegers(true).all(no);
const selMap = new Map([...V.selectDocumentVersions(V.readVersions(db))].map(([k, v]) => [k, v.seq]));
ok(filterSelectedRows(rowsOf('OB'), selMap).length === 0, '変換: OB の行は旧い版にしか無い = 採った版では 0 行 (墓石を送る)');
const aggA = aggregateOrderFinance('OA', filterSelectedRows(rowsOf('OA'), selMap));
ok(aggA.lines.length === 1 && aggA.lines[0].sales_principal_jpy === 1000 && aggA.stats.dedupRows === 2, `変換: OA = 新しい版の 2 行だけ (${aggA.stats.dedupRows} 行・${aggA.lines[0] && aggA.lines[0].sales_principal_jpy} 円)`);
// 1 つの注文が 2 つの決済に: S2 にも OB がある = S1 の新しい版から消えても S2 に残る = 墓石にしない
{
  const S2 = 'S-DV-2';
  const p2 = prepareReportTsv(tsvOf(V1_COLUMNS, [{ ...HDR1, 'settlement-id': S2, 'total-amount': '50.00' }, v1line({ 'settlement-id': S2, 'order-id': 'OB', 'merchant-order-id': 'OB', 'transaction-type': 'Refund', 'adjustment-id': 'AJ', 'price-type': 'Principal', 'price-amount': '50.00' })]), 'R-S2', 'x');
  ingestSettlement(db, p2.headerRow, p2.lineRows, p2.ctx);
  const sel2 = new Map([...V.selectDocumentVersions(V.readVersions(db))].map(([k, v]) => [k, v.seq]));
  const kept = filterSelectedRows(rowsOf('OB'), sel2);
  ok(kept.length === 1 && kept[0].source_settlement_id === S2, '🚨 注文 OB が決済 S1 と S2 にあり、S1 の新しい版からだけ消えた = 採っている全部の決済の和集合では 1 行 = 墓石にしない (R23 M2)');
}

// ─── ② 採る版の規則: JS と SQL の view が一致 (乱数・同順位を多く) ───
{
  const layers = ['sp_api_v1', 'sp_api_v2', 'manual_csv', 'other'];
  const times = ['2026-01-01 00:00:00', '2026-01-01 00:00:00', '2026-02-01 00:00:00', '2026-03-01 00:00:00'];
  let seed = 7; const rnd = (n) => { seed = (seed * 1103515245 + 12345) % 2147483648; return seed % n; };
  for (let i = 0; i < 300; i++) {
    const meta = { source_layer: layers[rnd(4)], report_type: null, report_id: `FZ-${i}`, report_document_id: null, file_hash: `fz${i}`, normalization_version: 'v' };
    const v = V.registerDocumentVersion(db, { ...meta, document_version_id: V.documentVersionId(meta), source_document_id: `FZ-${i}`, settlement_id: `FZ-S${rnd(25)}`, ingested_at: times[rnd(4)] }, { registeredBy: 'backfill' });
    // 🆕 #1567 R1 Medium 3: 中身の確かな版 (detail_valid = 1) だけが候補 = 乱数で 1 / 0 / null を混ぜる
    db.prepare(`UPDATE amazon_settlement_document_versions SET detail_valid = ?, header_count = ?, detail_stale = 0 WHERE seq = ?`).run([1, 1, 0, null][rnd(4)], [1, 1, 0, 2, null][rnd(5)], v.seq);   // R2: 見出しの行数も混ぜる
  }
  const js = V.selectDocumentVersions(V.readVersions(db));
  const sql = db.prepare(`SELECT settlement_id, document_version_seq FROM v_amazon_settlement_selected_documents`).all();
  ok(sql.length === js.size && sql.every((r) => js.get(r.settlement_id) && js.get(r.settlement_id).seq === r.document_version_seq), `🚨 採る版: JS の selectDocumentVersions = SQL の view (${sql.length} 決済・層 → 新しい順 → ID のバイトの順)`);
  // 手で: 層 1 が 2 より先 (manual が新しくても) / 同じ層・同じ時刻 = ID の小さい方
  const pick = (vs) => V.selectDocumentVersions(vs.map((v, i) => ({ settlement_id: 'X', seq: i, detail_valid: 1, header_count: 1, ...v }))).get('X')?.seq ?? null;
  // 🚨 採る版に要る列が欠けた行を渡すと throw (#1567 Codex R2 High 1: 送り手の SQL に header_count が無く、黙って別の版を採っていた)
  throws(() => V.selectDocumentVersions([{ settlement_id: 'X', seq: 1, document_version_id: 'a', source_layer: 'sp_api_v1', ingested_at: '2026', detail_valid: 1 }]), /header_count/,
    '🚨 selectDocumentVersions の入口: 列 (header_count) が欠けた版の行は throw (呼び手ごとに列の集まりが違うと採る版が変わる)');
  ok(V.VERSION_SELECT_COLUMNS.includes('header_count') && V.VERSION_SELECT_COLUMNS.includes('detail_valid'), '採る版に要る列 = detail_valid・header_count を含む');
  ok(pick([{ source_layer: 'manual_csv', ingested_at: '2027', document_version_id: 'a' }, { source_layer: 'sp_api_v2', ingested_at: '2026', document_version_id: 'b' }]) === 1, '層 1 (API) が manual より先 (manual が新しくても)');
  ok(pick([{ source_layer: 'sp_api_v1', ingested_at: '2026', document_version_id: 'b' }, { source_layer: 'sp_api_v2', ingested_at: '2026', document_version_id: 'a' }]) === 1, 'V1 と V2 は同じ順位 → 同じ時刻なら ID のバイトの順');
  ok(pick([{ source_layer: 'sp_api_v1', ingested_at: '2026', document_version_id: 'a' }, { source_layer: 'sp_api_v2', ingested_at: '2027', document_version_id: 'b', detail_valid: 0 }]) === 0, '🚨 中身の悪い新しい版 (detail_valid 0) は良い旧い版を押しのけない (#1567 R1 Medium 3)');
  ok(pick([{ source_layer: 'sp_api_v2', ingested_at: '2027', document_version_id: 'b', detail_valid: 0 }]) === 0, '中身の悪い版しか無い = その中で一番の版を仮に採る (#1567 R2 Medium 1・全部を止めない)');
  ok(pick([{ source_layer: 'sp_api_v1', ingested_at: '2026', document_version_id: 'a', header_count: 1 }, { source_layer: 'sp_api_v1', ingested_at: '2027', document_version_id: 'b', header_count: 0 }]) === 0,
    '🚨 見出し 0 行の新しい版 (過去の backfill・detail_valid 1) は見出し 1 行の古い版を押しのけない (#1567 R2 High 1)');
  ok(pick([{ source_layer: 'sp_api_v1', ingested_at: '2026', document_version_id: 'a', header_count: 0 }]) === 0, '見出し 0 行の版はほかに候補が無いときだけ採る');
  ok(pick([{ source_layer: 'sp_api_v1', ingested_at: '2027', document_version_id: 'a', header_count: 1, detail_valid: 0 }, { source_layer: 'sp_api_v1', ingested_at: '2026', document_version_id: 'b', header_count: 0 }]) === 1,
    '並びの頭は中身の確かさ (detail_valid) → 見出しの行数');
}

// ─── ⑤ trigger: UPDATE は OLD と NEW の両方 / DELETE / backfill は数えない ───
{
  V.clearDirtyOrders(db, Object.keys(dirty()), rev());
  const id = db.prepare(`SELECT id FROM raw_amazon_settlement_lines WHERE amazon_order_id = 'OA' AND document_version_seq = ? LIMIT 1`).get(rb.documentVersionSeq).id;
  const before = rev();
  db.prepare(`UPDATE raw_amazon_settlement_lines SET amazon_order_id = 'OC' WHERE id = ?`).run(id);
  ok(rev() === before + 1 && Object.hasOwn(dirty(), 'OA') && Object.hasOwn(dirty(), 'OC'), `🚨 UPDATE = 版 +1・読み直す注文は OLD (OA) と NEW (OC) の両方 (${Object.keys(dirty())})`);
  ok(db.prepare(`SELECT detail_stale s FROM amazon_settlement_document_versions WHERE seq = ?`).get(rb.documentVersionSeq).s === 1, 'UPDATE で版の要約が古い印に');
  db.prepare(`UPDATE raw_amazon_settlement_lines SET amazon_order_id = 'OA' WHERE id = ?`).run(id);
  V.refreshStaleVersionDetails(db);
  const b2 = rev();
  db.prepare(`DELETE FROM raw_amazon_settlement_lines WHERE amazon_order_id = 'OB' AND source_settlement_id = ?`).run(S1);
  ok(rev() > b2 && Object.hasOwn(dirty(), 'OB'), 'DELETE = 版が増え・読み直す注文に記録');
  // 過去の行 (版なし) を直接入れる → backfill の UPDATE は数えない (最後に 1 つだけ進める)
  const ins = db.prepare(`INSERT INTO raw_amazon_settlement_lines (physical_line_hash, business_line_key, source_document_id, source_file_hash, source_path, source_line_no, source_layer, parser_version,
    source_settlement_id, posted_date_utc, posted_datetime_jst, economic_date, year_month_int, amazon_order_id, seller_sku_normalized, transaction_type, price_type, price_amount_micro, currency, ingested_at)
    VALUES (?, ?, 'LEG-1', 'lh', 'p', ?, 'sp_api_v1', 'v1.0.0', 'S-LEG', 'x', 'x', ?, ?, ?, 'sku-l', 'Order', 'Principal', 1000000, 'JPY', '2026-01-01 00:00:00')`);
  for (let i = 1; i <= 5; i++) ins.run(`leg-${i}`, `k-${i}`, i, `${YM}-03`, Number(YM.replace('-', '')), `OL${i}`);
  ok(!V.documentVersionsReady(db), '版の無い行がある = ready でない (build・送り手は止まる)');
  let out = '';
  try { execFileSync(process.execPath, ['scripts/amazon-finance/build-daily-fact.js', '--data-dir', tmpDir, '--month', YM], { cwd: repoRoot, env: { ...process.env, DATA_DIR: tmpDir }, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }); } catch (e) { out = String(e.stdout || '') + String(e.stderr || ''); }
  ok(/文書の版/.test(out), '🚨 版の無い行があれば build は止まる (黙って行を落とさない)');
  V.clearDirtyOrders(db, Object.keys(dirty()), rev());
  const b3 = rev();
  const bf = V.backfillDocumentVersions(db, { batchIds: 2 });
  ok(bf.groups === 1 && bf.lines === 5 && V.documentVersionsReady(db), `backfill: 1 文書・5 行 (id の範囲ごと) → ready (${JSON.stringify(bf)})`);
  ok(rev() === b3 + 1 && Object.keys(dirty()).length === 0, `🚨 backfill の UPDATE は数えない (版は最後に +1 だけ・読み直す注文は 0) (${b3} → ${rev()})`);
  const leg = db.prepare(`SELECT * FROM amazon_settlement_document_versions WHERE source_document_id = 'LEG-1'`).get();
  ok(leg && leg.report_id === 'LEG-1' && leg.report_document_id === null && leg.file_hash === 'lh' && leg.normalization_version === 'v1.0.0' && leg.settlement_id === 'S-LEG' && leg.line_count === 5 && leg.header_count === 0,
    '過去の行の版 = report ID は source_document_id・文書 ID は null・parser の版 = normalization_version・見出しが無い (header_count 0)');
  ok(leg.document_version_id === V.documentVersionId({ source_layer: 'sp_api_v1', report_type: V.REPORT_TYPES.v1, report_id: 'LEG-1', report_document_id: null, file_hash: 'lh', normalization_version: 'v1.0.0' }), '過去の行の版の ID の規則');
  ok(db.prepare(`SELECT COUNT(*) n FROM raw_amazon_settlement_lines WHERE source_document_id = 'LEG-1' AND normalization_version = 'v1.0.0'`).get().n === 5, 'backfill は normalization_version も埋める');
  ok(V.backfillDocumentVersions(db).groups === 0, 'backfill は冪等 (2 回目は何もしない)');
}

// ─── 🆕 #1567 R1 High 1: backfill が途中で止まる (行の UPDATE の後・要約の前 / 登録の後・行の前) = ready にしない (build が行を黙って落とさない) ───
{
  const insLeg = (doc, sid, n0) => {
    const ins = db.prepare(`INSERT INTO raw_amazon_settlement_lines (physical_line_hash, business_line_key, source_document_id, source_file_hash, source_path, source_line_no, source_layer, parser_version,
      source_settlement_id, posted_date_utc, posted_datetime_jst, economic_date, year_month_int, amazon_order_id, seller_sku_normalized, transaction_type, price_type, price_amount_micro, currency, ingested_at)
      VALUES (?, ?, ?, 'kh', 'p', ?, 'sp_api_v1', 'v1.0.0', ?, 'x', 'x', ?, ?, ?, 'sku-k', 'Order', 'Principal', 1000000, 'JPY', '2026-01-01 00:00:00')`);
    for (let i = 1; i <= 3; i++) ins.run(`${doc}-${i}`, `${doc}-k${i}`, doc, i, sid, `${YM}-04`, Number(YM.replace('-', '')), `${doc}-O${i}`);
  };
  const buildFails = () => { let out = ''; try { execFileSync(process.execPath, ['scripts/amazon-finance/build-daily-fact.js', '--data-dir', tmpDir, '--month', YM], { cwd: repoRoot, env: { ...process.env, DATA_DIR: tmpDir }, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }); } catch (e) { out = String(e.stdout || '') + String(e.stderr || ''); } return out; };
  const stopAt = (k) => { let n = 0; return () => { n++; if (n >= k) throw new Error(`わざと止めた (${k} 回目)`); }; };
  // (1) 行の UPDATE の後・要約 (refresh) の前で止める: check = 登録 1 → 明細 2 → 版の +1 3 → 要約 4
  insLeg('KILL-A', 'S-KILL-A');
  let stopped = null; try { V.backfillDocumentVersions(db, { check: stopAt(4) }); } catch (e) { stopped = e.message; }
  const va = db.prepare(`SELECT * FROM amazon_settlement_document_versions WHERE source_document_id = 'KILL-A'`).get();
  ok(/4 回目/.test(stopped || '') && db.prepare(`SELECT COUNT(*) n FROM raw_amazon_settlement_lines WHERE source_document_id = 'KILL-A' AND document_version_seq IS NULL`).get().n === 0, `前提: 行の UPDATE は済み・要約の前で止まった (${stopped})`);
  ok(va && va.settlement_id === 'S-KILL-A' && va.detail_stale === 1, `🚨 (a) 決済 ID は版の登録と同じ取引で入る (止まっても決済 ID の無い版を残さない) (${va && va.settlement_id}・要約が古い ${va && va.detail_stale})`);
  const pa = V.documentVersionProblems(db).map((p) => p.code);
  ok(pa.includes('version_incomplete') && !V.documentVersionsReady(db), `🚨 (b) 要約の古い版があれば ready にしない (${pa.join(', ')})`);
  ok(/要約が古い/.test(buildFails()), '🚨 build は止まる (0 行・0 円で作り直さない)');
  // (2) 版の登録の後・行の UPDATE の前で止める
  insLeg('KILL-B', 'S-KILL-B');
  stopped = null; try { V.backfillDocumentVersions(db, { check: stopAt(2) }); } catch (e) { stopped = e.message; }
  const pb = V.documentVersionProblems(db).map((p) => p.code);
  ok(/2 回目/.test(stopped || '') && pb.includes('rows_without_version'), `登録の後・行の前で止まった = 版の無い行が残る = ready にしない (${pb.join(', ')})`);
  // 流し直すと両方そろう (冪等・古い要約も作り直す)
  const again = V.backfillDocumentVersions(db);
  ok(V.documentVersionsReady(db) && again.refreshed >= 2, `流し直すと ready (要約を作り直した版 ${again.refreshed})`);
  ok(!/文書の版|要約が古い/.test(buildFails()), '流し直した後は build が通る');
}

// ─── 🆕 #1567 R1 Medium 3: 中身の悪い新しい版は採らない・悪い版しか無い決済は blocked (build と送り手を止める) ───
{
  const Sbad = 'S-DV-BAD';
  const good = prepareReportTsv(tsvOf(V1_COLUMNS, [{ ...HDR1, 'settlement-id': Sbad, 'total-amount': '1000.00' }, v1line({ 'settlement-id': Sbad, 'order-id': 'OG', 'price-type': 'Principal', 'price-amount': '1000.00' })]), 'R-G', 'x');
  const rg = ingestSettlement(db, good.headerRow, good.lineRows, good.ctx, { now: () => new Date('2026-09-01T00:00:00Z') });
  // 新しい版: 見出しの total が明細の合計と違う (途中で切れたファイル)
  const bad = prepareReportTsv(tsvOf(V1_COLUMNS, [{ ...HDR1, 'settlement-id': Sbad, 'total-amount': '1000.00' }, v1line({ 'settlement-id': Sbad, 'order-id': 'OG', 'price-type': 'Principal', 'price-amount': '999.00' })]), 'R-G', 'x2', { reportDocumentId: 'DOC-NEW' });
  const rbad = ingestSettlement(db, bad.headerRow, bad.lineRows, bad.ctx, { now: () => new Date('2026-09-02T00:00:00Z') });
  const vbad = db.prepare(`SELECT detail_valid FROM amazon_settlement_document_versions WHERE seq = ?`).get(rbad.documentVersionSeq);
  ok(vbad.detail_valid === 0 && rbad.selectedChanged === false && V.selectedVersionOf(db, Sbad).seq === rg.documentVersionSeq, '🚨 新しい版の部品の合計 ≠ total = 候補にしない = 良い旧い版のまま (押しのけない)');
  const sqlSel = db.prepare(`SELECT document_version_seq s FROM v_amazon_settlement_selected_documents WHERE settlement_id = ?`).get(Sbad);
  ok(sqlSel && sqlSel.s === rg.documentVersionSeq, 'SQL の view も同じ (旧い版)');
  ok(V.documentVersionsReady(db), '良い版がある決済は blocked にしない');
  // 🆕 #1567 Codex R3 High 2: 見出しが 2 行の文書 (V1) = parser は 2 行とも返す・取込は拒む (版も行も作らない)
  const S2b = 'S-DV-BAD2';
  const two = prepareReportTsv(tsvOf(V1_COLUMNS, [{ ...HDR1, 'settlement-id': S2b, 'total-amount': '10.00' }, { ...HDR1, 'settlement-id': S2b, 'total-amount': '11.00' }, v1line({ 'settlement-id': S2b, 'order-id': 'OB2', 'price-type': 'Principal', 'price-amount': '10.00' })]), 'R-B2', 'x');
  ok(two.headerRowCount === 2 && two.headerRows.length === 2 && String(two.headerRows[0].total_amount_micro) !== String(two.headerRows[1].total_amount_micro), '🚨 parser は見出しを 2 行とも返す (前は最後の 1 行に潰した)');
  throws(() => ingestSettlement(db, two.headerRow, two.lineRows, two.ctx), /見出しが 2 行/, '🚨 V1: 見出しが 2 行の文書は取り込まない');
  ok(!db.prepare(`SELECT 1 FROM amazon_settlement_document_versions WHERE settlement_id = ?`).get(S2b) && !db.prepare(`SELECT 1 FROM raw_amazon_settlement_lines WHERE source_settlement_id = ?`).get(S2b),
    'V1: 版も行も作らない (仮に採る壊れた版にもならない)');
  const { inspectManualFile } = await import('./amazon-settlement-manual-file.js');
  ok(inspectManualFile(tsvOf(V1_COLUMNS, [{ ...HDR1, 'settlement-id': S2b, 'total-amount': '10.00' }, { ...HDR1, 'settlement-id': S2b, 'total-amount': '10.00' }, v1line({ 'settlement-id': S2b, 'order-id': 'OB2', 'price-type': 'Principal', 'price-amount': '10.00' })]), { format: 'v1' })
    .problems.some((p) => /見出しの行が 2 行/.test(p)), '手のファイル (V1) も見出しが 2 行なら積まない');
  // 悪い版しか無い決済 (見出しの total ≠ 明細の合計)
  const bad1 = prepareReportTsv(tsvOf(V1_COLUMNS, [{ ...HDR1, 'settlement-id': S2b, 'total-amount': '11.00' }, v1line({ 'settlement-id': S2b, 'order-id': 'OB2', 'price-type': 'Principal', 'price-amount': '10.00' })]), 'R-B2', 'x');
  const rb1 = ingestSettlement(db, bad1.headerRow, bad1.lineRows, bad1.ctx);
  // 🚨 版の見出しの数 = 物理の行の数: 同じ鍵 (business_line_key) の見出しを直接もう 1 行 (過去の行の形) = 2 と数える (前は鍵の種類で 1)
  const hsrc = db.prepare(`SELECT * FROM raw_amazon_settlement_headers WHERE source_settlement_id = ?`).get(S2b);
  const { id: _hid, ...hrest } = hsrc;
  db.prepare(`INSERT INTO raw_amazon_settlement_headers (${Object.keys(hrest).join(',')}) VALUES (${Object.keys(hrest).map((k) => '@' + k).join(',')})`).run({ ...hrest, physical_line_hash: hrest.physical_line_hash + '-2' });
  V.refreshStaleVersionDetails(db);
  const vh = db.prepare(`SELECT header_count, detail_valid FROM amazon_settlement_document_versions WHERE seq = ?`).get(rb1.documentVersionSeq);
  ok(vh.header_count === 2 && vh.detail_valid === 0, `🚨 版の見出しの数 = 物理の行の数 (同じ鍵の 2 行も 2・detail_valid 0) (${vh.header_count}・${vh.detail_valid})`);
  // 🆕 #1567 R2 Medium 1 (案 C): 良い版が無い決済 = 中身の悪い版を仮に採る (PR の前もその行で build していた)・全部を止めない・朝の報告と coverage の理由に出す
  ok(V.documentVersionsReady(db) && !V.documentVersionProblems(db).length, '🚨 壊れた版しか無い決済があっても build と送り手は止めない (1 つの決済の違反で全部を止めない)');
  const warns = V.documentVersionWarnings(db);
  ok(warns.some((w) => w.code === 'provisional_broken_version' && w.human && w.detail.includes(S2b)), `「🚨 仮に採った壊れた版」として出す (${warns.map((w) => w.code).join(', ')})`);
  ok(V.selectedVersionOf(db, S2b) && db.prepare(`SELECT COUNT(*) n FROM v_amazon_settlement_unified WHERE source_settlement_id = ?`).get(S2b).n === 1, '仮に採った版の行は build に入る (黙って落とさない)');
  // 片付け (以降の試験のため): 壊れた決済の行を消す
  db.prepare(`DELETE FROM raw_amazon_settlement_headers WHERE source_settlement_id = ?`).run(S2b);
  db.prepare(`DELETE FROM raw_amazon_settlement_lines WHERE source_settlement_id = ?`).run(S2b);
  V.refreshStaleVersionDetails(db);
  ok(V.documentVersionsReady(db) && !V.documentVersionWarnings(db).some((w) => w.code === 'provisional_broken_version'), '行の無い版は数えない');
}

// ─── 🆕 #1567 R2 Medium 2 / R3 Medium 1: 決済 ID が 2 つある過去の文書 (3 つの形) = 版を付けずに止まる (明示の allowUnresolved だけ進める) ───
//   形 1 = 見出しが 2 つ (S-U1 と S-U2)・明細は S-U1 / 形 2 = 見出しは S-U1・明細は S-U1 と S-U2 / 形 3 = 見出しなし・明細は S-U1 と S-U2
{
  const { runMigrate } = await import('./migrate-settlement-document-versions.js');
  const ym = Number(YM.replace('-', ''));
  const insL = db.prepare(`INSERT INTO raw_amazon_settlement_lines (physical_line_hash, business_line_key, source_document_id, source_file_hash, source_path, source_line_no, source_layer, parser_version,
    source_settlement_id, posted_date_utc, posted_datetime_jst, economic_date, year_month_int, amazon_order_id, seller_sku_normalized, transaction_type, price_type, price_amount_micro, currency, ingested_at)
    VALUES (?, ?, ?, 'uh', 'p', ?, 'sp_api_v1', 'v1.0.0', ?, 'x', 'x', ?, ?, ?, 'sku-u', 'Order', 'Principal', 1000000, 'JPY', '2026-01-01 00:00:00')`);
  const insH = db.prepare(`INSERT INTO raw_amazon_settlement_headers (physical_line_hash, business_line_key, source_document_id, source_file_hash, source_path, source_line_no, source_layer, parser_version,
    source_settlement_id, settlement_start_date, settlement_end_date, total_amount_micro, currency, ingested_at)
    VALUES (?, ?, ?, 'uh', 'p', 0, 'sp_api_v1', 'v1.0.0', ?, ?, ?, 1000000, 'JPY', '2026-01-01 00:00:00')`);
  const FORMS = [
    { n: 1, headers: ['S-U1', 'S-U2'], lines: ['S-U1'] },
    { n: 2, headers: ['S-U1'], lines: ['S-U1', 'S-U2'] },
    { n: 3, headers: [], lines: ['S-U1', 'S-U2'] },
  ];
  for (const form of FORMS) {
    const doc = `UNRES-${form.n}`;
    form.headers.forEach((sid, i) => insH.run(`${doc}-h${i}`, `${doc}-hk${i}`, doc, sid, `${YM}-01T00:00:00+00:00`, `${YM}-15T00:00:00+00:00`));
    form.lines.forEach((sid, i) => insL.run(`${doc}-l${i}`, `${doc}-lk${i}`, doc, i + 1, sid, `${YM}-05`, ym, `${doc}-O${i}`));
    const logs = [];
    await runMigrate({ commit: false, log: (m) => logs.push(m) });
    ok(logs.some((l) => l.includes(`決済 ID が 2 つ以上ある文書 1 (${doc} 2)`)), `形 ${form.n}: 版付けの dry-run で「決済 ID が 2 つ以上ある文書」を数える`);
    let threw = null; try { V.backfillDocumentVersions(db); } catch (e) { threw = e; }
    const nullRows = db.prepare(`SELECT COUNT(*) n FROM raw_amazon_settlement_lines WHERE source_document_id = ? AND document_version_seq IS NULL`).get(doc).n;
    ok(threw && threw.code === 'UNRESOLVED_SETTLEMENT_DOCS' && nullRows === form.lines.length && !db.prepare(`SELECT 1 FROM amazon_settlement_document_versions WHERE source_document_id = ?`).get(doc),
      `🚨 形 ${form.n}: 版を付けずに止まる (行は版の無いまま・版も作らない) (${threw && threw.code})`);
    ok(V.documentVersionProblems(db).some((p) => p.code === 'rows_without_version') && !V.documentVersionsReady(db), `形 ${form.n}: build と送り手は「版の無い行」で止まる (S-U2 の行を黙って落とさない・墓石を送らない)`);
    let mthrew = null; try { await runMigrate({ commit: true, log: () => {}, isAlive: () => false }); } catch (e) { mthrew = e; }
    ok(mthrew && mthrew.code === 'UNRESOLVED_SETTLEMENT_DOCS', `形 ${form.n}: migrate --commit も止まる (${mthrew && mthrew.code})`);
    // 明示の allowUnresolved (人が確かめた後) だけ進める = 決済 ID が null の版・止めない・人が直す code
    await runMigrate({ commit: true, allowUnresolved: true, log: () => {}, isAlive: () => false });
    const v = db.prepare(`SELECT seq, settlement_id FROM amazon_settlement_document_versions WHERE source_document_id = ?`).get(doc);
    V.refreshVersionDetail(db, v.seq);   // 要約を作り直しても見出しの 1 行目で決済 ID を埋めない (和集合が 2 つ)
    const v2 = db.prepare(`SELECT settlement_id FROM amazon_settlement_document_versions WHERE seq = ?`).get(v.seq);
    ok(v.settlement_id === null && v2.settlement_id === null, `🚨 形 ${form.n}: 決済 ID は見出しと明細の和集合で決める = 決まらない = null (見出しの 1 行目の ${form.headers[0] || '-'} にしない)`);
    ok(V.documentVersionsReady(db) && V.documentVersionWarnings(db).some((w) => w.code === 'version_unresolved_settlement' && w.detail.includes(doc)),
      `形 ${form.n}: allowUnresolved の後は止めない・「🚨 決済 ID の決まらない版」(人が直す)`);
    ok(!V.selectedVersionOf(db, 'S-U1') || V.selectedVersionOf(db, 'S-U1').seq !== v.seq, `形 ${form.n}: その版はどの決済にも採られない (S-U1 の壊れた版として仮に採らない)`);
    db.prepare(`DELETE FROM raw_amazon_settlement_lines WHERE source_document_id = ?`).run(doc);   // 片付け
    db.prepare(`DELETE FROM raw_amazon_settlement_headers WHERE source_document_id = ?`).run(doc);
    V.refreshStaleVersionDetails(db);
  }
}

// ─── 🆕 #1567 Codex R3 High 2: 過去の行の版付け = 見出しの物理の行が 2 行の文書 (同じ鍵でも) は版を付けずに止まる ───
{
  const { runMigrate } = await import('./migrate-settlement-document-versions.js');
  const ym = Number(YM.replace('-', ''));
  const doc = 'MULTI-H';
  db.prepare(`INSERT INTO raw_amazon_settlement_lines (physical_line_hash, business_line_key, source_document_id, source_file_hash, source_path, source_line_no, source_layer, parser_version,
    source_settlement_id, posted_date_utc, posted_datetime_jst, economic_date, year_month_int, amazon_order_id, seller_sku_normalized, transaction_type, price_type, price_amount_micro, currency, ingested_at)
    VALUES ('mh-l0', 'mh-lk0', ?, 'mh', 'p', 2, 'sp_api_v1', 'v1.0.0', 'S-MH', 'x', 'x', ?, ?, 'MH-O', 'sku-mh', 'Order', 'Principal', 1000000, 'JPY', '2026-01-01 00:00:00')`).run(doc, `${YM}-05`, ym);
  const insH = db.prepare(`INSERT INTO raw_amazon_settlement_headers (physical_line_hash, business_line_key, source_document_id, source_file_hash, source_path, source_line_no, source_layer, parser_version,
    source_settlement_id, settlement_start_date, settlement_end_date, total_amount_micro, currency, ingested_at)
    VALUES (?, 'mh-hk', ?, 'mh', 'p', 1, 'sp_api_v1', 'v1.0.0', 'S-MH', ?, ?, 1000000, 'JPY', '2026-01-01 00:00:00')`);
  insH.run('mh-h0', doc, `${YM}-01T00:00:00+00:00`, `${YM}-15T00:00:00+00:00`);
  insH.run('mh-h1', doc, `${YM}-01T00:00:00+00:00`, `${YM}-15T00:00:00+00:00`);   // 同じ鍵・物理の行は別
  const logs = [];
  await runMigrate({ commit: false, log: (m) => logs.push(m) });
  ok(logs.some((l) => l.includes(`見出しの行が 2 行以上ある文書 1 (${doc} 2 行)`)), '版付けの dry-run で「見出しの行が 2 行以上ある文書」を数える (本番のコピーで 0 件を確かめる)');
  let threw = null; try { V.backfillDocumentVersions(db); } catch (e) { threw = e; }
  ok(threw && threw.code === 'MULTI_HEADER_DOCS' && !db.prepare(`SELECT 1 FROM amazon_settlement_document_versions WHERE source_document_id = ?`).get(doc), `🚨 過去の行の版付け: 見出しの物理の行が 2 行 (同じ鍵) = 版を付けずに止まる (${threw && threw.code})`);
  let mthrew = null; try { await runMigrate({ commit: true, allowUnresolved: true, log: () => {}, isAlive: () => false }); } catch (e) { mthrew = e; }
  ok(mthrew && mthrew.code === 'MULTI_HEADER_DOCS', `migrate --commit --allow-unresolved でも止まる (${mthrew && mthrew.code})`);
  db.prepare(`DELETE FROM raw_amazon_settlement_lines WHERE source_document_id = ?`).run(doc);   // 片付け
  db.prepare(`DELETE FROM raw_amazon_settlement_headers WHERE source_document_id = ?`).run(doc);
}

// ─── 🆕 #1567 R2 High 1: 過去の行で、見出しの無い新しい一部だけの文書が、見出しつきの全部の古い文書を押しのけない ───
{
  const Sh = 'S-H0';
  const insL = db.prepare(`INSERT INTO raw_amazon_settlement_lines (physical_line_hash, business_line_key, source_document_id, source_file_hash, source_path, source_line_no, source_layer, parser_version,
    source_settlement_id, posted_date_utc, posted_datetime_jst, economic_date, year_month_int, amazon_order_id, seller_sku_normalized, transaction_type, price_type, price_amount_micro, currency, ingested_at)
    VALUES (?, ?, ?, ?, 'p', ?, 'sp_api_v1', 'v1.0.0', ?, 'x', 'x', ?, ?, ?, 'sku-h0', 'Order', 'Principal', 1000000000, 'JPY', ?)`);
  const ym = Number(YM.replace('-', ''));
  for (const [i, o] of ['H-A', 'H-B', 'H-C'].entries()) insL.run(`h0-full-${i}`, `h0-k-${o}`, 'H0-FULL', 'hf', i + 2, Sh, `${YM}-03`, ym, o, '2026-03-01 00:00:00');
  db.prepare(`INSERT INTO raw_amazon_settlement_headers (physical_line_hash, business_line_key, source_document_id, source_file_hash, source_path, source_line_no, source_layer, parser_version,
    source_settlement_id, settlement_start_date, settlement_end_date, total_amount_micro, currency, ingested_at)
    VALUES ('h0-full-h', 'h0-hdr', 'H0-FULL', 'hf', 'p', 1, 'sp_api_v1', 'v1.0.0', ?, ?, ?, 3000000000, 'JPY', '2026-03-01 00:00:00')`).run(Sh, `${YM}-01T00:00:00+00:00`, `${YM}-15T00:00:00+00:00`);
  insL.run('h0-part-0', 'h0-k-H-A', 'H0-PART', 'hp', 1, Sh, `${YM}-03`, ym, 'H-A', '2026-03-02 00:00:00');   // 新しい・見出しなし・一部だけ
  V.backfillDocumentVersions(db);
  const sel = V.selectedVersionOf(db, Sh);
  const sql = db.prepare(`SELECT s.document_version_seq q, v.source_document_id d FROM v_amazon_settlement_selected_documents s JOIN amazon_settlement_document_versions v ON v.seq = s.document_version_seq WHERE s.settlement_id = ?`).get(Sh);
  ok(sel && sel.source_document_id === 'H0-FULL' && sql && sql.d === 'H0-FULL', `🚨 採る版 = 見出しつきの全部の古い文書 (JS ${sel && sel.source_document_id} / SQL ${sql && sql.d})`);
  const u = db.prepare(`SELECT COUNT(DISTINCT amazon_order_id) n, SUM(price_amount_micro) / 1000000 y FROM v_amazon_settlement_unified WHERE source_settlement_id = ?`).get(Sh);
  ok(u.n === 3 && u.y === 3000, `build が見る行 = 3 注文・3,000 円 (${u.n} 注文・${u.y} 円 = 1 注文・1,000 円に落ちない)`);
  const h0 = V.header0MultiVersionSettlements(db);
  ok(h0.some((x) => x.settlement_id === Sh && x.versions === 2 && x.header0 === 1), '「見出し 0 行の版があり、ほかの版もある決済」を数える (本番のコピーで 0 件を確かめる問い合わせ)');
}

// ─── 🆕 #1567 R1 Medium 3: 本番のコピーで V1 (採っている版) と V2 の明細を比べる道具 (check-settlement-v1-v2.js・読むだけ) ───
{
  const { compareV2WithSelected } = await import('./check-settlement-v1-v2.js');
  const S10 = 'S-DV-10';
  const a = prepareReportTsv(tsvOf(V1_COLUMNS, [{ ...HDR1, 'settlement-id': S10, 'total-amount': '1000.00' }, v1line({ 'settlement-id': S10, 'order-id': 'O10', 'merchant-order-id': 'O10', 'price-type': 'Principal', 'price-amount': '1000.00' }), v1line({ 'settlement-id': S10, 'order-id': 'O10', 'merchant-order-id': 'O10', 'quantity-purchased': '1' })]), 'R-10', 'x');
  ingestSettlement(db, a.headerRow, a.lineRows, a.ctx);
  const cnt = () => db.prepare(`SELECT (SELECT COUNT(*) FROM raw_amazon_settlement_lines) l, (SELECT COUNT(*) FROM amazon_settlement_document_versions) v`).get();
  const c0 = cnt();
  const same = compareV2WithSelected(db, tsvOf(V2_COLUMNS, [{ ...HDR2, 'settlement-id': S10, 'total-amount': '1000.00' }, v2line({ 'settlement-id': S10, 'order-id': 'O10', 'merchant-order-id': 'O10', amount: '1000.00' })]), 'R-10v2');
  ok(same.status === 'match' && same.v2.valid, `V2 と採っている V1 の明細が同じ = match (${same.status})`);
  const diff = compareV2WithSelected(db, tsvOf(V2_COLUMNS, [{ ...HDR2, 'settlement-id': S10, 'total-amount': '1100.00' }, v2line({ 'settlement-id': S10, 'order-id': 'O10', 'merchant-order-id': 'O10', amount: '1000.00' }), v2line({ 'settlement-id': S10, 'order-id': 'O11', 'merchant-order-id': 'O11', 'order-item-code': 'I9', amount: '100.00' })]), 'R-10v2b');
  ok(diff.status === 'differs', `V2 にだけある注文 = differs (${diff.status})`);
  ok(JSON.stringify(cnt()) === JSON.stringify(c0), '比べる道具は書かない (行も版も同じ)');
}

// ─── 🆕 #1567 R1 Medium 2: 表の用意は 2 回目以降は何もしない (毎回の initDB で ALTER・索引・trigger を作り直さない) ───
{
  const schema = () => db.prepare(`SELECT type, name, sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name`).all();
  const before = JSON.stringify(schema());
  const r0 = V.readSourceRevision(db);
  V.createSettlementVersionSchema(db); V.createSettlementVersionSchema(db);
  ok(JSON.stringify(schema()) === before && V.readSourceRevision(db) === r0, '表の用意を 2 回流しても表・列・索引・trigger・view と source_revision は同じ');
}

// ─── ⑥ R 以下だけ消す ───
{
  V.clearDirtyOrders(db, Object.keys(dirty()), rev());
  db.prepare(`UPDATE raw_amazon_settlement_lines SET quantity_purchased = 2 WHERE amazon_order_id = 'OA' AND document_version_seq = ? AND quantity_purchased = 1`).run(rb.documentVersionSeq);
  const R = rev();
  db.prepare(`UPDATE raw_amazon_settlement_lines SET quantity_purchased = 1 WHERE amazon_order_id = 'OA' AND document_version_seq = ? AND quantity_purchased = 2`).run(rb.documentVersionSeq);   // 読み取りの後に入った変更
  ok(V.clearDirtyOrders(db, ['OA'], R) === 0 && Object.hasOwn(dirty(), 'OA'), '🚨 読み取りの版 R より後の記録は消さない (R14 M1)');
  ok(V.clearDirtyOrders(db, ['OA'], rev()) === 1, '新しい版まで読んだ回なら消せる');
}

// ─── ⑧ lease ───
{
  let alive = true;
  const isAlive = () => alive;
  const g1 = V.acquireCoverageLease(db, { isAlive, pid: 111, now: new Date('2026-10-01T00:00:00Z') });
  ok(g1.ok && g1.lease.runToken, 'lease を取る');
  V.setLeaseGeneration(db, g1.lease, 5);
  const g2 = V.acquireCoverageLease(db, { isAlive, pid: 222 });
  ok(!g2.ok && g2.held.pid === 111 && g2.held.generation === 5, '🚨 持ち主が生きている = 奪わない (心拍の期限では奪わない)');
  alive = false;
  const g3 = V.acquireCoverageLease(db, { isAlive, pid: 333 });
  ok(g3.ok && g3.recovered && g3.recovered.pid === 111 && g3.lease.runToken !== g1.lease.runToken, '持ち主が死んだ = 新しい回が lease を取る (token が変わる)');
  V.setLeaseGeneration(db, g3.lease, 6);
  throws(() => V.assertLease(db, g1.lease), /lease/, '古い token は assertLease で止まる');
  // 古い token の子の取引は生の表に書けない (何も書かない)
  const S7 = 'S-DV-7';
  const p7 = prepareReportTsv(tsvOf(V1_COLUMNS, [{ ...HDR1, 'settlement-id': S7, 'total-amount': '10.00' }, v1line({ 'settlement-id': S7, 'order-id': 'O7', 'price-type': 'Principal', 'price-amount': '10.00' })]), 'R-7', 'x');
  const cnt = () => db.prepare(`SELECT (SELECT COUNT(*) FROM raw_amazon_settlement_lines) l, (SELECT COUNT(*) FROM amazon_settlement_document_versions) v, (SELECT revision FROM amazon_settlement_source_revision) r`).get();
  const c0 = cnt();
  throws(() => ingestSettlement(db, p7.headerRow, p7.lineRows, p7.ctx, { lease: g1.lease }), /lease/, '🚨 古い token の子の取込は生の表に書けない (LEASE_LOST)');
  ok(JSON.stringify(cnt()) === JSON.stringify(c0), '書けなかった取込は行・版・source_revision のどれも変えない');
  const r7 = ingestSettlement(db, p7.headerRow, p7.lineRows, p7.ctx, { lease: g3.lease });
  ok(r7.lineInserted === 1, '今の持ち主の token なら書ける');
  // 一覧の記録も同じ (古い token = 記録しない)
  const fakeSp = { callAPI: async (req) => (req.operation === 'getReports' ? { reports: [] } : {}) };
  const res = await runSettlementFetch({ reportId: null, dryRun: false, source: 'v2' }, { db, sp: fakeSp, inventorySp: fakeSp, runId: 'run-old', downloadTsv: async () => '', lease: g1.lease, coverage: { generation: 5, runToken: g1.lease.runToken, evidenceEpoch: null } });
  ok(res.inventory && res.inventory.id === null && /lease/.test(res.inventory.recordError || ''), `🚨 古い token の一覧は記録しない (${res.inventory && res.inventory.recordError})`);
  const res2 = await runSettlementFetch({ reportId: null, dryRun: false, source: 'v2' }, { db, sp: fakeSp, inventorySp: fakeSp, runId: 'run-new', downloadTsv: async () => '', lease: g3.lease, coverage: { generation: 6, runToken: g3.lease.runToken, evidenceEpoch: 2 } });
  const run = db.prepare(`SELECT * FROM amazon_settlement_report_inventory_runs WHERE id = ?`).get(res2.inventory.id);
  ok(run && run.coverage_generation === 6 && run.run_token === g3.lease.runToken && run.evidence_epoch === 2 && run.completed_at, '今の持ち主の一覧の回 = 世代・token・印の epoch を書く');
  ok(V.releaseCoverageLease(db, g1.lease) === false && V.releaseCoverageLease(db, g3.lease) === true, '放すのは自分の token だけ');
  const g4 = V.acquireCoverageLease(db, { isAlive: () => true, pid: 444 });
  ok(g4.ok, '放した lease は (持ち主が生きていても) 取れる');
  V.releaseCoverageLease(db, g4.lease);
}

// ─── ⑨ build と変換: 版が 2 つある決済で、新しい版から消えた相殺の行を数えない ───
{
  // S1 は採る版 = V1B (相殺の行なし)。旧い版 V1A には OB の +100 / −100 (Other) があった → 日次の財務の other_amount・分けられない部品に出てはいけない
  V.refreshStaleVersionDetails(db);
  execFileSync(process.execPath, ['scripts/amazon-finance/build-daily-fact.js', '--data-dir', tmpDir, '--month', YM], { cwd: repoRoot, env: { ...process.env, DATA_DIR: tmpDir }, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] });
  const f = db.prepare(`SELECT SUM(sales_principal_jpy) p, SUM(other_amount_jpy) o, COUNT(*) n FROM f_amazon_finance_sku_daily_v1 WHERE seller_sku = 'sku-a' AND date_jst = ?`).get(`${YM}-02`);
  // SKU-A (2 日) = S1 の採った版 (OA 1,000) + S-DV-7 (O7 10) + S-DV-BAD の良い旧い版 (OG 1,000・中身の悪い新しい版 999 は採らない) + S-DV-10 (O10 1,000)。
  //   S-DV-2 の OB の返品 (50) は返金 = 売上ではない・S-DV-9 は取り込んでいない
  ok(f && f.p === 1000 + 10 + 1000 + 1000 && f.o === 0, `🚨 build: 採った版だけ = 本体 3,010・other_amount 0 (旧い版の OB の 300・相殺の行・悪い版の 999 を数えない) (${f && [f.p, f.o]})`);
  const unified = db.prepare(`SELECT COUNT(*) n FROM v_amazon_settlement_unified WHERE source_settlement_id = ?`).get(S1).n;
  ok(unified === 2, `表示用の集まり (v_amazon_settlement_unified) も採った版の 2 行だけ (${unified})`);
}

console.log(failed ? `\n❌ ${failed} 件 失敗` : '\n=== 文書の版テスト ALL PASS ===');
process.exit(failed ? 1 : 0);
