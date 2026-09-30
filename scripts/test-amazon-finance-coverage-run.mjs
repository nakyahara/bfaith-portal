#!/usr/bin/env node
/**
 * test-amazon-finance-coverage-run.mjs — 決済のそろい (coverage) の miniPC 側 = coordinator (D7b-1b-3) の受入試験
 *
 * 設計 = AI_reference『CompanyDB構想/13_Amazon利益のmart_設計_20260930.md』v26 §3.1・D-65・D-66。
 *   Render = 本物の router (PR #1561 の受け口・0050) を PGlite で。SP-API = 作り物 (一覧・取込の一覧・文書)。一時 DATA_DIR。本番には触れない。
 *   場面:
 *     取込だけ (財務のバックフィル前) / 初期の印が無い = ⚠️ complete にしない / 印の決済が SQLite に無い = ⚠️ / 手のファイル (順番待ち) → complete /
 *     dry-run は何も変えない / Render の世代を追う (台帳が古い) / 読み直しと complete の隙間 (complete の直前に生の行を変える) /
 *     最後の chunk の送信中に生の行を変える / 同じ report ID で file hash が変わる = 採る版が変わる → 旧い版にだけある注文は墓石・別の決済に残る注文は残る /
 *     一覧の欠け (一覧にあるのに取り込めていない) / CANCELLED / 期間の分からない report / 一覧が最後のページまで取れない /
 *     lease: 生きている持ち主からは奪わない・死んだら奪う・回の途中で lease を置き換えられた = 古い token の子は書けない /
 *     complete の応答だけ失われた = 同じ中身の再送が same / 窓の空白 (保持期間以上あいた) = ⚠️ / Render に 0050 が無い = 今までの送り方 /
 *     coordinator を通らない単独の送り手 (token の無い chunk) = Render は complete を無効にする / 完成の判定の単体
 * 実行: node scripts/test-amazon-finance-coverage-run.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import Database from 'better-sqlite3';
import express from 'express';
import { PGlite } from '@electric-sql/pglite';
import { applyMigrations, pgliteAdapter } from './company-db/migrate.mjs';

const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'cov-run-test-'));
process.env.DATA_DIR = tmpDir;
process.env.MIRROR_SYNC_KEY = 'k';
process.env.COMPANY_DB_URL = 'postgres://pglite';
const { initDB, getDB } = await import('../apps/warehouse/db.js');
const { runCoverage, STATUS_PATH } = await import('../apps/warehouse/amazon-finance-coverage-run.js');
const { completionBlockers, frontierFrom, evaluateCoverage } = await import('../apps/warehouse/amazon-finance-coverage.js');
const { runMarkerCli, normalizeMarker, yenToMicro } = await import('../apps/warehouse/amazon-finance-initial-marker.js');
const { queueManualFile } = await import('../apps/warehouse/amazon-settlement-manual-file.js');
const V = await import('../apps/warehouse/amazon-settlement-versions.js');
const { V2_COLUMNS } = await import('../apps/warehouse/amazon-settlement-v2.js');
const { openLedger } = await import('../apps/company-db/push/ledger.mjs');
const { pushAmazonFinance, FINANCE_KIND, META } = await import('../apps/company-db/push/amazon-finance.mjs');
const { default: companyDbRouter, requireSyncKey, __setPgClientFactory } = await import('../apps/company-db/router.mjs');

let ok = 0, ng = 0;
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message || e)); } };
const logs = [];
const log = (m) => logs.push(String(m));

await initDB();
const db = getDB();

// ─── Render (PGlite + 本物の router) ───
const pg = new PGlite();
const applied = await applyMigrations(pgliteAdapter(pg), { log: () => {} });
assert.ok(applied.applied.includes('0050'), '0050 (PR #1561) が流れていない = PR #1561 の部品を取り込んでから流す');
const one = async (sql, p = []) => (await pg.query(sql, p)).rows[0];
__setPgClientFactory(async () => ({
  query: async (text, params) => {
    if (params && params.length) return pg.query(text, params);
    if (text.includes(';')) { await pg.exec(text); return { rows: [] }; }
    return pg.query(text);
  },
  end: async () => {},
}));
const app = express();
app.use('/apps/company-db/sync', requireSyncKey);
app.use('/apps/company-db/sync', companyDbRouter);
const server = await new Promise((resolve) => { const s = app.listen(0, '127.0.0.1', () => resolve(s)); });
const BASE = `http://127.0.0.1:${server.address().port}/apps/company-db/sync`;
const cov = async () => (await one(`select state, generation::text g, run_token, complete_to::text complete_to, source_revision::text rev, invalidated_reason from core.finance_coverage where mall = 'amazon' and scope_key = 'jp'`)) || null;
const covState = async () => (await one(`select complete_to::text complete_to, generation::text g from core.finance_coverage_state(1::smallint, 'amazon', 'jp', 'amazon_settlement_unified')`));
const receipt = async (no) => (await one(`select lines from core.order_finance_receipts where mall_order_no = $1`, [no])) || null;

// ─── 作り物の決済 (V2) ───
const tsvOf = (cols, rows) => [cols.join('\t'), ...rows.map((r) => cols.map((c) => r[c] ?? '').join('\t'))].join('\n') + '\n';
const v2t = (iso) => `${iso.slice(0, 10).replace(/-/g, '/')} ${iso.slice(11, 19)} UTC`;
/** 決済 1 つの V2 のファイル。lines = [{ kind: 'order' | 'refund' | 'storage', order, sku, yen, day (ISO の日時) }] */
function settlementTsv(sid, startIso, endIso, lines) {
  const total = lines.reduce((s, l) => s + l.yen * 100, 0) / 100;
  const hdr = { 'settlement-id': sid, 'settlement-start-date': v2t(startIso), 'settlement-end-date': v2t(endIso), 'deposit-date': v2t(endIso), 'total-amount': total.toFixed(2), currency: 'JPY' };
  const rows = [hdr, ...lines.map((l, i) => {
    const at = v2t(l.day);
    const base = { 'settlement-id': sid, 'marketplace-name': 'Amazon.co.jp', 'posted-date': at.slice(0, 10), 'posted-date-time': at };
    if (l.kind === 'storage') return { ...base, 'marketplace-name': '', 'transaction-type': 'other-transaction', 'amount-type': 'other-transaction', 'amount-description': 'Storage Fee', amount: l.yen.toFixed(2) };
    if (l.kind === 'refund') return { ...base, 'transaction-type': 'Refund', 'order-id': l.order, 'merchant-order-id': l.order, 'adjustment-id': `AJ-${sid}-${i}`, 'order-item-code': `OI-${l.order}`, sku: l.sku, 'amount-type': 'ItemPrice', 'amount-description': 'Principal', amount: l.yen.toFixed(2) };
    return { ...base, 'transaction-type': 'Order', 'order-id': l.order, 'merchant-order-id': l.order, 'shipment-id': `SH-${l.order}`, 'fulfillment-id': 'AFN', 'order-item-code': `OI-${l.order}`, sku: l.sku, 'quantity-purchased': '1', 'amount-type': 'ItemPrice', 'amount-description': 'Principal', amount: l.yen.toFixed(2) };
  })];
  return tsvOf(V2_COLUMNS, rows);
}
// 期間 (実時刻 UTC)。起点 = 2026-01-01 JST 00:00 = 2025-12-31T15:00:00Z
const P = {
  S0: ['2025-12-29T10:00:00Z', '2026-01-12T10:00:00Z'], S1: ['2026-01-12T10:00:00Z', '2026-01-26T10:00:00Z'], S2: ['2026-01-26T10:00:00Z', '2026-02-09T10:00:00Z'],
  S3: ['2026-02-09T10:00:00Z', '2026-02-23T10:00:00Z'], S4: ['2026-02-23T10:00:00Z', '2026-03-09T10:00:00Z'],
};
const L = {
  S0: [{ kind: 'order', order: 'O-0', sku: 'SKU-A', yen: 800, day: '2026-01-05T01:00:00Z' }],
  S1: [{ kind: 'order', order: 'O-1', sku: 'SKU-A', yen: 900, day: '2026-01-15T01:00:00Z' }],
  S2: [{ kind: 'order', order: 'O-A', sku: 'SKU-A', yen: 1000, day: '2026-01-28T01:00:00Z' }, { kind: 'order', order: 'O-B', sku: 'SKU-B', yen: 500, day: '2026-02-01T01:00:00Z' }],
  S3: [{ kind: 'refund', order: 'O-B', sku: 'SKU-B', yen: -500, day: '2026-02-12T01:00:00Z' }, { kind: 'order', order: 'O-C', sku: 'SKU-C', yen: 700, day: '2026-02-15T01:00:00Z' }],
  S4: [{ kind: 'storage', yen: -300, day: '2026-02-25T01:00:00Z' }, { kind: 'order', order: 'O-D', sku: 'SKU-D', yen: 200, day: '2026-03-01T01:00:00Z' }],
};
const DOCS = { D2: settlementTsv('S2', ...P.S2, L.S2), D3: settlementTsv('S3', ...P.S3, L.S3), D4: settlementTsv('S4', ...P.S4, L.S4) };
const V2T = 'GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2';
const rep = (id, status, doc, [s, e], created, over = {}) => ({ reportId: id, reportType: V2T, processingStatus: status, createdTime: created, dataStartTime: s, dataEndTime: e, ...(doc ? { reportDocumentId: doc } : {}), ...over });
const BASE_REPORTS = [rep('R2', 'DONE', 'D2', P.S2, '2026-02-10T00:00:00Z'), rep('R3', 'DONE', 'D3', P.S3, '2026-02-24T00:00:00Z'), rep('R4', 'DONE', 'D4', P.S4, '2026-03-10T00:00:00Z')];
const SP = { ing: BASE_REPORTS, inv: BASE_REPORTS, invEndless: false };
const sp = {
  async callAPI(req) {
    if (req.operation !== 'getReports') throw new Error(`想定外 ${req.operation}`);
    const q = req.query;
    if (q.nextToken) return SP.invEndless ? { reports: [], nextToken: 'more' } : { reports: [] };
    if (q.createdSince) return SP.invEndless ? { reports: SP.inv, nextToken: 'more' } : { reports: SP.inv };
    return { reports: SP.ing };
  },
};
const downloadTsv = async (docId) => { if (!Object.hasOwn(DOCS, docId)) throw new Error(`文書が無い ${docId}`); return DOCS[docId]; };

// ─── coordinator の呼び方 ───
let NOW = Date.parse('2026-03-20T00:00:00Z');
const BIG = { limitBytes: 1e15, rowBytes: 1000, replaceFactor: 2, walAllowanceBytes: 0, marginBytes: 0, orderBytes: 300 };
let aliveFn = () => false;
// 回ごとに時計を 1 分進める (採る版の「新しい順」= 版の ingested_at が回ごとに違う・本番と同じ)
const run = (x = {}) => { NOW += 60000; return runCoverage({ dataDir: tmpDir, fetchImpl: x.fetchImpl || fetch, base: BASE, syncKey: 'k', sp, inventorySp: sp, downloadTsv, now: () => new Date(NOW), isAlive: (...a) => aliveFn(...a),
  log, capacity: BIG, sleep: async () => {}, businessDate: '2026-03-20', pid: 4242, ...x }); };
const codes = (r) => [...new Set((r.reasons || []).map((x) => x.code))].sort();
const rawCounts = () => db.prepare(`SELECT (SELECT COUNT(*) FROM raw_amazon_settlement_lines) l, (SELECT COUNT(*) FROM amazon_settlement_document_versions) v, (SELECT revision FROM amazon_settlement_source_revision) r`).get();
const ledgerMeta = (k) => { const l = openLedger(tmpDir, { kind: FINANCE_KIND }); try { return l.getMeta(k); } finally { l.close(); } };
const setLedgerMeta = (k, v) => { const l = openLedger(tmpDir, { kind: FINANCE_KIND }); try { l.putMeta(k, v); } finally { l.close(); } };
/** Render の POST を数える・差し込む fetch */
function spyFetch({ onChunk = null, onCoverage = null, statusOverride = null } = {}) {
  const calls = { updating: 0, complete: 0, chunks: 0, tokenedChunks: 0 };
  const f = async (url, init = {}) => {
    const u = String(url);
    if (statusOverride && init.method !== 'POST' && u.includes('/order-finance/coverage/status')) return statusOverride();
    if (init.method === 'POST' && u.endsWith('/order-finance/coverage')) {
      const b = JSON.parse(init.body);
      calls[b.state]++;
      if (onCoverage) { const o = await onCoverage(b, () => fetch(url, init)); if (o) return o; }
    } else if (init.method === 'POST' && u.endsWith('/order-finance')) {
      const b = JSON.parse(init.body);
      calls.chunks++; if (b.coverage_generation && b.run_token) calls.tokenedChunks++;
      if (onChunk) await onChunk(b);
    }
    return fetch(url, init);
  };
  f.calls = calls;
  return f;
}

console.log('① 取込だけ (Company DB の財務のバックフィルの完了印の前)');
await t('完了印の前 = 取込だけ・Render に触れない・最後の行に「財務 push: ⏭️」(exit 0)', async () => {
  const f = spyFetch();
  const r = await run({ fetchImpl: f });
  assert.equal(r.exitCode, 0, r.summary); assert.equal(r.mode, 'ingest_only');
  assert.match(r.summary, /^✅ Amazon 決済と財務: .*財務 push: ⏭️/);
  assert.equal(f.calls.updating + f.calls.chunks, 0);
  assert.equal(await cov(), null);
  assert.equal(db.prepare(`SELECT COUNT(*) n FROM amazon_settlement_document_versions WHERE report_id IN ('R2','R3','R4')`).get().n, 3);
  const inv = db.prepare(`SELECT * FROM amazon_settlement_report_inventory_runs ORDER BY id DESC LIMIT 1`).get();
  assert.ok(inv.completed_at && inv.coverage_generation === null && inv.run_token && inv.evidence_epoch === null, '一覧の回 (世代なし・token = lease)');
  assert.equal(V.readLease(db).released_at != null, true, 'lease を放した');
});
setLedgerMeta(META.backfill, '1');

console.log('② coverage の回');
await t('🚨 初期の印が無い = complete にしない (⚠️ exit 0)・Render は updating (世代 1)・全部の chunk に世代と token', async () => {
  const f = spyFetch();
  const r = await run({ fetchImpl: f });
  assert.equal(r.mode, 'coverage'); assert.equal(r.generation, 1); assert.equal(r.exitCode, 0, r.summary);
  assert.ok(codes(r).includes('initial_marker_missing'), codes(r).join(','));
  assert.match(r.summary, /^⚠️ .*coverage: ⚠️ complete にしない/);
  assert.equal(f.calls.updating, 1); assert.equal(f.calls.complete, 0);
  assert.ok(f.calls.chunks > 0 && f.calls.tokenedChunks === f.calls.chunks, `chunk ${f.calls.chunks} / token 付き ${f.calls.tokenedChunks}`);
  const c = await cov();
  assert.deepEqual([c.state, c.g], ['updating', '1']);
  assert.equal((await covState()).complete_to, null);
  assert.equal(ledgerMeta('coverage_generation'), '1');
});
// 初期の印 (Seller Central の過去の決済情報 = S0・S1・S2)
const MARKER = { evidence_kind: 'seller_central_payments_export', verified_from: '2026-01-01', verified_through: '2026-02-09', captured_at: '2026-03-15T10:00:00+09:00',
  settlements: [
    { settlement_id: 'S0', start: '2025-12-29', end: '2026-01-12', total: '800', currency: 'JPY' },
    { settlement_id: 'S1', start: '2025-01-12'.replace('2025', '2026'), end: '2026-01-26', total: '900.00', currency: 'jpy' },
    { settlement_id: 'S2', start: '2026-01-26', end: '2026-02-09', total: '1,500', currency: 'JPY', report_id: 'R2' },
  ] };
const markerFile = path.join(tmpDir, 'marker.json');
fs.writeFileSync(markerFile, JSON.stringify(MARKER));
await t('初期の印: dry-run は書かない・SQLite に無い決済を示す / --commit で epoch 1 を書く (追記だけ = UPDATE・DELETE は拒む)', async () => {
  const d = runMarkerCli(db, { file: markerFile, commit: false }, { log: () => {}, now: new Date(NOW) });
  assert.equal(d.committed, false); assert.deepEqual(d.summary, { match: 1, missing: 2, differs: 0 });
  assert.equal(db.prepare(`SELECT COUNT(*) n FROM initial_marker_headers`).get().n, 0);
  const w = runMarkerCli(db, { file: markerFile, commit: true }, { log: () => {}, now: new Date(NOW) });
  assert.equal(w.epoch, 1); assert.match(w.markerId, /^im-1-20260315$/);
  const h = db.prepare(`SELECT * FROM initial_marker_headers`).get();
  assert.deepEqual([h.verified_from, h.verified_through, h.captured_at], ['2025-12-31T15:00:00Z', '2026-02-09T15:00:00Z', '2026-03-15T01:00:00Z']);
  assert.throws(() => db.prepare(`UPDATE initial_marker_headers SET note = 'x'`).run(), /追記だけ/);
  assert.throws(() => db.prepare(`DELETE FROM initial_marker_settlements`).run(), /追記だけ/);
  assert.equal(yenToMicro('1,500', 'x'), 1500000000n); assert.equal(yenToMicro('-0.5', 'x'), -500000n);
  assert.throws(() => normalizeMarker({ ...MARKER, verified_from: '2026-02-10' }), /verified_from/);
  assert.throws(() => normalizeMarker({ ...MARKER, settlements: [MARKER.settlements[0], MARKER.settlements[0]] }), /2 回/);
  assert.throws(() => normalizeMarker({ ...MARKER, captured_at: '2027-01-01T00:00:00Z' }, { now: new Date(NOW) }), /未来/);
});
await t('🚨 印の決済 S0・S1 が SQLite に無い = complete にしない (⚠️ marker_settlement_missing・起点を覆う見出しが無い)', async () => {
  const r = await run({ fetchImpl: spyFetch() });
  assert.equal(r.exitCode, 0, r.summary);
  assert.ok(codes(r).includes('marker_settlement_missing') && codes(r).includes('origin_not_covered'), codes(r).join(','));
  assert.deepEqual([(await cov()).state, (await cov()).g], ['updating', '2']);
});
await t('手のファイル (Seller Central の V2) を順番待ちに積む → coordinator の回で manual_csv の版として入る → 🚨 complete (complete_to = 最後の決済の end の JST の前日)', async () => {
  for (const s of ['S0', 'S1']) {
    const q = queueManualFile(db, tmpDir, settlementTsv(s, ...P[s], L[s]), { fileName: `${s}.txt`, now: new Date(NOW) });
    assert.equal(q.queued, true); assert.equal(q.ok, true);
  }
  assert.equal(db.prepare(`SELECT COUNT(*) n FROM raw_amazon_settlement_lines WHERE source_settlement_id IN ('S0','S1')`).get().n, 0, '積むだけでは生の表に書かない');
  const f = spyFetch();
  const r = await run({ fetchImpl: f });
  assert.equal(r.exitCode, 0, `${r.summary}\n${JSON.stringify(r.reasons)}`);
  assert.deepEqual(codes(r), []);
  assert.match(r.summary, /^✅ .*coverage: ✅ complete \(世代 3・complete_to 2026-03-08/);
  const c = await cov();
  assert.deepEqual([c.state, c.g, c.complete_to], ['complete', '3', '2026-03-08']);
  assert.equal((await covState()).complete_to, '2026-03-08');
  assert.equal(c.rev, String(V.readSourceRevision(db)));
  assert.deepEqual(r.manual.map((m) => m.status), ['ingested', 'ingested']);
  assert.equal(db.prepare(`SELECT COUNT(*) n FROM amazon_settlement_document_versions WHERE source_layer = 'manual_csv' AND report_id IS NULL AND report_document_id IS NULL`).get().n, 2);
  assert.equal(f.calls.updating, 1); assert.equal(f.calls.complete, 1);
  assert.equal(db.prepare(`SELECT COUNT(*) n FROM amazon_settlement_dirty_orders`).get().n, 0, '送れた注文の読み直す記録は消えた');
  assert.equal(V.readLease(db).coverage_generation, 3);
});

console.log('③ dry-run・世代');
await t('🚨 dry-run は coverage・台帳・lease・生の表に触れない (manifest の判定だけ)', async () => {
  const before = { c: await cov(), g: ledgerMeta('coverage_generation'), lease: V.readLease(db), raw: rawCounts() };
  const f = spyFetch();
  const r = await run({ fetchImpl: f, dryRun: true });
  assert.equal(r.exitCode, 0, r.summary);
  assert.equal(f.calls.updating + f.calls.complete + f.calls.chunks, 0);
  assert.deepEqual(await cov(), before.c); assert.equal(ledgerMeta('coverage_generation'), before.g);
  assert.deepEqual(V.readLease(db), before.lease); assert.deepEqual(rawCounts(), before.raw);
  assert.ok(codes(r).includes('inventory_this_run'), '今回の一覧の回は本番の回でだけ');
});
await t('Render の世代を追う (台帳の世代が古い = Render の復元の後) → 変わりが無くても新しい世代で complete', async () => {
  setLedgerMeta('coverage_generation', '1');
  const r = await run({ fetchImpl: spyFetch() });
  assert.equal(r.exitCode, 0, r.summary); assert.equal(r.generation, 4);
  assert.equal(r.push.changed, 0);
  assert.deepEqual([(await cov()).state, (await cov()).g], ['complete', '4']);
});

console.log('④ 読み取りの後の変化 (source_revision)');
await t('🚨 読み直しと complete の隙間: complete の直前に生の行を変える = complete にしない (❌ source_revision_changed)・Render は updating → 次の回で complete', async () => {
  const r = await run({ fetchImpl: spyFetch(), hooks: { beforeComplete: (dbx) => dbx.prepare(`UPDATE raw_amazon_settlement_lines SET marketplace_name = 'Amazon.co.jp ' WHERE amazon_order_id = 'O-A'`).run() } });
  assert.equal(r.exitCode, 1, r.summary); assert.ok(codes(r).includes('source_revision_changed'), codes(r).join(','));
  assert.deepEqual([(await cov()).state, (await cov()).g], ['updating', '5']);
  const r2 = await run({ fetchImpl: spyFetch() });
  assert.equal(r2.exitCode, 0, r2.summary);
  assert.deepEqual([(await cov()).state, (await cov()).g], ['complete', '6']);
});
await t('🚨 同じ report ID で file hash が変わる (O-C が消えた新しい文書) + 最後の chunk の送信中に生の行を変える = complete にしない → 次の回で complete・O-C は墓石・O-B は S2 に残る', async () => {
  DOCS.D3 = settlementTsv('S3', ...P.S3, [L.S3[0]]);   // O-C の行が無い
  let touched = false;
  const f = spyFetch({ onChunk: (b) => { if (b.last && !touched) { touched = true; db.prepare(`UPDATE raw_amazon_settlement_lines SET marketplace_name = 'Amazon.co.jp' WHERE amazon_order_id = 'O-A'`).run(); } } });
  const r = await run({ fetchImpl: f });
  assert.ok(touched, 'chunk の送信中に変えた');
  assert.equal(r.exitCode, 1, r.summary); assert.ok(codes(r).includes('source_revision_changed'), codes(r).join(','));
  assert.equal((await cov()).state, 'updating');
  const sel = V.selectedVersionOf(db, 'S3');
  assert.equal(sel.report_id, 'R3'); assert.equal(sel.line_count, 1, '採る版 = 新しい文書 (O-C が無い)');
  assert.equal((await receipt('O-C')).lines, 0, 'O-C は旧い版にだけある = 墓石');
  assert.ok((await receipt('O-B')).lines > 0, 'O-B は S2 に残る = 墓石にしない');
  const r2 = await run({ fetchImpl: spyFetch() });
  assert.equal(r2.exitCode, 0, `${r2.summary} ${JSON.stringify(r2.reasons)}`);
  assert.equal((await cov()).state, 'complete');
});

console.log('⑤ 一覧・report の状態');
// 🚨 一覧に 1 度でも出た report は積み上げた期待の集合に残る (設計 R19 H1 = 保持期間で一覧から消えた report の欠けを見逃さない)。
//   = 未充足の report が出たら、Seller Central で印を作り直す (新しい epoch = 積み上げは新しい印の後の回だけ) のが runbook。試験も同じ手で戻す
const remark = () => runMarkerCli(db, { file: markerFile, commit: true }, { log: () => {}, now: new Date(NOW) });
const restore = () => { SP.ing = BASE_REPORTS; SP.inv = BASE_REPORTS; SP.invEndless = false; remark(); };
await t('🚨 一覧の欠け (一覧にある DONE の report を取り込めていない) = complete にしない (❌ report_not_imported)', async () => {
  SP.inv = [...BASE_REPORTS, rep('R-MISS', 'DONE', 'D-MISS', P.S3, '2026-03-11T00:00:00Z')];
  try {
    const r = await run({ fetchImpl: spyFetch() });
    assert.equal(r.exitCode, 1, r.summary); assert.ok(codes(r).includes('report_not_imported'), codes(r).join(','));
    assert.equal((await cov()).state, 'updating');
  } finally { restore(); }
});
await t('🚨 CANCELLED = 未充足 (⚠️ report_cancelled・satisfied_empty は作らない)', async () => {
  SP.inv = [...BASE_REPORTS, rep('R-CXL', 'CANCELLED', null, P.S4, '2026-03-11T00:00:00Z')];
  try {
    const r = await run({ fetchImpl: spyFetch() });
    assert.equal(r.exitCode, 0, r.summary); assert.ok(codes(r).includes('report_cancelled'), codes(r).join(','));
    assert.equal((await cov()).state, 'updating');
  } finally { restore(); }
});
await t('並べ直しの規則に無い V2 で、その決済にほかの版がある = 終了コード 3 にしない・coverage は満たせない (⚠️ report_blocked = 規則を足す)', async () => {
  const badRow = tsvOf(V2_COLUMNS, [{ 'settlement-id': 'S3', 'transaction-type': 'NewThing', 'marketplace-name': 'Amazon.co.jp', 'posted-date': '2026/02/13', 'posted-date-time': '2026/02/13 01:00:00 UTC', 'amount-type': 'Mystery', 'amount-description': 'x', amount: '-7.00' }]).split('\n')[1];
  DOCS['D3-BAD'] = DOCS.D3 + badRow + '\n';
  SP.ing = [...BASE_REPORTS, rep('R3-BAD', 'DONE', 'D3-BAD', P.S3, '2026-03-11T00:00:00Z')]; SP.inv = SP.ing;
  try {
    const r = await run({ fetchImpl: spyFetch() });
    assert.notEqual(r.exitCode, 3, r.summary); assert.equal(r.exitCode, 0, r.summary);
    assert.ok(codes(r).includes('report_blocked'), codes(r).join(','));
    assert.match(r.summary, /取り込めない V2 1 本 \(ほかの版あり\)/);
    assert.equal((await cov()).state, 'updating');
  } finally { restore(); }
});
await t('🚨 期間の分からない report を対象の外にしない: IN_QUEUE で期間 null / DONE で期間 null / 期間が逆 = ⚠️ report_period_unknown', async () => {
  for (const bad of [rep('R-Q', 'IN_QUEUE', null, [undefined, undefined], '2026-03-19T00:00:00Z'), rep('R-N', 'DONE', 'D4', [null, null], '2026-03-19T00:00:00Z'), rep('R-X', 'DONE', 'D4', [P.S4[1], P.S4[0]], '2026-03-19T00:00:00Z')]) {
    SP.inv = [...BASE_REPORTS, bad];
    try {
      const r = await run({ fetchImpl: spyFetch() });
      assert.ok(codes(r).includes('report_period_unknown'), `${bad.reportId}: ${codes(r).join(',')}`);
      assert.equal((await cov()).state, 'updating');
    } finally { restore(); }
  }
});
await t('🚨 一覧が最後のページまで取れない (nextToken が残ったまま上限) = 回の失敗 = complete にしない (❌ inventory_this_run)', async () => {
  SP.invEndless = true;
  try {
    const r = await run({ fetchImpl: spyFetch() });
    assert.equal(r.exitCode, 1, r.summary); assert.ok(codes(r).includes('inventory_this_run'), codes(r).join(','));
    const inv = db.prepare(`SELECT last_page_reached p FROM amazon_settlement_report_inventory_runs ORDER BY id DESC LIMIT 1`).get();
    assert.equal(inv.p, 0);
  } finally { restore(); }
  const r2 = await run({ fetchImpl: spyFetch() });
  assert.equal(r2.exitCode, 0, `${r2.summary} ${JSON.stringify(r2.reasons)}`); assert.equal((await cov()).state, 'complete');
});
await t('🚨 未充足の report は印を作り直すまで期待の集合に残る (一覧から消えても) → 新しい印 (新しい epoch) の後の回だけを積み上げる', async () => {
  SP.inv = [...BASE_REPORTS, rep('R-CXL2', 'CANCELLED', null, P.S4, '2026-03-11T00:00:00Z')];
  const r1 = await run({ fetchImpl: spyFetch() });
  assert.ok(codes(r1).includes('report_cancelled'));
  SP.inv = BASE_REPORTS;   // 次の一覧には居ない
  const r2 = await run({ fetchImpl: spyFetch() });
  assert.ok(codes(r2).includes('report_cancelled'), '前の回の一覧で見た CANCELLED は残る (黙って消さない)');
  const e0 = db.prepare(`SELECT MAX(evidence_epoch) e FROM initial_marker_headers`).get().e;
  remark();
  assert.equal(db.prepare(`SELECT MAX(evidence_epoch) e FROM initial_marker_headers`).get().e, e0 + 1);
  const r3 = await run({ fetchImpl: spyFetch() });
  assert.equal(r3.exitCode, 0, `${r3.summary} ${JSON.stringify(r3.reasons)}`); assert.equal((await cov()).state, 'complete');
  assert.equal(db.prepare(`SELECT evidence_epoch e FROM amazon_settlement_report_inventory_runs ORDER BY id DESC LIMIT 1`).get().e, e0 + 1, '一覧の回は最新の印の epoch を持つ');
});

await t('🚨 同じ決済に別の report の文書 (中身が同じ) が後から来た = 旧い report は採った版で満たす (satisfied_by_selected_settlement・detail_digest の完全な一致) / 中身が違う = 満たさない (⚠️)', async () => {
  DOCS.D4b = DOCS.D4 + '\n';   // 行の集合は同じ・file hash だけ違う
  SP.ing = [...BASE_REPORTS, rep('R4b', 'DONE', 'D4b', P.S4, '2026-03-12T00:00:00Z')]; SP.inv = SP.ing;
  const r = await run({ fetchImpl: spyFetch() });
  assert.equal(r.exitCode, 0, `${r.summary} ${JSON.stringify(r.reasons)}`); assert.equal((await cov()).state, 'complete');
  assert.equal(V.selectedVersionOf(db, 'S4').report_id, 'R4b', '採る版 = 新しい文書');
  assert.ok(r.push.scanSnapshot.diag.results.satisfied_by_selected_settlement >= 1, JSON.stringify(r.push.scanSnapshot.diag.results));
  // 中身が違う文書 (O-D が 250 円) がさらに後から = 採る版は R4c・R4 と R4b は満たさない
  DOCS.D4c = settlementTsv('S4', ...P.S4, [L.S4[0], { ...L.S4[1], yen: 250 }]);
  SP.ing = [...SP.ing, rep('R4c', 'DONE', 'D4c', P.S4, '2026-03-13T00:00:00Z')]; SP.inv = SP.ing;
  const r2 = await run({ fetchImpl: spyFetch() });
  assert.ok(codes(r2).includes('report_selected_differs'), codes(r2).join(','));
  assert.equal((await cov()).state, 'updating');
  assert.equal((await receipt('O-D')).lines, 1);
  // 戻す = 同じ中身の文書 (R4d) をさらに後から + 印を作り直す (積み上げた R4c を消すのは人の判断)
  DOCS.D4d = DOCS.D4 + '\n\n';
  SP.ing = [...BASE_REPORTS, rep('R4d', 'DONE', 'D4d', P.S4, '2026-03-14T00:00:00Z')]; SP.inv = SP.ing; remark();
  const r3 = await run({ fetchImpl: spyFetch() });
  assert.equal(r3.exitCode, 0, `${r3.summary} ${JSON.stringify(r3.reasons)}`); assert.equal((await cov()).state, 'complete', `${r3.summary} ${JSON.stringify(r3.reasons)}`);
  restore();
});

console.log('⑥ lease');
await t('🚨 lease を生きている持ち主が持つ = 見送る (❌・Render に触れない) / 持ち主が死んだ = 奪って回す', async () => {
  aliveFn = () => true;
  const held = V.acquireCoverageLease(db, { isAlive: () => false, pid: 999, now: new Date(NOW) });
  assert.ok(held.ok);
  const g0 = (await cov()).g;
  const f = spyFetch();
  const r = await run({ fetchImpl: f });
  assert.equal(r.exitCode, 1); assert.match(r.summary, /lease を持っている \(pid 999/);
  assert.equal(f.calls.updating, 0); assert.equal((await cov()).g, g0);
  aliveFn = () => false;
  logs.length = 0;
  const r2 = await run({ fetchImpl: spyFetch() });
  assert.equal(r2.exitCode, 0, r2.summary);
  assert.ok(logs.some((l) => /pid 999・.*死んでいた = lease を取った/.test(l)), logs.filter((l) => /lease/.test(l)).join(' | '));
});
await t('🚨 回の途中で lease を置き換えられた (別の回が取った) = 古い token の子の取込は書けない (❌)・生の表は変わらない・Render は updating のまま', async () => {
  DOCS.D5 = settlementTsv('S5', '2026-03-09T10:00:00Z', '2026-03-16T10:00:00Z', [{ kind: 'order', order: 'O-E', sku: 'SKU-E', yen: 50, day: '2026-03-10T01:00:00Z' }]);
  SP.ing = [...BASE_REPORTS, rep('R5', 'DONE', 'D5', ['2026-03-09T10:00:00Z', '2026-03-16T10:00:00Z'], '2026-03-17T00:00:00Z')];
  SP.inv = SP.ing;
  try {
    const before = db.prepare(`SELECT COUNT(*) n FROM raw_amazon_settlement_lines WHERE source_settlement_id = 'S5'`).get().n;
    const r = await run({ fetchImpl: spyFetch(), hooks: { afterUpdating: (dbx) => { V.releaseCoverageLease(dbx, V.readLease(dbx).run_token ? { runToken: V.readLease(dbx).run_token } : null); V.acquireCoverageLease(dbx, { isAlive: () => false, pid: 777 }); } } });
    assert.equal(r.exitCode, 1, r.summary); assert.match(r.summary, /lease/);
    assert.equal(db.prepare(`SELECT COUNT(*) n FROM raw_amazon_settlement_lines WHERE source_settlement_id = 'S5'`).get().n, before, '古い token の子は書けない');
    assert.equal((await cov()).state, 'updating');
    // 置き換えた回 (pid 777) が死んだ後の次の回は取れる = S5 が入って complete
    const r2 = await run({ fetchImpl: spyFetch() });
    assert.equal(r2.exitCode, 0, `${r2.summary} ${JSON.stringify(r2.reasons)}`);
    assert.equal((await cov()).state, 'complete'); assert.equal((await cov()).complete_to, '2026-03-15');
  } finally { /* R5 は残す (以降の回も同じ一覧) */ }
});

console.log('⑦ complete の応答・窓の空白・0050 の前・単独の送り手');
await t('🚨 complete の応答だけ失われた = 同じ中身で送り直して same (complete のまま)', async () => {
  let lost = false;
  const f = spyFetch({ onCoverage: async (b, real) => { if (b.state === 'complete' && !lost) { lost = true; await real(); throw new Error('socket hang up (作り物)'); } return null; } });
  const r = await run({ fetchImpl: f });
  assert.equal(r.exitCode, 0, `${r.summary} ${JSON.stringify(r.reasons)}`);
  assert.equal(f.calls.complete, 2); assert.equal(r.coverage.status, 'same');
  assert.equal((await cov()).state, 'complete');
});
await t('Render に 0050 が無い (status が 409 not_migrated) = 今までの送り方 (token なし・coverage を送らない・⚠️)', async () => {
  const f = spyFetch({ statusOverride: () => new Response(JSON.stringify({ error: 'not_migrated' }), { status: 409, headers: { 'content-type': 'application/json' } }) });
  const r = await run({ fetchImpl: f });
  assert.equal(r.mode, 'legacy'); assert.equal(r.exitCode, 0, r.summary); assert.match(r.summary, /^⚠️ .*coverage: Render に 0050 が無い/);
  assert.equal(f.calls.updating + f.calls.complete + f.calls.tokenedChunks, 0);
});
await t('🚨 coordinator を通らない単独の送り手 (token の無い chunk が受領記録を変える) = Render は complete を無効にする', async () => {
  const r0 = await run({ fetchImpl: spyFetch() });
  assert.equal(r0.exitCode, 0, r0.summary); assert.equal((await cov()).state, 'complete');
  // 台帳の指紋を 1 つ消して (変わった注文として) 単独で送る = token なし
  const L1 = openLedger(tmpDir, { kind: FINANCE_KIND });
  const w = new Database(path.join(tmpDir, 'warehouse.db'), { readonly: true });
  try {
    await pg.query(`update core.order_finance_receipts set set_checksum = 'x' where mall_order_no = 'O-D'`);   // Render の受領記録をずらす (送り直すと applied = 受領記録が変わる)
    L1.db.prepare(`update sent set fp = 'x' where kind = ? and key like '%O-D'`).run(FINANCE_KIND);
    const r = await pushAmazonFinance({ warehouse: w, ledger: L1, base: BASE, syncKey: 'k', mode: 'range', from: '2026-03-01', to: '2026-03-01', log: () => {}, sleep: async () => {}, capacity: BIG });
    assert.ok(r.applied >= 1, `applied ${r.applied}`);
  } finally { w.close(); L1.close(); }
  const c = await cov();
  assert.deepEqual([c.state, c.invalidated_reason], ['updating', 'untokened_finance_write']);
  assert.equal((await covState()).complete_to, null);
  const r2 = await run({ fetchImpl: spyFetch() });
  assert.equal(r2.exitCode, 0, r2.summary); assert.equal((await cov()).state, 'complete', '次の coordinator の回 (新しい世代) で complete に戻る');
});
await t('🚨 一覧の窓の空白 (前の成功した回から 85 日以上あいた) = complete にしない (⚠️ evidence_chain_gap = Seller Central で印を作り直す)', async () => {
  NOW = Date.parse('2026-07-01T00:00:00Z');
  const r = await run({ fetchImpl: spyFetch() });
  assert.equal(r.exitCode, 0, r.summary); assert.ok(codes(r).includes('evidence_chain_gap'), codes(r).join(','));
  assert.equal((await cov()).state, 'updating');
  NOW = Date.parse('2026-03-20T00:00:00Z');
});

console.log('⑧ 判定の部品 (単体)');
await t('frontierFrom: 起点を含む区間から・重なりは可・隙間で止まる・起点を覆わなければ null', async () => {
  const o = '2026-01-01T00:00:00Z';
  assert.equal(frontierFrom([{ start: '2025-12-30T00:00:00Z', end: '2026-01-10T00:00:00Z' }, { start: '2026-01-09T00:00:00Z', end: '2026-01-20T00:00:00Z' }], o), '2026-01-20T00:00:00Z');
  assert.equal(frontierFrom([{ start: '2025-12-30T00:00:00Z', end: '2026-01-10T00:00:00Z' }, { start: '2026-01-11T00:00:00Z', end: '2026-01-20T00:00:00Z' }], o), '2026-01-10T00:00:00Z');
  assert.equal(frontierFrom([{ start: '2026-01-02T00:00:00Z', end: '2026-01-10T00:00:00Z' }], o), null);
});
await t('completionBlockers: range / 送れない注文 / 整形できない / stale / 読み直す鍵 / 鍵の分からない行 / 送れなかった疑似注文 / 読み直す注文 / 版の変化 / 判定の理由 = どれも complete にしない', async () => {
  const r = { mode: 'full', dryRun: false, failed: [], transformErrors: [], stale: 0 };
  const snap = { reasons: [], sourceRevision: 7 };
  assert.deepEqual(completionBlockers({ r, snapshot: snap, retryLeft: 0, dirtyLeft: 0, sourceRevisionNow: 7, unkeyed: 0, pseudoBlocked: 0 }), []);
  const one1 = (x) => completionBlockers({ r, snapshot: snap, retryLeft: 0, dirtyLeft: 0, sourceRevisionNow: 7, unkeyed: 0, pseudoBlocked: 0, ...x }).map((y) => y.code);
  assert.deepEqual(one1({ r: { ...r, mode: 'range' } }), ['range']);
  assert.deepEqual(one1({ r: { ...r, failed: [{}] } }), ['send_failed']);
  assert.deepEqual(one1({ r: { ...r, transformErrors: [{}] } }), ['transform_errors']);
  assert.deepEqual(one1({ r: { ...r, stale: 1 } }), ['stale']);
  assert.deepEqual(one1({ retryLeft: 2 }), ['retry_keys']);
  assert.deepEqual(one1({ unkeyed: 1 }), ['unkeyed']);
  assert.deepEqual(one1({ pseudoBlocked: 1 }), ['pseudo_blocked']);
  assert.deepEqual(one1({ dirtyLeft: 1 }), ['dirty_left']);
  assert.deepEqual(one1({ sourceRevisionNow: 8 }), ['source_revision_changed']);
  assert.deepEqual(one1({ snapshot: { reasons: [{ code: 'x', human: true }], sourceRevision: 7 } }), ['x']);
  assert.deepEqual(one1({ snapshot: null }), ['no_snapshot']);
});

console.log(ng ? `\n❌ coordinator (D7b-1b-3): ok ${ok} / NG ${ng}` : `\n✅ coordinator (D7b-1b-3): ok ${ok} / NG 0`);
server.close();
await pg.close();
process.exit(ng ? 1 : 0);
