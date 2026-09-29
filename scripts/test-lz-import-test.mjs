/**
 * test-lz-import-test.mjs — 少数件の実機の試験のランナー (scripts/logizard-import/lz-import-test.mjs・portal-io.mjs。マスタ正本切替 ③c-1b-2b-1c)
 *
 * 固定する契約 (設計 = AI_reference CompanyDB構想/10 §6.3「③c-1b-2b 契約 v3」):
 *   1 計画: その日の lz-daily の正式な証跡と直前の書き出しから・承認の印 (sha256)・00:00〜01:30 と L-16 (占有の確かめ) の無い回は断る
 *   2 取込: 承認の印・試験の CSV・計画の照らし直し (K2)・バーコードの部品 (K4) が合わない = 鍵の前 / 押す前に止める
 *   3 押す前にそろえる記録 (D)・importing が書けない = 押さない・押す前の失敗 = failed_before_execute・押した後の失敗 = unknown (K7)
 *   4 ポータルの書き込みの 3 つの結末 (K5): 応答が分からない = 状態で照らす・照らせない = 押さない / 結果を書けない = 手元に残して知らせる
 *   5 直後の書き出し (商品・バーコード) と確かめ → verified / verify_failed・partial は差を残して partial のまま・書き出しの失敗 = 未確かめのまま (H)
 *   6 知らせ: 止まった状態を GChat・状態と出来事の番号で知らせ済み (K9)・送り直し
 *   7 共通の仕組み (lz-import-engine.mjs・③c-1b-3b-1): 閉じた決まり (POLICIES) の外の決まり = 何も触らずに断る・試験の決まりの中身・ランナーは同じ部品を使う
 * 使い方: node scripts/test-lz-import-test.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';

process.env.DAILY_SYNC_RUN_ID = 'ds_test';
const { default: iconv } = await import('iconv-lite');
const T = await import('./logizard-import/lz-import-test.mjs');
const E = await import('./logizard-import/lz-import-engine.mjs');
const V = await import('../apps/master-decisions/lz-import-verify.mjs');
const TP = await import('../apps/master-decisions/lz-import-test-plan.mjs');
const IO = await import('./logizard-import/portal-io.mjs');
const S = await import('../apps/logizard-import-state/store.js');
const G = await import('../tools/logizard-automation/import-guard.js');
const { LZ_SHOHIN } = await import('../apps/master-decisions/lz-cdb.mjs');
const { writeEvidence } = await import('../apps/company-db/push/evidence.mjs');
const SE = await import('../tools/logizard-automation/shohin-export.js');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const sj = (s) => iconv.encode(s, 'cp932');
const q = (c) => `"${String(c).replace(/"/g, '""')}"`;
const sha = (b) => crypto.createHash('sha256').update(b).digest('hex');
const H = LZ_SHOHIN.header;
const col = (n) => H.indexOf(n);

// 今 = 2030-01-16 12:00 JST (昼)。lz-daily の正式な証跡 = 2030-01-16 の朝 (期限 = 翌日 01:00)
const NOW = new Date('2030-01-16T03:00:00Z');
const AS_OF = '2030-01-16', RUN_DIR = 'lzd_20300116T070000000Z_abcdef';
const DAILY = [['A-1', '新しい名前', '新しい名前', '1200', '0007'], ['B-2', 'B', 'B', '0', '0002']];
function lzCells(id, over = {}) {
  const c = H.map((h) => `${h}-${id}`);
  Object.assign(c, { [col('商品ID')]: id, [col('削除フラグ')]: '0', [col('登録日時')]: '2030/01/01 00:00', [col('変更日時')]: '2030/01/01 00:00', [col('インポート日時')]: '' });
  for (const [k, v] of Object.entries(over)) c[col(k)] = v;
  return c;
}
const csvBuf = (header, rows) => sj([header.map(q).join(','), ...rows.map((r) => r.map(q).join(','))].join('\r\n'));
const dailyBuf = () => csvBuf(['形式/型番', '商品名', 'ふりがな', '仕入単価', '取引先id'], DAILY);

function setupData() {
  const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'lzt-'));
  const rel = `lz-daily/${AS_OF}/${RUN_DIR}/cdb_logizard_shohinmaster_upload.csv`;
  fs.mkdirSync(path.join(dataDir, path.dirname(rel)), { recursive: true });
  const buf = dailyBuf();
  fs.writeFileSync(path.join(dataDir, rel), buf);
  writeEvidence(dataDir, 'lz-daily', { state: 'complete', as_of: AS_OF, run_id: RUN_DIR, verdict: 'pass', deadline: '2030-01-17T01:00:00+09:00', csv: { path: rel, sha256: sha(buf), rows: DAILY.length } },
    { now: new Date('2030-01-15T22:00:00Z'), warn: () => {} });
  return dataDir;
}

/** 偽物のロジザード (商品マスタとバーコード)。executeImport は CSV の値を入れる */
function fakeLz({ over = {} } = {}) {
  const st = { lz: new Map([['A-1', lzCells('A-1')], ['B-2', lzCells('B-2')], ['C-3', lzCells('C-3')]]), bc: [['A-1', 'a', '4900000000001'], ['B-2', 'b', '4900000000002'], ['C-3', 'c', '4900000000003']], calls: [] };   // 本物と同じく全商品にバーコード (9/29)
  const exportShohin = async () => {
    st.calls.push('exportShohin');
    if (over.postExportFails && st.calls.includes('execute')) throw new Error('書き出しに失敗');
    if (over.shohinTransientAlways) throw new Error('通信が切れた');
    // 中身が壊れている = 本物の書き出しと同じ検証と例外 (validateShohinCsv → invalidCsvError)
    if (over.corruptAlways || (over.postExportCorrupt && st.calls.includes('execute'))) { const v = SE.validateShohinCsv(Buffer.from([0x82, 0xff, 0x0a]), { minRows: 1 }); throw SE.invalidCsvError(v.reason); }
    return { buf: csvBuf(H, [...st.lz.values()]) };
  };
  const ops = {
    exportShohin,
    exportBarcodes: async () => { st.calls.push('exportBarcodes'); if (over.barcodeCorruptAlways) throw SE.invalidCsvError('3 行目の列数が違います (ヘッダ 3 / この行 2)'); return { buf: csvBuf(['商品ID', '商品名', 'バーコード'], st.bc) }; },
    previewImport: async (p) => { st.calls.push('preview'); if (over.previewThrows) throw new Error('プレビューに失敗'); st.previewed = p; if (over.previewDelayMs) await new Promise((r) => setTimeout(r, over.previewDelayMs)); return { previewed: true }; },
    executeImport: async ({ guard, onExecuteIssued }) => {
      if (over.throwBeforeIssue) { const e = new Error('押す前に失敗'); e.executeIssued = false; throw e; }
      guard.check('実行ボタン');
      onExecuteIssued();
      st.calls.push('execute');
      if (over.throwAfterIssue) { const e = new Error('押した後に失敗'); e.executeIssued = true; throw e; }
      const csv = iconv.decode(fs.readFileSync(st.previewed), 'cp932').split('\r\n').slice(1).map((l) => l.split(',').map((x) => x.replace(/^"|"$/g, '')));
      let processed = 0;
      for (const r of csv) {
        if (over.errorRow === r[0]) continue;
        const c = st.lz.get(r[0]);
        c[col('商品名')] = r[1]; c[col('検索名称')] = r[2]; c[col('仕入単価')] = r[3]; c[col('商品予備項目００３')] = r[4]; c[col('インポート日時')] = '2030/01/16 12:01';
        processed++;
      }
      if (over.touchOther) st.lz.get('C-3')[col('商品名')] = '書き換わった';
      if (over.touchBarcode) st.bc[0][2] = '4900000000999';
      if (over.postBarcodeCut) st.bc = st.bc.filter((r) => r[0] !== 'C-3');   // 後のバーコードの書き出しが途中で切れた (末尾の商品が無い)
      const errors = csv.length - processed;
      return { executeIssued: true, confirm: 'clicked', reason: null, resultText: `インポート結果 総件数 : ${csv.length} 処理件数 : ${processed} 処理不要件数 : 0 エラー件数 : ${errors}` };
    },
  };
  return { st, withSession: async (fn) => fn(ops) };
}

/** ポータル = 本物の状態の機械 (メモリの SQLite)。faults で応答を失わせる */
function portal({ faults = {} } = {}) {
  const db = S.openImportStateDb(':memory:');
  const { init_id } = S.init(db, { by: 'x', now: Date.now() });
  const net = () => Object.assign(new Error('fetch failed'), { code: 'unreachable', status: null });
  const wrap = (name, f) => async (b) => {
    const mode = typeof faults[name] === 'function' ? faults[name](b) : faults[name];
    if (mode === 'lost_before') throw net();          // 届かなかった (更新していない)
    const r = f(b);
    if (mode === 'lost_after') throw net();           // 更新したが応答を失った
    return { ok: true, ...r };
  };
  const client = {
    status: async (n = 20) => ({ ok: true, ...S.getStatus(db, { events: n }) }),
    acquire: wrap('acquire', (b) => S.acquire(db, { initId: b.init_id, holder: b.holder, purpose: b.purpose, runId: b.run_id, ttlSec: b.ttl_sec, by: b.by })),
    extend: wrap('extend', (b) => S.extend(db, { lockToken: b.lock_token, ttlSec: b.ttl_sec })),
    release: wrap('release', (b) => S.release(db, { lockToken: b.lock_token, by: b.by })),
    transition: wrap('transition', (b) => S.transition(db, { lockToken: b.lock_token, runId: b.run_id, to: b.to, detail: b.detail, by: b.by })),
    notified: wrap('notified', (b) => S.markNotified(db, { runId: b.run_id, state: b.state, stateEventId: b.state_event_id, by: b.by })),
  };
  const checkInit = async (c) => { const s = await c.status(5); return { ok: true, reason: null, status: s }; };
  return { db, init_id, client, checkInit };
}

async function planned(dataDir, lz, tests = { normal: ['A-1', 'B-2'] }) {
  const p = await T.planTest({ lzMinRows: 1, dataDir, now: NOW, tests, occupancy: '倉庫は使っていない (中原さん確認)', withSession: lz.withSession, log: () => {} });
  return p;
}
const runOpts = (dataDir, p, pt, extra = {}) => ({ lzMinRows: 1, dataDir, planId: p.planId, sha256: p.planSha256, occupancy: '倉庫は使っていない (中原さん確認)', now: NOW, localInitFile: 'x', client: pt.client, checkInit: pt.checkInit,
  capabilities: { exportBarcodes: true }, createGuard: G.createGuard, log: () => {}, heartbeatMs: 60000, ...extra });
const stagesOf = (r) => r.record.stages.map((x) => x.name);

console.log('test-lz-import-test');

await ta('[1] 計画: その日の lz-daily の正式な証跡と直前の書き出しから・承認の印・一覧 / 00:00〜01:30・占有の確かめなし・証跡なし = 作らない', async () => {
  const dataDir = setupData();
  const lz = fakeLz();
  const p = await planned(dataDir, lz);
  const plan = JSON.parse(fs.readFileSync(path.join(p.dir, 'plan.json'), 'utf8'));
  assert.deepEqual([plan.source.run_id, plan.test_csv.rows, plan.rows.map((r) => r.id), fs.existsSync(path.join(p.dir, 'test.csv')), fs.existsSync(path.join(p.dir, 'plan-pre.csv'))], [RUN_DIR, 2, ['A-1', 'B-2'], true, true]);
  assert.match(fs.readFileSync(path.join(p.dir, 'summary.txt'), 'utf8'), new RegExp(`承認の印 \\(sha256\\) = ${p.planSha256}`));
  await assert.rejects(T.planTest({ lzMinRows: 1, dataDir, now: new Date('2030-01-15T15:30:00Z'), tests: { normal: ['A-1'] }, occupancy: '倉庫は使っていない', withSession: lz.withSession, log: () => {} }), /00:00〜01:30/);
  await assert.rejects(T.planTest({ lzMinRows: 1, dataDir, now: NOW, tests: { normal: ['A-1'] }, occupancy: '短い', withSession: lz.withSession, log: () => {} }), /occupancy/);
  await assert.rejects(T.planTest({ dataDir: fs.mkdtempSync(path.join(os.tmpdir(), 'lzt-empty-')), now: NOW, tests: { normal: ['A-1'] }, occupancy: '倉庫は使っていない', withSession: lz.withSession, log: () => {} }), /証跡が無い/);
});

await ta('[2] 取込の正しい流れ: 承認の印 → 鍵 → 直前 (商品・バーコード) → 照らし直し → 押す前の記録 → プレビュー → importing → 押す → 結果 → 直後 → 確かめ → verified → 鍵を返す', async () => {
  const dataDir = setupData(); const lz = fakeLz(); const pt = portal();
  const p = await planned(dataDir, lz);
  const r = await T.runTest({ ...runOpts(dataDir, p, pt), withSession: lz.withSession, notify: async () => { throw new Error('知らせない'); } });
  assert.equal(r.state, 'verified');
  assert.deepEqual(stagesOf(r), ['begin', 'prepared', 'previewed', 'state_importing', 'execute_issued', 'state_imported_unverified', 'verified_checked', 'state_verified', 'released']);
  for (const f of ['pre.csv', 'pre-barcode.csv', 'import.csv', 'post.csv', 'post-barcode.csv', 'verify.json', 'import.json']) assert.ok(fs.existsSync(path.join(r.runDir, f)), f);
  assert.equal(sha(fs.readFileSync(path.join(r.runDir, 'import.csv'))), JSON.parse(fs.readFileSync(path.join(p.dir, 'plan.json'))).test_csv.sha256);
  const st = S.getStatus(pt.db);
  assert.deepEqual([st.state, st.lock, st.run.detail.mode, st.run.detail.target_as_of, st.run.detail.plan_id], ['verified', null, 'test', AS_OF, p.planId]);
  assert.deepEqual(lz.st.calls, ['exportShohin', 'exportShohin', 'exportBarcodes', 'preview', 'execute', 'exportShohin', 'exportBarcodes']);   // 計画の書き出し + 取込の回
});

await ta('[3] 押す前に止める: 承認の印が違う・試験の CSV が変わった・バーコードの部品が無い (鍵の前) / 承認の後に一覧が変わった (照らし直し・鍵は返す・状態は変えない)', async () => {
  const dataDir = setupData(); const lz = fakeLz(); const pt = portal();
  const p = await planned(dataDir, lz);
  await assert.rejects(T.runTest({ ...runOpts(dataDir, p, pt, { sha256: 'f'.repeat(64) }), withSession: lz.withSession, notify: async () => true }), /承認の印/);
  await assert.rejects(T.runTest({ ...runOpts(dataDir, p, pt, { capabilities: { exportBarcodes: false } }), withSession: lz.withSession, notify: async () => true }), /バーコードの書き出しの部品が無い/);
  const tc = path.join(p.dir, 'test.csv'), orig = fs.readFileSync(tc);
  fs.writeFileSync(tc, Buffer.concat([orig, Buffer.from('\r\n')]));
  await assert.rejects(T.runTest({ ...runOpts(dataDir, p, pt), withSession: lz.withSession, notify: async () => true }), /承認した CSV と違う/);
  fs.writeFileSync(tc, orig);
  assert.equal(S.getStatus(pt.db).lock, null);
  lz.st.lz.get('B-2')[col('仕入単価')] = '999';   // 承認の後に一覧が変わった
  const r = await T.runTest({ ...runOpts(dataDir, p, pt), withSession: lz.withSession, notify: async () => true });
  assert.ok(stagesOf(r).includes('plan_changed'));
  assert.ok(!lz.st.calls.includes('execute') && !lz.st.calls.includes('preview'));
  assert.deepEqual([S.getStatus(pt.db).state, S.getStatus(pt.db).lock], ['idle', null]);
});

await ta('[4] ポータルの書き込み (K5): importing の応答を失ったが入っていた = 状態で照らして押す / 届かなかった = 押さない / 鍵の応答を失った = 止める', async () => {
  let dataDir = setupData(), lz = fakeLz(), pt = portal({ faults: { transition: (b) => (b.to === 'importing' ? 'lost_after' : null) } });
  let p = await planned(dataDir, lz);
  let r = await T.runTest({ ...runOpts(dataDir, p, pt), withSession: lz.withSession, notify: async () => true });
  assert.equal(r.state, 'verified');
  assert.ok(r.record.stages.find((s) => s.name === 'state_importing').confirmed);
  dataDir = setupData(); lz = fakeLz(); pt = portal({ faults: { transition: (b) => (b.to === 'importing' ? 'lost_before' : null) } });
  p = await planned(dataDir, lz);
  r = await T.runTest({ ...runOpts(dataDir, p, pt), withSession: lz.withSession, notify: async () => true });
  assert.ok(!lz.st.calls.includes('execute'), '押さない');
  assert.deepEqual([S.getStatus(pt.db).state, S.getStatus(pt.db).lock], ['idle', null]);
  dataDir = setupData(); lz = fakeLz(); pt = portal({ faults: { acquire: 'lost_after' } });
  p = await planned(dataDir, lz);
  await assert.rejects(T.runTest({ ...runOpts(dataDir, p, pt), withSession: lz.withSession, notify: async () => true }), /鍵を取れない/);
  assert.ok(!lz.st.calls.includes('preview'));
});

await ta('[5] 押す前の失敗 = failed_before_execute (前の状態に戻る) / 押した後の失敗 = unknown + 知らせ + 知らせ済み (K7・K9)', async () => {
  let dataDir = setupData(), lz = fakeLz({ over: { throwBeforeIssue: true } }), pt = portal();
  let p = await planned(dataDir, lz);
  let r = await T.runTest({ ...runOpts(dataDir, p, pt), withSession: lz.withSession, notify: async () => true });
  assert.deepEqual([S.getStatus(pt.db).state, stagesOf(r).includes('execute_not_issued'), stagesOf(r).includes('state_failed_before_execute')], ['idle', true, true]);
  dataDir = setupData(); lz = fakeLz({ over: { throwAfterIssue: true } }); pt = portal();
  p = await planned(dataDir, lz);
  const sent = [];
  r = await T.runTest({ ...runOpts(dataDir, p, pt), withSession: lz.withSession, notify: async (t) => { sent.push(t); return true; } });
  const st = S.getStatus(pt.db);
  assert.deepEqual([st.state, st.notified, sent.length, r.record.notified], ['unknown', true, 1, true]);
  assert.match(sent[0], /止まった: 状態 unknown/);
});

await ta('[6] 結果: エラー 1 件 = partial (直後の書き出しと差を残す・partial のまま) / ほかの商品やバーコードが変わった = verify_failed / 直後の書き出しの失敗 = 未確かめのまま → 確かめのやり直しで verified (H)', async () => {
  let dataDir = setupData(), lz = fakeLz({ over: { errorRow: 'B-2' } }), pt = portal();
  let p = await planned(dataDir, lz);
  let r = await T.runTest({ ...runOpts(dataDir, p, pt), withSession: lz.withSession, notify: async () => true });
  assert.deepEqual([S.getStatus(pt.db).state, fs.existsSync(path.join(r.runDir, 'verify.json'))], ['partial', true]);
  for (const over of [{ touchOther: true }, { touchBarcode: true }]) {
    dataDir = setupData(); lz = fakeLz({ over }); pt = portal();
    p = await planned(dataDir, lz);
    r = await T.runTest({ ...runOpts(dataDir, p, pt), withSession: lz.withSession, notify: async () => true });
    assert.equal(S.getStatus(pt.db).state, 'verify_failed', JSON.stringify(over));
  }
  dataDir = setupData(); lz = fakeLz({ over: { postExportFails: true } }); pt = portal();
  p = await planned(dataDir, lz);
  r = await T.runTest({ ...runOpts(dataDir, p, pt), withSession: lz.withSession, notify: async () => true });
  assert.deepEqual([S.getStatus(pt.db).state, stagesOf(r).includes('post_export_failed')], ['imported_unverified', true]);
  const lz2 = fakeLz(); lz2.st.lz = lz.st.lz; lz2.st.bc = lz.st.bc;   // 同じロジザード・書き出しは今度は通る
  const v = await T.verifyOnly({ lzMinRows: 1, dataDir, runId: r.runId, occupancy: '倉庫は使っていない (中原さん確認)', now: NOW, localInitFile: 'x', client: pt.client, checkInit: pt.checkInit, withSession: lz2.withSession, capabilities: { exportBarcodes: true }, notify: async () => true, log: () => {} });
  assert.deepEqual([v.state, S.getStatus(pt.db).state, S.getStatus(pt.db).lock], ['verified', 'verified', null]);
});

await ta('[7] 確かめのやり直し: 記録が壊れた (import.csv の sha256 が違う) = verify_failed (evidence_broken) + 知らせ / imported_unverified でない = 断る', async () => {
  const dataDir = setupData(); const lz = fakeLz({ over: { postExportFails: true } }); const pt = portal();
  const p = await planned(dataDir, lz);
  const r = await T.runTest({ ...runOpts(dataDir, p, pt), withSession: lz.withSession, notify: async () => true });
  fs.appendFileSync(path.join(r.runDir, 'import.csv'), 'x');
  const sent = [];
  const common = { lzMinRows: 1, dataDir, runId: r.runId, occupancy: '倉庫は使っていない (中原さん確認)', now: NOW, localInitFile: 'x', client: pt.client, checkInit: pt.checkInit, withSession: fakeLz().withSession, capabilities: { exportBarcodes: true }, notify: async (t) => { sent.push(t); return true; }, log: () => {} };
  const v = await T.verifyOnly(common);
  assert.deepEqual([v.state, v.reason, S.getStatus(pt.db).state, sent.length, S.getStatus(pt.db).notified], ['verify_failed', 'evidence_broken', 'verify_failed', 1, true]);
  await assert.rejects(T.verifyOnly(common), /やり直せる状態でない/);
});

await ta('[7b] 確かめのやり直し: verify_failed の応答を失ったが入っていた = 知らせる + 知らせ済み (出来事の番号は状態から)', async () => {
  const dataDir = setupData(); const lz = fakeLz({ over: { postExportFails: true } }); const pt = portal({ faults: { transition: (b) => (b.to === 'verify_failed' ? 'lost_after' : null) } });
  const p = await planned(dataDir, lz);
  const r = await T.runTest({ ...runOpts(dataDir, p, pt), withSession: lz.withSession, notify: async () => true });
  fs.appendFileSync(path.join(r.runDir, 'import.csv'), 'x');
  const sent = [];
  const v = await T.verifyOnly({ lzMinRows: 1, dataDir, runId: r.runId, occupancy: '倉庫は使っていない (中原さん確認)', now: NOW, localInitFile: 'x', client: pt.client, checkInit: pt.checkInit, withSession: fakeLz().withSession, capabilities: { exportBarcodes: true }, notify: async (x) => { sent.push(x); return true; }, log: () => {} });
  assert.deepEqual([v.state, S.getStatus(pt.db).state, sent.length, S.getStatus(pt.db).notified], ['verify_failed', 'verify_failed', 1, true]);
});

await ta('[8] 結果を書けない (応答が分からず照らしても入っていない) = 手元に残して「ポータルに書けない」と知らせる・取込はやり直さない', async () => {
  const dataDir = setupData(); const lz = fakeLz();
  const pt = portal({ faults: { transition: (b) => (b.to === 'imported_unverified' ? 'lost_before' : null) } });
  const p = await planned(dataDir, lz);
  const sent = [];
  const r = await T.runTest({ ...runOpts(dataDir, p, pt), withSession: lz.withSession, notify: async (t) => { sent.push(t); return true; } });
  assert.ok(stagesOf(r).includes('result_not_written'));
  assert.equal(lz.st.calls.filter((c) => c === 'execute').length, 1);
  assert.equal(S.getStatus(pt.db).state, 'importing');   // ポータルは importing のまま = 次の起動で unknown にする (人が見る)
  assert.equal(sent.length, 1);
});

await ta('[9] 鍵の延長が断られた = 旗が止まる = 押さない (failed_before_execute) (K7)', async () => {
  const dataDir = setupData(); const lz = fakeLz({ over: { previewDelayMs: 300 } });
  const pt = portal({ faults: {} });
  pt.client.extend = async () => { throw Object.assign(new Error('鍵が切れた'), { code: 'lock_lost', status: 409 }); };
  const p = await planned(dataDir, lz);
  const r = await T.runTest({ ...runOpts(dataDir, p, pt, { heartbeatMs: 50 }), withSession: lz.withSession, notify: async () => true });
  assert.ok(!lz.st.calls.includes('execute'), '押さない');
  assert.equal(r.record.heartbeat.kind, 'extend_failed');
  assert.equal(S.getStatus(pt.db).state, 'idle');
});

await ta('[10] 知らせの送り直し: 止まった状態で知らせがまだ = 送る → 知らせ済み / もう知らせた = 送らない / 送れない = send_failed', async () => {
  const pt = portal();
  const L = S.acquire(pt.db, { initId: pt.init_id, holder: 'auto', purpose: 'import', runId: 'lzim_test_x_1', ttlSec: 30, by: 't' });
  S.transition(pt.db, { lockToken: L.lock_token, runId: 'lzim_test_x_1', to: 'importing', detail: { csv_sha256: 'a'.repeat(64), rows: 1, mode: 'test', target_as_of: AS_OF, plan_id: 'lzt_p' }, by: 't' });
  S.transition(pt.db, { lockToken: L.lock_token, runId: 'lzim_test_x_1', to: 'unknown', by: 't' });
  assert.deepEqual(await T.notifyPending({ client: pt.client, notify: async () => false }), { sent: false, reason: 'send_failed', state: 'unknown' });
  const sent = [];
  const r = await T.notifyPending({ client: pt.client, notify: async (t) => { sent.push(t); return true; } });
  assert.deepEqual([r.sent, r.marked, S.getStatus(pt.db).notified], [true, 'ok', true]);
  assert.match(sent[0], /計画 lzt_p/);
  assert.equal((await T.notifyPending({ client: pt.client, notify: async () => true })).reason, 'already_notified');
});

await ta('[11] ポータルの書き込みの分け方 (K5): 決まった 4xx の断り・送る前に止まった = refused / 5xx・通信・読めない応答 = unknown → 照らす', async () => {
  const c = IO.classifyPortalError;
  assert.deepEqual([c({ status: 409, code: 'state' }), c({ status: 409, code: 'nightly_done' }), c({ status: 401, code: 'unauthorized' }), c({ status: null, code: 'no_token' }), c({ status: 503, code: 'not_configured' })], ['refused', 'refused', 'refused', 'refused', 'refused']);
  assert.deepEqual([c({ status: 500, code: 'internal' }), c({ status: null, code: 'unreachable' }), c({ status: 409, code: 'something_new' }), c({ status: 502, code: 'http_502' }), c(null)], ['unknown', 'unknown', 'unknown', 'unknown', 'unknown']);
  let r = await IO.portalWrite(async () => ({ ok: true, x: 1 }), { expect: (x) => x.x === 1 });
  assert.equal(r.outcome, 'ok');
  r = await IO.portalWrite(async () => ({ ok: true }), { expect: (x) => x.x === 1, confirm: async () => true });
  assert.deepEqual([r.outcome, r.confirmed], ['ok', true]);   // 読めない応答 = 照らして入っていた
  r = await IO.portalWrite(async () => { throw Object.assign(new Error('x'), { status: 500, code: 'internal' }); }, { confirm: async () => false });
  assert.deepEqual([r.outcome, r.confirmed], ['unknown', false]);
  r = await IO.portalWrite(async () => { throw Object.assign(new Error('x'), { status: 409, code: 'busy' }); }, { confirm: async () => { throw new Error('呼ばれない'); } });
  assert.equal(r.outcome, 'refused');
});

await ta('[12] 00:00〜01:30 は動かない・GChat は https だけ・引数', async () => {
  assert.deepEqual(['2030-01-15T14:59:00Z', '2030-01-15T15:00:00Z', '2030-01-15T16:29:00Z', '2030-01-15T16:30:00Z'].map((t) => T.inNightBlock(new Date(t))), [false, true, true, false]);
  assert.equal(await T.sendGChat('x', { env: { GCHAT_WEBHOOK: 'http://example.test/hook' }, fetchImpl: async () => ({ ok: true }) }), false);
  assert.equal(await T.sendGChat('x', { env: { GCHAT_WEBHOOK: 'https://example.test/hook' }, fetchImpl: async () => ({ ok: true }) }), true);
  assert.deepEqual(T.parseArgs(['plan', '--normal', 'A-1,B-2', '--missing', 'N-1:A-1', '--case', 'Abc-1:abc-1', '--occupancy', 'x']).tests,
    { normal: ['A-1', 'B-2'], missing: [{ id: 'N-1', copy_from: 'A-1' }], deleted: [], case: [{ id: 'Abc-1', from: 'abc-1' }] });
  assert.throws(() => T.parseArgs(['run', '--plan', 'p', '--sha256', 'short']), /64 桁/);
  assert.throws(() => T.parseArgs(['destroy']), /使い方/);
  assert.throws(() => T.parseArgs(['plan', '--force']), /知らない引数/);
});

await ta('[13] 夜の止め: 始めた後に 00:00 の手前を越えた = 押さない (鍵を延ばしても上限は越えない。Codex #1524 R1 High)', async () => {
  const dataDir = setupData(); const lz = fakeLz({ over: { previewDelayMs: 900 } }); const pt = portal();
  const p = await planned(dataDir, lz);
  // 23:59:50 JST に始める・夜の止めの余白 4.5 秒 → 旗の余白 5 秒を引くと押してよいのは 23:59:50.5 まで = プレビューの間に越える
  const r = await T.runTest({ ...runOpts(dataDir, p, pt, { now: new Date('2030-01-16T14:59:50Z'), nightMarginMs: 4500, heartbeatMs: 100 }), withSession: lz.withSession, notify: async () => true });
  assert.ok(!lz.st.calls.includes('execute'), '押さない');
  assert.ok(lz.st.calls.includes('preview'));
  assert.deepEqual([S.getStatus(pt.db).state, S.getStatus(pt.db).lock], ['idle', null]);
  assert.match(r.record.error, /押してよい時刻を過ぎた/);
  assert.equal(r.record.heartbeat.kind, 'extended');   // 鍵は延びた (それでも上限は越えない)
  // 鍵の延長は呼び手の締め切りの決まりで旗を動かす
  const set = [];
  const hb = IO.startHeartbeat({ client: { extend: async () => ({ ok: true, expires_at: 1000000 }) }, lockToken: 't', guard: { setDeadline: (d) => set.push(d), stop: () => {} }, mapDeadline: (e) => Math.min(e - 20000, 500000), setTimer: () => null, clearTimer: () => {} });
  await hb.tick();
  assert.deepEqual(set, [500000]);
  assert.equal(T.nextNightStart(new Date('2030-01-16T14:59:50Z')), Date.parse('2030-01-16T15:00:00Z'));
  assert.equal(T.nextNightStart(new Date('2030-01-15T16:00:00Z')), Date.parse('2030-01-16T15:00:00Z'));   // 01:00 JST = 次の夜は翌 00:00
});

await ta('[14] 確かめのやり直しの結果を書けない・違う応答 = verified と返さない (imported_unverified・result_not_written) + 知らせ (K5。Codex #1524 R1 High)', async () => {
  const dataDir = setupData(); const lz = fakeLz({ over: { postExportFails: true } }); const faults = {}; const pt = portal({ faults });
  const p = await planned(dataDir, lz);
  const r = await T.runTest({ ...runOpts(dataDir, p, pt), withSession: lz.withSession, notify: async () => true });
  assert.equal(S.getStatus(pt.db).state, 'imported_unverified');
  const lz2 = fakeLz(); lz2.st.lz = lz.st.lz; lz2.st.bc = lz.st.bc;
  const sent = [];
  const common = { lzMinRows: 1, dataDir, runId: r.runId, occupancy: '倉庫は使っていない (中原さん確認)', now: NOW, localInitFile: 'x', client: pt.client, checkInit: pt.checkInit, withSession: lz2.withSession, capabilities: { exportBarcodes: true }, notify: async (x) => { sent.push(x); return true; }, log: () => {} };
  faults.transition = (b) => (b.to === 'verified' ? 'lost_before' : null);
  let v = await T.verifyOnly(common);
  assert.deepEqual([v.state, v.reason, v.compared, S.getStatus(pt.db).state, S.getStatus(pt.db).lock, sent.length], ['imported_unverified', 'result_not_written', 'verified', 'imported_unverified', null, 1]);
  assert.match(sent[0], /結果をポータルに書けない/);
  delete faults.transition;
  const orig = pt.client.transition;
  pt.client.transition = async (b) => (b.to === 'verified' ? { ok: true, state: 'imported_unverified' } : orig(b));   // 書いていないのに成功の応答
  v = await T.verifyOnly(common);
  assert.deepEqual([v.state, v.reason, S.getStatus(pt.db).state, sent.length], ['imported_unverified', 'result_not_written', 'imported_unverified', 2]);
  pt.client.transition = orig;
  v = await T.verifyOnly(common);
  assert.deepEqual([v.state, S.getStatus(pt.db).state], ['verified', 'verified']);
});

await ta('[15] 押した後の失敗で unknown を書けない = 知らせる (importing のまま) / 送り直しは importing のまま鍵が無い回も拾う・鍵が生きている回は拾わない (Codex #1524 R1 Medium)', async () => {
  const dataDir = setupData(); const lz = fakeLz({ over: { throwAfterIssue: true } }); const pt = portal({ faults: { transition: (b) => (b.to === 'unknown' ? 'lost_before' : null) } });
  const p = await planned(dataDir, lz);
  const sent = [];
  const r = await T.runTest({ ...runOpts(dataDir, p, pt), withSession: lz.withSession, notify: async (x) => { sent.push(x); return true; } });
  assert.deepEqual([S.getStatus(pt.db).state, stagesOf(r).includes('result_not_written'), sent.length, lz.st.calls.filter((c) => c === 'execute').length], ['importing', true, 1, 1]);
  assert.match(sent[0], /ポータルに書けない \(unknown\)/);
  assert.match(sent[0], /importing のまま/);
  const n = await T.notifyPending({ client: pt.client, notify: async (x) => { sent.push(x); return true; } });
  assert.deepEqual([n.sent, n.reason, sent.length], [true, 'stuck_importing', 2]);
  const pt2 = portal();
  const L = S.acquire(pt2.db, { initId: pt2.init_id, holder: 'auto', purpose: 'import', runId: 'lzim_test_y_1', ttlSec: 60, by: 't' });
  S.transition(pt2.db, { lockToken: L.lock_token, runId: 'lzim_test_y_1', to: 'importing', detail: { csv_sha256: 'a'.repeat(64), rows: 1, mode: 'test', target_as_of: AS_OF, plan_id: 'lzt_p' }, by: 't' });
  assert.equal((await T.notifyPending({ client: pt2.client, notify: async () => true })).reason, 'nothing_to_notify');   // 取込の途中
  // 結果を書けない + ポータルの状態も読めない = それでも知らせる (手元の記録で)
  const dataDir3 = setupData(); const lz3 = fakeLz({ over: { throwAfterIssue: true } }); const pt3 = portal({ faults: { transition: (b) => (b.to === 'unknown' ? 'lost_before' : null) } });
  const p3 = await planned(dataDir3, lz3);
  const st3 = pt3.client.status;
  pt3.client.status = async (n) => { if (lz3.st.calls.includes('execute')) throw new Error('fetch failed'); return st3(n); };
  const sent3 = [];
  await T.runTest({ ...runOpts(dataDir3, p3, pt3), withSession: lz3.withSession, notify: async (x) => { sent3.push(x); return true; } });
  assert.equal(sent3.length, 1);
  assert.match(sent3[0], /ポータルに書けない \(unknown\)/);
});

await ta('[16] 記録を書けない: 押す前 (prepared・execute_issued) = 押さない / 押した後 = 状態は進めて知らせる (Codex #1524 R1 Medium)', async () => {
  for (const at of ['prepared', 'execute_issued']) {
    const dataDir = setupData(); const lz = fakeLz(); const pt = portal();
    const p = await planned(dataDir, lz);
    const wj = (f, obj) => { if (obj && obj.stage === at) throw new Error('ENOSPC'); T.writeJsonAtomic(f, obj); };
    await T.runTest({ ...runOpts(dataDir, p, pt, { writeJson: wj }), withSession: lz.withSession, notify: async () => true });
    assert.ok(!lz.st.calls.includes('execute'), at + ' 押さない');
    assert.deepEqual([S.getStatus(pt.db).state, S.getStatus(pt.db).lock], ['idle', null], at);
  }
  const dataDir = setupData(); const lz = fakeLz(); const pt = portal();
  const p = await planned(dataDir, lz);
  let broken = false;
  const wj = (f, obj) => { if (broken) throw new Error('ENOSPC'); T.writeJsonAtomic(f, obj); if (obj && obj.stage === 'execute_issued') broken = true; };
  const sent = [];
  const r = await T.runTest({ ...runOpts(dataDir, p, pt, { writeJson: wj }), withSession: lz.withSession, notify: async (x) => { sent.push(x); return true; } });
  assert.deepEqual([r.state, S.getStatus(pt.db).state, sent.length], ['verified', 'verified', 1]);
  assert.match(sent[0], /記録を書けない/);
});

await ta('[17] 状態の書き込みの成功の応答が行き先と違う = 入ったと見ない (状態を読み直して照らす。K5・Codex #1524 R1 High)', async () => {
  let dataDir = setupData(), lz = fakeLz(), pt = portal();
  let p = await planned(dataDir, lz);
  let orig = pt.client.transition;
  pt.client.transition = async (b) => (b.to === 'importing' ? { ok: true, state: 'idle' } : orig(b));   // 書いていないのに成功の応答
  await T.runTest({ ...runOpts(dataDir, p, pt), withSession: lz.withSession, notify: async () => true });
  assert.ok(!lz.st.calls.includes('execute'), '押さない');
  assert.equal(S.getStatus(pt.db).state, 'idle');
  dataDir = setupData(); lz = fakeLz({ over: { throwBeforeIssue: true } }); pt = portal();
  p = await planned(dataDir, lz);
  orig = pt.client.transition;
  pt.client.transition = async (b) => (b.to === 'failed_before_execute' ? { ok: true, state: 'verified' } : orig(b));   // 前の状態 (idle) と違う・書いていない
  const sent = [];
  const r = await T.runTest({ ...runOpts(dataDir, p, pt), withSession: lz.withSession, notify: async (x) => { sent.push(x); return true; } });
  assert.deepEqual([S.getStatus(pt.db).state, stagesOf(r).includes('result_not_written'), sent.length], ['importing', true, 1]);
});

await ta('[18] 返す状態 = この回の結末だけ: 前の回が verified でも、プレビューで止まった = not_started・押す前の失敗 = failed_before_execute (前の verified に戻っても成功と返さない。Codex #1524 R2)', async () => {
  for (const [over, want] of [[{ previewThrows: true }, 'not_started'], [{ throwBeforeIssue: true }, 'failed_before_execute']]) {
    const dataDir = setupData(); const lz = fakeLz({ over }); const pt = portal();
    const L = S.acquire(pt.db, { initId: pt.init_id, holder: 'auto', purpose: 'import', runId: 'lzim_test_prev_1', ttlSec: 60, by: 't' });
    S.transition(pt.db, { lockToken: L.lock_token, runId: 'lzim_test_prev_1', to: 'importing', detail: { csv_sha256: 'a'.repeat(64), rows: 1, mode: 'test', target_as_of: AS_OF, plan_id: 'lzt_p' }, by: 't' });
    S.transition(pt.db, { lockToken: L.lock_token, runId: 'lzim_test_prev_1', to: 'imported_unverified', by: 't' });
    S.transition(pt.db, { lockToken: L.lock_token, runId: 'lzim_test_prev_1', to: 'verified', by: 't' });
    S.release(pt.db, { lockToken: L.lock_token, by: 't' });
    const p = await planned(dataDir, lz);
    const r = await T.runTest({ ...runOpts(dataDir, p, pt), withSession: lz.withSession, notify: async () => true });
    assert.deepEqual([r.state, r.portalState, S.getStatus(pt.db).state, lz.st.calls.includes('execute')], [want, 'verified', 'verified', false], JSON.stringify(over));
  }
});

await ta('[19] 確かめのやり直しで記録 (verify-*.json) を書けない = 比べた結果の状態は書いて、書けなかったことも知らせる (Codex #1524 R2)', async () => {
  for (const touch of [true, false]) {
    const dataDir = setupData(); const lz = fakeLz({ over: { postExportFails: true } }); const pt = portal();
    const p = await planned(dataDir, lz);
    const r = await T.runTest({ ...runOpts(dataDir, p, pt), withSession: lz.withSession, notify: async () => true });
    const lz2 = fakeLz(); lz2.st.lz = lz.st.lz; lz2.st.bc = lz.st.bc.map((x) => [...x]);
    if (touch) lz2.st.bc[0][2] = '4900000000999';   // バーコードが変わった = verify_failed
    const sent = [];
    const wj = (f, obj) => { if (/^verify-/.test(path.basename(f))) throw new Error('ENOSPC'); T.writeJsonAtomic(f, obj); };
    const v = await T.verifyOnly({ lzMinRows: 1, dataDir, runId: r.runId, occupancy: '倉庫は使っていない (中原さん確認)', now: NOW, localInitFile: 'x', client: pt.client, checkInit: pt.checkInit, withSession: lz2.withSession, capabilities: { exportBarcodes: true }, notify: async (x) => { sent.push(x); return true; }, log: () => {}, writeJson: wj });
    const want = touch ? 'verify_failed' : 'verified';
    assert.deepEqual([v.state, S.getStatus(pt.db).state, sent.length], [want, want, 1], String(touch));
    assert.match(sent[0], /確かめの記録を書けない \(verify-/);
    if (touch) assert.equal(S.getStatus(pt.db).notified, true);
  }
});

await ta('[20] 直後の書き出しの中身が壊れている (本物の書き出しの検証の例外 invalid_csv) = 確かめの失敗 (verify_failed)・一時の失敗 (通信など) は未確かめのまま (K4・Codex #1524 R2)', async () => {
  let dataDir = setupData(), lz = fakeLz({ over: { postExportCorrupt: true } }), pt = portal();
  let p = await planned(dataDir, lz);
  let r = await T.runTest({ ...runOpts(dataDir, p, pt), withSession: lz.withSession, notify: async () => true });
  assert.deepEqual([r.state, S.getStatus(pt.db).state, stagesOf(r).includes('post_export_invalid')], ['verify_failed', 'verify_failed', true]);
  dataDir = setupData(); lz = fakeLz({ over: { postExportFails: true } }); pt = portal();
  p = await planned(dataDir, lz);
  r = await T.runTest({ ...runOpts(dataDir, p, pt), withSession: lz.withSession, notify: async () => true });
  assert.equal(S.getStatus(pt.db).state, 'imported_unverified');   // 一時の失敗 = 未確かめのまま
  const lz2 = fakeLz({ over: { corruptAlways: true } }); lz2.st.lz = lz.st.lz; lz2.st.bc = lz.st.bc;
  const v = await T.verifyOnly({ lzMinRows: 1, dataDir, runId: r.runId, occupancy: '倉庫は使っていない (中原さん確認)', now: NOW, localInitFile: 'x', client: pt.client, checkInit: pt.checkInit, withSession: lz2.withSession, capabilities: { exportBarcodes: true }, notify: async () => true, log: () => {} });
  assert.deepEqual([v.state, S.getStatus(pt.db).state], ['verify_failed', 'verify_failed']);
  // 本物の検証の例外: 壊れた Shift-JIS・列数違い = invalid_csv / HTML (ログイン切れ) = export_not_csv (一時の失敗)・文言は前と同じ
  const bad = SE.validateShohinCsv(Buffer.from([0x82, 0xff, 0x0a]), { minRows: 1 });
  assert.deepEqual([bad.ok, SE.invalidCsvError(bad.reason).code], [false, 'invalid_csv']);
  const html = SE.validateShohinCsv(Buffer.from('<html>user_id</html>'), { minRows: 1 });
  assert.equal(SE.invalidCsvError(html.reason).code, 'export_not_csv');
  assert.equal(SE.invalidCsvError('x').message, 'CSVの検証に失敗: x (既存CSVは温存しました)');
  assert.match(fs.readFileSync(new URL('../tools/logizard-automation/shohin-export.js', import.meta.url), 'utf8'), /throw invalidCsvError\(v\.reason\)/, '本物の書き出しが印つきの例外を投げる');
  assert.deepEqual([T.isInvalidExport(SE.invalidCsvError(bad.reason)), T.isInvalidExport(SE.invalidCsvError(html.reason)), T.isInvalidExport(new Error('通信'))], [true, false, false]);
});

await ta('[21] 確かめのやり直し: 書き出した CSV の保存に失敗しても、バーコードの書き出しの壊れで verify_failed まで進む・保存の失敗も知らせる (Codex #1524 R3)', async () => {
  const dataDir = setupData(); const lz = fakeLz({ over: { postExportFails: true } }); const pt = portal();
  const p = await planned(dataDir, lz);
  const r = await T.runTest({ ...runOpts(dataDir, p, pt), withSession: lz.withSession, notify: async () => true });
  const lz2 = fakeLz({ over: { barcodeCorruptAlways: true } }); lz2.st.lz = lz.st.lz; lz2.st.bc = lz.st.bc;
  const sent = [];
  const save = (f, buf) => { if (/^post-\d/.test(path.basename(f))) throw new Error('ENOSPC'); T.saveOnce(f, buf); };
  const v = await T.verifyOnly({ lzMinRows: 1, dataDir, runId: r.runId, occupancy: '倉庫は使っていない (中原さん確認)', now: NOW, localInitFile: 'x', client: pt.client, checkInit: pt.checkInit, withSession: lz2.withSession, capabilities: { exportBarcodes: true }, notify: async (x) => { sent.push(x); return true; }, log: () => {}, save });
  assert.deepEqual([v.state, S.getStatus(pt.db).state, sent.length, lz2.st.calls], ['verify_failed', 'verify_failed', 1, ['exportShohin', 'exportBarcodes']]);
  assert.match(sent[0], /確かめの記録を書けない \(post-\d/);
});

await ta('[22] 直後の書き出し: 商品の書き出しが失敗してもバーコードは取って残す (partial の戻しの証跡・K4)・一時の失敗 = partial のまま / 保存の失敗でも比べて verified まで進み知らせる (Codex #1524 R3)', async () => {
  const dataDir = setupData(); const lz = fakeLz({ over: { errorRow: 'B-2', postExportFails: true } }); const pt = portal();
  const p = await planned(dataDir, lz);
  const r = await T.runTest({ ...runOpts(dataDir, p, pt), withSession: lz.withSession, notify: async () => true });
  const after = lz.st.calls.slice(lz.st.calls.indexOf('execute') + 1);
  assert.deepEqual([S.getStatus(pt.db).state, after, fs.existsSync(path.join(r.runDir, 'post-barcode.csv')), fs.existsSync(path.join(r.runDir, 'post.csv')), stagesOf(r).includes('post_export_failed')],
    ['partial', ['exportShohin', 'exportBarcodes'], true, false, true]);
  const dataDir2 = setupData(); const lz3 = fakeLz(); const pt3 = portal();
  const p3 = await planned(dataDir2, lz3);
  const sent = [];
  const save = (f, buf) => { if (path.basename(f) === 'post.csv') throw new Error('ENOSPC'); T.saveOnce(f, buf); };
  const r3 = await T.runTest({ ...runOpts(dataDir2, p3, pt3, { save }), withSession: lz3.withSession, notify: async (x) => { sent.push(x); return true; } });
  assert.deepEqual([r3.state, S.getStatus(pt3.db).state, sent.length], ['verified', 'verified', 1]);
  assert.match(sent[0], /記録を書けない \(post\.csv\)/);
});

await ta('[23] 商品の書き出しが一時の失敗でも、取れたバーコードに差があれば verify_failed (取込の直後・確かめのやり直しの両方・Codex #1524 R4)', async () => {
  let dataDir = setupData(), lz = fakeLz({ over: { postExportFails: true, touchBarcode: true } }), pt = portal();
  let p = await planned(dataDir, lz);
  let r = await T.runTest({ ...runOpts(dataDir, p, pt), withSession: lz.withSession, notify: async () => true });
  assert.deepEqual([r.state, S.getStatus(pt.db).state], ['verify_failed', 'verify_failed']);
  const vj = JSON.parse(fs.readFileSync(path.join(r.runDir, 'verify.json'), 'utf8'));
  assert.deepEqual([vj.product.diffs[0].kind, vj.barcode.ok], ['post_not_exported', false]);
  dataDir = setupData(); lz = fakeLz({ over: { postExportFails: true } }); pt = portal();
  p = await planned(dataDir, lz);
  r = await T.runTest({ ...runOpts(dataDir, p, pt), withSession: lz.withSession, notify: async () => true });
  assert.equal(S.getStatus(pt.db).state, 'imported_unverified');   // 取れたバーコードは合っている = 未確かめのまま
  const lz2 = fakeLz({ over: { shohinTransientAlways: true } }); lz2.st.lz = lz.st.lz; lz2.st.bc = lz.st.bc.map((x) => [...x]);
  const common = { lzMinRows: 1, dataDir, runId: r.runId, occupancy: '倉庫は使っていない (中原さん確認)', now: NOW, localInitFile: 'x', client: pt.client, checkInit: pt.checkInit, withSession: lz2.withSession, capabilities: { exportBarcodes: true }, notify: async () => true, log: () => {} };
  let v = await T.verifyOnly(common);
  assert.deepEqual([v.state, v.reason, S.getStatus(pt.db).state], ['imported_unverified', 'post_export_failed', 'imported_unverified']);
  lz2.st.bc[0][2] = '4900000000999';   // バーコードが変わった
  v = await T.verifyOnly(common);
  assert.deepEqual([v.state, S.getStatus(pt.db).state], ['verify_failed', 'verify_failed']);
});

await ta('[24] 確かめのやり直しで import.json を書けない (始めの記録の後) = 状態は書いて、書けなかったことを知らせる (Codex #1524 R4)', async () => {
  const dataDir = setupData(); const lz = fakeLz({ over: { postExportFails: true } }); const pt = portal();
  const p = await planned(dataDir, lz);
  const r = await T.runTest({ ...runOpts(dataDir, p, pt), withSession: lz.withSession, notify: async () => true });
  const lz2 = fakeLz(); lz2.st.lz = lz.st.lz; lz2.st.bc = lz.st.bc;
  const sent = [];
  const wj = (f, obj) => { if (path.basename(f) === 'import.json' && obj.stage !== 'verify_only_begin') throw new Error('EACCES'); T.writeJsonAtomic(f, obj); };
  const v = await T.verifyOnly({ lzMinRows: 1, dataDir, runId: r.runId, occupancy: '倉庫は使っていない (中原さん確認)', now: NOW, localInitFile: 'x', client: pt.client, checkInit: pt.checkInit, withSession: lz2.withSession, capabilities: { exportBarcodes: true }, notify: async (x) => { sent.push(x); return true; }, log: () => {}, writeJson: wj });
  assert.deepEqual([v.state, S.getStatus(pt.db).state, sent.length], ['verified', 'verified', 1]);
  assert.match(sent[0], /確かめの記録を書けない \(import\.json: EACCES\)/);
});

await ta('[25] バーコードの書き出しが途中で切れた (同じ回の商品マスタの商品が無い): 直前 = 押さない (K4) / 後 = verify_failed (Codex #1530 R2 High)', async () => {
  let dataDir = setupData(), lz = fakeLz(), pt = portal();
  let p = await planned(dataDir, lz);
  lz.st.bc = lz.st.bc.filter((r) => r[0] !== 'C-3');   // 直前の書き出しから C-3 が抜けた
  let r = await T.runTest({ ...runOpts(dataDir, p, pt), withSession: lz.withSession, notify: async () => true });
  assert.ok(!lz.st.calls.includes('execute'), '押さない');
  assert.match(r.record.error, /直前のバーコードの書き出しに無い商品がある.*C-3/);
  assert.deepEqual([r.state, S.getStatus(pt.db).state], ['not_started', 'idle']);
  dataDir = setupData(); lz = fakeLz({ over: { postBarcodeCut: true } }); pt = portal();
  p = await planned(dataDir, lz);
  r = await T.runTest({ ...runOpts(dataDir, p, pt), withSession: lz.withSession, notify: async () => true });
  assert.deepEqual([r.state, S.getStatus(pt.db).state], ['verify_failed', 'verify_failed']);
  const vj = JSON.parse(fs.readFileSync(path.join(r.runDir, 'verify.json'), 'utf8'));
  assert.ok(vj.barcode.diffs.some((d) => d.kind === 'missing_in_post_barcode' && d.head.includes('C-3')), JSON.stringify(vj.barcode.diffs));
});

await ta('[26] 取込の後に確かめられない形 = 押す前に止める: 比べる商品がバーコードの書き出しの最後の商品 / 商品ごとの行がひとまとまりでない (Codex #1530 R4 Medium)', async () => {
  for (const [bc, re] of [[[['A-1', 'a', '4900000000001'], ['C-3', 'c', '4900000000003'], ['B-2', 'b', '4900000000002']], /比べる商品 B-2 がバーコードの書き出しの最後の商品/],
    [[['A-1', 'a', '4900000000001'], ['B-2', 'b', '4900000000002'], ['A-1', 'a', '4900000000009'], ['C-3', 'c', '4900000000003']], /ひとまとまりでない/]]) {
    const dataDir = setupData(); const lz = fakeLz(); const pt = portal();
    const p = await planned(dataDir, lz);
    lz.st.bc = bc;
    const r = await T.runTest({ ...runOpts(dataDir, p, pt), withSession: lz.withSession, notify: async () => true });
    assert.ok(!lz.st.calls.includes('execute') && !lz.st.calls.includes('preview'), '押さない');
    assert.match(r.record.error, re);
    assert.deepEqual([r.state, S.getStatus(pt.db).state], ['not_started', 'idle']);
  }
});

await ta('[27] 共通の仕組み (3b-1): POLICIES の外の決まり (試験の決まりの写しでも) = 初期化の照合・鍵・ロジザードに触らずに断る / 決まりは凍結・decided:false を許すのは試験だけ / ランナーは同じ部品', async () => {
  const dataDir = setupData(); const lz = fakeLz(); const pt = portal();
  const p = await planned(dataDir, lz);
  const touched = [], callsAfterPlan = lz.st.calls.length;   // 計画の書き出しの後から数える
  const spyInit = async (...a) => { touched.push('checkInit'); return pt.checkInit(...a); };
  const spySession = (fn) => { touched.push('session'); return lz.withSession(fn); };
  const copy = { ...E.POLICIES.test };
  const { plan_id: _pid, created_at: _cat, occupancy: _occ, ...body } = JSON.parse(fs.readFileSync(path.join(p.dir, 'plan.json'), 'utf8'));
  const base = { lzMinRows: 1, runsDir: path.join(dataDir, 'engine-runs'), csvBuf: fs.readFileSync(path.join(p.dir, 'test.csv')),
    csv: { sha256: body.test_csv.sha256, rows: body.test_csv.rows, target_as_of: body.source.as_of, source_run_id: body.source.run_id },
    occupancy: '倉庫は使っていない (中原さん確認)', now: NOW, localInitFile: 'x', client: pt.client, checkInit: spyInit, withSession: spySession,
    capabilities: { exportBarcodes: true }, notify: async () => true, createGuard: G.createGuard, log: () => {}, heartbeatMs: 60000 };
  await assert.rejects(E.importOne({ ...base, policy: copy }), /POLICIES に無い/);
  await assert.rejects(E.importOne({ ...base, policy: undefined }), /POLICIES に無い/);
  await assert.rejects(E.verifyAgain({ ...base, policy: copy, runId: 'lzim_test_x', locateRun: () => { touched.push('locate'); return dataDir; } }), /POLICIES に無い/);
  // 試験の決まりでも、呼び手の値 (context) の欠け・余計なキー (予約のキーで固定の項目を上書き)・計画と承認の印・CSV の違い = 何も触らずに断る (Codex #1535 R1・R2)
  const ctx = { planId: p.planId, planSha256: p.planSha256, plan: body };
  const emptyGroups = { ...body, groups: [] };
  const otherCsv = csvBuf(['形式/型番', '商品名', 'ふりがな', '仕入単価', '取引先id'], [['A-1', '別の名前', '別の名前', '1', '0007'], ['B-2', 'B', 'B', '0', '0002']]);
  const broken = Buffer.from('abc');
  for (const [name, over, re] of [
    ['context なし', { context: undefined }, /context が無い/],
    ['計画なし', { context: { planId: ctx.planId, planSha256: ctx.planSha256 } }, /キーは/],
    ['前の形 (preCheck・extraIds を呼び手が渡す)', { context: { planId: ctx.planId, planSha256: ctx.planSha256, preCheck: () => null, extraIds: [] } }, /キーは/],
    ['tag で mode を上書き', { context: { ...ctx, tag: { mode: 'nightly' } } }, /キーは/],
    ['recExtra で run_id を上書き', { context: { ...ctx, recExtra: { run_id: 'x' } } }, /キーは/],
    ['計画が承認の印と違う', { context: { ...ctx, plan: { ...body, rows: body.rows.slice(1) } } }, /承認の印と違う/],
    ['組の商品が空の計画 (印も計算し直した)', { context: { ...ctx, plan: emptyGroups, planSha256: TP.planSha256(emptyGroups) } }, /計画の組の商品/],
    ['計画の CSV と違う CSV', { context: ctx, csvBuf: otherCsv, csv: { ...base.csv, sha256: sha(otherCsv), rows: 2 } }, /計画の CSV と違う/],
    ['CSV の出どころが計画と違う', { context: ctx, csv: { ...base.csv, source_run_id: 'lzd_other' } }, /CSV の識別が計画と違う/],
    ['承認の印の形', { context: { ...ctx, planSha256: 'short' } }, /planSha256/],
    ['計画 ID の形', { context: { ...ctx, planId: '../x' } }, /planId/],
    ['CSV の識別が中身と違う', { context: ctx, csv: { ...base.csv, sha256: 'b'.repeat(64) } }, /csv\.sha256/],
    ['CSV の識別に余計なキー', { context: ctx, csv: { ...base.csv, mode: 'nightly' } }, /キーは/],
    ['壊れた CSV (sha256 は合っている)', { context: ctx, csvBuf: broken, csv: { ...base.csv, sha256: sha(broken) } }, /取り込む CSV の形が違う/],
    ['行数が CSV と違う', { context: ctx, csv: { ...base.csv, rows: base.csv.rows + 1 } }, /csv\.rows/],
    ['実在しない日', { context: ctx, csv: { ...base.csv, target_as_of: '2030-02-30' } }, /target_as_of/],
    ['出どころが無い', { context: ctx, csv: { ...base.csv, source_run_id: undefined } }, /source_run_id/],
  ]) await assert.rejects(E.importOne({ ...base, policy: E.POLICIES.test, ...over }), re, name);
  for (const c of [{}, { readExtraIds: () => ['A-1'] }]) await assert.rejects(E.verifyAgain({ ...base, policy: E.POLICIES.test, runId: 'lzim_test_x', context: c, locateRun: () => { touched.push('locate'); return dataDir; } }), /キーは/);
  assert.deepEqual([touched, lz.st.calls.slice(callsAfterPlan), S.getStatus(pt.db).state, fs.existsSync(base.runsDir)], [[], [], 'idle', false]);
  // 決まりは凍結 (書き換えて使えない)・decided:false の確かめの決まりを許すのは試験だけ
  assert.ok(Object.isFrozen(E.POLICIES) && Object.values(E.POLICIES).every((x) => Object.isFrozen(x)));
  for (const x of Object.values(E.POLICIES)) assert.ok(x.allowUndecided ? x.mode === 'test' : V.compileRules(x.rules).decided, x.name);
  assert.deepEqual([E.POLICIES.test.holder, E.POLICIES.test.mode, E.POLICIES.test.by, E.POLICIES.test.rules, E.POLICIES.test.barcode], ['auto', 'test', 'lz-import-test', V.RULES_2B1, true]);
  assert.match(T.newRunId(NOW), /^lzim_test_20300116T030000_[0-9a-f]{6}$/);
  for (const k of ['STOP_STATES', 'inNightBlock', 'nextNightStart', 'writeJsonAtomic', 'saveOnce', 'grabPostExports', 'isInvalidExport']) assert.equal(T[k], E[k], k);
  // 試験のランナーを通すと試験の決まりで動く (鍵の持ち主 auto・mode test・名乗り)
  const r = await T.runTest({ ...runOpts(dataDir, p, pt), withSession: lz.withSession, notify: async () => true });
  const st = S.getStatus(pt.db);
  assert.deepEqual([r.state, st.state, st.run.by, st.run.detail.mode, st.run.detail.plan_id, r.record.mode, r.record.plan_id], ['verified', 'verified', 'auto', 'test', p.planId, 'test', p.planId]);
  // 確かめは決まりの確かめの決まりで (版を記録に残す)
  const vj = JSON.parse(fs.readFileSync(path.join(r.runDir, 'verify.json'), 'utf8'));
  assert.deepEqual([r.record.verify.rules_version, vj.product.rules_version, st.run.detail.verify_detail.rules_version], [V.RULES_2B1.version, V.RULES_2B1.version, V.RULES_2B1.version]);
  // ポータルの importing の値・記録の頭は今までと同じ (取り出しで変わっていない)
  const tcsv = JSON.parse(fs.readFileSync(path.join(p.dir, 'plan.json'), 'utf8')).test_csv;
  const { started_at: _s, result: _r, result_detail: _rd, result_at: _ra, verify: _v, verify_detail: _vd, verify_at: _va, ...imp } = st.run.detail;
  assert.deepEqual(imp, { csv_sha256: tcsv.sha256, rows: tcsv.rows, mode: 'test', target_as_of: AS_OF, source_run_id: RUN_DIR, plan_id: p.planId });
  assert.deepEqual([Object.keys(r.record).slice(0, 6), r.record.plan_sha256, r.record.occupancy], [['run_id', 'plan_id', 'plan_sha256', 'mode', 'started_at', 'occupancy'], p.planSha256, '倉庫は使っていない (中原さん確認)']);
});

await ta('[28] 共通の仕組みに渡す試験だけの値が効く (3b-1): importing の応答不明の照らしは計画 ID まで見る / 確かめのやり直しは同じ持ち主の回だけ / 計画の組の商品 (CSV に無い) のバーコードも比べる', async () => {
  // importing は入ったが応答を失った・読み直した状態の計画 ID が違う = この回と見ない = 押さない
  let dataDir = setupData(), lz = fakeLz(), pt = portal({ faults: { transition: (b) => (b.to === 'importing' ? 'lost_after' : null) } });
  let p = await planned(dataDir, lz);
  const client = { ...pt.client, status: async (n) => { const s = await pt.client.status(n); if (s.state === 'importing' && s.run && s.run.detail) s.run.detail = { ...s.run.detail, plan_id: 'lzt_other' }; return s; } };
  const sentTexts = [];
  let r = await T.runTest({ ...runOpts(dataDir, p, pt, { client }), withSession: lz.withSession, notify: async (x) => { sentTexts.push(x); return true; } });
  assert.ok(!lz.st.calls.includes('execute'), '押さない');
  assert.match(r.record.error, /importing を書けない/);
  // 知らせの文は今までと同じ (試験の名前・計画の ID)
  assert.deepEqual(sentTexts, [`⚠️ ロジザードの取込の試験 ${r.runId} が止まった: ポータルは importing のまま・計画 ${p.planId}。記録 = ${r.runDir}\n解除は人 (ロジザードのインポート履歴を確かめてから import-state-cli.js resolve / mark-unknown)`]);
  // 確かめのやり直しは、この決まりの回 (持ち主 auto・mode test) で、記録も同じ回のときだけ (鍵を取りに行く前に断る。Codex #1535 R1 Medium)
  const runId = 'lzim_test_20300116T030000_abcdef';
  const unverified = ({ holder, mode, recMode = 'test', byOverride = null }) => {
    const dd = setupData(), l = fakeLz(), q = portal();
    // 毎晩の回 = ③c-1b-3b の旗を立てて、同じ識別の成果物がポータルにあるときだけ始められる (K3-1)。旧い手の ③ = 旗が無いとき (今までの動き)
    const prevFlag = process.env.LZ_MANUAL_V4;
    if (mode === 'nightly') process.env.LZ_MANUAL_V4 = 'on'; else delete process.env.LZ_MANUAL_V4;
    try {
      if (holder === 'manual_daily') S.halt(q.db, { by: 'x', reason: '旧い手の ③ の試験' });
      const a = S.acquire(q.db, { initId: q.init_id, holder, purpose: 'import', runId, by: 'x' });
      const buf = dailyBuf();
      const detail = { mode, target_as_of: AS_OF, csv_sha256: sha(buf), rows: DAILY.length, source_run_id: RUN_DIR };
      if (mode === 'nightly') {
        assert.throws(() => S.transition(q.db, { lockToken: a.lock_token, runId, to: 'importing', detail, by: 'x' }), (e) => e.code === 'artifact_missing');   // 成果物が無い = 始めない
        S.putArtifact(q.db, { sourceRunId: RUN_DIR, targetAsOf: AS_OF, verdict: 'pass', csvBuf: buf, sha256: sha(buf), rows: DAILY.length, by: 'lz-daily' });
      }
      S.transition(q.db, { lockToken: a.lock_token, runId, to: 'importing', detail, by: 'x' });
      S.transition(q.db, { lockToken: a.lock_token, runId, to: 'imported_unverified', by: 'x' });
      S.release(q.db, { lockToken: a.lock_token, by: 'x' });
    } finally {
      if (prevFlag === undefined) delete process.env.LZ_MANUAL_V4; else process.env.LZ_MANUAL_V4 = prevFlag;
    }
    const rd = path.join(dd, 'lz-import-test', 'lzt_x', 'runs', runId);
    fs.mkdirSync(rd, { recursive: true });
    fs.writeFileSync(path.join(rd, 'import.json'), JSON.stringify({ run_id: runId, mode: recMode, stages: [] }));
    // byOverride = ポータルが返す回の持ち主だけ違う (本物の状態の機械では mode と持ち主は組 = 持ち主の照らしを単独で試す)
    const c = byOverride ? { ...q.client, status: async (n) => { const x = await q.client.status(n); if (x.run) x.run = { ...x.run, by: byOverride }; return x; } } : q.client;
    return { dd, l, q, c };
  };
  // 旧い手の ③ の回 (manual_daily・旗が無いときだけ作れる) も試験の決まりでは確かめない
  for (const [name, o, re] of [['旧い手の ③ の回', { holder: 'manual_daily', mode: 'manual' }, /確かめをやり直せる状態でない/], ['毎晩の回 (同じ持ち主 auto)', { holder: 'auto', mode: 'nightly' }, /確かめをやり直せる状態でない \(その回の mode = nightly/],
    ['持ち主が違う回', { holder: 'auto', mode: 'test', byOverride: 'manual_daily' }, /確かめをやり直せる状態でない \(今 = /], ['記録の mode が違う', { holder: 'auto', mode: 'test', recMode: 'nightly' }, /記録が違う回/]]) {
    const { dd, l, q, c } = unverified(o);
    await assert.rejects(T.verifyOnly({ lzMinRows: 1, dataDir: dd, runId, occupancy: '倉庫は使っていない (中原さん確認)', now: NOW, localInitFile: 'x', client: c, checkInit: q.checkInit,
      withSession: l.withSession, capabilities: { exportBarcodes: true }, notify: async () => true, log: () => {} }), re, name);
    assert.deepEqual([S.getStatus(q.db).state, S.getStatus(q.db).lock, l.st.calls], ['imported_unverified', null, []], name);
  }
  // 計画の組: N-1 (無い商品・A-1 の写し) を取り込む = CSV に A-1 は無いが、組の A-1 のバーコードが変わった = 差として残す
  dataDir = setupData(); lz = fakeLz({ over: { errorRow: 'N-1', touchBarcode: true } }); pt = portal();
  const mp = await T.planTest({ lzMinRows: 1, dataDir, now: NOW, tests: { normal: ['B-2'], missing: [{ id: 'N-1', copy_from: 'A-1' }] }, mapping: { version: 'm1', furiganaCol: '検索名称', costRule: 'same' },
    occupancy: '倉庫は使っていない (中原さん確認)', withSession: lz.withSession, log: () => {} });
  r = await T.runTest({ ...runOpts(dataDir, mp, pt), withSession: lz.withSession, notify: async () => true });
  const vj = JSON.parse(fs.readFileSync(path.join(r.runDir, 'verify.json'), 'utf8'));
  assert.ok(vj.barcode.diffs.some((d) => d.id === 'A-1'), JSON.stringify(vj.barcode.diffs));
  assert.equal(S.getStatus(pt.db).state, 'partial');
  // 確かめのやり直し: その回の計画 (plan.json) が記録の計画の ID・承認の印と違う (組を空に・ID を変えた) = 比べる商品が分からない = 確かめない・未確かめのまま・知らせる (Codex #1535 R2 Medium)
  for (const tamper of [(x) => ({ ...x, groups: [] }), (x) => ({ ...x, plan_id: 'lzt_other' })]) {
    const dd = setupData(); const l = fakeLz({ over: { postExportFails: true } }); const q = portal();
    const pp = await planned(dd, l);
    const rr = await T.runTest({ ...runOpts(dd, pp, q), withSession: l.withSession, notify: async () => true });
    assert.equal(S.getStatus(q.db).state, 'imported_unverified');
    const pf = path.join(pp.dir, 'plan.json');
    fs.writeFileSync(pf, JSON.stringify(tamper(JSON.parse(fs.readFileSync(pf, 'utf8')))));
    const sent = [];
    await assert.rejects(T.verifyOnly({ lzMinRows: 1, dataDir: dd, runId: rr.runId, occupancy: '倉庫は使っていない (中原さん確認)', now: NOW, localInitFile: 'x', client: q.client, checkInit: q.checkInit,
      withSession: fakeLz().withSession, capabilities: { exportBarcodes: true }, notify: async (x) => { sent.push(x); return true; }, log: () => {} }), /計画 \(plan\.json\) が記録の計画の ID・承認の印と違う/);
    assert.deepEqual([S.getStatus(q.db).state, S.getStatus(q.db).lock, sent.length], ['imported_unverified', null, 1]);
    assert.match(sent[0], /途中で失敗: 計画 \(plan\.json\)/);
  }
});

console.log(`\n${passed} 件 PASS${process.exitCode ? ' (NG あり)' : ''}`);
process.exit(process.exitCode || 0);
