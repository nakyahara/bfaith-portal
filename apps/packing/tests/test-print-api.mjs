/**
 * test-print-api.mjs — 🖨 印刷キューAPI の認証境界と受け渡し (実HTTPで確認)
 *
 * ここで守りたいのは「誰が送り状を刷れるか」。
 *   - 認証なし / iPad の端末Cookie では**絶対に**印刷ジョブを取れない
 *   - 出力先プリンターはサーバが決めた名前しか返さない
 *   - PDF は lease を持っている端末にだけ渡し、中身が登録時と違えば渡さない
 * 実行: node apps/packing/tests/test-print-api.mjs
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import http from 'node:http';
import crypto from 'node:crypto';

const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'packing-printapi-test-'));
process.env.DATA_DIR = tmpDir;
delete process.env.PACKING_REPRINT_WEBHOOK;   // 通知は飛ばさない

const express = (await import('express')).default;
const { initPackingDB, getDB, utcNow, createDevice } = await import('../db.js');
const { enqueuePrintJob } = await import('../print-queue.js');
const { REPRINTS_DIR } = await import('../reprint-pdf.js');
const router = (await import('../router.js')).default;

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };
const eq = (a, b, label) => ok(JSON.stringify(a) === JSON.stringify(b), `${label} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

initPackingDB();
const db = getDB();
const now = utcNow();
db.prepare(`INSERT INTO pk_pack_batches (id, tb_key, folder_name, work_date, slip_count, line_count,
  total_qty, match_status, status, worker, csv_sha256, imported_by, created_at, updated_at)
  VALUES (1, 'TB1', '出荷_32', '2026-08-28', 1, 1, 1, 'ok', 'packing', '大場', 'x', 't', ?, ?)`).run(now, now);
const reprintId = Number(db.prepare(`INSERT INTO pk_pack_reprints (batch_id, slip_seq, ne_slip_no,
  site_order_no, folder_name, recipient_name, requested_by, created_at, kind, pdf_token, pdf_by,
  pdf_printable, pdf_ink_ratio)
  VALUES (1, 22, '1538886', '503-6875639-2990233', '出荷_32', '川野', '大場', ?, 'reprint', 'tokenAAAAAAAAAAAA', 'manifest', 1, 0.28)`)
  .run(now).lastInsertRowid);

// 印刷対象のPDF実体 (中身は何でもよい — 突合するのは sha256)
fs.mkdirSync(REPRINTS_DIR, { recursive: true });
const pdfBytes = Buffer.from('%PDF-1.4 dummy label');
fs.writeFileSync(path.join(REPRINTS_DIR, 'tokenAAAAAAAAAAAA.pdf'), pdfBytes);
const sha = crypto.createHash('sha256').update(pdfBytes).digest('hex');
const { id: jobId } = enqueuePrintJob(reprintId, { pdfSha256: sha });

const agent = createDevice('出荷PC', 'test', { kind: 'agent', printerName: 'Munbyn ITPP941(300DPI)' });
const ipad = createDevice('梱包iPad1', 'test');

const app = express();
app.use(express.json());
app.use((req, res, next) => { req.session = {}; next(); });   // 未ログイン相当
app.use('/apps/packing', router);
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const base = `http://127.0.0.1:${server.address().port}/apps/packing`;

const call = async (method, url, { token = null, cookie = null, body = null } = {}) => {
  const headers = {};
  if (token) headers.Authorization = `Bearer ${token}`;
  if (cookie) headers.Cookie = cookie;
  if (body) headers['Content-Type'] = 'application/json';
  const res = await fetch(base + url, { method, headers, body: body ? JSON.stringify(body) : undefined });
  const ct = res.headers.get('content-type') || '';
  return { status: res.status, body: ct.includes('json') ? await res.json().catch(() => ({})) : null, res };
};

console.log('── 誰が印刷ジョブを取れるか ──');
{
  eq((await call('GET', '/print/next')).status, 401, '認証なしでは取れない');
  eq((await call('GET', '/print/next', { token: 'not-a-real-token' })).status, 401, '不正トークンは取れない');
  // 🚨 共用iPadのCookieで送り状を勝手に刷れる状態を作らない
  eq((await call('GET', '/print/next', { cookie: `pk_pack_device=${ipad.token}` })).status, 401,
    'iPad の端末Cookieでは取れない');
  eq((await call('GET', '/print/next', { token: ipad.token })).status, 401,
    'iPad のトークンを Authorization に付けても取れない (kind=agent のみ)');
}

console.log('\n── エージェントの一連の流れ ──');
let lease = null;
{
  const r = await call('GET', '/print/next', { token: agent.token });
  eq(r.status, 200, 'エージェントは lease できる');
  lease = r.body.job;
  eq({ id: lease.id, printer: lease.printerName, ne: lease.neSlipNo },
    { id: jobId, printer: 'Munbyn ITPP941(300DPI)', ne: '1538886' },
    '出力先はサーバが決めた名前・対象は積んだジョブ');
  eq((await call('GET', '/print/next', { token: agent.token })).status, 204, '他に無ければ 204');
}

console.log('\n── PDF の受け渡し ──');
{
  const r = await call('GET', `/print/${jobId}/pdf?lease=${lease.leaseToken}`, { token: agent.token });
  eq(r.status, 200, 'lease を持つ端末には渡す');
  eq(Buffer.from(await r.res.arrayBuffer()).toString(), pdfBytes.toString(), '中身が一致');
  eq((await call('GET', `/print/${jobId}/pdf`, { token: ipad.token })).status, 401, 'iPad には渡さない');
  eq((await call('GET', `/print/${jobId}/pdf?lease=not-a-real-lease`, { token: agent.token })).status, 404,
    'lease token が違えば渡さない');
  eq((await call('GET', `/print/${jobId}/pdf`, { token: agent.token })).status, 404,
    'lease token を省略しても渡さない (端末認証だけでは不十分)');

  // 抜き出した後にファイルが差し替わっていたら刷らせない
  fs.writeFileSync(path.join(REPRINTS_DIR, 'tokenAAAAAAAAAAAA.pdf'), Buffer.from('%PDF-1.4 すり替え'));
  eq((await call('GET', `/print/${jobId}/pdf?lease=${lease.leaseToken}`, { token: agent.token })).status, 409,
    '登録時と中身が違うPDFは渡さない');
  fs.writeFileSync(path.join(REPRINTS_DIR, 'tokenAAAAAAAAAAAA.pdf'), pdfBytes);
}

console.log('\n── 報告 ──');
{
  eq((await call('POST', `/print/${jobId}/submitted`, {
    token: agent.token, body: { lease: 'not-a-real-lease', spool_job_id: '1' },
  })).status, 409, '偽 lease の投入報告は拒否');
  eq((await call('POST', `/print/${jobId}/submitted`, {
    token: agent.token, body: { lease: lease.leaseToken, spool_job_id: '42' },
  })).status, 200, 'スプーラー投入を報告できる');
  eq((await call('POST', `/print/${jobId}/completed`, {
    token: agent.token, body: { lease: lease.leaseToken },
  })).status, 400, 'ok を省略した完了報告は拒否 (欠落を成功扱いしない)');
  eq((await call('POST', `/print/${jobId}/completed`, {
    token: agent.token, body: { lease: lease.leaseToken, ok: 'true' },
  })).status, 400, 'ok が文字列なら拒否');
  // 応答が失われた再送を 409 で弾くとエージェントが復旧できない。成功として返し、
  // 「前回と同じ報告」だと分かるよう replayed を返す
  const again = await call('POST', `/print/${jobId}/submitted`, {
    token: agent.token, body: { lease: lease.leaseToken, spool_job_id: '42' },
  });
  eq({ status: again.status, replayed: again.body.replayed }, { status: 200, replayed: true },
    '同じ投入報告の再送は replayed:true の成功');
  eq((await call('POST', `/print/${jobId}/completed`, {
    token: agent.token, body: { lease: lease.leaseToken, ok: true },
  })).status, 200, '完了を報告できる');
  const done2 = await call('POST', `/print/${jobId}/completed`, {
    token: agent.token, body: { lease: lease.leaseToken, ok: true },
  });
  eq({ status: done2.status, replayed: done2.body.replayed }, { status: 200, replayed: true },
    '同じ完了報告の再送も replayed:true の成功');
  const st = await call('GET', `/print/${jobId}/status`, { token: agent.token });
  eq({ status: st.status, state: st.body.job?.state, spool: st.body.job?.spool_job_id },
    { status: 200, state: 'completed', spool: '42' }, '再起動後に状態を照会できる');
  eq((await call('GET', `/print/${jobId}/status`, { token: agent.token })).status, 200, '照会は何度でもよい');
  eq(db.prepare('SELECT state, spool_job_id FROM pk_print_jobs WHERE id=?').get(jobId),
    { state: 'completed', spool_job_id: '42' }, 'DBに反映される');
  eq((await call('POST', '/print/heartbeat', { token: agent.token, body: { note: '準備完了' } })).status, 200,
    '生存報告できる');
  eq((await call('POST', '/print/heartbeat', { body: { note: 'x' } })).status, 401, '認証なしの生存報告は拒否');
}

console.log('\n── 出力先が登録されていない端末には配らない ──');
{
  const noPrinter = createDevice('設定途中のPC', 'test', { kind: 'agent', printerName: null });
  const rid2 = Number(db.prepare(`INSERT INTO pk_pack_reprints (batch_id, slip_seq, ne_slip_no,
    site_order_no, folder_name, recipient_name, requested_by, created_at, kind, pdf_token, pdf_by, pdf_printable)
    VALUES (1, 23, '1538887', '503-0000000-0000001', '出荷_32', '川野', '大場', ?, 'reprint', 'tokenBBBBBBBBBBBB', 'manifest', 1)`)
    .run(now).lastInsertRowid);
  enqueuePrintJob(rid2, { pdfSha256: sha });
  eq((await call('GET', '/print/next', { token: noPrinter.token })).status, 409,
    'どこに出るか分からないまま刷らせない');
  // 掴んでから断ると、正常なジョブの試行回数だけが減って failed に落ちてしまう
  eq(db.prepare('SELECT state, attempt_count FROM pk_print_jobs WHERE ne_slip_no=?').get('1538887'),
    { state: 'queued', attempt_count: 0 }, '断られたジョブは queued のまま (試行回数を消費しない)');
}

await new Promise((r) => server.close(r));
try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* 無視 */ }
console.log(`\n${failed === 0 ? '✅ 全テスト PASS' : `❌ ${failed} 件失敗`}`);
process.exitCode = failed === 0 ? 0 : 1;
