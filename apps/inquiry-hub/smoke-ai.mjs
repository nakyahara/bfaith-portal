// AI返信案基盤 (ai-jobs.js + ai-api.js) のスモーク
// 使い方: DATA_DIR=<作業ディレクトリ> node apps/inquiry-hub/smoke-ai.mjs
import fs from 'fs';
import path from 'path';
import express from 'express';

if (!process.env.DATA_DIR) {
  console.error('FATAL: DATA_DIR が未指定です (例: DATA_DIR=c:/tmp/ih-ai-smoke)');
  process.exit(2);
}
const baseDir = process.env.DATA_DIR;
fs.mkdirSync(baseDir, { recursive: true });
const workDir = fs.mkdtempSync(path.join(baseDir, 'smoke-ai-'));
process.env.DATA_DIR = workDir;
process.env.INQUIRY_HUB_AI_KEY = 'test-ai-key';

const { initInquiryHubDB, getDB, toUtcIso } = await import('./db.js');
const { maskPii, enqueueAiJobs, listAiQueue, claimAiJobs, submitAiResult, failAiJob, requeueExpiredAiJobs, validateDraftOutput } = await import('./ai-jobs.js');
const aiApi = (await import('./ai-api.js')).default;

initInquiryHubDB();
const db = getDB();

let passed = 0, failed = 0;
const check = (name, cond, detail) => {
  if (cond) { passed++; console.log(`  PASS ${name}`); }
  else { failed++; console.error(`  FAIL ${name}${detail ? ' — ' + detail : ''}`); }
};

// ─── fixture ───
const shop = db.prepare("INSERT INTO shops (channel_type, shop_name, account_identifier) VALUES ('email','メール','info@b-faith.biz')").run().lastInsertRowid;
const now = Date.now();
const mkInq = (ext, { status = 'open', lastIncoming = true, rev = 1, ai = 0, daysAgo = 1 } = {}) => {
  const at = toUtcIso(now - daysAgo * 86400000);
  const id = db.prepare(`INSERT INTO inquiries (channel_type, shop_id, external_inquiry_id, subject, customer_name, internal_status, ai_needed, conversation_rev, received_at, last_message_at)
    VALUES ('email', ?, ?, '在庫はありますか', '顧客A', ?, ?, ?, ?, ?)`).run(shop, ext, status, ai, rev, at, at).lastInsertRowid;
  db.prepare(`INSERT INTO inquiry_messages (inquiry_id, external_message_id, sender_type, message_body_text, is_incoming, received_at)
    VALUES (?, ?, 'customer', '在庫ありますか? 電話は090-1234-5678です。〒530-0001 メールはtaro@example.comへ', 1, ?)`).run(id, ext + '-m1', at);
  if (!lastIncoming) {
    db.prepare(`INSERT INTO inquiry_messages (inquiry_id, external_message_id, sender_type, message_body_text, is_incoming, received_at)
      VALUES (?, ?, 'shop', '在庫ございます', 0, ?)`).run(id, ext + '-m2', toUtcIso(now - daysAgo * 86400000 + 60000));
  }
  return id;
};

// ─── 1. PIIマスク ───
console.log('1. PIIマスク');
{
  const m = maskPii('電話090-1234-5678、固定06-6123-4567、〒530-0001、taro@example.com、注文373343-20260617-49607914o');
  check('電話番号マスク', !m.includes('090-1234-5678') && !m.includes('06-6123-4567') && m.includes('[電話番号]'));
  check('郵便番号マスク', !m.includes('530-0001') && m.includes('[郵便番号]'));
  check('メールマスク', !m.includes('taro@example.com') && m.includes('[メールアドレス]'));
  check('注文番号は保持', m.includes('373343-20260617-49607914o'));
}

// ─── 2. 自動投入 (enqueue) ───
console.log('2. 自動投入');
const iTarget = mkInq('ai-1', { ai: 1 });
const iReplied = mkInq('ai-2', { lastIncoming: false });
const iDone = mkInq('ai-3', { status: 'done' });
const iOld = mkInq('ai-4', { daysAgo: 20 });
{
  const n = enqueueAiJobs();
  check('対象=返信待ちの1件のみ投入 (店舗返信済/done/14日超は除外)', n === 1
    && db.prepare('SELECT COUNT(*) c FROM ai_jobs').get().c === 1
    && db.prepare('SELECT inquiry_id FROM ai_jobs').get().inquiry_id === iTarget);
  check('再sweepは重複投入しない', enqueueAiJobs() === 0);
}

// ─── 3. claim → result (正常系) ───
console.log('3. claim→result');
{
  const q = listAiQueue();
  check('queue一覧に出る', q.length === 1 && q[0].inquiry_id === iTarget);
  const { jobs, qa } = claimAiJobs([q[0].id]);
  check('claim: payload+lease+inputRev', jobs.length === 1 && jobs[0].inquiryId === iTarget && !!jobs[0].leaseToken && jobs[0].inputRev === 1);
  check('claim: 本文はPIIマスク済み', jobs[0].messages[0].body.includes('[電話番号]') && !jobs[0].messages[0].body.includes('090-1234-5678'));
  check('claim: Q&A配列が付く', Array.isArray(qa));
  check('二重claim不可', claimAiJobs([q[0].id]).jobs.length === 0);

  const r = submitAiResult({ jobId: jobs[0].jobId, leaseToken: jobs[0].leaseToken, inputRev: jobs[0].inputRev,
    summary: '在庫確認', category: '在庫', draftBody: '顧客A様\nお問い合わせありがとうございます。在庫を確認してご案内いたします。', notes: '', confirmationItems: '在庫数の確認' });
  check('result: done', r.outcome === 'done');
  const d = db.prepare('SELECT * FROM ai_drafts WHERE inquiry_id = ?').get(iTarget);
  check('ai_drafts保存+job done', d.draft_body.includes('在庫を確認') && db.prepare('SELECT status FROM ai_jobs WHERE inquiry_id = ?').get(iTarget).status === 'done');
  check('done後は再投入されない (未staleドラフトあり)', enqueueAiJobs() === 0);
}

// ─── 4. rev競合=破棄 / lease失効 / 出力検証 ───
console.log('4. 安全系');
{
  // rev競合: claim後に新着 → result破棄+queuedへ
  db.prepare('UPDATE ai_drafts SET is_stale = 1').run(); // 前段のドラフトをstale化して再投入対象に
  enqueueAiJobs();
  const q = listAiQueue();
  const { jobs } = claimAiJobs([q[0].id]);
  db.prepare('UPDATE inquiries SET conversation_rev = conversation_rev + 1 WHERE id = ?').run(iTarget); // 新着相当
  const r = submitAiResult({ jobId: jobs[0].jobId, leaseToken: jobs[0].leaseToken, inputRev: jobs[0].inputRev, draftBody: 'x' });
  check('rev競合: discarded+queuedへ戻る', r.outcome === 'discarded'
    && db.prepare('SELECT status FROM ai_jobs WHERE id = ?').get(jobs[0].jobId).status === 'queued');

  // lease失効の自動回収
  const { jobs: jobs2 } = claimAiJobs([jobs[0].jobId]);
  db.prepare("UPDATE ai_jobs SET lease_until = '2020-01-01T00:00:00Z' WHERE id = ?").run(jobs[0].jobId);
  check('期限切れprocessingはqueuedへ回収', requeueExpiredAiJobs() === 1
    && db.prepare('SELECT status FROM ai_jobs WHERE id = ?').get(jobs[0].jobId).status === 'queued');
  // 回収後の古いlease tokenでのresultは拒否
  const rStale = submitAiResult({ jobId: jobs2[0].jobId, leaseToken: jobs2[0].leaseToken, inputRev: jobs2[0].inputRev, draftBody: 'x' });
  check('回収後の古いleaseは not_found', rStale.outcome === 'not_found');

  // 出力検証
  check('URL混入は拒否', validateDraftOutput({ draftBody: '詳細は https://evil.example.com を見てください' }) !== null);
  check('許可ドメインURLはOK', validateDraftOutput({ draftBody: 'ストア https://shopping.yahoo.co.jp/ です' }) === null);
  check('許可外メール混入は拒否', validateDraftOutput({ draftBody: 'ご連絡は evil@phish.example まで' }) !== null);
  check('info@b-faith.bizはOK', validateDraftOutput({ draftBody: 'ご連絡は info@b-faith.biz まで' }) === null);
  check('空本文は拒否', validateDraftOutput({ draftBody: '  ' }) !== null);
  check('5000字超は拒否', validateDraftOutput({ draftBody: 'あ'.repeat(5001) }) !== null);

  // 検証NGのresultはfailed
  const { jobs: jobs3 } = claimAiJobs([jobs[0].jobId]);
  const rBad = submitAiResult({ jobId: jobs3[0].jobId, leaseToken: jobs3[0].leaseToken, inputRev: jobs3[0].inputRev,
    draftBody: 'こちらへ https://evil.example.com' });
  check('検証NG: rejected+failed', rBad.outcome === 'rejected'
    && db.prepare('SELECT status FROM ai_jobs WHERE id = ?').get(jobs[0].jobId).status === 'failed');
}

// ─── 5. HTTP (X-AI-Key認証) ───
console.log('5. HTTP');
{
  const app = express();
  app.use('/ai-api', express.json({ limit: '1mb' }), aiApi);
  const server = await new Promise(r => { const s = app.listen(0, '127.0.0.1', () => r(s)); });
  const base = `http://127.0.0.1:${server.address().port}/ai-api`;
  const call = (method, p, body, key) => fetch(base + p, {
    method, headers: { ...(key ? { 'X-AI-Key': key } : {}), ...(body ? { 'Content-Type': 'application/json' } : {}) },
    body: body ? JSON.stringify(body) : undefined,
  });

  check('キー無しは401', (await call('GET', '/queue')).status === 401);
  check('誤キーは401', (await call('GET', '/queue', null, 'wrong')).status === 401);
  delete process.env.INQUIRY_HUB_AI_KEY;
  check('キー未設定はfail-closed 503', (await call('GET', '/queue', null, 'test-ai-key')).status === 503);
  process.env.INQUIRY_HUB_AI_KEY = 'test-ai-key';

  // 正常フロー: queue→claim→result
  db.prepare("UPDATE ai_jobs SET status = 'queued', lease_token = NULL WHERE 1").run();
  const q = await (await call('GET', '/queue', null, 'test-ai-key')).json();
  check('GET /queue', Array.isArray(q.jobs) && q.jobs.length >= 1);
  const c = await (await call('POST', '/claim', { job_ids: [q.jobs[0].id] }, 'test-ai-key')).json();
  check('POST /claim', c.jobs?.length === 1 && !!c.jobs[0].leaseToken);
  const rr = await (await call('POST', '/result', {
    job_id: c.jobs[0].jobId, lease_token: c.jobs[0].leaseToken, input_rev: c.jobs[0].inputRev,
    summary: '要約', draft_body: '顧客A様\nご案内いたします。',
  }, 'test-ai-key')).json();
  check('POST /result → done', rr.outcome === 'done');
  const rl = await call('POST', '/run-log', { runner_info: 'smoke/claude-p', claimed: 1, done: 1, failed: 0, discarded: 0 }, 'test-ai-key');
  check('POST /run-log', rl.status === 200 && db.prepare('SELECT COUNT(*) c FROM ai_runs').get().c === 1);
  const rf = await call('POST', '/fail', { job_id: 99999, lease_token: 'x', error: 'e' }, 'test-ai-key');
  check('POST /fail (対象なし) は404', rf.status === 404);

  server.close();
}

// ─── 6. 旧スキーマDBからの移行 (本番相当。Step 1の旧ai_runsが残っている状態でinitして新カラムが揃うか) ───
console.log('6. 旧ai_runs移行');
{
  const { execFileSync } = await import('child_process');
  const migDir = fs.mkdtempSync(path.join(baseDir, 'smoke-ai-mig-'));
  const script = `
    import Database from 'better-sqlite3';
    import path from 'path';
    const p = path.join(process.env.DATA_DIR, 'inquiry-hub.db');
    // 旧Step 1スキーマの ai_runs を先に作る (本番DBの状態を再現)
    const pre = new Database(p);
    pre.exec("CREATE TABLE ai_runs (id INTEGER PRIMARY KEY AUTOINCREMENT, started_at TEXT NOT NULL, completed_at TEXT, processed_count INTEGER NOT NULL DEFAULT 0, failed_count INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'running', detail TEXT)");
    pre.close();
    const { initInquiryHubDB, getDB } = await import('./apps/inquiry-hub/db.js');
    initInquiryHubDB();
    const db = getDB();
    const cols = db.prepare('PRAGMA table_info(ai_runs)').all().map(c => c.name);
    db.prepare('INSERT INTO ai_runs (runner_info, started_at, claimed, done, failed, discarded) VALUES (?,?,?,?,?,?)')
      .run('mig-test', new Date().toISOString(), 1, 1, 0, 0);
    console.log(JSON.stringify({ cols, rows: db.prepare('SELECT COUNT(*) c FROM ai_runs').get().c }));
  `;
  const out = execFileSync(process.execPath, ['--input-type=module', '-e', script], {
    cwd: process.cwd(), env: { ...process.env, DATA_DIR: migDir }, encoding: 'utf8',
  });
  const mig = JSON.parse(out.trim().split('\n').pop());
  check('旧スキーマ検出→新スキーマへ移行 (runner_info等が揃う)', mig.cols.includes('runner_info') && mig.cols.includes('claimed') && mig.cols.includes('discarded'));
  check('移行後にrun-log相当のINSERTが通る', mig.rows === 1);
  fs.rmSync(migDir, { recursive: true, force: true });
}

check('DBは一時サブディレクトリのみに作成', fs.existsSync(path.join(workDir, 'inquiry-hub.db')) && !fs.existsSync(path.join(baseDir, 'inquiry-hub.db')));

console.log(`\n${passed} PASS / ${failed} FAIL`);
db.close();
fs.rmSync(workDir, { recursive: true, force: true });
process.exitCode = failed ? 1 : 0;
