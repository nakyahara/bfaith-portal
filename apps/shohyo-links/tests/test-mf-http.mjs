/**
 * shohyo-links MF照合画面のHTTPスモーク (ルーティング・リダイレクト先・入力検証)
 * MF本体には接続しない (未接続エラーまでを確認する)。実行: node apps/shohyo-links/tests/test-mf-http.mjs
 */
import express from 'express';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'shohyo-links-http-'));
const { default: router } = await import('../router.js');

const app = express();
app.use((req, _res, next) => { req.session = {}; next(); });
app.use('/apps/shohyo-links', express.json({ limit: '8mb' }), router);
const server = app.listen(0);
await new Promise(r => server.once('listening', r));
const base = `http://127.0.0.1:${server.address().port}`;

let ng = 0;
const check = (name, cond, extra = '') => {
  console.log((cond ? 'OK  ' : 'NG  ') + name + (cond ? '' : ' ' + extra));
  if (!cond) ng++;
};

// /mf は200・/mf/ は308で /mf へ寄せる
let r = await fetch(base + '/apps/shohyo-links/mf', { redirect: 'manual' });
check('/mf は200', r.status === 200, String(r.status));
r = await fetch(base + '/apps/shohyo-links/mf/?x=1', { redirect: 'manual' });
check('/mf/ は308で /mf?x=1 へ', r.status === 308 && r.headers.get('location') === '/apps/shohyo-links/mf?x=1',
  `${r.status} ${r.headers.get('location')}`);

// コールバックの戻り先が絶対パス (相対だと /mf/mf になる)
r = await fetch(base + '/apps/shohyo-links/mf/callback?error=access_denied', { redirect: 'manual' });
check('callback(error) は /apps/shohyo-links/mf?error=... へ',
  r.headers.get('location') === '/apps/shohyo-links/mf?error=access_denied', String(r.headers.get('location')));
r = await fetch(base + '/apps/shohyo-links/mf/callback?code=x&state=y', { redirect: 'manual' });
check('callback(state不一致) も絶対パスで戻る',
  r.headers.get('location') === '/apps/shohyo-links/mf?error=state_mismatch', String(r.headers.get('location')));

// 添付のバリデーション
const post = (body) => fetch(base + '/apps/shohyo-links/api/mf/attach', {
  method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body),
});
r = await post({});
check('ファイル無しは400 file_required', r.status === 400 && (await r.json()).error === 'file_required');
r = await post({ journal_id: 'j', file_name: 'a'.repeat(256) + '.pdf', file_data: 'AAAA' });
check('名称256文字超は400', r.status === 400 && (await r.json()).error === 'file_name_too_long');
r = await post({ journal_id: 'j', file_name: 'a.pdf', file_data: 'A'.repeat(Math.ceil(5 * 1024 * 1024 * 4 / 3) + 100) });
check('5MB超は413', r.status === 413 && (await r.json()).error === 'file_too_large_5mb');
r = await post({ journal_id: 'j', file_name: 'a.pdf', file_data: 'QUJD' });
check('制限内は未接続エラーまで進む (mf_not_connected)', (await r.json()).error === 'mf_not_connected');

// 期間バリデーションと未接続
r = await fetch(base + '/apps/shohyo-links/api/mf/unattached?start=2026-8-1&end=2026-08-31');
check('不正な期間は400 bad_period', r.status === 400 && (await r.json()).error === 'bad_period');
r = await fetch(base + '/apps/shohyo-links/api/mf/unattached?start=2026-08-01&end=2026-08-31');
check('未接続は401', r.status === 401 && (await r.json()).error === 'mf_not_connected');

// 明細ビュー
r = await fetch(base + '/apps/shohyo-links/mf/transactions', { redirect: 'manual' });
check('/mf/transactions は200', r.status === 200, String(r.status));
r = await fetch(base + '/apps/shohyo-links/mf/transactions/', { redirect: 'manual' });
check('/mf/transactions/ は308で寄せる', r.status === 308 && r.headers.get('location') === '/apps/shohyo-links/mf/transactions', String(r.headers.get('location')));
r = await fetch(base + '/apps/shohyo-links/api/mf/transactions?start=2026-8-1&end=2026-08-31');
check('明細API 不正な期間は400', r.status === 400 && (await r.json()).error === 'bad_period');
r = await fetch(base + '/apps/shohyo-links/api/mf/transactions?start=2026-08-01&end=2026-08-31');
check('明細API 未接続は401', r.status === 401 && (await r.json()).error === 'mf_not_connected');

server.close();
console.log(ng ? `\n${ng}件NG` : '\n全件パス');
process.exitCode = ng ? 1 : 0;
