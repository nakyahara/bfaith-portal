import { temporaryTestDataDir } from './test-temp-dir.mjs';
await temporaryTestDataDir(import.meta.url, 'test-lp-compose-api-');
/**
 * LP 構成の AI 生成 — 画面 API・service-api・仕様書の取込 (apps/product-hub/router.js・段階1)
 * 実行: node scripts/test-ph-lp-compose-api.mjs
 * 設計 = 正本 AI_reference『商品ハブ_LP構成AI生成_段階1設計_20260930.md』§4.3〜§4.4
 *
 * ここで守りたいのは:
 *   ① 機能フラグが無ければ受け付けない (fail-closed)。service-api はトークンが無ければ 503
 *   ② 押す → claim → 予約 → 結果 を HTTP で通して done になる (実行役がやる手順そのまま)
 *   ③ 商品画像は **その依頼の packet に固定済みのものだけ** 配る (任意の fileId を覗けない)
 *   ④ 仕様書の .xlsx を上げると全タブがテキストになり、同じ中身なら版が増えない
 *   ⑤ 画面 API は done のときだけ本文を返し、押せない理由をそのまま返す
 */
process.env.PH_SERVICE_TOKEN = 'test-token-lp-compose';
process.env.PH_LP_COMPOSE_ENABLED = '1';

const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const dbmod = await import('../apps/product-hub/db.js');
const db = dbmod.initProductHubDB();
const lp = await import('../apps/product-hub/lib/lp-compose.js');
const express = (await import('express')).default;
const { default: router, serviceApiRouter } = await import('../apps/product-hub/router.js');

let pass = 0, fail = 0;
const ok = (c, l) => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}`); } };
const eq = (a, b, l) => ok(JSON.stringify(a) === JSON.stringify(b), `${l} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

// ── 画面側はセッション認証の中。テストでは擬似セッションを差す ──
const app = express();
// 役割はヘッダで切り替えられるようにする (admin 限定の口を確かめるため)
app.use((req, _res, next) => { req.session = { email: 'nakahara@x', role: req.get('X-Test-Role') || 'admin' }; next(); });
app.use('/apps/product-hub', router);
app.use('/apps/product-hub/service-api', serviceApiRouter);
const server = app.listen(0);
await new Promise((r) => server.once('listening', r));
const base = `http://127.0.0.1:${server.address().port}/apps/product-hub`;

const api = async (method, path, { body = null, raw = null, token = null, query = '', lease = null, role = null } = {}) => {
  const headers = {};
  if (token) headers.Authorization = `Bearer ${token}`;
  // 🚨 lease はヘッダ。クエリに載せるとプロキシやアクセスログに URL ごと残る (Codex API R1 #3)
  if (lease) headers['X-LP-Compose-Lease'] = lease;
  if (role) headers['X-Test-Role'] = role;
  let payload;
  if (raw) { headers['Content-Type'] = 'application/octet-stream'; payload = raw; }
  else if (body) { headers['Content-Type'] = 'application/json'; payload = JSON.stringify(body); }
  const res = await fetch(`${base}${path}${query}`, { method, headers, body: payload });
  const ct = res.headers.get('content-type') || '';
  return { status: res.status, json: ct.includes('json') ? await res.json() : null, buf: ct.includes('json') ? null : Buffer.from(await res.arrayBuffer()), ct };
};
const svc = (method, path, opts = {}) => api(method, `/service-api${path}`, { token: process.env.PH_SERVICE_TOKEN, ...opts });

// ── 材料: 商品 + 商品情報 + 商品画像 2 枚 ──
const draftId = Number(db.prepare(
  `INSERT INTO product_drafts (ne_code, name, created_by) VALUES ('LP-API-1', 'ハッカ油スプレー 100ml', 'test')`
).run().lastInsertRowid);
db.prepare(`INSERT INTO draft_image_production (draft_id, product_info_text) VALUES (?, ?)`)
  .run(draftId, '天然ハッカ油 100ml。虫除け・消臭に。');
const insImg = db.prepare(`INSERT INTO draft_images (draft_id, sort, drive_file_id, drive_modified_time) VALUES (?, ?, ?, ?)`);
insImg.run(draftId, 0, 'FILEIDTOP001', '2026-09-30T00:00:00.000Z');
insImg.run(draftId, 1, 'FILEIDIMG002', '2026-09-30T00:00:00.000Z');

console.log('① 仕様書を上げる前は押せない');
const st0 = await api('GET', `/api/drafts/${draftId}/lp-compose`);
eq(st0.status, 200, '状態は取れる');
ok(String(st0.json.blocked || '').includes('仕様書'), '押せない理由が「仕様書がまだ」になる');
eq((await api('POST', `/api/drafts/${draftId}/lp-compose`, { body: { idempotency_key: 'key-0001' } })).status, 409,
  '仕様書が無ければ受け付けない');

console.log('② 仕様書の .xlsx を上げる (実物 3 本のうち LP制作システム)');
const fs = await import('node:fs');
const specPath = 'G:/共有ドライブ/AI_reference/システム設計/LP仕様書_snapshot/LP制作システム.xlsx';
const haveReal = fs.existsSync(specPath);
let xlsx;
if (haveReal) {
  xlsx = fs.readFileSync(specPath);
  console.log('  (実物の .xlsx を使う)');
} else {
  // 共有ドライブが無い PC でも通るように、同じ形の .xlsx を作って使う
  const ExcelJS = (await import('exceljs')).default;
  const wb = new ExcelJS.Workbook();
  const ws1 = wb.addWorksheet('出力形式');
  ws1.addRow(['最終ヘッダー', '必須', '# LP制作システム V2.1']);
  const ws2 = wb.addWorksheet('AIプロンプトV2.2');
  ws2.addRow(['冒頭', '固定', '## ⑦ AI画像生成プロンプト']);
  xlsx = Buffer.from(await wb.xlsx.writeBuffer());
  console.log('  (共有ドライブが無いので代わりの .xlsx を作った)');
}
// 仕様書の差し替えは admin 限定 (生成依頼は誰でも押せるが、ここは別権限・Codex API R1 #1)
eq((await api('POST', '/api/lp-specs', { raw: xlsx, role: 'staff' })).status, 403,
  '🚨 admin でなければ仕様書を差し替えられない (API R1 #1)');
const up1 = await api('POST', '/api/lp-specs', { raw: xlsx, query: '?kind=product_analysis&title=LP制作システム' });
eq(up1.status, 200, '上げられる');
ok(up1.json.created, '新しい版になる');
ok(up1.json.spec.sheet_titles.length >= 2, `全タブが入る (${up1.json.spec.sheet_titles.length} タブ)`);
ok(up1.json.spec.chars > 500, `本文がテキストになる (${up1.json.spec.chars} 文字)`);
if (haveReal) {
  const body = lp.latestSpec(db, 'product_analysis').body;
  ok(body.includes('## 出力形式'), 'タブ名が見出しになる');
  ok(body.includes('取込互換'), '🚨 実物の「取込互換」の項が入っている (lint の根拠)');
  ok(body.includes('0枚目｜サムネイル'), '🚨 V2.2 の 0 枚目サムネイルが入っている');
}
const up2 = await api('POST', '/api/lp-specs', { raw: xlsx, query: '?kind=product_analysis&title=LP制作システム' });
ok(up2.status === 200 && !up2.json.created, '同じ中身を上げ直しても版が増えない');
eq((await api('POST', '/api/lp-specs', { raw: Buffer.from('これは xlsx ではない') })).status, 400, '.xlsx でなければ断る');
eq((await api('POST', '/api/lp-specs', { raw: Buffer.alloc(0) })).status, 400, '空は断る');

console.log('②b zip 爆弾を load の前に弾く (Codex API R1 #2 / R3 #1)');
const { zipExpandedSize, assertXlsxExpandsSafely } = await import('../apps/product-hub/lib/xlsx-guard.js');
const real = zipExpandedSize(xlsx);
ok(real.entries > 0 && real.expandedBytes > 0, `展開せずに中身の大きさが分かる (${real.entries} ファイル / ${Math.round(real.expandedBytes/1024)}KB)`);
ok(real.expandedBytes / xlsx.length < 200, `実物の圧縮比は普通 (${Math.round(real.expandedBytes/xlsx.length)} 倍)`);
ok((() => { try { assertXlsxExpandsSafely(xlsx); return true; } catch { return false; } })(), '実物の仕様書は通る');
// zip 爆弾は exceljs の出力では作れない (同じ文字は共有文字列で重複排除される)。
// ガードは「中央ディレクトリの申告サイズ」を見るので、そこを書き換えて検査する
const bombBuf = Buffer.from(xlsx);
let cdh = -1;
for (let i = 0; i + 4 <= bombBuf.length; i++) {
  if (bombBuf.readUInt32LE(i) === 0x02014b50) { cdh = i; break; }
}
ok(cdh >= 0, '中央ディレクトリが見つかる');
bombBuf.writeUInt32LE(900 * 1024 * 1024, cdh + 24);   // 展開後 900MB と申告する
const expect = (fn) => { try { fn(); return null; } catch (e) { return e; } };
const bombCaught = expect(() => assertXlsxExpandsSafely(bombBuf));
ok(bombCaught && bombCaught.tooLarge,
  `🚨 展開後が大きい申告は load の前に弾く (${bombCaught && bombCaught.message})`);
const ratioCaught = expect(() => assertXlsxExpandsSafely(xlsx, { maxRatio: 2 }));
ok(ratioCaught && ratioCaught.tooLarge, `🚨 圧縮比でも弾ける (${ratioCaught && ratioCaught.message})`);
ok(expect(() => assertXlsxExpandsSafely(xlsx, { maxEntries: 2 }))?.tooLarge, 'ファイル数でも弾ける');
// 🚨 件数の過少申告で迂回できない (entries=1 と書いて 100 個置く手・Codex API R4 #1)
const lieBuf = Buffer.from(xlsx);
let eocd = -1;
for (let i = lieBuf.length - 22; i >= 0; i--) {
  if (lieBuf.readUInt32LE(i) === 0x06054b50) { eocd = i; break; }
}
ok(eocd >= 0, 'EOCD が見つかる');
lieBuf.writeUInt16LE(1, eocd + 10);   // 「1 ファイルだけ」と嘘をつく
const lieCaught = expect(() => zipExpandedSize(lieBuf));
ok(lieCaught && /件数の申告が合いません/.test(lieCaught.message),
  `🚨 件数の過少申告を弾く (${lieCaught && lieCaught.message})`);
const ExcelJS2 = (await import('exceljs')).default;
ok((() => { try { zipExpandedSize(Buffer.from('zip でない')); return false; } catch { return true; } })(),
  'ZIP でなければ普通の Error (= 400)');
// セル数・文字数の超過は 413 (「読めない」400 と区別する)
const bigger = new ExcelJS2.Workbook();
const gws = bigger.addWorksheet('big');
gws.addRow(['y'.repeat(600000)]);
const r413 = await api('POST', '/api/lp-specs', { raw: Buffer.from(await bigger.xlsx.writeBuffer()) });
eq(r413.status, 413, '🚨 大きすぎるは 413 (bad_xlsx の 400 と区別する・R3 #3)');


console.log('③ 押す → claim → 予約 → 結果 (実行役の手順そのまま)');
const req1 = await api('POST', `/api/drafts/${draftId}/lp-compose`, { body: { idempotency_key: 'key-0001' } });
eq(req1.status, 200, '押せる');
eq(req1.json.job.status, 'queued', 'キューに入る');
const req1b = await api('POST', `/api/drafts/${draftId}/lp-compose`, { body: { idempotency_key: 'key-0001' } });
ok(req1b.status === 200 && !req1b.json.created, '二重クリックで増えない');
eq((await api('POST', `/api/drafts/${draftId}/lp-compose`, { body: { idempotency_key: 'key-0002' } })).status, 409,
  '動いている間は 2 件目を受け付けない');
eq((await api('POST', `/api/drafts/${draftId}/lp-compose`, { body: { idempotency_key: 'x' } })).status, 400,
  'idempotency_key の形が不正なら断る');

eq((await api('POST', '/service-api/lp-compose/claim', { body: { runner_run_id: 'r1' } })).status, 401,
  '🚨 service-api はトークンが無ければ 401 (env 未設定なら 503)');
const cl = await svc('POST', '/lp-compose/claim', { body: { runner_run_id: 'run-1' } });
eq(cl.status, 200, 'claim できる');
const job = cl.json.job;
ok(job && job.job_id === req1.json.job.id, '押した依頼が来る');
ok(job.spec.body.length > 500, '仕様書の全文が付いてくる');
eq(job.packet.images.length, 2, '商品画像 2 枚が packet に固定されている');
ok(job.packet.product_info.includes('ハッカ油'), '商品情報が入っている');
ok(job.packet.color_variations.includes('カラバリ'), 'カラバリが入っている (定型文と同じ組み立て)');
eq(job.prompt_version, lp.PROMPT_VERSION, 'prompt の版が来る');
ok((await svc('POST', '/lp-compose/claim', { body: { runner_run_id: 'run-2' } })).json.job === null,
  '同じ依頼を 2 つの実行役が掴まない');

console.log('④ 商品画像は packet に固定済みのものだけ');
const img404 = await svc('GET', `/lp-compose/jobs/${job.job_id}/images/3`, { lease: job.lease_token });
eq(img404.status, 404, '渡していない番号は 404');
const imgNoLease = await svc('GET', `/lp-compose/jobs/${job.job_id}/images/1`, { lease: 'wrong-lease-token' });
eq(imgNoLease.status, 409, '🚨 lease が違えば配らない');
// Drive を叩くので本番の画像は取れない。「lease が通って Drive まで行った」ことだけ確かめる
const img1 = await svc('GET', `/lp-compose/jobs/${job.job_id}/images/1`, { lease: job.lease_token });
ok([200, 403, 404, 502].includes(img1.status), `lease が通れば Drive まで行く (status ${img1.status})`);

console.log('④b 壊れた URL で別の依頼を掴まない (codex exec review の P2)');
// 共有の intParam は Number.parseInt なので "12abc" を 12 にしていた。
// lib の posInt を厳しくしても router で変換して渡すとそこに届かず、
// 壊れた URL が別の正当な job / generation を指してしまっていた
for (const bad of [`${job.job_id}abc`, ` ${job.job_id}`, `${job.job_id}.0`, '0']) {
  const r = await svc('POST', `/lp-compose/jobs/${encodeURIComponent(bad)}/reserve`, {
    body: { lease_token: job.lease_token, model: 'claude-opus-5', prompt_version: lp.PROMPT_VERSION },
  });
  ok(r.status === 404 || r.status === 400, `🚨 "${bad}" は別の依頼にならない (status ${r.status})`);
}
eq((await svc('GET', `/lp-compose/jobs/${encodeURIComponent(job.job_id + 'abc')}/images/1`, { lease: job.lease_token })).status,
  404, '🚨 画像の口でも同じ');
eq((await svc('GET', `/lp-compose/jobs/${job.job_id}/images/1abc`, { lease: job.lease_token })).status,
  400, '🚨 画像の番号でも同じ');

console.log('⑤ 予約 → 結果');
eq((await svc('POST', `/lp-compose/jobs/${job.job_id}/reserve`, {
  body: { lease_token: job.lease_token, model: 'claude-opus-5', prompt_version: 'ふるい版' },
})).status, 400, '🚨 prompt の版が違えば AI を呼ぶ前に断る');
const rv = await svc('POST', `/lp-compose/jobs/${job.job_id}/reserve`, {
  body: { lease_token: job.lease_token, model: 'claude-opus-5', prompt_version: lp.PROMPT_VERSION },
});
eq(rv.status, 200, '予約できる');
eq((await svc('POST', `/lp-compose/jobs/${job.job_id}/fail`, { body: { lease_token: job.lease_token, code: 'x' } })).status, 409,
  '🚨 予約後に fail は使えない');
const OUT = '# LP制作システム V2.1\n\n## ⑦ AI画像生成プロンプト\n\n### AI画像生成プロンプト 出力テンプレート V2.2\n…';
// 🚨 証跡は実行役から受け取らない。サーバが画像を配ったときの記録を使う (codex exec review P1)。
//    Drive が無い環境では配れないので、ここでは記録だけ直接作って通す
eq((await svc('POST', `/lp-compose/generations/${rv.json.generation_id}/result`, {
  body: { packet_hash: job.packet_hash, verdict: 'accepted', output: OUT, review_rounds: 1, lint: { ok: true } },
})).status, 400, '🚨 サーバが画像を配っていなければ accepted を受け取らない');
eq((await svc('POST', `/lp-compose/generations/${rv.json.generation_id}/result`, {
  body: { packet_hash: job.packet_hash, verdict: 'accepted', output: OUT, review_rounds: 1 },
})).status, 400, '🚨 lint が無ければ accepted を受け取らない');
lp.recordImageServed(db, job.job_id, { fileId: 'FILEIDTOP001', sha256: 'c'.repeat(64), bytes: 2222 });
const sub = await svc('POST', `/lp-compose/generations/${rv.json.generation_id}/result`, {
  body: {
    packet_hash: job.packet_hash, verdict: 'accepted', output: OUT, review_rounds: 1,
    lint: { ok: true },
  },
});
eq(sub.status, 200, '結果を受け取れる');
eq(sub.json.receipt.images[0].sha256, 'c'.repeat(64), '🚨 証跡はサーバの記録');
eq(sub.json.status, 'done', 'done になる');
eq((await svc('POST', `/lp-compose/generations/${rv.json.generation_id}/result`, {
  body: { packet_hash: job.packet_hash, verdict: 'accepted', output: OUT, review_rounds: 1, lint: { ok: true } },
})).json.already, true, '同じ結果の再送は保存済みを返す');

console.log('⑥ 画面に出る');
const st1 = await api('GET', `/api/drafts/${draftId}/lp-compose`);
eq(st1.json.job.status, 'done', 'done が見える');
eq(st1.json.job.output_text, OUT, '本文が返る (コピーできる)');
eq(st1.json.job.within_deadline, true, '3 分以内だったか');
eq(st1.json.blocked, null, '次も押せる');
ok(st1.json.spec && st1.json.spec.imported_at, '「仕様書: ○○ (取込日)」が返る');

console.log('⑦ 機能フラグ (fail-closed)');
process.env.PH_LP_COMPOSE_ENABLED = '0';
eq((await api('POST', `/api/drafts/${draftId}/lp-compose`, { body: { idempotency_key: 'key-0003' } })).status, 503,
  'フラグが無ければ受け付けない');
eq((await svc('POST', '/lp-compose/claim', { body: { runner_run_id: 'run-3' } })).status, 503,
  'フラグが無ければ claim もしない');
process.env.PH_LP_COMPOSE_ENABLED = '1';

server.close();
console.log(`\n${fail === 0 ? '✅' : '❌'} ${pass} 件成功 / ${fail} 件失敗`);
process.exit(fail === 0 ? 0 : 1);
