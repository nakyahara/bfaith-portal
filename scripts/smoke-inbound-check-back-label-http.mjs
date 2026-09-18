/**
 * 🆕 裏面ラベル写真 — HTTP スモーク (server.js を子プロセスで起動し、実際の経路で叩く)
 *
 * 単体テスト (test-inbound-check-back-label.mjs) では通れない道をここで見る:
 *   ・確認 API が「新商品 + 未撮影」を **400 destination_required / missing=[back_label]** で止めるか
 *   ・写真を multipart で受け取れるか (multer の上限・種類の検査を含む)
 *   ・撮った直後に同じ確認が通るか / 消すとまた止まるか
 *   ・認証境界 (未認証では撮れない・見られない) と Origin チェック
 *
 * 実行: node scripts/smoke-inbound-check-back-label-http.mjs
 */
import fs from 'fs';
import path from 'path';
import { spawn } from 'child_process';
import { fileURLToPath } from 'url';
import { temporaryTestDataDir } from './test-temp-dir.mjs';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const PORT = 3463;
const BASE = `http://127.0.0.1:${PORT}`;
const APP = `${BASE}/apps/inbound-check`;
const DATA_DIR = await temporaryTestDataDir(import.meta.url, 'ic-bl-http-');

let pass = 0, fail = 0;
const ok = (c, l) => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}`); } };

// ─── 起動前の種まき (起動後に別プロセスから書くと SQLITE_BUSY を招く) ───
process.env.DATA_DIR = DATA_DIR;
{
  const { initMirrorDB, getMirrorDB } = await import('../apps/warehouse-mirror/db.js');
  initMirrorDB();
  const m = getMirrorDB();
  // inbound-check のテーブルを作る (初回利用時の冪等作成をここで走らせる)
  const { createTables } = await import('../apps/inbound-check/db.js');
  createTables(m);
  const now = new Date().toISOString();
  const today = new Date(Date.now() + 9 * 3600 * 1000).toISOString().slice(0, 10);
  const old = new Date(Date.now() - 300 * 86400000).toISOString().slice(0, 10);

  // 商品マスタ: 新商品 (今日登録・入庫履歴なし) と 既存品
  const insProd = m.prepare(`INSERT INTO mirror_products
    (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, new_product_launch_date, updated_at)
    VALUES (?,?,?,'単品','取扱中','unknown',?,?)`);
  insProd.run(910001, 'BL-NEW', '新商品 (裏面テスト)', today, now);
  insProd.run(910002, 'BL-OLD', '既存品 (裏面テスト)', old, now);
  // 現物が1つも来なかった新商品 (「これ以上来ない — 不足◯個」で閉じる。撮る実物が無い)
  insProd.run(910003, 'BL-NONE', '新商品 (1つも来なかった)', today, now);
  // 入庫情報を入れて行き先を確定させる = ゲートに出るのが裏面ラベルだけになる
  const insInfo = m.prepare(`INSERT INTO f_inbound_info
    (code_key, 商品コード, 商品名, 入数, 入庫時BCシール貼りフラグ, いろは在庫化作業有無, source, created_at, updated_at)
    VALUES (?, ?, ?, 1, '不要', '無し', 'manual', ?, ?)`);
  insInfo.run('bl-new', 'BL-NEW', '新商品 (裏面テスト)', now, now);
  insInfo.run('bl-old', 'BL-OLD', '既存品 (裏面テスト)', now, now);
  insInfo.run('bl-none', 'BL-NONE', '新商品 (1つも来なかった)', now, now);

  // 入荷受付伝票 (active バッチ) を1つ作る
  m.prepare(`INSERT INTO f_inbound_check_batches
    (id, source, file_name, file_hash, csv_generated_at, row_count, slip_count, imported_at, status, work_date)
    VALUES (900, 'manual_upload', 'bl.csv', 'bl-hash', ?, 3, 1, ?, 'active', date('now','+9 hours'))`).run(now, now);
  m.prepare(`INSERT INTO f_inbound_check_slips (batch_id, ar_no, line_count, seq) VALUES (900, 'AR900', 3, 1)`).run();
  const insLine = m.prepare(`INSERT INTO f_inbound_check_lines
    (batch_id, line_key, ar_no, line_no, detail_no, product_id, code_key, product_name, planned_qty, seq)
    VALUES (900, ?, 'AR900', ?, 1, ?, ?, ?, 3, ?)`);
  insLine.run('BL1', 1, 'BL-NEW', 'bl-new', '新商品 (裏面テスト)', 1);
  insLine.run('BL2', 2, 'BL-OLD', 'bl-old', '既存品 (裏面テスト)', 2);
  insLine.run('BL3', 3, 'BL-NONE', 'bl-none', '新商品 (1つも来なかった)', 3);
  const insState = m.prepare(`INSERT INTO f_inbound_check_line_state (batch_id, line_key, status) VALUES (900, ?, 'unchecked')`);
  insState.run('BL1'); insState.run('BL2'); insState.run('BL3');
  m.close();
}

const child = spawn(process.execPath, ['server.js'], {
  cwd: ROOT,
  env: {
    ...process.env, DATA_DIR, PORT: String(PORT), PORTAL_PASS: 'smoke', NODE_ENV: 'development',
    SESSION_SECRET: 'smoke-secret', INBOUND_INFO_SYNC_ENABLED: 'false',
    // Drive は未設定のまま = 写真はサーバーに残り、キューは送りに行かない (現場は止まらない)
    GOOGLE_SERVICE_ACCOUNT_KEY: '',
  },
  stdio: ['ignore', 'pipe', 'pipe'],
});
let logs = '';
child.stdout.on('data', (d) => { logs += d; });
child.stderr.on('data', (d) => { logs += d; });

async function waitUp() {
  for (let i = 0; i < 120; i++) {
    try { const r = await fetch(`${BASE}/login`); if (r.status === 200) return true; } catch { /* まだ */ }
    await new Promise((r) => setTimeout(r, 500));
  }
  return false;
}

function jar() {
  const store = new Map();
  return {
    absorb(res) {
      for (const c of (res.headers.getSetCookie ? res.headers.getSetCookie() : [])) {
        const [kv] = c.split(';');
        const i = kv.indexOf('=');
        const k = kv.slice(0, i).trim(), v = kv.slice(i + 1).trim();
        if (v === '') store.delete(k); else store.set(k, v);
      }
    },
    header() { return [...store.entries()].map(([k, v]) => `${k}=${v}`).join('; '); },
  };
}
async function req(j, url, { method = 'GET', body, headers = {}, form, multipart } = {}) {
  const h = { 'x-forwarded-proto': 'https', origin: BASE, ...headers };
  if (j) h.cookie = j.header();
  let payload;
  if (multipart) payload = multipart;
  else if (form) { h['content-type'] = 'application/x-www-form-urlencoded'; payload = new URLSearchParams(form).toString(); }
  else if (body !== undefined) { h['content-type'] = 'application/json'; payload = JSON.stringify(body); }
  const r = await fetch(url, { method, headers: h, body: payload, redirect: 'manual' });
  if (j) j.absorb(r);
  const buf = Buffer.from(await r.arrayBuffer());
  let json = null; try { json = JSON.parse(buf.toString('utf8')); } catch { /* 画像など */ }
  return { status: r.status, buf, json, ctype: r.headers.get('content-type') || '' };
}

/** 中身が本物の JPEG (先頭バイトで判定されるため) */
const jpeg = (size = 4096) => Buffer.concat([Buffer.from([0xFF, 0xD8, 0xFF, 0xE0]), Buffer.alloc(size, 9)]);
const shot = (opId, { __file = null, ...extra } = {}) => {
  const fd = new FormData();
  fd.append('operation_id', opId);
  fd.append('product_code', 'BL-NEW');   // ← 紐づけの主キー (撮った時点の商品)
  fd.append('batch_id', '900');
  fd.append('line_key', 'BL1');
  for (const [k, v] of Object.entries(extra)) fd.set(k, v);
  fd.append('file', new Blob([__file || jpeg()]), 'back.jpg');
  return fd;
};

try {
  console.log('DATA_DIR =', DATA_DIR);
  if (!await waitUp()) throw new Error('server did not start\n' + logs.slice(-3000));

  console.log('\n[A] 未認証では撮れない・見られない');
  let r = await req(null, `${APP}/api/back-label`, { method: 'POST', multipart: shot('op-unauth01') });
  ok(r.status === 401 && r.json?.error === 'unauthorized', '未認証の撮影は 401');
  r = await req(null, `${APP}/api/back-label/1/file`);
  ok(r.status === 401, '未認証では写真も見られない');

  console.log('\n[B] ログインして一覧を見る');
  const J = jar();
  r = await req(J, `${BASE}/login`, { method: 'POST', form: { email: 'd.nakahara@b-faith.biz', password: 'smoke' } });
  ok(r.status === 302, 'ログイン成功');
  r = await req(J, `${APP}/api/state`);
  const lineOf = (k) => r.json.lines.find((l) => l.line_key === k);
  ok(r.status === 200 && r.json.lines.length === 3, '3行の一覧が出る');
  ok(lineOf('BL1').new_product.verdict === 'new' && lineOf('BL1').back_label_required === true, '新商品の行に「撮るまで確認できない」印が付く');
  ok(lineOf('BL1').dest.missing.includes('back_label'), '確認の前に聞く項目に back_label が入る');
  ok(lineOf('BL2').new_product.verdict === 'not_new' && !lineOf('BL2').dest.missing.includes('back_label'), '既存品は求めない');

  console.log('\n[C] 撮るまで確認できない');
  const check = (lineKey) => req(J, `${APP}/api/lines/check`, { method: 'POST', body: {
    batch_id: 900, line_key: lineKey, expect_version: 1, expect_quantity_version: 1,
    result: 'exact', mode: 'fill_remaining', fill_event: { client_event_id: `bl-fill-${lineKey}-${Date.now()}` },
    client_operation_id: `bl-op-${lineKey}-${Date.now()}`,
  } });
  r = await check('BL1');
  ok(r.status === 400 && r.json.error === 'destination_required', '新商品の確認は 400 で止まる');
  ok(Array.isArray(r.json.missing) && r.json.missing.includes('back_label'), '足りないものとして back_label を返す');
  ok(/裏面/.test(r.json.message || ''), '画面に出せる文言が返る');
  ok(r.json.new_product && r.json.new_product.verdict === 'new', 'なぜ止めたか (新商品の判定) も返す');
  // 行き先などを「選んだ」ことにしても、写真がなければ通らない
  r = await req(J, `${APP}/api/lines/check`, { method: 'POST', body: {
    batch_id: 900, line_key: 'BL1', expect_version: 1, expect_quantity_version: 1, result: 'exact',
    mode: 'fill_remaining', fill_event: { client_event_id: 'bl-fill-choice' }, client_operation_id: 'bl-op-choice',
    choice: { destination: 'bfaith', bc_seal: '不要', irisu: 1 },
  } });
  ok(r.status === 400 && r.json.error === 'destination_required' && r.json.missing.includes('back_label'),
    `選択肢では埋められない (写真そのものが要る) — ${r.status} ${r.json?.error || ''}`);

  console.log('\n[D] 撮る');
  r = await req(J, `${APP}/api/back-label`, { method: 'POST', multipart: shot('op-http0001') });
  ok(r.status === 200 && r.json.ok && r.json.photo.status === 'stored', '写真を受け取れる (Drive 未設定でも成功する)');
  const photoId = r.json.photo.id;
  ok(r.json.photos.length === 1, 'その商品の写真一覧が返る');
  r = await req(J, `${APP}/api/back-label`, { method: 'POST', multipart: shot('op-http0001') });
  ok(r.status === 200 && r.json.already === true && r.json.photos.length === 1, '同じ送信IDの再送は二重登録しない');
  r = await req(J, `${APP}/api/back-label/${photoId}/file`);
  ok(r.status === 200 && r.ctype.includes('image/jpeg') && r.buf[0] === 0xFF && r.buf[1] === 0xD8, '撮った写真を開ける');

  console.log('\n[E] 撮ったら確認できる');
  r = await req(J, `${APP}/api/state`);
  ok(lineOf('BL1').back_label_required === false && lineOf('BL1').back_labels.length === 1, '一覧から「撮るまで確認できない」が消える');
  r = await check('BL1');
  ok(r.status === 200 && r.json.state.status === 'checked', '確認が通る');

  console.log('\n[F] 受け取りの検証 (multipart)');
  r = await req(J, `${APP}/api/back-label`, { method: 'POST', multipart: shot('op-txt00001', { __file: Buffer.from('これは画像ではありません') }) });
  ok(r.status === 400 && r.json.error === 'bad_file', '画像でないファイルは拒否 (拡張子を信じない)');
  r = await req(J, `${APP}/api/back-label`, { method: 'POST', multipart: shot('op-big00001', { __file: jpeg(9 * 1024 * 1024) }) });
  ok(r.status === 413 && r.json.error === 'too_large', '大きすぎる写真は 413 (HTML の 500 にしない)');
  // ⭐一覧が入れ替わっても、撮った商品コードで付く (伝票の控えだけ捨てる — Codex R1 #3)
  r = await req(J, `${APP}/api/back-label`, { method: 'POST', multipart: shot('op-stale001', { batch_id: '999' }) });
  ok(r.status === 200 && r.json.ok, '一覧が入れ替わっていても撮った商品に付く');
  // ⭐明細が別の商品を指していても、商品コードが勝つ (取り違えない)
  r = await req(J, `${APP}/api/back-label`, { method: 'POST', multipart: shot('op-mixed001', { line_key: 'BL2' }) });
  ok(r.status === 200 && r.json.photos.every((x) => x.product_id === 'BL-NEW'), '明細が別商品でも撮った商品に付く');
  r = await req(J, `${APP}/api/back-label`, { method: 'POST', multipart: shot('op-nocode01', { product_code: 'NOT-A-PRODUCT' }) });
  ok(r.status === 404 && r.json.error === 'not_found', 'どこにも無い商品コードは付けられない');
  r = await req(J, `${APP}/api/back-label`, { method: 'POST', multipart: shot('ab', {}) });
  ok(r.status === 400 && r.json.error === 'bad_request', '送信IDが不正なら拒否');
  r = await req(J, `${APP}/api/back-label`, { method: 'POST', multipart: shot('op-origin01'), headers: { origin: 'https://evil.example' } });
  ok(r.status === 403 && r.json.error === 'bad_origin', '別オリジンからは撮れない (CSRF)');

  console.log('\n[G] 撮り直し');
  r = await req(J, `${APP}/api/back-label/${photoId}/delete`, { method: 'POST', body: {} });
  ok(r.status === 200 && r.json.ok && r.json.photos.every((x) => x.id !== photoId), '消せる');
  // 消した送信IDへの再送は成功にしない (端末が手元の写真を捨てないように — Codex R1 #7)
  r = await req(J, `${APP}/api/back-label`, { method: 'POST', multipart: shot('op-http0001') });
  ok(r.status === 409 && r.json.error === 'gone', '消した写真の送信IDで送り直しても成功にしない');
  r = await req(J, `${APP}/api/back-label/${photoId}/file`);
  ok(r.status === 404, '消した写真は開けない');

  console.log('\n[I] 現物が1つも来なかった行は撮影を求めない (Codex R1 #1)');
  r = await req(J, `${APP}/api/state`);
  ok(lineOf('BL3').new_product.verdict === 'new', 'BL3 も新商品と判定される');
  ok(lineOf('BL3').back_label_required === true, '一覧では「撮るまで確認できない」と出る (届けば撮る)');
  // 「これ以上来ない — 不足3個で確認済みにする」= 実数0のまま確定する
  r = await req(J, `${APP}/api/lines/check`, { method: 'POST', body: {
    batch_id: 900, line_key: 'BL3', expect_version: 1, expect_quantity_version: 1,
    result: 'shortage', mode: 'current', client_operation_id: 'bl-op-none-1',
  } });
  ok(r.status === 200 && r.json.state.status === 'checked' && r.json.state.finalized_result === 'shortage',
    '撮る実物が無い確定は写真を求めずに通る (求めると永久に閉じられない)');
  r = await req(J, `${APP}/api/state`);
  ok(lineOf('BL3').new_product.verdict === 'new', '閉じた後も新商品のまま (実数0の確定は入庫の証拠にしない)');

  console.log('\n[H] 管理画面');
  r = await req(J, `${APP}/admin`);
  const html = r.buf.toString('utf8');
  ok(r.status === 200 && html.includes('新商品のパッケージ裏面ラベル写真'), '管理画面に節が出る');
  ok(html.includes('_裏面ラベル'), '保存先フォルダ名が出る');
  ok(/Drive 保存:\s*<b class="ng">未設定/.test(html), 'Drive 未設定なら未設定と出る (黙って捨てたように見せない)');
  r = await req(J, `${APP}/admin/back-labels/retry`, { method: 'POST', body: {} });
  ok(r.status === 200 && r.json.ok && r.json.disabled === true, 'Drive 未設定のときは送りに行かない');
} catch (e) {
  fail++;
  console.error('\n❌ 例外:', e.message);
  console.error(logs.slice(-3000));
} finally {
  child.kill();
}

console.log(`\n${fail === 0 ? '✅' : '❌'} ${pass} PASS / ${fail} FAIL`);
process.exit(fail === 0 ? 0 : 1);
