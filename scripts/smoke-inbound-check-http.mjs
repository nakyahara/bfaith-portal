/**
 * 入荷受付チェック — HTTP スモーク (server.js を子プロセスで起動し、実際の経路で叩く)
 *
 * 実行: node scripts/smoke-inbound-check-http.mjs [入荷状況照会CSVのパス]
 *   一時 DATA_DIR / PORT=3457 / PORTAL_PASS=smoke で起動。既定管理者 (d.nakahara@b-faith.biz) でログイン。
 * 検証: 未認証の扱い / セッションでの取込・確認 / 端末登録 (セッション破棄→端末Cookie) / 端末Cookieの権限境界 / Origin チェック
 */
import fs from 'fs';
import os from 'os';
import path from 'path';
import { spawn } from 'child_process';
import { fileURLToPath } from 'url';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const PORT = 3457;
const BASE = `http://127.0.0.1:${PORT}`;
const APP = `${BASE}/apps/inbound-check`;
const DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'ic-http-'));
const csvPath = process.argv[2];

let pass = 0, fail = 0;
const ok = (c, l) => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}`); } };

// ─── 起動前の種まき ───
// /api/info の編集を実経路で確かめるには、CSV の商品が入庫情報に載っている必要がある。
// サーバーを起動する前に入れて接続を閉じる (起動後に別プロセスから書くと SQLITE_BUSY を招く)
if (csvPath && fs.existsSync(csvPath)) {
  process.env.DATA_DIR = DATA_DIR;
  const { initMirrorDB, getMirrorDB } = await import('../apps/warehouse-mirror/db.js');
  initMirrorDB();
  const m = getMirrorDB();
  const now = new Date().toISOString();
  m.prepare(`INSERT INTO f_inbound_info (code_key, 商品コード, 商品名, 入数, 入庫時BCシール貼りフラグ, 直接ピックロケ保管, BF保管荷姿, いろは在庫化作業有無, source, created_at, updated_at)
    VALUES ('asahilabo15g', 'asahilabo15g', 'アサヒ商品', 10, '不要', '', 'そのまま', '無し', 'manual', ?, ?)`).run(now, now);
  // 他の商品も「行き先が決まっている」状態にしておく。
  // ⭐こうしないと確認のたびに行き先ゲートが開き、作業者記録や version の検証ができない
  const { parseInboundCsv } = await import('../apps/inbound-check/csv.js');
  const seedIns = m.prepare(`INSERT OR IGNORE INTO f_inbound_info
    (code_key, 商品コード, 商品名, 入数, 入庫時BCシール貼りフラグ, いろは在庫化作業有無, source, created_at, updated_at)
    VALUES (?, ?, ?, 1, '不要', '無し', 'manual', ?, ?)`);
  for (const row of parseInboundCsv(fs.readFileSync(csvPath)).rows) {
    if (row.product_id === 'b07bl10ml') continue;    // ← 行き先ゲート / 未登録の導線に使うので残す
    if (row.product_id === 'b17ls10ml') continue;  // ← 商品マスタにも無い商品の逃げ道に使う
    seedIns.run(row.code_key, row.product_id, row.product_name, now, now);
  }
  // b07bl10ml は商品マスタにだけ載せる = その場で選んだ行き先を入庫情報へ自動登録できる状態。
  // b17ls10ml はどちらにも載せない = 登録できないケース (現場は止めず台帳だけ残す) の確認用
  m.prepare(`INSERT INTO mirror_products (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, updated_at)
    VALUES (900002, 'b07bl10ml', 'ブレンドオイル', '単品', '取扱中', 'unknown', ?)`).run(now);
  // 2行目の商品は入庫情報に無い状態にしておく (未登録 → 登録の導線を確かめるため)
  m.prepare(`INSERT INTO mirror_products (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, updated_at)
    VALUES (900001, 'ohanautsuwa', 'お花の器', '単品', '取扱中', 'unknown', ?)`).run(now);
  m.close();
}

const child = spawn(process.execPath, ['server.js'], {
  cwd: ROOT,
  env: { ...process.env, DATA_DIR, PORT: String(PORT), PORTAL_PASS: 'smoke', NODE_ENV: 'development', SESSION_SECRET: 'smoke-secret',
    INBOUND_INFO_SYNC_ENABLED: 'false', STAFF_EXPORT_TOKEN: 'smoke-export-token' },
  stdio: ['ignore', 'pipe', 'pipe'],
});
let logs = '';
child.stdout.on('data', d => { logs += d; });
child.stderr.on('data', d => { logs += d; });

async function waitUp() {
  for (let i = 0; i < 120; i++) {
    try { const r = await fetch(`${BASE}/login`); if (r.status === 200) return true; } catch {}
    await new Promise(r => setTimeout(r, 500));
  }
  return false;
}

// 最小 cookie jar
function jar() {
  const store = new Map();
  return {
    absorb(res) {
      const set = res.headers.getSetCookie ? res.headers.getSetCookie() : [];
      for (const c of set) {
        const [kv, ...attrs] = c.split(';');
        const i = kv.indexOf('=');
        const k = kv.slice(0, i).trim(), v = kv.slice(i + 1).trim();
        const expires = attrs.map(a => a.trim().toLowerCase()).find(a => a.startsWith('expires=') || a.startsWith('max-age='));
        if ((expires && /max-age=0|1970/.test(expires)) || v === '') store.delete(k); else store.set(k, v);
      }
    },
    header() { return [...store.entries()].map(([k, v]) => `${k}=${v}`).join('; '); },
    has(k) { return store.has(k); },
    del(k) { store.delete(k); },
  };
}
async function req(j, url, { method = 'GET', body, headers = {}, form, multipart } = {}) {
  const h = { 'x-forwarded-proto': 'https', origin: BASE, ...headers };   // 変更系は Origin 必須 (ブラウザの fetch と同じ)   // セッションCookieは secure:true (trust proxy 経由の https として叩く)
  if (j) h.cookie = j.header();
  let payload;
  if (multipart) payload = multipart;
  else if (form) { h['content-type'] = 'application/x-www-form-urlencoded'; payload = new URLSearchParams(form).toString(); }
  else if (body !== undefined) { h['content-type'] = 'application/json'; payload = JSON.stringify(body); }
  const r = await fetch(url, { method, headers: h, body: payload, redirect: 'manual' });
  if (j) j.absorb(r);
  const text = await r.text();
  let json = null; try { json = JSON.parse(text); } catch {}
  return { status: r.status, text, json, location: r.headers.get('location'), ctype: r.headers.get('content-type') || '', cacheControl: r.headers.get('cache-control') || '' };
}

try {
  console.log('DATA_DIR =', DATA_DIR);
  if (!await waitUp()) throw new Error('server did not start\n' + logs.slice(-3000));

  console.log('\n[A] 未認証');
  let r = await req(null, `${APP}/`);
  ok(r.status === 302 && /\/apps\/inbound-check\/enroll/.test(r.location || ''), '作業画面 → 端末登録画面へ (iPad にログインを求めない)');
  r = await req(null, `${APP}/api/state`);
  ok(r.status === 401 && r.json?.error === 'unauthorized', 'API → 401');
  r = await req(null, `${APP}/manifest.json`);
  ok(r.status === 200 && r.json?.name === '入荷受付チェック', 'manifest.json は認証不要');
  r = await req(null, `${APP}/admin/upload`, { method: 'POST', body: {} });
  ok(r.status === 302 && /\/(login|apps\/inbound-check\/enroll)/.test(r.location || ''), '未認証の取込 POST はリダイレクトされ、取り込めない');

  console.log('\n[B] セッション (管理者)');
  const J = jar();
  r = await req(J, `${BASE}/login`, { method: 'POST', form: { email: 'd.nakahara@b-faith.biz', password: 'smoke' } });
  ok(r.status === 302 && J.header().length > 0, 'ログイン成功');
  r = await req(J, `${APP}`);
  ok(r.status === 308 && /\/apps\/inbound-check\/$/.test(r.location || ''), '末尾スラッシュ正規化 308');
  r = await req(J, `${APP}/`);
  ok(r.status === 200 && r.text.includes('入荷受付チェック') && r.text.includes('workerPick'), '作業画面 HTML');
  r = await req(J, `${APP}/api/state`);
  ok(r.status === 200 && r.json.ok && r.json.batch === null && r.json.me.admin === true, 'state (取込なし)');
  r = await req(J, `${APP}/admin`);
  ok(r.status === 200 && r.text.includes('取込履歴') && r.text.includes('この端末を登録'), '管理画面 (admin 節あり)');
  ok(r.text.includes('いろはへ送る商品') && r.text.includes('destinations.csv'), '管理画面に いろはへ送る商品 の一覧が出る');
  ok(r.text.includes('期限管理 (ロジザード商品マスタ)') && r.text.includes('fetchMasterBtn'), '管理画面に 期限管理 (商品マスタ) の節が出る');
  // Drive にファイルが無い環境でも 400 で返る (500 にしない = 原因が画面で分かる)
  r = await req(J, `${APP}/admin/fetch-product-master`, { method: 'POST', body: {} });
  ok(r.status === 400 && !!r.json?.message, `商品マスタの取込は取れないとき 400 で理由を返す (${r.json?.message?.slice(0, 60)})`);
  r = await req(null, `${APP}/admin/fetch-product-master`, { method: 'POST', body: {} });
  ok(r.status === 302 || r.status === 401, '未認証は商品マスタを取り込めない');
  // 作業者 = スタッフマスタ (apps/staff)。seed 13名が入っている
  r = await req(J, `${BASE}/apps/staff/api/list`);
  ok(r.status === 200 && r.json.staff.length === 13 && r.json.candidates[0].staff_no === '0001', 'スタッフマスタ seed 13名');
  r = await req(J, `${BASE}/apps/staff/`);
  ok(r.status === 200 && r.text.includes('スタッフマスタ') && r.text.includes('星 立夏'), 'スタッフマスタ管理画面');
  r = await req(J, `${APP}/admin/workers`, { method: 'POST', body: { name: '山田' } });
  ok(r.status === 404, '入荷側の作業者追加 API は無い (404)');
  r = await req(J, `${BASE}/apps/staff/api/staff`, { method: 'POST', body: { staff_no: '90001', display_name: 'テスト 一郎' } });
  ok(r.status === 200 && r.json.staff?.staff_no === '90001', 'スタッフ追加');
  r = await req(J, `${BASE}/apps/staff/api/staff`, { method: 'POST', body: { staff_no: '90001', display_name: '重複' } });
  ok(r.status === 400 && /既に/.test(r.json.message || ''), '番号重複は 400');
  r = await req(J, `${BASE}/apps/staff/api/staff`, { method: 'POST', body: { staff_no: '90002', display_name: 'x' }, headers: { origin: 'http://evil.example' } });
  ok(r.status === 403 && r.json?.error === 'bad_origin', '別 Origin の POST は 403');
  r = await req(J, `${BASE}/apps/staff/api/staff`, { method: 'POST', body: { staff_no: '90002', display_name: 'x' }, headers: { origin: '' } });
  ok(r.status === 403, 'Origin も Referer も無い POST は 403');
  r = await req(J, `${BASE}/apps/staff/api/staff`, { method: 'POST', body: { staff_no: '90002', display_name: 'x' }, headers: { origin: 'null' } });
  ok(r.status === 403, 'Origin: null は 403');
  r = await req(J, `${BASE}/apps/staff/api/staff`, { method: 'POST', body: { staff_no: '90002', display_name: 'テスト 二郎' }, headers: { origin: '', referer: BASE + '/apps/staff/' } });
  ok(r.status === 200 && r.json.staff?.staff_no === '90002', 'Origin 無しでも同一ホストの Referer があれば通る');
  const s2 = r.json.staff;
  r = await req(J, `${BASE}/apps/staff/api/staff/${s2.id}/active`, { method: 'POST', body: { active: 'false', expect_version: s2.version } });
  ok(r.status === 400, "active: 'false' (文字列) は 400");
  r = await req(J, `${BASE}/apps/staff/api/staff/${s2.id}/active`, { method: 'POST', body: { active: false } });
  ok(r.status === 400, 'active: expect_version 無しは 400');
  r = await req(J, `${BASE}/apps/staff/api/staff/${s2.id}/active`, { method: 'POST', body: { active: false, expect_version: s2.version, left_on: 'bad' } });
  ok(r.status === 400, 'active: 不正な left_on は 400');
  r = await req(J, `${BASE}/apps/staff/api/staff/${s2.id}/active`, { method: 'POST', body: { active: false, expect_version: s2.version } });
  ok(r.status === 200 && r.json.staff.active === 0, 'スタッフ無効化 (version 付き)');
  r = await req(J, `${BASE}/apps/staff/api/staff/${s2.id}`, { method: 'POST', body: { fields: { note: 'x' }, expect_version: s2.version } });
  ok(r.status === 409 && r.json.error === 'conflict', '無効化後の古い version での編集は 409');
  r = await req(J, `${BASE}/apps/staff/api/staff/${s2.id}`, { method: 'POST', body: { fields: { note: 'x' } } });
  ok(r.status === 400, '編集: expect_version 無しは 400');
  r = await req(J, `${BASE}/apps/staff/api/staff/${s2.id}`);
  ok(r.status === 200 && r.json.staff.version === s2.version + 1 && r.json.audit.length >= 2, 'スタッフ詳細 (version+1・監査あり)');
  r = await req(J, `${BASE}/apps/staff/api/staff/${s2.id}`, { method: 'POST', body: { fields: { note: 'y' }, expect_version: r.json.staff.version } });
  ok(r.status === 200 && r.json.staff.note === 'y', '最新 version での編集は 200');
  r = await req(J, `${APP}/admin`);
  ok(r.status === 200 && r.text.includes('星 立夏') && !r.text.includes('谷川 泰仁'), '入荷側の管理画面は倉庫作業のスタッフだけ表示 (事務は出ない)');
  r = await req(null, `${BASE}/apps/staff/export`);
  ok(r.status === 401, 'export: トークン無しは 401');
  r = await req(null, `${BASE}/apps/staff/export`, { headers: { authorization: 'Bearer wrong' } });
  ok(r.status === 401, 'export: 不正トークンは 401');
  r = await req(null, `${BASE}/apps/staff/export`, { headers: { authorization: 'Bearer smoke-export-token' } });
  ok(r.status === 200 && r.json.staff.length === 15 && r.json.staff[0].staff_no === '0001', 'export: 正しいトークンで全員 (無効含む)');
  ok(r.ctype.includes('json') && r.cacheControl === 'no-store', 'export: Cache-Control no-store');

  // 取込 (multipart)
  const csvBuf = csvPath && fs.existsSync(csvPath) ? fs.readFileSync(csvPath) : null;
  let batchId = null, firstKey = null;
  if (csvBuf) {
    const fd = new FormData();
    fd.append('file', new Blob([csvBuf]), 'CA04001_test.csv');
    fd.append('file_modified', String(Date.now() - 60_000));
    r = await req(J, `${APP}/admin/upload`, { method: 'POST', multipart: fd });
    ok(r.status === 200 && r.json.ok && r.json.rowCount === 16, `CSV 取込 16行 (${r.status} ${r.json?.message || ''})`);
    batchId = r.json.batch?.id;
    r = await req(J, `${APP}/admin/upload`, { method: 'POST', multipart: (() => { const f = new FormData(); f.append('file', new Blob([csvBuf]), 'again.csv'); f.append('file_modified', String(Date.now())); return f; })() });
    ok(r.status === 409 && r.json.error === 'duplicate_file', '同じCSVの再取込は 409');
    r = await req(J, `${APP}/api/state`);
    ok(r.json.lines.length === 16 && r.json.slips.length === 1 && r.json.totals.checked === 0, 'state 16行/1伝票');
    firstKey = r.json.lines[0].line_key;
    ok(r.json.lines[0].info === null || typeof r.json.lines[0].info === 'object', '補助情報フィールドあり');
    const K = n => `AR00110005164|${n}|1`;
    const fin = (body) => req(J, `${APP}/api/lines/check`, { method: 'POST', body });
    const allPresent = (n, extra = {}) => fin({ batch_id: batchId, line_key: K(n), expect_version: 1, expect_quantity_version: 1,
      result: 'exact', mode: 'fill_remaining', fill_event: { client_event_id: `sm-fill-${n}` }, worker_code: '20250901', ...extra });
    r = await allPresent(6);
    ok(r.status === 200 && r.json.state.status === 'checked' && r.json.state.checked_by === '星 立夏' && r.json.state.finalized_result === 'exact',
      '全部あり (セッション + スタッフ管理番号)');
    r = await allPresent(6);
    ok(r.status === 409 && r.json.error === 'finalized' && r.json.current.status === 'checked', '確定済みへの再確定 → 409 finalized');
    r = await fin({ batch_id: batchId + 99, line_key: K(6), worker_code: '20250901', expect_version: 1, expect_quantity_version: 1, result: 'exact' });
    ok(r.status === 409 && r.json.error === 'stale_batch', '旧/不明 batch_id → 409 stale_batch');
    r = await fin({ batch_id: batchId, line_key: K(7), expect_version: 1, expect_quantity_version: 1, result: 'exact',
      mode: 'fill_remaining', fill_event: { client_event_id: 'sm-fill-7' } });
    ok(r.status === 200 && r.json.state.checked_by === '中原 大輔', '作業者コード無し (セッション) = 表示名で記録');
    r = await fin({ batch_id: batchId, line_key: K(8), worker_code: 'w99', expect_version: 1, expect_quantity_version: 1, result: 'exact' });
    ok(r.status === 400 && r.json.error === 'worker_required', '不明なスタッフ管理番号 → 400');
    r = await fin({ batch_id: batchId, line_key: K(9), worker_code: '90002', expect_version: 1, expect_quantity_version: 1, result: 'exact' });
    ok(r.status === 400 && r.json.error === 'worker_required', '無効化したスタッフ → 400');
    for (const bad of [undefined, null, 0, -1, 1.5, 'x', '']) {
      r = await fin({ batch_id: batchId, line_key: K(10), worker_code: '20250901', expect_version: bad, expect_quantity_version: 1, result: 'exact' });
      ok(r.status === 400 && r.json.error === 'bad_request', `expect_version=${String(bad)} → 400`);
    }
    r = await fin({ batch_id: batchId, line_key: K(10), worker_code: '20250901', expect_version: '1', expect_quantity_version: '1',
      result: 'exact', mode: 'fill_remaining', fill_event: { client_event_id: 'sm-fill-10' } });
    ok(r.status === 200, "expect_version='1' (文字列の整数) は許容");

    console.log('\n[B1a] 数量 (部分確認) の HTTP 経路');
    {
      // ⚠ここは「箱を数えている最中に通信が切れた」「2台で同時に数えた」を通す道。正常系より事故系を先に見る
      const QE = `${APP}/api/lines/quantity-events`;
      const line = (await req(J, `${APP}/api/state`)).json.lines.find(l => l.line_key === K(1));
      const planned = line.planned_qty;
      const box = (id, q) => ({ client_event_id: id, action: 'add', quantity: q, input_kind: 'box', unit_size: q });
      const post = body => req(J, QE, { method: 'POST', body: { batch_id: batchId, line_key: K(1), worker_code: '20250901', ...body } });

      r = await post({ expect_quantity_version: 1, events: [box('sm-q-0001', 10)], pack_qty: 10 });
      ok(r.status === 200 && r.json.state.found_qty === 10 && r.json.state.quantity_version === 2, '＋1箱 → 200 が返ってから数が増える');
      r = await post({ expect_quantity_version: 2, events: [box('sm-q-0001', 10)] });
      ok(r.status === 200 && r.json.replayed && r.json.state.found_qty === 10, '同じ client_event_id の再送は二重加算しない (応答だけ失われた時の押し直し)');
      r = await post({ expect_quantity_version: 2, events: [box('sm-q-0001', 20)] });
      ok(r.status === 409 && r.json.error === 'idempotency_conflict', '同じ操作IDで違う内容 → 409');
      r = await post({ expect_quantity_version: 1, events: [box('sm-q-0002', 10)] });
      ok(r.status === 409 && r.json.error === 'conflict' && r.json.current.found_qty === 10, '古い quantity_version → 409 conflict + 現在値');
      r = await post({ expect_quantity_version: 2, events: [box('sm-q-0002', 10)] });
      ok(r.status === 200 && r.json.state.found_qty === 20, '別の端末の加算は足し算 (絶対値の上書きではない)');
      r = await post({ expect_quantity_version: 3, events: [{ client_event_id: 'sm-q-bad1', action: 'add', quantity: 0, input_kind: 'box' }] });
      ok(r.status === 400 && r.json.error === 'bad_request', '数量0は 400');
      r = await post({ expect_quantity_version: 3, events: [{ client_event_id: 'x', action: 'add', quantity: 1, input_kind: 'box' }] });
      ok(r.status === 400 && r.json.error === 'bad_request', '短すぎる client_event_id は 400');
      r = await req(J, `${APP}/api/lines/events?batch_id=${batchId}&line_key=${encodeURIComponent(K(1))}`);
      ok(r.status === 200 && r.json.events.length === 2, '数量イベント履歴が引ける (訂正パネル用)');
      const target = r.json.events[0];
      r = await post({ expect_quantity_version: 3, events: [
        { client_event_id: 'sm-q-0003', action: 'reversal', quantity: 10, input_kind: 'correction', reverses_event_seq: target.event_seq },
        { client_event_id: 'sm-q-0004', action: 'add', quantity: 12, input_kind: 'correction', unit_size: 12, replaces_event_seq: target.event_seq } ] });
      ok(r.status === 200 && r.json.state.found_qty === 22, '入数の訂正 (10入り→12入り) → 22');
      r = await post({ expect_quantity_version: 4, events: [
        { client_event_id: 'sm-q-0005', action: 'reversal', quantity: 10, input_kind: 'correction', reverses_event_seq: target.event_seq } ] });
      ok(r.status === 409 && r.json.error === 'already_reversed', '二重打ち消し → 409 already_reversed');

      // 人が選んだ意味と実数が食い違ったまま確定させない
      r = await fin({ batch_id: batchId, line_key: K(1), expect_version: 1, expect_quantity_version: 4, result: 'exact', worker_code: '20250901' });
      ok(r.status === 409 && r.json.error === 'result_mismatch', `22/${planned} で exact → 409 result_mismatch`);
      r = await fin({ batch_id: batchId, line_key: K(1), expect_version: 1, expect_quantity_version: 4, result: 'shortage', worker_code: '20250901', client_operation_id: 'sm-op-0001',
        choice: { destination: 'bfaith', bc_seal: '不要', irisu: 10 } });
      ok(r.status === 200 && r.json.state.finalized_result === 'shortage' && r.json.state.found_qty === 22, '不足のまま確定 → 22個で checked');
      r = await fin({ batch_id: batchId, line_key: K(1), expect_version: 1, expect_quantity_version: 4, result: 'shortage', worker_code: '20250901', client_operation_id: 'sm-op-0001',
        choice: { destination: 'bfaith', bc_seal: '不要', irisu: 10 } });
      ok(r.status === 200 && r.json.replayed, '確定の再送は replayed (台帳を増やさない)');
      r = await post({ expect_quantity_version: 5, events: [box('sm-q-0006', 10)] });
      ok(r.status === 409 && r.json.error === 'finalized', '確定済みの行には数を足せない → 409 finalized');
      r = await req(J, `${APP}/api/lines/uncheck`, { method: 'POST', body: { batch_id: batchId, line_key: K(1), expect_version: 2, worker_code: '20250901' } });
      ok(r.status === 200 && r.json.state.status === 'unchecked' && r.json.state.found_qty === 22, 'やり直す → 未確認に戻るが数量22は残る');
      r = await req(J, `${APP}/api/state`);
      const l5 = r.json.lines.find(l => l.line_key === K(1));
      ok(l5.quantity_relation === 'shortage' && l5.remaining_qty === planned - 22 && r.json.totals.partial >= 1,
        'state に found_qty / remaining_qty / quantity_relation / totals.partial が載る');
    }
    console.log('\n[B1b] 入庫情報を iPad からその場で直す');
    {
      // 入数やいろはは f_inbound_info (= /apps/inbound-info と同じ正本) に書く。値札印刷にもそのまま効く
      r = await req(J, `${APP}/api/state`);
      const seeded = r.json.lines.find(l => l.product_id === 'asahilabo15g');
      ok(seeded && seeded.info && seeded.info.irisu === 10 && seeded.info.version >= 1, `入数と version が state に載る (入数=${seeded?.info?.irisu} version=${seeded?.info?.version})`);
      const key = seeded.code_key, ver = seeded.info.version;
      // 作業者なしでは書かせない (誰が直したか残らない更新を作らない)
      r = await req(J, `${APP}/api/info`, { method: 'POST', body: { code_key: key, fields: { 入数: 24 }, expect_version: ver, worker_code: 'w99' } });
      ok(r.status === 400 && r.json.error === 'worker_required', '不明な作業者では保存できない');
      // version 必須 (楽観ロック)
      r = await req(J, `${APP}/api/info`, { method: 'POST', body: { code_key: key, fields: { 入数: 24 }, worker_code: '20250901' } });
      ok(r.status === 400 && r.json.error === 'bad_request', 'expect_version 無しは 400');
      // 編集を許していない列は通さない
      r = await req(J, `${APP}/api/info`, { method: 'POST', body: { code_key: key, fields: { 商品名: '書き換え' }, expect_version: ver, worker_code: '20250901' } });
      ok(r.status === 400 && r.json.error === 'bad_request', '商品名など許可外の列だけなら 400 (iPad から書き換えられない)');
      // 保存
      r = await req(J, `${APP}/api/info`, { method: 'POST', body: { code_key: key, fields: { 入数: 24, memo: '内箱つぶれ注意' }, expect_version: ver, worker_code: '20250901' } });
      ok(r.status === 200 && r.json.row.入数 === 24 && r.json.row.memo === '内箱つぶれ注意', '入数とメモを保存');
      ok(/星 立夏/.test(r.json.row.updated_by || ''), `誰が直したか残る (${r.json.row.updated_by})`);
      const ver2 = r.json.row.version;
      ok(ver2 === ver + 1, '保存すると version が上がる');
      // 古い version では上書きできない
      r = await req(J, `${APP}/api/info`, { method: 'POST', body: { code_key: key, fields: { 入数: 99 }, expect_version: ver, worker_code: '20250901' } });
      ok(r.status === 409 && r.json.error === 'conflict', '古い version は 409 (他の人の変更を消さない)');
      // いろは=有り にすると、いろは側で在庫化する3項目はサーバーが「－」に倒す
      r = await req(J, `${APP}/api/info`, { method: 'POST', body: { code_key: key, fields: { いろは在庫化作業有無: '有り' }, expect_version: ver2, worker_code: '20250901' } });
      ok(r.status === 200 && r.json.row.BF保管荷姿 === '－' && r.json.row.入庫時BCシール貼りフラグ === '－', 'いろは=有り で BCシール・荷姿が「－」になる');
      // 画面に反映される
      r = await req(J, `${APP}/api/state`);
      const after = r.json.lines.find(l => l.product_id === 'asahilabo15g');
      ok(after.info.irisu === 24 && /有り/.test(after.info.iroha), '一覧にも反映される');

      // 入庫情報が無い商品 → 登録の導線
      const unreg = r.json.lines.find(l => l.info === null);
      ok(!!unreg, `入庫情報が未登録の明細がある (${unreg?.product_id})`);
      r = await req(J, `${APP}/api/info/register`, { method: 'POST', body: { code: 'ohanautsuwa', worker_code: '20250901' } });
      ok(r.status === 200 && r.json.row && r.json.row.code_key === 'ohanautsuwa', '商品マスタにあるコードは登録できる');
      r = await req(J, `${APP}/api/info/register`, { method: 'POST', body: { code: 'ohanautsuwa', worker_code: '20250901' } });
      ok(r.status === 409 && r.json.error === 'duplicate', '二重登録は 409');
      r = await req(J, `${APP}/api/info/register`, { method: 'POST', body: { code: 'nosuchcode-xyz', worker_code: '20250901' } });
      ok(r.status === 400 && r.json.error === 'not_in_master', '商品マスタに無いコードは登録できない');
    }

    r = await req(J, `${APP}/admin/history.csv?batch_id=${batchId}`);
    ok(r.status === 200 && /text\/csv/.test(r.ctype) && r.text.includes('確認'), '履歴 CSV');
    r = await req(J, `${APP}/admin/history?batch_id=${batchId}`);
    ok(r.status === 200 && r.json.events.length >= 3 && r.json.events.every(e => e.result || e.action === 'uncheck'),
      '履歴 JSON に確認/やり直しが残り、確認には result (exact/shortage/excess) が入る');
    ok(r.json.events.some(e => e.result === 'shortage' && e.found_qty === 22 && e.planned_qty_snapshot === 100),
      '履歴に確定時点の実数と予定数のスナップショットが残る (後で数量を訂正しても当時の値が読める)');

    console.log('\n[B1a] 確認するときに行き先 (いろは / B-Faith) を確定させる');
    {
      r = await req(J, `${APP}/api/state`);
      const undecided = r.json.lines.find(l => l.product_id === 'b07bl10ml');
      ok(undecided && undecided.dest.missing.includes('iroha'), '入庫情報が無い商品は「行き先 未設定」として出る');
      ok(r.json.totals.undecided >= 1, `画面上部のアラート件数が出る (${r.json.totals.undecided}件)`);
      const key = undecided.line_key, ver = undecided.version;
      // 確定は「全部あり」= 残りを足して exact 確定する形で通す (失敗した回はロールバックされるので fill の id は使い回してよい)
      const body = extra => ({ batch_id: batchId, line_key: key, expect_version: ver, expect_quantity_version: undecided.quantity_version,
        result: 'exact', mode: 'fill_remaining', fill_event: { client_event_id: 'sm-gate-fill-1' }, worker_code: '20250901', ...extra });

      // 行き先を決めずには確認できない
      r = await req(J, `${APP}/api/lines/check`, { method: 'POST', body: body() });
      ok(r.status === 400 && r.json.error === 'destination_required' && r.json.missing.includes('iroha'), '行き先が未設定なら確認できない (400 destination_required)');

      // B-Faith を選んだらラベルと入数が要る
      r = await req(J, `${APP}/api/lines/check`, { method: 'POST', body: body({ choice: { destination: 'bfaith' } }) });
      ok(r.status === 400 && r.json.missing.includes('bc_seal'), 'B-Faith 入庫はラベル (BCシール) が無いと進めない');
      r = await req(J, `${APP}/api/lines/check`, { method: 'POST', body: body({ choice: { destination: 'bfaith', bc_seal: '不要' } }) });
      ok(r.status === 400 && r.json.missing.includes('irisu'), 'B-Faith 入庫は入数が無いと進めない');
      r = await req(J, `${APP}/api/lines/check`, { method: 'POST', body: body({ choice: { destination: 'bfaith', bc_seal: '不要', irisu: 0 } }) });
      ok(r.status === 400, '入数 0 は拒否');

      // いろはを選ぶ → 確認が通り、選んだ内容が入庫情報に登録される
      r = await req(J, `${APP}/api/lines/check`, { method: 'POST', body: body({ choice: { destination: 'iroha' } }) });
      ok(r.status === 200 && r.json.state.status === 'checked', 'いろはを選べば確認できる');
      r = await req(J, `${APP}/api/state`);
      const after = r.json.lines.find(l => l.product_id === 'b07bl10ml');
      ok(after.info && after.info.iroha === '有り', '選んだ行き先が入庫情報に自動登録される');
      ok(after.dest.destination === 'iroha' && after.dest.missing.length === 0, '次からは聞かれない (データに沿って判定)');
      ok(r.json.totals.toIroha >= 1, `いろは行きの件数が出る (${r.json.totals.toIroha}件)`);

      // 台帳に残る
      r = await req(J, `${APP}/admin/destinations`);
      const led = r.json.rows.find(x => x.product_id === 'b07bl10ml');
      ok(led && led.destination === 'iroha' && led.decided_from === 'chosen' && !led.cancelled_at, 'いろはへ送る実績が台帳に残る');
      ok(led.planned_qty === after.planned_qty && led.ar_no === after.ar_no, '数量と入荷管理番号も台帳に残る');

      // 取り消すと台帳も取り消される (行は消さない)
      r = await req(J, `${APP}/api/lines/uncheck`, { method: 'POST', body: { batch_id: batchId, line_key: key, expect_version: ver + 1, worker_code: '20250901' } });
      ok(r.status === 200, '確認を取り消せる');
      r = await req(J, `${APP}/admin/destinations?include_cancelled=1`);
      const led2 = r.json.rows.find(x => x.product_id === 'b07bl10ml');
      ok(led2 && led2.cancelled_at, '取り消すと台帳は消さずに取消日時が入る');
      r = await req(J, `${APP}/admin/destinations`);
      ok(!r.json.rows.some(x => x.product_id === 'b07bl10ml'), '既定の一覧からは取り消し分が外れる');
      r = await req(J, `${APP}/admin/destinations.csv`);
      ok(r.status === 200 && /text\/csv/.test(r.ctype) && r.text.includes('有効期限'), '台帳を CSV で出せる');
    }

    console.log('\n[B1c] 期限管理商品は確認のたびに有効期限を聞く');
    {
      r = await req(J, `${APP}/api/state`);
      const line = r.json.lines.find(l => l.check_status !== 'checked' && !l.dest.missing.length);
      ok(!!line, '行き先が決まっている未確認の明細を用意');
      // 期限管理ありに設定
      r = await req(J, `${APP}/api/product-flags`, { method: 'POST', body: { code_key: line.code_key, expiry_managed: true, worker_code: '20250901' } });
      ok(r.status === 200 && r.json.expiry_managed === true, '期限管理あり に設定できる');
      r = await req(J, `${APP}/api/state`);
      const l2 = r.json.lines.find(x => x.line_key === line.line_key);
      ok(l2.expiry_managed === true && l2.expiry_source === 'manual', '一覧に期限管理あり として出る');
      ok(l2.dest.missing.includes('expiry'), '期限管理商品は毎回 有効期限 を聞かれる');
      const body = extra => ({ batch_id: batchId, line_key: line.line_key, expect_version: l2.version, expect_quantity_version: l2.quantity_version,
        result: 'exact', mode: 'fill_remaining', fill_event: { client_event_id: 'sm-exp-fill-1' }, worker_code: '20250901', ...extra });
      r = await req(J, `${APP}/api/lines/check`, { method: 'POST', body: body() });
      ok(r.status === 400 && r.json.missing.includes('expiry'), '有効期限なしでは確認できない');
      r = await req(J, `${APP}/api/lines/check`, { method: 'POST', body: body({ choice: { expiry_date: '2026-02-31' } }) });
      ok(r.status === 400, '実在しない日付は拒否 (2026-02-31)');
      r = await req(J, `${APP}/api/lines/check`, { method: 'POST', body: body({ choice: { expiry_date: '2027-06' } }) });
      ok(r.status === 200, '年月だけ (日は指定なし) でも確認できる');
      r = await req(J, `${APP}/admin/destinations?destination=all`);
      const led = r.json.rows.find(x => x.line_key === line.line_key);
      ok(led && led.expiry_date === '2027-06', '有効期限が台帳に残る');
      ok(led.decided_from === 'master', '行き先そのものは入庫情報どおりと記録される (期限だけ聞いた場合)');
      // 期限管理を戻しておく (後続のテストが巻き添えにならないように)
      await req(J, `${APP}/api/product-flags`, { method: 'POST', body: { code_key: line.code_key, expiry_managed: false, worker_code: '20250901' } });
    }

    console.log('\n[B1d] 商品マスタに無い商品でも現場を止めない');
    {
      // 入庫情報を作れないケース。**確認は通し、行き先は台帳に残す**。
      // ここで止めると、荷受けの現場が「登録できないから消し込めない」で詰まる
      r = await req(J, `${APP}/api/state`);
      const orphan = r.json.lines.find(l => l.product_id === 'b17ls10ml');
      ok(orphan && orphan.info === null, '入庫情報も商品マスタも無い明細を用意');
      r = await req(J, `${APP}/api/lines/check`, { method: 'POST', body: { batch_id: batchId, line_key: orphan.line_key, expect_version: orphan.version, expect_quantity_version: orphan.quantity_version,
      result: 'exact', mode: 'fill_remaining', fill_event: { client_event_id: 'sm-orphan-fill-1' }, worker_code: '20250901', choice: { destination: 'iroha' } } });
      ok(r.status === 200 && r.json.state.status === 'checked', '商品マスタに無くても確認できる');
      ok(/商品マスタ/.test(r.json.warning || ''), `登録できなかったことは画面に伝える (${r.json.warning})`);
      r = await req(J, `${APP}/admin/destinations`);
      ok(r.json.rows.some(x => x.product_id === 'b17ls10ml'), '入庫情報が作れなくても行き先の記録は残る');
    }
  } else {
    console.log('  (CSV パス未指定: 取込系はスキップ)');
  }

  console.log('\n[B2] 登録コードで iPad を登録する (ログイン不要の経路)');
  {
    // 未登録の端末が作業画面を開くと、/login ではなく登録画面へ送られる
    const IPAD = jar();
    let r2 = await req(IPAD, `${APP}/`);
    ok(r2.status === 302 && /\/apps\/inbound-check\/enroll/.test(r2.location || ''), '未登録の端末は登録画面へ (ログイン画面ではない)');
  // 手順書は登録前の iPad からこそ読まれるページ。認証なしで 200 を返すこと
  r = await req(null, `${APP}/guide`);
  ok(r.status === 200 && r.text.includes('入荷受付チェックの使い方') && r.text.includes('数量を数える'), '手順書 /guide は未認証でも読める');
    r2 = await req(IPAD, `${APP}/enroll`);
    ok(r2.status === 200 && r2.text.includes('6桁の登録コード'), '登録画面はログイン不要で開ける');
    // 管理者が PC でコードを発行
    r2 = await req(J, `${APP}/admin/enroll-codes`, { method: 'POST', body: { label: '入荷iPad-smoke' } });
    ok(r2.status === 200 && /^\d{6}$/.test(r2.json.code || ''), `管理者がコードを発行 (${r2.json.code})`);
    const code = r2.json.code;
    // 一般ユーザー・端末では発行できない
    const r3 = await req(null, `${APP}/admin/enroll-codes`, { method: 'POST', body: { label: 'x' } });
    ok(r3.status === 401 || r3.status === 403 || r3.status === 302, `未認証はコードを発行できない (${r3.status})`);
    // iPad が引き換え
    r2 = await req(IPAD, `${APP}/enroll/redeem`, { method: 'POST', body: { code: '000000' === code ? '111111' : '000000' } });
    ok(r2.status === 400, '違うコードは 400');
    r2 = await req(IPAD, `${APP}/enroll/redeem`, { method: 'POST', body: { code } });
    ok(r2.status === 200 && r2.json.ok && IPAD.has('ic_device'), '正しいコードで端末Cookieが入る');
    r2 = await req(IPAD, `${APP}/`);
    ok(r2.status === 200, '以後はログインなしで作業画面が開く');
    r2 = await req(IPAD, `${APP}/api/state`);
    ok(r2.status === 200 && r2.json.me.device?.label === '入荷iPad-smoke', '端末として認識される');
    // 端末は管理系を触れない (権限境界)
    r2 = await req(IPAD, `${APP}/admin/enroll-codes`, { method: 'POST', body: { label: 'x' } });
    ok(r2.status === 403, '端末はコードを発行できない');
    r2 = await req(IPAD, `${APP}/enroll`);
    ok(r2.status === 302 && /\/apps\/inbound-check\/$/.test(r2.location || ''), '登録済みなら登録画面は作業画面へ戻す');
    // 同じコードは二度使えない
    const IPAD2 = jar();
    r2 = await req(IPAD2, `${APP}/enroll/redeem`, { method: 'POST', body: { code } });
    ok(r2.status === 400 && r2.json.error === 'used', '使用済みのコードは別端末でも使えない');
  }
  console.log('\n[B3] 登録コードの総当たりは途中で止まる');
  {
    // ⚠6桁 = 100万通り。存在しないコードを順に試されても止まらないと、有効期限の10分でも十分に危ない
    const ATTACK = jar();
    let last = null, blockedAt = -1;
    for (let i = 0; i < 12; i++) {
      last = await req(ATTACK, `${APP}/enroll/redeem`, { method: 'POST', body: { code: String(900000 + i) } });
      if (last.status === 429) { blockedAt = i; break; }
    }
    ok(blockedAt >= 0, `存在しないコードの連打は ${blockedAt + 1} 回目で 429 になる`);
    ok(!!last.json?.message, '止めた理由が画面に出せる (メッセージつき)');
    // 打ち止め中は、管理者が発行した正しいコードでも通らない
    const issued = await req(J, `${APP}/admin/enroll-codes`, { method: 'POST', body: { label: '入荷iPad-rate' } });
    const r = await req(ATTACK, `${APP}/enroll/redeem`, { method: 'POST', body: { code: issued.json.code } });
    ok(r.status === 429 && !ATTACK.has('ic_device'), '打ち止め中は当たりのコードでも登録されない');
  }

  console.log('\n[C] 端末登録 → 端末Cookie');
  r = await req(J, `${APP}/admin/devices`, { method: 'POST', body: { label: '入荷iPad1' } });
  ok(r.status === 200 && r.json.ok && r.json.loggedOut === true && J.has('ic_device'), '端末登録: ic_device Cookie 発行');
  const savedDeviceToken = (J.header().match(/ic_device=([^;]+)/) || [])[1];
  r = await req(J, `${APP}/admin`);
  ok(r.status === 302 && /\/login/.test(r.location || ''), '登録と同時にセッション破棄 (管理画面はログインへ)');
  r = await req(J, `${APP}/api/state`);
  ok(r.status === 200 && r.json.me.device?.label === '入荷iPad1' && r.json.me.session === null, '端末Cookieで state 取得');
  r = await req(J, `${APP}/`);
  ok(r.status === 200, '端末Cookieで作業画面');
  r = await req(J, `${APP}/admin/upload`, { method: 'POST', body: {} });
  ok(r.status === 403 && r.json.error === 'session_required', '端末Cookieでは取込不可 (403)');
  // ⭐iPad の実経路: 端末Cookie だけで入庫情報を直せること (ログインは求めない)。
  //   作業者は名前タップ = worker_code で渡すので、誰が直したかは端末名と組で残る
  if (batchId) {
    r = await req(J, `${APP}/api/state`);
    const target = r.json.lines.find(l => l.info && l.info.version >= 1);
    if (target) {
      r = await req(J, `${APP}/api/info`, { method: 'POST', body: { code_key: target.code_key, fields: { memo: '端末から記入' }, expect_version: target.info.version, worker_code: '20250901' } });
      ok(r.status === 200 && r.json.row.memo === '端末から記入', '端末Cookieだけで入庫情報を直せる (ログイン不要)');
      ok(/星 立夏 \(入荷iPad1\)/.test(r.json.row.updated_by || ''), `作業者と端末名が残る (${r.json.row.updated_by})`);
      r = await req(J, `${APP}/api/info`, { method: 'POST', body: { code_key: target.code_key, fields: { memo: 'x' }, expect_version: r.json.row.version } });
      ok(r.status === 400 && r.json.error === 'worker_required', '端末でも作業者の指定は必須');
    } else {
      ok(false, '入庫情報のある明細が見つからない (テストの前提が崩れている)');
    }
  }
  r = await req(J, `${BASE}/apps/staff/api/staff`, { method: 'POST', body: { staff_no: '90003', display_name: 'x' } });
  ok(r.status === 401, '端末Cookieではスタッフ追加不可 (401)');
  r = await req(J, `${APP}/admin/history.csv?batch_id=1`);
  ok(r.status === 403 || r.status === 302, '端末Cookieでは履歴不可');
  if (batchId) {
    const cur = (await req(J, `${APP}/api/state`)).json.lines.find(l => l.check_status === 'checked');
    r = await req(J, `${APP}/api/lines/uncheck`, { method: 'POST', body: { batch_id: batchId, line_key: cur.line_key, expect_version: cur.version, worker_code: '20250901' } });
    ok(r.status === 200 && r.json.state.status === 'unchecked' && r.json.state.found_qty === cur.found_qty,
      '端末Cookie + 作業者コードでやり直し (数量は残る)');
    r = await req(J, `${APP}/api/lines/check`, { method: 'POST', body: { batch_id: batchId, line_key: firstKey, expect_version: 3 } });
    ok(r.status === 400 && r.json.error === 'worker_required', '端末Cookieで作業者未選択 → 400');
  }
  r = await req(J, `${APP}/device/exit`, { method: 'POST', body: {} });
  ok(r.status === 200 && r.json.revoked === true && !J.has('ic_device'), '端末解除で Cookie 削除 + サーバー側も失効');
  r = await req(null, `${APP}/api/state`, { headers: { cookie: 'ic_device=' + (savedDeviceToken || '') } });
  ok(r.status === 401, '解除後は控えておいたトークンでも 401 (失効済み)');
  r = await req(J, `${APP}/api/state`);
  ok(r.status === 401, '解除後は 401');

  console.log('\n[D] 一般ユーザー (非admin・アプリ権限あり) の境界');
  // 管理者で再ログインしてユーザー作成 → そのユーザーでログイン
  const A = jar();
  await req(A, `${BASE}/login`, { method: 'POST', form: { email: 'd.nakahara@b-faith.biz', password: 'smoke' } });
  r = await req(A, `${BASE}/admin/users/add`, { method: 'POST', form: { email: 'staff@example.com', password: 'staffpass', displayName: '事務', role: 'user', allowedApps: 'inbound-check' } });
  const U = jar();
  r = await req(U, `${BASE}/login`, { method: 'POST', form: { email: 'staff@example.com', password: 'staffpass' } });
  if (r.status === 302 && U.header()) {
    r = await req(U, `${APP}/api/state`);
    ok(r.status === 200 && r.json.me.admin === false, '一般ユーザー: state OK / admin=false');
    r = await req(U, `${APP}/admin`);
    ok(r.status === 200 && !r.text.includes('この端末を登録'), '一般ユーザー: 管理画面は見えるが端末登録節は出ない');
    r = await req(U, `${APP}/admin/devices`, { method: 'POST', body: { label: 'x' } });
    ok(r.status === 403, '一般ユーザー: 端末登録は 403');
    r = await req(U, `${APP}/admin/history.csv?batch_id=1`);
    ok(r.status === 403, '一般ユーザー: 履歴CSV は 403');
    r = await req(U, `${BASE}/apps/staff/api/list`);
    ok(r.status === 403, '一般ユーザー: スタッフマスタ API は 403');
    r = await req(U, `${BASE}/apps/staff/`);
    ok(r.status === 403, '一般ユーザー: スタッフマスタ画面は 403');
    if (csvBuf) {
      const f = new FormData(); f.append('file', new Blob([Buffer.from('broken')]), 'x.csv');
      r = await req(U, `${APP}/admin/upload`, { method: 'POST', multipart: f });
      ok(r.status === 400 && r.json.error === 'bad_csv', '一般ユーザー: 取込は可 (壊れたCSVは 400)');
    }
  } else {
    console.log('  (ユーザー作成 API の形式が想定と異なるためスキップ: ' + r.status + ')');
  }
} catch (e) {
  fail++;
  console.log('  ✗ 例外:', e.message);
} finally {
  child.kill();
}
console.log(`\n${pass} PASS / ${fail} FAIL`);
if (fail) console.log('--- server log tail ---\n' + logs.slice(-2500));
process.exitCode = fail ? 1 : 0;
