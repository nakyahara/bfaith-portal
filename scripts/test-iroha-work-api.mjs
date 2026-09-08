/**
 * いろは在庫化作業アプリ — HTTP の口 (router) のテスト
 *
 * 実行: node scripts/test-iroha-work-api.mjs
 *
 * db.js のテスト (test-iroha-work.mjs) は関数を直接呼ぶので、
 * **router が値を渡し忘れていても気づけない** (feedback: 画面テストが green でも何も検証していない)。
 * ここは実際に express にマウントして HTTP で叩く。
 *
 * 検証項目 (中原さん 2026-09-05 の依頼):
 *   1. 作業する人を1人も選ばずに開始できない (worker_ids: [] / 壊れた値)
 *   2. 複数人まとめて開始 → 人数ぶんのセッションが返る
 *   3. 別カードで作業中の人が混ざると誰も開始しない (名指しで断る)
 *   4. まとめ終了 (session_ids) / 1人だけ終了
 *   5. 記録の検索 (人・期間・商品) と CSV
 */
import fs from 'fs';
import os from 'os';
import path from 'path';
import express from 'express';

if (!process.env.DATA_DIR) {
  process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'iroha-api-test-'));
}

let pass = 0, fail = 0;
function ok(cond, label) {
  if (cond) { pass++; console.log(`  ✓ ${label}`); } else { fail++; console.log(`  ✗ ${label}`); }
}

const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const { default: router } = await import('../apps/iroha-work/router.js');
const { getDB, addIrohaWorker, setMetaValue, createDevice } = await import('../apps/iroha-work/db.js');
const { upsertTaskFromImport } = await import('../apps/iroha-work/tasks-db.js');

// 参照テーブルは本物の init で作る (列名を想像しない)
const { createTables: icCreateTables } = await import('../apps/inbound-check/db.js');
icCreateTables(getDB());

// 正本をアプリにする (いまの本番と同じ経路を通す)
setMetaValue('source_of_truth', 'app');

// ポータルにログイン済みの職員として通す (認証そのものは別のテストの担当)。
// role はテストの途中で切り替える — CSV は管理者だけなので、両方の立場で確かめる
let sessionRole = 'admin';
const app = express();
// 本番と同じ (server.js は Cloudflare Tunnel の 1 段だけを信じる)。
// これが無いと req.ip が socket のアドレスになり、接続元ごとの上限を試せない
app.set('trust proxy', 1);
app.use(express.json());
app.use((req, _res, next) => {
  // ⭐sessionRole = null は「ログインしていない人」。セッションを入れない
  //   (入れてしまうと、ログイン不要の口を試したつもりが管理者として通っている — Codex R5 中2)
  if (sessionRole === null) { req.session = {}; return next(); }
  req.session = { authenticated: true, email: 'test@b-faith.biz', displayName: 'テスト', allowedApps: '*', role: sessionRole };
  next();
});
app.use('/apps/iroha-work', router);

const server = await new Promise((resolve) => { const s = app.listen(0, () => resolve(s)); });
const port = server.address().port;
const HOST = `127.0.0.1:${port}`;
const BASE = `http://${HOST}/apps/iroha-work`;

/** 画面と同じヘッダで叩く (checkOrigin を通すため Origin を付ける) */
async function post(pathname, body, cookie) {
  const r = await fetch(BASE + pathname, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Origin: `http://${HOST}`, Host: HOST, ...(cookie ? { Cookie: cookie } : {}) },
    body: JSON.stringify(body),
  });
  return { status: r.status, json: await r.json().catch(() => ({})) };
}
async function get(pathname, cookie) {
  const r = await fetch(BASE + pathname, { headers: cookie ? { Host: HOST, Cookie: cookie } : { Host: HOST } });
  const buf = Buffer.from(await r.arrayBuffer());
  const text = buf.toString('utf8');
  let json = null;
  try { json = JSON.parse(text); } catch { /* CSV はそのまま text で見る */ }
  return { status: r.status, json, text, buf, type: r.headers.get('content-type') || '' };
}

const wid = (name, type = 'member') => addIrohaWorker({ displayName: name, workerType: type, actor: 'test' }).id;
const A = wid('あべ'), B = wid('いのうえ'), C = wid('うえだ'), S = wid('えんどう', 'staff');
const INACTIVE = wid('おかだ');
getDB().prepare('UPDATE f_iroha_workers SET active = 0 WHERE id = ?').run(INACTIVE);

const mkTask = (name, code) => upsertTaskFromImport({
  notion_page_id: 'api-' + code, status: 'not_started', facility_code: 'iroha',
  destination_id: null, product_code: code, product_name: name, qty: 100,
  arrival_date: '2026-09-01', master_snapshot: { material_code: 'T10-15', units_per_container: 120 },
}, { batchId: 'test-api' }).id;
const T1 = mkTask('みつろうクリーム 木工用 60g', 'API-001');
const T2 = mkTask('レザーウェア洗剤 100ml', 'API-002');

console.log('\n[1] 作業する人を選ばないと開始できない (中原さん 2026-09-05)');
{
  const empty = await post('/api/sessions/start', { id: T1, worker_id: A, worker_ids: [] });
  ok(empty.status === 400 && empty.json.error === 'worker_required', '0人では 400 worker_required');
  ok(/えらぶ|選/.test(empty.json.message || ''), '断る理由が日本語で返る');
  const bad = await post('/api/sessions/start', { id: T1, worker_id: A, worker_ids: 'あべ' });
  ok(bad.status === 400 && bad.json.error === 'worker_required', '配列でない worker_ids も断る');
  const zeros = await post('/api/sessions/start', { id: T1, worker_id: A, worker_ids: [0, -1, 'x'] });
  ok(zeros.status === 400, '中身が全部おかしければ断る');
  // 壊れた値を黙って捨てて「残った1人で開始」にしない (選んだつもりの人が記録から抜ける)
  const partly = await post('/api/sessions/start', { id: T1, worker_id: A, worker_ids: [A, 'こわれた値'] });
  ok(partly.status === 400 && /こわれ/.test(partly.json.message || ''), '一部が壊れていても断る (残りだけで開始しない)');
  const unknown = await post('/api/sessions/start', { id: T1, worker_id: A, worker_ids: [A, 99999] });
  ok(unknown.status === 400 && /名簿/.test(unknown.json.message || ''), '名簿にない人が混ざれば断る');
  const off = await post('/api/sessions/start', { id: T1, worker_id: A, worker_ids: [A, INACTIVE] });
  ok(off.status === 400 && /おかだ/.test(off.json.message || ''), '無効な人は名指しで断る');
  ok(getDB().prepare('SELECT COUNT(*) c FROM f_iroha_work_sessions').get().c === 0, 'ここまで1件も記録が入っていない');
}

console.log('\n[2] 複数人まとめて開始');
{
  const r = await post('/api/sessions/start', { id: T1, worker_id: A, worker_ids: [A, B, C] });
  ok(r.status === 200 && r.json.ok === true, '3人で開始できる');
  ok(Array.isArray(r.json.sessions) && r.json.sessions.length === 3, 'sessions が3件返る');
  ok(r.json.sessions.every((s) => s.sessionId && s.workerName), 'sessionId と名前が入っている (画面がそのまま札にできる)');
  ok(r.json.sessionId === r.json.sessions.find((s) => s.workerId === A).sessionId, 'sessionId は操作した人ぶん');
  ok(r.json.status === 'in_progress', '未着手のカードは「作業中」になる');
  ok(getDB().prepare('SELECT COUNT(*) c FROM f_iroha_work_sessions WHERE task_id = ? AND ended_at IS NULL').get(T1).c === 3,
    'DB にも3行 (人ごとに1行)');
  ok(getDB().prepare("SELECT COUNT(*) c FROM f_iroha_app_events WHERE action = 'session_start' AND ok = 1").get().c === 3,
    '記録 (イベント) も人ごとに3件残る');

  const again = await post('/api/sessions/start', { id: T1, worker_id: A, worker_ids: [A, B, C] });
  ok(again.json.ok === true && again.json.sessions.every((s) => s.already), '同じ人たちの再送は already (二重に増えない)');
  ok(getDB().prepare('SELECT COUNT(*) c FROM f_iroha_work_sessions WHERE task_id = ?').get(T1).c === 3, '行は増えていない');

  const add = await post('/api/sessions/start', { id: T1, worker_id: A, worker_ids: [S] });
  ok(add.json.ok === true && add.json.sessions.length === 1 && !add.json.sessions[0].already, '途中から職員を足せる');
}

console.log('\n[3] 別カードで作業中の人が混ざったら誰も開始しない');
{
  const r = await post('/api/sessions/start', { id: T2, worker_id: A, worker_ids: [A, B] });
  ok(r.status === 409 && r.json.error === 'busy', '409 busy で断る');
  ok(/あべ/.test(r.json.message) && /みつろう/.test(r.json.message), '誰がどのカードで作業中かを出す');
  ok(Array.isArray(r.json.busy) && r.json.busy.length === 2, '作業中の人を全員返す (画面が外せる)');
  ok(getDB().prepare('SELECT COUNT(*) c FROM f_iroha_work_sessions WHERE task_id = ?').get(T2).c === 0,
    'T2 には1行も入っていない (途中まで入れない)');
}

console.log('\n[4] 終了 — 1人だけ / まとめて');
{
  const openIds = getDB().prepare('SELECT id, worker_id FROM f_iroha_work_sessions WHERE task_id = ? AND ended_at IS NULL ORDER BY id').all(T1);
  ok(openIds.length === 4, '4人が作業中');

  const one = await post('/api/sessions/stop', { id: T1, worker_id: A, session_ids: [openIds[0].id], reason: 'done' });
  ok(one.json.ok === true && one.json.stopped.length === 1 && one.json.remainingActive === 3,
    '1人だけ先に上がれる (残り3名)');
  ok(one.json.stopped[0].workerName === 'あべ', '誰が上がったかを返す');

  const mixed = await post('/api/sessions/stop', { id: T2, worker_id: A, session_ids: [openIds[1].id], reason: 'done' });
  ok(mixed.status === 409 && mixed.json.error === 'not_started', '別カードの session_id は断る');
  ok(getDB().prepare('SELECT COUNT(*) c FROM f_iroha_work_sessions WHERE task_id = ? AND ended_at IS NULL').get(T1).c === 3,
    '断られたときは1件も閉じていない');

  const rest = openIds.slice(1).map((x) => x.id);
  const all = await post('/api/sessions/stop', { id: T1, worker_id: A, session_ids: rest, reason: 'done' });
  ok(all.json.ok === true && all.json.stopped.length === 3 && all.json.remainingActive === 0, '残り3人をまとめて終了');
  ok(typeof all.json.totalSeconds === 'number', '合計時間が返る (画面のトーストに出す)');
  ok(all.json.session && all.json.session.id === rest[0], '単数版と同じ形も返す (古い画面が動く)');

  const resend = await post('/api/sessions/stop', { id: T1, worker_id: A, session_ids: rest, reason: 'done' });
  ok(resend.json.ok === true && resend.json.stopped.every((s) => s.already), '再送は冪等');
}

console.log('\n[5] 記録の検索');
{
  const all = await get('/admin/sessions/search');
  ok(all.status === 200 && all.json.ok === true, '検索できる');
  ok(all.json.summary.count === 4 && all.json.rows.length === 4, '4件 (4人ぶん)');
  ok(all.json.rows[0].mates.length >= 1, 'いっしょにやった人が入っている');

  const byWho = await get('/admin/sessions/search?worker_id=' + A);
  ok(byWho.json.summary.count === 1 && byWho.json.rows[0].worker_name === 'あべ', '人でしぼれる');

  const byQ = await get('/admin/sessions/search?q=' + encodeURIComponent('みつろう'));
  ok(byQ.json.summary.count === 4, '商品名でしぼれる');
  ok((await get('/admin/sessions/search?q=' + encodeURIComponent('レザー'))).json.summary.count === 0,
    '作業していない商品は0件');

  const today = new Date(Date.now() + 9 * 3600000).toISOString().slice(0, 10);
  ok((await get(`/admin/sessions/search?from=${today}&to=${today}`)).json.summary.count === 4, '今日 (JST) で引ける');
  const yesterday = new Date(Date.now() + 9 * 3600000 - 86400000).toISOString().slice(0, 10);
  ok((await get(`/admin/sessions/search?from=${yesterday}&to=${yesterday}`)).json.summary.count === 0, '昨日は0件');

  ok((await get('/admin/sessions/search?limit=2')).json.truncated === true, '件数を絞ると続きがあると分かる');
  ok((await get('/admin/sessions/search?limit=99999')).json.rows.length === 4, '大きすぎる limit でも落ちない');

  const csv = await get('/admin/sessions/search.csv?q=' + encodeURIComponent('みつろう'));
  ok(csv.status === 200 && /text\/csv/.test(csv.type), 'CSV で返る');
  ok(csv.buf[0] === 0xEF && csv.buf[1] === 0xBB && csv.buf[2] === 0xBF, 'BOM 付き (Excel が文字化けしない)');
  const lines = csv.text.trim().split('\r\n');
  ok(lines.length === 5, '見出し + 4行');
  ok(lines[0].includes('作業した人') && lines[0].includes('いっしょにやった人'), '見出しが日本語');
  ok(lines.slice(1).every((l) => l.includes('みつろうクリーム 木工用 60g')), '商品名が入っている');
  ok(lines.slice(1).some((l) => l.includes('あべ')), '作業した人が入っている');
}

console.log('\n[6] おかしい検索条件は黙って広げず、理由をつけて断る (Codex レビュー 2026-09-05)');
{
  const bad = async (qs) => (await get('/admin/sessions/search?' + qs));
  const w1 = await bad('worker_id=99999');
  ok(w1.status === 400 && /見つかりません/.test(w1.json.message), '知らない作業者IDは 400 (全員ぶんを返さない)');
  ok((await bad('worker_id=abc')).status === 400, '数でない作業者IDも 400');
  const d1 = await bad('from=2026-02-30');
  ok(d1.status === 400 && /2026-02-30/.test(d1.json.message), '実在しない日 (2/30) は 400 — 3/2 に繰り上げて通さない');
  ok((await bad('to=こわれた日付')).status === 400, '日付でない文字も 400');
  const order = await bad('from=2026-09-05&to=2026-09-01');
  ok(order.status === 400 && /いつから/.test(order.json.message), 'from > to は 400');
  ok((await bad('q=' + 'あ'.repeat(101))).status === 400, '長すぎる検索文字は 400');
  // 空文字は「指定なし」。画面が空欄のまま押しても通る
  ok((await bad('worker_id=&from=&to=&q=')).status === 200, '空欄は「指定なし」として通る');
  // LIMIT/OFFSET に整数でない値が来ても 500 にしない (SQLite は datatype mismatch を投げる)
  for (const qs of ['limit=1.5', 'offset=Infinity', 'limit=-5', 'limit=1e9', 'offset=abc']) {
    ok((await bad(qs)).status === 200, `${qs} でも 500 にならない`);
  }
}

console.log('\n[7] CSV — 欠けたものを渡さない / Excel の数式にしない');
{
  // 商品名は Notion・仕入先から来るので「=」で始まることがある (CSVインジェクション)
  const T3 = mkTask('=HYPERLINK("http://example.com","クリック")', 'API-003');
  const st = await post('/api/sessions/start', { id: T3, worker_id: A, worker_ids: [A] });
  ok(st.json.ok === true, '前提: 数式に見える商品名のカードで作業する');
  await post('/api/sessions/stop', { id: T3, worker_id: A, session_ids: [st.json.sessions[0].sessionId], reason: 'done' });
  const csv = await get('/admin/sessions/search.csv?q=' + encodeURIComponent('HYPERLINK'));
  ok(csv.status === 200, 'CSV は出せる');
  ok(/"'=HYPERLINK/.test(csv.text), '「=」で始まる値にはアポストロフィを前置する (Excel が数式として実行しない)');
  ok(!/,=HYPERLINK/.test(csv.text), '生の =HYPERLINK は入っていない');

  // 上限を超えたら「先頭だけのCSV」を渡さず断る (工賃計算に使うので、欠けたと気づけないのがいちばん危ない)
  const { getDB: gdb } = await import('../apps/iroha-work/db.js');
  const db = gdb();
  const now = new Date().toISOString();
  const ins = db.prepare(`INSERT INTO f_iroha_work_sessions
    (page_id, product_code, title_snapshot, worker_id, worker_name, started_at, ended_at, end_reason, raw_seconds)
    VALUES (?, 'BULK-001', 'かさ増しテスト商品', ?, 'あべ', ?, ?, 'done', 60)`);
  // ⭐1ページ (500件) を超えても全部入る。ここが欠けると工賃の計算が静かに狂う
  const ins2 = db.prepare(`INSERT INTO f_iroha_work_sessions
    (page_id, product_code, title_snapshot, worker_id, worker_name, started_at, ended_at, end_reason, raw_seconds)
    VALUES (?, 'PAGE-001', 'ページ送りテスト商品', ?, 'あべ', ?, ?, 'done', 60)`);
  db.transaction(() => { for (let i = 0; i < 700; i++) ins2.run('page-' + i, A, now, now); })();
  const paged = await get('/admin/sessions/search.csv?q=' + encodeURIComponent('ページ送り'));
  ok(paged.status === 200, '700件でも CSV は出せる');
  ok(paged.text.trim().split('\r\n').length === 701, '見出し + 700行 (500件で切れない)');
  ok((await get('/admin/sessions/search?q=' + encodeURIComponent('ページ送り'))).json.summary.count === 700,
    '画面の合計も 700 件 (表に出ている分だけの合計にしない)');

  db.transaction(() => { for (let i = 0; i < 5100; i++) ins.run('bulk-' + i, A, now, now); })();
  const over = await get('/admin/sessions/search.csv?q=' + encodeURIComponent('かさ増し'));
  ok(over.status === 422 && /しぼって/.test(over.json.message), '5000件を超えたら 422 で断る (欠けたCSVを出さない)');
  const okCsv = await get('/admin/sessions/search.csv?q=' + encodeURIComponent('みつろう'));
  ok(okCsv.status === 200, '条件を絞れば出せる');

  // ⭐CSV は管理者だけ (中原さん 2026-09-05)。画面で見るのと違い、利用者の作業時間を
  //   まとめて持ち出せるため。検索そのもの (画面) は職員も使える
  sessionRole = 'user';
  const denied = await get('/admin/sessions/search.csv?q=' + encodeURIComponent('みつろう'));
  ok(denied.status === 403 && denied.json.error === 'forbidden', '管理者でなければ CSV は 403');
  ok(!/みつろう/.test(denied.text), '断ったときに中身を漏らさない');
  ok((await get('/admin/sessions/search?q=' + encodeURIComponent('みつろう'))).status === 200,
    '画面の検索は管理者でなくても使える (いろはアプリを許可された職員)');
  sessionRole = 'admin';
  ok((await get('/admin/sessions/search.csv?q=' + encodeURIComponent('みつろう'))).status === 200, '管理者に戻せば出せる');

  // CSV を出したことは操作履歴に残す (誰がいつ何件持ち出したか)
  const logged = getDB().prepare("SELECT COUNT(*) c FROM f_iroha_app_events WHERE action = 'sessions_csv'").get().c;
  ok(logged >= 1, 'CSV の書き出しが操作履歴に残る');
}

// ─── 外部施設の専用 URL (HTTP で通す。§AB-11 の 6) ───
{
  console.log('\n[F] 外部施設の専用 URL');
  const D = await import('../apps/iroha-work/db.js');
  const CG = await import('../apps/iroha-work/consign.js');
  const B2 = await import('../apps/iroha-work/batches.js');
  const TD2 = await import('../apps/iroha-work/tasks-db.js');

  // 発行は管理者だけ
  sessionRole = 'user';
  const denied = await post('/admin/facility-links', { facility_code: 'workcenter', label: 'だめ' });
  ok(denied.status === 403, '⭐管理者でなければ発行できない');
  sessionRole = 'admin';
  const made = await post('/admin/facility-links', { facility_code: 'workcenter', label: 'ワークセンター 事務所' });
  ok(made.status === 200 && /^\/apps\/iroha-work\/f\//.test(made.json.url), '管理者は発行できる');
  const token = made.json.url.split('/f/')[1];

  // ⭐ログインも端末登録もなしで開ける (Host だけ付けて素の GET)
  const raw = await fetch(`http://${HOST}${made.json.url}`, { headers: { Host: HOST } });
  const html = await raw.text();
  ok(raw.status === 200 && /おあずかりしている商品/.test(html), '⭐URL だけで画面が開く');
  ok((raw.headers.get('x-robots-tag') || '').includes('noindex'), '⭐検索よけのヘッダーが付く');
  ok((raw.headers.get('cache-control') || '').includes('no-store'), '⭐キャッシュさせない');
  ok((raw.headers.get('referrer-policy') || '') === 'no-referrer', '⭐リファラを送らない');

  // 中身 (その施設のぶんだけ)
  const t = upsertTaskFromImport({ notion_page_id: 'flapi-1', status: 'not_started', facility_code: 'iroha',
    destination_id: 8801, product_name: '専用URL の商品', qty: 400 }, { batchId: 'flapi' }).id;
  const b0 = B2.listBatchesOfTask(getDB(), t)[0];
  const cg = CG.startConsignment({ taskId: t, batchId: b0.id, facilityCode: 'workcenter', qty: 400,
    expectVersion: TD2.getTask(t).version });
  CG.markHanded({ consignmentId: cg.consignment.id, expectVersion: cg.consignment.version });
  const api = await fetch(`http://${HOST}${made.json.url}/api/view`, { headers: { Host: HOST } });
  const view = await api.json();
  ok(api.status === 200 && view.ok && view.facility.code === 'workcenter', '⭐中身も URL だけで読める');
  ok(view.items.some((x) => x.product_name === '専用URL の商品' && x.handed_qty === 400), '預けたぶんが出る');

  // ⭐書き込みは受け付けない
  const wr = await fetch(`http://${HOST}${made.json.url}/api/view`, {
    method: 'POST', headers: { Host: HOST, Origin: `http://${HOST}`, 'Content-Type': 'application/json' }, body: '{}' });
  ok(wr.status === 404 || wr.status === 405, '⭐POST は受け付けない (見るだけ)');

  // ⭐**ログインしていない人**として確かめる (ここまではテスト用の middleware が管理者セッションを
  //   入れていたので、「URL だけで開く」ことを本当には試せていなかった — Codex R5 中2)
  {
    sessionRole = null;
    const anon = await fetch(`http://${HOST}${made.json.url}`, { headers: { Host: HOST } });
    const anonHtml = await anon.text();
    ok(anon.status === 200 && /おあずかりしている商品/.test(anonHtml),
      '⭐ログインしていなくても、専用 URL は開ける');
    const anonApi = await fetch(`http://${HOST}${made.json.url}/api/view`, { headers: { Host: HOST } });
    ok(anonApi.status === 200 && (await anonApi.json()).ok, '⭐中身も読める');
    // ⭐同じ「ログインしていない」状態で、社内の口は閉じている
    const inner = await fetch(`http://${HOST}/apps/iroha-work/api/state`, { headers: { Host: HOST } });
    ok(inner.status === 401, '⭐社内の一覧はログインしていないと 401');
    const issue = await fetch(`http://${HOST}/apps/iroha-work/admin/facility-links`, {
      method: 'POST', headers: { Host: HOST, Origin: `http://${HOST}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ facility_code: 'workcenter', label: 'だめ' }), redirect: 'manual' });
    ok(issue.status >= 300 && issue.status !== 200, '⭐URL の発行はログインしていないとできない (' + issue.status + ')');
    const revoke = await fetch(`http://${HOST}/apps/iroha-work/admin/facility-links/1/revoke`, {
      method: 'POST', headers: { Host: HOST, Origin: `http://${HOST}` }, redirect: 'manual' });
    ok(revoke.status >= 300 && revoke.status !== 200, '⭐失効もできない (' + revoke.status + ')');
    const board = await fetch(`http://${HOST}/apps/iroha-work/admin`, { headers: { Host: HOST }, redirect: 'manual' });
    ok(board.status >= 300 && board.status !== 200, '⭐管理画面も開けない (' + board.status + ')');
    sessionRole = 'admin';
  }

  // ⭐/f/ の下は、受けなかったパス・メソッドがこの先へ流れない (Codex R1)
  for (const [method, p] of [
    ['POST', made.json.url], ['PUT', made.json.url], ['DELETE', made.json.url], ['PATCH', made.json.url],
    ['GET', made.json.url + '/api'],
    // ⭐パスをエンコードして .. を潜ませても、作業アプリ側へは届かない (fetch は生の ../ を送る前に正規化するので、エンコードで試す)
    ['GET', made.json.url + '/%2e%2e%2f%2e%2e%2fapi/state'],
    ['GET', '/apps/iroha-work/f'], ['GET', '/apps/iroha-work/f/'],
    ['POST', '/apps/iroha-work/f/' + token + '/api/view'],
  ]) {
    const r = await fetch(`http://${HOST}${p}`, { method, headers: { Host: HOST, Origin: `http://${HOST}` },
      redirect: 'manual' });
    const body = await r.text();
    ok(r.status >= 400 && r.status < 500, `⭐${method} ${p} は 4xx で終わる (通り抜けない)`);
    ok(!/"capabilities"|"cards"|hold_memo/.test(body), `⭐${method} ${p} で作業アプリの中身が漏れない`);
  }
  // HEAD では「見た日時」を書かない (書くのは人が開いた GET だけ)
  {
    const l0 = D.listFacilityLinks(true).find((l) => l.label === 'ワークセンター 事務所');
    getDB().prepare('UPDATE f_iroha_facility_links SET last_seen_at = NULL WHERE id = ?').run(l0.id);
    await fetch(`http://${HOST}${made.json.url}`, { method: 'HEAD', headers: { Host: HOST } });
    const after = D.listFacilityLinks(true).find((l) => l.id === l0.id);
    ok(after.last_seen_at == null, '⭐HEAD では「見た日時」を書かない');
    await fetch(`http://${HOST}${made.json.url}`, { headers: { Host: HOST } });
    ok(D.listFacilityLinks(true).find((l) => l.id === l0.id).last_seen_at != null, 'GET なら書く');
  }

  // ⭐繰り返し叩かれても、社内の画面まで巻き込まれない (Codex R2 中2)
  {
    const fresh = await post('/admin/facility-links', { facility_code: 'workcenter', label: '回数の上限テスト' });
    let limited = 0;
    let served = 0;
    for (let i = 0; i < 80; i++) {
      const r = await fetch(`http://${HOST}${fresh.json.url}/api/view`, { headers: { Host: HOST } });
      if (r.status === 429) limited++; else if (r.status === 200) served++;
    }
    ok(served >= 30 && limited > 0, '⭐ふつうに見るぶんは通り、叩きすぎると 429 で断る (' + served + ' 回通過 / ' + limited + ' 回拒否)');
    // ⭐社内の画面は止まらない
    ok((await get('/api/state')).status === 200, '⭐外から叩かれている間も、社内の一覧はふつうに開ける');
    // 別のトークンは巻き添えにしない (接続元の上限は別枠なので、IP を変えて確かめる)
    const other = await post('/admin/facility-links', { facility_code: 'rashinban', label: '別のリンク' });
    ok((await fetch(`http://${HOST}${other.json.url}/api/view`,
      { headers: { Host: HOST, 'X-Forwarded-For': '10.0.0.9' } })).status === 200,
      '⭐別の施設のリンクは巻き添えにしない (リンクごとに数える)');
  }

  // ⭐でたらめなトークンを毎回変えて送られても、内側が重くならない (Codex R3 重大1)
  {
    const t0 = Date.now();
    let ok404 = 0;
    let blocked = 0;
    for (let i = 0; i < 300; i++) {
      // 毎回ちがうトークン = 記録を無限に増やす狙いの叩き方
      const r = await fetch(`http://${HOST}/apps/iroha-work/f/${'a'.repeat(30)}${i}`,
        { headers: { Host: HOST, 'X-Forwarded-For': '203.0.113.' + (i % 250) } });
      if (r.status === 404) ok404++; else if (r.status === 429) blocked++;
    }
    const ms = Date.now() - t0;
    ok(ok404 + blocked === 300, '⭐でたらめなトークンは 404 か 429 で終わる (' + ok404 + ' / ' + blocked + ')');
    // ⭐内側の画面がふつうに開ける (叩かれても巻き込まれない)
    const t1 = Date.now();
    const inner = await get('/api/state');
    ok(inner.status === 200 && Date.now() - t1 < 3000,
      '⭐外から叩かれている間も、社内の一覧がすぐ開く (' + (Date.now() - t1) + 'ms)');
    ok(ms < 20000, '300 回の無効アクセス自体も現実的な時間で終わる (' + ms + 'ms)');
    // ⭐同じ接続元から繰り返せば、そこで頭打ちになる
    let ipBlocked = 0;
    for (let i = 0; i < 200; i++) {
      const r = await fetch(`http://${HOST}/apps/iroha-work/f/${'b'.repeat(30)}${i}`,
        { headers: { Host: HOST, 'X-Forwarded-For': '198.51.100.7' } });
      if (r.status === 429) ipBlocked++;
    }
    ok(ipBlocked > 0, '⭐同じ接続元から叩き続けると 429 で断る (' + ipBlocked + ' 回)');

    // ⭐送り手が X-Forwarded-For の**先頭**を書き換えても、接続元ごとの上限をすり抜けられない。
    //   先頭は誰でも好きに書けるので、そこを鍵にすると「毎回ちがう値」で素通りできてしまう
    //   (重大1 と同じ「外から自由に作れるものを鍵にする」誤り)
    let spoofBlocked = 0;
    for (let i = 0; i < 200; i++) {
      const r = await fetch(`http://${HOST}/apps/iroha-work/f/${'c'.repeat(30)}${i}`, {
        headers: { Host: HOST, 'X-Forwarded-For': '9.9.9.' + i + ', 198.51.100.42' } });
      if (r.status === 429) spoofBlocked++;
    }
    ok(spoofBlocked > 0, '⭐X-Forwarded-For の先頭を毎回変えても、上限をすり抜けられない (' + spoofBlocked + ' 回で断った)');

    // ⭐叩かれている**最中**でも社内の画面が開く (終わったあとの計測では分からない — Codex R4)
    {
      let worst = 0;
      const attack = (async () => {
        for (let i = 0; i < 400; i++) {
          await fetch(`http://${HOST}/apps/iroha-work/f/${'d'.repeat(30)}${i}`,
            { headers: { Host: HOST, 'X-Forwarded-For': '192.0.2.' + (i % 200) } }).catch(() => {});
        }
      })();
      for (let i = 0; i < 10; i++) {
        const t = Date.now();
        const r = await get('/api/state');
        worst = Math.max(worst, Date.now() - t);
        ok(r.status === 200, '⭐叩かれている最中でも社内の一覧が開く (' + (i + 1) + '回目)');
      }
      await attack;
      ok(worst < 3000, '⭐叩かれている最中でも社内の一覧が待たされない (いちばん遅くて ' + worst + 'ms)');
    }

    // ⭐たくさんの接続元で記録をあふれさせても、いったん断った相手が数え直せない (Codex R4 中1)
    {
      const victim = '198.51.100.99';
      let blockedOnce = false;
      for (let i = 0; i < 200 && !blockedOnce; i++) {
        const r = await fetch(`http://${HOST}/apps/iroha-work/f/${'e'.repeat(30)}${i}`,
          { headers: { Host: HOST, 'X-Forwarded-For': victim } });
        if (r.status === 429) blockedOnce = true;
      }
      ok(blockedOnce, '前提: この接続元はいったん断られた');
      // 別の接続元を大量に作って、記録を押し出そうとする
      for (let i = 0; i < 300; i++) {
        await fetch(`http://${HOST}/apps/iroha-work/f/${'f'.repeat(30)}${i}`,
          { headers: { Host: HOST, 'X-Forwarded-For': '203.0.114.' + (i % 250) } }).catch(() => {});
      }
      const again = await fetch(`http://${HOST}/apps/iroha-work/f/${'g'.repeat(30)}`,
        { headers: { Host: HOST, 'X-Forwarded-For': victim } });
      ok(again.status === 429,
        '⭐別の接続元をたくさん作っても、断られた記録は消せない (生きている記録は追い出さない)');
    }

    // ⭐たくさんの接続元で埋められても、**正しい URL を持っている施設は締め出されない** (自己レビュー)。
    //   接続元の上限は「満杯なら断る」作りなので、正しいトークンにまで掛けると
    //   外から接続元を大量に作るだけで先方を締め出せてしまう
    {
      const good = await post('/admin/facility-links', { facility_code: 'workcenter', label: '締め出されないこと' });
      for (let i = 0; i < 400; i++) {
        await fetch(`http://${HOST}/apps/iroha-work/f/${'h'.repeat(30)}${i}`,
          { headers: { Host: HOST, 'X-Forwarded-For': '203.0.115.' + (i % 250) } }).catch(() => {});
      }
      // 同じ (埋めるのに使った) 接続元から、正しい URL で開く
      const r = await fetch(`http://${HOST}${good.json.url}/api/view`,
        { headers: { Host: HOST, 'X-Forwarded-For': '203.0.115.7' } });
      ok(r.status === 200, '⭐接続元が埋まっていても、正しい URL なら開ける (先方を巻き添えにしない)');
    }
  }

  // でたらめ・失効したトークン
  const bad = await fetch(`http://${HOST}/apps/iroha-work/f/${'x'.repeat(43)}`, { headers: { Host: HOST } });
  ok(bad.status === 404, 'でたらめなトークンは 404');
  const badBody = await bad.text();
  ok(!/おあずかりしている商品/.test(badBody) && /見られません/.test(badBody), '⭐理由は分けて教えない (総当たりの手がかりにしない)');
  const links = D.listFacilityLinks(true);
  const mine = links.find((l) => l.label === 'ワークセンター 事務所');
  const rev = await post('/admin/facility-links/' + mine.id + '/revoke');
  ok(rev.status === 200, '失効させられる');
  ok((await fetch(`http://${HOST}${made.json.url}`, { headers: { Host: HOST } })).status === 404,
    '⭐失効させたら開けない');
  sessionRole = 'user';
  ok((await post('/admin/facility-links/' + mine.id + '/revoke')).status === 403, '⭐失効も管理者だけ');
  sessionRole = 'admin';
}

console.log('\n[預ける計画] GET /api/consign-plan (§AB-11 の 7b)');
{
  // ⭐実際に HTTP で叩く。service を直接呼ぶテストでは、router が値を渡し忘れていても気づけない
  const t = upsertTaskFromImport({ notion_page_id: 'api-cplan', status: 'not_started', facility_code: 'iroha',
    destination_id: null, product_code: 'API-CPLAN', product_name: '預け計画の検査', qty: 700,
    arrival_date: '2026-09-01', master_snapshot: { units_per_container: 70, process_count: 2 },
  }, { batchId: 'test-api' }).id;
  const batch = getDB().prepare('SELECT id FROM f_iroha_task_batches WHERE task_id = ?').get(t);
  const ver = getDB().prepare('SELECT version FROM f_iroha_tasks WHERE id = ?').get(t).version;
  const made = await post('/api/consign', { id: t, batch_id: batch.id, facility_code: 'workcenter',
    qty: 700, worker_id: S, expect_version: ver, due_date: '2026-10-01' });
  ok(made.status === 200 && made.json.ok, '(前提) 預けを 1 件つくれる');

  const r = await get('/api/consign-plan');
  ok(r.status === 200 && r.json && r.json.ok, '職員なら 200 で返る');
  const row = (r.json.rows || []).find((x) => x.id === made.json.consignment.id);
  ok(!!row, '作った預けが行に出る');
  ok(row.state === 'planned' && row.qty === 700 && row.boxes === 10,
    '⭐状態・数・箱数がそろって返る (router が渡し忘れていない)');
  ok(row.due_date === '2026-10-01', '返却の期限も返る');
  ok(Array.isArray(r.json.facilities) && r.json.facilities.every((f) => f.offsite),
    '拠点は物を持ち帰るところだけ');
  ok(r.json.counts && r.json.counts.to_prepare >= 1, '入口に出す件数も返る');
  ok(r.json.facility_loads && typeof r.json.facility_loads === 'object', '受け入れ枠も返る (画面がそのまま描ける)');

  // ⭐ログインしていない人には出さない
  sessionRole = null;
  const anon = await get('/api/consign-plan');
  ok(anon.status === 401 || anon.status === 403, '⭐ログインしていなければ出さない (' + anon.status + ')');
  // ⭐**登録ずみの iPad から、職員モードに入らずに**叩いたら 403。
  //   ログインの有無だけを見ていると、この口の職員チェックを外しても気づけない (Codex R1 軽微1)
  const dev = createDevice('検査用 iPad (預ける計画)', 'test');
  const asDevice = await get('/api/consign-plan', 'iw_device=' + dev.token);
  ok(asDevice.status === 403 && asDevice.json && asDevice.json.error === 'staff_required',
    '⭐端末で入っただけ (職員モードなし) では 403 staff_required');
  ok(/職員/.test((asDevice.json || {}).message || ''), '断る理由が読める');
  sessionRole = 'admin';

  // ⭐書き込みの口ではない
  const w = await post('/api/consign-plan', {});
  ok(w.status === 404 || w.status === 405, '⭐POST は受けない (読むだけの画面)');
}

console.log('\n[人数だけの作業] POST /api/sessions/start-crew (§AB-10)');
{
  const t = upsertTaskFromImport({ notion_page_id: 'api-crew', status: 'not_started', facility_code: 'rehas',
    destination_id: null, product_code: 'API-CREW', product_name: '人数の検査', qty: 100,
    arrival_date: '2026-09-01', master_snapshot: { units_per_container: 10, process_count: 2 },
  }, { batchId: 'test-api' }).id;
  const ok200 = await post('/api/sessions/start-crew', { id: t, worker_id: S, facility_code: 'rehas', crew_size: 3 });
  ok(ok200.status === 200 && ok200.json.ok, '職員ならはじめられる');
  ok(ok200.json.session && ok200.json.session.crewSize === 3, '人数が返る');
  ok(ok200.json.status === 'in_progress', '未着手のカードは作業中になる');
  // 一覧に「誰が作業中か」が出る (router の渡し忘れを見る)
  const st = await get('/api/state');
  const card = (st.json.cards || []).find((c) => c.id === t);
  ok(card && (card.active || []).length === 1, 'カードに作業中の記録が 1 件出る');
  ok(card.active[0].worker_name === null && card.active[0].facility_code === 'rehas' && card.active[0].crew_size === 3,
    '⭐名前は無く、拠点と人数が返る (画面が「パレット 3人」と描ける)');
  ok((st.json.capabilities || []).includes('task.work.crew'), '職員には 人数だけの作業 の許可が出る');
  // 断り方
  const bad = await post('/api/sessions/start-crew', { id: t, worker_id: S, facility_code: 'workcenter', crew_size: 2 });
  ok(bad.status === 400 && bad.json.error === 'bad_facility', '物を持ち帰る拠点は断る (400)');
  const n0 = await post('/api/sessions/start-crew', { id: t, worker_id: S, facility_code: 'rehas', crew_size: 0 });
  ok(n0.status === 400, '人数 0 は断る');
  const noFac = await post('/api/sessions/start-crew', { id: t, worker_id: S });
  ok(noFac.status === 400 && /どこ/.test(noFac.json.message || ''), '拠点を選ばなければ断る');
  // 終わらせる (session_ids の口はそのまま使える)
  const stop = await post('/api/sessions/stop', { id: t, worker_id: S, session_ids: [ok200.json.sessionId], reason: 'done' });
  ok(stop.status === 200 && stop.json.ok, '⭐終わらせるのは今までの口でできる');
  // 職員でない端末からは断る
  sessionRole = null;
  const dev2 = createDevice('検査用 iPad (人数)', 'test');
  const asDev = await post('/api/sessions/start-crew', { id: t, worker_id: S, facility_code: 'rehas', crew_size: 2 },
    'iw_device=' + dev2.token);
  ok(asDev.status === 403, '⭐端末で入っただけ (職員モードなし) では 403 (' + asDev.status + ')');
  sessionRole = 'admin';
}

console.log('\n[箱ラベル] POST /api/print/jobs はまとまり単位 (§AB-12)');
{
  const t = upsertTaskFromImport({ notion_page_id: 'api-label', status: 'not_started', facility_code: 'iroha',
    destination_id: null, product_code: 'API-LABEL', product_name: 'ラベルの検査', qty: 200,
    arrival_date: '2026-09-02', barcode: 'X000T1GS6F', expiry: '2027-03',
    master_snapshot: { units_per_container: 120 } }, { batchId: 'test-api' }).id;
  const at = new Date().toISOString();
  getDB().prepare("INSERT INTO f_iroha_task_batches (task_id, seq, planned_qty, expiry, work_status, created_at, updated_at) VALUES (?, 2, 80, '2028-06', 'not_started', ?, ?)")
    .run(t, at, at);
  // 印刷係 (いろはPC) を 1 台登録する。これが無いと、まとまりを見る前に「印刷係がいません」で断られる
  createDevice('検査用 いろはPC', 'test', { kind: 'agent', printerName: 'Brother QL-800' });
  const cid = () => 'apilbl' + Date.now().toString(36) + Math.random().toString(36).slice(2, 8);
  const noPick = await post('/api/print/jobs', { task_id: t, copies: 1, pack_qty: 120, client_request_id: cid(), worker_id: S });
  ok(noPick.status === 400 && noPick.json.error === 'pick_batch',
    '⭐分かれたカードで、どのぶんか選ばずに出そうとしたら断る (' + noPick.status + ')');
  ok(Array.isArray(noPick.json.batches) && noPick.json.batches.length === 2, '選べるように候補を返す');
  const pick = noPick.json.batches.find((b) => b.seq === 2);
  ok(pick && pick.expiry === '2028-06', '⭐ぶんごとの期限も返る (これを見て選ぶ)');
  const okJob = await post('/api/print/jobs', { task_id: t, batch_id: pick.id, copies: 1, pack_qty: 120, client_request_id: cid(), worker_id: S });
  ok(okJob.status === 200 && okJob.json.ok, '選べば出せる');
  ok(getDB().prepare('SELECT batch_id FROM f_iroha_print_jobs WHERE id = ?').get(okJob.json.job.id).batch_id === pick.id,
    '⭐どのぶんを刷ったかが残る (router が渡し忘れていない)');
  getDB().prepare('DELETE FROM f_iroha_print_jobs WHERE task_id = ?').run(t);
}

console.log('\n[入荷予定] 🚚 仕入先0001の入荷予定を読む口 (中原さん 2026-09-08)');
{
  sessionRole = 'admin';
  const { importCsv } = await import('../apps/inbound-check/db.js');
  const iconv = (await import('iconv-lite')).default;
  const db = getDB();
  db.prepare(`INSERT INTO mirror_products (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, 仕入先コード, updated_at)
    VALUES (901, 'IP-AMC', 'マスタ名', '単品', '取扱中', 'ok', '0001', '2026-09-08T00:00:00Z')`).run();
  db.prepare(`INSERT INTO mirror_products (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, 仕入先コード, updated_at)
    VALUES (902, 'IP-OTHER', 'よそのマスタ名', '単品', '取扱中', 'ok', '0002', '2026-09-08T00:00:00Z')`).run();
  db.prepare(`INSERT INTO f_inbound_info (code_key, 商品コード, 商品名, いろは在庫化作業有無, source, created_at, updated_at)
    VALUES ('ip-amc', 'IP-AMC', 'マスタ名', '有り', 'manual', '2026-09-08T00:00:00Z', '2026-09-08T00:00:00Z')`).run();
  const HEADER = ['入荷管理番号', '入荷管理行番号', '入荷管理詳細行番号', 'ステータス', '入荷予定日', '入荷受付日',
    '商品ID', '商品名', '予定数', '受付数', '作成日時', '更新日時', 'バーコード'];
  const line = (no, pid, qty) => [ 'AR-IP', no, 1, '受付済', '20260908', '20260908', pid, `商品 ${pid}`, qty, qty, '20260908090000', '20260908090000', '4500000000000'];
  const csv = iconv.encode([HEADER, line(1, 'IP-AMC', 12), line(2, 'IP-OTHER', 99)]
    .map((r) => r.map((v) => `"${v}"`).join(',')).join('\r\n') + '\r\n', 'cp932');
  const imported = importCsv(csv, { source: 'manual_upload', fileName: 'ip.csv' });
  ok(imported.ok, '検査用の入荷受付CSVを取り込めた' + (imported.ok ? '' : ': ' + imported.message));

  const r = await get('/api/inbound-plan');
  ok(r.status === 200 && r.json && r.json.ok, '職員は読める (' + r.status + ')');
  ok(r.json.supplier && r.json.supplier.codes.join() === '0001', '仕入先コード 0001 を返す');
  ok(r.json.rows.length === 1 && r.json.rows[0].product_code === 'IP-AMC',
    '⭐0001 の商品だけ返す (router が仕入先で絞ったものをそのまま渡している)');
  ok(r.json.rows[0].qty === 12 && r.json.rows[0].iroha === '有り' && r.json.rows[0].product_name === '商品 IP-AMC',
    '⭐商品名・数量・いろは在庫化区分がそろっている');
  ok(r.json.batch && r.json.batch.imported_at && r.json.serverNow, 'いつ取り込んだか・サーバーの今の時刻も返る (画面が「きょう」を出すため)');

  // ⭐利用者の iPad (職員モードなし) からも読める — この画面は職員だけのものではない
  const dev = createDevice('検査用 iPad (入荷予定)', 'test');
  sessionRole = null;
  const asDevice = await get('/api/inbound-plan', 'iw_device=' + dev.token);
  ok(asDevice.status === 200 && asDevice.json && asDevice.json.ok, '⭐登録ずみの iPad からも読める (' + asDevice.status + ')');
  const anon = await get('/api/inbound-plan');
  ok(anon.status === 401 || anon.status === 403, '⭐ログインも端末登録もしていなければ出さない (' + anon.status + ')');
  sessionRole = 'admin';
}

console.log(`\n結果: ${pass} PASS / ${fail} FAIL`);
// process.exit で落とすと、開いたままの接続を libuv が abort することがある
// (feedback_notify_job_exit_libuv_crash)。閉じてから終了コードだけ置いて自然に終わらせる
process.exitCode = fail > 0 ? 1 : 0;
server.closeAllConnections?.();
server.close();
