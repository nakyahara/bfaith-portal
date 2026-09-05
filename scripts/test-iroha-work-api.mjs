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
const { getDB, addIrohaWorker, setMetaValue } = await import('../apps/iroha-work/db.js');
const { upsertTaskFromImport } = await import('../apps/iroha-work/tasks-db.js');

// 参照テーブルは本物の init で作る (列名を想像しない)
const { createTables: icCreateTables } = await import('../apps/inbound-check/db.js');
icCreateTables(getDB());

// 正本をアプリにする (いまの本番と同じ経路を通す)
setMetaValue('source_of_truth', 'app');

// ポータルにログイン済みの職員として通す (認証そのものは別のテストの担当)
const app = express();
app.use(express.json());
app.use((req, _res, next) => {
  req.session = { authenticated: true, email: 'test@b-faith.biz', displayName: 'テスト', allowedApps: '*' };
  next();
});
app.use('/apps/iroha-work', router);

const server = await new Promise((resolve) => { const s = app.listen(0, () => resolve(s)); });
const port = server.address().port;
const HOST = `127.0.0.1:${port}`;
const BASE = `http://${HOST}/apps/iroha-work`;

/** 画面と同じヘッダで叩く (checkOrigin を通すため Origin を付ける) */
async function post(pathname, body) {
  const r = await fetch(BASE + pathname, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Origin: `http://${HOST}`, Host: HOST },
    body: JSON.stringify(body),
  });
  return { status: r.status, json: await r.json().catch(() => ({})) };
}
async function get(pathname) {
  const r = await fetch(BASE + pathname, { headers: { Host: HOST } });
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
}

console.log(`\n結果: ${pass} PASS / ${fail} FAIL`);
// process.exit で落とすと、開いたままの接続を libuv が abort することがある
// (feedback_notify_job_exit_libuv_crash)。閉じてから終了コードだけ置いて自然に終わらせる
process.exitCode = fail > 0 ? 1 : 0;
server.closeAllConnections?.();
server.close();
