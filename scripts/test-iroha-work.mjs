/**
 * いろは在庫化作業アプリ — notion-read / service / db のテスト
 *
 * 実行: node scripts/test-iroha-work.mjs
 * Notion API は global.fetch をモックして再現する (実 API は叩かない)。
 *
 * 検証項目 (要件定義 §1.5/§1.7 と Codex設計相談R2 §2 のシナリオ):
 *   1. env 未設定なら fail-closed (エラー文言つきで古いキャッシュ表示)
 *   2. 取得: 未完了は全件 + 棚入完了は期間フィルタ / 「取消」は取らない / 重複ページは1回
 *   3. パース: ステータス未設定 → 未着手 / 名前なし → (名称なし)
 *   4. キャッシュ鮮度: 期間内は再取得しない / force で取り直す
 *   5. 取得失敗: キャッシュは残す + last_refresh_error に記録
 *   6. ページング打ち切り → truncated (黙って欠けさせない)
 *   7. buildList: 作業仕様 (master優先/カード代用) ・ 未登録バッジ ・ 優先度と並び順
 *   8. priorityOf: 欠損をゼロ代用しない (新商品/在庫データなし/販売なし)
 *   9. changeStatus: 成功 (反映確認+キャッシュ更新) / already / conflict / card_gone /
 *      職員ゲート (棚入完了への変更・取り消し) / verify_failed
 *  10. 作業者名簿: 追加・重複・無効化
 *  11. 端末登録: コード発行 → 引き換え → 検証。使用済みコードは再利用不可
 */
import fs from 'fs';
import os from 'os';
import path from 'path';

if (!process.env.DATA_DIR) {
  process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'iroha-work-test-'));
}

let pass = 0, fail = 0;
function ok(cond, label) {
  if (cond) { pass++; console.log(`  ✓ ${label}`); } else { fail++; console.log(`  ✗ ${label}`); }
}

// ─── fetch モック ───
const mock = {
  pages: [],            // Notion 上の全ページ (mkPage で作る)
  pageSize: 2,          // わざと小さくしてページネーションを踏ませる
  queryCalls: 0,
  lastFilters: [],
  patched: [],          // PATCH /pages/:id の記録
  patchStatusOverride: null,   // PATCH 応答のステータスを差し替え (verify_failed 再現)
  missingPages: new Set(),
  failQuery: null,      // { status } query を失敗させる
  onQuery: null,        // query 応答の直前に1回呼ぶフック (取得中の変更レース再現用)
  // ensureCardSchema 用の DB スキーマ (実DBと同じ型。inbound-check テストと同じ)
  dbProps: {
    '名前': { type: 'title' }, 'ステータス': { type: 'select' }, '商品コード': { type: 'rich_text' },
    '数量': { type: 'number' }, '入庫日': { type: 'date' }, '入荷管理番号': { type: 'rich_text' },
    'バーコード': { type: 'rich_text' }, '取引先': { type: 'select' }, '仕入先': { type: 'number' },
    '取扱区分': { type: 'select' }, '作業拠点': { type: 'select' },
    '過去30日販売数': { type: 'rich_text' }, '外部出し目安': { type: 'rich_text' }, '入数': { type: 'number' },
    '資材セットID': { type: 'select' }, '収納容器': { type: 'select' }, '備考': { type: 'rich_text' },
  },
};

function sel(name) { return name == null ? { type: 'select', select: null } : { type: 'select', select: { name } }; }
function rt(s) { return { type: 'rich_text', rich_text: [{ plain_text: s }] }; }

let seq = 0;
function mkPage({ status = '未着手', title = '商品', code = 'PROD-A', qty = 10, arrival = '2026-09-01', props = {} } = {}) {
  const id = `page-${++seq}`;
  const page = {
    id, url: `https://www.notion.so/${id}`, archived: false,
    parent: { database_id: 'testdb' },
    last_edited_time: '2026-09-02T00:00:00.000Z',
    properties: {
      '名前': { type: 'title', title: title == null ? [] : [{ plain_text: title }] },
      'ステータス': sel(status),
      '商品コード': rt(code),
      '数量': { type: 'number', number: qty },
      '入庫日': { type: 'date', date: { start: arrival } },
      ...props,
    },
  };
  mock.pages.push(page);
  return page;
}

const statusOf = (p) => p.properties['ステータス']?.select?.name || null;

global.fetch = async (url, opts = {}) => {
  const method = opts.method || 'GET';
  const body = opts.body ? JSON.parse(opts.body) : null;
  const u = String(url);
  const respond = (status, obj) => ({ ok: status < 300, status, headers: { get: () => null }, json: async () => obj });

  if (u.endsWith('/databases/testdb') && method === 'GET') {
    return respond(200, { object: 'database', properties: JSON.parse(JSON.stringify(mock.dbProps)) });
  }
  if (u.endsWith('/databases/testdb') && method === 'PATCH') {
    for (const [k, cfg] of Object.entries(body.properties || {})) mock.dbProps[k] = { type: Object.keys(cfg)[0] };
    return respond(200, { object: 'database' });
  }
  if (u.endsWith('/databases/testdb/query') && method === 'POST') {
    mock.queryCalls++;
    mock.lastFilters.push(body?.filter || null);
    if (mock.failQuery) return respond(mock.failQuery.status, { object: 'error', message: 'query failed' });
    // レース再現: 応答内容を先にスナップショットしてからフックを走らせる
    // (「取得はもう始まっていたのに、その間にアプリでステータスが変わった」を作る)
    const snapshot = JSON.parse(JSON.stringify(mock.pages));
    if (mock.onQuery) { const h = mock.onQuery; mock.onQuery = null; await h(); }
    const f = body?.filter || {};
    const conds = f.and || [f];
    const stOfSnap = (p) => p.properties['ステータス']?.select?.name || null;
    let hits = snapshot.filter(p => !p.archived);
    for (const c of conds) {
      if (c.property === 'ステータス' && c.select?.does_not_equal) hits = hits.filter(p => stOfSnap(p) !== c.select.does_not_equal);
      if (c.property === 'ステータス' && c.select?.equals) hits = hits.filter(p => stOfSnap(p) === c.select.equals);
      // timestamp フィルタはモックでは素通し (期間の値は lastFilters で検証する)
    }
    const start = Number(body.start_cursor || 0);
    const items = hits.slice(start, start + mock.pageSize);
    const hasMore = start + mock.pageSize < hits.length;
    return respond(200, { results: items, has_more: hasMore, next_cursor: hasMore ? String(start + mock.pageSize) : null });
  }
  const pageGet = u.match(/\/pages\/([^/]+)$/);
  if (pageGet && method === 'GET') {
    const id = pageGet[1];
    if (mock.missingPages.has(id)) return respond(404, { object: 'error', message: 'not found' });
    const p = mock.pages.find(x => x.id === id);
    if (!p) return respond(404, { object: 'error', message: 'not found' });
    return respond(200, p);
  }
  if (pageGet && method === 'PATCH') {
    const id = pageGet[1];
    const p = mock.pages.find(x => x.id === id);
    if (!p) return respond(404, { object: 'error', message: 'not found' });
    mock.patched.push({ id, body });
    const name = body.properties?.['ステータス']?.select?.name;
    if (name) p.properties['ステータス'] = sel(mock.patchStatusOverride || name);
    p.last_edited_time = '2026-09-02T01:00:00.000Z';
    return respond(200, p);
  }
  return respond(500, { object: 'error', message: `unexpected ${method} ${u}` });
};

// ─── セットアップ ───
console.log('DATA_DIR =', process.env.DATA_DIR);
delete process.env.NOTION_TOKEN;
delete process.env.INBOUND_CHECK_NOTION_DB_ID;

const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const { getDB, replaceCache, listCache, addIrohaWorker, setIrohaWorkerActive, getIrohaWorker, listIrohaWorkers,
  setWorkerPin, verifyWorkerPin, _clearPinFails,
  createEnrollCode, redeemEnrollCode, verifyDevice, logEvent, listEvents,
  startSession, stopSession, activeSessionsByPage, estimateByProduct, voidSession, listSessionsForAdmin } = await import('../apps/iroha-work/db.js');
const { ensureFresh, refreshFromNotion, changeStatus, fetchCardLive, parsePage, STATUSES, cacheStatsForAdmin } = await import('../apps/iroha-work/notion-read.js');
const { buildList, priorityOf, clearEnrichCache } = await import('../apps/iroha-work/service.js');

const db = getDB();

console.log('\n[1] env 未設定なら fail-closed');
{
  const r = await ensureFresh();
  ok(r.fresh === false && /未設定/.test(r.error || ''), '未設定エラーの文言を返す');
  ok(mock.queryCalls === 0, 'API は呼ばない');
}
process.env.NOTION_TOKEN = 'test-token';
process.env.INBOUND_CHECK_NOTION_DB_ID = 'testdb';

console.log('\n[2] 取得とキャッシュ全置換');
const pA = mkPage({ status: '未着手', title: '商品A', code: 'PROD-A', qty: 120, props: { '台帳キー': rt('d1-abc'), 'バーコード': rt('4501234567890'), '有効期限': rt('2027-06') } });
const pB = mkPage({ status: '作業中', title: '商品B', code: 'PROD-B', qty: 48 });
const pNew = mkPage({ status: null, title: null, code: 'PROD-NEW', qty: 60, props: { '資材セットID': sel('D-8'), '収納容器': sel('20Lコンテナ'), '入数': { type: 'number', number: 180 } } });
const pDone = mkPage({ status: '棚入完了', title: '商品D', code: 'PROD-D', qty: 40 });
mkPage({ status: '取消', title: '取消済み', code: 'PROD-X', qty: 1 });
{
  const r = await refreshFromNotion();
  ok(r.count === 4, `取消を除く4枚 (実際 ${r.count})`);
  const rows = listCache();
  ok(rows.length === 4, 'キャッシュも4行');
  ok(!rows.some(x => x.status === '取消'), '「取消」は取らない');
  ok(rows.find(x => x.page_id === pDone.id), '棚入完了 (期間内) は入る');
  const doneFilter = mock.lastFilters.find(f => f?.and?.some(c => c.select?.equals === '棚入完了'));
  ok(doneFilter && doneFilter.and.some(c => c.timestamp === 'last_edited_time' && c.last_edited_time?.on_or_after), '棚入完了は期間フィルタつき');
}

console.log('\n[3] パース');
{
  const row = listCache().find(x => x.page_id === pNew.id);
  ok(row.status === '未着手', 'ステータス未設定 → 未着手扱い');
  ok(row.title === '(名称なし)', '名前なし → (名称なし)');
  const parsed = parsePage(pA);
  ok(parsed.props['バーコード'] === '4501234567890' && parsed.props['有効期限'] === '2027-06', 'rich_text プロパティを素の値に');
  ok(parsed.dedupeKey === 'd1-abc', '台帳キーを拾う');
}

console.log('\n[4] キャッシュ鮮度');
{
  await ensureFresh({ force: true });   // last_attempt_at を打つ (鮮度は「最後に試みた時刻」で判定)
  const calls = mock.queryCalls;
  const r = await ensureFresh();
  ok(r.fresh === true && mock.queryCalls === calls, '期間内は再取得しない');
  await ensureFresh({ force: true });
  ok(mock.queryCalls > calls, 'force は取り直す');
}

console.log('\n[5] 取得失敗はキャッシュ温存 + エラー記録');
{
  mock.failQuery = { status: 400 };
  const r = await ensureFresh({ force: true });
  mock.failQuery = null;
  ok(r.fresh === false && r.error, 'エラーを返す');
  ok(listCache().length === 4, 'キャッシュは消えない');
  ok(cacheStatsForAdmin().lastRefreshError, '管理画面用にも残る');
}

console.log('\n[6] ページング打ち切り → truncated + 古い完全キャッシュを守る');
{
  const before = mock.pages.length;
  const beforeRows = listCache().length;
  for (let i = 0; i < 25; i++) mkPage({ title: `量産${i}`, code: `BULK-${i}` });
  const r = await refreshFromNotion();
  ok(r.truncated === true, '上限超えで truncated');
  ok(cacheStatsForAdmin().truncated === true, 'meta にも残る');
  ok(listCache().length === beforeRows, '部分データでキャッシュを置き換えない (見えていたカードを消さない)');
  mock.pages.length = before;   // 量産分を戻す
  seq = before;
  const r2 = await refreshFromNotion();
  ok(r2.truncated === false && cacheStatsForAdmin().truncated === false, '収まれば解除される');
  ok(listCache().length === beforeRows, '収まったら通常の全置換に戻る');
}

console.log('\n[7] buildList: 作業仕様・優先度・並び');
{
  // 参照テーブルは本物の init で作る (列名を想像しない — 2026-09-02 supplier_name の教訓)
  const { createTables: icCreateTables } = await import('../apps/inbound-check/db.js');
  icCreateTables(db);
  db.prepare(`INSERT INTO f_iroha_work_master (code_key, 商品コード, material_code, storage_container, units_per_container, process_count, note, version, updated_at)
    VALUES ('prod-a', 'PROD-A', 'D-8', '20Lコンテナ', 180, 3, '割れ注意', 1, ?)`).run(new Date().toISOString());
  const today = new Date().toISOString().slice(0, 10);
  db.prepare(`INSERT INTO mirror_sales_daily (日付, 商品コード, モール, 数量, データ種別, チャネル, updated_at)
    VALUES (?, 'PROD-A', 'rakuten', 30, 'by_product', '', ?)`).run(today, today);
  db.prepare(`INSERT INTO mirror_logizard_stock (商品ID, 品質区分名, 在庫数, 引当数, captured_at, synced_at)
    VALUES ('PROD-A', '良品', 3, 1, ?, ?)`).run(today, today);
  db.prepare(`INSERT INTO mirror_logizard_stock (商品ID, 品質区分名, 在庫数, 引当数, captured_at, synced_at)
    VALUES ('PROD-B', '良品', 500, 0, ?, ?)`).run(today, today);
  db.prepare(`INSERT INTO mirror_sales_daily (日付, 商品コード, モール, 数量, データ種別, チャネル, updated_at)
    VALUES (?, 'PROD-B', 'rakuten', 15, 'by_product', '', ?)`).run(today, today);
  clearEnrichCache();

  const { cards } = buildList();
  ok(cards.length === 4, `4枚 (実際 ${cards.length})`);
  const a = cards.find(c => c.product_code === 'PROD-A');
  ok(a.master.source === 'master' && a.master.material_code === 'D-8' && a.master.missing.length === 0,
    '作業仕様はマスタ優先・未登録なし');
  ok(a.priority.kind === 'urgent' && /残り2\.0日分/.test(a.priority.label), `急ぎ (フリー2個÷日販1個 = ${a.priority.label})`);
  const n = cards.find(c => c.product_code === 'PROD-NEW');
  ok(n.priority.kind === 'new' && n.priority.label === '新商品', '販売も在庫も無し → 新商品');
  ok(n.master.source === 'card' && n.master.material_code === 'D-8' && n.master.units_per_container === 180,
    'マスタ未登録はカード作成時の値で代用 (source=card)');
  ok(n.master.missing.includes('工程'), 'カードにも無い項目は未登録扱い');
  const b = cards.find(c => c.product_code === 'PROD-B');
  ok(b.priority.kind === 'normal' && /残り/.test(b.priority.label), '在庫潤沢は normal + 残り日数');
  ok(cards[0].product_code === 'PROD-A', '並び: 急ぎが先頭');
  ok(cards.indexOf(n) < cards.indexOf(b), '新商品は通常より上');
}

console.log('\n[8] priorityOf: 欠損をゼロ代用しない');
{
  ok(priorityOf(null, null).kind === 'new', '両方なし = 新商品');
  ok(priorityOf(30, null).kind === 'unknown' && priorityOf(30, null).label === '在庫データなし', '在庫欠損は「在庫データなし」');
  ok(priorityOf(0, 100).kind === 'calm' && priorityOf(null, 100).kind === 'calm', '販売なしは calm (在庫だけあっても急がない)');
  ok(priorityOf(30, 0).kind === 'urgent' && priorityOf(30, 0).label === '在庫切れ', '在庫0×販売あり = 在庫切れ');
  ok(priorityOf(30, 300).kind === 'normal', '300日分は normal');
}

console.log('\n[9] changeStatus');
{
  const r = await changeStatus({ pageId: pA.id, to: '作業中', expect: '未着手', isStaff: false });
  ok(r.ok === true && r.status === '作業中', '成功 + 反映確認');
  ok(listCache().find(x => x.page_id === pA.id).status === '作業中', 'キャッシュも更新');
  ok(mock.patched.some(p => p.id === pA.id), 'PATCH が飛んでいる');

  const r2 = await changeStatus({ pageId: pA.id, to: '作業中', expect: '未着手', isStaff: false });
  ok(r2.ok === true && r2.already === true, '既に目的の状態なら成功扱い (二重タップ・他端末)');

  const r3 = await changeStatus({ pageId: pA.id, to: '中断', expect: '未着手', isStaff: false });
  ok(r3.ok === false && r3.error === 'conflict' && r3.current === '作業中', '期待と違えば conflict + 今の値');
  ok(/Notion 側で/.test(r3.message), '競合の文言 (最新を表示します)');

  const r4 = await changeStatus({ pageId: pA.id, to: '棚入完了', expect: '作業中', isStaff: false });
  ok(r4.ok === false && r4.error === 'staff_required', '棚入完了への変更は職員のみ');
  const r5 = await changeStatus({ pageId: pA.id, to: '棚入完了', expect: '作業中', isStaff: true });
  ok(r5.ok === true, '職員なら棚入完了にできる');
  const r6 = await changeStatus({ pageId: pA.id, to: '作業中', expect: '棚入完了', isStaff: false });
  ok(r6.ok === false && r6.error === 'staff_required', '棚入完了からの取り消しも職員のみ');

  mock.missingPages.add(pB.id);
  const r7 = await changeStatus({ pageId: pB.id, to: '作業中', expect: '作業中', isStaff: false });
  ok(r7.ok === false && r7.error === 'card_gone', '404 は card_gone');
  ok(!listCache().some(x => x.page_id === pB.id), '消えたカードはキャッシュから外す');
  mock.missingPages.delete(pB.id);

  pDone.archived = true;
  const r8 = await changeStatus({ pageId: pDone.id, to: '作業中', expect: '棚入完了', isStaff: true });
  ok(r8.ok === false && r8.error === 'card_gone', 'アーカイブ済みも card_gone');
  pDone.archived = false;

  mock.patchStatusOverride = '未着手';
  const r9 = await changeStatus({ pageId: pNew.id, to: '作業中', expect: '未着手', isStaff: false });
  mock.patchStatusOverride = null;
  ok(r9.ok === false && r9.error === 'verify_failed', 'PATCH 応答の値が違えば verify_failed (HTTP成功を信じない)');

  const bad = await changeStatus({ pageId: pA.id, to: '完成', expect: '作業中', isStaff: true });
  ok(bad.ok === false && bad.error === 'bad_status', '未知のステータスへの変更は拒否');

  const noExpect = await changeStatus({ pageId: pA.id, to: '中断', expect: null, isStaff: true });
  ok(noExpect.ok === false && noExpect.error === 'bad_request', 'expect 省略は拒否 (競合検出を素通りさせない)');

  // ステータスが select 型でない (人が status 型に作り替えた等) → 明確に止める
  const pTyped = mkPage({ status: '未着手', title: '型違い', code: 'PROD-T' });
  pTyped.properties['ステータス'] = { type: 'status', status: { name: '未着手' } };
  const rT = await changeStatus({ pageId: pTyped.id, to: '作業中', expect: '未着手', isStaff: false });
  ok(rT.ok === false && rT.error === 'schema_mismatch', 'select 型でなければ schema_mismatch');

  // 別 DB のページ ID を渡されても書き換えない (同じインテグレーションが触れる他DBの防御)
  const pForeign = mkPage({ status: '未着手', title: 'よそのDB', code: 'PROD-F' });
  pForeign.parent = { database_id: 'other-db' };
  const rF = await changeStatus({ pageId: pForeign.id, to: '作業中', expect: '未着手', isStaff: true });
  ok(rF.ok === false && rF.error === 'wrong_database', '対象DB外のページは拒否');
  ok(!mock.patched.some(p => p.id === pForeign.id), 'PATCH 自体を送らない');
}

console.log('\n[9b] 同時変更はページ単位で直列化 (後勝ち消失を防ぐ)');
{
  const pC = mkPage({ status: '未着手', title: '同時', code: 'PROD-C' });
  const [r1, r2] = await Promise.all([
    changeStatus({ pageId: pC.id, to: '作業中', expect: '未着手', isStaff: false }),
    changeStatus({ pageId: pC.id, to: '中断', expect: '未着手', isStaff: false }),
  ]);
  const oks = [r1, r2].filter(x => x.ok);
  const conflicts = [r1, r2].filter(x => x.error === 'conflict');
  ok(oks.length === 1 && conflicts.length === 1, `片方だけ成功し、もう片方は競合 (${r1.error || 'ok'}/${r2.error || 'ok'})`);
  ok(statusOf(pC) === oks[0].status, '成功した方の値が Notion に残る (黙って上書きされない)');
}

console.log('\n[9c] 取得中のステータス変更を全置換で巻き戻さない');
{
  const pR = mkPage({ status: '未着手', title: 'レース', code: 'PROD-R' });
  await refreshFromNotion();   // まずキャッシュに入れる
  // 全体取得の query 応答スナップショット後 (=取得開始後) にアプリ経由で変更が走るレース
  mock.onQuery = async () => {
    const r = await changeStatus({ pageId: pR.id, to: '作業中', expect: '未着手', isStaff: false });
    if (!r.ok) throw new Error('レース用の変更が失敗: ' + r.error);
  };
  await refreshFromNotion();
  const row = listCache().find(x => x.page_id === pR.id);
  ok(row && row.status === '作業中', '古い取得結果がキャッシュ全置換で変更を巻き戻さない');
}

console.log('\n[9d] 取得中の 棚入完了→未完了 で行が消えない (R2 #1)');
{
  // 棚入完了のカードをキャッシュに入れておく
  const pD = mkPage({ status: '棚入完了', title: '完了から戻す', code: 'PROD-D2' });
  await refreshFromNotion();
  ok(listCache().some(x => x.page_id === pD.id), '前提: 完了カードがキャッシュにある');
  // 未完了クエリのスナップショット後 (=このカードは含まれない) に 棚入完了→作業中 へ変更。
  // 完了クエリ時点ではもう作業中なので、どちらのクエリ結果にも入らない
  mock.onQuery = async () => {
    const r = await changeStatus({ pageId: pD.id, to: '作業中', expect: '棚入完了', isStaff: true });
    if (!r.ok) throw new Error('レース用の変更が失敗: ' + r.error);
  };
  await refreshFromNotion();
  const row = listCache().find(x => x.page_id === pD.id);
  ok(!!row, '行ごと消えない (upsert で戻し入れる)');
  ok(row && row.status === '作業中', '変更後のステータスで残る');
}

console.log('\n[10] 作業者名簿');
{
  const a = addIrohaWorker({ displayName: 'たにがわ', workerType: 'staff', actor: 'test' });
  ok(a.ok === true, '職員を追加');
  const m = addIrohaWorker({ displayName: 'さとう', workerType: 'member', actor: 'test' });
  ok(m.ok === true, '利用者を追加');
  ok(addIrohaWorker({ displayName: 'さとう', workerType: 'member', actor: 'test' }).error === 'duplicate', '重複は拒否');
  ok(addIrohaWorker({ displayName: 'x', workerType: 'boss', actor: 'test' }).error === 'bad_type', '区分は member/staff のみ');
  ok(listIrohaWorkers().length === 2, '有効2名');
  setIrohaWorkerActive(m.id, false);
  ok(listIrohaWorkers().length === 1 && listIrohaWorkers(true).length === 2, '無効化は一覧から外れる (履歴は残る)');
  ok(getIrohaWorker(m.id).active === 0, 'getIrohaWorker は無効でも引ける (routerで弾く)');
}

console.log('\n[10b] 職員PIN (worker_id の自己申告を職員権限にしない)');
{
  const staff = listIrohaWorkers(true).find(w => w.worker_type === 'staff');
  const member = listIrohaWorkers(true).find(w => w.worker_type === 'member');
  ok(verifyWorkerPin(staff.id, '1234').error === 'pin_required', 'PIN未設定は職員操作できない (設定を促す)');
  ok(setWorkerPin(member.id, '1234', 'test').error === 'not_staff', '利用者にはPINを設定できない');
  ok(setWorkerPin(staff.id, '12', 'test').error === 'bad_pin', '桁数チェック');
  ok(setWorkerPin(staff.id, '4649', 'test').ok === true, '職員にPIN設定');
  ok(listIrohaWorkers(true).find(w => w.id === staff.id).pin_set === 1, 'pin_set フラグが立つ (ハッシュは出さない)');
  ok(verifyWorkerPin(staff.id, '4649').ok === true, '正しいPINは通る');
  ok(verifyWorkerPin(staff.id, '0000').error === 'pin_invalid', '間違いは弾く');
  for (let i = 0; i < 5; i++) verifyWorkerPin(staff.id, '9999');
  ok(verifyWorkerPin(staff.id, '4649').error === 'pin_locked', '5回失敗でロック (正しいPINでも通さない)');
  _clearPinFails();
  ok(verifyWorkerPin(staff.id, '4649').ok === true, 'ロック解除後は通る');
}

console.log('\n[11] 端末登録');
{
  const { code } = createEnrollCode('いろはiPad 1号機', 'admin@test');
  const r = redeemEnrollCode(code);
  ok(r.ok === true && r.label === 'いろはiPad 1号機', 'コード引き換え成功');
  ok(verifyDevice(r.token)?.label === 'いろはiPad 1号機', 'トークンで端末を検証できる');
  ok(redeemEnrollCode(code).error === 'used', '使用済みコードは再利用不可');
  ok(redeemEnrollCode('000000').ok !== true, 'でたらめなコードは失敗');
  ok(verifyDevice('bogus') === null, 'でたらめなトークンは null');
}

console.log('\n[12] 操作履歴');
{
  logEvent({ action: 'status_change', pageId: pA.id, workerId: 1, workerName: 'たにがわ', deviceLabel: 'test', from: '未着手', to: '作業中', ok: true });
  const ev = listEvents(5);
  ok(ev.length > 0 && ev[0].action === 'status_change' && ev[0].ok === 1, '履歴が残る');
}

console.log('\n[12b] fetchCardLive (作業開始前の実ページ判定)');
{
  const pLive = mkPage({ status: '未着手', title: '開始判定', code: 'PROD-L' });
  const r = await fetchCardLive(pLive.id);
  ok(r.ok === true && r.status === '未着手', '実ページから今の状態を取る');
  ok(listCache().some(x => x.page_id === pLive.id), '取った内容はキャッシュへ upsert される');
  pLive.properties['ステータス'] = { type: 'select', select: { name: '棚入完了' } };
  const r2 = await fetchCardLive(pLive.id);
  ok(r2.ok === true && r2.status === '棚入完了', 'キャッシュが新しくても実ページの変更を見る (開始ゲートの根拠)');
  mock.missingPages.add(pLive.id);
  const r3 = await fetchCardLive(pLive.id);
  ok(r3.ok === false && r3.error === 'card_gone', '削除済みは card_gone (開始拒否)');
  mock.missingPages.delete(pLive.id);
  pLive.parent = { database_id: 'other-db' };
  const r4 = await fetchCardLive(pLive.id);
  ok(r4.ok === false && r4.error === 'wrong_database', '別DBのページは拒否');
  pLive.parent = { database_id: 'testdb' };
}

console.log('\n[13] 作業時間セッション');
{
  const w1 = addIrohaWorker({ displayName: 'やまだ', workerType: 'member', actor: 'test' });
  const w2 = addIrohaWorker({ displayName: 'すずき', workerType: 'member', actor: 'test' });
  const worker1 = getIrohaWorker(w1.id), worker2 = getIrohaWorker(w2.id);

  const s1 = startSession({ pageId: 'sess-p1', productCode: 'PROD-A', title: '商品A', worker: worker1, deviceLabel: 'test' });
  ok(s1.ok === true && s1.startedAt, '開始できる');
  const again = startSession({ pageId: 'sess-p1', worker: worker1 });
  ok(again.ok === true && again.already === true && again.sessionId === s1.sessionId,
    '同じカードの再送は成功扱いで既存セッションを返す (応答消失からの復旧)');
  const busy = startSession({ pageId: 'sess-p2', worker: worker1 });
  ok(busy.error === 'busy' && busy.open.page_id === 'sess-p1', '別カードは busy (どのカードか返す) — 1人1件の原則');

  // 「活動中1件」は DB 制約でも守られている (アプリを迂回した INSERT が弾かれる)
  let uniqErr = null;
  try {
    getDB().prepare(`INSERT INTO f_iroha_work_sessions (page_id, worker_id, worker_name, started_at)
      VALUES ('sess-px', ?, 'x', ?)`).run(worker1.id, new Date().toISOString());
  } catch (e) { uniqErr = e; }
  ok(uniqErr && /UNIQUE/i.test(uniqErr.message), '部分ユニーク索引が二重の活動中セッションを拒否');

  const s2 = startSession({ pageId: 'sess-p1', productCode: 'PROD-A', title: '商品A', worker: worker2 });
  ok(s2.ok === true, '別の人は同じカードに参加できる (複数人=複数行)');
  ok((activeSessionsByPage().get('sess-p1') || []).length === 2, '活動中2名が一覧に出る');

  const st1 = stopSession({ pageId: 'sess-p1', workerId: worker1.id, sessionId: s1.sessionId, reason: 'done' });
  ok(st1.ok === true && st1.session.raw_seconds >= 0 && st1.remainingActive === 1, '終了 (raw_seconds はサーバー計算・残り1名)');
  const w3 = addIrohaWorker({ displayName: 'いとう', workerType: 'member', actor: 'test' });
  ok(stopSession({ pageId: 'sess-p1', workerId: w3.id, sessionId: s2.sessionId, reason: 'done' }).error === 'not_started',
    '他人のセッションIDでは終了できない');
  ok(stopSession({ pageId: 'sess-p1', workerId: worker2.id, sessionId: s2.sessionId, reason: 'bogus' }).error === 'bad_request', '不正な理由は拒否');
  ok(stopSession({ pageId: 'sess-p1', workerId: worker2.id, reason: 'done' }).error === 'bad_request', 'session_id なしは拒否');
  const st2 = stopSession({ pageId: 'sess-p1', workerId: worker2.id, sessionId: s2.sessionId, reason: 'pause' });
  ok(st2.ok === true && st2.remainingActive === 0, '中断で全員離脱 (remainingActive=0 → 画面が「中断にする?」を出す)');
  const st2again = stopSession({ pageId: 'sess-p1', workerId: worker2.id, sessionId: s2.sessionId, reason: 'pause' });
  ok(st2again.ok === true && st2again.already === true && st2again.session.id === st2.session.id,
    '同じセッションIDの再送は成功扱い — 冪等');

  // 遅延再送が「後から始めた新しいセッション」を誤終了しない (Codex PR2-R2 P2)
  const sB = startSession({ pageId: 'sess-p1', productCode: 'PROD-A', title: '商品A', worker: worker2 });
  const stale = stopSession({ pageId: 'sess-p1', workerId: worker2.id, sessionId: s2.sessionId, reason: 'pause' });
  ok(stale.ok === true && stale.already === true, '古いIDの再送は already で返る');
  ok((activeSessionsByPage().get('sess-p1') || []).some(a => a.id === sB.sessionId), '新しいセッションは終了されず動き続ける');
  stopSession({ pageId: 'sess-p1', workerId: worker2.id, sessionId: sB.sessionId, reason: 'done' });

  // 実測の集計: カード単位の合計を商品ごとに平均。voided は外す
  const db2 = getDB();
  const insSess = db2.prepare(`INSERT INTO f_iroha_work_sessions
    (page_id, product_code, worker_id, worker_name, started_at, ended_at, end_reason, raw_seconds)
    VALUES (?, 'PROD-EST', 1, 'x', ?, ?, 'done', ?)`);
  insSess.run('est-p1', '2026-09-01T00:00:00Z', '2026-09-01T01:00:00Z', 300);
  insSess.run('est-p1', '2026-09-01T00:00:00Z', '2026-09-01T01:00:00Z', 300);   // 同カード2人 → 合計600
  insSess.run('est-p2', '2026-09-02T00:00:00Z', '2026-09-02T01:00:00Z', 1200);
  const est = estimateByProduct().get('prod-est');
  ok(est && est.cards === 2 && est.avgSeconds === 900, `カード合計の平均 (600+1200)/2=900 (実際 ${est && est.avgSeconds})`);
  ok(est.lastSeconds === 1200, '直近カードの実績も持つ (1回だけなら「前回」表示に使う)');

  // 取り消し (論理削除) → 集計から外れる
  const openS = startSession({ pageId: 'est-p3', productCode: 'PROD-EST', title: 'x', worker: worker1 });
  const v = voidSession(getDB().prepare('SELECT id FROM f_iroha_work_sessions WHERE page_id = ?').get('est-p3').id, 'admin@test', '押し忘れ');
  ok(openS.ok && v.ok === true, '活動中セッションも取り消せる (閉じて void)');
  ok(voidSession(getDB().prepare('SELECT id FROM f_iroha_work_sessions WHERE page_id = ?').get('est-p3').id, 'admin@test').error === 'already_voided', '二重取り消しは拒否');
  ok((activeSessionsByPage().get('est-p3') || []).length === 0, '取り消したら活動中から消える');
  const rows = listSessionsForAdmin(10);
  ok(rows.length > 0 && typeof rows[0].elapsed_seconds === 'number', '管理画面用一覧 (経過秒つき)');

  // 活動中は件数制限に埋もれない (終了忘れの導線を失わない)
  const oldOpen = startSession({ pageId: 'old-open', productCode: 'X', title: '古い作業中', worker: worker2 });
  const insClosed = getDB().prepare(`INSERT INTO f_iroha_work_sessions
    (page_id, product_code, worker_id, worker_name, started_at, ended_at, end_reason, raw_seconds)
    VALUES (?, 'X', 99, 'x', ?, ?, 'done', 60)`);
  for (let i = 0; i < 15; i++) insClosed.run('bulk-' + i, new Date().toISOString(), new Date().toISOString());
  const rows2 = listSessionsForAdmin(10);
  ok(oldOpen.ok && rows2.some(r => r.page_id === 'old-open' && !r.ended_at),
    '終了済みが増えても活動中セッションは管理一覧に必ず出る');
  stopSession({ pageId: 'old-open', workerId: worker2.id, sessionId: oldOpen.sessionId, reason: 'done' });
}

console.log('\n[14] 完成写真・動画 (outbox → Drive → Notion)');
{
  process.env.IROHA_WORK_DRIVE_FOLDER_ID = 'folder-test';
  const { addMedia, sniffKind, countActivePhotos, softDeleteMedia, resetMedia, processMediaQueue,
    mediaByPage, listMediaForAdmin, _setDriveUpload, MEDIA_DIR } = await import('../apps/iroha-work/media.js');
  const worker1 = listIrohaWorkers(true).find(w => w.display_name === 'やまだ');
  const worker2 = listIrohaWorkers(true).find(w => w.display_name === 'すずき');
  const pMedia = mkPage({ status: '作業中', title: '写真対象', code: 'PROD-M' });

  const jpeg = Buffer.concat([Buffer.from([0xFF, 0xD8, 0xFF, 0xE0]), Buffer.alloc(64, 1)]);
  const mp4 = Buffer.concat([Buffer.from([0, 0, 0, 24]), Buffer.from('ftypisom'), Buffer.alloc(64, 2)]);
  ok(sniffKind(jpeg) === 'photo' && sniffKind(mp4) === 'video' && sniffKind(Buffer.alloc(20)) === null,
    'マジックバイト判定 (JPEG/MP4/不明)');

  const tmp = (name, buf) => { const p = path.join(process.env.DATA_DIR, name); fs.writeFileSync(p, buf); return p; };
  const a1 = addMedia({ pageId: pMedia.id, productCode: 'PROD-M', kind: 'photo', mime: 'image/jpeg',
    filePath: tmp('a1.jpg', jpeg), worker: worker1, operationId: 'op-photo-0001' });
  ok(a1.ok === true && a1.media.status === 'stored', '受信 → stored (即応答できる形)');
  ok(fs.existsSync(path.join(MEDIA_DIR, 'op-photo-0001.jpg')), '実体は MEDIA_DIR へ移動');
  const a1b = addMedia({ pageId: pMedia.id, kind: 'photo', filePath: tmp('a1b.jpg', jpeg), worker: worker1, operationId: 'op-photo-0001' });
  ok(a1b.ok === true && a1b.already === true, '同じ operation_id の再送は既存を返す (二重登録しない)');
  ok(addMedia({ pageId: pMedia.id, kind: 'photo', filePath: tmp('bad.jpg', mp4), worker: worker1, operationId: 'op-bad-000001' }).error === 'bad_file',
    '中身が写真でなければ拒否 (Content-Type を信じない)');
  addMedia({ pageId: pMedia.id, kind: 'photo', filePath: tmp('a2.jpg', jpeg), worker: worker1, operationId: 'op-photo-0002' });
  addMedia({ pageId: pMedia.id, kind: 'photo', filePath: tmp('a3.jpg', jpeg), worker: worker2, operationId: 'op-photo-0003' });
  ok(addMedia({ pageId: pMedia.id, kind: 'photo', filePath: tmp('a4.jpg', jpeg), worker: worker1, operationId: 'op-photo-0004' }).error === 'cap_reached',
    '写真は3枚まで');
  const v1 = addMedia({ pageId: pMedia.id, kind: 'video', mime: 'video/mp4', filePath: tmp('v1.mp4', mp4), worker: worker1, operationId: 'op-video-0001' });
  ok(v1.ok === true, '動画も受信できる');
  ok(countActivePhotos(pMedia.id) === 3, 'countActivePhotos = 3 (棚入完了ゲートの根拠)');

  // キュー: 1回目は Drive 失敗 → next_retry が付く。再実行 (reset) 後に成功 → Notion まで反映
  let driveCalls = 0;
  _setDriveUpload(async () => { driveCalls++; if (driveCalls <= 1) throw new Error('drive down'); return { fileId: 'f' + driveCalls, url: 'https://drive.google.com/file/d/f' + driveCalls + '/view' }; });
  await processMediaQueue();
  let rows = listMediaForAdmin(10).filter(m => m.page_id === pMedia.id);
  ok(rows.some(m => m.error && m.next_retry_at), '失敗は next_retry 付きで記録 (すぐ連打しない)');
  for (const m of rows) if (m.error) resetMedia(m.id);
  await new Promise(r => setTimeout(r, 30));   // resetMedia の schedule 分を待つ
  await processMediaQueue();
  await processMediaQueue();   // 2巡目で残りの upload + Notion 反映
  rows = listMediaForAdmin(10).filter(m => m.page_id === pMedia.id && !m.deleted_at);
  ok(rows.every(m => m.status === 'synced' && m.drive_url), `全件 Drive→Notion まで反映 (実際: ${rows.map(m => m.status).join(',')})`);
  ok(!fs.existsSync(path.join(MEDIA_DIR, 'op-photo-0001.jpg')), 'アップロード後に実体を削除');
  const patched = mock.patched.filter(p => p.id === pMedia.id && p.body.properties?.['完成写真']);
  ok(patched.length > 0 && patched[patched.length - 1].body.properties['完成写真'].files.length === 4,
    'Notion「完成写真」に4件 (写真3+動画1) が付く');

  // 論理削除: 他人は消せない / 本人は消せる / Notion から外れる
  const target = rows.find(m => m.operation_id === 'op-photo-0003');
  ok(softDeleteMedia(target.id, { workerId: worker1.id }).error === 'forbidden', '他人の写真は消せない');
  ok(softDeleteMedia(target.id, { workerId: worker2.id }).ok === true, '本人は消せる (論理削除)');
  ok(countActivePhotos(pMedia.id) === 2, '削除後は2枚');
  await new Promise(r => setTimeout(r, 30));
  await processMediaQueue();
  const last = mock.patched.filter(p => p.id === pMedia.id && p.body.properties?.['完成写真']).pop();
  ok(last.body.properties['完成写真'].files.length === 3, 'Notion 側も貼り直されて3件 (写真2+動画1)');
  ok((mediaByPage().get(pMedia.id) || []).length === 3, '画面用一覧にも削除分は出ない');
  _setDriveUpload(null);
}

console.log(`\n結果: ${pass} PASS / ${fail} FAIL`);
process.exit(fail > 0 ? 1 : 0);
