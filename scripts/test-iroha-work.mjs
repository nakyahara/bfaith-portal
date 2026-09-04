/**
 * いろは在庫化作業アプリ — notion-read / service / db のテスト
 *
 * 実行: node scripts/test-iroha-work.mjs
 * Notion API は global.fetch をモックして再現する (実 API は叩かない)。
 *
 * 検証項目 (要件定義 §1.5/§1.7 と Codex設計相談R2 §2 のシナリオ):
 *   1. env 未設定なら fail-closed (エラー文言つきで古いキャッシュ表示)
 *   2. 取得: 未完了は全件 / 棚入完了・取消・在庫化対象外・作業完了は取らない / 重複ページは1回
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
  dbPatches: 0,         // PATCH /databases/:id の回数 (移行の調査が Notion を書き換えないことの検証)
  lastSorts: null,      // query の sorts (移行の取得順の検証)
  missingPages: new Set(),
  failQuery: null,      // { status } query を失敗させる
  // 実DB「ステータス」の選択肢 (2026-09-03 実機の Available options)。「取消」も「中断」も無い。
  // 実 Notion は DB に無い選択肢名でフィルタすると 400 を返す (キャッシュ 0 枚の実機障害の原因)
  statusOptions: ['未着手', '作業完了', '棚入完了', '在庫化対象外', '資材不足で作業中断', '作業中', '次回',
    '羅針盤', 'ワークセンター', 'ジョブサポ', 'リハス', 'いろは'],
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

// 実 API の GET /databases は select プロパティに選択肢一覧 (select.options[].name) を含む
mock.dbProps['ステータス'].select = { options: mock.statusOptions.map(name => ({ name })) };

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
    mock.dbPatches++;
    for (const [k, cfg] of Object.entries(body.properties || {})) mock.dbProps[k] = { type: Object.keys(cfg)[0] };
    return respond(200, { object: 'database' });
  }
  if (u.endsWith('/databases/testdb/query') && method === 'POST') {
    mock.queryCalls++;
    mock.lastFilters.push(body?.filter || null);
    mock.lastSorts = body?.sorts || null;
    if (mock.failQuery) return respond(mock.failQuery.status, { object: 'error', message: 'query failed' });
    // レース再現: 応答内容を先にスナップショットしてからフックを走らせる
    // (「取得はもう始まっていたのに、その間にアプリでステータスが変わった」を作る)
    const snapshot = JSON.parse(JSON.stringify(mock.pages));
    if (mock.onQuery) { const h = mock.onQuery; mock.onQuery = null; await h(); }
    const f = body?.filter || {};
    const conds = f.and || [f];
    // 実 Notion の挙動: DB に無い選択肢名を select フィルタに使うと 400 (2026-09-03 実機で判明)
    for (const c of conds) {
      const v = c.property === 'ステータス' ? (c.select?.equals ?? c.select?.does_not_equal) : null;
      if (v && !mock.statusOptions.includes(v)) {
        return respond(400, { object: 'error', code: 'validation_error',
          message: `select option "${v}" not found for property "ステータス". Available options: ${mock.statusOptions.map(o => `"${o}"`).join(', ')}.` });
      }
    }
    const stOfSnap = (p) => p.properties['ステータス']?.select?.name || null;
    let hits = snapshot.filter(p => !p.archived);
    for (const c of conds) {
      if (c.property === 'ステータス' && c.select?.does_not_equal) hits = hits.filter(p => stOfSnap(p) !== c.select.does_not_equal);
      if (c.property === 'ステータス' && c.select?.equals) hits = hits.filter(p => stOfSnap(p) === c.select.equals);
      // last_edited_time の窓 (移行の差分取込): 実 Notion と同じく境界を含む
      if (c.timestamp === 'last_edited_time' && c.last_edited_time) {
        const le = c.last_edited_time;
        hits = hits.filter(p => (!le.on_or_after || p.last_edited_time >= le.on_or_after) && (!le.on_or_before || p.last_edited_time <= le.on_or_before));
      }
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
    // 応答前に1回だけ呼ぶフック (PATCH 中に別リクエストが割り込むレースの再現)
    if (mock.onPagePatch) { const h = mock.onPagePatch; mock.onPagePatch = null; await h(); }
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
  startSession, stopSession, activeSessionsByPage, activeSessionsByTask, estimateByProduct, getMeta, setMetaValue, voidSession, listSessionsForAdmin } = await import('../apps/iroha-work/db.js');
const { ensureFresh, refreshFromNotion, changeStatus, fetchCardLive, parsePage, STATUSES, cacheStatsForAdmin } = await import('../apps/iroha-work/notion-read.js');
const { buildList, priorityOf, clearEnrichCache, previousPhotosOf } = await import('../apps/iroha-work/service.js');

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
mkPage({ status: '在庫化対象外', title: '対象外', code: 'PROD-Y', qty: 1 });
mkPage({ status: '作業完了', title: '作業は終わった', code: 'PROD-Z', qty: 1 });
{
  const r = await refreshFromNotion();
  ok(r.count === 3, `棚入完了・取消・在庫化対象外・作業完了を除く3枚 (実際 ${r.count})`);
  const rows = listCache();
  ok(rows.length === 3, 'キャッシュも3行');
  ok(!rows.some(x => ['棚入完了', '取消', '在庫化対象外', '作業完了'].includes(x.status)), '「棚入完了」「取消」「在庫化対象外」「作業完了」は取らない (中原さん 2026-09-03)');
  ok(!rows.find(x => x.page_id === pDone.id), '棚入完了のカードは入らない');
  ok(!mock.lastFilters.some(f => f?.and?.some(c => c.select?.equals === '棚入完了')), '棚入完了を別クエリで引かない');
  const activeFilter = mock.lastFilters.find(f => JSON.stringify(f || {}).includes('在庫化対象外'));
  ok(activeFilter && JSON.stringify(activeFilter).includes('作業完了'), 'DB に実在する除外ステータスは Notion 側のフィルタで落とす (全件を引かない)');
  ok(!mock.lastFilters.some(f => JSON.stringify(f || {}).includes('取消')),
    '「取消」をフィルタに使わない (DB に無い選択肢名は Notion が 400 を返す — 2026-09-03 実機障害)');
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
  ok(listCache().length === 3, 'キャッシュは消えない');
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
  ok(cards.length === 3, `3枚 (棚入完了は一覧に出ない。実際 ${cards.length})`);
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
  ok(r5.listed === false && !listCache().some(x => x.page_id === pA.id), '棚入完了にしたカードは一覧 (キャッシュ) から外れる (listed=false)');
  const r6 = await changeStatus({ pageId: pA.id, to: '作業中', expect: '棚入完了', isStaff: false });
  ok(r6.ok === false && r6.error === 'staff_required', '棚入完了からの取り消しも職員のみ');
  const r6b = await changeStatus({ pageId: pA.id, to: '作業中', expect: '棚入完了', isStaff: true });
  ok(r6b.ok === true && r6b.listed === true && listCache().some(x => x.page_id === pA.id), '職員が棚入完了から戻すと一覧に再び現れる (listed=true)');

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
  // 棚入完了のカードは取り込まない (キャッシュに無い) — そこから作業中へ戻すと行が現れる
  const pD = mkPage({ status: '棚入完了', title: '完了から戻す', code: 'PROD-D2' });
  await refreshFromNotion();
  ok(!listCache().some(x => x.page_id === pD.id), '前提: 棚入完了のカードはキャッシュに無い (取り込まない)');
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
  const { addMedia, sniffKind, countActiveMedia, photosByCodeKey, softDeleteMedia, resetMedia, processMediaQueue,
    mediaByPage, listMediaForAdmin, _setDriveUpload, MEDIA_DIR } = await import('../apps/iroha-work/media.js');
  const worker1 = listIrohaWorkers(true).find(w => w.display_name === 'やまだ');
  const worker2 = listIrohaWorkers(true).find(w => w.display_name === 'すずき');
  const pMedia = mkPage({ status: '作業中', title: '写真対象', code: 'PROD-M' });

  const jpeg = Buffer.concat([Buffer.from([0xFF, 0xD8, 0xFF, 0xE0]), Buffer.alloc(64, 1)]);
  const mp4 = Buffer.concat([Buffer.from([0, 0, 0, 24]), Buffer.from('ftypisom'), Buffer.alloc(64, 2)]);
  const heic = Buffer.concat([Buffer.from([0, 0, 0, 24]), Buffer.from('ftypheic'), Buffer.alloc(64, 3)]);
  ok(sniffKind(jpeg) === 'photo' && sniffKind(mp4) === 'video' && sniffKind(Buffer.alloc(20)) === null,
    'マジックバイト判定 (JPEG/MP4/不明)');
  ok(sniffKind(heic) === null, 'HEIC (ftypでも静止画コンテナ) は動画として通らない');

  const tmp = (name, buf) => { const p = path.join(process.env.DATA_DIR, name); fs.writeFileSync(p, buf); return p; };
  const a1 = addMedia({ pageId: pMedia.id, productCode: 'PROD-M', kind: 'photo', mime: 'image/jpeg',
    filePath: tmp('a1.jpg', jpeg), worker: worker1, deviceId: 11, operationId: 'op-photo-0001' });
  ok(a1.ok === true && a1.media.status === 'stored', '受信 → stored (即応答できる形)');
  ok(fs.existsSync(path.join(MEDIA_DIR, 'op-photo-0001.jpg')), '実体は MEDIA_DIR へ移動');
  const a1b = addMedia({ pageId: pMedia.id, kind: 'photo', filePath: tmp('a1b.jpg', jpeg), worker: worker1, deviceId: 11, operationId: 'op-photo-0001' });
  ok(a1b.ok === true && a1b.already === true, '同じ operation_id の再送は既存を返す (二重登録しない)');
  ok(!!a1b.deleteToken && a1b.deleteToken !== a1.deleteToken,
    '同じ端末からの再送には削除トークンを発行し直す (応答消失でも削除できる — PR3-R3)');
  const a1c = addMedia({ pageId: pMedia.id, kind: 'photo', filePath: tmp('a1c.jpg', jpeg), worker: worker1, deviceId: 22, operationId: 'op-photo-0001' });
  // ⭐再送でトークンを配り直しても、前のトークンは何世代か生きている (応答の順が入れ替わっても
  //   撮った本人が消せる — Codex PR1 R11)
  {
    const first = addMedia({ pageId: pMedia.id, kind: 'photo', filePath: tmp('tok1.jpg', jpeg), worker: worker1, deviceId: 44, operationId: 'op-token-0001' });
    const t1 = first.deleteToken;
    let last = null;
    for (const n of [2, 3, 4]) {
      last = addMedia({ pageId: pMedia.id, kind: 'photo', filePath: tmp('tok' + n + '.jpg', jpeg), worker: worker1, deviceId: 44, operationId: 'op-token-0001' }).deleteToken;
    }
    ok(t1 && last && t1 !== last, '再送のたびに新しいトークンを返す');
    ok(softDeleteMedia(first.media.id, { deleteToken: t1 }).ok === true, '3 回配り直した後でも、最初のトークンで消せる');
    ok(softDeleteMedia(first.media.id, { deleteToken: 'wrong-token' }).ok === false, '関係ないトークンでは消せない');
  }
  ok(a1c.already === true && !a1c.deleteToken, '別端末からの同 operation_id にはトークンを返さない');
  ok(addMedia({ pageId: pMedia.id, kind: 'photo', filePath: tmp('bad.jpg', mp4), worker: worker1, operationId: 'op-bad-000001' }).error === 'bad_file',
    '中身が写真でなければ拒否 (Content-Type を信じない)');
  addMedia({ pageId: pMedia.id, productCode: 'PROD-M', kind: 'photo', filePath: tmp('a2.jpg', jpeg), worker: worker1, operationId: 'op-photo-0002' });
  const a3 = addMedia({ pageId: pMedia.id, productCode: 'PROD-M', kind: 'photo', filePath: tmp('a3.jpg', jpeg), worker: worker2, deviceId: 11, operationId: 'op-photo-0003' });
  ok(a1.deleteToken && a3.deleteToken && a1.deleteToken !== a3.deleteToken, '削除トークンは行ごとに別');
  ok(addMedia({ pageId: pMedia.id, kind: 'photo', filePath: tmp('a4.jpg', jpeg), worker: worker1, operationId: 'op-photo-0004' }).error === 'cap_reached',
    '写真は3枚まで');
  // 動画は当面なし (中原さん 2026-09-03: iPad で再生できなかった)。入口で断る
  const v1 = addMedia({ pageId: pMedia.id, kind: 'video', mime: 'video/mp4', filePath: tmp('v1.mp4', mp4), worker: worker1, operationId: 'op-video-0001' });
  ok(v1.ok === false && v1.error === 'video_disabled', '動画は受け付けない (video_disabled)');
  ok(fs.existsSync(path.join(process.env.DATA_DIR, 'v1.mp4')), '断ったときは一時ファイルを取り込まない (呼び元が片づける)');
  ok(countActiveMedia(pMedia.id, 'video') === 0, '動画は 1 本も入らない');
  ok(countActiveMedia(pMedia.id, 'photo') === 3, '有効な写真 3 枚 (枚数上限の根拠)');

  // キュー: 1回目は「Drive に作成できたが応答が消えた」→ 再試行は operation_id で**回収**し
  // 二重作成しない (Codex PR3 #3 の契約をモックで再現)
  let driveCalls = 0;
  const driveFiles = new Map();
  _setDriveUpload(async ({ operationId }) => {
    driveCalls++;
    if (driveFiles.has(operationId)) return driveFiles.get(operationId);
    const rec = { fileId: 'f-' + operationId, url: 'https://drive.google.com/file/d/f-' + operationId + '/view' };
    driveFiles.set(operationId, rec);
    if (driveCalls === 1) throw new Error('created but response lost');
    return rec;
  });
  await processMediaQueue();
  let rows = listMediaForAdmin(10).filter(m => m.page_id === pMedia.id);
  ok(rows.some(m => m.error && m.next_retry_at), '失敗は next_retry 付きで記録 (すぐ連打しない)');
  for (const m of rows) if (m.error) resetMedia(m.id);
  await new Promise(r => setTimeout(r, 30));   // resetMedia の schedule 分を待つ
  await processMediaQueue();
  await processMediaQueue();   // 2巡目で残りの upload + Notion 反映
  rows = listMediaForAdmin(10).filter(m => m.page_id === pMedia.id && !m.deleted_at);
  ok(rows.every(m => m.status === 'synced' && m.drive_url), `全件 Drive→Notion まで反映 (実際: ${rows.map(m => m.status).join(',')})`);
  ok(driveFiles.size === 3, `Drive のファイルは3つだけ (写真のみ・再試行で二重作成しない。実際 ${driveFiles.size})`);
  ok(rows.find(m => m.operation_id === 'op-photo-0001').drive_file_id === 'f-op-photo-0001',
    '応答消失した1件も同じファイルを回収して紐づく');
  ok(!fs.existsSync(path.join(MEDIA_DIR, 'op-photo-0001.jpg')), 'アップロード後に実体を削除');
  const patched = mock.patched.filter(p => p.id === pMedia.id && p.body.properties?.['完成写真']);
  ok(patched.length > 0 && patched[patched.length - 1].body.properties['完成写真'].files.length === 3,
    'Notion「完成写真」に3件 (写真3) が付く');

  // 「前回の完成形」: 同じ商品コードの**別カード**に、Drive 保存済みの写真が新しい順で付く
  // (中原さん 2026-09-03: 写真は完了の証拠ではなく、次に同じ商品を作る人への見本)
  {
    const { upsertCachePage } = await import('../apps/iroha-work/db.js');
    const pNext = mkPage({ status: '未着手', title: '同じ商品の次回', code: 'prod-m' });   // 大小文字違いでも同じ商品
    upsertCachePage(parsePage(pNext));
    upsertCachePage(parsePage(pMedia));   // 写真を撮ったカード自身も一覧に居る状態で比べる
    clearEnrichCache();
    const list = buildList().cards;
    const next = list.find(c => c.page_id === pNext.id);
    const self = list.find(c => c.page_id === pMedia.id);
    ok(next && next.previous_photos.length === 3, `次回のカードに前回の写真3枚 (実際 ${next && next.previous_photos.length})`);
    ok(next && next.previous_photos.every(p => p.page_id === pMedia.id && p.worker_name && p.created_at), '前回のカードの写真 (撮った人・日時つき)');
    ok(next && next.previous_photos[0].id > next.previous_photos[2].id, '新しい順');
    ok(self && self.previous_photos.length === 0, '自分のカードの写真は「前回」に含めない');
    ok(self && self.media.every(m => m.viewable === true), 'Drive 保存済みは viewable (画面は /api/media/:id/file で表示)');
  }

  // 論理削除: 削除トークンが合わなければ消せない (worker_id 偽装は無意味) / 合えば消せる
  const target = rows.find(m => m.operation_id === 'op-photo-0003');
  ok(softDeleteMedia(target.id, { deleteToken: a1.deleteToken }).error === 'forbidden', '別の写真のトークンでは消せない');
  ok(softDeleteMedia(target.id, { deleteToken: null }).error === 'forbidden', 'トークンなしも消せない');
  ok(softDeleteMedia(target.id, { deleteToken: a3.deleteToken }).ok === true, '撮影した端末のトークンなら消せる (論理削除)');
  ok(countActiveMedia(pMedia.id, 'photo') === 2, '削除後は2枚');
  await new Promise(r => setTimeout(r, 30));
  await processMediaQueue();
  const last = mock.patched.filter(p => p.id === pMedia.id && p.body.properties?.['完成写真']).pop();
  ok(last.body.properties['完成写真'].files.length === 2, 'Notion 側も貼り直されて2件 (写真2)');
  ok((mediaByPage().get(pMedia.id) || []).length === 2, '画面用一覧にも削除分は出ない');

  // 動画をやめる前に撮ってあった動画は、そのまま残す (行・ドライブのファイル・削除トークン・Notion の貼り付け)。
  // 写真を足したり消したりしても影響を受けない (Codex 動画 R1)
  {
    const pLegacy = mkPage({ status: '作業中', title: '前に動画も撮ったカード', code: 'PROD-LEG' });
    const now = new Date().toISOString();
    getDB().prepare(`INSERT INTO f_iroha_card_media (operation_id, page_id, product_code, kind, mime, size, drive_file_id, drive_url,
        status, worker_id, worker_name, created_at, uploaded_at, delete_token_hash)
      VALUES ('op-legacy-vid1', ?, 'PROD-LEG', 'video', 'video/mp4', 1234, 'f-legacy-vid1', 'https://drive/legacy-vid1',
        'uploaded', ?, 'やまだ', ?, ?, 'hash-legacy')`).run(pLegacy.id, worker1.id, now, now);
    const before = getDB().prepare("SELECT * FROM f_iroha_card_media WHERE operation_id = 'op-legacy-vid1'").get();
    const p1 = addMedia({ pageId: pLegacy.id, productCode: 'PROD-LEG', kind: 'photo', filePath: tmp('leg1.jpg', jpeg), worker: worker1, operationId: 'op-legacy-ph01' });
    ok(p1.ok, '前提: 同じカードに写真を足せる');
    await processMediaQueue(); await processMediaQueue();
    const patchedLeg = mock.patched.filter(x => x.id === pLegacy.id && x.body.properties?.['完成写真']).pop();
    const urls = (patchedLeg ? patchedLeg.body.properties['完成写真'].files : []).map(f => f.external.url);
    ok(urls.includes('https://drive/legacy-vid1'), 'Notion の完成写真に、前からある動画の URL が残る');
    ok(urls.length === 2, '写真とあわせて 2 件 (動画を外さない)');
    ok(softDeleteMedia(getDB().prepare("SELECT id FROM f_iroha_card_media WHERE operation_id = 'op-legacy-ph01'").get().id, { deleteToken: p1.deleteToken }).ok, '前提: 足した写真を消す');
    await new Promise(r => setTimeout(r, 30));
    await processMediaQueue();
    const afterDel = mock.patched.filter(x => x.id === pLegacy.id && x.body.properties?.['完成写真']).pop();
    ok(afterDel.body.properties['完成写真'].files.map(f => f.external.url).includes('https://drive/legacy-vid1'), '写真を消しても動画は Notion に残る');
    const after = getDB().prepare("SELECT * FROM f_iroha_card_media WHERE operation_id = 'op-legacy-vid1'").get();
    ok(after.deleted_at == null && after.drive_file_id === before.drive_file_id && after.delete_token_hash === before.delete_token_hash
      && after.page_id === before.page_id && after.kind === 'video', '動画の行はそのまま (消えない・変わらない)');
    ok((mediaByPage().get(pLegacy.id) || []).some(m => m.kind === 'video'), '画面用のデータには動画も入っている (出すかどうかは画面側の判断)');
  }

  // 最後の1件を消したときも Notion を「空にする」PATCH が飛ぶ (Codex PR3 #1)
  const pSolo = mkPage({ status: '作業中', title: '1枚だけ', code: 'PROD-S' });
  const solo = addMedia({ pageId: pSolo.id, kind: 'photo', filePath: tmp('s1.jpg', jpeg), worker: worker1, operationId: 'op-solo-00001' });
  await processMediaQueue(); await processMediaQueue();
  ok(softDeleteMedia(getDB().prepare("SELECT id FROM f_iroha_card_media WHERE operation_id = 'op-solo-00001'").get().id,
    { deleteToken: solo.deleteToken }).ok === true, '最後の1件を削除');
  await new Promise(r => setTimeout(r, 30));
  await processMediaQueue();
  const soloLast = mock.patched.filter(p => p.id === pSolo.id && p.body.properties?.['完成写真']).pop();
  ok(soloLast && soloLast.body.properties['完成写真'].files.length === 0, 'Notion の完成写真が空になる');
  ok(countActiveMedia(pSolo.id, 'photo') === 0, '削除後は 0 枚');

  // 恒久失敗 (10回) した写真は棚入完了ゲートに数えない (Codex PR3 #4)
  const pDead = mkPage({ status: '作業中', title: '失敗だけ', code: 'PROD-X2' });
  addMedia({ pageId: pDead.id, kind: 'photo', filePath: tmp('d1.jpg', jpeg), worker: worker1, operationId: 'op-dead-00001' });
  ok(countActiveMedia(pDead.id, 'photo') === 1, '送信待ち (stored) も枚数上限には数える');
  getDB().prepare("UPDATE f_iroha_card_media SET next_retry_at = '9999-12-31T00:00:00.000Z', error = 'x' WHERE operation_id = 'op-dead-00001'").run();
  ok(countActiveMedia(pDead.id, 'photo') === 1, '停止した失敗写真も枚数上限には残る (削除して撮り直す)');
  ok(!(photosByCodeKey().get('prod-x2') || []).length, '停止した失敗写真 (Drive 未保存) は「前回の完成形」候補にならない');
  ok(mediaByPage().get(pDead.id).find(m => m.status === 'stored').viewable === false, '停止した送信待ち (実体が残る保証なし) は表示対象外 (viewable=false)');

  // 「前回の完成形」は直近に撮った**1カードぶん** (複数カードの写真を混ぜない) / Drive から消えた写真は候補外 (Codex R1 #4 #5)
  {
    const { markMediaUnavailable } = await import('../apps/iroha-work/media.js');
    const cands = [
      { id: 30, page_id: 'pg-new' }, { id: 29, page_id: 'pg-new' },
      { id: 20, page_id: 'pg-old' }, { id: 19, page_id: 'pg-old' }, { id: 18, page_id: 'pg-old' }, { id: 17, page_id: 'pg-old' },
    ];
    ok(previousPhotosOf(cands, 'pg-me').map(p => p.id).join() === '30,29', '直近に撮ったカード (pg-new) の写真だけ (古いカードの写真を混ぜない)');
    ok(previousPhotosOf(cands, 'pg-new').map(p => p.id).join() === '20,19,18', '自分が直近なら、その前のカードの写真 (最大3枚)');
    ok(previousPhotosOf([{ id: 1, page_id: 'pg-me' }], 'pg-me').length === 0, '自分の写真しか無ければ空');

    const mid = getDB().prepare("SELECT id FROM f_iroha_card_media WHERE operation_id = 'op-photo-0001'").get().id;
    ok(markMediaUnavailable(mid, 'Drive 404') === true, 'ドライブから消えた印を付ける');
    ok(!(photosByCodeKey().get('prod-m') || []).some(p => p.id === mid), '消えた写真は「前回の完成形」候補から外れる');
    ok(mediaByPage().get(pMedia.id).find(m => m.id === mid).viewable === false, '消えた写真は表示対象外 (viewable=false)');
    ok(markMediaUnavailable(mid, 'again') === false, '二重に印は付けない');
    // ⭐配信 (GET) から呼ぶ報告は、その場では DB を変えない — キューの回でまとめて印を付ける (Codex PR1 R9)
    {
      const { reportMediaUnavailable, flushUnavailableReports } = await import('../apps/iroha-work/media.js');
      const other = getDB().prepare("SELECT id FROM f_iroha_card_media WHERE operation_id = 'op-photo-0002'").get().id;
      reportMediaUnavailable(other, 'Drive 404');
      ok(getDB().prepare('SELECT unavailable_at FROM f_iroha_card_media WHERE id = ?').get(other).unavailable_at == null,
        '報告しただけでは印は付かない (読むだけの画面で写真を開いても DB が変わらない)');
      ok(flushUnavailableReports() === 1, 'キューの回で印を付ける');
      ok(getDB().prepare('SELECT unavailable_at FROM f_iroha_card_media WHERE id = ?').get(other).unavailable_at != null, '印が付いている');
      ok(flushUnavailableReports() === 0, '溜まった報告は一度で片づく');
      // 報告してから反映するまでに送り直されていたら、新しい実体に古い 404 を貼らない (Codex PR1 R13)
      getDB().prepare('UPDATE f_iroha_card_media SET unavailable_at = NULL, error = NULL, drive_file_id = ? WHERE id = ?')
        .run('f-old-file', other);
      reportMediaUnavailable(other, 'Drive 404', 'f-old-file');
      getDB().prepare('UPDATE f_iroha_card_media SET drive_file_id = ? WHERE id = ?').run('f-new-file', other);
      ok(flushUnavailableReports() === 0, '送り直された後なら印を付けない');
      ok(getDB().prepare('SELECT unavailable_at FROM f_iroha_card_media WHERE id = ?').get(other).unavailable_at == null,
        '新しい実体に古い 404 は貼られない');
      getDB().prepare('UPDATE f_iroha_card_media SET unavailable_at = NULL, error = NULL WHERE id = ?').run(other);
    }
    // 印の解除は Drive で実在を確かめてから (未検証のまま候補へ戻さない — Codex R2 #3)
    const { recheckUnavailable, _setDriveExists, etagMatches, ifRangeMatches, singleRange } = await import('../apps/iroha-work/media.js');
    _setDriveExists(async () => false);
    ok((await recheckUnavailable(mid)).error === 'still_unavailable', 'Drive に無ければ再確認しても印は残る');
    ok(mediaByPage().get(pMedia.id).find(m => m.id === mid).unavailable === true, '未検証のまま表示・候補へ戻さない');
    _setDriveExists(async () => { const e = new Error('gone'); e.response = { status: 404 }; throw e; });
    ok((await recheckUnavailable(mid)).error === 'still_unavailable', 'Drive の 404 も「無い」扱い');
    _setDriveExists(async () => { throw new Error('timeout'); });
    ok((await recheckUnavailable(mid)).error === 'drive_error', '一時的な失敗では解除も確定もしない');
    _setDriveExists(async () => true);
    ok((await recheckUnavailable(mid)).ok === true, 'Drive にあることを確認してから印を消す');
    ok(mediaByPage().get(pMedia.id).find(m => m.id === mid).unavailable === false, '再確認後は表示・候補に戻る');
    ok((await recheckUnavailable(mid)).error === 'not_unavailable', '印の無い行には何もしない');
    _setDriveExists(null);

    // 条件付きリクエストの小道具 (配信 API)
    ok(etagMatches('"a", W/"b"', '"b"') && etagMatches('*', '"x"') && !etagMatches('"a"', '"b"') && !etagMatches('', '"a"'),
      'If-None-Match は複数・W/・* を受ける (弱い比較)');
    ok(ifRangeMatches('"b"', '"b"') && !ifRangeMatches('W/"b"', '"b"') && !ifRangeMatches('*', '"b"')
      && !ifRangeMatches('Wed, 21 Oct 2015 07:28:00 GMT', '"b"') && !ifRangeMatches('"a", "b"', '"b"'),
      'If-Range は単一の強い ETag だけ (W/・*・日付・複数は不一致 = 全体を返す)');
    ok(singleRange('bytes=0-99') === 'bytes=0-99' && singleRange('bytes=100-') === 'bytes=100-' && singleRange('bytes=-500') === 'bytes=-500',
      '単一 Range は転送');
    ok(singleRange('bytes=0-99,200-299') === null && singleRange('bytes=5-2') === null && singleRange('items=0-1') === null
      && singleRange('bytes=-') === null && singleRange('') === null && singleRange(undefined) === null,
      '複数・逆転・不正・空は無視 (全体を返す)');

    // 送信待ちの実体が消えたら、一覧生成時にその場で停止扱い (画面は失敗を出して撮り直せる — Codex R2 #1)
    const pLost = mkPage({ status: '作業中', title: '実体消失', code: 'PROD-LOST' });
    addMedia({ pageId: pLost.id, productCode: 'PROD-LOST', kind: 'photo', filePath: tmp('lost.jpg', jpeg), worker: worker1, operationId: 'op-lost-00001' });
    const lostRow = getDB().prepare("SELECT * FROM f_iroha_card_media WHERE operation_id = 'op-lost-00001'").get();
    ok(mediaByPage().get(pLost.id)[0].viewable === true, '実体があるうちは表示できる');
    fs.unlinkSync(lostRow.local_path);
    const lostPub = mediaByPage().get(pLost.id)[0];
    ok(lostPub.viewable === false && /実体ファイルがありません/.test(lostPub.error || ''), '実体が消えたら停止扱い (viewable=false・失敗の理由つき)');
    // パス自体が欠損した送信待ち (移行ミス等) も同じ扱い (Codex R3)
    const pNoPath = mkPage({ status: '作業中', title: 'パス欠損', code: 'PROD-NOPATH' });
    addMedia({ pageId: pNoPath.id, productCode: 'PROD-NOPATH', kind: 'photo', filePath: tmp('nopath.jpg', jpeg), worker: worker1, operationId: 'op-nopath-0001' });
    getDB().prepare("UPDATE f_iroha_card_media SET local_path = NULL WHERE operation_id = 'op-nopath-0001'").run();
    const noPathPub = mediaByPage().get(pNoPath.id)[0];
    ok(noPathPub.viewable === false && /実体ファイルがありません/.test(noPathPub.error || ''), 'local_path が空の送信待ちも停止扱い');
    ok(getDB().prepare("SELECT next_retry_at FROM f_iroha_card_media WHERE operation_id = 'op-nopath-0001'").get().next_retry_at === '9999-12-31T00:00:00.000Z',
      'キューの10回再試行を待たずに BLOCKED');
  }

  // Notion PATCH 中に削除が割り込んでも、削除の同期要求が消えない (revision 方式 — PR3-R2)
  const pRace2 = mkPage({ status: '作業中', title: '同期レース', code: 'PROD-R2' });
  addMedia({ pageId: pRace2.id, kind: 'photo', filePath: tmp('r1.jpg', jpeg), worker: worker1, operationId: 'op-race-00001' });
  const rm2 = addMedia({ pageId: pRace2.id, kind: 'photo', filePath: tmp('r2.jpg', jpeg), worker: worker1, operationId: 'op-race-00002' });
  mock.onPagePatch = async () => {
    const id2 = getDB().prepare("SELECT id FROM f_iroha_card_media WHERE operation_id = 'op-race-00002'").get().id;
    const d = softDeleteMedia(id2, { deleteToken: rm2.deleteToken });
    if (!d.ok) throw new Error('レース用の削除が失敗: ' + d.error);
  };
  await processMediaQueue();   // upload×2 → 同期PATCH (この最中に2枚目が削除される)
  ok(getDB().prepare('SELECT COUNT(*) c FROM f_iroha_media_page_sync WHERE page_id = ?').get(pRace2.id).c === 1,
    'PATCH 中の削除要求は完了扱いで消されず、キューに残る');
  await new Promise(r => setTimeout(r, 30));
  await processMediaQueue();
  const raceLast = mock.patched.filter(p => p.id === pRace2.id && p.body.properties?.['完成写真']).pop();
  ok(raceLast.body.properties['完成写真'].files.length === 1, '次の巡回で削除後の1枚に貼り直される');
  ok(getDB().prepare('SELECT COUNT(*) c FROM f_iroha_media_page_sync WHERE page_id = ?').get(pRace2.id).c === 0,
    '最新内容で同期できたらキューから消える');
  _setDriveUpload(null);
}

console.log('\n[16] 作業のやり方の選択肢 (資材・保管箱): Excel 由来の候補 + その場登録');
{
  const { listWorkOptions, workOptionsByKind, addWorkOption, setWorkOptionActive, setWorkOptionImage, seedWorkOptionsFromMaster,
    normalizeOptionCode, validateOptionImageUrl, _resetSeedFingerprint } = await import('../apps/iroha-work/db.js');
  const { addWorkMasterRow: addRow, updateWorkMasterRow: updRow } = await import('../apps/inbound-check/work-master.js');
  // 前提: マスタ 2 行に資材・保管箱 (2 行目は全角・小文字の表記揺れ)。mirror_products に無いコードなので直接 INSERT (他テストと同じ)
  const insMaster = getDB().prepare(`INSERT INTO f_iroha_work_master (code_key, 商品コード, material_code, storage_container, version, updated_at)
    VALUES (?, ?, ?, ?, 1, ?)`);
  insMaster.run('opt-seed-1', 'OPT-SEED-1', ' D-8 ', '20Lコンテナ', new Date().toISOString());
  insMaster.run('opt-seed-2', 'OPT-SEED-2', 'ｄ－８', '20lコンテナ', new Date().toISOString());
  void addRow; void updRow;
  _resetSeedFingerprint();
  const seeded = seedWorkOptionsFromMaster();
  ok(seeded.material >= 1 && seeded.container >= 1 && !seeded.skipped, `作業仕様マスタの資材・保管箱が候補に補充される (資材${seeded.material}/保管箱${seeded.container})`);
  const d8 = listWorkOptions('material').filter(o => normalizeOptionCode(o.code) === 'D-8');
  ok(d8.length === 1 && d8[0].code === 'D-8' && d8[0].sort_order <= -2, `全角/小文字の同じ値は1候補にまとまり (表記は半角を優先)、使用回数ぶん上に並ぶ (実際 ${JSON.stringify(d8)})`);
  ok(listWorkOptions('container').filter(o => normalizeOptionCode(o.code) === '20LコンテナHOGE'.replace('HOGE', '')).length === 1, '保管箱も大小文字を同一視');
  const again = seedWorkOptionsFromMaster();
  ok(again.skipped === true, 'マスタが変わっていなければ走らない (フィンガープリント)');
  getDB().prepare("UPDATE f_iroha_work_master SET updated_at = ? WHERE code_key = 'opt-seed-1'").run(new Date(Date.now() + 3600 * 1000).toISOString());   // マスタが更新された (MAX(updated_at) が進む)
  const after = seedWorkOptionsFromMaster();
  ok(after.skipped === false && after.material === 0, 'マスタが更新されたら走る (新しい値が無ければ増えない = 冪等)');
  ok(normalizeOptionCode('　Ｄ－８　 x\t') === 'D-8 X', 'normalize = NFKC + 空白統一 + trim + 大文字');

  const a = addWorkOption({ kind: 'material', code: '  D-99   x ', actor: 'test' });
  ok(a.ok === true && a.option.code === 'D-99 x', '追加 (前後の空白は落とし、連続空白は1つに。表示は入力どおり)');
  ok(addWorkOption({ kind: 'material', code: 'd-99 X' }).already === true, '大小文字違いは既存を返す (増やさない)');
  ok(addWorkOption({ kind: 'shelf', code: 'x' }).error === 'bad_kind' && addWorkOption({ kind: 'container', code: ' 　 ' }).error === 'bad_code', '種類・空値 (全角空白) は拒否');
  ok(setWorkOptionActive(a.option.id, false) === true && !listWorkOptions('material').some(o => o.id === a.option.id), '無効化で画面の候補から消える');
  ok(listWorkOptions('material', true).some(o => o.id === a.option.id), '管理画面 (無効含む) には残る');
  const staffTry = addWorkOption({ kind: 'material', code: 'D-99 x' });
  ok(staffTry.ok === false && staffTry.error === 'inactive_option' && !listWorkOptions('material').some(o => o.id === a.option.id),
    '職員の「新しく登録」では管理者が外した候補を戻せない (Codex R1 #1)');
  const back = addWorkOption({ kind: 'material', code: 'D-99 x', allowReactivate: true });
  ok(back.already === true && back.reactivated === true && listWorkOptions('material').some(o => o.id === a.option.id), '管理者 (allowReactivate) なら戻せる');

  // 画像リンク: 全 iPad が読みに行くので許可先を絞る (Codex R1 #2)
  ok(validateOptionImageUrl('').ok && validateOptionImageUrl('').value === null, '空 = 外す');
  ok(validateOptionImageUrl('/apps/iroha-work/api/media/12/file').ok, 'ポータル内のパスは可');
  ok(!validateOptionImageUrl('/apps/../etc/passwd').ok && !validateOptionImageUrl('/etc/x.jpg').ok, 'ポータル外のパス・.. は不可');
  ok(!validateOptionImageUrl('http://drive.google.com/x.jpg').ok, 'http は不可');
  ok(!validateOptionImageUrl('https://evil.example.com/track.gif').ok && !validateOptionImageUrl('https://192.168.68.62/x.jpg').ok && !validateOptionImageUrl('https://localhost/x.jpg').ok, '許可外ホスト・LAN・localhost は不可');
  ok(!validateOptionImageUrl('https://user:pw@drive.google.com/x.jpg').ok, '認証情報つきは不可');
  ok(!validateOptionImageUrl('javascript:alert(1)').ok && !validateOptionImageUrl('https://' + 'a'.repeat(600)).ok, 'javascript: と長すぎるリンクは不可');
  ok(validateOptionImageUrl('https://drive.google.com/uc?id=abc').ok, '許可ホストの https は可');
  ok(setWorkOptionImage(a.option.id, 'https://evil.example.com/x.jpg').error === 'bad_url' && setWorkOptionImage(a.option.id, 'https://lh3.googleusercontent.com/d99.jpg').ok === true, 'setWorkOptionImage も同じ検証');
  ok(setWorkOptionImage(999999, 'https://drive.google.com/y.jpg').error === 'not_found', '無い id は not_found');
  const by = workOptionsByKind();
  ok(Array.isArray(by.material) && Array.isArray(by.container) && by.material.find(o => o.id === a.option.id).image_url === 'https://lh3.googleusercontent.com/d99.jpg', 'kind ごとの一覧に画像が載る');

  // %2e%2e や混在エンコードで /apps/ の境界を抜けられない。ポータル内は配信エンドポイントそのものだけ (Codex R2 #2)
  ok(!validateOptionImageUrl('/apps/%2e%2e/admin').ok && !validateOptionImageUrl('/apps/%2E%2E/admin').ok
    && !validateOptionImageUrl('/apps/iroha-work/api/media/1/%2E%2E/../file').ok && !validateOptionImageUrl('/apps/iroha-work/api/media/%31/file').ok,
    '%2e%2e・%2E%2E・混在・数字のエンコードは不可');
  ok(!validateOptionImageUrl('/apps/iroha-work/admin').ok && !validateOptionImageUrl('/apps/other/api/media/1/file').ok
    && !validateOptionImageUrl('/apps/iroha-work/api/media/12/file?x=1').ok && !validateOptionImageUrl('//evil.example.com/x.jpg').ok,
    'ポータル内でも配信エンドポイント以外・クエリつき・// 始まりは不可');
  ok(validateOptionImageUrl('/apps/iroha-work/api/media/12/file').value === '/apps/iroha-work/api/media/12/file', '配信エンドポイントは正規化済みの値で保存');
  // 同じポータルの絶対 URL でも相対パスと同じ制限 (Codex R3)
  ok(validateOptionImageUrl('https://bfaith-portal.onrender.com/apps/iroha-work/api/media/12/file').value === '/apps/iroha-work/api/media/12/file', 'ポータルの絶対 URL は相対パスに揃えて保存');
  ok(!validateOptionImageUrl('https://bfaith-portal.onrender.com/apps/other/api/media/1/file').ok && !validateOptionImageUrl('https://bfaith-portal.onrender.com/apps/iroha-work/admin').ok
    && !validateOptionImageUrl('https://bfaith-portal.onrender.com/apps/%2e%2e/admin').ok && !validateOptionImageUrl('https://bfaith-portal.onrender.com/apps/iroha-work/api/media/12/file?x=1').ok,
    'ポータルの絶対 URL でも配信エンドポイント以外・エンコード・クエリは不可');

  // 古い版 (normalized_code 無し・UNIQUE(kind, code)) のテーブルが残っていても、起動時に統合して作り直す (Codex R2 #1)
  {
    const { createTables } = await import('../apps/iroha-work/db.js');
    const db = getDB();
    db.exec('DROP TABLE f_iroha_work_options');
    db.exec(`CREATE TABLE f_iroha_work_options (id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL, code TEXT NOT NULL, image_url TEXT,
      sort_order INTEGER NOT NULL DEFAULT 0, active INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL, created_by TEXT, UNIQUE(kind, code))`);
    const insOld = db.prepare('INSERT INTO f_iroha_work_options (kind, code, image_url, sort_order, active, created_at) VALUES (?,?,?,?,?,?)');
    insOld.run('material', 'ｄ－８', null, -1, 0, '2026-09-01T00:00:00.000Z');
    insOld.run('material', 'D-8', 'https://drive.google.com/x.jpg', -3, 1, '2026-09-02T00:00:00.000Z');
    insOld.run('container', '20Lコンテナ', null, 0, 1, '2026-09-02T00:00:00.000Z');
    createTables(db);
    ok(db.prepare('PRAGMA table_info(f_iroha_work_options)').all().some(c => c.name === 'normalized_code'), '古いテーブルは normalized_code 付きに作り直される');
    const mat = db.prepare("SELECT * FROM f_iroha_work_options WHERE kind = 'material'").all();
    ok(mat.length === 1 && mat[0].code === 'D-8' && mat[0].normalized_code === 'D-8' && mat[0].active === 1
      && mat[0].image_url === 'https://drive.google.com/x.jpg' && mat[0].sort_order === -3 && mat[0].created_at === '2026-09-01T00:00:00.000Z',
      `表記揺れの旧行は1つに統合 (半角表記・有効・画像・使用回数最大・最古の作成日時を採用) 実際 ${JSON.stringify(mat)}`);
    ok(db.prepare("SELECT COUNT(*) c FROM f_iroha_work_options WHERE kind = 'container'").get().c === 1, '保管箱はそのまま');
    let dupErr = null;
    try { db.prepare("INSERT INTO f_iroha_work_options (kind, code, normalized_code, active, created_at) VALUES ('material','d-8','D-8',1,'x')").run(); } catch (e) { dupErr = e; }
    ok(dupErr && /UNIQUE/.test(dupErr.message), '作り直し後は UNIQUE(kind, normalized_code) が効く');
    createTables(db);
    ok(db.prepare("SELECT COUNT(*) c FROM f_iroha_work_options").get().c === 2, '2回目の起動では何もしない (冪等)');
    _resetSeedFingerprint();
  }
}

console.log('\n[15] 作業仕様のその場登録 (classify・版管理・動画リンク・スナップショット)');
{
  const { classifyMasterEdit } = await import('../apps/iroha-work/service.js');
  const { updateWorkMasterRow, addWorkMasterRow } = await import('../apps/inbound-check/work-master.js');
  const db3 = getDB();

  // classify: 空欄埋め vs 上書き
  const row = db3.prepare("SELECT * FROM f_iroha_work_master WHERE code_key = 'prod-a'").get();
  const c1 = classifyMasterEdit(row, { video_url: 'https://youtu.be/x' });
  ok(c1.fills.includes('video_url') && c1.overwrites.length === 0, '空欄への登録は fills (誰でも可)');
  const c2 = classifyMasterEdit(row, { material_code: 'D-9' });
  ok(c2.overwrites.includes('material_code'), '入っている値の変更は overwrites (職員のみ)');
  const c3 = classifyMasterEdit(row, { material_code: '' });
  ok(c3.overwrites.includes('material_code'), '値の削除も overwrites');
  const c4 = classifyMasterEdit(row, { material_code: row.material_code, note: row.note });
  ok(c4.fills.length === 0 && c4.overwrites.length === 0, '同じ値は変更なし');
  const c5 = classifyMasterEdit(null, { material_code: 'D-1' });
  ok(c5.fills.includes('material_code'), '行が無い商品への登録も fills');

  // 権限迂回の防止 (PR4-R3): 権限判定はカードフォールバック込みの**実効値**で行う。
  // マスタ空欄+カード表示 D-8 の項目: D-8 の確定保存=誰でも / D-9 への変更=職員 (router はこの2判定を併用)
  const { masterOf: masterOfSvc } = await import('../apps/iroha-work/service.js');
  const eff = masterOfSvc({ version: 1, material_code: null, storage_container: null, units_per_container: null, process_count: null, note: null, video_url: null },
    { '資材セットID': 'D-8' });
  ok(eff.material_code === 'D-8', '実効値はカード値で埋まる');
  const permSame = classifyMasterEdit(eff, { material_code: 'D-8' });
  ok(permSame.overwrites.length === 0, '表示どおりの値の確定保存は上書き扱いにならない (誰でも可)');
  const permDiff = classifyMasterEdit(eff, { material_code: 'D-9' });
  ok(permDiff.overwrites.includes('material_code'), '表示と違う値への変更は上書き (職員PIN必要)');

  // 動画リンクの検証と版管理
  const bad = updateWorkMasterRow('prod-a', { video_url: 'javascript:alert(1)' }, 'test', row.version);
  ok(bad.ok === false && bad.error === 'bad_url', 'http(s) 以外の動画リンクは拒否');
  const up = updateWorkMasterRow('prod-a', { video_url: 'https://youtu.be/abc' }, 'たにがわ (いろはアプリ)', row.version);
  ok(up.ok === true && up.row.video_url === 'https://youtu.be/abc' && up.row.version === row.version + 1,
    '動画リンク登録 + version が進む');
  ok(updateWorkMasterRow('prod-a', { note: 'x' }, 'test', row.version).error === 'conflict',
    '古い version では更新できない (2台同時編集の検出)');

  // buildList へ video_url と version が出る
  clearEnrichCache();
  const { cards } = buildList();
  const a = cards.find(c => c.product_code === 'PROD-A');
  ok(a.master.video_url === 'https://youtu.be/abc' && a.master.version === row.version + 1,
    '画面データに video_url と version (楽観ロック用) が載る');

  // #1 マスタ行が一部だけでも、カード値へ項目単位でフォールバック (表示が消えない)
  db3.prepare(`INSERT INTO mirror_products (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, 仕入先コード, updated_at)
    VALUES (2, 'PROD-NEW', '新商品', '単品', '取扱中', '確定', '0002', '2026-09-02T00:00:00Z')`).run();
  ok(addWorkMasterRow('PROD-NEW', 'test').ok === true, '新商品の行を作成');
  const newRow = db3.prepare("SELECT * FROM f_iroha_work_master WHERE code_key = 'prod-new'").get();
  ok(updateWorkMasterRow('prod-new', { video_url: 'https://youtu.be/new' }, 'test', newRow.version).ok === true, '動画だけ登録');
  clearEnrichCache();
  const cards2 = buildList().cards;
  const nw = cards2.find(c => c.product_code === 'PROD-NEW');
  ok(nw.master.source === 'master' && nw.master.video_url === 'https://youtu.be/new', 'マスタ行が使われる');
  ok(nw.master.material_code === 'D-8' && nw.master.units_per_container === 180,
    '動画だけの行でも資材・入数はカード値で表示され続ける (項目単位フォールバック)');
  ok(nw.master.missing.includes('工程') && !nw.master.missing.includes('資材'),
    '未登録バッジもフォールバック込みで判定');

  // ④開始時スナップショット
  const wS = addIrohaWorker({ displayName: 'すなぷ', workerType: 'member', actor: 'test' });
  const sS = startSession({ pageId: 'snap-p1', productCode: 'PROD-A', title: '商品A', worker: getIrohaWorker(wS.id) });
  const sessRow = db3.prepare('SELECT master_snapshot FROM f_iroha_work_sessions WHERE id = ?').get(sS.sessionId);
  const snap = JSON.parse(sessRow.master_snapshot);
  ok(snap.material_code === 'D-8' && snap.video_url === 'https://youtu.be/abc',
    '開始時点の作業仕様がセッションに残る (§1.7 ④)');
  stopSession({ pageId: 'snap-p1', workerId: wS.id, sessionId: sS.sessionId, reason: 'done' });

  // routerが渡す「実効値」スナップショット (フォールバック合成) がそのまま保存される
  const sS2 = startSession({ pageId: 'snap-p2', productCode: 'PROD-NEW', title: '新商品',
    worker: getIrohaWorker(wS.id), masterSnapshot: { source: 'master', material_code: 'D-8', units_per_container: 180 } });
  const snap2 = JSON.parse(db3.prepare('SELECT master_snapshot FROM f_iroha_work_sessions WHERE id = ?').get(sS2.sessionId).master_snapshot);
  ok(snap2.material_code === 'D-8' && snap2.units_per_container === 180,
    '呼び元が渡した実効値 (カードフォールバック込み) を保存 (PR4-R2 #1)');
  stopSession({ pageId: 'snap-p2', workerId: wS.id, sessionId: sS2.sessionId, reason: 'done' });
}

console.log('\n[17] アプリ正本化 (v1.1): 状態モデル・Notion 移行・タスク操作');
// ⭐タスクの書き換えは「アプリ正本のときだけ」通る (更新と同じトランザクションで見る — Codex PR1 R15)。
//   この節は正本をアプリにして試す (終わりで既定に戻す)
{ const { setMetaValue: sm17 } = await import('../apps/iroha-work/db.js'); sm17('source_of_truth', 'app'); }
{
  const T = await import('../apps/iroha-work/tasks.js');
  const TD = await import('../apps/iroha-work/tasks-db.js');
  const M = await import('../apps/iroha-work/migrate.js');

  // ── 純粋関数: 写像・遷移・不変条件 ──
  const map = (s) => T.mapLegacyStatus(s);
  ok(map('未着手').status === 'not_started' && map('作業中').status === 'in_progress' && map('作業完了').status === 'ready_for_stocking', '未着手/作業中/作業完了 の写像');
  ok(map('資材不足で作業中断').status === 'on_hold' && map('資材不足で作業中断').hold_reason === 'materials_shortage', '資材不足 → 保留 (理由つき)');
  ok(map('次回').status === 'not_started' && map('').status === 'not_started' && map('').confidence === 'inferred', '「次回」と未設定は未着手');
  ok(map('棚入完了').close_reason === 'stocked' && map('取消').close_reason === 'cancelled' && map('在庫化対象外').close_reason === 'out_of_scope', '完了系は closed + 理由');
  ok(map('羅針盤').status === 'in_progress' && map('羅針盤').facility === 'rashinban' && map('羅針盤').confidence === 'inferred', '施設名 = その施設に預けて作業中');
  ok(map('いろは').confidence === 'needs_review' && map('いろは').facility === 'iroha', '「いろは」は意味未確認 → 要確認');
  ok(map('中断').confidence === 'rejected' && map('中断').status === null, '未知のステータスは取り込まない');
  ok(T.canTransition('not_started', 'in_progress') && !T.canTransition('not_started', 'ready_for_stocking') && T.canTransition('ready_for_stocking', 'closed') && !T.canTransition('closed', 'not_started'), '許可遷移');
  ok(T.transitionNeedsStaff('ready_for_stocking', 'closed') && T.transitionNeedsStaff('closed', 'in_progress') && T.transitionNeedsStaff('ready_for_stocking', 'in_progress') && !T.transitionNeedsStaff('not_started', 'in_progress'), '職員限定遷移');
  ok(T.validateTaskInvariants({ status: 'closed', close_reason: 'stocked', closed_at: 'x' }).length === 0
    && T.validateTaskInvariants({ status: 'closed' }).length === 2
    && T.validateTaskInvariants({ status: 'on_hold' }).length === 1
    && T.validateTaskInvariants({ status: 'on_hold', hold_reason_code: 'other' }).length === 1
    && T.validateTaskInvariants({ status: 'in_progress', close_reason: 'stocked' }).length === 1, '不変条件 (closed の理由/時刻・on_hold の理由・その他は備考)');
  ok(T.statusLabel({ status: 'on_hold', hold_reason_code: 'label_shortage' }) === '保留 · ラベル待ち' && T.statusLabel({ status: 'closed', close_reason: 'stocked' }) === '終了 · 棚入完了', '表示ラベル');
  ok(TD.listFacilities().map(f => f.code).join(',') === 'iroha,rashinban,workcenter,jobsupport,rehas', '拠点の初期値が入っている');

  // ── 移行: 調査 (読むだけ) → dry-run → 本取込 (冪等) → バックフィル → 照合 ──
  const num = (n) => ({ type: 'number', number: n });
  const pM1 = mkPage({ status: '未着手', title: '移行A', code: 'MIG-A', qty: 10, props: { destination_id: num(9001) } });
  const pM2 = mkPage({ status: '羅針盤', title: '移行B (外部)', code: 'MIG-B', qty: 5, props: { destination_id: num(9002) } });
  const pM3 = mkPage({ status: '棚入完了', title: '移行C (完了)', code: 'MIG-C', qty: 3, props: { destination_id: num(9003) } });
  const pM4 = mkPage({ status: 'いろは', title: '移行D (要確認)', code: 'MIG-D', qty: 1, props: { destination_id: num(9004) } });
  const pM5 = mkPage({ status: '中断', title: '移行E (未知)', code: 'MIG-E', qty: 1, props: { destination_id: num(9005) } });
  const pM6 = mkPage({ status: '作業中', title: '移行F (dest 重複)', code: 'MIG-F', qty: 2, props: { destination_id: num(9001) } });
  const wM = listIrohaWorkers(true).find(w => w.display_name === 'やまだ');
  const sM = startSession({ pageId: pM2.id, productCode: 'MIG-B', title: '移行B (外部)', worker: getIrohaWorker(wM.id) });
  stopSession({ pageId: pM2.id, workerId: wM.id, sessionId: sM.sessionId, reason: 'done' });
  const before = getDB().prepare('SELECT COUNT(*) c FROM f_iroha_tasks').get().c;
  const patchesBefore = mock.dbPatches;
  const survey = await M.surveyNotion({ save: false });
  ok(survey.count === mock.pages.filter(p => !p.archived).length && !survey.truncated, `調査: Notion の全ページを読む (${survey.count}枚)`);
  ok(mock.dbPatches === patchesBefore && Array.isArray(survey.pages) && survey.cutoff, '調査は Notion の DB を書き換えない (スキーマは検証だけ) + 窓の上限 cutoff を返す');
  ok(mock.lastFilters[mock.lastFilters.length - 1] === null, '全件調査はフィルタなし');
  ok(survey.orphans.unlinked && survey.orphans.missingInNotion, '全件調査は「未紐づけ」と「Notion にも無い」の両方を出す');
  ok(Array.isArray(mock.lastSorts) && mock.lastSorts[0]?.timestamp === 'last_edited_time' && mock.lastSorts[0]?.direction === 'ascending', '取得は last_edited_time 昇順で固定');
  {
    const saved = await M.surveyNotion({ save: true });
    ok(saved.rawFile && fs.existsSync(saved.rawFile) && saved.file && fs.existsSync(saved.file), '調査は生レスポンスと集計を別ファイルに保存する');
    const raw = JSON.parse(fs.readFileSync(saved.rawFile, 'utf8'));
    ok(Array.isArray(raw.pages) && raw.pages.length === saved.count && raw.pages[0].properties && raw.pages[0].properties['ステータス']?.type === 'select', '生ファイルには Notion のプロパティ構造がそのまま残る (再解析できる)');
    const maxEdited = mock.pages.map(p => p.last_edited_time).sort().pop();   // PATCH で進んだページがあるので最大値から
    const win = await M.surveyNotion({ save: false, since: new Date(Date.parse(maxEdited) + 1000).toISOString() });
    ok(win.count === 0, `差分の窓 (since が全ページの更新時刻より後) なら 0 枚 (実際 ${win.count})`);
    const win2 = await M.surveyNotion({ save: false, since: maxEdited });
    ok(win2.count >= 1 && win2.count <= survey.count && win2.pages.every(p => p.lastEditedTime >= maxEdited), '境界 (since = 更新時刻) は含む');
  }
  ok(survey.byStatus['羅針盤'] === 1 && survey.issues.unknownStatus.some(i => i.pageId === pM5.id) && survey.issues.dupDestination.some(i => i.destinationId === 9001), '調査: ステータス別件数・未知値・destination 重複を検出');
  ok(getDB().prepare('SELECT COUNT(*) c FROM f_iroha_tasks').get().c === before, '調査は DB を変えない (dry-run)');
  const plan = M.planImport(survey.pages);
  const rowOf = (p) => plan.rows.find(r => r.notion_page_id === p.id);
  ok(rowOf(pM1).mapped_status === 'not_started' && rowOf(pM1).destination_id === 9001 && rowOf(pM1).will_import, 'dry-run: 未着手 → not_started');
  ok(rowOf(pM2).mapped_status === 'in_progress' && rowOf(pM2).facility_code === 'rashinban' && rowOf(pM2).warnings.includes('started_at_unknown'), 'dry-run: 羅針盤 → 作業中 + 拠点 (開始時刻は不明の警告)');
  ok(rowOf(pM3).mapped_status === 'closed' && rowOf(pM3).close_reason === 'stocked' && rowOf(pM3).warnings.includes('closed_at_approx'), 'dry-run: 棚入完了 → closed:stocked (終了時刻は近似)');
  ok(rowOf(pM4).migration_review === 1 && rowOf(pM4).warnings.includes('needs_review'), 'dry-run: 「いろは」は要確認');
  ok(rowOf(pM5).will_import === false && /未知/.test(rowOf(pM5).skip_reason), 'dry-run: 未知ステータスは取り込まない');
  ok(rowOf(pM6).destination_id === null && rowOf(pM6).warnings.some(w => w.startsWith('dup_destination')), 'dry-run: destination 重複の 2 枚目は destination_id を外して取り込む');
  ok(plan.summary.rejected >= 1 && plan.summary.needsReview >= 1 && typeof plan.summary.byMapped['closed:stocked'] === 'number', 'dry-run: サマリ');
  const csv = M.planToCsv(plan.rows);
  ok(csv.startsWith('﻿notion_page_id,') && csv.includes('MIG-A'), 'dry-run CSV (BOM つき)');

  const apply1 = M.applyImport(plan.rows, { batchId: 'test-batch-1', actor: 'test' });
  ok(apply1.inserted === plan.summary.willImport && apply1.skipped === plan.summary.rejected, `本取込: 取込予定 ${plan.summary.willImport} 件が入り、${plan.summary.rejected} 件を飛ばす`);
  ok(apply1.backfill.sessions >= 1 && TD.getTaskByPageId(pM2.id) && getDB().prepare('SELECT task_id FROM f_iroha_work_sessions WHERE id = ?').get(sM.sessionId).task_id === TD.getTaskByPageId(pM2.id).id, '作業時間の task_id をバックフィル');
  const tA = TD.getTaskByPageId(pM1.id), tC = TD.getTaskByPageId(pM3.id);
  ok(tA.status === 'not_started' && tA.destination_id === 9001 && tA.version === 1 && tA.updated_by === 'import:test-batch-1', '取り込まれた行 (import が主体)');
  ok(tC.status === 'closed' && tC.close_reason === 'stocked' && tC.closed_at && tC.closed_by === 'import', '完了カードは closed で残る');
  ok(!TD.listOpenTasks().some(t => t.id === tC.id) && TD.listClosedTasks().some(t => t.id === tC.id), 'closed は一覧に出ず、履歴に出る');
  const apply2 = M.applyImport(plan.rows, { batchId: 'test-batch-2', actor: 'test' });
  ok(apply2.inserted === 0 && apply2.updated === 0 && apply2.kept === plan.summary.willImport, '2 回取り込んでも増えない・変わらない (冪等)');
  const rec1 = M.reconcile(plan.rows);
  ok(rec1.missing.length === 0 && rec1.rejected.length === plan.summary.rejected && rec1.tasksWithPage === plan.summary.willImport, '照合: 欠けなし・取り込まない分は説明できる');
  ok(TD.listTasksNeedingReview().some(t => t.notion_page_id === pM4.id), '要確認一覧に「いろは」のカードが出る');

  // アプリで変えた状態は、差分取込 (Notion の古い値) で戻らない
  const moved = TD.changeTaskStatus({ taskId: tA.id, to: 'in_progress', expectVersion: tA.version, actor: 'たにがわ', isStaff: false });
  ok(moved.ok === true && moved.task.status === 'in_progress' && moved.task.started_at, 'アプリで未着手→作業中 (started_at が付く)');
  pM1.properties['数量'] = num(11);   // Notion 側で数量だけ直された
  pM1.properties['資材セットID'] = sel('D-99');   // 作業仕様も Notion 側で変わった (取込済みの指示は変えない)
  TD.clearMigrationReview({ taskId: TD.getTaskByPageId(pM4.id).id, expectVersion: TD.getTaskByPageId(pM4.id).version, actor: 'たにがわ' });
  const survey2 = await M.surveyNotion({ save: false, since: '2026-01-01T00:00:00Z' });
  const lastF = mock.lastFilters[mock.lastFilters.length - 1];
  ok(lastF && (lastF.and || []).some(c => c.timestamp === 'last_edited_time' && c.last_edited_time?.on_or_after) && (lastF.and || []).some(c => c.last_edited_time?.on_or_before === survey2.cutoff),
    '差分取込は since〜cutoff の窓で絞る (上限は取得開始時刻で固定)');
  ok(survey2.orphans.missingInNotion === undefined, '差分調査では「Notion にも無い」を出さない (窓の外の正常ページを孤立扱いしない)');
  const apply3 = M.applyImport(M.planImport(survey2.pages).rows, { batchId: 'test-batch-3', actor: 'test' });
  const tA2 = TD.getTask(tA.id);
  ok(apply3.updated >= 1 && tA2.qty === 11 && tA2.status === 'in_progress' && tA2.updated_by === 'たにがわ', '差分取込: 商品情報は追随し、アプリで変えた状態は戻さない');
  ok(JSON.parse(tA2.master_snapshot).material_code == null, '差分取込でも作業仕様スナップショット (作成時) は変えない');
  ok(TD.getTaskByPageId(pM4.id).migration_review === 0, '職員が確認済みにした要確認は差分取込で復活しない');
  const rDelta = M.reconcile(M.planImport(survey2.pages).rows, { mode: 'delta' });
  ok(rDelta.mode === 'delta' && rDelta.extra === null && rDelta.balanced === true, '差分照合は余りを評価しない');
  const rFull = M.reconcile(M.planImport(survey2.pages).rows, { mode: 'full' });
  ok(rFull.extra !== null && rFull.balanced === (rFull.missing.length === 0 && rFull.extra.length === 0), '全件照合は欠けも余りも 0 のときだけ釣り合う');

  // 既存 task (別ページ) と同じ destination のカードが Notion にできても、本取込は一意制約で丸ごと戻らない (外して要確認)
  // 9777 = Notion には無く、既存 task (db-only-1) だけが持つ destination → 取得内の重複ではなく DB との衝突として分類される
  TD.upsertTaskFromImport({ notion_page_id: 'db-only-1', status: 'not_started', destination_id: 9777, product_code: 'DBONLY' }, { batchId: 'test-batch-dbonly' });
  const pM7 = mkPage({ status: '未着手', title: '移行G (dest が既存 task と衝突)', code: 'MIG-G', qty: 1, props: { destination_id: num(9777) } });
  const plan7 = M.planImport(M.planImport ? (await M.surveyNotion({ save: false })).pages : []);
  const row7 = plan7.rows.find(r => r.notion_page_id === pM7.id);
  ok(row7.destination_id === null && row7.warnings.some(w => w.startsWith('dup_destination_db:task')) && row7.migration_review === 1, 'dry-run: 既存 task の destination と衝突 → 外して要確認');
  const apply7 = M.applyImport(plan7.rows, { batchId: 'test-batch-7', actor: 'test' });
  ok(apply7.inserted === 1 && TD.getTaskByPageId(pM7.id).destination_id === null && apply7.journal, '本取込は成功し (丸ごと戻らない)、証跡の状態を返す');
  // dry-run を通さない行で一意制約に当たると、その取込は全部戻る
  const rowsBad = [
    { notion_page_id: 'bad-1', mapped_status: 'not_started', facility_code: 'iroha', destination_id: 9900, will_import: true, warnings: [], master_snapshot: {}, payload: {} },
    { notion_page_id: 'bad-2', mapped_status: 'not_started', facility_code: 'iroha', destination_id: 9003, will_import: true, warnings: [], master_snapshot: {}, payload: {} },
  ];
  let badErr = null; try { M.applyImport(rowsBad, { batchId: 'test-batch-bad' }); } catch (e) { badErr = e; }
  ok(badErr && /UNIQUE/.test(badErr.message) && !TD.getTaskByPageId('bad-1'), '一意制約違反があれば 1 行目も含めて全部戻る (半端に残らない)');

  // ── タスク操作 ──
  const bad1 = TD.changeTaskStatus({ taskId: tA2.id, to: 'ready_for_stocking', expectVersion: 0, actor: 'x' });
  ok(bad1.error === 'conflict' && bad1.current, 'version が違えば conflict (最新を返す)');
  ok(TD.changeTaskStatus({ taskId: tA2.id, to: 'not_started', expectVersion: tA2.version }).error === 'bad_transition', '作業中→未着手は許可されない');
  const holdNo = TD.changeTaskStatus({ taskId: tA2.id, to: 'on_hold', expectVersion: tA2.version });
  ok(holdNo.error === 'hold_reason_required', '保留には理由が必要');
  const held = TD.changeTaskStatus({ taskId: tA2.id, to: 'on_hold', expectVersion: tA2.version, holdReason: 'label_shortage', workerId: wM.id, workerName: 'やまだ' });
  ok(held.ok && held.task.status === 'on_hold' && held.task.hold_reason_code === 'label_shortage', '保留 (ラベル待ち)');
  const resumed = TD.changeTaskStatus({ taskId: tA2.id, to: 'in_progress', expectVersion: held.task.version });
  ok(resumed.ok && resumed.task.hold_reason_code === null, '再開で保留理由が消える');
  const ready = TD.changeTaskStatus({ taskId: tA2.id, to: 'ready_for_stocking', expectVersion: resumed.task.version });
  ok(ready.ok && ready.task.ready_at, '棚入待ち (ready_at)');
  ok(TD.changeTaskStatus({ taskId: tA2.id, to: 'closed', expectVersion: ready.task.version, closeReason: 'stocked', isStaff: false }).error === 'staff_required', '棚入完了は職員のみ');
  ok(TD.changeTaskStatus({ taskId: tA2.id, to: 'closed', expectVersion: ready.task.version, isStaff: true }).error === 'close_reason_required', '終了には理由が必要');
  const closed = TD.changeTaskStatus({ taskId: tA2.id, to: 'closed', expectVersion: ready.task.version, closeReason: 'stocked', isStaff: true, actor: 'たにがわ' });
  ok(closed.ok && closed.task.closed_at && closed.task.closed_by === 'たにがわ' && !TD.listOpenTasks().some(t => t.id === tA2.id), '棚入完了 → 一覧から消える');
  ok(TD.changeTaskStatus({ taskId: tA2.id, to: 'in_progress', expectVersion: closed.task.version, isStaff: true }).error === 'bad_request', '終了からの再開には理由が必要');
  const reopened = TD.changeTaskStatus({ taskId: tA2.id, to: 'in_progress', expectVersion: closed.task.version, isStaff: true, reason: '数え間違い' });
  ok(reopened.ok && reopened.task.close_reason === null && reopened.task.closed_at === null && reopened.task.ready_at === null && reopened.task.started_at,
    '職員が理由つきで再開すると終了情報と棚入待ち時刻が消える (初回開始時刻は残る)');
  let chkErr = null;
  try { getDB().prepare("UPDATE f_iroha_tasks SET status = 'closed' WHERE id = ?").run(tA2.id); } catch (e) { chkErr = e; }
  ok(chkErr && /CHECK/.test(chkErr.message), 'DB の CHECK が不変条件を守る (理由なしの closed は SQL でも入らない)');
  let fkErr = null;
  try { getDB().prepare("UPDATE f_iroha_tasks SET facility_code = 'nowhere' WHERE id = ?").run(tA2.id); } catch (e) { fkErr = e; }
  ok(fkErr && /FOREIGN KEY/.test(fkErr.message), '拠点は f_iroha_facilities に無い値を入れられない (FK)');
  ok(TD.clearMigrationReview({ taskId: tA2.id, expectVersion: 0 }).error === 'conflict', '要確認クリアも version 競合を返す');
  ok(getDB().prepare("SELECT COUNT(*) c FROM f_iroha_app_events WHERE task_id = ? AND action = 'task_status'").get(tA2.id).c >= 6, '状態変更は履歴に残る (task_id つき)');
  // ⚠固定日付を「今日」になり得る日にすると、その日に後段の「今日やるが先頭」の検証が壊れる (2026-09-04 に実際に壊れた) → 来ない日付にする
  const planned = TD.setPlannedDate({ taskId: tA2.id, plannedDate: '2099-12-31', expectVersion: reopened.task.version });
  ok(planned.ok && planned.task.planned_date === '2099-12-31' && TD.setPlannedDate({ taskId: tA2.id, plannedDate: '9/4', expectVersion: planned.task.version }).error === 'bad_request', '「今日やる」= planned_date (形式検証)');

  // 取消: 未着手・実績なしは自動終了 / 実績ありは要確認 → 職員が判断
  const tB = TD.getTaskByPageId(pM2.id);   // 作業中・作業時間あり
  const cB = TD.requestCancellation({ destinationId: 9002, source: 'inbound_reversal' });
  ok(cB.action === 'review' && cB.task.cancellation_requested_at && TD.listTasksNeedingReview().some(t => t.id === tB.id), '着手済み (作業時間あり) の取消は要確認');
  ok(TD.resolveCancellation({ taskId: tB.id, decision: 'cancel', expectVersion: cB.task.version, isStaff: false }).error === 'staff_required', '取消の判断は職員のみ');
  const cont = TD.resolveCancellation({ taskId: tB.id, decision: 'continue', expectVersion: cB.task.version, isStaff: true, actor: 'たにがわ' });
  ok(cont.ok && cont.task.cancellation_requested_at === null && cont.task.status === 'in_progress', '続行を選ぶと要確認が消える');
  const tNew = TD.upsertTaskFromImport({ notion_page_id: 'fresh-1', status: 'not_started', destination_id: 9100, product_code: 'FRESH', product_name: '未着手だけ' }, { batchId: 'test-batch-4' });
  const cN = TD.requestCancellation({ destinationId: 9100 });
  ok(cN.action === 'closed' && cN.task.close_reason === 'cancelled' && cN.task.closed_at, '未着手・実績なしの取消は自動で終了 (取消)');
  ok(TD.requestCancellation({ destinationId: 424242 }).action === 'none', '該当 task が無ければ何もしない');
  let dupErr = null;
  try { TD.upsertTaskFromImport({ notion_page_id: 'fresh-2', status: 'not_started', destination_id: 9100 }, { batchId: 'x' }); } catch (e) { dupErr = e; }
  ok(dupErr && /UNIQUE/.test(dupErr.message), 'destination_id は一意 (別ページで同じ destination は入らない)');
  let invErr = null;
  try { TD.upsertTaskFromImport({ notion_page_id: 'fresh-3', status: 'on_hold' }, { batchId: 'x' }); } catch (e) { invErr = e; }
  ok(invErr && /不変条件/.test(invErr.message), '不変条件に反する取込行は拒否');

  // ラベル待ち
  const lw = TD.upsertLabelWait({ taskId: tB.id, fields: { occurred_on: '2026-09-03', recorded_by_name: 'やまだ', label_ordered: true, qty: 33, location: 'Z', note: '再発注' } });
  ok(lw.ok && lw.row.label_ordered === 1 && lw.row.location === 'Z' && lw.row.done === 0, 'ラベル待ちの登録');
  ok(TD.upsertLabelWait({ taskId: tB.id, fields: { location: 'X' } }).error === 'bad_request' && TD.upsertLabelWait({ taskId: tB.id, fields: { occurred_on: '9/3' } }).error === 'bad_request' && TD.upsertLabelWait({ taskId: tB.id, fields: { qty: -1 } }).error === 'bad_request', 'ロケーション・日付・数量の検証');
  const lw2 = TD.upsertLabelWait({ id: lw.row.id, taskId: tB.id, fields: { line_notified_on: '2026-09-04', done: true }, expectVersion: lw.row.version });
  ok(lw2.ok && lw2.row.line_notified_on === '2026-09-04' && lw2.row.done === 1 && lw2.row.version === 2, 'ラベル待ちの更新 (version が進む)');
  ok(TD.upsertLabelWait({ id: lw.row.id, taskId: tB.id, fields: { note: 'x' }, expectVersion: 1 }).error === 'conflict', '古い version では更新できない');
  ok(TD.listLabelWaits({ taskId: tB.id, openOnly: true }).length === 0 && TD.listLabelWaits({ taskId: tB.id, openOnly: false }).length === 1, '完了したものは未完了一覧から消える');
  ok(TD.upsertLabelWait({ taskId: 999999, fields: {} }).error === 'not_found', '無い task には登録できない');

  // 証跡ファイルが書けなくても取込は成功 (DB は commit 済み)・応答にパスやOSエラーを出さない (Codex A1 R2 #2 #3)
  {
    const migDir = path.join(process.env.DATA_DIR, 'iroha-migration');
    const applied = JSON.parse(fs.readFileSync(path.join(migDir, `apply-${apply1.batchId}.json`), 'utf8'));
    ok(applied.journal === 'saved', '証跡ファイル自身にも journal=saved が残る');
    fs.rmSync(migDir, { recursive: true, force: true });
    fs.writeFileSync(migDir, 'not a dir');   // 書けない状態を作る
    const pJ = mkPage({ status: '未着手', title: '証跡失敗', code: 'MIG-J', qty: 1, props: { destination_id: num(9800) } });
    const planJ = M.planImport(M.planImport ? [{ ...(await M.surveyNotion({ save: false })).pages.find(p => p.pageId === pJ.id) }] : []);
    const outJ = M.applyImport(planJ.rows, { batchId: 'test-batch-journal' });
    ok(outJ.inserted === 1 && TD.getTaskByPageId(pJ.id) && /^failed \(参照 [a-z0-9]+\)$/.test(outJ.journal), '取込は成功し、journal は参照番号つきの固定文言 (パス・OSエラーを出さない)');
    fs.rmSync(migDir, { force: true });
  }

  // 古い版 (CHECK・FK 無し) のタスク表が残っていても、起動時に行を移して作り直す (Codex A1 R2 #1)
  {
    const db = getDB();
    const { createTables } = await import('../apps/iroha-work/db.js');
    const nTasks = db.prepare('SELECT COUNT(*) c FROM f_iroha_tasks').get().c;
    const nLabel = db.prepare('SELECT COUNT(*) c FROM f_iroha_label_waits').get().c;
    const cols = db.prepare('PRAGMA table_info(f_iroha_tasks)').all().map(c => c.name);
    db.pragma('foreign_keys = OFF');
    db.exec(`CREATE TABLE f_iroha_tasks__old AS SELECT * FROM f_iroha_tasks;
      CREATE TABLE f_iroha_label_waits__old AS SELECT * FROM f_iroha_label_waits;
      DROP TABLE f_iroha_label_waits; DROP TABLE f_iroha_tasks;`);
    // R1 版相当: 同じ列で CHECK (不変条件) と FK が無い
    db.exec(`CREATE TABLE f_iroha_tasks (${cols.map(c => c === 'id' ? 'id INTEGER PRIMARY KEY AUTOINCREMENT' : c === 'status' ? 'status TEXT NOT NULL' : c === 'facility_code' ? "facility_code TEXT NOT NULL DEFAULT 'iroha'" : ['migration_review', 'version'].includes(c) ? `${c} INTEGER NOT NULL DEFAULT ${c === 'version' ? 1 : 0}` : ['created_at', 'updated_at'].includes(c) ? `${c} TEXT NOT NULL` : `${c} TEXT`).join(', ')});
      INSERT INTO f_iroha_tasks (${cols.join(', ')}) SELECT ${cols.map((c) => (c === 'facility_code' ? "COALESCE(facility_code, 'iroha')" : c)).join(', ')} FROM f_iroha_tasks__old;
      CREATE TABLE f_iroha_label_waits (id INTEGER PRIMARY KEY AUTOINCREMENT, task_id INTEGER NOT NULL, occurred_on TEXT, recorded_by_worker_id INTEGER, recorded_by_name TEXT,
        label_ordered INTEGER NOT NULL DEFAULT 0, lot_expiry TEXT, qty INTEGER, location TEXT, reattach INTEGER NOT NULL DEFAULT 0, line_notified_on TEXT, re_notified_on TEXT,
        restocked_on TEXT, done INTEGER NOT NULL DEFAULT 0, note TEXT, version INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
      INSERT INTO f_iroha_label_waits SELECT * FROM f_iroha_label_waits__old;
      DROP TABLE f_iroha_tasks__old; DROP TABLE f_iroha_label_waits__old;`);
    // 古い版に溜まり得る汚れ: task の無いラベル待ち / 宙ぶらりんの session.task_id / 拠点コードの誤り
    db.prepare("INSERT INTO f_iroha_label_waits (task_id, note, version, created_at, updated_at) VALUES (999999, '孤立', 1, 'x', 'x')").run();
    const sidDangling = db.prepare('SELECT MIN(id) id FROM f_iroha_work_sessions').get().id;
    db.prepare('UPDATE f_iroha_work_sessions SET task_id = 999998 WHERE id = ?').run(sidDangling);
    db.prepare("INSERT INTO f_iroha_tasks (status, facility_code, notion_page_id, version, created_at, updated_at) VALUES ('not_started', 'nowhere', 'old-bad-fac', 1, 'x', 'x')").run();
    db.pragma('foreign_keys = ON');
    const oldSql = db.prepare("SELECT sql FROM sqlite_master WHERE name = 'f_iroha_tasks'").get().sql;
    ok(!/CHECK \(\(status/.test(oldSql), '前提: 古い版には不変条件の CHECK が無い');
    let migErr = null; try { createTables(db); } catch (e) { migErr = e; }
    ok(migErr && /FK 違反/.test(migErr.message) && !/REFERENCES f_iroha_facilities/.test(db.prepare("SELECT sql FROM sqlite_master WHERE name = 'f_iroha_tasks'").get().sql),
      '補正できない違反 (拠点コードの誤り) があれば作り直しを中止して旧テーブルのまま (全部戻る)');
    ok(db.pragma('foreign_keys', { simple: true }) === 1, '中止しても foreign_keys は ON に戻る');
    db.pragma('foreign_keys = OFF');
    db.prepare("UPDATE f_iroha_tasks SET facility_code = 'iroha' WHERE notion_page_id = 'old-bad-fac'").run();   // 人が直した
    db.pragma('foreign_keys = ON');
    createTables(db);
    const newSql = db.prepare("SELECT sql FROM sqlite_master WHERE name = 'f_iroha_tasks'").get().sql;
    const newLabelSql = db.prepare("SELECT sql FROM sqlite_master WHERE name = 'f_iroha_label_waits'").get().sql;
    ok(/\(status = 'closed'\) = \(close_reason IS NOT NULL\)/.test(newSql) && /REFERENCES f_iroha_facilities/.test(newSql) && /REFERENCES f_iroha_tasks/.test(newLabelSql) && /location IN \('Z','Y','none'\)/.test(newLabelSql),
      '直した後は CHECK・FK 付きへ作り直される');
    ok(db.prepare('SELECT COUNT(*) c FROM f_iroha_tasks').get().c === nTasks + 1 && db.prepare('SELECT COUNT(*) c FROM f_iroha_label_waits').get().c === nLabel, '行はそのまま移る (孤立ラベル待ちは本体から外れる)');
    ok(db.prepare("SELECT COUNT(*) c FROM f_iroha_label_waits__orphan WHERE task_id = 999999").get().c === 1, '孤立ラベル待ちは消さずに __orphan へ退避');
    ok(db.prepare('SELECT task_id FROM f_iroha_work_sessions WHERE id = ?').get(sidDangling).task_id === null, '宙ぶらりんの task_id は外す (page_id は残る)');
    ok(db.prepare("SELECT name FROM sqlite_master WHERE type = 'index' AND name = 'idx_iroha_tasks_destination'").get(), '索引も作り直される');
    let chk2 = null; try { db.prepare("UPDATE f_iroha_tasks SET status = 'closed', close_reason = NULL WHERE id = ?").run(tB.id); } catch (e) { chk2 = e; }
    ok(chk2 && /CHECK/.test(chk2.message), '作り直し後は CHECK が効く');
    ok(db.pragma('foreign_keys', { simple: true }) === 1 && db.pragma('foreign_key_check').length === 0, 'foreign_keys は ON に戻り、FK 違反は無い');
    createTables(db);
    ok(db.prepare("SELECT COUNT(*) c FROM sqlite_master WHERE name LIKE 'f_iroha_tasks%'").get().c === 1, '2 回目は何もしない (冪等)');
  }
}

console.log('\n[18] アプリ正本の画面データ (A1b): tasks 版の一覧・作業時間・写真・作り直し');
{
  const T = await import('../apps/iroha-work/tasks.js');
  const TD = await import('../apps/iroha-work/tasks-db.js');
  const S = await import('../apps/iroha-work/service.js');
  const { addMedia, mediaByTask, moveStoredFile, promoteStagedMedia, sweepStagedMedia, countActiveMedia, _setDriveUpload, processMediaQueue } = await import('../apps/iroha-work/media.js');
  const db = getDB();
  const w1 = listIrohaWorkers(true).find(w => w.display_name === 'やまだ');
  const w2 = listIrohaWorkers(true).find(w => w.display_name === 'すずき');

  // 一覧 (アプリ正本): 未完了だけ・拠点・保留理由つきラベル・今日やるが先頭
  const mk = (over = {}) => TD.upsertTaskFromImport({
    notion_page_id: over.page || null, status: over.status || 'not_started', facility_code: over.facility || 'iroha',
    hold_reason_code: over.hold || null, hold_reason_note: over.holdNote || null,
    destination_id: over.dest ?? null, product_code: over.code || 'PROD-A', product_name: over.name || '一覧テスト',
    qty: over.qty ?? 10, arrival_date: over.arrival || '2026-09-01',
    master_snapshot: over.snapshot === undefined ? { material_code: 'D-8', units_per_container: 180 } : over.snapshot,
    closed_at: over.status === 'closed' ? '2026-09-03T00:00:00.000Z' : null, close_reason: over.close || null,
  }, { batchId: 'test-a1b' }).id;
  const tOpen = mk({ page: 'a1b-open', code: 'PROD-A', name: '未着手カード', dest: 8801 });
  const tHold = mk({ page: 'a1b-hold', status: 'on_hold', hold: 'label_shortage', code: 'PROD-B', name: 'ラベル待ち', dest: 8802 });
  const tExt = mk({ page: 'a1b-ext', status: 'in_progress', facility: 'rashinban', code: 'PROD-C', name: '外部で作業中', dest: 8803 });
  const tDone = mk({ page: 'a1b-done', status: 'closed', close: 'stocked', code: 'PROD-D', name: '終了ずみ', dest: 8804 });
  clearEnrichCache();
  const list = S.buildTaskList();
  const byId = (id) => list.cards.find(c => c.id === id);
  ok(list.mode === 'app' && list.cards.length >= 3 && !byId(tDone), 'アプリ正本の一覧: 未完了だけ (終了は出ない)');
  ok(byId(tOpen).id === tOpen && byId(tOpen).status === 'not_started' && byId(tOpen).status_label === '未着手' && byId(tOpen).version >= 1,
    'カードは id・状態・表示名・version を持つ');
  ok(byId(tHold).status_label === '保留 · ラベル待ち' && byId(tHold).hold_reason_code === 'label_shortage', '保留は理由つきの表示名');
  ok(byId(tExt).facility_code === 'rashinban', '拠点を持つ (外部施設のバッジ用)');
  ok(byId(tOpen).master.material_code === 'D-8' && byId(tOpen).master.units_per_container === 180, '作業仕様は作成時スナップショットから (masterOfTask)');
  ok(Array.isArray(list.statuses) && list.statuses[0].value === 'not_started' && list.transitions.not_started.includes('in_progress')
    && list.holdReasons.some(r => r.value === 'label_shortage') && list.closeReasons.some(r => r.value === 'stocked')
    && list.facilities.length === 5 && /^\d{4}-\d{2}-\d{2}$/.test(list.today),
    '画面に必要な選択肢 (状態・遷移・保留理由・終了理由・拠点・今日) を返す');

  // 「今日やる」は一覧の先頭
  const planned = TD.setPlannedDate({ taskId: tHold, plannedDate: S.jstToday(), expectVersion: TD.getTask(tHold).version, actor: 'test' });
  ok(planned.ok, '前提: 「今日やる」を付ける');
  clearEnrichCache();
  const list2 = S.buildTaskList();
  ok(list2.cards[0].id === tHold && list2.cards[0].today === true, '「今日やる」が一覧の先頭に来る');
  ok(S.jstToday(new Date('2026-09-03T15:30:00Z')) === '2026-09-04', 'jstToday は JST で日付を出す (UTC 15:30 = 翌日)');

  // 作業時間 (task_id)。page_id のセッションと混ざらない
  const s1 = startSession({ taskId: tOpen, productCode: 'PROD-A', title: '未着手カード', worker: getIrohaWorker(w1.id) });
  ok(s1.ok && !s1.already, 'task で作業開始');
  ok(startSession({ taskId: tOpen, worker: getIrohaWorker(w1.id) }).already === true, '同じ task の再送は既存を返す');
  const busy = startSession({ taskId: tExt, worker: getIrohaWorker(w1.id) });
  ok(busy.ok === false && busy.error === 'busy', '別の task では二重に始められない');
  const s2 = startSession({ taskId: tOpen, productCode: 'PROD-A', worker: getIrohaWorker(w2.id) });
  ok(s2.ok && activeSessionsByTask().get(tOpen).length === 2, '複数人が同じ task で作業できる');
  ok(!activeSessionsByPage().has(String(tOpen)), 'task のセッションは page 側の一覧に出ない');
  const wrongCard = stopSession({ taskId: tExt, workerId: w1.id, sessionId: s1.sessionId, reason: 'done' });
  ok(wrongCard.ok === false && wrongCard.error === 'not_started', '別カードの id では終了できない');
  const st1 = stopSession({ taskId: tOpen, workerId: w1.id, sessionId: s1.sessionId, reason: 'done' });
  ok(st1.ok && st1.remainingActive === 1, 'task で終了 (残りの作業者を数える)');
  stopSession({ taskId: tOpen, workerId: w2.id, sessionId: s2.sessionId, reason: 'done' });
  ok(db.prepare('SELECT COUNT(*) c FROM f_iroha_work_sessions WHERE task_id = ? AND page_id IS NULL').get(tOpen).c === 2,
    'アプリ正本のセッションは page_id なしで記録される');

  // 実測 (カード単位) は task ベースでも数えられる
  db.prepare("UPDATE f_iroha_work_sessions SET raw_seconds = 600, ended_at = '2026-09-03T01:00:00.000Z' WHERE task_id = ?").run(tOpen);
  const est = estimateByProduct().get('prod-a');
  ok(est && est.cards >= 1 && est.avgSeconds > 0, '実測の集計に task のカードが入る');

  // 写真 (task_id)。「前回の完成形」は同じ商品の別 task から
  _setDriveUpload(async ({ operationId }) => ({ fileId: `f-${operationId}`, url: `https://drive/${operationId}` }));
  const jpeg = Buffer.concat([Buffer.from([0xFF, 0xD8, 0xFF, 0xE0]), Buffer.alloc(64, 1)]);
  const tmp2 = (name) => { const p = path.join(process.env.DATA_DIR, name); fs.writeFileSync(p, jpeg); return p; };
  const m1 = addMedia({ taskId: tOpen, productCode: 'PROD-A', kind: 'photo', filePath: tmp2('a1b1.jpg'), worker: getIrohaWorker(w1.id), operationId: 'op-a1b-0001' });
  ok(m1.ok && mediaByTask().get(tOpen).length === 1, 'task に写真が付く');
  // 取り消した送信は、遅れて届いても成立させない (通信が切れている間に「やめる」が通っている — Codex PR1 R18)
  {
    const { recordMediaCancel, sweepMediaCancels } = await import('../apps/iroha-work/media.js');
    ok(recordMediaCancel('op-cancel-0001', { deviceId: 31 }) === true, '取り消しを控える (行が無くても)');
    const late = addMedia({ taskId: tOpen, productCode: 'PROD-A', kind: 'photo', filePath: tmp2('late.jpg'),
      worker: getIrohaWorker(w1.id), deviceId: 31, operationId: 'op-cancel-0001' });
    ok(late.ok === false && late.error === 'cancelled', '遅れて届いた元の送信は成立しない');
    ok(db.prepare("SELECT COUNT(*) c FROM f_iroha_card_media WHERE operation_id = 'op-cancel-0001'").get().c === 0, '写真も増えていない');
    db.prepare("UPDATE f_iroha_media_cancels SET created_at = '2000-01-01T00:00:00.000Z' WHERE operation_id = 'op-cancel-0001'").run();
    ok(sweepMediaCancels() >= 1, '古い控えは片づく (既定 7 日)');
    ok(db.prepare("SELECT COUNT(*) c FROM f_iroha_media_cancels").get().c === 0, '控えは残らない');
  }
  // ⭐実体を置く前に落ちた行 (staged_at あり) は「無いもの」として扱い、再送で置き直せる (Codex PR1 R7)
  const { setMetaValue: setMeta18 } = await import('../apps/iroha-work/db.js');
  setMeta18('source_of_truth', 'app');   // 写真の公開は「そのカードにいま書けるとき」だけ (Codex PR1 R10)
  {
    const st = addMedia({ taskId: tOpen, productCode: 'PROD-A', kind: 'photo', filePath: tmp2('stage1.jpg'),
      worker: getIrohaWorker(w1.id), deviceId: 31, operationId: 'op-stage-0001', deferMove: true });
    ok(st.ok && st.move && !fs.existsSync(st.move.to), '実体はまだ置かれていない (トランザクションの外で置く)');
    ok(mediaByTask().get(tOpen).length === 1, '置く前の行は一覧に出ない');
    ok(db.prepare("SELECT COUNT(*) c FROM f_iroha_card_media WHERE staged_at IS NOT NULL").get().c === 1, '印 (staged_at) は付いている');
    const other = addMedia({ taskId: tOpen, productCode: 'PROD-A', kind: 'photo', filePath: tmp2('stage9.jpg'),
      worker: getIrohaWorker(w1.id), deviceId: 99, operationId: 'op-stage-0001', deferMove: true });
    ok(other.ok === false && other.error === 'not_ready', '別の端末からの同じ送信では置き直せない (乗っ取り防止)');
    const again = addMedia({ taskId: tOpen, productCode: 'PROD-A', kind: 'photo', filePath: tmp2('stage2.jpg'),
      worker: getIrohaWorker(w1.id), deviceId: 31, operationId: 'op-stage-0001', deferMove: true });
    ok(again.ok && again.already && again.media.id === st.media.id && again.move, '撮った端末からの再送は同じ行を返し、置き直す指示が付く');
    ok(again.claim && again.claim !== st.claim, '札は要求ごとに新しくなる (古い要求は公開できない)');
    ok(again.stale && again.stale === st.move.to, '前の札で置いたファイルを片づける指示が付く (置き場所が変わるため)');
    moveStoredFile(again.move);
    ok(promoteStagedMedia(again.media.id, again.move.to, { claim: st.claim }).reason === 'taken', '古い札では公開できない');
    const promoted = promoteStagedMedia(again.media.id, again.move.to, { claim: again.claim });
    ok(promoted.ok && fs.existsSync(again.move.to), '実体を置いてから公開する');
    ok(mediaByTask().get(tOpen).length === 2, '置いてはじめて一覧に出る');
    const twice = promoteStagedMedia(again.media.id, again.move.to, { claim: again.claim });
    ok(twice.ok === false && twice.reason === 'taken' && twice.media, '二重に公開はせず、公開済みの行を返す');
    // ⭐実体を置いている間にカードが終わっていたら公開しない (Codex PR1 R10)
    {
      const st2 = addMedia({ taskId: tOpen, productCode: 'PROD-A', kind: 'photo', filePath: tmp2('gate1.jpg'),
        worker: getIrohaWorker(w1.id), deviceId: 31, operationId: 'op-gate-00001', deferMove: true });
      moveStoredFile(st2.move);
      setMeta18('source_of_truth', 'notion');
      ok(promoteStagedMedia(st2.media.id, st2.move.to, { claim: st2.claim }).reason === 'not_writable',
        '正本が Notion に戻っていたら公開しない (下見のカードに写真が増えない)');
      setMeta18('source_of_truth', 'app');
      const wasStatus = TD.getTask(tOpen).status;
      db.prepare("UPDATE f_iroha_tasks SET status = 'closed', close_reason = 'stocked', closed_at = ?, closed_by = 'test' WHERE id = ?").run(new Date().toISOString(), tOpen);
      ok(promoteStagedMedia(st2.media.id, st2.move.to, { claim: st2.claim }).reason === 'not_writable',
        '終了したカードにも公開しない (履歴の写真が後から増えない)');
      db.prepare('UPDATE f_iroha_tasks SET status = ?, close_reason = NULL, closed_at = NULL, closed_by = NULL WHERE id = ?').run(wasStatus, tOpen);
      db.prepare("DELETE FROM f_iroha_card_media WHERE operation_id = 'op-gate-00001'").run();
      try { fs.unlinkSync(st2.move.to); } catch { /* 無ければよい */ }
      // 関門は Notion のカードに紐づく写真にも効く (page_id だけの行 — Codex PR1 R11)
      const { cardWriteBlockReason } = await import('../apps/iroha-work/media.js');
      ok(cardWriteBlockReason({ task_id: tOpen, page_id: null }) === null, 'アプリ正本の未終了カードなら書ける');
      ok(cardWriteBlockReason({ task_id: 999999, page_id: null }) === 'card_required', '無いカードには書けない');
      ok(cardWriteBlockReason({ task_id: null, page_id: 'pg-any' }) === 'notion_card_retired',
        'アプリ正本になったら、Notion のカードに紐づく写真はもう変えられない');
      // 置いている間に枠が埋まったら公開しない (Codex PR1 R11)
      const st3 = addMedia({ taskId: tOpen, productCode: 'PROD-A', kind: 'photo', filePath: tmp2('cap1.jpg'),
        worker: getIrohaWorker(w1.id), deviceId: 31, operationId: 'op-cap-000001', deferMove: true });
      moveStoredFile(st3.move);
      const insPub = db.prepare(`INSERT INTO f_iroha_card_media (operation_id, task_id, product_code, kind, mime, size,
          drive_file_id, status, worker_id, worker_name, created_at)
        VALUES (?, ?, 'PROD-A', 'photo', 'image/jpeg', 100, ?, 'uploaded', ?, 'やまだ', ?)`);
      insPub.run('op-cap-fill01', tOpen, 'f-cap-1', w1.id, new Date().toISOString());   // これで公開済みが上限の 3 枚
      ok(mediaByTask().get(tOpen).length === 3, '公開済みが 3 枚 (上限) になった');
      ok(promoteStagedMedia(st3.media.id, st3.move.to, { claim: st3.claim }).reason === 'cap_reached',
        '枠が埋まっていれば公開しない (後から上限を超えない)');
      db.prepare("DELETE FROM f_iroha_card_media WHERE operation_id IN ('op-cap-000001','op-cap-fill01')").run();
      try { fs.unlinkSync(st3.move.to); } catch { /* 無ければよい */ }
      ok(mediaByTask().get(tOpen).length === 2, '片づけて元に戻す');
      // 消された staging 行は公開しない (職員が管理画面から消した後に実体を結びつけない — Codex PR1 R12)
      {
        const st4 = addMedia({ taskId: tOpen, productCode: 'PROD-A', kind: 'photo', filePath: tmp2('del1.jpg'),
          worker: getIrohaWorker(w1.id), deviceId: 31, operationId: 'op-staged-del1', deferMove: true });
        moveStoredFile(st4.move);
        db.prepare('UPDATE f_iroha_card_media SET deleted_at = ?, deleted_by = ? WHERE id = ?')
          .run(new Date().toISOString(), 'test', st4.media.id);
        ok(promoteStagedMedia(st4.media.id, st4.move.to, { claim: st4.claim }).reason === 'gone',
          '消された行は公開できない');
        db.prepare("UPDATE f_iroha_card_media SET staged_at = '2000-01-01T00:00:00.000Z' WHERE id = ?").run(st4.media.id);
        const sw0 = sweepStagedMedia();
        ok(sw0.promoted === 0 && sw0.dropped === 0, '片づけの対象にもしない (消された行はそのまま履歴に残す)');
        ok(db.prepare('SELECT COUNT(*) c FROM f_iroha_card_media WHERE id = ?').get(st4.media.id).c === 1,
          '行そのものは消さない (論理削除の記録を残す)');
        // 消された行の実体は、十分に古くなってから片づける (行は残したまま)
        const { sweepOrphanFiles } = await import('../apps/iroha-work/media.js');
        ok(sweepOrphanFiles().removed === 0, '新しいファイルには触らない (送信中・再送中を巻き込まない)');
        ok(fs.existsSync(st4.move.to), 'まだ実体は残っている');
        const old = new Date(Date.now() - 48 * 3600 * 1000);
        fs.utimesSync(st4.move.to, old, old);
        ok(sweepOrphanFiles().removed === 1 && !fs.existsSync(st4.move.to),
          '十分に古くなれば、消された行の実体は片づける');
        // 受信中の一時領域 (MEDIA_DIR/tmp) に取り残されたファイルも片づける (Codex PR1 R16)
        {
          const tmpDir = path.join(process.env.DATA_DIR, 'iroha-media', 'tmp');
          fs.mkdirSync(tmpDir, { recursive: true });
          const leftover = path.join(tmpDir, 'multer-leftover');
          fs.writeFileSync(leftover, jpeg);
          ok(sweepOrphanFiles().removed === 0 && fs.existsSync(leftover), '受信中のものには触らない');
          fs.utimesSync(leftover, old, old);
          ok(sweepOrphanFiles().removed === 1 && !fs.existsSync(leftover), '十分に古くなれば片づける');
        }
        db.prepare('DELETE FROM f_iroha_card_media WHERE id = ?').run(st4.media.id);
      }
    }
    // 後の検査 (「前回の完成形」の枚数) に影響させないよう片づける
    db.prepare('DELETE FROM f_iroha_card_media WHERE operation_id = ?').run('op-stage-0001');
    try { fs.unlinkSync(again.move.to); } catch { /* 無ければよい */ }
    ok(mediaByTask().get(tOpen).length === 1, '片づけたので元の 1 枚に戻る');
  }
  // ⭐置き去りの staging 行を片づける (iPad の再読込などで再送が来なくなった分 — Codex PR1 R8)
  {
    const before = countActiveMedia({ taskId: tOpen }, 'photo');
    const g1 = addMedia({ taskId: tOpen, productCode: 'PROD-A', kind: 'photo', filePath: tmp2('sweep1.jpg'),
      worker: getIrohaWorker(w1.id), deviceId: 31, operationId: 'op-sweep-0001', deferMove: true });
    const g2 = addMedia({ taskId: tOpen, productCode: 'PROD-A', kind: 'photo', filePath: tmp2('sweep2.jpg'),
      worker: getIrohaWorker(w1.id), deviceId: 31, operationId: 'op-sweep-0002', deferMove: true });
    ok(countActiveMedia({ taskId: tOpen }, 'photo') === before + 2, '送信中の行は枚数上限に数える (並行送信で上限を超えない)');
    moveStoredFile(g2.move);   // 実体は置けたが、公開の前に落ちた
    db.prepare("UPDATE f_iroha_card_media SET staged_at = ? WHERE operation_id IN ('op-sweep-0001','op-sweep-0002')")
      .run('2000-01-01T00:00:00.000Z');
    ok(countActiveMedia({ taskId: tOpen }, 'photo') === before, '置き去りになった古い行は枠に数えない (sweep 待ちで詰まらせない)');
    const sw = sweepStagedMedia();
    ok(sw.promoted === 1 && sw.dropped === 1, '実体が置けているものは公開し、無いものは破棄する');
    ok(db.prepare("SELECT COUNT(*) c FROM f_iroha_card_media WHERE operation_id = 'op-sweep-0001'").get().c === 0, '実体の無い行は残らない');
    ok(mediaByTask().get(tOpen).some((m) => m.id === g2.media.id), '実体のある行は一覧に出る');
    db.prepare("DELETE FROM f_iroha_card_media WHERE operation_id = 'op-sweep-0002'").run();
    try { fs.unlinkSync(g2.move.to); } catch { /* 無ければよい */ }
    ok(mediaByTask().get(tOpen).length === 1, '片づけて元に戻す');
  }
  setMeta18('source_of_truth', null);   // 正本は既定 (Notion) に戻す
  await processMediaQueue();
  const tNext = mk({ page: 'a1b-next', code: 'PROD-A', name: '次の入荷 (同じ商品)', dest: 8805 });
  clearEnrichCache();
  const prev = S.buildTaskList().cards.find(c => c.id === tNext).previous_photos;
  ok(prev.length === 1 && prev[0].id === m1.media.id, '同じ商品の別 task の写真が「前回の完成形」に出る');
  ok(S.buildTaskList().cards.find(c => c.id === tOpen).previous_photos.length === 0, '自分の task の写真は「前回」に含めない');
  _setDriveUpload(null);

  // 正本の切替 (meta)
  ok(getMeta('source_of_truth') == null, '既定は Notion 正本 (meta 未設定)');
  setMetaValue('source_of_truth', 'app');
  ok(getMeta('source_of_truth') === 'app', '切替は meta に持つ (再起動しても続く)');
  setMetaValue('source_of_truth', null);
  // ここから先はタスクを書き換える (アプリ正本のときだけ通る — Codex PR1 R15)。節の終わりで戻す
  setMetaValue('source_of_truth', 'app');

  // 作業開始 (アプリ正本) は 1 トランザクション: 終了カードでは始めない・未着手→作業中が同時に確定 (Codex A1b R1 #2)
  {
    const tStart = mk({ page: 'a1b-start', code: 'PROD-S', name: '開始テスト', dest: 8806 });
    const r1 = TD.startTaskSession({ taskId: tStart, worker: getIrohaWorker(w1.id), deviceLabel: 'ipad-1', snapshotOf: () => ({ material_code: 'D-1' }) });
    ok(r1.ok && !r1.already && r1.task.status === 'in_progress' && r1.task.started_at, '開始でセッションが作られ、未着手→作業中になる');
    ok(JSON.parse(db.prepare('SELECT master_snapshot FROM f_iroha_work_sessions WHERE id = ?').get(r1.sessionId).master_snapshot).material_code === 'D-1', '開始時スナップショットは snapshotOf の値');
    const r2 = TD.startTaskSession({ taskId: tStart, worker: getIrohaWorker(w1.id) });
    ok(r2.ok && r2.already && r2.sessionId === r1.sessionId, '再送は既存セッション (already)');
    stopSession({ taskId: tStart, workerId: w1.id, sessionId: r1.sessionId, reason: 'done' });
    const cur = TD.getTask(tStart);
    const closed = TD.changeTaskStatus({ taskId: tStart, to: 'closed', closeReason: 'stocked', expectVersion: cur.version, isStaff: true, actor: 'test' });
    ok(closed.ok, '前提: 終了にする');
    const nBefore = db.prepare('SELECT COUNT(*) c FROM f_iroha_work_sessions WHERE task_id = ?').get(tStart).c;
    const r3 = TD.startTaskSession({ taskId: tStart, worker: getIrohaWorker(w1.id) });
    ok(r3.ok === false && r3.error === 'done_card', '終了したカードでは始められない');
    ok(db.prepare('SELECT COUNT(*) c FROM f_iroha_work_sessions WHERE task_id = ?').get(tStart).c === nBefore, '終了カードにセッションは増えない');
    ok(TD.startTaskSession({ taskId: 999999, worker: getIrohaWorker(w1.id) }).error === 'not_found', '無いカードは not_found');
    // 状態変更が通らないときはセッションごと戻す (ロールバック経路を本当に踏む — Codex A1b R2 Low):
    // セッション INSERT の直後に「別端末が同じタスクを変えた」(version が進む) をフックで起こす
    const tRb = mk({ page: 'a1b-rollback', code: 'PROD-S', name: '巻き戻し', dest: 8807 });
    const before = { sessions: db.prepare('SELECT COUNT(*) c FROM f_iroha_work_sessions').get().c,
      events: db.prepare('SELECT COUNT(*) c FROM f_iroha_app_events').get().c, task: JSON.stringify(TD.getTask(tRb)) };
    TD._setStartTaskSessionHook((t) => { db.prepare('UPDATE f_iroha_tasks SET version = version + 1 WHERE id = ?').run(t.id); });
    let r4;
    try { r4 = TD.startTaskSession({ taskId: tRb, worker: getIrohaWorker(w1.id) }); } finally { TD._setStartTaskSessionHook(null); }
    ok(r4.ok === false && r4.error === 'conflict' && r4.current, '状態変更が競合したら開始は失敗 (conflict・現在値つき)');
    ok(db.prepare('SELECT COUNT(*) c FROM f_iroha_work_sessions').get().c === before.sessions, 'セッションは残らない (ロールバック)');
    ok(db.prepare('SELECT COUNT(*) c FROM f_iroha_app_events').get().c === before.events, '履歴 (session_start) も残らない');
    ok(JSON.stringify({ ...TD.getTask(tRb), version: JSON.parse(before.task).version }) === before.task && TD.getTask(tRb).status === 'not_started', 'タスクは未着手のまま (フックの version 更新も戻る)');
    ok(TD.getTask(tRb).version === JSON.parse(before.task).version, 'version も元のまま');
    const r5 = TD.startTaskSession({ taskId: tRb, worker: getIrohaWorker(w1.id) });
    ok(r5.ok && r5.task.status === 'in_progress', 'フックを外せば通る');
    stopSession({ taskId: tRb, workerId: w1.id, sessionId: r5.sessionId, reason: 'done' });
  }

  // 正本を app にしてからの記録の数 (Notion に戻す前の警告)
  {
    const since = new Date(Date.now() - 60_000).toISOString();
    const c = TD.countChangesSince(since);
    ok(c.tasks > 0 && c.updatedTasks > 0 && c.sessions > 0 && c.media > 0, 'countChangesSince は状態変更の回数・更新タスク数・作業時間・写真を数える');
    ok(c.tasks === db.prepare("SELECT COUNT(*) c FROM f_iroha_app_events WHERE action = 'task_status' AND ok = 1 AND at > ?").get(since).c, '状態変更は履歴の回数 (同じタスクの 2 回は 2)');
    // [17] の旧版タスク行はダミーの時刻 ('x') を持つ — 文字列比較で未来扱いになるので、ここでは実時刻に直してから数える
    db.prepare("UPDATE f_iroha_tasks SET updated_at = created_at WHERE updated_at NOT LIKE '20%'").run();
    db.prepare("UPDATE f_iroha_tasks SET updated_at = '2026-01-01T00:00:00.000Z' WHERE updated_at NOT LIKE '20%'").run();
    const z = TD.countChangesSince(new Date(Date.now() + 3600_000).toISOString());
    ok(z.tasks === 0 && z.updatedTasks === 0 && z.sessions === 0 && z.media === 0, '未来を起点にすると 0');
    const edge = db.prepare('SELECT MAX(started_at) m FROM f_iroha_work_sessions WHERE task_id IS NOT NULL').get().m;
    ok(TD.countChangesSince(edge).sessions >= 1, '切替と同じミリ秒の記録も数える (境界は >=)');
  }

  // 写真の再送は同じカードのときだけ (Codex A1b R1 #3)。task の行は Notion 同期キューに入らない (同 #8)
  {
    const { softDeleteMedia } = await import('../apps/iroha-work/media.js');
    const other = mk({ page: 'a1b-other', code: 'PROD-Z', name: '別カード', dest: 8808 });
    const conflict = addMedia({ taskId: other, productCode: 'PROD-Z', kind: 'photo', filePath: tmp2('a1b2.jpg'), worker: getIrohaWorker(w1.id), operationId: 'op-a1b-0001' });
    ok(conflict.ok === false && conflict.error === 'operation_conflict', '別カードで同じ operation_id を送ると operation_conflict');
    const conflict2 = addMedia({ pageId: 'some-page', productCode: 'PROD-A', kind: 'photo', filePath: tmp2('a1b3.jpg'), worker: getIrohaWorker(w1.id), operationId: 'op-a1b-0001' });
    ok(conflict2.ok === false && conflict2.error === 'operation_conflict', 'Notion カードとして同じ operation_id を送っても返さない');
    const both = addMedia({ pageId: 'p', taskId: other, productCode: 'PROD-Z', kind: 'photo', filePath: tmp2('a1b4.jpg'), worker: getIrohaWorker(w1.id), operationId: 'op-a1b-0002' });
    ok(both.ok === false && both.error === 'bad_request', 'page_id と task_id の両方は bad_request');
    const same = addMedia({ taskId: tOpen, productCode: 'PROD-A', kind: 'photo', filePath: tmp2('a1b5.jpg'), worker: getIrohaWorker(w1.id), operationId: 'op-a1b-0001' });
    ok(same.ok && same.already && same.media.id === m1.media.id, '同じカードの再送は既存行 (冪等)');
    const syncBefore = db.prepare('SELECT COUNT(*) c FROM f_iroha_media_page_sync').get().c;
    ok(db.prepare('SELECT status FROM f_iroha_card_media WHERE id = ?').get(m1.media.id).status === 'uploaded', 'task の写真は Drive 保存 (uploaded) が最終状態');
    ok(db.prepare('SELECT COUNT(*) c FROM f_iroha_media_page_sync WHERE page_id IS NULL').get().c === 0, '同期キューに NULL ページが積まれていない');
    const del = softDeleteMedia(m1.media.id, { isSession: true, actor: 'test' });
    ok(del.ok && db.prepare('SELECT COUNT(*) c FROM f_iroha_media_page_sync').get().c === syncBefore, 'task の写真を削除しても同期キューは増えない');
  }

  // 古い版の作業時間・写真は起動時に作り直す (Codex A1b R1 #4 #5 + Low)。
  //   (a) 最初の版 = page_id NOT NULL・task_id 無し。task の行は入れられないので、page の行だけの状態を作って検証する
  //   (b) 途中の版 = page_id NULL 可・task_id 列あり・CHECK/FK 無し → 全行そのまま (id 含む) 保って作り直す
  //   (c) FK 違反 (存在しない task_id) があれば全部戻す
  {
    const { createTables } = await import('../apps/iroha-work/db.js');
    const sqlOf = (t) => db.prepare("SELECT sql FROM sqlite_master WHERE type='table' AND name = ?").get(t)?.sql || '';
    const rowsOf = (t) => JSON.stringify(db.prepare(`SELECT * FROM ${t} ORDER BY id`).all());
    const OLD_SESSIONS = `CREATE TABLE f_iroha_work_sessions (id INTEGER PRIMARY KEY AUTOINCREMENT, page_id TEXT NOT NULL, product_code TEXT, title_snapshot TEXT,
        worker_id INTEGER NOT NULL, worker_name TEXT NOT NULL, device_label TEXT, started_at TEXT NOT NULL, ended_at TEXT, end_reason TEXT,
        raw_seconds INTEGER, voided_at TEXT, voided_by TEXT, void_reason TEXT, master_snapshot TEXT)`;
    const OLD_MEDIA = `CREATE TABLE f_iroha_card_media (id INTEGER PRIMARY KEY AUTOINCREMENT, operation_id TEXT NOT NULL UNIQUE, page_id TEXT NOT NULL,
        product_code TEXT, kind TEXT NOT NULL, mime TEXT, size INTEGER, local_path TEXT, drive_file_id TEXT, drive_url TEXT, status TEXT NOT NULL, error TEXT,
        attempt_count INTEGER NOT NULL DEFAULT 0, next_retry_at TEXT, worker_id INTEGER, worker_name TEXT, device_label TEXT, created_at TEXT NOT NULL,
        uploaded_at TEXT, synced_at TEXT, deleted_at TEXT, deleted_by TEXT, delete_token_hash TEXT, uploader_device_id INTEGER, unavailable_at TEXT)`;
    const sessCols = 'id, page_id, product_code, title_snapshot, worker_id, worker_name, device_label, started_at, ended_at, end_reason, raw_seconds, voided_at, voided_by, void_reason, master_snapshot';
    const mediaCols = 'id, operation_id, page_id, product_code, kind, mime, size, local_path, drive_file_id, drive_url, status, error, attempt_count, next_retry_at, worker_id, worker_name, device_label, created_at, uploaded_at, synced_at, deleted_at, deleted_by, delete_token_hash, uploader_device_id, unavailable_at';

    // (a) 最初の版 — task の行を退避し、page の行だけで旧テーブルを組む
    db.pragma('foreign_keys = OFF');
    db.exec(`CREATE TEMP TABLE keep_s AS SELECT * FROM f_iroha_work_sessions WHERE page_id IS NULL;
      CREATE TEMP TABLE keep_m AS SELECT * FROM f_iroha_card_media WHERE page_id IS NULL;
      CREATE TEMP TABLE old_s AS SELECT * FROM f_iroha_work_sessions WHERE page_id IS NOT NULL;
      CREATE TEMP TABLE old_m AS SELECT * FROM f_iroha_card_media WHERE page_id IS NOT NULL;
      DROP TABLE f_iroha_work_sessions; DROP TABLE f_iroha_card_media;
      ${OLD_SESSIONS}; ${OLD_MEDIA};
      INSERT INTO f_iroha_work_sessions (${sessCols}) SELECT ${sessCols} FROM old_s;
      INSERT INTO f_iroha_card_media (${mediaCols}) SELECT ${mediaCols} FROM old_m;`);
    db.pragma('foreign_keys = ON');
    const pageRowsS = rowsOf('f_iroha_work_sessions'), pageRowsM = rowsOf('f_iroha_card_media');
    ok(/page_id TEXT NOT NULL/.test(sqlOf('f_iroha_work_sessions')) && !/task_id/.test(sqlOf('f_iroha_card_media')), '前提: 最初の版 (page_id NOT NULL・task_id 無し)');
    createTables(db);
    const newS = sqlOf('f_iroha_work_sessions'), newM = sqlOf('f_iroha_card_media');
    ok(/task_id\s+INTEGER REFERENCES f_iroha_tasks\(id\)/.test(newS) && /CHECK \(page_id IS NOT NULL OR task_id IS NOT NULL\)/.test(newS)
      && !/page_id\s+TEXT NOT NULL/.test(newS), '作業時間: 新しい定義 (task_id FK・CHECK・page_id NULL 可) で作り直す');
    ok(/task_id\s+INTEGER REFERENCES f_iroha_tasks\(id\)/.test(newM) && /CHECK \(page_id IS NOT NULL OR task_id IS NOT NULL\)/.test(newM)
      && /operation_id\s+TEXT NOT NULL UNIQUE/.test(newM), '写真: 同上 (UNIQUE も残る)');
    const strip = (j) => JSON.stringify(JSON.parse(j).map((r) => { const { task_id, staged_at, staged_claim, delete_token_hash_prev, delete_token_hashes, ...rest } = r; return rest; }));
    ok(strip(rowsOf('f_iroha_work_sessions')) === pageRowsS && strip(rowsOf('f_iroha_card_media')) === pageRowsM, '行 (id・全列) はそのまま。task_id は NULL');
    ok(db.prepare("SELECT COUNT(*) c FROM sqlite_master WHERE type='index' AND name IN ('idx_iroha_sessions_task','idx_iroha_media_task','idx_iroha_sessions_page','idx_iroha_media_page')").get().c === 4, '索引 4 本が作り直される');
    let bothNull = null;
    try { db.prepare("INSERT INTO f_iroha_work_sessions (worker_id, worker_name, started_at) VALUES (1, 'x', 'y')").run(); } catch (e) { bothNull = e; }
    ok(bothNull && /CHECK/.test(bothNull.message), 'page_id も task_id も無い行は入らない (CHECK)');
    let badFk = null;
    try { db.prepare("INSERT INTO f_iroha_work_sessions (task_id, worker_id, worker_name, started_at) VALUES (999999, 1, 'x', 'y')").run(); } catch (e) { badFk = e; }
    ok(badFk && /FOREIGN KEY/.test(badFk.message), '存在しない task_id は入らない (FK)');
    // 退避した task の行を戻す (id そのまま)
    const keepColsS = db.prepare('PRAGMA table_info(keep_s)').all().map((c) => c.name).join(', ');
    const keepColsM = db.prepare('PRAGMA table_info(keep_m)').all().map((c) => c.name).join(', ');
    db.exec(`INSERT INTO f_iroha_work_sessions (${keepColsS}) SELECT ${keepColsS} FROM keep_s;
      INSERT INTO f_iroha_card_media (${keepColsM}) SELECT ${keepColsM} FROM keep_m;
      DROP TABLE keep_s; DROP TABLE keep_m; DROP TABLE old_s; DROP TABLE old_m;`);
    const maxId = db.prepare('SELECT MAX(id) m FROM f_iroha_work_sessions').get().m;
    const ins = db.prepare("INSERT INTO f_iroha_work_sessions (task_id, worker_id, worker_name, started_at, ended_at) VALUES (?, 1, 'x', 'y', 'z')").run(tOpen);
    ok(Number(ins.lastInsertRowid) > maxId, '作り直し後も id は続き番号 (sqlite_sequence が引き継がれる)');
    db.prepare('DELETE FROM f_iroha_work_sessions WHERE id = ?').run(Number(ins.lastInsertRowid));

    // (b) 途中の版 (page_id NULL 可・task_id あり・CHECK/FK 無し) — 全行 (task の行も) そのまま保って作り直す
    const allS = rowsOf('f_iroha_work_sessions'), allM = rowsOf('f_iroha_card_media');
    const MID_SESSIONS = OLD_SESSIONS.replace('page_id TEXT NOT NULL', 'page_id TEXT, task_id INTEGER');
    const MID_MEDIA = OLD_MEDIA.replace('page_id TEXT NOT NULL', 'page_id TEXT, task_id INTEGER').replace('unavailable_at TEXT)', 'unavailable_at TEXT, staged_at TEXT, staged_claim TEXT, delete_token_hash_prev TEXT, delete_token_hashes TEXT)');
    const allColsS = db.prepare('PRAGMA table_info(f_iroha_work_sessions)').all().map((c) => c.name).join(', ');
    const allColsM = db.prepare('PRAGMA table_info(f_iroha_card_media)').all().map((c) => c.name).join(', ');
    db.pragma('foreign_keys = OFF');
    db.exec(`CREATE TEMP TABLE mid_s AS SELECT * FROM f_iroha_work_sessions; CREATE TEMP TABLE mid_m AS SELECT * FROM f_iroha_card_media;
      DROP TABLE f_iroha_work_sessions; DROP TABLE f_iroha_card_media; ${MID_SESSIONS}; ${MID_MEDIA};
      INSERT INTO f_iroha_work_sessions (${allColsS}) SELECT ${allColsS} FROM mid_s;
      INSERT INTO f_iroha_card_media (${allColsM}) SELECT ${allColsM} FROM mid_m; DROP TABLE mid_s; DROP TABLE mid_m;`);
    db.pragma('foreign_keys = ON');
    ok(!/CHECK/.test(sqlOf('f_iroha_work_sessions')) && !/REFERENCES/.test(sqlOf('f_iroha_card_media')), '前提: 途中の版 (page_id NULL 可だが CHECK/FK 無し)');
    // (c) まず FK 違反を仕込む → 全部戻ること
    db.prepare("INSERT INTO f_iroha_work_sessions (task_id, worker_id, worker_name, started_at) VALUES (999999, 1, 'orphan', 'y')").run();
    let thrown = null;
    try { createTables(db); } catch (e) { thrown = e; }
    ok(thrown && /FK 違反/.test(thrown.message), '存在しない task_id があると作り直しを中止する');
    ok(!/CHECK/.test(sqlOf('f_iroha_work_sessions')) && db.prepare("SELECT COUNT(*) c FROM sqlite_master WHERE name LIKE 'f_iroha_%__new'").get().c === 0,
      '中止したら古い表のまま・作業表も残らない (全部戻る)');
    ok(db.pragma('foreign_keys', { simple: true }) === 1, '中止しても foreign_keys は ON に戻る');
    db.prepare("DELETE FROM f_iroha_work_sessions WHERE worker_name = 'orphan'").run();
    createTables(db);
    ok(/CHECK \(page_id IS NOT NULL OR task_id IS NOT NULL\)/.test(sqlOf('f_iroha_work_sessions')) && /REFERENCES f_iroha_tasks/.test(sqlOf('f_iroha_card_media')), '途中の版も新しい定義に作り直す');
    ok(rowsOf('f_iroha_work_sessions') === allS && rowsOf('f_iroha_card_media') === allM, '全行 (task の行・id 含む) がそのまま残る');
    createTables(db);
    ok(db.prepare("SELECT COUNT(*) c FROM sqlite_master WHERE name LIKE 'f_iroha_work_sessions%'").get().c === 1 && rowsOf('f_iroha_work_sessions') === allS, '2 回目は何もしない (冪等)');

    // (d) task_id FK と紐付け CHECK はあるが、既存の UNIQUE / CHECK だけ欠ける版 (Codex A1b R2 #2)
    const LATE_SESSIONS = sqlOf('f_iroha_work_sessions').replace("end_reason     TEXT CHECK (end_reason IS NULL OR end_reason IN ('done','pause','admin'))", 'end_reason TEXT');
    const LATE_MEDIA = sqlOf('f_iroha_card_media').replace('operation_id  TEXT NOT NULL UNIQUE', 'operation_id TEXT NOT NULL')
      .replace("status        TEXT NOT NULL CHECK (status IN ('stored','uploaded','synced'))", 'status TEXT NOT NULL')
      .replace("kind          TEXT NOT NULL CHECK (kind IN ('photo','video'))", 'kind TEXT NOT NULL');
    ok(!/end_reason IN/.test(LATE_SESSIONS) && !/UNIQUE/.test(LATE_MEDIA) && !/status IN/.test(LATE_MEDIA) && !/kind IN/.test(LATE_MEDIA) && /task_id\s+INTEGER REFERENCES/.test(LATE_MEDIA), '前提: 制約だけ欠けた版を用意');
    db.pragma('foreign_keys = OFF');
    db.exec(`CREATE TEMP TABLE late_s AS SELECT * FROM f_iroha_work_sessions; CREATE TEMP TABLE late_m AS SELECT * FROM f_iroha_card_media;
      DROP TABLE f_iroha_work_sessions; DROP TABLE f_iroha_card_media; ${LATE_SESSIONS}; ${LATE_MEDIA};
      INSERT INTO f_iroha_work_sessions (${allColsS}) SELECT ${allColsS} FROM late_s;
      INSERT INTO f_iroha_card_media (${allColsM}) SELECT ${allColsM} FROM late_m; DROP TABLE late_s; DROP TABLE late_m;`);
    db.pragma('foreign_keys = ON');
    createTables(db);
    ok(/end_reason IN \('done','pause','admin'\)/.test(sqlOf('f_iroha_work_sessions')) && /operation_id\s+TEXT NOT NULL UNIQUE/.test(sqlOf('f_iroha_card_media'))
      && /status IN \('stored','uploaded','synced'\)/.test(sqlOf('f_iroha_card_media')) && /kind IN \('photo','video'\)/.test(sqlOf('f_iroha_card_media')), 'UNIQUE / CHECK (end_reason・status・kind) だけ欠けた版も作り直す');
    ok(rowsOf('f_iroha_work_sessions') === allS && rowsOf('f_iroha_card_media') === allM, '行はそのまま');
  }
}

setMetaValue('source_of_truth', null);   // [18] で立てた正本を既定 (Notion) に戻す
console.log('\n[19] HTTP (アプリ正本): 端末登録 → 一覧 → 開始 → 終了 → 状態変更 → 写真 (Notion API を呼ばない)');
{
  const express = (await import('express')).default;
  const http = await import('node:http');
  const TD = await import('../apps/iroha-work/tasks-db.js');
  const { _setDriveUpload, processMediaQueue } = await import('../apps/iroha-work/media.js');
  const { default: router } = await import('../apps/iroha-work/router.js');
  const app = express();
  // 管理者セッションの代わり (ヘッダで注入。本番は express-session が入れる)
  app.use((req, res, next) => {
    if (req.headers['x-test-admin'] === '1') req.session = { authenticated: true, email: 'admin@test.local', role: 'admin', allowedApps: '*' };
    next();
  });
  app.use('/apps/iroha-work', express.json({ limit: '256kb' }), router);
  const server = await new Promise((r) => { const sv = http.createServer(app).listen(0, '127.0.0.1', () => r(sv)); });
  const port = server.address().port;
  const origin = `http://127.0.0.1:${port}`;
  // global.fetch は Notion モックなので、HTTP は node:http で直接叩く
  function call(method, p, { body = null, headers = {}, cookie = null } = {}) {
    return new Promise((resolve, reject) => {
      const data = body == null ? null : Buffer.isBuffer(body) ? body : Buffer.from(JSON.stringify(body));
      const h = { origin, ...(cookie ? { cookie } : {}), ...headers };
      if (data && !h['content-type']) h['content-type'] = 'application/json';
      if (data) h['content-length'] = data.length;
      const req = http.request({ host: '127.0.0.1', port, path: '/apps/iroha-work' + p, method, headers: h }, (res) => {
        const chunks = [];
        res.on('data', (c) => chunks.push(c));
        res.on('end', () => {
          const text = Buffer.concat(chunks).toString('utf8');
          let json = null; try { json = JSON.parse(text); } catch { /* HTML 等 */ }
          resolve({ status: res.statusCode, json, text, headers: res.headers });
        });
      });
      req.on('error', reject);
      if (data) req.write(data);
      req.end();
    });
  }
  function multipart(fields, file) {
    const b = '----iwtest' + Date.now();
    const parts = [];
    for (const [k, v] of Object.entries(fields)) parts.push(Buffer.from(`--${b}\r\nContent-Disposition: form-data; name="${k}"\r\n\r\n${v}\r\n`));
    parts.push(Buffer.from(`--${b}\r\nContent-Disposition: form-data; name="file"; filename="${file.name}"\r\nContent-Type: ${file.mime}\r\n\r\n`), file.data, Buffer.from(`\r\n--${b}--\r\n`));
    return { body: Buffer.concat(parts), headers: { 'content-type': `multipart/form-data; boundary=${b}` } };
  }
  const notionCalls = () => mock.queryCalls + mock.patched.length + mock.dbPatches;
  const calls0 = notionCalls();
  const w1 = listIrohaWorkers(true).find((w) => w.display_name === 'やまだ');

  // 端末登録 (コード → Cookie)
  ok((await call('GET', '/api/state')).status === 401, '未登録の端末は 401');
  const code = createEnrollCode('テストiPad', 'test').code;
  const redeem = await call('POST', '/enroll/redeem', { body: { code } });
  const cookie = (redeem.headers['set-cookie'] || []).map((c) => c.split(';')[0]).find((c) => c.startsWith('iw_device='));
  ok(redeem.status === 200 && redeem.json.ok && cookie, '登録コードで端末 Cookie が出る');

  // 正本の切替は管理者だけ・CSRF あり・未完了タスクがあれば app へ (HTTP で通す — Codex A1b R2 Low)
  const admin = { headers: { 'x-test-admin': '1' } };
  ok((await call('POST', '/admin/source', { cookie, body: { to: 'app', confirm: 'SWITCH' } })).status === 403, '端末 Cookie だけでは切り替えられない (管理者のみ)');
  ok((await call('POST', '/admin/source', { headers: { ...admin.headers, origin: 'http://evil.example' }, body: { to: 'app', confirm: 'SWITCH' } })).status === 403, '別 Origin は拒否');
  ok((await call('POST', '/admin/source', { ...admin, body: { to: 'app' } })).json.error === 'confirm_required', 'confirm=SWITCH が要る');
  const sw = await call('POST', '/admin/source', { ...admin, body: { to: 'app', confirm: 'SWITCH' } });
  ok(sw.status === 200 && sw.json.ok && sw.json.source === 'app' && sw.json.openTasks > 0 && getMeta('source_of_truth') === 'app', 'notion → app に切替 (未完了あり)');
  const switchedAt = getMeta('source_switched_at');
  ok(switchedAt && Date.now() - Date.parse(switchedAt) < 10_000, '切替時刻が meta に残る');
  ok((await call('POST', '/admin/source', { ...admin, body: { to: 'app', confirm: 'SWITCH' } })).json.unchanged === true, '同じ正本への切替は何もしない');
  // 監査ログが書けなければ正本も切替時刻も戻る (1 トランザクション — Codex A1b R3 #1)
  {
    const auditN = db.prepare("SELECT COUNT(*) c FROM f_iroha_app_events WHERE action = 'source_switch'").get().c;
    TD._setSwitchSourceHook(() => { throw new Error('audit insert failed (test)'); });
    let failed;
    try { failed = await call('POST', '/admin/source', { ...admin, body: { to: 'notion', confirm: 'SWITCH', force: true } }); } finally { TD._setSwitchSourceHook(null); }
    ok(failed.status === 500 && failed.json.ok === false, '監査ログが書けないと 500');
    ok(getMeta('source_of_truth') === 'app' && getMeta('source_switched_at') === switchedAt, '正本も切替時刻も戻る');
    ok(db.prepare("SELECT COUNT(*) c FROM f_iroha_app_events WHERE action = 'source_switch'").get().c === auditN, '監査ログも増えない');
  }
  process.env.GOOGLE_SERVICE_ACCOUNT_KEY = 'test-key';   // 実際の Drive は _setDriveUpload で差し替える
  _setDriveUpload(async ({ operationId }) => ({ fileId: `f-${operationId}`, url: `https://drive/${operationId}` }));
  try {
    const task = TD.upsertTaskFromImport({ notion_page_id: 'http-1', status: 'not_started', facility_code: 'iroha', destination_id: 8901,
      product_code: 'HTTP-A', product_name: 'HTTP テスト', qty: 3, arrival_date: '2026-09-03', master_snapshot: null }, { batchId: 'test-http' });
    const st = await call('GET', '/api/state', { cookie });
    const card = st.json?.cards?.find((c) => c.id === task.id);
    ok(st.status === 200 && st.json.mode === 'app' && card && card.status === 'not_started' && card.status_label === '未着手', '一覧はアプリ正本 (mode=app・id で引ける)');
    ok(Array.isArray(st.json.statuses) && st.json.statuses[0].value === 'not_started' && st.json.transitions && st.json.holdReasons && st.json.today, '状態・遷移・保留理由・今日を返す');
    ok((await call('POST', '/api/sessions/start', { cookie, headers: { origin: 'http://evil.example' }, body: { id: String(task.id), worker_id: w1.id } })).status === 403, '別 Origin からの変更は拒否 (CSRF)');
    const start = await call('POST', '/api/sessions/start', { cookie, body: { id: String(task.id), worker_id: w1.id } });
    ok(start.status === 200 && start.json.ok && start.json.sessionId && start.json.status === 'in_progress', '開始 (文字列の id) → 作業中');
    const stop = await call('POST', '/api/sessions/stop', { cookie, body: { id: task.id, worker_id: w1.id, session_id: start.json.sessionId, reason: 'done' } });
    ok(stop.status === 200 && stop.json.ok && stop.json.task.status === 'in_progress', '終了 (数値の id)');
    const cur = TD.getTask(task.id);
    const hold = await call('POST', '/api/status', { cookie, body: { id: String(task.id), worker_id: w1.id, to: 'on_hold', hold_reason: 'label_shortage', expect_version: cur.version } });
    ok(hold.status === 200 && hold.json.ok && hold.json.status === 'on_hold' && hold.json.status_label === '保留 · ラベル待ち' && hold.json.listed === true, '状態変更 → 保留 (ラベル待ち)');
    const stale = await call('POST', '/api/status', { cookie, body: { id: task.id, worker_id: w1.id, to: 'in_progress', expect_version: cur.version } });
    ok(stale.status === 409 && stale.json.error === 'conflict' && stale.json.current?.status === 'on_hold', '古い version は競合 (現在値つき)');
    for (const bad of ['1.5', '-1', 'abc', '0', '', '9007199254740992', '1e3']) {
      const r = await call('POST', '/api/status', { cookie, body: { id: bad, worker_id: w1.id, to: 'in_progress', expect_version: 1 } });
      if (!(r.status === 400 && r.json.error === 'bad_request')) ok(false, `不正な id "${bad}" は 400 (実際 ${r.status} ${r.json?.error})`);
    }
    ok((await call('POST', '/api/status', { cookie, body: { id: '1.5', worker_id: w1.id, to: 'in_progress', expect_version: 1 } })).status === 400, '不正な id (小数・負数・文字・0・空・2^53 超・指数表記) は 400');
    ok((await call('POST', '/api/planned', { ...admin, body: { id: 'x', worker_id: w1.id } })).status === 400 && (await call('GET', '/api/label-waits?task_id=abc', { cookie })).status === 400, '今日やる・ラベル待ちも id を検査する');
    const jpeg = Buffer.concat([Buffer.from([0xFF, 0xD8, 0xFF, 0xE0]), Buffer.alloc(64, 1)]);
    const mp = multipart({ id: String(task.id), kind: 'photo', worker_id: String(w1.id), operation_id: 'op-http-00001' }, { name: 'a.jpg', mime: 'image/jpeg', data: jpeg });
    const up = await call('POST', '/api/media', { cookie, body: mp.body, headers: mp.headers });
    ok(up.status === 200 && up.json.ok && up.json.media && up.json.deleteToken, '写真アップロード (task に付く)');
    ok(db.prepare('SELECT task_id, page_id FROM f_iroha_card_media WHERE id = ?').get(up.json.media.id).task_id === task.id, '写真の行は task_id 付き・page_id なし');
    const other = TD.upsertTaskFromImport({ notion_page_id: 'http-2', status: 'not_started', facility_code: 'iroha', destination_id: 8902,
      product_code: 'HTTP-B', product_name: '別カード', qty: 1, arrival_date: '2026-09-03', master_snapshot: null }, { batchId: 'test-http' });
    const mp2 = multipart({ id: String(other.id), kind: 'photo', worker_id: String(w1.id), operation_id: 'op-http-00001' }, { name: 'b.jpg', mime: 'image/jpeg', data: jpeg });
    const dup = await call('POST', '/api/media', { cookie, body: mp2.body, headers: mp2.headers });
    ok(dup.status === 409 && dup.json.error === 'operation_conflict', '別カードで同じ operation_id は 409');
    // 動画は入口で断る。大きすぎるファイルは HTML や 500 でなく JSON の 413 (Codex 動画 R1)
    {
      const mp4 = Buffer.concat([Buffer.from([0, 0, 0, 0x18]), Buffer.from('ftypisom', 'latin1'), Buffer.alloc(64, 2)]);
      const mv = multipart({ id: String(task.id), kind: 'video', worker_id: String(w1.id), operation_id: 'op-http-video1' },
        { name: 'v.mp4', mime: 'video/mp4', data: mp4 });
      const rv = await call('POST', '/api/media', { cookie, body: mv.body, headers: mv.headers });
      ok(rv.status === 400 && rv.json.error === 'video_disabled', '動画は 400 video_disabled');
      ok(db.prepare("SELECT COUNT(*) c FROM f_iroha_card_media WHERE operation_id = 'op-http-video1'").get().c === 0, '動画は 1 行も入らない');
      const big = Buffer.concat([Buffer.from([0xFF, 0xD8, 0xFF, 0xE0]), Buffer.alloc(9 * 1024 * 1024, 1)]);
      const mb = multipart({ id: String(task.id), kind: 'photo', worker_id: String(w1.id), operation_id: 'op-http-big001' },
        { name: 'big.jpg', mime: 'image/jpeg', data: big });
      const rb = await call('POST', '/api/media', { cookie, body: mb.body, headers: mb.headers });
      ok(rb.status === 413 && rb.json && rb.json.error === 'too_large', '大きすぎる写真は JSON の 413 (HTML や 500 にしない)');
      ok(db.prepare("SELECT COUNT(*) c FROM f_iroha_card_media WHERE operation_id = 'op-http-big001'").get().c === 0, '受け取らなかった分は記録も残らない');
      const bigV = multipart({ id: String(task.id), kind: 'video', worker_id: String(w1.id), operation_id: 'op-http-bigv01' },
        { name: 'big.mp4', mime: 'video/mp4', data: big });
      const rbv = await call('POST', '/api/media', { cookie, body: bigV.body, headers: bigV.headers });
      ok(rbv.status === 413 && rbv.json.error === 'too_large', '大きすぎる動画も JSON で返る (上限で先に弾かれる)');
      const { MEDIA_DIR: MD } = await import('../apps/iroha-work/media.js');
      const tmpDir = path.join(MD, 'tmp');
      ok(!fs.existsSync(tmpDir) || fs.readdirSync(tmpDir).length === 0, '一時ファイルを残さない');
    }
    await processMediaQueue();
    ok(db.prepare('SELECT status FROM f_iroha_card_media WHERE id = ?').get(up.json.media.id).status === 'uploaded', 'Drive 保存で完了 (uploaded)');
    ok(db.prepare('SELECT COUNT(*) c FROM f_iroha_media_page_sync WHERE page_id IS NULL').get().c === 0, '同期キューに NULL ページは無い');
    const st2 = await call('GET', '/api/state', { cookie });
    const card2 = st2.json.cards.find((c) => c.id === task.id);
    ok(card2 && card2.media?.length === 1 && card2.media[0].id === up.json.media.id, '一覧の詳細に写真が出る');
    const del = await call('POST', `/api/media/${up.json.media.id}/delete`, { cookie, body: { delete_token: up.json.deleteToken, worker_id: w1.id } });
    ok(del.status === 200 && del.json.ok, '削除 (削除トークン)');
    // 履歴 (終了したカード) は読むだけ: 写真を足せない・消せない (アプリ正本でも。PR1 の境界)
    {
      const closedId = TD.upsertTaskFromImport({ notion_page_id: 'hist-media', status: 'closed', close_reason: 'stocked',
        closed_at: '2026-09-04T00:00:00Z', destination_id: 8899, product_code: 'HIST-A', product_name: '終了したカード', qty: 1,
        facility_code: 'iroha' }, { batchId: 'test-hist' }).id;
      const mpc = multipart({ id: String(closedId), kind: 'photo', worker_id: String(w1.id), operation_id: 'op-hist-000001' },
        { name: 'h.jpg', mime: 'image/jpeg', data: jpeg });
      const addClosed = await call('POST', '/api/media', { cookie, body: mpc.body, headers: mpc.headers });
      ok(addClosed.status === 409 && addClosed.json.error === 'closed_task', '終了したカードには写真を足せない');
      ok(db.prepare("SELECT COUNT(*) c FROM f_iroha_card_media WHERE operation_id = 'op-hist-000001'").get().c === 0, '行も残らない');
      // 終了前に撮ってあった写真は、終了後は消せない (履歴として残る)
      db.prepare(`INSERT INTO f_iroha_card_media (operation_id, task_id, product_code, kind, mime, size, drive_file_id, drive_url,
          status, worker_id, worker_name, created_at, uploaded_at, delete_token_hash)
        VALUES ('op-hist-old001', ?, 'HIST-A', 'photo', 'image/jpeg', 100, 'f-hist-old', 'https://drive/hist-old',
          'uploaded', ?, 'やまだ', ?, ?, 'hash-hist')`).run(closedId, w1.id, new Date().toISOString(), new Date().toISOString());
      const oldId = db.prepare("SELECT id FROM f_iroha_card_media WHERE operation_id = 'op-hist-old001'").get().id;
      const delClosed = await call('POST', '/api/media/' + oldId + '/delete', { ...admin, body: {} });
      ok(delClosed.status === 409 && delClosed.json.error === 'closed_task', '終了したカードの写真は消せない');
      ok(db.prepare('SELECT deleted_at FROM f_iroha_card_media WHERE id = ?').get(oldId).deleted_at == null, '写真は残る');
      const pv = await call('GET', '/api/task-previews/' + closedId, { cookie });
      ok(pv.status === 200 && pv.json.ok && pv.json.card.status === 'closed' && pv.json.capabilities.length === 0,
        '終了したカードの詳細は読むだけで開ける');
      // 写真だけでなく、タスクに紐づく書き込み口はすべて断る (Codex PR1 R2 重大)
      const snapHist = () => JSON.stringify([
        db.prepare('SELECT * FROM f_iroha_tasks WHERE id = ?').get(closedId),
        db.prepare('SELECT COUNT(*) c FROM f_iroha_label_waits WHERE task_id = ?').get(closedId).c,
        db.prepare('SELECT COUNT(*) c FROM f_iroha_card_media WHERE task_id = ?').get(closedId).c,
        db.prepare('SELECT COUNT(*) c FROM f_iroha_work_options').get().c,
        db.prepare('SELECT COUNT(*) c FROM f_iroha_work_master').get().c,
        db.prepare('SELECT COUNT(*) c FROM f_iroha_work_sessions WHERE task_id = ?').get(closedId).c,
        db.prepare('SELECT COUNT(*) c FROM f_iroha_app_events WHERE task_id = ?').get(closedId).c,
      ]);
      const staffHist = listIrohaWorkers(true).find((x) => x.worker_type === 'staff');
      const SVC = await import('../apps/iroha-work/service.js');
      const beforeHist = snapHist();
      const closedWrites = [
        ['/api/planned', { id: closedId, worker_id: w1.id, planned_date: SVC.jstToday(), expect_version: TD.getTask(closedId).version }, 'admin'],
        ['/api/review-cleared', { id: closedId, worker_id: staffHist.id, pin: '4649', expect_version: TD.getTask(closedId).version }],
        ['/api/label-waits', { task_id: closedId, worker_id: w1.id, fields: { note: '履歴に足す' } }],
        ['/api/master', { id: closedId, code: 'HIST-A', worker_id: w1.id, fields: { note: 'x' }, expect_version: 1 }],
        ['/api/options', { id: closedId, kind: 'material', code: 'HIST-NEW', worker_id: staffHist.id, pin: '4649' }],
        ['/api/external-ready', { id: closedId, ready: true, worker_id: w1.id, expect_version: TD.getTask(closedId).version }],
      ];
      for (const [p2, body, as] of closedWrites) {
        // as='admin' = 職員だけの口 (計画) は管理画面から叩いて「終了カードは 409」を見る
        const r = await call('POST', p2, as === 'admin' ? { ...admin, body } : { cookie, body });
        if (!(r.status === 409 && (r.json.error === 'closed_task' || r.json.error === 'done_card'))) {
          ok(false, `${p2} に終了カードの id を送ると 409 (実際 ${r.status} ${r.json && r.json.error})`);
        }
      }
      ok(true, '今日やる・確認ずみ・ラベル待ち・作業のやり方・候補・外部準備OK に終了カードの id を送ると 409');
      // 終了カードでは作業を止められない (記録も足さない)
      const stopClosed = await call('POST', '/api/sessions/stop', { cookie, body: { id: closedId, worker_id: w1.id, session_id: 1, reason: 'done' } });
      ok(stopClosed.status === 409 && stopClosed.json.error === 'closed_task', '終了したカードでは作業を止められない');
      // カードの指定を省いても、作業のやり方・候補は登録できない (検査を素通りできない)
      for (const p3 of ['/api/master', '/api/options']) {
        for (const body of [{ kind: 'material', code: 'X1', worker_id: staffHist.id, pin: '4649' },
          { id: '', kind: 'material', code: 'X2', worker_id: staffHist.id, pin: '4649' },
          { id: 'zzz', kind: 'material', code: 'X3', worker_id: staffHist.id, pin: '4649' },
          { id: 999999, kind: 'material', code: 'X4', worker_id: staffHist.id, pin: '4649' }]) {
          const r = await call('POST', p3, { cookie, body });
          if (!(r.status === 409 && (r.json.error === 'card_required' || r.json.error === 'closed_task'))) {
            ok(false, `${p3} は カードの指定 ${JSON.stringify(body.id)} を断る (実際 ${r.status} ${r.json && r.json.error})`);
          }
        }
      }
      ok(true, '作業のやり方・候補の登録は、書けるカードを添えないと通らない (省略・空・でたらめ・無い id)');
      // 添えたカードと、書き換える商品が同じでなければ通さない
      // (適当な未終了カードを添えて、下見・履歴にしかない別商品のやり方を書き換えられないように — Codex PR1 R4)
      {
        const other = TD.upsertTaskFromImport({ notion_page_id: 'mismatch-open', status: 'not_started', destination_id: 8896,
          product_code: 'PROD-NEW', product_name: '開いているカード', qty: 1, facility_code: 'iroha' }, { batchId: 'test-mis' }).id;
        const beforeMaster = db.prepare("SELECT COUNT(*) c FROM f_iroha_work_master WHERE code_key = 'hist-a'").get().c;
        const bad = await call('POST', '/api/master', { cookie, body: { id: other, code: 'HIST-A',
          fields: { note: 'よそのカードから' }, worker_id: staffHist.id, pin: '4649', expect_version: 1 } });
        ok(bad.status === 409 && bad.json.error === 'card_mismatch', 'カードとちがう商品の作業のやり方は書き換えられない');
        ok(db.prepare("SELECT COUNT(*) c FROM f_iroha_work_master WHERE code_key = 'hist-a'").get().c === beforeMaster,
          'その商品のマスタは 1 行も増えていない');
        const mv = db.prepare("SELECT version FROM f_iroha_work_master WHERE code_key = 'prod-new'").get();
        const good = await call('POST', '/api/master', { cookie, body: { id: other, code: 'PROD-NEW',
          fields: { note: '自分のカードから' }, worker_id: staffHist.id, pin: '4649', expect_version: mv ? mv.version : 1 } });
        ok(good.status === 200 && good.json.ok, '自分のカードの商品なら通る (商品マスタにある商品)');
      }
      ok(snapHist() === beforeHist, 'ここまでで終了カードの記録は 1 行も変わっていない');
      ok(snapHist() === beforeHist, '終了カードの id をどの書き込み API に送っても DB が変わらない');
      // ラベル待ちの記録は「そのカードのもの」でなければ書き換えられない (別カードの id を添えても通らない)
      {
        const openTask = TD.upsertTaskFromImport({ notion_page_id: 'lw-owner', status: 'on_hold', hold_reason_code: 'label_shortage',
          destination_id: 8898, product_code: 'LW-A', product_name: 'ラベル待ちの持ち主', qty: 1, facility_code: 'iroha' }, { batchId: 'test-lw' }).id;
        const otherTask = TD.upsertTaskFromImport({ notion_page_id: 'lw-other', status: 'not_started',
          destination_id: 8897, product_code: 'LW-B', product_name: 'よその card', qty: 1, facility_code: 'iroha' }, { batchId: 'test-lw' }).id;
        const lw = TD.upsertLabelWait({ taskId: openTask, fields: { occurred_on: '2026-09-04', qty: 1 } });
        ok(lw.ok, '前提: ラベル待ちを 1 件登録');
        const stolen = await call('POST', '/api/label-waits', { cookie, body: { id: lw.row.id, task_id: otherTask, worker_id: w1.id,
          expect_version: lw.row.version, fields: { note: 'よそから書き換え' } } });
        ok(stolen.status === 404 && stolen.json.error === 'not_found', '別のカードの id を添えた更新は通らない');
        ok(db.prepare('SELECT note FROM f_iroha_label_waits WHERE id = ?').get(lw.row.id).note == null, '中身も変わらない');
        const mine = await call('POST', '/api/label-waits', { cookie, body: { id: lw.row.id, task_id: openTask, worker_id: w1.id,
          expect_version: lw.row.version, fields: { note: '正しい持ち主から' } } });
        ok(mine.status === 200 && mine.json.ok, '正しいカードからは更新できる');
      }
    }
    // まとめて棚入完了 (PR-C): 棚入待ちのものだけ・職員だけ
    {
      const mk = (dest, name, status) => TD.upsertTaskFromImport({ notion_page_id: `bulk-${dest}`, status, destination_id: dest,
        product_code: 'BULK-X', product_name: name, qty: 1, facility_code: 'iroha',
        closed_at: status === 'closed' ? '2026-09-03T00:00:00Z' : null, close_reason: status === 'closed' ? 'stocked' : null }, { batchId: 'test-bulk' }).id;
      const r1 = mk(9101, '棚入待ち1', 'ready_for_stocking');
      const r2 = mk(9102, '棚入待ち2', 'ready_for_stocking');
      const wip = mk(9103, '作業中', 'in_progress');
      const already = mk(9104, 'もう棚入完了', 'closed');
      const staff = listIrohaWorkers(true).find((x) => x.worker_type === 'staff');
      const bv = (id) => ({ id, version: TD.getTask(id) ? TD.getTask(id).version : 0 });   // 画面が添える「選んだときの版」
      ok((await call('POST', '/api/bulk-stocked', { cookie, body: { ids: [bv(r1)], worker_id: w1.id } })).status === 403, '利用者は まとめて棚入完了 にできない (職員のみ)');
      ok((await call('POST', '/api/bulk-stocked', { cookie, body: { ids: [bv(r1)], worker_id: staff.id, pin: '0000' } })).status === 403, '職員でも PIN が違えば拒否');
      ok((await call('POST', '/api/bulk-stocked', { ...admin, body: { ids: [], worker_id: w1.id } })).json.error === 'bad_request', '1 件も選んでいなければ bad_request');
      ok((await call('POST', '/api/bulk-stocked', { ...admin, body: { ids: [bv(r1), { id: '1.5', version: 1 }], worker_id: w1.id } })).status === 400, '不正な id が混ざっていれば 400 (何もしない)');
      ok((await call('POST', '/api/bulk-stocked', { ...admin, body: { ids: [r1], worker_id: w1.id } })).status === 400, '版を添えない古い画面からの要求は 400 (何もしない)');
      // 選んでから別の端末が変えていた分は飛ばす (楽観ロック — Codex PR1 R7)
      ok((await call('POST', '/api/bulk-stocked', { cookie, body: { ids: [{ id: r1, version: TD.getTask(r1).version + 5 }],
        worker_id: staff.id, pin: '4649' } })).json.skipped[0].reason === 'conflict', '選んだときと版が違えば飛ばす');
      ok(TD.getTask(r1).status === 'ready_for_stocking', '飛ばした分は状態も変わらない');
      ok(TD.getTask(r1).status === 'ready_for_stocking', '拒否されたカードは棚入待ちのまま');
      const bulk = await call('POST', '/api/bulk-stocked', { cookie, body: { ids: [bv(r1), bv(r2), bv(wip), bv(already), bv(999999)], worker_id: staff.id, pin: '4649' } });
      ok(bulk.status === 200 && bulk.json.ok && bulk.json.done.length === 2 && bulk.json.done.includes(r1) && bulk.json.done.includes(r2), '職員 + PIN で 棚入待ち 2 件だけが棚入完了');
      const why = Object.fromEntries((bulk.json.skipped || []).map((x) => [x.id, x.reason]));
      ok(why[wip] === 'not_ready' && why[already] === 'already' && why[999999] === 'not_found', '飛ばした理由を返す (棚入待ちでない・すでに完了・見つからない)');
      const done1 = TD.getTask(r1);
      ok(done1.status === 'closed' && done1.close_reason === 'stocked' && done1.closed_at && done1.ready_at && TD.getTask(wip).status === 'in_progress', '棚入完了 (終了理由・時刻が入り、作業中は動かない)');
      ok(db.prepare("SELECT COUNT(*) c FROM f_iroha_app_events WHERE action = 'task_status' AND task_id = ? AND to_value LIKE '%まとめて棚入完了%'").get(r1).c === 1, '履歴に残る');
      ok(!(await call('GET', '/api/state', { cookie })).json.cards.some((c) => c.id === r1), '棚入完了したカードは一覧から消える');
      // 履歴 (PR-C): 期間・検索・件数
      const hist = await call('GET', '/api/history?q=BULK-X', { cookie });
      ok(hist.status === 200 && hist.json.ok && hist.json.total >= 3 && hist.json.rows.some((x) => x.id === r1 && x.close_reason === 'stocked'), '履歴に終了したカードが出る (検索つき)');
      ok(hist.json.rows[0].closed_at >= hist.json.rows[hist.json.rows.length - 1].closed_at, '新しい順');
      const none = await call('GET', '/api/history?from=2999-01-01', { cookie });
      ok(none.status === 200 && none.json.total === 0 && none.json.fromDate === '2999-01-01', '未来を起点にすると 0 件');
      for (const bad of ['zzz', '2026-99-99', '2026-02-30', '2026-13-01', '20260903']) {
        const r = await call('GET', '/api/history?from=' + bad, { cookie });
        if (r.status !== 400) ok(false, `不正な日付 ${bad} は 400 (実際 ${r.status})`);
      }
      ok((await call('GET', '/api/history?from=2026-99-99', { cookie })).status === 400
        && (await call('GET', '/api/history?to=2026-02-30', { cookie })).status === 400, '実在しない日付は 400 (500 にしない)');
      ok((await call('GET', '/api/history?from=2026-02-29', { cookie })).status === 400
        && (await call('GET', '/api/history?from=2024-02-29', { cookie })).status === 200, '閏日は年で判定する (2026/2/29 は無い・2024/2/29 はある)');
      // JST の日付境界: 終了日はその日を含む
      db.prepare("UPDATE f_iroha_tasks SET closed_at = '2026-09-03T14:59:59.000Z' WHERE id = ?").run(r1);   // JST 9/3 23:59:59
      db.prepare("UPDATE f_iroha_tasks SET closed_at = '2026-09-03T15:00:00.000Z' WHERE id = ?").run(r2);   // JST 9/4 00:00:00
      const d3 = await call('GET', '/api/history?from=2026-09-03&to=2026-09-03&q=BULK-X', { cookie });
      ok(d3.json.rows.some((x) => x.id === r1) && !d3.json.rows.some((x) => x.id === r2), '終了日 9/3 を指定すると 9/3 23:59:59 JST まで入り、9/4 00:00 は入らない');
      const d4 = await call('GET', '/api/history?from=2026-09-04&to=2026-09-04&q=BULK-X', { cookie });
      ok(d4.json.rows.some((x) => x.id === r2) && !d4.json.rows.some((x) => x.id === r1), '9/4 を指定すると 9/4 の分だけ');
      const far = await call('GET', '/api/history?to=9999-12-31&q=BULK-X', { cookie });
      ok(far.status === 200 && far.json.rows.length > 0, '上限の年 (9999-12-31) でも 500 にならず全部出る (翌日が西暦10000 になる罠)');
      ok((await call('GET', '/api/history?from=0001-01-01&to=9999-12-31', { cookie })).status === 200, '端から端までの指定も通る');
      const one = await call('GET', '/api/history?q=' + encodeURIComponent('棚入待ち1'), { cookie });
      ok(one.json.total === 1 && one.json.rows[0].id === r1 && one.json.rows[0].facility_name === 'いろは', '商品名で絞れる (拠点名つき)');
      // 外部施設に出す準備OK (HTTP)
      const extTask = mk(9105, '外部に出す予定', 'in_progress');
      const e1 = await call('POST', '/api/external-ready', { cookie, body: { id: extTask, ready: true, expect_version: TD.getTask(extTask).version, worker_id: w1.id } });
      ok(e1.status === 200 && e1.json.ok && e1.json.task.external_ready === true, '外部に出す準備OK にできる (職員でなくてよい)');
      ok((await call('GET', '/api/state', { cookie })).json.cards.find((c) => c.id === extTask).external_ready === true, '一覧にも出る');
      const e2 = await call('POST', '/api/external-ready', { cookie, body: { id: extTask, ready: false, expect_version: 1, worker_id: w1.id } });
      ok(e2.status === 409 && e2.json.error === 'conflict' && e2.json.current, '古い version は 409 (現在値つき)');
      ok((await call('POST', '/api/external-ready', { cookie, body: { id: 'x', ready: true, worker_id: w1.id } })).status === 400, '不正な id は 400');
      ok((await call('POST', '/api/external-ready', { ...admin, body: { id: extTask, ready: false, expect_version: TD.getTask(extTask).version } })).status === 400,
        '作業者を選んでいなければ 400');
      // ready は true/false だけ (欠落・文字列・数値を「解除」と読まない — Codex FB R2)
      for (const bad of [undefined, null, 'true', 1, 0, {}]) {
        const body = { id: extTask, expect_version: TD.getTask(extTask).version, worker_id: w1.id };
        if (bad !== undefined) body.ready = bad;
        const r = await call('POST', '/api/external-ready', { cookie, body });
        if (!(r.status === 400 && r.json.error === 'bad_request')) ok(false, `ready=${JSON.stringify(bad)} は 400 (実際 ${r.status})`);
      }
      ok(TD.getTask(extTask).external_ready === 1, '不正な ready でチェックが外れていない');
      // ══ ⭐3 軸: いつ (明日やる) / どこが (拠点) — 計画は職員だけ (要件 §W-5) ══
      {
        const S2 = await import('../apps/iroha-work/service.js');
        const staffP = listIrohaWorkers(true).find((x) => x.worker_type === 'staff');
        const memberP = listIrohaWorkers().find((x) => x.worker_type !== 'staff');   // 有効な利用者だけ
        const today = S2.jstToday();
        const tomorrow = S2.jstTomorrow(today);
        const t1 = mk(9401, '計画テストA', 'not_started');
        const v = () => TD.getTask(t1).version;

        // ① 利用者には計画を許さない (capabilities にも API にも)
        const stM = await call('GET', '/api/state', { cookie });
        ok(!stM.json.capabilities.includes('task.plan.assign') && !stM.json.capabilities.includes('task.facility.assign'),
          '職員モードでなければ「いつ」「どこが」は許可リストに無い');
        ok(stM.json.staff_mode && stM.json.staff_mode.staff === false, '職員モードでないことが state に出る');
        const byMember = await call('POST', '/api/plan', { cookie, body: { id: t1, when: 'tomorrow', expect_version: v(), worker_id: memberP.id } });
        ok(byMember.status === 403 && byMember.json.error === 'staff_required', '利用者が「いつ」を変えようとすると 403');
        ok(TD.getTask(t1).planned_date == null, 'DB は変わらない');
        const facByMember = await call('POST', '/api/facility', { cookie, body: { id: t1, facility_code: 'rashinban', expect_version: v(), worker_id: memberP.id } });
        ok(facByMember.status === 403 && facByMember.json.error === 'staff_required', '利用者が「どこが」を変えようとすると 403');
        ok((await call('GET', '/api/plan', { cookie })).status === 403, '明日の計画は利用者には見せない');

        // ② 職員モード: PIN を 1 回 → 30 分
        ok((await call('POST', '/api/staff-unlock', { cookie, body: { worker_id: staffP.id, pin: '0000' } })).status === 403, 'PIN が違えば職員モードに入れない');
        ok((await call('POST', '/api/staff-unlock', { cookie, body: { worker_id: memberP.id, pin: '4649' } })).status === 403, '利用者は職員モードに入れない');
        const unlock = await call('POST', '/api/staff-unlock', { cookie, body: { worker_id: staffP.id, pin: '4649' } });
        ok(unlock.status === 200 && unlock.json.staff_mode.staff === true && unlock.json.staff_mode.until, '職員 PIN で職員モードに入る (期限つき)');
        const stS = await call('GET', '/api/state', { cookie });
        ok(stS.json.capabilities.includes('task.plan.assign') && stS.json.capabilities.includes('task.facility.assign'),
          '職員モード中は「いつ」「どこが」が許可リストに入る');

        // ③ PIN なしで続けてタップできる (計画は何件も続く)
        const p1 = await call('POST', '/api/plan', { cookie, body: { id: t1, when: 'tomorrow', expect_version: v(), worker_id: staffP.id } });
        ok(p1.status === 200 && p1.json.ok && p1.json.task.planned_date === tomorrow, 'PIN なしで「明日やる」にできる (実日付が入る)');
        ok(p1.json.task.when === 'tomorrow', '返ってくるカードの when が tomorrow');
        const p2 = await call('POST', '/api/plan', { cookie, body: { id: t1, when: 'today', expect_version: v(), worker_id: staffP.id } });
        ok(p2.status === 200 && p2.json.task.planned_date === today && p2.json.task.when === 'today', '「今日やる」にもできる');
        const p3 = await call('POST', '/api/plan', { cookie, body: { id: t1, when: null, expect_version: v(), worker_id: staffP.id } });
        ok(p3.status === 200 && p3.json.task.planned_date == null && p3.json.task.when == null, '「未定」に戻せる');
        ok((await call('POST', '/api/plan', { cookie, body: { id: t1, when: 'sometime', expect_version: v(), worker_id: staffP.id } })).status === 400, '知らない when は 400');
        ok((await call('POST', '/api/plan', { cookie, body: { id: t1, when: 'tomorrow', expect_version: 999, worker_id: staffP.id } })).status === 409, '版が古ければ 409');

        // ④ 「どこが」は拠点だけを変える (進捗も予定も変えない)
        await call('POST', '/api/plan', { cookie, body: { id: t1, when: 'tomorrow', expect_version: v(), worker_id: staffP.id } });
        const before = TD.getTask(t1);
        const f1 = await call('POST', '/api/facility', { cookie, body: { id: t1, facility_code: 'rashinban', expect_version: v(), worker_id: staffP.id } });
        ok(f1.status === 200 && f1.json.task.facility_code === 'rashinban', '拠点を変えられる');
        const afterF = TD.getTask(t1);
        ok(afterF.status === before.status && afterF.planned_date === before.planned_date, '拠点を変えても進捗と予定は変わらない');
        const f2 = await call('POST', '/api/facility', { cookie, body: { id: t1, facility_code: null, expect_version: v(), worker_id: staffP.id } });
        ok(f2.status === 200 && f2.json.task.facility_code == null, '「未定」に戻せる (NULL)');
        ok((await call('POST', '/api/facility', { cookie, body: { id: t1, facility_code: 'nowhere', expect_version: v(), worker_id: staffP.id } })).status === 400, '無い拠点は 400');
        const beforePlan = TD.getTask(t1).planned_date;
        await call('POST', '/api/facility', { cookie, body: { id: t1, facility_code: 'iroha', expect_version: v(), worker_id: staffP.id } });
        ok(TD.getTask(t1).planned_date === beforePlan, '拠点の変更で予定が消えない');

        // ⑤ 明日の計画のデータ
        const plan = await call('GET', '/api/plan', { cookie });
        ok(plan.status === 200 && plan.json.ok && plan.json.today_ymd === today && plan.json.tomorrow_ymd === tomorrow, '明日の計画が開ける (今日・明日の日付つき)');
        ok(plan.json.tomorrow.some((c) => c.id === t1), '明日やる分に入っている');
        ok(!plan.json.candidates.some((c) => c.id === t1), '候補には出ない (もう予定がある)');
        ok(plan.json.candidates.every((c) => c.when == null && c.status === 'not_started'), '候補は「予定なし かつ 未着手」だけ');
        ok(plan.json.by_facility.some((f) => f.code === 'iroha' && f.count >= 1), '拠点ごとの内訳が出る');
        ok(typeof plan.json.totals.hours === 'number' && typeof plan.json.totals.unknown_hours_count === 'number', '合計時間と「時間不明」の件数が出る');
        ok(plan.json.target_hours.min === 4 && plan.json.target_hours.max === 6, '目安は 4〜6 時間');
        // 並びが決定的
        const twice = await call('GET', '/api/plan', { cookie });
        ok(JSON.stringify(plan.json.candidates.map((c) => c.id)) === JSON.stringify(twice.json.candidates.map((c) => c.id)), '候補の並びは何度引いても同じ');

        // ⑥ ゲージ用の合計が一覧にも出る
        const stG = await call('GET', '/api/state', { cookie });
        ok(stG.json.tomorrow_plan && stG.json.tomorrow_plan.count >= 1 && stG.json.today_ymd === today, 'ボードのゲージ用に「明日やる分」の件数と合計が出る');
        ok(stG.json.cards.find((c) => c.id === t1).when === 'tomorrow', 'カードに when が付く');
        ok('size_label' in stG.json.cards[0] && 'size_rank' in stG.json.cards[0], 'カードに大きさ (配送方法) が付く');

        // ⑦ やり残し = 今日より前の予定で、まだ終わっていない。**自動では動かさない**
        const t2 = mk(9402, 'やり残しテスト', 'in_progress');
        db.prepare('UPDATE f_iroha_tasks SET planned_date = ? WHERE id = ?').run('2020-01-01', t2);
        const plan2 = await call('GET', '/api/plan', { cookie });
        ok(plan2.json.carry_over.some((c) => c.id === t2), 'やり残しに出る');
        await call('GET', '/api/state', { cookie });
        await call('GET', '/api/plan', { cookie });
        ok(TD.getTask(t2).planned_date === '2020-01-01', '一覧や計画を開いても予定日は自動で動かない');

        // ⑧ 職員モードが切れたら断る。ポータル (管理画面) は常に通る
        ok((await call('POST', '/api/staff-lock', { cookie })).json.staff_mode.staff === false, '職員モードを終えられる');
        const afterLock = await call('POST', '/api/plan', { cookie, body: { id: t1, when: null, expect_version: v(), worker_id: staffP.id } });
        ok(afterLock.status === 403 && ['staff_required', 'pin_required'].includes(afterLock.json.error), '職員モードが切れたら 403 (PIN を入れ直す)');
        const withPin = await call('POST', '/api/plan', { cookie, body: { id: t1, when: null, expect_version: v(), worker_id: staffP.id, pin: '4649' } });
        ok(withPin.status === 200 && withPin.json.staff_mode.staff === true, 'PIN を添えれば通り、そのまま職員モードに入る');
        ok((await call('GET', '/api/plan', { ...admin })).status === 200, 'ポータル (管理画面) からは職員モードなしで見られる');

        // ⑨ 古い入口 (/api/planned) も同じ関門を通る — 画面から隠しても直接叩けてしまうため (Codex P1 R1)
        await call('POST', '/api/staff-lock', { cookie });
        const oldApi = await call('POST', '/api/planned', { cookie, body: { id: t1, planned_date: tomorrow, expect_version: v(), worker_id: memberP.id } });
        ok(oldApi.status === 403, '古い /api/planned も利用者には通さない (画面から隠しても直接叩ける)');
        ok(TD.getTask(t1).planned_date == null, '断ったので予定も入っていない');
        // ⑩ 職員モード中でも、記録に残る「やった人」が職員でなければ通さない
        await call('POST', '/api/staff-unlock', { cookie, body: { worker_id: staffP.id, pin: '4649' } });
        const asMember = await call('POST', '/api/plan', { cookie, body: { id: t1, when: 'tomorrow', expect_version: v(), worker_id: memberP.id } });
        ok(asMember.status === 403 && asMember.json.error === 'staff_required',
          '職員が開けた端末でも、利用者の名前では計画を変えられない (「利用者がやった」記録を作らない)');

        // ⑪ 拠点が未定 (NULL) のまま読める — 一覧・明日の計画・詳細
        const t3 = mk(9403, '拠点未定のまま', 'not_started');
        db.prepare('UPDATE f_iroha_tasks SET facility_code = NULL WHERE id = ?').run(t3);
        const stN = await call('GET', '/api/state', { cookie });
        const cN = stN.json.cards.find((c) => c.id === t3);
        ok(stN.status === 200 && cN && cN.facility_code == null, '拠点が未定でも一覧に出る');
        ok((await call('GET', '/api/plan', { cookie })).status === 200, '拠点が未定のカードがあっても明日の計画が開ける');
        ok((await call('GET', '/api/task-previews/' + t3, { cookie })).status === 200, '拠点が未定でも詳細が開ける');
        ok((await call('GET', '/api/history', { cookie })).status === 200, '履歴も開ける');

        // ⑫ 不正な要求では職員モードが開かない (中身を先に見る — Codex P1 R2)
        await call('POST', '/api/staff-lock', { cookie });
        const badWhen = await call('POST', '/api/plan', { cookie, body: { id: t1, when: 'someday', expect_version: v(), worker_id: staffP.id, pin: '4649' } });
        ok(badWhen.status === 400, '知らない when は 400');
        ok((await call('GET', '/api/state', { cookie })).json.staff_mode.staff === false, '正しい PIN でも、中身が不正なら職員モードは開かない');
        const badId = await call('POST', '/api/facility', { cookie, body: { id: 'x', facility_code: 'iroha', worker_id: staffP.id, pin: '4649' } });
        ok(badId.status === 400 && (await call('GET', '/api/state', { cookie })).json.staff_mode.staff === false, '拠点の口も同じ');

        // ⑬ 古い入口も「今日 / 明日 / 未定」だけ (先の日付・存在しない日付を直に入れられない)
        const far = await call('POST', '/api/planned', { ...admin, body: { id: t1, planned_date: '2099-12-31', expect_version: v(), worker_id: staffP.id } });
        ok(far.status === 400 && far.json.error === 'bad_request', '古い入口に先の日付を送ると 400');
        const nodate = await call('POST', '/api/planned', { ...admin, body: { id: t1, planned_date: '2026-02-31', expect_version: v(), worker_id: staffP.id } });
        ok(nodate.status === 400, '存在しない日付も 400');
        ok(TD.getTask(t1).planned_date == null, 'どちらも DB は変わらない');
        const okOld = await call('POST', '/api/planned', { ...admin, body: { id: t1, planned_date: tomorrow, expect_version: v(), worker_id: staffP.id } });
        ok(okOld.status === 200 && TD.getTask(t1).planned_date === tomorrow, '明日なら古い入口でも通る');

        // ⑭ 書く直前に「いまも有効な職員か」を見る (別の接続で無効にされ得る)
        await call('POST', '/api/staff-unlock', { cookie, body: { worker_id: staffP.id, pin: '4649' } });
        setIrohaWorkerActive(staffP.id, false, 'test');
        const gone = await call('POST', '/api/plan', { cookie, body: { id: t1, when: null, expect_version: v(), worker_id: staffP.id } });
        ok(gone.status !== 200, '無効にされた職員では通らない');
        ok(TD.getTask(t1).planned_date === tomorrow, 'DB も変わらない');
        setIrohaWorkerActive(staffP.id, true, 'test');
      }

      // 名前のないカードの片づけ (管理者のみ・confirm 必須)
      const stray = Number(db.prepare(`INSERT INTO f_iroha_tasks (status, facility_code, product_name, version, created_at, created_by, updated_at, updated_by)
        VALUES ('not_started', 'iroha', NULL, 1, ?, 'test', ?, 'test')`).run(new Date().toISOString(), new Date().toISOString()).lastInsertRowid);
      ok((await call('POST', '/admin/tasks/remove', { cookie, body: { task_id: stray, confirm: 'REMOVE' } })).status === 403, '片づけは管理者だけ');
      ok((await call('POST', '/admin/tasks/remove', { ...admin, body: { task_id: stray } })).json.error === 'confirm_required', 'confirm=REMOVE が要る');
      const rm = await call('POST', '/admin/tasks/remove', { ...admin, body: { task_id: stray, confirm: 'REMOVE' } });
      ok(rm.status === 200 && rm.json.action === 'deleted' && TD.getTask(stray) === null && typeof rm.json.remaining === 'number', '消せる (残り件数も返す)');
      ok((await call('GET', '/admin/migration/status', { ...admin })).json.nameless !== undefined, '管理画面の状態に名前のないカードが載る');
      ok((await call('POST', '/admin/tasks/remove', { ...admin, body: { task_id: task.id, confirm: 'REMOVE' } })).status === 409,
        'ふつうのカード (名前あり・Notion 紐づきあり) は 409 で消せない');
      ok(TD.getTask(task.id) !== null, '消えていない');
    }
    ok(notionCalls() === calls0, 'ここまで Notion API を 1 回も呼んでいない');
    // Notion に戻す: app 正本以降の記録があるので 409 → force で通る (件数・監査ログ・切替時刻)
    const back = await call('POST', '/admin/source', { ...admin, body: { to: 'notion', confirm: 'SWITCH' } });
    ok(back.status === 409 && back.json.error === 'app_changes_exist' && back.json.changes.tasks >= 2 && back.json.changes.updatedTasks >= 1
      && back.json.changes.sessions >= 1 && back.json.changes.media >= 1, '記録があるうちは 409 app_changes_exist (状態変更 2 回以上・作業時間・写真)');
    ok(getMeta('source_of_truth') === 'app', '409 のときは切り替わらない');
    const forced = await call('POST', '/admin/source', { ...admin, body: { to: 'notion', confirm: 'SWITCH', force: true } });
    ok(forced.status === 200 && forced.json.ok && forced.json.source === 'notion' && forced.json.changes.tasks === back.json.changes.tasks, 'force で戻せる (件数つき)');
    ok(getMeta('source_of_truth') === 'notion' && getMeta('source_switched_at') > switchedAt, '正本と切替時刻が更新される');
    const audit = db.prepare("SELECT * FROM f_iroha_app_events WHERE action = 'source_switch' ORDER BY id DESC LIMIT 1").get();
    ok(audit && audit.from_value === 'app' && /^notion \(未完了 \d+ 件・Notion 未反映: 状態変更 \d+ 回\/更新タスク \d+\/作業時間 \d+\/写真 \d+ \(force\)\)$/.test(audit.to_value)
      && audit.device_label === 'session:admin@test.local', '監査ログに件数と force と誰がが残る');
    ok((await call('GET', '/api/state', { cookie })).json.mode === 'notion', '戻した後の一覧は Notion 正本');

    // ⭐切替前の下見 (中原さん 2026-09-03「正本にしないでも、どういう形か見たい」):
    //   読むだけの 3 本は Notion 正本のままでも開ける。書き変えは今までどおり断る
    {
      const pv = await call('GET', '/api/preview-tasks', { cookie });
      ok(pv.status === 200 && pv.json.ok && pv.json.preview === true && pv.json.mode === 'app', '下見のタスク一覧は Notion 正本でも開ける (preview=true)');
      ok(Array.isArray(pv.json.cards) && Array.isArray(pv.json.statuses) && Array.isArray(pv.json.facilities), 'ボードに必要なもの (カード・状態・拠点) が入っている');
      ok(pv.json.cards.every((c) => typeof c.id === 'number' && c.status && c.status_label), 'カードはアプリ側の形 (id は数値・状態は値と表示名)');
      const lw = await call('GET', '/api/label-waits', { cookie });
      ok(lw.status === 200 && lw.json.ok && lw.json.preview === true && Array.isArray(lw.json.rows), 'ラベル待ちの一覧も開ける (preview=true)');
      const hi = await call('GET', '/api/history?q=BULK-X', { cookie });
      ok(hi.status === 200 && hi.json.ok && hi.json.preview === true && hi.json.rows.length > 0, '履歴も開ける (preview=true)');
      // 書き変えは断る (切替前に触ると Notion と食い違い、切替時の取込でも上書きされない)
      const staff2 = listIrohaWorkers(true).find((x) => x.worker_type === 'staff');
      const writes = [
        ['POST', '/api/bulk-stocked', { ids: [{ id: 1, version: 1 }], worker_id: staff2.id, pin: '4649' }],
        ['POST', '/api/label-waits', { task_id: 1, worker_id: w1.id, fields: { note: 'x' } }],
        ['POST', '/api/planned', { id: 1, worker_id: w1.id }],
        ['POST', '/api/cancellation', { id: 1, worker_id: staff2.id, decision: 'cancel', pin: '4649' }],
        ['POST', '/api/review-cleared', { id: 1, worker_id: staff2.id, pin: '4649' }],
        ['POST', '/api/external-ready', { id: 1, ready: true, worker_id: w1.id }],
      ];
      for (const [m, p2, body] of writes) {
        const r = await call(m, p2, { cookie, body });
        if (!(r.status === 409 && r.json.error === 'notion_mode')) ok(false, `${p2} は Notion 正本では 409 notion_mode (実際 ${r.status} ${r.json && r.json.error})`);
      }
      ok(true, '下見では書き変えできない (まとめて棚入完了・ラベル待ち登録・今日やる・取消の判断・確認ずみ)');
      // 状態は動いていない
      ok((await call('GET', '/api/state', { cookie })).json.mode === 'notion', '下見を見ても正本は Notion のまま');

      // ⭐下見の詳細 (要件 v1.3 §P Q5 / PR1): ボード・履歴から 1 枚だけ読むだけで開ける。何も許さない (capabilities 空)。
      //   下見のカード id をどの書き込み API に送っても DB は変わらない
      {
        const stN = await call('GET', '/api/state', { cookie });
        ok(Array.isArray(stN.json.capabilities) && ['task.status.change', 'task.work.start', 'task.media.add', 'task.master.edit'].every((c) => stN.json.capabilities.includes(c))
          && !stN.json.capabilities.includes('task.plan.assign'), 'Notion 正本の一覧は 状態変更・作業開始・写真・作業のやり方 を許し、今日やる等は許さない');
        ok(Array.isArray(pv.json.capabilities) && pv.json.capabilities.length === 0, '下見のボードは何も許さない (capabilities が空)');
        const open = db.prepare("SELECT id, product_code FROM f_iroha_tasks WHERE status != 'closed' ORDER BY id LIMIT 1").get();
        const closed = db.prepare("SELECT id FROM f_iroha_tasks WHERE status = 'closed' ORDER BY id LIMIT 1").get();
        const d = await call('GET', '/api/task-previews/' + open.id, { cookie });
        ok(d.status === 200 && d.json.ok && d.json.preview === true && d.json.card && d.json.card.id === open.id, '下見の詳細が 1 枚だけ返る (id は数値)');
        ok(Array.isArray(d.json.capabilities) && d.json.capabilities.length === 0, '下見の詳細は何も許さない (capabilities が空)');
        ok(d.json.card.master && Array.isArray(d.json.card.media) && Array.isArray(d.json.card.previous_photos) && Array.isArray(d.json.card.active)
          && d.json.card.live && 'plan_hours' in d.json.card && 'boxes' in d.json.card && 'loc_at' in d.json.card,
          '詳細に必要なもの (作業のやり方・写真・前回の完成形・作業中の人・販売/在庫・想定時間・必要保管箱・在庫の時刻) が入っている');
        ok(typeof d.json.notionSyncedAt === 'string' && typeof d.json.serverNow === 'string', 'Notion を取り込んだ時刻 (取込で記録) と、サーバーの今の時刻が付く');
        if (closed) {
          const dc = await call('GET', '/api/task-previews/' + closed.id, { cookie });
          ok(dc.status === 200 && dc.json.ok && dc.json.card.status === 'closed', '履歴 (終了) のカードも読める');
        } else ok(false, '終了したタスクがテストデータに無い');
        ok((await call('GET', '/api/task-previews/999999', { cookie })).status === 404, '無いカードは 404');
        ok((await call('GET', '/api/task-previews/abc', { cookie })).status === 404, '数値でない id は 404');
        // 下見のカード id を書き込み API に送っても、DB は 1 行も変わらない
        const mediaTables = db.prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'f_iroha%media%' AND name NOT LIKE '%sync%'").all().map((r) => r.name);
        const snap = () => JSON.stringify({
          tasks: db.prepare('SELECT id, status, version, updated_at, updated_by FROM f_iroha_tasks ORDER BY id').all(),
          sessions: db.prepare('SELECT COUNT(*) c FROM f_iroha_work_sessions').get().c,
          media: mediaTables.map((t) => db.prepare(`SELECT COUNT(*) c FROM ${t}`).get().c),
          master: db.prepare('SELECT code_key, note, version FROM f_iroha_work_master ORDER BY code_key').all(),
        });
        const before = snap();
        const writes2 = [
          ['POST', '/api/status', { id: open.id, to: '作業中', worker_id: w1.id }],
          ['POST', '/api/master', { id: open.id, code: open.product_code || 'PROD-A', fields: { note: 'preview-write' }, worker_id: w1.id }],
          ['POST', '/api/sessions/start', { id: open.id, worker_id: w1.id }],
          ['POST', '/api/sessions/stop', { id: open.id, worker_id: w1.id, reason: 'pause' }],
        ];
        for (const [m, p2, body] of writes2) {
          const r = await call(m, p2, { cookie, body });
          if (!(r.status === 409 && r.json.error === 'notion_mode')) ok(false, `${p2} に下見の id を送ると 409 notion_mode (実際 ${r.status} ${r.json && r.json.error})`);
        }
        ok(true, '状態変更・作業のやり方・作業の開始/中断 に下見の id を送ると 409 notion_mode');
        const jpegPv = Buffer.concat([Buffer.from([0xFF, 0xD8, 0xFF, 0xE0]), Buffer.alloc(64, 1)]);
        const mpPv = multipart({ id: String(open.id), kind: 'photo', worker_id: String(w1.id), operation_id: 'op-preview-00001' }, { name: 'a.jpg', mime: 'image/jpeg', data: jpegPv });
        const rm = await call('POST', '/api/media', { cookie, body: mpPv.body, headers: mpPv.headers });
        ok(rm.status === 409 && rm.json.error === 'notion_mode', '写真の送信に下見の id を送ると 409 notion_mode');
        ok(snap() === before, '下見の id をどの書き込み API に送っても DB が変わらない (tasks・作業時間・写真・作業のやり方)');
        ok(mediaTables.length > 0, '写真のテーブルを数えている (テーブル名の前提が崩れていない)');
        // Codex R1: 写真の削除・選択肢の登録も下見/履歴の境界を越えない。認可も確かめる
        ok((await call('GET', '/api/task-previews/' + open.id)).status === 401, '端末登録もログインも無ければ下見の詳細は 401');
        // 撮った端末は削除トークンを持ったまま。Notion 正本に戻った後でも tasks の写真を消せないこと
        db.prepare(`INSERT INTO f_iroha_card_media (operation_id, task_id, product_code, kind, mime, size, drive_file_id, drive_url,
            status, worker_id, worker_name, created_at, uploaded_at, delete_token_hash)
          VALUES ('op-preview-del1', ?, ?, 'photo', 'image/jpeg', 100, 'f-preview-del1', 'https://drive/preview-del1',
            'uploaded', ?, 'やまだ', ?, ?, 'hash-preview')`).run(open.id, open.product_code || 'PROD-A', w1.id, new Date().toISOString(), new Date().toISOString());
        const mediaRow = db.prepare("SELECT id FROM f_iroha_card_media WHERE operation_id = 'op-preview-del1'").get();
        const del = await call('POST', '/api/media/' + mediaRow.id + '/delete', { ...admin, body: {} });
        ok(del.status === 409 && del.json.error === 'notion_mode', 'Notion 正本の間は tasks の写真を消せない (削除トークンやポータルでも)');
        // 断った試行そのものも、そのカードの履歴に足さない (履歴もカードの中身 — Codex PR1 R17)
        {
          const evBefore = db.prepare('SELECT COUNT(*) c FROM f_iroha_app_events WHERE task_id IS NOT NULL').get().c;
          await call('POST', '/api/sessions/stop', { cookie, body: { id: open.id, worker_id: w1.id, reason: 'pause', session_id: 1 } });
          ok(db.prepare('SELECT COUNT(*) c FROM f_iroha_app_events WHERE task_id IS NOT NULL').get().c === evBefore,
            '下見のカードには、断った操作の記録も残さない');
        }
        ok(db.prepare('SELECT deleted_at FROM f_iroha_card_media WHERE id = ?').get(mediaRow.id).deleted_at == null, '写真は消えていない');
        const optBefore = db.prepare('SELECT COUNT(*) c FROM f_iroha_work_options').get().c;
        const optRes = await call('POST', '/api/options', { cookie, body: { id: open.id, kind: 'material', code: 'PREVIEW-X', worker_id: staff2.id, pin: '4649' } });
        ok(optRes.status === 409 && optRes.json.error === 'notion_mode', '下見の id を添えた選択肢の登録は 409');
        ok(db.prepare('SELECT COUNT(*) c FROM f_iroha_work_options').get().c === optBefore, '選択肢も増えていない');
        // ⭐読むだけの詳細は、写真の「実体が無い」印すら書かない (開くだけで DB が変わらない — Codex PR1 R8)
        {
          db.prepare(`INSERT INTO f_iroha_card_media (operation_id, task_id, product_code, kind, mime, size, local_path,
              status, worker_id, worker_name, created_at)
            VALUES ('op-preview-lost1', ?, 'PROD-A', 'photo', 'image/jpeg', 100, ?, 'stored', ?, 'やまだ', ?)`)
            .run(open.id, path.join(process.env.DATA_DIR, 'no-such-file.jpg'), w1.id, new Date().toISOString());
          const lost = db.prepare("SELECT id FROM f_iroha_card_media WHERE operation_id = 'op-preview-lost1'").get();
          const d2 = await call('GET', '/api/task-previews/' + open.id, { cookie });
          ok(d2.status === 200, '実体の無い写真があっても下見の詳細は開ける');
          ok(db.prepare('SELECT next_retry_at, error FROM f_iroha_card_media WHERE id = ?').get(lost.id).next_retry_at == null,
            '読むだけなので「失敗」の印は付けない');
          db.prepare('DELETE FROM f_iroha_card_media WHERE id = ?').run(lost.id);
        }
        // 商品コードの無い Notion カードを添えても、よその商品の作業のやり方は書き換えられない (Codex PR1 R5)
        {
          db.prepare(`INSERT OR REPLACE INTO f_iroha_app_notion_cache (page_id, status, title, product_code, fetched_at)
            VALUES ('cache-nocode', '未着手', '商品コードなし', NULL, ?)`).run(new Date().toISOString());
          const beforeM = db.prepare("SELECT COUNT(*) c FROM f_iroha_work_master WHERE code_key = 'hist-a'").get().c;
          const r = await call('POST', '/api/master', { cookie, body: { id: 'cache-nocode', code: 'HIST-A',
            fields: { note: '商品コードなしのカードから' }, worker_id: staff2.id, pin: '4649', expect_version: 1 } });
          ok(r.status === 409 && r.json.error === 'card_mismatch', '商品コードの無いカードを添えた作業のやり方の書き換えは 409');
          ok(db.prepare("SELECT COUNT(*) c FROM f_iroha_work_master WHERE code_key = 'hist-a'").get().c === beforeM,
            'その商品のマスタは変わっていない');
        }
        // 詳細には「このカードの終わった作業」が付く (一覧には付けない — 2222 枚ぶん引かない)
        const withWork = db.prepare(`SELECT task_id FROM f_iroha_work_sessions
          WHERE task_id IS NOT NULL AND ended_at IS NOT NULL AND voided_at IS NULL GROUP BY task_id ORDER BY task_id LIMIT 1`).get();
        if (withWork) {
          const dw = await call('GET', '/api/task-previews/' + withWork.task_id, { cookie });
          ok(dw.status === 200 && Array.isArray(dw.json.card.work_history) && dw.json.card.work_history.length > 0
            && dw.json.card.work_history.every((s) => s.worker_name && s.started_at && s.ended_at), 'そのカードの終わった作業が時系列で返る');
          ok(pv.json.cards.every((c) => c.work_history === undefined), '一覧・ボードのカードには付けない (件数ぶん引かない)');
        } else ok(false, '終わった作業がテストデータに無い');
      }
    }
    const ms = await call('GET', '/admin/migration/status', { ...admin });
    ok(ms.status === 200 && ms.json.ok && Array.isArray(ms.json.linkConflicts) && typeof ms.json.linkConflictsTotal === 'number', '移行の状態 (管理画面の元データ) に紐付け衝突の一覧と総件数が載る');

    // 紐付け衝突の統合 (Codex PR-B R3 Medium): 確定側 (行き先あり・ページなし) と 取込側 (ページあり) が同じカード → 1 枚に
    {
      const { createTaskForDestination, listLinkConflicts, countLinkConflicts } = await import('../apps/iroha-work/task-intake.js');
      const { startSession, stopSession } = await import('../apps/iroha-work/db.js');
      const { addMedia, _setDriveUpload } = await import('../apps/iroha-work/media.js');
      db.exec(`CREATE TABLE IF NOT EXISTS f_inbound_check_destinations (id INTEGER PRIMARY KEY, batch_id INTEGER, line_key TEXT, ar_no TEXT, product_id TEXT, product_name TEXT,
        planned_qty INTEGER, destination TEXT, decided_from TEXT, worker TEXT, decided_at TEXT, cancelled_at TEXT, expiry_date TEXT, work_date TEXT, code_key TEXT, actual_qty INTEGER, notion_page_id TEXT)`);
      db.prepare(`INSERT OR REPLACE INTO f_inbound_check_destinations (id, batch_id, line_key, ar_no, product_id, product_name, planned_qty, destination, decided_from, worker, decided_at, work_date, code_key, actual_qty, notion_page_id)
        VALUES (9950, 1, 'M|1|1', 'AR-M', 'MERGE-X', '統合テスト', 5, 'iroha', 'chosen', 'test', '2026-09-03T00:00:00Z', '2026-09-03', 'merge-x', 5, 'merge-page-1')`).run();
      const inbound = createTaskForDestination(db.prepare('SELECT * FROM f_inbound_check_destinations WHERE id = 9950').get(), { actor: 'test' }).id;
      const imported = TD.upsertTaskFromImport({ notion_page_id: 'merge-page-1', status: 'in_progress', destination_id: null, product_code: 'MERGE-X', product_name: '統合テスト', qty: 5, started_at: '2026-09-03T01:00:00Z' }, { batchId: 'test-merge' }).id;
      ok(countLinkConflicts() >= 1 && listLinkConflicts().some((c) => c.task_id === inbound && c.other_task_id === imported), '前提: 衝突がある');
      // 確定側に記録を付けておく (統合で残す側へ移ることを見る)
      const s = startSession({ taskId: inbound, worker: getIrohaWorker(w1.id) });
      stopSession({ taskId: inbound, workerId: w1.id, sessionId: s.sessionId, reason: 'done' });
      _setDriveUpload(async ({ operationId }) => ({ fileId: 'f-' + operationId, url: 'https://drive/' + operationId }));
      const jpeg2 = Buffer.concat([Buffer.from([0xFF, 0xD8, 0xFF, 0xE0]), Buffer.alloc(64, 1)]);
      const mp3 = path.join(process.env.DATA_DIR, 'merge.jpg'); fs.writeFileSync(mp3, jpeg2);
      const m = addMedia({ taskId: inbound, productCode: 'MERGE-X', kind: 'photo', filePath: mp3, worker: getIrohaWorker(w1.id), operationId: 'op-merge-0001' });
      _setDriveUpload(null);
      ok(m.ok, '前提: 確定側に作業時間と写真');
      ok((await call('POST', '/admin/migration/link-conflicts/merge', { cookie, body: { task_id: inbound, keep: 'import' } })).status === 403, '統合は管理者だけ');
      ok((await call('POST', '/admin/migration/link-conflicts/merge', { ...admin, body: { task_id: inbound, keep: 'both' } })).status === 400, 'keep は import / inbound');
      const mg = await call('POST', '/admin/migration/link-conflicts/merge', { ...admin, body: { task_id: inbound, keep: 'import' } });
      ok(mg.status === 200 && mg.json.ok && mg.json.kept === imported && mg.json.closed === inbound, '統合 (取込側を残す)');
      const kept = TD.getTask(imported), gone = TD.getTask(inbound);
      ok(kept.destination_id === 9950 && kept.notion_page_id === 'merge-page-1' && kept.status === 'in_progress', '残す側に行き先とページが集まる (状態はそのまま)');
      ok(gone.destination_id == null && gone.notion_page_id == null && gone.status === 'closed' && gone.close_reason === 'cancelled' && /統合 → task#/.test(gone.migration_note), '消える側は行き先・ページを失って終了 (取消)。どこへ統合したか残る');
      ok(db.prepare('SELECT COUNT(*) c FROM f_iroha_work_sessions WHERE task_id = ?').get(imported).c === 1 && db.prepare('SELECT COUNT(*) c FROM f_iroha_card_media WHERE task_id = ?').get(imported).c === 1
        && db.prepare('SELECT COUNT(*) c FROM f_iroha_work_sessions WHERE task_id = ?').get(inbound).c === 0, '作業時間・写真は残す側へ付け替わる');
      ok(db.prepare("SELECT COUNT(*) c FROM f_iroha_app_events WHERE action = 'task_merge' AND task_id = ?").get(imported).c === 1, '履歴に統合が残る');
      ok(!listLinkConflicts().some((c) => c.task_id === inbound) && mg.json.remaining === countLinkConflicts(), '衝突は解消 (一覧から消える・残数を返す)');
      ok((await call('POST', '/admin/migration/link-conflicts/merge', { ...admin, body: { task_id: inbound, keep: 'import' } })).status === 404, '解消済みをもう一度統合しようとすると 404');
      // 確定側を残す向きも通る
      db.prepare(`INSERT OR REPLACE INTO f_inbound_check_destinations (id, batch_id, line_key, ar_no, product_id, product_name, planned_qty, destination, decided_from, worker, decided_at, work_date, code_key, actual_qty, notion_page_id)
        VALUES (9951, 1, 'M|2|1', 'AR-M', 'MERGE-Y', '統合テスト2', 2, 'iroha', 'chosen', 'test', '2026-09-03T00:00:00Z', '2026-09-03', 'merge-y', 2, 'merge-page-2')`).run();
      const inbound2 = createTaskForDestination(db.prepare('SELECT * FROM f_inbound_check_destinations WHERE id = 9951').get()).id;
      const imported2 = TD.upsertTaskFromImport({ notion_page_id: 'merge-page-2', status: 'not_started', destination_id: null, product_code: 'MERGE-Y', qty: 2 }, { batchId: 'test-merge' }).id;
      const mg2 = await call('POST', '/admin/migration/link-conflicts/merge', { ...admin, body: { task_id: inbound2, keep: 'inbound' } });
      ok(mg2.status === 200 && mg2.json.kept === inbound2 && TD.getTask(inbound2).notion_page_id === 'merge-page-2' && TD.getTask(imported2).status === 'closed' && TD.getTask(imported2).notion_page_id == null, '確定側を残す統合 (取込側が終了)');

      // Codex PR-B R4: 残す側が終了なら拒否 / 両方終了は通す / 消える側に作業中の人がいれば拒否 / 着手済みを未着手へ統合すると作業中に昇格 / 取消済み行き先は取消を伝える
      const seedDest = (id, code, page) => db.prepare(`INSERT OR REPLACE INTO f_inbound_check_destinations (id, batch_id, line_key, ar_no, product_id, product_name, planned_qty, destination, decided_from, worker, decided_at, work_date, code_key, actual_qty, notion_page_id)
        VALUES (?, 1, ?, 'AR-M', ?, ?, 1, 'iroha', 'chosen', 'test', '2026-09-03T00:00:00Z', '2026-09-03', ?, 1, ?)`).run(id, 'M|' + id, code, code, code.toLowerCase(), page);
      const mkPair = (id, code, { importStatus = 'not_started', inboundStatus = null } = {}) => {
        seedDest(id, code, 'merge-page-' + id);
        const inb = createTaskForDestination(db.prepare('SELECT * FROM f_inbound_check_destinations WHERE id = ?').get(id)).id;
        const imp = TD.upsertTaskFromImport({ notion_page_id: 'merge-page-' + id, status: importStatus, close_reason: importStatus === 'closed' ? 'stocked' : null, closed_at: importStatus === 'closed' ? '2026-09-03T00:00:00Z' : null,
          destination_id: null, product_code: code, qty: 1, started_at: importStatus === 'not_started' ? null : '2026-09-03T01:00:00Z' }, { batchId: 'test-merge' }).id;
        if (inboundStatus) db.prepare("UPDATE f_iroha_tasks SET status = ?, started_at = '2026-09-03T02:00:00Z' WHERE id = ?").run(inboundStatus, inb);
        return { inb, imp };
      };
      const merge = (taskId, keep) => call('POST', '/admin/migration/link-conflicts/merge', { ...admin, body: { task_id: taskId, keep } });
      const w2 = listIrohaWorkers(true).find((w) => w.display_name === 'すずき');
      // (1) 取込側が終了 (棚入完了) で keep=import → 409、keep=inbound は通る
      const p1 = mkPair(9960, 'MERGE-C1', { importStatus: 'closed' });
      const r1 = await merge(p1.inb, 'import');
      ok(r1.status === 409 && r1.json.error === 'keep_closed' && TD.getTask(p1.inb).destination_id === 9960, '残す側が終了なら 409 keep_closed (何も変わらない)');
      const r1b = await merge(p1.inb, 'inbound');
      ok(r1b.status === 200 && TD.getTask(p1.inb).notion_page_id === 'merge-page-9960' && TD.getTask(p1.imp).close_reason === 'stocked' && /統合 → task#/.test(TD.getTask(p1.imp).migration_note), '開いている確定側を残せば通る。既に終了していた側は終了理由そのまま (stocked)');
      // (2) 両方終了 → 通す
      const p2 = mkPair(9961, 'MERGE-C2', { importStatus: 'closed' });
      db.prepare("UPDATE f_iroha_tasks SET status = 'closed', close_reason = 'cancelled', closed_at = '2026-09-03T03:00:00Z' WHERE id = ?").run(p2.inb);
      const r2 = await merge(p2.inb, 'import');
      ok(r2.status === 200 && TD.getTask(p2.imp).destination_id === 9961 && TD.getTask(p2.imp).status === 'closed', '両方終了なら統合できる (履歴の整理)');
      // (3) 消える側で作業中 → 409 from_active (両方向)
      const p3 = mkPair(9962, 'MERGE-C3', { importStatus: 'in_progress' });
      const sA = startSession({ taskId: p3.inb, worker: getIrohaWorker(w1.id) });
      const r3 = await merge(p3.inb, 'import');
      ok(r3.status === 409 && r3.json.error === 'from_active' && db.prepare('SELECT task_id FROM f_iroha_work_sessions WHERE id = ?').get(sA.sessionId).task_id === p3.inb, '消える側 (確定側) で作業中なら 409 from_active (セッションは動かない)');
      stopSession({ taskId: p3.inb, workerId: w1.id, sessionId: sA.sessionId, reason: 'pause' });
      const sB = startSession({ taskId: p3.imp, worker: getIrohaWorker(w2.id) });
      const r3b = await merge(p3.inb, 'inbound');
      ok(r3b.status === 409 && r3b.json.error === 'from_active', '消える側 (取込側) で作業中でも 409 from_active');
      stopSession({ taskId: p3.imp, workerId: w2.id, sessionId: sB.sessionId, reason: 'pause' });
      // (4) 取込側が作業中 (人はいない) を、未着手の確定側に統合 → 確定側が作業中へ昇格・started_at を引き継ぐ
      const r4 = await merge(p3.inb, 'inbound');
      const k4 = TD.getTask(p3.inb);
      ok(r4.status === 200 && r4.json.promoted && k4.status === 'in_progress' && k4.started_at === '2026-09-03T01:00:00Z' && k4.notion_page_id === 'merge-page-9962', '着手済みの側を未着手側へ統合すると作業中に昇格 (started_at を引き継ぐ)');
      ok(db.prepare('SELECT COUNT(*) c FROM f_iroha_work_sessions WHERE task_id = ?').get(p3.inb).c === 2 && /読み直して/.test(r4.json.note), '作業時間は残す側に集まる。応答に再読込の案内');
      // (5) 行き先が取消済み → 統合後に残す側へ取消を伝える (未着手・実績なしなら自動で終了:取消)
      const p5 = mkPair(9963, 'MERGE-C5');
      db.prepare("UPDATE f_inbound_check_destinations SET cancelled_at = '2026-09-03T04:00:00Z' WHERE id = 9963").run();
      const r5 = await merge(p5.inb, 'import');
      const k5 = TD.getTask(p5.imp);
      ok(r5.status === 200 && r5.json.cancellation === 'closed' && k5.status === 'closed' && k5.close_reason === 'cancelled' && k5.destination_id === 9963, '取消済み行き先の統合は残す側も取消 (未着手・実績なし)');
      // (6) 消える側に取消の要求 (要確認) があれば残す側へ引き継ぐ
      const p6 = mkPair(9964, 'MERGE-C6', { importStatus: 'in_progress' });
      db.prepare("UPDATE f_iroha_tasks SET cancellation_requested_at = '2026-09-03T05:00:00Z', cancellation_source = 'inbound_reversal' WHERE id = ?").run(p6.inb);
      const r6 = await merge(p6.inb, 'import');
      ok(r6.status === 200 && TD.getTask(p6.imp).cancellation_requested_at === '2026-09-03T05:00:00Z' && TD.getTask(p6.imp).cancellation_source === 'inbound_reversal', '消える側の取消要求 (要確認) は残す側へ引き継ぐ');
      // (7) 残す側が過去に「続行」済み (日時なし・古い出どころだけ残る) へ、取消要求中の消える側を統合 → 日時と出どころは対で消える側から (Codex PR-B R5)
      const p7 = mkPair(9965, 'MERGE-C7', { importStatus: 'in_progress' });
      db.prepare("UPDATE f_iroha_tasks SET cancellation_requested_at = NULL, cancellation_source = 'inbound_reversal' WHERE id = ?").run(p7.imp);
      db.prepare("UPDATE f_iroha_tasks SET cancellation_requested_at = '2026-09-03T06:00:00Z', cancellation_source = 'inbound_import' WHERE id = ?").run(p7.inb);
      const r7 = await merge(p7.inb, 'import');
      ok(r7.status === 200 && TD.getTask(p7.imp).cancellation_requested_at === '2026-09-03T06:00:00Z' && TD.getTask(p7.imp).cancellation_source === 'inbound_import', '日時と出どころは同じ側から対で引き継ぐ (古い出どころと混ざらない)');
      // (8) 残す側に要求があれば、消える側の要求で上書きしない
      const p8 = mkPair(9966, 'MERGE-C8', { importStatus: 'in_progress' });
      db.prepare("UPDATE f_iroha_tasks SET cancellation_requested_at = '2026-09-03T07:00:00Z', cancellation_source = 'inbound_reversal' WHERE id = ?").run(p8.imp);
      db.prepare("UPDATE f_iroha_tasks SET cancellation_requested_at = '2026-09-03T08:00:00Z', cancellation_source = 'inbound_import' WHERE id = ?").run(p8.inb);
      const r8 = await merge(p8.inb, 'import');
      ok(r8.status === 200 && TD.getTask(p8.imp).cancellation_requested_at === '2026-09-03T07:00:00Z' && TD.getTask(p8.imp).cancellation_source === 'inbound_reversal', '残す側の要求はそのまま (消える側で上書きしない)');
      ok(!listLinkConflicts().some((c) => [p1.inb, p2.inb, p3.inb, p5.inb, p6.inb, p7.inb, p8.inb].includes(c.task_id)), '統合した衝突は一覧から消える');
    }
  } finally {
    _setDriveUpload(null);
    delete process.env.GOOGLE_SERVICE_ACCOUNT_KEY;
    setMetaValue('source_of_truth', null);
    server.close();
  }
}

console.log('\n[20] 入荷受付からのタスク生成 (task-intake) と差分取込の整合');
{
  const TD = await import('../apps/iroha-work/tasks-db.js');
  const M = await import('../apps/iroha-work/migrate.js');
  const S = await import('../apps/iroha-work/service.js');
  const num = (n) => ({ type: 'number', number: n });
  const { createTaskForDestination, linkTaskToNotionPage } = await import('../apps/iroha-work/task-intake.js');
  const { sourceOfTruth } = await import('../apps/iroha-work/db.js');
  const db = getDB();
  db.prepare(`INSERT OR REPLACE INTO f_iroha_work_master (code_key, 商品コード, material_code, storage_container, units_per_container, process_count, note, version, updated_at)
    VALUES ('intake-x', 'INTAKE-X', 'D-9', '箱', 12, 1, NULL, 2, '2026-09-03T00:00:00Z')`).run();
  const dest = { id: 9901, destination: 'iroha', batch_id: 1, line_key: 'X|1|1', ar_no: 'AR-X', product_id: 'INTAKE-X', product_name: '入荷商品', planned_qty: 10, actual_qty: 8,
    work_date: '2026-09-03', expiry_date: null, code_key: 'intake-x', cancelled_at: null };
  const made = createTaskForDestination(dest, { actor: 'やまだ' });
  const t = TD.getTask(made.id);
  ok(made.action === 'inserted' && t.destination_id === 9901 && t.qty === 8 && t.status === 'not_started' && t.created_by === 'inbound:やまだ', '行き先 1 行からタスク 1 枚 (数量は実数)');
  ok(t.master_snapshot && JSON.parse(t.master_snapshot).material_code === 'D-9' && JSON.parse(t.master_snapshot).units_per_container === 12, '作業仕様があればスナップショットに載る');
  ok(createTaskForDestination(dest).action === 'exists', '同じ行き先は 2 枚にならない');
  ok(createTaskForDestination({ ...dest, id: 9902, destination: 'bfaith' }).action === 'skipped' && createTaskForDestination({ ...dest, id: 9903, cancelled_at: 'x' }).action === 'skipped', 'B-Faith 行き・取消済みは作らない');
  clearEnrichCache();
  ok(S.buildTaskList().cards.some(c => c.id === made.id && c.master.material_code === 'D-9' && c.qty === 8), '一覧にそのまま出る (作業仕様つき)');
  // Notion 正本の間にカードが作られたら紐付く → 差分取込は同じタスクの更新になる (衝突扱いしない)
  const pg = mkPage({ status: '未着手', title: '入荷商品 (Notion 側)', code: 'INTAKE-X', qty: 8, props: { destination_id: num(9901) } });
  const pagesOf = async () => (await M.surveyNotion({ save: false })).pages.filter(p => p.pageId === pg.id);
  const plan0 = M.planImport(await pagesOf());
  ok(plan0.rows[0].warnings.some(w => /dup_destination_db/.test(w)), '紐付け前は DB 既存 destination との衝突として要確認');
  ok(linkTaskToNotionPage(9901, pg.id) === 1 && TD.getTask(made.id).notion_page_id === pg.id, 'カード作成時に notion_page_id を紐付ける');
  ok(linkTaskToNotionPage(9901, 'other-page') === 0 && TD.getTask(made.id).notion_page_id === pg.id, '既に付いていれば触らない');
  ok(linkTaskToNotionPage(null, pg.id) === 0 && linkTaskToNotionPage(9901, null) === 0, '引数が無ければ何もしない');
  const plan1 = M.planImport(await pagesOf());
  ok(!plan1.rows[0].warnings.some(w => /dup_destination_db/.test(w)) && plan1.rows[0].will_import, '紐付け後は同じタスクとして取り込める');
  const applied = M.applyImport(plan1.rows, { batchId: 'test-intake' });
  ok(applied.inserted === 0 && (applied.updated + applied.kept) === 1 && TD.getTaskByDestination(9901).id === made.id && TD.getTask(made.id).notion_page_id === pg.id, '取込は既存タスクの更新 (2 枚目を作らない)');
  ok(sourceOfTruth() === 'notion', 'sourceOfTruth は db.js から引ける (既定 notion)');
}

console.log('\n[21] まとめて棚入完了・履歴・ラベル待ちの一覧 (PR-C)');
{
  setMetaValue('source_of_truth', 'app');   // タスクの書き換えはアプリ正本のときだけ通る (Codex PR1 R15)
  const TD = await import('../apps/iroha-work/tasks-db.js');
  const S = await import('../apps/iroha-work/service.js');
  const { workSecondsByTask } = await import('../apps/iroha-work/db.js');
  const db = getDB();
  const w1 = listIrohaWorkers(true).find((w) => w.display_name === 'やまだ');
  const mk = (dest, name, status) => TD.upsertTaskFromImport({ notion_page_id: `prc-${dest}`, status, destination_id: dest,
    product_code: 'PRC-A', product_name: name, qty: 3, facility_code: dest % 2 ? 'iroha' : 'rashinban', arrival_date: '2026-09-01' }, { batchId: 'test-prc' }).id;

  // まとめて棚入完了の境界
  const bvv = (id) => ({ id, version: TD.getTask(id) ? TD.getTask(id).version : 0 });   // 選んだときの版
  // 記録を入れる直前の見直し (Notion の実ページを取っている間に正本が変わることがある — Codex PR1 R14)
  {
    const wG = listIrohaWorkers(true).find((x) => x.display_name === 'すずき');
    const before = db.prepare('SELECT COUNT(*) c FROM f_iroha_work_sessions').get().c;
    const blocked = startSession({ pageId: 'guard-page', worker: getIrohaWorker(wG.id),
      guard: () => ({ ok: false, error: 'notion_mode', message: '正本が変わりました' }) });
    ok(blocked.ok === false && blocked.error === 'notion_mode', '直前の見直しで断れる');
    ok(db.prepare('SELECT COUNT(*) c FROM f_iroha_work_sessions').get().c === before, '記録も増えていない');
  }
  ok(TD.bulkCloseReady({ taskIds: [] }).error === 'bad_request', '空は bad_request');
  ok(TD.bulkCloseReady({ taskIds: Array.from({ length: 201 }, (_, i) => bvv(i + 1)) }).error === 'bad_request', '201 件は bad_request (一度に 200 件まで)');
  ok(TD.bulkCloseReady({ taskIds: [1] }).error === 'bad_request', '版を添えない指定は受けない (入口だけの検査にしない)');
  // ⭐正本が Notion に戻っていたら、タスクの書き換えは通らない (正本の切替は version を変えないので、
  //   楽観ロックでは気づけない。更新と同じトランザクションの中で見る — Codex PR1 R15)
  {
    const nt = mk(9299, '正本チェック用', 'ready_for_stocking');
    setMetaValue('source_of_truth', null);
    ok(TD.bulkCloseReady({ taskIds: [bvv(nt)] }).error === 'notion_mode', 'まとめて棚入完了は断る');
    ok(TD.setPlannedDate({ taskId: nt, plannedDate: '2099-12-31', expectVersion: TD.getTask(nt).version }).error === 'notion_mode', '今日やるも断る');
    ok(TD.setExternalReady({ taskId: nt, ready: true, expectVersion: TD.getTask(nt).version }).error === 'notion_mode', '外部準備OKも断る');
    ok(TD.changeTaskStatus({ taskId: nt, to: 'in_progress', expectVersion: TD.getTask(nt).version, isStaff: true, actor: 'test' }).error === 'notion_mode', '状態変更も断る');
    ok(TD.upsertLabelWait({ taskId: nt, fields: { note: 'x' } }).error === 'notion_mode', 'ラベル待ちの登録も断る');
    db.prepare('UPDATE f_iroha_tasks SET cancellation_requested_at = ? WHERE id = ?').run(new Date().toISOString(), nt);
    ok(TD.resolveCancellation({ taskId: nt, decision: 'continue', expectVersion: TD.getTask(nt).version, isStaff: true, actor: 'test' }).error === 'notion_mode',
      '取消の判断 (続行) も断る');
    ok(TD.resolveCancellation({ taskId: nt, decision: 'cancel', expectVersion: TD.getTask(nt).version, isStaff: true, actor: 'test' }).error === 'notion_mode',
      '取消の判断 (取消) も断る');
    ok(TD.getTask(nt).cancellation_requested_at != null, '取消の要確認は消えていない');
    ok(TD.taskErrorStatus('notion_mode') === 409, '正本が切り替わった競合は 409 (入力不正の 400 ではない)');
    ok(TD.startTaskSession({ taskId: nt, worker: getIrohaWorker(w1.id) }).error === 'notion_mode', '作業開始も断る');
    ok(db.prepare('SELECT COUNT(*) c FROM f_iroha_work_sessions WHERE task_id = ?').get(nt).c === 0, '記録も増えていない');
    db.prepare('UPDATE f_iroha_tasks SET cancellation_requested_at = NULL WHERE id = ?').run(nt);
    ok(TD.getTask(nt).status === 'ready_for_stocking' && TD.getTask(nt).external_ready === 0, 'どれも DB を変えていない');
    setMetaValue('source_of_truth', 'app');
  }
  const a = mk(9201, '棚入待ちA', 'ready_for_stocking');
  const b = mk(9202, '棚入待ちB', 'ready_for_stocking');
  const r = TD.bulkCloseReady({ taskIds: [bvv(a), bvv(b), bvv(a)], actor: 'たにがわ (いろはアプリ)', workerId: w1.id, workerName: 'やまだ', deviceLabel: 'ipad-1' });
  ok(r.ok && r.done.length === 2 && r.skipped.length === 0, '同じ id を 2 回渡しても 1 回だけ処理する');
  ok(TD.getTask(a).closed_by === 'たにがわ (いろはアプリ)' && TD.getTask(a).close_reason === 'stocked', 'だれが棚入完了にしたかが残る');
  const again = TD.bulkCloseReady({ taskIds: [bvv(a)] });
  ok(again.ok && again.done.length === 0 && again.skipped[0].reason === 'already' && again.skipped[0].title === '棚入待ちA', '2 回目は already (何も変えない)');

  // 履歴: 期間・検索・作業時間
  const c = mk(9203, '履歴C', 'ready_for_stocking');
  const s1 = startSession({ taskId: c, worker: getIrohaWorker(w1.id) });
  stopSession({ taskId: c, workerId: w1.id, sessionId: s1.sessionId, reason: 'done' });
  db.prepare("UPDATE f_iroha_work_sessions SET raw_seconds = 3660 WHERE task_id = ?").run(c);
  TD.bulkCloseReady({ taskIds: [bvv(c)], actor: 'test' });
  const secs = workSecondsByTask([c, a]);
  ok(secs.get(c).seconds === 3660 && secs.get(c).people === 1 && !secs.has(a), '作業時間の合計をタスクごとに引ける (記録が無いタスクは入らない)');
  ok(workSecondsByTask([]).size === 0 && workSecondsByTask([0, -1, 1.5]).size === 0, '不正な id は数えない');
  const h = S.buildHistory({ q: 'PRC-A' });
  ok(h.total === 3 && h.rows.length === 3 && h.rows[0].id === c, '履歴は新しい順 (終了した 3 件)');
  const row = h.rows.find((x) => x.id === c);
  ok(row.work_seconds === 3660 && row.workers === 1 && row.status_label === '終了 · 棚入完了' && row.facility_name === 'いろは', '履歴に作業時間・結果・拠点名が出る');
  ok(h.rows.find((x) => x.id === 9202 || x.title === '棚入待ちB').facility_name === '羅針盤', '外部拠点の名前も出る');
  ok(S.buildHistory({ q: 'PRC-A', limit: 1 }).rows.length === 1 && S.buildHistory({ q: 'PRC-A', limit: 1 }).total === 3, '上限をかけても総件数は正しい');
  ok(S.buildHistory({ q: 'PRC-A', from: '2999-01-01T00:00:00Z' }).total === 0, '期間で絞れる');
  ok(TD.countClosedTasks({ q: 'ないものPRC' }) === 0, '検索は商品名・商品コードだけ');

  // 監査ログ (操作履歴) が書けなければ、棚入完了ごと戻す (権限のいる操作なので記録を落とさない — Codex PR-C R1)
  {
    const e1 = mk(9205, 'ログ失敗E', 'ready_for_stocking');
    db.exec('ALTER TABLE f_iroha_app_events RENAME TO f_iroha_app_events__bak');
    let threw = null;
    try { TD.bulkCloseReady({ taskIds: [bvv(e1)], actor: 'test' }); } catch (e) { threw = e; }
    db.exec('ALTER TABLE f_iroha_app_events__bak RENAME TO f_iroha_app_events');
    ok(threw && /no such table/i.test(threw.message), '履歴を書けないと例外になる (握り潰さない)');
    ok(TD.getTask(e1).status === 'ready_for_stocking' && TD.getTask(e1).closed_at == null, '棚入完了は取り消される (棚入待ちのまま)');
    ok(TD.bulkCloseReady({ taskIds: [bvv(e1)], actor: 'test' }).done.length === 1, '履歴が戻ればやり直せる');
  }

  // ラベル待ちの一覧に商品情報が乗る
  const d = TD.upsertTaskFromImport({ notion_page_id: 'prc-9204', status: 'on_hold', hold_reason_code: 'label_shortage', destination_id: 9204,
    product_code: 'PRC-A', product_name: 'ラベル待ちD', qty: 3, facility_code: 'iroha', arrival_date: '2026-09-01' }, { batchId: 'test-prc' }).id;
  const lw = TD.upsertLabelWait({ taskId: d, fields: { occurred_on: '2026-09-03', recorded_by_name: '和田', qty: 5, location: 'Z', label_ordered: true, note: 'メーカー連絡待ち' } });
  ok(lw.ok, '前提: ラベル待ちを登録');
  const rows = TD.listLabelWaits({});
  const mine = rows.find((x) => x.id === lw.row.id);
  ok(mine && mine.product_name === 'ラベル待ちD' && mine.product_code === 'PRC-A' && mine.task_status === 'on_hold' && mine.hold_reason_code === 'label_shortage',
    '一覧に商品名・商品コード・カードの状態が乗る');
  ok(mine.location === 'Z' && mine.label_ordered === 1 && mine.qty === 5 && mine.note === 'メーカー連絡待ち', 'xlsx の項目がそのまま返る');
  const upd = TD.upsertLabelWait({ id: lw.row.id, taskId: d, expectVersion: lw.row.version, fields: { done: true, restocked_on: '2026-09-10' } });
  ok(upd.ok && upd.row.done === 1, '完了にできる');
  ok(!TD.listLabelWaits({}).some((x) => x.id === lw.row.id) && TD.listLabelWaits({ openOnly: false }).some((x) => x.id === lw.row.id), '完了は既定の一覧から消え、「すべて」には出る');
  ok(TD.listLabelWaits({ taskId: d, openOnly: false }).length === 1 && TD.listLabelWaits({ taskId: 999999, openOnly: false }).length === 0, 'カードで絞れる');
}

console.log('\n[23] 想定作業時間・必要保管箱 (Notion の計算式をそのまま)');
{
  const utcNowT = () => new Date().toISOString();
  const S2 = await import('../apps/iroha-work/service.js');
  const TD = await import('../apps/iroha-work/tasks-db.js');
  const db = getDB();
  // 想定作業時間 = round(数量 × 工程数 × 5 / 3600 × 10) / 10
  ok(S2.planHours(100, 2) === 0.3, '100個×2工程 = 0.3 時間 (1000秒→0.277…を小数1桁に)');
  ok(S2.planHours(720, 1) === 1, '720個×1工程 = ちょうど 1 時間');
  ok(S2.planHours(1, 1) === 0, '小さすぎる場合は 0 (切り捨てでなく四捨五入)');
  ok(S2.planHours(null, 2) === null && S2.planHours(100, null) === null && S2.planHours(0, 2) === null && S2.planHours(100, 0) === null,
    '数量か工程数が無い・0 なら出さない');
  // 必要保管箱 = 入数で割る。Z 在庫があればその引当を引いた数、無ければ数量
  ok(S2.neededBoxes(100, 10) === '10箱', '割り切れれば「N箱」');
  ok(S2.neededBoxes(105, 10) === '10箱+5', '余りは「N箱+余り」');
  ok(S2.neededBoxes(5, 10) === '0箱+5', '入数に満たなければ 0箱+余り');
  ok(S2.neededBoxes(100, null) === null && S2.neededBoxes(100, 0) === null, '入数が無い・0 なら出さない (Notion と同じ)');
  ok(S2.neededBoxes(100, 10, 30, 5) === '2箱+5', 'Z 在庫があれば Z在庫−Z引当 で計算 (30−5=25 → 2箱+5)');
  ok(S2.neededBoxes(100, 10, 0, 0) === '10箱', 'Z 在庫が 0 なら数量で計算');
  ok(S2.neededBoxes(100, 10, 20, 20) === '0箱', 'Z 在庫が全部引当済みなら 0箱');
  ok(S2.neededBoxes(100, 10, 20, 30) === null, 'Z 引当が Z 在庫より多ければ (負になる) 出さない');
  ok(S2.neededBoxes(100.5, 10) === null && S2.neededBoxes(100, 10.5) === null, '小数は出さない (箱数を保証できない)');
  ok(S2.neededBoxes(Number.MAX_VALUE, 10) === null && S2.planHours(Number.MAX_VALUE, 2) === null, '桁があふれる値は出さない (Infinity を返さない)');
  ok(S2.planHours(-100, 2) === null && S2.neededBoxes(-100, 10) === null, '負の数は出さない');
  // ⭐日付は JST。UTC 15:00 = JST 翌日 0:00 の境目を固定の時刻で確かめる (Codex P1 R1)
  ok(S2.jstToday(new Date('2026-09-04T14:59:59Z')) === '2026-09-04', 'UTC 14:59 はまだ 9/4 (JST 23:59)');
  ok(S2.jstToday(new Date('2026-09-04T15:00:00Z')) === '2026-09-05', 'UTC 15:00 で 9/5 になる (JST 0:00)');
  ok(S2.jstTomorrow('2026-09-04') === '2026-09-05' && S2.jstTomorrow('2026-08-31') === '2026-09-01'
    && S2.jstTomorrow('2026-12-31') === '2027-01-01' && S2.jstTomorrow('2028-02-28') === '2028-02-29',
    '明日の計算は月末・年末・うるう年でも合う');
  ok(S2.whenOf('2026-09-05', '2026-09-04') === 'tomorrow' && S2.whenOf('2026-09-04', '2026-09-04') === 'today'
    && S2.whenOf('2026-09-03', '2026-09-04') === 'over' && S2.whenOf('2026-09-30', '2026-09-04') === 'later'
    && S2.whenOf(null, '2026-09-04') === null, '今日 / 明日 / やり残し / 先 / 未定 を日付で分ける');
  ok(S2.whenOf('2026-08-31', '2026-09-01') === 'over' && S2.whenOf('2026-09-01', '2026-08-31') === 'tomorrow',
    '月をまたいでも「昨日」「明日」を取り違えない');
  // 大きさ (配送方法で見なす)。大きいほど先にやる
  ok(S2.sizeOfShipping('定形外郵便').rank === 1 && S2.sizeOfShipping('ヤマト(ネコポス)').rank === 2
    && S2.sizeOfShipping('ゆうパケットパフ').rank === 3 && S2.sizeOfShipping('ヤマト宅急便【50サイズ専用】').rank === 4
    && S2.sizeOfShipping('ヤマト(発払い)B2v6').rank === 5, '配送方法から大きさの順が出る (定形外<ネコポス<ゆうパケット<50<発払い)');
  ok(S2.sizeOfShipping('') === null && S2.sizeOfShipping(null) === null && S2.sizeOfShipping('AES') === null,
    '分からない配送方法は null (並びは最後・画面は「大きさ 不明」)');
  ok(S2.sizeOfShipping('宅急便50サイズ以下').rank === 4, '「50サイズ」は 60 の規則に吸われない (上から大きい順に見る)');

  // 一覧のカードに乗る
  const t = TD.upsertTaskFromImport({ notion_page_id: 'plan-1', status: 'not_started', destination_id: 9301,
    product_code: 'PLAN-A', product_name: '想定時間テスト', qty: 100, facility_code: 'iroha',
    master_snapshot: { units_per_container: 10, process_count: 2 } }, { batchId: 'test-plan' }).id;
  clearEnrichCache();
  const card = S2.buildTaskList().cards.find(c => c.id === t);
  ok(card && card.plan_hours === 0.3 && card.boxes === '10箱', '一覧のカードに想定作業時間と必要保管箱が乗る');
  // Z ロケの在庫があれば、そちらで数える
  db.exec("DELETE FROM mirror_logizard_stock WHERE 商品ID = 'PLAN-A'");
  db.prepare(`INSERT INTO mirror_logizard_stock (商品ID, 商品名, ロケ, ブロック略称, 品質区分名, 在庫数, 引当数, captured_at, synced_at)
    VALUES ('PLAN-A', '想定時間テスト', 'Z01-001-001-01', 'Z01', '良品', 30, 5, ?, ?)`).run(new Date().toISOString(), new Date().toISOString());
  clearEnrichCache();
  const card2 = S2.buildTaskList().cards.find(c => c.id === t);
  ok(card2.boxes === '2箱+5' && card2.z_stock === 30, 'Z ロケに在庫があれば、その分で必要保管箱を出す');
  // 不良品は外に出せないので数えない (中原さん 2026-09-03)
  db.prepare(`INSERT INTO mirror_logizard_stock (商品ID, 商品名, ロケ, ブロック略称, 品質区分名, 在庫数, 引当数, captured_at, synced_at)
    VALUES ('PLAN-A', '想定時間テスト', 'Z01-001-001-02', 'Z01', '不良品', 100, 0, ?, ?)`).run(new Date().toISOString(), new Date().toISOString());
  clearEnrichCache();
  const card3 = S2.buildTaskList().cards.find(c => c.id === t);
  ok(card3.z_stock === 30 && card3.boxes === '2箱+5', 'Z ロケでも不良品は数えない (良品だけ)');
  // Z 以外のロケは数えない
  db.prepare(`INSERT INTO mirror_logizard_stock (商品ID, 商品名, ロケ, ブロック略称, 品質区分名, 在庫数, 引当数, captured_at, synced_at)
    VALUES ('PLAN-A', '想定時間テスト', 'P3F-001-001-01', 'P3F', '良品', 500, 0, ?, ?)`).run(new Date().toISOString(), new Date().toISOString());
  clearEnrichCache();
  ok(S2.buildTaskList().cards.find(c => c.id === t).z_stock === 30, 'Z 以外のロケ (本館・いろは棟) は数えない');
  // 有効期限・入荷日で分かれた行は合算する。ブロック略称だけが Z の行も数える (Codex FB R1)
  const insZ = db.prepare(`INSERT INTO mirror_logizard_stock (商品ID, 商品名, ロケ, ブロック略称, 品質区分名, 有効期限, 在庫数, 引当数, captured_at, synced_at)
    VALUES ('PLAN-A', '想定時間テスト', ?, ?, '良品', ?, ?, ?, ?, ?)`);
  const nowIso = new Date().toISOString();
  insZ.run('Z01-001-001-03', 'Z01', '2027-01', 12, 2, nowIso, nowIso);       // ロケも略称も Z
  insZ.run('AAAA-001-001-01', 'ZZZ', '2027-02', 8, 0, nowIso, nowIso);        // 略称だけ Z (棚以外)
  clearEnrichCache();
  const card4 = S2.buildTaskList().cards.find(c => c.id === t);
  ok(card4.z_stock === 50, 'Z の行は期限・入荷日で分かれていても合算する (30+12+8)');
  ok(card4.boxes === '4箱+3', '引当を引いて計算する (50−7=43 → 4箱+3)');
  // 画面に出す用: 引当と、在庫ミラーをいつ取ったか (毎時更新。画面は 60 秒ごとに読み直す)
  ok(card4.z_allocated === 7 && card4.z_free === 43, '引当と使える数もカードに乗る');
  ok(card4.z_at && card4.z_at === db.prepare("SELECT MAX(captured_at) m FROM mirror_logizard_stock WHERE 商品ID = 'PLAN-A'").get().m,
    '在庫ミラーの取得時刻 (いちばん新しいもの) が乗る');
  // 在庫が動いたら、次に一覧を作るときには新しい数になる (キャッシュしない)
  db.prepare("UPDATE mirror_logizard_stock SET 在庫数 = 在庫数 + 100 WHERE 商品ID = 'PLAN-A' AND ロケ = 'Z01-001-001-01'").run();
  clearEnrichCache();
  ok(S2.buildTaskList().cards.find(c => c.id === t).z_stock === 150, 'ロジザード在庫が更新されたら次の読み込みで反映される');
  db.prepare("UPDATE mirror_logizard_stock SET 在庫数 = 在庫数 - 100 WHERE 商品ID = 'PLAN-A' AND ロケ = 'Z01-001-001-01'").run();

  // 羅針盤・ワークセンターに出したカードは Y ロケ (外に出している分) を見せる (中原さん 2026-09-03)
  ok(S2.stockLocOf('rashinban') === 'Y' && S2.stockLocOf('workcenter') === 'Y', '羅針盤・ワークセンターは Y');
  ok(S2.stockLocOf('iroha') === 'Z' && S2.stockLocOf('jobsupport') === 'Z' && S2.stockLocOf('rehas') === 'Z' && S2.stockLocOf(null) === 'Z',
    'いろは・ジョブサポ・リハス・拠点なしは Z');
  db.prepare(`INSERT INTO mirror_logizard_stock (商品ID, 商品名, ロケ, ブロック略称, 品質区分名, 在庫数, 引当数, captured_at, synced_at)
    VALUES ('PLAN-A', '想定時間テスト', 'Y01-001-001-01', 'Y01', '良品', 40, 4, ?, ?)`).run(nowIso, nowIso);
  const tY = TD.upsertTaskFromImport({ notion_page_id: 'plan-y', status: 'in_progress', destination_id: 9303, facility_code: 'rashinban',
    product_code: 'PLAN-A', product_name: '羅針盤に出した', qty: 100, master_snapshot: { units_per_container: 10, process_count: 2 } }, { batchId: 'test-plan' }).id;
  clearEnrichCache();
  const cards = S2.buildTaskList().cards;
  const cY = cards.find(c => c.id === tY);
  const cZ = cards.find(c => c.id === t);
  ok(cY.loc_kind === 'Y' && cY.loc_stock === 40 && cY.loc_allocated === 4 && cY.loc_free === 36, '羅針盤のカードは Y ロケの在庫 (引当・使える数つき)');
  ok(cZ.loc_kind === 'Z' && cZ.loc_stock === 50, 'いろはのカードは Z ロケのまま');
  ok(cY.z_stock === 50, '必要保管箱は Notion の式のまま Z を使う (表示だけ Y に変える)');
  db.prepare("DELETE FROM mirror_logizard_stock WHERE ロケ LIKE 'Y%'").run();
  clearEnrichCache();
  ok(S2.buildTaskList().cards.find(c => c.id === tY).loc_stock === null, 'Y に在庫が無ければ出さない (Z を代わりに出さない)');
  db.exec("DELETE FROM mirror_logizard_stock WHERE 商品ID = 'PLAN-A'");
  clearEnrichCache();
  ok(S2.buildTaskList().cards.find(c => c.id === t).z_stock === null, 'Z に在庫が無ければ null (数量で計算)');
  db.exec("DELETE FROM mirror_logizard_stock WHERE 商品ID = 'PLAN-A'");
  clearEnrichCache();

  // 古い DB (external_ready が無い) に列を足しても、0/1 しか入らない (Codex FB R1)
  {
    const { createTables } = await import('../apps/iroha-work/db.js');
    db.pragma('foreign_keys = OFF');
    db.exec('CREATE TEMP TABLE er_bak AS SELECT * FROM f_iroha_tasks');
    const cols = db.prepare('PRAGMA table_info(f_iroha_tasks)').all().map((c) => c.name).filter((c) => c !== 'external_ready');
    db.exec(`DROP TABLE f_iroha_tasks; CREATE TABLE f_iroha_tasks (${db.prepare("SELECT sql FROM sqlite_master WHERE name = 'er_bak'").get() ? '' : ''}`
      + cols.map((c) => `${c} ${c === 'id' ? 'INTEGER PRIMARY KEY AUTOINCREMENT' : 'TEXT'}`).join(', ') + ');');
    db.exec(`INSERT INTO f_iroha_tasks (${cols.join(', ')}) SELECT ${cols.join(', ')} FROM er_bak; DROP TABLE er_bak;`);
    db.pragma('foreign_keys = ON');
    ok(!db.prepare('PRAGMA table_info(f_iroha_tasks)').all().some((c) => c.name === 'external_ready'), '前提: external_ready の無い古い版');
    createTables(db);
    const col = db.prepare('PRAGMA table_info(f_iroha_tasks)').all().find((c) => c.name === 'external_ready');
    ok(col && col.dflt_value === '0' && col.notnull === 1, '起動時に列が足される (既定 0・NOT NULL)');
    let bad = null;
    try { db.prepare('UPDATE f_iroha_tasks SET external_ready = 2 WHERE id = ?').run(t); } catch (e) { bad = e; }
    ok(bad && /CHECK/.test(bad.message), '足した列にも CHECK が効く (0/1 しか入らない)');
  }

  // 外部施設に出す準備OK (状態とは別のチェック。Notion のチェックボックスの置き換え)
  {
    const cur = TD.getTask(t);
    ok(cur.external_ready === 0, '既定はチェックなし');
    const on = TD.setExternalReady({ taskId: t, ready: true, expectVersion: cur.version, actor: 'test' });
    ok(on.ok && on.task.external_ready === 1 && on.task.version === cur.version + 1, 'チェックできる (version が進む)');
    ok(TD.setExternalReady({ taskId: t, ready: false, expectVersion: cur.version, actor: 'test' }).error === 'conflict', '古い version は競合');
    clearEnrichCache();
    ok(S2.buildTaskList().cards.find(c => c.id === t).external_ready === true, '一覧のカードに出る');
    ok(db.prepare("SELECT COUNT(*) c FROM f_iroha_app_events WHERE action = 'task_external_ready' AND task_id = ?").get(t).c === 1, '履歴に残る');
    const off = TD.setExternalReady({ taskId: t, ready: false, expectVersion: TD.getTask(t).version, actor: 'test' });
    ok(off.ok && off.task.external_ready === 0, 'やめられる');
    ok(TD.setExternalReady({ taskId: 999999, ready: true, expectVersion: 1 }).error === 'not_found', '無いカードは not_found');
    // 終了したカードでは触らない
    const done = TD.upsertTaskFromImport({ notion_page_id: 'ext-closed', status: 'closed', close_reason: 'stocked',
      closed_at: '2026-09-03T00:00:00Z', destination_id: 9302, product_code: 'PLAN-A', qty: 1 }, { batchId: 'test-plan' }).id;
    ok(TD.setExternalReady({ taskId: done, ready: true, expectVersion: TD.getTask(done).version }).error === 'done_card', '終了したカードは変えられない');
  }

  // 名前のないカードの片づけ (中原さん 2026-09-03「名称なしのカードが 2 つある。Notion にも無いので消して」)
  {
    // Notion にも入荷受付にも紐づかない行 (素性の分からないカード) を作る
    const insStray = db.prepare(`INSERT INTO f_iroha_tasks (status, facility_code, product_name, version, created_at, created_by, updated_at, updated_by)
      VALUES ('not_started', 'iroha', ?, 1, ?, 'test', ?, 'test')`);
    const mkNameless = (name) => Number(insStray.run(name, utcNowT(), utcNowT()).lastInsertRowid);
    const a = mkNameless(null);
    const b = mkNameless('(名称なし)');
    const c2 = mkNameless('  ');
    const keep = TD.upsertTaskFromImport({ notion_page_id: 'stray-keep', status: 'not_started', destination_id: 9404,
      product_code: 'X', product_name: 'ちゃんと名前がある', qty: 1, facility_code: 'iroha' }, { batchId: 'test-stray' }).id;
    const listed = TD.listNamelessTasks().map(x => x.id);
    ok(listed.includes(a) && listed.includes(b) && listed.includes(c2) && !listed.includes(keep),
      '名前が無い・「(名称なし)」・空白だけのカードが挙がる (名前があるものは挙がらない)');
    ok(TD.listNamelessTasks().find(x => x.id === a).sessions === 0, '作業・写真・ラベル待ちの数も返す (消していいか判断するため)');
    // 記録が無ければ行ごと消す
    const r1 = TD.removeStrayTask({ taskId: a, actor: 'admin@test' });
    ok(r1.ok && r1.action === 'deleted' && TD.getTask(a) === null, '記録が無ければ消える');
    ok(db.prepare("SELECT COUNT(*) c FROM f_iroha_app_events WHERE action = 'task_removed'").get().c >= 1, '消したことは履歴に残る');
    ok(TD.removeStrayTask({ taskId: a }).error === 'not_found', '消した後にもう一度呼ぶと not_found');
    // 記録があれば消さずに「終了 (在庫化対象外)」
    const w1x = listIrohaWorkers(true).find((x) => x.display_name === 'やまだ');
    const s1 = startSession({ taskId: b, worker: getIrohaWorker(w1x.id) });
    stopSession({ taskId: b, workerId: w1x.id, sessionId: s1.sessionId, reason: 'done' });
    const r2 = TD.removeStrayTask({ taskId: b, actor: 'admin@test' });
    const bAfter = TD.getTask(b);
    ok(r2.ok && r2.action === 'closed' && bAfter && bAfter.status === 'closed' && bAfter.close_reason === 'out_of_scope',
      '作業の記録があれば消さずに 終了 (在庫化対象外)');
    ok(db.prepare('SELECT COUNT(*) c FROM f_iroha_work_sessions WHERE task_id = ?').get(b).c === 1, '記録は残る (持ち主を消さない)');
    ok(/管理画面から片づけ/.test(bAfter.migration_note || ''), 'なぜ片づけたかが残る');
    ok(!TD.listNamelessTasks().some(x => x.id === b), '片づけたら一覧から消える');

    // 片づけていいのは「名前が無い・Notion に紐づかない・入荷受付に紐づかない・終わっていない」だけ (Codex FB R3)
    {
      const named = Number(insStray.run('名前がある', utcNowT(), utcNowT()).lastInsertRowid);
      ok(TD.removeStrayTask({ taskId: named }).error === 'not_stray', '名前があるカードは片づけられない');
      ok(TD.getTask(named) !== null, '消えていない');
      const withPage = Number(insStray.run(null, utcNowT(), utcNowT()).lastInsertRowid);
      db.prepare("UPDATE f_iroha_tasks SET notion_page_id = 'stray-has-page' WHERE id = ?").run(withPage);
      ok(TD.removeStrayTask({ taskId: withPage }).error === 'not_stray', 'Notion のカードに紐づくものは片づけられない');
      ok(!TD.listNamelessTasks().some(x => x.id === withPage), '一覧にも出ない');
      const withDest = Number(insStray.run(null, utcNowT(), utcNowT()).lastInsertRowid);
      db.prepare('UPDATE f_iroha_tasks SET destination_id = 9499 WHERE id = ?').run(withDest);
      ok(TD.removeStrayTask({ taskId: withDest }).error === 'not_stray', '入荷受付の行き先に紐づくものは片づけられない');
      // 作業中の人がいるうちは片づけない
      const busy = Number(insStray.run(null, utcNowT(), utcNowT()).lastInsertRowid);
      const wB = listIrohaWorkers(true).find((x) => x.display_name === 'すずき');
      const sB = startSession({ taskId: busy, worker: getIrohaWorker(wB.id) });
      ok(TD.removeStrayTask({ taskId: busy }).error === 'active_sessions', '作業中の人がいれば片づけられない');
      ok(TD.getTask(busy).status !== 'closed', '状態も変わらない');
      stopSession({ taskId: busy, workerId: wB.id, sessionId: sB.sessionId, reason: 'done' });
      ok(TD.removeStrayTask({ taskId: busy }).action === 'closed', '作業を終えれば片づけられる (記録があるので終了)');
    }

    // 作業中の人がいるまま終了にしない (終了カードは読むだけなので、記録が開いたまま取り残される — Codex PR1 R3)
    {
      const t2 = TD.upsertTaskFromImport({ notion_page_id: 'busy-close', status: 'ready_for_stocking', destination_id: 9505,
        product_code: 'PLAN-A', product_name: '作業中のまま終了', qty: 1, facility_code: 'iroha' }, { batchId: 'test-busy' }).id;
      const wB2 = listIrohaWorkers(true).find((x) => x.display_name === 'すずき');
      const sB2 = startSession({ taskId: t2, worker: getIrohaWorker(wB2.id) });
      const closeTry = TD.changeTaskStatus({ taskId: t2, to: 'closed', closeReason: 'stocked',
        expectVersion: TD.getTask(t2).version, isStaff: true, actor: 'test' });
      ok(closeTry.error === 'active_sessions', '作業中の人がいれば終了にできない');
      ok(TD.getTask(t2).status === 'ready_for_stocking', '状態も変わらない');
      const bulkTry = TD.bulkCloseReady({ taskIds: [{ id: t2, version: TD.getTask(t2).version }], actor: 'test' });
      ok(bulkTry.done.length === 0 && bulkTry.skipped[0].reason === 'active_sessions', 'まとめて棚入完了でも飛ばす');
      stopSession({ taskId: t2, workerId: wB2.id, sessionId: sB2.sessionId, reason: 'done' });
      ok(TD.bulkCloseReady({ taskIds: [{ id: t2, version: TD.getTask(t2).version }], actor: 'test' }).done.length === 1, '作業を終えれば終了にできる');
      // 版ずれと「作業中」を取り違えない: 別の端末が先に動かしていたら competing でなく conflict (Codex PR1 R5)
      {
        const t3 = TD.upsertTaskFromImport({ notion_page_id: 'busy-stale', status: 'ready_for_stocking', destination_id: 9506,
          product_code: 'PLAN-A', product_name: '版ずれ + 作業中', qty: 1, facility_code: 'iroha' }, { batchId: 'test-busy' }).id;
        const wB3 = listIrohaWorkers(true).find((x) => x.display_name === 'すずき');
        const sB3 = startSession({ taskId: t3, worker: getIrohaWorker(wB3.id) });
        const stale = TD.getTask(t3).version - 1;
        const r3 = TD.changeTaskStatus({ taskId: t3, to: 'closed', closeReason: 'stocked', expectVersion: stale, isStaff: true, actor: 'test' });
        ok(r3.ok === false && r3.error === 'conflict', '版が古ければ (作業中でも) 競合として断る — 作業中扱いにしない');
        stopSession({ taskId: t3, workerId: wB3.id, sessionId: sB3.sessionId, reason: 'done' });
        // 取り消したセッションは「作業中」に数えない (終わっていない行として残っていても — Codex PR1 R6)
        const t4 = TD.upsertTaskFromImport({ notion_page_id: 'busy-void', status: 'ready_for_stocking', destination_id: 9507,
          product_code: 'PLAN-A', product_name: '取り消し済みの作業', qty: 1, facility_code: 'iroha' }, { batchId: 'test-busy' }).id;
        db.prepare(`INSERT INTO f_iroha_work_sessions (task_id, worker_id, worker_name, started_at, voided_at, voided_by)
          VALUES (?, ?, 'すずき', ?, ?, 'test')`).run(t4, wB3.id, utcNowT(), utcNowT());
        ok(db.prepare('SELECT ended_at FROM f_iroha_work_sessions WHERE task_id = ?').get(t4).ended_at == null,
          '取り消し済みだが終わっていない記録がある');
        ok(TD.bulkCloseReady({ taskIds: [{ id: t4, version: TD.getTask(t4).version }], actor: 'test' }).done.length === 1, 'それでもまとめて棚入完了は通る');
        ok(TD.getTask(t4).status === 'closed', '終了になっている');
      }
      // 終了からのやり直しは、理由の記録が書けなければ再開そのものを取り消す
      db.exec('ALTER TABLE f_iroha_app_events RENAME TO f_iroha_app_events__bak2');
      let reopenErr = null;
      try {
        TD.changeTaskStatus({ taskId: t2, to: 'in_progress', expectVersion: TD.getTask(t2).version,
          isStaff: true, actor: 'test', reason: 'やり直し' });
      } catch (e) { reopenErr = e; }
      db.exec('ALTER TABLE f_iroha_app_events__bak2 RENAME TO f_iroha_app_events');
      ok(reopenErr && /no such table/i.test(reopenErr.message), 'やり直しの理由を書けないと例外になる');
      ok(TD.getTask(t2).status === 'closed', '再開も取り消される (理由の残らない再開をしない)');
      ok(TD.changeTaskStatus({ taskId: t2, to: 'in_progress', expectVersion: TD.getTask(t2).version,
        isStaff: true, actor: 'test', reason: 'やり直し' }).ok, '記録が戻ればやり直せる');
    }
    TD.removeStrayTask({ taskId: c2, actor: 'admin@test' });
  }

  // タスク表を作り直しても「外部に出す準備OK」が 0 に戻らない (Codex FB R3)
  {
    const { createTables } = await import('../apps/iroha-work/db.js');
    const keep = TD.upsertTaskFromImport({ notion_page_id: 'ext-keep', status: 'not_started', destination_id: 9420,
      product_code: 'PLAN-A', product_name: '再構築でも残る', qty: 1, facility_code: 'iroha' }, { batchId: 'test-rebuild' }).id;
    TD.setExternalReady({ taskId: keep, ready: true, expectVersion: TD.getTask(keep).version, actor: 'test' });
    ok(TD.getTask(keep).external_ready === 1, '前提: チェックが付いている');
    // CHECK を 1 つ落とした「古い版」にして、作り直しを起こす
    const sql = db.prepare("SELECT sql FROM sqlite_master WHERE name = 'f_iroha_tasks'").get().sql;
    const cols = db.prepare('PRAGMA table_info(f_iroha_tasks)').all().map((c) => c.name);
    db.pragma('foreign_keys = OFF');
    db.exec('CREATE TEMP TABLE rb AS SELECT * FROM f_iroha_tasks; DROP TABLE f_iroha_tasks;');
    db.exec(sql.replace("CHECK ((status = 'closed') = (close_reason IS NOT NULL)),", ''));
    db.exec(`INSERT INTO f_iroha_tasks (${cols.join(', ')}) SELECT ${cols.join(', ')} FROM rb; DROP TABLE rb;`);
    db.pragma('foreign_keys = ON');
    createTables(db);
    ok(/\(status = 'closed'\) = \(close_reason IS NOT NULL\)/.test(db.prepare("SELECT sql FROM sqlite_master WHERE name = 'f_iroha_tasks'").get().sql), '前提: 作り直しが起きた');
    ok(TD.getTask(keep).external_ready === 1, '作り直しても「外部に出す準備OK」は残る');

    // 列はあるが制約が欠けている「途中の版」も作り直す。NULL が入っていても既定値に寄せて止まらない (Codex FB R4)
    {
      const sql2 = db.prepare("SELECT sql FROM sqlite_master WHERE name = 'f_iroha_tasks'").get().sql;
      const cols2 = db.prepare('PRAGMA table_info(f_iroha_tasks)').all().map((c) => c.name);
      const loose = sql2.replace('external_ready   INTEGER NOT NULL DEFAULT 0 CHECK (external_ready IN (0,1)),', 'external_ready INTEGER,')
        .replace('migration_review INTEGER NOT NULL DEFAULT 0 CHECK (migration_review IN (0,1)),', 'migration_review INTEGER,');
      ok(!/external_ready\s+INTEGER NOT NULL/.test(loose), '前提: external_ready の制約を落とした版を作る');
      db.pragma('foreign_keys = OFF');
      db.exec('CREATE TEMP TABLE rb2 AS SELECT * FROM f_iroha_tasks; DROP TABLE f_iroha_tasks;');
      db.exec(loose);
      db.exec(`INSERT INTO f_iroha_tasks (${cols2.join(', ')}) SELECT ${cols2.join(', ')} FROM rb2; DROP TABLE rb2;`);
      db.prepare('UPDATE f_iroha_tasks SET external_ready = NULL, migration_review = NULL WHERE id = ?').run(keep);
      db.pragma('foreign_keys = ON');
      ok(db.prepare('SELECT external_ready e FROM f_iroha_tasks WHERE id = ?').get(keep).e === null, '前提: 古いデータに NULL がある');
      createTables(db);
      const def = db.prepare('PRAGMA table_info(f_iroha_tasks)').all().find((c) => c.name === 'external_ready');
      ok(def.notnull === 1 && def.dflt_value === '0', '制約だけ欠けた版も作り直して NOT NULL・既定値が戻る');
      ok(/CHECK \(external_ready IN \(0,1\)\)/.test(db.prepare("SELECT sql FROM sqlite_master WHERE name = 'f_iroha_tasks'").get().sql), 'CHECK も戻る');
      ok(TD.getTask(keep).external_ready === 0 && TD.getTask(keep).migration_review === 0, 'NULL は既定値 (0) に寄せる (コピーが止まらない)');
      ok(db.prepare('SELECT COUNT(*) c FROM f_iroha_tasks').get().c > 0, '行は残っている');
    }
  }
}

setMetaValue('source_of_truth', null);   // [21]〜[23] で立てた正本を既定 (Notion) に戻す
console.log('\n[22] 作業画面の構造 (別画面から戻れる・クリックは委譲する)');
{
  const html = fs.readFileSync(new URL('../apps/iroha-work/views/index.html', import.meta.url), 'utf8');
  // ⭐#views (画面切替) が .page の中にあると、ボードへ移った瞬間に切替ボタンごと消えて戻れない (Codex PR-C R1 重大)
  const viewsAt = html.indexOf('<div class="views"');
  const firstPageAt = html.indexOf('<div class="page ');
  ok(viewsAt > 0 && firstPageAt > 0 && viewsAt < firstPageAt, '画面切替は最初の .page より前にある (どの画面でも出る)');
  const headerAt = html.indexOf('<header class="top">');
  ok(headerAt > 0 && headerAt < firstPageAt, '見出しも .page の外');
  ok(html.indexOf('id="errBanner"') < firstPageAt && html.indexOf('id="warnBanner"') < firstPageAt, 'エラー・注意のバナーも .page の外 (どの画面でも見える)');
  for (const id of ['listpage', 'boardpage', 'labelpage', 'historypage']) {
    ok(html.includes(`class="page ${id}"`), `${id} は .page (詳細を開くと隠れる)`);
  }
  // 値をインラインの onclick に埋めない (HTML エスケープは JS 文字列のエスケープではない)
  const inline = [...html.matchAll(/onclick="[^"]*(toggleBulk|openLw|openDetail|pickFacility|openSt)\(/g)].map((m) => m[0]);
  ok(inline.length === 0, 'カード・行・チップのクリックは委譲 (onclick に値を埋め込まない): ' + (inline[0] || 'なし'));
  ok(/data-fac=/.test(html) && /data-id=/.test(html) && /data-st=/.test(html), '値は data 属性で渡す');
  // 絞り込みを変えたときに、見えないカードが選ばれたまま残らない
  ok(/function syncBulkSelection/.test(html) && /renderBoard\(\)[\s\S]{0,400}syncBulkSelection/.test(html), 'ボードは描くたびに選択を見えているものへそろえる');
  ok(/function renderList\(\)[\s\S]{0,900}syncBulkSelection/.test(html), '一覧も同じ');
  // 画面を戻したときに描き直さないと、ボードで選んだ分が一覧の絞り込みと合わないまま残る (Codex PR-C R2)
  ok(/function setView\(v\)[\s\S]{0,700}if \(v === 'list'\) renderList\(\);/.test(html), '画面を切り替えたら、その画面を描き直す (一覧も)');
  // 一覧の後始末 (exitBulk) が curView のガードの中にないと、ボードで選んでいる最中に /api/state が返るだけで選択が消える
  const listBody = html.slice(html.indexOf('function renderList()'), html.indexOf('function renderList()') + 900);
  ok(/if \(curView === 'list'\) \{[\s\S]{0,200}exitBulk\(\)/.test(listBody), 'まとめて選択の解除は「一覧を見ているとき」だけ (ボードの選択を消さない)');
  // 選択のチェックボックスに tabindex="-1" を付けない (見出しに付けるのは、読み上げの位置を移すためなので別)
  ok(!/<input[^>]*class="cb"[^>]*tabindex="-1"/.test(html), 'チェックボックスはキーボードでも操作できる');
  // ダイアログ: 役割・見出しとの関連付け・背景固定・フォーカスの戻し (Codex R1)
  ok(/role="dialog" aria-modal="true" aria-labelledby="mvTitle"/.test(html), 'ダイアログとして扱われる (読み上げ)');
  ok(/body\.modal-open\{position:fixed/.test(html) && /document\.body\.classList\.add\('modal-open'\)/.test(html), '開いている間は背景を固定する');
  ok(/window\.scrollTo\(0, overlayScrollY\)/.test(html) && /overlayReturn\.focus\(\)/.test(html), '閉じたら元の位置とフォーカスに戻す');
  ok(/if \(e\.key !== 'Tab'\) return;/.test(html), 'Tab はダイアログの中で回る');
  ok(/aria-pressed=/.test(html) && /role="group" aria-label="資材の候補"/.test(html), 'どれを選んでいるかが支援技術にも伝わる');
  ok(/\.optnew\{[^}]*grid-column:1\/-1/.test(html), '新しく登録の欄は横いっぱいに出る');
  ok(/const targets = mvCtx\.field \? \[\[mvCtx\.field, MV_MAP\[mvCtx\.field\]\]\] : Object\.entries\(MV_MAP\)/.test(html), '送るのは開いた項目だけ');
  ok(!/\$\('#(mv|st|lw)Ov'\)\.classList\.(add|remove)\('on'\)/.test(html), 'ダイアログの開け閉めは openOverlay / closeOverlay に揃える (背景固定の解除漏れを作らない)');
  // 切替前の下見: 画面切替はいつでも出す。ボードは正本が Notion なら下見データを読む。書き変えの導線は出さない
  ok(/function renderViews\(\) \{ const el = \$\('#views'\); if \(el\) el\.hidden = false; \}/.test(html), '画面切替はいつでも出る (正本を問わない)');
  ok(/if \(v === 'board'\) \{ if \(isApp\(\)\) renderBoard\(\); else loadPreview\(\); \}/.test(html), 'ボードは正本が Notion なら下見を読み込む');
  ok(/const PREVIEW_NOTE = /.test(html) && /見るだけの下見です/.test(html), '下見であることを画面に書く');
  // 下見でもカードは開く (v1.3) — ただし state の openDetail には流さない (id が正本のカードと別)。1 枚だけ読むだけで取る
  ok(/if \(!isApp\(\)\) \{ openPreviewDetail\(el\.dataset\.id\); return; \}\r?\n  if \(el\.dataset\.pick === '1'\) toggleBulk\(el\.dataset\.id\); else openDetail\(el\.dataset\.id\);/.test(html),
    '下見のカードは state の openDetail に流さない (id が正本のカードと別)');
  ok(/const canBulk = stateCan\('tasks\.bulk_stocked'\) &&/.test(html), '下見では「まとめて棚入完了」を出さない (許可リストで判定)');
  ok(/<span id="lwSaveWrap"><\/span>/.test(html), 'ラベル待ちの「保存」は静的に置かない (許可されたときだけ描く — Codex PR1 R11)');
  ok(/const may = stateCan\('task\.label_wait\.edit'\);/.test(html) && /wrap\.innerHTML = may \?/.test(html),
    '下見ではラベル待ちの保存ボタンを描かない (無効化して見せるのではなく)');
  // 正本が変わったとき・つながらなかったときの追随 (Codex 下見 R1)
  ok(/if \(wasApp !== isApp\(\)\) \{[\s\S]{0,80}preview = null; previewAt = null; previewDown = false;/.test(html), '正本が変わったら下見のデータを捨てる (古い下見を新しく見せない)');
  ok(/if \(curView === 'board'\) \{ if \(isApp\(\)\) renderBoard\(\); else loadPreview\(\); \}/.test(html), '更新のたびに、Notion 正本なら下見も取り直す (502 から戻ったときもここで回復)');
  ok(/previewInflight = loadPreviewOnce\(\)/.test(html), '下見の取得は同時に 1 本だけ (遅れて返った古い応答で上書きしない)');
  ok(/previewDown = true;[\s\S]{0,300}if \(!preview\)/.test(html), 'つながらないときは前回の下見を消さない');
  ok(/previewAt = Date\.now\(\);/.test(html) && /この画面を取ったのは/.test(html), 'いつ取った下見かを画面に出す');
  ok(/function renderLwSaveState/.test(html) && /renderLwSaveState\(\);/.test(html), 'ラベル待ちを開いたままでも保存ボタンが正本に追随する');
  ok(/if \(!stateCan\('task\.label_wait\.edit'\)\) \{ \$\('#lwMsg'\)\.textContent = '下見なので保存できません/.test(html), '保存の入口でも下見なら止める');
  ok(/btn\.disabled = !stateCan\('task\.label_wait\.edit'\);   \/\/ 保存中に許可が変わっていたら/.test(html),
    '保存後にボタンを無条件で戻さない (許可リストで判断する。正本だけを見ない)');
  // 実機FB (2026-09-03): ボードに写真・項目タップで変更・想定作業時間の合計
  ok(/<div class="th">' \+ thumbHtml\(c\) \+ '<\/div>/.test(html), 'ボードのカードに写真を出す');
  ok(!/onclick="openMaster/.test(html) && /data-reg="/.test(html), '作業のやり方は項目タップで変更 (編集ボタンなし)');
  ok(/\+ \(empty \? '＋ 登録' : '✎ 変更'\) \+/.test(html), '値があれば「変更」、無ければ「登録」と出す');
  ok(!/mvVideo/.test(html.replace(/\/\/.*$/gm, '')), '作り方どうがは画面から外した (コメントだけ残す)');
  ok(/const hours = mine\.reduce/.test(html), 'ボードの列に想定作業時間の合計を出す');
  ok(/kv\('必要保管箱', c\.boxes\)/.test(html) && /kv\('想定作業時間'/.test(html), '詳細に必要保管箱と想定作業時間を出す');
  ok(/reg: 'units_per_container'/.test(html) && /reg: 'storage_container'/.test(html), '保管箱と入数は別々にタップできる');
  // タップした項目だけ出す・ダイアログはスクロールできる・候補は正方形のタイル (中原さん 2026-09-03)
  ok(/class="mvf" data-f="material_code"/.test(html) && /class="mvf" data-f="storage_container"/.test(html)
    && /class="mvf" data-f="units_per_container"/.test(html) && /class="mvf" data-f="note"/.test(html), '登録・変更の項目はひとつずつ枠に入っている');
  ok(/el\.hidden = !!mvCtx\.field && el\.dataset\.f !== mvCtx\.field/.test(html), 'タップした項目だけ出す (他は隠す)');
  ok(/\$\('#mvTitle'\)\.textContent = mvCtx\.field/.test(html), '見出しにその項目の名前を出す');
  ok(/\.overlay\{[^}]*overflow-y:auto/.test(html), 'ダイアログが画面に収まらないときスクロールできる');
  ok(/\.opts\{display:grid[^}]*minmax\(104px/.test(html) && /aspect-ratio:1\/1/.test(html), '候補は正方形のタイル');
  ok(/<span class="thumb">' \+ \(img \?/.test(html), 'タイルに画像の枠がある (画像が無ければ絵文字)');
  ok(/\.opts\.chips\{display:flex/.test(html) && /<div class="opts chips" id="lwLoc">/.test(html),
    'ラベル待ちのロケーション (Z/Y/なし) はタイルにしない');
  ok(/onerror="optImgFail\(this\)"/.test(html) && /function optImgFail\(el\)/.test(html),
    '画像が読めなくてもタイルが空にならない');
  ok(/const keepField = mvCtx\.field;/.test(html) && /values: cur2, field: keepField/.test(html),
    '競合で読み直しても「開いた項目だけ送る」は変わらない');
  ok(/if \(!ov\.contains\(document\.activeElement\)\) \{ e\.preventDefault\(\)/.test(html),
    'フォーカスが背景に残っていても Tab はダイアログへ戻る');
  ok(/Zロケ在庫 \(一時保管\)/.test(html) && /Yロケ在庫 \(外に出している分\)/.test(html) && /時点/.test(html),
    '詳細に在庫と取得時刻を出す (拠点によって Z か Y)');
  // 中断の理由は「〜で中断」。ラベル待ちならそのまま記録をつける (中原さん 2026-09-03)
  ok(/r\.label \+ '\u3067\u4e2d\u65ad'/.test(html), '中断の理由は「ラベル待ちで中断」「資材不足で中断」と出す');
  ok(/const order = \['label_shortage', 'materials_shortage'\]/.test(html), 'よく使う 2 つを先に出す');
  ok(/holdReason === 'label_shortage'\) setTimeout\(\(\) => openLwNew\(c\)/.test(html), 'ラベル待ちで中断したら、そのままラベル待ちの記録を開く');
  ok(/occurred_on: state\.today, qty: c\.qty, location: 'Z'/.test(html), '発生日・数量・ロケーションを入れた状態で開く (打つ手間を減らす)');
  ok(!/lot_expiry: c\.expiry/.test(html) && /\$\('#lwLot'\); if \(el\) el\.focus\(\)/.test(html), 'ロット・期限は入れずに、そこへカーソルを置く (現物を見て打つ)');
  ok(/外部に出す準備OK にする/.test(html) && /async function setExternalReady/.test(html), '詳細に「外部に出す準備OK」の切り替えがある');
  ok(/c\.external_ready \? '📦 外部に出せます'/.test(html) && /tag ready/.test(html), 'ボードと一覧に「外部に出せます」の印が出る');

  // ⭐下見・履歴の詳細を読むだけで開く (要件 v1.3 §P Q5 / PR1): 許可リスト (capabilities) に無い操作は描かない (default-deny)
  ok(/const can = \(name\) => detailCaps\.includes\(name\)/.test(html), '許された操作は detailCaps (サーバーの capabilities) で判定する');
  ok(/if \(!isApp\(\)\) \{ openPreviewDetail\(el\.dataset\.id\); return; \}/.test(html), 'ボードのカードは下見でも開く (読むだけ)');
  ok(!/下見なので開けません/.test(html), '「下見なので開けません」は無くなった');
  ok(/openPreviewDetail\(tr\.dataset\.id\)/.test(html), '履歴の行からも読むだけで開く (終了したカードは一覧に無い)');
  ok(/apiFetch\('\/api\/task-previews\/' \+ encodeURIComponent\(id\)\)/.test(html), '詳細は 1 枚だけサーバーから取る');
  ok(/showDetail\(fallback, 'preview', \[\], \{ stale: true, fetchedAt: null \}\)/.test(html), '取れなければボードに載っていた中身で開き、最新でないと出す');
  ok(/if \(j\.error === 'not_found'\)/.test(html) && /このカードはもうありません/.test(html), '消えたカードは閉じる (404)');
  ok(/<span id="dstateWrap"><\/span>/.test(html) && !/<button class="st todo" id="dstate"/.test(html), '状態のボタンは静的に置かない (許されたときだけ描く)');
  ok(/can\('task\.status\.change'\)\s*\?\s*'<button class="st /.test(html) && /'<span class="st ro /.test(html), '許されなければ状態は札 (span) で出す');
  ok(/if \(!reg \|\| !can\('task\.master\.edit'\)\) return/.test(html), '作業のやり方の「変更・登録」は許されたときだけ data-reg を付ける');
  ok(/const addP = can\('task\.media\.add'\) && photos < 3/.test(html)
    && /const photos = media\.filter\(m => m\.kind === 'photo'\)\.length \+ pending\.filter\(p => p\.kind === 'photo'\)\.length;/.test(html),
    '「写真をとる」の枠は許されたときだけ。送信中の分も枚数に数える (Codex PR1 R13)');
  ok(/own && can\('task\.media\.add'\)/.test(html) && /\(canPhoto \? pend : ''\)/.test(html), '写真の × と送信中の枠も許されたときだけ');
  ok(/if \(!can\('task\.work\.start'\)\) \{/.test(html), '「作業をはじめる」「中断」「できあがり」は許されたときだけ');
  ok(/can\('task\.plan\.assign'\) && \(c\.status === 'not_started'/.test(html) && /can\('task\.external_ready'\) && c\.status !== 'closed'/.test(html)
    && /can\('task\.label_wait\.edit'\) && c\.status !== 'closed'/.test(html), '今日やる・外部準備OK・ラベル待ち登録も許可リストで出し分ける');
  ok(/function detailNoteHtml\(c\)/.test(html) && /に取り込んだ時点/.test(html) && /いまのアプリの記録/.test(html), '下見の詳細には「何がいつ時点か」を分けて出す');
  ok(/if \(!can\('task\.master\.edit'\)\) return;/.test(html) && /if \(curDetail && can\('task\.status\.change'\)\) openSt/.test(html)
    && /if \(!can\('task\.work\.start'\)\) return;/.test(html) && /if \(!can\('task\.media\.add'\)\) return;/.test(html), '変更・開始・撮影の入口も許可リストで止める (二重の守り)');
  ok(/capabilities: j\.capabilities/.test(html) && /capabilities: \[\], me: \{\} \};/.test(html),
    '端末に残した前回の一覧からは許可リストを復元しない (つながるまでは何も許さない — Codex PR1 R12)');
  // Codex R1 の指摘 (下見・履歴の読み取り専用境界)
  ok(/const stateCan = \(name\) => \(state\.capabilities \|\| \[\]\)\.includes\(name\)/.test(html),
    '一覧・ボード側も許可リストで判定する (前回の一覧に許可が無ければ何も許さない)');
  ok(/stateCan\('task\.status\.change'\)\r?\n\s+\? '<button class="st /.test(html) && /'<span class="st ro /.test(html),
    '一覧の状態は、変更が許されたときだけタップできる札 (data-st) にする');
  ok(/function openSt\(ev, id\) \{[\s\S]{0,120}if \(!stateCan\('task\.status\.change'\)\) return;/.test(html)
    && /function startBulk\(\) \{\r?\n  if \(!stateCan\('tasks\.bulk_stocked'\)\) return;/.test(html),
    'ステータス変更・まとめて棚入完了は入口でも許可リストで止める (二重の守り)');
  ok(/else forceCloseDetail\('このカードは一覧から外れました'\)/.test(html)
    && /if \(curDetail\) forceCloseDetail\('正本が変わったので詳細を閉じました'\)/.test(html),
    'カードが一覧から消えた・正本が変わったら、開いている詳細とダイアログを閉じる (古いボタンを残さない)');
  // 保存中でも「もう触れない」ようにし、通信が終わったら必ず閉じる (Codex PR1 R2)
  ok(/function forceCloseDetail\(msg\) \{[\s\S]{0,200}detailCaps = \[\];/.test(html), '閉じるときは許可リストを空にする (失敗応答でボタンが戻らない)');
  ok(/pendingForceClose = msg/.test(html) && /function settleForceClose/.test(html)
    && (html.match(/settleForceClose\(\);/g) || []).length >= 2, '保存中は閉じるのを予約し、通信が終わったら閉じる (作業のやり方・状態変更の両方)');
  // 遅れて返った詳細の応答で、別のカードや閉じた画面を開かない
  ok(/let detailGen = 0;/.test(html) && /const gen = \+\+detailGen;/.test(html)
    && /if \(gen !== detailGen \|\| isApp\(\) !== wasApp\) return;/.test(html), '詳細の取得に世代を持たせ、遅れた応答は捨てる');
  // 下見・履歴の詳細も巡回で取り直す
  ok(/if \(curDetail && detailSrc === 'preview'\) \{\s*\r?\n\s*openPreviewDetail\(curDetail, \{ silent: true \}\);/.test(html),
    '下見・履歴の詳細も 60 秒ごとに取り直す (「いまの記録」と書いている以上、古いまま置かない)');
  // 入口の守り (許可リストが無ければ関数の中で止まる)
  for (const [fn, cap] of [['doBulkStocked', "stateCan\\('tasks\\.bulk_stocked'\\)"], ['saveMaster', "can\\('task\\.master\\.edit'\\)"],
    ['setExternalReady', "can\\('task\\.external_ready'\\)"], ['setPlanned', "can\\('task\\.plan\\.assign'\\)"],
    ['stopWork', "can\\('task\\.work\\.start'\\)"], ['doSetSt', "stateCan\\('task\\.status\\.change'\\)"]]) {
    const body = html.slice(html.indexOf('function ' + fn + '('), html.indexOf('function ' + fn + '(') + 400);
    if (!new RegExp(cap).test(body)) ok(false, fn + ' の入口で許可リストを見ている');
  }
  ok(/function retryUpload\(ev, opId\) \{ ev\.stopPropagation\(\); if \(!can\('task\.media\.add'\)\) return;/.test(html)
    && /async function delMedia[\s\S]{0,160}if \(!can\('task\.media\.add'\)\) return;/.test(html), '写真の再送・削除も入口で止める');
  ok(/function dropPending\(ev, opId\)/.test(html) && /pendingTiles\.delete\(opId\);/.test(html),
    '送れなかった写真は「やめる」で捨てられる (枠を空けて撮り直せる — Codex PR1 R14)');
  ok(true, '書き込みの関数はすべて入口で許可リストを見る');
  ok(/function historyCardHtml\(c\)/.test(html) && /これまでの作業 — このカード/.test(html)
    && /const END_REASON = \{ done: 'できあがり', pause: '中断', admin: '職員が終了' \}/.test(html),
    '詳細に「このカードの終わった作業」(誰が・いつ・何分・理由) を読むだけで出す');
  ok(/if \(curDetail && detailSrc === 'state'\)/.test(html), '一覧の再取得で下見の詳細を上書きしない');
  ok(/detailCard \? \[detailCard, \.\.\.state\.cards\] : state\.cards/.test(html), '写真を大きく見るときは開いている詳細のカードから探す (下見は一覧に無い)');
  const sw = fs.readFileSync(new URL('../apps/iroha-work/views/sw.js', import.meta.url), 'utf8');
  ok(/const CACHE = 'iroha-work-shell-v2'/.test(sw), '画面キャッシュの版を上げる (古い画面が残らない)');
}

console.log('\n[23] 画面に許す操作 (capabilities) — 正本ごとの許可リスト');
{
  const { capabilitiesFor, CAP } = await import('../apps/iroha-work/capabilities.js');
  const app = capabilitiesFor('app'), notion = capabilitiesFor('notion'), pv = capabilitiesFor('preview');
  ok(Array.isArray(pv) && pv.length === 0, '下見・履歴は何も許さない');
  ok([CAP.STATUS_CHANGE, CAP.WORK_START, CAP.MEDIA_ADD, CAP.MASTER_EDIT].every(c => notion.includes(c))
    && ![CAP.PLAN_ASSIGN, CAP.FACILITY_ASSIGN, CAP.EXTERNAL_READY, CAP.CANCELLATION, CAP.REVIEW_CLEAR, CAP.LABEL_WAIT_EDIT, CAP.BULK_STOCKED].some(c => notion.includes(c)),
    'Notion 正本 = 状態変更・作業開始・写真・作業のやり方 (今日やる等は許さない)');
  ok(notion.every(c => app.includes(c)) && [CAP.EXTERNAL_READY, CAP.CANCELLATION, CAP.REVIEW_CLEAR, CAP.LABEL_WAIT_EDIT, CAP.BULK_STOCKED].every(c => app.includes(c)),
    'アプリ正本 = Notion 正本の全部 + 外部準備OK・取消の判断・確認ずみ・ラベル待ち・まとめて棚入完了');
  // ⭐計画 (いつ / どこが) は職員のときだけ (要件 §W-1)
  const staffCaps = capabilitiesFor('app', { staff: true });
  ok(!app.includes(CAP.PLAN_ASSIGN) && !app.includes(CAP.FACILITY_ASSIGN), '利用者には「いつ」「どこが」を許さない');
  ok(staffCaps.includes(CAP.PLAN_ASSIGN) && staffCaps.includes(CAP.FACILITY_ASSIGN), '職員には「いつ」「どこが」を許す');
  ok(app.every((c) => staffCaps.includes(c)), '職員は利用者にできることを全部できる');
  ok(capabilitiesFor('notion', { staff: true }).length === notion.length, 'Notion 正本では職員でも計画は許さない (planned_date はアプリ正本の持ちもの)');
  ok(capabilitiesFor('preview', { staff: true }).length === 0, '下見・履歴は職員でも何も許さない');
  // 書き込み口が capability を持たないまま増えていないか (Codex PR1 R6)
  ok(app.length === 9 && new Set(app).size === app.length, 'アプリ正本 (利用者) の許可は 9 個・重複なし (増やしたら画面の判定も足す)');
  ok(staffCaps.length === 11 && new Set(staffCaps).size === staffCaps.length, 'アプリ正本 (職員) の許可は 11 個・重複なし');
  app.push('x'); pv.push('y');
  ok(!capabilitiesFor('app').includes('x') && capabilitiesFor('preview').length === 0, '返した配列を壊しても共有の定義は変わらない');
  ok(capabilitiesFor('unknown').length === 0 && capabilitiesFor(undefined).length === 0, '知らないモードは何も許さない (default-deny)');
}

console.log(`\n結果: ${pass} PASS / ${fail} FAIL`);
process.exit(fail > 0 ? 1 : 0);
