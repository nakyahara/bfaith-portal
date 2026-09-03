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
  ok(a1c.already === true && !a1c.deleteToken, '別端末からの同 operation_id にはトークンを返さない');
  ok(addMedia({ pageId: pMedia.id, kind: 'photo', filePath: tmp('bad.jpg', mp4), worker: worker1, operationId: 'op-bad-000001' }).error === 'bad_file',
    '中身が写真でなければ拒否 (Content-Type を信じない)');
  addMedia({ pageId: pMedia.id, productCode: 'PROD-M', kind: 'photo', filePath: tmp('a2.jpg', jpeg), worker: worker1, operationId: 'op-photo-0002' });
  const a3 = addMedia({ pageId: pMedia.id, productCode: 'PROD-M', kind: 'photo', filePath: tmp('a3.jpg', jpeg), worker: worker2, deviceId: 11, operationId: 'op-photo-0003' });
  ok(a1.deleteToken && a3.deleteToken && a1.deleteToken !== a3.deleteToken, '削除トークンは行ごとに別');
  ok(addMedia({ pageId: pMedia.id, kind: 'photo', filePath: tmp('a4.jpg', jpeg), worker: worker1, operationId: 'op-photo-0004' }).error === 'cap_reached',
    '写真は3枚まで');
  const v1 = addMedia({ pageId: pMedia.id, kind: 'video', mime: 'video/mp4', filePath: tmp('v1.mp4', mp4), worker: worker1, operationId: 'op-video-0001' });
  ok(v1.ok === true, '動画も受信できる');
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
  ok(driveFiles.size === 4, `Drive のファイルは4つだけ (再試行で二重作成しない。実際 ${driveFiles.size})`);
  ok(rows.find(m => m.operation_id === 'op-photo-0001').drive_file_id === 'f-op-photo-0001',
    '応答消失した1件も同じファイルを回収して紐づく');
  ok(!fs.existsSync(path.join(MEDIA_DIR, 'op-photo-0001.jpg')), 'アップロード後に実体を削除');
  const patched = mock.patched.filter(p => p.id === pMedia.id && p.body.properties?.['完成写真']);
  ok(patched.length > 0 && patched[patched.length - 1].body.properties['完成写真'].files.length === 4,
    'Notion「完成写真」に4件 (写真3+動画1) が付く');

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
  ok(last.body.properties['完成写真'].files.length === 3, 'Notion 側も貼り直されて3件 (写真2+動画1)');
  ok((mediaByPage().get(pMedia.id) || []).length === 3, '画面用一覧にも削除分は出ない');

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
  const planned = TD.setPlannedDate({ taskId: tA2.id, plannedDate: '2026-09-04', expectVersion: reopened.task.version });
  ok(planned.ok && planned.task.planned_date === '2026-09-04' && TD.setPlannedDate({ taskId: tA2.id, plannedDate: '9/4', expectVersion: planned.task.version }).error === 'bad_request', '「今日やる」= planned_date (形式検証)');

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
      INSERT INTO f_iroha_tasks SELECT * FROM f_iroha_tasks__old;
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
  const { addMedia, mediaByTask, _setDriveUpload, processMediaQueue } = await import('../apps/iroha-work/media.js');
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

  // 古い版 (page_id NOT NULL) の作業時間・写真は起動時に作り直す
  {
    const { createTables } = await import('../apps/iroha-work/db.js');
    const nS = db.prepare('SELECT COUNT(*) c FROM f_iroha_work_sessions').get().c;
    const nM = db.prepare('SELECT COUNT(*) c FROM f_iroha_card_media').get().c;
    db.pragma('foreign_keys = OFF');
    db.exec(`CREATE TABLE s__old AS SELECT * FROM f_iroha_work_sessions; DROP TABLE f_iroha_work_sessions;
      CREATE TABLE f_iroha_work_sessions (id INTEGER PRIMARY KEY AUTOINCREMENT, page_id TEXT NOT NULL, product_code TEXT, title_snapshot TEXT,
        worker_id INTEGER NOT NULL, worker_name TEXT NOT NULL, device_label TEXT, started_at TEXT NOT NULL, ended_at TEXT, end_reason TEXT,
        raw_seconds INTEGER, voided_at TEXT, voided_by TEXT, void_reason TEXT, master_snapshot TEXT, task_id INTEGER);
      INSERT INTO f_iroha_work_sessions (id, page_id, product_code, title_snapshot, worker_id, worker_name, device_label, started_at, ended_at,
        end_reason, raw_seconds, voided_at, voided_by, void_reason, master_snapshot, task_id)
        SELECT id, COALESCE(page_id, 'legacy-' || id), product_code, title_snapshot, worker_id, worker_name, device_label, started_at, ended_at,
        end_reason, raw_seconds, voided_at, voided_by, void_reason, master_snapshot, task_id FROM s__old;
      DROP TABLE s__old;`);
    db.pragma('foreign_keys = ON');
    ok(db.prepare('PRAGMA table_info(f_iroha_work_sessions)').all().some(c => c.name === 'page_id' && c.notnull === 1), '前提: 古い版は page_id NOT NULL');
    createTables(db);
    ok(!db.prepare('PRAGMA table_info(f_iroha_work_sessions)').all().some(c => c.name === 'page_id' && c.notnull === 1), 'page_id を NULL 可に作り直す');
    ok(db.prepare('SELECT COUNT(*) c FROM f_iroha_work_sessions').get().c === nS && db.prepare('SELECT COUNT(*) c FROM f_iroha_card_media').get().c === nM, '行はそのまま残る');
    ok(db.prepare("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_iroha_sessions_task'").get(), '索引も作り直される');
    let bothNull = null;
    try { db.prepare("INSERT INTO f_iroha_work_sessions (worker_id, worker_name, started_at) VALUES (1, 'x', 'y')").run(); } catch (e) { bothNull = e; }
    ok(bothNull && /CHECK/.test(bothNull.message), 'page_id も task_id も無い行は入らない (CHECK)');
    createTables(db);
    ok(db.prepare("SELECT COUNT(*) c FROM sqlite_master WHERE name LIKE 'f_iroha_work_sessions%'").get().c === 1, '2 回目は何もしない (冪等)');
  }
}

console.log(`\n結果: ${pass} PASS / ${fail} FAIL`);
process.exit(fail > 0 ? 1 : 0);
