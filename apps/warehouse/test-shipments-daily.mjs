/**
 * test-shipments-daily.mjs — 日次出荷サマリ (出荷日 × モール × 配送方法) の smoke テスト
 *
 * 対象:
 *   ne-order-base-upsert.js  … NE 受注ベース → raw_ne_order_base の変換 / UPSERT
 *   rebuild-shipments-daily.js … raw_ne_order_base → f_shipments_daily の集計
 *   backfill-ne-order-base.js  … 月分割ロジック
 *
 * 実行: node apps/warehouse/test-shipments-daily.mjs
 * 本番 DB には触れない (一時 DATA_DIR に専用 warehouse.db を作り、終了時に削除)。
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

// ★ db.js は import 時に DATA_DIR を読むため、動的 import より前に設定する
const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'shipments-daily-test-'));
process.env.DATA_DIR = tmpDir;

const { initDB, getDB } = await import('./db.js');
const { toOrderBaseRow, makeNeOrderBaseUpserter } = await import('./ne-order-base-upsert.js');
const { rebuildShipmentsDaily } = await import('./rebuild-shipments-daily.js');
const { splitMonths, validateRange } = await import('./backfill-ne-order-base.js');
const { splitWindow } = await import('./ne-api.js');

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };
const eq = (a, b, label) => ok(JSON.stringify(a) === JSON.stringify(b), `${label} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

await initDB();
const db = getDB();
const upsert = makeNeOrderBaseUpserter(db);

// ───────────────────────── 1. toOrderBaseRow ─────────────────────────
console.log('\n── toOrderBaseRow (NE レコード → 行) ──');
{
  const item = {
    receive_order_id: '1514960',
    receive_order_shop_cut_form_id: '249-7640680-6223045',
    receive_order_shop_id: '4',
    receive_order_date: '2026-08-02 08:22:41',
    receive_order_send_date: '2026-08-04 16:14:09',
    receive_order_order_status_id: '50',
    receive_order_order_status_name: '出荷確定済（完了）',
    receive_order_cancel_type_id: '0',
    receive_order_cancel_date: '',
    receive_order_delivery_id: '71',
    receive_order_delivery_name: 'AES',
    receive_order_delivery_cut_form_id: '766281505894',
  };
  const row = toOrderBaseRow(item);
  eq(row.伝票番号, '1514960', '伝票番号');
  eq(row.配送方法名, 'AES', '配送方法名 = AES (Amazon Easy Ship)');
  eq(row.配送方法ID, '71', '配送方法ID');
  eq(row.送り状番号, '766281505894', '送り状番号');
  // cancel_type_id='0' は「キャンセルではない」。'0' は JS で truthy なので素の判定だと全件誤ラベル
  eq(row.キャンセル区分, '有効', "cancel_type_id='0' は有効");

  eq(toOrderBaseRow({ ...item, receive_order_cancel_type_id: '1' }).キャンセル区分, 'キャンセル', "cancel_type_id='1' はキャンセル");
  eq(toOrderBaseRow({ ...item, receive_order_delivery_name: undefined }).配送方法名, '', '配送方法名なし → 空文字');
  ok(toOrderBaseRow({ receive_order_id: '' }) === null, '伝票番号なし → null');
}

// ───────────────────────── 2. UPSERT ─────────────────────────
console.log('\n── makeNeOrderBaseUpserter (状態更新つき UPSERT) ──');
const mkRow = (o) => ({
  伝票番号: '1000', 受注番号: 'R-1', 店舗コード: '1', 受注日: '2026-08-01 10:00:00',
  出荷確定日: '', 受注状態区分: '20', 受注状態: '引当待ち', キャンセル区分: '有効',
  受注キャンセル日: '', 配送方法ID: '28', 配送方法名: 'ヤマト(ネコポス)', 送り状番号: '',
  ...o,
});
{
  eq(upsert(mkRow({}), 'T1'), 'inserted', '新規 → inserted');
  eq(upsert(mkRow({}), 'T2'), 'unchanged', '同一内容の再投入 → unchanged (WAL を無駄に太らせない)');

  // 出荷確定日が後から付く = 出荷件数が初めて数えられるようになる瞬間
  eq(upsert(mkRow({ 出荷確定日: '2026-08-02 15:00:00', 受注状態区分: '50', 受注状態: '出荷確定済（完了）' }), 'T3'),
    'updated', '出荷確定日が付いた → updated');
  eq(db.prepare('SELECT 出荷確定日 FROM raw_ne_order_base WHERE 伝票番号=?').get('1000').出荷確定日,
    '2026-08-02 15:00:00', '出荷確定日が保存されている');

  // 現場で配送方法を変えたケース
  eq(upsert(mkRow({ 出荷確定日: '2026-08-02 15:00:00', 受注状態区分: '50', 受注状態: '出荷確定済（完了）', 配送方法ID: '20', 配送方法名: 'ヤマト(発払い)B2v6' }), 'T4'),
    'updated', '配送方法の変更 → updated');
  eq(db.prepare('SELECT 配送方法名 FROM raw_ne_order_base WHERE 伝票番号=?').get('1000').配送方法名,
    'ヤマト(発払い)B2v6', '変更後の配送方法が保存されている');

  // 識別系は初回スナップショットを保持する。ただし店舗コードは集計キーなので追随する
  upsert(mkRow({ 受注日: '1999-01-01 00:00:00', 店舗コード: '9', 受注番号: 'R-CHANGED', 出荷確定日: '2026-08-02 15:00:00', 受注状態区分: '50', 受注状態: '出荷確定済（完了）', 配送方法ID: '20', 配送方法名: 'ヤマト(発払い)B2v6' }), 'T5');
  const keep = db.prepare('SELECT 受注日, 店舗コード, 受注番号 FROM raw_ne_order_base WHERE 伝票番号=?').get('1000');
  eq([keep.受注日, keep.受注番号], ['2026-08-01 10:00:00', 'R-1'], '識別系 (受注日/受注番号) は初回値を保持');
  eq(keep.店舗コード, '9', '店舗コードは更新される (集計キーなので誤分類が自己修復しないと困る)');

  // 出荷確定が取り消された = 出荷確定日が空に戻るケース (更新日ベース取得で拾う)
  eq(upsert(mkRow({ 伝票番号: '1000', 店舗コード: '9', 出荷確定日: '', 受注状態区分: '20', 受注状態: '引当待ち', 配送方法ID: '20', 配送方法名: 'ヤマト(発払い)B2v6' }), 'T6'),
    'updated', '出荷確定の取り消し → updated');
  eq(db.prepare('SELECT 出荷確定日 FROM raw_ne_order_base WHERE 伝票番号=?').get('1000').出荷確定日, '',
    '出荷確定日が空に戻る (件数から自動的に外れる)');
}

// ───────────────────────── 3. 集計 ─────────────────────────
console.log('\n── rebuildShipmentsDaily (出荷日 × 店舗 × 配送方法) ──');
db.prepare('DELETE FROM raw_ne_order_base').run();
{
  const rows = [
    // 8/4 Amazon: AES 2件 + ネコポス 1件
    mkRow({ 伝票番号: 'A1', 店舗コード: '4', 出荷確定日: '2026-08-04 09:00:00', 配送方法ID: '71', 配送方法名: 'AES' }),
    mkRow({ 伝票番号: 'A2', 店舗コード: '4', 出荷確定日: '2026-08-04 18:30:00', 配送方法ID: '71', 配送方法名: 'AES' }),
    mkRow({ 伝票番号: 'A3', 店舗コード: '4', 出荷確定日: '2026-08-04 10:00:00', 配送方法ID: '28', 配送方法名: 'ヤマト(ネコポス)' }),
    // 8/4 楽天: ネコポス 1件 + キャンセル 1件
    mkRow({ 伝票番号: 'R1', 店舗コード: '1', 出荷確定日: '2026-08-04 11:00:00', 配送方法ID: '28', 配送方法名: 'ヤマト(ネコポス)' }),
    mkRow({ 伝票番号: 'R2', 店舗コード: '1', 出荷確定日: '2026-08-04 11:30:00', 配送方法ID: '28', 配送方法名: 'ヤマト(ネコポス)', キャンセル区分: 'キャンセル' }),
    // 8/5 Amazon AES 1件
    mkRow({ 伝票番号: 'A4', 店舗コード: '4', 出荷確定日: '2026-08-05 08:00:00', 配送方法ID: '71', 配送方法名: 'AES' }),
    // 未出荷 (出荷確定日なし) → 集計対象外
    mkRow({ 伝票番号: 'N1', 店舗コード: '4', 出荷確定日: '', 配送方法ID: '71', 配送方法名: 'AES' }),
    // 配送方法が未設定のまま出荷確定したケース
    mkRow({ 伝票番号: 'U1', 店舗コード: '2', 出荷確定日: '2026-08-04 12:00:00', 配送方法ID: '', 配送方法名: '' }),
  ];
  for (const r of rows) upsert(r, 'T');

  const r1 = rebuildShipmentsDaily(db, { from: '2026-08-01', to: '2026-08-31' });
  // slips = 出荷確定した伝票すべて (キャンセルは内数)。未出荷だけが対象外
  eq(r1.slips, 7, '出荷確定伝票の合計 (全8件 − 未出荷1件 = 7件。キャンセルも発送1件として数える)');

  const get = (d, shop, dv) => db.prepare(
    'SELECT slips, cancelled_slips, delivery_name FROM f_shipments_daily WHERE ship_date=? AND shop_code=? AND delivery_id=?'
  ).get(d, shop, dv);

  eq(get('2026-08-04', '4', '71').slips, 2, '8/4 Amazon × AES = 2件');
  eq(get('2026-08-04', '4', '28').slips, 1, '8/4 Amazon × ネコポス = 1件 (Amazon でも AES 以外がある)');
  eq(get('2026-08-05', '4', '71').slips, 1, '8/5 Amazon × AES = 1件');
  eq([get('2026-08-04', '1', '28').slips, get('2026-08-04', '1', '28').cancelled_slips], [2, 1],
    '8/4 楽天 × ネコポス = 出荷2件・うちキャンセル1件 (cancelled_slips は slips の内数)');
  eq(get('2026-08-04', '2', '').delivery_name, '(未設定)', '配送方法なしは (未設定) で残す');
  ok(db.prepare("SELECT COUNT(*) n FROM f_shipments_daily WHERE ship_date='2026-08-06'").get().n === 0,
    '出荷確定日が無い伝票は集計されない');

  // 冪等性: 2回流しても同じ
  const before = db.prepare('SELECT ship_date, shop_code, delivery_id, slips FROM f_shipments_daily ORDER BY 1,2,3').all();
  rebuildShipmentsDaily(db, { from: '2026-08-01', to: '2026-08-31' });
  const after = db.prepare('SELECT ship_date, shop_code, delivery_id, slips FROM f_shipments_daily ORDER BY 1,2,3').all();
  eq(after, before, '同じ期間を再構築しても結果が変わらない (冪等)');

  // 窓の外を消さない
  db.prepare("INSERT INTO f_shipments_daily (ship_date, shop_code, delivery_id, delivery_name, slips, cancelled_slips, updated_at) VALUES ('2026-07-01','1','28','ヤマト(ネコポス)',99,0,'X')").run();
  rebuildShipmentsDaily(db, { from: '2026-08-01', to: '2026-08-31' });
  eq(db.prepare("SELECT slips FROM f_shipments_daily WHERE ship_date='2026-07-01'").get().slips, 99,
    '期間外の既存行は消えない (増分再構築)');

  // --all は全期間を作り直す (期間外に置いた偽行は実データから作り直されて消える)
  rebuildShipmentsDaily(db, { all: true });
  ok(db.prepare("SELECT COUNT(*) n FROM f_shipments_daily WHERE ship_date='2026-07-01'").get().n === 0,
    '--all は全期間を実データから作り直す');
}

// ─────────────── 3b. 集計の追加ケース (Codex R1 対応) ───────────────
console.log('\n── 集計: 配送方法名の揺れ / 出荷取消 / 日付境界 ──');
db.prepare('DELETE FROM raw_ne_order_base').run();
db.prepare('DELETE FROM f_shipments_daily').run();
{
  // 同じ配送方法ID で名称が変わった (NE マスタの改名)。新しい方の名前を採用する
  upsert(mkRow({ 伝票番号: 'M1', 店舗コード: '1', 出荷確定日: '2026-07-01 10:00:00', 配送方法ID: '28', 配送方法名: 'ヤマト(ネコポス)' }), 'T');
  upsert(mkRow({ 伝票番号: 'M2', 店舗コード: '1', 出荷確定日: '2026-08-04 10:00:00', 配送方法ID: '28', 配送方法名: 'ネコポス(新名称)' }), 'T');
  rebuildShipmentsDaily(db, { all: true });
  const names = db.prepare("SELECT DISTINCT delivery_name FROM f_shipments_daily WHERE delivery_id='28'").all().map(r => r.delivery_name);
  eq(names, ['ネコポス(新名称)'], '同一IDで名称が変わったら最新の名称に揃う (辞書順MAXではない)');

  // 日付境界: 00:00:00 と 23:59:59 が正しく当日に入る
  db.prepare('DELETE FROM raw_ne_order_base').run();
  upsert(mkRow({ 伝票番号: 'B1', 店舗コード: '1', 出荷確定日: '2026-08-04 00:00:00', 配送方法ID: '28' }), 'T');
  upsert(mkRow({ 伝票番号: 'B2', 店舗コード: '1', 出荷確定日: '2026-08-04 23:59:59', 配送方法ID: '28' }), 'T');
  upsert(mkRow({ 伝票番号: 'B3', 店舗コード: '1', 出荷確定日: '2026-08-05 00:00:00', 配送方法ID: '28' }), 'T');
  rebuildShipmentsDaily(db, { from: '2026-08-04', to: '2026-08-04' });
  eq(db.prepare("SELECT slips FROM f_shipments_daily WHERE ship_date='2026-08-04'").get().slips, 2,
    '00:00:00 と 23:59:59 は当日に入り、翌日 00:00:00 は入らない');
  ok(db.prepare("SELECT COUNT(*) n FROM f_shipments_daily WHERE ship_date='2026-08-05'").get().n === 0,
    '窓の外 (翌日) は作られない');

  // 出荷確定が取り消されたら --all の再構築で件数から消える
  upsert(mkRow({ 伝票番号: 'B1', 店舗コード: '1', 出荷確定日: '', 配送方法ID: '28' }), 'T');
  rebuildShipmentsDaily(db, { all: true });
  eq(db.prepare("SELECT slips FROM f_shipments_daily WHERE ship_date='2026-08-04'").get().slips, 1,
    '出荷確定が取り消された伝票は再構築で件数から消える');
}

// ─────────────── 3e. 再構築時刻の記録 (Render 同期の鮮度判定に使う) ───────────────
console.log('\n── 再構築時刻を sync_meta に残す ──');
{
  const metaOf = (k) => db.prepare('SELECT value FROM sync_meta WHERE key = ?').get(k);
  db.prepare("DELETE FROM sync_meta WHERE key LIKE 'shipments_daily%'").run();

  rebuildShipmentsDaily(db, { all: true });
  const m = metaOf('shipments_daily_rebuilt_at');
  ok(!!m && !Number.isNaN(Date.parse(m.value)), '再構築のたびに shipments_daily_rebuilt_at を更新する');
  ok(Math.abs(Date.now() - Date.parse(m.value)) < 60000, '記録される時刻は現在時刻');
  eq(metaOf('shipments_daily_rebuilt_range').value, 'all', '--all の範囲が記録される');
  const full1 = metaOf('shipments_daily_full_rebuilt_at').value;
  ok(!!full1, '--all のときは shipments_daily_full_rebuilt_at も立つ');

  // 部分再構築では「全期間を作り直した」印を更新しない
  // (Render へは表の全行を送るので、部分再構築の時刻を根拠に送らない)
  rebuildShipmentsDaily(db, { from: '2026-08-01', to: '2026-08-31' });
  eq(metaOf('shipments_daily_rebuilt_range').value, '2026-08-01~2026-08-31', '部分再構築の範囲が記録される');
  eq(metaOf('shipments_daily_full_rebuilt_at').value, full1, '部分再構築では full_rebuilt_at が進まない');
}

// ─────────────── 3f. Render へ送ってよいかの鮮度判定 ───────────────
console.log('\n── isShipmentsRebuildFresh (古い表を Render に送らない) ──');
{
  const { isShipmentsRebuildFresh } = await import('./sync-to-render.js');
  // 2026-08-05 10:00 JST = 01:00 UTC に daily-sync が Render 同期まで来た想定
  const now = Date.parse('2026-08-05T01:00:00Z');
  const iso = (s) => new Date(s).toISOString();
  ok(isShipmentsRebuildFresh(iso('2026-08-04T22:20:00Z'), now), '同日 07:20 JST の再構築 → 送る');
  ok(!isShipmentsRebuildFresh(iso('2026-08-03T22:20:00Z'), now), '前日の再構築 → 送らない (当日分が失敗した日)');
  ok(!isShipmentsRebuildFresh(null, now), 'メタが無い (初回・失敗) → 送らない');
  ok(!isShipmentsRebuildFresh('not-a-date', now), '壊れた値 → 送らない');
  ok(!isShipmentsRebuildFresh('2026-08-05', now), '日付だけの値 → 送らない (保存形式と違う)');
  ok(!isShipmentsRebuildFresh('2026-08-04T22:20:00+09:00', now), 'ISO でもオフセット表記 → 送らない');
  ok(!isShipmentsRebuildFresh(iso('2026-08-04T22:20:00Z').replace('.000Z', 'Z'), now), 'ミリ秒なしの表記 → 送らない');
  ok(!isShipmentsRebuildFresh(iso('2026-08-05T02:00:00Z'), now), '未来の時刻 → 送らない');
  // JST 日付で判定する (UTC 日付だと 2026-08-04T22:20Z を「前日」と誤判定する)
  ok(isShipmentsRebuildFresh(iso('2026-08-04T15:10:00Z'), Date.parse('2026-08-04T23:00:00Z')),
    'JST 8/5 00:10 の再構築は JST 8/5 08:00 時点で有効 (UTC 日付では別日)');
}

// ─────────────── 3c. 取得区間の二分割 ───────────────
console.log('\n── splitWindow (1レスポンスに収まらない区間の二分割) ──');
{
  eq(splitWindow('2026-08-01 00:00:00', '2026-08-01 23:59:59'), null, '同一日は割れない → null');
  eq(splitWindow('2026-08-01 00:00:00', '2026-08-02 23:59:59'),
    [['2026-08-01 00:00:00', '2026-08-01 23:59:59'], ['2026-08-02 00:00:00', '2026-08-02 23:59:59']], '2日 → 1日ずつ');
  const [a, b] = splitWindow('2026-08-01 00:00:00', '2026-08-31 23:59:59');
  eq([a[0], b[1]], ['2026-08-01 00:00:00', '2026-08-31 23:59:59'], '1ヶ月を割っても両端は保たれる');
  ok(a[1] < b[0], '前半の終わりと後半の始まりが重ならない (二重計上しない)');
  eq([a[1].slice(0, 10), b[0].slice(0, 10)], ['2026-08-16', '2026-08-17'], '境界は日単位で連続する (欠けない)');
}

// ─────────────── 3d. バックフィルの引数検証 ───────────────
console.log('\n── validateRange (不正な引数で NE API を叩かない) ──');
{
  const throws = (fn) => { try { fn(); return false; } catch { return true; } };
  ok(throws(() => validateRange('2026-13-01', '2026-08-05', 12)), '存在しない月 → エラー');
  ok(throws(() => validateRange('2026-02-31', '2026-08-05', 12)), '存在しない日 (2/31) → エラー');
  ok(throws(() => validateRange('2026-08-05', '2026-08-01', 12)), 'from > to → エラー');
  ok(throws(() => validateRange('2026-08-01', '2026-08-05', 0)), '--months 0 → エラー');
  ok(throws(() => validateRange('2026-08-01', '2026-08-05', NaN)), '--months NaN → エラー');
  ok(!throws(() => validateRange('2025-08-05', '2026-08-05', 12)), '正常な1年指定は通る');
}

// ───────────────────────── 4. 月分割 ─────────────────────────
console.log('\n── splitMonths (バックフィルの月分割) ──');
{
  eq(splitMonths('2026-08-01', '2026-08-31'), [['2026-08-01', '2026-08-31']], '同一月まるごと');
  eq(splitMonths('2026-08-03', '2026-08-05'), [['2026-08-03', '2026-08-05']], '月の途中だけ');
  eq(splitMonths('2025-12-15', '2026-02-03'),
    [['2025-12-15', '2025-12-31'], ['2026-01-01', '2026-01-31'], ['2026-02-01', '2026-02-03']], '年またぎ');
  eq(splitMonths('2024-02-01', '2024-02-29'), [['2024-02-01', '2024-02-29']], 'うるう年の2月末');
  eq(splitMonths('2026-02-01', '2026-02-28'), [['2026-02-01', '2026-02-28']], '平年の2月末');
  eq(splitMonths('2025-08-05', '2026-08-05').length, 13, '1年 = 13区間 (両端の欠け月を含む)');
}

// ───────────────────────── 後始末 ─────────────────────────
db.close();
fs.rmSync(tmpDir, { recursive: true, force: true });
console.log(`\n${failed === 0 ? '✅ 全テスト PASS' : `❌ ${failed} 件失敗`}`);
process.exit(failed === 0 ? 0 : 1);
