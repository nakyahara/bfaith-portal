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
const { splitMonths } = await import('./backfill-ne-order-base.js');

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

  // 識別系は初回スナップショットを保持する
  upsert(mkRow({ 受注日: '1999-01-01 00:00:00', 店舗コード: '9', 受注番号: 'R-CHANGED', 出荷確定日: '2026-08-02 15:00:00', 受注状態区分: '50', 受注状態: '出荷確定済（完了）', 配送方法ID: '20', 配送方法名: 'ヤマト(発払い)B2v6' }), 'T5');
  const keep = db.prepare('SELECT 受注日, 店舗コード, 受注番号 FROM raw_ne_order_base WHERE 伝票番号=?').get('1000');
  eq([keep.受注日, keep.店舗コード, keep.受注番号], ['2026-08-01 10:00:00', '1', 'R-1'], '識別系 (受注日/店舗/受注番号) は初回値を保持');
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
  eq(r1.slips, 6, '有効伝票の合計 (全8件 − キャンセル1件 − 未出荷1件 = 6件)');

  const get = (d, shop, dv) => db.prepare(
    'SELECT slips, cancelled_slips, delivery_name FROM f_shipments_daily WHERE ship_date=? AND shop_code=? AND delivery_id=?'
  ).get(d, shop, dv);

  eq(get('2026-08-04', '4', '71').slips, 2, '8/4 Amazon × AES = 2件');
  eq(get('2026-08-04', '4', '28').slips, 1, '8/4 Amazon × ネコポス = 1件 (Amazon でも AES 以外がある)');
  eq(get('2026-08-05', '4', '71').slips, 1, '8/5 Amazon × AES = 1件');
  eq([get('2026-08-04', '1', '28').slips, get('2026-08-04', '1', '28').cancelled_slips], [1, 1],
    '8/4 楽天 × ネコポス = 有効1件 / キャンセル1件 (別カウント)');
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
