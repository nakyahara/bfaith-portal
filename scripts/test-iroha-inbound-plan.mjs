/**
 * いろは在庫化作業アプリ 🚚 入荷予定 — apps/iroha-work/inbound-plan.js
 *
 * 実行: node scripts/test-iroha-inbound-plan.mjs
 *
 * 中原さん 2026-09-08「入荷予定が見れるようにしてほしい。仕入先コード0001だけ商品名数量、いろは在庫化区分が見れたらいい」
 *
 * 守りたいのは4つ。
 *   ① 仕入先コード 0001 の商品**だけ**を出す。'0001' と '1' (発注管理の正規形) の揺れはどちらも 0001 として扱い、
 *      仕入先コードが空の商品・別の仕入先の商品は絶対に混ぜない
 *   ② 数量は「入荷予定日 × 商品」でまとめて足す (同じ商品が複数の伝票・複数の行に分かれて載る)
 *   ③ いろは在庫化区分は入庫情報管理の値そのまま。「状況による」等の人が入れた値を 有り/無し へ寄せない
 *   ④ 元データ (入荷受付伝票) が無い・取込が前の日のままでも落ちない
 */
import fs from 'fs';
import os from 'os';
import path from 'path';
import iconv from 'iconv-lite';

if (!process.env.DATA_DIR) process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'iw-inbound-plan-'));
const { initMirrorDB, getMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const mirror = getMirrorDB();

const { importCsv, workDateJst } = await import('../apps/inbound-check/db.js');
const { listInboundPlan, SUPPLIER_CODES } = await import('../apps/iroha-work/inbound-plan.js');

let pass = 0, fail = 0;
const ok = (cond, label) => { if (cond) { pass++; console.log(`  ✓ ${label}`); } else { fail++; console.log(`  ✗ ${label}`); } };
const eq = (a, b, label) => ok(JSON.stringify(a) === JSON.stringify(b), `${label} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

console.log('DATA_DIR =', process.env.DATA_DIR);
ok(JSON.stringify(SUPPLIER_CODES) === JSON.stringify(['0001']), '見せる仕入先は 0001 だけ');

// ─── 元データ (ロジザードの入荷受付CSV) ───
const HEADER = ['入荷管理番号', '入荷管理行番号', '入荷管理詳細行番号', 'ステータス', '荷主入荷NO', '入荷予定日', '入荷受付日', '入荷確定日',
  '取引先ID', '取引先名', '業務区分名', '商品ID', '商品名', '品質区分名', 'ロケーション', '予定数', '受付数', '検品数', '作成日時', '更新日時', 'バーコード', '備考'];
const makeCsv = (rows) => {
  const q = v => `"${String(v == null ? '' : v).replace(/"/g, '""')}"`;
  return iconv.encode([HEADER.map(q).join(','), ...rows.map(r => HEADER.map(h => q(r[h])).join(','))].join('\r\n') + '\r\n', 'cp932');
};
const row = (ar, no, detail, pid, qty, planned = '20260908') => ({
  入荷管理番号: ar, 入荷管理行番号: no, 入荷管理詳細行番号: detail, ステータス: '受付済',
  入荷予定日: planned, 入荷受付日: planned, 取引先ID: '9999', 取引先名: 'ロジザードの取引先', 業務区分名: '通常入荷',
  商品ID: pid, 商品名: `商品 ${pid}`, 品質区分名: '良品', 予定数: qty, 受付数: qty,
  作成日時: '20260908090000', 更新日時: '20260908090000', バーコード: '4500000000000',
});

// ─── 商品マスタ (仕入先コードの正本) ───
const insProduct = mirror.prepare(`INSERT INTO mirror_products (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, 仕入先コード, updated_at)
  VALUES (?, ?, ?, '単品', '取扱中', 'ok', ?, '2026-09-08T00:00:00Z')`);
insProduct.run(1, 'amc-a', 'マスタ名A', '0001');      // AMC (ゼロ埋め)
insProduct.run(2, 'amc-b', 'マスタ名B', '1');          // AMC (発注管理の正規形。同じ会社として扱う)
insProduct.run(3, 'amc-c', 'マスタ名C', '0001');       // AMC・入庫情報が未登録
insProduct.run(4, 'amc-d', 'マスタ名D', '0001');       // AMC・人が入れた値 (状況による)
insProduct.run(5, 'other-e', 'マスタ名E', '0002');     // 別の仕入先 → 出ない
insProduct.run(6, 'nosup-f', 'マスタ名F', null);       // 仕入先コード空 → 出ない

// ─── 入庫情報管理 (いろは在庫化区分の正本) ───
const insInfo = mirror.prepare(`INSERT INTO f_inbound_info (code_key, 商品コード, 商品名, いろは在庫化作業有無, source, created_at, updated_at)
  VALUES (?, ?, ?, ?, 'manual', '2026-09-08T00:00:00Z', '2026-09-08T00:00:00Z')`);
insInfo.run('amc-a', 'amc-a', 'マスタ名A', '有り');
insInfo.run('amc-b', 'amc-b', 'マスタ名B', '無し');
insInfo.run('amc-d', 'amc-d', 'マスタ名D', '状況による');
insInfo.run('other-e', 'other-e', 'マスタ名E', '有り');   // いろは行きでも仕入先が違えば出ない
insInfo.run('nosup-f', 'nosup-f', 'マスタ名F', '有り');

// ─── 仕入先名 (見出し用) ───
mirror.exec(`CREATE TABLE IF NOT EXISTS po_suppliers (supplier_code TEXT PRIMARY KEY, name TEXT)`);
mirror.prepare('INSERT INTO po_suppliers (supplier_code, name) VALUES (?, ?)').run('1', 'アメージングクラフト様');

// ─── ① 取込が1件も無いとき ───
console.log('\n[1] 入荷受付の取込がまだ無いとき');
{
  const r = listInboundPlan();
  eq(r.batch, null, 'バッチは null');
  eq(r.rows, [], '一覧は空');
  eq(r.totals, { products: 0, qty: 0, iroha_products: 0, iroha_qty: 0 }, '合計は 0');
  eq(r.supplier.codes, ['0001'], '仕入先コードは返る');
  eq(r.supplier.name, 'アメージングクラフト様', '仕入先名は正規形 (1) で引ける');
  ok(r.day_stale === false, '取込が無ければ「前の日」にはしない');
}

// ─── ② 仕入先で絞る / 数量をまとめる ───
console.log('\n[2] 仕入先 0001 だけ・入荷予定日 × 商品でまとめる');
// ⚠ 入荷予定日は**伝票 (AR番号) 単位** — f_inbound_check_slips に 1 行しか無いので、同じ伝票の行は全部同じ日になる
const imp = importCsv(makeCsv([
  row('AR1', 1, 1, 'amc-a', 10),
  row('AR1', 2, 1, 'amc-a', 5),               // 同じ伝票の別行 → 足して 15
  row('AR2', 1, 1, 'amc-a', 3),               // 別伝票・同じ日 → さらに足して 18
  row('AR2', 2, 1, 'amc-b', 40),
  row('AR2', 3, 1, 'other-e', 999),           // 別の仕入先
  row('AR2', 4, 1, 'nosup-f', 999),           // 仕入先コード空
  row('AR2', 5, 1, 'unknown-z', 999),         // 商品マスタに無い
  row('AR3', 1, 1, 'amc-c', 7, '20260907'),   // 前の日の伝票
  row('AR4', 1, 1, 'amc-d', 2, '20260910'),   // 先の日の伝票
  row('AR4', 2, 1, 'amc-a', 4, '20260910'),   // 同じ商品でも伝票の日が違えば別の行
]), { source: 'manual_upload', fileName: 'test.csv' });
ok(imp.ok, `取込 ok (${imp.ok ? imp.rowCount + '行' : imp.message})`);

const r = listInboundPlan();
eq(r.rows.map(x => `${x.planned_date} ${x.product_code} ${x.qty} ${x.iroha}`), [
  '2026-09-07 amc-c 7 未記入',
  '2026-09-08 amc-a 18 有り',
  '2026-09-08 amc-b 40 無し',
  '2026-09-10 amc-a 4 有り',
  '2026-09-10 amc-d 2 状況による',
], '0001 の商品だけ・日付順・同じ日の同じ商品は合算');
eq(r.rows.find(x => x.product_code === 'amc-a' && x.planned_date === '2026-09-08').lines, 3, '合算した明細の数を持つ');
eq(r.rows.find(x => x.product_code === 'amc-a' && x.planned_date === '2026-09-08').ar_nos, ['AR1', 'AR2'], 'まとめた伝票番号を持つ');
eq(r.rows.map(x => x.iroha_kind), ['unknown', 'yes', 'no', 'yes', 'other'], '区分の種別 (未記入・有り・無し・その他) を分けて返す');
eq(r.rows[0].product_name, '商品 amc-c', '商品名はロジザードの明細から');
eq(r.totals, { products: 5, qty: 71, iroha_products: 2, iroha_qty: 22 }, '合計と いろは分の内訳');
ok(!r.rows.some(x => ['other-e', 'nosup-f', 'unknown-z'].includes(x.product_code)), '別の仕入先・仕入先空・マスタに無い商品は出さない');

// ─── ③ 商品名が空のとき / 入荷予定日が空のとき ───
console.log('\n[3] 商品名・入荷予定日が空の明細');
{
  const imp2 = importCsv(makeCsv([
    { ...row('AR5', 1, 1, 'amc-a', 1), 商品名: '', 入荷予定日: '' },
    row('AR6', 1, 1, 'amc-b', 2, '20260909'),
  ]), { source: 'manual_upload', fileName: 'test2.csv' });
  ok(imp2.ok, `取込 ok (${imp2.ok ? imp2.rowCount + '行' : imp2.message})`);
  const r2 = listInboundPlan();
  eq(r2.rows.map(x => `${x.planned_date} ${x.product_name}`), [
    '2026-09-09 商品 amc-b',
    'null マスタ名A',
  ], '入荷予定日が空の行は末尾へ / 商品名が空なら商品マスタで補う');
}

// ─── ④ 本日の取込がまだ来ていない (前の日の一覧) ───
console.log('\n[4] 前の日の取込のまま');
{
  mirror.prepare("UPDATE f_inbound_check_batches SET work_date = '2000-01-01' WHERE status = 'active'").run();
  ok(listInboundPlan().day_stale === true, 'work_date が今日でなければ day_stale');
  mirror.prepare('UPDATE f_inbound_check_batches SET work_date = ?, carried_from = ? WHERE status = ?').run(workDateJst(), '2000-01-01', 'active');
  ok(listInboundPlan().day_stale === true, '倉庫側が繰り越した後 (carried_from) も day_stale');
  mirror.prepare('UPDATE f_inbound_check_batches SET carried_from = NULL WHERE status = ?').run('active');
  ok(listInboundPlan().day_stale === false, '当日の取込なら day_stale ではない');
}

// ─── ⑤ 画面・API の配線 ───
console.log('\n[5] 画面と API の配線');
{
  const html = fs.readFileSync(new URL('../apps/iroha-work/views/index.html', import.meta.url), 'utf8');
  const router = fs.readFileSync(new URL('../apps/iroha-work/router.js', import.meta.url), 'utf8');
  ok(/id="vInbound" onclick="setView\('inbound'\)"/.test(html), '作業画面のナビに 🚚 入荷予定 がある');
  ok(/inbound: '\.inboundpage'/.test(html) && /inbound: '#vInbound'/.test(html), 'PAGES / VIEW_BTN に登録されている (他の画面を隠す側も動く)');
  ok(/if \(v === 'inbound'\) loadInboundPlan\(\);/.test(html), '開いたときに取りにいく');
  ok(/<table class="tbl plain">[\s\S]{0,400}入荷予定日[\s\S]{0,200}商品[\s\S]{0,200}数量[\s\S]{0,200}いろは在庫化区分/.test(html),
    '表の列は 入荷予定日 / 商品 / 数量 / いろは在庫化区分');
  ok(/\.tbl\.plain tbody tr\{cursor:default\}/.test(html), '行は押せない見た目にする (開く先が無い)');
  ok(/router\.get\('\/api\/inbound-plan'/.test(router), 'GET /api/inbound-plan がある');
  ok(!/\/api\/inbound-plan'[\s\S]{0,200}checkOrigin/.test(router), '読むだけなので書き込みの口 (POST) は作らない');
}

console.log(`\n${fail === 0 ? '✅' : '❌'} ${pass} passed / ${fail} failed`);
process.exit(fail === 0 ? 0 : 1);
