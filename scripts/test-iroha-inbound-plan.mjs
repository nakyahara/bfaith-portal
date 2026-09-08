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

const { importCsv, workDateJst, getActiveBatch, finalizeLine, reopenLine, applyQuantityEvents, resolveDestination, infoForLine }
  = await import('../apps/inbound-check/db.js');

// 倉庫の iPad (router) が確定のときに渡す「行き先の決め方」と同じもの。
// これを渡さないと行き先の台帳に行が立たない = 本番と違う道を試すことになる
const decide = (line) => {
  const { info, expiryManaged } = infoForLine(line.code_key);
  const d = resolveDestination(info, { expiryManaged });
  return d.destination ? { ok: true, ...d, decidedFrom: 'master' } : { ok: false };
};
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
// ⭐日付は**今日からの日数**で作る。固定日にすると「過去5日だけ出す」の窓から外れて、
//   ある日を境に落ちるテストになる
const TODAY = workDateJst();
function shiftDay(ymd, days) {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(ymd);
  const d = new Date(Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3])));
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString().slice(0, 10);
}
const iso = (offset) => shiftDay(TODAY, offset);          // 'YYYY-MM-DD'
const ymd8 = (offset) => iso(offset).replace(/-/g, '');   // CSV の 'YYYYMMDD'

const row = (ar, no, detail, pid, qty, planned = ymd8(0)) => ({
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
  eq(r.totals, { products: 0, qty: 0, iroha_products: 0, iroha_qty: 0, arrived_products: 0, arrived_qty: 0, old_products: 0, old_qty: 0 }, '合計は 0');
  eq(r.supplier.codes, ['0001'], '仕入先コードは返る');
  eq(r.supplier.name, 'アメージングクラフト様', '仕入先名は正規形 (1) で引ける');
  ok(r.day_stale === false, '取込が無ければ「前の日」にはしない');
}

// ─── ② 仕入先で絞る / 数量をまとめる ───
console.log('\n[2] 仕入先 0001 だけ・取得日 × 商品でまとめる');
// ⚠ 軸は**入荷リストに載った日 (取込日)**。ロジザードの入荷予定日は現場で正確に入れていないので使わない
//   (中原さん 2026-09-09)。この取込は全部「今日」なので、同じ商品はひとまとまりになる
const imp = importCsv(makeCsv([
  row('AR1', 1, 1, 'amc-a', 10),
  row('AR1', 2, 1, 'amc-a', 5),               // 同じ伝票の別行 → 足して 15
  row('AR2', 1, 1, 'amc-a', 3),               // 別伝票・同じ日 → さらに足して 18
  row('AR2', 2, 1, 'amc-b', 40),
  row('AR2', 3, 1, 'other-e', 999),           // 別の仕入先
  row('AR2', 4, 1, 'nosup-f', 999),           // 仕入先コード空
  row('AR2', 5, 1, 'unknown-z', 999),         // 商品マスタに無い
  row('AR3', 1, 1, 'amc-c', 7, ymd8(-1)),   // 前の日の伝票
  row('AR4', 1, 1, 'amc-d', 2, ymd8(2)),    // 先の日の伝票
  row('AR4', 2, 1, 'amc-a', 4, ymd8(2)),    // 同じ商品でも伝票の日が違えば別の行
]), { source: 'manual_upload', fileName: 'test.csv' });
ok(imp.ok, `取込 ok (${imp.ok ? imp.rowCount + '行' : imp.message})`);

const r = listInboundPlan();
eq(r.rows.map(x => `${x.fetched_on} ${x.product_code} ${x.qty} ${x.iroha}`), [
  `${iso(0)} amc-a 22 有り`,
  `${iso(0)} amc-c 7 未記入`,
  `${iso(0)} amc-d 2 状況による`,
  `${iso(0)} amc-b 40 無し`,
], '⭐0001 の商品だけ・取得日 × 商品で合算 / ⭐同じ取得日の中は 有り → 空欄 → 状況による → 無し の順');
ok(!r.rows.some(x => 'planned_date' in x || 'past' in x),
  '⭐入荷予定日も「遅れている」印も返さない (使わないものを画面へ渡さない)');
eq(r.rows.find(x => x.product_code === 'amc-a').lines, 4, '合算した明細の数を持つ');
eq(r.rows.find(x => x.product_code === 'amc-a').ar_nos, ['AR1', 'AR2', 'AR4'], 'まとめた伝票番号を持つ');
eq(r.rows.map(x => x.iroha_kind), ['yes', 'unknown', 'other', 'no'], '区分の種別 (有り・未記入・その他・無し) を分けて返す');
eq(r.rows[0].product_name, '商品 amc-a', '商品名はロジザードの明細から');
eq(r.totals, { products: 4, qty: 71, iroha_products: 1, iroha_qty: 22, arrived_products: 0, arrived_qty: 0, old_products: 0, old_qty: 0 },
  '合計と いろは分の内訳 (まだ何も届いていないので出さない分は 0)');
ok(!r.rows.some(x => ['other-e', 'nosup-f', 'unknown-z'].includes(x.product_code)), '別の仕入先・仕入先空・マスタに無い商品は出さない');

// ─── ③ 商品名が空のとき ───
console.log('\n[3] 商品名が空の明細');
{
  const imp2 = importCsv(makeCsv([
    { ...row('AR5', 1, 1, 'amc-a', 1), 商品名: '', 入荷予定日: '' },
    row('AR6', 1, 1, 'amc-b', 2, ymd8(1)),
  ]), { source: 'manual_upload', fileName: 'test2.csv' });
  ok(imp2.ok, `取込 ok (${imp2.ok ? imp2.rowCount + '行' : imp2.message})`);
  const r2 = listInboundPlan();
  eq(r2.rows.map(x => x.product_name), ['マスタ名A', '商品 amc-b'],
    '商品名が空なら商品マスタで補う (入荷予定日が空でも困らない — 使っていないので)');
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

// ─── ⑤ 商品コードの揺れ (全角・大文字小文字違いで仕入先が食い違う行) ───
//   ⓐ SQLite の lower() は ASCII 限定なので、全角英字の商品コードは SQL 側だけでは引けない。
//      引けないと「仕入先が分からない」= 一覧から黙って消えるので、生の商品IDでも拾う
//   ⓑ 同じ code_key の行が 2 つあり仕入先が食い違うとき、SQLite の返す順で決めない (Codex R1 P2)
console.log('\n[5] 商品コードの揺れ (全角 / 大文字小文字違いで仕入先が食い違う)');
{
  const WIDE = 'ＡＭＣ-Ｚ';                       // 全角の商品コード
  const wideKey = WIDE.trim().toLowerCase();      // JS の code_key ('ａｍｃ-ｚ')。SQL の lower() では作れない
  insProduct.run(7, WIDE, 'マスタ全角', '0001');
  insProduct.run(8, 'DUP-X', 'マスタ大文字', '0002');   // 同じ code_key で仕入先が食い違う
  insProduct.run(9, 'dup-x', 'マスタ小文字', '0001');   //   ↑ 片方が 0001
  insProduct.run(10, 'OTH-Y', 'よそ大文字', '0002');    // 食い違うが、どちらも 0001 ではない
  insProduct.run(11, 'oth-y', 'よそ小文字', '0003');
  // ⭐入荷側とマスタで**全角の大小が違う**ケース (ｗｉｄｅ-ｑ / ＷＩＤＥ-Ｑ)。
  //   SQLite の lower() でも生の比較でも当たらない — 非ASCII だけ読んで JS で突き合わせる経路 (Codex R3 P2)
  const WIDE_UP = 'ＷＩＤＥ-Ｑ';
  const WIDE_LOW = 'ｗｉｄｅ-ｑ';          // 入荷受付CSV 側の表記
  insProduct.run(12, WIDE_UP, 'マスタ全角大文字', '0001');
  // ⭐②で「たまたま同じ表記の行」(別の仕入先) が当たっても、大小違いの 0001 の行を取り逃さない (Codex R4 P2)
  const CONF_UP = 'ＣＯＮ-Ｆ';               // 入荷受付CSV と同じ表記だが 0002
  const CONF_LOW = 'ｃｏｎ-ｆ';               // 大小違いで 0001
  insProduct.run(13, CONF_UP, 'よその全角', '0002');
  insProduct.run(14, CONF_LOW, 'いろはの全角', '0001');
  insInfo.run(wideKey, WIDE, 'マスタ全角', '有り');
  insInfo.run('dup-x', 'DUP-X', 'マスタ大文字', '有り');
  insInfo.run(WIDE_LOW.trim().toLowerCase(), WIDE_LOW, 'マスタ全角大文字', '無し');
  insInfo.run(CONF_UP.trim().toLowerCase(), CONF_UP, 'いろはの全角', '有り');

  const impW = importCsv(makeCsv([
    row('AR7', 1, 1, WIDE, 3),
    row('AR7', 2, 1, 'DUP-X', 6),
    row('AR7', 3, 1, 'OTH-Y', 9),
    row('AR7', 4, 1, WIDE_LOW, 5),
    row('AR7', 5, 1, CONF_UP, 8),
  ]), { source: 'manual_upload', fileName: 'test3.csv' });
  ok(impW.ok, `取込 ok (${impW.ok ? impW.rowCount + '行' : impW.message})`);

  const r5 = listInboundPlan();
  ok(r5.rows.some((x) => x.product_code === WIDE && x.qty === 3),
    '⭐全角の商品コードでも仕入先を引けて一覧に出る (SQL の lower() では引けない)');
  const dup = r5.rows.find((x) => x.product_code === 'DUP-X');
  ok(dup && dup.qty === 6, '⭐大文字小文字違いの行のうち 0001 のほうを採る (出すべきものを落とさない)');
  ok(dup && dup.supplier_conflict === true, '⭐仕入先が食い違っていることを画面へ伝える (黙って決めない)');
  ok(r5.rows.find((x) => x.product_code === WIDE).supplier_conflict === false, '食い違っていない行には印を付けない');
  ok(!r5.rows.some((x) => x.product_code === 'OTH-Y'), 'どちらも 0001 でなければ出さない');
  const wideLow = r5.rows.find((x) => x.product_code === WIDE_LOW);
  ok(wideLow && wideLow.qty === 5 && wideLow.iroha === '無し',
    '⭐入荷側とマスタで全角の大小が違っても引ける (ｗｉｄｅ-ｑ ↔ ＷＩＤＥ-Ｑ)');
  const conf = r5.rows.find((x) => x.product_code === CONF_UP);
  ok(conf && conf.qty === 8,
    '⭐同じ表記の別仕入先の行が先に当たっても、大小違いの 0001 の行を取り逃さない');
  ok(conf && conf.supplier_conflict === true, 'その食い違いも画面へ伝える');
  // ⭐同じ入力から同じ結果 (SQLite の返す順に左右されない)
  eq(JSON.stringify(listInboundPlan().rows), JSON.stringify(r5.rows), '⭐何度呼んでも同じ一覧になる');
}

// ─── ⑥ 見出しの仕入先名の引き方 (Codex R2 P2) ───
//   po_suppliers は発注管理の正規形 ('1') で持つが、'0001' と '1' が別の会社として
//   両方登録されている場合がある。いきなり正規形にすると、絞り込んでいるのと違う会社の名前が出る
console.log('\n[6] 見出しの仕入先名');
{
  const insSup = mirror.prepare('INSERT OR REPLACE INTO po_suppliers (supplier_code, name) VALUES (?, ?)');
  const del = (c) => mirror.prepare('DELETE FROM po_suppliers WHERE supplier_code = ?').run(c);

  insSup.run('0001', 'ゼロ埋めで登録された別の会社様');
  eq(listInboundPlan().supplier.name, 'ゼロ埋めで登録された別の会社様',
    '⭐生コード (0001) の完全一致を、正規形 (1) より先に見る');
  del('0001');
  eq(listInboundPlan().supplier.name, 'アメージングクラフト様', '完全一致が無ければ正規形で引く');

  insSup.run('01', 'まぎらわしい会社様');   // 正規形はどちらも '1' — どちらか決められない
  eq(listInboundPlan().supplier.name, null, '⭐同じ正規形に別名が 2 つあれば名前を出さない (取り違えない)');
  del('01');

  // 売れ筋共有の表示名は mirror_products と同じ体系なので、こちらが最優先
  mirror.prepare(`INSERT OR REPLACE INTO supplier_share_master (仕入先コード, 表示名, created_at, updated_at)
    VALUES (?, ?, '2026-09-08T00:00:00Z', '2026-09-08T00:00:00Z')`).run('0001', '共有の表示名様');
  eq(listInboundPlan().supplier.name, '共有の表示名様', '売れ筋共有の表示名があればそれを使う');
  mirror.prepare('DELETE FROM supplier_share_master WHERE 仕入先コード = ?').run('0001');
}

// ─── ⑦ 画面・API の配線 ───
console.log('\n[7] 画面と API の配線');
{
  const html = fs.readFileSync(new URL('../apps/iroha-work/views/index.html', import.meta.url), 'utf8');
  const router = fs.readFileSync(new URL('../apps/iroha-work/router.js', import.meta.url), 'utf8');
  ok(/id="vInbound" onclick="setView\('inbound'\)"/.test(html), '作業画面のナビに 🚚 入荷予定 がある');
  ok(/inbound: '\.inboundpage'/.test(html) && /inbound: '#vInbound'/.test(html), 'PAGES / VIEW_BTN に登録されている (他の画面を隠す側も動く)');
  ok(/if \(v === 'inbound'\) loadInboundPlan\(\);/.test(html), '開いたときに取りにいく');
  ok(/<table class="tbl plain fit">[\s\S]{0,300}<th class="day">取得日<\/th><th>商品<\/th><th>数量<\/th><th>いろは在庫化区分<\/th>/.test(html),
    '表の列は 取得日 / 商品 / 数量 / いろは在庫化区分');
  ok(!/<th>入荷予定日<\/th>/.test(html) && !/r\.planned_date/.test(html),
    '⭐入荷予定日は列にも行にも出さない (正確に入っていないので — 中原さん 2026-09-09。説明文で触れるのはよい)');
  ok(/ipDayLabel\(r\.fetched_on\)/.test(html), '⭐出す日付は「取得日」');
  ok(/\.tbl\.fit\{min-width:0\}/.test(html) && /@media \(max-width:560px\)/.test(html),
    '⭐幅の狭い端末でも横スクロールなしで収まる (min-width を外す + 狭いとき用の指定)');
  ok(/\.tbl\.fit \.day\{display:none\}/.test(html) && /class="dayline"/.test(html),
    '⭐幅が狭いときは「取得日」の列を畳んで商品名の下へ回す (商品名に幅を与える)');
  ok(/\.tbl\.plain tbody tr\{cursor:default\}/.test(html), '行は押せない見た目にする (開く先が無い)');
  ok(!/遅れています/.test(html) && !/tr\.past/.test(html),
    '⭐「遅れている」印もグレーアウトも出さない (入荷予定日が当てにならないので判断しない — 中原さん 2026-09-09)');
  ok(/id="ipHidden"/.test(html) && /出していないもの: /.test(html), '⭐出していない分の理由を画面に書く (黙って減らさない)');
  ok(/router\.get\('\/api\/inbound-plan'/.test(router), 'GET /api/inbound-plan がある');
  ok(!/\/api\/inbound-plan'[\s\S]{0,200}checkOrigin/.test(router), '読むだけなので書き込みの口 (POST) は作らない');
}

// ─── ⑧ 届いたものは出さない / 取り込んでから古すぎるものは出さない (中原さん 2026-09-09) ───
//   「来たものは非表示にして、来ていないものは過去五日間だけ」
//   ⭐軸は**取得日**。入荷予定日は現場で正確に入れていないので使わない。
//   「取得日」= その明細を初めて取り込んだ日 (取込は全置換なので MIN でよい)
console.log('\n[8] 取得日 / 届いたもの / 古くなったもの');
{
  // 取込を 3 回に分けて、明細ごとに「取得日」を作る (取込日は imported_at を後から動かして作る)
  const backdate = (offset) => {
    const id = getActiveBatch().id;
    mirror.prepare('UPDATE f_inbound_check_batches SET imported_at = ? WHERE id = ?').run(`${iso(offset)}T00:00:00.000Z`, id);
  };
  ok(importCsv(makeCsv([
    row('AR10', 1, 1, 'amc-c', 7),
    row('AR14', 1, 1, 'amc-c', 9),   // ⭐この明細はあとで別の商品に差し替わる
  ]), { source: 'manual_upload', fileName: 'old6.csv' }).ok, '6 日前の取込');
  backdate(-6);
  ok(importCsv(makeCsv([row('AR11', 1, 1, 'amc-d', 2)]), { source: 'manual_upload', fileName: 'old5.csv' }).ok, '5 日前の取込');
  backdate(-5);
  const imp8 = importCsv(makeCsv([
    row('AR8', 1, 1, 'amc-a', 10),   // 今日はじめて取り込んだ — あとで「届いた」ことにする
    row('AR8', 2, 1, 'amc-b', 40),   // 今日はじめて取り込んだ
    row('AR10', 1, 1, 'amc-c', 7),   // 6 日前から載っている = 古すぎるので出さない
    row('AR11', 1, 1, 'amc-d', 2),   // ちょうど 5 日前から = 境界。まだ出す
    // ⭐同じ明細 (AR14|1|1) の商品が差し替わった。新しい商品は「今日 載った」扱いにする —
    //   前の商品の日付を引き継ぐと、取り込んだ当日に「5 日より前」として消えてしまう (Codex P1)
    row('AR14', 1, 1, 'amc-b', 3),
  ]), { source: 'manual_upload', fileName: 'today.csv' });
  ok(imp8.ok, `今日の取込 ok (${imp8.ok ? imp8.rowCount + '行' : imp8.message})`);

  const before = listInboundPlan();
  eq(before.past_days, 5, '何日ぶんまで出すかを画面へ返す');
  eq(before.today, TODAY, 'サーバーが決めた今日 (JST) を返す — iPad の時計を信じない');
  eq(before.rows.map((x) => `${x.fetched_on} ${x.product_code} ${x.qty}`), [
    `${iso(-5)} amc-d 2`,
    `${iso(0)} amc-a 10`,
    `${iso(0)} amc-b 43`,
  ], '⭐6 日前から載っているものは出さない / ちょうど 5 日前は出す / 先に取り込んだ順');
  ok(before.rows.some((x) => x.product_code === 'amc-b' && x.ar_nos.includes('AR14')),
    '⭐商品が差し替わった明細は、新しい商品として「今日 取り込んだ」扱いにする (前の商品の日付を引き継がない)');
  eq([before.totals.old_products, before.totals.old_qty], [1, 7], '出さなかった古い分を数える');

  // ── 倉庫の iPad が「確認」を確定する = 現物が届いた ──
  const b8 = getActiveBatch();
  const fin = finalizeLine({ batchId: b8.id, lineKey: 'AR8|1|1', expectVersion: 1, expectQuantityVersion: 1,
    result: 'exact', mode: 'fill_remaining', fillEvent: { client_event_id: 'ev-ip-ar8-1' },
    decide, worker: '倉庫の人', deviceLabel: '倉庫iPad' });
  ok(fin.ok, '確認を確定できた' + (fin.ok ? '' : ': ' + fin.message));

  const after = listInboundPlan();
  ok(!after.rows.some((x) => x.product_code === 'amc-a'),
    '⭐届いた明細は入荷予定から消える (いろは行きは 📋 作業 のカードになる)');
  eq([after.totals.arrived_products, after.totals.arrived_qty], [1, 10], '届いた分を数えて画面に理由を出せる');
  ok(after.rows.some((x) => x.product_code === 'amc-d' && x.qty === 2), '届いていない他の行は残る');

  // ── やり直し (誤タップの取り消し) をしたら、また「まだ来ていない」に戻る ──
  const un = reopenLine({ batchId: b8.id, lineKey: 'AR8|1|1', expectVersion: fin.state.version, worker: '倉庫の人' });
  ok(un.ok, 'やり直せた' + (un.ok ? '' : ': ' + un.message));
  ok(listInboundPlan().rows.some((x) => x.product_code === 'amc-a' && x.qty === 10),
    '⭐やり直したら また出る (取り消した確認で消えたままにしない)');

  // ── 「数えたけれど 1 個も来なかった」は届いた扱いにしない ──
  const zero = finalizeLine({ batchId: b8.id, lineKey: 'AR8|2|1', expectVersion: 1, expectQuantityVersion: 1,
    result: 'shortage', mode: 'current', decide, worker: '倉庫の人', deviceLabel: '倉庫iPad' });
  ok(zero.ok, '0 個のまま不足で確定できた' + (zero.ok ? '' : ': ' + zero.message));
  const z = listInboundPlan();
  ok(z.rows.some((x) => x.product_code === 'amc-b' && x.qty === 43),
    '⭐数えた結果 0 のものは「届いた」にしない (まだ来ていないものとして出す)');
  eq(z.totals.arrived_products, 0, '届いた分にも数えない');

  // ── 予定より少なく届いた (不足で確定) ときは、届いた分を**数えた数**で数える ──
  //    予定 10 個で 4 個しか届いていない行を「10 個 届いた」と出さない (Codex P2)
  {
    // AR14|1|1 = amc-b の 3 個。1 個だけ数えて不足で確定する
    ok(applyQuantityEvents({ batchId: b8.id, lineKey: 'AR14|1|1', expectQuantityVersion: 1,
      events: [{ client_event_id: 'ev-ip-partial', action: 'add', quantity: 1, input_kind: 'loose' }],
      worker: '倉庫の人' }).ok, '1 個だけ数えた');
    const part = finalizeLine({ batchId: b8.id, lineKey: 'AR14|1|1', expectVersion: 1, expectQuantityVersion: 2,
      result: 'shortage', mode: 'current', decide, worker: '倉庫の人', deviceLabel: '倉庫iPad' });
    ok(part.ok, '不足で確定できた' + (part.ok ? '' : ': ' + part.message));
    const pr = listInboundPlan();
    eq([pr.totals.arrived_products, pr.totals.arrived_qty], [1, 1],
      '⭐届いた数は予定数 (3) ではなく数えた数 (1) で出す');
    ok(pr.rows.some((x) => x.product_code === 'amc-b' && x.qty === 40),
      '同じ商品でも、確認していない別の明細 (40 個) はそのまま残る');
  }

  // ⭐商品コードに空白が入っていても、取得日との境目が曖昧にならない (まとめるキーの区切りは NUL)
  insProduct.run(20, `${iso(0)} amc-a`, '空白入りのコード', '0001');
  const impSp = importCsv(makeCsv([
    row('AR12', 1, 1, 'amc-a', 11),
    row('AR13', 1, 1, `${iso(0)} amc-a`, 22),
  ]), { source: 'manual_upload', fileName: 'space.csv' });
  ok(impSp.ok, `取込 ok (${impSp.ok ? impSp.rowCount + '行' : impSp.message})`);
  eq(listInboundPlan().rows.map((x) => `${x.product_code} ${x.qty}`), [
    'amc-a 11',                 // 有り
    `${iso(0)} amc-a 22`,       // 入庫情報が無いので 未記入 → 後ろ
  ], '⭐空白入りの商品コードが、別の商品とまとまってしまわない');
}

console.log(`\n${fail === 0 ? '✅' : '❌'} ${pass} passed / ${fail} failed`);
process.exit(fail === 0 ? 0 : 1);
