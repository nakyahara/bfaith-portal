/**
 * test-master-edit-ui.mjs — マスタの入力の新しい画面 (一覧・1 つの商品) の JS を、描いたページで本当に動かす試験 (#1589 Codex R1 M4)
 *
 * Company DB = PGlite (切替の段階まで本物の関数で進める)・本物の router を express に載せ、Playwright (Chromium) で開く。保存も本当に流す。
 * 確かめること:
 *   1 未保存の数 = data-dirty-field の欄だけ (構成の見せ方の切り替え・保存の理由は 0 件・保存する欄を変えると 1 件)
 *   2 離れるときの確認: 左の列のリンク・全体から探す・ブラウザの戻る。「ここに残る」で押した所へフォーカスが戻る・「捨てて移る」で移る
 *   3 Ctrl+S で保存 (本当に DB に入る)・保存の後は離れても聞かない
 *   4 通信が切れた保存の押し直しは同じ request_id (2 回入らない)
 *   5 開き直しが要る 409 (その間の変更) の後は、欄を触っても保存のボタンが戻らない
 *   6 構成の行を並べ替えると、読み上げの名前が今の行番号・コードになる
 *   7 1280×720 の 100 / 125 / 150% (= 幅 1280 / 1024 / 853) と 1024×768 で、ページが横にはみ出さない (セットの構成の表は囲いの中で横に送る)
 *   8 JAN の欄に打ったまま (Enter を押さずに) ほかの欄も変えて Ctrl+S = JAN も保存する / 形の違う JAN なら保存しない (#1589 Codex R2 M1)
 *   9 画面を開いた後に登録をやめた (cancelled_sku) = 開き直しが要る (欄を触っても保存のボタンが戻らない)・やめた商品は初めから見るだけ (M3)
 *  10 先の日付の原価がある = 該当する原価の欄だけ閉じる (ほかの欄は保存できる) (M2)
 *  11 保存が通ったら画面を読み直す (10/5): 読み直すまでは入力の場所を閉じる (#1589 Codex R3 M1)・欄と見出しは保存した後の値・上に知らせが 1 回・続けて直せる・戻る 1 回で一覧
 *  12 変わった項目が無い保存 (5 と 5.0) の後は未保存が残らない (R3 L3)
 *  13 登録をやめた商品はカードの操作 (もう一度作る・結ぶ) も出さない (R3 M2・画面だけ。API の拒否は master からある穴 = 別 PR)
 *  14 絞る欄の Enter = 絞る・IME の変換を確かめる Enter では送らない・表の行の Enter = 開く・パンくずで同じ絞り込みへ戻る (10/5)
 *  15 かなの同一視 (ひらがな・カタカナ・半角カナ・全角英数・大文字小文字) = 一覧・全体から探す・画面とサーバーの決まりが同じ (10/5)
 *  16 原価を変える理由 = 選ぶ (既定 = メーカーからの値上げ通知)・その他は書かないと保存できない・記録に残る (10/5)
 *  17 1440 / 1280 / 1024 幅: 一覧をスクロールしても見出しの行が上の帯の下に見えている・横に送っても列がずれない (10/5)
 *  18 詳細検索: 商品コードを複数 (貼り付け・大文字・全角) = ぴったり・見つからないコード・URL に残る・クリア (10/5)
 *  19 詳細検索の項目: JAN・仕入先・代表 (親)・商品名・原価 / 売価の範囲・税率・売上分類・取扱区分 (10/5)
 *  20 注文残 (発注アプリ)・在庫 (ロジザード): 一覧の列・絞り込み・1 つの商品の画面の内訳と時刻・セットは作れる数・古い / 読めない (10/5)
 *  21 在庫の範囲はセットなら作れる数で絞る (#1620 Codex R1 M1)
 *  22 注文残は発注アプリの利用権がある人だけ (列・絞り込み・内訳) (#1620 Codex R1 M3)
 *  23 詳細検索の長い条件 = 本物の HTTP で 500 件の境目・印 (?s=)・GET なら 431・Origin と JSON の守り・期限切れ (#1620 Codex R1 M2)
 *  24 FBA (JP) の在庫 (参考・Company DB の在庫の日次): 一覧の列と見出しの時刻・1 × 1 の出品だけ・「—」と 0・単品の画面の内訳とまとめ売り / セットの出品・古い (26 時間) / 読めない (権限なし)・流し直しで戻る (10/5)
 *  25 売れた数 (参考・商品管理リストの公開の回): 一覧の列と「M/D まで」・セット / 無い商品は「—」・単品の画面の FBA / FBA 以外・モール別・古い / 読めない (10/5)
 *  26 区分の列・コードのコピーのボタン (クリップボード・知らせ・行を開かない・キーボード)・全部コピー (絞った全件)・CSV のリンク (ダウンロード)・区分の順 (10/5)
 *  35〜44 第 2 段 (10/5・#1628): 新商品の登録 (単品・セット)・Amazon SKU・NE 登録の CSV・変更の記録 = 未保存 (data-dirty-field だけ)・離れるときの確認・保存 → 読み直し / できた商品の画面へ・
 *        削除は 1 回だけ確かめる・申告はファイルを落として sha256 を照合・1440 / 1280 / 1024 / 150% で横にはみ出さない
 *        (41〜46 = 構成品の照合・カードを作らないときの欄・確かめ直しの上限・Yahoo! の残値と誤りの欄・Amazon の変わらない保存)
 *  49〜50 利益 (1 個あたり・10/6): 単品 / セットの欄を変えると保存の前にその場で計算し直す (保存すると ○ → ○)・数でない・マイナスは赤・元に戻す・一覧の列と利益の少ない順
 *  55 新しい seller SKU を自分で作る (10/11): 一覧の上の欄 = 打つとその場で確かめる・もしかしてを見た後は作る画面へ直行・まだ売れていない SKU を作って保存・もうある = 開く
 *  60 NE 登録の CSV の「この形を実機で確かめた」(10/12 中原さん): 引退した版 (ne-reg-single-v1) の試し用のファイル = ボタンも「本番の行数で作れるようになります」も出さず一言・
 *     門の無い版 (single-v2・variation-v1) も出さない・門のある版 (セット) は今までどおり出る (押すと記録)
 *  51 円の欄 (標準売価・原価・例外原価) の読みはサーバーと同じ (カンマ・全角の数字・空白): 保存すると変わること・前との差・保存するとこうなる・利益
 *  67 スマホ 390 / 360 / 境目の 760 (10/13): 主な画面 22 (一覧・商品・新商品 3 種・Amazon SKU 7 つ・NE 登録の CSV・NE から消す・発注の設定の届け待ち・つかいかた・マスタの判断 3 つ) が
 *     200 で開き・横にはみ出さない (表は表の箱の中で横に送る)・上の帯は 1 行 (未保存と発注の札が出ても)・押す所 (上の帯・左の列・「絞る」・本文のボタン / 切り替え / 札 / 欄・保存の箱) は
 *     44px で真ん中を押すとその物に当たる・本文の最後が下の保存の箱に隠れない
 *  68 スマホ 390: 開く物 (Amazon SKU の構成の候補・全体から探す・左の列の吹き出し・列の板・マスタの判断の引き出し・隠すの窓・離れるときの確認) が画面の中で切れていない・押せる
 *  69 スマホ 390: 上の帯から行ける道 (ポータル・左の列・全体から探す)。隠すのはダッシュボードと「マスタの入力」の札だけ (ほかの .btn は隠さない)。写し = MASTER_EDIT_UI_SHOTS_390=フォルダ
 *  70 Amazon SKU の構成の欄を日本語の名前で探す (10/16): 本物の IME (CDP) で変換中から候補・確定の Enter の 3 つの並び (Windows の Chrome・Edge / Safari) で箱を閉じない・数の欄へ移らない・
 *     マウスで確定した後の Enter は食べない
 *  72〜74 1 つの商品の画面の「← 前へ」「次へ →」(10/17): 来た一覧と同じ並び・絞り込みの前・次と場所・続けて次へ・最初 / 最後・一覧に無い・出さないとき・
 *     未保存のまま押す = 離れるときの確認 (ここに残る / 捨てて移る)・スマホ 390 ではみ出さない・44px
 *  75 一覧の画像の列 (10/17): コードのすぐ後ろ・52px の四角・取得中 / 画像なしの印・読み込めない画像は印・列の板で外す / 出す・画像を押しても開く・390 ではみ出さない
 *  76 Amazon SKU の未登録の一覧の「その場で登録」(10/17): 行の下に欄・名前 = Amazon の商品名・構成の原価
 *  77 新商品 (10/17): 「セット商品を作るか = 作る」の作るセットの表 (初めの値・付いていく・重なり / 0 行は ③ で止める・保留で隠す・保存で product-hub にセットの下書き)
 *  78 Amazon SKU を CSV でまとめて登録 (10/17): 試し取込 (Shift_JIS・表・絞る札・だめな理由・何も書かない)・保存 (DB に入る)・結果の CSV (BOM・式の注入の対策)・同じ CSV = 変わらない・390 ではみ出さない
 *  79 ⑤-1 商品の画面の先方品番・入数 (10/18): 足す → 保存 (発注アプリの対応表)・未保存に数える・直して保存・消す
 *  80〜82 商品の画面のタブ (10/19): 基本情報 / 販売数のグラフ / 入出庫の履歴・role と aria・開いたタブだけ読む (1 回)・#tab=… (読み込み直す・戻る・# を打つ)・
 *     ← → Home End・打った値と未保存はそのまま・Ctrl+S は基本情報で保存・前へ / 次へ・セットの構成品へのリンクも同じタブ・
 *     帯は上に残る (保存の箱と開いた板は帯の下)・基本情報に戻るとスクロールの位置も戻る・スマホ 390 で 44px・はみ出さない
 *  83 モールの商品ページへのリンク (10/20): 見出しの下の札 (楽天・Amazon・Yahoo!・Qoo10)・新しいタブ (noopener)・×N・組んだ出品は出さない・どのタブでも見える・
 *     出品が無い = 一言・スマホ 390 で 44px・はみ出さない
 * Playwright か Chromium が無い = 失敗 (exit 1)。飛ばすのは MASTER_EDIT_UI_SKIP=1 を付けたときだけ (#1589 Codex R2 M4 = 成功と見分けがつかないので黙って飛ばさない)
 * 使い方: node scripts/test-master-edit-ui.mjs
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import http from 'node:http';
import express from 'express';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
// 広げる道 PR-2: 画面は DB の active に従う。試験の DB は全部の列を company にする = このコードの能力も全部 (code_behind の試験だけ戻す)
const W2 = await import('./fixtures/master-widen-pr1.mjs');
const OG = await import('../lib/master-owner-gate.mjs');
OG.__setCapableForTest((await import('../config/master-ownership.mjs')).OWNED_COLUMNS);

if (process.env.MASTER_EDIT_UI_SKIP === '1') { console.log('⏭️ MASTER_EDIT_UI_SKIP=1 = 画面の JS の試験 (test-master-edit-ui) を飛ばす'); process.exit(0); }
let chromium;
let browser;
try {
  ({ chromium } = await import('playwright'));
  browser = await chromium.launch();
} catch (e) {
  console.error(`NG 画面の JS の試験を動かせない (Playwright / Chromium): ${e.message}\n   入れる = npx playwright install chromium / この試験だけ飛ばす = MASTER_EDIT_UI_SKIP=1`);
  process.exit(1);
}

const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const { createRoles } = await import('./company-db/create-watch-roles.mjs');
const { createMasterEditRoles } = await import('./company-db/create-master-edit-roles.mjs');
const { runInitialLoad } = await import('../apps/company-db/load/engine.mjs');
// 試験の基準 = 切替前の持ち主表 (全部 load)。⑤-3b の PR から config/master-ownership.mjs (configured) は 10/5 の 13 キーが company = 基準にしない
const MASTER_OWNERSHIP = Object.freeze(Object.fromEntries((await import('../config/master-ownership.mjs')).OWNED_COLUMNS.map((k) => [k, 'load'])));
const W = await import('../lib/master-write.mjs');
const C = await import('../lib/master-cutover.mjs');
const { seedActiveEpoch } = await import('./fixtures/master-epoch.mjs');
// 発注アプリの台帳・ロジザードの写し (warehouse-mirror.db) = 使い捨ての DATA_DIR (router を読み込む前に。warehouse-mirror/db.js は読み込んだ時に DATA_DIR を決める)
const DATA_TMP = fs.mkdtempSync(path.join(os.tmpdir(), 'meux-ui-'));
process.env.DATA_DIR = DATA_TMP;
const { default: router, __setPgClientFactory, __setShippingRatesProvider } = await import('../apps/master-edit/router.mjs');
const { __clearStockCache } = await import('../apps/master-edit/extras.mjs');
const { __clearSearchTokens } = await import('../apps/master-edit/search-token.mjs');

const quiet = () => {};
const ALL = Object.fromEntries(Object.keys(MASTER_OWNERSHIP).map((k) => [k, 'company']));
const RATES = new Map([['S01', { method: 'ゆうパケット', cost: 210 }], ['S02', { method: '宅急便', cost: 520 }]]);
const MANIFEST = { entries: [{ id: 'warehouse.register.post', kind: 'code' }, { id: 'ne.product_screen', kind: 'manual' }] };
const BUILDS = { render: ['r1'], minipc: ['m1'] };

// ── Company DB (PGlite) ──
const pg = new PGlite();
const sessionUser = (await pg.query('select session_user::text as u')).rows[0].u;
await pg.query(`create role deploy with createrole nocreatedb nosuperuser login password 'd'`);
await pg.query(`alter database ${(await pg.query('select current_database() as d')).rows[0].d} owner to deploy`);
await pg.query('set role deploy');
const db = pgliteAdapter(pg);
await applyMigrations(db, { log: quiet });
await createRoles(pg, { watcherPw: 'a', writerPw: 'b' });
await createMasterEditRoles(pg, {});
await W2.useReal0058(pg, { leases: ['single', 'set'], futureSetLease: true });   // 広げる道 PR-2: 本物の 0058 の上で試験の許可を置く (この DB は構成も C = セットの許可は将来の形)
const sku = (code, name, kind, taxRate, salesClass, cost) => ({
  code, name, kind, taxRate, taxClass: taxRate === 0.08 ? 'REDUCED_8' : 'STANDARD_10', handling: 'active', salesClass,
  cost: { jpy: cost, source: kind === 'set' ? 'set_calc' : 'ne', status: 'COMPLETE' }, standardPriceJpy: 1000, shippingCode: 'S02', shippingMethod: '宅急便', shippingCostJpy: 520, reorderMonths: 2,
});
const singles = [sku('s001', '単品 1', 'single', 0.1, 3, 100), sku('s002', '単品 2 (長い名前の試験: 国産 有機 シリコン 保存袋 Sサイズ 3枚入 まとめ買い 12 個セット)', 'single', 0.1, 3, 200), sku('s003', '単品 3', 'single', 0.08, 1, 50),
  // 長い一覧 (見出しの行を上に残す試験) と かなの同一視の試験の名前 (10/5)
  sku('k001', '国産 はちみつ 500g', 'single', 0.08, 1, 300), sku('k002', 'ハチミツ レモン', 'single', 0.08, 1, 310), sku('k003', 'ﾊﾁﾐﾂ ｷｬﾝﾃﾞｨ', 'single', 0.08, 1, 120),
  sku('k004', 'ＡＢＣ 保存袋', 'single', 0.1, 3, 90), sku('k005', 'abc 小袋', 'single', 0.1, 3, 80), sku('k006', 'みかん', 'single', 0.08, 1, 70),
  ...Array.from({ length: 40 }, (_, i) => sku(`z${String(i).padStart(3, '0')}`, `一覧を長くする単品 ${i}`, 'single', 0.1, 3, 100 + i))];
const lr = await runInitialLoad(db, {
  skus: [...singles, sku('set001', 'セット 1', 'set', 0.1, null, 300)], variationGroups: [{ code: 'k001', name: '国産 はちみつ', childCodes: ['k002'], status: 'active' }],
  setComponents: [{ parentCode: 'set001', childCode: 's001', qty: 1, source: 'ne' }, { parentCode: 'set001', childCode: 's002', qty: 1, source: 'ne' }],
  listings: [], observations: [], physicals: [], compliance: [], workers: [],
  suppliers: [{ code: '0001', name: 'AMC' }], supplierSkus: singles.map((s) => ({ supplierCode: '0001', skuCode: s.code })), primarySuppliers: singles.map((s) => ({ skuCode: s.code, supplierCode: '0001' })),
  reorder: { available: true, runId: 'pml_ui' },
}, { log: quiet, runId: 'load_ui', now: new Date(Date.now() - 5 * 86400e3) });
assert.equal(lr.ok, true, lr.error);
async function as(role, fn) { await pg.query(`set role ${role}`); try { return await fn(); } finally { await pg.query('set role deploy'); } }
async function asGate(host, fn) { await pg.query(`set session authorization master_gate_${host}`); try { return await fn(); } finally { await pg.query(`set session authorization ${sessionUser}`); await pg.query('set role deploy'); } }
async function toPhase(to) {
  const seen = { frozen: 'legacy_open', company_owner: 'frozen', new_open: 'company_owner' }[to];
  const own = to === 'frozen' ? MASTER_OWNERSHIP : ALL;
  for (const [host, inst, buildId] of [['render', 'r-a', 'r1'], ['minipc', 'm-a', 'm1']]) await asGate(host, () => C.recordLegacyGateAck(db, { host, instanceId: inst, buildId, manifest: MANIFEST, ownership: own, phaseSeen: seen }));
  const mh = await C.manifestHashOf(db, MANIFEST);
  if (to !== 'frozen') await seedActiveEpoch(db, ALL);
  const now = new Date().toISOString();
  const evidence = to === 'frozen'
    ? { expected_builds: BUILDS, manifest_hash: mh, owner_hash: C.ownershipHash(MASTER_OWNERSHIP), manual_entries_stopped: [{ id: 'ne.product_screen', by: 'naka@test', at: now }], drain: { done: true, checked_by: 'naka@test', checked_at: now } }
    : { expected_builds: BUILDS, manifest_hash: mh, owner_hash: C.ownershipHash(ALL) };
  return as('master_ops', () => C.advanceCutoverPhase(db, { to, actor: 'naka@test', evidence }));
}
await toPhase('frozen');
const bp = (await pg.query('select * from ops.registration_backfill_plan()')).rows[0];
await as('master_ops', () => pg.query('select ops.backfill_sku_registrations($1, $2, $3)', [bp.sku_count, bp.snapshot_hash, 'naka@test']));
await toPhase('company_owner');
await toPhase('new_open');
await (await import('./fixtures/master-widen.mjs')).seedNewEntryLease(db, { withSet: true });   // 0058: 単品の新商品は DB の関数が開放の許可を確かめる = 試験の許可を置く (PR-2: セットの画面は JS の門がセットの許可も見る = 将来の形の許可も)
// ── 発注アプリの台帳 (注文残) とロジザードの写し (在庫) の見本 (PR2) ──
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
const mirrorDb = initMirrorDB();
const { initPurchaseOrders } = await import('../apps/purchase-orders/db.js');
initPurchaseOrders();
{
  const now = new Date().toISOString();
  mirrorDb.prepare(`insert into po_settings (key, value, effective_at) values ('tracking_started_at', '2026-07-13T00:00:00.000Z', ?)`).run(now);
  const po = (sup, name, issuedAt, poNo, items) => {
    const id = mirrorDb.prepare(`insert into po_orders (supplier_code, supplier_name, status, created_at, updated_at) values (?, ?, 'draft', ?, ?)`).run(sup, name, now, now).lastInsertRowid;
    const ids = items.map((it) => mirrorDb.prepare('insert into po_order_items (order_id, product_code, product_key, product_name, qty, promised_date) values (?, ?, ?, ?, ?, ?)')
      .run(id, it.code, it.code.trim().toLowerCase(), it.code, it.qty, it.promised || null).lastInsertRowid);
    mirrorDb.prepare(`update po_orders set status = 'issued', issued_at = ?, po_number = ?, tracking_mode = 'tracked' where id = ?`).run(issuedAt, poNo, id);
    return ids;
  };
  const [k001Item] = po('0001', 'AMC', '2026-09-20T01:00:00.000Z', 'PO-2026-0001', [{ code: 'K001', qty: 10 }, { code: 's002', qty: 4, promised: '2026-10-20' }]);
  // 一部取消 3 = 残 7
  mirrorDb.prepare(`insert into po_item_events (order_item_id, event_type, qty, effective_date, recorded_at, actor_type) values (?, 'cancel', 3, '2026-09-25', '2026-09-25T00:00:00.000Z', 'user')`).run(k001Item);
  po('0001', 'AMC', '2020-01-01T00:00:00.000Z', 'PO-2020-0001', [{ code: 'k003', qty: 50 }]);   // 境界より前 = 数えない
  const lz = mirrorDb.prepare(`insert into mirror_logizard_stock (商品ID, 商品名, ブロック略称, ロケ, 品質区分名, 在庫数, 引当数, captured_at, synced_at) values (?, ?, ?, ?, ?, ?, 0, ?, ?)`);
  const cap = new Date(Date.now() - 20 * 60e3).toISOString();
  for (const [code, block, loke, q, n] of [['K001', 'P', 'A-01', '良品', 12], ['k001', 'P', 'A-02', '不良', 3], ['s001', 'P', 'B-01', '良品', 8], ['S001', 'R', 'Z-01', '良品', 2], ['s002', 'P', 'B-02', '良品', 5]]) lz.run(code, code, block, loke, q, n, cap, now);
}
// ── FBA (JP) の在庫の日次の見本 (Company DB・10/5): k001 × 1 の出品 2 つ・k001 × 3 (まとめ売り)・k001 + k002 (セットの出品)・k006 × 1 (在庫 0)。k002 だけの出品は無い ──
const { ingestStockDay } = await import('../apps/company-db/ingest/stock-daily.mjs');
const FBA_CAPTURED = new Date(Date.now() - 3 * 3600e3).toISOString();
{
  const sid = async (code) => Number((await pg.query('select sku_id from core.skus where code = $1', [code])).rows[0].sku_id);
  const k1 = await sid('k001'), k2 = await sid('k002'), k6 = await sid('k006');
  const listing = async (code, comps) => {
    const id = (await pg.query(`insert into core.listings (company_id, mall, shop_code, listing_code, status) values (1, 'amazon', 'main@A1VC38T7YXB528', $1, 'active') returning listing_id`, [code])).rows[0].listing_id;
    let i = 0;
    for (const [skuId, qty] of comps) await pg.query(`insert into core.listing_components (company_id, listing_id, sku_id, qty, sort_order, resolution, resolved_by_type) values (1, $1, $2, $3, $4, 'exact', 'system')`, [id, skuId, qty, i++]);
  };
  await listing('pr-k001', [[k1, 1]]); await listing('pr-k001-b', [[k1, 1]]); await listing('pr-k001-3p', [[k1, 3]]); await listing('pr-k1k2', [[k1, 1], [k2, 1]]); await listing('pr-k006', [[k6, 1]]);
  const r = (code, a, x, pr, c, w = 0) => ({ code, fba_available: a, fba_fc_transfer: x, fba_fc_processing: pr, fba_customer_order: c, fba_inbound_working: w, fba_inbound_shipped: 0, fba_inbound_received: 0 });
  const today = new Date(Date.now() + 9 * 3600e3).toISOString().slice(0, 10);
  const got = await ingestStockDay(db, { source: 'fba_jp', snapshot_date: today, captured_at: FBA_CAPTURED,
    rows: [r('pr-k001', 20, 1, 0, 2, 5), r('pr-k001-b', 4, 0, 1, 0), r('pr-k001-3p', 6, 0, 0, 0), r('pr-k1k2', 3, 0, 0, 0), r('pr-k006', 0, 0, 0, 0)] }, { todayJst: today });
  assert.equal(got.day_status, 'complete');
}
const row = async (code) => (await pg.query('select s.name, s.reorder_months::float8 as months, s.handling from core.skus s where s.code = $1', [code])).rows[0];
/** 画面の外で同じ商品を直す (その間の変更 = 画面の編集の印が古くなる) */
async function changeBehind(code, values) {
  const id = (await pg.query('select sku_id::text as id from core.skus where code = $1', [code])).rows[0].id;
  const token = W.editTokenOf(await W.readCurrent(db, id, W.jstDate(new Date())));
  return as('master_edit', () => W.saveSku(db, { actor: 'other@test', requestId: crypto.randomUUID(), code, reason: '画面の外で', seen: { token }, values }, { ownership: ALL, open: true, shippingRates: RATES }));
}

// ── 本物の router ──
process.env.COMPANY_DB_URL = 'postgres://owner@localhost/ui';
process.env.COMPANY_DB_MASTER_EDIT_URL = 'postgres://master_edit@localhost/ui';
process.env.MASTER_EDITORS = 'naka@test';
process.env.MASTER_EDIT_OPEN = '1';
let chain = Promise.resolve();
__setPgClientFactory(async (url) => {
  // 1 つの PGlite を順番に使う (同時の要求でロールが混ざらない)
  let release; const prev = chain; chain = new Promise((r) => { release = r; }); await prev;
  await pg.query(`set role ${/master_edit@/.test(url) ? 'master_edit' : 'deploy'}`);
  return { query: (t, p) => pg.query(t, p), end: async () => { await pg.query('set role deploy'); release(); }, on: () => {} };
});
// 持ち主表 = DB の active (広げる道 PR-2)
__setShippingRatesProvider(async () => RATES);
const app = express();
app.set('view engine', 'ejs');
// 利用権 (試験の途中で変える: 発注アプリの利用権が無い人には注文残を出さない = #1620 Codex R1 M3)
let SESSION_APPS = ['master-edit', 'purchase-orders'];
app.use((req, res, next) => { req.session = { authenticated: true, email: 'naka@test', displayName: '中原', role: 'user', allowedApps: SESSION_APPS }; next(); });
app.use('/apps/master-edit', router);
// マスタの判断 (左の列から行く・同じ master-edit.css を読む = [67] スマホ 390 の幅で開く)。同じ PGlite を順番に使う (master-edit と同じ鎖)
const { default: decisionsRouter, __setPgClientFactory: __setDecisionsPgClientFactory } = await import('../apps/master-decisions/router.mjs');
__setDecisionsPgClientFactory(async () => {
  let release; const prev = chain; chain = new Promise((r) => { release = r; }); await prev;
  await pg.query('set role deploy');
  return { query: (t, p) => pg.query(t, p), end: async () => { release(); }, on: () => {} };
});
app.use('/apps/master-decisions', decisionsRouter);
app.get('/', (req, res) => res.send('<p>portal</p>'));
const server = http.createServer(app);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const B = `http://127.0.0.1:${server.address().port}/apps/master-edit`;

let passed = 0;
async function ta(name, fn, viewport = { width: 1440, height: 900 }, scale = 1) {
  if (process.env.MASTER_EDIT_UI_ONLY && !name.includes(process.env.MASTER_EDIT_UI_ONLY)) return;   // 1 つだけ流す (目で見る・直すとき)。CI では付けない
  const ctx = await browser.newContext({ viewport, deviceScaleFactor: scale, locale: 'ja-JP' });
  const p = await ctx.newPage();
  const errors = [];
  p.on('pageerror', (e) => errors.push(e.message));
  try {
    await fn(p, ctx);
    assert.deepEqual(errors, [], '画面の JS の例外');
    passed++; console.log(`  ok  ${name}`);
  } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; }
  await ctx.close();
}
const dirty = (p) => p.evaluate(() => window.MasterEdit.dirty().n);
const active = (p) => p.evaluate(() => document.activeElement && (document.activeElement.id || document.activeElement.getAttribute('aria-label') || document.activeElement.tagName));

await ta('[1] 未保存の数: 構成の見せ方・保存の理由は 0 件、保存する欄を変えると 1 件 (保存のボタンもそれに合わせる)', async (p) => {
  await p.goto(B + '/sku/set001');
  assert.ok((await p.content()).includes('セット 1'), '描けている');
  await p.click('#set-mode button[data-v="edit"]');
  await p.click('#set-mode button[data-v="now"]');
  await p.fill('#reason', '理由だけ入れた');
  assert.equal(await dirty(p), 0);
  assert.equal(await p.isDisabled('#save'), true);
  assert.equal(await p.isHidden('#unsaved'), true);
  await p.click('#name-edit');   // 名前は見出しのところで直す (10/6)
  await p.fill('#f-name', 'セット 1 改');
  assert.equal(await dirty(p), 1);
  assert.equal(await p.isDisabled('#save'), false);
  assert.match(await p.textContent('#unsaved'), /未保存 1 件/);
  await p.fill('#f-name', 'セット 1');
  assert.equal(await dirty(p), 0);
});

await ta('[2] 離れるときの確認: 左の列のリンク (ここに残る = フォーカスが戻る)・全体から探す・ブラウザの戻る (捨てて移る)', async (p) => {
  await p.goto(B + '/');
  await Promise.all([p.waitForNavigation(), p.click('a.rowlink:has-text("s002")')]);
  await p.fill('#f-reorder_months', '4');
  await p.focus('.rail a[aria-label="つかいかた"]');
  await p.keyboard.press('Enter');
  assert.equal(await p.locator('#leave-bg.on').count(), 1);
  assert.match(await p.textContent('#leave-list'), /推奨保有月数/);
  assert.equal(await active(p), 'leave-stay');
  for (let i = 0; i < 3; i++) await p.keyboard.press('Tab');
  assert.equal(await active(p), 'leave-stay', 'Tab は窓の中で回る');
  await p.click('#leave-stay');
  assert.equal(await active(p), 'つかいかた', '「ここに残る」で押した所へ戻る');
  assert.match(p.url(), /sku\/s002$/);
  await p.keyboard.press('Control+k');
  await p.keyboard.type('つかいかた');
  await p.keyboard.press('Enter');
  assert.equal(await p.locator('#leave-bg.on').count(), 1, '全体から探すからの移動も聞く');
  await p.click('#leave-stay');
  await p.goBack();
  await p.waitForTimeout(200);
  assert.equal(await p.locator('#leave-bg.on').count(), 1, 'ブラウザの戻るも聞く');
  assert.match(p.url(), /sku\/s002$/);
  await Promise.all([p.waitForNavigation(), p.click('#leave-drop')]);
  assert.match(p.url(), /master-edit\/$/);
  assert.equal((await row('s002')).months, 2, '捨てたので保存していない');
});

await ta('[3] Ctrl+S で保存 (DB に入る)・保存の後は離れても聞かない・/ は絞る欄の無い画面では何もしない', async (p) => {
  await p.goto(B + '/sku/s003');
  await p.keyboard.press('/');
  assert.equal(await p.locator('#palette-bg.on').count(), 0);
  await p.fill('#f-reorder_months', '5');
  await p.fill('#reason', 'Ctrl+S の試験');
  await p.keyboard.press('Control+s');
  await p.waitForSelector('.result.ok');
  assert.equal((await row('s003')).months, 5);
  assert.equal(await dirty(p), 0);
  await Promise.all([p.waitForNavigation(), p.click('.rail a[aria-label="つかいかた"]')]);
});

await ta('[4] 通信が切れた保存の押し直しは同じ request_id (2 回入らない)', async (p) => {
  await p.goto(B + '/sku/s001');
  const bodies = [];
  let first = true;
  await p.route('**/api/sku/s001', async (route) => {
    bodies.push(JSON.parse(route.request().postData()));
    if (first) { first = false; await route.abort('connectionreset'); } else await route.continue();
  });
  await p.fill('#f-reorder_months', '3');
  await p.click('#save');
  await p.waitForSelector('#msg.err');
  assert.match(await p.textContent('#msg'), /通信できませんでした/);
  assert.equal(await p.isDisabled('#save'), false);
  await p.click('#save');
  await p.waitForSelector('.result.ok');
  assert.equal(bodies.length, 2);
  assert.equal(bodies[0].request_id, bodies[1].request_id);
  assert.match(bodies[0].request_id, /^[0-9a-f-]{36}$/);
  assert.equal((await row('s001')).months, 3);
});

await ta('[5] 開き直しが要る 409 (その間の変更) の後は、欄や理由を触っても保存のボタンが戻らない・Ctrl+S も保存しない', async (p) => {
  await p.goto(B + '/sku/s002');
  await changeBehind('s002', { name: '画面の外で直した' });
  await p.fill('#f-reorder_months', '6');
  await p.click('#save');
  await p.waitForSelector('.result.err');
  assert.match(await p.textContent('.result.err'), /その間の変更/);
  assert.equal(await p.locator('#reload').count(), 1);
  assert.equal(await p.isDisabled('#save'), true);
  await p.fill('#reason', '触った');
  await p.fill('#f-reorder_months', '7');
  assert.equal(await p.isDisabled('#save'), true, '欄を触っても戻らない');
  await p.keyboard.press('Control+s');
  await p.waitForTimeout(200);
  assert.equal((await row('s002')).months, 2, '保存していない');
  await Promise.all([p.waitForNavigation(), p.click('#reload')]);
  assert.match(await p.inputValue('#f-name'), /画面の外で直した/);
});

await ta('[6] 構成の行を並べ替えると、読み上げの名前が今の行番号・コードになる', async (p) => {
  await p.goto(B + '/sku/set001');
  await p.click('#set-mode button[data-v="edit"]');
  const rows = p.locator('#comp-rows tr.comp-row');
  assert.equal(await rows.nth(1).locator('.c-qty').getAttribute('aria-label'), '2 行目 (s002) の数');
  await rows.nth(1).locator('button[data-act="up"]').click();
  assert.equal(await rows.nth(0).locator('.c-qty').getAttribute('aria-label'), '1 行目 (s002) の数');
  assert.equal(await rows.nth(0).locator('button[data-act="del"]').getAttribute('aria-label'), '1 行目 (s002) を外す');
  assert.equal(await rows.nth(1).locator('button[data-act="up"]').getAttribute('aria-label'), '2 行目 (s001) を上へ');
  assert.equal(await dirty(p), 1, '並びも依頼の中身');
});

const jan13 = (b) => { const d = b.split('').map(Number).reverse(); const sum = d.reduce((a, x, i) => a + x * (i % 2 === 0 ? 3 : 1), 0); return b + ((10 - (sum % 10)) % 10); };
const jansOf = async (code) => (await pg.query(`select e.external_value as v from core.external_ids e join core.skus s on s.product_id = e.entity_id
   where s.code = $1 and e.entity_type = 'product' and e.system = 'jan' and e.valid_to is null order by 1`, [code])).rows.map((r) => r.v);

await ta('[8] JAN の欄に打ったまま (Enter なし) ほかの欄も変えて Ctrl+S = JAN も保存する / 形の違う JAN なら何も保存しない', async (p) => {
  const good = jan13('490000000777');
  await p.goto(B + '/sku/s003');
  await p.fill('#f-reorder_months', '7');
  await p.fill('#jan-in', '1234567');
  assert.equal(await dirty(p), 2, '打ったままの JAN も未保存に数える');
  await p.keyboard.press('Control+s');
  await p.waitForTimeout(300);
  assert.match(await p.textContent('#msg'), /JAN の欄を直してから保存/);
  assert.equal(await active(p), 'jan-in');
  assert.equal((await row('s003')).months, 5, '形の違う JAN のときは何も保存しない');
  await p.fill('#jan-in', good);
  await p.keyboard.press('Control+s');
  await p.waitForSelector('.result.ok');
  assert.equal((await row('s003')).months, 7);
  assert.deepEqual(await jansOf('s003'), [good], 'JAN も保存した');
  assert.equal(await dirty(p), 0);
});

/** 試験だけ: 登録の状態を直に変える (本番は ops.transition_sku_registration の決まった進み方だけ。表の守りの印を同じ取引の中で立てる) */
async function setReg(code, state) {
  await pg.query('begin');
  try {
    await pg.query("select pg_catalog.set_config('ops.registration_protocol', '1', true)");
    await pg.query('update ops.master_registrations set state = $2, state_changed_at = now() where sku_id = (select sku_id from core.skus where code = $1)', [code, state]);
    await pg.query('commit');
  } catch (e) { await pg.query('rollback'); throw e; }
}

await ta('[9] 開いた後に登録をやめた = cancelled_sku は開き直しが要る / やめた商品は初めから見るだけ (帯・保存のボタンなし)', async (p) => {
  await p.goto(B + '/sku/s001');
  await setReg('s001', 'cancelled');
  try {
    await p.fill('#f-reorder_months', '9');
    await p.click('#save');
    await p.waitForSelector('.result.err');
    assert.equal(await p.locator('#reload').count(), 1, '開き直すボタン');
    await p.fill('#reason', '触った');
    assert.equal(await p.isDisabled('#save'), true, '欄を触っても保存のボタンが戻らない');
    await Promise.all([p.waitForNavigation(), p.click('#reload')]);
    assert.equal(await p.locator('#cancelled-band').count(), 1, 'やめた = 理由の帯');
    assert.equal(await p.locator('#save').count(), 0, 'やめた = 保存のボタンを出さない');
    assert.equal(await p.locator('#f-name').count(), 0, 'やめた = 入力欄を出さない');
  } finally {
    await setReg('s001', 'available');
  }
});

await ta('[10] 先の日付の原価 (使っているセット) = 原価の欄だけ閉じる・ほかの欄は保存できる', async (p) => {
  const fut = new Date(Date.now() + 40 * 86400e3 + 9 * 3600e3).toISOString().slice(0, 10);
  await pg.query("update core.sku_costs set valid_to = $1::date - 1 where valid_to is null and sku_id = (select sku_id from core.skus where code = 'set001')", [fut]);
  await pg.query("insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from) select 1, sku_id, 999, 'set_calc', 'COMPLETE', $1::date from core.skus where code = 'set001'", [fut]);
  try {
    await p.goto(B + '/sku/s002');
    assert.equal(await p.locator('#btn-cost-open').count(), 0, '原価を変えるボタンを出さない');
    assert.match(await p.textContent('#cost-future'), /set001/);
    await p.fill('#f-reorder_months', '8');
    await p.click('#save');
    await p.waitForSelector('.result.ok');
    assert.equal((await row('s002')).months, 8);
  } finally {
    await pg.query("delete from core.sku_costs where valid_from = $1::date and sku_id = (select sku_id from core.skus where code = 'set001')", [fut]);
    await pg.query("update core.sku_costs set valid_to = null where valid_to = $1::date - 1 and sku_id = (select sku_id from core.skus where code = 'set001')", [fut]);
  }
});

await ta('[11] 保存が通ったら画面を読み直す (10/5): 読み直すまでは打てない・欄は保存した後の値・上に知らせが 1 回・続けて別の欄を直して保存できる・戻る 1 回で一覧へ', async (p) => {
  await p.goto(B + '/');
  await Promise.all([p.waitForNavigation(), p.click('a.rowlink:has-text("s003")')]);
  await p.fill('#f-reorder_months', '6');
  // 読み直しを一度止めて (ME.reloadPage を差し替え)、読み直すまでは入力の場所が閉じていることを見る
  await p.evaluate(() => { window.__reload = window.MasterEdit.reloadPage; window.MasterEdit.reloadPage = () => { window.__reloadAsked = true; }; });
  await p.click('#save');
  await p.waitForFunction(() => window.__reloadAsked === true);
  assert.match(await p.textContent('#msg'), /読み直しています/);
  for (const sel of ['#f-name', '#f-reorder_months', '#jan-in', '#reason', '.handling-top .seg button']) assert.equal(await p.locator(sel).first().isDisabled(), true, `${sel} は読み直すまで閉じる`);
  assert.equal(await p.evaluate(() => document.getElementById('f').firstElementChild.hasAttribute('inert')), true);
  assert.equal(await dirty(p), 0, '保存した = 未保存 0 (離れても聞かない)');
  await p.evaluate(() => window.__reload());
  await p.waitForSelector('#saved-note');
  assert.equal(await p.inputValue('#f-reorder_months'), '6', '欄は保存した後の値');
  assert.match(await p.textContent('#saved-note'), /保存しました[\s\S]*推奨保有月数/);
  assert.match(await p.textContent('#toast-t'), /保存しました \(推奨保有月数/);
  assert.equal((await row('s003')).months, 6);
  assert.equal(await dirty(p), 0);
  assert.equal(await p.isDisabled('#f-name'), false, '読み直した後は打てる');
  await p.reload();
  assert.equal(await p.locator('#saved-note').count(), 0, '知らせは 1 回だけ');
  // 続けて直す (読み直しで新しい編集の印)
  await p.click('#name-edit');
  await p.fill('#f-name', '単品 3 改');
  await p.keyboard.press('Control+s');
  await p.waitForSelector('#saved-note');
  assert.equal(await p.inputValue('#f-name'), '単品 3 改');
  assert.equal(await p.textContent('h1'), '単品 3 改', '見出しも保存した後の値');
  assert.equal((await row('s003')).name, '単品 3 改');
  await p.goBack();
  await p.waitForSelector('#list-tbl');
  assert.match(p.url(), /master-edit\/$/, '戻る 1 回で一覧 (同じ画面が履歴に 2 つ並ばない)');
});

await ta('[12] 変わった項目が無い保存 (6 → 6.0) の後は未保存が残らない (保存のボタン・離れるときの確認も)', async (p) => {
  await p.goto(B + '/sku/s003');
  await p.fill('#f-reorder_months', '6.0');
  assert.equal(await dirty(p), 1);
  await p.click('#save');
  await p.waitForSelector('.result');
  assert.match(await p.textContent('.result'), /変わった項目がありません/);
  assert.equal(await dirty(p), 0);
  assert.equal(await p.isDisabled('#save'), true);
  await Promise.all([p.waitForNavigation(), p.click('.rail a[aria-label="つかいかた"]')]);
});

await ta('[13] 登録をやめた商品は、カードの操作 (もう一度作る) も出さない', async (p) => {
  const MR = await import('../lib/master-register.mjs');
  await as('master_edit', () => MR.registerNewSku(db, { actor: 'naka@test', requestId: crypto.randomUUID(), kind: 'single', code: 'ui-card-1',
    values: { name: 'カードの試験', standard_price: '1500', shipping_code: 'S01', tax_rate: '10', primary_supplier: '0001', sales_class: '3', expiry_managed: '0', reorder_months: '1' }, card: { create: true } },
  { ownership: ALL, open: true, shippingRates: RATES }));
  await p.goto(B + '/sku/ui-card-1');
  assert.equal(await p.locator('#card-retry').count(), 1, '下書きのうちは出す (名簿の人)');
  const id = (await pg.query("select sku_id::text as id from core.skus where code = 'ui-card-1'")).rows[0].id;
  await pg.query('select ops.transition_sku_registration($1, $2, $3, $4, $5)', [id, 'cancelled', 'human', 'naka@test', '試験でやめた']);
  await p.goto(B + '/sku/ui-card-1');
  assert.equal(await p.locator('#cancelled-band').count(), 1);
  assert.equal(await p.locator('#card-retry').count(), 0, 'やめた商品にカードの操作を出さない');
  assert.equal(await p.locator('#card-link').count(), 0);
});

const listCodes = (p) => p.$$eval('#list-tbl a.rowlink', (as) => as.map((a) => a.textContent.trim()));

await ta('[14] 絞る欄の Enter (10/5): 絞る欄で Enter = 絞る (URL の q)・IME の変換を確かめる Enter では送らない・↓ で表へ移った後の Enter = その行を開く・パンくずで同じ絞り込みの一覧へ戻る', async (p) => {
  await p.goto(B + '/');
  // 作った keydown (ブラウザ自身は送らない) で、画面の JS の決まりそのものを見る: 変換中 = 送らない / 変換でない = 送る
  await p.focus('#q');
  await p.fill('#q', 'みかん');
  await p.evaluate(() => document.getElementById('q').dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', isComposing: true, keyCode: 229, bubbles: true, cancelable: true })));
  await p.waitForTimeout(300);
  assert.doesNotMatch(p.url(), /q=/, '変換を確かめる Enter (isComposing) では送らない');
  await p.evaluate(() => document.getElementById('q').dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', keyCode: 229, bubbles: true, cancelable: true })));
  await p.waitForTimeout(300);
  assert.doesNotMatch(p.url(), /q=/, 'keyCode 229 (IME が受けた Enter) では送らない');
  await Promise.all([p.waitForNavigation(), p.evaluate(() => document.getElementById('q').dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', keyCode: 13, bubbles: true, cancelable: true })))]);
  assert.equal(new URL(p.url()).searchParams.get('q'), 'みかん', '画面の JS の Enter で送る');
  assert.deepEqual(await listCodes(p), ['k006']);
  // 本物の IME (CDP): 変換中の Enter は字が確かまるだけ → もう一度 Enter で絞る
  await p.goto(B + '/');
  await p.click('#q');
  const cdp = await p.context().newCDPSession(p);
  await cdp.send('Input.imeSetComposition', { text: 'はちみつ', selectionStart: 4, selectionEnd: 4 });
  await cdp.send('Input.dispatchKeyEvent', { type: 'rawKeyDown', key: 'Enter', code: 'Enter', windowsVirtualKeyCode: 229, nativeVirtualKeyCode: 229 });
  await cdp.send('Input.insertText', { text: 'はちみつ' });
  await p.waitForTimeout(300);
  assert.doesNotMatch(p.url(), /q=/, 'IME の変換を確かめる Enter では送らない');
  assert.equal(await p.inputValue('#q'), 'はちみつ');
  await Promise.all([p.waitForNavigation(), p.keyboard.press('Enter')]);
  assert.equal(new URL(p.url()).searchParams.get('q'), 'はちみつ');
  assert.deepEqual(await listCodes(p), ['k001', 'k002', 'k003']);
  // ↓ で表の 1 行目へ・Enter = その行を開く (絞る欄の外)
  await p.focus('#q');
  await p.keyboard.press('ArrowDown');
  assert.equal(await p.evaluate(() => document.activeElement.textContent.trim()), 'k001');
  await Promise.all([p.waitForNavigation(), p.keyboard.press('Enter')]);
  assert.match(p.url(), /sku\/k001$/);
  // パンくずの「商品・セット」= さっきの絞り込みの一覧へ
  assert.equal(new URL(await p.locator('.crumb a').first().evaluate((a) => a.href)).searchParams.get('q'), 'はちみつ');
  await Promise.all([p.waitForNavigation(), p.click('.crumb a')]);
  assert.deepEqual(await listCodes(p), ['k001', 'k002', 'k003']);
});

await ta('[15] かなの同一視 (10/5): ひらがな・カタカナ・半角カナ・全角英数・大文字小文字が同じ商品に当たる (一覧・全体から探す)・画面とサーバーの決まりが同じ', async (p) => {
  for (const q of ['はちみつ', 'ハチミツ', 'ﾊﾁﾐﾂ', 'ﾊﾁみつ']) {
    await p.goto(B + '/?q=' + encodeURIComponent(q));
    assert.deepEqual(await listCodes(p), ['k001', 'k002', 'k003'], q);
  }
  for (const q of ['ＡＢＣ', 'abc', 'ABC', 'Ａｂｃ']) {
    await p.goto(B + '/?q=' + encodeURIComponent(q));
    assert.deepEqual(await listCodes(p), ['k004', 'k005'], q);
  }
  // 全体から探す (Ctrl+K) の「〜で商品・セットを絞る」も同じ一覧
  await p.goto(B + '/sku/s001');
  await p.keyboard.press('Control+k');
  await p.keyboard.type('ﾊﾁﾐﾂ');
  await Promise.all([p.waitForNavigation(), p.keyboard.press('Enter')]);
  assert.deepEqual(await listCodes(p), ['k001', 'k002', 'k003']);
  // 操作の候補もかなを同じに見る (「ツカイカタ」で「つかいかた」)
  await p.keyboard.press('Control+k');
  await p.keyboard.type('ツカイカタ');
  assert.match(await p.textContent('#pal-res'), /つかいかた/);
  await p.keyboard.press('Escape');
  // 画面の JS (ME.fold) とサーバー (search-fold.mjs) が同じ答え
  const { foldSearch } = await import('../apps/master-edit/search-fold.mjs');
  const samples = ['はちみつ', 'ﾊﾁﾐﾂ', 'ｶﾞｷﾞｸﾞ', 'ＡＢＣ１２３', 'ゔゝゞぁゖ', 'ラーメン', 'ｧｨｩ', 'Ｍｉｘ ミックス みっくす'];
  assert.deepEqual(await p.evaluate((xs) => xs.map((x) => window.MasterEdit.fold(x)), samples), samples.map(foldSearch));
});

await ta('[16] 原価を変える理由 (10/5): 既定 = メーカーからの値上げ通知で保存できる・その他で空なら保存できない・その他に書いた文が記録に残る', async (p) => {
  const costReason = async (code) => (await pg.query('select c.reason from core.sku_costs c join core.skus s on s.sku_id = c.sku_id where s.code = $1 and c.valid_to is null', [code])).rows[0].reason;
  await p.goto(B + '/sku/k006');
  await p.click('#btn-cost-open');
  assert.equal(await p.isChecked('input[name="cost-reason-pick"][value="メーカーからの値上げ通知"]'), true, '最初から選ばれている');
  assert.equal(await p.isHidden('#cost-reason'), true, 'その他の欄は隠れている');
  await p.fill('#cost-jpy', '75');
  assert.equal(await dirty(p), 1, '理由の選択は未保存に数えない (原価だけ)');
  await p.click('#save');
  await p.waitForSelector('#saved-note');
  assert.equal(await costReason('k006'), 'メーカーからの値上げ通知');
  assert.match(await p.textContent('#sku-page'), /メーカーからの値上げ通知/, '原価の履歴に出る');
  // その他 = 書かないと保存できない
  await p.goto(B + '/sku/k005');
  await p.click('#btn-cost-open');
  await p.fill('#cost-jpy', '85');
  await p.check('input[name="cost-reason-pick"][data-other]');
  assert.equal(await p.isVisible('#cost-reason'), true, 'その他 = 書く欄が出る');
  assert.equal(await active(p), 'cost-reason', '書く欄へ');
  assert.equal(await p.isDisabled('#save'), true, 'その他で空 = 保存できない');
  await p.keyboard.press('Control+s');
  await p.waitForTimeout(200);
  assert.equal(await costReason('k005'), 'initial load load_ui', '保存していない (今の原価は取り込みの行のまま)');
  await p.fill('#cost-reason', '送料込みの仕入値に変わった');
  assert.equal(await p.isDisabled('#save'), false);
  await p.click('#save');
  await p.waitForSelector('#saved-note');
  assert.equal(await costReason('k005'), '送料込みの仕入値に変わった');
  assert.match(await p.textContent('#sku-page'), /送料込みの仕入値に変わった/, '原価の履歴に出る');
  // 選び直すと書く欄は隠れる
  await p.click('#btn-cost-open');
  await p.check('input[name="cost-reason-pick"][data-other]');
  await p.check('input[name="cost-reason-pick"][value="メーカーからの値下げ通知"]');
  assert.equal(await p.isHidden('#cost-reason'), true);
});

// ── 詳細検索・注文残・在庫 (PR2・10/5) ──
const SHOT2 = process.env.MASTER_EDIT_UI_SHOTS2 || '';
// 列の位置は見出しから (登録日の列 (0057) などで位置が変わっても同じ)
const cells = (p) => p.$eval('#list-tbl', (tbl) => {
  const h = [...tbl.querySelectorAll('thead th')].map((th) => th.textContent.trim());
  const iS = h.findIndex((x) => x.startsWith('在庫'));
  const iP = h.findIndex((x) => x.startsWith('注文残'));
  const iF = h.findIndex((x) => x.startsWith('FBA'));
  return [...tbl.querySelectorAll('tbody tr')].map((tr) => { const t = [...tr.children].map((td) => td.textContent.replace(/\s+/g, ' ').trim()); return { code: tr.querySelector('td[data-col="code"]').textContent.replace(/\s+/g, ' ').trim(), stock: iS < 0 ? undefined : t[iS], po: iP < 0 ? undefined : t[iP], fba: iF < 0 ? undefined : t[iF] }; });
});
const advGo = async (p, fill) => {
  await p.goto(B + '/');
  await p.click('#adv > summary');
  await fill();
  await Promise.all([p.waitForNavigation(), p.click('#adv-form button[type="submit"]')]);
};

await ta('[18] 詳細検索 (10/5): 商品コードを複数 (Excel の列の貼り付け・大文字・全角) = ぴったり・見つからないコードを上に出す・URL に残る・クリア', async (p) => {
  await advGo(p, () => p.fill('#adv-codes', 'K001\tｋ００２\r\nnope-1, s003\nNOPE-2\nk001'));
  assert.deepEqual(await listCodes(p), ['k001', 'k002', 's003']);
  assert.match(await p.textContent('#not-found'), /2 件 見つからない[\s\S]*nope-1, NOPE-2/);
  const u = new URL(p.url());
  assert.match(u.searchParams.get('codes'), /NOPE-2/, '条件は URL に');
  assert.equal((await p.inputValue('#adv-codes')).split('\n').length, 6, '貼り付けた値は 1 行 1 つに直して欄に残る');
  assert.equal(await p.getAttribute('#adv', 'open'), '', '詳細検索を使っている = 板を開いておく');
  if (SHOT2) await p.screenshot({ path: `${SHOT2}/詳細検索_商品コードを複数.png`, fullPage: true });
  await p.reload();
  assert.deepEqual(await listCodes(p), ['k001', 'k002', 's003'], '戻る・開き直すで同じ結果');
  // 札・絞る欄と一緒に使える (区分の札を押しても詳細検索の条件は残る)
  await Promise.all([p.waitForNavigation(), p.click('.chips a.chip:has-text("単品")')]);
  assert.deepEqual(await listCodes(p), ['k001', 'k002', 's003']);
  await Promise.all([p.waitForNavigation(), p.click('#adv-form a:has-text("クリア")')]);
  assert.equal(new URL(p.url()).search, '');
  assert.ok((await listCodes(p)).length > 40, 'クリア = 全部');
  assert.equal(await p.locator('#not-found').count(), 0);
});

await ta('[19] 詳細検索の項目 (10/5): JAN (複数)・仕入先 (0001 と 1)・代表 (親)・商品名 (かな)・原価 / 売価の範囲・税率・売上分類・取扱区分', async (p) => {
  const q = async (qs) => { await p.goto(B + '/?' + new URLSearchParams(qs)); return listCodes(p); };
  assert.deepEqual(await q({ jans: `0000000\n${jan13('490000000777')}` }), ['s003']);
  const bySup = await q({ sups: '1', kind: 'single' });
  assert.ok(bySup.includes('k001') && bySup.includes('s001') && !bySup.includes('set001'), '代表の仕入先 0001 (1 と書いても同じ)');
  assert.deepEqual(await q({ sups: '9999' }), []);
  assert.deepEqual(await q({ parents: 'K001' }), ['k001', 'k002'], '代表 k001 = 子 k002 と代表そのもの');
  assert.deepEqual(await q({ name: 'ﾊﾁﾐﾂ' }), ['k001', 'k002', 'k003']);
  assert.deepEqual(await q({ cost_min: '305', cost_max: '310' }), ['k002']);
  // 🆕 0071: 登録をやめた商品 (ui-card-1 = [13] でやめた) はふだんの一覧に出さない・「やめた」で絞ると出る
  assert.deepEqual(await q({ price_min: '1,200' }), [], '売価の範囲 (カンマ付きでも)・やめた商品はふだん出さない');
  assert.deepEqual(await q({ price_min: '1,200', reg: 'cancelled' }), ['ui-card-1'], '売価の範囲 (カンマ付きでも)・「やめた」で絞ると出る');
  assert.deepEqual(await q({ name: 'はちみつ', tax: '8' }), ['k001', 'k002', 'k003']);
  assert.deepEqual(await q({ name: 'はちみつ', tax: '10' }), []);
  assert.deepEqual(await q({ name: 'はちみつ', sales: '1' }), ['k001', 'k002', 'k003']);
  assert.deepEqual(await q({ name: 'はちみつ', sales: '3' }), []);
  assert.deepEqual(await q({ codes: 'set001\ns001', sales: '3' }), ['s001', 'set001'], 'セットの売上分類は構成品から導いて絞る');
  assert.deepEqual(await q({ codes: 'k001\nk002', state: 'discontinued' }), []);
  // 画面の部品から: 選ぶ欄・範囲
  await advGo(p, async () => { await p.fill('#adv-name', 'はちみつ'); await p.selectOption('#adv-tax', '8'); await p.fill('input[name="cost_max"]', '200'); });
  assert.deepEqual(await listCodes(p), ['k003']);
});

await ta('[20] 注文残 (発注アプリ)・在庫 (ロジザード) (10/5): 一覧の列・「注文残あり」と在庫の範囲で絞る・1 つの商品の画面の内訳と「いつの写しか」・セットは作れる数・古い / 読めない', async (p) => {
  await p.goto(B + '/?' + new URLSearchParams({ codes: 'k001\ns001\ns002\nset001\nk006' }));
  const c = Object.fromEntries((await cells(p)).map((x) => [x.code, x]));
  assert.deepEqual([c.k001.stock, c.k001.po], ['15', '7'], 'k001: 在庫 = 全部のロケ・品質区分の合計 (大文字の商品ID も同じ)・注文残 = 10 − 取消 3');
  assert.deepEqual([c.s001.stock, c.s001.po], ['10', ''], '注文残 0 = 空');
  assert.deepEqual([c.s002.stock, c.s002.po], ['5', '4']);
  assert.equal(c.set001.stock, '5作れる', 'セット = 構成品から作れる数 (s001 10 ÷ 1・s002 5 ÷ 1 の小さい方)');
  assert.deepEqual([c.k006.stock, c.k006.po], ['0', ''], '写しに無い = 0');
  const th = await p.textContent('#th-stock');
  assert.match(th, /在庫\d{2}:\d{2}/, '見出しに写しの時刻');
  assert.doesNotMatch(th, /古い|読めない/);
  if (SHOT2) await p.screenshot({ path: `${SHOT2}/一覧_在庫と注文残の列.png`, fullPage: true });
  const q = async (qs) => { await p.goto(B + '/?' + new URLSearchParams(qs)); return listCodes(p); };
  assert.deepEqual(await q({ po: '1' }), ['k001', 's002'], '注文残あり (境界より前の発注は入らない)');
  const st10 = await q({ stock_min: '10', kind: 'single' });
  assert.deepEqual(st10, ['k001', 's001']);
  assert.deepEqual(await q({ stock_max: '0', name: 'みかん' }), ['k006'], '在庫 0 (写しに無い) も範囲に入る');
  // 1 つの商品の画面
  await p.goto(B + '/sku/k001');
  assert.equal(await p.textContent('#ref-stock'), '15');
  assert.match(await p.textContent('#ref-stock-when'), /時点/);
  assert.doesNotMatch(await p.textContent('#ref-stock-when'), /古い/);
  assert.equal(await p.textContent('#ref-po'), '7');
  const lines = await p.textContent('#ref-po-lines');
  assert.match(lines, /AMC/); assert.match(lines, /未定/);
  if (SHOT2) await p.screenshot({ path: `${SHOT2}/単品_在庫と注文残.png`, fullPage: true });
  await p.goto(B + '/sku/s002');
  assert.match(await p.textContent('#ref-po-lines'), /10\/20 \(火\) 回答/);
  await p.goto(B + '/sku/set001');
  assert.equal(await p.textContent('#ref-stock'), '5');
  assert.match(await p.textContent('#ref-box'), /作れる数/);
  assert.equal(await p.locator('#ref-po').count(), 0, 'セットは注文残を出さない (発注は単品)');
  // 古い (26 時間前 = 写しの時間 (09〜18 時) の外でも古い。18 時台の写しは次の朝 10 時まで古くない = 3 時間前だと夜は古くならない)
  mirrorDb.prepare('update mirror_logizard_stock set captured_at = ?').run(new Date(Date.now() - 26 * 3600e3).toISOString());
  __clearStockCache();
  await p.goto(B + '/?' + new URLSearchParams({ codes: 'k001' }));
  assert.match(await p.textContent('#th-stock'), /古い/);
  await p.goto(B + '/sku/k001');
  assert.match(await p.textContent('#ref-stock-when'), /古い/);
  // 読めない (写しが無い)
  const keep = mirrorDb.prepare('select * from mirror_logizard_stock').all();
  mirrorDb.prepare('delete from mirror_logizard_stock').run();
  __clearStockCache();
  await p.goto(B + '/?' + new URLSearchParams({ codes: 'k001' }));
  assert.match(await p.textContent('#th-stock'), /読めない/);
  assert.equal((await cells(p))[0].stock, '—');
  assert.deepEqual(await q({ stock_min: '1' }), [], '読めないときは在庫の範囲で当てない');
  await p.goto(B + '/sku/k001');
  assert.match(await p.textContent('#ref-stock-when'), /読めない/);
  const ins = mirrorDb.prepare(`insert into mirror_logizard_stock (${Object.keys(keep[0]).map((k) => `"${k}"`).join(', ')}) values (${Object.keys(keep[0]).map(() => '?').join(', ')})`);
  for (const r of keep) ins.run(...Object.values(r));
  mirrorDb.prepare('update mirror_logizard_stock set captured_at = ?').run(new Date(Date.now() - 20 * 60e3).toISOString());
  __clearStockCache();
});

await ta('[21] 在庫の範囲で絞る = 一覧に出す値 (セットは作れる数) で絞る (#1620 Codex R1 M1)', async (p) => {
  const q = async (qs) => { await p.goto(B + '/?' + new URLSearchParams(qs)); return listCodes(p); };
  // set001 は構成品 s001 (10) と s002 (5) から 5 作れる (セット自身の行はロジザードに無い)
  assert.deepEqual(await q({ codes: 'set001', stock_min: '1' }), ['set001'], '作れる数 5 は 1 以上に入る');
  assert.deepEqual(await q({ codes: 'set001', stock_max: '0' }), [], '作れる数 5 は 0 以下に入らない');
  assert.deepEqual(await q({ codes: 'set001\ns001\ns002', stock_min: '5', stock_max: '5' }), ['s002', 'set001'], '単品は在庫・セットは作れる数');
  assert.equal(await p.textContent('#list-tbl tbody tr:has-text("set001") td[data-col="stock"]'), '5作れる');
});

await ta('[22] 注文残は発注アプリの利用権がある人だけ (#1620 Codex R1 M3): 無い人 = 列・絞り込み・内訳を出さず「権限がないので出せません」', async (p) => {
  SESSION_APPS = ['master-edit'];
  try {
    await p.goto(B + '/?' + new URLSearchParams({ codes: 'k001\ns002' }));
    assert.doesNotMatch(await p.textContent('#list-tbl thead'), /注文残/, '列を出さない');
    assert.deepEqual((await cells(p)).map((x) => x.code), ['k001', 's002']);
    assert.equal(await p.locator('#list-tbl tbody tr').first().locator('td:visible:not(.c-chk)').count(), 16, '行の欄も 1 つ少ない (17 → 16。FBA (JP)・区分・売れた数・利益・利益率の列を足した・10/8 の仕入先の列はいつもは出さない・10/17 の画像の列はいつも出す)');
    assert.equal(await p.locator('#list-tbl tbody tr').first().locator('td[data-col="po"]').count(), 0, '注文残のセルは描かない (隠すだけでなく)');
    assert.equal(await p.locator('input[name="po"]').count(), 0, '「注文残あり」を出さない');
    assert.match(await p.textContent('#po-denied'), /注文残 \(発注アプリの権限がないので出せません\)/);
    // URL に po=1 を付けても、注文残のある商品だけに絞らない (どの商品に注文残があるかを出さない)
    await p.goto(B + '/?' + new URLSearchParams({ codes: 'k001\ns001\ns002', po: '1' }));
    assert.deepEqual(await listCodes(p), ['k001', 's001', 's002']);
    await p.goto(B + '/sku/k001');
    assert.match(await p.textContent('#ref-po-why'), /注文残 \(発注アプリの権限がないので出せません\)/);
    assert.equal(await p.locator('#ref-po').count(), 0);
    assert.equal(await p.locator('#ref-po-lines').count(), 0, '内訳 (仕入先・発注日・数・納期) を出さない');
    assert.doesNotMatch(await p.content(), /PO-2026-0001|AMC<\/td>/);
    assert.equal(await p.textContent('#ref-stock'), '15', '在庫 (ロジザード) は出す');
  } finally { SESSION_APPS = ['master-edit', 'purchase-orders']; }
  // ある人 (* も同じ)
  SESSION_APPS = '*';
  try {
    await p.goto(B + '/sku/k001');
    assert.equal(await p.textContent('#ref-po'), '7');
  } finally { SESSION_APPS = ['master-edit', 'purchase-orders']; }
});

await ta('[24] FBA (JP) の在庫 (10/5): 一覧の列 (参考・灰色)・見出しの下に何時時点か・1 × 1 の出品だけ・「—」と 0・単品の画面の内訳・まとめ売り / セットの出品は別・古い / 読めない', async (p) => {
  const hhmm = (iso) => { const d = new Date(Date.parse(iso) + 9 * 3600e3); return `${d.getUTCMonth() + 1}/${d.getUTCDate()} ${String(d.getUTCHours()).padStart(2, '0')}:${String(d.getUTCMinutes()).padStart(2, '0')}`; };
  await p.goto(B + '/?' + new URLSearchParams({ codes: 'k001\nk002\nk006\ns001' }));
  const c = Object.fromEntries((await cells(p)).map((x) => [x.code, x]));
  assert.deepEqual([c.k001.fba, c.k002.fba, c.k006.fba, c.s001.fba], ['24', '0出品なし', '0', '0出品なし'], 'k001 = 1 × 1 の出品 2 つ (20 + 4)・まとめ売り / セットの出品は入れない / k002・s001 = 1 × 1 の出品の行なし = 0 + 「出品なし」(mart.v_sku_stock と同じ 0) / k006 = レポートに 0');
  const th = p.locator('#th-fba');
  assert.equal((await th.textContent()).trim(), 'FBA (JP)' + hhmm(FBA_CAPTURED), '見出しの下に取得の時刻 (月/日 時:分)');
  assert.match(await th.getAttribute('title'), /FBA \(日本\) の販売可能 · .* 時点 \(朝のレポート\) · 参考 · この SKU 1 個の出品だけの合計/);
  assert.equal(await th.evaluate((x) => x.className), 'n ref');
  // 参考の列 = 灰色・小さめ (在庫の列と同じ見せ方)
  const [fbaStyle, stockStyle] = await p.evaluate(() => {
    const td = (i) => document.querySelector('#list-tbl tbody tr').children[i];
    const h = [...document.querySelectorAll('#list-tbl thead th')].map((x) => x.textContent.trim());
    const pick = (el) => { const s = getComputedStyle(el); return [s.color, s.fontSize]; };
    return [pick(td(h.findIndex((x) => x.startsWith('FBA')))), pick(td(h.findIndex((x) => x.startsWith('在庫'))))];
  });
  assert.deepEqual(fbaStyle, stockStyle, 'FBA の列は在庫の列と同じ灰色・大きさ');
  if (SHOT2) await p.screenshot({ path: `${SHOT2}/一覧_FBAの列.png`, fullPage: true });
  // 単品の画面
  await p.goto(B + '/sku/k001');
  assert.equal(await p.textContent('#ref-fba'), '24');
  assert.match(await p.textContent('#ref-fba-when'), /時点 \(Amazon の朝のレポート\)/);
  assert.doesNotMatch(await p.textContent('#ref-fba-when'), /古い/);
  const parts = await p.$$eval('#ref-fba-parts tbody tr', (trs) => trs.map((tr) => [...tr.children].map((td) => td.textContent.trim())));
  assert.deepEqual(parts, [['販売可能', '24'], ['FC 移管中', '1'], ['FC 処理中', '1'], ['出荷待ち (注文の引き当て)', '2'], ['入荷待ち (納品の途中)', '5']]);
  assert.match(await p.textContent('#ref-fba-skus'), /出品 SKU 2 つの合計: pr-k001 20 · pr-k001-b 4/);
  assert.match(await p.textContent('#ref-fba-bundles'), /まとめ売り・セットの出品 \(上の数に入れていない\): pr-k001-3p ×3 = 6 · pr-k1k2 \(ほか 1 品と\) = 3/);
  if (SHOT2) await p.screenshot({ path: `${SHOT2}/単品_FBAの内訳.png`, fullPage: true });
  await p.goto(B + '/sku/k002');
  assert.equal(await p.textContent('#ref-fba'), '0');
  assert.match(await p.textContent('#ref-fba-none'), /FBA の出品なし \(この SKU 1 個だけの出品の行が、その日の FBA のレポートに無い = 販売可能 0 と数える\)/);
  assert.equal(await p.locator('#ref-fba-parts').count(), 0);
  assert.match(await p.textContent('#ref-fba-bundles'), /pr-k1k2 \(ほか 1 品と\) = 3/);
  // 古い (取得が 26 時間より前)
  const keepCap = (await pg.query("select captured_at from snapshots.stock_capture_days where source = 'fba_jp'")).rows[0].captured_at;
  await pg.query("update snapshots.stock_capture_days set captured_at = now() - interval '27 hours' where source = 'fba_jp'");
  try {
    await p.goto(B + '/?' + new URLSearchParams({ codes: 'k001' }));
    assert.match(await p.textContent('#th-fba'), /^FBA \(JP\)古い \d+\/\d+ \d{2}:\d{2}$/);
    assert.equal(await p.locator('#th-fba').evaluate((x) => x.className), 'n ref stale');
    assert.equal((await cells(p))[0].fba, '24', '古くても値は出す');
    await p.goto(B + '/sku/k001');
    assert.match(await p.textContent('#ref-fba-when'), /^古い · /);
    assert.equal(await p.locator('#ref-fba-when').evaluate((x) => x.className), 'when stale');
  } finally { await pg.query("update snapshots.stock_capture_days set captured_at = $1 where source = 'fba_jp'", [keepCap]); }
  // 読めない (画面のロールに権限が無い = 流し直しの前の本番) → 流し直すと戻る
  await pg.query('revoke select on snapshots.stock_capture_days from master_edit');
  try {
    await p.goto(B + '/?' + new URLSearchParams({ codes: 'k001' }));
    assert.equal((await p.textContent('#th-fba')).trim(), 'FBA (JP)読めない');
    assert.equal(await p.locator('#th-fba').evaluate((x) => x.className), 'n ref bad');
    assert.equal((await cells(p))[0].fba, '—');
    assert.equal((await cells(p))[0].stock, '15', 'ロジザードの在庫はそのまま');
    await p.goto(B + '/sku/k001');
    assert.match(await p.textContent('#ref-fba-when'), /読めない \(画面のロールに FBA の在庫を読む権限がまだ無い/);
    assert.equal(await p.locator('#ref-fba-parts').count(), 0);
    assert.equal(await p.textContent('#ref-stock'), '15');
  } finally { await createMasterEditRoles(pg, {}); await W2.useReal0058(pg, { leases: ['single', 'set'], futureSetLease: true }); }
  await p.goto(B + '/?' + new URLSearchParams({ codes: 'k001' }));
  assert.equal((await cells(p))[0].fba, '24', '流し直した = 読める');
});

await ta('[25] 売れた数 (10/5): 一覧の列 (参考・灰色・見出しの下に「M/D まで」)・セット / 商品管理リストに無い = 「—」・単品の画面の FBA / FBA 以外・モール別・古い / 読めない', async (p) => {
  // 写し (initMirrorDB の本物の表): 商品管理リストの公開の回 = 前日まで
  const ymd = (d) => new Date(Date.now() + 9 * 3600e3 - d * 864e5).toISOString().slice(0, 10);
  const md = (s) => `${Number(s.slice(5, 7))}/${Number(s.slice(8, 10))}`;
  const y1 = ymd(1);
  const now = new Date().toISOString();
  mirrorDb.prepare(`insert or replace into mirror_pml_published (id, run_id, status, as_of_date, src_velocity_as_of, synced_at) values (1, 'pml_ui_sales', 'ok', ?, ?, ?)`).run(ymd(0), y1, now);
  const ins = mirrorDb.prepare(`insert into mirror_pml_snapshot_rows (run_id, 商品コード, 販売数7日_FBA, 販売数7日_FBA以外, 販売数7日_合計, 販売数30日_FBA, 販売数30日_FBA以外, 販売数30日_合計) values ('pml_ui_sales', ?, ?, ?, ?, ?, ?, ?)`);
  ins.run('K001', 3, 9, 12, 10, 38, 48); ins.run('k002', 0, 0, 0, 0, 0, 0); ins.run('set001', 0, 0, 0, 0, 0, 0);
  const mi = mirrorDb.prepare('insert into mirror_f_sales_velocity_by_product_mall (商品コード, mall, qty_7d, qty_30d, as_of_date, synced_at) values (?, ?, ?, ?, ?, ?)');
  mi.run('k001', 'rakuten', 6, 20, y1, now); mi.run('k001', 'amazon_fba', 3, 10, y1, now); mi.run('k001', 'yahoo', 3, 18, y1, now);
  try {
    await p.goto(B + '/?' + new URLSearchParams({ codes: 'k001\nk002\nk006\nset001' }));
    const th = p.locator('#th-sales');
    assert.equal((await th.textContent()).trim(), `売れた 7日/30日${md(y1)} まで`);
    assert.equal(await th.evaluate((x) => x.className), 'n ref');
    const cell = (code) => p.locator(`#list-tbl tbody tr:has(a.rowlink:text-is("${code}")) td[data-col="sales"]`).textContent().then((x) => x.trim());
    assert.deepEqual([await cell('k001'), await cell('k002'), await cell('k006'), await cell('set001')], ['12 / 48', '0 / 0', '—', '—'], 'k006 = 商品管理リストに無い / set001 = 構成品に入る');
    const [sStyle, stStyle] = await p.evaluate(() => {
      const tr = document.querySelector('#list-tbl tbody tr');
      const pick = (el) => { const s = getComputedStyle(el); return [s.color, s.fontSize]; };
      return [pick(tr.querySelector('td[data-col="sales"]')), pick(tr.querySelector('td[data-col="stock"]'))];
    });
    assert.deepEqual(sStyle, stStyle, '売れた数の列は在庫の列と同じ灰色・大きさ');
    if (SHOT2) await p.screenshot({ path: `${SHOT2}/一覧_売れた数の列.png`, fullPage: true });
    await p.goto(B + '/sku/k001');
    assert.equal(await p.textContent('#ref-sales'), '12 / 48');
    assert.match(await p.textContent('#ref-sales-asof'), new RegExp(`^${md(y1)} \\(.\\) まで \\(7 日 = ${md(ymd(7))}〜・30 日 = ${md(ymd(30))}〜・今日は入れない\\)`));
    const rows = (sel) => p.$$eval(`${sel} tbody tr`, (trs) => trs.map((tr) => [...tr.children].map((td) => td.textContent.trim())));
    assert.deepEqual(await rows('#ref-sales-parts'), [['Amazon FBA', '3', '10'], ['FBA 以外 (NE の受注)', '9', '38']]);
    assert.deepEqual(await rows('#ref-sales-malls'), [['楽天', '6', '20'], ['Yahoo!', '3', '18'], ['Amazon FBA', '3', '10']], '多い順・名前は dim_mall');
    if (SHOT2) await p.screenshot({ path: `${SHOT2}/単品_売れた数.png`, fullPage: true });
    await p.goto(B + '/sku/set001');
    assert.match(await p.textContent('#ref-sales-set'), /セットで売れた分は構成品の数に入る/);
    // 古い (前日までになっていない・正午の前でも 3 日前は古い)
    mirrorDb.prepare('update mirror_pml_published set src_velocity_as_of = ? where id = 1').run(ymd(3));
    await p.goto(B + '/?' + new URLSearchParams({ codes: 'k001' }));
    assert.equal((await p.textContent('#th-sales')).trim(), `売れた 7日/30日古い ${md(ymd(3))} まで`);
    assert.equal(await p.locator('#th-sales').evaluate((x) => x.className), 'n ref stale');
    // 読めない (公開の回が無い)
    mirrorDb.prepare('delete from mirror_pml_published').run();
    await p.goto(B + '/?' + new URLSearchParams({ codes: 'k001' }));
    assert.equal((await p.textContent('#th-sales')).trim(), '売れた 7日/30日読めない');
    assert.equal(await p.locator('#th-sales').evaluate((x) => x.className), 'n ref bad');
    assert.equal(await cell('k001'), '—');
    await p.goto(B + '/sku/k001');
    assert.match(await p.textContent('#ref-sales-when'), /^読めない/);
  } finally {
    mirrorDb.prepare('delete from mirror_pml_published').run();
    mirrorDb.prepare(`delete from mirror_pml_snapshot_rows where run_id = 'pml_ui_sales'`).run();
    mirrorDb.prepare('delete from mirror_f_sales_velocity_by_product_mall').run();
  }
});

await ta('[26] 区分の列・コードのコピー (10/5): 行のボタン = クリップボードに入り「コピーしました」を 1 回・行は開かない・キーボードでも / 全部コピー = 絞った全件 (ページに関係なく・改行区切り) / CSV のリンクで今の絞り込みの CSV が落ちる', async (p, ctx) => {
  await ctx.grantPermissions(['clipboard-read', 'clipboard-write'], { origin: new URL(B).origin });
  await p.goto(B + '/?kind=single');
  const url0 = p.url();
  // 区分の列 (札)
  const kindOf = (code) => p.locator(`#list-tbl tbody tr:has(a.rowlink:text-is("${code}")) td[data-col="kind"]`).textContent().then((x) => x.replace(/\s+/g, ' ').trim());
  assert.equal(await kindOf('k001'), '単品');
  // 行のコピーのボタン: 押す = クリップボード・知らせ 1 回・行は開かない (覆いのリンクより上)
  const btn = p.locator('#list-tbl tbody tr:has(a.rowlink:text-is("k002")) button.copybtn');
  assert.equal(await btn.getAttribute('aria-label'), '商品コード k002 をコピー');
  await btn.click();
  await p.waitForFunction(() => document.querySelector('#toast').classList.contains('on'));
  assert.equal(await p.textContent('#toast-t'), 'コピーしました: k002');
  assert.equal(await p.evaluate(() => navigator.clipboard.readText()), 'k002');
  assert.equal(p.url(), url0, '行を開かない');
  assert.equal(await p.locator('#toast[role="status"]').count(), 1, '知らせは読み上げの場所 (role=status) 1 つ');
  // キーボード: ボタンに Tab で来て Enter
  await p.locator('#list-tbl tbody tr:has(a.rowlink:text-is("k003")) a.rowlink').focus();
  await p.keyboard.press('Tab');
  assert.equal(await p.evaluate(() => document.activeElement.getAttribute('aria-label')), '商品コード k003 をコピー');
  await p.keyboard.press('Enter');
  await p.waitForFunction(() => document.querySelector('#toast-t').textContent === 'コピーしました: k003');
  assert.equal(await p.evaluate(() => navigator.clipboard.readText()), 'k003');
  assert.equal(p.url(), url0);
  // 全部コピー = 絞った全件 (サーバーの一覧の全件と同じ・並びも同じ)
  const total = Number(await p.getAttribute('#copy-all', 'data-n'));
  assert.ok(total > 0);
  await p.click('#copy-all');
  await p.waitForFunction(() => /件 \(1 行 1 つ\)$/.test(document.querySelector('#toast-t').textContent));
  assert.equal(await p.textContent('#toast-t'), `コピーしました: 商品コード ${total} 件 (1 行 1 つ)`);
  const clip = await p.evaluate(() => navigator.clipboard.readText());
  const want = (await (await fetch(B + '/api/codes?kind=single')).json()).codes;
  assert.deepEqual(clip.split(/\r?\n/), want, '1 行 1 つ (Windows のクリップボードは CRLF で返す)');
  assert.equal(want.length, total);
  assert.equal(await p.locator('#copy-all').isDisabled(), false, '終わったらまた押せる');
  // CSV のリンク = 今の絞り込みのまま (ダウンロード)
  const [dl] = await Promise.all([p.waitForEvent('download'), p.click('#csv-link')]);
  assert.match(dl.suggestedFilename(), /^master-list_\d{8}-\d{4}\.csv$/);
  const file = await dl.path();
  const buf = fs.readFileSync(file);
  assert.deepEqual([...buf.subarray(0, 3)], [0xef, 0xbb, 0xbf]);
  const lines = buf.subarray(3).toString('utf8').trimEnd().split('\r\n');
  assert.equal(lines.length, total + 1, '見出し + 絞った全件');
  assert.match(lines[0], /^"商品コード","区分",/);
  await dl.delete();
  // 区分の順の並び (札で選ぶ)
  await p.goto(B + '/?sort=kind');
  const order = await p.$$eval('#list-tbl tbody td[data-col="kind"]', (tds) => tds.map((td) => td.textContent.trim().split(/\s/)[0]));
  assert.ok(order.indexOf('セット') > order.lastIndexOf('単品'), '単品 → セット');
});

await ta('[23] 詳細検索の長い条件 (#1620 Codex R1 M2): 本物の HTTP で商品コード 500 件 = 印 (?s=) で開ける・501 件 = 500 件まで・GET に載せると 431・Origin と JSON の守り・期限切れ', async (p) => {
  const origin = new URL(B).origin;
  const code30 = (i) => `nope-${String(i).padStart(4, '0')}-${'x'.repeat(30)}`.slice(0, 30);
  const codes500 = ['k001', ...Array.from({ length: 499 }, (_, i) => code30(i))];
  const post = (body, headers = {}) => fetch(B + '/api/search', { method: 'POST', headers: { Origin: origin, 'Content-Type': 'application/json', Accept: 'application/json', ...headers }, body: JSON.stringify(body) });
  // GET の URL にそのまま載せる = Express に届く前に 431 (これを避けるための印)
  const raw = await fetch(B + '/?' + new URLSearchParams({ codes: codes500.join('\n') }));
  assert.equal(raw.status, 431, `GET に 500 件 = ${raw.status}`);
  // 守り = 保存の POST と同じ (Origin が Host と同じ・JSON)
  assert.equal((await post({ codes: 'k001' }, { Origin: 'https://evil.example' })).status, 403);
  assert.equal((await fetch(B + '/api/search', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' })).status, 403, 'Origin なし');
  assert.equal((await post({ codes: 'k001' }, { 'Content-Type': 'application/x-www-form-urlencoded' })).status, 415);
  // 500 件 = 全部使う
  const j = await (await post({ codes: codes500.join('\n'), kind: 'single', q: '' })).json();
  assert.equal(j.ok, true);
  assert.match(j.url, /^\/apps\/master-edit\/\?kind=single&s=[A-Za-z0-9_-]{22}$/, j.url);
  const page500 = await (await fetch(origin + j.url)).text();
  assert.match(page500, /499 件 見つからない/);
  assert.doesNotMatch(page500, /500 件までを使いました/);
  assert.match(page500, /href="sku\/k001"/);
  // 501 件 = 500 件まで (知らせる)
  const j2 = await (await post({ codes: [...codes500, 'k002'].join('\n') })).json();
  const page501 = await (await fetch(origin + j2.url)).text();
  assert.match(page501, /500 件までを使いました/);
  assert.doesNotMatch(page501, /href="sku\/k002"/, '501 件目は使わない');
  // 画面から: 500 件を貼って「この条件で探す」= 印の URL で開く・区分の札を押しても条件が残る (URL は短いまま)
  await p.goto(B + '/');
  await p.click('#adv > summary');
  await p.fill('#adv-codes', codes500.join('\n'));
  await Promise.all([p.waitForNavigation(), p.click('#adv-form button[type="submit"]')]);
  assert.match(p.url(), /\?s=[A-Za-z0-9_-]{22}$/);
  assert.deepEqual(await listCodes(p), ['k001']);
  assert.match(await p.textContent('#not-found'), /499 件 見つからない/);
  assert.equal((await p.inputValue('#adv-codes')).split('\n').length, 500, '欄には条件が戻る');
  await Promise.all([p.waitForNavigation(), p.click('.chips a.chip:has-text("単品")')]);
  assert.ok(p.url().length < 200, `札の URL も短い (${p.url().length})`);
  assert.deepEqual(await listCodes(p), ['k001']);
  // 期限切れ・再起動で消えた印
  __clearSearchTokens();
  await p.reload();
  assert.match(await p.textContent('#search-expired'), /条件の期限が切れました。もう一度検索してください/);
  assert.deepEqual(await listCodes(p), [], '消えた条件で全部を出さない');
  // 短い条件は今までどおり GET (URL に条件が残る)
  await p.goto(B + '/');
  await p.click('#adv > summary');
  await p.fill('#adv-codes', 'k001\nk002');
  await Promise.all([p.waitForNavigation(), p.click('#adv-form button[type="submit"]')]);
  assert.equal(new URL(p.url()).searchParams.get('codes').replace(/\r/g, ''), 'k001\nk002');
});

// ─── 第 2 段 (10/5): 新商品の登録・Amazon SKU・NE 登録の CSV・変更の記録 を新しいデザインに ───
/** 🆕 0061 (2026-10-08 中原さん): 下書きの保存で要る欄 = 単品の売上分類・ロジザードの有効期限の管理・推奨保有月数 / セットの推奨保有月数 */
const fillReq = async (p, { set = false } = {}) => {
  if (!set) { await p.click('#f-sales_class button[data-v="3"]'); await p.click('#f-expiry button[data-v="0"]'); }
  await p.fill('#f-reorder_months', '1');
  // 🆕 0080: 単品のロジザードのバーコード (必須 = 値か「空欄」)。ほかの決まりの試験は「空欄」
  if (!set && (await p.locator('#f-lzbc-blank').count())) await p.check('#f-lzbc-blank');
};
/**
 * 🆕 2026-10-13 (入力なし): 値の無い必須でない項目を全部「入力なし」にする (画面でチェックを押すのと同じ・発注条件グループ / 原料グループは「なし」)。
 *   ほかの決まりを確かめる試験で、保存の前に使う。押した数を返す
 */
const noneRest = (p) => p.evaluate(() => {
  let n = 0;
  for (const id of ['po-cond-mode', 'po-mat-mode']) {
    const sg = document.getElementById(id);
    if (sg && !sg.getAttribute('data-value')) { sg.querySelector('button[data-v="none"]').click(); n++; }
  }
  document.querySelectorAll('input[type="checkbox"][data-none]').forEach((c) => {
    if (c.disabled || c.checked || window.MasterEdit.none.has(c.getAttribute('data-none'))) return;
    c.click(); n++;
  });
  return n;
});
const skuRow = async (code) => (await pg.query('select s.name, s.standard_price_jpy::int as price, s.tax_rate::float8 as tax, s.shipping_code from core.skus s where s.code = $1', [code])).rows[0];
const mapOf = async (sku) => (await pg.query('select m.name, m.state from core.amazon_sku_maps m where m.seller_sku = $1', [sku])).rows[0];

await ta('[35] 新商品の登録 (単品・10/5): あと N つ (① がそろうまで押せない)・① を押すとその欄へ・コードをその場で確かめる・理由は数えない・離れるときの確認 (左の列・種類の札)・Ctrl+S で下書き → できた商品の画面へ (知らせ・戻る 1 回で一覧)', async (p) => {
  await p.goto(B + '/');
  await Promise.all([p.waitForNavigation(), p.click('.ph-actions a:has-text("新しい単品")')]);
  assert.match(p.url(), /new\?kind=single$/);
  assert.equal(await dirty(p), 0);
  // 🆕 2026-10-13 (入力なし): 必須 8 + 必須でない 18 (原価・Amazon・公式ページ・発送方法・セット商品・発注の設定 6・入荷日の管理・参考 URL・Yahoo! 5) = 26
  //   🆕 0080: + ロジザードのバーコード (必須 = 値か空欄) = 27 / 🆕 0084: + メーカー (値か入力なし) = 28
  assert.match(await p.textContent('#remain'), /あと\s*28\s*つ/);
  assert.equal(await p.isDisabled('#save'), true);
  assert.match(await p.textContent('#save'), /あと 28 つ: 商品コード/);
  await p.fill('#reason', '理由だけ');
  assert.equal(await dirty(p), 0, '保存の理由は数えない');
  await p.click('#checklist button:has-text("税率")');
  assert.equal(await p.evaluate(() => !!document.activeElement.closest('#f-tax_rate')), true, '① の税率を押すと税率の欄へ: ' + await p.evaluate(() => document.activeElement.outerHTML.slice(0, 120)));
  // コードをその場で確かめる (もうある・🆕 0064: 大文字も使える = 重なりは大文字小文字を問わず・使えない文字はその場で)
  await p.fill('#code', 's001');
  await p.waitForSelector('#code-msg.err');
  assert.match(await p.textContent('#code-msg'), /もう Company DB にあります/);
  await p.fill('#code', 'S001');
  await p.waitForSelector('#code-msg.err');
  assert.match(await p.textContent('#code-msg'), /商品コード S001 はもう Company DB にあります/);
  await p.fill('#code', 'UI-NEW');
  await p.waitForSelector('#code-msg.ok');
  assert.match(await p.textContent('#code-msg'), /使えます/);
  await p.fill('#code', 'ui.new');
  await p.waitForSelector('#code-msg.err');
  assert.match(await p.textContent('#code-msg'), /使える文字は英字 \(大文字・小文字\)/);
  await p.fill('#code', 'SET-ui');
  await p.waitForSelector('#code-msg.err');
  assert.match(await p.textContent('#code-msg'), /set- で始まるコードは使えません/);
  await p.fill('#code', 'ui-new-1');
  await p.waitForSelector('#code-msg.ok');
  await p.fill('#f-name', 'UI 新商品 1');
  await p.fill('#f-standard_price', '1280');
  assert.match(await p.textContent('#save'), /あと 25 つ: 税率/);
  await p.click('#f-tax_rate button[data-v="0.08"]');
  // 2026-10-08 中原さん: 代表の仕入先は ① (必須)・発送方法は ③ (あとでも可) = 発送方法を選んでも保存できない
  await p.selectOption('#shipping', 'S02');
  assert.match(await p.textContent('#save'), /あと 23 つ: 代表の仕入先/);
  assert.equal(await p.isDisabled('#save'), true, '代表の仕入先が無い = 押せない');
  await p.selectOption('#f-primary_supplier', '0001');
  // 🆕 0061: 売上分類・推奨保有月数・ロジザードの有効期限の管理もそろうまで押せない
  assert.match(await p.textContent('#save'), /あと 22 つ: 売上分類/);
  assert.equal(await p.isDisabled('#save'), true);
  await fillReq(p);
  // 🆕 2026-10-13: 必須がそろっても、必須でない項目が「値」も「入力なし」も無いうちは押せない
  assert.match(await p.textContent('#save'), /あと 18 つ: 原価/);
  assert.equal(await p.isDisabled('#save'), true);
  // 出品カードを作らない = ③ と Yahoo! を数えない (product-hub につながない)
  await p.click('#card-create button[data-v="0"]');
  assert.equal(await p.isHidden('#card-fields'), true);
  assert.match(await p.textContent('#checklist'), /カードを作らない/);
  assert.match(await p.textContent('#save'), /あと 9 つ: 原価/, 'カードを作らない = 原価・メーカー・発注の設定 6・入荷日の管理');
  // ② (NE 登録の CSV まで) も入れる・残りは入力なし
  await p.fill('#cost-jpy', '500');
  await noneRest(p);
  assert.match(await p.textContent('#remain'), /下書きを保存できます/);
  assert.equal(await p.isDisabled('#save'), false);
  assert.equal(await p.locator('#jump a.todo').count(), 0, '飛び先の帯に黄色が残らない');
  assert.ok(await dirty(p) >= 6, String(await dirty(p)));
  // 離れるときの確認 (左の列・種類の札)
  await p.click('.rail a[aria-label="つかいかた"]');
  assert.equal(await p.locator('#leave-bg.on').count(), 1);
  assert.match(await p.textContent('#leave-list'), /名前: UI 新商品 1/);
  await p.click('#leave-stay');
  await p.click('a.kindcard[href="?kind=set"]');
  assert.equal(await p.locator('#leave-bg.on').count(), 1, '種類を変える (画面が変わる) も聞く');
  await p.click('#leave-stay');
  assert.match(p.url(), /new\?kind=single$/);
  // Ctrl+S = 下書きを保存 → できた商品の画面へ (履歴を置き換える)・上に知らせ
  await Promise.all([p.waitForURL(/\/sku\/ui-new-1$/), p.keyboard.press('Control+s')]);
  await p.waitForSelector('#saved-note');
  assert.match(await p.textContent('#saved-note'), /保存しました/);
  assert.deepEqual(await skuRow('ui-new-1'), { name: 'UI 新商品 1', price: 1280, tax: 0.08, shipping_code: 'S02' });
  assert.match(await p.content(), /下書きです/);
  await p.goBack();
  await p.waitForSelector('#list-tbl');
  assert.match(p.url(), /master-edit\/$/, '戻る 1 回で一覧 (入力の途中の画面へは戻らない)');
});

await ta('[35b] 新商品の登録 (2026-10-08 中原さん): 発送方法を選ばなくても下書きにできる (③ に「あとでも可」)・代表の仕入先が無いと押せない', async (p) => {
  await p.goto(B + '/new?kind=single');
  await p.fill('#code', 'ui-noship-1');
  await p.waitForSelector('#code-msg.ok');
  await p.fill('#f-name', '発送方法なしの単品');
  await p.fill('#f-standard_price', '990');
  await p.click('#f-tax_rate button[data-v="0.1"]');
  assert.match(await p.textContent('#save'), /あと 24 つ: 代表の仕入先/);
  assert.equal(await p.isDisabled('#save'), true);
  await p.selectOption('#f-primary_supplier', '0001');
  await fillReq(p);
  await noneRest(p);   // 🆕 2026-10-13: 発送方法も「入力なし」(届いてから)
  assert.match(await p.textContent('#remain'), /下書きを保存できます/);
  assert.match(await p.textContent('#checklist'), /発送方法 \(届いてからでも可\)/);
  assert.equal(await p.locator('#checklist li.need').count(), 0, '発送方法は ① に無い');
  await Promise.all([p.waitForURL(/\/sku\/ui-noship-1$/), p.click('#save')]);
  assert.deepEqual(await skuRow('ui-noship-1'), { name: '発送方法なしの単品', price: 990, tax: 0.1, shipping_code: null });
  assert.equal((await pg.query("select count(*)::int as n from core.supplier_skus x join core.skus s on s.sku_id = x.sku_id where s.code = 'ui-noship-1' and x.is_primary")).rows[0].n, 1);
});

await ta('[35c] 商品の画面 (2026-10-08 中原さん): 下書き ([35b] の ui-noship-1) はロジザードの有効期限・入荷日の管理を切り替えで直せる (未保存・前 → 後・保存・知らせ)・前からある商品 (s001) は 🔒 と理由・出品カードの板', async (p) => {
  await p.goto(B + '/sku/ui-noship-1');
  assert.equal(await p.getAttribute('#p-logi [data-field="expiry_managed"]', 'data-value'), '0');
  await p.click('[data-field="expiry_managed"] button[data-v="1"]');
  await p.click('[data-field="inbound_date_managed"] button[data-v="1"]');
  assert.equal(await dirty(p), 2);
  assert.match(await p.textContent('#save-diff'), /ロジザードの有効期限の管理[\s\S]*ロジザードの入荷日の管理/);
  await p.click('#save');
  await p.waitForSelector('#saved-note');
  assert.match(await p.textContent('#saved-note'), /ロジザードの有効期限の管理: なし → あり/);
  assert.deepEqual((await pg.query("select p.expiry_managed, p.inbound_date_managed from core.products p join core.skus s on s.product_id = p.product_id where s.code = 'ui-noship-1'")).rows[0], { expiry_managed: true, inbound_date_managed: true });
  assert.equal(await p.getAttribute('[data-field="expiry_managed"]', 'data-value'), '1', '読み直した後は保存した値');
  assert.ok(await p.isVisible('#p-card'), '出品カードの板');
  await p.goto(B + '/sku/s001');
  assert.equal(await p.locator('[data-field="expiry_managed"]').count(), 0, '下書きでない = 切り替えを出さない');
  assert.match(await p.textContent('[data-row="expiry_managed"]'), /ロジザードが正/);
});

await ta('[36] 新商品の登録 (セット・10/5): 空の行は数えない・構成品のコードで名前と計算の見込み (8% と 10% = 8%・分類は小さい番号・原価の合計)・並べ替えで読み上げの名前・あと N つ', async (p) => {
  // ui-new-1 ([35] で登録) の売上分類を空にする = 「売上分類が未入力の構成品」(0061 の前の下書き・NE から来た空の商品と同じ形。下で導けない構成に使う)
  await pg.query("update core.products set sales_class = null where display_code = 'ui-new-1'");
  await p.goto(B + '/new?kind=set');
  assert.equal(await dirty(p), 0, '空の 2 行は数えない');
  assert.match(await p.textContent('#remain'), /あと\s*15\s*つ/, '必須 5 + 必須でない 10 (例外原価・発送方法・Amazon・公式・参考 URL・Yahoo! 5)');
  const rows = p.locator('#comp-rows tr.comp-row');
  await rows.nth(0).locator('.c-code').fill('s001');
  await rows.nth(0).locator('.c-code').press('Enter');
  assert.equal(await p.evaluate(() => document.activeElement.classList.contains('c-qty')), true, 'コードで Enter = 数の欄へ (保存しない)');
  await rows.nth(1).locator('.c-code').fill('s003');
  await rows.nth(1).locator('.c-code').press('Tab');
  await p.waitForFunction(() => /単品 3/.test(document.querySelectorAll('#comp-rows .c-name')[1].textContent) && /単品 1/.test(document.querySelectorAll('#comp-rows .c-name')[0].textContent));
  assert.match(await p.textContent('#t-tax'), /8%[\s\S]*混ざって/);
  assert.match(await p.textContent('#t-sales'), /1\s*自社/);
  assert.match(await p.textContent('#t-cost'), /150\s*円/);
  assert.equal(await dirty(p), 1, '構成は 1 件 (中身どうし)');
  await rows.nth(1).locator('button[data-act="up"]').click();
  assert.equal(await rows.nth(0).locator('.c-qty').getAttribute('aria-label'), '1 行目 (s003) の数');
  assert.match(await p.textContent('#remain'), /あと\s*14\s*つ/);
  assert.equal(await p.isDisabled('#save'), true);
  // 売上分類の上書き (#1628 Codex R1 M2): 構成品から導ける (3 と 1 = 1) 間は押せない
  const ovr = p.locator('#f-set_sales_class_override');
  assert.equal(await ovr.isDisabled(), true, '導ける間は上書きを押せない');
  assert.match(await p.textContent('#override-hint'), /導けるので、上書きはできません/);
  // 売上分類が未入力の構成品 (ui-new-1 = [35] で分類を入れずに登録) = 導けない = 上書きで決める
  await rows.nth(0).locator('.c-code').fill('ui-new-1');
  await rows.nth(0).locator('.c-code').press('Tab');
  await p.waitForFunction(() => /UI 新商品 1/.test(document.querySelectorAll('#comp-rows .c-name')[0].textContent));
  assert.equal(await ovr.isDisabled(), false, '導けないときは上書きできる');
  assert.match(await p.textContent('#t-sales'), /決まりません/);
  await p.click('#sec-comp details.more > summary');
  await ovr.selectOption('2');
  assert.match(await p.textContent('#t-sales'), /2\s*取引先限定[\s\S]*導けないので/);
  // 導ける構成に戻す = サーバーが断る形 = ① で止める (保存のボタンも)
  await rows.nth(0).locator('.c-code').fill('s003');
  await rows.nth(0).locator('.c-code').press('Tab');
  await p.waitForFunction(() => /単品 3/.test(document.querySelectorAll('#comp-rows .c-name')[0].textContent));
  assert.match(await p.textContent('#t-sales'), /上書きできません/);
  assert.match(await p.textContent('#checklist'), /売上分類の上書きを空にする/);
  assert.equal(await ovr.isDisabled(), false, '入れてある上書きは空にできる');
  await ovr.selectOption('');
  assert.equal(await ovr.isDisabled(), true);
  assert.ok(!/売上分類の上書きを空にする/.test(await p.textContent('#checklist')));
  // 照合中は保存しない (#1628 Codex R2 M1): 導けない構成 + 上書き (保存できる) から、導ける構成品へ打ち直した直後 = 古い答えで保存しない
  const newCalls = [];
  p.on('request', (rq) => { if (/\/api\/new$/.test(rq.url())) newCalls.push(rq.url()); });
  await p.fill('#code', 'ui-set-race');
  await p.waitForSelector('#code-msg.ok');
  await p.fill('#f-name', '照合中の試験');
  await p.fill('#f-standard_price', '1500');
  await p.selectOption('#shipping', 'S02');
  await fillReq(p, { set: true });
  await noneRest(p);
  await rows.nth(0).locator('.c-code').fill('ui-new-1');
  await rows.nth(0).locator('.c-code').press('Tab');
  await p.waitForFunction(() => /UI 新商品 1/.test(document.querySelectorAll('#comp-rows .c-name')[0].textContent));
  await ovr.selectOption('2');
  assert.equal(await p.isDisabled('#save'), false, '導けない構成 + 上書き = 保存できる');
  await rows.nth(0).locator('.c-code').fill('s003');   // 欄を離れない (照合は打つのが止まってから)
  assert.equal(await p.isDisabled('#save'), true, '打ち直した直後 = 照合中 = 押せない');
  assert.match(await p.textContent('#checklist'), /構成品を確かめています/);
  await p.keyboard.press('Control+s');
  await p.waitForFunction(() => /単品 3/.test(document.querySelectorAll('#comp-rows .c-name')[0].textContent));
  assert.deepEqual(newCalls, [], '照合中の Ctrl+S で送らない');
  assert.match(await p.textContent('#checklist'), /売上分類の上書きを空にする/, '照合の後は導ける = 上書きを空にするまで止める');
  assert.equal(await p.isDisabled('#save'), true);
  await ovr.selectOption('');
  assert.equal(await p.isDisabled('#save'), false, '上書きを空にすれば保存できる');
});

await ta('[40] 新商品の登録 (#1628 Codex R2 M2): カードを作らないでも値の残る欄は隠さない (保存のときに確かめる)・カードの欄の誤りは開いてその欄へ・空にすると畳む', async (p) => {
  await p.goto(B + '/new?kind=single');
  await p.fill('#amazon-url', 'not-a-url');
  await p.click('#card-create button[data-v="0"]');
  assert.equal(await p.isHidden('#card-fields'), false, '値が残っている = 隠さない');
  assert.match(await p.textContent('#card-off-note'), /要らなければ空にしてください/);
  await p.fill('#code', 'ui-nocard-1');
  await p.waitForSelector('#code-msg.ok, #code-msg.err, #code-msg.warn');
  assert.match(await p.getAttribute('#code-msg', 'class'), /ok/, await p.textContent('#code-msg'));
  await p.fill('#f-name', 'カードの試験');
  await p.fill('#f-standard_price', '900');
  await p.click('#f-tax_rate button[data-v="0.1"]');
  await p.selectOption('#shipping', 'S01');
  await p.selectOption('#f-primary_supplier', '0001');
  await fillReq(p);
  await noneRest(p);
  await p.click('#save');
  await p.waitForSelector('.result.err');
  assert.match(await p.textContent('.result.err'), /URL/);
  assert.equal(await p.isHidden('#card-fields'), false);
  assert.equal(await p.locator('[data-row="card.amazon_url"].err').count(), 1, '誤りの欄に印');
  assert.equal(await active(p), 'amazon-url', '誤りの欄へ');
  assert.equal(await skuRow('ui-nocard-1'), undefined, '登録していない');
  await p.fill('#amazon-url', '');
  await p.focus('#f-name');
  await p.waitForFunction(() => document.getElementById('card-fields').hidden === true);
  assert.match(await p.textContent('#card-off-note'), /カードは作りません \(保存しても/);
});

await ta('[41] 新商品 (セット・#1628 Codex R3 M1 / L3): 構成品の照合が 503 = 答えにせず 3 秒後にもう一度 (直れば保存の止めが外れる)・A → B → A と打ち直したら前の A の答えを使わない', async (p) => {
  let n503 = 0;
  await p.route('**/api/lookup?code=k001', async (route) => {
    if (n503 === 0) { n503++; await route.fulfill({ status: 503, contentType: 'application/json', body: JSON.stringify({ ok: false, error: 'Company DB につながりません' }) }); }
    else await route.continue();
  });
  await p.goto(B + '/new?kind=set');
  const rows = p.locator('#comp-rows tr.comp-row');
  await rows.nth(0).locator('.c-code').fill('k001');
  await rows.nth(0).locator('.c-code').press('Tab');
  await p.waitForFunction(() => /3 秒後にもう一度/.test(document.querySelector('#comp-rows .c-name').textContent));
  assert.match(await p.textContent('#checklist'), /構成品を確かめています/, '503 は「確かめている」のまま (できないコードと決めない)');
  assert.ok(!/構成品にできないコード/.test(await p.textContent('#checklist')));
  await p.waitForFunction(() => /国産 はちみつ/.test(document.querySelector('#comp-rows .c-name').textContent), null, { timeout: 10000 });
  assert.equal(n503, 1);
  assert.ok(!/構成品を確かめています|構成品にできないコード/.test(await p.textContent('#checklist')), '直ったら止めが外れる');
  // A → B → A (欄を離れずに 0.5 秒以内) = 前の A の答えは捨てる (① で止まる)
  await rows.nth(0).locator('.c-code').fill('k002');
  await rows.nth(0).locator('.c-code').fill('k001');
  assert.match(await p.textContent('#checklist'), /構成品を確かめています/, '打ち直した瞬間に古い答えを捨てる');
  await p.waitForFunction(() => /国産 はちみつ/.test(document.querySelector('#comp-rows .c-name').textContent));
  assert.ok(!/構成品を確かめています/.test(await p.textContent('#checklist')));
});

await ta('[42] 新商品 (#1628 Codex R3 M2): 「作らない」の理由の欄は、カードを作らないときも出す・理由が無くて断られたら理由の欄へ', async (p) => {
  await p.goto(B + '/new?kind=single');
  await p.click('#set-plan button[data-v="none"]');
  await p.click('#card-create button[data-v="0"]');
  assert.equal(await p.isHidden('#card-fields'), false);
  assert.equal(await p.isHidden('#row-set-reason'), false, '作らない理由の欄も出す');
  await p.fill('#code', 'ui-setplan-1');
  await p.waitForSelector('#code-msg.ok');
  await p.fill('#f-name', 'セット判断の試験');
  await p.fill('#f-standard_price', '700');
  await p.click('#f-tax_rate button[data-v="0.1"]');
  await p.selectOption('#shipping', 'S01');
  await p.selectOption('#f-primary_supplier', '0001');
  await fillReq(p);
  await noneRest(p);
  await p.click('#save');
  await p.waitForSelector('.result.err');
  assert.match(await p.textContent('.result.err'), /作らない理由を選んでください/);
  assert.equal(await p.locator('#row-set-reason.err').count(), 1, '理由の欄に印');
  assert.equal(await active(p), 'set-reason', '理由の欄へ');
  assert.equal(await skuRow('ui-setplan-1'), undefined);
  // 「入力なし」(前の「まだ決めない」) にして空にすれば畳む
  await p.click('[data-row="card.set_decision"] .nonechk');
  assert.equal(await p.getAttribute('#set-plan', 'data-value'), '', '入力なし = 選んだ値を消す');
  await p.focus('#f-name');
  await p.waitForFunction(() => document.getElementById('card-fields').hidden === true);
});

await ta('[43] 新商品 (セット・#1628 Codex R4 M1): 確かめ直しは 3 回まで → 「もう一度確かめる」(保存は止めたまま)・401/403 は自動で繰り返さない・行を消したら確かめ直しも止まる', async (p) => {
  await p.addInitScript(() => { window.__meRetryWaits = [150, 300, 450]; });   // 本物は 3 秒・10 秒・30 秒
  const hits = { k002: 0, k003: 0, k004: 0 };
  let k002Down = true;
  await p.route('**/api/lookup?code=*', async (route) => {
    const code = new URL(route.request().url()).searchParams.get('code');
    if (code in hits) hits[code]++;
    if (code === 'k002' && k002Down) return route.fulfill({ status: 503, contentType: 'application/json', body: JSON.stringify({ ok: false, error: 'Company DB につながりません' }) });
    if (code === 'k003') { await new Promise((r) => setTimeout(r, 700)); try { await route.fulfill({ status: 503, contentType: 'application/json', body: '{}' }); } catch { /* 画面が止めた (行を消した) */ } return; }
    if (code === 'k004') return route.fulfill({ status: 403, contentType: 'application/json', body: JSON.stringify({ ok: false, error: 'このアプリの権限がありません' }) });
    return route.continue();
  });
  await p.goto(B + '/new?kind=set');
  const rows = p.locator('#comp-rows tr.comp-row');
  await rows.nth(0).locator('.c-code').fill('k002');
  await rows.nth(0).locator('.c-code').press('Tab');
  await p.waitForSelector('#comp-rows tr.comp-row:nth-child(1) button[data-act="relookup"]', { timeout: 10000 });
  assert.equal(hits.k002, 4, '最初の 1 回 + 確かめ直し 3 回で止まる');
  await p.waitForTimeout(1200);
  assert.equal(hits.k002, 4, '上限の後は自動で聞かない');
  assert.match(await p.textContent('#checklist'), /構成品を確かめています/, '保存は止めたまま');
  k002Down = false;
  await rows.nth(0).locator('button[data-act="relookup"]').click();
  await p.waitForFunction(() => /ハチミツ/.test(document.querySelector('#comp-rows .c-name').textContent));
  assert.ok(!/構成品を確かめています/.test(await p.textContent('#checklist')), '「もう一度確かめる」で直る');
  // 401/403 = 自動で繰り返さない (ログインし直す・開き直す)
  await rows.nth(1).locator('.c-code').fill('k004');
  await rows.nth(1).locator('.c-code').press('Tab');
  await p.waitForSelector('#comp-rows tr.comp-row:nth-child(2) button[data-act="relookup"]');
  assert.match(await rows.nth(1).locator('.c-name').textContent(), /ログインし直すか/);
  await p.waitForTimeout(800);
  assert.equal(hits.k004, 1, '403 は自動で繰り返さない');
  // 返事待ちの行を消したら、遅れて来た返事で確かめ直しを始めない (#1628 Codex R4 M1)
  await rows.nth(1).locator('.c-code').fill('k003');
  await rows.nth(1).locator('.c-code').press('Tab');
  await p.waitForFunction(() => /引き当てています/.test(document.querySelectorAll('#comp-rows .c-name')[1].textContent));
  await rows.nth(1).locator('button[data-act="del"]').click();
  await p.waitForTimeout(2000);
  assert.equal(hits.k003, 1, '消した行は確かめ直さない');
});

await ta('[44] 新商品 (#1628 Codex R4 M2): カードを作らない + Yahoo! の欄だけ値がある = 「Yahoo! の欄も確かめる」と出す・Yahoo!売価 0 で保存 = その欄を開いて止める', async (p) => {
  await p.goto(B + '/new?kind=single');
  await p.click('#sec-yahoo > summary');
  await p.fill('#y-price', '0');
  await p.click('#sec-yahoo > summary');   // 畳む
  await p.click('#card-create button[data-v="0"]');
  assert.equal(await p.isHidden('#card-fields'), true, 'カードの欄は空 = 畳む');
  assert.match(await p.textContent('#card-off-note'), /「Yahoo! を楽天と変えるときだけ」の欄に入れた値は保存のときに確かめます/);
  await p.fill('#code', 'ui-yahoo-1');
  await p.waitForSelector('#code-msg.ok');
  await p.fill('#f-name', 'Yahoo! の試験');
  await p.fill('#f-standard_price', '800');
  await p.click('#f-tax_rate button[data-v="0.1"]');
  await p.selectOption('#shipping', 'S01');
  await p.selectOption('#f-primary_supplier', '0001');
  await fillReq(p);
  await noneRest(p);
  await p.click('#save');
  assert.match(await p.textContent('#msg'), /Yahoo!売価は 1 円以上/);
  assert.equal(await p.evaluate(() => document.getElementById('sec-yahoo').open), true, 'Yahoo! の欄を開く');
  assert.equal(await active(p), 'y-price', 'その欄へ');
  assert.equal(await skuRow('ui-yahoo-1'), undefined);
  await p.fill('#y-price', '');
  await p.focus('#f-name');
  await p.waitForFunction(() => /保存しても product-hub/.test(document.getElementById('card-off-note').textContent));
});

await ta('[37] Amazon SKU (10/5): 新しい対応 = 名前で未保存 1 件 (理由は数えない)・離れるときの確認・Ctrl+S → 読み直し (知らせ)・削除の理由は数えない・削除は 1 回だけ確かめる (Esc で戻る)・墓標・変更の記録のカード', async (p) => {
  await p.goto(B + '/amazon/');
  assert.match(await p.textContent('#h-um'), /売れたのに対応が無い SKU/);
  await p.fill('#open-sku', 'pr-k001-b');
  await Promise.all([p.waitForNavigation(), p.press('#open-sku', 'Enter')]);
  assert.match(p.url(), /amazon\/sku\?sku=pr-k001-b$/);
  assert.match(await p.textContent('.idrow'), /対応なし/);
  assert.equal(await dirty(p), 0);
  assert.equal(await p.inputValue('#comp-rows tr.comp-row .c-code'), 'k001', '今の構成 (夜間の取り込み) から始まる');
  assert.match(await p.textContent('#comp-rows tr.comp-row'), /代表/);
  await p.fill('#name', 'UI の出品');
  assert.equal(await dirty(p), 1);
  assert.match(await p.textContent('#save-diff'), /名前 \(社内\)[\s\S]*UI の出品/);
  assert.match(await p.textContent('#save-impact-list'), /新しく作ります[\s\S]*07:00|新しく作ります[\s\S]*7:00/);
  await p.fill('#reason', 'UI の試験');
  assert.equal(await dirty(p), 1, '保存の理由は数えない');
  await p.click('.rail a[aria-label="商品・セット"]');
  assert.equal(await p.locator('#leave-bg.on').count(), 1);
  await p.click('#leave-stay');
  await p.keyboard.press('Control+s');
  await p.waitForSelector('#saved-note');
  assert.match(await p.textContent('#saved-note'), /保存しました[\s\S]*新しい対応を作りました/);
  assert.deepEqual(await mapOf('pr-k001-b'), { name: 'UI の出品', state: 'active' });
  assert.equal(await dirty(p), 0);
  assert.equal(await p.inputValue('#name'), 'UI の出品', '読み直した後の値');
  // 削除 = 理由を入れると押せる・未保存には数えない・1 回だけ確かめる
  await p.click('#del-box > summary');
  assert.equal(await p.isDisabled('#del'), true);
  await p.fill('#del-reason', 'UI の試験で消す');
  assert.equal(await dirty(p), 0, '削除の理由は数えない');
  assert.equal(await p.isDisabled('#del'), false);
  await p.click('#del');
  assert.equal(await p.locator('#del-bg.on').count(), 1);
  assert.match(await p.textContent('#del-why'), /UI の試験で消す/);
  assert.equal(await active(p), 'del-stay', '確かめの窓は「やめる」から');
  await p.keyboard.press('Escape');
  assert.equal(await p.locator('#del-bg.on').count(), 0);
  assert.equal(await active(p), 'del', 'Esc で押した所へ戻る');
  assert.equal((await mapOf('pr-k001-b')).state, 'active', 'やめたので消していない');
  await p.click('#saved-note-close');
  await p.click('#del');
  await p.click('#del-go');
  await p.waitForSelector('#saved-note');
  assert.match(await p.textContent('#saved-note'), /削除 \(墓標に\) しました/);
  assert.equal((await mapOf('pr-k001-b')).state, 'deleted');
  assert.match(await p.content(), /削除済み \(墓標\) です/);
  await Promise.all([p.waitForNavigation(), p.click('.ph-actions a:has-text("変更の記録")')]);
  assert.ok(await p.locator('.hcard').count() >= 2, '保存 1 回 = 1 枚');
  assert.match(await p.textContent('#hist-cards'), /UI の試験で消す/);
  await p.click('#hist-filter button[data-hf="load"]').catch(() => {});
  // 対応の無い seller SKU を、夜間の取り込みの名前・構成のまま (変えた欄 0) 新しく登録できる (#1628 Codex R1 M1)
  await pg.query("update core.listings set title = 'はちみつ 3 個組' where listing_code = 'pr-k001-3p'");
  await p.goto(B + '/amazon/sku?sku=pr-k001-3p');
  assert.equal(await dirty(p), 0);
  assert.equal(await p.inputValue('#name'), 'はちみつ 3 個組');
  assert.equal(await p.isDisabled('#save'), false, '初期値のままでも保存できる');
  assert.match(await p.textContent('#save-impact-list'), /新しく作ります \(今の名前・構成のまま\)/);
  assert.match(await p.textContent('#save-empty'), /このまま保存すると|この名前・構成のまま保存すると/);
  await p.click('#save');
  await p.waitForSelector('#saved-note');
  assert.match(await p.textContent('#saved-note'), /新しい対応を作りました/);
  assert.deepEqual(await mapOf('pr-k001-3p'), { name: 'はちみつ 3 個組', state: 'active' });
  assert.equal(await p.isDisabled('#save'), true, '作った後は変えた欄が無ければ押せない');
});

await ta('[38] NE 登録の CSV (🆕 10/20 ボタン 1 つ): 「NE 登録の CSV を作る」= 作って配ってその場でダウンロード → 「NE に取り込んだ」1 つで申告 → 前のファイルは「これまでの記録」(たたむ) の中で申告し直せる (書きかけ = 未保存・違うファイルは照合で止める)', async (p, ctx) => {
  process.env.MASTER_DECISION_APPROVERS = 'naka@test';
  // 今日の照合の回と NE の元のコード (前からある商品 = NE にある / ui-new-1 = 無い)
  const run = 'mc_' + new Date().toISOString().replace(/[-:.]/g, '') + '_abcdef';
  await pg.query('insert into ops.master_compare_runs (compare_run_id, observed_at, candidates) values ($1, now(), 0)', [run]);
  const codes = (await pg.query("select code_norm from core.skus where code_norm <> 'ui-new-1'")).rows.map((r) => r.code_norm);
  await pg.query('select ops.record_ne_codes($1::jsonb)', [JSON.stringify({ compare_run_id: run, entries: codes.map((c) => ({ code_norm: c, kind: 'product', state: 'ok', ne_code: c, spellings: [c] })) })]);
  // 0058: 本番と同じく、その照合の回の封 → ゲートの許可 (配る = 許可の回と NE のコードの回が同じときだけ・配ったファイルは配った時の許可の間だけ渡す)
  await (await import('./fixtures/master-widen.mjs')).seedNewEntryLease(db, { runId: run, withSet: true });
  try {
    await p.goto(B + '/reg-csv');
    assert.match(await p.textContent('#next'), /ui-new-1/);
    assert.equal(await p.locator('#history').evaluate((d) => d.open), false, '前のファイルはたたんである');
    const buildBtn = p.locator('button[data-act="build-all"]');
    assert.equal(await buildBtn.isDisabled(), false);
    assert.equal(await dirty(p), 0);
    // ボタン 1 つ = 作る → 配る → ダウンロード (画面は移らない)
    const [dl] = await Promise.all([p.waitForEvent('download'), buildBtn.click()]);
    const file = path.join(DATA_TMP, 'ui-reg.csv');
    await dl.saveAs(file);
    assert.match(fs.readFileSync(file, 'utf8'), /ui-new-1/);
    await p.waitForSelector('.saved-note:has-text("CSV ができました")');
    assert.ok(/\/reg-csv$/.test(new URL(p.url()).pathname), p.url());
    // 「NE で取り込む」のカード = 「NE に取り込んだ」1 つ
    const todo = p.locator('#todo article.todo-card').first();
    assert.match(await todo.textContent(), /単品 \d+ 件の CSV/);
    await todo.locator('button[data-act="declare-quick"][data-result="ok"]').click();
    await p.waitForSelector('.saved-note:has-text("NE に取り込んだ」を記録しました")');
    const st = (await pg.query("select r.state from ops.master_registrations r join core.skus s on s.sku_id = r.sku_id where s.code_norm = 'ui-new-1'")).rows[0].state;
    assert.equal(st, 'ne_pending');
    assert.equal(await p.locator('#todo article.todo-card').count(), 0, '取り込んだファイルのカードは消える');
    // 「これまでの記録」の中のファイル = 申告し直せる (書きかけ = 未保存・違うファイルは照合で止める)
    await p.click('#history > summary');
    const c2 = p.locator('#history article.exp').first();
    await c2.locator('button[data-act="show-declare"]').first().click();
    await c2.locator('input[name="ne_message"]').fill('1件成功しました。');
    assert.equal(await dirty(p), 1, '申告の書きかけ = 未保存');
    await p.click('.rail a[aria-label="つかいかた"]');
    assert.equal(await p.locator('#leave-bg.on').count(), 1);
    assert.match(await p.textContent('#leave-list'), /の申告 \(書きかけ\)/);
    await p.click('#leave-stay');
    const bad = path.join(DATA_TMP, 'ui-bad.csv');
    fs.writeFileSync(bad, 'not the file');
    await c2.locator('input[data-sha-file]').setInputFiles(bad);
    await p.waitForSelector('[data-drop].bad');
    await c2.locator('label.choice.r-ok').click();
    await c2.locator('button[data-act="declare"]').click();
    assert.match(await c2.locator('[data-drawer="declare"] [data-msg]').textContent(), /sha256 が合いません/);
    await c2.locator('input[data-sha-file]').setInputFiles(file);
    await p.waitForSelector('[data-drop].done');
    // いまの時刻を入れる (#1628 Codex R1 M3): 秒まで・今の時刻 = 配った直後 (配った時刻の秒が 0 でない) でも断られない
    await c2.locator('[data-now]').click();
    const imp = await c2.locator('input[name="imported_at"]').inputValue();
    assert.match(imp, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$/, '秒まで入る: ' + imp);
    assert.ok(Math.abs(new Date(imp).getTime() - Date.now()) < 5000, imp);
    await c2.locator('button[data-act="declare"]').click();
    await p.waitForSelector('.saved-note:has-text("申告しました")');
    const att = (await pg.query('select imported_at, declared_at from ops.ne_reg_attempts order by attempt_id desc limit 1')).rows[0];
    assert.ok(att.imported_at && new Date(att.imported_at) <= new Date(att.declared_at), JSON.stringify(att));
    assert.equal(await dirty(p), 0);
  } finally { delete process.env.MASTER_DECISION_APPROVERS; }
});

await ta('[60] NE 登録の CSV (10/12 中原さんの画面の写真): 試し用・閉じた・商品が全部 NE で確かめ済みのファイル = 引退した v1 は「この形を実機で確かめた」「問題があった (ng)」と「本番の行数で作れるようになります」を出さず「引退しました … このファイルですることはありません」/ 門の無い v2・まとまりも出さない / 🆕 0100: セットも v1 は引退・v2 は門なし = どれにもボタンを出さない', async (p) => {
  process.env.MASTER_DECISION_APPROVERS = 'naka@test';
  const { seedTrialDoneExport } = await import('./fixtures/reg-csv-trial-done.mjs');
  const ids = {};
  for (const [schema, code] of [['ne-reg-single-v1', 's003'], ['ne-reg-set-v1', 'set001'], ['ne-reg-single-v2', 'k004'], ['ne-reg-variation-v1', 'k002'], ['ne-reg-set-v2', 'set001']]) ids[schema] = await seedTrialDoneExport(pg, { sessionUser, schema, code });
  try {
    await p.goto(B + '/reg-csv');
    // 🆕 0100: 門のある今の版は無い = 上の「NE で取り込む」に「問題なし (この形で本番にする)」は出ない・前のファイルは「これまでの記録」(たたんである) の中
    for (const k of ['ne-reg-single-v1', 'ne-reg-single-v2', 'ne-reg-variation-v1', 'ne-reg-set-v1', 'ne-reg-set-v2']) assert.equal(await p.locator(`#todo article[data-id="${ids[k]}"]`).count(), 0, k);
    await p.click('#history > summary');
    const card = (k) => p.locator(`#history article.exp[data-id="${ids[k]}"]`);
    const v1 = card('ne-reg-single-v1');
    await v1.waitFor();
    assert.equal(await v1.locator('button[data-act="verified"]').count(), 0, 'v1 = 確かめた / ng のボタンを出さない');
    const t1 = await v1.textContent();
    assert.match(t1, /形 ne-reg-single-v1/);
    assert.match(t1, /この形 \(ne-reg-single-v1\) は引退しました。今の形 \(ne-reg-single-v2\) は実機の確かめが要らず、最初から本番の行数で作れます。このファイルですることはありません/);
    assert.doesNotMatch(t1, /本番の行数で作れるようになります/);
    assert.equal(await v1.locator('[data-verify-none="retired"]').isVisible(), true);
    for (const k of ['ne-reg-single-v2', 'ne-reg-variation-v1', 'ne-reg-set-v2']) {
      const c = card(k);
      assert.equal(await c.locator('button[data-act="verified"]').count(), 0, k);
      assert.equal(await c.locator('[data-verify-none="no_gate"]').isVisible(), true, k);
      assert.match(await c.textContent(), /実機の確かめが要(らず|りません).*このファイルですることはありません/, k);
    }
    // 🆕 0100: セットの v1 も引退 = ボタンなし・「引退しました」の一言 (今の形 = ne-reg-set-v2)
    const set = card('ne-reg-set-v1');
    assert.equal(await set.locator('button[data-act="verified"]').count(), 0);
    assert.match(await set.textContent(), /この形 \(ne-reg-set-v1\) は引退しました。今の形 \(ne-reg-set-v2\) は実機の確かめが要らず、最初から本番の行数で作れます/);
    assert.doesNotMatch(await set.textContent(), /本番の行数で作れるようになります/);
  } finally { delete process.env.MASTER_DECISION_APPROVERS; }
});

await ta('[45] 新商品 (#1628 Codex R5 L1): Yahoo! の 1 行に 2 つある欄 (佐川・path) の誤り = 畳んだ Yahoo! の欄を開き、その欄に印を付けてそこへ', async (p) => {
  await p.goto(B + '/new?kind=single');
  await p.fill('#code', 'ui-yahoo-2');
  await p.waitForSelector('#code-msg.ok');
  await p.fill('#f-name', 'Yahoo! の欄の試験');
  await p.fill('#f-standard_price', '800');
  await p.click('#f-tax_rate button[data-v="0.1"]');
  await p.selectOption('#shipping', 'S01');
  await p.selectOption('#f-primary_supplier', '0001');
  await fillReq(p);
  for (const [id, val, re] of [['y-price-sagawa', 'abc', /Yahoo!売価 \(佐川\)/], ['y-path', 'a\tb', /Yahoo!path/]]) {
    await p.evaluate(() => { document.getElementById('sec-yahoo').open = true; });
    await p.fill('#' + id, val);
    await noneRest(p);   // 🆕 2026-10-13: ほかの項目は入力なし (値を入れた欄は入力なしにしない)
    await p.evaluate(() => { document.getElementById('sec-yahoo').open = false; });   // 畳む
    assert.equal(await p.evaluate(() => document.getElementById('sec-yahoo').open), false);
    await p.click('#save');
    await p.waitForSelector('.result.err');
    assert.match(await p.textContent('.result.err'), re);
    assert.equal(await p.evaluate(() => document.getElementById('sec-yahoo').open), true, id + ': Yahoo! の欄を開く');
    assert.equal(await active(p), id, id + ': その欄へ');
    assert.equal(await p.evaluate((x) => document.getElementById(x).closest('.f').classList.contains('err'), id), true, id + ': 誤りの印');
    await p.fill('#' + id, '');
    await p.evaluate(() => { document.getElementById('result').innerHTML = ''; document.querySelectorAll('.f.err').forEach((x) => x.classList.remove('err')); });
  }
  assert.equal(await skuRow('ui-yahoo-2'), undefined);
});

await ta('[46] Amazon SKU (#1628 Codex R5 L2): 変わった項目が無い保存 (数 3 → 03) の後は読み直す = その後に構成を変えて「元に戻す」で未保存 0', async (p) => {
  await p.goto(B + '/amazon/sku?sku=pr-k001-3p');
  const qty = p.locator('#comp-rows tr.comp-row .c-qty').first();
  assert.equal(await qty.inputValue(), '3');
  await qty.fill('03');
  assert.equal(await dirty(p), 1);
  await p.click('#save');
  await p.waitForSelector('#saved-note');
  assert.match(await p.textContent('#saved-note'), /変わった項目がありません/);
  assert.equal(await p.locator('#comp-rows tr.comp-row .c-qty').first().inputValue(), '3', '読み直した = サーバーの値');
  assert.equal(await dirty(p), 0);
  await p.locator('#comp-rows tr.comp-row .c-qty').first().fill('5');
  assert.equal(await dirty(p), 1);
  await p.click('#revert');
  assert.equal(await p.locator('#comp-rows tr.comp-row .c-qty').first().inputValue(), '3');
  assert.equal(await dirty(p), 0, '元に戻したら未保存 0');
  assert.equal(await p.isDisabled('#save'), true);
});

// MASTER_EDIT_UI_SHOTS_NAME=フォルダ を付けると [47] の写し (1440 幅) を残す
const SHOTS_NAME = process.env.MASTER_EDIT_UI_SHOTS_NAME || '';
const shotName = async (p, file) => { if (!SHOTS_NAME) return; await p.evaluate(() => window.scrollTo(0, 0)); await p.waitForTimeout(150); await p.screenshot({ path: `${SHOTS_NAME}/${file}.png` }); };
await ta('[47] 名前を見出しのところで直す (10/6): 単品・セットとも ✎ で見出しが入力欄になる・名前の欄は 1 つだけ・Esc で戻す・変えた間は閉じない・Ctrl+S → 見出しが新しい名前 / 🆕 0093 NE 登録の CSV を配った後も直せるの印', async (p) => {
  // 🆕 0093 (2026-10-19 中原さん): NE 登録の CSV を配って申告した ui-new-1 ([38]) も、ほかの商品と同じように見出しで直せる (🔒 にしない)
  for (const [code, next, label] of [['k005', 'abc 小袋 改', '単品'], ['set001', 'セット 1 新', 'セット'], ['ui-new-1', 'UI 新商品 1 改', '単品 (NE 登録の CSV を配った後)']]) {
    await p.goto(B + '/sku/' + code);
    assert.equal(await p.locator('[data-field="name"]').count(), 1, label + ': 名前の欄は 1 つだけ (下の「基本」に無い)');
    assert.equal(await p.locator('#p-basic [data-field="name"]').count(), 0);
    assert.equal(await p.isVisible('#name-edit'), true, label + ': 見出しの横に「名前を直す」');
    assert.equal(await p.isVisible('#f-name'), false);
    const before = await p.textContent('#sku-title');
    await shotName(p, label + '_直せるとき_見出し');
    await p.click('#name-edit');
    assert.equal(await active(p), 'f-name', label + ': 押すと見出しの場所で入力欄');
    assert.equal(await p.evaluate(() => document.getElementById('sku-title').classList.contains('sr')), true, label + ': 見出しは見た目だけ隠す');
    assert.equal(await p.getByRole('heading', { level: 1 }).count(), 1, label + ': 編集中も h1 (商品名) が読み上げの見出しに残る (#1631 Codex R1 L2)');
    await p.keyboard.press('Escape');
    assert.equal(await p.evaluate(() => document.getElementById('sku-title').classList.contains('sr')), false, label + ': Esc で戻す');
    assert.equal(await dirty(p), 0);
    await p.click('#name-edit');
    await p.fill('#f-name', next);
    assert.equal(await dirty(p), 1);
    assert.match(await p.textContent('#save-diff'), new RegExp((label === 'セット' ? 'セット名' : '名前') + '[\\s\\S]*' + next));
    await p.click('#name-close');
    assert.equal(await p.isVisible('#f-name'), true, label + ': 変えた間は閉じない');
    await shotName(p, label + '_直せるとき_入力中');
    await p.keyboard.press('Control+s');
    await p.waitForSelector('#saved-note');
    assert.equal((await p.textContent('#sku-title')).trim(), next, label + ': 保存の後は見出しが新しい名前');
    assert.notEqual(before.trim(), next);
    assert.equal((await row(code)).name, next);
    assert.equal(await p.isVisible('#f-name'), false, label + ': 読み直した後は見出しに戻る');
  }
  // 配った後の ui-new-1 に 🔒・案内のカードを出さない (0093)
  assert.equal(await p.locator('.name-lock').count(), 0);
  assert.equal(await p.getByText('鍵がかかっています').count(), 0);
  // 🔒 = 保存を開いていない (セット)。入力欄を出さず、見出しの横に理由
  delete process.env.MASTER_EDIT_OPEN;
  try {
    await p.goto(B + '/sku/set001');
    assert.equal(await p.locator('#name-edit').count(), 0);
    assert.equal(await p.locator('#f-name').count(), 0);
    assert.match(await p.textContent('.name-lock'), /切替前|いまは保存できません/);
    await shotName(p, 'セット_🔒_保存を開いていない');
  } finally { process.env.MASTER_EDIT_OPEN = '1'; }
});

await ta('[48] 名前の欄 (#1631 Codex R1): 日本語入力の変換中の Esc は名前を戻さない・変換中の Ctrl+S は保存しない・空の名前で断られたら名前の欄へ (aria-invalid・誤りの文と結ぶ)・ふだんの Esc は戻す', async (p) => {
  await p.goto(B + '/sku/k004');
  const posts = [];
  p.on('request', (rq) => { if (rq.method() === 'POST' && /\/api\/sku\//.test(rq.url())) posts.push(rq.url()); });
  const before = await p.inputValue('#f-name');
  await p.click('#name-edit');
  await p.fill('#f-name', before + ' へんかんちゅう');
  const key = (init) => p.evaluate((x) => document.getElementById('f-name').dispatchEvent(new KeyboardEvent('keydown', { ...x, bubbles: true, cancelable: true })), init);
  await key({ key: 'Escape', isComposing: true });
  assert.equal(await p.inputValue('#f-name'), before + ' へんかんちゅう', '変換中の Esc では名前を戻さない');
  assert.equal(await p.isVisible('#name-box'), true);
  await key({ key: 's', ctrlKey: true, isComposing: true });
  await p.waitForTimeout(400);
  assert.deepEqual(posts, [], '変換中の Ctrl+S は保存しない');
  assert.equal(await dirty(p), 1);
  // 空の名前 = サーバーが断る (名前が空です) → 名前の欄を開いてそこへ・誤りの文と結ぶ
  await p.fill('#f-name', '');
  await p.click('#save');
  await p.waitForSelector('#save-err');
  assert.equal(posts.length, 1);
  assert.match(await p.textContent('#save-err'), /名前が空です/);
  assert.equal(await active(p), 'f-name', '保存のボタンに残さず名前の欄へ');
  assert.equal(await p.getAttribute('#f-name', 'aria-invalid'), 'true');
  assert.equal(await p.getAttribute('#f-name', 'aria-describedby'), 'save-err');
  assert.equal(await p.locator('[data-row="name"].err').count(), 1, '名前の行に誤りの印');
  // 誤りの印 (aria-invalid・aria-describedby・行の赤) は、打ち直し・Esc・元に戻すのどれでもまとめて外れる (#1631 Codex R2 L1)
  const errState = () => p.evaluate(() => { const i = document.getElementById('f-name'); return [i.getAttribute('aria-invalid'), i.getAttribute('aria-describedby'), document.querySelectorAll('[data-row="name"].err').length]; });
  const failEmpty = async () => { await p.fill('#f-name', ''); await p.click('#save'); await p.waitForFunction(() => document.getElementById('f-name').getAttribute('aria-invalid') === 'true'); assert.deepEqual(await errState(), ['true', 'save-err', 1]); };
  await p.type('#f-name', 'x');
  assert.deepEqual(await errState(), [null, null, 0], '打ち直したら印が全部外れる');
  await failEmpty();
  // ふだんの Esc = 直す前に戻して閉じる・印も外れる (開き直しても古い誤りを読み上げない)
  await p.keyboard.press('Escape');
  assert.equal(await p.inputValue('#f-name'), before);
  assert.equal(await p.isVisible('#name-box'), false);
  assert.equal(await dirty(p), 0);
  assert.deepEqual(await errState(), [null, null, 0], 'Esc の後は印が全部外れる');
  await p.click('#name-edit');
  await failEmpty();
  await p.click('#revert');
  assert.equal(await p.inputValue('#f-name'), before);
  assert.deepEqual(await errState(), [null, null, 0], '元に戻すの後も印が全部外れる');
});

// 第 2 段の画面の幅: 1440 / 1280 / 1024 と 1280×720 の 150% で横にはみ出さない・板からはみ出さない・構成の表は横に送る囲いの中。
// MASTER_EDIT_UI_SHOTS_STAGE2=フォルダ を付けると 1440 / 1280 / 1024 の写しを残す (目で見る用。MASTER_EDIT_UI_SHOTS2 は [20] / [24] の写しで使っている)
const SHOTS2 = process.env.MASTER_EDIT_UI_SHOTS_STAGE2 || '';
const fillNew = async (p) => { await p.fill('#code', 'shot-new-1'); await p.waitForSelector('#code-msg.ok'); await p.fill('#f-name', '国産 はちみつ レモン 500g'); await p.fill('#f-standard_price', '1680'); await p.click('#f-tax_rate button[data-v="0.08"]'); };
const fillSet = async (p) => {
  const rows = p.locator('#comp-rows tr.comp-row');
  await rows.nth(0).locator('.c-code').fill('k001'); await rows.nth(0).locator('.c-code').press('Tab');
  await rows.nth(1).locator('.c-code').fill('k002'); await rows.nth(1).locator('.c-code').press('Tab');
  await p.waitForFunction(() => /ハチミツ/.test(document.querySelectorAll('#comp-rows .c-name')[1].textContent));
  await p.fill('#f-name', 'はちみつ 2 種 ギフト');
};
const SCREENS2 = [
  ['新商品_単品', '/new?kind=single', fillNew], ['新商品_セット', '/new?kind=set', fillSet],
  ['Amazon_一覧', '/amazon/', null], ['Amazon_未登録', '/amazon/unmapped?channel=all', null], ['Amazon_1つのSKU', '/amazon/sku?sku=pr-k001', null],
  ['Amazon_変更の記録', '/amazon/sku/history?sku=pr-k001-b', null], ['Amazon_新しいSKU', '/amazon/new?sku=pr-k001-bx', null], ['NE登録のCSV', '/reg-csv', null], ['変更の記録', '/sku/s003/history', null],
];
for (const [label, vp, scale] of [['1440', { width: 1440, height: 900 }, 1], ['1280', { width: 1280, height: 720 }, 1], ['1024', { width: 1024, height: 768 }, 1], ['1280×720 150%', { width: 853, height: 480 }, 1.5]]) {
  await ta(`[39] ${label}: 第 2 段の画面 (新商品・Amazon・NE 登録の CSV・変更の記録) が横にはみ出さない`, async (p) => {
    process.env.MASTER_DECISION_APPROVERS = 'naka@test';   // NE 登録の CSV を操作できる人の画面で
    try {
    for (const [name, url, prep] of SCREENS2) {
      await p.goto(B + url);
      await p.waitForFunction(() => document.querySelector('.page').getAnimations().every((a) => a.playState !== 'running'));
      if (prep) await prep(p);
      const [sw, cw] = await p.evaluate(() => [document.documentElement.scrollWidth, document.documentElement.clientWidth]);
      assert.ok(sw <= cw + 1, `${name}: 横幅 ${sw} > ${cw}`);
      const spill = await p.evaluate(() => [...document.querySelectorAll('.page .panel, .page .filecard')].filter((x) => x.offsetParent !== null && x.scrollWidth > x.clientWidth + 1).map((x) => (x.querySelector('h2, .ttl') || x).textContent.trim().slice(0, 20)));
      assert.deepEqual(spill, [], `${name}: 板からはみ出している`);
      const bare = await p.evaluate(() => [...document.querySelectorAll('table.comp')].filter((t) => { const w = t.parentElement; return !w || !w.classList.contains('scrollx') || getComputedStyle(w).overflowX !== 'auto'; }).length);
      assert.equal(bare, 0, `${name}: 構成の表が横に送る囲いに入っていない`);
      if (SHOTS2 && scale === 1) await p.evaluate(() => { if (document.activeElement) document.activeElement.blur(); window.scrollTo(0, 0); }).then(() => p.waitForTimeout(150)).then(() => p.screenshot({ path: `${SHOTS2}/${name}_${label}.png`, fullPage: true }));
    }
    } finally { delete process.env.MASTER_DECISION_APPROVERS; }
  }, vp, scale);
}

// 利益 (1 個あたり・参考・10/6)。MASTER_EDIT_UI_PROFIT_SHOTS=フォルダ を付けると 1440 幅の写し (単品・セット・一覧) を残す (目で見る用)
const PROFIT_SHOTS = process.env.MASTER_EDIT_UI_PROFIT_SHOTS || '';
const pfText = (p, sel) => p.evaluate((s) => document.querySelector(s).textContent.replace(/\s+/g, ' ').trim(), sel);
await ta('[49] 利益 (1 個あたり・10/6): 単品 = 売価・税率・送料・原価を変えると保存の前にその場で計算し直す (保存すると ○ → ○)・数でない = 理由・マイナスは赤・元に戻す = いまの値 (未保存の数は変えない)', async (p) => {
  await p.goto(B + '/sku/z030');   // 売価 1,000・原価 130・税率 10%・送料 S02 (520) (ほかの試験が触らない商品)
  await p.waitForSelector('html[data-me-profit="ready"]');
  const st = async () => ({ val: await pfText(p, '#pf-val'), rate: await pfText(p, '#pf-rate'), when: await pfText(p, '#pf-when'), neg: await p.evaluate(() => document.querySelector('#pf-val').classList.contains('neg')),
    next: await p.evaluate(() => { const n = document.querySelector('#pf-next'); return n.hidden ? null : n.textContent.replace(/\s+/g, ' ').trim(); }) });
  // 1000 − 100 − 130 × 1.10 (143) − 520 = 237 円・23.7%
  assert.deepEqual(await st(), { val: '237円', rate: '23.7%', when: 'いまの値で', neg: false, next: null });
  assert.equal(await pfText(p, '#pf-calc'), '売価 1,000 − 手数料 100 (10%) − 税込原価 143 (130 × 1.10) − 配送料 520 = 利益 237 円');
  // 売価 2,500: 2500 − 250 − 143 − 520 = 1,587 円・63.5%
  await p.fill('#f-standard_price', '2500');
  assert.deepEqual(await st(), { val: '1,587円', rate: '63.5%', when: '画面の値で (未保存)', neg: false, next: '保存すると利益 237 円 (23.7%) → 1,587 円 (63.5%)' });
  assert.equal(await dirty(p), 1, '利益の欄は未保存に数えない (売価の 1 件だけ)');
  assert.equal(await pfText(p, '#pf-calc'), '売価 2,500 − 手数料 250 (10%) − 税込原価 143 (130 × 1.10) − 配送料 520 = 利益 1,587 円');
  // 税率 8%: 2500 − 250 − 140.4 − 520 = 1589.6 → 1,590
  await p.click('.seg[data-field="tax_rate"] button[data-v="0.08"]');
  assert.equal((await st()).val, '1,590円');
  // 送料 S01 (210): 2500 − 250 − 140.4 − 210 = 1899.6 → 1,900
  await p.selectOption('#f-shipping_code', 'S01');
  assert.equal((await st()).val, '1,900円');
  assert.equal(await pfText(p, '#pf-calc'), '売価 2,500 − 手数料 250 (10%) − 税込原価 140.4 (130 × 1.08) − 配送料 210 = 1,899.6 → 利益 1,900 円 (四捨五入)', '内訳の式と右の数が合う (円未満も出す)');
  const shot = async (name) => {
    if (!PROFIT_SHOTS) return;
    await p.evaluate(() => { if (document.activeElement) document.activeElement.blur(); document.querySelector('#profit-row').scrollIntoView({ block: 'center' }); });
    await p.waitForTimeout(150);
    await p.screenshot({ path: `${PROFIT_SHOTS}/${name}.png` });
  };
  await shot('単品_1440_売価と税率と送料を変えた');
  // 原価を変える 3,000 (カンマも読む): 2500 − 250 − 3240 − 210 = −1,200 (赤・下がった印)
  await p.click('#btn-cost-open');
  await p.fill('#cost-jpy', '3,000');
  let s = await st();
  assert.deepEqual([s.val, s.rate, s.neg], ['−1,200円', '−48.0%', true]);
  assert.equal(s.next, '保存すると利益 237 円 (23.7%) → −1,200 円 (−48.0%)');
  assert.equal(await p.evaluate(() => document.querySelector('#pf-next').classList.contains('down')), true, '利益が下がる = 赤い枠');
  await shot('単品_1440_原価を3000円にして赤字');
  // 数でない売価 = 計算しない (理由)
  await p.fill('#f-standard_price', 'abc');
  s = await st();
  assert.deepEqual([s.val, s.rate], ['—', '—']);
  assert.equal(await pfText(p, '#pf-why'), '入れた標準売価が数ではないので計算できません');
  assert.equal(await p.isHidden('#pf-calc'), true);
  // 売価を空 = 未入力 (0 円として計算しない)
  await p.fill('#f-standard_price', '');
  assert.equal(await pfText(p, '#pf-why'), '標準売価が未入力なので計算できません');
  await shot('単品_1440_売価を空にした');
  // 元に戻す = いまの値
  await p.click('#revert');
  await p.waitForTimeout(50);
  assert.deepEqual(await st(), { val: '237円', rate: '23.7%', when: 'いまの値で', neg: false, next: null });
  assert.equal(await dirty(p), 0);
  if (PROFIT_SHOTS) { await p.evaluate(() => window.scrollTo(0, 0)); await p.waitForTimeout(150); await p.screenshot({ path: `${PROFIT_SHOTS}/単品_1440_開いたとき.png`, fullPage: true }); }
});

await ta('[50] 利益 (セット・10/6): セットの売価・セットの原価 (例外原価 / 合計に戻す)・セットの税率・送料で同じ式・保存の前にその場で / 一覧の利益・利益率の列と利益の少ない順', async (p) => {
  await p.goto(B + '/sku/set001');
  await p.waitForSelector('html[data-me-profit="ready"]');
  const P = await p.evaluate(() => JSON.parse(document.getElementById('me-page').textContent).profit);
  const hand = (price, cost, tax, ship) => Math.round(price - Math.round(price * 0.1) - cost * (1 + (tax ?? 0.1)) - (ship ?? 0));
  const yen = (v) => (v < 0 ? '−' : '') + Math.abs(v).toLocaleString('ja-JP') + '円';
  assert.ok(P.price > 0 && P.cost != null && P.setCostSum != null, JSON.stringify(P));
  assert.equal(await pfText(p, '#pf-val'), yen(hand(P.price, P.cost, P.taxRate, P.shipping)), 'いまの値 (セットの売価・原価・税率・送料)');
  assert.match(await pfText(p, '#h-profit'), /利益/);
  // 例外原価 100 円 (例外のときの中)
  await p.click('details.more.panel > summary');
  await p.fill('#xcost-jpy', '100');
  assert.equal(await pfText(p, '#pf-val'), yen(hand(P.price, 100, P.taxRate, P.shipping)));
  assert.match(await pfText(p, '#pf-next'), /^保存すると利益 .+ → .+$/);
  // 例外原価をやめる (構成品の合計に戻す) = 構成品の合計
  await p.check('#xcost-clear');
  assert.equal(await pfText(p, '#pf-val'), yen(hand(P.price, P.setCostSum, P.taxRate, P.shipping)));
  await p.uncheck('#xcost-clear');
  // 売価と送料
  await p.fill('#f-standard_price', '3000');
  await p.selectOption('#f-shipping_code', 'S01');
  assert.equal(await pfText(p, '#pf-val'), yen(hand(3000, 100, P.taxRate, 210)));
  if (PROFIT_SHOTS) {
    await p.evaluate(() => { document.activeElement && document.activeElement.blur(); document.querySelector('#p-profit').scrollIntoView({ block: 'center' }); });
    await p.waitForTimeout(150);
    await p.screenshot({ path: `${PROFIT_SHOTS}/セット_1440_売価と例外原価と送料を変えた.png` });
  }
  await p.click('#revert');
  await p.waitForTimeout(50);
  assert.equal(await pfText(p, '#pf-val'), yen(hand(P.price, P.cost, P.taxRate, P.shipping)));
  assert.equal(await p.isHidden('#pf-next'), true);
  if (PROFIT_SHOTS) { await p.evaluate(() => window.scrollTo(0, 0)); await p.waitForTimeout(150); await p.screenshot({ path: `${PROFIT_SHOTS}/セット_1440_開いたとき.png`, fullPage: true }); }
  // 一覧: 列 (z030 = 237 円・23.7%)・並びのリンク → 利益の少ない順。z039 は画面の外で原価 3,000 円に = 1000 − 100 − 3300 − 520 = −2,920 (赤字・一番上)
  await changeBehind('z039', { cost: { jpy: '3000', reason: '赤字の見本' } });
  await p.goto(B + '/');
  const cell = (code, col) => p.evaluate(([c, k]) => { const a = [...document.querySelectorAll('#list-tbl a.rowlink')].find((x) => x.textContent === c); return a ? a.closest('tr').querySelector(`td[data-col="${k}"]`).textContent.replace(/\s+/g, ' ').trim() : null; }, [code, col]);
  assert.equal(await p.textContent('#th-profit'), '利益1 個・参考');
  await Promise.all([p.waitForNavigation(), p.click('#th-profit a.sorth')]);   // 10/8 PR1: 見出しを押す = 利益の小さい順
  assert.match(p.url(), /sort=profit(&|$)/);
  assert.equal(await p.getAttribute('#th-profit', 'aria-sort'), 'ascending');
  assert.equal(await cell('z030', 'profit'), '237');
  assert.equal(await cell('z030', 'profit-rate'), '23.7%');
  assert.equal(await p.textContent('#list-tbl tbody tr:first-child a.rowlink'), 'z039', '赤字が一番上');
  assert.deepEqual([await cell('z039', 'profit'), await cell('z039', 'profit-rate')], ['−2,920', '−292.0%']);
  assert.equal(await p.evaluate(() => getComputedStyle(document.querySelector('#list-tbl tbody tr:first-child td[data-col="profit"] .pf')).color), 'rgb(255, 112, 133)', 'マイナスは赤 (--err)');
  const vals = await p.evaluate(() => [...document.querySelectorAll('#list-tbl td[data-col="profit"]')].map((td) => td.textContent.replace(/仮/g, '').trim()).filter((t) => t !== '—').map((t) => Number(t.replace('−', '-').replace(/,/g, ''))));
  assert.ok(vals.length > 10);
  for (let i = 1; i < vals.length; i++) assert.ok(vals[i - 1] <= vals[i], `利益の少ない順 ${vals[i - 1]} → ${vals[i]}`);
  const [sw, cw] = await p.evaluate(() => [document.documentElement.scrollWidth, document.documentElement.clientWidth]);
  assert.ok(sw <= cw + 1, `一覧が横にはみ出さない ${sw} > ${cw}`);
  if (PROFIT_SHOTS) {
    await p.waitForFunction(() => document.querySelector('.page').getAnimations().every((a) => a.playState !== 'running'));
    await p.screenshot({ path: `${PROFIT_SHOTS}/一覧_1440_利益の少ない順.png` });
    await p.evaluate(() => { const w = document.querySelector('#list-tbl').closest('.tblwrap'); w.scrollLeft = 400; document.querySelector('#list-tbl').scrollIntoView(); });
    await p.waitForTimeout(150);
    await p.screenshot({ path: `${PROFIT_SHOTS}/一覧_1440_利益の列.png` });
  }
});

await ta('[51] 円の欄の読み = サーバーと同じ (10/6): 原価「3,000」・全角「３，０００」・空白入りでも「保存すると変わること」「保存するとこうなる」・前との差・利益が同じ数 / 整数でない = 打った字のまま・利益は理由', async (p) => {
  await p.goto(B + '/sku/z031');   // 売価 1,000・原価 131・税率 10%・送料 S02 (520) (ほかの試験が触らない商品)
  await p.waitForSelector('html[data-me-profit="ready"]');
  // 画面の読み (サーバーの intIn と同じ規則)
  assert.deepEqual(await p.evaluate(() => ['3,000', '３，０００', '3 000', '3　000', '－5', '1.5', '', 'ab', ' 42 '].map((s) => window.MasterEdit.intOf(s))), [3000, 3000, 3000, 3000, -5, null, null, null, 42]);
  const diffTo = (label) => p.evaluate((l) => { const li = [...document.querySelectorAll('#save-diff li')].find((x) => x.querySelector('.k span').textContent.startsWith(l)); return li ? li.querySelector('.to').textContent : null; }, label);
  await p.click('#btn-cost-open');
  for (const typed of ['3,000', '３，０００', '3 000']) {
    await p.fill('#cost-jpy', typed);
    assert.equal(await diffTo('原価'), '3,000 円', `${typed}: 保存すると変わること`);
    assert.match(await p.textContent('#cost-delta'), /いまの 131 円から\s*\+2,869 円/, `${typed}: 前との差`);
    assert.match(await p.textContent('#save-impact-list'), /この商品の原価は 3,000 円/, `${typed}: 保存するとこうなる`);
    assert.equal(await pfText(p, '#pf-val'), '−2,920円', `${typed}: 利益 (1000 − 100 − 3300 − 520)`);
  }
  await p.fill('#cost-jpy', 'ab');
  assert.equal(await diffTo('原価'), '「ab」(整数ではない)');
  assert.equal(await p.textContent('#cost-delta'), '');
  assert.equal(await pfText(p, '#pf-why'), '入れた原価が数ではないので計算できません');
  // 標準売価も同じ読み (全角・カンマ)
  await p.fill('#cost-jpy', '3000');
  await p.fill('#f-standard_price', '２，５００');
  assert.equal(await diffTo('標準売価'), '2,500 円');
  assert.equal(await pfText(p, '#pf-val'), '−1,570円', '2500 − 250 − 3300 − 520');
  await p.click('#revert');
  assert.equal(await dirty(p), 0);
});

const SHOTDIR = process.env.MASTER_EDIT_UI_SHOTS || '';
for (const [label, vp] of [['1440', { width: 1440, height: 900 }], ['1280', { width: 1280, height: 720 }], ['1024', { width: 1024, height: 768 }]]) {
  await ta(`[17] ${label} 幅: 一覧をスクロールしても見出しの行 (コード・名前・…) が上の帯のすぐ下に見えている・横に送っても列がずれない (10/5)`, async (p) => {
    await p.goto(B + '/');
    // 画面の出だしの動き (.page の rise) が終わってから測る (動きの途中は数 px 動く)
    await p.waitForFunction(() => document.querySelector('.page').getAnimations().every((a) => a.playState !== 'running'));
    if (SHOTDIR) await p.screenshot({ path: `${SHOTDIR}/一覧_${label}_上.png` });
    const pos = () => p.evaluate(() => {
      const th = document.querySelector('#list-tbl thead th:not(.c-chk)'); const r = th.getBoundingClientRect();   // 左のチェックの列 (まとめて変える・PR2) の次 = コード
      // 見えている所 (表の囲いの左から 40px) で、見出しの行の高さの真ん中 = 一番上に見えているのが見出しか (横に送っても)
      const wr = th.closest('.tblwrap').getBoundingClientRect();
      const hit = document.elementFromPoint(wr.left + 40, r.top + r.height / 2);
      const td = document.querySelector('#list-tbl tbody tr td:not(.c-chk)');
      return { top: r.top, left: r.left, tdLeft: td.getBoundingClientRect().left, hdr: document.querySelector('.hdr').getBoundingClientRect().bottom, seen: !!(hit && hit.closest('thead')), text: th.textContent.trim() };
    });
    const before = await pos();
    assert.ok(before.top > before.hdr + 100, '始めは表の上 (動かしていない)');
    await p.evaluate(() => window.scrollTo(0, document.querySelector('#list-tbl').getBoundingClientRect().top + window.scrollY + 500));
    await p.waitForTimeout(150);
    const after = await pos();
    assert.ok(Math.abs(after.top - after.hdr) <= 1.5, `見出しの行が帯の下 (${after.top} / 帯 ${after.hdr})`);
    assert.equal(after.seen, true, '見出しの行が行の上に見えている (重なりで隠れない)');
    assert.equal(after.text, 'コード');
    assert.ok(Math.abs(after.left - after.tdLeft) <= 1, '見出しと行の左の位置が同じ');
    // 横に送れる幅なら、送っても見出しと行の列がずれない
    const wrapScroll = await p.evaluate(() => { const w = document.querySelector('#list-tbl').closest('.tblwrap'); w.scrollLeft = 120; return w.scrollLeft; });
    if (wrapScroll > 0) {
      await p.waitForTimeout(50);
      const s = await pos();
      assert.ok(Math.abs(s.left - s.tdLeft) <= 1, '横に送っても列がずれない');
      assert.equal(s.seen, true);
      if (SHOTDIR) await p.screenshot({ path: `${SHOTDIR}/一覧_${label}_スクロールして横にも送った.png` });
      await p.evaluate(() => { document.querySelector('.tblwrap').scrollLeft = 0; });
    }
    if (SHOTDIR) await p.screenshot({ path: `${SHOTDIR}/一覧_${label}_スクロールした.png` });
    // 表の終わりより下では見出しも表の中に止まる (表の外へ出ない)
    await p.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await p.waitForTimeout(150);
    const end = await p.evaluate(() => { const t = document.querySelector('#list-tbl'); const th = t.querySelector('thead th').getBoundingClientRect(); return { thBottom: th.bottom, tblBottom: t.getBoundingClientRect().bottom }; });
    assert.ok(end.thBottom <= end.tblBottom + 1, '見出しは表の外へ出ない');
    const [sw, cw] = await p.evaluate(() => [document.documentElement.scrollWidth, document.documentElement.clientWidth]);
    assert.ok(sw <= cw + 1, `ページが横にはみ出さない ${sw} > ${cw}`);
  }, vp);
}

// ── 一覧の列・見出しの並び替え (10/8 PR1・見本 masterlist_mock)。MASTER_EDIT_UI_LIST_SHOTS=フォルダ = 見本と同じ状態の写しを残す (PR の目視用) ──
const LIST_SHOTS = process.env.MASTER_EDIT_UI_LIST_SHOTS || '';
// 列の板の列 (data-c) だけ。まとめて変えるの左のチェックの列 (PR2・.c-chk) は板の外 = 数えない
const shownHeads = (p) => p.$$eval('#list-tbl thead th[data-c]', (ths) => ths.filter((th) => !th.hidden).map((th) => th.getAttribute('data-c')));
const prefsPost = (p, body) => p.evaluate(async (b) => { const r = await fetch('api/view-prefs', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(b) }); return { status: r.status, j: await r.json() }; }, body);
const { DEFAULT_VIEW: LIST_DEFAULT } = await import('../apps/master-edit/list-columns.mjs');
await ta('[52] 一覧の列 (10/8 PR1): 列の板 = 出す / 出さない・↑↓ / つまみで並べ替え = 表がすぐ変わる・「未保存」・保存 → 別の窓 (別の PC) でも同じ・初期に戻す・Esc で閉じる', async (p) => {
  await p.goto(B + '/');
  assert.equal((await prefsPost(p, { order: [...LIST_DEFAULT.order], shown: [...LIST_DEFAULT.shown] })).status, 200);   // 前の試験の設定を消す
  await p.goto(B + '/?q=' + encodeURIComponent('単品'));
  assert.deepEqual(await shownHeads(p), LIST_DEFAULT.shown, 'いつもの列 (仕入先は出さない)');
  assert.equal(await p.textContent('#colcnt'), '17/23');   // 画像の列 (出す)・🆕 0084 メーカー・🆕 0087 大きさ・重さ・🆕 0088 ブランド・段階の 3 列 (出さない)
  assert.equal(await p.isVisible('#list-tbl tbody tr:first-child .sup-sub'), true, '仕入先の列が無い = 名前の下に「仕入先」');
  // 板を開く: コードは外せない・↑↓ も押せない・FBA などは「並べられない」
  await p.click('#colbtn');
  assert.equal(await p.isVisible('#colpanel'), true);
  assert.equal(await p.getAttribute('#colbtn', 'aria-expanded'), 'true');
  assert.equal(await p.isDisabled('#collist li[data-id="code"] input'), true);
  assert.equal(await p.isDisabled('#collist li[data-id="code"] [data-mv="1"]'), true);
  assert.equal(await p.isDisabled('#collist li[data-id="img"] [data-mv="-1"]'), true, 'コードより上には行かない (コードのすぐ後ろ = 画像 10/17)');
  assert.match(await p.textContent('#collist li[data-id="fba"]'), /並べられない/);
  assert.equal(await p.isDisabled('#cp-save'), true, '変えていない = 保存は押せない');
  // 出さない: 状態 → 表から消える (見出しも行も)・「未保存」
  await p.click('#collist li[data-id="state"] input');
  assert.ok(!(await shownHeads(p)).includes('state'));
  assert.equal(await p.isHidden('#list-tbl tbody tr:first-child td[data-col="state"]'), true);
  assert.match(await p.getAttribute('#colbtn', 'class'), /unsaved/);
  // 出す: 仕入先 → 表に出る・名前の下の「仕入先」の字は消える
  await p.click('#collist li[data-id="sup"] input');
  assert.ok((await shownHeads(p)).includes('sup'));
  assert.equal(await p.isHidden('#list-tbl tbody tr:first-child .sup-sub'), true);
  assert.match(await p.textContent('#list-tbl tbody tr:first-child td[data-col="sup"]'), /0001\s*AMC/);
  // ↑ で仕入先をコードのすぐ後ろへ (押した後もそのボタンにフォーカス = 続けて押せる)
  for (let i = 0; i < 20 && (await shownHeads(p))[1] !== 'sup'; i++) await p.click('#collist li[data-id="sup"] [data-mv="-1"]');
  assert.deepEqual((await shownHeads(p)).slice(0, 4), ['code', 'sup', 'img', 'name'], '仕入先はコードのすぐ後ろ (画像 10/17 は 1 つ後ろへ)');
  assert.equal(await p.evaluate(() => document.activeElement.closest('li')?.getAttribute('data-id')), 'sup');
  assert.deepEqual(await p.$$eval('#list-tbl tbody tr:first-child td:not(.c-chk)', (tds) => tds.filter((td) => !td.hidden).slice(0, 4).map((td) => td.getAttribute('data-col'))), ['code', 'sup', 'img', 'name'], '行のセルも同じ並び');
  // つまみで動かす (売価を名前の前へ)
  await p.dragAndDrop('#collist li[data-id="price"] .grip', '#collist li[data-id="name"]');
  assert.deepEqual((await shownHeads(p)).slice(0, 5), ['code', 'sup', 'img', 'price', 'name']);
  assert.equal(await p.textContent('#colcnt'), '17/23');   // 画像の列 (出す)・🆕 0084 メーカー・🆕 0087 大きさ・重さ・🆕 0088 ブランド・段階の 3 列 (出さない)
  if (LIST_SHOTS) await p.waitForTimeout(400), await p.screenshot({ path: `${LIST_SHOTS}/1280_cols.png` });
  // 保存 → 「未保存」が消える・別の窓 (= 別の PC。同じログイン) で開いても同じ列
  await p.click('#cp-save');
  await p.waitForFunction(() => !document.querySelector('#colbtn').classList.contains('unsaved'));
  assert.match(await p.textContent('#toast-t'), /列を保存しました \(中原 さんの設定 · 会社と家の PC で同じ\)/);
  const want = await shownHeads(p);
  const c2 = await browser.newContext({ viewport: { width: 1440, height: 900 }, locale: 'ja-JP' });
  const p2 = await c2.newPage();
  await p2.goto(B + '/');
  assert.deepEqual(await shownHeads(p2), want, '別の窓でも同じ列 (サーバーに保存)');
  assert.equal(await p2.evaluate(() => document.querySelector('#colbtn').classList.contains('unsaved')), false);
  await c2.close();
  // Esc で閉じる (列のボタンにフォーカスが戻る)
  await p.keyboard.press('Escape');
  assert.equal(await p.isHidden('#colpanel'), true);
  assert.equal(await active(p), 'colbtn');
  // 初期に戻す = いつもの列 (まだ保存していない) → 保存 = いつもの列 (設定を消す)
  await p.click('#colbtn');
  await p.click('#cp-reset');
  assert.deepEqual(await shownHeads(p), LIST_DEFAULT.shown);
  assert.match(await p.getAttribute('#colbtn', 'class'), /unsaved/);
  await p.click('#cp-save');
  await p.waitForFunction(() => !document.querySelector('#colbtn').classList.contains('unsaved'));
  assert.match(await p.textContent('#toast-t'), /いつもの列にしました/);
  await p.goto(B + '/');
  assert.deepEqual(await shownHeads(p), LIST_DEFAULT.shown);
  // 板の外を押すと閉じる
  await p.click('#colbtn');
  await p.mouse.click(5, 300);
  assert.equal(await p.isHidden('#colpanel'), true);
});
await ta('[53] 一覧の見出しで並び替え (10/8 PR1): 押す = 全件で昇順 (aria-sort)・もう一度 = 降順・表の上の「並び」・横に送ってもコードの列が残る・行のどこを押しても開く (Ctrl = 別のタブ)', async (p, ctx) => {
  await p.goto(B + '/?q=' + encodeURIComponent('単品'));
  if (LIST_SHOTS) await p.waitForTimeout(400), await p.screenshot({ path: `${LIST_SHOTS}/1280_list.png` });
  await Promise.all([p.waitForNavigation(), p.click('#list-tbl th[data-c="name"] a.sorth')]);
  assert.match(p.url(), /[?&]sort=name(&|$)/);
  assert.equal(await p.getAttribute('#list-tbl th[data-c="name"]', 'aria-sort'), 'ascending');
  assert.match(await p.textContent('#sortnow'), /並び: 名前 あいうえお・ABC 順/);
  assert.equal(await p.locator('#list-tbl th[aria-sort]').count(), 1);
  const names = await p.$$eval('#list-tbl tbody td[data-col="name"] .nm', (xs) => xs.map((x) => x.textContent));
  assert.deepEqual(names, [...names].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0)), '名前の順 (文字の番号の順)');
  await Promise.all([p.waitForNavigation(), p.click('#list-tbl th[data-c="name"] a.sorth')]);
  assert.match(p.url(), /sort=name&dir=desc/);
  assert.equal(await p.getAttribute('#list-tbl th[data-c="name"]', 'aria-sort'), 'descending');
  assert.match(await p.textContent('#sortnow'), /並び: 名前 逆の順/);
  // 原価の大きい順 (2 回押す)・絞り込みはそのまま
  await Promise.all([p.waitForNavigation(), p.click('#list-tbl th[data-c="cost"] a.sorth')]);
  await Promise.all([p.waitForNavigation(), p.click('#list-tbl th[data-c="cost"] a.sorth')]);
  assert.match(p.url(), /sort=cost&dir=desc/);
  assert.match(p.url(), /q=/);
  const costs = await p.$$eval('#list-tbl tbody td[data-col="cost"]', (xs) => xs.map((x) => Number(x.textContent.replace(/[^0-9]/g, ''))));
  assert.deepEqual(costs, [...costs].sort((a, b) => b - a), '原価の大きい順');
  if (LIST_SHOTS) await p.waitForTimeout(400), await p.screenshot({ path: `${LIST_SHOTS}/1280_sorted.png` });
  // 横に送る: コードの列は左に残る・影が出る・送った列はコードの列の下を通る (塗ってある)
  await p.setViewportSize({ width: 1024, height: 768 });
  await p.$eval('#tblwrap', (w) => { w.scrollLeft = 600; w.dispatchEvent(new Event('scroll')); });
  const [wl, cl, nl] = await p.evaluate(() => [document.querySelector('#tblwrap').getBoundingClientRect().left, document.querySelector('#list-tbl tbody tr:first-child td.c-code').getBoundingClientRect().left, document.querySelector('#list-tbl tbody tr:first-child td[data-col="name"]').getBoundingClientRect().left]);
  // 左のチェックの列 (まとめて変える・PR2・名簿の人だけ) があれば、コードの列はその幅だけ右に残る
  const chkW = await p.evaluate(() => { const c = document.querySelector('#list-tbl tbody tr:first-child td.c-chk'); return c ? c.getBoundingClientRect().width : 0; });
  assert.ok(Math.abs(cl - wl - chkW) <= 2, `コードの列が左に残る (${cl} / ${wl} + ${chkW})`);
  assert.ok(nl < wl, '名前の列は左へ送られた');
  assert.equal(await p.evaluate(() => document.querySelector('#tblwrap').classList.contains('scrolled')), true);
  const onTop = await p.evaluate(() => { const td = document.querySelector('#list-tbl tbody tr:first-child td.c-code'); const r = td.getBoundingClientRect(); const el = document.elementFromPoint(r.left + 20, r.top + r.height / 2); return td.contains(el); });
  assert.equal(onTop, true, 'コードの列の上に送った列が重ならない');
  if (LIST_SHOTS) await p.waitForTimeout(400), await p.screenshot({ path: `${LIST_SHOTS}/1024_scrolled.png` });
  await p.$eval('#tblwrap', (w) => { w.scrollLeft = 0; });
  // 行のどこを押しても開く (売価の欄・名前の欄)・Ctrl = 別のタブ (今の画面のまま)
  const code = (await p.textContent('#list-tbl tbody tr:nth-child(2) a.rowlink')).trim();
  const [popup] = await Promise.all([ctx.waitForEvent('page'), p.click('#list-tbl tbody tr:nth-child(2) td[data-col="price"]', { modifiers: ['Control'] })]);
  await popup.waitForLoadState();
  assert.match(popup.url(), new RegExp(`/sku/${code}$`));
  await popup.close();
  assert.match(p.url(), /sort=cost/, 'Ctrl のときは今の画面のまま');
  await Promise.all([p.waitForNavigation(), p.click('#list-tbl tbody tr:nth-child(2) td[data-col="name"]')]);
  assert.match(p.url(), new RegExp(`/sku/${code}$`));
});
await ta('[54] 一覧の列の板 (狭い画面 390): 下から出る・板ははみ出さない', async (p) => {
  await p.goto(B + '/?q=' + encodeURIComponent('単品'));
  const spill = () => p.evaluate(() => [...document.querySelectorAll('body *')].filter((e) => e.offsetParent !== null && e.getBoundingClientRect().right > innerWidth + 1 && !e.closest('.tblwrap')).map((e) => (e.id ? '#' + e.id : e.tagName.toLowerCase() + '.' + [...e.classList].join('.')) + ':' + Math.round(e.getBoundingClientRect().right)).slice(0, 12));
  // この画面の 390 幅のはみ出し (上の帯・詳細検索の欄) は 10/13 に直した ([67] が見る)。ここでは「列の板がはみ出しを増やさない」を見る
  const before = await spill();
  await p.click('#colbtn');
  await p.waitForTimeout(300);
  assert.deepEqual(await spill(), before, '列の板を開いてもはみ出す物は増えない');
  assert.deepEqual((await spill()).filter((x) => /colpanel|collist|cp-/.test(x)), []);   // 出てくる動き (0.15 秒・4px 上がる) が終わってから測る
  const r = await p.evaluate(() => { const b = document.querySelector('#colpanel').getBoundingClientRect(); return { left: b.left, right: b.right, bottom: b.bottom, vw: innerWidth, vh: innerHeight }; });
  assert.ok(r.left <= 1 && r.right >= r.vw - 1 && Math.abs(r.bottom - r.vh) <= 1, JSON.stringify(r));
  if (LIST_SHOTS) await p.waitForTimeout(400), await p.screenshot({ path: `${LIST_SHOTS}/390_cols.png` });
}, { width: 390, height: 844 });
await ta('[75] 一覧の画像の列 (10/17 中原さん「楽天の白抜き商品画像でいい。サムネイルみたいなかたちで」): コードのすぐ後ろ・明るい台の 52px の四角 (行の高さが画像の有無で変わらない)・取得中 / 画像なしの印・読み込めない画像は印に替える・画像を押しても商品が開く・列の板で外す / 出す・390 で横にはみ出さない', async (p, ctx) => {
  // 画像のキャッシュ = picking.db (同じ DATA_DIR)。楽天の呼び出しは止めておく (取得中のまま)・画像の取得は Playwright が返す (外へ出ない)
  const PK = await import('../apps/picking/db.js');
  const LI = await import('../apps/master-edit/list-images.mjs');
  const IM = await import('../apps/picking/images.js');
  const pdb = PK.getDB();
  const put = (ne, o) => pdb.prepare(`INSERT OR REPLACE INTO pk_product_images (ne_code, manage_number, white_bg_url, top_image_url, variant_image_url, status, fetched_at)
    VALUES (?, NULL, ?, NULL, NULL, ?, ?)`).run(ne, o.white ?? null, o.status ?? 'ok', PK.utcNow());
  put('s001', { white: 'https://image.rakuten.co.jp/b-faith/cabinet/ui-s001.jpg' });
  put('z002', { white: 'https://image.rakuten.co.jp/b-faith/cabinet/ui-broken.jpg' });
  put('s003', { status: 'not_found' });
  let open = null;
  const gate = new Promise((res) => { open = res; });
  LI.__setListImageDeps({ loadMaps: () => ({ rakutenByNe: new Map(), itemNumbers: new Map() }), fetchDetails: async (mns) => { await gate; return { items: [], failed: mns.map((m) => ({ manageNumber: m, reason: 'Not found' })) }; } });
  const PNG = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==', 'base64');
  await ctx.route('https://image.rakuten.co.jp/**', (route) => (route.request().url().includes('ui-broken') ? route.fulfill({ status: 404, body: '' }) : route.fulfill({ status: 200, contentType: 'image/png', body: PNG })));
  try {
    await p.goto(B + '/');
    assert.equal((await prefsPost(p, { order: [...LIST_DEFAULT.order], shown: [...LIST_DEFAULT.shown] })).status, 200);
    // コードで絞る (前の試験が名前を直した商品があっても同じ行が出る)
    await p.goto(B + '/?' + new URLSearchParams({ codes: ['s001', 's003', 'z000', 'z001', 'z002'].join('\n') }));
    assert.deepEqual((await shownHeads(p)).slice(0, 3), ['code', 'img', 'name'], '画像はコードのすぐ後ろ');
    const cell = (code) => `#list-tbl tbody tr:has(a.rowlink[href="sku/${code}"]) td.c-img`;
    // 画像あり: 52px の四角・明るい台・object-fit: contain・referrer を送らない
    await p.waitForFunction((sel) => { const i = document.querySelector(sel + ' img'); return i && i.complete && i.naturalWidth > 0; }, cell('s001'));
    const box = await p.$eval(cell('s001') + ' .thumb', (t) => { const r = t.getBoundingClientRect(); const i = t.querySelector('img'); const ir = i.getBoundingClientRect(); const s = getComputedStyle(t); return { w: r.width, h: r.height, iw: ir.width, ih: ir.height, bg: s.backgroundColor, fit: getComputedStyle(i).objectFit, ref: i.referrerPolicy, lazy: i.loading, altIsName: !!i.alt && i.alt === t.closest('tr').querySelector('.nm').textContent }; });
    assert.deepEqual(box, { w: 52, h: 52, iw: 50, ih: 50, bg: 'rgb(243, 245, 249)', fit: 'contain', ref: 'no-referrer', lazy: 'lazy', altIsName: true }, 'alt = 名前');
    // 読み込めない画像 (楽天で消えた) = 画像なしの印に替える (壊れた画像の印を出さない)
    await p.waitForSelector(cell('z002') + ' .thumb.none');
    assert.equal(await p.locator(cell('z002') + ' img').count(), 0);
    // 楽天に無い = 印・キャッシュに無い = 取得中
    assert.equal(await p.getAttribute(cell('s003') + ' .thumb', 'class'), 'thumb none not_found');
    assert.equal(await p.getAttribute(cell('s003') + ' .thumb', 'title'), '楽天に画像がありません');
    assert.equal(await p.getAttribute(cell('z000') + ' .thumb', 'class'), 'thumb none pending');
    assert.equal((await p.textContent(cell('z000') + ' .thumb .tw')).trim(), '取得中');
    // 行の高さは画像の有無で同じ (読み込みで動かない)
    const hs = [];
    for (const c of ['s001', 's003', 'z000', 'z001']) hs.push(await p.$eval(cell(c), (td) => Math.round(td.closest('tr').getBoundingClientRect().height)));
    assert.ok(Math.max(...hs) - Math.min(...hs) <= 1, `行の高さ ${hs}`);
    // 列の板で外す / 出す (保存しない)
    await p.click('#colbtn');
    await p.click('#collist li[data-id="img"] input');
    assert.ok(!(await shownHeads(p)).includes('img'));
    assert.equal(await p.isHidden(cell('s001')), true);
    assert.equal(await p.textContent('#colcnt'), '16/23');   // 画像の列 (出す)・🆕 0084 メーカー・🆕 0087 大きさ・重さ・🆕 0088 ブランド・段階の 3 列 (出さない)
    await p.click('#collist li[data-id="img"] input');
    assert.deepEqual((await shownHeads(p)).slice(0, 2), ['code', 'img']);
    assert.match(await p.textContent('#collist li[data-id="img"]'), /画像\s*新/);
    assert.match(await p.textContent('#collist li[data-id="img"]'), /並べられない/);
    await p.keyboard.press('Escape');
    // 390: 横にはみ出さない (表は表の箱の中で送る)・画像の大きさはそのまま
    await p.setViewportSize({ width: 390, height: 844 });
    const [sw, cw] = await p.evaluate(() => [document.documentElement.scrollWidth, document.documentElement.clientWidth]);
    assert.ok(sw <= cw + 1, `390 で横にはみ出さない ${sw} > ${cw}`);
    assert.deepEqual(await p.$eval(cell('s001') + ' .thumb', (t) => [t.getBoundingClientRect().width, t.getBoundingClientRect().height]), [52, 52]);
    if (LIST_SHOTS) await p.screenshot({ path: `${LIST_SHOTS}/390_images.png` });
    await p.setViewportSize({ width: 1440, height: 900 });
    if (LIST_SHOTS) await p.screenshot({ path: `${LIST_SHOTS}/1440_images.png` });
    // 画像を押しても商品が開く (行のどこを押しても開くのと同じ)
    await Promise.all([p.waitForURL(/\/sku\/s001$/), p.click(cell('s001') + ' img')]);
  } finally {
    open();
    await IM.queueEnsureImages([], '');
    LI.__setListImageDeps(null);
    pdb.prepare("DELETE FROM pk_product_images WHERE ne_code IN ('s001', 'z002', 's003')").run();
  }
});

// 拡大 125% / 150% = 画面の CSS の幅が 1/1.25・1/1.5 になる。MASTER_EDIT_UI_SHOTS=フォルダ を付けると、そのフォルダに写しを残す (目で見る用)
const SHOTS = process.env.MASTER_EDIT_UI_SHOTS || '';
for (const [label, vp, scale] of [['1280×720 100%', { width: 1280, height: 720 }, 1], ['1280×720 125%', { width: 1024, height: 576 }, 1.25], ['1280×720 150%', { width: 853, height: 480 }, 1.5], ['1024×768', { width: 1024, height: 768 }, 1]]) {
  await ta(`[7] ${label}: ページが横にはみ出さない (一覧・単品・セットの 3 つの表)`, async (p) => {
    for (const [url, act] of [['/', null], ['/sku/s001', null], ['/sku/set001', 'cmp'], ['/sku/set001', 'edit'], ['/sku/set001', 'now']]) {
      await p.goto(B + url);
      if (act) await p.click(`#set-mode button[data-v="${act}"]`).catch(() => {});
      const [sw, cw] = await p.evaluate(() => [document.documentElement.scrollWidth, document.documentElement.clientWidth]);
      assert.ok(sw <= cw + 1, `${url} ${act || ''}: 横幅 ${sw} > ${cw}`);
      // 板 (左の列の箱) の中身が板からはみ出さない = 表は囲い (.scrollx) の中で横に送る (右の保存の箱に重ならない)
      const spill = await p.evaluate(() => [...document.querySelectorAll('.page .panel')].filter((x) => x.offsetParent !== null && x.scrollWidth > x.clientWidth + 1).map((x) => (x.querySelector('h2') || x).textContent.trim().slice(0, 20)));
      assert.deepEqual(spill, [], `${url} ${act || ''}: 板からはみ出している`);
      // 試験のデータは短くて収まってしまう = 長い名前・拡大で広がったときの逃げ道 (横に送る囲い) があることも見る
      const bare = await p.evaluate(() => [...document.querySelectorAll('table.comp')].filter((t) => { const w = t.parentElement; return !w || !w.classList.contains('scrollx') || getComputedStyle(w).overflowX !== 'auto'; }).length);
      assert.equal(bare, 0, `${url} ${act || ''}: セットの表が横に送る囲いに入っていない`);
      if (SHOTS) await p.screenshot({ path: `${SHOTS}/${label.replace(/[×% ]/g, '_')}_${url.replace(/\W+/g, '_')}${act || ''}.png`, fullPage: true });
    }
  }, vp, scale);
}

// MASTER_EDIT_UI_AMAZON_NEW_SHOTS=フォルダ を付けると、入口の写し (もしかして・もうある) を残す (目で見る用)
const AMZ_NEW_SHOTS = process.env.MASTER_EDIT_UI_AMAZON_NEW_SHOTS || '';
await ta('[55] 新しい seller SKU を自分で作る (10/11): 一覧の上の欄に打つ = その場で確かめる (出品だけある・もしかして・もうある・形の誤り)・もしかしてを見た後の Enter は作る画面へ直行・まだ売れていない SKU を作って保存・もうある SKU は「開く」', async (p) => {
  const out = () => p.evaluate(() => document.getElementById('new-sku-out').textContent.replace(/\s+/g, ' ').trim());
  const waitOut = (re) => p.waitForFunction((s) => new RegExp(s).test(document.getElementById('new-sku-out').textContent), re.source);
  await p.goto(B + '/amazon/');
  assert.match(await p.textContent('#h-create'), /新しい seller SKU の対応を作る/);
  // 夜間の取り込みの出品 (対応なし) = 作れる
  await p.fill('#new-sku', 'PR-K001');
  await waitOut(/Amazon の出品はあります/);
  // 打ち間違い = もしかして (1 文字違いの出品)・止めない
  await p.fill('#new-sku', 'pr-k001x');
  await waitOut(/もしかして/);
  assert.match(await out(), /pr-k001x は、まだありません[\s\S]*もしかして[\s\S]*pr-k001[\s\S]*1 文字違い/);
  assert.equal(await p.locator('#new-sku-out a.linkchip[href$="/amazon/sku?sku=pr-k001"]').count(), 1, 'もしかしての SKU を開ける');
  if (AMZ_NEW_SHOTS) await p.screenshot({ path: AMZ_NEW_SHOTS + '/入口_もしかして.png' });
  await Promise.all([p.waitForNavigation(), p.press('#new-sku', 'Enter')]);
  assert.match(p.url(), /\/amazon\/sku\?sku=pr-k001x$/, '見た後は /amazon/new で止めずに作る画面へ');
  // 作る画面 (出品も無い) で名前と構成を入れて保存
  assert.match(await p.textContent('.idrow'), /対応なし/);
  assert.match(await p.textContent('#h-listing ~ .hint, .panel.soft .hint'), /まだ売れていない/);
  await p.fill('#name', '自分で作った 2 個組');
  await p.click('#comp-add');
  const row = p.locator('#comp-rows tr.comp-row').last();
  await row.locator('.c-code').fill('k001'); await row.locator('.c-code').press('Tab');
  await row.locator('.c-qty').fill('2'); await row.locator('.c-qty').press('Tab');
  await p.waitForFunction(() => !document.getElementById('save').disabled);
  await p.keyboard.press('Control+s');
  await p.waitForSelector('#saved-note');
  assert.match(await p.textContent('#saved-note'), /新しい対応を作りました/);
  assert.deepEqual(await mapOf('pr-k001x'), { name: '自分で作った 2 個組', state: 'active' });
  assert.equal((await pg.query("select catalog_item_id from core.listings where listing_code = 'pr-k001x'")).rows[0].catalog_item_id, null, 'ASIN の無い出品を作った');
  // もうある = 「開く」に変わる・Enter でその画面へ
  await p.goto(B + '/amazon/');
  await p.fill('#new-sku', ' PR-K001X ');
  await waitOut(/もうあります/);
  assert.match(await out(), /もうあります[\s\S]*pr-k001x[\s\S]*有効/);
  assert.match(await p.textContent('#new-sku-go'), /開く/);
  if (AMZ_NEW_SHOTS) await p.screenshot({ path: AMZ_NEW_SHOTS + '/入口_もうある.png' });
  await Promise.all([p.waitForNavigation(), p.press('#new-sku', 'Enter')]);
  assert.match(p.url(), /\/amazon\/sku\?sku=pr-k001x$/);
  // 形の誤り = その場で赤・押すと /amazon/new が 400 で同じ理由
  await p.goto(B + '/amazon/');
  await p.fill('#new-sku', 'a'.repeat(256));
  await waitOut(/255 文字を超える/);
  assert.equal(await p.locator('#new-sku-out .fmsg.err').count(), 1);
  // 出品の無い SKU を全角で先に作る = その場で止める・半角にそろえた形で確かめ直すボタン (#1685 Codex R1 High)
  await p.fill('#new-sku', 'ｐｒ－ｋ００１ｚ');
  await waitOut(/この形では先に作れません/);
  assert.equal(await p.locator('#new-sku-out .fmsg.err').count(), 1);
  await p.click('#use-canonical');
  assert.equal(await p.inputValue('#new-sku'), 'pr-k001z');
  await waitOut(/もしかして/);
  // 空の Enter は送らない
  await p.fill('#new-sku', '');
  await p.press('#new-sku', 'Enter');
  await p.waitForTimeout(150);
  assert.match(p.url(), /\/amazon\/$/);
  // JS より先に送った (または直接開いた) = /amazon/new が「もしかして」を出す → 同じ値のまま「作り始める」= 作る画面へ
  await p.goto(B + '/amazon/new?sku=pr-k001y');
  assert.match(await p.textContent('#h-confirm'), /打ち間違いがないか/);
  assert.match(await p.textContent('#new-result'), /もしかして[\s\S]*pr-k001x/);
  await Promise.all([p.waitForNavigation(), p.click('#new-sku-go')]);
  assert.match(p.url(), /\/amazon\/sku\?sku=pr-k001y$/);
  assert.equal(await mapOf('pr-k001y'), undefined, '開いただけ = 作っていない');
});

// ── 構成の行の「NE の商品」の欄で探す (10/12 中原さん「既存の商品コードを検索して選んで登録できるように」) ──
const setRegUi = async (code, state) => {
  await pg.query(`select set_config('ops.registration_protocol', '1', false)`);
  try { await pg.query('update ops.master_registrations set state = $2 where sku_id = (select sku_id from core.skus where code = $1)', [code, state]); }
  finally { await pg.query(`select set_config('ops.registration_protocol', '', false)`); }
};
const regUi = async (code) => (await pg.query('select r.state from ops.master_registrations r join core.skus s on s.sku_id = r.sku_id where s.code = $1', [code])).rows[0].state;
const compsUi = async (sku) => (await pg.query(`select k.code, c.qty from core.listing_components c join core.skus k on k.sku_id = c.sku_id
  where c.listing_id = (select listing_id from core.listings where mall = 'amazon' and listing_norm = core.norm_code($1)) order by c.sort_order`, [sku])).rows.map((r) => [r.code, r.qty]);
const sugOpen = (p) => p.waitForFunction(() => { const s = document.getElementById('comp-suggest'); return s && !s.hidden && !s.classList.contains('stale') && s.querySelector('.co, .cs-empty:not(:first-child), .cs-h'); });
/** 候補の問い合わせを遅らせる (打ち足した直後 = 新しい候補がまだ来ない間を確実に作る) */
const slowSearch = (p) => p.route('**/api/amazon/components**', async (r) => { await new Promise((res) => setTimeout(res, 1500)); await r.continue().catch(() => {}); });
const fastSearch = (p) => p.unroute('**/api/amazon/components**');
const sugCodes = (p) => p.evaluate(() => [...document.querySelectorAll('#comp-suggest .co')].map((o) => (o.classList.contains('off') ? '×' : '') + o.querySelector('.cc').textContent));
const selCode = (p) => p.evaluate(() => { const o = document.querySelector('#comp-suggest .co.sel'); return o ? o.querySelector('.cc').textContent : null; });

await ta('[56] Amazon SKU の構成で探す (10/12): 名前の言葉 (かな・半角カナも) で候補・使えない商品は灰色で理由 (選べない・↓ で飛ばす)・↑↓ と Enter で選ぶ → コードが入って名前と登録の状態・ほかの行にある商品は選べない・マウスで選ぶ・Esc で閉じる・保存', async (p) => {
  const keep = await regUi('k003');
  await setRegUi('k003', 'cancelled');   // 使えない見本 = やめた (0073 から 下書き・NE 登録待ち・要確認は使える。やめた商品はコードをぴったり打ったときだけ出る)
  try {
    await p.goto(B + '/amazon/sku?sku=pr-k006');
    assert.equal(await p.inputValue('#comp-rows tr.comp-row .c-code'), 'k006');
    await p.click('#comp-add');
    const row2 = p.locator('#comp-rows tr.comp-row').nth(1);
    const in2 = row2.locator('.c-code');
    assert.equal(await active(p), '2 行目の NE コード', '足した行の欄へ');
    assert.equal(await in2.getAttribute('role'), 'combobox', '足した行の欄も探せる');
    // 1 文字では探さない
    await in2.fill('は');
    await p.waitForTimeout(400);
    assert.equal(await p.isHidden('#comp-suggest'), true, '1 文字では出さない');
    await in2.fill('はちみつ');
    await sugOpen(p);
    assert.equal(await in2.getAttribute('aria-expanded'), 'true');
    assert.deepEqual(await sugCodes(p), ['k001', 'k002'], 'はちみつ = ハチミツ (かなの同一視)・やめた k003 (ﾊﾁﾐﾂ) は名前では出さない');
    assert.match(await p.locator('#comp-suggest .co').first().textContent(), /単品[\s\S]*登録済み[\s\S]*在庫 15[\s\S]*国産 はちみつ 500g/, '単品・登録の状態・在庫 (ロジザード)・名前');
    assert.equal(await p.locator('#comp-suggest .co').first().locator('mark').count(), 1, '当たった言葉に印');
    if (process.env.MASTER_EDIT_UI_SEARCH_SHOTS) { await in2.press('ArrowDown'); await p.screenshot({ path: `${process.env.MASTER_EDIT_UI_SEARCH_SHOTS}/1440_候補.png` }); await in2.press('ArrowUp'); }
    assert.equal(await selCode(p), null, 'ぴったりでなければ、はじめは選んでいない (Enter は今までどおり)');
    await in2.press('ArrowDown'); assert.equal(await selCode(p), 'k001');
    await in2.press('ArrowDown'); assert.equal(await selCode(p), 'k002');
    await in2.press('ArrowDown'); assert.equal(await selCode(p), 'k002', '最後で止まる');
    assert.equal(await in2.getAttribute('aria-activedescendant'), 'cs-opt-1');
    await in2.press('ArrowUp'); assert.equal(await selCode(p), 'k001');
    await in2.press('Enter');
    assert.equal(await p.isHidden('#comp-suggest'), true, '選ぶと閉じる');
    assert.equal(await in2.inputValue(), 'k001');
    assert.equal(await active(p), '2 行目 (k001) の数', '数の欄へ');
    await p.waitForFunction(() => /国産 はちみつ/.test(document.querySelectorAll('#comp-rows .c-name')[1].textContent));
    await p.waitForFunction(() => /登録済み/.test(document.querySelectorAll('#comp-rows .c-reg')[1].textContent));
    assert.equal(await dirty(p), 2, '構成と、空だった名前 (社内) に選んだ商品の名前 ([58])');
    assert.equal(await p.inputValue('#name'), (await row('k001')).name);
    assert.match(await p.textContent('#save-diff'), /k006×1, k001×1/);
    // 3 行目: コードの一部。上の行にある商品は選べない (同じ品は 1 行に)・使えない商品を押しても入らない・マウスで選ぶ
    await p.click('#comp-add');
    const in3 = p.locator('#comp-rows tr.comp-row').nth(2).locator('.c-code');
    await in3.fill('k00');
    await sugOpen(p);
    assert.deepEqual(await sugCodes(p), ['×k001', 'k002', 'k004', 'k005', '×k006'], '前方一致・ほかの行にある k001 と k006 は灰色・やめた k003 は前方一致では出さない');
    const off = p.locator('#comp-suggest .co.off').first();
    assert.equal(await off.getAttribute('aria-disabled'), 'true');
    assert.match(await p.locator('#comp-suggest .co.off').first().textContent(), /この構成の 2 行目にもうあります/);
    await p.locator('#comp-suggest .co.off').last().click({ force: true });   // aria-disabled = Playwright は押せない物と見る。人は押せる (押しても入らないことを確かめる)
    assert.equal(await in3.inputValue(), 'k00', '使えない商品は入らない');
    assert.match(await p.textContent('#toast-t'), /使えません: この構成の 1 行目にもうあります/);
    assert.equal(await p.isVisible('#comp-suggest'), true, '押しても閉じない (欄から出ない)');
    await p.locator('#comp-suggest .co:not(.off)').filter({ hasText: 'k005' }).click();
    assert.equal(await in3.inputValue(), 'k005');
    await p.waitForFunction(() => /abc 小袋/.test(document.querySelectorAll('#comp-rows .c-name')[2].textContent));
    // やめた商品 = コードをぴったり打ったときだけ灰色で出る (理由つき・選べない)
    await in3.fill('k003');
    await sugOpen(p);
    assert.deepEqual(await sugCodes(p), ['×k003']);
    assert.match(await p.locator('#comp-suggest .co.off').textContent(), /使えません: 登録をやめた商品/);
    await in3.press('ArrowDown');
    assert.equal(await selCode(p), null, '↓ でも灰色には止まらない');
    // Esc で閉じる (欄の値は変えない・画面の Esc の動きにしない)・欄から出ると閉じる
    await in3.fill('k0');
    await sugOpen(p);
    await in3.press('Escape');
    assert.equal(await p.isHidden('#comp-suggest'), true);
    assert.equal(await in3.inputValue(), 'k0');
    await in3.fill('k005');
    await sugOpen(p);
    assert.equal(await selCode(p), 'k005', 'ぴったりで使える = はじめから選んでいる');
    await in3.press('Tab');
    assert.equal(await p.isHidden('#comp-suggest'), true, '欄から出ると閉じる');
    // 打ち足して すぐ Enter / すぐ押す = 前の候補 (古い) は入らない (#1689 Codex R1 M1)
    await in3.fill('k005');
    await sugOpen(p);
    assert.equal(await selCode(p), 'k005');
    await slowSearch(p);
    try {
      await in3.press('x');
      assert.equal(await selCode(p), null, '打ち足したら選んだ印が消える');
      assert.equal(await p.evaluate(() => document.getElementById('comp-suggest').classList.contains('stale')), true, '古い候補 = 薄く');
      await in3.press('Enter');
      assert.equal(await in3.inputValue(), 'k005x', 'Enter で古い k005 が入らない');
      await in3.fill('k00');
      await fastSearch(p);
      await sugOpen(p);
      await slowSearch(p);
      await in3.press('2');
      await p.locator('#comp-suggest .co:not(.off)').filter({ hasText: 'k004' }).click();
      assert.equal(await in3.inputValue(), 'k002', '押しても古い候補 (k004) は入らない');
    } finally { await fastSearch(p); }
    await in3.fill('k005');
    await sugOpen(p);
    await in3.press('Tab');
    // 当たらない
    await in2.fill('zzzz-none');
    await p.waitForSelector('#comp-suggest .cs-empty:has-text("当たる NE の商品はありません")');
    await in2.fill('k001');
    await in2.press('Tab');
    assert.equal(await p.isHidden('#comp-suggest'), true);
    await p.fill('#name', '探して選んだ構成');
    await p.keyboard.press('Control+s');
    await p.waitForSelector('#saved-note');
    assert.match(await p.textContent('#saved-note'), /保存しました/);
    assert.deepEqual(await compsUi('pr-k006'), [['k006', 1], ['k001', 1], ['k005', 1]]);
  } finally { await setRegUi('k003', keep); }
});

await ta('[57] Amazon SKU の構成で探す (スマホ 390・タッチ): 候補の箱は画面の中・横にはみ出さない・候補と欄は 44px 以上・タッチで選ぶ / JAN で探す', async (p, ctx) => {
  await ctx.close().catch(() => {});
  const c2 = await browser.newContext({ viewport: { width: 390, height: 844 }, hasTouch: true, locale: 'ja-JP' });
  const m = await c2.newPage();
  const errs = []; m.on('pageerror', (e) => errs.push(e.message));
  try {
    await pg.query(`insert into core.external_ids (company_id, entity_type, entity_id, system, id_kind, external_value, resolution, resolved_by_type)
      values (1, 'product', (select product_id from core.skus where code = 'k004'), 'jan', 'jan', '4900000000017', 'manual', 'human')`);
    await m.goto(B + '/amazon/sku?sku=pr-k1k2');
    const spill = () => m.evaluate(() => [document.documentElement.scrollWidth, document.documentElement.clientWidth]);
    const [sw0, cw0] = await spill();
    if (process.env.MASTER_EDIT_UI_SEARCH_SHOTS) console.log(`      (390 幅: 開く前の横幅 ${sw0} / ${cw0})`);
    await m.tap('#comp-add');
    const inp = m.locator('#comp-rows tr.comp-row').nth(2).locator('.c-code');
    const ib = await inp.boundingBox();
    assert.ok(ib.height >= 43.5, '欄の高さ ' + ib.height);
    assert.ok((await m.locator('#comp-add').boundingBox()).height >= 43.5, '「NE の商品を足す」の高さ');
    await inp.fill('ABC');
    await sugOpen(m);
    assert.deepEqual(await sugCodes(m), ['k004', 'k005'], 'ＡＢＣ (全角) と abc に当たる');
    if (process.env.MASTER_EDIT_UI_SEARCH_SHOTS) await m.screenshot({ path: `${process.env.MASTER_EDIT_UI_SEARCH_SHOTS}/390_候補.png` });
    const box = await m.evaluate(() => { const b = document.getElementById('comp-suggest').getBoundingClientRect(); return { l: b.left, r: b.right, vw: innerWidth }; });
    assert.ok(box.l >= 0 && box.r <= box.vw + 0.5, '候補の箱は画面の中: ' + JSON.stringify(box));
    const [sw, cw] = await spill();
    assert.ok(sw <= Math.max(cw, sw0) + 1, `開いても横にはみ出さない ${sw} > ${cw} (前 ${sw0}/${cw0})`);
    for (const h of await m.locator('#comp-suggest .co').evaluateAll((xs) => xs.map((x) => x.getBoundingClientRect().height))) assert.ok(h >= 44, '候補の高さ ' + h);
    // 打ち足して すぐタップ = 古い候補は入らない (#1689 Codex R1 M1)
    await slowSearch(m);
    try {
      await inp.press('D');
      await m.locator('#comp-suggest .co').filter({ hasText: 'k005' }).tap();
      assert.equal(await inp.inputValue(), 'ABCD', 'タップでも古い候補は入らない');
    } finally { await fastSearch(m); }
    await inp.fill('ABC');
    await sugOpen(m);
    await m.locator('#comp-suggest .co').filter({ hasText: 'k005' }).tap();
    assert.equal(await inp.inputValue(), 'k005', 'タッチで選べる');
    await m.waitForFunction(() => /abc 小袋/.test(document.querySelectorAll('#comp-rows .c-name')[2].textContent));
    // JAN
    await inp.fill('4900000000017');
    await sugOpen(m);
    assert.deepEqual(await sugCodes(m), ['k004'], 'JAN で探せる');
    await m.locator('#comp-suggest .co').first().tap();
    assert.equal(await inp.inputValue(), 'k004');
    assert.deepEqual(errs, [], '画面の JS の例外');
  } finally { await c2.close(); }
});

await ta('[59] Amazon SKU の構成に まだ NE に無い新商品・要確認 (0073・10/12): NE 登録待ち・要確認の商品は候補で選べる・「NE 未登録 (登録待ち)」「要確認 (NE で直接作った商品)」の印・下の説明の文', async (p) => {
  const keep = await regUi('k004');
  await setRegUi('k004', 'ne_pending');
  try {
    await p.goto(B + '/amazon/sku?sku=pr-k006');
    assert.match(await p.textContent('.f[data-row="components"]'), /まだ NE に無い新商品 \(下書き・NE 登録待ち\)・NE で直接作った商品 \(要確認\) も使えます/);
    await p.click('#comp-add');
    const inp = p.locator('#comp-rows tr.comp-row').last().locator('.c-code');
    await inp.fill('k004');
    await sugOpen(p);
    const o = p.locator('#comp-suggest .co').first();
    assert.equal(await o.getAttribute('aria-disabled'), null, '灰色でない');
    assert.match(await o.textContent(), /NE登録待ち[\s\S]*NE 未登録 \(登録待ち\)/);
    await inp.press('Enter');
    assert.equal(await inp.inputValue(), 'k004', '選べる');
    // 要確認 (NE で直接作った商品) も選べる・印
    await setRegUi('k002', 'quarantined');
    await p.click('#comp-add');
    const inp2 = p.locator('#comp-rows tr.comp-row').last().locator('.c-code');
    await inp2.fill('k002');
    await sugOpen(p);
    const o2 = p.locator('#comp-suggest .co').first();
    assert.equal(await o2.getAttribute('aria-disabled'), null, '要確認は灰色でない');
    assert.match(await o2.textContent(), /要確認[\s\S]*要確認 \(NE で直接作った商品\)/);
    await inp2.press('Enter');
    assert.equal(await inp2.inputValue(), 'k002', '要確認も選べる');
  } finally { await setRegUi('k004', keep); await setRegUi('k002', 'available'); }
});

await ta('[70] Amazon SKU の構成で 日本語の名前で探す (10/16 中原さん「名前を日本語で打つと何も出ない」): 本物の IME (CDP)・変換中から候補・確定の Enter で箱を閉じない / 数の欄へ移らない (Windows の Chrome・Edge で compositionend の後に keyCode 13 で来る並び・keydown 229 が先・Safari = 後に 229)・空白の後の言葉・↓ Enter で選ぶ・確定の後の Enter は今までどおり数の欄へ', async (p) => {
  await p.goto(B + '/amazon/sku?sku=pr-ime-a');
  const inp = p.locator('#comp-rows tr.comp-row').first().locator('.c-code');
  assert.equal(await active(p), '1 行目の NE コード');
  const cdp = await p.context().newCDPSession(p);
  const key = (type, k, vk) => cdp.send('Input.dispatchKeyEvent', { type, key: k, code: k === 'Enter' ? 'Enter' : 'KeyA', windowsVirtualKeyCode: vk, nativeVirtualKeyCode: vk });
  // 変換中のキー = keyCode 229 (Process)。字は 1 つずつ増える
  const compose = async (steps) => { for (const s of steps) { await key('rawKeyDown', 'Process', 229); await cdp.send('Input.imeSetComposition', { text: s, selectionStart: s.length, selectionEnd: s.length }); await key('keyUp', 'a', 65); } };
  // 確定の Enter: win = keydown 229 → 確定 → keyup / late = 確定 → keydown 13 (isComposing なし) → keyup / safari = 確定 → keydown 229 → keyup
  const commit = async (text, order) => {
    if (order === 'win') await key('rawKeyDown', 'Process', 229);
    await cdp.send('Input.insertText', { text });
    if (order === 'late') await key('rawKeyDown', 'Enter', 13);
    if (order === 'safari') await key('rawKeyDown', 'Enter', 229);
    await key('keyUp', 'Enter', 13);
  };
  const hasHoney = async (why) => { const c = await sugCodes(p); assert.ok(c.includes('k001') && c.includes('k002'), why + ': ' + JSON.stringify(c)); };
  for (const [order, text] of [['late', 'ハチミツ'], ['win', 'ハチミツ'], ['safari', 'はちみつ']]) {
    await inp.fill('');
    await compose(['は', 'はち', 'はちみ', 'はちみつ']);
    await sugOpen(p);
    await hasHoney(order + ': 変換中 (はちみつ) から候補');
    if (text !== 'はちみつ') await compose([text]);
    await commit(text, order);
    await p.waitForTimeout(100);
    assert.equal(await active(p), '1 行目の NE コード', order + ': 確定の Enter で数の欄へ移らない');
    assert.equal(await inp.inputValue(), text);
    await sugOpen(p);
    await hasHoney(order + ': 確定の後も候補 (' + text + ')');
    assert.equal(await p.textContent('#comp-rows .c-name'), '', order + ': 欄を出ない = 引き当て (見つかりません) をしない');
    await inp.press('Escape');
    assert.equal(await p.isHidden('#comp-suggest'), true, 'Esc で閉じる');
  }
  // 空白の後に言葉を足す → 絞れる・↓ Enter で選ぶ
  await inp.fill('');
  await compose(['は', 'はちみつ', 'ハチミツ']);
  await commit('ハチミツ', 'late');
  await p.keyboard.type(' 500g');
  await sugOpen(p);
  assert.deepEqual(await sugCodes(p), ['k001'], '空白で区切ると全部を含む');
  await inp.press('ArrowDown');
  await inp.press('Enter');
  assert.equal(await inp.inputValue(), 'k001');
  assert.equal(await active(p), '1 行目 (k001) の数', '選ぶと数の欄へ');
  await p.waitForFunction(() => /国産 はちみつ/.test(document.querySelector('#comp-rows .c-name').textContent));
  // 確定の後に選ばずに Enter = 今までどおり数の欄へ (閉じる)
  await inp.fill('');
  await compose(['み', 'みかん']);
  await commit('みかん', 'win');
  await sugOpen(p);
  await inp.press('Enter');
  assert.equal(await p.isHidden('#comp-suggest'), true);
  assert.match(await active(p), /の数$/, '確定の後の Enter は数の欄へ');
  // マウス・タッチで変換の候補を選んで確定 (キーなし = keyup が来ない) → しばらく後の Enter は人の Enter (食べない)
  await inp.fill('');
  await compose(['み', 'みかん']);
  await cdp.send('Input.insertText', { text: 'みかん' });
  await sugOpen(p);
  await p.waitForTimeout(1100);
  await inp.press('Enter');
  assert.match(await active(p), /の数$/, 'マウスで確定した後の Enter は数の欄へ');
});

await ta('[58] Amazon SKU の名前 (社内) が空なら NE の商品名を入れる (10/12): 候補から選ぶ = 入る (変更の印・保存すると変わること・一言)・次の行では上書きしない・人が直すと一言が消える・元に戻す / 空白だけも空 / コードをぴったり打つ = 入る / 使えない商品 (灰色・打っても) = 入らない / 名前のある対応 = 入らない', async (p) => {
  const keep = await regUi('k003');
  await setRegUi('k003', 'cancelled');   // 使えない見本 = やめた (0073 から NE 登録待ち・要確認は使える)
  try {
    await p.goto(B + '/amazon/sku?sku=pr-k1k2');
    assert.equal(await p.inputValue('#name'), '', '前提: 名前が空の新しい対応');
    assert.equal(await dirty(p), 0);
    const rows = p.locator('#comp-rows tr.comp-row');
    const pickRow = async (q, code) => {
      await p.click('#comp-add');
      const inp = rows.last().locator('.c-code');
      await inp.fill(q);
      await sugOpen(p);
      while ((await selCode(p)) !== code) await inp.press('ArrowDown');
      await inp.press('Enter');
      return inp;
    };
    // 候補から選ぶ = 入る
    await pickRow('みかん', 'k006');
    const mikan = (await row('k006')).name;
    assert.equal(await p.inputValue('#name'), mikan, '選んだ商品の名前');
    assert.match(await p.textContent('#name-filled'), /NE の商品名 \(k006\) を入れました/);
    assert.equal(await dirty(p), 2, '名前と構成の 2 件');
    assert.ok((await p.textContent('#save-diff')).includes(mikan), '保存すると変わることに名前');
    assert.equal(await p.evaluate(() => document.querySelector('.f[data-row="name"]').classList.contains('dirty')), true, '変更の印');
    // 次の行では上書きしない
    await pickRow('abc', 'k005');
    assert.equal(await p.inputValue('#name'), mikan, '書いてある名前は変えない');
    // 人が直す = そのまま・一言は消える
    await p.fill('#name', 'みかん と abc');
    assert.equal(await p.locator('#name-filled').count(), 0, '人が直したら一言を消す');
    await pickRow('ABC 保存', 'k004');
    assert.equal(await p.inputValue('#name'), 'みかん と abc', '人が直した名前はそのまま');
    // 元に戻す = 名前も空に・一言も無い
    await p.click('#revert');
    assert.equal(await p.inputValue('#name'), '');
    assert.equal(await p.locator('#name-filled').count(), 0);
    assert.equal(await dirty(p), 0);
    // 空白だけ = 空
    await p.fill('#name', '  \u3000 ');
    await pickRow('k005', 'k005');
    assert.equal(await p.inputValue('#name'), (await row('k005')).name, '空白だけは空と同じ');
    // 候補から選んだ後に、名前を空にしてコードを手で変え、同じコードをぴったり打ち直す = 入る (選んだ印は手で変えたら捨てる・#1692 Codex R1 Medium)
    const again = rows.last().locator('.c-code');
    await p.fill('#name', '');
    await again.fill('zzzz-none');
    await again.press('Tab');
    await p.waitForFunction(() => /見つかりません|ありません/.test([...document.querySelectorAll('#comp-rows .c-name')].pop().textContent));
    assert.equal(await p.inputValue('#name'), '', '無いコードでは入らない');
    await again.fill('k005');
    await again.press('Tab');
    await p.waitForFunction(() => document.getElementById('name').value !== '', null, { timeout: 5000 });
    assert.equal(await p.inputValue('#name'), (await row('k005')).name, '同じコードを打ち直しても入る');
    await p.click('#revert');
    // 使えない商品 = 灰色を押しても・コードをぴったり打っても入らない
    await p.click('#comp-add');
    let inp = rows.last().locator('.c-code');
    await inp.fill('k003');
    await sugOpen(p);
    assert.deepEqual(await sugCodes(p), ['×k003']);
    await p.locator('#comp-suggest .co.off').click({ force: true });
    await inp.press('Tab');
    await p.waitForFunction(() => /ﾊﾁﾐﾂ/.test([...document.querySelectorAll('#comp-rows .c-name')].pop().textContent));
    await p.waitForTimeout(600);   // 名前を入れる確かめ (候補の API) が終わるのを待つ
    assert.equal(await p.inputValue('#name'), '', '使えない商品 (やめた) では入れない');
    assert.equal(await p.locator('#name-filled').count(), 0);
    // コードをぴったり打つ (候補を選ばない) = 使える商品なら入る
    await inp.fill('K004');
    await inp.press('Tab');
    await p.waitForFunction(() => document.getElementById('name').value !== '');
    const abc = (await row('k004')).name;
    assert.equal(await p.inputValue('#name'), abc, 'ぴったり打った商品の名前');
    assert.match(await p.textContent('#name-filled'), /\(k004\)/);
    assert.equal(await dirty(p), 2, '後から (引き当ての後に) 入れた名前も未保存に数える');
    assert.ok((await p.textContent('#save-diff')).includes(abc), '保存すると変わることにも出る');
    // 保存できる = 名前も入る
    await p.keyboard.press('Control+s');
    await p.waitForSelector('#saved-note');
    assert.deepEqual(await mapOf('pr-k1k2'), { name: abc, state: 'active' });
    // 名前のある対応 = 入らない
    await pickRow('みかん', 'k006');
    assert.equal(await p.inputValue('#name'), abc, '名前のある対応は変えない');
    assert.equal(await p.locator('#name-filled').count(), 0);
  } finally { await setRegUi('k003', keep); }
});

// ─── 🆕 2026-10-13 中原さん「入力なし」: 必須でない項目は 値 か 入力なし。全部そろうまで下書きの保存は押せない ───
const regReason = async (code) => ((await pg.query('select e.reason from ops.master_registration_events e join core.skus s on s.sku_id = e.sku_id where s.code = $1 order by e.event_id limit 1', [code])).rows[0] || {}).reason;
const fillSingleReq = async (p, code) => {
  await p.fill('#code', code);
  await p.waitForSelector('#code-msg.ok');
  await p.fill('#f-name', '入力なしの試験 ' + code);
  await p.fill('#f-standard_price', '1200');
  await p.click('#f-tax_rate button[data-v="0.1"]');
  await p.selectOption('#f-primary_supplier', '0001');
  await fillReq(p);
};
const liOf = (p, t) => p.locator('#checklist li', { hasText: t }).first();

await ta('[64] 入力なし (単品・1440): 必須がそろっても押せない・入力なしを押すと値を消して「なし」の印・値を入れるとチェックが外れる・グループの「なし」= 入力なし・Yahoo! はまとめて・全部そろうと押せる → 登録の記録 (理由) に入力なしの一覧', async (p) => {
  await p.goto(B + '/new?kind=single');
  await fillSingleReq(p, 'ui-none-1');
  assert.equal(await p.isDisabled('#save'), true, '必須だけでは押せない');
  assert.match(await p.textContent('#save'), /あと 19 つ: 原価/);
  assert.match(await p.textContent('#checklist'), /④ そのほかの項目/);
  // 原価: 値を入れてから「入力なし」= 値を消す・印
  await p.fill('#cost-jpy', '300');
  assert.equal(await liOf(p, '原価').getAttribute('class'), 'done opt');
  await p.click('[data-row="cost"] .nonechk');
  assert.equal(await p.inputValue('#cost-jpy'), '', '入力なし = 値を消す');
  assert.equal(await p.isChecked('[data-none="cost"]'), true);
  assert.match(await p.getAttribute('[data-row="cost"]', 'class'), /isnone/);
  assert.match(await p.getAttribute('[data-row="cost"] .nonechk', 'class'), /\bon\b/);
  assert.match(await liOf(p, '原価').getAttribute('class'), /\bnone\b/);
  assert.match(await liOf(p, '原価').textContent(), /なし[\s\S]*入力なし/);
  // 値を入れると入力なしは自動で外れる (両方は持たない)
  await p.fill('#cost-jpy', '250');
  assert.equal(await p.isChecked('[data-none="cost"]'), false, '値を入れたらチェックが外れる');
  assert.ok(!/isnone/.test(await p.getAttribute('[data-row="cost"]', 'class')));
  await p.fill('#cost-jpy', '');
  assert.match(await liOf(p, '原価').getAttribute('class'), /todo/, '値を消しただけ = まだ (入力なしにはならない)');
  // ASIN / Amazon URL: どちらかに値 = 入力なしが外れる
  await p.click('[data-row="card.amazon_url"] .nonechk');
  assert.equal(await p.isChecked('[data-none="card.amazon"]'), true);
  await p.fill('#asin', 'B0UINONE01');
  assert.equal(await p.isChecked('[data-none="card.amazon"]'), false, 'ASIN を入れたら Amazon の入力なしが外れる');
  // 切り替え (入荷日の管理): 入力なし → 「あり」を押すと外れる・入力なしを押すと選んだのを消す
  await p.click('[data-row="inbound_date_managed"] .nonechk');
  await p.click('#f-inbound button[data-v="1"]');
  assert.equal(await p.isChecked('[data-none="inbound_date_managed"]'), false);
  await p.click('[data-row="inbound_date_managed"] .nonechk');
  assert.equal(await p.getAttribute('#f-inbound', 'data-value'), '');
  // 発注条件グループ = 「なし」が入力なし (右の一覧も「なし」)
  await p.click('#po-cond-mode button[data-v="none"]');
  assert.match(await liOf(p, '発注条件グループ').getAttribute('class'), /\bnone\b/);
  // 🆕 中原さん 10/13 (答え 2 = b): まとめて「入力なし」にするボタンは無い (Yahoo!・ケース・容量も 1 つずつ押す)
  assert.equal(await p.locator('[data-none-all], .nonebulk').count(), 0, 'まとめて入力なしにするボタンがある');
  assert.match(await p.textContent('#sec-yahoo'), /欄ごとに「入力なし」を押します/);
  await p.click('#sec-yahoo > summary');
  await p.fill('#y-price', '1500');
  for (const k of ['card.yahoo.price_sagawa', 'card.yahoo.delivery_label', 'card.yahoo.category_id', 'card.yahoo.path']) await p.click(`[data-row="${k}"] .nonechk`);
  assert.deepEqual(await p.evaluate(() => ['card.yahoo.price', 'card.yahoo.price_sagawa', 'card.yahoo.delivery_label', 'card.yahoo.category_id', 'card.yahoo.path'].map((k) => document.querySelector(`[data-none="${k}"]`).checked)), [false, true, true, true, true]);
  assert.equal(await p.inputValue('#y-price'), '1500', 'ほかの欄の入力なしで値は消えない');
  // 押す所は 44px 以上
  const small = await p.evaluate(() => [...document.querySelectorAll('.nonechk')].filter((x) => x.offsetParent !== null).map((x) => x.getBoundingClientRect()).filter((r) => r.height < 44 || r.width < 44).length);
  assert.equal(small, 0, '入力なしの押す所が 44px より小さい');
  // 残りを入力なしにする前は押せない → 全部そろうと押せる
  assert.equal(await p.isDisabled('#save'), true);
  await p.fill('#cost-jpy', '480');
  await noneRest(p);
  assert.match(await p.textContent('#remain'), /下書きを保存できます/);
  assert.equal(await p.locator('#checklist li.todo').count(), 0);
  // 🆕 0080 (CompanyDB構想 23 v4): ロジザードのバーコード = 必須 (値か「空欄」)。空欄の印を外す = 押せない・値を打つと空欄の印が外れる・形の一言
  await p.uncheck('#f-lzbc-blank');
  assert.equal(await p.isDisabled('#save'), true, 'ロジザードのバーコードが無いのに押せる');
  assert.match(await p.textContent('#checklist'), /ロジザードのバーコード/);
  await p.fill('#f-lzbc-in', '4901234567890');
  assert.match(await p.textContent('#f-lzbc-msg'), /チェック数字/);
  assert.equal(await p.isDisabled('#save'), true);
  await p.check('#f-lzbc-blank');
  assert.equal(await p.inputValue('#f-lzbc-in'), '', '空欄に印で値が残った');
  await p.fill('#f-lzbc-in', 'x00uin0001');
  assert.equal(await p.isChecked('#f-lzbc-blank'), false, '値を打ったのに空欄の印が残った');
  assert.match(await p.textContent('#f-lzbc-msg'), /FNSKU の形/);
  const bodies = [];
  p.on('request', (rq) => { if (/\/api\/new$/.test(rq.url())) bodies.push(JSON.parse(rq.postData())); });
  await Promise.all([p.waitForURL(/\/sku\/ui-none-1$/), p.click('#save')]);
  assert.deepEqual(bodies[0].logizard_barcode, { kind: 'value', value: 'X00UIN0001' });
  assert.deepEqual((await pg.query(`select r.rule, r.value, r.via from ops.sku_logizard_barcode_rules r join core.skus s on s.sku_id = r.sku_id where s.code = 'ui-none-1'`)).rows, [{ rule: 'fnsku', value: 'X00UIN0001', via: 'register' }]);
  // 登録の後の商品の画面に「ロジザードのバーコード」(いま = 打った値)
  assert.match(await p.innerText('#sec-lzbc'), /X00UIN0001/);
  // 送る印 = サーバーと同じ並び (lib/master-register-blanks.mjs の BLANK_KEYS)
  assert.deepEqual(bodies[0].blanks, ['maker', 'order_settings.order_lot', 'order_settings.condition', 'order_settings.material', 'order_settings.case_lot', 'order_settings.case_group', 'order_settings.capacity_per_unit',
    'shipping_code', 'inbound_date_managed', 'card.official_url', 'card.reference_urls', 'card.set_decision', 'card.yahoo.price_sagawa', 'card.yahoo.delivery_label', 'card.yahoo.category_id', 'card.yahoo.path']);
  // 発注の設定の入力なしは理由の後ろの一言 (要求のハッシュに入らない・#1706 Codex R2 M)
  assert.equal(await regReason('ui-none-1'), '新商品の登録 · 入力なし: メーカー・発送方法・入荷日の管理・公式ページURL・参考URL・セット商品・Yahoo!売価(佐川)・Yahoo!配送方法・Yahoo!カテゴリID・Yahoo!path · 発注の設定の入力なし: 発注ロット・発注条件グループ・原料グループ・ケースロット・ケースグループ・容量');
  assert.deepEqual(await skuRow('ui-none-1'), { name: '入力なしの試験 ui-none-1', price: 1200, tax: 0.1, shipping_code: null });
});

await ta('[65] 入力なし (スマホ 390): 横にはみ出さない・押す所は 44px・押すと入力なし・下の帯から保存まで', async (p) => {
  await p.goto(B + '/new?kind=single');
  await fillSingleReq(p, 'ui-none-390');
  // 単品の登録の画面の 390 幅のはみ出し (上の帯・円の欄) は 10/13 に直した ([67] が全体を見る)。
  //    ここでは「入力なし」の札と参考 URL の下の行がはみ出さない・小さくないことを見る (まとまりの画面は [📱390] が全体を見る)
  const out0 = await p.evaluate(() => [...document.querySelectorAll('.refsfoot')].filter((x) => x.offsetParent !== null && x.getBoundingClientRect().right > document.documentElement.clientWidth + 1).length);
  assert.equal(out0, 0, '参考 URL の下の行がはみ出す');
  const small = await p.evaluate(() => [...document.querySelectorAll('.nonechk')].filter((x) => x.offsetParent !== null).map((x) => x.getBoundingClientRect()).filter((r) => r.height < 44 || r.width < 44).length);
  assert.equal(small, 0, '入力なしの押す所が 44px より小さい');
  const out = await p.evaluate(() => [...document.querySelectorAll('.nonechk')].filter((x) => x.offsetParent !== null).filter((x) => { const r = x.getBoundingClientRect(); return r.right > document.documentElement.clientWidth + 1; }).length);
  assert.equal(out, 0, '入力なしが画面の外');
  await p.click('[data-row="shipping_code"] .nonechk');
  assert.equal(await p.isChecked('[data-none="shipping_code"]'), true);
  await noneRest(p);
  await p.click('#savebox-toggle');
  assert.match(await p.textContent('#remain'), /下書きを保存できます/);
  await Promise.all([p.waitForURL(/\/sku\/ui-none-390$/), p.click('#save')]);
  assert.match(await regReason('ui-none-390'), /入力なし: 原価・/);
}, { width: 390, height: 844 });

await ta('[66] 入力なし (セット): 例外原価 (例外のときの欄) も 値か入力なし・カードを作らない = カードの欄は数えない・記録に残る', async (p) => {
  await p.goto(B + '/new?kind=set');
  const rows = p.locator('#comp-rows tr.comp-row');
  await rows.nth(0).locator('.c-code').fill('s001'); await rows.nth(0).locator('.c-code').press('Tab');
  await p.waitForFunction(() => /単品 1/.test(document.querySelector('#comp-rows .c-name').textContent));
  await p.fill('#code', 'ui-none-set');
  await p.waitForSelector('#code-msg.ok');
  await p.fill('#f-name', '入力なしのセット');
  await p.fill('#f-standard_price', '2400');
  await fillReq(p, { set: true });
  await p.click('#card-create button[data-v="0"]');
  assert.match(await p.textContent('#save'), /あと 2 つ: 発送方法|あと 2 つ: 例外原価/, 'カードを作らない = 例外原価・発送方法の 2 つ');
  await p.click('#sec-comp details.more > summary');
  await p.click('[data-row="exception_cost"] .nonechk');
  await p.click('[data-row="shipping_code"] .nonechk');
  assert.match(await p.textContent('#remain'), /下書きを保存できます/);
  await Promise.all([p.waitForURL(/\/sku\/ui-none-set$/), p.click('#save')]);
  assert.equal(await regReason('ui-none-set'), '新商品の登録 · 入力なし: 例外原価・発送方法');
});

await ta('[61] Amazon SKU の新しい対応 (10/13): 開くと構成の 1 行目 (空のコード・数 1・代表) とコードの欄にフォーカス・未保存 0・構成 → 名前 (社内) の順 (Tab も)・「足す」は空の行があれば足さない・コード → 名前が入る・2 行目を足せる (空の行は数えない)・元に戻す・保存', async (p) => {
  await p.goto(B + '/amazon/sku?sku=pr-row1-a');
  const rows = p.locator('#comp-rows tr.comp-row');
  assert.equal(await rows.count(), 1, '開くと 1 行目がある');
  const r1 = rows.first();
  assert.deepEqual([await r1.locator('.c-code').inputValue(), await r1.locator('.c-qty').inputValue(), (await r1.locator('.no').textContent()).trim()], ['', '1', '1']);
  assert.match(await r1.textContent(), /代表/);
  assert.equal(await r1.locator('.c-code').getAttribute('aria-label'), '1 行目の NE コード');
  assert.equal(await active(p), '1 行目の NE コード', 'コードの欄にフォーカス (マウスの画面)');
  assert.equal(await dirty(p), 0, '開いただけでは未保存にしない');
  assert.equal(await p.isDisabled('#save'), true, '空の 1 行では保存できない');
  // 並び = 構成の表 → 名前 (社内)。Tab も「足す」の次が名前
  assert.equal(await p.evaluate(() => !!(document.getElementById('comp').compareDocumentPosition(document.getElementById('name')) & Node.DOCUMENT_POSITION_FOLLOWING)), true, '構成の後に名前');
  await p.focus('#comp-add');
  await p.keyboard.press('Tab');
  assert.equal(await active(p), 'name', '「足す」の次の Tab = 名前 (社内)');
  // 「足す」= 最後の行が空なら足さずにその欄へ
  await p.click('#comp-add');
  assert.equal(await rows.count(), 1, '空の行を重ねない');
  assert.equal(await active(p), '1 行目の NE コード');
  // コード → 空の名前に商品名 (#1692)
  const c1 = r1.locator('.c-code');
  await c1.fill('k004');
  await c1.press('Tab');
  await p.waitForFunction(() => document.getElementById('name').value !== '');
  const nm = (await row('k004')).name;
  assert.equal(await p.inputValue('#name'), nm);
  assert.equal(await dirty(p), 2);
  // 2 行目を足せる・空の 2 行目は数えない
  await p.click('#comp-add');
  assert.equal(await rows.count(), 2);
  assert.equal(await active(p), '2 行目の NE コード');
  assert.equal(await dirty(p), 2);
  assert.match(await p.textContent('#save-diff'), /なし[\s\S]*→[\s\S]*k004×1(?!,)/, '構成 = k004×1 だけ (空の行は入らない)');
  // 元に戻す = 1 行目だけの形・未保存 0
  await p.click('#revert');
  assert.equal(await rows.count(), 1);
  assert.deepEqual([await rows.first().locator('.c-code').inputValue(), (await rows.first().locator('.no').textContent()).trim()], ['', '1']);
  assert.match(await rows.first().textContent(), /代表/);
  assert.equal(await p.inputValue('#name'), '');
  assert.equal(await dirty(p), 0);
  // 入れ直して保存 (空の 2 行目があっても送らない)
  await rows.first().locator('.c-code').fill('k004');
  await rows.first().locator('.c-code').press('Tab');
  await p.waitForFunction(() => document.getElementById('name').value !== '');
  await p.click('#comp-add');
  await p.keyboard.press('Control+s');
  await p.waitForSelector('#saved-note');
  assert.match(await p.textContent('#saved-note'), /新しい対応を作りました/);
  assert.deepEqual(await mapOf('pr-row1-a'), { name: nm, state: 'active' });
  assert.deepEqual(await compsUi('pr-row1-a'), [['k004', 1]]);
  assert.equal(await rows.count(), 1, '読み直した後 = 保存した構成の 1 行 (空の行を足さない)');
  assert.equal(await rows.first().locator('.c-code').inputValue(), 'k004');
  // 対応を直す画面も同じ並び (構成 → 名前)
  assert.equal(await p.evaluate(() => !!(document.getElementById('comp').compareDocumentPosition(document.getElementById('name')) & Node.DOCUMENT_POSITION_FOLLOWING)), true);
});

await ta('[63] Amazon SKU (#1704 Codex R1): 既にある行のコードを消して保存 = 保存しない・理由 (構成から外れない) / 外すなら消える / 数だけ変えた新しい行は空の行でない (未保存に数える・「足す」は足す・保存は止める)', async (p) => {
  const rows = p.locator('#comp-rows tr.comp-row');
  const saveAndWait = async () => { await p.keyboard.press('Control+s'); await p.waitForSelector('#saved-note'); };
  // A×1・B×1 の対応を作る
  await p.goto(B + '/amazon/sku?sku=pr-row1-c');
  await rows.first().locator('.c-code').fill('k004');
  await rows.first().locator('.c-code').press('Tab');
  await p.waitForFunction(() => document.getElementById('name').value !== '');
  await p.click('#comp-add');
  await rows.nth(1).locator('.c-code').fill('k005');
  await rows.nth(1).locator('.c-code').press('Tab');
  await saveAndWait();
  assert.deepEqual(await compsUi('pr-row1-c'), [['k004', 1], ['k005', 1]]);
  assert.equal(await rows.count(), 2);
  // 既にある行 (読み込んだ k004) のコードを消して Ctrl+S = 保存しない・理由・その欄へ
  await rows.first().locator('.c-code').fill('');
  assert.equal(await dirty(p), 1, 'コードを消した = 変更 (構成から外れたことにはしない)');
  assert.match(await p.textContent('#save-diff'), /k004×1, k005×1[\s\S]*→[\s\S]*×1, k005×1/);
  await p.keyboard.press('Control+s');
  await p.waitForFunction(() => /コード\) が空です/.test(document.getElementById('msg').textContent));
  assert.match(await p.textContent('#msg'), /^1 行目の NE の商品 \(コード\) が空です。何も保存していません。.*「外す」/);
  assert.equal(await active(p), '1 行目の NE コード');
  assert.deepEqual(await compsUi('pr-row1-c'), [['k004', 1], ['k005', 1]], '何も保存していない (k004 は外れていない)');
  // 「外す」(ゴミ箱) なら構成から外れる
  await rows.first().locator('button[data-act="del"]').click();
  assert.equal(await rows.count(), 1);
  await saveAndWait();
  assert.deepEqual(await compsUi('pr-row1-c'), [['k005', 1]]);
  // 数だけ 2 にした新しい行 = 空の行ではない (未保存に数える・「足す」は新しい行を足す・保存は止める)
  await p.click('#comp-add');
  assert.equal(await rows.count(), 2);
  assert.equal(await dirty(p), 0, '足しただけの行は数えない');
  await rows.nth(1).locator('.c-qty').fill('2');
  assert.equal(await dirty(p), 1, '数を変えた新しい行は数える');
  await p.click('#comp-add');
  assert.equal(await rows.count(), 3, '数を変えた行は空の行でない = 「足す」は新しい行を足す (判定は 1 つ)');
  await p.keyboard.press('Control+s');
  await p.waitForFunction(() => /2 行目の NE の商品 \(コード\) が空です/.test(document.getElementById('msg').textContent));
  assert.deepEqual(await compsUi('pr-row1-c'), [['k005', 1]], '何も保存していない');
  // 一度コードを入れた新しい行 = 既にある行と同じ (消しても数える)
  await rows.nth(2).locator('.c-code').fill('k002');
  await rows.nth(2).locator('.c-code').fill('');
  await p.click('#comp-add');
  assert.equal(await rows.count(), 4, '一度コードが入った行は空の行でない');
  await p.click('#revert');
  assert.equal(await rows.count(), 1);
  assert.equal(await dirty(p), 0);
  assert.equal(await p.textContent('#msg'), '', '元に戻すと理由も消える');
});

await ta('[62] Amazon SKU の新しい対応 (390・タッチ): 1 行目がある・勝手にキーボードを出さない (フォーカスしない)・「足す」は 44px・未保存 0', async (p, ctx) => {
  await ctx.close().catch(() => {});
  const c2 = await browser.newContext({ viewport: { width: 390, height: 844 }, hasTouch: true, isMobile: true, locale: 'ja-JP' });
  const m = await c2.newPage();
  const errs = []; m.on('pageerror', (e) => errs.push(e.message));
  try {
    await m.goto(B + '/amazon/sku?sku=pr-row1-b');
    assert.equal(await m.locator('#comp-rows tr.comp-row').count(), 1);
    assert.equal(await m.evaluate(() => window.matchMedia('(pointer: fine)').matches), false, '前提: タッチの画面');
    assert.equal(await m.evaluate(() => document.activeElement && document.activeElement.classList.contains('c-code')), false, 'スマホでは勝手にコードの欄へ行かない');
    assert.ok((await m.locator('#comp-add').boundingBox()).height >= 43.5);
    assert.equal(await m.evaluate(() => window.MasterEdit.dirty().n), 0);
    assert.deepEqual(errs, [], '画面の JS の例外');
  } finally { await c2.close(); }
});

// ── [67] スマホの幅 (390 / 360 / 境目の 760・10/13): 主な画面がどれも横にはみ出さない (表は表の箱の中で横に送る)・上の帯は 1 行・押す所は 44px で真ん中を押すとその物に当たる ──
// [68] 390 で開く物 (候補の一覧・全体から探す・吹き出し・引き出し・窓・列の板) が画面の中で切れていない・押せる / [69] 390 の上の帯から行ける道 (ポータル・画面の切り替え・全体から探す)
// MASTER_EDIT_UI_SHOTS_390=フォルダ を付けると 390 幅の写しを残す (目で見る用)
const SHOTS390 = process.env.MASTER_EDIT_UI_SHOTS_390 || '';
{
  // Amazon の出品一覧 (0074) の見本 = 「まだつないでいない SKU」の行と seller SKU の画面の「Amazon の出品一覧では」を描く
  const IL = await import('../apps/company-db/ingest/amazon-listings.mjs');
  const today = new Date(Date.now() + 9 * 3600e3).toISOString().slice(0, 10);
  const lr = (sku, name, fba = true) => ({ seller_sku: sku, item_name: name, asin: 'B0000UI390', open_date: today, open_date_raw: `${today} 10:00:00 JST`, fulfillment: fba ? 'FBA' : 'FBM', channel_raw: fba ? 'AMAZON_NA' : 'DEFAULT', status: 'active', status_raw: 'Active', quantity: 3, price: '1280.00' });
  const rows = [lr('pr-k001', '国産 はちみつ 500g'), lr('pr-ui390-unlinked-very-long-seller-sku-0001', '長い名前の試験: 国産 有機 シリコン 保存袋 Sサイズ 3枚入 まとめ買い 12 個セット 送料無料 ギフト ラッピング対応'), lr('pr-ui390-b', 'はちみつ レモン キャンディ')];
  const gen = Date.now() - 3600e3;
  const got = await IL.ingestAmazonListings(db, { shop_code: IL.SHOP_CODE, generation: gen, snapshot_at: new Date(gen).toISOString(), report_run_id: 'r_ui390', checksum: IL.listingChecksum(rows.map((r) => IL.listingRowOf(r))), row_count: rows.length, skipped_count: 0, rows }, { nowMs: Date.now() });
  assert.equal(got.status, 'applied');
  // マスタの判断の候補 1 件 (一覧の行と右から出る引き出しを描く)
  const { writeDecisions } = await import('../apps/company-db/master-compare/decisions.mjs');
  const fp = 'f'.repeat(64);
  await writeDecisions(db, { compareRunId: 'mc_20300101T000000001Z_abcdef', observedAt: '2030-01-01T00:00:00Z', decisions: [{ fingerprint: fp, subject_key: 'value:s001', code_norm: 's001', col: 'tax_rate', child: null, cls: 'ne_no_value', reason_kind: 'tax_fallback',
    semantic: 'tax_fallback@1', print: { code_norm: 's001', col: 'tax_rate', n: null, n_state: 'empty', c: 0.1, reason: { reason: 'tax_fallback' } }, resolutions: ['accept_difference', 'fix_ne'], proposal: { op: 'set_ne_value', value: 0.1 } }] });
}
// [名前, 道, その画面にあるはずの物 (見えている)]
const MOBILE_SCREENS = [
  ['一覧', '/', '#list-page #list-search'], ['一覧_絞り込み', '/?q=はちみつ', '#list-page #list-tbl'], ['単品', '/sku/s002', '#sku-page #f-reorder_months'], ['セット', '/sku/set001', '#sku-page #set-mode'], ['変更の記録', '/sku/s003/history', '#history-page'],
  ['新商品_単品', '/new?kind=single', '#new-page #code'], ['新商品_セット', '/new?kind=set', '#new-page #comp'], ['新商品_まとまり', '/new?kind=variation', '#new-page .kindcards'],
  ['Amazon_一覧', '/amazon/', '#amazon-page #list-search'], ['Amazon_1つのSKU', '/amazon/sku?sku=pr-k001', '#amazon-sku-page #comp'], ['Amazon_新しいSKU', '/amazon/sku?sku=pr-ui390-b', '#amazon-sku-page #comp-rows .c-code'],
  ['Amazon_つないでいない', '/amazon/unlinked', '#unlinked-page .ul-row'], ['Amazon_対応が無い', '/amazon/unmapped?channel=all', '#unmapped-page'], ['Amazon_変更の記録', '/amazon/sku/history?sku=pr-k001-b', '#history-page'],
  ['Amazon_作る', '/amazon/new?sku=pr-k001-bx', '#amazon-new-page'], ['NE登録のCSV', '/reg-csv', '#rc'], ['NEから消す', '/ne-delete', '#nd'], ['発注の設定の届け待ち', '/order-settings-outbox', '#poq-page'], ['つかいかた', '/manual', '.page.doc'],
  ['マスタの判断', '/apps/master-decisions/', '#md-page #rows a.rowlink'], ['マスタの判断_CSV', '/apps/master-decisions/csv', '#csv-page'], ['マスタの判断_つかいかた', '/apps/master-decisions/manual', '.page.doc'],
];
const mobileUrl = (url) => (url.startsWith('/apps/') ? new URL(url, B).href : B + url);
/** 開いて待つ: 200・上の帯・画面ごとの物があること (無ければ投げる)・動きが終わるまで */
async function openMobile(p, name, url, sel) {
  const res = await p.goto(mobileUrl(url));
  assert.equal(res.status(), 200, `${name}: ${res.status()}`);
  await p.waitForLoadState('networkidle');
  await p.waitForSelector(sel, { state: 'visible', timeout: 5000 }).catch(() => { throw new Error(`${name}: 画面の物 ${sel} が見えない`); });
  assert.equal(await p.locator('header.hdr').isVisible(), true, `${name}: 上の帯が無い`);
  await p.evaluate(() => Promise.all(document.getAnimations().map((a) => a.finished.catch(() => {}))));
}
/**
 * スマホの幅の測り: ページの横幅・はみ出している一番外の要素 (横に送る箱の中・fixed は除く)・上の帯の高さ・
 * 押す所 (上の帯・左の列・「絞る」・本文のボタン / 切り替え / 札 / 欄・下の保存の箱) の大きさと、真ん中を押すとその物に当たるか (重なり・保存の箱の下に隠れない)
 */
const measureMobile = (p) => p.evaluate(() => {
  const W = document.documentElement.clientWidth;
  const tag = (el) => `${el.tagName.toLowerCase()}${el.id ? '#' + el.id : ''}${typeof el.className === 'string' && el.className ? '.' + el.className.trim().split(/\s+/).join('.') : ''}`;
  const inBox = (el) => { for (let a = el.parentElement; a && a !== document.body; a = a.parentElement) { const s = getComputedStyle(a); if (s.overflowX !== 'visible' || s.position === 'fixed') return true; } return false; };
  const out = [...document.querySelectorAll('body *')].filter((el) => { const r = el.getBoundingClientRect(); return r.width > 0 && r.height > 0 && r.right > W + 1 && getComputedStyle(el).position !== 'fixed' && !inBox(el); });
  const wide = out.filter((el) => !out.includes(el.parentElement)).slice(0, 6).map((el) => `${tag(el)} 右 ${Math.round(el.getBoundingClientRect().right)}`);
  const seen = (el) => { const r = el.getBoundingClientRect(); const s = getComputedStyle(el); return r.width > 0 && r.height > 0 && s.visibility === 'visible' && !el.closest('[hidden], .sr') && el.checkVisibility({ contentVisibilityAuto: true, opacityProperty: true }) && !(el.closest('details:not([open])') && !el.closest('summary')); };   // 閉じた details の中 = 見えない (Chromium は大きさを持つ)
  const square = [...document.querySelectorAll('.hdr a, .hdr button, .rail a.nav, .bigsearch .go')].filter(seen);
  const tall = [...document.querySelectorAll('main .btn, main .seg button, main .chip, main input.in, main select.in, main textarea.in, .savebox .btn')].filter(seen).filter((el) => !square.includes(el));
  const small = [...square.filter((el) => { const r = el.getBoundingClientRect(); return r.height < 43.5 || r.width < 43.5; }), ...tall.filter((el) => el.getBoundingClientRect().height < 43.5)]
    .slice(0, 8).map((el) => `${tag(el)} ${Math.round(el.getBoundingClientRect().width)}×${Math.round(el.getBoundingClientRect().height)}`);
  const blocked = [];
  for (const el of [...square, ...tall]) {
    if (getComputedStyle(el).position !== 'fixed' && !el.closest('.hdr, .rail, .savebox')) el.scrollIntoView({ block: 'center', inline: 'center', behavior: 'instant' });
    const r = el.getBoundingClientRect();
    const hit = document.elementFromPoint(r.left + r.width / 2, r.top + r.height / 2);
    if (!hit || !(hit === el || el.contains(hit) || (hit.control && hit.control === el))) blocked.push(`${tag(el)} の上に ${hit ? tag(hit) : '何も無い'}`);
    if (blocked.length >= 6) break;
  }
  // 下に固定の保存の箱: いちばん下まで送ったとき、本文の最後の物が箱の下に隠れない (箱の上で読める・押せる)
  const sb = [...document.querySelectorAll('.savebox')].find((x) => seen(x) && getComputedStyle(x).position === 'fixed');
  if (sb) {
    window.scrollTo({ top: document.documentElement.scrollHeight, behavior: 'instant' });
    const bottom = Math.max(...[...document.querySelectorAll('main *')].filter((x) => seen(x) && !sb.contains(x) && getComputedStyle(x).position !== 'fixed' && !x.closest('.palette-bg, .modal-bg')).map((x) => x.getBoundingClientRect().bottom));
    const top = sb.getBoundingClientRect().top;
    if (bottom > top + 1) blocked.push(`本文の最後 (下端 ${Math.round(bottom)}) が保存の箱 (上端 ${Math.round(top)}) の下に隠れる`);
  }
  window.scrollTo(0, 0);
  const hdr = document.querySelector('.hdr');
  return { sw: document.documentElement.scrollWidth, W, wide, small, blocked, hdrH: hdr ? Math.round(hdr.getBoundingClientRect().height) : 0, n: square.length + tall.length };
});
for (const w of [390, 360, 760]) {
  await ta(`[67] スマホ ${w}: 主な画面 22 (一覧・商品・新商品 3 種・Amazon SKU 7 つ・NE 登録の CSV・NE から消す・マスタの判断・つかいかた) が横にはみ出さない・上の帯は 1 行・押す所は 44px で押せる`, async (p) => {
    process.env.MASTER_DECISION_APPROVERS = 'naka@test';   // NE 登録の CSV・NE から消す・マスタの判断を操作できる人の画面で (ボタンが一番多い)
    const bad = [];
    let targets = 0;
    try {
      for (const [name, url, sel] of MOBILE_SCREENS) {
        await openMobile(p, name, url, sel);
        const m = await measureMobile(p);
        targets += m.n;
        if (m.sw > m.W || m.wide.length || m.small.length || m.blocked.length) bad.push(`${name} (${url}): 横幅 ${m.sw} / ${m.W}・はみ出す ${JSON.stringify(m.wide)}・小さい ${JSON.stringify(m.small)}・押せない ${JSON.stringify(m.blocked)}`);
        if (m.hdrH > 58) bad.push(`${name}: 上の帯が ${m.hdrH}px (1 行の 58px に収まらない)`);
        if (SHOTS390 && w === 390) await p.screenshot({ path: `${SHOTS390}/${name}.png`, fullPage: true });
      }
      // 上の帯に「未保存」と「発注の設定が届いていない商品」の札が両方出たとき (一番混む形) も 1 行・はみ出さない・押せる
      await openMobile(p, '札が全部', '/sku/s002', '#f-reorder_months');
      await p.fill('#f-reorder_months', '5');
      await p.evaluate(() => { document.getElementById('poq-pill').hidden = false; document.getElementById('poq-pill-t').textContent = '発注の設定が届いていない商品 12 件'; });
      assert.equal(await p.isVisible('#unsaved'), true);
      assert.equal(await p.isVisible('#poq-pill'), true);
      const m = await measureMobile(p);
      if (m.sw > m.W || m.wide.length || m.small.length || m.blocked.length || m.hdrH > 58) bad.push(`札が全部出た上の帯: 横幅 ${m.sw} / ${m.W}・高さ ${m.hdrH}・はみ出す ${JSON.stringify(m.wide)}・小さい ${JSON.stringify(m.small)}・押せない ${JSON.stringify(m.blocked)}`);
      if (SHOTS390) await p.screenshot({ path: `${SHOTS390}/上の帯_札が全部_${w}.png` });
    } finally { delete process.env.MASTER_DECISION_APPROVERS; }
    assert.deepEqual(bad, [], `${w} 幅ではみ出す・押せない画面:\n${bad.join('\n')}`);
    assert.ok(targets > 300, `押す所を見ている数 ${targets}`);
  }, { width: w, height: 844 });
}

/** 開いた物が画面の中にあり、overflow の箱 (表の箱・板など) に切られていない・真ん中を押すとその物に当たる */
const unclipped = (p, sel) => p.evaluate((sel) => {
  const el = document.querySelector(sel);
  if (!el) return [`${sel} が無い`];
  const r = el.getBoundingClientRect();
  const bad = [];
  if (!(r.width > 0 && r.height > 0)) return [`${sel} が見えない`];
  if (r.left < -1 || r.right > document.documentElement.clientWidth + 1 || r.top < -1 || r.bottom > innerHeight + 1) bad.push(`${sel} が画面の外 ${JSON.stringify([r.left, r.top, r.right, r.bottom].map(Math.round))}`);
  for (let a = el.parentElement; a && a !== document.documentElement; a = a.parentElement) {
    const s = getComputedStyle(a);
    if (s.overflowX === 'visible' && s.overflowY === 'visible') continue;
    const b = a.getBoundingClientRect();
    if (r.left < b.left - 1 || r.right > b.right + 1 || r.top < b.top - 1 || r.bottom > b.bottom + 1) bad.push(`${sel} が ${a.tagName.toLowerCase()}${a.id ? '#' + a.id : ''}.${a.className} に切られる`);
  }
  const hit = document.elementFromPoint(r.left + r.width / 2, r.top + Math.min(r.height / 2, 20));
  if (!hit || !(hit === el || el.contains(hit))) bad.push(`${sel} の上に ${hit ? hit.tagName + '.' + hit.className : '何も無い'}`);
  return bad;
}, sel);
const settle = (p) => p.waitForTimeout(350);   // 出てくる動き (0.15〜0.22 秒) の後に測る
await ta('[68] スマホ 390: 開く物 (Amazon SKU の構成の候補・全体から探す・左の列の吹き出し・列の板・マスタの判断の引き出し・隠すの窓・離れるときの確認) が画面の中で切れていない・押せる', async (p) => {
  process.env.MASTER_DECISION_APPROVERS = 'naka@test';
  const bad = [];
  const shot = async (n) => { if (SHOTS390) await p.screenshot({ path: `${SHOTS390}/開く_${n}.png` }); };
  try {
    // Amazon SKU の構成の候補 (表の箱の中の欄の下に出る・箱の外に 1 つだけ置いてある)
    await openMobile(p, 'Amazon_新しいSKU', '/amazon/sku?sku=pr-ui390-b', '#comp-rows .c-code');
    const code = p.locator('#comp-rows tr.comp-row').first().locator('.c-code');
    await code.evaluate((el) => window.scrollTo(0, el.getBoundingClientRect().top + window.scrollY - 120));
    await code.fill('はち');
    await p.waitForSelector('#comp-suggest:not([hidden]) [role="option"]');
    await settle(p);
    bad.push(...await unclipped(p, '#comp-suggest'), ...await unclipped(p, '#comp-suggest [role="option"]'));
    await shot('構成の候補');
    // 全体から探す (虫めがね → 窓 → 候補)
    await openMobile(p, '一覧', '/', '#list-search');
    await p.click('#open-palette');
    await p.waitForSelector('#palette-bg.on #pal-in');
    await p.fill('#pal-in', 'k001');
    await p.waitForSelector('#pal-res [role="option"]');
    await settle(p);
    bad.push(...await unclipped(p, '#palette-bg .palette'), ...await unclipped(p, '#pal-res [role="option"]'));
    await shot('全体から探す');
    await p.keyboard.press('Escape');
    // 左の列の吹き出し (アイコンの名前)
    await p.hover('.rail a.nav[aria-label="Amazon SKU"]');
    await settle(p);
    bad.push(...await unclipped(p, '.rail a.nav[aria-label="Amazon SKU"] .lbl'));
    await shot('左の列の吹き出し');
    // 列の板 (下から出る)
    await p.mouse.move(300, 600);
    await p.click('#colbtn');
    await settle(p);
    bad.push(...await unclipped(p, '#colpanel'));
    // マスタの判断の引き出し (右から出る)
    await openMobile(p, 'マスタの判断', '/apps/master-decisions/', '#rows a.rowlink');
    await p.click('#rows a.rowlink');
    await p.waitForSelector('#dr-bg', { state: 'visible' });
    await settle(p);
    bad.push(...await unclipped(p, '#drawer'));
    await shot('判断の引き出し');
    // まだつないでいない SKU の「隠す」の窓
    await openMobile(p, 'Amazon_つないでいない', '/amazon/unlinked', '.ul-row button[data-act="hide"]');
    await p.click('.ul-row button[data-act="hide"]');
    await p.waitForSelector('#hide-bg.on .modal');
    await settle(p);
    bad.push(...await unclipped(p, '#hide-bg .modal'));
    await shot('隠すの窓');
    // 離れるときの確認 (未保存のまま左の列を押す)
    await openMobile(p, '単品', '/sku/s002', '#f-reorder_months');
    await p.fill('#f-reorder_months', '6');
    await p.click('.rail a.nav[aria-label="つかいかた"]');
    await p.waitForSelector('#leave-bg.on .modal');
    await settle(p);
    bad.push(...await unclipped(p, '#leave-bg .modal'), ...await unclipped(p, '#leave-stay'));
    await shot('離れるときの確認');
  } finally { delete process.env.MASTER_DECISION_APPROVERS; }
  assert.deepEqual(bad, [], `切れている・押せない:\n${bad.join('\n')}`);
}, { width: 390, height: 844 });

await ta('[69] スマホ 390: 上の帯から行ける道 = ポータル (B-Faith Portal)・画面の切り替え (左の列)・全体から探す が見えて押せる。隠すのは「ダッシュボード」(ポータルと同じ行き先) と「マスタの入力」の札だけ', async (p) => {
  for (const [name, url, sel] of [MOBILE_SCREENS[0], MOBILE_SCREENS[19]]) {
    await openMobile(p, name, url, sel);
    // 上の帯の押す物で見えないのは決めた 2 つだけ (ほかの .btn = ログアウトなどを足しても隠れない)
    await p.evaluate(() => { const a = document.createElement('a'); a.className = 'btn sm'; a.id = 'probe-logout'; a.href = '/logout'; a.textContent = 'ログアウト'; document.querySelector('.hdr .user').appendChild(a); });
    const hiddenOnes = await p.evaluate(() => [...document.querySelectorAll('.hdr a, .hdr button')].filter((el) => !el.closest('[hidden]') && el.getBoundingClientRect().width === 0).map((el) => el.classList.contains('hdr-dash') ? 'ダッシュボード' : el.classList.contains('app-pill') ? '札' : el.id || el.className));
    assert.deepEqual(hiddenOnes.sort(), ['ダッシュボード', '札'].sort(), `${name}: 上の帯で隠れている物`);
    assert.equal(await p.isVisible('#probe-logout'), true, `${name}: 上の帯のほかのボタン (ログアウトなど) は隠さない`);
    assert.equal(await p.getAttribute('.hdr .brand > a:first-child', 'href'), await p.getAttribute('.hdr .hdr-dash', 'href'), 'ポータルの名前とダッシュボードは同じ行き先');
  }
  // ポータルへ (名前を押す)
  await openMobile(p, '一覧', '/', '#list-search');
  await Promise.all([p.waitForURL((u) => new URL(u).pathname === '/'), p.click('.hdr .brand > a:first-child')]);
  // 画面の切り替え (左の列)
  await openMobile(p, '一覧', '/', '#list-search');
  await Promise.all([p.waitForURL(/\/amazon\/$/), p.click('.rail a.nav[aria-label="Amazon SKU"]')]);
  await Promise.all([p.waitForURL(/master-decisions\/$/), p.click('.rail a.nav[aria-label="マスタの判断 (NE との差)"]')]);
  // 全体から探す → 操作で別の画面へ
  await p.click('#open-palette');
  await p.waitForSelector('#palette-bg.on #pal-in');
  await p.fill('#pal-in', 'NE 登録');
  const op = p.locator('#pal-res [role="option"]', { hasText: 'NE 登録の CSV を開く' });
  await op.waitFor();
  await Promise.all([p.waitForURL(/\/reg-csv$/), op.click()]);
}, { width: 390, height: 844 });

await ta('[71] 発注の設定のグループを名前・ID で絞る (10/16 中原さん): ひらがなでカタカナの名前に当たる・空白で全部を含む・当たり 1 つ = 選ぶ・選んだグループは絞っても消えない・代表の仕入先のグループだけ・絞る欄の Enter で登録に送らない', async (p) => {
  const now = new Date().toISOString();
  const insM = mirrorDb.prepare('insert or replace into po_material_groups (group_id, name, min_order_qty, unit, created_at, updated_at) values (?, ?, ?, ?, ?, ?)');
  for (const [id, name] of [['djktcleaner', 'ダウンジャケット 洗剤'], ['handcream-base', 'ハンドクリーム 原料'], ['gum-base', 'ガムベース']]) insM.run(id, name, 100, 'kg', now, now);
  const insC = mirrorDb.prepare('insert or replace into po_order_conditions (condition_id, supplier_code, display_name, condition_type, condition_value, unit, created_at, updated_at) values (?, ?, ?, ?, ?, ?, ?, ?)');
  for (const [id, sup, name] of [['maruma-30000', '1', 'マルマ商事 3 万円以上'], ['maruma-box', '1', 'マルマ 箱単位'], ['maruma-other', '2', 'マルマ よその仕入先']]) insC.run(id, sup, name, 'min_amount', 30000, '円', now, now);
  const opts = (sel) => p.$$eval(`${sel} option`, (os) => os.filter((o) => o.value && !o.disabled).map((o) => o.value));
  try {
    await p.goto(B + '/new?kind=single');
    await p.waitForSelector('#po-mat-mode');
    // 原料グループ
    await p.click('#po-mat-mode button[data-v="pick"]');
    assert.deepEqual((await opts('#po-material_group_id')).sort(), ['djktcleaner', 'gum-base', 'handcream-base']);
    await p.fill('#po-mat-q', 'だうん');
    assert.deepEqual(await opts('#po-material_group_id'), ['djktcleaner'], 'ひらがなでカタカナの名前に当たる');
    assert.equal(await p.inputValue('#po-material_group_id'), 'djktcleaner', '当たり 1 つ = 選ぶ');
    assert.match(await p.textContent('#po-mat-q-n'), /当たり 1 件/);
    // 絞り込みが選んだグループは、打ち直した言葉に当たらなくなったら取り消す (Codex R1 Medium)
    await p.fill('#po-mat-q', 'base');
    assert.deepEqual((await opts('#po-material_group_id')).sort(), ['gum-base', 'handcream-base'], 'ID でも当たる');
    assert.equal(await p.inputValue('#po-material_group_id'), '', '絞り込みが選んだグループは当たらなくなったら取り消す');
    assert.match(await p.textContent('#po-mat-q-n'), /当たり 2 件/);
    // 人が選んだグループは、絞っても消えない・取り消さない
    await p.selectOption('#po-material_group_id', 'gum-base');
    await p.fill('#po-mat-q', 'だうん');
    assert.deepEqual((await opts('#po-material_group_id')).sort(), ['djktcleaner', 'gum-base'], '人が選んだグループは消えない');
    assert.equal(await p.inputValue('#po-material_group_id'), 'gum-base', '人が選んだグループは選び直さない');
    assert.match(await p.textContent('#po-mat-q-n'), /当たり 1 件/, '選んでいるだけで当たらないグループは数えない');
    await p.fill('#po-mat-q', 'zzz');
    assert.match(await p.textContent('#po-mat-q-n'), /当たるグループはありません/);
    await p.fill('#po-mat-q', '');
    assert.equal(await p.isHidden('#po-mat-q-n'), true);
    assert.equal((await opts('#po-material_group_id')).length, 3, '消すと全部に戻る');
    assert.equal(await p.inputValue('#po-material_group_id'), 'gum-base');
    // 変換中 (IME) は選ばない = 「かみ」の変換中に「カミ」を選んで「紙」に確定しても残る、をしない (Codex R1 Medium)
    insM.run('kami-a', 'カミ 原料', 1, 'kg', now, now); insM.run('paper-b', '紙 原料', 1, 'kg', now, now);
    await p.goto(B + '/new?kind=single');
    await p.waitForSelector('#po-mat-mode');
    await p.click('#po-mat-mode button[data-v="pick"]');
    await p.focus('#po-mat-q');
    const cdp = await p.context().newCDPSession(p);
    await cdp.send('Input.imeSetComposition', { text: 'かみ', selectionStart: 2, selectionEnd: 2 });
    assert.deepEqual(await opts('#po-material_group_id'), ['kami-a'], '変換中も絞る');
    assert.equal(await p.inputValue('#po-material_group_id'), '', '変換中は選ばない');
    await cdp.send('Input.insertText', { text: '紙' });
    await p.waitForFunction(() => document.getElementById('po-material_group_id').value !== '');
    assert.equal(await p.inputValue('#po-material_group_id'), 'paper-b', '確定した言葉で選ぶ');
    // 絞る欄の Enter = 登録に送らない (画面はそのまま)
    const url = p.url();
    await p.focus('#po-mat-q'); await p.keyboard.press('Enter');
    assert.equal(p.url(), url);
    // 発注条件グループ = 代表の仕入先のグループだけ・空白で全部を含む
    await p.selectOption('#f-primary_supplier', '0001');
    await p.click('#po-cond-mode button[data-v="pick"]');
    await p.fill('#po-cond-q', 'まるま');
    assert.deepEqual((await opts('#po-condition_id')).sort(), ['maruma-30000', 'maruma-box'], 'よその仕入先のグループは当たらない');
    assert.equal(await p.inputValue('#po-condition_id'), '', '当たり 2 つ = まだ選ばない');
    await p.fill('#po-cond-q', 'まるま　箱');
    assert.deepEqual(await opts('#po-condition_id'), ['maruma-box'], '空白 (全角も) で区切った言葉を全部含む');
    assert.equal(await p.inputValue('#po-condition_id'), 'maruma-box');
    assert.ok((await dirty(p)) >= 1, '選んだ = 未保存に数える');
  } finally {
    mirrorDb.prepare("delete from po_material_groups where group_id in ('djktcleaner', 'handcream-base', 'gum-base', 'kami-a', 'paper-b')").run();
    mirrorDb.prepare("delete from po_order_conditions where condition_id in ('maruma-30000', 'maruma-box', 'maruma-other')").run();
  }
});

// ── [72]〜[74] 1 つの商品の画面の「← 前へ」「次へ →」(10/17 中原さん) ──
/** 前へ・次へが出るまで待って、{ pos, prev, next } (押せない = null) */
const pnState = async (p) => {
  await p.waitForSelector('#sku-pn:not([hidden]) #pn-pos', { timeout: 5000 });
  return p.evaluate(() => {
    const hrefOf = (id) => { const el = document.getElementById(id); return el && el.tagName === 'A' ? decodeURIComponent(el.getAttribute('href').split('/sku/')[1]) : null; };
    return { pos: document.getElementById('pn-pos').textContent.trim(), prev: hrefOf('pn-prev'), next: hrefOf('pn-next'),
      prevOff: document.getElementById('pn-prev').disabled === true, nextOff: document.getElementById('pn-next').disabled === true };
  });
};
const codesFor = (p, qs) => p.evaluate((u) => fetch(u).then((r) => r.json()).then((j) => j.codes), `${B}/api/codes${qs}`);
await ta('[72] 前へ・次へ (10/17): 来た一覧 (同じ絞り込み・並び) の中の前・次と「2 / 3」・続けて押しても同じ一覧・最初 / 最後は押せない・一覧に無い・一覧から来ていない / 期限切れ = 出さない・パンくずは同じ一覧へ', async (p) => {
  await p.goto(B + '/?q=' + encodeURIComponent('はちみつ'));
  const codes = await listCodes(p);
  assert.deepEqual(codes, await codesFor(p, '?q=' + encodeURIComponent('はちみつ')));
  assert.equal(codes.length, 3);
  await Promise.all([p.waitForNavigation(), p.click(`a.rowlink:text-is("${codes[1]}")`)]);
  assert.deepEqual(await pnState(p), { pos: '2 / 3', prev: codes[0], next: codes[2], prevOff: false, nextOff: false });
  // 次へ → 最後 (次へは押せない)
  await Promise.all([p.waitForNavigation(), p.click('#pn-next')]);
  assert.match(p.url(), new RegExp(`sku/${codes[2]}$`));
  assert.deepEqual(await pnState(p), { pos: '3 / 3', prev: codes[1], next: null, prevOff: false, nextOff: true });
  // 前へを 2 回 = 最初 (前へは押せない)。続けて押しても同じ一覧の中
  await Promise.all([p.waitForNavigation(), p.click('#pn-prev')]);
  assert.deepEqual((await pnState(p)).pos, '2 / 3');
  await Promise.all([p.waitForNavigation(), p.click('#pn-prev')]);
  assert.match(p.url(), new RegExp(`sku/${codes[0]}$`));
  assert.deepEqual(await pnState(p), { pos: '1 / 3', prev: null, next: codes[1], prevOff: true, nextOff: false });
  // パンくずの「商品・セット」= 同じ絞り込みの一覧のまま
  assert.equal(new URL(await p.locator('.crumb a').first().evaluate((a) => a.href)).searchParams.get('q'), 'はちみつ');
  // 一覧に無い商品 (同じタブで別の商品を開いた・直して絞り込みに合わなくなった) = 「一覧に無い」・前へ・次へは押せない
  await p.goto(B + '/sku/s001');
  const out = await pnState(p);
  assert.match(out.pos, /^一覧に無い \(全 3 件\)$/);
  assert.deepEqual([out.prevOff, out.nextOff], [true, true]);
  // 並び (名前の逆順・区分で絞る) も一覧と同じ
  await p.goto(B + '/?kind=single&sort=name&dir=desc');
  const byName = await codesFor(p, '?kind=single&sort=name&dir=desc');
  assert.deepEqual(await listCodes(p), byName.slice(0, 100));
  await Promise.all([p.waitForNavigation(), p.click(`a.rowlink:text-is("${byName[4]}")`)]);
  assert.deepEqual(await pnState(p), { pos: `5 / ${byName.length}`, prev: byName[3], next: byName[5], prevOff: false, nextOff: false });
  for (let i = 5; i < 8; i++) {
    await Promise.all([p.waitForNavigation(), p.click('#pn-next')]);
    assert.match(p.url(), new RegExp(`sku/${byName[i]}$`), `${i + 1} 番目`);
    assert.equal((await pnState(p)).pos, `${i + 1} / ${byName.length}`);
  }
  // 在庫 (ロジザードの写し) で並べた一覧・在庫で絞った一覧でも同じ並び (写しが読める = この試験の見本)
  for (const qs of ['sort=stock&dir=desc', 'stock_min=1&sort=stock']) {
    const sc = await codesFor(p, '?' + qs);
    assert.ok(sc.length >= 2, qs);
    const got = await p.evaluate((u) => fetch(u).then((r) => r.json()), `${B}/api/neighbors?${qs}&cur=${encodeURIComponent(sc[1])}`);
    assert.deepEqual(got, { ok: true, total: sc.length, pos: 2, prev: sc[0], next: sc[2] ?? null, offset: 0 }, qs);
  }
  // パンくずの戻り先 = この商品が載る一覧のページ (覚えた一覧が 2 ページ目でも、商品が 1 ページ目にあれば offset を外す)
  await p.evaluate((base) => sessionStorage.setItem('master-edit:list-url', base + '/?kind=single&offset=100'), new URL(B).pathname);
  await p.goto(B + '/sku/s002');
  await pnState(p);
  const crumb = new URL(await p.locator('.crumb a').first().evaluate((a) => a.href));
  assert.deepEqual([crumb.searchParams.get('kind'), crumb.searchParams.has('offset')], ['single', false]);
  // 条件の印 (?s=) が消えた (期限切れ・再起動) = 出さない (パンくずはそのまま)
  await p.evaluate((base) => sessionStorage.setItem('master-edit:list-url', base + '/?s=AAAAAAAAAAAAAAAAAAAAAA'), new URL(B).pathname);
  const [gone] = await Promise.all([p.waitForResponse((r) => r.url().includes('/api/neighbors?')), p.goto(B + '/sku/s002')]);
  assert.equal(gone.status(), 410);
  await p.waitForTimeout(200);
  assert.equal(await p.locator('#sku-pn').isHidden(), true, '期限切れ = 出さない');
  // 一覧から来ていない (覚えた一覧が無い) = 聞かない・出さない
  await p.evaluate(() => sessionStorage.clear());
  const asked = [];
  p.on('request', (r) => { if (r.url().includes('/api/neighbors?')) asked.push(r.url()); });
  await p.goto(B + '/sku/s002');
  await p.waitForTimeout(300);
  assert.deepEqual(asked, [], '一覧から来ていない = 聞かない');
  assert.equal(await p.locator('#sku-pn').isHidden(), true, '一覧から来ていない = 出さない');
});

await ta('[73] 前へ・次へ と未保存 (10/17): 変えたまま「次へ」= 離れるときの確認 (ここに残る = 同じ画面・押した所へフォーカス / 捨てて移る = 次の商品・保存していない)', async (p) => {
  await p.goto(B + '/?q=s00');
  const codes = await listCodes(p);
  assert.ok(codes.length >= 3, codes.join(','));
  await Promise.all([p.waitForNavigation(), p.click(`a.rowlink:text-is("${codes[1]}")`)]);
  const st = await pnState(p);
  assert.equal(st.next, codes[2]);
  const before = (await row(codes[1])).months;
  await p.fill('#f-reorder_months', String(before + 3));
  assert.equal(await dirty(p), 1);
  await p.click('#pn-next');
  assert.equal(await p.locator('#leave-bg.on').count(), 1, '次へでも聞く');
  assert.match(await p.textContent('#leave-list'), /推奨保有月数/);
  await p.click('#leave-stay');
  assert.match(p.url(), new RegExp(`sku/${codes[1]}$`), 'ここに残る = 同じ画面');
  assert.equal(await active(p), 'pn-next', '押した所へフォーカスが戻る');
  assert.equal(await dirty(p), 1, '変えた値は残る');
  // キーボード (Enter) でも聞く
  await p.keyboard.press('Enter');
  assert.equal(await p.locator('#leave-bg.on').count(), 1, 'Enter でも聞く');
  await p.click('#leave-stay');
  await p.click('#pn-prev');
  assert.equal(await p.locator('#leave-bg.on').count(), 1, '前へでも聞く');
  await p.click('#leave-stay');
  await p.click('#pn-next');
  await Promise.all([p.waitForNavigation(), p.click('#leave-drop')]);
  assert.match(p.url(), new RegExp(`sku/${codes[2]}$`), '捨てて移る = 次の商品');
  assert.equal((await row(codes[1])).months, before, '捨てたので保存していない');
  assert.equal((await pnState(p)).pos, `3 / ${codes.length}`, '移った先も同じ一覧');
});

await ta('[74] 前へ・次へ スマホ 390 (10/17): パンくずと前へ・次へが横にはみ出さない・押す所は 44px で押せる (長いコードでも)', async (p) => {
  await p.goto(B + '/?q=' + encodeURIComponent('はちみつ'));
  const codes = await listCodes(p);
  await Promise.all([p.waitForNavigation(), p.click(`a.rowlink:text-is("${codes[1]}")`)]);
  await pnState(p);
  await p.evaluate(() => Promise.all(document.getAnimations().map((a) => a.finished.catch(() => {}))));
  const m = await measureMobile(p);
  assert.ok(m.sw <= m.W && !m.wide.length && !m.small.length && !m.blocked.length, `横幅 ${m.sw} / ${m.W}・はみ出す ${JSON.stringify(m.wide)}・小さい ${JSON.stringify(m.small)}・押せない ${JSON.stringify(m.blocked)}`);
  const box = await p.evaluate(() => ['pn-prev', 'pn-next'].map((id) => { const r = document.getElementById(id).getBoundingClientRect(); return [Math.round(r.height), Math.round(r.right)]; }));
  for (const [h, right] of box) { assert.ok(h >= 44, `高さ ${h}`); assert.ok(right <= 390, `右 ${right}`); }
  // 長いコードの見出し (パンくず) でも はみ出さない
  await p.evaluate(() => { document.querySelector('.crumb .mono').textContent = 'pr-ui390-unlinked-very-long-seller-sku-0001-0002-0003'; });
  const m2 = await measureMobile(p);
  assert.ok(m2.sw <= m2.W && !m2.wide.length, `長いコード: 横幅 ${m2.sw} / ${m2.W}・はみ出す ${JSON.stringify(m2.wide)}`);
  await Promise.all([p.waitForNavigation(), p.click('#pn-next')]);
  assert.match(p.url(), new RegExp(`sku/${codes[2]}$`));
}, { width: 390, height: 844 });

// ── 未登録の一覧の「その場で登録」(10/17 中原さん)・構成の原価 ──
{
  // 売れたのに対応が無い FBA の SKU 2 つ (出品一覧 = FBA・Amazon の商品名つき) と、今日の注文
  const IL = await import('../apps/company-db/ingest/amazon-listings.mjs');
  const today = new Date(Date.now() + 9 * 3600e3).toISOString().slice(0, 10);
  const lr = (sku, name) => ({ seller_sku: sku, item_name: name, asin: null, open_date: today, open_date_raw: null, fulfillment: 'FBA', channel_raw: 'AMAZON_NA', status: 'active', status_raw: 'Active', quantity: 1, price: '980.00' });
  const rows = [lr('pr-ui-inl-a', 'Amazon の名前 みかん 2 個'), lr('pr-ui-inl-b', 'Amazon の名前 セット')];
  const gen = Date.now() - 600e3;
  const got = await IL.ingestAmazonListings(db, { shop_code: IL.SHOP_CODE, generation: gen, snapshot_at: new Date(gen).toISOString(), report_run_id: 'r_ui_inline', checksum: IL.listingChecksum(rows.map((r) => IL.listingRowOf(r))), row_count: rows.length, skipped_count: 0, rows, force: true }, { nowMs: Date.now() });
  assert.ok(['applied', 'refreshed'].includes(got.status), JSON.stringify(got));
  let n = 0;
  for (const code of ['pr-ui-inl-a', 'pr-ui-inl-b']) {
    const o = (await pg.query(`insert into core.orders (company_id, mall, scope_key, mall_order_no, source_system, ordered_at, order_date_jst, status, is_cancelled, received_batch_seq, source_updated_at, transform_version, content_hash)
      values (1, 'amazon', 'amazon', $1, 'mall_api', now(), $2::date, 'shipped', false, 1, now(), 't', 'h') returning order_id`, [`ui-inl-${++n}`, today])).rows[0].order_id;
    await pg.query(`insert into core.order_lines (company_id, order_id, line_key, listing_id, unresolved_code, qty, cancelled_qty, received_batch_seq) values (1, $1, 'l1', null, $2, 2, 0, 1)`, [o, code]);
  }
}
const inlSugOpen = (p) => p.waitForFunction(() => { const s = document.querySelector('.inl .csug'); return s && !s.hidden && !s.classList.contains('stale') && s.querySelector('.co, .cs-h'); });
const inlCodes = (p) => p.evaluate(() => [...document.querySelectorAll('.inl .csug .co')].map((o) => (o.classList.contains('off') ? '×' : '') + o.querySelector('.cc').textContent));
await ta('[76] 未登録の一覧の「その場で登録」(10/17): 行の下に欄・名前 = Amazon の商品名・NE の商品の候補 (日本語入力の確定の Enter で閉じない・ほかの行にある商品は選べない)・↓ Enter で選ぶ → 数の欄・足す・保存 = 1 つの画面と同じ道 (DB に入る)・押し直しても 2 回入らない・離れるときに確かめる / 構成の原価', async (p) => {
  await p.goto(B + '/amazon/unmapped');
  const btn = p.locator('button[data-inline][data-sku="pr-ui-inl-a"]');
  assert.equal(await btn.count(), 1, 'その場で登録のボタン');
  assert.ok((await p.textContent('#unmapped-list')).includes('Amazon の名前 みかん 2 個'), '行に Amazon の商品名');
  await btn.click();
  const box = p.locator('li.inl-box[data-inline-for="pr-ui-inl-a"]');
  await box.locator('.inl-body').waitFor({ state: 'visible' });
  assert.equal(await btn.getAttribute('aria-expanded'), 'true');
  assert.equal(await box.locator('.inl-name').inputValue(), 'Amazon の名前 みかん 2 個', '名前の初めの値 = Amazon の出品一覧の商品名');
  assert.equal(await active(p), '1 行目の NE コード (代表)', '開くと 1 行目の欄へ');
  assert.equal(await dirty(p), 0, '開いただけ = 未保存ではない');
  const in1 = box.locator('tr.comp-row').nth(0).locator('.c-code');
  // 日本語入力 (CDP): 変換中から候補・確定の Enter (Windows の並び = 確定の後に keyCode 13) で閉じない・数の欄へ移らない
  const cdp = await p.context().newCDPSession(p);
  const key = (type, k, vk) => cdp.send('Input.dispatchKeyEvent', { type, key: k, code: k === 'Enter' ? 'Enter' : 'KeyA', windowsVirtualKeyCode: vk, nativeVirtualKeyCode: vk });
  for (const s of ['み', 'みか', 'みかん']) { await key('rawKeyDown', 'Process', 229); await cdp.send('Input.imeSetComposition', { text: s, selectionStart: s.length, selectionEnd: s.length }); await key('keyUp', 'a', 65); }
  await inlSugOpen(p);
  assert.ok((await inlCodes(p)).includes('k006'), '変換中から候補');
  await cdp.send('Input.insertText', { text: 'みかん' });
  await key('rawKeyDown', 'Enter', 13); await key('keyUp', 'Enter', 13);
  await p.waitForTimeout(100);
  assert.equal(await active(p), '1 行目の NE コード (代表)', '確定の Enter で数の欄へ移らない');
  await inlSugOpen(p);
  await in1.press('ArrowDown');
  await in1.press('Enter');
  assert.equal(await in1.inputValue(), 'k006');
  assert.equal(await active(p), '1 行目 (k006) の数', '選ぶと数の欄へ');
  assert.equal(await box.locator('tr.comp-row').nth(0).locator('.c-name').textContent(), 'みかん');
  assert.equal(await dirty(p), 1, '入れた = 離れるときに確かめる');
  await p.keyboard.press('Control+a'); await p.keyboard.type('2');
  // 足す → 2 行目で同じ k006 は選べない・k005 を選ぶ
  await box.locator('.inl-add').click();
  const in2 = box.locator('tr.comp-row').nth(1).locator('.c-code');
  assert.equal(await active(p), '2 行目の NE コード');
  await in2.fill('k00');
  await inlSugOpen(p);
  assert.ok((await inlCodes(p)).includes('×k006'), 'ほかの行にある商品は灰色');
  await in2.fill('k005');
  await inlSugOpen(p);
  await in2.press('Enter');
  assert.equal(await in2.inputValue(), 'k005', 'ぴったりのコードは Enter で入る');
  // 保存 (通信が切れた 1 回目 → 押し直し = 同じ request_id)
  const ids = [];
  // 1 回目: 保存は入るが、途中の機械が 502 の JSON を返す (#Codex R2 M1 = JSON の誤りでも確かな返事でない = 送り直しに残す)
  let first = true;
  await p.route('**/api/amazon/save', async (r) => {
    ids.push(JSON.parse(r.request().postData()).request_id);
    if (first) { first = false; await r.fetch(); await r.fulfill({ status: 502, contentType: 'application/json', body: JSON.stringify({ error: 'Bad Gateway' }) }); return; }
    await r.continue();
  });
  await box.locator('.inl-save').click();
  await p.waitForFunction(() => /通信できませんでした/.test(document.querySelector('.inl-msg').textContent));
  assert.equal(await in1.isDisabled(), true, '結果の分からない保存の後 = 欄を閉じる (同じ中身だけを送り直す・#Codex R1 M)');
  await box.locator('.inl-save').click();
  await box.locator('.result.ok').waitFor();
  assert.equal(ids.length, 2); assert.equal(ids[0], ids[1], '押し直し = 同じ request_id');
  assert.match(await box.locator('.result.ok').textContent(), /保存しました \(前に保存した結果\)/, '2 回目は前の結果');
  assert.deepEqual(await compsUi('pr-ui-inl-a'), [['k006', 2], ['k005', 1]], 'DB に入った (1 行目が代表)');
  assert.equal((await mapOf('pr-ui-inl-a')).name, 'Amazon の名前 みかん 2 個');
  assert.equal(await btn.textContent(), '登録しました'); assert.equal(await btn.isDisabled(), true);
  assert.equal(await dirty(p), 0, '保存した後 = 離れても確かめない');
  // もう 1 つの行: 名前を空にして NE の商品を選ぶ = 選んだ商品の名前が入る
  await p.locator('button[data-inline][data-sku="pr-ui-inl-b"]').click();
  const box2 = p.locator('li.inl-box[data-inline-for="pr-ui-inl-b"]');
  await box2.locator('.inl-body').waitFor({ state: 'visible' });
  await box2.locator('.inl-name').fill('');
  const c2 = box2.locator('tr.comp-row').nth(0).locator('.c-code');
  await c2.fill('k004');
  await inlSugOpen(p);
  await c2.press('Enter');
  assert.equal(await box2.locator('.inl-name').inputValue(), 'ＡＢＣ 保存袋', '名前が空 = 選んだ NE の商品の名前');
  await box2.locator('.inl-cancel').click();
  assert.equal(await box2.count(), 1, '入れた欄を「やめる」= 確かめる (試験のブラウザは「キャンセル」)');
  // 構成の原価 (1 つの画面・見るだけ): k006 × 2 + k005 × 1 (原価は前の試験で変わることがある = 今日の原価の行を読む)
  const costNow = async (code) => Number((await pg.query(`select y.cost_jpy from core.sku_costs y join core.skus s on s.sku_id = y.sku_id where s.code = $1
    and y.valid_from <= (now() at time zone 'Asia/Tokyo')::date and (y.valid_to is null or y.valid_to >= (now() at time zone 'Asia/Tokyo')::date) order by y.valid_from desc, y.created_at desc, y.sku_cost_id desc limit 1`, [code])).rows[0].cost_jpy);
  const c6 = await costNow('k006'), c5 = await costNow('k005');
  await p.goto(B + '/amazon/sku?sku=pr-ui-inl-a');
  assert.equal(await p.textContent('#cost-total'), `${(c6 * 2 + c5).toLocaleString('ja-JP')} 円`);
  assert.ok((await p.textContent('#comp-cost')).includes(`${(c6 * 2).toLocaleString('ja-JP')} 円`), '小計 = 原価 × 数');
});

await ta('[77] 新商品 (2026-10-17 中原さん): 「セット商品を作るか = 作る」で作るセットの表 (初めの値 = 単品-2・単品の名前 2個セット / 単品の名前に付いていく / 足すと 3 個)・行どうしの重なりと 0 行は ③ で止める・保留で隠して数えない・保存で card.sets を送り product-hub にセットの下書き', async (p) => {
  const sent = [];
  p.on('request', (rq) => { if (/\/api\/new$/.test(rq.url()) && rq.method() === 'POST') sent.push(JSON.parse(rq.postData() || '{}')); });
  await p.goto(B + '/new?kind=single');
  assert.equal(await p.isHidden('#row-set-plans'), true, '作るを選ぶまで出さない');
  await p.fill('#code', 'ui-sp-1');
  await p.waitForSelector('#code-msg.ok');
  await p.fill('#f-name', 'UI ネームタグ 【100枚】');
  await p.fill('#f-standard_price', '1000');
  await p.click('#f-tax_rate button[data-v="0.1"]');
  await p.selectOption('#shipping', 'S01');
  await p.selectOption('#f-primary_supplier', '0001');
  await fillReq(p);
  await p.click('#set-plan button[data-v="create"]');
  assert.equal(await p.isHidden('#row-set-plans'), false);
  const rows = p.locator('#set-plan-rows tr.sp-row');
  assert.equal(await rows.count(), 1, '初めに 1 行');
  assert.deepEqual([await rows.nth(0).locator('.sp-qty').inputValue(), await rows.nth(0).locator('.sp-code').inputValue(), await rows.nth(0).locator('.sp-name').inputValue()],
    ['2', 'ui-sp-1-2', 'UI ネームタグ 【100枚】 2個セット']);
  await p.waitForSelector('#set-plan-rows .sp-msg.ok');
  // 直していない行は単品の名前に付いていく
  await p.fill('#f-name', 'UI ネームタグ 【50枚】');
  assert.equal(await rows.nth(0).locator('.sp-name').inputValue(), 'UI ネームタグ 【50枚】 2個セット');
  // 足す = 次の個数 (3)・コードの重なり (大文字小文字だけ違う) = ③ で止める
  await p.click('#set-plan-add');
  assert.deepEqual([await rows.nth(1).locator('.sp-qty').inputValue(), await rows.nth(1).locator('.sp-code').inputValue()], ['3', 'ui-sp-1-3']);
  await rows.nth(1).locator('.sp-code').fill('UI-SP-1-2');
  assert.match(await rows.nth(1).locator('.sp-msg').textContent(), /2 行あります/);
  assert.match(await p.textContent('#checklist'), /作るセットを直す \(1 行\)/);
  // 直したコードは単品の名前を変えても付いていかない
  await rows.nth(1).locator('.sp-code').fill('ui-sp-1-3x');
  await p.fill('#f-name', 'UI ネームタグ');
  assert.equal(await rows.nth(1).locator('.sp-code').inputValue(), 'ui-sp-1-3x');
  assert.equal(await rows.nth(0).locator('.sp-name').inputValue(), 'UI ネームタグ 2個セット');
  await p.waitForFunction(() => document.querySelectorAll('#set-plan-rows .sp-msg.ok').length === 2);
  await noneRest(p);
  assert.match(await p.textContent('#checklist'), /作るセット \(2 個\)/);
  assert.match(await p.textContent('#remain'), /下書きを保存できます/);
  // 0 行 = 止める (「作る」なら 1 つ以上)
  await rows.nth(1).locator('[data-sp-del]').click();
  await rows.nth(0).locator('[data-sp-del]').click();
  assert.equal(await rows.count(), 0);
  assert.match(await p.textContent('#checklist'), /作るセット \(個数・コード・名前を 1 つ以上\)/);
  assert.equal(await p.isDisabled('#save'), true);
  // 保留 = 隠す・数えない / 作るに戻すと 1 行 (2 個) から
  await p.click('#set-plan button[data-v="hold"]');
  assert.equal(await p.isHidden('#row-set-plans'), true);
  assert.ok(!/作るセット/.test(await p.textContent('#checklist')));
  await p.click('#set-plan button[data-v="create"]');
  assert.equal(await rows.count(), 1);
  await p.click('#set-plan-add');
  await p.waitForFunction(() => document.querySelectorAll('#set-plan-rows .sp-msg.ok').length === 2);
  assert.match(await p.textContent('#remain'), /下書きを保存できます/);
  await Promise.all([p.waitForURL(/\/sku\/ui-sp-1$/), p.click('#save')]);
  const body = sent[sent.length - 1];
  assert.deepEqual(body.card.set_decision.decision, 'create');
  assert.deepEqual(body.card.sets, [{ qty: 2, code: 'ui-sp-1-2', name: 'UI ネームタグ 2個セット' }, { qty: 3, code: 'ui-sp-1-3', name: 'UI ネームタグ 3個セット' }]);
  await p.waitForSelector('#saved-note');
  assert.match(await p.textContent('#saved-note'), /セット商品 ui-sp-1-2 \(2個\)・ui-sp-1-3 \(3個\) も NE に登録してください/);
  const { getDB: phDb } = await import('../apps/product-hub/db.js');
  const parent = phDb().prepare("SELECT id FROM product_drafts WHERE ne_code = 'ui-sp-1'").get();
  assert.ok(parent, 'カードができた');
  assert.deepEqual(phDb().prepare('SELECT ne_code, name, provisional_code, price FROM product_drafts WHERE parent_draft_id = ? ORDER BY id').all(parent.id),
    [{ ne_code: 'ui-sp-1-2', name: 'UI ネームタグ 2個セット', provisional_code: 1, price: 2000 }, { ne_code: 'ui-sp-1-3', name: 'UI ネームタグ 3個セット', provisional_code: 1, price: 3000 }]);
});

await ta('[78] CSV でまとめて登録 (10/17): ファイルを選ぶ → 試し取込 (表・絞る札・だめな理由・何も書かない) → 離れるときに確かめる → 保存 (20 件ずつ・結果の列) → 出し直すと「変わらない」/ Shift_JIS の CSV / スマホ 390 で横にはみ出さない', async (p) => {
  const iconv = (await import('iconv-lite')).default;
  await p.goto(B + '/amazon/');
  await Promise.all([p.waitForURL(/\/amazon\/bulk$/), p.click('a[href="bulk"]')]);
  assert.equal(await p.isDisabled('#bulk-preview'), true, 'ファイルを選ぶまで押せない');
  const NL = String.fromCharCode(13, 10);
  const csv = ['seller_sku,name,ne_code,qty', 'pr-ui-bulk-1,一括 1,k001,2', 'pr-ui-bulk-1,一括 1,k002,1', 'pr-ui-bulk-2,,k004,1', 'pr-ui-bulk-3,=無いコード,nope-x,1'].join(NL) + NL;
  await p.setInputFiles('#bulk-file', { name: '一括の試験.csv', mimeType: 'text/csv', buffer: iconv.encode(csv, 'cp932') });
  await p.click('#bulk-preview');
  await p.locator('#bulk-result').waitFor({ state: 'visible' });
  assert.match(await p.textContent('#bulk-file-info'), /一括の試験\.csv · Shift_JIS/);
  assert.equal(await p.locator('#bulk-rows tr').count(), 3);
  assert.match(await p.textContent('#bulk-rows tr[data-sku="pr-ui-bulk-3"]'), /だめ[\s\S]*nope-x は Company DB にありません/);
  assert.match(await p.textContent('#bulk-rows tr[data-sku="pr-ui-bulk-2"]'), /ＡＢＣ 保存袋/, '名前が空 = 代表の NE の商品名');
  assert.equal(await p.textContent('#bulk-apply'), '保存する (2 件)');
  assert.equal(await dirty(p), 1, '試し取込の後に保存していない = 離れるときに確かめる');
  assert.equal(await mapOf('pr-ui-bulk-1'), undefined, '試し取込は何も書かない');
  await p.click('#bulk-filter button[data-f="error"]');
  assert.equal(await p.locator('#bulk-rows tr').count(), 1, 'だめだけに絞る');
  await p.click('#bulk-filter button[data-f="all"]');
  // スマホ 390: 横にはみ出さない (表は表の箱の中で横に送る)
  await p.setViewportSize({ width: 390, height: 844 });
  const [sw, cw] = await p.evaluate(() => [document.documentElement.scrollWidth, document.documentElement.clientWidth]);
  assert.ok(sw <= cw + 1, `390 で横にはみ出す (${sw} > ${cw})`);
  await p.setViewportSize({ width: 1440, height: 900 });
  await p.click('#bulk-apply');
  await p.waitForFunction(() => /終わりました/.test(document.getElementById('bulk-apply-msg').textContent));
  assert.match(await p.textContent('#bulk-apply-msg'), /できた 2 件 · だめ 0 件/);
  assert.match(await p.textContent('#bulk-rows tr[data-sku="pr-ui-bulk-1"] .bulk-res'), /作りました \(出品も\)/);
  assert.deepEqual(await compsUi('pr-ui-bulk-1'), [['k001', 2], ['k002', 1]], 'DB に入った (1 行目が代表)');
  assert.equal((await mapOf('pr-ui-bulk-2')).name, 'ＡＢＣ 保存袋');
  assert.equal(await dirty(p), 0, '保存した後 = 確かめない');
  assert.equal(await p.isDisabled('#bulk-apply'), true);
  // 結果の CSV (画面が手元で作る・BOM・式の注入の対策 = list-csv.mjs の csvCell と同じ答え)
  const [dl] = await Promise.all([p.waitForEvent('download'), p.click('#bulk-report')]);
  assert.match(dl.suggestedFilename(), /^amazon-bulk-result_[0-9]{8}-[0-9]{4}[.]csv$/);
  const rep = fs.readFileSync(await dl.path());
  assert.deepEqual([...rep.subarray(0, 3)], [0xef, 0xbb, 0xbf]);
  const repText = rep.toString('utf8');
  assert.ok(repText.includes(`"pr-ui-bulk-3","5","だめ","'=無いコード"`), repText);
  assert.ok(repText.includes('"pr-ui-bulk-1","2","作る (出品も)","一括 1","k001×2, k002×1","作った"'), '保存の結果も入る');
  const { csvCell } = await import('../apps/master-edit/list-csv.mjs');
  for (const v of ['=a', ' +b', '	-c', '@d', 'ok', 'x"y', String.fromCharCode(0xfeff) + '=e', String.fromCharCode(0x200b) + '-f']) assert.equal(await p.evaluate((x) => window.MasterEdit.bulkCsvCell(x), v), csvCell(v), JSON.stringify(v));
  // 同じ CSV をもう一度 = 「変わらない」(保存する SKU が無い)
  await p.setInputFiles('#bulk-file', { name: '一括の試験.csv', mimeType: 'text/csv', buffer: Buffer.from(csv, 'utf8') });
  await p.click('#bulk-preview');
  await p.waitForFunction(() => /試し取込をしました/.test(document.getElementById('bulk-msg').textContent) && /UTF-8/.test(document.getElementById('bulk-file-info').textContent));
  assert.match(await p.textContent('#bulk-rows tr[data-sku="pr-ui-bulk-1"]'), /変わらない/);
  assert.equal(await p.textContent('#bulk-apply'), '保存する SKU がありません');
});

await ta('[79] ⑤-1 商品の画面の先方品番・入数 (CompanyDB構想 27 §2.4): 足す → 保存 (発注アプリの対応表に入る)・打った行は未保存に数える・直して保存・古い版 = 409 の文・消す (確かめ) = 空の「足す」行に戻る・ほかの仕入先の行は消すだけ', async (p) => {
  const NSK = await import('../apps/purchase-orders/new-skus.js');
  const t0 = new Date().toISOString();
  mirrorDb.prepare('insert or ignore into po_suppliers (supplier_code, name, created_at, updated_at) values (?,?,?,?)').run('1', 'AMC', t0, t0);
  mirrorDb.prepare('insert or ignore into po_suppliers (supplier_code, name, created_at, updated_at) values (?,?,?,?)').run('7', 'ほかの仕入先', t0, t0);
  mirrorDb.prepare("insert into po_vendor_code_map (supplier_code, product_key, product_code, vendor_code, qty_per_unit, updated_at) values ('7', 'k001', 'k001', 'OLD-7', null, ?)").run(t0);
  const vrow = (sup) => mirrorDb.prepare('select * from po_vendor_code_map where supplier_code = ? and product_key = ?').get(sup, 'k001') || null;
  NSK.setNewSkuReaderForTest(async () => ({ ok: true, map: new Map([['k001', { code: 'k001', name: 'k001', state: 'available', supplier: '1' }]]) }));
  try {
    p.on('dialog', (d) => d.accept());
    await p.goto(B + '/sku/k001');
    const r1 = '#po-vendor .po-vrow[data-sup="1"]';
    await p.waitForSelector(r1);
    assert.equal(await p.textContent(`${r1} .po-v-save`), '足す');
    assert.equal(await p.isDisabled(`${r1} .po-v-save`), true, '何も打っていないのに押せる');
    assert.equal(await p.isDisabled('#po-vendor .po-vrow[data-sup="7"] .po-v-code'), true, 'ほかの仕入先の行を直せる');
    const d0 = await dirty(p);
    await p.fill(`${r1} .po-v-code`, 'AC-K1');
    await p.fill(`${r1} .po-v-qpu`, '２４');
    assert.equal(await dirty(p), d0 + 1, '打った先方品番の行を未保存に数えない');
    // 送っている間は欄も閉じる (送っていない値を「保存した」にしない・Codex R1 M1)
    let release; const gate = new Promise((r) => { release = r; });
    await p.route('**/vendor-code', async (route) => { await gate; await route.continue(); });
    await p.click(`${r1} .po-v-save`);
    await p.waitForFunction((s) => document.querySelector(s + ' .po-v-code').disabled && document.querySelector(s + ' .po-v-qpu').disabled, r1);
    release();
    await p.waitForFunction((s) => /保存しました/.test(document.querySelector(s + ' .po-v-msg').textContent), r1);
    await p.unroute('**/vendor-code');
    assert.equal(await p.isDisabled(`${r1} .po-v-code`), false, '保存の後に欄が開かない');
    assert.deepEqual([vrow('1').vendor_code, vrow('1').qty_per_unit], ['AC-K1', 24]);
    assert.equal(await p.inputValue(`${r1} .po-v-qpu`), '24');
    assert.equal(await dirty(p), d0, '保存した後も未保存に数えている');
    assert.equal(await p.textContent(`${r1} .po-v-save`), '保存');
    // 画面の外で直す → この画面で保存 = 409 の文 (何も書かない)
    const before = vrow('1');
    mirrorDb.prepare("update po_vendor_code_map set vendor_code = 'BEHIND', updated_at = ? where supplier_code = '1' and product_key = 'k001'").run(new Date(Date.now() + 5000).toISOString());
    await p.fill(`${r1} .po-v-code`, 'AC-K2');
    await p.press(`${r1} .po-v-code`, 'Enter');
    await p.waitForFunction((s) => /他の画面で変更されています/.test(document.querySelector(s + ' .po-v-msg').textContent), r1);
    assert.equal(vrow('1').vendor_code, 'BEHIND');
    mirrorDb.prepare("update po_vendor_code_map set vendor_code = ?, updated_at = ? where supplier_code = '1' and product_key = 'k001'").run(before.vendor_code, before.updated_at);
    await p.fill(`${r1} .po-v-code`, 'AC-K2');
    await p.click(`${r1} .po-v-save`);
    await p.waitForFunction((s) => /保存しました/.test(document.querySelector(s + ' .po-v-msg').textContent), r1);
    assert.equal(vrow('1').vendor_code, 'AC-K2');
    // 消す (確かめ) = 空の「足す」行に戻る
    await p.click(`${r1} .po-v-del`);
    await p.waitForFunction((s) => /消しました/.test(document.querySelector(s + ' .po-v-msg').textContent), r1);
    assert.equal(vrow('1'), null);
    assert.deepEqual([await p.inputValue(`${r1} .po-v-code`), await p.textContent(`${r1} .po-v-save`), await p.$(`${r1} .po-v-del`)], ['', '足す', null]);
    assert.equal(await p.isDisabled(`${r1} .po-v-code`), false, '消した後に「足す」の欄が開いている (統合の Codex P2)');
    // ほかの仕入先の古い行 = 消すだけ (行ごと消える)
    await p.click('#po-vendor .po-vrow[data-sup="7"] .po-v-del');
    await p.waitForFunction(() => !document.querySelector('#po-vendor .po-vrow[data-sup="7"]'));
    assert.equal(vrow('7'), null);
    // 1024 幅 / 390 幅で横にはみ出さない
    for (const w of [1024, 390]) {
      await p.setViewportSize({ width: w, height: 800 });
      assert.ok(await p.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth + 1), `${w} 幅で横にはみ出す`);
    }
  } finally { NSK.setNewSkuReaderForTest(null); }
});

// ── 🆕 10/19 中原さん「グラフとか入出庫履歴とか行くときにスクロールしないといけないからボタンかタブにしてほしい」= 商品の画面のタブ (public/me-tabs.js)
const tabState = (p) => p.evaluate(() => ({
  hash: location.hash,
  sel: [...document.querySelectorAll('#sku-tabs [role="tab"]')].filter((t) => t.getAttribute('aria-selected') === 'true').map((t) => t.dataset.tab),
  shown: [...document.querySelectorAll('[role="tabpanel"]')].filter((x) => !x.hidden).map((x) => x.id),
}));
/** 履歴・月別・グラフの API を読んだ回数 (開いたタブだけ読む) */
function countReads(p) {
  const n = { moves: 0, monthly: 0, sales: 0 };
  p.on('request', (r) => {
    const u = new URL(r.url()).pathname;
    if (/\/api\/sku\/[^/]+\/movements$/.test(u)) n.moves++;
    if (/\/api\/sku\/[^/]+\/monthly$/.test(u)) n.monthly++;
    if (/\/api\/sku\/[^/]+\/sales-chart$/.test(u)) n.sales++;
  });
  return n;
}

await ta('[80] 商品の画面のタブ (10/19): 基本情報 / 販売数のグラフ / 入出庫の履歴・role と aria・開いたタブだけ読む (1 回だけ)・#tab=… に残る (読み込み直す・戻る・# を打つ)・← → Home End・タブを替えても打った値と未保存はそのまま・Ctrl+S は基本情報に戻して保存', async (p) => {
  const n = countReads(p);
  await p.goto(B + '/sku/s002');
  await p.waitForLoadState('networkidle');
  // 帯と板の印 (tablist・tab・tabpanel・aria-selected・aria-controls ⇔ aria-labelledby)
  const a11y = await p.evaluate(() => [...document.querySelectorAll('#sku-tabs [role="tab"]')].map((t) => {
    const tp = document.getElementById(t.getAttribute('aria-controls'));
    const name = [...t.childNodes].filter((x) => !(x.classList && x.classList.contains('tab-dirty'))).map((x) => x.textContent).join('').trim();   // 未保存の印 (隠れている) は除く
    return [t.dataset.tab, name, t.getAttribute('aria-selected'), t.tabIndex, tp && tp.getAttribute('role'), tp && tp.getAttribute('aria-labelledby') === t.id];
  }));
  assert.equal(await p.getAttribute('#sku-tabs', 'role'), 'tablist');
  assert.deepEqual(a11y, [['basic', '基本情報', 'true', 0, 'tabpanel', true], ['sales', '販売数のグラフ', 'false', -1, 'tabpanel', true], ['moves', '入出庫の履歴・月別', 'false', -1, 'tabpanel', true]]);
  assert.deepEqual(await tabState(p), { hash: '', sel: ['basic'], shown: ['tp-basic'] });
  assert.deepEqual(n, { moves: 0, monthly: 0, sales: 0 }, '開いたとき = 隠れたタブ (グラフ・履歴) は読まない');
  assert.equal(await p.isVisible('#f'), true);
  // 打った値・未保存 → グラフのタブ (印は基本情報のタブにも)
  const before = (await row('s002')).months;
  await p.fill('#f-reorder_months', String(before + 2));
  assert.equal(await dirty(p), 1);
  assert.equal(await p.isVisible('#tab-basic-dirty'), true, '基本情報のタブに未保存の印');
  await p.click('#tab-sales');
  assert.deepEqual(await tabState(p), { hash: '#tab=sales', sel: ['sales'], shown: ['tp-sales'] });
  await p.waitForFunction(() => document.getElementById('sc-sum').textContent !== '読んでいます…');
  assert.deepEqual(n, { moves: 0, monthly: 0, sales: 1 }, 'グラフのタブを開いた = グラフだけ読む');
  assert.equal(await p.isHidden('#f'), true);
  assert.equal(await p.isVisible('#unsaved'), true, '上の帯の「未保存 N 件」は残る');
  // キー: → = 入出庫の履歴 (フォーカスも)・→ = 先頭に回る・End / Home
  await p.focus('#tab-sales');
  await p.keyboard.press('ArrowRight');
  assert.deepEqual(await tabState(p), { hash: '#tab=moves', sel: ['moves'], shown: ['tp-moves'] });
  assert.equal(await active(p), 'tab-moves');
  await p.waitForFunction(() => document.getElementById('mv-head').textContent !== '読んでいます…' && document.getElementById('mo-head').textContent !== '読んでいます…');
  assert.deepEqual(n, { moves: 1, monthly: 1, sales: 1 }, '履歴のタブ = 履歴と月別を 1 回ずつ');
  await p.keyboard.press('ArrowRight');
  assert.deepEqual(await tabState(p), { hash: '', sel: ['basic'], shown: ['tp-basic'] }, '最後の → = 先頭 (基本情報 = # なし)');
  assert.equal(await active(p), 'tab-basic');
  await p.keyboard.press('ArrowLeft');
  assert.deepEqual((await tabState(p)).sel, ['moves'], '先頭の ← = 最後');
  await p.keyboard.press('Home');
  assert.deepEqual((await tabState(p)).sel, ['basic']);
  await p.keyboard.press('End');
  assert.deepEqual((await tabState(p)).sel, ['moves']);
  // 打った値・未保存の印はそのまま (隠しただけ)。行き来しても読み直さない
  await p.click('#tab-basic');
  assert.equal(await p.inputValue('#f-reorder_months'), String(before + 2));
  assert.equal(await dirty(p), 1);
  assert.equal(await p.locator('[data-row="reorder_months"].dirty').count(), 1, '欄の黄色い印も残る');
  // 未保存のままアドレスの # を打つ (同じ画面の中の移り = 新しい履歴) = 離れるときの確認は出さない・そのタブ (Codex R1 High)
  await p.evaluate(() => { location.hash = '#tab=moves'; });
  await p.waitForTimeout(150);
  assert.deepEqual((await tabState(p)).sel, ['moves']);
  assert.equal(await p.locator('#leave-bg.on').count(), 0, '# を打っただけ = 聞かない');
  assert.equal(await dirty(p), 1);
  // ブラウザの戻る: 見張りの履歴へ戻っただけ = 聞かない (タブは戻る) → もう 1 回 = この画面の前へ = 今までどおり聞く
  await p.goBack(); await p.waitForTimeout(150);
  assert.equal(await p.locator('#leave-bg.on').count(), 0, '見張りの履歴へ戻っただけ = 聞かない');
  assert.deepEqual((await tabState(p)).sel, ['basic']);
  await p.goBack(); await p.waitForTimeout(200);
  assert.equal(await p.locator('#leave-bg.on').count(), 1, 'この画面の前へ戻る = 聞く');
  await p.click('#leave-stay');
  assert.match(p.url(), /sku\/s002/);
  assert.equal(await dirty(p), 1, 'ここに残る = 打った値はそのまま');
  await p.click('#tab-sales'); await p.click('#tab-moves'); await p.click('#tab-basic');
  await p.waitForTimeout(200);
  assert.deepEqual(n, { moves: 1, monthly: 1, sales: 1 }, 'もう一度開いても読み直さない');
  // 履歴のタブで Ctrl+S = 基本情報に戻して保存 (DB に入る)・読み直した後も基本情報
  await p.click('#tab-moves');
  await Promise.all([p.waitForNavigation(), p.keyboard.press('Control+s')]);
  await p.waitForSelector('#saved-note');
  assert.equal((await row('s002')).months, before + 2, '保存した');
  assert.deepEqual(await tabState(p), { hash: '', sel: ['basic'], shown: ['tp-basic'] });
  // # を打つ (同じ画面の #tab リンク) = そのタブ / 読み込み直す = 同じタブ (開いたらすぐ読む・グラフは読まない)
  await p.goto(B + '/sku/s002#tab=moves');
  assert.deepEqual(await tabState(p), { hash: '#tab=moves', sel: ['moves'], shown: ['tp-moves'] }, '# だけ変えた (hashchange)');
  const n2 = countReads(p);
  await p.reload();
  await p.waitForLoadState('networkidle');
  assert.deepEqual(await tabState(p), { hash: '#tab=moves', sel: ['moves'], shown: ['tp-moves'] }, '読み込み直しても同じタブ');
  assert.deepEqual(n2, { moves: 1, monthly: 1, sales: 0 }, '開いているタブだけ読む');
  // ほかの画面へ行って戻る = 同じタブ
  await Promise.all([p.waitForNavigation(), p.click('.crumb a')]);
  await p.goBack();
  await p.waitForSelector('#sku-tabs');
  assert.deepEqual((await tabState(p)).sel, ['moves'], '戻るでも同じタブ');
  // 知らない名前 = 基本情報 (# を外す)
  await p.goto(B + '/sku/s001#tab=zzz');
  assert.deepEqual(await tabState(p), { hash: '', sel: ['basic'], shown: ['tp-basic'] });
});

await ta('[81] タブと前へ・次へ・セット・狭い画面 (10/19): 前へ・次へにも開いているタブを付ける (替えたら書き直す)・セットの履歴のタブから構成品の履歴のタブへ・帯は上に残り、保存の箱と開いた板の上端は帯の下・基本情報に戻るとスクロールの位置も戻る・スマホ 390 = 帯は 44px で横にはみ出さない', async (p) => {
  await p.goto(B + '/?q=' + encodeURIComponent('はちみつ'));
  const codes = await listCodes(p);
  await Promise.all([p.waitForNavigation(), p.click(`a.rowlink:text-is("${codes[1]}")`)]);
  await pnState(p);
  await p.click('#tab-sales');
  assert.match(await p.getAttribute('#pn-next', 'href'), new RegExp(`/sku/${codes[2]}#tab=sales$`));
  await Promise.all([p.waitForNavigation(), p.click('#pn-next')]);
  assert.deepEqual(await tabState(p), { hash: '#tab=sales', sel: ['sales'], shown: ['tp-sales'] }, '次の商品も同じタブ');
  await pnState(p);
  assert.match(await p.getAttribute('#pn-prev', 'href'), new RegExp(`/sku/${codes[1]}#tab=sales$`), '描いた時から付いている');
  await p.click('#tab-basic');
  assert.match(await p.getAttribute('#pn-prev', 'href'), new RegExp(`/sku/${codes[1]}$`), '基本情報 = # なし');
  // セット: 履歴のタブ = 構成品へのリンク (構成品の画面も履歴のタブ)
  await p.goto(B + '/sku/set001#tab=moves');
  assert.deepEqual((await tabState(p)).sel, ['moves']);
  assert.equal((await p.textContent('#tab-moves')).trim(), '入出庫の履歴', 'セットは月別なし');
  await Promise.all([p.waitForNavigation(), p.click('#mv-set a:text-is("s001")')]);
  assert.deepEqual((await tabState(p)).sel, ['moves']);
  // 1440: 下へスクロール = 帯は上の帯 (58px) の下に残る・右の保存の箱は帯に隠れない → グラフ = 板の上端は帯の下 → 基本情報 = 元の位置
  await p.goto(B + '/sku/s003');
  await p.evaluate(() => window.scrollTo(0, 900));
  await p.waitForTimeout(200);
  const g = await p.evaluate(() => ({ field: document.getElementById('f-reorder_months').getBoundingClientRect().top, bar: document.getElementById('sku-top').getBoundingClientRect().toJSON(), side: document.querySelector('#tp-basic .side').getBoundingClientRect().top }));
  assert.ok(Math.abs(g.bar.top - 58) < 1, `帯が上に残る ${g.bar.top}`);
  assert.ok(g.side >= g.bar.bottom - 1, `保存の箱が帯に隠れない ${g.side} < ${g.bar.bottom}`);
  await p.click('#tab-sales');
  const s = await p.evaluate(() => ({ panel: document.getElementById('tp-sales').getBoundingClientRect().top, bar: document.getElementById('sku-top').getBoundingClientRect().bottom }));
  assert.ok(s.panel >= s.bar - 1 && s.panel < s.bar + 40, `グラフの板の上端が帯のすぐ下 ${JSON.stringify(s)}`);
  await p.click('#tab-basic');
  await p.waitForTimeout(150);   // 小見出しが出る・消える (帯の高さ) の後の合わせ直しまで
  const back = await p.evaluate(() => document.getElementById('f-reorder_months').getBoundingClientRect().top);
  assert.ok(Math.abs(back - g.field) <= 2, `基本情報に戻ると同じ欄が同じ所に見える ${back} ≠ ${g.field}`);
  if (process.env.MASTER_TABS_SHOT) await p.screenshot({ path: process.env.MASTER_TABS_SHOT });
  // スマホ 390: 帯の中だけで横に送る (ページははみ出さない)・押す所は 44px
  await p.setViewportSize({ width: 390, height: 844 });
  await p.goto(B + '/sku/s003#tab=moves');
  const m = await p.evaluate(() => ({ doc: document.documentElement.scrollWidth, view: innerWidth,
    tabs: [...document.querySelectorAll('#sku-tabs [role="tab"]')].map((t) => Math.round(t.getBoundingClientRect().height)),
    bar: document.getElementById('sku-tabs').getBoundingClientRect().right }));
  assert.ok(m.doc <= m.view, `横にはみ出す ${m.doc} > ${m.view}`);
  assert.ok(m.bar <= m.view + 0.5, `帯がはみ出す ${m.bar}`);
  assert.ok(m.tabs.every((h) => h >= 44), `タブの高さ ${m.tabs}`);
  await p.click('#tab-sales');
  assert.deepEqual((await tabState(p)).sel, ['sales']);
  if (process.env.MASTER_TABS_SHOT_SP) await p.screenshot({ path: process.env.MASTER_TABS_SHOT_SP });
});

await ta('[82] タブと戻るの見張り (10/19・Codex R2 Medium): 未保存 → アドレスに #tab=… を打つ → Ctrl+S で保存 (読み直し) → また直す → ブラウザの戻る = 今までどおり確認が出る', async (p) => {
  await p.goto(B + '/');
  await Promise.all([p.waitForNavigation(), p.click('a.rowlink:has-text("s003")')]);
  const before = (await row('s003')).months;
  await p.fill('#f-reorder_months', String(before + 1));
  await p.evaluate(() => { location.hash = '#tab=sales'; });
  await p.waitForTimeout(150);
  assert.equal(await p.locator('#leave-bg.on').count(), 0, '# を打っただけ = 聞かない');
  await Promise.all([p.waitForNavigation(), p.keyboard.press('Control+s')]);
  await p.waitForSelector('#saved-note');
  assert.equal((await row('s003')).months, before + 1, '保存した');
  await p.fill('#f-reorder_months', String(before + 2));
  assert.equal(await dirty(p), 1);
  await p.goBack(); await p.waitForTimeout(200);
  assert.equal(await p.locator('#leave-bg.on').count(), 1, '保存の後にまた直して戻る = 確認が出る');
  await p.click('#leave-stay');
  assert.equal(await dirty(p), 1, 'ここに残る = 打った値はそのまま');
  assert.equal(await p.inputValue('#f-reorder_months'), String(before + 2));
});

// ── 🆕 10/20 中原さん「商品マスタからそれぞれのモールに飛ぶリンク」= 見出しの下のモールの札 (apps/master-edit/mall-links.mjs) ──
{
  const sidOf = async (code) => (await pg.query('select sku_id from core.skus where code = $1', [code])).rows[0].sku_id;
  const k4 = await sidOf('k004'), k5 = await sidOf('k005');
  const lst = async (mall, code, comps, { itemId = null, asin = null } = {}) => {
    const id = (await pg.query(`insert into core.listings (company_id, mall, shop_code, listing_code, mall_item_id, status) values (1, $1, $2, $3, $4, 'active') returning listing_id`,
      [mall, mall === 'amazon' ? 'main@A1VC38T7YXB528' : 'main', code, itemId])).rows[0].listing_id;
    let i = 0;
    for (const [skuId, qty] of comps) await pg.query(`insert into core.listing_components (company_id, listing_id, sku_id, qty, sort_order, resolution, resolved_by_type) values (1, $1, $2, $3, $4, 'exact', 'system')`, [id, skuId, qty, i++]);
    if (asin) {
      const ci = (await pg.query(`insert into core.catalog_items (marketplace_id, asin) values ('A1VC38T7YXB528', $1) returning catalog_item_id`, [asin])).rows[0].catalog_item_id;
      await pg.query('update core.listings set catalog_item_id = $1 where listing_id = $2', [ci, id]);
    }
  };
  await lst('rakuten', 'k004', [[k4, 1]], { itemId: 'k004' });
  await lst('amazon', 'ml-k004', [[k4, 1]], { asin: 'B0MLK00401' });
  await lst('amazon', 'ml-k004-2p', [[k4, 2]], { asin: 'B0MLK00402' });
  await lst('amazon', 'ml-k4k5', [[k4, 1], [k5, 1]], { asin: 'B0MLK00499' });   // ほかの商品と組んだ出品 = どちらの画面にも出さない
  await lst('yahoo', 'k004', [[k4, 1]]);
  await lst('qoo10', '1067124910', [[k4, 1]], { itemId: '1067124910' });
}
const mallState = (p) => p.evaluate(() => {
  const box = document.getElementById('sku-malls');
  const r = box && box.getBoundingClientRect();
  return {
    shown: !!box && r.width > 0 && r.height > 0 && !box.closest('[hidden]'),
    inHead: !!box && !!box.closest('#sku-ph'),
    links: box ? [...box.querySelectorAll('a.mall')].map((a) => [a.dataset.mall, a.textContent.replace('(新しいタブで開く)', '').trim(), a.getAttribute('href'), a.target, a.rel]) : [],
    none: box ? (box.querySelector('.malls-none') || {}).textContent || '' : null,
  };
});
await ta('[83] モールの商品ページへのリンク (10/20): 見出しの下に楽天・Amazon・Yahoo!・Qoo10 の札・新しいタブ (noopener)・まとめ売りは ×N・組んだ出品は出さない・どのタブでも見える・出品が無い = 一言・スマホ 390 で 44px・はみ出さない', async (p, ctx) => {
  await p.goto(B + '/sku/k004');
  const s = await mallState(p);
  assert.equal(s.shown, true, 'モールの行が見える');
  assert.equal(s.inHead, true, '見出しの中 (どのタブでも同じ所)');
  const NR = 'noopener noreferrer';
  assert.deepEqual(s.links, [
    ['rakuten', '楽天', 'https://item.rakuten.co.jp/b-faith/k004/', '_blank', NR],
    ['amazon', 'AmazonB0MLK00401', 'https://www.amazon.co.jp/dp/B0MLK00401', '_blank', NR],
    ['amazon', 'Amazon×2', 'https://www.amazon.co.jp/dp/B0MLK00402', '_blank', NR],
    ['yahoo', 'Yahoo!', 'https://store.shopping.yahoo.co.jp/b-faith01/k004.html', '_blank', NR],
    ['qoo10', 'Qoo10', 'https://www.qoo10.jp/g/1067124910', '_blank', NR],
  ]);
  assert.match(await p.getAttribute('#sku-malls a[data-mall="amazon"] >> nth=1', 'title'), /2 個のまとめ売り/);
  if (process.env.MASTER_MALLS_SHOT_PC) await p.screenshot({ path: process.env.MASTER_MALLS_SHOT_PC });
  // どのタブでも見える (見出しはタブの外)
  for (const tab of ['#tab-sales', '#tab-moves', '#tab-basic']) {
    await p.click(tab);
    assert.equal((await mallState(p)).shown, true, `${tab} でも見える`);
  }
  // 押すと新しいタブで開く (この画面はそのまま・未保存の確かめも出ない)
  await ctx.route(/^https:\/\/item\.rakuten\.co\.jp\//, (route) => route.fulfill({ status: 200, contentType: 'text/html', body: '<p>rakuten</p>' }));
  const [popup] = await Promise.all([ctx.waitForEvent('page'), p.click('#sku-malls a[data-mall="rakuten"]')]);
  await popup.waitForLoadState();
  assert.equal(popup.url(), 'https://item.rakuten.co.jp/b-faith/k004/');
  assert.equal(await popup.evaluate(() => window.opener), null, 'noopener = 開いた先から元の画面を触れない');
  await popup.close();
  assert.equal(new URL(p.url()).pathname, '/apps/master-edit/sku/k004');
  assert.equal(await p.locator('#leave-bg.on').count(), 0);
  // 組んだ出品しか無い商品・出品が無い商品 = 一言だけ (札は出さない)
  for (const code of ['k005', 's003']) {
    await p.goto(B + '/sku/' + code);
    const n = await mallState(p);
    assert.deepEqual([n.shown, n.links, n.none], [true, [], '出品が見つかりません'], code);
  }
  // スマホ 390: 札は 44px・折り返してはみ出さない
  await p.setViewportSize({ width: 390, height: 844 });
  await p.goto(B + '/sku/k004');
  const m = await p.evaluate(() => ({ doc: document.documentElement.scrollWidth, view: innerWidth,
    chips: [...document.querySelectorAll('#sku-malls a.mall')].map((a) => { const r = a.getBoundingClientRect(); return [Math.round(r.height), Math.round(r.right)]; }) }));
  assert.ok(m.doc <= m.view, `横にはみ出す ${m.doc} > ${m.view}`);
  assert.equal(m.chips.length, 5);
  assert.ok(m.chips.every(([h, right]) => h >= 44 && right <= m.view), `札 ${JSON.stringify(m.chips)}`);
  if (process.env.MASTER_MALLS_SHOT) await p.screenshot({ path: process.env.MASTER_MALLS_SHOT });
});

await browser.close();
server.close();
try { mirrorDb.close(); fs.rmSync(DATA_TMP, { recursive: true, force: true }); } catch { /* 消せなくてもよい */ }
console.log(`\n${passed} 件 ok`);
if (process.exitCode) console.error('NG があります');
