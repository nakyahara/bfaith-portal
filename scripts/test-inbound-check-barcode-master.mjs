/**
 * 🔢 バーコードマスタ (共有ドライブの バーコードマスタ.csv) の取込と、
 *    それを使ったバーコード解決・検索・仕入先名の表示を確かめる。
 *
 * 中原さん 2026-09-06 の3点:
 *   ① 仕入先コードは入っていたが**仕入先名が入っていなかった** (コードの持ち方が2系統ある)
 *   ② 粟国の塩のバーコードが無かった → バーコードマスタ.csv を取り込む
 *   ③ 検索にカメラ読み取り (読んだ値で検索できること = サーバー側は完全一致で引けること)
 *
 * 実行: DATA_DIR=$(mktemp -d) node scripts/test-inbound-check-barcode-master.mjs
 */
import fs from 'fs';
import os from 'os';
import path from 'path';
import http from 'http';

if (!process.env.DATA_DIR) process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'ic-bcm-'));
const { initMirrorDB, getMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const mirror = getMirrorDB();

let pass = 0, fail = 0;
const ok = (c, l) => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}`); } };
const eq = (a, b, l) => ok(JSON.stringify(a) === JSON.stringify(b), `${l} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);
const now = new Date().toISOString();

const dbMod = await import('../apps/inbound-check/db.js');
const { createTables, getDB, resolveBarcode, setProductBarcode, searchProducts, listProductSuppliers, getProductForPrint } = dbMod;
createTables();
const db = getDB();

// ─── 種まき: 商品マスタ・仕入先 (2系統)・在庫 ───
mirror.exec(`
  CREATE TABLE IF NOT EXISTS po_suppliers (supplier_code TEXT PRIMARY KEY, name TEXT NOT NULL, order_memo TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
  CREATE TABLE IF NOT EXISTS supplier_share_master (仕入先コード TEXT PRIMARY KEY, 表示名 TEXT NOT NULL, memo TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
`);
// 🚨 発注管理は**正規形** (先頭ゼロを外す) で持つ。商品マスタ側はゼロ埋めのことがある
mirror.prepare('INSERT INTO po_suppliers VALUES (?,?,?,?,?)').run('7', 'エーエムシー', null, now, now);
mirror.prepare('INSERT INTO po_suppliers VALUES (?,?,?,?,?)').run('AWA', '粟国の塩', null, now, now);
mirror.prepare('INSERT INTO supplier_share_master VALUES (?,?,?,?,?)').run('0099', 'サロンジェ', null, now, now);
// 両方にある仕入先: 売れ筋共有 (NE と同じ体系) が勝つ
mirror.prepare('INSERT INTO po_suppliers VALUES (?,?,?,?,?)').run('0099', '発注管理の名前 (別体系かもしれない)', null, now, now);
const insP = mirror.prepare(`INSERT INTO mirror_products (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, 仕入先コード, updated_at)
  VALUES (?, ?, ?, '単品', '取扱中', 'unknown', ?, ?)`);
insP.run(1, 'awa-shio-250', '粟国の塩 250g_その他', 'AWA', now);
insP.run(2, 'pashima-single-KI', 'パシーマ キルトケット【シングル】_K-60', '0007', now);
insP.run(3, '01520-01', 'サロンジェ 子供エプロン【110cm】_白ビ袋', '0099', now);
insP.run(4, 'nosup-01', '仕入先なしの商品_その他', null, now);

console.log('[1] ① 仕入先名 — コードの持ち方が2系統あっても名前を出す');
{
  const sups = listProductSuppliers({});
  const byCode = Object.fromEntries(sups.map(s => [s.code, s.name]));
  eq(byCode['0007'], 'エーエムシー', 'ゼロ埋めコード 0007 → 発注管理の正規形 7 と突き合わせて名前が出る');
  eq(byCode['AWA'], '粟国の塩', '数字でないコードはそのまま突き合わせる');
  eq(byCode['0099'], 'サロンジェ', '売れ筋共有の表示名 (mirror_products と同じ体系) を優先する');
  ok(sups.length === 3 && !sups.some(s => s.code == null), '仕入先コードが無い商品は一覧に出ない');
  ok(sups[0].name && sups[sups.length - 1].name, '名前が分かるものを先に並べる');
  const rows = searchProducts({ supplier: '0007' }).rows;
  ok(rows.length === 1 && rows[0].supplier_name === 'エーエムシー', '検索結果の行にも仕入先名が付く');
  eq(searchProducts({ q: '仕入先なし' }).rows[0].supplier_name, null, '仕入先が無い商品は null (名前をでっち上げない)');
  // 正規形が同じコードに別の名前が2つぶら下がっていたら、どちらか分からないので名前を出さない
  mirror.prepare('INSERT INTO po_suppliers VALUES (?,?,?,?,?)').run('0012', 'A社', null, now, now);
  mirror.prepare('INSERT INTO po_suppliers VALUES (?,?,?,?,?)').run('12', 'B社', null, now, now);
  insP.run(5, 'amb-01', 'あいまいな仕入先の商品_その他', '00012', now);
  eq(searchProducts({ q: 'あいまい' }).rows[0].supplier_name, null, '正規形が衝突したら名前を出さない (別の会社の名前を出さない)');
  insP.run(6, 'exact-01', '生コードが一致する商品_その他', '0012', now);
  eq(searchProducts({ q: '生コード' }).rows[0].supplier_name, 'A社', '生コードが完全一致すればそれを使う');
}

console.log('\n[2] ② バーコードマスタの取込 (CSV の検証)');
const bcm = await import('../apps/inbound-check/barcode-master.js');
const { parseBarcodeMasterCsv, importBarcodeMaster, barcodeMasterStatus } = bcm;
const iconv = (await import('iconv-lite')).default;
const q = v => `"${String(v == null ? '' : v).replace(/"/g, '""')}"`;
const csvOf = (rows, header = ['商品ID', '商品名', '検索名称', 'バーコード', '有効区分']) =>
  iconv.encode([header.map(q).join(',')].concat(rows.map(r => r.map(q).join(','))).join('\r\n') + '\r\n', 'cp932');
{
  const bad = (buf, why) => {
    let e = null;
    try { parseBarcodeMasterCsv(buf); } catch (x) { e = x; }
    ok(e && e.code === 'bad_csv', why + (e ? ` (${e.message.slice(0, 40)})` : ' — 通ってしまった'));
  };
  bad(Buffer.alloc(0), '空ファイルは拒否');
  bad(csvOf([['a', 'b', 'c']], ['商品ID', '商品名', '検索名称']), '必須列 バーコード が無ければ拒否');
  bad(csvOf([['awa-shio-250', '粟国の塩', '粟国の塩', '4936695001014']], ['商品ID', '商品名', '検索名称', 'バーコード']), '有効区分の列が消えたら拒否 (区分不明のまま本番で使わない)');
  bad(csvOf([['a1', 'A', 'A', '4936695001014', '01'], ['a2', 'B', 'B', '4936695001014', '01']]), '同じバーコードが別の商品に付いていたら取込ごと拒否');
  bad(csvOf([['awa-shio-250', '粟国の塩', '粟国の塩', '4936695001014', '01'], ['x', 'y']]), '列数が違う行は拒否');
  bad(csvOf([]), '中身が無ければ拒否 (マスタが空になることは無い)');
  bad(csvOf([['awa-shio-250', '粟国の塩', '粟国の塩', '49-366-95', '01']]), '刷れる形のバーコードが1件も無ければ拒否');

  const parsed = parseBarcodeMasterCsv(csvOf([
    ['awa-shio-250', '粟国の塩 250g', '粟国の塩', '4936695001014', '01'],
    ['pashima-single-KI', 'パシーマ', 'パシーマ', 'X002ABCD1F', '01'],      // FNSKU が先
    ['pashima-single-KI', 'パシーマ', 'パシーマ', '4903357200047', '01'],   // JAN が後 → 代表は JAN
    ['01520-01', 'エプロン', 'エプロン', '4975953878050', '02'],
    ['01520-01', 'エプロン', 'エプロン', '4975953878050', '02'],            // 同じ商品の同じバーコード = 無害
    ['bad-code', 'ダメ', 'ダメ', '4903-357', '01'],                          // 記号入り = 刷れない
    ['', '商品IDなし', '', '4900000000001', '01'],
  ]));
  eq(parsed.rows.length, 4, '刷れる形のバーコードだけ積む (重複と記号入りと ID なしを除く)');
  eq(parsed.products, 3, '商品数');
  eq(parsed.invalidBarcodes, 1, '刷れない形の件数を数える');
  eq(parsed.blankRows, 1, '商品IDかバーコードが空の行も数える (出力が途中で切れたのを見つける)');
  eq(parsed.kubunCounts, { '01': 3, '02': 1 }, '有効区分の内訳を返す (実データで判断できるように)');
  const pas = parsed.rows.filter(r => r.code_key === 'pashima-single-ki');
  eq(pas.find(r => r.rank === 0).barcode, '4903357200047', '商品ごとの代表は JAN (後から出てきても JAN が勝つ)');
  // 代表はチェックデジットまで正しい GTIN を優先 (社内の数字コードや桁数違いを刷らない)
  const mixed = parseBarcodeMasterCsv(csvOf([
    ['mix-1', 'まぜ', 'まぜ', '12345', '01'],                 // 数字だが GTIN でない
    ['mix-1', 'まぜ', 'まぜ', 'X002ABCD1F', '01'],            // FNSKU
    ['mix-1', 'まぜ', 'まぜ', '4903357200047', '01'],         // 正しい JAN
    ['mix-2', 'まぜ2', 'まぜ2', '4903357200041', '01'],       // チェックデジットが違う
    ['mix-2', 'まぜ2', 'まぜ2', 'X002ZZZZ9Z', '01'],
  ]));
  eq(mixed.rows.filter(r => r.code_key === 'mix-1' && r.rank === 0)[0].barcode, '4903357200047', '正しい GTIN が代表');
  eq(mixed.rows.filter(r => r.code_key === 'mix-2' && r.rank === 0)[0].barcode, 'X002ZZZZ9Z', 'チェックデジットが合わない数字より FNSKU を代表にする');
  ok(bcm.isValidGtin('4903357200047') && bcm.isValidGtin('4936695001014') && !bcm.isValidGtin('4903357200041') && !bcm.isValidGtin('12345'), 'GTIN のチェックデジット判定');
  ok(pas.find(r => r.rank === 1).barcode === 'X002ABCD1F', 'FNSKU も残る (どちらで読んでも引ける)');

  const r = importBarcodeMaster(csvOf([
    ['awa-shio-250', '粟国の塩 250g', '粟国の塩', '4936695001014', '01'],
    ['pashima-single-KI', 'パシーマ', 'パシーマ', 'X002ABCD1F', '01'],
    ['pashima-single-KI', 'パシーマ', 'パシーマ', '4903357200047', '01'],
  ]), { actor: 'test' });
  ok(r.ok && r.total === 3 && r.products === 2 && r.added === 3 && r.removed === 0, '取り込める');
  eq(barcodeMasterStatus().total, 3, '状態 (件数)');
  eq(barcodeMasterStatus().products, 2, '状態 (商品数)');
  // 全量置換: マスタから消えたものは残さない
  const r2 = importBarcodeMaster(csvOf([
    ['awa-shio-250', '粟国の塩 250g', '粟国の塩', '4936695001014', '01'],
    ['pashima-single-KI', 'パシーマ', 'パシーマ', '4903357200047', '01'],
  ]), { actor: 'test' });
  ok(r2.ok && r2.removed === 1 && r2.added === 0, 'マスタから消えたバーコードは消す (刷り続けない)');
  eq(db.prepare("SELECT COUNT(*) n FROM f_inbound_check_barcode_master WHERE barcode = 'X002ABCD1F'").get().n, 0, '消えている');

  // 🚨 全量置換の安全弁: CSV が途中で切れて件数が激減したら取り込まない
  const many = [];
  for (let i = 0; i < 200; i++) many.push(['bulk-' + i, '大量' + i, '大量' + i, 'X00BULK' + String(i).padStart(3, '0'), '01']);
  ok(importBarcodeMaster(csvOf(many), { actor: 'test' }).ok, '200件を取り込む');
  const cut = importBarcodeMaster(csvOf(many.slice(0, 100)), { actor: 'test' });
  ok(!cut.ok && cut.error === 'shrink_guard' && /50%/.test(cut.message), '半分に減った CSV は取り込まない (途中で切れた疑い)');
  eq(barcodeMasterStatus().total, 200, '拒否したので前の中身のまま');
  // 境界: ちょうど2割減 (200→160) は拒否、2割未満 (200→161) は通る
  eq(importBarcodeMaster(csvOf(many.slice(0, 160)), { actor: 'test' }).error, 'shrink_guard', 'ちょうど2割減も拒否する');
  ok(importBarcodeMaster(csvOf(many.slice(0, 161)), { actor: 'test' }).ok, '2割未満の減少は通る');
  ok(importBarcodeMaster(csvOf(many), { actor: 'test' }).ok, '戻す');
  // 承認は「画面が見せた件数と同じとき」だけ通す (誤タップ・古い画面で気づかず全部消さない)
  const mism = importBarcodeMaster(csvOf(many.slice(0, 100)), { actor: 'test', allowShrink: true, confirm: { before: 999, after: 100 } });
  ok(!mism.ok && mism.error === 'confirm_mismatch', '確認したときと件数が違えば承認でも取り込まない');
  eq(barcodeMasterStatus().total, 200, '拒否したので前の中身のまま');
  ok(importBarcodeMaster(csvOf(many.slice(0, 100)), { actor: 'test', allowShrink: true, confirm: { before: 200, after: 100 } }).ok, '画面の件数と合っていれば取り込める');
  eq(barcodeMasterStatus().total, 100, '承認後は入れ替わる');
  ok(importBarcodeMaster(csvOf(many.slice(0, 95)), { actor: 'test' }).ok, '1割未満の減少なら承認なしで通る');
  // 🚨 世代の追い越し: 遅れて着いた古い CSV で新しいマスタを巻き戻さない
  ok(importBarcodeMaster(csvOf(many), { actor: 'test', sourceModifiedAt: '2026-09-06T05:00:00.000Z' }).ok, '新しい世代を取り込む');
  const old = importBarcodeMaster(csvOf(many.slice(0, 195)), { actor: 'test', sourceModifiedAt: '2026-09-06T04:00:00.000Z' });
  ok(!old.ok && old.error === 'stale_source', '古い世代の CSV は後から来てもコミットしない');
  eq(barcodeMasterStatus().total, 200, '巻き戻らない');
  eq(barcodeMasterStatus().sourceModifiedAt, '2026-09-06T05:00:00.000Z', '取り込んだ世代を覚えている');
  // 元に戻す (以降のテスト用)
  ok(importBarcodeMaster(csvOf([
    ['awa-shio-250', '粟国の塩 250g', '粟国の塩', '4936695001014', '01'],
    ['pashima-single-KI', 'パシーマ', 'パシーマ', '4903357200047', '01'],
  ]), { actor: 'test', allowShrink: true, sourceModifiedAt: '2026-09-06T06:00:00.000Z' }).ok, '整え直す');
}

console.log('\n[3] ② バーコードの解決 — マスタが最優先 (粟国の塩が刷れるようになる)');
{
  const awa = resolveBarcode('awa-shio-250');
  ok(awa && awa.barcode === '4936695001014' && awa.barcode_type === 'jan' && awa.source === 'master' && awa.live,
    '入荷したことも在庫も無い商品 (粟国の塩) がマスタから引ける');
  ok(getProductForPrint('awa-shio-250').barcode === '4936695001014', '値札の印刷にも同じ値が渡る');
  // 在庫ミラーに違う値があってもマスタが勝つ
  mirror.prepare(`INSERT INTO mirror_logizard_stock (商品ID, 商品名, バーコード, ブロック略称, ロケ, 在庫数, 引当数, captured_at, synced_at)
    VALUES (?,?,?,?,?,?,?,?,?)`).run('awa-shio-250', '粟国の塩', '4900000000099', 'P1F', 'A-001', 5, 0, now, now);
  eq(resolveBarcode('awa-shio-250').barcode, '4936695001014', '在庫ミラーより マスタが勝つ (マスタが正本)');
  // マスタに無い商品は今までどおり
  const noSup = resolveBarcode('nosup-01');
  eq(noSup, null, 'マスタにも他にも無ければ null (作り話をしない)');
  ok(setProductBarcode('nosup-01', '4900000000123', '山田', { expected: null }).ok, 'マスタに無い商品は人が入れられる');
  eq(resolveBarcode('nosup-01').source, 'manual', '手入力が使われる');
  // マスタにある商品は人が変えられない
  const ro = setProductBarcode('awa-shio-250', '4900000000077', '山田', { expected: '4936695001014' });
  ok(!ro.ok && ro.error === 'readonly_barcode' && /バーコードマスタ/.test(ro.message),
    'マスタにある商品は画面で変えられない (ロジザード側を直してもらう)');
}

console.log('\n[4] ③ 読んだバーコードで商品を引ける (カメラ read → 検索)');
{
  const byJan = searchProducts({ q: '4936695001014' });
  ok(byJan.total === 1 && byJan.rows[0].product_id === 'awa-shio-250', 'JAN の完全一致で引ける');
  importBarcodeMaster(csvOf([
    ['awa-shio-250', '粟国の塩 250g', '粟国の塩', '4936695001014', '01'],
    ['pashima-single-KI', 'パシーマ', 'パシーマ', '4903357200047', '01'],
    ['pashima-single-KI', 'パシーマ', 'パシーマ', 'X002ABCD1F', '01'],
  ]), { actor: 'test' });
  const byFnsku = searchProducts({ q: 'X002ABCD1F' });
  ok(byFnsku.total === 1 && byFnsku.rows[0].product_id === 'pashima-single-KI',
    '代表でない方のバーコード (FNSKU) で読んでも同じ商品に行き着く');
  eq(searchProducts({ q: '4900000000123' }).rows[0]?.product_id, 'nosup-01', '手入力の控えでも引ける');
  eq(searchProducts({ q: '9999999999999' }).total, 0, '無いバーコードは 0 件');
  eq(searchProducts({ q: 'a\\b' }).total, 0, 'バックスラッシュを含む検索語でも落ちない');
}

console.log('\n[5] ② 伝票からの発行でも マスタ → 明細 の順で刷る');
{
  const { importCsv } = dbMod;
  const HEADER = ['入荷管理番号', '入荷管理行番号', '入荷管理詳細行番号', 'ステータス', '荷主入荷NO', '入荷予定日', '入荷受付日', '入荷確定日', '取引先ID', '取引先名', '業務区分名', '商品ID', '商品名', '品質区分名', 'ロケーション', '予定数', '受付数', '検品数', '作成日時', '更新日時', 'バーコード', '備考'];
  const rowCsv = (pid, bc, line) => ['AR9', line, 1, '受付済', '', '20260906', '20260906', '', '0002', 'BF', '通常入荷', pid, '商品 ' + pid, '良品', '', 5, 5, '', '20260906080000', '20260906080000', bc, ''];
  // 伝票には古い (マスタと違う) バーコードが入っている状況を作る
  const csv = iconv.encode([
    HEADER.map(q).join(','),
    rowCsv('awa-shio-250', '4900000000011', 1).map(q).join(','),
    rowCsv('nosup-01', '4900000000222', 2).map(q).join(','),
  ].join('\r\n') + '\r\n', 'cp932');
  ok(importCsv(csv, { fileName: 'CA04001_t.csv', source: 'manual_upload', actor: 'test' }).ok, '伝票を取り込む');
  const pq = await import('../apps/inbound-check/print-queue.js');
  const agent = dbMod.createDevice('倉庫PC', 'admin', { kind: 'agent', printerName: 'Brother QL-700' });
  pq.recordHeartbeat(agent.id, { note: 'ready', bpac: true, paperFormatOk: true, version: 'test' });
  const batch = db.prepare("SELECT id FROM f_inbound_check_batches WHERE status = 'active'").get();
  let n = 0; const rid = () => 'req-' + Date.now() + '-' + (++n);
  const a = pq.enqueuePrintJob({ batchId: batch.id, lineKey: 'AR9|1|1', copies: 1, clientRequestId: rid() });
  ok(a.ok && a.job.barcode === '4936695001014',
    '伝票の古い値ではなく マスタの値で刷る (ロジザードで直したらマスタの方が新しい)');
  const b = pq.enqueuePrintJob({ batchId: batch.id, lineKey: 'AR9|2|1', copies: 1, clientRequestId: rid() });
  ok(b.ok && b.job.barcode === '4900000000222', 'マスタに無い商品は その明細自身の値で刷る');
}

console.log('\n[6] HTTP — 管理画面の取込ボタンの入口');
{
  const express = (await import('express')).default;
  const router = (await import('../apps/inbound-check/router.js')).default;
  const app = express();
  app.use(express.json());
  app.use((req, res, next) => {
    const s = req.headers['x-test-session'];
    req.session = s ? { authenticated: true, email: 'tester@example.com', displayName: 'テスター', allowedApps: '*', role: 'user', destroy: (cb) => cb() } : {};
    next();
  });
  app.use('/apps/inbound-check', router);
  const server = http.createServer(app);
  await new Promise(r => server.listen(0, '127.0.0.1', r));
  const origin = `http://127.0.0.1:${server.address().port}`;
  const call = async (method, url, { session = null, body = null } = {}) => {
    const headers = {};
    if (session) headers['x-test-session'] = session;
    if (body !== null) { headers['Content-Type'] = 'application/json'; headers.Origin = origin; }
    const res = await fetch(origin + '/apps/inbound-check' + url, { method, headers, body: body !== null ? JSON.stringify(body) : undefined, redirect: 'manual' });
    const ct = res.headers.get('content-type') || '';
    return { status: res.status, body: ct.includes('json') ? await res.json().catch(() => ({})) : await res.text() };
  };
  eq((await call('POST', '/admin/fetch-barcode-master', { body: {} })).status, 302, 'セッション無しは入れない');
  // 承認つきの再送は「画面に出ていた件数」を一緒に送らないと受け付けない
  const noConfirm = await call('POST', '/admin/fetch-barcode-master', { session: 'user', body: { allow_shrink: true } });
  ok(noConfirm.status === 400 && noConfirm.body.error === 'bad_request', '減っても取り込む場合は件数の確認が要る');
  const r = await call('POST', '/admin/fetch-barcode-master', { session: 'user', body: {} });
  ok(r.status === 400 && /バーコードマスタ|Drive|GOOGLE_SERVICE_ACCOUNT_KEY/.test(r.body.message || ''),
    'Drive を見に行けない環境では理由つきで 400 (取り込み済みのデータは壊さない)');
  eq(barcodeMasterStatus().total, 3, '失敗しても既存のマスタは残る');
  const cam = await call('GET', '/api/products?q=4936695001014', { session: 'user' });
  ok(cam.status === 200 && cam.body.rows.length === 1 && cam.body.rows[0].product_id === 'awa-shio-250'
    && cam.body.rows[0].supplier_name === '粟国の塩' && cam.body.rows[0].barcode_source === 'master',
    'API: 読んだバーコードで検索でき、仕入先名とバーコードの出どころが付く');
  server.close();
}

console.log(`\n結果: ${pass} PASS / ${fail} FAIL`);
process.exitCode = fail ? 1 : 0;
