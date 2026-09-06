/**
 * 🔍 商品から探す — 入荷受付伝票に無い商品の 入庫情報の参照・編集 と 値札印字 (2026-09-06 中原さん)
 *
 * 実行: node scripts/test-inbound-check-products.mjs
 *
 * 守りたいのは3つ。
 *   ① 値札に刷るバーコードは「見つけたら控える・無ければ人が入れる」で、刷れない形 (記号入り等) を絶対に積まない
 *   ② 印刷キューは伝票からの発行と**同じ規則** (進行中は積まない / 結果不明は実物確認の証跡 / 画面の値を信じない)
 *   ③ 印刷ジョブ表の作り直し (source 列・batch_id NULL 可) で、進行中のジョブを落とさない
 */
import fs from 'fs';
import os from 'os';
import path from 'path';
import http from 'http';

if (!process.env.DATA_DIR) process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'ic-products-'));
const { initMirrorDB, getMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const mirror = getMirrorDB();

let pass = 0, fail = 0;
const ok = (c, l) => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}`); } };
const eq = (a, b, l) => ok(JSON.stringify(a) === JSON.stringify(b), `${l} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);
const now = new Date().toISOString();

// ─── ③ 旧版の印刷ジョブ表 (2026-09-05 初版: batch_id NOT NULL・source 無し) を先に作っておく ───
console.log('[1] 印刷ジョブ表の作り直し (旧版 → source 列 + batch_id NULL 可)');
mirror.exec(`
  CREATE TABLE f_inbound_check_devices (id INTEGER PRIMARY KEY AUTOINCREMENT, token_hash TEXT NOT NULL UNIQUE, label TEXT NOT NULL,
    created_by TEXT NOT NULL, created_at TEXT NOT NULL, last_seen_at TEXT, revoked_at TEXT);
  INSERT INTO f_inbound_check_devices (id, token_hash, label, created_by, created_at) VALUES (1, 'h1', '倉庫PC(旧)', 't', '${now}');
  CREATE TABLE f_inbound_check_print_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT, client_request_id TEXT NOT NULL UNIQUE,
    batch_id INTEGER NOT NULL, line_key TEXT NOT NULL, code_key TEXT NOT NULL, product_code TEXT NOT NULL, product_name TEXT NOT NULL,
    barcode TEXT NOT NULL, barcode_type TEXT NOT NULL CHECK (barcode_type IN ('jan','fnsku')), pack_qty TEXT NOT NULL DEFAULT '',
    copies INTEGER NOT NULL CHECK (copies BETWEEN 1 AND 50), printer_name TEXT NOT NULL, target_device_id INTEGER NOT NULL,
    requested_by TEXT, requested_device TEXT, acknowledged_job_id INTEGER, acknowledged_at TEXT,
    state TEXT NOT NULL CHECK (state IN ('queued','leased','submitted','completed','failed','manual','unknown')),
    lease_device_id INTEGER, lease_token TEXT, lease_expires_at TEXT, spool_job_id TEXT, error TEXT,
    created_at TEXT NOT NULL, updated_at TEXT NOT NULL, leased_at TEXT, submitted_at TEXT, finished_at TEXT, alerted_state TEXT);
  INSERT INTO f_inbound_check_print_jobs (client_request_id, batch_id, line_key, code_key, product_code, product_name, barcode, barcode_type, copies, printer_name, target_device_id, state, lease_device_id, lease_token, lease_expires_at, created_at, updated_at, leased_at)
    VALUES ('old-leased-1', 7, 'AR1|1|1', 'pashima', 'pashima', 'パシーマ', '4903357200047', 'jan', 3, 'Brother QL-700', 1, 'leased', 1, 'tok-old', '2099-01-01T00:00:00.000Z', '${now}', '${now}', '${now}');
  INSERT INTO f_inbound_check_print_jobs (client_request_id, batch_id, line_key, code_key, product_code, product_name, barcode, barcode_type, copies, printer_name, target_device_id, state, created_at, updated_at, finished_at)
    VALUES ('old-done-2', 7, 'AR1|2|1', 'toretate', 'toretate', 'とれたて', 'X002ABCD1F', 'fnsku', 1, 'Brother QL-700', 1, 'completed', '${now}', '${now}', '${now}');
`);
const dbMod = await import('../apps/inbound-check/db.js');
const { createTables, getDB, importCsv, createDevice, verifyDevice, resolveBarcode, setProductBarcode, listProductSuppliers, searchProducts, getProductForPrint } = dbMod;
createTables();
{
  const db = getDB();
  const cols = db.prepare('PRAGMA table_info(f_inbound_check_print_jobs)').all();
  const by = Object.fromEntries(cols.map(c => [c.name, c]));
  ok(!!by.source, 'source 列が足された');
  eq(by.batch_id.notnull, 0, 'batch_id は NULL 可になった');
  eq(by.line_key.notnull, 0, 'line_key は NULL 可になった');
  const rows = db.prepare('SELECT id, client_request_id, source, state, lease_token FROM f_inbound_check_print_jobs ORDER BY id').all();
  eq(rows.length, 2, '旧版の行を全部引き継ぐ');
  ok(rows[0].id === 1 && rows[0].state === 'leased' && rows[0].lease_token === 'tok-old' && rows[0].source === 'line', '進行中 (leased) のジョブも id・lease ごと残る (エージェントの報告が通る)');
  ok(rows[1].source === 'line', '旧行の source は line');
  createTables();
  eq(db.prepare('SELECT COUNT(*) n FROM f_inbound_check_print_jobs').get().n, 2, '2回目の createTables では作り直さない (冪等)');
  ok(!db.prepare("SELECT 1 FROM sqlite_master WHERE name = 'f_inbound_check_print_jobs__new'").get(), '一時表が残らない');
}

// ─── 商品マスタ・仕入先・入庫情報・在庫ミラーの種まき ───
const db = getDB();
mirror.exec(`
  CREATE TABLE IF NOT EXISTS po_suppliers (supplier_code TEXT PRIMARY KEY, name TEXT NOT NULL, order_memo TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
  INSERT INTO po_suppliers VALUES ('S001', 'エーエムシー', NULL, '${now}', '${now}');
  INSERT INTO po_suppliers VALUES ('S002', 'サロンジェ', NULL, '${now}', '${now}');
`);
const insP = mirror.prepare(`INSERT INTO mirror_products (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, 仕入先コード, updated_at) VALUES (?, ?, ?, ?, ?, 'unknown', ?, ?)`);
insP.run(1, 'pashima', 'パシーマ パットシーツ', '単品', '取扱中', 'S001', now);
insP.run(2, 'toretate', 'とれたてキャップ', '単品', '取扱中', 'S001', now);
insP.run(3, 'apron-01', 'サロンジェ 子供エプロン 110', '単品', '取扱中', 'S002', now);
insP.run(4, 'apron-old', 'サロンジェ 旧エプロン', '単品', '取扱中止', 'S002', now);
insP.run(5, 'set-01', 'ギフトセット', 'セット', '取扱中', 'S001', now);
insP.run(6, 'nosup', '仕入先なし商品', '単品', '取扱中', null, now);
mirror.exec(`
  CREATE TABLE IF NOT EXISTS f_inbound_info (code_key TEXT PRIMARY KEY, 商品コード TEXT, 商品名 TEXT, 入数 INTEGER, 入庫時BCシール貼りフラグ TEXT, 直接ピックロケ保管 TEXT, BF保管荷姿 TEXT, いろは在庫化作業有無 TEXT, memo TEXT, source TEXT, version INTEGER NOT NULL DEFAULT 1, created_at TEXT, updated_at TEXT, updated_by TEXT);
  INSERT INTO f_inbound_info (code_key, 商品コード, 商品名, 入数, 入庫時BCシール貼りフラグ, いろは在庫化作業有無, source, created_at, updated_at)
    VALUES ('pashima', 'pashima', 'パシーマ パットシーツ', 14, 'BCシール貼付必要', '無し', 'manual', '${now}', '${now}');
  INSERT INTO mirror_logizard_stock (商品ID, 商品名, バーコード, ブロック略称, ロケ, 在庫数, 引当数, captured_at, synced_at)
    VALUES ('apron-01', 'サロンジェ 子供エプロン 110', '4936968448386', 'P3F', 'A-01-01', 12, 0, '${now}', '${now}');
`);

console.log('\n[2] 仕入先の一覧と商品の検索');
{
  const sup = listProductSuppliers();
  eq(sup.map(s => s.code), ['S001', 'S002'], '仕入先 = 商品マスタに紐づくものだけ (単品・取扱中)');
  eq(sup[0].name, 'エーエムシー', '名前は po_suppliers から');
  eq(sup[0].products, 2, '商品数 (セットは数えない)');
  const all = searchProducts({ supplier: 'S002' });
  eq(all.rows.map(r => r.product_id), ['apron-01'], '仕入先で絞る (既定は取扱中だけ)');
  const inc = searchProducts({ supplier: 'S002', includeInactive: true });
  eq(inc.rows.map(r => r.product_id).sort(), ['apron-01', 'apron-old'], '取扱中止も');
  const byName = searchProducts({ q: 'パシーマ' });
  eq(byName.rows.map(r => r.product_id), ['pashima'], '商品名で探す');
  const byCode = searchProducts({ q: 'toreta' });
  eq(byCode.rows.map(r => r.product_id), ['toretate'], '商品コードの部分一致');
  const like = searchProducts({ q: '100%' });
  eq(like.total, 0, 'LIKE の記号は文字として扱う');
  const row = byName.rows[0];
  ok(row.info && row.info.irisu === 14 && row.pack_qty === 14, '入庫情報 (入数) が付く');
  ok(row.supplier_name === 'エーエムシー', '仕入先名が付く');
  const none = searchProducts({ q: 'nosup' });
  ok(none.rows[0].supplier_code == null, '仕入先なしの商品も探せる');
  const lim = searchProducts({ supplier: 'S001', limit: 1 });
  ok(lim.rows.length === 1 && lim.total === 2, 'limit と total');
  const pg2 = searchProducts({ supplier: 'S001', limit: 1, offset: 1 });
  ok(pg2.rows.length === 1 && pg2.rows[0].product_id !== lim.rows[0].product_id, 'offset で次のページ');
}

console.log('\n[3] バーコードは 控え → 取込行 → 在庫 → 入荷予定 の順に探し、見つけたら控える');
{
  // 在庫ミラーにある商品
  const a = resolveBarcode('apron-01');
  ok(a && a.barcode === '4936968448386' && a.barcode_type === 'jan' && a.source === 'stock', '在庫ミラーから見つかる');
  const cached = db.prepare('SELECT * FROM f_inbound_check_barcodes WHERE code_key = ?').get('apron-01');
  ok(cached && cached.source === 'stock', '見つけたら控えに写す');
  mirror.prepare('DELETE FROM mirror_logizard_stock WHERE 商品ID = ?').run('apron-01');
  const a2 = resolveBarcode('apron-01');
  ok(a2 && a2.barcode === '4936968448386', '在庫が消えても (行が無くなっても) 控えから引ける');
  // 取込行にある商品 (入荷受付伝票の明細)
  const HEADER = ['入荷管理番号', '入荷管理行番号', '入荷管理詳細行番号', 'ステータス', '荷主入荷NO', '入荷予定日', '入荷受付日', '入荷確定日', '取引先ID', '取引先名', '業務区分名', '商品ID', '商品名', '品質区分名', 'ロケーション', '予定数', '受付数', '検品数', '作成日時', '更新日時', 'バーコード', '備考'];
  const iconv = (await import('iconv-lite')).default;
  const q = v => `"${String(v == null ? '' : v).replace(/"/g, '""')}"`;
  const rowCsv = (pid, bc) => [ 'AR9', 1, 1, '受付済', '', '20260906', '20260906', '', '0002', 'BF', '通常入荷', pid, `商品 ${pid}`, '良品', '', 5, 5, '', '20260906080000', '20260906080000', bc, '' ];
  const csv = iconv.encode([HEADER.map(q).join(','), rowCsv('pashima', '4903357200047').map(q).join(',')].join('\r\n') + '\r\n', 'cp932');
  ok(importCsv(csv, { fileName: 'CA04001_t.csv', source: 'manual_upload', actor: 'test' }).ok, '取込');
  const p = resolveBarcode('pashima');
  ok(p && p.barcode === '4903357200047' && p.source === 'line', '取込行から見つかる');
  // どこにも無い商品
  eq(resolveBarcode('toretate'), null, '見つからなければ null (作り話をしない)');
  eq(searchProducts({ q: 'toretate' }).rows[0].barcode, null, '検索結果でも null');
  // 人が入れる
  const bad = setProductBarcode('toretate', '4903-357');
  ok(!bad.ok && bad.error === 'bad_barcode', '記号入りは拒否 (刷れない形を残さない)');
  const man = setProductBarcode('toretate', ' X002ABCD1F ', '山田');
  ok(man.ok && man.barcode === 'X002ABCD1F' && man.barcode_type === 'fnsku', '英数字 = FNSKU として控える (trim)');
  const t = resolveBarcode('toretate');
  ok(t && t.source === 'manual' && t.barcode === 'X002ABCD1F', '手入力は控えの最優先');
  const over = setProductBarcode('apron-01', '4900000000001', '山田');
  ok(over.ok && resolveBarcode('apron-01').barcode === '4900000000001' && resolveBarcode('apron-01').source === 'manual', '在庫から控えた値も人が直せる');
  const clr = setProductBarcode('toretate', '');
  ok(clr.ok && clr.cleared && resolveBarcode('toretate') === null, '空にすると手入力分を消す');
  ok(searchProducts({ q: '490000000000' }).rows.some(r => r.product_id === 'apron-01'), '控えたバーコードで検索できる');
  ok(!setProductBarcode('', '123').ok, '商品なしは拒否');
  eq(setProductBarcode('no-such-code', '4900000000009').error, 'not_found', '商品マスタに無いコードには控えを作らない');
  // 画面が見ていた値 (expected) と今の値が違えば書かない (別の人が先に入れた/直した)
  ok(setProductBarcode('toretate', 'X002ABCD1F', '山田').ok, '手入力');
  const stale = setProductBarcode('toretate', 'X002ZZZZ9Z', '佐藤', { expected: null });
  ok(!stale.ok && stale.error === 'state_changed' && /X002ABCD1F/.test(stale.message), '画面が「未登録」のまま送った入力は、先に別の人が入れていれば書かない (state_changed)');
  ok(setProductBarcode('toretate', 'X002ZZZZ9Z', '佐藤', { expected: 'X002ABCD1F' }).ok, '見ていた値が今の値と同じなら直せる');
  ok(setProductBarcode('toretate', '', '佐藤', { expected: 'X002ZZZZ9Z' }).ok, '消すときも expected で見張る');
  eq(resolveBarcode('toretate'), null, '消えた');
  // LIKE: ESCAPE 文字そのもの (\\) を含む検索語でも落ちない・全件に化けない
  eq(searchProducts({ q: 'a\\b' }).total, 0, 'バックスラッシュを含む検索語は文字として扱う');
  for (const bad of [Infinity, 1.5, 1e12, -1, 'x']) ok(Array.isArray(searchProducts({ supplier: 'S001', limit: bad, offset: bad }).rows), 'limit/offset に ' + String(bad) + ' が来ても落ちない');
}

console.log('\n[4] 値札の印刷キュー (商品モード) — 伝票からの発行と同じ規則');
const pq = await import('../apps/inbound-check/print-queue.js');
const { enqueuePrintJob, leaseNextJob, markSubmitted, markFinished, latestJobsForProducts, latestJobsForBatch, sweepPrintJobs, REPORT_DEADLINE_SEC } = pq;
let seq = 0; const rid = () => `req-${Date.now()}-${++seq}`;
const agent = createDevice('倉庫PC', 'admin', { kind: 'agent', printerName: 'Brother QL-700' });
const agentRow = verifyDevice(agent.token);
{
  const r0 = enqueuePrintJob({ productCode: 'no-such', copies: 1, clientRequestId: rid() });
  ok(!r0.ok && r0.error === 'not_found', '商品マスタに無い商品は積めない');
  const r1 = enqueuePrintJob({ productCode: 'toretate', copies: 1, clientRequestId: rid() });
  ok(!r1.ok && r1.error === 'bad_barcode' && /入力してください/.test(r1.message), 'バーコードが分からない商品は「入力して」と返す');
  const r2 = enqueuePrintJob({ productCode: 'toretate', barcodeOverride: '4903-357', copies: 1, clientRequestId: rid() });
  ok(!r2.ok && r2.error === 'bad_barcode', '画面から来たバーコードも形式を検査する');
  const r3 = enqueuePrintJob({ productCode: 'toretate', barcodeOverride: 'X002ABCD1F', copies: 2, packQty: 12, clientRequestId: rid(), requestedBy: '山田', requestedDevice: 'iPad2' });
  ok(r3.ok && r3.created && r3.job.source === 'product' && r3.job.barcode === 'X002ABCD1F' && r3.job.barcode_type === 'fnsku' && r3.job.pack_qty === '12', '画面のバーコードで積める (source=product)');
  ok(r3.job.line_key == null && r3.job.product_name === 'とれたてキャップ', '明細は無し・商品名は商品マスタから (画面の値ではない)');
  const dup = enqueuePrintJob({ productCode: 'toretate', barcodeOverride: 'X002ABCD1F', copies: 1, clientRequestId: rid() });
  ok(!dup.ok && dup.error === 'in_progress' && dup.job.id === r3.job.id, '同じ商品の進行中ジョブがあれば積めない');
  const m = latestJobsForProducts(['toretate', 'pashima']);
  ok(m.get('toretate')?.id === r3.job.id && m.get('pashima')?.id === 1, '商品ごとの最新ジョブ (旧伝票からのジョブも商品単位で出る)');
  eq(latestJobsForBatch(999).size, 0, '伝票側の一覧には商品モードのジョブが混ざらない');
  // 控え (manual) があるときは override 不要
  setProductBarcode('pashima', '4903357200047', '山田');
  // 🚨 旧伝票 (作り直す前) の進行中ジョブが同じ商品なら、商品画面からも積めない (紙は同じ1枚)。終われば積める
  const oldBlock = enqueuePrintJob({ productCode: 'pashima', copies: 1, clientRequestId: rid() });
  ok(!oldBlock.ok && oldBlock.error === 'in_progress' && oldBlock.job.id === 1 && oldBlock.job.source === 'line', '旧伝票の進行中ジョブ (leased) が同じ商品なら商品画面からも in_progress');
  ok(markSubmitted(1, { deviceId: 1, leaseToken: 'tok-old', spoolJobId: 'old-spool-1' }).ok && markFinished(1, { deviceId: 1, leaseToken: 'tok-old', ok: true }).ok, '旧ジョブの投入・完了報告は作り直しをまたいで通る (id・lease を保っている)');
  const r4 = enqueuePrintJob({ productCode: 'PASHIMA ', copies: 1, clientRequestId: rid() });
  ok(r4.ok && r4.job.barcode === '4903357200047' && r4.job.code_key === 'pashima', '控えのバーコードで積める (商品コードは lower/trim で照合)');
  // エージェント側の契約は変わらない
  const j = leaseNextJob(agentRow);
  ok(j && j.id === r3.job.id && j.lineKey === '' && j.barcodeType === 'fnsku' && j.productCode === 'toretate', 'lease の JSON: 明細が無ければ lineKey は空文字 (エージェントは参考情報にしか使わない)');
  eq(Object.keys(j).sort(), ['barcode', 'barcodeType', 'copies', 'id', 'leaseExpiresAt', 'leaseToken', 'lineKey', 'packQty', 'printerName', 'productCode', 'productName', 'requestedBy'], 'JSON の形は agent.ps1 の前提のまま');
  ok(markSubmitted(j.id, { deviceId: agentRow.id, leaseToken: j.leaseToken, spoolJobId: 'nefuda-p1' }).ok, '投入報告');
  ok(markFinished(j.id, { deviceId: agentRow.id, leaseToken: j.leaseToken, ok: true }).ok, '完了報告');
  eq(latestJobsForProducts(['toretate']).get('toretate').state, 'completed', '完了が商品の最新に出る');
  const again = enqueuePrintJob({ productCode: 'toretate', barcodeOverride: 'X002ABCD1F', copies: 1, clientRequestId: rid() });
  ok(again.ok, '終わった後は追加で発行できる');
  // 結果不明 → 証跡なしでは積めない (伝票側と同じ)
  const j2 = leaseNextJob(agentRow);   // pashima の分 (r4) が先
  const j3 = leaseNextJob(agentRow);
  const tor = [j2, j3].find(x => x.productCode === 'toretate');
  sweepPrintJobs({ now: new Date(Date.now() + (REPORT_DEADLINE_SEC + 1) * 1000).toISOString() });
  const noAck = enqueuePrintJob({ productCode: 'toretate', barcodeOverride: 'X002ABCD1F', copies: 1, clientRequestId: rid() });
  ok(!noAck.ok && noAck.error === 'confirm_unknown' && noAck.job.id === tor.id, 'unknown の後は実物確認の証跡が要る');
  const withAck = enqueuePrintJob({ productCode: 'toretate', barcodeOverride: 'X002ABCD1F', copies: 1, clientRequestId: rid(), acknowledgeUnknownJobId: tor.id });
  ok(withAck.ok && withAck.job.acknowledged_job_id === tor.id, '証跡を付ければ積める');
  // 🚨 二重印刷の見張りは商品単位: 商品画面で結果不明のまま → 伝票からも証跡なしでは積めない (Codex R1 High-1)
  const batch = db.prepare("SELECT id FROM f_inbound_check_batches WHERE status = 'active'").get();
  const cross0 = enqueuePrintJob({ batchId: batch.id, lineKey: 'AR9|1|1', copies: 1, clientRequestId: rid() });
  ok(!cross0.ok && cross0.error === 'confirm_unknown' && cross0.job.id === j2.id && cross0.job.source === 'product', '商品画面からの発行が結果不明なら、伝票からも実物確認の証跡が要る (商品単位で見張る)');
  const line = enqueuePrintJob({ batchId: batch.id, lineKey: 'AR9|1|1', copies: 1, clientRequestId: rid(), acknowledgeUnknownJobId: j2.id });
  ok(line.ok && line.job.source === 'line' && line.job.line_key === 'AR9|1|1' && line.job.acknowledged_job_id === j2.id, '伝票からの発行は source=line (商品画面のジョブを証跡にできる)');
  const lineDup = enqueuePrintJob({ batchId: batch.id, lineKey: 'AR9|1|1', copies: 1, clientRequestId: rid() });
  ok(!lineDup.ok && lineDup.error === 'in_progress', '伝票側の連打は進行中');
  const cross1 = enqueuePrintJob({ productCode: 'pashima', copies: 1, clientRequestId: rid() });
  ok(!cross1.ok && cross1.error === 'in_progress' && cross1.job.id === line.job.id && cross1.job.source === 'line', '伝票から印刷中なら商品画面からも積めない');
  eq(latestJobsForProducts(['pashima']).get('pashima').id, line.job.id, '商品画面の行には伝票からのジョブも出る (最新はどちらでも)');
  eq(latestJobsForBatch(batch.id).get('AR9|1|1').id, line.job.id, '伝票側の行には伝票のジョブ');
  // 画面が見ていたバーコードと控えが違えば刷らない (別の人が直した後の古い入力で違うシールを出さない。Codex R1 High-2)
  const staleBc = enqueuePrintJob({ productCode: 'toretate', barcodeOverride: 'X002OLD00A', copies: 1, clientRequestId: rid() });
  ok(!staleBc.ok && staleBc.error === 'state_changed' && /X002ABCD1F/.test(staleBc.message), '控えと違うバーコードを送ってきたら state_changed (刷る値は必ず控えの値)');
  eq(db.prepare('SELECT barcode FROM f_inbound_check_barcodes WHERE code_key = ?').get('toretate').barcode, 'X002ABCD1F', '控えは上書きされない');
  ok(getProductForPrint('nosup') && getProductForPrint('nosup').barcode === null, 'getProductForPrint: バーコード無しは null');
}

console.log('\n[5] HTTP — 画面と API の入口');
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
  const base = `${origin}/apps/inbound-check`;
  const call = async (method, url, { cookie = null, session = null, body = null } = {}) => {
    const headers = {};
    if (cookie) headers.Cookie = cookie;
    if (session) headers['x-test-session'] = session;
    if (body) { headers['Content-Type'] = 'application/json'; headers.Origin = origin; }
    const res = await fetch(base + url, { method, headers, body: body ? JSON.stringify(body) : undefined, redirect: 'manual' });
    const ct = res.headers.get('content-type') || '';
    return { status: res.status, body: ct.includes('json') ? await res.json().catch(() => ({})) : await res.text() };
  };
  const ipad = createDevice('入荷iPad3', 'admin');
  eq((await call('GET', '/products')).status, 302, '未登録は登録画面へ');
  const page = await call('GET', '/products', { cookie: `ic_device=${ipad.token}` });
  ok(page.status === 200 && /商品を探す/.test(page.body) && /printBox/.test(page.body), '端末Cookie で画面が出る');
  eq((await call('GET', '/api/products?q=x')).status, 401, 'API は未認証 401');
  const sup = await call('GET', '/api/products/suppliers', { session: 'user' });
  ok(sup.status === 200 && sup.body.suppliers.length === 2, '仕入先の一覧');
  const list = await call('GET', '/api/products?supplier=S001', { cookie: `ic_device=${ipad.token}` });
  ok(list.status === 200 && list.body.rows.length === 2 && list.body.rows.every(r => 'print_job' in r) && Array.isArray(list.body.workers) && list.body.field_options['BF保管荷姿'], '検索結果に印刷ジョブ・作業者・選択肢が付く');
  ok(list.body.rows.find(r => r.product_id === 'toretate').print_job.state === 'queued', '商品ごとの直近ジョブが行に付く');
  const ps = await call('POST', '/api/products/print-status', { cookie: `ic_device=${ipad.token}`, body: { code_keys: ['toretate', 'PASHIMA', 'zzz'] } });
  ok(ps.status === 200 && ps.body.jobs.toretate && ps.body.jobs.toretate.state === 'queued' && ps.body.jobs.pashima && ps.body.jobs.pashima.source === 'line' && !('zzz' in ps.body.jobs) && Array.isArray(ps.body.print_agents), '表示中の商品の印刷状況だけ取り直せる (伝票からのジョブも・200件超でも)');
  const bcStale = await call('POST', '/api/products/barcode', { session: 'user', body: { code_key: 'toretate', barcode: 'X002NEW00B', expected: null } });
  ok(bcStale.status === 409 && bcStale.body.error === 'state_changed', 'HTTP: 先に別の人が入れていれば 409');
  eq((await call('POST', '/api/products/barcode', { session: 'user', body: { code_key: 'no-such-code', barcode: '4900000000009' } })).status, 404, 'HTTP: 商品マスタに無ければ 404');
  const bc0 = await call('POST', '/api/products/barcode', { cookie: `ic_device=${ipad.token}`, body: { code_key: 'nosup', barcode: '4900000000002' } });
  ok(bc0.status === 400 && bc0.body.error === 'worker_required', '端末からのバーコード入力は作業者が要る');
  const bc1 = await call('POST', '/api/products/barcode', { session: 'user', body: { code_key: 'nosup', barcode: '4900000000002' } });
  ok(bc1.status === 200 && bc1.body.ok && bc1.body.barcode_type === 'jan', 'セッションなら入れられる');
  const bc2 = await call('POST', '/api/products/barcode', { session: 'user', body: { code_key: 'nosup', barcode: 'ab-cd' } });
  eq(bc2.status, 400, '不正な形は 400');
  const pj = await call('POST', '/api/print/jobs', { session: 'user', body: { product_code: 'nosup', copies: 1, client_request_id: rid() } });
  ok(pj.status === 200 && pj.body.ok && pj.body.job.source === 'product' && pj.body.job.barcode === '4900000000002', 'HTTP から商品モードで積める (控えのバーコード)');
  const pj2 = await call('POST', '/api/print/jobs', { session: 'user', body: { product_code: 'apron-old', barcode: 'bad-code', copies: 1, client_request_id: rid() } });
  ok(pj2.status === 400 && pj2.body.error === 'bad_barcode', 'HTTP でも形式検査');
  server.close();
}

console.log(`\n結果: ${pass} PASS / ${fail} FAIL`);
process.exitCode = fail ? 1 : 0;
