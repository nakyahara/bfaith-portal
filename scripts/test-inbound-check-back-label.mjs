import { temporaryTestRoot } from './test-temp-dir.mjs';
await temporaryTestRoot(import.meta.url);
/**
 * 🆕 新商品の判定 + パッケージ裏面ラベル写真のテスト (2026-09-18 中原さん指示)
 *
 * 見るところ:
 *   ①新商品の定義 = NE商品登録日が3週間以内 かつ 入庫履歴なし。
 *     「データが無い」を「新商品ではない」に読み替えていないか (unknown で返すか)
 *   ②撮影が確認の前提条件になるか (getState の dest.missing / backLabelGate)
 *   ③写真の受け取り: 冪等・上限・不正ファイル・撮り直し
 *   ④Drive 送信キュー: 成功で uploaded / 失敗で再試行 / 使い切ったら停止と管理画面からの解除
 *   ⑤緊急停止 (env INBOUND_CHECK_BACK_LABEL_REQUIRED=0) で必須が外れるか
 *
 * 実行: node scripts/test-inbound-check-back-label.mjs
 */
import fs from 'fs';
import os from 'os';
import path from 'path';

if (!process.env.DATA_DIR) {
  process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'ic-bl-test-'));
}
// Drive への送信は _setDriveUpload でモックするが、キューは「Drive が設定されているか」を
// 先に見る (未設定なら送りにこない = 本番と同じ判断)。鍵の中身は使わないのでダミーで良い
process.env.GOOGLE_SERVICE_ACCOUNT_KEY = Buffer.from('{"type":"service_account"}').toString('base64');

let pass = 0, fail = 0;
const ok = (c, l) => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}`); } };

const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();

const np = await import('../apps/inbound-check/new-product.js');
const bl = await import('../apps/inbound-check/back-label.js');
// 実体を失った行の仕分けは Drive に聞く。既定は「Drive にも無い」(実 API を叩かない)。
// 「実は Drive にあった」「Drive に聞けなかった」は個別のテストで差し替える
bl._setDriveFind(async () => null);
const { getDB, getState, backLabelGate, lineForBackLabel } = await import('../apps/inbound-check/db.js');

const db = getDB();
const now = new Date().toISOString();
const today = '2026-09-18';
const d = (daysAgo) => {
  const t = new Date(Date.parse(`${today}T00:00:00Z`) - daysAgo * 86400000);
  return t.toISOString().slice(0, 10);
};

// ─── 種まき: 商品マスタ (登録日つき) ───
const insProd = db.prepare(`INSERT INTO mirror_products
  (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, new_product_launch_date, updated_at)
  VALUES (?,?,?,'単品','取扱中','unknown',?,?)`);
insProd.run(1, 'NEW-A', '新商品A (初入荷)', d(3), now);            // 新商品
insProd.run(2, 'OLD-B', '既存品B', d(200), now);                   // 登録日が古い
insProd.run(3, 'NEW-C', '新商品C (仕入済み)', d(5), now);          // 登録は新しいが最終仕入日あり
insProd.run(4, 'NEW-D', '新商品D (在庫あり)', d(5), now);          // 登録は新しいがロジザードに在庫行
insProd.run(5, 'NEW-E', '新商品E (過去に受入済)', d(5), now);      // 過去に確認済み
insProd.run(6, 'NODATE-F', '登録日が空の商品F', null, now);        // 判定できない
insProd.run(7, 'FUTURE-G', '登録日が未来の商品G', '2099-01-01', now); // 誤入力

// 商品管理リスト snapshot (最終仕入日の正本)
db.prepare(`INSERT INTO mirror_pml_published (id, run_id, status, synced_at) VALUES (1, 'run-1', 'published', ?)`).run(now);
const insPml = db.prepare(`INSERT INTO mirror_pml_snapshot_rows (run_id, 商品コード, 商品名, 最終仕入日, 登録日) VALUES ('run-1',?,?,?,?)`);
insPml.run('NEW-A', '新商品A (初入荷)', '', d(3));
insPml.run('OLD-B', '既存品B', d(10), d(200));
insPml.run('NEW-C', '新商品C (仕入済み)', d(1), d(5));
insPml.run('NEW-D', '新商品D (在庫あり)', '', d(5));
insPml.run('NEW-E', '新商品E (過去に受入済)', '', d(5));

// ロジザード在庫 (在庫ゼロでも行が残る = 一度は入庫している)
db.prepare(`INSERT INTO mirror_logizard_stock (商品ID, 商品名, 品質区分名, 在庫数, 引当数, captured_at, synced_at)
  VALUES ('new-d', '新商品D (在庫あり)', '良品', 0, 0, ?, ?)`).run(now, now);

// このアプリの過去の受入実績
db.prepare(`INSERT INTO f_inbound_check_destinations
  (batch_id, line_key, ar_no, product_id, product_name, planned_qty, destination, decided_from, worker, decided_at)
  VALUES (0, 'OLD', 'AR000', 'NEW-E', '新商品E (過去に受入済)', 1, 'bfaith', 'master', 'テスト', ?)`).run(now);

console.log('[1] 日付の正規化 (NE の 作成日 は "YYYY-MM-DD HH:MM:SS" でも来る)');
{
  ok(np.normalizeDate('2026-09-16 18:23:46') === '2026-09-16', '時刻つきでも日付だけ取れる');
  ok(np.normalizeDate('2026/9/6') === '2026-09-06', 'スラッシュ・1桁でもゼロ埋めされる');
  ok(np.normalizeDate('2026-02-30') === null, '実在しない日付は null (繰り上げて別日にしない)');
  ok(np.normalizeDate('2026-13-01') === null, '13月は null');
  ok(np.normalizeDate('') === null && np.normalizeDate(null) === null, '空は null');
}

console.log('[2] 3週間の窓');
{
  ok(np.isWithinWindow(d(0), today) === true, '今日の登録は新商品');
  ok(np.isWithinWindow(d(21), today) === true, 'ちょうど21日前は含む');
  ok(np.isWithinWindow(d(22), today) === false, '22日前は含まない');
  ok(np.isWithinWindow('2099-01-01', today) === false, '未来日付は誤入力扱い (新商品にしない)');
  ok(np.isWithinWindow(null, today) === null, '読めない日付は null = 判定できない');
}

console.log('[3] 新商品の判定 (登録日 × 入庫履歴)');
{
  const m = np.buildNewProductContext(db, ['NEW-A', 'OLD-B', 'NEW-C', 'NEW-D', 'NEW-E', 'NODATE-F', 'FUTURE-G', 'MISSING-Z'], { today });
  ok(m.get('new-a').verdict === 'new', '登録3日前 + 入庫履歴なし → 新商品');
  ok(m.get('old-b').verdict === 'not_new', '登録200日前 → 新商品ではない');
  ok(m.get('new-c').verdict === 'not_new' && /最終仕入日/.test(m.get('new-c').reason), '最終仕入日があれば入庫済み');
  ok(m.get('new-d').verdict === 'not_new' && /ロジザード/.test(m.get('new-d').reason), '在庫の記録があれば入庫済み');
  ok(m.get('new-e').verdict === 'not_new' && /過去に受け入れ/.test(m.get('new-e').reason), 'このアプリの受入実績があれば入庫済み');
  ok(m.get('nodate-f').verdict === 'unknown', '登録日が空 → 判定できない (新商品ではない、にしない)');
  ok(m.get('future-g').verdict === 'not_new', '未来の登録日は新商品にしない');
  ok(m.get('missing-z').verdict === 'unknown', '商品マスタに無いコード → 判定できない');
  ok(np.judgeNewProduct(db, 'NEW-A', { today }).verdict === 'new', '1件だけの判定も同じ結果');
}

console.log('[4] 登録日を引ける表が1つも無ければ全部 unknown (取れなかったを0にしない)');
{
  const savedProd = db.prepare('SELECT * FROM mirror_products').all();
  const savedPml = db.prepare('SELECT * FROM mirror_pml_snapshot_rows').all();
  db.exec('DELETE FROM mirror_products');
  db.exec('DELETE FROM mirror_pml_snapshot_rows');
  const m = np.buildNewProductContext(db, ['NEW-A'], { today });
  ok(m.get('new-a').verdict === 'unknown', 'どちらも空なら「新商品」とも「違う」とも言わない');
  ok(/商品マスタがまだ届いていない/.test(m.get('new-a').reason), '理由が画面に出せる文になっている');
  const ins = db.prepare(`INSERT INTO mirror_products
    (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, new_product_launch_date, updated_at) VALUES (?,?,?,?,?,?,?,?)`);
  for (const r of savedProd) ins.run(r.product_id, r.商品コード, r.商品名, r.商品区分, r.取扱区分, r.原価状態, r.new_product_launch_date, r.updated_at);
  ok(np.buildNewProductContext(db, ['NEW-A'], { today }).get('new-a').verdict === 'new', '商品マスタだけでも判定できる');
  for (const r of savedPml) insPml.run(r.商品コード, r.商品名, r.最終仕入日, r.登録日);
  ok(np.buildNewProductContext(db, ['NEW-C'], { today }).get('new-c').verdict === 'not_new', '商品管理リストを戻したら最終仕入日も効く');
}

console.log('[5] 受け取る写真の検証');
{
  const tmp = path.join(process.env.DATA_DIR, 'probe');
  fs.mkdirSync(tmp, { recursive: true });
  const jpg = path.join(tmp, 'a.jpg');
  fs.writeFileSync(jpg, Buffer.concat([Buffer.from([0xFF, 0xD8, 0xFF, 0xE0]), Buffer.alloc(2048, 7)]));
  const txt = path.join(tmp, 'a.txt');
  fs.writeFileSync(txt, 'これは画像ではありません');
  const empty = path.join(tmp, 'empty.jpg');
  fs.writeFileSync(empty, '');
  ok(bl.inspectUpload({ filePath: jpg, operationId: 'op-abcdef' }).ok === true, 'JPEG は通る');
  ok(bl.inspectUpload({ filePath: txt, operationId: 'op-abcdef' }).error === 'bad_file', '中身が画像でなければ拒否 (拡張子を信じない)');
  ok(bl.inspectUpload({ filePath: empty, operationId: 'op-abcdef' }).error === 'bad_file', '空ファイルは拒否');
  ok(bl.inspectUpload({ filePath: jpg, operationId: 'ab' }).error === 'bad_request', '短すぎる送信IDは拒否');
  ok(bl.inspectUpload({ filePath: jpg, operationId: "x'; DROP--" }).error === 'bad_request', '変な文字の送信IDは拒否');
  ok(bl.sniffImage(Buffer.from([0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0, 0, 0, 0])) === 'image/png', 'PNG も受ける');
  ok(bl.sniffImage(Buffer.from('ftypheic'.padEnd(12, ' '))) === null, 'HEIC など未対応は受けない (端末で JPEG に直す)');
}

// ─── 撮影 (受け取り) ───
const makeJpeg = (name) => {
  const dir = path.join(process.env.DATA_DIR, 'incoming');
  fs.mkdirSync(dir, { recursive: true });
  const p = path.join(dir, name);
  fs.writeFileSync(p, Buffer.concat([Buffer.from([0xFF, 0xD8, 0xFF, 0xE0]), Buffer.alloc(1024, 3)]));
  return p;
};

console.log('[6] 写真の保存・冪等・上限・撮り直し');
let firstPhotoId = null;
{
  const r1 = bl.addPhoto({ codeKey: 'NEW-A', productId: 'NEW-A', productName: '新商品A (初入荷)', filePath: makeJpeg('1.jpg'), operationId: 'op-000001', worker: '中原' });
  ok(r1.ok === true && r1.photo.status === 'stored', '1枚目が保存できる (Drive を待たずに成功が返る)');
  firstPhotoId = r1.photo.id;
  ok(bl.countPhotos('new-a') === 1, '枚数が数えられる (大文字小文字は問わない)');
  const r2 = bl.addPhoto({ codeKey: 'NEW-A', productId: 'NEW-A', filePath: makeJpeg('2.jpg'), operationId: 'op-000001', worker: '中原' });
  ok(r2.ok === true && r2.already === true && bl.countPhotos('new-a') === 1, '同じ送信IDの再送は二重登録しない');
  const r3 = bl.addPhoto({ codeKey: 'OTHER-X', productId: 'OTHER-X', filePath: makeJpeg('3.jpg'), operationId: 'op-000001', worker: '中原' });
  ok(r3.ok === false && r3.error === 'operation_conflict', '同じ送信IDを別の商品に使うと拒否 (取り違え防止)');
  for (let i = 2; i <= bl.MAX_PHOTOS_PER_PRODUCT; i++) {
    bl.addPhoto({ codeKey: 'NEW-A', productId: 'NEW-A', filePath: makeJpeg(`m${i}.jpg`), operationId: `op-cap${i}0000`, worker: '中原' });
  }
  ok(bl.countPhotos('new-a') === bl.MAX_PHOTOS_PER_PRODUCT, `上限 ${bl.MAX_PHOTOS_PER_PRODUCT} 枚まで撮れる`);
  const over = bl.addPhoto({ codeKey: 'NEW-A', productId: 'NEW-A', filePath: makeJpeg('over.jpg'), operationId: 'op-over0000', worker: '中原' });
  ok(over.ok === false && over.error === 'cap_reached', '上限を超えると断る');
  ok(fs.existsSync(path.join(process.env.DATA_DIR, 'incoming', 'over.jpg')), '断ったときは実体を動かしていない (呼び出し側が片づける)');

  const del = bl.deletePhoto(firstPhotoId, { actor: 'テスト' });
  ok(del.ok === true && bl.countPhotos('new-a') === bl.MAX_PHOTOS_PER_PRODUCT - 1, '撮り直し (論理削除) で枠が空く');
  ok(db.prepare('SELECT deleted_at FROM f_inbound_check_back_labels WHERE id = ?').get(firstPhotoId).deleted_at != null, '行は消さずに印を付ける (履歴を壊さない)');
  ok(bl.photosOf('new-a').every((p) => p.id !== firstPhotoId), '消した写真は一覧に出ない');
  ok(bl.photoSource(firstPhotoId) === null, '消した写真は開けない');
}

console.log('[7] ファイル名 (人が Drive で探せる形・個人名を入れない)');
{
  const row = db.prepare('SELECT * FROM f_inbound_check_back_labels ORDER BY id LIMIT 1').get();
  const name = bl.filenameFor(row);
  ok(name.startsWith('NEW-A_裏面_'), '商品コードが先頭 (同じ商品が固まって並ぶ)');
  ok(name.endsWith('.jpg'), '拡張子が付く');
  ok(!name.includes('中原'), '撮った人の名前は入れない');
}

console.log('[8] 確認の前提条件 (ゲート)');
{
  const gNew = backLabelGate('NEW-B-FRESH');
  ok(gNew.required === false, '商品マスタに無い商品は求めない (判定できないものは止めない)');
  // 撮っていない新商品を1つ用意する
  db.prepare(`INSERT INTO mirror_products (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, new_product_launch_date, updated_at)
    VALUES (10, 'NEW-H', '新商品H (未撮影)', '単品', '取扱中', 'unknown', ?, ?)`).run(new Date(Date.now() - 2 * 86400000).toISOString().slice(0, 10), now);
  const g = backLabelGate('NEW-H');
  ok(g.new_product.verdict === 'new' && g.photos === 0 && g.required === true, '新商品で未撮影なら確認できない');
  const shot = bl.addPhoto({ codeKey: 'NEW-H', productId: 'NEW-H', filePath: makeJpeg('h.jpg'), operationId: 'op-h000000', worker: '中原' });
  ok(shot.ok === true && backLabelGate('NEW-H').required === false, '1枚撮れば確認できる');
  // 消すと戻る
  bl.deletePhoto(shot.photo.id, { actor: 'テスト' });
  ok(backLabelGate('NEW-H').required === true, '全部消すとまた撮るまで確認できない');
}

console.log('[9] 緊急停止 (カメラ故障などで入荷受付そのものが止まったとき)');
{
  const before = process.env.INBOUND_CHECK_BACK_LABEL_REQUIRED;
  process.env.INBOUND_CHECK_BACK_LABEL_REQUIRED = '0';
  ok(bl.isBackLabelRequired() === false && backLabelGate('NEW-H').required === false, 'env=0 で必須が外れる');
  ok(backLabelGate('NEW-H').new_product.verdict === 'new', '判定そのものは止めない (画面には🆕を出し続ける)');
  process.env.INBOUND_CHECK_BACK_LABEL_REQUIRED = before == null ? '' : before;
  if (before == null) delete process.env.INBOUND_CHECK_BACK_LABEL_REQUIRED;
  ok(bl.isBackLabelRequired() === true, '既定は必須 (中原さん 2026-09-18)');
}

console.log('[10] 一覧 (getState) に載る');
{
  db.prepare(`INSERT INTO f_inbound_check_batches (id, source, file_name, file_hash, csv_generated_at, row_count, slip_count, imported_at, status, work_date)
    VALUES (1, 'manual_upload', 't.csv', 'h1', ?, 2, 1, ?, 'active', date('now','+9 hours'))`).run(now, now);
  db.prepare(`INSERT INTO f_inbound_check_slips (batch_id, ar_no, line_count, seq) VALUES (1, 'AR001', 2, 1)`).run();
  const insLine = db.prepare(`INSERT INTO f_inbound_check_lines
    (batch_id, line_key, ar_no, line_no, detail_no, product_id, code_key, product_name, planned_qty, seq)
    VALUES (1, ?, 'AR001', ?, 1, ?, ?, ?, 5, ?)`);
  insLine.run('L1', 1, 'NEW-H', 'new-h', '新商品H (未撮影)', 1);
  insLine.run('L2', 2, 'OLD-B', 'old-b', '既存品B', 2);
  const insState = db.prepare(`INSERT INTO f_inbound_check_line_state (batch_id, line_key, status) VALUES (1, ?, 'unchecked')`);
  insState.run('L1'); insState.run('L2');

  const s = getState();
  const l1 = s.lines.find((l) => l.line_key === 'L1');
  const l2 = s.lines.find((l) => l.line_key === 'L2');
  ok(l1.new_product.verdict === 'new', '新商品の行に印が付く');
  ok(l1.back_label_required === true && l1.dest.missing.includes('back_label'), '確認の前に聞く項目として back_label が入る');
  ok(Array.isArray(l1.back_labels) && l1.back_labels.length === 0, '撮った写真の一覧が行に載る (まだ0枚)');
  ok(l2.new_product.verdict === 'not_new' && l2.back_label_required === false, '既存品は求めない');
  ok(!l2.dest.missing.includes('back_label'), '既存品の確認は今まで通り');

  const shot = bl.addPhoto({ codeKey: 'new-h', productId: 'NEW-H', filePath: makeJpeg('h2.jpg'), operationId: 'op-state001', worker: '中原' });
  ok(shot.ok === true, '一覧の商品を撮れる');
  const s2 = getState();
  const l1b = s2.lines.find((l) => l.line_key === 'L1');
  ok(l1b.back_labels.length === 1 && l1b.back_label_required === false, '撮ると前提条件が外れる');
  ok(!l1b.dest.missing.includes('back_label'), 'missing からも消える');
}

console.log('[11] 紐づけ先は商品コードが主キー (撮った相手は一覧が入れ替わっても変わらない)');
{
  const t = lineForBackLabel({ batchId: 1, lineKey: 'L1', productCode: 'NEW-H' });
  ok(t.ok === true && t.subject.codeKey === 'new-h' && t.subject.productName === '新商品H (未撮影)', '明細と商品コードが一致すれば商品名も伝票も残す');
  ok(t.subject.arNo === 'AR001', 'どの伝票で撮ったかの控えが付く');
  // ⭐撮っている間に一覧が入れ替わっても、撮った商品に付く (Codex R1 #3)
  const stale = lineForBackLabel({ batchId: 99, lineKey: 'L1', productCode: 'NEW-H' });
  ok(stale.ok === true && stale.subject.codeKey === 'new-h' && stale.subject.batchId === null,
    '一覧が入れ替わっていても商品コードで付く (伝票の控えは捨てる)');
  // ⭐line_key が別の商品を指していても、商品コードが勝つ = 取り違えない
  const mixed = lineForBackLabel({ batchId: 1, lineKey: 'L2', productCode: 'NEW-H' });
  ok(mixed.ok === true && mixed.subject.codeKey === 'new-h' && mixed.subject.lineKey === null,
    '明細が別の商品を指していたら控えを捨てる (別商品に付けない)');
  const byCode = lineForBackLabel({ productCode: 'old-b' });
  ok(byCode.ok === true && byCode.subject.productId === 'OLD-B', '🔍 商品からも撮れる (商品マスタの表記で保存する)');
  ok(lineForBackLabel({ productCode: 'NOT-EXIST' }).error === 'not_found', 'どこにも無いコードは not_found');
  ok(lineForBackLabel({}).error === 'bad_request', '何も無ければ bad_request');
  ok(lineForBackLabel({ batchId: 1, lineKey: 'L1' }).subject.codeKey === 'new-h', '商品コードを送らない古い呼び方でも引ける (互換)');
  ok(lineForBackLabel({ batchId: 99, lineKey: 'L1' }).error === 'stale_batch', '明細だけで古いバッチなら stale_batch');
}

console.log('[12] Drive 送信キュー');
{
  const sent = [];
  bl._setDriveUpload(async ({ localPath, filename, operationId }) => {
    sent.push({ filename, operationId });
    ok(fs.existsSync(localPath), `送るときに実体がある (${filename})`);
    return { fileId: 'drive-' + operationId, url: 'https://drive.example/' + operationId };
  });
  const pendingBefore = db.prepare("SELECT COUNT(*) c FROM f_inbound_check_back_labels WHERE status='stored' AND deleted_at IS NULL").get().c;
  const r = await bl.processBackLabelQueue();
  ok(r.ok === true && r.uploaded === pendingBefore && r.failed === 0, `待っていた ${pendingBefore} 枚を全部送った`);
  const row = db.prepare('SELECT * FROM f_inbound_check_back_labels WHERE operation_id = ?').get('op-state001');
  ok(row.status === 'uploaded' && row.drive_file_id === 'drive-op-state001', 'Drive のファイルIDが記録される');
  ok(row.local_path === null, 'Drive に届いたらローカルの実体は手放す');
  ok(bl.photoSource(row.id).kind === 'drive', '実体が無くなったら Drive から配信する');
  ok((await bl.processBackLabelQueue()).uploaded === 0, 'もう一度回しても送り直さない (冪等)');
  ok(sent.every((x) => /_裏面_/.test(x.filename)), 'ファイル名に「裏面」が入る');
}

console.log('[13] Drive が落ちているとき');
{
  const shot = bl.addPhoto({ codeKey: 'NEW-H', productId: 'NEW-H', filePath: makeJpeg('fail.jpg'), operationId: 'op-fail0001', worker: '中原' });
  ok(shot.ok === true, 'Drive が落ちていても撮影そのものは成功する (現場を止めない)');
  ok(backLabelGate('NEW-H').required === false, 'Drive 未送信でも確認できる (サーバーに写真は届いている)');
  bl._setDriveUpload(async () => { throw new Error('Drive が応答しません'); });
  const r = await bl.processBackLabelQueue();
  ok(r.failed === 1, '失敗として数える');
  let row = db.prepare('SELECT * FROM f_inbound_check_back_labels WHERE operation_id = ?').get('op-fail0001');
  ok(row.status === 'stored' && /応答しません/.test(row.error) && row.next_retry_at, '理由を残して次の再試行を予約する');
  ok(fs.existsSync(row.local_path), '失敗しても実体は消さない (次の回で送る)');
  // 再試行を使い切ると止まる
  for (let i = 0; i < 12; i++) {
    db.prepare('UPDATE f_inbound_check_back_labels SET next_retry_at = NULL WHERE operation_id = ?').run('op-fail0001');
    await bl.processBackLabelQueue();
  }
  row = db.prepare('SELECT * FROM f_inbound_check_back_labels WHERE operation_id = ?').get('op-fail0001');
  ok(row.next_retry_at === '9999-12-31T00:00:00.000Z', '使い切ったら自動再試行を止める (Drive を叩き続けない)');
  const st = bl.backLabelStatus();
  ok(st.blocked === 1 && st.failing.length === 1, '管理画面に止まっている件数と理由が出る');

  // 管理画面の「もう一度送る」で解除して送り直せる
  bl._setDriveUpload(async ({ operationId }) => ({ fileId: 'ok-' + operationId, url: 'https://drive.example/ok' }));
  const reset = bl.resetBackLabelQueue();
  ok(reset === 1, '止まっていた行を解除できる');
  await bl.processBackLabelQueue();
  row = db.prepare('SELECT * FROM f_inbound_check_back_labels WHERE operation_id = ?').get('op-fail0001');
  ok(row.status === 'uploaded' && row.error === null, '原因を直したら送れる');
  bl._setDriveUpload(null);
}

console.log('[14] 実体が消えた写真 (再起動など)');
{
  const shot = bl.addPhoto({ codeKey: 'NEW-H', productId: 'NEW-H', filePath: makeJpeg('gone.jpg'), operationId: 'op-gone0001', worker: '中原' });
  const row0 = db.prepare('SELECT * FROM f_inbound_check_back_labels WHERE operation_id = ?').get('op-gone0001');
  fs.unlinkSync(row0.local_path);
  bl._setDriveUpload(async () => { throw new Error('ここには来ないはず'); });
  const r = await bl.processBackLabelQueue();
  ok(r.missing === 1 && r.failed === 0, '実体が無く Drive にも無い行は「撮り直しが要る」として印を付ける (再試行もしない)');
  const row = db.prepare('SELECT * FROM f_inbound_check_back_labels WHERE operation_id = ?').get('op-gone0001');
  ok(/実体ファイルがありません/.test(row.error), '理由が「撮り直してください」と分かる文になっている');
  ok(bl.photoSource(shot.photo.id) === null, '開けない写真は配信もしない');
  bl._setDriveUpload(null);
}

console.log('[15] 実体を失った写真は「撮ってある」に数えない (Codex R1 #2)');
{
  // まだ1枚も撮っていない新商品を用意する (NEW-H は前のテストで Drive に上がった写真が残っている)
  db.prepare(`INSERT INTO mirror_products (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, new_product_launch_date, updated_at)
    VALUES (30, 'NEW-L', '新商品L (実体を失う)', '単品', '取扱中', 'unknown', ?, ?)`).run(d(2), now);
  ok(backLabelGate('NEW-L').required === true, '撮る前は確認できない');
  const shot = bl.addPhoto({ codeKey: 'NEW-L', productId: 'NEW-L', filePath: makeJpeg('lost.jpg'), operationId: 'op-lost0001', worker: '中原' });
  ok(shot.ok === true && backLabelGate('NEW-L').required === false, '撮った直後は確認できる');
  const row = db.prepare('SELECT * FROM f_inbound_check_back_labels WHERE operation_id = ?').get('op-lost0001');
  fs.unlinkSync(row.local_path);                 // 再起動で DATA_DIR が飛んだ状況
  bl._setDriveUpload(async () => { throw new Error('ここには来ないはず'); });
  await bl.processBackLabelQueue();
  bl._setDriveUpload(null);
  ok(db.prepare('SELECT missing_file_at FROM f_inbound_check_back_labels WHERE id = ?').get(row.id).missing_file_at != null,
    '実体が無い写真に印が付く');
  ok(bl.photosOf('new-l').length === 0, '見られない写真は一覧に出ない');
  ok(backLabelGate('NEW-L').required === true, 'また撮るまで確認できない (見られない写真でゲートを通さない)');
  ok(bl.backLabelStatus().missing >= 1, '管理画面に「撮り直しが要る」件数が出る');
  ok(bl.resetBackLabelQueue() === 0, '実体が無い行は「もう一度送る」で解除しない (送るものが無い)');
}

console.log('[16] 消された写真への再送は成功にしない (Codex R1 #7)');
{
  const shot = bl.addPhoto({ codeKey: 'NEW-H', productId: 'NEW-H', filePath: makeJpeg('dup.jpg'), operationId: 'op-dup00001', worker: '中原' });
  ok(shot.ok === true, '1枚保存');
  bl.deletePhoto(shot.photo.id, { actor: 'テスト' });
  const again = bl.addPhoto({ codeKey: 'NEW-H', productId: 'NEW-H', filePath: makeJpeg('dup2.jpg'), operationId: 'op-dup00001', worker: '中原' });
  ok(again.ok === false && again.error === 'gone', '消された送信IDは「もう一度送って」と返す (端末が写真を捨てない)');
}

console.log('[17] 実数0で確定した受入は「入庫済み」の証拠にしない (Codex R1 #1)');
{
  db.prepare(`INSERT INTO mirror_products (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, new_product_launch_date, updated_at)
    VALUES (20, 'NEW-Z', '新商品Z', '単品', '取扱中', 'unknown', ?, ?)`).run(d(2), now);
  db.prepare(`INSERT INTO f_inbound_check_destinations
    (batch_id, line_key, ar_no, product_id, product_name, planned_qty, actual_qty, destination, decided_from, worker, decided_at)
    VALUES (0, 'Z0', 'AR000', 'NEW-Z', '新商品Z', 5, 0, 'bfaith', 'master', 'テスト', ?)`).run(now);
  ok(np.judgeNewProduct(db, 'NEW-Z', { today }).verdict === 'new',
    '「これ以上来ない — 不足5個」で閉じた行があっても、現物は来ていないので新商品のまま');
  db.prepare(`INSERT INTO f_inbound_check_destinations
    (batch_id, line_key, ar_no, product_id, product_name, planned_qty, actual_qty, destination, decided_from, worker, decided_at)
    VALUES (0, 'Z1', 'AR000', 'NEW-Z', '新商品Z', 5, 5, 'bfaith', 'master', 'テスト', ?)`).run(now);
  ok(np.judgeNewProduct(db, 'NEW-Z', { today }).verdict === 'not_new', '実際に受け入れた行があれば入庫済み');
}

console.log('[18] 登録日は商品管理リストの 登録日 が正本 (手動の発売日で判定しない — Codex R1 #4)');
{
  // mirror_products 側だけ「発売日」を古く手で設定した商品。PML には NE の 作成日 がそのまま入る
  db.prepare(`INSERT INTO mirror_products (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, new_product_launch_date, updated_at)
    VALUES (21, 'NEW-M', '新商品M (発売日を手で設定)', '単品', '取扱中', 'unknown', '2020-01-01', ?)`).run(now);
  insPml.run('NEW-M', '新商品M (発売日を手で設定)', '', d(2));
  const m = np.buildNewProductContext(db, ['NEW-M'], { today });
  ok(m.get('new-m').verdict === 'new' && m.get('new-m').launch_date === d(2),
    'PML の 登録日 (NE 作成日) で判定する (手で入れた 2020-01-01 では判定しない)');
  // PML に無い商品は商品マスタの値で判定する (今日 NE に登録された商品など)
  db.prepare(`INSERT INTO mirror_products (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, new_product_launch_date, updated_at)
    VALUES (22, 'NEW-P', '新商品P (PMLにまだ無い)', '単品', '取扱中', 'unknown', ?, ?)`).run(d(1), now);
  ok(np.judgeNewProduct(db, 'NEW-P', { today }).verdict === 'new', 'PML にまだ載っていない商品は商品マスタの値で拾う');
}

console.log('[19] 実体を失っても Drive に届いていれば拾い直す (Codex R2 #2)');
{
  db.prepare(`INSERT INTO mirror_products (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, new_product_launch_date, updated_at)
    VALUES (40, 'NEW-R', '新商品R', '単品', '取扱中', 'unknown', ?, ?)`).run(d(2), now);
  const shot = bl.addPhoto({ codeKey: 'NEW-R', productId: 'NEW-R', filePath: makeJpeg('recover.jpg'), operationId: 'op-recov001', worker: '中原' });
  ok(shot.ok === true, '1枚保存');
  const row0 = db.prepare('SELECT * FROM f_inbound_check_back_labels WHERE operation_id = ?').get('op-recov001');
  fs.unlinkSync(row0.local_path);                       // 実体は消えたが…
  bl._setDriveFind(async ({ operationId }) => ({ fileId: 'drv-' + operationId, url: 'https://drive.example/r' }));
  const r = await bl.processBackLabelQueue();
  bl._setDriveFind(async () => null);
  ok(r.recovered >= 1 && r.missing === 0, '実は Drive に上がっていた写真を拾い直す (撮り直しを求めない)');
  const row = db.prepare('SELECT * FROM f_inbound_check_back_labels WHERE id = ?').get(row0.id);
  ok(row.status === 'uploaded' && row.drive_file_id === 'drv-op-recov001' && row.missing_file_at === null, 'uploaded として記録し直す');
  ok(bl.photoSource(row0.id).kind === 'drive', '拾い直した写真は Drive から配信できる');
}

console.log('[20] Drive に聞けなかったときは「無い」と決めつけない (Codex R2 #2)');
{
  db.prepare(`INSERT INTO mirror_products (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, new_product_launch_date, updated_at)
    VALUES (41, 'NEW-S', '新商品S', '単品', '取扱中', 'unknown', ?, ?)`).run(d(2), now);
  const shot = bl.addPhoto({ codeKey: 'NEW-S', productId: 'NEW-S', filePath: makeJpeg('ask.jpg'), operationId: 'op-ask00001', worker: '中原' });
  ok(shot.ok === true, '1枚保存');
  const row0 = db.prepare('SELECT * FROM f_inbound_check_back_labels WHERE operation_id = ?').get('op-ask00001');
  fs.unlinkSync(row0.local_path);
  bl._setDriveFind(async () => { throw new Error('Drive が応答しません'); });
  const r = await bl.processBackLabelQueue();
  bl._setDriveFind(async () => null);
  ok(r.missing === 0 && r.failed >= 1, '聞けなかった回は印を付けない (失敗として数える)');
  const row = db.prepare('SELECT * FROM f_inbound_check_back_labels WHERE id = ?').get(row0.id);
  ok(row.missing_file_at === null && /確認できませんでした/.test(row.error || ''), '理由を残して次の回に持ち越す');
  // 次の回で「無い」と分かったら印を付ける (再試行の時刻が来たことにする)
  db.prepare('UPDATE f_inbound_check_back_labels SET next_retry_at = NULL WHERE id = ?').run(row0.id);
  await bl.processBackLabelQueue();
  ok(db.prepare('SELECT missing_file_at FROM f_inbound_check_back_labels WHERE id = ?').get(row0.id).missing_file_at != null,
    'Drive に無いと分かった回に印を付ける');
}

console.log('[21] 見回りより先に同じ送信IDが再送されても、実体が無ければ成功にしない (Codex R2 #3)');
{
  db.prepare(`INSERT INTO mirror_products (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, new_product_launch_date, updated_at)
    VALUES (31, 'NEW-Q', '新商品Q', '単品', '取扱中', 'unknown', ?, ?)`).run(d(2), now);
  const shot = bl.addPhoto({ codeKey: 'NEW-Q', productId: 'NEW-Q', filePath: makeJpeg('race.jpg'), operationId: 'op-race0001', worker: '中原' });
  const row = db.prepare('SELECT * FROM f_inbound_check_back_labels WHERE operation_id = ?').get('op-race0001');
  fs.unlinkSync(row.local_path);                        // キューが回る前に実体が消えた
  const again = bl.addPhoto({ codeKey: 'NEW-Q', productId: 'NEW-Q', filePath: makeJpeg('race2.jpg'), operationId: 'op-race0001', worker: '中原' });
  ok(again.ok === false && again.error === 'gone', '「もう入っています」と返さない (端末が写真を捨てない)');
  ok(backLabelGate('NEW-Q').required === true, '確認の直前にも実体を確かめるので、撮るまで確認できない');
}

console.log('[22] 巡回は通常の送信を待たせない (Codex R3 P1)');
{
  db.prepare(`INSERT INTO mirror_products (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, new_product_launch_date, updated_at)
    VALUES (50, 'NEW-T', '新商品T', '単品', '取扱中', 'unknown', ?, ?)`).run(d(2), now);
  // 実体を失った行を 8 件ぶん作る (1商品4枚までなので商品を分ける)
  const lost = [];
  for (let i = 0; i < 8; i++) {
    const code = `NEW-T${i}`;
    db.prepare(`INSERT INTO mirror_products (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, new_product_launch_date, updated_at)
      VALUES (?, ?, ?, '単品', '取扱中', 'unknown', ?, ?)`).run(60 + i, code, `新商品T${i}`, d(2), now);
    const r = bl.addPhoto({ codeKey: code, productId: code, filePath: makeJpeg(`many${i}.jpg`), operationId: `op-many000${i}`, worker: '中原' });
    if (!r.ok) { ok(false, `種まきに失敗 (${r.error})`); break; }
    const row = db.prepare('SELECT * FROM f_inbound_check_back_labels WHERE operation_id = ?').get(`op-many000${i}`);
    fs.unlinkSync(row.local_path);
    lost.push(row.id);
  }
  ok(lost.length === 8, '実体を失った行を8件用意した');
  let asked = 0;
  bl._setDriveFind(async () => { asked++; return null; });
  await bl.processBackLabelQueue();
  bl._setDriveFind(async () => null);
  ok(asked <= 5, `1回の巡回で Drive に聞くのは 5 件まで (実際 ${asked} 件) — 通常の送信を待たせない`);
  // 印が付いた行はすぐには聞き直さない (1時間おき)
  let asked2 = 0;
  bl._setDriveFind(async () => { asked2++; return null; });
  await bl.processBackLabelQueue();
  bl._setDriveFind(async () => null);
  ok(asked2 <= 5, '2回目も上限を守る');
  const marked = db.prepare(`SELECT COUNT(*) c FROM f_inbound_check_back_labels
    WHERE id IN (${lost.map(() => '?').join(',')}) AND missing_file_at IS NOT NULL`).get(...lost).c;
  ok(marked > 0 && marked < lost.length + 1, '印は少しずつ付く (全部を一度に処理しようとしない)');
  const future = db.prepare(`SELECT COUNT(*) c FROM f_inbound_check_back_labels
    WHERE id IN (${lost.map(() => '?').join(',')}) AND missing_file_at IS NOT NULL AND next_retry_at > ?`).get(...lost, now).c;
  ok(future > 0, '印を付けた行は次に聞き直す時刻を先に置く (毎回聞き直さない)');
}

console.log('[23] 再送で置き換えた旧行は Drive から拾い直しても二重にしない (Codex R3 P2)');
{
  db.prepare(`INSERT INTO mirror_products (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, new_product_launch_date, updated_at)
    VALUES (51, 'NEW-U', '新商品U', '単品', '取扱中', 'unknown', ?, ?)`).run(d(2), now);
  const first = bl.addPhoto({ codeKey: 'NEW-U', productId: 'NEW-U', filePath: makeJpeg('u1.jpg'), operationId: 'op-u0000001', worker: '中原' });
  const oldRow = db.prepare('SELECT * FROM f_inbound_check_back_labels WHERE operation_id = ?').get('op-u0000001');
  fs.unlinkSync(oldRow.local_path);                       // 実体は消えたが Drive には届いていた
  const again = bl.addPhoto({ codeKey: 'NEW-U', productId: 'NEW-U', filePath: makeJpeg('u2.jpg'), operationId: 'op-u0000001', worker: '中原' });
  ok(again.error === 'gone', '同じ送信IDの再送は gone');
  ok(db.prepare('SELECT deleted_at FROM f_inbound_check_back_labels WHERE id = ?').get(oldRow.id).deleted_at != null,
    '端末が送り直すので、旧行はその場で退ける');
  // 端末が新しい送信IDで送り直す
  const resent = bl.addPhoto({ codeKey: 'NEW-U', productId: 'NEW-U', filePath: makeJpeg('u3.jpg'), operationId: 'op-u0000002', worker: '中原' });
  ok(resent.ok === true, '新しい送信IDでは入る');
  // その後、巡回が「旧行は Drive にあった」と気づいても二重にしない
  bl._setDriveFind(async ({ operationId }) => ({ fileId: 'drv-' + operationId, url: 'https://drive.example/u' }));
  await bl.processBackLabelQueue();
  bl._setDriveFind(async () => null);
  ok(bl.countPhotos('new-u') === 1, '有効な写真は1枚のまま (同じ写真が2枚にならない)');
  ok(bl.photosOf('new-u').every((x) => x.id !== oldRow.id), '退けた旧行は一覧にも出ない');
}

console.log(`\n${fail === 0 ? '✅' : '❌'} PASS ${pass} / FAIL ${fail}`);
process.exit(fail === 0 ? 0 : 1);
