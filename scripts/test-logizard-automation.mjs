/**
 * test-logizard-automation.mjs — ロジザードの自動化の正本 (tools/logizard-automation。マスタ正本切替 ③c-1b-0)
 *
 * 固定する契約 (設計 = AI_reference CompanyDB構想/10 §6.3「③c-1b 契約 v3」H8・L-10):
 *   1 書き出しの検証 (shohin-export.js validateShohinCsv) は auto-shohin-csv.js v0.1 の validateCsv と同じ
 *   2 auto-shohin-csv.js は書き出しの手順と検証を shohin-export.js から使う (同じ手順を 2 つ持たない)
 *   3 写し方 (deploy.mjs): 見るだけ / 写す (前のファイルを残す・DEPLOYED.json) / 同じなら何もしない / 動いている間は断る /
 *     未コミットは断る / ずれの検出 / 戻す / 途中の失敗は戻す
 *   4 .bat は CRLF・.js は LF (各 PC の今のファイルと同じバイト)
 *   5 入荷バーコード連携 (auto-barcode.js・③c-1b-3a): JST 00:00〜01:30 は動かない (CSV・鍵・ブラウザの前 / 各ステップと実行ボタンの前) /
 *     切替の PR (L-23) から ①② だけ (③ の CSV を見ない・前の設定 LOGIZARD_BC_DAILY が残っていても ③ はしない) / 引数は --dry だけ
 *   6 バーコードの書き出し (barcode-export.js・③c-1b-2b K4): 検証は印つきの例外・全件の条件・承認は「エクスポート処理を行います」だけ・② の出力には書かない
 * 使い方: node scripts/test-logizard-automation.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import { spawnSync } from 'node:child_process';
import { fileURLToPath, pathToFileURL } from 'node:url';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const TOOL = path.join(ROOT, 'tools', 'logizard-automation');
const { default: iconv } = await import('iconv-lite');
const E = await import('../tools/logizard-automation/shohin-export.js');
const DEP = await import('../tools/logizard-automation/deploy.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const sj = (s) => iconv.encode(s, 'cp932');
const sha = (b) => crypto.createHash('sha256').update(b).digest('hex');

console.log('test-logizard-automation');

await ta('[1] 書き出しの検証: HTML・Shift-JIS でない・必須列が無い・列の数違い・少なすぎる = 使わない / 有効期限区分の内訳', async () => {
  const csv = (rows) => sj(['"商品ID","商品名","有効期限区分"', ...rows].join('\r\n'));
  let v = E.validateShohinCsv(csv(['"A-1","商品A","管理する"', '"B-2","商品B",""']), { minRows: 2 });
  assert.deepEqual([v.ok, v.dataRows, v.kubunCounts], [true, 2, { '管理する': 1, '(空欄)': 1 }]);
  assert.match(E.validateShohinCsv(Buffer.from('<html><body>login</body></html>'), { minRows: 1 }).reason, /HTML/);
  assert.match(E.validateShohinCsv(Buffer.from([0x22, 0x81, 0x22]), { minRows: 1 }).reason, /Shift-JIS/);
  assert.match(E.validateShohinCsv(sj('"商品ID","商品名"\r\n"A","x"'), { minRows: 1 }).reason, /必須列/);
  assert.match(E.validateShohinCsv(csv(['"A-1","商品A"']), { minRows: 1 }).reason, /列数が違います/);
  assert.match(E.validateShohinCsv(csv(['"A-1","商品A",""']), { minRows: 2 }).reason, /少なすぎます/);
  assert.match(E.validateShohinCsv(csv(['"A-1","商品A",""'])).reason, /下限 100/);   // 既定の下限 = 前と同じ 100
  assert.equal(E.validateShohinCsv(Buffer.alloc(0)).reason, '中身が空です');
  assert.equal(E.jstTodaySlash(new Date('2030-01-14T15:30:00Z')), '2030/01/15');   // JST の日付・画面の表記
});

await ta('[2] auto-shohin-csv.js は書き出しの手順と検証を shohin-export.js から使う (同じ手順を 2 か所に持たない)', async () => {
  const s = fs.readFileSync(path.join(TOOL, 'auto-shohin-csv.js'), 'utf8');
  assert.match(s, /import \{ exportShohinMaster, validateShohinCsv \} from '\.\/shohin-export\.js';/);
  assert.match(s, /exportShohinMaster\(page, \{ dlDir: DL_DIR, minRows: MIN_ROWS, dry: DRY, log \}\)/);
  for (const gone of ['function validateCsv', 'function dismissNotice', "openFunctionBar('FM08_01')", 'FM08_01_executeBtn', 'waitForEvent(\'download\'']) assert.ok(!s.includes(gone), gone);
  // 保存先・前回の半分・Drive への転送・その日の成功の印は今までどおり auto-shohin-csv.js
  for (const kept of ['v.dataRows * 2 < prev.dataRows', "execFileSync(RCLONE_EXE, ['copyto'", 'markRanToday();', "acquireLock({ name: 'logizard-session.lock' })"]) assert.ok(s.includes(kept), kept);
  assert.ok(s.includes("'C:\\\\tools\\\\rclone\\\\rclone.exe'"), 'rclone の既定の場所 (バックスラッシュが 2 つ)');
  // 確かめ用の書き出しは本番の保存先・Drive・成功の印に触らない
  const t = fs.readFileSync(path.join(TOOL, 'export-shohin-to.js'), 'utf8');
  for (const gone of ['RCLONE', 'markRanToday', 'shohin-last-success']) assert.ok(!t.includes(gone), gone);
  for (const kept of ["flag: 'wx'", '本番の保存先には書かない', "acquireLock({ name: 'logizard-session.lock' })", 'exportShohinMaster(page']) assert.ok(t.includes(kept), kept);
  const x = fs.readFileSync(path.join(TOOL, 'shohin-export.js'), 'utf8');
  for (const kept of ["SHOHIN_FILE_ID = '5'", "fill('#FM08_01_BR010_fromTargetDate', '')", "check('#FM08_01_BR010_expStatus2')", "fill('#FM08_01_fileName', 'shohin_master')", 'エクスポート処理を行います']) assert.ok(x.includes(kept), kept);
});

await ta('[3] 写し方: 見るだけ → 写す (前を残す・DEPLOYED.json・sha256) → 同じなら何もしない → ずれの検出 → 戻す', async () => {
  const src = fs.mkdtempSync(path.join(os.tmpdir(), 'lza-src-')), tgt = fs.mkdtempSync(path.join(os.tmpdir(), 'lza-tgt-'));
  fs.writeFileSync(path.join(src, 'manifest.json'), JSON.stringify({ version: 1, pcs: { minipc: ['a.js', 'b.bat', 'c.js'] } }));
  fs.writeFileSync(path.join(src, 'a.js'), 'new a\n'); fs.writeFileSync(path.join(src, 'b.bat'), 'same\r\n'); fs.writeFileSync(path.join(src, 'c.js'), 'new c\n');
  fs.writeFileSync(path.join(tgt, 'a.js'), 'old a\n'); fs.writeFileSync(path.join(tgt, 'b.bat'), 'same\r\n'); fs.writeFileSync(path.join(tgt, '.env'), 'SECRET=x\n');
  const git = () => ({ commit: 'c0ffee00c0ffee00', dirty: false });
  const run = (o) => DEP.deploy({ srcDir: src, target: tgt, pc: 'minipc', git, now: new Date('2030-01-15T01:00:00Z'), ...o });
  let r = run({ action: 'plan' });
  assert.deepEqual(r.plan.map((x) => [x.name, x.status]), [['a.js', 'changed'], ['b.bat', 'same'], ['c.js', 'new']]);
  assert.equal(fs.readFileSync(path.join(tgt, 'a.js'), 'utf8'), 'old a\n');   // 見るだけ = 変えない
  r = run({ action: 'apply' });
  assert.deepEqual([r.ok, r.replaced, r.added], [true, ['a.js'], ['c.js']]);
  assert.deepEqual([fs.readFileSync(path.join(tgt, 'a.js'), 'utf8'), fs.readFileSync(path.join(tgt, 'c.js'), 'utf8'), fs.readFileSync(path.join(tgt, '.env'), 'utf8')], ['new a\n', 'new c\n', 'SECRET=x\n']);
  assert.equal(fs.readFileSync(path.join(tgt, 'deploy-backup', r.deployId, 'a.js'), 'utf8'), 'old a\n');   // 前のファイルを残す
  const rec = JSON.parse(fs.readFileSync(path.join(tgt, 'DEPLOYED.json'), 'utf8'));
  assert.deepEqual([rec.commit, rec.pc, rec.files['a.js'], rec.files['b.bat']], ['c0ffee00c0ffee00', 'minipc', sha(Buffer.from('new a\n')), sha(Buffer.from('same\r\n'))]);
  assert.equal(run({ action: 'check' }).ok, true);
  assert.equal(run({ action: 'apply' }).unchanged, true);   // 同じコミット・同じ中身 = 何もしない
  // 写した後に写す先で直された = ずれ
  fs.writeFileSync(path.join(tgt, 'a.js'), 'hand edit\n');
  let c = run({ action: 'check' });
  assert.deepEqual([c.ok, c.drift, c.outdated], [false, ['a.js'], ['a.js']]);
  fs.writeFileSync(path.join(tgt, 'a.js'), 'new a\n');
  // リポジトリのほうが新しい
  fs.writeFileSync(path.join(src, 'c.js'), 'newer c\n');
  c = run({ action: 'check' });
  assert.deepEqual([c.ok, c.drift, c.outdated, c.reason], [false, [], ['c.js'], 'リポジトリのほうが新しい']);
  fs.writeFileSync(path.join(src, 'c.js'), 'new c\n');
  // 戻す = 替えたものを前に・足したものを消す・DEPLOYED.json も前に (初回 = 無い)
  r = run({ action: 'rollback', rollbackId: r.deployId });
  assert.deepEqual([r.ok, fs.readFileSync(path.join(tgt, 'a.js'), 'utf8'), fs.existsSync(path.join(tgt, 'c.js')), fs.existsSync(path.join(tgt, 'DEPLOYED.json'))], [true, 'old a\n', false, false]);
  assert.equal(run({ action: 'rollback', rollbackId: 'dep_x' }).ok, false);
  assert.equal(run({ action: 'check' }).reason, 'まだ写していない (DEPLOYED.json が無い)');
});

await ta('[4] 写し方: 動いている間 (セッションの鍵がある)・未コミット・manifest に無い PC・写す先が無い = 断る / 途中で失敗 = 戻す', async () => {
  const src = fs.mkdtempSync(path.join(os.tmpdir(), 'lza-src-')), tgt = fs.mkdtempSync(path.join(os.tmpdir(), 'lza-tgt-'));
  fs.writeFileSync(path.join(src, 'manifest.json'), JSON.stringify({ version: 1, pcs: { minipc: ['a.js', 'b.js'] } }));
  fs.writeFileSync(path.join(src, 'a.js'), 'new a\n'); fs.writeFileSync(path.join(src, 'b.js'), 'new b\n');
  fs.writeFileSync(path.join(tgt, 'a.js'), 'old a\n'); fs.writeFileSync(path.join(tgt, 'b.js'), 'old b\n');
  const git = () => ({ commit: 'c0ffee00', dirty: false });
  const run = (o) => DEP.deploy({ srcDir: src, target: tgt, pc: 'minipc', git, ...o });
  fs.mkdirSync(path.join(tgt, 'logs'));
  fs.writeFileSync(path.join(tgt, 'logs', 'logizard-session.lock'), '{}');
  let r = run({ action: 'apply' });
  assert.deepEqual([r.ok, /動いている/.test(r.reason)], [false, true]);
  assert.equal(fs.readFileSync(path.join(tgt, 'a.js'), 'utf8'), 'old a\n');
  fs.unlinkSync(path.join(tgt, 'logs', 'logizard-session.lock'));
  fs.writeFileSync(path.join(tgt, 'logs', 'logizard-barcode.lock'), '{}');
  assert.equal(run({ action: 'apply' }).ok, false);
  fs.unlinkSync(path.join(tgt, 'logs', 'logizard-barcode.lock'));
  assert.match(run({ action: 'apply', git: () => ({ commit: 'x', dirty: true }) }).reason, /未コミット/);
  assert.match(DEP.deploy({ srcDir: src, target: tgt, pc: 'streamdeck', git, action: 'plan' }).reason, /PC「streamdeck」が無い/);
  assert.match(DEP.deploy({ srcDir: src, target: path.join(tgt, 'nothing'), pc: 'minipc', git, action: 'plan' }).reason, /写す先が無い/);
  // 2 つ目を写した後の読み直しが合わない = それまでに替えた a.js も戻す
  r = run({ action: 'apply', hooks: { readBack: (file) => (path.basename(file) === 'b.js' ? Buffer.from('broken') : fs.readFileSync(file)) } });
  assert.deepEqual([r.ok, /途中で失敗して戻した/.test(r.reason)], [false, true]);
  assert.deepEqual([fs.readFileSync(path.join(tgt, 'a.js'), 'utf8'), fs.readFileSync(path.join(tgt, 'b.js'), 'utf8'), fs.existsSync(path.join(tgt, 'DEPLOYED.json'))], ['old a\n', 'old b\n', false]);
});

await ta('[4b] 写す・戻すあいだは鍵を自分で持つ / 戻しきれなかったものを名前と理由で返す / 戻せるのはいちばん新しい回だけ・写した後に直されていたら戻さない (Codex #1512 R1)', async () => {
  const src = fs.mkdtempSync(path.join(os.tmpdir(), 'lza-src-')), tgt = fs.mkdtempSync(path.join(os.tmpdir(), 'lza-tgt-'));
  fs.writeFileSync(path.join(src, 'manifest.json'), JSON.stringify({ version: 1, pcs: { minipc: ['a.js', 'b.js'] } }));
  fs.writeFileSync(path.join(src, 'a.js'), 'a1\n'); fs.writeFileSync(path.join(src, 'b.js'), 'b0\n');
  fs.writeFileSync(path.join(tgt, 'a.js'), 'a0\n'); fs.writeFileSync(path.join(tgt, 'b.js'), 'b0\n');
  let commit = 'c1';
  const run = (o) => DEP.deploy({ srcDir: src, target: tgt, pc: 'minipc', git: () => ({ commit, dirty: false }), ...o });
  const lockPaths = DEP.RUNNING_LOCKS.map((n) => path.join(tgt, 'logs', n));
  // 写すあいだ = 鍵が 2 つとも deploy.mjs のもの。終わったら消える
  const seen = [];
  let r = run({ action: 'apply', hooks: { readBack: (file) => { seen.push(lockPaths.map((p) => { try { return JSON.parse(fs.readFileSync(p, 'utf8')).by; } catch { return null; } })); return fs.readFileSync(file); } } });
  assert.equal(r.ok, true, r.reason);
  assert.deepEqual(seen, [['deploy.mjs', 'deploy.mjs']]);
  assert.deepEqual(lockPaths.map((p) => fs.existsSync(p)), [false, false]);
  const depA = r.deployId;
  // 2 回目 (B): a.js と b.js を替える
  commit = 'c2';
  fs.writeFileSync(path.join(src, 'a.js'), 'a2\n'); fs.writeFileSync(path.join(src, 'b.js'), 'b2\n');
  r = run({ action: 'apply', now: new Date(Date.now() + 1000) });
  assert.equal(r.ok, true, r.reason);
  const depB = r.deployId;
  // 古い回 (A) は戻さない = 戻すと b.js だけ B のまま混ざる
  r = run({ action: 'rollback', rollbackId: depA });
  assert.deepEqual([r.ok, /いちばん新しい回だけ/.test(r.reason)], [false, true]);
  assert.deepEqual([fs.readFileSync(path.join(tgt, 'a.js'), 'utf8'), fs.readFileSync(path.join(tgt, 'b.js'), 'utf8')], ['a2\n', 'b2\n']);
  // 写した後に直された = 戻さない
  fs.writeFileSync(path.join(tgt, 'b.js'), 'hand\n');
  r = run({ action: 'rollback', rollbackId: depB });
  assert.deepEqual([r.ok, r.drift], [false, ['b.js']]);
  fs.writeFileSync(path.join(tgt, 'b.js'), 'b2\n');
  // B → A の順に 1 回ずつなら戻せる
  r = run({ action: 'rollback', rollbackId: depB });
  assert.equal(r.ok, true, r.reason);
  assert.deepEqual([fs.readFileSync(path.join(tgt, 'a.js'), 'utf8'), fs.readFileSync(path.join(tgt, 'b.js'), 'utf8'), JSON.parse(fs.readFileSync(path.join(tgt, 'DEPLOYED.json'), 'utf8')).deploy_id], ['a1\n', 'b0\n', depA]);
  r = run({ action: 'rollback', rollbackId: depA });
  assert.equal(r.ok, true, r.reason);
  assert.deepEqual([fs.readFileSync(path.join(tgt, 'a.js'), 'utf8'), fs.existsSync(path.join(tgt, 'DEPLOYED.json'))], ['a0\n', false]);
  assert.deepEqual(lockPaths.map((p) => fs.existsSync(p)), [false, false]);
  // 途中の失敗 + 戻すのも失敗 = 「戻しきれなかった」と名前・理由・前のファイルの場所
  commit = 'c3';
  fs.writeFileSync(path.join(src, 'a.js'), 'a3\n'); fs.writeFileSync(path.join(src, 'b.js'), 'b3\n');
  let writes = 0;
  const realWrite = (file, buf) => { const tmp = `${file}.t`; fs.writeFileSync(tmp, buf); fs.renameSync(tmp, file); };
  r = run({ action: 'apply', hooks: {
    readBack: (file) => (path.basename(file) === 'b.js' ? Buffer.from('broken') : fs.readFileSync(file)),
    writeAtomic: (file, buf) => { writes++; if (path.basename(file) === 'a.js' && writes > 1) throw new Error('disk full'); realWrite(file, buf); },
  } });
  assert.deepEqual([r.ok, /戻しきれなかった/.test(r.reason), r.restore_failed.map((x) => [x.name, x.error])], [false, true, [['a.js', 'disk full']]]);
  assert.ok(r.backup && fs.existsSync(path.join(r.backup, 'a.js')), '前のファイルの場所');
  assert.deepEqual(lockPaths.map((p) => fs.existsSync(p)), [false, false]);   // 失敗しても鍵は返す
  // 「途中で失敗した回」の記録が残る → その間は次を写さない → 原因を直して同じ実行 ID で戻せる (Codex #1512 R3)
  const failedId = r.deployId;
  let rec = JSON.parse(fs.readFileSync(path.join(tgt, 'DEPLOYED.json'), 'utf8'));
  assert.deepEqual([rec.deploy_id, rec.state, /disk full|写した後の中身が違う/.test(rec.error)], [failedId, 'failed_partial', true]);
  assert.match(r.reason, new RegExp(`--rollback ${failedId}`));
  r = run({ action: 'apply' });
  assert.deepEqual([r.ok, /途中で失敗したまま/.test(r.reason)], [false, true]);
  assert.equal(fs.readFileSync(path.join(tgt, 'a.js'), 'utf8'), 'a3\n');   // 混ざったまま (a.js 新・b.js 旧)
  r = run({ action: 'rollback', rollbackId: failedId });
  assert.equal(r.ok, true, r.reason);
  assert.deepEqual([fs.readFileSync(path.join(tgt, 'a.js'), 'utf8'), fs.readFileSync(path.join(tgt, 'b.js'), 'utf8'), fs.existsSync(path.join(tgt, 'DEPLOYED.json'))], ['a0\n', 'b0\n', false]);
  // 戻す書き込みが黙って違う中身を書いた = 読み直して「戻しきれなかった」
  writes = 0;
  r = run({ action: 'apply', hooks: {
    readBack: (file) => (path.basename(file) === 'b.js' ? Buffer.from('broken') : fs.readFileSync(file)),
    writeAtomic: (file, buf) => { writes++; realWrite(file, path.basename(file) === 'a.js' && writes > 1 ? Buffer.from('garbage') : buf); },
  } });
  assert.deepEqual([r.ok, r.restore_failed.map((x) => [x.name, x.error])], [false, [['a.js', '戻した後の中身が違う']]]);
  // 鍵の片方がすでにある = 断る・もう片方も作らない (残さない)
  fs.mkdirSync(path.join(tgt, 'logs'), { recursive: true });
  fs.writeFileSync(lockPaths[1], '{"pid":1}');
  r = run({ action: 'rollback', rollbackId: 'dep_x' });
  assert.deepEqual([r.ok, /動いている/.test(r.reason), fs.existsSync(lockPaths[0]), fs.readFileSync(lockPaths[1], 'utf8')], [false, true, false, '{"pid":1}']);
});

await ta('[4c] 戻す途中で替えたものが戻らない = 足した部品は消さずに残す (新しい呼び手が読む)・直してからもう一度戻せる (Codex #1512 R2)', async () => {
  const src = fs.mkdtempSync(path.join(os.tmpdir(), 'lza-src-')), tgt = fs.mkdtempSync(path.join(os.tmpdir(), 'lza-tgt-'));
  fs.writeFileSync(path.join(src, 'manifest.json'), JSON.stringify({ version: 1, pcs: { minipc: ['a.js', 'b.js', 'c.js'] } }));
  fs.writeFileSync(path.join(src, 'a.js'), 'a1\n'); fs.writeFileSync(path.join(src, 'b.js'), 'b1 imports c\n'); fs.writeFileSync(path.join(src, 'c.js'), 'c1\n');
  fs.writeFileSync(path.join(tgt, 'a.js'), 'a0\n'); fs.writeFileSync(path.join(tgt, 'b.js'), 'b0\n');
  const run = (o) => DEP.deploy({ srcDir: src, target: tgt, pc: 'minipc', git: () => ({ commit: 'c1', dirty: false }), ...o });
  let r = run({ action: 'apply' });
  assert.deepEqual([r.ok, r.replaced, r.added], [true, ['a.js', 'b.js'], ['c.js']]);
  const dep = r.deployId;
  const realWrite = (file, buf) => { const tmp = `${file}.t`; fs.writeFileSync(tmp, buf); fs.renameSync(tmp, file); };
  // 1 回目の戻し: a.js は戻る・b.js が戻らない (アクセス拒否) → c.js (b.js の新しい版が読む部品) は消さない
  r = run({ action: 'rollback', rollbackId: dep, hooks: { writeAtomic: (file, buf) => { if (path.basename(file) === 'b.js') throw new Error('EPERM'); realWrite(file, buf); } } });
  assert.deepEqual([r.ok, r.restore_failed.map((x) => x.name), r.kept], [false, ['b.js'], ['c.js']]);
  assert.match(r.reason, /消さずに残した = c\.js/);
  assert.deepEqual(['a.js', 'b.js', 'c.js'].map((n) => fs.readFileSync(path.join(tgt, n), 'utf8')), ['a0\n', 'b1 imports c\n', 'c1\n']);
  assert.equal(JSON.parse(fs.readFileSync(path.join(tgt, 'DEPLOYED.json'), 'utf8')).deploy_id, dep);   // 記録はまだその回
  // 直してからもう一度: もう戻した a.js を「直された」と見ない = 続きから戻せる
  r = run({ action: 'rollback', rollbackId: dep });
  assert.equal(r.ok, true, r.reason);
  assert.deepEqual([fs.readFileSync(path.join(tgt, 'a.js'), 'utf8'), fs.readFileSync(path.join(tgt, 'b.js'), 'utf8'), fs.existsSync(path.join(tgt, 'c.js')), fs.existsSync(path.join(tgt, 'DEPLOYED.json'))], ['a0\n', 'b0\n', false, false]);
  // 写すときの途中の失敗も同じ: 替えたものが戻らない = 足したものを残す
  r = run({ action: 'apply', hooks: {
    readBack: (file) => (path.basename(file) === 'c.js' ? Buffer.from('broken') : fs.readFileSync(file)),
    writeAtomic: (file, buf) => { if (path.basename(file) === 'a.js' && fs.readFileSync(file, 'utf8') === 'a1\n') throw new Error('EPERM'); realWrite(file, buf); },
  } });
  assert.deepEqual([r.ok, r.restore_failed.map((x) => x.name), r.kept], [false, ['a.js'], ['c.js']]);
  assert.equal(fs.existsSync(path.join(tgt, 'c.js')), true);
  // 戻しの途中で止まった続き (替えたものは戻した・足したものは消した・記録だけ残った) = もう一度戻すと続きから終わる
  const t2 = fs.mkdtempSync(path.join(os.tmpdir(), 'lza-tgt-'));
  fs.writeFileSync(path.join(t2, 'a.js'), 'a0\n'); fs.writeFileSync(path.join(t2, 'b.js'), 'b0\n');
  const run2 = (o) => DEP.deploy({ srcDir: src, target: t2, pc: 'minipc', git: () => ({ commit: 'c1', dirty: false }), ...o });
  r = run2({ action: 'apply' });
  assert.equal(r.ok, true, r.reason);
  fs.writeFileSync(path.join(t2, 'a.js'), 'a0\n'); fs.writeFileSync(path.join(t2, 'b.js'), 'b0\n'); fs.unlinkSync(path.join(t2, 'c.js'));
  r = run2({ action: 'rollback', rollbackId: r.deployId });
  assert.equal(r.ok, true, r.reason);
  assert.equal(fs.existsSync(path.join(t2, 'DEPLOYED.json')), false);
});

await ta('[5] 正本の形: manifest のファイルが全部ある・.bat は CRLF・.js は LF・.env などを入れていない', async () => {
  const m = DEP.readManifest(TOOL);
  for (const [pc, files] of Object.entries(m.pcs)) for (const f of files) assert.ok(fs.existsSync(path.join(TOOL, f)), `${pc}: ${f}`);
  for (const f of fs.readdirSync(TOOL)) {
    const b = fs.readFileSync(path.join(TOOL, f)).toString('latin1');
    if (f.endsWith('.bat')) assert.ok(!/(?<!\r)\n/.test(b) && /\r\n/.test(b), `${f} は CRLF`);
    if (f.endsWith('.js') || f.endsWith('.mjs')) assert.ok(!/\r/.test(b), `${f} は LF`);
  }
  assert.ok(!fs.readdirSync(TOOL).some((f) => /^\.env|\.lock$|^logs$|^out$|^downloads$/.test(f)));
  assert.equal(DEP.parseArgs(['--pc', 'minipc', '--apply']).action, 'apply');
  assert.deepEqual([DEP.parseArgs(['--pc', 'minipc', '--rollback', 'dep_1']).rollbackId, DEP.parseArgs(['--pc', 'x']).target], ['dep_1', 'C:\\tools\\logizard-automation']);
  assert.throws(() => DEP.parseArgs(['--apply']), /--pc/);
});

// ── 入荷バーコード連携の起動の決まり (③c-1b-3a) ──
const BM = await import('../tools/logizard-automation/barcode-mode.js');
const jst = (hhmm, day = '2030-01-16') => new Date(`${day}T${hhmm}:00+09:00`);

await ta('[6] 夜の止め: JST 00:00 以上 01:30 未満は動かない (境目・日付をまたぐ) / 起動の形: ①② だけ (設定に依らない・前の設定 LOGIZARD_BC_DAILY は注意を出すだけ) / 引数は --dry だけ', async () => {
  const times = ['23:59', '00:00', '00:15', '00:55', '01:29', '01:30', '08:40', '12:00'];
  assert.deepEqual(times.map((t) => BM.inNightBlock(jst(t))), [false, true, true, true, true, false, false, false]);
  assert.deepEqual([BM.jstMinuteOfDay(new Date('2030-01-15T15:00:00Z')), BM.jstMinuteOfDay(new Date('2030-01-15T16:29:00Z'))], [0, 89]);   // UTC の前の日 15 時 = JST 0 時
  assert.throws(() => BM.assertOutsideNightBlock('①の前', jst('00:20')), (e) => e.nightBlock === true && /^①の前: いま 00:20 \(JST\)。00:00〜01:30 は動きません/.test(e.message));
  BM.assertOutsideNightBlock('①の前', jst('01:30'));
  // 押す持ち時間 (Codex #1518 R2): 次の 00:00 の 2 秒前まで・最大 30 秒 / 00:00 の直前 2 秒と止めの中は押さない
  const at = (iso) => new Date(iso);
  assert.deepEqual([BM.msUntilNightBlock(at('2030-01-15T23:59:50+09:00')), BM.msUntilNightBlock(jst('00:10')), BM.msUntilNightBlock(jst('01:30'))], [10000, 0, 22.5 * 3600 * 1000]);
  assert.deepEqual([BM.clickBudgetMs('x', at('2030-01-15T23:59:50+09:00')), BM.clickBudgetMs('x', at('2030-01-15T23:59:57.9+09:00')), BM.clickBudgetMs('x', jst('12:00'))], [8000, 100, 30000]);
  for (const t of ['2030-01-15T23:59:58+09:00', '2030-01-15T23:59:59.5+09:00', '2030-01-16T00:00:00+09:00', '2030-01-16T01:29:59+09:00']) {
    assert.throws(() => BM.clickBudgetMs('確認の OK の前', at(t)), (e) => e.nightBlock === true && /^確認の OK の前: /.test(e.message), t);
  }
  assert.match(BM.nightError('x', at('2030-01-15T23:59:58.5+09:00')).message, /00:00 の直前 2 秒からは押さない/);
  const plain = new Error('Timeout 2900ms exceeded.');
  assert.equal(BM.asNightError(plain, 'x', jst('12:00')), plain);   // 夜と関係ない失敗はそのまま
  const n = BM.asNightError(plain, '確認の OK の前', at('2030-01-15T23:59:58.1+09:00'));
  assert.ok(n.nightBlock && /\[Timeout 2900ms exceeded\.\]/.test(n.message), n.message);
  const mode = (env, argv = []) => BM.resolveBarcodeMode({ env, argv });
  assert.deepEqual(mode({}), { dry: false, showMode: false, label: BM.LABEL, notes: [] });
  assert.deepEqual([mode({}, ['--show-mode']).showMode, mode({}, ['--show-mode']).dry], [true, false]);   // 配った版の読み戻し (Codex #1558 R2 High)
  assert.ok(!/→ ③/.test(BM.LABEL) && /③ 毎日の商品マスタは miniPC の自動/.test(BM.LABEL), BM.LABEL);
  // 前の設定が残っていても ①② だけ (値で ③ を戻せない = fail-closed)・現場の ①② は止めない (注意を出すだけ)
  for (const v of ['manual', 'auto', ' auto ', 'Auto', 'yes', '']) {
    const m = mode({ LOGIZARD_BC_DAILY: v });
    assert.deepEqual([Object.keys(m).sort(), m.label, m.notes.length], [['dry', 'label', 'notes', 'showMode'], BM.LABEL, 1], JSON.stringify(v));
    assert.match(m.notes[0], /LOGIZARD_BC_DAILY はもう使いません/);
  }
  assert.equal(mode({ LOGIZARD_BC_DAILY: 'manual' }, ['--dry']).dry, true);
  assert.throws(() => mode({}, ['--dyr']), /知らない引数です: --dyr/);
  assert.throws(() => mode({}, ['--dry', 'x']), /知らない引数/);
  assert.throws(() => mode({ LOGIZARD_BC_DAILY: 'manual' }, ['--only-daily']), /この道具から外しました.*ポータルの画面の「手の取込」/);
});

await ta('[7] auto-barcode.js: 夜の止めは CSV・鍵・ブラウザに触る前 / 実行ボタンの前と各ステップの前でも見る / ③ が無い (③ の CSV を見ない・前の設定が残っていても。本物のファイルを時刻を差し替えて動かす)', async () => {
  const s = fs.readFileSync(path.join(TOOL, 'auto-barcode.js'), 'utf8');
  // 実行ボタン (① ③ = FM07_01・② = 書き出し) の直前に夜の止め
  // 処理を始めるボタン (実行・始める確認の OK) は持ち時間つきの nightClick だけで押す (Codex #1518 R1・R2)
  for (const gone of ["page.click('#FM07_01_executeBtn'", 'page.click(exeSel'].concat([])) assert.ok(!s.includes(gone), gone);
  for (const kept of ["await nightClick('#FM07_01_executeBtn', `${stepName} (実行ボタンの前)`);", "await nightClick(exeSel, '② (実行ボタンの前)');",
    "first(), `${stepName} (確認の OK の前)`);", "first(), '② (確認の OK の前)').catch((e) => { if (e && e.nightBlock) throw e; });",
    "beforeSubmit: () => clickBudgetMs('ログインのボタンの前')", 'const timeout = clickBudgetMs(where);', 'throw asNightError(e, where);']) assert.ok(s.includes(kept), kept);
  assert.equal((s.match(/login\(page, loginOpts\(\)\)\.catch\(\(err\) => \{ throw asNightError\(err, 'ログインのボタンの前'\); \}\)/g) || []).length, 2, '最初のログインと再ログイン');
  const cm = fs.readFileSync(path.join(TOOL, 'logizard-common.js'), 'utf8');
  assert.ok(cm.includes("page.click('#login', clickOpts)") && cm.includes('if (Number.isFinite(ms) && ms > 0) clickOpts = { timeout: ms };'), '共通部品のログインのボタンに持ち時間');
  for (const where of ['ログインの前', '①の前', '②の前']) assert.ok(s.includes(`assertOutsideNightBlock('${where}')`), where);
  // ブラウザの確認 dialog: 夜の止めの中なら承認しない (承認の分岐より前に見る)
  const dlg = s.slice(s.indexOf("page.on('dialog'"), s.indexOf('function assertNoUnexpectedDialog'));
  assert.ok(dlg.indexOf('if (inNightBlock())') > 0 && dlg.indexOf('if (inNightBlock())') < dlg.indexOf('d.accept()'), 'dialog の承認の前に夜の止め');
  const order = ['if (inNightBlock())', 'precheckImportCsv(IMPORT1_CSV', 'acquireLock(', 'await launchBrowser('].map((x) => s.indexOf(x));
  assert.ok(order.every((v, k) => v > 0 && (k === 0 || v > order[k - 1])), `順番 ${order}`);
  // Stream Deck の bat の見出しも ①② だけ (Codex #1558 R2 Low)
  const bat = fs.readFileSync(path.join(TOOL, 'run-barcode.bat'), 'latin1');
  assert.ok(!/import shohin/.test(bat) && bat.includes('(import bc_upload - export master)') && /imported by the miniPC nightly/.test(bat), 'run-barcode.bat の見出し');
  // ③ 毎日の商品マスタの取込は無い (切替の PR・L-23)。GAS の ③ に戻すのは lz-gas-rollback の固定の版だけ
  for (const gone of ['IMPORT2', 'import2', 'pre2', 'デイリー取込商品マスタ', 'MODE.daily', "'③の前'", "withRelogin('③'"]) assert.ok(!s.includes(gone), gone);
  assert.equal((s.match(/runImport\(/g) || []).length, 2, '取込は ① の 1 か所だけ (定義 + 呼び 1 つ)');

  // 本物の auto-barcode.js を一時フォルダ (playwright-core を読めるようにリポジトリの中) に写して、時刻を差し替えて動かす
  const tmp = fs.mkdtempSync(path.join(ROOT, '.tmp-lzbc-'));
  try {
    for (const f of ['auto-barcode.js', 'barcode-mode.js', 'logizard-common.js', 'csv-util.js']) fs.copyFileSync(path.join(TOOL, f), path.join(tmp, f));
    fs.writeFileSync(path.join(tmp, 'fake-now.mjs'), `const FIXED = Date.parse(process.env.FAKE_NOW);
const R = Date;
class D extends R { constructor(...a) { super(...(a.length ? a : [FIXED])); } static now() { return FIXED; } }
globalThis.Date = D;
`);
    fs.writeFileSync(path.join(tmp, 'in1.csv'), sj('"商品ID","バーコード"\r\n"A-1","4900000000001"\r\n'));
    const envBase = ['LOGIZARD_USER_ID=u', 'LOGIZARD_PASSWORD=p', 'LOGIZARD_SKIP_DRIVE_CHECK=1', 'LOGIZARD_SKIP_LOGIN_CHECK=1', 'LOGIZARD_BC_MAX_AGE_HOURS=0',
      `LOGIZARD_BC_IMPORT1=${path.join(tmp, 'in1.csv')}`, `LOGIZARD_BC_IMPORT2=${path.join(tmp, 'no-daily.csv')}`, `LOGIZARD_BC_OUT=${path.join(tmp, 'no-dir', 'bc.csv')}`];
    const run = (at, { daily = null, args = [] } = {}) => {
      fs.writeFileSync(path.join(tmp, '.env'), [...envBase, ...(daily === null ? [] : [`LOGIZARD_BC_DAILY=${daily}`])].join('\n') + '\n');
      const { LOGIZARD_BC_DAILY: _drop, ...env } = process.env;
      const c = spawnSync(process.execPath, ['--import', pathToFileURL(path.join(tmp, 'fake-now.mjs')).href, path.join(tmp, 'auto-barcode.js'), ...args],
        { cwd: tmp, encoding: 'utf8', env: { ...env, FAKE_NOW: at.toISOString() }, timeout: 60000 });
      return { status: c.status, out: c.stdout, err: c.stderr };
    };
    const touched = () => fs.existsSync(path.join(tmp, 'logs'));
    for (const t of ['00:00', '00:20', '01:29']) {
      const r = run(jst(t));
      assert.equal(r.status, 1, t);
      assert.match(r.err, new RegExp(`いま ${t} \\(JST\\)。00:00〜01:30 は動きません`), t + r.err);
      assert.ok(!r.out.includes('📥') && !touched(), `${t}: CSV も鍵も見ない`);
    }
    let r = run(jst('01:30'));   // 止めの外 = ①② だけ (③ の CSV を見ない。LOGIZARD_BC_IMPORT2 に無いファイルがあっても) → 次の確かめ (② の保存先) まで進む
    assert.equal(r.status, 1);
    assert.ok(!/③取込CSV/.test(r.err + r.out), r.err);
    assert.match(r.err, /②の保存先フォルダがありません/);
    assert.match(r.out, /③ 毎日の商品マスタは miniPC の自動が取り込む/);
    assert.ok(!/LOGIZARD_BC_DAILY/.test(r.out), '前の設定が無い = 注意も無い');
    for (const daily of ['manual', 'auto', 'yes']) {   // 前の設定が残っていても ③ はしない (消してよいと出す)
      r = run(jst('23:59', '2030-01-15'), { daily });
      assert.equal(r.status, 1, daily);
      assert.ok(!/③取込CSV/.test(r.err + r.out), daily + r.err);
      assert.match(r.err, /②の保存先フォルダがありません/, daily);
      assert.match(r.out, /LOGIZARD_BC_DAILY はもう使いません/, daily);
    }
    // 配った版の読み戻し --show-mode (Codex #1558 R2 High): 夜でも・何にも触らずに ①② の見出しを出して exit 0
    r = run(jst('00:20'), { args: ['--show-mode'] });
    assert.equal(r.status, 0, r.err);
    assert.match(r.out, /① 新商品の取込 → ② バーコード情報の書き出し \(③ 毎日の商品マスタは miniPC の自動が取り込む\)/);
    assert.match(r.out, /③ 毎日の商品マスタの取込: この版には無い/);
    assert.ok(!r.out.includes('📥') && !touched(), '--show-mode は CSV も鍵も見ない');
    for (const [o, re] of [[{ args: ['--dyr'] }, /知らない引数です: --dyr/], [{ args: ['--only-daily'] }, /この道具から外しました/]]) {
      r = run(jst('10:00'), o);
      assert.equal(r.status, 1);
      assert.match(r.err, re);
      assert.ok(!r.out.includes('📥'));
    }
    assert.ok(!touched(), '鍵のフォルダ (logs) は一度も作られていない');
    // ③ のある古い版 (戻しの固定の版 = tag の commit) に --show-mode = 知らない引数で止まる = 古い作業場所から配ったと分かる
    const { JOBS_REGISTRY } = await import('../config/jobs-registry.mjs');
    const rb = JOBS_REGISTRY.find((e) => e.id === 'lz-gas-rollback').rollback;
    const old = fs.mkdtempSync(path.join(ROOT, '.tmp-lzbc-old-'));
    try {
      for (const f of ['auto-barcode.js', 'barcode-mode.js', 'logizard-common.js', 'csv-util.js']) {
        const g = spawnSync('git', ['show', `${rb.commit}:tools/logizard-automation/${f}`], { cwd: ROOT, encoding: 'buffer', maxBuffer: 16 * 1024 * 1024 });
        assert.equal(g.status, 0, `${f} (git fetch origin tag ${rb.tag})`);
        fs.writeFileSync(path.join(old, f), g.stdout);
      }
      fs.writeFileSync(path.join(old, 'fake-now.mjs'), fs.readFileSync(path.join(tmp, 'fake-now.mjs')));
      fs.writeFileSync(path.join(old, '.env'), envBase.join('\n') + '\n');
      const { LOGIZARD_BC_DAILY: _drop, ...env } = process.env;
      const c = spawnSync(process.execPath, ['--import', pathToFileURL(path.join(old, 'fake-now.mjs')).href, path.join(old, 'auto-barcode.js'), '--show-mode'],
        { cwd: old, encoding: 'utf8', env: { ...env, FAKE_NOW: jst('10:00').toISOString() }, timeout: 60000 });
      assert.equal(c.status, 1, c.stdout + c.stderr);
      assert.match(c.stderr, /知らない引数です: --show-mode/);
    } finally {
      fs.rmSync(old, { recursive: true, force: true, maxRetries: 10, retryDelay: 500 });
    }
  } finally {
    fs.rmSync(tmp, { recursive: true, force: true, maxRetries: 10, retryDelay: 500 });   // 子のブラウザがつかんでいる間は待って消す
  }
});

// 偽物のロジザード (本物の auto-barcode.js を動かす。時計を止めて、画面の操作の途中で 00:00 をまたがせる)
const MOCK_LZ = `import fs from 'node:fs';
import pw from 'playwright-core';
const RealDate = Date;
let fixed = RealDate.parse(process.env.FAKE_NOW);
let flowFrom = null;   // 進めた後に時計を流す場面 (押せるようになるまでの待ちで 00:00 を越える)
const nowMs = () => fixed + (flowFrom === null ? 0 : RealDate.now() - flowFrom);
class D extends RealDate { constructor(...a) { super(...(a.length ? a : [nowMs()])); } static now() { return nowMs(); } }
globalThis.Date = D;
const jump = () => { fixed = RealDate.parse(process.env.MOCK_JUMP_TO); st.jumped = true; if (/wait$/.test(process.env.MOCK_SCENARIO || '')) flowFrom = RealDate.now(); };
const SC = process.env.MOCK_SCENARIO || '';
const st = { logins: 0, exec: 0, ok: 0, pm07: 0, closed: false, jumped: false, other: [] };
const save = () => fs.writeFileSync(process.env.MOCK_STATE, JSON.stringify(st));
save();
let loggedIn = false;
const LOGIN = '<html><body><form method="post" action="/LPSTD405/login"><input id="user_id" name="u"><input id="password" name="p" type="password"><input id="err_login" value=""><button id="login" type="submit">login</button></form></body></html>';
const LOGIN_SLOW = LOGIN.replace('</form>', '</form><script>const b = document.getElementById("login"); b.disabled = true; setTimeout(() => { b.disabled = false; }, 6000);</script>');
const MENU = '<html><body><a onclick="openFunctionBar(1)" href="#">menu</a></body></html>';
const PM07 = \`<html><body><a onclick="openFunctionBar('FM07_01')" href="#">import</a>
<div id="FM07_01_FORM"><select id="FM07_01_fileId"><option value="">-</option><option value="5">商品マスタ</option></select>
<select id="FM07_01_ptrnId"><option value="">-</option><option value="1">新商品バーコード登録</option><option value="2">デイリー取込商品マスタ</option></select>
<input type="file" id="FM07_01_impFile"><input type="button" id="FM07_01_executeBtn" value="実行"></div>
<div id="cfm" class="ui-dialog" style="display:none">ファイルアップロードを開始します <input type="button" value="OK" id="cfmOk"></div>
<div id="res" class="ui-dialog" style="display:none">インポート結果 総件数 : 1 処理件数 : 1 処理不要件数 : 0 エラー件数 : 0 <input type="button" value="OK" onclick="this.parentNode.style.display='none'"></div>
<script>function openFunctionBar() {}
document.getElementById('FM07_01_executeBtn').onclick = async () => { const r = await fetch('/LPSTD405/mock/exec', { method: 'POST' }); const ok = document.getElementById('cfmOk');
  if ((await r.text()) === 'slow') { ok.disabled = true; setTimeout(() => { ok.disabled = false; }, 6000); }
  document.getElementById('cfm').style.display = 'block'; };
document.getElementById('cfmOk').onclick = async () => { document.getElementById('cfm').style.display = 'none'; await fetch('/LPSTD405/mock/ok', { method: 'POST' }); document.getElementById('res').style.display = 'block'; };
</script></body></html>\`;
const launch = pw.chromium.launch.bind(pw.chromium);
pw.chromium.launch = async () => {
  const browser = await launch({ headless: true });
  const close = browser.close.bind(browser);
  browser.close = async () => { st.closed = true; save(); return close(); };
  const newContext = browser.newContext.bind(browser);
  browser.newContext = async (o) => {
    const ctx = await newContext(o);
    await ctx.route('https://ap003.logizard.net/**', async (route) => {
      const req = route.request();
      const p = new URL(req.url()).pathname;
      const html = (body) => route.fulfill({ status: 200, contentType: 'text/html; charset=utf-8', body });
      if (p === '/LPSTD405/') { if ((SC === 'login' || SC === 'loginwait') && !loggedIn) jump(); save(); return html(loggedIn ? MENU : SC === 'loginwait' ? LOGIN_SLOW : LOGIN); }
      if (p === '/LPSTD405/login') { st.logins++; if (SC === 'loginretry' && st.logins === 1) { jump(); save(); return html(LOGIN); } loggedIn = true; save(); return html(MENU); }
      if (p === '/LPSTD405/PM07/Index') { st.pm07++; if (SC === 'relogin' && st.pm07 === 1) { loggedIn = false; jump(); save(); return html(LOGIN); } save(); return html(PM07); }
      if (p === '/LPSTD405/mock/exec') { st.exec++; if (SC === 'confirm' || SC === 'confirmwait') jump(); save(); return route.fulfill({ status: 200, body: SC === 'confirmwait' ? 'slow' : 'ok' }); }
      if (p === '/LPSTD405/mock/ok') { st.ok++; if (SC === 'after1') jump(); save(); return route.fulfill({ status: 200, body: 'ok' }); }
      st.other.push(p); save(); return route.abort();
    });
    return ctx;
  };
  return browser;
};
`;

await ta('[8] 偽物のロジザードで本物の auto-barcode.js: 押す直前に 00:00 をまたいだら押さない (ログインのボタン・共通部品のリトライ・再ログイン・取込を始める確認の OK・次のステップ / 押せるようになるまでの待ちで 00:00 を越えない) / NIGHT_BLOCK の記録・ブラウザを閉じる・鍵を返す / 01:30 の後の再実行は済んだ ① を飛ばす (Codex #1518 R1)', async () => {
  try { await import('playwright-core'); } catch { console.log('      (playwright-core が無い = この試験はとばす)'); passed--; return; }
  const tmp = fs.mkdtempSync(path.join(ROOT, '.tmp-lzbc-'));
  try {
    for (const f of ['auto-barcode.js', 'barcode-mode.js', 'logizard-common.js', 'csv-util.js']) fs.copyFileSync(path.join(TOOL, f), path.join(tmp, f));
    fs.writeFileSync(path.join(tmp, 'mock-lz.mjs'), MOCK_LZ);
    fs.writeFileSync(path.join(tmp, 'in1.csv'), sj('"商品ID","バーコード"\r\n"A-1","4900000000001"\r\n'));
    fs.mkdirSync(path.join(tmp, 'out'));
    fs.writeFileSync(path.join(tmp, '.env'), ['LOGIZARD_USER_ID=u', 'LOGIZARD_PASSWORD=p', 'LOGIZARD_SKIP_DRIVE_CHECK=1', 'LOGIZARD_SKIP_LOGIN_CHECK=1', 'LOGIZARD_BC_MAX_AGE_HOURS=0',
      `LOGIZARD_BC_IMPORT1=${path.join(tmp, 'in1.csv')}`, `LOGIZARD_BC_OUT=${path.join(tmp, 'out', 'bc.csv')}`].join('\n') + '\n');
    const logs = path.join(tmp, 'logs');
    const run = (scenario, at, jumpTo = '2030-01-16T00:00:01+09:00') => {
      const stateFile = path.join(tmp, `state-${scenario || 'none'}-${Math.random().toString(16).slice(2)}.json`);
      const before = fs.existsSync(logs) ? fs.readdirSync(logs) : [];
      const { LOGIZARD_BC_DAILY: _drop, ...env } = process.env;
      const c = spawnSync(process.execPath, ['--import', pathToFileURL(path.join(tmp, 'mock-lz.mjs')).href, path.join(tmp, 'auto-barcode.js')],
        { cwd: tmp, encoding: 'utf8', timeout: 120000,
          env: { ...env, FAKE_NOW: new Date(at).toISOString(), MOCK_JUMP_TO: new Date(jumpTo).toISOString(), MOCK_SCENARIO: scenario, MOCK_STATE: stateFile } });
      const added = fs.readdirSync(logs).filter((f) => !before.includes(f) && /^barcode_result_.*\.json$/.test(f));
      assert.equal(added.length, 1, `${scenario}: 結果の記録が 1 つ ${c.stdout}${c.stderr}`);
      return { status: c.status, text: c.stdout + c.stderr, st: JSON.parse(fs.readFileSync(stateFile, 'utf8')),
        result: JSON.parse(fs.readFileSync(path.join(logs, added[0]), 'utf8')) };
    };
    const imported = () => { try { return JSON.parse(fs.readFileSync(path.join(logs, 'barcode_imported.json'), 'utf8')); } catch { return {}; } };
    const lockGone = () => !fs.existsSync(path.join(logs, 'logizard-session.lock'));
    const T0 = '2030-01-15T23:59:50+09:00';

    // ログインのページを開いた間に 00:00 → ログインのボタンを押さない
    let r = run('login', T0);
    assert.deepEqual([r.status, r.result.status, r.st.jumped, r.st.logins, r.st.exec, r.st.closed, lockGone()], [1, 'NIGHT_BLOCK', true, 0, 0, true, true], r.text);
    assert.match(r.text, /ログインのボタンの前: いま 00:00 \(JST\)。00:00〜01:30 は動きません/);
    // 1 回目のログインが通らず、共通部品のリトライの前に 00:00 → リトライでもボタンを押さない
    r = run('loginretry', T0);
    assert.deepEqual([r.status, r.result.status, r.st.jumped, r.st.logins, r.st.exec, r.st.closed, lockGone()], [1, 'NIGHT_BLOCK', true, 1, 0, true, true], r.text);
    assert.match(r.text, /リトライ[\s\S]*ログインのボタンの前: いま 00:00/);
    // ログインのボタンが押せるようになるのを待つ間に 00:00 の直前 → 押さない (持ち時間切れ・Codex #1518 R2)
    const T1 = '2030-01-15T23:59:55+09:00';
    r = run('loginwait', T0, T1);
    assert.deepEqual([r.status, r.result.status, r.st.jumped, r.st.logins, r.st.exec, r.st.closed, lockGone()], [1, 'NIGHT_BLOCK', true, 0, 0, true, true], r.text);
    assert.match(r.text, /ログインのボタンの前: いま 23:59 \(JST\)。00:00〜01:30 は動きません .*00:00 の直前 2 秒からは押さない.*\[.*Timeout/);
    // ① の画面で追い出された (セッション切れ) 間に 00:00 → 再ログインしない
    r = run('relogin', T0);
    assert.deepEqual([r.status, r.result.status, r.st.jumped, r.st.logins, r.st.exec, r.st.closed, lockGone()], [1, 'NIGHT_BLOCK', true, 1, 0, true, true], r.text);
    assert.match(r.text, /① \(再ログインの前\): いま 00:00/);
    // 実行ボタンの後・確認の OK の前に 00:00 → OK を押さない (取込は始まらない)
    r = run('confirm', T0);
    assert.deepEqual([r.status, r.result.status, r.st.exec, r.st.ok, r.st.closed, lockGone(), r.result.progress], [1, 'NIGHT_BLOCK', 1, 0, true, true, '①未 ②未'], r.text);
    assert.match(r.text, /①新商品バーコード登録 \(確認の OK の前\): いま 00:00/);
    assert.deepEqual(imported(), {});
    // 確認の OK が押せるようになるのを待つ間に 00:00 の直前 → 押さない (取込は始まらない・Codex #1518 R2)
    r = run('confirmwait', T0, T1);
    assert.deepEqual([r.status, r.result.status, r.st.exec, r.st.ok, r.st.closed, lockGone(), r.result.progress], [1, 'NIGHT_BLOCK', 1, 0, true, true, '①未 ②未'], r.text);
    assert.match(r.text, /①新商品バーコード登録 \(確認の OK の前\): いま 23:59 \(JST\).*00:00 の直前 2 秒からは押さない.*\[.*Timeout/);
    assert.deepEqual(imported(), {});
    // ① が済んだ後に 00:00 → ② に進まない・① は取込済みとして残る
    r = run('after1', T0);
    assert.deepEqual([r.status, r.result.status, r.st.exec, r.st.ok, r.st.closed, lockGone(), r.result.progress], [1, 'NIGHT_BLOCK', 1, 1, true, true, '①済 ②未'], r.text);
    assert.match(r.text, /②の前: いま 00:00/);
    assert.ok(imported().import1 && imported().import1.sha256, '① は取込済みとして記録');
    assert.deepEqual(r.st.other, [], '② の画面は開いていない');
    // 01:30 の後にもう一度押す = ① は同じ中身なので飛ばして ② へ (② の画面はこの偽物に無い = 失敗で終わる)
    r = run('', '2030-01-16T01:30:00+09:00');
    assert.equal(r.st.exec, 0, '① をもう一度取り込まない');
    assert.match(r.text, /⏭ ①: 前回取込済み/);
    assert.deepEqual([r.status, r.result.status, r.st.other, r.st.closed, lockGone()], [1, 'FAILED', ['/LPSTD405/PM08/Index'], true, true], r.text);
  } finally {
    fs.rmSync(tmp, { recursive: true, force: true, maxRetries: 10, retryDelay: 500 });   // 子のブラウザがつかんでいる間は待って消す
  }
});

await ta('[9] 押してよいかの旗 (import-guard.js・③c-1b-2b-1b K7): 持ち時間 = 締め切りまでの残り (余白を引く) と最大の短い方・余白の内 = 止める・止めるのは 1 回・止めた後に登録した処理もすぐ呼ぶ', async () => {
  const G = await import('../tools/logizard-automation/import-guard.js');
  let t = 1_000_000;
  const g = G.createGuard({ now: () => t, deadlineMs: t + 60_000, marginMs: 5_000, maxClickMs: 30_000 });
  assert.equal(g.check('a'), 30_000);
  t += 40_000;
  assert.equal(g.check('b'), 15_000);   // 60 - 40 - 5
  g.setDeadline(t + 100_000);
  assert.equal(g.check('c'), 30_000);
  t += 95_001;
  assert.throws(() => g.check('実行ボタンの前'), (e) => e instanceof G.StopError && e.reason === 'deadline' && /実行ボタンの前/.test(e.message));
  assert.deepEqual([g.isStopped(), g.reason, g.stop('other')], [true, 'deadline', false]);   // 1 回だけ
  const seen = [];
  g.onStop((r) => seen.push(r));
  assert.deepEqual(seen, ['deadline']);
  const h = G.createGuard();
  const hs = [];
  h.onStop((r) => hs.push(r)); h.onStop(() => { throw new Error('止める処理の失敗は無視'); }); h.onStop((r) => hs.push(`2:${r}`));
  assert.equal(h.check('x'), 30_000);   // 締め切りなし
  assert.equal(h.stop('lock_extend_failed'), true);
  assert.deepEqual(hs, ['lock_extend_failed', '2:lock_extend_failed']);
  assert.throws(() => h.check('確認の OK'), (e) => e.stopped && e.reason === 'lock_extend_failed');
});

await ta('[10] バーコードの書き出し (barcode-export.js・③c-1b-2b K4): 検証 (HTML・Shift-JIS でない・必須列 商品ID / バーコード・列が 2 つ・列の数違い・少なすぎる) = invalid_csv / 全件の条件 (登録日・開始日なし・有効 + 無効) / 承認は「エクスポート処理を行います」だけ / ② の出力 (バーコードマスタ.csv) には書かない', async () => {
  const B = await import('../tools/logizard-automation/barcode-export.js');
  // 実機の見出し (2026-09-29 の全件の書き出し = 商品ID,商品名,検索名称,バーコード,有効期限区分)
  const csv = (rows) => sj(['"商品ID","商品名","検索名称","バーコード","有効期限区分"', ...rows].join('\r\n'));
  let v = B.validateBarcodeCsv(csv(['"A-1","商品A","商品A","4900000000001","0"', '"A-1","商品A","商品A","4900000000002","0"']), { minRows: 2 });
  assert.deepEqual([v.ok, v.dataRows, v.header], [true, 2, ['商品ID', '商品名', '検索名称', 'バーコード', '有効期限区分']]);
  assert.match(B.validateBarcodeCsv(Buffer.from('<html><body>login</body></html>'), { minRows: 1 }).reason, /HTML/);
  assert.match(B.validateBarcodeCsv(Buffer.from([0x22, 0x81, 0x22]), { minRows: 1 }).reason, /Shift-JIS/);
  assert.match(B.validateBarcodeCsv(sj('"商品ID","商品名"\r\n"A","x"'), { minRows: 1 }).reason, /必須列がありません: バーコード/);
  assert.match(B.validateBarcodeCsv(sj('"商品ID","バーコード","バーコード"\r\n"A","1","2"'), { minRows: 1 }).reason, /2 つあります/);
  assert.match(B.validateBarcodeCsv(csv(['"A-1","商品A"']), { minRows: 1 }).reason, /列数が違います/);
  assert.match(B.validateBarcodeCsv(csv(['"A-1","商品A","商品A","1","0"']), { minRows: 2 }).reason, /少なすぎます/);
  assert.match(B.validateBarcodeCsv(csv(['"A-1","商品A","商品A","1","0"'])).reason, /下限 4000/);   // 9/29 の全件 = 5,188 行
  // 途中で切れた (Codex #1530 R1 High): 本物は末尾が改行で終わらない = 改行で終わる = 行の切れ目で切れた疑い / 行の途中で切れた = 列の数・引用符
  assert.match(B.validateBarcodeCsv(Buffer.concat([csv(['"A-1","商品A","商品A","1","0"', '"B-2","商品B","商品B","2","0"']), Buffer.from('\r\n')]), { minRows: 1 }).reason, /末尾が改行/);
  assert.equal(B.validateBarcodeCsv(csv(['"A-1","商品A","商品A","1","0"', '"B-2","商品B"']), { minRows: 1 }).ok, false);
  assert.equal(B.validateBarcodeCsv(Buffer.alloc(0)).reason, '中身が空です');
  // 検証に落ちた = 印つきの例外 (取込の試験が「中身の壊れ = 確かめの失敗」に使う)
  const src = fs.readFileSync(path.join(TOOL, 'barcode-export.js'), 'utf8');
  assert.match(src, /if \(!v\.ok\) \{ await errorShot\(page, 'bcx-invalid-csv'\); throw invalidCsvError\(v\.reason\); \}/);
  assert.equal(E.invalidCsvError(B.validateBarcodeCsv(Buffer.from([0x22, 0x81, 0x22]), { minRows: 1 }).reason).code, 'invalid_csv');
  // 全件の条件 (登録日・開始日を空・終わり = 今日・有効 + 無効) を設定して読み戻し、合わない = 実行しない
  for (const s of ["page.check('#FM08_01_BR010_targetDate1')", "page.fill('#FM08_01_BR010_fromTargetDate', '')", "page.fill('#FM08_01_BR010_toTargetDate', today)",
    "page.check('#FM08_01_BR010_expStatus1')", "page.check('#FM08_01_BR010_expStatus2')", "c.from === '' && c.to === today && c.exp1 === true && c.exp2 === true"]) assert.ok(src.includes(s), s);
  assert.ok(src.indexOf("if (!condOk) {") < src.indexOf("await page.click('#FM08_01_executeBtn')"), '条件を確かめてから実行');
  // 種類・抽出パターンは表示の文字の完全一致で 1 つだけ
  assert.deepEqual([B.BARCODE_TYPE_LABEL, B.BARCODE_PATTERN_LABEL], ['SKU', 'バーコード情報']);
  // 承認のモーダルは「エクスポート処理を行います」のときだけ OK
  // 承認の文は空白を除いて完全一致 (知らない文が足されていたら押さない。Codex #1530 R3 Medium)
  assert.ok(src.includes("if (!EXPORT_CONFIRM_RE.test(confirmMsg.replace(/\\s+/g, ''))) {"));
  assert.deepEqual(['エクスポート処理を行いますよろしいですか', 'エクスポート処理を行います。よろしいですか？', 'エクスポート処理を行います', 'エクスポート処理を行います.よろしいですか?'].map((t) => B.EXPORT_CONFIRM_RE.test(t)), [true, true, true, true]);   // 1 つ目 = 実機の文 (9/29「エクスポート処理を行います よろしいですか」から空白を除いた形)
  assert.deepEqual(['エクスポート処理を行います。在庫も削除します', '在庫を削除します。エクスポート処理を行います', 'エクスポート処理を行いますか', ''].map((t) => B.EXPORT_CONFIRM_RE.test(t)), [false, false, false, false]);
  // 閉じてよいのは知っている注意文だけ (キャンセル)・承認のモーダルには触らない・知らない文 = 何も押さずに止める (Codex #1530 R1 Medium)
  const fakePage = (text) => { const clicks = []; return { clicks, evaluate: async () => text, click: async (sel) => { clicks.push(sel); }, waitForFunction: async () => {} }; };
  let fp = fakePage(null);
  assert.equal(await B.dismissNotice(fp, () => {}), null);
  fp = fakePage('1年以上離れた日付が指定されています。');
  await B.dismissNotice(fp, () => {});
  assert.deepEqual(fp.clicks, ['#popup_cancel']);
  fp = fakePage('エクスポート処理を行います。よろしいですか？');
  await B.dismissNotice(fp, () => {});
  assert.deepEqual(fp.clicks, []);
  // 知っている注意文も完全一致 (ほかの文が足されていたら止める)・キャンセルを押せない = OK は押さずに止める (Codex #1530 R4 Medium)
  fp = fakePage('1年以上離れた日付が指定されています');
  fp.click = async (sel) => { fp.clicks.push(sel); if (sel === '#popup_cancel') throw new Error('見つからない'); };
  await assert.rejects(B.dismissNotice(fp, () => {}), /キャンセルを押せない/);
  assert.deepEqual(fp.clicks, ['#popup_cancel']);
  for (const t of ['在庫を削除します。よろしいですか？', '在庫を削除します。続行するには OK を押してください。', '1年以上離れた日付が指定されています。在庫も削除します']) {
    fp = fakePage(t);
    await assert.rejects(B.dismissNotice(fp, () => {}), /想定外のモーダル/);
    assert.deepEqual(fp.clicks, [], t);
  }
  // 確かめの道具が書いてよい場所 = このフォルダの out\ の下だけ・実体で見る (共有ドライブ・ネットワーク・似た名前のフォルダ・一時フォルダ・ジャンクションの先には書かない。Codex #1530 R1・R2 Medium)
  const TD = fs.mkdtempSync(path.join(os.tmpdir(), 'bcx-dir-'));
  fs.mkdirSync(path.join(TD, 'out', 'sub'), { recursive: true });
  const ELSE = fs.mkdtempSync(path.join(os.tmpdir(), 'bcx-else-'));
  fs.symlinkSync(ELSE, path.join(TD, 'out', 'link'), 'junction');   // out\ の中に外を指すジャンクション
  assert.deepEqual([path.join(TD, 'out', 'x.csv'), path.join(TD, 'out', 'sub', 'x.csv'), path.join(TD, 'out', 'new', 'deep', 'x.csv')].map((p) => B.isAllowedOut(p, { dir: TD })), [true, true, true]);
  assert.deepEqual(['G:\\共有ドライブ\\入荷バーコード発行\\x.csv', '\\\\server\\share\\x.csv', path.join(TD, 'x.csv'), path.join(TD, 'out-evil', 'x.csv'), path.join(TD, 'out'),
    path.join(os.tmpdir(), 'x.csv'), path.join(TD, 'out', 'link', 'x.csv'), path.join(TD, 'out', 'link', 'new', 'x.csv'), ''].map((p) => B.isAllowedOut(p, { dir: TD })), [false, false, false, false, false, false, false, false, false]);
  assert.equal(B.isAllowedOut(path.join(TD, 'out', 'x.csv'), {}), false);
  // out\ そのものがジャンクション (外を指す) = 書かない (Codex #1530 R3 Medium)
  const TD2 = fs.mkdtempSync(path.join(os.tmpdir(), 'bcx-dir2-'));
  fs.symlinkSync(ELSE, path.join(TD2, 'out'), 'junction');
  assert.equal(B.isAllowedOut(path.join(TD2, 'out', 'x.csv'), { dir: TD2 }), false);
  // 引用符の形 (parseCsv は寛容なので別に見る。Codex #1530 R2 Medium)
  assert.deepEqual(['"a","b"\r\n"c","d"', '"a"x,"b"', 'a"b,c', '"a,b', '"a""b",c', 'a,b\r\n"c"'].map((t) => B.csvQuoteError(t)), [null, 'after_quote', 'bare_quote', 'unterminated', null, null]);
  assert.match(B.validateBarcodeCsv(sj('"商品ID","バーコード"\r\n"A"x,"1"'), { minRows: 1 }).reason, /引用符の形が壊れています \(after_quote\)/);
  // 固定の出力先に書かない (本体は Buffer を返すだけ) / 確かめの道具は ② の出力 (バーコードマスタ.csv)・既存のファイルに書かない
  assert.ok(!/writeFileSync|G:\\\\|共有ドライブ/.test(src.replace(/\/\*\*[\s\S]*?\*\//g, '')), '本体は書かない');
  const cli = (args) => spawnSync(process.execPath, [path.join(TOOL, 'export-barcode-to.js'), ...args], { encoding: 'utf8', cwd: TOOL });
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'bcx-'));
  const exists = path.join(tmp, 'x.csv'); fs.writeFileSync(exists, 'x');
  const envOk = fs.existsSync(path.join(TOOL, '.env'));
  if (envOk) {   // .env がある PC (本物) だけ CLI の断りを見る (ログインの前に断る)
    assert.match(cli([]).stderr, /--out/);
    assert.match(cli(['--out', exists]).stderr, /すでにある/);
    assert.match(cli(['--out', path.join(tmp, 'バーコードマスタ.csv')]).stderr, /バーコードマスタ\.csv/);
  }
  const t = fs.readFileSync(path.join(TOOL, 'export-barcode-to.js'), 'utf8');
  for (const kept of ["flag: 'wx'", "acquireLock({ name: 'logizard-session.lock' })", 'exportBarcodeMaster(page', 'バーコードマスタ\\.csv$', 'isAllowedOut(OUT, { dir: DIR })',
    "process.once('exit', releaseOnExit)", "process.removeListener('exit', releaseOnExit)"]) assert.ok(t.includes(kept), kept);   // 共通部品が process.exit しても鍵を返す (Codex #1530 R4 Low)
  assert.ok(t.indexOf("process.once('exit', releaseOnExit)") > t.indexOf("acquireLock({ name: 'logizard-session.lock' })") && t.indexOf("process.once('exit', releaseOnExit)") < t.indexOf('launchBrowser({'), '鍵を取った直後・ブラウザの前');
  // 写す一覧: miniPC (取込の試験が動く PC) に barcode-export.js と export-barcode-to.js
  const m = JSON.parse(fs.readFileSync(path.join(TOOL, 'manifest.json'), 'utf8'));
  assert.ok(m.pcs.minipc.includes('barcode-export.js') && m.pcs.minipc.includes('export-barcode-to.js'));
});

console.log(`\n${passed} 件 PASS${process.exitCode ? ' (NG あり)' : ''}`);
process.exit(process.exitCode || 0);
