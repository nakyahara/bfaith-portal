/**
 * test-logizard-automation.mjs — ロジザードの自動化の正本 (tools/logizard-automation。マスタ正本切替 ③c-1b-0)
 *
 * 固定する契約 (設計 = AI_reference CompanyDB構想/10 §6.3「③c-1b 契約 v3」H8・L-10):
 *   1 書き出しの検証 (shohin-export.js validateShohinCsv) は auto-shohin-csv.js v0.1 の validateCsv と同じ
 *   2 auto-shohin-csv.js は書き出しの手順と検証を shohin-export.js から使う (同じ手順を 2 つ持たない)
 *   3 写し方 (deploy.mjs): 見るだけ / 写す (前のファイルを残す・DEPLOYED.json) / 同じなら何もしない / 動いている間は断る /
 *     未コミットは断る / ずれの検出 / 戻す / 途中の失敗は戻す
 *   4 .bat は CRLF・.js は LF (各 PC の今のファイルと同じバイト)
 * 使い方: node scripts/test-logizard-automation.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';

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
  // 戻す書き込みが黙って違う中身を書いた = 読み直して「戻しきれなかった」 (前の試しで a.js は新しいまま = 元に戻してから)
  fs.writeFileSync(path.join(tgt, 'a.js'), 'a0\n');
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

console.log(`\n${passed} 件 PASS${process.exitCode ? ' (NG あり)' : ''}`);
process.exit(process.exitCode || 0);
