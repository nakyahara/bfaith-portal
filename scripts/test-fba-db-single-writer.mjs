#!/usr/bin/env node
/**
 * test-fba-db-single-writer.mjs — fba.db の「2 プロセスの書き戻しで行が消える」事故 (2026-09-20) の再発防止の試験。
 *   ① fba-replenishment/db.js の歯止め: 外から書き換えられたファイルを黙って上書きしない (読み直して例外・やり直せば通る・相手の行は消えない)。
 *      判定はプロセス間の lock (SQLite のファイルロック) の中で、ファイルの中の世代の印 + 更新時刻とサイズ。lock の排他は「持っている間に相手が保存しに来る」を決定的に再現して確かめる
 *   ② 朝の cron (snapshot-fba-stock.js): 常駐サーバに頼んで待つ。頼めなければ (起動していない・認証・404・5xx・応答なし) 自分では書かずに失敗。--direct は止めてあるときだけ
 *   ③ 本体 (fba-report-snapshot.js): 保存の順番・business_date・US・何も取れなかった回は失敗・「外から書かれた」を握りつぶさない
 * DATA_DIR を一時ディレクトリに向けるので、本番・開発の fba.db には触れない。SP-API にも行かない (取得は差し替え)。
 * 🚨 試験に無いもの: 常駐サーバの口 (POST /service-api/fba/snapshot-reports) を HTTP で叩くこと (応答の形は service-router.js / fba-service.js のソースと突き合わせるだけ)・SP-API への本物の要求
 */
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import Database from 'better-sqlite3';

const root = path.join(path.dirname(fileURLToPath(import.meta.url)), '..');
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'fba-db-sw-'));
process.env.DATA_DIR = tmp;                 // db.js は import の時点で DATA_DIR を読む
process.env.FBA_DB_LOCK_WAIT_MS = '300';    // lock を待つ上限 (試験では短く。🚨 SQLite の busy の待ちは Windows では 1 秒刻み = 実際には 1 秒ほど待つ)
const dbUrl = pathToFileURL(path.join(root, 'apps', 'fba-replenishment', 'db.js')).href;
const dbFile = path.join(tmp, 'fba.db');

let ok = 0, ng = 0;
const t = async (name, fn) => { try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message || e)); } };
const quiet = () => {};
/** ファイルの更新時刻が確実に変わるまで待つ */
const tick = () => new Promise((r) => setTimeout(r, 25));
const codeOf = (fn) => { try { fn(); return null; } catch (e) { return e.code || e.message; } };
/** ファイルの中身を、db.js を通さずに読む (🚨 db.js の initDb() は最後に保存する = 確認のつもりで使うとファイルを書き換えてしまう) */
const readFile = () => { const f = new Database(dbFile, { readonly: true, fileMustExist: true }); try { return { snaps: f.prepare(`select snapshot_date d, amazon_sku sku, fba_available a, fba_fc_processing p from daily_snapshots order by 1, 2`).all(), settings: Object.fromEntries(f.prepare(`select key, value from settings`).all().map((r) => [r.key, r.value])) }; } finally { f.close(); } };
const snapRow = (sku, n = 1) => ({ amazon_sku: sku, fba_available: n, fba_fc_transfer: 1, fba_fc_processing: 2, fba_customer_order: 3, fba_inbound_working: 0, fba_inbound_shipped: 0, fba_inbound_received: 0 });

console.log('① fba.db の歯止め (module の実体を分ける = 別プロセスの代わり。最後に本物の 2 プロセス)');
const A = await import(dbUrl + '?proc=A');   // 常駐サーバの役 (読んだメモリを持ち続ける)
const B = await import(dbUrl + '?proc=B');   // 別プロセスの書き手の役 (後から読んで書く)
await t('🚨 本番で起きた形: A が読む → B が読んで日次を書く → A が何か保存 … A は上書きせず例外 (FBA_DB_EXTERNAL_WRITE)。B の入れた行はファイルに残り、A のメモリにも入る。A がやり直すと通り、両方が残る', async () => {
  await A.initDb();
  A.updateSetting('draft_memo', 'a1');
  await tick();
  await B.initDb();
  B.saveRestockInventoryToDailySnapshot([snapRow('sku-1', 7)], '2026-09-18');
  assert.equal(A.getDailySnapshots('sku-1').length, 0, '前提: A のメモリは B の行を知らない');
  await tick();
  const gen0 = A.getDbGeneration();
  assert.equal(codeOf(() => A.updateSetting('draft_memo', 'a2')), 'FBA_DB_EXTERNAL_WRITE');
  const f1 = readFile();
  assert.deepEqual([f1.snaps.map((r) => [r.d, r.sku, r.a, r.p]), f1.settings.draft_memo], [[['2026-09-18', 'sku-1', 7, 2]], 'a1'], 'ファイルは B が書いたまま');
  assert.deepEqual([A.getDailySnapshots('sku-1').length, A.getSettings().draft_memo, A.getDbGeneration()], [1, 'a1', gen0 + 1], 'A は読み直した (世代が 1 つ進む)');
  A.updateSetting('draft_memo', 'a2');
  const f2 = readFile();
  assert.deepEqual([f2.snaps.length, f2.settings.draft_memo], [1, 'a2']);
});
await t('取引つきの保存 (BEGIN → COMMIT → 保存) でも同じ例外が出る: catch の ROLLBACK が「取引が無い」で元の例外を隠さない。古くなった側 (今度は B) は、もう一度で通る', async () => {
  await tick();
  A.updateSetting('draft_memo', 'a3');
  await tick();
  let err = null;
  try { B.saveRestockInventoryToDailySnapshot([snapRow('sku-2')], '2026-09-19'); } catch (e) { err = e; }
  assert.equal(err && err.code, 'FBA_DB_EXTERNAL_WRITE', `元の例外が隠れた: ${err && err.message}`);
  assert.match(err.message, /もう一度実行する/);
  B.saveRestockInventoryToDailySnapshot([snapRow('sku-2')], '2026-09-19');
  const f3 = readFile();
  assert.deepEqual([f3.snaps.map((r) => r.sku), f3.settings.draft_memo], [['sku-1', 'sku-2'], 'a3']);
});
await t('🚨 更新時刻もサイズも同じ書き換えでも見つける (判定はファイルの中の世代の印。Codex R1 #3): 更新時刻とサイズの比較を「同じ」に固定しても、B の書き換えを A は上書きしない = 印の比較を外すと、この試験は落ちる', async () => {
  await tick();
  assert.equal(codeOf(() => A.updateSetting('draft_memo', 'a4')), 'FBA_DB_EXTERNAL_WRITE');   // 直前に B が書いたので、まず A を最新にする
  A.updateSetting('draft_memo', 'a4');
  await tick();
  assert.equal(codeOf(() => B.updateSetting('draft_memo', 'b4')), 'FBA_DB_EXTERNAL_WRITE');
  B.updateSetting('draft_memo', 'b4');
  A._testHooks.stampAlwaysSame = true;   // 更新時刻とサイズでは見分けられない、を作る
  try {
    const tokenBefore = A._fileStampState().knownToken;
    assert.equal(codeOf(() => A.updateSetting('other', 'x')), 'FBA_DB_EXTERNAL_WRITE');
    assert.notEqual(A._fileStampState().knownToken, tokenBefore, 'A は読み直して、B の印を覚え直す');
    assert.equal(readFile().settings.draft_memo, 'b4', 'B の値が A に消された');
    A.updateSetting('other', 'x');       // 印が合えば、固定したままでも通る
    assert.deepEqual([readFile().settings.draft_memo, readFile().settings.other], ['b4', 'x']);
  } finally { A._testHooks.stampAlwaysSame = false; }
});
await t('メモリが読み直されたら、未保存の変更を抱えた処理は「保存した」ことにしない: flushInboundDb(控えた世代) は FBA_DB_RELOADED (納品履歴の明細 = 100 件ごとに保存・間に await。Codex R1 #5)', async () => {
  const gen = A.getDbGeneration();
  A.flushInboundDb(gen);                                   // 変わっていなければ通る
  await tick();
  assert.equal(codeOf(() => B.updateSetting('z', '1')), 'FBA_DB_EXTERNAL_WRITE'); B.updateSetting('z', '1');
  await tick();
  assert.equal(codeOf(() => A.updateSetting('y', '1')), 'FBA_DB_EXTERNAL_WRITE');   // ← 別のリクエストの保存が読み直しを起こした、の役
  assert.equal(codeOf(() => A.flushInboundDb(gen)), 'FBA_DB_RELOADED');
  A.flushInboundDb();                                      // 世代を渡さない呼び出し (今までの形) は通る
  assert.equal(A.getDbGeneration() > gen, true);
  const src = fs.readFileSync(path.join(root, 'apps', 'fba-replenishment', 'inbound-history.js'), 'utf8');
  assert.equal((src.match(/flushInboundDb\(dbGeneration\)/g) || []).length, 2, '明細の 2 か所の保存が世代を渡していない');
});
await t('ふつうの 1 プロセスの連続した保存は止めない / ファイルが消えていたらそのまま書く / 書きかけで止まったファイルは、読み込まない (FBA_DB_FILE_TORN)・正しいメモリを持つ側の保存で書き直す', async () => {
  const F = await import(dbUrl + '?proc=F'); await F.initDb();
  for (let i = 0; i < 5; i++) F.updateSetting('n', String(i));
  assert.equal(F.getSettings().n, '4');
  fs.rmSync(dbFile);
  F.updateSetting('n', 'after-delete');
  assert.equal(readFile().settings.n, 'after-delete');
  const full = fs.readFileSync(dbFile);
  fs.writeFileSync(dbFile, full.subarray(0, Math.floor(full.length / 2)));   // 保存の途中でプロセスが落ちた形
  const G = await import(dbUrl + '?proc=G');
  await assert.rejects(G.initDb(), (e) => e.code === 'FBA_DB_FILE_TORN');
  F.updateSetting('n', 'after-torn');
  assert.deepEqual([fs.statSync(dbFile).size >= full.length, readFile().settings.n], [true, 'after-torn']);
});
await t('🚨 lock の排他 (同じプロセスの別の接続): A が lock を持っている間に B が保存しに来ると、B は書かずに FBA_DB_LOCK_TIMEOUT。A の保存の後、B は「外から書かれた」に気づく → やり直して両方残る / lock の中で例外が出ても lock は外れる', async () => {
  const H = await import(dbUrl + '?proc=H'); await H.initDb();
  const I = await import(dbUrl + '?proc=I'); await I.initDb();
  await tick(); assert.equal(codeOf(() => H.updateSetting('warm', '1')), 'FBA_DB_EXTERNAL_WRITE'); H.updateSetting('warm', '1');   // H を最新に (I の initDb が最後に保存している)
  let inner = 'not-called';
  H._testHooks.insideSaveLock = () => { H._testHooks.insideSaveLock = null; inner = codeOf(() => I.updateSetting('from_i', '1')); };
  H.updateSetting('from_h', '1');
  assert.equal(inner, 'FBA_DB_LOCK_TIMEOUT', 'H が lock を持っている間に I が保存区間へ入れた');
  assert.deepEqual([readFile().settings.from_h, readFile().settings.from_i], ['1', undefined]);
  assert.equal(codeOf(() => I.updateSetting('from_i', '1')), 'FBA_DB_EXTERNAL_WRITE');
  I.updateSetting('from_i', '1');
  assert.deepEqual([readFile().settings.from_h, readFile().settings.from_i], ['1', '1']);
  I._testHooks.insideSaveLock = () => { I._testHooks.insideSaveLock = null; throw new Error('boom inside lock'); };
  let boom = null; try { I.updateSetting('x', '1'); } catch (e) { boom = e; }
  assert.deepEqual([boom && boom.code, boom && boom.cause && boom.cause.message, I.isFbaDbConflict(boom)], ['FBA_DB_SAVE_FAILED', 'boom inside lock', true], '保存の途中の失敗は FBA_DB_* にそろえる (原因は cause に残す)');
  I.updateSetting('x', '2');   // 例外の後も lock は外れている (取れなければ FBA_DB_LOCK_TIMEOUT)
  assert.equal(readFile().settings.x, '2');
  // 🚨 lock 用のファイルを開けない (SQLITE_CANTOPEN など) も FBA_DB_* (Codex R3 #1): 素の SQLITE_* のまま出すと、保存の失敗を警告に落とす catch を素通りして「保存していないのに成功」になる
  const lockDbFile = dbFile + '.lockdb';
  fs.rmSync(lockDbFile, { force: true }); fs.mkdirSync(lockDbFile);
  let cant = null; try { I.updateSetting('x', '3'); } catch (e) { cant = e; }
  fs.rmdirSync(lockDbFile);
  assert.deepEqual([cant && cant.code, I.isFbaDbConflict(cant), !!(cant && cant.cause)], ['FBA_DB_LOCK_ERROR', true, true]);
  assert.equal(readFile().settings.x, '2', 'lock を取れないのに書いている');
  assert.deepEqual([I.isFbaDbConflict(Object.assign(new Error('x'), { code: 'SQLITE_CANTOPEN' })), I.isFbaDbConflict(new Error('guard')), I.isFbaDbConflict(null)], [false, false, false]);
  I.updateSetting('x', '3');
  assert.equal(readFile().settings.x, '3');
});
const holder = (mode) => {
  // 子プロセス: lock を持ったまま 'inside' と書いて止まる。mode = 'finish' (4 秒後に保存して終わる。親の待ち = 約 1 秒より十分長く) / 'die' (親に殺されるまで止まる = 保存しない)
  const code = `
    import fs from 'node:fs';
    const db = await import(${JSON.stringify(dbUrl)});
    await db.initDb();
    db._testHooks.insideSaveLock = () => { db._testHooks.insideSaveLock = null; fs.writeSync(1, 'inside\\n'); Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ${mode === 'die' ? 60000 : 4000}); };
    db.updateSetting('child_${mode}', '1');
    fs.writeSync(1, 'saved\\n');`;
  const cp = spawn(process.execPath, ['--input-type=module', '-e', code], { env: { ...process.env, DATA_DIR: tmp, FBA_DB_LOCK_WAIT_MS: '20000' }, stdio: ['ignore', 'pipe', 'pipe'] });
  let out = '', err = '';
  cp.stdout.on('data', (d) => { out += d; }); cp.stderr.on('data', (d) => { err += d; });
  const until = (word) => new Promise((resolve, reject) => { const t0 = Date.now(); const iv = setInterval(() => { if (out.includes(word)) { clearInterval(iv); resolve(); } else if (cp.exitCode !== null || Date.now() - t0 > 30000) { clearInterval(iv); reject(new Error(`child (${mode}) は '${word}' を出さずに終わった: ${err.slice(-300)}`)); } }, 20); });
  const exited = new Promise((resolve) => cp.on('exit', resolve));
  return { cp, until, exited };
};
await t('🚨 lock の排他 (本物の別プロセス・順序を合図で固定): 子が lock を持っている間、親の保存は書かずに FBA_DB_LOCK_TIMEOUT → 子が保存して終わった後、親は「外から書かれた」→ やり直して両方残る', async () => {
  const P = await import(dbUrl + '?proc=P'); await P.initDb();
  const h = holder('finish');
  await h.until('inside');                                   // ← 子は保存区間の中 (確かめた後・書く前)
  assert.equal(codeOf(() => P.updateSetting('parent', '1')), 'FBA_DB_LOCK_TIMEOUT');
  await h.until('saved'); await h.exited;
  assert.equal(readFile().settings.child_finish, '1');
  assert.equal(codeOf(() => P.updateSetting('parent', '1')), 'FBA_DB_EXTERNAL_WRITE');
  P.updateSetting('parent', '1');
  assert.deepEqual([readFile().settings.child_finish, readFile().settings.parent], ['1', '1']);
});
await t('🚨 lock を持ったままプロセスが死んでも、lock は残らない (OS が外す = 古い lock を捨てる処理が要らない。Codex R2 #1・#2): 子を保存区間の中で殺す → 親の保存はすぐ通る・ファイルは壊れていない', async () => {
  const P = await import(dbUrl + '?proc=Q'); await P.initDb();
  const h = holder('die');
  await h.until('inside');
  h.cp.kill('SIGKILL'); await h.exited;
  await tick();
  const t0 = Date.now();
  // 子の initDb() が最後に保存しているので、1 回目は「外から書かれた」。lock が残っていたら FBA_DB_LOCK_TIMEOUT になる
  assert.equal(codeOf(() => P.updateSetting('after_kill', '1')), 'FBA_DB_EXTERNAL_WRITE');
  P.updateSetting('after_kill', '1');
  assert.ok(Date.now() - t0 < 5000, 'lock を待っている');
  assert.deepEqual([readFile().settings.after_kill, readFile().settings.child_die], ['1', undefined]);
});
await t('起動と重なった外からの書き込みで初期化を失敗のままにしない (読む → 表をそろえる → 保存 の保存で気づいたら、最初からやり直す。やり直さないと FBA の画面が再起動まで 503。Codex R1 #6)', async () => {
  const W = await import(dbUrl + '?proc=W'); await W.initDb();
  const S = await import(dbUrl + '?proc=S');
  let hits = 0;
  S._testHooks.afterLoad = async () => { hits++; if (hits === 1) { await tick(); W.updateSetting('during_boot', 'w'); await tick(); } };
  await S.initDb();
  S._testHooks.afterLoad = null;
  assert.deepEqual([hits, S.getSettings().during_boot], [2, 'w'], '1 回目は外からの書き込みに気づいてやり直し、2 回目で W の値を持って起動する');
  S.updateSetting('after_boot', 's');
  assert.deepEqual([readFile().settings.during_boot, readFile().settings.after_boot], ['w', 's']);
});
await t('本物の 2 プロセスが保存し続けても、どちらの行も消えない (負荷の試験。排他そのものは上の「合図で順序を固定した」試験が確かめる。やり直しの回数は参考 = 起動の速さで変わる)', async () => {
  const startAt = Date.now() + 2500;   // 両方が initDb() を終えてから、同じ時刻に書き始める (重ならないと、この試験は何も確かめない)
  const child = (name) => new Promise((resolve, reject) => {
    const code = `
      const db = await import(${JSON.stringify(dbUrl)});
      const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
      await db.initDb();
      await sleep(Math.max(0, ${startAt} - Date.now()));
      let retries = 0;
      for (let i = 0; i < 12; i++) {
        await sleep(8);
        for (let n = 0; ; n++) {
          try { db.updateSetting('${name}_' + i, String(i)); break; }
          catch (e) { if ((e.code !== 'FBA_DB_EXTERNAL_WRITE' && e.code !== 'FBA_DB_LOCK_TIMEOUT') || n > 200) throw e; retries++; }
        }
      }
      console.log('done ${name} retries=' + retries);`;
    const p = spawn(process.execPath, ['--input-type=module', '-e', code], { env: { ...process.env, DATA_DIR: tmp, FBA_DB_LOCK_WAIT_MS: '20000' }, stdio: ['ignore', 'pipe', 'pipe'] });
    let out = '', err = '';
    p.stdout.on('data', (d) => { out += d; }); p.stderr.on('data', (d) => { err += d; });
    p.on('exit', (c) => (c === 0 ? resolve(out.trim()) : reject(new Error(`child ${name} exit ${c}: ${err.slice(-400)}`))));
  });
  const outs = await Promise.all([child('p'), child('q')]);
  const s = readFile().settings;
  const missing = []; for (const n of ['p', 'q']) for (let i = 0; i < 12; i++) if (s[`${n}_${i}`] !== String(i)) missing.push(`${n}_${i}`);
  assert.deepEqual(missing, [], `消えた行: ${missing.join(', ')} (${outs.join(' / ')})`);
  assert.ok(outs.every((o) => /^done [pq] retries=\d+$/.test(o.split('\n').pop())), outs.join(' / '));
  console.log('      (' + outs.map((o) => o.split('\n').pop()).join(' / ') + ' = 例外になってやり直した回数)');
});

console.log('② 朝の cron: 常駐サーバに頼んで待つ / 頼めなければ自分では書かずに失敗');
const { snapshotViaServer, runSnapshotCli, isConnectionRefused, isServerListening, resolveBusinessDate } = await import(pathToFileURL(path.join(root, 'apps', 'warehouse', 'snapshot-fba-stock.js')).href);
const json = (status, body) => ({ ok: status >= 200 && status < 300, status, json: async () => body });
const refused = () => Object.assign(new TypeError('fetch failed'), { cause: Object.assign(new Error('connect ECONNREFUSED 127.0.0.1:3000'), { code: 'ECONNREFUSED' }) });
const via = (fetchImpl, extra = {}) => snapshotViaServer({ businessDate: '2026-09-20', base: 'http://127.0.0.1:3000', token: 'tok', fetchImpl, sleepFn: async () => {}, log: quiet, ...extra });
const jobRes = (job) => json(200, { ok: true, job });   // GET /service-api/jobs/:id の応答の形 (service-router.js: okResponse(res, { job }))
await t('応答の形は実物と同じ: ジョブの確認は { ok, job: {...} } (service-router.js)・頼んだときは { ok, jobId, status, businessDate } (fba-service.js)。試験の形がソースとずれたら落ちる (R1 #1 = 形を読み違えて、終わったジョブに気づかず必ず時間切れだった)', async () => {
  const sr = fs.readFileSync(path.join(root, 'apps', 'warehouse', 'service-router.js'), 'utf8');
  assert.match(sr, /router\.get\('\/jobs\/:jobId'[\s\S]{0,400}?okResponse\(res, \{ job \}\);/);
  const eh = fs.readFileSync(path.join(root, 'apps', 'warehouse', 'error-handler.js'), 'utf8');
  assert.match(eh, /res\.status\(statusCode\)\.json\(\{ ok: true, \.\.\.data \}\)/);
  const jm = fs.readFileSync(path.join(root, 'apps', 'warehouse', 'job-manager.js'), 'utf8');
  assert.match(jm, /status: job\.status,\s*\n\s*progress: job\.progress,\s*\n\s*result: job\.status === 'completed' \? job\.result : undefined,/);
  const svc = fs.readFileSync(path.join(root, 'apps', 'warehouse', 'fba-service.js'), 'utf8');
  assert.match(svc, /okResponse\(res, \{ \.\.\.job, businessDate \}, 202\);/);
  assert.match(svc, /okResponse\(res, \{ jobId: snapshotReportsJobId, businessDate: snapshotReportsDate, status: 'already_running'/);
});
await t('頼む → ジョブを待つ (running → completed) → 結果をそのまま返す。business_date とトークンを付ける', async () => {
  const calls = []; let polls = 0;
  const f = async (url, init = {}) => {
    calls.push([init.method || 'GET', url.replace('http://127.0.0.1:3000', ''), init.headers && init.headers['x-service-token'], init.body || null]);
    if ((init.method || 'GET') === 'POST') return json(202, { ok: true, jobId: 'job-1', status: 'running', businessDate: '2026-09-20' });
    polls++;
    return polls < 3 ? jobRes({ jobId: 'job-1', status: 'running', progress: { step: 'fetching' } }) : jobRes({ jobId: 'job-1', status: 'completed', result: { ok: true, lastLine: '✅ FBA在庫スナップショット 2026-09-20: JP restock=3992 planning=3999 / US planning=15 restock=15' } });
  };
  const r = await via(f);
  assert.deepEqual([r.mode, r.result.ok, polls], ['server', true, 3]);
  assert.deepEqual(calls[0], ['POST', '/service-api/fba/snapshot-reports', 'tok', JSON.stringify({ businessDate: '2026-09-20' })]);
  assert.deepEqual(calls[1].slice(0, 3), ['GET', '/service-api/jobs/job-1', 'tok']);
});
await t('🚨 頼めないときは例外 (自分では書かない): 応答なし (timeout)・401・404・500 / 接続拒否だけが not_running', async () => {
  assert.deepEqual(await via(async () => { throw refused(); }), { mode: 'not_running' });
  assert.equal(isConnectionRefused(Object.assign(new TypeError('fetch failed'), { cause: new AggregateError([Object.assign(new Error('x'), { code: 'ECONNREFUSED' }), Object.assign(new Error('y'), { code: 'ECONNREFUSED' })]) })), true);
  assert.equal(isConnectionRefused(Object.assign(new TypeError('fetch failed'), { cause: new AggregateError([Object.assign(new Error('x'), { code: 'ECONNREFUSED' }), Object.assign(new Error('y'), { code: 'ETIMEDOUT' })]) })), false);
  await assert.rejects(via(async () => { throw Object.assign(new Error('The operation was aborted due to timeout'), { name: 'TimeoutError' }); }), /自分では fba\.db に書かない/);
  await assert.rejects(via(async () => json(401, { ok: false })), /HTTP 401 = SERVICE_TOKEN/);
  await assert.rejects(via(async () => json(404, { ok: false })), /HTTP 404 = 常駐サーバが古い版/);
  await assert.rejects(via(async () => json(500, { ok: false, message: 'レポート取得の lock を作れない: EPERM' })), /HTTP 500: レポート取得の lock を作れない/);
  assert.deepEqual([await isServerListening('http://x', async () => json(401, {})), await isServerListening('http://x', async () => { throw refused(); })], [true, false]);
  await assert.rejects(isServerListening('http://x', async () => { throw new Error('socket hang up'); }), /居るか分からない/);
});
await t('ジョブの失敗・ジョブが消えた (途中で再起動)・時間切れ・知らない形は例外 / 確認の一時的な失敗は続ける', async () => {
  const post = json(202, { ok: true, jobId: 'job-2', status: 'running', businessDate: '2026-09-20' });
  const seq = (answers) => { let i = 0; return async (url, init = {}) => ((init.method || 'GET') === 'POST' ? post : answers[Math.min(i++, answers.length - 1)]()); };
  await assert.rejects(via(seq([() => jobRes({ status: 'failed', error: { code: 'X', message: 'SP-API 403' } })])), /ジョブが失敗: SP-API 403/);
  await assert.rejects(via(seq([() => json(404, { ok: false, error: 'JOB_NOT_FOUND' })])), /知らない \(途中で再起動した\?\)/);
  let clock = 0;
  await assert.rejects(via(seq([() => jobRes({ status: 'running' })]), { now: () => (clock += 60000), waitMs: 5 * 60000 }), /5 分で終わらない/);
  await assert.rejects(via(seq([() => json(200, { ok: true, status: 'completed', result: { ok: true } })])), /ジョブの応答の形が違う/, 'job で包まれていない応答を「実行中」と読み続けない');
  const flaky = await via(seq([() => { throw new Error('socket hang up'); }, () => json(503, null), () => jobRes({ status: 'completed', result: { ok: true, lastLine: '✅ x' } })]));
  assert.equal(flaky.mode, 'server');
});
await t('🚨 「既に実行中」を成功のスキップにしない (R1 #4): 同じ日付のスナップショットなら、そのジョブの終わりを待つ (失敗なら失敗) / UI の手動取得・別の日付なら、終わるのを待って頼み直す / 待ち切れなければ例外', async () => {
  const same = async (url, init = {}) => ((init.method || 'GET') === 'POST' ? json(202, { ok: true, status: 'already_running', jobId: 'job-9', businessDate: '2026-09-20' }) : jobRes({ status: 'failed', error: { message: 'RESTOCK 403' } }));
  await assert.rejects(via(same), /ジョブが失敗: RESTOCK 403/);
  let posts = 0; const slept = [];
  const manualThenOk = async (url, init = {}) => {
    if ((init.method || 'GET') !== 'POST') return jobRes({ status: 'completed', result: { ok: true, lastLine: '✅ y' } });
    posts++;
    if (posts === 1) return json(202, { ok: true, status: 'already_running', holder: { source: 'manual', pid: 1 } });
    if (posts === 2) return json(202, { ok: true, status: 'already_running', jobId: 'job-old', businessDate: '2026-09-19' });   // 別の日付のジョブには相乗りしない
    return json(202, { ok: true, jobId: 'job-3', status: 'running', businessDate: '2026-09-20' });
  };
  const r = await via(manualThenOk, { sleepFn: async (ms) => { slept.push(ms); }, busyRetryMs: 30000 });
  assert.deepEqual([r.mode, posts, slept.filter((x) => x === 30000).length], ['server', 3, 2]);
  let clock = 0;
  await assert.rejects(via(async () => json(202, { ok: true, status: 'already_running', holder: { source: 'manual' } }), { now: () => (clock += 60000), waitMs: 3 * 60000, busyRetryMs: 30000 }), /ほかの取得が 3 分待っても終わらず、頼めなかった/);
  // 期限の後に要求を始めない (Codex R3 Low): 時計は sleep の中でだけ進む。期限を過ぎた後の要求は 0 回
  let t2 = 0, late = 0;
  const f2 = async (url, init = {}) => { if (t2 >= 100000) late++; return (init.method || 'GET') === 'POST' ? json(202, { ok: true, jobId: 'job-7', status: 'running', businessDate: '2026-09-20' }) : jobRes({ status: 'running' }); };
  await assert.rejects(via(f2, { now: () => t2, sleepFn: async (ms) => { t2 += ms; }, waitMs: 100000, pollMs: 30000 }), /で終わらない/);
  assert.equal(late, 0, '期限を過ぎてから要求を始めている');
});
await t('CLI: 常駐サーバが起動していなくても自分では書かない (exit 1) / --direct は接続拒否のときだけ自分で書く・待っているプロセスが居れば拒む / 終了コードは結果の ok / 最後の行に経路 / business_date の解決', async () => {
  const env = { WAREHOUSE_BUSINESS_DATE: '2026-09-20', PORT: '3100', SERVICE_TOKEN: 'tok' };
  let directCalls = 0; const direct = async (d) => { directCalls++; return { mode: 'direct', result: { ok: true, lastLine: `✅ FBA在庫スナップショット ${d}: JP restock=1 planning=1 / US 未設定` } }; };
  const seenBase = [];
  const r1 = await runSnapshotCli({ env, argv: ['7'], viaServer: async (o) => { seenBase.push([o.base, o.token, o.businessDate]); return { mode: 'not_running' }; }, direct, log: quiet });
  assert.deepEqual([r1.exitCode, directCalls, seenBase[0]], [1, 0, ['http://127.0.0.1:3100', 'tok', '2026-09-20']]);
  assert.match(r1.lastLine, /^❌ FBA在庫スナップショット: 常駐サーバが起動していない .*自分では書かない/);
  const r2 = await runSnapshotCli({ env, argv: [], viaServer: async () => ({ mode: 'server', result: { ok: false, lastLine: '❌ FBA在庫スナップショット 2026-09-20: JP のレポートが 1 つも取れなかった' } }), direct, log: quiet });
  assert.deepEqual([r2.exitCode, directCalls, r2.lastLine.endsWith('[常駐サーバ経由]')], [1, 0, true]);
  const r3 = await runSnapshotCli({ env, argv: [], viaServer: async () => ({ mode: 'server', result: { ok: true, lastLine: '✅ FBA在庫スナップショット 2026-09-20: JP restock=3992 planning=3999 / US planning=15 restock=15' } }), direct, log: quiet });
  assert.deepEqual([r3.exitCode, r3.lastLine.startsWith('✅ FBA在庫スナップショット 2026-09-20')], [0, true]);
  await assert.rejects(runSnapshotCli({ env, argv: [], viaServer: async () => { throw new Error('常駐サーバに頼めなかった (HTTP 401)'); }, direct, log: quiet }), /HTTP 401/);
  let viaCalls = 0;
  const d1 = await runSnapshotCli({ env, argv: ['--direct'], viaServer: async () => { viaCalls++; return { mode: 'not_running' }; }, direct, listening: async () => false, log: quiet });
  assert.deepEqual([d1.exitCode, directCalls, viaCalls, d1.lastLine.endsWith('[直接 (--direct)]')], [0, 1, 0, true]);
  const d2 = await runSnapshotCli({ env, argv: ['--direct'], viaServer: async () => { viaCalls++; return { mode: 'not_running' }; }, direct, listening: async () => true, log: quiet });
  assert.deepEqual([d2.exitCode, directCalls, /--direct は常駐サーバを止めてあるときだけ/.test(d2.lastLine)], [1, 1, true]);
  assert.equal((await runSnapshotCli({ env, argv: [], viaServer: async () => ({ mode: 'server', result: { lastLine: '✅ 形が足りない' } }), direct, log: quiet })).exitCode, 1);
  assert.equal((await runSnapshotCli({ env: { WAREHOUSE_BUSINESS_DATE: '2026-13-01' }, argv: [], viaServer: async () => ({ mode: 'not_running' }), direct, log: quiet })).exitCode, 1);
  assert.deepEqual([resolveBusinessDate({}, ['7', '--date=2026-09-01']), resolveBusinessDate({}, ['7'], new Date('2026-09-19T16:00:00Z'))], ['2026-09-01', '2026-09-20'], 'daily-sync の runScript は引数が無いと 7 を足す / 既定は JST');
});

console.log('③ 本体: 保存の順番と、黙った緑にしないこと');
const { runFbaReportSnapshot, isBusinessDate } = await import(pathToFileURL(path.join(root, 'apps', 'warehouse', 'fba-report-snapshot.js')).href);
const fakeDb = (over = {}) => {
  const calls = [];
  const rec = (name, ret) => (...args) => { calls.push([name, ...args.map((a) => (Array.isArray(a) ? a.length : a && typeof a === 'object' ? Object.keys(a).sort().join(',') : a))]); return typeof ret === 'function' ? ret(...args) : ret; };
  return { calls, saveRestockInventoryToDailySnapshot: rec('restockDaily', (rows) => ({ updated: 0, inserted: rows.length })), saveRestockLatest: rec('restockLatest', (rows) => ({ saved: rows.length })), updateFnskuBatch: rec('fnskuUpdate'),
    savePlanningData: rec('planning', (rows) => rows.length), savePlanningLatest: rec('planningLatest', (rows) => ({ saved: rows.length })), syncFnskuBatch: rec('fnskuSync'), saveUsDailySnapshots: rec('us', () => ({ inserted: 1, updated: 0 })), ...over };
};
const restockRow = (sku) => ({ 'Merchant SKU': sku, FNSKU: 'X00' + sku, ASIN: 'B0' + sku, Available: '3' });
const planningRow = (sku) => ({ sku, fnsku: 'X00' + sku, available: '3' });
const noUs = { market: 'us' };
await t('保存の順番は RESTOCK 先行 → PLANNING (在庫の区分を後から 0 で潰さない)。どの保存にも同じ business_date を渡す (UTC の今日にしない)。US は env がそろっているときだけ', async () => {
  const db = fakeDb();
  const r = await runFbaReportSnapshot({ db, businessDate: '2026-09-20', fetchReports: async () => ({ restock: [restockRow('a'), restockRow('b')], planning: [planningRow('a'), planningRow('b'), planningRow('c')], errors: [] }), usContext: noUs, log: quiet, warn: quiet });
  assert.deepEqual(db.calls.map((c) => c[0]), ['restockDaily', 'restockLatest', 'fnskuUpdate', 'planning', 'planningLatest', 'fnskuSync']);
  assert.deepEqual([db.calls[0][2], db.calls[3][2]], ['2026-09-20', '2026-09-20']);
  assert.deepEqual([r.ok, r.us, r.jp.restockDaily, r.jp.planning], [true, null, 2, 3]);
  assert.match(r.lastLine, /^✅ FBA在庫スナップショット 2026-09-20: JP restock=2 planning=3 \/ US 未設定$/);
  const db2 = fakeDb(); const seenCtx = [];
  const us = { market: 'us', refresh_token: 'r', client_id: 'c', client_secret: 's' };
  const r2 = await runFbaReportSnapshot({ db: db2, businessDate: '2026-09-20', fetchReports: async (ctx) => { seenCtx.push(ctx ? ctx.market : 'jp'); return { restock: [restockRow('a')], planning: [planningRow('a')], errors: [] }; }, usContext: us, log: quiet, warn: quiet });
  assert.deepEqual([seenCtx, db2.calls.at(-1)[0], r2.us.planning, /US planning=1 restock=1$/.test(r2.lastLine)], [['jp', 'us'], 'us', 1, true]);
  assert.doesNotThrow(() => JSON.stringify(r2), 'ジョブの結果として JSON で返せない');
});
await t('🚨 黙った緑にしない: JP が 1 つも取れなかった回は ok = false (今までは exit 0。9/17 の 403 の朝も ✅ だった) / RESTOCK だけ取れなかった回は ⚠️ +「0 ではなく不明」/ US の失敗は全体を落とさない', async () => {
  const none = await runFbaReportSnapshot({ db: fakeDb(), businessDate: '2026-09-17', fetchReports: async () => ({ restock: null, planning: null, errors: [{ report: 'restock', error: 'Access to requested resource is denied.' }, { report: 'planning', error: 'Access to requested resource is denied.' }] }), usContext: noUs, log: quiet, warn: quiet });
  assert.equal(none.ok, false);
  assert.match(none.lastLine, /^❌ FBA在庫スナップショット 2026-09-17: JP のレポートが 1 つも取れなかった \(restock: Access to requested/);
  const noRestock = await runFbaReportSnapshot({ db: fakeDb(), businessDate: '2026-09-20', fetchReports: async () => ({ restock: [], planning: [planningRow('a')], errors: [{ report: 'restock', error: 'timeout' }] }), usContext: noUs, log: quiet, warn: quiet });
  assert.equal(noRestock.ok, true);
  assert.match(noRestock.lastLine, /^⚠️ .*取れなかったレポート: restock.*RESTOCK が取れていない = この日の FC 移管中・処理中・出荷待ちは 0 ではなく不明/);
  const us = { market: 'us', refresh_token: 'r', client_id: 'c', client_secret: 's' };
  const usFail = await runFbaReportSnapshot({ db: fakeDb(), businessDate: '2026-09-20', fetchReports: async (ctx) => { if (ctx) throw new Error('US 403'); return { restock: [restockRow('a')], planning: [planningRow('a')], errors: [] }; }, usContext: us, log: quiet, warn: quiet });
  assert.deepEqual([usFail.ok, usFail.us.error, /US ❌ US 403/.test(usFail.lastLine)], [true, 'US 403', true]);
  assert.deepEqual([isBusinessDate('2026-09-20'), isBusinessDate('2026-02-30'), isBusinessDate('2026-9-1'), isBusinessDate(undefined)], [true, false, false, false]);
  await assert.rejects(runFbaReportSnapshot({ db: fakeDb(), businessDate: 'x', fetchReports: async () => ({}), usContext: noUs, log: quiet }), /business_date が不正/);
});
await t('🚨 「外から書き換えられていた」(FBA_DB_EXTERNAL_WRITE) は、失敗を警告にする try の中でも握りつぶさない (保存されていない回を成功にしない)', async () => {
  const ext = () => { throw Object.assign(new Error('fba.db がほかのプロセスに書き換えられていた'), { code: 'FBA_DB_EXTERNAL_WRITE' }); };
  const fetchReports = async () => ({ restock: [restockRow('a')], planning: [planningRow('a')], errors: [] });
  await assert.rejects(runFbaReportSnapshot({ db: fakeDb({ saveRestockLatest: ext }), businessDate: '2026-09-20', fetchReports, usContext: noUs, log: quiet, warn: quiet }), (e) => e.code === 'FBA_DB_EXTERNAL_WRITE');
  await assert.rejects(runFbaReportSnapshot({ db: fakeDb({ savePlanningLatest: ext }), businessDate: '2026-09-20', fetchReports, usContext: noUs, log: quiet, warn: quiet }), (e) => e.code === 'FBA_DB_EXTERNAL_WRITE');
  const us = { market: 'us', refresh_token: 'r', client_id: 'c', client_secret: 's' };
  await assert.rejects(runFbaReportSnapshot({ db: fakeDb({ saveUsDailySnapshots: ext }), businessDate: '2026-09-20', fetchReports, usContext: us, log: quiet, warn: quiet }), (e) => e.code === 'FBA_DB_EXTERNAL_WRITE');
  const soft = await runFbaReportSnapshot({ db: fakeDb({ saveRestockLatest: () => { throw new Error('guard'); } }), businessDate: '2026-09-20', fetchReports, usContext: noUs, log: quiet, warn: quiet });
  assert.deepEqual([soft.ok, soft.jp.restockLatest], [true, 0]);
});
await t('常駐サーバの口と cron の形 (ソース): 口は lock を取り、本体 (runFbaReportSnapshot) を常駐の DB で呼ぶ・lock を作れないのは「実行中」に混ぜない / cron が自分で initDb() するのは --direct の中だけ / 保存の失敗を警告に落とす catch は fba.db の競合を投げ直す (UI 取得 2・Render 同期 2・納品除外 1)', async () => {
  const svcAll = fs.readFileSync(path.join(root, 'apps', 'warehouse', 'fba-service.js'), 'utf8');
  assert.equal((svcAll.match(/if \(db\.isFbaDbConflict\(e\)\) throw e;/g) || []).length, 2);
  const rt = fs.readFileSync(path.join(root, 'apps', 'fba-replenishment', 'router.js'), 'utf8');
  assert.equal((rt.match(/if \(isFbaDbConflict\(e\)\) throw e;/g) || []).length, 2);
  assert.match(rt, /try \{ removeProvisionalItem\(amazon_sku\); \} catch \(e\) \{[\s\S]{0,500}?if \(isFbaDbConflict\(e\)\) return res\.status\(409\)\.json\(/);
  const svc = fs.readFileSync(path.join(root, 'apps', 'warehouse', 'fba-service.js'), 'utf8');
  const i = svc.indexOf("router.post('/snapshot-reports'"), j = svc.indexOf("router.post('/pml/fba-refresh'");
  assert.ok(i > 0 && j > i);
  const route = svc.slice(i, j);
  assert.match(route, /acquireFbaFetchLock\('cron-via-server'\)/);
  assert.match(route, /lock\.holder && lock\.holder\.error[\s\S]{0,200}?FBA_FETCH_LOCK_ERROR/);
  assert.match(route, /runFbaReportSnapshot\(\{ db, businessDate, log \}\)/);
  assert.match(route, /releaseFbaFetchLock\(lock\)/);
  const cron = fs.readFileSync(path.join(root, 'apps', 'warehouse', 'snapshot-fba-stock.js'), 'utf8');
  assert.equal((cron.match(/initDb\(\)/g) || []).length, 1);
  assert.match(cron, /async function snapshotDirect[\s\S]*?await db\.initDb\(\);[\s\S]*?\n}/);
  assert.equal(/from '\.\.\/fba-replenishment\/db\.js'/.test(cron), false, 'cron が db.js を静的に import している (常駐サーバ経由のときも fba.db を開く準備をしてしまう)');
});

try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* Windows: lock 用の SQLite を開いたままの module があるので消せないことがある (一時ディレクトリに残るだけ) */ }
console.log(`\n${ok} ok / ${ng} NG`);
process.exit(ng ? 1 : 0);
