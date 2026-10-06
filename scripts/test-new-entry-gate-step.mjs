/**
 * test-new-entry-gate-step.mjs — 毎朝の照合 ② の次の 1 段「新商品の許可」(PR-7・計画 newentry_min_plan.md §3 の 2)
 *
 * 固定する契約:
 *   1 照合 (マスタ照合) が失敗・見送り = この段を流さない (⏭️・blocked = この段だけを retry に載せない)。成功 (⚠️ を含む) = 流す
 *   2 許可が出た = 「🆕 新商品の入口: 開 (〜10/08 07:00 JST」(DB の expires_at)・exit 0・grant は ('single', その朝の照合の回) で 1 回
 *   3 拒まれた = 新しい接続で revoke → 閉じたのを確かめて理由つきで「閉」・exit 1。widen の前だけで拒まれた = ⏸️ 閉・exit 0 (準備中)
 *   4 接続先が無い = 「未設定」・exit 0・接続しない / 関数が無い (0058 の前) = 「0058 の前」・exit 0・grant も revoke も呼ばない
 *   5 照合 ② が判定できない・落ちた・完了していない = grant を呼ばない (新しい接続で閉じて確かめる・exit 0) / 証跡が無い・別の回・別の日 = exit 1
 *   6 接続できない・問い合わせの失敗 = exit 1 (新しい接続で閉じて確かめる)
 *   7 本物のプロセス (CLI): 未設定 = 最後の行が「未設定」で exit 0
 *   8 daily-sync の配線: 照合の直後 (ロジザードの影の前)・skipAfterCompare で守る・retry の対象・失敗したら「マスタ照合」も retry に載せる
 *   9 retry: JOB_DEFINITIONS・RETRY_ORDER (照合より後)・RERUN_AFTER・UPSTREAM_OF・許可だけが残っても照合からやり直す
 *  10 台帳: warehouse-daily-sync の purpose / runbook に載る (新しいエントリは作らない)・写しの門の一覧 (止めない側) に載る
 *  11 (#1645 Codex R1 High) grant の後に成功を確かめられない = 新しい接続で revoke → new_entry_lease_valid = false を確かめて「閉」・
 *     確かめられない = 「⚠️ 状態不明 (開いている可能性)」・exit 1。「閉」と出すのは閉じたのを確かめたときだけ
 *  12 (#1645 Codex R2 High) DATA_DIR が無い・引数の間違い・最上位の例外も、URL があれば新しい接続で閉じて確かめる (開いた許可が閉じる)・
 *     確かめられない = 状態不明。URL が無い = 「閉」と言わない。どの試験でも「閉」の行は、偽の DB が閉じたのを確かめた回だけ (run・cli の共通の確かめ)
 * 使い方: node scripts/test-new-entry-gate-step.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const S = await import('../apps/company-db/master-compare/new-entry-gate.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }

const NOW = new Date('2026-10-07T00:30:00Z');   // JST 2026-10-07 09:30
const AS_OF = '2026-10-07';
const RUN = 'ds_20261006220000000';
const CR = 'mc_20261006T221500123Z_abc123';
const ENV = { COMPANY_DB_NEW_ENTRY_GATE_URL: 'postgres://new_entry_gate@x/db', DAILY_SYNC_RUN_ID: RUN };
const evOk = (over = {}) => ({ name: 'master-compare', state: 'complete', as_of: AS_OF, sync_run_id: RUN, compare_run_id: CR, verdict: 'pass', ne: { verdict: 'pass' }, ...over });

/**
 * 偽の DB (0058 の関数の約束どおり: 拒む = 'lease_denied: …' の例外)。接続ごとに番号 (1 から) を付け、問い合わせはどの接続かを残す。
 *   DB の状態 = 許可が開いているか (grant で開く・revoke で閉じる・new_entry_lease_valid が読む)
 *   grantDropped = grant は DB で通った (開いた) が応答が切れた・その接続はもう使えない
 */
function fakeDb({ hasFn = true, grant = undefined, grantError = null, grantDropped = false, revokeError = null, validError = null, validAfterRevoke = false,
  queryError = null, connectFailFrom = null, openAtStart = false } = {}) {
  const calls = [];
  let closed = 0, n = 0, open = openAtStart, verified = false;
  const connect = async () => {
    n++;
    if (connectFailFrom != null && n >= connectFailFrom) throw new Error(`ECONNREFUSED (接続 ${n})`);
    const id = n;
    let dead = false;
    const db = {
      async query(text, params) {
        calls.push({ conn: id, text, params });
        if (dead) throw new Error('Client was closed and is not queryable');
        if (queryError && id === 1) throw new Error(queryError);
        if (/to_regprocedure\('ops\.grant_new_entry_lease\(text, text\)'\)/.test(text)) return { rows: [{ ok: hasFn }] };
        if (/ops\.grant_new_entry_lease\(\$1, \$2\)/.test(text)) {
          if (grantError) { const e = new Error(grantError); e.code = 'P0001'; throw e; }
          open = true;
          if (grantDropped) { dead = true; throw new Error('Connection terminated unexpectedly'); }
          return { rows: [{ r: grant !== undefined ? grant : { lease_id: '7', kind: 'single', result_id: '42', compare_run_id: params[1], expires_at: '2026-10-07T22:00:00+00:00' } }] };
        }
        if (/ops\.revoke_new_entry_lease\(\$1, \$2\)/.test(text)) {
          if (revokeError) throw new Error(revokeError);
          open = false;
          return { rows: [{ r: { revoked: 1, floor_result_id: '42' } }] };
        }
        if (/ops\.new_entry_lease_valid\(\$1\)/.test(text)) {
          if (validError) throw new Error(validError);
          const v = validAfterRevoke ? true : open;
          if (v === false) verified = true;
          return { rows: [{ v }] };
        }
        throw new Error(`知らない問い合わせ: ${text}`);
      },
    };
    return { db, close: async () => { closed++; } };
  };
  return { calls, connect, closed: () => closed, connections: () => n, isOpen: () => open, verifiedClosed: () => verified };
}
/** 「閉」の語 (閉じ… を除く) を出した行は、偽の DB が閉じたのを確かめた回だけ (#1645 Codex R2 High) */
function assertClosedOnlyIfVerified(r, f) {
  if (/閉(?!じ)/.test(r.line)) assert.ok(f.verifiedClosed() && !f.isOpen(), `確かめずに「閉」: ${r.line}`);
  return r;
}
const grants = (f) => f.calls.filter((c) => /grant_new_entry_lease\(\$1/.test(c.text));
const revokes = (f) => f.calls.filter((c) => /revoke_new_entry_lease/.test(c.text));
const valids = (f) => f.calls.filter((c) => /new_entry_lease_valid/.test(c.text));
/** revoke と確かめが、grant (や最初の確かめ) をした接続ではない新しい接続で、この順に走った */
function assertFreshCloseVerify(f) {
  const rv = revokes(f), va = valids(f);
  assert.equal(rv.length, 1, 'revoke は 1 回');
  assert.equal(va.length, 1, '確かめは 1 回');
  assert.notEqual(rv[0].conn, 1, 'revoke は新しい接続');
  assert.equal(va[0].conn, rv[0].conn, '確かめは revoke と同じ新しい接続');
  assert.ok(f.calls.indexOf(va[0]) > f.calls.indexOf(rv[0]), 'revoke の後に確かめる');
  assert.deepEqual(va[0].params, ['single']);
}
const run = async (f, { env = ENV, ev = evOk(), name = 'master-compare' } = {}) =>
  assertClosedOnlyIfVerified(await S.runGateStep({ env, dataDir: 'D:/fake', now: NOW, connect: f.connect, readEv: (d, a) => { assert.equal(a, AS_OF); return ev === null ? {} : { [name]: ev }; } }), f);

await ta('[1] 照合が失敗・見送り = 流さない (⏭️・blocked)。成功 (⚠️ を含む) = 流す', async () => {
  for (const r of [{ success: false, summary: '❌ x' }, { success: false, blocked: true, gated: true, summary: '⚠️ 見送り' }, undefined, null]) {
    const s = S.skipAfterCompare(r);
    assert.deepEqual([s.name, s.success, s.skipped, s.blocked], ['新商品の許可', false, true, true]);
    assert.match(s.summary, /^⏭️ 見送り \(マスタ照合が失敗 = この段は流さない・今朝は許可を出していない/);
    assert.doesNotMatch(s.summary, /閉/);   // 確かめていない = 「閉」と言わない
  }
  assert.equal(S.skipAfterCompare({ success: true, summary: '✅' }), null);
  assert.equal(S.skipAfterCompare({ success: true, summary: '⚠️ ②: 判定できない' }), null);   // blocked の判定はこの段が証跡で見る
});

await ta('[2] 許可が出た = 開 (〜DB の期限)・exit 0・grant は (single, その朝の照合の回) で 1 回・接続を閉じる・revoke しない', async () => {
  const f = fakeDb();
  const r = await run(f);
  assert.equal(r.code, 0);
  assert.equal(r.state, 'opened');
  assert.match(r.line, /^🆕 新商品の入口: 開 \(〜10\/08 07:00 JST・照合 mc_20261006T221500123Z_abc123・許可 #7\)$/);
  assert.deepEqual(grants(f).map((c) => c.params), [['single', CR]]);
  assert.equal(revokes(f).length + valids(f).length, 0);
  assert.deepEqual([f.closed(), f.connections(), f.isOpen()], [1, 1, true]);
  // ② の警告 (差がある) は許可を止めない = DB の関数が kind_gate で決める
  const g = fakeDb();
  assert.equal((await run(g, { ev: evOk({ verdict: 'breach', ne: { verdict: 'breach' } }) })).state, 'opened');
});

await ta('[3] 拒まれた = 新しい接続で revoke → 閉じたのを確かめて閉・exit 1 / widen の前だけ = ⏸️ 閉・exit 0 / 確かめられない = 状態不明・exit 1', async () => {
  const f = fakeDb({ grantError: 'lease_denied: kind_gate: 区分のゲートの数が 0 でない (E_only) / stop_floor: 結果の行 41 は止めた時点の行 (41) より新しくない' });
  const r = await run(f);
  assert.deepEqual([r.code, r.state], [1, 'denied']);
  assert.match(r.line, /^🆕 新商品の入口: 閉 \(拒まれた: kind_gate: 区分のゲートの数が 0 でない \(E_only\) \/ stop_floor: /);
  assertFreshCloseVerify(f);
  assert.equal(revokes(f)[0].params[0], 'single');
  assert.match(revokes(f)[0].params[1], /^許可を出せない \(mc_20261006T221500123Z_abc123\): lease_denied: kind_gate/);
  // widen の前 (区分の持ち主が company でない・widen の記録が無い) だけ = 準備中
  const p = fakeDb({ grantError: 'lease_denied: sku_kind_not_company: 区分の持ち主が company でない (widen の前) / not_widened: skus.sku_kind を広げた記録が無い' });
  const rp = await run(p);
  assert.deepEqual([rp.code, rp.state], [0, 'prep']);
  assert.match(rp.line, /^⏸️ 🆕 新商品の入口: 閉 \(widen の前 = 準備中: sku_kind_not_company/);
  assertFreshCloseVerify(p);
  // widen の前 + ほかの理由 = 拒まれた (exit 1)
  const q = await run(fakeDb({ grantError: 'lease_denied: not_widened: x / compare_run_mismatch: y' }));
  assert.deepEqual([q.code, q.state], [1, 'denied']);
  // 取り消しが落ちた = 状態不明・exit 1 (準備中でも)
  const v = fakeDb({ grantError: 'lease_denied: not_widened: x', revokeError: 'permission denied' });
  const rv = await run(v);
  assert.deepEqual([rv.code, rv.state], [1, 'unknown']);
  assert.match(rv.line, /^⚠️ 🆕 新商品の入口: 状態不明 \(開いている可能性・widen の前 = 準備中: .* \/ 取り消しも失敗 \(permission denied\)\)$/);
  // 取り消したのに有効と読めた・確かめが落ちた (new_entry_gate に EXECUTE が無いなど) = 状態不明
  const w = await run(fakeDb({ grantError: 'lease_denied: kind_gate: x', validAfterRevoke: true }));
  assert.deepEqual([w.code, w.state], [1, 'unknown']);
  assert.match(w.line, /取り消した後も有効と読めた \(true\)/);
  const x = await run(fakeDb({ grantError: 'lease_denied: kind_gate: x', validError: 'permission denied for function new_entry_lease_valid' }));
  assert.deepEqual([x.code, x.state], [1, 'unknown']);
  assert.match(x.line, /取り消したが閉じたかを読めない \(permission denied/);
  // 拒むのでない失敗 (鍵の待ちの打ち切りなど) = 許可を出せない・新しい接続で閉じて確かめる・exit 1
  const t = fakeDb({ grantError: 'canceling statement due to statement timeout' });
  const rt = await run(t);
  assert.deepEqual([rt.code, rt.state], [1, 'error']);
  assert.match(rt.line, /^🆕 新商品の入口: 閉 \(許可を出せない: canceling statement/);
  assertFreshCloseVerify(t);
  assert.deepEqual(S.deniedCodes('lease_denied: kind_gate: a / shape: {"x": 1} / not_today: b'), ['kind_gate', 'shape', 'not_today']);
  assert.deepEqual(S.deniedCodes('別の失敗'), []);
});

await ta('[4] 接続先が無い = 未設定・exit 0・接続しない / 関数が無い (0058 の前) = 0058 の前・exit 0・grant も revoke も呼ばない', async () => {
  let connected = 0;
  for (const env of [{ DAILY_SYNC_RUN_ID: RUN }, { COMPANY_DB_NEW_ENTRY_GATE_URL: '  ', DAILY_SYNC_RUN_ID: RUN }]) {
    const r = await S.runGateStep({ env, dataDir: 'D:/fake', now: NOW, connect: async () => { connected++; throw new Error('x'); }, readEv: () => ({ 'master-compare': evOk() }) });
    assert.deepEqual([r.code, r.state], [0, 'not_configured']);
    assert.equal(r.line, '🆕 新商品の入口: 未設定 (COMPANY_DB_NEW_ENTRY_GATE_URL が無い = この段は飛ばした)');
  }
  assert.equal(connected, 0);
  const f = fakeDb({ hasFn: false });
  const r = await run(f);
  assert.deepEqual([r.code, r.state], [0, 'not_applied']);
  assert.equal(r.line, '🆕 新商品の入口: 0058 の前 (許可の関数が無い = 許可そのものが無い・この段は飛ばした)');
  assert.equal(grants(f).length + revokes(f).length + valids(f).length, 0);
  assert.equal(f.connections(), 1);
  // 照合 ② が判定できない朝でも 0058 の前なら同じ (exit 0)
  const g = fakeDb({ hasFn: false });
  assert.equal((await run(g, { ev: evOk({ ne: { verdict: 'blocked', blocked_reason: 'x' } }) })).code, 0);
  assert.equal(revokes(g).length, 0);
});

await ta('[5] 照合 ② が判定できない・落ちた・完了していない = grant を呼ばない (新しい接続で閉じて確かめる・exit 0) / 証跡が無い・別の回・別の日 = exit 1', async () => {
  const closedCases = [
    [evOk({ ne: { verdict: 'blocked', blocked_reason: 'material_not_matched' } }), /照合 ② が判定できない \(material_not_matched\)/],
    [evOk({ ne: { verdict: 'error', error: 'timeout' } }), /照合 ② が落ちた \(timeout\)/],
    [evOk({ ne: null }), /照合 ② が流れていない/],
    [{ state: 'skipped', as_of: AS_OF, sync_run_id: RUN, reason: 'COMPANY_DB_WATCH_URL が無い' }, /照合が完了していない \(skipped: COMPANY_DB_WATCH_URL が無い\)/],
    [{ state: 'running', as_of: AS_OF, sync_run_id: RUN, compare_run_id: CR }, /照合が完了していない \(running\)/],
    [{ state: 'failed', as_of: AS_OF, sync_run_id: RUN, compare_run_id: CR }, /照合が完了していない \(failed\)/],
  ];
  for (const [ev, re] of closedCases) {
    const f = fakeDb();
    const r = await run(f, { ev });
    assert.deepEqual([r.code, r.state], [0, 'compare_not_ready'], JSON.stringify(ev));
    assert.match(r.line, /^🆕 新商品の入口: 閉 \(/);
    assert.match(r.line, re);
    assert.equal(grants(f).length, 0);
    assertFreshCloseVerify(f);
  }
  const errorCases = [
    [null, /照合の証跡が無い/],
    [evOk({ sync_run_id: 'ds_other' }), /この daily-sync の回のものでない \(ds_other\)/],
    [evOk({ sync_run_id: null }), /この daily-sync の回のものでない \(なし\)/],
    [evOk({ as_of: '2026-10-06' }), /今日のものでない \(2026-10-06\)/],
    [evOk({ compare_run_id: null }), /照合の回 \(compare_run_id\) が無い/],
    [{ name: 'master-compare', error: '証跡が読めない: Unexpected token' }, /照合の証跡が読めない/],
  ];
  for (const [ev, re] of errorCases) {
    const f = fakeDb();
    const r = await run(f, { ev });
    assert.deepEqual([r.code, r.state], [1, 'error'], JSON.stringify(ev));
    assert.match(r.line, re);
    assert.equal(grants(f).length, 0);
  }
  // 閉じたのを確かめられない = 状態不明・exit 1
  const g = fakeDb({ revokeError: 'boom' });
  const rg = await run(g, { ev: evOk({ ne: { verdict: 'blocked' } }) });
  assert.deepEqual([rg.code, rg.state], [1, 'unknown']);
  assert.match(rg.line, /^⚠️ 🆕 新商品の入口: 状態不明 \(開いている可能性・照合 ② が判定できない .*取り消しも失敗 \(boom\)\)$/);
  // 手で流した回 (DAILY_SYNC_RUN_ID が無い) = 手の照合の証跡 master-compare.manual を読む
  const m = fakeDb();
  const rm = await run(m, { env: { COMPANY_DB_NEW_ENTRY_GATE_URL: 'postgres://x' }, ev: evOk({ sync_run_id: null }), name: 'master-compare.manual' });
  assert.equal(rm.state, 'opened');
  const m2 = fakeDb();
  assert.equal((await run(m2, { env: { COMPANY_DB_NEW_ENTRY_GATE_URL: 'postgres://x' }, ev: evOk() })).state, 'error');   // 朝の証跡は手の回には使わない
  assert.equal(grants(m2).length, 0);
});

await ta('[6] 接続できない・問い合わせの失敗 = exit 1 (新しい接続で閉じて確かめる。確かめられない = 状態不明)', async () => {
  // 最初の接続だけ落ちた → 新しい接続で閉じたのを確かめた = 閉
  const a = fakeDb({ connectFailFrom: 1 });
  let k = 0;
  const conn = async (u) => { k++; if (k === 1) throw new Error('ECONNREFUSED'); return a.connect(u); };
  const ra = await S.runGateStep({ env: ENV, dataDir: 'D:/fake', now: NOW, connect: conn, readEv: () => ({ 'master-compare': evOk() }) });
  // a.connect は 1 回目から落ちる設定 = 2 回目の接続 (閉じる側) も落ちる = 状態不明
  assert.deepEqual([ra.code, ra.state], [1, 'unknown']);
  assert.match(ra.line, /^⚠️ 🆕 新商品の入口: 状態不明 \(開いている可能性・接続できない: ECONNREFUSED \/ 取り消しの接続もできない/);
  const b = fakeDb();
  let kb = 0;
  const connB = async (u) => { kb++; if (kb === 1) throw new Error('ECONNREFUSED'); return b.connect(u); };
  const rb = await S.runGateStep({ env: ENV, dataDir: 'D:/fake', now: NOW, connect: connB, readEv: () => ({ 'master-compare': evOk() }) });
  assert.deepEqual([rb.code, rb.state], [1, 'error']);
  assert.match(rb.line, /^🆕 新商品の入口: 閉 \(接続できない: ECONNREFUSED\)$/);
  assert.equal(revokes(b).length, 1);
  assert.equal(valids(b).length, 1);
  // 最初の接続で問い合わせが落ちる = 新しい接続で閉じて確かめる
  const f = fakeDb({ queryError: 'connection terminated' });
  const r2 = await run(f);
  assert.deepEqual([r2.code, r2.state], [1, 'error']);
  assert.match(r2.line, /^🆕 新商品の入口: 閉 \(許可を出せない: connection terminated\)$/);
  assertFreshCloseVerify(f);
  assert.equal(f.closed(), 2);
});

await ta('[7] 本物のプロセス: 未設定 = 最後の行が「未設定」で exit 0 / 知らない引数 = exit 1', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'neg-'));
  const env = { ...process.env, DATA_DIR: dir, DAILY_SYNC_RUN_ID: RUN, DOTENV_CONFIG_PATH: path.join(dir, 'none.env') };
  delete env.COMPANY_DB_NEW_ENTRY_GATE_URL;
  const out = execFileSync(process.execPath, [path.join(ROOT, 'apps/company-db/master-compare/new-entry-gate.mjs'), '--daily'], { cwd: dir, env, encoding: 'utf8', timeout: 30000 });
  assert.equal(out.trim().split('\n').pop(), '🆕 新商品の入口: 未設定 (COMPANY_DB_NEW_ENTRY_GATE_URL が無い = この段は飛ばした)');
  let st = null, bogus = '';
  try { execFileSync(process.execPath, [path.join(ROOT, 'apps/company-db/master-compare/new-entry-gate.mjs'), '--bogus'], { cwd: dir, env, encoding: 'utf8', timeout: 30000, stdio: 'pipe' }); } catch (e) { st = e.status; bogus = String(e.stdout).trim().split('\n').pop(); }
  assert.equal(st, 1);
  // URL が無い = 確かめられない = 「閉」と言わない
  assert.equal(bogus, '🆕 新商品の入口: この段が落ちた: 知らない引数: --bogus (COMPANY_DB_NEW_ENTRY_GATE_URL が無い = 許可は出していない)');
  fs.rmSync(dir, { recursive: true, force: true });
});

await ta('[8] daily-sync の配線: 照合の直後 (影の前)・skipAfterCompare で守る・retry の対象・失敗したら「マスタ照合」も載せる', async () => {
  const ds = fs.readFileSync(path.join(ROOT, 'apps/warehouse/daily-sync.js'), 'utf8').replace(/\r\n/g, '\n');
  const iCompare = ds.indexOf("runScript('apps/company-db/master-compare/run.mjs --daily', 'マスタ照合'");
  const iGuard = ds.indexOf('const newEntryGateSkip = skipAfterCompare(masterCompareResult);');
  const iGate = ds.indexOf("runScript('apps/company-db/master-compare/new-entry-gate.mjs --daily', NEW_ENTRY_GATE_STEP");
  const iLz = ds.indexOf("runScript('scripts/company-db/lz-daily.mjs --daily'");
  assert.ok(iCompare > 0 && iCompare < iGuard && iGuard < iGate && iGate < iLz, [iCompare, iGuard, iGate, iLz].join(','));
  assert.match(ds.slice(iGuard, iGate + 300), /const newEntryGateSkip = skipAfterCompare\(masterCompareResult\);\n\s*if \(newEntryGateSkip\) results\.push\(newEntryGateSkip\);\n\s*else \{\n\s*const newEntryGateResult = runScript\(/);
  assert.match(ds.slice(iGate, iGate + 300), /results\.push\(\{ name: NEW_ENTRY_GATE_STEP, \.\.\.newEntryGateResult, warn: newEntryGateResult\.success && isWarnSummary\(newEntryGateResult\.summary\) \}\);/);
  assert.equal([...ds.matchAll(/new-entry-gate\.mjs --daily/g)].length, 1);
  assert.match(ds, /import \{ skipAfterCompare, gateRetryJobs, STEP_NAME as NEW_ENTRY_GATE_STEP \} from '\.\.\/company-db\/master-compare\/new-entry-gate\.mjs';/);
  const rj = ds.match(/const RETRYABLE_JOBS = \[([^\]]+)\]/)[1];
  assert.ok(rj.includes("'新商品の許可'"));
  assert.equal(S.STEP_NAME, '新商品の許可');
  // retry-state に載せる名前は gateRetryJobs を通す (許可が失敗した朝は照合も載る)
  assert.match(ds, /const retryableFailed = gateRetryJobs\(results\n[^\n]*\n\s*\.filter\(r => RETRYABLE_JOBS\.includes\(r\.name\) && !r\.success && !r\.blocked\)\n\s*\.map\(r => r\.name\)\);/);
  assert.deepEqual(S.gateRetryJobs(['新商品の許可']), ['新商品の許可', 'マスタ照合']);
  assert.deepEqual(S.gateRetryJobs(['f_sales', '新商品の許可', 'マスタ照合']), ['f_sales', '新商品の許可', 'マスタ照合']);
  assert.deepEqual(S.gateRetryJobs(['f_sales']), ['f_sales']);
  assert.deepEqual(S.gateRetryJobs(undefined), []);
});

await ta('[9] retry: 定義・順 (照合より後)・照合が直ったら流す・照合をこの回で再試行して失敗したら見送る・許可だけが残っても照合からやり直す', async () => {
  const R = await import('../apps/warehouse/retry-failed-jobs.js');
  assert.deepEqual(R.JOB_DEFINITIONS['新商品の許可'], { script: 'apps/company-db/master-compare/new-entry-gate.mjs', args: ['--daily'], timeoutMs: 120000 });
  assert.ok(fs.existsSync(path.join(ROOT, R.JOB_DEFINITIONS['新商品の許可'].script)));
  const o = R.RETRY_ORDER;
  assert.ok(o.indexOf('マスタ照合') >= 0 && o.indexOf('マスタ照合') < o.indexOf('新商品の許可'));
  assert.ok(R.RERUN_AFTER['マスタ照合'].includes('新商品の許可'));
  assert.equal(R.UPSTREAM_OF['新商品の許可'], 'マスタ照合');
  assert.deepEqual(R.rerunAfterProblems(), []);
  const quiet = () => {};
  const fakeRun = (fails = {}) => { const calls = []; return { calls, run: (script, name) => { calls.push(name); return fails[name] ? { success: false, summary: '❌ 失敗' } : { success: true, summary: '✅' }; } }; };
  // 照合が retry で直った → 新商品の許可も流す (照合の直後)
  const a = fakeRun();
  R.runRetryRound(['マスタ照合'], { run: a.run, log: quiet });
  assert.deepEqual(a.calls.slice(0, 2), ['マスタ照合', '新商品の許可']);
  // Render同期 が直った → 照合 → 新商品の許可 (連鎖)
  const b = fakeRun();
  R.runRetryRound(['Render同期'], { run: b.run, log: quiet });
  assert.deepEqual(b.calls.slice(0, 3), ['Render同期', 'マスタ照合', '新商品の許可']);
  // 照合が再失敗 = 流さない
  const c = fakeRun({ 'マスタ照合': true });
  R.runRetryRound(['マスタ照合'], { run: c.run, log: quiet });
  assert.deepEqual(c.calls, ['マスタ照合']);
  // 新商品の許可も残っていて照合が再失敗 = 見送り (⏸️)
  const d = fakeRun({ 'マスタ照合': true });
  const rd = R.runRetryRound(['マスタ照合', '新商品の許可'], { run: d.run, log: quiet });
  assert.deepEqual(d.calls, ['マスタ照合']);
  assert.deepEqual(rd.map((r) => [r.name, r.success]), [['マスタ照合', false], ['新商品の許可', false]]);
  assert.match(rd[1].summary, /^⏸️ skipped \(マスタ照合 再失敗\)/);
  // (#1645 R1 Medium) 許可だけが失敗した日の retry = 照合 → 許可の順 (新しい照合の回で close → record → grant)
  const e = fakeRun();
  const re = R.runRetryRound(['新商品の許可'], { run: e.run, log: quiet });
  assert.deepEqual(e.calls.slice(0, 2), ['マスタ照合', '新商品の許可']);
  assert.equal(e.calls.filter((x) => x === '新商品の許可').length, 1);   // 1 回だけ
  assert.deepEqual(re.slice(0, 2).map((r) => [r.name, r.success]), [['マスタ照合', true], ['新商品の許可', true]]);
  // 許可がまた失敗した = 次の回の remaining にも照合が入る (gateRetryJobs) = また照合からやり直す
  const f = fakeRun({ '新商品の許可': true });
  const rf = R.runRetryRound(['新商品の許可'], { run: f.run, log: quiet });
  const next = rf.filter((r) => !r.success).map((r) => r.name);
  assert.deepEqual(next, ['新商品の許可']);
  const g = fakeRun();
  R.runRetryRound(next, { run: g.run, log: quiet });
  assert.deepEqual(g.calls.slice(0, 2), ['マスタ照合', '新商品の許可']);
});

await ta('[10] 台帳: warehouse-daily-sync の purpose / runbook に載る (新しいエントリは作らない)・手の順は照合 → 許可・写しの門は止めない側', async () => {
  const { JOBS_REGISTRY } = await import('../config/jobs-registry.mjs');
  const e = JOBS_REGISTRY.find((j) => j.id === 'warehouse-daily-sync');
  assert.match(e.purpose, /「新商品の許可」\(apps\/company-db\/master-compare\/new-entry-gate\.mjs --daily。新しい定期実行ではない/);
  assert.match(e.purpose, /COMPANY_DB_NEW_ENTRY_GATE_URL/);
  assert.match(e.purpose, /失敗した朝の retry は「マスタ照合」も載せて/);
  assert.match(e.runbook, /「新商品の許可」が閉/);
  assert.match(e.runbook, /手で流す = 必ず「マスタ照合」\(node -r dotenv\/config apps\/company-db\/master-compare\/run\.mjs --daily\) → 「新商品の許可」\(node -r dotenv\/config apps\/company-db\/master-compare\/new-entry-gate\.mjs --daily\) の順/);
  assert.match(e.runbook, /状態不明 = 開いている可能性/);
  assert.deepEqual(JOBS_REGISTRY.filter((j) => j.id !== 'warehouse-daily-sync' && /new-entry-gate|新商品の許可/.test(`${j.id} ${j.where || ''}`)).map((j) => j.id), []);
  const G = await import('../apps/warehouse/publish-gate.js');
  assert.ok(Object.hasOwn(G.PUBLISH_UNGATED_SCRIPTS, 'apps/company-db/master-compare/new-entry-gate.mjs'));
  assert.equal(G.publishGateDecision('apps/company-db/master-compare/new-entry-gate.mjs --daily', { broken: true }).skip, false);
});

await ta('[11] (#1645 R1 High) grant の後に成功を確かめられない = 新しい接続で revoke → 確かめて閉 / 確かめられない = 状態不明', async () => {
  // ① grant は DB で通った (開いた) が応答が切れた = 新しい接続で閉じて確かめる = 閉・exit 1
  const a = fakeDb({ grantDropped: true });
  const ra = await run(a);
  assert.deepEqual([ra.code, ra.state], [1, 'error']);
  assert.match(ra.line, /^🆕 新商品の入口: 閉 \(許可を出せない: Connection terminated unexpectedly\)$/);
  assertFreshCloseVerify(a);
  assert.equal(a.isOpen(), false);
  assert.equal(a.closed(), 2);
  // ② 返り値が壊れている (null・期限が無い・期限が読めない・形が違う) = 新しい接続で閉じて確かめる = 閉・exit 1
  for (const bad of [null, { lease_id: '1' }, { lease_id: '1', expires_at: 'あした' }, 'ok']) {
    const b = fakeDb({ grant: bad });
    const rb = await run(b);
    assert.deepEqual([rb.code, rb.state], [1, 'error'], JSON.stringify(bad));
    assert.match(rb.line, /^🆕 新商品の入口: 閉 \(許可の返り値が読めない: /);
    assertFreshCloseVerify(b);
    assert.equal(b.isOpen(), false);
  }
  // ③ 応答が切れた後、新しい接続も落ちる = 状態不明 (開いている可能性)・exit 1
  const c = fakeDb({ grantDropped: true, connectFailFrom: 2 });
  const rc = await run(c);
  assert.deepEqual([rc.code, rc.state], [1, 'unknown']);
  assert.match(rc.line, /^⚠️ 🆕 新商品の入口: 状態不明 \(開いている可能性・許可を出せない: Connection terminated unexpectedly \/ 取り消しの接続もできない \(ECONNREFUSED \(接続 2\)\)\)$/);
  assert.equal(c.isOpen(), true);   // 本当に開いたまま = 「閉」と言ってはいけない
  assert.equal(revokes(c).length, 0);
  // 返り値が壊れていて新しい接続も落ちる = 状態不明
  const d = await run(fakeDb({ grant: { lease_id: '1' }, connectFailFrom: 2 }));
  assert.deepEqual([d.code, d.state], [1, 'unknown']);
  // 「閉」と出る行は全部、閉じたのを確かめた回 (状態不明の行に「閉」の語が出ない)
  assert.doesNotMatch(rc.line, /入口: 閉/);
  assert.equal(typeof S.closeAndVerify, 'function');
});

await ta('[12] (#1645 R2 High) DATA_DIR が無い・引数の間違い・最上位の例外 = 開いた許可を新しい接続で閉じて確かめる / 確かめられない = 状態不明', async () => {
  const cli = async (f, argv, { env = { ...ENV, DATA_DIR: 'D:/fake' }, now = NOW } = {}) =>
    assertClosedOnlyIfVerified(await S.runCli(argv, { env, connect: f.connect, now, readEv: () => ({ 'master-compare': evOk() }) }), f);
  // DATA_DIR が無い (朝に開いた許可が残っている)
  const a = fakeDb({ openAtStart: true });
  const ra = await S.runGateStep({ env: ENV, dataDir: '', now: NOW, connect: a.connect });
  assertClosedOnlyIfVerified(ra, a);
  assert.deepEqual([ra.code, ra.state, ra.line], [1, 'error', '🆕 新商品の入口: 閉 (DATA_DIR が無い = 照合の証跡を読めない)']);
  assert.equal(a.isOpen(), false);   // DB の許可も閉じた
  assert.deepEqual([revokes(a).length, valids(a).length, grants(a).length], [1, 1, 0]);
  assert.match(revokes(a)[0].params[1], /DATA_DIR が無い/);
  const ra2 = await cli(fakeDb({ openAtStart: true }), ['--daily'], { env: { ...ENV, DATA_DIR: '' } });
  assert.equal(ra2.line, '🆕 新商品の入口: 閉 (DATA_DIR が無い = 照合の証跡を読めない)');
  // DATA_DIR が無く、閉じる接続も落ちる = 状態不明 (DB は開いたまま)
  const b = fakeDb({ openAtStart: true, connectFailFrom: 1 });
  const rb = assertClosedOnlyIfVerified(await S.runGateStep({ env: ENV, dataDir: '', now: NOW, connect: b.connect }), b);
  assert.deepEqual([rb.code, rb.state], [1, 'unknown']);
  assert.match(rb.line, /^⚠️ 🆕 新商品の入口: 状態不明 \(開いている可能性・DATA_DIR が無い = 照合の証跡を読めない \/ 取り消しの接続もできない/);
  assert.equal(b.isOpen(), true);
  // 引数の間違い = 閉じて確かめる
  const c = fakeDb({ openAtStart: true });
  const rc = await cli(c, ['--bogus']);
  assert.deepEqual([rc.code, rc.state, rc.line], [1, 'error', '🆕 新商品の入口: 閉 (この段が落ちた: 知らない引数: --bogus)']);
  assert.equal(c.isOpen(), false);
  assert.deepEqual([revokes(c).length, valids(c).length, grants(c).length], [1, 1, 0]);
  // 引数の間違い + 確かめられない (取り消した後も有効と読めた) = 状態不明
  const d = fakeDb({ openAtStart: true, validAfterRevoke: true });
  const rd = await cli(d, ['--bogus']);
  assert.deepEqual([rd.code, rd.state], [1, 'unknown']);
  assert.match(rd.line, /^⚠️ 🆕 新商品の入口: 状態不明 \(開いている可能性・この段が落ちた: 知らない引数: --bogus \/ 取り消した後も有効と読めた \(true\)\)$/);
  // 最上位の例外 (段の中で思いがけず投げた = 時計が壊れた) = 閉じて確かめる / 確かめられない = 状態不明
  const e = fakeDb({ openAtStart: true });
  const re = await cli(e, ['--daily'], { now: { getTime() { throw new Error('時計が壊れた'); } } });
  assert.equal(re.code, 1);
  assert.match(re.line, /^🆕 新商品の入口: 閉 \(この段が落ちた: /);
  assert.equal(e.isOpen(), false);
  assert.equal(grants(e).length, 0);
  const g = fakeDb({ openAtStart: true, revokeError: 'permission denied' });
  const rg = await cli(g, ['--daily'], { now: { getTime() { throw new Error('時計が壊れた'); } } });
  assert.deepEqual([rg.code, rg.state], [1, 'unknown']);
  assert.match(rg.line, /^⚠️ 🆕 新商品の入口: 状態不明 \(開いている可能性・この段が落ちた: .* \/ 取り消しも失敗 \(permission denied\)\)$/);
  assert.equal(g.isOpen(), true);
  // ふつうの CLI の回 (引数が正しい) は runGateStep と同じ = 開く
  const h = fakeDb();
  assert.equal((await cli(h, ['--daily'])).state, 'opened');
  // 本体の「閉」の語の出口は、閉じたのを確かめた後 (closeOut・runCli) だけ
  const src = fs.readFileSync(path.join(ROOT, 'apps/company-db/master-compare/new-entry-gate.mjs'), 'utf8').replace(/\r\n/g, '\n');
  const outs = src.split('\n').filter((l) => !/^\s*(\*|\/\/|\/\*\*)/.test(l) && /\$\{HEAD\} 閉(?!じ)/.test(l));
  assert.equal(outs.length, 3, outs.join('\n'));
  assert.ok(outs.every((l) => /line: line \|\| `\$\{HEAD\} 閉 \(\$\{why\}\)`|closeOut\(`widen の前|return v\.ok \? \{ code: 1, state: 'error', line: `\$\{HEAD\} 閉/.test(l)), outs.join('\n'));
});

console.log(`\n${passed} 件 ok${process.exitCode ? ' (NG あり)' : ''}`);
