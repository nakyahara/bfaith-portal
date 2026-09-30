/**
 * lz-cutover-check.mjs — ロジザードの毎日の商品マスタの取込の切替・戻しの手順の確かめ (マスタ正本切替・2b-2 契約 v3 の切替の PR)
 *
 *   node scripts/logizard-import/lz-cutover-check.mjs --expect before|cutover|ready|rollback [--data-dir <DATA_DIR>]
 *
 * 手順の各段階で、ポータル (Render) と miniPC の設定が思ったとおりかを見る。切替の PR (#1558) より先にマージして、
 * 切替の前 (練習・手順 2 と 5) から miniPC のリポジトリで使えるようにしてある (Codex #1558 R1 High)。
 * 書かない (ping・知らせ・記録なし)。副作用は cutover / ready の「旧い手の ③ (manual_daily) の鍵の要求 1 回」だけ =
 * 旗 (LZ_MANUAL_V4) が立っているのを読み戻した後に送り、ポータルが DB に触る前に retired で断るはずのもの。
 * 万一取れてしまった (= 旗が効いていない) ときは、すぐ返して ❌。
 *
 * どの段階も: Render の時計で夜の窓の外 (JST 01:30〜23:30。N8)・止めてある・鍵が無い・手の取込が開いていない・未解決の取込が無い
 *   before   = 切替の手順 2 (止めた後・旗の前): 旗は off・miniPC の毎晩の本番は off
 *   cutover  = 手順 5 (旗 on と cutover_phase の後): 旗 on (status と readiness の両方)・cutover_phase を人が cutover に設定した・
 *              GAS の CSV の手の取込を断る (本当の道と同じ判定 = gas_closed)・成果物の無い毎晩を断る (artifact_missing)・
 *              旧い手の ③ (manual_daily) を断る (retired)・DATA_DIR がこの miniPC のもの (初期化の印が同じ)・次の夜の済みの印が無い
 *   ready    = 手順 8 の後 (miniPC の LZ_DAILY_IMPORT=on・止めの解除の前): cutover の全部 + LZ_DAILY_IMPORT=on・送り先 GCHAT_WEBHOOK_JOBS が本番と同じ判定で使える
 *   rollback = GAS への戻し (K3-8・N2) の後: 旗は off・miniPC の毎晩の本番は off・戻しの固定の版 (台帳 lz-gas-rollback) の期限の内 (Render の日付)
 *
 * 設定 = リポジトリ直下の .env (LZ_LOCK_TOKEN・LZ_IMPORT_STATE_URL・DATA_DIR・LZ_DAILY_IMPORT・GCHAT_WEBHOOK_JOBS)。値は出さない (on / off と有無だけ)。
 * 終了コード: 0 = 全部そのとおり / 1 = どれかが違う・届かない
 * 手順 = db/company/README.md「毎晩の本番の切替と GAS への戻し」
 */
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
import dotenv from 'dotenv';
import { nightlyMarker } from './lz-nightly.mjs';
import { jobsHook } from './notify-jobs.mjs';
import { JOBS_REGISTRY } from '../../config/jobs-registry.mjs';

const REPO_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..');
export const EXPECTS = Object.freeze(['before', 'cutover', 'ready', 'rollback']);
/** 切替・戻しをしてよい時刻 (Render の時計・JST の分)。00:15〜00:55 の毎晩の取込・影と、Stream Deck の夜の止め 00:00〜01:30 を避ける (N8) */
export const OPS_WINDOW = Object.freeze({ fromMin: 90, toMin: 23 * 60 + 30 });
const BY = 'lz-cutover-check';
const DAY_MS = 24 * 3600 * 1000;
const JST_MS = 9 * 3600 * 1000;
const isOn = (v) => String(v ?? '').trim().toLowerCase() === 'on';
const nextJstDate = (ymd) => new Date(Date.parse(`${ymd}T00:00:00Z`) + DAY_MS).toISOString().slice(0, 10);
const hhmm = (m) => `${String(Math.floor(m / 60)).padStart(2, '0')}:${String(m % 60).padStart(2, '0')}`;

/**
 * 確かめる。client = import-state-client の口 (status・nightlyReadiness・acquire・release)。
 * @returns {Promise<{ ok: boolean, expect: string, checks: { name: string, ok: boolean, detail: string }[] }>}
 */
export async function cutoverCheck({ expect, client, env = process.env, dataDir = null, registry = JOBS_REGISTRY, log = () => {} }) {
  if (!EXPECTS.includes(expect)) throw new Error(`--expect は ${EXPECTS.join(' / ')}`);
  const checks = [];
  const check = (name, ok, detail = '') => { checks.push({ name, ok: !!ok, detail: String(detail) }); log(`${ok ? '✅' : '❌'} ${name}${detail ? ` (${detail})` : ''}`); return !!ok; };
  const done = () => ({ ok: checks.every((c) => c.ok), expect, checks });

  let st;
  try { st = await client.status(5); } catch (e) { check('ポータルの状態を読む', false, `${e.code || 'error'}: ${e.message}`); return done(); }
  if (!check('ポータルが初期化されている', st.initialized === true)) return done();
  const clk = st.clock || {};
  // 夜の窓の外か (Render の時計。N8)。窓の中の readiness は outside_window と一緒に artifact_missing も返す = 時刻で先に止める (Codex #1558 R1 Medium)
  const serverNow = Number(clk.server_now);
  if (!Number.isFinite(serverNow)) check(`切替・戻しをしてよい時刻 (Render の時計で JST ${hhmm(OPS_WINDOW.fromMin)}〜${hhmm(OPS_WINDOW.toMin)})`, false, 'status.clock.server_now が無い');
  else {
    const d = new Date(serverNow + JST_MS);
    const m = d.getUTCHours() * 60 + d.getUTCMinutes();
    check(`切替・戻しをしてよい時刻 (Render の時計で JST ${hhmm(OPS_WINDOW.fromMin)}〜${hhmm(OPS_WINDOW.toMin)})`, m >= OPS_WINDOW.fromMin && m < OPS_WINDOW.toMin, `今 ${hhmm(m)}`);
  }
  const v4 = !!(st.manual && st.manual.v4 === true);
  // どの段階も: 止めてある・鍵が無い・手の取込が開いていない・未解決の取込が無い (切替・戻しは止めている間だけ = 手順 1・2)
  check('自動の取込を止めてある (halt)', st.halted === true, st.halted ? `止めの番号 ${st.halt_revision}` : '止めていない');
  check('生きた鍵が無い', !st.lock, st.lock ? `${st.lock.holder}・${st.lock.purpose}・${st.lock.run_id}` : '');
  check('手の取込が開いていない', !(st.manual && st.manual.open), '');
  check('未解決の取込が無い (状態 = idle / verified)', ['idle', 'verified'].includes(st.state), `状態 = ${st.state}`);

  const wantV4 = expect === 'cutover' || expect === 'ready';
  check(`ポータルの旗 LZ_MANUAL_V4 = ${wantV4 ? 'on' : 'off'} (読み戻し)`, v4 === wantV4, `manual.v4 = ${v4}`);
  const nightlyOn = isOn(env.LZ_DAILY_IMPORT);
  check(`miniPC の毎晩の本番 LZ_DAILY_IMPORT = ${expect === 'ready' ? 'on' : 'off'}`, nightlyOn === (expect === 'ready'), nightlyOn ? 'on' : 'off');

  if (expect === 'cutover' || expect === 'ready') {
    // 成果物の無い毎晩 (形は正しい・無い識別) = 断る。cutover_phase と GAS の CSV の判定も同じ答えで読み戻す (N4 = 本当の道と同じ照らし)
    let rd = null;
    try {
      rd = await client.nightlyReadiness({ source_run_id: 'lzd_cutover_check_absent', csv_sha256: '0'.repeat(64), rows: 1, target_as_of: clk.expected_target_as_of });
    } catch (e) { check('毎晩の本番を始められるかを聞く (nightly-readiness)', false, `${e.code || 'error'}: ${e.message}`); }
    if (rd) {
      check('cutover_phase = cutover を人が設定した (手順 4)', rd.cutover_phase === 'cutover' && rd.cutover_phase_explicit === true, `cutover_phase = ${rd.cutover_phase}・設定 ${rd.cutover_phase_explicit === true ? 'あり' : rd.cutover_phase_explicit === false ? '無し (既定で読んだだけ)' : '不明 (古い Render の版)'}`);
      // 本当の道 (openManualSession の gas_upload) と同じ判定の答え。古い Render (この答えが無い) = ❌
      check('GAS の CSV の手の取込を断る (gas_closed)', rd.gas_upload === 'gas_closed', `gas_upload = ${rd.gas_upload ?? '答えが無い (古い Render の版)'}`);
      check('成果物の無い毎晩を断る (artifact_missing)', rd.ready === false && Array.isArray(rd.codes) && rd.codes.includes('artifact_missing'), `ready = ${rd.ready}・codes = ${(rd.codes || []).join(',') || '-'}`);
      check('readiness の旗も on', !!(rd.manual && rd.manual.v4 === true), `manual.v4 = ${rd.manual && rd.manual.v4}`);
    }
    // 旧い手の ③ (manual_daily) を断る (retired)。旗が on と読み戻せたときだけ送る (off で送ると止めている間は鍵が取れてしまう)
    if (v4) {
      const runId = `lzcut_${Date.now()}_${crypto.randomBytes(3).toString('hex')}`;
      try {
        const got = await client.acquire({ init_id: st.init_id, holder: 'manual_daily', purpose: 'import', run_id: runId, ttl_sec: 30, by: BY });
        let back = '返した';
        try { await client.release({ lock_token: got.lock_token, run_id: runId, by: BY }); } catch (e) { back = `返せない (${e.code || 'error'}: ${e.message})・期限 30 秒で切れる`; }
        check('旧い手の ③ (manual_daily) を断る (retired)', false, `鍵が取れてしまった = 旗が効いていない・${back}`);
      } catch (e) {
        check('旧い手の ③ (manual_daily) を断る (retired)', e && e.code === 'retired', `${(e && e.code) || 'error'}`);
      }
    } else check('旧い手の ③ (manual_daily) を断る (retired)', false, '旗が off と読めた = 送らない');
    // DATA_DIR がこの miniPC の本物か (初期化の印がポータルと同じ)。違う場所だと「済みの印が無い」が偽の ok になる (Codex #1558 R1 Medium)
    let dirOk = false;
    if (!dataDir) check('DATA_DIR がこの miniPC のもの (初期化の印がポータルと同じ)', false, 'DATA_DIR が無い');
    else {
      const initFile = path.join(dataDir, 'lz-import', 'init.json');
      let local = null, why = '';
      try { local = JSON.parse(fs.readFileSync(initFile, 'utf8')); } catch (e) { why = e.code === 'ENOENT' ? `初期化の印が無い (${initFile})` : `初期化の印が読めない (${String(e.message).slice(0, 80)})`; }
      dirOk = !!(local && local.init_id === st.init_id);
      check('DATA_DIR がこの miniPC のもの (初期化の印がポータルと同じ)', dirOk, why || (dirOk ? '' : `印 ${local && local.init_id}・ポータル ${st.init_id}`));
    }
    // 次の夜の済みの印が無い (間違って作られていれば、その夜の毎晩の本番が「済み」で何もしない。N8)
    if (dirOk && clk.jst_date) {
      const next = nextJstDate(clk.jst_date);
      const f = nightlyMarker(dataDir, next);
      check(`次の夜 (${next}) の済みの印が無い`, !fs.existsSync(f), fs.existsSync(f) ? f : '');
      const today = nightlyMarker(dataDir, clk.jst_date);
      if (fs.existsSync(today)) log(`ℹ 今日 (${clk.jst_date}) の済みの印はある (${today})・次の夜には効かない`);
    } else check('次の夜の済みの印が無い', false, dirOk ? 'status.clock.jst_date が無い' : 'DATA_DIR が確かめられない = 見られない');
  }
  if (expect === 'ready') {
    // 本番 (lz-nightly.mjs の nightlyMain) と同じ判定。値は出さない
    check('要対応スペースの送り先 GCHAT_WEBHOOK_JOBS が使える (本番と同じ判定)', !!jobsHook(env), '値は出さない');
    if (isOn(env.LZ_DAILY_IMPORT_SHADOW)) log('ℹ LZ_DAILY_IMPORT_SHADOW=on が残っている (本番が on の夜は影はしない・消してよい)');
  }
  if (expect === 'rollback') {
    // 戻しの固定の版の期限 (台帳 lz-gas-rollback の remove_by・Render の日付)。過ぎた = 使わない (Codex #1558 R1 Medium)
    const rb = registry.find((e) => e.id === 'lz-gas-rollback');
    if (!rb || !rb.rollback) check('戻しの固定の版 (台帳 lz-gas-rollback) がある', false, '台帳に無い = 期限の後に消した = 固定の版では戻さない');
    else if (!clk.jst_date) check(`戻しの固定の版の期限の内 (${rb.remove_by} まで)`, false, 'status.clock.jst_date が無い');
    else check(`戻しの固定の版の期限の内 (${rb.remove_by} まで・tag ${rb.rollback.tag})`, clk.jst_date <= rb.remove_by, `今日 ${clk.jst_date}`);
  }
  return done();
}

function parseArgs(argv) {
  const out = { expect: null, dataDir: null };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--expect') out.expect = argv[++i];
    else if (a === '--data-dir') out.dataDir = argv[++i];
    else throw new Error(`知らない引数: ${a}`);
  }
  if (!EXPECTS.includes(out.expect)) throw new Error(`--expect ${EXPECTS.join('|')} が要る`);
  return out;
}

const fold = (x) => (process.platform === 'win32' ? x.toLowerCase() : x);
const isMain = (() => { try { return !!process.argv[1] && fold(fs.realpathSync.native(process.argv[1])) === fold(fs.realpathSync.native(fileURLToPath(import.meta.url))); } catch { return false; } })();
if (isMain) {
  let code = 1;
  try {
    dotenv.config({ path: path.join(REPO_ROOT, '.env') });   // リポジトリ直下の .env (1 つだけ)
    const a = parseArgs(process.argv.slice(2));
    const { createImportStateClient } = await import('../../tools/logizard-automation/import-state-client.js');
    const client = createImportStateClient({ url: process.env.LZ_IMPORT_STATE_URL || undefined, token: process.env.LZ_LOCK_TOKEN });
    const r = await cutoverCheck({ expect: a.expect, client, env: process.env, dataDir: (a.dataDir || process.env.DATA_DIR || '').trim() || null, log: (s) => console.log(s) });
    console.log(r.ok ? `🎉 ${a.expect}: 全部そのとおり` : `❌ ${a.expect}: 違うものがある (上の ❌ を直してから次の手順へ)`);
    code = r.ok ? 0 : 1;
  } catch (e) {
    console.log(`❌ ${String(e && e.message).replace(/\s+/g, ' ').slice(0, 300)}`);
  }
  process.exitCode = code;
  setTimeout(() => process.exit(code), 5000).unref();   // fetch の後の process.exit をすぐ呼ばない (Windows の 127)
}
