#!/usr/bin/env node
/**
 * remote-load.mjs — miniPC (や手元) から Render の Company DB 同期 API を叩く。
 *
 *   node scripts/company-db/remote-load.mjs status [--counts]         GET /status (既定は counts=0 = Postgres に繋がない)
 *   node scripts/company-db/remote-load.mjs load [--apply] [--wait]   POST /load (既定 dry-run)。--wait で終わるまで 10 秒おきに見る
 *   node scripts/company-db/remote-load.mjs wait [run_id]             その run (省略時は直近) が終わるまで待つ (最大 30 分)。失敗・中断・結果不明は終了コード 1
 *   node scripts/company-db/remote-load.mjs reports                   GET /reports (report の一覧)
 *   node scripts/company-db/remote-load.mjs report <run_id> [--md] [--out <file>]   GET /report/:run_id (明細)。--out でファイルに保存
 *
 * 🚨 RENDER_MIRROR_URL は末尾にパスが付いている (実測 `https://<host>/apps/mirror`) ので origin だけ使う (apps/expected-profit/publish.js と同じ罠)。
 *    RENDER_PORTAL_URL があればそちら (同じホストに限る)。認証はヘッダ x-sync-key = MIRROR_SYNC_KEY。値は表示しない
 */
import 'dotenv/config';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

/**
 * 叩く先の origin。RENDER_PORTAL_URL があれば RENDER_MIRROR_URL と同じホストのときだけ使い、別ホストなら '' (止める)。
 * apps/expected-profit/publish.js の syncBaseUrl と同じ規則 (別ホストへ鍵を送らない)。https 以外は ''
 */
export function baseOrigin(env = process.env) {
  const mirror = String(env.RENDER_MIRROR_URL || '').trim();
  const portalRaw = env.RENDER_PORTAL_URL;
  let m; try { m = mirror ? new URL(mirror) : null; } catch { m = null; }
  if (portalRaw != null && portalRaw !== '') {   // 指定がある (空白だけも「指定」) のに読めない / https でない / 別ホスト → 止める (mirror に落ちない。syncBaseUrl と同じ)
    let p; try { p = new URL(String(portalRaw).trim()); } catch { return ''; }
    if (p.protocol !== 'https:' || (m && p.host !== m.host)) return '';
    return p.origin;
  }
  if (m && m.protocol === 'https:') return m.origin;
  return '';
}

/**
 * /status の応答から「その run が終わって成功したか」を判定する (純関数)。
 *   - current がその run → { done: false }
 *   - interrupted にその run が残っている、または interrupted_error (running.json が壊れて確認できない) → 結果不明 = 失敗扱い
 *   - last (プロセス内の記録) がある → その run なら status が done で成功、別の run なら「記録が無い」= 失敗 (latest には落ちない)
 *   - last が無い (再起動した) → latest.json がその run なら ok で判定
 *   - どれにも無い → 結果不明 = 失敗扱い
 * runId を渡さないときは「直近の run」: current が無く、interrupted が無く、last があれば last (done で成功)、無ければ latest.ok
 */
export function judgeRun(body, runId) {
  const cur = body?.current;
  if (cur && (!runId || cur.run_id === runId)) return { done: false, ok: false, reason: `running ${cur.run_id}` };
  const inter = body?.interrupted;
  if (inter && (!runId || inter.run_id === runId)) return { done: true, ok: false, reason: `interrupted ${inter.run_id} (結果不明。committed=${inter.committed})` };
  if (body?.interrupted_error) return { done: true, ok: false, reason: `interrupted を確認できない (${body.interrupted_error})` };   // running.json が壊れている = 中断の有無が分からない → 結果不明
  const last = body?.last;
  if (last) {
    if (runId && last.run_id !== runId) return { done: true, ok: false, reason: `run ${runId} の記録が無い (last は ${last.run_id})` };
    return { done: true, ok: last.status === 'done', reason: `last(${last.run_id}).status=${last.status}${last.error ? ' ' + last.error : ''}` };
  }
  const latest = body?.latest;
  if (latest && (!runId || latest.run_id === runId)) return { done: true, ok: latest.ok === true, reason: `latest(${latest.run_id}).ok=${latest.ok}${latest.error ? ' ' + latest.error : ''}` };
  return { done: true, ok: false, reason: runId ? `run ${runId} の記録が無い` : '記録が無い' };
}

const isMain = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) {
  const args = process.argv.slice(2);
  const cmd = args[0] || 'status';
  const flag = (f) => args.includes(f);
  const argAfter = (f) => { const i = args.indexOf(f); return i >= 0 && i < args.length - 1 ? args[i + 1] : null; };
  const origin = baseOrigin();
  const key = process.env.MIRROR_SYNC_KEY || '';
  if (!origin) { console.error('RENDER_MIRROR_URL (https。RENDER_PORTAL_URL を使うなら同じホスト) が要る'); process.exit(2); }
  if (!key) { console.error('MIRROR_SYNC_KEY が要る'); process.exit(2); }

  // リダイレクトは追わない (別 origin へ鍵を転送しない)。失敗は例外 → 終了コード 1
  const call = async (p, opt = {}) => {
    let res;
    try { res = await fetch(`${origin}${p}`, { ...opt, redirect: 'error', headers: { 'x-sync-key': key, ...(opt.headers || {}) } }); }
    catch (e) { console.error(`request failed: ${e.message}`); process.exit(1); }
    const text = await res.text();
    let body; try { body = JSON.parse(text); } catch { body = text; }
    return { status: res.status, body };
  };
  const show = (r) => console.log(typeof r.body === 'string' ? r.body : JSON.stringify(r, null, 2));
  const expect = (r, want = 200) => { show(r); return r.status === want ? 0 : 1; };
  const waitDone = async (runId) => {
    const t0 = Date.now();
    for (;;) {
      const r = await call('/apps/company-db/sync/status?counts=0');
      if (r.status !== 200) { show(r); return 1; }
      const j = judgeRun(r.body, runId);
      if (j.done) { show({ status: r.status, body: { last: r.body.last, latest: r.body.latest, interrupted: r.body.interrupted } }); console.log(`${j.ok ? 'OK' : 'NG'}: ${j.reason}`); return j.ok ? 0 : 1; }
      console.log(`${j.reason} (${Math.round((Date.now() - t0) / 1000)}s)`);
      if (Date.now() - t0 > 30 * 60 * 1000) { console.error('30 分たっても終わらない'); return 1; }
      await new Promise((r2) => setTimeout(r2, 10000));
    }
  };

  let code = 0;
  if (cmd === 'status') code = expect(await call(`/apps/company-db/sync/status?counts=${flag('--counts') ? 1 : 0}`));
  else if (cmd === 'load') { const r = await call(`/apps/company-db/sync/load${flag('--apply') ? '?apply=1' : ''}`, { method: 'POST' }); show(r); if (r.status !== 202) code = 1; else if (flag('--wait')) code = await waitDone(r.body.run_id); }
  else if (cmd === 'wait') code = await waitDone(args[1] || null);
  else if (cmd === 'reports') code = expect(await call('/apps/company-db/sync/reports'));
  else if (cmd === 'report') {
    const runId = args[1]; if (!runId) { console.error('run_id が要る'); process.exit(2); }
    const r = await call(`/apps/company-db/sync/report/${encodeURIComponent(runId)}${flag('--md') ? '?format=md' : ''}`);
    if (r.status !== 200) { show(r); code = 1; }
    else { const text = typeof r.body === 'string' ? r.body : JSON.stringify(r.body, null, 2); const out = argAfter('--out'); if (out) { fs.writeFileSync(out, text); console.log(`saved ${out} (${text.length} bytes)`); } else console.log(text); }
  } else { console.error('unknown command'); code = 2; }
  process.exit(code);
}
