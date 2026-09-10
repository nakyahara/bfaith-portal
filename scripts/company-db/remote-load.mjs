#!/usr/bin/env node
/**
 * remote-load.mjs — miniPC (や手元) から Render の Company DB 同期 API を叩く。
 *
 *   node scripts/company-db/remote-load.mjs status [--counts]         GET /status (既定は counts=0 = Postgres に繋がない)
 *   node scripts/company-db/remote-load.mjs load [--apply] [--wait]   POST /load (既定 dry-run)。--wait で終わるまで 10 秒おきに見る
 *   node scripts/company-db/remote-load.mjs wait                      current が無くなるまで待つ (最大 30 分)
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

export function baseOrigin(env = process.env) {
  const mirror = String(env.RENDER_MIRROR_URL || '').trim();
  const portal = String(env.RENDER_PORTAL_URL || '').trim();
  let m; try { m = mirror ? new URL(mirror) : null; } catch { m = null; }
  let p; try { p = portal ? new URL(portal) : null; } catch { p = null; }
  if (p && (!m || p.host === m.host) && p.protocol === 'https:') return p.origin;
  if (m && m.protocol === 'https:') return m.origin;
  return '';
}

const isMain = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) {
  const args = process.argv.slice(2);
  const cmd = args[0] || 'status';
  const flag = (f) => args.includes(f);
  const argAfter = (f) => { const i = args.indexOf(f); return i >= 0 && i < args.length - 1 ? args[i + 1] : null; };
  const origin = baseOrigin();
  const key = process.env.MIRROR_SYNC_KEY || '';
  if (!origin) { console.error('RENDER_MIRROR_URL (https) が要る'); process.exit(2); }
  if (!key) { console.error('MIRROR_SYNC_KEY が要る'); process.exit(2); }

  const call = async (p, opt = {}) => {
    const res = await fetch(`${origin}${p}`, { ...opt, headers: { 'x-sync-key': key, ...(opt.headers || {}) } });
    const text = await res.text();
    let body; try { body = JSON.parse(text); } catch { body = text; }
    return { status: res.status, body };
  };
  const show = (r) => console.log(typeof r.body === 'string' ? r.body : JSON.stringify(r, null, 2));
  const waitDone = async () => {
    const t0 = Date.now();
    for (;;) {
      const r = await call('/apps/company-db/sync/status?counts=0');
      if (r.status !== 200) { show(r); return 1; }
      const cur = r.body?.current;
      if (!cur) { show({ status: r.status, body: { last: r.body.last, latest: r.body.latest, interrupted: r.body.interrupted } }); return r.body.last?.status === 'failed' ? 1 : 0; }
      console.log(`running ${cur.run_id} since ${cur.started_at} (${Math.round((Date.now() - t0) / 1000)}s)`);
      if (Date.now() - t0 > 30 * 60 * 1000) { console.error('30 分たっても終わらない'); return 1; }
      await new Promise((r2) => setTimeout(r2, 10000));
    }
  };

  let code = 0;
  if (cmd === 'status') show(await call(`/apps/company-db/sync/status?counts=${flag('--counts') ? 1 : 0}`));
  else if (cmd === 'load') { const r = await call(`/apps/company-db/sync/load${flag('--apply') ? '?apply=1' : ''}`, { method: 'POST' }); show(r); if (r.status !== 202) code = 1; else if (flag('--wait')) code = await waitDone(); }
  else if (cmd === 'wait') code = await waitDone();
  else if (cmd === 'reports') show(await call('/apps/company-db/sync/reports'));
  else if (cmd === 'report') {
    const runId = args[1]; if (!runId) { console.error('run_id が要る'); process.exit(2); }
    const r = await call(`/apps/company-db/sync/report/${encodeURIComponent(runId)}${flag('--md') ? '?format=md' : ''}`);
    if (r.status !== 200) { show(r); code = 1; }
    else { const text = typeof r.body === 'string' ? r.body : JSON.stringify(r.body, null, 2); const out = argAfter('--out'); if (out) { fs.writeFileSync(out, text); console.log(`saved ${out} (${text.length} bytes)`); } else console.log(text); }
  } else { console.error('unknown command'); code = 2; }
  process.exit(code);
}
