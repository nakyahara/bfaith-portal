#!/usr/bin/env node
/**
 * ne-update-send.mjs — NE へ送る (AI_reference CompanyDB構想/25・2026-10-19 の方針 = 編集は自由・自社 ≠ 今朝の NE の欄だけを CSV にして NE の API で上げる・migration 0093)
 *
 * miniPC の送り手。NE に上げるのはここだけ (NE のトークン data/ne-tokens.json は miniPC にある = Render からは上げない)。
 *   --daily   = 毎朝の daily-sync の段「NEへ送る」(照合 ② とロジザードの影の後)。今朝の差を 1 回だけ作って上げる (ops.ne_upd_build('auto'))。
 *               多すぎる朝 (100 行を超える・ふだんの 5 倍) は DB が作らない = まとめに「多すぎるので止めた」。同じ今朝の NE の回で 2 回目は前の答え
 *   --pending = 画面の「今すぐ NE へ送る」の頼みを拾う (数分おきの定期実行)。頼みが無ければ NE の受付の結果を見直すだけ
 * どちらも最初に: 生きている印 (ops.ne_upd_worker_seen = 画面が「今すぐ送る」を出してよいか) → 前に上げたファイルの NE の受付の結果を見直す
 *
 * NE の API (公式 developer.next-engine.com・2026-10-19 に確かめた):
 *   単品 = POST /api_v1_master_goods/upload・セット = /api_v1_master_setgoods/upload (data_type = csv・data = CSV の中身・wait_flag = 1)。返り = que_id (非同期)
 *   受付の結果 = /api_v1_system_que/search (fields = que_id,que_status_id,que_message・que_id-eq) の que_status_id: 2 = 全部成功 / 1 = 処理中 / 0 = 待ち / -1 = 失敗 (理由 = que_message)
 *   アプリの「アクセス情報」に 商品マスタ / セット商品マスタのアップロードとアップロードキューの検索の許可が要る (無い = 006001 / 004001「アクセス情報[...]の許可が必要です」)
 *
 * 🚨 最初は止めて出す: env NE_UPDATE_UPLOAD=1 (上げてよい) が無ければ何も上げない (差の数だけまとめに出す)。自動は NE_UPDATE_AUTO=1 も要る。
 *    中原さんが NE の実物で試してから入れる (設計 25 §2.9)
 * env: COMPANY_DB_WATCH_WRITER_URL (watch_writer = 0093 で ops.ne_upd_build ほかの実行を渡してある)・NE_CLIENT_ID / NE_CLIENT_SECRET (apps/warehouse/ne-api.js)
 * 使い方: node -r dotenv/config scripts/company-db/ne-update-send.mjs --daily | --pending [--json]
 * 最後の行 = まとめの 1 行 (daily-sync が朝のまとめに出す)。DB に届かない・NE の API が落ちた = ❌ (exit 1)。止めてある・差なし・多すぎ = exit 0
 */
import os from 'node:os';

export const STEP_NAME = 'NEへ送る';
export const UPLOAD_ENV = 'NE_UPDATE_UPLOAD';
export const AUTO_ENV = 'NE_UPDATE_AUTO';
/** 上げた後に NE の受付の結果を待つ長さと間 (試験は縮める) */
export const QUE_WAIT_MS = 180000;
export const QUE_POLL_MS = 15000;
/** --pending の 1 回で拾う頼みの数 */
export const PENDING_MAX = 3;
const FNS = Object.freeze(['ops.ne_upd_build(text, text)', 'ops.ne_upd_file_result(bigint, text, integer, text)', 'ops.ne_upd_queued_files()',
  'ops.ne_upd_worker_seen(text, boolean, boolean, text)', 'ops.ne_upd_overview()']);
export const COL_WORDS = Object.freeze({
  'products:name': '名前', 'products:handling': '取扱', 'products:tax_rate': '税率', 'products:standard_price_jpy': '売価', 'products:cost': '原価',
  'products:primary_supplier': '代表の仕入先', 'sets:name': 'セットの名前', 'sets:standard_price_jpy': 'セットの売価', spelling: 'コードの書き方',
});
const on = (env, k) => String(env[k] ?? '').trim() === '1';
const oneLine = (s, n = 200) => String(s ?? '').replace(/\s+/g, ' ').trim().slice(0, n);
const jsonOf = (r) => (typeof r === 'string' ? JSON.parse(r) : r);

/** NE の API (本物)。apps/warehouse/ne-api.js の callNE (トークンの更新もそこ) を使う */
export async function realNeClient() {
  const { callNE } = await import('../../apps/warehouse/ne-api.js');
  return {
    async upload(kind, csvText) {
      const ep = kind === 'sets' ? '/api_v1_master_setgoods/upload' : '/api_v1_master_goods/upload';
      const r = await callNE(ep, { data_type: 'csv', data: csvText });
      if (r.result !== 'success' || r.que_id == null) throw new Error(`NE がアップロードを受け付けなかった: ${oneLine(r.message || JSON.stringify(r), 300)}`);
      return { que_id: String(r.que_id) };
    },
    async queStatus(queId) {
      const r = await callNE('/api_v1_system_que/search', { fields: 'que_id,que_status_id,que_message', 'que_id-eq': String(queId) });
      const row = Array.isArray(r.data) ? r.data.find((x) => String(x.que_id) === String(queId)) : null;
      if (!row) return { status: null, message: null };
      return { status: row.que_status_id == null || row.que_status_id === '' ? null : Number(row.que_status_id), message: row.que_message ?? null };
    },
  };
}

/** 関数があって、この接続のロールが実行できるか (0093 の前・ロールの流し直しの前 = 何もしない) */
async function ready(db) {
  for (const f of FNS) {
    const s = (await db.query(`select case when to_regprocedure($1::text) is null then 'not_applied'
      when has_function_privilege(to_regprocedure($1::text)::oid, 'execute') then 'ok' else 'no_privilege' end as s`, [f])).rows[0].s;
    if (s !== 'ok') return s;
  }
  return 'ok';
}

/** NE の受付の結果を見直す (queued)・作ったのに上げていないまま 30 分たったファイル (送り手が途中で止まった) は failed にする */
async function refreshQueue(db, ne, { now = Date.now() } = {}) {
  const files = jsonOf((await db.query('select ops.ne_upd_queued_files() as r')).rows[0].r);
  const out = { checked: 0, ok: 0, failed: 0, waiting: 0 };
  for (const f of files) {
    if (f.state === 'made') {
      if (now - Date.parse(f.created_at) > 30 * 60000) {
        await db.query('select ops.ne_upd_file_result($1, null, null, $2)', [f.file_id, '上げる前に送り手が止まった (次の回で作り直す)']);
        out.failed++;
      }
      continue;
    }
    if (!ne || !f.que_id) continue;
    let st;
    try { st = await ne.queStatus(f.que_id); } catch (e) { out.waiting++; continue; }   // 見られない = 次に見る
    out.checked++;
    if (st.status == null || st.status === 0 || st.status === 1) {
      if (st.status != null) await db.query('select ops.ne_upd_file_result($1, $2, $3, $4)', [f.file_id, f.que_id, st.status, st.message]);
      out.waiting++;
      continue;
    }
    const r = jsonOf((await db.query('select ops.ne_upd_file_result($1, $2, $3, $4) as r', [f.file_id, f.que_id, st.status, st.message])).rows[0].r);
    if (r.state === 'ok') out.ok++; else if (r.state === 'failed') out.failed++;
  }
  return out;
}

/** 作ったファイルを NE に上げて、受付の結果を待つ (待ちきれない = queued のまま・次の回で見る) */
async function uploadFiles(db, ne, files, { waitMs = QUE_WAIT_MS, pollMs = QUE_POLL_MS, sleep }) {
  const res = [];
  for (const f of files) {
    if (!f.bytes_b64) continue;
    const text = Buffer.from(f.bytes_b64, 'base64').toString('utf8');
    let que;
    try {
      que = await ne.upload(f.kind, text);
    } catch (e) {
      const msg = oneLine(e && e.message, 500);
      await db.query('select ops.ne_upd_file_result($1, null, null, $2)', [f.file_id, msg]);
      res.push({ file_id: f.file_id, kind: f.kind, state: 'failed', message: msg });
      continue;
    }
    await db.query('select ops.ne_upd_file_result($1, $2, null, null)', [f.file_id, que.que_id]);
    const t0 = Date.now();
    let st = { status: null, message: null };
    for (;;) {
      try { st = await ne.queStatus(que.que_id); } catch { st = { status: null, message: null }; }
      if (st.status === 2 || st.status === -1) break;
      if (Date.now() - t0 >= waitMs) break;
      await sleep(pollMs);
    }
    const r = jsonOf((await db.query('select ops.ne_upd_file_result($1, $2, $3, $4) as r', [f.file_id, que.que_id, st.status, st.message])).rows[0].r);
    res.push({ file_id: f.file_id, kind: f.kind, state: r.state, que_id: que.que_id, status: st.status, message: st.message });
  }
  return res;
}

/** 回の中身の言葉 (原価 2・売価 1 …) */
export function colsText(summary) {
  const c = summary && summary.cols ? summary.cols : {};
  return Object.entries(c).sort((a, b) => b[1] - a[1]).map(([k, n]) => `${COL_WORDS[k] || k} ${n}`).join('・');
}
/** 1 回の結果のまとめの 1 行 */
export function lineOf(how, b, uploads) {
  const head = how === 'auto' ? `${STEP_NAME} (毎朝)` : `${STEP_NAME} (今すぐ)`;
  if (b.state === 'empty') return `✅ ${head}: 送る差はありませんでした`;
  if (b.state === 'held') return `⚠️ ${head}: 送りませんでした — ${oneLine(b.reason, 300)}`;
  if (b.state === 'nothing') return `✅ ${head}: 頼みなし`;
  if (b.state === 'busy') return `⏸️ ${head}: 前に送ったファイルを NE がまだ受け付け中 (次の回で送る)`;
  if (b.replayed && !uploads.length) return `✅ ${head}: 今朝の分はもう送ってあります (${b.state === 'held' ? `送らなかった — ${oneLine(b.reason, 200)}` : b.state})`;
  const failed = uploads.filter((u) => u.state === 'failed');
  const waiting = uploads.filter((u) => u.state === 'queued');
  const what = `${b.rows ?? '?'} 欄 (${colsText(b.summary) || '—'})${b.left ? `・残り ${b.left} 行は次の回` : ''}`;
  if (failed.length) return `❌ ${head}: ${what} を送ろうとして NE に断られた / 上げられなかった — ${oneLine(failed.map((u) => u.message).filter(Boolean).join(' / ') || '理由なし', 300)}`;
  if (waiting.length) return `✅ ${head}: ${what} を NE に上げた (NE の受付の結果はまだ = 次の回に見る)`;
  return `✅ ${head}: ${what} を NE に上げた (NE が受け付けた・翌朝の照合で NE と同じになれば一覧から消える)`;
}

/**
 * 1 回分。p = { db, ne (NE の API・無ければ上げない), mode ('daily' | 'pending'), env, host, sleep, waitMs, pollMs }
 * 戻り値 = { code (exit), line (まとめの 1 行), sends: [...] }
 */
export async function runNeUpdateSend({ db, ne = null, mode, env = process.env, host = os.hostname(), sleep = (ms) => new Promise((r) => setTimeout(r, ms)), waitMs = QUE_WAIT_MS, pollMs = QUE_POLL_MS }) {
  const r0 = await ready(db);
  if (r0 !== 'ok') return { code: 0, line: `⏭️ ${STEP_NAME}: ${r0 === 'not_applied' ? 'Company DB にまだ仕組みが無い (migration 0093 の前)' : 'ロールの流し直しの前 (実行権が無い)'}`, sends: [] };
  const uploadOn = on(env, UPLOAD_ENV);
  const autoOn = uploadOn && on(env, AUTO_ENV);
  await db.query('select ops.ne_upd_worker_seen($1, $2, $3, $4)', [String(host).slice(0, 100), uploadOn, autoOn, 'ne-upd-v1']);
  const nec = uploadOn ? ne : null;
  const q = await refreshQueue(db, nec);
  if (!uploadOn || (mode === 'daily' && !autoOn)) {
    const ov = jsonOf((await db.query('select ops.ne_upd_overview() as r')).rows[0].r);
    const n = (ov.units || []).filter((u) => u.status === 'send').length;
    return { code: 0, line: `⏸️ ${STEP_NAME}: 自動で NE に上げるのは止めてあります (${mode === 'daily' ? `${AUTO_ENV}=1 と ` : ''}${UPLOAD_ENV}=1 で始める)・今の差 ${n} 欄 (ポータルの「NEへ送る」で CSV にできます)`, sends: [], queue: q };
  }
  if (!nec) throw new Error('NE の API が無い');
  const sends = [];
  const lines = [];
  const rounds = mode === 'daily' ? 1 : PENDING_MAX;
  for (let i = 0; i < rounds; i++) {
    // 前のファイルを NE がまだ受け付け中なら、少し待ってから (自動は 1 回だけの朝なので待つ)
    if (mode === 'daily') {
      const t0 = Date.now();
      while ((await refreshQueue(db, nec)).waiting > 0 && Date.now() - t0 < waitMs) await sleep(pollMs);
    }
    const b = jsonOf((await db.query('select ops.ne_upd_build($1, $2) as r', [mode === 'daily' ? 'auto' : 'now', String(host).slice(0, 100)])).rows[0].r);
    if (b.state === 'nothing') break;
    const files = (b.files || []).filter((f) => f.bytes_b64);
    const uploads = b.state === 'built' || (b.replayed && files.length) ? await uploadFiles(db, nec, files, { waitMs, pollMs, sleep }) : [];
    sends.push({ ...b, files: (b.files || []).map(({ bytes_b64, ...f }) => f), uploads });
    lines.push(lineOf(mode === 'daily' ? 'auto' : 'now', b, uploads));
    if (b.state === 'busy') break;
  }
  if (!lines.length) lines.push(q.ok || q.failed ? `✅ ${STEP_NAME}: NE の受付の結果を見直した (受け付けた ${q.ok}・断られた ${q.failed})` : `✅ ${STEP_NAME}: 頼みなし`);
  const bad = sends.some((s) => (s.uploads || []).some((u) => u.state === 'failed'));
  return { code: bad ? 1 : 0, line: lines.join(' / '), sends, queue: q };
}

const isMain = process.argv[1] && /ne-update-send\.mjs$/i.test(process.argv[1]);
if (isMain) {
  let code = 1, last = '';
  const args = process.argv.slice(2);
  const mode = args.includes('--daily') ? 'daily' : args.includes('--pending') ? 'pending' : null;
  let client = null;
  try {
    if (!mode) throw new Error('--daily か --pending が要る');
    const url = (process.env.COMPANY_DB_WATCH_WRITER_URL || '').trim();
    if (!url) {
      last = `⏭️ ${STEP_NAME}: 未設定 (COMPANY_DB_WATCH_WRITER_URL)`;
      code = 0;
    } else {
      const { openPgClient, pgAdapter } = await import('./migrate.mjs');
      client = await openPgClient(url);
      await client.query(`set statement_timeout = '120s'`);
      const ne = on(process.env, UPLOAD_ENV) ? await realNeClient() : null;
      const r = await runNeUpdateSend({ db: pgAdapter(client), ne, mode });
      if (args.includes('--json')) console.log(JSON.stringify(r, null, 1));
      last = r.line; code = r.code;
    }
  } catch (e) {
    last = `❌ ${STEP_NAME}: ${oneLine(e && e.message, 400)}`;
    code = 1;
  } finally {
    if (client) { try { await client.end(); } catch { /* */ } }
  }
  console.log(oneLine(last, 1000));
  // pg の直後に process.exit() しない (Windows の Node は libuv の assertion で 127 になる。#1386)
  process.exitCode = code;
  setTimeout(() => process.exit(code), 10000).unref();
}
