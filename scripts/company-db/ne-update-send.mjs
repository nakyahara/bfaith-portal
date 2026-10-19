#!/usr/bin/env node
/**
 * ne-update-send.mjs — NE へ送る (AI_reference CompanyDB構想/25・2026-10-19 の方針 = 編集は自由・自社 ≠ 今朝の NE の欄だけを CSV にする・migration 0093)
 *
 * miniPC の送り手。
 *   --daily   = 毎朝の daily-sync の段「NEへ送る」(照合 ② とロジザードの影の後)。今朝の差の CSV を 1 回だけ作る (ops.ne_upd_build('auto'))。
 *               いちばん使う形 = 中原さんが画面「NEへ送る」で CSV を作って (or 受け取って) NE の画面で取り込む。まとめに 1 行 (差 N 欄・欄の内訳)。
 *               多すぎる朝 (100 欄を超える・ふだんの 5 倍) は DB が作らない = まとめに「多すぎるので止めた」。同じ今朝の NE の回で 2 回目は前の答え
 *   --pending = 画面の「今すぐ NE へ送る」の頼みを拾って API で上げる (API の口を開けたときだけ・数分おきの定期実行)
 * API の口 (既定は止めてある): env NE_UPDATE_UPLOAD=1 で開く (今すぐ)・毎朝の分も API で上げるのは NE_UPDATE_AUTO=1 も。中原さんが NE の実物で試してから
 *   単品 = POST /api_v1_master_goods/upload・セット = /api_v1_master_setgoods/upload (data_type = csv・data = CSV の中身)。返り = que_id (非同期)
 *   🚨 呼ぶ回数を少なく (2026-10-19 coordinator): 1 ファイル = 1 回。キューの結果 (/api_v1_system_que/search) は既定で聞かない = 翌朝の照合で NE と同じになったかを確かめる。
 *      env NE_UPDATE_QUE_CHECK=1 のときだけ、上げて 1 分後に 1 回だけ聞く (2 = 全部成功 / -1 = 失敗)
 *   アプリの「アクセス情報」に 商品マスタ / セット商品マスタのアップロード (とキューを聞くならアップロードキューの検索) の許可が要る (無い = 006001 / 004001)
 *   API がはっきり断った (result = error) = failed (もう一度送れる) / 通信が切れた・応答が読めない = unknown (受付が分からない = 自動では送り直さない・翌朝の照合を待つ)
 *   同じファイルは作った送り手が作った時の 1 回だけ上げる (DB が claimed で守る・前の答えにバイト列は無い)
 * env: COMPANY_DB_WATCH_WRITER_URL (watch_writer = 0093 で ops.ne_upd_build ほかの実行を渡してある)・NE_CLIENT_ID / NE_CLIENT_SECRET (apps/warehouse/ne-api.js)
 * 使い方: node -r dotenv/config scripts/company-db/ne-update-send.mjs --daily | --pending [--json]
 * 最後の行 = まとめの 1 行 (daily-sync が朝のまとめに出す)。DB に届かない・NE が断った = ❌ (exit 1)。差なし・多すぎ = exit 0
 */
import os from 'node:os';

export const STEP_NAME = 'NEへ送る';
export const UPLOAD_ENV = 'NE_UPDATE_UPLOAD';
export const AUTO_ENV = 'NE_UPDATE_AUTO';
export const QUE_CHECK_ENV = 'NE_UPDATE_QUE_CHECK';
/** キューを 1 回だけ聞くとき、上げてから待つ長さ (試験は縮める) */
export const QUE_WAIT_MS = 60000;
/** --pending の 1 回で拾う頼みの数 */
export const PENDING_MAX = 3;
const FNS = Object.freeze(['ops.ne_upd_build(text, text, boolean)', 'ops.ne_upd_file_result(bigint, text, text, integer, text)', 'ops.ne_upd_queued_files()',
  'ops.ne_upd_worker_seen(text, boolean, boolean, text)', 'ops.ne_upd_overview()']);
export const COL_WORDS = Object.freeze({
  'products:name': '名前', 'products:handling': '取扱', 'products:tax_rate': '税率', 'products:standard_price_jpy': '売価', 'products:cost': '原価',
  'products:primary_supplier': '代表の仕入先', 'sets:name': 'セットの名前', 'sets:standard_price_jpy': 'セットの売価', spelling: 'コードの書き方',
});
const on = (env, k) => String(env[k] ?? '').trim() === '1';
const oneLine = (s, n = 200) => String(s ?? '').replace(/\s+/g, ' ').trim().slice(0, n);
const jsonOf = (r) => (typeof r === 'string' ? JSON.parse(r) : r);

/** NE の API が「はっきり断った」誤り (受付していない = もう一度送ってよい)。通信の誤り・読めない応答 = 受付が分からない */
export class NeRejected extends Error {}

/** NE の API (本物)。apps/warehouse/ne-api.js の callNE (トークンの更新もそこ) を使う */
export async function realNeClient() {
  const { callNE } = await import('../../apps/warehouse/ne-api.js');
  return {
    async upload(kind, csvText) {
      const ep = kind === 'sets' ? '/api_v1_master_setgoods/upload' : '/api_v1_master_goods/upload';
      let r;
      try { r = await callNE(ep, { data_type: 'csv', data: csvText }); } catch (e) {
        // callNE は result = error を「NE API エラー: …」で投げる = NE が受け付けなかった。それ以外 (通信・JSON) = 分からない
        if (/^NE API エラー/.test(String(e && e.message))) throw new NeRejected(String(e.message));
        throw e;
      }
      if (r.result !== 'success') throw new NeRejected(`NE がアップロードを受け付けなかった: ${oneLine(r.message || JSON.stringify(r), 300)}`);
      if (r.que_id == null) throw new Error('NE の応答に que_id が無い (受付が分からない)');
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
const fileResult = async (db, fileId, queId, outcome, status = null, message = null) =>
  jsonOf((await db.query('select ops.ne_upd_file_result($1, $2, $3, $4, $5) as r', [fileId, queId, outcome, status, message])).rows[0].r);

/**
 * 前の回の後片付け: 上げる途中で止まったファイル (作って 30 分たっても上げた記録が無い) = unknown (上げたかどうか分からない = 送り直さない)。
 * キューを聞く設定 (NE_UPDATE_QUE_CHECK=1) なら、まだ聞いていない上げたファイルを 1 回だけ聞く
 */
async function tidy(db, ne, { queCheck = false, now = Date.now() } = {}) {
  const files = jsonOf((await db.query('select ops.ne_upd_queued_files() as r')).rows[0].r);
  const out = { unknown: 0, ok: 0, failed: 0 };
  for (const f of files) {
    if (f.state === 'made' && now - Date.parse(f.claimed_at) > 30 * 60000) {
      await fileResult(db, f.file_id, null, 'unknown', null, '上げる途中で送り手が止まった (NE に入ったか分からない = 翌朝の照合で確かめる)');
      out.unknown++;
    } else if (queCheck && ne && f.state === 'queued' && f.que_status == null && f.que_id && now - Date.parse(f.uploaded_at) > 60000) {
      let st;
      try { st = await ne.queStatus(f.que_id); } catch { continue; }
      if (st.status === 2 || st.status === -1) {
        const r = await fileResult(db, f.file_id, f.que_id, st.status === 2 ? 'ok' : 'failed', st.status, st.message);
        if (r.state === 'ok') out.ok++; else if (r.state === 'failed') out.failed++;
      }
    }
  }
  return out;
}

/** 作ったファイルを NE に上げる (1 ファイル = 1 回)。キューを聞く設定なら少し待って 1 回だけ聞く */
async function uploadFiles(db, ne, files, { queCheck = false, waitMs = QUE_WAIT_MS, sleep }) {
  const res = [];
  for (const f of files) {
    if (!f.bytes_b64) continue;
    const text = Buffer.from(f.bytes_b64, 'base64').toString('utf8');
    let que;
    try {
      que = await ne.upload(f.kind, text);
    } catch (e) {
      const msg = oneLine(e && e.message, 500);
      const outcome = e instanceof NeRejected ? 'failed' : 'unknown';
      await fileResult(db, f.file_id, null, outcome, null, msg);
      res.push({ file_id: f.file_id, kind: f.kind, state: outcome, message: msg });
      continue;
    }
    await fileResult(db, f.file_id, que.que_id, 'queued');
    res.push({ file_id: f.file_id, kind: f.kind, state: 'queued', que_id: que.que_id });
  }
  if (queCheck && res.some((u) => u.state === 'queued')) {
    await sleep(waitMs);
    for (const u of res.filter((x) => x.state === 'queued')) {
      let st;
      try { st = await ne.queStatus(u.que_id); } catch { continue; }
      if (st.status === 2 || st.status === -1) {
        const r = await fileResult(db, u.file_id, u.que_id, st.status === 2 ? 'ok' : 'failed', st.status, st.message);
        Object.assign(u, { state: r.state, message: st.message });
      }
    }
  }
  return res;
}

/** 回の中身の言葉 (原価 2・売価 1 …) */
export function colsText(summary) {
  const c = summary && summary.cols ? summary.cols : {};
  return Object.entries(c).sort((a, b) => b[1] - a[1]).map(([k, n]) => `${COL_WORDS[k] || k} ${n}`).join('・');
}
/** 1 回の結果のまとめの 1 行 */
export function lineOf(how, b, uploads, { upload = false } = {}) {
  const head = how === 'auto' ? STEP_NAME : `${STEP_NAME} (今すぐ)`;
  if (b.state === 'empty') return `✅ ${head}: NE とちがう所はありません`;
  if (b.state === 'held') return `⚠️ ${head}: ${oneLine(b.reason, 300)}`;
  if (b.state === 'nothing') return `✅ ${head}: 頼みなし`;
  if (b.state === 'busy') return `⏸️ ${head}: 前に上げたファイルがまだ動いているので、次の回に送ります`;
  const what = `${b.rows ?? '?'} 欄 (${colsText(b.summary) || '—'})`;
  if (b.replayed && !uploads.length) return `✅ ${head}: 今朝の分はもう作ってあります (${what})`;
  if (!upload) return `✅ ${head}: NE とちがう所 ${what} → ポータルの「NEへ送る」で CSV を作って NE に取り込んでください`;
  const failed = uploads.filter((u) => u.state === 'failed');
  const unknown = uploads.filter((u) => u.state === 'unknown');
  if (failed.length) return `❌ ${head}: ${what} を NE に上げようとして断られました — ${oneLine(failed.map((u) => u.message).filter(Boolean).join(' / ') || '理由なし', 300)} (ポータルの「NEへ送る」で CSV を作れます)`;
  if (unknown.length) return `⚠️ ${head}: ${what} を NE に上げましたが、届いたか分かりません (通信が切れた)。翌朝の照合で確かめます — ${oneLine(unknown[0].message, 200)}`;
  return `✅ ${head}: ${what} を NE に上げました (翌朝の照合で NE と同じになれば一覧から消えます)`;
}

/**
 * 1 回分。p = { db, ne (NE の API・API の口を開けたときだけ), mode ('daily' | 'pending'), env, host, sleep, waitMs }
 * 戻り値 = { code (exit), line (まとめの 1 行), sends: [...] }
 */
export async function runNeUpdateSend({ db, ne = null, mode, env = process.env, host = os.hostname(), sleep = (ms) => new Promise((r) => setTimeout(r, ms)), waitMs = QUE_WAIT_MS }) {
  const r0 = await ready(db);
  if (r0 !== 'ok') return { code: 0, line: `⏭️ ${STEP_NAME}: ${r0 === 'not_applied' ? 'Company DB にまだ仕組みが無い (migration 0093 の前)' : 'ロールの流し直しの前 (実行権が無い)'}`, sends: [] };
  const uploadOn = on(env, UPLOAD_ENV) && !!ne;
  const autoUpload = uploadOn && on(env, AUTO_ENV);
  const queCheck = uploadOn && on(env, QUE_CHECK_ENV);
  const hostName = String(host).slice(0, 100);
  await db.query('select ops.ne_upd_worker_seen($1, $2, $3, $4)', [hostName, uploadOn, autoUpload, 'ne-upd-v2']);
  const t = await tidy(db, uploadOn ? ne : null, { queCheck });
  if (mode === 'pending' && !uploadOn) return { code: 0, line: `⏸️ ${STEP_NAME} (今すぐ): API の口は止めてあります`, sends: [], tidy: t };
  const sends = [];
  const lines = [];
  const rounds = mode === 'daily' ? 1 : PENDING_MAX;
  for (let i = 0; i < rounds; i++) {
    const up = mode === 'daily' ? autoUpload : true;
    const b = jsonOf((await db.query('select ops.ne_upd_build($1, $2, $3) as r', [mode === 'daily' ? 'auto' : 'now', hostName, up])).rows[0].r);
    if (b.state === 'nothing') break;
    const files = (b.files || []).filter((f) => f.bytes_b64);
    const uploads = up && b.state === 'built' && files.length ? await uploadFiles(db, ne, files, { queCheck, waitMs, sleep }) : [];
    sends.push({ ...b, files: (b.files || []).map(({ bytes_b64, ...f }) => f), uploads });
    lines.push(lineOf(mode === 'daily' ? 'auto' : 'now', b, uploads, { upload: up && !b.replayed }));
    if (b.state === 'busy') break;
  }
  if (t.failed) lines.push(`前に上げたファイル ${t.failed} つを NE が断りました (ポータルの「NEへ送る」で CSV を作れます)`);
  if (!lines.length) lines.push(`✅ ${STEP_NAME} (今すぐ): 頼みなし`);
  const bad = t.failed > 0 || sends.some((s) => (s.uploads || []).some((u) => u.state === 'failed'));
  return { code: bad ? 1 : 0, line: lines.join(' / '), sends, tidy: t };
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
