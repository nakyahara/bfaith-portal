/**
 * 商品別 想定利益 — 世代を Render へ送って公開する (miniPC 側)
 *
 * 正本 = §7.5 / §15-7
 *
 * 🚨 成功 ping の条件は「Render の公開ポインタを読み戻して、対象世代への切替を確認できた」こと。
 *    プロセスの正常終了やローカルの published では ok を打たない (Codex R2-4)。
 */
import { getExpectedProfitDB } from './db.js';
import { chunkChecksum } from './publish-api.js';

const CHUNK_SIZE = 500;

/** 世代をチャンクに分ける (純関数。テストで固定する) */
export function makeChunks(rows, size = CHUNK_SIZE) {
  const out = [];
  for (let i = 0; i < rows.length; i += size) {
    const slice = rows.slice(i, i + size);
    out.push({ chunk_index: out.length, rows: slice, checksum: chunkChecksum(slice) });
  }
  return out;
}

/** 送信する manifest (件数・内容ハッシュ) */
export function makeManifest(gen) {
  return {
    built_at: gen.built_at,
    row_count: gen.row_count,
    ok_count: gen.ok_count,
    incomplete_count: gen.incomplete_count,
    rank_eligible_count: gen.rank_eligible_count,
    content_hash: gen.content_hash,
    malls_included: gen.malls_included ? JSON.parse(gen.malls_included) : [],
    malls_degraded: gen.malls_degraded ? JSON.parse(gen.malls_degraded) : [],
  };
}

/**
 * 世代を送って公開し、読み戻して確認する。
 *
 * @param {object} db expected-profit.db
 * @param {string} generationId
 * @param {object} deps { postChunk, postPublish, getPublished, chunkSize }
 * @returns {Promise<{ok:boolean, confirmed:boolean, ...}>}
 */
export async function publishToRender(db, generationId, deps) {
  const gen = db.prepare('SELECT * FROM expected_profit_generation WHERE generation_id = ?').get(generationId);
  if (!gen) return { ok: false, error: 'generation_not_found' };
  // 🚨 検証を通っていない世代は送らない
  if (gen.local_status !== 'validated') {
    return { ok: false, error: `local_status_${gen.local_status}` };
  }

  const rows = db.prepare('SELECT * FROM mart_listing_expected_profit WHERE generation_id = ? ORDER BY mall, shop_id, mall_item_key')
    .all(generationId);
  if (rows.length === 0) return { ok: false, error: 'no_rows' };

  const manifest = makeManifest(gen);
  const chunks = makeChunks(rows, deps.chunkSize || CHUNK_SIZE);

  db.prepare("UPDATE expected_profit_generation SET remote_status = 'sending' WHERE generation_id = ?").run(generationId);

  for (const c of chunks) {
    // 🚨 チャンクごとに期限を見る。全チャンク送り切るまで止まらないと期限を越える
    if (deps.deadline && new Date() >= deps.deadline) {
      db.prepare("UPDATE expected_profit_generation SET remote_status = 'not_sent' WHERE generation_id = ?").run(generationId);
      return { ok: false, error: 'deadline_exceeded', sentChunks: c.chunk_index };
    }
    const res = await deps.postChunk(generationId, { ...c, seq: gen.seq, manifest });
    if (!res?.ok) {
      db.prepare("UPDATE expected_profit_generation SET remote_status = 'not_sent' WHERE generation_id = ?").run(generationId);
      return { ok: false, error: 'chunk_failed', detail: res };
    }
  }

  // 🚨 最後のチャンクの応答を待つ間に期限を跨ぐことがある。公開の直前にもう一度見る (Codex R6-2)。
  //    ここを通さないと、06:00 を過ぎてから公開が始まる
  if (deps.deadline && new Date() >= deps.deadline) {
    db.prepare("UPDATE expected_profit_generation SET remote_status = 'received' WHERE generation_id = ?").run(generationId);
    return { ok: false, error: 'deadline_exceeded', phase: 'before_publish', chunks: chunks.length };
  }
  const pub = await deps.postPublish({ generation_id: generationId, seq: gen.seq, manifest });
  if (!pub?.ok) {
    db.prepare("UPDATE expected_profit_generation SET remote_status = 'received' WHERE generation_id = ?").run(generationId);
    return { ok: false, error: 'publish_failed', detail: pub };
  }

  // 🚨 読み戻して確認するまで成功にしない
  const published = await deps.getPublished();
  const confirmed = published?.generation_id === generationId && published?.seq === gen.seq;
  db.prepare('UPDATE expected_profit_generation SET remote_status = ? WHERE generation_id = ?')
    .run(confirmed ? 'published' : 'received', generationId);
  if (confirmed) {
    // ローカルでも前世代を superseded にする
    db.prepare(`UPDATE expected_profit_generation SET remote_status = 'superseded'
                WHERE remote_status = 'published' AND generation_id <> ?`).run(generationId);
    db.prepare("UPDATE expected_profit_generation SET remote_status = 'published' WHERE generation_id = ?").run(generationId);
  }

  return {
    ok: confirmed,
    confirmed,
    generationId,
    seq: gen.seq,
    chunks: chunks.length,
    rows: rows.length,
    published,
    error: confirmed ? null : 'publish_not_confirmed',
  };
}

/** HTTP 版の deps (Render の受け口を叩く) */
// 🚨 timeout を必ず付ける。無いと転送が固まったまま期限を越える (Codex R4-9)
const HTTP_TIMEOUT_MS = 120_000;

export function httpDeps(deadline = null) {
  const base = (process.env.RENDER_PORTAL_URL || '').replace(/\/+$/, '');
  const key = process.env.MIRROR_SYNC_KEY;
  if (!base) throw new Error('RENDER_PORTAL_URL not configured');
  if (!key) throw new Error('MIRROR_SYNC_KEY not configured');
  const headers = { 'Content-Type': 'application/json', 'x-sync-key': key };
  const url = (p) => `${base}/apps/expected-profit/sync${p}`;
  // 🚨 timeout は「残り時間」を超えない。固定 120 秒だと期限を跨いで待ち続ける
  const withTimeout = (ms = HTTP_TIMEOUT_MS) => {
    const cap = deadline ? Math.max(1000, deadline.getTime() - Date.now()) : ms;
    return AbortSignal.timeout(Math.min(ms, cap));
  };
  return {
    deadline,
    postChunk: async (id, body) => (await fetch(url(`/generations/${encodeURIComponent(id)}/chunks`),
      { method: 'POST', headers, body: JSON.stringify(body), signal: withTimeout() })).json(),
    postPublish: async (body) => (await fetch(url('/publish'),
      { method: 'POST', headers, body: JSON.stringify(body), signal: withTimeout() })).json(),
    getPublished: async () => {
      const r = await (await fetch(url('/published'), { headers, signal: withTimeout(30_000) })).json();
      return r?.published || null;
    },
  };
}
