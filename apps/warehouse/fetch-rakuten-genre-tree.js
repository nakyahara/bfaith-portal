#!/usr/bin/env node
/**
 * fetch-rakuten-genre-tree.js — LINEギフト Phase 1 A-5a-1
 *   楽天 RakutenGenreSearch API でジャンルツリーを取得して m_rakuten_genres に投入
 *
 * 設計書: g:/共有ドライブ/AI_reference/システム設計/LINEギフトPhase1設計書_v0.9_20260526.md §13-2 / §13-6
 * Codex 議論: Round 2-6
 *
 * 認証: RAKUTEN_APP_ID (楽天市場 アプリID、.env)
 * API:  https://app.rakuten.co.jp/services/api/IchibaGenre/Search/20140222
 *       params: applicationId, genreId, formatVersion=2
 *       response: { current, children, parents }
 *
 * 動作:
 *   - root (genre_id=0) から再帰的に全ジャンルを取得
 *   - 1 req/sec 厳守 (楽天 Web Service レート制限)
 *   - 失敗時は指数バックオフ (1s, 2s, 4s, ..., 最大60s) + ジッタ
 *   - SHA-256 hash (genre_id + name + is_active) で差分検知
 *   - 差分があれば SCD2 更新 (旧行を valid_to で閉じる + 新行 INSERT)
 *   - tree_version = 'YYYYMMDD-HHMM' (取得バッチ単位)
 *
 * 使い方:
 *   node apps/warehouse/fetch-rakuten-genre-tree.js                     全件取得
 *   node apps/warehouse/fetch-rakuten-genre-tree.js --max-depth=3       階層制限
 *   node apps/warehouse/fetch-rakuten-genre-tree.js --root=100939       特定 root から
 *   node apps/warehouse/fetch-rakuten-genre-tree.js --dry-run           取得のみ、DB 書込なし
 *
 * 注意:
 *   - 楽天ジャンルは数万件規模 → 全件取得は数時間かかる可能性
 *   - 通常運用は月次で十分 (設計書 §13-6 推奨)
 *   - 売上に関係する範囲だけ取得したい場合は --root + --max-depth で絞る
 */
import 'dotenv/config';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { initDB, getDB } from './db.js';

const APP_ID = process.env.RAKUTEN_APP_ID;
const API_URL = 'https://app.rakuten.co.jp/services/api/IchibaGenre/Search/20140222';
const RATE_LIMIT_MS = 1100;            // 1 req/sec + 100ms 余裕
const MAX_RETRY = 6;                    // 指数バックオフ最大 6回 (1+2+4+8+16+32 = 63秒)
const RETRY_BASE_MS = 1000;
const JITTER_MAX_MS = 250;
const LOCK_FILE = path.resolve('data', 'fetch-rakuten-genre-tree.lock');

// JST 時刻は UTC ベースで +9h して壁時計を構築する (feedback_jst_to_iso_string_trap.md)
// toISOString().replace('Z', '+09:00') は実行環境 TZ 依存で破綻する (Codex Round 1 Critical)
function jstWallclock() {
  const utcMs = Date.now();
  return new Date(utcMs + 9 * 60 * 60 * 1000); // .getUTC* が JST 壁時計値
}

function nowJstIso() {
  const j = jstWallclock();
  const yy = j.getUTCFullYear();
  const mm = String(j.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(j.getUTCDate()).padStart(2, '0');
  const hh = String(j.getUTCHours()).padStart(2, '0');
  const mi = String(j.getUTCMinutes()).padStart(2, '0');
  const ss = String(j.getUTCSeconds()).padStart(2, '0');
  return `${yy}-${mm}-${dd}T${hh}:${mi}:${ss}+09:00`;
}

function jstYyyymmddHhmm() {
  const j = jstWallclock();
  const yy = j.getUTCFullYear();
  const mm = String(j.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(j.getUTCDate()).padStart(2, '0');
  const hh = String(j.getUTCHours()).padStart(2, '0');
  const mi = String(j.getUTCMinutes()).padStart(2, '0');
  return `${yy}${mm}${dd}-${hh}${mi}`;
}

// lockOwned: 「このプロセスが lock を取得済か」のフラグ
// Codex Round 2 Critical 反映: acquireLock 失敗時に finally の releaseLock で
// 他プロセスの lock を消す事故を防止する
let lockOwned = false;

function acquireLock() {
  try {
    const fd = fs.openSync(LOCK_FILE, 'wx');
    fs.writeFileSync(fd, JSON.stringify({ pid: process.pid, started_at: nowJstIso() }), 'utf8');
    fs.closeSync(fd);
    lockOwned = true;
    return true;
  } catch (e) {
    if (e.code === 'EEXIST') {
      const content = fs.existsSync(LOCK_FILE) ? fs.readFileSync(LOCK_FILE, 'utf8') : '?';
      console.error(`[fetch-rakuten-genre-tree] FATAL: lock file exists (${LOCK_FILE}): ${content}`);
      console.error('  warehouse.db 並列書込みは禁止。前回プロセスが残っていないか確認、必要なら手動で lock 削除。');
      return false;
    }
    throw e;
  }
}

function releaseLock() {
  if (!lockOwned) return;
  try { fs.unlinkSync(LOCK_FILE); } catch (_) {}
  lockOwned = false;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function jitter() {
  return Math.floor(Math.random() * JITTER_MAX_MS);
}

function sha256Hex(s) {
  return crypto.createHash('sha256').update(s).digest('hex');
}

/**
 * 1 ジャンル分の API リクエスト (指数バックオフ + ジッタ、fail-closed: 最終 throw で全体停止)
 * Codex Round 1 High 3 反映: fail-open ではなく fail-closed が現運用方針 (再実行で復旧)
 */
async function fetchGenreNode(genreId) {
  const url = `${API_URL}?applicationId=${encodeURIComponent(APP_ID)}&genreId=${genreId}&formatVersion=2`;
  let lastErr;
  for (let attempt = 0; attempt < MAX_RETRY; attempt++) {
    try {
      const res = await fetch(url);
      if (res.status === 429 || (res.status >= 500 && res.status < 600)) {
        const wait = RETRY_BASE_MS * Math.pow(2, attempt) + jitter();
        console.warn(`  [retry] genreId=${genreId} HTTP ${res.status}, sleep ${wait}ms (attempt ${attempt + 1}/${MAX_RETRY})`);
        await sleep(wait);
        continue;
      }
      if (!res.ok) {
        throw new Error(`HTTP ${res.status} ${res.statusText}`);
      }
      const body = await res.json();
      if (body?.error) {
        throw new Error(`RakutenAPI error: ${body.error} ${body.error_description || ''}`);
      }
      return body;
    } catch (e) {
      lastErr = e;
      const wait = RETRY_BASE_MS * Math.pow(2, attempt) + jitter();
      console.warn(`  [retry] genreId=${genreId} ${e.message}, sleep ${wait}ms (attempt ${attempt + 1}/${MAX_RETRY})`);
      await sleep(wait);
    }
  }
  throw new Error(`fetchGenreNode failed after ${MAX_RETRY} retries: ${lastErr?.message}`);
}

/**
 * BFS で genre tree 全件取得
 * formatVersion=2 の response: { current: {genreId, genreName, genreLevel}, children: [{genreId, genreName, ...}], parents: [...] }
 */
async function fetchGenreTreeBfs(rootGenreId, maxDepth, onNode) {
  const queue = [{ genreId: rootGenreId, parentGenreId: null, depth: 0 }];
  const seen = new Set();
  let processed = 0;

  while (queue.length > 0) {
    const { genreId, parentGenreId, depth } = queue.shift();
    if (seen.has(genreId)) continue;
    seen.add(genreId);

    const body = await fetchGenreNode(genreId);
    processed += 1;
    if (processed % 50 === 0) {
      console.log(`  [progress] ${processed} nodes fetched, queue=${queue.length}`);
    }

    const current = body?.current || {};
    const children = body?.children || [];

    // formatVersion=2 では children[i] が直接 genre オブジェクト
    const node = {
      genreId: String(current.genreId ?? genreId),
      genreName: current.genreName ?? '',
      genreLevel: current.genreLevel ?? depth,
      parentGenreId: parentGenreId == null ? null : String(parentGenreId),
      isLeaf: children.length === 0 ? 1 : 0,
      rawPayload: body,
    };
    onNode(node);

    if (maxDepth != null && depth >= maxDepth) continue;

    for (const child of children) {
      const childId = String(child.genreId ?? '');
      if (!childId || seen.has(childId)) continue;
      queue.push({ genreId: childId, parentGenreId: node.genreId, depth: depth + 1 });
    }

    await sleep(RATE_LIMIT_MS + jitter());
  }

  return { processed, totalNodes: seen.size };
}

/**
 * genre_path を構築 (parent から root までを ' > ' で連結)
 * 現在 active な行 (is_active=1 AND valid_to IS NULL) のみを辿る。
 * tree_version で絞らない (Codex Round 1 Critical 1 反映): 不変ノードでも path 更新可能にする。
 */
function buildGenrePath(db, genreId) {
  const parts = [];
  let cursor = genreId;
  let guard = 0;
  const seen = new Set();
  while (cursor && guard < 30 && !seen.has(cursor)) {
    seen.add(cursor);
    const row = db
      .prepare(`SELECT genre_name, parent_genre_id FROM m_rakuten_genres
                WHERE genre_id=? AND is_active=1 AND valid_to IS NULL`)
      .get(cursor);
    if (!row) break;
    parts.unshift(row.genre_name);
    cursor = row.parent_genre_id;
    guard += 1;
  }
  return parts.join(' > ');
}

/**
 * 1 ノードを m_rakuten_genres に UPSERT (SCD2、transaction で atomic)
 * source_hash は構造変化 (parent/leaf/level) も検知するよう拡張 (Codex Round 1 High 1)
 */
function upsertGenre(db, node, treeVersion, syncedAt) {
  // 構造的変化を検知できるよう拡張: parent_genre_id / is_leaf / level_no を含める (Codex Round 1 High 1)
  const sourceHashInput = [
    node.genreId,
    node.genreName,
    String(node.parentGenreId ?? ''),
    String(node.isLeaf),
    String(node.genreLevel ?? ''),
    '1', // is_active
  ].join('\t');
  const sourceHash = sha256Hex(sourceHashInput);

  // 既存 active 行を取得
  const existing = db
    .prepare(`SELECT id, source_hash FROM m_rakuten_genres WHERE genre_id=? AND is_active=1 AND valid_to IS NULL`)
    .get(node.genreId);

  if (existing && existing.source_hash === sourceHash) {
    // 変更なし: 何もしない (path 更新は再 pass の更新で行う)
    return { action: 'unchanged', genreId: node.genreId };
  }

  // SCD2 close + insert を transaction で atomic 化 (Codex Round 1 Critical 3)
  const txn = db.transaction(() => {
    if (existing) {
      db.prepare(`UPDATE m_rakuten_genres SET is_active=0, valid_to=?, synced_at=? WHERE id=?`)
        .run(syncedAt, syncedAt, existing.id);
    }
    db.prepare(`
      INSERT INTO m_rakuten_genres (
        genre_id, genre_name, genre_path, parent_genre_id, level_no, is_leaf,
        tree_version, is_active, source_payload_json, source_hash,
        valid_from, valid_to, synced_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, NULL, ?)
    `).run(
      node.genreId, node.genreName, null,
      node.parentGenreId, node.genreLevel, node.isLeaf,
      treeVersion,
      JSON.stringify(node.rawPayload), sourceHash,
      syncedAt, syncedAt
    );
  });
  txn();
  return { action: existing ? 'updated' : 'inserted', genreId: node.genreId };
}

async function main() {
  const args = process.argv.slice(2);
  const dryRun = args.includes('--dry-run');
  const rootArg = args.find((a) => a.startsWith('--root='));
  const depthArg = args.find((a) => a.startsWith('--max-depth='));

  const rootGenreId = rootArg ? rootArg.split('=')[1] : '0';
  const maxDepth = depthArg ? Number(depthArg.split('=')[1]) : null;

  if (!APP_ID) {
    console.error('[fetch-rakuten-genre-tree] FATAL: RAKUTEN_APP_ID が .env に未設定');
    process.exit(1);
  }

  if (!dryRun && !acquireLock()) {
    process.exit(2);
  }

  console.log(`[fetch-rakuten-genre-tree] start (root=${rootGenreId}, maxDepth=${maxDepth ?? 'unlimited'}, dryRun=${dryRun})`);

  initDB();
  const db = getDB();

  const treeVersion = jstYyyymmddHhmm();
  const syncedAt = nowJstIso();
  const counters = { unchanged: 0, inserted: 0, updated: 0, total: 0 };

  const collected = [];
  const stats = await fetchGenreTreeBfs(rootGenreId, maxDepth, (node) => {
    counters.total += 1;
    collected.push(node);
    if (!dryRun) {
      const r = upsertGenre(db, node, treeVersion, syncedAt);
      counters[r.action] = (counters[r.action] ?? 0) + 1;
    }
  });

  if (!dryRun) {
    // path を再 pass で構築 (現在 active 行を tree_version 不問で更新、Codex Round 1 Critical 1)
    console.log(`[fetch-rakuten-genre-tree] building genre_path for ${counters.total} nodes...`);
    const pathTxn = db.transaction(() => {
      for (const node of collected) {
        const p = buildGenrePath(db, node.genreId);
        db.prepare(`UPDATE m_rakuten_genres SET genre_path=? WHERE genre_id=? AND is_active=1 AND valid_to IS NULL`)
          .run(p, node.genreId);
      }
    });
    pathTxn();
  }

  console.log(`[fetch-rakuten-genre-tree] done`);
  console.log(`  fetched=${stats.processed}, totalNodes=${stats.totalNodes}`);
  console.log(`  counters=${JSON.stringify(counters)}`);
  console.log(`  tree_version=${treeVersion}`);
}

main().catch((e) => {
  console.error('[fetch-rakuten-genre-tree] FATAL:', e);
  process.exit(1);
}).finally(() => {
  releaseLock();
});
