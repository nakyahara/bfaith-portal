/**
 * P3: 楽天RMS 自動出品 (2026-07-26 権限smoke実証 → 実装)。
 *
 * スコープ (中原さん決定):
 *   - ジャンルID・商品属性は**手入力** (雛形方式は採らない)
 *   - 画像は Drive → R-Cabinet 自動転送
 *   - 登録は**公開状態で行う** (2026-08-05 中原さん指示。在庫0で登録するので売れることはなく、
 *     在庫は NE 連携が入れる)。〜2026-08-05 は非公開登録→アプリの公開ボタンの二段階だった
 *   - 単品ページ + **カラバリ (バリエーションページ)** に対応 (2026-09-02)。カラバリは
 *     項目選択肢の見出しを draft_rakuten.variant_selector_name に、SKU ごとの値を
 *     draft_sku_selector_values に持つ (NE 側に軸の情報が無いため画面で手入力する)
 *
 * 経路: Render (この service) → miniPC /service-api/rakuten-rms/* (Cloudflare Tunnel)。
 * 楽天キーは miniPC にだけある。Render 側 env は RYS と共通の WAREHOUSE_* / CF_ACCESS_*。
 *
 * smoke で判明した RMS 2.0 の事実 (2026-07-26):
 *   - PUT /es/2.0/items/manage-numbers/{mn} → 201。genreId ごとに必須属性 (IE0418)
 *   - articleNumber は value か exemptionReason が必須 (IE0229)
 *   - 既存商品の attributes は GET の形のまま PUT に使える (= [{name, values:[..]}])
 */
import crypto from 'node:crypto';
import sharp from 'sharp';
import { google } from 'googleapis';

import { getDB, logEvent } from '../db.js';
import { resolveVariationGroup, getNeCost } from '../lib/variation.js';
import { imageTrackBlockReason } from '../lib/workflow-progress.js';
import { validatePageInfo, buildPageInfoHtml, mapNeShippingToRakuten } from '../lib/page-info.js';
// URL の検証は miniPC 側と同じものを使う (別に書くと判定がズレる)
import { parseRakutenItemUrl } from '../../../lib/rakuten-item-page.js';
// 配送方法の「値の意味」の正本 (定数と変換はこの1ファイルだけが決める)。
// db.js のマイグレーションからも使うため、循環参照を避けて lib/ に置いてある
import {
  SHIPPING_METHOD_GROUPS, ALL_SHIPPING_METHOD_GROUPS, YAHOO_OVERRIDE_SHIPPING_GROUPS, toRakutenShippingGroup,
} from '../lib/shipping-groups.js';

// 既存の import 元 (router.js / smoke.mjs) を壊さないための再公開。新しいコードは
// lib/shipping-groups.js から直接取ること
export { SHIPPING_METHOD_GROUPS, ALL_SHIPPING_METHOD_GROUPS, YAHOO_OVERRIDE_SHIPPING_GROUPS, toRakutenShippingGroup };

// ─── miniPC proxy client (rakuten-rms-proxy.js と同じ env / 認証) ───

function requireEnv(name) {
  const v = process.env[name];
  if (!v || !String(v).trim()) {
    const err = new Error(`${name} not configured on Render (fail-closed)`);
    err.statusCode = 503;
    throw err;
  }
  return v.trim();
}

async function callWarehouse(path, { method = 'GET', body = null, timeoutMs = 120_000 } = {}) {
  const base = requireEnv('WAREHOUSE_URL').replace(/\/+$/, '');
  const res = await fetch(`${base}${path}`, {
    method,
    headers: {
      'CF-Access-Client-Id': requireEnv('CF_ACCESS_CLIENT_ID'),
      'CF-Access-Client-Secret': requireEnv('CF_ACCESS_CLIENT_SECRET'),
      'Authorization': `Bearer ${requireEnv('WAREHOUSE_SERVICE_TOKEN')}`,
      'Content-Type': 'application/json',
    },
    body: body !== null ? JSON.stringify(body) : undefined,
    signal: AbortSignal.timeout(timeoutMs),
  });
  let data = null;
  try { data = await res.json(); } catch (_) { /* non-JSON */ }
  return { status: res.status, data };
}

/**
 * 店舗内カテゴリ (お店の棚) のツリーを RMS から取得する (miniPC 経由、2026-08-02)。
 * 楽天ジャンルとは別物 = Category API 2.0。miniPC 側で 24h キャッシュ済み。
 * @returns {Promise<{ok: true, trees: Array}|{ok: false, error: string}>}
 */
export async function fetchShopCategoryTree({ force = false, fetcher = callWarehouse } = {}) {
  const r = await fetcher(
    `/service-api/rakuten-rms/shop-categories/tree${force ? '?refresh=1' : ''}`,
    { timeoutMs: 60_000 },
  );
  if (r.status !== 200) {
    const msg = r.data?.message || r.data?.error || `HTTP ${r.status}`;
    return { ok: false, error: String(msg).slice(0, 300) };
  }
  const trees = r.data?.trees ?? r.data?.data?.trees;
  if (!Array.isArray(trees)) return { ok: false, error: '応答に trees がありません (miniPC の更新が必要かもしれません)' };
  return { ok: true, trees };
}

// ─── Drive 画像ダウンロード (fba-replenishment/drive-upload.js と同じ SA) ───

function getDriveClient() {
  const keyBase64 = requireEnv('GOOGLE_SERVICE_ACCOUNT_KEY');
  const credentials = JSON.parse(Buffer.from(keyBase64, 'base64').toString('utf-8'));
  const auth = new google.auth.GoogleAuth({
    credentials,
    scopes: ['https://www.googleapis.com/auth/drive.readonly'],
  });
  return google.drive({ version: 'v3', auth });
}

async function downloadDriveImage(fileId) {
  const drive = getDriveClient();
  const res = await drive.files.get(
    { fileId, alt: 'media', supportsAllDrives: true },
    { responseType: 'arraybuffer' },
  );
  return Buffer.from(res.data);
}

/**
 * Drive フォルダ直下の画像ファイル一覧 (共有ドライブ対応・ページング対応)。
 * 1000件を超えるフォルダは商品画像フォルダではない可能性が高いので fail-closed
 * (Codex R2: 打ち切った不完全な一覧で既存画像を置換しないため)。
 * @returns {Promise<Array<{id: string, name: string, mimeType: string}>>}
 */
export async function listDriveFolderImages(folderId) {
  const drive = getDriveClient();
  const files = [];
  let pageToken;
  do {
    const res = await drive.files.list({
      q: `'${folderId}' in parents and trashed = false and mimeType contains 'image/'`,
      fields: 'nextPageToken, files(id, name, mimeType, modifiedTime)',
      pageSize: 200,
      orderBy: 'name',
      supportsAllDrives: true,
      includeItemsFromAllDrives: true,
      pageToken,
    });
    files.push(...(res.data.files || []));
    pageToken = res.data.nextPageToken;
    if (files.length > 1000) {
      throw new Error('フォルダ内の画像が1000件を超えています。商品の画像フォルダを指定しているか確認してください');
    }
  } while (pageToken);
  return files;
}

// ─── サムネイル取得 (アプリ内プロキシ /api/thumb 用) ───
// drive.google.com/thumbnail 直リンクは「閲覧者の Google セッション」をサードパーティ
// Cookie として送れる前提で、Cookie ブロックや複数アカウント (authuser 不一致) で 403 になる。
// SA で Drive の thumbnailLink (トークン付き短命URL・Cookie不要) を取り、その画像バイトを
// アプリから返す。thumbnailLink が取れない場合だけ原本DL+縮小にフォールバック。

const THUMB_CACHE_MAX = 300;
const THUMB_CACHE_MAX_BYTES = 30 * 1024 * 1024; // 件数と総バイトの両方で上限 (Codex R1 medium)
const THUMB_CACHE_TTL_MS = 24 * 60 * 60 * 1000;
const THUMB_CACHE_UNVERSIONED_TTL_MS = 2 * 60 * 1000; // 版数なし = 上書き検知不能なので短命
const THUMB_SOURCE_MAX_BYTES = 40 * 1024 * 1024; // これを超える取得元は商品画像ではないとみなす
const thumbCache = new Map(); // `${fileId}:${width}` → { buf, at } (Map 挿入順 = LRU)
let thumbCacheBytes = 0;
const thumbInflight = new Map(); // 同一キーの同時リクエストは 1 回の Drive アクセスに束ねる

// 異なる画像への同時リクエストも束ねる (詳細画面は最大21枚を一斉に要求してくる。
// 原本DLフォールバック時のメモリ/帯域スパイクを抑える。Codex R1 medium)
const THUMB_MAX_CONCURRENCY = 4;
let thumbActive = 0;
const thumbWaiters = [];
async function withThumbSlot(fn) {
  if (thumbActive >= THUMB_MAX_CONCURRENCY) await new Promise((resolve) => thumbWaiters.push(resolve));
  thumbActive++;
  try {
    return await fn();
  } finally {
    thumbActive--;
    thumbWaiters.shift()?.();
  }
}

function thumbCacheSet(key, entry) {
  const old = thumbCache.get(key);
  if (old) {
    thumbCacheBytes -= old.buf.length;
    thumbCache.delete(key);
  }
  thumbCache.set(key, entry);
  thumbCacheBytes += entry.buf.length;
  while (thumbCache.size > THUMB_CACHE_MAX || thumbCacheBytes > THUMB_CACHE_MAX_BYTES) {
    const oldest = thumbCache.keys().next().value;
    thumbCacheBytes -= thumbCache.get(oldest).buf.length;
    thumbCache.delete(oldest);
  }
}

/** thumbnailLink 末尾の =s220 をリクエスト幅に差し替える (pure・smoke対象) */
export function sizedThumbnailLink(link, width) {
  return /=s\d+$/.test(link) ? link.replace(/=s\d+$/, `=w${width}`) : link;
}

/** SSRF 防御: thumbnailLink は https の googleusercontent.com 配下のみ許可 (pure・smoke対象) */
export function isAllowedThumbnailHost(link) {
  try {
    const u = new URL(link);
    return u.protocol === 'https:'
      && (u.hostname === 'googleusercontent.com' || u.hostname.endsWith('.googleusercontent.com'));
  } catch (_) {
    return false;
  }
}

export async function getDriveThumbnail(fileId, width, version = '') {
  // version = Drive の更新日時。同じ fileId でも中身が差し替われば別キーになり、
  // 古いサムネを返し続けない (2026-08-08 スタッフ指摘)。
  // 版数が無い画像 (旧データ・個別追加) は上書きに追従できないので短命にする (Codex high)
  const key = `${fileId}:${width}:${version}`;
  const ttl = version ? THUMB_CACHE_TTL_MS : THUMB_CACHE_UNVERSIONED_TTL_MS;
  const hit = thumbCache.get(key);
  if (hit) {
    if (Date.now() - hit.at <= ttl) {
      thumbCache.delete(key);
      thumbCache.set(key, hit); // LRU 更新
      return hit;
    }
    thumbCacheBytes -= hit.buf.length;
    thumbCache.delete(key);
  }
  if (thumbInflight.has(key)) return thumbInflight.get(key);
  const p = withThumbSlot(() => fetchDriveThumbnail(fileId, width))
    .then((entry) => {
      thumbCacheSet(key, entry);
      return entry;
    })
    .finally(() => thumbInflight.delete(key));
  thumbInflight.set(key, p);
  return p;
}

/** fetch Response の body を上限付きで読む。超過したら中断して throw (全量バッファ後の検査にしない) */
async function readBodyLimited(res, maxBytes) {
  const reader = res.body.getReader();
  const chunks = [];
  let total = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    total += value.length;
    if (total > maxBytes) {
      await reader.cancel();
      throw new Error('サムネイル取得元のレスポンスが大きすぎます');
    }
    chunks.push(value);
  }
  return Buffer.concat(chunks);
}

/** Drive 原本を stream で上限付きダウンロード (超過時は途中で破棄) */
async function downloadDriveImageLimited(fileId, maxBytes) {
  const drive = getDriveClient();
  const res = await drive.files.get(
    { fileId, alt: 'media', supportsAllDrives: true },
    { responseType: 'stream' },
  );
  return await new Promise((resolve, reject) => {
    const chunks = [];
    let total = 0;
    res.data.on('data', (c) => {
      total += c.length;
      if (total > maxBytes) {
        res.data.destroy(new Error('サムネイル生成対象の画像が大きすぎます'));
        return;
      }
      chunks.push(c);
    });
    res.data.on('error', reject);
    res.data.on('end', () => resolve(Buffer.concat(chunks)));
  });
}

async function fetchDriveThumbnail(fileId, width) {
  const drive = getDriveClient();
  const meta = await drive.files.get({ fileId, fields: 'thumbnailLink', supportsAllDrives: true });
  const link = meta.data.thumbnailLink; // 例: https://lh3.googleusercontent.com/...=s220
  let raw = null;
  if (link && isAllowedThumbnailHost(link)) {
    try {
      // redirect: 'error' = 許可ホスト検証をリダイレクトで迂回させない (失敗したら原本DLへ)
      const res = await fetch(sizedThumbnailLink(link, width), {
        redirect: 'error',
        signal: AbortSignal.timeout(30_000),
      });
      const declared = Number(res.headers.get('content-length'));
      if (res.ok
        && (res.headers.get('content-type') || '').startsWith('image/')
        && !(Number.isFinite(declared) && declared > THUMB_SOURCE_MAX_BYTES)) {
        raw = await readBodyLimited(res, THUMB_SOURCE_MAX_BYTES);
      } else {
        // 拒否したレスポンスの転送を続けさせない (タイムアウトまで接続を保持しないよう明示中断)
        await res.body?.cancel();
      }
    } catch (_) {
      raw = null; // フォールバックへ
    }
  }
  if (!raw) {
    raw = await downloadDriveImageLimited(fileId, THUMB_SOURCE_MAX_BYTES);
  }
  // 取得元に関わらず sharp で再エンコード = 「本当に画像である」ことの検証 +
  // 出力を width 上限の JPEG に固定 (Content-Type 偽装をアプリ起点で配らない。Codex R1 medium)
  const buf = await sharp(raw).rotate()
    .resize({ width, height: width, fit: 'inside', withoutEnlargement: true })
    .jpeg({ quality: 80 })
    .toBuffer();
  return { buf, at: Date.now() };
}

/**
 * R-Cabinet 制約 (JPEG / 2MB / 3840px) に収める。
 * RYS image-uploader と同じ「品質を段階的に落とす」方式。
 */
export async function toCabinetJpeg(buf) {
  const MAX = 2 * 1024 * 1024;
  for (const { width, quality } of [
    { width: 1920, quality: 85 },
    { width: 1920, quality: 70 },
    { width: 1200, quality: 70 },
    { width: 800, quality: 60 },
  ]) {
    const out = await sharp(buf).rotate()
      .resize({ width, height: width, fit: 'inside', withoutEnlargement: true })
      .jpeg({ quality })
      .toBuffer();
    if (out.length <= MAX) return out;
  }
  throw new Error('画像を2MB以下に圧縮できませんでした');
}

// ─── R-Cabinet フォルダ (app 専用の1フォルダを使い回す) ───

// 2026-08-05 本番作成済み: /appnewitems (FolderId 13702390, 表示名「商品登録ハブ」)。
// ハイフン入り directoryName は RMS 実機で未検証のため避けた
const CABINET_DIR = 'appnewitems';

async function ensureCabinetFolder(db) {
  const cached = db.prepare(`SELECT value FROM ph_intake_state WHERE key = 'cabinet_folder_id'`).get()?.value;
  if (cached) return Number(cached);
  const r = await callWarehouse('/service-api/rakuten-rms/cabinet/folder-ensure', {
    method: 'POST',
    body: { directoryName: CABINET_DIR, folderName: '商品登録ハブ' },
  });
  const folderId = r.data?.folderId ?? r.data?.data?.folderId;
  if (r.status !== 200 || !folderId) {
    throw new Error(`R-Cabinet フォルダの用意に失敗: ${r.data?.message || r.data?.error || `HTTP ${r.status}`}`);
  }
  db.prepare(`INSERT INTO ph_intake_state (key, value) VALUES ('cabinet_folder_id', ?)
              ON CONFLICT(key) DO UPDATE SET value = excluded.value`).run(String(folderId));
  return Number(folderId);
}

/**
 * ドラフトの Drive 画像を R-Cabinet へ転送する (転送済みはスキップ = 冪等)。
 * 1件の失敗で全体を止めず、画像ごとに結果を返す。
 */
export async function transferImagesToCabinet(draftId, { actor = null } = {}) {
  const db = getDB();
  const draft = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(draftId);
  if (!draft) return { ok: false, error: 'draft_not_found' };
  const images = db.prepare('SELECT * FROM draft_images WHERE draft_id = ? ORDER BY sort, id').all(draftId);
  // 白抜き背景画像 (whiteBgImage) も同じ転送に載せる。
  // ファイル名を "-white" にして商品画像と区別する (payload 側は drive_file_id で判別)
  const rkRow = db.prepare('SELECT white_bg_drive_file_id, white_bg_modified_time FROM draft_rakuten WHERE draft_id = ?').get(draftId);
  const whiteBgId = rkRow?.white_bg_drive_file_id || null;
  if (whiteBgId) {
    images.push({ drive_file_id: whiteBgId, isWhiteBg: true, drive_modified_time: rkRow?.white_bg_modified_time || null });
  }
  if (images.length === 0) return { ok: false, error: 'no_images' };

  // Drive で上書きされていないか確認してから転送する (再取込を忘れても追従する)
  await refreshDriveModifiedTimes(draftId);
  const refreshed = db.prepare('SELECT drive_file_id, drive_modified_time FROM draft_images WHERE draft_id = ?').all(draftId);
  const mtimeNow = new Map(refreshed.map((r) => [r.drive_file_id, r.drive_modified_time]));
  const rkNow = db.prepare('SELECT white_bg_modified_time FROM draft_rakuten WHERE draft_id = ?').get(draftId);
  for (const img of images) {
    img.drive_modified_time = img.isWhiteBg
      ? (rkNow?.white_bg_modified_time || null)
      : (mtimeNow.has(img.drive_file_id) ? mtimeNow.get(img.drive_file_id) : img.drive_modified_time || null);
  }
  const folderId = await ensureCabinetFolder(db);
  const results = [];
  let n = 0;
  for (const img of images) {
    n += 1;
    const done = db.prepare(
      'SELECT cabinet_location, drive_modified_time FROM draft_cabinet_images WHERE draft_id = ? AND drive_file_id = ?',
    ).get(draftId, img.drive_file_id);
    // Drive で同じファイルを上書きした場合、ID は同じでも更新日時が変わる。
    // 「転送済み」で素通りさせると R-Cabinet だけ旧画像のまま楽天へ出てしまう (Codex high)
    const fresh = done && String(done.drive_modified_time || '') === String(img.drive_modified_time || '');
    if (fresh) {
      results.push({ driveFileId: img.drive_file_id, outcome: 'already', location: done.cabinet_location });
      continue;
    }
    try {
      // どの段で落ちたかを結果に残す (呼び出し側が「Drive の共有を疑え」の案内を出すか決める。
      // メッセージの文言で判定すると、Google が返す 'File not found' のような文に引っかからない
      // — Codex R1 medium)
      let raw;
      try {
        raw = await downloadDriveImage(img.drive_file_id);
      } catch (e) {
        throw Object.assign(new Error(String(e.message || e)), { source: 'drive' });
      }
      const jpeg = await toCabinetJpeg(raw);
      // ファイル名は「商品コード-連番」(白抜きは -white)。同名は overWrite=true で置き換わる (再転送に安全)。
      // 20文字を超える商品コードはハッシュ入りの短い名前になる (cabinetFilePath 参照)
      const filePath = cabinetFilePath(draft.ne_code, img.isWhiteBg ? 'white' : String(n));
      const up = await callWarehouse('/service-api/rakuten-rms/cabinet/upload', {
        method: 'POST',
        body: { folderId, filePath, fileName: img.isWhiteBg ? `${draft.ne_code} 白抜き` : `${draft.ne_code} ${n}`, fileBase64: jpeg.toString('base64') },
      });
      const location = up.data?.location ?? up.data?.data?.location;
      const fileId = up.data?.fileId ?? up.data?.data?.fileId;
      if (up.status !== 200 || !location) {
        throw new Error(up.data?.message || up.data?.error || `HTTP ${up.status}`);
      }
      db.prepare(`
        INSERT INTO draft_cabinet_images (draft_id, drive_file_id, cabinet_location, cabinet_file_id, drive_modified_time)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(draft_id, drive_file_id) DO UPDATE SET
          cabinet_location = excluded.cabinet_location, cabinet_file_id = excluded.cabinet_file_id,
          drive_modified_time = excluded.drive_modified_time,
          uploaded_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
      `).run(draftId, img.drive_file_id, location, fileId ?? null, img.drive_modified_time || null);
      results.push({ driveFileId: img.drive_file_id, outcome: 'uploaded', location });
    } catch (e) {
      // Drive の権限エラー (SA がフォルダに共有されていない等) と R-Cabinet 側の失敗を
      // source で区別して画像単位で報告する
      results.push({
        driveFileId: img.drive_file_id, outcome: 'failed',
        source: e?.source === 'drive' ? 'drive' : 'cabinet',
        error: String(e.message || e).slice(0, 300),
      });
    }
  }
  const uploaded = results.filter((r) => r.outcome === 'uploaded').length;
  const failed = results.filter((r) => r.outcome === 'failed').length;
  logEvent(db, draftId, 'cabinet_transfer', `uploaded=${uploaded} failed=${failed}`, actor);
  return { ok: failed === 0, uploaded, failed, results };
}

/**
 * 画像フォルダを引き直して draft_images / 白抜き / SKU画像の Drive 更新日時を最新化する。
 * これが無いと「Drive で上書きしたがフォルダ再取込はしていない」状態で日時が古いまま一致し、
 * 転送済み判定をすり抜けて**旧画像で出品**される (Codex R3 high)。
 * 転送・登録の直前に呼ぶ。フォルダ未設定や Drive エラーは fail-soft (呼び出し側の処理は続行)。
 */
export async function refreshDriveModifiedTimes(draftId) {
  const db = getDB();
  const draft = db.prepare('SELECT ne_code, drive_folder_url FROM product_drafts WHERE id = ?').get(draftId);
  const m = String(draft?.drive_folder_url || '').match(/\/folders\/([\w-]+)/);
  if (!m) return { ok: false, error: 'no_folder' };
  let files;
  try {
    files = await listDriveFolderImages(m[1]);
  } catch (e) {
    console.error('[product-hub] refreshDriveModifiedTimes failed:', String(e.message || e).slice(0, 200));
    return { ok: false, error: 'drive_unavailable' };
  }
  const mtimeById = new Map(files.map((f) => [f.id, f.modifiedTime || null]));
  let changed = 0;
  db.transaction(() => {
    for (const row of db.prepare('SELECT drive_file_id, drive_modified_time FROM draft_images WHERE draft_id = ?').all(draftId)) {
      if (!mtimeById.has(row.drive_file_id)) continue; // フォルダ外の個別追加は触らない
      const t = mtimeById.get(row.drive_file_id);
      if (String(t || '') === String(row.drive_modified_time || '')) continue;
      db.prepare('UPDATE draft_images SET drive_modified_time = ? WHERE draft_id = ? AND drive_file_id = ?').run(t, draftId, row.drive_file_id);
      changed += 1;
    }
    const rk = db.prepare('SELECT white_bg_drive_file_id, white_bg_modified_time FROM draft_rakuten WHERE draft_id = ?').get(draftId);
    if (rk?.white_bg_drive_file_id && mtimeById.has(rk.white_bg_drive_file_id)) {
      const t = mtimeById.get(rk.white_bg_drive_file_id);
      if (String(t || '') !== String(rk.white_bg_modified_time || '')) {
        db.prepare('UPDATE draft_rakuten SET white_bg_modified_time = ? WHERE draft_id = ?').run(t, draftId);
        changed += 1;
      }
    }
    for (const row of db.prepare('SELECT sku_code, drive_file_id, drive_modified_time FROM draft_sku_images WHERE draft_id = ?').all(draftId)) {
      if (!mtimeById.has(row.drive_file_id)) continue;
      const t = mtimeById.get(row.drive_file_id);
      if (String(t || '') === String(row.drive_modified_time || '')) continue;
      // SKU画像は転送状態を同じ行に持つ。更新日時だけ直すと cabinet_location が残り、
      // 「転送済み」で素通りして旧画像を楽天へ紐づけてしまう (Codex R4 high) → 同時にクリア
      db.prepare(`
        UPDATE draft_sku_images
        SET drive_modified_time = ?, cabinet_location = NULL, cabinet_file_id = NULL,
            uploaded_at = NULL, synced_at = NULL
        WHERE draft_id = ? AND sku_code = ?
      `).run(t, draftId, row.sku_code);
      changed += 1;
    }
  })();
  if (changed > 0) logEvent(db, draftId, 'drive_images_updated', `Driveで更新された画像 ${changed} 件を検出`, null);
  return { ok: true, changed };
}

/**
 * 転送履歴 (draft_cabinet_images) を「ファイルID + Drive更新日時」で引ける Map にする。
 * ID だけで突き合わせると、Drive で同じファイルを上書きした後も「転送済み」に見えてしまい、
 * R-Cabinet に残る**旧画像のまま出品**される (Codex R2 high)。
 */
export function freshCabinetMap(cabinetRows) {
  return new Map((cabinetRows || []).map((c) => [
    `${c.drive_file_id}|${c.drive_modified_time || ''}`, c.cabinet_location,
  ]));
}

/** freshCabinetMap のキー (画像行 → 転送履歴の照合キー) */
export function cabinetKeyOf(row) {
  return `${row.drive_file_id}|${row.drive_modified_time || ''}`;
}

// ─── SKU画像 (バリエーションページの SKU 選択時に出る画像。2026-08-07 中原さん指示) ───
// 運用: 画像フォルダに「SKUコード」名 (例 sueders-db.jpg) で保存されている。
// 流れ: フォルダから取り込み → R-Cabinet 転送 → 楽天の variants[sku].images へ PATCH。
// PATCH は per-SKU マージ (zz- テスト商品で実測 2026-08-07) なので、
// 手動登録済みのバリエーションページにも SKU画像だけ安全に後付けできる。

/** ファイル名から拡張子を落として SKU コード比較用に正規化 (pure・smoke対象) */
export function skuImageKeyOfFileName(name) {
  return String(name || '').trim().replace(/\.[A-Za-z0-9]{1,5}$/, '').trim().toLowerCase();
}

/**
 * R-Cabinet の filePath の上限 (拡張子 .jpg を除いた文字数)。
 *
 * 🚨 2026-09-04 に実測した楽天の実際の制限。1文字でも超えると file insert が
 * **resultCode 3001 (Input Parameter Error)** で拒否される:
 *   silicateclay800-whitx (21文字) → HTTP 400 / resultCode 3001
 *   silicateclay800-whit  (20文字) → HTTP 200 / resultCode 0
 * それまでコードは「31文字以内」を前提にしており、商品コードが 15 文字以上の商品は
 * 白抜き画像 (`<コード>-white`) が必ず転送できなかった (#1163)。
 */
export const CABINET_PATH_MAX = 20;

/** 先頭は英数字でなければならない (R-Cabinet の制約)。切り詰めの後始末も兼ねる */
function cabinetSafeHead(s, max) {
  const head = String(s).slice(0, Math.max(1, max)).replace(/-+$/, '');
  if (!head) return 'x';
  return /^[a-z0-9]/.test(head) ? head : `x${head}`.slice(0, Math.max(1, max));
}

/**
 * 商品画像 / 白抜き背景画像の R-Cabinet ファイルパス (pure・smoke対象)。
 *
 * **20文字に収まるときは従来どおりの名前**にする — 既に転送済みの商品のファイル名を
 * 変えると、R-Cabinet に同じ画像が二重にでき、出品済みページの画像URLとも食い違う。
 * 超えるときだけ、商品コードのハッシュを挟んだ短い名前にする。単純な切り詰めだと
 * 先頭が同じ別商品と衝突し、overWrite=true で**他商品の画像を静かに上書き**してしまう
 *
 * @param {string} neCode 商品コード (NE)
 * @param {string} suffix 'white' (白抜き) か '1'〜'20' (商品画像の枠番)
 */
export function cabinetFilePath(neCode, suffix) {
  const sfx = String(suffix);
  const safe = String(neCode).toLowerCase().replace(/[^a-z0-9\-]/g, '-').replace(/^-+/, '');
  const plain = `${safe}-${sfx}`;
  if (safe && /^[a-z0-9]/.test(plain) && plain.length <= CABINET_PATH_MAX) return `${plain}.jpg`;
  // ハッシュは 8 桁 (Codex R1 最優先): 20文字枠でも 8 桁は入る。桁を削ると同接頭辞の商品が
  // 増えたときの衝突確率が跳ね上がり、衝突は overWrite=true で他商品の画像を静かに壊す
  const tail = `-${crypto.createHash('sha1').update(String(neCode)).digest('hex').slice(0, 8)}-${sfx}`;
  return `${cabinetSafeHead(safe || 'x', CABINET_PATH_MAX - tail.length)}${tail}.jpg`;
}

/**
 * SKU画像の R-Cabinet ファイルパス (pure・smoke対象)。
 * 単純な英数置換だと a_b / a.b が同じ a-b になり別SKUの画像を上書きし得る (Codex High-3)
 * → 置換で情報が落ちる場合は元コードのハッシュ8桁を付けて一意にする。
 * cabinet/upload の制約 (先頭英数・計20文字以内 + .jpg) に収まるよう切り詰める
 */
export function skuCabinetFilePath(skuCode) {
  const sku = String(skuCode);
  const safe = sku.replace(/[^a-z0-9\-]/g, '-').replace(/^-+/, '');
  // 置換で情報が落ちる場合に加え、**切り詰めが必要な場合も**ハッシュを付ける
  // (Codex R2 High: 先頭が同じ長いSKU同士が切り詰めで衝突する)
  const lossless = safe === sku && safe.length <= CABINET_PATH_MAX - '-sku'.length;
  const tail = lossless ? '-sku' : `-${crypto.createHash('sha1').update(sku).digest('hex').slice(0, 8)}-sku`;
  return `${cabinetSafeHead(safe || 'x', CABINET_PATH_MAX - tail.length)}${tail}.jpg`;
}

/** Drive フォルダから「SKUコード」名のファイルを draft_sku_images に取り込む */
export async function importSkuImagesFromFolder(draftId, { folderUrlOverride = null } = {}) {
  const db = getDB();
  const draft = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(draftId);
  if (!draft) return { ok: false, error: 'ドラフトが見つかりません' };
  const vari = resolveVariationGroup(db, draft.ne_code, { draftId, withMembers: true });
  if (vari.kind !== 'variation' || vari.members.length === 0) {
    return { ok: false, error: 'バリエーション商品ではありません (SKU画像は不要です)' };
  }
  const folderUrl = folderUrlOverride || draft.drive_folder_url;
  if (!folderUrl) return { ok: false, error: '画像フォルダURLが未設定です (画像タブでフォルダを取り込んでください)' };
  const m = String(folderUrl).match(/\/folders\/([\w-]+)/);
  if (!m) return { ok: false, error: '画像フォルダURLがDriveのフォルダリンクではありません' };
  let files;
  try {
    files = await listDriveFolderImages(m[1]);
  } catch (e) {
    return { ok: false, error: `フォルダ一覧の取得に失敗しました (${String(e.message || e).slice(0, 200)})` };
  }
  const byKey = new Map(); // sku(lower) → files[]
  for (const f of files) {
    const key = skuImageKeyOfFileName(f.name);
    if (!byKey.has(key)) byKey.set(key, []);
    byKey.get(key).push(f);
  }
  const matched = [];
  const missing = [];
  const conflicts = [];
  for (const mem of vari.members) {
    const sku = String(mem.商品コード || '').trim().toLowerCase();
    if (!sku) continue;
    const hits = byKey.get(sku) || [];
    if (hits.length === 0) { missing.push(sku); continue; }
    if (hits.length > 1) { conflicts.push({ sku, names: hits.map((h) => h.name) }); continue; }
    matched.push({ sku, file: hits[0] });
  }
  if (conflicts.length > 0) {
    return { ok: false, error: '同じSKUコード名のファイルが複数あります。フォルダを整理してからやり直してください', conflicts };
  }
  // 取り込みは「フォルダのスナップショット」として1トランザクションで反映する:
  // 今回マッチしなかった既存行は削除 (Codex High-1: フォルダから消した画像が
  // DB に残り、古い画像を楽天へ再同期できてしまう)
  db.transaction(() => {
    const keep = matched.map((x) => x.sku);
    const del = db.prepare(
      `DELETE FROM draft_sku_images WHERE draft_id = ? AND sku_code NOT IN (${keep.map(() => '?').join(',') || "''"})`,
    ).run(draftId, ...keep);
    for (const { sku, file } of matched) {
      // ファイルが変わっていたら転送・紐づけ状態をリセット (古い画像のまま「済み」に見せない)
      db.prepare(`
        INSERT INTO draft_sku_images (draft_id, sku_code, drive_file_id, file_name, drive_modified_time)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(draft_id, sku_code) DO UPDATE SET
          drive_file_id = excluded.drive_file_id,
          file_name = excluded.file_name,
          drive_modified_time = excluded.drive_modified_time,
          -- ファイルIDだけでなく**更新日時**も同じときにだけ転送/紐づけ状態を引き継ぐ
          -- (同じIDへ上書きされた場合に古い Cabinet 画像を使い続けない。Codex high)
          cabinet_location = CASE WHEN draft_sku_images.drive_file_id = excluded.drive_file_id AND IFNULL(draft_sku_images.drive_modified_time,'') = IFNULL(excluded.drive_modified_time,'') THEN draft_sku_images.cabinet_location ELSE NULL END,
          cabinet_file_id  = CASE WHEN draft_sku_images.drive_file_id = excluded.drive_file_id AND IFNULL(draft_sku_images.drive_modified_time,'') = IFNULL(excluded.drive_modified_time,'') THEN draft_sku_images.cabinet_file_id ELSE NULL END,
          uploaded_at      = CASE WHEN draft_sku_images.drive_file_id = excluded.drive_file_id AND IFNULL(draft_sku_images.drive_modified_time,'') = IFNULL(excluded.drive_modified_time,'') THEN draft_sku_images.uploaded_at ELSE NULL END,
          synced_at        = CASE WHEN draft_sku_images.drive_file_id = excluded.drive_file_id AND IFNULL(draft_sku_images.drive_modified_time,'') = IFNULL(excluded.drive_modified_time,'') THEN draft_sku_images.synced_at ELSE NULL END
      `).run(draftId, sku, file.id, file.name, file.modifiedTime || null);
    }
    if (del.changes > 0) logEvent(db, draftId, 'sku_images_removed', `removed=${del.changes} (フォルダに無いSKU)`, null);
  })();
  logEvent(db, draftId, 'sku_images_imported', `matched=${matched.length} missing=${missing.length}`, null);
  return { ok: true, matched: matched.map((x) => ({ sku: x.sku, name: x.file.name })), missing };
}

/** 取り込み済み SKU画像を R-Cabinet へ転送する (転送済みはスキップ = 冪等) */
export async function transferSkuImagesToCabinet(draftId, { actor = null } = {}) {
  const db = getDB();
  const draft = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(draftId);
  if (!draft) return { ok: false, error: 'draft_not_found' };
  // Drive で上書きされていれば転送状態がクリアされ、下の「転送済みスキップ」を通り抜ける
  await refreshDriveModifiedTimes(draftId);
  const rows = db.prepare('SELECT * FROM draft_sku_images WHERE draft_id = ? ORDER BY sku_code').all(draftId);
  if (rows.length === 0) return { ok: false, error: 'SKU画像が未取り込みです (先に「SKU画像を取り込む」)' };
  const folderId = await ensureCabinetFolder(db);
  const results = [];
  for (const row of rows) {
    if (row.cabinet_location) {
      results.push({ sku: row.sku_code, outcome: 'already', location: row.cabinet_location });
      continue;
    }
    try {
      const raw = await downloadDriveImage(row.drive_file_id);
      const jpeg = await toCabinetJpeg(raw);
      // 商品画像 (<代表>-N.jpg) と衝突しないよう "-sku" を付ける。同名は overWrite で置換 (再転送に安全)
      const up = await callWarehouse('/service-api/rakuten-rms/cabinet/upload', {
        method: 'POST',
        body: { folderId, filePath: skuCabinetFilePath(row.sku_code), fileName: `${row.sku_code} SKU画像`, fileBase64: jpeg.toString('base64') },
      });
      const location = up.data?.location ?? up.data?.data?.location;
      const fileId = up.data?.fileId ?? up.data?.data?.fileId;
      if (up.status !== 200 || !location) throw new Error(up.data?.message || up.data?.error || `HTTP ${up.status}`);
      // 読み出した drive_file_id **と更新日時**を条件に含める (Codex: 転送中に再取り込みや
      // Drive 上書きが起きていたら 0 行更新 = 旧画像の結果を新しい行へ書き戻さない)
      const upd = db.prepare(`
        UPDATE draft_sku_images SET cabinet_location = ?, cabinet_file_id = ?,
          uploaded_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
        WHERE draft_id = ? AND sku_code = ? AND drive_file_id = ?
          AND IFNULL(drive_modified_time,'') = IFNULL(?,'')
      `).run(location, fileId ?? null, draftId, row.sku_code, row.drive_file_id, row.drive_modified_time ?? null);
      if (upd.changes === 0) {
        results.push({ sku: row.sku_code, outcome: 'failed', error: '転送中に画像が差し替わりました。もう一度「R-Cabinetへ転送」を実行してください' });
      } else {
        results.push({ sku: row.sku_code, outcome: 'uploaded', location });
      }
    } catch (e) {
      results.push({ sku: row.sku_code, outcome: 'failed', error: String(e.message || e).slice(0, 300) });
    }
  }
  const uploaded = results.filter((r) => r.outcome === 'uploaded').length;
  const failed = results.filter((r) => r.outcome === 'failed').length;
  logEvent(db, draftId, 'sku_images_transferred', `uploaded=${uploaded} failed=${failed}`, actor);
  return { ok: failed === 0, uploaded, failed, results };
}

/**
 * 転送済み SKU画像を楽天の variants[sku].images へ紐づける (PATCH)。
 * 手動登録ページ対応のため、まず GET で実在の SKU キーを取り、
 * 大文字小文字を RMS 側の表記に合わせてから一致した SKU だけ送る。
 */
export async function syncSkuImagesToRms(draftId, { actor = null } = {}) {
  const db = getDB();
  const draft = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(draftId);
  if (!draft) return { ok: false, error: 'draft_not_found' };
  // 上書きされた画像を楽天へ紐づけないよう、ここでも Drive の最新状態を反映してから引く
  await refreshDriveModifiedTimes(draftId);
  const rows = db.prepare(`SELECT * FROM draft_sku_images WHERE draft_id = ? AND cabinet_location IS NOT NULL ORDER BY sku_code`).all(draftId);
  if (rows.length === 0) return { ok: false, error: '転送済みのSKU画像がありません (先に「R-Cabinetへ転送」)' };
  const mn = String(draft.ne_code).trim().toLowerCase();
  const got = await callWarehouse(`/service-api/rakuten-rms/items/manage-numbers/${encodeURIComponent(mn)}`);
  if (got.status === 404) {
    return { ok: false, error: `楽天に ${mn} のページが見つかりません (先にバリエーションページを登録してください)` };
  }
  if (got.status !== 200) {
    return { ok: false, error: `楽天ページの取得に失敗しました (HTTP ${got.status})` };
  }
  const rmsKeys = Object.keys(got.data?.variants || {});
  const rmsByLower = new Map(rmsKeys.map((k) => [String(k).trim().toLowerCase(), k]));
  const variants = Object.create(null);
  const sent = []; // { sku, location } — synced_at の付与条件に使う
  const unmatched = [];
  for (const row of rows) {
    const rmsKey = rmsByLower.get(row.sku_code);
    if (!rmsKey) { unmatched.push(row.sku_code); continue; }
    variants[rmsKey] = { images: [{ type: 'CABINET', location: row.cabinet_location }] };
    sent.push({ sku: row.sku_code, location: row.cabinet_location });
  }
  if (sent.length === 0) {
    return { ok: false, error: `楽天ページのSKUと一致しません (ページ側: ${rmsKeys.slice(0, 10).join(', ')})`, unmatched };
  }
  const r = await callWarehouse(`/service-api/rakuten-rms/items/manage-numbers/${encodeURIComponent(mn)}/sku-images`, {
    method: 'PATCH', body: { variants },
  });
  if (r.status < 200 || r.status >= 300) {
    const msg = r.data?.message || r.data?.error || `HTTP ${r.status}`;
    logEvent(db, draftId, 'sku_images_sync_failed', String(msg).slice(0, 300), actor);
    return { ok: false, error: String(msg).slice(0, 300) };
  }
  // 送った location と行が一致するときだけ synced 扱い (Codex R2 High: PATCH 中に
  // 再取込で画像が差し替わった行へ synced_at を付けない。0行更新なら未同期のまま =
  // 画面は「未紐づけ」に戻り、人がもう一度紐づけて新画像で上書きできる)
  const stamp = db.prepare(`
    UPDATE draft_sku_images SET synced_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
    WHERE draft_id = ? AND sku_code = ? AND cabinet_location = ?
  `);
  const synced = [];
  const staleAfterPatch = [];
  for (const s of sent) {
    if (stamp.run(draftId, s.sku, s.location).changes > 0) synced.push(s.sku);
    else staleAfterPatch.push(s.sku);
  }
  logEvent(db, draftId, 'sku_images_synced', `synced=${synced.length} unmatched=${unmatched.length}${staleAfterPatch.length ? ` stale=${staleAfterPatch.length}` : ''}`, actor);
  return { ok: true, synced, unmatched, staleAfterPatch };
}

// ─── 出品 payload ───

/**
 * メーカー型番のジャンル属性名 (2026-08-31 中原さん指摘)。
 * RMS の入力項目は 1 つなのに、このアプリは「メーカー型番」欄と商品属性の 2 箇所に
 * 入れさせていた。入口は **article_number (メーカー型番欄) だけ** に統一し、
 * ジャンル属性に この名前があるジャンルでは送信時に自動で属性へも積む
 * (カタログID を JAN 欄から自動付与しているのと同じ方式)。
 * 画面はこの名前の属性行を出さない・保存時に落とす。
 */
export const MODEL_ATTR_NAME = 'メーカー型番';

/** attributes_json をパースして RMS 形式に整える。壊れた JSON は null */
export function parseAttributes(json) {
  if (!json || !String(json).trim()) return [];
  let v;
  try { v = JSON.parse(json); } catch (_) { return null; }
  if (!Array.isArray(v)) return null;
  const out = [];
  for (const a of v) {
    const name = a && typeof a.name === 'string' ? a.name.trim() : '';
    let values = Array.isArray(a?.values) ? a.values : (a?.value != null ? [a.value] : []);
    values = values.map((x) => String(x).trim()).filter(Boolean);
    if (!name || values.length === 0) return null;
    out.push({ name, values });
  }
  return out;
}

/** 楽天の商品ページは画像20枚まで */
// ─── ジャンル属性辞書 (Genre API、2026-07-28 実証) ───
// endpoint: miniPC GET /genres/:id/attributes → RMS /es/2.0/navigation/genres/{id}/attributes
// ⚠️ SELECTIVE 属性の選択肢一覧は API では取れない (自由入力 + RMS 検証に任せる)

/** RMS の genre attributes 応答をアプリ用に正規化する (pure、テスト可能) */
export function normalizeGenreAttributes(rmsData) {
  const g = rmsData?.genre;
  if (!g || !Number.isFinite(Number(g.genreId))) return null;
  const attributes = (Array.isArray(g.attributes) ? g.attributes : []).map((a) => {
    const p = a?.properties || {};
    return {
      name: String(a?.nameJa || '').trim(),
      dataType: a?.dataType || 'STRING',
      mandatory: p.rmsMandatoryFlg === true,
      mandatoryType: p.rmsMandatoryType || null, // MANDATORY / OPTIONAL_NAVIGATION / OPTIONAL_ITEM_PAGE
      multiValueLimit: Number.isFinite(p.rmsMultiValueLimit) ? p.rmsMultiValueLimit : null,
      inputMethod: p.rmsInputMethod || null,     // DESCRIPTIVE / SELECTIVE
      maxLength: Number.isFinite(a?.maxLength) ? a.maxLength : null,
      unit: a?.unit || null,
    };
  }).filter((a) => a.name);
  return {
    genreId: String(g.genreId),
    genreName: g.nameJa || null,
    genrePath: Array.isArray(g.nameJaPath) ? g.nameJaPath.join(' > ') : null,
    itemRegisterable: g.properties?.itemRegisterFlg !== false, // 末端ジャンルのみ登録可
    fixedAt: rmsData?.version?.fixedAt || null,
    attributes,
  };
}

const GENRE_CACHE_TTL_MS = 24 * 60 * 60 * 1000;

/**
 * キャッシュ済み辞書。maxAgeMs を指定すると鮮度切れは null (= 辞書なし扱い)。
 * ⚠️ buildItemPayload は必ず鮮度付きで呼ぶこと (Codex R1 High-1: 古い辞書を根拠に
 * 正しい属性を IE1002 扱いで弾いたり、廃止されたカタログIDを付与したりする)。
 */
export function getCachedGenreAttributes(db, genreId, { maxAgeMs = null } = {}) {
  const id = String(genreId ?? '').trim();
  if (!id) return null;
  const row = db.prepare('SELECT * FROM ph_genre_attributes WHERE genre_id = ?').get(id);
  if (!row) return null;
  if (maxAgeMs != null) {
    const age = Date.now() - Date.parse(row.fetched_at || 0);
    if (!Number.isFinite(age) || age > maxAgeMs) return null;
  }
  let attributes;
  try { attributes = JSON.parse(row.payload_json); } catch (_) { return null; }
  if (!Array.isArray(attributes)) return null;
  return {
    genreId: row.genre_id, genreName: row.genre_name, genrePath: row.genre_path,
    fixedAt: row.fixed_at, fetchedAt: row.fetched_at, attributes,
  };
}

/**
 * 辞書を取得してキャッシュする (24h 以内のキャッシュがあればそれを返す。force で強制再取得)。
 * @returns {{ok:true, genre}|{ok:false, notFound?:true, error?:string}}
 */
export async function fetchGenreAttributes(db, genreId, { force = false, fetcher = callWarehouse, timeoutMs = 120_000 } = {}) {
  const id = String(genreId ?? '').trim();
  if (!/^\d{1,12}$/.test(id)) return { ok: false, error: 'ジャンルIDは数字で指定してください' };

  if (!force) {
    const cached = getCachedGenreAttributes(db, id, { maxAgeMs: GENRE_CACHE_TTL_MS });
    if (cached) return { ok: true, genre: cached, cached: true };
  }

  const r = await fetcher(`/service-api/rakuten-rms/genres/${id}/attributes${force ? '?refresh=1' : ''}`, { timeoutMs });
  if (r.status === 404) {
    // ジャンル廃止・非末端化。古い成功キャッシュを残すと「画面では見つからないのに
    // 登録時は古い辞書で判定する」矛盾になる (Codex R1 High-2) → 行ごと消す
    db.prepare('DELETE FROM ph_genre_attributes WHERE genre_id = ?').run(id);
    return { ok: false, notFound: true };
  }
  if (r.status !== 200) {
    return { ok: false, error: r.data?.message || r.data?.error || `HTTP ${r.status}` };
  }
  const norm = normalizeGenreAttributes(r.data);
  if (!norm) return { ok: false, error: 'ジャンル情報を解釈できませんでした' };
  db.prepare(`
    INSERT INTO ph_genre_attributes (genre_id, genre_name, genre_path, payload_json, fixed_at, fetched_at)
    VALUES (?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    ON CONFLICT(genre_id) DO UPDATE SET
      genre_name = excluded.genre_name, genre_path = excluded.genre_path,
      payload_json = excluded.payload_json, fixed_at = excluded.fixed_at,
      fetched_at = excluded.fetched_at
  `).run(norm.genreId, norm.genreName, norm.genrePath, JSON.stringify(norm.attributes), norm.fixedAt);
  return { ok: true, genre: { ...norm, fetchedAt: new Date().toISOString() }, cached: false };
}

export const MAX_RAKUTEN_IMAGES = 20;

/**
 * draft_yahoo.tax_rate ('8%' / '10%' の文字列) → RMS payment.taxRate。
 * 2026-08-05 平串の実登録で「送らない = 店舗デフォルト」のはずが RMS の消費税率が
 * 未設定のままだった (中原さん指摘) → **常に明示して送る**。
 * 8% は 0.08、それ以外 (10%・空欄) は 0.1。不正値は buildItemPayload の検証で事前に止まる
 */
export function taxRateToPayment(taxRateText) {
  const m = String(taxRateText || '').trim().match(/^(\d+)\s*%?$/);
  if (m && Number(m[1]) === 8) return { taxRate: 0.08 };
  return { taxRate: 0.1 };
}

/**
 * GTIN (JAN-8 / UPC-12 / JAN-13) の桁数 + チェックデジット検証。
 * 右端 (チェックデジット除く) から 3,1,3,1… の重みで合計し、(10 - sum%10)%10 が末尾と一致すること。
 */
export function isValidGtin(code) {
  const s = String(code || '').trim();
  if (!/^(\d{8}|\d{12}|\d{13})$/.test(s)) return false;
  const digits = s.split('').map(Number);
  const check = digits.pop();
  let sum = 0;
  digits.reverse().forEach((d, i) => { sum += d * (i % 2 === 0 ? 3 : 1); });
  return (10 - (sum % 10)) % 10 === check;
}

/**
 * R-Cabinet の location (/dir/file.jpg) → 店舗の公開画像URLのベース。
 * 例の実URL: https://image.rakuten.co.jp/b-faith/cabinet/image3/.../xxx_01.jpg
 */
const CABINET_IMAGE_BASE = 'https://image.rakuten.co.jp/b-faith/cabinet';

function escAttr(v) {
  return String(v).replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
}

/**
 * PC用販売説明文: 商品画像を width100% で縦に並べる画像HTML (店舗の従来運用。
 * 旧運用ではスプレッドシートで生成して手貼りしていたもの)。
 */
export function buildSalesDescriptionHtml(locations) {
  // env が https URL でなければ既定に戻す (Codex R1 Medium: 属性注入の fail-safe)。
  // src には結合後のURL全体をエスケープして入れる
  let base = (process.env.PH_CABINET_IMAGE_BASE || CABINET_IMAGE_BASE).replace(/\/+$/, '');
  if (!/^https:\/\/[\w.-]+(?:\/[\w.\-\/]*)?$/.test(base)) base = CABINET_IMAGE_BASE;
  return locations
    .map((loc) => `<img src="${escAttr(`${base}${loc}`)}" width="100%" border="0"><br><br><br>`)
    .join('\n');
}

/** RMS の各説明文の文字数上限 (超過は登録エラー。最終判定は RMS) */
export const DESC_LIMIT = 10240;

// ─── 出品時に商品画像の末尾へ自動追加する店舗共通バナー (2026-08-03 中原さん指定) ───
// いずれも R-Cabinet に既存の店舗共通画像 (location = image.rakuten.co.jp/b-faith/cabinet 以下)。
// Drive には無いので draft_images/転送とは独立に、payload 組み立て時だけ付け足す
// (配送方法を後から変えても再取込なしで追従させるため DB には持たない)
export const SHIPPING_BANNER_LOCATIONS = {
  '1': '/07722747/08581403/teikeigai_soryomuryo.jpg', // 定形外 送料無料
  '5': '/07722747/09610094/imgrc0104897185.jpg',      // ネコポス
  '9': '/07722747/09610098/rakutensouko.jpg',         // ゆうパケットパフ (楽天倉庫)
};
export const COMMON_TRAILING_BANNERS = [
  { location: '/coupon/imgrc0122590661.jpg', label: 'クーポン' },
  { location: '/11720388/same_day.jpg', label: '当日出荷' },
  { location: '/11720388/refund.jpg', label: '返金保証' },
];

/**
 * 配送方法の解決: アプリ指定グループ > NE配送方法のマッピング。
 * 商品ページ表記の発送方法表示と末尾バナーの選択で共通に使う (router のプレビューも同じ関数)
 */
export function effectiveShippingForDraft(db, neCode, shippingMethodGroup) {
  // 値の解釈は toRakutenShippingGroup ただ1つ。DB には楽天グループIDしか入らないが、
  // 旧データ (分解前の複合キー) が残っていても同じ結果になるようここを通す
  const r = toRakutenShippingGroup(shippingMethodGroup);
  // 不正な明示指定は NE へフォールバックして隠さない (buildItemPayload 側は理由で停止する。
  // プレビューも「配送バナーなし」に揃え、直すべき状態が見えるようにする — Codex R2)
  if (!r.ok) return { group: null, label: null, invalid: true };
  if (r.group) {
    return {
      group: r.group,
      label: ALL_SHIPPING_METHOD_GROUPS[r.group],   // 選択肢から外した値でも名前は出す
      ...(r.yahooOverride ? { yahooDelivery: r.yahooOverride.yahooDelivery } : {}),
    };
  }
  return mapNeShippingToRakuten(db, getNeCost(db, neCode)?.shippingMethod);
}

/**
 * RMS へ送る配送方法グループ (`variants[].shipping.shippingMethodGroup`)。
 *
 * 🚨 **セットだけ**が NE の配送方法へフォールバックする (2026-09-04 §4.4 決⑥)。
 *   - セット: `shipping_method_group` を親からコピーしない設計なので、ここで NE に落とさないと
 *     画面と末尾バナーは NE の「ネコポス」を出しているのに RMS へは何も送られず**店舗デフォルト**になる。
 *     決⑥ の「本コード確定後は自動で NE の値になる」が成り立たない
 *   - 単品: **従来どおり**「アプリ未指定なら送らない = 店舗デフォルトに任せる」。
 *     単品まで NE に落とすと、いま店舗デフォルトで出ている商品の配送方法が黙って変わる。
 *     それは決⑥の範囲外の運用変更なので、中原さんの判断を待つ (Codex high 2026-09-04 2巡目)
 *
 * @param {{parent_draft_id: number|null}} draft
 * @param {{group: string|null}} effective  effectiveShippingForDraft の結果 (アプリ指定 > NE)
 * @param {{group: string|null}} resolved   toRakutenShippingGroup の結果 (アプリ指定だけ)
 */
export function payloadShippingGroup(draft, effective, resolved) {
  const isSet = draft?.parent_draft_id != null;
  return (isSet ? effective?.group : resolved?.group) || '';
}

/** 配送方法グループ → 末尾に自動追加するバナー location 一覧 (配送バナー + 共通3枚) */
export function trailingBannerLocations(shippingGroup) {
  const g = String(shippingGroup ?? '').trim();
  return [
    ...(SHIPPING_BANNER_LOCATIONS[g] ? [SHIPPING_BANNER_LOCATIONS[g]] : []),
    ...COMMON_TRAILING_BANNERS.map((b) => b.location),
  ];
}

/**
 * 説明文3欄の組み立て (§10 店舗フォーマット)。buildItemPayload と 3欄プレビューの共通部。
 * 楽天の入力欄と1:1対応: pc=PC用商品説明文 / sales=PC用販売説明文 / sp=スマートフォン用商品説明文
 */
export function composeDescriptions({ productName, ai, specs, pageInfo, cabinetLocations }) {
  // 「説明」行 = AI特徴 + AI仕様 (仕様表・注意書きは表の別行に載る)。
  // **楽天タイトルは入れない** (2026-08-31 中原さん): タイトルは検索用に語を並べたもので、
  // 説明として読ませる文ではない。表の先頭に丸ごと出ると SEO 語の羅列がそのまま載る
  const descTexts = [];
  if (ai.desc_features) descTexts.push(String(ai.desc_features).trim());
  if (ai.desc_spec) descTexts.push(String(ai.desc_spec).trim());
  const pc = buildPageInfoHtml({
    // AI 文が 1 つも無いときだけ「商品名」行として使われる。ここは NE の商品名を渡す
    // (楽天タイトルを渡すと、上で外したはずの SEO 語がフォールバックで出てしまう)
    productName,
    info: pageInfo, // 未保存 (null) でも説明/注意事項/仕様表/広告文責の行は載る
    descriptionText: descTexts.join('\n\n'),
    notesText: ai.desc_notes ? String(ai.desc_notes).trim() : null,
    specs,
  });
  const sales = buildSalesDescriptionHtml(cabinetLocations);
  const sp = [sales, pc].filter(Boolean).join('\n');
  return { pc, sales, sp };
}

/**
 * 楽天の説明文3欄のプレビュー (2026-08-03 中原さん要望「楽天と同じ項目をアプリにも」)。
 * buildItemPayload と違い、登録要件 (ジャンル・画像転送など) が未達でも現時点の素材で組み立てて返す。
 */
export function buildDescriptionPreview(db, draftId) {
  const draft = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(draftId);
  if (!draft) return { ok: false, error: 'draft_not_found' };
  const rk = db.prepare('SELECT * FROM draft_rakuten WHERE draft_id = ?').get(draftId) || {};
  const ai = {};
  for (const r of db.prepare('SELECT kind, content FROM draft_ai_outputs WHERE draft_id = ?').all(draftId)) {
    ai[r.kind] = r.content;
  }
  const specs = db.prepare('SELECT spec_key, spec_value FROM draft_specs WHERE draft_id = ? ORDER BY sort, id').all(draftId);
  const pageInfo = db.prepare('SELECT * FROM draft_page_info WHERE draft_id = ?').get(draftId) || null;
  // 転送済みの商品画像 (白抜き除く・バナーは §15 どおり salesDescription に入れない) — buildItemPayload と同じ突合
  const cabinetAll = db.prepare('SELECT drive_file_id, cabinet_location, drive_modified_time FROM draft_cabinet_images WHERE draft_id = ? ORDER BY id').all(draftId);
  const byFile = freshCabinetMap(cabinetAll);
  const whiteBgId = rk.white_bg_drive_file_id || null;
  const current = db.prepare('SELECT drive_file_id, drive_modified_time FROM draft_images WHERE draft_id = ? ORDER BY sort, id')
    .all(draftId).filter((i) => i.drive_file_id !== whiteBgId);
  const locations = current.filter((i) => byFile.has(cabinetKeyOf(i))).map((i) => byFile.get(cabinetKeyOf(i)));
  const d = composeDescriptions({
    productName: draft.name, ai, specs, pageInfo, cabinetLocations: locations,
  });
  return {
    ok: true, ...d,
    imageCount: current.length, transferredCount: locations.length,
    limit: DESC_LIMIT,
  };
}

/** 楽天の商品ページURL (公開後のリンク表示用。店舗slugは cabinet と同じ b-faith) */
export function rakutenItemPageUrl(manageNumber) {
  const shop = (process.env.PH_RAKUTEN_SHOP_SLUG || 'b-faith').trim();
  return `https://item.rakuten.co.jp/${encodeURIComponent(shop)}/${encodeURIComponent(String(manageNumber).trim().toLowerCase())}/`;
}

/**
 * 他社の商品ページURL → 楽天ジャンルID (2026-09-04)。
 *
 * 🚨 取得は **miniPC が代理で行う**。Render から同じ URL を取ると読み取れなかった
 * (miniPC = 日本のIP からは同じ実装で必ず取れる)。楽天への口を miniPC に集約する方針とも合う。
 * URL の検証・SSRF 対策・ページからの抽出は lib/rakuten-item-page.js (miniPC 側で動く)。
 *
 * @returns {{ok:true, genreId, shopCode, itemCode}|{ok:false, error}}
 */
export async function genreIdFromItemUrl(url, { fetcher = callWarehouse } = {}) {
  // 明らかに違う入力は miniPC を呼ばずに断る (往復を無駄にしない。判定は共用モジュール)
  const parsed = parseRakutenItemUrl(url);
  if (!parsed) {
    return { ok: false, error: '楽天の商品ページURL (https://item.rakuten.co.jp/店舗コード/商品管理番号/) を貼ってください' };
  }
  let r;
  try {
    // miniPC 側は取得を直列化する (実行中1 + 待ち1・2秒間隔・15秒で打ち切り) ので
    // 最悪 2*(15+2) = 34秒。それより短く切ると、待たされた要求が必ず失敗する
    r = await fetcher('/service-api/rakuten-rms/item-page/genre', {
      method: 'POST', body: { url: parsed.url }, timeoutMs: 40_000,
    });
  } catch (e) {
    return { ok: false, error: `商品ページを調べられませんでした (${String(e.message || e).slice(0, 120)})` };
  }
  const genreId = r.data?.genreId ?? r.data?.data?.genreId;
  if (r.status === 200 && genreId) {
    return { ok: true, genreId: String(genreId), shopCode: parsed.shopCode, itemCode: parsed.itemCode, cached: r.data?.cached === true };
  }
  // miniPC は理由を 400 で返す (Cloudflare が本文を差し替えないステータス)。
  // 本文が取れないときは状況が分かるように HTTP を出す
  return { ok: false, error: r.data?.message || r.data?.error || `商品ページを調べられませんでした (HTTP ${r.status})` };
}

/**
 * RMS の商品編集ページURL (2026-09-04 中原さんから実URLを受領)。
 *   https://item.rms.rakuten.co.jp/rms-sku/shops/373343/item/edit/silicateclay800
 * セッションやトークンは含まれず、**店舗ID + 商品管理番号だけ**で開ける。
 * 商品管理番号は RMS 仕様で小文字 (registerItem も toLowerCase して送っている)。
 * 店舗ID (373343) は env PH_RAKUTEN_SHOP_ID で上書きできる
 */
export function rakutenRmsItemUrl(manageNumber) {
  const code = String(manageNumber ?? '').trim().toLowerCase();
  if (!code) return null;
  // env は「未設定なら既定値」。空文字や空白を既定値へ倒すと、設定ミスに気づけない
  const raw = process.env.PH_RAKUTEN_SHOP_ID;
  const shopId = (raw === undefined ? '373343' : String(raw)).trim();
  if (!/^\d+$/.test(shopId)) return null;   // 設定ミスで壊れたURLを出さない
  return `https://item.rms.rakuten.co.jp/rms-sku/shops/${shopId}/item/edit/${encodeURIComponent(code)}`;
}

/** UI プレビュー用: location → 公開画像URL (buildSalesDescriptionHtml と同じベース解決) */
export function cabinetImageUrl(location) {
  let base = (process.env.PH_CABINET_IMAGE_BASE || CABINET_IMAGE_BASE).replace(/\/+$/, '');
  if (!/^https:\/\/[\w.-]+(?:\/[\w.\-\/]*)?$/.test(base)) base = CABINET_IMAGE_BASE;
  return `${base}${location}`;
}

/**
 * SKU ごとの商品仕様 (2026-09-03 中原さん: RMS と同じ「SKU 列 × 項目行」の表で入力)。
 * ページ共通の attributes_json / article_number を既定値に、draft_sku_attributes (value '' = 明示的に空) で
 * SKU ごとに上書きする。画面 (detail) と出品 payload の両方がこの関数を使う = 見えている値がそのまま送られる。
 * @returns {{ names: string[], bySku: Map<string, Map<string, string>> }} bySku のキーは LOWER(TRIM(商品コード))
 */
export function splitAttributeValues(raw) {
  // RMS の商品仕様と同じく「|」区切りで複数値 (Codex R1 medium: 共通の多値属性を 1 文字列に潰さない)
  return String(raw == null ? '' : raw).split('|').map((v) => v.trim()).filter(Boolean);
}

export function skuAttributeGrid(db, draftId, rk, members) {
  const base = new Map();           // name → string[] (ページ共通の既定値)
  const legacyModels = [];          // 旧データで属性側に残っているメーカー型番
  const legacyCatalogIds = [];      // 旧データで属性側に残っているカタログID (SKU 表では JAN の専用行が入口なので展開しない)
  for (const a of (parseAttributes(rk?.attributes_json) || [])) {
    if (a.name === MODEL_ATTR_NAME) { for (const v of a.values) if (!legacyModels.includes(v)) legacyModels.push(v); continue; }
    if (a.name === 'カタログID') { for (const v of a.values) if (!legacyCatalogIds.includes(v)) legacyCatalogIds.push(v); continue; }
    base.set(a.name, a.values.slice());
  }
  // メーカー型番の既定値: 欄 (article_number) > 属性側に 1 つだけ残っている旧値。食い違い (複数・欄と別) は
  // 既定値にせず空の行にして、SKU ごとの入力を促す (buildItemPayload が SKU 名つきで止める。Codex R1 high)
  const article = String(rk?.article_number || '').trim();
  const legacyModelConflict = legacyModels.length > 1 || (legacyModels.length === 1 && !!article && legacyModels[0] !== article);
  if (article) base.set(MODEL_ATTR_NAME, [article]);
  else if (legacyModels.length === 1) base.set(MODEL_ATTR_NAME, [legacyModels[0]]);
  else if (legacyModels.length > 1) base.set(MODEL_ATTR_NAME, []);
  const names = [...base.keys()];
  const bySku = new Map();
  const explicit = new Map();       // skuKey → Set(name): SKU 行で明示的に上書きされた項目
  for (const m of members || []) {
    const key = String(m.商品コード || '').trim().toLowerCase();
    bySku.set(key, new Map([...base.entries()].map(([n, vals]) => [n, vals.slice()])));
    explicit.set(key, new Set());
  }
  const rows = db.prepare('SELECT sku_code, name, value FROM draft_sku_attributes WHERE draft_id = ? ORDER BY rowid').all(draftId);
  for (const r of rows) {
    const cell = bySku.get(String(r.sku_code));
    if (!cell) continue; // 外した SKU の残骸は読まない
    const values = splitAttributeValues(r.value);
    if (!names.includes(r.name) && values.length > 0) names.push(r.name);
    cell.set(r.name, values);
    explicit.get(String(r.sku_code)).add(r.name);
  }
  return { names, bySku, explicit, legacyModels, legacyModelConflict, legacyCatalogIds };
}

/**
 * 出品 payload を組み立てる。送れない状態なら reasons を返す (dry_run と live で共通)。
 */
export function buildItemPayload(db, draftId) {
  const draft = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(draftId);
  if (!draft) return { ok: false, reasons: ['ドラフトが見つかりません'] };
  const rk = db.prepare('SELECT * FROM draft_rakuten WHERE draft_id = ?').get(draftId) || {};
  const yahooRow = db.prepare('SELECT tax_rate FROM draft_yahoo WHERE draft_id = ?').get(draftId) || {};
  const ai = {};
  for (const r of db.prepare('SELECT kind, content FROM draft_ai_outputs WHERE draft_id = ?').all(draftId)) {
    ai[r.kind] = r.content;
  }
  const specs = db.prepare('SELECT spec_key, spec_value FROM draft_specs WHERE draft_id = ? ORDER BY sort, id').all(draftId);
  const cabinetAll = db.prepare('SELECT drive_file_id, cabinet_location, drive_modified_time FROM draft_cabinet_images WHERE draft_id = ? ORDER BY id').all(draftId);
  // 白抜き背景画像は images[] ではなく whiteBgImage で送る (検索サムネ用の別枠)
  const whiteBgId = rk.white_bg_drive_file_id || null;
  const whiteBg = whiteBgId
    ? cabinetAll.find((c) => c.drive_file_id === whiteBgId
        && String(c.drive_modified_time || '') === String(rk.white_bg_modified_time || '')) || null
    : null;
  // 送る画像は「現在の draft_images」と転送履歴の JOIN (Codex R1 Medium-2:
  // 履歴だけから作ると、転送後に削除・差し替えた画像や旧白抜き画像が payload に残る)。
  // 突合は ID + Drive更新日時 — 上書きされた画像は「未転送」に落として登録を止める (Codex R2 high)。
  // 並びも draft_images の sort に従う
  const cabinetByFile = freshCabinetMap(cabinetAll);
  const currentImages = db.prepare('SELECT drive_file_id, sort, drive_modified_time FROM draft_images WHERE draft_id = ? ORDER BY sort, id').all(draftId)
    .filter((i) => i.drive_file_id !== whiteBgId);
  const cabinet = currentImages
    .filter((i) => cabinetByFile.has(cabinetKeyOf(i)))
    .map((i) => ({ cabinet_location: cabinetByFile.get(cabinetKeyOf(i)) }));
  const untransferredCount = currentImages.length - cabinet.length;

  // 発送方法 (アプリ指定 > NEマッピング)。商品ページ表記の表示名と末尾バナーの選択で共通
  const effectiveShip = effectiveShippingForDraft(db, draft.ne_code, rk.shipping_method_group);
  const trailingBanners = trailingBannerLocations(effectiveShip.group);

  const reasons = [];
  // セット派生の仮コードのまま出さない (2026-08-23)。manage_number は登録後に変えられないので、
  // 仮コード (SET-xxx-01) で出すと商品ページを作り直す羽目になる。NE 登録後に本コードへ差し替える
  if (draft.provisional_code === 1) {
    reasons.push(/^SET-/i.test(String(draft.ne_code || ''))
      ? `商品コードが仮のままです (${draft.ne_code})。ネクストエンジンに登録した本コードへ差し替えてください`
      : `商品コード「${draft.ne_code}」がNE商品マスタに見つかりません (登録待ちか取込待ち)。取り込まれると自動で解除されます`);
  }
  // 画像トラック (依頼 → 制作 → 登録 → 承認) が終わるまで出さない。
  // #888 で「出品時のゲート」と画面に書いたまま配線が抜けていた。画像作成承認者の追加 (2026-08-23) で配線
  const imageBlock = imageTrackBlockReason(db, draftId);
  if (imageBlock) reasons.push(imageBlock);
  // カラバリ (バリエーションページ) は 2026-09-02 から対応。SKU ごとに
  // 項目選択肢の値 (selectorValues) と カタログID が要る。材料は下の buildVariants で検証する
  const vari = resolveVariationGroup(db, draft.ne_code, { draftId, withMembers: true });
  const isVariation = vari.kind === 'variation' && vari.memberCount > 1;
  const title = (ai.rakuten_title || draft.name || '').trim();
  if (!title) reasons.push('楽天タイトル (または商品名) がありません');
  if (title.length > 255) reasons.push(`楽天タイトルが長すぎます (${title.length}文字 / 上限255)`);
  if (!rk.genre_id || !/^\d+$/.test(String(rk.genre_id).trim())) reasons.push('ジャンルIDが未入力か数字ではありません');
  if (!Number.isInteger(draft.price) || draft.price < 1 || draft.price > 100_000_000) {
    reasons.push('売価が未入力か範囲外です (1〜1億円)');
  }
  const attributes = parseAttributes(rk.attributes_json);
  if (attributes === null) reasons.push('商品属性の形式が不正です (name と value を埋めてください)');
  if (cabinet.length === 0) reasons.push('R-Cabinet へ転送済みの画像がありません (先に「画像を転送」)');
  // TOP画像 (商品画像1) は <商品コード>_top を使う (2026-08-08 スタッフ指摘)。
  // 枠1 = sort 0。フォルダ取込で _top が無いと枠1が空くので、詰めて別の画像が
  // TOP になる前に止める (旧データは sort 0 から詰まっているため素通りする)
  if (currentImages.length > 0 && !currentImages.some((i) => Number(i.sort) === 0)) {
    reasons.push(`TOP画像がありません。画像フォルダに「${draft.ne_code}_top」を置いて「フォルダから自動セット」をやり直してください`);
  }
  if (untransferredCount > 0) {
    reasons.push(`商品画像 ${untransferredCount} 枚が R-Cabinet に未転送です (「画像を転送」を実行)`);
  }
  if (cabinet.length + trailingBanners.length > MAX_RAKUTEN_IMAGES) {
    reasons.push(`楽天の商品画像は最大 ${MAX_RAKUTEN_IMAGES} 枚です (商品画像 ${cabinet.length} 枚 + 自動追加バナー ${trailingBanners.length} 枚 — 商品画像を減らしてください)`);
  }
  if (whiteBgId && !whiteBg) reasons.push('白抜き背景画像が R-Cabinet に未転送です (先に「画像を転送」)');
  // 税率は 8% / 10% / 空欄 (=店舗デフォルト10%) 以外を fail-closed で止める (Codex R1 Medium-1)
  const taxText = String(yahooRow.tax_rate ?? '').trim();
  if (taxText && !/^(8|10)\s*%?$/.test(taxText)) {
    reasons.push(`税率「${taxText}」が不正です (8% / 10% / 空欄のみ)`);
  }
  // ─── カタログID (JAN) の入口は基本情報タブだけ (2026-09-02 中原さん: 基本情報タブと
  //     カテゴリ・属性タブの 2 箇所に JAN を入れさせない) ───
  //   単品         : product_drafts.jan_code (基本情報タブの JANコード欄)
  //   バリエーション: draft_sku_jans (基本情報タブの SKU 表)。ページ代表の jan_code は楽天には使わない
  // 商品属性の行に「カタログID」を手入力する経路は廃止した。SKU ごとに値が違うので 1 行では表せず、
  // ページ代表の値を全 SKU に付けると SKU の articleNumber と食い違う
  // (根本対策: 「カタログID」属性は下で SKU ごとに自分の JAN から自動付与する)
  // (バリエーションは skuAttributeGrid の legacyCatalogIds で別に案内する)
  if (!isVariation && Array.isArray(attributes) && attributes.some((a) => a.name === 'カタログID')) {
    reasons.push('商品属性の行に「カタログID」があります — カタログID (JAN) はカテゴリ・属性タブの表の「カタログID」の行で入力すると自動で送ります。属性の行からは削除してください');
  }
  const jan = isVariation ? '' : String(draft.jan_code || '').trim();
  if (jan && !isValidGtin(jan)) {
    reasons.push(`JANコード「${jan}」の形式が不正です (8/12/13桁 + チェックデジット)`);
  }
  // カタログID (= API の articleNumber) が無いときの理由。JAN がある SKU には使わない。
  // 未設定は 5 (該当製品コードなし) = 2026-09-02 以前の固定値なので、既存ドラフトの送信内容は変わらない
  const rawReason = Number(rk.catalog_id_exemption_reason);
  const catalogExemptionReason = Number.isInteger(rawReason) && rawReason >= 1 && rawReason <= 6 ? rawReason : 5;

  // ─── カラバリ (バリエーションページ) の材料 (2026-09-02) ───
  // 楽天の形: item.variantSelectors [{key, displayName, values:[{displayValue}]}]
  //           + variants[sku].selectorValues {軸キー: 値}   (variation-resolver.js の実測に合わせる)
  // 軸は 1 ページ 1 つだけ対応する (2軸以上は RMS 画面で組む)
  const selectorName = String(rk.variant_selector_name || '').trim();
  let variantRows = null;   // [{ skuCode, selectorValue, jan, price }]
  if (isVariation) {
    if (!selectorName) {
      reasons.push('バリエーションの項目名 (「種類」「カラー」など) が未入力です — カテゴリ・属性タブの「バリエーション」で入れてください');
    }
    const selRows = new Map(db.prepare('SELECT sku_code, value FROM draft_sku_selector_values WHERE draft_id = ?')
      .all(draftId).map((r) => [String(r.sku_code), String(r.value)]));
    const janRows = new Map(db.prepare('SELECT sku_code, jan_code FROM draft_sku_jans WHERE draft_id = ?')
      .all(draftId).map((r) => [String(r.sku_code), String(r.jan_code)]));
    // 画面で入れた SKU別売価 (draft_sku_prices)。入っていればこれが最優先
    const priceRows = new Map(db.prepare('SELECT sku_code, price FROM draft_sku_prices WHERE draft_id = ?')
      .all(draftId).map((r) => [String(r.sku_code), Number(r.price)]));
    // SKU ごとの「カタログIDなしの理由」(無ければページ共通の理由) と商品仕様 (2026-09-03 SKU 表)
    const exemptionRows = new Map(db.prepare('SELECT sku_code, reason FROM draft_sku_catalog_exemptions WHERE draft_id = ?')
      .all(draftId).map((r) => [String(r.sku_code), Number(r.reason)]));
    const grid = skuAttributeGrid(db, draftId, rk, vari.members);
    // 旧データ (2026-09-02 までの単品の属性行) の「カタログID」は SKU 表に展開しない。JAN は専用行に入れる方式なので、
    // 表の警告ボタンで削除してもらう (Codex R3 high: 展開すると全 SKU で止まり、画面から消せなかった)
    if (grid.legacyCatalogIds.length > 0) {
      reasons.push(`旧データの商品仕様に「カタログID」(${grid.legacyCatalogIds.join(' / ')}) が残っています — カテゴリ・属性タブの表の上の警告「旧データの「カタログID」属性を削除する」で消してください (JAN は表の「カタログID」の行に SKU ごとに入れます)`);
    }
    variantRows = [];
    for (const m of vari.members) {
      const skuCode = String(m.商品コード || '').trim();
      const key = skuCode.toLowerCase();
      const selectorValue = String(selRows.get(key) || '').trim();
      const skuJan = String(janRows.get(key) || '').trim();
      const cell = grid.bySku.get(key) || new Map();
      if ((cell.get('カタログID') || []).length > 0) {
        reasons.push(`SKU「${skuCode}」の商品仕様に「カタログID」の行があります — カタログID (JAN) は表の「カタログID」の行で入力してください`);
      }
      const skuAttrs = [...cell.entries()]
        .filter(([n, vals]) => vals.length > 0 && n !== MODEL_ATTR_NAME && n !== 'カタログID')
        .map(([n, vals]) => ({ name: n, values: vals.slice() }));
      // メーカー型番は 1 値 (RMS の articleNumber 相当ではなく属性として送るが、値は 1 つ)。
      // 「A | B」のように複数入っていたら先頭だけ黙って送らず止める (Codex R2 high)
      const modelVals = cell.get(MODEL_ATTR_NAME) || [];
      if (modelVals.length > 1) {
        reasons.push(`SKU「${skuCode}」の${MODEL_ATTR_NAME}が複数あります (${modelVals.join(' / ')}) — 1 つだけにしてください`);
      }
      const skuModel = String(modelVals[0] || '').trim();
      const modelExplicit = (grid.explicit.get(key) || new Set()).has(MODEL_ATTR_NAME);
      const rawSkuReason = exemptionRows.get(key);
      const skuReason = Number.isInteger(rawSkuReason) && rawSkuReason >= 1 && rawSkuReason <= 6 ? rawSkuReason : catalogExemptionReason;
      if (!selectorValue) reasons.push(`SKU「${skuCode}」の選択肢 (${selectorName || 'バリエーション'}) が未入力です — 基本情報タブのSKU表で入れてください`);
      if (skuJan && !isValidGtin(skuJan)) {
        reasons.push(`SKU「${skuCode}」のJANコード「${skuJan}」の形式が不正です (8/12/13桁 + チェックデジット)`);
      }
      // 売価の優先順: 画面で入れた SKU別売価 > NE の標準売価 > ページ代表の売価 (RMS は SKU ごとに必須)
      const manualPrice = Number(priceRows.get(key));
      const rawPrice = Number(m.標準売価);
      const price = Number.isFinite(manualPrice) && manualPrice >= 1 ? Math.round(manualPrice)
        : (Number.isFinite(rawPrice) && rawPrice >= 1 ? Math.round(rawPrice) : draft.price);
      if (!Number.isInteger(price) || price < 1) {
        reasons.push(`SKU「${skuCode}」の売価がありません (NEの標準売価も代表の売価も未設定)`);
      }
      variantRows.push({ skuCode, selectorValue, jan: skuJan, price, attrs: skuAttrs, model: skuModel, modelExplicit, reason: skuReason });
    }
    // 旧データでメーカー型番が食い違っている (属性側に複数 / 欄と別) バリエーション: 既定値にできないので、
    // SKU 表の「メーカー型番」の行が全 SKU で明示的に入るまで止める (黙って捨てない・隠さない。Codex R1 high)
    if (grid.legacyModelConflict && variantRows.some((v) => !v.modelExplicit)) {
      const vals = [...new Set([...grid.legacyModels, String(rk.article_number || '').trim()].filter(Boolean))];
      reasons.push(`旧データの${MODEL_ATTR_NAME}が食い違っています (${vals.join(' / ')}) — カテゴリ・属性タブの表「${MODEL_ATTR_NAME}」の行に SKU ごとの値を入れてください (全 SKU に入れば旧データは使いません)`);
    }
    // 同じ値が 2 SKU に付くと組み合わせが重複して RMS に弾かれる (DB の UNIQUE でも防ぐが、
    // 取込など別経路で入った分に備えてここでも見る)
    const seenValues = new Map();
    for (const v of variantRows) {
      if (!v.selectorValue) continue;
      if (seenValues.has(v.selectorValue)) {
        reasons.push(`${selectorName || '項目選択肢'}「${v.selectorValue}」が ${seenValues.get(v.selectorValue)} と ${v.skuCode} で重複しています`);
      } else seenValues.set(v.selectorValue, v.skuCode);
    }
    if (variantRows.length > 40) reasons.push(`SKUが多すぎます (${variantRows.length}件 / 楽天の上限40件)`);
  }
  // 送る SKU の一覧 (単品 = 商品コード 1 行)。カタログIDの必須判定と「カタログID」属性の自動付与は
  // この単位で行う (単品とバリエーションで判定を分けない)
  // 単品の attrs/model は下で確定する (ページ共通の属性 + メーカー型番欄)
  const skuRows = isVariation && variantRows
    ? variantRows
    : [{ skuCode: String(draft.ne_code).trim(), selectorValue: null, jan, price: draft.price, attrs: null, model: null, reason: catalogExemptionReason }];
  // 1 (セット商品) は articleNumberForSet (構成品の JAN 一覧) が別に必須になる。まだ送れないので止める
  if (skuRows.some((s) => !s.jan && s.reason === 1)) {
    reasons.push('カタログIDなしの理由「1: セット商品」は未対応です (構成品のJAN一覧を送る仕組みがまだありません)。JANを入力するか別の理由を選んでください');
  }

  // メーカー型番はカタログIDと同じ扱い (2026-08-31 中原さん: RMS でも入力項目は 1 つなのに
  // 画面で 2 箇所に入れさせていた)。**入口は「メーカー型番」欄 (article_number) だけ**にして、
  // ジャンル属性に「メーカー型番」があるときは下でその値を自動付与する。
  // 旧データで属性側にも残っている場合に備え、食い違いだけは止める
  const articleNo = String(rk.article_number || '').trim();
  const modelAttrs = Array.isArray(attributes) ? attributes.filter((a) => a.name === MODEL_ATTR_NAME) : [];
  let manualModel = null;
  // バリエーションは SKU 表の「メーカー型番」の行が入口 (旧データの食い違いは上で SKU 表への入力を促している)
  if (isVariation) {
    manualModel = modelAttrs[0] || null;
  } else if (modelAttrs.length > 1) {
    reasons.push(`商品属性「${MODEL_ATTR_NAME}」が複数あります (1件にまとめてください)`);
  } else if (modelAttrs.length === 1) {
    manualModel = modelAttrs[0];
    if (manualModel.values.length !== 1) {
      reasons.push(`商品属性「${MODEL_ATTR_NAME}」の値は1個だけにしてください`);
    } else if (articleNo && manualModel.values[0] !== articleNo) {
      reasons.push(`商品属性の${MODEL_ATTR_NAME} (${manualModel.values[0]}) と メーカー型番欄 (${articleNo}) が一致しません — メーカー型番欄に揃えてください`);
    }
  }
  // ─── ジャンル属性辞書による事前検証 (2026-07-28 Genre API) ───
  // 辞書キャッシュがある場合だけ検証する (無ければ RMS が最終検証する)。
  // genre_id をキーに引くので、ジャンルを変えた直後は辞書未取得 = 検証スキップになる
  // 鮮度 24h 以内の辞書だけ検証に使う。古い/無い場合は検証スキップ (RMS が最終検証)。
  // 登録経路 (registerItem/preview) は事前に fetchGenreAttributes で鮮度を回復させている
  const genreDict = (rk.genre_id && /^\d+$/.test(String(rk.genre_id).trim()))
    ? getCachedGenreAttributes(db, String(rk.genre_id).trim(), { maxAgeMs: GENRE_CACHE_TTL_MS })
    : null;
  let dictHasCatalogId = false;
  let dictHasModel = false;
  if (genreDict && Array.isArray(attributes)) {
    const dictByName = new Map(genreDict.attributes.map((a) => [a.name, a]));
    dictHasCatalogId = dictByName.has('カタログID');
    dictHasModel = dictByName.has(MODEL_ATTR_NAME);
    const genreLabel = genreDict.genreName || rk.genre_id;
    // 検証の単位は SKU (単品 = 1 行)。バリエーションは SKU 表の値、単品はページ共通の属性 + メーカー型番欄
    for (const s of skuRows) {
      const skuAttrs = isVariation ? s.attrs : attributes;
      const skuModel = isVariation ? s.model : articleNo;
      const tag = isVariation ? `SKU「${s.skuCode}」の` : '';
      // ① 辞書に無い属性名は IE1002 で登録が失敗する → 送る前に止める
      for (const a of skuAttrs) {
        if (!dictByName.has(a.name)) {
          reasons.push(`${tag}属性「${a.name}」はジャンル「${genreLabel}」の属性辞書にありません (登録エラー IE1002 になります)`);
        }
      }
      // ② 必須属性の欠落 (カタログIDは JAN、メーカー型番は欄/セルの値で判定する)
      const presentNames = new Set(skuAttrs.map((a) => a.name));
      for (const da of genreDict.attributes) {
        if (!da.mandatory || presentNames.has(da.name)) continue;
        if (da.name === 'カタログID') {
          if (!s.jan) {
            reasons.push(isVariation
              ? `SKU「${s.skuCode}」のカタログID (JAN) が未入力です (ジャンル ${genreLabel} の必須属性) — カテゴリ・属性タブのSKU表「カタログID」の行で入れてください`
              : `カタログID (JAN) が未入力です (ジャンル ${genreLabel} の必須属性) — カテゴリ・属性タブの「カタログID」の行で入れてください`);
          }
          continue; // JAN があれば下で SKU ごとに自動付与する
        }
        if (da.name === MODEL_ATTR_NAME && skuModel) continue; // メーカー型番の欄/セルから自動付与する
        reasons.push(`${tag}必須属性「${da.name}」が未入力です (ジャンル ${genreLabel} の必須)`);
      }
      // ③ 値の数・長さの軽い検証 (RMS と同じ基準で早めに教える)。メーカー型番も辞書にあれば同じ基準で見る
      const checkList = skuModel && dictByName.has(MODEL_ATTR_NAME)
        ? [...skuAttrs, { name: MODEL_ATTR_NAME, values: [skuModel] }]
        : skuAttrs;
      for (const a of checkList) {
        const da = dictByName.get(a.name);
        if (!da) continue;
        if (da.multiValueLimit && a.values.length > da.multiValueLimit) {
          reasons.push(`${tag}属性「${a.name}」の値は最大 ${da.multiValueLimit} 個です`);
        }
        if (da.maxLength) {
          for (const v of a.values) {
            if (String(v).length > da.maxLength) reasons.push(`${tag}属性「${a.name}」の値が長すぎます (上限 ${da.maxLength} 文字)`);
          }
        }
      }
    }
  }

  // 配送方法は生値で判定しない (複合選択肢 '1y5' は楽天の値ではないので、生値を楽天IDとして
  // 見ると「画面では指定済みなのに不正扱い」になる — #725 で作り込んだ不具合)
  const shippingResolved = toRakutenShippingGroup(rk.shipping_method_group);
  if (!shippingResolved.ok) {
    reasons.push('配送方法の指定が不正です');
  }
  if (rk.normal_delivery_date_id != null && String(rk.normal_delivery_date_id).trim() !== ''
      && !/^\d+$/.test(String(rk.normal_delivery_date_id).trim())) {
    reasons.push('納期情報IDは数字で入力してください (RMS の納期設定のID)');
  }
  // 商品ページ表記 (化粧品・健康食品は楽天必須記載が揃うまで登録をブロック)
  const pageInfo = db.prepare('SELECT * FROM draft_page_info WHERE draft_id = ?').get(draftId) || null;
  reasons.push(...validatePageInfo(pageInfo).map((r) => `商品ページ表記: ${r}`));

  if (reasons.length > 0) return { ok: false, reasons };

  // ─── 説明文3欄 (2026-08-01 店舗フォーマット確定。組み立ては composeDescriptions に共通化) ───
  //   PC用商品説明文 (productDescription.pc) = 表1枚 (説明/注意事項/仕様表/商品ページ表記)
  //   PC用販売説明文 (salesDescription)      = 商品画像を width100% で並べた画像HTML
  //   スマホ用商品説明文 (productDescription.sp) = 販売説明文 + 商品説明文
  const { pc: pcHtml, sales: salesHtml, sp: spHtml } = composeDescriptions({
    productName: draft.name, ai, specs, pageInfo,
    cabinetLocations: cabinet.map((c) => c.cabinet_location),
  });
  if (pcHtml.length > DESC_LIMIT) reasons.push(`PC用商品説明文が長すぎます (${pcHtml.length}字 / 上限${DESC_LIMIT}字)`);
  if (salesHtml.length > DESC_LIMIT) reasons.push(`PC用販売説明文 (画像HTML) が長すぎます (${salesHtml.length}字 / 上限${DESC_LIMIT}字 — 画像を減らしてください)`);
  if (spHtml.length > DESC_LIMIT) reasons.push(`スマホ用商品説明文 (販売説明文+商品説明文) が長すぎます (${spHtml.length}字 / 上限${DESC_LIMIT}字)`);
  if (reasons.length > 0) return { ok: false, reasons };

  // JAN → カタログID属性の自動付与は**ジャンル属性辞書にあるときだけ** (2026-07-28 確定)。
  // 実測: 「カタログID」が辞書に無いジャンル (111145等) へ付与すると IE1002 で登録自体が失敗する。
  // 辞書未取得のジャンルでは付与しない (安全側。必要なら「ジャンル情報を取得」してから登録する)。
  // 値は **SKU ごとに自分の JAN** (2026-09-02: ページ代表の JAN を全 SKU に付けていた穴を塞ぐ)
  // SKU ごとに送る属性 (2026-09-03): バリエーションは SKU 表の値、単品はページ共通の属性。
  // メーカー型番は辞書にあるジャンルだけ補う (2026-08-31。単品は欄の値、旧データで属性側に同じ値が
  // 残っている場合は二重に足さない — 上で不一致は弾いてある)
  const attrsForSku = (s) => {
    const list = isVariation ? s.attrs.slice() : attributes.slice();
    const model = isVariation ? s.model : (manualModel ? '' : articleNo);
    if (dictHasModel && model) list.push({ name: MODEL_ATTR_NAME, values: [model] });
    if (dictHasCatalogId && s.jan) list.push({ name: 'カタログID', values: [s.jan] });
    return list;
  };

  // 送料・配送方法 (variants[].shipping)。未設定の項目は送らず店舗デフォルトに任せる。
  // RMS へ出るのは**必ず楽天グループID** (複合選択肢は上で '1' 等に解決済み)。
  // セットだけ NE へフォールバックする理由は payloadShippingGroup を見ること (§4.4 決⑥)
  const shippingGroup = payloadShippingGroup(draft, effectiveShip, shippingResolved);
  const shipping = {
    ...(shippingGroup ? { shippingMethodGroup: shippingGroup } : {}),
    ...(rk.postage_included != null ? { postageIncluded: rk.postage_included === 1 } : {}),
  };
  const deliveryDateId = String(rk.normal_delivery_date_id ?? '').trim();

  const payment = taxRateToPayment(yahooRow.tax_rate);

  // 商品コード (NE商品コード) は SKU管理番号 (variants キー)・商品番号・システム連携用SKU番号の
  // 3ヶ所に同じ表記で入れる (Codex R1 medium: 正規化を1回にして揃える)。
  // 商品管理番号だけは RMS 仕様で小文字必須のため呼び出し側で toLowerCase する
  const productCode = String(draft.ne_code).trim();

  const payload = {
    title,
    // 商品番号 = 商品コード (2026-08-05 中原さん指示)。バリエーションページ (P3.5) でも代表商品コードを入れる
    itemNumber: productCode,
    ...(ai.desc_catch ? { tagline: String(ai.desc_catch).trim() } : {}),
    productDescription: { pc: pcHtml || title, ...(spHtml ? { sp: spHtml } : {}) },
    ...(salesHtml ? { salesDescription: salesHtml } : {}),
    genreId: String(rk.genre_id).trim(),
    // 公開で登録 (2026-08-05 中原さん指示)。在庫は Item API では送れない (Inventory API 2.1 が別) ため
    // 新規 SKU は在庫0 = 公開しても「売り切れ」で売れない (2026-08-05 平串の実測・中原さんスクショで確認)。
    // 在庫は NE 連携が入れる。miniPC 側 route も hideItem=false を透過する (それ以前は true を強制)
    hideItem: false,
    itemType: 'NORMAL',
    // 商品画像の末尾に店舗共通バナー (配送方法バナー + 共通3枚) を自動追加
    images: [
      ...cabinet.map((c) => ({ type: 'CABINET', location: c.cabinet_location })),
      ...trailingBanners.map((loc) => ({ type: 'CABINET', location: loc })),
    ],
    ...(whiteBg ? { whiteBgImage: { type: 'CABINET', location: whiteBg.cabinet_location } } : {}),
    payment, // 消費税率は常に明示 (2026-08-05〜)

    // カラバリは軸の定義を item 直下に置く (variation-resolver.js が読んでいる形と同じ)
    ...(isVariation && variantRows ? {
      variantSelectors: [{
        key: selectorName,
        displayName: selectorName,
        values: variantRows.map((v) => ({ displayValue: v.selectorValue })),
      }],
    } : {}),

    variants: isVariation && variantRows ? Object.fromEntries(variantRows.map((v) => [v.skuCode, {
      merchantDefinedSkuId: v.skuCode,
      standardPrice: v.price,
      // カタログIDなしの理由は SKU ごと (無ければページ共通の理由)
      articleNumber: v.jan ? { value: v.jan } : { exemptionReason: v.reason },
      selectorValues: { [selectorName]: v.selectorValue },
      ...(attrsForSku(v).length > 0 ? { attributes: attrsForSku(v) } : {}),
      ...(Object.keys(shipping).length > 0 ? { shipping } : {}),
      ...(deliveryDateId ? { normalDeliveryDateId: Number(deliveryDateId) } : {}),
    }])) : {
      [productCode]: {
        // システム連携用SKU番号 = 商品コード (2026-08-05 中原さん指示。NE との SKU 突合キー)
        merchantDefinedSkuId: productCode,
        standardPrice: draft.price,
        // 🚨 articleNumber = RMS 画面の「カタログID」= **JAN 等の製品コード**。
        //    メーカー型番ではない (メーカー型番は下の attrs に「メーカー型番」属性として積む)。
        //    2026-09-02 まで rk.article_number (メーカー型番) を value に入れており、
        //    型番を欄に入れた商品が IE0228 Invalid articleNumber で必ず落ちていた (shaganshi)。
        //    〜2026-08-31 は欄が空で exemptionReason=5 に落ちていたため表面化しなかった。
        // 免除理由 (2026-08-05 平串の IE0429 で実測確定):
        //   1=セット商品 (articleNumberForSet が必須になる) / 2=サービス商品 / 3=店舗オリジナル商品
        //   4=項目選択肢別在庫商品 / 5=該当製品コードなし / 6=頒布会商品
        articleNumber: jan
          ? { value: jan }
          : { exemptionReason: catalogExemptionReason },
        ...(attrsForSku(skuRows[0]).length > 0 ? { attributes: attrsForSku(skuRows[0]) } : {}),
        ...(Object.keys(shipping).length > 0 ? { shipping } : {}),
        ...(deliveryDateId ? { normalDeliveryDateId: Number(deliveryDateId) } : {}),
      },
    },
  };
  return { ok: true, payload, draft };
}

/**
 * RMS のエラーコードを「人が次に何をすればいいか」に翻訳する (2026-09-02)。
 * 生の英語 + JSON のままだと、画面に出ても直しようがない (shaganshi の IE0228 / IE0418 で実証)。
 * 翻訳できないコードは null を返し、呼び出し側が原文をそのまま見せる (握りつぶさない)。
 */
export function translateRmsError(code, message, metadata) {
  const md = metadata && typeof metadata === 'object' ? metadata : {};
  const path = String(md.propertyPath || '');
  const details = Array.isArray(md.details) ? md.details : [];
  // 属性名は details[].properties.attributeName に入る (実測 2026-09-02)
  const attrName = details.map((d) => d && d.properties && d.properties.attributeName).find((n) => n);
  const attrLabel = attrName ? `「${attrName}」` : '';

  if (code === 'IE0228' && /articleNumber/i.test(path)) {
    return 'カタログID (JANコード) の値を楽天が受け付けませんでした。カテゴリ・属性タブの「カタログID」を確認してください'
      + ' — メーカー型番を入れる欄ではありません (型番は「商品仕様」のメーカー型番へ)';
  }
  if (code === 'IE0229' && /articleNumber/i.test(path)) {
    return 'カタログIDが空です。カテゴリ・属性タブの「カタログID」でJANを入れるか、IDなしの理由を選んでください';
  }
  if (code === 'IE0418' && details.some((d) => d && d.code === 'invalidSelectiveValue')) {
    return `商品属性${attrLabel}の値が、楽天がこのジャンルで用意している選択肢にありません。`
      + '選択式の属性なので自由入力はできません — RMS の商品編集画面で選べる値を確かめ、同じ表記で入れ直してください';
  }
  if (code === 'IE0418') {
    return `商品属性${attrLabel}かジャンルIDが不正です${path ? ` (${path})` : ''}`;
  }
  if (code === 'IE1002') {
    return `このジャンルに存在しない商品属性が含まれています${attrLabel}。「ジャンル情報を取得」で候補を出し直してください`;
  }
  if (code === 'IE0429') {
    return 'カタログIDなしの理由がこの商品に使えません (例: セット商品を選ぶと構成品のJAN一覧が必要)。理由を選び直してください';
  }
  return null;
}

/** RMS のエラー本文から人が読める文を取り出す */
export function extractRmsErrors(data) {
  if (data == null) return '';
  if (typeof data === 'string') return data.slice(0, 800);
  const list = Array.isArray(data?.errors) ? data.errors : null;
  if (list) {
    return list.map((e) => {
      const raw = `${e.code || ''}: ${e.message || ''}${e.metadata ? ' ' + JSON.stringify(e.metadata) : ''}`;
      const jp = translateRmsError(e.code, e.message, e.metadata);
      // 日本語を先に出す。原文は括弧で残す (楽天サポートに投げるときに要る)
      return jp ? `${jp}\n  └ ${raw}` : raw;
    }).join('\n').slice(0, 2500);
  }
  return JSON.stringify(data).slice(0, 800);
}

/**
 * 楽天に公開状態で登録する (2026-08-05 中原さん指示。それ以前は非公開登録→公開ボタンの二段階)。
 * 在庫0で登録するので公開でも売れない (在庫は NE 連携が入れる)。
 * 既存商品の上書きは miniPC 側が 409 で拒否する (稼働中ページを潰さない)。
 */
export async function registerItem(draftId, { actor = null } = {}) {
  const db = getDB();
  // 登録直前に辞書の鮮度を回復する (24h キャッシュ内なら通信なし)。
  // 失敗しても登録は止めない — 辞書が無ければ検証スキップで RMS が最終検証する
  const rkRow = db.prepare('SELECT genre_id FROM draft_rakuten WHERE draft_id = ?').get(draftId);
  if (rkRow?.genre_id && /^\d+$/.test(String(rkRow.genre_id).trim())) {
    try { await fetchGenreAttributes(db, String(rkRow.genre_id).trim()); } catch (_) { /* best-effort */ }
  }
  // Drive 側で画像が差し替わっていたら「未転送」に落として登録を止める (Codex R3 high)
  await refreshDriveModifiedTimes(draftId);
  const built = buildItemPayload(db, draftId);
  if (!built.ok) return { ok: false, reasons: built.reasons };
  const mn = String(built.draft.ne_code).trim().toLowerCase();

  // PUT の「結果不明」をそのまま失敗にすると、実は登録が通っていた場合に公開ページの孤児
  // (アプリは未登録扱い・再実行は 409) になる (Codex R1〜R3 High — 公開直行化で影響が大きい)。
  // 結果不明 = transport 例外 / warehouse が RMS PUT を試みた後の 5xx (RMS_API_ERROR や
  // ゲートウェイの生 502)。GET で実在を照合して成功/失敗を確定させる。
  // PUT が RMS に届く前に確定失敗したと分かる 5xx (既存確認failや書込ゲートOFF) は照合不要
  const itemPath = `/service-api/rakuten-rms/items/manage-numbers/${encodeURIComponent(mn)}`;
  let r = null;
  let transportError = null;
  try {
    r = await callWarehouse(itemPath, { method: 'PUT', body: built.payload });
  } catch (e) { transportError = e; }

  const definitiveFail = new Set(['RMS_PRECHECK_FAILED', 'RMS_WRITE_DISABLED']);
  const outcomeUnknown = r === null || (r.status >= 500 && !definitiveFail.has(r.data?.error));
  if (outcomeUnknown) {
    let probe = null;
    try { probe = await callWarehouse(itemPath); } catch (_) { /* 照合も不通 */ }
    if (probe?.status === 404) {
      // 直後の 404 は失敗を確定しない (Codex R2 High): 元の PUT が warehouse 側でまだ処理中なら
      // この後に商品が作られ得る。warehouse→RMS のタイムアウト上限 (90s) を待てば PUT は必ず
      // 決着しているので、そこで再照合した結果だけを信じる (レアな経路なので待ち時間は許容)
      await new Promise((resolve) => setTimeout(resolve, 90_000));
      probe = null;
      try { probe = await callWarehouse(itemPath); } catch (_) { /* 照合も不通 */ }
    }
    if (probe?.status === 200) {
      r = { status: 200, data: probe.data }; // 登録自体は通っていた → 成功として記録を続行
    } else if (probe?.status === 404) {
      // 登録されていないことを確認できた → 失敗扱い (再実行で作り直せる)。
      // warehouse の 5xx レスポンスがあればそのまま下の失敗記録へ、transport 例外なら投げ直す
      if (r === null) throw transportError;
    } else {
      const cause = transportError ? String(transportError.message || transportError) : `HTTP ${r?.status}`;
      const err = new Error(`楽天への登録結果が確認できませんでした (${String(cause).slice(0, 120)})。RMS で商品管理番号 ${mn} の有無を確認してから再実行してください`);
      // 呼び出し元 (ボードからの出品) が「失敗 = やり直せる」と区別するための印 (Codex R1 critical):
      // 実は PUT が通っている可能性があるので、これは自動でも人の一押しでも再実行させてはいけない
      err.code = 'RMS_OUTCOME_UNKNOWN';
      throw err;
    }
  }
  // 公開登録なので published_at も登録時刻で埋める (公開/非公開ボタンの表示状態と整合させる)
  const saveResult = db.prepare(`
    INSERT INTO draft_rakuten (draft_id, registered_at, published_at, last_error)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(draft_id) DO UPDATE SET
      registered_at = COALESCE(excluded.registered_at, draft_rakuten.registered_at),
      published_at = COALESCE(excluded.published_at, draft_rakuten.published_at),
      last_error = excluded.last_error,
      updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
  `);

  if (r.status === 201 || r.status === 200) {
    const now = new Date().toISOString();
    saveResult.run(draftId, now, now, null);
    logEvent(db, draftId, 'rakuten_registered', mn, actor);
    // status の更新はここではしない (PR4): 呼び出し元 (router) の markRakutenListed が
    // 楽天モールを done にし、その中で工程からの導出 (recomputeDraftStatus) が listed へ進める
    // 店舗内カテゴリ (お店の棚) は商品 payload に載らない別 API なので、登録成功後に続けて反映する。
    // ここで失敗しても登録自体は成功しているので止めない (結果は shopCategories として返し、
    // draft_rakuten.shop_categories_error にも残るので画面から再実行できる)
    let shopCategories = null;
    try {
      shopCategories = await syncShopCategoriesToRms(draftId, { actor });
    } catch (e) {
      shopCategories = { ok: false, error: String(e.message || e).slice(0, 300) };
    }
    return { ok: true, manageNumber: mn, status: r.status, shopCategories };
  }

  const errText = r.data?.message || extractRmsErrors(r.data) || `HTTP ${r.status}`;
  saveResult.run(draftId, null, null, String(errText).slice(0, 1500));
  logEvent(db, draftId, 'rakuten_register_failed', String(errText).slice(0, 500), actor);
  return { ok: false, status: r.status, error: errText };
}

/**
 * 選択した店舗内カテゴリ (お店の棚) を RMS に反映する (2026-08-02、item-mappings API)。
 *
 * ⚠️ 商品 API には店舗内カテゴリのフィールドが無いので、出品 payload には載せられず
 * 登録とは別の API 呼び出しになる。そのため「登録は成功したが棚は未反映」が起こり得る
 * → 結果を draft_rakuten.shop_categories_synced_at / last_error に残し、画面から再実行できるようにする。
 *
 * @param {number} draftId
 * @param {{actor?: string|null, silent?: boolean}} opts silent=true なら登録直後の自動反映 (失敗しても throw しない)
 */
export async function syncShopCategoriesToRms(draftId, { actor = null } = {}) {
  const db = getDB();
  const draft = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(draftId);
  if (!draft) return { ok: false, error: 'draft_not_found' };
  const rk = db.prepare('SELECT * FROM draft_rakuten WHERE draft_id = ?').get(draftId);
  if (!rk?.registered_at) {
    // 未登録は「状態として自明」(画面に登録済みバッジが出ない) ので error 欄には残さない
    return { ok: false, error: 'このアプリから楽天に登録した商品だけ棚を反映できます (先に「楽天に登録」)' };
  }
  // どの失敗経路でも理由を残す (Codex R1 medium: リロードすると原因が消えていた)
  const fail = (msg, extra = {}) => {
    db.prepare(`
      INSERT INTO draft_rakuten (draft_id, shop_categories_error) VALUES (?, ?)
      ON CONFLICT(draft_id) DO UPDATE SET
        shop_categories_error = excluded.shop_categories_error,
        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
    `).run(draftId, String(msg).slice(0, 1000));
    logEvent(db, draftId, 'rakuten_shop_categories_failed', String(msg).slice(0, 500), actor);
    return { ok: false, error: msg, ...extra };
  };

  // 枠 (slot) 順 = RMS の「表示先カテゴリ 1〜5」の並び。枠1 がメインページになる
  const rows = db.prepare(`
    SELECT c.category_id, c.path FROM draft_shop_categories s
    JOIN ph_shop_categories c ON c.id = s.shop_category_id
    WHERE s.draft_id = ? ORDER BY s.slot, c.id
  `).all(draftId);
  if (rows.length === 0) {
    return fail('店舗内カテゴリが選択されていません (「カテゴリ・属性」タブで選んでください)');
  }
  // item-mappings の PUT は「割り当て全体の置き換え」なので、一部だけ送ると画面の選択と
  // RMS の状態がズレる。カテゴリIDが無い棚が1件でもあれば反映しない (Codex R1 medium)
  const missing = rows.filter((r) => r.category_id == null || String(r.category_id).trim() === '');
  if (missing.length > 0) {
    return fail(
      `カテゴリIDが分からない棚があります (${missing.map((m) => m.path).join(' ／ ').slice(0, 200)})。`
      + '一覧画面の「楽天から取り込む」でカテゴリ一覧を同期し直してください',
      { missingPaths: missing.map((m) => m.path) },
    );
  }

  const categoryIds = rows.map((r) => String(r.category_id).trim());
  const syncedKey = categoryIds.join(',');
  const mn = String(draft.ne_code).trim().toLowerCase();
  // mainPluralCategoryId は送らない (2026-08-05 平串の IE0128 で実測確定):
  // これは「PLURAL 形式 (1ページ複数商品形式) のカテゴリ」のメインページ指定専用で、
  // 通常の棚 (LIST/GALLERY 形式) の ID を渡すと IE0128 で拒否される。
  // 「複数カテゴリなら必須」という #671 時の理解は誤りだった
  const body = { categoryIds };

  let r;
  try {
    r = await callWarehouse(
      `/service-api/rakuten-rms/item-mappings/manage-numbers/${encodeURIComponent(mn)}`,
      { method: 'PUT', body, timeoutMs: 90_000 },
    );
  } catch (e) {
    return fail(`楽天への接続に失敗しました: ${String(e.message || e).slice(0, 200)}`);
  }
  if (r.status === 404 && !r.data) {
    return fail('miniPC 側の店舗内カテゴリ反映ルートが未反映です (miniPC で git pull + Restart-Service)');
  }
  if (r.status >= 200 && r.status < 300) {
    db.prepare(`
      INSERT INTO draft_rakuten (draft_id, shop_categories_synced_at, shop_categories_synced_key, shop_categories_error)
      VALUES (?, ?, ?, NULL)
      ON CONFLICT(draft_id) DO UPDATE SET
        shop_categories_synced_at = excluded.shop_categories_synced_at,
        shop_categories_synced_key = excluded.shop_categories_synced_key,
        shop_categories_error = NULL,
        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
    `).run(draftId, new Date().toISOString(), syncedKey);
    logEvent(db, draftId, 'rakuten_shop_categories_synced',
      rows.map((r2) => r2.path).join(' ／ ').slice(0, 500), actor);
    return { ok: true, count: categoryIds.length, paths: rows.map((r2) => r2.path) };
  }
  const errText = r.data?.message || extractRmsErrors(r.data) || `HTTP ${r.status}`;
  return fail(errText, { status: r.status });
}

/** 現在選択中の棚が RMS へ反映済みか (反映後に選択を変えたら「未反映」に戻す) */
export function shopCategorySyncState(db, draftId, rakuten) {
  if (!rakuten?.registered_at) return null;
  const ids = db.prepare(`
    SELECT c.category_id FROM draft_shop_categories s
    JOIN ph_shop_categories c ON c.id = s.shop_category_id
    WHERE s.draft_id = ? ORDER BY s.slot, c.id
  `).all(draftId).map((r) => (r.category_id == null ? '' : String(r.category_id).trim()));
  if (ids.length === 0) return 'none';
  const currentKey = ids.join(',');
  if (rakuten.shop_categories_synced_at && rakuten.shop_categories_synced_key === currentKey) return 'synced';
  if (rakuten.shop_categories_synced_at) return 'stale';   // 反映後に棚を変えた
  return rakuten.shop_categories_error ? 'failed' : 'pending';
}

/**
 * 公開/非公開の切り替え (2026-07-27 仕様: 公開に必要な情報は全てアプリから入れ、
 * 公開操作もアプリで行う。RMS 画面での手直しは最終手段)。
 * miniPC 側の visibility ルートで hideItem だけを反転する — このアプリから登録した商品限定。
 * ⚠️ miniPC が visibility ルート未反映 (出社時デプロイ前) の間は明示エラーになる。
 */
export async function setItemVisibility(draftId, { hide, actor = null } = {}) {
  const db = getDB();
  const draft = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(draftId);
  if (!draft) return { ok: false, error: 'draft_not_found' };
  const rk = db.prepare('SELECT * FROM draft_rakuten WHERE draft_id = ?').get(draftId);
  if (!rk?.registered_at) {
    return { ok: false, error: 'このアプリから楽天に登録した商品だけ公開切替できます (先に「楽天に登録」)' };
  }
  const mn = String(draft.ne_code).trim().toLowerCase();
  const r = await callWarehouse(
    `/service-api/rakuten-rms/items/manage-numbers/${encodeURIComponent(mn)}/visibility`,
    { method: 'POST', body: { hide: hide === true } },
  );
  if (r.status === 404 && !r.data) {
    // ルート自体が無い (express の HTML 404)。RMS の 404 は JSON で返るので区別できる
    return { ok: false, error: 'miniPC 側の公開切替ルートが未反映です (miniPC で git pull + Restart-Service — 出社時対応)' };
  }
  if (r.status === 200) {
    db.prepare(`UPDATE draft_rakuten SET published_at = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE draft_id = ?`)
      .run(hide === true ? null : new Date().toISOString(), draftId);
    logEvent(db, draftId, hide === true ? 'rakuten_unpublished' : 'rakuten_published', mn, actor);
    return { ok: true, hidden: hide === true };
  }
  const errText = r.data?.message || extractRmsErrors(r.data) || `HTTP ${r.status}`;
  logEvent(db, draftId, 'rakuten_visibility_failed', String(errText).slice(0, 500), actor);
  return { ok: false, error: errText };
}
