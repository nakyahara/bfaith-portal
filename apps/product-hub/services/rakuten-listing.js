/**
 * P3: 楽天RMS 自動出品 (2026-07-26 権限smoke実証 → 実装)。
 *
 * スコープ (中原さん決定):
 *   - ジャンルID・商品属性は**手入力** (雛形方式は採らない)
 *   - 画像は Drive → R-Cabinet 自動転送
 *   - 登録は**倉庫指定 (非公開) のみ**。公開は人が RMS 画面で行う
 *     (miniPC 側 route が hideItem=true を強制するので、ここで細工しても公開はできない)
 *   - まずは**単品ページのみ** (バリエーションページの variants/selector 構成は P3.5)
 *
 * 経路: Render (この service) → miniPC /service-api/rakuten-rms/* (Cloudflare Tunnel)。
 * 楽天キーは miniPC にだけある。Render 側 env は RYS と共通の WAREHOUSE_* / CF_ACCESS_*。
 *
 * smoke で判明した RMS 2.0 の事実 (2026-07-26):
 *   - PUT /es/2.0/items/manage-numbers/{mn} → 201。genreId ごとに必須属性 (IE0418)
 *   - articleNumber は value か exemptionReason が必須 (IE0229)
 *   - 既存商品の attributes は GET の形のまま PUT に使える (= [{name, values:[..]}])
 */
import sharp from 'sharp';
import { google } from 'googleapis';

import { getDB, logEvent } from '../db.js';
import { resolveVariationGroup, getNeCost } from '../lib/variation.js';
// ⚠️ page-info.js はこのファイルの SHIPPING_METHOD_GROUPS を import する (循環参照)。
//    双方とも関数呼び出し時にしか使わないので ESM 的に安全。module 評価時に参照しないこと
import { validatePageInfo, buildPageInfoHtml, mapNeShippingToRakuten } from '../lib/page-info.js';

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
      fields: 'nextPageToken, files(id, name, mimeType)',
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

export async function getDriveThumbnail(fileId, width) {
  const key = `${fileId}:${width}`;
  const hit = thumbCache.get(key);
  if (hit) {
    if (Date.now() - hit.at <= THUMB_CACHE_TTL_MS) {
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
      if (res.ok && (res.headers.get('content-type') || '').startsWith('image/')) {
        const b = Buffer.from(await res.arrayBuffer());
        if (b.length <= THUMB_SOURCE_MAX_BYTES) raw = b;
      }
    } catch (_) {
      raw = null; // フォールバックへ
    }
  }
  if (!raw) {
    raw = await downloadDriveImage(fileId);
    if (raw.length > THUMB_SOURCE_MAX_BYTES) throw new Error('サムネイル生成対象の画像が大きすぎます');
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

const CABINET_DIR = 'app-newitems';

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
  const rkRow = db.prepare('SELECT white_bg_drive_file_id FROM draft_rakuten WHERE draft_id = ?').get(draftId);
  const whiteBgId = rkRow?.white_bg_drive_file_id || null;
  if (whiteBgId) images.push({ drive_file_id: whiteBgId, isWhiteBg: true });
  if (images.length === 0) return { ok: false, error: 'no_images' };

  const folderId = await ensureCabinetFolder(db);
  const results = [];
  let n = 0;
  for (const img of images) {
    n += 1;
    const done = db.prepare('SELECT cabinet_location FROM draft_cabinet_images WHERE draft_id = ? AND drive_file_id = ?')
      .get(draftId, img.drive_file_id);
    if (done) {
      results.push({ driveFileId: img.drive_file_id, outcome: 'already', location: done.cabinet_location });
      continue;
    }
    try {
      const raw = await downloadDriveImage(img.drive_file_id);
      const jpeg = await toCabinetJpeg(raw);
      // ファイル名は「商品コード-連番」(白抜きは -white)。同名は overWrite=true で置き換わる (再転送に安全)
      const baseName = String(draft.ne_code).toLowerCase().replace(/[^a-z0-9\-]/g, '-');
      const filePath = img.isWhiteBg ? `${baseName}-white.jpg` : `${baseName}-${n}.jpg`;
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
        INSERT INTO draft_cabinet_images (draft_id, drive_file_id, cabinet_location, cabinet_file_id)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(draft_id, drive_file_id) DO UPDATE SET
          cabinet_location = excluded.cabinet_location, cabinet_file_id = excluded.cabinet_file_id,
          uploaded_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
      `).run(draftId, img.drive_file_id, location, fileId ?? null);
      results.push({ driveFileId: img.drive_file_id, outcome: 'uploaded', location });
    } catch (e) {
      // Drive の権限エラーはここに落ちる (SA がフォルダに共有されていない等)。画像単位で報告
      results.push({ driveFileId: img.drive_file_id, outcome: 'failed', error: String(e.message || e).slice(0, 300) });
    }
  }
  const uploaded = results.filter((r) => r.outcome === 'uploaded').length;
  const failed = results.filter((r) => r.outcome === 'failed').length;
  logEvent(db, draftId, 'cabinet_transfer', `uploaded=${uploaded} failed=${failed}`, actor);
  return { ok: failed === 0, uploaded, failed, results };
}

// ─── 出品 payload ───

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

/** B-Faith 店舗の配送方法グループ (RYS rakuten-to-notion-draft.js の実測変換表と同一 ID) */
export const SHIPPING_METHOD_GROUPS = {
  '1': '定形外',
  '3': '飛脚宅配便',
  '4': '宅急便',
  '5': 'ネコポス',
  '6': 'クリックポスト',
  '7': 'ヤマト運輸宅急便',
  '8': '宅急便50サイズ以上',
  '9': 'ゆうパケットパフ',
};

/**
 * draft_yahoo.tax_rate ('8%' / '10%' の文字列) → RMS payment.taxRate。
 * RYS 実測 (2026-06-28): B-Faith 店舗は default 10% で、軽減税率 8% の商品だけ明示する運用。
 * → 8% のときだけ payment を送り、10%・未設定・想定外値は送らない (店舗デフォルトに任せる)
 */
export function taxRateToPayment(taxRateText) {
  const m = String(taxRateText || '').trim().match(/^(\d+)\s*%?$/);
  if (m && Number(m[1]) === 8) return { taxRate: 0.08 };
  return null;
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
  const cabinetAll = db.prepare('SELECT drive_file_id, cabinet_location FROM draft_cabinet_images WHERE draft_id = ? ORDER BY id').all(draftId);
  // 白抜き背景画像は images[] ではなく whiteBgImage で送る (検索サムネ用の別枠)
  const whiteBgId = rk.white_bg_drive_file_id || null;
  const whiteBg = whiteBgId ? cabinetAll.find((c) => c.drive_file_id === whiteBgId) || null : null;
  // 送る画像は「現在の draft_images」と転送履歴の JOIN (Codex R1 Medium-2:
  // 履歴だけから作ると、転送後に削除・差し替えた画像や旧白抜き画像が payload に残る)。
  // 並びも draft_images の sort に従う
  const cabinetByFile = new Map(cabinetAll.map((c) => [c.drive_file_id, c.cabinet_location]));
  const currentImages = db.prepare('SELECT drive_file_id FROM draft_images WHERE draft_id = ? ORDER BY sort, id').all(draftId)
    .filter((i) => i.drive_file_id !== whiteBgId);
  const cabinet = currentImages
    .filter((i) => cabinetByFile.has(i.drive_file_id))
    .map((i) => ({ cabinet_location: cabinetByFile.get(i.drive_file_id) }));
  const untransferredCount = currentImages.length - cabinet.length;

  const reasons = [];
  // 単品のみ (バリエーションページの variants/selector 構成は P3.5)
  const vari = resolveVariationGroup(db, draft.ne_code, { draftId, withMembers: false });
  if (vari.kind === 'variation' && vari.memberCount > 1) {
    reasons.push(`バリエーションページ (${vari.memberCount} SKU) の自動出品は未対応です (P3.5 で対応予定)`);
  }
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
  if (untransferredCount > 0) {
    reasons.push(`商品画像 ${untransferredCount} 枚が R-Cabinet に未転送です (「画像を転送」を実行)`);
  }
  if (cabinet.length > MAX_RAKUTEN_IMAGES) {
    reasons.push(`楽天の商品画像は最大 ${MAX_RAKUTEN_IMAGES} 枚です (現在 ${cabinet.length} 枚 — 減らしてください)`);
  }
  if (whiteBgId && !whiteBg) reasons.push('白抜き背景画像が R-Cabinet に未転送です (先に「画像を転送」)');
  // 税率は 8% / 10% / 空欄 (=店舗デフォルト10%) 以外を fail-closed で止める (Codex R1 Medium-1)
  const taxText = String(yahooRow.tax_rate ?? '').trim();
  if (taxText && !/^(8|10)\s*%?$/.test(taxText)) {
    reasons.push(`税率「${taxText}」が不正です (8% / 10% / 空欄のみ)`);
  }
  // JAN はチェックデジットまで検証し、手入力のカタログID属性と食い違ったら止める (Codex R1 Medium-4)。
  // カタログID属性は最大1件・値1個に限定し、その値自体も GTIN 検証する
  // (R2 Medium: 複数記述で不一致検査を迂回できた / JAN欄が空だと手入力値が未検証だった)
  const jan = String(draft.jan_code || '').trim();
  const catalogAttrs = Array.isArray(attributes) ? attributes.filter((a) => a.name === 'カタログID') : [];
  let manualCatalog = null;
  if (catalogAttrs.length > 1) {
    reasons.push('商品属性「カタログID」が複数あります (1件にまとめてください)');
  } else if (catalogAttrs.length === 1) {
    manualCatalog = catalogAttrs[0];
    if (manualCatalog.values.length !== 1) {
      reasons.push('商品属性「カタログID」の値は1個だけにしてください');
    } else if (!isValidGtin(manualCatalog.values[0])) {
      reasons.push(`商品属性のカタログID「${manualCatalog.values[0]}」の形式が不正です (8/12/13桁 + チェックデジット)`);
    }
  }
  if (jan) {
    if (!isValidGtin(jan)) {
      reasons.push(`JANコード「${jan}」の形式が不正です (8/12/13桁 + チェックデジット)`);
    } else if (manualCatalog && manualCatalog.values.length === 1 && manualCatalog.values[0] !== jan) {
      reasons.push(`商品属性のカタログID (${manualCatalog.values[0]}) と JAN欄 (${jan}) が一致しません — どちらかに揃えてください`);
    }
  }
  // ─── ジャンル属性辞書による事前検証 (2026-07-28 Genre API) ───
  // 辞書キャッシュがある場合だけ検証する (無ければ RMS が最終検証する)。
  // genre_id をキーに引くので、ジャンルを変えた直後は辞書未取得 = 検証スキップになる
  // 鮮度 24h 以内の辞書だけ検証に使う。古い/無い場合は検証スキップ (RMS が最終検証)。
  // 登録経路 (registerHidden/preview) は事前に fetchGenreAttributes で鮮度を回復させている
  const genreDict = (rk.genre_id && /^\d+$/.test(String(rk.genre_id).trim()))
    ? getCachedGenreAttributes(db, String(rk.genre_id).trim(), { maxAgeMs: GENRE_CACHE_TTL_MS })
    : null;
  let dictHasCatalogId = false;
  if (genreDict && Array.isArray(attributes)) {
    const dictByName = new Map(genreDict.attributes.map((a) => [a.name, a]));
    dictHasCatalogId = dictByName.has('カタログID');
    // ① 辞書に無い属性名は IE1002 で登録が失敗する → 送る前に止める
    for (const a of attributes) {
      if (!dictByName.has(a.name)) {
        reasons.push(`属性「${a.name}」はジャンル「${genreDict.genreName || rk.genre_id}」の属性辞書にありません (登録エラー IE1002 になります)`);
      }
    }
    // ② 必須属性の欠落 (カタログIDは JAN欄からの自動付与があるので別扱い)
    const presentNames = new Set(attributes.map((a) => a.name));
    for (const da of genreDict.attributes) {
      if (!da.mandatory || presentNames.has(da.name)) continue;
      if (da.name === 'カタログID' && jan) continue; // 下で自動付与する
      reasons.push(`必須属性「${da.name}」が未入力です (ジャンル ${genreDict.genreName || rk.genre_id} の必須)`);
    }
    // ③ 値の数・長さの軽い検証 (RMS と同じ基準で早めに教える)
    for (const a of attributes) {
      const da = dictByName.get(a.name);
      if (!da) continue;
      if (da.multiValueLimit && a.values.length > da.multiValueLimit) {
        reasons.push(`属性「${a.name}」の値は最大 ${da.multiValueLimit} 個です`);
      }
      if (da.maxLength) {
        for (const v of a.values) {
          if (String(v).length > da.maxLength) reasons.push(`属性「${a.name}」の値が長すぎます (上限 ${da.maxLength} 文字)`);
        }
      }
    }
  }

  if (rk.shipping_method_group != null && String(rk.shipping_method_group).trim() !== ''
      && !SHIPPING_METHOD_GROUPS[String(rk.shipping_method_group).trim()]) {
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

  // ─── 説明文3欄 (2026-08-01 店舗フォーマット確定) ───
  //   PC用商品説明文 (productDescription.pc) = 表1枚 (説明/注意事項/仕様表/商品ページ表記)
  //   PC用販売説明文 (salesDescription)      = 商品画像を width100% で並べた画像HTML
  //   スマホ用商品説明文 (productDescription.sp) = 販売説明文 + 商品説明文
  // 発送方法の表示名はアプリ指定の配送方法グループ > NE配送方法のマッピング の順で決める
  const pageShipGroup = String(rk.shipping_method_group ?? '').trim();
  const pageShippingLabel = pageShipGroup && SHIPPING_METHOD_GROUPS[pageShipGroup]
    ? SHIPPING_METHOD_GROUPS[pageShipGroup]
    : mapNeShippingToRakuten(db, getNeCost(db, draft.ne_code)?.shippingMethod).label;
  // 「説明」行 = 商品名 + AI特徴 + AI仕様 (仕様表・注意書きは表の別行に載る)
  const descTexts = [title];
  if (ai.desc_features) descTexts.push(String(ai.desc_features).trim());
  if (ai.desc_spec) descTexts.push(String(ai.desc_spec).trim());
  const pcHtml = buildPageInfoHtml({
    productName: title,
    info: pageInfo, // 未保存 (null) でも説明/注意事項/仕様表/広告文責の行は載る
    shippingLabel: pageShippingLabel,
    descriptionText: descTexts.join('\n\n'),
    notesText: ai.desc_notes ? String(ai.desc_notes).trim() : null,
    specs,
  });
  const salesHtml = buildSalesDescriptionHtml(cabinet.map((c) => c.cabinet_location));
  const spHtml = [salesHtml, pcHtml].filter(Boolean).join('\n');
  if (pcHtml.length > DESC_LIMIT) reasons.push(`PC用商品説明文が長すぎます (${pcHtml.length}字 / 上限${DESC_LIMIT}字)`);
  if (salesHtml.length > DESC_LIMIT) reasons.push(`PC用販売説明文 (画像HTML) が長すぎます (${salesHtml.length}字 / 上限${DESC_LIMIT}字 — 画像を減らしてください)`);
  if (spHtml.length > DESC_LIMIT) reasons.push(`スマホ用商品説明文 (販売説明文+商品説明文) が長すぎます (${spHtml.length}字 / 上限${DESC_LIMIT}字)`);
  if (reasons.length > 0) return { ok: false, reasons };

  // JAN → カタログID属性の自動付与は**ジャンル属性辞書にあるときだけ** (2026-07-28 確定)。
  // 実測: 「カタログID」が辞書に無いジャンル (111145等) へ付与すると IE1002 で登録自体が失敗する。
  // 辞書未取得のジャンルでは付与しない (安全側。必要なら「ジャンル情報を取得」してから登録する)
  const attrs = attributes.slice();
  if (dictHasCatalogId && jan && !manualCatalog) {
    attrs.push({ name: 'カタログID', values: [jan] });
  }

  // 送料・配送方法 (variants[].shipping)。未設定の項目は送らず店舗デフォルトに任せる
  const shippingGroup = String(rk.shipping_method_group ?? '').trim();
  const shipping = {
    ...(shippingGroup ? { shippingMethodGroup: shippingGroup } : {}),
    ...(rk.postage_included != null ? { postageIncluded: rk.postage_included === 1 } : {}),
  };
  const deliveryDateId = String(rk.normal_delivery_date_id ?? '').trim();

  const payment = taxRateToPayment(yahooRow.tax_rate);

  const payload = {
    title,
    ...(ai.desc_catch ? { tagline: String(ai.desc_catch).trim() } : {}),
    productDescription: { pc: pcHtml || title, ...(spHtml ? { sp: spHtml } : {}) },
    ...(salesHtml ? { salesDescription: salesHtml } : {}),
    genreId: String(rk.genre_id).trim(),
    hideItem: true, // 登録は常に非公開。公開はアプリの「公開」ボタン (setItemVisibility) で行う
    itemType: 'NORMAL',
    images: cabinet.map((c) => ({ type: 'CABINET', location: c.cabinet_location })),
    ...(whiteBg ? { whiteBgImage: { type: 'CABINET', location: whiteBg.cabinet_location } } : {}),
    ...(payment ? { payment } : {}),
    variants: {
      [draft.ne_code]: {
        standardPrice: draft.price,
        articleNumber: rk.article_number && String(rk.article_number).trim() !== ''
          ? { value: String(rk.article_number).trim() }
          : { exemptionReason: 1 },
        ...(attrs.length > 0 ? { attributes: attrs } : {}),
        ...(Object.keys(shipping).length > 0 ? { shipping } : {}),
        ...(deliveryDateId ? { normalDeliveryDateId: Number(deliveryDateId) } : {}),
      },
    },
  };
  return { ok: true, payload, draft };
}

/** RMS のエラー本文から人が読める文を取り出す */
export function extractRmsErrors(data) {
  if (data == null) return '';
  if (typeof data === 'string') return data.slice(0, 800);
  const list = Array.isArray(data?.errors) ? data.errors : null;
  if (list) {
    return list.map((e) => `${e.code || ''}: ${e.message || ''}${e.metadata ? ' ' + JSON.stringify(e.metadata) : ''}`).join('\n').slice(0, 1500);
  }
  return JSON.stringify(data).slice(0, 800);
}

/**
 * 非公開 (倉庫指定) で楽天に登録する。
 * 既存商品の上書きは miniPC 側が 409 で拒否する (稼働中ページを潰さない)。
 */
export async function registerHidden(draftId, { actor = null } = {}) {
  const db = getDB();
  // 登録直前に辞書の鮮度を回復する (24h キャッシュ内なら通信なし)。
  // 失敗しても登録は止めない — 辞書が無ければ検証スキップで RMS が最終検証する
  const rkRow = db.prepare('SELECT genre_id FROM draft_rakuten WHERE draft_id = ?').get(draftId);
  if (rkRow?.genre_id && /^\d+$/.test(String(rkRow.genre_id).trim())) {
    try { await fetchGenreAttributes(db, String(rkRow.genre_id).trim()); } catch (_) { /* best-effort */ }
  }
  const built = buildItemPayload(db, draftId);
  if (!built.ok) return { ok: false, reasons: built.reasons };
  const mn = String(built.draft.ne_code).trim().toLowerCase();

  const r = await callWarehouse(`/service-api/rakuten-rms/items/manage-numbers/${encodeURIComponent(mn)}`, {
    method: 'PUT', body: built.payload,
  });
  const saveResult = db.prepare(`
    INSERT INTO draft_rakuten (draft_id, registered_at, last_error)
    VALUES (?, ?, ?)
    ON CONFLICT(draft_id) DO UPDATE SET
      registered_at = COALESCE(excluded.registered_at, draft_rakuten.registered_at),
      last_error = excluded.last_error,
      updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
  `);

  if (r.status === 201 || r.status === 200) {
    saveResult.run(draftId, new Date().toISOString(), null);
    logEvent(db, draftId, 'rakuten_registered_hidden', mn, actor);
    // approved からの登録なら「楽天出品済み」へ進める (他ステータスからは変えない)
    const d = db.prepare('SELECT status FROM product_drafts WHERE id = ?').get(draftId);
    if (d?.status === 'approved') {
      db.prepare(`UPDATE product_drafts SET status = 'listed', updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id = ? AND status = 'approved'`).run(draftId);
    }
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
  saveResult.run(draftId, null, String(errText).slice(0, 1500));
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
    return { ok: false, error: 'このアプリから楽天に登録した商品だけ棚を反映できます (先に「非公開で登録」)' };
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
  const body = { categoryIds };
  // 複数のときは「メインの棚」= 先頭 (画面の並び順で最初に選ばれた棚) を指定する
  if (categoryIds.length > 1) body.mainPluralCategoryId = categoryIds[0];

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
    return { ok: false, error: 'このアプリから楽天に登録した商品だけ公開切替できます (先に「非公開で登録」)' };
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
