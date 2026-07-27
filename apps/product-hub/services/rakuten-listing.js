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
import { resolveVariationGroup } from '../lib/variation.js';

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
      // ファイル名は「商品コード-連番」。同名は overWrite=true で置き換わる (再転送に安全)
      const filePath = `${String(draft.ne_code).toLowerCase().replace(/[^a-z0-9\-]/g, '-')}-${n}.jpg`;
      const up = await callWarehouse('/service-api/rakuten-rms/cabinet/upload', {
        method: 'POST',
        body: { folderId, filePath, fileName: `${draft.ne_code} ${n}`, fileBase64: jpeg.toString('base64') },
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

/**
 * 出品 payload を組み立てる。送れない状態なら reasons を返す (dry_run と live で共通)。
 */
export function buildItemPayload(db, draftId) {
  const draft = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(draftId);
  if (!draft) return { ok: false, reasons: ['ドラフトが見つかりません'] };
  const rk = db.prepare('SELECT * FROM draft_rakuten WHERE draft_id = ?').get(draftId) || {};
  const ai = {};
  for (const r of db.prepare('SELECT kind, content FROM draft_ai_outputs WHERE draft_id = ?').all(draftId)) {
    ai[r.kind] = r.content;
  }
  const specs = db.prepare('SELECT spec_key, spec_value FROM draft_specs WHERE draft_id = ? ORDER BY sort, id').all(draftId);
  const cabinet = db.prepare('SELECT cabinet_location FROM draft_cabinet_images WHERE draft_id = ? ORDER BY id').all(draftId);

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
  if (reasons.length > 0) return { ok: false, reasons };

  const descParts = [];
  if (ai.desc_features) descParts.push(ai.desc_features);
  if (ai.desc_spec) descParts.push(ai.desc_spec);
  if (specs.length > 0) descParts.push(specs.map((s) => `${s.spec_key}: ${s.spec_value || ''}`.trim()).join('\n'));
  if (ai.desc_notes) descParts.push(ai.desc_notes);

  const payload = {
    title,
    ...(ai.desc_catch ? { tagline: String(ai.desc_catch).trim() } : {}),
    productDescription: { pc: descParts.join('\n\n') || title },
    genreId: String(rk.genre_id).trim(),
    hideItem: true, // miniPC 側でも強制されるが、意図としてここでも明示
    itemType: 'NORMAL',
    images: cabinet.map((c) => ({ type: 'CABINET', location: c.cabinet_location })),
    variants: {
      [draft.ne_code]: {
        standardPrice: draft.price,
        articleNumber: rk.article_number && String(rk.article_number).trim() !== ''
          ? { value: String(rk.article_number).trim() }
          : { exemptionReason: 1 },
        ...(attributes.length > 0 ? { attributes } : {}),
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
    return { ok: true, manageNumber: mn, status: r.status };
  }

  const errText = r.data?.message || extractRmsErrors(r.data) || `HTTP ${r.status}`;
  saveResult.run(draftId, null, String(errText).slice(0, 1500));
  logEvent(db, draftId, 'rakuten_register_failed', String(errText).slice(0, 500), actor);
  return { ok: false, status: r.status, error: errText };
}
