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
  if (rk.shipping_method_group != null && String(rk.shipping_method_group).trim() !== ''
      && !SHIPPING_METHOD_GROUPS[String(rk.shipping_method_group).trim()]) {
    reasons.push('配送方法の指定が不正です');
  }
  if (rk.normal_delivery_date_id != null && String(rk.normal_delivery_date_id).trim() !== ''
      && !/^\d+$/.test(String(rk.normal_delivery_date_id).trim())) {
    reasons.push('納期情報IDは数字で入力してください (RMS の納期設定のID)');
  }
  if (reasons.length > 0) return { ok: false, reasons };

  const descParts = [];
  if (ai.desc_features) descParts.push(ai.desc_features);
  if (ai.desc_spec) descParts.push(ai.desc_spec);
  if (specs.length > 0) descParts.push(specs.map((s) => `${s.spec_key}: ${s.spec_value || ''}`.trim()).join('\n'));
  if (ai.desc_notes) descParts.push(ai.desc_notes);

  // JAN → カタログID属性の**自動付与はしない** (2026-07-28 本番検証で確定)。
  // zz- テスト商品での実測: 属性「カタログID」「JANコード」「GTIN」は IE1002
  // (ジャンル属性辞書に無い / genre 111145)、variant.catalogId / catalogIdExemptionReason は
  // IE0002 (フィールド自体が存在しない)。= 属性辞書は**ジャンルごと**で、無いジャンルに
  // 自動付与すると登録そのものが失敗する。
  // → カタログIDを属性で持つジャンルでは人が属性欄に手入力する (JAN欄との一致検証は上で実施済み)。
  //   Genre API (次の玉) で属性辞書を引けるようになったら「辞書にあるときだけ自動付与」に戻す。
  const attrs = attributes.slice();

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
    productDescription: { pc: descParts.join('\n\n') || title },
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
