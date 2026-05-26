/**
 * ギフトセット組み依頼 — データ層 (Render 完結)
 *
 * warehouse-mirror.db の f_giftset / f_giftset_components を扱う。
 * 接続は warehouse-mirror の getMirrorDB() を共有 (foreign_keys=ON / busy_timeout=5000)。
 * 商品コードの検証・候補は同 DB の mirror_products (m_products のミラー) を参照。
 *
 * ミニPC は一切使わない。
 */
import crypto from 'node:crypto';
import { getMirrorDB } from '../warehouse-mirror/db.js';

// ─── 日付ユーティリティ (UTC 環境でも JST 日付が崩れないように Intl を使う) ───
const JST_DATE_FORMATTER = new Intl.DateTimeFormat('en-CA', {
  timeZone: 'Asia/Tokyo', year: 'numeric', month: '2-digit', day: '2-digit',
});
export function getJstDateString(date = new Date()) {
  return JST_DATE_FORMATTER.format(date); // 'YYYY-MM-DD'
}
export function utcIsoNow() {
  return new Date().toISOString();
}

// ─── giftset_code 自動採番 (衝突回避のため ms + 乱数まで含める) ───
function genGiftsetCode() {
  const ms = Date.now().toString(36);
  const rnd = crypto.randomBytes(4).toString('hex');
  return `gs_${ms}_${rnd}`;
}

// ─── 商品コード正規化 (大文字小文字/前後空白の揺れを吸収) ───
function normCode(code) {
  return String(code == null ? '' : code).trim();
}

// ─── 商品候補 (登録画面のオートコンプリート用) ───
export function suggestProducts(q) {
  const term = normCode(q);
  if (term.length < 1) return [];
  const like = `%${term}%`;
  const db = getMirrorDB();
  return db.prepare(
    `SELECT 商品コード, 商品名, 取扱区分
       FROM mirror_products
      WHERE 商品コード LIKE ? OR 商品名 LIKE ?
      ORDER BY (取扱区分 = '取扱中') DESC, 商品コード
      LIMIT 20`
  ).all(like, like);
}

// ─── 商品コードを正本(mirror_products)で解決。存在しなければ null ───
export function resolveProduct(code) {
  const c = normCode(code);
  if (!c) return null;
  const db = getMirrorDB();
  return db.prepare(
    `SELECT 商品コード, 商品名
       FROM mirror_products
      WHERE lower(trim(商品コード)) = lower(?)
      LIMIT 1`
  ).get(c) || null;
}

// ─── ギフトセット一覧 (有効分のみ、構成品数つき) ───
export function listGiftsets() {
  const db = getMirrorDB();
  return db.prepare(
    `SELECT g.giftset_code, g.giftset_name, g.unit_price, g.photo_url, g.video_url,
            g.production_lot_size,
            g.manual_url, g.manual_source, g.estimated_duration_min, g.difficulty,
            COUNT(c.商品コード) AS component_count
       FROM f_giftset g
       LEFT JOIN f_giftset_components c ON c.giftset_code = g.giftset_code
      WHERE g.is_active = 1
      GROUP BY g.giftset_code
      ORDER BY g.giftset_name`
  ).all();
}

// ─── ギフトセット1件 (ヘッダー + 構成品) ───
export function getGiftset(code) {
  const c = normCode(code);
  if (!c) return null;
  const db = getMirrorDB();
  const header = db.prepare(
    `SELECT * FROM f_giftset WHERE giftset_code = ? AND is_active = 1`
  ).get(c);
  if (!header) return null;
  const components = db.prepare(
    `SELECT 商品コード, 商品名, 数量, sort_order
       FROM f_giftset_components
      WHERE giftset_code = ?
      ORDER BY sort_order, 商品コード`
  ).all(c);
  return { ...header, components };
}

// マニュアル URL は http/https のみ受け入れる (XSS / javascript: スキーム回避)。
//   Notion / Drive とも https URL なので十分。
//   Codex R1 low #2: URL() コンストラクタで完全なパース検証も追加 (前方一致だけだと不正な URL が混入する可能性)。
const MANUAL_URL_MAX = 2000; // 一般的な URL 長上限
function validateManualUrl(raw) {
  const s = String(raw == null ? '' : raw).trim();
  if (!s) return { ok: true, value: null };
  if (s.length > MANUAL_URL_MAX) return { ok: false };
  // スキームを厳格に http/https 限定 (javascript:, data:, file: 等を排除)
  if (!/^https?:\/\//i.test(s)) return { ok: false };
  // URL() コンストラクタで完全パース (host が空、不正文字等を弾く)。Node 16+ で WHATWG 仕様。
  try {
    const u = new URL(s);
    if (u.protocol !== 'http:' && u.protocol !== 'https:') return { ok: false };
    if (!u.host) return { ok: false };
  } catch {
    return { ok: false };
  }
  return { ok: true, value: s };
}

// manual_source 列挙値の中心定義 (Codex R1 nit #3)。
//   ここを更新する場合は以下も同期する (検索キー: MANUAL_SOURCE):
//     - apps/warehouse-mirror/db.js の CHECK 句および起動時整合性チェック
//     - apps/giftset-assembly/views/index.html の <select id="giftManualSource"> オプション
//     - apps/giftset-assembly/notion.js の srcLabel 分岐
export const MANUAL_SOURCES = ['notion', 'drive', 'internal'];
export const MANUAL_SOURCE_LABELS = {
  notion: 'Notion',
  drive: 'Google Drive',
  internal: '社内マニュアル',
};
export function manualSourceLabel(src) {
  return MANUAL_SOURCE_LABELS[src] || 'マニュアル';
}
const MANUAL_SOURCE_VALUES = new Set(MANUAL_SOURCES);

/**
 * ギフトセットの登録/更新。
 * payload: { giftset_code?, giftset_name, photo_url?, video_url?, unit_price?, memo?,
 *            production_lot_size?, manual_url?, manual_source?,
 *            estimated_duration_min?, difficulty?,
 *            components: [{ code, qty }] }
 * 構成品コードは mirror_products に存在することを必須検証。
 *
 * production_lot_size (任意、デフォルト 1):
 *   1ロットあたりの完成個数。半端な箱開封ができない構成品 (例: agneyemask5 を
 *   ばらして使う relaxgiftset は 5個ロット必須) のための制約。構成品の数量は
 *   「1ロット分」で登録する。依頼時の qty は production_lot_size の倍数を強制。
 *
 * manual_url / manual_source (任意、両方セットか両方未設定):
 *   製造マニュアルの保管先 URL (http/https のみ) と種別 ('notion'|'drive'|'internal')。
 *   委託先 (いろは施設) が見るマニュアル。Notion 卒業時は manual_source で切替可能。
 *
 * estimated_duration_min (任意): 1ロット製造の目安時間 (分、1-100000 整数)。
 * difficulty (任意): 1-5 の難易度。
 *
 * @returns { giftset_code }
 * @throws Error (err.code = 'VALIDATION', err.detail に詳細)
 */
export function upsertGiftset(payload, user) {
  const name = String(payload?.giftset_name || '').trim();
  const errs = [];
  if (!name) errs.push('giftset_name');

  // unit_price: 任意。数値なら 0 以上。
  let unitPrice = null;
  if (payload?.unit_price !== undefined && payload?.unit_price !== null && payload?.unit_price !== '') {
    const n = Number(payload.unit_price);
    if (!Number.isFinite(n) || n < 0) errs.push('unit_price');
    else unitPrice = n;
  }

  // production_lot_size: 任意、未指定/null/'' は 1。1〜10000 の整数のみ。
  let lotSize = 1;
  const rawLot = payload?.production_lot_size;
  if (rawLot !== undefined && rawLot !== null && rawLot !== '') {
    const n = Number(rawLot);
    if (!Number.isInteger(n) || n < 1 || n > 10000) errs.push('production_lot_size');
    else lotSize = n;
  }

  // manual_url / manual_source: ペア検証 (片方だけはエラー)。URL は http/https のみ。
  const manualUrlRaw = payload?.manual_url;
  const manualSourceRaw = payload?.manual_source;
  let manualUrl = null;
  let manualSource = null;
  const hasUrlInput = manualUrlRaw !== undefined && manualUrlRaw !== null && String(manualUrlRaw).trim() !== '';
  const hasSourceInput = manualSourceRaw !== undefined && manualSourceRaw !== null && String(manualSourceRaw).trim() !== '';
  if (hasUrlInput) {
    const r = validateManualUrl(manualUrlRaw);
    if (!r.ok) errs.push('manual_url');
    else manualUrl = r.value;
  }
  if (hasSourceInput) {
    const s = String(manualSourceRaw).trim().toLowerCase();
    if (!MANUAL_SOURCE_VALUES.has(s)) errs.push('manual_source');
    else manualSource = s;
  }
  // ペア整合: 'どちらもある' or 'どちらもない' のみ許可。
  if ((manualUrl && !manualSource) || (!manualUrl && manualSource)) {
    errs.push('manual_url_source_pair');
  }

  // estimated_duration_min: 任意、整数 1-100000。
  let durationMin = null;
  const rawDur = payload?.estimated_duration_min;
  if (rawDur !== undefined && rawDur !== null && rawDur !== '') {
    const n = Number(rawDur);
    if (!Number.isInteger(n) || n < 1 || n > 100000) errs.push('estimated_duration_min');
    else durationMin = n;
  }

  // difficulty: 任意、整数 1-5。
  let difficulty = null;
  const rawDiff = payload?.difficulty;
  if (rawDiff !== undefined && rawDiff !== null && rawDiff !== '') {
    const n = Number(rawDiff);
    if (!Number.isInteger(n) || n < 1 || n > 5) errs.push('difficulty');
    else difficulty = n;
  }

  // 構成品の検証 + 正本解決 + 同一コードのマージ(数量合算)
  const rawComps = Array.isArray(payload?.components) ? payload.components : [];
  const merged = new Map(); // 正本商品コード -> { 商品コード, 商品名, 数量 }
  const invalid = [];
  for (const item of rawComps) {
    const code = normCode(item?.code);
    if (!code) continue; // 空行はスキップ
    const qtyNum = Number(item?.qty);
    if (!Number.isInteger(qtyNum) || qtyNum < 1 || qtyNum > 100000) {
      errs.push(`qty:${code}`);
      continue;
    }
    const resolved = resolveProduct(code);
    if (!resolved) { invalid.push(code); continue; }
    const key = resolved.商品コード;
    if (merged.has(key)) {
      merged.get(key).数量 += qtyNum;
    } else {
      merged.set(key, { 商品コード: resolved.商品コード, 商品名: resolved.商品名 || null, 数量: qtyNum });
    }
  }
  if (invalid.length) {
    const e = new Error('構成品コードが商品マスタに見つかりません: ' + invalid.join(', '));
    e.code = 'VALIDATION';
    e.detail = { invalidCodes: invalid };
    throw e;
  }
  if (merged.size === 0) errs.push('components');
  if (errs.length) {
    const e = new Error('入力エラー: ' + errs.join(', '));
    e.code = 'VALIDATION';
    e.detail = { fields: errs };
    throw e;
  }

  const db = getMirrorDB();
  const now = utcIsoNow();
  const code = normCode(payload?.giftset_code) || genGiftsetCode();
  const by = user || null;

  // 既存セットの lot_size 変更は禁止 (Codex review R1 high #1)。
  //   理由: 構成品.数量 は「1ロット分」として既に登録済み。lot_size を 1→5 等に変更すると
  //   既存数量の意味が変わり、出荷予定数が 1/5 等の誤計算になる。
  //   変更したい場合は: 削除 (deactivate) → 新規登録 (新 giftset_code) で運用。
  const existing = db.prepare(
    `SELECT production_lot_size FROM f_giftset WHERE giftset_code = ?`
  ).get(code);
  if (existing && Number.isInteger(existing.production_lot_size)
      && existing.production_lot_size !== lotSize) {
    const e = new Error(
      `既存ギフトセットの1ロット個数は変更できません (現在 ${existing.production_lot_size}、` +
      `変更後 ${lotSize})。lot_size を変えたい場合は一度削除して新規登録してください。`
    );
    e.code = 'VALIDATION';
    e.detail = { current_lot_size: existing.production_lot_size, requested_lot_size: lotSize };
    throw e;
  }

  const tx = db.transaction(() => {
    db.prepare(
      `INSERT INTO f_giftset
         (giftset_code, giftset_name, photo_url, video_url, unit_price, memo,
          production_lot_size, manual_url, manual_source,
          estimated_duration_min, difficulty,
          is_active, created_at, updated_at, created_by, updated_by)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
       ON CONFLICT(giftset_code) DO UPDATE SET
         giftset_name           = excluded.giftset_name,
         photo_url              = excluded.photo_url,
         video_url              = excluded.video_url,
         unit_price             = excluded.unit_price,
         memo                   = excluded.memo,
         production_lot_size    = excluded.production_lot_size,
         manual_url             = excluded.manual_url,
         manual_source          = excluded.manual_source,
         estimated_duration_min = excluded.estimated_duration_min,
         difficulty             = excluded.difficulty,
         is_active              = 1,
         updated_at             = excluded.updated_at,
         updated_by             = excluded.updated_by`
    ).run(
      code, name,
      payload?.photo_url ? String(payload.photo_url).trim() : null,
      payload?.video_url ? String(payload.video_url).trim() : null,
      unitPrice,
      payload?.memo ? String(payload.memo).trim() : null,
      lotSize,
      manualUrl, manualSource,
      durationMin, difficulty,
      now, now, by, by
    );

    // 構成品は全入れ替え (DELETE → INSERT)
    db.prepare(`DELETE FROM f_giftset_components WHERE giftset_code = ?`).run(code);
    const insComp = db.prepare(
      `INSERT INTO f_giftset_components
         (giftset_code, 商品コード, 数量, sort_order, 商品名, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?)`
    );
    let i = 0;
    for (const c of merged.values()) {
      insComp.run(code, c.商品コード, c.数量, i++, c.商品名, now, now);
    }
  });
  tx();
  return { giftset_code: code };
}

// ─── 論理削除 (一覧から外す。履歴は残す) ───
export function deactivateGiftset(code, user) {
  const c = normCode(code);
  if (!c) return { ok: false };
  const db = getMirrorDB();
  const info = db.prepare(
    `UPDATE f_giftset SET is_active = 0, updated_at = ?, updated_by = ?
      WHERE giftset_code = ? AND is_active = 1`
  ).run(utcIsoNow(), user || null, c);
  return { ok: info.changes > 0 };
}

/**
 * ロジザード貼り付け用の行を生成。
 * @returns { giftset, qty, lots, production_lot_size, rows: [{商品ID, 品質区分, 出荷予定数, 単価}] }
 * 品質区分は常に「良品」、単価は常に 0 (中原さん確定 2026-05-20)。
 *
 * 数量計算:
 *   構成品.数量 は「1ロット分」で登録されているため、出荷予定数 = 数量 × lots。
 *   lots = qty / production_lot_size (qty は production_lot_size の倍数必須)。
 *   production_lot_size = 1 (デフォルト) なら従来通り 出荷予定数 = 数量 × qty。
 */
export function buildPickingRows(code, qty) {
  const set = getGiftset(code);
  if (!set) { const e = new Error('ギフトセットが見つかりません'); e.code = 'NOT_FOUND'; throw e; }
  const n = Number(qty);
  if (!Number.isInteger(n) || n < 1 || n > 1000000) {
    const e = new Error('個数が不正です'); e.code = 'VALIDATION'; throw e;
  }
  // production_lot_size は ALTER TABLE 移行直後の既存行は DEFAULT 1 が入る (SQLite 仕様)。
  // 念のため null/undefined フォールバック。
  const lotSize = Number.isInteger(set.production_lot_size) && set.production_lot_size >= 1
    ? set.production_lot_size : 1;
  if (n % lotSize !== 0) {
    const e = new Error(
      `個数は ${lotSize} の倍数で入力してください (1ロット = ${lotSize}個。現在 ${n} は ${lotSize} で割り切れません)`
    );
    e.code = 'VALIDATION';
    e.detail = { qty: n, production_lot_size: lotSize };
    throw e;
  }
  const lots = n / lotSize;
  const rows = set.components.map((c) => ({
    商品ID: c.商品コード,
    品質区分: '良品',
    出荷予定数: c.数量 * lots,
    単価: 0,
  }));
  return {
    giftset: { giftset_code: set.giftset_code, giftset_name: set.giftset_name },
    qty: n,
    lots,
    production_lot_size: lotSize,
    rows,
  };
}
