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

/**
 * ギフトセットの登録/更新。
 * payload: { giftset_code?, giftset_name, photo_url?, video_url?, unit_price?, memo?,
 *            components: [{ code, qty }] }
 * 構成品コードは mirror_products に存在することを必須検証。
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

  const tx = db.transaction(() => {
    db.prepare(
      `INSERT INTO f_giftset
         (giftset_code, giftset_name, photo_url, video_url, unit_price, memo,
          is_active, created_at, updated_at, created_by, updated_by)
       VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
       ON CONFLICT(giftset_code) DO UPDATE SET
         giftset_name = excluded.giftset_name,
         photo_url    = excluded.photo_url,
         video_url    = excluded.video_url,
         unit_price   = excluded.unit_price,
         memo         = excluded.memo,
         is_active    = 1,
         updated_at   = excluded.updated_at,
         updated_by   = excluded.updated_by`
    ).run(
      code, name,
      payload?.photo_url ? String(payload.photo_url).trim() : null,
      payload?.video_url ? String(payload.video_url).trim() : null,
      unitPrice,
      payload?.memo ? String(payload.memo).trim() : null,
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
 * @returns { giftset, rows: [{商品ID, 品質区分, 出荷予定数, 単価}] }
 * 品質区分は常に「良品」、単価は常に 0 (中原さん確定 2026-05-20)。
 */
export function buildPickingRows(code, qty) {
  const set = getGiftset(code);
  if (!set) { const e = new Error('ギフトセットが見つかりません'); e.code = 'NOT_FOUND'; throw e; }
  const n = Number(qty);
  if (!Number.isInteger(n) || n < 1 || n > 1000000) {
    const e = new Error('個数が不正です'); e.code = 'VALIDATION'; throw e;
  }
  const rows = set.components.map((c) => ({
    商品ID: c.商品コード,
    品質区分: '良品',
    出荷予定数: c.数量 * n,
    単価: 0,
  }));
  return { giftset: { giftset_code: set.giftset_code, giftset_name: set.giftset_name }, qty: n, rows };
}
