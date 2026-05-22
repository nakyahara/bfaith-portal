/**
 * 梱包機振り分け・配送方法判定 — サービス層 (取込/判定/編集/出力/マスタ)
 */
import crypto from 'node:crypto';
import { getMirrorDB } from '../warehouse-mirror/db.js';
import {
  ensureSchema, utcIsoNow, norm, buildSkuKey, normProductCode,
  finalizeLine, classifyOrder, shippingMethodMap, mallGroupOf,
  saveAssortDecision, comboKeyOf, listUnregistered, mirrorFreshness,
  COL,
} from './db.js';
import { parseNeCsv, buildNeCsv } from './csv.js';

const BATCH_TTL_HOURS = 48; // 揮発: 取込から48h で自動削除対象

function vErr(message, detail) {
  const e = new Error(message); e.code = 'VALIDATION'; if (detail) e.detail = detail; return e;
}

// name_csv → code の逆引き
function methodNameToCode(smMap) {
  const m = new Map();
  for (const sm of smMap.values()) m.set(sm.name_csv, sm.code);
  return m;
}

// ───────────────────────── 取込 + 判定 ─────────────────────────

export function importCsv(buffer, filename, user) {
  ensureSchema();
  const parsed = parseNeCsv(buffer);
  if (!parsed.ok) throw vErr('CSV ヘッダ検証に失敗しました', { errors: parsed.errors });
  const { header, rows } = parsed;
  const db = getMirrorDB();
  const smMap = shippingMethodMap();
  const nameToCode = methodNameToCode(smMap);

  // 行を構造化
  const lines = rows.map((r, idx) => {
    const productCode = normProductCode(r[COL.productCode]);
    const sku_key = buildSkuKey(r[COL.productCode], r[COL.colorName], r[COL.sizeName]);
    // 厳密パース: 数字のみ受理。"1abc"/"08x" 等は 0 → 有効行から除外され fail-safe で要判断へ。
    const qtyRaw = String(r[COL.qty] ?? '').trim();
    const qtyNum = /^\d+$/.test(qtyRaw) ? parseInt(qtyRaw, 10) : 0;
    return {
      row_no: idx,
      shop_name: norm(r[COL.shop]),
      order_no: norm(r[COL.orderNo]),
      mall_group: mallGroupOf(r[COL.shop]),
      product_code: productCode,
      sku_key,
      qty: Number.isFinite(qtyNum) ? qtyNum : 0,
      pref: norm(r[COL.pref]),
      cur_method_code: nameToCode.get(r[COL.shippingMethod]) || null,
      raw: r,
    };
  });

  // 伝票(shop+注文番号)でグループ化
  const groups = new Map();
  for (const l of lines) {
    const key = l.shop_name + '' + l.order_no;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(l);
  }

  const batch_id = crypto.randomUUID();
  const now = utcIsoNow();
  const expires = new Date(Date.now() + BATCH_TTL_HOURS * 3600 * 1000).toISOString();

  const insLine = db.prepare(`INSERT INTO pd_import_line
    (batch_id,row_no,order_no,shop_name,mall_group,product_code,sku_key,qty,pref,
     order_type,shipping_method_code,packing_machine_code,row_status,reason_code,raw_cols)
    VALUES (@batch_id,@row_no,@order_no,@shop_name,@mall_group,@product_code,@sku_key,@qty,@pref,
     @order_type,@shipping_method_code,@packing_machine_code,@row_status,@reason_code,@raw_cols)`);

  const tx = db.transaction(() => {
    db.prepare(`INSERT INTO pd_import_batch
      (batch_id,filename,uploaded_by,uploaded_at,status,row_count,order_count,header_json,expires_at)
      VALUES (?,?,?,?, 'classifying', ?, ?, ?, ?)`)
      .run(batch_id, filename || null, user || null, now, lines.length, groups.size, JSON.stringify(header), expires);

    for (const g of groups.values()) {
      const d = classifyOrder(g, smMap);
      for (const l of g) {
        insLine.run({
          batch_id, row_no: l.row_no, order_no: l.order_no, shop_name: l.shop_name,
          mall_group: l.mall_group, product_code: l.product_code, sku_key: l.sku_key,
          qty: l.qty, pref: l.pref, order_type: d.order_type,
          shipping_method_code: d.shipping_method_code, packing_machine_code: d.packing_machine_code,
          row_status: d.row_status, reason_code: d.reason_code,
          raw_cols: JSON.stringify(l.raw),
        });
      }
    }
  });
  tx();
  purgeExpiredBatches();
  return batchSummary(batch_id);
}

// ───────────────────────── バッチ参照 ─────────────────────────

export function batchSummary(batch_id) {
  const db = ensureSchema();
  const b = db.prepare(`SELECT * FROM pd_import_batch WHERE batch_id=?`).get(batch_id);
  if (!b) return null;
  const byStatus = db.prepare(`SELECT row_status, COUNT(*) c FROM pd_import_line WHERE batch_id=? GROUP BY row_status`).all(batch_id);
  const byReason = db.prepare(`SELECT reason_code, COUNT(*) c FROM pd_import_line WHERE batch_id=? GROUP BY reason_code`).all(batch_id);
  const counts = {}; for (const r of byStatus) counts[r.row_status] = r.c;
  const reasons = {}; for (const r of byReason) reasons[r.reason_code || 'none'] = r.c;
  return { batch: b, counts, reasons };
}

// 注文単位の一覧 (伝票ごとに1行に集約。要判断を優先表示)
export function listOrders(batch_id, filter = {}) {
  const db = ensureSchema();
  // 注文単位の状態 = 要判断が1件でもあれば要判断 / 次に確定 / それ以外 auto
  // 配送方法/梱包機/種別/モール/都道府県/理由 は伝票内で一律(classify は伝票単位で同値を全行に付与)。
  // SQLite の bare column 不定値を避けるため明示的に MAX() で代表値を取る。
  const rows = db.prepare(`
    SELECT order_no, shop_name, MAX(mall_group) AS mall_group, MAX(order_type) AS order_type, MAX(pref) AS pref,
           CASE
             WHEN SUM(row_status='要判断')>0 THEN '要判断'
             WHEN SUM(row_status='確定')>0   THEN '確定'
             ELSE 'auto'
           END AS row_status,
           MAX(shipping_method_code) AS shipping_method_code,
           MAX(packing_machine_code) AS packing_machine_code,
           MAX(reason_code) AS reason_code,
           COUNT(*) AS line_count, SUM(qty) AS total_qty,
           GROUP_CONCAT(product_code || ' x' || qty, ' / ') AS items
      FROM pd_import_line
     WHERE batch_id=?
     GROUP BY order_no, shop_name
     ORDER BY (CASE
                 WHEN SUM(row_status='要判断')>0 THEN 0
                 WHEN SUM(row_status='確定')>0   THEN 1
                 ELSE 2 END), order_no
     LIMIT 5000
  `).all(batch_id);
  return filter.status ? rows.filter((r) => r.row_status === filter.status) : rows;
}

// ───────────────────────── 注文の判断確定 / 編集 ─────────────────────────

/**
 * 1伝票の配送方法・梱包機を確定。order_type='assort' かつ learn=true なら ③ にも学習登録。
 */
export function decideOrder(batch_id, shop_name, order_no, { shipping_method_code, packing_machine_code, learn }, user) {
  const db = ensureSchema();
  const batch = db.prepare(`SELECT status FROM pd_import_batch WHERE batch_id=?`).get(batch_id);
  if (!batch) throw vErr('バッチが見つかりません');
  if (batch.status === 'exported') throw vErr('出力済みのバッチは編集できません');
  const lines = db.prepare(`SELECT * FROM pd_import_line WHERE batch_id=? AND shop_name=? AND order_no=?`)
    .all(batch_id, shop_name, order_no);
  if (!lines.length) throw vErr('対象の伝票が見つかりません');
  if (lines.some((l) => l.shipping_method_code === 'aes' || l.reason_code === 'aes_locked')) {
    throw vErr('AES の伝票は変更対象外です');
  }
  const fin = finalizeLine(shipping_method_code, packing_machine_code); // 整合矯正(ネコポス以外→manual)
  const now = utcIsoNow();
  const tx = db.transaction(() => {
    db.prepare(`UPDATE pd_import_line
        SET shipping_method_code=?, packing_machine_code=?, row_status='確定', reason_code='manual_decided'
      WHERE batch_id=? AND shop_name=? AND order_no=?`)
      .run(fin.shipping_method_code, fin.packing_machine_code, batch_id, shop_name, order_no);

    if (learn && lines[0].order_type === 'assort') {
      // 同一SKU合算した明細で学習
      const bySku = new Map();
      for (const l of lines) {
        if (!l.sku_key || !(l.qty > 0)) continue;
        bySku.set(l.sku_key, (bySku.get(l.sku_key) || 0) + l.qty);
      }
      const items = [...bySku.entries()].map(([sku_key, qty]) => ({ sku_key, qty }));
      const detail = items.map((i) => `${i.sku_key} x${i.qty}`);
      saveAssortDecision({ items, combo_detail: detail, shipping_method_code: fin.shipping_method_code, packing_machine_code: fin.packing_machine_code }, user);
    }
  });
  tx();
  return { ok: true };
}

// ───────────────────────── 出力 (ゲート + CAS) ─────────────────────────

export function exportCsv(batch_id, user) {
  const db = ensureSchema();
  const b = db.prepare(`SELECT * FROM pd_import_batch WHERE batch_id=?`).get(batch_id);
  if (!b) throw vErr('バッチが見つかりません');
  const pending = db.prepare(`SELECT COUNT(*) c FROM pd_import_line WHERE batch_id=? AND row_status='要判断'`).get(batch_id).c;
  if (pending > 0) throw vErr(`未判断が ${pending} 件あります。すべて確定してから出力してください。`, { pending });

  const lineRows = db.prepare(`SELECT * FROM pd_import_line WHERE batch_id=? ORDER BY row_no`).all(batch_id);
  if (lineRows.length !== b.row_count) throw vErr('行数不整合のため出力中止 (取込時と異なります)');

  const smMap = shippingMethodMap();
  // 元 raw を1行目=ヘッダとして復元するため、row_no 0..n を raw_cols から再構築 + ヘッダは別途必要
  // raw_cols はデータ行のみ。ヘッダは保持していないので、出力ヘッダは取込時のヘッダを使う必要がある。
  const headerRow = JSON.parse(b.header_json || 'null');
  const outRows = lineRows.map((l) => {
    const raw = JSON.parse(l.raw_cols);
    if (l.shipping_method_code && l.shipping_method_code !== 'aes') {
      const fin = finalizeLine(l.shipping_method_code, l.packing_machine_code, smMap);
      for (const [idx, val] of Object.entries(fin.cols)) raw[Number(idx)] = val;
    }
    return raw;
  });

  // CAS で exported へ (二重出力防止)
  const cas = db.prepare(`UPDATE pd_import_batch SET status='exported' WHERE batch_id=? AND status IN ('classifying','locked_for_export')`)
    .run(batch_id);
  if (cas.changes === 0) throw vErr('このバッチは既に出力済みか、状態が変わっています');

  const buf = buildNeCsv(headerRow, outRows);
  audit('export', batch_id, { rows: outRows.length }, user);
  return { buffer: buf, rowCount: outRows.length };
}

function audit(action, target, detail, who) {
  try {
    getMirrorDB().prepare(`INSERT INTO pd_audit_log (ts,who,action,target,detail) VALUES (?,?,?,?,?)`)
      .run(utcIsoNow(), who || null, action, target || null, detail ? JSON.stringify(detail) : null);
  } catch (e) { console.error('[packing] audit', e.message); }
}

// ───────────────────────── マスタ② CRUD (区間連続性検証) ─────────────────────────

export function listRules(productCode) {
  const db = ensureSchema();
  const pc = normProductCode(productCode);
  return db.prepare(`SELECT * FROM pd_shipping_rule WHERE product_code=? ORDER BY sku_key, mall_group, qty_min`).all(pc);
}

/**
 * (sku_key, mall_group) の区間ルールを全置換。tiers は qty_min 昇順で 1 から連続・最終 qty_max は NULL(無限)。
 * tiers: [{ qty_min, qty_max(null可), shipping_method_code, packing_machine_code }]
 */
export function upsertRules({ product_code, color_name, size_name, mall_group, tiers }, user) {
  const db = ensureSchema();
  const pc = normProductCode(product_code);
  if (!pc) throw vErr('商品コードが必要です');
  const mg = (mall_group === 'amazon') ? 'amazon' : 'rakuten';
  const sku_key = buildSkuKey(product_code, color_name, size_name);
  if (!Array.isArray(tiers) || tiers.length === 0) throw vErr('数量帯を1つ以上指定してください');

  const sorted = [...tiers].sort((a, b) => a.qty_min - b.qty_min);
  // 連続性検証
  let expectMin = 1;
  for (let i = 0; i < sorted.length; i++) {
    const t = sorted[i];
    if (!Number.isInteger(t.qty_min) || t.qty_min !== expectMin) {
      throw vErr(`数量帯が1から連続していません (${i + 1}段目の下限が ${t.qty_min}、期待 ${expectMin})`);
    }
    const isLast = i === sorted.length - 1;
    if (isLast) {
      if (t.qty_max != null) throw vErr('最終段の上限は無制限(空)にしてください (穴を作らないため)');
    } else {
      if (!Number.isInteger(t.qty_max) || t.qty_max < t.qty_min) throw vErr(`${i + 1}段目の上限が不正です`);
      expectMin = t.qty_max + 1;
    }
    finalizeLine(t.shipping_method_code, t.packing_machine_code); // コード妥当性 + 整合の事前検証
  }

  const now = utcIsoNow();
  const tx = db.transaction(() => {
    db.prepare(`DELETE FROM pd_shipping_rule WHERE sku_key=? AND mall_group=?`).run(sku_key, mg);
    const ins = db.prepare(`INSERT INTO pd_shipping_rule
      (sku_key,product_code,mall_group,qty_min,qty_max,shipping_method_code,packing_machine_code,updated_at,updated_by)
      VALUES (?,?,?,?,?,?,?,?,?)`);
    for (const t of sorted) {
      const fin = finalizeLine(t.shipping_method_code, t.packing_machine_code);
      ins.run(sku_key, pc, mg, t.qty_min, t.qty_max ?? null, fin.shipping_method_code, fin.packing_machine_code, now, user || null);
    }
  });
  tx();
  audit('rule_upsert', sku_key, { mall_group: mg, tiers: sorted.length }, user);
  return { sku_key, mall_group: mg };
}

// コピー: from(sku_key,mall_group) のルールを別バリアントへ複製 (登録時コピー機能)
export function copyRules({ from_sku_key, from_mall_group, to_product_code, to_color_name, to_size_name, to_mall_group }, user) {
  const db = ensureSchema();
  const src = db.prepare(`SELECT * FROM pd_shipping_rule WHERE sku_key=? AND mall_group=? ORDER BY qty_min`).all(from_sku_key, from_mall_group);
  if (!src.length) throw vErr('コピー元のルールがありません');
  const tiers = src.map((r) => ({ qty_min: r.qty_min, qty_max: r.qty_max, shipping_method_code: r.shipping_method_code, packing_machine_code: r.packing_machine_code }));
  return upsertRules({ product_code: to_product_code, color_name: to_color_name, size_name: to_size_name, mall_group: to_mall_group, tiers }, user);
}

// ───────────────────────── TTL ─────────────────────────

export function purgeExpiredBatches() {
  const db = ensureSchema();
  const now = utcIsoNow();
  const expired = db.prepare(`SELECT batch_id FROM pd_import_batch WHERE expires_at IS NOT NULL AND expires_at < ?`).all(now);
  if (!expired.length) return 0;
  const tx = db.transaction(() => {
    for (const e of expired) {
      db.prepare(`DELETE FROM pd_import_line WHERE batch_id=?`).run(e.batch_id);
      db.prepare(`DELETE FROM pd_import_batch WHERE batch_id=?`).run(e.batch_id);
    }
  });
  tx();
  return expired.length;
}

export { listUnregistered, mirrorFreshness };
