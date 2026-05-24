/**
 * 梱包機振り分け・配送方法判定 — サービス層 (取込/判定/編集/出力/マスタ)
 */
import crypto from 'node:crypto';
import { getMirrorDB } from '../warehouse-mirror/db.js';
import {
  ensureSchema, utcIsoNow, norm, buildSkuKey, normProductCode,
  finalizeLine, classifyOrder, shippingMethodMap, mallGroupOf, isLineGift,
  saveAssortDecision, comboKeyOf, listUnregistered, mirrorFreshness,
  recordAssortUsage, comboKeysByOrderRef, getAssortByCombo, purgeOldUsage,
  COL, MALL_GROUPS, COMBO_KEY_VERSION,
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
      // アソートは combo_key と注文番号/受注番号を利用ログに記録(後で注文番号から学習を辿れるように)
      if (d.order_type === 'assort' && d.combo_key) {
        recordAssortUsage({ combo_key: d.combo_key, order_no: g[0].order_no, uketsuke_no: g[0].raw[COL.uketsuke], shop_name: g[0].shop_name });
      }
    }
  });
  tx();
  purgeExpiredBatches();
  purgeOldUsage();
  return batchSummary(batch_id);
}

// ───────────────────────── バッチ参照 ─────────────────────────

// 取込済みバッチの一覧 (新しい順、TTL内のみ)。画面を閉じても再表示できるようにする。
// 設計上わざとユーザーで絞っていない: 本アプリは requireAppAccess 配下の倉庫スタッフ専用で、
// 「アップロードしたデータを複数人で共同編集」が要件 (引き継ぎ書 §共同編集)。
// 同一ワークスペースを全員で共有する前提なので、batch は uploaded_by に関わらず一覧・再開できる。
export function listBatches(limit = 20) {
  const db = ensureSchema();
  const now = utcIsoNow();
  const lim = Number.isInteger(limit) && limit > 0 ? Math.min(limit, 100) : 20;
  return db.prepare(`
    SELECT b.batch_id, b.filename, b.uploaded_by, b.uploaded_at, b.status, b.row_count, b.order_count,
           (SELECT COUNT(*) FROM pd_import_line l WHERE l.batch_id=b.batch_id AND l.row_status='要判断') AS pending
      FROM pd_import_batch b
     WHERE b.expires_at IS NULL OR b.expires_at >= ?
     ORDER BY b.uploaded_at DESC
     LIMIT ?
  `).all(now, lim);
}

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

// 1伝票の詳細 (NE 個別伝票風の表示用)。order-level は先頭行の raw_cols から、商品名は各行から。
export function getOrderDetail(batch_id, shop_name, order_no) {
  const db = ensureSchema();
  const lines = db.prepare(`SELECT * FROM pd_import_line WHERE batch_id=? AND shop_name=? AND order_no=? ORDER BY row_no`)
    .all(batch_id, shop_name, order_no);
  if (!lines.length) return null;
  const raw0 = JSON.parse(lines[0].raw_cols || '[]');
  const items = lines.map((l) => {
    const r = JSON.parse(l.raw_cols || '[]');
    return { product_code: l.product_code, product_name: r[COL.productName] || '', qty: l.qty };
  });
  return {
    order_no, shop_name, mall_group: lines[0].mall_group, order_type: lines[0].order_type,
    recipient: raw0[COL.recipient] || '',
    pref: raw0[COL.pref] || '',
    delivery_date: raw0[COL.deliveryDate] || '',
    delivery_time: raw0[COL.deliveryTime] || '',
    current_shipping_csv: raw0[COL.shippingMethod] || '',
    shipping_method_code: lines[0].shipping_method_code,
    packing_machine_code: lines[0].packing_machine_code,
    row_status: lines.some((l) => l.row_status === '要判断') ? '要判断' : (lines.some((l) => l.row_status === '確定') ? '確定' : 'auto'),
    reason_code: lines[0].reason_code,
    items,
  };
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
  // LINEギフトは必ず手動出荷 (要件)。ユーザーが梱包機を選んでも manual に固定。
  const pmReq = isLineGift(shop_name) ? 'manual' : packing_machine_code;
  const fin = finalizeLine(shipping_method_code, pmReq); // 整合矯正(ネコポス以外→manual)
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
      const { combo_key } = saveAssortDecision({ items, combo_detail: detail, shipping_method_code: fin.shipping_method_code, packing_machine_code: fin.packing_machine_code }, user);
      const raw0 = JSON.parse(lines[0].raw_cols || '[]');
      recordAssortUsage({ combo_key, order_no, uketsuke_no: raw0[COL.uketsuke], shop_name });
    }
  });
  tx();
  return { ok: true };
}

// ───────────────────────── アソート学習の検索・編集 ─────────────────────────

// sku_key("code::color::size") の先頭=商品コードから商品名を引く(表示用・best-effort)
function resolveItemName(db, sku_key) {
  const code = String(sku_key || '').split('::')[0];
  if (!code) return '';
  const row = db.prepare(`SELECT 商品名 FROM mirror_products WHERE lower(trim(商品コード))=? LIMIT 1`).get(code);
  return row ? row.商品名 : '';
}

/**
 * 学習を 注文番号/受注番号 または 商品コード で検索。アクティブな学習(③)を明細・利用注文つきで返す。
 */
export function searchAssort(q) {
  const db = ensureSchema();
  const term = norm(q);
  if (!term) return [];
  const combos = new Set(comboKeysByOrderRef(term));
  // 商品コードでも検索: sku_key は "商品コード::色::サイズ"。入力コードを
  //   ① "code::%"(完全一致+色サイズ変種) ② "code-%"(ハイフン区切りの派生コード, 例 0726-001794-bk)
  //   の2パターンで照合する。これで base コードでも派生コードがヒットしつつ、別商品(0726-0017940 等)の
  //   誤一致は避けられる。LIKE のワイルドカードは literal 化。
  const pc = normProductCode(term);
  if (pc) {
    const e = pc.replace(/[\\%_]/g, '\\$&');
    for (const r of db.prepare(`
      SELECT DISTINCT d.combo_key FROM pd_assort_decision d
        JOIN pd_assort_decision_item i ON i.decision_id=d.id
       WHERE d.is_active=1 AND d.combo_key_version=?
         AND (i.sku_key LIKE ? ESCAPE '\\' OR i.sku_key LIKE ? ESCAPE '\\')
       LIMIT 200`).all(COMBO_KEY_VERSION, e + '::%', e + '-%')) combos.add(r.combo_key);
  }
  const out = [];
  for (const ck of combos) {
    const d = getAssortByCombo(ck);
    if (!d) continue;
    out.push({
      combo_key: ck,
      shipping_method_code: d.shipping_method_code,
      packing_machine_code: d.packing_machine_code,
      decided_by: d.decided_by, decided_at: d.decided_at,
      items: d.items.map((it) => ({ sku_key: it.sku_key, product_code: String(it.sku_key).split('::')[0], product_name: resolveItemName(db, it.sku_key), qty: it.qty })),
      usage: d.usage,
    });
    if (out.length >= 50) break;
  }
  return out;
}

/** 学習の配送方法・梱包機を更新(明細はそのまま)。SCD2で旧版は is_active=0 に。 */
export function updateAssort(combo_key, shipping_method_code, packing_machine_code, user) {
  const d = getAssortByCombo(combo_key);
  if (!d) throw vErr('対象の学習が見つかりません');
  const items = d.items.map((i) => ({ sku_key: i.sku_key, qty: i.qty }));
  const detail = items.map((i) => `${i.sku_key} x${i.qty}`);
  return saveAssortDecision({ items, combo_detail: detail, shipping_method_code, packing_machine_code }, user);
}

// ───────────────────────── 出力 (ゲート + CAS) ─────────────────────────

export function exportCsv(batch_id, user, opts = {}) {
  const downgradeMeltline = !!opts.downgradeMeltline; // MeltLine 導入前: meltline を手動出荷に落として出力
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
      let pm = l.packing_machine_code;
      if (downgradeMeltline && pm === 'meltline') pm = 'manual'; // MeltLine 導入前は手動出荷で出す
      if (isLineGift(l.shop_name)) pm = 'manual';                // LINEギフトは必ず手動出荷 (安全網)
      const fin = finalizeLine(l.shipping_method_code, pm, smMap);
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
  const mg = MALL_GROUPS.includes(mall_group) ? mall_group : 'rakuten';
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

/**
 * 正規化商品コード集合 → 商品名 の Map。mirror_products を1回だけスキャンして必要分を拾う
 * (lower(trim(商品コード)) はインデックスが効かないため、関数IN を複数回回すより全件1スキャンが速い)。
 */
function fetchProductNames(db, codes) {
  const want = new Set((codes || []).filter(Boolean));
  const map = new Map();
  if (!want.size) return map;
  // 旧実装 MAX(商品名) と同値: 同一コードに商品名揺れがあれば辞書順最大を採用(通常コードは一意なので実質1件)。
  for (const r of db.prepare(`SELECT 商品コード AS code, 商品名 AS name FROM mirror_products WHERE 商品コード IS NOT NULL`).iterate()) {
    const pc = normProductCode(r.code);
    if (!want.has(pc)) continue;
    const prev = map.get(pc);
    if (prev === undefined || (r.name != null && String(r.name) > String(prev))) map.set(pc, r.name);
  }
  return map;
}

/**
 * コピー元検索: 商品コード or 商品名で登録済みルールを引き、(sku_key, mall_group) 単位に tiers をまとめて返す。
 * UI 側で結果を選ぶと編集フォームへ tiers を流し込み、編集→保存できる(登録時コピー)。
 * 高速化: 重い「関数付き JOIN + 商品名 LIKE」を避け、(1)一致 product_code を収集 → (2)idx_pd_rule_pc で見出し
 *         → (3)商品名は mirror_products を1スキャンで一括取得、の3段に分解。
 */
export function searchRules(q) {
  const db = ensureSchema();
  const term = norm(q);
  if (!term) return [];
  const pc = normProductCode(term);
  // LIKE のワイルドカード(% _ \)を literal 化してから部分一致 (ESCAPE 指定)
  const safe = term.replace(/[\\%_]/g, '\\$&');
  const codeLike = '%' + normProductCode(safe) + '%';
  const nameLike = '%' + safe.toLowerCase() + '%'; // 商品名一致は lower(trim()) で旧実装と同一(大小文字・前後空白を吸収)

  // 商品名一致は「非相関サブクエリ」で product_code 集合に変換する。これにより mirror_products は
  // 1回だけ評価され(クエリプラン上 LIST SUBQUERY + BLOOM FILTER)、旧実装の「関数付き JOIN による
  // 行ごとの mirror 走査」を回避。IN は変数展開ではないので件数上限の心配もなく、打ち切りも無いので
  // ORDER BY product_code LIMIT 100 が真の top-100 を決定的に返す(旧実装と完全同値)。
  const heads = db.prepare(`
    SELECT sku_key, product_code, mall_group
      FROM pd_shipping_rule r
     WHERE r.product_code = ?
        OR r.product_code LIKE ? ESCAPE '\\'
        OR r.product_code IN (
             SELECT lower(trim(商品コード)) FROM mirror_products
              WHERE lower(trim(商品名)) LIKE ? ESCAPE '\\' AND 商品コード IS NOT NULL
           )
     GROUP BY sku_key, mall_group, product_code
     ORDER BY product_code, sku_key, mall_group
     LIMIT 100
  `).all(pc, codeLike, nameLike);
  if (!heads.length) return [];
  // 商品名を一括取得 (mirror を1スキャン)
  const nameMap = fetchProductNames(db, heads.map((h) => h.product_code));

  // 見出しの tiers を1クエリでまとめて取得 (N+1回避)。複合キーは char(1) 連結で IN 照合
  // (mall_group は {amazon,rakuten,yahoo}、sku_key は正規化済みのため 0x01 と衝突しない)。
  const SEP = String.fromCharCode(1); // SQL の char(1) と一致させる区切り
  const keys = heads.map((h) => h.sku_key + SEP + h.mall_group);
  const ph = keys.map(() => '?').join(',');
  const tierRows = db.prepare(`
    SELECT sku_key, mall_group, qty_min, qty_max, shipping_method_code, packing_machine_code
      FROM pd_shipping_rule
     WHERE sku_key || char(1) || mall_group IN (${ph})
     ORDER BY qty_min
  `).all(...keys);
  const tiersByKey = new Map();
  for (const t of tierRows) {
    const k = t.sku_key + SEP + t.mall_group;
    if (!tiersByKey.has(k)) tiersByKey.set(k, []);
    tiersByKey.get(k).push({
      qty_min: t.qty_min, qty_max: t.qty_max,
      shipping_method_code: t.shipping_method_code, packing_machine_code: t.packing_machine_code,
    });
  }
  return heads.map((h) => {
    const parts = String(h.sku_key || '').split('::');
    const variant = [parts[1], parts[2]].filter(Boolean).join('/');
    const tiers = (tiersByKey.get(h.sku_key + SEP + h.mall_group) || []).sort((a, b) => a.qty_min - b.qty_min);
    return {
      sku_key: h.sku_key, product_code: h.product_code, product_name: nameMap.get(h.product_code) || '',
      mall_group: h.mall_group, variant, tiers,
    };
  });
}

// ───────────────────────── 条件検索 + 一括編集 ─────────────────────────

/**
 * 条件(数量N・配送方法・梱包機・モール、すべて任意)で登録済みルール(帯)を検索。
 * 数量N指定時は「N を含む帯 (qty_min<=N AND (qty_max IS NULL OR qty_max>=N))」に限定。
 * AES(変更対象外)は除外。商品名は mirror を1スキャンで一括取得(行ごとの相関サブクエリは遅い)。LIMIT 2000(超過は capped)。
 */
export function searchRulesByCondition({ qty, shipping_method_code, packing_machine_code, mall_group } = {}) {
  const db = ensureSchema();
  const where = [`r.shipping_method_code <> 'aes'`];
  const params = [];
  const qtyRaw = String(qty ?? '').trim();
  if (qtyRaw !== '') {
    if (!/^\d+$/.test(qtyRaw) || parseInt(qtyRaw, 10) < 1) throw vErr('数量は1以上の整数で指定してください');
    const qn = parseInt(qtyRaw, 10);
    where.push(`r.qty_min <= ? AND (r.qty_max IS NULL OR r.qty_max >= ?)`);
    params.push(qn, qn);
  }
  if (shipping_method_code) { where.push(`r.shipping_method_code = ?`); params.push(shipping_method_code); }
  if (packing_machine_code) { where.push(`r.packing_machine_code = ?`); params.push(packing_machine_code); }
  if (mall_group) {
    if (!MALL_GROUPS.includes(mall_group)) throw vErr('不正なモール指定です');
    where.push(`r.mall_group = ?`); params.push(mall_group);
  }
  const rows = db.prepare(`
    SELECT r.sku_key, r.product_code, r.mall_group, r.qty_min, r.qty_max,
           r.shipping_method_code, r.packing_machine_code
      FROM pd_shipping_rule r
     WHERE ${where.join(' AND ')}
     ORDER BY r.product_code, r.sku_key, r.mall_group, r.qty_min
     LIMIT 2001
  `).all(...params);
  const capped = rows.length > 2000;
  const out = capped ? rows.slice(0, 2000) : rows;
  const nameMap = fetchProductNames(db, out.map((r) => r.product_code));
  for (const r of out) r.product_name = nameMap.get(r.product_code) || '';
  return { rows: out, capped };
}

/**
 * 選択した帯(sku_key, mall_group, qty_min)の配送方法/梱包機を一括変更。
 * shipping/packing は未指定(空)なら「変更しない」。各行で finalizeLine 整合矯正(ネコポス以外→梱包機manual)。
 * AES 行はスキップ。帯(qty_min/qty_max)自体は変えないので区間連続性は不変。
 */
export function bulkUpdateRules({ items, shipping_method_code, packing_machine_code }, user) {
  const db = ensureSchema();
  if (!Array.isArray(items) || !items.length) throw vErr('対象が選択されていません');
  if (items.length > 2000) throw vErr('一括変更は一度に2000件までです');
  const newSm = shipping_method_code || null;
  const newPm = packing_machine_code || null;
  if (!newSm && !newPm) throw vErr('変更する項目(配送方法 または 梱包機)を指定してください');
  if (newSm === 'aes') throw vErr('AES へは変更できません');
  // 事前にコード妥当性を検証(配送方法・梱包機どちらの単独更新でも確実に。不正コードによる途中失敗を防ぐ)
  if (newSm && !shippingMethodMap().has(newSm)) throw vErr('未知の配送方法コード: ' + newSm);
  if (newPm && !db.prepare(`SELECT 1 FROM pd_packing_machine WHERE code=?`).get(newPm)) throw vErr('未知の梱包機コード: ' + newPm);

  // 帯は (sku_key, mall_group, qty_min) で一意(DB UNIQUE)。qty_max も条件に含め、検索後に帯が
  // 作り変えられていたら更新しない(notFound として安全側に倒す)。qty_max IS ? は NULL 安全比較。
  const sel = db.prepare(`SELECT shipping_method_code, packing_machine_code FROM pd_shipping_rule
                           WHERE sku_key=? AND mall_group=? AND qty_min=? AND qty_max IS ?`);
  const upd = db.prepare(`UPDATE pd_shipping_rule SET shipping_method_code=?, packing_machine_code=?, updated_at=?, updated_by=?
                           WHERE sku_key=? AND mall_group=? AND qty_min=? AND qty_max IS ?`);
  const now = utcIsoNow();
  let updated = 0, skippedAes = 0, corrected = 0, notFound = 0;
  // all-or-nothing: 整合不能(コード不正等)で1件でも例外なら全体ロールバック。
  //   notFound(検索後に消えた/変わった) と skippedAes(変更対象外) は正当なスキップで、成功分はコミット。
  const tx = db.transaction(() => {
    for (const it of items) {
      if (!it || !it.sku_key || !it.mall_group || !Number.isInteger(it.qty_min)) { notFound++; continue; }
      const qmax = Number.isInteger(it.qty_max) ? it.qty_max : null;
      const cur = sel.get(it.sku_key, it.mall_group, it.qty_min, qmax);
      if (!cur) { notFound++; continue; }
      if (cur.shipping_method_code === 'aes') { skippedAes++; continue; }
      const wantSm = newSm ?? cur.shipping_method_code;
      const wantPm = newPm ?? cur.packing_machine_code;
      const fin = finalizeLine(wantSm, wantPm); // 整合矯正(ネコポス以外→梱包機manual)
      if (fin.packing_machine_code !== wantPm) corrected++; // 梱包機が manual に矯正された件数
      upd.run(fin.shipping_method_code, fin.packing_machine_code, now, user || null, it.sku_key, it.mall_group, it.qty_min, qmax);
      updated++;
    }
  });
  tx();
  audit('rule_bulk_update', null, { updated, shipping: newSm, packing: newPm }, user);
  return { updated, corrected, skippedAes, notFound };
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
