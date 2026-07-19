/**
 * 梱包機振り分け・配送方法判定 — サービス層 (取込/判定/編集/出力/マスタ)
 */
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { getMirrorDB } from '../warehouse-mirror/db.js';
import {
  ensureSchema, utcIsoNow, norm, buildSkuKey, normProductCode,
  finalizeLine, classifyOrder, shippingMethodMap, mallGroupOf, isLineGift,
  saveAssortDecision, comboKeyOf, listUnregistered, mirrorFreshness,
  recordAssortUsage, comboKeysByOrderRef, getAssortByCombo, purgeOldUsage,
  COL, MALL_GROUPS, COMBO_KEY_VERSION,
} from './db.js';
import { parseNeCsv, buildNeCsv, decodeCp932 } from './csv.js';

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
    const key = l.shop_name + "_" + l.order_no;
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
 * 配送指定日(CSV第18列)の書き戻し。pickerYMD は HTML date input の 'YYYY-MM-DD' か ''(クリア)。
 * 元の値の区切り形式(スラッシュ/ハイフン/8桁連結)を踏襲。未変更なら元文字列をそのまま保持(形式の揺れを防ぐ)。
 */
function formatDeliveryDate(origRaw, pickerYMD) {
  const p = String(pickerYMD ?? '').trim();
  if (p === '') return ''; // クリア
  const m = p.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) return origRaw == null ? '' : String(origRaw); // 想定外入力は元のまま(安全)
  const [, y, mo, d] = m;
  // 暦上ありえない日付(例 2026-02-31)は弾く(API直叩き対策)。不正なら元のまま。
  const dt = new Date(Date.UTC(+y, +mo - 1, +d));
  if (dt.getUTCFullYear() !== +y || dt.getUTCMonth() + 1 !== +mo || dt.getUTCDate() !== +d) {
    return origRaw == null ? '' : String(origRaw);
  }
  const o = String(origRaw ?? '');
  const op = o.match(/(\d{4})\D?(\d{1,2})\D?(\d{1,2})/);
  if (op && +op[1] === +y && +op[2] === +mo && +op[3] === +d) return o; // 同一日付は元の表記を温存
  if (/^\d{8}$/.test(o)) return `${y}${mo}${d}`;     // 20260524
  if (o.includes('-')) return `${y}-${mo}-${d}`;     // 2026-05-24
  return `${y}/${mo}/${d}`;                            // 2026/05/24 (既定: NE/日本で一般的)
}

/**
 * 1伝票の配送方法・梱包機を確定。order_type='assort' かつ learn=true なら ③ にも学習登録。
 * delivery_date(picker 'YYYY-MM-DD'/'') と delivery_time(フリーテキスト) が渡されたら、
 * 伝票の全明細の raw_cols(第18/19列) も書き換える(配送会社変更時の日時変換用)。
 */
export function decideOrder(batch_id, shop_name, order_no, { shipping_method_code, packing_machine_code, learn, delivery_date, delivery_time }, user) {
  const db = ensureSchema();
  const batch = db.prepare(`SELECT status FROM pd_import_batch WHERE batch_id=?`).get(batch_id);
  if (!batch) throw vErr('バッチが見つかりません');
  if (batch.status === 'exported') throw vErr('出力済みのバッチは編集できません');
  // 出力処理中 (Drive 保存中) は編集を拒否。build 済みCSVと確定内容がずれるのを防ぐ (Codex High②)。
  if (batch.status === 'locked_for_export') throw vErr('出力処理中のため編集できません。少し待ってから操作してください');
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

    // 配送指定日・時間帯の編集を伝票の全明細の raw_cols に反映 (送られてきた時のみ)
    if (delivery_date !== undefined || delivery_time !== undefined) {
      const updRaw = db.prepare(`UPDATE pd_import_line SET raw_cols=? WHERE batch_id=? AND row_no=?`);
      for (const l of lines) {
        const raw = JSON.parse(l.raw_cols || '[]');
        if (delivery_date !== undefined) raw[COL.deliveryDate] = formatDeliveryDate(raw[COL.deliveryDate], delivery_date);
        if (delivery_time !== undefined) {
          const cur = raw[COL.deliveryTime] == null ? '' : String(raw[COL.deliveryTime]);
          const next = String(delivery_time ?? '');
          if (next !== cur) raw[COL.deliveryTime] = next.trim(); // 変更時のみ書換(無変更は元値温存)
        }
        updRaw.run(JSON.stringify(raw), batch_id, l.row_no);
      }
    }

    // letterpack は学習対象外 (A4/4kg制限のため毎回目視判断が必要)。learn=true でもスキップ。
    if (learn && lines[0].order_type === 'assort' && fin.shipping_method_code !== 'letterpack') {
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
 * アソート学習を検索。アクティブな学習(③)を明細・利用注文つきで返す。
 * フィルタ:
 *   - q: 注文番号 / 受注番号 / 商品コード のテキスト検索
 *   - shipping_method_code / packing_machine_code: 完全一致
 *   - qtyMin / qtyMax: アソート合計数量 (items の SUM qty)
 *   - dateFrom / dateTo: 登録日 (decided_at) の範囲 (YYYY-MM-DD、JST扱いで境界包含)
 * 何も指定なし → 空配列を返す (現状互換、暴発防止)。
 * いずれか1つ以上指定で AND 合成、結果は 50 件 LIMIT。
 */
// 'YYYY-MM-DD' を JST 0:00 として UTC ISO 文字列へ。実在しない日付は null。
function jstMidnightIso(ymd) {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(ymd || ''));
  if (!m) return null;
  const y = +m[1], mo = +m[2], d = +m[3];
  const dt = new Date(`${m[0]}T00:00:00+09:00`);
  if (isNaN(dt.getTime())) return null;
  // 逆引きで実在チェック (例: 2026-02-31 のような不正値は弾く)
  const jstDt = new Date(dt.getTime() + 9 * 3600 * 1000);
  if (jstDt.getUTCFullYear() !== y || jstDt.getUTCMonth() + 1 !== mo || jstDt.getUTCDate() !== d) return null;
  return dt.toISOString();
}

export function searchAssort(filters = {}) {
  const db = ensureSchema();
  // 旧シグネチャ (q を文字列で直接渡す) との後方互換
  if (typeof filters === 'string') filters = { q: filters };
  const term = norm(filters.q);
  const sm = norm(filters.shipping_method_code);
  const pm = norm(filters.packing_machine_code);
  // 合計数量は 1以上の整数。入力ありで不正なら VALIDATION エラー (silent 無視はしない、日付と方針統一)。
  const parseQty = (v, label) => {
    if (v == null || v === '') return null;
    const n = Number(v);
    if (!Number.isInteger(n) || n <= 0) throw vErr(`${label} は 1 以上の整数で指定してください: ${v}`);
    return n;
  };
  const qtyMin = parseQty(filters.qtyMin, '合計数量の最小');
  const qtyMax = parseQty(filters.qtyMax, '合計数量の最大');
  if (qtyMin !== null && qtyMax !== null && qtyMin > qtyMax) {
    throw vErr('合計数量の最小が最大より大きい指定です');
  }
  // 登録日は JST 基準。入力ありで実在しない日付は VALIDATION エラー (silent 無視はしない)。
  // dateFrom 00:00 JST 以降、dateTo の翌日 00:00 JST 未満 (両端包含)。
  const dateFromIso = filters.dateFrom ? jstMidnightIso(filters.dateFrom) : null;
  if (filters.dateFrom && !dateFromIso) throw vErr(`登録日(from)の日付が不正です: ${filters.dateFrom}`);
  let dateToIsoExclusive = null;
  let dateToIsoInclusiveBase = null;
  if (filters.dateTo) {
    const toBase = jstMidnightIso(filters.dateTo);
    if (!toBase) throw vErr(`登録日(to)の日付が不正です: ${filters.dateTo}`);
    dateToIsoInclusiveBase = toBase;
    const next = new Date(toBase);
    next.setUTCDate(next.getUTCDate() + 1);
    dateToIsoExclusive = next.toISOString();
  }
  if (dateFromIso && dateToIsoInclusiveBase && dateFromIso > dateToIsoInclusiveBase) {
    throw vErr('登録日 from が to より後ろの指定です');
  }
  const hasAnyFilter = term || sm || pm || qtyMin !== null || qtyMax !== null || dateFromIso || dateToIsoExclusive;
  if (!hasAnyFilter) return [];

  // 1) text 検索: term 指定時は combos を注文番号/商品コードから絞る (= 集合の上限)。
  //    無指定なら null (= 上限なし、後段の DB filter のみで決定)。
  let combos = null;
  if (term) {
    combos = new Set(comboKeysByOrderRef(term));
    // 商品コードでも検索: sku_key は "商品コード::色::サイズ"。
    //   ① "code::%"(完全一致+色サイズ変種) ② "code-%"(ハイフン区切りの派生コード, 例 0726-001794-bk)
    //   の2パターンで照合。LIKE のワイルドカードは literal 化。
    const pc = normProductCode(term);
    if (pc) {
      const e = pc.replace(/[\\%_]/g, '\\$&');
      for (const r of db.prepare(`
        SELECT DISTINCT d.combo_key FROM pd_assort_decision d
          JOIN pd_assort_decision_item i ON i.decision_id=d.id
         WHERE d.is_active=1 AND d.combo_key_version=?
           AND (i.sku_key LIKE ? ESCAPE '\\' OR i.sku_key LIKE ? ESCAPE '\\')
         ORDER BY d.decided_at DESC
         LIMIT 500`).all(COMBO_KEY_VERSION, e + '::%', e + '-%')) combos.add(r.combo_key);
    }
  }

  // 2) DB 側で絞れるフィルタ (sm/pm/date) を適用。
  //    - text 指定済 (combos != null): combos ∩ allowed
  //    - text 無指定 (combos == null): allowed をそのまま combos に
  //    ORDER BY decided_at DESC + LIMIT 2000 で「古い分が先に落ちる」事故を防ぐ。
  const needDbFilter = sm || pm || dateFromIso || dateToIsoExclusive || combos === null;
  if (needDbFilter) {
    const where = ['is_active=1', 'combo_key_version=?'];
    const params = [COMBO_KEY_VERSION];
    if (sm) { where.push('shipping_method_code=?'); params.push(sm); }
    if (pm) { where.push('packing_machine_code=?'); params.push(pm); }
    if (dateFromIso) { where.push('decided_at >= ?'); params.push(dateFromIso); }
    if (dateToIsoExclusive) { where.push('decided_at < ?'); params.push(dateToIsoExclusive); }
    const allowed = new Set(db.prepare(`SELECT combo_key FROM pd_assort_decision WHERE ${where.join(' AND ')} ORDER BY decided_at DESC LIMIT 2000`)
      .all(...params).map((r) => r.combo_key));
    if (combos === null) combos = allowed;
    else for (const ck of [...combos]) if (!allowed.has(ck)) combos.delete(ck);
  }

  // 3) 全候補を hydrate → 合計数量で post-filter → 登録日降順で sort → 50件 slice
  //    (Set 走査中に 50件 break + 後 sort だと「先に拾われた 50件」になるため、全候補を sort 対象に)
  const hydrated = [];
  for (const ck of combos) {
    const d = getAssortByCombo(ck);
    if (!d) continue;
    const totalQty = d.items.reduce((s, it) => s + Number(it.qty || 0), 0);
    if (qtyMin !== null && totalQty < qtyMin) continue;
    if (qtyMax !== null && totalQty > qtyMax) continue;
    hydrated.push({
      combo_key: ck,
      shipping_method_code: d.shipping_method_code,
      packing_machine_code: d.packing_machine_code,
      decided_by: d.decided_by, decided_at: d.decided_at,
      total_qty: totalQty,
      items: d.items.map((it) => ({ sku_key: it.sku_key, product_code: String(it.sku_key).split('::')[0], product_name: resolveItemName(db, it.sku_key), qty: it.qty })),
      usage: d.usage,
    });
  }
  hydrated.sort((a, b) => (a.decided_at < b.decided_at ? 1 : a.decided_at > b.decided_at ? -1 : 0));
  return hydrated.slice(0, 50);
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

// ── 出力ロック取得 (classifying → locked_for_export、CAS) ──
// Drive 保存を始める前に「このバッチだけが今出力中」と宣言する (Codex High①②③)。
//  - 既に exported / 存在しない → changes=0 で throw (古い/出力済みバッチが Drive を上書きするのを防ぐ)
//  - locked_for_export の再取得は冪等 (前回の途中失敗からの再実行を許可)
// pending>0・行数不整合はここで先に弾く (ロックしてから build 失敗→release の往復を避ける)。
export function beginExport(batch_id) {
  const db = ensureSchema();
  const now = utcIsoNow();
  const b = db.prepare(`SELECT * FROM pd_import_batch WHERE batch_id=?`).get(batch_id);
  if (!b) throw vErr('バッチが見つかりません');
  if (b.status === 'exported') throw vErr('このバッチは既に出力済みです');
  // TTL 切れバッチは出力禁止 (明細が purge され得る・内容が古い、ID直叩き対策、Codex High)。
  if (b.expires_at && b.expires_at < now) throw vErr('このバッチは期限切れ(48h超)です。再度CSVをアップロードしてください');
  // 自分より新しい取込バッチが (TTL内で) 存在するなら出力禁止 (Codex High:世代逆転)。
  // 固定ファイル logi_dispatch.csv は「最新の取込」を載せる運用。再表示ドロップダウンから古い
  // バッチを選んで出力し、共有ファイルを古い内容で上書きする事故を防ぐ (最新バッチのみ出力可)。
  const newer = db.prepare(`SELECT batch_id FROM pd_import_batch
      WHERE (expires_at IS NULL OR expires_at >= ?)
        AND ( uploaded_at > ? OR (uploaded_at = ? AND batch_id > ?) )
      LIMIT 1`).get(now, b.uploaded_at, b.uploaded_at, batch_id);
  if (newer) throw vErr('より新しい取込バッチがあります。最新のバッチを表示して出力してください (古いバッチの出力は共有ファイルを古い内容で上書きするため禁止)');
  const pending = db.prepare(`SELECT COUNT(*) c FROM pd_import_line WHERE batch_id=? AND row_status='要判断'`).get(batch_id).c;
  if (pending > 0) throw vErr(`未判断が ${pending} 件あります。すべて確定してから出力してください。`, { pending });
  const cas = db.prepare(`UPDATE pd_import_batch SET status='locked_for_export'
      WHERE batch_id=? AND status IN ('classifying','locked_for_export')`).run(batch_id);
  if (cas.changes === 0) throw vErr('このバッチは出力できる状態ではありません (状態が変わっています)');
}

// ── 出力ロック解放 (locked_for_export → classifying、CAS) ──
// Drive 保存や確定に失敗したときに呼び、再編集・再出力できる状態へ戻す。best-effort。
export function releaseExport(batch_id) {
  const db = ensureSchema();
  db.prepare(`UPDATE pd_import_batch SET status='classifying'
      WHERE batch_id=? AND status='locked_for_export'`).run(batch_id);
}

// ── CSV バッファ生成 (副作用なし。CAS/tracking を伴わない) ──
// Drive 保存を「先に」行い、成功時だけ commitExport() で CAS 確定する二段構えのため、
// 生成 (再試行安全) と 確定 (一度きり) を分離する (Codex High①②: CAS後の取りこぼし/二重投入防止)。
// lineRows も返し、commit 側で再クエリせず同じスナップショットで tracking 登録できるようにする。
export function buildExportCsv(batch_id, opts = {}) {
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
      // 沖縄県宛 × ネコポス は梱包機マーカーを「手動出荷」に矯正。
      // 航空便の液体規制は、ヤマト端末で送り状QRを読んだときに「沖縄エラー」で別管理導線へ
      // 入る運用。梱包機出荷(pasline/meltline)だと送り状にQRが付かずヤマト端末でエラーが
      // 出ないため、手動出荷扱いで吐き出して送り状経由で必ずエラーを検知させる。
      // (配送方法はネコポスのまま=送料/契約区分は変えない、91列目の梱包機マーカーだけ落とす)
      if (l.shipping_method_code === 'nekopos' && norm(l.pref).startsWith('沖縄')) pm = 'manual';
      const fin = finalizeLine(l.shipping_method_code, pm, smMap);
      for (const [idx, val] of Object.entries(fin.cols)) raw[Number(idx)] = val;
    }
    return raw;
  });

  const buf = buildNeCsv(headerRow, outRows);
  return { buffer: buf, rowCount: outRows.length, lineRows };
}

// ── 出力確定 (CAS で exported 化 + tracking 登録 + audit) ──
// buildExportCsv() が成功し、外部保存 (Drive) も済んだ後に呼ぶ。CAS が 0 件なら二重出力として throw。
// lineRows は buildExportCsv の戻りをそのまま渡す (再クエリ省略。無ければ再取得)。
export function commitExport(batch_id, user, { lineRows = null, rowCount = null } = {}) {
  const db = ensureSchema();
  const rows = lineRows || db.prepare(`SELECT * FROM pd_import_line WHERE batch_id=? ORDER BY row_no`).all(batch_id);
  // CAS で exported へ (二重出力防止) + tracking UPSERT を 1 transaction に。
  // どちらか失敗したら状態は変えず、ユーザーが再試行できる状態にする。
  let trackingStats = { registered: 0, syncedSkipped: 0, candidates: 0 };
  const tx = db.transaction(() => {
    const cas = db.prepare(`UPDATE pd_import_batch SET status='exported' WHERE batch_id=? AND status IN ('classifying','locked_for_export')`)
      .run(batch_id);
    if (cas.changes === 0) throw vErr('このバッチは既に出力済みか、状態が変わっています');
    // pd_shipment_tracking に登録 (CSV 出力した伝票だけが NE 反映対象)。
    // 同一 ne_uketsuke_no (col 20) を伝票単位で 1 行に集約。AES は対象外 (NE 側で別管理)。
    // synced 済み (=既に NE 反映完了) は触らない、それ以外は pending リセット + 追跡番号クリア。
    trackingStats = registerShipmentTrackingFromBatch(db, rows, user);
  });
  tx();

  // 登録件数を audit log に残す (Codex R0 High 2: 登録件数が低くても検知できるように)
  audit('export', batch_id, { rows: rowCount ?? rows.length, tracking: trackingStats }, user);
  return { tracking: trackingStats };
}

// 従来の一括版 (ブラウザ直DL経路 /api/export/:id 用)。生成→確定を一続きで行う。
// Drive 保存経路は buildExportCsv → (Drive保存) → commitExport の順で使うこと。
export function exportCsv(batch_id, user, opts = {}) {
  const built = buildExportCsv(batch_id, opts);
  const { tracking } = commitExport(batch_id, user, { lineRows: built.lineRows, rowCount: built.rowCount });
  return { buffer: built.buffer, rowCount: built.rowCount, tracking };
}

// 過去の exported バッチを走査して pd_shipment_tracking に backfill する admin 機能。
// 2026-06-04 hotfix: PR #231 (tracking テーブル新設) より前に exportCsv された伝票は
// pd_shipment_tracking に登録されていないため、48h 以内に残っている pd_import_line から
// 補填する。
//
// ⚠️ backfill は「未登録を補う」だけ (Codex R1 High)。既存行 (今日紐付け済 / ready / skipped / error 等)
// は INSERT OR IGNORE で完全保護する。再出力時の破壊的 UPSERT は exportCsv 専用 (別経路)。
export function backfillShipmentTrackingFromExportedBatches({ dryRun = false, todayJstOnly = true } = {}, user) {
  const db = ensureSchema();
  // exported 状態のバッチのみ対象 (まだ TTL 内のもの)。
  // ⚠️ locked_for_export は「出力処理中/クラッシュ残留」の不確定な中間状態で、Drive にCSVが
  //    載っているとは限らない (Codex High)。これを「CSV出力済み」とみなして tracking を作ると、
  //    Drive にファイルが無い/別バッチ内容なのに NE 反映対象だけ生成され得るため backfill から除外する。
  // 2026-06-04 中原さん指摘: todayJstOnly (デフォ ON) で「今日 JST にアップしたバッチだけ」に絞れる。
  // 前日以前のバッチを backfill すると、B2 CSV 再投入対象外なのに pending として残って混乱するため。
  const todayFilter = todayJstOnly
    ? `AND date(uploaded_at, '+9 hours') = date('now', '+9 hours')` : '';
  const batches = db.prepare(`SELECT batch_id, status, uploaded_at, row_count
    FROM pd_import_batch
    WHERE status = 'exported'
      AND (expires_at IS NULL OR expires_at >= ?)
      ${todayFilter}
    ORDER BY uploaded_at`).all(utcIsoNow());
  // INSERT-only (既存行は ne_uketsuke_no UNIQUE で弾く、ON CONFLICT DO NOTHING)。
  // product_summary も exportCsv と同じロジックで作る (中身の仕様統一)。
  const buildSummaryLocal = (items) => {
    if (!items.length) return null;
    const parts = items.map(i => `${i.product_code} ${i.product_name} ×${i.qty}`.trim());
    const maxLen = 480;
    let summary = parts[0].slice(0, maxLen);
    let i = 1;
    while (i < parts.length) {
      const candidate = summary + ' ／ ' + parts[i];
      if (candidate.length > maxLen) break;
      summary = candidate; i++;
    }
    if (i < parts.length) summary += ` ／ ...他${parts.length - i}件`;
    return summary;
  };
  const insertOnly = db.prepare(`
    INSERT INTO pd_shipment_tracking
      (ne_uketsuke_no, shipping_method_code, shop_name, order_no,
       sync_status, method_at_export, product_summary, product_items_json, exported_at,
       added_at, added_by, updated_at, updated_by)
    VALUES (?,?,?,?, 'pending', ?,?,?,?, ?, ?, ?, ?)
    ON CONFLICT(ne_uketsuke_no) DO NOTHING
  `);
  const now = utcIsoNow();
  let totalInserted = 0;
  let totalAlreadyExists = 0;
  let totalCandidates = 0;
  const perBatch = [];
  for (const b of batches) {
    const lineRows = db.prepare(`SELECT * FROM pd_import_line WHERE batch_id=? ORDER BY row_no`).all(b.batch_id);
    if (!lineRows.length) {
      perBatch.push({ batch_id: b.batch_id, uploaded_at: b.uploaded_at, candidates: 0, inserted: 0, already_exists: 0, note: 'no lines (TTL?)' });
      continue;
    }
    // 伝票単位に集約
    const byUke = new Map();
    for (const l of lineRows) {
      if (!l.shipping_method_code || l.shipping_method_code === 'aes') continue;
      const raw = JSON.parse(l.raw_cols || '[]');
      const uke = norm(raw[COL.uketsuke]);
      if (!uke) continue;
      if (!byUke.has(uke)) byUke.set(uke, {
        ne_uketsuke_no: uke, shipping_method_code: l.shipping_method_code,
        shop_name: l.shop_name, order_no: l.order_no, items: [],
      });
      byUke.get(uke).items.push({
        product_code: l.product_code || '',
        product_name: norm(raw[COL.productName] || ''),
        qty: l.qty || 0,
      });
    }
    const candidateCount = byUke.size;
    totalCandidates += candidateCount;
    if (dryRun) {
      // dry-run は事前 SELECT で「未登録だけ」をカウント (確度の高い予測)
      let willInsert = 0;
      const exists = db.prepare(`SELECT 1 FROM pd_shipment_tracking WHERE ne_uketsuke_no=? LIMIT 1`);
      for (const uke of byUke.keys()) { if (!exists.get(uke)) willInsert++; }
      perBatch.push({ batch_id: b.batch_id, uploaded_at: b.uploaded_at,
        candidates: candidateCount, inserted: willInsert, already_exists: candidateCount - willInsert, dry_run: true });
      totalInserted += willInsert;
      totalAlreadyExists += (candidateCount - willInsert);
      continue;
    }
    // 実 INSERT-only を transaction で
    const tx = db.transaction(() => {
      let ins = 0, dup = 0;
      for (const t of byUke.values()) {
        const summary = buildSummaryLocal(t.items);
        const itemsJson = JSON.stringify(t.items);
        const info = insertOnly.run(t.ne_uketsuke_no, t.shipping_method_code, t.shop_name, t.order_no,
          t.shipping_method_code, summary, itemsJson, now,
          now, user || 'backfill', now, user || 'backfill');
        if (info.changes > 0) ins++; else dup++;
      }
      return { ins, dup };
    });
    const { ins, dup } = tx();
    perBatch.push({ batch_id: b.batch_id, uploaded_at: b.uploaded_at,
      candidates: candidateCount, inserted: ins, already_exists: dup });
    totalInserted += ins;
    totalAlreadyExists += dup;
  }
  audit(dryRun ? 'tracking_backfill_dry_run' : 'tracking_backfill', null,
    { batch_count: batches.length, today_jst_only: todayJstOnly,
      total_candidates: totalCandidates,
      total_inserted: totalInserted, total_already_exists: totalAlreadyExists }, user);
  return { dry_run: dryRun, today_jst_only: todayJstOnly, batch_count: batches.length,
    total_candidates: totalCandidates, total_inserted: totalInserted, total_already_exists: totalAlreadyExists,
    per_batch: perBatch };
}

// duplicate file_hash で再投入できない取込履歴を削除する admin 機能 (Codex R1 Critical)。
// 2026-06-04 backfill 後に同じ B2 CSV を再アップロードして紐付けし直すための窓開け。
// 期間 / source を絞って削除。関連 pd_method_change_log は FK で先に消す。
export function deleteRecentTrackingImports({ hoursBack = 24, sources = null, dryRun = false } = {}, user) {
  const db = ensureSchema();
  const allowedSources = ['yamato_b2', 'yamato_b2_50', 'yupacketpuff'];
  const targetSources = Array.isArray(sources) && sources.length
    ? sources.filter(s => allowedSources.includes(s))
    : allowedSources;
  if (!targetSources.length) throw vErr('source が未指定です');
  const cutoff = new Date(Date.now() - hoursBack * 3600 * 1000).toISOString();
  const placeholders = targetSources.map(() => '?').join(',');
  const params = [cutoff, ...targetSources];
  const targets = db.prepare(`SELECT import_id, source, filename, imported_at, row_count
    FROM pd_tracking_import
    WHERE imported_at >= ? AND source IN (${placeholders})
    ORDER BY imported_at DESC`).all(...params);
  if (dryRun) {
    audit('tracking_import_delete_dry_run', null, { hours_back: hoursBack, sources: targetSources, count: targets.length }, user);
    return { dry_run: true, hours_back: hoursBack, sources: targetSources, targets };
  }
  const tx = db.transaction(() => {
    let logsDeleted = 0;
    for (const t of targets) {
      const r1 = db.prepare(`DELETE FROM pd_method_change_log WHERE import_id=?`).run(t.import_id);
      logsDeleted += r1.changes;
      db.prepare(`DELETE FROM pd_tracking_import WHERE import_id=?`).run(t.import_id);
    }
    return logsDeleted;
  });
  const logsDeleted = tx();
  audit('tracking_import_delete', null,
    { hours_back: hoursBack, sources: targetSources, imports_deleted: targets.length, logs_deleted: logsDeleted }, user);
  return { dry_run: false, hours_back: hoursBack, sources: targetSources,
    imports_deleted: targets.length, logs_deleted: logsDeleted, targets };
}

// 定形外伝票を一括で「定形外として反映する」(2026-06-04 中原さん指示)。
// 「📫 定形外 (本日アップロード分)」一覧で選択した NE 受注番号を 1 transaction で
// tracking_source='no_tracking' + sync_status='ready' に遷移させる。
//
// Codex R1 Critical 対応: setTrackingManual と異なり、既存 tracking_no がある行は
// 上書きしない (skip としてカウント、conflict 検出も同等)。サーバ側で以下を厳格に検証:
// - shipping_method_code='teikeigai' のみ (画面以外からの誤呼び出し防御)
// - sync_status が editable (pending/ready/error/conflict/manual_review) のみ
// - tracking_no が既にあれば skip (上書きしない)
//
// Codex R1 High 対応: IN 句の 999 件上限を避けるため、1 行ずつ UPDATE で処理。
// チャンク化は不要、transaction で atomic に。
export function bulkApplyTeikeigai({ ne_uketsuke_nos } = {}, user) {
  if (!Array.isArray(ne_uketsuke_nos) || !ne_uketsuke_nos.length) {
    throw vErr('ne_uketsuke_nos が空です');
  }
  const uniqueUkes = [...new Set(ne_uketsuke_nos.map(u => norm(u)).filter(Boolean))];
  if (!uniqueUkes.length) throw vErr('有効な NE 受注番号がありません');
  const db = ensureSchema();
  const now = utcIsoNow();
  const editableStatuses = new Set(['pending', 'ready', 'error', 'conflict', 'manual_review']);
  const sel = db.prepare(`SELECT ne_uketsuke_no, shipping_method_code, sync_status, tracking_no, tracking_source
    FROM pd_shipment_tracking WHERE ne_uketsuke_no=?`);
  const upd = db.prepare(`UPDATE pd_shipment_tracking
    SET tracking_source='no_tracking', sync_status='ready', sync_requested_at=?,
        last_error=NULL, last_error_code=NULL,
        updated_at=?, updated_by=?
    WHERE ne_uketsuke_no=?
      AND shipping_method_code='teikeigai'
      AND sync_status IN ('pending','ready','error','conflict','manual_review')
      AND tracking_no IS NULL`);
  let updated = 0, alreadyReady = 0, notFound = 0;
  const skippedHasTracking = [];
  const skippedNotTeikeigai = [];
  const skippedSynced = [];
  const skippedOther = [];
  const tx = db.transaction(() => {
    for (const uke of uniqueUkes) {
      const cur = sel.get(uke);
      if (!cur) { notFound++; continue; }
      if (cur.shipping_method_code !== 'teikeigai') {
        skippedNotTeikeigai.push({ ne_uketsuke_no: uke, method: cur.shipping_method_code });
        continue;
      }
      if (cur.sync_status === 'synced') {
        skippedSynced.push(uke);
        continue;
      }
      if (cur.sync_status === 'syncing' || cur.sync_status === 'skipped') {
        skippedOther.push({ ne_uketsuke_no: uke, status: cur.sync_status });
        continue;
      }
      if (!editableStatuses.has(cur.sync_status)) {
        skippedOther.push({ ne_uketsuke_no: uke, status: cur.sync_status });
        continue;
      }
      if (cur.tracking_no) {
        // 既に追跡番号あり → 上書きしない (Codex R1 Critical: setTrackingManual と違って defensive)
        skippedHasTracking.push({ ne_uketsuke_no: uke, current_tracking_no: cur.tracking_no });
        continue;
      }
      // 既に ready + tracking_source='no_tracking' で同状態なら idempotent (count として alreadyReady に)
      const isAlreadyReady = cur.sync_status === 'ready' && cur.tracking_source === 'no_tracking';
      const info = upd.run(now, now, user || null, uke);
      if (info.changes > 0) {
        if (isAlreadyReady) alreadyReady++; else updated++;
      } else {
        // WHERE で弾かれた (typically: race で他プロセスが先に更新)
        skippedOther.push({ ne_uketsuke_no: uke, status: cur.sync_status, reason: 'race_or_constraint' });
      }
    }
  });
  tx();
  audit('tracking_bulk_teikeigai', null,
    { input_count: uniqueUkes.length, updated, already_ready: alreadyReady, not_found: notFound,
      skipped_has_tracking: skippedHasTracking.length,
      skipped_not_teikeigai: skippedNotTeikeigai.length,
      skipped_synced: skippedSynced.length,
      skipped_other: skippedOther.length }, user);
  return {
    input_count: uniqueUkes.length,
    updated, already_ready: alreadyReady, not_found: notFound,
    skipped_has_tracking: skippedHasTracking.length,
    skipped_not_teikeigai: skippedNotTeikeigai.length,
    skipped_synced: skippedSynced.length,
    skipped_other: skippedOther.length,
    samples: {
      has_tracking: skippedHasTracking.slice(0, 10),
      not_teikeigai: skippedNotTeikeigai.slice(0, 10),
      synced: skippedSynced.slice(0, 10),
    },
  };
}

// 未マッチ行を pd_shipment_tracking に「反映に含める」(2026-06-04 中原さん指示)。
//
// 運用: スタッフが取込履歴詳細モーダルで未マッチ行を見て、NextEngine 側で実在確認した上で
// 「✅ 反映に含める」ボタンを押す。pd_shipment_tracking に sync_status='ready' で INSERT し、
// 次回 cron が NE 反映を試みる。NE 側に該当受注がなければ API エラー → status=error
// で manual_review (既存挙動)。typo の予防は運用 (目視確認) でカバー。
//
// Codex R1 対応:
// - Critical: shipping_method_code NOT NULL → enrich → source fallback の順で必ず確定
// - High 1: ready で直接 INSERT (sync_requested_at=now)、ボタン 1 押下で完結
// - High 2: changes=0 後に既存行を再 SELECT、tracking_no が異なれば vErr (conflict)
// - synced は拒否 (UI でも非表示だが二重チェック)
//
// source 別 fallback (enrich/import_line から取れなかった場合の最終手段):
//   yamato_b2_50  → takkyu50 (50サイズは固定)
//   yupacketpuff  → yupacketpuff (固定)
//   yamato_b2     → hatsubarai (デフォルト発払い、現場で多い側)
const SOURCE_FALLBACK_METHOD = {
  yamato_b2: 'hatsubarai',
  yamato_b2_50: 'takkyu50',
  yupacketpuff: 'yupacketpuff',
};
export function includeUnmatchedToTracking({ ne_uketsuke_no, tracking_no, source, enrich = null }, user) {
  if (!ne_uketsuke_no) throw vErr('ne_uketsuke_no が空です');
  if (!tracking_no) throw vErr('tracking_no が空です');
  const validSources = Object.keys(SOURCE_FALLBACK_METHOD);
  if (!validSources.includes(source)) throw vErr(`source は ${validSources.join(' / ')} のいずれか`);
  const e = enrich || {};
  // shipping_method_code: enrich (= pd_import_line の判定結果) を優先、なければ source fallback
  const shippingMethodCode = e.shipping_method_code || SOURCE_FALLBACK_METHOD[source];
  if (!shippingMethodCode) throw vErr(`shipping_method_code を決定できません (source=${source})`);
  const db = ensureSchema();
  const now = utcIsoNow();
  // 既存行を先に取得して状況別に処理 (Codex R2 High: 既存 pending+tracking_no=NULL を no-op で
  // 取りこぼさない、別 tracking_no は conflict、synced は拒否、既存ありで同値は idempotent)。
  const sel = db.prepare(`SELECT sync_status, tracking_no FROM pd_shipment_tracking WHERE ne_uketsuke_no=?`);
  const cur = sel.get(ne_uketsuke_no);
  if (cur && cur.sync_status === 'synced') {
    throw vErr('既に NE 反映済 (synced) のため反映に含められません。訂正は別経路で行ってください。');
  }
  if (cur && cur.tracking_no && cur.tracking_no !== tracking_no) {
    audit('tracking_unmatched_include_conflict', ne_uketsuke_no,
      { incoming_tracking_no: tracking_no, current_tracking_no: cur.tracking_no, source }, user);
    throw vErr(`既に異なる追跡番号で登録されています (既存=${cur.tracking_no} / 新=${tracking_no})。先にレターパック/手動で確認してください。`);
  }
  // 既存なし → INSERT、既存あり (synced 以外、tracking_no NULL or 同値) → UPDATE で ready 化
  const tx = db.transaction(() => {
    if (!cur) {
      db.prepare(`INSERT INTO pd_shipment_tracking
        (ne_uketsuke_no, shipping_method_code, shop_name, order_no,
         sync_status, sync_requested_at, tracking_no, tracking_source,
         method_at_export, product_summary,
         added_at, added_by, updated_at, updated_by)
        VALUES (?, ?, ?, ?, 'ready', ?, ?, ?, ?, ?, ?, ?, ?, ?)`).run(
        ne_uketsuke_no, shippingMethodCode,
        e.shop_name || null, e.order_no || null,
        now, tracking_no, source, shippingMethodCode, e.product_summary || null,
        now, user || null, now, user || null);
      return 'inserted';
    }
    // 既存あり: 同値 tracking_no + ready なら idempotent (touch しない)
    if (cur.tracking_no === tracking_no && cur.sync_status === 'ready') return 'idempotent_ready';
    // それ以外: tracking_no を確定 + ready 化 (他カラムは既存値を尊重、ただし enrich があれば補完)
    db.prepare(`UPDATE pd_shipment_tracking SET
        tracking_no = ?, tracking_source = ?,
        sync_status = 'ready', sync_requested_at = ?,
        last_error = NULL, last_error_code = NULL,
        shop_name = COALESCE(shop_name, ?),
        order_no = COALESCE(order_no, ?),
        product_summary = COALESCE(product_summary, ?),
        updated_at = ?, updated_by = ?
      WHERE ne_uketsuke_no=? AND sync_status IN ('pending','ready','error','conflict','manual_review')`).run(
      tracking_no, source, now,
      e.shop_name || null, e.order_no || null, e.product_summary || null,
      now, user || null, ne_uketsuke_no);
    return 'updated';
  });
  const action = tx();
  audit('tracking_unmatched_include', ne_uketsuke_no,
    { tracking_no, source, shipping_method_code: shippingMethodCode, action,
      existing_status: cur ? cur.sync_status : null }, user);
  const noteByAction = {
    inserted: '✅ NE 反映待ち (ready) に追加しました',
    updated: `✅ 既存 ${cur ? cur.sync_status : '?'} 行を ready に更新しました`,
    idempotent_ready: '既に ready で登録済 (変更なし)',
  };
  return { ne_uketsuke_no, tracking_no, source, action,
    shipping_method_code: shippingMethodCode, note: noteByAction[action] };
}

// CSV 出力時に各伝票を pd_shipment_tracking に UPSERT。NE 受注番号 (col 20) で 1 行に集約。
// 配送方法変更ログ機能 (中原さん 2026-06-04 指示) のため、以下も保存:
//   method_at_export: CSV 出力時の shipping_method_code (変更検出基準値)
//                     ※既に tracking_no が紐付け済の行は上書きしない (Codex R1 High 3)
//   product_summary:  商品名要約 (例: "商品A 名称 ×1 ／ 商品B ×2 ／ ...他3件") - 500文字制限
//   product_items_json: 商品配列 JSON (後で分析用)
//   exported_at:      CSV 出力時刻
function registerShipmentTrackingFromBatch(db, lineRows, user) {
  const now = utcIsoNow();
  // 伝票単位に集約。商品リストは line 単位で積み上げ。
  const byUketsuke = new Map();
  for (const l of lineRows) {
    if (!l.shipping_method_code || l.shipping_method_code === 'aes') continue;
    const raw = JSON.parse(l.raw_cols || '[]');
    const uke = norm(raw[COL.uketsuke]);
    if (!uke) continue;
    if (!byUketsuke.has(uke)) {
      byUketsuke.set(uke, {
        ne_uketsuke_no: uke,
        shipping_method_code: l.shipping_method_code,
        shop_name: l.shop_name,
        order_no: l.order_no,
        items: [],
      });
    }
    byUketsuke.get(uke).items.push({
      product_code: l.product_code || '',
      product_name: norm(raw[COL.productName] || ''),
      qty: l.qty || 0,
    });
  }
  // 商品要約整形 (先頭 N 件 + "他K件" 形式、500 文字以内)。
  // suffix 分を先に予約しておき、slice(0,500) で件数表示が消えるのを防ぐ (Codex R1 Low 1)。
  const buildSummary = (items) => {
    if (!items.length) return null;
    const parts = items.map(i => `${i.product_code} ${i.product_name} ×${i.qty}`.trim());
    const maxLen = 480; // 500 - "...他99件" (約20文字) のマージン
    let summary = parts[0].slice(0, maxLen);
    let i = 1;
    while (i < parts.length) {
      const candidate = summary + ' ／ ' + parts[i];
      if (candidate.length > maxLen) break;
      summary = candidate;
      i++;
    }
    if (i < parts.length) summary += ` ／ ...他${parts.length - i}件`;
    return summary;
  };
  if (!byUketsuke.size) return 0;
  // synced 済みは触らない。それ以外 (pending/ready/error/conflict/skipped/syncing/manual_review)
  // は最新値で上書き + sync_status を pending にリセット + 追跡番号/エラー/claim 系を全クリア。
  // 出力をやり直したらキャリア CSV から再紐付け必要。古いデータが残って NE 反映に混入するのを防ぐ。
  //
  // method_at_export は「tracking_no がまだ NULL のときだけ更新」(Codex R1 High 3)。
  // 既にキャリア CSV と紐付け済 (変更ログ作成済の可能性) の行は基準値を変えない。
  // 新規追加 (INSERT) 時は無条件で method_at_export = shipping_method_code とする。
  // product_summary / product_items_json / exported_at は最新出力時のスナップショットで更新。
  const upsert = db.prepare(`
    INSERT INTO pd_shipment_tracking
      (ne_uketsuke_no, shipping_method_code, shop_name, order_no,
       sync_status, method_at_export, product_summary, product_items_json, exported_at,
       added_at, added_by, updated_at, updated_by)
    VALUES (?,?,?,?, 'pending', ?,?,?,?, ?, ?, ?, ?)
    ON CONFLICT(ne_uketsuke_no) DO UPDATE SET
      shipping_method_code = excluded.shipping_method_code,
      shop_name = excluded.shop_name,
      order_no = excluded.order_no,
      sync_status = 'pending',
      tracking_no = NULL,
      tracking_source = NULL,
      sync_requested_at = NULL,
      claim_id = NULL,
      claim_token_hash = NULL,
      syncing_started_at = NULL,
      claim_expires_at = NULL,
      claim_attempt_count = 0,
      last_error = NULL,
      last_error_code = NULL,
      ne_last_modified_date = NULL,
      ne_synced_at = NULL,
      skip_reason = NULL,
      method_at_export = CASE
        WHEN pd_shipment_tracking.tracking_no IS NULL THEN excluded.method_at_export
        ELSE pd_shipment_tracking.method_at_export
      END,
      product_summary = excluded.product_summary,
      product_items_json = excluded.product_items_json,
      exported_at = excluded.exported_at,
      updated_at = excluded.updated_at,
      updated_by = excluded.updated_by
    WHERE pd_shipment_tracking.sync_status IN ('pending','ready','error','conflict','manual_review')
  `);
  // ↑ 構成 4 + Codex R5 助言: positive whitelist (synced / syncing / skipped / ne_unknown は触らない)。
  // CSV 出力で worker 管理中 (syncing) や成功確定 (synced) や曖昧 (ne_unknown) を巻き戻さない。
  // changes は INSERT 成功 or UPDATE 成功で 1、synced no-op で 0 を返す。
  // 「CSV は出たが登録 0/少数」を audit から検知できるよう実 UPSERT 件数を返す (Codex R0 High 2)。
  let registered = 0;
  let syncedSkipped = 0;
  for (const t of byUketsuke.values()) {
    const summary = buildSummary(t.items);
    const itemsJson = JSON.stringify(t.items);
    const info = upsert.run(t.ne_uketsuke_no, t.shipping_method_code, t.shop_name, t.order_no,
      t.shipping_method_code, summary, itemsJson, now,
      now, user || null, now, user || null);
    if (info.changes > 0) registered++; else syncedSkipped++;
  }
  return { registered, syncedSkipped, candidates: byUketsuke.size };
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

// 商品マスタ検索 (商品コード / 商品名 部分一致、上限 30 件、2026-06-05 中原さん指示)
// 商品マスタ編集フォームで「商品名から検索」用。mirror_products (NE 商品マスタ) から検索。
// 各候補に has_rule (pd_shipping_rule にルール登録済みかどうか) を付与。
export function searchProducts(q, limit = 30) {
  const db = ensureSchema();
  const term = norm(q);
  if (!term || term.length < 2) return []; // 1 文字で全件マッチ防止
  // LIKE のワイルドカード(% _ \)を literal 化
  const safe = term.replace(/[\\%_]/g, '\\$&');
  const codeLike = '%' + normProductCode(safe) + '%';
  const nameLike = '%' + safe.toLowerCase() + '%';
  // mirror_products から検索、pd_shipping_rule の有無を LEFT JOIN で確認
  const rows = db.prepare(`
    SELECT lower(trim(p.商品コード)) AS product_code,
           p.商品名 AS product_name,
           p.取扱区分 AS handling,
           CASE WHEN EXISTS(SELECT 1 FROM pd_shipping_rule r
                              WHERE r.product_code = lower(trim(p.商品コード)))
                THEN 1 ELSE 0 END AS has_rule
      FROM mirror_products p
     WHERE p.商品コード IS NOT NULL
       AND (lower(trim(p.商品コード)) LIKE ? ESCAPE '\\'
            OR lower(trim(p.商品名)) LIKE ? ESCAPE '\\')
     ORDER BY p.商品コード
     LIMIT ?
  `).all(codeLike, nameLike, Math.min(Math.max(1, parseInt(limit, 10) || 30), 100));
  return rows;
}

// ═══════════════════════════════════════════════════════════════════
//  追跡番号 / NE反映 (PR 1: CSV取込 + 一覧 + 手動入力 + ready 遷移)
//  PR 2/3 で sync-queue/result API + ミニPC CLI が後続。
// ═══════════════════════════════════════════════════════════════════

// ─── CSV パーサ ───

// キャリア CSV のエンコーディング検出 (ヤマト B2 = Shift-JIS / ゆうパケパフ = UTF-8 が多い)。
// 戦略: ① BOM ありなら UTF-8 ② TextDecoder('utf-8', {fatal:true}) で全バイトが妥当なら UTF-8
// ③ ダメなら CP932 (Shift-JIS)。byte 頻度判定は誤判定が多いので使わない (Codex指摘の Round1)。
function detectAndDecodeCsv(buffer) {
  if (buffer.length >= 3 && buffer[0] === 0xEF && buffer[1] === 0xBB && buffer[2] === 0xBF) {
    return buffer.slice(3).toString('utf8');
  }
  try {
    return new TextDecoder('utf-8', { fatal: true }).decode(buffer);
  } catch {
    return decodeCp932(buffer); // csv.js の iconv-lite 経由
  }
}

// RFC4180 風パーサ (csv.js の parseCsv とロジック同等、こちらは独立コピーで decode 分離)
function parseCsvText(text) {
  const rows = [];
  let row = [], field = '', inQuotes = false, i = 0;
  const n = text.length;
  while (i < n) {
    const c = text[i];
    if (inQuotes) {
      if (c === '"') {
        if (text[i + 1] === '"') { field += '"'; i += 2; continue; }
        inQuotes = false; i++; continue;
      }
      field += c; i++; continue;
    }
    if (c === '"') { inQuotes = true; i++; continue; }
    if (c === ',') { row.push(field); field = ''; i++; continue; }
    if (c === '\r') { i++; continue; }
    if (c === '\n') { row.push(field); rows.push(row); row = []; field = ''; i++; continue; }
    field += c; i++;
  }
  if (field.length > 0 || row.length > 0) { row.push(field); rows.push(row); }
  return rows;
}

// ヤマト B2 出力 CSV パーサ。
// 列番号はヘッダ名から検索する (2026-06-04 hotfix: 固定 col 番号は B2 アカウント設定で
// 並び順が変わるとずれる事故が起きた。ヤマト側で項目追加/削除が起きても堅牢にするため、
// 「送り状種類」「伝票番号」「荷扱い１」のヘッダ名で列を引く)。
// 想定: 送り状種類 = "0" (発払い) / "A" (ネコポス、配送方法変更ログ用)、伝票番号 = 追跡番号、
// 荷扱い１ = NE 伝票番号 (ne_uketsuke_no)
function parseYamatoB2Csv(buffer) {
  const text = detectAndDecodeCsv(buffer);
  const rows = parseCsvText(text);
  if (!rows.length) return { rows: [], errors: ['CSV が空です'] };
  const header = rows[0];
  const errors = [];
  // ヘッダ名で列を検索 (空白除去・全角半角統一)
  const normKey = (s) => norm(s).replace(/\s+/g, '');
  const findIdx = (...names) => {
    const set = new Set(names.map(normKey));
    return header.findIndex(h => set.has(normKey(h)));
  };
  const idxInvoice = findIdx('送り状種類');
  const idxTrack = findIdx('伝票番号');
  const idxNi = findIdx('荷扱い１', '荷扱い1');
  if (idxInvoice < 0) errors.push('「送り状種類」列が見つかりません');
  if (idxTrack < 0) errors.push('「伝票番号」列が見つかりません');
  if (idxNi < 0) errors.push('「荷扱い１」(または「荷扱い1」)列が見つかりません');
  if (errors.length) return { rows: [], errors, header };
  const dataRows = rows.slice(1).filter(r => r.length > Math.max(idxTrack, idxNi) && (r[idxTrack] || r[idxNi]));
  const items = dataRows.map((r, i) => ({
    row_no: i + 2,
    ne_uketsuke_no: norm(r[idxNi]),         // 荷扱い１
    tracking_no: norm(r[idxTrack]),          // 伝票番号
    invoice_type_raw: norm(r[idxInvoice]),   // 送り状種類 (配送方法変更判定用)
    raw: r,
  })).filter(x => x.ne_uketsuke_no && x.tracking_no);
  return { rows: items, errors, header };
}

// ゆうパケットパフ CSV (ゆうプリR 出力): お客様側管理番号 / お問い合わせ番号
function parseYupacketpuffCsv(buffer) {
  const text = detectAndDecodeCsv(buffer);
  const rows = parseCsvText(text);
  if (!rows.length) return { rows: [], errors: ['CSV が空です'] };
  const header = rows[0];
  const errors = [];
  // ヘッダから列番号を引く
  const findIdx = (name) => header.findIndex(h => norm(h).replace(/\s+/g, '') === norm(name).replace(/\s+/g, ''));
  const idxUke = findIdx('お客様側管理番号');
  const idxTrack = findIdx('お問い合わせ番号');
  if (idxUke < 0) errors.push('「お客様側管理番号」列が見つかりません');
  if (idxTrack < 0) errors.push('「お問い合わせ番号」列が見つかりません');
  if (idxUke < 0 || idxTrack < 0) return { rows: [], errors, header };
  const dataRows = rows.slice(1).filter(r => r.length > Math.max(idxUke, idxTrack) && (r[idxUke] || r[idxTrack]));
  const items = dataRows.map((r, i) => ({
    row_no: i + 2,
    ne_uketsuke_no: norm(r[idxUke]),
    tracking_no: norm(r[idxTrack]),
    raw: r,
  })).filter(x => x.ne_uketsuke_no && x.tracking_no);
  return { rows: items, errors, header };
}

const TRACKING_PARSERS = {
  yamato_b2: parseYamatoB2Csv,
  yamato_b2_50: parseYamatoB2Csv,    // 50 サイズ専用も同フォーマット
  yupacketpuff: parseYupacketpuffCsv,
};

// ─── CSV 取込 (キャリア CSV → pd_shipment_tracking 紐付け) ───

// source: 'yamato_b2' | 'yamato_b2_50' | 'yupacketpuff'
// 動作: CSV パース → file_hash 重複チェック → ne_uketsuke_no で pd_shipment_tracking 検索 →
//       マッチしたら tracking_no 紐付け (sync_status=pending のまま、ready 化は別アクション)。
//       既存 tracking_no と異なれば conflict。マッチしなければ unmatched としてカウント。
// source 別の actual_method 判定 (中原さん 2026-06-04 指示の配送方法変更ログ機能用)。
// 戻り値: { method, raw } または { method: null, raw } (unknown)
function judgeActualMethod(source, row) {
  if (source === 'yamato_b2_50') return { method: 'takkyu50', raw: null };
  if (source === 'yupacketpuff') return { method: 'yupacketpuff', raw: null };
  if (source === 'yamato_b2') {
    const raw = row.invoice_type_raw || '';
    if (raw === '0') return { method: 'hatsubarai', raw };
    if (raw === 'A') return { method: 'nekopos', raw };
    return { method: null, raw }; // unknown
  }
  return { method: null, raw: null };
}

export function importTrackingCsv({ source, buffer, filename }, user) {
  if (!TRACKING_PARSERS[source]) throw vErr(`未知の source: ${source}`);
  const db = ensureSchema();
  // file_hash チェックは parser 前に実施 (Codex R1 Medium 1: 種別ミス検出を parse エラーより前に)
  const file_hash = crypto.createHash('sha256').update(buffer).digest('hex');
  const existing = db.prepare(`SELECT import_id FROM pd_tracking_import WHERE source=? AND file_hash=? LIMIT 1`)
    .get(source, file_hash);
  if (existing) {
    return { import_id: existing.import_id, source, row_count: 0,
      matched_count: 0, unmatched_count: 0, conflict_count: 0,
      method_match_count: 0, method_change_count: 0, method_unknown_count: 0, method_skipped_count: 0,
      method_corrected_count: 0,
      duplicate: true, message: 'このファイルは既に取込済みです' };
  }
  // 同じファイルを別 source で取込済みなら警告 (中原さん指示: 種別ミス防止)
  const crossSource = db.prepare(`SELECT source, imported_at FROM pd_tracking_import WHERE file_hash=? LIMIT 1`)
    .get(file_hash);
  if (crossSource) {
    throw vErr(`このファイルは既に別の取込元 (${crossSource.source}) で取込済みです。種別を間違えていませんか?`,
      { other_source: crossSource.source, other_imported_at: crossSource.imported_at });
  }
  // hash check 後に parse
  const parser = TRACKING_PARSERS[source];
  const parsed = parser(buffer);
  if (parsed.errors && parsed.errors.length) {
    throw vErr('CSV の解析に失敗しました', { errors: parsed.errors });
  }
  if (!parsed.rows.length) {
    return { import_id: null, source, row_count: 0, matched_count: 0, unmatched_count: 0, conflict_count: 0,
      method_match_count: 0, method_change_count: 0, method_unknown_count: 0, method_skipped_count: 0,
      method_corrected_count: 0,
      duplicate: false, message: 'データ行がありません' };
  }

  const import_id = crypto.randomUUID();
  const now = utcIsoNow();
  let matched = 0, unmatched = 0, conflict = 0;
  let methodMatch = 0, methodChange = 0, methodUnknown = 0, methodSkipped = 0;
  let methodCorrected = 0;       // 現在の配送方法を CSV 実方法に矯正した件数 (High fix 2026-06-11)
  const unmatchedList = [];
  const conflictList = [];
  const methodCorrectedList = [];

  // pd_shipment_tracking の現状取得 (配送方法変更判定用に method_at_export も含める)
  const sel = db.prepare(`SELECT ne_uketsuke_no, shipping_method_code, tracking_no, tracking_source, sync_status,
    method_at_export, product_summary, shop_name, order_no
    FROM pd_shipment_tracking WHERE ne_uketsuke_no=?`);
  const upd = db.prepare(`UPDATE pd_shipment_tracking
    SET tracking_no=?, tracking_source=?, updated_at=?, updated_by=?
    WHERE ne_uketsuke_no=? AND sync_status IN ('pending','ready','error','conflict','manual_review')`);
  // High fix (2026-06-11): キャリア CSV は「実際にどのキャリアで出荷したか」の権威ソース。
  // judgeActualMethod が確定 (takkyu50/yupacketpuff/hatsubarai/nekopos) し、現在値と食い違うときは
  // 現在の配送方法も実方法に矯正する (誤分類 / 手動誤設定の self-heal、NE反映は shipping_method_code を使う)。
  const updWithMethod = db.prepare(`UPDATE pd_shipment_tracking
    SET shipping_method_code=?, tracking_no=?, tracking_source=?, updated_at=?, updated_by=?
    WHERE ne_uketsuke_no=? AND sync_status IN ('pending','ready','error','conflict','manual_review')`);
  const updConflict = db.prepare(`UPDATE pd_shipment_tracking
    SET sync_status='conflict', last_error=?, updated_at=?, updated_by=?
    WHERE ne_uketsuke_no=? AND sync_status IN ('pending','ready','error','conflict','manual_review')`);
  const insChangeLog = db.prepare(`INSERT INTO pd_method_change_log
    (detected_at, source, import_id, shipment_tracking_ne, ne_uketsuke_no, tracking_no,
     shop_name, order_no, product_summary, original_method, actual_method, actual_method_raw)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`);

  const tx = db.transaction(() => {
    // 親レコード (pd_tracking_import) を先に INSERT。pd_method_change_log の FK 参照を満たすため。
    // counts は後で UPDATE する (Codex R1 Critical 対応)。
    db.prepare(`INSERT INTO pd_tracking_import
      (import_id, source, filename, file_hash, row_count, matched_count, unmatched_count, conflict_count,
       method_match_count, method_change_count, method_unknown_count, method_skipped_count,
       imported_by, imported_at, detail_json)
      VALUES (?,?,?,?,?, 0,0,0, 0,0,0,0, ?,?,?)`).run(
      import_id, source, filename || null, file_hash, parsed.rows.length,
      user || null, now, null);

    for (const r of parsed.rows) {
      const cur = sel.get(r.ne_uketsuke_no);
      if (!cur) {
        unmatched++;
        if (unmatchedList.length < 50) unmatchedList.push({ ne_uketsuke_no: r.ne_uketsuke_no, tracking_no: r.tracking_no, row_no: r.row_no });
        continue;
      }
      // 紐付け更新の可否 (synced は対象外、conflict は別状態に遷移、それ以外は upd)。
      // 配送方法変更判定は紐付けと独立 (Codex R1 High 2 対応): synced であっても KPI 判定する。
      const isSynced = cur.sync_status === 'synced';
      const isConflict = !!cur.tracking_no && cur.tracking_no !== r.tracking_no;
      // judged = CSV が示す実キャリア (物理出荷キャリアの権威ソース)。matched/method 矯正の両方で使う。
      const judged = judgeActualMethod(source, r);
      if (isSynced) {
        unmatched++;
        if (unmatchedList.length < 50) unmatchedList.push({ ne_uketsuke_no: r.ne_uketsuke_no, tracking_no: r.tracking_no, row_no: r.row_no, note: 'synced' });
      } else if (isConflict) {
        conflict++;
        if (conflictList.length < 50) conflictList.push({
          ne_uketsuke_no: r.ne_uketsuke_no, current: cur.tracking_no, incoming: r.tracking_no });
        updConflict.run(`既存=${cur.tracking_no} / 新=${r.tracking_no} (source=${source})`,
          now, user || null, r.ne_uketsuke_no);
      } else if (judged.method && judged.method !== cur.shipping_method_code) {
        // High fix: 現在の配送方法を CSV 実方法に矯正 (tracking も同時に紐付け)。
        const info = updWithMethod.run(judged.method, r.tracking_no, source, now, user || null, r.ne_uketsuke_no);
        if (info.changes > 0) {
          methodCorrected++;
          if (methodCorrectedList.length < 50) methodCorrectedList.push({
            ne_uketsuke_no: r.ne_uketsuke_no, from: cur.shipping_method_code, to: judged.method });
        }
        matched++;
      } else {
        upd.run(r.tracking_no, source, now, user || null, r.ne_uketsuke_no);
        matched++;
      }

      // ── 配送方法変更判定 (matched/synced 関係なく、conflict 以外で method_at_export あり) ──
      // conflict は別の運用問題、変更ログ KPI 対象外。基準は method_at_export (出力時方法) のまま。
      if (isConflict) continue;
      if (!cur.method_at_export) {
        methodSkipped++;
        continue;
      }
      if (!judged.method) {
        methodUnknown++;
        continue;
      }
      if (judged.method === cur.method_at_export) {
        methodMatch++;
      } else {
        methodChange++;
        insChangeLog.run(now, source, import_id, cur.ne_uketsuke_no, cur.ne_uketsuke_no,
          r.tracking_no, cur.shop_name, cur.order_no, cur.product_summary,
          cur.method_at_export, judged.method, judged.raw);
      }
    }
    // counts を更新
    db.prepare(`UPDATE pd_tracking_import
      SET matched_count=?, unmatched_count=?, conflict_count=?,
          method_match_count=?, method_change_count=?, method_unknown_count=?, method_skipped_count=?,
          method_corrected_count=?,
          detail_json=?
      WHERE import_id=?`).run(
      matched, unmatched, conflict,
      methodMatch, methodChange, methodUnknown, methodSkipped,
      methodCorrected,
      JSON.stringify({ unmatched: unmatchedList, conflict: conflictList, method_corrected: methodCorrectedList }),
      import_id);
  });
  tx();
  // 軽い maintenance: 90日超ログ purge (1日1回まで、Codex R2 Medium 1)
  purgeOldMethodChangeLogOncePerDay();
  audit('tracking_import', import_id, { source, filename, file_hash, row_count: parsed.rows.length,
    matched, unmatched, conflict, method_corrected: methodCorrected,
    method_match: methodMatch, method_change: methodChange, method_unknown: methodUnknown, method_skipped: methodSkipped,
    method_corrected_samples: methodCorrectedList.slice(0, 20) }, user);
  return { import_id, source, row_count: parsed.rows.length,
    matched_count: matched, unmatched_count: unmatched, conflict_count: conflict,
    method_match_count: methodMatch, method_change_count: methodChange,
    method_unknown_count: methodUnknown, method_skipped_count: methodSkipped,
    method_corrected_count: methodCorrected,
    duplicate: false, unmatched_samples: unmatchedList.slice(0, 10), conflict_samples: conflictList.slice(0, 10),
    method_corrected_samples: methodCorrectedList.slice(0, 10) };
}

// ─── 手動入力 (レターパック / 定形外 等) ───

// レターパック等は CSV がないので手動で 1 件ずつ or 一括ペースト入力。
// entries: [{ne_uketsuke_no, tracking_no}]、tracking_no が空なら no_tracking 扱い (定形外想定)
export function setTrackingManual({ entries, source }, user) {
  if (!Array.isArray(entries) || !entries.length) throw vErr('entries が空です');
  const valid_sources = ['manual_letterpack', 'no_tracking'];
  if (!valid_sources.includes(source)) throw vErr(`source は ${valid_sources.join(' / ')} のいずれか`);
  const db = ensureSchema();
  const now = utcIsoNow();
  let updated = 0, notFound = 0, conflict = 0, synced = 0;
  const notFoundList = [];
  const conflictList = [];

  const sel = db.prepare(`SELECT ne_uketsuke_no, tracking_no, sync_status FROM pd_shipment_tracking WHERE ne_uketsuke_no=?`);
  const upd = db.prepare(`UPDATE pd_shipment_tracking
    SET tracking_no=?, tracking_source=?, updated_at=?, updated_by=?
    WHERE ne_uketsuke_no=? AND sync_status IN ('pending','ready','error','conflict','manual_review')`);
  const updConflict = db.prepare(`UPDATE pd_shipment_tracking
    SET sync_status='conflict', last_error=?, updated_at=?, updated_by=?
    WHERE ne_uketsuke_no=? AND sync_status IN ('pending','ready','error','conflict','manual_review')`);

  const tx = db.transaction(() => {
    for (const e of entries) {
      const uke = norm(e.ne_uketsuke_no);
      const trk = norm(e.tracking_no);
      if (!uke) { notFound++; continue; }
      const cur = sel.get(uke);
      if (!cur) { notFound++; if (notFoundList.length < 50) notFoundList.push(uke); continue; }
      if (cur.sync_status === 'synced') { synced++; continue; }
      if (cur.tracking_no && trk && cur.tracking_no !== trk) {
        conflict++; if (conflictList.length < 50) conflictList.push({ ne_uketsuke_no: uke, current: cur.tracking_no, incoming: trk });
        updConflict.run(`既存=${cur.tracking_no} / 新=${trk} (source=${source})`, now, user || null, uke);
        continue;
      }
      upd.run(trk || null, source, now, user || null, uke);
      updated++;
    }
  });
  tx();
  audit('tracking_manual', null, { source, updated, notFound, conflict, synced }, user);
  return { updated, not_found: notFound, conflict, synced_skipped: synced,
    not_found_samples: notFoundList.slice(0, 10), conflict_samples: conflictList.slice(0, 10) };
}

// ─── NE 手動反映用エクスポート (2026-06-04 中原さん指示) ───
//
// 運用: NE 反映 cron が未実装のため、当面は手動運用:
//   Step 1: 配送方法ごとに ready 状態の NE 受注番号をコピー → NE で一括検索 → 配送方法を変更
//   Step 2: NE 受注番号 + 追跡番号の 2 列 CSV をダウンロード → NE の追跡番号 CSV インポートに流す
// 対象は sync_status='ready' (= 「これで NE 反映する」ボタンを押し終わった伝票)

// 配送方法ごとの ready 件数 (UI のセレクトボックス表示用)。
// 2026-06-07 中原さん指示: ready 化ボタン廃止、対象を「紐付け済 (linked + ready)」に拡張。
//   linked = pending + (tracking_no あり OR tracking_source='no_tracking')
//   ready = 旧 markReady 経路 (NE 自動反映 cron が止まっているので将来使われない、後方互換のため残置)
const EXPORT_SQL_FILTER = `(
  (sync_status='pending' AND (tracking_no IS NOT NULL OR tracking_source='no_tracking'))
  OR sync_status='ready'
)`;

export function getReadyExportSummary({ scope = 'today' } = {}) {
  const db = ensureSchema();
  // Phase 1 UI 再設計 (2026-06-07): scope='today' で本日のみに絞る
  const todayWhere = scope === 'today'
    ? `AND date(COALESCE(exported_at, added_at), '+9 hours') = date('now', '+9 hours')` : '';
  const rows = db.prepare(`SELECT shipping_method_code,
      COUNT(*) AS total,
      SUM(CASE WHEN tracking_no IS NOT NULL THEN 1 ELSE 0 END) AS with_tracking,
      SUM(CASE WHEN tracking_no IS NULL THEN 1 ELSE 0 END) AS without_tracking
    FROM pd_shipment_tracking
    WHERE ${EXPORT_SQL_FILTER} ${todayWhere}
    GROUP BY shipping_method_code
    ORDER BY shipping_method_code`).all();
  const total = rows.reduce((s, r) => s + r.total, 0);
  const totalWithTracking = rows.reduce((s, r) => s + r.with_tracking, 0);
  return { rows, total, total_with_tracking: totalWithTracking, scope };
}

// 配送方法を指定して紐付け済の NE 受注番号一覧を返す (コピー用)。
// 必ず特定の配送方法を指定すること。'' / 'all' は拒否 (Codex R1 High: NextEngine で
// 一括検索 → 別配送方法を誤更新する事故防止のため、必ず配送方法を絞らせる)。
export function getReadyNeUketsukeNos({ method, scope = 'today' } = {}) {
  if (!method || method === 'all') {
    throw vErr('配送方法を指定してください (コピーは配送方法ごとに行います)');
  }
  const db = ensureSchema();
  // Codex R1 High 3: ready/summary と range が合うよう scope 対応
  const todayWhere = scope === 'today'
    ? ` AND date(COALESCE(exported_at, added_at), '+9 hours') = date('now', '+9 hours')` : '';
  const rows = db.prepare(`SELECT DISTINCT ne_uketsuke_no
    FROM pd_shipment_tracking
    WHERE ${EXPORT_SQL_FILTER} AND shipping_method_code=?${todayWhere}
    ORDER BY ne_uketsuke_no`).all(method);
  return rows.map(r => r.ne_uketsuke_no);
}

// NE 受注番号 + 追跡番号の 2 列 CSV を生成 (UTF-8 BOM 付き、Shift-JIS のため Excel/NE 互換)。
// tracking_no が NULL のもの (定形外/未紐付け) は除外。
// 配送方法フィルタは getReadyNeUketsukeNos と同じ仕様。
export function getReadyTrackingCsv({ method, scope = 'today' } = {}) {
  const db = ensureSchema();
  // Codex R1 High 3: scope 対応
  const todayWhere = scope === 'today'
    ? ` AND date(COALESCE(exported_at, added_at), '+9 hours') = date('now', '+9 hours')` : '';
  const where = (method && method !== 'all')
    ? ` AND shipping_method_code=?` : '';
  const params = (method && method !== 'all') ? [method] : [];
  const rows = db.prepare(`SELECT ne_uketsuke_no, tracking_no, shipping_method_code
    FROM pd_shipment_tracking
    WHERE ${EXPORT_SQL_FILTER} AND tracking_no IS NOT NULL${where}${todayWhere}
    ORDER BY shipping_method_code, ne_uketsuke_no`).all(...params);
  // CSV escape: ダブルクォート囲み + 内部の " は ""
  const esc = (v) => {
    const s = String(v == null ? '' : v);
    return /[",\r\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
  };
  const header = '受注番号,追跡番号';
  const lines = rows.map(r => `${esc(r.ne_uketsuke_no)},${esc(r.tracking_no)}`);
  // UTF-8 BOM + CRLF (Excel/Windows 互換)
  const bom = '﻿';
  return { csv: bom + [header, ...lines].join('\r\n') + '\r\n', count: rows.length };
}

// ─── 状態遷移 ───

// pending → ready 遷移 (「これで反映する」ボタン)。
// 対象: tracking_no がセットされている (定形外/no_tracking 含む) and sync_status='pending'
// 引数: { allPending: true } で全件、または { ids: [...] } で個別
export function markReady({ allPending, ids, scope = 'today' }, user) {
  const db = ensureSchema();
  const now = utcIsoNow();
  let n;
  // Codex R3 High (2026-06-07): scope='today' で本日 JST 分のみ ready 化、過去 hidden row 巻き込み防止。
  // Codex R4 High: fail-closed 化。'all' 明示時のみ全期間、それ以外 (null/typo/欠落) は本日扱い。
  const effectiveScope = scope === 'all' ? 'all' : 'today';
  const todayAnd = effectiveScope === 'today'
    ? ` AND date(COALESCE(exported_at, added_at), '+9 hours') = date('now', '+9 hours')` : '';
  // Codex R5 High: bulk mutation gate も fail-closed。{ allPending: "false" } 等の truthy malformed で
  // ids 指定のつもりが一括 ready 化する事故を防ぐため strict 比較。
  const isAllPending = allPending === true;
  if (isAllPending) {
    n = db.prepare(`UPDATE pd_shipment_tracking
      SET sync_status='ready', sync_requested_at=?, updated_at=?, updated_by=?
      WHERE sync_status='pending'
        AND (tracking_no IS NOT NULL OR tracking_source='no_tracking')${todayAnd}`)
      .run(now, now, user || null).changes;
  } else if (Array.isArray(ids) && ids.length) {
    const placeholders = ids.map(() => '?').join(',');
    n = db.prepare(`UPDATE pd_shipment_tracking
      SET sync_status='ready', sync_requested_at=?, updated_at=?, updated_by=?
      WHERE ne_uketsuke_no IN (${placeholders}) AND sync_status='pending'
        AND (tracking_no IS NOT NULL OR tracking_source='no_tracking')${todayAnd}`)
      .run(now, now, user || null, ...ids).changes;
  } else {
    throw vErr('allPending または ids を指定してください');
  }
  audit('tracking_mark_ready', null, { count: n, mode: isAllPending ? 'all' : 'ids', scope: effectiveScope }, user);
  return { updated: n };
}

// 欠品マーク (skipped 状態へ)
export function markSkipped({ ids, reason }, user) {
  if (!Array.isArray(ids) || !ids.length) throw vErr('ids が空です');
  if (!reason) throw vErr('skip_reason を指定してください');
  const db = ensureSchema();
  const now = utcIsoNow();
  const placeholders = ids.map(() => '?').join(',');
  // synced は除外 (既に反映済みは触らない)
  const n = db.prepare(`UPDATE pd_shipment_tracking
    SET sync_status='skipped', skip_reason=?, updated_at=?, updated_by=?
    WHERE ne_uketsuke_no IN (${placeholders}) AND sync_status IN ('pending','ready','error','conflict','manual_review')`)
    .run(reason, now, user || null, ...ids).changes;
  audit('tracking_mark_skipped', null, { count: n, reason }, user);
  return { updated: n };
}

// ─── 一覧 / サマリ ───

export function trackingSummary({ scope = 'today' } = {}) {
  const db = ensureSchema();
  // 2026-06-07 Phase 1 UI 再設計 (中原さん + Codex 助言、状態駆動・本日デフォルト)
  // - scope='today': 本日 (JST) アップロード分のみ (= exported_at JST = today、未exported は added_at JST = today で補完)
  // - scope='all': 全件 (過去履歴ボタンで使う)
  const todayWhere = scope === 'today'
    ? `WHERE date(COALESCE(exported_at, added_at), '+9 hours') = date('now', '+9 hours')` : '';
  const rows = db.prepare(`SELECT sync_status, COUNT(*) c FROM pd_shipment_tracking ${todayWhere} GROUP BY sync_status`).all();
  const raw = { pending: 0, ready: 0, syncing: 0, synced: 0, error: 0, conflict: 0, skipped: 0, manual_review: 0, ne_unknown: 0, total: 0 };
  for (const r of rows) { raw[r.sync_status] = r.c; raw.total += r.c; }
  // Codex R1 High 2 対応: pending を「未紐付け (waiting)」と「紐付け済 (linked)」に分離。
  // linked = pending + (tracking_no がある OR 定形外マーク tracking_source='no_tracking')
  //        = markReady で一発 ready 化できる、CSV 出力直前段階
  const pendingScopeAnd = scope === 'today'
    ? `AND date(COALESCE(exported_at, added_at), '+9 hours') = date('now', '+9 hours')` : '';
  const pendingNoTracking = db.prepare(`SELECT COUNT(*) c FROM pd_shipment_tracking
    WHERE sync_status='pending' AND tracking_no IS NULL AND (tracking_source IS NULL OR tracking_source != 'no_tracking')
    ${pendingScopeAnd}`).get().c;
  const pendingLinked = db.prepare(`SELECT COUNT(*) c FROM pd_shipment_tracking
    WHERE sync_status='pending' AND (tracking_no IS NOT NULL OR tracking_source='no_tracking')
    ${pendingScopeAnd}`).get().c;
  // 4 状態に正規化 (2026-06-07 中原さん指示で linked と csv_ready 統合):
  //   - 紐付け待ち (waiting): pending + tracking_no NULL + 定形外マーク未
  //   - CSV作成対象 (csv_ready): pending + 紐付け済 (= 旧 linked) OR ready (= markReady ステップ廃止で統合)
  //   - 要確認 (needs_check): conflict / error / manual_review / ne_unknown / syncing
  //   - 対象外 (excluded): skipped
  // NE 自動反映 cron が止まっている (PR #251) ので「ready 化」ボタンは廃止。linked = エクスポート可能。
  // synced (= 過去 API 反映済) は手動 CSV 運用では表示しない
  const buckets = {
    waiting: pendingNoTracking,
    linked: 0,  // 後方互換のため残置、UI では使わない
    csv_ready: pendingLinked + raw.ready,
    needs_check: raw.conflict + raw.error + raw.manual_review + raw.ne_unknown + raw.syncing,
    excluded: raw.skipped,
  };
  buckets.total = buckets.waiting + buckets.csv_ready + buckets.needs_check + buckets.excluded;
  // 業務状態: 完了 = waiting/needs_check が全て 0 かつ csv_ready > 0 (= 残作業なし、CSV ダウンロード可)
  const allDone = buckets.csv_ready > 0
    && buckets.waiting === 0 && buckets.needs_check === 0;
  return {
    scope,
    buckets,                  // 4 状態の件数
    raw,                       // 既存 9 状態 (デバッグ / 詳細表示用)
    pending_no_tracking: pendingNoTracking,  // 後方互換
    all_done: allDone,
  };
}

// listTracking: 2026-06-07 中原さん指示で status フィルタを業務 4 状態に再構成。
// status 値の規約 (linked と csv_ready 統合):
//   group:waiting    - 紐付け待ち (pending + tracking_no NULL + tracking_source != 'no_tracking')
//   group:csv_ready  - CSV作成対象 (旧 linked = pending + 追跡番号 or 定形外マーク) + 旧 ready
//   group:linked     - 後方互換 alias → csv_ready と同じ意味
//   group:needs_check - 要確認 (conflict / error / manual_review / ne_unknown / syncing)
//   group:excluded   - 対象外 (skipped)
//   <未指定>          - すべて
// 後方互換: 既存の単一 status 値 (pending/ready/synced 等) も受理 (詳細データ系で使う場合あり)
export function listTracking({ status, source, shop, q, scope = 'all', limit = 1000 } = {}) {
  const db = ensureSchema();
  const where = [];
  const params = [];
  // Codex R2 High 3 (2026-06-07): summary との件数ズレ対策。scope='today' で本日 JST のみ。
  if (scope === 'today') {
    where.push(`date(COALESCE(exported_at, added_at), '+9 hours') = date('now', '+9 hours')`);
  }
  if (status) {
    if (status === 'group:waiting') {
      where.push(`sync_status='pending' AND tracking_no IS NULL AND (tracking_source IS NULL OR tracking_source != 'no_tracking')`);
    } else if (status === 'group:linked' || status === 'group:csv_ready') {
      // 2026-06-07 中原さん指示: linked + ready 統合 (ready 化ボタン廃止)
      where.push(`(
        (sync_status='pending' AND (tracking_no IS NOT NULL OR tracking_source='no_tracking'))
        OR sync_status='ready'
      )`);
    } else if (status === 'group:needs_check') {
      where.push(`sync_status IN ('conflict','error','manual_review','ne_unknown','syncing')`);
    } else if (status === 'group:excluded') {
      where.push(`sync_status='skipped'`);
    } else {
      // 後方互換 (単一 status)
      where.push('sync_status = ?'); params.push(status);
    }
  }
  if (source) { where.push('tracking_source = ?'); params.push(source); }
  if (shop) { where.push('shop_name = ?'); params.push(shop); }
  if (q) {
    where.push('(ne_uketsuke_no LIKE ? OR tracking_no LIKE ? OR order_no LIKE ?)');
    const like = `%${norm(q)}%`;
    params.push(like, like, like);
  }
  // 2026-06-07 中原さん指示: 表示順は業務 5 状態優先 (未割当 → 要確認 → 紐付け済 → CSV作成対象 → 対象外 → 反映済)。
  // 「未割当が常に一番上に来て、減ったら下がる」運用導線。
  const sql = `SELECT * FROM pd_shipment_tracking
    ${where.length ? 'WHERE ' + where.join(' AND ') : ''}
    ORDER BY (CASE
        WHEN sync_status='pending' AND tracking_no IS NULL AND (tracking_source IS NULL OR tracking_source != 'no_tracking') THEN 0
        WHEN sync_status IN ('error','conflict','manual_review','ne_unknown','syncing') THEN 1
        WHEN sync_status='pending' THEN 2
        WHEN sync_status='ready' THEN 3
        WHEN sync_status='skipped' THEN 4
        WHEN sync_status='synced' THEN 5
        ELSE 6 END),
      updated_at DESC
    LIMIT ?`;
  // Codex R4 High: 負数 / NaN / 0 の defense
  const n = Number.parseInt(limit, 10);
  const safeLimit = Number.isFinite(n) && n > 0 ? Math.min(n, 5000) : 1000;
  return db.prepare(sql).all(...params, safeLimit);
}

export function listTrackingImports(limit = 50) {
  const db = ensureSchema();
  return db.prepare(`SELECT import_id, source, filename, row_count, matched_count, unmatched_count,
    conflict_count, method_corrected_count, imported_by, imported_at
    FROM pd_tracking_import ORDER BY imported_at DESC LIMIT ?`).all(Math.min(limit, 200));
}

export function getTrackingImportDetail(import_id) {
  const db = ensureSchema();
  const row = db.prepare(`SELECT * FROM pd_tracking_import WHERE import_id=?`).get(import_id);
  if (!row) return null;
  const detail = row.detail_json ? JSON.parse(row.detail_json) : null;
  if (!detail) return { ...row, detail: null };
  // 2026-06-04 中原さん要望: 未マッチ/競合の NE 受注番号と注文内容 (ショップ/注文番号/商品名) を見たい。
  // - 競合 (pd_shipment_tracking に既存): pd_shipment_tracking から enrich
  // - 未マッチ (pd_shipment_tracking に該当なし): 過去 48h の pd_import_line から JSON raw_cols 経由で enrich
  //   (TTL 切れ等で見つからなければ null のまま返す)
  const trkSel = db.prepare(`SELECT shop_name, order_no, product_summary, shipping_method_code
    FROM pd_shipment_tracking WHERE ne_uketsuke_no=?`);
  const enrichFromTracking = (uke) => trkSel.get(uke) || null;
  // 未マッチ enrich = 過去 48h の pd_import_line から ne_uketsuke_no (col 20) を引く。
  // Codex R1 High 1: pd_import_line は (batch_id, order_no) しか index がなく、NE受注番号で
  // 直接 WHERE できない (raw_cols は JSON 文字列)。詳細表示のたびに全件 JSON.parse は OOM/timeout 危険。
  // → ① detail の unmatched サンプルから「これから引きたい uke 集合」を先に作る
  //   ② 過去 48h の batch_id ごとに ORDER BY b.uploaded_at DESC で 1 バッチ分ずつ走査
  //   ③ 目的 uke の最初のバッチがヒットしたら以降の古いバッチは触らない
  // Codex R1 High 2: 最新バッチで決定打を取るため、最も新しい batch から見つけた行を採用
  const targetUkes = new Set((detail.unmatched || []).map(x => (x.ne_uketsuke_no || '').trim()).filter(Boolean));
  const enrichFromLinesMap = new Map();
  if (targetUkes.size) {
    // 過去 48h の batch を新しい順に走査
    const recentBatches = db.prepare(`SELECT batch_id FROM pd_import_batch
      WHERE uploaded_at >= datetime('now', '-48 hours')
      ORDER BY uploaded_at DESC`).all();
    const linesByBatch = db.prepare(`SELECT shop_name, order_no, product_code, qty, raw_cols, shipping_method_code
      FROM pd_import_line WHERE batch_id=? AND shipping_method_code IS NOT NULL`);
    for (const b of recentBatches) {
      if (!targetUkes.size) break; // 全 uke 解決済み → 以降のバッチは触らない
      const rows = linesByBatch.all(b.batch_id);
      for (const l of rows) {
        try {
          const raw = JSON.parse(l.raw_cols || '[]');
          const uke = (raw[COL.uketsuke] || '').trim();
          if (!uke || !targetUkes.has(uke)) continue;
          // 同バッチ内で同 uke 複数行 → 配列に集約
          if (!enrichFromLinesMap.has(uke)) enrichFromLinesMap.set(uke, []);
          enrichFromLinesMap.get(uke).push({
            shop_name: l.shop_name, order_no: l.order_no,
            product_code: l.product_code || '',
            product_name: (raw[COL.productName] || '').trim(),
            qty: l.qty || 0,
            shipping_method_code: l.shipping_method_code || null,
          });
        } catch { /* skip malformed JSON */ }
      }
      // このバッチで見つけた uke は targetUkes から除外 (古いバッチに混ぜないため)
      for (const uke of enrichFromLinesMap.keys()) targetUkes.delete(uke);
    }
  }
  const enrichFromLines = (uke) => {
    const lines = enrichFromLinesMap.get(uke);
    if (!lines || !lines.length) return null;
    const items = lines.map(l => `${l.product_code} ${l.product_name} ×${l.qty}`.trim()).join(' ／ ');
    return {
      shop_name: lines[0].shop_name, order_no: lines[0].order_no,
      product_summary: items.length > 500 ? items.slice(0, 500) + '…' : items,
      shipping_method_code: lines[0].shipping_method_code || null,
    };
  };
  const enrichOne = (uke) => enrichFromTracking(uke) || enrichFromLines(uke) || null;
  const enrichedUnmatched = (detail.unmatched || []).map(x => ({ ...x, enrich: enrichOne(x.ne_uketsuke_no) }));
  const enrichedConflict = (detail.conflict || []).map(x => ({ ...x, enrich: enrichOne(x.ne_uketsuke_no) }));
  // Codex R1 High 3: detail サンプルは最大 50 件で truncate されている (importTrackingCsv 内 50 上限)。
  // unmatched_count / conflict_count (永続化された総数) と sample_limit を一緒に返して UI で明示。
  return { ...row, detail: {
    ...detail,
    unmatched: enrichedUnmatched,
    conflict: enrichedConflict,
    sample_limit: 50,
    unmatched_total: row.unmatched_count,
    conflict_total: row.conflict_count,
  }};
}

// ─── 配送方法変更ログ集計 (中原さん 2026-06-04 指示) ───

// 数値パラメータの正規化ヘルパ (Codex R2 Medium 2: NaN/undefined 防御)
function clampInt(v, min, max, def) {
  const n = Number(v);
  if (!Number.isFinite(n)) return def;
  return Math.max(min, Math.min(Math.floor(n), max));
}

// 日付別 / source 別の集計を返す。期間 days (デフォルト 30)。
// 詳細ログ (pd_method_change_log) は 90日 TTL だが、件数集計 (pd_tracking_import) は永続。
export function getMethodChangeSummary({ days = 30 } = {}) {
  const db = ensureSchema();
  const limit = clampInt(days, 1, 365, 30);
  // 日別集計は JST (+9h) で。UTC 18:00-23:59 (JST 翌日 03:00-08:59) の取込が前日扱いになる事故防止 (Codex R1 Medium 3)
  const rows = db.prepare(`SELECT
      date(imported_at, '+9 hours') AS day, source,
      COUNT(*) AS import_count,
      SUM(method_match_count) AS method_match,
      SUM(method_change_count) AS method_change,
      SUM(method_unknown_count) AS method_unknown,
      SUM(method_skipped_count) AS method_skipped
    FROM pd_tracking_import
    WHERE imported_at >= datetime('now', '-' || ? || ' days')
      AND source IN ('yamato_b2', 'yamato_b2_50', 'yupacketpuff')
    GROUP BY date(imported_at, '+9 hours'), source
    ORDER BY day DESC, source`).all(limit);
  // 全期間集計
  const total = db.prepare(`SELECT
      SUM(method_match_count) AS method_match,
      SUM(method_change_count) AS method_change,
      SUM(method_unknown_count) AS method_unknown,
      SUM(method_skipped_count) AS method_skipped
    FROM pd_tracking_import
    WHERE imported_at >= datetime('now', '-' || ? || ' days')
      AND source IN ('yamato_b2', 'yamato_b2_50', 'yupacketpuff')`).get(limit);
  const denom = (total.method_match || 0) + (total.method_change || 0);
  const change_rate = denom > 0 ? Math.round((total.method_change || 0) / denom * 1000) / 10 : 0;
  return { days: limit, rows, total: { ...total, change_rate } };
}

// 配送方法変更の詳細ログ (90日以内、フィルタ可)。
export function listMethodChangeLog({ days = 30, source = '', limit = 500 } = {}) {
  const db = ensureSchema();
  const d = clampInt(days, 1, 90, 30);
  const lim = clampInt(limit, 1, 2000, 500);
  const params = [d];
  let where = `detected_at >= datetime('now', '-' || ? || ' days')`;
  if (source) { where += ` AND source = ?`; params.push(source); }
  const sql = `SELECT * FROM pd_method_change_log
    WHERE ${where}
    ORDER BY detected_at DESC
    LIMIT ?`;
  params.push(lim);
  return db.prepare(sql).all(...params);
}

// 90日超えた詳細ログを purge (件数集計は pd_tracking_import で永続)。
// daily-sync 等で呼び出すことを想定。
export function purgeOldMethodChangeLog() {
  const db = ensureSchema();
  const n = db.prepare(`DELETE FROM pd_method_change_log
    WHERE detected_at < datetime('now', '-90 days')`).run().changes;
  if (n > 0) audit('purge_method_change_log', null, { deleted: n }, 'system');
  return { deleted: n };
}

// 1日1回までに制限した purge (Codex R2 Medium 1: 確率制ではなく決定的に)。
// 最後の purge audit log を見て 24h 以内なら no-op。
function purgeOldMethodChangeLogOncePerDay() {
  try {
    const db = ensureSchema();
    const last = db.prepare(`SELECT ts FROM pd_audit_log
      WHERE action='purge_method_change_log'
      ORDER BY id DESC LIMIT 1`).get();
    if (last && last.ts) {
      const lastTime = new Date(last.ts).getTime();
      if (Date.now() - lastTime < 24 * 3600 * 1000) return; // 24h 以内なら skip
    }
    purgeOldMethodChangeLog();
  } catch {}
}

// 「本日 (JST) アップロードされた取込バッチ」の伝票を配送方法別に抽出 (定形外/レターパック表示用)。
// データソース: pd_import_batch (uploaded_at) + pd_import_line (確定済 / auto / 要判断 すべて含む)。
// 伝票単位 (batch_id + shop_name + order_no) で集約し、商品名/数量を結合表示用に整形。
// 業務理由 (中原さん 2026-06-04): 定形外/レターパックはキャリア CSV から紐付けされないため、
// 手動確認用の一覧が必要。「CSV ボタンを押す直前のやつ」 = pd_import_line で本日 uploaded。
export function listTodayImportLinesByMethod(method) {
  if (!method) throw vErr('method を指定してください');
  const db = ensureSchema();
  const now = new Date();
  // JST 今日 0:00 と 翌日 0:00 を UTC ISO で求める
  const jstNow = new Date(now.getTime() + 9 * 3600 * 1000);
  const ymd = `${jstNow.getUTCFullYear()}-${String(jstNow.getUTCMonth() + 1).padStart(2, '0')}-${String(jstNow.getUTCDate()).padStart(2, '0')}`;
  const fromIso = jstMidnightIso(ymd);
  const toIso = new Date(new Date(fromIso).getTime() + 24 * 3600 * 1000).toISOString();

  // 本日アップロードされた batch のうち、status='exported' 以外は「CSV出力前」、それも含めて表示。
  // (中原さん「CSV ボタンを押す直前のやつ」 = 出力済みも未済も両方表示するという解釈)
  // expires_at で TTL 切れは除外。
  const lines = db.prepare(`
    SELECT l.batch_id, l.row_no, l.order_no, l.shop_name, l.mall_group,
           l.product_code, l.qty, l.shipping_method_code, l.packing_machine_code,
           l.row_status, l.reason_code, l.raw_cols,
           b.uploaded_at, b.status AS batch_status, b.filename
      FROM pd_import_line l
      JOIN pd_import_batch b ON l.batch_id = b.batch_id
     WHERE b.uploaded_at >= ? AND b.uploaded_at < ?
       AND (b.expires_at IS NULL OR b.expires_at >= ?)
       AND l.shipping_method_code = ?
     ORDER BY b.uploaded_at, l.shop_name, l.order_no, l.row_no
  `).all(fromIso, toIso, utcIsoNow(), method);

  // 伝票単位に集約 (batch_id + shop_name + order_no)。
  // batch_id もキーに含めることで、同日中の再アップロード分は別件として扱う (Codex R1 Medium 指摘)。
  // row_status は「要判断 > 確定 > auto」の優先で集約 (listOrders と同方針)。
  const ROW_STATUS_RANK = { '要判断': 0, '確定': 1, 'auto': 2 };
  const byOrder = new Map();
  for (const l of lines) {
    const key = l.batch_id + "_" + l.shop_name + "_" + l.order_no;
    if (!byOrder.has(key)) {
      const raw = JSON.parse(l.raw_cols || '[]');
      byOrder.set(key, {
        batch_id: l.batch_id,
        batch_status: l.batch_status,
        uploaded_at: l.uploaded_at,
        filename: l.filename,
        shop_name: l.shop_name,
        order_no: l.order_no,
        ne_uketsuke_no: norm(raw[COL.uketsuke] || ''),
        pref: norm(raw[COL.pref] || ''),
        recipient: norm(raw[COL.recipient] || ''),
        shipping_method_code: l.shipping_method_code,
        packing_machine_code: l.packing_machine_code,
        row_status: l.row_status,
        items: [],
        total_qty: 0,
      });
    }
    const entry = byOrder.get(key);
    const raw = JSON.parse(l.raw_cols || '[]');
    entry.items.push({
      product_code: l.product_code,
      product_name: norm(raw[COL.productName] || ''),
      qty: l.qty || 0,
    });
    entry.total_qty += (l.qty || 0);
    // row_status は優先度の高い方 (要判断 > 確定 > auto) を採用 (listOrders と同方針)
    const curRank = ROW_STATUS_RANK[entry.row_status] ?? 99;
    const newRank = ROW_STATUS_RANK[l.row_status] ?? 99;
    if (newRank < curRank) entry.row_status = l.row_status;
  }
  return [...byOrder.values()];
}

// (旧) pd_shipment_tracking ベースの配送方法別一覧 (synced/skipped 除外)。
// PR で「pd_import_line ベースに変更」したため未使用。互換のため残す (後で削除予定)。
export function listTrackingByMethod(method) {
  if (!method) throw vErr('method を指定してください');
  const db = ensureSchema();
  return db.prepare(`SELECT * FROM pd_shipment_tracking
    WHERE shipping_method_code = ?
      AND sync_status NOT IN ('synced','skipped')
    ORDER BY added_at DESC, ne_uketsuke_no
    LIMIT 5000`).all(method);
}

// 追跡番号未割当アラート: 配送方法が「定形外」「レターパック」以外で tracking_no が NULL の
// pending/error 伝票。CSV から自動紐付けされるはずだが、CSV にない or 紐付け失敗のケース。
// (ready 状態は tracking_no セット済 or no_tracking なので含めない)
export function listMissingTracking() {
  const db = ensureSchema();
  const rows = db.prepare(`SELECT * FROM pd_shipment_tracking
    WHERE shipping_method_code NOT IN ('teikeigai','letterpack','aes')
      AND tracking_no IS NULL
      AND sync_status IN ('pending','error')
    ORDER BY shipping_method_code, added_at DESC, ne_uketsuke_no
    LIMIT 5000`).all();
  // 配送方法別の件数も併せて返す (アラート表示用)
  const byMethod = {};
  for (const r of rows) byMethod[r.shipping_method_code] = (byMethod[r.shipping_method_code] || 0) + 1;
  return { rows, total: rows.length, by_method: byMethod };
}

// レターパック (or 定形外) の追跡番号を 1 件設定。
// レターパック → tracking_no 必須、source='manual_letterpack'、shipping_method=letterpack のみ
// 定形外 → tracking_no 不要 (no_tracking、配送方法だけ NE 反映)、shipping_method=teikeigai のみ
// 既存 tracking_no と異なる場合は conflict 扱い (sync_status='conflict')。
// pending/ready/error/conflict/manual_review は更新可、syncing/synced/skipped は不可。
// 更新成功時は sync_status='pending' にリセット (error/conflict 修正パスを兼ねる)、last_error クリア。
// 1 件の配送方法変更 (2026-06-07 中原さん指示、レターパック→別配送方法等)。
// サイズ超過等で出荷現場が別配送方法を選んだケース。tracking_no/tracking_source は
// クリアして次の手動入力 or キャリア CSV 取込で紐付け直す形にする。
//
// Codex R1 High 対応:
//   #2: 現在の配送方法が letterpack の行のみ変更可、変更先 letterpack は禁止
//   #3: 変更先 teikeigai (定形外) は今回禁止 (LP→定形外の運用導線が無いため。
//       将来定形外対応を追加するなら、tracking_source='no_tracking' を自動セットして
//       定形外カードに出すフロー設計が必要)
//   #1: UPDATE 後 changes 確認、0 件なら conflict エラー (SELECT/UPDATE 間の race 検出)
// changeShippingMethod (2026-06-07 中原さん指示で waiting 行全対応に拡張):
//   未割当 (waiting = pending + tracking_no NULL + tracking_source != 'no_tracking') の伝票なら
//   レターパック / 定形外 / キャリア (ネコポス・発払い・50サイズ) に手動で変更可。
//   想定: 「ゆうパケットパフにしてたが、サイズオーバーでレターパックに変更」「ネコポスにして追跡番号も入れる」等。
//
//   変更先は letterpack / teikeigai / nekopos / hatsubarai / takkyu50 (2026-06-11 中原さん指示で
//   キャリア 3 種を追加):
//   - 手動で配送方法を確定でき、追跡番号も任意で同時入力できる。
//   - なお、後でキャリア CSV を取り込んだとき、CSV の実方法 (judgeActualMethod) が手動設定と
//     食い違う場合は importTrackingCsv 側で「実方法に矯正」して上書きする (self-heal、
//     method_corrected_count で可視化)。手動設定はあくまで暫定で、CSV (物理出荷の権威) が優先。
//
//   引数:
//     shipping_method_code (new): letterpack / teikeigai / nekopos / hatsubarai / takkyu50
//     tracking_no:
//       - new='teikeigai'  → 引数無視、tracking_source='no_tracking' 自動セット
//       - new='letterpack' → 必須。空ならエラー。tracking_source='manual_letterpack'
//       - new=キャリア      → 任意。入力あり→tracking_source='manual_carrier' で linked、
//                            空なら tracking_no/source とも NULL のまま waiting (キャリア CSV 取込が後で紐付け)
const ALLOWED_CHANGE_TARGETS = new Set(['letterpack', 'teikeigai', 'nekopos', 'hatsubarai', 'takkyu50']);
export function changeShippingMethod({ ne_uketsuke_no, shipping_method_code, tracking_no }, user) {
  const uke = norm(ne_uketsuke_no);
  if (!uke) throw vErr('ne_uketsuke_no が空です');
  if (!shipping_method_code) throw vErr('shipping_method_code が空です');
  // 変更先 whitelist (Codex R2 High: パーサーなし method 廃止)
  if (!ALLOWED_CHANGE_TARGETS.has(shipping_method_code)) {
    throw vErr(`変更先は ${[...ALLOWED_CHANGE_TARGETS].join(' / ')} のみ対応 (指定=${shipping_method_code})`);
  }
  const db = ensureSchema();
  const sm = db.prepare(`SELECT code, is_locked FROM pd_shipping_method WHERE code=?`).get(shipping_method_code);
  if (!sm) throw vErr(`未知の配送方法コード: ${shipping_method_code}`);
  if (sm.is_locked) throw vErr(`配送方法 ${shipping_method_code} は変更対象外 (lock)`);
  const cur = db.prepare(`SELECT sync_status, shipping_method_code, tracking_no, tracking_source
    FROM pd_shipment_tracking WHERE ne_uketsuke_no=?`).get(uke);
  if (!cur) throw vErr(`受注 ${uke} は pd_shipment_tracking にありません`);
  // Codex R1 High 1: 元 method も lock / aes 拒否 (AES 行を抜け道で逃さない)
  if (cur.shipping_method_code === 'aes') {
    throw vErr('AES 行はこの機能で変更できません');
  }
  const curSm = db.prepare(`SELECT is_locked FROM pd_shipping_method WHERE code=?`).get(cur.shipping_method_code);
  if (curSm && curSm.is_locked) {
    throw vErr(`現在の配送方法 ${cur.shipping_method_code} は lock のため変更できません`);
  }
  // waiting (未割当) 状態のみ変更可: pending + tracking_no NULL + tracking_source != 'no_tracking'
  const isWaiting = cur.sync_status === 'pending'
    && cur.tracking_no == null
    && cur.tracking_source !== 'no_tracking';
  if (!isWaiting) {
    throw vErr(`未割当 (waiting) 状態の伝票のみ変更できます (現在=${cur.sync_status}, tracking_no=${cur.tracking_no ? 'あり' : 'なし'}, source=${cur.tracking_source || 'なし'})`);
  }
  // 新配送方法別の tracking 設定
  //   teikeigai            : 追跡なし (no_tracking) で linked
  //   letterpack           : 追跡番号 必須 (manual_letterpack)
  //   nekopos/hatsubarai/takkyu50 (キャリア): 追跡番号 任意。
  //     入力あり → manual_carrier で linked。
  //     入力なし → tracking_no/source とも NULL のまま waiting で残し、後でキャリア CSV 取込が紐付ける。
  let newTrk = null;
  let newSource = null;
  const trkInput = norm(tracking_no);
  if (shipping_method_code === 'teikeigai') {
    newTrk = null;
    newSource = 'no_tracking';
  } else if (shipping_method_code === 'letterpack') {
    // Codex R1 High 4: LP 変更は追跡番号必須。空だと LP カード (pd_import_line 起点) に出ず宙ぶらりんになる。
    if (!trkInput) throw vErr('レターパックへ変更する場合は追跡番号が必須です');
    newTrk = trkInput;
    newSource = 'manual_letterpack';
  } else {
    // キャリア (nekopos/hatsubarai/takkyu50、whitelist で他は到達しない): 追跡番号は任意
    if (trkInput) { newTrk = trkInput; newSource = 'manual_carrier'; }
    else { newTrk = null; newSource = null; } // waiting のまま、キャリア CSV 取込で後から紐付け
  }
  if (cur.shipping_method_code === shipping_method_code && cur.tracking_no === newTrk
      && (cur.tracking_source || null) === newSource) {
    return { status: 'no_change', message: '既に同じ配送方法・状態です' };
  }
  const now = utcIsoNow();
  // Codex R1 High 2: WHERE に現 method を含めて CAS race 検出。
  // waiting 条件再確認 + 元 method 一致確認で stale 上書きを防ぐ。
  const info = db.prepare(`UPDATE pd_shipment_tracking
    SET shipping_method_code = ?,
        tracking_no = ?,
        tracking_source = ?,
        sync_status = 'pending',
        sync_requested_at = NULL,
        last_error = NULL, last_error_code = NULL,
        claim_id = NULL, claim_token_hash = NULL, claim_expires_at = NULL,
        updated_at = ?, updated_by = ?
    WHERE ne_uketsuke_no=?
      AND shipping_method_code=?
      AND sync_status='pending'
      AND tracking_no IS NULL
      AND (tracking_source IS NULL OR tracking_source != 'no_tracking')`)
    .run(shipping_method_code, newTrk, newSource, now, user || null, uke, cur.shipping_method_code);
  if (info.changes === 0) {
    throw vErr(`変更が反映されませんでした (他の操作で状態が変わった可能性、再読込してください)`);
  }
  audit('change_shipping_method', uke,
    { old_method: cur.shipping_method_code, new_method: shipping_method_code,
      tracking_no: newTrk, tracking_source: newSource }, user);
  return { status: 'ok', old_method: cur.shipping_method_code, new_method: shipping_method_code,
    tracking_no: newTrk, tracking_source: newSource };
}

export function setTrackingOne({ ne_uketsuke_no, tracking_no, source }, user) {
  const uke = norm(ne_uketsuke_no);
  if (!uke) throw vErr('ne_uketsuke_no が空です');
  // source ごとに対象配送方法を限定 (CSV取込→紐付けと整合する source のみ受け付ける)。
  //   manual_letterpack → letterpack
  //   no_tracking       → teikeigai (追跡なし)
  //   manual_carrier    → nekopos / hatsubarai / takkyu50 (キャリア手入力の追跡番号 修正用、2026-06-11)
  const SOURCE_TO_METHODS = {
    manual_letterpack: ['letterpack'],
    no_tracking: ['teikeigai'],
    manual_carrier: ['nekopos', 'hatsubarai', 'takkyu50'],
  };
  const allowedMethods = SOURCE_TO_METHODS[source];
  if (!allowedMethods) throw vErr(`source は ${Object.keys(SOURCE_TO_METHODS).join(' / ')} のいずれか`);
  const trk = source === 'no_tracking' ? null : norm(tracking_no);
  if (source !== 'no_tracking' && !trk) throw vErr('追跡番号が必要です');
  const db = ensureSchema();
  const now = utcIsoNow();
  const cur = db.prepare(`SELECT shipping_method_code, tracking_no, sync_status
    FROM pd_shipment_tracking WHERE ne_uketsuke_no=?`).get(uke);
  if (!cur) throw vErr('該当の伝票が見つかりません');
  // 配送方法ガード (CSV取込→紐付けと整合する source のみ受け付ける)
  if (!allowedMethods.includes(cur.shipping_method_code)) {
    throw vErr(`source=${source} は ${allowedMethods.join(' / ')} の伝票にのみ使えます (この伝票=${cur.shipping_method_code})`);
  }
  // 更新可能な状態のみ受け付ける
  const editableStatuses = ['pending', 'ready', 'error', 'conflict', 'manual_review'];
  if (!editableStatuses.includes(cur.sync_status)) {
    throw vErr(`状態が ${cur.sync_status} のため変更できません (編集可: ${editableStatuses.join(',')})`);
  }
  // manual_carrier は「修正導線」(スタッフが明示的に打ち直す) なので、既存値と違っても
  // conflict にせず上書きする (Codex R2 High 対応)。CSV 起点/レターパック手入力など他 source は
  // 既存値との差分を運用ミスとして conflict 記録する従来挙動を維持。
  if (source !== 'manual_carrier' && cur.tracking_no && trk && cur.tracking_no !== trk) {
    // 構成 4: positive whitelist (pending/ready/error/conflict/manual_review のみ)。
    // syncing/synced/skipped/ne_unknown は触らない。
    db.prepare(`UPDATE pd_shipment_tracking
      SET sync_status='conflict', last_error=?, claim_id=NULL,
          updated_at=?, updated_by=?
      WHERE ne_uketsuke_no=? AND sync_status IN ('pending','ready','error','conflict','manual_review')`).run(
      `既存=${cur.tracking_no} / 新=${trk} (source=${source})`, now, user || null, uke);
    audit('tracking_set_one_conflict', uke, { source, existing: cur.tracking_no, incoming: trk }, user);
    return { status: 'conflict', message: '既存の追跡番号と異なるため conflict として記録しました', existing: cur.tracking_no };
  }
  // 通常更新: sync_status を pending に戻す + last_error / claim 系をクリア (error/conflict 修正経路)
  db.prepare(`UPDATE pd_shipment_tracking
    SET tracking_no=?, tracking_source=?,
        sync_status='pending', sync_requested_at=NULL,
        last_error=NULL, last_error_code=NULL,
        claim_id=NULL, syncing_started_at=NULL,
        updated_at=?, updated_by=?
    WHERE ne_uketsuke_no=? AND sync_status IN ('pending','ready','error','conflict','manual_review')`)
    .run(trk, source, now, user || null, uke);
  audit('tracking_set_one', uke, { source, has_tracking: !!trk }, user);
  return { status: 'ok' };
}

// ───────────────────────── マスタ移行 (manual → meltline 一括書き換え) ─────────────────────────
//
// 目的: 2026-06-03 の meltline 本稼働に合わせ、既存「ネコポス × 手動出荷」設定を「ネコポス × meltline」
//   に一括書き換え。pd_shipping_rule と pd_assort_decision (is_active=1) の両方を直接 UPDATE。
//   レコード数不変。バックアップ JSON + audit log + ロールバック endpoint を併設。
// 設計レビュー: Codex gpt-5.5 (Critical なし、High 3件対応済)。

// dryRun トークンの揮発キャッシュ (5分有効)。同一プロセス内のみ有効、再起動で消えて OK (再 dryRun で再発行)。
const MIGRATION_TOKEN_TTL_MS = 5 * 60 * 1000;
const _migrationTokens = new Map(); // token -> { issued_at, target_ids: { rule:[], assort:[] } }

function _purgeOldMigrationTokens() {
  const cutoff = Date.now() - MIGRATION_TOKEN_TTL_MS;
  for (const [k, v] of _migrationTokens) if (v.issued_at < cutoff) _migrationTokens.delete(k);
}

// バックアップ保存先 (Render の永続ディスクが無いので process.cwd() の data/backup 配下、後で Web Shell でも取れる)。
function _backupDir() {
  const base = process.env.DATA_DIR || path.join(process.cwd(), 'data');
  const dir = path.join(base, 'backup', 'packing-dispatch');
  fs.mkdirSync(dir, { recursive: true });
  return dir;
}

const MIGRATION_MANUAL_TO_MELTLINE = 'manual_to_meltline_v1';
// 許可するバックアップファイル名 (whitelist 強化、path traversal 防御)
const BACKUP_FILENAME_RE = /^meltline_migration_[\w.\-]+\.json$/;
// commit 済み marker (filesystem 上の .committed ファイル)
function _committedMarker(backupPath) { return backupPath + '.committed'; }

// backup file が commit 済みかを判定 (marker or audit log のいずれかで OK)。
// marker 書込みが I/O 失敗した場合のフォールバック。audit は json_extract で完全一致。
function _isBackupCommitted(db, fullPath, backupFilename) {
  if (fs.existsSync(_committedMarker(fullPath))) return true;
  const row = db.prepare(`SELECT 1 FROM pd_audit_log
    WHERE action='meltline_migration'
      AND json_extract(detail, '$.backup_file') = ? LIMIT 1`).get(backupFilename);
  return !!row;
}

// 対象集合の fingerprint (id + content の sha256)。dry-run 後に内容が変わったケースまで検出する。
function _targetsFingerprint({ rules, assorts }) {
  const r = rules.map(x => [x.id, x.packing_machine_code, x.shipping_method_code, x.updated_at]);
  const a = assorts.map(x => [x.id, x.packing_machine_code, x.shipping_method_code, x.decided_at]);
  return crypto.createHash('sha256').update(JSON.stringify({ r, a })).digest('hex');
}

// 進行中バッチチェック (busy/import/export 中なら拒否)。txFn 内で再チェックも行う。
// expired (TTL 切れ) は除外して「実際に進行中」のみ拾う。
function _assertNoBusyBatch(db) {
  const now = utcIsoNow();
  const busy = db.prepare(`SELECT batch_id, status FROM pd_import_batch
    WHERE status IN ('classifying','locked_for_export')
      AND (expires_at IS NULL OR expires_at >= ?)
    LIMIT 5`).all(now);
  if (busy.length > 0) throw vErr('進行中の取込バッチがあるため移行できません。先に確定・出力を済ませてください。', { busy });
}

// audit log を throw 版で記録 (移行系は audit 失敗を見逃せない)。
function _auditOrThrow(action, target, detail, who) {
  const db = ensureSchema();
  const info = db.prepare(`INSERT INTO pd_audit_log (ts,who,action,target,detail) VALUES (?,?,?,?,?)`)
    .run(utcIsoNow(), who || null, action, target || null, detail ? JSON.stringify(detail) : null);
  if (!info.changes) throw new Error(`audit log 書込み失敗 (${action})`);
}

// 対象集合を確定する内部関数 (BEGIN IMMEDIATE 下でも、dryRun でも共通)。
// assort はあとで item も付ける (完全 backup のため)。
function _selectMeltlineTargets(db) {
  const rules = db.prepare(`
    SELECT id, sku_key, product_code, mall_group, qty_min, qty_max,
           shipping_method_code, packing_machine_code, updated_at, updated_by
      FROM pd_shipping_rule
     WHERE shipping_method_code='nekopos' AND packing_machine_code='manual'
     ORDER BY id
  `).all();
  const assorts = db.prepare(`
    SELECT id, combo_key, combo_key_version, combo_detail,
           shipping_method_code, packing_machine_code, is_active, decided_by, decided_at
      FROM pd_assort_decision
     WHERE shipping_method_code='nekopos' AND packing_machine_code='manual' AND is_active=1
     ORDER BY id
  `).all();
  return { rules, assorts };
}

// assort の item を id 群で取得 (backup 完全性のため)。
function _selectAssortItems(db, decisionIds) {
  if (!decisionIds.length) return [];
  const placeholders = decisionIds.map(() => '?').join(',');
  return db.prepare(`SELECT decision_id, sku_key, qty FROM pd_assort_decision_item
    WHERE decision_id IN (${placeholders}) ORDER BY decision_id, sku_key`).all(...decisionIds);
}

/**
 * dry-run: 対象件数・モール別内訳・サンプル + 5分有効な dryRunToken を返す。
 * 実行時はこの token と確認文言 "メルトライン移行を実行" を要求する。
 */
export function getMeltlineMigrationPreview() {
  const db = ensureSchema();
  _assertNoBusyBatch(db);
  // ターゲットチェック: meltline コードと nekopos コードが存在することを確認
  const pmExists = db.prepare(`SELECT code FROM pd_packing_machine WHERE code='meltline'`).get();
  const smExists = db.prepare(`SELECT code FROM pd_shipping_method WHERE code='nekopos'`).get();
  if (!pmExists) throw vErr('pd_packing_machine に meltline が未登録です');
  if (!smExists) throw vErr('pd_shipping_method に nekopos が未登録です');

  const { rules, assorts } = _selectMeltlineTargets(db);
  const byMall = {};
  for (const r of rules) byMall[r.mall_group] = (byMall[r.mall_group] || 0) + 1;

  if (rules.length === 0 && assorts.length === 0) {
    return {
      already_migrated: true,
      rule: { total: 0, by_mall_group: {}, samples: [] },
      assort: { total: 0, samples: [] },
      dryRunToken: null,
    };
  }

  _purgeOldMigrationTokens();
  const dryRunToken = crypto.randomBytes(24).toString('hex');
  _migrationTokens.set(dryRunToken, {
    issued_at: Date.now(),
    target_ids: { rule: rules.map(r => r.id), assort: assorts.map(a => a.id) },
    fingerprint: _targetsFingerprint({ rules, assorts }),
  });

  return {
    already_migrated: false,
    rule: {
      total: rules.length,
      by_mall_group: byMall,
      samples: rules.slice(0, 10).map(r => ({
        sku_key: r.sku_key, mall_group: r.mall_group, qty_min: r.qty_min, qty_max: r.qty_max,
      })),
    },
    assort: {
      total: assorts.length,
      samples: assorts.slice(0, 10).map(a => ({
        combo_key: a.combo_key, combo_detail: a.combo_detail,
      })),
    },
    dryRunToken,
    dryRunTokenTtlSec: Math.floor(MIGRATION_TOKEN_TTL_MS / 1000),
  };
}

/**
 * 本実行: BEGIN IMMEDIATE で書込ロック → 対象 id 確定 → backup JSON → UPDATE → post-check → COMMIT。
 * dryRunToken と confirm 文言 ('メルトライン移行を実行') が必須。
 */
export function executeMeltlineMigration({ dryRunToken, confirm }, user) {
  if (confirm !== 'メルトライン移行を実行') throw vErr('確認文言が一致しません');
  _purgeOldMigrationTokens();
  const tok = _migrationTokens.get(dryRunToken);
  if (!tok) throw vErr('dryRunToken が無効または期限切れです (5分以内に取り直してください)');

  const db = ensureSchema();

  const migration_id = crypto.randomUUID();
  const now = utcIsoNow();
  const ts = now.replace(/[:.]/g, '-').replace('T', '_').slice(0, 19);
  const backupFilename = `meltline_migration_${ts}_${migration_id.slice(0, 8)}.json`;
  if (!BACKUP_FILENAME_RE.test(backupFilename)) throw new Error('生成した backup filename が whitelist に合いません');
  const backupPath = path.join(_backupDir(), backupFilename);

  // BEGIN IMMEDIATE 相当: better-sqlite3 の transaction('immediate') で書込ロック取得
  // (better-sqlite3 v8 以降は db.transaction(fn).immediate() で呼ぶ)
  const txFn = db.transaction(() => {
    // 0. lock 取得後に busy check を再実施 (lock 外でのチェックは race を残す)
    _assertNoBusyBatch(db);
    // 1. 対象 id を再確定 (dryRun 時から状態が変わっていれば差分を検出)
    const { rules, assorts } = _selectMeltlineTargets(db);
    const assortItems = _selectAssortItems(db, assorts.map(a => a.id));
    const ruleIdsNow = new Set(rules.map(r => r.id));
    const assortIdsNow = new Set(assorts.map(a => a.id));
    const ruleIdsToken = new Set(tok.target_ids.rule);
    const assortIdsToken = new Set(tok.target_ids.assort);
    const ruleDiffAdded = [...ruleIdsNow].filter(x => !ruleIdsToken.has(x));
    const ruleDiffRemoved = [...ruleIdsToken].filter(x => !ruleIdsNow.has(x));
    const assortDiffAdded = [...assortIdsNow].filter(x => !assortIdsToken.has(x));
    const assortDiffRemoved = [...assortIdsToken].filter(x => !assortIdsNow.has(x));
    if (ruleDiffAdded.length || ruleDiffRemoved.length || assortDiffAdded.length || assortDiffRemoved.length) {
      throw vErr('dry-run 時から対象が変わっています。再度 dry-run を取り直してください', {
        diff: { ruleAdded: ruleDiffAdded.length, ruleRemoved: ruleDiffRemoved.length,
          assortAdded: assortDiffAdded.length, assortRemoved: assortDiffRemoved.length },
      });
    }
    // 内容 fingerprint も一致確認 (集合は同じだが内容が編集されたケース検出)
    const fpNow = _targetsFingerprint({ rules, assorts });
    if (fpNow !== tok.fingerprint) {
      throw vErr('dry-run 時から対象行の内容が変わっています。再度 dry-run を取り直してください');
    }

    // 2. backup JSON を書き込み (write + fsync + dir fsync + 存在確認)
    //    assort_items も含める (完全 backup)。executed_at は UPDATE の updated_at と同値にして
    //    rollback 時の「移行後に触られた行かどうか」判定に使う。
    const backup = {
      migration_id, migration_type: MIGRATION_MANUAL_TO_MELTLINE,
      executed_at: now, executed_by: user || null,
      predicate: { shipping_method_code: 'nekopos', packing_machine_code: 'manual' },
      counts: { rule: rules.length, assort: assorts.length, assort_items: assortItems.length },
      rules, assorts, assort_items: assortItems,
    };
    const tmpPath = backupPath + '.tmp';
    const fd = fs.openSync(tmpPath, 'w');
    try {
      fs.writeFileSync(fd, JSON.stringify(backup, null, 2), 'utf8');
      fs.fsyncSync(fd);
    } finally {
      fs.closeSync(fd);
    }
    fs.renameSync(tmpPath, backupPath);
    if (!fs.existsSync(backupPath)) throw new Error('backup JSON の保存に失敗しました');
    // ディレクトリの metadata flush (POSIX) - Windows では noop だが安全側で
    try {
      const dirFd = fs.openSync(_backupDir(), 'r');
      try { fs.fsyncSync(dirFd); } catch {} finally { fs.closeSync(dirFd); }
    } catch {}

    // 3. UPDATE rule
    const ruleUpd = db.prepare(`
      UPDATE pd_shipping_rule SET packing_machine_code='meltline', updated_at=?, updated_by=?
       WHERE shipping_method_code='nekopos' AND packing_machine_code='manual'
    `).run(now, user || null);

    // 4. UPDATE assort
    const assortUpd = db.prepare(`
      UPDATE pd_assort_decision SET packing_machine_code='meltline', decided_at=?, decided_by=?
       WHERE shipping_method_code='nekopos' AND packing_machine_code='manual' AND is_active=1
    `).run(now, user || null);

    // 5. post-check: nekopos + manual の残数が 0 件であること
    const ruleRemain = db.prepare(`SELECT COUNT(*) c FROM pd_shipping_rule
      WHERE shipping_method_code='nekopos' AND packing_machine_code='manual'`).get().c;
    const assortRemain = db.prepare(`SELECT COUNT(*) c FROM pd_assort_decision
      WHERE shipping_method_code='nekopos' AND packing_machine_code='manual' AND is_active=1`).get().c;
    if (ruleRemain !== 0 || assortRemain !== 0) {
      throw new Error(`post-check 失敗: 残 rule=${ruleRemain}, assort=${assortRemain} (ロールバック)`);
    }
    // 6. 件数突合 (UPDATE 件数 = dryRun token 時の対象数)
    if (ruleUpd.changes !== rules.length || assortUpd.changes !== assorts.length) {
      throw new Error(`件数不一致: rule updated=${ruleUpd.changes} expected=${rules.length} / assort updated=${assortUpd.changes} expected=${assorts.length}`);
    }

    // 7. audit log (まとめて 1 件)。失敗したら tx ロールバック (例外を握りつぶさない)
    _auditOrThrow('meltline_migration', migration_id, {
      rule_count: ruleUpd.changes, assort_count: assortUpd.changes,
      backup_path: backupPath, backup_file: backupFilename,
    }, user);

    return { migration_id, rule_count: ruleUpd.changes, assort_count: assortUpd.changes,
      backup_file: backupFilename, backup_path: backupPath };
  });
  // better-sqlite3 の immediate mode: 書込ロック取得
  let result;
  try {
    result = txFn.immediate();
  } catch (e) {
    // tx 内で throw → backup JSON は filesystem 上に残る可能性がある。orphan を削除。
    try { if (fs.existsSync(backupPath)) fs.unlinkSync(backupPath); } catch {}
    try { if (fs.existsSync(backupPath + '.tmp')) fs.unlinkSync(backupPath + '.tmp'); } catch {}
    throw e;
  }
  // 8. tx COMMIT 成功後に .committed marker を作成 (filesystem 側の orphan 検知に利用)。
  try { fs.writeFileSync(_committedMarker(backupPath), JSON.stringify({ committed_at: utcIsoNow() }), 'utf8'); } catch {}

  // token は使い切り
  _migrationTokens.delete(dryRunToken);
  return result;
}

/**
 * ロールバック: backup JSON を読んで、UPDATE 対象が現在 meltline であることを検証してから
 *   元の packing_machine_code に戻す。同じく BEGIN IMMEDIATE。
 */
export function rollbackMeltlineMigration({ backup_file, confirm }, user) {
  if (confirm !== 'メルトライン移行をロールバック') throw vErr('確認文言が一致しません');
  if (!backup_file || !BACKUP_FILENAME_RE.test(backup_file)) {
    throw vErr('backup_file が不正です');
  }
  const fullPath = path.join(_backupDir(), backup_file);
  if (!fs.existsSync(fullPath)) throw vErr('backup ファイルが見つかりません');
  // commit 判定 (marker or audit log。marker 書込み失敗の fail-safe で audit log も見る)
  if (!_isBackupCommitted(ensureSchema(), fullPath, backup_file)) {
    throw vErr('この backup は commit 確定マーカーが見つかりません (実行中断・失敗した可能性)');
  }
  const backup = JSON.parse(fs.readFileSync(fullPath, 'utf8'));
  if (backup.migration_type !== MIGRATION_MANUAL_TO_MELTLINE) throw vErr('backup の種別が違います');
  if (!backup.executed_at) throw vErr('backup に executed_at がありません');

  const db = ensureSchema();
  const now = utcIsoNow();

  const txFn = db.transaction(() => {
    _assertNoBusyBatch(db);
    let ruleRollback = 0, assortRollback = 0, ruleSkippedTouched = 0, assortSkippedTouched = 0;
    // rule: 現在値が meltline かつ updated_at = backup.executed_at (= 移行後に再 UPDATE されていない) のみ戻す。
    // 移行後に手動で再編集された行は対象外 (ユーザーの上書きを潰さない)。
    const updRule = db.prepare(`
      UPDATE pd_shipping_rule SET packing_machine_code=?, updated_at=?, updated_by=?
       WHERE id=? AND shipping_method_code='nekopos' AND packing_machine_code='meltline'
         AND updated_at = ?
    `);
    for (const r of backup.rules || []) {
      const info = updRule.run(r.packing_machine_code || 'manual', now, user || null, r.id, backup.executed_at);
      if (info.changes) ruleRollback += info.changes;
      else ruleSkippedTouched++;
    }
    // assort: 同様に decided_at = backup.executed_at で守る
    const updAssort = db.prepare(`
      UPDATE pd_assort_decision SET packing_machine_code=?, decided_at=?, decided_by=?
       WHERE id=? AND shipping_method_code='nekopos' AND packing_machine_code='meltline' AND is_active=1
         AND decided_at = ?
    `);
    for (const a of backup.assorts || []) {
      const info = updAssort.run(a.packing_machine_code || 'manual', now, user || null, a.id, backup.executed_at);
      if (info.changes) assortRollback += info.changes;
      else assortSkippedTouched++;
    }
    _auditOrThrow('meltline_migration_rollback', backup.migration_id, {
      backup_file, rule_rolled_back: ruleRollback, assort_rolled_back: assortRollback,
      rule_skipped_touched: ruleSkippedTouched, assort_skipped_touched: assortSkippedTouched,
      rule_in_backup: (backup.rules || []).length, assort_in_backup: (backup.assorts || []).length,
    }, user);
    return { migration_id: backup.migration_id, rule_rolled_back: ruleRollback,
      assort_rolled_back: assortRollback,
      rule_skipped_touched: ruleSkippedTouched, assort_skipped_touched: assortSkippedTouched,
      rule_in_backup: (backup.rules || []).length, assort_in_backup: (backup.assorts || []).length };
  });
  return txFn.immediate();
}

/** バックアップ一覧 (admin endpoint から呼ぶ)。.committed marker or audit log 確定で committed=true */
export function listMeltlineBackups() {
  const dir = _backupDir();
  if (!fs.existsSync(dir)) return [];
  const db = ensureSchema();
  return fs.readdirSync(dir)
    .filter(f => BACKUP_FILENAME_RE.test(f))
    .map(f => {
      const full = path.join(dir, f);
      const stat = fs.statSync(full);
      const committed = _isBackupCommitted(db, full, f);
      return { file: f, size: stat.size, mtime: stat.mtime.toISOString(), committed };
    })
    .sort((a, b) => (a.mtime < b.mtime ? 1 : -1));
}

/** バックアップ JSON を返す (admin endpoint から呼ぶ。filename whitelist で path traversal 防御) */
export function readMeltlineBackup(filename) {
  if (!filename || !BACKUP_FILENAME_RE.test(filename)) {
    throw vErr('filename が不正です');
  }
  const fullPath = path.join(_backupDir(), filename);
  if (!fs.existsSync(fullPath)) throw vErr('backup ファイルが見つかりません');
  return fs.readFileSync(fullPath, 'utf8');
}

// ═══════════════════════════════════════════════════════════════════
//  NE 反映ジョブ (Phase 2, 2026-06-06、構成 4: simple-hybrid + ne_unknown)
//
//  運用: 中原さんが 1 日 1 回 (夕方) UI ボタンで起動 → ミニPC worker が一括反映。
//
//  状態遷移:
//    pending → ready → syncing → synced              ← 正常完了
//                              → manual_review       ← NE 業務エラー (受注なし、キャンセル等)
//                              → ne_unknown          ← NE 反映成功後の DB / ack / クラッシュ
//
//  ルール:
//    - ne_unknown は自動 retry しない (= 滞留問題なし、中原さんが翌日確認)
//    - syncing 中の payload を巻き戻す経路を作らない (positive whitelist で防ぐ)
//    - claim_token / late-results / TTL release / journal は廃止 (構成 B から簡素化)
//    - Bearer 認証 (CF Access + RENDER_NE_SYNC_WORKER_KEY) で worker を保護
//    - 1 run = 1 attempt_id (= run_id を流用)、行ごとには発行しない
// ═══════════════════════════════════════════════════════════════════

const NE_SYNC_RUN_EXPIRY_MIN = 60; // running が 60 分以上更新なし → 次回 start で expired、syncing は ne_unknown

// 「Render が触ってよい状態」の positive whitelist (Codex R5 助言)
// payload を変える経路 (CSV 出力、setTrackingManual 等) は必ずこのいずれかでガードする
const PD_EDITABLE_STATUSES = ['pending', 'ready', 'error', 'conflict', 'manual_review'];
const PD_EDITABLE_PLACEHOLDERS = PD_EDITABLE_STATUSES.map(() => '?').join(',');

/**
 * NE 反映 run を開始 (UI ボタンから 1 回ずつ)。単一起動ロック: running が既にあれば 409。
 * 古い running (30 分以上更新なし) は expired として finalize してから新規開始。
 */
/**
 * NE 反映 run を開始 (UI ボタンから 1 回ずつ)。
 * - 単一起動ロック: running が既にあれば 409
 * - 古い running (60 分以上 heartbeat なし) は expired に
 * - 構成 4: 前回 run が中断して残った syncing 行は **ne_unknown** に落とす
 *   (= 自動 retry せず、中原さんが翌日確認)
 */
export function startNeSyncRun({ started_by } = {}) {
  const db = ensureSchema();
  const now = utcIsoNow();
  const tx = db.transaction(() => {
    // 古い running を expired に finalize (heartbeat_at 考慮)
    const cutoff = new Date(Date.now() - NE_SYNC_RUN_EXPIRY_MIN * 60 * 1000).toISOString();
    db.prepare(`UPDATE pd_ne_sync_run SET status='expired', ended_at=?,
        last_error='run expired (no heartbeat within ' || ? || ' min)'
        WHERE status='running' AND COALESCE(heartbeat_at, started_at) < ?`).run(now, NE_SYNC_RUN_EXPIRY_MIN, cutoff);
    // expired 化された run に紐づく syncing 行は ne_unknown に (= 自動 retry しない)
    db.prepare(`UPDATE pd_shipment_tracking
        SET sync_status='ne_unknown',
            last_error=COALESCE(last_error, 'syncing left after run expired'),
            last_error_code=COALESCE(last_error_code, 'NE_UNKNOWN_RUN_EXPIRED'),
            claim_id=NULL, syncing_started_at=NULL,
            updated_at=?, updated_by='ne_sync_system'
        WHERE sync_status='syncing'
          AND claim_id IN (SELECT run_id FROM pd_ne_sync_run WHERE status='expired')`).run(now);
    // 単一起動ロック
    const existing = db.prepare(`SELECT run_id, started_at, started_by FROM pd_ne_sync_run WHERE status='running' LIMIT 1`).get();
    if (existing) {
      const err = vErr(`既に実行中の run があります (run_id=${existing.run_id}, by=${existing.started_by || '?'}, started=${existing.started_at})`);
      err.code = 'CONFLICT';
      throw err;
    }
    // 念のため: claim_id が NULL の syncing (= 過去の異常状態) も ne_unknown に
    db.prepare(`UPDATE pd_shipment_tracking
        SET sync_status='ne_unknown',
            last_error=COALESCE(last_error, 'orphan syncing without claim_id'),
            last_error_code=COALESCE(last_error_code, 'NE_UNKNOWN_ORPHAN'),
            updated_at=?, updated_by='ne_sync_system'
        WHERE sync_status='syncing' AND claim_id IS NULL`).run(now);
    const run_id = crypto.randomUUID();
    db.prepare(`INSERT INTO pd_ne_sync_run (run_id, status, started_at, started_by) VALUES (?, 'running', ?, ?)`)
      .run(run_id, now, started_by || null);
    return run_id;
  });
  const run_id = tx();
  audit('ne_sync_run_start', run_id, { started_by }, started_by);
  return { run_id, started_at: now };
}

/**
 * Run の進行状況を取得 (UI のステータス表示用)。
 */
export function getNeSyncRun(run_id) {
  const db = ensureSchema();
  if (!run_id) throw vErr('run_id is required');
  const row = db.prepare(`SELECT * FROM pd_ne_sync_run WHERE run_id=?`).get(run_id);
  return row || null;
}

/**
 * 直近の run 一覧 (UI 履歴表示用、最大 50 件)。
 */
export function listRecentNeSyncRuns(limit = 50) {
  const db = ensureSchema();
  const lim = Math.max(1, Math.min(200, parseInt(limit, 10) || 50));
  return db.prepare(`SELECT * FROM pd_ne_sync_run ORDER BY started_at DESC LIMIT ?`).all(lim);
}

/**
 * ミニPC worker が呼ぶ: ready 行を atomic claim → syncing に遷移。
 * 構成 4: claim_id = run_id のみで管理。claim_token は廃止 (Bearer 認証で worker を保護)。
 * error 行も attempt 制限なしで再 claim 対象 (構成 4: manual_review/ne_unknown へ早く落とす)。
 */
export function claimReadyForNeSync({ run_id } = {}) {
  if (!run_id) throw vErr('run_id is required');
  const db = ensureSchema();
  const run = db.prepare(`SELECT status FROM pd_ne_sync_run WHERE run_id=?`).get(run_id);
  if (!run) throw vErr(`run not found: ${run_id}`);
  if (run.status !== 'running') throw vErr(`run is not running (status=${run.status})`);
  // 構成 4 不変条件 (Codex R3 High): claim は **常に 1 件**。
  // batch claim を許すと「途中失敗時に未処理行も ne_unknown になる」問題が再発する。
  // worker 側 CLI でも 1 固定だが、サーバ側でも強制してリクエスト偽造を防ぐ。
  const lim = 1;
  const now = utcIsoNow();
  const tx = db.transaction(() => {
    // Codex R4 High 対応: 同一 run_id で既に outstanding な syncing 行があれば新規 claim しない。
    // これがないと worker が results 前に /queue を連発して claim を貯められ、worker 落ち→
    // complete で複数行が ne_unknown になる (R3 High が経路を変えて再発)。
    // worker は 1 件 claim → 1 件 NE 反映 → 1 件 results POST → 次の 1 件 claim、の順を強制される。
    const outstanding = db.prepare(`SELECT COUNT(*) AS c FROM pd_shipment_tracking
      WHERE sync_status='syncing' AND claim_id=?`).get(run_id);
    if (outstanding.c > 0) {
      throw vErr(`outstanding syncing がある間は新規 claim できません (count=${outstanding.c}, run_id=${run_id}). 直前の results を POST してから再 claim してください。`);
    }
    // 候補: ready 行 と error 行 (構成 4: error の自動 retry は持つが、回数制限なし。
    // NE 業務エラーは worker 側で manual_review に落とす)
    // aes (is_locked=1) は除外。
    const candidates = db.prepare(`
      SELECT t.ne_uketsuke_no, t.tracking_no, t.tracking_source, t.shipping_method_code,
             t.method_at_export, t.shop_name, t.order_no, t.product_summary, t.sync_status,
             sm.ne_carrier_id, sm.is_locked
        FROM pd_shipment_tracking t
        JOIN pd_shipping_method sm ON sm.code = t.shipping_method_code
       WHERE t.sync_status IN ('ready', 'error')
         AND COALESCE(sm.is_locked, 0) = 0
       ORDER BY t.sync_requested_at ASC, t.ne_uketsuke_no ASC
       LIMIT ?
    `).all(lim);
    if (!candidates.length) return [];
    // syncing に遷移。WHERE で元 status を照合して race を防ぐ。
    const upd = db.prepare(`UPDATE pd_shipment_tracking
      SET sync_status='syncing', claim_id=?, syncing_started_at=?,
          updated_at=?, updated_by='ne_sync_worker'
      WHERE ne_uketsuke_no=? AND sync_status=?`);
    const claimed = [];
    for (const r of candidates) {
      const info = upd.run(run_id, now, now, r.ne_uketsuke_no, r.sync_status);
      if (info.changes > 0) {
        claimed.push({
          ne_uketsuke_no: r.ne_uketsuke_no,
          tracking_no: r.tracking_no,
          tracking_source: r.tracking_source,
          shipping_method_code: r.shipping_method_code,
          method_at_export: r.method_at_export,
          shop_name: r.shop_name,
          order_no: r.order_no,
          product_summary: r.product_summary,
          ne_carrier_id: r.ne_carrier_id,
        });
      }
    }
    db.prepare(`UPDATE pd_ne_sync_run SET target_count = target_count + ?, heartbeat_at=? WHERE run_id=?`)
      .run(claimed.length, now, run_id);
    return claimed;
  });
  const claimed = tx();
  audit('ne_sync_claim', run_id, { count: claimed.length }, 'ne_sync_worker');
  return { run_id, claimed_count: claimed.length, items: claimed };
}

/**
 * ミニPC worker が呼ぶ: バッチ結果を反映。
 * 構成 4: claim_id (= run_id) のみで照合、token なし。
 * 受理する status: 'synced' | 'manual_review' | 'ne_unknown'
 * (構成 4 では 'error' を worker 側で出さない: 業務エラーは manual_review、
 *  通信/クラッシュは Render 側で ne_unknown 化される)
 *
 * results: [{ ne_uketsuke_no, status, error_code?, error_message?, ne_synced_at? }]
 */
export function applyNeSyncResults({ run_id, results } = {}) {
  if (!run_id) throw vErr('run_id is required');
  if (!Array.isArray(results)) throw vErr('results must be an array');
  const db = ensureSchema();
  const run = db.prepare(`SELECT status FROM pd_ne_sync_run WHERE run_id=?`).get(run_id);
  if (!run) throw vErr(`run not found: ${run_id}`);
  if (run.status !== 'running') throw vErr(`run is not running (status=${run.status})`);
  const now = utcIsoNow();
  let synced = 0, manualReview = 0, neUnknown = 0, mismatched = 0;
  const ACCEPTED_STATUSES = new Set(['synced', 'manual_review', 'ne_unknown']);
  const tx = db.transaction(() => {
    for (const r of results) {
      if (!r || !r.ne_uketsuke_no || !ACCEPTED_STATUSES.has(r.status)) { mismatched++; continue; }
      const cur = db.prepare(`SELECT sync_status, claim_id FROM pd_shipment_tracking WHERE ne_uketsuke_no=?`).get(r.ne_uketsuke_no);
      // 構成 4: syncing + claim_id 一致だけで照合 (Bearer で worker を保護)
      if (!cur || cur.sync_status !== 'syncing' || cur.claim_id !== run_id) {
        mismatched++;
        continue;
      }
      if (r.status === 'synced') {
        db.prepare(`UPDATE pd_shipment_tracking
          SET sync_status='synced', ne_synced_at=?, claim_id=NULL, syncing_started_at=NULL,
              last_error=NULL, last_error_code=NULL,
              updated_at=?, updated_by='ne_sync_worker'
          WHERE ne_uketsuke_no=? AND claim_id=?`)
          .run(r.ne_synced_at || now, now, r.ne_uketsuke_no, run_id);
        synced++;
      } else if (r.status === 'manual_review') {
        db.prepare(`UPDATE pd_shipment_tracking
          SET sync_status='manual_review', last_error=?, last_error_code=?,
              claim_id=NULL, syncing_started_at=NULL,
              updated_at=?, updated_by='ne_sync_worker'
          WHERE ne_uketsuke_no=? AND claim_id=?`)
          .run(r.error_message || null, r.error_code || null, now, r.ne_uketsuke_no, run_id);
        manualReview++;
      } else {
        // ne_unknown: NE 反映に成功したが結果不明 (DB / 通信 / クラッシュ)
        // 自動 retry しない、中原さんが翌日確認
        db.prepare(`UPDATE pd_shipment_tracking
          SET sync_status='ne_unknown', last_error=?, last_error_code=?,
              claim_id=NULL, syncing_started_at=NULL,
              updated_at=?, updated_by='ne_sync_worker'
          WHERE ne_uketsuke_no=? AND claim_id=?`)
          .run(r.error_message || null, r.error_code || 'NE_UNKNOWN', now, r.ne_uketsuke_no, run_id);
        neUnknown++;
      }
    }
    db.prepare(`UPDATE pd_ne_sync_run SET
        synced_count = synced_count + ?,
        manual_review_count = manual_review_count + ?,
        error_count = error_count + ?,
        heartbeat_at = ?
        WHERE run_id=?`)
      .run(synced, manualReview, neUnknown, now, run_id);
  });
  tx();
  audit('ne_sync_results', run_id, { synced, manualReview, neUnknown, mismatched }, 'ne_sync_worker');
  return { run_id, synced, manual_review: manualReview, ne_unknown: neUnknown, mismatched };
}

/**
 * Run を完了。残った syncing 行 (worker が結果を返さなかった行) は **ne_unknown** に。
 * 構成 4: ready に戻さない、自動 retry しない、中原さんが翌日確認する運用。
 * status: 'done' | 'failed' | 'aborted'
 */
export function completeNeSyncRun({ run_id, status, last_error } = {}) {
  if (!run_id) throw vErr('run_id is required');
  const ok = ['done', 'failed', 'aborted'];
  if (!ok.includes(status)) throw vErr(`invalid status: ${status}`);
  const db = ensureSchema();
  const now = utcIsoNow();
  const tx = db.transaction(() => {
    const cur = db.prepare(`SELECT status FROM pd_ne_sync_run WHERE run_id=?`).get(run_id);
    if (!cur) throw vErr(`run not found: ${run_id}`);
    if (cur.status !== 'running') return; // idempotent: 既に finalize 済み
    // 構成 4: 残った syncing は ne_unknown に (= 自動 retry しない、中原さんが翌日確認)
    // NE 反映済みの可能性があるため ready に戻さない (二重反映防止)
    db.prepare(`UPDATE pd_shipment_tracking SET sync_status='ne_unknown',
        last_error=COALESCE(last_error, 'worker did not return result before run complete'),
        last_error_code=COALESCE(last_error_code, 'NE_UNKNOWN_NO_RESULT'),
        claim_id=NULL, syncing_started_at=NULL,
        updated_at=?, updated_by='ne_sync_worker'
        WHERE sync_status='syncing' AND claim_id=?`).run(now, run_id);
    db.prepare(`UPDATE pd_ne_sync_run SET status=?, ended_at=?, last_error=? WHERE run_id=?`)
      .run(status, now, last_error || null, run_id);
  });
  tx();
  audit('ne_sync_run_complete', run_id, { status, last_error }, 'ne_sync_worker');
  return getNeSyncRun(run_id);
}
