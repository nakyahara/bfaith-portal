/**
 * カバー率レポート — 「いまのマスタで、実際の出荷の何割が確定できるか」。
 *
 * 埋める作業のためのものさし。埋めた分だけ数字が上がるので、
 *   ・次に何を測ればいいか (頻度順の不足リスト)
 *   ・どこでやめていいか (もう伸びない)
 * が判断できる。
 *
 * 出荷実績の出どころは 2 つ:
 *   - warehouse         miniPC の warehouse.db (NE 受注)。出荷確定日つきで最も正確。miniPC でしか読めない
 *   - packing-dispatch  warehouse-mirror.db の pd_shipment_tracking (NE → ロジザードの CSV 出力履歴)。
 *                       Render で読める。2026-06-04 以降の定形外が全部ある (composition.js)
 * 既定は warehouse があればそれ、無ければ packing-dispatch。両方無ければ available:false。
 */
import Database from 'better-sqlite3';
import path from 'path';
import fs from 'fs';
import { judge, UNKNOWN_REASONS } from './engine.js';
import { getDB, getTariffVersionFor, getBands, getMaterialsMap, getOverheadTotalG, getSetting, jstToday } from './db.js';
import {
  mirrorAvailable, mirrorPath, loadMirrorShipments, lookupProductNameFromMirror, normalizeSku,
} from './composition.js';

const DATA_DIR = process.env.DATA_DIR || path.join(process.cwd(), 'data');
const WAREHOUSE_DB = process.env.POSTAGE_WAREHOUSE_DB || path.join(DATA_DIR, 'warehouse.db');

// NE の配送方法名。定形も「定形外郵便」で出るので、この 1 つで拾える (パターン名を真実として扱わない)
const NE_DELIVERY_METHOD = '定形外郵便';

export const SOURCE_LABELS = {
  warehouse: 'NE受注 (miniPC の warehouse.db)',
  'packing-dispatch': 'packing-dispatch の出力履歴 (Render)',
};

// loadShipments / lookupProductName が使う列。あるだけのファイル (空・壊れている・別物) は「読めない」扱い
const WAREHOUSE_PROBE = [
  'SELECT 伝票番号, 出荷確定日, 配送方法名 FROM raw_ne_order_base LIMIT 0',
  'SELECT 伝票番号, 商品コード, 商品名, 受注数, キャンセル区分 FROM raw_ne_orders LIMIT 0',
];
export function warehouseAvailable() {
  if (!fs.existsSync(WAREHOUSE_DB)) return false;
  try {
    const wh = new Database(WAREHOUSE_DB, { readonly: true, fileMustExist: true });
    try { for (const sql of WAREHOUSE_PROBE) wh.prepare(sql).all(); return true; } finally { wh.close(); }
  } catch {
    return false;   // 読めなければ packing-dispatch 側に切り替わる (resolveSource)
  }
}

/** いま読める出どころ (優先順)。 */
export function availableSources() {
  const out = [];
  if (warehouseAvailable()) out.push('warehouse');
  if (mirrorAvailable()) out.push('packing-dispatch');
  return out;
}

/** 定形外の伝票と明細を期間で取り出す (warehouse.db)。読み取り専用で開く (本番DBに絶対書かない)。 */
export function loadShipments({ since, until } = {}) {
  if (!warehouseAvailable()) return null;
  const wh = new Database(WAREHOUSE_DB, { readonly: true, fileMustExist: true });
  try {
    const params = [NE_DELIVERY_METHOD];
    let where = 'b.配送方法名 = ?';
    if (since) { where += ' AND b.出荷確定日 >= ?'; params.push(since); }
    if (until) { where += ' AND b.出荷確定日 < ?'; params.push(nextDay(until)); }
    const rows = wh.prepare(`
      SELECT b.伝票番号 AS slip_no,
             substr(b.出荷確定日, 1, 10) AS ship_date,
             o.商品コード AS sku_code,
             o.受注数 AS qty,
             o.商品名 AS product_name
        FROM raw_ne_order_base b
        JOIN raw_ne_orders o ON o.伝票番号 = b.伝票番号
       WHERE ${where}
         AND o.キャンセル区分 = '有効'
         AND b.出荷確定日 IS NOT NULL
    `).all(...params);

    const map = new Map();
    for (const r of rows) {
      if (!map.has(r.slip_no)) map.set(r.slip_no, { slip_no: r.slip_no, ship_date: r.ship_date, lines: [] });
      map.get(r.slip_no).lines.push({
        sku_code: normalizeSku(r.sku_code),
        qty: Number(r.qty),
        product_name: r.product_name,
      });
    }
    return [...map.values()];
  } finally {
    wh.close();
  }
}

/**
 * 商品名を引く (手で登録するとき、名前を打ち直さなくて済むように)。
 * NE の受注明細 → 無ければ packing-dispatch の出力履歴。出荷実績が無い新商品は見つからない。
 */
export function lookupProductName(skuCode) {
  const code = String(skuCode || '').trim();
  if (!code) return null;
  if (warehouseAvailable()) {
    const wh = new Database(WAREHOUSE_DB, { readonly: true, fileMustExist: true });
    try {
      const r = wh.prepare(`
        SELECT 商品名 AS name FROM raw_ne_orders
         WHERE 商品コード = ? AND 商品名 IS NOT NULL AND 商品名 <> ''
         ORDER BY rowid DESC LIMIT 1
      `).get(code);
      if (r?.name) return r.name;
    } catch {
      /* 期待した表が無い環境では黙って次へ (補完は「あれば便利」なだけ) */
    } finally {
      wh.close();
    }
  }
  return lookupProductNameFromMirror(code);
}

/** 判定に必要なマスタをまとめて読む。 */
export function buildContext(forDate) {
  const db = getDB();
  const skus = new Map();
  for (const r of db.prepare('SELECT * FROM pm_skus').all()) skus.set(r.sku_code, r);
  const tariff = getTariffVersionFor(forDate || jstToday());
  return {
    skus,
    materials: getMaterialsMap(),
    bands: tariff ? getBands(tariff.tariff_version_id) : [],
    tariff,
    overheadG: getOverheadTotalG(),
    boundaryMarginG: getSetting('boundary_margin_g', 5),
    thicknessMarginMm: getSetting('thickness_margin_mm', 1),
  };
}

function resolveSource(source) {
  if (source === 'warehouse') return warehouseAvailable() ? 'warehouse' : null;
  if (source === 'packing-dispatch') return mirrorAvailable() ? 'packing-dispatch' : null;
  return availableSources()[0] || null;
}

/**
 * 期間中の定形外を全部判定して集計する。
 * 料金表は **出荷日** に有効なものを使う (改定をまたいでも過去分が壊れない)。
 * @param {{since?: string, until?: string, source?: 'auto'|'warehouse'|'packing-dispatch'}} opts
 */
export function coverageReport({ since, until, source = 'auto' } = {}) {
  const src = resolveSource(source);
  if (!src) {
    return { available: false, source: null, warehousePath: WAREHOUSE_DB, mirrorPath: mirrorPath() };
  }
  const shipments = src === 'warehouse' ? loadShipments({ since, until }) : loadMirrorShipments({ since, until });
  const ctxCache = new Map();
  const ctxFor = (date) => {
    if (!ctxCache.has(date)) ctxCache.set(date, buildContext(date));
    return ctxCache.get(date);
  };

  const byBand = new Map();      // band_code → {display_name, mail_type, count, amount}
  const byReason = new Map();    // reason → count
  const missingSku = new Map();  // sku_code → {count, name, needs:Set}
  const byDate = new Map();      // ship_date → {total, confirmed}
  const blockedByMaterial = new Map(); // `${material_code}:${need}` → 資材側の未登録で止まっている通数
  let confirmed = 0, unknown = 0, totalAmount = 0;

  for (const s of shipments) {
    const ctx = ctxFor(s.ship_date);
    // packing-dispatch の記録が壊れている伝票は、読めた明細だけで判定しない (1 商品欠けたまま安い区分になる)
    const r = s.broken
      ? { status: 'unknown', reason: 'broken_composition' }
      : judge(s, ctx);
    const d = byDate.get(s.ship_date) || { date: s.ship_date, total: 0, confirmed: 0 };
    d.total++;
    if (r.status === 'confirmed') {
      confirmed++; d.confirmed++; totalAmount += r.amountYen;
      const b = byBand.get(r.bandCode) || { band_code: r.bandCode, display_name: r.displayName, mail_type: r.mailType, count: 0, amount: 0 };
      b.count++; b.amount += r.amountYen;
      byBand.set(r.bandCode, b);
    } else {
      unknown++;
      byReason.set(r.reason, (byReason.get(r.reason) || 0) + 1);
      // 「この資材を1つ直せば何通が動くか」— 直す優先順位がそのまま出る
      const need = r.reason === 'missing_dims' ? '外寸' : r.reason === 'missing_material_thickness' ? '厚み' : null;
      if (need && r.materialCode) {
        const k = `${r.materialCode}:${need}`;
        blockedByMaterial.set(k, (blockedByMaterial.get(k) || 0) + 1);
      }
      // 「何を測れば直るか」を SKU 単位で積む
      if (['missing_sku', 'missing_weight', 'missing_thickness'].includes(r.reason)) {
        const need = r.reason === 'missing_sku' ? '登録' : r.reason === 'missing_weight' ? '重さ' : '厚み';
        for (const code of String(r.detail || '').split(',').map((x) => x.trim()).filter(Boolean)) {
          const e = missingSku.get(code) || { sku_code: code, count: 0, needs: new Set(), name: null };
          e.count++; e.needs.add(need);
          if (!e.name) e.name = s.lines.find((l) => l.sku_code === code)?.product_name || null;
          missingSku.set(code, e);
        }
      }
    }
    byDate.set(s.ship_date, d);
  }

  const total = shipments.length;
  const materials = getMaterialsMap();
  return {
    available: true,
    source: src,
    sourceLabel: SOURCE_LABELS[src],
    period: { since: since || null, until: until || null },
    total,
    confirmed,
    unknown,
    confirmedRate: total ? confirmed / total : 0,
    totalAmount,
    bands: [...byBand.values()].sort((a, b) =>
      rank(a.mail_type) - rank(b.mail_type) || a.band_code.localeCompare(b.band_code)),
    reasons: [...byReason.entries()]
      .map(([reason, count]) => ({ reason, label: UNKNOWN_REASONS[reason] || reason, count }))
      .sort((a, b) => b.count - a.count),
    blockedMaterials: [...blockedByMaterial.entries()]
      .map(([key, count]) => {
        const [code, need] = key.split(':');
        const m = materials.get(code);
        return { material_code: code, display_name: m?.display_name || code, need, count };
      })
      .sort((a, b) => b.count - a.count),
    missingSkus: [...missingSku.values()]
      .map((e) => ({ ...e, needs: [...e.needs].join('＋') }))
      .sort((a, b) => b.count - a.count),
    byDate: [...byDate.values()].sort((a, b) => b.date.localeCompare(a.date)),
  };
}

/**
 * 「あと何個埋めれば何%になるか」。不足 SKU を頻度順に埋めた場合の伸びを試算する。
 * 実測では上位30で93.6%、50で96.2%だった (2026-06〜08)。
 */
export function fillCurve(report, steps = [10, 20, 30, 50, 100]) {
  if (!report.available) return [];
  const order = report.missingSkus.map((m) => m.sku_code);
  return steps.filter((k) => k <= order.length).map((k) => ({
    k,
    // 上位 k 個を埋めたときに解消する「不足由来の不明」件数の上限
    resolvableSlips: report.missingSkus.slice(0, k).reduce((a, m) => a + m.count, 0),
  }));
}

function rank(t) { return t === 'teikei' ? 1 : t === 'kikakunai' ? 2 : 3; }
function nextDay(ymd) {
  const d = new Date(`${ymd}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + 1);
  return d.toISOString().slice(0, 10);
}
