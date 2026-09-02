/**
 * 商品マスタの検索と、1商品ぶんの判定プレビュー。
 *
 * 重量表の一括取込 (import.js) とは別に、**商品ごとに手で登録する**ための土台。
 * 新商品が入ってきたとき、出荷が始まる前にここで重さ・厚み・資材を入れておける。
 *
 * 登録したその場で「この商品を1個だけ送ったらいくらか」を返す。
 * 入れた値が効いているかをその場で確かめられないと、埋める作業が続かない。
 */
import { getDB } from './db.js';
import { judge } from './engine.js';

/** 商品1件の状態。どこが埋まっていないかを一言で表す。 */
export function skuStatus(row) {
  if (!row.default_material_code) return 'no_material';
  if (row.unit_weight_g === null || row.unit_weight_g === undefined) return 'no_weight';
  if (row.thickness_mm === null || row.thickness_mm === undefined) return 'no_thickness';
  return 'ready';
}

export const STATUS_LABELS = {
  ready: '揃っている',
  no_material: '資材が未定',
  no_weight: '重さが未登録',
  no_thickness: '厚みが未登録',
};

/**
 * 状態の SQL 条件。**skuStatus() と同じ優先順位で、重ならないように** 書く。
 * ここが件数と一覧でズレると、タブに「3件」と出ているのに一覧に5件出る、が起きる。
 * 件数もこの条件から作るので、定義はこの1か所だけ。
 */
const STATUS_SQL = {
  no_material: `default_material_code IS NULL`,
  no_weight: `default_material_code IS NOT NULL AND unit_weight_g IS NULL`,
  no_thickness: `default_material_code IS NOT NULL AND unit_weight_g IS NOT NULL AND thickness_mm IS NULL`,
  ready: `default_material_code IS NOT NULL AND unit_weight_g IS NOT NULL AND thickness_mm IS NOT NULL`,
};

const FILTERS = {
  all: '',
  incomplete: `AND NOT (${STATUS_SQL.ready})`,
  ready: `AND (${STATUS_SQL.ready})`,
  no_material: `AND (${STATUS_SQL.no_material})`,
  no_weight: `AND (${STATUS_SQL.no_weight})`,
  no_thickness: `AND (${STATUS_SQL.no_thickness})`,
};
export const FILTER_LABELS = {
  all: 'すべて',
  incomplete: '未完了',
  ready: '揃っている',
  no_material: '資材が未定',
  no_weight: '重さが未登録',
  no_thickness: '厚みが未登録',
};

/**
 * 商品コード・商品名の部分一致で探す。
 * @returns {{rows, total, limit, offset}}
 */
export function searchSkus({ q = '', filter = 'all', limit = 50, offset = 0 } = {}) {
  const db = getDB();
  const where = FILTERS[filter] ?? '';
  const kw = String(q || '').normalize('NFKC').trim();
  // LIKE のワイルドカードを打ち消す (商品コードに _ を含むものが多く、素通しだと全件に当たる)
  const esc = kw.replace(/[\\%_]/g, (c) => `\\${c}`);
  const params = kw ? [`%${esc}%`, `%${esc}%`] : [];
  const kwWhere = kw ? `AND (sku_code LIKE ? ESCAPE '\\' OR display_name LIKE ? ESCAPE '\\')` : '';

  const total = db.prepare(`SELECT COUNT(*) n FROM pm_skus WHERE 1=1 ${where} ${kwWhere}`).get(...params).n;
  // 0 や負数が来たら既定に戻す (1件だけ返して「該当なし」に見える、を避ける)
  const nLim = Number(limit), nOff = Number(offset);
  const lim = Math.min(Number.isFinite(nLim) && nLim > 0 ? Math.floor(nLim) : 50, 200);
  const off = Number.isFinite(nOff) && nOff > 0 ? Math.floor(nOff) : 0;
  const rows = db.prepare(`
    SELECT * FROM pm_skus WHERE 1=1 ${where} ${kwWhere}
     ORDER BY
       CASE WHEN default_material_code IS NULL THEN 0
            WHEN unit_weight_g IS NULL THEN 1
            WHEN thickness_mm IS NULL THEN 2 ELSE 3 END,
       sku_code
     LIMIT ? OFFSET ?
  `).all(...params, lim, off);
  return { rows, total, limit: lim, offset: off };
}

/**
 * タブに出す件数。**FILTERS と同じ条件から作る** ので、件数と一覧が必ず一致する。
 */
export function countByStatus() {
  const r = getDB().prepare(`
    SELECT COUNT(*) total,
           SUM(CASE WHEN ${STATUS_SQL.no_material}  THEN 1 ELSE 0 END) no_material,
           SUM(CASE WHEN ${STATUS_SQL.no_weight}    THEN 1 ELSE 0 END) no_weight,
           SUM(CASE WHEN ${STATUS_SQL.no_thickness} THEN 1 ELSE 0 END) no_thickness,
           SUM(CASE WHEN ${STATUS_SQL.ready}        THEN 1 ELSE 0 END) ready
      FROM pm_skus`).get();
  const c = {
    total: r.total || 0,
    no_material: r.no_material || 0,
    no_weight: r.no_weight || 0,
    no_thickness: r.no_thickness || 0,
    ready: r.ready || 0,
  };
  c.incomplete = c.total - c.ready;
  return c;
}

/**
 * 「この商品を1個だけ送ったらどうなるか」。
 * 登録した値が効いているかをその場で見せるためのもので、実際の伝票判定とは別。
 */
export function previewOne(skuCode, ctx) {
  const r = judge({ lines: [{ sku_code: skuCode, qty: 1 }] }, ctx);
  if (r.status === 'confirmed') {
    return { ok: true, text: `${r.displayName}　${r.amountYen}円`, sub: `${r.weightG}g / 厚さ${r.thicknessMm}mm / ${r.materialName}` };
  }
  return { ok: false, text: `不明 — ${r.reasonLabel}`, sub: r.detail || null };
}
