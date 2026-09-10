/**
 * sources.mjs — Render 側の SQLite (既存の正本と写し) を読んで「ロード計画 (plan)」を作る。Company DB構想 06 §5.6 PR-B
 *
 * 読むだけ (readonly で開く)。書くのは engine.mjs (Postgres)。
 * 出どころ (06 §4.3「既存表の行き先」):
 *   warehouse-mirror.db : mirror_products / mirror_set_components / mirror_sku_master + mirror_sku_resolved / mirror_rakuten_sku_map /
 *                         mirror_qoo10_items / mirror_amazon_sku_fees / product_drafts + draft_page_info + draft_sku_jans /
 *                         f_inbound_check_barcode_master / f_inbound_info / po_suppliers + po_vendor_code_map / supplier_share_master
 *   fba.db              : sku_mapping (Sheet 由来。ASIN・JAN・FNSKU) / fba_sku_attrs (mirror モードの正。ASIN・FNSKU) — 別の出どころとして両方残す
 *   rakuten-yahoo-sync.db: notion_overrides (Yahoo JAN) / yahoo_registered_items
 *   postage.db          : pm_skus (定形外の実測重量)
 *   fba-box.db          : fbx_weight_refs (SP-API 梱包重量) / fbx_weight_current (実測)
 *   staff.db            : staff
 *
 * 🚨 コードの照合は lib/sku-norm.js normSku() (= Postgres 側 core.norm_code)。SQLite の lower() は ASCII 限定なので使わない
 * 🚨 属性は「観測」として出どころごとに残す (どれを採るかは engine が規則 v1 で決める)。ここで選ばない
 * 🚨 JAN・重量は「単品 1 個」(構成 1 行・qty=1) のときだけ商品の属性。複数個パック・セットの出品に付いた値は listing の属性 (scope 'listing')
 * 🚨 fba.db は sql.js がファイルごと書き戻すので、読むのはアプリが書いていない時 (夜間 or 手動時)
 * 🚨 観測時刻は出どころの更新時刻 (updated_at / fetched_at) を使う。無い表だけ「今」
 */
import fs from 'node:fs';
import path from 'node:path';
import Database from 'better-sqlite3';
import { normSku } from '../../../lib/sku-norm.js';
import { pickByPriority, FNSKU_SOURCE_PRIORITY } from './engine.mjs';

const MARKETPLACE_JP = 'A1VC38T7YXB528';
/**
 * 自社の店舗キー (core.listings.shop_code)。モール側の ID ではなく「うちの何店目か」の安定した識別 (Codex PR-B R1 M12)。
 * 2 店舗目ができたら値を足す。Amazon は marketplace を含める (同じ seller_sku が US にもあり得る)
 */
export const SHOP_CODES = { amazon: `main@${MARKETPLACE_JP}`, rakuten: 'main', yahoo: 'main', qoo10: 'main' };

function openRo(file) {
  if (!fs.existsSync(file)) return null;
  return new Database(file, { readonly: true, fileMustExist: true });
}
function hasTable(db, name) {
  return !!db.prepare("select 1 from sqlite_master where type = 'table' and name = ?").get(name);
}
function hasColumn(db, table, col) {
  return db.prepare(`pragma table_info(${table})`).all().some((c) => c.name === col);
}
function rows(db, sql, params = []) {
  return db.prepare(sql).all(...params);
}
const s = (v) => (v == null ? null : String(v).trim() || null);
const n = (v) => (v == null || v === '' || Number.isNaN(Number(v)) ? null : Number(v));
/** SQLite の時刻文字列 (ISO / 'YYYY-MM-DD HH:MM:SS' = localtime JST) → ISO。読めなければ null */
export function toIso(v) {
  const t = s(v); if (!t) return null;
  const d = new Date(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(t) ? `${t.replace(' ', 'T')}+09:00` : t);
  return Number.isFinite(d.getTime()) ? d.toISOString() : null;
}

export function mapSkuKind(kubun) {
  if (kubun === '単品') return 'single';
  if (kubun === 'セット') return 'set';
  if (kubun === '例外') return 'exception';
  return null;
}
export function mapHandling(v) {
  const t = s(v);
  if (!t) return 'unknown';
  if (t === '取扱中') return 'active';
  return 'discontinued';            // NE の「廃番」など取扱中以外は全部 discontinued (語彙は NE 側。unknown は空欄だけ)
}
export function mapTaxRate(v) {
  const x = n(v);
  if (x === 0.08 || x === 8) return 0.08;
  if (x === 0.1 || x === 0.10 || x === 10) return 0.10;
  return null;
}
export function mapTaxClass(v, rate) {
  const t = s(v);
  if (t && ['STANDARD_10', 'REDUCED_8', 'MIXED', 'UNKNOWN'].includes(t)) return t;
  if (rate === 0.08) return 'REDUCED_8';
  if (rate === 0.10) return 'STANDARD_10';
  return null;
}
export function mapCost(row) {
  const status = s(row['原価状態']);
  if (!['COMPLETE', 'OVERRIDDEN'].includes(status)) return null;
  const jpy = n(row['原価']);
  if (jpy == null) return null;
  const src = s(row['原価ソース']);
  const source = src === 'NE' ? 'ne' : src === 'セット計算' ? 'set_calc' : src === '例外' ? 'manual' : 'imported';
  return { jpy, source, status };
}
/** '100ml' / '50g' / '1.5L' / '12個入' / '500ml×2' → { num, unit } (取れなければ null)。日本語単位に \b は効かないので「次が英字でない」で切る */
export function parseContent(text) {
  const t = s(text);
  if (!t) return null;
  const m = /^\s*([0-9]+(?:\.[0-9]+)?)\s*(ml|mL|ML|l|L|g|G|kg|KG|mg|cc|個|本|枚|包|粒|錠)(?![A-Za-z])/i.exec(t.replace(/,/g, ''));
  if (!m) return null;
  return { num: Number(m[1]), unit: m[2].toLowerCase() === 'cc' ? 'ml' : m[2] };
}
export function mapWorkerType(kind) {
  return ({ employee: 'employee', part_time: 'part_time', contractor: 'contractor', iroha: 'iroha_staff', other: 'employee' })[kind] || 'employee';
}
export function isJan(v) { return /^\d{8}$|^\d{13}$/.test(String(v || '').trim()); }
/**
 * バリエーションのまとまりの名前を、子の商品名から決める (D-24 = A)。
 *   1. 「【」より前を取り、2 件以上が同じならそれ (例「ジャージ補修シート 【ブラック(黒)】_白ビ袋」→「ジャージ補修シート」)
 *   2. だめなら最長共通接頭辞から末尾の記号・数字を削る (例「メルカリ訳アリ品01」…→「メルカリ訳アリ品」)
 *   3. それも 2 文字未満なら代表コードそのまま
 */
export function variationGroupName(childNames, repCode) {
  const names = (childNames || []).map((x) => String(x ?? '').trim()).filter(Boolean);
  if (!names.length) return repCode;
  const heads = names.map((x) => { const i = x.indexOf('【'); return i > 0 ? x.slice(0, i).trim() : ''; }).filter((x) => x.length >= 2);
  if (heads.length) {
    const c = new Map();
    for (const h of heads) c.set(h, (c.get(h) || 0) + 1);
    const best = [...c.entries()].sort((a, b) => b[1] - a[1] || heads.indexOf(a[0]) - heads.indexOf(b[0]))[0];
    if (best && (best[1] >= 2 || names.length === 1)) return best[0];
  }
  let lcp = names[0];
  for (const x of names.slice(1)) { let i = 0; while (i < lcp.length && i < x.length && lcp[i] === x[i]) i++; lcp = lcp.slice(0, i); if (!lcp) break; }
  const trimmed = lcp.replace(/[\s\-_/,、・0-9０-９()（）【】[\]]+$/u, '').trim();
  return trimmed.length >= 2 ? trimmed : repCode;
}
/**
 * 出品の構成が「単品 1 個」(構成 1 行・qty=1・その SKU が単品) ならその NE コード。複数個パック・セット (セット SKU × 1 も)・未解決は null。
 * isSingle(code) を渡すと SKU 種別も見る (渡さないと構成の形だけ)
 */
export function singleUnitCode(components, isSingle) {
  if (!(components && components.length === 1 && Number(components[0].qty) === 1)) return null;
  const code = components[0].code;
  return isSingle && !isSingle(code) ? null : code;
}

/**
 * plan を作る。dataDir = Render の DATA_DIR。無いファイル・無い表は飛ばして report.sources に書く
 */
export function buildPlanFromRender({ dataDir, now = new Date(), log = () => {} } = {}) {
  const nowIso = now.toISOString();
  const plan = { skus: [], variationGroups: [], setComponents: [], listings: [], observations: [], physicals: [], compliance: [], suppliers: [], supplierSkus: [], workers: [], sources: {} };
  const src = plan.sources;
  const mirror = openRo(path.join(dataDir, 'warehouse-mirror.db'));
  if (!mirror) throw Object.assign(new Error(`warehouse-mirror.db が無い: ${dataDir}`), { code: 'NO_MIRROR' });

  try {
    // ── skus / products / costs ← mirror_products ──
    const products = rows(mirror, 'select * from mirror_products');
    src.mirror_products = products.length;
    const skuByNorm = new Map();
    for (const r of products) {
      const code = s(r['商品コード']); if (!code) continue;
      const kind = mapSkuKind(r['商品区分']);
      if (!kind) { (src.skipped ||= []).push({ table: 'mirror_products', code, reason: `商品区分 ${r['商品区分']}` }); continue; }
      const taxRate = mapTaxRate(r['消費税率']);
      const sc = n(r['売上分類']);
      const sku = {
        code, name: s(r['商品名']) || code, kind, taxRate, taxClass: mapTaxClass(r['税区分'], taxRate),
        handling: mapHandling(r['取扱区分']), salesClass: sc != null && sc >= 1 && sc <= 4 ? sc : null,
        representativeCode: s(r['代表商品コード']), supplierCode: s(r['仕入先コード']), cost: mapCost(r),
        shippingCode: s(r['送料コード']), shippingMethod: s(r['配送方法']),
      };
      plan.skus.push(sku);
      skuByNorm.set(normSku(code), sku);
    }
    const knownSku = (code) => skuByNorm.has(normSku(code));
    const isSingleSku = (code) => skuByNorm.get(normSku(code))?.kind === 'single';

    // ── バリエーションのまとまり ← 代表商品コード (D-24 = A。NE の代表コードは実在しない名札なので、engine が product を作って束ねる) ──
    const groupMap = new Map();   // norm(代表コード) → { code, childCodes, names, active }
    for (const sku of plan.skus) {
      const rep = sku.representativeCode;
      if (!rep || normSku(rep) === normSku(sku.code)) continue;
      const k = normSku(rep); if (!k) continue;
      if (!groupMap.has(k)) groupMap.set(k, { code: rep, childCodes: [], names: [], active: false });
      const g = groupMap.get(k);
      g.childCodes.push(sku.code); g.names.push(sku.name);
      if (sku.handling === 'active') g.active = true;
    }
    plan.variationGroups = [...groupMap.values()].map((g) => ({
      code: g.code, name: variationGroupName(g.names, g.code), childCodes: g.childCodes,
      status: g.active ? 'active' : 'discontinued',   // 子が全部 取扱中以外なら まとまりも discontinued
    }));
    src.variation_groups = plan.variationGroups.length;
    src.variation_children = plan.variationGroups.reduce((n, g) => n + g.childCodes.length, 0);

    // ── set components ──
    const comps = hasTable(mirror, 'mirror_set_components') ? rows(mirror, 'select * from mirror_set_components') : [];
    src.mirror_set_components = comps.length;
    for (const r of comps) plan.setComponents.push({ parentCode: s(r['セット商品コード']), childCode: s(r['構成商品コード']), qty: n(r['数量']) ?? 1, source: 'ne' });

    // ── suppliers ──
    const supMap = new Map();   // code_norm → {code, name, ...}
    if (hasTable(mirror, 'po_suppliers')) {
      for (const r of rows(mirror, 'select * from po_suppliers')) {
        const code = s(r.supplier_code); if (!code) continue;
        supMap.set(normSku(code), { code, name: s(r.name) || code, orderMethod: s(r.send_method), leadTimeDays: n(r.lead_days) });
      }
      src.po_suppliers = supMap.size;
    }
    if (hasTable(mirror, 'supplier_share_master')) {
      for (const r of rows(mirror, 'select * from supplier_share_master')) {
        const code = s(r['仕入先コード']); if (!code || supMap.has(normSku(code))) continue;
        supMap.set(normSku(code), { code, name: s(r['表示名']) || code });
      }
    }
    for (const sku of plan.skus) {
      if (sku.supplierCode && !supMap.has(normSku(sku.supplierCode))) supMap.set(normSku(sku.supplierCode), { code: sku.supplierCode, name: sku.supplierCode });
    }
    plan.suppliers = [...supMap.values()];
    const ssKeys = new Set();
    if (hasTable(mirror, 'po_vendor_code_map')) {
      const vc = rows(mirror, 'select * from po_vendor_code_map');
      src.po_vendor_code_map = vc.length;
      for (const r of vc) {
        const sc = s(r.supplier_code), pc = s(r.product_code); if (!sc || !pc) continue;
        ssKeys.add(`${normSku(sc)}|${normSku(pc)}`);
        plan.supplierSkus.push({ supplierCode: sc, skuCode: pc, vendorCode: s(r.vendor_code), stockUnitsPerOrderUnit: n(r.qty_per_unit) > 0 && Number.isInteger(n(r.qty_per_unit)) ? n(r.qty_per_unit) : null });
      }
    }
    for (const sku of plan.skus) {
      if (!sku.supplierCode) continue;
      const k = `${normSku(sku.supplierCode)}|${normSku(sku.code)}`;
      if (ssKeys.has(k)) continue;
      ssKeys.add(k);
      plan.supplierSkus.push({ supplierCode: sku.supplierCode, skuCode: sku.code });
    }

    // ── Amazon listings ← mirror_sku_master + mirror_sku_resolved (+ fees の ASIN, fba.db の ASIN/JAN/FNSKU) ──
    const fees = hasTable(mirror, 'mirror_amazon_sku_fees') ? new Map(rows(mirror, 'select seller_sku, asin from mirror_amazon_sku_fees where asin is not null').map((r) => [normSku(r.seller_sku), s(r.asin)])) : new Map();
    src.mirror_amazon_sku_fees_with_asin = fees.size;
    const fba = openRo(path.join(dataDir, 'fba.db'));
    // seller_sku norm → { sheet:{asin,jan,fnsku,ne_code,is_set}, attrs:{asin,fnsku,source,updatedAt} }。sku_mapping (Sheet) と fba_sku_attrs は別の出どころ (Codex R1-3)
    const fbaMap = new Map();
    if (fba) {
      try {
        if (hasTable(fba, 'sku_mapping')) {
          for (const r of rows(fba, 'select amazon_sku, asin, jan, fnsku, ne_code, is_set, logizard_code from sku_mapping')) {
            const k = normSku(r.amazon_sku); if (!k) continue;
            fbaMap.set(k, { ...(fbaMap.get(k) || {}), sheet: { asin: s(r.asin), jan: s(r.jan), fnsku: s(r.fnsku), ne_code: s(r.ne_code) || s(r.logizard_code), is_set: !!r.is_set } });
          }
        }
        if (hasTable(fba, 'fba_sku_attrs')) {
          const hasUpd = hasColumn(fba, 'fba_sku_attrs', 'updated_at'); const hasSrc = hasColumn(fba, 'fba_sku_attrs', 'source');
          for (const r of rows(fba, `select amazon_sku, asin, fnsku${hasSrc ? ', source' : ''}${hasUpd ? ', updated_at' : ''} from fba_sku_attrs`)) {
            const k = normSku(r.amazon_sku); if (!k) continue;
            fbaMap.set(k, { ...(fbaMap.get(k) || {}), attrs: { asin: s(r.asin), fnsku: s(r.fnsku), source: s(r.source) || 'unknown', updatedAt: toIso(r.updated_at) } });
          }
        }
        src.fba_sku_mapping = [...fbaMap.values()].filter((f) => f.sheet).length;
        src.fba_sku_attrs = [...fbaMap.values()].filter((f) => f.attrs).length;
      } finally { fba.close(); }
    }
    const master = hasTable(mirror, 'mirror_sku_master') ? rows(mirror, 'select seller_sku, 商品名 from mirror_sku_master') : [];
    const resolved = hasTable(mirror, 'mirror_sku_resolved') ? rows(mirror, 'select seller_sku, ne_code, quantity, sort_order from mirror_sku_resolved order by seller_sku, sort_order') : [];
    src.mirror_sku_master = master.length; src.mirror_sku_resolved = resolved.length;
    const compBySeller = new Map();
    for (const r of resolved) { const k = normSku(r.seller_sku); if (!compBySeller.has(k)) compBySeller.set(k, []); compBySeller.get(k).push({ code: s(r.ne_code), qty: n(r.quantity) ?? 1, resolution: 'imported', evidence: { source: 'm_sku_master' } }); }
    const amazonSeen = new Set();
    const pushAmazon = (sellerSku, title, components, evidenceSource) => {
      const k = normSku(sellerSku); if (!k || amazonSeen.has(k)) return; amazonSeen.add(k);
      const f = fbaMap.get(k) || {};
      const sheet = f.sheet; const attrs = f.attrs;
      // ASIN / FNSKU の候補は出どころ別に全部 (どれを採るかは engine の優先順 ASIN_SOURCE_PRIORITY。違えば conflict)
      const asinCandidates = [];
      if (attrs?.asin) asinCandidates.push({ asin: attrs.asin, source: 'fba_sku_attrs' });
      if (sheet?.asin) asinCandidates.push({ asin: sheet.asin, source: 'fba_sheet_import' });
      if (fees.get(k)) asinCandidates.push({ asin: fees.get(k), source: 'amazon_fees' });
      const fnskuCandidates = [];
      // fba_sku_attrs が Sheet 以外の出どころ (planning / restock) で FNSKU を空にしていたら「外された」= Sheet の古い FNSKU も使わない
      const fnskuCleared = attrs && attrs.source !== 'sheet_backfill' && !attrs.fnsku;
      if (attrs?.fnsku) fnskuCandidates.push({ fnsku: attrs.fnsku, source: 'fba_sku_attrs' });
      if (sheet?.fnsku && !fnskuCleared) fnskuCandidates.push({ fnsku: sheet.fnsku, source: 'fba_sheet_import' });
      if (fnskuCleared && sheet?.fnsku) (src.fnsku_cleared_by_attrs ||= []).push(sellerSku);
      const listing = { mall: 'amazon', shopCode: SHOP_CODES.amazon, marketplaceId: MARKETPLACE_JP, listingCode: sellerSku, title: title || null, status: 'active', components, asinCandidates, fnskuCandidates, fnskuCleared: !!fnskuCleared, evidenceSource };
      plan.listings.push(listing);
      if (sheet?.jan && isJan(sheet.jan)) {
        // JAN は商品の属性。🚨 単品 1 個 (構成 1 行・qty=1・単品 SKU) のときだけ商品に。複数個パック・セット (セット SKU × 1 も) は listing の属性 (06 §11-3 包装範囲。Codex R1-2 / R2 M3)
        // Sheet には時刻が無い → observedAt null (engine が「最新と同じ内容なら再送」で判定し、新しければロード時刻)
        const single = singleUnitCode(components, isSingleSku);
        if (single) plan.observations.push({ skuCode: single, attribute: 'jan', scope: 'item', valueText: sheet.jan, source: 'fba_sheet_import', sourceRef: `sku_mapping:${sellerSku}`, observedAt: null });
        else plan.observations.push({ listingRef: { mall: 'amazon', shopCode: SHOP_CODES.amazon, listingCode: sellerSku }, attribute: 'jan', scope: 'listing', valueText: sheet.jan, source: 'fba_sheet_import', sourceRef: `sku_mapping:${sellerSku}`, observedAt: null });
      }
    };
    for (const r of master) pushAmazon(s(r.seller_sku), s(r['商品名']), compBySeller.get(normSku(r.seller_sku)) || [], 'mirror_sku_master');
    // Sheet / attrs だけにある SKU (D-2: 差は 5 行以内)
    for (const [k, f] of fbaMap) {
      if (amazonSeen.has(k)) continue;
      const sheet = f.sheet;
      const comps2 = sheet?.ne_code && !sheet.is_set ? [{ code: sheet.ne_code, qty: 1, resolution: 'imported', evidence: { source: 'fba_sheet' } }] : [];
      pushAmazon(k, null, comps2, sheet ? 'fba_sheet' : 'fba_sku_attrs');
    }

    // ── 楽天 listings ← mirror_rakuten_sku_map (AM > AL > W の別名を 1 listing にまとめる) ──
    const rk = hasTable(mirror, 'mirror_rakuten_sku_map') ? rows(mirror, 'select rakuten_code, ne_code, source, manage_number from mirror_rakuten_sku_map') : [];
    src.mirror_rakuten_sku_map = rk.length;
    const PRI = { am: 1, al: 2, w: 3 };
    const kindOf = (source) => (source === 'am' ? 'system_sku_number' : source === 'al' ? 'sku_manage_number' : 'item_number');
    const groups = new Map();   // manage_number|ne_code → rows (manage_number が無い行は自分だけ)
    for (const r of rk) {
      const key = r.manage_number ? `${s(r.manage_number)}|${normSku(r.ne_code)}` : `#${normSku(r.rakuten_code)}`;
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key).push(r);
    }
    for (const list of groups.values()) {
      list.sort((a, b) => (PRI[a.source] || 9) - (PRI[b.source] || 9));
      const amCount = list.filter((r) => r.source === 'am').length;
      // 🚨 同じ商品ページ・同じ NE コードに AM (システム連携用 SKU 番号) が 2 つ以上 = 楽天側の別 SKU が同じ NE コードに解決している。
      //    別名として 1 つに束ねると混ざるので、行ごとに listing にして report に残す (人が見る。Codex R1 M11)
      const bundles = amCount > 1 ? list.map((r) => [r]) : [list];
      if (amCount > 1) (src.rakuten_alias_ambiguous ||= []).push({ manage_number: s(list[0].manage_number), ne_code: s(list[0].ne_code), am: amCount });
      for (const b of bundles) {
        const primary = b[0];
        plan.listings.push({
          mall: 'rakuten', shopCode: SHOP_CODES.rakuten, listingCode: s(primary.rakuten_code), mallItemId: s(primary.manage_number), status: 'active',
          components: [{ code: s(primary.ne_code), qty: 1, resolution: 'map', evidence: { source: `f_rakuten_sku_map:${primary.source}` } }],
          externalIds: b.map((r) => ({ system: 'rakuten', kind: kindOf(r.source), value: s(r.rakuten_code) })),
        });
      }
    }

    // ── Yahoo listings ← rakuten-yahoo-sync.db yahoo_registered_items (item_code = NE コードと同じ運用なら exact) ──
    const rys = openRo(path.join(dataDir, 'rakuten-yahoo-sync.db'));
    const notionJanByManage = new Map();
    if (rys) {
      try {
        if (hasTable(rys, 'yahoo_registered_items')) {
          const y = rows(rys, 'select item_code, yahoo_item_code, has_sub_code from yahoo_registered_items');
          src.yahoo_registered_items = y.length;
          for (const r of y) {
            const code = s(r.yahoo_item_code) || s(r.item_code); if (!code) continue;
            const ne = s(r.item_code);
            plan.listings.push({ mall: 'yahoo', shopCode: SHOP_CODES.yahoo, listingCode: code, status: 'active', components: ne ? [{ code: ne, qty: 1, resolution: 'exact', evidence: { source: 'yahoo_registered_items' } }] : [] });
          }
        }
        if (hasTable(rys, 'notion_overrides')) {
          for (const r of rows(rys, 'select rakuten_manage_number, yahoo_jan from notion_overrides where yahoo_jan is not null')) if (isJan(r.yahoo_jan)) notionJanByManage.set(s(r.rakuten_manage_number), s(r.yahoo_jan));
          src.notion_overrides_with_jan = notionJanByManage.size;
        }
      } finally { rys.close(); }
    }
    // Notion の JAN: manage_number → NE コードが 1 つに決まる場合だけ
    const neByManage = new Map();
    for (const r of rk) { const m = s(r.manage_number); if (!m) continue; if (!neByManage.has(m)) neByManage.set(m, new Set()); neByManage.get(m).add(normSku(r.ne_code)); }
    for (const [m, jan] of notionJanByManage) {
      const set = neByManage.get(m);
      if (!set || set.size !== 1) { (src.notion_jan_ambiguous ||= []).push(m); continue; }
      const norm = [...set][0]; const sku = skuByNorm.get(norm); if (!sku) continue;
      plan.observations.push({ skuCode: sku.code, attribute: 'jan', scope: 'item', valueText: jan, source: 'notion_import', sourceRef: `notion_overrides:${m}`, observedAt: null });
    }

    // ── Qoo10 listings ← mirror_qoo10_items ──
    if (hasTable(mirror, 'mirror_qoo10_items')) {
      const q = rows(mirror, 'select item_no, seller_code, item_name, brand from mirror_qoo10_items');
      src.mirror_qoo10_items = q.length;
      for (const r of q) {
        const itemNo = s(r.item_no); if (!itemNo) continue;
        const ne = s(r.seller_code);
        plan.listings.push({ mall: 'qoo10', shopCode: SHOP_CODES.qoo10, listingCode: itemNo, mallItemId: itemNo, title: s(r.item_name), status: 'active', components: ne ? [{ code: ne, qty: 1, resolution: 'exact', evidence: { source: 'mirror_qoo10_items' } }] : [] });
        if (ne && s(r.brand) && knownSku(ne)) plan.observations.push({ skuCode: ne, attribute: 'brand', scope: 'item', valueText: s(r.brand), source: 'qoo10', sourceRef: `qoo10:${itemNo}`, observedAt: null });
      }
    }

    // ── JAN: ロジザードのバーコードマスタ (rank 0 = 代表。副バーコードは jan_secondary として残すだけで採用しない。Codex R1-10) ──
    if (hasTable(mirror, 'f_inbound_check_barcode_master')) {
      const b = rows(mirror, "select barcode, code_key, product_id, rank, updated_at from f_inbound_check_barcode_master where barcode_type = 'jan' order by code_key, rank");
      src.barcode_master_jan = b.length;
      for (const r of b) {
        const code = s(r.product_id) || s(r.code_key); if (!code || !isJan(r.barcode)) continue;
        plan.observations.push({ skuCode: code, attribute: Number(r.rank) === 0 ? 'jan' : 'jan_secondary', scope: 'item', valueText: s(r.barcode), source: 'logizard', sourceRef: `barcode_master:rank${r.rank}`, observedAt: toIso(r.updated_at) });
      }
    }

    // ── product-hub: JAN / ブランド / 内容量 / 表示義務 ──
    if (hasTable(mirror, 'product_drafts')) {
      const hasUpd = hasColumn(mirror, 'product_drafts', 'updated_at');
      const drafts = rows(mirror, `select id, ne_code, name, jan_code, asin, own_brand, status${hasUpd ? ', updated_at' : ''} from product_drafts where status <> 'excluded'`);
      src.product_drafts = drafts.length;
      const pageInfo = hasTable(mirror, 'draft_page_info') ? new Map(rows(mirror, 'select * from draft_page_info').map((r) => [r.draft_id, r])) : new Map();
      const skuJans = hasTable(mirror, 'draft_sku_jans') ? rows(mirror, 'select draft_id, sku_code, jan_code, updated_at from draft_sku_jans') : [];
      const draftAt = new Map();
      for (const d of drafts) {
        const code = s(d.ne_code); if (!code) continue;
        const at = toIso(d.updated_at); draftAt.set(d.id, at);   // 無ければ null (時刻の無い観測)
        if (isJan(d.jan_code)) plan.observations.push({ skuCode: code, attribute: 'jan', scope: 'item', valueText: s(d.jan_code), source: 'product_hub', sourceRef: `product_drafts:${d.id}`, observedAt: at });
        const pi = pageInfo.get(d.id);
        if (pi) {
          const piAt = toIso(pi.updated_at) || at;
          if (s(pi.brand_name)) plan.observations.push({ skuCode: code, attribute: 'brand', scope: 'item', valueText: s(pi.brand_name), source: 'product_hub', sourceRef: `draft_page_info:${d.id}`, observedAt: piAt });
          const c = parseContent(pi.content_volume);
          if (c) plan.observations.push({ skuCode: code, attribute: 'net_content', scope: 'item', valueNum: c.num, unit: c.unit, rawText: s(pi.content_volume), source: 'product_hub', sourceRef: `draft_page_info:${d.id}`, observedAt: piAt });
          const ingredients = s(pi.ingredients) || s(pi.food_ingredients);
          if (ingredients || s(pi.usage_notes) || s(pi.seller_name) || s(pi.importer_name)) {
            plan.compliance.push({ skuCode: code, ingredients, precautions: s(pi.usage_notes), distributor: s(pi.seller_name) || s(pi.importer_name), manufacturerJp: null, allergens: null, source: 'product_hub', sourceRef: `draft_page_info:${d.id}` });
          }
        }
      }
      for (const j of skuJans) {
        const code = s(j.sku_code); if (!code || !isJan(j.jan_code)) continue;
        plan.observations.push({ skuCode: code, attribute: 'jan', scope: 'item', valueText: s(j.jan_code), source: 'product_hub', sourceRef: `draft_sku_jans:${j.draft_id}`, observedAt: toIso(j.updated_at) || draftAt.get(j.draft_id) || null });
      }
    }

    // ── 入数 ← f_inbound_info (意味は D-20 で確認中。観測として残すだけで、規則が無いので採用されない) ──
    if (hasTable(mirror, 'f_inbound_info')) {
      const ii = rows(mirror, 'select 商品コード, 入数, updated_at from f_inbound_info where 入数 is not null');
      src.f_inbound_info_with_count = ii.length;
      for (const r of ii) {
        const code = s(r['商品コード']); const v = n(r['入数']);
        if (!code || v == null) continue;
        plan.observations.push({ skuCode: code, attribute: 'unit_count', scope: 'item', valueNum: v, unit: '個', rawText: String(r['入数']), source: 'inbound_info', sourceRef: 'f_inbound_info', observedAt: toIso(r.updated_at) });
      }
    }

    // ── 重量 ← postage.db pm_skus / fba-box.db fbx_weight_* (観測時刻 = 出どころの更新時刻。Codex R1-9) ──
    const postage = openRo(path.join(dataDir, 'postage.db'));
    if (postage) {
      try {
        if (hasTable(postage, 'pm_skus')) {
          const hasUpd = hasColumn(postage, 'pm_skus', 'updated_at');
          const pm = rows(postage, `select sku_code, unit_weight_g, thickness_mm, weight_source${hasUpd ? ', updated_at' : ''} from pm_skus where unit_weight_g is not null`);
          src.pm_skus_with_weight = pm.length;
          for (const r of pm) {
            const src2 = r.weight_source === 'measured' ? 'measured' : r.weight_source === 'supplier' ? 'supplier' : 'postage_estimate';
            const at = toIso(r.updated_at);   // 無ければ null (時刻の無い観測。engine が「最新と同じ内容なら再送」で判定)
            plan.physicals.push({ skuCode: s(r.sku_code), scope: 'package', weightG: Math.round(n(r.unit_weight_g)), heightMm: n(r.thickness_mm) ? Math.round(n(r.thickness_mm)) : null, source: src2, sourceRef: 'pm_skus', isMeasured: src2 === 'measured', observedAt: at });
            plan.observations.push({ skuCode: s(r.sku_code), attribute: 'package_weight_g', scope: 'package', valueNum: Math.round(n(r.unit_weight_g)), unit: 'g', source: src2, sourceRef: 'pm_skus', observedAt: at });
          }
        }
      } finally { postage.close(); }
    }
    const fbx = openRo(path.join(dataDir, 'fba-box.db'));
    if (fbx) {
      try {
        // FNSKU → Amazon の listing。🚨 engine と同じ優先順で「採用される FNSKU」だけを逆引きに使う (不採用の古い FNSKU や取り合い中の FNSKU から重量を付けない。Codex R2 H7)。
        //    採用 FNSKU が 2 つ以上の listing で同じ = 取り合い → 逆引きしない (record)
        // 単品 1 個 (構成 1 行・qty=1・単品 SKU) なら商品の梱包属性、それ以外 (複数個パック・セット) は listing の属性
        const listingsByFnsku = new Map();
        for (const l of plan.listings) {
          if (l.mall !== 'amazon') continue;
          const win = pickByPriority(l.fnskuCandidates || [], FNSKU_SOURCE_PRIORITY, 'fnsku'); if (!win) continue;
          const k = normSku(win.fnsku); if (!listingsByFnsku.has(k)) listingsByFnsku.set(k, []); listingsByFnsku.get(k).push(l);
        }
        const listingByFnsku = new Map();
        for (const [k, ls] of listingsByFnsku) { if (ls.length === 1) listingByFnsku.set(k, ls[0]); else (src.fnsku_contended ||= []).push({ fnsku: ls[0].fnskuCandidates.find((c) => normSku(c.fnsku) === k)?.fnsku || k, listings: ls.map((l) => l.listingCode) }); }
        // via = { fnsku, listing }: engine は「その出品にその FNSKU が実際に付いた (same / 新規 / manual)」ときだけ入れる (DB 側で manual / 取り合いに負けた FNSKU の重量を付けない。Codex R3-3)
        const addW = (r, g, source, isMeasured, ref, at) => {
          const l = listingByFnsku.get(normSku(r.fnsku)); if (!l || !(g > 0)) return;
          const listingRef = { mall: 'amazon', shopCode: l.shopCode, listingCode: l.listingCode };
          const via = { fnsku: s(r.fnsku), listing: listingRef };
          const single = singleUnitCode(l.components, isSingleSku);
          if (single) {
            plan.physicals.push({ skuCode: single, scope: 'package', weightG: Math.round(g), source, sourceRef: ref, isMeasured, observedAt: at, via });
            plan.observations.push({ skuCode: single, attribute: 'package_weight_g', scope: 'package', valueNum: Math.round(g), unit: 'g', source, sourceRef: ref, observedAt: at, via });
          } else {
            plan.observations.push({ listingRef, attribute: 'package_weight_g', scope: 'listing', valueNum: Math.round(g), unit: 'g', source, sourceRef: ref, observedAt: at, via });
          }
        };
        if (hasTable(fbx, 'fbx_weight_refs')) {
          const w = rows(fbx, "select fnsku, weight_g, fetched_at from fbx_weight_refs where status = 'ok' and weight_g is not null"); src.fbx_weight_refs = w.length;
          for (const r of w) addW(r, n(r.weight_g), 'amazon_catalog', false, `fbx_weight_refs:${r.fnsku}`, toIso(r.fetched_at));
        }
        if (hasTable(fbx, 'fbx_weight_current')) {
          const w = rows(fbx, 'select fnsku, unit_g, source, updated_at from fbx_weight_current'); src.fbx_weight_current = w.length;
          for (const r of w) addW(r, n(r.unit_g), r.source === 'measured' ? 'measured' : 'amazon_catalog', r.source === 'measured', `fbx_weight_current:${r.fnsku}`, toIso(r.updated_at));
        }
      } finally { fbx.close(); }
    }

    // ── workers ← staff.db ──
    const staff = openRo(path.join(dataDir, 'staff.db'));
    if (staff) {
      try {
        if (hasTable(staff, 'staff')) {
          const st = rows(staff, 'select staff_no, display_name, portal_email, kind, active from staff');
          src.staff = st.length;
          for (const r of st) plan.workers.push({ staffNo: s(r.staff_no), displayName: s(r.display_name), loginEmail: s(r.portal_email), workerType: mapWorkerType(r.kind), active: !!r.active, companyId: r.kind === 'iroha' ? 2 : 1 });
        }
      } finally { staff.close(); }
    }
  } finally {
    mirror.close();
  }
  log(`plan: skus ${plan.skus.length}, components ${plan.setComponents.length}, listings ${plan.listings.length}, observations ${plan.observations.length}, physicals ${plan.physicals.length}, suppliers ${plan.suppliers.length}, workers ${plan.workers.length}`);
  return plan;
}
