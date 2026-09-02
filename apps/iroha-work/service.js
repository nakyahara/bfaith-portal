/**
 * いろは在庫化作業アプリ — 一覧の組み立て
 *
 * Notion キャッシュ (カード) に、自社 DB の参照情報を重ねる:
 *   - 作業仕様 = f_iroha_work_master (最新値。カード作成時のスナップショットより優先)
 *   - 販売/在庫 = mirror_sales_daily / mirror_logizard_stock → 優先度 (残り在庫日数)
 *   - 商品画像 = pk_product_images (picking のキャッシュ)
 *
 * 優先度 (要件定義 §5 / Codex R1 Q4):
 *   在庫日数 = フリー在庫 ÷ (30日販売数/30)。**理由を必ず付ける**。
 *   データ欠損をゼロで代用しない — 「在庫データなし」「新商品」はそれぞれ別の表示にする。
 */
import { buildEnrichContext } from '../inbound-check/notion-sync.js';
import { productImageMap } from '../inbound-check/db.js';
import { queueEnsureImages } from '../picking/images.js';
import { getDB, listCache, activeSessionsByPage, estimateByProduct } from './db.js';
import { STATUSES } from './notion-read.js';

// 「急ぎ」の線引き: 在庫切れ、または残り在庫日数がこれ以下
export const URGENT_DAYS = 3;

// 販売/在庫/仕入先などの参照マップは重い (mirror_sales_daily の30日集計など) ので数分キャッシュ
const ENRICH_TTL_MS = 5 * 60 * 1000;
let enrichCache = null; // { at, ctx }

function enrichContext() {
  if (enrichCache && Date.now() - enrichCache.at < ENRICH_TTL_MS) return enrichCache.ctx;
  const ctx = buildEnrichContext(getDB());
  enrichCache = { at: Date.now(), ctx };
  return ctx;
}

/** テスト・強制更新用 */
export function clearEnrichCache() { enrichCache = null; }

const keyOf = (code) => String(code || '').trim().toLowerCase();

/** 優先度 (kind: urgent > new > normal > unknown > calm。label は画面へそのまま出す理由) */
export function priorityOf(sales30, freeStock) {
  if (sales30 == null && freeStock == null) return { kind: 'new', label: '新商品', days: null };
  const s = Number(sales30) || 0;
  if (s > 0) {
    if (freeStock == null) return { kind: 'unknown', label: '在庫データなし', days: null };
    if (freeStock <= 0) return { kind: 'urgent', label: '在庫切れ', days: 0 };
    const days = freeStock / (s / 30);
    const label = `残り${days < 10 ? days.toFixed(1) : String(Math.round(days))}日分`;
    return { kind: days <= URGENT_DAYS ? 'urgent' : 'normal', label, days };
  }
  return { kind: 'calm', label: '30日販売なし', days: null };
}

const PRIORITY_RANK = { urgent: 0, new: 1, normal: 2, unknown: 3, calm: 4 };

/**
 * 作業仕様。f_iroha_work_master があればそれ (最新)、無ければカード作成時の
 * スナップショット (Notion プロパティ) で代用し、source で区別する。
 * missing = 現場が作業を始めるのに足りない項目 (⚠未登録バッジの根拠)
 */
function masterOf(wm, props) {
  const m = wm
    ? { source: 'master', material_code: wm.material_code || null, storage_container: wm.storage_container || null,
        units_per_container: wm.units_per_container ?? null, process_count: wm.process_count ?? null, note: wm.note || null }
    : { source: 'card', material_code: props['資材セットID'] || null, storage_container: props['収納容器'] || null,
        units_per_container: props['入数'] ?? null, process_count: props['工程数'] ?? null, note: props['備考'] || null };
  const missing = [];
  if (!m.material_code) missing.push('資材');
  if (!m.storage_container) missing.push('容器');
  if (m.units_per_container == null) missing.push('入数');
  if (m.process_count == null) missing.push('工程');
  m.missing = missing;
  return m;
}

/**
 * 一覧を組み立てて優先度順に並べる。
 * 並び = 急ぎ (在庫日数昇順) → 新商品 → 通常 (在庫日数昇順) → データなし → 販売なし、
 * 同順位は入庫日の古い順 (要件定義 §5)。タブごとの絞り込みは画面側で行う。
 */
export function buildList() {
  const rows = listCache();
  const ctx = enrichContext();
  const images = productImageMap(rows.map(r => r.product_code));
  const activeMap = activeSessionsByPage();
  const estimates = estimateByProduct();

  const cards = rows.map((r) => {
    let props = {};
    try { props = JSON.parse(r.payload || '{}'); } catch { /* 壊れた payload は素の表示になるだけ */ }
    const k = keyOf(r.product_code);
    const sales30 = k ? (ctx.sales30.get(k) ?? null) : null;
    const freeStock = k ? (ctx.freeStock.get(k) ?? null) : null;
    return {
      page_id: r.page_id,
      status: r.status,
      title: r.title,
      product_code: r.product_code,
      url: r.url,
      last_edited_time: r.last_edited_time,
      qty: props['数量'] ?? null,
      arrival: props['入庫日'] || null,
      ar_no: props['入荷管理番号'] || null,
      barcode: props['バーコード'] || null,
      expiry: props['有効期限'] || null,
      supplier: props['取引先'] || null,
      handling: props['取扱区分'] || null,
      sales_text: props['過去30日販売数'] || null,
      external_text: props['外部出し目安'] || null,
      image_url: images.get(k) || null,
      master: masterOf(k ? ctx.workMaster.get(k) : null, props),
      live: { sales30, free_stock: freeStock },
      priority: priorityOf(sales30, freeStock),
      // 作業時間: いま作業中の人 + 過去の実測 (カード単位合計の平均。1回だけなら「前回」表示)
      active: activeMap.get(r.page_id) || [],
      estimate: (k && estimates.get(k)) || null,
    };
  });

  cards.sort((a, b) => {
    const ra = PRIORITY_RANK[a.priority.kind] ?? 9;
    const rb = PRIORITY_RANK[b.priority.kind] ?? 9;
    if (ra !== rb) return ra - rb;
    const da = a.priority.days ?? Infinity;
    const db_ = b.priority.days ?? Infinity;
    if (da !== db_) return da - db_;
    const aa = a.arrival || '9999-99-99';
    const ab = b.arrival || '9999-99-99';
    if (aa !== ab) return aa < ab ? -1 : 1;
    return String(a.title).localeCompare(String(b.title), 'ja');
  });

  // 画像がまだ無い商品は裏で解決を仕掛けておく (次に開いたとき出る)
  const missingImg = [...new Set(cards.filter(c => c.product_code && !c.image_url).map(c => c.product_code))];
  if (missingImg.length > 0) {
    try { queueEnsureImages(missingImg, 'いろは作業アプリ'); } catch (e) { console.warn('[iroha-work] 画像解決を飛ばしました:', e.message); }
  }

  return { cards, statuses: STATUSES };
}
