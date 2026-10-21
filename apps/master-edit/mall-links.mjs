/**
 * mall-links.mjs — 商品の画面の上に出す「モールの商品ページへのリンク」(読むだけ。書かない) (10/20 中原さん「商品マスタからそれぞれのモールに飛ぶリンクつけてほしい」)
 *
 * 読み元 = Company DB の出品 (core.listings + core.listing_components + core.catalog_items)。画面のロール master_edit が select できる表だけ
 *   (scripts/company-db/create-master-edit-roles.mjs の MASTER_EDIT_SELECT・AMAZON_LISTING_SELECT)。miniPC の SQLite は読まない。
 *   出品は毎晩のロード (apps/company-db/load/sources.mjs) が Render の写しから合わせ直す:
 *     楽天   = mirror_rakuten_sku_map (出品の構成 = NE の商品コード・mall_item_id = 商品管理番号) → https://item.rakuten.co.jp/<店>/<商品管理番号>/
 *     Yahoo! = rakuten-yahoo-sync.db の yahoo_registered_items (Yahoo で見えた商品コード) → https://store.shopping.yahoo.co.jp/<店>/<商品コード>.html
 *              色違いは Yahoo の商品コード = 親 (楽天の商品管理番号と同じ) で、構成 (NE の商品コード) が付かない
 *              → 構成で当たらないときは「楽天の商品管理番号と同じコードの Yahoo の出品 (構成の無いもの)」を出す (site-products と同じ決まり = RYS が楽天から Yahoo へ写す)
 *                構成が付いた Yahoo の出品は、その構成の商品の出品 = ここでは使わない (別の商品のページへ飛ばさない)
 *     Qoo10  = mirror_qoo10_items (listing_code = Qoo10 の商品番号) → https://www.qoo10.jp/g/<商品番号>
 *     Amazon = 対応表 (seller SKU)。ASIN = 夜間ロードの catalog_items → 無ければ Amazon の出品一覧 (0074・前の夜) → https://www.amazon.co.jp/dp/<ASIN>
 *              (Amazon US = 出品 amazon_us があれば https://www.amazon.com/dp/<ASIN>。いまは Company DB に無い = 出ない)
 *   au PAY マーケット・LINE ギフト・メルカリ Shops は Company DB に出品 (商品ページの番号) が無い = 出さない (注文にも商品ページの番号が載っていない)。
 * 当て方: この商品だけでできた出品 (構成が 1 行 = この SKU。× N のまとめ売りも)。ほかの商品と組んだ出品 (セットの出品) は出さない
 *   (その出品はセットの商品の画面で出す = セットの商品コードに当たる出品)。fba-stock.mjs の「1 × 1 の出品」と同じ考え。
 * 🚨 URL は決まった形だけ (https・決まったホスト・コードは encodeURIComponent・形の違う ASIN / 商品番号は出さない)。出品の status = deleted は出さない。
 *   楽天・Yahoo の URL は店のコードが入る = 自社の 1 店目 (shop_code = 'main') の出品だけ。
 * 読めない (権限が無い・表が無い・SQL の誤り) = 出さない (画面は止めない)。
 */
import { COMPANY_ID } from '../../lib/master-write.mjs';
import { AMAZON_JP_SHOP_CODE } from '../../lib/amazon-map-write.mjs';
import { ListTimeoutError } from './deadline.mjs';

/** 並べる順と名前 (短く)。リンクの無いモールは出さない */
export const MALLS = Object.freeze([
  { mall: 'rakuten', label: '楽天' },
  { mall: 'amazon', label: 'Amazon' },
  { mall: 'amazon_us', label: 'Amazon US' },
  { mall: 'yahoo', label: 'Yahoo!' },
  { mall: 'aupay', label: 'au PAY' },
  { mall: 'qoo10', label: 'Qoo10' },
  { mall: 'linegift', label: 'LINE ギフト' },
  { mall: 'mercari', label: 'メルカリ' },
]);
/** 1 つのモールに出すリンクの数の上限 (超えた分は「ほか N」) */
export const MAX_PER_MALL = 4;
/** 1 回に読む出品の行の上限 (1 つの商品に数百の出品は無い。あっても画面を重くしない) */
const MAX_ROWS = 200;
/** 楽天・Yahoo の URL の店 = 自社の 1 店目 (apps/company-db/load/sources.mjs の SHOP_CODES) */
const MAIN_SHOP = 'main';
/** Amazon の marketplace (catalog_items.marketplace_id)。ASIN の行が別の国のものなら使わない */
const MARKETPLACE = Object.freeze({ amazon: 'A1VC38T7YXB528', amazon_us: 'ATVPDKIKX0DER' });

const SLUG_RE = /^[a-z0-9][a-z0-9_-]{0,63}$/i;
const ASIN_RE = /^[A-Z0-9]{10}$/;
const QOO10_RE = /^\d{1,15}$/;
/** 店のコード: env (ほかの画面と同じ名前) → 無い・形が違う = 既定 */
function shopSlug(envName, fallback) {
  const v = String(process.env[envName] ?? '').trim();
  return SLUG_RE.test(v) ? v : fallback;
}
export const rakutenShop = () => shopSlug('RAKUTEN_SHOP_SLUG', 'b-faith');   // picking/images.js・site-products と同じ
export const yahooStore = () => shopSlug('YAHOO_SELLER_ID', 'b-faith01');    // site-products と同じ (注文番号の頭 b-faith01-… と同じ店)

/** 商品ページのコード (楽天の商品管理番号・Yahoo の商品コード): 空・空白・制御文字・/ ? # \・長すぎ = 出さない。URL は小文字 (両モールとも商品ページの URL は小文字) */
function pageCode(v) {
  const s = String(v ?? '').trim();
  if (!s || s.length > 100 || /[\s\u0000-\u001f\u007f/\\?#%]/.test(s)) return null;
  return encodeURIComponent(s.toLowerCase());
}

/**
 * 1 つの出品の商品ページの URL。作れない (コードが無い・形が違う・ページの番号を持たないモール) = null。
 *   ids = { code (出品コード), itemId (モールの中の番号 = 楽天の商品管理番号 / Qoo10 の商品番号), asin }
 */
export function mallItemUrl(mall, ids = {}) {
  switch (mall) {
    case 'rakuten': { const c = pageCode(ids.itemId); return c ? `https://item.rakuten.co.jp/${rakutenShop()}/${c}/` : null; }
    case 'yahoo': { const c = pageCode(ids.code); return c ? `https://store.shopping.yahoo.co.jp/${yahooStore()}/${c}.html` : null; }
    case 'qoo10': { const n = String(ids.itemId || ids.code || '').trim(); return QOO10_RE.test(n) ? `https://www.qoo10.jp/g/${n}` : null; }
    case 'amazon':
    case 'amazon_us': {
      const a = String(ids.asin ?? '').trim().toUpperCase();
      if (!ASIN_RE.test(a)) return null;
      return mall === 'amazon' ? `https://www.amazon.co.jp/dp/${a}` : `https://www.amazon.com/dp/${a}`;
    }
    // au PAY (wowma.jp/item/<番号>)・LINE ギフト (gift.line.me/item/<番号>)・メルカリ Shops (商品の ID) は商品ページの番号を Company DB に持たない = 作らない
    default: return null;
  }
}

/** 同じモールに 2 つ以上あるときに見分ける一言: 楽天 = 商品管理番号・Qoo10 = 商品番号・Amazon = ASIN・Yahoo = 商品コード */
function linkKey(r) {
  if (r.mall === 'rakuten') return String(r.mall_item_id ?? '').trim();
  if (r.mall === 'amazon' || r.mall === 'amazon_us') return String(r.asin ?? '').trim().toUpperCase();
  if (r.mall === 'qoo10') return String(r.mall_item_id || r.listing_code || '').trim();
  return String(r.listing_code ?? '').trim();
}

/**
 * 出品の行 → モールごとのリンク (純粋な関数。試験はここを直に呼ぶ)。
 *   rows = [{ mall, shop_code, listing_code, mall_item_id, asin, marketplace_id, qty, via ('component' | 'rakuten_page') }]
 *   同じ URL は 1 つにまとめる (FBA と自社発送の seller SKU が同じ ASIN・楽天の別名が同じ商品管理番号)。まとめ売り (× N) は一言に ×N
 *   via = 'rakuten_page' (楽天の商品管理番号で見つけた Yahoo の出品) は、構成で当たった Yahoo の出品があれば出さない
 * 戻り値 [{ mall, label, links: [{ url, text (一言・1 つだけなら空), title }], more }] (MALLS の順・リンクの無いモールは入れない)
 */
export function buildMallLinks(rows) {
  const by = new Map();
  for (const r of rows || []) {
    if (!r || !MALLS.some((m) => m.mall === r.mall)) continue;
    if ((r.mall === 'rakuten' || r.mall === 'yahoo') && r.shop_code != null && r.shop_code !== MAIN_SHOP) continue;
    const asin = r.asin && (!r.marketplace_id || r.marketplace_id === MARKETPLACE[r.mall]) ? r.asin : null;
    const url = mallItemUrl(r.mall, { code: r.listing_code, itemId: r.mall_item_id, asin });
    if (!url) continue;
    if (!by.has(r.mall)) by.set(r.mall, { direct: false, links: new Map() });
    const g = by.get(r.mall);
    const via = r.via === 'rakuten_page' ? 'rakuten_page' : 'component';
    if (via === 'component') g.direct = true;
    const qty = Number(r.qty) > 1 ? Math.trunc(Number(r.qty)) : 1;
    const code = String(r.listing_code ?? '').trim();
    const cur = g.links.get(url);
    if (cur) {
      if (code && !cur.codes.includes(code)) cur.codes.push(code);
      if (via === 'component') cur.via = 'component';
      cur.qty = Math.min(cur.qty, qty);   // 1 個の出品と × N が同じページ = 1 個として見せる
      continue;
    }
    g.links.set(url, { url, key: linkKey({ ...r, asin }), codes: code ? [code] : [], qty, via });
  }
  const out = [];
  for (const m of MALLS) {
    const g = by.get(m.mall);
    if (!g) continue;
    // 構成で当たった Yahoo の出品がある = 楽天の商品管理番号で探した方は出さない (同じ商品の別ページを並べない)
    const links = [...g.links.values()].filter((l) => !(g.direct && l.via === 'rakuten_page'));
    links.sort((a, b) => a.qty - b.qty || (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
    const many = links.length > 1;
    const amz = m.mall === 'amazon' || m.mall === 'amazon_us';
    const shown = links.slice(0, MAX_PER_MALL).map((l) => ({
      url: l.url,
      text: l.qty > 1 ? `×${l.qty}` : many ? l.key : '',
      title: [`${m.label} の商品ページを開く`,
        l.codes.length ? `${amz ? 'seller SKU' : '出品コード'} ${l.codes.slice(0, 3).join('・')}${l.codes.length > 3 ? ` ほか ${l.codes.length - 3}` : ''}` : '',
        amz ? `ASIN ${l.key}` : '',
        l.qty > 1 ? `${l.qty} 個のまとめ売り` : '',
        l.via === 'rakuten_page' ? '楽天の商品管理番号と同じコードの商品' : ''].filter(Boolean).join(' · '),
    }));
    out.push({ mall: m.mall, label: m.label, links: shown, more: links.length - shown.length });
  }
  return out;
}

/**
 * この商品の出品 (構成がこの SKU だけ) と、Yahoo の色違い (楽天の商品管理番号と同じコードの Yahoo の出品) を読む。
 *   SQL は 1 回 (+ ASIN の無い Amazon の出品があるときだけ、前の夜の出品一覧を 1 回)。
 *   使う索引: ix_listing_components_sku (sku_id)・listing_components の主キー (listing_id, sku_id)・listings の unique (mall, shop_code, listing_norm)・
 *            amazon_listing_report の ix_amazon_listing_report_norm (company_id, shop_code, sku_norm)
 * { ok: true, malls: buildMallLinks の答え } / { ok: false } (読めない = 出さない)
 */
export async function readMallLinks(db, skuId) {
  if (!db || skuId == null || !/^\d+$/.test(String(skuId))) return { ok: false };
  try {
    const rows = (await db.query(`
      with mine as (
        select l.listing_id, l.mall, l.shop_code, l.listing_code, l.listing_norm, l.mall_item_id, l.catalog_item_id, lc.qty
          from core.listing_components lc
          join core.listings l on l.company_id = lc.company_id and l.listing_id = lc.listing_id
         where lc.company_id = $1 and lc.sku_id = $2::bigint and l.status <> 'deleted'
           and not exists (select 1 from core.listing_components o where o.company_id = lc.company_id and o.listing_id = lc.listing_id and o.sku_id <> lc.sku_id)
      ), ypage as (
        select y.listing_id, y.mall, y.shop_code, y.listing_code, y.listing_norm, y.mall_item_id, y.catalog_item_id, 1 as qty
          from core.listings y
         where y.company_id = $1 and y.mall = 'yahoo' and y.shop_code = $3 and y.status <> 'deleted'
           and y.listing_norm in (select core.norm_code(m.mall_item_id) from mine m where m.mall = 'rakuten' and m.shop_code = $3 and nullif(btrim(m.mall_item_id), '') is not null)
           -- 構成の無い出品だけ (構成がある = その商品の出品。この商品だけの出品は mine が拾う・別の商品 / 組んだ出品は出さない = 取り違えない。Codex R1 Medium)
           and not exists (select 1 from core.listing_components o where o.company_id = y.company_id and o.listing_id = y.listing_id)
      )
      select x.mall, x.shop_code, x.listing_code, x.listing_norm, x.mall_item_id, x.qty::int as qty, ci.asin, ci.marketplace_id, x.via
        from (select mine.*, 'component' as via from mine union all select ypage.*, 'rakuten_page' as via from ypage) x
        left join core.catalog_items ci on ci.catalog_item_id = x.catalog_item_id
       order by x.mall, x.qty, x.listing_code
       limit ${MAX_ROWS}`, [COMPANY_ID, String(skuId), MAIN_SHOP])).rows;
    // Amazon (日本) の ASIN が出品に無い = 前の夜の Amazon の出品一覧で埋める (amazon-read.mjs と同じ: 同じ正規化の SKU が 2 行あって ASIN が食い違う = 埋めない)
    const noAsin = [...new Set(rows.filter((r) => r.mall === 'amazon' && r.shop_code === AMAZON_JP_SHOP_CODE && !r.asin).map((r) => r.listing_norm))];
    if (noAsin.length) {
      try {
        const rep = (await db.query(`
          with cur as (select load_id from ops.amazon_listing_report_loads where company_id = $1 and shop_code = $2 order by generation desc limit 1)
          select r.sku_norm, case when count(distinct r.asin) = 1 then min(r.asin) end as asin
            from ops.amazon_listing_report r join cur on cur.load_id = r.load_id
           where r.company_id = $1 and r.shop_code = $2 and r.sku_norm = any($3::text[])
           group by r.sku_norm`, [COMPANY_ID, AMAZON_JP_SHOP_CODE, noAsin])).rows;
        const asinOf = new Map(rep.filter((x) => x.asin).map((x) => [x.sku_norm, x.asin]));
        for (const r of rows) {
          if (r.mall === 'amazon' && r.shop_code === AMAZON_JP_SHOP_CODE && !r.asin && asinOf.has(r.listing_norm)) { r.asin = asinOf.get(r.listing_norm); r.marketplace_id = MARKETPLACE.amazon; }
        }
      } catch (e) {
        if (e instanceof ListTimeoutError) throw e;
        console.error(`[master-edit] モールのリンク: Amazon の出品一覧を読めない (ASIN の無い出品は出さない): ${e && e.message}`);
      }
    }
    return { ok: true, malls: buildMallLinks(rows) };
  } catch (e) {
    if (e instanceof ListTimeoutError) throw e;
    console.error(`[master-edit] モールのリンクを読めない: ${e && e.message}`);
    return { ok: false };
  }
}
