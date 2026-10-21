/**
 * cdb-tax-rate.mjs — Company DB の税率を読む・ドラフトの税率を決める (product-hub・Company DB構想 10 §4 #6・14 §5 / PR #1565 Codex R1 H5)
 *
 * 税率の列 (skus.tax_rate) の古い入口が閉じた後 (段階 frozen 以降で税率が C (active ∪ prepared)・⑤-3b)、product-hub は税率の手入力 (draft_yahoo.tax_rate) を使わない
 *   (段階か持ち主が読めないときは、詳細画面は今の値を見るだけ・出品は止める = listing-tax.mjs):
 *   詳細画面の表示・利益の試算・楽天の出品 (プレビュー・登録) は Company DB (core.skus.tax_rate) の税率だけ。閉じる前は呼ばない (今までどおり)。
 * ドラフトの商品コードが代表コード (バリエーションの親・SKU の行が無いことが多い) なら、構成の SKU (NE の写しの代表商品コードでまとまる子) の税率を読む。
 *   子の税率が混ざる・どれかが無い / 未解決 = 決められない (出品は止める)。Company DB を読めない = 決められない (fail-closed)
 * env: COMPANY_DB_URL (Render。無ければ COMPANY_DB_WATCH_URL)
 *
 * 送料 (core.skus.shipping_*・持ち主 company) も**同じ問い合わせ**で読む (2026-10-21 中原さん: マスタ編集で送料コードを入れても
 *   商品ハブには翌朝の写し (mirror_products) まで出なかった。税率と同じく、開いたときの Company DB の値を使う)。
 *   接続は増やさない。送料は決められなくても税率の判断は変えない (送料は試算と、セットの楽天配送方法の補完に使うだけ)
 */
import { resolveVariationGroup } from '../lib/variation.js';

let reader = null;
/** 試験用: 読み方を差し替える (本番では呼ばない)。fn(codes) → { ok: true, rows: [{ code, tax_rate, tax_class }] } / { ok: false, reason } */
export function __setCdbTaxReader(fn) { reader = fn || null; }

export function formatTaxRate(rate) {
  const n = Number(rate);
  if (!Number.isFinite(n) || n <= 0) return null;
  return `${Math.round(n * 100)}%`;
}

/**
 * 商品コードの税率をまとめて読む。戻り値 { ok: true, byCode: Map(入れたコード → { found, tax_rate (数・null), tax_class }) } / { ok: false, reason }
 */
export async function readCdbTaxRates(codes, { env = process.env } = {}) {
  const list = [...new Set((codes || []).map((c) => String(c ?? '').trim()).filter(Boolean))];
  if (!list.length) return { ok: false, reason: '商品コードが無い' };
  let rows;
  if (reader) {
    const r = await reader(list);
    if (!r || !r.ok) return { ok: false, reason: (r && r.reason) || 'Company DB を読めない' };
    rows = r.rows || [];
  } else {
    const url = String(env.COMPANY_DB_URL || '').trim() || String(env.COMPANY_DB_WATCH_URL || '').trim();
    if (!url) return { ok: false, reason: 'Company DB の接続先が無い' };
    let client = null;
    try {
      const { openPgClient } = await import('../../../scripts/company-db/migrate.mjs');
      const { COMPANY_ID } = await import('../../../lib/master-write.mjs');
      client = await openPgClient(url, { application_name: 'product-hub-cdb-tax', connectionTimeoutMillis: 5000, statement_timeout: 5000, query_timeout: 6000 });
      rows = (await client.query(`select x.code, k.sku_id is not null as found, k.tax_rate::text as tax_rate, k.tax_class,
            k.shipping_code, k.shipping_method, k.shipping_cost_jpy::text as shipping_cost_jpy
          from unnest($2::text[]) as x(code) left join core.skus k on k.company_id = $1 and k.code_norm = core.norm_code(x.code)`, [COMPANY_ID, list])).rows;
    } catch (e) {
      return { ok: false, reason: `Company DB を読めない (${String((e && e.message) || e).slice(0, 120)})` };
    } finally {
      if (client) { try { await client.end(); } catch { /* 結果は変えない */ } }
    }
  }
  const byCode = new Map();
  for (const r of rows) {
    byCode.set(String(r.code), {
      found: r.found !== false, tax_rate: r.tax_rate == null ? null : Number(r.tax_rate), tax_class: r.tax_class ?? null,
      // 送料の列を読んだ行だけ shipping を付ける (読み方を差し替えた試験で列が無い = 分からない = 写しのまま)
      ...(Object.prototype.hasOwnProperty.call(r, 'shipping_code') ? { shipping: shippingOfRow(r) } : {}),
    });
  }
  for (const c of list) if (!byCode.has(c)) byCode.set(c, { found: false, tax_rate: null, tax_class: null });
  return { ok: true, byCode };
}

/** 1 行の送料 { code, method, cost } (送料コードが空 = 未登録・cost は数か null) */
function shippingOfRow(r) {
  const code = r.shipping_code == null || String(r.shipping_code).trim() === '' ? null : String(r.shipping_code).trim();
  const method = r.shipping_method == null || String(r.shipping_method).trim() === '' ? null : String(r.shipping_method).trim();
  const n = r.shipping_cost_jpy == null || r.shipping_cost_jpy === '' ? NaN : Number(r.shipping_cost_jpy);
  return { code, method, cost: Number.isFinite(n) && n >= 0 ? n : null };
}

/**
 * 読んだ行から送料の答え { byCode: { 正規化したコード: { code, method, cost } } } を作る。
 * Company DB にある (found) で送料の列を読めたコードだけ入れる。1 つも無ければ null (= 写しのまま)
 */
export function shippingFromByCode(codes, byCode) {
  const out = {};
  for (const c of codes) {
    const r = byCode.get(c);
    if (r && r.found && r.shipping) out[String(c).trim().toLowerCase()] = r.shipping;
  }
  return Object.keys(out).length ? { byCode: out } : null;
}

/** 読んだ税率から 1 つに決める。無い・未解決・混ざる = 決められない */
export function decideTax(codes, byCode) {
  const missing = [], unresolved = [], rates = new Map();
  for (const c of codes) {
    const r = byCode.get(c);
    if (!r || !r.found) { missing.push(c); continue; }
    const label = formatTaxRate(r.tax_rate);
    if (!label) { unresolved.push(`${c} (${r.tax_class || '空'})`); continue; }
    if (!rates.has(label)) rates.set(label, []);
    rates.get(label).push(c);
  }
  if (missing.length) return { ok: false, reason: `Company DB に無い商品コード: ${missing.slice(0, 5).join(', ')}${missing.length > 5 ? ` ほか ${missing.length - 5} 件` : ''}` };
  if (unresolved.length) return { ok: false, reason: `Company DB の税率が決まっていない: ${unresolved.slice(0, 5).join(', ')}` };
  if (rates.size !== 1) return { ok: false, reason: `SKU の税率が混ざっている (${[...rates.entries()].map(([l, cs]) => `${l}: ${cs.slice(0, 3).join(', ')}`).join(' / ')})` };
  const [label] = [...rates.keys()];
  return { ok: true, label, percent: Number(label.replace('%', '')) };
}

/** ドラフトの税率のもとにする商品コード (バリエーションなら構成の SKU・それ以外は本人) */
export function taxCodesForDraft(db, draft) {
  const v = resolveVariationGroup(db, draft.ne_code, { draftId: draft.id });
  if (v.kind === 'variation' && Array.isArray(v.members) && v.members.length) return v.members.map((m) => String(m.商品コード).trim()).filter(Boolean);
  return [String(draft.ne_code ?? '').trim()].filter(Boolean);
}

/** ドラフトの Company DB の税率。{ ok: true, label, percent, codes } / { ok: false, reason, codes } */
export async function resolveCdbDraftTax(db, draft, { env = process.env } = {}) {
  const codes = taxCodesForDraft(db, draft);
  if (!codes.length) return { ok: false, reason: '商品コードが無い', codes };
  const r = await readCdbTaxRates(codes, { env });
  if (!r.ok) return { ok: false, reason: r.reason, codes };
  // 送料は税率を決められなくても返す (試算と案内に使う。税率の ok/ng は変えない)
  return { ...decideTax(codes, r.byCode), codes, shipping: shippingFromByCode(codes, r.byCode) };
}

/** 互換 (1 つの商品コード) */
export async function readCdbTaxRate(code, { env = process.env } = {}) {
  const c = String(code ?? '').trim();
  const r = await readCdbTaxRates([c], { env });
  if (!r.ok) return { ok: false, reason: r.reason };
  return decideTax([c], r.byCode);
}
