/**
 * cdb-tax-rate.mjs — Company DB の税率を読む・ドラフトの税率を決める (product-hub・Company DB構想 10 §4 #6・14 §5 / PR #1565 Codex R1 H5)
 *
 * 切替で古い入口を閉じた後 (段階 frozen 以降・段階が読めないときも)、product-hub は税率の手入力 (draft_yahoo.tax_rate) を使わない:
 *   詳細画面の表示・利益の試算・楽天の出品 (プレビュー・登録) は Company DB (core.skus.tax_rate) の税率だけ。閉じる前は呼ばない (今までどおり)。
 * ドラフトの商品コードが代表コード (バリエーションの親・SKU の行が無いことが多い) なら、構成の SKU (NE の写しの代表商品コードでまとまる子) の税率を読む。
 *   子の税率が混ざる・どれかが無い / 未解決 = 決められない (出品は止める)。Company DB を読めない = 決められない (fail-closed)
 * env: COMPANY_DB_URL (Render。無ければ COMPANY_DB_WATCH_URL)
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
      rows = (await client.query(`select x.code, k.sku_id is not null as found, k.tax_rate::text as tax_rate, k.tax_class
          from unnest($2::text[]) as x(code) left join core.skus k on k.company_id = $1 and k.code_norm = core.norm_code(x.code)`, [COMPANY_ID, list])).rows;
    } catch (e) {
      return { ok: false, reason: `Company DB を読めない (${String((e && e.message) || e).slice(0, 120)})` };
    } finally {
      if (client) { try { await client.end(); } catch { /* 結果は変えない */ } }
    }
  }
  const byCode = new Map();
  for (const r of rows) byCode.set(String(r.code), { found: r.found !== false, tax_rate: r.tax_rate == null ? null : Number(r.tax_rate), tax_class: r.tax_class ?? null });
  for (const c of list) if (!byCode.has(c)) byCode.set(c, { found: false, tax_rate: null, tax_class: null });
  return { ok: true, byCode };
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
  return { ...decideTax(codes, r.byCode), codes };
}

/** 互換 (1 つの商品コード) */
export async function readCdbTaxRate(code, { env = process.env } = {}) {
  const c = String(code ?? '').trim();
  const r = await readCdbTaxRates([c], { env });
  if (!r.ok) return { ok: false, reason: r.reason };
  return decideTax([c], r.byCode);
}
