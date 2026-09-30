/**
 * cdb-tax-rate.mjs — Company DB の税率を読む (product-hub の詳細画面・Company DB構想 10 §4 #6・14 §5)
 *
 * 切替で古い入口を閉じた後 (段階 frozen 以降)、product-hub の税率の欄 (draft_yahoo.tax_rate の手入力) は外し、
 * Company DB (core.skus.tax_rate) の税率を見せるだけにする。閉じる前は呼ばない (今までどおり)。
 * 読めない (接続先が無い・つながらない・商品が無い) = { ok: false, reason } (画面は「読めません」と出す。書き込みは門が止めている)
 * env: COMPANY_DB_URL (Render。無ければ COMPANY_DB_WATCH_URL)
 */
let reader = null;
/** 試験用: 読み方を差し替える (本番では呼ばない) */
export function __setCdbTaxReader(fn) { reader = fn || null; }

export function formatTaxRate(rate) {
  const n = Number(rate);
  if (!Number.isFinite(n) || n <= 0) return null;
  return `${Math.round(n * 100)}%`;
}

export async function readCdbTaxRate(code, { env = process.env } = {}) {
  const c = String(code ?? '').trim();
  if (!c) return { ok: false, reason: '商品コードが無い' };
  if (reader) return reader(c);
  const url = String(env.COMPANY_DB_URL || '').trim() || String(env.COMPANY_DB_WATCH_URL || '').trim();
  if (!url) return { ok: false, reason: 'Company DB の接続先が無い' };
  let client = null;
  try {
    const { openPgClient } = await import('../../../scripts/company-db/migrate.mjs');
    const { COMPANY_ID } = await import('../../../lib/master-write.mjs');
    client = await openPgClient(url, { application_name: 'product-hub-cdb-tax', connectionTimeoutMillis: 5000, statement_timeout: 5000, query_timeout: 6000 });
    const r = (await client.query('select tax_rate::text as tax_rate, tax_class from core.skus where company_id = $1 and code_norm = core.norm_code($2)', [COMPANY_ID, c])).rows[0];
    if (!r) return { ok: false, reason: 'Company DB にこの商品コードが無い' };
    const label = formatTaxRate(r.tax_rate);
    return label ? { ok: true, label, tax_class: r.tax_class } : { ok: false, reason: `Company DB の税率が未解決 (${r.tax_class || '空'})` };
  } catch (e) {
    return { ok: false, reason: `Company DB を読めない (${String((e && e.message) || e).slice(0, 120)})` };
  } finally {
    if (client) { try { await client.end(); } catch { /* 結果は変えない */ } }
  }
}
