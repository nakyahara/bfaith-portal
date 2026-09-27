/**
 * ne-codes.js — 入荷予定の貼り付け (ロジザード) の商品ID の大文字・小文字を、Company DB の NE の元の書き方で補う
 *   (マスタ正本切替 M6。設計 = AI_reference CompanyDB構想/10 §6.2「M6」契約 v1〜v3。Codex M6-R0・R1)
 *
 * 書き方の優先 (v2):
 *   1. po_product_code_canonical (ロジザードの在庫 CSV・NE の CSV の取込で見た書き方。今のまま最優先)
 *   2. Company DB ops.master_ne_codes の kind = product・state = ok (毎朝の照合が NE の取得の世代から書く。canonical が無いときだけ)
 *   3. 経路ごとの今の予備 (PML のコード → 対応表・PO 明細・仮コードの入力値)
 * 警告は Company DB の状態だけで決める (v3 M3): collided / invalid は canonical があっても出す・ne_api_differs = ok で canonical と違う。
 * 🚨 collided (NE に大文字・小文字だけ違うコードが 2 つ) は、canonical があっても貼り付けから外す (v3 H1 = 別の商品に入荷するおそれ)。
 * Company DB が読めない (未設定・時間切れ・失敗・印なし) = 今の動きのまま (変換は止めない)。読むのは 3 秒まで (つなぎかけの接続も捨てる)
 */
import { pgClientOptions } from '../../scripts/company-db/migrate.mjs';

export const NE_CODES_DEADLINE_MS = 3000;
export const NE_CODES_STALE_DAYS = 7;
const DAY_MS = 24 * 3600 * 1000;

/** 印と元の書き方を 1 つの読み取りの取引で読む (0041 は全部の入れ替えと印を同じ取引で書く = 世代がずれない)。db.query(sql) → { rows } */
export async function loadNeCodes(db) {
  await db.query('begin transaction isolation level repeatable read read only');
  try {
    const mark = (await db.query('select compare_run_id, observed_at from ops.master_ne_code_mark where id = 1')).rows[0] ?? null;
    const rows = mark ? (await db.query("select code_norm, state, ne_code from ops.master_ne_codes where kind = 'product'")).rows : [];
    await db.query('commit');
    return { mark, rows };
  } catch (e) {
    try { await db.query('rollback'); } catch { /* 接続が切れていれば何もしない */ }
    throw e;
  }
}

/** 接続を待たずに捨てる (つなぎかけでも)。失敗は無視 */
function discard(client) {
  if (!client) return;
  try { const s = client.connection && client.connection.stream; if (s && typeof s.destroy === 'function') s.destroy(); } catch { /* */ }
  try { const p = client.end(); if (p && typeof p.catch === 'function') p.catch(() => {}); } catch { /* */ }
}

/**
 * Company DB の元の書き方を読む。全体の期限 (接続・文・後始末を含む) を過ぎたら、接続を捨てて「読めない」で返す。
 * @param {object} [o]
 * @param {string} [o.url]  COMPANY_DB_URL (無い = not_configured)
 * @param {() => Promise<{ query: Function, end: Function }>} [o.connect]  試験用 (接続の作り方を差し替える)
 * @returns {Promise<{ ok: boolean, reason: string|null, mark?: { compare_run_id, observed_at }, stale?: boolean, map?: Map<string, { state, ne_code }> }>}
 */
export async function readNeCodes({ url = process.env.COMPANY_DB_URL, connect = null, deadlineMs = NE_CODES_DEADLINE_MS, nowMs = Date.now() } = {}) {
  if (!url && !connect) return { ok: false, reason: 'not_configured' };
  let client = null, timer = null, timedOut = false;
  const work = (async () => {
    if (connect) { client = await connect(); if (timedOut) { discard(client); throw new Error('timeout'); } }
    else {
      const { default: pg } = await import('pg');
      const base = pgClientOptions(url);
      // pg の時間切れは予備 (期限の 2 倍)。先に効くのはこちらの全体の期限 (接続を捨てる)
      client = new pg.Client({ ...base, application_name: 'po-ne-codes', connectionTimeoutMillis: deadlineMs * 2, statement_timeout: deadlineMs, query_timeout: deadlineMs * 2,
        connectionString: base.connectionString, ssl: base.ssl });
      if (timedOut) { discard(client); throw new Error('timeout'); }
      await client.connect();
    }
    return loadNeCodes({ query: (sql, p) => client.query(sql, p) });
  })();
  const deadline = new Promise((resolve) => { timer = setTimeout(() => { timedOut = true; resolve({ timeout: true }); }, deadlineMs); });
  let r;
  try {
    r = await Promise.race([work.then((v) => ({ v }), (e) => ({ e })), deadline]);
  } finally {
    clearTimeout(timer);
    discard(client);   // 期限切れでも成功でも、後始末は待たない
  }
  if (r.timeout) return { ok: false, reason: 'timeout' };
  if (r.e) return { ok: false, reason: 'error', error: String(r.e && r.e.message).slice(0, 200) };
  const { mark, rows } = r.v;
  if (!mark) return { ok: false, reason: 'no_mark' };
  const observed = new Date(mark.observed_at);
  const map = new Map();
  for (const x of rows) map.set(String(x.code_norm), { state: x.state, ne_code: x.ne_code ?? null });
  return { ok: true, reason: null, mark: { compare_run_id: mark.compare_run_id, observed_at: observed.toISOString() },
    stale: !(nowMs - observed.getTime() <= NE_CODES_STALE_DAYS * DAY_MS), map };
}

let readerOverride = null;
/** 試験用: 読み手を差し替える (null で元に戻す) */
export function setNeCodeReaderForTest(fn) { readerOverride = fn; }
/** 要求ごとに読む (差し替えがあればそれ)。失敗しても投げない */
export async function readNeCodesForRequest() {
  try { return await (readerOverride ? readerOverride() : readNeCodes()); } catch (e) { return { ok: false, reason: 'error', error: String(e && e.message).slice(0, 200) }; }
}

/**
 * 1 行の商品ID を決める (純粋)。key = 発注アプリの鍵 (normProductCode = trim + 小文字)
 * @returns {{ productCode, caseSource: 'canonical'|'ne_api'|'fallback', caseVerified: boolean, caseWarning: string|null, pasteBlocked: string|null, neCode: string|null }}
 */
export function resolveLzCode({ key, canonical = null, fallback, ne = null }) {
  const e = ne && ne.ok && ne.map ? ne.map.get(key) : undefined;
  // 元の書き方を小文字にしたものが鍵と同じときだけ使う (別の商品の書き方を使わない)
  const neOk = e && e.state === 'ok' && typeof e.ne_code === 'string' && e.ne_code.toLowerCase() === key ? e.ne_code : null;
  let caseWarning = null, pasteBlocked = null;
  if (e && e.state === 'collided') { caseWarning = 'ne_code_collided'; pasteBlocked = 'ne_code_collided'; }
  else if (e && (e.state === 'invalid' || (e.state === 'ok' && !neOk))) caseWarning = 'ne_code_invalid';
  else if (neOk && canonical && canonical !== neOk) caseWarning = 'ne_api_differs';
  const caseSource = canonical ? 'canonical' : neOk ? 'ne_api' : 'fallback';
  return { productCode: canonical || neOk || fallback, caseSource, caseVerified: caseSource !== 'fallback', caseWarning, pasteBlocked, neCode: e ? (e.ne_code ?? null) : null };
}

/** 応答に載せる Company DB の読み取りの様子 (map は載せない) */
export function neCodesStatus(ne) {
  if (!ne) return { ok: false, reason: 'not_read' };
  return { ok: !!ne.ok, reason: ne.reason ?? null, ...(ne.mark ? { mark: ne.mark } : {}), ...(ne.ok ? { stale: !!ne.stale } : {}) };
}
