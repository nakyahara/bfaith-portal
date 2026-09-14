/**
 * ne-shipments-transform.mjs — NE の伝票 (warehouse.db の raw_ne_order_base + raw_ne_orders) を
 * Company DB の受け皿 `core.apply_shipment_batch()` の header / lines (0013、08 §4.7) に整える。D5a
 *
 * ここは純粋関数だけ (DB も HTTP も触らない)。miniPC の送り手 (ne-shipments.mjs) と試験が同じ物を使う。
 *
 * 約束:
 *   - 内部 ID は送らない (sku_code = NE 商品コード。Render の apply_* が core.skus に解決する。08 §4.7)
 *   - 時刻: NE の 'YYYY-MM-DD HH:MM:SS' は JST → '+09:00' を付けて送る (Render 側の ship_date_jst = JST の日付 = 旧 f_shipments_daily の substr(出荷確定日,1,10) と一致)。
 *     raw の synced_at は UTC ('YYYY-MM-DD HH:MM:SS') → 'Z' を付ける。形が違えば例外 (黙って null にしない)
 *   - 空文字は null (NE の '' = 未設定)。qty (受注数) が無い明細は例外 (欠落を 0 にしない = 0013 R1 #5 と同じ)
 *   - content_hash = ヘッダの業務内容だけの指紋 (source_updated_at / transform_version / content_hash 自身は含めない)。
 *     明細の指紋は Render が受け取った配列から計算する (lines_checksum。送り側の値は使わない = 0013 R1 #1) ので、ここでは作らない
 *   - 明細は line_no の数値順に並べる (毎回同じ形で送る)
 */
import crypto from 'node:crypto';

export const TRANSFORM_VERSION = 'ne-shipments-1';

const DT_RE = /^(\d{4})-(\d{2})-(\d{2})(?:[ T](\d{2}):(\d{2})(?::(\d{2}))?)?$/;

function parseDt(s, label) {
  if (s == null) return null;
  const t = String(s).trim();
  if (t === '') return null;
  const m = DT_RE.exec(t);
  if (!m) throw new Error(`${label}の形が違う: "${t}" (YYYY-MM-DD[ HH:MM[:SS]] のはず)`);
  const [, y, mo, d, hh = '00', mm = '00', ss = '00'] = m;
  const naive = `${y}-${mo}-${d}T${hh}:${mm}:${ss}`;
  if (Number.isNaN(Date.parse(`${naive}Z`))) throw new Error(`${label}が日時として不正: "${t}"`);
  return naive;
}

/** NE の日時 (JST) → ISO 8601 (+09:00)。空・null → null */
export function jstToIso(s, label = 'NE の日時') {
  const naive = parseDt(s, label);
  return naive ? `${naive}+09:00` : null;
}
/** NE の日時 (JST) → 'YYYY-MM-DD' (JST の日付)。空・null → null */
export function jstDateOnly(s, label = 'NE の日付') {
  const naive = parseDt(s, label);
  return naive ? naive.slice(0, 10) : null;
}
/** raw の synced_at (UTC 'YYYY-MM-DD HH:MM:SS') → ISO 8601 (Z)。空・null → null */
export function utcToIso(s, label = 'synced_at') {
  const naive = parseDt(s, label);
  return naive ? `${naive}Z` : null;
}

const nz = (v) => { if (v == null) return null; const t = String(v).trim(); return t === '' ? null : t; };
function intOrNull(v, label) {
  if (v == null || v === '') return null;
  const n = typeof v === 'number' ? v : Number(String(v).trim());
  if (!Number.isInteger(n)) throw new Error(`${label} が整数でない: "${v}"`);
  return n;
}

/** 鍵順を固定した JSON (指紋用。ネストも並べ替える) */
export function canonicalJson(v) {
  if (v === null || typeof v !== 'object') return JSON.stringify(v);
  if (Array.isArray(v)) return `[${v.map(canonicalJson).join(',')}]`;
  return `{${Object.keys(v).sort().map((k) => `${JSON.stringify(k)}:${canonicalJson(v[k])}`).join(',')}}`;
}

/** ヘッダの業務内容だけの指紋 (source_updated_at / transform_version / content_hash は除く) */
export function contentHash(header) {
  const { source_updated_at, transform_version, content_hash, ...rest } = header;   // eslint-disable-line no-unused-vars
  return crypto.createHash('sha256').update(canonicalJson(rest)).digest('hex');
}

/**
 * 1 伝票を header / lines に整える。
 * @param {object} base   raw_ne_order_base の 1 行 (列名は日本語のまま)
 * @param {object[]} lines raw_ne_orders の行 (同じ伝票番号。無ければ [] = 明細がまだ取れていない伝票。Render 側は明細 0 件で持つ)
 * @param {{ fallbackSourceUpdatedAt?: string }} [opts] synced_at が 1 つも無いときに使う時刻 (ISO)。無ければ例外
 * @returns {{ ne_slip_no: string, header: object, lines: object[], source_updated_at: string|null, no_synced_at: boolean }}
 *   source_updated_at = raw の synced_at (UTC 'YYYY-MM-DD HH:MM:SS') の最大値 (カーソルの材料)
 */
export function buildShipment(base, lines = [], opts = {}) {
  const slip = nz(base.伝票番号);
  if (!slip) throw new Error('伝票番号が無い');
  const cancelled = base.キャンセル区分 === 'キャンセル';
  const trackingNo = nz(base.送り状番号);
  const header = {
    ne_order_no: nz(base.受注番号),
    shop_code: nz(base.店舗コード),
    ne_status_code: nz(base.受注状態区分),
    is_cancelled: cancelled,
    cancelled_at: jstToIso(base.受注キャンセル日, `伝票 ${slip} の受注キャンセル日`),
    shipped_at: jstToIso(base.出荷確定日, `伝票 ${slip} の出荷確定日`),
    order_date_jst: jstDateOnly(base.受注日, `伝票 ${slip} の受注日`),
    carrier: null,                                   // NE の受注ベースには無い (配送方法名で分かる)
    delivery_method_code: nz(base.配送方法ID),
    delivery_method_name: nz(base.配送方法名),
    tracking_no: trackingNo,
    tracking_source: trackingNo ? 'ne' : null,
    batch_code: null,                                // 出荷バッチは raw に無い
    synced_to_ne_at: null,
  };
  const seen = new Set();
  const outLines = [];
  for (const l of lines) {
    if (nz(l.伝票番号) !== slip) throw new Error(`伝票 ${slip} に別の伝票 ${l.伝票番号} の明細が混ざっている`);
    const lineNo = intOrNull(l.明細行番号, `伝票 ${slip} の明細行番号`);
    if (lineNo == null) throw new Error(`伝票 ${slip} に明細行番号の無い明細がある`);
    const key = String(lineNo);
    if (seen.has(key)) throw new Error(`伝票 ${slip} の明細行番号 ${key} が重複している`);
    seen.add(key);
    const qty = intOrNull(l.受注数, `伝票 ${slip} 明細 ${key} の受注数`);
    if (qty == null) throw new Error(`伝票 ${slip} 明細 ${key} の受注数が無い (欠落を 0 にしない)`);
    outLines.push({
      line_no: key,
      sku_code: nz(l.商品コード),
      qty,
      allocated_qty: intOrNull(l.引当数, `伝票 ${slip} 明細 ${key} の引当数`),
      is_cancelled: l.キャンセル区分 === 'キャンセル',
    });
  }
  outLines.sort((a, b) => Number(a.line_no) - Number(b.line_no));
  const syncedAts = [base.synced_at, ...lines.map((l) => l.synced_at)].map(nz).filter(Boolean).sort();
  const sourceUpdatedAt = syncedAts.length ? syncedAts[syncedAts.length - 1] : null;
  let sourceUpdatedIso = sourceUpdatedAt ? utcToIso(sourceUpdatedAt, `伝票 ${slip} の synced_at`) : null;
  if (!sourceUpdatedIso) {
    if (!opts.fallbackSourceUpdatedAt) throw new Error(`伝票 ${slip} に synced_at が 1 つも無い`);
    sourceUpdatedIso = opts.fallbackSourceUpdatedAt;
  }
  return {
    ne_slip_no: slip,
    header: { ...header, source_updated_at: sourceUpdatedIso, transform_version: TRANSFORM_VERSION, content_hash: contentHash(header) },
    lines: outLines,
    source_updated_at: sourceUpdatedAt,
    no_synced_at: !sourceUpdatedAt,
  };
}
