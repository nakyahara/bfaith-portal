/**
 * sku-map-canonical.js — Amazon SKU の対応 (親 m_sku_master + 構成 m_sku_components) の「決まった並べ方」とハッシュ
 * (Company DB構想 16 §7 v2 M7・§8 契約 v3 High 1。PR ⑦-0)
 *
 * なぜ: Company DB の写し (⑦-2)・miniPC の SQLite・Render の mirror の 3 か所で **同じ中身なら同じハッシュ** にする。
 *   送り手は世代 (generation) と一緒にハッシュを送り、受け手は届いた行から出し直して照らす。
 *   どの場所でもこのファイルの関数だけでハッシュを出す (自前で並べない)。
 *
 * 決まり (版 = 'sku-map-canon-v1'。1 つでも変えるときは版を上げる。試験 scripts/test-sku-map-canonical.mjs の固定のハッシュが落ちる):
 *   - 数える列 (この順):
 *       親   (master)    = seller_sku, name (商品名), created_at, updated_at
 *       構成 (component) = seller_sku, ne_code, quantity (数量), sort_order, created_at, updated_at
 *     数えない列: created_by / updated_by (Render に無い・読み手がいない)・synced_at など受け手の時刻・mirror の source (いつも 'master')・
 *     mirror_sku_resolved の 商品名 / source_updated_at (親から写しただけの列。受け手は親と同じかを別に確かめる)
 *   - 1 行 = 列の値を上の順に並べた JSON の配列 (JSON.stringify)。
 *       NULL = null (列が無い = undefined は投げる) / 文字 = JSON の文字列 (文字は変えない。NFC にしない・前後の空白も削らない) /
 *       数 = 安全な整数だけ (10 進・先頭 0 なし・指数なし。-0 は 0) / 時刻 = 'YYYY-MM-DDTHH:MM:SS.sssZ' (UTC・ミリ秒) の文字列だけ (ほかの形は投げる)
 *   - 行の並び: 親 = seller_sku の UTF-8 のバイト順 / 構成 = (seller_sku, ne_code) の UTF-8 のバイト順。
 *     同じ鍵が 2 行あれば投げる (DB の照合順や JS の UTF-16 の順には頼らない)
 *   - 全体 = 'sku-map-canon-v1\n' + 'master <行数>\n' + 親の行 + 'components <行数>\n' + 構成の行 (どの行も後ろに '\n')。
 *     UTF-8 のバイトで sha256 → 16 進の小文字 64 文字
 *
 * 受ける決まり (validateSkuMap。Render の受け手が世代つきの対に使う。⑦-2 の送り手・写しも送る前に同じ関数で確かめる):
 *   0 件の世代は無い / 鍵は小文字・前後の空白なし・制御文字なし / 名前は空でない / 時刻は上の形 / 数量 > 0 /
 *   構成の SKU は親にある / 親には構成が 1 行以上 / sort_order は SKU ごとに 0..N-1 (隙間・重なりなし)
 *   🚨「空白」は SKU_MAP_EDGE_SPACE_CHARS (JS の trim が削る文字と同じ集合を固定で書いたもの = TAB・改行・NBSP・全角の空白 U+3000・BOM など)。
 *     miniPC (SQLite) と Company DB (PostgreSQL) の CHECK の trim / btrim は U+0020 しか削らないので、ここは **それより厳しい**
 *     (前後に全角の空白や TAB がある鍵は、SQLite には入るがここでは断る)。緩めない: SQL の LOWER(TRIM()) では外れ、
 *     JS の normSku では当たる鍵 = 読み手によって結びつきが変わる鍵を、写しの影運転 (⑦-2) で数えて先に直すため。
 *     鍵 = core.norm_code(鍵) までは求めない (中の空白・全角の文字は通す。正規化で重なる鍵は切替前の片付け = 16 §3 #4)
 */
import crypto from 'node:crypto';

export const SKU_MAP_CANON_FORMAT = 'sku-map-canon-v1';
export const SKU_MAP_MASTER_COLUMNS = Object.freeze(['seller_sku', 'name', 'created_at', 'updated_at']);
export const SKU_MAP_COMPONENT_COLUMNS = Object.freeze(['seller_sku', 'ne_code', 'quantity', 'sort_order', 'created_at', 'updated_at']);
/** 列の種類 (key = 鍵の文字 / text = 文字 / int = 整数 / ts = 時刻) */
const COLUMN_KIND = Object.freeze({
  seller_sku: 'key', ne_code: 'key', name: 'text', quantity: 'int', sort_order: 'int', created_at: 'ts', updated_at: 'ts',
});
export const SKU_MAP_HASH_RE = /^[0-9a-f]{64}$/;
/** 決まった時刻の形 (UTC・ミリ秒・Z) */
export const CANON_TS_RE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;
/** 世代の上限 = PostgreSQL の bigint / SQLite の INTEGER の上限 */
export const SKU_MAP_GENERATION_MAX = 9223372036854775807n;
/** 鍵 (seller_sku / ne_code) の長さの上限 */
const KEY_MAX_LEN = 255;
/**
 * 鍵の前後に置けない空白 (名前は「これだけ」なら空とみなす)。ECMAScript の WhiteSpace + LineTerminator
 * (= String.prototype.trim が削る文字) を固定で書く = 実行環境の Unicode の版で決まりが動かない。
 * TAB U+0009・LF U+000A・VT U+000B・FF U+000C・CR U+000D・空白 U+0020・NBSP U+00A0・U+1680・U+2000〜U+200A・
 * LS U+2028・PS U+2029・U+202F・U+205F・全角の空白 U+3000・BOM U+FEFF
 */
export const SKU_MAP_EDGE_SPACE_CHARS = '\u0009\u000a\u000b\u000c\u000d \u00a0\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a\u2028\u2029\u202f\u205f\u3000\ufeff';
const EDGE_SPACE_SET = new Set(SKU_MAP_EDGE_SPACE_CHARS);

export class SkuMapCanonError extends Error {
  constructor(message, code = 'SKU_MAP_CANON_INVALID') { super(message); this.code = code; }
}

// ─── 世代 (generation) ───

/**
 * 世代を bigint にする。受けるのは 10 進の文字列 ('1'..'9223372036854775807'・先頭 0 なし・符号なし) か、
 * 1 以上の安全な整数 (JSON の数)。それ以外 (0・負・小数・'01'・'1e3'・上限超え・真偽など) は null。
 * 🚨 比べるときは必ずこの bigint で (文字列で比べると '10' < '9' になる)
 * @returns {bigint|null}
 */
export function parseSkuMapGeneration(v) {
  let b = null;
  if (typeof v === 'number') {
    if (!Number.isSafeInteger(v) || v <= 0) return null;
    b = BigInt(v);
  } else if (typeof v === 'bigint') {
    b = v;
  } else if (typeof v === 'string') {
    if (!/^[1-9][0-9]{0,18}$/.test(v)) return null;
    b = BigInt(v);
  } else {
    return null;
  }
  return b > 0n && b <= SKU_MAP_GENERATION_MAX ? b : null;
}

/** 世代を送り・応答に書く形 (10 進の文字列。JSON の数だと 2^53 を超えると桁が落ちる) */
export function formatSkuMapGeneration(b) {
  const g = parseSkuMapGeneration(b);
  if (g === null) throw new SkuMapCanonError(`世代の形がおかしい: ${String(b)}`, 'SKU_MAP_BAD_GENERATION');
  return g.toString();
}

// ─── 時刻 ───

const DAYS_IN_MONTH = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

/**
 * 決まった形 ('YYYY-MM-DDTHH:MM:SS.sssZ') で、しかも暦の上で正しい時刻か。
 * `new Date(v).toISOString() === v` と同じ答え (試験 [6b] が網の目で照らす)。1 行に 2 回・読み直しでもう 1 回呼ぶので、
 * Date を作らずに数で確かめる (20k 親 / 40k 構成で Date を作ると 1 回 200ms かかった。PR ⑦-0 仮レビュー Low-5)
 */
export function isCanonicalTimestamp(v) {
  if (typeof v !== 'string' || !CANON_TS_RE.test(v)) return false;
  const y = +v.slice(0, 4), mo = +v.slice(5, 7), d = +v.slice(8, 10);
  if (mo < 1 || mo > 12 || d < 1 || +v.slice(11, 13) > 23 || +v.slice(14, 16) > 59 || +v.slice(17, 19) > 59) return false;
  const leap = (y % 4 === 0 && y % 100 !== 0) || y % 400 === 0;
  return d <= (mo === 2 && leap ? 29 : DAYS_IN_MONTH[mo - 1]);
}

const TS_IN_RE = /^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(Z|z|[+-]\d{2}(?::?\d{2})?)$/;

/**
 * 時刻を決まった形 (UTC・ミリ秒・Z) にする。送り手・写しが行を作るときに使う (受け手は変えずに形だけ確かめる)。
 * 受ける: Date / 時差つきの文字列 ('...Z'・'+09:00'・'+0900'・'+09'。'T' の代わりに空白も可 = PostgreSQL の timestamptz の文字の形)。
 * ミリ秒より細かい桁は切り捨て (PostgreSQL の to_char の MS と同じ)。
 * 時差の無い文字列 ('2026-10-01 09:00:00') は UTC か JST か分からないので投げる (推測しない)。この PC の時差 (TZ) は使わない。
 */
export function toCanonicalTimestamp(v) {
  if (v instanceof Date) {
    if (Number.isNaN(v.getTime())) throw new SkuMapCanonError('時刻が Invalid Date', 'SKU_MAP_BAD_TIMESTAMP');
    const s = v.toISOString();
    if (!CANON_TS_RE.test(s)) throw new SkuMapCanonError(`時刻が範囲の外: ${s}`, 'SKU_MAP_BAD_TIMESTAMP');
    return s;
  }
  if (typeof v !== 'string') throw new SkuMapCanonError(`時刻が文字でも Date でもない: ${typeof v}`, 'SKU_MAP_BAD_TIMESTAMP');
  const m = TS_IN_RE.exec(v);
  if (!m) throw new SkuMapCanonError(`時刻の形が分からない (時差が無い・形が違う): ${v}`, 'SKU_MAP_BAD_TIMESTAMP');
  const [, y, mo, d, h, mi, s, frac = '', zone] = m;
  const ms = Number((frac + '000').slice(0, 3));
  // 暦の上で正しいか (2/30・25 時などを弾く)。時差を引く前の値で確かめる
  const local = Date.UTC(+y, +mo - 1, +d, +h, +mi, +s, ms);
  const back = new Date(local);
  if (Number.isNaN(local) || back.getUTCFullYear() !== +y || back.getUTCMonth() !== +mo - 1 || back.getUTCDate() !== +d
    || back.getUTCHours() !== +h || back.getUTCMinutes() !== +mi || back.getUTCSeconds() !== +s) {
    throw new SkuMapCanonError(`暦に無い時刻: ${v}`, 'SKU_MAP_BAD_TIMESTAMP');
  }
  let offsetMin = 0;
  if (zone !== 'Z' && zone !== 'z') {
    const zm = /^([+-])(\d{2})(?::?(\d{2}))?$/.exec(zone);
    const zh = +zm[2], zmin = +(zm[3] || 0);
    if (zh > 23 || zmin > 59) throw new SkuMapCanonError(`時差がおかしい: ${v}`, 'SKU_MAP_BAD_TIMESTAMP');
    offsetMin = (zm[1] === '-' ? -1 : 1) * (zh * 60 + zmin);
  }
  const out = new Date(local - offsetMin * 60000).toISOString();
  if (!CANON_TS_RE.test(out)) throw new SkuMapCanonError(`時刻が範囲の外: ${v}`, 'SKU_MAP_BAD_TIMESTAMP');
  return out;
}

// ─── 1 つの値・1 行 ───

function encodeValue(kind, v, where) {
  if (v === undefined) throw new SkuMapCanonError(`${where}: 列が無い (null と区別する)`, 'SKU_MAP_MISSING_COLUMN');
  if (kind === 'key') {
    if (typeof v !== 'string') throw new SkuMapCanonError(`${where}: 鍵が文字でない`, 'SKU_MAP_BAD_VALUE');
    if (!v.isWellFormed()) throw new SkuMapCanonError(`${where}: 対になっていないサロゲート`, 'SKU_MAP_BAD_VALUE');
    return JSON.stringify(v);
  }
  if (v === null) return 'null';
  if (kind === 'text') {
    if (typeof v !== 'string') throw new SkuMapCanonError(`${where}: 文字でない`, 'SKU_MAP_BAD_VALUE');
    if (!v.isWellFormed()) throw new SkuMapCanonError(`${where}: 対になっていないサロゲート`, 'SKU_MAP_BAD_VALUE');
    return JSON.stringify(v);
  }
  if (kind === 'int') {
    let n = v;
    if (typeof n === 'bigint') {
      if (n > BigInt(Number.MAX_SAFE_INTEGER) || n < BigInt(Number.MIN_SAFE_INTEGER)) throw new SkuMapCanonError(`${where}: 整数が大きすぎる`, 'SKU_MAP_BAD_VALUE');
      n = Number(n);
    }
    if (typeof n !== 'number' || !Number.isSafeInteger(n)) throw new SkuMapCanonError(`${where}: 安全な整数でない (${JSON.stringify(v)})`, 'SKU_MAP_BAD_VALUE');
    return String(n === 0 ? 0 : n);   // -0 は 0
  }
  if (kind === 'ts') {
    if (!isCanonicalTimestamp(v)) throw new SkuMapCanonError(`${where}: 時刻が決まった形 (YYYY-MM-DDTHH:MM:SS.sssZ) でない: ${JSON.stringify(v)}`, 'SKU_MAP_BAD_TIMESTAMP');
    return JSON.stringify(v);
  }
  throw new SkuMapCanonError(`列の種類が分からない: ${kind}`);
}

function encodeRow(columns, row, where) {
  if (!row || typeof row !== 'object' || Array.isArray(row)) throw new SkuMapCanonError(`${where}: 行がオブジェクトでない`, 'SKU_MAP_BAD_VALUE');
  return `[${columns.map((c) => encodeValue(COLUMN_KIND[c], row[c], `${where}.${c}`)).join(',')}]`;
}

/**
 * 行を鍵の UTF-8 のバイト順に並べる (JS の < は UTF-16 の順なので、U+E000 以上と絵文字などの順が逆になる)。
 * 鍵のバイト列は行ごとに 1 回だけ作る (比べるたびに作ると 40k 行で倍の時間。PR ⑦-0 仮レビュー Low-5)
 */
function sortedLines(kindName, columns, keyCols, rows) {
  if (!Array.isArray(rows)) throw new SkuMapCanonError(`${kindName} が配列でない`, 'SKU_MAP_BAD_VALUE');
  const items = rows.map((r, i) => {
    const line = encodeRow(columns, r, `${kindName}[${i}]`);   // 鍵が文字でなければここで投げる
    return { keys: keyCols.map((k) => r[k]), bytes: keyCols.map((k) => Buffer.from(r[k], 'utf8')), line, i };
  });
  items.sort((x, y) => {
    for (let k = 0; k < keyCols.length; k++) {
      const c = Buffer.compare(x.bytes[k], y.bytes[k]);
      if (c !== 0) return c;
    }
    return 0;
  });
  for (let j = 1; j < items.length; j++) {
    if (keyCols.every((_, k) => items[j].keys[k] === items[j - 1].keys[k])) {
      throw new SkuMapCanonError(`${kindName}: 同じ鍵が 2 行 (${JSON.stringify(items[j].keys)})`, 'SKU_MAP_DUPLICATE_KEY');
    }
  }
  return items.map((x) => x.line);
}

// ─── 全体 ───

/**
 * 決まった並べ方の文字列。形が 1 つでも違えば SkuMapCanonError を投げる (黙って直さない)。
 * @param {{ master: object[], components: object[] }} canon 決まった形の行 (列名は SKU_MAP_*_COLUMNS)
 */
export function serializeSkuMap({ master, components }) {
  const m = sortedLines('master', SKU_MAP_MASTER_COLUMNS, ['seller_sku'], master);
  const c = sortedLines('components', SKU_MAP_COMPONENT_COLUMNS, ['seller_sku', 'ne_code'], components);
  let out = `${SKU_MAP_CANON_FORMAT}\nmaster ${m.length}\n`;
  for (const line of m) out += `${line}\n`;
  out += `components ${c.length}\n`;
  for (const line of c) out += `${line}\n`;
  return out;
}

/** 決まった並べ方のハッシュと行数 */
export function skuMapDigest(canon) {
  const text = serializeSkuMap(canon);
  return {
    format: SKU_MAP_CANON_FORMAT,
    content_hash: crypto.createHash('sha256').update(text, 'utf8').digest('hex'),
    master_rows: canon.master.length,
    component_rows: canon.components.length,
  };
}

/**
 * 送る世代の印 (送り手・写しが使う。/api/sync の body.sku_map_generation の形)
 * @param {bigint|string|number} generation 1 以上の整数
 */
export function buildSkuMapGeneration({ generation, master, components }) {
  const d = skuMapDigest({ master, components });
  return { format: d.format, generation: formatSkuMapGeneration(generation), content_hash: d.content_hash, master_rows: d.master_rows, component_rows: d.component_rows };
}

// ─── 受ける決まり ───

/**
 * 鍵 (seller_sku / ne_code) の問題 (無ければ null)。⑦-2 の影運転が「受け手で断られる行」を数えるのにも使う。
 * 大文字は ASCII の A-Z だけ見る (SQLite の lower() と同じ範囲。全角の Ａ などは通す)
 */
export function skuMapKeyProblem(v) {
  if (typeof v !== 'string') return '文字でない';
  if (v.length === 0) return '空';
  if (v.length > KEY_MAX_LEN) return `${KEY_MAX_LEN} 文字を超える`;
  if (!v.isWellFormed()) return '対になっていないサロゲート';
  if (/[\u0000-\u001f\u007f]/.test(v)) return '制御文字を含む';
  if (EDGE_SPACE_SET.has(v[0]) || EDGE_SPACE_SET.has(v[v.length - 1])) return '前後に空白 (全角の空白・NBSP・BOM も)';
  if (/[A-Z]/.test(v)) return '大文字を含む (小文字にそろえる)';
  return null;
}

/** 空白 (SKU_MAP_EDGE_SPACE_CHARS) だけ・空の文字か */
function onlySpaces(v) {
  for (const ch of v) if (!EDGE_SPACE_SET.has(ch)) return false;
  return true;
}

/**
 * 受ける決まりを確かめる。問題の一覧を返す (空 = 受けてよい)。多いときは先頭 limit 件だけ。
 * @returns {{ where: string, problem: string }[]}
 */
export function validateSkuMap({ master, components }, { limit = 20 } = {}) {
  const issues = [];
  const add = (where, problem) => { if (issues.length < limit) issues.push({ where, problem }); };
  if (!Array.isArray(master) || !Array.isArray(components)) { add('*', '親と構成の両方が配列であること'); return issues; }
  if (master.length === 0) add('master', '0 件 (0 件の世代は入れない)');
  if (components.length === 0) add('components', '0 件 (0 件の世代は入れない)');
  const masterKeys = new Set();
  master.forEach((r, i) => {
    const w = `master[${i}]`;
    if (!r || typeof r !== 'object' || Array.isArray(r)) { add(w, '行がオブジェクトでない'); return; }
    const kp = skuMapKeyProblem(r.seller_sku);
    if (kp) add(`${w}.seller_sku`, kp);
    else if (masterKeys.has(r.seller_sku)) add(`${w}.seller_sku`, `同じ SKU が 2 行: ${r.seller_sku}`);
    else masterKeys.add(r.seller_sku);
    if (typeof r.name !== 'string' || onlySpaces(r.name)) add(`${w}.name`, '名前が空 (空白だけも)');
    else if (!r.name.isWellFormed()) add(`${w}.name`, '対になっていないサロゲート');
    else if (r.name.includes('\u0000')) add(`${w}.name`, 'NUL を含む');
    for (const c of ['created_at', 'updated_at']) if (!isCanonicalTimestamp(r[c])) add(`${w}.${c}`, `時刻が決まった形でない: ${JSON.stringify(r[c] ?? null)}`);
  });
  const compKeys = new Set();
  const ordersBySku = new Map();
  components.forEach((r, i) => {
    const w = `components[${i}]`;
    if (!r || typeof r !== 'object' || Array.isArray(r)) { add(w, '行がオブジェクトでない'); return; }
    const kp = skuMapKeyProblem(r.seller_sku);
    const np = skuMapKeyProblem(r.ne_code);
    if (kp) add(`${w}.seller_sku`, kp);
    if (np) add(`${w}.ne_code`, np);
    if (!kp && !np) {
      const k = JSON.stringify([r.seller_sku, r.ne_code]);
      if (compKeys.has(k)) add(w, `同じ (SKU, NE コード) が 2 行: ${r.seller_sku} / ${r.ne_code}`);
      compKeys.add(k);
      if (!masterKeys.has(r.seller_sku)) add(`${w}.seller_sku`, `親に無い SKU: ${r.seller_sku}`);
    }
    if (!Number.isSafeInteger(r.quantity) || r.quantity <= 0) add(`${w}.quantity`, `数量が 1 以上の整数でない: ${JSON.stringify(r.quantity ?? null)}`);
    if (!Number.isSafeInteger(r.sort_order) || r.sort_order < 0) add(`${w}.sort_order`, `並びが 0 以上の整数でない: ${JSON.stringify(r.sort_order ?? null)}`);
    else if (!kp) {
      if (!ordersBySku.has(r.seller_sku)) ordersBySku.set(r.seller_sku, []);
      ordersBySku.get(r.seller_sku).push(r.sort_order);
    }
    for (const c of ['created_at', 'updated_at']) if (!isCanonicalTimestamp(r[c])) add(`${w}.${c}`, `時刻が決まった形でない: ${JSON.stringify(r[c] ?? null)}`);
  });
  for (const sku of masterKeys) {
    const orders = ordersBySku.get(sku);
    if (!orders) { add(`master[${JSON.stringify(sku)}]`, '構成が 0 行'); continue; }
    const sorted = [...orders].sort((a, b) => a - b);
    if (sorted.some((v, j) => v !== j)) add(`components[${JSON.stringify(sku)}]`, `並び (sort_order) が 0..${orders.length - 1} でない: ${JSON.stringify(sorted)}`);
  }
  return issues;
}

// ─── 形の変換 ───

/** miniPC の m_sku_master / m_sku_components を SELECT した行 → 決まった形の行 */
export function fromMiniPcRows({ masterRows, componentRows }) {
  return {
    master: (masterRows || []).map((r) => ({ seller_sku: r.seller_sku, name: r.商品名, created_at: r.created_at, updated_at: r.updated_at })),
    components: (componentRows || []).map((r) => ({
      seller_sku: r.seller_sku, ne_code: r.ne_code, quantity: r.数量, sort_order: r.sort_order, created_at: r.created_at, updated_at: r.updated_at,
    })),
  };
}

/**
 * 決まった形の行 → Render に送る形 (/api/sync の sku_master / sku_resolved)。
 * sku_resolved の 商品名 / source_updated_at は親から写す (今の送り方と同じ)。構成の時刻は component_created_at / component_updated_at
 */
export function toMirrorWireRows({ master, components }) {
  const byMaster = new Map(master.map((m) => [m.seller_sku, m]));
  return {
    sku_master: master.map((m) => ({ seller_sku: m.seller_sku, 商品名: m.name, source_created_at: m.created_at, source_updated_at: m.updated_at })),
    sku_resolved: components.map((c) => {
      const m = byMaster.get(c.seller_sku);
      return {
        seller_sku: c.seller_sku, ne_code: c.ne_code, quantity: c.quantity, source: 'master',
        商品名: m ? m.name : null, source_updated_at: m ? m.updated_at : null, sort_order: c.sort_order,
        component_created_at: c.created_at, component_updated_at: c.updated_at,
      };
    }),
  };
}

/**
 * Render に届いた形 (sku_master / sku_resolved の行) → 決まった形の行 + 問題の一覧。
 * sku_resolved の 商品名 / source_updated_at / source は親から写しただけの列なので、あれば親と同じか (source は 'master' か) を確かめる
 * (ハッシュに入れない列を黙って受けない)。数量は quantity だけ (古い送り手の 数量 は世代つきでは受けない)
 */
export function fromMirrorWireRows({ sku_master, sku_resolved }, { limit = 20 } = {}) {
  const issues = [];
  const add = (where, problem) => { if (issues.length < limit) issues.push({ where, problem }); };
  const plain = (r) => r && typeof r === 'object' && !Array.isArray(r);
  const master = (sku_master || []).map((r) => (plain(r)
    ? { seller_sku: r.seller_sku, name: r.商品名, created_at: r.source_created_at, updated_at: r.source_updated_at }
    : r));
  const byMaster = new Map();
  for (const m of master) if (plain(m) && typeof m.seller_sku === 'string' && !byMaster.has(m.seller_sku)) byMaster.set(m.seller_sku, m);
  const components = (sku_resolved || []).map((r, i) => {
    if (!plain(r)) return r;
    const m = byMaster.get(r.seller_sku);
    if (r.source !== undefined && r.source !== 'master') add(`sku_resolved[${i}].source`, `'master' でない: ${JSON.stringify(r.source)}`);
    if (m && r.商品名 !== undefined && r.商品名 !== m.name) add(`sku_resolved[${i}].商品名`, '親 (sku_master) の商品名と違う');
    if (m && r.source_updated_at !== undefined && r.source_updated_at !== m.updated_at) add(`sku_resolved[${i}].source_updated_at`, '親 (sku_master) の source_updated_at と違う');
    return {
      seller_sku: r.seller_sku, ne_code: r.ne_code, quantity: r.quantity, sort_order: r.sort_order,
      created_at: r.component_created_at, updated_at: r.component_updated_at,
    };
  });
  return { master, components, issues };
}
