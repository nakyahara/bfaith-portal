/**
 * yahoo-edit-item-probe.js — editItem 検証の部品 (Yahoo にも VPS にも接続しない)
 *
 * smoke-yahoo-edit-item.js から切り出した。
 * ここが間違っていると「消えた項目を見落とす」= 検証そのものが嘘になるので、単体でテストする。
 */
import { parseStringPromise } from 'xml2js';

/** 検証用の商品コードの接頭辞 */
export const TEST_CODE_PREFIX = 'zz-';
/**
 * 検証用商品の目印。商品名にこれが入っていないと動かさない。
 * ★接頭辞だけだと「たまたま zz- で始まる本番商品」を触りうる。
 *   目印は商品そのものに付いているので、コードの取り違えでは通らない。
 */
export const TEST_NAME_MARKER = 'zz検証用';

/**
 * 検証用の商品コードか。問題なければ null、駄目なら理由の文字列を返す。
 */
export function guardTestCode(itemCode) {
  const code = String(itemCode || '').trim();
  if (!code) return '商品コードを指定してください (例: zz-yahoo-m0-0901)';
  if (!code.toLowerCase().startsWith(TEST_CODE_PREFIX)) {
    return `安全のため "${TEST_CODE_PREFIX}" で始まる検証用商品でしか動かしません (指定: ${code})`;
  }
  return null;
}

/**
 * 取ってきた商品が「捨ててよい検証用商品」か。問題なければ null、駄目なら理由。
 * ★商品名に目印が入っていることを、書き込む前に実物で確かめる。
 * @param {Map<string,string>} flat flattenXml の戻り
 */
export function guardTestItem(flat) {
  const names = [...flat.entries()]
    .filter(([k]) => /(^|\/)Name\[\d+\]$/.test(k))
    .map(([, v]) => v);
  if (names.length === 0) return '商品名を読めませんでした。検証用商品か確かめられないので動かしません';
  if (!names.some((n) => n.includes(TEST_NAME_MARKER))) {
    return `商品名に「${TEST_NAME_MARKER}」が入っていません (実際: ${names[0]})。`
      + '捨ててよい検証用商品であることを確かめられないため動かしません';
  }
  return null;
}

/**
 * XML を「タグの道すじ → 値」の一覧に潰す。属性も拾う。
 *
 * ★自前の正規表現で読まない (Codex R1)。CDATA の中の "<"、シングルクォートの属性、
 *   分かれたテキストなどを取りこぼすと、消えた項目を見落として検証結果が嘘になる。
 * 同じタグが並ぶ時は連番を振る (SubCode[0] / SubCode[1] …) ので、
 * 「2つ目の SKU の価格」と「1つ目の SKU の価格」を取り違えない。
 *
 * @param {string} xml
 * @returns {Promise<Map<string,string>>}
 */
export async function flattenXml(xml) {
  const out = new Map();
  const src = String(xml ?? '').trim();
  if (!src) return out;
  const parsed = await parseStringPromise(src, {
    explicitArray: true,
    explicitCharkey: true,
    attrkey: '@',
    charkey: '#',
    trim: true,
    includeWhiteChars: false,
  });
  const rootName = Object.keys(parsed || {})[0];
  if (!rootName) return out;
  walk(parsed[rootName], `${rootName}[0]`, out);
  return out;
}

function walk(node, path, out) {
  if (node === null || node === undefined) return;
  if (typeof node !== 'object') {
    const t = String(node).trim();
    if (t) out.set(path, t);
    return;
  }
  for (const [key, value] of Object.entries(node)) {
    if (key === '@') {
      for (const [ak, av] of Object.entries(value || {})) out.set(`${path}@${ak}`, String(av));
      continue;
    }
    if (key === '#') {
      // 分かれたテキスト (CDATA と地の文が混ざる等) は連結して 1 つの値にする
      const text = (Array.isArray(value) ? value : [value]).map((v) => String(v)).join('').trim();
      if (text) out.set(path, text);
      continue;
    }
    const children = Array.isArray(value) ? value : [value];
    children.forEach((child, i) => walk(child, `${path}/${key}[${i}]`, out));
  }
}

/**
 * 前後の突き合わせ。
 * ★「消えた」を独立して数えるのが肝。全項目上書きなら、送らなかった項目がここに並ぶ。
 */
export function diff(beforeMap, afterMap) {
  const changed = [];
  const removed = [];
  const added = [];
  for (const [k, v] of beforeMap) {
    if (!afterMap.has(k)) removed.push({ path: k, before: v });
    else if (afterMap.get(k) !== v) changed.push({ path: k, before: v, after: afterMap.get(k) });
  }
  for (const [k, v] of afterMap) if (!beforeMap.has(k)) added.push({ path: k, after: v });
  return { changed, removed, added };
}

/** 価格の道すじか (価格だけが変わるのが期待値。それ以外が動いたら全項目上書き) */
export function isPricePath(path) {
  return /(^|\/)Price\[\d+\]$/.test(String(path || ''));
}

/** 商品本体の価格 (SubCode 配下ではないもの) を1つ返す。読めなければ null */
export function itemPriceOf(flat) {
  for (const [k, v] of flat) {
    if (!/(^|\/)Price\[0\]$/.test(k) || /SubCode/.test(k)) continue;
    const n = Number(v);
    return Number.isInteger(n) ? n : null;
  }
  return null;
}

/**
 * editItem の応答が成功か。Yahoo は HTTP 200 でも本文にエラーを入れてくることがある。
 * @returns {null|string} 問題なければ null、駄目なら理由
 */
export function editItemFailure({ status, body }) {
  if (status !== 200) return `HTTP ${status}: ${String(body || '').slice(0, 200)}`;
  const text = String(body || '');
  const err = text.match(/<Message>([\s\S]*?)<\/Message>/i) || text.match(/<Error[^>]*>([\s\S]*?)<\/Error>/i);
  if (err) return `応答にエラーが入っています: ${err[1].trim().slice(0, 200)}`;
  if (/<Code>(?!0<)/i.test(text)) return `応答にエラーコードが入っています: ${text.slice(0, 200)}`;
  return null;
}
