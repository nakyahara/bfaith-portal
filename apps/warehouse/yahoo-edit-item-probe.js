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
 * その道すじが「base の直下の tag」か (連番は問わない)。
 * ★正規表現を組み立てない。道すじには [ ] が入るので、埋め込むと壊れやすい。
 */
export function isDirectChild(path, base, tag) {
  if (!base) return false;
  const head = `${base}/${tag}[`;
  const p = String(path || '');
  if (!p.startsWith(head)) return false;
  const rest = p.slice(head.length);
  return rest.endsWith(']') && /^\d+$/.test(rest.slice(0, -1));
}

/**
 * 商品本体の要素の道すじを返す (例: "ResultSet[0]/Result[0]")。決められなければ null。
 *
 * ★「ルート直下が商品」と決め打ちしない。
 *   実測 (2026-09-01): getItem の応答は ResultSet > Result の二段で、商品本体は 2 階層目にある。
 * ★さらに **ItemCode の値が、こちらが指定したコードと一致する要素** に限る (Codex R1)。
 *   最初に見つけた ItemCode を使うと、応答に商品が複数あった時に別商品を本体とみなし、
 *   その別商品の名前に目印があれば門番を通り抜けてしまう。
 *   一致が 0 件でも 2 件以上でも null にする (決められないなら動かさない)。
 *
 * @param {Map<string,string>} flat
 * @param {string} expectedItemCode 指定した商品コード
 */
export function itemBaseOf(flat, expectedItemCode) {
  const want = String(expectedItemCode || '').trim().toLowerCase();
  if (!want) return null;
  const hits = [];
  for (const [k, v] of flat) {
    const m = String(k).match(/^(.*)\/ItemCode\[\d+\]$/);
    if (m && String(v).trim().toLowerCase() === want) hits.push(m[1]);
  }
  return hits.length === 1 ? hits[0] : null;
}

/**
 * 取ってきた商品が「捨ててよい検証用商品」か。問題なければ null、駄目なら理由。
 * ★商品名に目印が入っていることを、書き込む前に実物で確かめる。
 * @param {Map<string,string>} flat flattenXml の戻り
 * @param {string} itemCode 指定した商品コード (応答の中でこのコードの商品を特定するため)
 */
export function guardTestItem(flat, itemCode) {
  const base = itemBaseOf(flat, itemCode);
  if (!base) {
    return `応答の中から商品コード ${itemCode || '(未指定)'} の商品を1つに特定できませんでした。`
      + '検証用商品か確かめられないので動かしません';
  }
  // ★商品本体の名前だけを見る。入れ子のどこかに目印があれば通る、では門番にならない
  const names = [...flat.entries()]
    .filter(([k]) => isDirectChild(k, base, 'Name'))
    .map(([, v]) => v);
  if (names.length === 0) return '商品名を読めませんでした。検証用商品か確かめられないので動かしません';
  // ★商品名が複数あるのは想定外。「どれか1つに目印があれば通す」にすると、
  //   本番名と目印が並んでいる時に通ってしまう (Codex R2)。1つに定まらなければ動かさない
  if (names.length > 1) {
    return `商品名が ${names.length} 個ありました (${names.join(' / ')})。`
      + 'どれが商品名か決められないので動かしません';
  }
  if (!names[0].includes(TEST_NAME_MARKER)) {
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

/** 価格の道すじか (商品本体でも SKU でも) */
export function isPricePath(path) {
  return /(^|\/)Price\[\d+\]$/.test(String(path || ''));
}

/**
 * **商品本体の** 価格の道すじか。
 * ★「変わってよい」のはここだけ。SKU の価格まで見逃すと、
 *   item_code と price を送っただけで SKU の価格が動いても部分更新だと誤って結論する (Codex R5)。
 * @param {string} path
 * @param {string|null} itemBase itemBaseOf() の戻り。null なら「商品本体ではない」扱い (fail-closed)
 */
export function isItemPricePath(path, itemBase) {
  return isDirectChild(path, itemBase, 'Price');
}

/**
 * 「価格以外の変化」を数える。
 * ★変わった (価格以外) / 消えた / **増えた** の3つとも数える。
 *   増えたものを無視すると、項目が生えた時に部分更新だと誤って結論する (Codex R2)。
 * @param {{changed:Array,removed:Array,added:Array}} d diff() の戻り
 * @param {string|null} itemBase itemBaseOf() の戻り
 */
export function collateralOf(d, itemBase) {
  return [
    // ★除外するのは商品本体の価格だけ。SKU の価格が動いたのは「価格以外の変化」として数える
    ...(d.changed || []).filter((x) => !isItemPricePath(x.path, itemBase)),
    ...(d.removed || []),
    ...(d.added || []),
  ];
}

/**
 * 商品本体の価格。読めなければ null
 * @param {Map<string,string>} flat
 * @param {string} itemCode 指定した商品コード
 */
export function itemPriceOf(flat, itemCode) {
  const base = itemBaseOf(flat, itemCode);
  if (!base) return null;
  const values = [...flat.entries()].filter(([k]) => isItemPricePath(k, base)).map(([, v]) => v);
  // ★商品直下の Price が複数 = 想定外の構造。どれが売価か決められないので読めない扱いにする。
  //   最初の1つを採るとその値を基準に値付けしてしまう (Codex R3)
  if (values.length !== 1) return null;
  const n = Number(values[0]);
  // ★安全整数の範囲まで見る。範囲外だと currentPrice + 1 が currentPrice と同じ値になり、
  //   価格が変わっていないのに「変わった」と読んでしまう (Codex R4)
  return Number.isSafeInteger(n) ? n : null;
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
