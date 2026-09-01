/**
 * yahoo-edit-item-probe.js — editItem 検証の部品 (純関数)
 *
 * smoke-yahoo-edit-item.js から切り出した。Yahoo にも VPS にも接続しない。
 * ここが間違っていると「消えた項目を見落とす」= 検証そのものが嘘になるので、単体でテストする。
 */

/** 検証用の商品コードの接頭辞。本番商品を触らないための境界 */
export const TEST_CODE_PREFIX = 'zz-';

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
 * XML を「タグの道すじ → 値」の一覧に潰す。属性も拾う。
 * 同じタグが並ぶ時は連番を振る (SubCode[0] / SubCode[1] …) ので、
 * 「2つ目の SKU の価格」と「1つ目の SKU の価格」を取り違えない。
 *
 * @param {string} xml
 * @returns {Map<string,string>}
 */
export function flattenXml(xml) {
  const out = new Map();
  const stack = [];
  const counts = [new Map()];
  const re = /<([A-Za-z][\w.:-]*)((?:\s+[\w.:-]+\s*=\s*"[^"]*")*)\s*(\/?)>|<\/([A-Za-z][\w.:-]*)\s*>|([^<]+)/g;
  let m;
  while ((m = re.exec(String(xml ?? ''))) !== null) {
    const [, openTag, attrs, selfClose, closeTag, text] = m;
    if (openTag) {
      const level = counts[counts.length - 1];
      const idx = level.get(openTag) || 0;
      level.set(openTag, idx + 1);
      stack.push(`${openTag}[${idx}]`);
      if (attrs) {
        for (const a of attrs.matchAll(/([\w.:-]+)\s*=\s*"([^"]*)"/g)) {
          out.set(`${stack.join('/')}@${a[1]}`, a[2]);
        }
      }
      if (selfClose) stack.pop();
      else counts.push(new Map());
    } else if (closeTag) {
      stack.pop();
      counts.pop();
    } else if (text) {
      const t = text.trim();
      if (t) out.set(stack.join('/'), t);
    }
  }
  return out;
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
