/**
 * yahoo-edit-item-fields.js — editItem の「必須項目」を割り出すための部品 (純関数)
 *
 * 実測 (2026-09-01): editItem に item_code と price だけを送ると
 *   HTTP 400 / <Target>path</Target> <Code>it-01011</Code> 「パスは必須です」
 * が返る。つまり editItem は「変えたい項目だけ送る」形では呼べず、出品登録と同じで
 * 必須項目を毎回要求する。
 *
 * 応答の <Target> が **足りない項目の名前** を教えてくれるので、
 * getItem で取った「前」の値からその項目を1つずつ足して再送し、必須項目の最小集合を割り出す。
 *
 * ★知らない項目名が返ってきたら止める。当てずっぽうで値を作って送らない
 *   (間違った値で商品が書き換わる方が、分からないまま止まるより悪い)。
 */

/**
 * editItem の項目名 → getItem の応答のどこから値を取るか。
 * 値は「商品本体の道すじ」からの相対で書く。
 * ★ここに無い項目名が要求されたら、その回は止めて人に報告する。
 */
export const FIELD_SOURCES = {
  item_code: { tag: 'ItemCode' },
  name: { tag: 'Name' },
  // カテゴリのパス。PathList の中に複数入ることがあり、origFlag="1" が本命 (VPS のパーサと同じ扱い)
  path: { tag: 'Path', within: 'PathList', preferAttr: 'origFlag' },
  product_category: { tag: 'ProductCategory' },
  headline: { tag: 'Headline' },
  caption: { tag: 'Caption' },
  abstract: { tag: 'Abstract' },
  explanation: { tag: 'Explanation' },
  taxable: { tag: 'Taxable' },
  quantity: { tag: 'Quantity' },
  display: { tag: 'Display' },
  postage_set: { tag: 'PostageSet' },
  delivery: { tag: 'Delivery' },
  template_id: { tag: 'TemplateId' },
};

/**
 * editItem の応答から「どの項目が足りないか」を読む。
 * @param {{status:number, body:string}} res
 * @returns {{status:number, target:string|null, code:string|null, message:string|null}|null}
 *   問題なければ null
 */
export function editItemError(res) {
  const status = res?.status;
  const text = String(res?.body || '');
  const pick = (tag) => {
    const m = text.match(new RegExp(`<${tag}>(?:<!\\[CDATA\\[)?([\\s\\S]*?)(?:\\]\\]>)?</${tag}>`, 'i'));
    return m ? m[1].trim() : null;
  };
  const status2xx = status >= 200 && status < 300;
  const ng = /<Status>\s*NG\s*<\/Status>/i.test(text);
  if (status2xx && !ng && !/<Error[\s>]/i.test(text)) return null;
  return {
    status,
    target: pick('Target'),
    code: pick('Code'),
    message: pick('Message'),
  };
}

/**
 * その失敗は「送る前に弾かれた」= 商品は書き換わっていない、と言い切れるか。
 * ★言い切れる時だけ true。迷ったら false (戻しに行く方に倒す)。
 *   楽天側と同じ考え方: 意味の分かる失敗は書き込まれていない / 分からない失敗は不明として扱う。
 */
export function isDefiniteRejection(err) {
  if (!err) return false;
  // 4xx で、どの項目が悪いか・何のエラーかを名指しできている = Yahoo が受け付けずに返した
  if (err.status >= 400 && err.status < 500 && (err.target || err.code)) return true;
  return false;
}

/**
 * 「前」の応答から、指定した editItem 項目の値を取る。取れなければ null。
 * @param {Map<string,string>} flat flattenXml の戻り
 * @param {string} itemBase 商品本体の道すじ
 * @param {string} field editItem の項目名
 */
export function fieldValueFrom(flat, itemBase, field) {
  const spec = FIELD_SOURCES[field];
  if (!spec || !itemBase) return null;
  const base = spec.within ? `${itemBase}/${spec.within}[0]` : itemBase;
  const head = `${base}/${spec.tag}[`;
  const hits = [];
  for (const [k, v] of flat) {
    if (!k.startsWith(head)) continue;
    const rest = k.slice(head.length);
    if (!rest.endsWith(']') || !/^\d+$/.test(rest.slice(0, -1))) continue;
    hits.push({ path: k, value: v });
  }
  if (hits.length === 0) return null;
  if (hits.length === 1) return hits[0].value;
  // 複数あるときは、印のついたもの (origFlag="1") を本命とする
  if (spec.preferAttr) {
    const marked = hits.filter((h) => flat.get(`${h.path}@${spec.preferAttr}`) === '1');
    if (marked.length === 1) return marked[0].value;
  }
  // ★どれか決められないなら null。当てずっぽうで送らない
  return null;
}
