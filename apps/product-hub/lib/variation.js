/**
 * NE (ネクストエンジン) の 代表商品コード からバリエーションを判定する。
 *
 * 実データ調査 (2026-07-25, warehouse.db raw_ne_products 4,916件):
 *   - 代表商品コードが空 = 2,786件 (56.7%) … 単品
 *   - 代表商品コードあり = 2,130件 … バリエーションの子SKU
 *   - 代表コードは約365種類。**うち338種類 (93%) は商品として実在しない** =
 *     代表商品コードは「親商品の行」ではなく **グループキーの文字列**。
 *     実際の値も rooms / aromamist20 / ws100 のように楽天の管理番号相当。
 *   → product-hub は「楽天の1ページ = 1ドラフト」なので、
 *     **グループキー = 代表商品コードがあればそれ、無ければ商品コード自身** と定義する。
 *
 * ⚠️ NE データにはノイズがある (自動でまとめず、人に確認させる理由):
 *   - 代表コード `10` に mercari01〜50「メルカリ訳アリ品」が50件 (バリエーションではない)
 *   - `pokemonpatch2` に pokemonpatch-系 と pokemonpatch2-系 の2系統が混在
 *   - 代表コードがあるのに1SKUしかないグループが35件
 *
 * 照合は LOWER(TRIM()) で行う ([[feedback-sku-case-normalization]])。
 */

/** @typedef {'unknown'|'single'|'variation'|'conflict'} VariationKind */

/** IN 句のバインド上限 (SQLite 既定 999) に当たらないための分割単位 (Codex low-7) */
const IN_CHUNK = 400;

function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

const norm = (v) => (v == null ? '' : String(v).trim().toLowerCase());

/** mirror_products が未作成/空でも落ちないようにする (fail-soft) */
function mirrorReady(db) {
  try {
    db.prepare('SELECT 1 FROM mirror_products LIMIT 1').get();
    return true;
  } catch (_) {
    return false;
  }
}

/**
 * 1 商品コードのバリエーション判定。
 * @returns {{
 *   kind: VariationKind, groupKey: string, repCode: string|null,
 *   isChild: boolean, memberCount: number, members: object[], found: boolean
 * }}
 *   kind='unknown'   … NE に無い (これから登録する新商品など)。手動フラグへフォールバック
 *   kind='single'    … NE にあり代表コード無し = 単品
 *   kind='variation' … バリエーション。groupKey が楽天1ページに相当する
 *   isChild=true     … 渡されたコードが子SKUで、groupKey と異なる (まとめる候補)
 */
export function resolveVariationGroup(db, neCode, { withMembers = true } = {}) {
  const code = String(neCode == null ? '' : neCode).trim();
  const empty = {
    kind: 'unknown', groupKey: code, repCode: null,
    isChild: false, memberCount: 0, members: [], found: false,
  };
  if (!code || !mirrorReady(db)) return empty;

  // 正規化後に複数行ヒットする = mirror 側の表記ゆらぎ重複。
  // どちらのグループに属するか決められないので人に確認させる (Codex low-8)
  const selves = db.prepare(
    'SELECT 商品コード, 代表商品コード FROM mirror_products WHERE LOWER(TRIM(商品コード)) = ?'
  ).all(norm(code));
  if (selves.length > 1) {
    return { ...empty, kind: 'conflict', found: true, memberCount: selves.length };
  }
  const self = selves[0] || null;

  const rep = self && self.代表商品コード && String(self.代表商品コード).trim() !== ''
    ? String(self.代表商品コード).trim()
    : null;
  // 代表コードがあればそれがグループキー。無ければ自分自身がキー候補
  const groupKey = rep || code;

  const memberCount = db.prepare(
    'SELECT COUNT(*) AS c FROM mirror_products WHERE LOWER(TRIM(代表商品コード)) = ?'
  ).get(norm(groupKey)).c;

  if (memberCount === 0) {
    // 代表コードに紐づく行が無い = 単品 (NE に本人が居る場合) / 不明 (居ない場合)
    return { ...empty, kind: self ? 'single' : 'unknown', groupKey: code, found: !!self };
  }

  const members = withMembers
    ? db.prepare(`
        SELECT 商品コード, 商品名, 取扱区分, 在庫数, 標準売価
        FROM mirror_products WHERE LOWER(TRIM(代表商品コード)) = ?
        ORDER BY 商品コード
      `).all(norm(groupKey))
    : [];

  return {
    kind: 'variation',
    groupKey,
    repCode: rep,
    // 入力コードがグループキーと違う = 子SKU を入力された (まとめる候補)
    isChild: norm(code) !== norm(groupKey),
    memberCount,
    members,
    found: !!self,
  };
}

/**
 * 一覧向けの一括判定 (N+1 回避)。members は返さない。
 * @returns {Map<string, {kind: VariationKind, groupKey: string, memberCount: number, isChild: boolean}>}
 *   キーは正規化済み (LOWER(TRIM())) の商品コード
 */
export function resolveVariationGroupsBatch(db, neCodes) {
  const result = new Map();
  const codes = [...new Set((neCodes || []).map((c) => String(c == null ? '' : c).trim()).filter(Boolean))];
  if (codes.length === 0 || !mirrorReady(db)) {
    for (const c of codes) result.set(norm(c), { kind: 'unknown', groupKey: c, memberCount: 0, isChild: false });
    return result;
  }

  const selfByCode = new Map();
  const dupCodes = new Set(); // 正規化後に重複する = どのグループか決められない
  for (const part of chunk(codes.map(norm), IN_CHUNK)) {
    const ph = part.map(() => '?').join(',');
    const rows = db.prepare(
      `SELECT 商品コード, 代表商品コード FROM mirror_products WHERE LOWER(TRIM(商品コード)) IN (${ph})`
    ).all(...part);
    for (const r of rows) {
      const k = norm(r.商品コード);
      if (selfByCode.has(k)) dupCodes.add(k);
      else selfByCode.set(k, r);
    }
  }

  // 各コードのグループキーを決めてから、キー単位で件数をまとめて引く
  const groupKeyOf = new Map();
  for (const c of codes) {
    const s = selfByCode.get(norm(c));
    const rep = s && s.代表商品コード && String(s.代表商品コード).trim() !== '' ? String(s.代表商品コード).trim() : null;
    groupKeyOf.set(norm(c), rep || c);
  }
  const keys = [...new Set([...groupKeyOf.values()].map(norm))];
  const counts = new Map();
  for (const part of chunk(keys, IN_CHUNK)) {
    const kph = part.map(() => '?').join(',');
    for (const r of db.prepare(`
      SELECT LOWER(TRIM(代表商品コード)) AS k, COUNT(*) AS c
      FROM mirror_products WHERE LOWER(TRIM(代表商品コード)) IN (${kph})
      GROUP BY LOWER(TRIM(代表商品コード))
    `).all(...part)) counts.set(r.k, r.c);
  }

  for (const c of codes) {
    if (dupCodes.has(norm(c))) {
      result.set(norm(c), { kind: 'conflict', groupKey: c, memberCount: 0, isChild: false });
      continue;
    }
    const key = groupKeyOf.get(norm(c));
    const cnt = counts.get(norm(key)) || 0;
    const self = selfByCode.get(norm(c));
    if (cnt === 0) {
      result.set(norm(c), { kind: self ? 'single' : 'unknown', groupKey: c, memberCount: 0, isChild: false });
    } else {
      result.set(norm(c), { kind: 'variation', groupKey: key, memberCount: cnt, isChild: norm(c) !== norm(key) });
    }
  }
  return result;
}

/**
 * 画面表示用の has_variation。**NE を正とし、NE に無い商品だけ手入力値へフォールバック**
 * (中原さん決定 2026-07-25)。DB の has_variation は焼き直さないので、
 * NE 側でバリエーション構成が変わっても常に最新が映る。
 */
export function effectiveHasVariation(info, draft) {
  if (info && info.kind === 'variation') return { value: true, source: 'ne' };
  if (info && info.kind === 'single') return { value: false, source: 'ne' };
  return { value: !!(draft && draft.has_variation), source: 'manual' };
}
