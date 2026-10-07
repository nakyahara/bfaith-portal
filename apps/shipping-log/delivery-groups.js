/**
 * 配送方法の「配送区分」正規化 (出荷件数ダッシュボード用)
 *
 * NE 側で **同じ便の 配送方法ID と 配送方法名 が入れ替わる** ことがある。
 * f_shipments_daily / mirror_shipments_daily は伝票に入っていた ID をそのまま持つので、
 * 集計を素直に出すと切替日を境に「旧区分が 0 件」「新区分が別系列で生える」になる。
 *   2026-09-18 まで … 71 / 'AES'
 *   2026-09-19 から … 64 / 'Amazon Easy Ship'
 * 見たいのは「その便で何件出したか」なので、ここで 1 つの配送区分にまとめる。
 * 過去分も同じ区分に畳まれるので、切替日をまたぐ期間でも 1 本の系列として読める。
 *
 * 🚨 区分への突合は **ID と名前の両方が一致したときだけ**。NE は使わなくなった ID を
 * 別の便に再利用しうるので、ID だけで拾うと将来「別の便が Easy Ship に混ざる」。逆に名前が
 * また変わったときは、まとまらずに新しい系列が画面に出る = 人が気づける方に倒している。
 *
 * 🚨 絞り込みの値 (画面の option value) も、区分と生の配送方法IDで**名前空間を分ける**。
 * 区分は 'g:64'、区分に入らない配送方法は生の ID ('41' など)。同じ '64' で両方を表すと、
 * 区分に畳まれなかった (64, '別の便') が Easy Ship の絞り込みに混ざる (Codex R1 Medium)。
 */

/** 区分の絞り込み値につける印。生の配送方法ID と区別できれば何でもよい */
export const GROUP_KEY_PREFIX = 'g:';

/**
 * 1 区分 = 複数の (ID, 名前) の組。name が画面・CSV に出る区分名。
 * id は区分の代表 ID (=いちばん新しい ID)。
 */
export const DELIVERY_GROUPS = [
  {
    id: '64',
    name: 'Amazon Easy Ship (AES)',
    members: [
      { id: '64', name: 'Amazon Easy Ship' }, // 2026-09-19〜
      { id: '71', name: 'AES' },              // 〜2026-09-18
    ],
  },
];

const norm = (v) => String(v ?? '').trim();

/** (ID, 名前) を 1 本のキーにする。区切り文字が名前に現れても壊れない形で持つ */
const memberKey = (id, name) => JSON.stringify([norm(id), norm(name)]);

/** (ID, 名前) → 区分 */
const MEMBER_INDEX = new Map();
for (const g of DELIVERY_GROUPS) {
  for (const m of g.members) MEMBER_INDEX.set(memberKey(m.id, m.name), g);
}
const GROUP_BY_ID = new Map(DELIVERY_GROUPS.map((g) => [g.id, g]));

/**
 * 明細 1 行の配送方法を区分に正規化する。
 * どの区分にも当てはまらなければ、その行の ID と名前をそのまま返す (名前が空なら '(未設定)')。
 * key は絞り込みの値 = 画面の option value。
 * @param {string|number} deliveryId
 * @param {string} deliveryName
 * @returns {{ id: string, name: string, key: string }}
 */
export function normalizeDelivery(deliveryId, deliveryName) {
  const id = norm(deliveryId);
  const name = norm(deliveryName);
  const g = MEMBER_INDEX.get(memberKey(id, name));
  if (g) return { id: g.id, name: g.name, key: GROUP_KEY_PREFIX + g.id };
  return { id, name: name || '(未設定)', key: id };
}

/**
 * 画面から来た絞り込み値を、SQL の delivery_id IN (...) 用の**生の ID** に展開する。
 * ここは粗く引くだけ (正確な判定は matchesMethod)。展開しないと区分キーを選んだときに
 * 切替前後のどちらかが落ちる。
 * @param {Iterable<string>} keys 区分キー ('g:64') か生の配送方法ID ('41')
 * @returns {string[]}
 */
export function expandMethodIds(keys) {
  const out = new Set();
  for (const raw of keys) {
    const key = norm(raw);
    if (!key) continue;
    if (key.startsWith(GROUP_KEY_PREFIX)) {
      const g = GROUP_BY_ID.get(key.slice(GROUP_KEY_PREFIX.length));
      for (const m of g ? g.members : []) out.add(norm(m.id));
      continue;
    }
    out.add(key); // 生の配送方法ID は**その ID だけ**
  }
  return [...out];
}

/**
 * 行が絞り込みに合致するか。展開した ID で SQL を引いたあとの最終判定。
 * - 区分キー 'g:64' … その区分に畳まれた行だけ。(64, '別の便') は入らない
 * - 生の ID … その ID の行だけ
 *
 * 🚨 生の ID に「その ID を含む区分」まで足してはいけない。画面が区分外の配送方法に
 * 使う値も生の ID なので、NE が 71 を別の便に使い回したあとに「佐川急便 (71)」を選ぶと
 * Easy Ship (64) まで足されてしまう (Codex R2 Medium)。
 * 副作用として、古い URL の method=71 は切替前の AES しか出さない。画面は URL のクエリを
 * 読まない (絞り込みは毎回 API へ投げる) ので、実害のあるブックマークは無い。
 * @param {Set<string>} requested
 * @param {string|number} rawId 行の delivery_id
 * @param {string} rawName 行の delivery_name
 */
export function matchesMethod(requested, rawId, rawName) {
  const id = norm(rawId);
  const g = MEMBER_INDEX.get(memberKey(id, rawName));
  if (g && requested.has(GROUP_KEY_PREFIX + g.id)) return true;
  return requested.has(id);
}
