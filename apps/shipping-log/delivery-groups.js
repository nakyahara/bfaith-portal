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
 * 🚨 突合は **ID と名前の両方が一致したときだけ**。NE は使わなくなった ID を別の便に
 * 再利用しうるので、ID だけで拾うと将来「別の便が Easy Ship に混ざる」。逆に名前が
 * また変わったときは、まとまらずに新しい系列が画面に出る = 人が気づける方に倒している。
 */

/**
 * 1 区分 = 複数の (ID, 名前) の組。name が画面・CSV に出る区分名。
 * id は絞り込みの値 (=いちばん新しい ID) として使う。
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

/** (ID, 名前) → 区分。member 一覧を引くだけなので Map に畳んでおく */
const MEMBER_INDEX = new Map();
for (const g of DELIVERY_GROUPS) {
  for (const m of g.members) MEMBER_INDEX.set(memberKey(m.id, m.name), g);
}

/** 区分ID → その区分に属する全 ID (絞り込みを SQL に落とすときの展開用) */
const GROUP_MEMBER_IDS = new Map(
  DELIVERY_GROUPS.map((g) => [g.id, [...new Set(g.members.map((m) => norm(m.id)))]]),
);

/**
 * 明細 1 行の配送方法を区分に正規化する。
 * どの区分にも当てはまらなければ、その行の ID と名前をそのまま返す (名前が空なら '(未設定)')。
 * @param {string|number} deliveryId
 * @param {string} deliveryName
 * @returns {{ id: string, name: string }}
 */
export function normalizeDelivery(deliveryId, deliveryName) {
  const id = norm(deliveryId);
  const name = norm(deliveryName);
  const g = MEMBER_INDEX.get(memberKey(id, name));
  if (g) return { id: g.id, name: g.name };
  return { id, name: name || '(未設定)' };
}

/**
 * 画面から来た配送方法の絞り込み値を、SQL の delivery_id IN (...) 用に展開する。
 * 区分ID を選んだら、その区分に含まれる旧 ID も引く (選び直さなくても過去分が出る)。
 * @param {Iterable<string>} ids
 * @returns {string[]}
 */
export function expandMethodIds(ids) {
  const out = new Set();
  for (const raw of ids) {
    const id = norm(raw);
    if (!id) continue;
    out.add(id);
    for (const m of GROUP_MEMBER_IDS.get(id) || []) out.add(m);
  }
  return [...out];
}

/**
 * 行が絞り込みに合致するか。展開した ID で SQL を引いたあとの最終判定。
 * 「選んだ区分ID」か「行の生の ID」のどちらかに一致すれば残す。
 * - 区分ID 64 を選ぶ → 64 と (71,'AES') が残る
 * - 旧 URL で 71 を直接指定 → (71,'AES') が残る (ブックマークが壊れない)
 * - 将来 71 が別の便に再利用されたら、64 を選んでもその行は残らない
 * @param {Set<string>} requested
 * @param {string|number} rawId 行の delivery_id
 * @param {string} groupId normalizeDelivery が返した区分ID
 */
export function matchesMethod(requested, rawId, groupId) {
  return requested.has(norm(rawId)) || requested.has(norm(groupId));
}
