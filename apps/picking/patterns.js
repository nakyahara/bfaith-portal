/**
 * カンタン引当パターンマスタ (引当分類)。
 *
 * 正本 = AI_reference/システム設計/ロジザード作業自動化/カンタン引当_条件定義_20260721.md と
 * 同フォルダの hikiate-patterns.csv (実画面から採取した23パターン+配送方法対応)。
 * ロジザード側でパターンを増減したらここも追従する (CS03002 にはパターン名が入らないため、
 * 取込時にこのリストから人が選ぶ / システムが候補を推定する)。
 */

// { no, name, soft (送り状発行ソフト名), methods (配送方法名の配列), composition } 。
// composition: tanpin=単品 / multi=1SKU複数個 / assort=複数SKU複数個 / all=全て系 (内包)
export const HIKIATE_PATTERNS = [
  { no: 17, name: 'LINEギフト《単品、複数個を含む全て》', soft: null, methods: null, composition: 'all' },
  { no: 1, name: 'ネコポス【梱包機PAS-LINE《3つ折り》】単品', soft: 'B2(Ver6.0)', methods: ['ネコポス 陸便 元払い 営業所止めなし'], composition: 'tanpin' },
  { no: 5, name: 'ネコポス【梱包機PAS-LINE《3つ折り》】1SKU複数個', soft: 'B2(Ver6.0)', methods: ['ネコポス 陸便 元払い 営業所止めなし'], composition: 'multi' },
  { no: 9, name: 'ネコポス【梱包機PAS-LINE《3つ折り》】複数SKU複数個《単品を含むネコポス梱包機全て》', soft: 'B2(Ver6.0)', methods: ['ネコポス 陸便 元払い 営業所止めなし'], composition: 'all' },
  { no: 22, name: 'ネコポス【梱包機PAS-LINE《2つ折り》】単品', soft: 'B2(Ver6.0)', methods: ['ネコポス 陸便 元払い 営業所止めなし'], composition: 'tanpin' },
  { no: 23, name: 'ネコポス【梱包機PAS-LINE《2つ折り》】1SKU複数個', soft: 'B2(Ver6.0)', methods: ['ネコポス 陸便 元払い 営業所止めなし'], composition: 'multi' },
  { no: 24, name: 'ネコポス【梱包機PAS-LINE《2つ折り》】複数SKU複数個《単品を含むネコポス梱包機PAS-LINE《2つ折り》全て》', soft: 'B2(Ver6.0)', methods: ['ネコポス 陸便 元払い 営業所止めなし'], composition: 'all' },
  { no: 25, name: 'ネコポス【梱包機MELT-LINE】単品', soft: 'B2(Ver6.0)', methods: ['ネコポス 陸便 元払い 営業所止めなし'], composition: 'tanpin' },
  { no: 26, name: 'ネコポス【梱包機MELT-LINE】1SKU複数個', soft: 'B2(Ver6.0)', methods: ['ネコポス 陸便 元払い 営業所止めなし'], composition: 'multi' },
  { no: 27, name: 'ネコポス【梱包機MELT-LINE】複数SKU複数個《単品を含むネコポス梱包機MELT-LINE全て》', soft: 'B2(Ver6.0)', methods: ['ネコポス 陸便 元払い 営業所止めなし'], composition: 'all' },
  { no: 3, name: 'ネコポス手動単品', soft: 'B2(Ver6.0)', methods: ['ネコポス 陸便 元払い 営業所止めなし'], composition: 'tanpin' },
  { no: 6, name: 'ネコポス手動1SKU複数個', soft: 'B2(Ver6.0)', methods: ['ネコポス 陸便 元払い 営業所止めなし'], composition: 'multi' },
  { no: 10, name: 'ネコポス手動複数SKU複数個', soft: 'B2(Ver6.0)', methods: ['ネコポス 陸便 元払い 営業所止めなし'], composition: 'assort' },
  { no: 11, name: '50サイズ宅急便単品', soft: 'B2(Ver6.0)', methods: ['箱 陸便 元払い 営業所止めなし'], composition: 'tanpin' },
  { no: 13, name: '50サイズ宅急便複数個《単品を含む50サイズ全て》', soft: 'B2(Ver6.0)', methods: ['箱 陸便 元払い 営業所止めなし'], composition: 'all' },
  { no: 12, name: '60サイズ以上宅急便単品', soft: 'B2(Ver6.0)', methods: ['箱 陸便 元払い 営業所止めなし'], composition: 'tanpin' },
  { no: 14, name: '60サイズ以上宅急便複数個《単品を含む60サイズ全て》', soft: 'B2(Ver6.0)', methods: ['箱 陸便 元払い 営業所止めなし'], composition: 'all' },
  { no: 16, name: 'ゆうパケットパフ《単品、複数個を含む全て》', soft: 'ゆうプリR', methods: ['ゆうパケット 陸便 元払い 営業所止めなし'], composition: 'all' },
  { no: 19, name: '定形外手動《単品、複数個を含む全て》', soft: '汎用送り状', methods: ['汎用フォーマット01 陸便 元払い 通常出荷'], composition: 'all' },
  { no: 15, name: 'レターパック《単品、複数個を含む全て》', soft: '汎用送り状', methods: ['汎用フォーマット02 陸便 元払い 通常出荷'], composition: 'all' },
  { no: 21, name: 'AES《単品》', soft: 'DENZOU', methods: ['箱 陸便 元払い 営業所止めなし'], composition: 'tanpin' },
  { no: 28, name: 'AES《1SKU複数個》', soft: 'DENZOU', methods: ['箱 陸便 元払い 営業所止めなし'], composition: 'multi' },
  { no: 29, name: 'AES《複数SKU複数個》《単品、複数個を含む全て》', soft: 'DENZOU', methods: ['箱 陸便 元払い 営業所止めなし'], composition: 'all' },
];

// バッチの数量構成 (service.js が判定) → パターンの composition との適合。
// 「全て系 (all)」は内包 (残差) パターンなのでどの構成にも適合する。
// 混在 (まとめ引当で複数構成が同居) は all のみ適合。
const COMPOSITION_MATCH = {
  '単品': ['tanpin', 'all'],
  '1SKU複数個': ['multi', 'all'],
  'アソート': ['assort', 'all'],
  '混在': ['all'],
};

/**
 * CSVから読める情報 (送り状発行ソフト名・配送方法名・数量構成) でパターン候補を絞る。
 * softs/methods は distinct 配列 (LINEギフト等は1バッチに複数の配送方法が正当に混在するため、
 * 先頭行決め打ちにしない)。完全一致候補 → 構成のみ不一致の候補 の順で返す (先頭が推定値)。
 * どれにも合わなければ空配列 (画面側は全パターンから選ばせる)。
 */
export function suggestPatterns({ invoiceSofts, deliveryMethods, composition }) {
  const softs = invoiceSofts || [];
  const methods = deliveryMethods || [];
  const compatible = COMPOSITION_MATCH[composition] || ['all'];
  const bySoft = HIKIATE_PATTERNS.filter((p) =>
    (p.soft === null || softs.includes(p.soft)) &&
    (p.methods === null || p.methods.some((m) => methods.includes(m))));
  const exact = bySoft.filter((p) => compatible.includes(p.composition));
  const rest = bySoft.filter((p) => !compatible.includes(p.composition));
  // 具体的な候補ほど先頭に: 送り状ソフト・配送方法が明示一致 (+2) > ワイルドカード
  // (LINEギフトは soft/methods 不問なので常に候補に残るが、明示一致があるときは後ろへ)。
  // 構成の専用パターン (+1) は内包の「全て系」より具体的
  const score = (p) => (p.soft !== null ? 2 : 0) + (p.composition !== 'all' ? 1 : 0);
  exact.sort((a, b) => score(b) - score(a));
  return [...exact, ...rest].map((p) => p.name);
}

export function allPatternNames() {
  return HIKIATE_PATTERNS.map((p) => p.name);
}
