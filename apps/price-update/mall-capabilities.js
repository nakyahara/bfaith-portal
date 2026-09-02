/**
 * mall-capabilities.js — モールごとに「何ができるか」を1か所にまとめる。
 *
 * ★なぜ1か所にするか (2026-09-02):
 *   au PAY を更新できるようにした時、EXECUTABLE_MALLS と UPDATABLE_MALLS は直したのに
 *   pricing.js の「このモールは手動更新です」を直し忘れ、**画面では選べるのに
 *   送信の手前で必ず弾かれる**状態になった (テストで見つかった)。
 *   同じ事実が5か所に散っていると、増やし忘れが必ず起きる。
 *
 * ここに書くのは **設定だけ**。クライアントの実体 (どこへ送るか) は router が持つ。
 *
 *   updatable  … 画面で新売価を入れられる (false = 手動更新のチェックリスト行)
 *   executable … API で書き込める (送信経路がある)
 *   priceScope … 'sku'  = SKU (variant) ごとに価格を持つ … 楽天
 *                'item' = 商品に1つの価格。色は継承する … Yahoo / au PAY
 *                ※ここを取り違えると、1色ぶんのつもりの価格が全色に効いたうえで
 *                  照合が外れて失敗として記録される (2026-09-01 Yahoo / 09-02 au PAY で実測)
 *   killSwitch … 送信を許す env の名前。**明示的に有効でなければ送らない** (fail-closed)
 */
export const MALL_CAPABILITIES = {
  rakuten: {
    label: '楽天',
    updatable: true,
    executable: true,
    priceScope: 'sku',
    killSwitch: 'PRICE_UPDATE_RAKUTEN_ENABLED',
  },
  yahoo: {
    label: 'Yahoo',
    updatable: true,
    executable: true,
    priceScope: 'item',
    killSwitch: 'PRICE_UPDATE_YAHOO_ENABLED',
  },
  aupay: {
    label: 'au PAY',
    updatable: true,
    executable: true,
    priceScope: 'item',
    killSwitch: 'PRICE_UPDATE_AUPAY_ENABLED',
  },
  amazon: {
    label: 'Amazon',
    updatable: false,
    executable: false,
    // ★Amazon は「経路が無い」のではなく「このツールでは扱わない」。理由が違うので文言を分ける
    blockReason: 'Amazon は本ツールの更新対象外です (既存の価格管理の仕組みを使ってください)',
  },
  qoo10: {
    label: 'Qoo10',
    updatable: false,
    executable: false,
    blockReason: 'このモールは手動更新です (API更新の経路がまだありません)',
  },
};

/** 画面で新売価を入れられるモール */
export const UPDATABLE_MALLS = Object.entries(MALL_CAPABILITIES)
  .filter(([, c]) => c.updatable).map(([m]) => m);

/** API で書き込めるモール */
export const EXECUTABLE_MALLS = Object.entries(MALL_CAPABILITIES)
  .filter(([, c]) => c.executable).map(([m]) => m);

/** 「商品」に1つの価格しか持たないモール (色は商品価格を継承する) */
export const ITEM_PRICE_MALLS = new Set(Object.entries(MALL_CAPABILITIES)
  .filter(([, c]) => c.priceScope === 'item').map(([m]) => m));

/** そのモールの送信スイッチの env 名 (無ければ null) */
export function killSwitchKeyOf(mall) {
  return MALL_CAPABILITIES[mall]?.killSwitch || null;
}

/** 更新できないモールの理由 (更新できるなら null) */
export function blockReasonOf(mall) {
  const c = MALL_CAPABILITIES[mall];
  if (!c) return null;                      // 知らないモールは他の層で扱う
  if (c.updatable) return null;
  return c.blockReason || `${c.label || mall} はこのツールでは更新できません`;
}

/**
 * 設定の食い違いを見つける (起動時とテストで呼ぶ)。
 * ★「送れることになっているのにスイッチが無い」を放っておくと、
 *   env をいくら入れても開かないモールができる
 * @returns {string[]} 見つかった問題 (空なら問題なし)
 */
export function findCapabilityProblems() {
  const problems = [];
  for (const [mall, c] of Object.entries(MALL_CAPABILITIES)) {
    if (c.executable && !c.killSwitch) {
      problems.push(`${mall}: 送信できることになっているのに kill switch の名前がありません`);
    }
    if (c.executable && !c.updatable) {
      problems.push(`${mall}: 送信できるのに画面で新売価を入れられません (updatable が false)`);
    }
    if (c.executable && !c.priceScope) {
      problems.push(`${mall}: 送信できるのに価格の単位 (priceScope) が決まっていません`);
    }
    if (!c.updatable && !c.blockReason) {
      problems.push(`${mall}: 更新できない理由が書かれていません`);
    }
  }
  return problems;
}
