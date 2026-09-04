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
  linegift: {
    label: 'LINEギフト',
    // 2026-09-04〜: 現在売価は読める (GET /api/v1/shops/{shop_id}/items/{item_id} の price)。
    // ★更新はしない。価格専用の更新APIが無く PATCH /items/{item_id} = 商品まるごと更新しか無いため
    //   (Yahoo editItem と同じ「送らなかった項目が消える」危険)。さらに GET が返す画像は id+url なのに
    //   PATCH は temporary_uuid を要求するので、読んだ画像を送り返せない = 全上書きなら復元できない。
    //   部分更新かを実測するまで書き込みは作らない。
    updatable: false,
    executable: false,
    // variations に price が無い (在庫だけ) ことを実データで確認済 (2026-09-04 nikukyu15)
    priceScope: 'item',
    blockReason: 'LINEギフトは価格だけを変える API が無いため、本ツールでは表示のみです (更新は管理画面で)',
  },
  qoo10: {
    label: 'Qoo10',
    // 2026-09-02〜: ItemsOrder.SetGoodsPriceQty (価格専用 API) で更新できる。
    // 実測で「価格以外は1バイトも変わらない」ことを確認済み (M5実測結果_Qoo10)
    updatable: true,
    executable: true,
    priceScope: 'item',
    killSwitch: 'PRICE_UPDATE_QOO10_ENABLED',
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
