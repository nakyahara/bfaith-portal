/**
 * 配送方法の「値の意味」を決める唯一の場所 (2026-09-03)。
 *
 * 画面の配送方法プルダウンには **意味の違う2種類の値** が並ぶ:
 *   - 楽天の配送方法グループID ('1'〜'9')  … RMS へそのまま送る値
 *   - 複合選択肢 ('1y5' / '1y8')          … 「楽天=定形外 + ヤフーだけ別配送」の入力用プリセット。
 *                                            **楽天の値ではない** (RMS に送ると不正値)
 *
 * この2つを1つの列 (draft_rakuten.shipping_method_group) に混ぜていたため、
 * 読む側が毎回「生値か・解決してから使うか」を判断する必要があり、判断漏れで
 * 「複合選択肢を選ぶと楽天に出品できない」不具合が出た (#725 → #1149)。
 *
 * 現在の約束:
 *   - **DB (draft_rakuten.shipping_method_group) には楽天グループIDしか入れない**。
 *     複合キーは API 境界 (保存時) で toRakutenShippingGroup が分解する
 *   - ヤフーを別扱いにするかは draft_yahoo.shipping_override (0/1) が持つ
 *   - 画面へ戻すときだけ shippingSelectValueOf が複合キーへ逆引きする
 *
 * db.js からも使う (マイグレーション) ため、services/rakuten-listing.js ではなく
 * 依存の無いこのモジュールに置く (循環参照を作らない)。
 */

/** B-Faith 店舗の配送方法グループ (RYS rakuten-to-notion-draft.js の実測変換表と同一 ID) */
export const SHIPPING_METHOD_GROUPS = {
  '1': '定形外',
  '3': '飛脚宅配便',
  '4': '宅急便',
  '5': 'ネコポス',
  '6': 'クリックポスト',
  '7': 'ヤマト運輸宅急便',
  '8': '宅急便50サイズ以上',
  '9': 'ゆうパケットパフ',
};

/**
 * 楽天=定形外のまま Yahoo! だけ別配送にする複合選択肢 (2026-08-06 中原さん指示)。
 * 楽天側 (ページ表記・末尾バナー) は rakutenGroup として振る舞い、
 * Yahoo!の配送方法プルダウンには yahooDelivery を初期セットする
 */
export const YAHOO_OVERRIDE_SHIPPING_GROUPS = {
  '1y8': { label: '定形外（ヤフーのみ宅急便50サイズ）', rakutenGroup: '1', yahooDelivery: '宅急便50サイズ以上' },
  '1y5': { label: '定形外（ヤフーのみネコポス）', rakutenGroup: '1', yahooDelivery: 'ネコポス' },
};

/**
 * 画面プルダウンの値 → 楽天の配送方法グループID。**保存と出品の両方でここを通す**。
 *
 * @param {string|null|undefined} raw プルダウンの値 ('1'〜'9' / '1y5' / '1y8' / 空)
 * @returns {{ok: boolean, group: string|null, yahooOverride: object|null}}
 *   ok=false        … 選択肢に無い値 (呼び出し側が「配送方法の指定が不正です」で止める)
 *   group=''        … 未指定 (= 楽天へは送らず店舗デフォルトに任せる)
 *   yahooOverride   … 複合選択肢のときだけ非 null (ヤフーの初期配送方法を持つ)
 */
export function toRakutenShippingGroup(raw) {
  const g = String(raw ?? '').trim();
  if (!g) return { ok: true, group: '', yahooOverride: null };
  const ov = YAHOO_OVERRIDE_SHIPPING_GROUPS[g];
  if (ov) return { ok: true, group: ov.rakutenGroup, yahooOverride: ov };
  if (SHIPPING_METHOD_GROUPS[g]) return { ok: true, group: g, yahooOverride: null };
  return { ok: false, group: null, yahooOverride: null };
}

/**
 * DB の値 → 画面プルダウンに selected させる値 (toRakutenShippingGroup の逆)。
 * ヤフー別扱いのときは、楽天グループ + ヤフーの配送方法が一致する複合キーへ戻す。
 * 一致する複合が無い (人がヤフーの配送方法だけ別のものに変えた) ときは楽天グループを返す
 * — プルダウンは「定形外」を指し、ヤフー欄は shipping_override で開いたままになる
 *
 * @param {string|null} group draft_rakuten.shipping_method_group (楽天グループID)
 * @param {{shipping_override?: number, delivery_label?: string}|null} yahoo draft_yahoo の行
 */
export function shippingSelectValueOf(group, yahoo) {
  const g = String(group ?? '').trim();
  if (!g || !yahoo?.shipping_override) return g;
  const hit = Object.entries(YAHOO_OVERRIDE_SHIPPING_GROUPS)
    .find(([, ov]) => ov.rakutenGroup === g && ov.yahooDelivery === yahoo.delivery_label);
  return hit ? hit[0] : g;
}
