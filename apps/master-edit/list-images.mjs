/**
 * list-images.mjs — 商品・セットの一覧の「画像」の列 (10/17 中原さん「楽天の白抜き商品画像でいい。サムネイルみたいなかたちで」)
 *
 * 画像の元 = ピッキング画面と同じ楽天の画像のキャッシュ (apps/picking/images.js・picking.db の pk_product_images)。
 *   Render の同じプロセス・同じ DATA_DIR にある = ここから読むだけで足りる (入荷受付・梱包・いろはも同じ物を使っている)。
 *   選ぶ順もピッキングと同じ: そのSKU自身のバリエーション画像 → 白抜き → 1枚目 (別の色の写真を出さない・2026-08-25 現場指摘)。
 * 🚨 一覧の表示を楽天の呼び出しで止めない: ここではキャッシュを読むだけ。キャッシュに無い物は picking の直列キューに積む (fire-and-forget)。
 *    積むのはこのページに出す商品だけ・1 回に LIST_IMAGE_QUEUE_MAX 件まで (楽天に投げるコードは LIST_IMAGE_MAX_CODES = bulk 1 回まで)・一覧の待ちジョブは 1 つまで (開くたびに積み増さない)・「楽天に無い」は 7 日は聞き直さない。
 *    楽天につながらない環境 (WAREHOUSE_URL が無い = 開発の PC・試験) では積まない (画像の無い商品は「まだ取っていない」のまま)。
 * セット = セット自身のコードで楽天の画像を引く (楽天にセットの商品ページがあればその写真)。無ければ「画像なし」の印。
 *   構成品の写真では代わりにしない (1 つの構成品の写真を出すと、別の商品に見えて取り違えの元になる)。
 * 読めない (picking.db が開けない など) = 全部「画像なし」の印で一覧は出す (画像は「あれば嬉しい」もの)。CSV には入れない。
 */
import { imagesFor } from '../picking/images.js';

export const LIST_IMAGE_QUEUE_MAX = 50;   // 1 回に積む上限 (このページの商品のうち)
// 1 回のジョブで楽天に投げるコードの上限 = details-bulk 1 回ぶん (50 件)。楽天の商品管理番号が分からない商品は候補を最大 5 つ投げて確かめるので、
//   SKU の数だけで抑えると bulk を何回も呼んでピッキング取込の画像のキューを塞ぐ (Codex 10/17 Medium)。収まらない商品は次に開いたとき
export const LIST_IMAGE_MAX_CODES = 50;
// 「楽天に無い」を取り直すまで 7 日 (ピッキングは翌日)。マスタには楽天に出していない商品 (Amazon だけ・セット) が多い = 毎日聞き直さない。
//   ピッキング・入荷受付などが先に取り直せば、同じキャッシュなのでこちらにもすぐ出る
export const LIST_IMAGE_NOT_FOUND_RETRY_MS = 7 * 24 * 3600e3;
const LANE = 'master-edit-list';

let testDeps = null;
/** 試験用: 楽天の呼び出し (ensureImagesFor の deps = loadMaps / fetchDetails) を差し替える。null = 本物 */
export function __setListImageDeps(d) { testDeps = d || null; }

const key = (code) => String(code ?? '').trim().toLowerCase();
/** 楽天につながる環境か (rakuten-rms-proxy が要る env の入口)。無ければ積まない */
const rmsConfigured = () => !!String(process.env.WAREHOUSE_URL || '').trim();

/**
 * このページの行の画像。戻り値 = 行のコード → { url, state } を引く関数 (読めない = いつも { url: null, state: 'none' })。
 *   state: ok / pending (取得中) / not_found (楽天に画像なし) / error (取れなかった・あとで取り直す) / uncached (まだ取っていない) / none (読めない)
 */
export function listImages(rows, { queue = true } = {}) {
  const NONE = Object.freeze({ url: null, state: 'none' });
  let map = null;
  try {
    map = imagesFor((rows || []).map((r) => r && r.code).filter(Boolean), {
      queueMax: queue ? LIST_IMAGE_QUEUE_MAX : 0,
      canQueue: queue && (!!testDeps || rmsConfigured()),
      label: 'マスタの入力 一覧',
      lane: LANE,
      notFoundRetryMs: LIST_IMAGE_NOT_FOUND_RETRY_MS,
      maxCodes: LIST_IMAGE_MAX_CODES,
      deps: testDeps || undefined,
    });
  } catch (e) {
    console.warn('[master-edit] 一覧の画像を読めませんでした (画像なしで出す):', e.message);
  }
  return (code) => (map && map.get(key(code))) || NONE;
}

/** 画像の欄の言葉 (title・読み上げ)。url がある = 名前を alt に使う (ここは画像の無いときの言葉) */
export const IMAGE_STATE_WORDS = Object.freeze({
  pending: '楽天の画像を取得中 (開き直すと出ます)',
  not_found: '楽天に画像がありません',
  error: '楽天の画像を取れませんでした (あとで取り直します)',
  uncached: '楽天の画像をまだ取っていません (開き直すと取りに行きます)',
  none: '画像を読めません',
});
