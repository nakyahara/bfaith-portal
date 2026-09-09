/**
 * Amazon Easy Ship の配送料 (関西発・税込) — 想定利益の FBM 用
 *
 * 🚨 中原さん 2026-09-09:「Amazon の FBM は Easy Ship 料金なんだよね」。
 *    これまで FBM も**自社の送料マスタ (ヤマト等)** で計算していたので、
 *    実際に請求される Easy Ship 料金とずれていた。
 *
 * 🚨 **これは想定であって請求額ではない**。中原さん:「実際にどれを請求されているかは
 *    注文履歴をみないとわからない」。画面にもそう書く (§9.1)。
 *
 * 🚨 **宛先は関東に固定する** (中原さん 2026-09-09)。標準シナリオ (§3) は
 *    1注文・1個で宛先を決めていないが、Easy Ship は届け先地域で金額が変わる。
 *    実績の宛先分布で重み付けすると「販売実績を使わない」という前提 (§1) を崩すので、
 *    代表地域を 1 つ決める。**どの地域で計算したかは行に残す** (easyship_region)。
 *
 * 料率マスタと同じ扱いにする理由: 年に数回しか変わらず、miniPC と Render で
 * 必ず同じ値でなければならない。DB に置くと片側だけ古い状態が作れてしまう。
 * 変えたときは **EASYSHIP_RATE_VERSION を必ず上げる** (行に残るので後から追える)。
 */

/** 料金表の版。表を変えたら必ず上げる (input_snapshot に残る) */
export const EASYSHIP_RATE_VERSION = 'kansai_20260909';

/** 発地。料金表は発地ごとに違う (いまは関西倉庫のみ) */
export const EASYSHIP_ORIGIN = '関西';

/** 標準シナリオの宛先 (中原さん 2026-09-09) */
export const EASYSHIP_DEFAULT_REGION = '関東';

/** 料金表の地域列 (表の並びのまま) */
export const EASYSHIP_REGIONS = [
  '北海道', '東北', '関東', '信越', '北陸', '中部', '関西', '中国', '四国', '九州', '沖縄',
];

/** 全国一律の行を作る (メールサイズ・サイズ50) */
function flat(yen) {
  return Object.fromEntries(EASYSHIP_REGIONS.map((r) => [r, yen]));
}

/**
 * サイズ区分 → 地域別の配送料 (**税込・円**)。
 * 🚨 表のとおりに写す。おかしく見える値も直さない
 *    (サイズ120 の関東 728 円は サイズ100 の 748 円より安い。表がそうなっている)
 */
export const EASYSHIP_RATES = {
  MAIL: {
    label: 'メールサイズ', maxDimension: 'L34cm×W25cm×H3.5cm', maxWeightKg: 1,
    byRegion: flat(185),
  },
  SIZE_50: {
    label: 'サイズ50', maxDimension: '50cm', maxWeightKg: 5,
    byRegion: flat(425),
  },
  SIZE_60: {
    label: 'サイズ60', maxDimension: '60cm', maxWeightKg: 10,
    byRegion: {
      北海道: 779, 東北: 592, 関東: 430, 信越: 536, 北陸: 536, 中部: 510,
      関西: 430, 中国: 510, 四国: 510, 九州: 536, 沖縄: 913,
    },
  },
  SIZE_80: {
    label: 'サイズ80', maxDimension: '80cm', maxWeightKg: 10,
    byRegion: {
      北海道: 916, 東北: 668, 関東: 535, 信越: 588, 北陸: 588, 中部: 588,
      関西: 509, 中国: 588, 四国: 588, 九州: 588, 沖縄: 1044,
    },
  },
  SIZE_100: {
    label: 'サイズ100', maxDimension: '100cm', maxWeightKg: 10,
    byRegion: {
      北海道: 1202, 東北: 935, 関東: 748, 信越: 748, 北陸: 748, 中部: 748,
      関西: 668, 中国: 748, 四国: 748, 九州: 748, 沖縄: 1470,
    },
  },
  SIZE_120: {
    label: 'サイズ120', maxDimension: '120cm', maxWeightKg: 14.99,
    byRegion: {
      北海道: 1270, 東北: 1016, 関東: 728, 信越: 728, 北陸: 728, 中部: 728,
      関西: 647, 中国: 728, 四国: 728, 九州: 728, 沖縄: 1733,
    },
  },
  SIZE_140: {
    label: 'サイズ140', maxDimension: '140cm', maxWeightKg: 14.99,
    byRegion: {
      北海道: 1617, 東北: 1294, 関東: 924, 信越: 924, 北陸: 924, 中部: 924,
      関西: 785, 中国: 924, 四国: 924, 九州: 924, 沖縄: 2079,
    },
  },
  SIZE_160: {
    label: 'サイズ160', maxDimension: '160cm', maxWeightKg: 14.99,
    byRegion: {
      北海道: 1848, 東北: 1432, 関東: 1098, 信越: 1098, 北陸: 1098, 中部: 1098,
      関西: 924, 中国: 1098, 四国: 1098, 九州: 1098, 沖縄: 2426,
    },
  },
};

/**
 * 梱包サイズマスターのコード / 表示名 を、この表のキーに揃える。
 *
 * 🚨 **読めないコードは推測しない**。`es_package_size_master.package_size_code` は
 *    自由入力 (画面のヒントは「例: SIZE_60」) なので、書き方の揺れは吸収するが、
 *    どのサイズか決められないものは null を返して「判定できない」に落とす。
 *    近いサイズに寄せると、静かに違う送料で利益を出すことになる。
 *
 * 受ける形の例: SIZE_60 / size60 / 60 / サイズ60 / 60サイズ (26 cm x 19 cm x 11 cm)
 *              MAIL / メールサイズ / メール便
 */
export function normalizeEasyshipSize(codeOrLabel) {
  const raw = String(codeOrLabel ?? '').trim();
  if (!raw) return null;
  // 🚨 長音「ー」は消さない。消すと「メール便」が「メル便」になって読めなくなる
  const s = raw.toUpperCase().replace(/[\s_－-]/g, '');

  // メールサイズ (数字を持たないので先に見る)
  if (/^(MAIL|メール)/.test(s)) return 'MAIL';

  // 最初に出てくる数字をサイズとみなす。「60サイズ (26 cm x 19 cm x 11 cm)」の 26 を拾わないよう、
  // 寸法の括弧より前だけを見る
  const head = raw.split(/[(（]/)[0];
  const m = String(head).match(/(\d{2,3})/);
  if (!m) return null;
  const key = `SIZE_${m[1]}`;
  return Object.hasOwn(EASYSHIP_RATES, key) ? key : null;
}

/**
 * 梱包サイズマスターの 1 件 → 料金表のキー。
 *
 * 🚨 **コードと表示名の両方を見る**。コードは人が付ける自由入力なので古い書き方が残りうるし、
 *    表示名 (Easy Ship 画面の全文) だけ入っていることもある。片方しか見ないと、
 *    ちゃんと登録してある出品を「サイズが読めない」で落としてしまう (Codex P2 2026-09-09)。
 *
 * 🚨 **両方読めて食い違うときは決めない**。どちらが正しいか分からないまま片方を採ると、
 *    違うサイズの送料で利益を出すことになる。
 *    ※ このガードがあるので、**どちらを先に見るかは結果に影響しない** (両方読めれば必ず同じ)。
 *      「表示名が正」といった優先順を書かないこと。試験で守れない約束になる
 */
export function resolveEasyshipSize({ sizeCode, sizeLabel } = {}) {
  const byLabel = normalizeEasyshipSize(sizeLabel);
  const byCode = normalizeEasyshipSize(sizeCode);
  if (byLabel && byCode && byLabel !== byCode) return null;   // 食い違い = 決めない
  return byLabel || byCode || null;
}

/**
 * サイズ区分と宛先地域から配送料 (税込) を引く。
 * @returns {{ok:true, sizeCode:string, label:string, region:string, feeInclTax:number}
 *          |{ok:false, reason:'easyship_size_unmapped'|'easyship_region_unknown'}}
 */
export function easyshipFeeInclTax(codeOrLabel, region = EASYSHIP_DEFAULT_REGION) {
  const sizeCode = normalizeEasyshipSize(codeOrLabel);
  if (!sizeCode) return { ok: false, reason: 'easyship_size_unmapped' };
  const entry = EASYSHIP_RATES[sizeCode];
  const fee = entry.byRegion[region];
  if (!Number.isFinite(fee)) return { ok: false, reason: 'easyship_region_unknown' };
  return { ok: true, sizeCode, label: entry.label, region, feeInclTax: fee };
}
