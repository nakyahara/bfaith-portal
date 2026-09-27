/**
 * 米国用の NE 受注 CSV (日本の /api/export-ne-csv と同じ 61 列・Shift_JIS)。設計 = 米国FBA納品アプリ_設計方針 §12 / §12.6
 *
 * 日本との違い (中原さん 9/27):
 *   - 発送先 = フォワーダー (GBFF) センコー株式会社 印西第二LC内ECMSジャパン・会員 ID 2164 (2026-03 からの住所)
 *   - 発送方法 = 福山通運istar2 (NE の名前。packing-dispatch の一覧・NE の実データと一致)
 *   - 店舗伝票番号 = USFBA + YYYYMMDDHHmmss + 台帳の連番 (日本の FBA + 日時と混ざらない・同じ秒でも重ならない)
 *   - 受注名 = YYYYMMDD米国FBA納品。受注者の欄は日本と同じ自社
 * NE に取り込むときの店舗は日本の FBA 納品と同じ (店舗 15)。前回 (7/28) の米国分の伝票もそこに入っていた。
 */
import iconv from 'iconv-lite';

export const FORWARDER = {
  zip: '2701369',
  address1: '千葉県印西市鹿黒南1-2',
  address2: 'グッドマンビジネスパーク・ウエスト5階 2164',   // 2164 = 会員 ID (ShipperMaker ID)。フォワーダーの案内どおり住所の末尾に書く
  name: '(GBFF)センコー株式会社 印西第二LC内ECMSジャパン',
  tel: '052-325-2444',
};
export const SHIPPING_METHOD = '福山通運istar2';
// 受注者 (自社)。日本の FBA 伝票と同じ値 (apps/fba-replenishment/router.js /api/export-ne-csv)
const ORDERER = { zip: '5640038', address1: '大阪府吹田市南清和園町41‐36', address2: 'Amazon倉庫', tel: '09085325647' };

export const HEADERS = [
  '店舗伝票番号', '受注日', '受注郵便番号', '受注住所１', '受注住所２', '受注名', '受注名カナ',
  '受注電話番号', '受注メールアドレス', '発送郵便番号', '発送先住所１', '発送先住所２', '発送先名',
  '発送先カナ', '発送電話番号', '支払方法', '発送方法', '商品計', '税金', '発送料', '手数料',
  '手数料(0%対象)', '手数料(8%対象)', '手数料(10%対象)', 'ポイント', 'ポイント(0%対象)',
  'ポイント(8%対象)', 'ポイント(10%対象)', 'ポイント(按分)', 'ポイント(支払い)', 'その他費用',
  'その他費用(0%対象)', 'その他費用(8%対象)', 'その他費用(10%対象)', 'クーポン割引額',
  'クーポン割引額(0%対象)', 'クーポン割引額(8%対象)', 'クーポン割引額(10%対象)',
  'クーポン割引額(按分)', '請求金額(0%対象)', '請求金額(8%対象)', '請求額に対する税額(8%対象)',
  '請求金額(10%対象)', '請求額に対する税額(10%対象)', '合計金額', 'ギフトフラグ', '時間帯指定',
  '日付指定', '作業者欄', '備考', '商品名', '商品コード', '商品価格', '受注数量', '商品オプション',
  '出荷済フラグ', '顧客区分', '顧客コード', '消費税率（%）', 'のし', 'ラッピング',
];

function csvEscape(val) {
  const s = String(val ?? '');
  return (s.includes(',') || s.includes('"') || s.includes('\n') || s.includes('\r')) ? '"' + s.replace(/"/g, '""') + '"' : s;
}
const pad = (n, w = 2) => String(n).padStart(w, '0');

/** 日本時間の YYYYMMDD / HHmmss */
export function jstStamp(now) {
  const j = new Date(now.getTime() + 9 * 3600e3);
  return { date: `${j.getUTCFullYear()}${pad(j.getUTCMonth() + 1)}${pad(j.getUTCDate())}`, time: `${pad(j.getUTCHours())}${pad(j.getUTCMinutes())}${pad(j.getUTCSeconds())}` };
}

export function makeOrderNo(now, seq) {
  const { date, time } = jstStamp(now);
  return `USFBA${date}${time}-${seq}`;
}

/**
 * @param {{ code: string, qty: number, name?: string }[]} units  構成品 (NE 商品コード) ごとの個数。コード順に並べて出す
 * @returns {{ orderNo: string, csv: Buffer, filename: string }}
 */
export function buildUsNeCsv(units, { now, seq }) {
  if (!Array.isArray(units) || units.length === 0) throw new Error('NE 商品コードに直せる行がありません');
  const { date } = jstStamp(now);
  const orderNo = makeOrderNo(now, seq);
  const orderName = `${date}米国FBA納品`;
  const rows = [HEADERS.map(csvEscape).join(',')];
  for (const u of [...units].sort((a, b) => a.code.localeCompare(b.code))) {
    if (!u.code || !Number.isSafeInteger(u.qty) || u.qty <= 0) throw new Error(`構成品の行がおかしい: ${JSON.stringify(u)}`);
    const row = new Array(HEADERS.length).fill('');
    row[0] = orderNo;
    row[1] = date;
    row[2] = ORDERER.zip; row[3] = ORDERER.address1; row[4] = ORDERER.address2;
    row[5] = orderName;
    row[7] = ORDERER.tel;
    row[9] = FORWARDER.zip; row[10] = FORWARDER.address1; row[11] = FORWARDER.address2; row[12] = FORWARDER.name;
    row[14] = FORWARDER.tel;
    row[15] = '支払済';
    row[16] = SHIPPING_METHOD;
    row[17] = '0';
    row[44] = '0';
    row[45] = '0';
    row[49] = '米国FBA納品用の伝票です (フォワーダー ECMSジャパン宛て)。フォワーダーへ出荷した日に出荷確定してください。';
    row[50] = u.name || '';
    row[51] = u.code;
    row[52] = '0';
    row[53] = String(u.qty);
    row[55] = '0';
    row[56] = '0';
    rows.push(row.map(csvEscape).join(','));
  }
  const text = rows.join('\r\n');
  const csv = iconv.encode(text, 'Shift_JIS');
  // 往復で化ける文字 (Shift_JIS に無い文字) があれば出さない (住所・商品名が「?」で NE に入るのを防ぐ)
  if (iconv.decode(csv, 'Shift_JIS') !== text) throw new Error('Shift_JIS に直せない文字がある (商品名・住所を確かめてください)');
  return { orderNo, csv, filename: `hanyo-jyuchu_invoice_US_${date}_${seq}.csv` };
}
