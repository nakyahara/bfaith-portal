/**
 * FBA納品プラン → 福山通運 iS-2 の「CSVエントリ」用 26列CSV を組み立てる。
 *
 * ⭐これが入ると何が変わるか:
 *   U列「お客様管理番号」に **Amazonの納品番号 (FBA15GG2MM9B)** を入れる。
 *   すると出荷実績CSV (34列) の AF列に同じ値が返ってくるので、
 *   追跡番号の投入時に **FCコードや箱数からの推測が一切要らなくなる**。
 *   同じFC宛の納品が同時に複数あっても取り違えない (実際に発生する)。
 *   あわせて、人が宛先コードと箱数を手入力する転記もなくなる。
 *
 * 出力は現行GAS (fukutu.csv) と同じ形に揃える:
 *   - ヘッダー行 + データ行 (旧GASの「ヘッダー直後の空行」は iS-2 でエラー行になるので出さない・2026-08-25)
 *   - 🚨文字コードは **Shift_JIS** で出す (呼び出し側の責務)。UTF-8 だと iS-2 で住所が化ける (2026-08-25 実害)
 *   - 宛先は常に画面の住所を書く (iS-2 マスタに頼らない・2026-08-25)
 *   - 1箱1伝票 (個数=1)。福山の運賃は10個口までと11個口以上で表が変わるが、
 *     現行運用は例外なく1箱1伝票なのでそれに合わせる
 */

/** 付録1-1-1「CSVエントリフォーマット (記事3行)」26列 */
export const HEADER = [
  '荷受人ｺｰﾄﾞ（お届け先ｺｰﾄﾞ）', '電話番号', '住所', '住所２ ', '住所３ ', '名前 ', '名前２ ', '郵便番号 ',
  '特殊計 ', '着店コード ', '荷送人ｺｰﾄﾞ(ご依頼主ｺｰﾄﾞ)', '個数', '才数', '重量',
  '輸送商品１', '輸送商品２', '品名記事１', '品名記事２', '品名記事３', '配達指定日 ',
  'お客様管理番号', '予備', '元払区分', '保険金額', '出荷日付', '登録日付',
];

export const COL = {
  荷受人コード: 0, 電話番号: 1, 住所1: 2, 住所2: 3, 住所3: 4, 名前1: 5, 名前2: 6, 郵便番号: 7,
  荷送人コード: 10, 個数: 11, 品名記事1: 16, お客様管理番号: 20, 元払区分: 22, 出荷日付: 24,
};

const SENDER_CODE = '0648607868'; // 荷送人コード (ご依頼主) — 現行GASと同じ固定値

/**
 * Amazon FC宛の電話番号。画面に出ていない唯一の項目。
 * iS-2 のお届け先マスタでは Amazon宛 74件のうち 70件がこの番号 (2026-08-07 実測)。
 */
const AMAZON_TEL = '(06)6333-4858';

/** YYYYMMDD が実在する日付か (2026年13月40日 のような値を弾く) */
export function isRealYmd(v) {
  const t = String(v ?? '');
  if (!/^\d{8}$/.test(t)) return false;
  const y = Number(t.slice(0, 4)), m = Number(t.slice(4, 6)), d = Number(t.slice(6, 8));
  const dt = new Date(Date.UTC(y, m - 1, d));
  return dt.getUTCFullYear() === y && dt.getUTCMonth() === m - 1 && dt.getUTCDate() === d;
}

export function cErr(message) {
  const e = new Error(message);
  e.code = 'FUKUTSU_CSV';
  return e;
}

/** 住所を20文字ずつ3つに割る (マニュアル: 各20文字・住所は合計60文字まで) */
export function splitAddress(s) {
  const t = String(s ?? '').replace(/\s+/g, ' ').trim();
  return [t.slice(0, 20), t.slice(20, 40), t.slice(40, 60)];
}

/** 郵便番号は「123-4567」か「1234567」。それ以外は空にして人に気づかせる */
export function normPostal(s) {
  const t = String(s ?? '').replace(/[^0-9-]/g, '');
  return /^\d{3}-?\d{4}$/.test(t) ? t : '';
}

/**
 * 納品1件ぶんの行を作る。
 * @param {object} shipment {shipmentConfirmationId, fcCode, boxCount, address} — address は必須 (画面の住所)
 * @param {string} ymd 出荷日 YYYYMMDD
 */
export function buildRowsForShipment(shipment, ymd) {
  const fc = String(shipment.fcCode ?? '').trim().toUpperCase();
  const kanri = String(shipment.shipmentConfirmationId ?? '').trim().toUpperCase();
  const n = Number(shipment.boxCount);

  if (!fc) throw cErr('宛先FCコードが取れませんでした');
  if (!/^[A-Z0-9]{1,16}$/.test(kanri)) throw cErr(`納品番号が不正です: ${kanri}`);
  if (!Number.isInteger(n) || n < 1) throw cErr(`${kanri}: 輸送箱の数が取れませんでした`);

  // 宛先 = 画面 (Seller Central) の住所をそのまま書く。iS-2 のマスタには頼らない。
  // 欠けたまま伝票を出すと届かないので必ず止める
  const a = shipment.address ?? {};
  const parts = [a.stateOrProvinceCode, a.city, a.addressLine1].filter(Boolean).join('');
  const post = normPostal(a.postalCode);
  if (!post) throw cErr(`${kanri} (${fc}): 郵便番号が読み取れません`);
  if (parts.replace(/\s/g, '').length < 6) throw cErr(`${kanri} (${fc}): 住所が読み取れません`);
  if (parts.length > 60) throw cErr(`${kanri} (${fc}): 住所が60文字を超えています (${parts.length}文字)。切り捨てると届きません`);
  const [a1, a2, a3] = splitAddress(parts);

  const rows = [];
  for (let i = 0; i < n; i++) {
    const c = new Array(26).fill('');
    c[COL.荷受人コード] = fc;
    c[COL.電話番号] = AMAZON_TEL;
    c[COL.住所1] = a1; c[COL.住所2] = a2; c[COL.住所3] = a3;
    c[COL.名前1] = `Amazon.co.jp ${fc}`.slice(0, 20);
    c[COL.郵便番号] = post;
    c[COL.荷送人コード] = SENDER_CODE;
    c[COL.個数] = '1';           // 1箱1伝票
    c[COL.元払区分] = '1';        // 元払
    c[COL.出荷日付] = ymd;
    c[COL.お客様管理番号] = kanri; // ⭐これが追跡番号の突合の鍵
    rows.push(c);
  }
  return { rows, address: parts, postalCode: post };
}

const q = (v) => {
  const s = String(v ?? '');
  return /[",\r\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
};

/**
 * 納品の配列からCSV本文を作る。
 * @param {Array} shipments [{shipmentConfirmationId, fcCode, boxCount, address}]
 * @param {string} ymd 出荷日 YYYYMMDD
 * @returns {{csv: string, summary: object}}
 */
export function buildFukutsuCsv(shipments, ymd) {
  if (!isRealYmd(ymd)) throw cErr(`出荷日が不正です: ${ymd}`);
  if (!shipments?.length) throw cErr('対象の納品がありません');

  const all = [];
  const detail = [];
  for (const s of shipments) {
    const { rows, address, postalCode } = buildRowsForShipment(s, ymd);
    all.push(...rows);
    detail.push({
      納品番号: s.shipmentConfirmationId, fcCode: s.fcCode, 箱数: rows.length,
      宛先: `〒${postalCode} ${address}`,
    });
  }

  // ヘッダー + データ。🚨旧GASにあった「ヘッダー直後の空行」は iS-2 でエラー1件になる (2026-08-25 実機) ので出さない
  const lines = [HEADER.map(q).join(',')];
  for (const r of all) lines.push(r.map(q).join(','));

  return {
    csv: lines.join('\n') + '\n',
    summary: { 出荷日: ymd, 伝票枚数: all.length, 納品数: shipments.length, detail },
  };
}
