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
 *   - UTF-8 / ヘッダー行 + 空行 + データ行 (この形で実運用の取込に通っている)
 *   - 1箱1伝票 (個数=1)。福山の運賃は10個口までと11個口以上で表が変わるが、
 *     現行運用は例外なく1箱1伝票なのでそれに合わせる
 */
import { isRegisteredFc } from './fukutsu-master.js';

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
 * @param {object} shipment {shipmentConfirmationId, fcCode, boxCount, address?}
 * @param {string} ymd 出荷日 YYYYMMDD
 */
export function buildRowsForShipment(shipment, ymd) {
  const fc = String(shipment.fcCode ?? '').trim().toUpperCase();
  const kanri = String(shipment.shipmentConfirmationId ?? '').trim().toUpperCase();
  const n = Number(shipment.boxCount);

  if (!fc) throw cErr('宛先FCコードが取れませんでした');
  if (!/^[A-Z0-9]{1,16}$/.test(kanri)) throw cErr(`納品番号が不正です: ${kanri}`);
  if (!Number.isInteger(n) || n < 1) throw cErr(`${kanri}: 輸送箱の数が取れませんでした`);

  const registered = isRegisteredFc(fc);
  const rows = [];
  for (let i = 0; i < n; i++) {
    const c = new Array(26).fill('');
    c[COL.荷受人コード] = fc;
    c[COL.荷送人コード] = SENDER_CODE;
    c[COL.個数] = '1';           // 1箱1伝票
    c[COL.元払区分] = '1';        // 元払
    c[COL.出荷日付] = ymd;
    c[COL.お客様管理番号] = kanri; // ⭐これが追跡番号の突合の鍵
    if (!registered) {
      // マスタ未登録のFC = 住所を直接書いて伝票を出せるようにする (出荷を止めない)
      const a = shipment.address ?? {};
      const [a1, a2, a3] = splitAddress([a.stateOrProvinceCode, a.city, a.addressLine1].filter(Boolean).join(''));
      c[COL.住所1] = a1; c[COL.住所2] = a2; c[COL.住所3] = a3;
      c[COL.名前1] = `Amazon.co.jp ${fc}`.slice(0, 20);
      c[COL.郵便番号] = normPostal(a.postalCode);
    }
    rows.push(c);
  }
  return { rows, registered };
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
  if (!/^\d{8}$/.test(String(ymd ?? ''))) throw cErr(`出荷日の書式が不正です: ${ymd}`);
  if (!shipments?.length) throw cErr('対象の納品がありません');

  const all = [];
  const detail = [];
  for (const s of shipments) {
    const { rows, registered } = buildRowsForShipment(s, ymd);
    all.push(...rows);
    detail.push({
      納品番号: s.shipmentConfirmationId, fcCode: s.fcCode, 箱数: rows.length,
      宛先: registered ? 'マスタ登録済み (コードのみ)' : '⚠️ マスタ未登録のため住所を出力',
    });
  }

  // 現行GASと同じ形: ヘッダー + 空行 + データ (この形で取込に通っている実績がある)
  const lines = [HEADER.map(q).join(','), new Array(26).fill('').join(',')];
  for (const r of all) lines.push(r.map(q).join(','));

  return {
    csv: lines.join('\n') + '\n',
    summary: { 出荷日: ymd, 伝票枚数: all.length, 納品数: shipments.length, detail },
  };
}
