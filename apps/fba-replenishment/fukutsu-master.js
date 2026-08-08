/**
 * 福山通運 iS-2「お届け先マスタ」に登録済みの Amazon FC コード。
 *
 * 26列のアップロードCSVは、荷受人コードだけ入れれば住所がマスタから引かれる。
 * ただし **Amazonは新しいFCを開く** ので、マスタに無いコードが来ることが必ずある。
 * その日に伝票が出せず出荷が止まるのを避けるため、
 * **未登録なら住所・郵便番号・名前をCSVへ直接書き出す** (マニュアル付録1:
 * 「[荷受人コード]または[電話番号・住所・名前・郵便番号]のどちらかが必須」)。
 *
 * よってこの一覧がズレても安全側に倒れる (未登録扱い = 住所を書く)。
 * 出典 = iS-2 からエクスポートしたお届け先マスタ (2026-08-07 時点で Amazon宛 74件)。
 *
 * 🚨 除外しているコード:
 *   TPFB — マスタ上の名前・住所が XHD1 のものになっており、このコードで伝票を出すと
 *          XHD1 へ届いてしまう。中原さんの判断で「無いもの」として扱う (2026-08-07)。
 */
export const REGISTERED_FC_CODES = new Set([
  'FSZ1', 'HND2', 'HND3', 'HND6', 'HND9', 'HSG1', 'KIX1', 'KIX2',
  'KIX3', 'KIX4', 'KIX5', 'KIX6', 'NGO2', 'NGO3', 'NRT1', 'NRT2',
  'NRT5', 'QCB1', 'QCB3', 'QCB4', 'QCB5', 'QCB6', 'TPB1', 'TPB2',
  'TPB3', 'TPB5', 'TPB6', 'TPB7', 'TPB8', 'TPF2', 'TPF3', 'TPF4',
  'TPF6', 'TPF9', 'TPFA', 'TPFC', 'TPFD', 'TPFI', 'TPX1', 'TPX2',
  'TPY1', 'TPY2', 'TPY3', 'TPY5', 'TPZ2', 'TPZ3', 'TPZ4', 'TPZ5',
  'TPZ6', 'TPZ7', 'TPZ8', 'TPZ9', 'TYO1', 'TYO2', 'TYO3', 'TYO4',
  'TYO6', 'TYO7', 'TYO8', 'TYO9', 'VJGT', 'VJNA', 'VJNB', 'VJNC',
  'VJND', 'VJNE', 'VJWB', 'XHD1', 'XHD4', 'XJE2', 'XJW1', 'XKX2',
  'XKX4',
]);

/** マスタに登録済みか (大文字小文字とゆらぎを吸収) */
export function isRegisteredFc(code) {
  return REGISTERED_FC_CODES.has(String(code ?? '').trim().toUpperCase());
}
