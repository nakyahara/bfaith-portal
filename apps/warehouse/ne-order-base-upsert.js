/**
 * ne-order-base-upsert — raw_ne_order_base (NE受注ベース = 伝票粒度) への UPSERT 共通ヘルパー
 *
 * raw_ne_orders (明細行粒度) と同じ思想:
 *   ・PK (伝票番号) 衝突時、状態系だけを UPDATE
 *   ・識別系 (受注番号 / 受注日 / 店舗コード) は初回スナップショットを保持
 *   ・「実際に値が変わった行だけ」UPDATE する (毎日の再取込で全行を書き換えない = WAL 肥大防止)
 *   ・NULL 安全比較のため IS NOT を使用
 *
 * 配送方法 (配送方法ID/名) と 出荷確定日 は後から付く・後から変わる列なので必ず更新対象に入れる。
 * 現場で配送方法を変えた場合も、取込窓に入っていれば次回同期で追随する。
 */

const COLS = [
  '伝票番号', '受注番号', '店舗コード', '受注日', '出荷確定日',
  '受注状態区分', '受注状態', 'キャンセル区分', '受注キャンセル日',
  '配送方法ID', '配送方法名', '送り状番号', 'synced_at',
];

// 衝突時に更新する列 (状態系 + 配送方法系)。識別系は含めない。
const UPDATE_COLS = [
  '出荷確定日', '受注状態区分', '受注状態', 'キャンセル区分', '受注キャンセル日',
  '配送方法ID', '配送方法名', '送り状番号',
];

const UPSERT_SQL = `
  INSERT INTO raw_ne_order_base (${COLS.join(', ')})
  VALUES (${COLS.map(() => '?').join(',')})
  ON CONFLICT(伝票番号) DO UPDATE SET
    ${UPDATE_COLS.map(c => `${c} = excluded.${c}`).join(',\n    ')},
    synced_at = excluded.synced_at
  WHERE ${UPDATE_COLS.map(c => `raw_ne_order_base.${c} IS NOT excluded.${c}`).join('\n     OR ')}
`;

/**
 * NE 受注ベース API の 1 レコードを raw_ne_order_base の行に変換する。
 * fields に含めなかった項目は undefined で来るため、既存値を消さないよう null ではなく
 * 「取れた値だけ」を入れる責務は呼び出し側 (ここでは素直に空文字/null 化する)。
 *
 * @param {Record<string, any>} item NE API のレコード (receive_order_* キー)
 * @returns {{伝票番号: string} & Record<string, any> | null} 伝票番号が無ければ null
 */
export function toOrderBaseRow(item) {
  const denpyo = item.receive_order_id || '';
  if (!denpyo) return null;
  return {
    伝票番号: String(denpyo),
    受注番号: item.receive_order_shop_cut_form_id || '',
    店舗コード: item.receive_order_shop_id || '',
    受注日: item.receive_order_date || '',
    出荷確定日: item.receive_order_send_date || '',
    受注状態区分: item.receive_order_order_status_id || '',
    受注状態: item.receive_order_order_status_name || '',
    // NE API は非キャンセル受注に cancel_type_id='0' (文字列) を返す。'0' は JS で truthy なので
    // 素の truthy 判定だと全件が「キャンセル」に誤ラベルされる (raw_ne_orders 側で 2026-07-06 に
    // 実際に起きた事故と同じ罠)。
    キャンセル区分: (item.receive_order_cancel_type_id && item.receive_order_cancel_type_id !== '0')
      ? 'キャンセル' : '有効',
    受注キャンセル日: item.receive_order_cancel_date || '',
    配送方法ID: item.receive_order_delivery_id || '',
    配送方法名: item.receive_order_delivery_name || '',
    送り状番号: item.receive_order_delivery_cut_form_id || '',
  };
}

/**
 * @param {import('better-sqlite3').Database} db
 * @returns {(row: Record<string, any>, syncedAt: string) => 'inserted'|'updated'|'unchanged'}
 */
export function makeNeOrderBaseUpserter(db) {
  const upsertStmt = db.prepare(UPSERT_SQL);
  const existsStmt = db.prepare('SELECT 1 AS x FROM raw_ne_order_base WHERE 伝票番号 = ?');
  return function upsertOrderBase(row, syncedAt) {
    const existed = existsStmt.get(row.伝票番号) !== undefined;
    const info = upsertStmt.run(
      row.伝票番号, row.受注番号, row.店舗コード, row.受注日, row.出荷確定日,
      row.受注状態区分, row.受注状態, row.キャンセル区分, row.受注キャンセル日,
      row.配送方法ID, row.配送方法名, row.送り状番号, syncedAt
    );
    if (!existed) return 'inserted';
    return info.changes > 0 ? 'updated' : 'unchanged';
  };
}

/** NE 受注ベース API に渡す fields (配送方法つき)。取得系スクリプトで共有する */
export const NE_ORDER_BASE_FIELDS = [
  'receive_order_id',
  'receive_order_shop_cut_form_id',
  'receive_order_shop_id',
  'receive_order_date',
  'receive_order_send_date',
  'receive_order_order_status_id',
  'receive_order_order_status_name',
  'receive_order_cancel_type_id',
  'receive_order_cancel_date',
  'receive_order_delivery_id',
  'receive_order_delivery_name',
  'receive_order_delivery_cut_form_id',
].join(',');
