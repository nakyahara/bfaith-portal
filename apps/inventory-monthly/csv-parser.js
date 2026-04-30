/**
 * csv-parser.js — 月末棚卸しの3種類のCSVを解析する
 *
 *  1) 発注推奨レポート (Amazon SP-API GET_RESTOCK_INVENTORY_RECOMMENDATIONS_REPORT)
 *     Shift_JIS、28列。Merchant SKU を商品識別キーに、在庫あり / 進行中 / 出荷済み / 受領中
 *     を抽出する。FBA倉庫内 = 在庫あり。FBA輸送中 = 進行中 + 出荷済み + 受領中。
 *
 *  2) 自社倉庫CSV (NextEngine 棚卸しエクスポート: jishazaikotanaorosi.csv)
 *     Shift_JIS、A列=商品コード、E列=在庫数。原価はマスタ集約のため CSV内の値は使わない。
 *
 *  3) 米国FBA在庫CSV (Amazon US の Manage Inventory 等。Phase 1 では金額直接入力)
 *     現状は金額のみ集計するため、parseUsFba は数量と金額のサマリだけを返す。
 */
import iconv from 'iconv-lite';

function parseShiftJisCsv(buf) {
  let text;
  if (buf[0] === 0xEF && buf[1] === 0xBB && buf[2] === 0xBF) {
    text = buf.toString('utf-8').slice(1);
  } else {
    text = iconv.decode(buf, 'Shift_JIS');
  }

  // RFC 4180 準拠の最小限パーサ。
  //   - 引用フィールド内の `""` はエスケープされたダブルクオートとして 1 個の `"` に展開する
  //   - 引用フィールド内では改行も値の一部として保持する（複数行レコード対応）
  //   - 引用外の改行はレコード区切り
  const rows = [];
  let row = [];
  let cur = '';
  let inQuotes = false;
  const len = text.length;
  for (let i = 0; i < len; i++) {
    const ch = text[i];
    if (inQuotes) {
      if (ch === '"') {
        if (text[i + 1] === '"') { // escaped quote
          cur += '"';
          i++;
        } else {
          inQuotes = false;
        }
      } else {
        cur += ch;
      }
      continue;
    }
    if (ch === '"') { inQuotes = true; continue; }
    if (ch === ',') { row.push(cur.trim()); cur = ''; continue; }
    if (ch === '\r') { continue; }
    if (ch === '\n') {
      row.push(cur.trim());
      // 完全な空行は捨てる
      if (!(row.length === 1 && row[0] === '')) rows.push(row);
      row = [];
      cur = '';
      continue;
    }
    cur += ch;
  }
  if (cur !== '' || row.length > 0) {
    row.push(cur.trim());
    if (!(row.length === 1 && row[0] === '')) rows.push(row);
  }
  return rows;
}

const num = v => { const n = parseInt((v || '').replace(/,/g, '')); return isNaN(n) ? 0 : n; };

/**
 * 発注推奨レポートCSV → [{ seller_sku, asin, fba_warehouse, fba_inbound }]
 *
 * 列位置を固定 index で読むと、Amazon が将来列順を変えた場合に
 * 関係ない列を集計してしまい在庫評価額が誤る。よってヘッダー名で
 * 列を解決し、必須カラムが全部揃っているか厳格に検証する。
 */
// 評価額の算出に必須の列（無ければ集計が破綻するので拒否）
//
// FBA倉庫内在庫 (fba_warehouse) =
//   在庫あり + FC移管中 + (入出荷作業中)FC処理中 + (入出荷作業中)出荷待ち
// FBA輸送中 (fba_inbound) =
//   進行中 + 出荷済み + 受領中
// 「販売不可」は含めない（毀損品・要返品など、棚卸し対象外）
const RESTOCK_REQUIRED = {
  seller_sku: ['Merchant SKU'],
  warehouse: ['在庫あり', 'Available'],
  fc_transfer: ['FC移管中', 'FC Transfer'],
  fc_processing: ['入出荷作業中 - FC処理中', '入出荷作業中-FC処理中', 'FC Processing'],
  customer_order: ['入出荷作業中 - 出荷待ち', '入出荷作業中-出荷待ち', 'Customer Order'],
  working: ['進行中', 'Working'],
  shipped: ['出荷済み', 'Shipped'],
  receiving: ['受領中', 'Receiving'],
};
// 表示用のみ。欠けていても評価額は計算できるので、見つからなければ空文字で続行する
const RESTOCK_OPTIONAL = {
  asin: ['ASIN'],
  product_name: ['商品名', 'Product Name'],
};

function findHeaderIndex(header, candidates) {
  for (let i = 0; i < header.length; i++) {
    const h = (header[i] || '').trim();
    if (candidates.some(c => h === c || h.toLowerCase() === c.toLowerCase())) return i;
  }
  return -1;
}

export function parseRestockReport(buf) {
  const rows = parseShiftJisCsv(buf);
  if (rows.length < 2) throw new Error('CSVが空です');

  const header = rows[0];
  const idx = {};
  // 必須列は missing 検証
  const missing = [];
  for (const [key, cand] of Object.entries(RESTOCK_REQUIRED)) {
    const i = findHeaderIndex(header, cand);
    if (i < 0) missing.push(key);
    idx[key] = i;
  }
  if (missing.length > 0) {
    throw new Error(`発注推奨レポートに必須列が見つかりません: ${missing.join(', ')}（ヘッダー: ${header.slice(0, 10).join(' | ')}...）`);
  }
  // 任意列は見つからなくても続行（表示用途のみ）
  for (const [key, cand] of Object.entries(RESTOCK_OPTIONAL)) {
    idx[key] = findHeaderIndex(header, cand);
  }

  const safeGet = (r, i) => (i >= 0 ? (r[i] || '').trim() : '');

  const out = [];
  for (let i = 1; i < rows.length; i++) {
    const r = rows[i];
    const seller_sku = safeGet(r, idx.seller_sku);
    if (!seller_sku) continue;
    // FBA倉庫内在庫: FC内に物理的に存在する全ての数量を合算する
    //   在庫あり: 即時販売可能
    //   FC移管中: FC間の移管中（自社所有・FBA内）
    //   FC処理中: 入出荷の作業中
    //   出荷待ち: 受注済みで顧客に発送待ち（自社所有・FBA内）
    const fba_warehouse =
      num(r[idx.warehouse]) +
      num(r[idx.fc_transfer]) +
      num(r[idx.fc_processing]) +
      num(r[idx.customer_order]);
    // FBA輸送中: FCにまだ届いていない / 受領処理中の数量
    const fba_inbound = num(r[idx.working]) + num(r[idx.shipped]) + num(r[idx.receiving]);
    if (fba_warehouse === 0 && fba_inbound === 0) continue;
    out.push({
      seller_sku,
      asin: safeGet(r, idx.asin),
      product_name: safeGet(r, idx.product_name),
      fba_warehouse,
      fba_inbound,
    });
  }
  return out;
}

/**
 * 1行が自社倉庫CSVの「データ行」として有効か判定する。
 *   - A列(商品コード): 非空文字
 *   - E列(在庫数): 整数のみ受け付ける（小数は in_snapshot_detail.数量 INTEGER と
 *     不一致になり、parseInt で静かに切り捨てられる事故を起こすため明示的に弾く）
 * これでヘッダー行・説明行・小数を含む不整合行を漏れなく除外できる。
 */
function isOwnWarehouseDataRow(r) {
  if (!r || r.length < 5) return false;
  const code = (r[0] || '').trim();
  if (!code) return false;
  const qtyRaw = (r[4] || '').replace(/,/g, '').trim();
  if (qtyRaw === '') return false;
  return /^-?\d+$/.test(qtyRaw); // 整数のみ
}

/** 自社倉庫CSV (jishazaikotanaorosi.csv) → [{ 商品コード, 在庫数 }] */
export function parseOwnWarehouse(buf) {
  const rows = parseShiftJisCsv(buf);
  if (rows.length === 0) throw new Error('CSVが空です');

  // 小数点を含む在庫数行は parseInt で切り捨てられて在庫を過小評価する事故を起こすため、
  // ヘッダー判定で弾けず "明らかに数量らしいが小数" な行が出てきたら error にする。
  for (const r of rows) {
    if (!r || r.length < 5) continue;
    const code = (r[0] || '').trim();
    if (!code) continue;
    const qtyRaw = (r[4] || '').replace(/,/g, '').trim();
    if (qtyRaw === '') continue;
    if (/^-?\d+\.\d+$/.test(qtyRaw)) {
      throw new Error(`自社倉庫CSVに小数の在庫数が含まれています（商品コード=${code}, 数量=${qtyRaw}）。整数に揃えてから再度アップロードしてください。`);
    }
  }

  const out = [];
  for (const r of rows) {
    if (!isOwnWarehouseDataRow(r)) continue;
    const code = r[0].trim();
    const qty = parseInt(r[4].replace(/,/g, ''), 10);
    if (qty === 0) continue;
    out.push({
      商品コード: code.toLowerCase(),
      商品名: (r[1] || '').trim(),
      在庫数: qty,
    });
  }
  if (out.length === 0) {
    throw new Error('自社倉庫CSVから有効なデータ行を抽出できませんでした（A列=商品コード、E列=在庫数の形式を確認してください）');
  }
  return out;
}
