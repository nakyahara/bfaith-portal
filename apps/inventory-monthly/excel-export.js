/**
 * excel-export.js — 月末棚卸し履歴の Excel ダウンロード
 *
 * 旧 月末棚卸しデータ.xlsx の【集計データ】タブと近い形式で
 * サマリー + 区分別明細をブックに出力する。
 */
import ExcelJS from 'exceljs';

const CATEGORY_LABEL = {
  fba_warehouse: 'FBA倉庫内在庫',
  fba_inbound: 'FBA輸送中在庫',
  own_warehouse: '自社倉庫在庫',
  fba_us: '米国FBA在庫',
};

export async function exportSnapshotToXlsx(snap) {
  const wb = new ExcelJS.Workbook();
  wb.created = new Date();
  const s = snap.summary;

  // ── サマリーシート ──
  const sm = wb.addWorksheet('集計データ');
  sm.columns = [
    { header: '区分', key: 'label', width: 24 },
    { header: '金額（税抜）', key: 'amount', width: 20, style: { numFmt: '#,##0' } },
  ];
  sm.addRow({ label: 'FBA倉庫内在庫', amount: s.fba_warehouse });
  sm.addRow({ label: 'FBA輸送中在庫', amount: s.fba_inbound });
  sm.addRow({ label: '自社倉庫在庫', amount: s.own_warehouse });
  sm.addRow({ label: '米国FBA在庫', amount: s.fba_us });
  sm.addRow({ label: '発注後未着商品', amount: s.pending_orders });
  const totalRow = sm.addRow({ label: '合計', amount: s.total });
  totalRow.font = { bold: true };
  totalRow.eachCell(c => { c.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFFF3CD' } }; });
  sm.addRow({});
  sm.addRow({ label: '基準日', amount: s.snapshot_date });
  sm.addRow({ label: '作成日時', amount: s.created_at });

  // ── 区分毎の明細シート ──
  const grouped = new Map();
  for (const d of snap.details) {
    if (!grouped.has(d.category)) grouped.set(d.category, []);
    grouped.get(d.category).push(d);
  }
  for (const [cat, rows] of grouped) {
    const ws = wb.addWorksheet(CATEGORY_LABEL[cat] || cat);
    ws.columns = [
      { header: 'Amazon SKU', key: 'seller_sku', width: 22 },
      { header: '商品コード', key: 'code', width: 22 },
      { header: '商品名', key: 'name', width: 50 },
      { header: '数量', key: 'qty', width: 8, style: { numFmt: '#,##0' } },
      { header: '原価', key: 'cost', width: 12, style: { numFmt: '#,##0' } },
      { header: '金額', key: 'amount', width: 14, style: { numFmt: '#,##0' } },
      { header: '原価状態', key: 'status', width: 14 },
    ];
    ws.getRow(1).font = { bold: true };
    for (const d of rows) {
      ws.addRow({
        seller_sku: d.seller_sku || '',
        code: d.商品コード || '',
        name: d.商品名 || '',
        qty: d.数量,
        cost: d.原価,
        amount: d.金額,
        status: d.原価状態 || '',
      });
    }
  }

  // ── 発注後未着 ──
  if (snap.pending.length > 0) {
    const ws = wb.addWorksheet('発注後未着商品');
    ws.columns = [
      { header: '仕入先', key: 'supplier', width: 30 },
      { header: '金額（税抜）', key: 'amount', width: 18, style: { numFmt: '#,##0' } },
      { header: 'メモ', key: 'note', width: 30 },
    ];
    ws.getRow(1).font = { bold: true };
    for (const p of snap.pending) {
      ws.addRow({ supplier: p.supplier_name, amount: p.amount, note: p.note || '' });
    }
  }

  return await wb.xlsx.writeBuffer();
}
