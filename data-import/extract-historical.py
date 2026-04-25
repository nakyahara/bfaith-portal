"""Excel「集計用元データ」から税抜運賃・資材費を抽出"""
import openpyxl
import json
import os

SRC = r'E:\Users\info\Downloads\【集計用元データ】セグメント別売上データ.xlsx'
OUT = r'c:\Users\info\Downloads\bfaith-portal\data-import\historical-costs.json'

SKIP = {'合計', '運賃合計', '運賃合計(輸出)', None, ''}
TAX = 1.1

os.makedirs(os.path.dirname(OUT), exist_ok=True)
wb = openpyxl.load_workbook(SRC, data_only=True, read_only=True)

def ex(v):
    return int(round(float(v) / TAX))

def extract(sheet_name, key_field, extra=None):
    ws = wb[sheet_name]
    rows_iter = ws.iter_rows(values_only=True)
    headers = next(rows_iter)
    out = []
    for row in rows_iter:
        if not row or not row[0]:
            continue
        ym = row[0].strftime('%Y-%m') if hasattr(row[0], 'strftime') else None
        if not ym:
            continue
        for i, name in enumerate(headers):
            if i == 0 or name in SKIP:
                continue
            val = row[i] if i < len(row) else None
            if not val:
                continue
            try:
                amt = ex(val)
                if amt > 0:
                    rec = {'year_month': ym, key_field: name, 'amount': amt}
                    if extra:
                        rec.update(extra)
                    out.append(rec)
            except (TypeError, ValueError):
                pass
    return out

freight = extract('運賃集計', 'carrier')
export_freight = extract('輸出運賃', 'carrier',
                         extra={'cost_scope': 'export_only', 'target_segment': 4, 'target_mall_id': 'amazon_usa'})
for e in export_freight:
    e['carrier'] = e['carrier'] + '(輸出)'
freight.extend(export_freight)
material = extract('梱包資材費', 'supplier')

f_feb = sum(f['amount'] for f in freight if f['year_month'] == '2026-02')
m_feb = sum(m['amount'] for m in material if m['year_month'] == '2026-02')
print(f'2026-02 運賃(税抜): {f_feb:,} (期待 18,995,566)')
print(f'2026-02 資材(税抜): {m_feb:,} (期待 541,739)')
print(f'Freight: {len(freight)}, Material: {len(material)}')

with open(OUT, 'w', encoding='utf-8') as f:
    json.dump({'freight': freight, 'material': material}, f, ensure_ascii=False, indent=2)
print(f'saved: {OUT}')
