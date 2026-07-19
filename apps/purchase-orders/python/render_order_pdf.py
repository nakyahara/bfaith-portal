#!/usr/bin/env python3
"""
発注書PDF生成 (FAX送信用。eFaxメールゲートウェイに添付する)。

stdin: 発注書データJSON (UTF-8) — fax-pdf.js が組み立てる
stdout: PDFバイナリ
exit 0=成功 / 1=失敗 (stderrに理由)

使用ライブラリ: reportlab (fba-replenishment の requirements.txt 由来。Render venv に導入済み)
日本語フォント: fonts-takao-gothic (Docker) / msgothic・meiryo (Windowsローカル検証)
FAXは白黒・低解像度 (200dpi程度) で相手に届くため、罫線は太め・文字は9pt以上・色は使わない。
"""
import json
import os
import sys
from io import BytesIO

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle,
)


def register_font():
    """日本語フォントを登録 (annotate_picking_pdf.py と同じ探索順)。TrueTypeのみ。"""
    font_name = "Japanese"
    font_paths = [
        # Linux (Render/Docker) — fonts-takao-gothic
        "/usr/share/fonts/truetype/takao-gothic/TakaoGothic.ttf",
        "/usr/share/fonts/truetype/takao-gothic/TakaoPGothic.ttf",
        # Windows (ローカル検証)
        "C:/Windows/Fonts/msgothic.ttc",
        "C:/Windows/Fonts/meiryo.ttc",
    ]
    for fp in font_paths:
        if os.path.exists(fp):
            try:
                if fp.lower().endswith(".ttc"):
                    pdfmetrics.registerFont(TTFont(font_name, fp, subfontIndex=0))
                else:
                    pdfmetrics.registerFont(TTFont(font_name, fp))
                return font_name
            except Exception:
                pass
    raise RuntimeError("日本語フォントが見つかりません (fonts-takao-gothic / msgothic)")


def fmt_money(v):
    """1341960 → '1,341,960' / 1234.5 → '1,234.50' / None → ''"""
    if v is None or v == "":
        return ""
    f = float(v)
    if f == int(f):
        return f"{int(f):,}"
    return f"{f:,.2f}"


def build_pdf(data):
    font = register_font()
    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=15 * mm, rightMargin=15 * mm,
        topMargin=14 * mm, bottomMargin=16 * mm,
        title=data.get("po_number") or "発注書",
    )
    black = colors.black
    st_title = ParagraphStyle("title", fontName=font, fontSize=20, leading=24, alignment=1)
    st_base = ParagraphStyle("base", fontName=font, fontSize=10, leading=14)
    st_small = ParagraphStyle("small", fontName=font, fontSize=9, leading=12)
    st_supplier = ParagraphStyle("sup", fontName=font, fontSize=13, leading=17)
    st_cell = ParagraphStyle("cell", fontName=font, fontSize=9, leading=11)
    st_cell_r = ParagraphStyle("cellr", fontName=font, fontSize=9, leading=11, alignment=2)

    story = [Paragraph("発　注　書", st_title), Spacer(1, 6 * mm)]

    # ── ヘッダ: 左=宛先 / 右=発注情報+発行元 ──
    left_lines = [Paragraph(data["supplier_name"], st_supplier)]
    if data.get("contact_name"):
        left_lines.append(Paragraph("ご担当: " + data["contact_name"], st_base))
    left_lines.append(Spacer(1, 3 * mm))
    left_lines.append(Paragraph("下記の通り発注いたします。よろしくお願いいたします。", st_base))
    left_lines.append(Paragraph("希望納期: " + (data.get("nouki_text") or "指定なし"), st_base))

    right_lines = [
        Paragraph("発注日: " + data["issued_date"], st_base),
        Paragraph("発注伝票番号: " + data["po_number"], st_base),
        Spacer(1, 2 * mm),
    ]
    for ln in data.get("company_lines", []):
        right_lines.append(Paragraph(ln, st_small))
    if data.get("issuer_name"):
        right_lines.append(Paragraph("発行担当者: " + data["issuer_name"], st_small))

    head = Table(
        [[left_lines, right_lines]],
        colWidths=[100 * mm, 80 * mm],
        style=TableStyle([
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 0),
            ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ]),
    )
    story.append(head)
    story.append(Spacer(1, 5 * mm))

    # ── 明細表 ──
    vendor_col = bool(data.get("vendor_col_used"))
    header = ["No", "商品コード", "商品名", "希望納期", "単価", "数量", "小計"]
    if vendor_col:
        header.append("先方管理番号")
    rows = [header]
    for i, it in enumerate(data["items"], 1):
        row = [
            str(i),
            Paragraph(it.get("code") or "", st_cell),
            Paragraph(it.get("name") or "", st_cell),
            it.get("nouki") or "",
            fmt_money(it.get("unit_cost")),
            f"{int(it['qty']):,}",
            fmt_money(it.get("subtotal")),
        ]
        if vendor_col:
            row.append(Paragraph(it.get("vendor_code") or "", st_cell))
        rows.append(row)

    # 合計行 (小計列に合計金額。単価列ラベル)
    total = ["", "", "", "", "合計", f"{int(data['total_qty']):,}", fmt_money(data.get("total_amount"))]
    if vendor_col:
        total.append("")
    rows.append(total)

    if vendor_col:
        widths = [9 * mm, 30 * mm, 55 * mm, 20 * mm, 18 * mm, 13 * mm, 20 * mm, 25 * mm]
    else:
        widths = [10 * mm, 36 * mm, 72 * mm, 22 * mm, 20 * mm, 14 * mm, 22 * mm]

    tstyle = [
        ("FONTNAME", (0, 0), (-1, -1), font),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("GRID", (0, 0), (-1, -2), 0.8, black),
        ("BACKGROUND", (0, 0), (-1, 0), colors.Color(0.88, 0.88, 0.88)),
        ("ALIGN", (0, 0), (-1, 0), "CENTER"),
        ("ALIGN", (0, 1), (0, -1), "CENTER"),          # No
        ("ALIGN", (3, 1), (3, -1), "CENTER"),          # 希望納期
        ("ALIGN", (4, 1), (6, -1), "RIGHT"),           # 単価/数量/小計
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        # 合計行: 罫線は金額側のみ、太字風に枠を強調
        ("BOX", (4, -1), (-1, -1), 1.2, black),
        ("INNERGRID", (4, -1), (-1, -1), 0.8, black),
        ("FONTSIZE", (4, -1), (-1, -1), 10),
    ]
    story.append(Table(rows, colWidths=widths, repeatRows=1, style=TableStyle(tstyle)))
    story.append(Spacer(1, 4 * mm))
    story.append(Paragraph(f"明細 {len(data['items'])} 行 / 合計数量 {int(data['total_qty']):,} / 合計金額 ¥{fmt_money(data.get('total_amount'))}", st_base))

    def on_page(canv, _doc):
        canv.setFont(font, 8)
        canv.drawCentredString(A4[0] / 2, 8 * mm, f"{data['po_number']}　- {canv.getPageNumber()} -")

    doc.build(story, onFirstPage=on_page, onLaterPages=on_page)
    return buf.getvalue()


def main():
    try:
        data = json.loads(sys.stdin.buffer.read().decode("utf-8"))
        pdf = build_pdf(data)
        sys.stdout.buffer.write(pdf)
        sys.stdout.buffer.flush()
    except Exception as e:
        print(f"PDF生成失敗: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
