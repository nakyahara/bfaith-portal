#!/usr/bin/env python3
"""
入荷予定リストPDF生成 (画面の「🖨 印刷」と同じ内容を PDF にして Drive へ保存する用)。

stdin: JSON (UTF-8) — schedule-pdf.js が組み立てる
  {
    "title": "入荷予定リスト",
    "meta": ["入荷予定 26件", "CSV更新 2026-07-24 08:30", ...],   # 見出し下に1行で出す
    "columns": [{"label":"商品コード","key":"商品コード","width":22}, ...],  # width は mm
    "rows": [{"商品コード":"...", "商品名":"...", ...}, ...]
  }
stdout: PDFバイナリ
exit 0=成功 / 1=失敗 (stderrに理由)

使用ライブラリ: reportlab (fba-replenishment の requirements.txt 由来。Render venv に導入済み)
日本語フォント: fonts-takao-gothic (Docker) / msgothic・meiryo (Windowsローカル検証)
レイアウト: A4横。倉庫で手に持って照合するリストなので、罫線をはっきり・行間を詰めすぎない。
  見出し行は全ページ再掲 (repeatRows=1)、ページ番号は右下。
"""
import json
import os
import sys
from io import BytesIO
from xml.sax.saxutils import escape as xesc

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle


def register_font():
    """日本語フォントを登録 (render_order_pdf.py と同じ探索順)。TrueTypeのみ。"""
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


def draw_page_number(canvas, doc):
    """右下にページ番号 (総ページ数は2パス必要なので出さない)。"""
    canvas.saveState()
    canvas.setFont(doc.jp_font, 8)
    canvas.drawRightString(doc.pagesize[0] - 10 * mm, 7 * mm, f"{canvas.getPageNumber()} ページ")
    canvas.restoreState()


def main():
    # stdin は必ずバイナリで読んで UTF-8 と明示デコードする (render_order_pdf.py と同じ)。
    # json.load(sys.stdin) だとプラットフォームの locale (Windows は cp932) で解釈され、
    # 日本語がサロゲートに化けて PDF 保存時に UnicodeEncodeError になる。
    try:
        payload = json.loads(sys.stdin.buffer.read().decode("utf-8"))
    except Exception as e:
        print(f"stdin の JSON を読めません: {e}", file=sys.stderr)
        return 1

    font = register_font()
    columns = payload.get("columns") or []
    rows = payload.get("rows") or []
    if not columns:
        print("columns が空です", file=sys.stderr)
        return 1

    # 空リストでも「0件のPDF」を出す (前回のPDFが残って古い予定に見えるより、0件と分かる方が安全)
    styles = {
        "title": ParagraphStyle("t", fontName=font, fontSize=13, leading=15),
        "meta": ParagraphStyle("m", fontName=font, fontSize=8, leading=10, textColor=colors.HexColor("#333333")),
        "th": ParagraphStyle("th", fontName=font, fontSize=7.5, leading=9),
        "td": ParagraphStyle("td", fontName=font, fontSize=8, leading=9.5),
        "num": ParagraphStyle("n", fontName=font, fontSize=8, leading=9.5, alignment=2),  # 右寄せ
    }

    def cell(text, style):
        # Paragraph にすると長い商品名が折り返される (セル内で切れずに全文が出る)
        return Paragraph(xesc("" if text is None else str(text)), styles[style])

    data = [[cell(c.get("label", ""), "th") for c in columns]]
    for r in rows:
        line = []
        for c in columns:
            v = r.get(c.get("key"))
            line.append(cell(v, "num" if c.get("align") == "right" else "td"))
        data.append(line)

    widths = [float(c.get("width", 20)) * mm for c in columns]
    table = Table(data, colWidths=widths, repeatRows=1)
    table.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (-1, -1), font),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#333333")),
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e8e8e8")),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING", (0, 0), (-1, -1), 2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
        ("LEFTPADDING", (0, 0), (-1, -1), 3),
        ("RIGHTPADDING", (0, 0), (-1, -1), 3),
    ]))

    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=landscape(A4),
        leftMargin=8 * mm, rightMargin=8 * mm, topMargin=8 * mm, bottomMargin=10 * mm,
        title=payload.get("title") or "入荷予定リスト", author="B-Faith 入庫情報管理",
    )
    doc.jp_font = font
    story = [
        Paragraph(xesc(payload.get("title") or "入荷予定リスト"), styles["title"]),
        Paragraph(xesc(" ／ ".join(payload.get("meta") or [])), styles["meta"]),
        Spacer(1, 3 * mm),
        table,
    ]
    doc.build(story, onFirstPage=draw_page_number, onLaterPages=draw_page_number)
    sys.stdout.buffer.write(buf.getvalue())
    return 0


if __name__ == "__main__":
    sys.exit(main())
