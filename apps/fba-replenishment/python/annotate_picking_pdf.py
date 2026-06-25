# -*- coding: utf-8 -*-
"""
トータルピッキングリスト(TMP1) PDF に「納品プランNo」列を追記する CLI。

picking-list-app (Streamlit, github.com/nakyahara/picking-list-app) の app.py を、
Web UI を外して Node から呼べる一発実行スクリプトに移植したもの。

使い方:
  python annotate_picking_pdf.py --pdf <TMP1.pdf> --mapping <mapping.csv> --out <out.pdf>

  mapping.csv は 商品ID(または商品コード) → 納品プランNo(またはプランNo/ラベル) の対応表。
  bfaith-portal のピッキング準備が生成する fbanouhinbangoulist.csv と同形式。

stdout に処理結果を JSON で出力する: {"matched": n, "total": n, "unmatched": [...]}
"""
import argparse
import csv
import io
import json
import os
import sys

import pdfplumber
from pypdf import PdfReader, PdfWriter
from reportlab.pdfgen import canvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib.colors import HexColor, white, black


def register_font():
    """日本語フォントを登録する。TrueType(.ttf/.ttc) のみ (reportlab TTFont は CFF非対応)。"""
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
                pdfmetrics.registerFont(TTFont(font_name, fp))
                return font_name
            except Exception:
                pass
    return "Helvetica"


def read_mapping_csv(path):
    """マッピングCSVから 商品ID → 納品プランNo の辞書を作成。"""
    plan_map = {}
    with open(path, "rb") as f:
        raw = f.read()
    for enc in ["utf-8-sig", "utf-8", "cp932", "shift_jis"]:
        try:
            text = raw.decode(enc)
            break
        except (UnicodeDecodeError, AttributeError):
            continue
    else:
        text = raw.decode("utf-8", errors="replace")

    reader = csv.reader(io.StringIO(text))
    header = next(reader, None)
    if not header:
        return plan_map

    id_col = None
    plan_col = None
    for i, h in enumerate(header):
        h_clean = h.strip()
        if h_clean in ("商品ID", "商品コード"):
            id_col = i
        elif h_clean in ("納品プランNo", "プランNo", "ラベル"):
            plan_col = i
    if id_col is None or plan_col is None:
        id_col = 0
        plan_col = 1

    for row in reader:
        if len(row) > max(id_col, plan_col):
            pid = row[id_col].strip()
            plan_no = row[plan_col].strip()
            if pid and plan_no:
                plans = [p.strip() for p in plan_no.replace("\r", "").split("\n") if p.strip()]
                if plans:
                    plan_map[pid] = " / ".join(plans)
    return plan_map


def extract_tmp1_page_data(pdf_path):
    """TMP1 の各ページから商品IDとセル位置情報を抽出。"""
    page_data = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            tables_found = page.find_tables()
            if not tables_found:
                page_data.append([])
                continue
            data_table = tables_found[-1]
            rows = data_table.rows
            extracted = page.extract_tables()
            ext_table = extracted[-1] if extracted else []

            items = []
            ri = 3  # ヘッダー3行スキップ
            while ri < len(rows):
                row = rows[ri]
                cells = row.cells
                if cells and cells[0]:
                    y_top = cells[0][1]
                    y_bottom = cells[0][3]
                    product_id = ""
                    if ri < len(ext_table) and ext_table[ri][1]:
                        product_id = ext_table[ri][1].strip()
                    items.append({"product_id": product_id, "y_top": y_top, "y_bottom": y_bottom})
                    ri += 3
                else:
                    ri += 1
            page_data.append(items)
    return page_data


def create_merged_pdf(tmp1_path, plan_map, page_data, font_name, out_path):
    """元の TMP1 PDF に納品プランNo列をオーバーレイして保存。"""
    EXTRA_WIDTH = 75
    reader = PdfReader(tmp1_path)
    writer = PdfWriter()
    matched_count = 0
    unmatched_ids = set()

    for page_num, orig_page in enumerate(reader.pages):
        mb = orig_page.mediabox
        orig_width = float(mb.width)
        orig_height = float(mb.height)
        new_width = orig_width + EXTRA_WIDTH

        packet = io.BytesIO()
        c = canvas.Canvas(packet, pagesize=(new_width, orig_height))
        items = page_data[page_num] if page_num < len(page_data) else []

        table_right = 571.8
        col_x = table_right
        col_width = EXTRA_WIDTH

        def to_pdf_y(plumber_y):
            return orig_height - plumber_y

        if items:
            header_top = 117.9
            header_bottom = 160.4
            hy_top_pdf = to_pdf_y(header_top)
            hy_bottom_pdf = to_pdf_y(header_bottom)

            c.setFillColor(HexColor("#808080"))
            c.setStrokeColor(HexColor("#808080"))
            c.rect(col_x, hy_bottom_pdf, col_width, hy_top_pdf - hy_bottom_pdf, fill=1, stroke=1)
            c.setFillColor(white)
            c.setFont(font_name, 7)
            text_y = hy_bottom_pdf + (hy_top_pdf - hy_bottom_pdf) / 2 + 8
            c.drawCentredString(col_x + col_width / 2, text_y, "納品プラン")
            c.drawCentredString(col_x + col_width / 2, text_y - 12, "No")

            c.setStrokeColor(HexColor("#808080"))
            c.setLineWidth(0.5)
            for item in items:
                y_top_pdf = to_pdf_y(item["y_top"])
                y_bottom_pdf = to_pdf_y(item["y_bottom"])
                cell_height = y_top_pdf - y_bottom_pdf
                c.setFillColor(HexColor("#FFFFFF"))
                c.rect(col_x, y_bottom_pdf, col_width, cell_height, fill=1, stroke=1)
                pid = item["product_id"]
                plan_no = plan_map.get(pid, "")
                if plan_no:
                    matched_count += 1
                else:
                    plan_no = "(該当なし)"
                    if pid:
                        unmatched_ids.add(pid)
                c.setFillColor(black)
                font_size = 5.5 if " / " in plan_no else 7
                c.setFont(font_name, font_size)
                text_y = y_bottom_pdf + cell_height / 2 - 3
                c.drawCentredString(col_x + col_width / 2, text_y, plan_no)

        c.save()
        packet.seek(0)
        overlay_reader = PdfReader(packet)
        overlay_page = overlay_reader.pages[0]
        orig_page.mediabox.upper_right = (new_width, orig_height)
        if "/CropBox" in orig_page:
            orig_page.cropbox.upper_right = (new_width, orig_height)
        orig_page.merge_page(overlay_page)
        writer.add_page(orig_page)

    with open(out_path, "wb") as f:
        writer.write(f)
    return matched_count, unmatched_ids


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdf", required=True, help="TMP1 PDF のパス")
    ap.add_argument("--mapping", required=True, help="商品ID→納品プランNo の CSV パス")
    ap.add_argument("--out", required=True, help="出力 PDF のパス")
    args = ap.parse_args()

    font_name = register_font()
    plan_map = read_mapping_csv(args.mapping)
    page_data = extract_tmp1_page_data(args.pdf)
    total_items = sum(len(items) for items in page_data)
    matched, unmatched_ids = create_merged_pdf(args.pdf, plan_map, page_data, font_name, args.out)

    json.dump(
        {
            "matched": matched,
            "total": total_items,
            "unmatched": sorted(unmatched_ids),
            "font": font_name,
        },
        sys.stdout,
        ensure_ascii=False,
    )


if __name__ == "__main__":
    main()
