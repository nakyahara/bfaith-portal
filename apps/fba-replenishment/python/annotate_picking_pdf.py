# -*- coding: utf-8 -*-
"""
トータルピッキングリスト(TMP1) PDF の 構造化抽出 + 「納品プランNo」列の注番 CLI。

2つのモード:

1) 抽出モード — TMP1 の各アイテム行 (ブロック/ロケ/商品ID/数量/残数) を構造検証つきで
   JSON 出力する。残数は lz CSV に存在しないため、この抽出値が唯一の取得元。
     python annotate_picking_pdf.py --pdf <TMP1.pdf> --extract-json <out.json>

2) 注番モード — Node 側で突合・検証済みの プランNo 割り当て JSON (page/item 単位) を受け取り、
   納品プランNo列をオーバーレイした PDF を出力する。再抽出結果が plan-json の
   ページ数・行数・商品ID と一致しない場合はエラー終了する (検証後のズレ検出)。
     python annotate_picking_pdf.py --pdf <TMP1.pdf> --plan-json <plans.json> --out <out.pdf>

plan-json 形式 (行キーは page+item。商品ID辞書は同一IDの上書き事故があるため使わない):
  {"items": [{"page": 0, "item": 0, "productId": "vitaminc-30",
              "planNo": "通常_24", "status": "matched"}, ...]}
  status: matched | no-plan (no-plan は planNo="プランなし" 等の表示文字列を渡す)

構造検証 (帳票フォーマット変更・pdfplumber誤検出の fail-closed):
 - find_tables の最後のテーブルから rows(座標) と extract()(文字列) を同一 Table で取得
 - ヘッダー3行 + 1アイテム3行 (2,3行目は先頭セル空の継続行)
 - 商品ID非空 / ブロック+ロケーションが 'BLOCK\\nLOC' 形式 / 数量・残数が非負整数
 - 同一ページ内で行の Y 座標が単調増加
 違反はページ・行番号つきのエラーで exit 3 (空配列で続行しない)。

stdout に処理結果 JSON を出力する。エラーは stderr + 非0 exit。
"""
import argparse
import io
import json
import re
import sys

import pdfplumber
from pypdf import PdfReader, PdfWriter
from reportlab.pdfgen import canvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib.colors import HexColor, white, black

HEADER_ROWS = 3   # テーブル先頭のヘッダー行数
ROWS_PER_ITEM = 3  # 1アイテム = 3行 (商品ID行 / バーコード行 / 商品名行)
MIN_COLS = 6       # col0=ブロック+ロケ, col1=商品ID, col4=数量, col5=残数 を読むための最低列数
INT_RE = re.compile(r"^(0|[1-9][0-9]*)$")


class ExtractError(Exception):
    pass


def register_font():
    """日本語フォントを登録する。TrueType(.ttf/.ttc) のみ (reportlab TTFont は CFF非対応)。"""
    import os
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


def _clean_int(s):
    """'1,049' のような桁区切り・空白を除去して非負整数文字列へ。不正は None。"""
    t = re.sub(r"[,\s　]", "", str(s or ""))
    return t if INT_RE.match(t) else None


def extract_items(pdf_path):
    """TMP1 の各ページから構造検証つきでアイテム行を抽出する。

    Returns: pages — list[page] で page は list[item dict]:
      {page, item, block, location, product_id, qty, zansu, y_top, y_bottom}
    """
    pages = []
    # 同一ブロック/ロケが連続する行 (特に ZZZ 未引当) は帳票の繰り返し省略で
    # 2行目以降のセルが空になる → 直前アイテムから継承する (ページ跨ぎも許容)
    last_block = None
    last_location = None
    with pdfplumber.open(pdf_path) as pdf:
        for pno, page in enumerate(pdf.pages):
            tables = page.find_tables()
            if not tables:
                pages.append([])
                continue
            table = tables[-1]
            grid = table.extract()  # 座標(rows)と文字列(grid)を同一 Table から取得
            rows = table.rows
            if len(grid) != len(rows):
                raise ExtractError(f"p{pno + 1}: テーブル構造を解釈できません (rows={len(rows)} / extract={len(grid)})")

            items = []
            prev_top = None
            ri = HEADER_ROWS
            while ri < len(rows):
                cells = rows[ri].cells
                if not (cells and cells[0]):
                    # アイテム先頭行は必ず先頭セルを持つ。持たない行が現れたらスキップせず前進のみ
                    ri += 1
                    continue
                grow = grid[ri]
                if len(grow) < MIN_COLS:
                    raise ExtractError(f"p{pno + 1} 表{ri + 1}行目: 列数不足 ({len(grow)} < {MIN_COLS})")

                head = str(grow[0] or "").strip()
                parts = [p.strip() for p in head.split("\n") if p.strip()]
                if len(parts) >= 2:
                    block, location = parts[0], parts[1]
                elif len(parts) == 0:
                    if last_block is None or last_location is None:
                        raise ExtractError(f"p{pno + 1} 表{ri + 1}行目: ブロック/ロケーションが空で継承元もありません")
                    block, location = last_block, last_location
                else:
                    raise ExtractError(f"p{pno + 1} 表{ri + 1}行目: ブロック/ロケーションを解釈できません: {head!r}")
                last_block, last_location = block, location

                product_id = str(grow[1] or "").strip()
                if not product_id:
                    raise ExtractError(f"p{pno + 1} 表{ri + 1}行目: 商品IDが空です")

                qty_s = _clean_int(grow[4])
                zansu_s = _clean_int(grow[5])
                if qty_s is None:
                    raise ExtractError(f"p{pno + 1} {product_id}: 数量が非負整数ではありません: {grow[4]!r}")
                if zansu_s is None:
                    raise ExtractError(f"p{pno + 1} {product_id}: 残数が非負整数ではありません: {grow[5]!r}")

                # 3行構造: 続く2行は先頭セル空の継続行
                for k in (1, 2):
                    if ri + k >= len(rows):
                        raise ExtractError(f"p{pno + 1} {product_id}: アイテムの{k + 1}行目が欠けています (3行構造崩れ)")
                    nc = rows[ri + k].cells
                    if nc and nc[0]:
                        raise ExtractError(f"p{pno + 1} {product_id}: 3行構造が崩れています (継続行に先頭セルあり)")

                y_top, y_bottom = cells[0][1], cells[0][3]
                if prev_top is not None and y_top <= prev_top:
                    raise ExtractError(f"p{pno + 1} {product_id}: 行のY座標が単調増加していません")
                prev_top = y_top

                items.append({
                    "page": pno, "item": len(items),
                    "block": block, "location": location,
                    "product_id": product_id,
                    "qty": int(qty_s), "zansu": int(zansu_s),
                    "y_top": y_top, "y_bottom": y_bottom,
                })
                ri += ROWS_PER_ITEM
            pages.append(items)

    if sum(len(p) for p in pages) == 0:
        raise ExtractError("アイテム行を1件も抽出できませんでした (帳票フォーマットを確認してください)")
    return pages


def annotate(tmp1_path, pages, plan_items, font_name, out_path):
    """TMP1 に納品プランNo列をオーバーレイして保存。plan_items は (page,item) キー。"""
    plan_by_key = {}
    for it in plan_items:
        k = (int(it["page"]), int(it["item"]))
        if k in plan_by_key:
            raise ExtractError(f"plan-json の行キーが重複しています: page={k[0]} item={k[1]}")
        plan_by_key[k] = it

    # 再抽出との整合: 総行数・(page,item)ごとの商品ID一致
    total_items = sum(len(p) for p in pages)
    if total_items != len(plan_items):
        raise ExtractError(f"plan-json の件数({len(plan_items)})と再抽出行数({total_items})が一致しません")
    for pno, items in enumerate(pages):
        for it in items:
            plan = plan_by_key.get((pno, it["item"]))
            if plan is None:
                raise ExtractError(f"plan-json に p{pno + 1} {it['item'] + 1}行目の割り当てがありません")
            if str(plan.get("productId", "")).strip() != it["product_id"]:
                raise ExtractError(
                    f"p{pno + 1} {it['item'] + 1}行目: 商品ID不一致 (PDF={it['product_id']!r} / plan-json={plan.get('productId')!r})")

    EXTRA_WIDTH = 75
    reader = PdfReader(tmp1_path)
    writer = PdfWriter()
    if len(reader.pages) != len(pages):
        raise ExtractError(f"PDFページ数({len(reader.pages)})と抽出ページ数({len(pages)})が一致しません")

    matched = 0
    for page_num, orig_page in enumerate(reader.pages):
        mb = orig_page.mediabox
        orig_width = float(mb.width)
        orig_height = float(mb.height)
        new_width = orig_width + EXTRA_WIDTH

        packet = io.BytesIO()
        c = canvas.Canvas(packet, pagesize=(new_width, orig_height))
        items = pages[page_num]

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
            for it in items:
                y_top_pdf = to_pdf_y(it["y_top"])
                y_bottom_pdf = to_pdf_y(it["y_bottom"])
                cell_height = y_top_pdf - y_bottom_pdf
                c.setFillColor(HexColor("#FFFFFF"))
                c.rect(col_x, y_bottom_pdf, col_width, cell_height, fill=1, stroke=1)
                plan = plan_by_key[(page_num, it["item"])]
                plan_no = str(plan.get("planNo", "") or "").strip() or "(該当なし)"
                if plan.get("status") == "matched":
                    matched += 1
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
    return matched, total_items


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdf", required=True, help="TMP1 PDF のパス")
    ap.add_argument("--extract-json", help="抽出モード: 構造化抽出結果を書き出す JSON パス")
    ap.add_argument("--plan-json", help="注番モード: page/item 単位の プランNo 割り当て JSON パス")
    ap.add_argument("--out", help="注番モード: 出力 PDF のパス")
    args = ap.parse_args()

    try:
        pages = extract_items(args.pdf)
    except ExtractError as e:
        print(f"抽出エラー: {e}", file=sys.stderr)
        sys.exit(3)

    if args.extract_json:
        items = [
            {k: it[k] for k in ("page", "item", "block", "location", "product_id", "qty", "zansu")}
            for page in pages for it in page
        ]
        payload = {"pages": len(pages), "total": len(items), "items": items}
        with open(args.extract_json, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        json.dump({"mode": "extract", "pages": len(pages), "total": len(items)}, sys.stdout, ensure_ascii=False)
        return

    if not (args.plan_json and args.out):
        print("注番モードには --plan-json と --out が必要です", file=sys.stderr)
        sys.exit(2)

    with open(args.plan_json, "r", encoding="utf-8") as f:
        plan_items = json.load(f).get("items", [])

    font_name = register_font()
    try:
        matched, total = annotate(args.pdf, pages, plan_items, font_name, args.out)
    except ExtractError as e:
        print(f"注番エラー: {e}", file=sys.stderr)
        sys.exit(3)

    json.dump({"mode": "annotate", "matched": matched, "total": total, "font": font_name},
              sys.stdout, ensure_ascii=False)


if __name__ == "__main__":
    main()
