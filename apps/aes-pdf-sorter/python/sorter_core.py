"""
AES送り状並び替え コアロジック

Webアプリ (main.py) と Drive自動化ワーカー (drive_worker.py) の両方から使う。
FastAPI・Drive API・バーコードライブラリに依存しない
(バーコード読み取り器は extract_barcodes_from_doc(doc) を持つオブジェクトとして注入する)。

並び替えの仕様 (既存Webアプリと同一):
  1. CSVの「注文番号」「配送番号」列から 注文番号→配送番号 の対応表を作る
  2. 送り状PDFの各ページのバーコードから 配送番号→(PDF,ページ) の対応表を作る
  3. 納品書PDFから注文番号 (3桁-7桁-7桁) を出現順に抽出し、
     その順に送り状ページを並べたPDFを組み立てる
"""
import csv
import hashlib
import io
import json
import re

import fitz  # PyMuPDF
import numpy as np

# 出力ファイル名 (drive_worker から参照。Webアプリのダウンロード名とは無関係)
OUTPUT_PDF_NAME = 'AES送り状_並び替え済.pdf'
MANIFEST_NAME = 'AES送り状_並び替え済_manifest.json'

# manifest (出力ページ → 注文番号) のフォーマット版。読み手 (packing 再印刷) が検証する
MANIFEST_VERSION = 1

# 白紙判定用レンダリングの解像度。送り状1ページ ≒ 300x420px 程度で足り、
# 数十ページでも1秒未満で終わる (判定はグレースケールの非白ピクセル率のみ)
INK_RENDER_DPI = 36
# 「白」とみなす明度の下限 (0-255)。JPEG圧縮の地色ゆらぎを白側に寄せる
INK_WHITE_THRESHOLD = 245

# Amazon注文番号 (3桁-7桁-7桁)
ORDER_PATTERN = re.compile(r'\b\d{3}-\d{7}-\d{7}\b')

# CODABARの先頭・末尾のスタート/ストップ文字 (A-D) を1文字ずつ除去する
# (バーコード読み取り器側でも両端同時に付いている場合は除去されるため、
#  ここは片側だけ残ったケースの保険。既存Webアプリと同一の処理)
CODABAR_EDGE = re.compile(r'^[ABCD]|[ABCD]$')


def decode_csv_bytes(content):
    """CSVバイト列を cp932 → utf-8-sig の順でデコード。両方失敗なら None"""
    try:
        return content.decode('cp932')
    except UnicodeDecodeError:
        try:
            return content.decode('utf-8-sig')
        except UnicodeDecodeError:
            return None


def build_order_shipping_map(csv_text, order_shipping_map):
    """CSVテキストから 注文番号→配送番号 を order_shipping_map に追記する。

    ヘッダに「注文番号」「配送番号」が無ければ 'csv_header' を返す (正常時 None)。
    行の値が読めない等の例外は呼び出し側で捕捉する (既存挙動の維持)。
    """
    reader = csv.DictReader(io.StringIO(csv_text))
    if (reader.fieldnames is None
            or "注文番号" not in reader.fieldnames
            or "配送番号" not in reader.fieldnames):
        return 'csv_header'
    for row in reader:
        order_number = row.get("注文番号", "").strip()
        shipping_number = row.get("配送番号", "").strip()
        if order_number and shipping_number:
            order_shipping_map[order_number] = shipping_number
    return None


def build_shipping_barcode_map(label_files, extractor, duplicates=None, doc_names=None):
    """送り状PDF群からバーコードを読み、配送番号→(fitz doc, ページ番号) の対応表を作る。

    label_files: [(ファイル名, PDFバイト列)]
    extractor:   extract_barcodes_from_doc(doc) を持つオブジェクト (BarcodeExtractor)
    duplicates:  listを渡すと、同じ配送番号が異なるページで検出された場合に
                 その配送番号を追記する (Driveワーカーの競合検知用。
                 Webアプリは渡さない = 従来どおり後勝ち上書きで挙動不変)
    doc_names:   dictを渡すと id(doc) → 素材ファイル名 を記録する (manifestの出所記録用)
    戻り値: (barcode_map, errors, docs)
      errors は {"file", "error", "type"} の一覧 (既存Webアプリと同じ文言・種別)。
      docs は開いた fitz ドキュメント一覧。並び替えPDFを作り終えるまで開いたままにし、
      呼び出し側が必ず close すること。
    """
    barcode_map = {}
    errors = []
    docs = []

    for filename, content in label_files:
        try:
            pdf_doc = fitz.open(stream=content, filetype="pdf")
            docs.append(pdf_doc)
            if doc_names is not None:
                doc_names[id(pdf_doc)] = filename

            try:
                barcodes = extractor.extract_barcodes_from_doc(pdf_doc)
            except Exception as e:
                errors.append({"file": filename, "error": f"バーコード読み取り処理エラー: {str(e)}", "type": "barcode_processing"})
                barcodes = []

            if not barcodes:
                errors.append({"file": filename, "error": "バーコードが検出されませんでした", "type": "barcode_detection"})
                continue

            for barcode_info in barcodes:
                barcode_data = barcode_info['data']
                if barcode_info['format'] == 'CODABAR':
                    barcode_data = CODABAR_EDGE.sub('', barcode_data)
                entry = (pdf_doc, barcode_info['page'])
                if (duplicates is not None
                        and barcode_data in barcode_map
                        and barcode_map[barcode_data] != entry):
                    duplicates.append(barcode_data)
                barcode_map[barcode_data] = entry
        except Exception as e:
            errors.append({"file": filename, "error": f"配送ラベル処理エラー: {str(e)}", "type": "shipping_label_processing"})

    return barcode_map, errors, docs


def extract_order_numbers(pdf_doc):
    """納品書PDFから注文番号を出現順に抽出する (重複は初出のみ)"""
    extracted_orders = []
    for page_num in range(pdf_doc.page_count):
        page = pdf_doc.load_page(page_num)
        text = page.get_text()
        for match in ORDER_PATTERN.findall(text):
            if match not in extracted_orders:
                extracted_orders.append(match)
    return extracted_orders


def match_orders_detailed(extracted_orders, order_shipping_map, barcode_map):
    """注文番号を配送番号→送り状ページへ解決する (manifest 用に由来も返す)。

    戻り値: (matches, unmatched_orders)
      matches は納品書の出現順の
      [{"order_number", "shipping_number", "doc", "source_page"}]。
      source_page は素材PDF内の0始まりページ番号。
    """
    matches = []
    unmatched_orders = []
    for order_number in extracted_orders:
        shipping_number = order_shipping_map.get(order_number)
        if shipping_number and shipping_number in barcode_map:
            doc, page_num = barcode_map[shipping_number]
            matches.append({
                "order_number": order_number,
                "shipping_number": shipping_number,
                "doc": doc,
                "source_page": page_num,
            })
        else:
            unmatched_orders.append(order_number)
    return matches, unmatched_orders


def match_orders(extracted_orders, order_shipping_map, barcode_map):
    """注文番号を配送番号→送り状ページへ解決する。

    戻り値: (matched_pages, unmatched_orders)
      matched_pages は納品書の出現順の [(fitz doc, ページ番号)]。
    """
    matches, unmatched_orders = match_orders_detailed(
        extracted_orders, order_shipping_map, barcode_map)
    return [(m["doc"], m["source_page"]) for m in matches], unmatched_orders


def page_ink_ratio(doc, page_num, dpi=INK_RENDER_DPI):
    """ページの「非白ピクセル率」(0.0〜1.0)。白紙判定の材料。

    AES送り状は画像1枚のページなので、構造 (画像XObjectの有無) だけでは
    「白一色の画像」を見抜けない。低解像度でグレースケール描画して実際に
    インクが載っているかを見る (Codexレビュー C: レンダリング判定が最も確実)。
    失敗しても並び替え本体は止めない — Noneを返し、読み手側が
    「判定不能」として扱う (白紙と断定はしない)。
    """
    try:
        page = doc.load_page(page_num)
        pix = page.get_pixmap(dpi=dpi, colorspace=fitz.csGRAY, alpha=False)
        arr = np.frombuffer(pix.samples, dtype=np.uint8)
        if arr.size == 0:
            return None
        return round(float(np.count_nonzero(arr < INK_WHITE_THRESHOLD)) / arr.size, 6)
    except Exception:
        return None


def build_manifest(matches, doc_names, output_pdf_bytes, unmatched_orders, generated_at,
                   folder_name=None, invoice_files=()):
    """並び替え済PDFの「出力ページ → 注文番号」対応表 (JSONバイト列) を組み立てる。

    再印刷 (apps/packing) はこの対応表で**注文番号の完全一致**からページを決める。
    位置推定 (ページ数=伝票数なら page=seq-1) は、照合できなかった注文が
    ページごと落ちると1ページずつズレて**別人の送り状**を掴むため使わない。

    output_pdf_sha256 は「この manifest がどのPDFのものか」の照合用。
    読み手は実物のハッシュと突き合わせ、違えば使わない (fail-closed)。
    """
    pages = []
    for idx, m in enumerate(matches, start=1):
        pages.append({
            "page": idx,
            "order_number": m["order_number"],
            "shipping_number": m["shipping_number"],
            "source_file": doc_names.get(id(m["doc"])),
            "source_page": m["source_page"] + 1,
            "ink_ratio": page_ink_ratio(m["doc"], m["source_page"]),
        })
    manifest = {
        "version": MANIFEST_VERSION,
        "generated_at": generated_at.isoformat(),
        # どの出荷フォルダ・どの納品書から作られた対応表かを記録する。
        # 読み手は「要求元の出荷フォルダと一致するか」「対応表が梱包バッチより新しいか」を
        # 確かめてから使う (Drive障害で旧PDF+旧manifestが対で残るケースへの備え。Codexレビュー指摘2)
        "folder_name": folder_name,
        "invoice_files": [
            {"name": f.get('name'), "modified_time": f.get('modifiedTime')} for f in invoice_files
        ],
        "output_pdf": OUTPUT_PDF_NAME,
        "output_pdf_sha256": hashlib.sha256(output_pdf_bytes).hexdigest(),
        "page_count": len(pages),
        "ink_render_dpi": INK_RENDER_DPI,
        "ink_white_threshold": INK_WHITE_THRESHOLD,
        "pages": pages,
        "unmatched_orders": list(unmatched_orders),
    }
    return json.dumps(manifest, ensure_ascii=False, indent=2).encode('utf-8')


def build_invalid_manifest(now_jst, reasons, folder_name=None):
    """失効した manifest (JSONバイト列)。削除の代わりにこれで上書きする。

    古い manifest が残ると、新しいバッチの伝票を古い対応表で引いて
    **別人の送り状を印刷**しかねない。invalid=true を見たら読み手は必ず止まる。
    """
    return json.dumps({
        "version": MANIFEST_VERSION,
        "invalid": True,
        "invalidated_at": now_jst.isoformat(),
        "folder_name": folder_name,
        "reasons": list(reasons),
        "pages": [],
    }, ensure_ascii=False, indent=2).encode('utf-8')


def build_label_pdf(matched_pages):
    """並び替え済み送り状PDF (バイト列) を組み立てる"""
    label_doc = fitz.open()
    try:
        for shipping_doc, page_num in matched_pages:
            label_doc.insert_pdf(shipping_doc, from_page=page_num, to_page=page_num)
        return label_doc.tobytes(garbage=4, deflate=True, clean=True)
    finally:
        label_doc.close()


def build_error_csv(errors):
    """エラー一覧をCSV (BOM付きUTF-8バイト列) にする (既存Webアプリと同一形式)"""
    error_csv_content = "ファイル名,エラー内容,エラー種別\n"
    for error in errors:
        error_csv_content += f'"{error["file"]}","{error["error"]}","{error["type"]}"\n'
    return error_csv_content.encode('utf-8-sig')
