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
import io
import re

import fitz  # PyMuPDF

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


def build_shipping_barcode_map(label_files, extractor, duplicates=None):
    """送り状PDF群からバーコードを読み、配送番号→(fitz doc, ページ番号) の対応表を作る。

    label_files: [(ファイル名, PDFバイト列)]
    extractor:   extract_barcodes_from_doc(doc) を持つオブジェクト (BarcodeExtractor)
    duplicates:  listを渡すと、同じ配送番号が異なるページで検出された場合に
                 その配送番号を追記する (Driveワーカーの競合検知用。
                 Webアプリは渡さない = 従来どおり後勝ち上書きで挙動不変)
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


def match_orders(extracted_orders, order_shipping_map, barcode_map):
    """注文番号を配送番号→送り状ページへ解決する。

    戻り値: (matched_pages, unmatched_orders)
      matched_pages は納品書の出現順の [(fitz doc, ページ番号)]。
    """
    matched_pages = []
    unmatched_orders = []
    for order_number in extracted_orders:
        shipping_number = order_shipping_map.get(order_number)
        if shipping_number and shipping_number in barcode_map:
            matched_pages.append(barcode_map[shipping_number])
        else:
            unmatched_orders.append(order_number)
    return matched_pages, unmatched_orders


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
