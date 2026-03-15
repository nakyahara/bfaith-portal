"""
AES PDF Sorter - 内部バックエンドサービス
ポータルから認証済みリクエストがプロキシされるため、独自認証は不要
"""
import csv
import io
import re
from datetime import datetime
import fitz  # PyMuPDF
from typing import List
import traceback
import tempfile
import shutil
import os
import uuid
from barcode_extractor import BarcodeExtractor
import threading
import time
import urllib.parse

from fastapi import FastAPI, UploadFile, File, HTTPException, status
from fastapi.responses import StreamingResponse, JSONResponse

app = FastAPI()

# 一時ファイル管理用の設定
TEMP_DIR = tempfile.mkdtemp(prefix="aes_temp_")
temp_files_store = {}

import atexit

def cleanup_temp_dir():
    try:
        if os.path.exists(TEMP_DIR):
            shutil.rmtree(TEMP_DIR)
            print(f"一時ディレクトリを削除しました: {TEMP_DIR}")
    except Exception as e:
        print(f"一時ディレクトリの削除に失敗しました: {e}")

atexit.register(cleanup_temp_dir)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/process_aes_sorting")
async def process_aes_sorting(files: List[UploadFile] = File(...)):
    """AESラベル並び替え機能のメインエンドポイント"""
    process_time = datetime.now().strftime("%m%d%H%M")

    # 1. ファイル名検証
    file_validation_errors = []
    shipping_labels = []
    order_csvs = []
    invoice_pdfs = []

    for file in files:
        if not file.filename:
            file_validation_errors.append("不明なファイル: ファイル名が取得できません")
            continue
        filename = file.filename
        if filename.startswith("AES") and filename.lower().endswith(".pdf"):
            shipping_labels.append(file)
        elif filename.startswith("logi_jyuchu") and filename.lower().endswith(".csv"):
            order_csvs.append(file)
        elif filename.startswith("TMP") and filename.lower().endswith(".pdf"):
            invoice_pdfs.append(file)
        else:
            file_validation_errors.append(f"{filename}: ファイル名が規則に合致しません（AES*.pdf, logi_jyuchu*.csv, TMP*.pdfのいずれか）")

    if file_validation_errors:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"ファイル名検証エラー:\n" + "\n".join(file_validation_errors)
        )

    if not order_csvs:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="オーダーCSVファイル（logi_jyuchuで始まるファイル）が見つかりません。")
    if not shipping_labels:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="配送ラベルPDFファイル（AESで始まるファイル）が見つかりません。")
    if not invoice_pdfs:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="納品書PDFファイル（TMPで始まるファイル）が見つかりません。")

    common_errors = []

    try:
        # 3. オーダーCSVから注文番号と配送番号のマッピング作成
        order_shipping_map = {}

        for csv_file in order_csvs:
            try:
                csv_content = await csv_file.read()
                try:
                    csv_text = csv_content.decode('cp932')
                except UnicodeDecodeError:
                    try:
                        csv_text = csv_content.decode('utf-8-sig')
                    except UnicodeDecodeError:
                        common_errors.append({"file": csv_file.filename, "error": "CSVの文字コードが不正です。", "type": "csv_encoding"})
                        continue

                reader = csv.DictReader(io.StringIO(csv_text))
                if "注文番号" not in reader.fieldnames or "配送番号" not in reader.fieldnames:
                    common_errors.append({"file": csv_file.filename, "error": "CSVヘッダーに「注文番号」または「配送番号」列が見つかりません。", "type": "csv_header"})
                    continue

                for row in reader:
                    order_number = row.get("注文番号", "").strip()
                    shipping_number = row.get("配送番号", "").strip()
                    if order_number and shipping_number:
                        order_shipping_map[order_number] = shipping_number
            except Exception as e:
                common_errors.append({"file": csv_file.filename, "error": f"CSV処理エラー: {str(e)}", "type": "csv_processing"})

        if not order_shipping_map:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="オーダーCSVから有効なデータを読み取れませんでした。")

        # 4. 配送ラベルからバーコード読み取り
        shipping_barcode_map = {}
        extractor = BarcodeExtractor()

        for label_file in shipping_labels:
            try:
                pdf_content = await label_file.read()
                pdf_doc = fitz.open(stream=pdf_content, filetype="pdf")

                temp_pdf_path = None
                try:
                    with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as temp_file:
                        temp_file.write(pdf_content)
                        temp_file.flush()
                        temp_pdf_path = temp_file.name

                    barcodes = extractor.extract_barcodes_from_pdf_return_data(temp_pdf_path)
                except Exception as e:
                    common_errors.append({"file": label_file.filename, "error": f"バーコード読み取り処理エラー: {str(e)}", "type": "barcode_processing"})
                    barcodes = []
                finally:
                    if temp_pdf_path and os.path.exists(temp_pdf_path):
                        try:
                            os.unlink(temp_pdf_path)
                        except:
                            pass

                if not barcodes:
                    common_errors.append({"file": label_file.filename, "error": "バーコードが検出されませんでした", "type": "barcode_detection"})
                    pdf_doc.close()
                    continue

                for barcode_info in barcodes:
                    page_num = barcode_info['page']
                    barcode_data = barcode_info['data']
                    if barcode_info['format'] == 'CODABAR':
                        barcode_data = re.sub(r'^[ABCD]|[ABCD]$', '', barcode_data)
                    shipping_barcode_map[barcode_data] = (pdf_doc, page_num)
            except Exception as e:
                common_errors.append({"file": label_file.filename, "error": f"配送ラベル処理エラー: {str(e)}", "type": "shipping_label_processing"})

        # 5. 納品書ごとの処理
        result_files = []
        sequence_number = 1

        for invoice_file in invoice_pdfs:
            invoice_errors = []
            pdf_doc = None

            for common_error in common_errors:
                invoice_errors.append({"file": common_error["file"], "error": common_error["error"], "type": common_error["type"]})

            try:
                pdf_content = await invoice_file.read()
                pdf_doc = fitz.open(stream=pdf_content, filetype="pdf")

                order_pattern = re.compile(r'\b\d{3}-\d{7}-\d{7}\b')
                extracted_orders = []

                for page_num in range(pdf_doc.page_count):
                    page = pdf_doc.load_page(page_num)
                    text = page.get_text()
                    matches = order_pattern.findall(text)
                    for match in matches:
                        if match not in extracted_orders:
                            extracted_orders.append(match)

                if not extracted_orders:
                    invoice_errors.append({"file": invoice_file.filename, "error": "注文番号パターン（3桁-7桁-7桁）が見つかりませんでした", "type": "order_number_extraction"})
                    if invoice_errors:
                        error_csv_content = "ファイル名,エラー内容,エラー種別\n"
                        for error in invoice_errors:
                            error_csv_content += f'"{error["file"]}","{error["error"]}","{error["type"]}"\n'
                        invoice_name = os.path.splitext(invoice_file.filename)[0]
                        error_filename = f"{invoice_name}_AES処理済_エラーログ_#{sequence_number:03d}_{process_time}.csv"
                        result_files.append({"type": "error_log", "filename": error_filename, "content": error_csv_content.encode('utf-8-sig')})
                        sequence_number += 1
                    continue

                # 6. 注文番号と配送番号の紐づけ
                matched_shipping_pages = []
                unmatched_orders = []

                for order_number in extracted_orders:
                    shipping_number = order_shipping_map.get(order_number)
                    if shipping_number and shipping_number in shipping_barcode_map:
                        shipping_doc, page_num = shipping_barcode_map[shipping_number]
                        matched_shipping_pages.append((shipping_doc, page_num))
                    else:
                        unmatched_orders.append(order_number)
                        invoice_errors.append({"file": invoice_file.filename, "error": f"注文番号 {order_number} に対応する配送ラベルが見つかりません", "type": "order_shipping_mismatch"})

                if matched_shipping_pages and not unmatched_orders:
                    invoice_bytes = pdf_doc.tobytes(garbage=4, deflate=True, clean=True)
                    invoice_name = os.path.splitext(invoice_file.filename)[0]
                    invoice_filename = f"{invoice_name}_AES処理済_納品書_#{sequence_number:03d}_{process_time}.pdf"
                    result_files.append({"type": "invoice", "filename": invoice_filename, "content": invoice_bytes})

                    label_doc = fitz.open()
                    for shipping_doc, page_num in matched_shipping_pages:
                        label_doc.insert_pdf(shipping_doc, from_page=page_num, to_page=page_num)
                    label_bytes = label_doc.tobytes(garbage=4, deflate=True, clean=True)
                    label_filename = f"{invoice_name}_AES処理済_ラベル_#{sequence_number:03d}_{process_time}.pdf"
                    result_files.append({"type": "label", "filename": label_filename, "content": label_bytes})
                    label_doc.close()
                    sequence_number += 1
                else:
                    if invoice_errors:
                        error_csv_content = "ファイル名,エラー内容,エラー種別\n"
                        for error in invoice_errors:
                            error_csv_content += f'"{error["file"]}","{error["error"]}","{error["type"]}"\n'
                        invoice_name = os.path.splitext(invoice_file.filename)[0]
                        error_filename = f"{invoice_name}_AES処理済_エラーログ_#{sequence_number:03d}_{process_time}.csv"
                        result_files.append({"type": "error_log", "filename": error_filename, "content": error_csv_content.encode('utf-8-sig')})
                        sequence_number += 1

            except Exception as e:
                invoice_errors.append({"file": invoice_file.filename, "error": f"納品書処理エラー: {str(e)}", "type": "invoice_processing"})
                if invoice_errors:
                    error_csv_content = "ファイル名,エラー内容,エラー種別\n"
                    for error in invoice_errors:
                        error_csv_content += f'"{error["file"]}","{error["error"]}","{error["type"]}"\n'
                    invoice_name = os.path.splitext(invoice_file.filename)[0]
                    error_filename = f"{invoice_name}_AES処理済_エラーログ_#{sequence_number:03d}_{process_time}.csv"
                    result_files.append({"type": "error_log", "filename": error_filename, "content": error_csv_content.encode('utf-8-sig')})
                    sequence_number += 1
            finally:
                try:
                    if pdf_doc:
                        pdf_doc.close()
                except:
                    pass

        # 7. 結果ファイルの返却
        if not result_files:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="処理できるファイルが見つかりませんでした。")

        session_id = str(uuid.uuid4())
        download_links = []

        if not os.path.exists(TEMP_DIR):
            try:
                os.makedirs(TEMP_DIR, exist_ok=True)
            except Exception as e:
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"一時ディレクトリの再生成に失敗しました: {e}")

        for file_info in result_files:
            file_id = str(uuid.uuid4())
            file_path = os.path.join(TEMP_DIR, f"{file_id}_{file_info['filename']}")
            with open(file_path, 'wb') as f:
                f.write(file_info["content"])

            temp_files_store[file_id] = {
                "filename": file_info["filename"],
                "file_path": file_path,
                "type": file_info["type"],
                "session_id": session_id,
                "created_at": time.time()
            }

            download_links.append({
                "filename": file_info["filename"],
                "type": file_info["type"],
                "download_id": file_id
            })

        successful_invoices = len([f for f in result_files if f["type"] == "invoice"])
        error_logs_count = len([f for f in result_files if f["type"] == "error_log"])

        return JSONResponse(content={
            "success": True,
            "processed_count": successful_invoices,
            "error_count": error_logs_count,
            "files": download_links,
            "session_id": session_id,
            "message": f"処理完了: 成功 {successful_invoices}件、エラー {error_logs_count}件"
        })

    except HTTPException as e:
        raise e
    except Exception as e:
        detailed_error = traceback.format_exc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"AES処理でエラーが発生しました: {e}\n--- スタックトレース ---\n{detailed_error}"
        )
    finally:
        try:
            for shipping_doc, _ in shipping_barcode_map.values():
                try:
                    shipping_doc.close()
                except:
                    pass
        except:
            pass


@app.get("/download/{file_id}")
async def download_file(file_id: str):
    if file_id not in temp_files_store:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="ファイルが見つかりません。")

    file_info = temp_files_store[file_id]
    filename = file_info["filename"]
    file_path = file_info["file_path"]
    file_type = file_info["type"]

    if not os.path.exists(file_path):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="ファイルが見つかりません。")

    with open(file_path, 'rb') as f:
        content = f.read()

    encoded_filename = urllib.parse.quote(filename, safe='')
    headers = {'Content-Disposition': f'attachment; filename*=UTF-8\'\'{encoded_filename}'}

    if file_type == "error_log":
        return StreamingResponse(io.BytesIO(content), media_type='text/csv', headers=headers)
    else:
        return StreamingResponse(io.BytesIO(content), media_type='application/pdf', headers=headers)


@app.get("/cleanup_session/{session_id}")
async def cleanup_session(session_id: str):
    deleted_count = 0
    files_to_delete = []

    for file_id, file_info in temp_files_store.items():
        if file_info.get("session_id") == session_id:
            files_to_delete.append(file_id)

    for file_id in files_to_delete:
        if file_id in temp_files_store:
            file_path = temp_files_store[file_id].get("file_path")
            if file_path and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except Exception as e:
                    print(f"ファイル削除エラー {file_path}: {e}")
            del temp_files_store[file_id]
            deleted_count += 1

    return JSONResponse(content={"success": True, "deleted_count": deleted_count, "message": f"{deleted_count}個のファイルを削除しました。"})


# 一時ファイルクリーンアップスレッド
def cleanup_temp_files():
    while True:
        try:
            current_time = time.time()
            expired_files = []
            for file_id, file_info in temp_files_store.items():
                if 'created_at' in file_info and current_time - file_info['created_at'] > 3600:
                    expired_files.append(file_id)

            for file_id in expired_files:
                if file_id in temp_files_store:
                    file_path = temp_files_store[file_id].get("file_path")
                    if file_path and os.path.exists(file_path):
                        try:
                            os.remove(file_path)
                        except:
                            pass
                    del temp_files_store[file_id]
            time.sleep(3600)
        except Exception as e:
            print(f"クリーンアップエラー: {e}")
            time.sleep(3600)

cleanup_thread = threading.Thread(target=cleanup_temp_files, daemon=True)
cleanup_thread.start()
