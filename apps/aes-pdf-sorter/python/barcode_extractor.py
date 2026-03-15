#!/usr/bin/env python3
"""
調整された枠設定を使用してPDFからバーコードを読み取るスクリプト
"""

import fitz  # PyMuPDF
import cv2
import numpy as np
from pyzbar import pyzbar
from pyzbar.pyzbar import ZBarSymbol
import json
import os
import sys
import time
from tqdm import tqdm

class BarcodeExtractor:
    def __init__(self, config_file="bbox_config.json"):
        self.config_file = config_file
        self.boxes = {}
        self.load_config()

    def load_config(self):
        if not os.path.exists(self.config_file):
            # config_fileが見つからない場合、スクリプトのディレクトリを試す
            script_dir = os.path.dirname(os.path.abspath(__file__))
            alt_path = os.path.join(script_dir, os.path.basename(self.config_file))
            if os.path.exists(alt_path):
                self.config_file = alt_path
            else:
                print(f"設定ファイルが見つかりません: {self.config_file}")
                sys.exit(1)

        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)

            for name, box_data in config.items():
                self.boxes[name] = {
                    'rect': fitz.Rect(box_data['rect']),
                    'color': tuple(box_data['color']),
                    'name': box_data['name']
                }

            print(f"設定ファイル '{self.config_file}' から枠設定を読み込みました")

        except Exception as e:
            print(f"設定ファイル読み込みエラー: {e}")
            sys.exit(1)

    def extract_region_image(self, page, rect, box_name=None):
        try:
            if box_name == '青枠':
                target_dpi = 216
            else:
                target_dpi = 288

            scale = target_dpi / 72.0
            mat = fitz.Matrix(scale, scale)

            pix = page.get_pixmap(matrix=mat, clip=rect, colorspace=fitz.csGRAY, alpha=False)

            samples = pix.samples
            width = pix.width
            height = pix.height

            gray_array = np.frombuffer(samples, dtype=np.uint8).reshape((height, width))
            enhanced_img = self.enhance_barcode_image_from_array(gray_array)

            return enhanced_img

        except Exception as e:
            print(f"画像切り出しエラー: {e}")
            return None

    def enhance_barcode_image_from_array(self, gray_array):
        try:
            _, binary = cv2.threshold(gray_array, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
            return binary
        except Exception as e:
            print(f"画像処理エラー: {e}")
            return np.where(gray_array > 127, 255, 0).astype(np.uint8)

    def add_white_padding_numpy(self, gray_array, horizontal_padding=25, vertical_padding=10):
        try:
            height, width = gray_array.shape
            new_width = width + (horizontal_padding * 2)
            new_height = height + (vertical_padding * 2)
            padded_array = np.full((new_height, new_width), 255, dtype=np.uint8)
            padded_array[vertical_padding:vertical_padding + height,
                        horizontal_padding:horizontal_padding + width] = gray_array
            return padded_array
        except Exception as e:
            print(f"パディング追加エラー: {e}")
            return gray_array

    def read_barcode_from_numpy(self, gray_array, barcode_types, debug_info=None):
        try:
            padded_array = self.add_white_padding_numpy(gray_array)

            symbols = []
            if isinstance(barcode_types, str):
                barcode_types = [barcode_types]

            for barcode_type in barcode_types:
                if barcode_type == 'CODE128':
                    symbols.append(ZBarSymbol.CODE128)
                elif barcode_type == 'CODABAR':
                    symbols.append(ZBarSymbol.CODABAR)
                elif barcode_type == 'I25':
                    symbols.append(ZBarSymbol.I25)

            if not symbols:
                symbols = [ZBarSymbol.CODE128, ZBarSymbol.CODABAR, ZBarSymbol.I25]

            barcodes = pyzbar.decode(padded_array, symbols=symbols)

            if barcodes:
                barcode = barcodes[0]
                raw_data = barcode.data.decode('utf-8')

                if barcode.type == 'CODABAR' or 'CODABAR' in barcode_types:
                    cleaned_data = raw_data
                    if len(raw_data) >= 2:
                        start_char = raw_data[0].upper()
                        end_char = raw_data[-1].upper()
                        if start_char in 'ABCD' and end_char in 'ABCD':
                            cleaned_data = raw_data[1:-1]
                    return cleaned_data
                else:
                    return raw_data
            else:
                return None

        except Exception as e:
            if debug_info:
                print(f"{debug_info} バーコード読み取りエラー: {e}")
            return None

    def extract_barcodes_from_pdf_return_data(self, pdf_path):
        if not os.path.exists(pdf_path):
            return []

        barcodes_data = []

        doc = None
        try:
            doc = fitz.open(pdf_path)

            with tqdm(total=doc.page_count, desc="バーコード読み取り中", unit="ページ") as pbar:
                for page_num in range(doc.page_count):
                    page = doc.load_page(page_num)

                    box_configs = {
                        '赤枠': ['CODABAR'],
                        '青枠': ['CODE128'],
                        '緑枠': ['CODABAR']
                    }

                    for box_name in ['青枠', '緑枠', '赤枠']:
                        if box_name in self.boxes:
                            rect = self.boxes[box_name]['rect']
                            barcode_types = box_configs[box_name]

                            binary_array = self.extract_region_image(page, rect, box_name)

                            if binary_array is not None:
                                debug_label = None
                                barcode_data = self.read_barcode_from_numpy(binary_array, barcode_types, debug_label)

                                if barcode_data:
                                    barcodes_data.append({
                                        'page': page_num,
                                        'data': barcode_data,
                                        'format': barcode_types[0],
                                        'box': box_name
                                    })
                                    break

                    pbar.update(1)

            return barcodes_data

        except Exception as e:
            print(f"PDF処理エラー: {e}")
            return []
        finally:
            if doc is not None:
                doc.close()

def main():
    if len(sys.argv) < 2:
        print("使用方法: python barcode_extractor.py <PDFファイル>")
        sys.exit(1)

    pdf_path = sys.argv[1]
    extractor = BarcodeExtractor()
    extractor.extract_barcodes_from_pdf(pdf_path)

if __name__ == "__main__":
    main()
