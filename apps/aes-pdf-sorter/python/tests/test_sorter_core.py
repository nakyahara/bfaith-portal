# -*- coding: utf-8 -*-
"""sorter_core の単体テスト

実行: apps/aes-pdf-sorter/python で `python -m unittest discover -s tests -v`
依存: PyMuPDF のみ (バーコード読み取り器はフェイクを注入する)
"""
import unittest

import fitz

import sorter_core


def make_pdf(page_texts):
    """各ページに指定テキストを描いたPDFバイト列を作る"""
    doc = fitz.open()
    for text in page_texts:
        page = doc.new_page()
        page.insert_text((72, 72), text)
    data = doc.tobytes()
    doc.close()
    return data


def page_texts(pdf_bytes):
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        return [doc.load_page(i).get_text().strip() for i in range(doc.page_count)]
    finally:
        doc.close()


class FakeExtractor:
    """extract_barcodes_from_doc を持つフェイク。ページ順にバーコード値を返す"""

    def __init__(self, barcodes_per_doc):
        self.barcodes_per_doc = barcodes_per_doc
        self.calls = 0

    def extract_barcodes_from_doc(self, doc):
        result = self.barcodes_per_doc[self.calls]
        self.calls += 1
        return result


class FailingExtractor:
    def extract_barcodes_from_doc(self, doc):
        raise RuntimeError("boom")


class DecodeCsvBytesTest(unittest.TestCase):
    def test_cp932(self):
        text = '注文番号,配送番号\r\n123-1234567-1234567,DA100\r\n'
        self.assertEqual(sorter_core.decode_csv_bytes(text.encode('cp932')), text)

    def test_utf8_sig(self):
        # cp932で読めないバイト列 → utf-8-sig にフォールバック
        text = '注文番号,配送番号,備考\r\nA,B,\U0001F600\r\n'
        decoded = sorter_core.decode_csv_bytes(text.encode('utf-8-sig'))
        self.assertEqual(decoded, text)

    def test_undecodable(self):
        self.assertIsNone(sorter_core.decode_csv_bytes(b'\x81'))


class BuildOrderShippingMapTest(unittest.TestCase):
    def test_basic(self):
        csv_text = (
            '注文番号,配送番号,その他\n'
            '249-1111111-1111111,DA100,x\n'
            '249-2222222-2222222,DA200,y\n'
            ',DA300,\n'                      # 注文番号空 → 無視
            '249-3333333-3333333,,\n'        # 配送番号空 → 無視
        )
        m = {}
        self.assertIsNone(sorter_core.build_order_shipping_map(csv_text, m))
        self.assertEqual(m, {
            '249-1111111-1111111': 'DA100',
            '249-2222222-2222222': 'DA200',
        })

    def test_multi_sku_rows_share_shipping_number(self):
        # 複数SKU注文 = 同一注文番号が複数行 (配送番号は同一)
        csv_text = (
            '注文番号,配送番号\n'
            '249-1111111-1111111,DA100\n'
            '249-1111111-1111111,DA100\n'
        )
        m = {}
        sorter_core.build_order_shipping_map(csv_text, m)
        self.assertEqual(m, {'249-1111111-1111111': 'DA100'})

    def test_missing_header(self):
        m = {}
        self.assertEqual(sorter_core.build_order_shipping_map('foo,bar\n1,2\n', m), 'csv_header')
        self.assertEqual(m, {})

    def test_extra_columns_ok(self):
        # logi_dispatch.csv のような多列CSVでも列名で拾える
        csv_text = 'ショップコード,ショップ名,注文番号,行no,配送番号\n4,店,249-1111111-1111111,1,DA100\n'
        m = {}
        self.assertIsNone(sorter_core.build_order_shipping_map(csv_text, m))
        self.assertEqual(m, {'249-1111111-1111111': 'DA100'})


class BuildShippingBarcodeMapTest(unittest.TestCase):
    def test_basic_and_codabar_strip(self):
        pdf = make_pdf(['label page 0', 'label page 1'])
        extractor = FakeExtractor([[
            {'page': 0, 'data': 'DA1550380980', 'format': 'CODE128', 'box': '青枠'},
            {'page': 1, 'data': 'A484151453214', 'format': 'CODABAR', 'box': '赤枠'},
        ]])
        barcode_map, errors, docs = sorter_core.build_shipping_barcode_map(
            [('AES_test.pdf', pdf)], extractor)
        try:
            self.assertEqual(errors, [])
            # CODE128 はそのまま / CODABAR は両端がA-Dの場合のみその文字を除去
            self.assertIn('DA1550380980', barcode_map)
            self.assertIn('484151453214', barcode_map)  # 先頭Aは除去・末尾は数字なので残る
            self.assertEqual(barcode_map['DA1550380980'][1], 0)
            self.assertEqual(barcode_map['484151453214'][1], 1)
        finally:
            for d in docs:
                d.close()

    def test_codabar_strip_exact(self):
        pdf = make_pdf(['p0'])
        extractor = FakeExtractor([[
            {'page': 0, 'data': 'A123456B', 'format': 'CODABAR', 'box': '赤枠'},
        ]])
        barcode_map, errors, docs = sorter_core.build_shipping_barcode_map(
            [('AES_x.pdf', pdf)], extractor)
        try:
            self.assertEqual(list(barcode_map.keys()), ['123456'])
        finally:
            for d in docs:
                d.close()

    def test_no_barcode_detected(self):
        pdf = make_pdf(['p0'])
        extractor = FakeExtractor([[]])
        barcode_map, errors, docs = sorter_core.build_shipping_barcode_map(
            [('AES_x.pdf', pdf)], extractor)
        try:
            self.assertEqual(barcode_map, {})
            self.assertEqual(len(errors), 1)
            self.assertEqual(errors[0]['type'], 'barcode_detection')
        finally:
            for d in docs:
                d.close()

    def test_extractor_exception(self):
        pdf = make_pdf(['p0'])
        barcode_map, errors, docs = sorter_core.build_shipping_barcode_map(
            [('AES_x.pdf', pdf)], FailingExtractor())
        try:
            self.assertEqual(barcode_map, {})
            types = [e['type'] for e in errors]
            self.assertEqual(types, ['barcode_processing', 'barcode_detection'])
        finally:
            for d in docs:
                d.close()

    def test_broken_pdf(self):
        barcode_map, errors, docs = sorter_core.build_shipping_barcode_map(
            [('AES_x.pdf', b'not a pdf')], FakeExtractor([[]]))
        try:
            self.assertEqual(barcode_map, {})
            self.assertEqual(len(errors), 1)
            self.assertEqual(errors[0]['type'], 'shipping_label_processing')
        finally:
            for d in docs:
                d.close()

    def test_duplicates_recorded_when_list_passed(self):
        # 同じ配送番号が別ページで検出された場合、duplicates リストに記録される
        pdf = make_pdf(['p0', 'p1'])
        extractor = FakeExtractor([[
            {'page': 0, 'data': 'DA100', 'format': 'CODE128', 'box': '青枠'},
            {'page': 1, 'data': 'DA100', 'format': 'CODE128', 'box': '青枠'},
        ]])
        duplicates = []
        barcode_map, errors, docs = sorter_core.build_shipping_barcode_map(
            [('AES_x.pdf', pdf)], extractor, duplicates=duplicates)
        try:
            self.assertEqual(duplicates, ['DA100'])
            # マップ自体は従来どおり後勝ち (Webアプリ挙動の保持)
            self.assertEqual(barcode_map['DA100'][1], 1)
        finally:
            for d in docs:
                d.close()

    def test_duplicates_not_recorded_without_list(self):
        # duplicates を渡さない場合 (Webアプリ経路) は従来どおり黙って上書き
        pdf = make_pdf(['p0', 'p1'])
        extractor = FakeExtractor([[
            {'page': 0, 'data': 'DA100', 'format': 'CODE128', 'box': '青枠'},
            {'page': 1, 'data': 'DA100', 'format': 'CODE128', 'box': '青枠'},
        ]])
        barcode_map, errors, docs = sorter_core.build_shipping_barcode_map(
            [('AES_x.pdf', pdf)], extractor)
        try:
            self.assertEqual(errors, [])
            self.assertEqual(barcode_map['DA100'][1], 1)
        finally:
            for d in docs:
                d.close()


class ExtractOrderNumbersTest(unittest.TestCase):
    def test_order_and_dedup(self):
        pdf = make_pdf([
            'order: 249-1111111-1111111 and 249-2222222-2222222',
            'repeat 249-1111111-1111111 then 503-3333333-3333333',
            'not-an-order 12-3456-789',
        ])
        doc = fitz.open(stream=pdf, filetype="pdf")
        try:
            orders = sorter_core.extract_order_numbers(doc)
        finally:
            doc.close()
        self.assertEqual(orders, [
            '249-1111111-1111111',
            '249-2222222-2222222',
            '503-3333333-3333333',
        ])


class MatchOrdersTest(unittest.TestCase):
    def test_order_preserved_and_unmatched(self):
        order_map = {
            'O1': 'DA100',
            'O2': 'DA200',
            'O3': 'DA999',   # バーコード側に無い
        }
        barcode_map = {
            'DA100': ('doc', 5),
            'DA200': ('doc', 2),
        }
        matched, unmatched = sorter_core.match_orders(
            ['O2', 'O1', 'O3', 'O4'], order_map, barcode_map)
        self.assertEqual(matched, [('doc', 2), ('doc', 5)])  # 納品書の出現順
        self.assertEqual(unmatched, ['O3', 'O4'])


class BuildLabelPdfTest(unittest.TestCase):
    def test_page_order_matches_invoice_order(self):
        label_pdf = make_pdf(['LABEL-A', 'LABEL-B', 'LABEL-C'])
        doc = fitz.open(stream=label_pdf, filetype="pdf")
        try:
            out = sorter_core.build_label_pdf([(doc, 2), (doc, 0), (doc, 1)])
        finally:
            doc.close()
        self.assertEqual(page_texts(out), ['LABEL-C', 'LABEL-A', 'LABEL-B'])


class BuildErrorCsvTest(unittest.TestCase):
    def test_format(self):
        data = sorter_core.build_error_csv([
            {'file': 'f1.pdf', 'error': 'エラー1', 'type': 't1'},
        ])
        text = data.decode('utf-8-sig')
        self.assertEqual(
            text,
            'ファイル名,エラー内容,エラー種別\n"f1.pdf","エラー1","t1"\n')


if __name__ == '__main__':
    unittest.main()
