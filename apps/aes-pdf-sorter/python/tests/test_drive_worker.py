# -*- coding: utf-8 -*-
"""drive_worker の単体テスト (Drive APIはフェイクで代替)

実行: apps/aes-pdf-sorter/python で `python -m unittest discover -s tests -v`
"""
import unittest
from datetime import datetime, timedelta, timezone

import fitz

import drive_worker
from drive_worker import (
    Worker, decide_action, is_aes_pattern, parse_pattern_name,
    parse_poll_hours, within_hours,
    OUTPUT_PDF_NAME, ERROR_TXT_NAME, CSV_NAME, FOLDER_MIME,
)

UTC = timezone.utc

OLD = '2026-07-01T00:00:00.000Z'      # 十分昔 (整定済み)
OLDER = '2026-06-01T00:00:00.000Z'


def make_pdf(page_texts):
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


def meta(name, mtime=OLD, mime='application/pdf'):
    return {'id': f'id-{name}', 'name': name, 'mimeType': mime, 'modifiedTime': mtime}


class ParsePatternNameTest(unittest.TestCase):
    def test_bom_and_crlf(self):
        text = '﻿パターン番号: 21\r\nパターン表示名: AES《単品》\r\n伝票数: 12\r\n'
        self.assertEqual(parse_pattern_name(text), 'AES《単品》')

    def test_missing_line(self):
        self.assertIsNone(parse_pattern_name('パターン番号: 21\r\n'))
        self.assertIsNone(parse_pattern_name(None))


class IsAesPatternTest(unittest.TestCase):
    def test_by_content(self):
        self.assertTrue(is_aes_pattern('引当パターン_x.txt', 'パターン表示名: AES《1SKU複数個》\r\n'))
        self.assertFalse(is_aes_pattern('引当パターン_AES《単品》.txt', 'パターン表示名: ネコポス《単品》\r\n'))

    def test_fallback_filename(self):
        # 中身が読めない場合のみファイル名で判定
        self.assertTrue(is_aes_pattern('引当パターン_AES《単品》.txt', None))
        self.assertFalse(is_aes_pattern('引当パターン_LINEギフト.txt', 'ゴミデータ'))


class DecideActionTest(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 7, 22, 12, 0, 0, tzinfo=UTC)

    def test_no_attempt_settled(self):
        self.assertEqual(
            decide_action(self.now, [meta('納品書_20.pdf')], None, None),
            'process')

    def test_attempt_newer_than_inputs(self):
        out = meta(OUTPUT_PDF_NAME, '2026-07-02T00:00:00.000Z')
        self.assertEqual(
            decide_action(self.now, [meta('納品書_20.pdf', OLD)], out, None),
            'skip_done')

    def test_error_attempt_blocks_until_inputs_change(self):
        err = meta(ERROR_TXT_NAME, '2026-07-02T00:00:00.000Z')
        self.assertEqual(
            decide_action(self.now, [meta('納品書_20.pdf', OLD)], None, err),
            'skip_done')

    def test_inputs_newer_than_attempt_reprocess(self):
        err = meta(ERROR_TXT_NAME, '2026-07-02T00:00:00.000Z')
        newer_input = meta(CSV_NAME, '2026-07-03T00:00:00.000Z')
        self.assertEqual(
            decide_action(self.now, [meta('納品書_20.pdf', OLD), newer_input], None, err),
            'process')

    def test_settling_wait(self):
        recent = (self.now - timedelta(seconds=30)).strftime('%Y-%m-%dT%H:%M:%S.000Z')
        self.assertEqual(
            decide_action(self.now, [meta('納品書_20.pdf', recent)], None, None),
            'skip_settling')


class HoursTest(unittest.TestCase):
    def test_parse(self):
        self.assertEqual(parse_poll_hours('7-22'), (7, 22))
        with self.assertRaises(ValueError):
            parse_poll_hours('abc')

    def test_within_normal(self):
        jst = drive_worker.JST
        self.assertTrue(within_hours(datetime(2026, 7, 22, 7, 0, tzinfo=jst), (7, 22)))
        self.assertTrue(within_hours(datetime(2026, 7, 22, 21, 59, tzinfo=jst), (7, 22)))
        self.assertFalse(within_hours(datetime(2026, 7, 22, 22, 0, tzinfo=jst), (7, 22)))
        self.assertFalse(within_hours(datetime(2026, 7, 22, 3, 0, tzinfo=jst), (7, 22)))

    def test_within_wrap(self):
        jst = drive_worker.JST
        self.assertTrue(within_hours(datetime(2026, 7, 22, 23, 0, tzinfo=jst), (22, 7)))
        self.assertTrue(within_hours(datetime(2026, 7, 22, 3, 0, tzinfo=jst), (22, 7)))
        self.assertFalse(within_hours(datetime(2026, 7, 22, 12, 0, tzinfo=jst), (22, 7)))


# ───────────────────────── E2E (フェイクDrive) ─────────────────────────

class FakeDriveClient:
    """children/contents を辞書で持つフェイク。書き込みを記録する"""

    def __init__(self, children, contents):
        self.children = children    # folder_id -> [meta]
        self.contents = contents    # file_id -> bytes
        self.uploads = []           # (folder_id, name, content, mimetype)
        self.overwrites = []        # (file_id, content, mimetype)

    def list_children(self, folder_id):
        return list(self.children.get(folder_id, []))

    def download(self, file_id):
        return self.contents[file_id]

    def upload_new(self, folder_id, name, content, mimetype):
        self.uploads.append((folder_id, name, content, mimetype))
        return f'new-{name}'

    def overwrite(self, file_id, content, mimetype):
        self.overwrites.append((file_id, content, mimetype))
        return file_id


class FakeExtractor:
    def __init__(self, barcodes_per_doc):
        self.barcodes_per_doc = barcodes_per_doc
        self.calls = 0

    def extract_barcodes_from_doc(self, doc):
        result = self.barcodes_per_doc[self.calls]
        self.calls += 1
        return result


class FakeConfig:
    root_id = 'ROOT'
    material_folder_id = 'MATERIAL'
    csv_folder_id = 'CSVDIR'


def build_world(*, csv_text=None, include_labels=True, folder_files=(), contents=()):
    """テスト用のDrive世界を組み立てる"""
    children = {
        'ROOT': [meta('出荷_20', mime=FOLDER_MIME)],
        'MATERIAL': [],
        'CSVDIR': [],
        'id-出荷_20': list(folder_files),
    }
    all_contents = dict(contents)
    if include_labels:
        children['MATERIAL'].append(meta('AES_labels.pdf'))
    if csv_text is not None:
        children['CSVDIR'].append(meta(CSV_NAME, mime='text/csv'))
        all_contents[f'id-{CSV_NAME}'] = csv_text.encode('cp932')
    return children, all_contents


CSV_TEXT = (
    '注文番号,配送番号\n'
    '249-1111111-1111111,DA100\n'
    '249-2222222-2222222,DA200\n'
)

PATTERN_AES = '﻿パターン番号: 21\r\nパターン表示名: AES《単品》\r\n'
PATTERN_NEKOPOSU = '﻿パターン番号: 27\r\nパターン表示名: ネコポス《全て》\r\n'


class WorkerE2ETest(unittest.TestCase):
    def _run(self, children, contents, barcodes):
        client = FakeDriveClient(children, contents)
        worker = Worker(FakeConfig(), client,
                        extractor_factory=lambda: FakeExtractor([barcodes]))
        worker.run_cycle()
        return client

    def test_success_outputs_sorted_pdf(self):
        label_pdf = make_pdf(['LABEL-DA100', 'LABEL-DA200'])
        # 納品書の注文順は 222… → 111… (送り状は逆順で並ぶはず)
        invoice_pdf = make_pdf(['249-2222222-2222222', '249-1111111-1111111'])
        folder_files = [
            meta('引当パターン_AES《単品》.txt', mime='text/plain'),
            meta('納品書_20.pdf'),
        ]
        contents = {
            'id-引当パターン_AES《単品》.txt': PATTERN_AES.encode('utf-8'),
            'id-納品書_20.pdf': invoice_pdf,
            'id-AES_labels.pdf': label_pdf,
        }
        children, all_contents = build_world(
            csv_text=CSV_TEXT, folder_files=folder_files, contents=contents)

        client = self._run(children, all_contents, [
            {'page': 0, 'data': 'DA100', 'format': 'CODE128', 'box': '青枠'},
            {'page': 1, 'data': 'DA200', 'format': 'CODE128', 'box': '青枠'},
        ])

        self.assertEqual(len(client.uploads), 1)
        folder_id, name, content, mimetype = client.uploads[0]
        self.assertEqual((folder_id, name, mimetype), ('id-出荷_20', OUTPUT_PDF_NAME, 'application/pdf'))
        # 納品書の注文順 (DA200 → DA100) に並んでいること
        self.assertEqual(page_texts(content), ['LABEL-DA200', 'LABEL-DA100'])
        self.assertEqual(client.overwrites, [])

    def test_non_aes_folder_skipped(self):
        folder_files = [
            meta('引当パターン_ネコポス《全て》.txt', mime='text/plain'),
            meta('納品書_20.pdf'),
        ]
        contents = {
            'id-引当パターン_ネコポス《全て》.txt': PATTERN_NEKOPOSU.encode('utf-8'),
        }
        children, all_contents = build_world(
            csv_text=CSV_TEXT, folder_files=folder_files, contents=contents)

        client = self._run(children, all_contents, [])
        self.assertEqual(client.uploads, [])
        self.assertEqual(client.overwrites, [])

    def test_no_invoice_skipped(self):
        folder_files = [meta('引当パターン_AES《単品》.txt', mime='text/plain')]
        children, all_contents = build_world(
            csv_text=CSV_TEXT, folder_files=folder_files,
            contents={'id-引当パターン_AES《単品》.txt': PATTERN_AES.encode('utf-8')})
        client = self._run(children, all_contents, [])
        self.assertEqual(client.uploads, [])

    def test_unmatched_order_writes_error_txt_not_pdf(self):
        label_pdf = make_pdf(['LABEL-DA100'])
        invoice_pdf = make_pdf(['249-1111111-1111111 249-9999999-9999999'])
        folder_files = [
            meta('引当パターン_AES《単品》.txt', mime='text/plain'),
            meta('納品書_20.pdf'),
        ]
        contents = {
            'id-引当パターン_AES《単品》.txt': PATTERN_AES.encode('utf-8'),
            'id-納品書_20.pdf': invoice_pdf,
            'id-AES_labels.pdf': label_pdf,
        }
        children, all_contents = build_world(
            csv_text=CSV_TEXT, folder_files=folder_files, contents=contents)

        client = self._run(children, all_contents, [
            {'page': 0, 'data': 'DA100', 'format': 'CODE128', 'box': '青枠'},
        ])

        # 部分出力しない: PDFは出さずエラーtxtのみ
        self.assertEqual(len(client.uploads), 1)
        folder_id, name, content, mimetype = client.uploads[0]
        self.assertEqual(name, ERROR_TXT_NAME)
        text = content.decode('utf-8')
        self.assertIn('249-9999999-9999999', text)
        self.assertIn('失敗しました', text)

    def test_missing_csv_writes_error_txt(self):
        invoice_pdf = make_pdf(['249-1111111-1111111'])
        folder_files = [
            meta('引当パターン_AES《単品》.txt', mime='text/plain'),
            meta('納品書_20.pdf'),
        ]
        contents = {
            'id-引当パターン_AES《単品》.txt': PATTERN_AES.encode('utf-8'),
            'id-納品書_20.pdf': invoice_pdf,
        }
        children, all_contents = build_world(
            csv_text=None, folder_files=folder_files, contents=contents)

        client = self._run(children, all_contents, [])
        self.assertEqual(len(client.uploads), 1)
        _, name, content, _ = client.uploads[0]
        self.assertEqual(name, ERROR_TXT_NAME)
        self.assertIn(CSV_NAME, content.decode('utf-8'))

    def test_existing_error_txt_overwritten_and_resolved_on_success(self):
        label_pdf = make_pdf(['LABEL-DA100'])
        invoice_pdf = make_pdf(['249-1111111-1111111'])
        # 前回エラーtxtあり + 入力の方が新しい (再処理される状況)
        folder_files = [
            meta('引当パターン_AES《単品》.txt', mime='text/plain'),
            meta('納品書_20.pdf', '2026-07-02T00:00:00.000Z'),
            meta(ERROR_TXT_NAME, '2026-07-01T12:00:00.000Z', mime='text/plain'),
        ]
        contents = {
            'id-引当パターン_AES《単品》.txt': PATTERN_AES.encode('utf-8'),
            'id-納品書_20.pdf': invoice_pdf,
            'id-AES_labels.pdf': label_pdf,
        }
        children, all_contents = build_world(
            csv_text=CSV_TEXT, folder_files=folder_files, contents=contents)

        client = self._run(children, all_contents, [
            {'page': 0, 'data': 'DA100', 'format': 'CODE128', 'box': '青枠'},
        ])

        # PDFは新規作成、エラーtxtは削除せず「解消済み」に上書き
        self.assertEqual(len(client.uploads), 1)
        self.assertEqual(client.uploads[0][1], OUTPUT_PDF_NAME)
        self.assertEqual(len(client.overwrites), 1)
        file_id, content, mimetype = client.overwrites[0]
        self.assertEqual(file_id, f'id-{ERROR_TXT_NAME}')
        self.assertIn('解消済み', content.decode('utf-8'))

    def test_skip_when_output_up_to_date(self):
        folder_files = [
            meta('引当パターン_AES《単品》.txt', mime='text/plain'),
            meta('納品書_20.pdf', OLDER),
            meta(OUTPUT_PDF_NAME, OLD),  # 出力が入力より新しい
        ]
        children, all_contents = build_world(
            csv_text=CSV_TEXT, folder_files=folder_files,
            contents={'id-引当パターン_AES《単品》.txt': PATTERN_AES.encode('utf-8')})
        # 素材のmtimeもOLDERに揃える (入力側が古い状態を作る)
        for f in children['MATERIAL'] + children['CSVDIR']:
            f['modifiedTime'] = OLDER
        for f in children['id-出荷_20']:
            if f['name'] != OUTPUT_PDF_NAME:
                f['modifiedTime'] = OLDER

        client = self._run(children, all_contents, [])
        self.assertEqual(client.uploads, [])
        self.assertEqual(client.overwrites, [])


if __name__ == '__main__':
    unittest.main()
