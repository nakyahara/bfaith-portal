# -*- coding: utf-8 -*-
"""drive_worker の単体テスト (Drive APIはフェイクで代替)

実行: apps/aes-pdf-sorter/python で `python -m unittest discover -s tests -v`
"""
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

import fitz

import drive_worker
from drive_worker import (
    Worker, decide_action, has_unacknowledged_duplicates, has_incomplete_failure_write,
    has_stale_dup_ack,
    is_aes_pattern, parse_pattern_name,
    parse_poll_hours, within_hours, natural_key, build_invalid_pdf,
    OUTPUT_PDF_NAME, ERROR_TXT_NAME, CSV_NAME, FOLDER_MIME,
    CONFLICT_MARKER_PROP, DUP_ACK_PROP,
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
            decide_action(self.now, [meta('納品書_20.pdf')], [], [], []),
            'process')

    def test_attempt_newer_than_inputs(self):
        out = meta(OUTPUT_PDF_NAME, '2026-07-02T00:00:00.000Z')
        self.assertEqual(
            decide_action(self.now, [meta('納品書_20.pdf', OLD)], [], [out], []),
            'skip_done')

    def test_error_attempt_blocks_until_inputs_change(self):
        err = meta(ERROR_TXT_NAME, '2026-07-02T00:00:00.000Z')
        self.assertEqual(
            decide_action(self.now, [meta('納品書_20.pdf', OLD)], [], [], [err]),
            'skip_done')

    def test_failure_state_retries_on_shared_change(self):
        # 失敗中 (マーカー付きエラーtxt) のフォルダは、素材 (CSV) の置き直しで再試行する
        err = dict(meta(ERROR_TXT_NAME, '2026-07-02T00:00:00.000Z'),
                   appProperties={CONFLICT_MARKER_PROP: '1'})
        newer_csv = meta(CSV_NAME, '2026-07-03T00:00:00.000Z')
        self.assertEqual(
            decide_action(self.now, [meta('納品書_20.pdf', OLD)], [newer_csv], [], [err]),
            'process')

    def test_success_state_ignores_shared_change(self):
        # 成功済み (マーカー無し出力PDF) のフォルダは、素材がバッチ入れ替えで新しく
        # なっても再処理しない (朝の正常PDFを午後の素材で潰さない。2026-07-23 実障害)
        out = meta(OUTPUT_PDF_NAME, '2026-07-02T00:00:00.000Z')
        newer_csv = meta(CSV_NAME, '2026-07-03T00:00:00.000Z')
        newer_label = meta('AES2.pdf', '2026-07-03T00:00:00.000Z')
        self.assertEqual(
            decide_action(self.now, [meta('納品書_20.pdf', OLD)],
                          [newer_csv, newer_label], [out], []),
            'skip_done')

    def test_success_state_reprocesses_on_local_change(self):
        # 成功済みでも、フォルダ内の納品書が入れ替われば再処理する (翌日の再利用)
        out = meta(OUTPUT_PDF_NAME, '2026-07-02T00:00:00.000Z')
        self.assertEqual(
            decide_action(self.now, [meta('納品書_20.pdf', '2026-07-03T00:00:00.000Z')],
                          [], [out], []),
            'process')

    def test_settling_wait(self):
        recent = (self.now - timedelta(seconds=30)).strftime('%Y-%m-%dT%H:%M:%S.000Z')
        self.assertEqual(
            decide_action(self.now, [meta('納品書_20.pdf', recent)], [], [], []),
            'skip_settling')

    def test_success_state_local_trigger_still_settles_on_shared(self):
        # 成功状態のトリガーはlocalのみだが、整定待ちは素材も含めて判定する
        # (素材の入れ替え途中に処理して一時的な不一致を出さない)
        recent = (self.now - timedelta(seconds=30)).strftime('%Y-%m-%dT%H:%M:%S.000Z')
        out = meta(OUTPUT_PDF_NAME, '2026-07-02T00:00:00.000Z')
        local = meta('納品書_20.pdf', '2026-07-03T00:00:00.000Z')  # 出力より新しい (トリガー)
        shared = meta(CSV_NAME, recent)  # 30秒前に入れ替わったばかり
        self.assertEqual(
            decide_action(self.now, [local], [shared], [out], []),
            'skip_settling')

    def test_partial_attempt_retries(self):
        # 片方のアーティファクトだけ入力より新しい = 前回の書き込みが途中で失敗した状態
        # → skipせず再処理する (Codex2巡目 指摘1の検証)
        out = meta(OUTPUT_PDF_NAME, '2026-07-03T00:00:00.000Z')   # 入力より新しい
        err = meta(ERROR_TXT_NAME, '2026-07-01T00:00:00.000Z')   # 入力より古い
        newer_input = meta('納品書_20.pdf', '2026-07-02T00:00:00.000Z')
        self.assertEqual(
            decide_action(self.now, [newer_input], [], [out], [err]),
            'process')


class HasUnacknowledgedDuplicatesTest(unittest.TestCase):
    def test_no_duplicates(self):
        self.assertFalse(has_unacknowledged_duplicates([], []))
        self.assertFalse(has_unacknowledged_duplicates([meta(OUTPUT_PDF_NAME)], [meta(ERROR_TXT_NAME)]))

    def test_unmarked_duplicate_detected(self):
        out1 = meta(OUTPUT_PDF_NAME)
        out2 = dict(out1, id='id-dup')
        self.assertTrue(has_unacknowledged_duplicates([out1, out2], []))
        # 片方だけ通知済みでも未処理扱い
        acked = dict(out1, appProperties={CONFLICT_MARKER_PROP: '1', DUP_ACK_PROP: '1'})
        self.assertTrue(has_unacknowledged_duplicates([acked, out2], []))

    def test_all_acked_duplicates_acknowledged(self):
        acked = {CONFLICT_MARKER_PROP: '1', DUP_ACK_PROP: '1'}
        out1 = dict(meta(OUTPUT_PDF_NAME), appProperties=acked)
        out2 = dict(out1, id='id-dup')
        self.assertFalse(has_unacknowledged_duplicates([out1, out2], []))

    def test_failure_marker_alone_is_not_ack(self):
        # 二重実行で同時新規作成されたエラーtxtは両方とも失敗マーカー付きで生まれる。
        # 失敗マーカーだけでは「重複競合を通知済み」とは言えない (Codex4巡目 指摘2)
        failure_only = {CONFLICT_MARKER_PROP: '1'}
        err1 = dict(meta(ERROR_TXT_NAME, mime='text/plain'), appProperties=failure_only)
        err2 = dict(err1, id='id-dup')
        self.assertTrue(has_unacknowledged_duplicates([], [err1, err2]))

    def test_error_txt_duplicates_detected(self):
        err1 = meta(ERROR_TXT_NAME, mime='text/plain')
        err2 = dict(err1, id='id-dup')
        self.assertTrue(has_unacknowledged_duplicates([], [err1, err2]))


class HasStaleDupAckTest(unittest.TestCase):
    def test_single_without_ack_ok(self):
        self.assertFalse(has_stale_dup_ack([meta(OUTPUT_PDF_NAME)], []))
        marked = dict(meta(OUTPUT_PDF_NAME), appProperties={CONFLICT_MARKER_PROP: '1'})
        self.assertFalse(has_stale_dup_ack([marked], []))

    def test_single_with_ack_detected(self):
        # 重複整理後に残った通知済みファイル → 復旧のため再処理
        acked = dict(meta(OUTPUT_PDF_NAME),
                     appProperties={CONFLICT_MARKER_PROP: '1', DUP_ACK_PROP: '1'})
        self.assertTrue(has_stale_dup_ack([acked], []))
        err = dict(meta(ERROR_TXT_NAME, mime='text/plain'),
                   appProperties={CONFLICT_MARKER_PROP: '1', DUP_ACK_PROP: '1'})
        self.assertTrue(has_stale_dup_ack([], [err]))

    def test_still_duplicated_not_stale(self):
        # まだ重複している間は対象外 (重複側のロジックが扱う)
        acked = {CONFLICT_MARKER_PROP: '1', DUP_ACK_PROP: '1'}
        out1 = dict(meta(OUTPUT_PDF_NAME), appProperties=acked)
        out2 = dict(out1, id='id-dup')
        self.assertFalse(has_stale_dup_ack([out1, out2], []))


class HasIncompleteFailureWriteTest(unittest.TestCase):
    def test_consistent_states_ok(self):
        # 全件無し (成功後) / 全件付き (失敗通知後) は正常な定常状態
        self.assertFalse(has_incomplete_failure_write([], []))
        self.assertFalse(has_incomplete_failure_write([meta(OUTPUT_PDF_NAME)], []))
        marked = dict(meta(ERROR_TXT_NAME, mime='text/plain'),
                      appProperties={CONFLICT_MARKER_PROP: '1'})
        self.assertFalse(has_incomplete_failure_write([], [marked]))
        marked_pdf = dict(meta(OUTPUT_PDF_NAME), appProperties={CONFLICT_MARKER_PROP: '1'})
        self.assertFalse(has_incomplete_failure_write([marked_pdf], [marked]))

    def test_mixed_markers_detected(self):
        # エラーtxtは失敗マーカー付きなのにPDFに無い = PDF無効化が途中で失敗している
        marked_txt = dict(meta(ERROR_TXT_NAME, mime='text/plain'),
                          appProperties={CONFLICT_MARKER_PROP: '1'})
        plain_pdf = meta(OUTPUT_PDF_NAME)
        self.assertTrue(has_incomplete_failure_write([plain_pdf], [marked_txt]))
        # 逆 (PDFだけ失敗マーカー) も途中失敗
        marked_pdf = dict(meta(OUTPUT_PDF_NAME), appProperties={CONFLICT_MARKER_PROP: '1'})
        plain_txt = meta(ERROR_TXT_NAME, mime='text/plain')
        self.assertTrue(has_incomplete_failure_write([marked_pdf], [plain_txt]))


class NaturalKeyTest(unittest.TestCase):
    def test_numeric_order(self):
        names = ['納品書_10.pdf', '納品書_2.pdf', '納品書_1.pdf']
        self.assertEqual(
            sorted(names, key=natural_key),
            ['納品書_1.pdf', '納品書_2.pdf', '納品書_10.pdf'])


class BuildInvalidPdfTest(unittest.TestCase):
    def test_valid_pdf_with_marker(self):
        from datetime import datetime as dt
        data = build_invalid_pdf(dt(2026, 7, 23, 10, 0, tzinfo=drive_worker.JST))
        texts = page_texts(data)
        self.assertEqual(len(texts), 1)
        self.assertIn('INVALID - DO NOT USE', texts[0])


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


class DriveClientUploadBodyTest(unittest.TestCase):
    def test_upload_new_strips_null_app_properties(self):
        # 値None (キー削除指示) は update/copy 専用で、create に渡すと拒否され得る。
        # upload_new が body から除外することを実クライアント側で検証する (Codex6巡目)
        captured = {}

        class FakeRequest:
            def execute(self):
                return {'id': 'new-id'}

        class FakeFiles:
            def create(self, **kwargs):
                captured.update(kwargs)
                return FakeRequest()

        class FakeService:
            def files(self):
                return FakeFiles()

        client = drive_worker.DriveClient.__new__(drive_worker.DriveClient)
        client.service = FakeService()
        client._media = lambda content, mimetype: 'media-stub'

        client.upload_new('folder-id', ERROR_TXT_NAME, b'x', 'text/plain',
                          app_properties={CONFLICT_MARKER_PROP: '1', DUP_ACK_PROP: None})
        self.assertEqual(captured['body'].get('appProperties'), {CONFLICT_MARKER_PROP: '1'})

        # 全てNoneならappPropertiesキー自体を送らない
        captured.clear()
        client.upload_new('folder-id', ERROR_TXT_NAME, b'x', 'text/plain',
                          app_properties={CONFLICT_MARKER_PROP: None})
        self.assertNotIn('appProperties', captured['body'])


class DriveClientListChildrenTest(unittest.TestCase):
    """フォルダごとの所属ドライブ解決 (素材フォルダが別共有ドライブにある構成への対応)"""

    def _make_client(self, drive_ids, list_results):
        calls = {'get': [], 'list': []}

        class FakeRequest:
            def __init__(self, payload):
                self._payload = payload

            def execute(self):
                return self._payload

        class FakeFiles:
            def get(self, **kwargs):
                calls['get'].append(kwargs)
                fid = kwargs['fileId']
                payload = {'id': fid}
                if drive_ids.get(fid):
                    payload['driveId'] = drive_ids[fid]
                return FakeRequest(payload)

            def list(self, **kwargs):
                calls['list'].append(kwargs)
                return FakeRequest(list_results.pop(0))

        class FakeService:
            def files(self):
                return FakeFiles()

        client = drive_worker.DriveClient.__new__(drive_worker.DriveClient)
        client.service = FakeService()
        client._drive_id_cache = {}
        return client, calls

    def test_shared_drive_folder_uses_its_own_drive_id(self):
        client, calls = self._make_client(
            {'material-folder': 'other-drive'},
            [{'files': [{'id': 'x', 'name': 'AES_1.pdf'}]}])
        files = client.list_children('material-folder')
        self.assertEqual([f['id'] for f in files], ['x'])
        kwargs = calls['list'][0]
        self.assertEqual(kwargs['corpora'], 'drive')
        self.assertEqual(kwargs['driveId'], 'other-drive')

    def test_my_drive_folder_omits_corpora(self):
        client, calls = self._make_client({'folder-b': None}, [{'files': []}])
        client.list_children('folder-b')
        kwargs = calls['list'][0]
        self.assertNotIn('corpora', kwargs)
        self.assertNotIn('driveId', kwargs)

    def test_drive_id_resolution_cached(self):
        client, calls = self._make_client(
            {'folder-a': 'drive-1'}, [{'files': []}, {'files': []}])
        client.list_children('folder-a')
        client.list_children('folder-a')
        self.assertEqual(len(calls['get']), 1)  # files.getは1回だけ (キャッシュ)
        self.assertEqual(len(calls['list']), 2)


# ───────────────────────── E2E (フェイクDrive) ─────────────────────────

class FakeDriveClient:
    """children/contents を辞書で持つフェイク。書き込みを記録する"""

    def __init__(self, children, contents):
        self.children = children    # folder_id -> [meta]
        self.contents = contents    # file_id -> bytes
        self.uploads = []           # (folder_id, name, content, mimetype)
        self.overwrites = []        # (file_id, content, mimetype)
        self.app_properties = {}    # file_id/new-name -> 最後に指定された app_properties

    def list_children(self, folder_id):
        return list(self.children.get(folder_id, []))

    def download(self, file_id):
        return self.contents[file_id]

    def upload_new(self, folder_id, name, content, mimetype, app_properties=None):
        self.uploads.append((folder_id, name, content, mimetype))
        if app_properties is not None:
            self.app_properties[f'new-{name}'] = app_properties
        return f'new-{name}'

    def overwrite(self, file_id, content, mimetype, app_properties=None):
        self.overwrites.append((file_id, content, mimetype))
        if app_properties is not None:
            self.app_properties[file_id] = app_properties
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
    ship_parent_id = 'ROOT'
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

    def test_stale_output_invalidated_on_failure(self):
        # 前回の成功PDFが残った状態で新しい入力の処理が失敗 → 旧PDFを「使用禁止」PDFで上書き
        label_pdf = make_pdf(['LABEL-DA100'])
        invoice_pdf = make_pdf(['249-9999999-9999999'])  # CSVに無い注文 → 失敗
        folder_files = [
            meta('引当パターン_AES《単品》.txt', mime='text/plain'),
            meta('納品書_20.pdf', '2026-07-02T00:00:00.000Z'),
            meta(OUTPUT_PDF_NAME, '2026-07-01T12:00:00.000Z'),  # 前日の成功PDF
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

        # エラーtxtは新規作成、旧出力PDFは使用禁止PDFで上書きされる
        self.assertEqual(len(client.uploads), 1)
        self.assertEqual(client.uploads[0][1], ERROR_TXT_NAME)
        self.assertEqual(len(client.overwrites), 1)
        file_id, content, mimetype = client.overwrites[0]
        self.assertEqual(file_id, f'id-{OUTPUT_PDF_NAME}')
        self.assertEqual(mimetype, 'application/pdf')
        self.assertIn('INVALID - DO NOT USE', page_texts(content)[0])

    def test_multiple_invoices_natural_order(self):
        # 納品書_2 → 納品書_10 の自然順で連結される (辞書順だと10が先になる)
        label_pdf = make_pdf(['LABEL-DA100', 'LABEL-DA200'])
        invoice2 = make_pdf(['249-1111111-1111111'])   # 納品書_2 → DA100
        invoice10 = make_pdf(['249-2222222-2222222'])  # 納品書_10 → DA200
        folder_files = [
            meta('引当パターン_AES《単品》.txt', mime='text/plain'),
            meta('納品書_10.pdf'),
            meta('納品書_2.pdf'),
        ]
        contents = {
            'id-引当パターン_AES《単品》.txt': PATTERN_AES.encode('utf-8'),
            'id-納品書_2.pdf': invoice2,
            'id-納品書_10.pdf': invoice10,
            'id-AES_labels.pdf': label_pdf,
        }
        children, all_contents = build_world(
            csv_text=CSV_TEXT, folder_files=folder_files, contents=contents)

        client = self._run(children, all_contents, [
            {'page': 0, 'data': 'DA100', 'format': 'CODE128', 'box': '青枠'},
            {'page': 1, 'data': 'DA200', 'format': 'CODE128', 'box': '青枠'},
        ])

        self.assertEqual(len(client.uploads), 1)
        self.assertEqual(client.uploads[0][1], OUTPUT_PDF_NAME)
        self.assertEqual(page_texts(client.uploads[0][2]), ['LABEL-DA100', 'LABEL-DA200'])

    def test_multiple_pattern_txts_conflict(self):
        # AESと非AESのtxtが共存 → どちらのフォルダか確定できないためエラー停止
        invoice_pdf = make_pdf(['249-1111111-1111111'])
        folder_files = [
            meta('引当パターン_AES《単品》.txt', mime='text/plain'),
            meta('引当パターン_ネコポス《全て》.txt', mime='text/plain'),
            meta('納品書_20.pdf'),
        ]
        contents = {
            'id-引当パターン_AES《単品》.txt': PATTERN_AES.encode('utf-8'),
            'id-引当パターン_ネコポス《全て》.txt': PATTERN_NEKOPOSU.encode('utf-8'),
            'id-納品書_20.pdf': invoice_pdf,
        }
        children, all_contents = build_world(
            csv_text=CSV_TEXT, folder_files=folder_files, contents=contents)

        client = self._run(children, all_contents, [])
        self.assertEqual(len(client.uploads), 1)
        _, name, content, _ = client.uploads[0]
        self.assertEqual(name, ERROR_TXT_NAME)
        self.assertIn('複数あります', content.decode('utf-8'))

    def test_ship_parent_id_scans_subfolder(self):
        # 実運用では出荷_XXはドライブ直下でなく「出荷_no」フォルダの下にある。
        # AES_SHIP_PARENT_ID でその親フォルダを指定して走査できる
        label_pdf = make_pdf(['LABEL-DA100'])
        invoice_pdf = make_pdf(['249-1111111-1111111'])
        children, all_contents = build_world(csv_text=CSV_TEXT)
        children['ROOT'] = [meta('出荷_no', mime=FOLDER_MIME)]
        children['id-出荷_no'] = [meta('出荷_20', mime=FOLDER_MIME)]
        children['id-出荷_20'] = [
            meta('引当パターン_AES《単品》.txt', mime='text/plain'),
            meta('納品書_20.pdf'),
        ]
        all_contents.update({
            'id-引当パターン_AES《単品》.txt': PATTERN_AES.encode('utf-8'),
            'id-納品書_20.pdf': invoice_pdf,
            'id-AES_labels.pdf': label_pdf,
        })

        class SubfolderConfig(FakeConfig):
            ship_parent_id = 'id-出荷_no'

        client = FakeDriveClient(children, all_contents)
        worker = Worker(SubfolderConfig(), client,
                        extractor_factory=lambda: FakeExtractor([[
                            {'page': 0, 'data': 'DA100', 'format': 'CODE128', 'box': '青枠'},
                        ]]))
        worker.run_cycle()

        self.assertEqual(len(client.uploads), 1)
        self.assertEqual(client.uploads[0][1], OUTPUT_PDF_NAME)

    def test_non_ship_folder_ignored(self):
        # ルート直下でも 出荷_XX 以外の名前のフォルダは走査しない
        invoice_pdf = make_pdf(['249-1111111-1111111'])
        children, all_contents = build_world(csv_text=CSV_TEXT)
        children['ROOT'] = [meta('メモ置き場', mime=FOLDER_MIME)]
        children['id-メモ置き場'] = [
            meta('引当パターン_AES《単品》.txt', mime='text/plain'),
            meta('納品書_20.pdf'),
        ]
        all_contents['id-引当パターン_AES《単品》.txt'] = PATTERN_AES.encode('utf-8')
        all_contents['id-納品書_20.pdf'] = invoice_pdf

        client = self._run(children, all_contents, [])
        self.assertEqual(client.uploads, [])
        self.assertEqual(client.overwrites, [])

    def test_shared_download_failure_writes_error_txt(self):
        # CSVのダウンロード例外 → ログだけでなくエラーtxtで通知される
        class BrokenDownloadClient(FakeDriveClient):
            def download(self, file_id):
                if file_id == f'id-{CSV_NAME}':
                    raise RuntimeError('network down')
                return super().download(file_id)

        invoice_pdf = make_pdf(['249-1111111-1111111'])
        label_pdf = make_pdf(['LABEL-DA100'])
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

        client = BrokenDownloadClient(children, all_contents)
        worker = Worker(FakeConfig(), client,
                        extractor_factory=lambda: FakeExtractor([[]]))
        worker.run_cycle()

        self.assertEqual(len(client.uploads), 1)
        _, name, content, _ = client.uploads[0]
        self.assertEqual(name, ERROR_TXT_NAME)
        self.assertIn('素材の読み込みに失敗しました', content.decode('utf-8'))

    def test_duplicate_barcode_conflict_blocks_output(self):
        # 同じ配送番号が複数ページで検出 → 競合エラーで停止 (どちらのページか確定できない)
        label_pdf = make_pdf(['LABEL-A', 'LABEL-B'])
        invoice_pdf = make_pdf(['249-1111111-1111111'])
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
            {'page': 1, 'data': 'DA100', 'format': 'CODE128', 'box': '青枠'},
        ])

        self.assertEqual(len(client.uploads), 1)
        _, name, content, _ = client.uploads[0]
        self.assertEqual(name, ERROR_TXT_NAME)
        self.assertIn('DA100', content.decode('utf-8'))

    def test_shared_shipping_number_conflict_blocks_output(self):
        # 異なる注文が同一配送番号 → 同じページを複数回出さずエラー停止
        label_pdf = make_pdf(['LABEL-DA100'])
        invoice_pdf = make_pdf(['249-1111111-1111111', '249-2222222-2222222'])
        csv_text = (
            '注文番号,配送番号\n'
            '249-1111111-1111111,DA100\n'
            '249-2222222-2222222,DA100\n'   # 同じ配送番号
        )
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
            csv_text=csv_text, folder_files=folder_files, contents=contents)

        client = self._run(children, all_contents, [
            {'page': 0, 'data': 'DA100', 'format': 'CODE128', 'box': '青枠'},
        ])

        self.assertEqual(len(client.uploads), 1)
        _, name, content, _ = client.uploads[0]
        self.assertEqual(name, ERROR_TXT_NAME)
        self.assertIn('配送番号の重複', content.decode('utf-8'))

    def test_output_upload_failure_writes_error_txt(self):
        # 出力PDFのアップロード失敗もエラーtxtで通知される
        class BrokenUploadClient(FakeDriveClient):
            def upload_new(self, folder_id, name, content, mimetype, app_properties=None):
                if name == OUTPUT_PDF_NAME:
                    raise RuntimeError('upload failed')
                return super().upload_new(folder_id, name, content, mimetype, app_properties)

        label_pdf = make_pdf(['LABEL-DA100'])
        invoice_pdf = make_pdf(['249-1111111-1111111'])
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

        client = BrokenUploadClient(children, all_contents)
        worker = Worker(FakeConfig(), client,
                        extractor_factory=lambda: FakeExtractor([[
                            {'page': 0, 'data': 'DA100', 'format': 'CODE128', 'box': '青枠'},
                        ]]))
        worker.run_cycle()

        error_uploads = [u for u in client.uploads if u[1] == ERROR_TXT_NAME]
        self.assertEqual(len(error_uploads), 1)
        self.assertIn('アップロードに失敗しました', error_uploads[0][2].decode('utf-8'))

    def test_same_order_across_invoices_conflict(self):
        # 同じ注文番号が別々の納品書ファイルに出た → 古い納品書の残留を疑い競合停止
        # (黙って重複排除すると送り状1枚で「正常」扱いになってしまう。Codex2巡目 指摘2)
        label_pdf = make_pdf(['LABEL-DA100'])
        invoice2 = make_pdf(['249-1111111-1111111'])
        invoice10 = make_pdf(['249-1111111-1111111'])  # 納品書_10にも同じ注文
        folder_files = [
            meta('引当パターン_AES《単品》.txt', mime='text/plain'),
            meta('納品書_2.pdf'),
            meta('納品書_10.pdf'),
        ]
        contents = {
            'id-引当パターン_AES《単品》.txt': PATTERN_AES.encode('utf-8'),
            'id-納品書_2.pdf': invoice2,
            'id-納品書_10.pdf': invoice10,
            'id-AES_labels.pdf': label_pdf,
        }
        children, all_contents = build_world(
            csv_text=CSV_TEXT, folder_files=folder_files, contents=contents)

        client = self._run(children, all_contents, [
            {'page': 0, 'data': 'DA100', 'format': 'CODE128', 'box': '青枠'},
        ])

        self.assertEqual(len(client.uploads), 1)
        _, name, content, _ = client.uploads[0]
        self.assertEqual(name, ERROR_TXT_NAME)
        text = content.decode('utf-8')
        self.assertIn('複数の納品書', text)
        self.assertIn('249-1111111-1111111', text)
        self.assertIn('納品書_2.pdf', text)
        self.assertIn('納品書_10.pdf', text)

    def test_duplicate_output_files_conflict(self):
        # 同名の出力PDFが2つある (二重実行の痕跡) → 上書き先を確定できないため停止し、
        # 両方を「使用禁止」PDFで無効化する (Codex2巡目 指摘3)
        invoice_pdf = make_pdf(['249-1111111-1111111'])
        out1 = meta(OUTPUT_PDF_NAME, '2026-07-01T12:00:00.000Z')
        out2 = dict(out1, id='id-out-dup')
        folder_files = [
            meta('引当パターン_AES《単品》.txt', mime='text/plain'),
            meta('納品書_20.pdf', '2026-07-02T00:00:00.000Z'),
            out1,
            out2,
        ]
        contents = {
            'id-引当パターン_AES《単品》.txt': PATTERN_AES.encode('utf-8'),
            'id-納品書_20.pdf': invoice_pdf,
        }
        children, all_contents = build_world(
            csv_text=CSV_TEXT, folder_files=folder_files, contents=contents)

        client = self._run(children, all_contents, [])

        # エラーtxtは新規作成
        self.assertEqual(len(client.uploads), 1)
        _, name, content, _ = client.uploads[0]
        self.assertEqual(name, ERROR_TXT_NAME)
        self.assertIn('同名の出力ファイルが複数あります', content.decode('utf-8'))
        # 両方の旧出力PDFが使用禁止PDFで無効化される
        invalidated = {file_id for file_id, c, m in client.overwrites if m == 'application/pdf'}
        self.assertEqual(invalidated, {out1['id'], out2['id']})
        for _, c, m in client.overwrites:
            if m == 'application/pdf':
                self.assertIn('INVALID - DO NOT USE', page_texts(c)[0])

    def test_fresh_duplicate_outputs_flagged_despite_mtime(self):
        # 二重実行で作られた同名PDFはどちらも入力より新しい → mtime基準では skip_done に
        # なってしまうが、マーカーの無い重複は競合処理へ進み検出される (Codex3巡目 指摘2)
        invoice_pdf = make_pdf(['249-1111111-1111111'])
        out1 = meta(OUTPUT_PDF_NAME, '2026-07-03T00:00:00.000Z')  # 入力より新しい
        out2 = dict(out1, id='id-out-dup')
        folder_files = [
            meta('引当パターン_AES《単品》.txt', mime='text/plain'),
            meta('納品書_20.pdf', '2026-07-02T00:00:00.000Z'),
            out1,
            out2,
        ]
        contents = {
            'id-引当パターン_AES《単品》.txt': PATTERN_AES.encode('utf-8'),
            'id-納品書_20.pdf': invoice_pdf,
        }
        children, all_contents = build_world(
            csv_text=CSV_TEXT, folder_files=folder_files, contents=contents)

        client = self._run(children, all_contents, [])

        self.assertEqual(len(client.uploads), 1)
        _, name, content, _ = client.uploads[0]
        self.assertEqual(name, ERROR_TXT_NAME)
        self.assertIn('同名の出力ファイルが複数あります', content.decode('utf-8'))
        # 両方無効化され、失敗マーカー+通知済みマーカーが付く
        invalidated = {file_id for file_id, c, m in client.overwrites if m == 'application/pdf'}
        self.assertEqual(invalidated, {out1['id'], out2['id']})
        for file_id in invalidated:
            self.assertEqual(client.app_properties[file_id],
                             {CONFLICT_MARKER_PROP: '1', DUP_ACK_PROP: '1'})
        # エラーtxtは単独 (新規作成) なので失敗マーカーのみ (通知済みマーカーは消す側)
        self.assertEqual(client.app_properties[f'new-{ERROR_TXT_NAME}'],
                         {CONFLICT_MARKER_PROP: '1', DUP_ACK_PROP: None})

    def test_marked_duplicates_converge_no_rewrite(self):
        # 競合通知済み (重複PDF全件に通知済みマーカー・エラーtxtも新しい) なら
        # 再書き込みしない (収束)。単独のエラーtxtは失敗マーカーのみ (実運用と同じ)
        acked = {CONFLICT_MARKER_PROP: '1', DUP_ACK_PROP: '1'}
        out1 = dict(meta(OUTPUT_PDF_NAME, '2026-07-03T00:00:00.000Z'), appProperties=acked)
        out2 = dict(out1, id='id-out-dup')
        err = dict(meta(ERROR_TXT_NAME, '2026-07-03T00:00:00.000Z', mime='text/plain'),
                   appProperties={CONFLICT_MARKER_PROP: '1'})
        folder_files = [
            meta('引当パターン_AES《単品》.txt', mime='text/plain'),
            meta('納品書_20.pdf', '2026-07-02T00:00:00.000Z'),
            out1,
            out2,
            err,
        ]
        children, all_contents = build_world(
            csv_text=CSV_TEXT, folder_files=folder_files,
            contents={'id-引当パターン_AES《単品》.txt': PATTERN_AES.encode('utf-8')})
        # 入力側 (素材・CSV) も出力より古くしておく
        for f in children['MATERIAL'] + children['CSVDIR']:
            f['modifiedTime'] = '2026-07-02T00:00:00.000Z'

        client = self._run(children, all_contents, [])
        self.assertEqual(client.uploads, [])
        self.assertEqual(client.overwrites, [])

    def test_mixed_markers_force_reprocess(self):
        # 「PDFは正常内容のまま・エラーtxtは失敗マーカー付き」= 前回のPDF無効化が
        # 途中で失敗した状態。両方入力より新しくてもskipせず再処理し、
        # PDFを無効化し直す (Codex4巡目 指摘1)
        label_pdf = make_pdf(['LABEL-DA100'])
        invoice_pdf = make_pdf(['249-9999999-9999999'])  # CSVに無い注文 → 失敗が続く
        out = meta(OUTPUT_PDF_NAME, '2026-07-03T00:00:00.000Z')  # 正常内容・マーカー無し
        err = dict(meta(ERROR_TXT_NAME, '2026-07-03T00:00:00.000Z', mime='text/plain'),
                   appProperties={CONFLICT_MARKER_PROP: '1'})
        folder_files = [
            meta('引当パターン_AES《単品》.txt', mime='text/plain'),
            meta('納品書_20.pdf'),
            out,
            err,
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

        # エラーtxt上書き + 正常PDFの無効化が実行される
        self.assertEqual(client.uploads, [])
        by_id = {file_id: (content, m) for file_id, content, m in client.overwrites}
        self.assertIn(err['id'], by_id)
        self.assertIn('失敗しました', by_id[err['id']][0].decode('utf-8'))
        self.assertIn(out['id'], by_id)
        self.assertIn('INVALID - DO NOT USE', page_texts(by_id[out['id']][0])[0])
        self.assertEqual(client.app_properties[out['id']],
                         {CONFLICT_MARKER_PROP: '1', DUP_ACK_PROP: None})

    def test_simultaneous_error_txt_duplicates_flagged(self):
        # 二重実行で同時新規作成されたエラーtxt2つは、どちらも失敗マーカー付き・
        # 入力より新しい。通知済みマーカーが無いので競合として検出され、
        # 両方が競合エラー内容+通知済みマーカーで上書きされる (Codex4巡目 指摘2)
        invoice_pdf = make_pdf(['249-1111111-1111111'])
        failure_only = {CONFLICT_MARKER_PROP: '1'}
        err1 = dict(meta(ERROR_TXT_NAME, '2026-07-03T00:00:00.000Z', mime='text/plain'),
                    appProperties=dict(failure_only))
        err2 = dict(err1, id='id-err-dup', appProperties=dict(failure_only))
        folder_files = [
            meta('引当パターン_AES《単品》.txt', mime='text/plain'),
            meta('納品書_20.pdf'),
            err1,
            err2,
        ]
        contents = {
            'id-引当パターン_AES《単品》.txt': PATTERN_AES.encode('utf-8'),
            'id-納品書_20.pdf': invoice_pdf,
        }
        children, all_contents = build_world(
            csv_text=CSV_TEXT, folder_files=folder_files, contents=contents)

        client = self._run(children, all_contents, [])

        self.assertEqual(client.uploads, [])
        self.assertEqual(len(client.overwrites), 2)
        for file_id, content, mimetype in client.overwrites:
            self.assertIn(file_id, {err1['id'], err2['id']})
            self.assertEqual(mimetype, 'text/plain')
            self.assertIn('同名の出力ファイルが複数あります', content.decode('utf-8'))
            self.assertEqual(client.app_properties[file_id],
                             {CONFLICT_MARKER_PROP: '1', DUP_ACK_PROP: '1'})

    def test_dup_cleanup_recovery_reprocesses(self):
        # 競合通知後にユーザーが重複を1件に整理 → 残った通知済みファイルは入力より
        # 新しくても一度再処理され、正常なPDF+解消済みtxtに戻る (Codex5巡目 指摘)
        label_pdf = make_pdf(['LABEL-DA100'])
        invoice_pdf = make_pdf(['249-1111111-1111111'])
        out = dict(meta(OUTPUT_PDF_NAME, '2026-07-03T00:00:00.000Z'),
                   appProperties={CONFLICT_MARKER_PROP: '1', DUP_ACK_PROP: '1'})  # 残った無効PDF
        err = dict(meta(ERROR_TXT_NAME, '2026-07-03T00:00:00.000Z', mime='text/plain'),
                   appProperties={CONFLICT_MARKER_PROP: '1'})
        folder_files = [
            meta('引当パターン_AES《単品》.txt', mime='text/plain'),
            meta('納品書_20.pdf'),
            out,
            err,
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

        self.assertEqual(client.uploads, [])
        by_id = {file_id: (content, m) for file_id, content, m in client.overwrites}
        # 無効PDFは正常な並び替え済みPDFで上書きされ、マーカーは両方消える
        self.assertEqual(page_texts(by_id[out['id']][0]), ['LABEL-DA100'])
        self.assertEqual(client.app_properties[out['id']],
                         {CONFLICT_MARKER_PROP: None, DUP_ACK_PROP: None})
        # エラーtxtは解消済みに上書きされ、マーカーは両方消える
        self.assertIn('解消済み', by_id[err['id']][0].decode('utf-8'))
        self.assertEqual(client.app_properties[err['id']],
                         {CONFLICT_MARKER_PROP: None, DUP_ACK_PROP: None})

    def test_error_txt_failure_skips_pdf_invalidation(self):
        # エラーtxtの新規作成に失敗したら旧PDFの無効化も見送る。先にPDFを無効化すると
        # 「新しいPDFだけがある」状態になり恒久skipに陥るため (Codex3巡目 指摘1)
        class BrokenErrorTxtClient(FakeDriveClient):
            def upload_new(self, folder_id, name, content, mimetype, app_properties=None):
                if name == ERROR_TXT_NAME:
                    raise RuntimeError('txt upload failed')
                return super().upload_new(folder_id, name, content, mimetype, app_properties)

        label_pdf = make_pdf(['LABEL-DA100'])
        invoice_pdf = make_pdf(['249-9999999-9999999'])  # CSVに無い注文 → 失敗
        folder_files = [
            meta('引当パターン_AES《単品》.txt', mime='text/plain'),
            meta('納品書_20.pdf', '2026-07-02T00:00:00.000Z'),
            meta(OUTPUT_PDF_NAME, '2026-07-01T12:00:00.000Z'),  # 前日の成功PDF
        ]
        contents = {
            'id-引当パターン_AES《単品》.txt': PATTERN_AES.encode('utf-8'),
            'id-納品書_20.pdf': invoice_pdf,
            'id-AES_labels.pdf': label_pdf,
        }
        children, all_contents = build_world(
            csv_text=CSV_TEXT, folder_files=folder_files, contents=contents)

        client = BrokenErrorTxtClient(children, all_contents)
        worker = Worker(FakeConfig(), client,
                        extractor_factory=lambda: FakeExtractor([[
                            {'page': 0, 'data': 'DA100', 'format': 'CODE128', 'box': '青枠'},
                        ]]))
        worker.run_cycle()

        # 旧PDFは無効化されない (次サイクルでエラーtxtごと再試行される)
        self.assertEqual(client.overwrites, [])

    def test_build_label_pdf_failure_writes_error_txt(self):
        # PDF組み立て自体の失敗 (壊れたPDF等) もログだけでなくエラーtxtで通知される
        # (Codex2巡目 指摘4)
        label_pdf = make_pdf(['LABEL-DA100'])
        invoice_pdf = make_pdf(['249-1111111-1111111'])
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

        with mock.patch.object(drive_worker.sorter_core, 'build_label_pdf',
                               side_effect=RuntimeError('broken pdf')):
            client = self._run(children, all_contents, [
                {'page': 0, 'data': 'DA100', 'format': 'CODE128', 'box': '青枠'},
            ])

        self.assertEqual(len(client.uploads), 1)
        _, name, content, _ = client.uploads[0]
        self.assertEqual(name, ERROR_TXT_NAME)
        text = content.decode('utf-8')
        self.assertIn('生成/アップロードに失敗しました', text)
        self.assertIn('broken pdf', text)

    def test_success_output_survives_material_swap(self):
        # 朝成功したフォルダは、午後のバッチで素材 (AES*.pdf/CSV) が入れ替わっても
        # 再処理されず、正常PDFが使用禁止で潰されない (2026-07-23 実障害の再発防止)
        folder_files = [
            meta('引当パターン_AES《単品》.txt', mime='text/plain'),
            meta('納品書_20.pdf', OLDER),
            meta(OUTPUT_PDF_NAME, OLD),  # 朝の成功PDF (マーカー無し)
        ]
        children, all_contents = build_world(
            csv_text=CSV_TEXT, folder_files=folder_files,
            contents={'id-引当パターン_AES《単品》.txt': PATTERN_AES.encode('utf-8')})
        # 素材とCSVは出力より新しい (午後のバッチで入れ替わった状態)
        for f in children['MATERIAL'] + children['CSVDIR']:
            f['modifiedTime'] = '2026-07-02T00:00:00.000Z'
        for f in children['id-出荷_20']:
            if f['name'] != OUTPUT_PDF_NAME:
                f['modifiedTime'] = OLDER

        client = self._run(children, all_contents, [])
        self.assertEqual(client.uploads, [])
        self.assertEqual(client.overwrites, [])

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
