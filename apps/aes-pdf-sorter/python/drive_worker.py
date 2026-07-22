"""
AES送り状並び替え Drive自動化ワーカー

共有ドライブ「ネクストエンジン【出荷関係】」直下の出荷_XXフォルダを定期的に走査し、
AES引当フォルダ (引当パターン_AES….txt があるフォルダ) に納品書PDFが入っていたら、
logi_dispatch.csv と素材フォルダの AES*.pdf (送り状) を使って
納品書順に並び替えた「AES送り状_並び替え済.pdf」を同フォルダに出力する。
失敗時は「AES送り状_エラー.txt」を出力し、入力ファイルが変化したら自動リトライする。

設計書: G:\\共有ドライブ\\AI_reference\\システム設計\\AES送り状並び替え_Drive自動化_設計_20260722.md

安全設計:
- Driveへの書き込みは AES送り状_並び替え済.pdf / AES送り状_エラー.txt の
  新規作成・自ファイル上書きのみ。削除・移動・リネームは一切行わない。
- 失敗時に前回の成功PDFが残っていると誤って印刷・使用される恐れがあるため、
  削除の代わりに「使用禁止」表示のPDFで上書きして失効させる (Codexレビュー指摘1)。
- 突合の競合 (同一配送番号が複数ページ / 複数注文が同一ページ) は黙って進めず
  エラー停止する (Codexレビュー指摘3)。
- ワーカーはシングルスレッドの逐次処理 (フォルダを1つずつ)。Renderインスタンスは1台前提。
  万一二重実行が起きても、出力は同名ファイルへの上書きに収束するため事故にならない。
"""
import io
import json
import os
import re
import threading
import time
import traceback
from datetime import datetime, timedelta, timezone

import fitz  # PyMuPDF

import sorter_core

JST = timezone(timedelta(hours=9))

OUTPUT_PDF_NAME = 'AES送り状_並び替え済.pdf'
ERROR_TXT_NAME = 'AES送り状_エラー.txt'
CSV_NAME = 'logi_dispatch.csv'

SHIP_FOLDER_RE = re.compile(r'^出荷_\d+$')
PATTERN_TXT_RE = re.compile(r'^引当パターン_.*\.txt$')
INVOICE_RE = re.compile(r'^納品書_.*\.pdf$', re.IGNORECASE)
LABEL_RE = re.compile(r'^AES.*\.pdf$', re.IGNORECASE)

FOLDER_MIME = 'application/vnd.google-apps.folder'

# 入力ファイルのアップロード直後に処理が走らないための整定待ち (秒)
SETTLE_SECONDS = 120


def _log(msg):
    print(f"[AES-DriveWorker] {msg}", flush=True)


# ───────────────────────── 純粋ロジック (単体テスト対象) ─────────────────────────

def parse_rfc3339(s):
    """Drive APIの modifiedTime (RFC3339) をawareなdatetimeにする"""
    return datetime.fromisoformat(s.replace('Z', '+00:00'))


def natural_key(name):
    """自然順ソートキー (納品書_2 < 納品書_10 になるように)"""
    return [int(part) if part.isdigit() else part for part in re.split(r'(\d+)', name)]


def parse_pattern_name(text):
    """引当パターン_*.txt の中身から「パターン表示名:」の値を取り出す (無ければ None)"""
    if text is None:
        return None
    for line in text.lstrip('\ufeff').splitlines():
        if line.startswith('パターン表示名:'):
            return line.split(':', 1)[1].strip()
    return None


def is_aes_pattern(txt_filename, txt_text):
    """AES引当フォルダかどうかの判定。

    第一判定は中身の「パターン表示名:」がAESで始まるか。
    中身が読めない・行が無い場合のみファイル名の前方一致にフォールバックする。
    """
    name = parse_pattern_name(txt_text)
    if name is not None:
        return name.startswith('AES')
    return txt_filename.startswith('引当パターン_AES')


def decide_action(now_utc, input_files, output_file, error_file):
    """処理要否判定 (設計書§6)。

    input_files: 判定に使う入力ファイルのメタ一覧 [{'name','modifiedTime',...}]
                 (対象フォルダの納品書・引当パターンtxt + logi_dispatch.csv + 素材AES*.pdf)
    output_file / error_file: 前回試行の出力メタ (無ければ None)
    戻り値: 'process' / 'skip_done' (試行済みで入力に変化なし) / 'skip_settling' (整定待ち)

    出力PDFとエラーtxtが両方存在する場合、試行の完了時刻には「古い方」を使う。
    完全な試行は両方を更新するため、片方だけ新しい = 前回の書き込みが途中で失敗した
    状態であり、skipせず次サイクルで再試行する (Codexレビュー2巡目 指摘1)。
    """
    newest_input = max(parse_rfc3339(f['modifiedTime']) for f in input_files)

    attempts = [f for f in (output_file, error_file) if f]
    if attempts:
        attempt_completed = min(parse_rfc3339(f['modifiedTime']) for f in attempts)
        if newest_input <= attempt_completed:
            return 'skip_done'

    if now_utc - newest_input < timedelta(seconds=SETTLE_SECONDS):
        return 'skip_settling'

    return 'process'


def build_error_txt(now_jst, reasons):
    """エラー通知テキスト (BOM付きUTF-8/CRLF) を組み立てる"""
    lines = [
        '❌ AES送り状の並び替えに失敗しました',
        f'発生時刻: {now_jst.strftime("%Y-%m-%d %H:%M")} JST',
        '原因:',
    ]
    lines += [f'  - {r}' for r in reasons]
    lines += [
        '対処: 素材フォルダのCSV・送り状PDFが今日の分か確認し、正しいファイルを置き直してください。',
        '      ファイルを置き直せば数分後に自動で再処理されます。',
        '',
    ]
    return ('\ufeff' + '\r\n'.join(lines)).encode('utf-8')


def build_resolved_txt(now_jst):
    """エラー解消時にエラーtxtへ上書きする内容 (ファイルは削除しない)"""
    line = (f'✅ 解消済み ({OUTPUT_PDF_NAME} を出力しました) '
            f'{now_jst.strftime("%Y-%m-%d %H:%M")} JST')
    return ('\ufeff' + line + '\r\n').encode('utf-8')


def build_invalid_pdf(now_jst):
    """失敗時に旧出力PDFを失効させるための「使用禁止」PDFを作る。

    前回の成功PDFがそのまま残ると気づかず印刷・使用される恐れがあるため、
    削除の代わりに中身を差し替えて無効化する (Drive削除禁止ルールとの両立)。
    """
    doc = fitz.open()
    try:
        page = doc.new_page()
        lines = [
            ('INVALID - DO NOT USE', 24),
            ('❌ このPDFは使用しないでください', 20),
            ('並び替えに失敗したため、前回の内容を無効化しました。', 14),
            (f'同じフォルダの {ERROR_TXT_NAME} を確認してください。', 14),
            (f'{now_jst.strftime("%Y-%m-%d %H:%M")} JST', 12),
        ]
        y = 120
        for text, size in lines:
            page.insert_text((60, y), text, fontname='japan', fontsize=size)
            y += size * 2
        return doc.tobytes()
    finally:
        doc.close()


def parse_poll_hours(spec):
    """'7-22' 形式の稼働時間帯 (JST) をパースする"""
    m = re.fullmatch(r'(\d{1,2})-(\d{1,2})', (spec or '').strip())
    if not m:
        raise ValueError(f'AES_POLL_HOURS_JST の形式が不正です: {spec!r} (例: 7-22)')
    start, end = int(m.group(1)), int(m.group(2))
    if not (0 <= start <= 24 and 0 <= end <= 24):
        raise ValueError(f'AES_POLL_HOURS_JST の時刻が範囲外です: {spec!r}')
    return start, end


def within_hours(now_jst, hours):
    """稼働時間帯 (JST) 内か。start > end の場合は日跨ぎ (例: 22-7) とみなす"""
    start, end = hours
    if start == end:
        return True  # 例: 0-0 = 常時稼働
    if start < end:
        return start <= now_jst.hour < end
    return now_jst.hour >= start or now_jst.hour < end


# ───────────────────────── Drive APIクライアント ─────────────────────────

class DriveClient:
    """Drive API v3 の薄いラッパー (サービスアカウント認証)。

    書き込み系は upload_new / overwrite の2つだけ。削除系のメソッドは意図的に持たない。
    対象は単一の共有ドライブなので corpora='drive' + driveId で検索する
    (allDrives は incompleteSearch の恐れがあるため使わない。Codexレビュー指摘6)。
    """

    def __init__(self, service_account_json, drive_id):
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
        creds = service_account.Credentials.from_service_account_info(
            json.loads(service_account_json),
            scopes=['https://www.googleapis.com/auth/drive'],
        )
        self.service = build('drive', 'v3', credentials=creds, cache_discovery=False)
        self.drive_id = drive_id

    def list_children(self, folder_id):
        files = []
        page_token = None
        while True:
            res = self.service.files().list(
                q=f"'{folder_id}' in parents and trashed=false",
                corpora='drive',
                driveId=self.drive_id,
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
                fields='nextPageToken, incompleteSearch, files(id,name,mimeType,modifiedTime)',
                pageSize=200,
                pageToken=page_token,
            ).execute()
            if res.get('incompleteSearch'):
                # 不完全な結果を「ファイルが無い」と誤判定すると誤動作するため中断する
                raise RuntimeError(f'Drive検索結果が不完全です (incompleteSearch, folder={folder_id})')
            files.extend(res.get('files', []))
            page_token = res.get('nextPageToken')
            if not page_token:
                return files

    def download(self, file_id):
        from googleapiclient.http import MediaIoBaseDownload
        buf = io.BytesIO()
        request = self.service.files().get_media(fileId=file_id, supportsAllDrives=True)
        downloader = MediaIoBaseDownload(buf, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        return buf.getvalue()

    def _media(self, content, mimetype):
        from googleapiclient.http import MediaInMemoryUpload
        # 結合PDFは5MBを超え得るため常にresumableでアップロードする (Codexレビュー指摘5)
        return MediaInMemoryUpload(content, mimetype=mimetype, resumable=True)

    def upload_new(self, folder_id, name, content, mimetype):
        res = self.service.files().create(
            body={'name': name, 'parents': [folder_id]},
            media_body=self._media(content, mimetype),
            supportsAllDrives=True,
            fields='id',
        ).execute()
        return res['id']

    def overwrite(self, file_id, content, mimetype):
        self.service.files().update(
            fileId=file_id,
            media_body=self._media(content, mimetype),
            supportsAllDrives=True,
        ).execute()
        return file_id


# ───────────────────────── ワーカー本体 ─────────────────────────

class Config:
    def __init__(self, env):
        self.sa_json = env.get('GOOGLE_SERVICE_ACCOUNT_JSON') or ''
        self.root_id = env.get('AES_DRIVE_ROOT_ID') or ''
        self.material_folder_id = env.get('AES_MATERIAL_FOLDER_ID') or ''
        self.csv_folder_id = env.get('AES_CSV_FOLDER_ID') or ''
        self.interval_sec = int(env.get('AES_POLL_INTERVAL_SEC') or '300')
        self.hours = parse_poll_hours(env.get('AES_POLL_HOURS_JST') or '7-22')

        missing = [name for name, v in [
            ('GOOGLE_SERVICE_ACCOUNT_JSON', self.sa_json),
            ('AES_DRIVE_ROOT_ID', self.root_id),
            ('AES_MATERIAL_FOLDER_ID', self.material_folder_id),
            ('AES_CSV_FOLDER_ID', self.csv_folder_id),
        ] if not v]
        if missing:
            raise ValueError(f'環境変数が未設定です: {", ".join(missing)}')


class SharedInputs:
    """1サイクル内で共有する素材 (CSVマップ・バーコードマップ)。

    バーコード読み取りは重いので、処理が必要なフォルダが見つかった時に1回だけ構築する。
    ダウンロード等の例外は load_errors に格納し、呼び出し側がエラーtxtとして通知できる
    ようにする (握りつぶしてログだけにしない。Codexレビュー指摘4)。
    open済みのfitzドキュメントを保持するため、サイクル終了時に必ず close() を呼ぶこと。
    """

    def __init__(self, client, csv_meta, labels_meta, extractor_factory):
        self._client = client
        self._csv_meta = csv_meta
        self._labels_meta = labels_meta
        self._extractor_factory = extractor_factory
        self._loaded = False
        self.order_shipping_map = {}
        self.barcode_map = {}
        self.duplicate_barcodes = []  # 同一配送番号が複数ページにあった場合の配送番号一覧
        self.load_errors = []         # 人向けのエラー文言 (素材が読めない類)
        self._docs = []

    def ensure_loaded(self):
        if self._loaded:
            return
        try:
            csv_bytes = self._client.download(self._csv_meta['id'])
            csv_text = sorter_core.decode_csv_bytes(csv_bytes)
            if csv_text is None:
                self.load_errors.append(f'{CSV_NAME} の文字コードが不正です')
            else:
                try:
                    if sorter_core.build_order_shipping_map(csv_text, self.order_shipping_map) == 'csv_header':
                        self.load_errors.append(f'{CSV_NAME} のヘッダーに「注文番号」または「配送番号」列が見つかりません')
                except Exception as e:
                    self.load_errors.append(f'{CSV_NAME} の処理エラー: {e}')

            label_files = []
            for meta in self._labels_meta:
                label_files.append((meta['name'], self._client.download(meta['id'])))

            extractor = self._extractor_factory()
            self.barcode_map, label_errors, self._docs = sorter_core.build_shipping_barcode_map(
                label_files, extractor, duplicates=self.duplicate_barcodes)
            for err in label_errors:
                self.load_errors.append(f'{err["file"]}: {err["error"]}')
        except Exception as e:
            # ダウンロード失敗等。不完全なマップで処理しないよう空に戻す
            self.order_shipping_map = {}
            self.barcode_map = {}
            self.load_errors.append(f'素材の読み込みに失敗しました: {e}')
        finally:
            self._loaded = True

    def close(self):
        for doc in self._docs:
            try:
                doc.close()
            except Exception:
                pass
        self._docs = []


def _default_extractor_factory():
    from barcode_extractor import BarcodeExtractor
    return BarcodeExtractor()


class Worker:
    def __init__(self, config, client, extractor_factory=_default_extractor_factory):
        self.config = config
        self.client = client
        self.extractor_factory = extractor_factory

    def _now_utc(self):
        return datetime.now(timezone.utc)

    def run_cycle(self):
        client = self.client

        material_files = client.list_children(self.config.material_folder_id)
        labels_meta = sorted(
            (f for f in material_files if f.get('mimeType') != FOLDER_MIME and LABEL_RE.match(f['name'])),
            key=lambda f: natural_key(f['name']))
        csv_meta = next(
            (f for f in client.list_children(self.config.csv_folder_id)
             if f.get('mimeType') != FOLDER_MIME and f['name'] == CSV_NAME),
            None)

        shared = None
        if csv_meta is not None:
            shared = SharedInputs(client, csv_meta, labels_meta, self.extractor_factory)

        folders = [f for f in client.list_children(self.config.root_id)
                   if f.get('mimeType') == FOLDER_MIME and SHIP_FOLDER_RE.match(f['name'])]

        try:
            for folder in sorted(folders, key=lambda f: natural_key(f['name'])):
                try:
                    self._handle_folder(folder, shared, csv_meta, labels_meta)
                except Exception as e:
                    _log(f"フォルダ '{folder['name']}' の処理で想定外エラー (スキップ): {e}\n{traceback.format_exc()}")
        finally:
            if shared is not None:
                shared.close()

    def _handle_folder(self, folder, shared, csv_meta, labels_meta):
        client = self.client
        files = client.list_children(folder['id'])
        non_folder = [f for f in files if f.get('mimeType') != FOLDER_MIME]

        pattern_txts = sorted((f for f in non_folder if PATTERN_TXT_RE.match(f['name'])),
                              key=lambda f: natural_key(f['name']))
        if not pattern_txts:
            return

        invoices = sorted((f for f in non_folder if INVOICE_RE.match(f['name'])),
                          key=lambda f: natural_key(f['name']))
        if not invoices:
            return

        output_file = next((f for f in non_folder if f['name'] == OUTPUT_PDF_NAME), None)
        error_file = next((f for f in non_folder if f['name'] == ERROR_TXT_NAME), None)

        # 処理要否判定 (素材のメタも入力に含める。CSV不在時はフォルダ側の変化だけで判定し、
        # エラー通知→CSVが置かれたら modifiedTime が新しくなるので自動リトライされる)
        input_files = invoices + pattern_txts + labels_meta + ([csv_meta] if csv_meta else [])
        action = decide_action(self._now_utc(), input_files, output_file, error_file)
        if action != 'process':
            return

        # AES引当フォルダかの判定 (処理候補になった時だけtxtをダウンロード)
        aes_txts = []
        for txt_meta in pattern_txts:
            try:
                txt_text = client.download(txt_meta['id']).decode('utf-8', errors='replace')
            except Exception as e:
                _log(f"フォルダ '{folder['name']}' の {txt_meta['name']} が読めません: {e}")
                txt_text = None
            if is_aes_pattern(txt_meta['name'], txt_text):
                aes_txts.append(txt_meta)

        if not aes_txts:
            return

        _log(f"処理開始: {folder['name']} (納品書 {len(invoices)}件)")
        reasons = []

        # 引当パターンtxtが複数あるフォルダは、どのパターンのフォルダか確定できない
        # (フォルダ再利用で古いtxtが残った可能性)。誤処理を避けて停止する (Codexレビュー指摘2)
        if len(pattern_txts) > 1:
            names = ', '.join(t['name'] for t in pattern_txts)
            reasons.append(f'引当パターンのテキストが複数あります ({names})。'
                           'どのパターンのフォルダか判断できないため処理を停止しました。'
                           '不要なtxtを整理してください')

        if csv_meta is None:
            reasons.append(f'{CSV_NAME} が見つかりません (bfaithポータルdataフォルダを確認してください)')
        if not labels_meta:
            reasons.append('素材フォルダに AES*.pdf (送り状) が見つかりません')

        matched_pages = []
        if not reasons:
            shared.ensure_loaded()
            if not shared.order_shipping_map:
                reasons.append(f'{CSV_NAME} から有効なデータを読み取れませんでした')
                reasons.extend(shared.load_errors)
            elif not shared.barcode_map:
                reasons.append('送り状PDFからバーコードを読み取れませんでした')
                reasons.extend(shared.load_errors)
            elif shared.duplicate_barcodes:
                dups = ', '.join(sorted(set(shared.duplicate_barcodes)))
                reasons.append(f'同じ配送番号のバーコードが複数の送り状ページで検出されました: {dups} '
                               '(素材フォルダに古い送り状PDFが混在していないか確認してください)')
            else:
                all_orders = []
                for invoice_meta in invoices:
                    try:
                        invoice_bytes = client.download(invoice_meta['id'])
                        invoice_doc = fitz.open(stream=invoice_bytes, filetype="pdf")
                        try:
                            orders = sorter_core.extract_order_numbers(invoice_doc)
                        finally:
                            invoice_doc.close()
                        if not orders:
                            reasons.append(f'{invoice_meta["name"]}: 注文番号パターン（3桁-7桁-7桁）が見つかりませんでした')
                            continue
                        all_orders.extend(o for o in orders if o not in all_orders)
                    except Exception as e:
                        reasons.append(f'{invoice_meta["name"]}: 納品書処理エラー: {e}')

                if not reasons:
                    matched_pages, unmatched = sorter_core.match_orders(
                        all_orders, shared.order_shipping_map, shared.barcode_map)
                    for order_number in unmatched:
                        reasons.append(f'注文番号 {order_number} に対応する送り状が見つかりません')
                    if not matched_pages and not unmatched:
                        reasons.append('納品書から注文番号を1件も抽出できませんでした')
                    # 複数の注文が同じ送り状ページに解決された場合は競合 (配送番号の重複)。
                    # 黙って同じページを複数回出すと1枚足りない出荷につながるため停止する
                    page_keys = [(id(doc), page_num) for doc, page_num in matched_pages]
                    if len(set(page_keys)) != len(page_keys):
                        reasons.append('複数の注文が同じ送り状ページに紐づいています (配送番号の重複)。'
                                       f'{CSV_NAME} と送り状PDFの組み合わせを確認してください')

        if reasons:
            self._notify_failure(folder, reasons, output_file, error_file)
            return

        label_bytes = sorter_core.build_label_pdf(matched_pages)
        try:
            if output_file:
                client.overwrite(output_file['id'], label_bytes, 'application/pdf')
            else:
                client.upload_new(folder['id'], OUTPUT_PDF_NAME, label_bytes, 'application/pdf')
        except Exception as e:
            # 出力の書き込み失敗も人に見える形で通知する (ログだけにしない)
            self._notify_failure(
                folder,
                [f'{OUTPUT_PDF_NAME} のアップロードに失敗しました: {e}'],
                output_file, error_file)
            return

        # 過去のエラーtxtは削除せず「解消済み」に上書きする (Drive削除禁止ルール)
        if error_file:
            try:
                client.overwrite(error_file['id'], build_resolved_txt(datetime.now(JST)), 'text/plain')
            except Exception as e:
                _log(f"フォルダ '{folder['name']}' のエラーtxt更新に失敗 (出力は完了済み): {e}")

        _log(f"出力完了: {folder['name']} / {OUTPUT_PDF_NAME} ({len(matched_pages)}ページ)")

    def _notify_failure(self, folder, reasons, output_file, error_file):
        """エラーtxtを作成/上書きし、残っている旧出力PDFを「使用禁止」PDFで失効させる"""
        client = self.client
        now_jst = datetime.now(JST)
        try:
            content = build_error_txt(now_jst, reasons)
            if error_file:
                client.overwrite(error_file['id'], content, 'text/plain')
            else:
                client.upload_new(folder['id'], ERROR_TXT_NAME, content, 'text/plain')
        except Exception as e:
            _log(f"フォルダ '{folder['name']}' のエラー通知の書き込みに失敗: {e}")

        if output_file:
            # 前回の成功PDFが残っていると誤使用の恐れがあるため無効化する (削除はしない)
            try:
                client.overwrite(output_file['id'], build_invalid_pdf(now_jst), 'application/pdf')
            except Exception as e:
                _log(f"フォルダ '{folder['name']}' の旧出力PDFの無効化に失敗: {e}")

        _log(f"エラー通知: {folder['name']} ({len(reasons)}件)")


# ───────────────────────── 起動 ─────────────────────────

def _loop(config):
    client = None
    while True:
        try:
            if within_hours(datetime.now(JST), config.hours):
                if client is None:
                    client = DriveClient(config.sa_json, config.root_id)
                Worker(config, client).run_cycle()
        except Exception as e:
            _log(f"サイクルエラー: {e}\n{traceback.format_exc()}")
            client = None  # 認証・接続系の失敗に備えて次サイクルで作り直す
        time.sleep(config.interval_sec)


def start_drive_worker():
    """バックグラウンドスレッドでワーカーを開始する (main.py から env ゲート付きで呼ばれる)"""
    config = Config(os.environ)
    thread = threading.Thread(target=_loop, args=(config,), daemon=True, name='aes-drive-worker')
    thread.start()
    _log(f"起動しました (間隔 {config.interval_sec}秒 / 稼働 {config.hours[0]}-{config.hours[1]}時 JST)")
    return thread
