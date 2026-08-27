"""
AES送り状並び替え Drive自動化ワーカー

共有ドライブ「ネクストエンジン【出荷関係】」内の出荷_XXフォルダ (実運用では
「出荷_no」フォルダの下。AES_SHIP_PARENT_ID で指定、未指定ならドライブ直下) を定期的に走査し、
AES引当フォルダ (引当パターン_AES….txt があるフォルダ) に納品書PDFが入っていたら、
logi_dispatch.csv と素材フォルダの AES*.pdf (送り状) を使って
納品書順に並び替えた「AES送り状_並び替え済.pdf」を同フォルダに出力する。
失敗時は「AES送り状_エラー.txt」を出力し、入力ファイルが変化したら自動リトライする
(素材=AES*.pdf/CSVはバッチごとに日中入れ替わるため、素材の変化で再試行するのは
失敗中のフォルダのみ。成功済みフォルダはフォルダ内のファイルが変わるまで触らない)。

設計書: G:\\共有ドライブ\\AI_reference\\システム設計\\AES送り状並び替え_Drive自動化_設計_20260722.md

安全設計:
- Driveへの書き込みは AES送り状_並び替え済.pdf / AES送り状_並び替え済_manifest.json /
  AES送り状_エラー.txt の新規作成・自ファイル上書きのみ。削除・移動・リネームは一切行わない。
- manifest.json は「出力ページ→注文番号」の対応表。再印刷の自動印刷がこれを正本として
  完全一致でページを引くため、PDFと**必ず同時に**更新し、失敗時は**同時に失効**させる
  (古い対応表が残ると別人の送り状を印刷しかねない)。
- 失敗時に前回の成功PDFが残っていると誤って印刷・使用される恐れがあるため、
  削除の代わりに「使用禁止」表示のPDFで上書きして失効させる (Codexレビュー指摘1)。
- 突合の競合 (同一配送番号が複数ページ / 複数注文が同一ページ) は黙って進めず
  エラー停止する (Codexレビュー指摘3)。
- ワーカーはシングルスレッドの逐次処理 (フォルダを1つずつ)。Renderインスタンスは1台前提。
  Driveは同名ファイルを許すため、万一の二重実行等で同名の出力ファイルが複数できていた
  場合は上書き先を確定できず、競合エラーで停止する (Codexレビュー2巡目 指摘3)。
  重複ファイルは通常どちらも入力より新しいため、失敗ハンドラのマーカー
  (appProperties) が無い重複は mtime に関係なく検出する (Codexレビュー3巡目 指摘2)。
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

OUTPUT_PDF_NAME = sorter_core.OUTPUT_PDF_NAME
# 出力ページ→注文番号の対応表 (再印刷の自動印刷が完全一致でページを引くための正本)。
# 並び替え済PDFと**必ず同時に**更新・失効させる。古い対応表が残ると別人の送り状を印刷し得る
MANIFEST_NAME = sorter_core.MANIFEST_NAME
ERROR_TXT_NAME = 'AES送り状_エラー.txt'
CSV_NAME = 'logi_dispatch.csv'

# 出力ファイルに付ける appProperties マーカー2種。mtimeだけでは「前回の書き込みが
# 完全に終わったか」を判定できないケースがあるため、書き込みの種別を記録する。
# - CONFLICT_MARKER_PROP: 失敗ハンドラが書いたファイル。完全な失敗通知は全アーティ
#   ファクトに付け、完全な成功更新は全てから外す。混在 = 書き込み途中失敗の証拠
#   として再処理する (Codexレビュー4巡目 指摘1)
# - DUP_ACK_PROP: 同名重複の競合通知として書いたファイル。二重実行で同時新規作成
#   された重複は最初から CONFLICT_MARKER_PROP 付きになり得るため、重複の通知済み
#   判定には専用マーカーを使う (Codexレビュー3巡目 指摘2・4巡目 指摘2)
CONFLICT_MARKER_PROP = 'aesFailureWrite'
DUP_ACK_PROP = 'aesDupAcked'
# 非重複ファイルへの失敗書き込みでは、手動整理で重複が解消された後に残る古い
# DUP_ACK_PROP を同時に消す (残すと復旧再処理が毎サイクル繰り返される)
_MARK_FAILURE = {CONFLICT_MARKER_PROP: '1', DUP_ACK_PROP: None}
_MARK_FAILURE_DUP = {CONFLICT_MARKER_PROP: '1', DUP_ACK_PROP: '1'}
_CLEAR_FAILURE = {CONFLICT_MARKER_PROP: None, DUP_ACK_PROP: None}  # 値nullでキー削除

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


def decide_action(now_utc, local_inputs, shared_inputs, output_files, error_files,
                  manifest_files=()):
    """処理要否判定 (設計書§6)。

    local_inputs:  対象フォルダ内の入力メタ一覧 (納品書・引当パターンtxt)
    shared_inputs: フォルダ横断の素材メタ一覧 (素材AES*.pdf + logi_dispatch.csv)
    output_files / error_files / manifest_files: 前回試行の出力メタ一覧 (通常0〜1件。同名重複時は全件)
    戻り値: 'process' / 'skip_done' (試行済みで入力に変化なし) / 'skip_settling' (整定待ち)

    素材はバッチごとに日中何度も入れ替わる (2026-07-23 実運用で確認。設計時の
    「毎日1回入れ替え」前提は不成立)。前回成功済みのフォルダを新しいバッチの素材で
    再処理すると全件不一致になり、正常な出力PDFを使用禁止で潰してしまうため、
    成功状態 (失敗マーカー無し) のフォルダはフォルダ内の入力が変わらない限りskipする。
    失敗状態のフォルダは素材の入れ替えでも再試行する (CSV置き直しでの自動復旧)。

    ⭐ manifest が無いだけでは再処理しない (意図的)。導入前に処理済みのフォルダは
    manifest を持たないが、そこを再処理すると**今日の素材**で突合することになり、
    過去バッチの注文は全件不一致 → 正常な出力PDFを「使用禁止」で潰してしまう
    (上の段落と同じ理由)。manifest の無い過去フォルダは再印刷の自動印刷の対象外とし、
    読み手側が「manifest が無ければ印刷しない」でfail-closedに倒す (Codexレビュー指摘1への回答)。

    出力PDF・エラーtxtが複数存在する場合、試行の完了時刻には「最も古いもの」を使う。
    完全な試行は全アーティファクトを更新するため、一部だけ新しい = 前回の書き込みが
    途中で失敗した状態であり、skipせず次サイクルで再試行する (Codexレビュー2巡目 指摘1)。
    """
    attempts = list(output_files) + list(error_files) + list(manifest_files)
    failure_state = any(CONFLICT_MARKER_PROP in (f.get('appProperties') or {}) for f in attempts)
    if not attempts or failure_state:
        trigger_inputs = list(local_inputs) + list(shared_inputs)
    else:
        trigger_inputs = list(local_inputs)
    newest_trigger = max(parse_rfc3339(f['modifiedTime']) for f in trigger_inputs)

    if attempts:
        attempt_completed = min(parse_rfc3339(f['modifiedTime']) for f in attempts)
        if newest_trigger <= attempt_completed:
            return 'skip_done'

    # 整定待ちは常に全入力 (素材含む) で判定する。成功状態のトリガーはlocalのみだが、
    # 素材の入れ替え途中に処理すると一時的な不一致でPDFを無効化しかねないため
    # (Codexレビュー PR#605 medium)
    newest_any = max(parse_rfc3339(f['modifiedTime'])
                     for f in list(local_inputs) + list(shared_inputs))
    if now_utc - newest_any < timedelta(seconds=SETTLE_SECONDS):
        return 'skip_settling'

    return 'process'


def has_unacknowledged_duplicates(*groups):
    """同名出力ファイルの重複のうち、競合通知が済んでいない (DUP_ACK_PROP無し) ものがあるか。

    二重実行で作られた同名ファイルは通常どちらも入力より新しいため、mtime基準の
    decide_action では skip_done になってしまう。競合通知として書いたファイルには
    DUP_ACK_PROP が付くので、これの無い重複があれば mtime に関係なく競合処理へ
    進ませる。全件マーカー付きになれば通常判定に戻り、書き込みが毎サイクル
    繰り返されることもない (収束する)。CONFLICT_MARKER_PROP では代用できない:
    二重実行で同時新規作成されたエラーtxtは最初から両方に付いているため
    (Codexレビュー4巡目 指摘2)。
    """
    for group in groups:
        if len(group) > 1 and any(
                DUP_ACK_PROP not in (f.get('appProperties') or {}) for f in group):
            return True
    return False


def has_stale_dup_ack(*groups):
    """重複の手動整理後に、競合通知の出力 (通知済みマーカー付きの1件) が残っている状態か。

    競合通知後にユーザーが重複を1件に整理すると、残ったファイルは DUP_ACK_PROP
    付きのまま「全件失敗マーカー」の定常状態になり、mtime基準では入力が変わるまで
    再処理されない (無効PDF/競合エラーtxtが残り続ける)。通知済みマーカーの付いた
    非重複ファイルを見つけたら一度再処理して復旧させる (Codexレビュー5巡目 指摘)。
    再処理の書き込みは成功時 (_CLEAR_FAILURE)・失敗時 (_MARK_FAILURE) とも
    DUP_ACK_PROP を消すため、復旧処理が毎サイクル繰り返されることはない。
    """
    for group in groups:
        if len(group) == 1 and DUP_ACK_PROP in (group[0].get('appProperties') or {}):
            return True
    return False


def has_incomplete_failure_write(*groups):
    """失敗マーカーの付与状態が混在 = 前回の失敗通知/成功更新が途中で失敗している。

    完全な失敗通知は全アーティファクトに CONFLICT_MARKER_PROP を付け、完全な
    成功更新は全てから外すため、正常な定常状態では全件付きか全件無しになる。
    混在は「エラーtxtは失敗を示すのに正常内容のPDFが残っている」等の途中失敗で、
    両方のmtimeが入力より新しくなり得て decide_action では検出できないため、
    mtimeに関係なく再処理する (Codexレビュー4巡目 指摘1)。
    """
    flags = [CONFLICT_MARKER_PROP in (f.get('appProperties') or {})
             for group in groups for f in group]
    return any(flags) and not all(flags)


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
    検索は corpora='drive' + driveId で行う (allDrives は incompleteSearch の恐れが
    あるため使わない。Codexレビュー指摘6)。素材フォルダが別の共有ドライブにある
    実運用構成に対応するため、driveId はフォルダごとに files.get で解決する
    (2026-07-23 実機確認: 素材=別ドライブ、出荷_XX=ネクストエンジン【出荷関係】)。
    """

    def __init__(self, service_account_json):
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
        creds = service_account.Credentials.from_service_account_info(
            json.loads(service_account_json),
            scopes=['https://www.googleapis.com/auth/drive'],
        )
        self.service = build('drive', 'v3', credentials=creds, cache_discovery=False)
        self._drive_id_cache = {}

    def _drive_id_of(self, folder_id):
        """フォルダが属する共有ドライブのIDを解決する (キャッシュ)。

        共有ドライブのルートIDを渡した場合は driveId = そのID が返る。
        マイドライブ上のフォルダは driveId を持たないので None。
        """
        if folder_id not in self._drive_id_cache:
            res = self.service.files().get(
                fileId=folder_id, fields='id,driveId', supportsAllDrives=True,
            ).execute()
            self._drive_id_cache[folder_id] = res.get('driveId')
        return self._drive_id_cache[folder_id]

    def list_children(self, folder_id):
        drive_id = self._drive_id_of(folder_id)
        files = []
        page_token = None
        while True:
            kwargs = dict(
                q=f"'{folder_id}' in parents and trashed=false",
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
                fields='nextPageToken, incompleteSearch, files(id,name,mimeType,modifiedTime,appProperties)',
                pageSize=200,
                pageToken=page_token,
            )
            if drive_id:
                kwargs['corpora'] = 'drive'
                kwargs['driveId'] = drive_id
            res = self.service.files().list(**kwargs).execute()
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

    def upload_new(self, folder_id, name, content, mimetype, app_properties=None):
        body = {'name': name, 'parents': [folder_id]}
        if app_properties:
            # 値None (キー削除指示) はupdate/copy専用の仕様。createに渡すと拒否され得る
            # ため除外する (新規ファイルに削除すべきキーは無い。Codexレビュー6巡目 指摘)
            props = {k: v for k, v in app_properties.items() if v is not None}
            if props:
                body['appProperties'] = props
        res = self.service.files().create(
            body=body,
            media_body=self._media(content, mimetype),
            supportsAllDrives=True,
            fields='id',
        ).execute()
        return res['id']

    def overwrite(self, file_id, content, mimetype, app_properties=None):
        kwargs = {}
        if app_properties is not None:
            kwargs['body'] = {'appProperties': app_properties}
        self.service.files().update(
            fileId=file_id,
            media_body=self._media(content, mimetype),
            supportsAllDrives=True,
            **kwargs,
        ).execute()
        return file_id


# ───────────────────────── ワーカー本体 ─────────────────────────

class Config:
    def __init__(self, env):
        self.sa_json = env.get('GOOGLE_SERVICE_ACCOUNT_JSON') or ''
        self.root_id = env.get('AES_DRIVE_ROOT_ID') or ''
        # 出荷_XXフォルダの親。実運用ではドライブ直下ではなく「出荷_no」フォルダの下に
        # あるため個別指定 (未指定ならドライブ直下 = root_id を走査する従来挙動)
        self.ship_parent_id = env.get('AES_SHIP_PARENT_ID') or self.root_id
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
        self.doc_names = {}           # id(fitz doc) -> 素材ファイル名 (manifestの出所記録用)
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
                label_files, extractor, duplicates=self.duplicate_barcodes,
                doc_names=self.doc_names)
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

        folders = [f for f in client.list_children(self.config.ship_parent_id)
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

        output_files = [f for f in non_folder if f['name'] == OUTPUT_PDF_NAME]
        manifest_files = [f for f in non_folder if f['name'] == MANIFEST_NAME]
        error_files = [f for f in non_folder if f['name'] == ERROR_TXT_NAME]

        # 処理要否判定 (素材のメタも入力に含める。CSV不在時はフォルダ側の変化だけで判定し、
        # エラー通知→CSVが置かれたら modifiedTime が新しくなるので自動リトライされる)。
        # ただし①通知済みマーカーの無い同名重複 (二重実行の痕跡) ②失敗マーカーの混在
        # (前回の書き込みが途中で失敗した証拠) ③重複整理後に残った通知済みファイル
        # (競合解消後の復旧) がある場合は、mtime上は完了済みに見えても skip せず
        # 処理へ進ませる
        local_inputs = invoices + pattern_txts
        shared_inputs = labels_meta + ([csv_meta] if csv_meta else [])
        if not (has_unacknowledged_duplicates(output_files, error_files, manifest_files)
                or has_incomplete_failure_write(output_files, error_files, manifest_files)
                or has_stale_dup_ack(output_files, error_files, manifest_files)):
            action = decide_action(self._now_utc(), local_inputs, shared_inputs,
                                   output_files, error_files, manifest_files)
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

        # Driveは同名ファイルを許すため、二重実行等で出力ファイルが複数できていると
        # どれを更新すべきか確定できない。以後は片方だけ更新される事故になるため停止する
        # (削除はしない方針なので自動では直せない。Codexレビュー2巡目 指摘3)
        duplicated_outputs = [name for name, group in
                              ((OUTPUT_PDF_NAME, output_files), (MANIFEST_NAME, manifest_files),
                               (ERROR_TXT_NAME, error_files))
                              if len(group) > 1]
        if duplicated_outputs:
            reasons.append(f'同名の出力ファイルが複数あります ({", ".join(duplicated_outputs)})。'
                           'どれを更新すべきか確定できないため処理を停止しました。'
                           '重複ファイルを手で整理してください')

        if csv_meta is None:
            reasons.append(f'{CSV_NAME} が見つかりません (bfaithポータルdataフォルダを確認してください)')
        if not labels_meta:
            reasons.append('素材フォルダに AES*.pdf (送り状) が見つかりません')

        matched_pages = []
        matches = []
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
                order_invoice = {}   # 注文番号 -> 初出の納品書ファイル名
                cross_dups = []      # 納品書をまたいだ重複注文番号
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
                        # 同一納品書内の重複は extract_order_numbers 側でdedupe済み。
                        # 別の納品書ファイルに同じ注文番号が出た場合は、古い納品書PDFの
                        # 残留の疑いがあるため黙って除去せず競合停止する (Codex2巡目 指摘2)
                        for order_number in orders:
                            if order_number in order_invoice:
                                cross_dups.append(
                                    f'{order_number} ({order_invoice[order_number]} / {invoice_meta["name"]})')
                            else:
                                order_invoice[order_number] = invoice_meta['name']
                                all_orders.append(order_number)
                    except Exception as e:
                        reasons.append(f'{invoice_meta["name"]}: 納品書処理エラー: {e}')
                if cross_dups:
                    reasons.append('同じ注文番号が複数の納品書に含まれています: '
                                   + ', '.join(cross_dups)
                                   + ' (古い納品書PDFが残っていないか確認してください)')

                if not reasons:
                    matches, unmatched = sorter_core.match_orders_detailed(
                        all_orders, shared.order_shipping_map, shared.barcode_map)
                    matched_pages = [(m['doc'], m['source_page']) for m in matches]
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
            self._notify_failure(folder, reasons, output_files, error_files, manifest_files)
            return

        # ここに来た時点で output_files / manifest_files / error_files は0〜1件 (複数なら上で停止済み)
        output_file = output_files[0] if output_files else None
        manifest_file = manifest_files[0] if manifest_files else None
        try:
            # PDF組み立ての失敗 (壊れたPDF等) もログだけにせず人に見える形で通知する。
            # PDFとmanifestは**先に両方メモリ上で作ってから**書き込む (片方だけ作れた状態を作らない)。
            # 書き込みの途中で落ちても、manifest の output_pdf_sha256 が実物と一致しなければ
            # 読み手 (再印刷の自動印刷) は使わない = fail-closed
            label_bytes = sorter_core.build_label_pdf(matched_pages)
            manifest_bytes = sorter_core.build_manifest(
                matches, shared.doc_names, label_bytes, unmatched_orders=[],
                generated_at=datetime.now(JST), folder_name=folder['name'],
                invoice_files=invoices)
            if output_file:
                # 成功したので失敗ハンドラのマーカーは外す
                client.overwrite(output_file['id'], label_bytes, 'application/pdf',
                                 app_properties=_CLEAR_FAILURE)
            else:
                client.upload_new(folder['id'], OUTPUT_PDF_NAME, label_bytes, 'application/pdf')
            if manifest_file:
                client.overwrite(manifest_file['id'], manifest_bytes, 'application/json',
                                 app_properties=_CLEAR_FAILURE)
            else:
                client.upload_new(folder['id'], MANIFEST_NAME, manifest_bytes, 'application/json')
        except Exception as e:
            self._notify_failure(
                folder,
                [f'{OUTPUT_PDF_NAME} / {MANIFEST_NAME} の生成/アップロードに失敗しました: {e}'],
                output_files, error_files, manifest_files)
            return

        # 過去のエラーtxtは削除せず「解消済み」に上書きする (Drive削除禁止ルール)
        for error_file in error_files:
            try:
                client.overwrite(error_file['id'], build_resolved_txt(datetime.now(JST)), 'text/plain',
                                 app_properties=_CLEAR_FAILURE)
            except Exception as e:
                _log(f"フォルダ '{folder['name']}' のエラーtxt更新に失敗 (出力は完了済み): {e}")

        _log(f"出力完了: {folder['name']} / {OUTPUT_PDF_NAME} ({len(matched_pages)}ページ)")

    def _notify_failure(self, folder, reasons, output_files, error_files, manifest_files=()):
        """エラーtxtを作成/上書きしてから、残っている旧出力PDF・manifest全件を失効させる。

        順序が重要: 先にPDFを無効化した後でエラーtxtの書き込みに失敗すると、
        「入力より新しいPDFだけがある」状態になり decide_action が完了済みと誤判定して
        エラーtxtの無いまま恒久skipに陥る (Codexレビュー3巡目 指摘1)。
        エラーtxtを先に書き、書けなかった場合はPDF無効化も見送って次サイクルの
        再試行に任せる (全アーティファクトの最古mtime基準により必ず再試行される)。
        書いたファイルには CONFLICT_MARKER_PROP を付け、マーカーの混在 =
        書き込み途中失敗を検出できるようにする。同名重複グループへの書き込みには
        DUP_ACK_PROP も付け、競合通知済みであることを記録する。
        """
        client = self.client
        now_jst = datetime.now(JST)

        # 同名重複グループへの書き込みは「競合通知済み」マーカーも付ける
        txt_props = _MARK_FAILURE_DUP if len(error_files) > 1 else _MARK_FAILURE
        pdf_props = _MARK_FAILURE_DUP if len(output_files) > 1 else _MARK_FAILURE
        manifest_props = _MARK_FAILURE_DUP if len(manifest_files) > 1 else _MARK_FAILURE

        try:
            content = build_error_txt(now_jst, reasons)
            if error_files:
                for error_file in error_files:
                    client.overwrite(error_file['id'], content, 'text/plain',
                                     app_properties=txt_props)
            else:
                client.upload_new(folder['id'], ERROR_TXT_NAME, content, 'text/plain',
                                  app_properties=_MARK_FAILURE)
        except Exception as e:
            _log(f"フォルダ '{folder['name']}' のエラー通知の書き込みに失敗 "
                 f"(旧出力PDFの無効化は見送り、次サイクルで再試行): {e}")
            return

        # 前回の成功PDFが残っていると誤使用の恐れがあるため無効化する (削除はしない)
        for output_file in output_files:
            try:
                client.overwrite(output_file['id'], build_invalid_pdf(now_jst), 'application/pdf',
                                 app_properties=pdf_props)
            except Exception as e:
                _log(f"フォルダ '{folder['name']}' の旧出力PDFの無効化に失敗: {e}")

        # manifest も必ず同時に失効させる。古い対応表が残ると、新しいバッチの伝票を
        # 古いページ番号で引いて**別人の送り状を印刷**しかねない (自動印刷の最大リスク)
        for manifest_file in manifest_files:
            try:
                client.overwrite(manifest_file['id'],
                                 sorter_core.build_invalid_manifest(now_jst, reasons,
                                                                    folder_name=folder['name']),
                                 'application/json', app_properties=manifest_props)
            except Exception as e:
                _log(f"フォルダ '{folder['name']}' の旧manifestの無効化に失敗: {e}")

        _log(f"エラー通知: {folder['name']} ({len(reasons)}件)")


# ───────────────────────── 起動 ─────────────────────────

def _loop(config):
    client = None
    while True:
        try:
            if within_hours(datetime.now(JST), config.hours):
                if client is None:
                    client = DriveClient(config.sa_json)
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
