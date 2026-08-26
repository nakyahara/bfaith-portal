# GAS スクリプト

このディレクトリは、bfaith-portal の API を叩く Google Apps Script (.gs) を版管理するためのもの。

## `shipping-folder-cleanup.gs`

**用途**: Drive「出荷_no」(ID `110ONn2xHzfEG5HPt1DRy4P64zv2hpGjh`) 直下の `出荷_XX` フォルダを毎晩空にする (18:16 トリガー、関数 `trashAllFilesInFolder`)。前日のファイルが残ると翌朝の出荷作業と混ざるため、掃除そのものが業務要件。

- ファイルは `setTrashed(true)` でゴミ箱へ (共有ドライブでは完全削除に管理者ロールが要るため)
- `_要確認/<yyyyMMdd-HHmmss>_出荷_XX/` (旧版の隔離フォルダ) が残っていれば併せて片付ける。人が手で置いたフォルダには触らない
- スクリプトプロパティ `DRY_RUN=1` で件数を数えるだけ (ゴミ箱に入れない)
- 失敗時は throw する → Apps Script 標準の実行失敗通知メールで気づく。残ったファイルは翌晩の実行で再試行される

**⚠️ 2026-08-26 に「吸い上げ」を全廃**: 旧 `shipping-trash-ingest.gs` は削除直前に納品書PDF・ピッキングリストPDF を OCR して Render の `apps/shipping-log` へ送っていたが、納品書・ピッキングの情報は梱包支援 (`apps/packing`) / スマホピッキング (`apps/picking`) の Drive 取込が本番系になったため役目終了 (中原さん判断)。隔離・GChat 通知・Render 側取込 API・`sl_*` テーブルも同時に撤去した。Render env `SHIPPING_LOG_INGEST_TOKEN` / `SHIPPING_STAFF_CRON_ENABLED` と、GAS スクリプトプロパティ `SHIPPING_INGEST_BASE` / `SHIPPING_INGEST_TOKEN` / `SHIPPING_GCHAT_WEBHOOK` は不要 (削除してよい)。旧実装は git 履歴を参照。

## `sku-master-missing-checker.gs`

**用途**: bfaith-portal「マスタ登録」(`m_sku_master`) に登録済みで、Google Sheets「商品コード変換テーブル」(A列に SKU) にまだ載っていない SKU を、毎朝 1 回検出して **「商品コード変換テーブル」本体に直接追記** する (A=SKU / C=商品名 / D=NE商品コード / E=数量)。セット商品は components.length 行に展開。

### 構成

```
[マスタ登録ツール]                  [毎朝07:00 daily-sync]      [Render warehouse-mirror]
m_sku_master (miniPC)        ─────同期─────>                  mirror_sku_master
                                                                   ↓
                                                                   ↓ GET /api/sku-master/recent-missing-candidates
                                                                   ↓ (x-read-token: MIRROR_READ_TOKEN)
                                                                   ↓
                                                              [GAS 07:30]
                                                              スプレッドシート「商品コード変換テーブル」既存 SKU と差分
                                                              ↓
                                                              「商品コード変換テーブル」本体に追記
                                                              (A=SKU / C=商品名 / D=NEコード / E=数量、B/F+ は不変)
```

### セキュリティ設計

- **miniPC の `WAREHOUSE_API_KEY` (write 権限あり) は GAS に渡さない**
- Render mirror に `MIRROR_READ_TOKEN` (read-only) を新設、これだけを GAS の Script Properties に置く
- mirror が stale (当日同期されてない) なら GAS は処理を停止 (古いデータでの誤判定回避)

### 安全装置

- `WRITE_MODE=dry_run` (デフォルト) では実際の書き込みをせず、log のみ
- 着地行は **A 列の最終データ行 + 1** (シート全体の `getLastRow()` ではない、F 列等が下まで埋まってても A 列が中抜けしない)
- 書き込み直前安全装置: 着地行範囲 (A〜E) が空セルか再確認、**確認時点で**既存値があったら throw (誤上書き防止)。dry_run / live 共通で発火。Apps Script は getValues と setValues が別 RPC のため、RPC 間の極短時間競合は検知不可 (運用補完)
- 重複ガード: (seller_sku, ne_code) ペアで既存行を判定、ペアが無いものだけ追記 (セット商品の partial write/component 変更を補完できる)
- 同一実行内重複ガード: mirror が同じペアを重複返却しても 1 回しか追記しない
- 1 回の追記行数上限 (`ML_MAX_NEW_ROWS=500`) 超過で throw
- LockService で GAS 多重起動防止
- mirror freshness 26h 超えで throw
- レスポンス契約 (`since_days=7` / 各 item 型 / components 各要素型) 違反で throw

### 運用上の前提 (重要)

- **B 列は asin 用** (中原さんが手動入力)。新規追加行は GAS が空文字 `''` を書く設計のため、**B 列に既定値や数式を予約しないこと**。
- **A 列の最終データ行 + 1 から書く**。途中の空セルは無視して連続追記する設計。
- **トリガー起動時 (07:00〜08:00) と人手編集時間は重ねないこと**。Apps Script には sheet-level lock が無く、GAS と人手編集の完全排他は不可能。書き込み直前安全装置で誤上書きは止まるが、連続トリガー実行で時間を浪費する。トリガー時刻はスプレッドシートを誰も触らない時間帯に設定すること。

### スプレッドシート側セットアップ

1. 対象のスプレッドシートを開いて「拡張機能 → Apps Script」
2. `sku-master-missing-checker.gs` の中身を貼り付け
3. プロジェクトの設定 → スクリプト プロパティ:
   ```
   MIRROR_API_BASE    = https://bfaith-portal.onrender.com/apps/mirror
   MIRROR_READ_TOKEN  = (Render dashboard で発行した token)
   SOURCE_SHEET_NAME  = 商品コード変換テーブル
   SOURCE_SKU_COLUMN  = 1
   SOURCE_HEADER_ROWS = 1
   WRITE_MODE         = dry_run    ← まずこれで動作確認、ログを見て OK なら live に変更
   ```
4. **手動で `runDailyCheck` を一回実行** (dry-run なのでシート無変更)
   - 実行ログに「以下の行を追記する予定」が出る
   - 中原さんが内容を確認 → 問題なければ `WRITE_MODE=live` に書き換え → もう一度実行
5. トリガー追加 → 関数: `runDailyCheck` / 時間主導型 / 日付ベース / 午前7時〜8時

### Render 側セットアップ

bfaith-portal の Render dashboard で環境変数を追加:

```
MIRROR_READ_TOKEN = (新規生成、例: openssl rand -hex 24 で発行した 48 文字)
```

未設定の場合、`GET /api/sku-master/recent-missing-candidates` は 503 (fail-closed) を返す。
