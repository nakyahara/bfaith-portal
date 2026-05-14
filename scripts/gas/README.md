# GAS スクリプト

このディレクトリは、bfaith-portal の API を叩く Google Apps Script (.gs) を版管理するためのもの。

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
- 重複ガード: 既に A列にある seller_sku は skip
- 1 回の追記行数上限 (`ML_MAX_NEW_ROWS=500`) 超過で throw
- LockService で多重起動防止
- mirror freshness 26h 超えで throw
- レスポンス契約 (`since_days=7` 等) 違反で throw

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
