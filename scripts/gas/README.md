# GAS スクリプト

このディレクトリは、bfaith-portal の API を叩く Google Apps Script (.gs) を版管理するためのもの。

## `sku-master-missing-checker.gs`

**用途**: bfaith-portal「マスタ登録」(`m_sku_master`) に登録済みで、Google Sheets「商品コード変換テーブル」(A列に SKU) にまだ載っていない SKU を、毎朝 1 回検出して「変換テーブル未登録SKU」シートに上書き出力する。

### 構成

```
[マスタ登録ツール]                  [毎朝07:00 daily-sync]      [Render warehouse-mirror]
m_sku_master (miniPC)        ─────同期─────>                  mirror_sku_master
                                                                   ↓
                                                                   ↓ GET /api/sku-master/recent-missing-candidates
                                                                   ↓ (x-read-token: MIRROR_READ_TOKEN)
                                                                   ↓
                                                              [GAS 07:30]
                                                              スプレッドシート「商品コード変換テーブル」と差分
                                                              ↓
                                                              「変換テーブル未登録SKU」シートに上書き
```

### セキュリティ設計

- **miniPC の `WAREHOUSE_API_KEY` (write 権限あり) は GAS に渡さない**
- Render mirror に `MIRROR_READ_TOKEN` (read-only) を新設、これだけを GAS の Script Properties に置く
- mirror が stale (当日同期されてない) なら GAS は処理を停止 (古いデータでの誤判定回避)

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
   OUTPUT_SHEET_NAME  = 変換テーブル未登録SKU
   ```
4. トリガー追加 → 関数: `runDailyCheck` / 時間主導型 / 日付ベース / 午前7時〜8時

### Render 側セットアップ

bfaith-portal の Render dashboard で環境変数を追加:

```
MIRROR_READ_TOKEN = (新規生成、例: openssl rand -hex 24 で発行した 48 文字)
```

未設定の場合、`GET /api/sku-master/recent-missing-candidates` は 503 (fail-closed) を返す。
