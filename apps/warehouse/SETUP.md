# データウェアハウス セットアップ手順

## 1. ミニPCでの初期設定

### warehouse.db 初期化 + データ投入

```bash
cd C:\Users\bfaith\bfaith-portal
git pull
npm install

# 商品マスタ投入（NEからダウンロードしたCSV）
node apps/warehouse/csv-import.js products <nedldata.csvのパス>

# セット商品投入
node apps/warehouse/csv-import.js sets <セット商品CSVのパス>

# 受注明細投入（複数ファイル対応）
node apps/warehouse/csv-import.js orders <受注CSV1> <受注CSV2> ...
```

### API起動テスト（ローカル確認用）

```bash
node -e "
import express from 'express';
import router from './apps/warehouse/router.js';
const app = express();
app.use(express.json());
app.use('/warehouse', router);
app.listen(3001, () => console.log('Warehouse API: http://localhost:3001/warehouse'));
"
```

ブラウザで `http://localhost:3001/warehouse` を開いてダッシュボードが表示されればOK。

---

## 2. Cloudflare Tunnel でインターネット公開

### 前提条件
- Cloudflareアカウント（無料）: https://dash.cloudflare.com/sign-up
- ドメイン（b-faith.bizまたは新規取得）をCloudflareに登録
- ミニPCにインターネット接続

### 手順

#### Step 1: cloudflared インストール（ミニPC）

```powershell
# wingetでインストール
winget install Cloudflare.cloudflared

# または直接ダウンロード
# https://github.com/cloudflare/cloudflared/releases/latest
# cloudflared-windows-amd64.msi をダウンロードしてインストール
```

#### Step 2: Cloudflareにログイン

```bash
cloudflared tunnel login
```
ブラウザが開く → Cloudflareにログイン → ドメインを選択 → 認証完了

#### Step 3: トンネル作成

```bash
cloudflared tunnel create warehouse
```
トンネルID（UUID）が表示される。メモする。

#### Step 4: DNS設定

```bash
cloudflared tunnel route dns warehouse warehouse.b-faith.biz
```
※ `warehouse.b-faith.biz` の部分はお好みのサブドメインに変更可能

#### Step 5: 設定ファイル作成

`C:\Users\bfaith\.cloudflared\config.yml` を作成:

```yaml
tunnel: <トンネルID>
credentials-file: C:\Users\bfaith\.cloudflared\<トンネルID>.json

ingress:
  - hostname: warehouse.b-faith.biz
    service: http://localhost:3001
  - service: http_status:404
```

#### Step 6: トンネル起動（テスト）

```bash
cloudflared tunnel run warehouse
```

ブラウザで `https://warehouse.b-faith.biz/warehouse` を開いてダッシュボードが表示されればOK。

#### Step 7: Windows サービスとして自動起動

```bash
cloudflared service install
```

これでミニPC起動時に自動的にトンネルが起動する。

---

## 3. API認証設定

公開後はAPIキーで保護する。

### ミニPCの .env に追加:

```
WAREHOUSE_API_KEY=<任意の長い文字列>
```

### APIアクセス時にヘッダーを付与:

```bash
curl -H "X-API-Key: <APIキー>" https://warehouse.b-faith.biz/warehouse/api/stats
```

またはクエリパラメータ:

```
https://warehouse.b-faith.biz/warehouse/api/stats?api_key=<APIキー>
```

---

## 4. APIエンドポイント一覧

| メソッド | パス | 説明 |
|---------|------|------|
| GET | `/warehouse/api/stats` | DB統計 |
| GET | `/warehouse/api/products?search=&status=&limit=` | 商品検索 |
| GET | `/warehouse/api/products/all?search=&status=` | 単品+セット統合ビュー |
| GET | `/warehouse/api/products/:code` | 商品詳細 |
| GET | `/warehouse/api/sets?search=` | セット商品一覧（原価計算付き） |
| GET | `/warehouse/api/sets/:code` | セット商品詳細 |
| GET | `/warehouse/api/orders?product=&shop=&from=&to=` | 受注検索 |
| GET | `/warehouse/api/orders/daily?from=&to=&platform=` | 日別販売数集計 |
| GET | `/warehouse/api/orders/summary?group_by=shop\|product\|month` | サマリー |
| GET | `/warehouse/api/shops` | 店舗一覧 |
| GET | `/warehouse/api/query?sql=SELECT...` | 任意SQL（SELECT限定） |

---

## 5. CSVファイルの投入方法

### メインPC → ミニPC（共有フォルダ経由）

1. NEカスタムデータでCSVダウンロード（メインPC）
2. `Z:\`（= ミニPCの `data/import/`）にファイルを置く
3. ミニPCで投入コマンドを実行:

```bash
node apps/warehouse/csv-import.js products data/import/nedldata.csv
node apps/warehouse/csv-import.js sets data/import/set_products.csv
node apps/warehouse/csv-import.js orders data/import/orders_2024.csv data/import/orders_2025.csv
```

### 注意事項
- NEのCSVはcp932（Shift_JIS）エンコーディング。スクリプトが自動変換する
- 商品マスタはUPSERT（上書き更新）
- 受注明細はINSERT OR IGNORE（重複排除）
- セット商品はINSERT OR REPLACE（洗い替え）
