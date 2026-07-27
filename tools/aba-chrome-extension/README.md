# B-Faith ABAキーワード拡張 (社内用)

セラースプライトの代わりに、Amazonブランド分析 (ABA) の実データを Amazon.co.jp の画面に重ねて表示する Chrome 拡張。

- **商品ページ** (`/dp/...`): 注文ワード表 (キーワード / ABA順位 / 週間変化 / クリック共有 / 転換共有 / Top3比率 / 出現週)。一度照会した ASIN は miniPC 側で「監視対象」になり、古い週のデータも消えず履歴が貯まる
- **検索結果ページ** (`/s?k=...`): 各商品の下に BSR (小カテゴリ+大カテゴリ) / 梱包重量・サイズ / ブランド / ASIN (クリックでコピー) のバッジを表示

データ源は SP-API (公式)。**ページ表示中に Amazon への追加リクエストは発生しない** (DOM を読むだけ → miniPC の API に問い合わせ)。

## インストール (各PC 1回)

1. Chrome で `chrome://extensions` を開く
2. 右上「デベロッパーモード」を ON
3. 「パッケージ化されていない拡張機能を読み込む」→ このフォルダ (`tools/aba-chrome-extension`) を選択
4. 拡張の「詳細」→「拡張機能のオプション」→ APIトークンを貼り付けて保存 → 「接続テスト」で確認
   - トークンは miniPC の `.env` の `ABA_EXT_TOKEN` と同じ値 (管理者から受け取る)

## サーバー側の前提 (miniPC)

- `.env` に `ENABLE_ABA_EXT=1` と `ABA_EXT_TOKEN=<ランダム長文字列>` を設定して `Restart-Service`
- ABAレポートは毎朝の daily-sync (`ABA検索ワード` ジョブ) が自動取込 (要: ブランド登録 + SP-API Brand Analytics ロール)
- 手動取込: `node apps/aba-keywords/fetch-aba-search-terms.js [--backfill 4] [--week YYYY-MM-DD(日曜)] [--dry-run]`

## 注意

- 注文ワードが空 = そのASINが保有データの全週でどの検索語でもクリック上位3に入っていない (ABAの仕様。セラースプライトでも同じ)
- 検索結果バッジの初回表示は 1〜2秒 (SP-API 取得)。2回目以降はキャッシュで即表示 (既定24h)
