# モールCSV自動取得ツール (mall-csv-fetcher)

楽天RMS / Yahoo!管理画面から、手動DLしているCSVをminiPCが自動取得し、
分析ツールの取込パイプラインへ自動投入するための取得レーン。

- **要件定義**: `g:\共有ドライブ\AI_reference\システム設計\モール管理画面CSV自動取得基盤_要件定義_20260706.md`
- **大原則**: 取込タブ(手動アップロード)が正本、自動DLは上流。壊れた日は手動DLで無停止。
- **配置**: miniPC (家庭用IP)。Task Scheduler から毎朝実行。取得後は Render 取込APIへPOST。

## 現在のフェーズ: P0 スパイク (自動化可否の検証)

`rakuten-login-spike.mjs` は「楽天RMSの3段階ログインをPlaywrightで通過できるか」「CAPTCHA/SMS等の
追加認証が挟まるか」を実地確認するための検証スクリプト。ダウンロードの本実装ではない。

### セットアップ (初回のみ / 中原さん作業)

このスクリプトは認証情報が必要なため、**中原さんが実行**します (secret不触の原則)。

```powershell
# 1) Playwright を devDependency として導入 (このリポジトリで一度だけ)
cd C:\path\to\bfaith-portal        # ← 実行するリポジトリのルート
npm install -D playwright
npx playwright install chromium    # Chromium 本体を取得

# 2) 認証情報を用意
cd scripts\mall-csv-fetcher
copy .env.example .env
notepad .env                       # 店舗運用専用IDの認証情報を記入して保存

# 3) スパイク実行 (まずはブラウザ表示で目視)
cd ..\..                           # リポジトリルートへ戻る
node scripts/mall-csv-fetcher/rakuten-login-spike.mjs
```

### 見るべきポイント

- コンソールの `[判定]` セクション:
  - `✅ RMS管理画面ドメインに到達` → 追加認証なしで自動化できる見込み。P1-R本実装へ進める
  - `⚠️ 追加認証らしきキーワードを検出` → CAPTCHA/SMS等が挟まる。完全無人化は困難、要件定義§7へ反映
  - `[warn] ... セレクタ候補すべて不発` → 楽天がDOMを変えた。`spike-output/*.png` を見てセレクタ候補を追記
- `spike-output/` に各ステップのスクリーンショットが残る。詰まった画面を目視で確認する。

### 結果の扱い

- 判定結果 (追加認証の有無・効いたセレクタ) を中原さん経由でClaudeに共有 → 要件定義とP1実装計画を確定。
- スパイクで得たログインシーケンス (効いたセレクタ) が、P1本実装 `rakuten-login.mjs` の土台になる。

## 次フェーズの予定

- **P1-R (楽天)**: RPP/クーポンアドバンスのレポートDL → Render取込APIへPOST。720h遡及のため毎回過去30日を再取得しUPSERT。
- **P1-Y (Yahoo)**: ⚠️Yahooはデフォルト SMS 2段階認証。「パスワードのみログイン」設定 or リフレッシュトークン方式が使えるかを別途P0-2で検証してから着手。使えなければ手動DL継続。
- **P2**: らくらくーぽん置換 (レビューCSV自動DL + フォローメール + クーポン自動発行)。

## 注意

- `.env` はコミット禁止 (`.gitignore` 済み)。
- R-Loginパスワードは90日で失効 → 期限前GChatリマインドをP1で実装。au PAY APIキー月末ローテと同じ運用に載せる。
- miniPCで実行 (Renderのデータセンターipは楽天/Yahooに拒否されるリスク。Yahoo順位チェックの前例あり)。
