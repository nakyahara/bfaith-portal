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

### ★ログイン方式: 「信頼できる端末」で2FAをスキップ (2026-07-07確定)

P0実行で判明: 楽天RMSの新ログイン(安全認証)は**2段階認証が必須**。ただし楽天公式機能
「[信頼できる端末](https://glogin.rms.rakuten.co.jp/help/27_01.html)」に登録すると、
**その端末(=ブラウザのCookie)では14日間2FAがスキップ**される。これを使う:

- スクリプトは固定プロファイル `.profile-rakuten/` を使う (launchPersistentContext)。ここにセッションCookieが残る。
- **初回だけ人が手動ログイン**して2FA+「信頼できる端末に登録」にチェック → 以後14日間は自動ログインで2FA不要。
- 14日ごとにtrustが切れる → 自動ログインが2FA画面で止まる → GChat通知 → 人が30秒手動再ログイン (月2回程度)。
- TOTP(otplib)自動生成は楽天が対応方式か不確実なため不採用。信頼端末方式が公式かつ確実。

### 手順1: 手動セットアップ (初回 + 14日ごと / 中原さん作業)

```powershell
# .env は不要 (手で入力するため)。固定プロファイルでブラウザが開く
cd c:\tmp\mall-csv-fetcher-work
$env:MANUAL=1; node scripts/mall-csv-fetcher/rakuten-login-spike.mjs
```

開いたブラウザで:
1. R-Login → 楽天会員 と手でログイン
2. 2段階認証を完了 (未登録なら先に楽天会員情報ページで2FAを登録しておく。方式はViber/SMS/メール等お好みで)
3. **「信頼できる端末として登録する」に必ずチェック**
4. RMSトップまで入れたらブラウザを閉じる (プロファイルにセッション保存)

### 手順2: 自動ログイン検証 (以後何度でも)

```powershell
# .env に認証情報を記入しておく (copy .env.example .env → notepad .env)
cd c:\tmp\mall-csv-fetcher-work
node scripts/mall-csv-fetcher/rakuten-login-spike.mjs
```

- `✅ RMSにログイン成功 (信頼端末で2FAスキップ)` → 自動ログイン確立。P1-R本実装へ
- `⚠️ 2段階認証を要求` → 信頼端末が切れた/未登録。手順1をやり直す
- `spike-output/*.png` に各ステップのスクリーンショットが残る

### 結果の扱い

- 手順2で `✅` が出たら、そのプロファイルを使った自動ログインが14日間有効。P1でRPPレポートDLを実装。
- 本番(miniPC)では、手動セットアップもminiPC上の同じプロファイルで行う (端末=ブラウザ単位のため、別マシンのCookieは使えない)。

## P1-R: 楽天RPPレポート 取得〜取込パイプライン (2026-07-09 実装)

連携は **B案**: miniPCでDL→パース→warehouse.db fact→sync→Render mirror (Render取込APIへのPOST案は廃止)。

```
rakuten-rpp-download.mjs (Task Scheduler 別スロット)
  → downloads/ に保存 + <WAREHOUSE_DATA_DIR>/incoming/rakuten-ads/ へ投入 + report_fetch_log 記録
apps/warehouse/import-rakuten-ads-rpp.js (daily-sync 内)
  → incoming/ を走査、CSV/zip を fact_rakuten_ads_rpp (月次×SKU) / fact_rakuten_ads_rpp_daily (日次合計) へ UPSERT
  → 成功 processed/YYYY-MM/ へ、失敗 failed/ へ移動。同一sha256はduplicateスキップ (冪等)
apps/warehouse/sync-rakuten-ads-daily.js (daily-sync 内、取込成功時のみ)
  → mirror_rakuten_ads_rpp / mirror_rakuten_ads_rpp_daily へ chunk POST (--days 70)
```

- **手動フォールバック**: RMSから手でDLしたCSV/zipを `incoming/rakuten-ads/` に置くだけ (自動DLと同一経路)。
- 毎晩2レポート: ①商品別×月ごと(全商品レポートDL) ②すべての広告×日ごと(この条件でDL)。
  ⭐実測: 期間は「**1ヶ月以内**」制約 → **月ごとに1DLずつループ** (今月+先月、月初3日は前々月も。1晩4〜6DL)。
  「月ごとに表示」時の期間入力は YYYY-MM の月入力 (placeholder=Select start/end、id/name無し)。
  720h遡及・不正クリック控除は毎日UPSERTで追従。
- **空=正常**: 広告はスポット出稿 (5のつく日等) のため期間データ無しがあり得る。DLボタン不活性は
  `report_fetch_log` に status='empty' で記録して正常終了 (障害アラートにしない)。
- 単体実行:
  ```powershell
  node scripts/mall-csv-fetcher/fetch-all.mjs                      # 全モール一括 (Task Schedulerはこれを登録)
  node scripts/mall-csv-fetcher/rakuten-rpp-download.mjs           # 楽天のみ (RPP_REPORTS=item,daily で絞り込み)
  node apps/warehouse/import-rakuten-ads-rpp.js --data-dir <DATA_DIR> [--dry-run]
  node apps/warehouse/sync-rakuten-ads-daily.js --data-dir <DATA_DIR> --days 70 [--dry-run]
  ```

### エラー通知 (無音停止禁止) と多モール続行

- **Task Scheduler は `fetch-all.mjs` を登録**。モールごとに子プロセスで実行し、失敗しても次のモールへ続行
  (将来のYahoo等は `FETCHERS` に1行追加)。1レポート内でも失敗は該当レポートの残月のみスキップし他レポートは続行。
- **GChat通知はエラー時のみ** (env `GCHAT_WEBHOOK_MALL_FETCH`、無ければ `GCHAT_WEBHOOK`):
  取得スクリプト自身が業務エラー(FORM_VERIFY/2FA/DL失敗)の詳細+AI調査ガイド
  (実行ログパス/スクショ/再現コマンド/切り分け表)を送信。ランナーは子が通知できない
  異常終了(env不備/クラッシュ/タイムアウト)のみ通知 — 二重通知しない。
- 全console出力は `logs/<name>-<ts>.log` にも保存 (DOMダンプ含む、認証情報はマスク)。
  AI調査時はGChat通知のログパスをそのまま読めばよい。
- 取込側(import/sync)の失敗は daily-sync 既存のGChatサマリに載る (このレーンの通知は取得側のみ)。
- ⚠️ 「すべての広告×日ごと」のフォーム (ラジオID/日付欄DOM) は未実測。初回実行で
  `FORM_VERIFY:` エラーが出たらログの `[DOM:reports-form-*]` を見てセレクタを追記する
  (誤条件のデータを黙って取らないための設計)。

## 次フェーズの予定

- **P1-R 残**: miniPC移設 (プロファイル手動セットアップ+ACL、Task Scheduler 時間帯分離/多重起動禁止)、
  DL失敗/取込失敗の別GChat通知、クーポンアドバンスCSV横展開。
- **P1-Y (Yahoo)**: ⚠️Yahooはデフォルト SMS 2段階認証。「パスワードのみログイン」設定 or リフレッシュトークン方式が使えるかを別途P0-2で検証してから着手。使えなければ手動DL継続。
- **P2**: らくらくーぽん置換 (レビューCSV自動DL + フォローメール + クーポン自動発行)。

## 注意

- `.env` はコミット禁止 (`.gitignore` 済み)。
- R-Loginパスワードは90日で失効 → 期限前GChatリマインドをP1で実装。au PAY APIキー月末ローテと同じ運用に載せる。
- miniPCで実行 (Renderのデータセンターipは楽天/Yahooに拒否されるリスク。Yahoo順位チェックの前例あり)。
