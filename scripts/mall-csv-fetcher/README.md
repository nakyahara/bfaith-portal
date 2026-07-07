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

## 次フェーズの予定

- **P1-R (楽天)**: RPP/クーポンアドバンスのレポートDL → Render取込APIへPOST。720h遡及のため毎回過去30日を再取得しUPSERT。
- **P1-Y (Yahoo)**: ⚠️Yahooはデフォルト SMS 2段階認証。「パスワードのみログイン」設定 or リフレッシュトークン方式が使えるかを別途P0-2で検証してから着手。使えなければ手動DL継続。
- **P2**: らくらくーぽん置換 (レビューCSV自動DL + フォローメール + クーポン自動発行)。

## 注意

- `.env` はコミット禁止 (`.gitignore` 済み)。
- R-Loginパスワードは90日で失効 → 期限前GChatリマインドをP1で実装。au PAY APIキー月末ローテと同じ運用に載せる。
- miniPCで実行 (Renderのデータセンターipは楽天/Yahooに拒否されるリスク。Yahoo順位チェックの前例あり)。
