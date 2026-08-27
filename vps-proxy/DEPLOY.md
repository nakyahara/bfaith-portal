# VPS proxy デプロイ手順

`aupay-proxy.js` は さくら VPS (固定 IP `133.167.122.198`) 上で動く au PAY + Yahoo Shopping 統合プロキシ (port 8080)。Yahoo / au PAY API は固定 IP 必須なので VPS 経由。

## 構成

| 項目 | 値 |
|---|---|
| ファイル本体 | VPS `/home/rocky/aupay-proxy.js` ← **このリポジトリ `vps-proxy/aupay-proxy.js` が正** |
| systemd | `aupay-proxy.service` (`EnvironmentFile=/home/rocky/.env`、`Restart=always`) |
| SSH (miniPC から) | `ssh -i ~/.ssh/id_ed25519_vps rocky@133.167.122.198` |
| secret | `/home/rocky/.env` (PROXY_SECRET / YAHOO_CLIENT_ID / AUPAY_API_KEY 等) — **git 管理外、VPS 上のみ** |
| OAuth トークン | `/home/rocky/yahoo-tokens.json` — git 管理外、再認可で更新 (refresh token 28 日サイクル) |
| Yahoo 公開鍵 | `/home/rocky/yahoo-public-key.pem` — git 管理外 |
| 旧版 | `/home/rocky/yahoo-proxy.js` (Yahoo 専用、aupay-proxy.js に統合される前。deprecated、systemd は aupay-proxy.js を起動) |

## デプロイ構成 (Option A: VPS git clone + commit 指定 deploy)

VPS の `/home/rocky/bfaith-portal/` に bfaith-portal を **sparse-checkout** (`vps-proxy/` のみ取得)。
systemd は `/home/rocky/bfaith-portal/vps-proxy/aupay-proxy.js` を実行。

- VPS の GitHub 認証: read-only deploy key (`~/.ssh/id_ed25519_github_deploy`、`~/.ssh/config` で `github.com-bfaith` host、`IdentitiesOnly yes` + `StrictHostKeyChecking yes`)
- token / 公開鍵は repo ディレクトリ外に置く (git pull で消えない):
  - `.env` に `YAHOO_TOKEN_FILE=/home/rocky/yahoo-tokens.json` + `YAHOO_PUBLIC_KEY_PATH=/home/rocky/yahoo-public-key.pem`
- `.env` / `yahoo-tokens.json` は `chmod 600`、`~/.ssh/` は `700`
- **`vps-proxy/package.json` で `"type": "commonjs"` を宣言** — bfaith-portal repo root の `package.json` は `"type": "module"` (ESM) なので、これがないと `aupay-proxy.js` (CommonJS、`require()` 使用) が ESM 扱いされて `ReferenceError: require is not defined` で起動失敗する

## 変更フロー (regression 防止、必ずこの順)

1. **このリポジトリで `vps-proxy/aupay-proxy.js` を編集** (VPS 上で直編集しない)
2. PR → レビュー → merge → master の commit SHA を控える
3. miniPC から VPS で commit 指定 deploy (`git pull` ではなく `fetch` + `checkout <sha>` で再現可能に):
   ```bash
   ssh -i ~/.ssh/id_ed25519_vps rocky@133.167.122.198 '
     cd /home/rocky/bfaith-portal
     OLD=$(git rev-parse HEAD)            # rollback 用に控える
     git fetch origin master
     git checkout <new-sha or origin/master>
     sudo systemctl restart aupay-proxy.service
     sleep 2
     systemctl is-active aupay-proxy.service
   '
   ```
4. **smoke test 必須** (miniPC から):
   ```bash
   STRICT_YAHOO_ACCESS_TOKEN_SMOKE=1 node apps/warehouse/smoke-yahoo-proxy.js 14
   ```
   → `✅ Yahoo orderInfo proxy 動作確認 OK` を確認。
   PR-Y-B 以降は `/yahoo/orderContact` のスモークも:
   ```bash
   node apps/warehouse/smoke-yahoo-order-contact.js
   ```
   → `✅ orderContact OK` と `✅ orderInfo に ShipDate=true SocialGiftType=true` を確認 (メールの値は表示しない)。
   `STRICT_YAHOO_ACCESS_TOKEN_SMOKE=1` は `/yahoo/access-token` の verify (Cache-Control: no-store / 401 fail-closed) を必須化する。本番 deploy では必ず STRICT を付けること (`YAHOO_TOKEN_MINT_SECRET` 同期漏れで `503` のまま残るのを防ぐ)。
5. **smoke 失敗時の rollback** (手動判断):
   ```bash
   ssh -i ~/.ssh/id_ed25519_vps rocky@133.167.122.198 '
     cd /home/rocky/bfaith-portal && git checkout $OLD && sudo systemctl restart aupay-proxy.service
   '
   # → 再度 smoke で疎通確認
   ```
6. au PAY 側に影響ないか翌朝の daily-sync 結果で確認

## 初回セットアップ (移行時の手順、1 回のみ)

1. VPS で deploy 用 SSH key 生成:
   ```bash
   ssh-keygen -t ed25519 -f ~/.ssh/id_ed25519_github_deploy -N '' -C 'vps-aupay-proxy-deploy'
   ```
2. 公開鍵を GitHub に **read-only deploy key** として登録 (Allow write access オフ):
   ```bash
   # miniPC / ローカルから:
   gh repo deploy-key add <pubkey-file> --title 'vps-aupay-proxy-deploy' -R nakyahara/bfaith-portal
   ```
3. VPS の `~/.ssh/config` に host エントリ追加:
   ```
   Host github.com-bfaith
     HostName github.com
     User git
     IdentityFile ~/.ssh/id_ed25519_github_deploy
     IdentitiesOnly yes
     StrictHostKeyChecking yes
   ```
4. VPS で sparse clone:
   ```bash
   git clone --filter=blob:none --no-checkout git@github.com-bfaith:nakyahara/bfaith-portal.git ~/bfaith-portal
   cd ~/bfaith-portal && git sparse-checkout set vps-proxy && git checkout master
   ```
5. token / 公開鍵を repo 外に配置 + `.env` に path 追加:
   ```bash
   # 既存の /home/rocky/yahoo-tokens.json /home/rocky/yahoo-public-key.pem はそのまま
   echo 'YAHOO_TOKEN_FILE=/home/rocky/yahoo-tokens.json' >> /home/rocky/.env
   echo 'YAHOO_PUBLIC_KEY_PATH=/home/rocky/yahoo-public-key.pem' >> /home/rocky/.env
   chmod 600 /home/rocky/.env /home/rocky/yahoo-tokens.json
   ```
6. systemd unit を変更 (`/etc/systemd/system/aupay-proxy.service`):
   ```
   WorkingDirectory=/home/rocky/bfaith-portal/vps-proxy
   ExecStart=/usr/bin/node /home/rocky/bfaith-portal/vps-proxy/aupay-proxy.js
   ```
   → `sudo systemctl daemon-reload && sudo systemctl restart aupay-proxy.service`
7. smoke 確認 → OK なら旧 `/home/rocky/aupay-proxy.js` を `.bak_pre_gitdeploy_<日時>` にリネーム (動作確認後削除)

## エンドポイント早見表

| Method | Path | Auth | 用途 |
|---|---|---|---|
| GET | `/health` | (なし) | systemd 死活確認 |
| GET | `/yahoo/health` | secret | token 有無 / expires_at |
| GET | `/yahoo/auth-url` | secret | 認可 URL 取得 (再認可時) |
| POST | `/yahoo/token/init` | secret | 認可コードから token 初期化 |
| GET | `/yahoo/token/refresh` | secret | refresh だけ実行 (token は返さない) |
| **POST** | **`/yahoo/access-token`** | **secret + mint-secret** | **current access_token を返却。必要なら自動 refresh。Render/ローカルから Yahoo 商品系 API を直接叩く用 (RakutenYahooSync)** |
| GET | `/yahoo/item-search` | secret | 一般公開 itemSearch v3 (近傍商品法、カテゴリ AI 補完用)。appid=YAHOO_CLIENT_ID |
| GET | `/yahoo/category-search` | secret | 一般公開 categorySearch (カテゴリツリー)。appid=YAHOO_CLIENT_ID |
| GET | `/yahoo/orderList` | secret | 受注一覧 (固定 IP 必須) |
| GET/POST | `/yahoo/orderInfo` | secret | 受注詳細 |
| GET | `/yahoo/externalTalkList` | secret | 問い合わせ一覧 (inquiry-hub 受信同期。Bearer+公開鍵署名はVPS側で付与) |
| GET | `/yahoo/externalTalkDetail` | secret | 問い合わせ詳細 (同上。read-onlyのみ、返信系passthroughは作らない) |
| GET | `/wmshopapi/...` | secret | au PAY 透過 proxy |

- secret 認証は `X-Proxy-Secret` ヘッダ (`PROXY_SECRET` または rotation 中の `PROXY_SECRET_NEXT` のどちらでも可)
- `/yahoo/access-token` は **加えて `X-Token-Mint-Secret`** ヘッダが必要 (生 OAuth token を払い出す endpoint なので、PROXY_SECRET 漏えい時の blast radius 隔離)。
  - VPS `.env`: `YAHOO_TOKEN_MINT_SECRET=...` (必須)、rotation 中は `YAHOO_TOKEN_MINT_SECRET_NEXT=...` も併用可
  - Render `.env`: 同じ値を設定して `X-Token-Mint-Secret` ヘッダで送る
  - 未設定の VPS は `/yahoo/access-token` を **HTTP 503 で disable** (fail-closed)
  - 誤った mint secret は **HTTP 401**
- レスポンス例:
  ```json
  { "ok": true, "access_token": "...", "token_type": "Bearer",
    "expires_at": "2026-05-19T11:30:00.000Z", "refreshed": false }
  ```
  - 失敗時: token config エラー (yahoo-tokens.json 破損等) → **HTTP 500** `{ "ok": false, "error": "token config error" }`
  - upstream エラー (Yahoo OAuth 障害・timeout) → **HTTP 502** `{ "ok": false, "error": "upstream refresh failed" }`
  - token はサーバログにも書き出さない (refreshed フラグと expires_at のみ出力)
  - refresh fetch は `YAHOO_TOKEN_REFRESH_TIMEOUT_MS` (default 15000ms) で必ず完了する

## 鉄則 (memory: feedback_vps_proxy_change_isolation.md)

- **secret rotation と業務ロジック変更を同一変更に混ぜない** ← 2026-05-08 事故の根本原因
- orderInfo の XML 構造は `<Field>` を `<Target>` 内、`Item.` prefix なし、`<IsGetOrderDetail>` なし が正 (公式仕様 https://developer.yahoo.co.jp/webapi/shopping/orderInfo.html)
- 同型 regression (orderInfo XML が壊れて raw 全空文字) が 2026-04-11/12 と 2026-05-08 の 2 回発生

## 過去事故

- **2026-05-08〜10**: PROXY_SECRET rotation (dual_secret 機能追加) のついでに `yahooOrderInfo` 関数の XML 構造が 4/11 修正前に regression → Yahoo API が `od90101` を返却 → yahoo-orders.js が `<Error>` を検知できず空 row 1,254 件 INSERT を 3 日連続。cron は「✅ 1254件取得」と success report してたため気付かず。
  - 復旧: PR #94/#95/#96/#98/#99 (詳細: `g:/共有ドライブ/AI_reference/システム設計/Yahoo!Phase1.1_proxy_regression_fix_20260511.md`)
  - 再発防止: yahoo-orders.js fail-closed 化 (PR #96)、smoke スクリプト (PR #99、`apps/warehouse/smoke-yahoo-proxy.js`)、DQ 強化 (PR #98)、本ディレクトリでの git 管理化
