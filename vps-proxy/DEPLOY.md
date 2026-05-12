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

## 変更フロー (regression 防止、必ずこの順)

1. **このリポジトリで `vps-proxy/aupay-proxy.js` を編集** (VPS 上で直編集しない)
2. PR → レビュー → merge
3. miniPC から VPS にコピー (変更前に VPS 側でバックアップ推奨):
   ```bash
   ssh -i ~/.ssh/id_ed25519_vps rocky@133.167.122.198 \
     'cp /home/rocky/aupay-proxy.js /home/rocky/aupay-proxy.js.bak_$(date +%Y%m%d_%H%M%S)'
   scp -i ~/.ssh/id_ed25519_vps vps-proxy/aupay-proxy.js \
     rocky@133.167.122.198:/home/rocky/aupay-proxy.js
   ```
4. proxy 再起動:
   ```bash
   ssh -i ~/.ssh/id_ed25519_vps rocky@133.167.122.198 \
     'sudo systemctl restart aupay-proxy.service && systemctl status aupay-proxy.service --no-pager'
   ```
5. **smoke test 必須** (miniPC から):
   ```bash
   node apps/warehouse/smoke-yahoo-proxy.js 14
   ```
   → `✅ Yahoo orderInfo proxy 動作確認 OK` を確認。失敗なら即 rollback (`.bak_*` を戻す + restart)。
6. au PAY 側に影響ないか翌朝の daily-sync 結果で確認

## 鉄則 (memory: feedback_vps_proxy_change_isolation.md)

- **secret rotation と業務ロジック変更を同一変更に混ぜない** ← 2026-05-08 事故の根本原因
- orderInfo の XML 構造は `<Field>` を `<Target>` 内、`Item.` prefix なし、`<IsGetOrderDetail>` なし が正 (公式仕様 https://developer.yahoo.co.jp/webapi/shopping/orderInfo.html)
- 同型 regression (orderInfo XML が壊れて raw 全空文字) が 2026-04-11/12 と 2026-05-08 の 2 回発生

## 過去事故

- **2026-05-08〜10**: PROXY_SECRET rotation (dual_secret 機能追加) のついでに `yahooOrderInfo` 関数の XML 構造が 4/11 修正前に regression → Yahoo API が `od90101` を返却 → yahoo-orders.js が `<Error>` を検知できず空 row 1,254 件 INSERT を 3 日連続。cron は「✅ 1254件取得」と success report してたため気付かず。
  - 復旧: PR #94/#95/#96/#98/#99 (詳細: `g:/共有ドライブ/AI_reference/システム設計/Yahoo!Phase1.1_proxy_regression_fix_20260511.md`)
  - 再発防止: yahoo-orders.js fail-closed 化 (PR #96)、smoke スクリプト (PR #99、`apps/warehouse/smoke-yahoo-proxy.js`)、DQ 強化 (PR #98)、本ディレクトリでの git 管理化
