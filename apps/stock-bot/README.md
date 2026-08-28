# stock-bot — Google Chat 在庫検索ボット

専用スペースに商品名 (の一部)・商品ID・バーコードを送ると、ロジザード在庫スナップショット
(`mirror_logizard_stock`、毎時9〜18時更新) から候補を探してボタンで返し、タップで
ロケーション別フリー在庫を表示するボット。

```
中原さん: ティーツリー
ボット:   🔍 2件見つかりました。タップで在庫を表示します。
          [ティーツリーオイル10ml (フリー40)] [ティーツリーオイル20ml (フリー230)]
(タップ)
ボット:   ティーツリーオイル 20ml
          (teatree20)
          📍 在庫ロケーション (16:00時点)
          ・R1FA-002-001-01: 200個
          ・P1FB-001-002-01: 30個
          ・棚以外 (仮想ロケ等): 15個
```

- 表示は picking の欠品通知と同じ整形 (良品のみ・フリー在庫=在庫数−引当数・フリー降順・
  仮想ロケ(ZZZ等)は「棚以外」合算・「HH:MM時点」+180分超⚠)
- **費用ゼロ**: Chat APIは無料・検索はRenderローカルSQLite・外部従量APIなし
- 定期実行なし (イベント駆動のみ) → jobs-registry 登録対象外

## セキュリティ

- Google Chat からのリクエストは Bearer IDトークンを検証 (issuer=`chat@system.gserviceaccount.com`、
  audience=GCPプロジェクト番号)。検証不能は 401 (fail-closed)
- 発言者メールを `STOCK_BOT_ALLOWED_DOMAIN` (既定 `b-faith.biz`) で制限
- mount 自体を env `STOCK_BOT_PROJECT_NUMBER` でゲート — miniPC は未設定なので mount されない
  (同じ server.js を動かすための二重応答防止)

## セットアップ (中原さんの作業・15分)

1. **GCPコンソール** (プロジェクト = bfaith-portal のサービスアカウントと同じ `balmy-coral-488521-v4` を推奨)
   → 「APIとサービス」→ ライブラリ → **Google Chat API** を有効化
2. Google Chat API の **「設定」タブ** で Chat アプリを構成:
   - アプリ名: `在庫検索ボット` / 説明: 適当でOK / アバター: 任意
   - 機能: 「1:1のメッセージを受信する」「スペースとグループの会話に参加する」を ON
   - 接続設定: **HTTPエンドポイントURL** = `https://bfaith-portal.onrender.com/apps/stock-bot/chat-events`
   - ⚠ **認証オーディエンス (Authentication Audience) は「HTTPエンドポイントURL」ではなく
     「プロジェクト番号 (Project Number)」を選択** — サーバー側はプロジェクト番号を audience
     として検証するため、URLを選ぶと全イベントが401になる
   - 公開設定: ドメイン内 (`b-faith.biz`) の全員 or 特定ユーザー
3. **プロジェクト番号** (GCPコンソール「IAMと管理→設定」の数字のID) をコピーし、
   Render ダッシュボード → bfaith-portal → Environment に
   `STOCK_BOT_PROJECT_NUMBER=<プロジェクト番号>` を追加 → 自動再デプロイを待つ
   (デプロイログに `[server] stock-bot mounted` が出る)
4. Google Chat で**新しいスペース「在庫検索」を作成** → メンバーを追加 →
   「アプリを追加」で `在庫検索ボット` を追加
5. スペースに `ティーツリー` と送って動作確認

## トラブルシュート

- 反応がない → Render env `STOCK_BOT_PROJECT_NUMBER` 未設定 (mountされていない)。
  デプロイログに `[server] stock-bot mounted` があるか確認
- 401 → プロジェクト番号の不一致 (ChatアプリのGCPプロジェクトと env の番号が別)
- 「在庫データの準備中…」 → Render再起動直後のmirror初期化中、または mirror_logizard_stock 空
  (毎時ジョブ `logizard-stock-hourly` の稼働を確認)
- 在庫の鮮度は最大1時間遅れ (18時以降〜翌9時は前日18時時点)。応答の「HH:MM時点」で判断

## 関連

- データ供給: `scripts/logizard-stock/` (miniPC毎時パイプライン、PR #822)
- 整形ロジック: `apps/picking/stock-locations.js` (buildStockLocationsText を title/maxLines オプションで流用)
- テスト: `apps/stock-bot/test-stock-bot.mjs`

## Yahoo API 再認可 (2026-08-28、yahoo-reauth.js)
- 「yahoo再認可」と送る → 認可 URL と手順を返す。ログイン後に戻る `https://b-faith.biz/?code=…` の URL をそのまま貼る (or「yahoo code XXXX」) → VPS `/yahoo/token/init` でトークン交換 → 新しい期限を返す
- 実行できる人: Render env `YAHOO_REAUTH_USERS` (カンマ区切りメール)。未設定なら `PD_RULE_APPROVERS` を流用、それも無ければ誰も実行できない (fail-closed)
- 必要 env: `YAHOO_PROXY_URL` / `YAHOO_PROXY_SECRET` (未発送アラート等と共用で Render に設定済み)
- code はログに出さない。失敗時は上流の文言を返さない (「認可コードが古い可能性」案内のみ)
