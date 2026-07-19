# ai-insights PC runner (週次AI経営レポート)

毎週火曜朝、bfaith-portal から先週の事実データを取得し、Claude サブスク (`claude -p`) で
論点・助言を生成して GChat 経営インサイトスペースへ投稿する。

- 要件: AI_reference『システム設計/AI経営アドバイス定期配信_要件定義_20260719.md』
- サーバ側 (PR-1): `apps/ai-insights/` — report-input・ジョブ状態機械・⚙️設定画面
- 正本はこのフォルダ (`scripts/ai-insights/`)。実行環境へは**フォルダごとコピー**して使う

## セットアップ (中原さんPC、初回のみ)

前提: Node 20+ / Claude Code CLI ログイン済み (`claude --version` が通ること)

1. このフォルダを `C:\tools\ai-insights\` にコピー
   (⚠️ OneDrive 配下・全角パス配下に置かない — Task Scheduler 事故防止)
2. `.env.example` を `C:\tools\ai-insights\.env` にコピーして値を設定
   - `AI_READ_TOKEN` / `AI_INSIGHT_SERVICE_TOKEN` = Render env と同じ値
   - `GCHAT_WEBHOOK_URL` = 経営インサイトスペースの Incoming Webhook
3. 手動テスト: `C:\tools\ai-insights\run-weekly.bat` をダブルクリック
   → GChat に `*AI経営レポート(週次) ...*` が届けば成功
   → 同じ週の再実行は `[NOTIFY:status=skip] 投稿済み` になる (冪等)
4. Task Scheduler 登録 (taskschd.msc):
   - 全般: 「ユーザーがログオンしているときのみ実行」(**SYSTEM にしない** — Claude 認証がユーザー紐付き)
   - トリガー: 毎週 **火曜 8:30**、「繰り返し間隔 1時間 / 継続時間 10時間」、
     「スケジュールされた時刻を逃した場合、すぐにタスクを開始する」= ON (StartWhenAvailable)
   - 操作: プログラム `C:\tools\ai-insights\run-weekly.bat`
   - 条件: 「AC電源のときのみ」= OFF 推奨

## 動き (運用者向け)

- 火曜 8:30〜: データが揃っていれば生成→投稿して終了。以降の毎時実行は `skip` (冪等)
- データ未充足 (必須モール売上の取込欠損) の間は `retry_later` で待ち、**火曜 18:00 を過ぎても
  揃わなければ「生成なし (理由)」を投稿**する — 無音では止まらない
- `claude -p` が3回失敗 (サブスク上限など) → **フォールバック = 数字の機械整形だけ投稿**
  (`(週次)` ヘッダーは同じ。本文冒頭に「AI生成に失敗」と明記)
- 投稿の成否が不明なまま中断した場合は**自動再投稿しない**。30分後に ⚙️「AI経営レポート設定」
  画面の 🚨要照合 に出るので、GChat を見て「投稿済み扱い / 再投稿」を選ぶ
- PC が火曜オフでも、起動後のキャッチアップ実行で遅れて届く (定時性は要求しない)

## ログ

`C:\tools\ai-insights\logs\YYYY-MM.log`。各実行の終端に `[NOTIFY:status=...]`:

| status | 意味 |
|---|---|
| ok / fallback | 投稿完了 (AI生成 / 機械整形) |
| ok_repost | 生成済みレポートの再投稿完了 |
| skip / skip_busy | 投稿済み or 別プロセス実行中 (正常) |
| retry_later | データ未充足。次の毎時実行で再試行 (正常) |
| blocked_notice | 締切超過で「生成なし」通知を投稿 |
| reconciliation_required | ⚙️画面で人間照合が必要 |
| failed / failed_posting_uncertain | 失敗 (ログ参照) / 投稿成否不明→要照合へ |

## 手動実行・トラブル時

- 特定の週を作り直す: ⚙️画面で対象ジョブを確認してから
  `run-weekly.bat --period-start=YYYY-MM-DD` (月曜日付)
- 認証切れ (`claude_auth`): PowerShell で `claude` を起動してログインし直す → 再実行
- 更新の配布: repo の `scripts/ai-insights/` を再コピー (`.env` と `logs\` は残す)
