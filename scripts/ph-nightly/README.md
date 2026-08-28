# ph-nightly — product-hub「AI情報入力待ち」の夜間自動生成 (miniPC)

かんばんの「AI情報入力待ち」列を、毎晩 02:30 に **Claude Code (サブスク枠・headless)** が処理して
「商品説明確認」列へ進める。人の確認なしにモールへ出品されることはない (書き戻しは必ずレビュー列で止まる)。

- 方式の決定 (2026-08-28 中原さん): API ではなく **サブスク枠**。枠は毎週余っており、月 30〜100 件なら
  費用差も小さい。API 方式の唯一の優位は「認証の持続性」→ 毎晩の `claude auth status` 検査で補う
- 手順の正本 = [`.claude/skills/ph-generate/SKILL.md`](../../.claude/skills/ph-generate/SKILL.md)

## 構成

| もの | 場所 (miniPC) | 出所 |
|---|---|---|
| 作業ディレクトリ | `C:\tools\ph-nightly\` | install.ps1 が作る |
| 権限設定 | `C:\tools\ph-nightly\.claude\settings.json` | [`settings.json`](settings.json) |
| **phq** (固定機能 CLI) | `C:\tools\ph-nightly\phq.mjs` + `./phq` | [`phq.mjs`](phq.mjs) / [`phq`](phq) |
| lint | `C:\tools\ph-nightly\copy_lint.py` | [`copy_lint.py`](copy_lint.py) (正本 = AI_reference。miniPC に G: が無いので同梱) |
| スキル | `C:\tools\ph-nightly\.claude\skills` → clone の `.claude\skills` へのジャンクション | `git pull` で更新 |
| ランナー | [`run-ph-generate.ps1`](run-ph-generate.ps1) (clone 内をそのまま実行) | Task Scheduler `PhGenerateNightly` |
| ログ | `C:\tools\ph-nightly\logs\` (`runner.log` + 実行ごとの `*.out.log` / `*.err.log`) | |
| 監視 | jobs-monitor ping `ph-generate-nightly` (台帳 `config/jobs-registry.mjs`) | dead-man 方式 |

## 安全設計 (なぜこの形か)

**Claude はトークンを見ない。** 無人セッションがシェルで実行できるのは `./phq` と `codex exec` だけ
(`settings.json`: `curl` / `node` / `python` / `cat` / シェル類は deny)。HTTP の相手・メソッド・パスは
`phq.mjs` の中で固定され、`fetch` は GET のみ・本文なし・内部ネットワーク拒否・出力先は作業ディレクトリ内。
外部ページ由来の prompt injection があっても「秘密情報を読んで外へ送る」経路が無い (Codex R1 critical の対応)。

**`dontAsk`**: 非対話には Yes を押す相手がいない。止まる要因は「allowlist 外のツール」と「auto mode 分類器」の
2 層で、`dontAsk` は後者を通す。`bypassPermissions` は deny も無効になるので使わない。

**成否はサーバで判定。lease 中は進捗に数えない。** 実行前後で `queue` の `claimable + leased` (= 未処理) を比べる。
claim しただけで死んだ実行を「進んだ」と誤認しない (Codex R1 high)。
`claimable == 0 && leased == 0` → ok / 減った → partial (翌晩に続く) / 減らない・timeout → fail。
人待ち `blocked` は失敗に数えない (ボードで人に見える)。

**bfaith / Interactive**: SYSTEM 実行だとサブスクの OAuth (bfaith のプロファイル) を読めない。
保存パスワード方式は 2026-05-23 の `WarehouseDailySync` 無音停止と同じ構成になる。
既存の `MallCsvFetchAll` 等と同じ「ログオン中のみ実行」に載せる (bfaith は console にログオンしたまま)。

**1 件ずつ claim**: lease は 30 分。5 件まとめると後半が期限切れになる (Codex R1 high)。夜間上限 15 件。

## 導入 (miniPC、bfaith で)

```powershell
cd C:\Users\bfaith\bfaith-portal
git pull
npm install -g @anthropic-ai/claude-code @openai/codex        # 済 (2026-08-28、claude 2.1.250)
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\ph-nightly\install.ps1
```

人がやる一回きりの作業 (Claude Code のセッションはやらない・できない):

1. `C:\Users\bfaith\.claude\secrets\ph-service-token.txt` に Render の `PH_SERVICE_TOKEN` と同じ値を置く
2. `cd C:\tools\ph-nightly` → `claude` → `/login` (サブスクのアカウント)
3. `codex login` (ChatGPT サブスク)
4. 初回は手で `powershell -NoProfile -ExecutionPolicy Bypass -File C:\Users\bfaith\bfaith-portal\scripts\ph-nightly\run-ph-generate.ps1`
   を回し、`C:\tools\ph-nightly\logs\*.err.log` を見る。
   **permission denied が出ても安易に allow へ足さない** — その操作が `./phq` で代替できないか、
   迂回経路にならないかを先に確認する (allow を増やす = 権限境界を広げる)

未マージのブランチを試すときは worktree を作って `install.ps1 -Repo <worktree>` (スキルとタスクがそこを向く)。マージ後に `-Repo` 無しで再実行して戻す。

## 運用

- **人待ち (⚠ AIが止めました)**: ボードのカードに理由。基本情報を直して詳細画面の「解除する」→ 次の夜に再挑戦
- **朝のチェック**: jobs-monitor の要対応に `ph-generate-nightly` が出たら `C:\tools\ph-nightly\logs\runner.log` の末尾
  - `not logged in` → bfaith で `cd C:\tools\ph-nightly ; claude` → `/login`
  - `no progress` → 同じ logs の `*.err.log` (permission denied / codex 未ログイン / Amazon の HTML 構造変更)
  - `timeout` → 件数が多かっただけとは限らない (ハング・認証・Codex 停止も)。`*.out.log` で最後に何をしていたか見る
  - `partial` → 翌晩に続く。連日続くなら件数か時間の見直し
- **スキルを直したい**: PR で `.claude/skills/ph-generate/SKILL.md` を変更 → miniPC で `git pull` (ジャンクションなので即反映)
- **止めたい**: `Disable-ScheduledTask PhGenerateNightly`

## 費用の目安 (API 方式に切り替える場合の参考)

実測 (2026-08-28、48 件): 1 件あたり input ≈ 8,300 / output ≈ 6,620 トークン (生成+検品+修正 40%)。
Opus 5 で約 31 円/件、月 100 件で約 3,100 円。切り替えの判断ライン = 認証切れが月 1 回以上 / 中原さん以外が運用 / 件数増で夜間に収まらない。
