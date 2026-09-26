# ph-nightly — product-hub「AI情報入力待ち」の夜間自動生成 (miniPC)

かんばんの「AI情報入力待ち」列を、毎晩 02:30 に **Claude Code (サブスク枠・headless)** が処理して
「商品説明確認」列へ進める。人の確認なしにモールへ出品されることはない (書き戻しは必ずレビュー列で止まる)。

- 方式の決定 (2026-08-28 中原さん): API ではなく **サブスク枠**。枠は毎週余っており、月 30〜100 件なら
  費用差も小さい。API 方式の唯一の優位は「認証の持続性」→ 毎晩の `claude auth status` 検査で補う
- 手順の正本 = [`.claude/skills/ph-generate/SKILL.md`](../../.claude/skills/ph-generate/SKILL.md)

## 構成

| もの | 場所 (miniPC) | 出所 |
|---|---|---|
| コード (書き換え不可) | `C:\tools\ph-nightly\bin\` (phq.mjs, copy_lint.py) | install.ps1 が置き、ACL で bfaith に書き込み拒否 |
| 作業ディレクトリ (Claude の cwd) | `C:\tools\ph-nightly\work\` — `./phq` `./phreview` は書き込み拒否、生成物だけ書ける | install.ps1 が作る |
| 権限設定 | `C:\tools\ph-nightly\work\.claude\settings.json` (書き込み拒否) | [`settings.json`](settings.json) |
| **phq** / **phreview** | `bin\phq.mjs` + `work\phq` / `work\phreview` | [`phq.mjs`](phq.mjs) / [`phq`](phq) / [`phreview`](phreview) |
| lint | `bin\copy_lint.py` | [`copy_lint.py`](copy_lint.py) (正本 = AI_reference。miniPC に G: が無いので同梱) |
| スキル | `work\.claude\skills\ph-generate` (**コピー**・書き込み拒否) | `git pull` → `install.ps1` で更新 (ジャンクションだとセッションが次回読む手順を書き換えられる) |
| ランナー | `bin\run-ph-generate.ps1` + `bin\ping.ps1` (**保護されたコピー**。clone や worktree の実体は実行しない) | [`run-ph-generate.ps1`](run-ph-generate.ps1)、Task Scheduler `PhGenerateNightly` |
| ログ | `C:\tools\ph-nightly\logs\` (`runner.log` + 実行ごとの `*.out.log` / `*.err.log`) | |
| 監視 | jobs-monitor ping `ph-generate-nightly` (台帳 `config/jobs-registry.mjs`) | dead-man 方式 |

## 安全設計 (なぜこの形か)

**Claude はトークンを見ない。** 無人セッションがシェルで実行できるのは `./phq` と `./phreview` だけ
(`settings.json`: `curl` / `node` / `python` / `cat` / `codex` / シェル類は deny)。
- HTTP の相手・メソッド・パスは `phq.mjs` の中で固定。`fetch` は GET のみ・本文なし・DNS の A レコードが全部グローバル
  IPv4 でなければ拒否し、**その IP に接続を固定** (DNS rebinding 対策。IPv6 は使わない)・8MB 上限・出力先は作業ディレクトリ直下
- **phq のファイル引数は `page-<ID>.html` / `copy-<ID>.json` / `reason-<ID>.txt` という名前だけ** (パス付き・symlink は拒否)
  → phq 経由でトークンや .env を読ませられない (Codex R2 critical 1)
- `bin/` (phq.mjs, copy_lint.py)・`work/phq`・`work/phreview`・`work/.claude/` (settings + **スキルのコピー**) は
  **ACL で bfaith に書き込み拒否** → Claude が Write ツールで CLI・設定・次回読む手順を書き換えられない (R2/R3 critical)。
  install.ps1 が deny を外して更新し、途中で失敗しても `finally` で掛け直す
- タスクは **RunLevel Limited**: bfaith は Administrators 所属なので、Highest だと昇格トークンで ACL を外せてしまう (R3 high 1)
- `./phreview ID` は Codex 検品の固定ラッパー (プロンプト固定・`_ph_review_<ID>.md` 固定で実体ファイルか lstat 検査・
  `--sandbox read-only` 固定・timeout 600 秒)。`codex exec *` を直接許可すると任意プロンプト・任意オプションを渡せる (R2/R3 critical)。
  🚨 **ファイルの中身はラッパーが読んで stdin で渡す** (2026-09-01)。codex 0.150.1 の read-only sandbox は Windows で
  `powershell.exe` も `cmd.exe` も "blocked by policy" で spawn 拒否するため、「Codex に読ませる」形は必ず失敗する。
  stdin ならサンドボックスの exec 方針に依存せず、他のファイルを読ませない担保も強くなる
- 一時ファイルの削除も phq に閉じる (`./phq clean <ID>`)。`rm` は allow から外した — `rm -f a b c` のような形は
  パターンを外れて拒否され、逆に緩いパターンを足すと work の外まで消せてしまう
外部ページ由来の prompt injection があっても「秘密情報を読んで外へ送る」「コードや手順を書き換えて実行する」経路が無い。
残るリスク (承知の上): phq の lstat と open の間の TOCTOU (Claude は `ln`/`mklink` を実行できないので実用上は無視できる)。

**`dontAsk`**: 非対話には Yes を押す相手がいない。止まる要因は「allowlist 外のツール」と「auto mode 分類器」の
2 層で、`dontAsk` は後者を通す。`bypassPermissions` は deny も無効になるので使わない。

**成否はサーバで判定。lease 中は進捗に数えない。** 実行前後で `queue` の `claimable + leased` (= 未処理) を比べる。
claim しただけで死んだ実行を「進んだ」と誤認しない (Codex R1 high)。
`claimable == 0 && leased == 0` → ok / 減った → partial (翌晩に続く) / 減らない・timeout → fail。
人待ち `blocked` は失敗に数えない (ボードで人に見える)。

**bfaith / Interactive**: SYSTEM 実行だとサブスクの OAuth (bfaith のプロファイル) を読めない。
保存パスワード方式は 2026-05-23 の `WarehouseDailySync` 無音停止と同じ構成になる。
既存の `MallCsvFetchAll` 等と同じ「ログオン中のみ実行」に載せる (bfaith は console にログオンしたまま)。

**1 件ずつ claim** (phq は `--limit` を受け付けない): lease は 30 分。5 件まとめると後半が期限切れになる (Codex R1 high)。夜間上限 15 件。

**成否判定の限界**: 実行前から別の実行が lease していた draft がその実行で完了すると、このランナーの `done` に数えられる (R2 medium 11)。02:30 に手動実行が重なることは想定しない。

## 導入 (miniPC、bfaith で)

```powershell
cd C:\Users\bfaith\bfaith-portal
git pull
npm install -g @anthropic-ai/claude-code @openai/codex        # 済 (2026-08-28、claude 2.1.250)
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\ph-nightly\install.ps1
```

人がやる一回きりの作業 (Claude Code のセッションはやらない・できない):

1. `C:\Users\bfaith\.claude\secrets\ph-service-token.txt` に Render の `PH_SERVICE_TOKEN` と同じ値を置く
2. `cd C:\tools\ph-nightly\work` → `claude` → `/login` (サブスクのアカウント)
3. `codex login` (ChatGPT サブスク)
4. 初回は手で `powershell -NoProfile -ExecutionPolicy Bypass -File C:\tools\ph-nightly\bin\run-ph-generate.ps1`
   を回し、`C:\tools\ph-nightly\logs\*.err.log` を見る。
   **permission denied が出ても安易に allow へ足さない** — その操作が `./phq` で代替できないか、
   迂回経路にならないかを先に確認する (allow を増やす = 権限境界を広げる)

未マージのブランチを試すときは worktree を作って `install.ps1 -Repo <worktree>` (そこから bin/ と work/.claude へ**コピー**される。
タスクやスキルが worktree を直接参照することはない)。マージ後に `-Repo` 無しで再実行して戻す。
`git pull` しただけでは反映されない — 必ず `install.ps1` を回す (コピーが正)。

## 運用

- **人待ち (⚠ AIが止めました)**: ボードのカードに理由。基本情報を直して詳細画面の「解除する」→ 次の夜に再挑戦
- **朝のチェック**: jobs-monitor の要対応に `ph-generate-nightly` が出たら `C:\tools\ph-nightly\logs\runner.log` の末尾
  - `not logged in` → bfaith で `cd C:\tools\ph-nightly\work ; claude` → `/login`
  - `no progress` → 同じ logs の `*.err.log` (permission denied / codex 未ログイン / Amazon の HTML 構造変更)。
    `*.out.log` の `permission_denials` と Claude の最後の報告も見る。**検品ゲートが環境要因で全滅すると
    「lint は通るのに 1 件も submit できない」形で止まる** (9/1 の実例 = codex の sandbox が powershell を拒否)
  - `no progress` + claude が数秒で終了 (`*.out.log` に `Failed to refresh OAuth token`) →
    `~\.claude\.oauth_refresh.lock` の残骸 (9/2 の実例)。ランナーが実行前に削除 + 60 秒後に 1 回だけ再実行する。
    🚨 2026-09-23 (PR3-0) から、削除は **Claude 共通ロックを持っていて、かつ Claude のプロセスが 1 つも無いときだけ** (年齢では消さない)。
  - **Claude 共通ロック** (`scripts/claude-guard/ClaudeGuard.ps1` → install で `bin\ClaudeGuard.ps1`): 同じサブスクの OAuth を
    ProductKWScout (05:00・S4U) も使うので、Claude を動かすランナーは `C:\tools\claude-lock\claude.lock` を排他で開いたまま持つ
    (OS がプロセスの終了で放す)。ランナー自身は起動直後に KILL_ON_JOB_CLOSE の Job Object に入る = 親が落ちると node / claude も止まる。
    Claude の起動前に claude / claude-code / AI ランナーのプロセスが残っていないことも確かめる。
    `another Claude job held the lock` で失敗 → 前の晩のランナーや ProductKWScout がまだ動いていないか (タスクの状態・`Get-Process`) を見る
    それでも駄目なら bfaith で lock を消して `claude auth status` → 小さな `claude -p` で疎通を見る
  - `timeout` → 件数が多かっただけとは限らない (ハング・認証・Codex 停止も)。`*.out.log` で最後に何をしていたか見る
  - `partial` → 翌晩に続く。連日続くなら件数か時間の見直し
- **スキルを直したい**: PR で `.claude/skills/ph-generate/SKILL.md` を変更 → miniPC で `git pull` → `install.ps1` (コピーなので再 install が要る)
- **止めたい**: `Disable-ScheduledTask PhGenerateNightly`

## SP広告KW の夜間 AI (2026-09-23・PR3b)
同じランナーが、原稿のあとに **SP広告KW の AI の依頼**を 1 件ずつ処理する (新しいタスクは作らない)。設計 = AI_reference『Amazon_SP広告KW自動生成_設計方針_20260922.md』§5「PR3 実装計画」。
- 流れ: `bin\ad-kw-ai.mjs` が Render の service-api `/ad-kw-ai/*` から 1 件 claim → **AI を呼ぶ前に Render で予約** (1 依頼 1 回・1 日 `AD_KW_AI_DAILY_CAP` 回) →
  claude を **ツール無し** (`cli.cjs` の `ADKW1` = claude-sonnet-5・`--tools ""`・stdin・JSON) で 1 回 → **送る前に結果を `ad-kw-ai-data\pending\` に保存** → 送信 → 保存を消す
- 課金: `cli.cjs` の preflight (ANTHROPIC_* などの課金経路の環境変数・サブスク認証) + `bin\ad-kw-ai-config.json` の **billing_attestation** (人が「追加使用なし」を確認して `install.ps1 -AttestAdKwBilling '<名前>'` で書く。bin の中なので実行役は書き換えられない)。無ければ claim しない。実モデルが違う / 分からないときも止める
- 時間: ランナーの残り時間から最大 25 分 (原稿は最大 80 分)。実行役は受け取った締め切りまでに予約・CLI・送信を収める (予約の前に残り 9 分以上)
- ping は **`ph-adkw-ai-nightly`** (原稿の `ph-generate-nightly` とは別。原稿の成功で広告の失敗を隠さない)
  - `ok disabled on Render` = Render の `AD_KW_AI_ENABLED` が付いていない (実行役を入れたら付ける)
  - `fail billing_unverified` → `ad-kw-ai-config.json` が無い / 記録が不完全 / `fail preflight:BILLING_MODE_MISMATCH` → 環境変数を消す
  - `partial` = 一部失敗 (予約後の失敗は **needs_review** = 画面で「確認済みにする」→ もう一度頼む。自動では作り直さない) / 1 日の上限 / 未送信が残った (次の晩に先に再送) / 待ちが 36 時間を超えた
  - `fail ad queue check failed` = Render に届かない (「0 件」とは扱わない)
- 共通ロック・Job Object は原稿と同じ (ランナーが持ったまま実行役を起動する。親が落ちたら実行役と claude も止まる)
- 試験: `scripts/test-ph-ad-kw-ai-runner.mjs` (実行役 × 本物の service-api) / `scripts/test-ph-nightly-runner.mjs` (このランナーを偽の Claude で最初から最後まで)

### おまかせ全自動 (2026-09-26・PR3c)
中原さん「KW をこっちで指定するより推奨 KW を出してほしい。いつもチャッピーに聞く時は Amazon のタイトルだけ渡してる」→ **自社商品は毎晩自動**。人は朝に採否とコピーだけ。
設計 = 同じ設計書 §5「PR3c 計画 v1〜v3」。
- ランナーは広告の段の**最初に** `POST /ad-kw-ai/auto-enqueue` を呼ぶ (キューが空でも。1 日 `AD_KW_AUTO_DAILY` 件 (既定 3)・新しい商品から・1 商品 1 回。上限は Render が数える)。
  **対象** (中原さん 9/26) = NE コード `chlorellap` + ポータルで 2026-09-26 17:15 JST 以降に登録した新商品だけ (Notion の既存カードの取り込みは数えない。`apps/product-hub/lib/ad-kw-ai.js` の `AUTO_TARGET_*`)。対象外の商品は画面の「おまかせで作る」で人が頼む
  失敗は `fail auto-enqueue failed` (「0 件」とは扱わない)。フラグ OFF は `auto=off`
- 実行役は claim に `capabilities:['auto']` を付ける (付けない旧い版にはおまかせが渡らない)。おまかせの job は段ごと:
  1. **seeds** = 予約 → AI (種 KW 1〜5 個・材料 = 商品名・Amazon タイトル・楽天タイトル・仕様) → 送信
  2. **collecting** = Render に `POST /jobs/:id/collect` を順に (1 回 = 1 照会。Render が miniPC のサジェスト・ABA を叩く。残り 130 秒 + 余裕を切ったら手放す)
  3. **finalize** → 最終案の packet (観測語・競合 ASIN・Amazon タイトル)
  4. **final** = 予約 → AI → 送信 (Render が材料・競合 ASIN の自動採用・提案を 1 txn で書く)
- 時間が足りなければ段の途中で**手放す** (retries に数えない)。次の晩は続きの段から (claim は final → collecting → seeds の順)。**受付 3 件 ≠ 完了 3 件**
- 材料の取得失敗 (miniPC 停止・ABA 未取込など) → その job はその晩やめて 12 時間後 (`retry_wait`)。同じ照会が 3 晩失敗したら打ち切り
- ping の note: `auto=+N (今日/上限)`・`input=` (材料が見つからない = 画面で種を入れて続ける)・`failed=` (失敗で未確認 = 画面で「確認済みにする」まで partial)

## 費用の目安 (API 方式に切り替える場合の参考)

実測 (2026-08-28、48 件): 1 件あたり input ≈ 8,300 / output ≈ 6,620 トークン (生成+検品+修正 40%)。
Opus 5 で約 31 円/件、月 100 件で約 3,100 円。切り替えの判断ライン = 認証切れが月 1 回以上 / 中原さん以外が運用 / 件数増で夜間に収まらない。
