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
  - `timeout` → 件数が多かっただけとは限らない (ハング・認証・Codex 停止も)。`*.out.log` で最後に何をしていたか見る
  - `partial` → 翌晩に続く。連日続くなら件数か時間の見直し
- **スキルを直したい**: PR で `.claude/skills/ph-generate/SKILL.md` を変更 → miniPC で `git pull` → `install.ps1` (コピーなので再 install が要る)
- **止めたい**: `Disable-ScheduledTask PhGenerateNightly`

## 費用の目安 (API 方式に切り替える場合の参考)

実測 (2026-08-28、48 件): 1 件あたり input ≈ 8,300 / output ≈ 6,620 トークン (生成+検品+修正 40%)。
Opus 5 で約 31 円/件、月 100 件で約 3,100 円。切り替えの判断ライン = 認証切れが月 1 回以上 / 中原さん以外が運用 / 件数増で夜間に収まらない。
