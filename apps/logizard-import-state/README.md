# logizard-import-state — ロジザードの毎日の商品マスタの取込の「状態」(Render)

マスタ正本切替 ③c-1b-1。設計 = AI_reference `CompanyDB構想/10` §6.3「③c-1b 契約 v3」H1・H4・H5・H6。

**なぜ**: ロジザードの毎日の商品マスタの取込は、自動の ③ (miniPC の 00:20) と少数件の実機の試験が **1 つの状態と鍵を共用**する。どこからでも届くポータル (Render) に置く (ローカルの状態と真偽だけの旗を後で合わせる作りにしない)。

**戻し方 (③c-1b-3b 契約 v4 + 設計 R1・2026-09-29)**: 自動は 1 本。自動が止まった・miniPC が動かない = **人がどの端末でもブラウザでロジザードに取り込む**。
その取込はポータルの「手の取込」(manual session) の中で行う (始める → ポータルが保存した CSV をダウンロードしてロジザードに置く → 結果の文で終える)。
旧い手の ③ (持ち主 `manual_daily`・Stream Deck の PC で押す) はやめる (鍵を取れない = `retired`)。
**機能の旗 `LZ_MANUAL_V4=on`** (Render の env): 立つまでは今までの動き (旧い手の ③ を使える・nightly に成果物は要らない・手の取込と waiver は `disabled`)。
成果物の受け取り・設定・halt の知らせは旗に依らない (切替の前から成果物を貯める)。旗を立てるのは、成果物の受け口・画面・毎晩の本番がそろった切替のとき。
旗を外した後も、もう開いている / 確認待ちの手の取込は終える・取り消す・確認できる (片付け。新しく始めるのと waiver は旗が要る)。ポータルの状態を作り直した recover (止めた状態で作る) も halt と同じ知らせを積む。

## 持つもの (SQLite `DATA_DIR/logizard-import-state.db`)
- `state` = idle / importing / imported_unverified / verified / unknown / partial / verify_failed
- `halted` = 自動の取込を人が止めた旗 (state とは別。手の取込はこれが立っているときだけ始められる)
- 手の取込 `manual_sessions` (open → completed_ok / needs_review / cancelled・CSV の中身と識別と使うロジザードのアカウントを固定)
- 再適用待ちの義務 `reapply_obligations` と閉じ `reapply_closures` (reapplied / waived)・毎晩の成果物 `daily_artifacts`・毎晩の区切り `nightly_snapshots`・知らせ `outbox`・設定 `settings`
- いまの取込 (実行 ID・誰が・CSV の sha256 と行数・結果)・鍵 (持ち主・目的・期限)・初期化の識別子
- 出来事 (`import_events`) は追記だけ (更新・削除はトリガーで断る)

## 決まり
- 自動 = halted でなく state が idle / verified・開いた手の取込が無いときだけ鍵を取れる。
- **手の取込** (③c-1b-3b v4): 始める = halted・state idle / verified・生きた鍵なし・開いた手の取込なし・確認待ちの needs_review なし (1 つの取引)。
  CSV = 毎晩の成果物 (判定 pass) か、`cutover_phase = transition` の間だけ GAS の CSV (対象の日は今日か昨日・JST)。使うアカウントは `lz_accounts` から。
  始める取引で CSV の全部の商品の再適用待ちの義務を足す。開いている間 = 自動の鍵を取れない・resume できない。
  終える = 結果の文が成功 (総件数 = 行数・エラー 0) かつ ロジザードの履歴のファイル名 = 出した名前・日時 = 始めた後で今より前・アカウント = 固定したもの → completed_ok / どれかが違う → needs_review (知らせを積む・管理者の確認 (ack) まで resume できない)。取り消し = note 必須 (義務は残す)。
- **再適用待ち** = (手の取込, 商品) ごとの義務。毎晩 (nightly) の取込が verified になる取引の中で、その回の importing の前にあった義務のうち、その回の成果物にある商品だけ閉じる。残り = 知らせを積む。人は特定の義務だけ理由を書いて閉じられる (waived)。
- **毎晩の成果物**: バイト列から sha256・行数・CSV の形を計算し直して受け取る (申告と違う = 断る)。同じ source_run_id で違う中身 = 断る。**nightly の importing は同じ識別の成果物 (判定 pass) があるときだけ**。14 日より前は整理 (新しい 3 つと取込の途中の回が使うものは残す)。
- **知らせの outbox**: halt (どこから止めても)・残った再適用待ち・needs_review を同じ取引で積む。送れた印は 1 回だけ。
- **設定**: `cutover_phase` (無い → transition → cutover の一方通行・無い / cutover = GAS の CSV を断る)・`lz_accounts` (無い = 手の取込を始められない)。
- **成果物の台帳** `artifact_ledger` は中身の整理の後も残す = 同じ source_run_id の違う中身をいつまでも断る (同じ中身は入れ直せる)。
- **手の取込を終えるときの履歴の日時は分まで** (秒は 0)・「始めた分 < 履歴 ≦ 今の分」(**始めた分と同じ分 = 要確認** = 始める前の取込と見分けられない・安全側。Codex #1541 R1 High)。閉じ・確認の記録と outbox の送れた印は 1 回だけ一式で (表の決まり)。
- **機械の口 (`/api/status`) には数と真偽だけ** (`manual` = v4・open・needs_review_unacked・pending_reapply・outbox_unsent)。手の取込・設定・waiver の出来事は種類と時刻だけ (誰・アカウント・メモは画面の口 = 3b-4)。
- 実行ボタンを押す直前に `importing` (CSV の sha256・行数)。**始めるのは期限内の鍵・今の初期化の世代で取った鍵・まだ始めていない鍵だけ** (1 つの鍵で始めるのは 1 回だけ)。**一度始めた実行 ID は二度と使えない** (import_runs・追記だけ = 古い要求が同じ実行 ID の新しい回に当たらない)。
- 結果の画面で成功 = `imported_unverified` (**自動も手の ③ も**)。取り込んだ側が直後の書き出しで確かめて `verified`。verified になるまで次の取込・再開はしない。一部だけ = `partial`・分からない = `unknown`。
- **鍵が切れても state は戻らない**。その回の結果は、同じ鍵 (token と実行 ID) なら切れた後でも書ける (ほかは importing の間は鍵を取れない)。
- 起動したときに importing が残っている (鍵は切れている) = `mark-unknown` で unknown。自動では二度と押さない。
- `resolve`・`mark-unknown` は鍵を消す (解除した回を古い鍵で再開させない)。`recover` はまだ始めていない鍵を消す (取込の途中の鍵は残す = 結果は書ける)。
- 解除 (`resolve`) は人がロジザードのインポート履歴を確かめてから・note 必須。partial は「対象外の列に差が無い・差のある商品が全部次の夜の対象にある」か「人が直した」が要る。
- 再開 (`resume`) は未解決の取込が無いときだけ。
- **importing の詳細に mode と対象の日** (`mode` = 自動は nightly / test・手の ③ は manual・`target_as_of` = YYYY-MM-DD)。開始の履歴 (import_runs) に残す。**nightly は同じ対象の日に 1 回だけ** (resolve の後も・手元の済みの印に頼らない)。test / manual は数えない (③c-1b-2b 契約 v3 E)。
- **知らせ済みは「今の状態」と「状態を変えた出来事の番号 (`state_event_id`)」に結ぶ**。状態が変わるたびに消える。`notified` は送ってきた状態と番号が今と同じときだけ (違う = `stale` = 新しい状態を知らせ直す)。`status` の `notified` = 今の状態を知らせたか (K9)。
- 初期化は 1 回だけ (`init`・状態が無くても履歴が残っていれば断る = 消失)。各 PC は同じ識別子を手元の印に持ち、起動のたびに照合 (食い違い・片方が無い = 止める)。消失からの復旧 = `recover` (ポータルの状態が無いときは止めた状態で作る)。

## 口 (Render だけ・Bearer `LZ_LOCK_TOKEN`・無ければ 503)
`GET /apps/logizard-import-state/api/status`・`POST .../api/{init,recover,lock/acquire,lock/extend,lock/release,transition,mark-unknown,resolve,halt,resume,notified}`。
③c-1b-3b-2b: `POST .../api/artifacts?source_run_id&target_as_of&verdict&sha256&rows&by` (本文 = 毎晩の成果物の CSV のバイト列・`application/octet-stream`・4MB まで・この口だけの parser を Bearer の後に)・`GET .../api/artifacts[?limit]`・`GET .../api/artifacts/:source_run_id`・`GET .../api/outbox[?limit]`・`POST .../api/outbox/sent {id, by}`。手の取込・設定・waiver は機械の口に出さない (ログインして使う画面の口 = 3b-4)。
server.js の `JOBS_MONITOR_ENABLED` の中で、**どの body parser よりも前に** mount (miniPC は同じ server.js でも口を立てない = 状態が 2 つにならない・method や Content-Type によらず認証の前に本文を読まない)。断りの文言は決まったもの (本文・内部のパスを返さない)。

## 画面の口 (③c-1b-3b-4a・人がどの端末でもブラウザで使う)
`apps/logizard-import-state/admin-router.js` を `/apps/logizard-import-state/admin-api` に mount (server.js・Render だけ = JOBS_MONITOR_ENABLED・**セッションの後**)。
- 守りの順番 (本文を読む前に全部): ログイン + 管理者 (違う = JSON の 401 / 403) → 書く口は Origin = Host (ブラウザから) → Content-Type を口ごとに固定 (JSON / CSV のバイト列) → この口だけの parser (JSON 64KB・CSV 4MB)。
- 機械の口の Bearer は `/api` だけに掛ける = 画面の口は機械の口を素通りしてセッションの後へ。フォームの parser (urlencoded) もこの前置きは読まない。
- 誰 = セッションのメール (本文の by は使わない)。
- `GET /status` = 全部の見え方 (出来事の中身・開いた手の取込・確認待ち・最近の手の取込・待ち・送れていない知らせ・設定・成果物)。
- メモ・理由は文字で 4〜500 字 (オブジェクト・配列・数 = 400)。Content-Type はちょうどその型 (`application/json` / `application/octet-stream`・後ろの `; charset` は可・似た型 = 415)。
- `GET /pending[?after&limit&product]` = 待ちの義務 (番号の後から・商品で探す = 何件あっても全部に届く)。
- `POST /halt`・`/resume`・`/resolve`・`/mark-unknown`・`/manual/open` (毎晩の成果物)・`/manual/open-gas?lz_account&target_as_of` (本文 = GAS の CSV のバイト列)・`GET /manual/:id/csv` (attachment・no-store・nosniff)・`/manual/:id/complete`・`/cancel`・`/ack`・`/waive`・`/settings`。
- 止める・終える (needs_review) の後は、**今回積んだ知らせを真っ先に**すぐ送る (前の知らせが溜まっていても・要対応スペース `GCHAT_WEBHOOK_JOBS`。応答の `notified` = 今回の知らせを送れたか。送れない = outbox に残る = 定時の入口が送り直す)。
- **画面 (③c-1b-3b-4b)** = `https://bfaith-portal.onrender.com/apps/logizard-import-state/admin` (管理者だけ = requireAdmin・`views/admin.ejs`)。ダッシュボードのカードは作らない = GChat の知らせ (止めた・要確認・再適用待ち) に画面の場所を書く。
  値は全部 textContent で出す (結果の文・メモ・商品 ID・アカウントは信用しない値)。画面の中に手順 (📖 はじめに読む) がある。

## 手順書 (手の取込・どの端末でも)
画面の「📖 手順」と同じ。
1. いつ使う = 自動が止まったと GChat (要対応) に来た / miniPC が動かない / 今日のうちに取り込みたい。
2. 自動を止める (理由) → GChat に知らせ。
3. 手の取込を始める: **自分のロジザードのアカウント** (共通アカウントは使わない)・CSV = 毎晩の成果物から**対象の日が一番新しい判定 pass を自分で選ぶ** (一覧は対象の日の新しい順)。ほかの人が止め直した・再開した = 「画面を読み直して」= 読み直して今の止めの理由を見てから (画面が見ていた止めの番号をポータルが照らす)。
4. CSV をダウンロード (開いている手の取込だけ取れる) → ロジザードの商品マスタの取込の画面に**そのファイルをそのまま** (ファイル名を変えない) → **始めた時刻の次の分になってから**実行。
5. 結果の文をコピー・**取込の履歴から**ファイル名・日時 (分まで)・アカウントを写して「終える」(画面の「比べる用」を写さない = 入力欄は空から)。
6. 全部合う = 完了 / 合わない = 要確認 → ロジザードの履歴を見てメモを書いて「確認」。置かなかった = 取り消し。
7. 原因が直ったら「自動を再開」→ 次の夜の自動が Company DB の値で入れ直す (再適用待ちが減る)。残り続ける商品は理由を書いて waiver。
   **完了でも、結果の文と履歴は人が写したもの (自己申告)** = ロジザードの値が本当に正しいかは次の夜の自動の入れ直しと照合で確かめる。
8. 画面が開かない (Render が止まっている) = 手の取込は始められない = Render を待つ (急ぐ = 中原さんの判断でシステム全体の GAS への戻し)。
9. 手の取込は旗 `LZ_MANUAL_V4=on` (Render の env) が立ってから (切替のとき)。GAS の CSV は移行の段階 (transition) の間だけ。

## 使い方 (人)
`tools/logizard-automation/import-state-cli.js` (status / init / adopt / recover / halt / resume / resolve)。README = `tools/logizard-automation/README.md`。

## 試験
`node scripts/test-logizard-import-admin.mjs` (画面の口・画面・11 件。[11] は本物のブラウザ (playwright の chromium・端末の時間帯はニューヨーク) で画面の JS を動かす)。
`node scripts/test-logizard-import-state.mjs` (28 件)。手の取込・義務・成果物・outbox・設定の口 (画面・CLI) は 3b-2b 以降 (この段階では関数だけ = 使えない)。
