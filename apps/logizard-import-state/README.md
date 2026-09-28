# logizard-import-state — ロジザードの毎日の商品マスタの取込の「状態」(Render)

マスタ正本切替 ③c-1b-1。設計 = AI_reference `CompanyDB構想/10` §6.3「③c-1b 契約 v3」H1・H4・H5・H6。

**なぜ**: ロジザードの毎日の商品マスタの取込は、自動の ③ (miniPC の 00:20) と戻し方の手の ③ (Stream Deck の PC の auto-barcode.js `--only-daily`) の 2 か所から動く。
2 台の PC が **1 つの状態と鍵を共用**するために、両方から届くポータル (Render) に置く (ローカルの状態と真偽だけの旗を後で合わせる作りにしない)。

## 持つもの (SQLite `DATA_DIR/logizard-import-state.db`)
- `state` = idle / importing / imported_unverified / verified / unknown / partial / verify_failed
- `halted` = 自動の取込を人が止めた (戻し方の手の ③ はこれが立っているときだけ)
- いまの取込 (実行 ID・誰が・CSV の sha256 と行数・結果)・鍵 (持ち主・目的・期限)・初期化の識別子
- 出来事 (`import_events`) は追記だけ (更新・削除はトリガーで断る)

## 決まり
- 自動 = halted でなく state が idle / verified のときだけ鍵を取れる。手の ③ = halted かつ idle / verified。
- 実行ボタンを押す直前に `importing` (CSV の sha256・行数)。結果の画面で成功 = 自動は `imported_unverified` (直後の書き出しで確かめて `verified`)・手の ③ は `idle`。一部だけ = `partial`・分からない = `unknown`。
- **鍵が切れても state は戻らない**。その回の結果は、同じ鍵 (token と実行 ID) なら切れた後でも書ける (ほかは importing の間は鍵を取れない)。
- 起動したときに importing が残っている (鍵は切れている) = `mark-unknown` で unknown。自動では二度と押さない。
- 解除 (`resolve`) は人がロジザードのインポート履歴を確かめてから・note 必須。partial は「対象外の列に差が無い・差のある商品が全部次の夜の対象にある」か「人が直した」が要る。
- 再開 (`resume`) は未解決の取込が無いときだけ。
- 初期化は 1 回だけ (`init`)。各 PC は同じ識別子を手元の印に持ち、起動のたびに照合 (食い違い・片方が無い = 止める)。消失からの復旧 = `recover` (ポータルの状態が無いときは止めた状態で作る)。

## 口 (Render だけ・Bearer `LZ_LOCK_TOKEN`・無ければ 503)
`GET /apps/logizard-import-state/api/status`・`POST .../api/{init,recover,lock/acquire,lock/extend,lock/release,transition,mark-unknown,resolve,halt,resume,notified}`。
server.js の `JOBS_MONITOR_ENABLED` の中で mount (miniPC は同じ server.js でも口を立てない = 状態が 2 つにならない)。

## 使い方 (人)
`tools/logizard-automation/import-state-cli.js` (status / init / adopt / recover / halt / resume / resolve)。README = `tools/logizard-automation/README.md`。

## 試験
`node scripts/test-logizard-import-state.mjs` (11 件)
