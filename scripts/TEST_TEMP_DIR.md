# テスト用一時領域の後片付け

SQLiteを開いたまま process.exit() するテストは、同じプロセスの終了フックだけではWindows上でDBを削除できないことがある。scripts/test-temp-dir.mjs はテスト本体を子プロセスで動かし、その終了後に親が自分で作った一時領域だけを削除する。

## 使い方

DBを初期化するモジュールの動的importより前に呼び出す。既存のCLI引数と終了コードは引き継ぐ。

    import { temporaryTestDataDir } from './test-temp-dir.mjs';
    const dir = await temporaryTestDataDir(import.meta.url, 'example-test-');

標準では、呼び出し元に DATA_DIR があっても新しいテスト領域を作り、指定済みの保存先は変更・削除しない。呼び出し元の DATA_DIR を使う仕様を維持するテストだけ、reuseProvided: true を渡す。この場合は既存領域を使い、自動削除はしない。

    await temporaryTestDataDir(import.meta.url, 'example-test-', { reuseProvided: true });

テスト本体には DATA_DIR が設定される。テスト自身が別の場所に作る一時物は対象外なので、可能なら DATA_DIR の下へ置く。実行中のテストが起動した子サーバー等は、テストのfinallyで終了させる。

## os.tmpdir() を使うテスト

複数の一時フォルダを作るテストでは、先頭で次を呼ぶ。

    import { temporaryTestRoot } from './test-temp-dir.mjs';
    await temporaryTestRoot(import.meta.url);

子プロセスの TMP / TEMP / TMPDIR を専用領域に向け、os.tmpdir() 配下のファイルをまとめて回収する。DATA_DIR は変更しない。明示した別の保存先への書き込みは対象外なので、DATA_DIR 配下に一時物を作るテストには temporaryTestDataDir を使う。既存の終了処理・子サーバーの停止処理は残す。

## 適用範囲

- scripts/test-iroha-work.mjs
- scripts/test-iroha-work-print.mjs
- apps/purchase-orders/scripts/smoke.mjs
- scripts/smoke-inbound-check-http.mjs

上記4件に加え、scripts / apps の standalone テスト104ファイルへ適用した（後片付けなし62件、削除エラーを無視していた42件）。103件は temporaryTestRoot、DATA_DIR 配下に一時物を作る apps/inquiry-hub/smoke-cases.mjs は temporaryTestDataDir を使う。既存の後片付けが備わったテストや node:test の終了フックを使うテストはそのまま維持する。

従来どおり node ファイル名 で実行する。新たな定期清掃は作らず、テストごとに後片付けする。古い作業ブランチで実行する場合は、この修正を取り込む必要がある。

## 保護と限界

親が作成したパスと、その実体がシステム一時フォルダの直下にあることを削除直前にも確認する。ルートが別の場所へのリンクに差し替わった場合は削除を拒否し、失敗として報告する。通常終了・テスト失敗・例外終了後に削除し、削除失敗も成功扱いにしない。

SIGINT/SIGTERMは子へ転送し、終了を待って片付ける。OSによる親プロセスの強制終了や電源断は後片付けを実行できないことがある。その場合の残骸は、別途稼働状況を確認して整理する。

## 検証

    node --test scripts/test-test-temp-dir.mjs

成功、失敗、例外、既存DATA_DIRの保持、標準動作での隔離、内部リンク先の保持、ルート差替え時の拒否、複数の一時領域と開いたままのファイルの回収を子プロセスで確認する。

2026-09-14: 追加104ファイルの実行成功と各一時領域の消滅を確認。共通ヘルパーのテストも10件成功。Windowsで終了時にクラッシュした2件はサーバー停止を待って自然終了するよう修正し、旧スキーマ番号の完全一致で失敗した1件は対象マイグレーション以降であることを確認する形へ修正した。
