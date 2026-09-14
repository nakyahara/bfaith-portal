# テスト用 DATA_DIR の後片付け

SQLiteを開いたまま process.exit() するテストは、同じプロセスの終了フックだけではWindows上でDBを削除できないことがある。scripts/test-temp-dir.mjs はテスト本体を子プロセスで動かし、その終了後に親が自分で作った一時領域だけを削除する。

## 使い方

DBを初期化するモジュールの動的importより前に呼び出す。既存のCLI引数と終了コードは引き継ぐ。

    import { temporaryTestDataDir } from './test-temp-dir.mjs';
    const dir = await temporaryTestDataDir(import.meta.url, 'example-test-');

標準では、呼び出し元に DATA_DIR があっても新しいテスト領域を作り、指定済みの保存先は変更・削除しない。呼び出し元の DATA_DIR を使う仕様を維持するテストだけ、reuseProvided: true を渡す。この場合は既存領域を使い、自動削除はしない。

    await temporaryTestDataDir(import.meta.url, 'example-test-', { reuseProvided: true });

テスト本体には DATA_DIR が設定される。テスト自身が別の場所に作る一時物は対象外なので、可能なら DATA_DIR の下へ置く。実行中のテストが起動した子サーバー等は、テストのfinallyで終了させる。

## 現在の適用先

- scripts/test-iroha-work.mjs
- scripts/test-iroha-work-print.mjs
- apps/purchase-orders/scripts/smoke.mjs
- scripts/smoke-inbound-check-http.mjs

いずれも従来どおり node ファイル名 で実行する。新たな定期清掃は作らず、テストごとに後片付けする。他のテストへの適用はまだ行っていない。

## 保護と限界

親が作成したパスと、その実体がシステム一時フォルダの直下にあることを削除直前にも確認する。ルートが別の場所へのリンクに差し替わった場合は削除を拒否し、失敗として報告する。通常終了・テスト失敗・例外終了後に削除し、削除失敗も成功扱いにしない。

SIGINT/SIGTERMは子へ転送し、終了を待って片付ける。OSによる親プロセスの強制終了や電源断は後片付けを実行できないことがある。その場合の残骸は、別途稼働状況を確認して整理する。

## 検証

    node --test scripts/test-test-temp-dir.mjs

成功、失敗、例外、既存DATA_DIRの保持、標準動作での隔離、内部リンク先の保持、ルート差替え時の拒否を子プロセスで確認する。
