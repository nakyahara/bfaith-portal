/**
 * lib-env.mjs — モールCSV自動取得ツールの設定 (.env) の読み込み口。
 *
 * 設定は **リポジトリ直下の .env の 1 か所だけ** (中原さん 2026-09-13「いろんなところに ENV が散乱していて管理ができない」)。
 * 以前はこのフォルダの .env (scripts/mall-csv-fetcher/.env) を先に読み、無い項目だけ直下から拾っていた。
 * そのため直下の RMS パスワードを更新しても古い値が使われ続け、楽天レビューの取得が 9/5〜9/13 止まった。
 *
 * - このフォルダの .env は読まない。残っていたら警告だけ出す (中身は読まない)
 * - dotenv は既にある環境変数を上書きしない = タスクの $env:HEADLESS='1' などが優先
 * - どこから node を実行しても同じファイルを読む (process.cwd() ではなくこのファイルの位置から決める)
 */
import { config as loadEnv } from 'dotenv';
import { existsSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
export const REPO_ROOT = join(__dirname, '..', '..');
export const ROOT_ENV_PATH = join(REPO_ROOT, '.env');

loadEnv({ path: ROOT_ENV_PATH, quiet: true });

const RETIRED_ENV_PATH = join(__dirname, '.env');
if (existsSync(RETIRED_ENV_PATH)) {
  console.warn(`[env] ⚠️ ${RETIRED_ENV_PATH} は読みません。設定はリポジトリ直下の .env (${ROOT_ENV_PATH}) だけです。このファイルは消してください`);
}
