/**
 * libuv-transient-crash.js — Node/Windows の一過性 libuv クラッシュ判定 (2026-07-25)
 *
 * 症状: 子 node が
 *   `Assertion failed: !(handle->flags & UV_HANDLE_CLOSING), file src\win\async.c, line 76`
 *   を stderr に吐いて abort する。処理内容とは無関係で、プロセス起動直後 or 終了処理中に
 *   libuv の async handle が閉じるレースで落ちている (数秒で終わる短命ジョブに集中)。
 *
 * 実績 (miniPC daily-sync ログ 2026-07-12〜07-25):
 *   楽天レビュー取込 4回 (7/18, 7/20, 7/23, 7/25) ほか Yahoo finance DQ / Qoo10 finance sync など。
 *   7/25 は「CSV 665件取込・GChat通知・processed/ 移動・summary 出力まで完了してから abort」で、
 *   呼び出し側が失敗と判定し後続の mirror sync を丸ごとスキップした (手動 sync で復旧)。
 *
 * 判定は「これ以外を絶対に拾わない」方向で厳しくしている (Codex High 指摘):
 *   ① Windows 限定
 *   ② 終了コードが abort (0xC0000409) — timeout kill (signal あり)・通常の exit 1・
 *      spawn 失敗 (status=null) を除外する
 *   ③ stderr に当該 assert 行がある — 子が中継した他プロセスのログ (stdout) では発火させない
 * ①②③ が全部揃わなければ false。判定漏れは「従来どおり失敗として通知される」だけで、
 * 誤検知 (安全でない再実行) より害が小さい。
 */

/** Windows の abort() 終了コード (0xC0000409 = STATUS_STACK_BUFFER_OVERRUN)。
 *  2026-07-25 の実クラッシュで status=3221226505 を実測 */
export const WINDOWS_ABORT_STATUS = 3221226505;

const UV_TEARDOWN_ASSERT_RE =
  /Assertion failed: !\(handle->flags & UV_HANDLE_CLOSING\), file src[\\/]win[\\/]async\.c/;

/**
 * execFileSync / execSync が投げたエラーが「一過性 libuv クラッシュ」かを判定する。
 * @param {any} e - child_process の同期 API が throw したエラー
 * @param {object} [opts]
 * @param {string} [opts.platform] - テスト用のプラットフォーム上書き (既定 process.platform)
 * @returns {boolean} 再実行して安全な一過性クラッシュなら true
 */
export function isLibuvTransientCrash(e, { platform = process.platform } = {}) {
  if (platform !== 'win32') return false;
  if (!e || e.status !== WINDOWS_ABORT_STATUS) return false;
  if (e.signal) return false; // timeout kill 等 (シグナルで殺された) は対象外
  return UV_TEARDOWN_ASSERT_RE.test(String(e.stderr ?? ''));
}
