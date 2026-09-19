/**
 * amazon-fees-outcome.js — Amazon手数料の取得 (fetch-amazon-fees.js) の結果を「終了コード」と「最後の 1 行」に直す。純粋関数だけ (DB も API も触らない)。
 *
 * なぜ要るか (2026-09-18〜19):
 *   約 3,000 SKU のうち **1 SKU** が SP-API の ClientError (「client-side error. Please verify your inputs」= その SKU の入力の誤り。ASIN が消えた・価格が不正など) を返すだけで、
 *   ステップ全体が exit 1 になり、朝の通知が ❌・8:30 / 10:00 / 11:30 の自動再試行も 3 回とも同じ理由で失敗・最後に 🔴 が出ていた。
 *   この種の失敗は **何度やり直しても直らない** (再試行の対象にする意味が無い) 一方、その SKU は利益の計算に手数料が入らないので、**黙って緑にもできない**。
 *
 * 決め:
 *   - 「入力の誤り」= その SKU だけの、やり直しても直らない失敗 (ClientError / ASIN が無い)。全体は成功 (exit 0) とし、**最後の行に ⚠️ と SKU を出す**
 *     (daily-sync は最後の行を朝の通知にそのまま載せる = 毎朝見える。直すのは人 = Amazon の出品を確かめる)
 *   - それ以外 (通信の失敗・ServerError・応答の形が違う・SKU と突き合わせられない) = やり直せば直り得る・仕組みの側の問題 → 今までどおり失敗 (exit 1 = 自動再試行の対象)
 *   - 入力の誤りが多すぎるとき (5 件 と 試した数の 5% の大きいほうを超える) は、個々の SKU ではなく設定の問題 (マーケットプレイスの取り違えなど) を疑って失敗にする
 *   🚨 「取れなかった」を「手数料 0 円」にはしない: 取れなかった SKU は amazon_sku_fees に書かない (これは fetch 側の既存の動き。ここでは触らない)
 */
export const INPUT_ERROR_KINDS = new Set(['ClientError', 'No ASIN']);
export const INPUT_FAIL_MIN_LIMIT = 5;
export const INPUT_FAIL_RATE_LIMIT = 0.05;

export const isInputError = (e) => !!e && typeof e.error === 'string' && INPUT_ERROR_KINDS.has(e.error) && typeof e.sku === 'string' && e.sku !== '';

/** 失敗の一覧 (全部。先頭 N 件に切る前) を 入力の誤り / それ以外 に分ける */
export function splitErrors(errors) {
  const input = [], hard = [];
  for (const e of errors || []) (isInputError(e) ? input : hard).push(e);
  return { input, hard };
}

/**
 * @param {{ refreshed: number, skipped: number, inputErrors: object[], hardErrors: object[] }} r
 * @returns {{ exitCode: 0|1, level: 'ok'|'warn'|'fail', line: string, limit: number, attempted: number }}
 */
export function summarizeFeeOutcome({ refreshed = 0, skipped = 0, inputErrors = [], hardErrors = [] }) {
  const nIn = inputErrors.length, nHard = hardErrors.length;
  const attempted = refreshed + nIn + nHard;
  const limit = Math.max(INPUT_FAIL_MIN_LIMIT, Math.ceil(attempted * INPUT_FAIL_RATE_LIMIT));
  const head = `取り直し ${refreshed} / 期限内で見送り ${skipped}`;
  const names = (list) => list.slice(0, 3).map((e) => `${e.sku || e.identifier || '?'} (${e.error})`).join(', ') + (list.length > 3 ? ` ほか ${list.length - 3} 件` : '');
  if (nHard > 0) {
    return { exitCode: 1, level: 'fail', attempted, limit,
      line: `❌ Amazon手数料: ${head} / 取得に失敗 ${nHard} 件 (通信・サーバ側 = やり直せば直り得る: ${names(hardErrors)})${nIn ? ` / 入力の誤りで取れない SKU ${nIn} 件` : ''}` };
  }
  if (nIn > limit) {
    return { exitCode: 1, level: 'fail', attempted, limit,
      line: `❌ Amazon手数料: ${head} / 入力の誤りで取れない SKU が ${nIn} 件 (上限 ${limit} 件を超えた = 個々の SKU ではなく設定の問題を疑う: ${names(inputErrors)})` };
  }
  if (nIn > 0) {
    return { exitCode: 0, level: 'warn', attempted, limit,
      line: `⚠️ Amazon手数料: ${head} / 手数料が取れない SKU ${nIn} 件 (入力の誤り = やり直しても直らない: ${names(inputErrors)})。利益の計算に手数料が入らない → Amazon の出品 (ASIN・価格) を確かめる` };
  }
  return { exitCode: 0, level: 'ok', attempted, limit, line: `✅ Amazon手数料: ${head}` };
}
