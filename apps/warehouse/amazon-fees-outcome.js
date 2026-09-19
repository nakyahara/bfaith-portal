/**
 * amazon-fees-outcome.js — Amazon手数料の取得 (fetch-amazon-fees.js) の結果を「終了コード」と「最後の 1 行」に直す。純粋関数だけ (DB も API も触らない)。
 *
 * なぜ要るか (2026-09-16〜19。朝のログで確かめた: 9/16 = 1,644 件取れて失敗 1 / 9/18 = 450 と 1 / 9/19 = 41 と 1。どの朝も、その SKU の batch でほかは取れていた):
 *   約 3,000 SKU のうち **1 SKU** が SP-API getMyFeesEstimates の Status = ClientError (「client-side error. Please verify your inputs」) を返すだけで、
 *   ステップ全体が exit 1 になり、朝の通知が ❌・8:30 / 10:00 / 11:30 の自動再試行も 3 回とも同じ理由で失敗・最後に 🔴 が出ていた。
 *
 * 決め (Codex #1371 R1 を反映):
 *   - 🚨 **ClientError というだけでは「その SKU だけの、やり直しても直らない失敗」とは言えない** (要求の組み立て・共通の設定の誤りでも ClientError になる)。
 *     見逃してよいのは、**同じ回で仕組みが動いている証拠がある**ときだけ:
 *       ① その回でほかの SKU が 1 件以上取れている (refreshed >= 1)
 *       ② その SKU と **同じ要求 (batch) の中で、ほかの SKU が 1 件以上取れている** (成功が 1 件も無い batch は、件数に依らず batch の側の失敗 = scope 'batch' = 落とす。
 *          先に流れた別の batch の成功は、その要求が健全だった証拠にならない = Codex R2 #1。送り手は 1 件だけの batch を作らない = makeBatches)。
 *          同じ要求でほかが取れている = 認証・マーケットプレイス・要求の形は通っている → 残る原因はその SKU の入力 (ASIN・価格・出品の状態)。
 *          ただし Amazon のコードが明らかに仕組みの側 (認可・流量・サーバ) を指すものは、混ざっていても落とす (SYSTEMIC_CODES)。
 *          コードの一覧を「許すもの」で持たないのは、手数料 API のコードの全体を把握していないため (知らないコードを落とす側にすると、今回の 1 件も毎朝落ちる。⚠️ にコードを出して人が見る)
 *       ③ 件数が上限以内 (5 件 と、API に送った数の 5% の、大きいほうを **超えない**。5% は切り上げない)
 *     → 全体は成功 (exit 0) のまま、**最後の行に ⚠️ と SKU と Amazon のエラーコードを出す** (daily-sync は最後の行を朝の通知にそのまま載せる。直すのは人)
 *   - 証拠が無い・上限を超えた・通信の失敗・ServiceError・応答の形が違う・SKU と突き合わせられない = 今までどおり失敗 (exit 1 = 自動再試行の対象)。
 *     SP-API の Status は Success / ClientError / ServiceError。知らない値は落とす側
 *   - ASIN の分からない SKU (API に送っていない) は別に数える: 5 件までは ⚠️、超えたら失敗 (Amazon の出品ではなく、うちの SKU ⇄ ASIN の対応の側を疑う)
 *   🚨 「取れなかった」を「手数料 0 円」にはしない: 取れなかった SKU は amazon_sku_fees に書かない (fetch 側の既存の動き)。前に取れた値が残っていれば、それが使われ続ける
 */
export const INPUT_FAIL_MIN_LIMIT = 5;
export const INPUT_FAIL_RATE_LIMIT = 0.05;
export const NO_ASIN_LIMIT = 5;
/** 同じ要求でほかの SKU が取れていても、その SKU だけの問題とは見ないコード (仕組みの側)。大文字小文字は区別しない */
export const SYSTEMIC_CODES = new Set(['unauthorized', 'accessdenied', 'invalidaccesstoken', 'quotaexceeded', 'requestthrottled', 'throttled', 'internalfailure', 'internalerror', 'serviceunavailable']);

/**
 * 1 batch ぶんの失敗に scope を付ける: 'sku' = その SKU だけの ClientError と見てよい / 'batch' = batch の側の問題を疑う (落とす)。
 * @param {number} batchSize API に送った件数 @param {number} nSuccess 取れた件数 @param {object[]} errors fetchFeesBatch が返した失敗
 */
export function scopeBatchErrors(batchSize, nSuccess, errors) {
  const list = errors || [];
  const noSuccess = !(nSuccess >= 1);   // 成功が 1 件も無い要求は、1 件だけの batch でも batch の側の失敗として扱う
  const systemic = (e) => typeof e.code === 'string' && SYSTEMIC_CODES.has(e.code.toLowerCase());
  return list.map((e) => ({ ...e, scope: e && e.error === 'ClientError' && typeof e.sku === 'string' && e.sku !== '' && !noSuccess && !systemic(e) ? 'sku' : 'batch' }));
}

export const isSkuInputError = (e) => !!e && e.scope === 'sku' && e.error === 'ClientError' && typeof e.sku === 'string' && e.sku !== '';
export const isNoAsin = (e) => !!e && e.error === 'No ASIN' && typeof e.sku === 'string' && e.sku !== '';

/** 失敗の一覧 (全部。先頭 N 件に切る前) を SKU の入力の誤り / ASIN なし / それ以外 に分ける */
export function splitErrors(errors) {
  const input = [], noAsin = [], hard = [];
  for (const e of errors || []) (isSkuInputError(e) ? input : isNoAsin(e) ? noAsin : hard).push(e);
  return { input, noAsin, hard };
}

const nameOf = (e) => `${e.sku || e.identifier || '?'} (${e.error}${e.code ? `: ${e.code}` : ''})`;
const names = (list) => list.slice(0, 3).map(nameOf).join(', ') + (list.length > 3 ? ` ほか ${list.length - 3} 件` : '');

/**
 * @param {{ refreshed: number, skipped: number, inputErrors: object[], noAsinErrors: object[], hardErrors: object[] }} r
 * @returns {{ exitCode: 0|1, level: 'ok'|'warn'|'fail', line: string, limit: number, attempted: number }}
 */
export function summarizeFeeOutcome({ refreshed = 0, skipped = 0, inputErrors = [], noAsinErrors = [], hardErrors = [] }) {
  const nIn = inputErrors.length, nNo = noAsinErrors.length, nHard = hardErrors.length;
  const attempted = refreshed + nIn + nHard;   // API に送った数 (ASIN なしは送っていない)
  const limit = Math.max(INPUT_FAIL_MIN_LIMIT, attempted * INPUT_FAIL_RATE_LIMIT);
  const head = `取り直し ${refreshed} / 期限内で見送り ${skipped}`;
  const rest = `${nIn ? ` / ClientError の SKU ${nIn} 件 (${names(inputErrors)})` : ''}${nNo ? ` / ASIN の分からない SKU ${nNo} 件` : ''}`;
  const fail = (why) => ({ exitCode: 1, level: 'fail', attempted, limit, line: `❌ Amazon手数料: ${head} / ${why}` });
  if (nHard > 0) return fail(`取得に失敗 ${nHard} 件 (通信・サーバ側・batch ごとの失敗 = やり直せば直り得る: ${names(hardErrors)})${rest}`);
  if (nIn > 0 && refreshed === 0) return fail(`ClientError ${nIn} 件で、この回は 1 件も取れていない (API・設定が正しく動いている証拠が無い = その SKU だけの問題と決められない: ${names(inputErrors)})`);
  if (nIn > limit) return fail(`ClientError の SKU が ${nIn} 件 (上限 ${limit} 件を超えた = 個々の SKU ではなく要求・設定の問題を疑う: ${names(inputErrors)})`);
  if (nNo > NO_ASIN_LIMIT) return fail(`ASIN の分からない SKU が ${nNo} 件 (上限 ${NO_ASIN_LIMIT} 件を超えた = SKU と ASIN の対応の取込を疑う: ${names(noAsinErrors)})${nIn ? ` / ClientError の SKU ${nIn} 件` : ''}`);
  if (nIn > 0 || nNo > 0) {
    const parts = [];
    if (nIn) parts.push(`Amazon が ClientError を返す SKU ${nIn} 件 (${names(inputErrors)}) → その出品 (ASIN・価格・出品の状態) を Amazon で確かめる`);
    if (nNo) parts.push(`ASIN の分からない SKU ${nNo} 件 (${names(noAsinErrors)}) → SKU と ASIN の対応を確かめる`);
    const why = refreshed > 0 ? 'ほかの SKU は取れているので全体は成功扱い' : '全体は成功扱い';   // 取れた SKU が無い回 (ASIN なしだけ) に「取れている」と言わない
    return { exitCode: 0, level: 'warn', attempted, limit, line: `⚠️ Amazon手数料: ${head} / 手数料を取り直せていない: ${parts.join(' / ')}。${why} (前に取れた値があればそれが使われ続ける)` };
  }
  return { exitCode: 0, level: 'ok', attempted, limit, line: `✅ Amazon手数料: ${head}` };
}

/** 成功したステップの要約が「警告つき」か (daily-sync / retry-failed-jobs が、成功でも通知に残す・未達の検知を外さない判断に使う) */
export const isWarnSummary = (summary) => typeof summary === 'string' && summary.trimStart().startsWith('⚠️');
