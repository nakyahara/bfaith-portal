/**
 * record-guard.js — 「どの行が記録されるか」と「送る行が無い理由」を1か所にまとめる。
 *
 * ★なぜ作ったか (2026-09-07):
 *   `lightbluetb-100` の履歴 `pur-mtqnbj4j-qohhjo` が **LINEギフト1行だけ**でできてしまい、
 *   現場が「価格改定したのに価格が変わらない」と申告した。実際には1件も送っていない。
 *   起きたこと = 新売価は入れたが **行の左端のチェックを入れずに記録した**。
 *   チェックの無い行は記録されず、手動更新モール (Amazon / LINEギフト) の行だけは
 *   チェック無しでも自動で足されるので、**送れる行が0の履歴が黙って作れてしまった**。
 *
 *   → 記録の入口で止める (manualOnly) / 履歴側では「なぜ送る行が無いのか」を必ず言う。
 *
 * ここは純関数だけ (DB も fetch も触らない)。画面とサーバで同じ規則を使うため、
 * 選び分けの規則はこのファイルを正とする (router.js に直接書かない)。
 */
import { MALL_LABELS } from './mall-capabilities.js';

/**
 * 手動更新モール (updatable:false) の行を、チェック無しでも記録するか。
 * ★引き当てできなかった行は足さない — コード不明の行が手動リストに積み上がっても
 *   現場はチェックのしようがない。
 */
export function isAutoRecordedManualRow(row) {
  return !!row?.manual && row.confidence !== 'unresolved' && !!row.listingCode;
}

/** 記録される行 = 画面でチェックした行 + 手動更新モールのチェックリスト行 */
export function chooseRowsToRecord(rows = []) {
  return rows.filter((r) => r.selected || isAutoRecordedManualRow(r));
}

/**
 * これから記録しようとしている中身の内訳。
 * @returns {{chosen:Array, updatable:Array, manual:Array, sendable:Array, manualOnly:boolean}}
 *   manualOnly … 記録はできるが **モールへ送れる行が1行も無い** (チェックの入れ忘れが疑わしい)
 */
export function summarizeRecord(rows = []) {
  const chosen = chooseRowsToRecord(rows);
  const updatable = chosen.filter((r) => !r.manual);
  const manual = chosen.filter((r) => r.manual);
  const sendable = updatable.filter((r) => r.evaluation?.canUpdate);
  return {
    chosen,
    updatable,
    manual,
    sendable,
    // ★「⛔ で全部止まっている」は画面に数が出ているので、ここでは manualOnly と区別する。
    //   黙って死ぬのは「更新できるモールの行を1つも選んでいない」場合だけ
    manualOnly: chosen.length > 0 && updatable.length === 0,
  };
}

/** モール名の一覧を日本語で (重複なし・並びは入力順) */
function mallNamesOf(ops) {
  return [...new Set(ops.map((o) => MALL_LABELS[o.mall] || o.mall))].join('・');
}

/**
 * 履歴に「送る行」が1行も無いときの理由 (日本語)。
 *
 * ★「送る行がありません」だけだと、現場は**送られたのか送られていないのか**が分からない。
 *   2026-09-07 の申告はここで詰まった。何をすれば送れるようになるかまで書く。
 *
 * @param {Array<{mall:string, state:string, initial_state:string}>} operations 履歴の行 (DB のまま)
 * @returns {string}
 */
export function noTargetReasonOf(operations = []) {
  const ops = Array.isArray(operations) ? operations : [];
  if (ops.length === 0) return 'この履歴には行がありません。';

  const manual = ops.filter((o) => o.initial_state === 'manual_required');
  const blocked = ops.filter((o) => o.initial_state === 'blocked_preview');
  // 記録時は送れる行だったが、もう送り終えている (または送らないと決めた) 行
  const finished = ops.filter((o) => o.initial_state === 'previewed' && o.state !== 'previewed');

  if (manual.length === ops.length) {
    const names = mallNamesOf(manual);
    return `この履歴の ${ops.length} 行は、すべてこのツールから送れないモール (${names}) です`
      + `。価格は1件も送られていません — ${names} の価格は管理画面で直してください。`
      + 'このツールで送りたいときは、検索画面で送りたい行の左端のチェックを入れてから記録し直してください'
      + ' (チェックの無い行は記録されません)。';
  }

  const parts = [];
  if (blocked.length > 0) parts.push(`${blocked.length} 行はガードで止まっています (「判定」の列に理由が出ています)`);
  if (manual.length > 0) parts.push(`${manual.length} 行は ${mallNamesOf(manual)} (このツールからは送れません)`);
  if (finished.length > 0) parts.push(`${finished.length} 行はすでに送信を終えています`);
  return parts.length > 0
    ? `送る行がありません — ${parts.join(' / ')}。`
    : '送る行がありません (記録時のガードを通った行だけが対象です)。';
}

/** 記録の入口で止めるときの文言 (サーバ側の最後の関所。画面は先に確認を出す) */
export const MANUAL_ONLY_MESSAGE =
  '送れる行がありません。更新できるモールの行にチェックが入っていません'
  + ' (チェックの無い行は記録されません)。送りたい行の左端のチェックを入れてから記録してください。'
  + '手動更新のチェックリストとしてだけ残すなら、確認のうえもう一度押してください。';
