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
 *   → 記録の入口で止める / 履歴側では「なぜ送る行が無いのか」を必ず言う。
 *
 * ここは純関数だけ (DB も fetch も触らない)。画面とサーバで同じ規則・同じ文言を使うため、
 * 選び分けと文言はこのファイルを正とする (router.js や画面に直接書かない)。
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
 * @returns {{chosen:Array, updatable:Array, manual:Array, sendable:Array}}
 */
export function summarizeRecord(rows = []) {
  const chosen = chooseRowsToRecord(rows);
  const updatable = chosen.filter((r) => !r.manual);
  const manual = chosen.filter((r) => r.manual);
  const sendable = updatable.filter((r) => r.evaluation?.canUpdate);
  return { chosen, updatable, manual, sendable };
}

/**
 * 記録の前に確認を出す理由。**送れる行が0なら必ず出す** (null = そのまま記録してよい)。
 *
 * ★2026-09-07 の事故は 'manual_only' だが、「⛔ の行だけを選んだ」履歴も同じく死んでいる。
 *   どちらも「記録はできたのに1件も送られない」ので、区別するのは**文言だけ**にする
 *   (Codex R1 高: manualOnly だけを見ると、⛔ だけの履歴が確認なしで作れてしまう)。
 *
 * @returns {'manual_only'|'all_blocked'|null}
 */
export function noSendableReasonOf(summary) {
  const { chosen = [], updatable = [], sendable = [] } = summary || {};
  if (chosen.length === 0) return null;      // 「記録する行が選ばれていません」で別に断る
  if (sendable.length > 0) return null;
  return updatable.length === 0 ? 'manual_only' : 'all_blocked';
}

/** 確認で出す文言 (画面もサーバもここを使う。ずれると現場が別の話だと思う) */
export const NO_SENDABLE_MESSAGES = {
  manual_only:
    '送れる行がありません。チェックを入れ忘れていませんか？'
    + '\n\nこのまま記録すると、手で直すモール (Amazon・LINEギフト) のチェックリストができるだけで、'
    + 'モールへは価格が1件も送られません。'
    + '\n価格を送りたいときは、送りたい行の左端のチェックを入れてください。',
  all_blocked:
    '送れる行がありません。選んだ行はすべて止まっています (⛔ か、新売価が未入力)。'
    + '\n\nこのまま記録しても、モールへは価格が1件も送られません。'
    + '\n「判定」の列に止まっている理由が出ているので、直してから記録してください。',
};

/** 確認せずに送ってきた時にサーバが返す文言 */
export function noSendableMessageOf(reason) {
  const base = NO_SENDABLE_MESSAGES[reason] || NO_SENDABLE_MESSAGES.manual_only;
  return `${base}\n記録だけ残すなら、確認のうえもう一度押してください。`;
}

/** モール名の一覧を日本語で (重複なし・並びは入力順) */
function mallNamesOf(ops) {
  return [...new Set(ops.map((o) => MALL_LABELS[o.mall] || o.mall))].join('・');
}

/**
 * 送信が終わった行の分類。
 * ★「previewed 以外 = 送信済み」とまとめない (Codex R1 高)。
 *   結果不明・価格違いで送らず は**送信済みではない**。まとめて「送信を終えています」と
 *   書くと、README の「不明は再送しない = モールの画面で実物を見る」という運用を誤らせる。
 * ★`failed` を「送っていない」に入れてはいけない (Codex R2 高)。failed には
 *   「miniPC が送る前に弾いた」(変わっていない) と「送った後の照合が通らなかった」
 *   (**変わっているかもしれない** = execute.js が mayHaveChanged を付ける) が混ざる。
 *   状態だけでは決められないので、独立させて「モールの画面で確かめて」と書く。
 * ★`noop` は「もともと同じ価格だった」= **書き込んでいない** (execute.js)。送信済みと混ぜない。
 */
const CONFIRMED_STATES = new Set(['confirmed']);             // 送って、変わったことも確かめた
const NOOP_STATES = new Set(['noop']);                       // もともと同じ価格 (書き込んでいない)
const UNCERTAIN_STATES = new Set(['executing', 'unknown']);  // 送ったかどうか分からない
const FAILED_STATES = new Set(['failed']);                   // 送る前 / 送った後、どちらの失敗か状態だけでは決まらない
const NOT_SENT_STATES = new Set(['conflict', 'blocked', 'skipped']);  // 送っていない (価格の食い違い・ガード・停止)

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
  const moved = ops.filter((o) => o.initial_state === 'previewed' && o.state !== 'previewed');
  const pick = (set) => moved.filter((o) => set.has(o.state));
  const confirmed = pick(CONFIRMED_STATES);
  const noop = pick(NOOP_STATES);
  const uncertain = pick(UNCERTAIN_STATES);
  const failedOps = pick(FAILED_STATES);
  const notSent = pick(NOT_SENT_STATES);
  const known = [CONFIRMED_STATES, NOOP_STATES, UNCERTAIN_STATES, FAILED_STATES, NOT_SENT_STATES];
  const other = moved.filter((o) => !known.some((set) => set.has(o.state)));

  if (manual.length === ops.length) {
    const names = mallNamesOf(manual);
    return `この履歴の ${ops.length} 行は、すべてこのツールから送れないモール (${names}) です`
      + `。価格は1件も送られていません — ${names} の価格は管理画面で直してください。`
      + 'このツールで送りたいときは、検索画面で送りたい行の左端のチェックを入れてから記録し直してください'
      + ' (チェックの無い行は記録されません)。';
  }

  const parts = [];
  if (confirmed.length > 0) parts.push(`${confirmed.length} 行は更新済み (送って、変わったことも確かめています)`);
  if (noop.length > 0) parts.push(`${noop.length} 行はもともと同じ価格でした (書き込んでいません)`);
  // ★ここは軽く書かない。「送ったかどうか分からない」行はモールの画面で実物を見るしかない
  if (uncertain.length > 0) {
    parts.push(`${uncertain.length} 行は送信中または結果が不明です`
      + ' (自動では送り直しません。モールの画面で実際の価格を確かめてください)');
  }
  if (failedOps.length > 0) {
    parts.push(`${failedOps.length} 行は失敗しています`
      + ' (送る前に弾かれた場合と、送った後の照合が通らなかった場合があります。'
      + '後者なら価格は変わっているので、モールの画面で実際の価格を確かめてください)');
  }
  if (notSent.length > 0) parts.push(`${notSent.length} 行は送られていません (価格の食い違い・ガード・途中で停止)`);
  if (other.length > 0) parts.push(`${other.length} 行は上のどれにも当てはまらない状態です (下の表で確かめてください)`);
  if (blocked.length > 0) parts.push(`${blocked.length} 行はガードで止まっています (「判定」の列に理由が出ています)`);
  if (manual.length > 0) parts.push(`${manual.length} 行は ${mallNamesOf(manual)} (このツールからは送れません)`);
  return parts.length > 0
    ? `送る行がありません — ${parts.join(' / ')}。`
    : '送る行がありません (記録時のガードを通った行だけが対象です)。';
}
