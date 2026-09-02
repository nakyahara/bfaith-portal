/**
 * 工程ボードから楽天に出品する (2026-09-01 中原さん要望:「カードを出品・展開に動かしたら
 * 自動で楽天に出品されて、結果がカードに出るとありがたい。画像も一緒に送る」)。
 *
 * やること = 詳細画面で人が順に押していた 2 ボタンを 1 本にまとめる:
 *   ① 画像を Drive → R-Cabinet へ転送 (transferImagesToCabinet。転送済みは飛ばすので冪等)
 *   ② 公開状態で登録 (registerItem。前提チェックで止まれば reasons が返る)
 *   ③ 成功したら 楽天モール=完了 + 画像工程⑧「楽天登録」=完了 (詳細画面の登録ボタンと同じ後処理)
 *
 * 楽天への PUT は取り消せない。そのための守り (Codex R1 critical ×2 / high ×2):
 *   - 二重実行のロックは**詳細画面の「公開で登録」と共有** (acquireRakutenListingLock)。
 *     ボードで実行中に別タブの詳細画面から押しても 409
 *   - registerItem が「PUT の結果が確認できない」(RMS_OUTCOME_UNKNOWN) を投げたら、失敗ではなく
 *     **outcome=unknown** で止める。実は登録が通っている可能性があるので、カードに「やり直す」を出さず、
 *     人が RMS で確認したうえで管理者だけが再実行 (forceUnknown) できる
 *   - サーバー側でも「本流の工程が出品・展開まで来ている / 保留・除外でない / 登録済みでない」を見る。
 *     confirm:true は誤操作防止の印であって、認可ではない
 *   - 後処理 (モール=完了・工程⑧=完了) の失敗は成功に紛れさせず、戻り値と履歴に残す
 *
 * 試行の状態は draft_rakuten.listing_outcome / listing_attempt_at / last_error に残す。
 * ボードのカードはこれを読んで 実行中 / 失敗 (理由) / 結果不明 を出す。
 * registerItem は RMS のエラーしか last_error に書かないので、前提チェックの理由と転送の失敗はここで書く。
 */
import { getDB, logEvent } from '../db.js';
import { transferImagesToCabinet, registerItem, rakutenItemPageUrl } from './rakuten-listing.js';
import { markRakutenListed } from '../lib/mall-status.js';
import { setStepState } from '../lib/workflow-progress.js';

/** 楽天への登録を実行中の draft_id → 開始時刻 (プロセス内。Render は 1 プロセスなのでこれで足りる) */
const inFlight = new Map();

function httpError(status, message) {
  const e = new Error(message);
  e.status = status;
  return e;
}

/**
 * 楽天への登録 (PUT) を 1 商品 1 本に直列化するロック。ボードからの出品と詳細画面の
 * 「公開で登録」の**両方**がこれを通る (片方だけだと、もう片方から同じ PUT が走る)。
 * @returns {() => void} 解放関数 (finally で必ず呼ぶ。二度呼んでも安全)
 * @throws 409 実行中
 */
export function acquireRakutenListingLock(draftId) {
  const id = Number(draftId);
  if (inFlight.has(id)) throw httpError(409, 'この商品は楽天に出品中です。終わるまで待ってください');
  inFlight.set(id, Date.now());
  let released = false;
  return () => {
    if (released) return;
    released = true;
    inFlight.delete(id);
  };
}

export function isRakutenListingInFlight(draftId) {
  return inFlight.has(Number(draftId));
}

/**
 * 楽天登録が成功したあとの共通後処理 (詳細画面の「公開で登録」とボードからの出品で同じ)。
 * fail-soft: 出品は成功しているので、ここで失敗しても throw しない。ただし**黙らない** —
 * 戻り値で返し、履歴にも残す (Codex R1 high: モール状況が未更新だと登録済みなのにカードに
 * 「出品」ボタンが出続ける。カード側は registered_at を主判定にして、ここでの失敗は警告として出す)
 * @returns {{mallOk: boolean, stepOk: boolean}}
 */
export function afterRakutenRegistered(db, draft, actor) {
  const mallOk = markRakutenListed(db, draft.id, { itemUrl: rakutenItemPageUrl(draft.ne_code), actor }) === true;
  // 画像工程 v2 ⑧「楽天登録」も自動で完了 (対象外 skip はそのまま)
  let stepOk = true;
  try {
    const st = db.prepare("SELECT state FROM draft_step_progress WHERE draft_id = ? AND step_code = 'imgd_rakuten'").get(draft.id);
    if (st && st.state !== 'done' && st.state !== 'skip') {
      setStepState(draft.id, 'imgd_rakuten', { state: 'done' }, actor, { isAdmin: true, systemActor: true });
    }
  } catch (e) {
    stepOk = false;
    console.warn('[product-hub] imgd_rakuten auto-done failed:', e?.message || e);
  }
  if (!mallOk || !stepOk) {
    logEvent(db, draft.id, 'rakuten_postprocess_failed',
      `楽天登録は成功。後処理: モール状況=${mallOk ? 'ok' : '失敗'} / 画像工程⑧=${stepOk ? 'ok' : '失敗'}`, actor);
  }
  return { mallOk, stepOk };
}

/**
 * 試行の状態を残す。outcome=running で始め、失敗なら failed/unknown、成功なら NULL に戻す。
 * last_error は「今回の試行」のものだけを見せたいので、開始時に消す (keepError=true は
 * registerItem が RMS の原文を書いた直後に呼ぶとき — 上書きして原文を失わない)
 */
function setAttempt(db, draftId, { outcome, error = null, start = false, keepError = false }) {
  db.prepare(`
    INSERT INTO draft_rakuten (draft_id, listing_outcome, listing_attempt_at, last_error)
    VALUES (@id, @outcome, @at, @error)
    ON CONFLICT(draft_id) DO UPDATE SET
      listing_outcome = excluded.listing_outcome,
      listing_attempt_at = CASE WHEN @start = 1 THEN excluded.listing_attempt_at ELSE draft_rakuten.listing_attempt_at END,
      last_error = CASE WHEN @keep = 1 THEN draft_rakuten.last_error ELSE excluded.last_error END,
      updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
  `).run({
    id: Number(draftId), outcome, at: start ? new Date().toISOString() : null,
    error: error == null ? null : String(error).slice(0, 1500), start: start ? 1 : 0, keep: keepError ? 1 : 0,
  });
}

/**
 * 楽天に登録してよい状態か — **ボードと詳細画面の両経路で同じ判定** (Codex R2 critical:
 * 片方だけだと、もう片方から「結果不明」の商品を誰でも出し直せてしまう)。
 *   - 登録済み → 400
 *   - 前回の結果が unknown → 400 (forceUnknown = 管理者が RMS で未登録を確認済み、のときだけ通す)
 *   - running のまま実行中でない = 途中で落ちた。PUT が通った直後・registered_at を書く前に落ちた
 *     可能性があるので unknown と同じ扱い (「15 分経ったからやり直せる」にしない)
 * 実行中 (ロック中) は呼び出し側のロック取得で 409 になる。
 * 🚨**ロックを取る前に呼ぶこと** (Codex R3): 取った後だと inFlight が自分自身になり、途中で止まった
 * running を「いま動いている」と誤認して通してしまう
 * @throws 400
 */
export function assertRakutenListable(db, draft, { forceUnknown = false } = {}) {
  const id = Number(draft.id);
  const rk = db.prepare('SELECT registered_at, listing_outcome FROM draft_rakuten WHERE draft_id = ?').get(id);
  if (rk?.registered_at) {
    throw httpError(400, 'この商品は楽天に登録済みです (公開/非公開の切り替えは詳細画面から)');
  }
  const stuck = rk?.listing_outcome === 'unknown' || (rk?.listing_outcome === 'running' && !inFlight.has(id));
  if (stuck && !forceUnknown) {
    const head = rk.listing_outcome === 'running' ? '前回の出品処理が途中で止まっています' : '前回の登録結果が確認できていません';
    throw httpError(400, `${head}。RMS で商品管理番号「${String(draft.ne_code || '').toLowerCase()}」の有無を確認してください。`
      + '登録されていれば詳細画面の「モール別の展開状況」で楽天を完了に、無ければ管理者がボードの「確認済み → 再実行」で出し直せます');
  }
}

/**
 * registerItem が「PUT の結果が確認できない」(RMS_OUTCOME_UNKNOWN) を投げたときの記録 (両経路共通)。
 * 以後 assertRakutenListable が止めるので、人が RMS で確認するまで誰も再実行できない
 */
export function rememberUnknownOutcome(db, draftId, message, actor) {
  const msg = String(message || '').slice(0, 1200);
  setAttempt(db, draftId, { outcome: 'unknown', error: msg });
  logEvent(db, draftId, 'rakuten_listing_outcome_unknown', msg.slice(0, 480), actor);
}

/** 本流の「いま進める番」の工程 (全部決着なら null) */
function mainCurrentStep(db, draftId) {
  return db.prepare(`
    SELECT p.step_code, s.label FROM draft_step_progress p
    JOIN ph_steps s ON s.code = p.step_code AND s.active = 1
    WHERE p.draft_id = ? AND s.track = 'main' AND p.state NOT IN ('done', 'skip')
    ORDER BY s.sort, s.code LIMIT 1
  `).get(Number(draftId)) || null;
}

/**
 * @param {number} draftId
 * @param {{actor?: string|null, forceUnknown?: boolean, deps?: {transfer?: Function, register?: Function}}} opts
 *   forceUnknown = 前回 outcome=unknown の商品を、人が RMS で「登録されていない」と確認したうえで再実行する
 *   (router が管理者だけに許す)。deps はテスト用の差し替え口 (本番は既定のまま)
 * @returns {Promise<{ok: boolean, stage: 'transfer'|'register'|'done', outcome: 'failed'|'unknown'|null,
 *   retryable: boolean, transfer: object|null, register: object|null, postProcess?: object, error?: string}>}
 * @throws 400 実行できない状態 / 409 実行中
 */
export async function listToRakutenFromBoard(draftId, { actor = null, forceUnknown = false, deps = {} } = {}) {
  const db = getDB();
  const id = Number(draftId);
  const draft = db.prepare('SELECT id, ne_code, name, status FROM product_drafts WHERE id = ?').get(id);
  if (!draft) throw httpError(400, '商品が見つかりません');
  if (draft.status === 'on_hold' || draft.status === 'excluded') {
    throw httpError(400, '保留・除外中の商品は出品できません (詳細画面で再開してから)');
  }
  assertRakutenListable(db, draft, { forceUnknown });
  // ボードの意味を守る: 本流が「出品・展開」の番の商品だけ (URL 直叩きで工程を飛ばさせない)。
  // 全工程が決着している (= 楽天は完了か対象外で決着済み) 商品も通さない (Codex R2 high)
  const cur = mainCurrentStep(db, id);
  if (!cur) {
    throw httpError(400, '本流の工程「出品・展開」は完了しています。楽天を出し直すなら詳細画面の「モール別の展開状況」で楽天を未着手に戻してから');
  }
  if (cur.step_code !== 'listing') {
    throw httpError(400, `工程がまだ「出品・展開」まで進んでいません (いま: ${cur.label})。先に工程を進めてください`);
  }

  const release = acquireRakutenListingLock(id);
  const transfer = deps.transfer || transferImagesToCabinet;
  const register = deps.register || registerItem;
  const fail = (stage, message, { transfer: tr = null, register: reg = null, keepError = false } = {}) => {
    setAttempt(db, id, { outcome: 'failed', error: message, keepError });
    logEvent(db, id, 'rakuten_board_listing_failed', `${stage}: ${String(message).slice(0, 480)}`, actor);
    return { ok: false, stage, outcome: 'failed', retryable: true, transfer: tr, register: reg, error: message };
  };
  try {
    setAttempt(db, id, { outcome: 'running', start: true });
    logEvent(db, id, 'rakuten_board_listing_started', forceUnknown ? '結果不明を人が確認したうえで再実行' : null, actor);

    // ① 画像転送。転送済みは 'already' で飛ぶので、何度実行しても余計なアップロードは起きない
    let tr;
    try {
      tr = await transfer(id, { actor });
    } catch (e) {
      return fail('transfer', `画像の転送でエラー: ${String(e?.message || e).slice(0, 300)}`);
    }
    if (tr.error === 'no_images') {
      return fail('transfer', '商品画像がありません (画像タブでフォルダを取り込んでから出品してください)', { transfer: tr });
    }
    const transferSummary = {
      uploaded: tr.uploaded || 0,
      already: (tr.results || []).filter((r) => r.outcome === 'already').length,
      failed: tr.failed || 0,
      errors: (tr.results || []).filter((r) => r.outcome === 'failed').map((r) => r.error || '').filter(Boolean),
    };
    if (transferSummary.failed > 0) {
      // 未転送のまま登録しても registerItem の前提チェックで止まる。理由を転送側の言葉で残す
      return fail('transfer', `画像 ${transferSummary.failed} 枚を R-Cabinet に転送できませんでした`
        + (transferSummary.errors[0] ? ` (${transferSummary.errors[0].slice(0, 200)})` : '')
        + '。Drive の画像フォルダがサービスアカウントに共有されているか確認してください', { transfer: transferSummary });
    }

    // ② 登録 (前提チェック → RMS PUT)
    let reg;
    try {
      reg = await register(id, { actor });
    } catch (e) {
      if (e?.code === 'RMS_OUTCOME_UNKNOWN') {
        // 🚨 PUT が通ったかどうか分からない。失敗にすると「やり直す」で二重登録になるので、
        // unknown で止めて人の確認を待つ (registerItem の原文をそのまま残す)
        const msg = String(e.message || e).slice(0, 1200);
        rememberUnknownOutcome(db, id, msg, actor);
        return { ok: false, stage: 'register', outcome: 'unknown', retryable: false, transfer: transferSummary, register: null, error: msg };
      }
      return fail('register', `楽天への登録でエラー: ${String(e?.message || e).slice(0, 300)}`, { transfer: transferSummary });
    }
    if (!reg.ok) {
      if (reg.reasons) {
        // 前提チェック (registerItem は reasons を last_error に書かない → ここで残す)
        return fail('register', `出品の前提が揃っていません: ${reg.reasons.join(' / ')}`, { transfer: transferSummary, register: reg });
      }
      // RMS エラー: 原文は registerItem が last_error に書いた。上書きせず outcome だけ付ける
      return fail('register', reg.error || '楽天への登録に失敗しました', { transfer: transferSummary, register: reg, keepError: true });
    }

    // ③ 後処理 (モール=完了・画像工程⑧=完了)。失敗は戻り値で返す (成功に紛れさせない)
    const postProcess = afterRakutenRegistered(db, draft, actor);
    setAttempt(db, id, { outcome: null, keepError: true }); // last_error は registerItem が NULL にしている
    logEvent(db, id, 'rakuten_board_listing_done',
      `${reg.manageNumber || ''}${postProcess.mallOk && postProcess.stepOk ? '' : ' (後処理に失敗あり)'}`, actor);
    return { ok: true, stage: 'done', outcome: null, retryable: false, transfer: transferSummary, register: reg, postProcess };
  } finally {
    release();
  }
}
