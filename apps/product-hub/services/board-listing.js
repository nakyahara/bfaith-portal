/**
 * 工程ボードから楽天に出品する (2026-09-01 中原さん要望:「カードを出品・展開に動かしたら
 * 自動で楽天に出品されて、結果がカードに出るとありがたい。画像も一緒に送る」)。
 *
 * やること = 詳細画面で人が順に押していた 2 ボタンを 1 本にまとめる:
 *   ① 画像を Drive → R-Cabinet へ転送 (transferImagesToCabinet。転送済みは飛ばすので冪等)
 *   ② 公開状態で登録 (registerItem。前提チェックで止まれば reasons が返る)
 *   ③ 成功したら 楽天モール=完了 + 画像工程⑧「楽天登録」=完了 (詳細画面の登録ボタンと同じ後処理)
 *
 * 失敗はどの段階でも draft_rakuten.last_error に残す — ボードのカードはこれを読んで
 * 「❌ 出品できませんでした」を出す (詳細画面を開かなくても理由が分かる)。
 * registerItem は RMS のエラーしか last_error に書かないので、前提チェックの理由 (ジャンル未入力等)
 * と転送の失敗はここで書く。
 *
 * 楽天への PUT は取り消せないので、二重実行は 2 段で防ぐ: 登録済み (registered_at) なら拒否 /
 * 同じ商品の実行中は 409。
 */
import { getDB, logEvent } from '../db.js';
import { transferImagesToCabinet, registerItem, rakutenItemPageUrl } from './rakuten-listing.js';
import { markRakutenListed } from '../lib/mall-status.js';
import { setStepState } from '../lib/workflow-progress.js';

/** 実行中の draft_id (プロセス内)。Render は 1 プロセスなのでこれで足りる */
const inFlight = new Set();

function badRequest(message, extra = {}) {
  const e = new Error(message);
  e.status = 400;
  Object.assign(e, extra);
  return e;
}

/**
 * 楽天登録が成功したあとの共通後処理 (詳細画面の「公開で登録」とボードからの出品で同じ)。
 * どちらも fail-soft: 出品は成功しているので、ここで失敗しても結果は成功として返す
 */
export function afterRakutenRegistered(db, draft, actor) {
  markRakutenListed(db, draft.id, { itemUrl: rakutenItemPageUrl(draft.ne_code), actor });
  // 画像工程 v2 ⑧「楽天登録」も自動で完了
  try {
    const st = db.prepare("SELECT state FROM draft_step_progress WHERE draft_id = ? AND step_code = 'imgd_rakuten'").get(draft.id);
    if (st && st.state !== 'done' && st.state !== 'skip') {
      setStepState(draft.id, 'imgd_rakuten', { state: 'done' }, actor, { isAdmin: true, systemActor: true });
    }
  } catch (e) {
    console.warn('[product-hub] imgd_rakuten auto-done failed:', e?.message || e);
  }
}

/** ボードのカードに出す失敗理由を残す (registerItem の成功時に NULL へ戻る) */
function rememberFailure(db, draftId, message, actor) {
  db.prepare(`
    INSERT INTO draft_rakuten (draft_id, last_error) VALUES (?, ?)
    ON CONFLICT(draft_id) DO UPDATE SET
      last_error = excluded.last_error,
      updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
  `).run(draftId, String(message).slice(0, 1500));
  logEvent(db, draftId, 'rakuten_board_listing_failed', String(message).slice(0, 500), actor);
}

/**
 * @param {number} draftId
 * @param {{actor?: string|null, deps?: {transfer?: Function, register?: Function}}} opts
 *   deps はテスト用の差し替え口 (本番は既定のまま)
 * @returns {Promise<{ok: boolean, stage: 'transfer'|'register'|'done', transfer: object|null, register: object|null, error?: string}>}
 */
export async function listToRakutenFromBoard(draftId, { actor = null, deps = {} } = {}) {
  const db = getDB();
  const id = Number(draftId);
  const draft = db.prepare('SELECT id, ne_code, name FROM product_drafts WHERE id = ?').get(id);
  if (!draft) throw badRequest('商品が見つかりません');
  const rk = db.prepare('SELECT registered_at FROM draft_rakuten WHERE draft_id = ?').get(id);
  if (rk?.registered_at) {
    throw badRequest('この商品は楽天に登録済みです (公開/非公開の切り替えは詳細画面から)');
  }
  if (inFlight.has(id)) {
    const e = new Error('この商品は楽天に出品中です。終わるまで待ってください');
    e.status = 409;
    throw e;
  }
  const transfer = deps.transfer || transferImagesToCabinet;
  const register = deps.register || registerItem;

  inFlight.add(id);
  try {
    logEvent(db, id, 'rakuten_board_listing_started', null, actor);

    // ① 画像転送。転送済みは 'already' で飛ぶので、何度実行しても余計なアップロードは起きない
    let tr;
    try {
      tr = await transfer(id, { actor });
    } catch (e) {
      const msg = `画像の転送でエラー: ${String(e?.message || e).slice(0, 300)}`;
      rememberFailure(db, id, msg, actor);
      return { ok: false, stage: 'transfer', transfer: null, register: null, error: msg };
    }
    if (tr.error === 'no_images') {
      const msg = '商品画像がありません (画像タブでフォルダを取り込んでから出品してください)';
      rememberFailure(db, id, msg, actor);
      return { ok: false, stage: 'transfer', transfer: tr, register: null, error: msg };
    }
    const transferSummary = {
      uploaded: tr.uploaded || 0,
      already: (tr.results || []).filter((r) => r.outcome === 'already').length,
      failed: tr.failed || 0,
      errors: (tr.results || []).filter((r) => r.outcome === 'failed').map((r) => r.error || '').filter(Boolean),
    };
    if (transferSummary.failed > 0) {
      // 未転送のまま登録しても registerItem の前提チェックで止まる。理由を転送側の言葉で残す
      const msg = `画像 ${transferSummary.failed} 枚を R-Cabinet に転送できませんでした`
        + (transferSummary.errors[0] ? ` (${transferSummary.errors[0].slice(0, 200)})` : '')
        + '。Drive の画像フォルダがサービスアカウントに共有されているか確認してください';
      rememberFailure(db, id, msg, actor);
      return { ok: false, stage: 'transfer', transfer: transferSummary, register: null, error: msg };
    }

    // ② 登録 (前提チェック → RMS PUT)。RMS のエラーは registerItem が last_error に書く
    let reg;
    try {
      reg = await register(id, { actor });
    } catch (e) {
      const msg = `楽天への登録でエラー: ${String(e?.message || e).slice(0, 300)}`;
      rememberFailure(db, id, msg, actor);
      return { ok: false, stage: 'register', transfer: transferSummary, register: null, error: msg };
    }
    if (!reg.ok) {
      const msg = reg.error || (reg.reasons || []).join(' / ') || '楽天への登録に失敗しました';
      // 前提チェック (reasons) は registerItem が記録しないのでここで残す。RMS エラーは記録済み
      if (reg.reasons) rememberFailure(db, id, `出品の前提が揃っていません: ${msg}`, actor);
      else logEvent(db, id, 'rakuten_board_listing_failed', String(msg).slice(0, 500), actor);
      return { ok: false, stage: 'register', transfer: transferSummary, register: reg, error: msg };
    }

    // ③ 後処理 (モール=完了・画像工程⑧=完了)
    afterRakutenRegistered(db, draft, actor);
    logEvent(db, id, 'rakuten_board_listing_done', reg.manageNumber || null, actor);
    return { ok: true, stage: 'done', transfer: transferSummary, register: reg };
  } finally {
    inFlight.delete(id);
  }
}

/** テスト用: 実行中フラグを覗く */
export function _isListingInFlight(draftId) {
  return inFlight.has(Number(draftId));
}
