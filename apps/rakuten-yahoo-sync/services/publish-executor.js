/**
 * Phase E-5a: 実 publish executor (capacity-only、 vps-proxy 拡張前提なので Yahoo API 呼び出しは E-5b)。
 *
 * 本ファイルが担うのは:
 *   1. RYS_PUBLISH_ENABLED の dual check (router 受理時 + executor 直前)
 *   2. idempotency_key 生成 + dedupe + transition
 *   3. publish-pipeline.evaluateItemForPublish (dry-run=false) を呼んで readiness 通る商品だけ進める
 *   4. Yahoo API 呼び出し本体は 501 Not Implemented placeholder (E-5b で繋ぐ)
 *   5. 結果を publish_idempotency に persist + audit_log
 *
 * 設計原則 (Codex Phase E R3-R4 確定):
 *   - publish-pipeline と完全に分離 (pipeline は dry-run / executor は実 publish)
 *   - in_progress 状態の lease は idempotency table 自体が担う (UNIQUE PK 違反で検知)
 *   - 既存 success/failed は dedupe で前回結果を返す (HTTP 200 + dedupe: true)
 *   - 既存 in_progress は busy として 409 で返す (lease 喪失と同義)
 */

import crypto from 'crypto';
import { evaluateItemForPublish } from './publish-pipeline.js';
import { callEditItem, YahooProxyError } from '../lib/yahoo-publish-proxy.js';
import { uploadRakutenImagesToYahoo } from '../lib/image-uploader.js';
import { validateYahooEditItemFields, YahooFieldValidationError } from '../lib/yahoo-edititem-validator.js';
import { extractRakutenImageUrls } from '../lib/yahoo-image-extract.js';
import { rewriteAndSanitizeYahooHtml, YahooHtmlRewriteError } from '../lib/yahoo-html-image-rewriter.js';

const ALLOWED_STATUSES = new Set(['in_progress', 'success', 'failed', 'not_implemented']);

// Codex E-5a R1 M-2 + E-5b R1 H-4: in_progress lease の TTL。
//   実 publish の典型処理: filter → download (10 枚 × 20s timeout) + JPEG 変換 (sharp 数秒)
//   + uploadItemImage (10 枚 × 60s timeout) + editItem (60s timeout) = 最悪 ~15 分。
//   TTL を 30 分に設定して、 long-running publish 中の race を最小化。
//   E-5b R1 H-4: 10 分は短すぎ画像多 publish で stale steal による重複 Yahoo 副作用リスクあり。
const LEASE_TTL_MS = 30 * 60 * 1000; // 30 分

// Codex E-5a R1 M-1: 'not_implemented' は terminal だが dedupe 対象外。
//   E-5b で同 key で再投げると実 publish に進めるようにする (placeholder を再実行可能)。
// 2026-06-25 中原さん smoke で発覚: status='failed' を terminal 扱いすると同日中の retry が
// 全部 dedupe され、 中間バグ修正後の再実行ができない。 'success' のみ terminal とする。
// 'failed' / 'not_implemented' は retryable で、 既存 row を UPDATE in_progress して再実行する。
const DEDUPE_TERMINAL_STATUSES = new Set(['success']);
const RETRYABLE_STATUSES = new Set(['not_implemented', 'failed']);

export function isPublishEnabled() {
  return String(process.env.RYS_PUBLISH_ENABLED || '0').trim() === '1';
}

// Codex Phase E image-html R2 H-3: publish logic 改訂時に古い success record と
// 同じ key にならないよう version を必ず混ぜる。
// v2: PR #355 image-html rewrite (uploadItemImage で desc も) 採用版
// v3: PR #356 uploadLibImage 採用 (Yahoo it-14061 修正) 後の版
// v4: R16 楽天リンク根絶 (2026-07-04)。 v3 以前に publish した商品は楽天リンクが残っている
//     可能性があるため、 同日でも dedupe されず再 publish で上書き修正できるようにする。
export const IDEMPOTENCY_VERSION = 'rewrite_v4';

/**
 * idempotency_key 生成 (caller が override 可能、 ない場合は deterministic に組み立て)。
 *   key = sha256(IDEMPOTENCY_VERSION + item_code + manage_number + isoDate(scope) + extraSalt)
 *
 * Codex Phase E image-html R2 H-3: IDEMPOTENCY_VERSION を seed に含めることで、 logic 変更後の
 * 再 publish が古い success record に dedupe で吸収されない。
 */
export function buildIdempotencyKey({ itemCode, manageNumber, scope = 'day', extraSalt = '', isoDate = null }) {
  if (!itemCode) throw new Error('buildIdempotencyKey: itemCode required');
  const date = isoDate || new Date().toISOString().slice(0, 10);
  const seed = [IDEMPOTENCY_VERSION, itemCode, manageNumber || '', scope, date, extraSalt].join('|');
  return crypto.createHash('sha256').update(seed, 'utf8').digest('hex');
}

function getRow(db, idempotencyKey) {
  return db.prepare(`SELECT * FROM publish_idempotency WHERE idempotency_key = ?`).get(idempotencyKey);
}

function leaseExpiresAt() {
  return new Date(Date.now() + LEASE_TTL_MS).toISOString();
}

function insertInProgress(db, { idempotencyKey, itemCode, manageNumber, createdBy }) {
  try {
    db.prepare(`
      INSERT INTO publish_idempotency (idempotency_key, item_code, manage_number, status, created_by, lease_expires_at)
      VALUES (?, ?, ?, 'in_progress', ?, ?)
    `).run(idempotencyKey, itemCode, manageNumber, createdBy, leaseExpiresAt());
    return true;
  } catch (e) {
    if (/UNIQUE constraint failed/i.test(e.message)) return false;
    throw e;
  }
}

/**
 * Codex E-5a R1 M-2: 期限切れ in_progress を奪取する。
 *   - 既存行が in_progress かつ lease_expires_at < now なら attempt++ + lease 更新で再取得
 *   - 戻り値 true: 奪取成功 (caller は処理続行可)
 *           false: 既存行が in_progress だが lease 有効 → 真の同時実行
 *                  もしくは terminal status (caller 側で dedupe 判定)
 */
function tryStealStaleLease(db, idempotencyKey, createdBy) {
  const now = new Date().toISOString();
  // Codex E-5a R2 M-2: lease_expires_at IS NULL も stale 扱い
  const r = db.prepare(`
    UPDATE publish_idempotency
       SET attempt          = attempt + 1,
           lease_expires_at = ?,
           created_by       = ?,
           updated_at       = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
           error_message    = COALESCE(error_message, '') || ' [stale lease stolen at ' || ? || ']'
     WHERE idempotency_key  = ?
       AND status           = 'in_progress'
       AND (lease_expires_at IS NULL OR lease_expires_at < ?)
  `).run(leaseExpiresAt(), createdBy, now, idempotencyKey, now);
  return r.changes > 0;
}

/**
 * Codex E-5a R2 M-3: lease owner check (CAS) で finalize する。
 *   - 自分が取得した時の attempt を ownAttempt として渡す
 *   - WHERE で attempt = ? を追加し、 stale steal で attempt++ された旧 caller の finalize は弾く
 *   - changes = 0 なら caller が「自分のものではない」 ことを認識できる
 *
 * @returns {boolean} true: finalize 成功 (自分が owner だった) / false: 他 caller に奪取されている
 */
function finalize(db, { idempotencyKey, status, resultJson = null, errorMessage = null, ownAttempt = null }) {
  if (!ALLOWED_STATUSES.has(status)) throw new Error(`finalize: invalid status ${status}`);
  let sql, params;
  if (ownAttempt !== null) {
    sql = `
      UPDATE publish_idempotency
         SET status          = ?,
             result_json     = ?,
             error_message   = ?,
             lease_expires_at = NULL,
             completed_at    = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
             updated_at      = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
       WHERE idempotency_key = ? AND attempt = ?
    `;
    params = [status, resultJson, errorMessage, idempotencyKey, ownAttempt];
  } else {
    sql = `
      UPDATE publish_idempotency
         SET status          = ?,
             result_json     = ?,
             error_message   = ?,
             lease_expires_at = NULL,
             completed_at    = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
             updated_at      = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
       WHERE idempotency_key = ?
    `;
    params = [status, resultJson, errorMessage, idempotencyKey];
  }
  const r = db.prepare(sql).run(...params);
  return r.changes > 0;
}

function dedupeResponse(existing, idempotencyKey) {
  return {
    status: 'dedupe',
    idempotencyKey,
    dedupe: true,
    previousStatus: existing.status,
    executorResult: existing.result_json ? JSON.parse(existing.result_json) : null,
    previousError: existing.error_message,
    previousAttempt: existing.attempt,
  };
}

/**
 * 実 publish 本体 (Phase E-5b):
 *   1. 楽天画像を Yahoo に upload (filter + JPEG + 順次)
 *   2. editItem を form-encoded で叩いて商品登録 (fields.display=1 で公開)
 *   失敗時は throw (caller が CAS finalize で 'failed' に倒す)
 */
async function performRealPublish({ rakutenImages, fields, itemCode, customPatterns = [], rakutenItem = null }) {
  if (!fields) throw new Error('performRealPublish: fields required');

  // 0. editItem preflight 仮版 (Codex Phase E editItem R2): 画像 upload より前に
  //    Yahoo 仕様 (explanation 500字、 caption 5000字、 item_code 形式等) を fail-closed で検証。
  try {
    validateYahooEditItemFields(fields);
  } catch (e) {
    if (e instanceof YahooFieldValidationError) {
      throw new Error(`editItem_preflight_failed: ${e.message}`);
    }
    throw e;
  }

  // 1. 説明文画像 URL 抽出 (PC/SP)
  //    Codex Phase E image-html R2 medium: PC/SP まとめて + dedupe suffix 一貫割当
  const salesDescriptionHtml = rakutenItem?.salesDescription || '';
  const spDescriptionHtml = rakutenItem?.productDescription?.sp || '';
  const descUrlsRaw = [
    ...extractRakutenImageUrls(salesDescriptionHtml),
    ...extractRakutenImageUrls(spDescriptionHtml),
  ];
  const descUrlsDeduped = [...new Set(descUrlsRaw)]; // 順序保持で dedupe

  // 2. 画像 upload (main + desc 統合、 fail-closed)
  const imageResult = await uploadRakutenImagesToYahoo({
    rakutenImages: rakutenImages || [],
    itemCode,
    customPatterns,
    descriptionImageUrls: descUrlsDeduped,
  });

  // 3. additional1 / sp_additional を 楽天 URL → Yahoo CDN URL に rewrite + sanitize
  //    Codex Phase E image-html R2: immutable な rewrittenFields を新規 construct
  let rewrittenAdditional1, rewrittenSpAdditional;
  try {
    // Yahoo it-14061 修正 (2026-06-27): additional1/sp_additional の <img> は
    // 「追加画像」 (uploadLibImage) のみ許可。 商品画像 (mainUrlMap) は it-14061 で拒否される。
    rewrittenAdditional1 = rewriteAndSanitizeYahooHtml(salesDescriptionHtml, imageResult.libUrlMap, {
      allowedYahooHosts: imageResult.allowedYahooLibHosts,
      excludedRakutenUrls: imageResult.excludedRakutenUrls,
    });
    rewrittenSpAdditional = rewriteAndSanitizeYahooHtml(spDescriptionHtml, imageResult.libUrlMap, {
      allowedYahooHosts: imageResult.allowedYahooLibHosts,
      excludedRakutenUrls: imageResult.excludedRakutenUrls,
    });
  } catch (e) {
    if (e instanceof YahooHtmlRewriteError) {
      throw new Error(`image_html_rewrite_failed: ${e.message}`);
    }
    throw e;
  }

  const rewrittenFields = {
    ...fields,
    additional1: rewrittenAdditional1,
    sp_additional: rewrittenSpAdditional,
  };

  // 4. editItem preflight 確定 (rewrite 後の長さ + img host check)
  try {
    validateYahooEditItemFields(rewrittenFields, { allowedImgHosts: imageResult.allowedYahooLibHosts });
  } catch (e) {
    if (e instanceof YahooFieldValidationError) {
      throw new Error(`editItem_preflight_failed_post_rewrite: ${e.message}`);
    }
    throw e;
  }

  // 5. editItem (form-encoded) — rewrittenFields を送信
  const editResult = await callEditItem(rewrittenFields);
  return {
    images: {
      uploaded: imageResult.uploaded,
      mainCount: imageResult.mainUrlMap?.size || 0,
      libCount: imageResult.libUrlMap?.size || 0,
      yahooLibHosts: imageResult.allowedYahooLibHosts,
    },
    editItem: { status: editResult.status, bodyPreview: String(editResult.body).slice(0, 300) },
    fields: rewrittenFields,
  };
}

function audit(db, action, detail, { actor = 'http', result = 'success', errorMessage = null } = {}) {
  try {
    db.prepare(`
      INSERT INTO audit_log (actor, action, after_json, result, error_message)
      VALUES (?, ?, ?, ?, ?)
    `).run(actor, action, detail ? JSON.stringify(detail) : null, result, errorMessage);
  } catch (_) { /* best-effort */ }
}

/**
 * 実 publish エントリポイント。
 *
 * @param {object} opts
 * @param {Database} opts.db
 * @param {string} opts.itemCode
 * @param {string} [opts.manageNumber]
 * @param {string} [opts.createdBy='http']
 * @param {string} [opts.idempotencyKey]   省略時 buildIdempotencyKey() で生成
 * @param {object} [opts.publishOpts]      evaluateItemForPublish に渡す extras
 * @returns {Promise<{status, idempotencyKey, dedupe?, fields?, reasons?, executorResult?, error?}>}
 */
export async function executePublish({
  db, itemCode, manageNumber, createdBy = 'http',
  idempotencyKey: providedKey = null,
  publishOpts = {},
}) {
  if (!db) throw new Error('executePublish: db required');
  if (!itemCode) throw new Error('executePublish: itemCode required');

  // RYS_PUBLISH_ENABLED 二重 check (Codex E R2 推奨): executor 直前でも env を読む
  // (router 側でも check するが、 真の race を防ぐ最終 gate)
  if (!isPublishEnabled()) {
    audit(db, 'publish_execute_blocked_flag', { itemCode }, { result: 'failed', errorMessage: 'RYS_PUBLISH_ENABLED=0' });
    return {
      status: 'flag_off',
      idempotencyKey: providedKey,
      error: 'RYS_PUBLISH_ENABLED is 0 (kill-switch). Real publish is disabled.',
    };
  }

  const idempotencyKey = providedKey || buildIdempotencyKey({ itemCode, manageNumber });
  // Codex E-5a R3 H-1: lease 取得直後に attempt を固定する。 これ以降 stale steal で attempt が
  //   bump されても finalize は弾ける。 nullable で track して、 lease を握れなかった経路は CAS なしで戻る。
  let ownAttempt = null;

  // dedupe check
  //   Codex E-5a R1 M-1: 'not_implemented' は dedupe 対象外 (E-5b で再実行できる placeholder 扱い)
  //   Codex E-5a R1 M-2: in_progress の lease 期限切れなら stale lease 奪取
  const existing = getRow(db, idempotencyKey);
  if (existing) {
    if (existing.status === 'in_progress') {
      const now = new Date().toISOString();
      // Codex E-5a R2 M-2: lease_expires_at IS NULL は stale 扱い (旧 row や手動投入 row 救済)
      const stillAlive = existing.lease_expires_at && existing.lease_expires_at >= now;
      if (stillAlive) {
        return {
          status: 'in_progress_conflict',
          idempotencyKey,
          error: 'Another publish for this key is in progress (lease held).',
          leaseExpiresAt: existing.lease_expires_at,
        };
      }
      // stale lease を奪取して続行
      if (!tryStealStaleLease(db, idempotencyKey, createdBy)) {
        // 奪取失敗 (別 caller が同時に処理) → conflict 扱い
        const concurrent = getRow(db, idempotencyKey);
        if (concurrent && DEDUPE_TERMINAL_STATUSES.has(concurrent.status)) {
          return dedupeResponse(concurrent, idempotencyKey);
        }
        return {
          status: 'in_progress_conflict',
          idempotencyKey,
          error: `stale lease steal race lost (current status=${concurrent?.status})`,
        };
      }
      // 奪取成功 → 以降の pipeline を実行
      ownAttempt = getRow(db, idempotencyKey)?.attempt ?? null;
    } else if (DEDUPE_TERMINAL_STATUSES.has(existing.status)) {
      return dedupeResponse(existing, idempotencyKey);
    } else if (RETRYABLE_STATUSES.has(existing.status)) {
      // 'not_implemented' / 'failed' は dedupe しない → 既存 row を残したまま再実行可能。
      //   'not_implemented': E-5b で実 publish に flip された時の placeholder
      //   'failed':          中間バグ修正後 / 一時障害から復旧して再実行したいケース
      //
      // Codex 2026-06-25 race guard:
      //   2 caller が同 retryable row を読んだ場合、 片方の UPDATE は changes=1、 もう片方は 0。
      //   既存 (PR #347) 実装は 0 で in_progress_conflict 返してたが、 race で諦めると
      //   後発 caller が何も出来ない。 refreshed.status が retryable のままなら短期 retry。
      //   上限 RETRY_RACE_MAX で無限ループ防止。
      const RETRY_RACE_MAX = 3;
      let succeeded = false;
      for (let i = 0; i < RETRY_RACE_MAX; i += 1) {
        const r = db.prepare(`
          UPDATE publish_idempotency
             SET status           = 'in_progress',
                 attempt          = attempt + 1,
                 lease_expires_at = ?,
                 error_message    = NULL,
                 result_json      = NULL,
                 created_by       = ?,
                 updated_at       = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
           WHERE idempotency_key  = ?
             AND status           IN ('not_implemented', 'failed')
        `).run(leaseExpiresAt(), createdBy, idempotencyKey);
        if (r.changes === 1) { succeeded = true; break; }
        // changes === 0: race で別 caller が status を変えた
        const refreshed = getRow(db, idempotencyKey);
        if (!refreshed) {
          // row 自体が消えた (rare): 通常経路に流す = insert in_progress 試行
          return {
            status: 'in_progress_conflict',
            idempotencyKey,
            error: `${existing.status} row disappeared mid-retry`,
          };
        }
        if (DEDUPE_TERMINAL_STATUSES.has(refreshed.status)) {
          return dedupeResponse(refreshed, idempotencyKey);
        }
        if (refreshed.status === 'in_progress') {
          return {
            status: 'in_progress_conflict',
            idempotencyKey,
            error: `${existing.status} → in_progress retry race lost to concurrent caller`,
            leaseExpiresAt: refreshed.lease_expires_at,
          };
        }
        if (!RETRYABLE_STATUSES.has(refreshed.status)) {
          // 既知 retryable でも success でも in_progress でもない → dedupe (fail-closed)
          return dedupeResponse(refreshed, idempotencyKey);
        }
        // refreshed.status は retryable のまま → 短期 race、 もう一度 UPDATE 試す
      }
      if (!succeeded) {
        return {
          status: 'in_progress_conflict',
          idempotencyKey,
          error: `retryable → in_progress UPDATE failed ${RETRY_RACE_MAX} times (race exhausted)`,
        };
      }
      // retryable → in_progress 再取得成功 → attempt 固定
      ownAttempt = getRow(db, idempotencyKey)?.attempt ?? null;
    } else {
      // 未知 status → 安全側で dedupe
      return dedupeResponse(existing, idempotencyKey);
    }
  } else {
    // insert in_progress (lease 取得)
    const acquired = insertInProgress(db, {
      idempotencyKey, itemCode,
      manageNumber: manageNumber || '',
      createdBy,
    });
    if (!acquired) {
      // race で 他 process が先に挿入 → 再 read、 terminal なら dedupe を返す (Codex R1 M-3)
      const concurrent = getRow(db, idempotencyKey);
      if (concurrent && DEDUPE_TERMINAL_STATUSES.has(concurrent.status)) {
        return dedupeResponse(concurrent, idempotencyKey);
      }
      return {
        status: 'in_progress_conflict',
        idempotencyKey,
        error: `Another caller acquired the lease concurrently (status=${concurrent?.status}).`,
      };
    }
    // insert 成功 → attempt=1 が固定 owner
    ownAttempt = 1;
  }

  try {
    // pipeline 実行 (dry-run=false で readiness を jobs に persist)
    const evalResult = await evaluateItemForPublish({
      db, itemCode, manageNumber,
      dryRun: false,
      ...publishOpts,
    });

    // ownAttempt は lease 取得直後 (Codex E-5a R3 H-1) に固定済み。 ここでは再 read しない。
    if (evalResult.status === 'blocked') {
      const finalized = finalize(db, {
        idempotencyKey,
        status: 'failed',
        resultJson: JSON.stringify(evalResult),
        errorMessage: `readiness blocked: ${evalResult.reasons.slice(0, 3).join('; ')}`,
        ownAttempt,
      });
      if (!finalized) {
        return {
          status: 'lease_lost',
          idempotencyKey,
          error: 'finalize CAS failed: another caller has stolen the lease before we could write the result.',
        };
      }
      audit(db, 'publish_execute_blocked', { itemCode, reasons: evalResult.reasons }, { result: 'failed', errorMessage: 'readiness blocked' });
      return {
        status: 'readiness_blocked',
        idempotencyKey,
        reasons: evalResult.reasons,
        executorResult: evalResult,
      };
    }

    // Phase E-5b: 実 publish — 画像 upload → editItem 呼び出し
    let publishResult;
    try {
      publishResult = await performRealPublish({
        rakutenImages: evalResult.debug?.rakutenImages || [],
        fields: evalResult.fields,
        itemCode,
        customPatterns: evalResult.debug?.customImagePatterns || [],
        rakutenItem: evalResult.debug?.rakutenItem || null,
      });
    } catch (e) {
      const errMsg = e instanceof YahooProxyError ? `yahoo_proxy: ${e.message}` : (e.message || String(e));
      const finalized = finalize(db, {
        idempotencyKey,
        status: 'failed',
        resultJson: JSON.stringify({ ...evalResult, publishError: errMsg }),
        errorMessage: `real_publish_failed: ${errMsg}`,
        ownAttempt,
      });
      if (!finalized) {
        return {
          status: 'lease_lost',
          idempotencyKey,
          error: `publish threw "${errMsg}" but lease was stolen before finalize`,
        };
      }
      audit(db, 'publish_execute_publish_fail', { itemCode, error: errMsg }, { result: 'failed', errorMessage: errMsg });
      return {
        status: 'publish_failed',
        idempotencyKey,
        error: errMsg,
        executorResult: evalResult,
      };
    }

    const finalized = finalize(db, {
      idempotencyKey,
      status: 'success',
      resultJson: JSON.stringify({ ...evalResult, publishResult }),
      ownAttempt,
    });
    if (!finalized) {
      return {
        status: 'lease_lost',
        idempotencyKey,
        error: 'finalize CAS failed: lease was stolen after Yahoo publish call. Manual reconciliation required (the editItem call may have succeeded on Yahoo side).',
        publishResult,
      };
    }
    audit(db, 'publish_execute_success', { itemCode, idempotencyKey, publishResult }, { result: 'success' });
    return {
      status: 'success',
      idempotencyKey,
      publishResult,
      fields: evalResult.fields,
      executorResult: evalResult,
    };
  } catch (e) {
    // Codex E-5a R3 H-2: 例外時も CAS finalize。 lease を奪われていれば row は触らず audit のみ。
    const finalized = finalize(db, {
      idempotencyKey,
      status: 'failed',
      errorMessage: e.message || String(e),
      ownAttempt,
    });
    audit(db, 'publish_execute_fail', { itemCode, error: e.message, finalizedByOwner: finalized }, { result: 'failed', errorMessage: e.message });
    if (!finalized) {
      return {
        status: 'lease_lost',
        idempotencyKey,
        error: `pipeline threw "${e.message}" but lease was stolen before finalize could write failed state`,
      };
    }
    return {
      status: 'fail',
      idempotencyKey,
      error: e.message || String(e),
    };
  }
}
