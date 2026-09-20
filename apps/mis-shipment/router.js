/**
 * 誤出荷管理アプリ router (Render bfaith-portal 側)
 *
 * URL: /apps/mis-shipment/ (UI 4 画面) + /apps/mis-shipment/api/* (REST API)
 *
 * 認証: server.js で requireAppAccess('mis-shipment') を mount 時に適用 (社内ログイン)
 *       管理者専用操作は router 内で req.session.role === 'admin' を check
 *
 * 設計書: g:/共有ドライブ/AI_reference/システム設計/誤出荷管理システム_設計書_v5.md (中身 v7.3)
 * Codex 16 ラウンドレビュー完全 FIX
 */
import { Router } from 'express';
import express from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import {
  computePayloadHash, getJstDateString, utcIsoNow,
  insertSingleMisShipment, insertMixUpMisShipments,
  getMisShipmentDetail, listMisShipments,
  transitionStatus, patchEditableFields, softDelete,
  markFieldReviewed, countNeedsFieldReview, canCorrectFields,
  VALID_TRANSITIONS,
} from './db.js';
import { getMisShipmentDashboard } from './summary.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const router = Router();

// ─── 静的ファイル配信 (CSS / JS) ───
router.use('/public', express.static(path.join(__dirname, 'public'), {
  maxAge: '1h',
  index: false,
}));

// ─── 設定 ───
const LOOKUP_BASE = process.env.WAREHOUSE_URL || 'https://wh.bfaith-wh.uk';
const LOOKUP_TOKEN = process.env.WAREHOUSE_LOOKUP_TOKEN;
const LOOKUP_TIMEOUT_MS = 5000;

const MALL_ENUM = new Set(['amazon','rakuten','yahoo','linegift','mercari','aupay','qoo10','other']);
const MIS_TYPE_ENUM = new Set(['wrong_item','wrong_qty','damage','missing','wrong_address','mix_up','other']);
const PROCESS_STAGE_ENUM = new Set(['picking','packing','labeling','inspection','handover','unknown']);
const ROOT_CAUSE_STAGE_ENUM = new Set(['receiving','supplier','master_data','picking','packing','labeling','inspection','system','other','unknown']);

// ─── helper: admin check ───
function isAdmin(req) {
  return req.session && req.session.role === 'admin';
}
function requireAdminApi(req, res, next) {
  if (!isAdmin(req)) return res.status(403).json({ error: 'admin_required' });
  next();
}

// ─── helper: miniPC へ order lookup ───
async function lookupOrderFromMinipc(orderId) {
  if (!LOOKUP_TOKEN) {
    const err = new Error('LOOKUP_TOKEN_UNSET');
    err.code = 'token_unset';
    throw err;
  }
  const url = `${LOOKUP_BASE}/lookup-api/orders/lookup?order_id=${encodeURIComponent(orderId)}`;
  // Cloudflare Access (Zero Trust) bypass: wh.bfaith-wh.uk は Cloudflare Tunnel + Access で守られているため、
  // CF-Access-Client-Id / CF-Access-Client-Secret service token を付けないと 302 で認証ページに redirect される。
  // 既存 apps/fba-replenishment/router.js getServiceHeaders() と同じパターン。
  // 関連 env: CF_ACCESS_CLIENT_ID, CF_ACCESS_CLIENT_SECRET (Render env で既に設定済み)
  const headers = {
    'x-api-key': LOOKUP_TOKEN,
    'CF-Access-Client-Id': process.env.CF_ACCESS_CLIENT_ID || '',
    'CF-Access-Client-Secret': process.env.CF_ACCESS_CLIENT_SECRET || '',
  };
  let res;
  try {
    res = await fetch(url, {
      method: 'GET',
      headers,
      signal: AbortSignal.timeout(LOOKUP_TIMEOUT_MS),
    });
  } catch (e) {
    const err = new Error('LOOKUP_FETCH_FAILED');
    err.code = 'fetch_failed';
    err.cause = e;
    throw err;
  }
  if (res.status === 503) {
    const err = new Error('LOOKUP_UNAVAILABLE');
    err.code = 'unavailable';
    throw err;
  }
  if (!res.ok) {
    const err = new Error(`LOOKUP_HTTP_${res.status}`);
    err.code = 'http_error';
    err.status = res.status;
    throw err;
  }
  return res.json();
}

// ─── helper: 入力 record の正規化と CHECK 制約事前検証 (DB CHECK と二層防御) ───
function buildSnapshotFields(lookupResult) {
  // lookupResult: { found, mall, sku, product_name, ordered_qty, order_date }
  if (lookupResult && lookupResult.found) {
    return {
      lookup_source: 'mirror_auto',
      mall: lookupResult.mall || null,
      sku_snapshot: lookupResult.sku || null,
      product_name_snapshot: lookupResult.product_name || null,
      ordered_qty_snapshot: lookupResult.ordered_qty ?? null,
      order_date_snapshot: lookupResult.order_date || null,
    };
  }
  return {
    lookup_source: 'manual',
    mall: null,                   // manual で mall も無いケース (本来不在、フロントで防止)
    sku_snapshot: null,
    product_name_snapshot: null,
    ordered_qty_snapshot: null,
    order_date_snapshot: null,
  };
}

function validateRecordInput(input) {
  // 必須
  const errs = [];
  if (typeof input.client_submission_id !== 'string' || !/^[0-9a-fA-F-]{36}$/.test(input.client_submission_id)) errs.push('client_submission_id');
  if (typeof input.occurred_on !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(input.occurred_on)) errs.push('occurred_on');
  if (!MIS_TYPE_ENUM.has(input.mis_type)) errs.push('mis_type');
  if (!PROCESS_STAGE_ENUM.has(input.process_stage)) errs.push('process_stage');
  if (!Number.isInteger(input.qty_affected) || input.qty_affected < 1 || input.qty_affected > 1000) errs.push('qty_affected');
  if (!Number.isInteger(input.loss_amount_jpy) || input.loss_amount_jpy < 0 || input.loss_amount_jpy > 10000000) errs.push('loss_amount_jpy');
  if (typeof input.order_id_unknown !== 'boolean') errs.push('order_id_unknown');
  if (!input.order_id_unknown) {
    if (typeof input.mall_order_id !== 'string' || input.mall_order_id.length === 0 || input.mall_order_id.length > 100) errs.push('mall_order_id');
  }
  if (input.reporter_note != null && (typeof input.reporter_note !== 'string' || input.reporter_note.length > 2000)) errs.push('reporter_note');
  return errs;
}

// 制御文字除去 + max length truncate
// 正規表現には制御文字を直接書かない (NUL が混ざると git がこのファイルを
// バイナリ扱いし、PR の差分が読めなくなる)
function sanitizeText(s, max = 2000) {
  if (s == null) return null;
  const stripped = String(s).replace(/[\x00-\x1F\x7F]/g, '');
  return stripped.length > max ? stripped.slice(0, max) : stripped;
}

// ─── GET /api/orders/lookup?order_id=XXX (UI からのオンライン検索) ───
router.get('/api/orders/lookup', async (req, res) => {
  const orderId = String(req.query.order_id || '').trim();
  if (!orderId) return res.status(400).json({ error: 'order_id_required' });
  if (orderId.length > 100) return res.status(400).json({ error: 'order_id_too_long' });
  try {
    const result = await lookupOrderFromMinipc(orderId);
    return res.json(result);
  } catch (e) {
    if (e.code === 'token_unset') return res.status(503).json({ error: 'lookup_token_unset' });
    if (e.code === 'unavailable') return res.status(503).json({ error: 'lookup_unavailable' });
    console.error('[mis-shipment] lookup error:', e.message);
    return res.status(502).json({ error: 'lookup_failed' });
  }
});

// ─── POST /api/submissions (新規登録、mix_up 対応) ───
// payload: { mix_up: boolean, records: [{...}] (1 or 2 records) }
//          各 record: client_submission_id, occurred_on, mall_order_id, order_id_unknown,
//                     mis_type, qty_affected, loss_amount_jpy, process_stage, reporter_note
//          server-side authoritative fetch: order_id_unknown=false なら mall_order_id をサーバが lookup し直し、
//          snapshot を取得 (UI 経由の値は信用しない)
router.post('/api/submissions', async (req, res) => {
  const reportedBy = req.session.email;
  if (!reportedBy) return res.status(401).json({ error: 'session_expired' });

  const isMixUp = req.body && req.body.mix_up === true;
  const inputs = Array.isArray(req.body && req.body.records) ? req.body.records : [];
  if (isMixUp && inputs.length !== 2) return res.status(400).json({ error: 'mix_up_requires_2_records' });
  if (!isMixUp && inputs.length !== 1) return res.status(400).json({ error: 'single_requires_1_record' });

  // mix_up 強制: 両 record の mis_type は 'mix_up' に上書き (UI 経由値を server で confirm)
  if (isMixUp) {
    for (const r of inputs) r.mis_type = 'mix_up';
  } else {
    // 単独で mis_type='mix_up' は不正
    if (inputs[0].mis_type === 'mix_up') return res.status(400).json({ error: 'mix_up_must_use_records_pair' });
  }

  // 各 record を validate + サーバ側 lookup で snapshot 確定
  const normalized = [];
  for (const input of inputs) {
    const errs = validateRecordInput(input);
    if (errs.length > 0) return res.status(400).json({ error: 'validation_failed', fields: errs });

    let snapshot;
    if (input.order_id_unknown) {
      snapshot = {
        lookup_source: 'unknown',
        mall: null,
        sku_snapshot: null,
        product_name_snapshot: null,
        ordered_qty_snapshot: null,
        order_date_snapshot: null,
      };
    } else {
      let lookupResult;
      try {
        lookupResult = await lookupOrderFromMinipc(input.mall_order_id);
      } catch (e) {
        if (e.code === 'token_unset' || e.code === 'unavailable') {
          return res.status(503).json({ error: 'lookup_unavailable' });
        }
        return res.status(502).json({ error: 'lookup_failed' });
      }
      if (lookupResult && lookupResult.found) {
        // lookup ヒット → mirror_auto で snapshot 確定
        snapshot = buildSnapshotFields(lookupResult);
      } else {
        // lookup ノヒット (注文番号は分かるがマスターに無い) → manual モード (Codex round 17 high 指摘対応)
        // UI が manual_mall を指定している前提。指定無しは 400 を返す。
        const userMall = typeof input.manual_mall === 'string' ? input.manual_mall : null;
        if (!userMall || !MALL_ENUM.has(userMall)) {
          return res.status(400).json({
            error: 'lookup_miss_requires_manual_mall',
            detail: 'order_id がマスターに無い場合、manual_mall (Amazon等) を payload に含めてください',
          });
        }
        snapshot = {
          lookup_source: 'manual',
          mall: userMall,
          sku_snapshot: null,
          product_name_snapshot: null,
          ordered_qty_snapshot: null,
          order_date_snapshot: null,
        };
      }
    }

    const now = utcIsoNow();
    const record = {
      client_submission_id: input.client_submission_id,
      version: 0,
      occurred_on: input.occurred_on,
      reported_at: now,
      mall_order_id: input.order_id_unknown ? null : input.mall_order_id,
      order_id_unknown: input.order_id_unknown ? 1 : 0,
      mall: snapshot.mall,
      sku_snapshot: snapshot.sku_snapshot,
      product_name_snapshot: snapshot.product_name_snapshot,
      ordered_qty_snapshot: snapshot.ordered_qty_snapshot,
      order_date_snapshot: snapshot.order_date_snapshot,
      lookup_source: snapshot.lookup_source,
      mis_type: input.mis_type,
      qty_affected: input.qty_affected,
      loss_amount_jpy: input.loss_amount_jpy,
      process_stage: input.process_stage,
      root_cause_stage: 'unknown',
      root_cause_note: null,
      mix_up_group_id: null,  // 単独時 null、mix_up なら insertMixUp で上書き
      status: 'reported',
      reporter_note: sanitizeText(input.reporter_note, 2000),
      reported_by: reportedBy,
      created_at: now,
      updated_at: now,
      updated_by: reportedBy,
    };
    record.payload_hash = computePayloadHash({
      client_submission_id: record.client_submission_id,
      occurred_on: record.occurred_on,
      mall_order_id: record.mall_order_id,
      order_id_unknown: record.order_id_unknown,
      mall: record.mall,
      sku_snapshot: record.sku_snapshot,
      mis_type: record.mis_type,
      qty_affected: record.qty_affected,
      loss_amount_jpy: record.loss_amount_jpy,
      process_stage: record.process_stage,
      reporter_note: record.reporter_note,
    });
    normalized.push(record);
  }

  if (isMixUp) {
    const result = insertMixUpMisShipments(normalized[0], normalized[1]);
    if (result.conflict) return res.status(409).json({ error: 'mix_up_partial_conflict', detail: result.detail });
    if (result.idempotent) return res.status(200).json({ ids: result.ids, group_id: result.groupId, idempotent: true });
    return res.status(201).json({ ids: result.ids, group_id: result.groupId });
  } else {
    const result = insertSingleMisShipment(normalized[0]);
    if (result.conflict) return res.status(409).json({ error: 'payload_hash_conflict', existing_id: result.existingId });
    if (result.idempotent) return res.status(200).json({ id: result.existing.id, idempotent: true });
    return res.status(201).json({ id: result.id });
  }
});

// ─── GET /api/submissions ───
router.get('/api/submissions', (req, res) => {
  const filters = {
    mall: req.query.mall && MALL_ENUM.has(req.query.mall) ? req.query.mall : null,
    status: req.query.status,
    fromDate: req.query.from,
    toDate: req.query.to,
    processStage: req.query.process_stage,
    rootCauseStage: req.query.root_cause_stage,
    // 一覧の検索窓 (注文番号 / SKU / 商品名)。長すぎる入力は切る。
    q: typeof req.query.q === 'string' && req.query.q.trim() ? req.query.q.trim().slice(0, 100) : null,
    // 種別・工程が当てにならない行 (2026-09-20 の修正より前に登録) だけに絞る
    needsFieldReview: req.query.needs_field_review === '1',
    limit: Math.min(parseInt(req.query.limit || '100', 10), 500),
    offset: parseInt(req.query.offset || '0', 10),
  };
  const rows = listMisShipments(filters);
  // 数えられなかったときは null (0 と混ぜない)
  res.json({ rows, count: rows.length, needs_field_review_total: countNeedsFieldReview() });
});

// ─── GET /api/submissions/:id ───
router.get('/api/submissions/:id', (req, res) => {
  const id = parseInt(req.params.id, 10);
  if (!Number.isInteger(id)) return res.status(400).json({ error: 'invalid_id' });
  const detail = getMisShipmentDetail(id);
  if (!detail) return res.status(404).json({ error: 'not_found' });
  res.json(detail);
});

// ─── PATCH /api/submissions/:id (status 遷移 / フィールド更新、楽観ロック) ───
router.patch('/api/submissions/:id', (req, res) => {
  const id = parseInt(req.params.id, 10);
  if (!Number.isInteger(id)) return res.status(400).json({ error: 'invalid_id' });

  const { version, status, fields, change_note } = req.body || {};

  const userEmail = req.session.email;
  if (!userEmail) return res.status(401).json({ error: 'session_expired' });

  // 「直すところは無い」と管理者が確認した印。レコード自体は変えないので version は要らない。
  if (req.body && req.body.field_review === true) {
    if (!isAdmin(req)) return res.status(403).json({ error: 'admin_required_for_field_review' });
    const result = markFieldReviewed(id, userEmail);
    if (!result.ok) {
      if (result.reason === 'not_found') return res.status(404).json({ error: 'not_found' });
      return res.status(503).json({
        error: 'field_history_unavailable',
        detail: '訂正履歴テーブルが使えないため、確認の記録ができません',
      });
    }
    return res.json({ ok: true });
  }

  if (!Number.isInteger(version)) return res.status(400).json({ error: 'version_required' });

  if (status) {
    // status 遷移: investigating 以降は管理者のみ
    if (status !== 'investigating' && !isAdmin(req)) {
      return res.status(403).json({ error: 'admin_required_for_status_transition' });
    }
    const result = transitionStatus(id, version, status, userEmail, change_note || null);
    if (!result.ok) {
      if (result.reason === 'not_found') return res.status(404).json({ error: 'not_found' });
      if (result.reason === 'version_mismatch') return res.status(409).json({ error: 'version_mismatch', current_version: result.currentVersion });
      if (result.reason === 'invalid_transition') return res.status(400).json({ error: 'invalid_transition', from: result.from, to: result.to });
      // Codex round 17 high 指摘対応: root_cause_stage 確定前に resolved/closed 不可
      if (result.reason === 'root_cause_required') return res.status(400).json({
        error: 'root_cause_required',
        detail: '完了/クローズに進む前に根本原因 (root_cause_stage) を確定してください',
      });
      return res.status(500).json({ error: 'internal_error' });
    }
    return res.json({ ok: true });
  }

  if (fields && typeof fields === 'object') {
    // root_cause_stage / root_cause_note は管理者のみ
    if (('root_cause_stage' in fields || 'root_cause_note' in fields) && !isAdmin(req)) {
      return res.status(403).json({ error: 'admin_required_for_root_cause' });
    }
    // mis_type / process_stage は本来「起票時に確定して編集不可」。
    // 2026-09-20 の不具合 (選択と無関係に先頭の値が保存されていた) を直すためだけに、
    // 管理者に限って開けている。
    if (('mis_type' in fields || 'process_stage' in fields) && !isAdmin(req)) {
      return res.status(403).json({ error: 'admin_required_for_field_correction' });
    }
    if (('mis_type' in fields || 'process_stage' in fields) && !canCorrectFields()) {
      return res.status(503).json({
        error: 'field_history_unavailable',
        detail: '訂正履歴テーブルが使えないため、種別・発見工程の訂正はできません',
      });
    }
    if (fields.root_cause_stage && !ROOT_CAUSE_STAGE_ENUM.has(fields.root_cause_stage)) {
      return res.status(400).json({ error: 'invalid_root_cause_stage' });
    }
    if ('mis_type' in fields && !MIS_TYPE_ENUM.has(fields.mis_type)) {
      return res.status(400).json({ error: 'invalid_mis_type' });
    }
    if ('process_stage' in fields && !PROCESS_STAGE_ENUM.has(fields.process_stage)) {
      return res.status(400).json({ error: 'invalid_process_stage' });
    }
    const sanitized = {
      ...(('reporter_note' in fields) ? { reporter_note: sanitizeText(fields.reporter_note, 2000) } : {}),
      ...(('root_cause_stage' in fields) ? { root_cause_stage: fields.root_cause_stage } : {}),
      ...(('root_cause_note' in fields) ? { root_cause_note: sanitizeText(fields.root_cause_note, 2000) } : {}),
      ...(('mis_type' in fields) ? { mis_type: fields.mis_type } : {}),
      ...(('process_stage' in fields) ? { process_stage: fields.process_stage } : {}),
    };
    const result = patchEditableFields(id, version, sanitized, userEmail);
    if (!result.ok) {
      // Codex round 18 high 指摘対応: resolved/closed のレコードを root_cause_stage='unknown' に戻す試みを拒否
      if (result.reason === 'root_cause_unknown_forbidden_after_resolve') {
        return res.status(400).json({
          error: 'root_cause_unknown_forbidden_after_resolve',
          detail: '完了/クローズ済みのレコードの根本原因を「不明」に戻すことはできません',
        });
      }
      // テレコ (mix_up) は mix_up_group_id と対で CHECK 制約になっているので種別を変えられない
      if (result.reason === 'mix_up_type_locked') {
        return res.status(400).json({
          error: 'mix_up_type_locked',
          detail: 'テレコの誤出荷種別は変更できません (相方とグループで対になっているため)',
        });
      }
      if (result.reason === 'field_history_unavailable') {
        return res.status(503).json({
          error: 'field_history_unavailable',
          detail: '訂正履歴が書けないため、訂正を取り消しました',
        });
      }
      return res.status(409).json({ error: 'version_mismatch_or_not_found' });
    }
    return res.json({ ok: true, changed: result.changed ?? 0 });
  }

  return res.status(400).json({ error: 'no_action' });
});

// ─── DELETE /api/submissions/:id (論理削除、admin only) ───
router.delete('/api/submissions/:id', requireAdminApi, (req, res) => {
  const id = parseInt(req.params.id, 10);
  if (!Number.isInteger(id)) return res.status(400).json({ error: 'invalid_id' });
  const version = parseInt(req.body?.version ?? req.query.version ?? '-1', 10);
  if (!Number.isInteger(version) || version < 0) return res.status(400).json({ error: 'version_required' });
  const result = softDelete(id, version, req.session.email);
  if (!result.ok) return res.status(409).json({ error: 'version_mismatch_or_not_found' });
  res.json({ ok: true });
});

// ─── ダッシュボード API (Phase G) ───
// GET /api/summary?period=month|week|custom&from=YYYY-MM-DD&to=YYYY-MM-DD
//
// 入力検証 (Codex round 2 medium 指摘対応、2026-05-19):
//   - 正規表現だけだと '2026-02-31' のような不正日付が Date 正規化で別日付に黙って丸まる
//   - from > to も summary.js で負の期間として変な結果を返す
//   - period='custom' 以外で from/to を指定された場合は無視 (フォールバック)
function isValidJstDate(s) {
  if (typeof s !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(s)) return false;
  const [y, m, d] = s.split('-').map(Number);
  if (m < 1 || m > 12 || d < 1 || d > 31) return false;
  // 月別日数チェック (うるう年も判定)
  const dim = [31, (y % 4 === 0 && (y % 100 !== 0 || y % 400 === 0)) ? 29 : 28,
               31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  return d <= dim[m - 1];
}
const MAX_PERIOD_DAYS = 366;  // 1年が上限

router.get('/api/summary', (req, res) => {
  const period = ['month', 'week', 'custom'].includes(req.query.period) ? req.query.period : 'month';
  let from = null;
  let to = null;
  if (period === 'custom') {
    if (typeof req.query.from !== 'string' || typeof req.query.to !== 'string') {
      return res.status(400).json({ error: 'custom_period_requires_from_and_to' });
    }
    if (!isValidJstDate(req.query.from) || !isValidJstDate(req.query.to)) {
      return res.status(400).json({ error: 'invalid_date_format', detail: 'YYYY-MM-DD 形式の実在する日付が必要です' });
    }
    from = req.query.from;
    to = req.query.to;
    if (from > to) {
      return res.status(400).json({ error: 'from_after_to', detail: 'from は to 以前の日付にしてください' });
    }
    // 期間が長すぎると重い + UX 上意味薄い
    const fromMs = Date.UTC(...from.split('-').map((v, i) => i === 1 ? Number(v) - 1 : Number(v)));
    const toMs = Date.UTC(...to.split('-').map((v, i) => i === 1 ? Number(v) - 1 : Number(v)));
    const days = Math.floor((toMs - fromMs) / 86400000) + 1;
    if (days > MAX_PERIOD_DAYS) {
      return res.status(400).json({ error: 'period_too_long', detail: `期間は ${MAX_PERIOD_DAYS} 日以内にしてください (現在: ${days} 日)` });
    }
  }
  // period='month'/'week' のときは from/to を無視 (resolvePeriod が today から決定)
  try {
    const dashboard = getMisShipmentDashboard({ period, from, to });
    res.json(dashboard);
  } catch (e) {
    console.error('[mis-shipment] dashboard summary error:', e.message);
    res.status(500).json({ error: 'internal_error' });
  }
});

// ─── UI ルート (5 画面、 EJS) ───
// 画面側のレンダリングは views/ ディレクトリに置く (Phase D + Phase G)
router.get('/', (req, res) => {
  res.render(path.join(__dirname, 'views/index'), {
    title: '誤出荷管理',
    nav: 'list',
    username: req.session.email,
    displayName: req.session.displayName,
    role: req.session.role,
  });
});
router.get('/dashboard', (req, res) => {
  res.render(path.join(__dirname, 'views/dashboard'), {
    title: '誤出荷 ダッシュボード',
    nav: 'dashboard',
    username: req.session.email,
    displayName: req.session.displayName,
    role: req.session.role,
  });
});
router.get('/new', (req, res) => {
  res.render(path.join(__dirname, 'views/new'), {
    title: '誤出荷 新規登録',
    nav: 'new',
    username: req.session.email,
    displayName: req.session.displayName,
    role: req.session.role,
    mixUp: false,
  });
});
router.get('/mixup', (req, res) => {
  res.render(path.join(__dirname, 'views/new'), {
    title: 'テレコ登録',
    nav: 'mixup',
    username: req.session.email,
    displayName: req.session.displayName,
    role: req.session.role,
    mixUp: true,
  });
});
router.get('/detail/:id', (req, res) => {
  const id = parseInt(req.params.id, 10);
  if (!Number.isInteger(id)) return res.status(400).send('invalid id');
  res.render(path.join(__dirname, 'views/detail'), {
    title: `誤出荷 #${id}`,
    nav: 'detail',
    username: req.session.email,
    displayName: req.session.displayName,
    role: req.session.role,
    id,
  });
});

export default router;
