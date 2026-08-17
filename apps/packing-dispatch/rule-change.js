/**
 * 配送ルール変更の承認フロー (要件⑥ — 中原さん指示 2026-08-17)。
 *
 * 梱包現場 (miniPC apps/packing) から「モール・数量帯・配送方法・梱包機」の変更申請を受け、
 * Google Chat にカードを投稿。**承認ボタンで pd_* を書き換える**。
 *   - 1 SKU → pd_shipping_rule の該当数量帯 (帯構成は変えない = 区間連続性を壊さない)
 *   - 複数SKU → アソート学習 (saveAssortDecision — SCD2・letterpack学習不可はそのまま適用)
 *
 * カードのボタン処理は stock-bot の /chat-events (同じChatアプリ) が受けて applyDecision を呼ぶ。
 * カード投稿は GOOGLE_SERVICE_ACCOUNT_KEY (base64 JSON) + scope chat.bot。
 * env: PD_RULE_CHANGE_KEY (miniPCからの申請API認証) / PD_RULE_CHANGE_SPACE (spaces/xxx)
 */
import { JWT } from 'google-auth-library';
import {
  ensureSchema, utcIsoNow, MALL_GROUPS, buildSkuKey, normProductCode,
  saveAssortDecision, shippingMethodMap,
} from './db.js';
import { bulkUpdateRules } from './service.js';

function vErr(message) {
  const e = new Error(message);
  e.code = 'VALIDATION';
  return e;
}

let _schemaReady = false;
function db() {
  const d = ensureSchema();
  if (!_schemaReady) {
    d.exec(`CREATE TABLE IF NOT EXISTS pd_rule_change_request (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      kind TEXT NOT NULL CHECK(kind IN ('single','assort')),
      items_json TEXT NOT NULL,          -- [{sku, name, qty}] 依頼元伝票の明細
      mall_group TEXT,                   -- single のみ
      qty_min INTEGER, qty_max INTEGER,  -- single のみ (qty_max NULL=無制限)
      shipping_method_code TEXT NOT NULL,
      packing_machine_code TEXT NOT NULL,
      context TEXT,                      -- 出荷_XX / 伝票番号など
      status TEXT NOT NULL DEFAULT 'requested'
        CHECK(status IN ('requested','approved','rejected','failed')),
      requested_by TEXT NOT NULL,
      decided_by TEXT, decided_at TEXT, decide_note TEXT,
      created_at TEXT NOT NULL
    )`);
    _schemaReady = true;
  }
  return d;
}

/** セレクトの選択肢 (miniPCの梱包画面が使う)。 */
export function getRuleChangeOptions() {
  const d = db();
  const methods = d.prepare(
    "SELECT code, name_csv AS name FROM pd_shipping_method WHERE code != 'aes' ORDER BY rank, code"
  ).all();
  const machines = d.prepare('SELECT code, name_csv AS name FROM pd_packing_machine ORDER BY sort_order, code').all();
  return { mallGroups: MALL_GROUPS, methods, machines };
}

/**
 * 申請の作成 (miniPCから)。検証は fail-closed:
 *   - method/machine コードの実在 + 「梱包機なら配送方法はネコポス」制約 (DBのCHECKと同じ)
 *   - single は mall_group と数量帯が必須
 */
export function createRuleChangeRequest({
  kind, items, mallGroup, qtyMin, qtyMax, shippingMethodCode, packingMachineCode,
  requestedBy, context,
}) {
  const d = db();
  if (!['single', 'assort'].includes(kind)) throw vErr('kind が不正です');
  if (!Array.isArray(items) || items.length === 0) throw vErr('明細がありません');
  if (kind === 'single' && items.length !== 1) throw vErr('単品申請は明細1件のみです');
  if (kind === 'assort' && items.length < 2) throw vErr('アソート申請は明細2件以上です');
  for (const it of items) {
    if (!it?.sku || !Number.isInteger(Number(it.qty)) || Number(it.qty) < 1) throw vErr('明細の形式が不正です');
  }
  const sm = String(shippingMethodCode || '');
  const pm = String(packingMachineCode || '');
  if (!shippingMethodMap().has(sm)) throw vErr('未知の配送方法コード: ' + sm);
  if (sm === 'aes') throw vErr('AES へは変更できません');
  if (!d.prepare('SELECT 1 FROM pd_packing_machine WHERE code=?').get(pm)) throw vErr('未知の梱包機コード: ' + pm);
  if (pm !== 'manual' && sm !== 'nekopos') {
    throw vErr('梱包機を使う場合、配送方法はネコポスのみです (手動なら任意)');
  }
  if (kind === 'single') {
    if (!MALL_GROUPS.includes(mallGroup)) throw vErr('不正なモール指定です');
    if (!Number.isInteger(qtyMin) || qtyMin < 1) throw vErr('数量帯の下限が不正です');
    if (qtyMax != null && (!Number.isInteger(qtyMax) || qtyMax < qtyMin)) throw vErr('数量帯の上限が不正です');
  }
  const now = utcIsoNow();
  const info = d.prepare(`
    INSERT INTO pd_rule_change_request
      (kind, items_json, mall_group, qty_min, qty_max, shipping_method_code, packing_machine_code,
       context, status, requested_by, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'requested', ?, ?)
  `).run(kind, JSON.stringify(items), kind === 'single' ? mallGroup : null,
    kind === 'single' ? qtyMin : null, kind === 'single' ? (qtyMax ?? null) : null,
    sm, pm, context || null, String(requestedBy || '梱包現場'), now);
  return d.prepare('SELECT * FROM pd_rule_change_request WHERE id=?').get(Number(info.lastInsertRowid));
}

/**
 * 承認/却下 (Chatカードのボタンから)。
 * 承認: single → 既存の該当数量帯の method/machine を更新 (帯が無ければ failed=手動対応へ)。
 *       assort → saveAssortDecision (letterpackは学習不可でスキップ→failed扱い)
 * 冪等: requested 以外は already エラー (ボタン連打・古いカード対策)。
 */
export function applyRuleChangeDecision(id, decision, email) {
  const d = db();
  if (!['approve', 'reject'].includes(decision)) throw vErr('decision が不正です');
  const row = d.prepare('SELECT * FROM pd_rule_change_request WHERE id=?').get(id);
  if (!row) throw vErr('申請が見つかりません');
  if (row.status !== 'requested') {
    return { ...row, already: true };
  }
  const now = utcIsoNow();
  if (decision === 'reject') {
    d.prepare("UPDATE pd_rule_change_request SET status='rejected', decided_by=?, decided_at=? WHERE id=?")
      .run(email, now, id);
    return { ...row, status: 'rejected', decided_by: email };
  }
  const items = JSON.parse(row.items_json);
  let note = null;
  let status = 'approved';
  try {
    if (row.kind === 'single') {
      const it = items[0];
      // sku_key は buildSkuKey(商品コード) — カラー/サイズ違いのバリアント商品は一致しない
      // ことがある (その場合は notFound → failed で手動対応を案内)
      const sku_key = buildSkuKey(it.sku, '', '');
      const r = bulkUpdateRules({
        items: [{ sku_key, mall_group: row.mall_group, qty_min: row.qty_min, qty_max: row.qty_max ?? null }],
        shipping_method_code: row.shipping_method_code,
        packing_machine_code: row.packing_machine_code,
      }, `chat承認:${email}`);
      if (r.updated !== 1) {
        status = 'failed';
        note = `該当の数量帯が見つかりません (${normProductCode(it.sku)} / ${row.mall_group} / ${row.qty_min}〜${row.qty_max ?? '∞'})。packing-dispatch画面で帯構成から登録してください`;
      } else if (r.corrected > 0) {
        note = '梱包機の整合矯正あり (ネコポス以外→手動)';
      }
    } else {
      const r = saveAssortDecision({
        items: items.map((it) => ({ sku_key: buildSkuKey(it.sku, '', ''), qty: Number(it.qty) })),
        combo_detail: items.map((it) => ({ sku: it.sku, name: it.name || null, qty: Number(it.qty) })),
        shipping_method_code: row.shipping_method_code,
        packing_machine_code: row.packing_machine_code,
      }, `chat承認:${email}`);
      if (r.skipped) {
        status = 'failed';
        note = 'レターパックは学習対象外です (毎回目視判断)';
      }
    }
  } catch (e) {
    status = 'failed';
    note = e.message;
  }
  d.prepare("UPDATE pd_rule_change_request SET status=?, decided_by=?, decided_at=?, decide_note=? WHERE id=?")
    .run(status, email, now, note, id);
  return { ...row, status, decided_by: email, decide_note: note };
}

// ─── Chat カード ───

const KIND_LABEL = { single: '単品ルール', assort: 'アソート学習' };

export function describeRequest(row) {
  const items = JSON.parse(row.items_json);
  const methods = shippingMethodMap();
  const d = db();
  const pmName = d.prepare('SELECT name_csv FROM pd_packing_machine WHERE code=?').get(row.packing_machine_code)?.name_csv || row.packing_machine_code;
  const smName = methods.get(row.shipping_method_code)?.name_csv || row.shipping_method_code;
  const lines = items.map((it) => `・${it.name || it.sku} × ${it.qty}`).join('\n');
  const band = row.kind === 'single'
    ? `${row.mall_group} / 数量 ${row.qty_min}〜${row.qty_max ?? '∞'}`
    : 'この組み合わせ (アソート)';
  return { lines, band, smName, pmName };
}

/** 承認カードの投稿 (fail-soft: 失敗しても申請自体は成立。DBが正本)。 */
export async function postApprovalCard(row, { fetchFn = fetch } = {}) {
  const space = process.env.PD_RULE_CHANGE_SPACE;
  const keyBase64 = process.env.GOOGLE_SERVICE_ACCOUNT_KEY;
  if (!space || !keyBase64) {
    console.warn('[pd-rule-change] PD_RULE_CHANGE_SPACE / GOOGLE_SERVICE_ACCOUNT_KEY 未設定 → 承認カード投稿なし');
    return false;
  }
  const creds = JSON.parse(Buffer.from(keyBase64, 'base64').toString('utf8'));
  const jwt = new JWT({
    email: creds.client_email,
    key: creds.private_key,
    scopes: ['https://www.googleapis.com/auth/chat.bot'],
  });
  const { token } = await jwt.getAccessToken();
  const info = describeRequest(row);
  const body = {
    text: `🔧 配送ルールの変更申請 #${row.id} (${KIND_LABEL[row.kind]}) — 依頼: ${row.requested_by}${row.context ? ` (${row.context})` : ''}`,
    cardsV2: [{
      cardId: `pd-rule-${row.id}`,
      card: {
        sections: [{
          widgets: [
            { textParagraph: { text: `${info.lines}\n<b>${info.band}</b>\n→ 配送方法: <b>${info.smName}</b> / 梱包機: <b>${info.pmName}</b>` } },
            {
              buttonList: {
                buttons: [
                  { text: '✅ 承認してDB反映', onClick: { action: { function: 'pdRuleApprove', parameters: [{ key: 'id', value: String(row.id) }] } } },
                  { text: '却下', onClick: { action: { function: 'pdRuleReject', parameters: [{ key: 'id', value: String(row.id) }] } } },
                ],
              },
            },
          ],
        }],
      },
    }],
  };
  const res = await fetchFn(`https://chat.googleapis.com/v1/${space}/messages`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`Chat投稿失敗: HTTP ${res.status} ${await res.text().catch(() => '')}`.slice(0, 200));
  return true;
}

/** ボタン押下後の応答テキスト (stock-bot が UPDATE_MESSAGE で返す)。 */
export function decisionReplyText(row) {
  const info = describeRequest(row);
  const head = row.already
    ? `⚠ 申請 #${row.id} は既に処理済みです (${row.status})`
    : row.status === 'approved'
      ? `✅ 申請 #${row.id} を承認し、${KIND_LABEL[row.kind]}へ反映しました`
      : row.status === 'rejected'
        ? `❌ 申請 #${row.id} を却下しました`
        : `🚨 申請 #${row.id} の反映に失敗: ${row.decide_note || '不明なエラー'}`;
  return [
    head,
    info.lines,
    `${info.band} → ${info.smName} / ${info.pmName}`,
    `操作: ${row.decided_by || '-'}${row.decide_note && row.status === 'approved' ? ` (${row.decide_note})` : ''}`,
  ].join('\n');
}
