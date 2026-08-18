/**
 * 配送ルール変更の承認フロー (要件⑥ — 中原さん指示 2026-08-17)。
 *
 * 梱包現場 (miniPC apps/packing) から「モール・数量帯・配送方法・梱包機」の変更申請を受け、
 * Google Chat にカードを投稿。**承認ボタンで pd_* を書き換える**。
 *   - 1 SKU → pd_shipping_rule の該当数量帯 (帯構成は変えない = 区間連続性を壊さない)
 *   - 複数SKU → アソート学習 (saveAssortDecision — SCD2・letterpack学習不可はそのまま適用)
 *
 * 安全設計 (Codexレビュー反映):
 *   - 申請時に対象 (target_key) と現在値スナップショット (current_json) を確定・保存
 *   - 同一対象の未処理申請は重複拒否 / 承認時は compare-and-set (申請後にルールが変わっていたら failed)
 *   - 反映+ステータス確定は同一トランザクション / 単品は申請時に帯の実在を検証 (早期失敗)
 *
 * カードのボタン処理は stock-bot の /chat-events (同じChatアプリ) が受けて applyDecision を呼ぶ。
 * env: PD_RULE_CHANGE_KEY (申請API認証) / PD_RULE_CHANGE_SPACE (spaces/xxx)
 */
import { JWT } from 'google-auth-library';
import {
  ensureSchema, utcIsoNow, MALL_GROUPS, buildSkuKey, normProductCode,
  saveAssortDecision, shippingMethodMap, comboKeyOf,
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
    // 追加列 (デプロイ済みテーブルへの追記。既にあれば無視)
    for (const col of ['target_key TEXT', 'current_json TEXT', 'resolved_key TEXT']) {
      try { d.exec(`ALTER TABLE pd_rule_change_request ADD COLUMN ${col}`); } catch { /* 追加済み */ }
    }
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

const cap = (v, n) => String(v ?? '').trim().slice(0, n);

/** 単品: 商品ID→sku_key の解決 (バリアントの曖昧さは早期失敗)。 */
function resolveSingleSkuKey(d, sku, mallGroup) {
  const exact = buildSkuKey(sku, '', '');
  const keys = d.prepare(
    'SELECT DISTINCT sku_key FROM pd_shipping_rule WHERE product_code = ? AND mall_group = ?'
  ).all(normProductCode(sku), mallGroup).map((r) => r.sku_key);
  if (keys.includes(exact)) return exact;
  if (keys.length === 1) return keys[0];
  if (keys.length === 0) {
    throw vErr(`この商品×モールのルールが未登録です (${normProductCode(sku)} / ${mallGroup})。packing-dispatch画面から登録してください`);
  }
  throw vErr('この商品はカラー/サイズ違いのルールが複数あります。packing-dispatch画面から変更してください');
}

/**
 * 申請の作成 (miniPCから)。検証は fail-closed:
 *   - method/machine コードの実在 + 「梱包機ならネコポス」制約
 *   - 単品は対象帯の実在を申請時に確認し、現在値をスナップショット (承認時のCAS用)
 *   - 同一対象の未処理申請は重複拒否
 */
export function createRuleChangeRequest({
  kind, items, mallGroup, qtyMin, qtyMax, shippingMethodCode, packingMachineCode,
  requestedBy, context,
}) {
  const d = db();
  if (!['single', 'assort'].includes(kind)) throw vErr('kind が不正です');
  if (!Array.isArray(items) || items.length === 0 || items.length > 30) throw vErr('明細が不正です (1〜30件)');
  if (kind === 'single' && items.length !== 1) throw vErr('単品申請は明細1件のみです');
  if (kind === 'assort' && items.length < 2) throw vErr('アソート申請は明細2件以上です');
  const seen = new Set();
  const normItems = items.map((it) => {
    const sku = cap(it?.sku, 80);
    const qty = Number(it?.qty);
    if (!sku || !Number.isInteger(qty) || qty < 1 || qty > 999) throw vErr('明細の形式が不正です');
    const key = sku.toLowerCase();
    if (seen.has(key)) throw vErr(`明細にSKUの重複があります (${sku})`);
    seen.add(key);
    return { sku, name: cap(it?.name, 120) || null, qty };
  });
  const sm = cap(shippingMethodCode, 40);
  const pm = cap(packingMachineCode, 40);
  if (!shippingMethodMap().has(sm)) throw vErr('未知の配送方法コード: ' + sm);
  if (sm === 'aes') throw vErr('AES へは変更できません');
  if (!d.prepare('SELECT 1 FROM pd_packing_machine WHERE code=?').get(pm)) throw vErr('未知の梱包機コード: ' + pm);
  if (pm !== 'manual' && sm !== 'nekopos') {
    throw vErr('梱包機を使う場合、配送方法はネコポスのみです (手動なら任意)');
  }

  let targetKey = null;
  let resolvedKey = null;
  let currentJson = null;
  if (kind === 'single') {
    if (!MALL_GROUPS.includes(mallGroup)) throw vErr('不正なモール指定です');
    if (!Number.isInteger(qtyMin) || qtyMin < 1) throw vErr('数量帯の下限が不正です');
    if (qtyMax != null && (!Number.isInteger(qtyMax) || qtyMax < qtyMin)) throw vErr('数量帯の上限が不正です');
    const skuKey = resolveSingleSkuKey(d, normItems[0].sku, mallGroup);
    const band = d.prepare(
      'SELECT shipping_method_code, packing_machine_code FROM pd_shipping_rule WHERE sku_key=? AND mall_group=? AND qty_min=? AND qty_max IS ?'
    ).get(skuKey, mallGroup, qtyMin, qtyMax ?? null);
    if (!band) {
      throw vErr(`該当の数量帯がありません (${qtyMin}〜${qtyMax ?? '∞'})。既存の帯と同じ範囲を指定するか、packing-dispatch画面で帯構成から変更してください`);
    }
    targetKey = `single:${skuKey}:${mallGroup}:${qtyMin}:${qtyMax ?? 'inf'}`;
    resolvedKey = skuKey;
    currentJson = JSON.stringify(band);
  } else {
    const combo = comboKeyOf(normItems.map((it) => ({ sku_key: buildSkuKey(it.sku, '', ''), qty: it.qty })));
    targetKey = `assort:${combo}`;
    resolvedKey = combo;
    const cur = d.prepare(`
      SELECT shipping_method_code, packing_machine_code FROM pd_assort_decision
      WHERE combo_key=? AND is_active=1 ORDER BY id DESC LIMIT 1
    `).get(combo) || null;
    currentJson = JSON.stringify(cur);
  }

  // 同一対象の未処理申請は重複拒否 (古い申請が後から最新ルールを上書きしないように)
  const dup = d.prepare(
    "SELECT id FROM pd_rule_change_request WHERE target_key=? AND status='requested'"
  ).get(targetKey);
  if (dup) throw vErr(`同じ対象の未処理申請があります (#${dup.id})。GChatの承認カードを先に処理してください`);

  const now = utcIsoNow();
  const info = d.prepare(`
    INSERT INTO pd_rule_change_request
      (kind, items_json, mall_group, qty_min, qty_max, shipping_method_code, packing_machine_code,
       context, status, requested_by, created_at, target_key, current_json, resolved_key)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'requested', ?, ?, ?, ?, ?)
  `).run(kind, JSON.stringify(normItems), kind === 'single' ? mallGroup : null,
    kind === 'single' ? qtyMin : null, kind === 'single' ? (qtyMax ?? null) : null,
    sm, pm, cap(context, 80) || null, cap(requestedBy, 40) || '梱包現場', now, targetKey, currentJson, resolvedKey);
  return d.prepare('SELECT * FROM pd_rule_change_request WHERE id=?').get(Number(info.lastInsertRowid));
}

/**
 * 承認/却下 (Chatカードのボタンから)。反映とステータス確定は同一トランザクション。
 * 承認時は current_json との compare-and-set — 申請後にルールが変わっていたら failed。
 */
export function applyRuleChangeDecision(id, decision, email) {
  const d = db();
  if (!['approve', 'reject'].includes(decision)) throw vErr('decision が不正です');
  const now = utcIsoNow();
  return d.transaction(() => {
    const row = d.prepare('SELECT * FROM pd_rule_change_request WHERE id=?').get(id);
    if (!row) throw vErr('申請が見つかりません');
    if (row.status !== 'requested') {
      return { ...row, already: true };
    }
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
        const skuKey = row.resolved_key || buildSkuKey(items[0].sku, '', '');
        const band = d.prepare(
          'SELECT shipping_method_code, packing_machine_code FROM pd_shipping_rule WHERE sku_key=? AND mall_group=? AND qty_min=? AND qty_max IS ?'
        ).get(skuKey, row.mall_group, row.qty_min, row.qty_max ?? null);
        const snapshot = row.current_json ? JSON.parse(row.current_json) : null;
        if (!band) {
          status = 'failed';
          note = '申請後に数量帯の構成が変わっています。再申請してください';
        } else if (snapshot && (band.shipping_method_code !== snapshot.shipping_method_code
            || band.packing_machine_code !== snapshot.packing_machine_code)) {
          status = 'failed';
          note = `申請後にルールが変更されています (現在: ${band.shipping_method_code}/${band.packing_machine_code})。内容を確認して再申請してください`;
        } else {
          const r = bulkUpdateRules({
            items: [{ sku_key: skuKey, mall_group: row.mall_group, qty_min: row.qty_min, qty_max: row.qty_max ?? null }],
            shipping_method_code: row.shipping_method_code,
            packing_machine_code: row.packing_machine_code,
          }, `chat承認:${email}`);
          if (r.updated !== 1) {
            status = 'failed';
            note = '帯の更新に失敗しました (直前に構成が変わった可能性)。再申請してください';
          } else if (r.corrected > 0) {
            note = '梱包機の整合矯正あり (ネコポス以外→手動)';
          }
        }
      } else {
        const combo = row.resolved_key;
        const cur = d.prepare(`
          SELECT shipping_method_code, packing_machine_code FROM pd_assort_decision
          WHERE combo_key=? AND is_active=1 ORDER BY id DESC LIMIT 1
        `).get(combo) || null;
        const snapshot = row.current_json ? JSON.parse(row.current_json) : null;
        const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);
        if (!same(cur, snapshot)) {
          status = 'failed';
          note = '申請後にアソート学習が変更されています。内容を確認して再申請してください';
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
      }
    } catch (e) {
      status = 'failed';
      note = String(e.message).slice(0, 200);
    }
    d.prepare("UPDATE pd_rule_change_request SET status=?, decided_by=?, decided_at=?, decide_note=? WHERE id=?")
      .run(status, email, now, note, id);
    return { ...row, status, decided_by: email, decide_note: note };
  })();
}

// ─── Chat カード ───

const KIND_LABEL = { single: '単品ルール', assort: 'アソート学習' };
const escHtml = (s) => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

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
            { textParagraph: { text: `${escHtml(info.lines)}\n<b>${escHtml(info.band)}</b>\n→ 配送方法: <b>${escHtml(info.smName)}</b> / 梱包機: <b>${escHtml(info.pmName)}</b>` } },
            {
              buttonList: {
                buttons: [
                  // method パラメータ併記: Workspaceアドオン形式のクリックイベントでは
                  // function 名がそのまま届かないことがある (stock-bot/router.js の fn 解決参照)
                  { text: '✅ 承認してDB反映', onClick: { action: { function: 'pdRuleApprove', parameters: [{ key: 'method', value: 'pdRuleApprove' }, { key: 'id', value: String(row.id) }] } } },
                  { text: '却下', onClick: { action: { function: 'pdRuleReject', parameters: [{ key: 'method', value: 'pdRuleReject' }, { key: 'id', value: String(row.id) }] } } },
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
