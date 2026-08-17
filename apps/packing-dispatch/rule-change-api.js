/**
 * 配送ルール変更申請の受け口 (miniPC の apps/packing から呼ばれる)。
 * server.js で packing-dispatch 本体 (requireAppAccess) より前に mount する
 * (ne-sync-worker と同じ構成 — セッション外・x-api-key 認証)。
 * env: PD_RULE_CHANGE_KEY (未設定なら 503 fail-closed)
 */
import { Router } from 'express';
import crypto from 'node:crypto';
import { createRuleChangeRequest, applyRuleChangeDecision, getRuleChangeOptions, postApprovalCard } from './rule-change.js';

const router = Router();

function keyAuth(req, res, next) {
  const expected = process.env.PD_RULE_CHANGE_KEY;
  if (!expected) return res.status(503).json({ error: 'PD_RULE_CHANGE_KEY 未設定のため受付できません' });
  const got = String(req.headers['x-api-key'] || '');
  const a = Buffer.from(got);
  const b = Buffer.from(expected);
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  next();
}

router.use(keyAuth);

router.get('/options', (req, res) => {
  try {
    res.json({ ok: true, ...getRuleChangeOptions() });
  } catch (e) {
    console.error('[pd-rule-change] options失敗:', e);
    res.status(500).json({ error: e.message });
  }
});

router.post('/requests', async (req, res) => {
  try {
    const b = req.body || {};
    const row = createRuleChangeRequest({
      kind: b.kind,
      items: b.items,
      mallGroup: b.mall_group,
      qtyMin: b.qty_min == null ? null : Number(b.qty_min),
      qtyMax: b.qty_max == null ? null : Number(b.qty_max),
      shippingMethodCode: b.shipping_method_code,
      packingMachineCode: b.packing_machine_code,
      requestedBy: b.requested_by,
      context: b.context,
    });
    // カード投稿は fail-soft (DBの申請行が正本。失敗しても申請は受理し、cardPosted=false を返す)
    let cardPosted = false;
    try {
      cardPosted = await postApprovalCard(row);
    } catch (e) {
      console.warn(`[pd-rule-change] 承認カード投稿失敗 (#${row.id}): ${e.message}`);
    }
    res.json({ ok: true, id: row.id, cardPosted });
  } catch (e) {
    if (e.code === 'VALIDATION') return res.status(400).json({ error: e.message });
    console.error('[pd-rule-change] 申請失敗:', e);
    res.status(500).json({ error: 'サーバーエラーが発生しました' });
  }
});

export default router;
export { applyRuleChangeDecision };
