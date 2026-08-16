/**
 * packing の通知 (GChat webhook・fail-soft)。
 * ④配送方法変更: 通知はチャネルであり DB (pk_pack_ship_changes) が正本 —
 * webhook 成功を業務受付成功とみなさない (要件§5.4 の通知方針と同じ)。
 * env: PACKING_SHIP_CHANGE_WEBHOOK (未設定なら warn のみ)
 */

let _warnedNoWebhook = false;

export async function notifyShipChange({ folderName, neSlipNo, currentMethod, proposedMethod, reason, worker }) {
  const url = process.env.PACKING_SHIP_CHANGE_WEBHOOK;
  if (!url) {
    if (!_warnedNoWebhook) {
      _warnedNoWebhook = true;
      console.warn('[packing-notify] PACKING_SHIP_CHANGE_WEBHOOK 未設定 → 配送方法変更のGChat通知なし (管理画面キューのみ)');
    }
    return false;
  }
  // 読み手 (事務) ファースト: 何をすればいいかが1行目で分かる形 (feedback_gchat_report_reader_first)
  const text = [
    `🚚 *配送方法の変更依頼* — NE・ロジザードの変更と送り状の再発行をお願いします`,
    `伝票: *${neSlipNo}* (${folderName || '-'})`,
    `現行: ${currentMethod || '-'} → 提案: *${proposedMethod}*`,
    `理由: ${reason} / 依頼: ${worker}`,
    `対応状況の更新: https://picking.bfaith-wh.uk/apps/packing/admin/ship-changes`,
  ].join('\n');
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ text }),
  });
  if (!res.ok) throw new Error(`GChat webhook HTTP ${res.status}`);
  return true;
}
