/**
 * packing の通知 (GChat webhook・fail-soft)。
 * ④配送方法変更: 通知はチャネルであり DB (pk_pack_ship_changes) が正本 —
 * webhook 成功を業務受付成功とみなさない (要件§5.4 の通知方針と同じ)。
 * env: PACKING_SHIP_CHANGE_WEBHOOK (未設定なら warn のみ)
 */

let _warnedNoWebhook = false;

export async function notifyShipChange({ folderName, neSlipNo, currentMethod, proposedMethod, reason, worker, lines = [] }) {
  const url = process.env.PACKING_SHIP_CHANGE_WEBHOOK;
  if (!url) {
    if (!_warnedNoWebhook) {
      _warnedNoWebhook = true;
      console.warn('[packing-notify] PACKING_SHIP_CHANGE_WEBHOOK 未設定 → 配送方法変更のGChat通知なし (管理画面キューのみ)');
    }
    return false;
  }
  // 読み手 (事務) ファースト: 何をすればいいかが1行目で分かる形 (feedback_gchat_report_reader_first)。
  // 現物は「変更待ちの棚」にある — 画面での状態管理はしない (中原さん指示 2026-08-17)
  const text = [
    `🚚 *配送方法の変更依頼* — NE・ロジザードの変更と送り状の再発行をお願いします (現物は変更待ち棚)`,
    `伝票: *${neSlipNo}* (${folderName || '-'})`,
    ...lines.map((l) => `・${l.name || l.sku} × ${l.qty}個`),
    `現行: ${currentMethod || '-'} → 提案: *${proposedMethod}*`,
    `理由: ${reason} / 依頼: ${worker}`,
  ].join('\n');
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ text }),
  });
  if (!res.ok) throw new Error(`GChat webhook HTTP ${res.status}`);
  return true;
}

// ①再ピック / ②棚戻し / 品違いの現場通知。env: PACKING_TASK_WEBHOOK
// (未設定時は PACKING_SHIP_CHANGE_WEBHOOK へフォールバック — 同じ事務/管理スペース運用を想定)
let _warnedNoTaskWebhook = false;
async function postTask(text) {
  const url = process.env.PACKING_TASK_WEBHOOK || process.env.PACKING_SHIP_CHANGE_WEBHOOK;
  if (!url) {
    if (!_warnedNoTaskWebhook) {
      _warnedNoTaskWebhook = true;
      console.warn('[packing-notify] PACKING_TASK_WEBHOOK 未設定 → 再ピック/棚戻しのGChat通知なし (pickingキュー画面のみ)');
    }
    return false;
  }
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ text }),
  });
  if (!res.ok) throw new Error(`GChat webhook HTTP ${res.status}`);
  return true;
}

export async function notifyTask(info, worker) {
  const head = {
    repick: '🔴 *再ピック依頼* — 棚から取ってきてください',
    return: '↩ *棚戻し依頼* — 余った商品を棚へ (在庫差異の可能性あり・ロジザード在庫も確認)',
    wrong_item: '🟠 *品違い* — 正しい商品の再ピック+間違い品の棚戻し',
  }[info.kind] || info.kind;
  const lines = [
    head,
    `商品: *${info.sku}*${info.name ? ` (${info.name})` : ''} × ${info.qty}個`,
  ];
  if (info.actualSku) lines.push(`間違って入っていた: ${info.actualSku}`);
  if (info.locationLabel) lines.push(`ロケ: ${info.locationLabel}`);
  lines.push(`依頼元: ${info.folder || '-'}${info.slipSeq ? ` #${info.slipSeq}` : ''} / 依頼: ${worker}`);
  if (info.kind === 'repick' && info.repickBatchId) {
    lines.push('📦 ピッキング一覧に「🔴ピッキング漏れ」バッチを作成しました (計測対象外): https://picking.bfaith-wh.uk/apps/picking/');
  } else {
    lines.push('ピッキングのタスク画面: https://picking.bfaith-wh.uk/apps/picking/tasks');
  }
  if (info.stockText) lines.push(info.stockText);   // 棚戻し: 在庫ロケーション (戻し先の参考)
  return postTask(lines.join('\n'));
}

export async function notifyTaskUnavailable(task, worker) {
  return postTask([
    '🚨 *再ピックできません (在庫なし等)* — 出荷可否の判断が必要です',
    `商品: *${task.sku}* × ${task.req_qty}個 / 依頼元: ${task.folder_name || '-'}${task.slip_seq ? ` #${task.slip_seq}` : ''}`,
    `報告: ${worker}`,
  ].join('\n'));
}

// 🖨 伝票再印刷依頼 (2026-08-21 中原さん指示)。env: PACKING_REPRINT_WEBHOOK (バックオフィス連絡)。
// 通知はチャネル・DB (pk_pack_reprints) が正本。PDFリンクは抜き出せたときのみ付く
export async function notifyReprint({ folderName, slipSeq, neSlipNo, siteOrderNo, recipientName, worker, lines = [], pdfUrl = null, pdfError = null }) {
  const url = process.env.PACKING_REPRINT_WEBHOOK;
  if (!url) {
    console.warn('[packing-notify] PACKING_REPRINT_WEBHOOK 未設定 → 再印刷通知なし');
    return false;
  }
  const text = [
    '🖨 *伝票の再印刷をお願いします*',
    `NE伝票番号: *${neSlipNo}* / モール伝票番号: ${siteOrderNo || '-'}`,
    `出荷NO: ${folderName || '-'}${slipSeq ? ` #${slipSeq}` : ''} / 送り先: ${recipientName || '-'} / 依頼: ${worker}`,
    ...lines.map((l) => `・${l.name || l.sku} × ${l.qty}個`),
    pdfUrl ? `📄 送り状PDF (該当ページのみ): ${pdfUrl}`
      : pdfError ? `⚠ 送り状PDFの自動抜き出しはできませんでした (${pdfError}) — フォルダから該当分を印刷してください`
        : '📄 送り状PDFを抜き出し中です (できたらこのスペースに追送します)',
  ].join('\n');
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ text }),
    signal: AbortSignal.timeout(8000),
  });
  if (!res.ok) throw new Error(`GChat webhook HTTP ${res.status}`);
  return true;
}

/** 再印刷スペースへの追送 (PDFリンク等の短文)。env未設定はfalse。 */
export async function postReprintText(text) {
  const url = process.env.PACKING_REPRINT_WEBHOOK;
  if (!url) return false;
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ text }),
    signal: AbortSignal.timeout(8000),
  });
  if (!res.ok) throw new Error(`GChat webhook HTTP ${res.status}`);
  return true;
}
