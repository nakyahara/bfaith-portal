/**
 * packing の通知 (GChat webhook・fail-soft)。
 * ④配送方法変更: 通知はチャネルであり DB (pk_pack_ship_changes) が正本 —
 * webhook 成功を業務受付成功とみなさない (要件§5.4 の通知方針と同じ)。
 * env: PACKING_SHIP_CHANGE_WEBHOOK (未設定なら warn のみ)
 */

let _warnedNoWebhook = false;

// webhook 送信のタイムアウト (ms)。ポーラー (drive-sync) が直列で待つため、応答しない相手で全体を止めない。テストで短縮可
const WEBHOOK_TIMEOUT_MS = Number(process.env.PACKING_WEBHOOK_TIMEOUT_MS) || 8000;

export async function notifyShipChange({
  folderName, neSlipNo, slipNo = null, currentMethod, proposedMethod, reason, worker, lines = [],
}) {
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
  // ⭐元の送り状を消すのに使う番号を必ず載せる (三宅さん 2026-08-29)。
  //   お客様管理番号 = 出荷伝票NO (SP…) から SP を除いた数字。
  //   実データで一致を確認済み (送り状CSVの お客様管理番号 33/33 が SP番号の数字部分と一致)。
  //   これが無いと事務は名前で探すことになり、同姓や表記ゆれで手間取る
  //   ⚠ 接頭辞を機械的に剥がすと、採番が変わったときに「別の番号」を検索キーとして
  //     出してしまう。いま実データで確認できている SP+数字 の形だけを通す
  const kanri = /^SP(\d+)$/i.exec(String(slipNo ?? '').trim())?.[1] ?? null;
  // ネコポス二枚出し (中原さん指示 2026-09-02): 配送方法は変えず送り状を2枚 (2個口) にする依頼。
  // 事務のやることが違うので1行目を分ける (値 = service.js SHIP_CHANGE_TWO_LABELS。
  // import すると単体テストが service の依存を引き込むため文字列で持つ)
  const twoLabels = proposedMethod === 'ネコポス二枚出し';
  const text = [
    twoLabels
      ? `📦📦 *ネコポス二枚出しの依頼* — ネコポスの送り状を2枚 (2個口) 発行してください (現物は変更待ち棚)`
      : `🚚 *配送方法の変更依頼* — NE・ロジザードの変更と送り状の再発行をお願いします (現物は変更待ち棚)`,
    `伝票: *${neSlipNo}* (${folderName || '-'})`,
    ...(kanri ? [`🔎 元の送り状を消すとき: お客様管理番号 *${kanri}* (出荷伝票NO ${slipNo})`] : []),
    ...lines.map((l) => `・${l.name || l.sku} × ${l.qty}個`),
    twoLabels
      ? `現行: ${currentMethod || '-'} → *ネコポス 2枚出し (2個口)*`
      : `現行: ${currentMethod || '-'} → 提案: *${proposedMethod}*`,
    `理由: ${reason} / 依頼: ${worker}`,
  ].join('\n');
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ text }),
    signal: AbortSignal.timeout(WEBHOOK_TIMEOUT_MS),   // 応答しない webhook でポーラー全体を止めない (Codex R2 High)
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
    signal: AbortSignal.timeout(WEBHOOK_TIMEOUT_MS),   // 応答しない webhook でポーラー全体を止めない (Codex R2 High)
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
    lines.push('📦 ピッキング一覧に「🔴 ピッキング漏れ (梱包から)」バッチを作成しました (計測対象外): https://picking.bfaith-wh.uk/apps/picking/');
  } else {
    // 棚戻しはピッキング一覧の「↩ 棚戻し」カードから (例外処理監査 PR-4)。/tasks は一覧へ転送
    lines.push('↩ ピッキング一覧に「棚戻し」として出ます (戻したロケを記録して事務へ通知): https://picking.bfaith-wh.uk/apps/picking/');
  }
  if (info.stockText) lines.push(info.stockText);   // 棚戻し: 在庫ロケーション (戻し先の参考)
  return postTask(lines.join('\n'));
}

/**
 * ↩ 棚戻し完了 — 3階が「ここへ戻した」を押したとき (Q3 決定 2026-09-05: 戻したロケを記録して事務へ流す)。
 * 余り・品違いの棚戻しは在庫差異の兆候 — 事務はロジザードのロケ在庫と実物のずれをここで直す (例外処理監査 F-2)
 * @param task getTaskDetail の行 (returned_block/returned_location/incident_kind …)
 */
export async function notifyReturned(task, worker, { retry = false } = {}) {
  const reason = task.incident_kind === 'excess' ? '余り (バッチに多く入っていた)'
    : task.incident_kind === 'wrong_item' ? '品違い (間違って入っていた商品)' : '棚戻し';
  const join = (block, loc) => {
    const b = String(block || ''); const l = String(loc || '');
    if (!l) return b || '-';
    return (b && l !== b && !l.startsWith(`${b}-`)) ? `${b}-${l}` : l;
  };
  const returned = join(task.returned_block, task.returned_location);
  const hint = task.location ? join(task.block, task.location) : null;
  const lines = [
    `↩ *棚戻し完了${retry ? ' (通知の再送)' : ''}* — 戻したロケを記録しました (ロジザードのロケ在庫と違えば調整してください)`,
    `商品: *${task.sku}*${task.product_name ? ` (${task.product_name})` : ''} × ${task.req_qty}個`,
    `戻したロケ: *${returned}*${hint && hint !== returned ? ` (候補は ${hint} でした)` : ''}`,
    `理由: ${reason} / 依頼元: ${task.folder_name || task.batch_folder || '-'}${task.slip_seq ? ` #${task.slip_seq}` : ''} (依頼: ${task.requested_by || '-'}) / 戻した: ${task.returned_by || worker || '-'}`,
  ];
  return postTask(lines.join('\n'));
}

export async function notifyTaskUnavailable(task, worker, { remaining = null, altQty = 0 } = {}) {
  return postTask([
    '🚨 *再ピックできません (在庫なし)* — 1階が「在庫なしを確認」すると出荷保留の通知が届きます',
    `商品: *${task.sku}*${task.product_name ? ` (${task.product_name})` : ''} × ${task.req_qty}個 / 依頼元: ${task.folder_name || '-'}${task.slip_seq ? ` #${task.slip_seq}` : ''}`,
    ...(remaining != null && altQty > 0 ? [`うち ${altQty}個 は他ロケで確保して届けます (足りないのは ${remaining}個)`] : []),
    `報告: ${worker}`,
  ].join('\n'));
}

/**
 * 🚫 出荷保留 (在庫なし) — 1階の梱包者が3階の「在庫なし」を確認して伝票を閉じたとき (Q1 決定 2026-09-05 = 案a)。
 * 事務が NE で出荷保留にする起点。宛先 = 配送方法変更と同じ事務スペース (PACKING_SHIP_CHANGE_WEBHOOK)。
 * 通知はチャネル・伝票の状態 (pk_pack_slips cancelled/stockout) が正本
 */
export async function notifyStockout({ folder, slipSeq, neSlipNo, siteOrderNo = null, recipientName = null, worker, items = [] }) {
  const url = process.env.PACKING_SHIP_CHANGE_WEBHOOK;
  if (!url) {
    console.warn('[packing-notify] PACKING_SHIP_CHANGE_WEBHOOK 未設定 → 出荷保留 (在庫なし) の GChat 通知なし');
    return false;
  }
  const hm = (iso) => { const t = Date.parse(iso || ''); return Number.isFinite(t) ? new Date(t + 9 * 3600e3).toISOString().slice(11, 16) : ''; };
  // 商品名・送り先名は改行と装飾記号を落とす (通知の行構造を壊さない — Codex R1 Low)
  const ct = (v) => String(v ?? '-').replace(/[\r\n\t]+/g, ' ').replace(/[*_~`]/g, '').slice(0, 200) || '-';
  const text = [
    '🚫 *出荷保留 (在庫なし)* — NE で出荷保留にして、お客様対応をお願いします (3階に在庫がありませんでした)',
    `伝票: *${ct(neSlipNo)}* (${ct(folder)}${slipSeq ? ` #${slipSeq}` : ''}) / モール伝票番号: ${ct(siteOrderNo)} / 送り先: ${ct(recipientName)}`,
    ...items.map((i) => `・${ct(i.name || i.sku)} (${ct(i.sku)}) × ${i.qty}個 在庫なし${i.delivered > 0 ? ` (${i.delivered}個は他ロケから確保して1階へ)` : ''} — 3階: ${ct(i.claimedBy)} ${hm(i.at)}`),
    `確認: ${ct(worker)} (商品と納品書は出荷保留の棚)`,
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

// 🖨 伝票再印刷依頼 (2026-08-21 中原さん指示)。env: PACKING_REPRINT_WEBHOOK (バックオフィス連絡)。
// 通知はチャネル・DB (pk_pack_reprints) が正本。PDFリンクは抜き出せたときのみ付く
export async function notifyReprint({ kind = 'reprint', folderName, slipSeq, neSlipNo, siteOrderNo, recipientName, worker, lines = [], pdfUrl = null, pdfError = null }) {
  const url = process.env.PACKING_REPRINT_WEBHOOK;
  if (!url) {
    console.warn('[packing-notify] PACKING_REPRINT_WEBHOOK 未設定 → 再印刷通知なし');
    return false;
  }
  const text = [
    kind === 'label_missing'
      ? '📭 *送り状がありませんでした — 印刷をお願いします* (梱包者が束の中に見つけられず)'
      : '🖨 *伝票の再印刷をお願いします*',
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

// ─── ⚠ 取りこぼしの見張り (miss-watch.js) の通知 ───
// env: PACKING_MISS_WEBHOOK → 未設定なら PACKING_SHIP_CHANGE_WEBHOOK へフォールバック。
// 「梱包に来ていない出荷グループがある」は事務・管理が拾うべき知らせなので、
// 配送方法変更と同じスペースで受けられるようにしておく (専用にしたければ env を足すだけ)。
export function missWebhookConfigured() {
  return !!(process.env.PACKING_MISS_WEBHOOK || process.env.PACKING_SHIP_CHANGE_WEBHOOK);
}

export async function postMissText(text) {
  const url = process.env.PACKING_MISS_WEBHOOK || process.env.PACKING_SHIP_CHANGE_WEBHOOK;
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

// ─── 📦 梱包資材の変更通知 (要件『梱包資材表示_要件定義_20260823.md』§5.3) ───
// 送り先 = 「配送ルール承認」スペースの incoming webhook (中原さん決定 2026-08-24: 相乗り)。
// env: PACKING_MATERIAL_WEBHOOK → fallback PACKING_SHIP_CHANGE_WEBHOOK。
// 両方未設定 = 構成エラー (outbox は claim しない — 管理画面に常時表示)

export function materialWebhookConfigured() {
  return !!(process.env.PACKING_MATERIAL_WEBHOOK || process.env.PACKING_SHIP_CHANGE_WEBHOOK);
}

/** outbox の sendFn。true=送信成功 / throw=失敗 (outbox がバックオフ再試行)。 */
export async function postMaterialText(text) {
  const url = process.env.PACKING_MATERIAL_WEBHOOK || process.env.PACKING_SHIP_CHANGE_WEBHOOK;
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
