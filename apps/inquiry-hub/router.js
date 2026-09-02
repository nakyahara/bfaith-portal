/**
 * EC問い合わせ管理システム (inquiry-hub) — Step 1: 一覧/詳細画面 (read-only + 社内操作)
 *
 * メールディーラー置き換えの自社問い合わせ管理基盤。
 * 設計書: AI_reference/システム設計/問い合わせ管理システム_設計書_v1.2_20260716.md
 *
 * Step 1 スコープ (設計書§11):
 *   GET  /                        問い合わせ一覧 (フィルタ・検索)
 *   GET  /inquiries/:id           問い合わせ詳細 (スレッド・顧客情報・メモ・履歴)
 *   POST /api/inquiries/:id/status     社内ステータス変更
 *   POST /api/inquiries/:id/assign     担当者設定
 *   POST /api/inquiries/:id/customer-info  顧客情報の手入力 (注文番号+モール・商品。自動保存。確定情報はロック)
 *   POST /api/inquiries/:id/read       社内既読/未読切替
 *   POST /api/inquiries/:id/attention  要確認フラグ切替
 *   POST /api/inquiries/:id/ai-flag    AIフラグ設定 (0:不要 1:AI返信 2:社長確認 3:責任者確認)
 *   POST /api/inquiries/:id/notes      社内メモ追加
 *
 * 外部API同期 (Step 2)・返信送信 (Step 3〜5)・ロック (Step 6)・AI (Step 7) は未実装。
 * external_status / 最終同期日時 は表示のみ (同期実装前は seed 値がそのまま出る)。
 */
import crypto from 'crypto';
import express, { Router } from 'express';
import { getDB, logActivity } from './db.js';
import { CHANNELS, CHANNEL_GROUPS, STATUSES, AI_FLAGS, PAGE_SIZE, VIEWS, DEFAULT_VIEW, BULK_MAX, FILTER_BULK_MAX, listInquiries, listFilterOptions, countByView, countInboxByGroup, countViewsInContext, bulkUpdateInquiries, listInquiryIdsByFilter, getInquiryDetail, getAdjacentInquiries } from './queries.js';
import { importTemplatesCsv, importQaCsv, listTemplates, listQa } from './templates.js';
import { aiRewriteEnabled, rewriteReply, draftReply, REWRITE_STYLES } from './ai-rewrite.js';
import { findRelevantKnowledge, formatKnowledgeForPrompt } from './knowledge.js';
import { runSync, listSyncStatus } from './sync/engine.js';
import { DEFAULT_FROM_ADDRESS, DEFAULT_FROM_NAME } from './sync/adapters/gmail.js';
import { buildAdapterForShop, refreshShopAuthStatus } from './sync/cron.js';
import { listOutboxIssues, resolveUnknown, cancelJob, createReplyJob } from './outbox.js';
import { listFolders, countUnfiled, createFolder, updateFolder, deleteFolder, setInquiryFolder,
  FOLDER_NAME_MAX } from './folders.js';
import { listLabels, createLabel, updateLabel, deleteLabel, setInquiryLabel,
  labelTextColor, LABEL_NAME_MAX, LABEL_PALETTE } from './labels.js';
import { listQuickLinks, createQuickLink, updateQuickLink, deleteQuickLink,
  LINK_NAME_MAX, LINK_URL_MAX, MAX_ACTIVE_LINKS } from './links.js';
import { listBulkBatches, revertBulkBatch, BATCH_SOURCE_LABELS } from './batches.js';
import { CUSTOMER_INFO_FIELDS, CUSTOMER_INFO_KEYS, ORDER_MALLS, ORDER_MALL_KEYS, customerInfoState, setCustomerInfo, orderLinksOf } from './customer-info.js';
import { getAttachmentContext, fetchAttachmentBody, contentDispositionValue } from './attachments.js';
import { saveReplyAttachment, listPendingAttachments, deletePendingAttachment,
  MAX_FILE_BYTES, MAX_FILES_PER_REPLY, ALLOWED_LABEL } from './reply-attachments.js';
import { isImage, isInlineSafe, fmtBytes, resolveContentType } from './mime.js';
import { blockedReplyDestination } from './no-reply.js';
import { checkRecipientDomain } from './mx-check.js';
import { linkifyText, urlSafeCut } from './linkify.js';
import { listMailRules, addMailRule, setMailRuleActive, deleteMailRule, evaluateMailRules, importMailDealerRulesCsv,
  applyRuleToExistingMails, canApplyToExisting, validateConditions } from './mail-rules.js';
import { listMailShops, resolveMailShop, createComposeDraft, finalizeComposeDraft,
  pruneStaleComposeDrafts, validateBody, SUBJECT_MAX, BODY_MAX } from './compose.js';
import { listSignatures, getSignature, getDefaultSignature, createSignature, updateSignature,
  deleteSignature, composeBodyWithSignature, SIGNATURE_NAME_MAX, SIGNATURE_BODY_MAX } from './signatures.js';
import { getPermissionMatrix, createStaff, saveStaffWithPermissions, deactivateStaff, refundLimitOf,
  createPermission, updatePermission, setPermissionActive, deletePermission, listPermissionLogs,
  STAFF_NAME_MAX, STAFF_KEY_MAX, PERM_NAME_MAX, PERM_CODE_MAX } from './staff.js';
import { collectInsights, estimateCost, WMS_CUTOFFS } from './insights.js';
import { listCutoffItems, countCutoffItems, summarize, ackCutoffItem, unackCutoffItem,
  listCutoffExcludes, addCutoffExclude, removeCutoffExclude,
  nextCutoff, CUTOFF_KINDS, CUTOFF_TIMES, LOOKBACK_DAYS } from './cutoff.js';
import { CASE_TYPES, STAGES, WAITING_ON, NECESSITY, PROGRESS, CLOSE_REASONS,
  createCase, getCase, listSteps, listCaseInquiries, listCasesForInquiry, listEvents,
  listBoardCases, boardColumns, nextStepOf, blockersOf, stepLabel, updateStep, setWaiting,
  setAssignee, setRefund, closeCase, reopenCase, linkInquiry, countOpenCases,
  detectCaseKeywords, getTriage, setTriage, clearTriage, overdueDays, businessDaysFromNow, canDoException,
  STEP_TEMPLATES, jstDate as rcJstDate } from './return-cases.js';
import { toPreviewLine } from './text-utils.js';

// 返信エディタの Dark Launch フラグ (送信ワーカー稼働前にスタッフが誤って「送信したつもり」に
// ならないよう、既定は非表示。動作確認・Step 3 送信開始時に env で有効化する)
const replyEditorEnabled = () => {
  const v = process.env.INQUIRY_HUB_REPLY_EDITOR_ENABLED;
  return v === 'true' || v === '1';
};

const router = Router();

// ─── 共通ヘルパ ───
const he = s => String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
/** 宛先ドメインが「確実に受け取れない」ときだけ理由を返す (それ以外は null = 送信を止めない)。
 * 確認自体が失敗しても null を返す — DNSの不調で正常な送信を止めないため (mx-check.js) */
async function recipientDomainProblem(address) {
  try {
    const v = await checkRecipientDomain(address);
    return v.ok ? null : v.reason;
  } catch (e) {
    console.warn(`[inquiry-hub] 宛先ドメインの事前確認をスキップ: ${e?.message || e}`);
    return null;
  }
}
const actorOf = req => (req.session && (req.session.email || req.session.displayName)) || 'portal';

function fmtJst(iso) {
  if (!iso) return '—';
  const t = Date.parse(iso.includes('T') || iso.includes('Z') ? iso : iso.replace(' ', 'T') + 'Z');
  if (Number.isNaN(t)) return he(iso);
  const j = new Date(t + 9 * 3600 * 1000);
  const p = n => String(n).padStart(2, '0');
  return `${j.getUTCFullYear()}-${p(j.getUTCMonth() + 1)}-${p(j.getUTCDate())} ${p(j.getUTCHours())}:${p(j.getUTCMinutes())}`;
}

/** 本文表示の正規化: 行末の空白除去 + 3行以上の連続空行を1行に詰める。
 * 自動配信メールは空行を大量に含み、そのまま<br>にすると画面が延々と間延びする (2026-07-25 実測) */
export function normalizeBodyText(text) {
  return String(text || '')
    .replace(/\r\n?/g, '\n')
    .replace(/[ \t　]+$/gm, '')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

const badge = (meta, text) => meta ? `<span class="badge" style="${meta.badge}">${he(text != null ? text : meta.label)}</span>` : '';
const chBadge = ch => badge(CHANNELS[ch], null) || he(ch);
const stBadge = st => badge(STATUSES[st], null) || he(st);
/**
 * 一覧の本文プレビュー (2026-08-25)。件名だけでは仕分けできないため最新の顧客メッセージ冒頭を出す。
 * 引用 (>、"On … wrote:"、「〜さんは書きました」)・区切り線以降・URL羅列を落として要点だけ残す。
 * 表示は1行 (CSSで省略) なので改行は空白に畳む
 */
export function previewOf(text, max = 110) {
  return toPreviewLine(text, max);
}

/** 色付きラベルチップ (2026-08-24 メールディーラーのラベル相当)。color は labels.js が '#rrggbb' に検証済み */
const labelChip = (name, color) => name
  ? `<span class="lbl" style="background:${he(color || '#64748b')};color:${labelTextColor(color)}">${he(name)}</span>` : '';

// ─── 対応履歴の表示整形 (生JSONではなく日本語ラベルで出す) ───
const ACTION_LABELS = {
  status_change: '状態変更', assign: '担当変更', note_add: 'メモ追加', read_toggle: '既読/未読',
  folder_change: 'フォルダ変更', label_change: 'ラベル変更', bulk_revert: '一括操作の取り消し',
  attention_toggle: '要確認フラグ', ai_flag: 'AIフラグ', seed: 'テストデータ投入',
  reply_created: '返信ジョブ作成', reply_resolved: '送信結果の解決', reply_cancelled: '送信ジョブ取消',
  delivery_failed: '🔴配信失敗を検知', customer_info_edit: '顧客情報の編集',
};
function fmtLogValue(key, v) {
  switch (key) {
    case 'status': return (STATUSES[v] || {}).label || String(v);
    case 'order_number': case 'product_code': case 'product_name':
      return `${CUSTOMER_INFO_FIELDS[key].label}=${v == null || v === '' ? '(空)' : String(v)}`;
    case 'order_mall': return `モール=${v ? ((ORDER_MALLS[v] || {}).label || String(v)) : '(未選択)'}`;
    case 'is_unread': return v ? '未読' : '既読';
    case 'needs_attention': return v ? '⚠️要確認ON' : '要確認OFF';
    case 'ai_needed': return Number(v) === 0 ? 'AI不要' : ((AI_FLAGS[v] || {}).label || String(v));
    case 'assigned': return v ? String(v) : '未割当';
    case 'folder': return v ? `📁${v}` : '未分類';
    case 'label': return v ? `🏷️${v}` : 'ラベルなし';
    case 'length': return `${v}文字`;
    case 'source': return String(v);
    default: return typeof v === 'object' ? JSON.stringify(v) : String(v);
  }
}
/** before_json/after_json → 「対応中」「未読」等の表示文字列。null入力はnull返し */
function fmtLogJson(json) {
  if (!json) return null;
  try {
    const o = JSON.parse(json);
    return Object.entries(o).map(([k, v]) => fmtLogValue(k, v)).join(', ');
  } catch { return json; } // 表示用なので壊れたJSONはそのまま出す
}

// 注文リンク (NE個別受注明細 + モール側の注文詳細) の計算は customer-info.js の orderLinksOf() に集約
// (2026-08-16 中原さん要望 → 2026-08-31 手入力のモール選択に対応してモジュール側へ移動)

// ─── 一覧画面 ───
router.get('/', (req, res) => {
  const q = req.query || {};
  const kw = String(q.q || '').trim();
  const { rows, total, page, pages, view } = listInquiries(q);
  const { shops, assignees, countMap } = listFilterOptions();
  // 任意フォルダ (サイドバーから ?folder=<id> で来る。ビュー条件とはANDで重なる)
  const folders = listFolders();
  const curFolder = /^\d+$/.test(String(q.folder || '')) ? folders.find(f => f.id === Number(q.folder)) : null;
  // 色付きラベル (絞り込み・一括操作・行チップ表示。2026-08-24)
  const labels = listLabels();
  // 上部タブ (メール / モール問い合わせ)。ビューやフォルダとはANDで重なる
  const group = CHANNEL_GROUPS[q.group] ? q.group : '';

  // ─── 上部タブ: すべて / メール / モール問い合わせ (2026-08-15 スタッフ要望) ───
  // どのビュー・画面からでも「いまメール/モールに新着が何件あるか」が見え、1クリックで切り替える。
  // バッジは常に「新着 (受信トレイ)」の件数 = いま自分が対応すべき件数 (表示中のビューとは独立)。
  // タブ切替では channel (個別チャネルの絞り込み) と page をリセットし、他の条件は維持する
  let inboxCounts = {};
  try { inboxCounts = countInboxByGroup(); } catch { /* 初期化前などは件数なしで表示 */ }
  const tabLink = (g) => {
    const u = new URLSearchParams(Object.entries(q).filter(([k, v]) => !['group', 'channel', 'page'].includes(k) && v !== '' && v != null));
    if (g) u.set('group', g);
    const qs = u.toString();
    return `/apps/inquiry-hub${qs ? `?${qs}` : ''}`;
  };
  const tabCnt = (n) => `<span class="tab-cnt${n ? '' : ' zero'}" title="新着 (受信トレイ) の件数">新着${n || 0}</span>`;
  // ─── モール等への外部リンク (2026-08-25 中原さん要望。🔗リンク管理で登録した分を自動で出す) ───
  // このアプリで完結しない操作 (モール独自機能・同期を待たず直接見る) の逃げ道。
  // 楽天R-Messe / Yahoo!ストアクリエイターPro / Gmail は初回に既定として入る (db.js)。
  // 以後の追加・変更は画面から (コードに直書きしない)
  const quickLinks = listQuickLinks();
  const mallLinks = quickLinks.length
    ? `<span class="mall-links">${quickLinks.map(l =>
      `<a href="${he(l.url)}" target="_blank" rel="noopener" title="${he(l.name)} を新しいタブで開きます (${he(l.url)})">${he(l.icon || '🔗')} ${he(l.name)} ↗</a>`).join('')}</span>`
    : '';
  const chTabs = `
  <nav class="ch-tabs">
    <a class="${group === '' ? 'on' : ''}" href="${he(tabLink(''))}">🗂️ すべて ${tabCnt(inboxCounts.all)}</a>
    ${Object.entries(CHANNEL_GROUPS).map(([key, g]) =>
      `<a class="${group === key ? 'on' : ''}" href="${he(tabLink(key))}">${g.icon} ${he(g.label)} ${tabCnt(inboxCounts[key])}</a>`).join('')}
    ${mallLinks}
  </nav>`;

  // ─── 状態タブ (2026-08-17 スタッフ要望: モール問い合わせの中でも 新着/返信処理中/完了 を切替) ───
  // 件数は「いま見ている文脈 (チャネルグループ・フォルダ) の中での件数」= タブを押した先の実件数。
  // 切替では view と page だけ変え、他の絞り込みは維持する
  let viewCounts = {};
  try { viewCounts = countViewsInContext({ group, folder: q.folder }); } catch { /* 初期化前は件数なし */ }
  const viewLink = (v) => {
    const u = new URLSearchParams(Object.entries(q).filter(([k, val]) => !['view', 'page'].includes(k) && val !== '' && val != null));
    u.set('view', v);
    return `/apps/inquiry-hub?${u.toString()}`;
  };
  const viewTabs = `
  <nav class="view-tabs">
    ${Object.entries(VIEWS).map(([key, v]) => {
      const n = viewCounts[key];
      return `<a class="${view === key ? 'on' : ''}" href="${he(viewLink(key))}" title="${he(v.hint)}">${v.icon} ${he(v.label)}<span class="vt-cnt${n ? '' : ' zero'}">${n || 0}</span></a>`;
    }).join('')}
  </nav>`;

  // ─── クイック入口 (2026-08-25): 「今日の新着」と「古い滞留」を同じ入口に置かない ───
  // 6,400件の滞留の中から毎回新着を探すのは現実的でないため、よく使う出発点を1タップで出す。
  // 既存の絞り込みパラメータ (from/to/attention/assigned) の組み合わせなので、押した後も
  // 通常の絞り込みとして編集できる (専用の隠しモードを作らない)
  const jstDate = (offsetDays = 0) => {
    const j = new Date(Date.now() + 9 * 3600e3 + offsetDays * 86400e3);
    return `${j.getUTCFullYear()}-${String(j.getUTCMonth() + 1).padStart(2, '0')}-${String(j.getUTCDate()).padStart(2, '0')}`;
  };
  const me = String(actorOf(req));
  const quickEntries = [
    { key: 'recent', icon: '🆕', label: '今日・昨日の新着', params: { view: 'inbox', from: jstDate(-1) },
      title: '直近2日に届いた未対応。まずはここから' },
    { key: 'attention', icon: '⚠️', label: '要確認', params: { view: 'inbox', attention: '1' },
      title: '⚠️要確認を付けた未対応' },
    { key: 'mine', icon: '🙋', label: '自分の対応中', params: { view: 'inbox', assigned: me },
      title: `担当が ${me} の未対応` },
    { key: 'unassigned', icon: '📭', label: '未割当', params: { view: 'inbox', assigned: 'none' },
      title: '担当が決まっていない未対応' },
    { key: 'backlog', icon: '🗄️', label: '14日以前の滞留', params: { view: 'inbox', to: jstDate(-14) },
      title: '14日より前に届いた未対応 (滞留整理用。新着とは別の作業として扱う)' },
  ];
  const quickBar = `
  <nav class="quick-bar" aria-label="よく使う絞り込み">
    <span class="qb-label">よく使う:</span>
    ${quickEntries.map(e => {
      // 押した状態の判定は「その入口のパラメータが今の絞り込みに全部入っているか」
      const on = Object.entries(e.params).every(([k, v]) => String(q[k] ?? (k === 'view' ? view : '')) === String(v));
      const u = new URLSearchParams();
      if (group) u.set('group', group);
      if (q.folder) u.set('folder', String(q.folder));
      for (const [k, v] of Object.entries(e.params)) u.set(k, v);
      return `<a class="${on ? 'on' : ''}" href="/apps/inquiry-hub?${u.toString()}" title="${he(e.title)}">${e.icon} ${he(e.label)}</a>`;
    }).join('')}
  </nav>`;

  const opt = (v, label, cur) => `<option value="${he(v)}"${String(cur || '') === String(v) ? ' selected' : ''}>${he(label)}</option>`;
  // 絞り込み: PCは常時展開、スマホは折りたたみ (CSS details.fbox。絞り込み中は開いた状態で表示)
  // view/page はビュー切替・ページ送りであって「絞り込み条件」ではない (これを条件扱いすると
  // スマホで絞り込みが常に開いてしまう)
  const filtering = Object.entries(q).some(([k, v]) => !['page', 'view'].includes(k) && v !== '' && v != null);
  const filterBar = `
  <details class="fbox" open>
  <summary>🔍 絞り込み${filtering ? ' <span class="badge" style="background:#dbeafe;color:#1d4ed8">条件あり</span>' : ''}</summary>
  <div class="fbody">
  <form method="get" class="filters">
    <input type="hidden" name="view" value="${he(view)}">
    ${group ? `<input type="hidden" name="group" value="${he(group)}">` : ''}
    <select name="status"><option value="">状態: 全て</option>${Object.entries(STATUSES).map(([k, v]) => opt(k, `${v.label} (${countMap[k] || 0})`, q.status)).join('')}</select>
    <select name="channel"><option value="">チャネル: 全て</option>${Object.entries(CHANNELS).map(([k, v]) => opt(k, v.label, q.channel)).join('')}</select>
    <select name="shop"><option value="">店舗: 全て</option>${shops.map(s => opt(s.id, `${(CHANNELS[s.channel_type] || {}).label || s.channel_type} / ${s.shop_name}`, q.shop)).join('')}</select>
    <select name="folder"><option value="">フォルダ: 全て</option>${opt('none', '未分類', q.folder)}${folders.map(f => opt(f.id, `📁 ${f.name}`, q.folder)).join('')}</select>
    <select name="label"><option value="">ラベル: 全て</option>${opt('none', 'ラベルなし', q.label)}${labels.map(l => opt(l.id, `🏷️ ${l.name}`, q.label)).join('')}</select>
    <select name="assigned"><option value="">担当: 全て</option>${opt('none', '未割当', q.assigned)}${assignees.map(u => opt(u, u, q.assigned)).join('')}</select>
    <label class="chk"><input type="checkbox" name="unread" value="1"${q.unread === '1' ? ' checked' : ''}>未読</label>
    <label class="chk"><input type="checkbox" name="attention" value="1"${q.attention === '1' ? ' checked' : ''}>要確認</label>
    <label class="chk"><input type="checkbox" name="ai" value="1"${q.ai === '1' ? ' checked' : ''}>AIフラグ</label>
    <input type="date" name="from" value="${he(q.from || '')}" title="受信日 From">〜<input type="date" name="to" value="${he(q.to || '')}" title="受信日 To">
    <input type="search" name="q" value="${he(kw)}" placeholder="顧客名/件名/本文/注文番号/商品コード" style="min-width:240px">
    <button class="pri">検索</button>
    <a href="/apps/inquiry-hub?view=${he(view)}${curFolder ? `&folder=${curFolder.id}` : ''}${group ? `&group=${he(group)}` : ''}" class="ghost btn-link">クリア</a>
  </form>
  </div>
  </details>`;

  // 送信済みビュー: 返事を待っている日数を出す (返信したまま放置される案件を拾えるように)
  const waitingLabel = (r) => {
    // last_message_at_actual = メッセージ本体から取った実際の最終送信日時
    // (inquiries.last_message_at は手動の送信確定で更新されないため。Codexレビュー反映)
    const t = Date.parse(r.last_message_at_actual || r.last_message_at || r.received_at);
    if (!Number.isFinite(t)) return '返信待ち';
    const days = Math.floor((Date.now() - t) / 86400000);
    if (days >= 7) return `<span style="color:#b91c1c;font-weight:700">⏳ ${days}日 返信なし</span>`;
    if (days >= 3) return `<span style="color:#92400e">⏳ ${days}日 返信なし</span>`;
    return `返信待ち (${days === 0 ? '本日' : days + '日'})`;
  };

  // 受信日時セルの2行目 (2026-08-26 中原さん要望で受信日時を左側=常に見える位置へ移動)。
  // 「更新」は初回受信と最終メッセージが違うときだけ出す (1通だけの行を2行に膨らませない)
  const dtSub = (r) => {
    if (view === 'sent') return waitingLabel(r);
    const upd = fmtJst(r.last_message_at || r.received_at);
    return upd === fmtJst(r.received_at) ? '' : `更新 ${upd}`;
  };

  // 詳細画面へ引き継ぐ文脈 (戻るリンク・前後ナビが同じ一覧に沿う)
  const detailQs = `?view=${view}${curFolder ? `&folder=${curFolder.id}` : ''}${group ? `&group=${group}` : ''}`;
  // data-label / data-full = スマホでのカード表示用 (CSS table.cardable。PC表示には影響しない)
  const trs = rows.map(r => `
    <tr class="${r.is_unread ? 'unread' : ''}" onclick="location.href='/apps/inquiry-hub/inquiries/${r.id}${detailQs}'">
      <td class="selcell" onclick="event.stopPropagation()"><input type="checkbox" class="rowchk" value="${r.id}" aria-label="選択"></td>
      <td>${chBadge(r.channel_type)}<div class="sub">${he(r.shop_name)}</div></td>
      <td>${stBadge(r.internal_status)}${r.delivery_failed_at ? ' <span class="badge" style="background:#b91c1c;color:#fff" title="返信メールが宛先に届きませんでした">🔴配信失敗</span>' : ''}${r.needs_attention ? ' <span class="badge" style="background:#fee2e2;color:#b91c1c">⚠️要確認</span>' : ''}${r.is_unread ? ' <span class="dot" title="未読"></span>' : ''}</td>
      <td class="nowrap dtcol" data-label="受信日時">${fmtJst(r.received_at)}${dtSub(r) ? `<div class="sub">${dtSub(r)}</div>` : ''}</td>
      <td class="nowrap" data-label="ラベル"${r.label_name ? '' : ' data-empty'}>${r.label_name ? labelChip(r.label_name, r.label_color) : '—'}</td>
      <td class="nowrap" data-label="担当"${r.assigned_user_id ? '' : ' data-empty'}>${he(r.assigned_user_id || '—')}</td>
      <td class="nowrap" data-label="AI"${r.ai_needed ? '' : ' data-empty'}>${r.ai_needed ? badge(AI_FLAGS[r.ai_needed], null) : '—'}</td>
      <td class="subj" data-full><a href="/apps/inquiry-hub/inquiries/${r.id}${detailQs}">${he(r.subject || '(件名なし)')}</a>
        ${(() => { const p = previewOf(r.last_incoming_body); return p ? `<div class="preview" title="${he(p)}">${he(p)}</div>` : ''; })()}
        <div class="sub">${he(r.customer_name || '')}${r.customer_identifier ? ' &lt;' + he(r.customer_identifier) + '&gt;' : ''} ・ ${r.msg_count}通${r.folder_name ? ` ・ <span class="folder-chip">📁${he(r.folder_name)}</span>` : ''}</div></td>
      <td data-full data-label="注文 / 商品"${r.order_number || r.product_name || r.product_code ? '' : ' data-empty'}>${r.order_number ? he(r.order_number) : '—'}<div class="sub">${he(r.product_name || r.product_code || '')}</div></td>
    </tr>`).join('');

  const pageLink = p => {
    const u = new URLSearchParams(Object.entries(q).filter(([, v]) => v !== '' && v != null));
    u.set('view', view);
    u.set('page', p);
    return `/apps/inquiry-hub?${u.toString()}`;
  };
  const pager = pages > 1 ? `<div class="pager">
    ${page > 1 ? `<a href="${he(pageLink(page - 1))}">← 前</a>` : ''}
    <span>${page} / ${pages} ページ (全${total}件)</span>
    ${page < pages ? `<a href="${he(pageLink(page + 1))}">次 →</a>` : ''}</div>` : `<div class="pager"><span>全${total}件</span></div>`;

  const emptyMsg = curFolder ? `フォルダ「${curFolder.name}」に該当する問い合わせはありません` : {
    inbox: '受信トレイは空です 🎉 (返信が必要な問い合わせはありません)',
    sent: '返事待ちの案件はありません',
    done: '対応済みの問い合わせはまだありません',
    all: '問い合わせがありません',
  }[view];
  // フォルダを開いているときはフォルダ名を主役にする (ビューはAND条件として併記)
  const hintLine = curFolder
    ? `📁 <b>${he(curFolder.name)}</b> — このフォルダに入れた問い合わせ${q.folder === 'none' ? '' : ''}
       <span class="sub">(${he(VIEWS[view].label)}で絞り込み中)</span>`
    : q.folder === 'none'
      ? `🗃️ <b>未分類</b> — フォルダに入れていない問い合わせ <span class="sub">(${he(VIEWS[view].label)}で絞り込み中)</span>`
      : `${VIEWS[view].icon} <b>${he(VIEWS[view].label)}</b> — ${he(VIEWS[view].hint)}`;
  // ─── 一括操作バー (2026-08-17 スタッフ要望。メールディーラー相当) ───
  // チェックした行に対して 状態/フォルダ/担当/既読 をまとめて変更する。削除は提供しない
  // (取り消せない操作は一括で出さない)。実行前に必ず確認ダイアログを出す
  const bulkBar = `
  <div class="bulkbar" id="bulkBar" hidden>
    <span class="bulk-n"><b id="bulkCount">0</b>件を選択中</span>
    <select id="bulkStatus"><option value="">状態: 変更なし</option>${Object.entries(STATUSES).map(([k, v]) => `<option value="${k}">${he(v.label)}にする</option>`).join('')}</select>
    <select id="bulkFolder"><option value="">フォルダ: 変更なし</option><option value="none">📁 未分類に戻す</option>${folders.map(f => `<option value="${f.id}">📁 ${he(f.name)}へ</option>`).join('')}</select>
    <select id="bulkLabel"><option value="">ラベル: 変更なし</option><option value="none">🏷️ ラベルを外す</option>${labels.map(l => `<option value="${l.id}">🏷️ ${he(l.name)}</option>`).join('')}</select>
    <select id="bulkAssign"><option value="">担当: 変更なし</option><option value="__me__">自分にする</option><option value="__none__">未割当にする</option>${assignees.map(u => `<option value="${he(u)}">${he(u)}</option>`).join('')}</select>
    <select id="bulkRead"><option value="">既読: 変更なし</option><option value="read">既読にする</option><option value="unread">未読にする</option></select>
    <button class="pri" id="bulkApply">選択した${''}件に適用</button>
    <button class="ghost" id="bulkAll" hidden title="いまの検索条件・タブに一致する全件 (他のページの分も含む) を対象にします">📋 この条件の全${total}件を選択</button>
    <button class="ghost" id="bulkClear">選択を解除</button>
  </div>`;

  const body = `
  ${chTabs}
  ${viewTabs}
  <div class="view-hint">${hintLine}</div>
  ${quickBar}
  ${filterBar}
  ${bulkBar}
  <div class="card">
    <table class="cardable">
      <thead><tr><th class="selcell"><input type="checkbox" id="chkAll" aria-label="すべて選択"></th><th>チャネル/店舗</th><th>状態</th><th class="dtcol">${view === 'sent' ? '受信日時 / 返信待ち' : '受信日時'}</th><th>ラベル</th><th>担当</th><th>AI</th><th>件名 / 顧客</th><th>注文 / 商品</th></tr></thead>
      <tbody>${trs || `<tr><td colspan="9" class="empty">${he(emptyMsg)}</td></tr>`}</tbody>
    </table>
    ${pager}
  </div>`;
  // スマホ幅では絞り込みを畳む (PCは開いたまま)。条件が入っているときは畳まない。
  // 幅の変化 (回転・ウィンドウリサイズ) にも追従させる — PC幅で閉じたままだと summary が
  // 非表示なので絞り込みに触れなくなるため (Codexレビュー反映)。JSが動かない場合は開いたまま
  const listScript = `
  (function() {
    var fb = document.querySelector('details.fbox');
    if (!fb) return;
    var mq = window.matchMedia('(max-width: 700px)');
    function sync() {
      if (mq.matches && !${filtering}) fb.removeAttribute('open');
      else fb.setAttribute('open', '');
    }
    sync();
    mq.addEventListener ? mq.addEventListener('change', sync) : mq.addListener(sync);
  })();
  // ─── 一括操作 (2026-08-17 スタッフ要望) ───
  (function() {
    var bar = document.getElementById('bulkBar');
    if (!bar) return;
    var ME = ${JSON.stringify(String(actorOf(req))).replace(/</g, '\\u003c')};
    var chkAll = document.getElementById('chkAll');
    var countEl = document.getElementById('bulkCount');
    var applyBtn = document.getElementById('bulkApply');
    var allBtn = document.getElementById('bulkAll');
    // 「この条件の全件を選択」(2026-08-20 中原さん要望: 1ページ50件では足りない)。
    // ページの全行を選択した状態でだけ提案し、全件モード中に1つでも外せば解除される
    var allMode = false;
    var TOTAL = ${Number(total) || 0};
    var FILTER = ${JSON.stringify(Object.fromEntries(['view', 'status', 'group', 'channel', 'shop', 'folder', 'label', 'assigned', 'unread', 'attention', 'ai', 'from', 'to', 'q']
      .map(k => [k, String(q[k] ?? '')]).filter(([, v]) => v !== ''))).replace(/</g, '\\u003c')};
    function rows() { return Array.prototype.slice.call(document.querySelectorAll('.rowchk')); }
    function selected() { return rows().filter(function(c) { return c.checked; }).map(function(c) { return Number(c.value); }); }
    function refresh() {
      var n = selected().length;
      var all = rows();
      if (allMode && n !== all.length) allMode = false;
      countEl.textContent = allMode ? 'この条件の全' + TOTAL : n;
      applyBtn.textContent = allMode ? '全' + TOTAL + '件に適用' : '選択した' + n + '件に適用';
      bar.hidden = n === 0;
      allBtn.hidden = allMode || TOTAL <= all.length || n !== all.length;
      chkAll.checked = all.length > 0 && n === all.length;
      chkAll.indeterminate = n > 0 && n < all.length;
    }
    chkAll.addEventListener('change', function() {
      rows().forEach(function(c) { c.checked = chkAll.checked; });
      refresh();
    });
    rows().forEach(function(c) { c.addEventListener('change', refresh); });
    allBtn.addEventListener('click', function() { allMode = true; refresh(); });
    document.getElementById('bulkClear').addEventListener('click', function() {
      allMode = false;
      rows().forEach(function(c) { c.checked = false; });
      refresh();
    });
    applyBtn.addEventListener('click', function() {
      var ids = selected();
      if (!ids.length) return;
      var ops = {};
      var desc = [];
      var st = document.getElementById('bulkStatus');
      if (st.value) { ops.status = st.value; desc.push('状態→' + st.options[st.selectedIndex].textContent); }
      var fo = document.getElementById('bulkFolder');
      if (fo.value) {
        ops.folderId = fo.value === 'none' ? null : Number(fo.value);
        desc.push('フォルダ→' + fo.options[fo.selectedIndex].textContent);
      }
      var lb = document.getElementById('bulkLabel');
      if (lb.value) {
        ops.labelId = lb.value === 'none' ? null : Number(lb.value);
        desc.push('ラベル→' + lb.options[lb.selectedIndex].textContent);
      }
      var asg = document.getElementById('bulkAssign');
      if (asg.value) {
        ops.assigned = asg.value === '__me__' ? ME : (asg.value === '__none__' ? '' : asg.value);
        desc.push('担当→' + asg.options[asg.selectedIndex].textContent);
      }
      var rd = document.getElementById('bulkRead');
      if (rd.value) { ops.isUnread = rd.value === 'unread'; desc.push(rd.value === 'unread' ? '未読にする' : '既読にする'); }
      if (!desc.length) { toast('変更内容を選んでください'); return; }
      function jp(url, data) {
        return fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data) })
          .then(function(r) { return r.json().catch(function(){ return {}; }).then(function(j) {
            if (!r.ok) throw new Error(j.error || ('HTTP ' + r.status)); return j; }); });
      }
      function done(j) {
        toast(j.updated + '件を変更しました' + (j.skipped ? ' (' + j.skipped + '件は変更なし)' : '')
          + (j.batchId ? '。間違えたら⚙️運用管理から取り消せます' : ''));
        setTimeout(function(){ location.reload(); }, j.batchId ? 1200 : 800);
      }
      function fail(e) { toast('失敗: ' + e.message); applyBtn.disabled = false; }
      if (allMode) {
        // 全件モード: まずサーバーで最新の対象件数を数えてから確認する (表示後に増減していても正確)
        applyBtn.disabled = true;
        jp('/apps/inquiry-hub/api/inquiries/bulk-by-filter', { filter: FILTER, ops: ops, dryRun: true })
          .then(function(p) {
            if (!confirm('⚠️ いまの検索条件に一致する全' + p.matched + '件 (他のページの分も含む) をまとめて変更します。\\n\\n'
              + desc.join('\\n') + '\\n\\nよろしいですか?')) { applyBtn.disabled = false; return null; }
            return jp('/apps/inquiry-hub/api/inquiries/bulk-by-filter', { filter: FILTER, ops: ops });
          })
          .then(function(j) { if (j) done(j); })
          .catch(fail);
        return;
      }
      if (!confirm(ids.length + '件をまとめて変更します。\\n\\n' + desc.join('\\n') + '\\n\\nよろしいですか?')) return;
      applyBtn.disabled = true;
      jp('/apps/inquiry-hub/api/inquiries/bulk', { ids: ids, ops: ops }).then(done).catch(fail);
    });
    refresh();
  })();`;
  res.send(pageShell(
    curFolder ? `問い合わせ管理 — 📁${curFolder.name}` : `問い合わせ管理 — ${VIEWS[view].label}`,
    curFolder ? `folder:${curFolder.id}` : view, body, listScript, { group }));
});

// ─── 詳細画面 ───
router.get('/inquiries/:id', (req, res) => {
  const id = Number(req.params.id);
  const detail = getInquiryDetail(id);
  if (!detail) return res.status(404).send(pageShell('問い合わせ管理', DEFAULT_VIEW, '<div class="card empty">問い合わせが見つかりません。<a href="/apps/inquiry-hub">一覧に戻る</a></div>', ''));
  const { inquiry: inq, messages, attachments, notes, logs, draft } = detail;
  // 戻り先のビュー (一覧から ?view=/?folder=/?group= を引き継ぐ。直リンク時は受信トレイ)
  const backView = VIEWS[req.query?.view] ? req.query.view : DEFAULT_VIEW;
  const backFolderId = /^\d+$/.test(String(req.query?.folder || '')) ? Number(req.query.folder) : null;
  const backFolder = backFolderId ? listFolders().find(f => f.id === backFolderId) : null;
  const backGroup = CHANNEL_GROUPS[req.query?.group] ? req.query.group : '';
  const backUrl = `/apps/inquiry-hub?view=${he(backView)}${backFolder ? `&folder=${backFolder.id}` : ''}${backGroup ? `&group=${he(backGroup)}` : ''}`;
  const attByMsg = {};
  for (const a of attachments) (attByMsg[a.inquiry_message_id] = attByMsg[a.inquiry_message_id] || []).push(a);

  // 社内既読化は GET の副作用にせず、表示後にクライアントが POST /read を打つ
  // (Codex R1 medium: プリフェッチ/リンクプレビューでの意図しない既読化と監査ログ欠落の防止)

  const msgHtml = messages.map(m => {
    // 添付: 画像はその場でサムネイル表示 (クリックで原寸)、それ以外はダウンロードリンク。
    // 実体は保存せず表示のたびに外部から取る (attachments.js) ため lazy 読み込みにする
    const atts = (attByMsg[m.id] || []).map(a => {
      const ct = resolveContentType(a.file_name, a.content_type);
      const url = `/apps/inquiry-hub/attachments/${a.id}`;
      const size = fmtBytes(a.file_size);
      const label = `${he(a.file_name || '(名称不明)')}${size ? ` (${size})` : ''}`;
      if (isImage(ct)) {
        return `<figure class="att-img">
          <a href="${url}" target="_blank" rel="noopener">
            <img src="${url}" alt="${he(a.file_name || '添付画像')}" loading="lazy"
              onerror="this.closest('figure').classList.add('att-err')">
          </a>
          <figcaption>🖼️ <a href="${url}" target="_blank" rel="noopener">${label}</a>
            <a class="att-dl" href="${url}?download=1">⬇️保存</a>
            <span class="att-fail">取得できませんでした (モール/メール側で確認してください)</span></figcaption>
        </figure>`;
      }
      const icon = isInlineSafe(ct) ? '📄' : '📎';
      return `<span class="att">${icon} <a href="${url}${isInlineSafe(ct) ? '' : '?download=1'}" target="_blank" rel="noopener">${label}</a></span>`;
    }).join(' ');
    // Step 1 は text のみ表示 (message_body_html のサニタイズ表示は Gmail 同期実装時に導入)。
    // 取込済みデータには過剰な空行が残っているものがあるため表示時にも正規化する
    // (自動配信メールの空行がそのまま<br>になり、スマホで画面が延々と間延びしていた実測)
    const bodyText = normalizeBodyText(m.message_body_text) || '(本文なし)';
    const lines = bodyText.split('\n');
    // 長文 (メールの自動配信・署名込みの長い問い合わせ) はスレッドを追いやすいよう畳む
    const FOLD_LINES = 14, FOLD_CHARS = 700;
    const folded = lines.length > FOLD_LINES || bodyText.length > FOLD_CHARS;
    // headPart は必ず bodyText の先頭部分 (lines は bodyText.split(改行))。
    // URLの途中で切るとリンクが畳みの前後に割れるので urlSafeCut で境目をずらす
    const headLen = folded ? urlSafeCut(bodyText, lines.slice(0, 8).join('\n').slice(0, 400).length) : bodyText.length;
    const headPart = bodyText.slice(0, headLen);
    const restPart = folded ? bodyText.slice(headLen) : '';
    const br = s => linkifyText(s).replace(/\n/g, '<br>');
    return `
    <div class="msg ${m.is_incoming ? 'in' : 'out'}">
      <div class="msg-head">
        <b>${m.is_incoming ? '👤 ' : '🏪 '}${he(m.sender_name || (m.is_incoming ? '顧客' : '店舗'))}</b>
        ${m.sender_type === 'system' ? '<span class="badge" style="background:#f1f5f9;color:#64748b">system</span>' : ''}
        <span class="msg-date">${fmtJst(m.received_at || m.sent_at || m.created_at)}${m.is_incoming ? '' : m.sent_by_user_id ? ` ・ 送信者: ${he(m.sent_by_user_id)}` : ''}</span>
      </div>
      <div class="msg-body">${br(headPart)}${folded ? `<details class="more"><summary>… 続きを表示 (全${lines.length}行)</summary><div>${br(restPart)}</div></details>` : ''}</div>
      ${atts ? `<div class="msg-atts">${atts}</div>` : ''}
    </div>`;
  }).join('');

  const noteHtml = notes.map(n => `
    <div class="note"><div class="note-head"><b>${he(n.user_id)}</b> <span class="msg-date">${fmtJst(n.created_at)}</span></div>
    <div>${linkifyText(n.body).replace(/\n/g, '<br>')}</div></div>`).join('') || '<div class="empty">メモはありません</div>';

  const logHtml = logs.map(l => {
    const b = fmtLogJson(l.before_json);
    const a = fmtLogJson(l.after_json);
    const detail = b != null && a != null ? `${b} → ${a}` : (a != null ? a : '');
    return `<div class="log-row"><span class="msg-date">${fmtJst(l.created_at)}</span> <b>${he(l.user_id || l.actor_type)}</b> ${he(ACTION_LABELS[l.action_type] || l.action_type)} <span class="sub">${he(detail)}</span></div>`;
  }).join('') || '<div class="empty">履歴はありません</div>';

  // 任意フォルダ (分類ラベル。ステータスとは独立で、入れても受信トレイからは消えない)
  const folderList = listFolders();
  const folderOptions = `<option value="">未分類</option>` + folderList
    .map(f => `<option value="${f.id}"${inq.folder_id === f.id ? ' selected' : ''}>📁 ${he(f.name)}</option>`).join('');
  // 色付きラベル (2026-08-24 メールディーラー相当。1件1ラベル)
  const labelList = listLabels();
  const labelOptions = `<option value="">ラベルなし</option>` + labelList
    .map(l => `<option value="${l.id}"${inq.label_id === l.id ? ' selected' : ''}>🏷️ ${he(l.name)}</option>`).join('');

  const stOptions = Object.entries(STATUSES).map(([k, v]) => `<option value="${k}"${inq.internal_status === k ? ' selected' : ''}>${he(v.label)}</option>`).join('');
  const aiOptions = Object.entries(AI_FLAGS).map(([k, v]) => `<option value="${k}"${String(inq.ai_needed) === k ? ' selected' : ''}>${he(k === '0' ? 'AI不要' : v.label)}</option>`).join('');

  const aiPanel = draft ? `
    <div class="panel">
      <h3>🤖 AI返信案 ${draft.is_stale ? '<span class="badge" style="background:#fef3c7;color:#92400e">⚠️古い会話に基づく返信案</span>' : ''}</h3>
      ${draft.summary ? `<div class="sub">要約: ${he(draft.summary)}${draft.category ? ` ・ 分類: ${he(draft.category)}` : ''}</div>` : ''}
      <div class="ai-draft">${he(draft.draft_body || '').replace(/\n/g, '<br>')}</div>
      ${draft.notes ? `<div class="sub">⚠️ 注意: ${he(draft.notes)}</div>` : ''}
      ${draft.confirmation_items ? `<div class="sub">☑️ 送信前の確認: ${he(draft.confirmation_items)}</div>` : ''}
      ${replyEditorEnabled() && !draft.is_stale ? '<button class="ghost" id="useDraftBtn" style="margin-top:8px">📝 この案を返信欄へコピー</button>' : ''}
    </div>` : '';

  // ─── 返信エディタ (Dark Launch。設計書§7.1#3) ───
  // 送信ジョブは作成時点では pending。送信ワーカー (Step 3〜) が拾うまで実送信されない
  const outboxJobs = getDB().prepare(
    'SELECT * FROM outbox_replies WHERE inquiry_id = ? ORDER BY id DESC LIMIT 10').all(inq.id);
  const activeJob = outboxJobs.find(o => ['pending', 'sending', 'needs_review'].includes(o.status)
    || (o.status === 'unknown' && !o.resolution));
  const jobBadge = o => {
    const meta = OUTBOX_STATUS_LABELS[o.status] || { label: o.status, style: 'background:#f1f5f9;color:#475569' };
    return `<span class="badge" style="${meta.style}">${he(meta.label)}</span>`;
  };
  const jobAtts = o => {
    if (!o.attachments_json) return '';
    try {
      const names = JSON.parse(o.attachments_json).map(a => a.name).filter(Boolean);
      return names.length ? ` <span class="sub" title="${he(names.join(' / '))}">📎${names.length}件</span>` : '';
    } catch { return ''; }
  };
  // 送信ジョブの本文は**全文**を残す。2026-08-30 の楽天401 (ライセンスキー失効) で送信が失敗したとき、
  // 履歴には先頭60文字しか出ておらず「何を送ろうとしたか」が画面から分からなかった (中原さん要望)。
  // 返信欄が空いていれば (未決着ジョブが無ければ) そのまま書き戻して送り直せる
  const canRestoreBody = replyEditorEnabled() && !activeJob;
  const jobBody = (o) => {
    const body = String(o.body_text || '');
    if (!body) return '';
    let attNames = [];
    try { attNames = JSON.parse(o.attachments_json || '[]').map(a => a.name).filter(Boolean); } catch { /* 壊れていても本文は見せる */ }
    return `<details class="job-body">
      <summary class="sub">${he(body.slice(0, 60))}${body.length > 60 ? '…' : ''} <span class="job-body-more">▼全文</span></summary>
      <div class="job-body-full">${he(body)}</div>
      ${attNames.length ? `<div class="sub">📎 ${he(attNames.join(' / '))} — <b>添付は付け直してください</b> (送信済みの実体は保持していません)</div>` : ''}
      ${canRestoreBody ? '<button class="ghost job-restore" type="button">↩ この内容を返信欄に戻す</button>' : ''}
    </details>`;
  };
  const outboxHtml = outboxJobs.length ? `
    <div class="sub" style="margin-bottom:6px">送信ジョブ履歴 (<a href="/apps/inquiry-hub/admin">⚙️運用管理</a>で解決・取消):</div>
    ${outboxJobs.map(o => `<div class="log-row">${jobBadge(o)} <span class="msg-date">${fmtJst(o.created_at)}</span>${jobAtts(o)}
      ${jobBody(o)}
      ${o.error_message ? `<div class="sub" style="color:#b91c1c">└ ${he(String(o.error_message).slice(0, 500))}</div>` : ''}</div>`).join('')}` : '';
  // 📎 送信用添付: 未紐付け分 (ページ再読み込みしてもアップロード済みが消えないよう復元する)
  const pendingAtts = (replyEditorEnabled() && ['email', 'rakuten'].includes(inq.channel_type) && !activeJob)
    ? listPendingAttachments(inq.id) : [];
  // 送信ワーカーの状態に応じたバナー (チャネル別モード。env は起動時固定なので都度読んでも軽い)
  const outboxOn = ['true', '1'].includes(process.env.INQUIRY_HUB_OUTBOX_CRON_ENABLED || '');
  const SEND_MODE_ENV = { email: 'INQUIRY_HUB_MAIL_SEND_MODE', rakuten: 'INQUIRY_HUB_RAKUTEN_SEND_MODE', yahoo: 'INQUIRY_HUB_YAHOO_SEND_MODE' };
  const channelSupported = inq.channel_type in SEND_MODE_ENV;
  const sendLive = channelSupported && process.env[SEND_MODE_ENV[inq.channel_type]] === 'live';
  const workerBanner = !channelSupported
    ? '<div class="sub" style="background:#fef3c7;border-radius:8px;padding:8px 10px;margin-bottom:8px">⚠️ このチャネルの送信は未実装です。作成したジョブは送信されず保留されます</div>'
    : !outboxOn
      ? '<div class="sub" style="background:#fef3c7;border-radius:8px;padding:8px 10px;margin-bottom:8px">⚠️ 送信ワーカーは停止中です。作成した返信ジョブはまだ実際には送信されません (⚙️運用管理で確認・取消できます)</div>'
      : !sendLive
        ? '<div class="sub" style="background:#e0e7ff;border-radius:8px;padding:8px 10px;margin-bottom:8px">🧪 DRYRUNモード: このチャネルの送信ジョブは検証のみで、実際には送信されません (動作確認用)</div>'
        : '';
  // ─── 返信不可アドレス / 配信失敗の警告 (2026-08-26 no-reply@mercari-shops.com 事故) ───
  // メールチャネルの返信先は customer_identifier。通知専用アドレスだと送っても顧客に届かない
  // 空・解析不能なアドレスは塞がない (送信時にスレッドから復元される。no-reply.js 参照)
  const replyBlocked = inq.channel_type === 'email' ? blockedReplyDestination(inq.customer_identifier) : null;
  const blockedBanner = !replyBlocked ? '' : `
    <div style="background:#fee2e2;border:1px solid #fca5a5;border-radius:8px;padding:10px 12px;margin-bottom:8px;color:#7f1d1d">
      <b>🚫 このアドレスにメール返信はできません</b>
      <div class="sub" style="color:#7f1d1d">${he(replyBlocked.reason)}</div>
      <div class="sub" style="color:#7f1d1d;margin-top:4px"><b>➡ ${he(replyBlocked.guide)}</b></div>
      <div class="sub" style="color:#7f1d1d;margin-top:4px">下の欄で文面だけ作って、返信先の画面に貼り付けることはできます</div>
    </div>`;
  const bouncedBanner = inq.delivery_failed_at ? `
    <div style="background:#fee2e2;border:1px solid #fca5a5;border-radius:8px;padding:10px 12px;margin-bottom:8px;color:#7f1d1d">
      <b>🔴 前の返信が届いていません (配信失敗)</b>
      <div class="sub" style="color:#7f1d1d">${he(fmtJst(inq.delivery_failed_at))} に配信失敗通知 (バウンス) を受け取りました。
        送信済みに見えていても顧客には届いていません — 別の手段で連絡してください</div>
    </div>` : '';

  // モール側の「回答完了」を返信と同時に打てるチャネル (2026-08-26 スタッフ要望。いまは Yahoo! のみ。
  // 楽天 R-Messe にも完了APIはあるが miniPC passthrough 未整備 → 追加時はここと adapter.completeInquiry)
  const mallCompleteSupported = inq.channel_type === 'yahoo';

  const replyPanel = replyEditorEnabled() ? `
      <div class="panel">
        <h3>✉️ 返信を作成</h3>
        ${bouncedBanner}
        ${blockedBanner}
        ${workerBanner}
        ${outboxHtml}
        ${activeJob
          ? `<div class="sub" style="background:#e0e7ff;border-radius:8px;padding:8px 10px">この問い合わせには未決着の送信ジョブ (#${activeJob.id}) があります。<a href="/apps/inquiry-hub/admin">⚙️運用管理</a>で解決・取消してから新しい返信を作成してください</div>`
          : `<div class="row tpl-row" id="tplRow">
          <input type="search" id="tplSearch" placeholder="🔍 キーワードで絞り込み"
            title="テンプレート名・グループ・件名・本文・キーワードで絞り込みます (空白区切りで複数指定)">
          <select id="tplSel" title="テンプレートを選ぶ (カテゴリごとにまとまっています)"><option value="">📄 テンプレートを選ぶ…</option></select>
          <button class="ghost" id="tplApplyBtn" type="button">本文に反映</button>
        </div>
        ${aiRewriteEnabled() ? `<div class="row rw-row" id="draftRow">
          <button class="pri" type="button" id="draftBtn">✨ AIで下書き</button>
          <span class="sub">社内Q&amp;A・テンプレートを参照して返信案を作ります</span>
        </div>
        <div class="draft-warn" id="draftWarn" hidden></div>` : ''}
        <textarea id="replyBody" rows="6" placeholder="返信本文 (上のテンプレート選択からも入れられます)"></textarea>
        ${aiRewriteEnabled() ? `<div class="row rw-row" id="rwRow">
          <span class="sub">✨AIで整える:</span>
          ${Object.entries(REWRITE_STYLES).map(([k, v]) => `<button class="ghost rw-btn" type="button" data-style="${k}">${he(v.label)}</button>`).join('')}
          <button class="ghost" type="button" id="rwUndoBtn" style="display:none">↩ 元に戻す</button>
        </div>` : ''}
        ${['email', 'rakuten'].includes(inq.channel_type) ? `<div class="row rw-row" id="attRow">
          <button class="ghost" type="button" id="attBtn">📎 ファイルを添付</button>
          <input type="file" id="attFile" multiple accept=".pdf,.png,.jpg,.jpeg,.gif,.webp,.bmp,application/pdf,image/*" style="display:none">
          <span class="sub">${he(ALLOWED_LABEL)}・1ファイル${Math.round(MAX_FILE_BYTES / 1048576)}MB・最大${MAX_FILES_PER_REPLY}つ</span>
        </div>
        <div id="attList"></div>` : ''}
        <div class="row" style="margin-top:8px; justify-content:flex-end; gap:8px">
          ${replyBlocked
            ? `<button class="pri" id="replyBtn" disabled title="返信不可のアドレスのため送信できません">🚫 このアドレスには送信できません</button>`
            : `<span class="sub" style="margin-right:auto">${mallCompleteSupported
                ? '「送信して回答完了」= 返信の投稿と同時にモール側 (ストアクリエイターPro) も回答完了にします'
                : '「送信して完了」= 返信後にこの画面の状態を完了にします'}</span>
          <button class="ghost" id="replyBtn" type="button" title="送信後は「返信処理中」になります">✉️ 送信</button>
          <button class="pri" id="replyCompleteBtn" type="button" title="${mallCompleteSupported ? '送信と同時にモール側も回答完了にします' : '送信後に完了にします'}">✅ 送信して${mallCompleteSupported ? '回答完了' : '完了'}</button>`}
        </div>`}
      </div>` : `
      <div class="panel reply-note">${bouncedBanner}${blockedBanner}✉️ 返信機能はまだ有効になっていません (いまはメールディーラーから返信してください)。
        <span class="sub">管理者が env <code>INQUIRY_HUB_REPLY_EDITOR_ENABLED=true</code> を設定すると、この画面から返信できます</span></div>`;

  // 📧 このメールを今後どう扱うか (メールチャネルのみ)。自動配信メールが受信トレイを
  // 埋めるので、その場でルールを作れるようにする (2026-07-25 中原さん要望)
  const senderAddr = inq.channel_type === 'email' ? String(inq.customer_identifier || '').trim().toLowerCase() : '';
  const senderDomain = senderAddr.includes('@') ? senderAddr.slice(senderAddr.indexOf('@')) : '';
  const subjectSeed = String(inq.subject || '').replace(/[（(].*?[）)]/g, '').trim().slice(0, 40);
  // メールディーラーの振り分け設定と同じく複数条件を組み合わせられる
  // (例: 件名が「Your payment is on the way」を含む かつ Reply-To が ○○)
  const mrFieldOpts = (sel) => Object.entries(RULE_FIELD_LABELS)
    .map(([k, v]) => `<option value="${k}"${k === sel ? ' selected' : ''}>${v}</option>`).join('');
  const mrOpOpts = (sel) => Object.entries(RULE_OP_LABELS)
    .map(([k, v]) => `<option value="${k}"${k === sel ? ' selected' : ''}>${v}</option>`).join('');
  const mrRow = (n, field, op, value) => `
        <div class="row rule-row" style="margin-bottom:6px">
          <select id="mrField${n}">${n > 1 ? '<option value="">(条件なし)</option>' : ''}${mrFieldOpts(field)}</select>
          <input type="text" id="mrValue${n}" value="${he(value || '')}" placeholder="文字列">
          <select id="mrOp${n}">${mrOpOpts(op)}</select>
        </div>`;
  const mailRulePanel = inq.channel_type === 'email' ? `
      <div class="panel">
        <h3>📧 今後の自動処理</h3>
        <div class="sub" style="margin-bottom:8px">同じようなメールが来たときの扱いをルールにします。条件は組み合わせられます (📧メールルールタブでいつでも編集・削除できます)</div>
        ${mrRow(1, 'from', 'equals', senderAddr)}
        ${mrRow(2, '', 'contains', '')}
        ${mrRow(3, '', 'contains', '')}
        ${senderDomain ? `<div class="sub">よく使う値: <a href="#" id="mrUseDomain">ドメイン全体 (${he(senderDomain)}) にする</a></div>` : ''}
        <div class="row rule-row">
          <select id="mrMode">
            <option value="all">すべての条件を満たす (かつ)</option>
            <option value="any">いずれかの条件を満たす (または)</option>
          </select>
          <select id="mrAction">
            <option value="skip">取り込まない (問い合わせにしない)</option>
            <option value="import_done">取り込むが完了扱い (履歴には残す)</option>
            <option value="import">📁🏷️振り分けだけする (新着のまま)</option>
          </select>
        </div>
        <div class="row rule-row">
          <select id="mrFolder" title="取り込むメールを入れるフォルダ (完了扱いと組み合わせも可)">
            <option value="">📁 フォルダ指定なし</option>
            ${folderList.map(f => `<option value="${f.id}"${inq.folder_id === f.id ? ' selected' : ''}>📁 ${he(f.name)}</option>`).join('')}
          </select>
          <select id="mrLabel" title="条件に一致したメールへ取り込み時に付けるラベル (一覧に色付きで表示)">
            <option value="">🏷️ ラベルなし</option>
            ${labelList.map(l => `<option value="${l.id}"${inq.label_id === l.id ? ' selected' : ''}>🏷️ ${he(l.name)}</option>`).join('')}
          </select>
        </div>
        <label class="chk"><input type="checkbox" id="mrApplyExisting" checked>すでに溜まっている同じメールにも適用する</label>
        <div class="sub">※ 一括適用は差出人・件名の条件のみ対応 (Reply-To/To/本文は今後の取り込みから効きます)</div>
        <button class="pri" id="mrBtn" style="margin-top:6px">ルールを作成</button>
      </div>` : '';

  // ─── 注文リンク (2026-08-16 中原さん要望): NE個別受注明細 + モール側の注文詳細を直接開く ───
  // 計算は customer-info.js の orderLinksOf() に共通化 (顧客情報の自動保存APIも同じリンクを返す。2026-08-31)
  const ol = orderLinksOf(inq);
  const { neOrderNo, neOrderUrl, mallOrderUrl } = ol;
  const neLinkHtml = neOrderUrl ? `<a href="${he(neOrderUrl)}" target="_blank" rel="noopener" title="ネクストエンジンの個別受注明細を開く">🧾 NEで受注を開く ↗</a>` : '';
  // モールの注文詳細への直リンク。直リンク形式が未確認のモールは管理画面トップを「番号をコピーして開く」
  const mallLinkHtml = mallOrderUrl
    ? `<a href="${he(mallOrderUrl)}" target="_blank" rel="noopener" title="${he(ol.mallLabel || 'モール')}の注文詳細画面を開く">🛍️ ${he(ol.mallShort || 'モール')}で注文を開く ↗</a>`
    : ol.mallAdminUrl
      ? `<a href="${he(ol.mallAdminUrl)}" class="ci-copy-open" data-copy="${he(ol.orderNumber || '')}" target="_blank" rel="noopener" title="注文番号をコピーして${he(ol.mallLabel || 'モール')}の管理画面を開きます (注文検索に貼り付けてください)">🛍️ ${he(ol.mallShort || 'モール')}の管理画面を開く (番号をコピー) ↗</a>`
      : '';
  // ─── 顧客情報の手入力 (2026-08-31 中原さん要望): 注文番号等が付いて来ないメール問い合わせに、
  // NE等で調べた注文番号・商品を残す (次に開いた人が調べ直さなくて済む)。保存ボタンは無く、
  // 入力欄から出た時点で自動保存 (/customer-info)。モール同期が入れた確定情報 (楽天/Yahoo!の
  // 購入者問い合わせ等) は🔒表示で変更不可 (判定は customer-info.js)
  const ci = customerInfoState(inq);
  const ciLock = '<span class="ci-lock" title="モールから取得した確定情報のため変更できません">🔒</span>';
  const ciInput = (key, placeholder) => `<input type="text" class="ci-input" id="ci_${key}" data-key="${key}" value="${he(ci[key].value || '')}" placeholder="${he(placeholder)}" maxlength="${CUSTOMER_INFO_FIELDS[key].max}" autocomplete="off" title="調べた内容を入力すると自動で保存されます (入力欄から出た時点で保存)">`;
  // モール選択 (2026-08-31 追加要望): 手入力の注文番号に「どのモールか」を付け、そのモールの注文詳細へ飛べるようにする。
  // 楽天/Amazon/Yahoo! は注文番号の形式から自動で入る (違えば選び直せる)。表示は有効なモール (ol.mall)
  const ciMallSelect = `<select class="ci-select" id="ci_order_mall" data-key="order_mall" title="この注文番号のモール (注文番号の形式から自動で入ります。違えば選び直してください)"><option value="">モール…</option>${ORDER_MALL_KEYS.map(k => `<option value="${k}"${(ol.mall || '') === k ? ' selected' : ''}>${he(ORDER_MALLS[k].label)}</option>`).join('')}</select>`;
  const ciEditable = CUSTOMER_INFO_KEYS.some(k => !ci[k].locked);
  const ciLocked = CUSTOMER_INFO_KEYS.some(k => ci[k].locked);
  const ciHint = ciEditable
    ? `<div class="sub ci-hint">✏️ 空欄は調べた内容を入力すると自動で保存されます (入力欄から出た時点で保存)${ciLocked ? '。🔒 はモールから取得した確定情報で変更できません' : ''}</div>`
    : '<div class="sub ci-hint">🔒 注文番号・商品はモールから取得した確定情報です (変更できません)</div>';
  // 🔎 NEで受注検索 (2026-08-20 スタッフ要望): メール問い合わせには注文番号が無く受注に飛べない。
  // メールディーラーはNE APIでアドレス→受注を突合していたが、ここはAPI連携なしで
  // 「アドレスをコピーしてNEの受注検索画面を開く」導線にする (NEの検索条件はURLで渡せないため貼り付け方式)。
  // 注文番号からNE直リンクが出るときは不要なので出さない
  const neSearchMail = (!neOrderNo && String(inq.customer_identifier || '').includes('@'))
    ? String(inq.customer_identifier).trim() : null;

  // 前後ナビ (2026-08-10 スタッフ要望): 一覧と同じ並び・同じ文脈 (view/folder/group) で隣へ移動。
  // 存在しない側はグレー表示のまま残す (ボタンが消えて位置がずれるより分かりやすい)
  const adj = getAdjacentInquiries(id, { view: backView, folder: backFolder ? String(backFolder.id) : '', group: backGroup });
  const navQs = `?view=${he(backView)}${backFolder ? `&folder=${backFolder.id}` : ''}${backGroup ? `&group=${he(backGroup)}` : ''}`;
  const navLink = (row, label) => row
    ? `<a href="/apps/inquiry-hub/inquiries/${row.id}${navQs}" title="${he(row.subject || '(件名なし)')}">${label}</a>`
    : `<span class="nav-off" aria-disabled="true">${label}</span>`;
  // 1クリック対応完了 (2026-08-24 中原さん要望・メールディーラー踏襲): 右上のボタン1つで
  // 「完了」にして次の問い合わせへ進む (次が無ければ一覧へ戻る)。既に完了なら押せない表示
  const quickDoneNext = adj.next ? `/apps/inquiry-hub/inquiries/${adj.next.id}${navQs}` : backUrl;
  const quickDoneBtn = inq.internal_status === 'done'
    ? '<button class="ghost quick-done" disabled>✅ 完了済み</button>'
    : '<button class="pri quick-done" id="quickDoneBtn" title="1クリックで「完了」にして、次の問い合わせへ移動します">✅ 対応完了</button>';
  // ─── 📦返品・交換案件パネル (2026-09-01)。
  //   ⭐自動では案件にしない。キーワードで**候補を出すだけ**で、押すかどうかは人が決める
  //     (「返品できますか？」という質問まで案件になると、ボードが死ぬ)
  const linkedCases = (() => { try { return listCasesForInquiry(id); } catch { return []; } })();
  const caseTriage = (() => { try { return getTriage(id); } catch { return null; } })();
  const lastCustomerMsg = [...messages].reverse().find(m => m.is_incoming);
  const caseHits = linkedCases.length ? []
    : (() => { try { return detectCaseKeywords(inq.subject, lastCustomerMsg?.message_body_text || ''); } catch { return []; } })();
  const caseDefaultDate = (() => { try { return businessDaysFromNow(3); } catch { return ''; } })();
  const casePanel = linkedCases.length
    ? `<div class="panel">
        <h3>📦 返品・交換案件</h3>
        ${linkedCases.map(c => `<div style="padding:6px 0;border-bottom:1px solid #f1f5f9">
          <a href="/apps/inquiry-hub/cases/${c.id}"><b>${he(c.case_no)}</b></a>
          ${badge({ badge: CASE_TYPES[c.case_type]?.badge }, CASE_TYPES[c.case_type]?.label || c.case_type)}
          ${c.status === 'active'
            ? badge({ badge: WAITING_ON[c.waiting_on]?.badge }, WAITING_ON[c.waiting_on]?.label || c.waiting_on)
            : badge({ badge: 'background:#dcfce7;color:#166534' }, '完了')}
          <div class="sub">担当 ${he(c.assigned_user_id)}${c.next_action_at ? ' ・ 次回確認 ' + he(rcJstDate(c.next_action_at)) : ''}
            ${overdueDays(c.next_action_at) > 0 ? `<b style="color:#b91c1c"> ${overdueDays(c.next_action_at)}日超過</b>` : ''}</div>
        </div>`).join('')}
        <div class="sub" style="margin-top:8px">この問い合わせを「完了」にしても案件は残ります。
          返信が終わったことと、返金・代品が終わったことは別物です。</div>
      </div>`
    : caseHits.length && caseTriage?.result !== 'no_case_needed'
      ? `<div class="panel" style="border-left:3px solid #f59e0b">
          <h3>📦 返品・交換の対応が残りそうです</h3>
          <div class="sub">本文から「${he(caseHits.slice(0, 4).join('、'))}」を検出しました。
            案件にすると、返金や代品の手配が終わるまで追いかけます。</div>
          <label>案件種別
            <select id="caseType">
              <option value="">選んでください</option>
              ${Object.entries(CASE_TYPES).map(([k, v]) => `<option value="${k}">${he(v.label)}</option>`).join('')}
            </select></label>
          <label>次回確認日
            <input type="date" id="caseDate" value="${he(caseDefaultDate)}"></label>
          <div class="sub" id="casePreview">担当と工程は自動で入ります (入力はこの2つだけ)</div>
          <div class="row" style="margin-top:8px">
            <button class="pri" id="makeCase">返品・交換案件として管理</button>
            <button class="ghost" id="noCase">今回は案件にしない</button>
          </div>
        </div>`
      : caseTriage?.result === 'no_case_needed'
        ? `<div class="panel"><h3>📦 返品・交換案件</h3>
            <div class="sub">${he(caseTriage.decided_by || '担当者')} が「案件にしない」と判断しました
              (${he(fmtJst(caseTriage.decided_at))})。<a href="#" id="undoNoCase">やっぱり案件にする</a></div></div>`
        : '';

  const body = `
  <div class="detail-head">
    <div class="detail-nav">
      <a href="${backUrl}">← ${backFolder ? `📁${he(backFolder.name)}` : he(VIEWS[backView].label)}に戻る</a>
      <span class="detail-nav-adj">
        ${navLink(adj.prev, '← 前の問い合わせ')}
        ${navLink(adj.next, '次の問い合わせ →')}
      </span>
      ${quickDoneBtn}
    </div>
    <h2>${chBadge(inq.channel_type)} ${inq.label_name ? labelChip(inq.label_name, inq.label_color) + ' ' : ''}${he(inq.subject || '(件名なし)')}</h2>
  </div>
  <div class="detail-grid">
    <div class="thread">
      ${msgHtml || '<div class="empty">メッセージがありません</div>'}
      ${replyPanel}
    </div>
    <div class="side">
      ${casePanel}
      <div class="panel">
        <h3>顧客情報</h3>
        <dl>
          <dt>店舗</dt><dd>${he(inq.shop_name)} <span class="sub">(${he(inq.account_identifier)})</span></dd>
          <dt>顧客</dt><dd>${he(inq.customer_name || '—')}${inq.customer_identifier ? `<div class="sub">${he(inq.customer_identifier)}</div>` : ''}${neSearchMail ? `<div class="sub"><a href="#" id="neMailSearch" data-mail="${he(neSearchMail)}" title="メールアドレスをコピーして、ネクストエンジンの受注検索画面を新しいタブで開きます (検索欄に貼り付けて検索してください)">🔎 NEで受注検索 (アドレスをコピー) ↗</a></div>` : ''}</dd>
          <dt>注文番号</dt><dd>${ci.order_number.locked
            ? `${he(ci.order_number.value)}${ciLock}${mallLinkHtml ? `<div class="sub">${mallLinkHtml}</div>` : ''}`
            : `<div class="row ci-order-row">${ciMallSelect}${ciInput('order_number', '注文番号を調べて入力')}</div><div class="sub ci-links" id="ciOrderLinks">${mallLinkHtml}${neLinkHtml}</div>`}</dd>
          ${ci.order_number.locked && neOrderNo ? `<dt>NE受注</dt><dd><a href="${he(neOrderUrl)}" target="_blank" rel="noopener" title="ネクストエンジンの個別受注明細を開く">${he(neOrderNo)} ↗</a></dd>` : ''}
          <dt>商品</dt><dd>${ci.product_name.locked ? `${he(ci.product_name.value)}${ciLock}` : ciInput('product_name', '商品名を調べて入力')}${ci.product_code.locked ? `<div class="sub">${he(ci.product_code.value)}${ciLock}</div>` : ciInput('product_code', '商品コード')}</dd>
          <dt>モール側状態</dt><dd class="sub" title="外部モール側のステータス (参考表示。同期が上書き)">${he(inq.external_status || '—')}${inq.external_is_read != null ? ` / ${inq.external_is_read ? '既読' : '未読'}` : ''}</dd>
          <dt>最終同期</dt><dd class="sub">${fmtJst(inq.last_external_synced_at)}</dd>
          <dt>受信</dt><dd class="sub">${fmtJst(inq.received_at)}</dd>
        </dl>
        ${ciHint}
      </div>
      <div class="panel">
        <h3>対応状況</h3>
        <label>社内ステータス
          <select id="stSel">${stOptions}</select></label>
        <label>担当者
          <div class="row"><input id="asgInput" type="text" value="${he(inq.assigned_user_id || '')}" placeholder="メールアドレス等">
          <button class="ghost" id="asgMe" type="button">自分</button></div></label>
        <label>📁 フォルダ
          <select id="folderSel">${folderOptions}</select>
          <span class="sub">分類用。入れても未返信なら受信トレイに残ります (<a href="/apps/inquiry-hub/folders">フォルダ管理</a>)</span></label>
        <label>🏷️ ラベル
          <select id="labelSel">${labelOptions}</select>
          <span class="sub">一覧で色付きの目印になります (<a href="/apps/inquiry-hub/labels">ラベル管理</a>)</span></label>
        <label>AIフラグ <select id="aiSel">${aiOptions}</select></label>
        <label class="chk"><input type="checkbox" id="attnChk"${inq.needs_attention ? ' checked' : ''}>⚠️要確認</label>
        <label class="chk"><input type="checkbox" id="unreadChk"${inq.is_unread ? ' checked' : ''}>未読に戻す</label>
        <button class="pri" id="saveBtn">保存</button>
      </div>
      ${aiPanel}
      ${mailRulePanel}
      <div class="panel">
        <h3>📝 社内メモ</h3>
        <textarea id="noteBody" rows="3" placeholder="メモを追加"></textarea>
        <button class="ghost" id="noteBtn">メモ追加</button>
        <div id="notes">${noteHtml}</div>
      </div>
      <div class="panel">
        <h3>🕘 対応履歴</h3>
        ${logHtml}
      </div>
    </div>
  </div>`;

  const script = `
  var ID = ${id};
  var CUR = ${JSON.stringify({ status: inq.internal_status, assigned: inq.assigned_user_id || '', ai: inq.ai_needed, attention: !!inq.needs_attention, unread: !!inq.is_unread, folder: inq.folder_id == null ? '' : String(inq.folder_id), label: inq.label_id == null ? '' : String(inq.label_id) }).replace(/</g, '\\u003c')};
  var ME = ${JSON.stringify(String(actorOf(req))).replace(/</g, '\\u003c')};
  function post(path, data, opts) {
    return fetch('/apps/inquiry-hub/api/inquiries/' + ID + path, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data),
      keepalive: !!(opts && opts.keepalive)
    }).then(function(r) { return r.json().catch(function(){ return {}; }).then(function(j){ if (!r.ok) throw new Error(j.error || ('HTTP ' + r.status)); return j; }); });
  }
  // 顧客情報の自動保存 (2026-08-31 中原さん要望): 注文番号・商品コード・商品名は保存ボタン無しで、
  // 入力欄から出た時点 (change) に項目単位で保存する。Enter でも確定。失敗したら元の値に戻す。
  // 「次の問い合わせ →」を押した直後 (画面遷移中) でも保存が届くよう keepalive で送る
  var CI = ${JSON.stringify({ ...Object.fromEntries(CUSTOMER_INFO_KEYS.map(k => [k, ci[k].value || ''])), order_mall: ol.mall || '' }).replace(/</g, '\\u003c')};
  var CI_LABEL = ${JSON.stringify({ ...Object.fromEntries(CUSTOMER_INFO_KEYS.map(k => [k, CUSTOMER_INFO_FIELDS[k].label])), order_mall: 'モール' }).replace(/</g, '\\u003c')};
  var MALL_SHORT = ${JSON.stringify(Object.fromEntries(ORDER_MALL_KEYS.map(k => [k, ORDER_MALLS[k].short]))).replace(/</g, '\\u003c')};
  // 注文リンクの描画 (保存後にリロード無しで差し替え)。モールの直リンク → 無ければ管理画面 (番号コピー) → NE
  function renderOrderLinks(links) {
    var box = document.getElementById('ciOrderLinks'); if (!box) return;
    box.textContent = '';
    if (!links) return;
    var add = function(href, text, title, copy) {
      if (!href) return;
      var a = document.createElement('a'); a.href = href; a.target = '_blank'; a.rel = 'noopener'; a.title = title; a.textContent = text;
      if (copy) { a.className = 'ci-copy-open'; a.dataset.copy = copy; }
      box.appendChild(a);
    };
    var mallName = links.mall_label || 'モール', mallShort = links.mall_short || 'モール';
    add(links.mall_order_url, '🛍️ ' + mallShort + 'で注文を開く ↗', mallName + 'の注文詳細画面を開く');
    if (!links.mall_order_url) add(links.mall_admin_url, '🛍️ ' + mallShort + 'の管理画面を開く (番号をコピー) ↗',
      '注文番号をコピーして' + mallName + 'の管理画面を開きます (注文検索に貼り付けてください)', links.order_number || '');
    add(links.ne_order_url, '🧾 NEで受注を開く ↗', 'ネクストエンジンの個別受注明細を開く');
  }
  // 管理画面トップしか分からないモール: 番号をコピーしてから開く (逆順だとフォーカスが新タブへ移りコピーが失敗する)
  document.addEventListener('click', function(ev) {
    var a = ev.target && ev.target.closest ? ev.target.closest('a.ci-copy-open') : null;
    if (!a) return;
    ev.preventDefault();
    var open = function() { window.open(a.href, '_blank', 'noopener'); };
    var no = a.dataset.copy || '';
    if (no && navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(no).then(function() {
        toast('注文番号 ' + no + ' をコピーしました。管理画面の注文検索に貼り付けてください'); open();
      }, function() { toast('コピーできませんでした。注文番号を手でコピーしてください'); open(); });
    } else open();
  });
  Array.prototype.forEach.call(document.querySelectorAll('.ci-input, .ci-select'), function(inp) {
    var key = inp.dataset.key;
    if (inp.tagName === 'INPUT') inp.addEventListener('keydown', function(ev) { if (ev.key === 'Enter') { ev.preventDefault(); inp.blur(); } });
    inp.addEventListener('change', function() {
      var v = inp.value.trim();
      if (v === CI[key]) { inp.value = v; return; }
      var data = {}; data[key] = v;
      inp.classList.add('ci-saving');
      post('/customer-info', data, { keepalive: true }).then(function(j) {
        // サーバー側の正規化後の値 (空白畳み込み等) を正とする。モールは有効なモール (チャネル由来も含む)
        var saved = key === 'order_mall' ? ((j.links && j.links.mall) || '') : ((j.info && j.info[key] && j.info[key].value) || '');
        CI[key] = saved; inp.value = saved;
        inp.classList.remove('ci-saving'); inp.classList.add('ci-saved');
        setTimeout(function() { inp.classList.remove('ci-saved'); }, 1500);
        var msg = CI_LABEL[key] + (v ? 'を保存しました' : 'を空にしました');
        // 注文番号の形式からモールが自動で決まったら選択欄にも反映して知らせる
        var sel = document.getElementById('ci_order_mall');
        if (key === 'order_number' && sel && j.links) {
          var eff = j.links.mall || '';
          if (eff !== CI.order_mall) { CI.order_mall = eff; sel.value = eff; }
          if (j.guessed_mall) msg += ' (モール: ' + (MALL_SHORT[j.guessed_mall] || j.guessed_mall) + ' と判定)';
        }
        toast(msg);
        if (key === 'order_number' || key === 'order_mall') renderOrderLinks(j.links);
      }).catch(function(e) {
        inp.classList.remove('ci-saving'); inp.value = CI[key];
        toast('保存できませんでした: ' + e.message);
      });
    });
  });
  // 表示したら社内既読化 (GETの副作用にしない。失敗しても表示は継続)
  if (CUR.unread) {
    post('/read', { is_unread: false }).then(function() {
      CUR.unread = false;
      document.getElementById('unreadChk').checked = false;
    }).catch(function() {});
  }
  document.getElementById('asgMe').addEventListener('click', function() { document.getElementById('asgInput').value = ME; });
  // 🔎 NEで受注検索: 先にコピー→それからNEを開く (逆順だとフォーカスが新タブへ移りコピーが失敗する)
  var neMailBtn = document.getElementById('neMailSearch');
  if (neMailBtn) neMailBtn.addEventListener('click', function(ev) {
    ev.preventDefault();
    var openNe = function() { window.open('https://main.next-engine.com/Userjyuchu/', '_blank', 'noopener'); };
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(neMailBtn.dataset.mail || '').then(function() {
        toast('アドレスをコピーしました。NEの受注検索でメールアドレス欄に貼り付けて検索してください'); openNe();
      }, function() { toast('コピーできませんでした。アドレスを選択して手動でコピーしてください'); openNe(); });
    } else { toast('このブラウザではコピーできません。アドレスを選択してコピーしてください'); openNe(); }
  });
  // 1クリック対応完了 → 次の問い合わせへ (次が無ければ一覧へ)
  var quickDoneBtn = document.getElementById('quickDoneBtn');
  if (quickDoneBtn) quickDoneBtn.addEventListener('click', function() {
    var btn = this; btn.disabled = true;
    post('/status', { status: 'done' }).then(function() {
      location.href = ${JSON.stringify(quickDoneNext).replace(/</g, '\\u003c')};
    }).catch(function(e) { btn.disabled = false; toast('完了にできませんでした: ' + e.message); });
  });
  document.getElementById('saveBtn').addEventListener('click', function() {
    var btn = this; btn.disabled = true;
    var ops = [];
    var st = document.getElementById('stSel').value;
    var asg = document.getElementById('asgInput').value.trim();
    var ai = Number(document.getElementById('aiSel').value);
    var attn = document.getElementById('attnChk').checked;
    var unread = document.getElementById('unreadChk').checked;
    var folder = document.getElementById('folderSel').value;
    var label = document.getElementById('labelSel').value;
    if (st !== CUR.status) ops.push(post('/status', { status: st }));
    if (asg !== CUR.assigned) ops.push(post('/assign', { user: asg }));
    if (folder !== CUR.folder) ops.push(post('/folder', { folder_id: folder === '' ? null : Number(folder) }));
    if (label !== CUR.label) ops.push(post('/label', { label_id: label === '' ? null : Number(label) }));
    if (ai !== CUR.ai) ops.push(post('/ai-flag', { ai_needed: ai }));
    if (attn !== CUR.attention) ops.push(post('/attention', { needs_attention: attn }));
    if (unread !== CUR.unread) ops.push(post('/read', { is_unread: unread }));
    if (!ops.length) { btn.disabled = false; toast('変更はありません'); return; }
    Promise.all(ops).then(function() { location.reload(); })
      .catch(function(e) { btn.disabled = false; toast('保存失敗: ' + e.message); });
  });
  document.getElementById('noteBtn').addEventListener('click', function() {
    var btn = this;
    var body = document.getElementById('noteBody').value.trim();
    if (!body) { toast('メモが空です'); return; }
    btn.disabled = true;
    post('/notes', { body: body }).then(function() { location.reload(); })
      .catch(function(e) { btn.disabled = false; toast('追加失敗: ' + e.message); });
  });
  // 返信ジョブ作成 (Dark Launch時のみボタンが存在)。操作IDはページ描画時にサーバーが採番
  // → 同じ画面からの再送信 (リトライ) は冪等、新しい返信はリロード後の新IDで作成
  var REPLY_OP_ID = ${JSON.stringify(crypto.randomUUID())};
  var REPLY_BASE_REV = ${Number(inq.conversation_rev) || 0};
  var REPLY_CH = ${JSON.stringify((CHANNELS[inq.channel_type] || {}).label || inq.channel_type).replace(/</g, '\\u003c')};
  // AI返信案 → 返信欄へコピー (人間が必ず確認・編集してから送信ジョブを作る)
  var AI_DRAFT_BODY = ${JSON.stringify(draft?.draft_body || '').replace(/</g, '\\u003c')};
  var useDraftBtn = document.getElementById('useDraftBtn');
  if (useDraftBtn) useDraftBtn.addEventListener('click', function() {
    var ta = document.getElementById('replyBody');
    if (!ta) { toast('返信フォームが使えません (未決着の送信ジョブを解決してください)'); return; }
    if (ta.value.trim() && !confirm('返信欄の内容をAI返信案で置き換えますか?')) return;
    ta.value = AI_DRAFT_BODY;
    ta.scrollIntoView({ behavior: 'smooth', block: 'center' });
    ta.focus();
    toast('AI返信案をコピーしました。内容を確認・編集してから送信してください');
  });
  // 送信ジョブ履歴の本文 → 返信欄へ戻す (送信失敗した返信をそのまま作り直せるように)。
  // 本文は DOM のテキストから取る (属性に埋め込まないので長文でも安全)
  Array.prototype.forEach.call(document.querySelectorAll('.job-restore'), function(btn) {
    btn.addEventListener('click', function() {
      var full = btn.parentElement && btn.parentElement.querySelector('.job-body-full');
      var ta = document.getElementById('replyBody');
      if (!full || !ta) { toast('返信フォームが使えません (未決着の送信ジョブを解決してください)'); return; }
      if (ta.value.trim() && !confirm('返信欄の内容を、この送信ジョブの本文で置き換えますか?')) return;
      ta.value = full.textContent;
      ta.scrollIntoView({ behavior: 'smooth', block: 'center' });
      ta.focus();
      toast('本文を返信欄に戻しました。添付があった場合は付け直してください');
    });
  });
  // 📧 今後の自動処理 (メールルール作成。複数条件を組み合わせられる)
  var mrBtn = document.getElementById('mrBtn');
  if (mrBtn) {
    // フィールドを選んだら、この問い合わせの値を候補として自動で入れる (空欄のときだけ)
    var MR_SEED = ${JSON.stringify({ from: senderAddr || '', subject: subjectSeed || '' }).replace(/</g, '\\u003c')};
    [1, 2, 3].forEach(function(n) {
      var f = document.getElementById('mrField' + n), v = document.getElementById('mrValue' + n);
      if (!f || !v) return;
      f.addEventListener('change', function() {
        if (!v.value.trim() && MR_SEED[f.value]) v.value = MR_SEED[f.value];
      });
    });
    // フォルダ・ラベルを選んだのに扱いが「取り込まない」のままだと矛盾する (2026-08-20 実事故) →
    // 選択時に自動で「振り分けだけする」へ切り替える (完了扱い+振り分けは正当なので触らない)
    ['mrFolder', 'mrLabel'].forEach(function(selId) {
      document.getElementById(selId).addEventListener('change', function() {
        var actSel = document.getElementById('mrAction');
        if (this.value && actSel.value === 'skip') {
          actSel.value = 'import';
          toast('扱いを「📁🏷️振り分けだけする (新着のまま)」に切り替えました');
        }
      });
    });
    // 「ドメイン全体にする」: 1行目を From が @example.com で終わる に切り替える
    var mrUseDomain = document.getElementById('mrUseDomain');
    if (mrUseDomain) mrUseDomain.addEventListener('click', function(ev) {
      ev.preventDefault();
      document.getElementById('mrField1').value = 'from';
      document.getElementById('mrOp1').value = 'ends_with';
      document.getElementById('mrValue1').value = ${JSON.stringify(senderDomain || '')};
      toast('1行目をドメイン条件にしました');
    });
    function collectConditions() {
      var out = [];
      [1, 2, 3].forEach(function(n) {
        var field = document.getElementById('mrField' + n).value;
        var value = document.getElementById('mrValue' + n).value.trim();
        if (!field || !value) return;
        out.push({ field: field, op: document.getElementById('mrOp' + n).value, value: value });
      });
      return out;
    }
    mrBtn.addEventListener('click', function() {
      var conditions = collectConditions();
      if (!conditions.length) { toast('条件を1つ以上入力してください'); return; }
      var action = document.getElementById('mrAction').value;
      var folderSel = document.getElementById('mrFolder');
      var folderId = folderSel.value ? Number(folderSel.value) : null;
      var folderName = folderSel.value ? folderSel.options[folderSel.selectedIndex].textContent.replace(/^\\s*📁\\s*/, '') : '';
      var labelSel = document.getElementById('mrLabel');
      var labelId = labelSel.value ? Number(labelSel.value) : null;
      var labelName = labelSel.value ? labelSel.options[labelSel.selectedIndex].textContent.replace(/^\\s*🏷️\\s*/, '') : '';
      if (action === 'import' && !folderId && !labelId) { toast('「振り分けだけする」はフォルダかラベルを選んでください'); return; }
      // 🚨黙って捨てない (2026-08-20 実事故: フォルダを選んだまま扱い「取り込まない」で作成→
      // フォルダ無しのskipルールになり、一括適用が既存95件を完了化した)
      if (action === 'skip' && (folderId || labelId)) {
        toast('「取り込まない」にはフォルダ・ラベルを指定できません。振り分けたい場合は扱いを「📁🏷️振り分けだけする (新着のまま)」にしてください');
        return;
      }
      var assignDesc = [];
      if (folderId) assignDesc.push('フォルダ「' + folderName + '」へ');
      if (labelId) assignDesc.push('ラベル「' + labelName + '」を付ける');
      var actionLabel = action === 'skip' ? '取り込まない'
        : action === 'import' ? assignDesc.join(' + ') + ' (新着のまま)'
        : '取り込むが完了扱い' + (assignDesc.length ? ' + ' + assignDesc.join(' + ') : '');
      var matchMode = document.getElementById('mrMode').value;
      var applyExisting = document.getElementById('mrApplyExisting').checked;
      mrBtn.disabled = true;
      // まず件数を数えてから確認する (いきなり大量を変更しない)
      post('/mail-rule', { conditions: conditions, matchMode: matchMode, action: action, folderId: folderId, labelId: labelId, applyToExisting: applyExisting, dryRun: true })
        .then(function(p) {
          var msg = 'ルール: ' + p.description + '\\n扱い: ' + actionLabel;
          if (action === 'import_done' && (folderId || labelId)) {
            msg += '\\n※ 今後のメールは完了扱いで取り込まれます (新着のまま振り分けたい場合はキャンセルして「📁🏷️振り分けだけする」を選んでください)';
          }
          if (applyExisting) {
            msg += p.canApplyToExisting
              ? '\\n\\nすでに溜まっている同じメール ' + p.matched + '件 も' + (action === 'import' ? assignDesc.join('・') + 'ようにします' : '完了にします')
              : '\\n\\n⚠️ この条件は既存メールへの一括適用に対応していません (差出人・件名の条件のみ)。今後の取り込みからルールが効きます';
          }
          msg += '\\n\\n作成しますか?';
          if (!confirm(msg)) { mrBtn.disabled = false; return null; }
          return post('/mail-rule', { conditions: conditions, matchMode: matchMode, action: action, folderId: folderId, labelId: labelId,
            applyToExisting: applyExisting && p.canApplyToExisting });
        })
        .then(function(r) {
          if (!r) return;
          // 優先度順の先勝ちで別ルールが先に当たる場合、新ルールは効かない (858件の移行ルールと衝突しやすい)
          if (r.shadowedBy) {
            alert('⚠️ ルールは作成しましたが、このメールには既存ルール「' + (r.shadowedBy.name || '(名称なし)') + '」(優先度' + r.shadowedBy.priority + ') が先に当たるため、新しいルールは効きません。\\n📧メールルールタブで既存ルールの無効化・削除、または優先度の調整をしてください。');
          }
          var done = [];
          if (r.completed) done.push('既存' + r.completed + '件を完了に');
          if (r.foldered) done.push('既存' + r.foldered + '件をフォルダへ');
          if (r.labeled) done.push('既存' + r.labeled + '件にラベル付与');
          toast('ルールを作成しました' + (done.length ? ' (' + done.join('・') + ')' : ''));
          setTimeout(function(){ location.reload(); }, r.shadowedBy ? 2500 : 1000);
        })
        .catch(function(e) { toast('失敗: ' + e.message); mrBtn.disabled = false; });
    });
  }
  // 📄 テンプレート選択 → 本文へ反映 (2026-08-15 スタッフ要望: 📄タブへ行き来せずその場で入れる)。
  // 一覧は初回操作時に1回だけ取得 (全文を含むためページ埋め込みにしない)
  var tplRow = document.getElementById('tplRow');
  if (tplRow) (function() {
    var tplSel = document.getElementById('tplSel');
    var tplSearch = document.getElementById('tplSearch');
    var TPLS = null, CATS = [], tplLoading = null;
    function loadTpls() {
      if (tplLoading) return tplLoading;
      tplLoading = fetch('/apps/inquiry-hub/api/templates')
        .then(function(r) { if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); })
        .then(function(j) { TPLS = j.templates || []; CATS = j.categories || []; fillTplSel(); })
        .catch(function(e) { tplLoading = null; toast('テンプレート取得失敗: ' + e.message); });
      return tplLoading;
    }
    // カテゴリ = <optgroup> (太字の見出し) にまとめ、その下にテンプレートを並べる
    // (2026-08-26 スタッフ要望「テンプレートが増えて探しにくい。カテゴリ見出しの下にまとめて」= メールディーラーと同じ見え方)
    // 🔍絞り込み中はヒットしたものだけ並べる (2026-09-02 スタッフ要望)
    function fillTplSel() {
      if (!TPLS) return;
      var q = tplSearch.value;
      var shown = tplFilter(TPLS, q);
      var cur = tplSel.value;
      tplSel.textContent = '';
      var head = document.createElement('option');
      head.value = '';
      head.textContent = q.trim()
        ? (shown.length ? '🔍 ' + shown.length + '件ヒット — 選んでください' : '🔍 該当なし (キーワードを変えてください)')
        : '📄 テンプレートを選ぶ… (' + TPLS.length + '件)';
      tplSel.appendChild(head);
      var order = CATS.slice();
      shown.forEach(function(t) { if (t.category && order.indexOf(t.category) < 0) order.push(t.category); });
      order.push('');   // 未分類は最後に「その他」
      order.forEach(function(cat) {
        var items = shown.filter(function(t) { return (t.category || '') === cat; });
        if (!items.length) return;
        var g = document.createElement('optgroup');
        g.label = cat || 'その他';
        items.forEach(function(t) {
          var o = document.createElement('option');
          o.value = String(t.id);
          o.textContent = t.name;
          g.appendChild(o);
        });
        tplSel.appendChild(g);
      });
      tplSel.value = cur;
      if (tplSel.selectedIndex < 0) tplSel.value = '';
    }
    // マウスが返信パネルに乗った時点で先読みして、セレクトを開いた時には並んでいる状態にする
    tplRow.addEventListener('pointerover', loadTpls, { once: true });
    tplSel.addEventListener('focus', loadTpls);
    tplSearch.addEventListener('focus', loadTpls);
    tplSearch.addEventListener('input', function() { if (TPLS) fillTplSel(); else loadTpls(); });
    function applyTpl() {
      var t = (TPLS || []).find(function(x) { return String(x.id) === tplSel.value; });
      if (!t) { toast('テンプレートを選んでください'); return; }
      var ta = document.getElementById('replyBody');
      var text = t.body + (t.bodyBottom ? '\\n\\n' + t.bodyBottom : '');
      if (ta.value.trim() && !confirm('返信欄の内容をテンプレート「' + t.name + '」で置き換えますか?')) return;
      ta.value = text;
      ta.focus();
      toast('テンプレート「' + t.name + '」を反映しました。内容を確認・編集してください');
    }
    document.getElementById('tplApplyBtn').addEventListener('click', applyTpl);
    // セレクトで選んだだけでも反映する (ボタンは空欄時の説明・視認性のために残す)
    tplSel.addEventListener('change', function() { if (tplSel.value) applyTpl(); });
  })();
  // ✨ AI下書き (2026-08-17 第1段階): 社内Q&A/テンプレートを参照して返信案を作る。
  // AIが埋められなかった箇所は【要確認:】で残るので、警告を出して人が埋めてから送らせる
  var PLACEHOLDER_RE = /【要確認[:：][^】]*】/g;
  function updateDraftWarn() {
    var warn = document.getElementById('draftWarn');
    var ta = document.getElementById('replyBody');
    if (!warn || !ta) return;
    var hits = ta.value.match(PLACEHOLDER_RE) || [];
    if (!hits.length) { warn.hidden = true; return; }
    var uniq = hits.filter(function(v, i, a) { return a.indexOf(v) === i; });
    warn.hidden = false;
    // textContent で組み立てる (AI出力をそのまま innerHTML に入れない)
    warn.textContent = '';
    var head = document.createElement('b');
    head.textContent = '⚠️ AIが確認を求めています (' + uniq.length + '箇所)';
    warn.appendChild(head);
    var body = document.createElement('div');
    body.className = 'sub';
    body.textContent = '送信前に、実際の内容を調べて置き換えてください: ';
    uniq.forEach(function(s, i) {
      if (i) body.appendChild(document.createTextNode(' '));
      var c = document.createElement('code');
      c.textContent = s;
      body.appendChild(c);
    });
    warn.appendChild(body);
  }
  var draftBtn = document.getElementById('draftBtn');
  if (draftBtn) {
    var ta0 = document.getElementById('replyBody');
    if (ta0) ta0.addEventListener('input', updateDraftWarn);
    draftBtn.addEventListener('click', function() {
      var ta = document.getElementById('replyBody');
      if (ta.value.trim() && !confirm('返信欄の内容をAIの下書きで置き換えますか?')) return;
      draftBtn.disabled = true;
      var orig = draftBtn.textContent;
      draftBtn.textContent = '✨ 下書き作成中…';
      post('/ai-draft', {})
        .then(function(r) {
          ta.value = r.text;
          updateDraftWarn();
          ta.focus();
          var src = [];
          if (r.sources && r.sources.qa.length) src.push('Q&A ' + r.sources.qa.length + '件');
          if (r.sources && r.sources.templates.length) src.push('テンプレ ' + r.sources.templates.length + '件');
          toast('下書きを作成しました' + (src.length ? ' (' + src.join('・') + 'を参照)' : ' (参照できる社内Q&Aは見つかりませんでした)')
            + (r.placeholders.length ? ' — 要確認' + r.placeholders.length + '箇所' : ''));
        })
        .catch(function(e) { toast('下書き失敗: ' + e.message); })
        .then(function() { draftBtn.textContent = orig; draftBtn.disabled = false; });
    });
  }
  // ✨ AI書き換え (2026-08-17 スタッフ要望): 入力中の文章を丁寧に/やわらかく/簡潔に整える。
  // 書き換え前の文は保持して「元に戻す」で復元できる
  var rwRow = document.getElementById('rwRow');
  if (rwRow) (function() {
    var rwPrev = null;
    var undoBtn = document.getElementById('rwUndoBtn');
    var rwBtns = rwRow.querySelectorAll('.rw-btn');
    function setBusy(busy) {
      rwBtns.forEach(function(b) { b.disabled = busy; });
      undoBtn.disabled = busy;
    }
    rwBtns.forEach(function(btn) {
      btn.addEventListener('click', function() {
        var ta = document.getElementById('replyBody');
        var text = ta.value.trim();
        if (!text) { toast('先に返信文を入力してください'); return; }
        setBusy(true);
        var orig = btn.textContent;
        btn.textContent = '✨ 書き換え中…';
        post('/ai-rewrite', { style: btn.dataset.style, text: text })
          .then(function(r) {
            rwPrev = ta.value;
            ta.value = r.text;
            undoBtn.style.display = '';
            ta.focus();
            toast('書き換えました。内容を確認してから送信してください (↩で元に戻せます)');
          })
          .catch(function(e) { toast('書き換え失敗: ' + e.message); })
          .then(function() { btn.textContent = orig; setBusy(false); });
      });
    });
    undoBtn.addEventListener('click', function() {
      if (rwPrev == null) return;
      var ta = document.getElementById('replyBody');
      var cur = ta.value;
      ta.value = rwPrev;
      rwPrev = cur;  // もう一度押すと書き換え後に戻れる (トグル)
      toast('戻しました (もう一度押すと書き換え後に戻ります)');
    });
  })();
  // 📎 送信用添付 (2026-08-20 スタッフ要望「PDFなどを添付できるように」)。
  // 一覧はDOMをtextContentで構築 (ファイル名はユーザー入力なのでinnerHTMLに入れない)
  var ATT = ${JSON.stringify(pendingAtts).replace(/</g, '\\u003c')};
  var attBtn = document.getElementById('attBtn');
  if (attBtn) (function() {
    var input = document.getElementById('attFile');
    var list = document.getElementById('attList');
    var MAXF = ${MAX_FILES_PER_REPLY}, MAXB = ${MAX_FILE_BYTES};
    function fmtB(n) { return n < 1048576 ? Math.round(n / 1024) + 'KB' : (n / 1048576).toFixed(1) + 'MB'; }
    function render() {
      list.textContent = '';
      ATT.forEach(function(a) {
        var row = document.createElement('div');
        row.className = 'att-chip';
        var name = document.createElement('span');
        name.textContent = '📎 ' + a.name + ' (' + fmtB(a.size) + ')';
        var del = document.createElement('button');
        del.type = 'button'; del.className = 'ghost'; del.textContent = '✕'; del.title = 'この添付を取り消す';
        del.addEventListener('click', function() {
          del.disabled = true;
          post('/reply-attachments/' + a.id + '/delete', {}).then(function() {
            ATT = ATT.filter(function(x) { return x.id !== a.id; }); render();
          }).catch(function(e) { del.disabled = false; toast('削除失敗: ' + e.message); });
        });
        row.appendChild(name); row.appendChild(del);
        list.appendChild(row);
      });
    }
    render();
    attBtn.addEventListener('click', function() { input.click(); });
    input.addEventListener('change', function() {
      var files = Array.prototype.slice.call(input.files || []);
      input.value = '';
      (function next() {
        var f = files.shift();
        if (!f) return;
        if (ATT.length >= MAXF) { toast('添付は' + MAXF + 'つまでです'); return; }
        if (f.size > MAXB) { toast(f.name + ' は大きすぎます (上限' + Math.round(MAXB / 1048576) + 'MB)'); next(); return; }
        attBtn.disabled = true;
        fetch('/apps/inquiry-hub/api/inquiries/' + ID + '/reply-attachments', {
          method: 'POST',
          headers: { 'Content-Type': 'application/octet-stream', 'X-File-Name': encodeURIComponent(f.name) },
          body: f,
        }).then(function(r) { return r.json().catch(function(){ return {}; }).then(function(j) { if (!r.ok) throw new Error(j.error || ('HTTP ' + r.status)); return j; }); })
          .then(function(j) { ATT.push({ id: j.id, name: j.fileName, size: j.fileSize }); render(); attBtn.disabled = false; next(); })
          .catch(function(e) { toast('添付失敗: ' + f.name + ' — ' + e.message); attBtn.disabled = false; next(); });
      })();
    });
  })();
  var MALL_COMPLETE = ${mallCompleteSupported ? 'true' : 'false'};
  var replyBtn = document.getElementById('replyBtn');
  var replyCompleteBtn = document.getElementById('replyCompleteBtn');
  // 「送信」と「送信して回答完了」の2ボタン (2026-08-26 スタッフ要望: ストクリで別途「回答完了」を押す手間をなくす)
  function submitReply(complete) {
    var body = document.getElementById('replyBody').value.trim();
    if (!body) { toast('本文が空です'); return; }
    // AI下書きの【要確認:】が残ったまま送ろうとしたら止める (未確認の事実が客に飛ぶ事故の防止)
    var left = (body.match(PLACEHOLDER_RE) || []).filter(function(v, i, a) { return a.indexOf(v) === i; });
    if (left.length && !confirm('⚠️ 未確認の箇所が' + left.length + '件残っています:\\n\\n' + left.join('\\n')
      + '\\n\\nこのまま送信ジョブを作成しますか? (通常は実際の内容に置き換えてから送ります)')) return;
    var preview = body.length > 300 ? body.slice(0, 300) + '…' : body;
    var attIds = ATT.map(function(a) { return a.id; });
    if (!confirm('以下の内容で送信ジョブを作成しますか?\\n\\n宛先: ' + REPLY_CH + ' の顧客'
      + (attIds.length ? '\\n添付: ' + ATT.map(function(a) { return a.name; }).join(' / ') : '')
      + (complete ? (MALL_COMPLETE ? '\\n送信と同時にモール側も「回答完了」にします' : '\\n送信後に「完了」にします') : '\\n送信後は「返信処理中」になります') + '\\n\\n' + preview)) return;
    replyBtn.disabled = true; replyCompleteBtn.disabled = true;
    post('/reply', { body: body, clientOperationId: REPLY_OP_ID, baseConversationRev: REPLY_BASE_REV, completeOnSend: complete, attachmentIds: attIds })
      .then(function(r) { toast(r.duplicate ? '既に同じ操作で作成済みです' : '送信ジョブを作成しました'); setTimeout(function(){ location.reload(); }, 900); })
      .catch(function(e) { toast('作成失敗: ' + e.message); replyBtn.disabled = false; replyCompleteBtn.disabled = false; });
  }
  if (replyBtn && replyCompleteBtn) {
    replyBtn.addEventListener('click', function() { submitReply(false); });
    replyCompleteBtn.addEventListener('click', function() { submitReply(true); });
  }

  // ─── 📦返品・交換案件 (2026-09-01) ───
  var CASE_STEP_NAMES = ${JSON.stringify(Object.fromEntries(
    Object.entries(STEP_TEMPLATES).map(([k, v]) => [k, v.map(s => s.name)])))};
  var caseTypeSel = document.getElementById('caseType');
  if (caseTypeSel) {
    caseTypeSel.addEventListener('change', function() {
      var names = CASE_STEP_NAMES[caseTypeSel.value];
      var el = document.getElementById('casePreview');
      el.textContent = names
        ? names.length + '件の工程が作られます: ' + names.join(' / ')
        : '担当と工程は自動で入ります (入力はこの2つだけ)';
    });
  }
  var makeCaseBtn = document.getElementById('makeCase');
  if (makeCaseBtn) makeCaseBtn.addEventListener('click', function() {
    var type = caseTypeSel.value, date = document.getElementById('caseDate').value;
    if (!type) { toast('案件種別を選んでください'); return; }
    if (!date) { toast('次回確認日を入れてください'); return; }
    makeCaseBtn.disabled = true;
    createCaseReq(type, date, false);
  });
  function createCaseReq(type, date, allowDuplicate) {
    fetch('/apps/inquiry-hub/api/cases', { method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ inquiryId: ${id}, caseType: type, nextActionDate: date,
        allowDuplicate: !!allowDuplicate }) })
      .then(function(r) { return r.json().then(function(j) { return { status: r.status, ok: r.ok, j: j }; }); })
      .then(function(x) {
        // 同じ問い合わせに進行中の案件がある = 二度押しの可能性。作るなら人が確認してから
        if (x.status === 409 && x.j.duplicate) {
          if (confirm('この問い合わせには進行中の案件 ' + x.j.caseNo + ' があります。\\n'
            + '別の案件として新しく作りますか?\\n'
            + '(同じ件なら「キャンセル」を押して、既存の案件を開いてください)')) {
            createCaseReq(type, date, true);
          } else { makeCaseBtn.disabled = false; }
          return;
        }
        if (!x.ok) { toast(x.j.error || '作成できませんでした'); makeCaseBtn.disabled = false; return; }
        toast(x.j.case_no + ' を作成しました');
        setTimeout(function() { location.href = '/apps/inquiry-hub/cases/' + x.j.id; }, 700);
      })
      .catch(function(e) { toast('作成失敗: ' + e.message); makeCaseBtn.disabled = false; });
  }
  var noCaseBtn = document.getElementById('noCase');
  if (noCaseBtn) noCaseBtn.addEventListener('click', function() {
    fetch('/apps/inquiry-hub/api/inquiries/${id}/no-case', { method: 'POST',
      headers: { 'Content-Type': 'application/json' }, body: '{}' })
      .then(function() { toast('この問い合わせは案件にしません'); setTimeout(function(){ location.reload(); }, 700); });
  });
  var undoNoCase = document.getElementById('undoNoCase');
  if (undoNoCase) undoNoCase.addEventListener('click', function(e) {
    e.preventDefault();
    fetch('/apps/inquiry-hub/api/inquiries/${id}/triage-reset', { method: 'POST',
      headers: { 'Content-Type': 'application/json' }, body: '{}' })
      .then(function() { location.reload(); });
  });`;

  res.send(pageShell(`問い合わせ — ${inq.subject || inq.external_inquiry_id}`,
    backFolder ? `folder:${backFolder.id}` : backView, body, script, { group: backGroup }));
});

// ─── 操作API (すべて操作ログを記録) ───
// CSRF防御 (Codex R1 medium): ポータルはセッションCookie (SameSite=Lax) のみで CSRFトークンが無く、
// グローバル express.urlencoded() があるためクロスサイトのform POSTでも req.body が埋まる。
// Origin検証 (ヘッダーがあれば自ホストと一致必須) + JSON Content-Type 必須の二段で更新系を守る。
router.use('/api/', (req, res, next) => {
  if (req.method !== 'GET' && req.method !== 'HEAD') {
    const origin = req.headers.origin;
    if (origin) {
      let host = null;
      try { host = new URL(origin).host; } catch { /* 不正Originはhost不一致として拒否 */ }
      if (host !== req.headers.host) return res.status(403).json({ error: '不正なリクエスト元です' });
    }
    // 送信用添付のアップロードだけはバイナリ (octet-stream) を許可 (Origin検証は上で共通に通過済み)
    const isBinaryUpload = /^\/inquiries\/\d+\/reply-attachments$/.test(req.path) && req.is('application/octet-stream');
    if (!req.is('application/json') && !isBinaryUpload) {
      return res.status(415).json({ error: 'Content-Type は application/json が必要です' });
    }
  }
  next();
});

/**
 * 返信の送信用添付アップロード (2026-08-20 スタッフ要望「PDFなどを添付できるように」)。
 * multerを増やさず、1リクエスト1ファイルの生バイナリで受ける (ファイル名は X-File-Name ヘッダ。
 * URLエンコードで日本語対応)。検証 (形式・サイズ・件数) は reply-attachments.js。
 */
router.post('/api/inquiries/:id(\\d+)/reply-attachments',
  express.raw({ type: 'application/octet-stream', limit: MAX_FILE_BYTES + 1024 * 1024 }),
  (req, res) => {
    const inq = loadInquiry(req, res); if (!inq) return;
    if (!replyEditorEnabled()) return res.status(403).json({ error: '返信機能が有効になっていません' });
    let fileName = 'attachment';
    try { fileName = decodeURIComponent(String(req.headers['x-file-name'] || '')); } catch { /* 不正エンコードは既定名 */ }
    try {
      const saved = saveReplyAttachment({ inquiryId: inq.id, fileName, buffer: req.body, uploadedBy: actorOf(req) });
      res.json({ ok: true, ...saved });
    } catch (e) {
      res.status(400).json({ error: String(e?.message || e).slice(0, 300) });
    }
  });

/** ジョブ未紐付けの添付を取り消す (紐付け済みは消せない) */
router.post('/api/inquiries/:id(\\d+)/reply-attachments/:attId(\\d+)/delete', (req, res) => {
  const inq = loadInquiry(req, res); if (!inq) return;
  try {
    deletePendingAttachment(inq.id, Number(req.params.attId));
    res.json({ ok: true });
  } catch (e) {
    res.status(400).json({ error: String(e?.message || e).slice(0, 300) });
  }
});

// 返信エディタのテンプレート選択用 (2026-08-15 スタッフ要望)。📄タブと同じ listTemplates を
// JSONで返す。本文全文を含むため詳細ページには埋め込まず、初回操作時にここから取得する
router.get('/api/templates', (req, res) => {
  const { rows, categories } = listTemplates({});
  res.json({
    categories: categories.map(c => c.category),
    templates: rows.map(t => ({
      id: t.id,
      name: t.template_name,
      category: t.category || '',
      subject: t.subject || '',
      keywords: t.keywords || '',   // メールディーラーの「キーワード」列。画面の絞り込み検索の対象
      body: t.template_body || '',
      bodyBottom: t.body_bottom || '',
    })),
  });
});

// 一括操作 (2026-08-17 スタッフ要望)。一覧のチェックボックスから状態/フォルダ/担当/既読をまとめて変更。
// 削除系は提供しない (取り消せない操作は一括にしない)。1件ずつ操作ログを残す
router.post('/api/inquiries/bulk', (req, res) => {
  const b = req.body || {};
  try {
    const r = bulkUpdateInquiries(b.ids, b.ops || {}, { actorId: actorOf(req), source: 'bulk' });
    console.log(`[inquiry-hub] 一括操作 ${JSON.stringify(b.ops)} by ${actorOf(req)} — ${r.updated}件変更 / ${r.skipped}件変更なし`);
    res.json({ ok: true, ...r });
  } catch (e) {
    res.status(400).json({ error: String(e?.message || e).slice(0, 300) });
  }
});

// 「この条件の全件を選択」一括操作 (2026-08-20 中原さん要望: 新着1,500件超をまとめて完了に。
// ページの50件では足りない)。一覧と同じフィルタ条件をサーバーで再評価して全件に適用する。
// dryRun=true は件数だけ返す (画面が確認ダイアログに出す)。上限 FILTER_BULK_MAX 件
const BULK_FILTER_KEYS = ['view', 'status', 'group', 'channel', 'shop', 'folder', 'label', 'assigned', 'unread', 'attention', 'ai', 'from', 'to', 'q'];
router.post('/api/inquiries/bulk-by-filter', (req, res) => {
  const b = req.body || {};
  // フィルタは許可キーのみ・文字列化して受ける (listInquiries と同じ解釈になる)
  const filter = {};
  for (const k of BULK_FILTER_KEYS) {
    const v = (b.filter || {})[k];
    if (v != null && v !== '') filter[k] = String(v);
  }
  try {
    const ids = listInquiryIdsByFilter(filter);
    if (b.dryRun === true) return res.json({ ok: true, matched: ids.length });
    const r = bulkUpdateInquiries(ids, b.ops || {}, { actorId: actorOf(req), maxItems: FILTER_BULK_MAX,
      source: 'bulk_filter', filter });
    console.log(`[inquiry-hub] 全件一括操作 filter=${JSON.stringify(filter)} ops=${JSON.stringify(b.ops)} by ${actorOf(req)} — ${r.updated}件変更 / ${r.skipped}件変更なし`);
    res.json({ ok: true, ...r });
  } catch (e) {
    res.status(400).json({ error: String(e?.message || e).slice(0, 300) });
  }
});

// ✨ AI下書き (2026-08-17 第1段階)。社内Q&A・返信テンプレートから関連するものを選んでAIに渡し、
// 返信の下書きを作る。⚠️根拠のない事柄は AI に【要確認: ○○】で残させ、人が埋めてから送る運用。
// 自動学習 (返信結果でナレッジを書き換える) は意図的に入れていない — まず「どこで詰まるか」を実データで見る
router.post('/api/inquiries/:id(\\d+)/ai-draft', async (req, res) => {
  const inq = loadInquiry(req, res); if (!inq) return;
  if (!aiRewriteEnabled()) return res.status(403).json({ error: 'AI下書きは未設定です (env OPENAI_API_KEY)' });
  try {
    const messages = getDB().prepare(`SELECT is_incoming, sender_name, message_body_text
      FROM inquiry_messages WHERE inquiry_id = ?
      ORDER BY COALESCE(received_at, sent_at, created_at), id`).all(inq.id);
    const firstIncoming = messages.find(m => m.is_incoming)?.message_body_text || '';
    const knowledge = findRelevantKnowledge({
      subject: inq.subject, body: firstIncoming,
      productName: inq.product_name, productCode: inq.product_code,
    });
    const r = await draftReply({
      inquiry: inq, messages,
      knowledgeText: formatKnowledgeForPrompt(knowledge),
    });
    console.log(`[inquiry-hub] AI下書き (model=${r.model}, Q&A${knowledge.qa.length}件/テンプレ${knowledge.templates.length}件参照, 要確認${r.placeholders.length}箇所) by ${actorOf(req)} inquiry=${inq.id}`);
    res.json({ ok: true, text: r.text, placeholders: r.placeholders,
      sources: { qa: knowledge.qa.map(q => q.title), templates: knowledge.templates.map(t => t.name) } });
  } catch (e) {
    res.status(400).json({ error: String(e?.message || e).slice(0, 300) });
  }
});

// ✨ 返信文のAI書き換え (2026-08-17 スタッフ要望)。OPENAI_API_KEY があるときだけUI表示・実行可。
// 最新の顧客メッセージを文脈として渡し、文体だけ整える (内容の追加はプロンプトで禁止)
router.post('/api/inquiries/:id(\\d+)/ai-rewrite', async (req, res) => {
  const inq = loadInquiry(req, res); if (!inq) return;
  if (!aiRewriteEnabled()) return res.status(403).json({ error: 'AI書き換えは未設定です (env OPENAI_API_KEY)' });
  const b = req.body || {};
  try {
    const lastIncoming = getDB().prepare(`SELECT message_body_text FROM inquiry_messages
      WHERE inquiry_id = ? AND is_incoming = 1
      ORDER BY COALESCE(received_at, sent_at, created_at) DESC, id DESC LIMIT 1`).get(inq.id);
    const r = await rewriteReply({
      style: String(b.style || ''),
      text: String(b.text || ''),
      inquiryContext: lastIncoming?.message_body_text || '',
    });
    console.log(`[inquiry-hub] AI書き換え (${b.style}, model=${r.model}, ${String(b.text || '').length}→${r.text.length}文字) by ${actorOf(req)} inquiry=${inq.id}`);
    res.json({ ok: true, text: r.text });
  } catch (e) {
    res.status(400).json({ error: String(e?.message || e).slice(0, 300) });
  }
});

const NOW_SQL = "strftime('%Y-%m-%dT%H:%M:%SZ','now')"; // 日時の正準形式 (db.js toUtcIso と同形)

function loadInquiry(req, res) {
  const id = Number(req.params.id);
  const inq = Number.isInteger(id) ? getDB().prepare('SELECT * FROM inquiries WHERE id = ?').get(id) : null;
  if (!inq) { res.status(404).json({ error: '問い合わせが見つかりません' }); return null; }
  return inq;
}

/** 更新と操作ログを同一トランザクションで実行 (Codex R1 medium: 監査ログ欠落防止) */
function applyChange(inq, req, actionType, before, after, updateFn) {
  getDB().transaction(() => {
    updateFn();
    logActivity(inq.id, { userId: actorOf(req), actionType, before, after });
  })();
}

// 顧客情報の手入力 (2026-08-31 中原さん要望): 注文番号 (+そのモール)・商品コード・商品名を画面から保存 (自動保存)。
// body は { order_number?, order_mall?, product_code?, product_name? } の部分更新。'' = 空にする。
// モール同期で確定した項目 (customer-info.js のロック) への変更は 409 で拒否する。
// 応答の links は保存後の注文リンク (画面がリロード無しで モール/NE のリンクを差し替えるため)、
// guessed_mall は注文番号の形式から自動判定したモール (楽天/Amazon/Yahoo!)
router.post('/api/inquiries/:id/customer-info', (req, res) => {
  const inq = loadInquiry(req, res); if (!inq) return;
  const body = req.body || {};
  const patch = {};
  for (const k of [...CUSTOMER_INFO_KEYS, 'order_mall']) if (Object.prototype.hasOwnProperty.call(body, k)) patch[k] = body[k];
  if (!Object.keys(patch).length) return res.status(400).json({ error: '保存する項目がありません (注文番号・モール・商品コード・商品名)' });
  try {
    const r = setCustomerInfo(inq.id, patch, actorOf(req));
    // orderLinksOf はロック判定に manual_fields、モールに order_mall が要るので行ごと読む
    const now = getDB().prepare('SELECT i.*, s.account_identifier FROM inquiries i JOIN shops s ON s.id = i.shop_id WHERE i.id = ?').get(inq.id);
    const links = orderLinksOf(now || inq);
    res.json({ ok: true, unchanged: !!r.unchanged, changed: r.changed, info: r.state,
      order_mall: r.order_mall, guessed_mall: r.guessedMall,
      links: { mall: links.mall, mall_label: links.mallLabel, mall_short: links.mallShort, order_number: links.orderNumber,
        mall_order_url: links.mallOrderUrl, mall_admin_url: links.mallAdminUrl, ne_order_url: links.neOrderUrl } });
  } catch (e) {
    res.status(e && e.code === 'LOCKED' ? 409 : 400).json({ error: String(e?.message || e).slice(0, 200) });
  }
});

router.post('/api/inquiries/:id/status', (req, res) => {
  const inq = loadInquiry(req, res); if (!inq) return;
  const st = String((req.body || {}).status || '');
  if (!STATUSES[st]) return res.status(400).json({ error: '不正なステータス' });
  if (st === inq.internal_status) return res.json({ ok: true, unchanged: true });
  // completed_at は done への遷移時のみ刻む (既に値があれば保持)。done から離れたら消す (Codex R1 medium)
  applyChange(inq, req, 'status_change', { status: inq.internal_status }, { status: st }, () => {
    getDB().prepare(`UPDATE inquiries SET internal_status = ?,
      completed_at = CASE WHEN ? = 'done' THEN COALESCE(completed_at, ${NOW_SQL}) ELSE NULL END,
      updated_at = ${NOW_SQL} WHERE id = ?`).run(st, st, inq.id);
  });
  res.json({ ok: true });
});

router.post('/api/inquiries/:id/assign', (req, res) => {
  const inq = loadInquiry(req, res); if (!inq) return;
  const user = String((req.body || {}).user || '').trim().slice(0, 200);
  applyChange(inq, req, 'assign', { assigned: inq.assigned_user_id }, { assigned: user || null }, () => {
    getDB().prepare(`UPDATE inquiries SET assigned_user_id = ?, updated_at = ${NOW_SQL} WHERE id = ?`).run(user || null, inq.id);
  });
  res.json({ ok: true });
});

router.post('/api/inquiries/:id/read', (req, res) => {
  const inq = loadInquiry(req, res); if (!inq) return;
  const v = (req.body || {}).is_unread ? 1 : 0;
  if (v === inq.is_unread) return res.json({ ok: true, unchanged: true });
  applyChange(inq, req, 'read_toggle', { is_unread: inq.is_unread }, { is_unread: v }, () => {
    getDB().prepare(`UPDATE inquiries SET is_unread = ?, updated_at = ${NOW_SQL} WHERE id = ?`).run(v, inq.id);
  });
  res.json({ ok: true });
});

router.post('/api/inquiries/:id/attention', (req, res) => {
  const inq = loadInquiry(req, res); if (!inq) return;
  const v = (req.body || {}).needs_attention ? 1 : 0;
  applyChange(inq, req, 'attention_toggle', { needs_attention: inq.needs_attention }, { needs_attention: v }, () => {
    getDB().prepare(`UPDATE inquiries SET needs_attention = ?, updated_at = ${NOW_SQL} WHERE id = ?`).run(v, inq.id);
  });
  res.json({ ok: true });
});

// 任意フォルダの割当 (folder_id: 数値 or null=未分類)。ステータスには影響しない
router.post('/api/inquiries/:id/folder', (req, res) => {
  const inq = loadInquiry(req, res); if (!inq) return;
  const raw = (req.body || {}).folder_id;
  if (raw !== null && raw !== undefined && !Number.isInteger(raw)) {
    return res.status(400).json({ error: '不正なフォルダ指定です' });
  }
  try {
    const r = setInquiryFolder(inq.id, raw ?? null, actorOf(req));
    res.json({ ok: true, folder: r.folder, unchanged: !!r.unchanged });
  } catch (e) {
    res.status(400).json({ error: String(e?.message || e).slice(0, 200) });
  }
});

// 色付きラベルの割当 (label_id: 数値 or null=ラベルなし)。ステータスには影響しない (2026-08-24)
router.post('/api/inquiries/:id/label', (req, res) => {
  const inq = loadInquiry(req, res); if (!inq) return;
  const raw = (req.body || {}).label_id;
  if (raw !== null && raw !== undefined && !Number.isInteger(raw)) {
    return res.status(400).json({ error: '不正なラベル指定です' });
  }
  try {
    const r = setInquiryLabel(inq.id, raw ?? null, actorOf(req));
    res.json({ ok: true, label: r.label, unchanged: !!r.unchanged });
  } catch (e) {
    res.status(400).json({ error: String(e?.message || e).slice(0, 200) });
  }
});

router.post('/api/inquiries/:id/ai-flag', (req, res) => {
  const inq = loadInquiry(req, res); if (!inq) return;
  const v = Number((req.body || {}).ai_needed);
  if (![0, 1, 2, 3].includes(v)) return res.status(400).json({ error: '不正なAIフラグ' });
  applyChange(inq, req, 'ai_flag', { ai_needed: inq.ai_needed }, { ai_needed: v }, () => {
    getDB().prepare(`UPDATE inquiries SET ai_needed = ?, updated_at = ${NOW_SQL} WHERE id = ?`).run(v, inq.id);
  });
  res.json({ ok: true });
});

// 返信ジョブ作成 (Dark Launch。設計書§7.1#3 / §8.3)。実際の送信は送信ワーカー (Step 3〜) が担う
router.post('/api/inquiries/:id/reply', async (req, res) => {
  if (!replyEditorEnabled()) return res.status(403).json({ error: '返信機能は現在無効です (INQUIRY_HUB_REPLY_EDITOR_ENABLED)' });
  const inq = loadInquiry(req, res); if (!inq) return;
  const body = String((req.body || {}).body || '').trim();
  const opId = String((req.body || {}).clientOperationId || '');
  const baseRev = Number((req.body || {}).baseConversationRev);
  if (!body) return res.status(400).json({ error: '本文が空です' });
  if (body.length > 10000) return res.status(400).json({ error: '本文が長すぎます (10000文字まで)' });
  if (inq.channel_type === 'yahoo' && body.length > 2000) {
    return res.status(400).json({ error: `Yahoo!の返信は2000文字までです (現在${body.length}文字。短くしてください)` });
  }
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(opId)) {
    return res.status(400).json({ error: '不正な操作IDです (画面を再読み込みしてください)' });
  }
  if (!Number.isInteger(baseRev)) return res.status(400).json({ error: '不正なリクエストです (baseConversationRev)' });
  // completeOnSend は boolean のみ受け付ける (文字列 "false" を真と解釈して意図せず完了に
  // してしまわないため。Codexレビュー反映)
  const completeOnSend = (req.body || {}).completeOnSend;
  if (completeOnSend !== undefined && typeof completeOnSend !== 'boolean') {
    return res.status(400).json({ error: '不正なリクエストです (completeOnSend は true/false)' });
  }
  // 送信用添付 (2026-08-20)。ID配列のみ受け、実体・所属・件数・サイズの検証は createReplyJob 内の
  // claimAttachmentsForJob (同一トランザクション) が行う
  const attachmentIds = (req.body || {}).attachmentIds;
  if (attachmentIds !== undefined && (!Array.isArray(attachmentIds) || !attachmentIds.every(n => Number.isInteger(n) && n > 0))) {
    return res.status(400).json({ error: '不正なリクエストです (attachmentIds は数値の配列)' });
  }
  // 宛先ドメインがメールを受け取れるかの事前確認 (2026-08-27)。
  // 打ち間違いを送信前に止める。DNSが引けないときは止めない (fail-open。mx-check.js)。
  //  - 同じ操作IDの再POST (レスポンス消失後のリトライ) は既に作ったジョブを返すのが先。
  //    ここでDNS判定に引っかけると「送信済みなのに失敗表示」になる (Codexレビュー High)
  //  - customer_identifier が空の問い合わせは送信時にスレッドから宛先を復元するので見ない
  //    (その経路は送信ワーカー側で同じ確認をする。adapters/gmail.js)
  const alreadyJob = getDB().prepare('SELECT id FROM outbox_replies WHERE client_operation_id = ?').get(opId);
  if (!alreadyJob && inq.channel_type === 'email' && String(inq.customer_identifier || '').includes('@')) {
    const problem = await recipientDomainProblem(inq.customer_identifier);
    if (problem) return res.status(400).json({ error: `${problem} (未送信)` });
  }
  try {
    const r = createReplyJob({
      inquiryId: inq.id, channelType: inq.channel_type, bodyText: body,
      createdBy: actorOf(req), clientOperationId: opId, baseConversationRev: baseRev,
      completeOnSend: completeOnSend === true,   // メールディーラーの「返信して完了」
      attachmentIds: attachmentIds || [],
    });
    if (r.conflict) return res.status(409).json({ error: r.conflict });
    res.json({ ok: true, id: r.id, duplicate: !r.created });
  } catch (e) {
    res.status(400).json({ error: String(e?.message || e).slice(0, 300) });
  }
});

/**
 * 詳細画面から「今後の自動処理」ルールを作る (メールチャネルのみ。2026-07-25 中原さん要望)。
 * 条件はこの問い合わせの差出人/件名から組み立てる (任意条件は📧メールルールタブで)。
 * dryRun=true なら件数の下見だけ (画面が確認ダイアログに出す)
 */
router.post('/api/inquiries/:id/mail-rule', (req, res) => {
  const inq = loadInquiry(req, res); if (!inq) return;
  if (inq.channel_type !== 'email') return res.status(400).json({ error: 'メール以外のチャネルでは使えません' });
  const b = req.body || {};
  const action = String(b.action || '');
  if (!['skip', 'import_done', 'import'].includes(action)) return res.status(400).json({ error: '不正な扱いです' });
  // フォルダ振り分け (2026-08-17 中原さん要望: Gmailの振り分けのように、その場で「今後はこのフォルダへ」)
  const folderId = b.folderId != null && b.folderId !== '' ? Number(b.folderId) : null;
  if (folderId != null && !Number.isInteger(folderId)) return res.status(400).json({ error: 'フォルダ指定が不正です' });
  const folder = folderId != null ? listFolders().find(f => f.id === folderId) : null;
  if (folderId != null && !folder) return res.status(400).json({ error: '指定のフォルダが存在しません' });
  // ラベル付与 (2026-08-24 中原さん要望: メールディーラーのラベル振り分け相当)
  const labelId = b.labelId != null && b.labelId !== '' ? Number(b.labelId) : null;
  if (labelId != null && !Number.isInteger(labelId)) return res.status(400).json({ error: 'ラベル指定が不正です' });
  const label = labelId != null ? listLabels().find(l => l.id === labelId) : null;
  if (labelId != null && !label) return res.status(400).json({ error: '指定のラベルが存在しません' });
  if (action === 'import' && !folder && !label) return res.status(400).json({ error: '「振り分けだけする」はフォルダかラベルの指定が必要です' });

  // 条件は画面が組み立てて配列で渡す (メールディーラーと同じく複数条件の組み合わせが可能)。
  // 値の検証は mail-rules.js の validateConditions (フィールド/演算子のallow-list) に委ねる
  const conditions = Array.isArray(b.conditions) ? b.conditions : null;
  if (!conditions || conditions.length === 0) return res.status(400).json({ error: '条件を1つ以上指定してください' });
  const matchMode = b.matchMode === 'any' ? 'any' : 'all';
  const glue = matchMode === 'any' ? ' または ' : ' かつ ';
  let description;
  try {
    validateConditions(conditions);
    description = conditions
      .map(c => `${RULE_FIELD_LABELS[c.field] || c.field}が「${c.value}」${RULE_OP_LABELS[c.op] || c.op}`)
      .join(glue);
  } catch (e) {
    return res.status(400).json({ error: String(e?.message || e).slice(0, 200) });
  }
  const assignDesc = [folder ? `📁${folder.name}へ` : null, label ? `🏷️${label.name}` : null].filter(Boolean).join(' + ');
  const actionLabel = action === 'skip' ? '取り込まない'
    : action === 'import' ? assignDesc
      : `完了扱い${assignDesc ? ` + ${assignDesc}` : ''}`;

  const applyToExisting = b.applyToExisting === true;
  const applicable = canApplyToExisting(conditions);
  try {
    // 下見: ルールは作らず、既存で何件が対象になるかだけ返す
    if (b.dryRun === true) {
      const matched = applicable
        ? applyRuleToExistingMails(conditions, { matchMode, apply: false, action, folderId, labelId }).matched : 0;
      return res.json({ ok: true, description, canApplyToExisting: applicable, matched: applyToExisting ? matched : 0 });
    }
    // ルール作成と既存への一括適用は同一トランザクションで (途中で失敗したときに
    // ルールだけ残り、再試行で重複ルールができるのを防ぐ。Codexレビュー反映)
    const { created, completed, foldered, labeled } = getDB().transaction(() => {
      const c = addMailRule({
        name: `${description} → ${actionLabel}`.slice(0, 200),
        matchMode, conditions, action, priority: 50, folderId, labelId,
      });
      // 既存への一括適用は差出人・件名の条件のみ (Reply-To/To/本文は inquiries に無いため)。
      // 非対応の条件でも「今後の取り込み」からはルールが効く
      const r = (applyToExisting && applicable)
        ? applyRuleToExistingMails(conditions, { matchMode, apply: true, actorId: actorOf(req), action, folderId, labelId })
        : { completed: 0, foldered: 0, labeled: 0 };
      return { created: c, completed: r.completed, foldered: r.foldered, labeled: r.labeled };
    }).immediate();
    console.log(`[inquiry-hub] メールルール作成 (${description} → ${actionLabel}) by ${actorOf(req)} / 既存 完了${completed}件・フォルダ${foldered}件・ラベル${labeled}件`);
    // 先勝ち衝突の検知 (2026-08-20 実事故対応): この問い合わせと同じメールを流したとき、
    // 別の既存ルールが先に当たるなら新ルールは効かない。作成は成立させたうえで画面に警告を返す
    // (移行ルール858件と重なりやすい。優先度はここで直させず、メールルールタブへ誘導する)
    let shadowedBy = null;
    const probe = evaluateMailRules({ from: inq.customer_identifier || '', subject: inq.subject || '' });
    if (probe && probe.ruleId !== created.id) {
      shadowedBy = { id: probe.ruleId, name: probe.ruleName, priority: probe.priority, action: probe.action };
    }
    res.json({ ok: true, id: created.id, description, completed, foldered, labeled, shadowedBy });
  } catch (e) {
    res.status(400).json({ error: String(e?.message || e).slice(0, 300) });
  }
});

router.post('/api/inquiries/:id/notes', (req, res) => {
  const inq = loadInquiry(req, res); if (!inq) return;
  const body = String((req.body || {}).body || '').trim();
  if (!body) return res.status(400).json({ error: 'メモが空です' });
  if (body.length > 5000) return res.status(400).json({ error: 'メモが長すぎます (5000文字まで)' });
  applyChange(inq, req, 'note_add', null, { length: body.length }, () => {
    getDB().prepare('INSERT INTO internal_notes (inquiry_id, user_id, body) VALUES (?, ?, ?)').run(inq.id, actorOf(req), body);
  });
  res.json({ ok: true });
});

// ─── カテゴリ階層ツリー (メールディーラーのグループ階層は ' > ' 区切り。例: 商品についての質問 > エッセンシャルオイル > ハッカ油) ───
function buildCategoryTree(rows) {
  const root = { children: new Map(), items: [], total: 0 };
  for (const r of rows) {
    const parts = r.category ? String(r.category).split(' > ').map(s => s.trim()).filter(Boolean) : [];
    let node = root;
    root.total++;
    for (const p of parts) {
      if (!node.children.has(p)) node.children.set(p, { children: new Map(), items: [], total: 0 });
      node = node.children.get(p);
      node.total++;
    }
    node.items.push(r);
  }
  return root;
}

/**
 * 階層アコーディオンHTML。デフォルトは第一階層のみ表示 (全グループ閉)。
 * openAll=true (検索/絞込中や「全て展開」) で全階層+中身を開いた状態で出す。
 */
function renderCategoryTree(node, renderItem, openAll, depth = 0) {
  let html = '';
  for (const [name, child] of node.children) {
    html += `<details class="grp"${openAll ? ' open' : ''}>
      <summary><span class="grp-arrow">▶</span>📁 ${he(name)} <span class="grp-count">${child.total}件</span></summary>
      <div class="grp-body">${renderCategoryTree(child, renderItem, openAll, depth + 1)}${child.items.map(renderItem).join('')}</div>
    </details>`;
  }
  if (depth === 0 && node.items.length) {
    html += `<details class="grp"${openAll ? ' open' : ''}>
      <summary><span class="grp-arrow">▶</span>📁 (グループなし) <span class="grp-count">${node.items.length}件</span></summary>
      <div class="grp-body">${node.items.map(renderItem).join('')}</div>
    </details>`;
  }
  return html;
}

/**
 * カテゴリselectのoption (階層はインデント表示)。
 * ラベルにフルパスを残す: 別親の同名サブグループ (食品>オイル と 化粧品>オイル) を区別するため (Codexレビュー反映)
 */
function categoryOptions(categories, current) {
  return categories.map(c => {
    const parts = String(c.category).split(' > ');
    const label = '　'.repeat(parts.length - 1) + (parts.length > 1 ? '└ ' : '') + parts.join(' › ') + ` (${c.c})`;
    return `<option value="${he(c.category)}"${String(current || '') === String(c.category) ? ' selected' : ''}>${he(label)}</option>`;
  }).join('');
}

// 「全て展開/全て閉じる」ボタン (details.grp を一括開閉)
const EXPAND_BUTTONS = `
    <button class="ghost" type="button" id="expandAll">⬇️ 全て展開</button>
    <button class="ghost" type="button" id="collapseAll">⬆️ 全て閉じる</button>`;
const EXPAND_SCRIPT = `
  document.getElementById('expandAll').addEventListener('click', function() {
    document.querySelectorAll('details.grp').forEach(function(d) { d.open = true; });
  });
  document.getElementById('collapseAll').addEventListener('click', function() {
    document.querySelectorAll('details.grp').forEach(function(d) { d.open = false; });
  });`;

// ─── テンプレート管理 (メールディーラーCSV取込 + CRUD) ───
// ═══════════════════════════════════════════════════════════════
// ✉️ 新規メール作成 (2026-08-27 中原さん要望「メール作成ボタンからメールが送れるように」
//    「押したらテンプレート・To/From設定・署名を選ぶ画面へ飛ぶように」= メールディーラーと同じ2段階)
//      1段目 /compose      … テンプレート / To-From設定 / 署名を選ぶ
//      2段目 /compose/new  … 宛先・件名・本文を書いて送る (テンプレ本文+署名が入った状態で開く)
//    送信の仕組みは返信とまったく同じ (compose.js が問い合わせスレッドを1件作り、あとは outbox)。
//    → 送信済み一覧・スレッド表示・顧客からの返信の紐付けが自動的に効く
// ═══════════════════════════════════════════════════════════════

/** メール送信ワーカーの状態バナー (返信パネルと同じ判定をメールチャネル固定で) */
function mailWorkerBanner() {
  const box = 'border-radius:8px;padding:8px 10px;margin-bottom:10px';
  if (!['true', '1'].includes(process.env.INQUIRY_HUB_OUTBOX_CRON_ENABLED || '')) {
    return `<div class="sub" style="background:#fef3c7;${box}">⚠️ 送信ワーカーは停止中です。作成したメールはまだ実際には送信されません (⚙️運用管理で確認・取消できます)</div>`;
  }
  if (process.env.INQUIRY_HUB_MAIL_SEND_MODE !== 'live') {
    return `<div class="sub" style="background:#e0e7ff;${box}">🧪 DRYRUNモード: 送信ジョブは検証のみで、実際には送信されません (動作確認用)</div>`;
  }
  return '';
}

/** 送信機能そのものが無効なときの案内 (返信エディタと同じ env フラグで制御) */
function composeDisabledBody() {
  return `<div class="panel reply-note">✉️ メール送信機能はまだ有効になっていません (いまはメールディーラーから送ってください)。
    <span class="sub">管理者が env <code>INQUIRY_HUB_REPLY_EDITOR_ENABLED=true</code> を設定すると、この画面からメールを送れます</span></div>`;
}

/** 送信元 (To/From設定) の選択肢。いまは店舗1件だけの想定だが、増えたらそのまま並ぶ */
function mailShopOptions(shops, cur) {
  return shops.map(s => `<option value="${s.id}"${String(cur || '') === String(s.id) ? ' selected' : ''}>`
    + `To/From設定なし (${he(DEFAULT_FROM_ADDRESS)}) — ${he(s.shop_name)}</option>`).join('');
}

// ─── 1段目: テンプレート / To-From設定 / 署名を選ぶ ───
router.get('/compose', (req, res) => {
  if (!replyEditorEnabled()) {
    return res.send(pageShell('問い合わせ管理 — メール作成', 'compose', composeDisabledBody(), ''));
  }
  // 送られないまま残った下書き (添付用の器) の掃除はここで行う (cronを増やさない)
  try { pruneStaleComposeDrafts(); } catch (e) { console.warn(`[inquiry-hub] 下書き掃除に失敗: ${e?.message || e}`); }

  let shops = [];
  try { shops = listMailShops(); } catch { /* 初期化前は空 */ }
  if (!shops.length) {
    return res.send(pageShell('問い合わせ管理 — メール作成', 'compose',
      `<div class="card empty">メール送信に使える店舗が登録されていません。<a href="/apps/inquiry-hub/admin">⚙️運用管理</a>で確認してください</div>`, ''));
  }
  const signatures = listSignatures();
  const defSig = signatures.find(s => s.is_default) || null;

  const body = `
  <div class="view-hint">✉️ <b>新規メール作成</b> — テンプレート、To/From設定、署名を選択してください。
    <span class="sub">(次の画面で宛先・件名・本文を書いて送ります)</span></div>
  ${mailWorkerBanner()}
  <div class="card" style="padding:16px">
    <div class="cform">
      <label class="k" for="tplSel">テンプレート</label>
      <div>
        <div class="row tpl-row" style="margin-bottom:6px">
          <input type="search" id="tplSearch" placeholder="🔍 キーワードで絞り込み"
            title="テンプレート名・グループ・件名・本文・キーワードで絞り込みます (空白区切りで複数指定)">
          <select id="tplSel"><option value="">-------- (使わない)</option></select>
        </div>
        <div class="sub" style="margin-top:4px">選ぶと件名と本文が入った状態で次の画面が開きます (そのあと自由に編集できます)</div>
        <div class="cpreview" id="tplPrev" hidden></div>
      </div>

      <label class="k" for="shopSel">To/From設定</label>
      <div>
        <select id="shopSel">${mailShopOptions(shops, shops[0].id)}</select>
        <div class="sub" style="margin-top:4px">差出人: ${he(DEFAULT_FROM_NAME)} &lt;${he(DEFAULT_FROM_ADDRESS)}&gt;</div>
      </div>

      <label class="k" for="sigSel">署名</label>
      <div>
        <select id="sigSel">
          <option value="">署名なし</option>
          ${signatures.map(s => `<option value="${s.id}"${defSig && defSig.id === s.id ? ' selected' : ''}>${he(s.name)}${s.is_default ? ' (既定)' : ''}</option>`).join('')}
        </select>
        <div class="cpreview" id="sigPrev" hidden></div>
        <div class="sub" style="margin-top:4px">
          ${signatures.length ? '' : 'まだ署名が登録されていません。'}<a href="/apps/inquiry-hub/signatures">✍️ 署名を作る・編集する</a></div>
      </div>
    </div>
    <div class="cactions">
      <button class="pri" id="nextBtn" type="button">次へ →</button>
      <a class="btn-link" href="/apps/inquiry-hub">キャンセル</a>
    </div>
  </div>`;

  const script = `
  var SIGS = ${JSON.stringify(signatures.map(s => ({ id: s.id, body: s.body }))).replace(/</g, '\\u003c')};
  var TPLS = null, CATS = [];
  var tplSel = document.getElementById('tplSel');
  var tplSearch = document.getElementById('tplSearch');
  var sigSel = document.getElementById('sigSel');
  // カテゴリ = optgroup (太字の見出し)。未分類は最後に「その他」(返信画面と同じ並べ方)。
  // 🔍絞り込み中はヒットしたものだけ並べる (2026-09-02 スタッフ要望)
  function fillTplSel() {
    if (!TPLS) return;
    var q = tplSearch.value;
    var shown = tplFilter(TPLS, q);
    var cur = tplSel.value;
    tplSel.textContent = '';
    var head = document.createElement('option');
    head.value = '';
    head.textContent = q.trim()
      ? (shown.length ? '🔍 ' + shown.length + '件ヒット — 選んでください' : '🔍 該当なし (キーワードを変えてください)')
      : '-------- (テンプレートを使わない)';
    tplSel.appendChild(head);
    var order = CATS.slice();
    shown.forEach(function(t) { if (t.category && order.indexOf(t.category) < 0) order.push(t.category); });
    order.push('');
    order.forEach(function(cat) {
      var items = shown.filter(function(t) { return (t.category || '') === cat; });
      if (!items.length) return;
      var g = document.createElement('optgroup');
      g.label = cat || 'その他';
      items.forEach(function(t) {
        var o = document.createElement('option');
        o.value = String(t.id); o.textContent = t.name;
        g.appendChild(o);
      });
      tplSel.appendChild(g);
    });
    tplSel.value = cur;
    if (tplSel.selectedIndex < 0) tplSel.value = '';
  }
  tplSearch.addEventListener('input', fillTplSel);
  // テンプレートは件数が多いので本文ごと埋め込まず、返信画面と同じ /api/templates から取る
  fetch('/apps/inquiry-hub/api/templates')
    .then(function(r) { if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); })
    .then(function(j) {
      TPLS = j.templates || [];
      CATS = j.categories || [];
      fillTplSel();
    })
    .catch(function(e) { toast('テンプレート取得失敗: ' + e.message); });

  function showPreview(el, text) {
    el.textContent = text || '';
    el.hidden = !text;
  }
  tplSel.addEventListener('change', function() {
    var t = (TPLS || []).find(function(x) { return String(x.id) === tplSel.value; });
    showPreview(document.getElementById('tplPrev'),
      t ? ((t.subject ? '件名: ' + t.subject + '\\n\\n' : '') + t.body + (t.bodyBottom ? '\\n\\n' + t.bodyBottom : '')) : '');
  });
  function syncSigPrev() {
    var s = SIGS.find(function(x) { return String(x.id) === sigSel.value; });
    showPreview(document.getElementById('sigPrev'), s ? s.body : '');
  }
  sigSel.addEventListener('change', syncSigPrev);
  syncSigPrev();

  document.getElementById('nextBtn').addEventListener('click', function() {
    var qs = [];
    if (tplSel.value) qs.push('tpl=' + encodeURIComponent(tplSel.value));
    if (sigSel.value) qs.push('sig=' + encodeURIComponent(sigSel.value));
    if (document.getElementById('shopSel').value) qs.push('shop=' + encodeURIComponent(document.getElementById('shopSel').value));
    location.href = '/apps/inquiry-hub/compose/new' + (qs.length ? '?' + qs.join('&') : '');
  });`;

  res.send(pageShell('問い合わせ管理 — メール作成', 'compose', body, script));
});

// ─── 2段目: 宛先・件名・本文を書いて送る ───
router.get('/compose/new', (req, res) => {
  if (!replyEditorEnabled()) {
    return res.send(pageShell('問い合わせ管理 — メール作成', 'compose', composeDisabledBody(), ''));
  }
  const q = req.query || {};
  let shop;
  try {
    shop = resolveMailShop(/^\d+$/.test(String(q.shop || '')) ? Number(q.shop) : null);
  } catch (e) {
    return res.send(pageShell('問い合わせ管理 — メール作成', 'compose',
      `<div class="card empty">${he(String(e?.message || e))}</div>`, ''));
  }
  // 1段目で選んだテンプレート・署名を本文に展開しておく (以後はただのテキストとして編集できる)
  const tpl = /^\d+$/.test(String(q.tpl || ''))
    ? getDB().prepare('SELECT * FROM reply_templates WHERE id = ? AND is_active = 1').get(Number(q.tpl)) : null;
  const sig = /^\d+$/.test(String(q.sig || '')) ? getSignature(Number(q.sig)) : null;
  const tplBody = tpl ? String(tpl.template_body || '') + (tpl.body_bottom ? `\n\n${tpl.body_bottom}` : '') : '';
  const initialBody = composeBodyWithSignature(tplBody, sig && sig.is_active ? sig.body : '');
  const initialSubject = tpl ? String(tpl.subject || '') : '';
  // 操作IDはページ描画時にサーバーが採番 (返信と同じ。二重送信を防ぐ冪等キー)
  const opId = crypto.randomUUID();

  const body = `
  <div class="view-hint"><a href="/apps/inquiry-hub/compose">← テンプレート・署名を選び直す</a></div>
  ${mailWorkerBanner()}
  <div class="card" style="padding:16px">
    <div class="cform">
      <label class="k">送信元</label>
      <div class="sub" style="padding-top:9px">${he(DEFAULT_FROM_NAME)} &lt;${he(DEFAULT_FROM_ADDRESS)}&gt;
        <span style="margin-left:8px">(${he(shop.shop_name)})</span></div>

      <label class="k" for="cTo">宛先 <span style="color:#b91c1c">*</span></label>
      <div>
        <input type="email" id="cTo" placeholder="example@example.com" autocomplete="off" spellcheck="false">
        <div class="sub" style="margin-top:4px">1件だけ指定できます (Cc・複数宛先は未対応)</div>
      </div>

      <label class="k" for="cName">宛先の名前</label>
      <div>
        <input type="text" id="cName" placeholder="(任意) 山田 太郎 様 / 株式会社○○" maxlength="100">
        <div class="sub" style="margin-top:4px">一覧で誰宛か分かるようにするための表示名です (メールのヘッダーには入りません)</div>
      </div>

      <label class="k" for="cSubject">件名 <span style="color:#b91c1c">*</span></label>
      <div><input type="text" id="cSubject" maxlength="${SUBJECT_MAX}" value="${he(initialSubject)}" placeholder="件名"></div>

      <label class="k" for="cBody">本文 <span style="color:#b91c1c">*</span></label>
      <div>
        <div class="row tpl-row" id="tplRow" style="margin-bottom:6px">
          <input type="search" id="tplSearch" placeholder="🔍 キーワードで絞り込み"
            title="テンプレート名・グループ・件名・本文・キーワードで絞り込みます (空白区切りで複数指定)">
          <select id="tplSel" title="テンプレートを選ぶ (カテゴリごとにまとまっています)"><option value="">📄 テンプレートを本文に入れる…</option></select>
        </div>
        <textarea id="cBody" maxlength="${BODY_MAX}" placeholder="本文">${he(initialBody)}</textarea>
        <div class="row rw-row" id="attRow" style="margin-top:8px">
          <button class="ghost" type="button" id="attBtn">📎 ファイルを添付</button>
          <input type="file" id="attFile" multiple accept=".pdf,.png,.jpg,.jpeg,.gif,.webp,.bmp,application/pdf,image/*" style="display:none">
          <span class="sub">${he(ALLOWED_LABEL)}・1ファイル${Math.round(MAX_FILE_BYTES / 1048576)}MB・最大${MAX_FILES_PER_REPLY}つ</span>
        </div>
        <div id="attList"></div>
      </div>
    </div>
    <div class="cactions" style="justify-content:flex-end">
      <span class="sub" style="margin-right:auto">「送信して完了」= 送ったあとの状態を「完了」にします (返信を待つなら「送信」)</span>
      <a class="btn-link" href="/apps/inquiry-hub">キャンセル</a>
      <button class="ghost" id="sendBtn" type="button" title="送信後は「返信処理中」になります">✉️ 送信</button>
      <button class="pri" id="sendDoneBtn" type="button" title="送信後に「完了」にします">✅ 送信して完了</button>
    </div>
  </div>`;

  const script = `
  var SHOP_ID = ${Number(shop.id)};
  var OP_ID = ${JSON.stringify(opId)};
  var DRAFT_ID = null;
  var ATT = [];
  var MAXF = ${MAX_FILES_PER_REPLY}, MAXB = ${MAX_FILE_BYTES};

  function jpost(url, data) {
    return fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data || {}) })
      .then(function(r) { return r.json().catch(function(){ return {}; }).then(function(j) {
        if (!r.ok) throw new Error(j.error || ('HTTP ' + r.status)); return j; }); });
  }
  // 添付は問い合わせIDに紐付けて保存するので、添付する時点で下書き (器) を1件だけ作る。
  // 送られなければ24時間後に掃除される (compose.js)
  function ensureDraft() {
    if (DRAFT_ID) return Promise.resolve(DRAFT_ID);
    return jpost('/apps/inquiry-hub/api/compose/draft', { shopId: SHOP_ID })
      .then(function(j) { DRAFT_ID = j.id; return DRAFT_ID; });
  }

  // ─── テンプレートを後から本文に入れる (1段目で選ばなかった場合の入口) ───
  (function() {
    var tplSel = document.getElementById('tplSel');
    var tplSearch = document.getElementById('tplSearch');
    var TPLS = null, CATS = [], loading = null;
    // 🔍絞り込み中はヒットしたものだけ並べる (2026-09-02 スタッフ要望)
    function fillTplSel() {
      if (!TPLS) return;
      var q = tplSearch.value;
      var shown = tplFilter(TPLS, q);
      var cur = tplSel.value;
      tplSel.textContent = '';
      var head = document.createElement('option');
      head.value = '';
      head.textContent = q.trim()
        ? (shown.length ? '🔍 ' + shown.length + '件ヒット — 選んでください' : '🔍 該当なし (キーワードを変えてください)')
        : '📄 テンプレートを本文に入れる… (' + TPLS.length + '件)';
      tplSel.appendChild(head);
      var order = CATS.slice();
      shown.forEach(function(t) { if (t.category && order.indexOf(t.category) < 0) order.push(t.category); });
      order.push('');
      order.forEach(function(cat) {
        var items = shown.filter(function(t) { return (t.category || '') === cat; });
        if (!items.length) return;
        var g = document.createElement('optgroup');
        g.label = cat || 'その他';
        items.forEach(function(t) {
          var o = document.createElement('option');
          o.value = String(t.id); o.textContent = t.name;
          g.appendChild(o);
        });
        tplSel.appendChild(g);
      });
      tplSel.value = cur;
      if (tplSel.selectedIndex < 0) tplSel.value = '';
    }
    function load() {
      if (loading) return loading;
      loading = fetch('/apps/inquiry-hub/api/templates')
        .then(function(r) { if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); })
        .then(function(j) { TPLS = j.templates || []; CATS = j.categories || []; fillTplSel(); })
        .catch(function(e) { loading = null; toast('テンプレート取得失敗: ' + e.message); });
      return loading;
    }
    document.getElementById('tplRow').addEventListener('pointerover', load, { once: true });
    tplSel.addEventListener('focus', load);
    tplSearch.addEventListener('focus', load);
    tplSearch.addEventListener('input', function() { if (TPLS) fillTplSel(); else load(); });
    tplSel.addEventListener('change', function() {
      var t = (TPLS || []).find(function(x) { return String(x.id) === tplSel.value; });
      if (!t) return;
      var ta = document.getElementById('cBody');
      var text = t.body + (t.bodyBottom ? '\\n\\n' + t.bodyBottom : '');
      if (ta.value.trim() && !confirm('本文をテンプレート「' + t.name + '」で置き換えますか? (署名も置き換わります)')) return;
      ta.value = text;
      var sub = document.getElementById('cSubject');
      if (t.subject && !sub.value.trim()) sub.value = t.subject;
      ta.focus();
      toast('テンプレート「' + t.name + '」を入れました。内容を確認・編集してください');
    });
  })();

  // ─── 📎 添付 (返信画面と同じ仕組み・同じ上限) ───
  (function() {
    var attBtn = document.getElementById('attBtn');
    var input = document.getElementById('attFile');
    var list = document.getElementById('attList');
    function fmtB(n) { return n < 1048576 ? Math.round(n / 1024) + 'KB' : (n / 1048576).toFixed(1) + 'MB'; }
    function render() {
      list.textContent = '';
      ATT.forEach(function(a) {
        var row = document.createElement('div');
        row.className = 'att-chip';
        var name = document.createElement('span');
        name.textContent = '📎 ' + a.name + ' (' + fmtB(a.size) + ')';
        var del = document.createElement('button');
        del.type = 'button'; del.className = 'ghost'; del.textContent = '✕'; del.title = 'この添付を取り消す';
        del.addEventListener('click', function() {
          del.disabled = true;
          jpost('/apps/inquiry-hub/api/inquiries/' + DRAFT_ID + '/reply-attachments/' + a.id + '/delete', {})
            .then(function() { ATT = ATT.filter(function(x) { return x.id !== a.id; }); render(); })
            .catch(function(e) { del.disabled = false; toast('削除失敗: ' + e.message); });
        });
        row.appendChild(name); row.appendChild(del);
        list.appendChild(row);
      });
    }
    attBtn.addEventListener('click', function() { input.click(); });
    input.addEventListener('change', function() {
      var files = Array.prototype.slice.call(input.files || []);
      input.value = '';
      ensureDraft().then(function(id) {
        (function next() {
          var f = files.shift();
          if (!f) return;
          if (ATT.length >= MAXF) { toast('添付は' + MAXF + 'つまでです'); return; }
          if (f.size > MAXB) { toast(f.name + ' は大きすぎます (上限' + Math.round(MAXB / 1048576) + 'MB)'); next(); return; }
          attBtn.disabled = true;
          fetch('/apps/inquiry-hub/api/inquiries/' + id + '/reply-attachments', {
            method: 'POST',
            headers: { 'Content-Type': 'application/octet-stream', 'X-File-Name': encodeURIComponent(f.name) },
            body: f,
          }).then(function(r) { return r.json().catch(function(){ return {}; }).then(function(j) { if (!r.ok) throw new Error(j.error || ('HTTP ' + r.status)); return j; }); })
            .then(function(j) { ATT.push({ id: j.id, name: j.fileName, size: j.fileSize }); render(); attBtn.disabled = false; next(); })
            .catch(function(e) { toast('添付失敗: ' + f.name + ' — ' + e.message); attBtn.disabled = false; next(); });
        })();
      }).catch(function(e) { toast('添付の準備に失敗: ' + e.message); });
    });
  })();

  // ─── 送信 ───
  var sendBtn = document.getElementById('sendBtn');
  var sendDoneBtn = document.getElementById('sendDoneBtn');
  function send(complete) {
    var to = document.getElementById('cTo').value.trim();
    var subject = document.getElementById('cSubject').value.trim();
    var body = document.getElementById('cBody').value;
    if (!to) { toast('宛先を入力してください'); return; }
    if (!subject) { toast('件名を入力してください'); return; }
    if (!body.trim()) { toast('本文が空です'); return; }
    var preview = body.length > 300 ? body.slice(0, 300) + '…' : body;
    if (!confirm('このメールを送信しますか?\\n\\n宛先: ' + to + '\\n件名: ' + subject
      + (ATT.length ? '\\n添付: ' + ATT.map(function(a) { return a.name; }).join(' / ') : '')
      + (complete ? '\\n送信後に「完了」にします' : '\\n送信後は「返信処理中」になります')
      + '\\n\\n' + preview)) return;
    sendBtn.disabled = true; sendDoneBtn.disabled = true;
    jpost('/apps/inquiry-hub/api/compose/send', {
      draftId: DRAFT_ID, shopId: SHOP_ID, to: to, subject: subject,
      customerName: document.getElementById('cName').value.trim(), body: body,
      attachmentIds: ATT.map(function(a) { return a.id; }),
      completeOnSend: complete, clientOperationId: OP_ID,
    }).then(function(j) {
      toast(j.duplicate ? '既に同じ操作で作成済みです' : '送信ジョブを作成しました');
      setTimeout(function() { location.href = '/apps/inquiry-hub/inquiries/' + j.inquiryId; }, 900);
    }).catch(function(e) {
      toast('送信失敗: ' + e.message);
      sendBtn.disabled = false; sendDoneBtn.disabled = false;
    });
  }
  sendBtn.addEventListener('click', function() { send(false); });
  sendDoneBtn.addEventListener('click', function() { send(true); });`;

  res.send(pageShell('問い合わせ管理 — メール作成', 'compose', body, script));
});

// ─── 新規メールのAPI (CSRF・JSON必須ガードは上の router.use('/api/') が効く) ───

/** 添付を保存するための下書き (器) を1件作る。送られなければ24時間後に掃除される */
router.post('/api/compose/draft', (req, res) => {
  if (!replyEditorEnabled()) return res.status(403).json({ error: 'メール送信機能が有効になっていません' });
  try {
    const d = createComposeDraft({ shopId: (req.body || {}).shopId ?? null, createdBy: actorOf(req) });
    res.json({ ok: true, id: d.id });
  } catch (e) {
    res.status(400).json({ error: String(e?.message || e).slice(0, 300) });
  }
});

/**
 * 新規メールの送信ジョブ作成 (実送信は送信ワーカー = 返信とまったく同じ経路)。
 * 下書きを表に出す → 送信ジョブを作る の順で行い、ジョブが作れなければ下書きに戻す
 * (宛先も件名も入っているのに送信ジョブが無いスレッドを一覧に残さない)。
 */
router.post('/api/compose/send', async (req, res) => {
  if (!replyEditorEnabled()) return res.status(403).json({ error: 'メール送信機能が有効になっていません' });
  const b = req.body || {};
  const bodyText = String(b.body || '');
  if (!b.clientOperationId || typeof b.clientOperationId !== 'string') {
    return res.status(400).json({ error: '不正なリクエストです (clientOperationId)' });
  }
  const attachmentIds = b.attachmentIds;
  if (attachmentIds !== undefined && (!Array.isArray(attachmentIds) || !attachmentIds.every(n => Number.isInteger(n) && n > 0))) {
    return res.status(400).json({ error: '不正なリクエストです (attachmentIds は数値の配列)' });
  }
  try {
    validateBody(bodyText);
  } catch (e) {
    return res.status(400).json({ error: String(e?.message || e).slice(0, 300) });
  }
  const db = getDB();
  const opId = String(b.clientOperationId);
  // 宛先ドメインの事前確認 (2026-08-27)。形式の検証は finalizeComposeDraft が行うので、
  // ここでは「ドメインが実在してメールを受け取れるか」だけを見る (DNS不通なら止めない)。
  // 同じ操作IDの再POSTは既存ジョブを返すのが先 = DNSの結果で失敗表示にしない (Codexレビュー High)
  if (!db.prepare('SELECT id FROM outbox_replies WHERE client_operation_id = ?').get(opId)) {
    const problem = await recipientDomainProblem(b.to);
    if (problem) return res.status(400).json({ error: problem });
  }
  /** 同じ操作IDの再POST (レスポンスが届かなかった・二度押し) は既存ジョブをそのまま返す */
  const duplicateOf = (row) => {
    const e = new Error('duplicate');
    e.duplicate = { inquiryId: row.inquiry_id, outboxId: row.id };
    return e;
  };
  try {
    // 下書きの確定と送信ジョブの作成、そして操作IDの重複判定までを1つのトランザクションで行う。
    //  - 途中で落ちたら丸ごと巻き戻す = 「宛先も件名も入っているのに送信ジョブが無い問い合わせ」を
    //    一覧に残さない (Codexレビュー 1巡目 High-1)
    //  - 重複判定をトランザクションの外でやると、同じ操作IDの同時POSTが両方すり抜けて
    //    片方が空の問い合わせを作ってしまう (Codexレビュー 2巡目 High)。BEGIN IMMEDIATE の
    //    書き込みロックで直列化し、中で判定する
    const out = db.transaction(() => {
      const already = db.prepare('SELECT id, inquiry_id FROM outbox_replies WHERE client_operation_id = ?').get(opId);
      if (already) throw duplicateOf(already);
      const r = finalizeComposeDraft({ draftId: b.draftId ?? null, shopId: b.shopId ?? null,
        to: b.to, subject: b.subject, customerName: b.customerName, actor: actorOf(req) });
      const job = createReplyJob({
        inquiryId: r.inquiry.id, channelType: 'email', bodyText,
        attachmentIds: attachmentIds || [], createdBy: actorOf(req),
        clientOperationId: opId, baseConversationRev: r.inquiry.conversation_rev,
        completeOnSend: b.completeOnSend === true,
      });
      if (job.conflict) {
        const e = new Error(job.conflict);
        e.httpStatus = 409;
        throw e;   // 巻き戻して下書きのままにする
      }
      if (!job.created) {
        // 上の判定を抜けてここに来ることは無いはずだが、来たら今作った器ごと巻き戻す
        // (「新しい問い合わせIDに古い送信ジョブ」という食い違った応答を返さない)
        const existing = db.prepare('SELECT id, inquiry_id FROM outbox_replies WHERE id = ?').get(job.id);
        throw duplicateOf(existing || { id: job.id, inquiry_id: r.inquiry.id });
      }
      return { inquiryId: r.inquiry.id, outboxId: job.id, to: r.inquiry.customer_identifier };
    }).immediate();
    console.log(`[inquiry-hub] 新規メール送信ジョブ #${out.outboxId} (問い合わせ #${out.inquiryId} → ${out.to}) by ${actorOf(req)}`);
    res.json({ ok: true, inquiryId: out.inquiryId, outboxId: out.outboxId, duplicate: false });
  } catch (e) {
    if (e?.duplicate) return res.json({ ok: true, ...e.duplicate, duplicate: true });
    res.status(e?.httpStatus || 400).json({ error: String(e?.message || e).slice(0, 300) });
  }
});

// ═══════════════════════════════════════════════════════════════
// ✍️ 署名の設定 (2026-08-27 中原さん要望。新規メール作成1段目で選ぶ「署名」の登録先)
//    本文の末尾に付ける定型の差出人表記。選ぶと作成画面の本文に展開される (以後は普通のテキスト)
// ═══════════════════════════════════════════════════════════════
router.get('/signatures', (req, res) => {
  const rows = listSignatures();
  const trs = rows.map(s => `
    <tr>
      <td data-full data-label="名前">
        <input type="text" class="s-name" data-id="${s.id}" value="${he(s.name)}" maxlength="${SIGNATURE_NAME_MAX}" style="width:100%">
        <label class="chk" style="margin-top:6px"><input type="radio" name="sigdef" class="s-def" data-id="${s.id}"${s.is_default ? ' checked' : ''}>この署名を既定にする</label>
      </td>
      <td data-full data-label="本文">
        <textarea class="s-body" data-id="${s.id}" rows="6" maxlength="${SIGNATURE_BODY_MAX}"
          style="width:100%;padding:8px 10px;border:1px solid #cbd5e1;border-radius:8px;font-family:inherit;font-size:13px">${he(s.body)}</textarea>
      </td>
      <td data-label="表示順"><input type="number" class="s-order" data-id="${s.id}" value="${s.sort_order}" style="width:80px"></td>
      <td class="ops">
        <button class="pri s-save" data-id="${s.id}">保存</button>
        <button class="s-del" data-id="${s.id}" data-name="${he(s.name)}">削除</button>
      </td>
    </tr>`).join('');

  const body = `
  <div class="view-hint">✍️ <b>署名</b> — <a href="/apps/inquiry-hub/compose">✉️メール作成</a>の1段目で選ぶと、本文の末尾に入った状態で作成画面が開きます。
    入ったあとは普通の文章として編集できます (送信前にその場で直せます)。
    <b>既定</b>にした署名は、メール作成画面を開いたときに最初から選ばれています。</div>
  <div class="card" style="padding:12px 14px;margin-bottom:14px">
    <div class="cform">
      <label class="k" for="newName">署名の名前</label>
      <div><input type="text" id="newName" placeholder="例: 雑貨イズム署名" maxlength="${SIGNATURE_NAME_MAX}"></div>
      <label class="k" for="newBody">本文</label>
      <div><textarea id="newBody" rows="6" maxlength="${SIGNATURE_BODY_MAX}"
        placeholder="例:&#10;━━━━━━━━━━━━━━━&#10;雑貨イズム（B-Faith株式会社）&#10;〒000-0000 ○○県○○市…&#10;TEL 00-0000-0000 / info@b-faith.biz&#10;━━━━━━━━━━━━━━━"></textarea></div>
    </div>
    <div class="cactions"><button class="pri" id="createBtn" type="button">➕ 署名を作成</button></div>
  </div>
  <div class="card">
    <table class="cardable">
      <thead><tr><th style="width:26%">名前 / 既定</th><th>本文</th><th>表示順</th><th>操作</th></tr></thead>
      <tbody>${trs || '<tr><td colspan="4" class="empty">署名はまだありません。上の欄から作成してください</td></tr>'}</tbody>
    </table>
  </div>`;

  const script = `
  function api(path, data) {
    return fetch('/apps/inquiry-hub/api/signatures' + path, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data || {})
    }).then(function(r) { return r.json().catch(function(){ return {}; }).then(function(j){ if (!r.ok) throw new Error(j.error || ('HTTP ' + r.status)); return j; }); });
  }
  document.getElementById('createBtn').addEventListener('click', function() {
    var btn = this;
    var name = document.getElementById('newName').value.trim();
    var body = document.getElementById('newBody').value;
    if (!name) { toast('署名の名前を入力してください'); return; }
    if (!body.trim()) { toast('署名の本文を入力してください'); return; }
    btn.disabled = true;
    api('', { name: name, body: body }).then(function() { location.reload(); })
      .catch(function(e) { toast('作成失敗: ' + e.message); btn.disabled = false; });
  });
  document.querySelectorAll('.s-save').forEach(function(b) {
    b.addEventListener('click', function() {
      var id = b.dataset.id;
      b.disabled = true;
      api('/' + id, {
        name: document.querySelector('.s-name[data-id="' + id + '"]').value.trim(),
        body: document.querySelector('.s-body[data-id="' + id + '"]').value,
        sortOrder: Number(document.querySelector('.s-order[data-id="' + id + '"]').value),
        isDefault: document.querySelector('.s-def[data-id="' + id + '"]').checked,
      }).then(function() { location.reload(); })
        .catch(function(e) { toast('保存失敗: ' + e.message); b.disabled = false; });
    });
  });
  document.querySelectorAll('.s-del').forEach(function(b) {
    b.addEventListener('click', function() {
      if (!confirm('署名「' + b.dataset.name + '」を削除しますか?\\n\\n選択肢から消えるだけで、これまでに送ったメールの本文は変わりません。')) return;
      b.disabled = true;
      api('/' + b.dataset.id + '/delete', {}).then(function() {
        toast('削除しました');
        setTimeout(function(){ location.reload(); }, 700);
      }).catch(function(e) { toast('削除失敗: ' + e.message); b.disabled = false; });
    });
  });`;
  res.send(pageShell('問い合わせ管理 — 署名', 'signatures', body, script));
});

router.post('/api/signatures', (req, res) => {
  try {
    const b = req.body || {};
    const s = createSignature({ name: b.name, body: b.body, isDefault: b.isDefault === true, createdBy: actorOf(req) });
    console.log(`[inquiry-hub] 署名作成「${s.name}」 by ${actorOf(req)}`);
    res.json({ ok: true, ...s });
  } catch (e) { res.status(400).json({ error: String(e?.message || e).slice(0, 200) }); }
});

router.post('/api/signatures/:id(\\d+)', (req, res) => {
  try {
    const b = req.body || {};
    const r = updateSignature(Number(req.params.id), { name: b.name, body: b.body, sortOrder: b.sortOrder, isDefault: b.isDefault });
    res.json({ ok: true, ...r });
  } catch (e) { res.status(400).json({ error: String(e?.message || e).slice(0, 200) }); }
});

router.post('/api/signatures/:id(\\d+)/delete', (req, res) => {
  try {
    const r = deleteSignature(Number(req.params.id));
    console.log(`[inquiry-hub] 署名削除「${r.name}」 by ${actorOf(req)}`);
    res.json({ ok: true });
  } catch (e) { res.status(400).json({ error: String(e?.message || e).slice(0, 200) }); }
});

router.get('/templates', (req, res) => {
  const q = req.query || {};
  const { rows, categories } = listTemplates(q);
  const kw = String(q.q || '').trim();
  const filtering = !!(kw || q.category); // 検索/絞込中は結果がすぐ見えるよう全展開

  const filterBar = `
  <div class="filters">
    <form method="get" style="display:flex;gap:8px;flex-wrap:wrap;align-items:center">
      <select name="category"><option value="">グループ: 全て</option>${categoryOptions(categories, q.category)}</select>
      <input type="search" name="q" value="${he(kw)}" placeholder="テンプレート名/件名/本文/キーワード" style="min-width:240px">
      <button class="pri">🔍 検索</button>
      <a href="/apps/inquiry-hub/templates" class="ghost btn-link">クリア</a>
    </form>
    ${EXPAND_BUTTONS}
    <span style="flex:1"></span>
    <button class="ghost" id="newBtn">➕ 新規テンプレート</button>
    <label class="ghost btn-link" style="cursor:pointer">📥 メールディーラーCSV取込<input type="file" id="csvFile" accept=".csv" style="display:none"></label>
  </div>
  <div id="editPanel" class="panel" style="display:none">
    <h3 id="editTitle">テンプレート編集</h3>
    <div class="edit-grid">
      <label>テンプレート名 *<input type="text" id="fName"></label>
      <label>グループ<input type="text" id="fCategory" list="catList" placeholder="例: 領収書発行について"><datalist id="catList">${categories.map(c => `<option value="${he(c.category)}">`).join('')}</datalist></label>
      <label>件名<input type="text" id="fSubject"></label>
      <label>キーワード<input type="text" id="fKeywords"></label>
    </div>
    <label>本文 *<textarea id="fBody" rows="10"></textarea></label>
    <label>本文（下）= 署名等<textarea id="fBodyBottom" rows="3"></textarea></label>
    <label>備考<input type="text" id="fNotes"></label>
    <div style="display:flex;gap:8px;justify-content:flex-end">
      <button class="ghost" id="editCancel">キャンセル</button>
      <button class="pri" id="editSave">保存</button>
    </div>
  </div>`;

  const renderTplItem = r => `
    <details class="tpl" id="tpl-${r.id}">
      <summary><span class="tpl-icon">📄</span><b>${he(r.template_name)}</b>${r.subject ? ` <span class="sub">件名: ${he(r.subject)}</span>` : ''}
        <span class="sub" style="margin-left:auto">使用${r.usage_count}回 ・ 更新 ${fmtJst(r.source_updated_at || r.updated_at)}</span></summary>
      <div class="tpl-body">
        ${r.keywords ? `<div class="sub">🔑 ${he(r.keywords)}</div>` : ''}
        <pre>${he(r.template_body)}</pre>
        ${r.body_bottom ? `<pre class="sub">${he(r.body_bottom)}</pre>` : ''}
        ${r.notes ? `<div class="sub">備考: ${he(r.notes)}</div>` : ''}
        <div class="tpl-ops">
          <button class="pri" data-copy="${r.id}">📋 本文をコピー</button>
          <button class="ghost" data-edit="${r.id}">✏️ 編集</button>
          <button class="ghost" data-del="${r.id}">🗑 削除</button>
        </div>
      </div>
    </details>`;
  const items = renderCategoryTree(buildCategoryTree(rows), renderTplItem, filtering);

  const body = `${filterBar}
  <div class="card" style="padding:12px">
    <div class="sub" style="margin-bottom:8px">全${rows.length}件${filtering ? ' (絞り込み中 — 全グループ展開表示)' : ''} — 📁グループをクリックで開閉、📄テンプレートをクリックで本文表示。返信送信機能 (Step 3) 実装までは「📋コピー」でメールディーラー等に貼り付けて使用</div>
    ${items || '<div class="empty">テンプレートがありません。メールディーラーのエクスポートCSVを「📥CSV取込」から取り込んでください</div>'}
  </div>`;

  const script = `
  var TPL = ${JSON.stringify(Object.fromEntries(rows.map(r => [r.id, {
    template_name: r.template_name, category: r.category, subject: r.subject, keywords: r.keywords,
    template_body: r.template_body, body_bottom: r.body_bottom, notes: r.notes,
  }]))).replace(/</g, '\\u003c')};
  function post(path, data) {
    return fetch('/apps/inquiry-hub/api' + path, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data)
    }).then(function(r) { return r.json().catch(function(){ return {}; }).then(function(j){ if (!r.ok) throw new Error(j.error || ('HTTP ' + r.status)); return j; }); });
  }
  var editId = null;
  function openEdit(id) {
    editId = id;
    var d = id ? TPL[id] : {};
    document.getElementById('editTitle').textContent = id ? 'テンプレート編集' : '新規テンプレート';
    document.getElementById('fName').value = d.template_name || '';
    document.getElementById('fCategory').value = d.category || '';
    document.getElementById('fSubject').value = d.subject || '';
    document.getElementById('fKeywords').value = d.keywords || '';
    document.getElementById('fBody').value = d.template_body || '';
    document.getElementById('fBodyBottom').value = d.body_bottom || '';
    document.getElementById('fNotes').value = d.notes || '';
    document.getElementById('editPanel').style.display = 'block';
    window.scrollTo({ top: 0, behavior: 'smooth' });
  }
  document.getElementById('newBtn').addEventListener('click', function() { openEdit(null); });
  document.getElementById('editCancel').addEventListener('click', function() { document.getElementById('editPanel').style.display = 'none'; });
  document.getElementById('editSave').addEventListener('click', function() {
    var btn = this; btn.disabled = true;
    var data = {
      template_name: document.getElementById('fName').value,
      category: document.getElementById('fCategory').value,
      subject: document.getElementById('fSubject').value,
      keywords: document.getElementById('fKeywords').value,
      template_body: document.getElementById('fBody').value,
      body_bottom: document.getElementById('fBodyBottom').value,
      notes: document.getElementById('fNotes').value,
    };
    post(editId ? '/templates/' + editId : '/templates', data)
      .then(function() { location.reload(); })
      .catch(function(e) { btn.disabled = false; toast('保存失敗: ' + e.message); });
  });
  document.addEventListener('click', function(ev) {
    var t = ev.target;
    if (t.dataset && t.dataset.copy) {
      var d = TPL[t.dataset.copy];
      var text = d.template_body + (d.body_bottom ? '\\n\\n' + d.body_bottom : '');
      navigator.clipboard.writeText(text).then(function() {
        toast('本文をコピーしました');
        post('/templates/' + t.dataset.copy + '/copied', {}).catch(function(){});
      }, function() { toast('コピーに失敗しました'); });
    } else if (t.dataset && t.dataset.edit) {
      openEdit(t.dataset.edit);
    } else if (t.dataset && t.dataset.del) {
      var dd = TPL[t.dataset.del];
      if (!confirm('テンプレート「' + dd.template_name + '」を削除しますか?(論理削除。再取込で復活します)')) return;
      post('/templates/' + t.dataset.del + '/delete', {})
        .then(function() { location.reload(); })
        .catch(function(e) { toast('削除失敗: ' + e.message); });
    }
  });
  document.getElementById('csvFile').addEventListener('change', function() {
    var f = this.files[0]; this.value = '';
    if (!f) return;
    if (f.size > 1.5 * 1024 * 1024) { toast('CSVが大きすぎます (1.5MBまで。JSON化で膨らむためサーバー上限2MBの手前で制限)'); return; }
    f.text().then(function(text) {
      if (!confirm('メールディーラーのテンプレートCSVを取り込みますか?\\n(同じテンプレートIDはCSVの内容で上書き / 手動追加分には触りません)')) return;
      post('/templates/import', { csv: text }).then(function(r) {
        alert('取込完了: 新規' + r.inserted + '件 / 更新' + r.updated + '件 / スキップ' + r.skipped + '件' + (r.errors.length ? '\\n\\n' + r.errors.slice(0, 10).join('\\n') : ''));
        location.reload();
      }).catch(function(e) { toast('取込失敗: ' + e.message); });
    });
  });
  ${EXPAND_SCRIPT}`;
  res.send(pageShell('問い合わせ管理 — テンプレート', 'templates', body, script));
});

// ─── Q&A管理 (社内ナレッジ) ───
router.get('/qa', (req, res) => {
  const q = req.query || {};
  const { rows, categories } = listQa(q);
  const kw = String(q.q || '').trim();
  const filtering = !!(kw || q.category);

  const filterBar = `
  <div class="filters">
    <form method="get" style="display:flex;gap:8px;flex-wrap:wrap;align-items:center">
      <select name="category"><option value="">カテゴリ: 全て</option>${categoryOptions(categories, q.category)}</select>
      <input type="search" name="q" value="${he(kw)}" placeholder="件名/質問/回答" style="min-width:240px">
      <button class="pri">🔍 検索</button>
      <a href="/apps/inquiry-hub/qa" class="ghost btn-link">クリア</a>
    </form>
    ${EXPAND_BUTTONS}
    <span style="flex:1"></span>
    <button class="ghost" id="newBtn">➕ 新規Q&A</button>
    <label class="ghost btn-link" style="cursor:pointer">📥 メールディーラーCSV取込<input type="file" id="csvFile" accept=".csv" style="display:none"></label>
  </div>
  <div id="editPanel" class="panel" style="display:none">
    <h3 id="editTitle">Q&A編集</h3>
    <div class="edit-grid">
      <label>件名 *<input type="text" id="fTitle"></label>
      <label>カテゴリ<input type="text" id="fCategory" list="catList"><datalist id="catList">${categories.map(c => `<option value="${he(c.category)}">`).join('')}</datalist></label>
    </div>
    <label>質問内容<textarea id="fQuestion" rows="3"></textarea></label>
    <label>回答 *<textarea id="fAnswer" rows="6"></textarea></label>
    <label>備考<input type="text" id="fNotes"></label>
    <div style="display:flex;gap:8px;justify-content:flex-end">
      <button class="ghost" id="editCancel">キャンセル</button>
      <button class="pri" id="editSave">保存</button>
    </div>
  </div>`;

  const renderQaItem = r => `
    <details class="tpl" id="qa-${r.id}">
      <summary><span class="tpl-icon">❓</span><b>${he(r.title)}</b>
        <span class="sub" style="margin-left:auto">${he(r.staff || '')} ・ 更新 ${fmtJst(r.source_updated_at || r.updated_at)}</span></summary>
      <div class="tpl-body">
        ${r.question ? `<div class="qa-q">Q. ${he(r.question).replace(/\n/g, '<br>')}</div>` : ''}
        <div class="qa-a">A. ${he(r.answer).replace(/\n/g, '<br>')}</div>
        ${r.notes ? `<div class="sub">備考: ${he(r.notes)}</div>` : ''}
        <div class="tpl-ops">
          <button class="pri" data-copy="${r.id}">📋 回答をコピー</button>
          <button class="ghost" data-edit="${r.id}">✏️ 編集</button>
          <button class="ghost" data-del="${r.id}">🗑 削除</button>
        </div>
      </div>
    </details>`;
  const items = renderCategoryTree(buildCategoryTree(rows), renderQaItem, filtering);

  const body = `${filterBar}
  <div class="card" style="padding:12px">
    <div class="sub" style="margin-bottom:8px">全${rows.length}件${filtering ? ' (絞り込み中 — 全カテゴリ展開表示)' : ''} — 📁カテゴリをクリックで開閉、❓質問をクリックで回答表示。商品知識・対応ノウハウの社内ナレッジ</div>
    ${items || '<div class="empty">Q&amp;Aがありません。メールディーラーのエクスポートCSVを「📥CSV取込」から取り込んでください</div>'}
  </div>`;

  const script = `
  var QA = ${JSON.stringify(Object.fromEntries(rows.map(r => [r.id, {
    title: r.title, category: r.category, question: r.question, answer: r.answer, notes: r.notes,
  }]))).replace(/</g, '\\u003c')};
  function post(path, data) {
    return fetch('/apps/inquiry-hub/api' + path, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data)
    }).then(function(r) { return r.json().catch(function(){ return {}; }).then(function(j){ if (!r.ok) throw new Error(j.error || ('HTTP ' + r.status)); return j; }); });
  }
  var editId = null;
  function openEdit(id) {
    editId = id;
    var d = id ? QA[id] : {};
    document.getElementById('editTitle').textContent = id ? 'Q&A編集' : '新規Q&A';
    document.getElementById('fTitle').value = d.title || '';
    document.getElementById('fCategory').value = d.category || '';
    document.getElementById('fQuestion').value = d.question || '';
    document.getElementById('fAnswer').value = d.answer || '';
    document.getElementById('fNotes').value = d.notes || '';
    document.getElementById('editPanel').style.display = 'block';
    window.scrollTo({ top: 0, behavior: 'smooth' });
  }
  document.getElementById('newBtn').addEventListener('click', function() { openEdit(null); });
  document.getElementById('editCancel').addEventListener('click', function() { document.getElementById('editPanel').style.display = 'none'; });
  document.getElementById('editSave').addEventListener('click', function() {
    var btn = this; btn.disabled = true;
    var data = {
      title: document.getElementById('fTitle').value,
      category: document.getElementById('fCategory').value,
      question: document.getElementById('fQuestion').value,
      answer: document.getElementById('fAnswer').value,
      notes: document.getElementById('fNotes').value,
    };
    post(editId ? '/qa/' + editId : '/qa', data)
      .then(function() { location.reload(); })
      .catch(function(e) { btn.disabled = false; toast('保存失敗: ' + e.message); });
  });
  document.addEventListener('click', function(ev) {
    var t = ev.target;
    if (t.dataset && t.dataset.copy) {
      navigator.clipboard.writeText(QA[t.dataset.copy].answer).then(function() { toast('回答をコピーしました'); }, function() { toast('コピーに失敗しました'); });
    } else if (t.dataset && t.dataset.edit) {
      openEdit(t.dataset.edit);
    } else if (t.dataset && t.dataset.del) {
      if (!confirm('Q&A「' + QA[t.dataset.del].title + '」を削除しますか?(論理削除。再取込で復活します)')) return;
      post('/qa/' + t.dataset.del + '/delete', {})
        .then(function() { location.reload(); })
        .catch(function(e) { toast('削除失敗: ' + e.message); });
    }
  });
  document.getElementById('csvFile').addEventListener('change', function() {
    var f = this.files[0]; this.value = '';
    if (!f) return;
    if (f.size > 1.5 * 1024 * 1024) { toast('CSVが大きすぎます (1.5MBまで。JSON化で膨らむためサーバー上限2MBの手前で制限)'); return; }
    f.text().then(function(text) {
      if (!confirm('メールディーラーのQ&A CSVを取り込みますか?\\n(同じQ&A IDはCSVの内容で上書き / 手動追加分には触りません)')) return;
      post('/qa/import', { csv: text }).then(function(r) {
        alert('取込完了: 新規' + r.inserted + '件 / 更新' + r.updated + '件 / スキップ' + r.skipped + '件' + (r.errors.length ? '\\n\\n' + r.errors.slice(0, 10).join('\\n') : ''));
        location.reload();
      }).catch(function(e) { toast('取込失敗: ' + e.message); });
    });
  });
  ${EXPAND_SCRIPT}`;
  res.send(pageShell('問い合わせ管理 — Q&A', 'qa', body, script));
});

// ─── テンプレート/Q&A API ───
router.post('/api/templates/import', (req, res) => {
  const csv = String((req.body || {}).csv || '');
  if (!csv.trim()) return res.status(400).json({ error: 'CSVが空です' });
  try { res.json(importTemplatesCsv(csv)); }
  catch (e) { res.status(400).json({ error: e.message }); }
});

router.post('/api/qa/import', (req, res) => {
  const csv = String((req.body || {}).csv || '');
  if (!csv.trim()) return res.status(400).json({ error: 'CSVが空です' });
  try { res.json(importQaCsv(csv)); }
  catch (e) { res.status(400).json({ error: e.message }); }
});

/** テンプレートの入力検証。エラー時はnullを返しresへ400を書く */
function tplFields(body, res) {
  const b = body || {};
  const name = String(b.template_name || '').trim().slice(0, 200);
  const tbody = String(b.template_body || '');
  if (!name) { res.status(400).json({ error: 'テンプレート名は必須です' }); return null; }
  if (!tbody.trim()) { res.status(400).json({ error: '本文は必須です' }); return null; }
  if (tbody.length > 50000) { res.status(400).json({ error: '本文が長すぎます (50000文字まで)' }); return null; }
  return {
    template_name: name,
    category: String(b.category || '').trim().slice(0, 200) || null,
    subject: String(b.subject || '').trim().slice(0, 500) || null,
    keywords: String(b.keywords || '').trim().slice(0, 500) || null,
    template_body: tbody,
    body_bottom: String(b.body_bottom || '').slice(0, 50000).trim() ? String(b.body_bottom || '').slice(0, 50000) : null,
    notes: String(b.notes || '').trim().slice(0, 1000) || null,
  };
}

router.post('/api/templates', (req, res) => {
  const f = tplFields(req.body, res); if (!f) return;
  getDB().prepare(`INSERT INTO reply_templates (template_name, category, subject, keywords, template_body, body_bottom, notes)
    VALUES (@template_name, @category, @subject, @keywords, @template_body, @body_bottom, @notes)`).run(f);
  res.json({ ok: true });
});

router.post('/api/templates/:id(\\d+)', (req, res) => {
  const f = tplFields(req.body, res); if (!f) return;
  const r = getDB().prepare(`UPDATE reply_templates SET template_name = @template_name, category = @category,
    subject = @subject, keywords = @keywords, template_body = @template_body, body_bottom = @body_bottom,
    notes = @notes, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now')
    WHERE id = @id AND is_active = 1`).run({ ...f, id: Number(req.params.id) });
  if (!r.changes) return res.status(404).json({ error: 'テンプレートが見つかりません' });
  res.json({ ok: true });
});

router.post('/api/templates/:id(\\d+)/delete', (req, res) => {
  const r = getDB().prepare(`UPDATE reply_templates SET is_active = 0, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now')
    WHERE id = ? AND is_active = 1`).run(Number(req.params.id));
  if (!r.changes) return res.status(404).json({ error: 'テンプレートが見つかりません' });
  res.json({ ok: true });
});

router.post('/api/templates/:id(\\d+)/copied', (req, res) => {
  getDB().prepare('UPDATE reply_templates SET usage_count = usage_count + 1 WHERE id = ?').run(Number(req.params.id));
  res.json({ ok: true });
});

/** Q&Aの入力検証 */
function qaFields(body, res) {
  const b = body || {};
  const title = String(b.title || '').trim().slice(0, 300);
  const answer = String(b.answer || '');
  if (!title) { res.status(400).json({ error: '件名は必須です' }); return null; }
  if (!answer.trim()) { res.status(400).json({ error: '回答は必須です' }); return null; }
  if (answer.length > 50000) { res.status(400).json({ error: '回答が長すぎます (50000文字まで)' }); return null; }
  return {
    title,
    category: String(b.category || '').trim().slice(0, 200) || null,
    question: String(b.question || '').slice(0, 50000).trim() ? String(b.question || '').slice(0, 50000) : null,
    answer,
    notes: String(b.notes || '').trim().slice(0, 1000) || null,
  };
}

router.post('/api/qa', (req, res) => {
  const f = qaFields(req.body, res); if (!f) return;
  getDB().prepare(`INSERT INTO qa_entries (title, category, question, answer, notes)
    VALUES (@title, @category, @question, @answer, @notes)`).run(f);
  res.json({ ok: true });
});

router.post('/api/qa/:id(\\d+)', (req, res) => {
  const f = qaFields(req.body, res); if (!f) return;
  const r = getDB().prepare(`UPDATE qa_entries SET title = @title, category = @category, question = @question,
    answer = @answer, notes = @notes, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now')
    WHERE id = @id AND is_active = 1`).run({ ...f, id: Number(req.params.id) });
  if (!r.changes) return res.status(404).json({ error: 'Q&Aが見つかりません' });
  res.json({ ok: true });
});

router.post('/api/qa/:id(\\d+)/delete', (req, res) => {
  const r = getDB().prepare(`UPDATE qa_entries SET is_active = 0, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now')
    WHERE id = ? AND is_active = 1`).run(Number(req.params.id));
  if (!r.changes) return res.status(404).json({ error: 'Q&Aが見つかりません' });
  res.json({ ok: true });
});

// ═══════════════════════════════════════════════════════════════
// 📧 メールルール画面 (メールチャネルのノイズ除去。メールディーラー振り分け設定の移行先)
// ═══════════════════════════════════════════════════════════════

const RULE_FIELD_LABELS = { from: 'From', to: 'To', reply_to: 'Reply-To', subject: '件名', body: '本文' };
const RULE_OP_LABELS = {
  contains: 'を含む', not_contains: 'を含まない', equals: 'と一致', not_equals: 'と一致しない',
  starts_with: 'で始まる', ends_with: 'で終わる',
};
const RULE_ACTION_LABELS = {
  skip: { label: '🗑️取り込まない', style: 'background:#fee2e2;color:#b91c1c' },
  import_done: { label: '✅取込+完了扱い', style: 'background:#dcfce7;color:#166534' },
  import: { label: '📁フォルダに入れる', style: 'background:#fef9c3;color:#854d0e' },
};

router.get('/mail-rules', (req, res) => {
  const rules = listMailRules();
  const folders = listFolders();
  const folderNameById = Object.fromEntries(folders.map(f => [f.id, f.name]));
  const labelsAll = listLabels();
  const labelById = Object.fromEntries(labelsAll.map(l => [l.id, l]));
  const fmtConds = (r) => {
    let conds;
    try { conds = JSON.parse(r.conditions_json); } catch { return '(解析不能)'; }
    const glue = r.match_mode === 'any' ? ' または ' : ' かつ ';
    return conds.map(c => `${RULE_FIELD_LABELS[c.field] || c.field}が「${he(c.value)}」${RULE_OP_LABELS[c.op] || c.op}`).join(glue);
  };
  const trs = rules.map(r => {
    const meta = RULE_ACTION_LABELS[r.action] || { label: r.action, style: '' };
    const folderTag = r.folder_id ? ` <span class="folder-chip">📁${he(folderNameById[r.folder_id] || `#${r.folder_id} (削除済み)`)}</span>` : '';
    const labelTag = r.label_id ? ' ' + (labelById[r.label_id]
      ? labelChip(labelById[r.label_id].name, labelById[r.label_id].color)
      : `<span class="folder-chip">🏷️#${r.label_id} (削除済み)</span>`) : '';
    return `
    <tr data-search="${he((r.name || '') + ' ' + fmtConds(r)).toLowerCase()}"${r.is_active ? '' : ' style="opacity:.5"'}>
      <td class="nowrap" data-label="優先度">${r.priority}</td>
      <td data-full>${he(r.name || '—')}${r.external_key ? '<div class="sub">メールディーラー移行</div>' : '<div class="sub">手動追加</div>'}</td>
      <td style="overflow-wrap:anywhere" data-full data-label="条件">${fmtConds(r)}</td>
      <td data-label="アクション"><span class="badge" style="${meta.style}">${he(meta.label)}</span>${folderTag}${labelTag}</td>
      <td class="nowrap ops">
        <button onclick="toggleRule(${r.id}, ${r.is_active ? 0 : 1}, this)">${r.is_active ? '無効化' : '有効化'}</button>
        <button onclick="deleteRule(${r.id}, this)">削除</button>
      </td>
    </tr>`;
  }).join('');

  const fieldOpts = Object.entries(RULE_FIELD_LABELS).map(([k, v]) => `<option value="${k}">${v}</option>`).join('');
  const opOpts = Object.entries(RULE_OP_LABELS).map(([k, v]) => `<option value="${k}">${v}</option>`).join('');
  const body = `
  <div class="card" style="margin-bottom:16px">
    <div class="card-title">📥 メールディーラー「振り分けの設定」CSVの取込
      <span class="sub">(ゴミ箱/削除ルール→「取り込まない」、対応完了ルール→「取込+完了扱い」として移行。フォルダ振り分けのみのルールは対象外)</span></div>
    <div style="padding:12px 14px">
      <input type="file" id="csvFile" accept=".csv">
      <span class="sub">Shift-JIS/UTF-8 自動判定。まず内容を確認してから取込を実行します。再取込は同じ条件IDを上書き (手動追加分には触りません)</span>
    </div>
  </div>
  <div class="card" style="margin-bottom:16px">
    <div class="card-title">🧪 ルールのテスト <span class="sub">(実際のメールを想定してどのルールに当たるか確認)</span></div>
    <div style="padding:12px 14px" class="edit-grid">
      <label>From <input type="text" id="tFrom" placeholder="例: no-reply@rakuten.co.jp"></label>
      <label>件名 <input type="text" id="tSubject" placeholder="例: ご注文ありがとうございます"></label>
      <label>Reply-To <input type="text" id="tReplyTo"></label>
      <label>本文 (任意) <input type="text" id="tBody"></label>
    </div>
    <div style="padding:0 14px 12px"><button class="pri" onclick="testRule(this)">判定する</button> <span id="testResult" class="sub"></span></div>
  </div>
  <div class="card" style="margin-bottom:16px">
    <div class="card-title">➕ ルールを手動追加</div>
    <div style="padding:12px 14px">
      <div class="edit-grid">
        <label>名称 (任意) <input type="text" id="nName"></label>
        <label>優先度 (小さいほど先に評価) <input type="number" id="nPriority" value="50"></label>
      </div>
      ${[1, 2, 3].map(n => `
      <div class="row rule-row" style="margin-bottom:6px">
        <select id="nField${n}">${n > 1 ? '<option value="">(条件なし)</option>' : ''}${fieldOpts}</select>
        <input type="text" id="nValue${n}" placeholder="文字列">
        <select id="nOp${n}">${opOpts}</select>
      </div>`).join('')}
      <div class="row rule-row" style="align-items:center">
        <select id="nMode"><option value="all">すべての条件を満たす (かつ)</option><option value="any">いずれかの条件を満たす (または)</option></select>
        <select id="nAction"><option value="skip">🗑️取り込まない</option><option value="import_done">✅取込+完了扱い</option><option value="import">📁🏷️振り分けだけする (新着のまま)</option></select>
        <select id="nFolder"><option value="">📁 フォルダ指定なし</option>${folders.map(f => `<option value="${f.id}">📁 ${he(f.name)}</option>`).join('')}</select>
        <select id="nLabel"><option value="">🏷️ ラベルなし</option>${labelsAll.map(l => `<option value="${l.id}">🏷️ ${he(l.name)}</option>`).join('')}</select>
        <button class="pri" onclick="addRule(this)">追加</button>
      </div>
      <div class="sub">フォルダ・ラベルは「取り込まない」以外で指定できます (「振り分けだけする」ではどちらか必須。取込+完了扱いとの組み合わせも可)。ラベルは <a href="/apps/inquiry-hub/labels">🏷️ラベル管理</a> で作成できます</div>
    </div>
  </div>
  <div class="card">
    <div class="card-title">📜 ルール一覧 (${rules.length}件・優先度順に先勝ち)
      <input type="search" id="ruleFilter" placeholder="絞り込み" style="margin-left:12px;padding:4px 8px;border:1px solid #cbd5e1;border-radius:8px;font-size:13px;font-weight:normal"></div>
    <table class="cardable">
      <thead><tr><th>優先度</th><th>名称</th><th>条件</th><th>アクション</th><th>操作</th></tr></thead>
      <tbody id="ruleRows">${trs || '<tr><td colspan="5" class="empty">ルールがありません (CSVを取り込むか手動追加してください)</td></tr>'}</tbody>
    </table>
  </div>`;

  const script = `
async function post(url, data) {
  const r = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data || {}) });
  const j = await r.json().catch(function(){ return {}; });
  if (!r.ok) throw new Error(j.error || ('HTTP ' + r.status));
  return j;
}
document.getElementById('csvFile').addEventListener('change', function() {
  var f = this.files[0]; this.value = '';
  if (!f) return;
  if (f.size > 1.5 * 1024 * 1024) { toast('CSVが大きすぎます (1.5MBまで)'); return; }
  f.arrayBuffer().then(function(buf) {
    var text = new TextDecoder('utf-8').decode(buf);
    if (text.indexOf('条件ID') < 0) text = new TextDecoder('shift-jis').decode(buf);
    if (text.indexOf('条件ID') < 0) { toast('振り分け設定のエクスポートCSVではないようです'); return; }
    post('/apps/inquiry-hub/api/mail-rules/import', { csv: text, apply: false }).then(function(r) {
      var msg = '取込プレビュー:\\n・取り込まない(ノイズ除去): ' + r.toSkip + '件\\n・取込+完了扱い: ' + r.toImportDone + '件\\n・対象外(フォルダ振り分け等): ' + r.notTarget + '件\\n・移行不能(複合条件等): ' + r.unsupported.length + '件';
      if (r.unsupported.length) msg += '\\n\\n移行不能の例:\\n' + r.unsupported.slice(0, 5).map(function(u){ return '  #' + u.condId + ' ' + (u.name || '') + ' — ' + u.reason; }).join('\\n');
      msg += '\\n\\n取込を実行しますか?';
      if (!confirm(msg)) return;
      post('/apps/inquiry-hub/api/mail-rules/import', { csv: text, apply: true }).then(function(r2) {
        alert('取込完了: 新規' + r2.applied + '件 / 更新' + r2.updated + '件');
        location.reload();
      }).catch(function(e) { toast('取込失敗: ' + e.message); });
    }).catch(function(e) { toast('解析失敗: ' + e.message); });
  });
});
async function toggleRule(id, active, btn) {
  btn.disabled = true;
  try { await post('/apps/inquiry-hub/api/mail-rules/' + id + '/toggle', { active: !!active }); location.reload(); }
  catch (e) { toast('失敗: ' + e.message); btn.disabled = false; }
}
async function deleteRule(id, btn) {
  if (!confirm('このルールを削除しますか? (物理削除。CSV再取込で復活します)')) return;
  btn.disabled = true;
  try { await post('/apps/inquiry-hub/api/mail-rules/' + id + '/delete', {}); location.reload(); }
  catch (e) { toast('失敗: ' + e.message); btn.disabled = false; }
}
async function testRule(btn) {
  btn.disabled = true;
  try {
    var r = await post('/apps/inquiry-hub/api/mail-rules/test', {
      from: document.getElementById('tFrom').value, subject: document.getElementById('tSubject').value,
      reply_to: document.getElementById('tReplyTo').value, body: document.getElementById('tBody').value,
    });
    var actLabel = { skip: '🗑️取り込まない', import_done: '✅取込+完了扱い', import: '📁フォルダに入れる' };
    document.getElementById('testResult').textContent = r.match
      ? '→ ルール#' + r.match.ruleId + (r.match.ruleName ? ' (' + r.match.ruleName + ')' : '') + ' に一致: ' + (actLabel[r.match.action] || r.match.action)
      : '→ どのルールにも一致しない = 通常どおり問い合わせとして取り込む';
  } catch (e) { toast('失敗: ' + e.message); }
  btn.disabled = false;
}
async function addRule(btn) {
  var conditions = [];
  for (var n = 1; n <= 3; n++) {
    var field = document.getElementById('nField' + n).value;
    var value = document.getElementById('nValue' + n).value.trim();
    if (!field || !value) continue;
    conditions.push({ field: field, op: document.getElementById('nOp' + n).value, value: value });
  }
  if (!conditions.length) { toast('条件を1つ以上入力してください'); return; }
  btn.disabled = true;
  try {
    await post('/apps/inquiry-hub/api/mail-rules', {
      name: document.getElementById('nName').value, priority: Number(document.getElementById('nPriority').value),
      matchMode: document.getElementById('nMode').value, action: document.getElementById('nAction').value,
      folderId: document.getElementById('nFolder').value || null,
      labelId: document.getElementById('nLabel').value || null,
      conditions: conditions,
    });
    toast('追加しました'); setTimeout(function(){ location.reload(); }, 700);
  } catch (e) { toast('追加失敗: ' + e.message); btn.disabled = false; }
}
document.getElementById('ruleFilter').addEventListener('input', function() {
  var q = this.value.trim().toLowerCase();
  document.querySelectorAll('#ruleRows tr').forEach(function(tr) {
    tr.style.display = !q || (tr.dataset.search || '').indexOf(q) >= 0 ? '' : 'none';
  });
});
// フォルダを選んだのに「取り込まない」のままは矛盾 (2026-08-20 実事故) → 自動で「フォルダに入れる」へ
document.getElementById('nFolder').addEventListener('change', function() {
  var actSel = document.getElementById('nAction');
  if (this.value && actSel.value === 'skip') {
    actSel.value = 'import';
    toast('アクションを「📁フォルダに入れる (新着のまま)」に切り替えました');
  }
});`;
  res.send(pageShell('問い合わせ管理 — メールルール', 'mailrules', body, script));
});

router.post('/api/mail-rules', (req, res) => {
  try {
    const b = req.body || {};
    res.json({ ok: true, ...addMailRule({ name: b.name, matchMode: b.matchMode, conditions: b.conditions, action: b.action, priority: Number(b.priority), folderId: b.folderId, labelId: b.labelId }) });
  } catch (e) { res.status(400).json({ error: String(e?.message || e).slice(0, 300) }); }
});

router.post('/api/mail-rules/import', (req, res) => {
  const csv = String((req.body || {}).csv || '');
  if (!csv.trim()) return res.status(400).json({ error: 'CSVが空です' });
  try { res.json(importMailDealerRulesCsv(csv, { apply: !!(req.body || {}).apply })); }
  catch (e) { res.status(400).json({ error: String(e?.message || e).slice(0, 300) }); }
});

router.post('/api/mail-rules/test', (req, res) => {
  const b = req.body || {};
  const match = evaluateMailRules({
    from: String(b.from || ''), to: String(b.to || ''), reply_to: String(b.reply_to || ''),
    subject: String(b.subject || ''), body: String(b.body || ''),
  });
  res.json({ ok: true, match });
});

router.post('/api/mail-rules/:id(\\d+)/toggle', (req, res) => {
  try { setMailRuleActive(Number(req.params.id), !!(req.body || {}).active); res.json({ ok: true }); }
  catch (e) { res.status(404).json({ error: String(e?.message || e).slice(0, 300) }); }
});

router.post('/api/mail-rules/:id(\\d+)/delete', (req, res) => {
  try { deleteMailRule(Number(req.params.id)); res.json({ ok: true }); }
  catch (e) { res.status(404).json({ error: String(e?.message || e).slice(0, 300) }); }
});

// ═══════════════════════════════════════════════════════════════
// 📁 フォルダ管理 (2026-08-02 スタッフ要望「任意でフォルダを作成できるように」)
//    作成・改名・並び替え・削除をここに集約する (削除は問い合わせの所属も外すため、
//    一覧の片手間ではなく専用画面で行う)
// ═══════════════════════════════════════════════════════════════

router.get('/folders', (req, res) => {
  const folders = listFolders({ withCounts: true });
  const unfiled = countUnfiled();
  const rows = folders.map(f => `
    <tr>
      <td data-full data-label="フォルダ名"><input type="text" class="f-name" data-id="${f.id}" value="${he(f.name)}" maxlength="${FOLDER_NAME_MAX}"></td>
      <td data-label="表示順"><input type="number" class="f-order" data-id="${f.id}" value="${f.sort_order}" style="width:80px"></td>
      <td data-label="件数"><a href="/apps/inquiry-hub?view=all&folder=${f.id}">${f.total}件</a>
        <div class="sub">${f.open_count ? `未対応 ${f.open_count}件` : '未対応なし'}</div></td>
      <td class="ops">
        <button class="pri f-save" data-id="${f.id}">保存</button>
        <button class="f-del" data-id="${f.id}" data-name="${he(f.name)}" data-total="${f.total}">削除</button>
      </td>
    </tr>`).join('');

  const body = `
  <div class="view-hint">📁 <b>フォルダ</b> — 問い合わせを自由に分類できます。
    フォルダに入れても対応状況 (新着・完了など) は変わらないので、<b>未返信のものは受信トレイにも残ります</b>。</div>
  <div class="filters">
    <input type="text" id="newName" placeholder="新しいフォルダ名 (例: 返品・交換)" maxlength="${FOLDER_NAME_MAX}" style="min-width:260px">
    <button class="pri" id="createBtn">➕ 作成</button>
    <span style="flex:1"></span>
    <a class="ghost btn-link" href="/apps/inquiry-hub?view=all&folder=none">🗃️ 未分類 ${unfiled}件を見る</a>
  </div>
  <div class="card">
    <table class="cardable">
      <thead><tr><th>フォルダ名</th><th>表示順</th><th>件数</th><th>操作</th></tr></thead>
      <tbody>${rows || '<tr><td colspan="4" class="empty">フォルダはまだありません。上の欄から作成してください</td></tr>'}</tbody>
    </table>
  </div>`;

  const script = `
  function api(path, data) {
    return fetch('/apps/inquiry-hub/api/folders' + path, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data || {})
    }).then(function(r) { return r.json().catch(function(){ return {}; }).then(function(j){ if (!r.ok) throw new Error(j.error || ('HTTP ' + r.status)); return j; }); });
  }
  document.getElementById('createBtn').addEventListener('click', function() {
    var el = document.getElementById('newName'), name = el.value.trim();
    if (!name) { toast('フォルダ名を入力してください'); return; }
    this.disabled = true;
    var btn = this;
    api('', { name: name }).then(function() { location.reload(); })
      .catch(function(e) { toast('作成失敗: ' + e.message); btn.disabled = false; });
  });
  document.getElementById('newName').addEventListener('keydown', function(ev) {
    if (ev.key === 'Enter') document.getElementById('createBtn').click();
  });
  document.querySelectorAll('.f-save').forEach(function(b) {
    b.addEventListener('click', function() {
      var id = b.dataset.id;
      var name = document.querySelector('.f-name[data-id="' + id + '"]').value.trim();
      var order = Number(document.querySelector('.f-order[data-id="' + id + '"]').value);
      b.disabled = true;
      api('/' + id, { name: name, sortOrder: order }).then(function() { location.reload(); })
        .catch(function(e) { toast('保存失敗: ' + e.message); b.disabled = false; });
    });
  });
  document.querySelectorAll('.f-del').forEach(function(b) {
    b.addEventListener('click', function() {
      var n = Number(b.dataset.total);
      if (!confirm('フォルダ「' + b.dataset.name + '」を削除しますか?\\n\\n'
        + (n ? '中の ' + n + '件 は削除されず「未分類」に戻ります。' : '中身は空です。')
        + '\\n問い合わせそのものは消えません。')) return;
      b.disabled = true;
      api('/' + b.dataset.id + '/delete', {}).then(function(r) {
        toast('削除しました' + (r.detached ? ' (' + r.detached + '件を未分類に戻しました)' : ''));
        setTimeout(function(){ location.reload(); }, 800);
      }).catch(function(e) { toast('削除失敗: ' + e.message); b.disabled = false; });
    });
  });`;
  res.send(pageShell('問い合わせ管理 — フォルダ', 'folders', body, script));
});

router.post('/api/folders', (req, res) => {
  try {
    const f = createFolder((req.body || {}).name, actorOf(req));
    console.log(`[inquiry-hub] フォルダ作成「${f.name}」 by ${actorOf(req)}`);
    res.json({ ok: true, id: f.id, name: f.name });
  } catch (e) { res.status(400).json({ error: String(e?.message || e).slice(0, 200) }); }
});

router.post('/api/folders/:id(\\d+)', (req, res) => {
  try {
    const b = req.body || {};
    const r = updateFolder(Number(req.params.id), { name: b.name, sortOrder: b.sortOrder });
    res.json({ ok: true, ...r });
  } catch (e) { res.status(400).json({ error: String(e?.message || e).slice(0, 200) }); }
});

router.post('/api/folders/:id(\\d+)/delete', (req, res) => {
  try {
    const r = deleteFolder(Number(req.params.id), actorOf(req));
    console.log(`[inquiry-hub] フォルダ削除「${r.name}」 by ${actorOf(req)} / ${r.detached}件を未分類へ`);
    res.json({ ok: true, detached: r.detached });
  } catch (e) { res.status(400).json({ error: String(e?.message || e).slice(0, 200) }); }
});

// ═══════════════════════════════════════════════════════════════
// 🏷️ ラベルの設定 (2026-08-24 中原さん要望。メールディーラーの「ラベルの設定」相当)
//    色付きの目印。メールルールと組み合わせて条件一致で自動付与できる
// ═══════════════════════════════════════════════════════════════

router.get('/labels', (req, res) => {
  const labelsAll = listLabels({ withCounts: true });
  const swatches = id => LABEL_PALETTE.map(c =>
    `<button type="button" class="swatch" data-for="${id}" data-color="${c}" style="background:${c}" title="${c}" aria-label="色 ${c}"></button>`).join('');
  const rows = labelsAll.map(l => `
    <tr>
      <td data-full data-label="ラベル">${labelChip(l.name, l.color)}</td>
      <td data-full data-label="名前 / 色">
        <div class="row"><input type="text" class="l-name" data-id="${l.id}" value="${he(l.name)}" maxlength="${LABEL_NAME_MAX}">
        <input type="color" class="l-color" data-id="${l.id}" value="${he(l.color)}" title="色を選ぶ"></div>
        <div class="swatches">${swatches(l.id)}</div></td>
      <td data-label="表示順"><input type="number" class="l-order" data-id="${l.id}" value="${l.sort_order}" style="width:80px"></td>
      <td data-label="件数"><a href="/apps/inquiry-hub?view=all&label=${l.id}">${l.total}件</a></td>
      <td class="ops">
        <button class="pri l-save" data-id="${l.id}">保存</button>
        <button class="l-del" data-id="${l.id}" data-name="${he(l.name)}" data-total="${l.total}">削除</button>
      </td>
    </tr>`).join('');

  const body = `
  <div class="view-hint">🏷️ <b>ラベル</b> — 問い合わせに色付きの目印を付けられます (1件につき1つ)。
    付けても対応状況は変わりません。<b>📧メールルールと組み合わせると、条件に一致したメールへ取り込み時に自動で付きます</b>。</div>
  <div class="filters">
    <input type="text" id="newName" placeholder="新しいラベル名 (例: クレーム)" maxlength="${LABEL_NAME_MAX}" style="min-width:220px">
    <input type="color" id="newColor" value="${LABEL_PALETTE[0]}" title="色を選ぶ">
    <button class="pri" id="createBtn">➕ 作成</button>
  </div>
  <div class="filters swatches">${LABEL_PALETTE.map(c => `<button type="button" class="swatch" data-for="new" data-color="${c}" style="background:${c}" title="${c}" aria-label="色 ${c}"></button>`).join('')}</div>
  <div class="card">
    <table class="cardable">
      <thead><tr><th>ラベル</th><th>名前 / 色</th><th>表示順</th><th>件数</th><th>操作</th></tr></thead>
      <tbody>${rows || '<tr><td colspan="5" class="empty">ラベルはまだありません。上の欄から作成してください</td></tr>'}</tbody>
    </table>
  </div>`;

  const script = `
  function api(path, data) {
    return fetch('/apps/inquiry-hub/api/labels' + path, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data || {})
    }).then(function(r) { return r.json().catch(function(){ return {}; }).then(function(j){ if (!r.ok) throw new Error(j.error || ('HTTP ' + r.status)); return j; }); });
  }
  // 色見本タップで color input に反映 (スマホでカラーピッカーを開かなくても選べる)
  document.querySelectorAll('.swatch').forEach(function(s) {
    s.addEventListener('click', function() {
      var f = s.dataset.for;
      var input = f === 'new' ? document.getElementById('newColor') : document.querySelector('.l-color[data-id="' + f + '"]');
      if (input) input.value = s.dataset.color;
    });
  });
  document.getElementById('createBtn').addEventListener('click', function() {
    var el = document.getElementById('newName'), name = el.value.trim();
    if (!name) { toast('ラベル名を入力してください'); return; }
    this.disabled = true;
    var btn = this;
    api('', { name: name, color: document.getElementById('newColor').value }).then(function() { location.reload(); })
      .catch(function(e) { toast('作成失敗: ' + e.message); btn.disabled = false; });
  });
  document.getElementById('newName').addEventListener('keydown', function(ev) {
    if (ev.key === 'Enter') document.getElementById('createBtn').click();
  });
  document.querySelectorAll('.l-save').forEach(function(b) {
    b.addEventListener('click', function() {
      var id = b.dataset.id;
      var name = document.querySelector('.l-name[data-id="' + id + '"]').value.trim();
      var color = document.querySelector('.l-color[data-id="' + id + '"]').value;
      var order = Number(document.querySelector('.l-order[data-id="' + id + '"]').value);
      b.disabled = true;
      api('/' + id, { name: name, color: color, sortOrder: order }).then(function() { location.reload(); })
        .catch(function(e) { toast('保存失敗: ' + e.message); b.disabled = false; });
    });
  });
  document.querySelectorAll('.l-del').forEach(function(b) {
    b.addEventListener('click', function() {
      var n = Number(b.dataset.total);
      if (!confirm('ラベル「' + b.dataset.name + '」を削除しますか?\\n\\n'
        + (n ? '付いている ' + n + '件 からラベルが外れます。' : '使われていません。')
        + '\\nこのラベルを付けるメールルールがあれば、ラベル付与だけ解除されます。\\n問い合わせそのものは消えません。')) return;
      b.disabled = true;
      api('/' + b.dataset.id + '/delete', {}).then(function(r) {
        toast('削除しました' + (r.detached ? ' (' + r.detached + '件からラベルを外しました)' : ''));
        setTimeout(function(){ location.reload(); }, 800);
      }).catch(function(e) { toast('削除失敗: ' + e.message); b.disabled = false; });
    });
  });`;
  res.send(pageShell('問い合わせ管理 — ラベル', 'labels', body, script));
});

router.post('/api/labels', (req, res) => {
  try {
    const b = req.body || {};
    const l = createLabel(b.name, b.color, actorOf(req));
    console.log(`[inquiry-hub] ラベル作成「${l.name}」(${l.color}) by ${actorOf(req)}`);
    res.json({ ok: true, id: l.id, name: l.name, color: l.color });
  } catch (e) { res.status(400).json({ error: String(e?.message || e).slice(0, 200) }); }
});

router.post('/api/labels/:id(\\d+)', (req, res) => {
  try {
    const b = req.body || {};
    const r = updateLabel(Number(req.params.id), { name: b.name, color: b.color, sortOrder: b.sortOrder });
    res.json({ ok: true, ...r });
  } catch (e) { res.status(400).json({ error: String(e?.message || e).slice(0, 200) }); }
});

router.post('/api/labels/:id(\\d+)/delete', (req, res) => {
  try {
    const r = deleteLabel(Number(req.params.id), actorOf(req));
    console.log(`[inquiry-hub] ラベル削除「${r.name}」 by ${actorOf(req)} / ${r.detached}件から解除・ルール${r.rulesDetached}件から解除`);
    res.json({ ok: true, detached: r.detached, rulesDetached: r.rulesDetached });
  } catch (e) { res.status(400).json({ error: String(e?.message || e).slice(0, 200) }); }
});

// ═══════════════════════════════════════════════════════════════
// 🔗 リンクの設定 (2026-08-25 中原さん要望「リンク先はこっちで登録して自動で設定できるように」)
//    一覧上部に出す外部サイトへの導線。登録した分がそのまま上部バーに並ぶ
// ═══════════════════════════════════════════════════════════════

router.get('/links', (req, res) => {
  const links = listQuickLinks({ includeInactive: false });
  const rows = links.map(l => `
    <tr>
      <td data-label="表示" data-full><a href="${he(l.url)}" target="_blank" rel="noopener">${he(l.icon || '🔗')} ${he(l.name)} ↗</a></td>
      <td data-label="アイコン"><input type="text" class="k-icon" data-id="${l.id}" value="${he(l.icon || '')}" placeholder="🔗" style="width:60px;text-align:center"></td>
      <td data-label="リンク名"><input type="text" class="k-name" data-id="${l.id}" value="${he(l.name)}" maxlength="${LINK_NAME_MAX}"></td>
      <td data-full data-label="URL"><input type="text" class="k-url" data-id="${l.id}" value="${he(l.url)}" maxlength="${LINK_URL_MAX}" style="width:100%"></td>
      <td data-label="表示順"><input type="number" class="k-order" data-id="${l.id}" value="${l.sort_order}" style="width:80px"></td>
      <td class="ops">
        <button class="pri k-save" data-id="${l.id}">保存</button>
        <button class="k-del" data-id="${l.id}" data-name="${he(l.name)}">削除</button>
      </td>
    </tr>`).join('');

  const body = `
  <div class="view-hint">🔗 <b>リンク</b> — ここで登録したリンクが<b>一覧画面の上部にそのまま並びます</b>。
    楽天R-Messe・Yahoo!ストアクリエイターPro・Gmail は初回に登録済み (自由に変更・削除できます)。
    ネクストエンジンやロジザードなど、よく開くページを追加してください。</div>
  <div class="card" style="margin-bottom:16px">
    <div class="card-title">➕ リンクを追加 <span class="sub">(${links.length}/${MAX_ACTIVE_LINKS}件)</span></div>
    <div style="padding:12px 14px">
      <div class="filters">
        <input type="text" id="newIcon" placeholder="🔗" maxlength="8" style="width:70px;text-align:center" title="アイコン (絵文字1〜2字)">
        <input type="text" id="newName" placeholder="リンク名 (例: ネクストエンジン)" maxlength="${LINK_NAME_MAX}" style="min-width:220px">
        <input type="text" id="newUrl" placeholder="https://…" maxlength="${LINK_URL_MAX}" style="min-width:320px;flex:1">
        <button class="pri" id="createBtn">➕ 追加</button>
      </div>
      <div class="sub">URLは https:// から始まるものを貼り付けてください (http/https以外は登録できません)</div>
    </div>
  </div>
  <div class="card">
    <table class="cardable">
      <thead><tr><th>表示</th><th>アイコン</th><th>リンク名</th><th>URL</th><th>表示順</th><th>操作</th></tr></thead>
      <tbody>${rows || `<tr><td colspan="6" class="empty">リンクはまだありません。上の欄から追加してください</td></tr>`}</tbody>
    </table>
  </div>`;

  const script = `
  function api(path, data) {
    return fetch('/apps/inquiry-hub/api/links' + path, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data || {})
    }).then(function(r) { return r.json().catch(function(){ return {}; }).then(function(j){ if (!r.ok) throw new Error(j.error || ('HTTP ' + r.status)); return j; }); });
  }
  document.getElementById('createBtn').addEventListener('click', function() {
    var name = document.getElementById('newName').value.trim();
    var url = document.getElementById('newUrl').value.trim();
    if (!name) { toast('リンク名を入力してください'); return; }
    if (!url) { toast('URLを入力してください'); return; }
    var btn = this; btn.disabled = true;
    api('', { name: name, url: url, icon: document.getElementById('newIcon').value })
      .then(function() { location.reload(); })
      .catch(function(e) { toast('追加失敗: ' + e.message); btn.disabled = false; });
  });
  ['newName', 'newUrl'].forEach(function(id) {
    document.getElementById(id).addEventListener('keydown', function(ev) {
      if (ev.key === 'Enter') document.getElementById('createBtn').click();
    });
  });
  document.querySelectorAll('.k-save').forEach(function(b) {
    b.addEventListener('click', function() {
      var id = b.dataset.id;
      var v = function(cls) { return document.querySelector('.' + cls + '[data-id="' + id + '"]').value; };
      b.disabled = true;
      api('/' + id, { name: v('k-name').trim(), url: v('k-url').trim(), icon: v('k-icon'), sortOrder: Number(v('k-order')) })
        .then(function() { location.reload(); })
        .catch(function(e) { toast('保存失敗: ' + e.message); b.disabled = false; });
    });
  });
  document.querySelectorAll('.k-del').forEach(function(b) {
    b.addEventListener('click', function() {
      if (!confirm('リンク「' + b.dataset.name + '」を削除しますか?\\n\\n上部バーから消えます (リンク先のサイトには影響しません)。')) return;
      b.disabled = true;
      api('/' + b.dataset.id + '/delete', {}).then(function() {
        toast('削除しました');
        setTimeout(function(){ location.reload(); }, 600);
      }).catch(function(e) { toast('削除失敗: ' + e.message); b.disabled = false; });
    });
  });`;
  res.send(pageShell('問い合わせ管理 — リンク', 'links', body, script));
});

router.post('/api/links', (req, res) => {
  try {
    const b = req.body || {};
    const l = createQuickLink({ name: b.name, url: b.url, icon: b.icon }, actorOf(req));
    console.log(`[inquiry-hub] リンク追加「${l.name}」 ${l.url} by ${actorOf(req)}`);
    res.json({ ok: true, ...l });
  } catch (e) { res.status(400).json({ error: String(e?.message || e).slice(0, 200) }); }
});

router.post('/api/links/:id(\\d+)', (req, res) => {
  try {
    const b = req.body || {};
    res.json({ ok: true, ...updateQuickLink(Number(req.params.id), { name: b.name, url: b.url, icon: b.icon, sortOrder: b.sortOrder }) });
  } catch (e) { res.status(400).json({ error: String(e?.message || e).slice(0, 200) }); }
});

router.post('/api/links/:id(\\d+)/delete', (req, res) => {
  try {
    const r = deleteQuickLink(Number(req.params.id));
    console.log(`[inquiry-hub] リンク削除「${r.name}」 by ${actorOf(req)}`);
    res.json({ ok: true });
  } catch (e) { res.status(400).json({ error: String(e?.message || e).slice(0, 200) }); }
});

// ═══════════════════════════════════════════════════════════════
// 📎 添付ファイルの配信 (2026-08-02 スタッフ要望「添付された写真や画像を確認したい」)
//    実体はDBに持たず、都度チャネル (Gmail/楽天/Yahoo!) から取って返す (attachments.js)
// ═══════════════════════════════════════════════════════════════

router.get('/attachments/:id(\\d+)', async (req, res) => {
  const ctx = getAttachmentContext(Number(req.params.id));
  if (!ctx) return res.status(404).type('text/plain; charset=utf-8').send('添付が見つかりません');
  try {
    const { buffer, contentType, fileName } = await fetchAttachmentBody(ctx);
    // 画像・PDF以外、および ?download=1 は必ずダウンロード (ブラウザ内で開かせない)
    const inline = isInlineSafe(contentType) && req.query.download !== '1';
    res.setHeader('Content-Type', contentType);
    res.setHeader('Content-Length', String(buffer.length));
    res.setHeader('Content-Disposition', contentDispositionValue(fileName, inline));
    res.setHeader('X-Content-Type-Options', 'nosniff');
    // 添付は顧客から預かった中身そのもの → ディスクに残さない
    // (共有PCでログアウト後にブラウザキャッシュから開ける状態を作らない)
    res.setHeader('Cache-Control', 'private, no-store');
    // 他サイトからの埋め込み・PDFビューア経由の外部読み込みを塞ぐ
    res.setHeader('Cross-Origin-Resource-Policy', 'same-origin');
    res.setHeader('Content-Security-Policy', "default-src 'none'; img-src 'self'; object-src 'none'; sandbox");
    res.status(200).end(buffer);
  } catch (e) {
    // 混雑 (同時取得の待ち行列超過) は一時的なので 503 + Retry-After で返す
    if (e?.busy) {
      res.setHeader('Retry-After', '5');
      return res.status(503).type('text/plain; charset=utf-8').send(String(e.message));
    }
    // それ以外は画面には固定文言のみ (上流のURL・内部ホスト名・設定状況を利用者に出さない。
    // 原因はサーバーログとエラーIDで追う。Codexレビュー Medium-5)
    const errorId = crypto.randomUUID().slice(0, 8);
    console.warn(`[inquiry-hub] 添付取得失敗 id=${ctx.id} (${ctx.channel_type}) errorId=${errorId}: ${String(e?.message || e).slice(0, 300)}`);
    res.status(502).type('text/plain; charset=utf-8')
      .send(`添付を取得できませんでした (時間をおいて再度お試しください)。解決しない場合は管理者にエラーID ${errorId} をお伝えください`);
  }
});

// ═══════════════════════════════════════════════════════════════
// ⏰ 締め前確認 (2026-08-28 中原さん要望)
//    キャンセル・住所変更・日時指定の連絡を、ロジザードへ流す前 (09:00/12:30/14:30) に拾う。
//    人がネクストエンジンで直してから流す運用なので、ここは「気づくための画面」に徹する
//    (システムが出荷を止めたりはしない)
// ═══════════════════════════════════════════════════════════════

const CUTOFF_PAGE_CSS = `<style>
.cut-hero { display:flex; align-items:baseline; gap:12px; flex-wrap:wrap; padding:14px;
  border-radius:10px; background:#fffbeb; border:1px solid #fcd34d; margin-bottom:14px }
.cut-hero .big { font-size:26px; font-weight:700 }
.cut-item { border:1px solid #e2e8f0; border-left-width:5px; border-radius:10px; padding:12px;
  margin-bottom:10px; background:#fff }
.cut-item.acked { opacity:.55 }
.cut-head { display:flex; align-items:baseline; gap:8px; flex-wrap:wrap; margin-bottom:4px }
.cut-body { font-size:13px; color:#475569; margin:6px 0; white-space:pre-wrap; overflow-wrap:anywhere }
.cut-meta { font-size:12px; color:#64748b; display:flex; gap:10px; flex-wrap:wrap }
.cut-hit { font-size:11px; color:#94a3b8 }
.cut-ops { margin-top:8px; display:flex; gap:6px; flex-wrap:wrap }
.cut-empty { padding:24px; text-align:center; color:#166534; background:#f0fdf4;
  border:1px solid #86efac; border-radius:10px; font-size:15px }
</style>`;

router.get('/cutoff', (req, res) => {
  const showAcked = req.query.acked === '1';
  const showDone = req.query.done === '1';
  // 除外したせいで何が見えなくなっているかを確認できる導線 (間違えて外していないかを見つけられるように)
  const showExcluded = req.query.excluded === '1';
  const { items, truncated, excluded } = listCutoffItems({
    includeAcked: showAcked, includeDone: showDone, includeExcluded: showExcluded });
  const excludes = listCutoffExcludes();
  // いま出ている差出人を多い順に (お客さまでないものをまとめて外せるように)
  const senderCount = new Map();
  for (const it of items) {
    if (!it.sender) continue;
    senderCount.set(it.sender, (senderCount.get(it.sender) || 0) + 1);
  }
  const senderRanking = [...senderCount.entries()]
    .map(([sender, count]) => ({ sender, count }))
    .sort((a, b) => b.count - a.count || a.sender.localeCompare(b.sender))
    .slice(0, 15);
  const counts = summarize(items.filter(i => !i.ack));
  const next = nextCutoff();
  const h = Math.floor(next.minutesLeft / 60), m = next.minutesLeft % 60;

  const itemCards = items.map(it => `
    <div class="cut-item${it.ack ? ' acked' : ''}" style="border-left-color:${it.kind === 'cancel' ? '#dc2626' : it.kind === 'address' ? '#f59e0b' : '#3b82f6'}">
      <div class="cut-head">
        <span class="badge" style="${it.style}">${it.icon} ${he(it.kindLabel)}</span>
        ${chBadge(it.channel)}
        <a href="/apps/inquiry-hub/inquiries/${it.inquiryId}"><b>${he(it.subject || '(件名なし)')}</b></a>
        ${it.ack ? `<span class="badge" style="background:#f1f5f9;color:#475569">${it.ack.status === 'done' ? '✅対応済み' : '対象外'}</span>` : ''}
      </div>
      <div class="cut-meta">
        <span>📅 ${fmtJst(it.receivedAt)}</span>
        <span>👤 ${he(it.customerName || '(名前なし)')}</span>
        ${it.sender ? `<span>✉️ ${he(it.sender)}</span>` : ''}
        <span>${it.orderNumber ? '🔗 注文 ' + he(it.orderNumber) : '⚠️ 注文番号なし (NEで名前・メールから探す)'}</span>
        ${it.assignedTo ? `<span>担当 ${he(it.assignedTo)}</span>` : ''}
      </div>
      <div class="cut-body">${he(toPreviewLine(it.body, 300))}</div>
      <div class="cut-hit">引っかかった言葉: ${he(it.matched.join('、'))}</div>
      <div class="cut-ops">
        ${it.ack
          ? `<button class="cut-undo" data-msg="${it.messageId}" data-kind="${he(it.kind)}">↩️ 未対応に戻す</button>`
          : `<button class="pri cut-ack" data-msg="${it.messageId}" data-kind="${he(it.kind)}" data-status="done">✅ ネクストエンジンで直した</button>
             <button class="cut-ack" data-msg="${it.messageId}" data-kind="${he(it.kind)}" data-status="not_applicable">対象外だった</button>`}
        ${it.sender ? `<span style="flex:1"></span>
          <button class="cut-excl" data-sender="${he(it.sender)}" title="この差出人からのメールを今後この画面に出しません (問い合わせ自体は消えません)">🚫 この差出人は今後出さない</button>
          <button class="cut-excl-dom" data-domain="${he('@' + String(it.sender).split('@').pop())}" title="このドメインからのメールを今後この画面に出しません">🚫 @${he(String(it.sender).split('@').pop())} ごと</button>` : ''}
      </div>
    </div>`).join('');

  const body = `${CUTOFF_PAGE_CSS}
  <div class="view-hint">⏰ <b>締め前確認</b> — お客さまからの
    <b>キャンセル・住所変更・お届け日時の変更</b>を、ロジザードへ流す前に拾って並べます。
    <b>ネクストエンジンで先に直してから</b>流してください。
    <span class="sub">直近${LOOKBACK_DAYS}日ぶん・${showDone ? '完了した問い合わせも含む' : '完了にした問い合わせは対応済みとみなして出しません'}。
    言葉で拾っているので的外れなものも混じります (「対象外だった」で消せます)</span></div>

  ${truncated ? `<div class="card" style="padding:10px;background:#fef2f2;border-color:#fca5a5">
    ⚠️ <b>件数が多く、全部は表示しきれていません</b>。ここに出ていないものが残っている可能性があります。</div>` : ''}

  ${showExcluded ? `<div class="card" style="padding:10px;background:#f1f5f9">
    🚫 <b>除外した差出人のぶんも表示しています</b> (ふだんは出ません)。
    お客さまのメールが混じっていたら、下の「この画面に出さない差出人」から「外す」を押してください。</div>` : ''}

  <div class="cut-hero">
    <div>次の締めは <span class="big">${he(next.label)}</span>${next.isTomorrow ? ' <span class="sub">(翌朝)</span>' : ''}</div>
    <div class="sub">あと ${h > 0 ? h + '時間' : ''}${m}分</div>
    <span style="flex:1"></span>
    <div>${counts.total === 0
      ? '<b style="color:#166534">未対応 0件</b>'
      : `<b style="color:#b91c1c">未対応 ${counts.inquiries}件</b> <span class="sub">(${CUTOFF_KINDS.filter(k => counts.byKind[k.kind]).map(k => `${k.icon}${counts.byKind[k.kind]}`).join(' ')})</span>`}</div>
  </div>

  <div class="filters">
    <a class="${showAcked ? 'ghost' : 'pri'} btn-link" href="/apps/inquiry-hub/cutoff">未対応だけ</a>
    <a class="${showAcked ? 'pri' : 'ghost'} btn-link" href="/apps/inquiry-hub/cutoff?acked=1">対応済みも表示</a>
    <a class="${showDone ? 'pri' : 'ghost'} btn-link" href="/apps/inquiry-hub/cutoff?done=${showDone ? '0' : '1'}">完了した問い合わせも見る</a>
    <a class="${showExcluded ? 'pri' : 'ghost'} btn-link" href="/apps/inquiry-hub/cutoff?excluded=${showExcluded ? '0' : '1'}"
      title="除外した差出人のぶんも表示します。間違えて外していないかの確認に使ってください">🚫 除外したぶんも見る</a>
    <span style="flex:1"></span>
    <span class="sub">締め: ${CUTOFF_TIMES.map(c => c.label).join(' / ')}</span>
  </div>

  ${items.length ? itemCards : `<div class="cut-empty">✅ 検知された未対応は0件です<br>
    <span class="sub">キャンセル・住所変更・日時指定の連絡は見つかりませんでした。
    ただし言葉で拾っているので取りこぼしはありえます — いつもどおり確認して流してください。</span></div>`}

  ${senderRanking.length > 1 ? `<details class="card" style="padding:12px">
    <summary><b>📊 いま出ている差出人 (多い順)</b> <span class="sub">— お客さまでないものをまとめて外せます</span></summary>
    <table class="cardable" style="margin-top:10px">
      <thead><tr><th>差出人</th><th>件数</th><th></th></tr></thead>
      <tbody>${senderRanking.map(s => `
        <tr>
          <td><code>${he(s.sender)}</code></td>
          <td class="nowrap">${s.count}件</td>
          <td class="nowrap ops">
            <button class="cut-excl" data-sender="${he(s.sender)}">🚫 出さない</button>
            <button class="cut-excl-dom" data-domain="${he('@' + s.sender.split('@').pop())}">@${he(s.sender.split('@').pop())} ごと</button>
          </td>
        </tr>`).join('')}</tbody>
    </table>
  </details>` : ''}

  <details class="card" style="padding:12px">
    <summary><b>🚫 この画面に出さない差出人 (${excludes.length}件)</b>
      ${excluded ? `<span class="sub"> — 直近で ${excluded}通 を除外しました</span>` : ''}</summary>
    <div class="sub" style="margin:8px 0">業者からの連絡・Amazonの通知・自動配信など、<b>お客さまではない差出人</b>をここに入れておくと出なくなります。
      一覧の「🚫この差出人は今後出さない」からも足せます。<br>
      ⭐<b>間違えて入れても「外す」を押せば元どおり出るようになります</b> (取り消せます)。
      <b>問い合わせ自体が消えるわけでもありません</b> — 受信トレイには残るので、この画面に出さないだけです。<br>
      no-reply系・バウンスは登録しなくても自動で外れます。</div>
    <div class="filters">
      <input type="text" id="exclInput" placeholder="foo@example.com または @example.com (ドメインごと)" style="min-width:300px">
      <input type="text" id="exclNote" placeholder="メモ (任意)" style="min-width:160px">
      <button class="pri" id="exclAdd">➕ 追加</button>
    </div>
    <table class="cardable" style="margin-top:10px">
      <thead><tr><th>差出人 / ドメイン</th><th>メモ</th><th>登録</th><th></th></tr></thead>
      <tbody>${excludes.length ? excludes.map(e => `
        <tr>
          <td class="nowrap"><code>${he(e.pattern)}</code>${e.pattern.startsWith('@') ? ' <span class="badge" style="background:#f1f5f9;color:#475569">ドメイン</span>' : ''}</td>
          <td class="sub">${he(e.note || '')}</td>
          <td class="sub nowrap">${fmtJst(e.created_at)}</td>
          <td class="ops"><button class="excl-del" data-id="${e.id}" data-pattern="${he(e.pattern)}">外す</button></td>
        </tr>`).join('') : '<tr><td colspan="4" class="empty">まだ登録がありません</td></tr>'}</tbody>
    </table>
  </details>`;

  const script = `
  function api(path, data) {
    return fetch('/apps/inquiry-hub/api/cutoff' + path, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data || {})
    }).then(function(r) { return r.json().catch(function(){ return {}; }).then(function(j){ if (!r.ok) throw new Error(j.error || ('HTTP ' + r.status)); return j; }); });
  }
  document.querySelectorAll('.cut-ack').forEach(function(b) {
    b.addEventListener('click', function() {
      b.disabled = true;
      api('/ack', { messageId: Number(b.dataset.msg), kind: b.dataset.kind, status: b.dataset.status })
        .then(function() { location.reload(); })
        .catch(function(e) { toast('失敗: ' + e.message); b.disabled = false; });
    });
  });
  document.querySelectorAll('.cut-undo').forEach(function(b) {
    b.addEventListener('click', function() {
      b.disabled = true;
      api('/unack', { messageId: Number(b.dataset.msg), kind: b.dataset.kind })
        .then(function() { location.reload(); })
        .catch(function(e) { toast('失敗: ' + e.message); b.disabled = false; });
    });
  });
  // 差出人ごと・ドメインごとの除外 (一覧から1タップで足せる)
  function addExclude(pattern, note, btn) {
    btn.disabled = true;
    api('/exclude', { pattern: pattern, note: note })
      .then(function() { location.reload(); })
      .catch(function(e) { toast('失敗: ' + e.message); btn.disabled = false; });
  }
  var UNDO_NOTE = String.fromCharCode(10, 10)
    + '※ 間違えても、下の「🚫この画面に出さない差出人」から「外す」を押せば元に戻せます。'
    + String.fromCharCode(10) + '※ 問い合わせ自体は消えません (受信トレイには残ります)。';
  document.querySelectorAll('.cut-excl').forEach(function(b) {
    b.addEventListener('click', function() {
      if (!confirm(b.dataset.sender + ' からのメールを、今後この画面に出さないようにします。' + UNDO_NOTE)) return;
      addExclude(b.dataset.sender, '画面から追加', b);
    });
  });
  document.querySelectorAll('.cut-excl-dom').forEach(function(b) {
    b.addEventListener('click', function() {
      if (!confirm(b.dataset.domain + ' のドメイン全体を、今後この画面に出さないようにします。'
        + String.fromCharCode(10) + 'そのドメインにお客さまがいる場合は、個別のアドレスだけにしてください。' + UNDO_NOTE)) return;
      addExclude(b.dataset.domain, '画面からドメインごと追加', b);
    });
  });
  var exclAdd = document.getElementById('exclAdd');
  if (exclAdd) exclAdd.addEventListener('click', function() {
    var v = document.getElementById('exclInput').value.trim();
    if (!v) { toast('メールアドレス (または @ドメイン) を入れてください'); return; }
    addExclude(v, document.getElementById('exclNote').value.trim() || null, this);
  });
  document.querySelectorAll('.excl-del').forEach(function(b) {
    b.addEventListener('click', function() {
      if (!confirm(b.dataset.pattern + ' の除外を外します (また出るようになります)。')) return;
      b.disabled = true;
      api('/exclude/' + b.dataset.id + '/delete', {})
        .then(function() { location.reload(); })
        .catch(function(e) { toast('失敗: ' + e.message); b.disabled = false; });
    });
  });`;
  res.send(pageShell('問い合わせ管理 — 締め前確認', 'cutoff', body, script));
});

router.post('/api/cutoff/ack', (req, res) => {
  try {
    const b = req.body || {};
    const r = ackCutoffItem({ messageId: b.messageId, kind: b.kind, status: b.status, note: b.note }, actorOf(req));
    console.log(`[inquiry-hub] 締め前確認 ${r.kind}=${r.status} msg=${r.messageId} by ${actorOf(req)}`);
    res.json({ ok: true, ...r });
  } catch (e) { res.status(400).json({ error: String(e?.message || e).slice(0, 200) }); }
});

router.post('/api/cutoff/unack', (req, res) => {
  try {
    res.json({ ok: true, ...unackCutoffItem({ messageId: (req.body || {}).messageId, kind: (req.body || {}).kind }) });
  } catch (e) { res.status(400).json({ error: String(e?.message || e).slice(0, 200) }); }
});

router.post('/api/cutoff/exclude', (req, res) => {
  try {
    const b = req.body || {};
    const r = addCutoffExclude(b.pattern, b.note, actorOf(req));
    console.log(`[inquiry-hub] 締め前確認から除外「${r.pattern}」 by ${actorOf(req)}`);
    res.json({ ok: true, ...r });
  } catch (e) { res.status(400).json({ error: String(e?.message || e).slice(0, 200) }); }
});

router.post('/api/cutoff/exclude/:id(\\d+)/delete', (req, res) => {
  try {
    res.json({ ok: true, ...removeCutoffExclude(Number(req.params.id)) });
  } catch (e) { res.status(400).json({ error: String(e?.message || e).slice(0, 200) }); }
});

// ═══════════════════════════════════════════════════════════════
// 📦 返品・交換案件 (2026-09-01 中原さん要望「いま誰がボールを持っているか一発で分かるように」)
//    正本 = AI_reference『システム設計\返品交換案件管理_要件定義_20260901.md』
//    ⭐問い合わせの完了と案件の完了は別物。返信が終わっても、返金と回収が残っていれば案件は残る
// ═══════════════════════════════════════════════════════════════

const CASE_PAGE_CSS = `<style>
.board-wrap { overflow-x: auto; padding-bottom: 8px; }
.board { display: grid; grid-auto-flow: column; gap: 10px; align-items: start; min-width: min-content; }
.bcol { background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 10px; padding: 9px; width: 240px; }
.bcol.empty { width: 120px; background: transparent; border-style: dashed; }
.bcol-head { display: flex; justify-content: space-between; align-items: baseline; gap: 6px; margin-bottom: 8px; }
.bcol-head b { font-size: 13px; }
.bcol.empty .bcol-head b { color: #94a3b8; font-weight: 500; font-size: 12px; }
.bcol-head .n { font-size: 12px; color: #94a3b8; }
.bcard { display: block; background: #fff; border: 1px solid #e2e8f0; border-left: 3px solid #cbd5e1;
  border-radius: 8px; padding: 9px 10px; margin-bottom: 8px; text-decoration: none; color: inherit; }
.bcard:hover { border-color: #94a3b8; }
.bcard.late { border-left-color: #dc2626; }
.bcard.soon { border-left-color: #f59e0b; }
.bcard.done { opacity: .55; }
.bcard .no { font-size: 11px; color: #94a3b8; }
.bcard .who { font-size: 13px; font-weight: 700; margin-top: 1px; }
.bcard .item { font-size: 12px; color: #64748b; }
.bcard .next { font-size: 12px; margin-top: 7px; padding-top: 7px; border-top: 1px dashed #e2e8f0; }
.bcard .next .k { display: block; font-size: 11px; color: #94a3b8; }
.bcard .foot { display: flex; justify-content: space-between; gap: 6px; margin-top: 7px; font-size: 12px; color: #64748b; }
.bcard .over { color: #b91c1c; font-weight: 700; }
.bcol-empty { text-align: center; color: #94a3b8; font-size: 12px; padding: 8px 0; }
.case-facts { display: grid; grid-template-columns: repeat(auto-fit, minmax(140px,1fr)); gap: 1px;
  background: #e2e8f0; border: 1px solid #e2e8f0; border-radius: 8px; overflow: hidden; margin: 12px 0; }
.case-facts .f { background: #fff; padding: 9px 12px; }
.case-facts .k { font-size: 11px; color: #94a3b8; }
.case-facts .v { font-size: 14px; font-weight: 600; margin-top: 2px; }
.next-box { border: 1px solid #e2e8f0; border-left: 3px solid #2563eb; background: #eff6ff;
  border-radius: 8px; padding: 13px 15px; margin: 12px 0; }
.next-box.late { border-left-color: #dc2626; background: #fef2f2; }
.next-box.done { border-left-color: #16a34a; background: #f0fdf4; }
.next-box .lbl { font-size: 12px; color: #64748b; }
.next-box .what { font-size: 17px; font-weight: 700; margin-top: 2px; }
.next-box .who { font-size: 13px; color: #64748b; margin-top: 4px; }
.steps { border: 1px solid #e2e8f0; border-radius: 8px; overflow: hidden; }
.step-row { display: grid; grid-template-columns: 120px minmax(0,1fr) 130px 120px auto; gap: 12px;
  padding: 11px 13px; border-bottom: 1px solid #f1f5f9; align-items: start; }
.step-row:last-child { border-bottom: 0; }
.step-row.settled { background: #f8fafc; }
.step-row.now { background: #eff6ff; }
.step-row .nm { font-weight: 700; font-size: 14px; }
.step-row .nt { font-size: 12px; color: #64748b; margin-top: 2px; }
.step-row .cd { font-size: 11px; color: #cbd5e1; margin-top: 2px; }
.step-row .wp { font-size: 12px; color: #92400e; }
.step-row .as { font-size: 12px; color: #94a3b8; }
.step-row .due { font-size: 12px; color: #64748b; }
.step-row .due .over { color: #b91c1c; font-weight: 700; display: block; }
.step-row .ops { display: flex; gap: 5px; flex-wrap: wrap; justify-content: flex-end; }
@media (max-width: 900px) { .step-row { grid-template-columns: 1fr; gap: 6px; } .step-row .ops { justify-content: flex-start; } }
.case-hist { font-size: 13px; }
.case-hist .r { display: flex; gap: 10px; padding: 7px 0; border-top: 1px solid #f1f5f9; }
.case-hist .w { color: #94a3b8; font-size: 12px; white-space: nowrap; }
.case-panel { border: 1px solid #e2e8f0; border-left: 3px solid #2563eb; background: #f8fafc;
  border-radius: 8px; padding: 12px 14px; margin: 12px 0; }
.case-panel.suggest { border-left-color: #f59e0b; background: #fffbeb; }
.case-panel h4 { margin: 0 0 6px; font-size: 14px; }
.case-panel .row { display: flex; flex-wrap: wrap; gap: 6px 18px; font-size: 13px; }
.case-panel .ops { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 10px; }
</style>`;

/** ボードのカード1枚 */
function caseCard(c) {
  const late = c.over > 0, soon = !late && c.next_action_at
    && Date.parse(c.next_action_at) - Date.now() < 24 * 3600 * 1000;
  const cls = c.status !== 'active' ? 'done' : late ? 'late' : soon ? 'soon' : '';
  const due = c.next_action_at ? rcJstDate(c.next_action_at).slice(5).replace('-', '/') : '期限なし';
  return `<a class="bcard ${cls}" href="/apps/inquiry-hub/cases/${c.id}">
    <div class="no">${he(c.case_no)} ・ ${he(CASE_TYPES[c.case_type]?.label || c.case_type)}</div>
    <div class="who">${he(c.customer_name || '(顧客名なし)')}</div>
    <div class="item">${he(c.product_name || c.order_no || '')}</div>
    ${c.status === 'active' && c.next_step_label
      ? `<div class="next"><span class="k">次にやること</span>${he(c.next_step_label)}</div>` : ''}
    <div class="foot"><span>${he(c.assigned_user_id || '担当なし')}</span>
      <span class="${late ? 'over' : ''}">${he(due)}${late ? ' ・ ' + c.over + '日超過' : ` ・ ${c.steps_done}/${c.steps_total}工程`}</span></div>
  </a>`;
}

/** 📦 返品・交換案件ボード。⭐列は「誰待ち」が既定。工程は枝分かれするので既定にしない */
router.get('/cases', (req, res) => {
  const view = ['wait', 'stage', 'list'].includes(req.query.view) ? req.query.view : 'wait';
  const showDone = req.query.done === '1';
  const rows = listBoardCases({ includeCompleted: showDone });
  const active = rows.filter(r => r.status === 'active');
  const overdue = active.filter(r => r.over > 0).length;

  const viewLink = (v, label, title) =>
    `<a class="${view === v ? 'pri' : 'ghost'} btn-link" href="/apps/inquiry-hub/cases?view=${v}${showDone ? '&done=1' : ''}" title="${he(title)}">${he(label)}</a>`;

  let main = '';
  if (view === 'list') {
    // 件数が増えたとき用の表 (返品SaaSの主画面はこの形。Loop Returns / ReturnGO)
    main = `<div class="board-wrap"><table class="list-table" style="width:100%;font-size:13px">
      <thead><tr><th>案件番号</th><th>顧客・商品</th><th>種別</th><th>待ち先</th><th>工程</th><th>担当</th><th>次にやること</th><th>次回確認日</th></tr></thead>
      <tbody>${rows.map(c => `<tr>
        <td><a href="/apps/inquiry-hub/cases/${c.id}">${he(c.case_no)}</a></td>
        <td>${he(c.customer_name || '(顧客名なし)')}<br><span class="sub">${he(c.product_name || '')}</span></td>
        <td>${he(CASE_TYPES[c.case_type]?.label || c.case_type)}</td>
        <td>${badge({ badge: WAITING_ON[c.waiting_on]?.badge }, WAITING_ON[c.waiting_on]?.label || c.waiting_on)}</td>
        <td>${he(STAGES[c.stage]?.label || c.stage)}<br><span class="sub">${c.steps_done}/${c.steps_total}工程</span></td>
        <td>${he(c.assigned_user_id || '')}</td>
        <td>${c.status === 'active' ? he(c.next_step_label || '') : '<span class="sub">—</span>'}</td>
        <td>${c.next_action_at ? he(rcJstDate(c.next_action_at)) : '<span class="sub">なし</span>'}
          ${c.over > 0 ? `<br><b style="color:#b91c1c">${c.over}日超過</b>` : ''}</td>
      </tr>`).join('') || '<tr><td colspan="8" class="empty">案件はまだありません</td></tr>'}</tbody></table></div>`;
  } else {
    const key = view === 'stage' ? 'stage' : 'waiting_on';
    const cols = boardColumns(view === 'stage' ? 'stage' : 'wait');
    main = `<div class="board-wrap"><div class="board">${cols.map(col => {
      const items = rows.filter(c => (view === 'stage'
        ? (c.status === 'active' ? c.stage : 'COMPLETED')
        : (c.status === 'active' ? c.waiting_on : 'NONE')) === col.key);
      const empty = items.length === 0;
      return `<div class="bcol${empty ? ' empty' : ''}">
        <div class="bcol-head"><b>${he(col.label)}</b><span class="n">${items.length}</span></div>
        ${empty ? '<div class="bcol-empty">なし</div>' : items.map(caseCard).join('')}
      </div>`;
    }).join('')}</div></div>`;
  }

  const body = `${CASE_PAGE_CSS}
  <div class="view-hint">📦 <b>返品・交換案件</b> — 問い合わせのうち
    <b>商品が動く・お金が動くもの</b>だけを案件にして、終わるまで追いかけます。
    <span class="sub">問い合わせを完了にしても案件は残ります (返信が終わったことと、返金・代品が終わったことは別)。
    列は「対応状況」＝いま誰の返事・行動待ちか。工程は枝分かれするので、こちらを既定にしています</span></div>

  <div class="cut-hero">
    <div>未完了 <span class="big">${active.length}</span>件</div>
    <div class="sub">${overdue > 0 ? `<b style="color:#b91c1c">期限超過 ${overdue}件</b>` : '期限超過なし'}</div>
    <span style="flex:1"></span>
    <div class="sub">${he(rcJstDate(new Date().toISOString()))} 時点</div>
  </div>

  <div class="filters">
    ${viewLink('wait', '対応状況', '列＝いま誰の返事・行動を待っているか。日々の対応と放置防止はこの見方')}
    ${viewLink('stage', '処理工程', '列＝処理のどこまで進んだか。全体の進み具合を見るときの見方')}
    ${viewLink('list', '一覧', '件数が増えたとき用。表で見る')}
    <span style="flex:1"></span>
    <a class="${showDone ? 'pri' : 'ghost'} btn-link" href="/apps/inquiry-hub/cases?view=${view}${showDone ? '' : '&done=1'}">完了した案件も表示</a>
  </div>

  ${main}

  <div class="sub" style="margin-top:12px">
    カードのドラッグ移動は入れていません — 待ち先は工程の状態から決まるので、
    ボード上で直接動かせると「実際は終わっていないのに動いた」という嘘が入ります。
  </div>`;
  res.send(pageShell('問い合わせ管理 — 返品・交換案件', 'cases', body, ''));
});

/** 📦 案件詳細。⭐上から「次にやること」→「対応工程」→「履歴」 */
router.get('/cases/:id(\\d+)', (req, res) => {
  const c = getCase(Number(req.params.id));
  if (!c) return res.status(404).send(pageShell('案件が見つかりません', 'cases',
    '<div class="empty">案件が見つかりません</div>', ''));
  const steps = listSteps(c.id);
  const next = nextStepOf(steps);
  const inqs = listCaseInquiries(c.id);
  const events = listEvents(c.id, 20);
  const blockers = blockersOf(c.id);
  const over = overdueDays(c.next_action_at);
  // 例外操作 (必要な工程を外す / 未処理を残して完了) の権限。
  // ⭐担当者マスタが未整備のうちは通すが、その旨を画面に出す
  // ⚠️判定できなかったときは「できない」と出す。ここで allowed:true にすると、
  //   画面は押せそうに見えるのに API は 403 を返す、という食い違いになる
  const excPerm = (() => { try { return canDoException(actorOf(req)); }
    catch { return { allowed: false, unmanaged: false, error: true }; } })();
  const settled = s => ['completed', 'exception'].includes(s.progress_status) || s.necessity_status === 'not_required';

  const stepRow = s => {
    const isSettled = settled(s);
    const stateMeta = s.necessity_status === 'undecided' ? NECESSITY.undecided
      : s.necessity_status === 'not_required' ? NECESSITY.not_required
      : PROGRESS[s.progress_status];
    const sOver = !isSettled && s.due_at ? overdueDays(s.due_at) : 0;
    // ⭐「対応不要」は要否がまだ決まっていない工程だけ。
    //   必要と決まっている工程を外すのは例外操作 (理由 + 権限)。ボタンの見た目も分ける
    const ops = s.necessity_status === 'undecided'
      ? `<button class="stp" data-id="${s.id}" data-act="need">必要にする</button>
         <button class="stp ghost" data-id="${s.id}" data-act="skip">対応不要にする</button>`
      : isSettled
        ? `<button class="stp ghost" data-id="${s.id}" data-act="undo">戻す</button>`
        : `<button class="stp pri" data-id="${s.id}" data-act="complete">完了にする</button>
           <button class="stp" data-id="${s.id}" data-act="wait">回答・到着待ちにする</button>
           <button class="stp ghost stp-exc" data-id="${s.id}" data-act="skip_required"
             title="必要と決まっている工程を外します。理由が要ります">この工程を外す</button>`;
    return `<div class="step-row${isSettled ? ' settled' : ''}${next && next.id === s.id ? ' now' : ''}">
      <div>${badge({ badge: stateMeta?.badge }, stateMeta?.label || s.progress_status)}</div>
      <div><div class="nm">${he(stepLabel(c.case_type, s.step_type))}</div>
        ${s.note ? `<div class="nt">${he(s.note)}</div>` : ''}
        ${s.external_ref ? `<div class="nt">外部参照 ${he(s.external_ref)}</div>` : ''}
        <div class="cd">${he(s.step_type)}</div></div>
      <div>${s.waiting_party && !isSettled ? `<div class="wp">${he(s.waiting_party)}</div>` : ''}
        <div class="as">${isSettled && s.completed_by ? he(s.completed_by) : s.assignee_id ? '確認：' + he(s.assignee_id) : ''}</div></div>
      <div class="due">${isSettled
        ? (s.completed_at ? `<span class="sub">${he(rcJstDate(s.completed_at))}</span>` : '')
        : s.due_at ? `${he(rcJstDate(s.due_at).slice(5).replace('-', '/'))}${sOver > 0 ? `<span class="over">${sOver}日超過</span>` : ''}` : ''}</div>
      <div class="ops">${ops}</div>
    </div>`;
  };

  const openSteps = steps.filter(s => !settled(s));
  const doneSteps = steps.filter(settled);

  const body = `${CASE_PAGE_CSS}
  <div class="filters"><a class="ghost btn-link" href="/apps/inquiry-hub/cases">← 案件ボードに戻る</a></div>

  <div class="card" style="padding:16px">
    <div style="display:flex;flex-wrap:wrap;gap:12px;justify-content:space-between;align-items:flex-start">
      <div>
        <div class="sub">${he(c.case_no)}</div>
        <h2 style="margin:2px 0 0;font-size:19px">${he(c.product_name || c.customer_name || c.case_no)}</h2>
        <div class="sub">${he(c.customer_name || '')}${c.order_no ? ' ・ ' + he(c.order_no) : ''}${c.order_channel ? ' ・ ' + he(c.order_channel) : ''}</div>
      </div>
      <div style="display:flex;gap:6px;flex-wrap:wrap">
        ${badge({ badge: CASE_TYPES[c.case_type]?.badge }, CASE_TYPES[c.case_type]?.label || c.case_type)}
        ${badge({ badge: WAITING_ON[c.waiting_on]?.badge }, WAITING_ON[c.waiting_on]?.label || c.waiting_on)}
        ${c.status !== 'active' ? badge({ badge: 'background:#dcfce7;color:#166534' }, '完了') : ''}
      </div>
    </div>

    <div class="case-facts">
      <div class="f"><div class="k">確認担当</div><div class="v">${he(c.assigned_user_id)}</div></div>
      <div class="f"><div class="k">現在の待ち先</div><div class="v">${he(WAITING_ON[c.waiting_on]?.short || c.waiting_on)}</div></div>
      <div class="f"><div class="k">工程</div><div class="v">${he(STAGES[c.stage]?.label || c.stage)}</div></div>
      <div class="f"><div class="k">次回確認日</div><div class="v">${c.next_action_at ? he(rcJstDate(c.next_action_at)) : '<span class="sub">なし</span>'}
        ${over > 0 ? `<span style="color:#b91c1c"> ${over}日超過</span>` : ''}</div></div>
    </div>

    ${c.status === 'active' ? `
    <div class="next-box ${over > 0 ? 'late' : next ? '' : 'done'}">
      <div class="lbl">次にやること</div>
      <div class="what">${next ? he(stepLabel(c.case_type, next.step_type)) : 'すべての工程が片付きました'}</div>
      <div class="who">${next
        ? `${next.waiting_party ? '待ち先：' + he(next.waiting_party) + ' ／ ' : ''}確認担当：${he(next.assignee_id || c.assigned_user_id)}${next.due_at ? ' ／ 期限 ' + he(rcJstDate(next.due_at)) : ''}`
        : '案件を完了できます'}</div>
    </div>` : `
    <div class="next-box done"><div class="lbl">完了</div>
      <div class="what">${he(rcJstDate(c.closed_at || ''))} に ${he(c.closed_by || '')} が完了にしました</div>
      ${c.close_reason_code ? `<div class="who">例外として完了：${he(CLOSE_REASONS[c.close_reason_code] || c.close_reason_code)}${c.close_note ? ' — ' + he(c.close_note) : ''}</div>` : ''}
    </div>`}

    <div style="display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin-bottom:6px">
      <b style="font-size:15px">対応工程</b>
      <span class="sub">未完了 ${openSteps.length}件 / 全${steps.length}件</span>
      <span style="flex:1"></span>
      <button class="ghost" id="toggleDone">完了済みの工程 ${doneSteps.length}件 を表示</button>
    </div>
    <div class="steps">${openSteps.map(stepRow).join('') || '<div class="step-row"><div></div><div class="nm">残っている工程はありません</div><div></div><div></div><div></div></div>'}</div>
    <div class="steps" id="doneSteps" style="display:none;margin-top:8px">${doneSteps.map(stepRow).join('')}</div>

    <div style="margin-top:18px;display:flex;flex-wrap:wrap;gap:16px">
      <div style="flex:1 1 260px">
        <b style="font-size:14px">💴 返金の記録</b>
        <div class="sub" style="margin-bottom:6px">※返金の正データは各モールです。この画面は実施確認用です</div>
        <div style="display:flex;gap:6px;flex-wrap:wrap;align-items:center">
          <input id="refExp" type="text" inputmode="numeric" placeholder="予定額" value="${c.refund_expected_amount ?? ''}" style="width:100px">
          <input id="refDone" type="text" inputmode="numeric" placeholder="実績額" value="${c.refund_completed_amount ?? ''}" style="width:100px">
          <input id="refRef" type="text" placeholder="モールの処理番号" value="${he(c.refund_external_ref || '')}" style="width:150px">
          <button class="pri" id="saveRefund">記録する</button>
        </div>
      </div>
      <div style="flex:1 1 240px">
        <b style="font-size:14px">⏰ 待ち先と次回確認日</b>
        <div class="sub" style="margin-bottom:6px">外部待ちにするときは次回確認日が必ず入ります</div>
        <div style="display:flex;gap:6px;flex-wrap:wrap;align-items:center">
          <select id="waitOn">${Object.entries(WAITING_ON).filter(([k]) => k !== 'NONE')
            .map(([k, v]) => `<option value="${k}"${k === c.waiting_on ? ' selected' : ''}>${he(v.label)}</option>`).join('')}</select>
          <input id="waitDate" type="date" value="${c.next_action_at ? he(rcJstDate(c.next_action_at)) : ''}">
          <button class="pri" id="saveWaiting">変更する</button>
        </div>
      </div>
    </div>

    <div style="margin-top:18px">
      <b style="font-size:14px">🔗 関連する問い合わせ</b>
      <div class="sub">${inqs.length}件。問い合わせを完了にしても、この案件は残ります</div>
      ${inqs.map(i => `<div style="padding:6px 0;border-top:1px solid #f1f5f9;font-size:13px">
        ${i.link_role === 'origin' ? badge({ badge: 'background:#dbeafe;color:#1d4ed8' }, '元') : ''}
        <a href="/apps/inquiry-hub/inquiries/${i.inquiry_id}">${he(i.subject || '(件名なし)')}</a>
        <span class="sub">${he(STATUSES[i.internal_status]?.label || i.internal_status)}</span>
      </div>`).join('') || '<div class="sub">なし</div>'}
    </div>

    <div style="margin-top:18px" class="case-hist">
      <b style="font-size:14px">対応履歴</b>
      ${events.map(e => `<div class="r"><span class="w">${he(fmtJst(e.created_at))}</span>
        <span>${he(e.actor_id || 'システム')} — ${he(caseEventLabel(e))}</span></div>`).join('')
        || '<div class="sub">履歴はまだありません</div>'}
    </div>

    <div style="margin-top:18px;padding-top:14px;border-top:1px solid #e2e8f0;display:flex;flex-wrap:wrap;gap:10px;align-items:center">
      <span class="sub">${blockers.total > 0
        ? `未処理の工程が ${blockers.total}件 残っています`
        : '必要な工程はすべて片付いています'}${excPerm.unmanaged
        ? ' ／ ⚠️担当者と権限が未登録のため、いまは誰でも例外操作ができます (<a href="/apps/inquiry-hub/staff">担当者と権限</a>で登録してください)'
        : excPerm.error ? ' ／ ⚠️権限を確認できませんでした (例外操作はできません)'
        : excPerm.allowed ? '' : ' ／ 例外として完了する権限はありません'}</span>
      <span style="flex:1"></span>
      ${c.status === 'active'
        ? '<button class="pri" id="closeCase">案件を完了</button>'
        : '<button class="ghost" id="reopenCase">案件を開け直す</button>'}
    </div>
  </div>`;

  const script = `
  async function api(path, data) {
    const r = await fetch('/apps/inquiry-hub/api/cases/${c.id}' + path, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(data || {}) });
    const j = await r.json().catch(function() { return {}; });
    if (!r.ok) { toast(j.error || '失敗しました'); return null; }
    return j;
  }
  document.getElementById('toggleDone').addEventListener('click', function() {
    var el = document.getElementById('doneSteps');
    var open = el.style.display === 'none';
    el.style.display = open ? '' : 'none';
    this.textContent = '完了済みの工程 ${doneSteps.length}件 を' + (open ? '隠す' : '表示');
  });
  document.querySelectorAll('.stp').forEach(function(b) {
    b.addEventListener('click', async function() {
      var payload = { action: b.dataset.act };
      // 必要と決まっている工程を外すときは理由を必ず書かせる (履歴に残る)
      if (b.dataset.act === 'skip_required') {
        var why = prompt('この工程は「必要」と決まっています。外す理由を書いてください (履歴に残ります)\\n'
          + '例: 顧客が返送を希望しないため / 別案件で処理済みのため');
        if (!why || !why.trim()) return;
        payload.reason = why;
      }
      b.disabled = true;
      var r = await api('/steps/' + b.dataset.id, payload);
      if (r) location.reload(); else b.disabled = false;
    });
  });
  document.getElementById('saveRefund').addEventListener('click', async function() {
    var r = await api('/refund', { expected: document.getElementById('refExp').value,
      completed: document.getElementById('refDone').value, ref: document.getElementById('refRef').value });
    if (r) { toast('返金の記録を保存しました'); }
  });
  document.getElementById('saveWaiting').addEventListener('click', async function() {
    var r = await api('/waiting', { waitingOn: document.getElementById('waitOn').value,
      nextActionDate: document.getElementById('waitDate').value });
    if (r) location.reload();
  });
  var closeBtn = document.getElementById('closeCase');
  if (closeBtn) closeBtn.addEventListener('click', async function() {
    var r = await api('/close', {});
    if (!r) return;
    if (r.ok) { location.reload(); return; }
    var names = (r.blockers && r.blockers.steps || []).map(function(s) { return '・' + s.label; }).join('\\n');
    var reason = prompt('この案件はまだ完了できません。未処理の工程が ' + r.blockers.total + '件 残っています。\\n'
      + names + '\\n\\n例外として完了する場合は理由を選んでください:\\n'
      + '1=顧客と連絡が取れない 2=顧客が対応継続を希望しない 3=メーカー回答を得られない\\n'
      + '4=システム外で対応済み 5=誤って作成した案件 6=その他\\n(やめる場合は空欄でOK)');
    if (!reason) return;
    var codes = { '1': 'customer_unreachable', '2': 'customer_declined', '3': 'no_maker_response',
      '4': 'handled_elsewhere', '5': 'created_by_mistake', '6': 'other' };
    var code = codes[reason.trim()];
    if (!code) { toast('番号で選んでください'); return; }
    var note = prompt('未完了の工程を残したまま完了します。理由の詳細を書いてください (履歴に残ります)');
    if (!note || !note.trim()) { toast('詳細メモが必要です'); return; }
    var r2 = await api('/close', { force: true, reasonCode: code, note: note });
    if (r2) location.reload();
  });
  var reopenBtn = document.getElementById('reopenCase');
  if (reopenBtn) reopenBtn.addEventListener('click', async function() {
    var r = await api('/reopen', {});
    if (r) location.reload();
  });`;
  res.send(pageShell(`問い合わせ管理 — ${c.case_no}`, 'cases', body, script));
});

/** 履歴の1行を日本語にする */
function caseEventLabel(e) {
  const to = (() => { try { return JSON.parse(e.to_json || '{}'); } catch { return {}; } })();
  switch (e.event_type) {
    case 'case_created': return `案件を作成した (${CASE_TYPES[to.caseType]?.label || ''})`;
    case 'step_changed': {
      const act = { complete: '完了にした', skip: '対応不要にした', need: '必要にした',
        start: '対応を開始した', wait: '回答・到着待ちにした', undo: '戻した' }[to.action] || to.action;
      return `「${e.note || to.step_type}」を ${act}`;
    }
    case 'waiting_changed': return `待ち先を ${WAITING_ON[to.waiting_on]?.label || to.waiting_on} にした`;
    case 'assignee_changed': return `担当を ${to.assignee} にした`;
    case 'refund_recorded': return `返金を記録した (予定 ${to.expected ?? '—'} / 実績 ${to.completed ?? '—'})`;
    case 'case_closed': return '案件を完了した';
    case 'case_closed_exception': return `例外として完了した (${CLOSE_REASONS[to.reasonCode] || ''}${e.note ? ' — ' + e.note : ''})`;
    case 'case_reopened': return '案件を開け直した';
    case 'inquiry_linked': return '問い合わせを関連付けた';
    case 'inquiry_unlinked': return '問い合わせの関連付けを外した';
    default: return e.event_type;
  }
}

// ── 案件の API ──────────────────────────────────────────

router.post('/api/cases', (req, res) => {
  try {
    const b = req.body || {};
    const r = createCase({ inquiryId: b.inquiryId ? Number(b.inquiryId) : null, caseType: b.caseType,
      nextActionDate: b.nextActionDate, summary: b.summary, allowDuplicate: !!b.allowDuplicate,
      actor: actorOf(req) });
    console.log(`[inquiry-hub] 返品案件 ${r.case_no} を作成 (${b.caseType}) by ${actorOf(req)}`);
    res.json({ ok: true, ...r });
  } catch (e) {
    // ⭐二度押し・再送と「本当に別案件を作りたい」を区別する (409 を返して画面が確認する)
    if (e?.code === 'DUPLICATE_CASE') {
      return res.status(409).json({ error: String(e.message), duplicate: true, caseNo: e.caseNo });
    }
    res.status(400).json({ error: String(e?.message || e).slice(0, 200) });
  }
});

router.post('/api/cases/:id(\\d+)/steps/:stepId(\\d+)', (req, res) => {
  try {
    const b = req.body || {};
    updateStep(Number(req.params.id), Number(req.params.stepId), b.action,
      { note: b.note, externalRef: b.externalRef, reason: b.reason, actor: actorOf(req) });
    res.json({ ok: true });
  } catch (e) {
    const msg = String(e?.message || e).slice(0, 200);
    res.status(msg.includes('権限がありません') ? 403 : 400).json({ error: msg });
  }
});

router.post('/api/cases/:id(\\d+)/waiting', (req, res) => {
  try {
    const b = req.body || {};
    res.json({ ok: true, case: setWaiting(Number(req.params.id), { waitingOn: b.waitingOn,
      nextActionDate: b.nextActionDate, nextActionNote: b.nextActionNote, actor: actorOf(req) }) });
  } catch (e) { res.status(400).json({ error: String(e?.message || e).slice(0, 200) }); }
});

router.post('/api/cases/:id(\\d+)/assignee', (req, res) => {
  try {
    res.json({ ok: true, case: setAssignee(Number(req.params.id), (req.body || {}).assignee, actorOf(req)) });
  } catch (e) { res.status(400).json({ error: String(e?.message || e).slice(0, 200) }); }
});

router.post('/api/cases/:id(\\d+)/refund', (req, res) => {
  try {
    const b = req.body || {};
    res.json({ ok: true, case: setRefund(Number(req.params.id),
      { expected: b.expected, completed: b.completed, ref: b.ref, actor: actorOf(req) }) });
  } catch (e) { res.status(400).json({ error: String(e?.message || e).slice(0, 200) }); }
});

/** 案件を完了する。⭐必要な工程が残っていれば ok:false + 残りを返す (止める) */
router.post('/api/cases/:id(\\d+)/close', (req, res) => {
  try {
    const b = req.body || {};
    const caseId = Number(req.params.id);
    const r = closeCase(caseId, { force: !!b.force, reasonCode: b.reasonCode, note: b.note, actor: actorOf(req) });
    if (!r.ok) {
      const c = getCase(caseId);
      return res.json({ ok: false, blockers: {
        total: r.blockers.total,
        steps: r.blockers.steps.map(s => ({ label: stepLabel(c.case_type, s.step_type),
          state: s.necessity_status === 'undecided' ? '要否を判断' : PROGRESS[s.progress_status]?.label })),
        requests: r.blockers.requests.map(x => ({ label: x.subject || x.request_type, status: x.status })),
      } });
    }
    if (!r.already) console.log(`[inquiry-hub] 返品案件 ${r.case.case_no} を完了${b.force ? ' (例外)' : ''} by ${actorOf(req)}`);
    res.json({ ok: true, case: r.case });
  } catch (e) {
    const msg = String(e?.message || e).slice(0, 200);
    res.status(msg.includes('権限がありません') ? 403 : 400).json({ error: msg });
  }
});

router.post('/api/cases/:id(\\d+)/reopen', (req, res) => {
  try {
    res.json({ ok: true, case: reopenCase(Number(req.params.id), actorOf(req)) });
  } catch (e) { res.status(400).json({ error: String(e?.message || e).slice(0, 200) }); }
});

router.post('/api/cases/:id(\\d+)/link', (req, res) => {
  try {
    linkInquiry(Number(req.params.id), Number((req.body || {}).inquiryId), actorOf(req));
    res.json({ ok: true });
  } catch (e) { res.status(400).json({ error: String(e?.message || e).slice(0, 200) }); }
});

/** 案件化の判断をやり直す (「やっぱり案件にする」) */
router.post('/api/inquiries/:id(\\d+)/triage-reset', (req, res) => {
  try {
    clearTriage(Number(req.params.id));
    res.json({ ok: true });
  } catch (e) { res.status(400).json({ error: String(e?.message || e).slice(0, 200) }); }
});

/** 「今回は案件にしない」= 人の判断だけを保存する (同じ確認を繰り返さないため) */
router.post('/api/inquiries/:id(\\d+)/no-case', (req, res) => {
  try {
    setTriage(Number(req.params.id), 'no_case_needed', actorOf(req));
    res.json({ ok: true });
  } catch (e) { res.status(400).json({ error: String(e?.message || e).slice(0, 200) }); }
});

// ═══════════════════════════════════════════════════════════════
// 👥 担当者と権限マップ (2026-08-28 中原さん要望「権限マップをアプリに入れて自分で登録したい」)
//    ⭐AIに人を選ばせない設計の土台。AIが出すのは「必要な権限」までで、
//      誰に渡すかはこの表を見る決定的なルールが決める (staff.js の冒頭に理由)
// ═══════════════════════════════════════════════════════════════

const PERM_KIND_META = {
  decision: { label: '決裁権限', hint: '顧客に何を約束していいか', style: 'background:#dbeafe;color:#1d4ed8' },
  escalation: { label: 'エスカレーション', hint: '金額の大小で決められない領域。少額でも上位へ上げる', style: 'background:#fee2e2;color:#b91c1c' },
  system: { label: '操作権限', hint: 'どのシステムを触れるか', style: 'background:#dcfce7;color:#166534' },
};
const PERM_KINDS = ['decision', 'escalation', 'system'];

/**
 * 権限マップを編集できる人 (env INQUIRY_HUB_PERMISSION_ADMINS にカンマ区切りで指定)。
 * ⚠️ 未設定なら全員が編集できる — 他の設定画面 (フォルダ・メールルール) と同じ扱いにして、
 *   env を入れ忘れた状態で誰も編集できなくなる事故を避けるため。ただしこの表は
 *   「誰が返金を決めていいか」を決めるものなので、未設定のときは画面に注意を出す。
 */
function canEditPermissions(req) {
  const raw = process.env.INQUIRY_HUB_PERMISSION_ADMINS;
  if (!raw || !raw.trim()) return true;
  const allowed = raw.split(',').map(s => s.trim().toLowerCase()).filter(Boolean);
  if (!allowed.length) return true;
  return allowed.includes(String(actorOf(req) || '').toLowerCase());
}

/** 編集APIの共通ガード。編集できないときは 403 を返す */
function requirePermissionAdmin(req, res) {
  if (canEditPermissions(req)) return true;
  res.status(403).json({ error: '権限マップを変更できるのは管理者だけです (INQUIRY_HUB_PERMISSION_ADMINS)' });
  return false;
}

const STAFF_PAGE_CSS = `<style>
.perm-group { margin:10px 0 }
.perm-group-h { display:flex; align-items:center; gap:8px; margin-bottom:6px; flex-wrap:wrap }
.perm-chk { display:flex; align-items:baseline; gap:6px; padding:4px 6px; border-radius:6px; cursor:pointer }
.perm-chk:hover { background:#f8fafc }
.perm-code { font-family:ui-monospace,monospace; font-size:12px; color:#64748b; min-width:34px }
.staff-card { border:1px solid #e2e8f0; border-radius:10px; padding:14px; margin-bottom:14px; background:#fff }
.staff-card h3 { margin:0 0 4px; font-size:16px }
.staff-fields { display:flex; gap:10px; flex-wrap:wrap; align-items:flex-end }
.staff-fields label { display:flex; flex-direction:column; gap:3px; font-size:12px; color:#475569 }
.staff-fields input { padding:6px 8px; border:1px solid #cbd5e1; border-radius:6px }
.perm-holders { font-size:13px }
.perm-holders td { padding:4px 8px; border-bottom:1px solid #f1f5f9 }
</style>`;

router.get('/staff', (req, res) => {
  const { permissions, staff } = getPermissionMatrix();
  const activePerms = permissions.filter(p => p.is_active);
  const canEdit = canEditPermissions(req);
  const adminsConfigured = !!String(process.env.INQUIRY_HUB_PERMISSION_ADMINS || '').trim();
  const dis = canEdit ? '' : ' disabled';

  const permChecks = s => PERM_KINDS.map(kind => {
    const list = activePerms.filter(p => p.kind === kind);
    if (!list.length) return '';
    const m = PERM_KIND_META[kind];
    return `
      <div class="perm-group">
        <div class="perm-group-h"><span class="badge" style="${m.style}">${m.label}</span>
          <span class="sub">${m.hint}</span></div>
        ${list.map(p => `
          <label class="perm-chk" title="${he(p.description || '')}">
            <input type="checkbox" class="p-chk" data-staff="${s.id}" value="${he(p.code)}"${s.permissions.includes(p.code) ? ' checked' : ''}${dis}>
            <span class="perm-code">${he(p.code)}</span><span>${he(p.name)}</span>
          </label>`).join('')}
      </div>`;
  }).join('');

  const staffCards = staff.map(s => {
    // ⭐上限額を空にしたまま D2 を付けても「無制限」にはならない (0円 = 金額の判断はできない)。
    //   設定漏れが無制限の決裁者を作る fail-open を避けるため。画面では設定を促す
    const rl = refundLimitOf(s);
    return `
    <div class="staff-card" data-staff="${s.id}">
      <h3>${he(s.display_name)}</h3>
      ${rl.needsLimit ? `<div class="sub" style="color:#b91c1c">⚠️ 「少額の補償を決める (D2)」が付いていますが、
        <b>返金の上限額が未設定なので金額の判断はできません</b> (0円扱い)。下の欄に上限額を入れてください。</div>` : ''}
      ${rl.hasD2 && !rl.needsLimit ? `<div class="sub">返金は <b>${rl.limit.toLocaleString('ja-JP')}円</b> まで判断できます</div>` : ''}
      <div class="staff-fields">
        <label>表示名<input type="text" class="s-name" data-id="${s.id}" value="${he(s.display_name)}" maxlength="${STAFF_NAME_MAX}"${dis}></label>
        <label>担当者キー (担当欄に入る値)<input type="text" class="s-key" data-id="${s.id}" value="${he(s.user_key)}" maxlength="${STAFF_KEY_MAX}" style="min-width:220px"${dis}></label>
        <label title="空欄のままだと金額の判断はできません (無制限にはなりません)">返金の上限額 (D2用・空欄=判断できない)<input type="number" class="s-limit" data-id="${s.id}" value="${s.refund_limit_yen == null ? '' : s.refund_limit_yen}" min="0" style="width:150px"${dis}></label>
      </div>
      ${permChecks(s)}
      ${canEdit ? `<div class="ops" style="margin-top:10px">
        <button class="pri s-save" data-id="${s.id}">保存</button>
        <button class="s-off" data-id="${s.id}" data-name="${he(s.display_name)}">この担当者を使わなくする</button>
      </div>` : ''}
    </div>`;
  }).join('');

  // 権限別に「誰が持っているか」の俯瞰 (割り当ての抜けを見つけるため)
  const holderRows = activePerms.map(p => {
    const holders = staff.filter(s => s.permissions.includes(p.code));
    return `<tr>
      <td class="nowrap"><span class="perm-code">${he(p.code)}</span> ${he(p.name)}</td>
      <td${holders.length ? '' : ' style="color:#b91c1c"'}>${holders.length ? holders.map(h => he(h.display_name)).join('、') : '⚠️ 誰も持っていません'}</td>
    </tr>`;
  }).join('');

  const permAdminRows = permissions.map(p => `
    <tr${p.is_active ? '' : ' style="opacity:.55"'}>
      <td class="nowrap"><span class="perm-code">${he(p.code)}</span>${p.is_builtin ? '' : ' <span class="badge" style="background:#f1f5f9;color:#475569">独自</span>'}</td>
      <td>${he(PERM_KIND_META[p.kind]?.label || p.kind)}</td>
      ${p.is_builtin
        // ⚠️既定権限の名前と説明は変えられない — コード側 (ルーティング・AIプロンプト・監査画面) が
        //   D1 や S4 の意味を前提に動くため。補足は「社内メモ」に書く
        ? `<td><b>${he(p.name)}</b><div class="sub">変更できません</div></td>
           <td class="sub">${he(p.description || '')}</td>`
        : `<td><input type="text" class="pm-name" data-code="${he(p.code)}" value="${he(p.name)}" maxlength="${PERM_NAME_MAX}" style="min-width:180px"></td>
           <td><input type="text" class="pm-desc" data-code="${he(p.code)}" value="${he(p.description || '')}" style="min-width:240px"></td>`}
      <td><input type="text" class="pm-note" data-code="${he(p.code)}" value="${he(p.local_note || '')}" placeholder="社内メモ (自由)" style="min-width:200px"></td>
      <td class="nowrap ops">
        <button class="pm-save" data-code="${he(p.code)}">保存</button>
        <button class="pm-act" data-code="${he(p.code)}" data-active="${p.is_active}">${p.is_active ? '無効にする' : '有効に戻す'}</button>
        ${p.is_builtin ? '' : `<button class="pm-del" data-code="${he(p.code)}" data-name="${he(p.name)}">削除</button>`}
      </td>
    </tr>`).join('');

  const logRows = listPermissionLogs(30).map(l => `
    <tr>
      <td class="nowrap">${fmtJst(l.created_at)}</td>
      <td>${he(l.display_name || `(担当者#${l.staff_id})`)}</td>
      <td><span class="perm-code">${he(l.permission_code)}</span> ${he(l.permission_name || '')}</td>
      <td>${l.action === 'grant' ? '<span class="badge" style="background:#dcfce7;color:#166534">付与</span>' : '<span class="badge" style="background:#fee2e2;color:#b91c1c">剥奪</span>'}</td>
      <td>${he(l.actor || '—')}</td>
    </tr>`).join('');

  const body = `${STAFF_PAGE_CSS}
  <div class="view-hint">👥 <b>担当者と権限</b> — 問い合わせを「誰に振るか」ではなく
    「<b>どの権限が要るか</b>」で管理します。AIは必要な権限を見立てるところまでで、人を選ぶのはこの表です。
    担当者が変わってもこの表を直すだけで済み、<b>誰が何を約束していいかが記録として残ります</b>。</div>

  ${canEdit && !adminsConfigured ? `<div class="card" style="padding:10px;background:#fffbeb;border-color:#fcd34d">
    ⚠️ いまは<b>ログインできる人なら誰でもこの表を変更できます</b>。
    変更できる人を限る場合は Render の環境変数 <code>INQUIRY_HUB_PERMISSION_ADMINS</code> に
    メールアドレスをカンマ区切りで設定してください (例: <code>d.nakahara@b-faith.biz</code>)。</div>` : ''}
  ${canEdit ? '' : `<div class="card" style="padding:10px;background:#f1f5f9">
    🔒 閲覧のみです。この表を変更できるのは管理者だけです。</div>`}

  ${canEdit ? `<div class="filters">
    <input type="text" id="newName" placeholder="表示名 (例: 田中)" maxlength="${STAFF_NAME_MAX}" style="min-width:160px">
    <input type="text" id="newKey" placeholder="担当者キー (例: tanaka@b-faith.biz)" maxlength="${STAFF_KEY_MAX}" style="min-width:260px">
    <button class="pri" id="createBtn">➕ 担当者を追加</button>
  </div>` : ''}

  ${staffCards || '<div class="card"><div class="empty">担当者はまだ登録されていません。上の欄から追加してください</div></div>'}

  <details class="card" style="padding:12px">
    <summary><b>権限を誰が持っているか (俯瞰)</b></summary>
    <table class="perm-holders" style="width:100%;margin-top:10px">
      <tbody>${holderRows}</tbody>
    </table>
  </details>

  ${canEdit ? `<details class="card" style="padding:12px">
    <summary><b>権限の名前・説明を編集する / 独自の権限を足す</b></summary>
    <div class="sub" style="margin:8px 0">既定の19件は削除できません (トリアージのルーティングがこのコードを参照するため)。
      使わない権限は「無効にする」で画面から隠せます。</div>
    <div class="filters">
      <input type="text" id="npCode" placeholder="コード (英数字。例 S9)" maxlength="${PERM_CODE_MAX}" style="width:150px">
      <select id="npKind">
        <option value="decision">決裁権限</option>
        <option value="escalation">エスカレーション</option>
        <option value="system">操作権限</option>
      </select>
      <input type="text" id="npName" placeholder="権限名" maxlength="${PERM_NAME_MAX}" style="min-width:200px">
      <button class="pri" id="npAdd">➕ 権限を足す</button>
    </div>
    <table class="cardable" style="margin-top:10px">
      <thead><tr><th>コード</th><th>種別</th><th>名前</th><th>説明</th><th>社内メモ</th><th>操作</th></tr></thead>
      <tbody>${permAdminRows}</tbody>
    </table>
  </details>` : ''}

  <details class="card" style="padding:12px">
    <summary><b>権限の変更履歴 (直近30件)</b></summary>
    <table class="cardable" style="margin-top:10px">
      <thead><tr><th>日時</th><th>担当者</th><th>権限</th><th>操作</th><th>実行者</th></tr></thead>
      <tbody>${logRows || '<tr><td colspan="5" class="empty">まだ変更はありません</td></tr>'}</tbody>
    </table>
  </details>`;

  const script = `
  function api(path, data) {
    return fetch('/apps/inquiry-hub/api' + path, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data || {})
    }).then(function(r) { return r.json().catch(function(){ return {}; }).then(function(j){ if (!r.ok) throw new Error(j.error || ('HTTP ' + r.status)); return j; }); });
  }
  // 閲覧のみのときは追加フォーム自体が無い (要素が無くてもエラーにしない)
  var createBtn = document.getElementById('createBtn');
  if (createBtn) createBtn.addEventListener('click', function() {
    var name = document.getElementById('newName').value.trim();
    var key = document.getElementById('newKey').value.trim();
    if (!name || !key) { toast('表示名と担当者キーの両方を入れてください'); return; }
    var btn = this; btn.disabled = true;
    api('/staff', { displayName: name, userKey: key }).then(function() { location.reload(); })
      .catch(function(e) { toast('追加に失敗: ' + e.message); btn.disabled = false; });
  });
  document.querySelectorAll('.s-save').forEach(function(b) {
    b.addEventListener('click', function() {
      var id = b.dataset.id;
      var codes = [];
      document.querySelectorAll('.p-chk[data-staff="' + id + '"]').forEach(function(c) { if (c.checked) codes.push(c.value); });
      var limit = document.querySelector('.s-limit[data-id="' + id + '"]').value.trim();
      b.disabled = true;
      api('/staff/' + id, {
        displayName: document.querySelector('.s-name[data-id="' + id + '"]').value.trim(),
        userKey: document.querySelector('.s-key[data-id="' + id + '"]').value.trim(),
        refundLimitYen: limit === '' ? null : limit,
        permissions: codes
      }).then(function(r) {
        var msg = '保存しました';
        if (r.permissions) {
          var g = (r.permissions.granted || []).length, v = (r.permissions.revoked || []).length;
          if (g || v) msg += ' (権限 +' + g + ' / -' + v + ')';
        }
        toast(msg);
        setTimeout(function(){ location.reload(); }, 700);
      }).catch(function(e) { toast('保存に失敗: ' + e.message); b.disabled = false; });
    });
  });
  document.querySelectorAll('.s-off').forEach(function(b) {
    b.addEventListener('click', function() {
      if (!confirm(b.dataset.name + ' さんを使わなくしますか?' + String.fromCharCode(10, 10)
        + '権限はすべて外れます。過去の問い合わせに残っている担当の記録は消えません。')) return;
      b.disabled = true;
      api('/staff/' + b.dataset.id + '/deactivate', {}).then(function() { location.reload(); })
        .catch(function(e) { toast('失敗: ' + e.message); b.disabled = false; });
    });
  });
  var npAdd = document.getElementById('npAdd');
  if (npAdd) npAdd.addEventListener('click', function() {
    var code = document.getElementById('npCode').value.trim();
    var name = document.getElementById('npName').value.trim();
    if (!code || !name) { toast('コードと権限名を入れてください'); return; }
    var btn = this; btn.disabled = true;
    api('/permissions', { code: code, kind: document.getElementById('npKind').value, name: name })
      .then(function() { location.reload(); })
      .catch(function(e) { toast('追加に失敗: ' + e.message); btn.disabled = false; });
  });
  document.querySelectorAll('.pm-save').forEach(function(b) {
    b.addEventListener('click', function() {
      var c = b.dataset.code; b.disabled = true;
      // 既定権限は名前・説明の入力欄が無い (社内メモだけ送る)
      var nameEl = document.querySelector('.pm-name[data-code="' + c + '"]');
      var descEl = document.querySelector('.pm-desc[data-code="' + c + '"]');
      var payload = { localNote: document.querySelector('.pm-note[data-code="' + c + '"]').value.trim() };
      if (nameEl) { payload.name = nameEl.value.trim(); payload.description = descEl.value.trim(); }
      api('/permissions/' + encodeURIComponent(c), payload)
        .then(function() { toast('保存しました'); b.disabled = false; })
        .catch(function(e) { toast('保存に失敗: ' + e.message); b.disabled = false; });
    });
  });
  document.querySelectorAll('.pm-act').forEach(function(b) {
    b.addEventListener('click', function() {
      var on = b.dataset.active === '1'; b.disabled = true;
      api('/permissions/' + encodeURIComponent(b.dataset.code) + '/active', { active: !on })
        .then(function() { location.reload(); })
        .catch(function(e) { toast('失敗: ' + e.message); b.disabled = false; });
    });
  });
  document.querySelectorAll('.pm-del').forEach(function(b) {
    b.addEventListener('click', function() {
      if (!confirm('権限「' + b.dataset.name + '」を削除しますか?' + String.fromCharCode(10)
        + '付与済みの担当者からも外れます。')) return;
      b.disabled = true;
      api('/permissions/' + encodeURIComponent(b.dataset.code) + '/delete', {})
        .then(function() { location.reload(); })
        .catch(function(e) { toast('削除に失敗: ' + e.message); b.disabled = false; });
    });
  });`;
  res.send(pageShell('問い合わせ管理 — 担当者と権限', 'staff', body, script));
});

router.post('/api/staff', (req, res) => {
  if (!requirePermissionAdmin(req, res)) return;
  try {
    const b = req.body || {};
    const s = createStaff({ userKey: b.userKey, displayName: b.displayName,
      refundLimitYen: b.refundLimitYen, note: b.note }, actorOf(req));
    console.log(`[inquiry-hub] 担当者追加「${s.displayName}」(${s.userKey}) by ${actorOf(req)}`);
    res.json({ ok: true, ...s });
  } catch (e) { res.status(400).json({ error: String(e?.message || e).slice(0, 200) }); }
});

router.post('/api/staff/:id(\\d+)', (req, res) => {
  if (!requirePermissionAdmin(req, res)) return;
  try {
    const b = req.body || {};
    const r = saveStaffWithPermissions(Number(req.params.id), {
      userKey: b.userKey, displayName: b.displayName, refundLimitYen: b.refundLimitYen,
      note: b.note, sortOrder: b.sortOrder,
      permissions: Array.isArray(b.permissions) ? b.permissions : undefined,
    }, actorOf(req));
    if (r.permissions && (r.permissions.granted.length || r.permissions.revoked.length)) {
      console.log(`[inquiry-hub] 権限変更 ${r.displayName}: +[${r.permissions.granted.join(',')}] -[${r.permissions.revoked.join(',')}] by ${actorOf(req)}`);
    }
    res.json({ ok: true, ...r });
  } catch (e) { res.status(400).json({ error: String(e?.message || e).slice(0, 200) }); }
});

router.post('/api/staff/:id(\\d+)/deactivate', (req, res) => {
  if (!requirePermissionAdmin(req, res)) return;
  try {
    const r = deactivateStaff(Number(req.params.id), actorOf(req));
    console.log(`[inquiry-hub] 担当者を無効化「${r.displayName}」 権限${r.revoked}件を解除 by ${actorOf(req)}`);
    res.json({ ok: true, ...r });
  } catch (e) { res.status(400).json({ error: String(e?.message || e).slice(0, 200) }); }
});

router.post('/api/permissions', (req, res) => {
  if (!requirePermissionAdmin(req, res)) return;
  try {
    const b = req.body || {};
    const r = createPermission({ code: b.code, kind: b.kind, name: b.name, description: b.description });
    console.log(`[inquiry-hub] 権限を追加「${r.code} ${r.name}」 by ${actorOf(req)}`);
    res.json({ ok: true, ...r });
  } catch (e) { res.status(400).json({ error: String(e?.message || e).slice(0, 200) }); }
});

router.post('/api/permissions/:code([A-Za-z0-9_]+)', (req, res) => {
  if (!requirePermissionAdmin(req, res)) return;
  try {
    const b = req.body || {};
    res.json({ ok: true, ...updatePermission(req.params.code,
      { name: b.name, description: b.description, localNote: b.localNote, sortOrder: b.sortOrder }) });
  } catch (e) { res.status(400).json({ error: String(e?.message || e).slice(0, 200) }); }
});

router.post('/api/permissions/:code([A-Za-z0-9_]+)/active', (req, res) => {
  if (!requirePermissionAdmin(req, res)) return;
  try {
    const active = (req.body || {}).active;
    if (typeof active !== 'boolean') return res.status(400).json({ error: 'active は true/false で指定してください' });
    res.json({ ok: true, ...setPermissionActive(req.params.code, active) });
  } catch (e) { res.status(400).json({ error: String(e?.message || e).slice(0, 200) }); }
});

router.post('/api/permissions/:code([A-Za-z0-9_]+)/delete', (req, res) => {
  if (!requirePermissionAdmin(req, res)) return;
  try {
    const r = deletePermission(req.params.code, actorOf(req));
    console.log(`[inquiry-hub] 権限を削除「${r.code} ${r.name}」 by ${actorOf(req)}`);
    res.json({ ok: true, ...r });
  } catch (e) { res.status(400).json({ error: String(e?.message || e).slice(0, 200) }); }
});

// ═══════════════════════════════════════════════════════════════
// ⚙️ 運用管理画面 (設計書§7.1 #6 同期・エラー管理 + #4 送信結果不明の解決)
// ═══════════════════════════════════════════════════════════════

const OUTBOX_STATUS_LABELS = {
  unknown: { label: '❓結果不明', style: 'background:#fef3c7;color:#92400e' },
  needs_review: { label: '⏸️要確認 (新着競合)', style: 'background:#e0e7ff;color:#3730a3' },
  failed: { label: '❌送信失敗', style: 'background:#fee2e2;color:#b91c1c' },
  pending: { label: '⏳送信待ち', style: 'background:#f1f5f9;color:#475569' },
  sending: { label: '📤送信中', style: 'background:#dbeafe;color:#1d4ed8' },
  sent: { label: '✅送信済み', style: 'background:#dcfce7;color:#166534' },
  cancelled: { label: '🚫取消済み', style: 'background:#f1f5f9;color:#64748b' },
};

router.get('/admin', (req, res) => {
  const db = getDB();
  const syncRows = listSyncStatus();
  const nowMs = Date.now();

  const authBadge = (s) => {
    const warnDays = s.auth_expires_at ? Math.floor((Date.parse(s.auth_expires_at) - nowMs) / 86400000) : null;
    if (s.authentication_status !== 'ok') return `<span class="badge" style="background:#fee2e2;color:#b91c1c">認証: ${he(s.authentication_status)}</span>`;
    if (warnDays != null && warnDays <= 30) return `<span class="badge" style="background:#fef3c7;color:#92400e">認証期限 残${warnDays}日</span>`;
    return `<span class="badge" style="background:#dcfce7;color:#166534">認証OK</span>`;
  };
  const syncTrs = syncRows.map(s => {
    const failing = (s.consecutive_failures || 0) >= 3;
    const syncing = s.lease_until && Date.parse(s.lease_until) > nowMs;
    // 期限の手動登録フォーム (Yahoo!は同期時に自動更新されるため表示のみ)
    const expiryDateVal = s.auth_expires_at ? new Date(Date.parse(s.auth_expires_at) + 9 * 3600000).toISOString().slice(0, 10) : '';
    const expiryEdit = s.channel_type === 'yahoo'
      ? '<div class="sub">(再認可時に自動更新)</div>'
      : `<div class="sub expiry-edit">
           <input type="date" id="exp-${s.shop_id}" value="${he(expiryDateVal)}">
           <button onclick="saveAuthExpiry(${s.shop_id}, this)">保存</button>
         </div>`;
    return `
    <tr${failing ? ' style="background:#fef2f2"' : ''}>
      <td data-full>${chBadge(s.channel_type)}<div class="sub">${he(s.shop_name)}</div></td>
      <td data-full data-label="認証">${authBadge(s)}${s.auth_expires_at ? `<div class="sub">期限 ${fmtJst(s.auth_expires_at)}</div>` : ''}${expiryEdit}</td>
      <td class="nowrap" data-label="最終同期">${fmtJst(s.last_synced_at)}${syncing ? ' <span class="badge" style="background:#dbeafe;color:#1d4ed8">同期中…</span>' : ''}</td>
      <td class="nowrap" data-label="取り込み済み">${fmtJst(s.committed_until)}</td>
      <td data-full data-label="連続失敗"${failing || (s.consecutive_failures || 0) > 0 || s.last_error ? '' : ' data-empty'}>${failing ? `<span class="badge" style="background:#fee2e2;color:#b91c1c">連続失敗 ${s.consecutive_failures}回</span>` : (s.consecutive_failures || 0) > 0 ? `${s.consecutive_failures}回` : '—'}
        ${s.last_error ? `<div class="sub" title="${he(s.last_error)}">${he(String(s.last_error).slice(0, 80))}</div>` : ''}</td>
      <td data-label="未解決エラー"${s.open_errors > 0 ? '' : ' data-empty'}>${s.open_errors > 0 ? `<span class="badge" style="background:#fef3c7;color:#92400e">${s.open_errors}件</span>` : '—'}</td>
      <td class="nowrap ops">
        <button onclick="event.stopPropagation(); manualSync(${s.shop_id}, false, this)">▶ 今すぐ同期</button>
        <button onclick="event.stopPropagation(); manualSync(${s.shop_id}, true, this)" title="365日分を再照合 (数分かかることがあります)">🔎 deep</button>
      </td>
    </tr>`;
  }).join('');

  const errRows = db.prepare(`SELECT e.*, s.shop_name, s.channel_type FROM sync_errors e
    LEFT JOIN shops s ON s.id = e.shop_id
    WHERE e.resolved = 0 ORDER BY e.id DESC LIMIT 20`).all();
  const errTrs = errRows.map(e => `
    <tr>
      <td class="nowrap" data-label="発生">${fmtJst(e.created_at)}</td>
      <td data-label="店舗">${e.channel_type ? chBadge(e.channel_type) : ''} ${he(e.shop_name || '')}</td>
      <td data-label="種別"><span class="badge" style="background:#f1f5f9;color:#475569">${he(e.error_type)}</span></td>
      <td style="overflow-wrap:anywhere" data-full data-label="内容">${he(String(e.error_detail || '').slice(0, 300))}</td>
      <td class="nowrap ops"><button onclick="resolveSyncError(${e.id}, this)">解決済みにする</button></td>
    </tr>`).join('');

  const issues = listOutboxIssues();
  const issueTrs = issues.map(o => {
    const meta = OUTBOX_STATUS_LABELS[o.status] || { label: o.status, style: '' };
    const ops = o.status === 'unknown'
      ? `<button class="pri" onclick="resolveOutbox(${o.id}, 'confirmed_sent', this)">✅送信済みだった</button>
         <button onclick="resolveOutbox(${o.id}, 'confirmed_not_sent', this)">↩️未送信だった</button>
         <button onclick="resolveOutbox(${o.id}, 'abandoned', this)">🚫対応断念</button>`
      : (o.status === 'needs_review' || o.status === 'pending')
        ? `<button onclick="cancelOutbox(${o.id}, this)">🚫取消</button>`
        : '<span class="sub">再送は詳細画面から新しい返信として (Step 3)</span>';
    return `
    <tr>
      <td><span class="badge" style="${meta.style}">${he(meta.label)}</span><div class="sub">${fmtJst(o.created_at)}</div></td>
      <td class="chcell">${chBadge(o.inquiry_channel)}<div class="sub">${he(o.shop_name)}</div></td>
      <td data-full><a href="/apps/inquiry-hub/inquiries/${o.inquiry_id}">${he(o.subject || '(件名なし)')}</a>
        <div class="sub">${he(o.customer_name || '')} ・ 作成: ${he(o.created_by || '—')}</div></td>
      <td style="overflow-wrap:anywhere" data-full data-label="本文 / エラー"><div class="sub">${he(String(o.body_text || '').slice(0, 120))}${String(o.body_text || '').length > 120 ? '…' : ''}</div>
        ${o.error_detail ? `<div class="sub" style="color:#b91c1c">${he(String(o.error_detail).slice(0, 150))}</div>` : ''}</td>
      <td class="nowrap ops">${ops}</td>
    </tr>`;
  }).join('');

  // AIバッチ状況 (§9.2: ai_runs を管理画面に表示)
  const aiStats = db.prepare(`SELECT
      (SELECT COUNT(*) FROM ai_jobs WHERE status = 'queued') AS queued,
      (SELECT COUNT(*) FROM ai_jobs WHERE status = 'processing') AS processing,
      (SELECT COUNT(*) FROM ai_drafts WHERE is_stale = 0) AS drafts`).get();
  const aiRuns = db.prepare('SELECT * FROM ai_runs ORDER BY id DESC LIMIT 5').all();
  const aiRunTrs = aiRuns.map(r => `
    <tr>
      <td class="nowrap" data-label="実行">${fmtJst(r.started_at)}</td>
      <td data-label="ランナー">${he(r.runner_info || '—')}</td>
      <td data-full data-label="結果">claim ${r.claimed} / 生成 ${r.done} / 破棄 ${r.discarded} / 失敗 ${r.failed}</td>
      <td>${r.error ? `<span class="badge" style="background:#fee2e2;color:#b91c1c">${he(String(r.error).slice(0, 60))}</span>` : '<span class="badge" style="background:#dcfce7;color:#166534">OK</span>'}</td>
    </tr>`).join('');
  const aiCard = `
  <div class="card" style="margin-bottom:16px">
    <div class="card-title">🤖 AI返信案 <span class="sub">(待機 ${aiStats.queued}件 / 生成中 ${aiStats.processing}件 / 有効な返信案 ${aiStats.drafts}件。ローカルランナーが定時に生成)</span></div>
    ${aiRuns.length ? `<table class="cardable"><thead><tr><th>実行</th><th>ランナー</th><th>結果</th><th></th></tr></thead><tbody>${aiRunTrs}</tbody></table>`
      : '<div class="empty">まだAIバッチが実行されていません (ai-draft-runner.mjs を定時実行すると履歴が出ます)</div>'}
  </div>`;

  // 一括操作の履歴 (2026-08-25 安全装置: バッチ単位の取り消し)
  const bulkBatches = listBulkBatches({ limit: 20 });
  const folderNames = Object.fromEntries(listFolders({ includeInactive: true }).map(f => [f.id, f.name]));
  const labelNames = Object.fromEntries(listLabels({ includeInactive: true }).map(l => [l.id, l.name]));
  const batchOpsSummary = (b) => {
    let o = {};
    try { o = JSON.parse(b.ops_json) || {}; } catch { /* 表示用なので握りつぶす */ }
    const parts = [];
    if (o.status) parts.push(`状態→${(STATUSES[o.status] || {}).label || o.status}`);
    if (o.action === 'skip' || o.action === 'import_done') parts.push('完了扱い');
    if (o.folderId !== undefined && o.folderId !== null) parts.push(`📁${folderNames[o.folderId] || `#${o.folderId}`}へ`);
    else if (o.folderId === null && 'folderId' in o && !o.action) parts.push('フォルダ解除');
    if (o.labelId !== undefined && o.labelId !== null) parts.push(`🏷️${labelNames[o.labelId] || `#${o.labelId}`}`);
    else if (o.labelId === null && 'labelId' in o && !o.action) parts.push('ラベル解除');
    if (o.assigned !== undefined && o.assigned !== null) parts.push(`担当→${o.assigned || '未割当'}`);
    if (typeof o.isUnread === 'boolean') parts.push(o.isUnread ? '未読にする' : '既読にする');
    return parts.join(' / ') || '—';
  };
  const batchTrs = bulkBatches.map(b => `
    <tr${b.reverted_at ? ' style="opacity:.55"' : ''}>
      <td class="nowrap" data-label="実行">${fmtJst(b.created_at)}<div class="sub">#${b.id} ${he(b.actor || '—')}</div></td>
      <td data-label="種別"><span class="badge" style="background:#f1f5f9;color:#475569">${he(BATCH_SOURCE_LABELS[b.source] || b.source)}</span></td>
      <td data-full data-label="内容">${he(batchOpsSummary(b))}</td>
      <td class="nowrap" data-label="件数">${b.changed_count}件変更<div class="sub">対象${b.target_count}件</div></td>
      <td class="nowrap ops">${b.reverted_at
        ? `<span class="sub">↩️取り消し済み (${fmtJst(b.reverted_at)})</span>`
        : `<button onclick="revertBatch(${b.id}, ${b.changed_count}, this)">↩️ このバッチを取り消す</button>`}</td>
    </tr>`).join('');
  const batchCard = `
  <div class="card" style="margin-bottom:16px">
    <div class="card-title">🧺 一括操作の履歴 <span class="sub">(直近20件。誤った一括変更はバッチ単位で取り消せます — 後から手で変えた分は上書きしません)</span></div>
    ${bulkBatches.length ? `<table class="cardable"><thead><tr><th>実行</th><th>種別</th><th>内容</th><th>件数</th><th>操作</th></tr></thead><tbody>${batchTrs}</tbody></table>`
      : '<div class="empty">まだ一括操作は実行されていません</div>'}
  </div>`;

  const body = `
  <div class="filters">
    <a class="ghost btn-link" href="/apps/inquiry-hub/admin/insights">📊 実データ調査 (件数・締めまでの処理量・AI費用の試算)</a>
    <a class="ghost btn-link" href="/apps/inquiry-hub/staff">👥 担当者と権限</a>
  </div>
  ${aiCard}
  ${batchCard}
  <div class="card" style="margin-bottom:16px">
    <div class="card-title">🔄 受信同期の状態 <span class="sub">(cron: 15分間隔 + 深掘り 毎朝5:37。手動実行してもcronと衝突しません)</span></div>
    <table class="cardable">
      <thead><tr><th>チャネル/店舗</th><th>認証</th><th>最終同期</th><th>取り込み済み時刻</th><th>連続失敗</th><th>未解決エラー</th><th>手動同期</th></tr></thead>
      <tbody>${syncTrs || '<tr><td colspan="7" class="empty">アクティブな店舗がありません</td></tr>'}</tbody>
    </table>
  </div>
  ${errRows.length ? `
  <div class="card" style="margin-bottom:16px">
    <div class="card-title">⚠️ 未解決の同期エラー (直近20件)</div>
    <table class="cardable">
      <thead><tr><th>発生</th><th>店舗</th><th>種別</th><th>内容</th><th></th></tr></thead>
      <tbody>${errTrs}</tbody>
    </table>
  </div>` : ''}
  <div class="card">
    <div class="card-title">📮 送信の要対応 <span class="sub">(結果不明は自動再送しません。モール管理画面で実際の送信有無を確認してから選択してください)</span></div>
    <table class="cardable">
      <thead><tr><th>状態</th><th>チャネル/店舗</th><th>問い合わせ</th><th>本文/エラー</th><th>操作</th></tr></thead>
      <tbody>${issueTrs || '<tr><td colspan="5" class="empty">要対応の送信ジョブはありません</td></tr>'}</tbody>
    </table>
  </div>`;

  const script = `
async function post(url, data) {
  const r = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data || {}) });
  const j = await r.json().catch(function(){ return {}; });
  if (!r.ok) throw new Error(j.error || ('HTTP ' + r.status));
  return j;
}
async function revertBatch(id, changed, btn) {
  if (!confirm('バッチ #' + id + ' (' + changed + '件変更) を取り消しますか?\\n\\n'
    + '変更したフィールドだけを元の値に戻します。\\nバッチの後に手で変えた分は上書きしません。\\nこの操作は1回だけ実行できます。')) return;
  btn.disabled = true;
  try {
    const r = await post('/apps/inquiry-hub/api/bulk-batches/' + id + '/revert', {});
    toast(r.alreadyReverted ? '既に取り消し済みです'
      : r.reverted + '件を元に戻しました' + (r.skipped ? ' (' + r.skipped + '件は後から変更されていたため対象外)' : ''));
    setTimeout(function(){ location.reload(); }, 1200);
  } catch (e) { toast('取り消し失敗: ' + e.message); btn.disabled = false; }
}
async function manualSync(shopId, deep, btn) {
  btn.disabled = true; var old = btn.textContent; btn.textContent = '同期中…';
  try {
    var r = await post('/apps/inquiry-hub/api/admin/sync/' + shopId, { deep: deep });
    if (r.skipped === 'lease') toast('別の同期が実行中です。少し待ってから再実行してください');
    else if (r.ok) { toast('同期完了: 新規' + r.stats.newInquiries + '件 / 新着メッセージ' + r.stats.newMessages + '件'); setTimeout(function(){ location.reload(); }, 1200); }
    else toast('同期失敗: ' + (r.error || '不明なエラー'));
  } catch (e) { toast('同期失敗: ' + e.message); }
  btn.disabled = false; btn.textContent = old;
}
async function resolveOutbox(id, resolution, btn) {
  var confirms = {
    confirmed_sent: 'モール/メールの管理画面で「実際に送信されている」ことを確認しましたか?\\n(会話履歴に送信済みとして記録されます)',
    confirmed_not_sent: 'モール/メールの管理画面で「送信されていない」ことを確認しましたか?\\n(未送信=失敗として確定し、再送できるようになります)',
    abandoned: '対応を断念しますか? この操作は取り消せず、再送ボタンも出なくなります',
  };
  if (!confirm(confirms[resolution])) return;
  btn.disabled = true;
  try { await post('/apps/inquiry-hub/api/admin/outbox/' + id + '/resolve', { resolution: resolution }); toast('解決を記録しました'); setTimeout(function(){ location.reload(); }, 800); }
  catch (e) { toast('失敗: ' + e.message); btn.disabled = false; }
}
async function cancelOutbox(id, btn) {
  if (!confirm('この送信ジョブを取消しますか? (送り直す場合は詳細画面から新しい返信を作成します)')) return;
  btn.disabled = true;
  try { await post('/apps/inquiry-hub/api/admin/outbox/' + id + '/cancel', {}); toast('取消しました'); setTimeout(function(){ location.reload(); }, 800); }
  catch (e) { toast('失敗: ' + e.message); btn.disabled = false; }
}
async function resolveSyncError(id, btn) {
  btn.disabled = true;
  try { await post('/apps/inquiry-hub/api/admin/sync-errors/' + id + '/resolve', {}); toast('解決済みにしました'); setTimeout(function(){ location.reload(); }, 800); }
  catch (e) { toast('失敗: ' + e.message); btn.disabled = false; }
}
async function saveAuthExpiry(shopId, btn) {
  var date = document.getElementById('exp-' + shopId).value;
  btn.disabled = true;
  try { await post('/apps/inquiry-hub/api/admin/shops/' + shopId + '/auth-expiry', { date: date }); toast(date ? '認証期限を保存しました' : '認証期限を解除しました'); setTimeout(function(){ location.reload(); }, 800); }
  catch (e) { toast('失敗: ' + e.message); btn.disabled = false; }
}`;
  res.send(pageShell('問い合わせ管理 — 運用管理', 'admin', body, script));
});

// 手動同期 (運用管理画面の▶ボタン)。cronとの多重起動はエンジンのリースが防ぐ (skipped:'lease')
router.post('/api/admin/sync/:shopId(\\d+)', async (req, res) => {
  const db = getDB();
  const shop = db.prepare("SELECT * FROM shops WHERE id = ? AND is_active = 1 AND executor = 'server'").get(Number(req.params.shopId));
  if (!shop) return res.status(404).json({ error: '店舗が見つかりません' });
  const deep = !!(req.body || {}).deep;
  const adapter = buildAdapterForShop(shop, { deep });
  if (!adapter) return res.status(503).json({ error: `${shop.channel_type} の同期用環境変数が未設定です` });
  try {
    console.log(`[inquiry-hub] 手動同期 ${shop.shop_name}${deep ? ' (deep)' : ''} by ${actorOf(req)}`);
    // deep のときは直近数日の強制再照合 (repair) も併せて行う。取り込み済みスレッドに後から
    // 付いた情報 (配信失敗通知の検知など) を拾い直すための入口 (2026-08-26)
    const r = await runSync(shop.id, adapter, { repair: deep });
    await refreshShopAuthStatus(shop, adapter); // Yahoo!は認証期限も自動反映 (fail-soft)
    res.json(r);
  } catch (e) {
    res.status(500).json({ error: String(e?.message || e).slice(0, 300) });
  }
});

router.post('/api/admin/outbox/:id(\\d+)/resolve', (req, res) => {
  const resolution = String((req.body || {}).resolution || '');
  if (!['confirmed_sent', 'confirmed_not_sent', 'abandoned'].includes(resolution)) {
    return res.status(400).json({ error: '不正な resolution です' });
  }
  try {
    res.json({ ok: true, ...resolveUnknown(Number(req.params.id), resolution, actorOf(req)) });
  } catch (e) {
    res.status(409).json({ error: String(e?.message || e).slice(0, 300) });
  }
});

router.post('/api/admin/outbox/:id(\\d+)/cancel', (req, res) => {
  try {
    res.json({ ok: true, ...cancelJob(Number(req.params.id), actorOf(req)) });
  } catch (e) {
    res.status(409).json({ error: String(e?.message || e).slice(0, 300) });
  }
});

// 認証期限の手動登録 (楽天licenseKey等、APIから取得できないもの用。Yahoo!は同期時に自動更新される)
router.post('/api/admin/shops/:id(\\d+)/auth-expiry', (req, res) => {
  const db = getDB();
  const shop = db.prepare('SELECT * FROM shops WHERE id = ?').get(Number(req.params.id));
  if (!shop) return res.status(404).json({ error: '店舗が見つかりません' });
  const date = String((req.body || {}).date || '').trim();
  if (date && (!/^\d{4}-\d{2}-\d{2}$/.test(date) || Number.isNaN(Date.parse(date)))) {
    return res.status(400).json({ error: '日付は YYYY-MM-DD 形式の実在する日付で指定してください (空で解除)' });
  }
  // 日付のみの入力はJSTの当日終わりまで有効とみなす (UTC 15:00 = JST 24:00)
  const iso = date ? `${date}T15:00:00Z` : null;
  db.prepare(`UPDATE shops SET auth_expires_at = ?, updated_at = ${NOW_SQL} WHERE id = ?`).run(iso, shop.id);
  console.log(`[inquiry-hub] 認証期限を${date ? date + 'に設定' : '解除'}: ${shop.shop_name} by ${actorOf(req)}`);
  res.json({ ok: true, auth_expires_at: iso });
});

router.post('/api/admin/sync-errors/:id(\\d+)/resolve', (req, res) => {
  const r = getDB().prepare(`UPDATE sync_errors SET resolved = 1, resolved_at = ${NOW_SQL} WHERE id = ? AND resolved = 0`)
    .run(Number(req.params.id));
  if (!r.changes) return res.status(404).json({ error: '対象のエラーがありません (解決済みの可能性)' });
  res.json({ ok: true });
});

// ═══════════════════════════════════════════════════════════════
// 📊 実データ調査 (2026-08-28) — AIトリアージの設計・費用試算を推測でなく実測で決めるための画面
//    ロジザードの締めは 09:00 / 12:30 / 14:30 の3回。キャンセル・住所変更は
//    「1日に何件か」より「1回の締めまでに何件さばく必要があるか」で見る
// ═══════════════════════════════════════════════════════════════

router.get('/admin/insights', (req, res) => {
  const days = Math.min(365, Math.max(7, Number(req.query.days) || 30));
  const d = collectInsights({ days });
  // collectInsights は各項目を { data, error } で返す (失敗値を成功として扱わないため)
  const rowsOf = v => (v && Array.isArray(v.data) ? v.data : []);
  const dataOf = v => (v && v.error ? null : (v && v.data) || null);
  const errOf = v => (v && v.error) ? `<div class="sub" style="color:#b91c1c">取得できませんでした: ${he(v.error)}</div>` : '';

  const tbl = (head, rows, cells) => `
    <table class="cardable"><thead><tr>${head.map(h => `<th>${h}</th>`).join('')}</tr></thead>
    <tbody>${rows.length ? rows.map(cells).join('') : `<tr><td colspan="${head.length}" class="empty">データがありません</td></tr>`}</tbody></table>`;

  // 顧客受信の1日あたり件数 (費用試算の母数)
  const chRows = rowsOf(d.byChannel);
  const perDayTotal = chRows.reduce((a, r) => a + Number(r.per_day || 0), 0);
  const bodyLen = dataOf(d.bodyLength);
  const totals = dataOf(d.totals);
  const assign = dataOf(d.assignment);
  const cats = dataOf(d.categories);
  const ai = dataOf(d.aiStatus);
  const avgChars = (bodyLen && bodyLen.avg_chars) || 0;
  const cost = estimateCost({ avgChars, perDay: perDayTotal });

  // 締め窓ごとの件数 — この画面の主役
  const cutRows = rowsOf(d.cutoffWindows);
  const cutMax = Math.max(1, ...cutRows.map(r => Number(r.count) || 0));
  const cutoffCards = cutRows.map(r => `
    <div style="display:flex;align-items:center;gap:10px;margin:6px 0;flex-wrap:wrap">
      <div style="min-width:290px">${he(r.label)}</div>
      <div style="flex:1;min-width:120px;background:#f1f5f9;border-radius:4px;height:20px">
        <div style="width:${Math.round(100 * r.count / cutMax)}%;background:#f59e0b;height:100%;border-radius:4px"></div>
      </div>
      <div class="nowrap"><b>問い合わせ ${r.inquiries}件</b>
        <span class="sub">(メッセージ ${r.count}件 ・ 1日 ${r.perDay}件)</span></div>
    </div>`).join('');

  const body = `
  <div class="view-hint">📊 <b>実データ調査</b> — AIトリアージ (出荷前のキャンセル・住所変更の検知、担当の振り分け) を
    設計するための実測値です。直近 <b>${d.days}日</b> 分。
    <span class="sub">個人情報は出しません (件数・時刻・文字数だけ)</span></div>

  <div class="filters">
    <span>集計期間:</span>
    ${[14, 30, 60, 90].map(n => `<a class="${n === d.days ? 'pri btn-link' : 'ghost btn-link'}" href="?days=${n}">${n}日</a>`).join('')}
  </div>

  <div class="card" style="padding:14px">
    <h3 style="margin:0 0 4px">⏰ 締めまでに何件さばく必要があるか</h3>
    <div class="sub" style="margin-bottom:10px">ロジザードの締め = ${WMS_CUTOFFS.map(c => c.label).join(' / ')} の3回。
      キャンセル・住所変更・宛先変更に当たるキーワードを含む顧客メッセージを、受信時刻から「どの締めに間に合わせる必要があったか」で振り分けています。
      <b>これが出荷前トリアージの1回あたりの処理量</b>です。</div>
    ${cutoffCards || '<div class="empty">データがありません</div>'}
    ${errOf(d.cutoffWindows)}
  </div>

  <div class="card" style="padding:14px">
    <h3 style="margin:0 0 4px">💰 AIトリアージの費用 (実測した本文長からの試算)</h3>
    <div class="sub">顧客受信メッセージの平均 <b>${avgChars || '—'}文字</b> ・ 1日あたり <b>${perDayTotal.toFixed(1)}件</b> で計算。
      モデル = gpt-5.6-luna ($0.20 / $1.20 per 1M) ・ 1ドル150円換算</div>
    <table class="cardable" style="margin-top:8px"><tbody>
      <tr><td>1件あたりの入力/出力トークン</td><td>${cost.inTokens} / ${cost.outTokens}</td></tr>
      <tr><td>1件あたり</td><td><b>${cost.perCallJpy}円</b></td></tr>
      <tr><td>1日あたり (全件に投げた場合)</td><td>${cost.perDayJpy}円</td></tr>
      <tr><td><b>1ヶ月あたり</b></td><td><b>約${cost.perMonthJpy.toLocaleString('ja-JP')}円</b></td></tr>
    </tbody></table>
    <div class="sub" style="margin-top:6px">⚠️ これは「届いた顧客メッセージ全部に投げた場合」の上限です。
      自動配信メールを除外すれば下がります。実運用の値はトリアージ稼働後に実測に置き換えます。</div>
  </div>

  <div class="card" style="padding:14px">
    <h3 style="margin:0 0 8px">📥 どれだけ届いているか</h3>
    ${tbl(['チャネル', '件数', '1日あたり'], chRows, r => `<tr><td>${chBadge(r.channel)}</td><td>${r.c}</td><td>${r.per_day}</td></tr>`)}
    ${errOf(d.byChannel)}
    <div class="sub" style="margin-top:8px">問い合わせ総数 ${totals ? totals.inquiries_all : '—'}件 /
      未アーカイブ ${totals ? totals.inquiries_active : '—'}件 /
      未対応 (新着・対応中・保留) ${totals ? totals.open_like : '—'}件</div>
  </div>

  <div class="card" style="padding:14px">
    <h3 style="margin:0 0 8px">🔎 出荷前ブロック候補のキーワード</h3>
    <div class="sub" style="margin-bottom:8px">AIを使わない決定的な検知 (層1) の当たり具合。
      <b>取りこぼしを減らすため、わざと広めに引っかけています</b> — 実際に対応が要るのはこの一部です。</div>
    ${tbl(['キーワード', 'メッセージ数', '問い合わせ数', '1日あたり'], rowsOf(d.keywordHits),
      r => `<tr><td>${he(r.name)}</td><td>${r.count}</td><td>${r.inquiries}</td><td>${r.perDay}</td></tr>`)}
    ${errOf(d.keywordHits)}
  </div>

  <div class="card" style="padding:14px">
    <h3 style="margin:0 0 8px">🔗 注文番号がどれだけ埋まっているか</h3>
    <div class="sub" style="margin-bottom:8px">「どの注文の話か」を特定できる割合。
      <b>メール (Gmail) は取込時に注文番号を入れていないので0%に近いはず</b>です。
      ここが埋まらない分は「注文が特定できないキャンセル依頼」として、人が確認する列に残す必要があります。</div>
    ${tbl(['チャネル', '件数', '注文番号あり', '割合'], rowsOf(d.orderNumberFill), r => `<tr><td>${chBadge(r.channel)}</td><td>${r.c}</td><td>${r.with_order}</td><td>${r.pct}%</td></tr>`)}
    ${errOf(d.orderNumberFill)}
  </div>

  <details class="card" style="padding:12px">
    <summary><b>日別の受信件数 (直近14日)</b></summary>
    ${tbl(['日付 (JST)', '件数'], rowsOf(d.byDay), r => `<tr><td>${he(r.day)}</td><td>${r.c}</td></tr>`)}
  </details>

  <details class="card" style="padding:12px">
    <summary><b>時刻別 (キャンセル・住所変更まわり・JST)</b></summary>
    ${tbl(['JST時', '件数'], rowsOf(d.byHour), r => `<tr><td>${he(r.hour)}時台</td><td>${r.c}</td></tr>`)}
  </details>

  <details class="card" style="padding:12px">
    <summary><b>フォルダ別 (自動配信がどれだけ振り分けられているか)</b></summary>
    ${tbl(['フォルダ', '件数'], rowsOf(d.byFolder), r => `<tr><td>${he(r.folder)}</td><td>${r.c}</td></tr>`)}
  </details>

  <details class="card" style="padding:12px">
    <summary><b>いまの担当・エスカレーションの使われ方</b></summary>
    ${assign ? `
      ${tbl(['担当', '件数 (直近90日)'], assign.byAssignee || [], r => `<tr><td>${he(r.assignee)}</td><td>${r.c}</td></tr>`)}
      <div class="sub" style="margin-top:10px">AIフラグ (0:不要 1:AI返信必要 2:社長確認 3:責任者確認):
        ${(assign.aiNeeded || []).map(r => `${r.ai_needed}=${r.c}件`).join(' / ')}</div>
      <div class="sub">要確認フラグ: ${(assign.attention || []).map(r => `${r.needs_attention ? 'あり' : 'なし'}=${r.c}件`).join(' / ')}</div>
    ` : `<div class="empty">取得できませんでした</div>${errOf(d.assignment)}`}
  </details>

  <details class="card" style="padding:12px">
    <summary><b>テンプレート・Q&amp;Aのカテゴリ (実務の問い合わせ類型)</b></summary>
    <div class="sub" style="margin:8px 0">権限マップの分類が実態と合っているかの裏取りに使います。</div>
    ${cats ? `
      <div><b>返信テンプレート</b></div>
      ${tbl(['カテゴリ', '件数'], cats.templates || [], r => `<tr><td>${he(r.category)}</td><td>${r.c}</td></tr>`)}
      <div style="margin-top:10px"><b>Q&amp;Aナレッジ</b></div>
      ${tbl(['カテゴリ', '件数'], cats.qa || [], r => `<tr><td>${he(r.category)}</td><td>${r.c}</td></tr>`)}
    ` : `<div class="empty">取得できませんでした</div>${errOf(d.categories)}`}
  </details>

  <details class="card" style="padding:12px">
    <summary><b>AI基盤の稼働状況</b></summary>
    ${ai ? `<div class="sub" style="margin-top:8px">
      バッチ実行 ${ai.runs.c}回 (最終 ${fmtJst(ai.runs.last)}) /
      生成済み下書き ${ai.drafts.c}件 (最終 ${fmtJst(ai.drafts.last)}) /
      待機中ジョブ ${ai.queued.c}件</div>` : `<div class="empty">取得できませんでした</div>${errOf(d.aiStatus)}`}
  </details>

  <div class="sub" style="margin-top:10px">集計時刻: ${fmtJst(d.generatedAt)} ・
    <a href="/apps/inquiry-hub/admin">⚙️ 運用管理へ戻る</a></div>`;

  res.send(pageShell('問い合わせ管理 — 実データ調査', 'admin', body, ''));
});

// 一括操作の取り消し (2026-08-25 安全装置。取り消しは1バッチ1回だけ・再送は成功扱い)
router.post('/api/bulk-batches/:id(\\d+)/revert', (req, res) => {
  try {
    const r = revertBulkBatch(Number(req.params.id), actorOf(req));
    console.log(`[inquiry-hub] 一括操作の取り消し #${req.params.id} by ${actorOf(req)} — ${r.reverted}件を復元 / ${r.skipped}件対象外${r.alreadyReverted ? ' (取り消し済みだった)' : ''}`);
    res.json(r);
  } catch (e) { res.status(400).json({ error: String(e?.message || e).slice(0, 300) }); }
});

// ─── ページシェル ───
const CSS = `
* { box-sizing: border-box; }
body { margin: 0; font-family: -apple-system, "Segoe UI", "Hiragino Sans", "Noto Sans JP", sans-serif; background: #f1f5f9; color: #0f172a; font-size: 14px; }
header.app { background: #0f172a; color: #fff; padding: 8px 16px; display: flex; align-items: center; gap: 20px; flex-wrap: wrap; }
header.app h1 { font-size: 16px; margin: 0; white-space: nowrap; }
header.app .back { color: #94a3b8; text-decoration: none; margin-left: auto; font-size: 13px; }
header.app .back:hover { color: #fff; }
/* タブ: バイトさんでも今どこにいるか一目で分かるピル型 (アクティブ=白地) */
nav.tabs { display: flex; gap: 6px; flex-wrap: wrap; }
a.tab { display: inline-flex; align-items: center; gap: 7px; padding: 9px 18px; border-radius: 10px;
  color: #cbd5e1; text-decoration: none; font-size: 14px; font-weight: 600; line-height: 1; border: 1px solid transparent; }
a.tab:hover { background: rgba(255,255,255,.14); color: #fff; }
a.tab.on { background: #fff; color: #0f172a; box-shadow: 0 1px 3px rgba(0,0,0,.3); }
.tab-icon { font-size: 16px; }
.wrap { padding: 16px; max-width: 1400px; margin: 0 auto; }
.card { background: #fff; border-radius: 12px; box-shadow: 0 1px 3px rgba(0,0,0,.08); overflow-x: auto; }
table { border-collapse: collapse; width: 100%; }
th, td { padding: 8px 10px; text-align: left; border-bottom: 1px solid #e2e8f0; vertical-align: top; }
th { background: #f8fafc; font-size: 12px; color: #475569; white-space: nowrap; }
tbody tr { cursor: pointer; }
tbody tr:hover { background: #f8fafc; }
tr.unread { background: #fefce8; }
tr.unread:hover { background: #fef9c3; }
td.subj a { color: #1d4ed8; text-decoration: none; font-weight: 600; }
.sub { color: #64748b; font-size: 12px; }
.nowrap { white-space: nowrap; }
/* 受信日時列 (2026-08-26): 桁を揃えて必要最小幅に。件名欄を圧迫しないよう width:1% で内容幅にする */
.dtcol { width: 1%; white-space: nowrap; font-variant-numeric: tabular-nums; }
.badge { display: inline-block; padding: 2px 8px; border-radius: 999px; font-size: 12px; white-space: nowrap; }
.dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; background: #f59e0b; vertical-align: middle; }
.empty { color: #94a3b8; text-align: center; padding: 24px; }
.filters { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; margin-bottom: 12px; background: #fff; padding: 10px 12px; border-radius: 12px; box-shadow: 0 1px 3px rgba(0,0,0,.08); }
.filters select, .filters input { padding: 6px 8px; border: 1px solid #cbd5e1; border-radius: 8px; font-size: 13px; }
.chk { display: inline-flex; align-items: center; gap: 4px; font-size: 13px; white-space: nowrap; }
button, .btn-link { padding: 7px 14px; border-radius: 8px; border: 1px solid #cbd5e1; background: #fff; cursor: pointer; font-size: 13px; text-decoration: none; color: #0f172a; display: inline-block; }
button.pri { background: #1d4ed8; border-color: #1d4ed8; color: #fff; }
button:disabled { opacity: .5; cursor: default; }
.pager { padding: 10px; display: flex; gap: 16px; justify-content: center; color: #475569; }
.pager a { color: #1d4ed8; }
/* ═══ 上部タブ (すべて / メール / モール問い合わせ)。バッジ=新着件数 ═══ */
.ch-tabs { display: flex; gap: 6px; border-bottom: 2px solid #cbd5e1; margin-bottom: 14px; flex-wrap: wrap; }
.ch-tabs a { padding: 9px 16px; border-radius: 10px 10px 0 0; color: #475569; font-weight: 600;
  border: 1px solid transparent; border-bottom: none; margin-bottom: 0; white-space: nowrap; }
.ch-tabs a:hover { background: #e2e8f0; text-decoration: none; }
.ch-tabs a.on { background: #fff; color: #1d4ed8; border-color: #cbd5e1;
  position: relative; top: 2px; padding-bottom: 11px; }
/* モール管理画面への外部リンク (タブ行の右端。タブと見分けがつくよう控えめな見た目) */
.mall-links { margin-left: auto; display: flex; gap: 6px; align-items: center; flex-wrap: wrap; }
.mall-links a { padding: 6px 10px; border-radius: 8px; color: #475569; font-weight: 600; font-size: 13px;
  background: #e2e8f0; white-space: nowrap; }
.mall-links a:hover { background: #cbd5e1; text-decoration: none; }
.tab-cnt { display: inline-block; background: #fee2e2; color: #b91c1c; border-radius: 999px;
  padding: 1px 8px; font-size: 12px; margin-left: 2px; vertical-align: 1px; }
.tab-cnt.zero { background: #f1f5f9; color: #94a3b8; }
/* ═══ 状態タブ (新着/返信処理中/完了/すべて)。上部タブの下に並ぶ ═══ */
.view-tabs { display: flex; gap: 6px; margin: 0 0 12px; flex-wrap: wrap; }
.view-tabs a { padding: 6px 12px; border-radius: 999px; color: #475569; font-size: 13px; font-weight: 600;
  background: #f1f5f9; border: 1px solid transparent; white-space: nowrap; }
.view-tabs a:hover { background: #e2e8f0; text-decoration: none; }
.view-tabs a.on { background: #1d4ed8; color: #fff; border-color: #1d4ed8; }
.vt-cnt { display: inline-block; margin-left: 6px; background: rgba(0,0,0,.08); border-radius: 999px;
  padding: 0 7px; font-size: 12px; }
.view-tabs a.on .vt-cnt { background: rgba(255,255,255,.25); }
.vt-cnt.zero { opacity: .55; }
/* ═══ 一括操作バー ═══ */
.bulkbar { display: flex; align-items: center; gap: 8px; flex-wrap: wrap;
  background: #eff6ff; border: 1px solid #bfdbfe; border-radius: 12px; padding: 10px 12px; margin-bottom: 12px; }
.bulkbar .bulk-n { font-size: 13px; color: #1e40af; margin-right: 4px; }
.bulkbar select { padding: 5px 8px; }
.bulkbar button { margin: 0; }
th.selcell, td.selcell { width: 34px; text-align: center; }
td.selcell input, th.selcell input { width: 18px; height: 18px; cursor: pointer; }
.detail-head h2 { margin: 8px 0 12px; font-size: 18px; }
.detail-nav { display: flex; align-items: center; gap: 18px; flex-wrap: wrap; }
.detail-nav-adj { display: flex; gap: 14px; }
.detail-nav .nav-off { color: #cbd5e1; }
.detail-nav .quick-done { margin-left: auto; }
.detail-grid { display: grid; grid-template-columns: 1fr 360px; gap: 16px; align-items: start; }
@media (max-width: 900px) { .detail-grid { grid-template-columns: 1fr; } }
.thread { display: flex; flex-direction: column; gap: 10px; }
.msg { background: #fff; border-radius: 12px; padding: 10px 14px; box-shadow: 0 1px 3px rgba(0,0,0,.08); border-left: 4px solid #94a3b8; }
.msg.in { border-left-color: #f59e0b; }
.msg.out { border-left-color: #1d4ed8; background: #eff6ff; }
.msg-head { display: flex; gap: 8px; align-items: baseline; margin-bottom: 6px; flex-wrap: wrap; }
.msg-date { color: #94a3b8; font-size: 12px; }
.msg-body { white-space: normal; line-height: 1.7; overflow-wrap: anywhere; }
.msg-body a, .note a { color: #1d4ed8; text-decoration: underline; overflow-wrap: anywhere; }
/* 長文メール (自動配信・署名込み) はスレッドを追いやすいよう畳む */
.msg-body details.more > summary { cursor: pointer; color: #1d4ed8; font-size: 13px; margin-top: 6px;
  list-style: none; padding: 4px 0; }
.msg-body details.more > summary::-webkit-details-marker { display: none; }
.msg-body details.more[open] > summary { color: #64748b; }
.msg-atts { margin-top: 8px; display: flex; flex-wrap: wrap; gap: 6px 10px; align-items: flex-start; }
.att { display: inline-block; background: #f1f5f9; border-radius: 8px; padding: 3px 8px; font-size: 12px; }
.att a { color: #1d4ed8; }
/* 添付画像: その場でサムネイル表示 (クリックで原寸を別タブ)。取得失敗時は文言に差し替え */
figure.att-img { margin: 0; max-width: 240px; }
figure.att-img img { max-width: 240px; max-height: 240px; border-radius: 8px; border: 1px solid #e2e8f0; display: block; background: #f8fafc; }
figure.att-img figcaption { font-size: 12px; color: #475569; margin-top: 4px; overflow-wrap: anywhere; }
figure.att-img figcaption a { color: #1d4ed8; }
figure.att-img .att-fail { display: none; color: #b91c1c; }
figure.att-img.att-err img { display: none; }
figure.att-img.att-err .att-fail { display: block; }
.att-dl { margin-left: 6px; white-space: nowrap; }
/* 一覧のフォルダ表示 */
.folder-chip { background: #eef2ff; color: #3730a3; border-radius: 6px; padding: 1px 6px; }
/* 一覧の本文プレビュー (最新の顧客メッセージ冒頭)。1行で切って一覧のスキャン速度を落とさない */
.preview { color: #475569; font-size: 12.5px; margin-top: 2px; overflow: hidden;
  text-overflow: ellipsis; white-space: nowrap; max-width: 100%; }
/* クイック入口 (今日の新着 / 要確認 / 自分の対応中 / 未割当 / 滞留) */
.quick-bar { display: flex; gap: 6px; align-items: center; flex-wrap: wrap; margin-bottom: 10px; }
.quick-bar .qb-label { color: #64748b; font-size: 12px; }
.quick-bar a { padding: 5px 11px; border-radius: 999px; background: #fff; border: 1px solid #cbd5e1;
  color: #334155; font-size: 13px; font-weight: 600; white-space: nowrap; }
.quick-bar a:hover { background: #e2e8f0; text-decoration: none; }
.quick-bar a.on { background: #1d4ed8; border-color: #1d4ed8; color: #fff; }
.lbl { display: inline-block; border-radius: 6px; padding: 1px 8px; font-size: 12px; font-weight: 600; white-space: nowrap; vertical-align: 1px; }
.swatches { display: flex; gap: 6px; flex-wrap: wrap; margin-top: 4px; }
.swatch { width: 22px; height: 22px; border-radius: 6px; border: 1px solid rgba(0,0,0,.15); cursor: pointer; padding: 0; }
.panel { background: #fff; border-radius: 12px; padding: 12px 14px; box-shadow: 0 1px 3px rgba(0,0,0,.08); margin-bottom: 12px; }
.panel h3 { margin: 0 0 10px; font-size: 14px; }
.panel dl { margin: 0; display: grid; grid-template-columns: 90px 1fr; gap: 6px 8px; }
.panel dt { color: #64748b; font-size: 12px; padding-top: 2px; }
.panel dd { margin: 0; overflow-wrap: anywhere; }
.panel label { display: block; margin-bottom: 8px; font-size: 13px; color: #334155; }
.panel select, .panel input[type=text], .panel textarea { width: 100%; padding: 6px 8px; border: 1px solid #cbd5e1; border-radius: 8px; font-size: 13px; margin-top: 2px; }
.panel .row { display: flex; gap: 6px; }
/* 顧客情報の手入力 (2026-08-31): 自動保存の入力欄。保存直後は緑枠で「保存された」を示す */
.ci-input + .ci-input { margin-top: 4px; }
.ci-input.ci-saving, .ci-select.ci-saving { opacity: .6; }
.ci-input.ci-saved, .ci-select.ci-saved { border-color: #22c55e; box-shadow: 0 0 0 2px #bbf7d0; }
/* 注文番号の行: [モール▾][注文番号] を横並び (モール選択は4割) */
.ci-order-row { margin-top: 2px; }
.ci-order-row .ci-select { flex: 0 0 42%; min-width: 0; margin-top: 0; }
.ci-order-row .ci-input { flex: 1; min-width: 0; margin-top: 0; }
.ci-lock { font-size: 11px; margin-left: 4px; cursor: help; }
.ci-links:empty { display: none; }
.ci-links a + a { margin-left: 8px; }
.ci-hint { margin-top: 8px; }
/* 返信パネルのテンプレート選択行 */
.tpl-row { margin-bottom: 8px; flex-wrap: wrap; }
.tpl-row #tplSel { flex: 1; min-width: 160px; }
/* 🔍キーワード絞り込み (2026-09-02 スタッフ要望)。セレクトより控えめな幅で横に並べる */
.tpl-row #tplSearch { flex: 0 1 200px; min-width: 140px; }
/* カテゴリ見出し (optgroup) は太字・テンプレートは1段下げて、一目で区別がつくように */
.tpl-row #tplSel optgroup { font-weight: 700; font-style: normal; color: #0f172a; }
.tpl-row #tplSel option { font-weight: 400; padding-left: 1.2em; }
/* 送信ジョブ履歴: 送ろうとした本文の全文 (失敗したときに何を書いたか読めるように) */
.job-body summary { cursor: pointer; list-style: none; }
.job-body summary::-webkit-details-marker { display: none; }
.job-body .job-body-more { color: #4f46e5; margin-left: 4px; white-space: nowrap; }
.job-body[open] .job-body-more { visibility: hidden; }
.job-body-full { white-space: pre-wrap; overflow-wrap: anywhere; background: #f8fafc;
  border: 1px solid #e2e8f0; border-radius: 6px; padding: 8px 10px; margin: 6px 0;
  font-size: 13px; max-height: 340px; overflow-y: auto; }
.job-restore { margin-bottom: 4px; }
/* AI書き換えボタン行 */
.rw-row { margin-top: 6px; flex-wrap: wrap; align-items: center; }
.rw-row .rw-btn { margin: 0; }
/* AI下書きの「要確認」警告 */
.draft-warn { background: #fef3c7; border: 1px solid #fcd34d; border-radius: 8px;
  padding: 8px 10px; margin: 6px 0; font-size: 13px; color: #92400e; }
.draft-warn code { background: #fff7ed; border: 1px solid #fed7aa; border-radius: 4px;
  padding: 1px 5px; font-family: inherit; }
/* 📎 送信用添付のチップ */
.att-chip { display: inline-flex; align-items: center; gap: 4px; background: #eef2ff;
  border: 1px solid #c7d2fe; border-radius: 8px; padding: 2px 4px 2px 8px; margin: 4px 6px 0 0;
  font-size: 13px; color: #3730a3; max-width: 100%; overflow-wrap: anywhere; }
.att-chip button { margin: 0; padding: 0 6px; font-size: 12px; }
.panel .row input { flex: 1; }
.panel textarea { resize: vertical; }
.note { border-top: 1px solid #e2e8f0; padding: 8px 0; }
.note-head { margin-bottom: 4px; }
.log-row { border-top: 1px solid #f1f5f9; padding: 6px 0; font-size: 13px; overflow-wrap: anywhere; }
.reply-note { color: #64748b; text-align: center; }
.ai-draft { background: #f0fdfa; border-radius: 8px; padding: 8px 10px; margin: 8px 0; line-height: 1.7; }
#toast { display: none; position: fixed; bottom: 24px; left: 50%; transform: translateX(-50%); background: #0f172a; color: #fff; padding: 10px 18px; border-radius: 10px; z-index: 2000; }
/* 階層アコーディオン: 📁グループ (第一階層のみ既定表示) → サブグループ → 項目 */
details.grp { margin: 6px 0; border: 1px solid #dbe3ee; border-radius: 10px; background: #fff; overflow: hidden; }
details.grp > summary { display: flex; gap: 8px; align-items: center; padding: 11px 14px; cursor: pointer;
  font-weight: 600; background: #f1f5f9; list-style: none; user-select: none; }
details.grp > summary::-webkit-details-marker { display: none; }
details.grp > summary:hover { background: #e2e8f0; }
.grp-arrow { display: inline-block; font-size: 11px; color: #64748b; transition: transform .15s; }
details.grp[open] > .grp-arrow, details.grp[open] > summary .grp-arrow { transform: rotate(90deg); }
.grp-count { margin-left: auto; font-size: 12px; font-weight: normal; color: #475569; background: #fff;
  border: 1px solid #cbd5e1; border-radius: 999px; padding: 2px 10px; }
.grp-body { padding: 4px 10px 8px 22px; }
details.tpl { border-bottom: 1px solid #eef2f7; }
details.tpl:last-child { border-bottom: none; }
details.tpl summary { display: flex; gap: 10px; align-items: baseline; padding: 9px 6px; cursor: pointer; flex-wrap: wrap; }
details.tpl summary:hover { background: #f8fafc; border-radius: 8px; }
.tpl-icon { font-size: 14px; }
.tpl-body { padding: 4px 10px 12px; }
.tpl-body pre { background: #f8fafc; border-radius: 8px; padding: 10px 12px; white-space: pre-wrap; overflow-wrap: anywhere; font-family: inherit; line-height: 1.7; margin: 8px 0; }
.tpl-ops { display: flex; gap: 8px; margin-top: 8px; }
.edit-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 0 16px; }
@media (max-width: 700px) { .edit-grid { grid-template-columns: 1fr; } }
.qa-q { background: #fef9c3; border-radius: 8px; padding: 8px 12px; margin: 8px 0; line-height: 1.7; }
.qa-a { background: #dcfce7; border-radius: 8px; padding: 8px 12px; margin: 8px 0; line-height: 1.7; }
/* 運用管理画面 */
.card-title { padding: 12px 14px; font-weight: 700; border-bottom: 1px solid #e2e8f0; }
.card-title .sub { font-weight: normal; }
td.ops { white-space: normal; }
td.ops button { margin: 2px 4px 2px 0; }
.expiry-edit { display: flex; gap: 4px; margin-top: 4px; align-items: center; }
.expiry-edit input[type=date] { padding: 3px 6px; border: 1px solid #cbd5e1; border-radius: 6px; font-size: 12px; }
.expiry-edit button { padding: 3px 10px; font-size: 12px; }

/* 詳細サイドバーの条件行: 幅が狭いので [項目][条件] を上段・[値] を下段全幅に */
.panel .row.rule-row { display: flex; flex-wrap: wrap; gap: 6px; }
.panel .row.rule-row > select { flex: 1 1 calc(50% - 3px); min-width: 0; }
.panel .row.rule-row > input[type=text] { flex: 1 1 100%; min-width: 0; order: 3; margin-top: 0; }

/* ═══ Gmail風レイアウト: 左サイドバー + 本文 ═══ */
.layout { display: flex; align-items: flex-start; }
.side-nav { width: 210px; flex: 0 0 210px; padding: 12px 8px; position: sticky; top: 0;
  max-height: 100vh; overflow-y: auto; }
.nav-group { display: flex; flex-direction: column; gap: 2px; }
.nav-sep { height: 1px; background: #dbe3ee; margin: 10px 12px; }
/* ✉️ メール作成 (2026-08-27): メールディーラーと同じくサイドバー最上部。押すと選択画面 → 作成画面 */
.nav-compose { display: flex; align-items: center; justify-content: center; gap: 8px;
  margin: 2px 6px 12px; padding: 11px 14px; border-radius: 10px; background: #1d4ed8; color: #fff;
  font-weight: 700; font-size: 14px; text-decoration: none; box-shadow: 0 1px 3px rgba(0,0,0,.15); }
.nav-compose:hover { background: #1e40af; color: #fff; }
.nav-compose.on { background: #1e3a8a; }
/* 新規メール作成の入力欄 (1段目=選択・2段目=作成で共通) */
.cform { display: grid; grid-template-columns: 120px minmax(0, 1fr); gap: 12px 14px; align-items: start; }
.cform > label.k { color: #475569; font-size: 13px; font-weight: 700; padding-top: 8px; }
.cform input[type=text], .cform input[type=email], .cform select, .cform textarea {
  width: 100%; padding: 8px 10px; border: 1px solid #cbd5e1; border-radius: 8px; font-size: 14px;
  font-family: inherit; background: #fff; }
.cform textarea { min-height: 260px; line-height: 1.6; resize: vertical; }
.cpreview { background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px; padding: 10px 12px;
  white-space: pre-wrap; word-break: break-word; font-size: 13px; color: #334155; max-height: 220px; overflow-y: auto; }
.cactions { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; margin-top: 14px; }
@media (max-width: 700px) {
  .cform { grid-template-columns: 1fr; gap: 4px 0; }
  .cform > label.k { padding-top: 10px; }
}
.nav-item { display: flex; align-items: center; gap: 10px; padding: 9px 14px; border-radius: 999px;
  color: #334155; text-decoration: none; font-size: 14px; line-height: 1.3; }
.nav-item:hover { background: #e2e8f0; }
.nav-item.on { background: #dbeafe; color: #1d4ed8; font-weight: 700; }
.nav-icon { font-size: 16px; flex: 0 0 auto; }
.nav-label { flex: 1 1 auto; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.nav-count { flex: 0 0 auto; font-size: 12px; font-weight: 700; color: #1d4ed8; }
.nav-item.on .nav-count { color: #1d4ed8; }
.nav-toggle { display: none; background: transparent; border: none; color: #fff; font-size: 20px;
  padding: 4px 8px; cursor: pointer; min-height: 0; }
.nav-backdrop { display: none; }
.layout > .wrap { flex: 1 1 auto; min-width: 0; padding: 16px; max-width: 1400px; }
.view-hint { color: #64748b; font-size: 13px; margin: 0 0 10px; }

/* ═══ 絞り込みボックス: PCは常時展開・スマホは折りたたみ (画面上半分をフィルタで潰さない) ═══ */
details.fbox > summary { display: none; }   /* PCでは常に展開 (open属性はサーバーが常に付与) */

/* ═══════════ スマホ対応 (〜700px) ═══════════
   方針: 横スクロールする表を「1件=1カード」に組み替える (table.cardable)。
   thead を隠し、各 td の内容を data-label の見出し付きで縦に積む。
   data-full の td は1行占有、それ以外はバッジのように横に流す。
   PC表示 (701px〜) は一切変更しない */
@media (max-width: 700px) {
  body { font-size: 15px; }                       /* iOSの自動ズーム回避 (16px未満の入力欄対策と併用) */
  header.app { padding: 6px 8px; gap: 8px; }
  header.app h1 { font-size: 15px; }
  header.app .back { font-size: 12px; }
  /* サイドバーはドロワー化 (Gmailアプリと同じく ☰ で開く) */
  .nav-toggle { display: block; }
  .side-nav { position: fixed; top: 0; left: 0; bottom: 0; z-index: 1200; width: 268px; flex-basis: 268px;
    background: #fff; box-shadow: 2px 0 12px rgba(0,0,0,.18); padding: 14px 10px;
    transform: translateX(-100%); transition: transform .18s ease-out; max-height: none; }
  .side-nav.open { transform: translateX(0); }
  .nav-backdrop.on { display: block; position: fixed; inset: 0; background: rgba(15,23,42,.4); z-index: 1100; }
  .nav-item { padding: 12px 14px; min-height: 44px; }
  .layout > .wrap { padding: 10px; }

  /* タップターゲット確保 (指で押せる高さ)。font-size:16px = iOSの自動ズーム防止 */
  button, .btn-link, select, textarea,
  input[type=text], input[type=search], input[type=number], input[type=date], input[type=file] {
    min-height: 44px; font-size: 16px; }
  input[type=checkbox] { width: 20px; height: 20px; }
  .chk { padding: 6px 0; }
  .expiry-edit button, .tpl-ops button, td.ops button { min-height: 40px; }
  .pager { gap: 10px; padding: 14px 10px; }
  .pager a { padding: 10px 14px; background: #fff; border: 1px solid #cbd5e1; border-radius: 8px; text-decoration: none; }

  /* 絞り込みは折りたたみ */
  details.fbox { background: #fff; border-radius: 12px; box-shadow: 0 1px 3px rgba(0,0,0,.08); margin-bottom: 10px; }
  details.fbox > summary { display: flex; align-items: center; gap: 8px; padding: 12px 14px;
    font-weight: 600; cursor: pointer; list-style: none; }
  details.fbox > summary::-webkit-details-marker { display: none; }
  details.fbox .filters { box-shadow: none; margin-bottom: 0; padding: 0 12px 12px; }
  /* 入力欄は1行占有 (指で選びやすく)・ボタンは横に流す (縦に伸ばしすぎない) */
  .filters { align-items: stretch; }
  .filters select, .filters input { flex: 1 1 100%; width: 100%; }
  .filters button, .filters .btn-link, .filters .chk { flex: 1 1 auto; text-align: center; justify-content: center; }

  /* 表 → カード */
  table.cardable, table.cardable tbody { display: block; }
  table.cardable thead { display: none; }
  table.cardable tr { display: flex; flex-wrap: wrap; gap: 4px 12px; align-items: baseline;
    border: 1px solid #e2e8f0; border-radius: 12px; margin: 10px; padding: 12px; cursor: pointer; }
  table.cardable td { display: block; border: none; padding: 0; max-width: 100%; }
  table.cardable td[data-full] { flex-basis: 100%; }
  table.cardable td[data-empty] { display: none; }   /* 値が「—」だけの列はスマホでは省く */
  table.cardable td[data-label]::before { content: attr(data-label); display: block;
    color: #94a3b8; font-size: 11px; line-height: 1.4; }
  table.cardable td.empty { flex-basis: 100%; text-align: center; }
  /* 一覧: 件名を先頭・大きく。1列目 (チャネルバッジ) の店舗名はバッジの右に添える */
  table.cardable td.subj { order: -1; font-size: 15px; }
  table.cardable td:first-child > .sub, table.cardable td.chcell > .sub { display: inline; margin-left: 6px; }
  table.cardable td.ops { flex-basis: 100%; display: flex; flex-wrap: wrap; gap: 6px; margin-top: 4px; }
  table.cardable td.ops button { margin: 0; flex: 1 1 auto; }

  /* 上部タブ: スマホでは詰めて全タブが1〜2行に収まるように */
  .ch-tabs { gap: 4px; }
  .ch-tabs a { padding: 8px 10px; font-size: 13px; }
  /* モールリンクはタブの下に折り返す (右寄せをやめてタブを押しやすく保つ) */
  .mall-links { margin-left: 0; flex-basis: 100%; }
  .mall-links a { padding: 8px 10px; font-size: 12px; }
  /* クイック入口: 横スクロール1行 (折り返して縦に伸びると一覧が押し出される) */
  .quick-bar { flex-wrap: nowrap; overflow-x: auto; -webkit-overflow-scrolling: touch; }
  .quick-bar a { padding: 8px 12px; font-size: 13px; }
  /* カード表示では本文プレビューを2行まで見せる (1行だと情報が足りない) */
  table.cardable td.subj .preview { white-space: normal; display: -webkit-box;
    -webkit-line-clamp: 2; -webkit-box-orient: vertical; }
  .view-tabs a { padding: 6px 10px; font-size: 12px; }
  /* 一括操作: スマホは各コントロールを全幅に (誤タップ防止) */
  .bulkbar select, .bulkbar button { flex: 1 1 100%; }
  /* カード表示でも選択チェックは先頭に出す (件名 order:-1 より前) */
  table.cardable td.selcell { order: -2; width: auto; }
  table.cardable td.selcell::before { content: none; }

  /* 詳細画面 */
  .detail-head h2 { font-size: 16px; }
  .msg { padding: 10px 12px; }
  /* 添付画像はスマホでは2枚並び (指で押せる大きさを保つ) */
  figure.att-img { max-width: calc(50% - 8px); }
  figure.att-img img { max-width: 100%; max-height: 200px; }
  .panel dl { grid-template-columns: 78px 1fr; }
  .panel textarea, .panel input[type=text] { font-size: 16px; }
  .grp-body { padding: 4px 6px 8px 10px; }        /* テンプレ階層の左インデントを詰める */

  /* フォーム: 入力欄をカード幅いっぱいに (はみ出し・不揃い防止) */
  .edit-grid label { display: block; }
  .edit-grid input, .edit-grid select, .edit-grid textarea { width: 100%; }
  .row.rule-row { display: flex; flex-wrap: wrap; gap: 6px; }  /* .row は既定でflexではない (.panel .row のみ) */
  .row.rule-row > * { flex: 1 1 100%; width: 100%; }
}
`;

/**
 * ページ共通シェル (Gmail風の左サイドバー)。
 * active: 'inbox'|'sent'|'done'|'all'|'folder:<id>'|'folders'|'labels'|'links'|'templates'|'qa'|'mailrules'|'admin'
 * PCではサイドバー常時表示、スマホでは ☰ で開くドロワー
 */
function pageShell(title, active, body, script, opts = {}) {
  // 受信箱の件数はサイドバーに出す (Gmailの「受信トレイ (12)」と同じ位置づけ)。
  // 一覧以外の画面でも出すが、失敗しても画面は出す (fail-soft)
  let counts = {};
  try { counts = countByView(); } catch { /* 初期化前などは件数なしで表示 */ }
  const navItem = (href, icon, label, key, count) => `
    <a href="${href}" class="nav-item${active === key ? ' on' : ''}">
      <span class="nav-icon">${icon}</span><span class="nav-label">${label}</span>
      ${count ? `<span class="nav-count">${count}</span>` : ''}
    </a>`;
  // 上部タブ (メール/モール) を選択中はサイドバーのビュー移動でも維持する
  const groupQs = CHANNEL_GROUPS[opts.group] ? `&group=${opts.group}` : '';
  // 任意フォルダ (スタッフが作る分類箱)。件数は「そのフォルダの未対応 (新着・対応中・保留)」を出す
  // = 受信トレイと同じ「自分のタスク」基準。失敗しても画面は出す (fail-soft)
  let folderNav = [];
  try { folderNav = listFolders({ withCounts: true }); } catch { /* 初期化前などは出さない */ }
  // ⏰締め前確認の未対応件数。重い集計なので失敗しても画面は出す (fail-soft)
  let cutoffCount = 0;
  try { cutoffCount = countCutoffItems().inquiries; } catch { /* 初期化前などは0 */ }
  // 📦返品・交換案件の未完了件数 (fail-soft)
  let openCaseCount = 0;
  try { openCaseCount = countOpenCases(); } catch { /* 初期化前などは0 */ }
  const folderItems = folderNav.map(f =>
    navItem(`/apps/inquiry-hub?view=all&folder=${f.id}${groupQs}`, '📁', he(f.name), `folder:${f.id}`, f.open_count)).join('');

  const sidebar = `
  <aside class="side-nav" id="sideNav">
    <a href="/apps/inquiry-hub/compose" class="nav-compose${active === 'compose' ? ' on' : ''}"
      title="宛先を指定して新しくメールを送ります (テンプレート・署名を選んでから作成画面に進みます)">✉️ メール作成</a>
    <div class="nav-group">
      ${navItem(`/apps/inquiry-hub?view=inbox${groupQs}`, '📥', '受信トレイ', 'inbox', counts.inbox)}
      ${navItem(`/apps/inquiry-hub?view=sent${groupQs}`, '📤', '送信済み', 'sent', counts.sent)}
      ${navItem(`/apps/inquiry-hub?view=done${groupQs}`, '✅', '対応済み', 'done', 0)}
      ${navItem(`/apps/inquiry-hub?view=all${groupQs}`, '🗂️', 'すべて', 'all', 0)}
    </div>
    <div class="nav-sep"></div>
    <div class="nav-group">
      ${navItem('/apps/inquiry-hub/cutoff', '⏰', '締め前確認', 'cutoff', cutoffCount)}
      ${navItem('/apps/inquiry-hub/cases', '📦', '返品・交換案件', 'cases', openCaseCount)}
    </div>
    <div class="nav-sep"></div>
    <div class="nav-group">
      ${folderItems}
      ${navItem('/apps/inquiry-hub/folders', '⚙️', 'フォルダを作る・編集', 'folders', 0)}
      ${navItem('/apps/inquiry-hub/labels', '🏷️', 'ラベルを作る・編集', 'labels', 0)}
      ${navItem('/apps/inquiry-hub/links', '🔗', 'リンクを作る・編集', 'links', 0)}
    </div>
    <div class="nav-sep"></div>
    <div class="nav-group">
      ${navItem('/apps/inquiry-hub/templates', '📄', '返信テンプレート', 'templates', 0)}
      ${navItem('/apps/inquiry-hub/signatures', '✍️', '署名', 'signatures', 0)}
      ${navItem('/apps/inquiry-hub/qa', '❓', 'Q&amp;A', 'qa', 0)}
      ${navItem('/apps/inquiry-hub/mail-rules', '📧', 'メールルール', 'mailrules', 0)}
      ${navItem('/apps/inquiry-hub/staff', '👥', '担当者と権限', 'staff', 0)}
      ${navItem('/apps/inquiry-hub/admin', '⚙️', '運用管理', 'admin', 0)}
    </div>
  </aside>`;
  return `<!DOCTYPE html><html lang="ja"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>${he(title)}</title><style>${CSS}</style></head>
<body>
<header class="app">
  <button id="navToggle" class="nav-toggle" aria-label="メニュー">☰</button>
  <h1>💬 問い合わせ管理</h1>
  <a href="/" class="back">← ポータルに戻る</a>
</header>
<div class="layout">
  ${sidebar}
  <div class="nav-backdrop" id="navBackdrop"></div>
  <div class="wrap">${body}</div>
</div>
<div id="toast"></div>
<script>
function toast(msg) {
  var t = document.getElementById('toast');
  t.textContent = msg; t.style.display = 'block';
  clearTimeout(t._h); t._h = setTimeout(function(){ t.style.display = 'none'; }, 2800);
}
// テンプレートのキーワード絞り込み (2026-09-02 スタッフ要望「キーワード入力で探せたら便利」)。
// 全角/半角・大文字/小文字・ひらがな/カタカナの違いを吸収する。空白区切りは AND 条件
function tplNorm(s) {
  s = String(s || '');
  try { s = s.normalize('NFKC'); } catch (e) { /* 古い環境では正規化なしで続行 */ }
  return s.toLowerCase().replace(/[\\u3041-\\u3096]/g, function(c) { return String.fromCharCode(c.charCodeAt(0) + 0x60); });
}
function tplFilter(tpls, q) {
  var words = tplNorm(q).split(/\\s+/).filter(Boolean);
  if (!words.length) return tpls;
  return tpls.filter(function(t) {
    var hay = tplNorm([t.name, t.category, t.subject, t.keywords, t.body, t.bodyBottom].join('\\n'));
    return words.every(function(w) { return hay.indexOf(w) >= 0; });
  });
}
// サイドバー開閉 (スマホ)。PCでは常時表示なのでボタン自体をCSSで隠している
(function() {
  var btn = document.getElementById('navToggle'), nav = document.getElementById('sideNav'),
      bd = document.getElementById('navBackdrop');
  if (!btn || !nav) return;
  function close() { nav.classList.remove('open'); if (bd) bd.classList.remove('on'); }
  btn.addEventListener('click', function() {
    nav.classList.toggle('open');
    if (bd) bd.classList.toggle('on', nav.classList.contains('open'));
  });
  if (bd) bd.addEventListener('click', close);
})();
${script || ''}
</script>
</body></html>`;
}

export default router;
