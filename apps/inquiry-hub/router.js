/**
 * EC問い合わせ管理システム (inquiry-hub) — Step 1: 一覧/詳細画面 (read-only + 社内操作)
 *
 * メールディーラー置き換えの自社問い合わせ管理基盤。
 * 設計書: AI_reference/システム設計/問い合わせ管理システム_設計書_v1.2_20260716.md
 *
 * Step 1 スコープ (設計書§11):
 *   GET  /                        問い合わせ一覧 (フィルタ・検索)
 *   GET  /inquiries/:id           問い合わせ詳細 (スレッド・チケット情報・メモ・履歴)
 *   POST /api/inquiries/:id/status     社内ステータス変更
 *   POST /api/inquiries/:id/assign     担当者設定
 *   POST /api/inquiries/:id/read       社内既読/未読切替
 *   POST /api/inquiries/:id/attention  要確認フラグ切替
 *   POST /api/inquiries/:id/ai-flag    AIフラグ設定 (0:不要 1:AI返信 2:社長確認 3:責任者確認)
 *   POST /api/inquiries/:id/notes      社内メモ追加
 *
 * 外部API同期 (Step 2)・返信送信 (Step 3〜5)・ロック (Step 6)・AI (Step 7) は未実装。
 * external_status / 最終同期日時 は表示のみ (同期実装前は seed 値がそのまま出る)。
 */
import crypto from 'crypto';
import { Router } from 'express';
import { getDB, logActivity } from './db.js';
import { CHANNELS, STATUSES, AI_FLAGS, PAGE_SIZE, VIEWS, DEFAULT_VIEW, listInquiries, listFilterOptions, countByView, getInquiryDetail } from './queries.js';
import { importTemplatesCsv, importQaCsv, listTemplates, listQa } from './templates.js';
import { runSync, listSyncStatus } from './sync/engine.js';
import { buildAdapterForShop, refreshShopAuthStatus } from './sync/cron.js';
import { listOutboxIssues, resolveUnknown, cancelJob, createReplyJob } from './outbox.js';
import { listMailRules, addMailRule, setMailRuleActive, deleteMailRule, evaluateMailRules, importMailDealerRulesCsv } from './mail-rules.js';

// 返信エディタの Dark Launch フラグ (送信ワーカー稼働前にスタッフが誤って「送信したつもり」に
// ならないよう、既定は非表示。動作確認・Step 3 送信開始時に env で有効化する)
const replyEditorEnabled = () => {
  const v = process.env.INQUIRY_HUB_REPLY_EDITOR_ENABLED;
  return v === 'true' || v === '1';
};

const router = Router();

// ─── 共通ヘルパ ───
const he = s => String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
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

// ─── 対応履歴の表示整形 (生JSONではなく日本語ラベルで出す) ───
const ACTION_LABELS = {
  status_change: '状態変更', assign: '担当変更', note_add: 'メモ追加', read_toggle: '既読/未読',
  attention_toggle: '要確認フラグ', ai_flag: 'AIフラグ', seed: 'テストデータ投入',
  reply_created: '返信ジョブ作成', reply_resolved: '送信結果の解決', reply_cancelled: '送信ジョブ取消',
};
function fmtLogValue(key, v) {
  switch (key) {
    case 'status': return (STATUSES[v] || {}).label || String(v);
    case 'is_unread': return v ? '未読' : '既読';
    case 'needs_attention': return v ? '⚠️要確認ON' : '要確認OFF';
    case 'ai_needed': return Number(v) === 0 ? 'AI不要' : ((AI_FLAGS[v] || {}).label || String(v));
    case 'assigned': return v ? String(v) : '未割当';
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

// ─── 一覧画面 ───
router.get('/', (req, res) => {
  const q = req.query || {};
  const kw = String(q.q || '').trim();
  const { rows, total, page, pages, view } = listInquiries(q);
  const { shops, assignees, countMap } = listFilterOptions();

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
    <select name="status"><option value="">状態: 全て</option>${Object.entries(STATUSES).map(([k, v]) => opt(k, `${v.label} (${countMap[k] || 0})`, q.status)).join('')}</select>
    <select name="channel"><option value="">チャネル: 全て</option>${Object.entries(CHANNELS).map(([k, v]) => opt(k, v.label, q.channel)).join('')}</select>
    <select name="shop"><option value="">店舗: 全て</option>${shops.map(s => opt(s.id, `${(CHANNELS[s.channel_type] || {}).label || s.channel_type} / ${s.shop_name}`, q.shop)).join('')}</select>
    <select name="assigned"><option value="">担当: 全て</option>${opt('none', '未割当', q.assigned)}${assignees.map(u => opt(u, u, q.assigned)).join('')}</select>
    <label class="chk"><input type="checkbox" name="unread" value="1"${q.unread === '1' ? ' checked' : ''}>未読</label>
    <label class="chk"><input type="checkbox" name="attention" value="1"${q.attention === '1' ? ' checked' : ''}>要確認</label>
    <label class="chk"><input type="checkbox" name="ai" value="1"${q.ai === '1' ? ' checked' : ''}>AIフラグ</label>
    <input type="date" name="from" value="${he(q.from || '')}" title="受信日 From">〜<input type="date" name="to" value="${he(q.to || '')}" title="受信日 To">
    <input type="search" name="q" value="${he(kw)}" placeholder="顧客名/件名/本文/注文番号/商品コード" style="min-width:240px">
    <button class="pri">検索</button>
    <a href="/apps/inquiry-hub?view=${he(view)}" class="ghost btn-link">クリア</a>
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

  // data-label / data-full = スマホでのカード表示用 (CSS table.cardable。PC表示には影響しない)
  const trs = rows.map(r => `
    <tr class="${r.is_unread ? 'unread' : ''}" onclick="location.href='/apps/inquiry-hub/inquiries/${r.id}?view=${view}'">
      <td>${chBadge(r.channel_type)}<div class="sub">${he(r.shop_name)}</div></td>
      <td>${stBadge(r.internal_status)}${r.needs_attention ? ' <span class="badge" style="background:#fee2e2;color:#b91c1c">⚠️要確認</span>' : ''}${r.is_unread ? ' <span class="dot" title="未読"></span>' : ''}</td>
      <td class="subj" data-full><a href="/apps/inquiry-hub/inquiries/${r.id}?view=${view}">${he(r.subject || '(件名なし)')}</a>
        <div class="sub">${he(r.customer_name || '')}${r.customer_identifier ? ' &lt;' + he(r.customer_identifier) + '&gt;' : ''} ・ ${r.msg_count}通</div></td>
      <td data-full data-label="注文 / 商品"${r.order_number || r.product_name || r.product_code ? '' : ' data-empty'}>${r.order_number ? he(r.order_number) : '—'}<div class="sub">${he(r.product_name || r.product_code || '')}</div></td>
      <td data-label="担当"${r.assigned_user_id ? '' : ' data-empty'}>${he(r.assigned_user_id || '—')}</td>
      <td data-label="AI"${r.ai_needed ? '' : ' data-empty'}>${r.ai_needed ? badge(AI_FLAGS[r.ai_needed], null) : '—'}</td>
      <td class="nowrap" data-label="受信">${fmtJst(r.received_at)}<div class="sub">${view === 'sent' ? waitingLabel(r) : `更新 ${fmtJst(r.last_message_at || r.received_at)}`}</div></td>
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

  const emptyMsg = {
    inbox: '受信トレイは空です 🎉 (返信が必要な問い合わせはありません)',
    sent: '返事待ちの案件はありません',
    done: '対応済みの問い合わせはまだありません',
    all: '問い合わせがありません',
  }[view];
  const body = `
  <div class="view-hint">${VIEWS[view].icon} <b>${he(VIEWS[view].label)}</b> — ${he(VIEWS[view].hint)}</div>
  ${filterBar}
  <div class="card">
    <table class="cardable">
      <thead><tr><th>チャネル/店舗</th><th>状態</th><th>件名 / 顧客</th><th>注文 / 商品</th><th>担当</th><th>AI</th><th>${view === 'sent' ? '受信 / 返信待ち' : '受信'}</th></tr></thead>
      <tbody>${trs || `<tr><td colspan="7" class="empty">${he(emptyMsg)}</td></tr>`}</tbody>
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
  })();`;
  res.send(pageShell(`問い合わせ管理 — ${VIEWS[view].label}`, view, body, listScript));
});

// ─── 詳細画面 ───
router.get('/inquiries/:id', (req, res) => {
  const id = Number(req.params.id);
  const detail = getInquiryDetail(id);
  if (!detail) return res.status(404).send(pageShell('問い合わせ管理', DEFAULT_VIEW, '<div class="card empty">問い合わせが見つかりません。<a href="/apps/inquiry-hub">一覧に戻る</a></div>', ''));
  const { inquiry: inq, messages, attachments, notes, logs, draft } = detail;
  // 戻り先のビュー (一覧から ?view= を引き継ぐ。直リンク時は受信トレイ)
  const backView = VIEWS[req.query?.view] ? req.query.view : DEFAULT_VIEW;
  const attByMsg = {};
  for (const a of attachments) (attByMsg[a.inquiry_message_id] = attByMsg[a.inquiry_message_id] || []).push(a);

  // 社内既読化は GET の副作用にせず、表示後にクライアントが POST /read を打つ
  // (Codex R1 medium: プリフェッチ/リンクプレビューでの意図しない既読化と監査ログ欠落の防止)

  const msgHtml = messages.map(m => {
    const atts = (attByMsg[m.id] || []).map(a =>
      `<span class="att" title="取得状態: ${he(a.fetch_status)}">📎 ${he(a.file_name || '(名称不明)')}${a.file_size ? ` (${Math.round(a.file_size / 1024)}KB)` : ''}</span>`).join(' ');
    // Step 1 は text のみ表示 (message_body_html のサニタイズ表示は Gmail 同期実装時に導入)。
    // 取込済みデータには過剰な空行が残っているものがあるため表示時にも正規化する
    // (自動配信メールの空行がそのまま<br>になり、スマホで画面が延々と間延びしていた実測)
    const bodyText = normalizeBodyText(m.message_body_text) || '(本文なし)';
    const lines = bodyText.split('\n');
    // 長文 (メールの自動配信・署名込みの長い問い合わせ) はスレッドを追いやすいよう畳む
    const FOLD_LINES = 14, FOLD_CHARS = 700;
    const folded = lines.length > FOLD_LINES || bodyText.length > FOLD_CHARS;
    const headPart = folded ? lines.slice(0, 8).join('\n').slice(0, 400) : bodyText;
    const restPart = folded ? bodyText.slice(headPart.length) : '';
    const br = s => he(s).replace(/\n/g, '<br>');
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
    <div>${he(n.body).replace(/\n/g, '<br>')}</div></div>`).join('') || '<div class="empty">メモはありません</div>';

  const logHtml = logs.map(l => {
    const b = fmtLogJson(l.before_json);
    const a = fmtLogJson(l.after_json);
    const detail = b != null && a != null ? `${b} → ${a}` : (a != null ? a : '');
    return `<div class="log-row"><span class="msg-date">${fmtJst(l.created_at)}</span> <b>${he(l.user_id || l.actor_type)}</b> ${he(ACTION_LABELS[l.action_type] || l.action_type)} <span class="sub">${he(detail)}</span></div>`;
  }).join('') || '<div class="empty">履歴はありません</div>';

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
  const outboxHtml = outboxJobs.length ? `
    <div class="sub" style="margin-bottom:6px">送信ジョブ履歴 (<a href="/apps/inquiry-hub/admin">⚙️運用管理</a>で解決・取消):</div>
    ${outboxJobs.map(o => `<div class="log-row">${jobBadge(o)} <span class="msg-date">${fmtJst(o.created_at)}</span>
      <span class="sub">${he(String(o.body_text || '').slice(0, 60))}${String(o.body_text || '').length > 60 ? '…' : ''}</span></div>`).join('')}` : '';
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
  const replyPanel = replyEditorEnabled() ? `
      <div class="panel">
        <h3>✉️ 返信を作成</h3>
        ${workerBanner}
        ${outboxHtml}
        ${activeJob
          ? `<div class="sub" style="background:#e0e7ff;border-radius:8px;padding:8px 10px">この問い合わせには未決着の送信ジョブ (#${activeJob.id}) があります。<a href="/apps/inquiry-hub/admin">⚙️運用管理</a>で解決・取消してから新しい返信を作成してください</div>`
          : `<textarea id="replyBody" rows="6" placeholder="返信本文 (テンプレートは📄タブからコピーして貼り付け)"></textarea>
        <div class="row" style="margin-top:8px; justify-content:flex-end">
          <button class="pri" id="replyBtn">内容を確認して送信ジョブを作成</button>
        </div>`}
      </div>` : `
      <div class="panel reply-note">✉️ 返信機能は Step 3 以降で実装 (現在は read-only 運用。返信はメールディーラーから)</div>`;

  const body = `
  <div class="detail-head">
    <a href="/apps/inquiry-hub?view=${he(backView)}">← ${he(VIEWS[backView].label)}に戻る</a>
    <h2>${chBadge(inq.channel_type)} ${he(inq.subject || '(件名なし)')}</h2>
  </div>
  <div class="detail-grid">
    <div class="thread">
      ${msgHtml || '<div class="empty">メッセージがありません</div>'}
      ${replyPanel}
    </div>
    <div class="side">
      <div class="panel">
        <h3>チケット情報</h3>
        <dl>
          <dt>店舗</dt><dd>${he(inq.shop_name)} <span class="sub">(${he(inq.account_identifier)})</span></dd>
          <dt>顧客</dt><dd>${he(inq.customer_name || '—')}${inq.customer_identifier ? `<div class="sub">${he(inq.customer_identifier)}</div>` : ''}</dd>
          <dt>注文番号</dt><dd>${inq.order_number ? he(inq.order_number) : '—'}</dd>
          <dt>商品</dt><dd>${he(inq.product_name || '—')}${inq.product_code ? `<div class="sub">${he(inq.product_code)}</div>` : ''}</dd>
          <dt>モール側状態</dt><dd class="sub" title="外部モール側のステータス (参考表示。同期が上書き)">${he(inq.external_status || '—')}${inq.external_is_read != null ? ` / ${inq.external_is_read ? '既読' : '未読'}` : ''}</dd>
          <dt>最終同期</dt><dd class="sub">${fmtJst(inq.last_external_synced_at)}</dd>
          <dt>受信</dt><dd class="sub">${fmtJst(inq.received_at)}</dd>
        </dl>
      </div>
      <div class="panel">
        <h3>対応状況</h3>
        <label>社内ステータス
          <select id="stSel">${stOptions}</select></label>
        <label>担当者
          <div class="row"><input id="asgInput" type="text" value="${he(inq.assigned_user_id || '')}" placeholder="メールアドレス等">
          <button class="ghost" id="asgMe" type="button">自分</button></div></label>
        <label>AIフラグ <select id="aiSel">${aiOptions}</select></label>
        <label class="chk"><input type="checkbox" id="attnChk"${inq.needs_attention ? ' checked' : ''}>⚠️要確認</label>
        <label class="chk"><input type="checkbox" id="unreadChk"${inq.is_unread ? ' checked' : ''}>未読に戻す</label>
        <button class="pri" id="saveBtn">保存</button>
      </div>
      ${aiPanel}
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
  var CUR = ${JSON.stringify({ status: inq.internal_status, assigned: inq.assigned_user_id || '', ai: inq.ai_needed, attention: !!inq.needs_attention, unread: !!inq.is_unread }).replace(/</g, '\\u003c')};
  var ME = ${JSON.stringify(String(actorOf(req))).replace(/</g, '\\u003c')};
  function post(path, data) {
    return fetch('/apps/inquiry-hub/api/inquiries/' + ID + path, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data)
    }).then(function(r) { return r.json().catch(function(){ return {}; }).then(function(j){ if (!r.ok) throw new Error(j.error || ('HTTP ' + r.status)); return j; }); });
  }
  // 表示したら社内既読化 (GETの副作用にしない。失敗しても表示は継続)
  if (CUR.unread) {
    post('/read', { is_unread: false }).then(function() {
      CUR.unread = false;
      document.getElementById('unreadChk').checked = false;
    }).catch(function() {});
  }
  document.getElementById('asgMe').addEventListener('click', function() { document.getElementById('asgInput').value = ME; });
  document.getElementById('saveBtn').addEventListener('click', function() {
    var btn = this; btn.disabled = true;
    var ops = [];
    var st = document.getElementById('stSel').value;
    var asg = document.getElementById('asgInput').value.trim();
    var ai = Number(document.getElementById('aiSel').value);
    var attn = document.getElementById('attnChk').checked;
    var unread = document.getElementById('unreadChk').checked;
    if (st !== CUR.status) ops.push(post('/status', { status: st }));
    if (asg !== CUR.assigned) ops.push(post('/assign', { user: asg }));
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
  var replyBtn = document.getElementById('replyBtn');
  if (replyBtn) replyBtn.addEventListener('click', function() {
    var body = document.getElementById('replyBody').value.trim();
    if (!body) { toast('本文が空です'); return; }
    var preview = body.length > 300 ? body.slice(0, 300) + '…' : body;
    if (!confirm('以下の内容で送信ジョブを作成しますか?\\n\\n宛先: ' + REPLY_CH + ' の顧客\\n\\n' + preview)) return;
    replyBtn.disabled = true;
    post('/reply', { body: body, clientOperationId: REPLY_OP_ID, baseConversationRev: REPLY_BASE_REV })
      .then(function(r) { toast(r.duplicate ? '既に同じ操作で作成済みです' : '送信ジョブを作成しました'); setTimeout(function(){ location.reload(); }, 900); })
      .catch(function(e) { toast('作成失敗: ' + e.message); replyBtn.disabled = false; });
  });`;

  res.send(pageShell(`問い合わせ — ${inq.subject || inq.external_inquiry_id}`, backView, body, script));
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
    if (!req.is('application/json')) return res.status(415).json({ error: 'Content-Type は application/json が必要です' });
  }
  next();
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
router.post('/api/inquiries/:id/reply', (req, res) => {
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
  try {
    const r = createReplyJob({
      inquiryId: inq.id, channelType: inq.channel_type, bodyText: body,
      createdBy: actorOf(req), clientOperationId: opId, baseConversationRev: baseRev,
    });
    if (r.conflict) return res.status(409).json({ error: r.conflict });
    res.json({ ok: true, id: r.id, duplicate: !r.created });
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
};

router.get('/mail-rules', (req, res) => {
  const rules = listMailRules();
  const fmtConds = (r) => {
    let conds;
    try { conds = JSON.parse(r.conditions_json); } catch { return '(解析不能)'; }
    const glue = r.match_mode === 'any' ? ' または ' : ' かつ ';
    return conds.map(c => `${RULE_FIELD_LABELS[c.field] || c.field}が「${he(c.value)}」${RULE_OP_LABELS[c.op] || c.op}`).join(glue);
  };
  const trs = rules.map(r => {
    const meta = RULE_ACTION_LABELS[r.action] || { label: r.action, style: '' };
    return `
    <tr data-search="${he((r.name || '') + ' ' + fmtConds(r)).toLowerCase()}"${r.is_active ? '' : ' style="opacity:.5"'}>
      <td class="nowrap" data-label="優先度">${r.priority}</td>
      <td data-full>${he(r.name || '—')}${r.external_key ? '<div class="sub">メールディーラー移行</div>' : '<div class="sub">手動追加</div>'}</td>
      <td style="overflow-wrap:anywhere" data-full data-label="条件">${fmtConds(r)}</td>
      <td data-label="アクション"><span class="badge" style="${meta.style}">${he(meta.label)}</span></td>
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
        <select id="nAction"><option value="skip">🗑️取り込まない</option><option value="import_done">✅取込+完了扱い</option></select>
        <button class="pri" onclick="addRule(this)">追加</button>
      </div>
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
    document.getElementById('testResult').textContent = r.match
      ? '→ ルール#' + r.match.ruleId + (r.match.ruleName ? ' (' + r.match.ruleName + ')' : '') + ' に一致: ' + (r.match.action === 'skip' ? '🗑️取り込まない' : '✅取込+完了扱い')
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
});`;
  res.send(pageShell('問い合わせ管理 — メールルール', 'mailrules', body, script));
});

router.post('/api/mail-rules', (req, res) => {
  try {
    const b = req.body || {};
    res.json({ ok: true, ...addMailRule({ name: b.name, matchMode: b.matchMode, conditions: b.conditions, action: b.action, priority: Number(b.priority) }) });
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

  const body = `
  ${aiCard}
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
    const r = await runSync(shop.id, adapter);
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
.detail-head h2 { margin: 8px 0 12px; font-size: 18px; }
.detail-grid { display: grid; grid-template-columns: 1fr 360px; gap: 16px; align-items: start; }
@media (max-width: 900px) { .detail-grid { grid-template-columns: 1fr; } }
.thread { display: flex; flex-direction: column; gap: 10px; }
.msg { background: #fff; border-radius: 12px; padding: 10px 14px; box-shadow: 0 1px 3px rgba(0,0,0,.08); border-left: 4px solid #94a3b8; }
.msg.in { border-left-color: #f59e0b; }
.msg.out { border-left-color: #1d4ed8; background: #eff6ff; }
.msg-head { display: flex; gap: 8px; align-items: baseline; margin-bottom: 6px; flex-wrap: wrap; }
.msg-date { color: #94a3b8; font-size: 12px; }
.msg-body { white-space: normal; line-height: 1.7; overflow-wrap: anywhere; }
/* 長文メール (自動配信・署名込み) はスレッドを追いやすいよう畳む */
.msg-body details.more > summary { cursor: pointer; color: #1d4ed8; font-size: 13px; margin-top: 6px;
  list-style: none; padding: 4px 0; }
.msg-body details.more > summary::-webkit-details-marker { display: none; }
.msg-body details.more[open] > summary { color: #64748b; }
.msg-atts { margin-top: 8px; }
.att { display: inline-block; background: #f1f5f9; border-radius: 8px; padding: 3px 8px; font-size: 12px; margin-right: 6px; }
.panel { background: #fff; border-radius: 12px; padding: 12px 14px; box-shadow: 0 1px 3px rgba(0,0,0,.08); margin-bottom: 12px; }
.panel h3 { margin: 0 0 10px; font-size: 14px; }
.panel dl { margin: 0; display: grid; grid-template-columns: 90px 1fr; gap: 6px 8px; }
.panel dt { color: #64748b; font-size: 12px; padding-top: 2px; }
.panel dd { margin: 0; overflow-wrap: anywhere; }
.panel label { display: block; margin-bottom: 8px; font-size: 13px; color: #334155; }
.panel select, .panel input[type=text], .panel textarea { width: 100%; padding: 6px 8px; border: 1px solid #cbd5e1; border-radius: 8px; font-size: 13px; margin-top: 2px; }
.panel .row { display: flex; gap: 6px; }
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

/* ═══ Gmail風レイアウト: 左サイドバー + 本文 ═══ */
.layout { display: flex; align-items: flex-start; }
.side-nav { width: 210px; flex: 0 0 210px; padding: 12px 8px; position: sticky; top: 0;
  max-height: 100vh; overflow-y: auto; }
.nav-group { display: flex; flex-direction: column; gap: 2px; }
.nav-sep { height: 1px; background: #dbe3ee; margin: 10px 12px; }
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

  /* 詳細画面 */
  .detail-head h2 { font-size: 16px; }
  .msg { padding: 10px 12px; }
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
 * active: 'inbox'|'sent'|'done'|'all'|'templates'|'qa'|'mailrules'|'admin'
 * PCではサイドバー常時表示、スマホでは ☰ で開くドロワー
 */
function pageShell(title, active, body, script) {
  // 受信箱の件数はサイドバーに出す (Gmailの「受信トレイ (12)」と同じ位置づけ)。
  // 一覧以外の画面でも出すが、失敗しても画面は出す (fail-soft)
  let counts = {};
  try { counts = countByView(); } catch { /* 初期化前などは件数なしで表示 */ }
  const navItem = (href, icon, label, key, count) => `
    <a href="${href}" class="nav-item${active === key ? ' on' : ''}">
      <span class="nav-icon">${icon}</span><span class="nav-label">${label}</span>
      ${count ? `<span class="nav-count">${count}</span>` : ''}
    </a>`;
  const sidebar = `
  <aside class="side-nav" id="sideNav">
    <div class="nav-group">
      ${navItem('/apps/inquiry-hub?view=inbox', '📥', '受信トレイ', 'inbox', counts.inbox)}
      ${navItem('/apps/inquiry-hub?view=sent', '📤', '送信済み', 'sent', counts.sent)}
      ${navItem('/apps/inquiry-hub?view=done', '✅', '対応済み', 'done', 0)}
      ${navItem('/apps/inquiry-hub?view=all', '🗂️', 'すべて', 'all', 0)}
    </div>
    <div class="nav-sep"></div>
    <div class="nav-group">
      ${navItem('/apps/inquiry-hub/templates', '📄', '返信テンプレート', 'templates', 0)}
      ${navItem('/apps/inquiry-hub/qa', '❓', 'Q&amp;A', 'qa', 0)}
      ${navItem('/apps/inquiry-hub/mail-rules', '📧', 'メールルール', 'mailrules', 0)}
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
