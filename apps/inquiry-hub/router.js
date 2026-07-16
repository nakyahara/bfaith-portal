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
import { Router } from 'express';
import { getDB, logActivity } from './db.js';
import { CHANNELS, STATUSES, AI_FLAGS, PAGE_SIZE, listInquiries, listFilterOptions, getInquiryDetail } from './queries.js';

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

const badge = (meta, text) => meta ? `<span class="badge" style="${meta.badge}">${he(text != null ? text : meta.label)}</span>` : '';
const chBadge = ch => badge(CHANNELS[ch], null) || he(ch);
const stBadge = st => badge(STATUSES[st], null) || he(st);

// ─── 一覧画面 ───
router.get('/', (req, res) => {
  const q = req.query || {};
  const kw = String(q.q || '').trim();
  const { rows, total, page, pages } = listInquiries(q);
  const { shops, assignees, countMap } = listFilterOptions();

  const opt = (v, label, cur) => `<option value="${he(v)}"${String(cur || '') === String(v) ? ' selected' : ''}>${he(label)}</option>`;
  const filterBar = `
  <form method="get" class="filters">
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
    <a href="/apps/inquiry-hub" class="ghost btn-link">クリア</a>
  </form>`;

  const trs = rows.map(r => `
    <tr class="${r.is_unread ? 'unread' : ''}" onclick="location.href='/apps/inquiry-hub/inquiries/${r.id}'">
      <td>${chBadge(r.channel_type)}<div class="sub">${he(r.shop_name)}</div></td>
      <td>${stBadge(r.internal_status)}${r.needs_attention ? ' <span class="badge" style="background:#fee2e2;color:#b91c1c">⚠️要確認</span>' : ''}${r.is_unread ? ' <span class="dot" title="未読"></span>' : ''}</td>
      <td class="subj"><a href="/apps/inquiry-hub/inquiries/${r.id}">${he(r.subject || '(件名なし)')}</a>
        <div class="sub">${he(r.customer_name || '')}${r.customer_identifier ? ' &lt;' + he(r.customer_identifier) + '&gt;' : ''} ・ ${r.msg_count}通</div></td>
      <td>${r.order_number ? he(r.order_number) : '—'}<div class="sub">${he(r.product_name || r.product_code || '')}</div></td>
      <td>${he(r.assigned_user_id || '—')}</td>
      <td>${r.ai_needed ? badge(AI_FLAGS[r.ai_needed], null) : '—'}</td>
      <td class="nowrap">${fmtJst(r.received_at)}<div class="sub">更新 ${fmtJst(r.last_message_at || r.received_at)}</div></td>
    </tr>`).join('');

  const pageLink = p => {
    const u = new URLSearchParams(Object.entries(q).filter(([, v]) => v !== '' && v != null));
    u.set('page', p);
    return `/apps/inquiry-hub?${u.toString()}`;
  };
  const pager = pages > 1 ? `<div class="pager">
    ${page > 1 ? `<a href="${he(pageLink(page - 1))}">← 前</a>` : ''}
    <span>${page} / ${pages} ページ (全${total}件)</span>
    ${page < pages ? `<a href="${he(pageLink(page + 1))}">次 →</a>` : ''}</div>` : `<div class="pager"><span>全${total}件</span></div>`;

  const body = `
  ${filterBar}
  <div class="card">
    <table>
      <thead><tr><th>チャネル/店舗</th><th>状態</th><th>件名 / 顧客</th><th>注文 / 商品</th><th>担当</th><th>AI</th><th>受信</th></tr></thead>
      <tbody>${trs || '<tr><td colspan="7" class="empty">問い合わせがありません (同期実装前は手動投入データのみ表示されます)</td></tr>'}</tbody>
    </table>
    ${pager}
  </div>`;
  res.send(pageShell('問い合わせ管理 — 一覧', 'list', body, ''));
});

// ─── 詳細画面 ───
router.get('/inquiries/:id', (req, res) => {
  const id = Number(req.params.id);
  const detail = getInquiryDetail(id);
  if (!detail) return res.status(404).send(pageShell('問い合わせ管理', '', '<div class="card empty">問い合わせが見つかりません。<a href="/apps/inquiry-hub">一覧に戻る</a></div>', ''));
  const { inquiry: inq, messages, attachments, notes, logs, draft } = detail;
  const attByMsg = {};
  for (const a of attachments) (attByMsg[a.inquiry_message_id] = attByMsg[a.inquiry_message_id] || []).push(a);

  // 社内既読化は GET の副作用にせず、表示後にクライアントが POST /read を打つ
  // (Codex R1 medium: プリフェッチ/リンクプレビューでの意図しない既読化と監査ログ欠落の防止)

  const msgHtml = messages.map(m => {
    const atts = (attByMsg[m.id] || []).map(a =>
      `<span class="att" title="取得状態: ${he(a.fetch_status)}">📎 ${he(a.file_name || '(名称不明)')}${a.file_size ? ` (${Math.round(a.file_size / 1024)}KB)` : ''}</span>`).join(' ');
    // Step 1 は text のみ表示 (message_body_html のサニタイズ表示は Gmail 同期実装時に導入)
    const bodyText = m.message_body_text || '(本文なし)';
    return `
    <div class="msg ${m.is_incoming ? 'in' : 'out'}">
      <div class="msg-head">
        <b>${m.is_incoming ? '👤 ' : '🏪 '}${he(m.sender_name || (m.is_incoming ? '顧客' : '店舗'))}</b>
        ${m.sender_type === 'system' ? '<span class="badge" style="background:#f1f5f9;color:#64748b">system</span>' : ''}
        <span class="msg-date">${fmtJst(m.received_at || m.sent_at || m.created_at)}${m.is_incoming ? '' : m.sent_by_user_id ? ` ・ 送信者: ${he(m.sent_by_user_id)}` : ''}</span>
      </div>
      <div class="msg-body">${he(bodyText).replace(/\n/g, '<br>')}</div>
      ${atts ? `<div class="msg-atts">${atts}</div>` : ''}
    </div>`;
  }).join('');

  const noteHtml = notes.map(n => `
    <div class="note"><div class="note-head"><b>${he(n.user_id)}</b> <span class="msg-date">${fmtJst(n.created_at)}</span></div>
    <div>${he(n.body).replace(/\n/g, '<br>')}</div></div>`).join('') || '<div class="empty">メモはありません</div>';

  const ACTION_LABELS = {
    status_change: '状態変更', assign: '担当変更', note_add: 'メモ追加', read_toggle: '既読/未読',
    attention_toggle: '要確認フラグ', ai_flag: 'AIフラグ', seed: 'テストデータ投入',
  };
  const logHtml = logs.map(l => {
    let detail = '';
    try {
      const b = l.before_json ? JSON.parse(l.before_json) : null;
      const a = l.after_json ? JSON.parse(l.after_json) : null;
      if (b != null && a != null) detail = `${JSON.stringify(b)} → ${JSON.stringify(a)}`;
      else if (a != null) detail = JSON.stringify(a);
    } catch { /* 表示用なので壊れたJSONは無視 */ }
    return `<div class="log-row"><span class="msg-date">${fmtJst(l.created_at)}</span> <b>${he(l.user_id || l.actor_type)}</b> ${he(ACTION_LABELS[l.action_type] || l.action_type)} <span class="sub">${he(detail)}</span></div>`;
  }).join('') || '<div class="empty">履歴はありません</div>';

  const stOptions = Object.entries(STATUSES).map(([k, v]) => `<option value="${k}"${inq.internal_status === k ? ' selected' : ''}>${he(v.label)}</option>`).join('');
  const aiOptions = Object.entries(AI_FLAGS).map(([k, v]) => `<option value="${k}"${String(inq.ai_needed) === k ? ' selected' : ''}>${he(k === '0' ? 'AI不要' : v.label)}</option>`).join('');

  const aiPanel = draft ? `
    <div class="panel">
      <h3>🤖 AI返信案 ${draft.is_stale ? '<span class="badge" style="background:#fef3c7;color:#92400e">⚠️古い会話に基づく返信案</span>' : ''}</h3>
      ${draft.summary ? `<div class="sub">要約: ${he(draft.summary)}</div>` : ''}
      <div class="ai-draft">${he(draft.draft_body || '').replace(/\n/g, '<br>')}</div>
      ${draft.notes ? `<div class="sub">注意: ${he(draft.notes)}</div>` : ''}
    </div>` : '';

  const body = `
  <div class="detail-head">
    <a href="/apps/inquiry-hub">← 一覧に戻る</a>
    <h2>${chBadge(inq.channel_type)} ${he(inq.subject || '(件名なし)')}</h2>
  </div>
  <div class="detail-grid">
    <div class="thread">
      ${msgHtml || '<div class="empty">メッセージがありません</div>'}
      <div class="panel reply-note">✉️ 返信機能は Step 3 以降で実装 (現在は read-only 運用。返信はメールディーラーから)</div>
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
  });`;

  res.send(pageShell(`問い合わせ — ${inq.subject || inq.external_inquiry_id}`, '', body, script));
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

// ─── ページシェル ───
const CSS = `
* { box-sizing: border-box; }
body { margin: 0; font-family: -apple-system, "Segoe UI", "Hiragino Sans", "Noto Sans JP", sans-serif; background: #f1f5f9; color: #0f172a; font-size: 14px; }
header.app { background: #0f172a; color: #fff; padding: 10px 16px; display: flex; align-items: center; gap: 16px; flex-wrap: wrap; }
header.app h1 { font-size: 16px; margin: 0; }
header.app .back { color: #94a3b8; text-decoration: none; margin-left: auto; font-size: 13px; }
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
`;

function pageShell(title, active, body, script) {
  const tab = (href, label, key) => `<a href="${href}" style="color:${active === key ? '#fff' : '#94a3b8'};text-decoration:none;font-size:13px">${label}</a>`;
  return `<!DOCTYPE html><html lang="ja"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>${he(title)}</title><style>${CSS}</style></head>
<body>
<header class="app">
  <h1>💬 問い合わせ管理</h1>
  <nav style="display:flex;gap:14px">
    ${tab('/apps/inquiry-hub', '一覧', 'list')}
  </nav>
  <a href="/" class="back">← ポータルに戻る</a>
</header>
<div class="wrap">${body}</div>
<div id="toast"></div>
<script>
function toast(msg) {
  var t = document.getElementById('toast');
  t.textContent = msg; t.style.display = 'block';
  clearTimeout(t._h); t._h = setTimeout(function(){ t.style.display = 'none'; }, 2800);
}
${script || ''}
</script>
</body></html>`;
}

export default router;
