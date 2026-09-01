/**
 * inquiry-hub 返品・交換案件 (2026-09-01 中原さん要望)
 *
 * 正本 = AI_reference『システム設計\返品交換案件管理_要件定義_20260901.md』
 *
 * ⭐これは何のためのものか:
 *   「顧客への返信は終わったのに、返金と代品の手配だけが残っている」案件を、
 *   受信箱から切り離して追いかける。放置に気づくきっかけが**顧客からの催促**になっている状態をやめる。
 *
 * ⭐設計の芯 (ここを外すと全部壊れる):
 *   1. 状態は3軸に分ける (stage / waiting_on / assigned_user_id)。1つのステータスに混ぜない
 *   2. 問い合わせの完了と案件の完了は完全に分離する
 *      (一緒に閉じる→返金忘れ / 案件完了まで閉じられない→受信箱が詰まる。どちらも起きる)
 *   3. 在庫・受注・モールの返金を複製しない。持つのは「やった/やってない/不要/例外」と誰がいつ確認したか
 *   4. 自動で案件化しない・自動で完了にしない。キーワードは**候補提示まで**
 *      (「返品できますか？」という質問まで案件になると、ボードが死ぬ)
 *   5. カンバンを工程データの編集器にしない。列は工程の状態から決まる
 */
import { getDB } from './db.js';
import { stripQuoted, normalizeForMatch } from './text-utils.js';
import { hasPermission } from './staff.js';

/**
 * 「必要と決まっていた工程を対応不要にする」「未処理を残したまま完了する」に要る権限。
 * ⭐既存の権限マップ (staff.js BUILTIN_PERMISSIONS) の D3「高額・例外・規約外を決める」を使う。
 *   新しい権限を増やさない — 権限が増えるほど、誰も付与されていない権限が生まれるため
 */
export const EXCEPTION_PERMISSION = 'D3';

/**
 * 例外操作をしてよいか。
 *
 * ⭐担当者が**一度も登録されていない**とき (null) だけ通す + 呼び元が画面に注意を出す。
 *   導入直後に誰も操作できず業務が止まるのを避けるため。
 * ⚠️「全員を無効化した」は未導入と区別する (無効の行が残っていれば通さない)。
 *   未導入のまま通した例外操作は履歴に印を残す (bootstrap: true)
 */
export function canDoException(actor) {
  const r = hasPermission(actor, EXCEPTION_PERMISSION);
  return r === null
    ? { allowed: true, unmanaged: true }
    : { allowed: r === true, unmanaged: false };
}

// ─────────────────────────────────────────────────────────
// 定数 (画面表示は全部ここから引く。内部コードは画面に出さない)
// ─────────────────────────────────────────────────────────

/** 案件種別。選ぶと工程テンプレートが決まる */
export const CASE_TYPES = {
  RETURN_REFUND: { label: '返品・返金', hint: '商品を引き取って (または廃棄してもらって) お金を返す', badge: 'background:#fee2e2;color:#b91c1c' },
  EXCHANGE:      { label: '交換・代品発送', hint: '同じ商品を送り直す', badge: 'background:#dbeafe;color:#1d4ed8' },
  MANUFACTURER:  { label: 'メーカー対応', hint: '仕入先・メーカーに調査や代品を頼む', badge: 'background:#ede9fe;color:#6d28d9' },
  OTHER:         { label: 'その他', hint: '工程は自分で足す', badge: 'background:#f1f5f9;color:#475569' },
};

/**
 * 工程 (stage)。⭐**厳密な順序ではなく「現在の代表工程」**。
 * 返送不要 (顧客廃棄)・メーカー直送で枝分かれするので、一本道の状態機械にはしない。
 * ボードの「処理工程」ビューの列にもなる
 */
export const STAGES = {
  RECEIVED:          { label: '受付' },
  COLLECTING_INFO:   { label: '情報収集' },
  ASSESSING:         { label: '判定' },
  RETURN_IN_TRANSIT: { label: '返送・検品' },
  ARRANGING:         { label: '手配' },
  FOLLOWING_UP:      { label: '結果確認' },
  COMPLETED:         { label: '完了' },
  CANCELED:          { label: '取下げ' },
};

/**
 * いま誰待ちか。⭐**ボードの既定の列**。
 * stage を既定にしない理由 = 工程が枝分かれするから。
 * どんなに枝分かれした案件でも「顧客待ち / メーカー待ち」には必ず置ける
 */
export const WAITING_ON = {
  SELF:            { label: '自社対応', short: '自社', badge: 'background:#dbeafe;color:#1d4ed8' },
  CUSTOMER:        { label: '顧客待ち', short: '顧客', badge: 'background:#fef3c7;color:#92400e' },
  SUPPLIER:        { label: 'メーカー・仕入先待ち', short: 'メーカー', badge: 'background:#fef3c7;color:#92400e' },
  CARRIER:         { label: '配送業者待ち', short: '配送業者', badge: 'background:#fef3c7;color:#92400e' },
  MARKETPLACE:     { label: 'モール待ち', short: 'モール', badge: 'background:#fef3c7;color:#92400e' },
  SCHEDULED_EVENT: { label: '指定日待ち', short: '指定日', badge: 'background:#e0e7ff;color:#4338ca' },
  NONE:            { label: '完了', short: '完了', badge: 'background:#dcfce7;color:#166534' },
};

/** 外部待ちにしたときの既定の期限 (営業日)。⭐期限を空欄にはさせない */
export const DEFAULT_DUE_BUSINESS_DAYS = {
  SELF: 1, CUSTOMER: 3, SUPPLIER: 2, CARRIER: 2, MARKETPLACE: 3, SCHEDULED_EVENT: 5, NONE: null,
};

/** 「自社対応」以外は外部待ち = 次回確認日が必須になる */
export const EXTERNAL_WAITING = ['CUSTOMER', 'SUPPLIER', 'CARRIER', 'MARKETPLACE', 'SCHEDULED_EVENT'];

/** 工程の「必要かどうか」 */
export const NECESSITY = {
  undecided:    { label: '要否を判断', badge: 'background:#fff;color:#475569;border:1px dashed #94a3b8' },
  required:     { label: '必要',       badge: 'background:#f1f5f9;color:#475569' },
  not_required: { label: '対応不要',   badge: 'background:#f1f5f9;color:#94a3b8' },
};

/** 工程の「進み具合」。⭐画面に 'required' を「必須」と出さない (「未着手」と出す) */
export const PROGRESS = {
  not_started: { label: '未着手',        badge: 'background:#f1f5f9;color:#475569' },
  in_progress: { label: '対応中',        badge: 'background:#dbeafe;color:#1d4ed8' },
  waiting:     { label: '回答・到着待ち', badge: 'background:#fef3c7;color:#92400e' },
  completed:   { label: '完了',          badge: 'background:#dcfce7;color:#166534' },
  exception:   { label: '例外終了',      badge: 'background:#ede9fe;color:#6d28d9' },
};

/** 例外完了の理由 (少数に絞る。自由記述は close_note へ) */
export const CLOSE_REASONS = {
  customer_unreachable: '顧客と連絡が取れない',
  customer_declined:    '顧客が対応継続を希望しない',
  no_maker_response:    'メーカー回答を得られない',
  handled_elsewhere:    'システム外で対応済み',
  created_by_mistake:   '誤って作成した案件',
  other:                'その他',
};

/**
 * 工程テンプレート。案件種別を選ぶと、この順で case_steps が作られる。
 *
 * necessity: 'required' = 最初から必要 / 'undecided' = 要否を人が決める
 *   ⭐「返送不要で廃棄してもらう」「メーカーが客先へ直送する」という実務の分かれ道を
 *     テンプレートに持たせて、使わなかった工程は「対応不要」で消す。
 *     テンプレートに無い工程を毎回手で足させると、現場は使わなくなる
 * party: 待ち先の表示 (誰が動くか)。⭐社内の assignee とは別 — メーカーを担当者欄に入れない
 * days: 期限の目安 (営業日)。作成時は「次回確認日」を全工程の初期期限にはせず、
 *   最初の未完了工程にだけ入れる (期限だらけにすると意味を失うため)
 */
export const STEP_TEMPLATES = {
  RETURN_REFUND: [
    { code: 'confirm_request',           name: '返品・返金の受付内容を確認', necessity: 'required',  party: '自社',   days: 0 },
    { code: 'decide_return_requirement', name: '返送が必要か判断',           necessity: 'required',  party: '自社',   days: 0 },
    { code: 'send_return_instructions',  name: '返送方法を顧客へ案内',       necessity: 'undecided', party: '自社',   days: 1 },
    { code: 'wait_return_arrival',       name: '返送品の到着を確認',         necessity: 'undecided', party: '顧客',   days: 7 },
    { code: 'inspect_returned_item',     name: '返送品の状態を確認',         necessity: 'undecided', party: '倉庫',   days: 8 },
    { code: 'record_disposal_consent',   name: '返送不要・顧客廃棄を確認',   necessity: 'undecided', party: '自社',   days: 1 },
    { code: 'execute_refund',            name: '返金処理を実施',             necessity: 'required',  party: '自社',   days: 2 },
    { code: 'confirm_refund_result',     name: '返金実績を確認',             necessity: 'required',  party: '自社',   days: 3 },
    { code: 'notify_customer_completion', name: '顧客へ完了を連絡',          necessity: 'required',  party: '自社',   days: 3 },
  ],
  EXCHANGE: [
    { code: 'confirm_exchange_request',    name: '交換内容を確認',             necessity: 'required',  party: '自社',     days: 0 },
    { code: 'confirm_replacement_stock',   name: '交換品の手配可否を確認',     necessity: 'required',  party: '倉庫',     days: 1 },
    { code: 'decide_return_requirement',   name: '不具合品の返送要否を判断',   necessity: 'required',  party: '自社',     days: 1 },
    { code: 'send_return_instructions',    name: '返送方法を顧客へ案内',       necessity: 'undecided', party: '自社',     days: 1 },
    { code: 'arrange_replacement_shipment', name: '交換品の発送を手配',        necessity: 'required',  party: '倉庫',     days: 2 },
    { code: 'record_replacement_tracking', name: '交換品の送り状番号を記録',   necessity: 'required',  party: '倉庫',     days: 2 },
    { code: 'wait_replacement_delivery',   name: '交換品の配達完了を確認',     necessity: 'required',  party: '配送業者', days: 5 },
    { code: 'wait_return_arrival',         name: '返送品の到着を確認',         necessity: 'undecided', party: '顧客',     days: 7 },
    { code: 'confirm_customer_receipt',    name: '顧客の受取・問題解消を確認', necessity: 'required',  party: '自社',     days: 6 },
    { code: 'notify_customer_completion',  name: '交換対応の完了を連絡',       necessity: 'required',  party: '自社',     days: 6 },
  ],
  MANUFACTURER: [
    { code: 'collect_evidence',                name: '写真・症状・ロット番号を揃える', necessity: 'required',  party: '自社／顧客',     days: 2 },
    { code: 'contact_manufacturer',            name: 'メーカーへ調査を依頼',           necessity: 'required',  party: '自社',           days: 2 },
    { code: 'wait_manufacturer_response',      name: 'メーカーの回答を確認',           necessity: 'required',  party: 'メーカー',       days: 5 },
    { code: 'decide_resolution',               name: '対応方法を決定',                 necessity: 'required',  party: '自社',           days: 6 },
    { code: 'arrange_manufacturer_direct_ship', name: 'メーカー直送を手配',            necessity: 'undecided', party: 'メーカー',       days: 8 },
    { code: 'record_manufacturer_tracking',    name: 'メーカー直送の送り状番号を確認', necessity: 'undecided', party: 'メーカー',       days: 8 },
    { code: 'wait_replacement_delivery',       name: '代品の配達完了を確認',           necessity: 'undecided', party: '配送業者',       days: 11 },
    { code: 'arrange_customer_disposal',       name: '不具合品の廃棄を顧客へ案内',     necessity: 'undecided', party: '自社',           days: 7 },
    { code: 'wait_return_to_manufacturer',     name: '不具合品のメーカー返送を確認',   necessity: 'undecided', party: '顧客／メーカー', days: 14 },
    { code: 'confirm_customer_resolution',     name: '顧客の受取・問題解消を確認',     necessity: 'required',  party: '自社',           days: 12 },
    { code: 'notify_customer_completion',      name: '対応完了を顧客へ連絡',           necessity: 'required',  party: '自社',           days: 12 },
  ],
  OTHER: [
    { code: 'confirm_request',            name: '対応内容を確認',       necessity: 'required', party: '自社', days: 0 },
    { code: 'decide_resolution',          name: '対応方法を決定',       necessity: 'required', party: '自社', days: 1 },
    { code: 'notify_customer_completion', name: '顧客へ結果を連絡',     necessity: 'required', party: '自社', days: 2 },
  ],
};

/** 工程テンプレートから見た「代表工程 (stage)」。工程を完了したときに案件の stage を引き上げる */
const STEP_STAGE = {
  confirm_request: 'RECEIVED', confirm_exchange_request: 'RECEIVED',
  collect_evidence: 'COLLECTING_INFO',
  decide_return_requirement: 'ASSESSING', confirm_replacement_stock: 'ASSESSING',
  contact_manufacturer: 'ASSESSING', wait_manufacturer_response: 'ASSESSING', decide_resolution: 'ASSESSING',
  send_return_instructions: 'RETURN_IN_TRANSIT', wait_return_arrival: 'RETURN_IN_TRANSIT',
  inspect_returned_item: 'RETURN_IN_TRANSIT', record_disposal_consent: 'RETURN_IN_TRANSIT',
  wait_return_to_manufacturer: 'RETURN_IN_TRANSIT',
  execute_refund: 'ARRANGING', arrange_replacement_shipment: 'ARRANGING',
  record_replacement_tracking: 'ARRANGING', arrange_manufacturer_direct_ship: 'ARRANGING',
  record_manufacturer_tracking: 'ARRANGING', arrange_customer_disposal: 'ARRANGING',
  confirm_refund_result: 'FOLLOWING_UP', wait_replacement_delivery: 'FOLLOWING_UP',
  confirm_customer_receipt: 'FOLLOWING_UP', confirm_customer_resolution: 'FOLLOWING_UP',
  notify_customer_completion: 'FOLLOWING_UP',
};

/** 工程の待ち先 → 案件の waiting_on。工程を「回答・到着待ち」にしたとき案件の列が決まる */
const PARTY_TO_WAITING = {
  '自社': 'SELF', '倉庫': 'SELF', '自社／顧客': 'CUSTOMER', '顧客': 'CUSTOMER',
  'メーカー': 'SUPPLIER', '顧客／メーカー': 'SUPPLIER', '配送業者': 'CARRIER', 'モール': 'MARKETPLACE',
};

// ─────────────────────────────────────────────────────────
// 日付 (JST)
// ─────────────────────────────────────────────────────────

const JST_OFFSET_MS = 9 * 60 * 60 * 1000;

/**
 * UTC ISO → JST の 'YYYY-MM-DD' (⭐toISOString をそのまま使うと日付が1日ずれる)。
 * ⭐**壊れた値でも例外を投げない** — 手で書き換えられたデータが1行あるだけで
 *   ボードや詳細画面が真っ白になる、という壊れ方をさせない (fail-soft)
 */
export function jstDate(iso) {
  if (!iso) return '';
  const t = Date.parse(iso);
  if (Number.isNaN(t)) return '';
  try {
    return new Date(t + JST_OFFSET_MS).toISOString().slice(0, 10);
  } catch { return ''; }   // 範囲外の日時 (RangeError)
}

/**
 * JST の 'YYYY-MM-DD' → その日の 09:00 JST を表す UTC ISO ('YYYY-MM-DDT00:00:00Z')。
 * ⭐形式だけでなく**実在する日付か**も見る — '2026-02-31' は 3/3 に繰り上がって
 *   別の日として保存されてしまう (APIを直に叩かれたときの入口を塞ぐ)
 */
export function jstDateToIso(ymd) {
  const s = String(ymd || '');
  if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) return null;
  const t = Date.parse(`${s}T09:00:00+09:00`);
  if (Number.isNaN(t)) return null;
  const iso = new Date(t).toISOString().slice(0, 19) + 'Z';
  return jstDate(iso) === s ? iso : null;   // 繰り上がったら不正な日付
}

/** 今から n 営業日後 (土日を飛ばす。祝日は見ない — §11「高度なカレンダーは作らない」) の JST 日付 */
export function businessDaysFromNow(days, now = new Date()) {
  const d = new Date(now.getTime() + JST_OFFSET_MS);
  let left = Math.max(0, Number(days) || 0);
  while (left > 0) {
    d.setUTCDate(d.getUTCDate() + 1);
    const dow = d.getUTCDay();
    if (dow !== 0 && dow !== 6) left--;
  }
  // 週末に着地したら翌営業日へ寄せる (0営業日後の指定でも土日は避ける)
  while (d.getUTCDay() === 0 || d.getUTCDay() === 6) d.setUTCDate(d.getUTCDate() + 1);
  return d.toISOString().slice(0, 10);
}

/**
 * 期限からの超過日数。未超過なら 0。
 * ⭐**JST の日付どうしで比べる** — 時刻差で計算すると、次回確認日の当日 9:00 を過ぎただけで
 *   「1日超過」と赤くなる。当日はまだ超過ではない (その日のうちにやればいい)
 */
export function overdueDays(nextActionAt, now = new Date()) {
  if (!nextActionAt) return 0;
  const due = jstDate(nextActionAt);
  const today = jstDate(now.toISOString());
  if (!due || !today || due >= today) return 0;
  return Math.round((Date.parse(today + 'T00:00:00Z') - Date.parse(due + 'T00:00:00Z')) / 86400000);
}

/** 滞留日数 (waiting_since から今まで) */
export function stagnantDays(waitingSince, now = new Date()) {
  const t = Date.parse(waitingSince || '');
  if (Number.isNaN(t)) return 0;
  return Math.floor((now.getTime() - t) / 86400000);
}

const nowIso = () => new Date().toISOString().slice(0, 19) + 'Z';

// ─────────────────────────────────────────────────────────
// 案件化の候補検知 (⭐候補を出すだけ。自動では案件にしない)
// ─────────────────────────────────────────────────────────

/**
 * 案件になりそうな問い合わせのキーワード。
 * ⭐締め前確認 (cutoff.js) と同じ考え方 — 決定的なキーワードで拾い、判定結果は保存しない。
 *   取りこぼしを減らす方を優先する (誤検知は1タップで消えるが、取りこぼしは誰も気づけない)。
 * ⭐ただし「返品できますか」のような**質問**は案件ではない。だから自動案件化はしない
 */
export const CASE_KEYWORDS = [
  '返品', '返送', '交換', '不良', '初期不良', '故障', '壊れて', '割れて', 'われて', '破損', '破れて',
  '欠品', '足りない', '入っていない', '入ってない', '違う商品', '別の商品', '間違った商品',
  '返金', '返却', '代品', '代替品', '取り替え', '取替', '交換品', 'リコール',
  '動かない', '動作しない', '電源が入らない', '汚れて', 'キズ', 'きず', '傷が',
];

/** この問い合わせは案件になりそうか (当たったキーワードを返す。0件なら候補ではない) */
export function detectCaseKeywords(subject, body) {
  const text = normalizeForMatch(`${subject || ''}\n${stripQuoted(body || '')}`);
  if (!text) return [];
  const hits = [];
  for (const kw of CASE_KEYWORDS) {
    const n = normalizeForMatch(kw);
    if (n && text.includes(n) && !hits.includes(kw)) hits.push(kw);
  }
  return hits;
}

/** 案件化の判断が済んでいるか (案件を作った / 今回は作らないと決めた) */
export function getTriage(inquiryId) {
  return getDB().prepare('SELECT * FROM case_triage_results WHERE inquiry_id = ?').get(inquiryId) || null;
}

/** 判断を取り消す (「やっぱり案件にする」)。⭐候補バナーがまた出るようになる */
export function clearTriage(inquiryId) {
  getDB().prepare('DELETE FROM case_triage_results WHERE inquiry_id = ?').run(inquiryId);
}

export function setTriage(inquiryId, result, actor) {
  getDB().prepare(`INSERT INTO case_triage_results (inquiry_id, result, decided_by)
    VALUES (?,?,?) ON CONFLICT(inquiry_id) DO UPDATE SET
      result = excluded.result, decided_by = excluded.decided_by, decided_at = strftime('%Y-%m-%dT%H:%M:%SZ','now')`)
    .run(inquiryId, result, actor || null);
}

// ─────────────────────────────────────────────────────────
// 履歴
// ─────────────────────────────────────────────────────────

export function logCaseEvent(caseId, { eventType, from = null, to = null, actorType = 'user', actorId = null,
  sourceType = 'UI', sourceId = null, note = null }) {
  getDB().prepare(`INSERT INTO case_events
    (case_id, event_type, from_json, to_json, actor_type, actor_id, source_type, source_id, note)
    VALUES (?,?,?,?,?,?,?,?,?)`)
    .run(caseId, eventType, from == null ? null : JSON.stringify(from), to == null ? null : JSON.stringify(to),
      actorType, actorId, sourceType, sourceId, note);
}

// ─────────────────────────────────────────────────────────
// 案件の作成
// ─────────────────────────────────────────────────────────

/**
 * 'RC-2026-0001' を採番する (年ごとの連番。同一トランザクション内で呼ぶこと)。
 * ⭐**整数で最大値を取る** — 文字列順だと 'RC-2026-9999' > 'RC-2026-10000' になり、
 *   年1万件を超えた瞬間に同じ番号を作り続けて UNIQUE 違反で止まる
 */
function nextCaseNo(db, now = new Date()) {
  const year = new Date(now.getTime() + JST_OFFSET_MS).getUTCFullYear();
  const prefix = `RC-${year}-`;
  const row = db.prepare(`SELECT MAX(CAST(SUBSTR(case_no, ?) AS INTEGER)) AS mx
    FROM return_cases WHERE case_no LIKE ?`).get(prefix.length + 1, `${prefix}%`);
  const n = (row?.mx || 0) + 1;
  return prefix + String(n).padStart(4, '0');
}

/**
 * 問い合わせから案件を作る。
 * ⭐必須入力は **種別と次回確認日の2つだけ**。担当・注文番号・工程は自動で入れる
 *   (入力が3つを超えると押されなくなる)
 */
export function createCase({ inquiryId, caseType, nextActionDate, assignedUserId, summary = null,
  allowDuplicate = false, actor = null }) {
  const db = getDB();
  if (!CASE_TYPES[caseType]) throw new Error('案件種別が正しくありません');
  const nextActionAt = jstDateToIso(nextActionDate);
  if (!nextActionAt) throw new Error('次回確認日を入れてください');
  const inq = inquiryId
    ? db.prepare('SELECT * FROM inquiries WHERE id = ?').get(inquiryId)
    : null;
  if (inquiryId && !inq) throw new Error('問い合わせが見つかりません');
  const assignee = String(assignedUserId || inq?.assigned_user_id || actor || '').trim();
  if (!assignee) throw new Error('担当者が決まっていません (問い合わせに担当者を設定してから案件にしてください)');

  return db.transaction(() => {
    // ⭐二重作成を止める。1問い合わせに複数案件は正しい設計だが (商品Aは返金・商品Bは代品)、
    //   ボタンの二度押し・再送との区別がつかないので、既に未完了案件があるときは
    //   allowDuplicate を明示しない限り作らない (画面が「本当に別案件を作るか」を確認する)
    if (inquiryId && !allowDuplicate) {
      const dup = db.prepare(`SELECT c.case_no FROM case_inquiries ci
        JOIN return_cases c ON c.id = ci.case_id
        WHERE ci.inquiry_id = ? AND c.status = 'active' LIMIT 1`).get(inquiryId);
      if (dup) {
        const e = new Error(`この問い合わせには進行中の案件 ${dup.case_no} があります`);
        e.code = 'DUPLICATE_CASE';
        e.caseNo = dup.case_no;
        throw e;
      }
    }
    const caseNo = nextCaseNo(db);
    const info = db.prepare(`INSERT INTO return_cases
      (case_no, case_type, stage, waiting_on, status, assigned_user_id, next_action_at, waiting_since,
       customer_name, order_channel, order_no, product_name, summary, created_by)
      VALUES (?,?,'RECEIVED','SELF','active',?,?,?,?,?,?,?,?,?)`)
      .run(caseNo, caseType, assignee, nextActionAt, nowIso(),
        inq?.customer_name || null, inq?.order_mall || inq?.channel_type || null,
        inq?.order_number || null, inq?.product_name || null, summary, actor || null);
    const caseId = info.lastInsertRowid;

    // 工程をテンプレートから作る
    const insStep = db.prepare(`INSERT INTO case_steps
      (case_id, step_type, necessity_status, template_necessity, progress_status, assignee_id, waiting_party, due_at, sort_order)
      VALUES (?,?,?,?,'not_started',?,?,?,?)`);
    STEP_TEMPLATES[caseType].forEach((s, i) => {
      insStep.run(caseId, s.code, s.necessity, s.necessity, assignee, s.party,
        jstDateToIso(businessDaysFromNow(s.days)), (i + 1) * 10);
    });

    if (inquiryId) {
      db.prepare(`INSERT OR IGNORE INTO case_inquiries (case_id, inquiry_id, link_role, linked_by)
        VALUES (?,?,'origin',?)`).run(caseId, inquiryId, actor || null);
      setTriage(inquiryId, 'case_created', actor);
    }
    logCaseEvent(caseId, { eventType: 'case_created', to: { caseType, assignee, nextActionAt }, actorId: actor });
    return { id: caseId, case_no: caseNo };
  }).immediate();
}

/** 既存案件に問い合わせを関連付ける (別スレッドで「先日の返品の件ですが」と来たとき) */
export function linkInquiry(caseId, inquiryId, actor) {
  const db = getDB();
  const c = db.prepare('SELECT id FROM return_cases WHERE id = ?').get(caseId);
  if (!c) throw new Error('案件が見つかりません');
  db.transaction(() => {
    db.prepare(`INSERT OR IGNORE INTO case_inquiries (case_id, inquiry_id, link_role, linked_by)
      VALUES (?,?,'related',?)`).run(caseId, inquiryId, actor || null);
    setTriage(inquiryId, 'case_created', actor);
    logCaseEvent(caseId, { eventType: 'inquiry_linked', to: { inquiryId }, actorId: actor });
  }).immediate();
}

export function unlinkInquiry(caseId, inquiryId, actor) {
  const db = getDB();
  db.transaction(() => {
    db.prepare('DELETE FROM case_inquiries WHERE case_id = ? AND inquiry_id = ?').run(caseId, inquiryId);
    logCaseEvent(caseId, { eventType: 'inquiry_unlinked', from: { inquiryId }, actorId: actor });
  }).immediate();
}

// ─────────────────────────────────────────────────────────
// 参照
// ─────────────────────────────────────────────────────────

export function getCase(id) {
  return getDB().prepare('SELECT * FROM return_cases WHERE id = ?').get(id) || null;
}

export function listSteps(caseId) {
  return getDB().prepare('SELECT * FROM case_steps WHERE case_id = ? ORDER BY sort_order, id').all(caseId);
}

export function listCaseInquiries(caseId) {
  return getDB().prepare(`SELECT ci.*, i.subject, i.customer_name, i.internal_status, i.channel_type, i.last_message_at
    FROM case_inquiries ci JOIN inquiries i ON i.id = ci.inquiry_id
    WHERE ci.case_id = ? ORDER BY ci.link_role = 'origin' DESC, ci.linked_at`).all(caseId);
}

/** この問い合わせに紐づく案件 (問い合わせ詳細のパネルに出す) */
export function listCasesForInquiry(inquiryId) {
  return getDB().prepare(`SELECT c.*, ci.link_role FROM case_inquiries ci
    JOIN return_cases c ON c.id = ci.case_id
    WHERE ci.inquiry_id = ? ORDER BY c.status = 'active' DESC, c.id DESC`).all(inquiryId);
}

export function listEvents(caseId, limit = 30) {
  return getDB().prepare('SELECT * FROM case_events WHERE case_id = ? ORDER BY id DESC LIMIT ?').all(caseId, limit);
}

export function listExternalRequests(caseId) {
  return getDB().prepare('SELECT * FROM external_requests WHERE case_id = ? ORDER BY id').all(caseId);
}

/** サイドバーのバッジ (未完了の案件数)。失敗しても画面は出す側で握る */
export function countOpenCases() {
  return getDB().prepare("SELECT COUNT(*) AS c FROM return_cases WHERE status = 'active'").get().c;
}

/**
 * 次にやること = 最初の「必要かつ未完了」の工程。
 * ⭐カードにも詳細にもこれを出す。列名だけでは次の行動が分からないため
 */
export function nextStepOf(steps) {
  const active = steps.filter(s => s.necessity_status === 'required'
    && !['completed', 'exception'].includes(s.progress_status));
  if (active.length) return active[0];
  // 必要な工程が全部済んでいるなら、要否未確定の工程を片付けるのが次の行動
  return steps.find(s => s.necessity_status === 'undecided') || null;
}

/** 完了できない理由 (空配列なら完了できる) */
export function blockersOf(caseId) {
  const steps = listSteps(caseId);
  const out = steps.filter(s =>
    s.necessity_status === 'undecided' ||
    (s.necessity_status === 'required' && !['completed', 'exception'].includes(s.progress_status)));
  const reqs = getDB().prepare(`SELECT * FROM external_requests
    WHERE case_id = ? AND is_blocking = 1 AND status NOT IN ('COMPLETED','CANCELED')`).all(caseId);
  return { steps: out, requests: reqs, total: out.length + reqs.length };
}

/**
 * テンプレート上、その工程は「必要」だったか「要否未確定」だったか。
 * ⭐**列に頼らずテンプレートから引き直せる** — 先の版で作られた行 (退避値が無く、
 *   移行の既定値で 'required' になっている行) でも、正しい要否に戻せる
 */
export function templateNecessityOf(caseType, stepType) {
  const hit = (STEP_TEMPLATES[caseType] || []).find(s => s.code === stepType);
  return hit ? hit.necessity : null;
}

/** 工程の表示名 (テンプレートに無いコードでも落ちないようにする) */
export function stepLabel(caseType, code) {
  for (const list of [STEP_TEMPLATES[caseType] || [], ...Object.values(STEP_TEMPLATES)]) {
    const hit = list.find(s => s.code === code);
    if (hit) return hit.name;
  }
  return code;
}

/**
 * ボード用の一覧。
 * ⭐並びは ①期限超過 ②期限が近い ③期限なし ④長期滞留 の順 (Codex指摘: 列内の順序が意味を持つ)
 */
export function listBoardCases({ includeCompleted = true, assignee = null, caseType = null } = {}) {
  const db = getDB();
  const where = [];
  const params = [];
  if (!includeCompleted) where.push("c.status = 'active'");
  if (assignee) { where.push('c.assigned_user_id = ?'); params.push(assignee); }
  if (caseType) { where.push('c.case_type = ?'); params.push(caseType); }
  const rows = db.prepare(`SELECT c.* FROM return_cases c
    ${where.length ? 'WHERE ' + where.join(' AND ') : ''}
    ORDER BY c.status = 'active' DESC, c.next_action_at IS NULL, c.next_action_at, c.id DESC`).all(...params);
  const stepStat = db.prepare(`SELECT
      COUNT(*) AS total,
      SUM(CASE WHEN progress_status IN ('completed','exception') OR necessity_status = 'not_required'
          THEN 1 ELSE 0 END) AS done
    FROM case_steps WHERE case_id = ?`);
  const now = new Date();
  return rows.map(c => {
    const steps = listSteps(c.id);
    const next = nextStepOf(steps);
    const st = stepStat.get(c.id);
    return {
      ...c,
      over: overdueDays(c.next_action_at, now),
      stagnant: stagnantDays(c.waiting_since, now),
      next_step: next,
      next_step_label: next ? stepLabel(c.case_type, next.step_type) : null,
      steps_done: st.done || 0,
      steps_total: st.total || 0,
    };
  });
}

/** ボードの列 (誰待ち / 工程) */
export function boardColumns(groupBy) {
  return groupBy === 'stage'
    ? Object.entries(STAGES).filter(([k]) => k !== 'CANCELED').map(([key, v]) => ({ key, label: v.label }))
    : Object.entries(WAITING_ON).map(([key, v]) => ({ key, label: v.label }));
}

// ─────────────────────────────────────────────────────────
// 更新
// ─────────────────────────────────────────────────────────

function touchCase(db, caseId, patch = {}) {
  const cols = Object.keys(patch);
  const sql = `UPDATE return_cases SET ${cols.map(c => `${c} = ?`).join(', ')}${cols.length ? ',' : ''}
    updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id = ?`;
  db.prepare(sql).run(...cols.map(c => patch[c]), caseId);
}

/**
 * 案件の待ち先・次回確認日を変える。
 * ⭐waiting_on が変わったときだけ waiting_since を更新する (メモ修正で滞留がリセットされないように)
 * ⭐外部待ちにするときは次回確認日が必須 (空欄なら既定の営業日数で自動的に入れる)
 */
export function setWaiting(caseId, { waitingOn, nextActionDate, nextActionNote, actor }) {
  const db = getDB();
  if (!WAITING_ON[waitingOn]) throw new Error('待ち先が正しくありません');
  return db.transaction(() => {
    const cur = db.prepare('SELECT * FROM return_cases WHERE id = ?').get(caseId);
    if (!cur) throw new Error('案件が見つかりません');
    if (cur.status !== 'active') throw new Error('完了した案件は変更できません');
    const changed = cur.waiting_on !== waitingOn;
    let nextAt = nextActionDate ? jstDateToIso(nextActionDate) : (changed ? null : cur.next_action_at);
    if (nextActionDate && !nextAt) throw new Error('次回確認日の形式が正しくありません');
    // ⭐待ち先が変わったのに日付を指定しなかったら、**新しい待ち先の既定で取り直す**。
    //   前の待ち先の期限をそのまま持ち越すと、変えた瞬間に期限超過になったり、
    //   逆に遠い将来の期限を引きずったりする
    if (!nextAt && EXTERNAL_WAITING.includes(waitingOn)) {
      nextAt = jstDateToIso(businessDaysFromNow(DEFAULT_DUE_BUSINESS_DAYS[waitingOn] ?? 3));
    }
    if (!nextAt && !EXTERNAL_WAITING.includes(waitingOn)) nextAt = changed ? cur.next_action_at : cur.next_action_at;
    const patch = { waiting_on: waitingOn, next_action_at: nextAt };
    if (nextActionNote !== undefined) patch.next_action_note = nextActionNote || null;
    if (changed) patch.waiting_since = nowIso();
    touchCase(db, caseId, patch);
    logCaseEvent(caseId, { eventType: 'waiting_changed',
      from: { waiting_on: cur.waiting_on, next_action_at: cur.next_action_at },
      to: { waiting_on: waitingOn, next_action_at: nextAt }, actorId: actor });
    return getCase(caseId);
  }).immediate();
}

/** 案件の担当を変える (⭐外部待ちでも社内担当は必ず居る) */
export function setAssignee(caseId, assignee, actor) {
  const db = getDB();
  const name = String(assignee || '').trim();
  if (!name) throw new Error('担当者を空にはできません');
  db.transaction(() => {
    const cur = db.prepare('SELECT * FROM return_cases WHERE id = ?').get(caseId);
    if (!cur) throw new Error('案件が見つかりません');
    // ⭐完了した案件は変えない (閉じた時点の記録が後から書き換わると台帳として使えない)。
    //   直すときは「開け直す」を通す
    if (cur.status !== 'active') throw new Error('完了した案件は変更できません (開け直してから直してください)');
    touchCase(db, caseId, { assigned_user_id: name });
    logCaseEvent(caseId, { eventType: 'assignee_changed',
      from: { assignee: cur.assigned_user_id }, to: { assignee: name }, actorId: actor });
  }).immediate();
  return getCase(caseId);
}

/** 返金の記録 (⭐実行はモール管理画面。ここは「いくら返す約束で、いくら返したか」) */
export function setRefund(caseId, { expected, completed, ref, actor }) {
  const db = getDB();
  const num = v => (v === '' || v == null) ? null : Number(String(v).replace(/[,\s]/g, ''));
  const exp = num(expected), done = num(completed);
  if (exp != null && (!Number.isFinite(exp) || exp < 0)) throw new Error('返金予定額が正しくありません');
  if (done != null && (!Number.isFinite(done) || done < 0)) throw new Error('返金実績額が正しくありません');
  db.transaction(() => {
    const cur = db.prepare('SELECT * FROM return_cases WHERE id = ?').get(caseId);
    if (!cur) throw new Error('案件が見つかりません');
    // ⭐完了後に返金額が書き換わると、閉じた時点の記録が信用できなくなる
    if (cur.status !== 'active') throw new Error('完了した案件は変更できません (開け直してから直してください)');
    touchCase(db, caseId, {
      refund_expected_amount: exp, refund_completed_amount: done, refund_external_ref: ref || null,
    });
    logCaseEvent(caseId, { eventType: 'refund_recorded',
      from: { expected: cur.refund_expected_amount, completed: cur.refund_completed_amount },
      to: { expected: exp, completed: done, ref: ref || null }, actorId: actor });
  }).immediate();
  return getCase(caseId);
}

/**
 * 工程の状態を変える。
 * action: complete (完了にする) / skip (対応不要) / need (必要にする) / start (対応を開始) /
 *         wait (回答・到着待ちにする) / undo (戻す)
 *
 * ⭐工程を「回答・到着待ち」にしたら、案件の waiting_on もその待ち先に寄せる
 *   (カンバンの列を人が別途動かさなくて済む = 二重入力を作らない)
 */
export function updateStep(caseId, stepId, action, { note, externalRef, reason, actor } = {}) {
  const db = getDB();
  return db.transaction(() => {
    // ⭐判定に使う値はトランザクションの中で読む
    //   (外で読むと、履歴の「変更前」が実際と食い違う / 判定と更新の間に状態が変わる)
    const c = db.prepare('SELECT * FROM return_cases WHERE id = ?').get(caseId);
    if (!c) throw new Error('案件が見つかりません');
    if (c.status !== 'active') throw new Error('完了した案件の工程は変更できません');
    const step = db.prepare('SELECT * FROM case_steps WHERE id = ? AND case_id = ?').get(stepId, caseId);
    if (!step) throw new Error('工程が見つかりません');

    const before = { necessity: step.necessity_status, progress: step.progress_status };
    const patch = {};
    // 担当者が未登録のまま通した例外操作の印。⭐ログはいずれ消えるので履歴にも残す
    let stepBootstrap = false;
    switch (action) {
      case 'complete':
        patch.necessity_status = 'required';
        patch.progress_status = 'completed';
        patch.completed_at = nowIso();
        patch.completed_by = actor || null;
        break;
      case 'skip':
        // ⭐要否がまだ決まっていない工程だけ「対応不要」にできる。
        //   最初から必要な工程 (返金処理など) をここで消せると、完了ゲートが無意味になる
        if (step.necessity_status !== 'undecided') {
          throw new Error('必要と決まっている工程は「対応不要」にできません (どうしても外すときは「この工程を外す」を使ってください)');
        }
        patch.necessity_status = 'not_required';
        patch.necessity_before_skip = step.necessity_status;   // 戻すときはここへ戻す
        patch.progress_status = 'not_started';
        patch.completed_at = nowIso();
        patch.completed_by = actor || null;
        break;
      case 'skip_required': {
        // ⭐必要と決まっている工程を外す = 例外操作。理由と権限が要る (履歴にも別イベントで残す)
        if (step.necessity_status !== 'required') {
          throw new Error(step.necessity_status === 'not_required'
            ? 'すでに対応不要です'
            : '要否がまだ決まっていない工程は「対応不要にする」を使ってください');
        }
        if (!String(reason || '').trim()) throw new Error('必要な工程を外すには理由を書いてください');
        const perm = canDoException(actor);
        if (!perm.allowed) throw new Error('必要な工程を外す権限がありません (担当者と権限の画面で D3 が要ります)');
        if (perm.unmanaged) console.warn('[inquiry-hub] 担当者未登録のまま必要な工程を外しました case=' + caseId + ' step=' + stepId + ' by ' + (actor || '不明'));
        stepBootstrap = perm.unmanaged;
        patch.necessity_status = 'not_required';
        patch.necessity_before_skip = 'required';
        patch.progress_status = 'not_started';
        patch.completed_at = nowIso();
        patch.completed_by = actor || null;
        patch.note = String(reason).slice(0, 500);
        break;
      }
      case 'need':
        patch.necessity_status = 'required';
        break;
      case 'start':
        patch.necessity_status = 'required';
        patch.progress_status = 'in_progress';
        break;
      case 'wait':
        patch.necessity_status = 'required';
        patch.progress_status = 'waiting';
        break;
      case 'undo':
        // ⭐「対応不要」を戻すときは必要性も戻す。進捗だけ戻すと、戻したつもりで
        //   not_required のまま残り、完了ゲートをすり抜ける。
        //   戻し先は**外す直前の値** (人が「必要」と決めていたなら必要へ戻す。
        //   テンプレート値へ戻すと、その判断が消えてしまう)
        if (step.necessity_status === 'not_required') {
          // ①外す直前の値 → ②テンプレートから引き直した値 → ③列に残っている値 の順に戻す。
          // ⭐②があるので、退避値を持たない古い行 (移行で既定の 'required' になっている行) でも
          //   「テンプレートでは要否未確定だった工程」を required に化けさせない
          patch.necessity_status = step.necessity_before_skip
            || templateNecessityOf(c.case_type, step.step_type)
            || step.template_necessity || 'required';
          patch.necessity_before_skip = null;
        }
        patch.progress_status = 'not_started';
        patch.completed_at = null;
        patch.completed_by = null;
        break;
      default:
        throw new Error('操作が正しくありません');
    }
    if (note !== undefined) patch.note = note || null;
    if (externalRef !== undefined) patch.external_ref = externalRef || null;

    const cols = Object.keys(patch);
    db.prepare(`UPDATE case_steps SET ${cols.map(k => `${k} = ?`).join(', ')},
      updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id = ?`)
      .run(...cols.map(k => patch[k]), stepId);

    // 案件の stage を、済んだ工程のうち一番進んだところに寄せる (代表工程)
    const steps = listSteps(caseId);
    const stagePatch = {};
    const order = Object.keys(STAGES);
    const doneStages = steps
      .filter(s => s.progress_status === 'completed')
      .map(s => STEP_STAGE[s.step_type])
      .filter(Boolean);
    const next = nextStepOf(steps);
    const nextStage = next ? STEP_STAGE[next.step_type] : null;
    const candidate = nextStage || doneStages.sort((a, b) => order.indexOf(b) - order.indexOf(a))[0];
    if (candidate && candidate !== c.stage) stagePatch.stage = candidate;

    // 工程を待ちにしたら案件の待ち先もそこへ寄せる。⭐waiting_on が変わるときだけ waiting_since を更新
    if (action === 'wait') {
      const w = PARTY_TO_WAITING[step.waiting_party] || 'SELF';
      if (w !== c.waiting_on) { stagePatch.waiting_on = w; stagePatch.waiting_since = nowIso(); }
      if (!c.next_action_at && EXTERNAL_WAITING.includes(w)) {
        stagePatch.next_action_at = jstDateToIso(businessDaysFromNow(DEFAULT_DUE_BUSINESS_DAYS[w] ?? 3));
      }
    }
    // 工程が片付いて次が自社の工程なら、ボールは自社に戻る
    if ((action === 'complete' || action === 'skip') && next) {
      const w = PARTY_TO_WAITING[next.waiting_party] || 'SELF';
      if (w === 'SELF' && c.waiting_on !== 'SELF') { stagePatch.waiting_on = 'SELF'; stagePatch.waiting_since = nowIso(); }
    }
    if (Object.keys(stagePatch).length) touchCase(db, caseId, stagePatch);
    else touchCase(db, caseId, {});

    logCaseEvent(caseId, { eventType: action === 'skip_required' ? 'step_skipped_exception' : 'step_changed',
      from: before,
      to: { step_type: step.step_type, action, ...patch, bootstrap: stepBootstrap || undefined },
      actorId: actor, note: stepLabel(c.case_type, step.step_type) });
    return { case: getCase(caseId), steps: listSteps(caseId) };
  }).immediate();
}

/**
 * 案件を完了する。
 * ⭐**条件付き fail-closed**: 「必要」とされた工程が片付くまで完了できない。
 *   ただし一律ではない — 交換のみなら返金は不要、返送不要の返金もある。
 *   force=true は例外完了 (理由コード必須。誰がなぜ閉じたかを履歴に残す)
 */
export function closeCase(caseId, { force = false, reasonCode = null, note = null, actor = null } = {}) {
  const db = getDB();
  // ⭐ゲートの判定と完了の更新を**同じトランザクションの中でやる**。
  //   外で判定してから閉じると、その間に工程が増えても気づけない
  return db.transaction(() => {
    const c = db.prepare('SELECT * FROM return_cases WHERE id = ?').get(caseId);
    if (!c) throw new Error('案件が見つかりません');
    if (c.status !== 'active') return { ok: true, already: true, case: c };
    const blockers = blockersOf(caseId);
    if (blockers.total > 0 && !force) {
      return { ok: false, blockers, case: c };
    }
    let bootstrap = false;
    if (force) {
      if (!CLOSE_REASONS[reasonCode]) throw new Error('例外として完了するには理由を選んでください');
      if (!String(note || '').trim()) throw new Error('例外として完了するには詳細メモを入れてください');
      // ⭐例外完了は権限が要る (画面でボタンを隠すだけにしない — APIを直に叩けるため)
      const perm = canDoException(actor);
      if (!perm.allowed) throw new Error('例外として完了する権限がありません (担当者と権限の画面で D3 が要ります)');
      // 担当者が未登録のまま通した操作には印を残す (あとで「権限の外で閉じた案件」を洗い出せるように)
      bootstrap = perm.unmanaged;
      if (bootstrap) console.warn('[inquiry-hub] 担当者未登録のまま例外完了しました case=' + caseId + ' by ' + (actor || '不明'));
    }
    // status='active' と closed_at IS NULL の両方を条件にして二重完了を防ぐ
    const r = db.prepare(`UPDATE return_cases SET status = 'completed', stage = 'COMPLETED', waiting_on = 'NONE',
      closed_at = ?, closed_by = ?, close_reason_code = ?, close_note = ?,
      updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now')
      WHERE id = ? AND status = 'active' AND closed_at IS NULL`)
      .run(nowIso(), actor || null, force ? reasonCode : null, force ? note : (note || null), caseId);
    if (r.changes === 0) return { ok: true, already: true, case: getCase(caseId) };
    logCaseEvent(caseId, { eventType: force ? 'case_closed_exception' : 'case_closed',
      to: { reasonCode: force ? reasonCode : null, bootstrap: bootstrap || undefined },
      actorId: actor, note: note || null });
    return { ok: true, case: getCase(caseId), blockers: force ? blockers : null };
  }).immediate();
}

/** 完了した案件を開け直す (顧客から続きが来たときなど。自動では戻さない) */
export function reopenCase(caseId, actor) {
  const db = getDB();
  const c = getCase(caseId);
  if (!c) throw new Error('案件が見つかりません');
  if (c.status === 'active') return c;
  db.transaction(() => {
    db.prepare(`UPDATE return_cases SET status = 'active', waiting_on = 'SELF', stage = 'FOLLOWING_UP',
      closed_at = NULL, closed_by = NULL, close_reason_code = NULL,
      waiting_since = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id = ?`)
      .run(nowIso(), caseId);
    logCaseEvent(caseId, { eventType: 'case_reopened', actorId: actor });
  }).immediate();
  return getCase(caseId);
}

// ─────────────────────────────────────────────────────────
// 整合性チェック (⭐カンバンが嘘をついていないかを毎日見る)
// ─────────────────────────────────────────────────────────

/**
 * データの矛盾を洗い出す。1日1回まわして管理者に出す。
 * ⭐「更新されないカンバンは害になる」への対策は、ゲートを緩めることではなく、これを見せること
 */
export function findInconsistencies() {
  const db = getDB();
  const out = [];
  const add = (kind, label, rows) => { if (rows.length) out.push({ kind, label, rows }); };

  add('no_due', '外部待ちなのに次回確認日がない',
    db.prepare(`SELECT id, case_no, waiting_on FROM return_cases
      WHERE status = 'active' AND waiting_on IN ('CUSTOMER','SUPPLIER','CARRIER','MARKETPLACE','SCHEDULED_EVENT')
        AND next_action_at IS NULL`).all());

  add('closed_waiting', '完了しているのに待ち先が残っている',
    db.prepare(`SELECT id, case_no, waiting_on FROM return_cases
      WHERE status != 'active' AND waiting_on != 'NONE'`).all());

  add('no_assignee', '未完了なのに担当者がいない',
    db.prepare(`SELECT id, case_no FROM return_cases
      WHERE status = 'active' AND (assigned_user_id IS NULL OR TRIM(assigned_user_id) = '')`).all());

  add('stale', '30日以上動いていない',
    db.prepare(`SELECT id, case_no, updated_at FROM return_cases
      WHERE status = 'active' AND updated_at < datetime('now','-30 days')`).all());

  add('reply_but_waiting', '外部依頼は返信済みなのに案件が待ちのまま',
    db.prepare(`SELECT c.id, c.case_no FROM return_cases c
      WHERE c.status = 'active' AND c.waiting_on IN ('SUPPLIER','CARRIER','MARKETPLACE')
        AND NOT EXISTS (SELECT 1 FROM external_requests r
          WHERE r.case_id = c.id AND r.is_blocking = 1 AND r.status NOT IN ('COMPLETED','CANCELED','REPLIED'))
        AND EXISTS (SELECT 1 FROM external_requests r WHERE r.case_id = c.id AND r.status = 'REPLIED')`).all());

  add('refund_missing', '返金予定額があるのに実績が未記録のまま完了している',
    db.prepare(`SELECT id, case_no FROM return_cases
      WHERE status = 'completed' AND refund_expected_amount IS NOT NULL AND refund_completed_amount IS NULL`).all());

  return out;
}

/** 毎朝のダイジェスト用の集計 (⭐0件でも送る = dead-man) */
export function digestCounts(now = new Date()) {
  const rows = listBoardCases({ includeCompleted: false });
  const today = jstDate(now.toISOString());
  return {
    total: rows.length,
    overdue: rows.filter(r => r.over > 0),
    dueToday: rows.filter(r => r.over === 0 && r.next_action_at && jstDate(r.next_action_at) === today),
    noDue: rows.filter(r => !r.next_action_at),
    byWaiting: Object.fromEntries(Object.keys(WAITING_ON)
      .map(k => [k, rows.filter(r => r.waiting_on === k).length])),
  };
}
