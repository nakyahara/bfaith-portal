/**
 * inquiry-hub 新規メール作成 (2026-08-27 中原さん要望
 *   「メール作成ボタンからメールが送れるように / 押したらテンプレート・To-From設定・署名を選ぶ画面へ」)
 *
 * ■ 何をするものか
 *   受信した問い合わせへの「返信」ではなく、こちらから宛先を指定して出す新規メール
 *   (取引先への連絡・顧客への追いかけ連絡など。メールディーラーの「メール作成」相当)。
 *
 * ■ 設計の芯: 新規メールも「1件の問い合わせスレッド」として作る
 *   送信の仕組み (outbox_replies + 送信ワーカー + 二重送信を作らない状態機械) は
 *   inquiry_id を前提に組まれている。新規メール専用の送信経路を別に作ると、
 *   exactly-once にできない送信の扱いが二系統になって事故る。
 *   → 送信前に inquiries を1件作り、以後は返信とまったく同じ経路を通す。
 *   これで送信済み一覧・スレッド表示・顧客からの返信の紐付けが自動的に効く。
 *
 * ■ 外部ID (external_inquiry_id) の扱いと、会話が2つに割れないための備え
 *   Gmail のスレッドIDは送ってみるまで分からないため、作成時は仮ID 'compose:<uuid>' を入れる
 *   (compose-id.js)。送信成功時にアダプターが返す実スレッドIDへ差し替える (outbox.js)。
 *   差し替えと送信メッセージの記録は同一トランザクションなので、通常は同期が中途半端な状態を
 *   見ることはない。それでも次の2つは起こり得るので、片付け方を用意してある:
 *     (a) 同期が先に同じスレッドを取り込んでいた
 *         → adoptComposeInquiryByMessageIds() で送信済みメッセージIDから合流させる。
 *           合流できない (既に実スレッドのチケットが作られている) ときは両方に⚠️要確認を立てる
 *     (b) Gmail送信は成功したがDB記録前にプロセスが落ちた (送信結果 unknown)
 *         → 自動では直せない。送信ジョブは unknown のまま残り、⚙️運用管理で人が解決する
 *           (設計書§8.3。ここで自動再送・自動マージをすると二重送信や取り違えを生む)
 *
 * ■ 下書き (is_archived=1) の意味
 *   添付は inquiry_id に紐付けて保存する (reply-attachments.js) ので、添付を付ける時点で
 *   スレッドの器が要る。器だけ先に作って一覧に出すと「空のメールが並ぶ」ので、
 *   送信ジョブができるまでは is_archived=1 (=一覧・件数から除外) にしておく。
 *   送られなかった器は24時間後に掃除する (cronは増やさず、作成画面を開いたときに実施)。
 *
 * ■ 権限
 *   下書きの所有者チェックは入れていない。このアプリは社内スタッフ全員がすべての問い合わせを
 *   操作できる前提 (返信も誰でも作れる) で、下書きだけ別モデルにすると分かりにくくなるため。
 *   代わりに作成・確定は操作ログ (inquiry_activity_logs) に残して誰の操作か追えるようにする。
 */
import crypto from 'crypto';
import { getDB, logActivity, toUtcIso } from './db.js';
import { classifyReplyDestination, RAKUTEN_MASKED_RE } from './no-reply.js';
import { COMPOSE_PREFIX, isComposeThread } from './compose-id.js';

export { COMPOSE_PREFIX, isComposeThread };

export const SUBJECT_MAX = 200;
export const CUSTOMER_NAME_MAX = 100;
/** 本文の上限 (画面・APIの両方で弾く。Gmailの上限ではなく「業務メールとして異常な長さ」の線引き) */
export const BODY_MAX = 50000;
/** 送信されなかった下書き (器) を掃除するまでの時間 */
const DRAFT_TTL_HOURS = 24;

/** 'compose:' で始まる外部IDの範囲 (前方一致。LIKE と違って UNIQUE index が効く形で書く) */
const COMPOSE_RANGE = ['compose:', 'compose;'];   // ';' は ':' の次のコードポイント

/** ローカル部 (dot-atom)。引用符付きローカル部は業務では現れないので受け付けない */
const EMAIL_LOCAL_RE = /^[A-Za-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[A-Za-z0-9!#$%&'*+/=?^_`{|}~-]+)*$/;
/** ドメインの1ラベル (先頭・末尾のハイフン不可・63文字まで) */
const DOMAIN_LABEL_RE = /^[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?$/;

/**
 * 業務で使えるメールアドレスの形か (RFCの全許容ではなく、実在アドレスの形に絞る)。
 * 先頭・末尾・連続のドット、ハイフンで始まる/終わるドメインラベル、数字だけのTLDを弾く。
 */
export function isPlausibleEmail(address) {
  const addr = String(address || '');
  if (addr.length > 254) return false;
  const at = addr.lastIndexOf('@');
  if (at <= 0 || at === addr.length - 1) return false;
  const local = addr.slice(0, at);
  const domain = addr.slice(at + 1);
  if (local.length > 64 || !EMAIL_LOCAL_RE.test(local)) return false;
  if (domain.length > 253) return false;
  const labels = domain.split('.');
  if (labels.length < 2) return false;
  if (!labels.every(l => DOMAIN_LABEL_RE.test(l))) return false;
  return /^[A-Za-z][A-Za-z0-9-]*$/.test(labels[labels.length - 1]);   // TLDは英字始まり
}

/** 送信元として使えるメールチャネルの店舗 (メールディーラーの「To/From設定」に相当) */
export function listMailShops() {
  return getDB().prepare("SELECT * FROM shops WHERE channel_type = 'email' AND is_active = 1 ORDER BY id").all();
}

/** shopId 指定があればそれを、無ければ最初のメール店舗を返す。1件も無ければ throw */
export function resolveMailShop(shopId = null) {
  const shops = listMailShops();
  if (!shops.length) {
    throw new Error('メール送信に使える店舗が登録されていません (⚙️運用管理でメールチャネルの店舗を確認してください)');
  }
  if (shopId == null || shopId === '') return shops[0];
  const found = shops.find(s => s.id === Number(shopId));
  if (!found) throw new Error('指定された送信元が見つかりません');
  return found;
}

/**
 * 宛先の検証 (画面と送信ジョブ作成の両方で使う)。
 * ヘッダインジェクション対策の改行拒否、返信不可アドレス (no-reply 等) の門前払い、
 * 楽天マスクアドレスの除外を含む。
 * 小文字化はドメイン部だけに留める (RFC上ローカル部は大小を区別し得るため。
 * 実際の宛先はドメインの表記ゆれだけ吸収できれば足りる)。
 * @returns {string} 正規化した宛先
 */
export function normalizeRecipient(input) {
  const raw = String(input ?? '').trim();
  if (!raw) throw new Error('宛先メールアドレスを入力してください');
  if (/[\r\n\0,;]/.test(raw)) throw new Error('宛先は1件だけ指定してください (改行・カンマは使えません)');
  // eslint-disable-next-line no-control-regex -- 制御文字はヘッダに入れられないので明示的に弾く
  if (/[\x00-\x1f\x7f<>"\\()[\]:\s]/.test(raw)) throw new Error('宛先に使えない文字が含まれています');
  const at = raw.lastIndexOf('@');
  const to = at > 0 ? raw.slice(0, at) + '@' + raw.slice(at + 1).toLowerCase() : raw;
  if (!isPlausibleEmail(to)) throw new Error(`宛先メールアドレスの形式が正しくありません: ${to}`);
  const verdict = classifyReplyDestination(to);
  if (!verdict.sendable) throw new Error(`${verdict.reason}。${verdict.guide}`);
  // 楽天あんしんメルアド (…@fw.rakuten.ne.jp) は楽天SMTP経由でしか送れず、Gmailスレッドが
  // 残らないため「送った後の会話」がこのアプリで追えない。新規メールの宛先としては断り、
  // 楽天の問い合わせへの返信 (既存スレッドの「返信」) を使ってもらう
  if (RAKUTEN_MASKED_RE.test(to)) {
    throw new Error('楽天のマスクアドレス宛には新規メールを作成できません。'
      + '楽天のお客様へは、その問い合わせスレッドを開いて「返信」から送ってください');
  }
  return to;
}

/** 件名の検証。空件名のメールは送らない (受け取る側で埋もれるため) */
export function normalizeSubject(input) {
  const subject = String(input ?? '').replace(/[\r\n\t]/g, ' ').trim().replace(/\s{2,}/g, ' ');
  if (!subject) throw new Error('件名を入力してください');
  if (subject.length > SUBJECT_MAX) throw new Error(`件名が長すぎます (${SUBJECT_MAX}文字まで)`);
  return subject;
}

/** 本文の検証 (空と異常な長さだけ。整形はしない = 書いたとおりに送る) */
export function validateBody(input) {
  const body = String(input ?? '');
  if (!body.trim()) throw new Error('本文が空です');
  if (body.length > BODY_MAX) throw new Error(`本文が長すぎます (${BODY_MAX}文字まで)`);
  return body;
}

/** 宛先の表示名 (任意)。長すぎるものは黙って切らずに断る (件名・本文と揃える) */
export function normalizeCustomerName(input) {
  const name = String(input ?? '').replace(/[\r\n\t]/g, ' ').trim().replace(/\s{2,}/g, ' ');
  if (!name) return null;
  if (name.length > CUSTOMER_NAME_MAX) throw new Error(`宛先の名前が長すぎます (${CUSTOMER_NAME_MAX}文字まで)`);
  return name;
}

/**
 * 下書きの器を作る (添付を付けるために inquiry_id が必要になった時点で呼ぶ)。
 * is_archived=1 なので一覧・件数には出ない。
 */
export function createComposeDraft({ shopId = null, createdBy = null } = {}) {
  const db = getDB();
  const shop = resolveMailShop(shopId);
  const nowIso = toUtcIso(Date.now());
  const externalId = COMPOSE_PREFIX + crypto.randomUUID();
  return db.transaction(() => {
    const r = db.prepare(`INSERT INTO inquiries (
        channel_type, shop_id, external_inquiry_id, subject,
        internal_status, is_unread, is_archived, received_at, created_at, updated_at
      ) VALUES ('email', ?, ?, NULL, 'open', 0, 1, ?, ?, ?)`)
      .run(shop.id, externalId, nowIso, nowIso, nowIso);
    const id = Number(r.lastInsertRowid);
    logActivity(id, { actorType: 'user', userId: createdBy, actionType: 'compose_draft_created' });
    return { id, shopId: shop.id, externalInquiryId: externalId };
  }).immediate();
}

/** 下書き (未送信の器) を読む。下書きでなければ null */
export function getComposeDraft(id) {
  const inq = getDB().prepare('SELECT * FROM inquiries WHERE id = ?').get(Number(id));
  if (!inq) return null;
  if (inq.channel_type !== 'email' || !isComposeThread(inq.external_inquiry_id)) return null;
  if (!inq.is_archived) return null;   // 既に送信ジョブが作られて表に出ているものは下書きではない
  return inq;
}

/**
 * 宛先・件名を確定して下書きを表に出す。
 * ⚠️ 単独で呼ばず、送信ジョブ作成 (createReplyJob) と同じトランザクションの中で呼ぶこと。
 *   途中で落ちると「宛先も件名も入っているのに送信ジョブが無い問い合わせ」が一覧に残るため
 *   (Codexレビュー High-1)。呼び出しは router.js の /api/compose/send にまとめてある。
 * draftId が無ければ器も一緒に作る (添付なしで送るケース)。
 * @returns {{ inquiry, created: boolean }}
 */
export function finalizeComposeDraft({ draftId = null, shopId = null, to, subject, customerName = null, actor = null } = {}) {
  const db = getDB();
  const dest = normalizeRecipient(to);
  const subj = normalizeSubject(subject);
  const name = normalizeCustomerName(customerName);
  let inq = null;
  let created = false;
  if (draftId != null && draftId !== '') {
    inq = getComposeDraft(draftId);
    if (!inq) {
      // 既に送信ジョブを作った下書きを二度送ろうとした場合は、それと分かる案内を出す
      const done = db.prepare('SELECT id, is_archived, external_inquiry_id FROM inquiries WHERE id = ?').get(Number(draftId));
      if (done && !done.is_archived && isComposeThread(done.external_inquiry_id)) {
        throw new Error(`このメールは既に送信手続き済みです (問い合わせ #${done.id} を確認してください)`);
      }
      throw new Error('下書きが見つかりません (時間が経って破棄された可能性があります。画面を開き直してください)');
    }
  } else {
    const d = createComposeDraft({ shopId, createdBy: actor });
    inq = db.prepare('SELECT * FROM inquiries WHERE id = ?').get(d.id);
    created = true;
  }
  const nowIso = toUtcIso(Date.now());
  db.prepare(`UPDATE inquiries SET customer_identifier = ?, customer_name = ?, subject = ?,
      is_archived = 0, received_at = ?, updated_at = ? WHERE id = ?`)
    .run(dest, name, subj, nowIso, nowIso, inq.id);
  logActivity(inq.id, { actorType: 'user', userId: actor, actionType: 'compose_created',
    after: { to: dest, subject: subj } });
  return { inquiry: db.prepare('SELECT * FROM inquiries WHERE id = ?').get(inq.id), created };
}

/**
 * 送られないまま残った下書きの掃除 (作成画面を開いたときに実施。cronは増やさない)。
 * 対象 = compose 由来 / メッセージも送信ジョブも無い / 24時間経過。
 * is_archived は条件にしない: 送信ジョブ作成が異常終了して「表に出たのにジョブが無い」
 * ものも回収する (Codexレビュー High-1 の多層防御)。
 * 添付 (BLOB) → 操作ログ・メモ → 本体 の順に消す (FK 制約の順序)。
 */
export function pruneStaleComposeDrafts() {
  const db = getDB();
  const rows = db.prepare(`SELECT id FROM inquiries
    WHERE channel_type = 'email'
      AND external_inquiry_id >= ? AND external_inquiry_id < ?
      AND created_at < strftime('%Y-%m-%dT%H:%M:%SZ','now','-${DRAFT_TTL_HOURS} hours')
      AND NOT EXISTS (SELECT 1 FROM inquiry_messages m WHERE m.inquiry_id = inquiries.id)
      AND NOT EXISTS (SELECT 1 FROM outbox_replies o WHERE o.inquiry_id = inquiries.id)
    LIMIT 200`).all(...COMPOSE_RANGE);
  if (!rows.length) return 0;
  const tx = db.transaction(() => {
    let n = 0;
    for (const r of rows) {
      db.prepare('DELETE FROM outbox_attachments WHERE inquiry_id = ?').run(r.id);
      db.prepare('DELETE FROM inquiry_activity_logs WHERE inquiry_id = ?').run(r.id);
      db.prepare('DELETE FROM internal_notes WHERE inquiry_id = ?').run(r.id);
      n += db.prepare('DELETE FROM inquiries WHERE id = ?').run(r.id).changes;
    }
    return n;
  });
  return tx.immediate();
}

/** この店舗に外部スレッド未確定 (compose:) のチケットが残っているか。
 * 同期の毎スレッド処理で重い検索を走らせないための門番 (通常は0件で即 false) */
function hasPendingComposeThreads(db, channelType, shopId) {
  return !!db.prepare(`SELECT 1 FROM inquiries
    WHERE channel_type = ? AND shop_id = ? AND external_inquiry_id >= ? AND external_inquiry_id < ?
    LIMIT 1`).get(channelType, shopId, ...COMPOSE_RANGE);
}

/** 送信済みメッセージIDから、外部スレッド未確定のチケットを1件探す */
function findComposeInquiryByMessageIds(db, channelType, shopId, messageIds) {
  const ids = [...new Set((messageIds || []).map(v => String(v || '')).filter(Boolean))].slice(0, 50);
  if (!ids.length) return null;
  if (!hasPendingComposeThreads(db, channelType, shopId)) return null;
  return db.prepare(`SELECT i.* FROM inquiries i
      JOIN inquiry_messages m ON m.inquiry_id = i.id
    WHERE i.channel_type = ? AND i.shop_id = ?
      AND i.external_inquiry_id >= ? AND i.external_inquiry_id < ?
      AND m.external_message_id IN (${ids.map(() => '?').join(',')})
    ORDER BY i.id LIMIT 1`).get(channelType, shopId, ...COMPOSE_RANGE, ...ids) || null;
}

/**
 * 同期が取り込もうとしているスレッドが、このアプリから送った新規メール (仮IDのまま) かどうかを
 * 送信済みメッセージIDで探す。見つかったら実スレッドIDへ昇格させ、そのチケットを返す。
 * 「送信 → 差し替え前に同期が走る」順序でも会話が2つに割れないようにするための合流点
 * (Codexレビュー High-2)。sync/engine.js の取り込み処理から呼ばれる。
 * @returns {object|null} 昇格させた inquiry 行
 */
export function adoptComposeInquiryByMessageIds(db, { channelType, shopId, externalInquiryId, messageIds }) {
  if (!externalInquiryId) return null;
  const hit = findComposeInquiryByMessageIds(db, channelType, shopId, messageIds);
  if (!hit) return null;
  db.prepare('UPDATE inquiries SET external_inquiry_id = ?, is_archived = 0 WHERE id = ?')
    .run(String(externalInquiryId), hit.id);
  logActivity(hit.id, { actorType: 'system', actionType: 'compose_thread_adopted',
    after: { external_thread_id: String(externalInquiryId) } });
  return db.prepare('SELECT * FROM inquiries WHERE id = ?').get(hit.id);
}

/**
 * 既に実スレッドのチケットがある側から見て、同じメッセージを持つ compose チケットが
 * 残っていないか (= 会話が2つに割れていないか) を確かめ、割れていたら両方に⚠️要確認を立てる。
 * 自動マージはしない — メッセージ・添付・送信ジョブ・ログの移送は取り違えの危険があるため、
 * 人が中身を見て判断する (Codexレビュー Medium: 分裂状態を修復できる入口が無い)。
 * @returns {number|null} 割れている相手の inquiry_id
 */
export function flagComposeThreadSplit(db, { channelType, shopId, inquiryId, messageIds }) {
  const other = findComposeInquiryByMessageIds(db, channelType, shopId, messageIds);
  if (!other || other.id === inquiryId) return null;
  // 同じ組み合わせで何度も印を付け直さない (同期のたびにログが増えるのを防ぐ)
  const logged = db.prepare(`SELECT 1 FROM inquiry_activity_logs
    WHERE inquiry_id = ? AND action_type = 'compose_thread_split' LIMIT 1`).get(other.id);
  if (logged) return other.id;
  db.prepare('UPDATE inquiries SET needs_attention = 1 WHERE id IN (?, ?)').run(other.id, inquiryId);
  logActivity(other.id, { actorType: 'system', actionType: 'compose_thread_split',
    after: { split_with_inquiry_id: inquiryId, reason: '同じメールが2つの問い合わせに分かれています' } });
  logActivity(inquiryId, { actorType: 'system', actionType: 'compose_thread_split',
    after: { split_with_inquiry_id: other.id, reason: '同じメールが2つの問い合わせに分かれています' } });
  return other.id;
}

/**
 * 送信成功後に仮IDを実際の外部スレッドIDへ差し替える (outbox.js から呼ぶ)。
 * 既に同じスレッドIDのチケットがある (同期が先に取り込んだ) 場合は差し替えず、
 * ⚠️要確認を立てて人が気付けるようにする — UNIQUE 制約に当てて送信結果の記録ごと
 * 失敗させるより、印を付けて残すほうが安全 (Codexレビュー Medium-10)。
 * @returns {{ attached: boolean, conflictInquiryId?: number }}
 */
export function attachExternalThread(db, inquiryId, externalThreadId) {
  const tid = String(externalThreadId || '').trim();
  if (!tid) return { attached: false };
  const inq = db.prepare('SELECT * FROM inquiries WHERE id = ?').get(inquiryId);
  if (!inq || !isComposeThread(inq.external_inquiry_id)) return { attached: false };
  const clash = db.prepare('SELECT id FROM inquiries WHERE channel_type = ? AND shop_id = ? AND external_inquiry_id = ?')
    .get(inq.channel_type, inq.shop_id, tid);
  if (clash) {
    // 両方に印を付ける — 片側だけだと、同期が止まっている間は既存チケット側を見た人が
    // 会話が割れていることに気付けない
    markComposeThreadUnresolved(db, inquiryId, { external_thread_id: tid, existing_inquiry_id: clash.id });
    markComposeThreadUnresolved(db, clash.id, { external_thread_id: tid, split_with_inquiry_id: inquiryId });
    return { attached: false, conflictInquiryId: clash.id };
  }
  db.prepare('UPDATE inquiries SET external_inquiry_id = ? WHERE id = ?').run(tid, inquiryId);
  return { attached: true };
}

/** スレッドIDを確定できなかった新規メールに⚠️要確認を立てる (衝突・予期しないエラーの共通処理)。
 * ここまで失敗するのは DB 障害のときだけなので、呼び元はさらに握って送信結果の記録を守る */
export function markComposeThreadUnresolved(db, inquiryId, detail = {}) {
  db.prepare('UPDATE inquiries SET needs_attention = 1 WHERE id = ?').run(inquiryId);
  logActivity(inquiryId, { actorType: 'system', actionType: 'compose_thread_conflict', after: detail });
}
