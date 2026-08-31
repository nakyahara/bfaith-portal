/**
 * inquiry-hub ⏰締め前確認 (2026-08-28 中原さん要望)
 *
 * ⭐なぜ必要か (業務の流れ):
 *   お客さまからキャンセル・住所変更・お届け日時変更の連絡が来る
 *     → 人がネクストエンジンでキャンセル/修正する
 *     → そのあとロジザードへ流す (締め = 09:00 / 12:30 / 14:30 の3回)
 *   **人が必ず先に入るので、システムが出荷を止める必要はない。**
 *   必要なのは「締めまでに、そういう連絡が来ていると分かること」だけ。
 *
 * ⭐設計の要点:
 *   1. **検知はAIに頼らない**。決定的なキーワードで拾う。AIが止まっても動く
 *   2. **検知結果を保存しない**。毎回その場で判定する。キーワードを足せば過去分にも即反映される
 *   3. **取りこぼしを減らす方を優先**する。誤検知は「対象外」を1回押せば消えるが、
 *      取りこぼしは誰も気づけない
 *   4. だから **SQLで keyword 絞り込みをしない**。期間内の受信メッセージを全部JSで判定する。
 *      SQLの LIKE は正規化しないので、「お 届 け 先」「キャン セル」のような表記で
 *      候補にすら入らない取りこぼしが起きる (Codexレビュー指摘)
 */
import { getDB } from './db.js';
import { stripQuoted, normalizeForMatch } from './text-utils.js';
import { blockedReplyDestination } from './no-reply.js';

/** ロジザードの締め時刻 (JST)。⭐締め時刻の正本はここだけ。通知cronの時刻もここから導出する */
export const CUTOFF_TIMES = [
  { label: '09:00', minutes: 9 * 60 },
  { label: '12:30', minutes: 12 * 60 + 30 },
  { label: '14:30', minutes: 14 * 60 + 30 },
];

/** 締めの何分前に通知するか */
export const NOTICE_LEAD_MINUTES = 15;

/**
 * 判定ルールの版。**CUTOFF_KINDS を変えたら必ず上げる**。
 * 「対象外だった」と消したものは、ルールが変わったら もう一度見せるために使う
 * (古いルールでの「的外れ」判断を、新しいルールの結果にまで効かせない。Codexレビュー指摘)
 */
export const DETECTOR_VERSION = 2;

/** 何日前まで見るか */
export const LOOKBACK_DAYS = 14;
/** JS判定にかけるメッセージ数の上限 (超えたら truncated を立てて画面と通知で知らせる) */
const SCAN_LIMIT = 5000;
/** 画面に出す上限 */
const MAX_ROWS = 200;
/** 文の区切り (「対象の語」と「動作の語」は同じ文の中で揃ったときだけ検知する) */
const SENTENCE_SPLIT = /[。．.!?！？\n]+/;

/**
 * 検知する種別とキーワード。
 *
 * - strong : 1つ当たれば検知 (それ単体で用件が分かる語)
 * - target : 何について言っているか (住所・お届け日 など)
 * - action : どうしたいか (変更・訂正・指定 など)
 *   → **同じ文の中で target と action が揃ったとき**に検知する。
 *     単に「本文のどこかに2語ある」だと別々の話題が混ざる —
 *     「登録住所を確認しました。商品の色を変えてください」を住所変更と誤読する (Codexレビュー指摘)
 *
 * ⚠️ 否定表現 (「住所変更は不要です」) は敢えて除外しない。
 *   誤検知は「対象外」を1回押せば消えるが、否定判定を誤ると本物を落とすため。
 */
export const CUTOFF_KINDS = [
  {
    kind: 'cancel',
    label: 'キャンセル',
    icon: '🚫',
    style: 'background:#fee2e2;color:#b91c1c',
    strong: [
      'キャンセル', 'きゃんせる', 'cancel',
      '注文を取り消', '注文を取消', '注文の取消', '注文の取り消', 'ちゅうもんをとりけ',
      '注文をやめ', 'ちゅうもんをやめ', '購入をやめ', '購入を取り消', 'オーダーを取り消',
      '注文しないことに', '注文をなかったことに', '一旦白紙', '白紙にして',
      '注文した覚えがありません', '誤って購入', '誤って注文', '間違えて購入',
      '重複して注文', '二重に注文', '数量を0', '個数を0',
    ],
    target: ['注文', '購入', 'オーダー', 'ちゅうもん', '商品'],
    action: ['やめたい', 'やめます', 'やめさせて', '取り消', '取消', 'とりけ',
      '不要になり', 'いらなくなり', 'いりません', 'キャンセル', '返品したい'],
  },
  {
    kind: 'address',
    label: '住所・宛先の変更',
    icon: '🏠',
    style: 'background:#fef3c7;color:#92400e',
    strong: [
      '住所変更', '住所を変更', '住所の変更', '住所が間違', '住所を間違', '住所の間違',
      '宛先変更', '宛先を変更', 'お届け先を変更', 'お届け先の変更',
      '送り先を変更', '送り先の変更', '配送先を変更', '配送先の変更',
      '別の住所', '違う住所', '旧住所', '前の住所',
      '実家に送', '職場に送', '会社に送', 'ホテルに送',
      '引っ越し', '引越し', 'ひっこし', '転居',
      '営業所止め', '営業所留め', '局留め', '営業所受取',
      '部屋番号', '建物名', 'マンション名', '番地が抜け', '番地を追加',
      '受取人を変更', '受取人の変更', '名義を変更', '宛名を変更',
    ],
    target: ['住所', '宛先', 'お届け先', '送り先', '配送先', '届け先', '郵便番号', '〒',
      '番地', 'マンション', 'アパート', '部屋番号', '氏名', '名義', '宛名', '受取人'],
    action: ['変更', '変えて', '変えたい', '訂正', '間違え', 'まちがえ', '直して', '修正',
      '抜けて', '追加して', '転送', 'こちらです', 'に送って'],
  },
  {
    kind: 'datetime',
    label: 'お届け日時の指定・変更',
    icon: '📅',
    style: 'background:#dbeafe;color:#1d4ed8',
    strong: [
      '日時指定', '日時を指定', '日時の指定', '時間指定', '時間帯を変更',
      'お届け日を変更', 'お届け日の変更', 'お届け日時', '配達日を変更', '配達日の変更', '配達日時',
      '指定日を変更', '着日を変更', '受取日を変えたい', '配達を延期', '配達を早めて',
      '受け取れないので', '受け取れません', '不在にする', '不在になり', '留守にし',
      '置き配', '最短で', '指定なしに',
    ],
    // 日付・時刻の表記そのものを拾う (「8/30着で」「30日の18時以降」など。正規化後は半角)
    target: [/\d{1,2}[/月]\d{1,2}日?/, /\d{1,2}日/, /\d{1,2}時/,
      'お届け日', '配達日', '着日', '到着日', '指定日', '受取日', '希望日',
      '時間帯', '午前中', '午後', '夕方', '夜間', '平日', '土日', '明日', '明後日', '来週'],
    action: ['変更', '変えて', '変えたい', '指定', '希望', '着で', '着に', '届けて', '届くように',
      '配達', '配送', '受取', '受け取り', '以降', 'までに', '延期', '早めて', '遅らせ', '急ぎ'],
  },
];

const KIND_MAP = new Map(CUTOFF_KINDS.map(k => [k.kind, k]));
export const getKind = kind => KIND_MAP.get(String(kind || '')) || null;

/** 正規化済みの文から、語 (文字列 or 正規表現) の出現範囲をすべて拾う */
function spansOf(sentence, word) {
  const out = [];
  if (word instanceof RegExp) {
    const re = new RegExp(word.source, word.flags.includes('g') ? word.flags : word.flags + 'g');
    for (const m of sentence.matchAll(re)) out.push({ at: m.index, end: m.index + m[0].length, hit: m[0] });
    return out;
  }
  const w = normalizeForMatch(word);
  if (!w) return out;
  let i = sentence.indexOf(w);
  while (i >= 0) { out.push({ at: i, end: i + w.length, hit: word }); i = sentence.indexOf(w, i + w.length); }
  return out;
}

/**
 * 同じ文の中に、**文字として重なっていない** target と action があるか。
 * ⚠️重なりを見ないと自己マッチする: target「配達日」の中に action「配達」が入っているので、
 *   「配達日はいつですか」(ただの質問) が日時変更依頼として検知されてしまう。
 *   target「受取日」×action「受取」、target「指定日」×action「指定」も同じ
 * @returns {[string, string]|null} 当たった [target, action]
 */
function pairInSentence(sentence, kind) {
  const targets = kind.target.flatMap(w => spansOf(sentence, w));
  if (!targets.length) return null;
  const actions = kind.action.flatMap(w => spansOf(sentence, w));
  for (const t of targets) {
    for (const a of actions) {
      if (a.end <= t.at || a.at >= t.end) return [String(t.hit), String(a.hit)];
    }
  }
  return null;
}

/**
 * 1件のテキストがどの種別に当たるかを判定する。
 * @param {string} subject 件名 (呼び元が「この判定で件名を使うか」を決める。null可)
 * @param {string} body 本文 (引用込みで渡してよい。中で落とす)
 * @returns {Array<{kind, label, icon, style, matched: string[]}>}
 */
export function detectKinds(subject, body) {
  const raw = `${subject == null ? '' : subject}\n${stripQuoted(body, { keepAfterSeparator: true })}`;
  // 文ごとに正規化する (空白を落とす正規化を先にすると文の区切りが消えるため、分割が先)
  const sentences = raw.split(SENTENCE_SPLIT).map(normalizeForMatch).filter(Boolean);
  if (!sentences.length) return [];
  const whole = sentences.join('');
  const hits = [];
  for (const k of CUTOFF_KINDS) {
    const matched = [];
    // 1) 単独で用件が分かる語 — 文をまたいでもよい
    for (const w of k.strong) {
      if (whole.includes(normalizeForMatch(w))) { matched.push(w); break; }
    }
    // 2) 「何について」+「どうしたい」が**同じ文の中で**揃っているか
    if (!matched.length) {
      for (const s of sentences) {
        const pair = pairInSentence(s, k);
        if (pair) { matched.push(...pair); break; }
      }
    }
    if (matched.length) {
      hits.push({ kind: k.kind, label: k.label, icon: k.icon, style: k.style, matched: matched.slice(0, 6) });
    }
  }
  return hits;
}

// ─── 締め時刻 ───

/** いまが JST の何分目か */
export function jstMinutesOf(nowMs = Date.now()) {
  const j = new Date(nowMs + 9 * 3600000);
  return j.getUTCHours() * 60 + j.getUTCMinutes();
}

/**
 * 次の締めと、それまでの残り時間。14:30 を過ぎたら「翌朝09:00」。
 * @returns {{ label, minutesLeft, isTomorrow }}
 */
export function nextCutoff(nowMs = Date.now()) {
  const m = jstMinutesOf(nowMs);
  for (const c of CUTOFF_TIMES) {
    if (m < c.minutes) return { label: c.label, minutesLeft: c.minutes - m, isTomorrow: false };
  }
  return { label: CUTOFF_TIMES[0].label, minutesLeft: 24 * 60 - m + CUTOFF_TIMES[0].minutes, isTomorrow: true };
}

/**
 * 締め時刻から通知cronの式 (UTC) を導出する。
 * ⭐締め時刻を CUTOFF_TIMES 1か所で管理するため。時刻とcronを別々に書くと必ずずれる
 * (Codexレビュー指摘)。JSTは夏時間が無いので UTC への固定変換でよい。
 */
export function cutoffNoticeCrons(leadMinutes = NOTICE_LEAD_MINUTES) {
  return CUTOFF_TIMES.map(c => {
    const jst = (c.minutes - leadMinutes + 24 * 60) % (24 * 60);
    const utc = (jst - 9 * 60 + 24 * 60) % (24 * 60);
    return `${utc % 60} ${Math.floor(utc / 60)} * * *`;
  });
}

// ─── 差出人の除外 (2026-08-28 中原さん「的外れが多すぎる」) ───

/**
 * ⭐**メールチャネルには顧客のメールも業者の連絡もAmazonの販促も同じように届く。**
 *   「お客さんから問い合わせ以外で普通にメールが届くこともある」(中原さん) ので、
 *   チャネルごと外すと本物の顧客メールまで消える。だから差出人ごとに外す。
 *
 * 除外は2段構え:
 *   1. 自動 … no-reply / 通知専用ドメイン / バウンス (顧客ではないと確実に言えるもの)
 *   2. 手動 … cutoff_excludes。画面の各行から1タップで足せる (使いながら育てる)
 */

/** 除外リストを比較用のキー集合にして返す */
export function loadExcludeKeys() {
  return new Set(getDB().prepare('SELECT pattern_key FROM cutoff_excludes').all().map(r => r.pattern_key));
}

/** その差出人を締め前確認から外すか。@から始まるパターンはドメイン一致 */
export function isExcludedSender(identifier, keys) {
  const a = String(identifier || '').trim().toLowerCase();
  if (!a) return false;                       // 差出人不明は外さない (取りこぼしを作らない)
  if (keys.has(a)) return true;
  const at = a.lastIndexOf('@');
  return at >= 0 && keys.has(a.slice(at));    // '@example.com'
}

/**
 * 顧客からの連絡でないと**確実に**言えるものだけ true。
 * ⚠️狭く判定する — 迷うものは通す (取りこぼしは誰も気づけないため)。
 * 楽天のマスクアドレス (…@pc.fw.rakuten.ne.jp) は顧客なので、ここでは外れない
 */
export function isNonCustomerSender(identifier) {
  return !!blockedReplyDestination(identifier);
}

export function listCutoffExcludes() {
  return getDB().prepare('SELECT * FROM cutoff_excludes ORDER BY pattern_key').all();
}

/** 除外を1件足す。'@example.com' でドメインごとも指定できる */
export function addCutoffExclude(pattern, note = null, actorId = null) {
  const p = String(pattern || '').trim().toLowerCase().replace(/^mailto:/, '');
  if (!p) throw new Error('メールアドレス (または @ドメイン) を入れてください');
  if (p.length > 200) throw new Error('長すぎます (200文字まで)');
  const isDomain = p.startsWith('@');
  const ok = isDomain ? /^@[\w.-]+\.[a-z]{2,}$/i.test(p) : /^[^\s@]+@[\w.-]+\.[a-z]{2,}$/i.test(p);
  if (!ok) throw new Error(`メールアドレスの形式ではありません: ${p} (ドメイン全部なら @example.com のように書きます)`);
  getDB().prepare(`INSERT OR IGNORE INTO cutoff_excludes (pattern, pattern_key, note, created_by)
    VALUES (?,?,?,?)`).run(p, p, note ? String(note).slice(0, 200) : null, actorId);
  clearCutoffCountCache();
  return { pattern: p, isDomain };
}

/** 除外を外す (再び出るようになる)。既に無くても成功 (冪等) */
export function removeCutoffExclude(id) {
  const r = getDB().prepare('DELETE FROM cutoff_excludes WHERE id = ?').run(Number(id));
  clearCutoffCountCache();
  return { removed: r.changes };
}

// ─── 一覧 ───

/**
 * 締め前に確認すべきものを返す。
 *
 * 対象 = 直近days日の「顧客から届いた」メッセージのうち、
 *   ・まだ完了にしていない問い合わせ (完了 = 対応済みとみなす。画面にその旨を明記する)
 *   ・その種別をまだ片付けていないもの
 *
 * ⚠️ **SQLでキーワード絞り込みをしない**。期間内の受信メッセージを全部JSで判定する
 *   (SQLの LIKE は表記ゆれを吸収できず、候補に入らない取りこぼしが起きるため)。
 *
 * @returns {{ items: Array, truncated: boolean, scanned: number }}
 *   truncated = 上限に達して全部は見きれていない (画面と通知で「0件だから安全」と言わせないため)
 */
export function listCutoffItems({ nowMs = Date.now(), days = LOOKBACK_DAYS,
  includeAcked = false, includeDone = false, includeExcluded = false } = {}) {
  const db = getDB();
  const d = Math.min(90, Math.max(1, Number(days) || LOOKBACK_DAYS));
  // 期間の下限は JS 側で決める (SQLite の 'now' と nowMs がずれないように。Codexレビュー指摘)
  const since = new Date(nowMs - d * 86400000).toISOString().replace(/\.\d{3}Z$/, 'Z');

  const rows = db.prepare(`SELECT m.id AS message_id, m.message_body_text, m.received_at, m.sent_at,
      i.id AS inquiry_id, i.subject, i.customer_name, i.customer_identifier,
      i.order_number, i.product_name,
      i.channel_type, i.internal_status, i.assigned_user_id, s.shop_name,
      (SELECT MIN(m2.id) FROM inquiry_messages m2
        WHERE m2.inquiry_id = i.id AND m2.is_incoming = 1) AS first_incoming_id
    FROM inquiry_messages m
    JOIN inquiries i ON i.id = m.inquiry_id
    LEFT JOIN shops s ON s.id = i.shop_id
    WHERE m.is_incoming = 1
      AND i.is_archived = 0
      ${includeDone ? '' : "AND i.internal_status <> 'done'"}
      AND datetime(COALESCE(m.received_at, m.sent_at)) >= datetime(?)
    ORDER BY COALESCE(m.received_at, m.sent_at) DESC, m.id DESC
    LIMIT ?`).all(since, SCAN_LIMIT + 1);

  const truncated = rows.length > SCAN_LIMIT;
  const scan = truncated ? rows.slice(0, SCAN_LIMIT) : rows;

  const acked = new Map();
  for (const a of db.prepare('SELECT message_id, kind, status, acked_by, acked_at, detector_version FROM cutoff_acks').all()) {
    acked.set(`${a.message_id}:${a.kind}`, a);
  }

  const excludeKeys = loadExcludeKeys();
  const items = [];
  let excluded = 0;
  for (const r of scan) {
    // ⭐顧客以外の差出人 (業者の連絡・Amazonの販促・自動配信) は出さない。
    //   ⚠️ただしメールチャネルには本物の顧客メールも届くので、チャネルでなく差出人で外す
    if (!includeExcluded && (isExcludedSender(r.customer_identifier, excludeKeys)
      || isNonCustomerSender(r.customer_identifier))) { excluded++; continue; }
    // ⚠️件名はスレッド最初の受信メッセージにだけ使う。
    //   全メッセージに件名を効かせると、「注文キャンセル」という件名のスレッドに
    //   お礼の返信が来るたびに新しい未対応が湧く (Codexレビュー指摘)
    const useSubject = r.message_id === r.first_incoming_id;
    const hits = detectKinds(useSubject ? r.subject : null, r.message_body_text);
    for (const h of hits) {
      const ack = acked.get(`${r.message_id}:${h.kind}`) || null;
      // 「対象外だった」は判定ルールが変わったらもう一度見せる (古い判断を新しい結果に効かせない)。
      // 「対応した」は人が実際に処理した事実なので、ルールが変わっても有効
      const ackStale = !!ack && ack.status === 'not_applicable' && (ack.detector_version || 0) < DETECTOR_VERSION;
      if (ack && !ackStale && !includeAcked) continue;
      items.push({
        messageId: r.message_id,
        inquiryId: r.inquiry_id,
        kind: h.kind, kindLabel: h.label, icon: h.icon, style: h.style, matched: h.matched,
        subject: r.subject, customerName: r.customer_name, orderNumber: r.order_number,
        productName: r.product_name, channel: r.channel_type, shopName: r.shop_name,
        sender: r.customer_identifier,
        status: r.internal_status, assignedTo: r.assigned_user_id,
        receivedAt: r.received_at || r.sent_at,
        body: r.message_body_text,
        ack: ackStale ? null : ack,
        ackStale,
      });
      if (items.length >= MAX_ROWS) return { items, truncated: true, scanned: scan.length, excluded };
    }
  }
  return { items, truncated, scanned: scan.length, excluded };
}

/** 一覧の結果から件数を数える (同じ検索を2回走らせないための純粋関数) */
export function summarize(items = []) {
  const byKind = {};
  for (const k of CUTOFF_KINDS) byKind[k.kind] = 0;
  const inquiries = new Set();
  for (const it of items) { byKind[it.kind] = (byKind[it.kind] || 0) + 1; inquiries.add(it.inquiryId); }
  return { total: items.length, inquiries: inquiries.size, byKind };
}

/**
 * 種別ごとの未対応件数 (サイドバーのバッジ用)。
 * ⚠️サイドバーは**全ページ**で描画される。better-sqlite3 は同期APIなので、
 *   重い集計を全ページに置くとイベントループを塞いで問い合わせ画面や受信同期まで巻き添えになる。
 *   30秒だけ使い回す (締めは分単位の話なのでバッジが30秒古くても実務上の影響はない)
 */
const COUNT_CACHE_MS = 30000;
let countCache = null;

export function countCutoffItems(opts = {}) {
  const useCache = !opts.nowMs && !opts.days && !opts.includeAcked && !opts.includeDone && !opts.includeExcluded;
  if (useCache && countCache && Date.now() - countCache.at < COUNT_CACHE_MS) return countCache.value;
  const r = listCutoffItems(opts);
  const value = { ...summarize(r.items), truncated: r.truncated };
  if (useCache) countCache = { at: Date.now(), value };
  return value;
}

/** テスト用・押した直後にバッジを合わせたいとき */
export function clearCutoffCountCache() { countCache = null; }

// ─── 片付ける ───

/**
 * 「ネクストエンジンで対応した」/「対象外だった」を記録する。
 * 同じものを2回押しても成功 (冪等)。押し間違いは status を上書きして直せる。
 */
export function ackCutoffItem({ messageId, kind, status = 'done', note = null }, actorId = null) {
  if (!['done', 'not_applicable'].includes(status)) {
    throw new Error('status は done / not_applicable のどちらかです');
  }
  if (!getKind(kind)) throw new Error(`不明な種別です: ${kind}`);
  const db = getDB();
  const mid = Number(messageId);
  if (!Number.isInteger(mid)) throw new Error('messageId が不正です');
  const msg = db.prepare('SELECT id, inquiry_id FROM inquiry_messages WHERE id = ?').get(mid);
  if (!msg) throw new Error('メッセージが見つかりません');
  db.prepare(`INSERT INTO cutoff_acks (message_id, inquiry_id, kind, status, note, acked_by, detector_version)
      VALUES (?,?,?,?,?,?,?)
    ON CONFLICT(message_id, kind) DO UPDATE SET
      status = excluded.status, note = excluded.note, acked_by = excluded.acked_by,
      detector_version = excluded.detector_version,
      acked_at = strftime('%Y-%m-%dT%H:%M:%SZ','now')`)
    .run(mid, msg.inquiry_id, kind, status, note ? String(note).slice(0, 300) : null, actorId, DETECTOR_VERSION);
  clearCutoffCountCache();
  return { messageId: mid, inquiryId: msg.inquiry_id, kind, status };
}

/** 「やっぱり未対応に戻す」 */
export function unackCutoffItem({ messageId, kind }) {
  const r = getDB().prepare('DELETE FROM cutoff_acks WHERE message_id = ? AND kind = ?')
    .run(Number(messageId), String(kind || ''));
  clearCutoffCountCache();
  return { removed: r.changes };
}

// ─── 通知 ───

/**
 * 締め前通知の本文を組み立てる。
 * ⭐0件でも通知する — 来なければ「仕組みが止まった」と気づけるようにするため (dead-man)。
 * ⚠️ 0件は「システムが検知した未対応が0件」であって、安全の保証ではない。文言でもそう伝える
 *   (キーワード方式・14日制限・同期の遅れがあるため。Codexレビュー指摘)
 */
export function buildCutoffNotice({ nowMs = Date.now(), baseUrl = '' } = {}) {
  const next = nextCutoff(nowMs);
  const { items, truncated } = listCutoffItems({ nowMs });
  const counts = summarize(items);
  const url = `${String(baseUrl || '').replace(/\/+$/, '')}/apps/inquiry-hub/cutoff`;
  const when = `${next.label}${next.isTomorrow ? ' (翌朝)' : ''}`;

  if (!items.length) {
    return `*⏰ ${when} の締め前確認*\n\n`
      + 'キャンセル・住所変更・お届け日時の連絡は *検知0件* です。いつもどおり確認して流してください。\n'
      + `${url}`;
  }
  const lines = [`*⏰ ${when} の締め前確認 — ${counts.inquiries}件*`, ''];
  lines.push('ロジザードへ流す前に、ネクストエンジンで直してください。', '');
  for (const k of CUTOFF_KINDS) {
    const list = items.filter(i => i.kind === k.kind);
    if (!list.length) continue;
    lines.push(`${k.icon} *${k.label}* ${list.length}件`);
    for (const it of list.slice(0, 8)) {
      const who = it.customerName ? ` / ${it.customerName}` : '';
      const order = it.orderNumber ? ` / 注文 ${it.orderNumber}` : ' / 注文番号なし';
      lines.push(`　・${String(it.subject || '(件名なし)').slice(0, 40)}${who}${order}`);
    }
    if (list.length > 8) lines.push(`　…ほか ${list.length - 8}件`);
    lines.push('');
  }
  if (truncated) lines.push('⚠️ 件数が多く、全部は見きれていません。画面で確認してください。', '');
  lines.push(url);
  return lines.join('\n');
}
