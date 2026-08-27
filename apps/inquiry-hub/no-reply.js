/**
 * inquiry-hub 返信不可 (no-reply) 宛先の判定 + バウンス (配信失敗通知) の判定
 *
 * ■ 作った理由 (2026-08-26 本番事故)
 *   メルカリShopsの問い合わせ通知メール (no-reply@mercari-shops.com 発) がメールチャネルの
 *   問い合わせとして取り込まれ、スタッフが画面から返信 → 宛先が no-reply@mercari-shops.com に
 *   なり、Gmail が 45 時間リトライしたうえで配信不能 (mercari-shops.com の MX は SMTP を
 *   受け付けない) になった。顧客には何も届いていないのに、画面上は「送信済み」のままだった。
 *
 * ■ 対策の考え方
 *   (1) 送る前に止める  … no-reply 系の宛先は未送信確定で拒否し、どこで返信すべきかを案内する
 *   (2) 届かなかったら気付ける … バウンス通知を検知して問い合わせに「配信失敗」の印を付ける
 *   no-reply 判定は網羅できない (未知の通知専用アドレスは必ずある) ため、(1) だけに頼らず
 *   (2) を安全網として必ず併用する。
 *
 * ■ 判定の方針 (誤爆を避ける)
 *   - 顧客の実アドレスを誤って「返信不可」にすると返信できなくなるので、パターンは狭く取る
 *   - ローカル部の前方一致は no-reply 系のみ。mailer-daemon/postmaster/bounce は完全一致
 *   - ドメイン単位の「通知専用」は実証済みのものだけ (KNOWN_NOTIFY_ONLY_DOMAINS) に列挙する
 */

/** ドメインごと通知専用 (どんなローカル部でも顧客には届かない) と実証済みのもの。
 * 新しいモールを足すときは「返信はどこで行うのか」(guide) をセットで書くこと */
export const KNOWN_NOTIFY_ONLY_DOMAINS = {
  'mercari-shops.com': {
    name: 'メルカリShops',
    guide: 'メルカリShopsのストア管理画面 →「取引」→ 該当取引の「取引メッセージ」から返信してください',
  },
  'mercari.com': {
    name: 'メルカリ',
    guide: 'メルカリのアプリ/管理画面の取引メッセージから返信してください',
  },
  'mercari.jp': {
    name: 'メルカリ',
    guide: 'メルカリのアプリ/管理画面の取引メッセージから返信してください',
  },
};

/** 楽天あんしんメルアドサービスのマスクアドレス (…@pc.fw.rakuten.ne.jp / @mb.fw.rakuten.ne.jp 等)。
 * 返信 (既存スレッド) は楽天公式SMTP経由で送れるが (adapters/gmail.js)、Gmailスレッドが残らないため
 * 新規メールの宛先には使えない (compose.js)。両方から参照するのでここに置く */
export const RAKUTEN_MASKED_RE = /@(?:[a-z0-9-]+\.)?fw\.rakuten\.ne\.jp$/i;

/** ローカル部を比較用に正規化 ('No.Reply+tag' → 'noreply')。区切り記号と +タグを落とす */
function normalizeLocalPart(local) {
  return String(local || '').toLowerCase().split('+')[0].replace(/[._-]/g, '');
}

/** no-reply 系 (前方一致でよいもの)。'noreply-shop@' のような連番付きも拾う */
const NO_REPLY_PREFIXES = ['noreply', 'donotreply', 'dontreply', 'noresponse', 'nonreply'];
/** 完全一致でのみ返信不可とするもの (前方一致にすると顧客アドレスを巻き込む) */
const NO_REPLY_EXACT = new Set(['mailerdaemon', 'postmaster', 'bounce', 'bounces', 'nobody']);

/**
 * 返信先メールアドレスが顧客に届き得るかを判定する。
 * @param {string} address
 * @returns {{ sendable: boolean, kind?: string, reason?: string, guide?: string }}
 *   sendable=false のとき reason=なぜ送れないか / guide=ではどこで返信するか
 */
export function classifyReplyDestination(address) {
  const addr = String(address || '').trim().toLowerCase();
  const at = addr.lastIndexOf('@');
  if (at <= 0 || at === addr.length - 1) {
    return { sendable: false, kind: 'invalid', reason: `宛先メールアドレスが不正です (${addr || '空'})`,
      guide: '問い合わせ元のモール管理画面から返信してください' };
  }
  const local = addr.slice(0, at);
  const domain = addr.slice(at + 1);

  const notifyOnly = KNOWN_NOTIFY_ONLY_DOMAINS[domain];
  if (notifyOnly) {
    return { sendable: false, kind: 'notify_only_domain',
      reason: `${notifyOnly.name}の通知専用アドレス (${addr}) 宛です。メールでは顧客に届きません`,
      guide: notifyOnly.guide };
  }

  const norm = normalizeLocalPart(local);
  if (NO_REPLY_EXACT.has(norm) || NO_REPLY_PREFIXES.some(p => norm.startsWith(p))) {
    return { sendable: false, kind: 'no_reply_address',
      reason: `返信を受け付けないアドレス (${addr}) 宛です。メールでは顧客に届きません`,
      guide: '問い合わせ元のモール管理画面 (取引メッセージ等) から返信してください。'
        + '顧客の実アドレスが分かる場合は、そちらへ新規メールで連絡してください' };
  }
  return { sendable: true };
}

/**
 * 「確実に届かない」と分かっている宛先だけを返す (それ以外は null = 送信を止めない)。
 * 空・解析不能な customer_identifier は塞がない: 送信時にスレッドの実効差出人から宛先を
 * 復元する経路があり (adapters/gmail.js の宛先フォールバック。2026-08-15)、ここで
 * 止めると復元できるはずの返信まで送れなくなる。
 * 返信ジョブ作成のガードと画面の警告表示はこちらを使う。
 */
export function blockedReplyDestination(address) {
  if (!String(address || '').trim()) return null;
  const v = classifyReplyDestination(address);
  return (v.sendable || v.kind === 'invalid') ? null : v;
}

/** 画面表示用の1行 (返信パネルの警告バナー) */
export function noReplyWarningText(verdict) {
  if (!verdict || verdict.sendable) return '';
  return `${verdict.reason}。${verdict.guide}`;
}

/**
 * バウンス (配信失敗通知) メッセージの判定。
 * Gmail は元スレッドに紐づけて返すため、返信したスレッドの中に混ざって取り込まれる。
 * @param {{ from?: string, contentType?: string, subject?: string, hasFailedRecipients?: boolean }} sig
 */
export function isBounceSignature({ from = '', contentType = '', subject = '', hasFailedRecipients = false } = {}) {
  if (hasFailedRecipients) return true;
  const addr = String(from).trim().toLowerCase();
  const at = addr.lastIndexOf('@');
  if (at > 0) {
    const norm = normalizeLocalPart(addr.slice(0, at));
    // MAILER-DAEMON@ / postmaster@ は RFC で配信通知に使われる送信者
    if (norm === 'mailerdaemon' || norm === 'postmaster') return true;
  }
  // RFC 3464 の配信状態通知 (multipart/report; report-type=delivery-status)
  const ct = String(contentType).toLowerCase().replace(/\s+/g, '');
  if (ct.includes('multipart/report') && ct.includes('report-type=delivery-status')) return true;
  // 上2つが取れない中継経由の保険 (件名のみでの判定はここだけ。誤爆しても senderType が
  // system になり「配信失敗」の印が付くだけで、取り込み自体は行われる)
  const sub = String(subject).toLowerCase();
  return /delivery status notification \(failure\)|undelivered mail returned to sender|returned mail: see transcript|配信未完了|配信不能/.test(sub);
}
