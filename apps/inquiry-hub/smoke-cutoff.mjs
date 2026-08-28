// ⏰締め前確認 (cutoff.js + text-utils.js + router /cutoff + 締め前通知cron) のスモーク
// 使い方: DATA_DIR=<作業ディレクトリ> node apps/inquiry-hub/smoke-cutoff.mjs
import fs from 'fs';
import path from 'path';
import express from 'express';

if (!process.env.DATA_DIR) {
  console.error('FATAL: DATA_DIR が未指定です (例: DATA_DIR=c:/tmp/ih-cutoff-smoke)');
  process.exit(2);
}
const baseDir = process.env.DATA_DIR;
fs.mkdirSync(baseDir, { recursive: true });
const workDir = fs.mkdtempSync(path.join(baseDir, 'smoke-cutoff-'));
process.env.DATA_DIR = workDir;
const baseDbExistedAtStart = fs.existsSync(path.join(baseDir, 'inquiry-hub.db'));

const { initInquiryHubDB, getDB } = await import('./db.js');
const cut = await import('./cutoff.js');
const { stripQuoted, toPreviewLine, normalizeForMatch } = await import('./text-utils.js');
const routerModule = await import('./router.js');
const cronModule = await import('./sync/cron.js');

initInquiryHubDB();
const db = getDB();

let passed = 0, failed = 0;
const check = (name, cond, detail) => {
  if (cond) { passed++; console.log(`  PASS ${name}`); }
  else { failed++; console.error(`  FAIL ${name}${detail ? ' — ' + detail : ''}`); }
};
const throws = fn => { try { fn(); return false; } catch { return true; } };
const kindsOf = (subject, body) => cut.detectKinds(subject, body).map(h => h.kind).sort();

// ─── 1. 本文の整形 ───
console.log('1. 本文の整形');
{
  check('引用行を落とす', stripQuoted('キャンセルしたい\n> 前のメール\n続き') === 'キャンセルしたい\n続き');
  check('引用ヘッダ以降を落とす',
    stripQuoted('返品したい\n2026年8月20日(木) 18:20 雑貨イズム:\nお届け先は東京都…') === '返品したい');
  check('区切り線以降も落とす', stripQuoted('本文\n-----\n署名の住所') === '本文');
  check('全角/半角・大小文字を吸収', normalizeForMatch('キャンセル　ＯＫ') === 'キャンセルok');
  check('空白を詰める (「お 届 け 先」も拾える)', normalizeForMatch('お 届 け 先') === 'お届け先');
  // 既存の一覧プレビューが壊れていないこと (router.previewOf は toPreviewLine に委譲した)
  check('previewOf: 引用行・URL・改行を落として1行に',
    routerModule.previewOf('商品が届きません\nhttps://example.com/track\n> 前回のメール\n引用の続き') === '商品が届きません 引用の続き');
  check('previewOf: 長文は…で切る', routerModule.previewOf('あ'.repeat(200)).endsWith('…'));
  check('toPreviewLine: 空・nullは空文字', toPreviewLine(null) === '' && toPreviewLine('') === '');
}

// ─── 2. 検知 (取りこぼしを減らす方を優先) ───
console.log('2. 検知');
{
  check('キャンセル', kindsOf('', '注文をキャンセルしたいのですが').includes('cancel'));
  check('ひらがなのキャンセル', kindsOf('', 'きゃんせるしたいです').includes('cancel'));
  check('「注文を取り消し」', kindsOf('', '先ほどの注文を取り消してください').includes('cancel'));
  check('弱い語2つ (間違えて注文+重複)', kindsOf('', '間違えて注文してしまい、二重に注文しました').includes('cancel'));

  check('住所変更', kindsOf('', '住所を変更してください').includes('address'));
  check('お届け先の変更', kindsOf('', 'お届け先を変更したいです').includes('address'));
  check('引っ越し', kindsOf('', '引っ越したので新しい所に送ってください').includes('address'));
  check('実家に送って', kindsOf('', '実家に送ってもらえますか').includes('address'));
  check('件名だけでも拾う', kindsOf('住所変更のお願い', 'よろしくお願いします').includes('address'));

  check('日時指定', kindsOf('', '日時指定を追加できますか').includes('datetime'));
  check('お届け日の変更', kindsOf('', 'お届け日を変更したいです').includes('datetime'));
  check('受け取れない', kindsOf('', 'その日は受け取れませんので別の日に').includes('datetime'));
  check('時間指定', kindsOf('', '時間指定を午前中でお願いします').includes('datetime'));

  check('複数種別が同時に当たる',
    kindsOf('', 'お届け先を変更して、日時指定も追加してください').join() === 'address,datetime');

  // ⭐偽陽性を減らす: 弱い語1つだけでは拾わない
  check('「変更」だけでは拾わない', kindsOf('', 'パスワードの変更方法を教えてください').length === 0);
  check('署名の住所だけでは拾わない', kindsOf('', '商品ありがとうございました') === undefined || kindsOf('', '商品ありがとうございました').length === 0);
  check('普通の問い合わせは拾わない', kindsOf('', 'この商品の使い方を教えてください').length === 0);
  check('在庫の質問は拾わない', kindsOf('', '在庫はありますか').length === 0);

  // ⭐引用の中の店舗側の文言で誤検知しない (偽陽性の主因)
  check('引用された店舗の案内では拾わない',
    kindsOf('', 'ありがとうございます\n> お届け先の変更はキャンセル後に承ります\n> 日時指定も可能です').length === 0);
  check('引用があっても今回の発言は拾う',
    kindsOf('', 'やっぱりキャンセルします\n> 前回のご案内').includes('cancel'));

  check('空文字・nullは何も当たらない', cut.detectKinds('', '').length === 0 && cut.detectKinds(null, null).length === 0);

  // ⭐Codexレビューで挙がった取りこぼし例 (実務でありそうな言い回し)
  const recall = [
    ['注文しないことにしました', 'cancel'], ['購入をやめさせてください', 'cancel'],
    ['注文した覚えがありません', 'cancel'], ['誤って購入しました', 'cancel'],
    ['数量を0にしてください', 'cancel'], ['一旦白紙にしてください', 'cancel'],
    ['郵便番号を直してください', 'address'], ['番地が抜けています', 'address'],
    ['部屋番号を追加してください', 'address'], ['建物名が違います', 'address'],
    ['受取人を変更してください', 'address'], ['旧住所で注文してしまいました', 'address'],
    ['営業所止めにしてください', 'address'], ['ホテルに送ってください', 'address'],
    ['明日届けてください', 'datetime'], ['8/30着でお願いします', 'datetime'],
    ['30日の18時以降でお願いします', 'datetime'], ['平日配送でお願いします', 'datetime'],
    ['置き配にしてください', 'datetime'], ['配達を延期してください', 'datetime'],
    ['受取日を変えたい', 'datetime'], ['最短でお願いします', 'datetime'],
  ];
  const missed = recall.filter(([s, k]) => !kindsOf('', s).includes(k)).map(([s]) => s);
  check(`実務の言い回し ${recall.length}種をすべて拾う`, missed.length === 0, missed.join(' / '));

  // ⭐同じ文の中で揃ったときだけ拾う (別々の話題を混ぜない)
  check('別の文の語を組み合わせない',
    kindsOf('', '登録住所を確認しました。商品の色を変えてください').length === 0);
  check('同じ文なら拾う', kindsOf('', '住所ですが、変更をお願いします').includes('address'));

  // ⭐target と action が文字として重なるときは検知しない (自己マッチの防止)
  check('「配達日はいつですか」は拾わない (target内のactionで自己マッチしない)',
    kindsOf('', '配達日はいつですか。指定できない商品でしょうか').length === 0);
  check('「お届け日はいつになりますか」も拾わない', kindsOf('', 'お届け日はいつになりますか').length === 0);
  check('「受取日を教えてください」も拾わない', kindsOf('', '受取日を教えてください').length === 0);
  check('ただし「お届け日を30日に変更」は拾う', kindsOf('', 'お届け日を30日に変更してください').includes('datetime'));

  // ⭐区切り線の下に本文がある形式でも拾う (フォームメール・モール通知)
  check('区切り線の下の依頼も拾う',
    kindsOf('', '注文番号: 123\n----------------\n住所を変更してください').includes('address'));
}

// ─── 3. 次の締め ───
console.log('3. 次の締め');
{
  const at = (h, m) => Date.UTC(2026, 7, 28, h - 9, m); // JST h:m を UTC に
  check('07:30 → 次は09:00 (あと90分)',
    cut.nextCutoff(at(7, 30)).label === '09:00' && cut.nextCutoff(at(7, 30)).minutesLeft === 90);
  check('09:00 ちょうど → 次は12:30', cut.nextCutoff(at(9, 0)).label === '12:30');
  check('12:29 → 次は12:30 (あと1分)', cut.nextCutoff(at(12, 29)).minutesLeft === 1);
  check('12:30 ちょうど → 次は14:30', cut.nextCutoff(at(12, 30)).label === '14:30');
  check('14:30 ちょうど → 翌朝09:00', cut.nextCutoff(at(14, 30)).label === '09:00' && cut.nextCutoff(at(14, 30)).isTomorrow);
  check('23:00 → 翌朝09:00 (あと600分)',
    cut.nextCutoff(at(23, 0)).isTomorrow && cut.nextCutoff(at(23, 0)).minutesLeft === 600);
  check('締めは3回', cut.CUTOFF_TIMES.length === 3);
}

// ─── 4. 一覧 ───
console.log('4. 一覧');
let mCancel, mAddr, mOld, mDone;
{
  db.prepare(`INSERT INTO shops (channel_type, shop_name, account_identifier) VALUES ('email','テスト店','info@example.com')`).run();
  const shopId = db.prepare('SELECT id FROM shops').get().id;
  const mk = (ext, body, { status = 'open', daysAgo = 0, subject = '件名', order = null, incoming = 1 } = {}) => {
    const at = new Date(Date.now() - daysAgo * 86400000).toISOString().replace(/\.\d{3}Z$/, 'Z');
    const id = db.prepare(`INSERT INTO inquiries (channel_type, shop_id, external_inquiry_id, subject,
        internal_status, order_number, customer_name, received_at, last_message_at)
      VALUES ('email', ?, ?, ?, ?, ?, 'テスト太郎', ?, ?)`).run(shopId, ext, subject, status, order, at, at).lastInsertRowid;
    const mid = db.prepare(`INSERT INTO inquiry_messages (inquiry_id, external_message_id, sender_type,
        is_incoming, message_body_text, received_at) VALUES (?, ?, ?, ?, ?, ?)`)
      .run(id, `msg-${ext}`, incoming ? 'customer' : 'shop', incoming, body, at).lastInsertRowid;
    return { inquiryId: id, messageId: mid };
  };

  mCancel = mk('c1', '注文をキャンセルしたいです', { order: 'A-1' });
  mAddr = mk('a1', 'お届け先を変更してください', { subject: '住所の件' });
  mk('d1', '日時指定を追加でお願いします');
  mk('n1', 'この商品の使い方を教えてください');                       // 拾わない
  mOld = mk('o1', 'キャンセルします', { daysAgo: 30 });               // 古すぎ
  mDone = mk('done1', 'キャンセルします', { status: 'done' });        // 完了済み = 処理済み
  mk('s1', 'お届け先の変更を承りました', { incoming: 0 });            // 店舗発 = 対象外

  const items = cut.listCutoffItems().items;
  const ids = items.map(i => i.messageId);
  check('キャンセルを拾う', ids.includes(mCancel.messageId));
  check('住所変更を拾う', ids.includes(mAddr.messageId));
  check('日時指定を拾う', items.some(i => i.kind === 'datetime'));
  check('普通の問い合わせは拾わない', items.length === 3, JSON.stringify(items.map(i => [i.kind, i.subject])));
  check('14日より古いものは拾わない', !ids.includes(mOld.messageId));
  check('完了済みは拾わない (処理済みとみなす)', !ids.includes(mDone.messageId));
  check('店舗が送ったメッセージは拾わない', !items.some(i => i.body.includes('承りました')));
  check('注文番号が乗る', items.find(i => i.messageId === mCancel.messageId).orderNumber === 'A-1');
  check('引っかかった言葉が分かる', items[0].matched.length > 0);

  const counts = cut.countCutoffItems();
  check('種別ごとに数えられる', counts.byKind.cancel === 1 && counts.byKind.address === 1 && counts.byKind.datetime === 1,
    JSON.stringify(counts));
  check('問い合わせ数も出る', counts.inquiries === 3);
}

// ─── 5. 対応済みにする ───
console.log('5. 対応済み');
{
  cut.ackCutoffItem({ messageId: mCancel.messageId, kind: 'cancel', status: 'done' }, 'tester');
  check('対応済みにすると一覧から消える',
    !cut.listCutoffItems().items.some(i => i.messageId === mCancel.messageId));
  check('includeAcked では出る',
    cut.listCutoffItems({ includeAcked: true }).items.some(i => i.messageId === mCancel.messageId));
  check('対応済みの印が付く',
    cut.listCutoffItems({ includeAcked: true }).items.find(i => i.messageId === mCancel.messageId).ack.status === 'done');

  // 押し間違いを直せる / 二度押ししても壊れない
  cut.ackCutoffItem({ messageId: mCancel.messageId, kind: 'cancel', status: 'not_applicable' }, 'tester');
  check('押し間違いは上書きで直せる',
    cut.listCutoffItems({ includeAcked: true }).items.find(i => i.messageId === mCancel.messageId).ack.status === 'not_applicable');
  cut.ackCutoffItem({ messageId: mCancel.messageId, kind: 'cancel', status: 'done' }, 'tester');
  check('二度押ししても1行のまま (冪等)',
    db.prepare('SELECT COUNT(*) AS c FROM cutoff_acks WHERE message_id = ?').get(mCancel.messageId).c === 1);

  check('未対応に戻せる', cut.unackCutoffItem({ messageId: mCancel.messageId, kind: 'cancel' }).removed === 1);
  check('戻すと一覧に再び出る', cut.listCutoffItems().items.some(i => i.messageId === mCancel.messageId));
  check('存在しないものを戻しても壊れない',
    cut.unackCutoffItem({ messageId: 999999, kind: 'cancel' }).removed === 0);

  // ⭐「対象外だった」は判定ルールが変わったらもう一度見せる (古い判断を新ルールに効かせない)
  cut.ackCutoffItem({ messageId: mCancel.messageId, kind: 'cancel', status: 'not_applicable' }, 'tester');
  check('対象外にすると消える', !cut.listCutoffItems().items.some(i => i.messageId === mCancel.messageId));
  db.prepare('UPDATE cutoff_acks SET detector_version = 0 WHERE message_id = ? AND kind = ?')
    .run(mCancel.messageId, 'cancel');
  const revived = cut.listCutoffItems().items.find(i => i.messageId === mCancel.messageId);
  check('⭐ルールが変わると「対象外」は復活する', !!revived && revived.ackStale === true);

  // 「対応した」は人が実際に処理した事実なので、ルールが変わっても消えたまま
  cut.ackCutoffItem({ messageId: mCancel.messageId, kind: 'cancel', status: 'done' }, 'tester');
  db.prepare('UPDATE cutoff_acks SET detector_version = 0 WHERE message_id = ? AND kind = ?')
    .run(mCancel.messageId, 'cancel');
  check('「対応した」はルールが変わっても復活しない',
    !cut.listCutoffItems().items.some(i => i.messageId === mCancel.messageId));
  cut.unackCutoffItem({ messageId: mCancel.messageId, kind: 'cancel' });

  // ⭐スレッドの2通目以降は件名で再検知しない (お礼の返信のたびに湧かない)
  const thread = db.prepare(`INSERT INTO inquiries (channel_type, shop_id, external_inquiry_id, subject,
      internal_status, received_at, last_message_at)
    VALUES ('email', (SELECT id FROM shops LIMIT 1), 'thr1', '注文キャンセルのお願い', 'open',
      strftime('%Y-%m-%dT%H:%M:%SZ','now'), strftime('%Y-%m-%dT%H:%M:%SZ','now'))`).run().lastInsertRowid;
  db.prepare(`INSERT INTO inquiry_messages (inquiry_id, external_message_id, sender_type,
      is_incoming, message_body_text, received_at)
    VALUES (?, 'thr1-m1', 'customer', 1, 'よろしくお願いします', strftime('%Y-%m-%dT%H:%M:%SZ','now'))`).run(thread);
  const thanks = db.prepare(`INSERT INTO inquiry_messages (inquiry_id, external_message_id, sender_type,
      is_incoming, message_body_text, received_at)
    VALUES (?, 'thr1-m2', 'customer', 1, 'ありがとうございました', strftime('%Y-%m-%dT%H:%M:%SZ','now'))`).run(thread).lastInsertRowid;
  const threadItems = cut.listCutoffItems().items.filter(i => i.inquiryId === thread);
  check('件名は最初の受信メッセージにだけ効く', threadItems.length === 1, JSON.stringify(threadItems.map(i => i.messageId)));
  check('お礼の返信では新しい未対応が湧かない', !threadItems.some(i => i.messageId === thanks));

  check('不明な種別はthrow', throws(() => cut.ackCutoffItem({ messageId: mCancel.messageId, kind: 'nope' })));
  check('不正なstatusはthrow', throws(() => cut.ackCutoffItem({ messageId: mCancel.messageId, kind: 'cancel', status: 'x' })));
  check('存在しないメッセージはthrow', throws(() => cut.ackCutoffItem({ messageId: 999999, kind: 'cancel' })));
  check('messageIdが数字でなければthrow', throws(() => cut.ackCutoffItem({ messageId: 'abc', kind: 'cancel' })));

  // ⭐種別ごとに独立して片付く (住所は済んだが日時はまだ、が表現できる)
  const both = db.prepare(`INSERT INTO inquiries (channel_type, shop_id, external_inquiry_id, subject,
      internal_status, received_at, last_message_at)
    VALUES ('email', (SELECT id FROM shops LIMIT 1), 'both1', '件名', 'open',
      strftime('%Y-%m-%dT%H:%M:%SZ','now'), strftime('%Y-%m-%dT%H:%M:%SZ','now'))`).run().lastInsertRowid;
  const bothMsg = db.prepare(`INSERT INTO inquiry_messages (inquiry_id, external_message_id, sender_type,
      is_incoming, message_body_text, received_at)
    VALUES (?, 'msg-both', 'customer', 1, 'お届け先を変更して、日時指定も追加してください', strftime('%Y-%m-%dT%H:%M:%SZ','now'))`)
    .run(both).lastInsertRowid;
  check('1通で2種別が立つ', cut.listCutoffItems().items.filter(i => i.messageId === bothMsg).length === 2);
  cut.ackCutoffItem({ messageId: bothMsg, kind: 'address', status: 'done' }, 'tester');
  const rest = cut.listCutoffItems().items.filter(i => i.messageId === bothMsg);
  check('片方だけ片付けても、もう片方は残る', rest.length === 1 && rest[0].kind === 'datetime');
}

// ─── 6. 通知文 (0件でも送る) ───
console.log('6. 通知文');
{
  const text = cut.buildCutoffNotice({ baseUrl: 'https://example.com' });
  check('締め時刻が入る', /09:00|12:30|14:30/.test(text));
  check('件数が入る', /\d+件/.test(text), text.slice(0, 80));
  check('ネクストエンジンで直す旨が入る', text.includes('ネクストエンジン'));
  check('画面へのリンクが入る', text.includes('https://example.com/apps/inquiry-hub/cutoff'));
  check('種別の見出しが出る', text.includes('キャンセル') || text.includes('住所'));
  check('注文番号なしが分かる', text.includes('注文番号なし'));

  // ⭐0件でも通知する (来なければ「止まった」と気づけるように)
  for (const it of cut.listCutoffItems().items) cut.ackCutoffItem({ messageId: it.messageId, kind: it.kind, status: 'done' }, 'tester');
  const zero = cut.buildCutoffNotice({ baseUrl: 'https://example.com' });
  check('0件でも本文を作る', zero.includes('0件'));
  check('⭐0件を「安全の保証」と書かない (取りこぼしはありうる)',
    zero.includes('検知0件') && zero.includes('いつもどおり確認して') && !zero.includes('大丈夫'));
  check('0件でもリンクは載せる', zero.includes('/apps/inquiry-hub/cutoff'));
}

// ─── 7. 通知cron (Dark Launch / webhook未設定でも落ちない) ───
console.log('7. 通知cron');
{
  delete process.env.INQUIRY_HUB_CUTOFF_CRON_ENABLED;
  check('flag未設定ではスケジュールしない (Dark Launch)', cronModule.startInquiryHubCutoffCron() === null);

  process.env.INQUIRY_HUB_CUTOFF_CRON_ENABLED = 'true';
  process.env.INQUIRY_HUB_CUTOFF_CRONS = 'not-a-cron';
  check('cron式が不正ならスケジュールしない', cronModule.startInquiryHubCutoffCron() === null);
  delete process.env.INQUIRY_HUB_CUTOFF_CRONS;

  const tasks = cronModule.startInquiryHubCutoffCron();
  check('既定は3回スケジュールする (締めの回数と同じ)', Array.isArray(tasks) && tasks.length === 3, String(tasks && tasks.length));
  for (const t of tasks || []) t.stop?.();
  delete process.env.INQUIRY_HUB_CUTOFF_CRON_ENABLED;

  // ⭐cron式は締め時刻から導出する (時刻とcronを別々に書くと必ずずれる)
  check('既定の cron 式は締めの15分前 (JST08:45/12:15/14:15 = UTC23:45/03:15/05:15)',
    cronModule.DEFAULT_CUTOFF_CRONS.join() === '45 23 * * *,15 3 * * *,15 5 * * *',
    cronModule.DEFAULT_CUTOFF_CRONS.join());
  check('cron式は CUTOFF_TIMES から導出される (別々に持たない)',
    cut.cutoffNoticeCrons().join() === cronModule.DEFAULT_CUTOFF_CRONS.join());
  check('締めの回数とcronの本数が一致する', cut.cutoffNoticeCrons().length === cut.CUTOFF_TIMES.length);
  check('リード時間を変えると式も動く', cut.cutoffNoticeCrons(30)[0] === '30 23 * * *', cut.cutoffNoticeCrons(30)[0]);

  // webhook 未設定でも例外にしない (fail-soft)
  const saved = process.env.GCHAT_WEBHOOK;
  delete process.env.GCHAT_WEBHOOK;
  const r = await cronModule.runCutoffNoticeTick();
  check('GCHAT_WEBHOOK 未設定でも落ちず、本文は返す', r.sent === false && typeof r.text === 'string');

  // ⭐送れていないのに ping を出さない (「動いているが誰にも届かない」を緑にしない)
  check('送信できなかったときは sent=false を返す (ping条件になる)', r.sent === false);
  const r2 = await cronModule.runCutoffNoticeTick({ webhook: 'https://127.0.0.1:1/nowhere' });
  check('webhookが死んでいても例外にせず sent=false', r2.sent === false && !!r2.error);
  if (saved !== undefined) process.env.GCHAT_WEBHOOK = saved;
}

// ─── 7b. バッジのキャッシュ ───
console.log('7b. バッジのキャッシュ');
{
  cut.clearCutoffCountCache();
  const before = cut.countCutoffItems().total;
  // キャッシュが効くので、DBを直接書き換えても30秒間は数字が変わらない
  const inq = db.prepare(`INSERT INTO inquiries (channel_type, shop_id, external_inquiry_id, subject,
      internal_status, received_at, last_message_at)
    VALUES ('email', (SELECT id FROM shops LIMIT 1), 'cache1', '件名', 'open',
      strftime('%Y-%m-%dT%H:%M:%SZ','now'), strftime('%Y-%m-%dT%H:%M:%SZ','now'))`).run().lastInsertRowid;
  db.prepare(`INSERT INTO inquiry_messages (inquiry_id, external_message_id, sender_type,
      is_incoming, message_body_text, received_at)
    VALUES (?, 'msg-cache1', 'customer', 1, 'キャンセルしたいです', strftime('%Y-%m-%dT%H:%M:%SZ','now'))`).run(inq);
  check('30秒以内はキャッシュを返す (全ページの重い集計を避ける)', cut.countCutoffItems().total === before);
  cut.clearCutoffCountCache();
  check('捨てれば最新になる', cut.countCutoffItems().total === before + 1);
  // 押した直後はバッジが即座に減る (キャッシュを捨てている)
  const msgId = db.prepare("SELECT id FROM inquiry_messages WHERE external_message_id = 'msg-cache1'").get().id;
  cut.ackCutoffItem({ messageId: msgId, kind: 'cancel', status: 'done' }, 'tester');
  check('対応済みにした直後にバッジが減る', cut.countCutoffItems().total === before);
}

// ─── 8. 画面とAPI ───
console.log('8. 画面とAPI');
{
  cut.unackCutoffItem({ messageId: mAddr.messageId, kind: 'address' });   // 表示用に1件戻す
  const app = express();
  app.use('/apps/inquiry-hub', express.json({ limit: '2mb' }), routerModule.default);
  const server = await new Promise(r => { const s = app.listen(0, '127.0.0.1', () => r(s)); });
  const base = `http://127.0.0.1:${server.address().port}/apps/inquiry-hub`;
  const jpost = (p, data) => fetch(base + p, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data || {}) });

  const html = await (await fetch(base + '/cutoff')).text();
  check('画面が出る', html.includes('締め前確認'));
  check('次の締めが出る', html.includes('次の締めは'));
  check('ネクストエンジンで直す旨の案内が出る', html.includes('ネクストエンジンで先に直して'));
  check('該当の問い合わせが出る', html.includes('住所の件'));
  check('引っかかった言葉が出る', html.includes('引っかかった言葉'));
  check('対応済みボタンが出る', html.includes('ネクストエンジンで直した'));
  check('対象外ボタンが出る', html.includes('対象外だった'));
  check('サイドバーに導線が出る', html.includes('締め前確認'));
  check('締め時刻3回が出る', html.includes('09:00') && html.includes('12:30') && html.includes('14:30'));

  const r1 = await jpost('/api/cutoff/ack', { messageId: mAddr.messageId, kind: 'address', status: 'done' });
  check('対応済みAPI', r1.status === 200);
  check('不正な種別は400', (await jpost('/api/cutoff/ack', { messageId: mAddr.messageId, kind: 'nope' })).status === 400);
  check('不正なstatusは400', (await jpost('/api/cutoff/ack', { messageId: mAddr.messageId, kind: 'address', status: 'x' })).status === 400);
  check('存在しないメッセージは400', (await jpost('/api/cutoff/ack', { messageId: 999999, kind: 'address' })).status === 400);
  check('未対応に戻すAPI', (await jpost('/api/cutoff/unack', { messageId: mAddr.messageId, kind: 'address' })).status === 200);
  check('戻すAPIの再送も200 (冪等)', (await jpost('/api/cutoff/unack', { messageId: mAddr.messageId, kind: 'address' })).status === 200);

  const acked = await (await fetch(base + '/cutoff?acked=1')).text();
  check('対応済みも表示できる', acked.includes('対応済みも表示'));

  // 0件のときの画面
  for (const it of cut.listCutoffItems().items) cut.ackCutoffItem({ messageId: it.messageId, kind: it.kind, status: 'done' }, 'tester');
  const empty = await (await fetch(base + '/cutoff')).text();
  check('0件のときの画面も「検知0件」と書く (安全の保証にしない)',
    empty.includes('検知された未対応は0件') && empty.includes('取りこぼしはありえます'));

  // inline script の健全性 (2026-08-27 の事故防止)
  const open = (empty.match(/<script/g) || []).length, close = (empty.match(/<\/script>/g) || []).length;
  check(`scriptの開始と終了が一致 (${open}/${close})`, open === close);
  const so = (empty.match(/<style/g) || []).length, sc = (empty.match(/<\/style>/g) || []).length;
  check(`styleの開始と終了が一致 (${so}/${sc})`, so === sc);

  server.close();
}

// ─── 9. 定期実行の台帳登録 (グローバルルール) ───
console.log('9. 台帳登録');
{
  const reg = await import('../../config/jobs-registry.mjs');
  const list = reg.JOBS_REGISTRY || reg.default || Object.values(reg)[0];
  const arr = Array.isArray(list) ? list : Object.values(list).flat();
  const job = arr.find(j => j && j.id === 'inquiry-hub-cutoff');
  check('jobs-registry に登録されている', !!job);
  check('必須項目が埋まっている (owner/purpose/where/runbook)',
    job && job.owner && job.purpose && job.where && job.runbook);
  check('締め前 (08:45) をアンカーにしている', job && job.anchor_hour_jst === 8 && job.anchor_minute_jst === 45);
}

check('DBは一時サブディレクトリのみに作成',
  fs.existsSync(path.join(workDir, 'inquiry-hub.db')) && fs.existsSync(path.join(baseDir, 'inquiry-hub.db')) === baseDbExistedAtStart);

console.log(`\n${passed} PASS / ${failed} FAIL`);
db.close();
fs.rmSync(workDir, { recursive: true, force: true });
process.exitCode = failed ? 1 : 0;
