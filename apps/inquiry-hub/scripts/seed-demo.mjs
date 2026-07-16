// inquiry-hub Step 1 動作確認用のデモデータ投入 (設計書§11 Step 1 完了条件の「手動投入データ」)
// 使い方: DATA_DIR=<データディレクトリ> node apps/inquiry-hub/scripts/seed-demo.mjs
//
// 安全装置:
//   - DATA_DIR 未指定なら中断
//   - 既に inquiries に1件でもデータがあれば中断 (実データ・投入済みデモを上書きしない)
// デモデータの external_inquiry_id は 'demo:' プレフィックス。Step 2 の実同期開始前に
//   DELETE FROM inquiries WHERE external_inquiry_id LIKE 'demo:%' 等で除去する想定 (削除スクリプトは同期実装時に用意)。
import { initInquiryHubDB, getDB, logActivity, toUtcIso } from '../db.js';

if (!process.env.DATA_DIR) {
  console.error('FATAL: DATA_DIR が未指定です。例: DATA_DIR=/var/data node apps/inquiry-hub/scripts/seed-demo.mjs');
  process.exit(2);
}

initInquiryHubDB();
const db = getDB();

const existing = db.prepare('SELECT COUNT(*) AS c FROM inquiries').get().c;
if (existing > 0) {
  console.error(`FATAL: inquiries に既に ${existing} 件あります。デモデータは空のDBにのみ投入します`);
  process.exit(2);
}

// JST時刻指定 → 正準形式 (UTC 'YYYY-MM-DDTHH:MM:SSZ'、db.js toUtcIso) で保存
const iso = (daysAgo, hm = '10:00') => {
  const d = new Date(Date.now() - daysAgo * 86400000);
  return toUtcIso(`${d.toISOString().slice(0, 10)}T${hm}:00+09:00`);
};

const tx = db.transaction(() => {
  const insShop = db.prepare(`INSERT INTO shops (channel_type, shop_name, account_identifier, last_synced_at)
    VALUES (?, ?, ?, ?)`);
  const emailShop = insShop.run('email', '問い合わせメール', 'support@example.jp', iso(0, '07:00')).lastInsertRowid;
  const rakutenShop = insShop.run('rakuten', '楽天 B-Faith', 'demo-rakuten-shop', iso(0, '07:05')).lastInsertRowid;
  const yahooShop = insShop.run('yahoo', 'Yahoo! B-Faith', 'demo-yahoo-seller', iso(0, '07:10')).lastInsertRowid;

  const insInq = db.prepare(`INSERT INTO inquiries (
    channel_type, shop_id, external_inquiry_id, customer_name, customer_identifier, subject,
    internal_status, external_status, external_is_read, last_external_synced_at,
    assigned_user_id, order_number, product_code, product_name,
    is_unread, needs_attention, ai_needed, conversation_rev, received_at, last_message_at
  ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
  const insMsg = db.prepare(`INSERT INTO inquiry_messages (
    inquiry_id, external_message_id, sender_type, sender_name, message_body_text,
    is_incoming, sent_by_user_id, sent_at, received_at
  ) VALUES (?,?,?,?,?,?,?,?,?)`);
  const insAtt = db.prepare(`INSERT INTO inquiry_attachments (
    inquiry_message_id, external_attachment_id, file_name, content_type, file_size, fetch_status
  ) VALUES (?,?,?,?,?,?)`);
  const insNote = db.prepare('INSERT INTO internal_notes (inquiry_id, user_id, body) VALUES (?,?,?)');

  // 1. 楽天: 商品不良 (未対応・要確認・添付あり)
  {
    const id = insInq.run('rakuten', rakutenShop, 'demo:rk-10001', '山田 太郎', 'user-mask-001', '届いた商品が破損していました',
      'open', 'incomplete', 0, iso(0, '07:05'),
      null, '123456-20260714-0001', 'bf-oil-30ml', 'アロマオイル 30ml',
      1, 1, 2, 2, iso(1, '18:22'), iso(0, '06:40')).lastInsertRowid;
    insMsg.run(id, 'demo:rk-10001-m1', 'customer', '山田 太郎',
      '先日届いた商品ですが、箱を開けたところボトルにひびが入っており中身が漏れていました。写真を添付します。交換をお願いできますか。',
      1, null, null, iso(1, '18:22'));
    const m2 = insMsg.run(id, 'demo:rk-10001-m2', 'customer', '山田 太郎',
      '追伸: 注文番号は 123456-20260714-0001 です。よろしくお願いします。',
      1, null, null, iso(0, '06:40')).lastInsertRowid;
    insAtt.run(m2, 'demo:att-1', 'IMG_2041.jpg', 'image/jpeg', 1834520, 'pending');
    insNote.run(id, 'demo-seed', '破損クレーム。写真確認のうえ交換手配の方針 (デモデータ)');
    logActivity(id, { actorType: 'system', actionType: 'seed', after: { source: 'seed-demo' } });
  }

  // 2. メール: 配送状況の問い合わせ (対応中・返信済み)
  {
    const id = insInq.run('email', emailShop, 'demo:gm-thread-001', '佐藤 花子', 'hanako@example.com', '注文した商品はいつ届きますか',
      'in_progress', null, null, iso(0, '07:00'),
      'm.nakahara@b-faith.biz', '987654-20260713-0002', 'bf-soap-set', '石鹸ギフトセット',
      0, 0, 0, 2, iso(2, '09:15'), iso(1, '11:30')).lastInsertRowid;
    insMsg.run(id, 'demo:gm-msg-001', 'customer', '佐藤 花子',
      '7月13日に注文した石鹸ギフトセットですが、発送予定日を教えてください。プレゼント用なので7月20日までに届くと助かります。',
      1, null, null, iso(2, '09:15'));
    insMsg.run(id, 'demo:gm-msg-002', 'shop', 'B-Faith サポート',
      '佐藤様\nお問い合わせありがとうございます。ご注文の商品は本日発送予定で、7月18日頃のお届け見込みです。\n発送完了時に追跡番号をご案内いたします。',
      0, 'm.nakahara@b-faith.biz', iso(1, '11:30'), iso(1, '11:30'));
    logActivity(id, { actorType: 'system', actionType: 'seed', after: { source: 'seed-demo' } });
  }

  // 3. Yahoo!: 注文前の商品質問 (未対応・AI返信フラグ)
  {
    const id = insInq.run('yahoo', yahooShop, 'demo:yh-topic-501', 'ヤフー太郎', 'y-user-501', '成分について教えてください',
      'open', 'open', 0, iso(0, '07:10'),
      null, null, 'bf-cream-50g', 'ハンドクリーム 50g',
      1, 0, 1, 1, iso(0, '05:58'), iso(0, '05:58')).lastInsertRowid;
    insMsg.run(id, 'demo:yh-501-m1', 'customer', 'ヤフー太郎',
      'こちらのハンドクリームは敏感肌でも使えますか?アルコールは入っていますか?',
      1, null, null, iso(0, '05:58'));
    logActivity(id, { actorType: 'system', actionType: 'seed', after: { source: 'seed-demo' } });
  }

  // 4. メール: 返金依頼 (保留)
  {
    const id = insInq.run('email', emailShop, 'demo:gm-thread-002', '鈴木 一郎', 'ichiro@example.com', '返金をお願いします',
      'pending', null, null, iso(0, '07:00'),
      'm.nakahara@b-faith.biz', '555555-20260710-0009', null, null,
      0, 1, 3, 1, iso(4, '14:45'), iso(4, '14:45')).lastInsertRowid;
    insMsg.run(id, 'demo:gm-msg-003', 'customer', '鈴木 一郎',
      '注文をキャンセルしたので返金をお願いします。クレジットカードで支払いました。',
      1, null, null, iso(4, '14:45'));
    insNote.run(id, 'demo-seed', '返金可否は責任者確認待ち (デモデータ)');
    logActivity(id, { actorType: 'system', actionType: 'seed', after: { source: 'seed-demo' } });
  }

  // 5. 楽天: 領収書希望 (完了)
  {
    const id = insInq.run('rakuten', rakutenShop, 'demo:rk-10002', '田中 美咲', 'user-mask-002', '領収書を発行してほしい',
      'done', 'complete', 1, iso(0, '07:05'),
      'm.nakahara@b-faith.biz', '123456-20260708-0003', null, null,
      0, 0, 0, 2, iso(7, '10:00'), iso(6, '15:20')).lastInsertRowid;
    insMsg.run(id, 'demo:rk-10002-m1', 'customer', '田中 美咲',
      '会社用に領収書を発行していただけますか。宛名は「株式会社サンプル」でお願いします。',
      1, null, null, iso(7, '10:00'));
    insMsg.run(id, 'demo:rk-10002-m2', 'shop', 'B-Faith 楽天店',
      '田中様\n領収書を同封にて発送いたしました。ご確認ください。',
      0, 'm.nakahara@b-faith.biz', iso(6, '15:20'), iso(6, '15:20'));
    db.prepare("UPDATE inquiries SET completed_at = ? WHERE id = ?").run(iso(6, '15:21'), id);
    logActivity(id, { actorType: 'system', actionType: 'seed', after: { source: 'seed-demo' } });
  }
});
tx();

const c = db.prepare('SELECT COUNT(*) AS c FROM inquiries').get().c;
const m = db.prepare('SELECT COUNT(*) AS c FROM inquiry_messages').get().c;
console.log(`OK: shops 3件 / inquiries ${c}件 / messages ${m}件 を投入しました`);
