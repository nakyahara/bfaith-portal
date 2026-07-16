// inquiry-hub デモデータ削除 (Step 2 実同期開始前の必須作業。seed-demo.mjs の対)
//
// 使い方:
//   確認 (dry-run。何も消さない):  DATA_DIR=/data node apps/inquiry-hub/scripts/purge-demo.mjs
//   実行:                          DATA_DIR=/data node apps/inquiry-hub/scripts/purge-demo.mjs --apply
//
// 削除対象:
//   - external_inquiry_id が 'demo:' で始まる問い合わせと、その配下の全レコード
//     (添付/メッセージ/outbox/AI下書き/AIジョブ/メモ/操作ログ/同期エラー)
//   - seed-demo.mjs が作ったデモ店舗 (下記 DEMO_SHOP_IDS)。ただし削除後も
//     その店舗に問い合わせが残る場合 (実データが紐付いた場合) は店舗を残して警告
//
// 触らないもの: reply_templates / qa_entries (本番CSV取込済みデータ)、demo: 以外の問い合わせ
import { initInquiryHubDB, getDB } from '../db.js';

// seed-demo.mjs が投入する店舗の account_identifier (これ以外の店舗は絶対に消さない)
const DEMO_SHOP_IDS = ['support@example.jp', 'demo-rakuten-shop', 'demo-yahoo-seller'];

export function purgeDemo({ apply = false } = {}) {
  const db = getDB();
  const report = { apply, deleted: {}, keptShops: [] };

  const inqIds = db.prepare("SELECT id FROM inquiries WHERE external_inquiry_id LIKE 'demo:%'").all().map(r => r.id);
  const inqIn = `(${inqIds.map(() => '?').join(',') || 'NULL'})`;
  const msgIds = inqIds.length
    ? db.prepare(`SELECT id FROM inquiry_messages WHERE inquiry_id IN ${inqIn}`).all(...inqIds).map(r => r.id)
    : [];
  const msgIn = `(${msgIds.map(() => '?').join(',') || 'NULL'})`;

  const demoShops = db.prepare(`SELECT id, shop_name, account_identifier FROM shops
    WHERE account_identifier IN (${DEMO_SHOP_IDS.map(() => '?').join(',')})`).all(...DEMO_SHOP_IDS);

  const count = (sql, params) => db.prepare(sql).get(...params)?.c ?? 0;
  const plan = [
    ['inquiry_attachments', `SELECT COUNT(*) c FROM inquiry_attachments WHERE inquiry_message_id IN ${msgIn}`, msgIds,
      `DELETE FROM inquiry_attachments WHERE inquiry_message_id IN ${msgIn}`],
    ['inquiry_messages', `SELECT COUNT(*) c FROM inquiry_messages WHERE id IN ${msgIn}`, msgIds,
      `DELETE FROM inquiry_messages WHERE id IN ${msgIn}`],
    ['outbox_replies', `SELECT COUNT(*) c FROM outbox_replies WHERE inquiry_id IN ${inqIn}`, inqIds,
      `DELETE FROM outbox_replies WHERE inquiry_id IN ${inqIn}`],
    ['ai_drafts', `SELECT COUNT(*) c FROM ai_drafts WHERE inquiry_id IN ${inqIn}`, inqIds,
      `DELETE FROM ai_drafts WHERE inquiry_id IN ${inqIn}`],
    ['ai_jobs', `SELECT COUNT(*) c FROM ai_jobs WHERE inquiry_id IN ${inqIn}`, inqIds,
      `DELETE FROM ai_jobs WHERE inquiry_id IN ${inqIn}`],
    ['internal_notes', `SELECT COUNT(*) c FROM internal_notes WHERE inquiry_id IN ${inqIn}`, inqIds,
      `DELETE FROM internal_notes WHERE inquiry_id IN ${inqIn}`],
    ['inquiry_activity_logs', `SELECT COUNT(*) c FROM inquiry_activity_logs WHERE inquiry_id IN ${inqIn}`, inqIds,
      `DELETE FROM inquiry_activity_logs WHERE inquiry_id IN ${inqIn}`],
    ['sync_errors (inquiry)', `SELECT COUNT(*) c FROM sync_errors WHERE inquiry_id IN ${inqIn}`, inqIds,
      `DELETE FROM sync_errors WHERE inquiry_id IN ${inqIn}`],
    ['inquiries', `SELECT COUNT(*) c FROM inquiries WHERE id IN ${inqIn}`, inqIds,
      `DELETE FROM inquiries WHERE id IN ${inqIn}`],
  ];

  const run = () => {
    for (const [label, countSql, params, delSql] of plan) {
      report.deleted[label] = count(countSql, params);
      if (apply) db.prepare(delSql).run(...params);
    }
    // デモ店舗: demo以外の問い合わせが残っていなければ店舗ごと削除
    report.deleted.shops = 0;
    report.deleted['sync_state (shop)'] = 0;
    report.deleted['sync_errors (shop)'] = 0;
    for (const s of demoShops) {
      // apply時は上でinquiriesが消えている。dry-run時は「demo以外が紐付いているか」で判定
      const remaining = db.prepare(
        "SELECT COUNT(*) c FROM inquiries WHERE shop_id = ? AND external_inquiry_id NOT LIKE 'demo:%'").get(s.id).c;
      if (remaining > 0) {
        report.keptShops.push(`${s.shop_name} (${s.account_identifier}): demo以外の問い合わせ ${remaining} 件が紐付くため店舗は残します`);
        continue;
      }
      report.deleted['sync_state (shop)'] += count('SELECT COUNT(*) c FROM sync_state WHERE shop_id = ?', [s.id]);
      report.deleted['sync_errors (shop)'] += count('SELECT COUNT(*) c FROM sync_errors WHERE shop_id = ?', [s.id]);
      report.deleted.shops += 1;
      if (apply) {
        db.prepare('DELETE FROM sync_state WHERE shop_id = ?').run(s.id);
        db.prepare('DELETE FROM sync_errors WHERE shop_id = ?').run(s.id);
        db.prepare('DELETE FROM shops WHERE id = ?').run(s.id);
      }
    }
  };
  if (apply) db.transaction(run)(); else run();

  report.remainingInquiries = db.prepare('SELECT COUNT(*) c FROM inquiries').get().c;
  report.remainingDemo = db.prepare("SELECT COUNT(*) c FROM inquiries WHERE external_inquiry_id LIKE 'demo:%'").get().c;
  return report;
}

// CLI実行 (import時は動かない)
import { pathToFileURL } from 'url';
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  if (!process.env.DATA_DIR) {
    console.error('FATAL: DATA_DIR が未指定です。例: DATA_DIR=/data node apps/inquiry-hub/scripts/purge-demo.mjs --apply');
    process.exit(2);
  }
  const apply = process.argv.includes('--apply');
  initInquiryHubDB();
  const r = purgeDemo({ apply });
  console.log(apply ? '=== 削除を実行しました ===' : '=== dry-run (削除は行っていません。実行は --apply) ===');
  for (const [k, v] of Object.entries(r.deleted)) console.log(`  ${k}: ${v} 件`);
  for (const w of r.keptShops) console.log(`  ⚠ ${w}`);
  console.log(`  残り問い合わせ: ${r.remainingInquiries} 件 (うちdemo: ${r.remainingDemo} 件)`);
  if (apply && r.remainingDemo > 0) { console.error('NG: demoデータが残っています'); process.exit(1); }
}
