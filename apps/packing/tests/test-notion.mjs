/**
 * test-notion.mjs — ⑤ Notionカード自動移動 (梱包) のロジック検証
 *
 * packBatchNotionState (バッチ行→送信状態の純関数) と、共通部品 buildCardProperties の
 * 梱包スキーマでの写像を検証する。実API呼び出しは対象外 (トークン未設定=無効経路のみ確認)。
 *
 * 実行: node apps/packing/tests/test-notion.mjs
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'packing-notion-test-'));
process.env.DATA_DIR = tmpDir;
delete process.env.PACKING_NOTION_TOKEN;
delete process.env.PICKING_NOTION_TOKEN;

const { packBatchNotionState, enqueuePackBatchNotionSync, STATUS_PACKING, STATUS_PACK_DONE } =
  await import('../notion.js');
const { buildCardProperties } = await import('../../picking/notion.js');

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };
const eq = (a, b, label) => ok(JSON.stringify(a) === JSON.stringify(b), `${label} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

const base = {
  folder_name: '出荷_18', work_date: '2026-08-18', validity: 'valid', worker: '中原',
  slip_count: 20, started_at: '2026-08-18T01:00:00Z', finished_at: null, paused_total_sec: 0,
};

console.log('── packBatchNotionState ──');
{
  eq(packBatchNotionState(null), null, 'バッチ無しは null');
  eq(packBatchNotionState({ ...base, status: 'ready' }), null, 'ready はカードを触らない');
  eq(packBatchNotionState({ ...base, status: 'cancelled' }), null, 'cancelled はカードを触らない (中原さん決定)');
  eq(packBatchNotionState({ ...base, status: 'packing', validity: 'invalid' }), null, 'invalid はカードを触らない');

  const packing = packBatchNotionState({ ...base, status: 'packing' });
  eq(packing.label, STATUS_PACKING, '梱包中 → 梱包作業中');
  eq(packing.workerName, '中原', '担当者を渡す');
  eq(packing.folderName, '出荷_18', 'フォルダ名');
  eq(packing.workDate, '2026-08-18', '作業日');
  eq(packing.times.finishedAt, null, '作業中は終了なし (Notion側の終了/時間はクリアされる)');
  eq(packing.times.activeSec, null, '作業中は実働秒なし');

  eq(packBatchNotionState({ ...base, status: 'paused' }).label, STATUS_PACKING, '中断中もラベルは梱包作業中');

  const done = packBatchNotionState({
    ...base, status: 'done', finished_at: '2026-08-18T01:30:00Z', paused_total_sec: 120,
  });
  eq(done.label, STATUS_PACK_DONE, '完了 → 完了');
  eq(done.times.activeSec, 1680, '実働秒=経過1800秒-中断120秒');
  eq(done.times.lineCount, 20, '伝票数 (秒/伝票の分母)');

  // 完了取消でバッチが packing に戻れば、次の送信は自動的に梱包作業中+時間クリア
  const undone = packBatchNotionState({ ...base, status: 'packing', finished_at: null });
  eq(undone.label, STATUS_PACKING, '完了取消後は梱包作業中に戻る');
}

console.log('\n── buildCardProperties (梱包スキーマ) ──');
{
  const schema = {
    titleProp: '名前',
    statusProp: { name: 'ステータス', type: 'select' },
    workerProp: { name: '梱包担当者', type: 'select' },
    startProp: '梱包開始', endProp: '梱包終了', minutesProp: '梱包時間(分)', secPerLineProp: '秒/伝票',
  };
  const p = buildCardProperties(schema, STATUS_PACK_DONE, '中原', {
    startedAt: '2026-08-18T01:00:00Z', finishedAt: '2026-08-18T01:30:00Z', activeSec: 1680, lineCount: 20,
  });
  eq(p['ステータス'], { select: { name: '完了' } }, 'select型ステータス');
  eq(p['梱包担当者'], { select: { name: '中原' } }, '梱包担当者');
  eq(p['梱包開始'], { date: { start: '2026-08-18T10:00:00+09:00' } }, '開始はJST表記');
  eq(p['梱包終了'], { date: { start: '2026-08-18T10:30:00+09:00' } }, '終了はJST表記');
  eq(p['梱包時間(分)'], { number: 28 }, '分は小数1桁 (1680秒=28.0分)');
  eq(p['秒/伝票'], { number: 84 }, '秒/伝票=1680/20');

  const working = buildCardProperties(schema, STATUS_PACKING, 'x@b-faith.biz', {
    startedAt: '2026-08-18T01:00:00Z', finishedAt: null, activeSec: null, lineCount: 20,
  });
  ok(!working['梱包担当者'], 'メールアドレスは担当者selectに入れない');
  eq(working['梱包終了'], { date: null }, '作業中は終了をクリア');
  eq(working['梱包時間(分)'], { number: null }, '作業中は時間をクリア');

  // Notion側にプロパティが無い (未追加) 場合はステータス+担当者のみ
  const bare = buildCardProperties(
    { titleProp: '名前', statusProp: { name: 'ステータス', type: 'select' }, workerProp: null },
    STATUS_PACKING, '中原', { startedAt: '2026-08-18T01:00:00Z', finishedAt: null, activeSec: null, lineCount: 1 });
  eq(Object.keys(bare), ['ステータス'], '時間プロパティ未追加ならステータスだけ書く');
}

console.log('\n── トークン未設定 (連携オフ) ──');
{
  // enqueue はトークン未設定なら何もしない (例外を出さない)
  enqueuePackBatchNotionSync(99999);
  ok(true, 'トークン未設定の enqueue は無害');
}

try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* 後始末失敗は無視 */ }
console.log(`\n${failed === 0 ? '✅ 全テスト PASS' : `❌ ${failed} 件失敗`}`);
process.exitCode = failed === 0 ? 0 : 1;
