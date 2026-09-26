import { temporaryTestRoot } from './test-temp-dir.mjs';
await temporaryTestRoot(import.meta.url);
/**
 * 入荷受付チェック — Drive 自動取込のテスト (Drive へは繋がず、取得部分を差し替えて検証)
 *
 * 実行: node scripts/test-inbound-check-drive.mjs
 * 検証: Drive の modifiedTime が CSV 生成時刻になる / 同じファイルは取り込まない /
 *       古いファイルは拒否 / 0件CSV (ヘッダのみ) が空の一覧として通る / cron の有効判定
 */
import fs from 'fs';
import os from 'os';
import path from 'path';
import iconv from 'iconv-lite';

if (!process.env.DATA_DIR) process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'ic-drive-'));
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const { importCsv, getActiveBatch, getState } = await import('../apps/inbound-check/db.js');

let pass = 0, fail = 0;
const ok = (c, l) => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}`); } };

const HEADER = ['入荷管理番号', '入荷管理行番号', '入荷管理詳細行番号', 'ステータス', '入荷予定日', '入荷受付日',
  '商品ID', '商品名', '予定数', '受付数', '作成日時', '更新日時', 'バーコード'];
const q = v => `"${String(v ?? '').replace(/"/g, '""')}"`;
function csv(rows) {
  const lines = [HEADER.map(q).join(',')];
  for (const r of rows) lines.push(HEADER.map(h => q(r[h])).join(','));
  return iconv.encode(lines.join('\r\n') + '\r\n', 'cp932');
}
const row = (ar, no, pid, qty, upd = '20260901120000') => ({
  入荷管理番号: ar, 入荷管理行番号: no, 入荷管理詳細行番号: 1, ステータス: '受付済',
  入荷予定日: '20260901', 入荷受付日: '20260901', 商品ID: pid, 商品名: `商品${pid}`,
  予定数: qty, 受付数: qty, 作成日時: upd, 更新日時: upd, バーコード: '4500000000000',
});

console.log('DATA_DIR =', process.env.DATA_DIR);

console.log('\n[1] Drive の更新日時を CSV 生成時刻として取り込む');
{
  const buf = csv([row('AR1', 1, 'a', 10), row('AR1', 2, 'b', 5)]);
  const r = importCsv(buf, { fileName: 'nyuka_uketsuke.csv', source: 'auto', actor: 'cron', generatedAt: '2026-09-01T09:00:00.000Z' });
  ok(r.ok && r.rowCount === 2 && r.slipCount === 1, `取込 (${r.rowCount}行/${r.slipCount}伝票)`);
  ok(getActiveBatch().csv_generated_at === '2026-09-01T09:00:00.000Z', 'Drive の modifiedTime が csv_generated_at になる');
  ok(getActiveBatch().source === 'auto', "source='auto' で記録");
}

console.log('\n[2] 巡回しても同じファイルなら取り込まない');
{
  const buf = csv([row('AR1', 1, 'a', 10), row('AR1', 2, 'b', 5)]);
  const before = getActiveBatch().id;
  const r = importCsv(buf, { fileName: 'nyuka_uketsuke.csv', source: 'auto', actor: 'cron', generatedAt: '2026-09-01T09:30:00.000Z' });
  ok(!r.ok && r.error === 'duplicate_file' && getActiveBatch().id === before, '同一内容は duplicate_file (バッチを増やさない)');
}

console.log('\n[3] 古いファイルは取り込まない (Drive の巻き戻り)');
{
  const before = getActiveBatch().id;
  const r = importCsv(csv([row('AR9', 1, 'z', 1, '20260830090000')]), { source: 'auto', generatedAt: '2026-08-31T00:00:00.000Z' });
  ok(!r.ok && r.error === 'older_file' && getActiveBatch().id === before, '明細も生成時刻も古ければ拒否');
}

console.log('\n[3b] 🚨 Drive の自動取込: 明細は古くても生成時刻 (Drive の更新日時) が新しければ取り込む (2026-09-26: 前日の新しい行が検品で消え、古い受付だけ残った CSV を断り続けた)');
{
  const cur = getActiveBatch();
  const r = importCsv(csv([row('AR1', 1, 'a', 10, '20260831080000')]), { source: 'auto', generatedAt: '2026-09-01T10:00:00.000Z' });
  ok(r.ok && getActiveBatch().id === r.batch.id && getActiveBatch().id !== cur.id && r.rowCount === 1, '明細 (8/31) が今の一覧 (9/1) より古くても、生成時刻が新しい自動取込は通る');
  const r2 = importCsv(csv([row('AR1', 1, 'a', 10, '20260831070000')]), { source: 'drive_retry', generatedAt: '2026-09-01T09:45:00.000Z' });
  ok(!r2.ok && r2.error === 'older_file' && /生成時刻/.test(r2.message), '生成時刻が今の一覧より古ければ自動取込でも拒否 (Drive の巻き戻り)');
  // 手のアップロードは今まで通り明細の時刻でも断る (File.lastModified を信用しない)
  const r3 = importCsv(csv([row('AR1', 1, 'a', 10, '20260830070000')]), { source: 'manual_upload', generatedAt: '2026-09-01T11:00:00.000Z' });
  ok(!r3.ok && r3.error === 'older_file' && /明細/.test(r3.message), '手のアップロードは明細が古ければ拒否 (今まで通り)');
  // 元の一覧 (2 行) に戻しておく (以降の試験の前提)
  const back = importCsv(csv([row('AR1', 1, 'a', 10), row('AR1', 2, 'b', 5), row('AR1', 3, 'x', 1, '20260901120001')]), { source: 'auto', generatedAt: '2026-09-01T10:30:00.000Z' });
  ok(back.ok, '次の試験のため一覧を作り直す');
}

console.log('\n[4] 0件 (ヘッダのみ) の CSV = 空の一覧');
{
  const empty = csv([]);
  const r = importCsv(empty, { fileName: 'nyuka_uketsuke.csv', source: 'auto', generatedAt: '2026-09-02T09:00:00.000Z' });
  ok(r.ok && r.rowCount === 0 && r.slipCount === 0, '0件でも取り込める');
  const st = getState();
  ok(st.lines.length === 0 && st.slips.length === 0 && st.batch.id === r.batch.id, '一覧が空になる (検品が済んだ日の朝)');
  ok(getActiveBatch().data_max_at === null, '明細が無いので data_max_at は null');
}

console.log('\n[5] 0件の翌日に受付が復活しても取り込める (data_max_at が null でも詰まらない)');
{
  const r = importCsv(csv([row('AR2', 1, 'c', 3, '20260903100000')]), { source: 'auto', generatedAt: '2026-09-03T09:00:00.000Z' });
  ok(r.ok && r.rowCount === 1, '翌日の受付を取り込める');
  ok(getState().lines.length === 1, '一覧に戻る');
}

console.log('\n[6] cron の有効判定');
{
  const mod = await import('../apps/inbound-check/sync-job.js');
  const prev = { enabled: process.env.INBOUND_CHECK_SYNC_ENABLED, render: process.env.RENDER };
  process.env.INBOUND_CHECK_SYNC_ENABLED = 'false';
  ok(mod.startInboundCheckCron() === null, 'ENABLED=false なら起動しない');
  delete process.env.INBOUND_CHECK_SYNC_ENABLED;
  delete process.env.RENDER;
  ok(mod.startInboundCheckCron() === null, '非Render では既定で起動しない (二重取込を作らない)');
  process.env.INBOUND_CHECK_SYNC_ENABLED = 'true';
  const t = mod.startInboundCheckCron();
  ok(t !== null, '明示的に true なら非Renderでも起動する');
  mod.stopInboundCheckCron();
  if (prev.enabled === undefined) delete process.env.INBOUND_CHECK_SYNC_ENABLED; else process.env.INBOUND_CHECK_SYNC_ENABLED = prev.enabled;
  if (prev.render !== undefined) process.env.RENDER = prev.render;
}

console.log('\n[7] lib/drive-csv の戻り値の形 (取り違えると本番で落ちる)');
{
  // 🚨2026-09-01 実障害: downloadDriveCsv の戻り値を Buffer だと思って importCsv に渡し、
  //   本番で The "data" argument must be of type string or an instance of Buffer... で失敗した。
  //   実際は { ...info, buffer } なので、その契約を固定して壊れたら気づけるようにする
  const src = fs.readFileSync(new URL('../lib/drive-csv.js', import.meta.url), 'utf8');
  ok(/return \{ \.\.\.info, buffer \}/.test(src), 'downloadDriveCsv は { ...info, buffer } を返す');
  const df = fs.readFileSync(new URL('../apps/inbound-check/drive-fetch.js', import.meta.url), 'utf8');
  ok(/const buffer = dl\.buffer;/.test(df), 'drive-fetch は dl.buffer を取り出している');
  ok(/dl\.modified_time/.test(df), '生成時刻は DL 直後の metadata (dl.modified_time) から採る');
  ok(!/const buffer = await downloadDriveCsv/.test(df), '戻り値をそのまま Buffer として扱っていない');
}
console.log(`\n${pass} PASS / ${fail} FAIL`);
process.exitCode = fail ? 1 : 0;
