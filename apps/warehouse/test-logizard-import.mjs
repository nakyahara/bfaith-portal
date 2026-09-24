import { temporaryTestRoot } from '../../scripts/test-temp-dir.mjs';
await temporaryTestRoot(import.meta.url);
/**
 * test-logizard-import.mjs — csv-import.js の importLogizard 相当フローの検証
 *
 * 「壊れたCSVで既存在庫を全消ししない」ことが主題 (Codex R1 High-1):
 *   - 必須ヘッダー欠落 → 取込前に拒否
 *   - 商品IDが全行空 (実挿入 < 下限) → トランザクション全体をロールバック
 *   - 在庫数が数値でない → ロールバック
 * importLogizard は export されていないため、CLI (node csv-import.js logizard) を子プロセスで叩く。
 *
 * 実行: node apps/warehouse/test-logizard-import.mjs
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(__dirname, '..', '..');
const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'lzimport-test-'));
const dataDir = path.join(tmpDir, 'data');

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };

const HEADER = '在庫日,倉庫ID,倉庫名,ブロックID,ブロック略称,ロケ,商品ID,バーコード,商品名,有効期限,入荷日,品質区分ID,品質区分名,在庫数(引当数を含む),引当数,ロケ引当条件,ロケ業務区分,取置取引先ID,取置取引先名,棚卸状況,検索名称,検索名称２,大分類,中分類,小分類,商品予備項目００１,商品予備項目００２,商品予備項目００３,商品予備項目００４,商品予備項目００５,商品予備項目００６,商品予備項目００７,商品予備項目００８,商品予備項目００９,商品予備項目０１０,最終入荷日,最終出荷日,ブロック引当順';
const dataRow = (o = {}) => {
  const d = {
    在庫日: '20260815', ブロック略称: 'R1FA', ロケ: '001-001-01', 商品ID: 'hakkaspray100',
    バーコード: 'X0014Q5RST', 商品名: 'ハッカ油スプレー', 有効期限: '20280115', 品質区分名: '良品',
    在庫数: '200', 引当数: '0', ...o,
  };
  return `"${d.在庫日}","1","B-Faith","18","${d.ブロック略称}","${d.ロケ}","${d.商品ID}","${d.バーコード}","${d.商品名}","${d.有効期限}","","1","${d.品質区分名}","${d.在庫数}","${d.引当数}","指定","卸","","","","${d.商品名}","","","","","","","","","","","","","","","20260807","20260814","2"`;
};
const writeCsv = (name, lines) => {
  const p = path.join(tmpDir, name);
  fs.writeFileSync(p, lines.join('\r\n'), 'utf8');
  return p;
};
const runImport = (csvPath, extraEnv = {}) => {
  try {
    const out = execFileSync(process.execPath, [path.join(repoRoot, 'apps', 'warehouse', 'csv-import.js'), 'logizard', csvPath], {
      cwd: repoRoot,
      env: { ...process.env, DATA_DIR: dataDir, LZ_IMPORT_MIN_ROWS: '2', ...extraEnv },
      encoding: 'utf8',
    });
    return { code: 0, out };
  } catch (e) {
    return { code: e.status ?? 1, out: `${e.stdout || ''}${e.stderr || ''}` };
  }
};
const countRows = async () => {
  process.env.DATA_DIR = dataDir;
  const { initDB, getDB } = await import('./db.js');
  await initDB();
  const db = getDB();
  const n = db.prepare('SELECT COUNT(*) n FROM raw_lz_inventory').get().n;
  db.close();   // 開いたままだと後始末の rmSync が EPERM になる
  return n;
};

console.log('\n── 正常系 ──');
{
  const csv = writeCsv('ok.csv', [HEADER, dataRow(), dataRow({ ロケ: '002-002-01', 在庫数: '160' })]);
  const r = runImport(csv);
  ok(r.code === 0, `正常CSVは exit 0 (実際 ${r.code})`);
}

console.log('\n── 在庫を取った時刻と行数を残す (Render の写しへ送る。2026-09-25) ──');
{
  // 商品 ID が空の行を 1 行まぜる (読み飛ばされる) + CSV の時刻を決めておく
  const csv = writeCsv('meta.csv', [HEADER, dataRow(), dataRow({ ロケ: '003-003-01' }), dataRow({ 商品ID: '' })]);
  const mtime = new Date(Math.floor((Date.now() - 3600e3) / 1000) * 1000);   // 1 時間前 (秒単位にそろえる)
  const expectSource = new Date(mtime.getTime() - 10 * 60e3).toISOString();
  fs.utimesSync(csv, mtime, mtime);
  const r = runImport(csv);
  ok(r.code === 0, `素性つきの取り込み exit 0 (実際 ${r.code})`);
  process.env.DATA_DIR = dataDir;
  const { initDB, getDB } = await import('./db.js');
  await initDB();
  const db = getDB();
  const get = (k) => db.prepare('SELECT value FROM sync_meta WHERE key = ?').get(k)?.value ?? null;
  const importedAt = get('logizard_last_import');
  ok(get('logizard_source_at') === expectSource, `在庫を取った時刻 = CSV の時刻 − 10 分 (期待 ${expectSource} / 実際 ${get('logizard_source_at')})`);
  ok(get('logizard_rows_read') === '3' && get('logizard_skipped_rows') === '1', `CSV 3 行・読み飛ばし 1 行 (実際 ${get('logizard_rows_read')}/${get('logizard_skipped_rows')})`);
  ok(get('logizard_source_for') === importedAt, 'どの取り込みの値か = 今回の取り込み時刻');
  const { logizardSourceMeta } = await import('./sync-to-render.js');
  const m = logizardSourceMeta(db, importedAt);
  ok(JSON.stringify(m) === JSON.stringify({ source_at: expectSource, rows_read: 3, skipped_rows: 1 }), `送り手が今回の値を付ける (実際 ${JSON.stringify(m)})`);
  ok(JSON.stringify(logizardSourceMeta(db, '2020-01-01T00:00:00.000Z')) === '{}', '🚨 別の取り込みの値は送らない (素性を残さない経路で取り込んだとき)');
  db.close();
  // 後続の試験 (正常系 2 行が残っていること) のため、元の 2 行に戻す
  const back = runImport(writeCsv('ok-again.csv', [HEADER, dataRow(), dataRow({ ロケ: '002-002-01', 在庫数: '160' })]));
  ok(back.code === 0, '元の 2 行に戻す');
}

console.log('\n── 壊れたCSVで既存データを消さない ──');
{
  // 商品ID列の名前が変わった (列自体が見つからない) → ヘッダー検証で拒否
  const badHeader = HEADER.replace('商品ID', '商品コード');
  const r1 = runImport(writeCsv('bad-header.csv', [badHeader, dataRow(), dataRow()]));
  ok(r1.code !== 0 && r1.out.includes('必須列'), `必須ヘッダー欠落 → 拒否 (exit ${r1.code})`);

  // 商品IDが全行空 → 実挿入0件 < 下限 → ロールバック
  const r2 = runImport(writeCsv('empty-id.csv', [HEADER, dataRow({ 商品ID: '' }), dataRow({ 商品ID: '' })]));
  ok(r2.code !== 0 && r2.out.includes('実挿入件数'), `商品ID全行空 → ロールバック (exit ${r2.code})`);

  // 在庫数が数値でない → ロールバック
  const r3 = runImport(writeCsv('bad-qty.csv', [HEADER, dataRow({ 在庫数: 'abc' }), dataRow()]));
  ok(r3.code !== 0 && r3.out.includes('整数ではありません'), `在庫数が非数値 → ロールバック (exit ${r3.code})`);

  // 下限そのものが不正 → 拒否 (NaN でガード無効化しない)
  const r4 = runImport(writeCsv('ok2.csv', [HEADER, dataRow(), dataRow()]), { LZ_IMPORT_MIN_ROWS: 'abc' });
  ok(r4.code !== 0 && r4.out.includes('LZ_IMPORT_MIN_ROWS'), `LZ_IMPORT_MIN_ROWS 不正 → 拒否 (exit ${r4.code})`);
}

{
  const n = await countRows();
  ok(n === 2, `拒否4連発の後も正常系の2行が残っている (実際 ${n}行)`);
}

try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* 後始末失敗はテスト結果に影響させない */ }
console.log(`\n${failed === 0 ? '✅ 全テスト PASS' : `❌ ${failed} 件失敗`}`);
process.exitCode = failed === 0 ? 0 : 1;
