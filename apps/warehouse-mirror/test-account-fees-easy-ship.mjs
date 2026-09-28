import { temporaryTestRoot } from '../../scripts/test-temp-dir.mjs';
await temporaryTestRoot(import.meta.url);
/**
 * test-account-fees-easy-ship.mjs — Render の mirror_amazon_account_fees_monthly が easy_ship (Easy Ship の配送料・2026-09-28) を受け付けるか
 *   CHECK は後から変えられない → 古い表 (easy_ship の無い CHECK) なら initMirrorDB が中身を写して作り直す
 * 実行: node apps/warehouse-mirror/test-account-fees-easy-ship.mjs (一時 DATA_DIR)
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import Database from 'better-sqlite3';
import { fileURLToPath } from 'node:url';

const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'mirror-easyship-test-'));
process.env.DATA_DIR = tmpDir;
let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };

// 古い表 (2026-09-28 より前の CHECK) + 中身
const old = new Database(path.join(tmpDir, 'warehouse-mirror.db'));
old.exec(`CREATE TABLE mirror_amazon_account_fees_monthly (
  date_jst TEXT NOT NULL CHECK(date_jst GLOB '????-??-01'),
  fee_type TEXT NOT NULL CHECK(fee_type IN ('storage','long_term_storage','removal','inbound_defect','low_inventory','subscription','other_account_fee')),
  amount_jpy REAL NOT NULL DEFAULT 0, row_count INTEGER NOT NULL DEFAULT 0, source_run_id TEXT NOT NULL, source_row_hash TEXT NOT NULL, synced_at TEXT NOT NULL,
  PRIMARY KEY (date_jst, fee_type))`);
old.prepare(`INSERT INTO mirror_amazon_account_fees_monthly VALUES ('2026-08-01', 'storage', -315926, 1, 'r', 'h', 't')`).run();
let threw = false; try { old.prepare(`INSERT INTO mirror_amazon_account_fees_monthly VALUES ('2026-08-01', 'easy_ship', -1, 1, 'r', 'h', 't')`).run(); } catch { threw = true; }
ok(threw, '前提: 古い表は easy_ship を受け付けない');
old.close();

const { initMirrorDB, getMirrorDB } = await import('./db.js');
initMirrorDB();
const db = getMirrorDB();
const sql = db.prepare(`SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'mirror_amazon_account_fees_monthly'`).get().sql;
ok(sql.includes("'easy_ship'"), '初期化で表を作り直した (CHECK に easy_ship)');
ok(db.prepare(`SELECT amount_jpy a FROM mirror_amazon_account_fees_monthly WHERE date_jst = '2026-08-01' AND fee_type = 'storage'`).get()?.a === -315926, '中身 (8 月の保管料) は写して残った');
db.prepare(`INSERT INTO mirror_amazon_account_fees_monthly VALUES ('2026-08-01', 'easy_ship', -2012501, 10793, 'r', 'h', 't')`).run();
ok(db.prepare(`SELECT COUNT(*) n FROM mirror_amazon_account_fees_monthly WHERE fee_type = 'easy_ship'`).get().n === 1, 'easy_ship を受け付ける');
ok(!!db.prepare(`SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = 'idx_maafm_date'`).get(), '索引もある');
ok(!db.prepare(`SELECT 1 FROM sqlite_master WHERE name = 'mirror_amazon_account_fees_monthly_new'`).get(), '作り直しの仮の表は残らない');
threw = false; try { db.prepare(`INSERT INTO mirror_amazon_account_fees_monthly VALUES ('2026-08-01', 'nonsense', -1, 1, 'r', 'h', 't')`).run(); } catch { threw = true; }
ok(threw, '一覧に無い種類は今まで通り受け付けない');

// 参照する view があれば作り直さない (作り直すと view が壊れる)
{
  const dir2 = fs.mkdtempSync(path.join(os.tmpdir(), 'mirror-easyship-view-'));
  const o2 = new Database(path.join(dir2, 'warehouse-mirror.db'));
  o2.exec(`CREATE TABLE mirror_amazon_account_fees_monthly (date_jst TEXT NOT NULL, fee_type TEXT NOT NULL CHECK(fee_type IN ('storage')), amount_jpy REAL NOT NULL DEFAULT 0, row_count INTEGER NOT NULL DEFAULT 0, source_run_id TEXT NOT NULL, source_row_hash TEXT NOT NULL, synced_at TEXT NOT NULL, PRIMARY KEY (date_jst, fee_type))`);
  o2.exec(`CREATE VIEW v_fee_probe AS SELECT fee_type FROM mirror_amazon_account_fees_monthly`);
  o2.close();
  const { spawnSync } = await import('node:child_process');
  let out = '';
  const r2 = spawnSync(process.execPath, ['-e', `import('./apps/warehouse-mirror/db.js').then((m) => { m.initMirrorDB(); })`], { cwd: path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..'), env: { ...process.env, DATA_DIR: dir2 }, encoding: 'utf8' });
  out = String(r2.stdout || '') + String(r2.stderr || '');   // ⚠️ は標準エラーに出る
  const c2 = new Database(path.join(dir2, 'warehouse-mirror.db'), { readonly: true });
  const sql2 = c2.prepare(`SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'mirror_amazon_account_fees_monthly'`).get().sql;
  const view2 = c2.prepare(`SELECT COUNT(*) n FROM v_fee_probe`).get();
  c2.close();
  ok(!sql2.includes("'easy_ship'") && view2 && /作り直せない/.test(out), '参照する view があれば作り直さない (view は壊れない・⚠️ を出す)');
  fs.rmSync(dir2, { recursive: true, force: true });
}

console.log(failed ? `\n❌ ${failed} 件 失敗` : '\n=== Render の Easy Ship の種類テスト ALL PASS ===');
process.exit(failed ? 1 : 0);
