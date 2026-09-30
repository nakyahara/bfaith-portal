#!/usr/bin/env node
/**
 * fba-sheetless-backfill-once.mjs — FBA 補充の「Sheet なしのモード」(FBA_SHEETLESS_MODE・⑦-F) を入れる前に 1 回だけ流す移行。
 *
 * 何をするか:
 *   fba.db の sku_mapping (Google Sheet「商品コード変換テーブル」の写し) の asin / fnsku を、fba_sku_attrs に無い SKU にだけ入れる
 *   (起動のたびに流している backfill と同じ SQL) を最後に 1 回流し、印 (fba_migration_marks の 1 行・時刻と件数) を残す。
 *   印があると、以後の起動では backfill を流さない (モードを外しても流さない) = 止めた Sheet の古い値が再起動で戻らない
 *   (Codex 設計 R1 High 3)。既にある fba_sku_attrs の行は変えない。
 *
 * 使い方 (リポジトリの直下で。モードを入れる **前** に、Render と miniPC のそれぞれの fba.db で 1 回ずつ):
 *   node scripts/fba-sheetless-backfill-once.mjs --check   … 印の有無と件数を見るだけ (fba.db を書かない)
 *   node scripts/fba-sheetless-backfill-once.mjs           … 流して印を残す
 *   DATA_DIR が無ければ <リポジトリ>/data (db.js と同じ)。env は .env も読む (dotenv。既にある env は上書きしない)
 *
 * 断る (終了コード 1。何も書かない):
 *   - FBA_SHEETLESS_MODE が入っている (モードが入った後は sku_mapping の値を一切使わない約束)
 *   - 印がもうある (二度は流さない)
 *   - fba.db が無い (DATA_DIR の間違い。空の fba.db を作らない)
 *
 * 🚨 書き手: fba.db の書き手は常駐のサーバ 1 つが決まり (2026-09-20 の事故)。このスクリプトは 1 回だけの 2 人目の書き手になる。
 *   保存は db.js の saveToFile (ファイルの lock + 「外から書かれていたら上書きしない」) を通るので、相手の行は消えない。
 *   ぶつかったときは、こちらは読み直して最大 3 回やり直す。常駐のサーバ側は次の保存で 1 回だけ失敗して読み直す (やり直せば通る)。
 *   → 画面を使っていない時間 (06:00・09:40〜11:40 の定期処理を外す) に流す。miniPC は WarehouseServer を止めてから流すのがいちばん安全
 *
 * やり直し: 基本は要らない (入れるのは空いている SKU だけ)。どうしても要るときは、モードを外し、常駐のサーバを止めてから
 *   fba.db の fba_migration_marks から key = 'sku_mapping_to_fba_sku_attrs' の行を消して、このスクリプトをもう一度流す
 *   (印を消す口はわざと作らない)。
 */
import 'dotenv/config';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import Database from 'better-sqlite3';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const dataDir = process.env.DATA_DIR || path.join(root, 'data');
const dbFile = path.join(dataDir, 'fba.db');
const checkOnly = process.argv.includes('--check');
const { isSheetlessRequested, BACKFILL_MARK_KEY, SHEETLESS_ENV } = await import(pathToFileURL(path.join(root, 'apps', 'fba-replenishment', 'sheetless-mode.js')).href);

function fail(msg) {
  console.error(`[fba-sheetless-backfill] 断った: ${msg}`);
  process.exitCode = 1;
}

/** 書かずに見る (better-sqlite3 の読むだけの接続) */
function inspect() {
  const f = new Database(dbFile, { readonly: true, fileMustExist: true });
  try {
    const has = (t) => !!f.prepare(`SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?`).get(t);
    const count = (sql) => (has('sku_mapping') && has('fba_sku_attrs') ? Number(f.prepare(sql).get().n) : null);
    const mark = has('fba_migration_marks') ? f.prepare('SELECT key, done_at, detail FROM fba_migration_marks WHERE key = ?').get(BACKFILL_MARK_KEY) || null : null;
    return {
      mark,
      sku_mapping_rows: count('SELECT COUNT(*) AS n FROM sku_mapping'),
      attrs_rows: count('SELECT COUNT(*) AS n FROM fba_sku_attrs'),
      would_insert: count('SELECT COUNT(*) AS n FROM sku_mapping m WHERE m.amazon_sku NOT IN (SELECT amazon_sku FROM fba_sku_attrs)'),
    };
  } finally {
    f.close();
  }
}

async function main() {
  console.log(`[fba-sheetless-backfill] fba.db = ${dbFile} / ${SHEETLESS_ENV} = ${JSON.stringify(process.env[SHEETLESS_ENV] ?? null)}`);
  if (!fs.existsSync(dbFile)) return fail(`fba.db が無い (${dbFile})。DATA_DIR を確かめる`);
  const before = inspect();
  console.log(`[fba-sheetless-backfill] いま: 印=${before.mark ? `あり (${before.mark.done_at})` : 'なし'} / sku_mapping ${before.sku_mapping_rows} 行 / fba_sku_attrs ${before.attrs_rows} 行 / 入れる予定 ${before.would_insert} 行`);
  if (checkOnly) return;
  if (isSheetlessRequested()) return fail(`${SHEETLESS_ENV} が入っている間は流さない (sku_mapping の値を使わない約束)。モードを外してから流す`);
  if (before.mark) return fail(`もう済んでいる (${before.mark.done_at} ${before.mark.detail || ''})。二度は流さない`);

  const db = await import(pathToFileURL(path.join(root, 'apps', 'fba-replenishment', 'db.js')).href);
  await db.initDb();
  try {
    // initDb() は (印が無くモードも無いので) 起動時の backfill を今までどおり流す。ここで数える入った行は 0 になりやすいので、
    // 流す前に数えた件数も印に残す。保存の競合 (常駐のサーバが書いた) は読み直して 3 回までやり直す (試験 = test-fba-sheetless-mode.mjs)
    const r = db.runSkuMappingBackfillOnceRetrying({
      extra: { before_init: { attrs_rows: before.attrs_rows, would_insert: before.would_insert } },
      onRetry: (n) => console.warn(`[fba-sheetless-backfill] 流している間に fba.db が外から書かれた → 読み直してやり直す (${n}/3)`),
    });
    console.log(`[fba-sheetless-backfill] 済んだ: ${JSON.stringify(r)}`);
  } catch (e) {
    if (e && (e.code === 'FBA_BACKFILL_ALREADY_DONE' || e.code === 'FBA_BACKFILL_MODE_ON')) return fail(e.message);
    throw e;
  }
}

main().catch((e) => {
  console.error('[fba-sheetless-backfill] 失敗 (印は残していない):', e);
  process.exitCode = 1;
});
