#!/usr/bin/env node
/**
 * fba-sheetless-backfill-once.mjs — FBA 補充の「Sheet なしのモード」(⑦-F) を入れる前に 1 回だけ流す移行。
 *   Render は FBA_SHEETLESS_MODE、miniPC は FBA_SHEETLESS_IO を入れる **前** に、それぞれの fba.db で 1 回ずつ流す。
 *
 * 何をするか:
 *   fba.db の sku_mapping (Google Sheet「商品コード変換テーブル」の写し) の asin / fnsku を、fba_sku_attrs に無い SKU にだけ入れる
 *   (起動のたびに流している backfill と同じ SQL) を最後に 1 回流し、印 (fba_migration_marks の 1 行・時刻と件数) を残す。
 *   印があると、以後の起動では backfill を流さない (モードを外しても流さない) = 止めた Sheet の古い値が再起動で戻らない
 *   (Codex 設計 R1 High 3)。既にある fba_sku_attrs の行は変えない。
 *   流した後に「sku_mapping にあって fba_sku_attrs に無い SKU が 0」を確かめてから印を書く (0 でなければ巻き戻して断る)。
 *
 * 使い方 (リポジトリの直下で):
 *   node scripts/fba-sheetless-backfill-once.mjs --check   … 印の有無と件数を見るだけ (fba.db を書かない)
 *   node scripts/fba-sheetless-backfill-once.mjs           … 流して印を残す
 *   --min-rows N   sku_mapping の行の下限 (既定 100。本番の Sheet の写しは数千行。少なすぎる = 空の・違う fba.db とみなして断る)
 *   DATA_DIR が無ければ <リポジトリ>/data (db.js と同じ)。env は .env も読む (dotenv。既にある env は上書きしない)
 *
 * 断る (終了コード 1。fba.db に触らない):
 *   - FBA_SHEETLESS_MODE / FBA_SHEETLESS_IO が入っている (入った後は sku_mapping の値を一切使わない約束)
 *   - 印がもうある (二度は流さない)
 *   - fba.db が無い (DATA_DIR の間違い。空の fba.db を作らない)
 *   - sku_mapping か fba_sku_attrs の表が無い (違う DB) / sku_mapping が --min-rows より少ない (空の DB) (Codex PR R1 Medium 3)
 *   - 大小文字だけ違う SKU の衝突がある (下の説明。Codex PR R4 Medium)
 *
 * 🚨 書き手: fba.db の書き手は常駐のサーバ 1 つが決まり (2026-09-20 の事故)。このスクリプトは 1 回だけの 2 人目の書き手になる。
 *   保存は db.js の saveToFile (ファイルの lock + 「外から書かれていたら上書きしない」) を通るので、相手の行は消えない。
 *   ぶつかったときは、こちらは読み直して最大 3 回やり直す。常駐のサーバ側は次の保存で 1 回だけ失敗して読み直す (やり直せば通る)。
 *   → 画面を使っていない時間 (06:00・09:40〜11:40 の定期処理を外す) に流す。miniPC は WarehouseServer を止めてから流すのがいちばん安全
 *
 * 🚨 大小文字だけ違う SKU (Codex PR R4 Medium): SKU は LOWER(TRIM()) で突き合わせる (読み手と同じ)。Sheet の `Alpha-1` と attrs の `alpha-1` は同じ SKU
 *   = attrs に 2 行目を作らない。fba_sku_attrs に大小文字だけ違う行が既に 2 行以上ある鍵、Sheet の中で大小文字だけ違って attrs に無い鍵があれば、
 *   どちらの FNSKU が正しいか機械では決められないので **断る** (--check にも一覧が出る)。
 *   直し方 (人が決める): 常駐のサーバを止め、fba.db の fba_sku_attrs で、その鍵の行のうち Amazon の今のレポートと合う 1 行 (ふつうは
 *   source が restock / planning で updated_at が新しい行) だけを残して他の行を DELETE する → サーバを起動 → このスクリプトを流す。
 *   自動でまとめないのは、Sheet の backfill の行とレポートの行のどちらも「今の Amazon の値」とは言い切れない場合があるため
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
const minArg = process.argv.indexOf('--min-rows');
const minRows = minArg >= 0 ? Number(process.argv[minArg + 1]) : 100;
const { isSheetlessIoRequested, BACKFILL_MARK_KEY, SHEETLESS_ENV, SHEETLESS_IO_ENV } = await import(pathToFileURL(path.join(root, 'apps', 'fba-replenishment', 'sheetless-mode.js')).href);

function fail(msg) {
  console.error(`[fba-sheetless-backfill] 断った: ${msg}`);
  process.exitCode = 1;
}

/** 書かずに見る (better-sqlite3 の読むだけの接続) */
function inspect() {
  const f = new Database(dbFile, { readonly: true, fileMustExist: true });
  try {
    const has = (t) => !!f.prepare(`SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?`).get(t);
    const tables = { sku_mapping: has('sku_mapping'), fba_sku_attrs: has('fba_sku_attrs') };
    const count = (sql) => (tables.sku_mapping && tables.fba_sku_attrs ? Number(f.prepare(sql).get().n) : null);
    const mark = has('fba_migration_marks') ? f.prepare('SELECT key, done_at, detail FROM fba_migration_marks WHERE key = ?').get(BACKFILL_MARK_KEY) || null : null;
    return {
      mark, tables,
      sku_mapping_rows: count('SELECT COUNT(*) AS n FROM sku_mapping'),
      attrs_rows: count('SELECT COUNT(*) AS n FROM fba_sku_attrs'),
      would_insert: count('SELECT COUNT(*) AS n FROM sku_mapping m WHERE LOWER(TRIM(m.amazon_sku)) NOT IN (SELECT LOWER(TRIM(amazon_sku)) FROM fba_sku_attrs)'),
      // 流した後に大小文字だけ違う SKU が attrs に 2 行以上になる鍵 = 今ある衝突 + Sheet の中の大小文字違いで attrs に無いもの
      collisions: tables.sku_mapping && tables.fba_sku_attrs ? f.prepare(`
        WITH u AS (
          SELECT amazon_sku FROM fba_sku_attrs
          UNION
          SELECT m.amazon_sku FROM sku_mapping m WHERE LOWER(TRIM(m.amazon_sku)) NOT IN (SELECT LOWER(TRIM(amazon_sku)) FROM fba_sku_attrs)
        )
        SELECT LOWER(TRIM(amazon_sku)) AS norm_key, group_concat(amazon_sku, ' / ') AS skus FROM u
        GROUP BY LOWER(TRIM(amazon_sku)) HAVING COUNT(*) > 1 ORDER BY norm_key`).all() : [],
    };
  } finally {
    f.close();
  }
}

/** 流してよい fba.db か (違う DB・空の DB に印を付けない)。だめなら理由、よければ null */
function sourceProblem(before) {
  const missing = Object.entries(before.tables).filter(([, ok]) => !ok).map(([t]) => t);
  if (missing.length) return `FBA 補充の fba.db ではない (表 ${missing.join('・')} が無い)。DATA_DIR を確かめる`;
  if (!Number.isFinite(minRows) || minRows < 1) return `--min-rows の値がおかしい (${process.argv[minArg + 1]})`;
  if (before.sku_mapping_rows < minRows) return `sku_mapping が ${before.sku_mapping_rows} 行しかない (下限 ${minRows})。空の・違う fba.db に印を付けない`;
  if (before.collisions.length) return `大小文字だけ違う SKU が ${before.collisions.length} 組ある (流すと fba_sku_attrs に 2 行以上になる): ${before.collisions.slice(0, 20).map((c) => c.skus).join(' | ')}。説明の手順で 1 行にしてから流す`;
  return null;
}

async function main() {
  console.log(`[fba-sheetless-backfill] fba.db = ${dbFile} / ${SHEETLESS_ENV} = ${JSON.stringify(process.env[SHEETLESS_ENV] ?? null)} / ${SHEETLESS_IO_ENV} = ${JSON.stringify(process.env[SHEETLESS_IO_ENV] ?? null)}`);
  if (!fs.existsSync(dbFile)) return fail(`fba.db が無い (${dbFile})。DATA_DIR を確かめる`);
  let before;
  try { before = inspect(); } catch (e) { return fail(`fba.db を読めない (${e.code || ''} ${e.message})。SQLite でない・壊れている`); }
  console.log(`[fba-sheetless-backfill] いま: 印=${before.mark ? `あり (${before.mark.done_at})` : 'なし'} / sku_mapping ${before.sku_mapping_rows} 行 / fba_sku_attrs ${before.attrs_rows} 行 / 入れる予定 ${before.would_insert} 行`);
  const problem = sourceProblem(before);
  if (checkOnly) {
    if (problem) console.log(`[fba-sheetless-backfill] このままでは流せない: ${problem}`);
    return;
  }
  if (isSheetlessIoRequested()) return fail(`${SHEETLESS_ENV} / ${SHEETLESS_IO_ENV} が入っている間は流さない (sku_mapping の値を使わない約束)。外してから流す`);
  if (before.mark) return fail(`もう済んでいる (${before.mark.done_at} ${before.mark.detail || ''})。二度は流さない`);
  if (problem) return fail(problem);

  const db = await import(pathToFileURL(path.join(root, 'apps', 'fba-replenishment', 'db.js')).href);
  await db.initDb({ skipStartupBackfill: true });   // 起動時の backfill (大小文字まで同じものだけ見る) は流さない = 2 行目を作らない
  try {
    // initDb() は (印が無くモードも無いので) 起動時の backfill を今までどおり流す。ここで数える入った行は 0 になりやすいので、
    // 流す前に数えた件数も印に残す。保存の競合 (常駐のサーバが書いた) は読み直して 3 回までやり直す (試験 = test-fba-sheetless-mode.mjs)
    const r = db.runSkuMappingBackfillOnceRetrying({
      minSkuMappingRows: minRows,
      extra: { before_init: { attrs_rows: before.attrs_rows, would_insert: before.would_insert } },
      onRetry: (n) => console.warn(`[fba-sheetless-backfill] 流している間に fba.db が外から書かれた → 読み直してやり直す (${n}/3)`),
    });
    console.log(`[fba-sheetless-backfill] 済んだ: ${JSON.stringify(r)}`);
  } catch (e) {
    if (e && ['FBA_BACKFILL_ALREADY_DONE', 'FBA_BACKFILL_MODE_ON', 'FBA_BACKFILL_SOURCE_EMPTY', 'FBA_BACKFILL_VERIFY_FAILED', 'FBA_BACKFILL_ATTRS_COLLISION'].includes(e.code)) return fail(e.message);
    throw e;
  }
}

main().catch((e) => {
  console.error('[fba-sheetless-backfill] 失敗 (印は残していない):', e);
  process.exitCode = 1;
});
