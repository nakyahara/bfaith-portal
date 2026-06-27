#!/usr/bin/env node
/**
 * ローカル RYS (Downloads/RakutenYahooSync/data/rakuten-yahoo-sync.db) の
 * カテゴリ変換表データを bfaith-portal の RYS DB に bulk import。
 *
 * 経緯:
 *   2026-05-20〜26 に中原さん + Codex で完成済の楽天 genre → Yahoo カテゴリ変換表が
 *   ローカル RYS の DB に死蔵されていた。 Phase E 移植時に持ち込まれず、
 *   bfaith-portal は overlap 学習 (130 件) のみ簡易版になっていた。
 *   本 script は 4 テーブル (category_manual / category_decisions / category_default_path /
 *   rakuten_genre / rakuten_genre_stats) を一括移植する。
 *
 * 使い方:
 *   node apps/rakuten-yahoo-sync/scripts/import-local-rys-category.mjs --src "<source-db-path>" [--dry-run]
 *
 *   --src       : ローカル RYS DB の path (default: "C:/Users/中原　大輔/OneDrive/デスクトップ/Downloads/RakutenYahooSync/data/rakuten-yahoo-sync.db")
 *   --dry-run   : 集計のみ (実 import しない)
 *   --dst       : bfaith-portal DB の path (default: getDB() と同じ参照)
 *
 *   通常は引数なし → ローカル OneDrive 上のソース + bfaith-portal の DATA_DIR 上の DB を使う。
 *
 * 設計 (Codex R1):
 *   - 全 import を 1 transaction
 *   - 失敗時 rollback
 *   - 既存 row は ON CONFLICT DO UPDATE で upsert
 *   - manual vs decisions の衝突 (同 genre で異なる category) を検知してログ出力
 *   - yahoo_path NULL は default_path から補完
 */

import path from 'node:path';
import fs from 'node:fs';
import { fileURLToPath } from 'node:url';
import Db from 'better-sqlite3';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_SRC = 'C:/Users/中原　大輔/OneDrive/デスクトップ/Downloads/RakutenYahooSync/data/rakuten-yahoo-sync.db';
const DEFAULT_DST = path.resolve(process.env.RYS_DB_FILE || path.join(process.env.DATA_DIR || path.join(__dirname, '..', '..', '..', 'data'), 'rakuten-yahoo-sync.db'));

const args = process.argv.slice(2);
const opts = { src: DEFAULT_SRC, dst: DEFAULT_DST, dryRun: false };
for (let i = 0; i < args.length; i += 1) {
  if (args[i] === '--src' && args[i + 1]) { opts.src = args[i + 1]; i += 1; }
  else if (args[i] === '--dst' && args[i + 1]) { opts.dst = args[i + 1]; i += 1; }
  else if (args[i] === '--dry-run') { opts.dryRun = true; }
}

console.log(`[import-local-rys-category] src=${path.basename(opts.src)} dst=${path.basename(opts.dst)} dry-run=${opts.dryRun}`);

if (!fs.existsSync(opts.src)) {
  console.error(`source DB not found: ${opts.src}`);
  process.exit(1);
}
if (!fs.existsSync(opts.dst)) {
  console.error(`destination DB not found: ${opts.dst}. bfaith-portal を 1 回起動して migration を走らせてください。`);
  process.exit(1);
}

const src = new Db(opts.src, { readonly: true });
const dst = new Db(opts.dst);

// migration 015 のテーブル存在チェック
const requiredTables = ['category_manual', 'category_decisions', 'rakuten_genre', 'category_default_path', 'rakuten_genre_stats'];
for (const t of requiredTables) {
  const r = dst.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`).get(t);
  if (!r) {
    console.error(`destination DB に table '${t}' が存在しません。 migration 015 を走らせてください。`);
    process.exit(1);
  }
}

// ============================================================
// 1. 集計 (dry-run でも実行)
// ============================================================
const localManual = src.prepare('SELECT genre_id, product_category, note FROM category_manual').all();
const localDecisions = src.prepare(`
  SELECT genre_id, chosen_product_category AS yahoo_category_id,
         decision_source, confidence, sample_count, ambiguous, locked
  FROM category_decisions
  WHERE decision_source IN ('learned', 'ai')
    AND chosen_product_category IS NOT NULL
`).all();
const localGenre = src.prepare('SELECT genre_id, name_ja, path_ja, level FROM rakuten_genre').all();
// ローカル RYS の category_default_path 実 schema (確認済 2026-06-27):
//   yahoo_product_category (TEXT, PK), default_path (TEXT), source ('learned'|'manual'),
//   sample_count, locked, updated_at
// → bfaith 側の category_default_path は yahoo_category_id (INTEGER, PK), yahoo_path (TEXT)
const localDefaultPath = src.prepare(`
  SELECT yahoo_product_category AS yahoo_category_id, default_path AS path
  FROM category_default_path
  WHERE default_path IS NOT NULL
`).all();
const localGenreStats = src.prepare(`
  SELECT genre_id, top1_category, top1_prob, confidence_band, auto_mode, total_obs_count AS sample_count
  FROM genre_stats
`).all();

console.log(`\n[source 集計]`);
console.log(`  category_manual:       ${localManual.length} 件`);
console.log(`  category_decisions:    ${localDecisions.length} 件 (learned/ai)`);
console.log(`  rakuten_genre:         ${localGenre.length} 件`);
console.log(`  category_default_path: ${localDefaultPath.length} 件`);
console.log(`  genre_stats:           ${localGenreStats.length} 件`);

// 衝突検知 (Codex R1 high-2)
const manualByGenre = new Map(localManual.map((r) => [r.genre_id, r.product_category]));
const decisionsByGenre = new Map(localDecisions.map((r) => [r.genre_id, r.yahoo_category_id]));
const conflicts = [];
for (const [genre, manualCat] of manualByGenre.entries()) {
  const learnedCat = decisionsByGenre.get(genre);
  if (learnedCat && String(learnedCat) !== String(manualCat)) {
    conflicts.push({ genre, manual: manualCat, learned: learnedCat });
  }
}
if (conflicts.length > 0) {
  console.log(`\n⚠️  manual / learned 衝突 ${conflicts.length} 件 (manual 優先で進める):`);
  for (const c of conflicts.slice(0, 10)) {
    console.log(`     genre=${c.genre}: manual=${c.manual} learned=${c.learned}`);
  }
  if (conflicts.length > 10) console.log(`     ... 他 ${conflicts.length - 10} 件`);
} else {
  console.log(`\n✅ manual / learned 衝突なし`);
}

// yahoo_path 補完 (default_path から引く)
const defaultPathByCategory = new Map(localDefaultPath.map((r) => [String(r.yahoo_category_id), r.path]));

const manualWithPath = localManual.map((r) => ({
  rakuten_genre_id: r.genre_id,
  yahoo_category_id: parseInt(r.product_category, 10),
  yahoo_path: defaultPathByCategory.get(String(r.product_category)) || null,
  note: r.note,
}));
const decisionsWithPath = localDecisions.map((r) => ({
  rakuten_genre_id: r.genre_id,
  yahoo_category_id: parseInt(r.yahoo_category_id, 10),
  yahoo_path: defaultPathByCategory.get(String(r.yahoo_category_id)) || null,
  decision_source: r.decision_source,
  confidence: r.confidence,
  sample_count: r.sample_count,
  ambiguous: r.ambiguous,
  locked: r.locked,
}));

const manualPathMissing = manualWithPath.filter((r) => !r.yahoo_path).length;
const decisionsPathMissing = decisionsWithPath.filter((r) => !r.yahoo_path).length;
console.log(`\n[path 補完]`);
console.log(`  manual    yahoo_path 補完済: ${localManual.length - manualPathMissing}/${localManual.length} (NULL: ${manualPathMissing})`);
console.log(`  decisions yahoo_path 補完済: ${localDecisions.length - decisionsPathMissing}/${localDecisions.length} (NULL: ${decisionsPathMissing})`);

if (opts.dryRun) {
  console.log(`\n[dry-run] 実 import せず終了。`);
  process.exit(0);
}

// ============================================================
// 2. import (transaction)
// ============================================================
const upsertManual = dst.prepare(`
  INSERT INTO category_manual (rakuten_genre_id, yahoo_category_id, yahoo_path, note, source, created_at, updated_at)
  VALUES (?, ?, ?, ?, 'imported_from_local_rys',
          strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
          strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
  ON CONFLICT(rakuten_genre_id) DO UPDATE SET
    yahoo_category_id = excluded.yahoo_category_id,
    yahoo_path        = excluded.yahoo_path,
    note              = excluded.note,
    updated_at        = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
`);
const upsertDecisions = dst.prepare(`
  INSERT INTO category_decisions (rakuten_genre_id, yahoo_category_id, yahoo_path,
                                  decision_source, confidence, sample_count, ambiguous, locked,
                                  source, imported_at)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'imported_from_local_rys',
          strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
  ON CONFLICT(rakuten_genre_id) DO UPDATE SET
    yahoo_category_id = excluded.yahoo_category_id,
    yahoo_path        = excluded.yahoo_path,
    decision_source   = excluded.decision_source,
    confidence        = excluded.confidence,
    sample_count      = excluded.sample_count,
    ambiguous         = excluded.ambiguous,
    locked            = excluded.locked,
    imported_at       = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
`);
const upsertGenre = dst.prepare(`
  INSERT INTO rakuten_genre (genre_id, name_ja, path_ja, level, source, imported_at)
  VALUES (?, ?, ?, ?, 'imported_from_local_rys', strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
  ON CONFLICT(genre_id) DO UPDATE SET
    name_ja  = excluded.name_ja,
    path_ja  = excluded.path_ja,
    level    = excluded.level,
    imported_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
`);
const upsertDefaultPath = dst.prepare(`
  INSERT INTO category_default_path (yahoo_category_id, yahoo_path, source, imported_at)
  VALUES (?, ?, 'imported_from_local_rys', strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
  ON CONFLICT(yahoo_category_id) DO UPDATE SET
    yahoo_path  = excluded.yahoo_path,
    imported_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
`);
const upsertGenreStats = dst.prepare(`
  INSERT INTO rakuten_genre_stats (genre_id, top1_category, top1_prob, confidence_band, auto_mode, sample_count, source, imported_at)
  VALUES (?, ?, ?, ?, ?, ?, 'imported_from_local_rys', strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
  ON CONFLICT(genre_id) DO UPDATE SET
    top1_category    = excluded.top1_category,
    top1_prob        = excluded.top1_prob,
    confidence_band  = excluded.confidence_band,
    auto_mode        = excluded.auto_mode,
    sample_count     = excluded.sample_count,
    imported_at      = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
`);

const tx = dst.transaction(() => {
  let mc = 0, dc = 0, gc = 0, pc = 0, sc = 0;
  for (const r of manualWithPath) {
    upsertManual.run(r.rakuten_genre_id, r.yahoo_category_id, r.yahoo_path, r.note);
    mc += 1;
  }
  for (const r of decisionsWithPath) {
    upsertDecisions.run(
      r.rakuten_genre_id, r.yahoo_category_id, r.yahoo_path,
      r.decision_source, r.confidence, r.sample_count, r.ambiguous, r.locked,
    );
    dc += 1;
  }
  for (const r of localGenre) {
    upsertGenre.run(r.genre_id, r.name_ja, r.path_ja, r.level);
    gc += 1;
  }
  for (const r of localDefaultPath) {
    upsertDefaultPath.run(parseInt(r.yahoo_category_id, 10), r.path);
    pc += 1;
  }
  for (const r of localGenreStats) {
    upsertGenreStats.run(
      r.genre_id,
      r.top1_category ? parseInt(r.top1_category, 10) : null,
      r.top1_prob,
      r.confidence_band,
      r.auto_mode,
      r.sample_count,
    );
    sc += 1;
  }
  return { mc, dc, gc, pc, sc };
});

const result = tx();

console.log(`\n✅ import 完了 (transaction commit):`);
console.log(`   category_manual       upserted: ${result.mc}`);
console.log(`   category_decisions    upserted: ${result.dc}`);
console.log(`   rakuten_genre         upserted: ${result.gc}`);
console.log(`   category_default_path upserted: ${result.pc}`);
console.log(`   rakuten_genre_stats   upserted: ${result.sc}`);

// 完了後の bfaith-portal 側集計
const finalCounts = {
  manual: dst.prepare('SELECT COUNT(*) as c FROM category_manual').get().c,
  decisions: dst.prepare('SELECT COUNT(*) as c FROM category_decisions').get().c,
  decisionsLocked: dst.prepare('SELECT COUNT(*) as c FROM category_decisions WHERE locked = 1 AND ambiguous = 0').get().c,
  genre: dst.prepare('SELECT COUNT(*) as c FROM rakuten_genre').get().c,
  defaultPath: dst.prepare('SELECT COUNT(*) as c FROM category_default_path').get().c,
  stats: dst.prepare('SELECT COUNT(*) as c FROM rakuten_genre_stats').get().c,
};
console.log(`\n[bfaith-portal 側 row 数 (import 後)]:`);
for (const [k, v] of Object.entries(finalCounts)) {
  console.log(`   ${k}: ${v}`);
}

console.log(`\n次のステップ: dashboard で「いま再取得」 を押して 「出せる」 件数を確認してください。`);
src.close();
dst.close();
