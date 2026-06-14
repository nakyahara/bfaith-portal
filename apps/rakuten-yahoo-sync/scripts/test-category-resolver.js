/**
 * Phase E-11-c smoke: category-resolver の優先順位 (notion > learned > unresolved)。
 *
 * 使い方: node apps/rakuten-yahoo-sync/scripts/test-category-resolver.js
 */

import Database from 'better-sqlite3';
import { resolveCategoryAndPath, resolveByGenreId } from '../services/category-resolver.js';

const checks = [];
function expect(label, cond, detail) {
  checks.push({ label, ok: !!cond, detail });
  console.log(`${cond ? '✅' : '❌'} ${label}${detail ? ` — ${detail}` : ''}`);
}

const db = new Database(':memory:');
db.exec(`
  CREATE TABLE genre_yahoo_category_mapping (
    rakuten_genre_id TEXT NOT NULL,
    yahoo_category_id INTEGER NOT NULL,
    yahoo_path TEXT NOT NULL,
    sample_count INTEGER NOT NULL DEFAULT 1,
    is_primary INTEGER NOT NULL DEFAULT 0,
    first_learned_at TEXT NOT NULL,
    last_learned_at TEXT NOT NULL,
    PRIMARY KEY (rakuten_genre_id, yahoo_category_id, yahoo_path)
  );
`);
const NOW = '2026-06-14T10:00:00Z';
const ins = db.prepare(`INSERT INTO genre_yahoo_category_mapping (rakuten_genre_id, yahoo_category_id, yahoo_path, sample_count, is_primary, first_learned_at, last_learned_at) VALUES (?, ?, ?, ?, ?, ?, ?)`);
ins.run('100371', 12345, 'zakka/main', 5, 1, NOW, NOW);
ins.run('100371', 12346, 'zakka/sub', 1, 0, NOW, NOW);   // not primary
ins.run('200500', 99999, 'bungu', 2, 1, NOW, NOW);       // Codex R1 H-2: sample>=2 で primary 採用
ins.run('300700', 11111, 'risky', 1, 1, NOW, NOW);       // Codex R1 H-2: sample=1 → 採用しない想定

// 1. resolveByGenreId 単体
expect('resolveByGenreId(100371) → primary (12345, zakka/main, count=5)',
  resolveByGenreId(db, '100371')?.category === 12345 && resolveByGenreId(db, '100371')?.path === 'zakka/main');
expect('resolveByGenreId(200500) → (99999, bungu)',
  resolveByGenreId(db, '200500')?.category === 99999);
expect('resolveByGenreId(unknown) → null', resolveByGenreId(db, 'XXX') === null);
expect('resolveByGenreId(null) → null', resolveByGenreId(db, null) === null);
expect('resolveByGenreId(empty) → null', resolveByGenreId(db, '') === null);

// 2. resolveCategoryAndPath — 学習辞書からの推定
const r1 = resolveCategoryAndPath({ db, rakutenGenreId: '100371' });
expect('genre のみ → source=learned', r1.source === 'learned' && r1.category === 12345 && r1.path === 'zakka/main');

// 3. Notion override 優先 (両方 in)
const r2 = resolveCategoryAndPath({
  db, rakutenGenreId: '100371',
  notionOverride: { product_category: 77777, path: 'manual/path' },
});
expect('Notion override > 学習辞書 (両方 in)', r2.source === 'notion' && r2.category === 77777 && r2.path === 'manual/path');

// 4. (R1 H-3 で挙動変更) Notion override 不完全は notion_partial で blocked、 学習辞書 fallback しない。
//    詳細テストは下記 R1 H-3 セクション参照。

// 5. genre 未学習 → unresolved
const r4 = resolveCategoryAndPath({ db, rakutenGenreId: 'UNKNOWN' });
expect('unknown genre → source=unresolved, category=null', r4.source === 'unresolved' && r4.category === null);

// 6. genre null かつ Notion なし → unresolved
const r5 = resolveCategoryAndPath({ db });
expect('no input → unresolved', r5.source === 'unresolved');

// 7. migration 013 未適用環境 (DB が無い) → resolveByGenreId(no table) → null、 unresolved
const db2 = new Database(':memory:');
const r6 = resolveCategoryAndPath({ db: db2, rakutenGenreId: '100371' });
expect('migration 013 未適用環境でも crash しない', r6.source === 'unresolved');

// ───── Codex R1 H-2: sample_count 閾値 ─────
const rSample1 = resolveCategoryAndPath({ db, rakutenGenreId: '300700' });
expect('R1 H-2: sample=1 primary は採用しない (unresolved)', rSample1.source === 'unresolved',
  `got source=${rSample1.source}`);

// 環境変数で閾値を下げると sample=1 でも採用される
process.env.RYS_CATEGORY_MIN_SAMPLE = '1';
const rSample1Allowed = resolveCategoryAndPath({ db, rakutenGenreId: '300700' });
expect('R1 H-2: RYS_CATEGORY_MIN_SAMPLE=1 なら sample=1 も learned',
  rSample1Allowed.source === 'learned' && rSample1Allowed.category === 11111);
delete process.env.RYS_CATEGORY_MIN_SAMPLE;

// ───── Codex R1 H-3: Notion override 片方だけは notion_partial で blocked ─────
const rPartialCat = resolveCategoryAndPath({
  db, rakutenGenreId: '100371',
  notionOverride: { product_category: 77777, path: null },
});
expect('R1 H-3: Notion category だけ → notion_partial (学習辞書 fallback しない)',
  rPartialCat.source === 'notion_partial' && rPartialCat.category === null,
  JSON.stringify(rPartialCat));

const rPartialPath = resolveCategoryAndPath({
  db, rakutenGenreId: '100371',
  notionOverride: { product_category: null, path: 'manual/path' },
});
expect('R1 H-3: Notion path だけ → notion_partial',
  rPartialPath.source === 'notion_partial' && rPartialPath.path === null,
  JSON.stringify(rPartialPath));

// 両方空 → 学習辞書 fallback (H-3 影響なし)
const rEmpty = resolveCategoryAndPath({
  db, rakutenGenreId: '100371',
  notionOverride: { product_category: null, path: null },
});
expect('R1 H-3: Notion 両方空 → 学習辞書 fallback OK',
  rEmpty.source === 'learned' && rEmpty.category === 12345);

const failed = checks.filter((c) => !c.ok);
if (failed.length > 0) {
  console.error(`\n❌ ${failed.length}/${checks.length} checks failed`);
  process.exit(1);
}
console.log(`\n✅ all ${checks.length} checks passed`);
