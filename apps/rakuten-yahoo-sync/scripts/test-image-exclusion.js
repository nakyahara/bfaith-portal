/**
 * Phase E-8 smoke test: 画像ファイル名パターン除外 (image-exclusion-patterns + filterUploadableImageUrlsDetailed)。
 *
 * - in-memory DB に migration 010 を直接適用
 * - addPattern / removePattern / listAllPatterns CRUD
 * - filterUploadableImageUrls (legacy) / filterUploadableImageUrlsDetailed (E-8 new) の挙動
 * - builtin との conflict / duplicate / not-found エラーマッピング
 *
 * 使い方: node apps/rakuten-yahoo-sync/scripts/test-image-exclusion.js
 */

import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import {
  filterUploadableImageUrls,
  filterUploadableImageUrlsDetailed,
  EXCLUDED_URL_PATTERNS,
} from '../lib/yahoo-image.js';
import {
  listCustomPatterns,
  listAllPatterns,
  getCustomPatternStrings,
  addPattern,
  removePattern,
  matchPattern,
} from '../lib/image-exclusion-patterns.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const MIG_010 = path.join(__dirname, '..', 'db', 'migrations', '010_image_exclusion_patterns.sql');

const checks = [];
function expect(label, cond, detail) {
  checks.push({ label, ok: !!cond, detail });
  console.log(`${cond ? '✅' : '❌'} ${label}${detail ? ` — ${detail}` : ''}`);
}

function setupDb() {
  const db = new Database(':memory:');
  db.exec(fs.readFileSync(MIG_010, 'utf8'));
  return db;
}

const db = setupDb();

// 1) builtin only
{
  const imgs = [
    { location: 'https://example.com/normal_001.jpg' },
    { location: 'https://example.com/coupon_002.jpg' },     // builtin: coupon
    { location: 'https://example.com/REVIEW_special.png' }, // builtin: review (case-insensitive)
    { location: 'https://example.com/Plain_03.jpg' },
  ];
  const legacy = filterUploadableImageUrls(imgs);
  expect('legacy filter keeps 2', legacy.length === 2, `kept=${legacy.length}`);
  const det = filterUploadableImageUrlsDetailed(imgs);
  expect('detailed kept=2', det.kept.length === 2);
  expect('detailed excluded=2', det.excluded.length === 2);
  expect('detailed builtin source',
    det.excluded.every((e) => e.source === 'builtin'),
    JSON.stringify(det.excluded.map((e) => e.source)));
}

// 2) addPattern + custom pattern in filter
{
  const r = addPattern({ db, pattern: 'campaign', reason: 'キャンペーン素材', actor: 'smoke' });
  expect('addPattern returns id', typeof r.id === 'number' && r.id > 0, `id=${r.id}`);
  const all = listAllPatterns(db);
  expect('listAll includes builtin + 1 custom',
    all.length === EXCLUDED_URL_PATTERNS.length + 1,
    `all.length=${all.length}`);
  const customs = getCustomPatternStrings(db);
  expect('getCustomPatternStrings=[campaign]',
    customs.length === 1 && customs[0] === 'campaign');

  const imgs = [
    'https://example.com/main.jpg',
    'https://example.com/CAMPAIGN_big.jpg',
    'https://example.com/coupon_001.jpg',
  ];
  const det = filterUploadableImageUrlsDetailed(imgs, { customPatterns: customs });
  expect('custom matches campaign (case-insensitive)',
    det.excluded.some((e) => e.matchedPattern === 'campaign' && e.source === 'custom'));
  expect('builtin still matches coupon',
    det.excluded.some((e) => e.matchedPattern === 'coupon' && e.source === 'builtin'));
  expect('kept = main.jpg',
    det.kept.length === 1 && det.kept[0].url.includes('main.jpg'));
}

// 3) duplicate (case-insensitive)
{
  let dup;
  try { addPattern({ db, pattern: 'CAMPAIGN', reason: 'dup test', actor: 'smoke' }); }
  catch (e) { dup = e; }
  expect('duplicate add throws PATTERN_DUPLICATE',
    dup && dup.code === 'PATTERN_DUPLICATE');
}

// 4) builtin conflict
{
  let conflict;
  try { addPattern({ db, pattern: 'mycoupon', reason: 'should conflict', actor: 'smoke' }); }
  catch (e) { conflict = e; }
  expect('pattern containing builtin throws PATTERN_CONFLICT_BUILTIN',
    conflict && conflict.code === 'PATTERN_CONFLICT_BUILTIN');
}

// 5) validation
{
  let v;
  try { addPattern({ db, pattern: '   ', reason: 'x', actor: 'smoke' }); } catch (e) { v = e; }
  expect('empty pattern throws', v && /pattern is required/.test(v.message));
  let v2;
  try { addPattern({ db, pattern: 'foo', reason: '', actor: 'smoke' }); } catch (e2) { v2 = e2; }
  expect('empty reason throws', v2 && /reason is required/.test(v2.message));
}

// 6) removePattern
{
  const customs = listCustomPatterns(db);
  const target = customs[0];
  expect('we have 1 custom to remove', !!target);
  const r = removePattern({ db, id: target.id });
  expect('removePattern returns removed=true', r.removed === true);
  let nf;
  try { removePattern({ db, id: target.id }); } catch (e) { nf = e; }
  expect('second remove throws PATTERN_NOT_FOUND',
    nf && nf.code === 'PATTERN_NOT_FOUND');
  const customs2 = listCustomPatterns(db);
  expect('listCustomPatterns now empty', customs2.length === 0);
}

// 7) matchPattern helper
{
  const r1 = matchPattern('https://example.com/abc-coupon.jpg', ['coupon']);
  expect('matchPattern matches coupon', r1.matched === true && r1.matchedPattern === 'coupon');
  const r2 = matchPattern('https://example.com/abc.jpg', ['coupon']);
  expect('matchPattern misses non-match', r2.matched === false);
  const r3 = matchPattern('', ['coupon']);
  expect('matchPattern empty url returns false', r3.matched === false);
}

// 8) Codex E-8 R1 M-2: matchedPattern は元表記 (大文字含む) を返す
{
  const r = addPattern({ db, pattern: 'Campaign-A', reason: 'orig case', actor: 'smoke' });
  expect('addPattern preserves original case', r.pattern === 'Campaign-A');
  const customs = getCustomPatternStrings(db);
  const det = filterUploadableImageUrlsDetailed(
    ['https://example.com/campaign-a_001.jpg'],
    { customPatterns: customs }
  );
  expect('matchedPattern returns original (Camel) not lowercased',
    det.excluded.length === 1 && det.excluded[0].matchedPattern === 'Campaign-A',
    JSON.stringify(det.excluded));
  removePattern({ db, id: r.id });
}

// 9) Codex E-8 R1 M-3: builtin overlap エラーメッセージは状況別
{
  let e1; try { addPattern({ db, pattern: 'coupon', reason: 'x', actor: 'smoke' }); } catch (e) { e1 = e; }
  expect('exact match builtin → 「同じため登録不可」',
    e1 && /同じため/.test(e1.message), e1?.message);
  let e2; try { addPattern({ db, pattern: 'special-coupon', reason: 'x', actor: 'smoke' }); } catch (e) { e2 = e; }
  expect('contains builtin → 「組込みが既にカバー」',
    e2 && /既にカバー/.test(e2.message), e2?.message);
  let e3; try { addPattern({ db, pattern: 'coup', reason: 'x', actor: 'smoke' }); } catch (e) { e3 = e; }
  expect('substring of builtin → 「広く除外する恐れ」',
    e3 && /広く除外/.test(e3.message), e3?.message);
}

const failed = checks.filter((c) => !c.ok);
if (failed.length > 0) {
  console.error(`\n❌ ${failed.length}/${checks.length} checks failed`);
  process.exit(1);
}
console.log(`\n✅ all ${checks.length} checks passed`);
