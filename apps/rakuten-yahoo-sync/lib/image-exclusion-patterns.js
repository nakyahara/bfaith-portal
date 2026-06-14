/**
 * Phase E-8: 楽天画像ファイル名パターン除外管理 (CRUD)。
 *
 * 設計原則:
 *   - 「組込み」 (`coupon` / `review`) は lib/yahoo-image.js EXCLUDED_URL_PATTERNS に hardcode、
 *     UI からは削除不可、 listAll で source='builtin' として最初に並べる
 *   - ユーザー追加分は image_exclusion_patterns テーブル、 source='custom'
 *   - 部分一致 + 大文字小文字区別なし (中原さん R1 確定)
 *
 * 提供:
 *   - listCustomPatterns(db): [{ id, pattern, reason, created_at, created_by }]
 *   - listAllPatterns(db): [{ pattern, reason, source: 'builtin'|'custom', id?, created_at? }]
 *   - addPattern({ db, pattern, reason, actor })
 *   - removePattern({ db, id })
 *   - getCustomPatternStrings(db): string[] — yahoo-image.js に渡す用
 *   - matchPattern(url, patterns): { matched: bool, matchedPattern?: string }
 */

import { EXCLUDED_URL_PATTERNS as BUILTIN_PATTERNS } from './yahoo-image.js';

const BUILTIN_LABELS = {
  coupon: 'クーポン画像 (組込み: 楽天クーポン専用)',
  review: 'レビュー特典画像 (組込み: 楽天レビュー特典)',
};

function isoNow() { return new Date().toISOString(); }

function assertPattern(pattern) {
  if (typeof pattern !== 'string') {
    throw new Error('image-exclusion: pattern is required (string)');
  }
  const trimmed = pattern.trim();
  if (trimmed.length === 0) {
    throw new Error('image-exclusion: pattern is required (non-empty)');
  }
  if (trimmed.length > 200) {
    throw new Error('image-exclusion: pattern too long (max 200 chars)');
  }
  return trimmed;
}

function assertReason(reason) {
  if (typeof reason !== 'string' || reason.trim().length === 0) {
    throw new Error('image-exclusion: reason is required (non-empty string)');
  }
  return reason.trim();
}

/**
 * ユーザー追加分の pattern を返す (新しい順)。
 */
export function listCustomPatterns(db) {
  return db.prepare(`
    SELECT id, pattern, reason, created_at, created_by
    FROM image_exclusion_patterns
    ORDER BY id DESC
  `).all();
}

/**
 * 組込み + ユーザー追加分を統合して返す (UI 表示用)。
 */
export function listAllPatterns(db) {
  const out = [];
  for (const p of BUILTIN_PATTERNS) {
    out.push({
      pattern: p,
      reason: BUILTIN_LABELS[p] || `組込みパターン (${p})`,
      source: 'builtin',
    });
  }
  for (const row of listCustomPatterns(db)) {
    out.push({
      id: row.id,
      pattern: row.pattern,
      reason: row.reason,
      created_at: row.created_at,
      created_by: row.created_by,
      source: 'custom',
    });
  }
  return out;
}

/**
 * ユーザー追加分の pattern 文字列だけ返す (lib/yahoo-image.js に渡す)。
 */
export function getCustomPatternStrings(db) {
  return listCustomPatterns(db).map((r) => r.pattern);
}

/**
 * pattern を追加。 同 pattern (case-insensitive) は UNIQUE INDEX で防ぐ。
 */
export function addPattern({ db, pattern, reason, actor = 'manual' }) {
  const cleanPattern = assertPattern(pattern);
  const cleanReason = assertReason(reason);
  // 組込みと重複は拒否 (組込み側で既にカバー済、 ユーザー誤登録防止)
  // Codex E-8 R1 M-3: error message は UI に出るので、 どっち向きに overlap したかを明示する
  const lowerPattern = cleanPattern.toLowerCase();
  for (const p of BUILTIN_PATTERNS) {
    if (lowerPattern === p) {
      const err = new Error(`組込みパターン "${p}" と同じため登録不可です (組込みは常に有効です)`);
      err.code = 'PATTERN_CONFLICT_BUILTIN';
      throw err;
    }
    if (lowerPattern.includes(p)) {
      const err = new Error(`"${cleanPattern}" は組込みパターン "${p}" を含むため登録不要です (組込みが既にカバーします)`);
      err.code = 'PATTERN_CONFLICT_BUILTIN';
      throw err;
    }
    if (p.includes(lowerPattern)) {
      const err = new Error(`"${cleanPattern}" は組込みパターン "${p}" の一部のため登録不可です (誤って広く除外する恐れがあります)`);
      err.code = 'PATTERN_CONFLICT_BUILTIN';
      throw err;
    }
  }
  try {
    const r = db.prepare(`
      INSERT INTO image_exclusion_patterns (pattern, reason, created_at, created_by)
      VALUES (?, ?, ?, ?)
    `).run(cleanPattern, cleanReason, isoNow(), actor);
    return { id: r.lastInsertRowid, pattern: cleanPattern, reason: cleanReason, source: 'custom' };
  } catch (e) {
    if (e.code === 'SQLITE_CONSTRAINT_UNIQUE') {
      const err = new Error(`image-exclusion: pattern "${cleanPattern}" already exists`);
      err.code = 'PATTERN_DUPLICATE';
      throw err;
    }
    throw e;
  }
}

/**
 * pattern を削除 (組込みは削除できない、 そもそも id を持たないので別経路)。
 */
export function removePattern({ db, id }) {
  if (!Number.isInteger(id) || id <= 0) {
    throw new Error('image-exclusion: id must be a positive integer');
  }
  const r = db.prepare(`DELETE FROM image_exclusion_patterns WHERE id = ?`).run(id);
  if (r.changes === 0) {
    const err = new Error(`image-exclusion: pattern id=${id} not found`);
    err.code = 'PATTERN_NOT_FOUND';
    throw err;
  }
  return { removed: true, id };
}

/**
 * 1 URL に対して 「どのパターンでマッチしたか」 を返す helper。
 * combinedPatterns = builtin + custom の文字列配列。
 *   matched=false なら通る画像、 matched=true なら除外対象。
 */
export function matchPattern(url, combinedPatterns) {
  if (typeof url !== 'string' || url.trim() === '') {
    return { matched: false };
  }
  const lower = url.toLowerCase();
  for (const p of combinedPatterns) {
    if (typeof p !== 'string' || p.length === 0) continue;
    if (lower.includes(p.toLowerCase())) {
      return { matched: true, matchedPattern: p };
    }
  }
  return { matched: false };
}
