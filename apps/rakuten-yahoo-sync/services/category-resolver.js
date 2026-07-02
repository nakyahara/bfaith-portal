/**
 * 楽天 genre → Yahoo カテゴリ自動推定。
 *
 * 設計 (Phase E-12 拡張、 2026-06-27):
 *   ローカル RYS (Downloads/RakutenYahooSync) 由来の変換表 (migration 015) を統合し、
 *   多段 fallback で resolve する。
 *
 *   優先順序 (Codex Phase E-12 R1 → 再設計 R2 で ai を追加):
 *     1. Notion override (`notion_overrides.product_category` + `notion_path`)
 *        中原さんが意図的に片方だけ入れてる場合は fail-closed (notion_partial)
 *     2. category_manual (genre 単位、 中原さん人手確定済)
 *     3. category_decisions (overlap 学習、 locked=1 AND ambiguous=0 のみ自動採用)
 *     4. legacy genre_yahoo_category_mapping (PR #318 由来、 130 件)
 *     5. category_ai (再設計 R2: Claude Code 一括生成の AI 初期紐づけ)
 *        実績ベース (manual/decisions/learned) が無い genre の空白を埋める最終 fallback。
 *        AI 推定は実出品の観測より弱い根拠なので、 人手・実績の全 tier より下位に置く。
 *     6. null (blocked、 紐付け画面で人が確定)
 *
 *   yahoo_path 補完:
 *     - manual/decisions/ai に yahoo_path がなければ category_default_path から引く
 *     - それでも無ければ category は確定でも path null で blocked
 */

/**
 * Codex E-11 R1 H-2: sample_count が閾値未満の primary は誤学習リスクが高いので使わない。
 *   env RYS_CATEGORY_MIN_SAMPLE で上書き可、 default は安全側で 2。
 */
function minSampleCount() {
  const v = parseInt(process.env.RYS_CATEGORY_MIN_SAMPLE || '', 10);
  return Number.isFinite(v) && v >= 1 ? v : 2;
}

/**
 * 再設計 R6 (パフォーマンス): prepared statement を db 接続ごとに cache する。
 *   listProductsForUI が候補全件 (数百行) × 各 tier で resolver を呼ぶため、
 *   毎回 db.prepare() (SQL コンパイル) すると dashboard 表示のたびに
 *   数百×5 回のコンパイルが走っていた。 better-sqlite3 は自動 cache しない。
 *   WeakMap なので接続が閉じられれば cache も回収される。
 */
const _stmtCache = new WeakMap();

function cachedStmt(db, key, sql) {
  let m = _stmtCache.get(db);
  if (!m) { m = new Map(); _stmtCache.set(db, m); }
  let s = m.get(key);
  if (!s) { s = db.prepare(sql); m.set(key, s); }
  return s;
}

export function resolveByGenreId(db, genreId, { minSample = null } = {}) {
  if (!db || !genreId) return null;
  const threshold = minSample == null ? minSampleCount() : minSample;
  try {
    return cachedStmt(db, 'learned', `
      SELECT yahoo_category_id AS category, yahoo_path AS path, sample_count
      FROM genre_yahoo_category_mapping
      WHERE rakuten_genre_id = ? AND is_primary = 1 AND sample_count >= ?
      LIMIT 1
    `).get(genreId, threshold) || null;
  } catch (_) {
    // migration 013 未適用
    return null;
  }
}

/**
 * category_manual (中原さん人手確定値) を引く。 yahoo_path NULL なら category_default_path で補完。
 * migration 015 未適用 (table 無し) の場合は null (silent fallback で legacy へ)。
 *
 * Codex R2 medium: silent catch は schema drift / SQL typo も握ってしまうため、
 *   table 不在 (no such table) は静かに null、 他のエラーは console.warn で可視化。
 */
function resolveFromManual(db, genreId) {
  if (!db || !genreId) return null;
  try {
    const row = cachedStmt(db, 'manual', `
      SELECT m.yahoo_category_id, m.yahoo_path, m.note,
             COALESCE(m.yahoo_path, dp.yahoo_path) AS resolved_path
      FROM category_manual m
      LEFT JOIN category_default_path dp
        ON dp.yahoo_category_id = m.yahoo_category_id
      WHERE m.rakuten_genre_id = ?
      LIMIT 1
    `).get(genreId);
    if (!row || !Number.isFinite(row.yahoo_category_id)) return null;
    return {
      category: row.yahoo_category_id,
      path: row.resolved_path || null,
      note: row.note,
    };
  } catch (e) {
    if (!/no such table/i.test(e.message || '')) {
      console.warn(`[category-resolver] resolveFromManual error: ${e.message}`);
    }
    return null;
  }
}

/**
 * category_decisions (overlap 学習) を引く。 locked=1 AND ambiguous=0 のみ自動採用。
 * conditional (locked=0) や ambiguous=1 は採用しない (Codex E-12 R1: conditional は保存のみ)。
 */
function resolveFromDecisions(db, genreId) {
  if (!db || !genreId) return null;
  try {
    const row = cachedStmt(db, 'decisions', `
      SELECT cd.yahoo_category_id, cd.yahoo_path, cd.confidence, cd.sample_count,
             COALESCE(cd.yahoo_path, dp.yahoo_path) AS resolved_path
      FROM category_decisions cd
      LEFT JOIN category_default_path dp
        ON dp.yahoo_category_id = cd.yahoo_category_id
      WHERE cd.rakuten_genre_id = ?
        AND cd.locked = 1
        AND cd.ambiguous = 0
      LIMIT 1
    `).get(genreId);
    if (!row || !Number.isFinite(row.yahoo_category_id)) return null;
    return {
      category: row.yahoo_category_id,
      path: row.resolved_path || null,
      confidence: row.confidence,
      sampleCount: row.sample_count,
    };
  } catch (e) {
    if (!/no such table/i.test(e.message || '')) {
      console.warn(`[category-resolver] resolveFromDecisions error: ${e.message}`);
    }
    return null;
  }
}

/**
 * category_ai (再設計 R2: Claude Code 一括生成の AI 初期紐づけ) を引く。
 * path は category_default_path からのみ補完 (AI は path=店の棚 を推定しない。 店カテゴリは人の管轄)。
 *
 * Codex R2 R1 Medium ×2:
 *   - exact_match 以外は confidence >= 0.6 を要求 (seed 生成ミスの低確信行を採用しない二重ゲート)
 *   - yahoo_category_master に実在 + is_active=1 のカテゴリのみ採用 (廃止 ID の publish を防ぐ)
 */
const AI_MIN_CONFIDENCE = 0.6;

function resolveFromAi(db, genreId) {
  if (!db || !genreId) return null;
  try {
    const row = cachedStmt(db, 'ai', `
      SELECT ai.yahoo_category_id, ai.confidence, ai.decided_by,
             dp.yahoo_path AS resolved_path
      FROM category_ai ai
      JOIN yahoo_category_master m
        ON m.product_category = CAST(ai.yahoo_category_id AS TEXT)
       AND m.is_active = 1
      LEFT JOIN category_default_path dp
        ON dp.yahoo_category_id = ai.yahoo_category_id
      WHERE ai.rakuten_genre_id = ?
        AND (ai.decided_by = 'exact_match' OR ai.confidence >= ${AI_MIN_CONFIDENCE})
      LIMIT 1
    `).get(genreId);
    if (!row || !Number.isFinite(row.yahoo_category_id)) return null;
    return {
      category: row.yahoo_category_id,
      path: row.resolved_path || null,
      confidence: row.confidence,
      decidedBy: row.decided_by,
    };
  } catch (e) {
    if (!/no such table/i.test(e.message || '')) {
      console.warn(`[category-resolver] resolveFromAi error: ${e.message}`);
    }
    return null;
  }
}

/**
 * Notion override > 学習辞書 > null の優先順位で category/path を返す。
 *
 * @param {object} opts
 * @param {Database} opts.db
 * @param {string|null} opts.rakutenGenreId
 * @param {object|null} [opts.notionOverride]  notion_overrides 行 (product_category, path 列を持つ可能性)
 * @returns {{ category: number|null, path: string|null, source: 'notion'|'learned'|'unresolved', sampleCount?: number }}
 */
export function resolveCategoryAndPath({ db, rakutenGenreId, notionOverride = null } = {}) {
  // 1. Notion override
  //   Codex E-11 R1 H-3: 「片方だけ入力」 は中原さんが手で書き換えた意図のはずなので、
  //   その意図を尊重して fail-closed (学習辞書に勝手にフォールバックしない)。
  if (notionOverride) {
    // publish-pipeline.js から SELECT * で渡される時は notion_product_category / notion_path、
    // router.js (listProductsForUI / detail) から短縮 mapping で渡される時は product_category / path。
    // どちらの形でも読めるよう両方試す (PR #343 deploy 後の column-name mismatch fix)。
    const nc = notionOverride.product_category ?? notionOverride.notion_product_category;
    const np = notionOverride.path ?? notionOverride.notion_path;
    const hasCategory = nc != null && nc !== '' && Number.isFinite(Number(nc));
    const hasPath = typeof np === 'string' && np.trim().length > 0;
    if (hasCategory && hasPath) {
      return { category: Number(nc), path: np.trim(), source: 'notion' };
    }
    if (hasCategory || hasPath) {
      // 中原さんが意図的に片方だけ入れてる → 学習辞書で残り埋めず blocked
      return {
        category: null, path: null,
        source: 'notion_partial',
        notionPartialDetail: {
          hasCategory, hasPath,
          notionCategory: hasCategory ? Number(nc) : null,
          notionPath: hasPath ? np.trim() : null,
        },
      };
    }
  }
  // 2. category_manual (Phase E-12: ローカル RYS 由来の中原さん人手確定値)
  //   Codex R3 high: hit したのに path 補完できない場合は fail-closed (下位 fallback 禁止)。
  //   manual の確定カテゴリを legacy 学習で上書きされるのを防ぐ。
  if (rakutenGenreId) {
    const manual = resolveFromManual(db, rakutenGenreId);
    if (manual && Number.isFinite(manual.category)) {
      if (manual.path) {
        return { category: manual.category, path: manual.path, source: 'manual', note: manual.note };
      }
      return {
        category: null, path: null,
        source: 'manual_path_missing',
        manualCategory: manual.category, manualNote: manual.note,
      };
    }
  }
  // 3. category_decisions (Phase E-12: ローカル RYS 由来の overlap 学習結果、 locked かつ非 ambiguous のみ)
  //   Codex R3 high: 同様に hit + path 欠落は fail-closed。
  if (rakutenGenreId) {
    const decided = resolveFromDecisions(db, rakutenGenreId);
    if (decided && Number.isFinite(decided.category)) {
      if (decided.path) {
        return {
          category: decided.category, path: decided.path,
          source: 'decisions', confidence: decided.confidence, sampleCount: decided.sampleCount,
        };
      }
      return {
        category: null, path: null,
        source: 'decisions_path_missing',
        decisionsCategory: decided.category,
      };
    }
  }
  // 4. legacy genre_yahoo_category_mapping (PR #318 由来)
  if (rakutenGenreId) {
    const learned = resolveByGenreId(db, rakutenGenreId);
    if (learned && Number.isFinite(learned.category) && learned.path) {
      return { category: learned.category, path: learned.path, source: 'learned', sampleCount: learned.sample_count };
    }
  }
  // 5. category_ai (再設計 R2: AI 初期紐づけ。 実績系 tier が全部空振りした genre の空白を埋める)
  //   path が default_path で補完できない場合は fail-closed (ai_path_missing)。
  //   紐付け画面には「ID は AI 提案済、 path だけ入力」として出る (unresolved-genres 側で扱う)。
  if (rakutenGenreId) {
    const ai = resolveFromAi(db, rakutenGenreId);
    if (ai && Number.isFinite(ai.category)) {
      if (ai.path) {
        return {
          category: ai.category, path: ai.path,
          source: 'ai', confidence: ai.confidence, decidedBy: ai.decidedBy,
        };
      }
      return {
        category: null, path: null,
        source: 'ai_path_missing',
        aiCategory: ai.category,
      };
    }
  }
  // 6. 解決できず
  return { category: null, path: null, source: 'unresolved' };
}
