#!/usr/bin/env node
/**
 * generate-gift-season-occurrences.js — LINEギフト Phase 1 A-5d
 *   m_gift_seasons の rule_json を年単位で展開し d_gift_season_occurrences に投入
 *
 * 設計書: g:/共有ドライブ/AI_reference/システム設計/LINEギフトPhase1設計書_v0.9_20260526.md §13-3
 * Codex 議論: Round 2 Critical (可変祝日 nth_weekday の年別展開)
 *
 * rule_type の挙動:
 *   FIXED_RANGE  : {"start_mmdd":"12-01","end_mmdd":"12-25"}
 *                  start_date_jst = YEAR + start_mmdd、end_date_jst = YEAR + end_mmdd
 *                  anchor_date_jst = NULL
 *
 *   NTH_WEEKDAY  : {"month":5,"nth":2,"weekday":0,"pre_days":14,"post_days":0}
 *                  anchor = 母の日当日 = 5月第2日曜
 *                  start_date_jst = anchor - pre_days、end_date_jst = anchor + post_days
 *                  anchor_date_jst = anchor (YYYY-MM-DD)
 *
 *   CUSTOM       : {"start_mmdd":"12-26","end_mmdd":"01-03","year_anchor":"end"}
 *                  start < end の場合は通常範囲
 *                  start > end の場合は年跨ぎ
 *                    year_anchor='start': start_date_jst = YEAR + start_mmdd、end_date_jst = (YEAR+1) + end_mmdd、season_year = YEAR
 *                    year_anchor='end'  : start_date_jst = (YEAR-1) + start_mmdd、end_date_jst = YEAR + end_mmdd、season_year = YEAR
 *
 * 使い方:
 *   node apps/warehouse/generate-gift-season-occurrences.js                          当年 + 翌年
 *   node apps/warehouse/generate-gift-season-occurrences.js --years=2024,2025,2026   特定年
 *   node apps/warehouse/generate-gift-season-occurrences.js --dry-run                生成のみ、DB 書込なし
 */
import 'dotenv/config';
import fs from 'node:fs';
import path from 'node:path';
import { initDB, getDB } from './db.js';

const LOCK_FILE = path.resolve('data', 'generate-gift-season-occurrences.lock');

function jstWallclock() {
  return new Date(Date.now() + 9 * 60 * 60 * 1000);
}

function nowJstIso() {
  const j = jstWallclock();
  const yy = j.getUTCFullYear();
  const mm = String(j.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(j.getUTCDate()).padStart(2, '0');
  const hh = String(j.getUTCHours()).padStart(2, '0');
  const mi = String(j.getUTCMinutes()).padStart(2, '0');
  const ss = String(j.getUTCSeconds()).padStart(2, '0');
  return `${yy}-${mm}-${dd}T${hh}:${mi}:${ss}+09:00`;
}

let lockOwned = false;

function acquireLock() {
  try {
    fs.mkdirSync(path.dirname(LOCK_FILE), { recursive: true });
    const fd = fs.openSync(LOCK_FILE, 'wx');
    fs.writeFileSync(fd, JSON.stringify({ pid: process.pid, started_at: nowJstIso() }), 'utf8');
    fs.closeSync(fd);
    lockOwned = true;
    return true;
  } catch (e) {
    if (e.code === 'EEXIST') {
      const content = fs.existsSync(LOCK_FILE) ? fs.readFileSync(LOCK_FILE, 'utf8') : '?';
      console.error(`[generate-gift-season-occurrences] FATAL: lock file exists (${LOCK_FILE}): ${content}`);
      return false;
    }
    throw e;
  }
}

function releaseLock() {
  if (!lockOwned) return;
  try { fs.unlinkSync(LOCK_FILE); } catch (_) {}
  lockOwned = false;
}

function yyyymmdd(d) {
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')}`;
}

/**
 * 指定年・月の第 N 曜日 (weekday: 0=Sun ... 6=Sat) を Date オブジェクトで返す
 * 全て UTC ベースで計算 (JST 壁時計と同じ扱い)
 * Codex Round 1 Medium 反映: month/nth/weekday の範囲検証 + 結果が同月内に収まるかチェック
 */
function nthWeekdayOfMonth(year, month1to12, weekday0Sun, nth) {
  if (!Number.isInteger(month1to12) || month1to12 < 1 || month1to12 > 12) {
    throw new Error(`invalid month: ${month1to12} (must be 1-12)`);
  }
  if (!Number.isInteger(weekday0Sun) || weekday0Sun < 0 || weekday0Sun > 6) {
    throw new Error(`invalid weekday: ${weekday0Sun} (must be 0-6, 0=Sun)`);
  }
  if (!Number.isInteger(nth) || nth < 1 || nth > 5) {
    throw new Error(`invalid nth: ${nth} (must be 1-5)`);
  }
  const first = new Date(Date.UTC(year, month1to12 - 1, 1));
  const firstDow = first.getUTCDay();
  const delta = (weekday0Sun - firstDow + 7) % 7;
  const day = 1 + delta + (nth - 1) * 7;
  const result = new Date(Date.UTC(year, month1to12 - 1, day));
  // 「5月の第5月曜」のように月内に存在しないケースを検知 (月跨ぎ防止)
  if (result.getUTCMonth() !== month1to12 - 1) {
    throw new Error(`nth=${nth} weekday=${weekday0Sun} does not exist in year=${year} month=${month1to12}`);
  }
  return result;
}

function addDays(date, days) {
  return new Date(date.getTime() + days * 24 * 3600 * 1000);
}

function parseMmDd(mmdd) {
  const parts = String(mmdd || '').split('-');
  if (parts.length !== 2) throw new Error(`invalid mmdd: '${mmdd}' (expected 'MM-DD')`);
  const m = Number(parts[0]);
  const d = Number(parts[1]);
  // Codex Round 1 Medium 反映: 範囲検証 (1-12 / 1-31)
  if (!Number.isInteger(m) || m < 1 || m > 12) throw new Error(`invalid mmdd month: '${mmdd}' (m=${m})`);
  if (!Number.isInteger(d) || d < 1 || d > 31) throw new Error(`invalid mmdd day: '${mmdd}' (d=${d})`);
  return { m, d };
}

/**
 * 1 シーン × 1 年で occurrence を計算
 * @returns { start_date_jst, end_date_jst, anchor_date_jst, season_year } | null
 */
function computeOccurrence(season, year) {
  let rule;
  try { rule = JSON.parse(season.rule_json); } catch (e) { throw new Error(`invalid rule_json for ${season.season_code}: ${e.message}`); }

  if (season.rule_type === 'FIXED_RANGE') {
    const s = parseMmDd(rule.start_mmdd);
    const e = parseMmDd(rule.end_mmdd);
    return {
      start_date_jst: `${year}-${String(s.m).padStart(2,'0')}-${String(s.d).padStart(2,'0')}`,
      end_date_jst:   `${year}-${String(e.m).padStart(2,'0')}-${String(e.d).padStart(2,'0')}`,
      anchor_date_jst: null,
      season_year: year,
    };
  }

  if (season.rule_type === 'NTH_WEEKDAY') {
    const anchor = nthWeekdayOfMonth(year, rule.month, rule.weekday, rule.nth);
    const start = addDays(anchor, -(rule.pre_days || 0));
    const end = addDays(anchor, rule.post_days || 0);
    return {
      start_date_jst: yyyymmdd(start),
      end_date_jst: yyyymmdd(end),
      anchor_date_jst: yyyymmdd(anchor),
      season_year: year,
    };
  }

  if (season.rule_type === 'CUSTOM') {
    const s = parseMmDd(rule.start_mmdd);
    const e = parseMmDd(rule.end_mmdd);
    const yearAnchor = rule.year_anchor || 'start';
    // 年跨ぎ判定
    const startBeforeEnd = (s.m * 100 + s.d) <= (e.m * 100 + e.d);
    if (startBeforeEnd) {
      return {
        start_date_jst: `${year}-${String(s.m).padStart(2,'0')}-${String(s.d).padStart(2,'0')}`,
        end_date_jst:   `${year}-${String(e.m).padStart(2,'0')}-${String(e.d).padStart(2,'0')}`,
        anchor_date_jst: null,
        season_year: year,
      };
    }
    // 年跨ぎ (start > end)
    if (yearAnchor === 'end') {
      return {
        start_date_jst: `${year - 1}-${String(s.m).padStart(2,'0')}-${String(s.d).padStart(2,'0')}`,
        end_date_jst:   `${year}-${String(e.m).padStart(2,'0')}-${String(e.d).padStart(2,'0')}`,
        anchor_date_jst: null,
        season_year: year,
      };
    }
    return {
      start_date_jst: `${year}-${String(s.m).padStart(2,'0')}-${String(s.d).padStart(2,'0')}`,
      end_date_jst:   `${year + 1}-${String(e.m).padStart(2,'0')}-${String(e.d).padStart(2,'0')}`,
      anchor_date_jst: null,
      season_year: year,
    };
  }

  throw new Error(`unknown rule_type: ${season.rule_type}`);
}

function upsertOccurrence(db, occ, season, syncedAt) {
  const existing = db.prepare(`
    SELECT id, start_date_jst, end_date_jst, anchor_date_jst
    FROM d_gift_season_occurrences
    WHERE season_code = ? AND season_year = ?
  `).get(season.season_code, occ.season_year);

  const same = existing
    && existing.start_date_jst === occ.start_date_jst
    && existing.end_date_jst === occ.end_date_jst
    && (existing.anchor_date_jst ?? null) === (occ.anchor_date_jst ?? null);
  if (same) return { action: 'unchanged' };

  if (existing) {
    db.prepare(`
      UPDATE d_gift_season_occurrences
      SET start_date_jst=?, end_date_jst=?, anchor_date_jst=?, updated_by='system:generate', synced_at=?
      WHERE id=?
    `).run(occ.start_date_jst, occ.end_date_jst, occ.anchor_date_jst, syncedAt, existing.id);
    return { action: 'updated' };
  }
  db.prepare(`
    INSERT INTO d_gift_season_occurrences
      (season_code, season_year, start_date_jst, end_date_jst, anchor_date_jst,
       is_active, created_by, updated_by, synced_at)
    VALUES (?, ?, ?, ?, ?, 1, 'system:generate', 'system:generate', ?)
  `).run(season.season_code, occ.season_year, occ.start_date_jst, occ.end_date_jst, occ.anchor_date_jst, syncedAt);
  return { action: 'inserted' };
}

async function main() {
  const args = process.argv.slice(2);
  const dryRun = args.includes('--dry-run');
  const yearsArg = args.find((a) => a.startsWith('--years='));
  let targetYears;
  if (yearsArg) {
    targetYears = yearsArg.split('=')[1].split(',').map((s) => Number(s.trim())).filter(Boolean);
  } else {
    const now = jstWallclock();
    const curYear = now.getUTCFullYear();
    targetYears = [curYear, curYear + 1];
  }

  if (!dryRun && !acquireLock()) {
    process.exit(2);
  }

  console.log(`[generate-gift-season-occurrences] start (dryRun=${dryRun}, years=${targetYears.join(',')})`);

  initDB();
  const db = getDB();

  const seasons = db.prepare(`SELECT season_code, season_name, rule_type, rule_json FROM m_gift_seasons WHERE is_active=1 AND valid_to IS NULL`).all();
  console.log(`  m_gift_seasons: ${seasons.length} 件`);

  const syncedAt = nowJstIso();
  const counters = { inserted: 0, updated: 0, unchanged: 0, skipped_invalid: 0 };

  for (const season of seasons) {
    for (const year of targetYears) {
      let occ;
      try {
        occ = computeOccurrence(season, year);
      } catch (e) {
        console.warn(`  [skip] ${season.season_code} ${year}: ${e.message}`);
        counters.skipped_invalid += 1;
        continue;
      }
      if (dryRun) {
        console.log(`  [dry] ${season.season_code} year=${year} start=${occ.start_date_jst} end=${occ.end_date_jst} anchor=${occ.anchor_date_jst ?? '-'}`);
        continue;
      }
      const r = upsertOccurrence(db, occ, season, syncedAt);
      counters[r.action] = (counters[r.action] ?? 0) + 1;
    }
  }

  console.log('[generate-gift-season-occurrences] done');
  console.log(`  counters=${JSON.stringify(counters)}`);
}

main().catch((e) => {
  console.error('[generate-gift-season-occurrences] FATAL:', e);
  process.exit(1);
}).finally(() => {
  releaseLock();
});
