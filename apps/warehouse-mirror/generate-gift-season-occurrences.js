#!/usr/bin/env node
/**
 * generate-gift-season-occurrences.js — LINEギフト v1.0 Commit 4
 *   mart_gift_seasons の rule_json を年単位で展開し mart_gift_season_occurrences に投入
 *
 * 配置場所: apps/warehouse-mirror/  (Render warehouse-mirror.db に書く)
 *
 * 設計書: g:/共有ドライブ/AI_reference/システム設計/LINEギフトPhase1設計書_v1.0_20260527.md §3 / §6
 * Codex 議論 (v0.9 由来): Round 2 Critical (可変祝日 nth_weekday の年別展開)
 *
 * 排他制御: build-lock.js の acquireLock (LOCK_NAME='generate-gift-season-occurrences')
 *
 * rule_type の挙動:
 *   FIXED_RANGE  : {"start_mmdd":"12-01","end_mmdd":"12-25"}
 *   NTH_WEEKDAY  : {"month":5,"nth":2,"weekday":0,"pre_days":14,"post_days":0}
 *   CUSTOM       : {"start_mmdd":"12-26","end_mmdd":"01-03","year_anchor":"end"}
 *
 * 使い方:
 *   node apps/warehouse-mirror/generate-gift-season-occurrences.js                          当年 + 翌年
 *   node apps/warehouse-mirror/generate-gift-season-occurrences.js --years=2025,2026,2027   特定年
 *   node apps/warehouse-mirror/generate-gift-season-occurrences.js --dry-run                生成のみ、DB 書込なし
 */
import 'dotenv/config';
import { initMirrorDB, getMirrorDB } from './db.js';
import { acquireLock, releaseLock, updateHeartbeat, nowJstIso } from './build-lock.js';

const LOCK_NAME = 'generate-gift-season-occurrences';

function jstWallclock() {
  return new Date(Date.now() + 9 * 60 * 60 * 1000);
}

function yyyymmdd(d) {
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')}`;
}

/**
 * 指定年・月の第 N 曜日 (weekday: 0=Sun ... 6=Sat) を Date オブジェクトで返す
 * 月内に収まらない場合 (5月第5月曜が無い等) はエラー
 */
function nthWeekdayOfMonth(year, month1to12, weekday0Sun, nth) {
  if (!Number.isInteger(month1to12) || month1to12 < 1 || month1to12 > 12) {
    throw new Error(`invalid month: ${month1to12}`);
  }
  if (!Number.isInteger(weekday0Sun) || weekday0Sun < 0 || weekday0Sun > 6) {
    throw new Error(`invalid weekday: ${weekday0Sun}`);
  }
  if (!Number.isInteger(nth) || nth < 1 || nth > 5) {
    throw new Error(`invalid nth: ${nth}`);
  }
  const first = new Date(Date.UTC(year, month1to12 - 1, 1));
  const firstDow = first.getUTCDay();
  const delta = (weekday0Sun - firstDow + 7) % 7;
  const day = 1 + delta + (nth - 1) * 7;
  const result = new Date(Date.UTC(year, month1to12 - 1, day));
  if (result.getUTCMonth() !== month1to12 - 1) {
    throw new Error(`nth=${nth} weekday=${weekday0Sun} not in year=${year} month=${month1to12}`);
  }
  return result;
}

function addDays(date, days) {
  return new Date(date.getTime() + days * 24 * 3600 * 1000);
}

function parseMmDd(mmdd) {
  const parts = String(mmdd || '').split('-');
  if (parts.length !== 2) throw new Error(`invalid mmdd: '${mmdd}'`);
  const m = Number(parts[0]);
  const d = Number(parts[1]);
  if (!Number.isInteger(m) || m < 1 || m > 12) throw new Error(`invalid mmdd month: '${mmdd}'`);
  if (!Number.isInteger(d) || d < 1 || d > 31) throw new Error(`invalid mmdd day: '${mmdd}'`);
  return { m, d };
}

function computeOccurrence(season, year) {
  const rule = JSON.parse(season.rule_json);

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
    const startBeforeEnd = (s.m * 100 + s.d) <= (e.m * 100 + e.d);
    if (startBeforeEnd) {
      return {
        start_date_jst: `${year}-${String(s.m).padStart(2,'0')}-${String(s.d).padStart(2,'0')}`,
        end_date_jst:   `${year}-${String(e.m).padStart(2,'0')}-${String(e.d).padStart(2,'0')}`,
        anchor_date_jst: null,
        season_year: year,
      };
    }
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
    FROM mart_gift_season_occurrences
    WHERE season_code = ? AND season_year = ?
  `).get(season.season_code, occ.season_year);

  const same = existing
    && existing.start_date_jst === occ.start_date_jst
    && existing.end_date_jst === occ.end_date_jst
    && (existing.anchor_date_jst ?? null) === (occ.anchor_date_jst ?? null);
  if (same) return { action: 'unchanged' };

  if (existing) {
    db.prepare(`
      UPDATE mart_gift_season_occurrences
      SET start_date_jst=?, end_date_jst=?, anchor_date_jst=?, updated_by='system:generate', synced_at=?
      WHERE id=?
    `).run(occ.start_date_jst, occ.end_date_jst, occ.anchor_date_jst, syncedAt, existing.id);
    return { action: 'updated' };
  }
  db.prepare(`
    INSERT INTO mart_gift_season_occurrences
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
    const curYear = jstWallclock().getUTCFullYear();
    targetYears = [curYear, curYear + 1];
  }

  console.log(`[generate-gift-season-occurrences] start (dryRun=${dryRun}, years=${targetYears.join(',')})`);

  initMirrorDB();
  const db = getMirrorDB();

  const seasons = db.prepare(`SELECT season_code, season_name, rule_type, rule_json FROM mart_gift_seasons WHERE is_active=1 AND valid_to IS NULL`).all();
  console.log(`  mart_gift_seasons: ${seasons.length} 件`);

  if (dryRun) {
    for (const season of seasons) {
      for (const year of targetYears) {
        try {
          const occ = computeOccurrence(season, year);
          console.log(`  [dry] ${season.season_code} year=${year} start=${occ.start_date_jst} end=${occ.end_date_jst} anchor=${occ.anchor_date_jst ?? '-'}`);
        } catch (e) {
          console.warn(`  [skip] ${season.season_code} ${year}: ${e.message}`);
        }
      }
    }
    console.log('[generate-gift-season-occurrences] dry-run end');
    return;
  }

  // 排他 (Codex Round 2 Critical 2 整合)
  const lock = acquireLock(db, LOCK_NAME, { acquired_by: `pid:${process.pid}@generate-season`, lease_ttl_seconds: 120 });
  if (!lock) {
    console.error(`[generate-gift-season-occurrences] CONFLICT (409): lock ${LOCK_NAME} is currently held`);
    process.exit(2);
  }

  const heartbeat = setInterval(() => {
    try { updateHeartbeat(db, LOCK_NAME); } catch (_) {}
  }, 30_000);

  const syncedAt = nowJstIso();
  const counters = { inserted: 0, updated: 0, unchanged: 0, skipped_invalid: 0 };

  try {
    db.transaction(() => {
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
          const r = upsertOccurrence(db, occ, season, syncedAt);
          counters[r.action] = (counters[r.action] ?? 0) + 1;
        }
      }
    })();

    clearInterval(heartbeat);
    releaseLock(db, LOCK_NAME, 'COMPLETED');
    console.log('[generate-gift-season-occurrences] done (COMPLETED)');
    console.log(`  counters=${JSON.stringify(counters)}`);
  } catch (e) {
    clearInterval(heartbeat);
    releaseLock(db, LOCK_NAME, 'FAILED', e?.message || String(e));
    console.error('[generate-gift-season-occurrences] FAILED:', e);
    process.exit(1);
  }
}

main().catch((e) => {
  console.error('[generate-gift-season-occurrences] FATAL:', e);
  process.exit(1);
});
