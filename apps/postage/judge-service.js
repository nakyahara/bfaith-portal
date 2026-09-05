/**
 * 伝票番号のリストを受けて判定し、判定ログを残す (PR1b の芯)。
 *
 * 呼び出し元 = 伝票出しPCの shipping-upload-launcher。okurijo_teikeigai_*.csv を alldata.csv (P-touch の
 * 差し込み元) に連結する瞬間に、1列目「伝票番号」を全部送ってくる。ここは伝票ごとに
 *   郵便印字 (シールに刷る文言) / 区分 / 料金 / 判定状態 / 推奨資材 / 判定ID
 * を返す。ランチャーはそれを alldata.csv の右端に足すだけ。
 *
 * 設計の芯 (要件定義 §6):
 *   - 確定できないものは「不明 (理由)」と刷る。正しそうな誤印字より不明が安全
 *   - 印字した文言・金額は判定ログに固定保存 (料金表が変わっても紙に何を出したか追える)
 *   - 再印字は別レコード (同じ伝票を2回送れば2行)。判定IDで紙とDBを再接続する
 */
import crypto from 'crypto';
import { judge, UNKNOWN_REASONS } from './engine.js';
import { getDB, jstToday } from './db.js';
import { buildContext } from './coverage.js';
import {
  readCompositions, normalizeSlipNo, mirrorAvailable, COMPOSITION_SOURCE, TEIKEIGAI_METHOD_CODE, KNOWN_OTHER_METHOD_CODES,
} from './composition.js';

export const MAX_SLIPS_PER_CALL = 500;   // 1日の定形外は多くて 90 通。桁違いは呼び方の誤り
// 判定日として受ける範囲。過去は再印字・検証用に 400 日、未来は翌月の料金改定を試す程度
const DATE_PAST_DAYS = 400;
const DATE_FUTURE_DAYS = 45;

export class JudgeInputError extends Error {}

export const STATUS_LABELS = { confirmed: '確定', unknown: '不明', skipped: '対象外' };

/**
 * シールに刷る文言。
 *   確定   → 「定形 50g以内 110円」
 *   不明   → 「不明 (商品の重さ未登録)」  ← 理由まで刷る。現場が何を測ればいいか分かる
 *   対象外 → 空 (レターパック等。何も刷らない)
 */
export function printTextOf(r) {
  if (r.status === 'confirmed') return `${r.displayName} ${r.amountYen}円`;
  if (r.status === 'skipped') return '';
  return `不明 (${r.reasonLabel || UNKNOWN_REASONS[r.reason] || r.reason})`;
}

// 読み間違えやすい文字 (0/O, 1/I/L) を抜いた 30 文字。4 桁で 81 万通り、1 日 100 通なら衝突はまず無い。
// 衝突しても PK で弾いて引き直す
const ID_ALPHABET = '23456789ABCDEFGHJKMNPQRSTUVWXYZ';
export function newDecisionId(dateYmd) {
  const ymd = String(dateYmd).replace(/-/g, '').slice(2);   // 2026-09-05 → 260905
  const bytes = crypto.randomBytes(4);
  let s = '';
  for (let i = 0; i < 4; i++) s += ID_ALPHABET[bytes[i] % ID_ALPHABET.length];
  return `${ymd}-${s}`;
}

function validateDate(v) {
  if (v === undefined || v === null || v === '') return jstToday();
  // 2026-02-31 は Date.parse が 3/3 に丸めて通してしまう → 往復して同じ文字列になる日付だけ受ける
  if (typeof v !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(v)) throw new JudgeInputError('date は YYYY-MM-DD で指定してください');
  const t = Date.parse(`${v}T00:00:00Z`);
  if (!Number.isFinite(t) || new Date(t).toISOString().slice(0, 10) !== v) {
    throw new JudgeInputError(`date が実在しない日付です: ${v}`);
  }
  const today = Date.parse(`${jstToday()}T00:00:00Z`);
  const diffDays = (t - today) / 86400000;
  if (diffDays < -DATE_PAST_DAYS || diffDays > DATE_FUTURE_DAYS) {
    throw new JudgeInputError(`date は今日から ${DATE_PAST_DAYS} 日前〜${DATE_FUTURE_DAYS} 日後の範囲で指定してください: ${v}`);
  }
  return v;
}

function validateSlipNos(v) {
  if (!Array.isArray(v) || !v.length) throw new JudgeInputError('slip_nos (伝票番号の配列) が空です');
  if (v.length > MAX_SLIPS_PER_CALL) throw new JudgeInputError(`一度に送れるのは ${MAX_SLIPS_PER_CALL} 件までです (${v.length} 件)`);
  const out = [];
  for (const raw of v) {
    if (typeof raw !== 'string' && typeof raw !== 'number') throw new JudgeInputError('slip_nos は文字列の配列で送ってください');
    const s = normalizeSlipNo(raw);
    if (!s || s.length > 40) throw new JudgeInputError(`伝票番号が不正です: ${JSON.stringify(raw)}`);
    out.push(s);
  }
  return out;
}

function shortStr(v, max) {
  if (v === undefined || v === null) return null;
  if (typeof v !== 'string') throw new JudgeInputError('source / batch_ref は文字列で送ってください');
  const t = v.normalize('NFKC').trim();
  return t ? t.slice(0, max) : null;
}

/**
 * @param {{slip_nos: string[], date?: string, source?: string, batch_ref?: string, actor?: string}} input
 * @returns {{date, tariff, composition_available, results, summary}}
 */
export function judgeBatch(input = {}) {
  const slipNos = validateSlipNos(input.slip_nos);
  const date = validateDate(input.date);
  const source = shortStr(input.source, 40) || 'api';
  const batchRef = shortStr(input.batch_ref, 80);
  const actor = shortStr(input.actor, 120);

  const ctx = buildContext(date);
  // 構成が読めないときは API を落とさず「全件不明」で返す (印字自体は止めない。紙には「不明」が出る)
  let compositionAvailable = mirrorAvailable();
  let comps = new Map();
  let readError = null;
  if (compositionAvailable) {
    try { comps = readCompositions(slipNos); }
    catch (e) { compositionAvailable = false; readError = e; console.error('[postage judge] packing-dispatch の読み取りに失敗', e); }
  }

  const results = slipNos.map((slipNo) => {
    const comp = comps.get(slipNo) || null;
    let r;
    if (!comp) {
      r = {
        status: 'unknown', reason: 'missing_composition', reasonLabel: UNKNOWN_REASONS.missing_composition,
        detail: compositionAvailable ? 'packing-dispatch に出力の記録が無い'
          : (readError ? 'packing-dispatch のデータが読めません (読み取りエラー)' : 'packing-dispatch のデータが読めません'),
      };
    } else if (comp.method_code === TEIKEIGAI_METHOD_CODE) {
      r = comp.broken
        ? { status: 'unknown', reason: 'broken_composition', reasonLabel: UNKNOWN_REASONS.broken_composition, detail: null }
        : judge({ lines: comp.lines }, ctx);
    } else if (KNOWN_OTHER_METHOD_CODES.has(comp.method_code)) {
      // レターパック等、登録済みの別配送方法。定形外の判定を当てはめない (何も刷らない)
      r = { status: 'skipped', reason: 'not_teikeigai', reasonLabel: UNKNOWN_REASONS.not_teikeigai, detail: comp.method_code };
    } else {
      // 空・NULL・未知のコード。「対象外」にすると定形外の伝票が黙って空印字になるので不明に落とす
      r = { status: 'unknown', reason: 'unknown_method', reasonLabel: UNKNOWN_REASONS.unknown_method, detail: comp.method_code || '(空)' };
    }
    const printText = printTextOf(r);
    return {
      slip_no: slipNo,
      status: r.status,
      status_label: STATUS_LABELS[r.status],
      print_text: printText,
      band_name: r.status === 'confirmed' ? r.displayName : null,
      band_code: r.status === 'confirmed' ? r.bandCode : null,
      mail_type: r.status === 'confirmed' ? r.mailType : null,
      amount_yen: r.status === 'confirmed' ? r.amountYen : null,
      material_code: r.materialCode || null,
      material_name: r.status === 'confirmed' ? r.materialName : (r.materialCode ? (ctx.materials.get(r.materialCode)?.display_name || r.materialCode) : null),
      weight_g: Number.isFinite(r.weightG) ? r.weightG : null,
      thickness_mm: Number.isFinite(r.thicknessMm) ? r.thicknessMm : null,
      reason: r.status === 'confirmed' ? null : r.reason,
      reason_label: r.status === 'confirmed' ? null : (r.reasonLabel || null),
      detail: r.status === 'confirmed' ? null : (r.detail || null),
      method_code: comp ? (comp.method_code || null) : null,
      lines: comp && !comp.broken ? comp.lines.map((l) => ({ sku_code: l.sku_code, qty: l.qty })) : [],
      decision_id: null,   // 保存時に採番
    };
  });

  // ── 判定ログ。1回の呼び出しは全部入るか全部入らないか ──
  const db = getDB();
  const ins = db.prepare(`INSERT INTO pm_print_decisions
    (decision_id, slip_no, judge_date, source, batch_ref, status, reason, detail,
     mail_type, band_code, band_name, amount_yen, material_code, material_name, weight_g, thickness_mm,
     tariff_version_id, method_code, composition_source, lines_json, print_text, requested_by)
    VALUES (@decision_id, @slip_no, @judge_date, @source, @batch_ref, @status, @reason, @detail,
            @mail_type, @band_code, @band_name, @amount_yen, @material_code, @material_name, @weight_g, @thickness_mm,
            @tariff_version_id, @method_code, @composition_source, @lines_json, @print_text, @requested_by)`);
  db.transaction(() => {
    for (const row of results) {
      // 採番は衝突したら引き直す (4桁×30文字なので実際にはまず起きない)
      for (let attempt = 0; ; attempt++) {
        const id = newDecisionId(date);
        try {
          ins.run({
            decision_id: id, slip_no: row.slip_no, judge_date: date, source, batch_ref: batchRef,
            status: row.status, reason: row.reason, detail: row.detail,
            mail_type: row.mail_type, band_code: row.band_code, band_name: row.band_name, amount_yen: row.amount_yen,
            material_code: row.material_code, material_name: row.material_name,
            weight_g: row.weight_g, thickness_mm: row.thickness_mm,
            tariff_version_id: ctx.tariff?.tariff_version_id ?? null,
            method_code: row.method_code,
            composition_source: comps.has(row.slip_no) ? COMPOSITION_SOURCE : null,
            lines_json: JSON.stringify(row.lines),
            print_text: row.print_text,
            requested_by: actor,
          });
          row.decision_id = id;
          break;
        } catch (e) {
          if (attempt < 10 && /UNIQUE constraint failed|PRIMARY KEY/i.test(String(e.message))) continue;
          throw e;
        }
      }
    }
  })();

  const summary = { total: results.length, confirmed: 0, unknown: 0, skipped: 0, not_found: 0 };
  for (const r of results) {
    summary[r.status]++;
    if (r.reason === 'missing_composition') summary.not_found++;
  }
  return {
    date,
    tariff: ctx.tariff ? { id: ctx.tariff.tariff_version_id, name: ctx.tariff.name } : null,
    composition_available: compositionAvailable,
    results: results.map((r) => { const { lines, ...rest } = r; return rest; }),
    summary,
  };
}

/** 判定ログの一覧 (画面用)。不明 → 対象外 → 確定 の順に、同じ状態は新しい順。 */
export function listDecisions({ date, limit = 500 } = {}) {
  const d = date || jstToday();
  const db = getDB();
  const rows = db.prepare(`
    SELECT * FROM pm_print_decisions WHERE judge_date = ?
     ORDER BY CASE status WHEN 'unknown' THEN 0 WHEN 'skipped' THEN 1 ELSE 2 END, judged_at DESC, slip_no
     LIMIT ?
  `).all(d, Math.min(Math.max(Number(limit) || 500, 1), 2000));
  const summary = db.prepare(`
    SELECT COUNT(*) total,
           SUM(CASE WHEN status='confirmed' THEN 1 ELSE 0 END) confirmed,
           SUM(CASE WHEN status='unknown'   THEN 1 ELSE 0 END) unknown,
           SUM(CASE WHEN status='skipped'   THEN 1 ELSE 0 END) skipped,
           COALESCE(SUM(CASE WHEN status='confirmed' THEN amount_yen ELSE 0 END), 0) amount
      FROM pm_print_decisions WHERE judge_date = ?`).get(d);
  const reasons = db.prepare(`
    SELECT reason, COUNT(*) n FROM pm_print_decisions
     WHERE judge_date = ? AND status = 'unknown' GROUP BY reason ORDER BY n DESC`).all(d)
    .map((r) => ({ reason: r.reason, label: UNKNOWN_REASONS[r.reason] || r.reason, count: r.n }));
  const dates = db.prepare(`SELECT judge_date d, COUNT(*) n FROM pm_print_decisions GROUP BY judge_date ORDER BY d DESC LIMIT 30`).all();
  return { date: d, rows, summary, reasons, dates };
}
