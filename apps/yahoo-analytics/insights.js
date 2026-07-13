/**
 * yahoo-analytics insights.js — 診断ルールエンジン + AI異常検知の土台
 *
 * 設計 (Codex設計相談 2R で確定):
 *   - findings は実行時に SQL で計算し、occurrence (rule_key+target_key で一意) に UPSERT
 *   - 履歴は遷移イベントのみ薄く append (detected/resolved/reopened/status_changed) — 毎run書かない
 *   - dismissed は再発しても維持 (恒久無視 = ロスリーダー等)。severity が昇格した時のみ open に戻す
 *   - 実行は手動ボタン (将来 cron)。多重実行は BEGIN IMMEDIATE 相当のフラグでロック
 *   - AI契約: listInsights() の JSON が将来の自動修正指示エージェントの入力。
 *     evidence には期間・実測値・閾値・基準値・観測時刻を必ず含める (再現可能性)。
 *     suggested_action は構造化 {action_type, label, parameters, requires_approval}。
 *     AIは「提案」まで。実行は将来の action_queue (別PR) に登録する契約
 */
import { getMirrorDB } from '../warehouse-mirror/db.js';
import { jstToday, addDays, getSettings, validationError } from './queries.js';

export const RULES_VERSION = 1;
export const EVIDENCE_SCHEMA_VERSION = 1;
const SEV_RANK = { info: 0, warn: 1, high: 2 };

let _init = false;
export function ensureInsightTables() {
  if (_init) return;
  const db = getMirrorDB();
  db.exec(`CREATE TABLE IF NOT EXISTS yadash_insight_occurrences (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_key          TEXT NOT NULL,
    target_type       TEXT NOT NULL CHECK (target_type IN ('sku','keyword','store')),
    target_key        TEXT NOT NULL,
    severity          TEXT NOT NULL CHECK (severity IN ('info','warn','high')),
    title             TEXT NOT NULL,
    evidence_json     TEXT NOT NULL,
    suggested_json    TEXT NOT NULL,
    first_detected_at TEXT NOT NULL,
    last_detected_at  TEXT NOT NULL,
    last_run_id       INTEGER,
    resolved_at       TEXT,
    status            TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open','acked','dismissed','done')),
    status_memo       TEXT NOT NULL DEFAULT '',
    status_updated_at TEXT,
    UNIQUE (rule_key, target_key)
  )`);
  db.exec(`CREATE TABLE IF NOT EXISTS yadash_insight_events (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    occurrence_id INTEGER NOT NULL,
    event_type    TEXT NOT NULL CHECK (event_type IN ('detected','resolved','reopened','status_changed')),
    at            TEXT NOT NULL,
    detail_json   TEXT NOT NULL DEFAULT '{}'
  )`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_yie_occ ON yadash_insight_events(occurrence_id, at)`);
  db.exec(`CREATE TABLE IF NOT EXISTS yadash_insight_runs (
    run_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    trigger     TEXT NOT NULL CHECK (trigger IN ('manual','cron','sync')),
    status      TEXT NOT NULL CHECK (status IN ('running','succeeded','failed')),
    rules_version INTEGER NOT NULL,
    started_at  TEXT NOT NULL,
    finished_at TEXT,
    found_count INTEGER,
    resolved_count INTEGER,
    error_text  TEXT
  )`);
  _init = true;
}

// ─── ルール定義 (v1: 6本)。各ルールは findings 配列を返す ───
// finding = { rule_key, target_type, target_key, severity, title, evidence, suggested }
function act(action_type, label, parameters = {}) {
  return { action_type, label, parameters, requires_approval: true, risk_level: 'low' };
}

const RULES = [
  {
    key: 'loss_sku',
    label: '赤字SKU (粗利L1マイナス)',
    run(db, s, today) {
      const from = addDays(today, -29);
      return db.prepare(`
        SELECT yahoo_sku_key, MAX(product_name) AS name,
          SUM(gross_sales_jpy_incl) AS sales, SUM(variable_margin_partial_jpy_incl) AS vm,
          SUM(units_net_sold) AS units, MAX(unresolved_sku_flag) AS unres
        FROM mirror_yahoo_finance_sku_daily WHERE date_jst >= ?
        GROUP BY yahoo_sku_key HAVING vm < 0 AND sales >= 5000 AND unres = 0
      `).all(from).map(r => ({
        rule_key: 'loss_sku', target_type: 'sku', target_key: r.yahoo_sku_key,
        severity: r.vm < -10000 ? 'high' : 'warn',
        title: `赤字SKU: ${r.name || r.yahoo_sku_key} (直近30日 粗利 ¥${Math.round(r.vm).toLocaleString()})`,
        evidence: { period: { from, to: today }, sales_incl: Math.round(r.sales), variable_margin: Math.round(r.vm), units_net: r.units, threshold: 'vm<0 AND sales>=5000' },
        suggested: act('review_pricing', '値上げ・クーポン/ポイント設定・原価の見直し (継続赤字なら撤退検討)', { sku: r.yahoo_sku_key }),
      }));
    },
  },
  {
    key: 'unresolved_sku',
    label: 'SKU未解決 (原価0円計上)',
    run(db, s, today) {
      const from = addDays(today, -29);
      return db.prepare(`
        SELECT yahoo_sku_key, MAX(product_name) AS name, SUM(gross_sales_jpy_incl) AS sales
        FROM mirror_yahoo_finance_sku_daily
        WHERE unresolved_sku_flag = 1 AND date_jst >= ?
        GROUP BY yahoo_sku_key HAVING sales >= 3000
      `).all(from).map(r => ({
        rule_key: 'unresolved_sku', target_type: 'sku', target_key: r.yahoo_sku_key,
        severity: 'warn',
        title: `SKU未解決: ${r.name || r.yahoo_sku_key} (30日売上 ¥${Math.round(r.sales).toLocaleString()} が原価0円計上)`,
        evidence: { period: { from, to: today }, sales_incl: Math.round(r.sales), threshold: 'sales>=3000' },
        suggested: act('register_sku_map', 'f_yahoo_sku_map に NEコードを登録 (利益分析タブの未解決一覧から)', { yahoo_key: r.yahoo_sku_key }),
      }));
    },
  },
  {
    key: 'high_pv_low_cvr',
    label: 'PVがあるのに売れない',
    run(db, s, today) {
      const from = addDays(today, -13);
      // ストア全体の中央値CVRの半分未満 & PV上位
      const rows = db.prepare(`
        SELECT CASE WHEN sub_code <> '' THEN sub_code ELSE item_code END AS sku,
          MAX(item_name) AS name,
          SUM(COALESCE(pv_premium_ship,0) + COALESCE(pv_normal,0)) AS pv,
          SUM(COALESCE(visitors,0)) AS visitors, SUM(COALESCE(buyers,0)) AS buyers,
          SUM(COALESCE(sales_yen,0)) AS sales
        FROM mirror_yahoo_item_daily WHERE date_jst >= ?
        GROUP BY sku HAVING pv >= 200
      `).all(from).map(r => ({ ...r, cvr: r.visitors > 0 ? r.buyers / r.visitors * 100 : 0 }));
      if (rows.length < 5) return [];
      const cvrs = rows.map(r => r.cvr).sort((a, b) => a - b);
      const median = cvrs[Math.floor(cvrs.length / 2)];
      if (median <= 0) return [];
      return rows.filter(r => r.cvr < median * 0.5).map(r => ({
        rule_key: 'high_pv_low_cvr', target_type: 'sku', target_key: r.sku,
        severity: 'warn',
        title: `PVあるのに売れない: ${r.name || r.sku} (14日PV ${r.pv.toLocaleString()} / CVR ${Math.round(r.cvr * 10) / 10}% < 中央値${Math.round(median * 10) / 10}%の半分)`,
        evidence: { period: { from, to: today }, pv: r.pv, visitors: r.visitors, buyers: r.buyers, cvr_pct: Math.round(r.cvr * 10) / 10, baseline_median_cvr_pct: Math.round(median * 10) / 10, threshold: 'cvr < median*0.5 AND pv>=200' },
        suggested: act('review_listing', '商品ページ改善 (価格/画像/レビュー/優良配送)。露出は足りている', { sku: r.sku }),
      }));
    },
  },
  {
    key: 'sales_drop',
    label: '販売急落 (要因分解付き)',
    run(db, s, today) {
      const to = addDays(today, -1), from = addDays(to, -6);
      const pTo = addDays(from, -1), pFrom = addDays(pTo, -6);
      const agg = (f, t) => new Map(db.prepare(`
        SELECT yahoo_sku_key AS k, SUM(units_net_sold) AS u, SUM(gross_sales_jpy_incl) AS sal, MAX(product_name) AS name
        FROM mirror_yahoo_finance_sku_daily WHERE date_jst >= ? AND date_jst <= ? GROUP BY k
      `).all(f, t).map(r => [r.k, r]));
      const trafficAgg = (f, t) => new Map(db.prepare(`
        SELECT CASE WHEN sub_code <> '' THEN sub_code ELSE item_code END AS k,
          SUM(COALESCE(pv_premium_ship,0) + COALESCE(pv_normal,0)) AS pv,
          SUM(COALESCE(visitors,0)) AS v, SUM(COALESCE(buyers,0)) AS b
        FROM mirror_yahoo_item_daily WHERE date_jst >= ? AND date_jst <= ? GROUP BY k
      `).all(f, t).map(r => [r.k, r]));
      const cur = agg(from, to), prev = agg(pFrom, pTo);
      const curT = trafficAgg(from, to), prevT = trafficAgg(pFrom, pTo);
      const out = [];
      for (const [k, p] of prev) {
        if (p.u < s.movers_min_units * 2) continue;
        const c = cur.get(k) || { u: 0, sal: 0, name: p.name };
        const dropPct = (c.u - p.u) / p.u * 100;
        if (dropPct > -50) continue;
        // 要因分解 (親子構造: 子は evidence.factors に格納、occurrence は親のみ)
        const ct = curT.get(k), pt = prevT.get(k);
        const factors = {};
        if (ct && pt && pt.pv > 0) factors.pv_change_pct = Math.round((ct.pv - pt.pv) / pt.pv * 1000) / 10;
        if (ct && pt && pt.v > 0 && ct.v > 0) {
          const cCvr = ct.b / ct.v, pCvr = pt.b / pt.v;
          if (pCvr > 0) factors.cvr_change_pct = Math.round((cCvr - pCvr) / pCvr * 1000) / 10;
        }
        out.push({
          rule_key: 'sales_drop', target_type: 'sku', target_key: k,
          severity: p.sal >= 30000 ? 'high' : 'warn',
          title: `販売急落: ${c.name || k} (週次 ${p.u}→${c.u}個 ${Math.round(dropPct)}%)`,
          evidence: { period: { from, to }, baseline_period: { from: pFrom, to: pTo }, units: c.u, baseline_units: p.u, drop_pct: Math.round(dropPct), factors, threshold: 'units -50%以上 AND 前週>=閾値×2' },
          suggested: act('investigate_drop', factors.pv_change_pct !== undefined && factors.pv_change_pct < -30
            ? '露出低下が主因の疑い: 検索順位/広告/5のつく日設定を確認'
            : '転換低下/在庫切れ/価格変更の確認', { sku: k }),
        });
      }
      return out;
    },
  },
  {
    key: 'kw_inflow_drop',
    label: '検索KW流入の急減',
    run(db, s, today) {
      const to = addDays(today, -1), from = addDays(to, -6);
      const pTo = addDays(from, -1), pFrom = addDays(pTo, -6);
      const agg = (f, t) => new Map(db.prepare(`
        SELECT keyword AS k, SUM(COALESCE(inflow,0)) AS i, SUM(COALESCE(sales_yen,0)) AS sal
        FROM mirror_yahoo_keyword_daily WHERE date_jst >= ? AND date_jst <= ? GROUP BY k
      `).all(f, t).map(r => [r.k, r]));
      const cur = agg(from, to), prev = agg(pFrom, pTo);
      const out = [];
      for (const [k, p] of prev) {
        if (p.i < 50) continue;
        const c = cur.get(k) || { i: 0, sal: 0 };
        const dropPct = (c.i - p.i) / p.i * 100;
        if (dropPct > -50) continue;
        out.push({
          rule_key: 'kw_inflow_drop', target_type: 'keyword', target_key: k,
          severity: p.sal >= 20000 ? 'high' : 'warn',
          title: `KW流入急減: 「${k}」 (週次流入 ${p.i}→${c.i} ${Math.round(dropPct)}%)`,
          evidence: { period: { from, to }, baseline_period: { from: pFrom, to: pTo }, inflow: c.i, baseline_inflow: p.i, drop_pct: Math.round(dropPct), baseline_sales: Math.round(p.sal), threshold: 'inflow -50%以上 AND 前週>=50' },
          suggested: act('check_ranking', '検索順位低下/競合広告/商品名変更の影響を確認', { keyword: k }),
        });
      }
      return out;
    },
  },
  {
    key: 'data_freshness',
    label: 'データ鮮度異常',
    run(db, s, today) {
      const out = [];
      const checks = [
        { key: 'finance', table: 'mirror_yahoo_finance_sku_daily', maxLagDays: 2, label: '利益fact (daily-sync)' },
        { key: 'item_stats', table: 'mirror_yahoo_item_daily', maxLagDays: 3, label: '商品統計 (mall-csv-fetcher)' },
        { key: 'keyword_stats', table: 'mirror_yahoo_keyword_daily', maxLagDays: 3, label: '検索KW統計 (mall-csv-fetcher)' },
      ];
      for (const c of checks) {
        let latest = null;
        try { latest = db.prepare(`SELECT MAX(date_jst) AS d FROM ${c.table}`).get().d; } catch { continue; /* mirror表未作成=未導入とみなす */ }
        const lagLimit = addDays(today, -c.maxLagDays);
        if (!latest || latest < lagLimit) {
          out.push({
            rule_key: 'data_freshness', target_type: 'store', target_key: c.key,
            severity: 'high',
            title: `データ鮮度異常: ${c.label} の最新日が ${latest || '(データなし)'} (許容 ${c.maxLagDays}日遅延を超過)`,
            evidence: { latest_date: latest, max_lag_days: c.maxLagDays, checked_at: today, table: c.table },
            suggested: act('check_pipeline', 'miniPCのfetch-all/daily-syncのGChat通知とログを確認', { source: c.key }),
          });
        }
      }
      return out;
    },
  },
];

export function getRuleLabels() {
  return Object.fromEntries(RULES.map(r => [r.key, r.label]));
}

// ─── 実行 (手動ボタン。多重実行はrunning行の存在でロック、stale 10分で奪取) ───
export function runInsights(trigger = 'manual') {
  ensureInsightTables();
  const db = getMirrorDB();
  const now = () => new Date().toISOString();
  const staleBefore = new Date(Date.now() - 10 * 60 * 1000).toISOString();
  const running = db.prepare(`SELECT run_id, started_at FROM yadash_insight_runs WHERE status = 'running'`).get();
  if (running && running.started_at > staleBefore) throw validationError('診断が実行中です (数秒後に再読み込みしてください)');
  if (running) db.prepare(`UPDATE yadash_insight_runs SET status = 'failed', finished_at = ?, error_text = 'stale-takeover' WHERE run_id = ?`).run(now(), running.run_id);

  const runId = db.prepare(`INSERT INTO yadash_insight_runs (trigger, status, rules_version, started_at) VALUES (?, 'running', ?, ?)`)
    .run(trigger, RULES_VERSION, now()).lastInsertRowid;
  const started = Date.now();
  try {
    const settings = getSettings();
    const today = jstToday();
    // ルール単位で評価成否を記録。失敗ルールの occurrence を「非検出=解消」と誤判定しない
    // (Codex実装レビュー: 評価不能→誤resolve防止)
    const findings = [];
    const evaluated = new Set();
    const ruleErrors = [];
    for (const rule of RULES) {
      try {
        findings.push(...rule.run(db, settings, today));
        evaluated.add(rule.key);
      } catch (e) {
        ruleErrors.push(`${rule.key}: ${String(e.message).slice(0, 120)}`);
      }
    }
    if (evaluated.size === 0) throw new Error('全ルールの評価に失敗: ' + ruleErrors.join(' / '));

    const ts = now();
    let found = 0, resolvedCount = 0;
    const insEvent = db.prepare(`INSERT INTO yadash_insight_events (occurrence_id, event_type, at, detail_json) VALUES (?, ?, ?, ?)`);
    const tx = db.transaction(() => {
      const seen = new Set();
      for (const f of findings) {
        found++;
        seen.add(`${f.rule_key}|${f.target_key}`);
        const evidence = { ...f.evidence, observed_at: ts, run_id: runId, rules_version: RULES_VERSION, schema_version: EVIDENCE_SCHEMA_VERSION };
        const ex = db.prepare(`SELECT id, severity, status, resolved_at FROM yadash_insight_occurrences WHERE rule_key = ? AND target_key = ?`)
          .get(f.rule_key, f.target_key);
        if (!ex) {
          const id = db.prepare(`INSERT INTO yadash_insight_occurrences
            (rule_key, target_type, target_key, severity, title, evidence_json, suggested_json, first_detected_at, last_detected_at, last_run_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
            .run(f.rule_key, f.target_type, f.target_key, f.severity, f.title, JSON.stringify(evidence), JSON.stringify(f.suggested), ts, ts, runId).lastInsertRowid;
          insEvent.run(id, 'detected', ts, JSON.stringify({ evidence }));
        } else {
          // dismissed は severity 昇格時のみ reopen (恒久無視の両立 — Codex設計相談2R)
          const escalated = SEV_RANK[f.severity] > SEV_RANK[ex.severity];
          const reappeared = ex.resolved_at !== null;
          const reopen = (reappeared && ex.status !== 'dismissed') || (ex.status === 'dismissed' && escalated);
          db.prepare(`UPDATE yadash_insight_occurrences SET
              severity = ?, title = ?, evidence_json = ?, suggested_json = ?,
              last_detected_at = ?, last_run_id = ?, resolved_at = NULL
              ${reopen ? `, status = 'open', status_updated_at = ?` : ''}
            WHERE id = ?`)
            .run(...(reopen
              ? [f.severity, f.title, JSON.stringify(evidence), JSON.stringify(f.suggested), ts, runId, ts, ex.id]
              : [f.severity, f.title, JSON.stringify(evidence), JSON.stringify(f.suggested), ts, runId, ex.id]));
          if (reappeared || reopen) insEvent.run(ex.id, 'reopened', ts, JSON.stringify({ evidence, escalated, was_status: ex.status }));
        }
      }
      // 非検出 → resolved。ただし評価に成功したルールの occurrence のみ (失敗ルールは現状維持)
      const openRows = db.prepare(`SELECT id, rule_key, target_key, evidence_json FROM yadash_insight_occurrences WHERE resolved_at IS NULL`).all();
      for (const o of openRows) {
        if (!evaluated.has(o.rule_key)) continue;
        if (seen.has(`${o.rule_key}|${o.target_key}`)) continue;
        resolvedCount++;
        db.prepare(`UPDATE yadash_insight_occurrences SET resolved_at = ? WHERE id = ?`).run(ts, o.id);
        insEvent.run(o.id, 'resolved', ts, JSON.stringify({ last_evidence: JSON.parse(o.evidence_json || '{}') }));
      }
      db.prepare(`UPDATE yadash_insight_runs SET status = 'succeeded', finished_at = ?, found_count = ?, resolved_count = ?, error_text = ? WHERE run_id = ?`)
        .run(now(), found, resolvedCount, ruleErrors.length ? `partial: ${ruleErrors.join(' / ')}` : null, runId);
    });
    tx();
    return { run_id: runId, found, resolved: resolvedCount, duration_ms: Date.now() - started, ...listInsights({}) };
  } catch (e) {
    try {
      db.prepare(`UPDATE yadash_insight_runs SET status = 'failed', finished_at = ?, error_text = ? WHERE run_id = ?`)
        .run(now(), String(e.message).slice(0, 500), runId);
    } catch { /* run行更新失敗は握る (元エラー優先) */ }
    throw e;
  }
}

// ─── 一覧 (AI契約: この JSON が将来の修正指示エージェントの入力) ───
export function listInsights({ status, include_resolved }) {
  ensureInsightTables();
  const db = getMirrorDB();
  const conds = [];
  const params = [];
  if (status && ['open', 'acked', 'dismissed', 'done'].includes(status)) { conds.push('status = ?'); params.push(status); }
  if (!include_resolved) conds.push('resolved_at IS NULL');
  const rows = db.prepare(`
    SELECT * FROM yadash_insight_occurrences
    ${conds.length ? 'WHERE ' + conds.join(' AND ') : ''}
    ORDER BY CASE severity WHEN 'high' THEN 0 WHEN 'warn' THEN 1 ELSE 2 END, last_detected_at DESC
    LIMIT 500
  `).all(...params).map(r => ({
    occurrence_id: r.id,
    rule_key: r.rule_key, target_type: r.target_type, target_key: r.target_key,
    severity: r.severity, title: r.title,
    evidence: JSON.parse(r.evidence_json || '{}'),
    suggested_action: JSON.parse(r.suggested_json || '{}'),
    first_detected_at: r.first_detected_at, last_detected_at: r.last_detected_at,
    resolved_at: r.resolved_at, status: r.status, status_memo: r.status_memo,
  }));
  const lastRun = db.prepare(`SELECT * FROM yadash_insight_runs ORDER BY run_id DESC LIMIT 1`).get() || null;
  return { rules_version: RULES_VERSION, rule_labels: getRuleLabels(), last_run: lastRun, insights: rows };
}

export function setInsightStatus(id, status, memo) {
  ensureInsightTables();
  if (!['open', 'acked', 'dismissed', 'done'].includes(status)) throw validationError('status が不正です');
  const db = getMirrorDB();
  const ex = db.prepare(`SELECT id, status FROM yadash_insight_occurrences WHERE id = ?`).get(Number(id));
  if (!ex) throw validationError('対象のインサイトが見つかりません');
  const ts = new Date().toISOString();
  db.prepare(`UPDATE yadash_insight_occurrences SET status = ?, status_memo = ?, status_updated_at = ? WHERE id = ?`)
    .run(status, String(memo || '').slice(0, 300), ts, ex.id);
  db.prepare(`INSERT INTO yadash_insight_events (occurrence_id, event_type, at, detail_json) VALUES (?, 'status_changed', ?, ?)`)
    .run(ex.id, ts, JSON.stringify({ from: ex.status, to: status, memo: String(memo || '').slice(0, 300) }));
  return listInsights({});
}
