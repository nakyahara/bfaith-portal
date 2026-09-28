/**
 * 広告の進み — ボードの「📣 広告」タブ (2026-09-28 中原さん「広告をかけた商品は何か・どこまでやったかを管理したい」)
 *
 * 対象 = 自社商品 (own_brand=1) × Amazon SP広告。
 * 人が記録するもの (ph_ad_ops_events・append-only):
 *   - 段階 (stage) = 未着手 / KW作成済み / 出稿中 (出したキャンペーンの種類) / 停止 (理由)
 *   - 「調整した」(adjust) = 入札・KW の見直しをした日とメモ。段階は変えない
 * 画面を開いたときに読むもの (ここには写さない = 状態を 2 か所に持たない):
 *   - SP広告KW の進み (ph_ad_kw_*: 採用した語の数・コピーした日)
 *   - 広告の実績 (mirror_amazon_ads_sku_daily: 毎日 miniPC から届く spAdvertisedProduct の SKU/ASIN 別)
 * 記録と実績が食い違うとき (出稿中なのに表示 0 / 未着手・停止なのに広告費が出ている) は警告を出す。
 * 段階を実績から自動で動かすことはしない (人の記録を黙って書き換えない)。
 *
 * 🚨 実績の結びつけ = 商品の NE 商品コード → Amazon の出品 SKU (mirror_sku_resolved・構成がこの商品だけの SKU)
 *    + 商品の ASIN → SKU (mirror_amazon_sku_fees) + ASIN 粒度の行。
 *    構成に別の商品が混ざる SKU (詰め合わせ) は数えない (その商品の広告かどうか決められないため)。
 */
import { logEvent, extractAsin } from '../db.js';

export const AD_OPS_STAGES = ['none', 'kw_ready', 'running', 'stopped'];
export const AD_OPS_STAGE_JA = { none: '未着手', kw_ready: 'KW作成済み', running: '出稿中', stopped: '停止' };
export const AD_OPS_CAMPAIGN_TYPES = ['auto', 'manual_kw', 'product_target'];
export const AD_OPS_CAMPAIGN_TYPE_JA = { auto: 'オート', manual_kw: 'マニュアルKW', product_target: '商品ターゲット' };
/** 出稿中で、最後の調整 (または出稿開始) からこの日数が過ぎたら「調整から N 日」を目立たせる */
export const ADJUST_STALE_DAYS = 14;
/** 実績の集計期間 (実績の最新日から数える) */
export const ACTUAL_WINDOW_DAYS = 30;
/** 「いま表示されているか」の判定期間 (実績の最新日から数える) */
export const ACTIVE_WINDOW_DAYS = 7;
/** 実績の最新日が今日からこの日数より古いときは「実績が古い」として食い違いの警告を出さない */
export const ACTUAL_STALE_DAYS = 4;
const MEMO_MAX = 500;
const HISTORY_PER_DRAFT = 5;

/** JST の今日 (YYYY-MM-DD) */
export function jstToday(now = Date.now()) {
  return new Date(now + 9 * 3600 * 1000).toISOString().slice(0, 10);
}
/** YYYY-MM-DD 同士の日数差 (b - a)。どちらかが無ければ null */
export function daysBetween(a, b) {
  if (!a || !b) return null;
  const ta = Date.parse(`${a}T00:00:00Z`), tb = Date.parse(`${b}T00:00:00Z`);
  if (!Number.isFinite(ta) || !Number.isFinite(tb)) return null;
  return Math.round((tb - ta) / 86400000);
}
/** YYYY-MM-DD から n 日ずらした日 */
export function addDays(ymd, n) {
  const t = Date.parse(`${ymd}T00:00:00Z`);
  return new Date(t + n * 86400000).toISOString().slice(0, 10);
}
const isYmd = (s) => typeof s === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(s) && new Date(`${s}T00:00:00Z`).toISOString().slice(0, 10) === s;
const parseTypes = (s) => {
  try { const a = JSON.parse(s || '[]'); return Array.isArray(a) ? a.filter((t) => AD_OPS_CAMPAIGN_TYPES.includes(t)) : []; } catch { return []; }
};

/** 商品のいまの段階の行 (最新の stage 行)。無ければ null (= 未着手) */
export function currentStageEventOf(db, draftId) {
  return db.prepare(`
    SELECT * FROM ph_ad_ops_events WHERE draft_id = ? AND mall = 'amazon' AND kind = 'stage' ORDER BY id DESC LIMIT 1
  `).get(draftId) || null;
}

/**
 * 段階 / 「調整した」を 1 件記録する (append-only)。
 * @param body {kind:'stage'|'adjust', stage?, campaign_types?, memo?, happened_on?, base_stage_event_id?}
 *   base_stage_event_id = 画面が見ていた「いまの段階」の行 id (無ければ 0)。別の人が先に段階を変えていたら 409 で止める
 * @returns {{ok:true, event}|{code, error, status?}}
 */
export function recordAdOps(db, draft, body, actor, { now = Date.now() } = {}) {
  if (!draft || draft.own_brand !== 1) return { code: 'not_own_brand', error: '広告の記録は「自社商品」にチェックのある商品だけです' };
  const kind = String(body?.kind || '');
  if (kind !== 'stage' && kind !== 'adjust') return { code: 'bad_kind', error: '記録の種類が不正です' };
  const today = jstToday(now);
  const happenedOn = body?.happened_on == null || body.happened_on === '' ? today : String(body.happened_on);
  if (!isYmd(happenedOn)) return { code: 'bad_date', error: '日付は YYYY-MM-DD で入れてください' };
  if (happenedOn > today) return { code: 'bad_date', error: '未来の日付は入れられません' };
  if (happenedOn < '2020-01-01') return { code: 'bad_date', error: '日付が古すぎます' };
  const memo = body?.memo == null ? '' : String(body.memo).trim();
  if (memo.length > MEMO_MAX) return { code: 'bad_memo', error: `メモは ${MEMO_MAX} 文字までです` };

  let stage = null, types = null;
  if (kind === 'stage') {
    stage = String(body?.stage || '');
    if (!AD_OPS_STAGES.includes(stage)) return { code: 'bad_stage', error: '段階の値が不正です' };
    if (stage === 'running') {
      const raw = Array.isArray(body?.campaign_types) ? body.campaign_types.map(String) : [];
      if (raw.some((t) => !AD_OPS_CAMPAIGN_TYPES.includes(t))) return { code: 'bad_types', error: 'キャンペーンの種類が不正です' };
      types = AD_OPS_CAMPAIGN_TYPES.filter((t) => raw.includes(t));
      if (!types.length) return { code: 'bad_types', error: '出したキャンペーンの種類 (オート / マニュアルKW / 商品ターゲット) を 1 つ以上選んでください' };
    } else if (body?.campaign_types != null && Array.isArray(body.campaign_types) && body.campaign_types.length) {
      return { code: 'bad_types', error: 'キャンペーンの種類は「出稿中」のときだけ選べます' };
    }
    if (stage === 'stopped' && !memo) return { code: 'bad_memo', error: '停止の理由を入れてください' };
  }

  return db.transaction(() => {
    const cur = currentStageEventOf(db, draft.id);
    if (kind === 'stage') {
      const base = Number(body?.base_stage_event_id) || 0;
      if ((cur ? cur.id : 0) !== base) {
        return { code: 'stale', status: 409, error: `別の人が先に段階を「${AD_OPS_STAGE_JA[cur ? cur.stage : 'none']}」に変えています。画面を読み直してから記録してください` };
      }
      const same = (cur ? cur.stage : 'none') === stage && JSON.stringify(cur ? parseTypes(cur.campaign_types) : []) === JSON.stringify(types || []);
      if (same && !memo && (!cur || cur.happened_on === happenedOn)) return { code: 'no_change', error: 'いまの記録と同じです' };
    } else if (!cur || cur.stage !== 'running') {
      return { code: 'not_running', error: '「調整した」は出稿中の商品だけ記録できます (先に段階を「出稿中」にしてください)' };
    }
    const info = db.prepare(`
      INSERT INTO ph_ad_ops_events (draft_id, mall, kind, stage, campaign_types, memo, happened_on, actor)
      VALUES (?, 'amazon', ?, ?, ?, ?, ?, ?)
    `).run(draft.id, kind, stage, types ? JSON.stringify(types) : null, memo || null, happenedOn, actor || null);
    const typeText = types ? ` (${types.map((t) => AD_OPS_CAMPAIGN_TYPE_JA[t]).join('・')})` : '';
    logEvent(db, draft.id, kind === 'stage' ? 'ad_ops_stage' : 'ad_ops_adjust',
      kind === 'stage'
        ? `Amazon 広告: ${AD_OPS_STAGE_JA[cur ? cur.stage : 'none']} → ${AD_OPS_STAGE_JA[stage]}${typeText}・${happenedOn}${memo ? `・${memo}` : ''}`
        : `Amazon 広告: 調整した・${happenedOn}${memo ? `・${memo}` : ''}`,
      actor);
    return { ok: true, event: db.prepare('SELECT * FROM ph_ad_ops_events WHERE id = ?').get(info.lastInsertRowid) };
  })();
}

/** ボードのカードに出す札用: いまの段階 (未着手は入れない)。Map<draft_id, stage> */
export function adStagesByDraft(db) {
  const rows = db.prepare(`
    SELECT e.draft_id, e.stage FROM ph_ad_ops_events e
    JOIN (SELECT draft_id, MAX(id) AS mid FROM ph_ad_ops_events WHERE mall = 'amazon' AND kind = 'stage' GROUP BY draft_id) m ON m.mid = e.id
    WHERE e.stage <> 'none'
  `).all();
  return new Map(rows.map((r) => [r.draft_id, r.stage]));
}

const hasTable = (db, name) => !!db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?").get(name);

/**
 * 商品 → 実績の対象 (SKU / ASIN) の対応を作る。
 * @returns {{skusOf: Map<draft_id, Set<lower sku>>, asinOf: Map<draft_id, lower asin>}}
 */
function targetsOf(db, drafts) {
  const skusOf = new Map(), asinOf = new Map();
  for (const d of drafts) {
    skusOf.set(d.id, new Set());
    const a = extractAsin(d);
    if (a) asinOf.set(d.id, a.toLowerCase());
  }
  const codes = [...new Set(drafts.map((d) => String(d.ne_code || '').trim()).filter(Boolean))];
  if (codes.length && hasTable(db, 'mirror_sku_resolved')) {
    // 構成がこの商品だけの SKU (単品・同じ商品の複数個セット)。詰め合わせは数えない
    const rows = db.prepare(`
      SELECT seller_sku, MIN(ne_code) AS ne_code FROM mirror_sku_resolved
      WHERE seller_sku IN (SELECT seller_sku FROM mirror_sku_resolved WHERE ne_code IN (SELECT value FROM json_each(?)))
      GROUP BY seller_sku HAVING COUNT(DISTINCT ne_code) = 1
    `).all(JSON.stringify(codes));
    const byCode = new Map();
    for (const r of rows) {
      if (!byCode.has(r.ne_code)) byCode.set(r.ne_code, []);
      byCode.get(r.ne_code).push(String(r.seller_sku).toLowerCase());
    }
    for (const d of drafts) for (const s of byCode.get(String(d.ne_code || '').trim()) || []) skusOf.get(d.id).add(s);
  }
  const asins = [...new Set([...asinOf.values()])];
  if (asins.length && hasTable(db, 'mirror_amazon_sku_fees')) {
    const rows = db.prepare(`
      SELECT seller_sku, LOWER(asin) AS asin FROM mirror_amazon_sku_fees WHERE LOWER(asin) IN (SELECT value FROM json_each(?))
    `).all(JSON.stringify(asins));
    const byAsin = new Map();
    for (const r of rows) {
      if (!byAsin.has(r.asin)) byAsin.set(r.asin, []);
      byAsin.get(r.asin).push(String(r.seller_sku).toLowerCase());
    }
    for (const d of drafts) for (const s of byAsin.get(asinOf.get(d.id)) || []) skusOf.get(d.id).add(s);
  }
  return { skusOf, asinOf };
}

/**
 * 広告の実績 (実績の最新日から 30 日)。
 * @returns {{latest:string|null, byTarget: Map<'sku:x'|'asin:x', {campaigns: Map<id, agg>}>, lastEver: Map<key, date>, campaignInfo: Map<id,{name,status}>}}
 */
function actualsOf(db, skus, asins) {
  const empty = { latest: null, from: null, from7: null, byKey: new Map(), lastEver: new Map(), campaignInfo: new Map() };
  if (!hasTable(db, 'mirror_amazon_ads_sku_daily')) return empty;
  const latest = db.prepare(`SELECT MAX(date_jst) AS d FROM mirror_amazon_ads_sku_daily WHERE mall = 'amazon'`).get()?.d || null;
  if (!latest || (!skus.length && !asins.length)) return { ...empty, latest };
  const from = addDays(latest, -(ACTUAL_WINDOW_DAYS - 1));
  const from7 = addDays(latest, -(ACTIVE_WINDOW_DAYS - 1));
  const targetWhere = `mall = 'amazon' AND ((target_granularity = 'sku' AND target IN (SELECT value FROM json_each(@skus)))
      OR (target_granularity = 'asin' AND target IN (SELECT value FROM json_each(@asins))))`;
  const params = { skus: JSON.stringify(skus), asins: JSON.stringify(asins), from, from7 };
  const rows = db.prepare(`
    SELECT target_granularity AS g, target AS t, campaign_id,
      SUM(impressions) AS imp, SUM(clicks) AS clicks, SUM(ad_cost) AS cost, SUM(ad_sales) AS sales, SUM(ad_units) AS units,
      SUM(CASE WHEN date_jst >= @from7 THEN impressions ELSE 0 END) AS imp7,
      SUM(CASE WHEN date_jst >= @from7 THEN ad_cost ELSE 0 END) AS cost7
    FROM mirror_amazon_ads_sku_daily
    WHERE date_jst >= @from AND ${targetWhere}
    GROUP BY g, t, campaign_id
  `).all(params);
  const byKey = new Map();
  for (const r of rows) {
    const k = `${r.g}:${r.t}`;
    if (!byKey.has(k)) byKey.set(k, []);
    byKey.get(k).push(r);
  }
  // 最後に表示された日 (期間の外も見る = 停止した商品の「最後に出ていた日」)
  const lastEver = new Map(db.prepare(`
    SELECT target_granularity || ':' || target AS k, MAX(date_jst) AS d
    FROM mirror_amazon_ads_sku_daily WHERE (impressions > 0 OR ad_cost > 0) AND ${targetWhere}
    GROUP BY k
  `).all(params).map((r) => [r.k, r.d]));
  const campaignIds = [...new Set(rows.map((r) => String(r.campaign_id)))];
  const campaignInfo = new Map();
  if (campaignIds.length && hasTable(db, 'mirror_amazon_ads_campaign_daily')) {
    for (const r of db.prepare(`
      SELECT c.campaign_id, c.campaign_name, c.campaign_status FROM mirror_amazon_ads_campaign_daily c
      JOIN (SELECT campaign_id, MAX(date_jst) AS d FROM mirror_amazon_ads_campaign_daily
            WHERE campaign_id IN (SELECT value FROM json_each(?)) GROUP BY campaign_id) m
        ON m.campaign_id = c.campaign_id AND m.d = c.date_jst
    `).all(JSON.stringify(campaignIds))) {
      campaignInfo.set(String(r.campaign_id), { name: r.campaign_name || '', status: r.campaign_status || '' });
    }
  }
  return { latest, from, from7, byKey, lastEver, campaignInfo };
}

/** SP広告KW の進み (開いている依頼の採用数・最後にコピーした日)。Map<draft_id, {...}> */
function kwStateOf(db, draftIds) {
  const out = new Map();
  if (!draftIds.length || !hasTable(db, 'ph_ad_kw_requests')) return out;
  const reqs = db.prepare(`
    SELECT r.draft_id, r.id FROM ph_ad_kw_requests r
    JOIN (SELECT draft_id, MAX(id) AS mid FROM ph_ad_kw_requests
          WHERE status IN ('collecting', 'review_ready') AND draft_id IN (SELECT value FROM json_each(?)) GROUP BY draft_id) m ON m.mid = r.id
  `).all(JSON.stringify(draftIds));
  if (!reqs.length) return out;
  const reqIds = JSON.stringify(reqs.map((r) => r.id));
  const adopted = new Map();
  for (const r of db.prepare(`
    SELECT d.request_id, c.kind, COUNT(*) AS n FROM ph_ad_kw_decisions d
    JOIN (SELECT candidate_id, MAX(id) AS mid FROM ph_ad_kw_decisions
          WHERE request_id IN (SELECT value FROM json_each(?)) GROUP BY candidate_id) m ON m.mid = d.id
    JOIN ph_ad_kw_candidates c ON c.id = d.candidate_id
    WHERE d.decision = 'adopt' GROUP BY d.request_id, c.kind
  `).all(reqIds)) {
    const a = adopted.get(r.request_id) || { kw: 0, asin: 0 };
    if (r.kind === 'asin') a.asin += r.n; else if (r.kind === 'kw') a.kw += r.n;
    adopted.set(r.request_id, a);
  }
  const copiedAt = new Map();
  for (const r of db.prepare(`SELECT request_id, copied_json FROM ph_ad_kw_exports WHERE request_id IN (SELECT value FROM json_each(?))`).all(reqIds)) {
    let c = {};
    try { c = JSON.parse(r.copied_json || '{}') || {}; } catch { c = {}; }
    for (const v of Object.values(c)) if (typeof v === 'string' && (!copiedAt.has(r.request_id) || v > copiedAt.get(r.request_id))) copiedAt.set(r.request_id, v);
  }
  for (const r of reqs) {
    const a = adopted.get(r.id) || { kw: 0, asin: 0 };
    const c = copiedAt.get(r.id) || null;
    out.set(r.draft_id, { requestId: r.id, adoptedKw: a.kw, adoptedAsin: a.asin, copiedOn: c ? jstToday(Date.parse(c)) : null });
  }
  return out;
}

/**
 * 「📣 広告」タブの表の行。自社商品 (除外した商品を除く) すべて。
 * @returns {{rows, counts:{none,kw_ready,running,stopped,warn}, actualLatest, actualFrom, actualStale}}
 */
export function adOpsRows(db, { now = Date.now() } = {}) {
  const today = jstToday(now);
  const drafts = db.prepare(`
    SELECT id, ne_code, name, asin, amazon_url, status, parent_draft_id, created_at FROM product_drafts
    WHERE own_brand = 1 AND status <> 'excluded' ORDER BY id DESC
  `).all();
  const ids = drafts.map((d) => d.id);
  const idsJson = JSON.stringify(ids);
  const stageOf = new Map(), adjustOf = new Map(), historyOf = new Map();
  if (ids.length) {
    for (const e of db.prepare(`
      SELECT e.* FROM ph_ad_ops_events e
      JOIN (SELECT draft_id, MAX(id) AS mid FROM ph_ad_ops_events
            WHERE mall = 'amazon' AND kind = 'stage' AND draft_id IN (SELECT value FROM json_each(?)) GROUP BY draft_id) m ON m.mid = e.id
    `).all(idsJson)) stageOf.set(e.draft_id, e);
    for (const e of db.prepare(`
      SELECT e.* FROM ph_ad_ops_events e
      JOIN (SELECT draft_id, MAX(id) AS mid FROM ph_ad_ops_events
            WHERE mall = 'amazon' AND kind = 'adjust' AND draft_id IN (SELECT value FROM json_each(?)) GROUP BY draft_id) m ON m.mid = e.id
    `).all(idsJson)) adjustOf.set(e.draft_id, e);
    for (const e of db.prepare(`
      SELECT * FROM (
        SELECT e.*, ROW_NUMBER() OVER (PARTITION BY draft_id ORDER BY id DESC) AS rn FROM ph_ad_ops_events e
        WHERE mall = 'amazon' AND draft_id IN (SELECT value FROM json_each(?))
      ) WHERE rn <= ${HISTORY_PER_DRAFT} ORDER BY id DESC
    `).all(idsJson)) {
      if (!historyOf.has(e.draft_id)) historyOf.set(e.draft_id, []);
      historyOf.get(e.draft_id).push(e);
    }
  }
  const { skusOf, asinOf } = targetsOf(db, drafts);
  const allSkus = [...new Set([...skusOf.values()].flatMap((s) => [...s]))];
  const allAsins = [...new Set([...asinOf.values()])];
  const act = actualsOf(db, allSkus, allAsins);
  const actualStale = !act.latest || daysBetween(act.latest, today) > ACTUAL_STALE_DAYS;
  const kw = kwStateOf(db, ids);

  const counts = { none: 0, kw_ready: 0, running: 0, stopped: 0, warn: 0 };
  const rows = drafts.map((d) => {
    const st = stageOf.get(d.id) || null;
    const stage = st ? st.stage : 'none';
    // 実績 (SKU と ASIN の行を合算。同じキャンペーン × 同じ日の行が SKU と ASIN の両方に出ることは無い = 対象は SKU があれば SKU)
    const keys = [...(skusOf.get(d.id) || [])].map((s) => `sku:${s}`);
    if (asinOf.get(d.id)) keys.push(`asin:${asinOf.get(d.id)}`);
    const agg = { imp: 0, clicks: 0, cost: 0, sales: 0, units: 0, imp7: 0, cost7: 0 };
    const camp = new Map();
    let lastActive = null;
    for (const k of keys) {
      for (const r of act.byKey.get(k) || []) {
        for (const f of Object.keys(agg)) agg[f] += Number(r[f]) || 0;
        const c = camp.get(String(r.campaign_id)) || { imp: 0, cost: 0, imp7: 0 };
        c.imp += Number(r.imp) || 0; c.cost += Number(r.cost) || 0; c.imp7 += Number(r.imp7) || 0;
        camp.set(String(r.campaign_id), c);
      }
      const le = act.lastEver.get(k);
      if (le && (!lastActive || le > lastActive)) lastActive = le;
    }
    const campaigns = [...camp.entries()]
      .map(([id, c]) => ({ id, name: act.campaignInfo.get(id)?.name || `(名前不明 ${id})`, status: act.campaignInfo.get(id)?.status || '', cost: Math.round(c.cost), active: c.imp7 > 0 }))
      .sort((a, b) => b.cost - a.cost);
    const active = agg.imp7 > 0 || agg.cost7 > 0;
    const linked = keys.length > 0;
    // 食い違い (実績が古いとき・結びつく SKU/ASIN が無いときは判定しない)
    let warn = null;
    if (!actualStale && linked) {
      if (stage === 'running' && !active) warn = `出稿中の記録ですが、直近 ${ACTIVE_WINDOW_DAYS} 日の表示が 0 です (止まっていないか確認)`;
      else if (stage !== 'running' && active) warn = `${AD_OPS_STAGE_JA[stage]}の記録ですが、直近 ${ACTIVE_WINDOW_DAYS} 日に広告が表示されています (段階を「出稿中」に?)`;
    }
    if (warn) counts.warn++;
    counts[stage]++;
    const adj = adjustOf.get(d.id) || null;
    // 調整からの日数 = 出稿中のとき、最後の調整 (無ければ出稿を記録した日) から
    // (いまの「出稿中」より前の調整は数えない = 一度止めて出し直した商品の古い調整日で安心させない)
    let sinceAdjust = null, sinceKind = null;
    if (stage === 'running') {
      const byAdjust = !!(adj && adj.id > st.id && adj.happened_on >= st.happened_on);
      sinceKind = byAdjust ? 'adjust' : 'running';
      sinceAdjust = daysBetween(byAdjust ? adj.happened_on : st.happened_on, today);
    }
    return {
      id: d.id, neCode: d.ne_code, name: d.name, asin: asinOf.get(d.id) ? asinOf.get(d.id).toUpperCase() : null,
      isSet: d.parent_draft_id != null,
      stage, stageLabel: AD_OPS_STAGE_JA[stage], stageEventId: st ? st.id : 0,
      stageOn: st ? st.happened_on : null, stageMemo: st ? st.memo : null,
      campaignTypes: st ? parseTypes(st.campaign_types) : [],
      lastAdjustOn: adj ? adj.happened_on : null, lastAdjustMemo: adj ? adj.memo : null,
      sinceAdjust, sinceKind, adjustStale: sinceAdjust != null && sinceAdjust >= ADJUST_STALE_DAYS,
      kw: kw.get(d.id) || null,
      linked, skuCount: (skusOf.get(d.id) || new Set()).size,
      actual: {
        cost: Math.round(agg.cost), sales: Math.round(agg.sales), units: agg.units, clicks: agg.clicks, imp: agg.imp,
        acos: agg.sales > 0 ? Math.round((agg.cost / agg.sales) * 1000) / 10 : null,
        active, lastActive, campaigns,
      },
      warn,
      history: (historyOf.get(d.id) || []).map((e) => ({
        id: e.id, kind: e.kind, stage: e.stage, stageLabel: e.stage ? AD_OPS_STAGE_JA[e.stage] : null,
        types: parseTypes(e.campaign_types), memo: e.memo, on: e.happened_on, actor: e.actor, at: e.created_at,
      })),
    };
  });
  // 並び: 食い違い → 出稿中 (調整から日数が長い順) → KW作成済み → 未着手 → 停止
  const order = { running: 1, kw_ready: 2, none: 3, stopped: 4 };
  rows.sort((a, b) => (b.warn ? 1 : 0) - (a.warn ? 1 : 0)
    || order[a.stage] - order[b.stage]
    || (b.sinceAdjust ?? -1) - (a.sinceAdjust ?? -1)
    || b.id - a.id);
  return { rows, counts, actualLatest: act.latest, actualFrom: act.from, actualStale, today };
}
