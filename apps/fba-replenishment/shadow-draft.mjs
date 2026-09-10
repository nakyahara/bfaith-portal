/**
 * shadow-draft.mjs — FBA 補充の「影の下書き」(Company DB構想 Phase 2 ステップ 1)
 *
 * 何をするか:
 *   毎朝の同期のあとに、**今ある計算エンジンをそのまま**走らせて、その日の提案を Company DB に記録する。
 *   画面には出さない。人が使うものは何も変わらない。外への書き込みも一切しない。
 *
 * なぜ先にこれか (正本: AI_reference『FBA在庫補充_AI自動化_方向性_Codex議論_20260827.md』§5):
 *   画面や AI より先に「毎朝、計算できたか・なぜその数量か・昨日と何が変わったか」が残る状態を作る。
 *   7 日連続で自動実行され、**欠損 / 0 / 取得失敗を区別できる**ようになったら、このステップは終わり。
 *   ここで貯まった記録が、あとの「人が承認する画面」(ステップ 3) と「先月の提案のうち間違っていたもの」
 *   (Phase 2 の完了条件) の材料になる。
 *
 * 🚨 約束:
 *   - **数量を決めるのは今までどおり決定論的エンジン**。AI は何も決めない (3 層分担: 計算 / 制御 / 説明)
 *   - **記録するだけ**。ai.decisions に入れるだけで、納品プランも CSV も作らない。autonomy_level = 0
 *   - **Company DB 側で失敗しても業務を止めない** (二重書き期間の共通ルール)。呼び出し側は best-effort で呼ぶ
 *   - 提案できた行だけでなく **提案できなかった行も記録する** (「0 個」と「計算できなかった」を混ぜない)
 *   - 毎朝ぜんぶ計算し直すので、前日の未処理は superseded にする (古い数量が残らない)
 *
 * 記録先:
 *   ai.decisions  ... 1 行 = 1 出品 (Amazon の SKU)。提案 (proposal) と 提案不能 (finding)
 *   ops.job_runs  ... 1 行 = 1 回の実行。件数・データ品質・前日との差分を summary に
 */
import { normSku } from '../../lib/sku-norm.js';

/** 計算式の版。エンジンの規則を変えたら上げる (記録から「どの版の提案か」を追えるように) */
export const RULE_VERSION = 'fba-reco-v1';
export const DOMAIN = 'fba_replenishment';
/** 台帳の id。独立したスケジュールは作らず、既存の毎朝の同期に相乗りする */
export const JOB_ID = 'fba-daily-sync';
/** 提案が古くなったら承認できない (設計 §4-2: 48〜72 時間)。影の段階でも同じ期限を入れておく */
export const EXPIRES_HOURS = 72;
const COMPANY_ID = 1;
const AMAZON_MALL = 'amazon';

/** 1 回の実行の名前。時刻順に並ぶ + 同じミリ秒でも衝突しない */
export function newShadowRunId(now = new Date()) {
  const t = new Date(now.getTime() + 9 * 3600 * 1000).toISOString().replace(/[-:TZ.]/g, '').slice(0, 15);
  return `fbasd_${t}_${Math.random().toString(16).slice(2, 8)}`;
}

/** 数値として意味のある値だけ返す (null / undefined / NaN は null。「0」と「無い」を混ぜない) */
const num = (v) => (typeof v === 'number' && Number.isFinite(v) ? v : null);

/**
 * その行が「提案できない」理由。null なら提案できる。
 * 🚨 ここで返す理由コードが、あとで「なぜ計算できなかったか」を数える材料になる
 */
export function blockedReason(item) {
  if (item.invalid_mapping) return 'invalid_mapping';        // SKU の対応づけが壊れている
  if (!item.ne_code) return 'no_ne_code';                    // 自社の商品コードに結び付いていない
  if (num(item.warehouse_available) === null) return 'warehouse_unknown';   // 自社在庫が取れていない (0 ではなく不明)
  if (num(item.units_sold_30d) === null) return 'sales_unknown';            // 販売数が取れていない
  return null;
}

/**
 * エンジンの出力から、記録する行だけを選ぶ。
 *   proposals = 今日「送ろう」と言える行 (補正後の数量が 1 以上)
 *   blocked   = 計算できなかった行 (「0 個」ではないので分けて残す)
 * それ以外 (計算できて 0 個 = 今日は送らなくていい) は記録しない。件数だけ数える。
 */
export function pickDraftRows(items) {
  const proposals = []; const blocked = [];
  let calm = 0;
  for (const it of items) {
    const reason = blockedReason(it);
    if (reason) { blocked.push({ item: it, reason }); continue; }
    if (num(it.adjusted_qty) > 0) proposals.push(it);
    else calm++;
  }
  return { proposals, blocked, calm };
}

/** 提案 1 件の「なぜその数量か」。人が読んで確かめられる言葉で */
export function rationaleOf(it) {
  const parts = [];
  const dos = num(it.days_of_supply);
  if (dos !== null) parts.push(`FBA 在庫はあと ${dos} 日分`);
  if (num(it.reorder_point_days) !== null) parts.push(`発注点 ${it.reorder_point_days} 日を下回った`);
  if (num(it.target_days) !== null) parts.push(`目標 ${it.target_days} 日分まで積む`);
  if (num(it.warehouse_available) !== null) parts.push(`自社在庫 ${it.warehouse_available} 個`);
  if (it.amazon_reco_capped) parts.push(`Amazon の推奨 ${it.amazon_recommended_qty} 個で頭打ち`);
  if (it.expiry_limited) parts.push(`賞味期限 (${it.expiry_date}) で頭打ち`);
  if (it.location_adjusted) parts.push(`置き場の都合で調整 (${it.location_detail || ''})`.trim());
  if (it.recent_arrival_adjusted) parts.push('最近入荷したぶんを見込んだ');
  return parts.join(' / ') || '発注点を下回ったため';
}

/** 提案 1 件が参照した値。あとから「その日の入力」を再現できるだけ残す */
export function inputsOf(it, ctx) {
  return {
    run_id: ctx.runId,
    calculated_at: ctx.calculatedAt,
    data_as_of: ctx.snapshotDate,          // どの日のスナップショットで計算したか
    data_source: ctx.dataSource,
    amazon_sku: it.amazon_sku,
    asin: it.asin || null,
    ne_code: it.ne_code || null,
    is_set: !!it.is_set,
    stock_state: it.stock_state || null,
    fba_available: num(it.fba_available),
    fba_inbound_working: num(it.fba_inbound_working_effective),
    effective_fba_stock: num(it.effective_fba_stock),
    units_sold_7d: num(it.units_sold_7d),
    units_sold_30d: num(it.units_sold_30d),
    daily_sales: num(it.daily_sales),
    days_of_supply: num(it.days_of_supply),
    reorder_point: num(it.reorder_point),
    target_days: num(it.target_days),
    target_stock: num(it.target_stock),
    warehouse_available: num(it.warehouse_available),
    recommended_qty: num(it.recommended_qty),      // 丸める前
    rounded_qty: num(it.rounded_qty),              // 入数で丸めたあと
    adjusted_qty: num(it.adjusted_qty),            // 置き場の都合まで見たあと = 画面の「補正後」
    amazon_recommended_qty: num(it.amazon_recommended_qty),
    urgency_score: num(it.urgency_score),
    alerts: it.alerts || null,
    exception_type: it.exception_type || null,
    prev_qty: ctx.prevQty ?? null,                 // 前回の提案数量 (無ければ null)
  };
}

/** dedupe_key = 出品 1 つにつき 1 本。翌朝の実行が前日ぶんを superseded にするための鍵 */
export const dedupeKeyOf = (amazonSku) => `${DOMAIN}:${normSku(amazonSku) || String(amazonSku || '').toLowerCase()}`;

/**
 * Amazon の SKU を Company DB の出品に結び付ける。
 * 🚨 正規化は core.norm_code (= lib/sku-norm.js の normSku) と同じものを使う。
 *    結び付かなくても記録は残す (subject_id = null + 理由)。Company DB 側の取りこぼしが見えるように
 */
export async function resolveListings(db, amazonSkus) {
  const norms = [...new Set(amazonSkus.map((s) => normSku(s)).filter(Boolean))];
  if (!norms.length) return new Map();
  const { rows } = await db.query(
    `select listing_id, listing_norm from core.listings
      where company_id = $1 and mall = $2 and listing_norm = any($3::text[])`,
    [COMPANY_ID, AMAZON_MALL, norms]);
  return new Map(rows.map((r) => [r.listing_norm, Number(r.listing_id)]));
}

/**
 * 影の下書きを 1 回ぶん記録する。
 * @param db    { query(sql, params) } (pg の Client を包んだもの)
 * @param result 計算エンジンの戻り値 (generateRecommendations())
 * @returns { runId, proposals, blocked, calm, unresolved, diff, status }
 */
export async function recordShadowDraft(db, result, { host = 'render', log = () => {}, now = new Date() } = {}) {
  const runId = newShadowRunId(now);
  const startedAt = now.toISOString();
  const items = Array.isArray(result?.items) ? result.items : [];
  const { proposals, blocked, calm } = pickDraftRows(items);
  const dq = result?.data_quality || {};

  await db.query('begin');
  try {
    // 前回の提案 (まだ開いているもの) を読む → 差分を数えるため。読んでから superseded にする
    const { rows: prevRows } = await db.query(
      `select dedupe_key, (inputs_ref->>'adjusted_qty')::numeric as qty
         from ai.decisions
        where domain = $1 and status in ('new','reviewable') and dedupe_key is not null`, [DOMAIN]);
    const prev = new Map(prevRows.map((r) => [r.dedupe_key, r.qty === null ? null : Number(r.qty)]));

    // 🚨 毎朝ぜんぶ計算し直すので、前日の未処理は「差し替え済み」にする (古い数量を残さない)
    await db.query(
      `update ai.decisions set status = 'superseded'
        where domain = $1 and status in ('new','reviewable')`, [DOMAIN]);

    const listingOf = await resolveListings(db, items.map((i) => i.amazon_sku));
    const ctxBase = {
      runId,
      calculatedAt: result?.generated_at || startedAt,
      snapshotDate: result?.snapshot_date || null,
      dataSource: dq.data_source || null,
    };

    const ins = async (row) => {
      await db.query(
        `insert into ai.decisions
           (company_id, domain, decision_kind, subject_type, subject_id, summary, rationale, severity,
            proposed_action, inputs_ref, model, rule_version, generated_by, autonomy_level, status, dedupe_key, expires_at)
         values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'rule',0,'new',$13,$14)`,
        [COMPANY_ID, DOMAIN, row.kind, row.subjectType, row.subjectId, row.summary, row.rationale, row.severity,
          row.proposedAction, row.inputs, `rule:${RULE_VERSION}`, RULE_VERSION, row.dedupeKey, row.expiresAt]);
    };
    const expiresAt = new Date(now.getTime() + EXPIRES_HOURS * 3600 * 1000).toISOString();
    let unresolved = 0;

    for (const it of proposals) {
      const key = dedupeKeyOf(it.amazon_sku);
      const listingId = listingOf.get(normSku(it.amazon_sku)) ?? null;
      if (listingId === null) unresolved++;
      const qty = num(it.adjusted_qty);
      await ins({
        kind: 'proposal',
        subjectType: listingId === null ? null : 'listing',
        subjectId: listingId,
        summary: `${it.product_name || it.amazon_sku} を ${qty} 個 FBA へ`,
        rationale: rationaleOf(it),
        severity: null,
        proposedAction: {
          action_type: 'fba_replenish',
          parameters: { amazon_sku: it.amazon_sku, qty, ne_code: it.ne_code || null },
          requires_approval: true,          // 影の段階では誰も承認しない。実行もしない
        },
        inputs: { ...inputsOf(it, { ...ctxBase, prevQty: prev.has(key) ? prev.get(key) : null }), listing_resolved: listingId !== null },
        dedupeKey: key,
        expiresAt,
      });
    }

    for (const b of blocked) {
      const it = b.item;
      const key = dedupeKeyOf(it.amazon_sku);
      const listingId = listingOf.get(normSku(it.amazon_sku)) ?? null;
      await ins({
        kind: 'finding',
        subjectType: listingId === null ? null : 'listing',
        subjectId: listingId,
        summary: `${it.amazon_sku} は提案を出せない (${b.reason})`,
        rationale: '「0 個でよい」ではなく「計算できなかった」。直さないと、この SKU はいつまでも提案に出てこない',
        severity: 'warn',
        proposedAction: null,
        inputs: { ...inputsOf(it, { ...ctxBase, prevQty: null }), blocked_reason: b.reason, listing_resolved: listingId !== null },
        dedupeKey: key,
        expiresAt,
      });
    }

    // 前日はあったのに今日は消えた提案 (送らなくてよくなった / 計算対象から外れた)
    const todayKeys = new Set([...proposals, ...blocked.map((b) => b.item)].map((i) => dedupeKeyOf(i.amazon_sku)));
    const gone = [...prev.keys()].filter((k) => !todayKeys.has(k));
    const changed = proposals.filter((it) => {
      const k = dedupeKeyOf(it.amazon_sku);
      return prev.has(k) && prev.get(k) !== num(it.adjusted_qty);
    }).length;
    const added = proposals.filter((it) => !prev.has(dedupeKeyOf(it.amazon_sku))).length;

    const status = blocked.length || dq.unmapped_active_count ? 'partial' : 'ok';
    const summary = [
      `run=${runId}`,
      `提案 ${proposals.length} 件 (新 ${added} / 数量変更 ${changed} / 消えた ${gone.length})`,
      `送らなくてよい ${calm} 件`,
      `提案不能 ${blocked.length} 件`,
      unresolved ? `Company DB に出品が無い ${unresolved} 件` : null,
      dq.unmapped_active_count ? `未マップ(実績あり) ${dq.unmapped_active_count} 件` : null,
      `対象日 ${ctxBase.snapshotDate || '不明'}`,
    ].filter(Boolean).join(' / ');

    await db.query(
      `insert into ops.job_runs (job_id, host, started_at, finished_at, status, summary)
       values ($1,$2,$3,now(),$4,$5)`,
      [JOB_ID, host, startedAt, status, summary.slice(0, 2000)]);

    await db.query('commit');
    log(`影の下書き: ${summary}`);
    return { runId, proposals: proposals.length, blocked: blocked.length, calm, unresolved, added, changed, gone: gone.length, status, summary };
  } catch (e) {
    try { await db.query('rollback'); } catch { /* 接続が死んでいれば rollback も失敗する */ }
    throw e;
  }
}
