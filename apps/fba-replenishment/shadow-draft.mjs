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
 *
 * 🚨 いまの限界 (Codex 2026-09-10 R3。**次の PR で直す**):
 *   販売数 (units_sold_30d / 7d) の「取れなかった」は、**この仕組みより手前で 0 になっている**。
 *   レポートを読むところ (sp-api-reports.js) と保存するところ (db.js) が `|| 0` で埋めるため、
 *   ここに届いた時点で「売れていない」と見分けが付かない。
 *   → だから `sales_unknown` は **今は出ない**。直すには取り込みと保存で null を保つ必要があり、
 *     それは既存の画面のデータ経路に触るので別の PR にする (Amazon 推奨数は既に null を保つ形になっており、
 *     同じやり方をほかの列にも広げる)。
 *
 *   いま確かに言えるのはこれだけ:
 *     - PLANNING レポートに行が無い (planning_missing)
 *     - 倉庫に行が無い (warehouse_row_missing) / セットの構成に行が無い
 *     - SKU 対応表に無い (unmapped_active)
 *     - 対応表の構成が壊れている (invalid_mapping)
 *     - 準備中数量が取れなかった (inbound_working_state)
 *     - 計算そのものが失敗した (errors)
 *
 * 🚨 約束:
 *   - **数量を決めるのは今までどおり決定論的エンジン**。AI は何も決めない (3 層分担: 計算 / 制御 / 説明)
 *   - **記録するだけ**。ai.decisions に入れるだけで、納品プランも CSV も作らない。autonomy_level = 0
 *   - **Company DB 側で失敗しても業務を止めない** (二重書き期間の共通ルール)
 *   - **「送らなくてよい 0 個」と「計算できなかった」を混ぜない**。エンジンが `|| 0` で埋める前の姿
 *     (`item.data_gaps`) を見て分ける。ここが混ざると、このステップをやる意味が無くなる
 *   - **計算そのものが失敗した日 (errors) は、前日の提案を消さない**。「今日は何も要らない」と区別する
 *   - 毎朝ぜんぶ計算し直すので、前日の**未処理だけ** superseded にする (人が触った行は残す)
 *
 * 記録先:
 *   ai.decisions  ... 1 行 = 1 出品。提案 (proposal) と 提案不能・要対応 (finding)
 *   ops.job_runs  ... 1 行 = 1 回の実行 (成功も失敗も)。件数・データ品質・前日との差分を summary に
 */
import { normSku } from '../../lib/sku-norm.js';

/** 計算式の版。エンジンの規則を変えたら上げる (記録から「どの版の提案か」を追えるように) */
export const RULE_VERSION = 'fba-reco-v1';
export const DOMAIN = 'fba_replenishment';
/** 台帳の id。独立したスケジュールは作らず、既存の毎朝の同期に相乗りする */
export const JOB_ID = 'fba-daily-sync';
/** 提案が古くなったら承認できない (設計 §4-2: 48〜72 時間) */
export const EXPIRES_HOURS = 72;
/** この仕組みが作った行だけを差し替える印 (人が触った行・他の出どころを巻き込まない) */
export const GENERATOR = 'fba-shadow-draft';
const COMPANY_ID = 1;
const AMAZON_MALL = 'amazon';
/** Amazon の日本店。同じ SKU が別店舗にもあり得るので、店舗まで指定して引く */
export const AMAZON_SHOP_CODE = 'main@A1VC38T7YXB528';

/** 1 回の実行の名前。時刻順に並ぶ + 同じミリ秒でも衝突しない */
export function newShadowRunId(now = new Date()) {
  const t = new Date(now.getTime() + 9 * 3600 * 1000).toISOString().replace(/[-:TZ.]/g, '').slice(0, 15);
  return `fbasd_${t}_${Math.random().toString(16).slice(2, 8)}`;
}

/** 数値として意味のある値だけ返す (null / undefined / NaN は null) */
const num = (v) => (typeof v === 'number' && Number.isFinite(v) ? v : null);

/**
 * その行が「提案を出せない」理由。null なら数量を出せる。
 * 🚨 エンジンは取れなかった値を `|| 0` で 0 にしてしまうので、**0 になった値そのものではなく**
 *    `data_gaps` (0 にする前の姿) を見る。ここを間違えると「取れていない」が「売れていない」に化ける
 */
export function blockedReason(item) {
  if (item.invalid_mapping) return 'invalid_mapping';          // SKU の対応づけが壊れている
  // 🚨 根っこから順に見る。商品コードが無いから倉庫が引けないので、コード欠落を先に返す
  if (!item.ne_code) return 'no_ne_code';                      // 自社の商品コードに結び付いていない
  const g = item.data_gaps || {};
  if (g.sales_30d_missing) return 'sales_unknown';             // 販売数が取れていない (売れていない ではない)
  if (g.planning_missing) return 'planning_missing';           // PLANNING に無い = 7 日販売が 0 扱いになっている
  if (g.warehouse_row_missing) return 'warehouse_unknown';     // 倉庫に行が無い (在庫 0 ではない)
  return null;
}

/** その行に付いている「気をつけて見るべき点」(数量は出せたが、素性が怪しいところ) */
export function cautionsOf(item) {
  const g = item.data_gaps || {};
  return [
    g.sales_7d_missing ? 'sales_7d_missing' : null,
    g.fba_available_missing ? 'fba_available_missing' : null,
    g.per_unit_volume_missing ? 'per_unit_volume_missing' : null,
    g.amazon_reco_missing ? 'amazon_reco_missing' : null,
    (g.warehouse_missing_components || []).length ? 'warehouse_components_missing' : null,
    g.inbound_working_source === 'none' ? 'inbound_working_none' : null,
  ].filter(Boolean);
}

/** 数量が 0 だった行の「なぜ送らなくてよいか」。0 の意味を 1 つにまとめない */
export function calmReason(item) {
  // 🚨 状態を先に見る。長期欠品・廃番候補は 30 日販売が 0 なので、あとに置くと
  //    「まだ発注点を下回っていない」に全部吸われて見えなくなる (Codex 2026-09-10 R2)
  if (item.stock_state === 'dead_candidate') return 'dead_candidate';       // 売れず在庫も無く、Amazon も勧めない
  if (item.stock_state === 'revivable_long_oos') return 'long_oos';         // 長く欠品。復活の見込みあり
  if (item.skipped_min_days) return 'skipped_min_days';                     // 最低出荷日数に満たない
  if (!item.needs_replenishment) return 'above_reorder_point';              // まだ発注点を下回っていない
  if (num(item.warehouse_available) === 0) return 'no_warehouse_stock';     // 自社に在庫が無い (取れている上での 0)
  return 'zero_after_caps';                                                 // 上限で削られて 0 になった
}

/**
 * エンジンの出力から、記録する行を選ぶ。
 *   proposals = 今日「送ろう」と言える行
 *   blocked   = 数量を出せなかった行 (理由つき)
 *   calm      = 計算できて「送らなくてよい」行 (理由ごとに数える。全件は記録しない)
 */
export function pickDraftRows(items) {
  const proposals = []; const blocked = []; const calm = [];
  for (const it of items) {
    const reason = blockedReason(it);
    if (reason) { blocked.push({ item: it, reason }); continue; }
    if (num(it.adjusted_qty) > 0) proposals.push(it);
    else calm.push({ item: it, reason: calmReason(it) });
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
    data_source: ctx.dataSource,           // 'restock' か 'legacy_snapshots' か
    inbound_working_state: ctx.inboundState || null,   // 準備中数量を「どう手に入れたか」(fresh/cache/failed…)
    settings: ctx.settings || null,        // その日の設定 (規則を変えた日が分かる)
    amazon_sku: it.amazon_sku,
    asin: it.asin || null,
    ne_code: it.ne_code || null,
    is_set: !!it.is_set,
    set_components: it.set_components || null,
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
    warehouse_components: it.warehouse_components || null,   // 構成ごとの在庫 (セットの再計算に要る)
    per_unit_volume: num(it.per_unit_volume),
    is_seasonal: it.is_seasonal || null,
    season_name: it.season_name || null,
    recommended_qty: num(it.recommended_qty),      // 丸める前
    rounded_qty: num(it.rounded_qty),              // 入数で丸めたあと
    adjusted_qty: num(it.adjusted_qty),            // 置き場の都合まで見たあと = 画面の「補正後」
    amazon_recommended_qty: num(it.amazon_recommended_qty),
    amazon_reco_capped: !!it.amazon_reco_capped,
    expiry_limited: !!it.expiry_limited,
    expiry_date: it.expiry_date || null,
    location_adjusted: !!it.location_adjusted,
    location_detail: it.location_detail || null,
    urgency_score: num(it.urgency_score),
    alerts: it.alerts || null,
    exception_type: it.exception_type || null,
    // 🚨 0 で埋めた項目・取れていなかった項目 (この行の数値をどこまで信じてよいか)
    data_gaps: it.data_gaps || null,
    cautions: cautionsOf(it),
    prev_qty: ctx.prevQty ?? null,                 // 前回の提案数量 (無ければ null)
    prev_kind: ctx.prevKind ?? null,               // 前回の判定 (proposal / finding)
  };
}

/** run 単位の記録の鍵。出品ごとの行ではないので、前日との差分の数え上げからは外す */
export const RUN_SUMMARY_KEY = `${DOMAIN}:__run__`;

/** dedupe_key = 出品 1 つにつき 1 本。翌朝の実行が前日ぶんを superseded にするための鍵 */
export const dedupeKeyOf = (amazonSku) => `${DOMAIN}:${normSku(amazonSku) || String(amazonSku || '').toLowerCase()}`;

/**
 * Amazon の SKU を Company DB の出品に結び付ける。
 * 🚨 一意なのは mall + shop_code + listing_norm なので **店舗まで指定して引く**。
 *    それでも候補が 2 つ以上あったら結び付けない (後勝ちで別の出品に付けない)
 */
export async function resolveListings(db, amazonSkus, { shopCode = AMAZON_SHOP_CODE } = {}) {
  const norms = [...new Set(amazonSkus.map((s) => normSku(s)).filter(Boolean))];
  if (!norms.length) return new Map();
  const { rows } = await db.query(
    `select listing_norm, min(listing_id) as listing_id, count(*)::int as n
       from core.listings
      where company_id = $1 and mall = $2 and shop_code = $3 and listing_norm = any($4::text[])
      group by listing_norm`,
    [COMPANY_ID, AMAZON_MALL, shopCode, norms]);
  const out = new Map();
  for (const r of rows) {
    if (r.n === 1) out.set(r.listing_norm, Number(r.listing_id));
    // 2 つ以上 = どれか決められない。結び付けない (記録には「解決できなかった」として残る)
  }
  return out;
}

/**
 * 失敗した実行も履歴に残す。
 * 🚨 接続そのものが死んでいると、同じ接続では書けない。呼び出し側が `openFresh` を渡していれば
 *    **別の接続を開いて**書く (2 回目の失敗はあきらめてログだけ。Codex 2026-09-10 R2)
 */
export async function writeFailedRun(db, { host, startedAt, summary, log = () => {}, openFresh = null }) {
  const sql = `insert into ops.job_runs (job_id, host, started_at, finished_at, status, summary)
               values ($1,$2,$3,now(),'fail',$4)`;
  const params = [JOB_ID, host, startedAt, String(summary).slice(0, 2000)];
  try {
    await db.query(sql, params);
    return true;
  } catch (e) {
    log(`失敗の記録を今の接続で書けなかった: ${e.message}`);
  }
  if (!openFresh) return false;
  let fresh = null;
  try {
    fresh = await openFresh();
    await fresh.db.query(sql, params);
    return true;
  } catch (e2) {
    log(`別の接続でも書けなかった: ${e2.message}`);
    return false;
  } finally {
    if (fresh?.close) { try { await fresh.close(); } catch { /* もう閉じている */ } }
  }
}

/**
 * 影の下書きを 1 回ぶん記録する。
 * @param db     { query(sql, params) }
 * @param result 計算エンジンの戻り値 (generateRecommendations(false, inboundOverride))
 */
export async function recordShadowDraft(db, result, {
  host = 'render', log = () => {}, now = new Date(), inboundState = null, settings = null, openFresh = null,
  onFailRecorded = () => {},
} = {}) {
  const runId = newShadowRunId(now);
  const startedAt = now.toISOString();
  const items = Array.isArray(result?.items) ? result.items : [];
  const errors = Array.isArray(result?.errors) ? result.errors.filter(Boolean) : [];
  const dq = result?.data_quality || {};

  // 🚨 計算そのものが失敗した日 (スナップショットが無い・マッピングが無い) は、
  //    前日の提案を消さない。「今日は何も要らない」と混ぜると、翌朝いきなり提案が消える
  if (errors.length) {
    const summary = `run=${runId} / 計算できなかった: ${errors.join(' / ')}`;
    if (await writeFailedRun(db, { host, startedAt, summary, log, openFresh })) onFailRecorded();
    log(`影の下書き: ${summary}`);
    return { runId, ok: false, engineFailed: true, errors, proposals: 0, blocked: 0, calm: 0, status: 'fail', summary };
  }

  const { proposals, blocked, calm } = pickDraftRows(items);
  // 未マッピング (エンジンの items にそもそも出てこない SKU) も「提案不能」として残す。
  // 実績あり = 納品漏れ候補なので warn、実績なし = 情報として info
  const unmappedActive = Array.isArray(dq.unmapped_active) ? dq.unmapped_active : [];
  const unmappedInactive = Array.isArray(dq.unmapped_inactive_skus) ? dq.unmapped_inactive_skus : [];

  // 🚨 BEGIN 自体が失敗する (接続が死んでいる) こともあるので、ここから丸ごと包む
  try {
    await db.query('begin');
    // 前回の判定を読む → 差分を数えるため。読んでから superseded にする
    const { rows: prevRows } = await db.query(
      `select dedupe_key, decision_kind, (inputs_ref->>'adjusted_qty')::numeric as qty,
              inputs_ref->>'calm_reason' as calm_reason, inputs_ref->>'blocked_reason' as blocked_reason
         from ai.decisions
        where company_id = $1 and domain = $2 and status = 'new'
          and inputs_ref->>'generator' = $3 and dedupe_key is not null
          and dedupe_key <> $4`, [COMPANY_ID, DOMAIN, GENERATOR, RUN_SUMMARY_KEY]);
    const prev = new Map(prevRows.map((r) => [r.dedupe_key, {
      qty: r.qty === null ? null : Number(r.qty), kind: r.decision_kind,
      calmReason: r.calm_reason, blockedReason: r.blocked_reason,
    }]));

    // 🚨 差し替えるのは **この仕組みが作った未処理の行だけ**。
    //    人が見始めた行 (reviewable) や承認済み、他の出どころの行は触らない
    await db.query(
      `update ai.decisions set status = 'superseded'
        where company_id = $1 and domain = $2 and status = 'new' and inputs_ref->>'generator' = $3`,
      [COMPANY_ID, DOMAIN, GENERATOR]);

    const allSkus = [...items.map((i) => i.amazon_sku), ...unmappedActive.map((u) => u.sku), ...unmappedInactive];
    const listingOf = await resolveListings(db, allSkus);
    const ctxBase = {
      runId,
      calculatedAt: result?.generated_at || startedAt,
      snapshotDate: result?.snapshot_date || null,
      dataSource: dq.data_source || null,
      inboundState,
      settings,
    };

    const ins = async (row) => {
      await db.query(
        `insert into ai.decisions
           (company_id, domain, decision_kind, subject_type, subject_id, summary, rationale, severity,
            proposed_action, inputs_ref, model, rule_version, generated_by, autonomy_level, status, dedupe_key, expires_at)
         values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'rule',0,'new',$13,$14)`,
        [COMPANY_ID, DOMAIN, row.kind, row.subjectType, row.subjectId, row.summary, row.rationale, row.severity,
          row.proposedAction, { ...row.inputs, generator: GENERATOR }, `rule:${RULE_VERSION}`, RULE_VERSION, row.dedupeKey, row.expiresAt]);
    };
    const expiresAt = new Date(now.getTime() + EXPIRES_HOURS * 3600 * 1000).toISOString();
    const listingIdOf = (sku) => listingOf.get(normSku(sku)) ?? null;
    let unresolved = 0;

    for (const it of proposals) {
      const key = dedupeKeyOf(it.amazon_sku);
      const listingId = listingIdOf(it.amazon_sku);
      if (listingId === null) unresolved++;
      const qty = num(it.adjusted_qty);
      const p = prev.get(key);
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
        inputs: { ...inputsOf(it, { ...ctxBase, prevQty: p?.qty ?? null, prevKind: p?.kind ?? null }), listing_resolved: listingId !== null },
        dedupeKey: key,
        expiresAt,
      });
    }

    for (const b of blocked) {
      const it = b.item;
      const key = dedupeKeyOf(it.amazon_sku);
      const listingId = listingIdOf(it.amazon_sku);
      const p = prev.get(key);
      await ins({
        kind: 'finding',
        subjectType: listingId === null ? null : 'listing',
        subjectId: listingId,
        summary: `${it.amazon_sku} は数量を出せない (${b.reason})`,
        rationale: '「0 個でよい」ではなく「計算できなかった」。直さないと、この SKU はいつまでも提案に出てこない',
        severity: 'warn',
        proposedAction: null,
        inputs: { ...inputsOf(it, { ...ctxBase, prevQty: p?.qty ?? null, prevKind: p?.kind ?? null }), blocked_reason: b.reason, listing_resolved: listingId !== null },
        dedupeKey: key,
        expiresAt,
      });
    }

    // 未マッピング: エンジンの計算対象にすら入っていない = 提案が永久に出てこない
    for (const u of unmappedActive) {
      const key = dedupeKeyOf(u.sku);
      const listingId = listingIdOf(u.sku);
      await ins({
        kind: 'finding',
        subjectType: listingId === null ? null : 'listing',
        subjectId: listingId,
        summary: `${u.sku} は SKU 対応表に無い (実績あり = 納品漏れ候補)`,
        rationale: `30 日販売 ${u.units_sold_30d} / FBA在庫 ${u.fba_available} / 入荷中 ${u.fba_inbound}。対応表に足すまで計算対象に入らない`,
        severity: 'warn',
        proposedAction: null,
        inputs: { ...ctxBase, amazon_sku: u.sku, blocked_reason: 'unmapped_active', unmapped: u, listing_resolved: listingId !== null, prev_qty: prev.get(key)?.qty ?? null },
        dedupeKey: key,
        expiresAt,
      });
    }

    // 前日は出ていたのに今日は出てこない行 (送らなくてよくなった / 対象から外れた)。
    // 🚨 0 の行は全部は記録しない (毎日 7,000 行では読めない) が、**前日から変わったものだけ**は残す
    const todayKeys = new Set([
      ...proposals.map((i) => dedupeKeyOf(i.amazon_sku)),
      ...blocked.map((b) => dedupeKeyOf(b.item.amazon_sku)),
      ...unmappedActive.map((u) => dedupeKeyOf(u.sku)),
    ]);
    const calmByKey = new Map(calm.map((c) => [dedupeKeyOf(c.item.amazon_sku), c]));
    const goneAll = [...prev.entries()].filter(([k]) => !todayKeys.has(k));
    // 🚨 前日ぶんが「消えた」記録そのものだった場合、同じ状態なら **もう一度残さない**。
    //    毎朝作り直すと「過去に一度でも出た SKU」がずっと積み上がる (毎日 7,000 行になり得る。Codex R2)
    const gone = goneAll.filter(([key, p]) => {
      const c = calmByKey.get(key);
      const nowReason = c ? c.reason : 'not_in_items';
      const wasGone = p.kind === 'finding' && p.calmReason;
      return !(wasGone && p.calmReason === nowReason);
    });
    const goneSame = goneAll.length - gone.length;
    for (const [key, p] of gone) {
      const c = calmByKey.get(key);
      const it = c?.item;
      const listingId = it ? listingIdOf(it.amazon_sku) : null;
      await ins({
        kind: 'finding',
        subjectType: listingId === null ? null : 'listing',
        subjectId: listingId,
        summary: `${it?.amazon_sku || key.split(':')[1]} は今日は提案に出ない (${c ? c.reason : 'not_in_items'})`,
        rationale: p.qty !== null ? `前回は ${p.qty} 個の提案だった。何が変わったかを追えるように残す` : '前回は提案不能だった',
        severity: null,
        proposedAction: null,
        inputs: it
          ? { ...inputsOf(it, { ...ctxBase, prevQty: p.qty, prevKind: p.kind }), calm_reason: c.reason, listing_resolved: listingId !== null }
          : { ...ctxBase, amazon_sku: key.split(':')[1], calm_reason: 'not_in_items', prev_qty: p.qty, prev_kind: p.kind, listing_resolved: false },
        dedupeKey: key,
        expiresAt,
      });
    }

    // 🚨 run 単位の記録を 1 行必ず残す。全部「送らなくてよい」の日は提案の行が 0 件になるので、
    //    その日の設定・データ品質・準備中数量の取れ方が どこにも残らなくなる (Codex R2)
    await ins({
      kind: 'finding',
      subjectType: null,
      subjectId: null,
      summary: `${ctxBase.snapshotDate || '日付不明'} の影の下書き (提案 ${proposals.length} / 出せない ${blocked.length} / 送らなくてよい ${calm.length})`,
      rationale: 'この日の入力ひとそろい。あとから同じ計算をやり直すための控え',
      severity: null,
      proposedAction: null,
      inputs: {
        ...ctxBase,
        run_summary: true,
        counts: { proposals: proposals.length, blocked: blocked.length, calm: calm.length, unmapped_active: unmappedActive.length, unmapped_inactive: unmappedInactive.length },
        data_quality: dq,
        engine_generated_at: result?.generated_at || null,
      },
      dedupeKey: RUN_SUMMARY_KEY,
      expiresAt,
    });

    const changed = proposals.filter((it) => {
      const p = prev.get(dedupeKeyOf(it.amazon_sku));
      return p && p.qty !== num(it.adjusted_qty);
    }).length;
    const added = proposals.filter((it) => !prev.has(dedupeKeyOf(it.amazon_sku))).length;

    // 0 の行は理由ごとに数える (全件は記録しない)
    const calmByReason = {};
    for (const c of calm) calmByReason[c.reason] = (calmByReason[c.reason] || 0) + 1;

    const status = (blocked.length || unmappedActive.length || unresolved
      || (inboundState && ['failed', 'stale_cache', 'empty'].includes(inboundState.source))) ? 'partial' : 'ok';
    const summary = [
      `run=${runId}`,
      `提案 ${proposals.length} 件 (新 ${added} / 数量変更 ${changed} / 消えた ${gone.length}${goneSame ? `, 同じ状態のまま ${goneSame}` : ''})`,
      `送らなくてよい ${calm.length} 件 [${Object.entries(calmByReason).map(([k, v]) => `${k}:${v}`).join(' ') || '-'}]`,
      `数量を出せない ${blocked.length} 件`,
      unmappedActive.length ? `未マップ(実績あり) ${unmappedActive.length} 件` : null,
      unresolved ? `Company DB に出品が無い ${unresolved} 件` : null,
      inboundState ? `準備中=${inboundState.source}${inboundState.reused_cache ? '(使い回し)' : ''}(${inboundState.count})` : null,
      `対象日 ${ctxBase.snapshotDate || '不明'} / 出どころ ${ctxBase.dataSource || '不明'}`,
    ].filter(Boolean).join(' / ');

    await db.query(
      `insert into ops.job_runs (job_id, host, started_at, finished_at, status, summary)
       values ($1,$2,$3,now(),$4,$5)`,
      [JOB_ID, host, startedAt, status, summary.slice(0, 2000)]);

    await db.query('commit');
    log(`影の下書き: ${summary}`);
    return {
      runId, ok: true, engineFailed: false, proposals: proposals.length, blocked: blocked.length,
      calm: calm.length, calmByReason, unmappedActive: unmappedActive.length,
      unresolved, added, changed, gone: gone.length, goneSame, status, summary,
    };
  } catch (e) {
    try { await db.query('rollback'); } catch { /* 接続が死んでいれば rollback も失敗する */ }
    // 🚨 中身は巻き戻すが、「この日は失敗した」という事実は残す (連続成功を数えられるように)
    if (await writeFailedRun(db, { host, startedAt, summary: `run=${runId} / 記録に失敗: ${e.message}`, log, openFresh })) onFailRecorded();
    throw e;
  }
}
