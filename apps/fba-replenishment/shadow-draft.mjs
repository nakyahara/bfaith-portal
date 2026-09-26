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
 * 🚨 「取れなかった」を 0 にしないのは **いちばん手前から** (Codex 2026-09-10 R1〜R3 で 3 回やり直した):
 *   レポートを読むところ (sp-api-reports.js) → 保存 (db.js) → データ結合 (mergeRestockWithPlanning) の
 *   どこか 1 つでも `|| 0` すると、以降どこでも二度と区別できない。3 段すべてで null を保つ。
 *   (Amazon 推奨数が先に持っていた正解 = parseIntOrNull + 保存で null 維持 を、販売数にも広げた)
 *
 *   区別できるもの: 販売数 30日/7日・PLANNING の行なし・倉庫の行なし (セットの構成も)・
 *   SKU 対応表に無い・対応表が壊れている・準備中数量が取れない・計算そのものの失敗
 *
 *   🚨 例外: 旧 daily_snapshots にフォールバックした日 (restock が空の日) は、元表が 0 埋めなので
 *   販売数の欠損を判定できない。その場合 `data_gaps.sales_*_missing` は **null** (false ではない) にして、
 *   「取れていた」と言い切らない。`sales_gaps_detectable: false` が付く
 *
 * 🚨 約束:
 *   - **数量を決めるのは今までどおり決定論的エンジン**。AI は何も決めない (3 層分担: 計算 / 制御 / 説明)
 *   - **記録するだけ**。ai.decisions に入れるだけで、納品プランも CSV も作らない。autonomy_level = 0
 *   - **Company DB 側で失敗しても業務を止めない** (二重書き期間の共通ルール)
 *   - **「送らなくてよい 0 個」と「計算できなかった」を混ぜない**。エンジンが `|| 0` で埋める前の姿
 *     (`item.data_gaps`) を見て分ける。ここが混ざると、このステップをやる意味が無くなる
 *   - **計算そのものが失敗した日・入力の関所に当たった日は「今日は何も要らない」と書かない**。
 *     ただし前日以前の提案も使えない状態 (superseded) にする (2026-09-24 変更。以前は残していたが、
 *     自動で使う段階では古い数量で送ってしまう。Codex 設計レビュー 2 High 2)
 *   - 毎朝ぜんぶ計算し直すので、前日の**未処理だけ** superseded にする (人が触った行は残す)
 *
 * 記録先:
 *   ai.decisions  ... 1 行 = 1 出品。提案 (proposal) と 提案不能・要対応 (finding)
 *   ops.job_runs  ... 1 行 = 1 回の実行 (成功も失敗も)。件数・データ品質・前日との差分を summary に
 */
import { normSku } from '../../lib/sku-norm.js';

/** 計算式の版。エンジンの規則を変えたら上げる (記録から「どの版の提案か」を追えるように) */
//   v2 (2026-09-24) = 倉庫在庫の配分 (自社出荷ぶんを残す・同じ NE 商品の取り合いを止める。PR #1434) + 入力の関所
export const RULE_VERSION = 'fba-reco-v2';
/** 決まりの版 (エンジンの opts.rules) → 記録する rule_version */
export const RULE_VERSION_OF = { v2: 'fba-reco-v2', v3: 'fba-reco-v3' };
export const DOMAIN = 'fba_replenishment';
/** 台帳の id。独立したスケジュールは作らず、既存の毎朝の同期に相乗りする */
export const JOB_ID = 'fba-daily-sync';
/** 提案が古くなったら承認できない (設計 §4-2: 48〜72 時間) */
export const EXPIRES_HOURS = 72;
/** この仕組みが作った行だけを差し替える印 (人が触った行・他の出どころを巻き込まない) */
export const GENERATOR = 'fba-shadow-draft';
export const COMPANY_ID = 1;
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
  // 自社出荷の日販が分からない構成品を使う SKU。自社ぶんの上限をかけられないので、数量を出さない
  //   (その構成品を使う SKU はまとめて保留になる。関係ない商品は止めない。Codex 2026-09-24 設計レビュー 2 High 3)
  if (g.self_sales_missing) return 'self_sales_unknown';
  return null;
}

/**
 * 入力の関所。どれか 1 つでも当たれば、その日は提案を出さない (「今日は決められない」だけ残す)。
 * 🚨 9/17 は準備中の取得が失敗したまま提案を出し、出荷待ちの SKU を 117 件もう一度提案した (自動なら二重納品)。
 *    9/19〜9/22 は Amazon のレポートが 9/17 のまま 4 日同じ提案を出した。「取れていない・古い」で決めない
 * @param {object} p
 * @param {object|null} p.inboundState     router の getInboundWorkingState() (準備中数量をどう手に入れたか)
 * @param {object|null} p.inputFreshness   db.getInputFreshness() (入力ごとの取り込み時刻)
 * @param {object} p.dq                    エンジンの data_quality
 * @param {Date} p.now
 * @returns {{ reasons: {code: string, detail: string}[] }}
 */
export const GATE_WAREHOUSE_MAX_HOURS = 36;
/** Amazon のレポートを取ってからの上限。miniPC は毎朝 7 時台に取る = 翌朝 6 時の下書きでも 23 時間。1 日取れなければ止まる */
export const GATE_REPORT_MAX_HOURS = 36;
export function inputGate({ inboundState, inputFreshness, dq = {}, now = new Date() }) {
  const reasons = [];
  const add = (code, detail) => reasons.push({ code, detail });
  // ① 準備中 (作成済みの納品プラン)。今回ちゃんと取れたときだけ (取れなかった・空・古いのを使い回した は止める)
  if (!inboundState || inboundState.source !== 'fresh') {
    add('inbound_working_not_fresh', `準備中の数量 = ${inboundState?.source || '不明'}${inboundState?.error ? ` (${String(inboundState.error).slice(0, 120)})` : ''}`);
  }
  // ② Amazon のレポート。計算が読む 2 つの表 (RESTOCK・PLANNING) の **それぞれ** の「元データを取った時刻」を見る。
  //   🚨 daily_snapshots の最新日だけでは、片方だけ取れた日・履歴は保存できて最新表の保存に失敗した日を通してしまう
  //   (Codex PR #1438 R1 High 1)。保存時刻 (updated_at) は Render が同期した時刻なので使わない
  for (const [name, at, missing] of [
    ['RESTOCK', inputFreshness?.restock_source_at, inputFreshness?.restock_source_missing],
    ['PLANNING', inputFreshness?.planning_source_at, inputFreshness?.planning_source_missing],
  ]) {
    const ms = at ? Date.parse(String(at).replace(' ', 'T') + 'Z') : NaN;   // UTC で保存されている
    if (!Number.isFinite(ms) || Number(missing) > 0 || now.getTime() - ms > GATE_REPORT_MAX_HOURS * 3600e3) {
      add('fba_report_stale', `${name} を取った時刻 = ${at || '不明'} (UTC)${Number(missing) > 0 ? ` / 取得時刻の無い行 ${missing}` : ''}`);
    }
  }
  // ③ 倉庫在庫。手動 CSV なら取り込み時刻 (このプロセスの localtime で保存)、ロジザードの写しで計算した日は
  //   写しの「在庫を取った時刻」(ISO、warehouse_source_at) で見る。36 時間より古い・未来 なら止める
  const whSrc = inputFreshness?.warehouse_source_at || null;
  const wh = whSrc || inputFreshness?.warehouse_uploaded_at || null;
  const whMs = whSrc ? Date.parse(whSrc) : (wh ? new Date(String(wh).replace(' ', 'T')).getTime() : NaN);
  if (!Number.isFinite(whMs) || now.getTime() - whMs > GATE_WAREHOUSE_MAX_HOURS * 3600e3 || whMs > now.getTime() + 60e3) {
    add('warehouse_stale', `倉庫在庫の取り込み = ${wh || 'なし'}`);
  }
  // ④ 自社出荷の日販 (商品管理リスト)。自社ぶんを残す設定なのに使えない = 倉庫を丸ごと FBA に回す計算になる
  const al = dq.allocation;
  if (al && al.mode === 'equal_days' && !al.self_sales?.used) {
    add('self_sales_unavailable', `自社日販 = ${al.self_sales?.status || '不明'}${al.self_sales?.error ? ` (${al.self_sales.error})` : ''}`);
  }
  // ⑤ 出荷待ちの FBA 伝票を数えられない = 同じ在庫を二度配りうる
  if (al && ['error', 'no_warehouse'].includes(al.pending_slips?.status)) {
    add('pending_slips_unknown', `出荷待ちの FBA 伝票 = ${al.pending_slips.status}${al.pending_slips.error ? ` (${al.pending_slips.error})` : ''}`);
  }
  return { reasons };
}

/**
 * 0 にした理由を SKU ごとに残す対象と、その中身 (段階ごとの数量)。
 * 🚨 9/13〜9/19 に人が足した 254 件のうち 225 件は「計算して 0」だったが、理由が件数しか残っておらず追えなかった。
 *    長期欠品は 30 日販売 0 なので、販売ありだけに絞ると追えない (Codex 設計レビュー 2 Medium 8)
 */
export function zeroReasonsOf(calm) {
  const out = [];
  for (const { item: it, reason } of calm) {
    const sold = num(it.units_sold_30d) > 0;
    const reco = num(it.amazon_recommended_qty) > 0;
    const oosWithStock = ['revivable_long_oos', 'dead_candidate'].includes(it.stock_state) && num(it.warehouse_available) > 0;
    if (!sold && !reco && !oosWithStock) continue;
    const al = it.allocation || null;
    out.push({
      sku: it.amazon_sku,
      reason,
      state: it.stock_state || null,
      eff: num(it.effective_fba_stock),               // 実質 FBA 在庫 (販売可 + 輸送中 + 受領中 + 準備中)
      working: num(it.fba_inbound_working_effective),
      daily: num(it.daily_sales),
      dos: num(it.days_of_supply),
      rp_days: num(it.reorder_point_days),
      rp: num(it.reorder_point),
      target_days: num(it.target_days),
      need: num(it.raw_needed_before_amazon_cap ?? it.raw_needed),   // ① 自社の理論値
      reco: num(it.amazon_recommended_qty),                          // ② Amazon の推奨 (これで頭打ち)
      reco_capped: !!it.amazon_reco_capped,
      wh: num(it.warehouse_available),                               // ③ 倉庫
      expiry_same: it.expiry_limited ? num(it.expiry_same_qty) : null,
      before_alloc: al ? al.before : null,                            // ④ 配分の前 → 後
      self_cut: al ? al.self_cut : null,
      shared_cut: al ? al.shared_cut : null,
      min_days_cut: al ? al.min_days_cut : null,
      skipped_min_days: !!it.skipped_min_days,
    });
  }
  return out;
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
  // 倉庫在庫の配分で 0 にした行 (2026-09-24)。「自社に在庫が無い」とは別の理由として残す
  const al = item.allocation;
  if (al && al.before > 0 && al.after === 0) {
    if (al.self_cut > 0 && al.self_cut >= al.shared_cut) return 'self_reserve';   // 自社出荷ぶんを残した
    if (al.shared_cut > 0) return 'shared_stock';                                  // 同じ商品を使う他の SKU に先に配った
  }
  if (item.skipped_min_days) return 'skipped_min_days';                     // 最低出荷日数に満たない
  if (!item.needs_replenishment) return 'above_reorder_point';              // まだ発注点を下回っていない
  if (num(item.warehouse_available) === 0) return 'no_warehouse_stock';     // 自社に在庫が無い (取れている上での 0)
  // 自社の理論値はあるのに、Amazon の推奨が 0 で頭打ちされた (Amazon の推奨に合わせる規則の効きめを見る)
  if (item.amazon_reco_capped && num(item.amazon_recommended_qty) === 0) return 'amazon_reco_zero';
  if (item.expiry_limited && num(item.expiry_same_qty) === 0) return 'expiry_zero';   // 同じ期限で送れる在庫が無い
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
  const al = it.allocation;
  if (al?.self_cut > 0) {
    const days = (al.units || []).map((u) => u.equal_days).filter((d) => d !== null);
    parts.push(`自社出荷ぶんを残して ${al.self_cut} 個減らした${days.length ? ` (FBA と自社が約 ${Math.min(...days)} 日分でそろう)` : ''}`);
  }
  if (al?.shared_cut > 0) parts.push(`同じ商品を使う他の SKU に先に配って ${al.shared_cut} 個減らした`);
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
    location_inputs: it.location_inputs || null,   // 期限・置き場の補正に使った棚 (補正後の数量の再現に要る)
    per_unit_volume: num(it.per_unit_volume),
    is_seasonal: it.is_seasonal || null,
    season_name: it.season_name || null,
    recommended_qty: num(it.recommended_qty),      // 丸める前
    rounded_qty: num(it.rounded_qty),              // 入数で丸めたあと
    adjusted_qty: num(it.adjusted_qty),            // 置き場の都合まで見たあと = 画面の「補正後」
    allocation: it.allocation || null,             // 倉庫在庫の配分 (自社出荷ぶん・他 SKU との取り合い) の前後と材料
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
export async function writeFailedRun(db, { host, startedAt, summary, log = () => {}, openFresh = null, jobId = JOB_ID }) {
  const sql = `insert into ops.job_runs (job_id, host, started_at, finished_at, status, summary)
               values ($1,$2,$3,now(),'fail',$4)`;
  const params = [jobId, host, startedAt, String(summary).slice(0, 2000)];
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

/** この仕組みが作った未処理 (new) の行を全部 superseded にする。失敗しても投げない (呼び出し側が記録する) */
async function supersedeOpenRows(db, log) {
  try {
    await db.query(
      `update ai.decisions set status = 'superseded'
        where company_id = $1 and domain = $2 and status = 'new' and inputs_ref->>'generator' = $3`,
      [COMPANY_ID, DOMAIN, GENERATOR]);
    return true;
  } catch (e) {
    log(`前日以前の提案を無効にできなかった: ${e.message}`);
    return false;
  }
}

/**
 * 前日以前の提案を使えない状態にする。今の接続で駄目なら (接続が死んでいる・rollback 直後など) 別の接続を開いて。
 * 🚨 失敗・止めた日に前日の提案が new のまま残ると、自動で使う段階で古い数量を送ってしまう
 *    (Codex 設計レビュー 2 High 2 / PR #1438 R1 High 2)。成否を返すので、呼び出し側は結果を記録に残す
 */
export async function supersedeOpenRowsSafely(db, { openFresh = null, log = () => {} } = {}) {
  if (db && await supersedeOpenRows(db, log)) return true;
  if (!openFresh) return false;
  let fresh = null;
  try {
    fresh = await openFresh();
    return await supersedeOpenRows(fresh.db, log);
  } catch (e) {
    log(`別の接続でも前日以前の提案を無効にできなかった: ${e.message}`);
    return false;
  } finally {
    if (fresh?.close) { try { await fresh.close(); } catch { /* もう閉じている */ } }
  }
}

/**
 * 最初の接続に失敗した日。別の接続を開いて、前日以前の提案を無効にしてから「失敗」を記録する。
 * 🚨 失敗の記録だけ書いて無効化を忘れると、前日の提案が使える状態で残る (Codex PR #1438 R2 High)
 * @returns {Promise<{ superseded: boolean, recorded: boolean }>}
 */
export async function recordConnectFailure({ error, openFresh, log = () => {}, host = 'render', startedAt = new Date().toISOString(), jobId = JOB_ID }) {
  const superseded = await supersedeOpenRowsSafely(null, { openFresh, log });
  const summary = `Company DB に接続できない: ${error?.message || error}${superseded ? '' : ' / 🚨 前日以前の提案を無効にできなかった'}`;
  const recorded = await writeFailedRun({ query: async () => { throw error; } }, { host, startedAt, summary, log, openFresh, jobId });
  return { superseded, recorded };
}

/**
 * 入力の関所に当たった日の記録。提案は出さない。前日以前の提案も superseded (使えない) にする。
 * 残すもの = 「今日は決められない」理由・0 の理由 (参考)・データ品質・入力の取り込み時刻
 */
async function recordGatedRun(db, { runId, startedAt, now, host, log, openFresh, onFailRecorded, gate, result, dq, items, inboundState, settings, inputFreshness, jobId, runMeta, beforeCommit, ruleVersion = RULE_VERSION }) {
  const { calm } = pickDraftRows(items);
  const codes = gate.reasons.map((r) => r.code);
  const summary = [
    `run=${runId}`,
    `今日は決められない: ${gate.reasons.map((r) => `${r.code} (${r.detail})`).join(' / ')}`,
    '提案 0 件 (前日以前の提案も無効にした)',
    inboundState ? `準備中=${inboundState.source}(${inboundState.count})` : null,
    `対象日 ${result?.snapshot_date || '不明'}`,
  ].filter(Boolean).join(' / ');
  // 🚨 前日以前の提案の無効化は、要約の記録とは別に **先に** 確定させる。同じトランザクションに入れると、
  //    要約の INSERT が失敗したときに無効化まで巻き戻って古い提案が生き返る (Codex PR #1438 R1 High 2)
  const superseded = await supersedeOpenRowsSafely(db, { openFresh, log });
  if (!superseded) {
    const s2 = `${summary} / 🚨 前日以前の提案を無効にできなかった`;
    if (await writeFailedRun(db, { host, startedAt, summary: s2, log, openFresh, jobId })) onFailRecorded();
    throw new Error('前日以前の提案を無効にできなかった (決められない日)');
  }
  try {
    await db.query('begin');
    await db.query(
      `insert into ai.decisions
         (company_id, domain, decision_kind, subject_type, subject_id, summary, rationale, severity,
          proposed_action, inputs_ref, model, rule_version, generated_by, autonomy_level, status, dedupe_key, expires_at)
       values ($1,$2,'finding',null,null,$3,$4,'warn',null,$5,$6,$7,'rule',0,'new',$8,$9)`,
      [COMPANY_ID, DOMAIN,
        `${result?.snapshot_date || '日付不明'} は決められない (${codes.join(', ')})`,
        '入力が取れていない・古いので、この日は提案を出さない。前日以前の提案も使えない状態にした',
        {
          run_id: runId, generator: GENERATOR, run_summary: true, gated: true, gate,
          calculated_at: result?.generated_at || startedAt, data_as_of: result?.snapshot_date || null,
          data_source: dq.data_source || null, inbound_working_state: inboundState || null,
          settings: settings || null, data_quality: dq, input_freshness: inputFreshness,
          zero_reasons: zeroReasonsOf(calm),
          ...(runMeta || {}),
        },
        `rule:${ruleVersion}`, ruleVersion, RUN_SUMMARY_KEY,
        new Date(now.getTime() + EXPIRES_HOURS * 3600 * 1000).toISOString()]);
    await db.query(
      `insert into ops.job_runs (job_id, host, started_at, finished_at, status, summary)
       values ($1,$2,$3,now(),'partial',$4)`,
      [jobId, host, startedAt, summary.slice(0, 2000)]);
    if (beforeCommit) await beforeCommit();   // 投げたら巻き戻す (時間切れなど)
    await db.query('commit');
  } catch (e) {
    try { await db.query('rollback'); } catch { /* 接続が死んでいれば rollback も失敗する */ }
    if (await writeFailedRun(db, { host, startedAt, summary: `run=${runId} / 記録に失敗: ${e.message}`, log, openFresh, jobId })) onFailRecorded();
    throw e;
  }
  log(`影の下書き: ${summary}`);
  return { runId, ok: true, gated: true, reasons: gate.reasons, engineFailed: false, proposals: 0, blocked: 0, calm: calm.length, status: 'partial', summary };
}

/**
 * 影の下書きを 1 回ぶん記録する。
 * @param db     { query(sql, params) }
 * @param result 計算エンジンの戻り値 (generateRecommendations(false, inboundOverride))
 */
export async function recordShadowDraft(db, result, {
  host = 'render', log = () => {}, now = new Date(), inboundState = null, settings = null, openFresh = null,
  onFailRecorded = () => {}, inputFreshness = null, gate = null,
  // 9:40 の自動決定 (decision-job.js) が渡す: 記録する ops.job_runs の job_id・試行を始めた時刻・
  //   run 要約行に足す情報 (business_date / decision_final / 倉庫在庫の出どころと手動 CSV との差 など)
  jobId = JOB_ID, startedAt: startedAtOpt = null, runMeta = null,
  // 確定 (commit) の直前に呼ぶ。投げたら巻き戻して失敗として記録する (9:40 の自動決定の時間切れ。Codex PR #1455 R1 Medium)
  beforeCommit = null,
  // どの決まりで計算した提案か (rule_version 列)。既定は v2。9:40 の自動決定は v3 を渡す (決まりの変更 v3-1)
  ruleVersion = RULE_VERSION,
} = {}) {
  const runId = newShadowRunId(now);
  const startedAt = startedAtOpt || now.toISOString();
  const items = Array.isArray(result?.items) ? result.items : [];
  const errors = Array.isArray(result?.errors) ? result.errors.filter(Boolean) : [];
  const dq = result?.data_quality || {};

  // 🚨 計算そのものが失敗した日 (スナップショットが無い・マッピングが無い) は「今日は何も要らない」ではない。
  //    ただし前日以前の提案を「今日も使える」状態に残してもいけない (自動で使うと古い数量で送る。
  //    Codex 2026-09-24 設計レビュー 2 High 2) → この仕組みの未処理の行は superseded にして、失敗を記録する
  if (errors.length) {
    const superseded = await supersedeOpenRowsSafely(db, { openFresh, log });
    const summary = `run=${runId} / 計算できなかった: ${errors.join(' / ')}${superseded ? '' : ' / 🚨 前日以前の提案を無効にできなかった'}`;
    if (await writeFailedRun(db, { host, startedAt, summary, log, openFresh, jobId })) onFailRecorded();
    log(`影の下書き: ${summary}`);
    return { runId, ok: false, engineFailed: true, errors, proposals: 0, blocked: 0, calm: 0, status: 'fail', summary };
  }

  // 🚨 入力の関所に当たった日は、提案を 1 件も出さない。前日以前の提案も使えない状態にし、
  //    「今日は決められない (理由)」と、0 の理由・データ品質だけ残す
  if (gate && Array.isArray(gate.reasons) && gate.reasons.length) {
    return recordGatedRun(db, { runId, startedAt, now, host, log, openFresh, onFailRecorded, gate, result, dq, items, inboundState, settings, inputFreshness, jobId, runMeta, beforeCommit, ruleVersion });
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

    // 前日は出ていたのに今日は出てこない行 (送らなくてよくなった / 対象から外れた) を、差し替えの**前**に決める。
    // 🚨 0 の行は全部は記録しない (毎日 7,000 行では読めない) が、前回から状態が変わったものだけは残す
    const todayKeys = new Set([
      ...proposals.map((i) => dedupeKeyOf(i.amazon_sku)),
      ...blocked.map((b) => dedupeKeyOf(b.item.amazon_sku)),
      ...unmappedActive.map((u) => dedupeKeyOf(u.sku)),
    ]);
    const calmByKey = new Map(calm.map((c) => [dedupeKeyOf(c.item.amazon_sku), c]));
    const goneAll = [...prev.entries()].filter(([k]) => !todayKeys.has(k));
    const isSameGone = ([key, p]) => {
      const c = calmByKey.get(key);
      const nowReason = c ? c.reason : 'not_in_items';
      return p.kind === 'finding' && !!p.calmReason && p.calmReason === nowReason;
    };
    const gone = goneAll.filter((e) => !isSameGone(e));
    // 🚨 同じ状態のままの行は **書き直さず、差し替えもしない (開いたまま持ち越す)**。
    //    差し替えてしまうと翌日に比較元が無くなり、「補充不要 → 同じ → 廃番候補」の変化を見落とす (Codex R4)
    const keepKeys = goneAll.filter(isSameGone).map(([k]) => k);
    const goneSame = keepKeys.length;

    // 🚨 差し替えるのは **この仕組みが作った未処理の行だけ** (持ち越す行を除く)。
    //    人が見始めた行 (reviewable) や承認済み、他の出どころの行は触らない。
    // ⚠️ 次の段階 (人のレビューを始めるとき) の宿題: 比較元の取得は status='new' だけを見ているので、
    //    行が reviewable に移るとそこから先の変化を比べられない。そのときは「比較元を引く」と
    //    「差し替えてよい行を選ぶ」を分けること (Codex R5 軽微。影の段階は人が触らないので影響なし)
    await db.query(
      `update ai.decisions set status = 'superseded'
        where company_id = $1 and domain = $2 and status = 'new' and inputs_ref->>'generator' = $3
          and (dedupe_key is null or not (dedupe_key = any($4::text[])))`,
      [COMPANY_ID, DOMAIN, GENERATOR, keepKeys]);

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
    // 🚨 inputs_ref に直接入れるときは、出品ごとの行 (inputsOf) と **同じ名前** にそろえる。
    //    runId のまま入れると、run_id で行どうしをつなげない (Codex R4 の試験で発覚)
    const runRef = {
      run_id: runId,
      calculated_at: ctxBase.calculatedAt,
      data_as_of: ctxBase.snapshotDate,
      data_source: ctxBase.dataSource,
      inbound_working_state: inboundState || null,
    };

    const ins = async (row) => {
      await db.query(
        `insert into ai.decisions
           (company_id, domain, decision_kind, subject_type, subject_id, summary, rationale, severity,
            proposed_action, inputs_ref, model, rule_version, generated_by, autonomy_level, status, dedupe_key, expires_at)
         values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'rule',0,'new',$13,$14)`,
        [COMPANY_ID, DOMAIN, row.kind, row.subjectType, row.subjectId, row.summary, row.rationale, row.severity,
          row.proposedAction, { ...row.inputs, generator: GENERATOR }, `rule:${ruleVersion}`, ruleVersion, row.dedupeKey, row.expiresAt]);
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
        inputs: { ...runRef, amazon_sku: u.sku, blocked_reason: 'unmapped_active', unmapped: u, listing_resolved: listingId !== null, prev_qty: prev.get(key)?.qty ?? null },
        dedupeKey: key,
        expiresAt,
      });
    }

    // 状態が変わった「消えた行」だけを書く (同じ状態のままの行は上で持ち越し済み)
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
          : { ...runRef, amazon_sku: key.split(':')[1], calm_reason: 'not_in_items', prev_qty: p.qty, prev_kind: p.kind, listing_resolved: false },
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
        ...runRef,
        settings: settings || null,
        run_summary: true,
        counts: { proposals: proposals.length, blocked: blocked.length, calm: calm.length, unmapped_active: unmappedActive.length, unmapped_inactive: unmappedInactive.length },
        data_quality: dq,
        engine_generated_at: result?.generated_at || null,
        // 入力ごとの取り込み時刻と行数 (PLANNING だけ古い日 などを見分ける)。各行とは run_id でつながる
        input_freshness: inputFreshness,
        gate: gate || null,
        // 0 にした理由を SKU ごとに (段階ごとの数量つき)。1 SKU 1 行にはしない = ai.decisions を膨らませない
        zero_reasons: zeroReasonsOf(calm),
        ...(runMeta || {}),
      },
      dedupeKey: RUN_SUMMARY_KEY,
      expiresAt,
    });

    // 前回が「提案」だったものだけを数量の変化として数える。前回が不能・消えた記録なら「新しく上がった」
    const changed = proposals.filter((it) => {
      const p = prev.get(dedupeKeyOf(it.amazon_sku));
      return p && p.kind === 'proposal' && p.qty !== num(it.adjusted_qty);
    }).length;
    const added = proposals.filter((it) => {
      const p = prev.get(dedupeKeyOf(it.amazon_sku));
      return !p || p.kind !== 'proposal';
    }).length;

    // 0 の行は理由ごとに数える (全件は記録しない)
    const calmByReason = {};
    for (const c of calm) calmByReason[c.reason] = (calmByReason[c.reason] || 0) + 1;

    const status = (blocked.length || unmappedActive.length || unresolved
      || (inboundState && ['failed', 'stale_cache', 'empty', 'inconsistent'].includes(inboundState.source))) ? 'partial' : 'ok';
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
      [jobId, host, startedAt, status, summary.slice(0, 2000)]);

    if (beforeCommit) await beforeCommit();   // 投げたら下の catch で巻き戻す
    await db.query('commit');
    log(`影の下書き: ${summary}`);
    return {
      runId, ok: true, engineFailed: false, proposals: proposals.length, blocked: blocked.length,
      calm: calm.length, calmByReason, unmappedActive: unmappedActive.length,
      unresolved, added, changed, gone: gone.length, goneSame, status, summary,
    };
  } catch (e) {
    try { await db.query('rollback'); } catch { /* 接続が死んでいれば rollback も失敗する */ }
    // 🚨 中身は巻き戻すが、「この日は失敗した」という事実は残す (連続成功を数えられるように)。
    //    巻き戻しで前日の提案が new のまま残るので、別に無効化する (Codex PR #1438 R1 High 2)
    const superseded = await supersedeOpenRowsSafely(db, { openFresh, log });
    const s2 = `run=${runId} / 記録に失敗: ${e.message}${superseded ? '' : ' / 🚨 前日以前の提案を無効にできなかった'}`;
    if (await writeFailedRun(db, { host, startedAt, summary: s2, log, openFresh, jobId })) onFailRecorded();
    throw e;
  }
}
