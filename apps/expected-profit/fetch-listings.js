/**
 * 商品別 想定利益 — 出品列挙 + 価格取得 (miniPC で動く)
 *
 * 正本 = AI_reference『商品別想定利益_要件定義_20260907.md』§5 / §15-4
 *
 * 🚨 母集団は「出品列挙の結果」。売れた実績や手数料キャッシュを起点にしない (§5.1)。
 *    起点を間違えると「売れていない商品が出ない」という元の問題が再発する。
 *
 * 🚨 部分列挙で見つからないことを、削除・未出品の根拠にしない (§5.2.1)。
 *    今夜が partial なら前回の完全集合と UNION し、見つからなかった行は前回の期限を引き継ぐ。
 *
 * 使い方:
 *   node apps/expected-profit/fetch-listings.js            (全モール)
 *   node apps/expected-profit/fetch-listings.js --mall amazon
 */
import { getExpectedProfitDB, initExpectedProfitDB } from './db.js';
import { newRunId, nowIso, addDays } from './util.js';
import { archiveItems } from '../../scripts/mall-items/archive-items.mjs';

// 失効期限 (§15-8)
const LISTING_ENUM_VALID_DAYS = 7;
const PRICE_VALID_DAYS = 3;

/** 前回の完全集合から消えた率がこれを超えたら partial 扱い (レポート破損の疑い) */
const DISAPPEARED_RATIO_LIMIT = 0.20;

const AMAZON_SHOP_ID = () =>
  `${process.env.SP_API_SELLER_ID || 'unknown'}@${process.env.SP_API_MARKETPLACE_ID || 'A1VC38T7YXB528'}`;
const RAKUTEN_SHOP_ID = () => process.env.RAKUTEN_SHOP_CODE || '1';

/** 整数円として読めた時だけ返す (price-update の toIntPrice と同じ規約) */
export function toIntPrice(v) {
  if (typeof v === 'number') return Number.isInteger(v) ? v : null;
  if (typeof v !== 'string') return null;
  const s = v.trim().replace(/,/g, '');
  if (!/^\d+$/.test(s)) return null;
  const n = Number(s);
  return Number.isSafeInteger(n) ? n : null;
}

/** Amazon 出品レポートの「ステータス」列 → listing_status */
export function amazonListingStatus(raw) {
  const s = String(raw || '').trim().toLowerCase();
  if (s === 'active') return 'active';
  if (s === 'inactive') return 'inactive';
  if (s === 'incomplete') return 'incomplete';
  return 'unknown';
}

/**
 * フルフィルメント・チャンネル列 → FBA / FBM。
 * 🚨 欠損・未知を FBM に倒さない (Codex R3)。FBA の行を FBM として計算すると
 *    配送費 (fba_fee か自社配送関係費か) も費用範囲も変わり、利益の符号と順位が狂う
 */
export function amazonFulfillment(raw) {
  const s = String(raw ?? '').trim().toUpperCase();
  if (!s) return null;                                   // 列が無い / 空 = 未解決
  if (s.includes('AMAZON') || s.startsWith('AFN')) return 'FBA';
  if (s === 'DEFAULT' || s.startsWith('MFN') || s.includes('MERCHANT')) return 'FBM';
  return null;                                           // 未知の値も未解決 (勝手に決めない)
}

/** レポート行 → snapshot 行 (純関数。テストで固定する) */
/**
 * Amazon の出品が「送料込み」か。
 *
 * 🚨 FBA はプライム配送なので常に送料込み。
 * 🚨 FBM (自社出荷) は**配送パターン**で決まる (中原さん決定 2026-09-08)。
 *    実測 (7,597 出品): FBM 3,466 件のうち
 *      ネコポスマケプレプライム設定             3,377
 *      移行された配送パターン                     72
 *      ヤマト北海道・沖縄送料別途設定               11
 *      Selected Self-Ship Template…               3
 *      プライム配送パターン                        3
 *    マケプレプライムは**プライム会員への配送料無料が条件** (沖縄・北海道・離島を除く)。
 *    標準シナリオ (本州・1個・通常購入) では送料込みとして扱う。
 *    → 中原さんの決定 = **送料込みとして扱う**。楽天と同じ扱いに揃える。
 *
 * 🚨 知らない配送パターンは **null (不明)** にする。勝手に送料込みへ倒さない。
 *    新しい配送パターンを作ったときに、黙って利益を高く見せないため。
 */
/**
 * 🚨 ここに載せてよいのは「**プライム扱い = 配送料無料が条件**」と根拠が言えるものだけ。
 *    「移行された配送パターン」(72件) は名前からは何も分からないので**入れない** (Codex R12)。
 *    根拠なく入れると、送料を別途もらっている出品の利益を高く見せてしまう。
 *    → 参考値 (`shipping_revenue_unknown`) のままにして、中原さんの確認を待つ。
 */
export const AMAZON_POSTAGE_INCLUDED_GROUPS = [
  'ネコポスマケプレプライム設定',                                  // 実測 3,377 件
  'プライム配送パターン',                                          // 実測 3 件
  'Selected Self-Ship Templateネコポスマケプレプライム設定',       // 実測 3 件
];

export function amazonPostageIncluded(fulfillment, shippingGroup) {
  if (fulfillment === 'FBA') return true;
  if (fulfillment !== 'FBM') return null;              // 出荷区分が未解決なら判断しない
  const g = String(shippingGroup ?? '').trim();
  if (!g) return null;                                  // 配送パターンが読めない
  if (AMAZON_POSTAGE_INCLUDED_GROUPS.includes(g)) return true;
  return null;                                          // 知らないパターンは不明のまま
}

export function amazonRowToSnapshot(row, { runId, shopId, fetchedAt, validUntil }) {
  const sku = (row['出品者SKU'] || row['seller-sku'] || '').trim();
  const asin = (row['商品ID'] || row['asin1'] || '').trim();
  const price = toIntPrice(row['価格'] ?? row['price']);
  const channel = row['フルフィルメント・チャンネル'] ?? row['fulfillment-channel'];
  const fulfillment = amazonFulfillment(channel);
  // 🚨 「明示的な0」と「列そのものが無い/読めない」を分ける (Codex R1-3)。
  //    ここで 0 に倒すと、ポイント付き出品を誤った条件で見積もってしまう
  const shippingGroup = row['merchant-shipping-group'] ?? row['配送パターン'];
  const postageIncluded = amazonPostageIncluded(fulfillment, shippingGroup);
  const pointsRaw = row['ポイント'] ?? row['points'];
  const hasPointsColumn = pointsRaw !== undefined;
  const points = hasPointsColumn && String(pointsRaw).trim() === '' ? 0 : toIntPrice(pointsRaw);
  return {
    run_id: runId,
    mall: 'amazon',
    shop_id: shopId,
    mall_item_key: sku,
    mall_item_ref: asin,                 // 手数料見積の入力キー (ASIN 単位で引く)
    fulfillment,                         // null = 未解決 (見積も計算も通さない)
    ne_code: null,                       // 対応付けは build 側で行う
    price_type: 'normal',
    price_incl_tax: price,
    price_tax_included: 1,               // Amazon の出品価格は税込 (決済の Principal + Tax と一致)
    price_raw: price,
    mall_tax_rate: null,                 // Amazon は税率を返さない (商品マスタ側を使う)
    // 送料込みかどうかは配送パターンで決める (上の amazonPostageIncluded を見よ)
    postage_included: postageIncluded == null ? null : (postageIncluded ? 1 : 0),
    postage_revenue_incl_tax: postageIncluded === true ? 0 : null,
    shipping_group: shippingGroup == null ? null : String(shippingGroup).trim() || null,
    points,                              // null = 取得不能 (見積入力未解決として扱う)
    listing_status: amazonListingStatus(row['ステータス'] || row['status']),
    fetch_status: price == null ? 'not_found' : 'ok',
    resolve_status: 'unresolved',        // build 側で解決する
    resolve_reason: null,
    valid_until: validUntil,
    source: 'merchant_listings_all_data',
    fetched_at: fetchedAt,
  };
}

/** 楽天 items/search の 1 商品 → snapshot 行 (variant ごとに1行) */
export function rakutenItemToSnapshots(item, { runId, shopId, fetchedAt, validUntil }) {
  const r = rakutenItemToSnapshotsDetailed(item, { runId, shopId, fetchedAt, validUntil });
  return r.rows;
}

/**
 * 変換結果と「解析できなかった variant の数」を一緒に返す。
 * 🚨 行が1つでもできれば OK とすると、壊れた variant が静かに消えて
 *    欠落した集合が次の「完全集合」になる (Codex R3)
 */
export function rakutenItemToSnapshotsDetailed(item, { runId, shopId, fetchedAt, validUntil }) {
  const manageNumber = String(item?.manageNumber || '').trim();
  if (!manageNumber) return { rows: [], unparsable: 1 };
  // 🚨 variants は「SKU管理番号 → variant」のオブジェクト。
  //    配列で来たら添字が SKU 管理番号になってしまうので、解析失敗として扱う (Codex R2)
  const v0 = item?.variants;
  if (!v0 || typeof v0 !== 'object' || Array.isArray(v0)) return { rows: [], unparsable: 1 };
  const variants = v0;
  let unparsable = 0;
  // 🚨 payment は **item レベル** にある (variant には無い)。
  //    実データで確認: item のキー = manageNumber, ..., payment, ..., variants
  //    ここを variant から読むと、全出品が「税区分が不明」になって1件も計算できない
  const payment = item?.payment && typeof item.payment === 'object' ? item.payment : null;
  const itemTaxIncluded = payment?.taxIncluded;
  const itemTaxRate = payment?.taxRate != null ? Number(payment.taxRate) : null;
  const hideItem = item?.hideItem === true;
  const out = [];
  for (const [variantKey, v] of Object.entries(variants)) {
    // 要素が object でない / キーが空 は解析不能 (行を作らない = 呼び出し側が unparsable に数える)
    if (!variantKey || !v || typeof v !== 'object' || Array.isArray(v)) { unparsable++; continue; }
    const price = toIntPrice(v?.standardPrice);            // 🚨 文字列で返る ("1080")
    // 税区分は item レベル。variant 側にあれば (details-bulk 等) そちらを優先する
    const taxRate = v?.payment?.taxRate != null ? Number(v.payment.taxRate) : itemTaxRate;
    // 🚨 standardPrice が税込とは限らない (Codex R1-5)。taxIncluded を確認し、
    //    税抜登録や不明な区分は「価格が読めなかった」扱いにして、後段で税を二重に割り戻さない
    const taxIncluded = v?.payment?.taxIncluded ?? itemTaxIncluded;
    const postageIncluded = typeof v?.shipping?.postageIncluded === 'boolean' ? v.shipping.postageIncluded : null;
    const singleItemShipping = toIntPrice(v?.shipping?.singleItemShipping);
    const hidden = hideItem || v?.hidden === true;
    out.push({
      run_id: runId,
      mall: 'rakuten',
      shop_id: shopId,
      mall_item_key: `${manageNumber}/${variantKey}`,
      mall_item_ref: v?.merchantDefinedSkuId || null,
      fulfillment: 'self',
      ne_code: null,
      price_type: 'normal',
      price_incl_tax: taxIncluded === true ? price : null,
      price_tax_included: taxIncluded === true ? 1 : (taxIncluded === false ? 0 : null),
      price_raw: price,                                   // 元の値は残す (調査用)
      mall_tax_rate: Number.isFinite(taxRate) ? taxRate : null,
      postage_included: postageIncluded == null ? null : (postageIncluded ? 1 : 0),
      // 送料込みなら収入 0。別途なら singleItemShipping (取れなければ NULL = unknown)
      postage_revenue_incl_tax: postageIncluded === true ? 0 : singleItemShipping,
      points: 0,
      listing_status: hidden ? 'hidden' : 'active',
      fetch_status: price == null ? 'not_found'
        : (taxIncluded === true ? 'ok' : 'tax_included_unknown'),
      resolve_status: 'unresolved',
      resolve_reason: null,
      valid_until: validUntil,
      shipping_group: null,              // 楽天に配送パターンの概念は無い
      source: 'rms_items_search',
      fetched_at: fetchedAt,
    });
  }
  return { rows: out, unparsable };
}

/**
 * 前回の完全集合と突き合わせて、消えた出品の数と partial 判定を返す (§5.2.1)。
 * 🚨 消えた率が大きいときはレポート破損の疑いなので partial にする。
 *    partial のときは呼び出し側が前回集合と UNION する
 */
export function evaluateEnumeration(previousKeys, currentKeys) {
  // 🚨 0 件を正常として通さない (§7.1)。
  //    「正常に全件列挙した結果の 0 件」と「API がおかしい」を外形では区別できない。
  //    B-Faith が全モールで出品ゼロになることは現実に起きないので、異常として扱う。
  //    (前回集合が無い初回でも同じ。ここを ok にすると、空レポートで世代が全消えする)
  if (!currentKeys || currentKeys.size === 0) {
    return { status: 'failed', disappeared: previousKeys?.size || 0, ratio: 1, reason: 'empty_enumeration' };
  }
  if (!previousKeys || previousKeys.size === 0) {
    return { status: 'ok', disappeared: 0, ratio: 0 };   // 初回は比較対象が無い
  }
  let disappeared = 0;
  for (const k of previousKeys) if (!currentKeys.has(k)) disappeared++;
  const ratio = disappeared / previousKeys.size;
  return {
    status: ratio > DISAPPEARED_RATIO_LIMIT ? 'partial' : 'ok',
    disappeared,
    ratio,
  };
}

/**
 * 解析失敗・重複キーがあれば列挙を partial に落とす (Codex R1-2)。
 * 🚨 20%判定は「前回と比べて消えた」を見る補助的な異常検知であって、
 *    解析が成功したことの代わりにはならない。1行でも読めなければ完全集合を名乗らせない
 */
export function enumStatusWithParseFailures(evalResult, unparsable, duplicates) {
  if (evalResult.status === 'failed') return { ...evalResult, unparsable, duplicates };
  if (unparsable > 0 || duplicates > 0) {
    return { ...evalResult, status: 'partial', unparsable, duplicates, reason: 'parse_failure' };
  }
  return { ...evalResult, unparsable, duplicates };
}

function enumSummary(evalResult) {
  if (evalResult.reason === 'empty_enumeration') return '出品が0件で返った (API異常の疑い。0件を正常として通さない)';
  if (evalResult.reason === 'parse_failure') {
    return `解析できない行 ${evalResult.unparsable} 件 / 重複キー ${evalResult.duplicates} 件 (完全集合として扱わない)`;
  }
  return null;
}

// ────────────────────────────────────────────────────────────
// 実行 (I/O を伴う部分)
// ────────────────────────────────────────────────────────────

function insertSnapshots(db, rows) {
  const stmt = db.prepare(`
    INSERT OR REPLACE INTO mall_price_snapshot
      (run_id, mall, shop_id, mall_item_key, mall_item_ref, fulfillment, ne_code, price_type, price_incl_tax,
       price_tax_included, price_raw, mall_tax_rate, postage_included, postage_revenue_incl_tax, points, listing_status,
       shipping_group,
       fetch_status, resolve_status, resolve_reason, valid_until, source, fetched_at)
    VALUES
      (@run_id, @mall, @shop_id, @mall_item_key, @mall_item_ref, @fulfillment, @ne_code, @price_type, @price_incl_tax,
       @price_tax_included, @price_raw, @mall_tax_rate, @postage_included, @postage_revenue_incl_tax, @points, @listing_status,
       @shipping_group,
       @fetch_status, @resolve_status, @resolve_reason, @valid_until, @source, @fetched_at)
  `);
  const tx = db.transaction((list) => { for (const r of list) stmt.run(r); });
  tx(rows);
}

/**
 * 商品一覧の履歴保存 (Company DB構想 06 §7 Step 0、中原さん決定 2026-09-09 D-17)。
 * 取った応答を丸ごと gz + manifest で残す (scripts/mall-items/archive-items.mjs)。
 *
 * 🚨 保存の失敗で取得結果 (listing_enum_status / price_fetch_run) を変えない (fail-soft)。
 *    結果は戻り値 archive に載せ、nightly が ping の note に写す (ロジザード在庫 run-hourly.ps1 step 2b と同じ)。
 * 🚨 0 件・形式不正のときは呼ばない (呼んでも empty で skip する)。
 * deps.archive === false で止められる (試験・手動)。deps.archive に関数を渡せば差し替え
 */
export async function archiveListings(deps, args) {
  if (deps.archive === false) return { code: 'disabled', action: 'skipped' };
  const fn = typeof deps.archive === 'function' ? deps.archive : archiveItems;
  try {
    // 🚨 offsite (rclone) はここでは行わない。最大 3 分待つ同期処理を取得の途中に挟むと、
    //    06:00 の期限を越えて世代作成が中断する (Codex R1-1)。nightly が公開のあとに残り時間の範囲でまとめて行う
    const r = await fn({ ...args, noOffsite: true });
    return {
      code: r.code, action: r.action, file: r.relFile || null, items: r.items ?? null,
      sameAsPrevious: r.sameAsPrevious ?? null, complete: r.complete ?? null, offsite: r.offsite ?? null,
    };
  } catch (e) {
    return { code: e.code || 'error', action: 'error', error: String(e.message).slice(0, 200) };
  }
}

/** 直近で完全列挙できた run の出品キー集合 (§5.2.1 の「完全集合」) */
export function loadLastCompleteKeys(db, mall) {
  const run = db.prepare(`
    SELECT run_id FROM price_fetch_run
    WHERE mall = ? AND listing_enum_status = 'ok'
    ORDER BY started_at DESC LIMIT 1
  `).get(mall);
  if (!run) return { runId: null, keys: new Set() };
  const rows = db.prepare('SELECT shop_id, mall_item_key FROM mall_price_snapshot WHERE run_id = ?').all(run.run_id);
  return { runId: run.run_id, keys: new Set(rows.map(r => `${r.shop_id}${r.mall_item_key}`)) };
}

export async function fetchAmazonListings(db, deps = {}) {
  const runId = newRunId();
  const startedAt = nowIso();
  const shopId = AMAZON_SHOP_ID();
  db.prepare(`INSERT INTO price_fetch_run (run_id, mall, started_at, status, listing_enum_status)
              VALUES (?, 'amazon', ?, 'running', 'failed')`).run(runId, startedAt);

  try {
    // 🚨 期限を過ぎていたら取得そのものを始めない (Codex R5-3)
    if (deps.deadline && new Date() >= deps.deadline) throw new Error('deadline_exceeded');
    const getReport = deps.getActiveListingsReport
      || (await import('../profit-calculator/sp-api.js')).getActiveListingsReport;
    // includeRawText: 履歴保存のために TSV の原文をもらう (画面向けの経路には付かない)
    const report = await getReport({ deadline: deps.deadline, includeRawText: true });
    // 🚨 レスポンス形式そのものを検証する。{} が返ったのを「0件」として通さない (Codex R1-2)
    if (!report || !Array.isArray(report.listings)) {
      throw new Error('出品レポートの形式が不正 (listings が配列でない)');
    }
    const fetchedAt = nowIso();
    const validUntil = addDays(fetchedAt, PRICE_VALID_DAYS);
    const parsed = report.listings.map(r => amazonRowToSnapshot(r, { runId, shopId, fetchedAt, validUntil }));
    // SKU が読めない行は黙って消さず、件数を残す
    const rows = parsed.filter(r => r.mall_item_key);
    const unparsable = parsed.length - rows.length;

    const currentKeys = new Set(rows.map(r => `${r.shop_id}${r.mall_item_key}`));
    // 🚨 重複キーを INSERT OR REPLACE で隠さない
    const duplicates = rows.length - currentKeys.size;
    const prev = loadLastCompleteKeys(db, 'amazon');
    const evalResult = enumStatusWithParseFailures(
      evaluateEnumeration(prev.keys, currentKeys), unparsable, duplicates);

    // 履歴保存 (Step 0)。レポートは 1 文書なので取得範囲は常に完走 (complete=true)。
    // 原文 (rawText) があればそのまま、無ければ (試験の差し替え) 解析済みの行を NDJSON で
    const hasRaw = typeof report.rawText === 'string';
    const archive = await archiveListings(deps, {
      mall: 'amazon', shopId, source: 'merchant_listings_all_data', runId, fetchedAt,
      format: hasRaw ? 'tsv' : 'ndjson',
      payload: hasRaw ? report.rawText : report.listings,
      items: report.listings.length,
      meta: { complete: true, enum_status: evalResult.status, api_version: report.apiVersion || null },
    });

    insertSnapshots(db, rows);
    db.prepare(`UPDATE price_fetch_run SET finished_at = ?, status = ?, listing_enum_status = ?,
                expected_count = ?, fetched_count = ?, failed_count = ?, disappeared_count = ?,
                error_summary = ? WHERE run_id = ?`)
      .run(nowIso(), evalResult.status, evalResult.status,
        rows.length, rows.filter(r => r.fetch_status === 'ok').length,
        rows.filter(r => r.fetch_status !== 'ok').length, evalResult.disappeared,
        enumSummary(evalResult), runId);
    return { runId, count: rows.length, unparsable, duplicates, ...evalResult, archive };
  } catch (e) {
    db.prepare(`UPDATE price_fetch_run SET finished_at = ?, status = 'failed', listing_enum_status = 'failed',
                error_summary = ? WHERE run_id = ?`).run(nowIso(), String(e.message).slice(0, 500), runId);
    throw e;
  }
}

export async function fetchRakutenListings(db, deps = {}) {
  const runId = newRunId();
  const startedAt = nowIso();
  const shopId = RAKUTEN_SHOP_ID();
  db.prepare(`INSERT INTO price_fetch_run (run_id, mall, started_at, status, listing_enum_status)
              VALUES (?, 'rakuten', ?, 'running', 'failed')`).run(runId, startedAt);

  try {
    // 🚨 既存の /items/all-codes・/items/all-skus は 100頁 = 10,000件で打ち切る (§15-4)。
    //    ここでは /items/search を自分でページングし、打ち切りを partial として検出する
    const searchPage = deps.searchPage || defaultRakutenSearchPage;
    const fetchedAt = nowIso();
    const validUntil = addDays(fetchedAt, PRICE_VALID_DAYS);
    const rows = [];
    const rawItems = [];                      // 履歴保存用: 応答の要素をそのまま (解析前の姿)
    let cursorMark = '*';
    let pages = 0;
    let unparsable = 0;
    let deadlineHit = false;
    const MAX_PAGES = deps.maxPages || 500;   // 50,000 商品。到達したら partial (打ち切りを隠さない)
    let truncated = false;
    const archiveArgs = (meta) => ({
      mall: 'rakuten', shopId, source: 'rms_items_search', runId, fetchedAt,
      format: 'ndjson', payload: rawItems, sortKey: (r) => (r?.item || r)?.manageNumber,
      items: rawItems.length,
      meta: { api_version: 'es/2.0 items/search', ...meta },
    });
    try {
      for (;;) {
        if (pages >= MAX_PAGES) { truncated = true; break; }
        // 🚨 ページごとに期限を見る。1ページ目だけ見ても、39ページ回る間に期限を越える
        if (deps.deadline && new Date() >= deps.deadline) { truncated = true; deadlineHit = true; break; }
        const data = await searchPage(cursorMark);
        pages++;
        // 🚨 形式を検証する。results も items も無いレスポンスを「0件」として通さない
        const items = Array.isArray(data?.results) ? data.results
          : (Array.isArray(data?.items) ? data.items : null);
        if (items === null) throw new Error(`RMS items/search の形式が不正 (${pages}頁目)`);
        for (const r of items) {
          rawItems.push(r);
          const item = r?.item || r;
          const made = rakutenItemToSnapshotsDetailed(item, { runId, shopId, fetchedAt, validUntil });
          // 🚨 商品まるごとの失敗も、variant 単位の失敗も数える
          unparsable += made.unparsable;
          rows.push(...made.rows);
        }
        const next = data?.nextCursorMark;
        if (!next || next === cursorMark || items.length === 0) break;
        cursorMark = next;
      }
    } catch (e) {
      // 途中のページで落ちた (503 など)。取得は失敗のまま (外側で price_fetch_run を failed にする) だが、
      // 取れた分は complete=false の証拠として残す (Codex R1-5)。0 件なら保存器が empty で skip する
      if (rawItems.length > 0) {
        await archiveListings(deps, archiveArgs({
          complete: false, enum_status: 'failed', pages, truncated: true, deadline_hit: false,
          note: `途中で失敗 (${pages}頁目まで取得): ${String(e.message).slice(0, 120)}`,
        }));
      }
      throw e;
    }

    const currentKeys = new Set(rows.map(r => `${r.shop_id}${r.mall_item_key}`));
    const duplicates = rows.length - currentKeys.size;
    const prev = loadLastCompleteKeys(db, 'rakuten');
    const evalResult = enumStatusWithParseFailures(
      evaluateEnumeration(prev.keys, currentKeys), unparsable, duplicates);
    // 0件 (failed) は打ち切りより重い。failed > partial > ok の順で厳しい方を採る
    const enumStatus = evalResult.status === 'failed' ? 'failed' : (truncated ? 'partial' : evalResult.status);
    const summary = enumSummary(evalResult)
      || (deadlineHit ? '全体終了期限に達したので取得を打ち切った'
        : (truncated ? `ページ上限 ${MAX_PAGES} に到達 (打ち切りの疑い)` : null));

    // 履歴保存 (Step 0)。打ち切り・期限切れの夜は complete=false で残す (証拠にはするが削除判定には使わない)
    const archive = await archiveListings(deps, archiveArgs({
      complete: !truncated && !deadlineHit, enum_status: enumStatus, pages, truncated, deadline_hit: deadlineHit,
    }));

    insertSnapshots(db, rows);
    db.prepare(`UPDATE price_fetch_run SET finished_at = ?, status = ?, listing_enum_status = ?,
                expected_count = ?, fetched_count = ?, failed_count = ?, disappeared_count = ?,
                error_summary = ? WHERE run_id = ?`)
      .run(nowIso(), enumStatus, enumStatus,
        rows.length, rows.filter(r => r.fetch_status === 'ok').length,
        rows.filter(r => r.fetch_status !== 'ok').length, evalResult.disappeared,
        summary, runId);
    return { runId, count: rows.length, pages, truncated, deadlineHit, unparsable, duplicates, ...evalResult, status: enumStatus, archive };
  } catch (e) {
    db.prepare(`UPDATE price_fetch_run SET finished_at = ?, status = 'failed', listing_enum_status = 'failed',
                error_summary = ? WHERE run_id = ?`).run(nowIso(), String(e.message).slice(0, 500), runId);
    throw e;
  }
}

async function defaultRakutenSearchPage(cursorMark) {
  const { callRakutenProxy } = await import('./rms-client.js');
  return callRakutenProxy(`/service-api/rakuten-rms/items/search?cursorMark=${encodeURIComponent(cursorMark)}&hits=100`);
}

// ────────────────────────────────────────────────────────────
if (process.argv[1] && process.argv[1].endsWith('fetch-listings.js')) {
  const mall = process.argv.includes('--mall') ? process.argv[process.argv.indexOf('--mall') + 1] : null;
  const db = initExpectedProfitDB();
  const run = async () => {
    if (!mall || mall === 'amazon') console.log('[amazon]', JSON.stringify(await fetchAmazonListings(db)));
    if (!mall || mall === 'rakuten') console.log('[rakuten]', JSON.stringify(await fetchRakutenListings(db)));
  };
  run().then(() => db.close()).catch(e => { console.error(e); process.exit(1); });
}
