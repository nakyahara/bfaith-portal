/**
 * amazon-map-migrate.mjs — miniPC の SKU マスタ (m_sku_master + m_sku_components) → Company DB の Amazon SKU の対応 (core.amazon_sku_maps + core.listing_components)
 * (Company DB構想 16 §3 #2・§4「切替前 (T-7 から) の影運転」「当日 ③ 全件の移行」・§5 の 4「切替前の片付け」・§7 v2 M7 / M8。PR ⑦-1・0054)
 *
 * 2 つの使い方 (CLI = scripts/company-db/amazon-map-migrate.mjs):
 *   shadow (影運転・切替の前に毎日): 試し用の DB だけ。1 つの取引で全部を移して、Company DB から写しの決まった並べ方 (sku-map-canon-v1) を作り直し、
 *     古い表のハッシュと照らして **必ず巻き戻す**。切替を止める項目 (下の blockers) を数える (目標は全部 0)
 *   apply (切替の日 ③): 段階が frozen の間 か、🆕 段階 new_open で listing_components.amazon を足す広げる道の試みが開いている間 (0059・PR-B) だけ・
 *     対応の表が空のときだけ・止める項目が 0 件のときだけ・古い表のハッシュが渡された H0 と同じときだけ。
 *     1 つの取引で移して、作り直したハッシュが H0 と同じときだけ commit (違えば巻き戻す)
 *     new_open の窓 (migrationWidenWindow = DB の ops.amazon_map_migration_window): 指した試み (--attempt) が開いていて足すキーに listing_components.amazon・
 *     widen の判定と同じ共通の部品 (epoch・全部のプロセスの 2 版の ack・書きかけ 0・手の入口 gas:logizard-sheet-and-sku-map の停止) が全部通る。
 *     鍵 = epoch の共有 → 段階の共有 → マスタの書き込みの排他 (widen = epoch → 段階 → 書き込みの排他と同じ順) = 試みの cancel / widen は移行の commit を待つ
 *   🆕 reconcile (#1648 Codex R1 Medium 2・中原さんの決定 b): 移行 (apply) の後に試みを cancel し、古い入口が開いて古い表が変わった後のやり直し。
 *     段階 new_open で Amazon を足す試みの窓 (apply と同じ ops.amazon_map_migration_window・--attempt) が開き、持ち主が load の間だけ。
 *     今の Company DB の写しのハッシュ (--expect-cdb-hash) と古い表のハッシュ (--expect-hash) を照らし、origin = portal の行が無いときだけ、
 *     origin = legacy の対応を今の古い表に合わせ直す (足す・直す・墓標を戻す・古い表から消えた対応は墓標 = 0054 は DELETE を断る)。
 *     止める項目は apply と同じ (1 件でも断る)。合わせた後の Company DB のハッシュ = 古い表のハッシュのときだけ commit (違えば巻き戻す)。1 つの取引
 * 移し方 (seller SKU ごと):
 *   出品 (mall amazon・shop_code = Amazon (日本)・listing_norm = core.norm_code(seller_sku)) が無ければ作る → 対応 (origin = legacy・
 *   registered_at = created_at・changed_at = updated_at・registered_by = created_by・changed_by = updated_by) → 構成をちょうど古い表と同じに:
 *     同じ (SKU・数量・並び) の行 = 時刻 (created_at / updated_at) だけそろえる (変更の記録・出品の version を増やさない = 0049 の印を増やさない) /
 *     数量・並びが違う行 = 直す / 足りない行 = 足す / 古い表に無い行 (FBM の完全一致・前の取り込みの残り) = 消す。どれも resolution = imported
 *   書く取引の設定 core.source_system = 'amazon_map_migration' (0054 の書き手の守り)・夜間ロードと同じマスタの書き込みの鍵 (排他)
 * 🚨 古い表 (SQLite) は読むだけ (readonly で開く)。Render・miniPC の SQLite には書かない (M8 = frozen までは本番の SQLite / Render を変えない)
 */
import Database from 'better-sqlite3';
import { MASTER_WRITE_EXCLUSIVE_LOCK_SQL, CUTOVER_SHARED_LOCK_SQL } from './master-cutover.mjs';
import { fromMiniPcRows, skuMapDigest, skuMapKeyProblem, isCanonicalTimestamp } from './sku-map-canonical.js';
import { readCompanyAmazonMapCanon, AMAZON_JP_SHOP_CODE } from './amazon-map-write.mjs';
import { normSku } from './sku-norm.js';

export const MIGRATION_SOURCE = 'amazon_map_migration';
// epoch の鍵 (0055・apps/company-db/load/ownership-state.mjs と同じ文。lib から apps を import しない)
const OWNERSHIP_LOCK_EXISTS_SQL = `select to_regprocedure('ops.master_ownership_lock_key()') is not null as ok`;
const OWNERSHIP_SHARED_LOCK_SQL = 'select pg_advisory_xact_lock_shared(ops.master_ownership_lock_key())';
const TS = (col) => `to_char((${col}) at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')`;
const SAMPLE = 20;
/** 写しの NE コードの形 (SKU のコードの前後の U+0020 を除いて ASCII だけ小文字 = readCompanyAmazonMapCanon と同じ) */
export const neCodeOfSku = (code) => String(code).replace(/^ +| +$/g, '').replace(/[A-Z]/g, (c) => c.toLowerCase());

/** miniPC の warehouse.db から SKU マスタの 2 表を読む (読むだけで開く) */
export function readLegacyAmazonMaps(file) {
  const db = new Database(file, { readonly: true, fileMustExist: true });
  try { return readLegacyAmazonMapsFrom(db); } finally { db.close(); }
}

/** 開いている SQLite (better-sqlite3) から SKU マスタの 2 表を読む (読むだけ。widen の CLI が作り直しの確かめと同じ接続で使う) */
export function readLegacyAmazonMapsFrom(db) {
  const masterRows = db.prepare('select seller_sku, 商品名, created_at, updated_at, created_by, updated_by from m_sku_master').all();
  const componentRows = db.prepare('select seller_sku, ne_code, 数量, sort_order, created_at, updated_at from m_sku_components').all();
  return { masterRows, componentRows };
}

/** listing_components.amazon (Amazon SKU の対応) の持ち主表のキー */
export const AMAZON_MAP_WIDEN_KEY = 'listing_components.amazon';

/**
 * 段階 new_open で移行 (apply) を通してよい窓か (0059・PR-B)。取引の中で鍵の後に呼ぶ。attemptId = 人が指した試み (--attempt・要る)。
 * 窓 = DB の ops.amazon_map_migration_window (widen の判定と同じ共通の部品 ops._widen_attempt_common = 試みが開いている・epoch の prepared がその試みのもの・
 *      全部のプロセスの 2 版の ack が prepare の後・書きかけ 0・新しい・試みの manifest・active / prepared を見た・capable ⊇ 足すキー・手の入口の停止) +
 *      足すキーに listing_components.amazon (#1648 Codex R1 Medium 1)
 * @returns {Promise<{ open: boolean, why: string|null, attempt: string|null }>}
 */
export async function migrationWidenWindow(db, attemptId) {
  const has = (await db.query(`select to_regprocedure('ops.amazon_map_migration_window(uuid)') is not null as ok`)).rows[0].ok === true;
  if (!has) return { open: false, why: 'Amazon を足す広げる道 (0059) の前の DB', attempt: null };
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(String(attemptId || ''))) return { open: false, why: '試み (--attempt <widen_prepare_id>) が要る', attempt: null };
  const r = (await db.query('select ops.amazon_map_migration_window($1::uuid) as r', [attemptId])).rows[0].r;
  if (r.ok === true) return { open: true, why: null, attempt: r.widen_prepare_id };
  return { open: false, why: `試み ${attemptId} の窓でない: ${(r.problems || []).join(' / ')}`, attempt: r.widen_prepare_id ?? null };
}

/**
 * 古い表 (SQLite) と Company DB (active の対応) の写しの決まった並べ方のハッシュを照らす (widen / check の CLI・0059)。
 * db = Company DB の取引の中 (widen は鍵の後) で読む。戻り値の master_rows / component_rows = Company DB の行の数 (DB が鍵の後に数えた数と照らす)
 */
export async function amazonMapHashEvidence(db, legacy) {
  const l = legacyDigest(legacy);
  const c = skuMapDigestSafe(await readCompanyAmazonMapCanon(db));
  return {
    legacy_hash: l.content_hash, company_hash: c.content_hash, master_rows: c.master_rows, component_rows: c.component_rows,
    legacy_rows: { master: l.master_rows, component: l.component_rows }, error: l.error || c.error || null,
    match: !!l.content_hash && l.content_hash === c.content_hash,
  };
}

/**
 * widen の CLI の 1 段 (0059): widenOwnership の beforeCall (3 つの排他の鍵の後・同じ取引) で Company DB を読み、古い表 (legacy = 先に読んだ SQLite の 2 表) と照らす。
 * 違う = 投げる (AMAZON_MAP_HASH_MISMATCH = 広げない)。同じ = 写しの証拠の amazon_map (ハッシュ 2 つ・Company DB の行の数 = DB が鍵の後に数え直して照らす)。result() = 照らした結果
 * 🆕 #1651: 今の fba.db の Sheet にだけある SKU の出品 (対応なし) に構成が 1 行でもあれば投げる (AMAZON_MAP_SHEET_ONLY_HAS_COMPONENTS)。
 *   移行の後に持ち主 load の夜間ロードが Sheet から作った構成はハッシュ (対応のある出品だけ) では見えない。出どころによらない・消さない = 人が見て決める。
 *   readSheetOnly (要る) = fba.db を読み直す関数 (async 可・配列を返す)。**3 つの鍵の後に**呼んで、続けて Company DB を読む
 *   (鍵の前に読んだ一覧を使うと、鍵を待つ間に fba.db が変わったときに見落とす = Codex #1651 R4 High)。読めない = 投げる = 広げない
 */
export function amazonWidenEvidenceStep(legacy, { readSheetOnly } = {}) {
  if (typeof readSheetOnly !== 'function') throw Object.assign(new Error('fba.db を読み直す関数 (readSheetOnly) が要る (#1651)'), { code: 'AMAZON_MAP_MIGRATE_INVALID' });
  let last = null;
  return {
    result: () => last,
    beforeCall: async (db) => {
      last = await amazonMapHashEvidence(db, legacy);
      if (!last.match) {
        throw Object.assign(new Error(`広げない: 古い表のハッシュ ${last.legacy_hash || last.error} が Company DB のハッシュ ${last.company_hash || last.error} と違う`), { code: 'AMAZON_MAP_HASH_MISMATCH' });
      }
      const rows = await freshSheetOnlyComponents(db, readSheetOnly);
      last.sheet_only_components = rows.length;
      if (rows.length) {
        throw Object.assign(new Error(`広げない: Sheet にだけある SKU の出品に構成が ${rows.length} 行ある (${rows.slice(0, 3).map((r) => `${r.listing_code}→${r.sku_code} (${r.source ?? '?'})`).join('・')})。人が見て決めてから`), { code: 'AMAZON_MAP_SHEET_ONLY_HAS_COMPONENTS' });
      }
      return { amazon_map: { legacy_hash: last.legacy_hash, company_hash: last.company_hash, master_rows: last.master_rows, component_rows: last.component_rows } };
    },
  };
}

/** fba.db の Sheet の写し (sku_mapping) にだけある seller SKU (16 §5 の 4・≤ 5 件のはず。PR-D から気をつける項目 = 止めない)。表が無い = 投げる (0 件と読まない・Codex #1586 R1 M3) */
export function readSheetOnlySkus(fbaFile, legacy) {
  const db = new Database(fbaFile, { readonly: true, fileMustExist: true });
  try {
    const has = db.prepare(`select 1 from sqlite_master where type = 'table' and name = 'sku_mapping'`).get();
    if (!has) throw Object.assign(new Error(`fba.db に sku_mapping の表が無い (違うファイル?): ${fbaFile}`), { code: 'AMAZON_MAP_MIGRATE_FBA_DB' });
    const known = new Set(legacy.masterRows.map((r) => normSku(r.seller_sku)));
    return db.prepare('select amazon_sku from sku_mapping').all().map((r) => r.amazon_sku).filter((s) => s && !known.has(normSku(s)));
  } finally { db.close(); }
}

/** 古い表の決まった並べ方のハッシュ (形が違えば error)。H0 = 切替の日の最後の同期のこの値 */
export function legacyDigest(legacy, sellerSkus = null) {
  const canon = fromMiniPcRows(legacy);
  const pick = sellerSkus ? { master: canon.master.filter((r) => sellerSkus.has(r.seller_sku)), components: canon.components.filter((r) => sellerSkus.has(r.seller_sku)) } : canon;
  try { return { ...skuMapDigest(pick), error: null }; } catch (e) { return { content_hash: null, master_rows: pick.master.length, component_rows: pick.components.length, error: e.message }; }
}

/**
 * fba.db を読み直して (readSheetOnly = 配列を返す関数)、続けて Company DB の構成を読む (#1651 R4 = widen は 3 つの鍵の後・check も同じ読み方)。
 * 一覧が配列でない = 投げる (照らせない)
 */
export async function freshSheetOnlyComponents(db, readSheetOnly) {
  const sheetOnly = await readSheetOnly();
  if (!Array.isArray(sheetOnly)) throw Object.assign(new Error('Sheet にだけある SKU の一覧が読めない'), { code: 'AMAZON_MAP_MIGRATE_INVALID' });
  return sheetOnlyComponents(db, sheetOnly);
}

/**
 * Sheet にだけある SKU の出品 (Amazon (日本)・listing_norm = core.norm_code(SKU)) のうち、対応 (core.amazon_sku_maps・墓標も) の無い出品の構成の行 (#1651)。
 * 出どころ (evidence.source)・manual によらず全部 (読むだけ)。移行の計画は古い表の出品を除く (呼び手) / widen の照らしは全部 (移行の後 = 古い表の出品は対応がある)
 */
export async function sheetOnlyComponents(db, sheetOnly) {
  if (!sheetOnly.length) return [];
  return (await db.query(`select l.listing_id::text as listing_id, l.listing_code, k.code as sku_code, c.qty, c.resolution, c.evidence ->> 'source' as source
      from core.listings l join core.listing_components c on c.listing_id = l.listing_id left join core.skus k on k.sku_id = c.sku_id
     where l.company_id = 1 and l.mall = 'amazon' and l.shop_code = $2 and l.listing_norm in (select core.norm_code(x) from unnest($1::text[]) as t(x))
       and not exists (select 1 from core.amazon_sku_maps m where m.listing_id = l.listing_id)
     order by l.listing_code, c.sort_order, k.code`, [sheetOnly, AMAZON_JP_SHOP_CODE])).rows;
}

/**
 * 移す計画 (DB は読むだけ)。止める項目 (blockers) を seller SKU ごとに数える。目標は全部 0 (16 §4・§5 の 4):
 *   key (seller SKU・NE コードが写しの受け手の決まりに合わない) / name_blank / timestamp (時刻が決まった形でない) / qty (1 以上の整数でない) /
 *   no_components (構成が 0 行) / sort_gap (並びが 0..N-1 でない) / orphan_component (親に無い構成) /
 *   not_in_company (NE コードが Company DB の SKU に無い) / component_collision (2 つの NE コードが正規化で同じ SKU) /
 *   seller_sku_collision (2 つの seller SKU が正規化で同じ = 同じ出品) / ne_code_differs (Company DB の SKU のコードから作る NE コードが古い表と違う = ハッシュが合わない) /
 *   sheet_only_has_components (Sheet にだけある SKU の出品 (対応なし・古い表の出品の外) に Company DB の構成が 1 行でもある。出どころによらない・消さない・#1651)
 * 気をつける (止めない): exception_sku (例外の SKU を構成品にしている) /
 *   sheet_only (Sheet にだけある SKU。PR-D で止める項目から外した = 10/8 中原さん「スプレッドシートは使用していないので無視」。
 *   本番の FBA は 6/11 から FBA_SKU_MAPPING_SOURCE=mirror (SKU マスタ直結) で、Sheet の写しを FBA の対応に使っていない。数・例は今までどおり出す)
 */
export async function planAmazonMapMigration(db, legacy, { sheetOnly = [] } = {}) {
  const blockers = {};
  const warnings = {};
  const add = (bag, k, v) => { (bag[k] ||= { count: 0, samples: [] }).count++; if (bag[k].samples.length < SAMPLE) bag[k].samples.push(v); };
  const blocked = new Set();
  const block = (sku, k, v) => { add(blockers, k, v); blocked.add(sku); };
  const masters = new Map();
  for (const r of legacy.masterRows) {
    masters.set(r.seller_sku, { ...r, comps: [] });
    const kp = skuMapKeyProblem(r.seller_sku);
    if (kp) block(r.seller_sku, 'key', { seller_sku: r.seller_sku, problem: kp });
    if (typeof r.商品名 !== 'string' || /^[\s\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff]*$/.test(r.商品名)) block(r.seller_sku, 'name_blank', { seller_sku: r.seller_sku });
    for (const c of ['created_at', 'updated_at']) if (!isCanonicalTimestamp(r[c])) block(r.seller_sku, 'timestamp', { seller_sku: r.seller_sku, column: c, value: r[c] ?? null });
  }
  for (const c of legacy.componentRows) {
    const m = masters.get(c.seller_sku);
    if (!m) { add(blockers, 'orphan_component', { seller_sku: c.seller_sku, ne_code: c.ne_code }); continue; }
    m.comps.push(c);
    const np = skuMapKeyProblem(c.ne_code);
    if (np) block(c.seller_sku, 'key', { seller_sku: c.seller_sku, ne_code: c.ne_code, problem: np });
    if (!Number.isSafeInteger(c.数量) || c.数量 <= 0) block(c.seller_sku, 'qty', { seller_sku: c.seller_sku, ne_code: c.ne_code, qty: c.数量 });
    for (const k of ['created_at', 'updated_at']) if (!isCanonicalTimestamp(c[k])) block(c.seller_sku, 'timestamp', { seller_sku: c.seller_sku, ne_code: c.ne_code, column: k, value: c[k] ?? null });
  }
  // NE コード → SKU (core.norm_code で引く = 夜間ロードと同じ)
  const codes = [...new Set(legacy.componentRows.map((c) => c.ne_code))];
  const skuOf = new Map();
  for (let i = 0; i < codes.length; i += 5000) {
    for (const r of (await db.query(`select x as raw, k.sku_id::text as sku_id, k.code, k.sku_kind from unnest($1::text[]) as t(x)
        left join core.skus k on k.company_id = 1 and k.code_norm = core.norm_code(x)`, [codes.slice(i, i + 5000)])).rows) skuOf.set(r.raw, r);
  }
  // seller SKU → 出品 (正規化の重なりも)
  const skus = [...masters.keys()];
  const listingOf = new Map();
  const normCount = new Map();
  for (let i = 0; i < skus.length; i += 5000) {
    for (const r of (await db.query(`select x as raw, core.norm_code(x) as norm, l.listing_id::text as listing_id from unnest($1::text[]) as t(x)
        left join core.listings l on l.company_id = 1 and l.mall = 'amazon' and l.shop_code = $2 and l.listing_norm = core.norm_code(x)`, [skus.slice(i, i + 5000), AMAZON_JP_SHOP_CODE])).rows) {
      listingOf.set(r.raw, r.listing_id);
      normCount.set(r.norm, [...(normCount.get(r.norm) || []), r.raw]);
    }
  }
  for (const raws of normCount.values()) if (raws.length > 1) for (const s of raws) block(s, 'seller_sku_collision', { seller_sku: s, same_as: raws.filter((x) => x !== s) });
  const plan = [];
  for (const [sku, m] of masters) {
    if (!m.comps.length) { block(sku, 'no_components', { seller_sku: sku }); continue; }
    const orders = m.comps.map((c) => c.sort_order).sort((a, b) => a - b);
    if (orders.some((v, j) => v !== j)) block(sku, 'sort_gap', { seller_sku: sku, sort_orders: orders });
    const seen = new Map();
    const rows = [];
    for (const c of m.comps) {
      const k = skuOf.get(c.ne_code);
      if (!k || !k.sku_id) { block(sku, 'not_in_company', { seller_sku: sku, ne_code: c.ne_code }); continue; }
      if (seen.has(k.sku_id)) { block(sku, 'component_collision', { seller_sku: sku, ne_codes: [seen.get(k.sku_id), c.ne_code], sku: k.code }); continue; }
      seen.set(k.sku_id, c.ne_code);
      if (neCodeOfSku(k.code) !== c.ne_code) block(sku, 'ne_code_differs', { seller_sku: sku, ne_code: c.ne_code, company_code: k.code });
      if (!['single', 'set'].includes(k.sku_kind)) add(warnings, 'exception_sku', { seller_sku: sku, ne_code: c.ne_code });
      rows.push({ sku_id: k.sku_id, qty: c.数量, sort_order: c.sort_order, created_at: c.created_at, updated_at: c.updated_at });
    }
    plan.push({ seller_sku: sku, name: m.商品名, created_at: m.created_at, updated_at: m.updated_at, created_by: m.created_by ?? null, updated_by: m.updated_by ?? null,
      listing_id: listingOf.get(sku) ?? null, rows: rows.sort((a, b) => a.sort_order - b.sort_order) });
  }
  // PR-D: Sheet にだけある SKU は気をつける項目 (止めない・blocked_skus にも数えない)。Sheet は FBA の対応に使っていない (10/8 中原さん)
  for (const s of sheetOnly) add(warnings, 'sheet_only', { seller_sku: s });
  // #1651: Sheet にだけある SKU の出品 (対応・墓標なし・古い表の出品の外) に Company DB の構成が 1 行でもある = 止める項目 (出どころ・manual によらない)。
  //   何も消さない = 人が見て決める (移行の後も、持ち主 company の夜間ロードはその構成を消さない = 注文が違う SKU に結ばれうる。
  //   自動で消すと正しい FBM の行まで消しうる = Codex #1651 R1〜R3)。2026-10-08 の本番は 2 件とも構成 0 行 = 止まらない
  const legacyListings = new Set([...listingOf.values()].filter((x) => x != null));
  for (const r of await sheetOnlyComponents(db, sheetOnly)) {
    if (!legacyListings.has(r.listing_id)) add(blockers, 'sheet_only_has_components', { seller_sku: r.listing_code, sku: r.sku_code, qty: r.qty, resolution: r.resolution, source: r.source ?? null });
  }
  const ok = plan.filter((p) => !blocked.has(p.seller_sku));
  return {
    legacy: { master_rows: legacy.masterRows.length, component_rows: legacy.componentRows.length },
    blockers, warnings, blocked_skus: blocked.size, plan: ok,
    blocker_total: Object.values(blockers).reduce((n, b) => n + b.count, 0),
  };
}

/**
 * 移す (shadow = 必ず巻き戻す / apply = 照らして同じなら commit)。db = 表の持ち主のロールの接続 ({ query })。
 * opts = { mode: 'shadow' | 'apply' | 'reconcile', actor, expectHash (apply / reconcile で要る = 古い表のハッシュ), expectCdbHash (reconcile で要る = 今の Company DB の写しのハッシュ),
 *          sheetOnly (要る = fba.db の Sheet にだけある SKU の配列・warnings.sheet_only に数える = 止めない。その出品に構成があれば止める項目 sheet_only_has_components), attemptId (new_open の apply / reconcile で要る = 広げる道の試み), log,
 *          afterWrite (試験だけ: 書いた後・最後の照らしの前に同じ取引で呼ぶ) }
 * 戻り値 { mode, committed, phase, legacy_digest (全部), subset: { legacy, company, match }, counts, blockers, warnings, blocked_skus }
 */
export async function runAmazonMapMigration(db, legacy, opts = {}) {
  const mode = ['apply', 'reconcile'].includes(opts.mode) ? opts.mode : 'shadow';
  const writes = mode !== 'shadow';
  const actor = String(opts.actor || 'amazon_map_migration');
  const log = opts.log || (() => {});
  const legacyAll = legacyDigest(legacy);
  // Sheet にだけある SKU (fba.db) = 渡されなければ 0 件と読まずに断る (影運転も apply も・Codex #1586 R1 M3)。
  // PR-D で気をつける項目 (止めない) にしたが、一覧を渡すことは今までどおり要る (数・例を出す・0 件と読まない)
  if (!Array.isArray(opts.sheetOnly)) throw Object.assign(new Error('Sheet にだけある SKU の一覧 (fba.db から・sheetOnly) が要る'), { code: 'AMAZON_MAP_MIGRATE_INVALID' });
  if (writes) {
    if (!/^[0-9a-f]{64}$/.test(String(opts.expectHash || ''))) throw Object.assign(new Error(`${mode} には古い表のハッシュ (H0・--expect-hash) が要る`), { code: 'AMAZON_MAP_MIGRATE_INVALID' });
    if (legacyAll.content_hash !== opts.expectHash) throw Object.assign(new Error(`古い表のハッシュ ${legacyAll.content_hash || legacyAll.error} が H0 ${opts.expectHash} と違う (最後の同期の後に古い表が変わった)`), { code: 'AMAZON_MAP_MIGRATE_H0' });
  }
  if (mode === 'reconcile' && !/^[0-9a-f]{64}$/.test(String(opts.expectCdbHash || ''))) {
    throw Object.assign(new Error('reconcile には今の Company DB の写しのハッシュ (--expect-cdb-hash・--cdb-hash で出す) が要る'), { code: 'AMAZON_MAP_MIGRATE_INVALID' });
  }
  await db.query('begin');
  let committed = false;
  try {
    await db.query(`select set_config('core.actor_type', 'system', true), set_config('core.actor_id', $1, true), set_config('core.source_system', $2, true),
        set_config('core.run_id', $3, true), set_config('core.reason', $4, true)`, [actor, MIGRATION_SOURCE, `amazon_map_${mode}_${Date.now()}`, mode === 'apply' ? '切替の日の移行 (16 §4 ③)' : mode === 'reconcile' ? '移行の後の cancel からのやり直し (古い表に合わせ直す)' : '移行の影運転 (巻き戻す)']);
    // 0059: epoch の共有の鍵を先に (広げる道の prepare / cancel / widen = epoch の排他 と並ぶ = 試みの窓を読んでから commit まで試みが閉じない。0055 の前の DB は取らない)
    if ((await db.query(OWNERSHIP_LOCK_EXISTS_SQL)).rows[0].ok) await db.query(OWNERSHIP_SHARED_LOCK_SQL);
    await db.query(CUTOVER_SHARED_LOCK_SQL);
    await db.query(MASTER_WRITE_EXCLUSIVE_LOCK_SQL);   // 夜間ロード・画面の保存と並ぶ (夜間ロードと同じ排他の鍵)
    const phase = (await db.query('select phase from ops.master_cutover_state where id = 1')).rows[0]?.phase ?? null;
    let widenAttempt = null;
    if (mode === 'apply' && phase !== 'frozen') {
      // 🆕 0059 (PR-B): new_open では listing_components.amazon を足す試みが開いている間だけ (手の入口の停止の後)
      const w = phase === 'new_open' ? await migrationWidenWindow(db, opts.attemptId) : { open: false, why: null };
      if (!w.open) throw Object.assign(new Error(`apply は切替の段階が frozen の間か、new_open で ${AMAZON_MAP_WIDEN_KEY} を足す広げる道の試みが開いている間だけ (今 = ${phase}${w.why ? `・${w.why}` : ''})`), { code: 'AMAZON_MAP_MIGRATE_PHASE' });
      widenAttempt = w.attempt;
    }
    let cdbBefore = null;
    if (mode === 'reconcile') {
      // 段階 new_open の Amazon を足す試みの窓だけ (frozen でも通さない = frozen の道は apply)・持ち主が load の間だけ
      const w = phase === 'new_open' ? await migrationWidenWindow(db, opts.attemptId) : { open: false, why: null };
      if (!w.open) throw Object.assign(new Error(`reconcile は段階 new_open で ${AMAZON_MAP_WIDEN_KEY} を足す広げる道の試みが開いている間だけ (今 = ${phase}${w.why ? `・${w.why}` : ''})`), { code: 'AMAZON_MAP_MIGRATE_PHASE' });
      widenAttempt = w.attempt;
      const owner = (await db.query(`select coalesce(ops.master_ownership_active_map() ->> $1, 'load') as o`, [AMAZON_MAP_WIDEN_KEY])).rows[0].o;
      if (owner !== 'load') throw Object.assign(new Error(`reconcile は ${AMAZON_MAP_WIDEN_KEY} の持ち主が load の間だけ (今 = ${owner})`), { code: 'AMAZON_MAP_MIGRATE_PHASE' });
    }
    const existing = Number((await db.query('select count(*)::int as n from core.amazon_sku_maps')).rows[0].n);
    // 🚨 shadow / apply は今までどおり「空のときだけ」(reconcile のために EXISTS を外さない)
    if (mode !== 'reconcile' && existing) throw Object.assign(new Error(`Company DB にもう対応が ${existing} 件ある (移行は 1 回だけ。影運転は試し用の DB に戻してから。移行の後の cancel からのやり直しは --reconcile)`), { code: 'AMAZON_MAP_MIGRATE_EXISTS' });
    if (mode === 'reconcile') {
      if (!existing) throw Object.assign(new Error('Company DB に対応が 0 件 = 合わせ直すものが無い (--apply を使う)'), { code: 'AMAZON_MAP_RECONCILE_EMPTY' });
      const portal = Number((await db.query("select count(*)::int as n from core.amazon_sku_maps where origin <> 'legacy'")).rows[0].n);
      if (portal) throw Object.assign(new Error(`画面 (origin = portal) で作った対応が ${portal} 件ある = 古い表に合わせ直さない (持ち主が load の間は画面が閉じている = 無いはず。何が書いたか先に調べる)`), { code: 'AMAZON_MAP_RECONCILE_PORTAL' });
      cdbBefore = skuMapDigestSafe(await readCompanyAmazonMapCanon(db));
      if (cdbBefore.content_hash !== opts.expectCdbHash) {
        throw Object.assign(new Error(`今の Company DB の写しのハッシュ ${cdbBefore.content_hash || cdbBefore.error} が --expect-cdb-hash ${opts.expectCdbHash} と違う (読んだ後に変わった)`), { code: 'AMAZON_MAP_RECONCILE_CDB_HASH' });
      }
    }
    const p = await planAmazonMapMigration(db, legacy, { sheetOnly: opts.sheetOnly });
    if (writes && (p.blocker_total || p.blocked_skus)) {
      throw Object.assign(new Error(`切替を止める項目が ${p.blocker_total} 件ある (${Object.entries(p.blockers).map(([k, v]) => `${k} ${v.count}`).join('・')})。先に片付ける`), { code: 'AMAZON_MAP_MIGRATE_BLOCKED', blockers: p.blockers });
    }
    const counts = await writePlan(db, p.plan, actor, { reconcile: mode === 'reconcile' });
    log(`${mode === 'reconcile' ? '合わせ直した' : '移した'}: 対応 ${p.plan.length} 件・出品を作った ${counts.listings_created}${mode === 'reconcile' ? `・対応を足した ${counts.maps_inserted}・対応を直した ${counts.maps_updated}・墓標にした ${counts.maps_tombstoned}` : ''}・構成 同じ ${counts.same} (時刻だけ ${counts.time_only})・直した ${counts.updated}・足した ${counts.inserted}・消した ${counts.deleted}`);
    if (opts.afterWrite) await opts.afterWrite(db);   // 試験だけ (最後の照らしが効くことを確かめる)
    const set = new Set(p.plan.map((x) => x.seller_sku));
    // reconcile = Company DB の active の全部 (墓標にし残した対応も数える) / shadow・apply = 移せた SKU
    const company = skuMapDigestSafe(await readCompanyAmazonMapCanon(db, mode === 'reconcile' ? {} : { sellerSkus: [...set] }));
    const legacySub = legacyDigest(legacy, set);
    const match = !!company.content_hash && company.content_hash === legacySub.content_hash;
    if (writes) {
      if (!match || legacySub.content_hash !== legacyAll.content_hash || company.content_hash !== legacyAll.content_hash) {
        throw Object.assign(new Error(`作り直したハッシュ ${company.content_hash || company.error} が古い表のハッシュ ${legacyAll.content_hash} と違う (巻き戻した)`), { code: 'AMAZON_MAP_MIGRATE_MISMATCH' });
      }
      await db.query('commit');
      committed = true;
    } else {
      await db.query('rollback');
    }
    return { mode, committed, phase, widen_attempt: widenAttempt, cdb_before: cdbBefore, legacy_digest: legacyAll, subset: { skus: set.size, legacy: legacySub, company, match }, counts,
      blockers: p.blockers, warnings: p.warnings, blocked_skus: p.blocked_skus, blocker_total: p.blocker_total, legacy_rows: p.legacy };
  } catch (e) {
    if (!committed) { try { await db.query('rollback'); } catch { /* */ } }
    throw e;
  }
}

/**
 * reconcile: 古い表に無くなった origin = legacy の active の対応を墓標にする (構成の行を消してから = 0054 の不変条件「墓標に構成が無い」)。
 * 対応の行は消さない (0054 は DELETE を断る・夜間ロードは墓標の出品に自動の構成を作らない・消えた対応 (lost) にならない)。戻り値 = 墓標にした数
 */
async function tombstoneMissing(db, plan, actor) {
  const keep = plan.map((x) => x.listing_id).filter((x) => x != null);
  const keepSkus = plan.map((x) => x.seller_sku);
  const gone = (await db.query(`select m.listing_id::text as id from core.amazon_sku_maps m
      where m.state = 'active' and m.origin = 'legacy' and not (m.listing_id = any($1::bigint[])) and not (m.seller_sku = any($2::text[]))`, [keep, keepSkus])).rows.map((r) => r.id);
  if (!gone.length) return 0;
  await db.query('delete from core.listing_components c where c.listing_id = any($1::bigint[])', [gone]);
  await db.query(`update core.amazon_sku_maps set state = 'deleted', deleted_at = clock_timestamp(), deleted_by = $2, deleted_reason = '古い表 (SKU マスタ) から消えた (reconcile)'
      where listing_id = any($1::bigint[])`, [gone, actor]);
  return gone.length;
}

function skuMapDigestSafe(canon) {
  try { return { ...skuMapDigest(canon), error: null }; } catch (e) { return { content_hash: null, master_rows: canon.master.length, component_rows: canon.components.length, error: e.message }; }
}

/** 計画を書く (1 つの取引の中・集合で)。reconcile = 対応は upsert (変わった行だけ)・古い表に無い origin = legacy の active の対応は墓標 */
async function writePlan(db, plan, actor, { reconcile = false } = {}) {
  const counts = { listings_created: 0, maps: 0, same: 0, time_only: 0, updated: 0, inserted: 0, deleted: 0, ...(reconcile ? { maps_inserted: 0, maps_updated: 0, maps_tombstoned: 0 } : {}) };
  if (reconcile) counts.maps_tombstoned = await tombstoneMissing(db, plan, actor);
  if (!plan.length) return counts;
  // 1. 出品 (無いものだけ作る)
  const missing = plan.filter((x) => !x.listing_id);
  for (let i = 0; i < missing.length; i += 5000) {
    const chunk = missing.slice(i, i + 5000);
    const r = (await db.query(`insert into core.listings (company_id, mall, shop_code, listing_code, title, status, created_by_type, created_by_id)
        select 1, 'amazon', $3, t.code, t.title, 'active', 'system', $4 from unnest($1::text[], $2::text[]) as t(code, title)
      returning listing_id::text as listing_id, listing_code`, [chunk.map((x) => x.seller_sku), chunk.map((x) => x.name), AMAZON_JP_SHOP_CODE, actor])).rows;
    const byCode = new Map(r.map((x) => [x.listing_code, x.listing_id]));
    for (const x of chunk) x.listing_id = byCode.get(x.seller_sku);
    counts.listings_created += r.length;
  }
  // 2. 対応
  for (let i = 0; i < plan.length; i += 5000) {
    const c = plan.slice(i, i + 5000);
    const params = [c.map((x) => x.listing_id), c.map((x) => x.seller_sku), c.map((x) => x.name), c.map((x) => x.created_at), c.map((x) => x.created_by), c.map((x) => x.updated_at), c.map((x) => x.updated_by)];
    if (!reconcile) {
      await db.query(`insert into core.amazon_sku_maps (listing_id, company_id, seller_sku, name, state, origin, registered_at, registered_by, changed_at, changed_by)
          select t.lid, 1, t.sku, t.name, 'active', 'legacy', t.ca, t.cb, t.ua, t.ub
            from unnest($1::bigint[], $2::text[], $3::text[], $4::timestamptz[], $5::text[], $6::timestamptz[], $7::text[]) as t(lid, sku, name, ca, cb, ua, ub)`, params);
      counts.maps += c.length;
    } else {
      // 変わった行だけ (同じ行は書かない = 変更の記録・version を増やさない = 2 回流しても同じ)。墓標は active に戻す (古い表にまたある)
      const r = (await db.query(`insert into core.amazon_sku_maps as m (listing_id, company_id, seller_sku, name, state, origin, registered_at, registered_by, changed_at, changed_by)
          select t.lid, 1, t.sku, t.name, 'active', 'legacy', t.ca, t.cb, t.ua, t.ub
            from unnest($1::bigint[], $2::text[], $3::text[], $4::timestamptz[], $5::text[], $6::timestamptz[], $7::text[]) as t(lid, sku, name, ca, cb, ua, ub)
        on conflict (listing_id) do update set seller_sku = excluded.seller_sku, name = excluded.name, state = 'active', registered_at = excluded.registered_at, registered_by = excluded.registered_by,
            changed_at = excluded.changed_at, changed_by = excluded.changed_by, deleted_at = null, deleted_by = null, deleted_reason = null
          where (m.seller_sku, m.name, m.state, m.registered_at, m.registered_by, m.changed_at, m.changed_by)
                is distinct from (excluded.seller_sku, excluded.name, 'active', excluded.registered_at, excluded.registered_by, excluded.changed_at, excluded.changed_by)
        returning (xmax::text = '0') as ins`, params)).rows;
      counts.maps += r.length;
      counts.maps_inserted += r.filter((x) => x.ins === true).length;
      counts.maps_updated += r.filter((x) => x.ins !== true).length;
    }
  }
  // 3. 構成 (今の行と比べる)
  const lids = plan.map((x) => x.listing_id);
  const curBy = new Map();
  for (let i = 0; i < lids.length; i += 5000) {
    for (const r of (await db.query(`select listing_id::text as listing_id, sku_id::text as sku_id, qty, sort_order, ${TS('created_at')} as created_at,
          ${TS('coalesce(updated_at, created_at)')} as updated_at
        from core.listing_components where listing_id = any($1::bigint[])`, [lids.slice(i, i + 5000)])).rows) {
      if (!curBy.has(r.listing_id)) curBy.set(r.listing_id, new Map());
      curBy.get(r.listing_id).set(r.sku_id, r);
    }
  }
  const del = []; const timeOnly = []; const upd = []; const ins = [];
  for (const x of plan) {
    const cur = curBy.get(x.listing_id) || new Map();
    const want = new Set(x.rows.map((r) => r.sku_id));
    for (const [sid] of cur) if (!want.has(sid)) del.push([x.listing_id, sid]);
    for (const r of x.rows) {
      const c = cur.get(r.sku_id);
      if (!c) { ins.push([x.listing_id, r]); continue; }
      if (Number(c.qty) === r.qty && Number(c.sort_order) === r.sort_order) {
        counts.same++;
        if (c.created_at !== r.created_at || c.updated_at !== r.updated_at) timeOnly.push([x.listing_id, r]);
      } else upd.push([x.listing_id, r]);
    }
  }
  const run = async (sql, list, cols, extra = []) => { for (let i = 0; i < list.length; i += 5000) { const c = list.slice(i, i + 5000); await db.query(sql, [...cols.map((f) => c.map(f)), ...extra]); } };
  await run('delete from core.listing_components c using unnest($1::bigint[], $2::bigint[]) as d(lid, sid) where c.listing_id = d.lid and c.sku_id = d.sid',
    del, [(d) => d[0], (d) => d[1]]);
  // 同じ行は時刻だけ (created_at / updated_at は変更の記録と親の version の比べる列ではない = 記録も version も増えない)
  await run(`update core.listing_components c set created_at = d.ca, updated_at = d.ua from unnest($1::bigint[], $2::bigint[], $3::timestamptz[], $4::timestamptz[]) as d(lid, sid, ca, ua)
     where c.listing_id = d.lid and c.sku_id = d.sid`, timeOnly, [(d) => d[0], (d) => d[1].sku_id, (d) => d[1].created_at, (d) => d[1].updated_at]);
  await run(`update core.listing_components c set qty = d.qty, sort_order = d.so, resolution = 'imported', resolved_by_type = 'system', resolved_by_id = $7,
        evidence = jsonb_build_object('source', 'm_sku_master', 'via', 'amazon_map_migration'), created_at = d.ca, updated_at = d.ua
      from unnest($1::bigint[], $2::bigint[], $3::int[], $4::smallint[], $5::timestamptz[], $6::timestamptz[]) as d(lid, sid, qty, so, ca, ua)
     where c.listing_id = d.lid and c.sku_id = d.sid`,
  upd, [(d) => d[0], (d) => d[1].sku_id, (d) => d[1].qty, (d) => d[1].sort_order, (d) => d[1].created_at, (d) => d[1].updated_at], [actor]);
  await run(`insert into core.listing_components (company_id, listing_id, sku_id, qty, sort_order, resolution, resolved_by_type, resolved_by_id, evidence, created_at, updated_at)
      select 1, d.lid, d.sid, d.qty, d.so, 'imported', 'system', $7, jsonb_build_object('source', 'm_sku_master', 'via', 'amazon_map_migration'), d.ca, d.ua
        from unnest($1::bigint[], $2::bigint[], $3::int[], $4::smallint[], $5::timestamptz[], $6::timestamptz[]) as d(lid, sid, qty, so, ca, ua)`,
  ins, [(d) => d[0], (d) => d[1].sku_id, (d) => d[1].qty, (d) => d[1].sort_order, (d) => d[1].created_at, (d) => d[1].updated_at], [actor]);
  counts.time_only = timeOnly.length; counts.updated = upd.length; counts.inserted = ins.length; counts.deleted = del.length;
  return counts;
}
