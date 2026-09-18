/**
 * FBA箱詰め記録 — 「この商品は期限管理商品か」を決めて、納品回の行に焼く (中原さん 2026-09-18)
 *
 * これまで iPad の投入画面は、残りのある**全商品**に賞味期限のプルダウンを出していた
 * (要件 §3 は「期限管理品は割当時に期限入力」だが、当時は判定できるデータが無かった。
 *  `fbx_rows.requires_expiry` は PR1 からある空の列で、ここで初めて使う)。
 *
 * 判定の正本 = ロジザード商品マスタの有効期限区分 (入荷受付チェックと同じ。
 * apps/warehouse-mirror/expiry-managed.js に規則を 1 つだけ置いている)。
 * 商品コードにたどり着く道 = FNSKU → SKU → 商品コード:
 *   ①行の seller_sku (Excel 添付後 / picking に SKU があるとき)
 *   ②fba-replenishment の fba_sku_attrs (FNSKU → amazon_sku)
 *   → mirror_sku_resolved (source='master') で SKU → 商品コード。**セットは構成品ぜんぶ**
 *
 * 🚨 **「期限管理でない (0)」と「分からない (NULL)」を分ける**。分からない商品は今までどおり
 * プルダウンを出す。隠す方の間違いは、現場が入れず本社も STA に入れられず、納品が止まって初めて
 * 分かる = 外で弾かれるまで誰も気づけない [[feedback_safe_side_is_directional]]
 *
 * 判定は納品回の行に焼く (作業中に答えが変わらない = 重量ルールの snapshot と同じ考え方)。
 * マスタが後から直ったときのために、管理画面から「もう一度判定する」で焼き直せる。
 * best-effort: mirror が読めなくても納品回の作成・作業は止めない (全部「分からない」になるだけ)。
 */
import { getDB, listRowsForExpiry, saveRowExpiryFlags } from './db.js';

/** 判定の元データ源 (mirror DB と fba_sku_attrs)。テストで差し替え可 (null で既定に戻す) */
const defaultSource = async () => {
  const [{ getMirrorDB }, { expiryManagedByCode }, rep] = await Promise.all([
    import('../warehouse-mirror/db.js'),
    import('../warehouse-mirror/expiry-managed.js'),
    import('../fba-replenishment/db.js'),
  ]);
  const mdb = getMirrorDB();
  return {
    // SKU (小文字) → 商品コード[]。紐付けの正本 = master のみ (auto の推測で商品コードを間違えると、
    // 「期限管理でない」と読み違えて入力欄を消してしまう)
    skuToCodes: (skus) => {
      const map = new Map();
      const keys = [...new Set(skus)];
      for (let i = 0; i < keys.length; i += 400) {
        const part = keys.slice(i, i + 400);
        const ph = part.map(() => '?').join(',');
        for (const r of mdb.prepare(`SELECT lower(trim(seller_sku)) AS sku, ne_code FROM mirror_sku_resolved
            WHERE source = 'master' AND lower(trim(seller_sku)) IN (${ph})`).all(...part)) {
          if (!map.has(r.sku)) map.set(r.sku, []);
          map.get(r.sku).push(r.ne_code);
        }
      }
      return map;
    },
    expiryManagedByCode: (codes) => expiryManagedByCode(codes, mdb),
    fbaSkuAttrs: () => rep.getFbaSkuAttrs(),
  };
};
let source = defaultSource;
export function _setExpirySource(fn) { source = fn || defaultSource; }

const lower = (s) => String(s ?? '').trim().toLowerCase();

/** 直近の判定結果 (管理画面の診断用。run_id → {at, ...result}) */
const lastRun = new Map();
export function getLastExpiryResult(runId) { return lastRun.get(Number(runId)) || null; }

/**
 * 商品コードの答え (複数 = セットの構成品) から、その商品の答えを 1 つに決める。
 * 🚨 どれか 1 つでも「期限管理」ならその商品は期限管理 (構成品のうち 1 つでも期限があれば期限を聞く)。
 * 分からないものが混じっていたら「分からない」に倒す — 残りが全部「管理しない」でも消さない
 */
export function decideForCodes(answers) {
  if (!answers || answers.length === 0) return { requires: null, source: 'no_code' };
  const managed = answers.find((a) => a.managed === true);
  if (managed) return { requires: 1, source: managed.source };
  if (answers.some((a) => a.managed == null)) return { requires: null, source: 'unknown' };
  return { requires: 0, source: answers.some((a) => a.source === 'manual') ? 'manual' : 'logizard' };
}

/**
 * 納品回の行に期限管理の判定を焼く。
 * @param {number} runId
 * @param {{force?: boolean}} [opts] force = 判定済みの行もやり直す (マスタが直ったとき)
 * @returns {Promise<{ok, checked, managed, notManaged, unknown, skipped, error}>} throw しない
 */
export async function ensureRunExpiryFlags(runId, { force = false } = {}) {
  const t0 = Date.now();
  const out = { ok: false, checked: 0, managed: 0, notManaged: 0, unknown: 0, skipped: 0, error: null };
  try {
    const rows = listRowsForExpiry(runId, { onlyUnresolved: !force });
    out.checked = rows.length;
    if (rows.length === 0) { out.ok = true; return out; }
    const src = await source();

    // ① 行 → SKU 候補 (行の SKU + FNSKU から引いた SKU)
    // 🚨 SKU 属性 (FNSKU → SKU) が読めなかったら**何も焼かない** (Codex PR #1356 R2 #2)。
    //    続けると「行の SKU は期限管理でない・FNSKU 側の別 SKU が期限管理」の商品を 0 に確定してしまい、
    //    しかも判定済みになるので直っても焼き直さない。焼かなければ次に開いたときにやり直す
    const byFnsku = new Map();
    let attrs = [];
    try {
      attrs = (await src.fbaSkuAttrs()) || [];
    } catch (e) {
      out.error = `sku属性が読めませんでした (判定は次回に持ち越し): ${e.message}`;
      out.skipped = rows.length;
      return out;
    }
    for (const a of attrs) {
      if (!a.fnsku || !a.amazon_sku) continue;
      const k = String(a.fnsku).trim().toUpperCase();
      if (!byFnsku.has(k)) byFnsku.set(k, []);
      byFnsku.get(k).push(lower(a.amazon_sku));
    }
    const skusOf = (r) => [...new Set([
      ...(r.seller_sku ? [lower(r.seller_sku)] : []),
      ...(byFnsku.get(String(r.fnsku || '').trim().toUpperCase()) || []),
    ].filter(Boolean))];

    // ② SKU → 商品コード → 期限管理
    const codeMap = src.skuToCodes([...new Set(rows.flatMap(skusOf))]);
    const allCodes = [...new Set([...codeMap.values()].flat().map(lower).filter(Boolean))];
    const managedMap = src.expiryManagedByCode(allCodes);

    // ③ 行ごとに決めて焼く
    const flags = rows.map((r) => {
      // 🚨 商品コードにたどり着けなかった SKU を**黙って落とさない** (Codex R1 #1)。
      //    落とすと「片方の SKU は分からない・もう片方は期限管理でない」が 0 に確定し、期限欄が消える
      const codes = new Set();
      let unresolved = false;
      for (const s of skusOf(r)) {
        const got = codeMap.get(s);
        if (!got || got.length === 0) { unresolved = true; continue; }
        for (const c of got) { const k = lower(c); if (k) codes.add(k); else unresolved = true; }
      }
      const answers = [...codes].map((c) => managedMap.get(c) || { managed: null, source: 'unknown' });
      if (unresolved) answers.push({ managed: null, source: 'unknown' });
      const d = decideForCodes(answers);
      if (d.requires === 1) out.managed++; else if (d.requires === 0) out.notManaged++; else out.unknown++;
      return { id: r.id, requires: d.requires, source: d.source };
    });
    saveRowExpiryFlags(flags);
    out.ok = true;
    return out;
  } catch (e) {
    // 読めなくても作業は止めない (行は「分からない」のまま = いままでどおり全部に期限欄が出る)
    out.error = e.message;
    return out;
  } finally {
    out.ms = Date.now() - t0;
    lastRun.set(Number(runId), { at: new Date().toISOString(), ...out });
  }
}

/** 管理画面の診断用: この回の判定の内訳 */
export function expirySummary(runId) {
  const d = getDB();
  const rows = d.prepare(`SELECT requires_expiry AS req, expiry_source AS src, COUNT(*) AS n
    FROM fbx_rows WHERE run_id = ? AND match_state != 'retired' GROUP BY req, src`).all(Number(runId));
  const sum = { managed: 0, notManaged: 0, unknown: 0, unresolved: 0, bySource: {} };
  for (const r of rows) {
    if (r.src == null) sum.unresolved += r.n;
    else if (r.req === 1) sum.managed += r.n;
    else if (r.req === 0) sum.notManaged += r.n;
    else sum.unknown += r.n;
    if (r.src) sum.bySource[r.src] = (sum.bySource[r.src] || 0) + r.n;
  }
  return sum;
}
