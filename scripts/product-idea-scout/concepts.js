'use strict';
// products.jsonl (ASIN単位) を「商品テーマ」単位に束ねる。
//
// なぜ必要か:
//   代表 (中原さん) が判断する対象は「Amazonの商品」ではなく「自社で作る商品企画」。
//   ASIN 15,509行のCSVを人が見ることは現実には起きない (実際3週間放置された)。
//   同じ市場の色違い・容量違いを何度も審査させないために、
//   **リーフカテゴリ × 剤型 (作り方)** で束ねてから人の前に出す。
//
// 束ねる単位を「リーフカテゴリ × 剤型」にした理由:
//   Keepaから取れるのは商品タイトル・ブランド・カテゴリパス・月販・価格・レビュー・寸法だけで、
//   「顧客が実際に検索した語」は取れない。検索語を機械的に復元したことにすると
//   事実でないものを事実として扱うことになるので、**検索キーワードは仮説の欄**に留め、
//   束ねる軸には Amazon 自身が持っているカテゴリ構造と、AMCの作り方 (剤型) を使う。
//
// 使い方:
//   node concepts.js            → output/concepts_YYYYMMDD.csv / .json
//   node concepts.js --top 30   → 上位30テーマを画面に表示 (レビュー用)
const fs = require('fs');
const path = require('path');

const ROOT = process.env.SCOUT_HOME || __dirname;
const config = require(path.join(ROOT, 'config.json'));
const { FORMS, FORM_OTHER, detectForm, isSmall: checkSmall, parseProducts, evidence, purchaseSignal } = require('./quality.cjs');
const DATA = path.join(ROOT, 'data');
const OUTDIR = path.join(ROOT, 'output');


// ── AMCの作り方 (金型レス4剤型) ────────────────────────────────
// 「AMCで作れるか」は最初のハードゲート。ここに当たらないテーマは企画候補にならない。
// 判定はタイトル+カテゴリパスのキーワード。順番に見て最初に当たったものを採る
// (シートとオイルの両方に当たる商品があるため、優先順位を固定して再現性を持たせる)。
module.exports = { FORMS, FORM_OTHER, detectForm };

const isAsciiBrand = (b) => !!b && /^[\x20-\x7E]+$/.test(b);

const median = (arr) => {
  if (!arr.length) return null;
  const s = [...arr].sort((a, b) => a - b);
  const m = Math.floor(s.length / 2);
  return s.length % 2 ? s[m] : Math.round((s[m - 1] + s[m]) / 2);
};

/** FBA小型区分に収まるか (config.fbaSizeTiers の目安値) */
function isSmall(p) {
  return checkSmall(p, config.fbaSizeTiers.small);
}

function loadProducts() {
  const file = path.join(DATA, 'products.jsonl');
  if (!fs.existsSync(file)) { console.error('data/products.jsonl がありません'); process.exit(1); }
  return parseProducts(fs.readFileSync(file, 'utf8'));
}

/** finder が把握している対象ASIN数 (= 進捗の分母)。無ければ null */
function readAsinCount(catId) {
  const file = path.join(DATA, `finder-${catId}.json`);
  if (!fs.existsSync(file)) return null;
  try { return new Set(JSON.parse(fs.readFileSync(file, 'utf8')).asins || []).size; } catch (e) { return null; }
}

/** 取得元 finder ファイルの完全性 (分母が信用できるか) */
function loadCompleteness() {
  const out = new Map();
  for (const f of fs.readdirSync(DATA).filter((x) => x.startsWith('finder-') && x.endsWith('.json'))) {
    try {
      const j = JSON.parse(fs.readFileSync(path.join(DATA, f), 'utf8'));
      const id = String((j.category && j.category.id) || '');
      out.set(id, {
        name: (j.category && j.category.name) || id,
        complete: (j.queryVersion || 1) >= 2 ? !!j.complete : null,
        estimatedMissing: j.estimatedMissing ?? null,
        fetchedAt: j.fetchedAt || null,
      });
    } catch (e) {}
  }
  return out;
}

if (require.main !== module) return;

(async () => {
  const rows = loadProducts();
  const completeness = loadCompleteness();
  const big = config.knownBigBrands || [];
  const exTitle = (config.filters.excludeTitleKeywords || []).filter(k => !['食品', 'サプリ', '飲料', 'お菓子'].includes(k));

  // ── 基本フィルタ (score.js と同じ土俵) ──
  const base = rows.filter((p) => {
    const price = p.priceBuyBox ?? p.priceNew;
    if (!price || price < config.finder.current_NEW_gte || price > config.finder.current_NEW_lte) return false;
    if ((p.monthlySold ?? 0) < config.finder.monthlySold_gte) return false;
    if (p.packageWeightG && p.packageWeightG > config.filters.maxPackageWeightG) return false;
    if (exTitle.some((k) => p.title.includes(k))) return false;
    return true;
  });
  console.log(`商品 ${rows.length}件 → 基本フィルタ後 ${base.length}件`);

  // ── リーフカテゴリ × 剤型 で束ねる ──
  const groups = new Map();
  for (const p of base) {
    const form = detectForm(p);
    // ⚠️Keepa がカテゴリを返さない商品が実際にある (26,611件中23件)。
    //   空文字のまま束ねるとテーマの識別子が定まらない。かといって落とすと
    //   「どこにも出てこない商品」ができて後から気づけないので、ラベルを付けて残す。
    const rootName = (completeness.get(String(p.rootCategory)) || {}).name || String(p.rootCategory);
    const categoryPath = p.categoryPath || `${rootName} > (カテゴリ不明)`;
    const key = `${categoryPath} ${form.key}`;
    let g = groups.get(key);
    if (!g) {
      g = {
        categoryPath, rootCategory: p.rootCategory,
        formKey: form.key, formLabel: form.label, amcCapable: form.amc,
        items: [],
      };
      groups.set(key, g);
    }
    g.items.push(p);
  }

  // ── テーマ1件ぶんの指標を作る ──
  const concepts = [];
  for (const g of groups.values()) {
    const items = g.items;
    const brandCount = new Map();
    for (const p of items) brandCount.set(p.brand || '(不明)', (brandCount.get(p.brand || '(不明)') || 0) + 1);
    const brandsSorted = [...brandCount].sort((a, b) => b[1] - a[1]);
    const top1 = brandsSorted[0];
    const top3Share = Math.round(brandsSorted.slice(0, 3).reduce((s, b) => s + b[1], 0) / items.length * 100);
    const bigInTop3 = brandsSorted.slice(0, 3).some(([b]) => big.some((k) => String(b).includes(k)));

    const prices = items.map((p) => p.priceBuyBox ?? p.priceNew).filter(Boolean);
    const totalSold = purchaseSignal(items);
    const quality = evidence(items);
    const smallFlags = items.map(isSmall);
    const smallKnown = smallFlags.filter((v) => v !== null);
    const asciiRate = Math.round(items.filter((p) => isAsciiBrand(p.brand)).length / items.length * 100);
    const feePct = median(items.map((p) => p.referralFeePct).filter((v) => v != null));

    // 代表商品 = 月販上位5件。人が「どういう市場か」を30秒で掴むための材料
    const examples = [...items].sort((a, b) => (b.monthlySold || 0) - (a.monthlySold || 0)).slice(0, 5)
      .map((p) => ({
        asin: p.asin, title: p.title, brand: p.brand,
        monthlySold: p.monthlySold, price: p.priceBuyBox ?? p.priceNew,
        reviewCount: p.reviewCount, observedAt: p.observedAt ?? null, parentAsin: p.parentAsin ?? null,
      }));

    const src = completeness.get(String(g.rootCategory));

    concepts.push({
      concept: `${g.categoryPath.split(' > ').slice(-1)[0]} × ${g.formLabel}`,
      categoryPath: g.categoryPath,
      rootCategoryName: src ? src.name : String(g.rootCategory),
      form: g.formLabel,
      amcCapable: g.amcCapable,          // true=作れる / false=作れない / null=要判定
      productCount: items.length,
      totalMonthlySold: totalSold,
      brandCount: brandsSorted.length,
      top1Brand: top1 ? top1[0] : null,
      top1SharePct: top1 ? Math.round(top1[1] / items.length * 100) : null,
      top3SharePct: top3Share,
      bigBrandInTop3: bigInTop3,
      medianPrice: median(prices),
      medianReferralFeePct: feePct,
      smallSizeRatePct: smallKnown.length ? Math.round(smallKnown.filter(Boolean).length / smallKnown.length * 100) : null,
      unknownSizeCount: smallFlags.filter((v) => v === null).length,
      asciiBrandRatePct: asciiRate,      // 高いほど中華コモディティ疑い
      medianReviewCount: median(items.map((p) => p.reviewCount).filter((v) => v != null && v >= 0)),
      // ⭐分母の質。これが false/null のテーマは「市場規模の下限」としてしか読めない
      sourceComplete: src ? src.complete : null,
      sourceFetchedAt: quality.observedFrom,
      quality,
      examples,
    });
  }

  // ── ゲート判定 ──
  // ⭐総合点に潰さない。中原さんの判定は「ゲートを順に通す」もので、
  //   点数にすると「大型だが需要が大きい」テーマが上位に来てしまう
  //   (実際 v1 の並びは シャンプー(小型率1%) / スプレー(6%) / 室外機カバー(0%) が上位に来た)。
  //   ハードゲートで落としたものは消さずに、落ちた理由を付けて別枠に置く。
  const SIZE_PASS_PCT = 50;      // テーマの過半がFBA小型区分に収まるか
  const COMMODITY_ASCII_PCT = 60; // 無名英字ブランドがこの割合を超えたら中華コモディティ疑い
  for (const c of concepts) {
    const sizeKnown = c.productCount - c.unknownSizeCount;
    const gates = {
      freshness: c.quality.freshness,
      // ゲート1: AMCで作れるか (剤型)
      amc: c.amcCapable === true ? 'pass' : (c.amcCapable === false ? 'fail' : 'unknown'),
      // ゲート2: FBA小型区分に収まるか (NG②大型 = ハードゲート)
      size: sizeKnown < Math.max(3, c.productCount * 0.3)
        ? 'unknown'                                   // 寸法が取れている商品が少なすぎて言えない
        : (c.smallSizeRatePct >= SIZE_PASS_PCT ? 'pass' : 'fail'),
      // ゲート3: 中華コモディティ疑い (NG① — 落とさず「高確率NG」として分類する)
      commodity: c.asciiBrandRatePct >= COMMODITY_ASCII_PCT ? 'suspect' : 'ok',
      // ゲート4: 大手が押さえているか (落とさない。勝てる理由が要るという印)
      bigBrand: (c.bigBrandInTop3 || c.top1SharePct >= 50) ? 'dominated' : 'open',
    };
    c.gates = gates;
    // ハードゲート = amc と size。どちらかが fail なら土俵に乗らない
    c.hardGate = (gates.amc === 'fail' || gates.size === 'fail') ? 'fail'
      : ((gates.amc === 'unknown' || gates.size === 'unknown' || gates.freshness !== 'pass') ? 'unknown' : 'pass');
    c.gateFailReason = [
      gates.amc === 'fail' ? `対象外 (${c.form})` : null,
      gates.freshness !== 'pass' ? '商品観測日が不明、または30日超。需要の再確認が必要' : null,
      gates.size === 'fail' ? `FBA小型区分外 (小型率${c.smallSizeRatePct}%)` : null,
      gates.amc === 'unknown' ? '商品形態・必要工程の確認が必要' : null,
      gates.size === 'unknown' ? '寸法データが足りない' : null,
    ].filter(Boolean).join(' / ') || null;
  }

  // 並び順は「人がどれから見るか」だけを決める。判定はしない。
  // ゲートを通ったものの中でのみ、需要と競合構造で並べる。
  const rank = (c) => {
    let v = Math.log10(c.totalMonthlySold + 1) * 10;  // 需要
    v += (100 - c.top1SharePct) / 5;                  // ブランド分散 (寡占でない)
    v -= c.gates.bigBrand === 'dominated' ? 8 : 0;    // 大手が押さえている
    v -= c.gates.commodity === 'suspect' ? 10 : 0;    // 中華コモディティ疑い
    v -= (c.medianReviewCount > 1000) ? 5 : 0;        // レビュー壁
    return v;
  };
  const order = { pass: 0, unknown: 1, fail: 2 };
  concepts.sort((a, b) => (order[a.hardGate] - order[b.hardGate]) || (rank(b) - rank(a)));
  concepts.forEach((c, i) => { c.rank = i + 1; });

  // ── 収集の工程表 (ポータルの進捗表示の材料) ──
  // ⭐「取得完了」と「分析完了」を混ぜない。ここは取得の話だけを出す。
  //   未投入カテゴリも必ず行として出す — 現在のカテゴリだけ見せると1周の停滞が見えない。
  // 「いつ最後に前進したか」= products.jsonl が最後に伸びた時刻。
  // ⭐件数の比較だけで「収集中」を名乗ると、Keepaが返さない数件が残ったカテゴリが
  //   永遠に「収集中」になり、信号が緑のまま止まっていることを隠してしまう。
  //   直したはずの空回りが別の形で戻るので、実際に伸びた時刻で判定する。
  let lastProgressAt = null;
  try { lastProgressAt = fs.statSync(path.join(DATA, 'products.jsonl')).mtime.toISOString(); } catch (e) {}

  const collected = new Set(rows.map((p) => p.asin));
  const collection = [];
  for (const c of config.rootCategories || []) {
    const id = String(c.id);
    const src = completeness.get(id);
    const target = src ? (readAsinCount(id) ?? null) : null;
    const finderFile = path.join(DATA, `finder-${id}.json`);
    const targets = fs.existsSync(finderFile) ? JSON.parse(fs.readFileSync(finderFile, 'utf8')).asins : [];
    const fetched = new Set((targets || []).filter((asin) => collected.has(asin))).size;
    let state = 'not_started';
    if (src) state = (target != null && fetched >= target) ? 'collected' : 'collecting';
    collection.push({
      rootCategory: id,
      name: c.name,
      state,
      asinTarget: target,
      fetched,
      complete: src ? src.complete : null,
      estimatedMissing: src ? src.estimatedMissing : null,
      fetchedAt: src ? src.fetchedAt : null,
      remaining: (target != null) ? Math.max(0, target - fetched) : null,
    });
  }

  const day = new Date(Date.now() + 9 * 3600 * 1000).toISOString().slice(0, 10).replace(/-/g, '');
  const jsonPath = path.join(OUTDIR, `concepts_${day}.json`);
  const payload = {
    generatedAt: new Date().toISOString(),
    algorithmVersion: 2,
    sourceProducts: rows.length,
    afterBaseFilter: base.length,
    completeness: [...completeness.entries()].map(([id, v]) => ({ rootCategory: id, ...v })),
    collection,
    lastProgressAt,
    remainingTotal: collection.reduce((n, c) => n + (c.remaining || 0), 0),
    concepts,
  };
  if (process.argv.includes('--check')) { console.log(JSON.stringify(payload)); return; }
  fs.mkdirSync(OUTDIR, { recursive: true });
  fs.writeFileSync(jsonPath, JSON.stringify(payload, null, 1));

  const csvPath = path.join(OUTDIR, `concepts_${day}.csv`);
  const head = ['順位', 'ハードゲート', 'ゲート落ち理由', 'コモディティ疑い', '大手寡占', 'テーマ', 'カテゴリ', '剤型', '工程候補', '商品数', '購入表示合計(市場規模ではない)', 'ブランド数',
    'Top1ブランド', 'Top1商品数比率%', 'Top3商品数比率%', '大手Top3内', '中央価格', '手数料率%',
    '小型率%', '英字ブランド率%', 'レビュー中央値', '分母の完全性', '代表商品'];
  const esc = (v) => `"${String(v ?? '').replace(/"/g, '""')}"`;
  const lines = [head.join(',')];
  for (const c of concepts) {
    lines.push([
      c.rank,
      c.hardGate === 'pass' ? '通過' : (c.hardGate === 'fail' ? '落ち' : '要確認'),
      esc(c.gateFailReason), c.gates.commodity === 'suspect' ? 'Y' : '', c.gates.bigBrand === 'dominated' ? 'Y' : '',
      esc(c.concept), esc(c.categoryPath), esc(c.form),
      c.amcCapable === true ? '工程候補' : (c.amcCapable === false ? '対象外' : '要確認'),
      c.productCount, c.totalMonthlySold, c.brandCount, esc(c.top1Brand), c.top1SharePct, c.top3SharePct,
      c.bigBrandInTop3 ? 'Y' : '', c.medianPrice, c.medianReferralFeePct,
      c.smallSizeRatePct ?? '', c.asciiBrandRatePct, c.medianReviewCount,
      c.sourceComplete === true ? '完全' : (c.sourceComplete === false ? '不完全' : '不明'),
      esc(c.examples.map((e) => `${e.brand}/${e.title}/月販${e.monthlySold}/¥${e.price}`).join(' | ')),
    ].join(','));
  }
  fs.writeFileSync(csvPath, '﻿' + lines.join('\n'));

  const passed = concepts.filter((c) => c.hardGate === 'pass');
  const amcOk = concepts.filter((c) => c.amcCapable === true).length;
  console.log(`ハードゲート通過 ${passed.length}件 / 要確認 ${concepts.filter((c)=>c.hardGate==="unknown").length} / 落ち ${concepts.filter((c)=>c.hardGate==="fail").length}`);
  console.log(`テーマ ${concepts.length}件 (工程候補 ${amcOk} / 不可 ${concepts.filter((c) => c.amcCapable === false).length} / 要判定 ${concepts.filter((c) => c.amcCapable === null).length})`);
  console.log(`→ ${csvPath}`);
  console.log(`→ ${jsonPath}`);

  const topArg = process.argv.indexOf('--top');
  if (topArg >= 0) {
    const n = Number(process.argv[topArg + 1]) || 20;
    for (const c of concepts.slice(0, n)) {
      console.log(`\n#${c.rank} ${c.concept}  [${c.rootCategoryName}]`);
      console.log(`   商品${c.productCount} 購入表示合計${c.totalMonthlySold.toLocaleString()} ブランド${c.brandCount} Top1=${c.top1Brand}(${c.top1SharePct}%)${c.bigBrandInTop3 ? ' ⚠️大手' : ''}`);
      console.log(`   中央価格¥${c.medianPrice} 手数料${c.medianReferralFeePct}% 小型${c.smallSizeRatePct}% 英字ブランド${c.asciiBrandRatePct}% レビュー中央${c.medianReviewCount}`);
      for (const e of c.examples.slice(0, 3)) console.log(`   - ${e.brand} / ${e.title} / 月販${e.monthlySold} / ¥${e.price}`);
    }
  }
})().catch((e) => { console.error(e.message); process.exitCode = 1; });
