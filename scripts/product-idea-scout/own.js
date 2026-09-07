'use strict';
// 自社/AMC商品を Amazon のカテゴリに載せて、競合テーマと同じ土俵に置く。
//
// なぜ必要か:
//   これから採否を貯めても年に数十件しか溜まらない。
//   一方 **自社商品1,153ファミリーは「すでに採用した企画」の実例**で、
//   終売の理由は商品状態からは確定できない。失敗例と断定しない。
//   これを入れて初めて「どういうテーマを採ると当たるか」を過去から言えるようになる。
//
// 入力: data/own-products.json
//        ← bfaith-portal の scripts/export-own-products.cjs が warehouse.db から作る
//          (warehouse.db は miniPC にしか無いので、そちら側で先に実行しておくこと)
// 出力: data/own-asins.jsonl  … ASINごとの Keepa 情報 (冪等・中断しても続きから)
//       output/own_YYYYMMDD.json … ファミリー×テーマの対応 + 実績。push.js がポータルへ送る
//
// 使い方:
//   node own.js            → 未取得ASINを取得して出力まで
//   node own.js --build    → 取得済みぶんだけで出力を作り直す (APIを叩かない)
const fs = require('fs');
const path = require('path');
const ROOT = process.env.SCOUT_HOME || __dirname;
const { keepaCall, DeadlineExceeded } = require(path.join(ROOT, 'lib/keepa'));
const { detectForm } = require('./quality.cjs');

const DATA = path.join(ROOT, 'data');
const OUTDIR = path.join(ROOT, 'output');
fs.mkdirSync(OUTDIR, { recursive: true });
const IN = path.join(DATA, 'own-products.json');
const JSONL = path.join(DATA, 'own-asins.jsonl');

// products.js と同じ考え方 — 強制終了されるとランナー末尾の ping に到達できないので、手前で降りる
const STARTED_AT = Date.now();
const MAX_RUN_HOURS = Number(process.env.SCOUT_OWN_MAX_RUN_HOURS || 6);
const DEADLINE_MS = STARTED_AT + MAX_RUN_HOURS * 3600 * 1000;

const EXIT_DONE = 0;
const EXIT_ERROR = 1;
const EXIT_PARTIAL = 3;

function loadFetched() {
  const map = new Map();
  if (!fs.existsSync(JSONL)) return map;
  for (const line of fs.readFileSync(JSONL, 'utf8').split('\n')) {
    if (!line.trim()) continue;
    try { const o = JSON.parse(line); map.set(o.asin, o); } catch (e) {}
  }
  return map;
}

(async () => {
  if (!fs.existsSync(IN)) {
    console.error(`${IN} がありません。先に miniPC で bfaith-portal の`);
    console.error('  node scripts/export-own-products.cjs');
    console.error('を実行してください (warehouse.db を読んで書き出します)');
    process.exit(EXIT_ERROR);
  }
  const src = JSON.parse(fs.readFileSync(IN, 'utf8'));
  const families = src.families || [];
  const wanted = new Set();
  for (const f of families) for (const a of f.asins || []) wanted.add(a);

  const fetched = loadFetched();
  const todo = [...wanted].filter((a) => !fetched.has(a));
  console.log(`自社ファミリー ${families.length} / ASIN ${wanted.size} / 取得済み ${fetched.size} / 残り ${todo.length}`);

  let stoppedByTime = false;
  if (!process.argv.includes('--build') && todo.length) {
    const stream = fs.createWriteStream(JSONL, { flags: 'a' });
    let got = 0;
    for (let i = 0; i < todo.length; i += 100) {
      if (Date.now() >= DEADLINE_MS) { stoppedByTime = true; break; }
      const batch = todo.slice(i, i + 100);
      let json;
      try {
        // 自社商品は月販フィルタを掛けない。売れていない商品こそ負例として要る。
        // stats/history は不要 (欲しいのはカテゴリと寸法だけ) なのでトークンを最小に
        json = await keepaCall('/product', { asin: batch.join(','), stats: 30, history: 0, rating: 1 },
          null, { deadlineMs: DEADLINE_MS });
      } catch (e) {
        if (e instanceof DeadlineExceeded || e.code === 'DEADLINE_EXCEEDED') {
          console.log(`  ${e.message}`); stoppedByTime = true; break;
        }
        throw e;
      }
      for (const p of json.products || []) {
        const slim = {
          asin: p.asin,
          title: p.title || '',
          brand: p.brand || null,
          rootCategory: p.rootCategory ?? null,
          categoryPath: (p.categoryTree || []).map((c) => c.name).join(' > '),
          monthlySold: p.monthlySold ?? null,
          reviewCount: (p.stats && p.stats.current && p.stats.current[17] >= 0) ? p.stats.current[17] : null,
        };
        stream.write(JSON.stringify(slim) + '\n');
        fetched.set(slim.asin, slim);
        got++;
      }
      if ((i / 100) % 5 === 0) console.log(`  ${Math.min(i + 100, todo.length)}/${todo.length} 問い合わせ (取得 ${got}件)`);
    }
    await new Promise((resolve, reject) => { stream.once('error', reject); stream.end(resolve); });
    console.log(`今回 ${got}件 取得`);
    // ⭐この回で取得したなら、products.js は明日に回す。
    //   own.js と products.js が同じ日に長時間走ると Task Scheduler の20時間上限を超え、
    //   ランナー末尾の ping に到達できない (打ち切られたバッチが無音になる元の障害)。
    if (got > 0) stoppedByTime = true;
  }

  // ── ファミリー → テーマ (categoryPath × 剤型) の対応を作る ──
  const out = [];
  let placed = 0;
  for (const f of families) {
    // ファミリー内のASINのうち、カテゴリが取れたものの多数決で代表カテゴリを決める。
    // 1商品が複数カテゴリに出ていることがあるため、1件目を採らず多い方に寄せる
    const paths = new Map();
    for (const a of f.asins || []) {
      const p = fetched.get(a);
      if (!p || !p.categoryPath) continue;
      paths.set(p.categoryPath, (paths.get(p.categoryPath) || 0) + 1);
    }
    const top = [...paths].sort((a, b) => b[1] - a[1])[0];
    // 剤型は自社の商品名から判定する (Amazonのタイトルではなく自社の呼び名で決める)
    const form = detectForm({ title: f.familyKey, categoryPath: top ? top[0] : '' });
    if (top) placed++;
    out.push({
      familyKey: f.familyKey,
      salesClass: f.salesClass ?? null,
      products: f.products || [],
      sourceUpdatedAt: f.sourceUpdatedAt ?? null,
      skuCount: f.skuCount,
      asinCount: (f.asins || []).length,
      categoryPath: top ? top[0] : null,   // null = Amazonカテゴリが分からなかった
      form: form.label,
      amcCapable: form.amc,
      launchedOn: f.launchedOn,
      lastSoldOn: f.lastSoldOn,
      qty180: f.qty180,
      qtyAll: f.qtyAll,
      activeSkus: f.activeSkus,
      discontinuedSkus: f.discontinuedSkus,
      medianPrice: f.medianPrice,
      outcome: f.outcome,
    });
  }

  const day = new Date(Date.now() + 9 * 3600 * 1000).toISOString().slice(0, 10).replace(/-/g, '');
  const outPath = path.join(OUTDIR, `own_${day}.json`);
  fs.writeFileSync(outPath, JSON.stringify({
    generatedAt: new Date().toISOString(),
    algorithmVersion: 2,
    sourceGeneratedAt: src.generatedAt,
    sourceUpdatedAt: src.sourceUpdatedAt ?? null,
    skuCount: src.skuCount,
    familyCount: out.length,
    placedCount: placed,
    families: out,
  }, null, 1));

  const byOutcome = out.reduce((a, f) => { a[f.outcome] = (a[f.outcome] || 0) + 1; return a; }, {});
  console.log(`ファミリー ${out.length} / Amazonカテゴリに載った ${placed} (${Math.round(placed / out.length * 100)}%)`);
  console.log(`結果: ${JSON.stringify(byOutcome)}`);
  console.log(`→ ${outPath}`);

  if (stoppedByTime) {
    console.log('時間上限で中断しました。次回は続きから取得します');
    process.exitCode = EXIT_PARTIAL;
  }
})().catch((e) => { console.error(e.message); process.exit(EXIT_ERROR); });
