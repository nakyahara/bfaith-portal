'use strict';
// finder が集めた ASIN の商品詳細を 100件/リクエストで取得 → data/products.jsonl に追記保存
// 中断しても再実行すれば取得済み ASIN はスキップする (冪等)
const fs = require('fs');
const path = require('path');
const ROOT = process.env.SCOUT_HOME || __dirname;
const { keepaCall, tokensLeft, DeadlineExceeded } = require(path.join(ROOT, 'lib/keepa'));
const config = require(path.join(ROOT, 'config.json'));
const { parseProducts, refreshTargets } = require('./quality.cjs');

// ⭐締切はプロセス開始の瞬間に確定させる (Task Scheduler の20時間も起動時から数えるため)。
// ファイル読み込みに数分かかっても、その分だけ安全余裕が減るようにする。
const PROCESS_STARTED_AT = Date.now();

const DATA = path.join(ROOT, 'data');
const OUT = path.join(DATA, 'products.jsonl');

// 終了コード = run-products.bat が jobs-monitor へ打つ ping の種類
const EXIT_DONE = 0;     // 全件終わった      → ping ok
const EXIT_PARTIAL = 3;  // 動いたが未完走    → ping partial (その日の締切は満たす)
const EXIT_ERROR = 1;    // 異常              → batが5分後にリトライ
// ⭐EXIT_IDLE (2026-08-28): 「正常に動いたが、やる仕事が1件も無い」= 空回り。
//   2026-08-07 にペット用品を取り終えてから 8/27 まで20日間、毎日この状態で ok を打ち続け、
//   監視は緑のまま「次のカテゴリを投入し忘れている」ことに誰も気づかなかった。
//   ok を打たないことで dead-man 側が翌日 late (赤) に落ちる。ジョブ自体は異常ではないが、
//   **仕事が入っていないという運用の異常**なので緑にしてはいけない。
const EXIT_IDLE = 4;

// 実行時間の上限。超えたらバッチの切れ目で中断し、EXIT_PARTIAL で「正常に」終わる。
// Task Scheduler の強制終了 (PT20H) に任せると run-products.bat の最終行に到達せず、
// ping が永久に打たれない = 見張り役から「一度も動いていない」に見える
// (2026-08-01 の実行が実際にこれで打ち切られ、翌朝の要対応サマリに載った)。
// 強制終了はあくまで外側の保険なので、それより十分小さくすること。
const TASK_SCHEDULER_LIMIT_HOURS = 20;
function resolveMaxRunHours() {
  const raw = process.env.SCOUT_MAX_RUN_HOURS;
  if (raw === undefined || raw === '') return 19;
  const h = Number(raw);
  if (!Number.isFinite(h) || h <= 0 || h >= TASK_SCHEDULER_LIMIT_HOURS) {
    // 黙って既定値に落とすと「設定したつもり」で気づけない。止めて知らせる。
    console.error(
      `SCOUT_MAX_RUN_HOURS が不正です (${raw})。0より大きく ${TASK_SCHEDULER_LIMIT_HOURS} 未満の数値にしてください`
    );
    process.exit(EXIT_ERROR);
  }
  return h;
}
const MAX_RUN_MS = resolveMaxRunHours() * 60 * 60 * 1000;
const DEADLINE_MS = PROCESS_STARTED_AT + MAX_RUN_MS;

function loadDoneAsins() {
  const done = new Set();
  if (fs.existsSync(OUT)) {
    for (const line of fs.readFileSync(OUT, 'utf8').split('\n')) {
      if (!line.trim()) continue;
      try { done.add(JSON.parse(line).asin); } catch (e) {}
    }
  }
  return done;
}

function slim(p) {
  const cur = (p.stats && p.stats.current) || [];
  const num = (v) => (typeof v === 'number' && v >= 0 ? v : null);
  return {
    asin: p.asin,
    observedAt: new Date().toISOString(),
    parentAsin: p.parentAsin || null,
    title: p.title || '',
    brand: p.brand || null,
    monthlySold: p.monthlySold ?? null,
    rootCategory: p.rootCategory ?? null,
    categoryPath: (p.categoryTree || []).map((c) => c.name).join(' > '),
    leafCategoryId: (p.categoryTree || []).length ? p.categoryTree[p.categoryTree.length - 1].catId : null,
    priceNew: num(cur[1]),
    priceBuyBox: num(cur[18]),
    rating: num(cur[16]),
    reviewCount: num(cur[17]),
    packageMm: [p.packageLength, p.packageWidth, p.packageHeight].map((v) => v ?? null),
    packageWeightG: p.packageWeight ?? null,
    referralFeePct: p.referralFeePercentage ?? null,
    fbaPickPackFee: (p.fbaFees && p.fbaFees.pickAndPackFee) ?? null,
  };
}

async function notifyGChat(text) {
  // .env に GCHAT_WEBHOOK があれば通知 (無ければ何もしない)
  try {
    const { loadEnv } = require(path.join(ROOT, 'lib/keepa'));
    const env = loadEnv();
    if (!env.GCHAT_WEBHOOK) return;
    await fetch(env.GCHAT_WEBHOOK, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text }),
      // 通知先が固まっても実行時間を食い潰さない (失敗しても本体は続ける)
      signal: AbortSignal.timeout(15000),
    });
    console.log('[NOTIFY:status=sent]');
  } catch (e) {
    console.log('[NOTIFY:status=failed] ' + e.message);
  }
}

(async () => {
  const files = fs.readdirSync(DATA).filter((f) => f.startsWith('finder-') && f.endsWith('.json'));
  if (!files.length) { console.error('data/finder-*.json がありません。先に node finder.js'); process.exit(1); }
  const all = new Set();
  const finderMeta = [];
  for (const f of files) {
    const j = JSON.parse(fs.readFileSync(path.join(DATA, f), 'utf8'));
    for (const a of j.asins) all.add(a);
    finderMeta.push({
      id: String((j.category && j.category.id) || f.replace(/^finder-|\.json$/g, '')),
      name: (j.category && j.category.name) || f,
      queryVersion: j.queryVersion || 1,
      complete: j.queryVersion >= 2 ? !!j.complete : null, // v1 は完全性が分からない
      estimatedMissing: j.estimatedMissing ?? null,
    });
  }
  const done = loadDoneAsins();
  const refreshArg = process.argv.indexOf('--refresh-limit');
  const refreshMode = refreshArg >= 0;
  const todo = refreshMode
    ? refreshTargets(fs.existsSync(OUT) ? parseProducts(fs.readFileSync(OUT, 'utf8')) : [], all, Number(process.argv[refreshArg + 1]))
    : [...all].filter((a) => !done.has(a));
  if (refreshMode && todo.length === 0) { console.log('再取得が必要な対象はありません'); return; }
  if (refreshMode) console.log(`既存商品の観測を最大${todo.length}件更新します`);
  console.log(`対象 ${all.size} ASIN / 取得済み ${done.size} / 残り ${todo.length}`);

  // 分母の質をログに出す (「100%」が嘘かどうかを後から言えるようにする)
  for (const m of finderMeta) {
    const state = m.complete === null ? '完全性不明 (旧版finder)' : (m.complete ? '完全' : `不完全 (約${m.estimatedMissing}件 取りこぼし)`);
    console.log(`  分母: ${m.name} = ${state}`);
  }

  // ⭐空回りの検出。「取る対象が1件も無い」= 次のカテゴリが投入されていない (または1周完了)。
  //   ここで ok を打つと監視が緑のままになる (2026-08-07〜27 の20日間がまさにそれ)。
  if (todo.length === 0) {
    const queued = new Set(finderMeta.map((m) => m.id));
    const notQueued = (config.rootCategories || []).filter((c) => !queued.has(String(c.id)));
    const incomplete = finderMeta.filter((m) => m.complete === false);
    const reason = notQueued.length
      ? `未投入カテゴリ ${notQueued.length}件: ${notQueued.map((c) => c.name).join('/')}`
      : (incomplete.length
        ? `全カテゴリ投入済みだが不完全な取得あり: ${incomplete.map((m) => m.name).join('/')}`
        : '全カテゴリ取得済み (1周完了)。次の探索対象を決めてください');
    console.log(`[SCOUT:idle] 取得対象が0件です — ${reason}`);
    console.log('やる仕事が無いため ok は打ちません (監視を緑にしない)。finder を投入するか、タスクを止めてください');
    try { fs.writeFileSync(path.join(DATA, 'last-idle.txt'), reason); } catch (e) {}
    // 未投入カテゴリがあるだけなら、ランナーが直後に finder --next で自動投入する。
    // そこで人を呼ぶのは「オオカミ少年」なので黙る。人を呼ぶのは本当に打ち止めのときだけ。
    if (notQueued.length) {
      process.exitCode = EXIT_IDLE;
      return;
    }
    // 毎日同じことを言われると読み飛ばされる。GChat は3日に1回だけ。
    // (監視の赤は毎日出るので、気づく経路は二重になっている)
    const stamp = path.join(DATA, '.last-idle-notify');
    let notifiedRecently = false;
    try {
      const prev = Number(fs.readFileSync(stamp, 'utf8'));
      notifiedRecently = Number.isFinite(prev) && Date.now() - prev < 3 * 24 * 3600 * 1000;
    } catch (e) {}
    if (!notifiedRecently) {
      await notifyGChat(
        `🟡 *新商品スカウト* 空回りしています (取得対象0件)\n${reason}\n` +
        '次の一手: miniPCで `node finder.js <カテゴリID>` を実行するか、探索方針を決め直してください'
      );
      try { fs.writeFileSync(stamp, String(Date.now())); } catch (e) {}
    }
    process.exitCode = EXIT_IDLE;
    return;
  }
  console.log(`概算トークン: ~${todo.length} (1/ASIN)。tokensLeft を見ながら自動待機します。`);

  // 進捗は3つを別々に数える。「要求した数」で進捗を語ると、
  // Keepa が一部しか返さない日に「進んだつもり」になる
  let requested = 0; // 問い合わせたASIN数
  let returned = 0;  // Keepa が実際に返した商品数
  let stoppedByTime = false;

  const stream = fs.createWriteStream(OUT, { flags: 'a' });
  let streamError = null;
  stream.on('error', (e) => { streamError = e; });
  // 書き込みが追いつかないときは詰まるまで待つ (19時間ぶんのバッファ膨張を防ぐ)
  const writeLine = (line) => new Promise((resolve, reject) => {
    if (stream.write(line)) return resolve();
    stream.once('drain', resolve);
    stream.once('error', reject);
  });

  for (let i = 0; i < todo.length; i += 100) {
    if (Date.now() >= DEADLINE_MS) { stoppedByTime = true; break; }
    const batch = todo.slice(i, i + 100);
    let json;
    try {
      json = await keepaCall(
        '/product',
        { asin: batch.join(','), stats: 30, history: 0, rating: 1 },
        null,
        { deadlineMs: DEADLINE_MS },
      );
    } catch (e) {
      // 締切に当たった = 異常ではない。ここまでの取得を保存して partial で終わる
      if (e instanceof DeadlineExceeded || e.code === 'DEADLINE_EXCEEDED') {
        console.log(`  ${e.message}`);
        stoppedByTime = true;
        break;
      }
      throw e;
    }
    const products = json.products || [];
    for (const p of products) await writeLine(JSON.stringify(slim(p)) + '\n');
    if (streamError) throw streamError;
    requested += batch.length;
    returned += products.length;
    if ((i / 100) % 10 === 0) {
      console.log(`  ${requested}/${todo.length} 問い合わせ (取得 ${returned}件・tokensLeft=${tokensLeft()})`);
    }
  }

  // 「保存済みなので次回は続きから」と言い切るために、書き終わりを見届けてから終了する
  await new Promise((resolve, reject) => {
    stream.once('error', reject);
    stream.end(resolve);
  });
  if (streamError) throw streamError;

  const remaining = todo.length - returned;
  // ランナーが ping の note に載せるための1行 (bat から set /p で読む)。
  // ログを findstr するより確実で、失敗しても本体は落とさない
  try { fs.writeFileSync(path.join(DATA, 'last-remaining.txt'), String(remaining)); } catch (e) {}

  console.log(`[SCOUT:remaining=${remaining}]`);

  if (stoppedByTime) {
    // 異常ではない。EXIT_PARTIAL で抜けることで bat が partial ping を打つ
    // (その日の締切は満たすが「完走」にはならない = 連続すると stalled で鳴る)
    const hours = ((Date.now() - PROCESS_STARTED_AT) / 3600000).toFixed(1);
    console.log(`時間上限で中断 (${hours}時間・今回 ${returned}件取得/${todo.length}件中)。次回は続きから再開します`);
    process.exitCode = EXIT_PARTIAL;
    return;
  }

  // ⭐全バッチを問い合わせても Keepa が返さないASINは取得済みにならない。
  // これを「完走」と呼ぶと、毎日再問い合わせして毎日 ok になり、
  // データが欠けたまま監視が永久に緑になる (Codex R2 high)。残りがあるなら partial。
  if (remaining > 0) {
    console.log(`全件問い合わせましたが ${remaining}件は Keepa から返りませんでした。完了とはせず次回また問い合わせます`);
    console.log(`(この状態が続く場合は、そのASINが取得不能かどうかを人が判断してください)`);
    process.exitCode = EXIT_PARTIAL;
    return;
  }

  console.log(`完了 → ${OUT}。次は node score.js`);
  if (todo.length > 0) {
    await notifyGChat(`🔎 *新商品スカウト* 商品詳細の取得が完了しました (今回 ${returned}件 / 累計 ${all.size}件)。\n次: 結果回収→スコアリング (Claudeに「スカウト結果まとめて」と依頼)`);
  }
})().catch((e) => { console.error(e.message); process.exit(EXIT_ERROR); });
