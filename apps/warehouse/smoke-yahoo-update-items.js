/**
 * smoke-yahoo-update-items.js — 商品一括更新API で「価格だけ」変えられるかを実機で確かめる (M3 の M0・第3弾)
 *
 * ★背景: editItem は全項目上書きだと実測で確定した (送らなかった説明文が消えた)。
 *   公式ドキュメントを当たったところ **商品一括更新API (updateItems)** は
 *   「指定された項目だけを更新し、省略された項目は更新しません」とある。
 *   本当にそうなら、M3 は商品名・説明・画像を触らずに価格だけ変えられる。
 *
 * やること:
 *   1. getItem で「前」の全項目を取る
 *   2. updateItems に **price と sale_price だけ** を送る (公式: price を更新する時は両方必須)
 *   3. 「後」を取って全項目を突き合わせる → **説明文などが消えていないか**
 *   4. submitItem でフロント反映 (updateItems は反映しない)
 *   5. publishHistoryDetail (publish_id=0) で未反映項目を見る
 *   6. 価格を元に戻す
 *
 * 安全装置:
 *   - 書き込みは --live のときだけ
 *   - 商品コードは zz- で始まるものだけ + 商品名に「zz検証用」が入っていること (実物で確認)
 *   - ★セール価格が入っている商品では動かさない (空文字を送ると消えるため)
 *   - 送るのは price と sale_price だけ。他の項目は一切送らない
 *
 * 実行 (miniPC):
 *   node apps/warehouse/smoke-yahoo-update-items.js zz-yahoo-m0-0901            … 見るだけ
 *   node apps/warehouse/smoke-yahoo-update-items.js zz-yahoo-m0-0901 --live     … 実際に試す
 */
import 'dotenv/config';
import {
  flattenXml, diff, diffCount, withoutVolatile, collateralOf,
  guardTestCode, guardTestItem, itemPriceOf, itemBaseOf, isDirectChild, TEST_NAME_MARKER,
} from './yahoo-edit-item-probe.js';

const TIMEOUT_MS = 30_000;
const MAX_PRICE = 999999999;
/** 1クエリー/秒の制限があるので、続けて叩く時は間を空ける */
const API_GAP_MS = 1200;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const args = process.argv.slice(2);
const itemCode = (args.find((a) => !a.startsWith('--')) || '').trim();
const live = args.includes('--live');

function requireEnv(...names) {
  for (const n of names) {
    const v = process.env[n];
    if (v && String(v).trim()) return String(v).trim();
  }
  throw new Error(`env ${names.join(' / ')} のどれかが必要です`);
}
const BASE = requireEnv('YAHOO_PROXY_URL', 'YAHOO_PROXY_BASE_URL').replace(/\/+$/, '');
const SECRET = requireEnv('YAHOO_PROXY_SECRET');
const headers = (json = true) => ({ 'X-Proxy-Secret': SECRET, ...(json ? { 'Content-Type': 'application/json' } : {}) });

function oneLine(s) { return String(s || '').slice(0, 260).replace(/\s+/g, ' '); }

async function getRawXml(code) {
  const res = await fetch(`${BASE}/yahoo/get-item-detail`, {
    method: 'POST', headers: headers(),
    body: JSON.stringify({ itemCode: code, raw: true }),
    signal: AbortSignal.timeout(TIMEOUT_MS),
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`getItem HTTP ${res.status}: ${text.slice(0, 300)}`);
  const json = JSON.parse(text);
  if (!json.xml) throw new Error('raw:true に対応していないプロキシです');
  return json.xml;
}

/**
 * 価格を更新して、そのままフロント反映まで行う (プロキシ側が1操作にまとめている)。
 * ★sale_price に空文字を送る = セール価格を消す。消してよいと明示する
 *   (この smoke はセール価格が入っている商品では動かないようにしてある)
 */
async function updatePrice(code, price) {
  const res = await fetch(`${BASE}/yahoo/update-items`, {
    method: 'POST', headers: headers(),
    body: JSON.stringify({
      items: [{ item_code: code, price: String(price), sale_price: '' }],
      clearSalePrice: true,
    }),
    signal: AbortSignal.timeout(TIMEOUT_MS),
  });
  const text = await res.text();
  let json = null;
  try { json = JSON.parse(text); } catch { /* エラー時は XML やテキストが返る */ }
  return { status: res.status, body: text, json };
}

/**
 * 未反映項目に載っている対象キーを集める (publish_id=0)。
 * ★1ページだけ見て includes で探すと、ページ漏れと部分一致で見落とす (Codex R2)。
 *   ページを送りながら TargetKey を集め、値として完全一致で判定する。
 */
async function unpublishedKeys({ maxPages = 50, perPage = 100 } = {}) {
  const keys = new Set();
  let pages = 0;
  let complete = false;
  for (let start = 1; pages < maxPages; start += perPage) {
    const res = await fetch(`${BASE}/yahoo/publish-history?publish_id=0&start=${start}&results=${perPage}`, {
      method: 'GET', headers: headers(false), signal: AbortSignal.timeout(TIMEOUT_MS),
    });
    const text = await res.text();
    if (!res.ok) throw new Error(`publish-history HTTP ${res.status}: ${text.slice(0, 200)}`);
    pages++;
    const flat = await flattenXml(text);
    let found = 0;
    for (const [k, v] of flat) {
      if (/(^|\/)TargetKey\[\d+\]$/.test(k)) { keys.add(String(v).trim().toLowerCase()); found++; }
    }
    if (found < perPage) { complete = true; break; }   // 最後のページまで読めた
    await sleep(API_GAP_MS);
  }
  // ★上限で打ち切ったら「載っていない」と言い切れない (Codex R3)。
  //   全ページ読めた時だけ complete。読み切れていなければ判定に使わない
  return { keys, pages, complete };
}

/**
 * 反映が済んで、未反映一覧からその商品が消えるまで待つ。
 * ★Yahoo の反映は非同期。消えなければ「まだ未反映」と正直に報告して終わる。
 */
async function waitUntilPublished(code, { rounds = 4, waitMs = 15_000 } = {}) {
  const want = String(code).trim().toLowerCase();
  for (let i = 1; i <= rounds; i++) {
    const { keys, pages, complete } = await unpublishedKeys();
    const still = keys.has(want);
    console.log(`  未反映の確認 (${i}/${rounds}): 未反映 ${keys.size} 件 / ${pages}ページ`
      + `${complete ? '' : ' (★読み切れていません)'} / この商品は ${still ? '★まだ載っている' : '載っていない'}`);
    // ★読み切れていないのに「載っていない」= 見落としかもしれない。反映済みとは言わない
    if (!still && complete) return true;
    if (!complete) {
      console.error('  未反映一覧を最後まで読めませんでした。反映できたかを判定できません');
      return false;
    }
    if (i < rounds) await sleep(waitMs);
  }
  return false;
}

function report(label, d) {
  const v = withoutVolatile(d);
  console.log(`\n── ${label} ──`);
  const volatileCount = (d.changed.length + d.removed.length + d.added.length)
    - (v.changed.length + v.removed.length + v.added.length);
  if (v.changed.length === 0 && v.removed.length === 0 && v.added.length === 0) {
    console.log(`  変化なし${volatileCount > 0 ? ` (書き込みで必ず変わる項目 ${volatileCount} 件は除く)` : ''}`);
    return;
  }
  for (const x of v.changed) console.log(`  変わった  ${x.path}: ${JSON.stringify(x.before)} → ${JSON.stringify(x.after)}`);
  for (const x of v.removed) console.log(`  🚨消えた  ${x.path}: ${JSON.stringify(x.before)}`);
  for (const x of v.added) console.log(`  増えた    ${x.path}: ${JSON.stringify(x.after)}`);
  if (volatileCount > 0) console.log(`  (書き込みで必ず変わる項目 ${volatileCount} 件は除いています)`);
}

/** セール価格が入っているか。入っていたら理由を返す (空文字を送ると消えるため動かさない) */
function salePriceGuard(flat, itemBase) {
  const hit = [...flat.entries()].filter(([k]) => isDirectChild(k, itemBase, 'SalePrice'));
  const value = hit.length === 1 ? String(hit[0][1]).trim() : '';
  if (hit.length > 1) return 'セール価格の項目が複数あります。判断できないので動かしません';
  if (value) {
    return `この商品にはセール価格 (${value}) が入っています。`
      + 'sale_price に空文字を送ると消えてしまうため動かしません';
  }
  return null;
}

async function main() {
  const guard = guardTestCode(itemCode);
  if (guard) throw new Error(guard);

  console.log(`商品コード: ${itemCode} / ${live ? '★実際に書き込みます (--live)' : '見るだけ (--live なし)'}`);
  const before = await flattenXml(await getRawXml(itemCode));
  const itemBase = itemBaseOf(before, itemCode);
  const currentPrice = itemPriceOf(before, itemCode);
  console.log(`「前」の項目数: ${before.size} / いまの価格: ${currentPrice ?? '(読めません)'}`);
  for (const tag of ['Name', 'Headline', 'Caption', 'Explanation', 'SalePrice', 'Display', 'EditingFlag']) {
    const hit = [...before.entries()].find(([k]) => isDirectChild(k, itemBase, tag));
    console.log(`  ${tag.padEnd(12)} = ${hit ? oneLine(hit[1]) : '(空)'}`);
  }

  const itemGuard = guardTestItem(before, itemCode);
  const saleGuard = salePriceGuard(before, itemBase);
  for (const g of [itemGuard, saleGuard]) if (g) console.log(`\n⚠️ ${g}`);
  if (live && itemGuard) throw new Error(`書き込みできません。商品名に「${TEST_NAME_MARKER}」を入れてください`);
  if (live && saleGuard) throw new Error(`書き込みできません。${saleGuard}`);
  if (!live) {
    console.log('\n--live を付けると、price と sale_price だけを送って前後を突き合わせます。');
    return;
  }
  if (!Number.isSafeInteger(currentPrice) || currentPrice <= 0 || currentPrice >= MAX_PRICE) {
    throw new Error(`いまの価格 (${currentPrice}) が扱える範囲にないため、書き込みは行いません`);
  }

  const probePrice = currentPrice + 1;
  console.log(`\n★ updateItems に price と sale_price だけを送ります (${currentPrice} → ${probePrice})。`);
  console.log('  商品名・説明・画像は一切送りません。それでも残るかを見ます。');

  let attempted = false;
  try {
    attempted = true;
    const r1 = await updatePrice(itemCode, probePrice);
    console.log(`updateItems + 反映: HTTP ${r1.status} / ${oneLine(r1.body)}`);
    if (!r1.json?.ok) {
      throw new Error(`送信できていません (${oneLine(r1.json?.updateBody || r1.body)})`);
    }
    console.log(`  反映 (submitItem): ${(r1.json.submits || []).map((s) => `${s.item_code}=${s.ok ? 'OK' : 'NG'}`).join(', ') || '(していない)'}`);

    await sleep(API_GAP_MS);
    const after = await flattenXml(await getRawXml(itemCode));
    const d1 = diff(before, after);
    report('price だけ送ったあとの差分', d1);

    const afterPrice = itemPriceOf(after, itemCode);
    if (afterPrice !== probePrice) {
      console.error(`\n⚠️ 価格が ${probePrice} になっていません (実際: ${afterPrice})。判定できません`);
      process.exitCode = 1;
      return;
    }
    const collateral = collateralOf(d1, itemBase);
    console.log(`\n${collateral.length === 0
      ? '✅ 価格だけが変わりました → **updateItems は指定した項目だけを更新する**。'
        + '\n   商品名・説明・画像を触らずに価格改定できます (M3 はこの API を使う)'
      : `🚨 価格以外が ${collateral.length} 項目 変わった/消えた → この API でも巻き添えが出ます`}`);

    await sleep(API_GAP_MS);
    console.log('\n★ 反映されたか (未反映一覧から消えたか) を確かめます');
    await waitUntilPublished(itemCode, { rounds: 2, waitMs: 10_000 });
  } finally {
    if (attempted) {
      await sleep(API_GAP_MS);
      console.log(`\n価格を元に戻します (${probePrice} → ${currentPrice})`);
      try {
        const r3 = await updatePrice(itemCode, currentPrice);
        console.log(`updateItems + 反映: HTTP ${r3.status} / ${oneLine(r3.body)}`);
        // ★戻しの「反映」まで通っていることを確かめる。
        //   ここを見ないと、管理側は戻っているのにフロントには検証中の価格が出たまま終わる
        const submits = r3.json?.submits || [];
        const submitOk = submits.length > 0 && submits.every((s) => s.ok);
        console.log(`  反映 (submitItem): ${submits.map((s) => `${s.item_code}=${s.ok ? 'OK' : 'NG'}`).join(', ') || '(していない)'}`);
        if (!r3.json?.ok || !submitOk) {
          console.error(`🚨 戻しが完了していません (更新 ${r3.json?.updateStatus ?? r3.status} / 反映 ${submitOk ? 'OK' : 'NG'})`);
          console.error(`   Yahoo の管理画面で ${itemCode} の価格を ${currentPrice} に直し、反映してください`);
          process.exitCode = 1;
        }
        await sleep(API_GAP_MS);
        const restored = await flattenXml(await getRawXml(itemCode));
        const back = diff(before, restored);
        report('いまの状態と、最初との差分', back);
        if (diffCount(back) !== 0) {
          console.error(`\n🚨 最初の状態と ${diffCount(back)} 項目 違います。Yahoo の管理画面で確かめてください`);
          process.exitCode = 1;
        }
        // ★管理側が戻っただけでは終わりにしない。フロントに反映されるまで見届ける (Codex R2)。
        //   EditingFlag は差分から外しているので、ここを見ないと「戻った」と誤って言い切る
        console.log('\n★ 戻しが反映されたか (未反映一覧から消えたか) を確かめます');
        const published = await waitUntilPublished(itemCode);
        if (!published) {
          console.error(`\n🚨 ${itemCode} が未反映のまま残っています。`);
          console.error('   管理側の価格は戻っていますが、フロントにはまだ出ていない可能性があります。');
          console.error('   Yahoo の管理画面で反映状況を確かめてください。');
          process.exitCode = 1;
        } else if (diffCount(back) === 0) {
          console.log('\n✅ 商品は最初の状態に戻り、フロントにも反映されています');
        }
      } catch (e) {
        console.error(`🚨 戻す処理でエラー: ${e.message}`);
        console.error(`   Yahoo の管理画面で ${itemCode} の価格を ${currentPrice} に直してください`);
        process.exitCode = 1;
      }
    }
  }
}

main().catch((e) => {
  console.error('エラー:', e.message);
  process.exitCode = 1;
});
