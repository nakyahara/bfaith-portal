/**
 * smoke-yahoo-edit-item.js — Yahoo editItem が「部分更新」か「全項目上書き」かを実機で確かめる (M3 の M0)
 *
 * ★これを確かめないと Yahoo の価格更新は作れない。
 *   全項目上書きなら、価格だけ送ったつもりで **商品名・説明・画像が消える**。
 *   仕様書では分からないので、捨ててよい検証用商品で 1 回だけ実際に試す。
 *
 * やること:
 *   1. getItem で「前」の全項目を取る (応答XMLをそのまま)
 *   2. editItem に **item_code と price だけ** を送る
 *   3. 送信が成功したか・価格が実際に変わったかを確かめる (変わっていなければ何も結論しない)
 *   4. 全項目を突き合わせて、価格以外が変わっていない / 消えていないかを見る
 *   5. 価格を元に戻して、最初の状態と突き合わせる (途中で失敗しても必ず戻しに行く)
 *
 * 安全装置:
 *   - 書き込みは --live のときだけ (既定は「前」の中身を見るだけ)
 *   - 商品コードは **zz- で始まるものだけ**
 *   - さらに **商品名に「zz検証用」が入っていること** を実物で確かめてから書き込む
 *     (コードの取り違えで本番商品を触らないため)
 *   - 送る項目は item_code / price のみ。他の項目は一切送らない (それが検証の目的)
 *   - 価格が整数で読めなければ書き込まない
 *
 * 実行 (miniPC):
 *   node apps/warehouse/smoke-yahoo-edit-item.js zz-yahoo-m0-0901            … 見るだけ
 *   node apps/warehouse/smoke-yahoo-edit-item.js zz-yahoo-m0-0901 --live     … 実際に試す
 *
 * 必要な env: YAHOO_PROXY_URL (または YAHOO_PROXY_BASE_URL) / YAHOO_PROXY_SECRET
 */
import 'dotenv/config';
import {
  flattenXml, diff, collateralOf, guardTestCode, guardTestItem, itemPriceOf, editItemFailure,
  TEST_NAME_MARKER,
} from './yahoo-edit-item-probe.js';

const TIMEOUT_MS = 30_000;

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

async function getRawXml(code) {
  const res = await fetch(`${BASE}/yahoo/get-item-detail`, {
    method: 'POST',
    headers: { 'X-Proxy-Secret': SECRET, 'Content-Type': 'application/json' },
    body: JSON.stringify({ itemCode: code, raw: true }),
    signal: AbortSignal.timeout(TIMEOUT_MS),
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`getItem HTTP ${res.status}: ${text.slice(0, 300)}`);
  let json;
  try { json = JSON.parse(text); } catch { throw new Error(`getItem の応答が JSON ではありません: ${text.slice(0, 200)}`); }
  if (!json.xml) throw new Error('raw:true に対応していないプロキシです (VPS のデプロイがまだです)');
  return json.xml;
}

async function editPrice(code, price) {
  const params = new URLSearchParams({ item_code: code, price: String(price) });
  const res = await fetch(`${BASE}/yahoo/editItem`, {
    method: 'POST',
    headers: { 'X-Proxy-Secret': SECRET, 'Content-Type': 'application/x-www-form-urlencoded' },
    body: params.toString(),
    signal: AbortSignal.timeout(TIMEOUT_MS),
  });
  return { status: res.status, body: await res.text() };
}

function report(label, d) {
  console.log(`\n── ${label} ──`);
  if (d.changed.length === 0 && d.removed.length === 0 && d.added.length === 0) {
    console.log('  変化なし');
    return;
  }
  for (const x of d.changed) console.log(`  変わった  ${x.path}: ${JSON.stringify(x.before)} → ${JSON.stringify(x.after)}`);
  for (const x of d.removed) console.log(`  🚨消えた  ${x.path}: ${JSON.stringify(x.before)}`);
  for (const x of d.added) console.log(`  増えた    ${x.path}: ${JSON.stringify(x.after)}`);
}

async function main() {
  const guard = guardTestCode(itemCode);
  if (guard) throw new Error(guard);

  console.log(`商品コード: ${itemCode} / ${live ? '★実際に書き込みます (--live)' : '見るだけ (--live なし)'}`);
  const beforeXml = await getRawXml(itemCode);
  const before = await flattenXml(beforeXml);
  console.log(`「前」の項目数: ${before.size} (XML ${beforeXml.length} バイト)`);

  const currentPrice = itemPriceOf(before);
  console.log(`いまの価格: ${currentPrice ?? '(読めません)'}`);
  const sample = [...before.entries()].filter(([, v]) => v && v.length < 60).slice(0, 15);
  console.log('項目の例:');
  for (const [k, v] of sample) console.log(`  ${k} = ${v}`);

  const itemGuard = guardTestItem(before);
  if (itemGuard) {
    console.log(`\n⚠️ ${itemGuard}`);
    if (live) throw new Error(`書き込みできません。商品名に「${TEST_NAME_MARKER}」を入れてください`);
  }
  if (!live) {
    console.log('\n--live を付けると、価格だけを送って前後を突き合わせます。');
    return;
  }
  if (!Number.isInteger(currentPrice) || currentPrice <= 0) {
    throw new Error('いまの価格を整数で読めないため、書き込みは行いません');
  }

  const probePrice = currentPrice + 1;
  console.log(`\n★ item_code と price だけを送ります (${currentPrice} → ${probePrice})。他の項目は一切送りません。`);

  // ★送信を「試した」時点から戻しの責任が発生する。
  //   応答が返る前に切れた場合でも Yahoo 側では変わっているかもしれないので、
  //   editPrice の呼び出しごと try の中に入れる (Codex R2)
  let attempted = false;
  try {
    attempted = true;
    const r1 = await editPrice(itemCode, probePrice);
    console.log(`editItem: HTTP ${r1.status} / ${r1.body.slice(0, 200).replace(/\s+/g, ' ')}`);
    const sendFailure = editItemFailure(r1);
    if (sendFailure) throw new Error(`送信できていません (${sendFailure})。書き換わっていないので、この回では何も判定しません`);

    const after = await flattenXml(await getRawXml(itemCode));
    console.log(`「後」の項目数: ${after.size}`);
    const d1 = diff(before, after);
    report('価格だけ送ったあとの差分', d1);

    // ★価格が実際に変わっていなければ、「他が変わっていない」ことに意味は無い
    const afterPrice = itemPriceOf(after);
    if (afterPrice !== probePrice) {
      console.log(`\n⚠️ 価格が ${probePrice} になっていません (実際: ${afterPrice})。`
        + '送信が効いていないので、部分更新かどうかは判定できません');
      return;
    }
    const collateral = collateralOf(d1);
    console.log(`\n${collateral.length === 0
      ? '✅ 価格は変わり、価格以外は変わっていません → editItem は「送った項目だけ変える」= 部分更新'
      : `🚨 価格以外が ${collateral.length} 項目 変わった/消えた → editItem は全項目上書き。価格だけ送ってはいけない`}`);
  } finally {
    // ★finally の中で return しない (待っている例外を握りつぶしてしまう)
    if (attempted) {
      console.log(`\n価格を元に戻します (${probePrice} → ${currentPrice})`);
      try {
        const r2 = await editPrice(itemCode, currentPrice);
        console.log(`editItem: HTTP ${r2.status} / ${r2.body.slice(0, 200).replace(/\s+/g, ' ')}`);
        const restoreFailure = editItemFailure(r2);
        if (restoreFailure) {
          console.error(`🚨 戻せていません (${restoreFailure})。Yahoo の管理画面で価格を ${currentPrice} に直してください`);
          process.exitCode = 1;
        } else {
          const restored = await flattenXml(await getRawXml(itemCode));
          const restoredPrice = itemPriceOf(restored);
          report('元に戻したあと、最初との差分', diff(before, restored));
          if (restoredPrice !== currentPrice) {
            console.error(`🚨 価格が ${currentPrice} に戻っていません (実際: ${restoredPrice})。Yahoo の管理画面で直してください`);
            process.exitCode = 1;
          }
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
