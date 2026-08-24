/**
 * 楽天RMS 価格PATCH の smoke (価格一括改定ツール用)。
 *
 * standardPrice を含む PATCH が「その SKU の価格以外を壊さないか」を、
 * 本番ストアに作った**非公開のテスト商品**で確かめて、最後に削除する。
 * 初回実施 = 2026-08-24 (M0)。結果は AI_reference『価格一括改定ツール_M0実証結果_20260824.md』。
 *
 * 🚨 これは本番の楽天ストアに書き込む。実行は miniPC 上・リポジトリ直下から。
 *    書き込み系は `--live` を付けたときだけ実行され、無ければ dry-run (送信内容の表示のみ)。
 *    テスト商品は必ず hideItem:true (非公開) で作られ、cleanup で削除できる
 *    (miniPC の DELETE ルートと同じく zz- 始まりのみを対象にする安全弁つき)。
 *
 * 使い方:
 *   node apps/warehouse/smoke-rakuten-price-patch.js probe <manageNumber>  … 既存商品の構造を読む (read-only)
 *   node apps/warehouse/smoke-rakuten-price-patch.js create --live         … zz- テスト商品を作る
 *   node apps/warehouse/smoke-rakuten-price-patch.js snapshot              … PATCH前の状態を保存
 *   node apps/warehouse/smoke-rakuten-price-patch.js patch --live          … 1SKUだけ価格をPATCH
 *   node apps/warehouse/smoke-rakuten-price-patch.js verify                … 全項目 deep diff で比較
 *   node apps/warehouse/smoke-rakuten-price-patch.js probe-patch <case> --live … 追加検証 (multi/ghost/same/zero/negative)
 *   node apps/warehouse/smoke-rakuten-price-patch.js cleanup --live        … テスト商品を削除
 *
 * env: RAKUTEN_SERVICE_SECRET / RAKUTEN_LICENSE_KEY (miniPC の .env)
 *      M0_MANAGE_NUMBER で管理番号を変えられる (既定 zz-price-m0-0824)
 */
import 'dotenv/config';
import { writeFileSync, readFileSync, existsSync, appendFileSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { rakutenRequest } from './rakuten-client.js';

const MN = process.env.M0_MANAGE_NUMBER || 'zz-price-m0-0824';
const OUT_DIR = process.env.DATA_DIR || tmpdir();
const SNAP_BEFORE = join(OUT_DIR, 'm0-rakuten-before.json');
const SNAP_AFTER = join(OUT_DIR, 'm0-rakuten-after.json');
const LOG = join(OUT_DIR, 'm0-rakuten-log.jsonl');

const args = process.argv.slice(2);
const cmd = args[0];
const LIVE = args.includes('--live');
const argOf = (i) => args.filter((a) => !a.startsWith('--'))[i];

function log(entry) {
  const line = JSON.stringify({ at: new Date().toISOString(), ...entry });
  try { appendFileSync(LOG, line + '\n'); } catch { /* ログ失敗で本処理は止めない */ }
  console.log(line);
}

const getItem = (mn) => rakutenRequest({ path: `/es/2.0/items/manage-numbers/${mn}` });

// PATCH の影響を見るとき、更新時刻そのものは必ず変わるので比較から除く
const IGNORE_PATHS = new Set(['updated']);

/**
 * 生JSON同士の全項目 deep diff。手で項目を列挙すると見落とすので全部見る。
 * 配列は JSON 文字列で比較 (順序変化も差分として検出したい)。
 */
function diff(before, after, path = '') {
  const out = [];
  if (IGNORE_PATHS.has(path)) return out;
  const bothPlainObj = before && after
    && typeof before === 'object' && typeof after === 'object'
    && !Array.isArray(before) && !Array.isArray(after);
  if (bothPlainObj) {
    const keys = new Set([...Object.keys(before), ...Object.keys(after)]);
    for (const k of keys) out.push(...diff(before[k], after[k], path ? `${path}.${k}` : k));
    return out;
  }
  if (JSON.stringify(before) !== JSON.stringify(after)) {
    out.push({ path: path || '(root)', before, after });
  }
  return out;
}

/** 差分を読みやすく1行に */
function fmtDiff(x) {
  const s = (v) => {
    if (v === undefined) return '(キー無し)';
    const j = JSON.stringify(v);
    return j.length > 160 ? j.slice(0, 160) + `…(${j.length}文字)` : j;
  };
  return `  ${x.path}\n      before: ${s(x.before)}\n      after : ${s(x.after)}`;
}

/** 商品の要約 (probe/snapshot の目視用) */
function summarize(item) {
  const variants = item?.variants || {};
  return {
    manageNumber: item?.manageNumber,
    title: String(item?.title || '').slice(0, 40),
    hideItem: item?.hideItem,
    genreId: item?.genreId,
    topLevelKeys: Object.keys(item || {}).sort(),
    variantSelectors: item?.variantSelectors ?? null,
    variantCount: Object.keys(variants).length,
    variants: Object.fromEntries(Object.entries(variants).slice(0, 4).map(([k, v]) => [k, {
      standardPrice: v?.standardPrice,
      standardPriceType: typeof v?.standardPrice,
      selectorValues: v?.selectorValues ?? null,
      merchantDefinedSkuId: v?.merchantDefinedSkuId ?? null,
      hidden: v?.hidden ?? null,
      keys: Object.keys(v || {}).sort(),
    }])),
  };
}

/**
 * テスト商品のペイロード。2 SKU のバリエーション商品にして
 * 「価格を変えた SKU 以外」「バリエーション軸の定義」が無傷かまで見る。
 */
function buildTestPayload() {
  const sku1 = `${MN}-a`;
  const sku2 = `${MN}-b`;
  // 実データ (aromamist20) の形に合わせる: key は Key0 形式、選択肢は displayValue
  const AXIS = 'Key0';
  // ジャンル 216681 (バッファー・爪やすり) の必須属性。すべて DESCRIPTIVE (自由記述)
  const MANDATORY_ATTRS = [
    { name: 'ブランド名', values: ['テスト'] },
    { name: 'シリーズ名', values: ['テスト'] },
    { name: 'メーカー型番', values: ['M0-TEST'] },
    { name: 'カラー', values: ['テスト'] },
    { name: '原産国／製造国', values: ['日本'] },
  ];
  const mkVariant = (sku, price, sel) => ({
    merchantDefinedSkuId: sku,
    standardPrice: price,
    articleNumber: { exemptionReason: 5 },
    selectorValues: { [AXIS]: sel },
    attributes: MANDATORY_ATTRS,
  });
  return {
    title: 'M0検証用テスト商品(価格一括改定ツール) 販売用ではありません',
    itemNumber: MN,
    tagline: 'M0検証用のタグライン(PATCH後に残るか確認)',
    genreId: '216681',
    hideItem: true,
    itemType: 'NORMAL',
    productDescription: { pc: 'M0検証用。価格PATCHの影響範囲を確認するためのテスト商品です。', sp: 'M0検証用(SP)。' },
    salesDescription: 'M0検証用の販売説明文。',
    payment: { taxIncluded: true, taxRate: 0.1 },
    variantSelectors: [{
      key: AXIS,
      displayName: 'テスト種別',
      values: [{ displayValue: 'エー' }, { displayValue: 'ビー' }],
    }],
    variants: {
      [sku1]: mkVariant(sku1, 1000, 'エー'),
      [sku2]: mkVariant(sku2, 2000, 'ビー'),
    },
  };
}

async function main() {
  if (cmd === 'probe') {
    const target = argOf(1);
    if (!target) { console.error('probe には manageNumber が必要です'); process.exitCode = 1; return; }
    const r = await getItem(target);
    console.log('status:', r.status);
    if (r.status !== 200) { console.log(JSON.stringify(r.data).slice(0, 800)); return; }
    console.log(JSON.stringify(summarize(r.data), null, 2).slice(0, 5000));
    return;
  }

  if (cmd === 'create') {
    const existing = await getItem(MN);
    if (existing.status === 200) {
      console.log(`既に ${MN} が存在します。snapshot から続行してください。`);
      return;
    }
    if (existing.status !== 404) {
      console.log(`既存確認に失敗 (HTTP ${existing.status}) — 中断`);
      process.exitCode = 1;
      return;
    }
    const payload = buildTestPayload();
    console.log('--- PUT payload ---');
    console.log(JSON.stringify(payload, null, 2));
    if (!LIVE) { console.log('\n(dry-run: --live を付けると実行します)'); return; }
    const r = await rakutenRequest({
      path: `/es/2.0/items/manage-numbers/${MN}`, method: 'PUT', body: payload,
      timeoutMs: 90_000, maxAttempts: 1,
    });
    log({ step: 'create', mn: MN, status: r.status, data: r.data });
    return;
  }

  if (cmd === 'snapshot') {
    const r = await getItem(MN);
    if (r.status !== 200) { console.log(`GET 失敗 HTTP ${r.status}`); process.exitCode = 1; return; }
    writeFileSync(SNAP_BEFORE, JSON.stringify(r.data, null, 2));
    console.log(`before を保存: ${SNAP_BEFORE}`);
    console.log(JSON.stringify(summarize(r.data), null, 2));
    return;
  }

  if (cmd === 'patch') {
    if (!existsSync(SNAP_BEFORE)) { console.log('先に snapshot を実行してください'); process.exitCode = 1; return; }
    const before = JSON.parse(readFileSync(SNAP_BEFORE, 'utf8'));
    const keys = Object.keys(before?.variants || {}).sort();
    if (keys.length === 0) { console.log('variants がありません'); process.exitCode = 1; return; }
    const targetSku = keys[0];
    const curRaw = before.variants[targetSku]?.standardPrice;
    const cur = parseInt(String(curRaw ?? ''), 10);
    const next = cur + 111; // 値上げ方向・分かりやすい差分
    // ★ 本番実装と同じ形: サーバ側で body を組み立て直し、standardPrice 以外は一切入れない
    const body = { variants: { [targetSku]: { standardPrice: next } } };
    console.log(`対象SKU: ${targetSku}  ${curRaw} (${typeof curRaw}) -> ${next}`);
    console.log('--- PATCH body ---');
    console.log(JSON.stringify(body, null, 2));
    if (!LIVE) { console.log('\n(dry-run: --live を付けると実行します)'); return; }
    const r = await rakutenRequest({
      path: `/es/2.0/items/manage-numbers/${MN}`, method: 'PATCH', body,
      maxAttempts: 1, // 変更系は自動リトライしない
    });
    log({ step: 'patch', mn: MN, sku: targetSku, from: curRaw, to: next, status: r.status, data: r.data });
    console.log(`PATCH status=${r.status} (204 が成功)`);
    return;
  }

  if (cmd === 'verify') {
    if (!existsSync(SNAP_BEFORE)) { console.log('先に snapshot を実行してください'); process.exitCode = 1; return; }
    const before = JSON.parse(readFileSync(SNAP_BEFORE, 'utf8'));
    const r = await getItem(MN);
    if (r.status !== 200) { console.log(`GET 失敗 HTTP ${r.status}`); process.exitCode = 1; return; }
    writeFileSync(SNAP_AFTER, JSON.stringify(r.data, null, 2));
    const d = diff(before, r.data);
    console.log(`=== 全項目 deep diff: ${d.length} 件 (updated は除外) ===`);
    for (const x of d) console.log(fmtDiff(x));

    const priceDiffs = d.filter((x) => /^variants\.[^.]+\.standardPrice$/.test(x.path));
    const otherDiffs = d.filter((x) => !/^variants\.[^.]+\.standardPrice$/.test(x.path));
    console.log('\n=== 判定 ===');
    console.log(`  価格の変化      : ${priceDiffs.length} 件 ${priceDiffs.map((p) => `${p.path}(${p.before}→${p.after})`).join(', ')}`);
    console.log(`  価格以外の変化  : ${otherDiffs.length} 件`);
    console.log(otherDiffs.length === 0
      ? '  ✅ standardPrice の PATCH は他項目・他SKUを壊さない (per-SKUマージ確認)'
      : '  ❌ 価格以外にも変化あり — 上の差分を確認');
    log({ step: 'verify', mn: MN, diffCount: d.length, priceDiffs, otherDiffs });
    return;
  }

  // 追加検証: 複数SKU同時 / 存在しないSKU混在時の原子性 / 冪等性 / 異常値
  if (cmd === 'probe-patch') {
    const cur = await getItem(MN);
    if (cur.status !== 200) { console.log(`GET 失敗 HTTP ${cur.status}`); process.exitCode = 1; return; }
    const keys = Object.keys(cur.data?.variants || {}).sort();
    const [skuA, skuB] = keys;
    const priceOf = (k) => parseInt(String(cur.data.variants[k]?.standardPrice ?? ''), 10);
    const which = argOf(1);

    const cases = {
      // 2SKUを1回のPATCHで同時更新できるか
      multi: {
        desc: '複数SKU同時更新',
        body: { variants: { [skuA]: { standardPrice: priceOf(skuA) + 1 }, [skuB]: { standardPrice: priceOf(skuB) + 1 } } },
      },
      // 存在しないSKUを混ぜたとき、実在SKUの側だけ適用されてしまわないか (原子性)
      ghost: {
        desc: '存在しないSKU混在 (原子性の確認)',
        body: { variants: { [skuA]: { standardPrice: priceOf(skuA) + 500 }, 'zz-no-such-sku-xxx': { standardPrice: 999 } } },
      },
      // 同じ値をもう一度送ったとき (冪等)
      same: {
        desc: '同一価格の再送 (冪等性)',
        body: { variants: { [skuA]: { standardPrice: priceOf(skuA) } } },
      },
      // 0円 (APIが弾くか、通ってしまうか)
      zero: { desc: '0円', body: { variants: { [skuA]: { standardPrice: 0 } } } },
      // 負数
      negative: { desc: '負数', body: { variants: { [skuA]: { standardPrice: -100 } } } },
    };
    const c = cases[which];
    if (!c) { console.log(`ケース: ${Object.keys(cases).join(' | ')}`); return; }

    console.log(`--- ${which}: ${c.desc} ---`);
    console.log('現在価格:', keys.map((k) => `${k}=${priceOf(k)}`).join(' '));
    console.log('body:', JSON.stringify(c.body));
    if (!LIVE) { console.log('(dry-run: --live で実行)'); return; }
    const r = await rakutenRequest({
      path: `/es/2.0/items/manage-numbers/${MN}`, method: 'PATCH', body: c.body, maxAttempts: 1,
    });
    console.log(`PATCH status=${r.status}`, r.data ? JSON.stringify(r.data).slice(0, 500) : '');
    // 送信後の実際の値を読み戻す
    const after = await getItem(MN);
    const nowPrices = Object.fromEntries(keys.map((k) => [k, after.data?.variants?.[k]?.standardPrice]));
    console.log('送信後の価格:', JSON.stringify(nowPrices));
    log({ step: `probe-patch:${which}`, mn: MN, status: r.status, data: r.data, before: Object.fromEntries(keys.map((k) => [k, priceOf(k)])), after: nowPrices });
    return;
  }

  if (cmd === 'cleanup') {
    const cur = await getItem(MN);
    if (cur.status === 404) { console.log('既に存在しません'); return; }
    if (cur.status !== 200) { console.log(`GET 失敗 HTTP ${cur.status}`); process.exitCode = 1; return; }
    if (cur.data?.hideItem !== true) {
      console.log('⚠ 非公開ではありません。削除前に hideItem:true にしてください');
      process.exitCode = 1;
      return;
    }
    if (!MN.startsWith('zz-')) { console.log('zz- 以外は削除しません'); process.exitCode = 1; return; }
    if (!LIVE) { console.log(`(dry-run) DELETE /es/2.0/items/manage-numbers/${MN}`); return; }
    const r = await rakutenRequest({
      path: `/es/2.0/items/manage-numbers/${MN}`, method: 'DELETE', maxAttempts: 1,
    });
    log({ step: 'cleanup', mn: MN, status: r.status, data: r.data });
    console.log(`DELETE status=${r.status}`);
    return;
  }

  console.log('usage: node apps/warehouse/smoke-rakuten-price-patch.js'
    + ' probe <mn> | create | snapshot | patch | verify | probe-patch <case> | cleanup   [--live]');
}

main().catch((e) => { console.error('ERROR', e?.message || e); process.exitCode = 1; });
