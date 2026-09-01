/**
 * smoke-yahoo-edit-item.js — Yahoo editItem の必須項目を割り出し、送らなかった項目が消えるか確かめる (M3 の M0)
 *
 * ★これを確かめないと Yahoo の価格更新は作れない。
 *   送らなかった項目が消えるなら、価格だけ送ったつもりで **商品名・説明・画像が消える**。
 *
 * 実測 (2026-09-01): item_code と price だけを送ると
 *   HTTP 400 / <Target>path</Target> 「パスは必須です」
 * つまり editItem は「変えたい項目だけ送る」形では呼べない。出品登録と同じで必須項目を毎回要求する。
 *
 * やること:
 *   1. getItem で「前」の全項目を取る (応答XMLをそのまま)
 *   2. item_code + price から始めて送る
 *   3. 「この項目が足りない」と言われたら、その項目を **「前」の値から** 足して再送する
 *      → これを繰り返して **必須項目の最小集合** を割り出す
 *   4. 通ったら「後」を取り、全項目を突き合わせて **送らなかった項目が消えていないか** を見る
 *   5. 価格を元に戻して、最初の状態と突き合わせる
 *
 * 安全装置:
 *   - 書き込みは --live のときだけ (既定は「前」の中身を見るだけ)
 *   - 商品コードは **zz- で始まるものだけ**
 *   - さらに **商品名に「zz検証用」が入っていること** を実物で確かめてから書き込む
 *   - 送る値はすべて **getItem で取った「前」の値そのまま**。当てずっぽうの値は作らない
 *   - 知らない項目名を要求されたら止める (適当な値で商品を書き換えない)
 *   - **実測したエラーコードで弾かれた回だけ** は書き込みが起きていないので、戻しの送信もしない
 *     (もし送らなかった項目が消える仕様なら、戻しのつもりの書き込み自体が画像などを消しかねない)。
 *     見たことのない応答は「書き込まれたかもしれない」に倒して戻しに行く
 *   - 送信の応答が返らなかった回は、戻したあとも時間を置いて確かめ直し、
 *     最後は「あとで管理画面でもう一度確かめてください」と人に引き継ぐ (証明できないため)
 *
 * 実行 (miniPC):
 *   node apps/warehouse/smoke-yahoo-edit-item.js zz-yahoo-m0-0901            … 見るだけ
 *   node apps/warehouse/smoke-yahoo-edit-item.js zz-yahoo-m0-0901 --live     … 実際に試す
 *
 * 必要な env: YAHOO_PROXY_URL (または YAHOO_PROXY_BASE_URL) / YAHOO_PROXY_SECRET
 */
import 'dotenv/config';
import {
  flattenXml, diff, collateralOf, guardTestCode, guardTestItem, itemPriceOf, itemBaseOf,
  TEST_NAME_MARKER,
} from './yahoo-edit-item-probe.js';
import {
  editItemError, isDefiniteRejection, missingFieldTarget, fieldValueFrom, FIELD_SOURCES, KNOWN_REJECT_CODES,
} from './yahoo-edit-item-fields.js';

const TIMEOUT_MS = 30_000;
/** 送信がタイムアウトした後、遅れて効いてくる変更を捕まえるための待ち時間 */
const SETTLE_WAIT_MS = 15_000;
const SETTLE_ROUNDS = 3;
/** 価格の上限 (楽天側のガードと同じ)。これ以上は検証用としても扱わない */
const MAX_PRICE = 999999999;
/** 足せる必須項目の数の上限 (堂々巡りを避ける) */
const MAX_FIELDS_TO_ADD = 15;
/** 送信回数の上限。最後に足した項目でもう一度送るぶん +1 (Codex R1) */
const MAX_SENDS = MAX_FIELDS_TO_ADD + 1;
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

async function callEditItem(fields) {
  const params = new URLSearchParams();
  for (const [k, v] of Object.entries(fields)) params.append(k, String(v));
  const res = await fetch(`${BASE}/yahoo/editItem`, {
    method: 'POST',
    headers: { 'X-Proxy-Secret': SECRET, 'Content-Type': 'application/x-www-form-urlencoded' },
    body: params.toString(),
    signal: AbortSignal.timeout(TIMEOUT_MS),
  });
  return { status: res.status, body: await res.text() };
}

function oneLine(s) { return String(s || '').slice(0, 220).replace(/\s+/g, ' '); }

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

/**
 * 必須と言われた項目を「前」の値から足しながら送る。
 * @returns {{applied:boolean, fields:object, required:string[], stopReason:string|null, uncertain:boolean}}
 */
async function sendAddingRequiredFields(before, itemBase, price) {
  const fields = { item_code: itemCode, price: String(price) };
  const required = [];
  let uncertain = false;

  for (let round = 1; round <= MAX_SENDS; round++) {
    const sending = Object.keys(fields).join(', ');
    console.log(`\n[${round}] 送る項目: ${sending}`);
    let res;
    try {
      res = await callEditItem(fields);
    } catch (e) {
      // 応答が返ってこなかった。Yahoo 側では通っているかもしれない
      uncertain = true;
      return { applied: false, fields, required, uncertain, stopReason: `送信の応答が返りませんでした (${e.message})` };
    }
    const err = editItemError(res);
    if (!err) {
      console.log(`    → 通りました (HTTP ${res.status})`);
      return { applied: true, fields, required, uncertain, stopReason: null };
    }
    console.log(`    → HTTP ${err.status} / Target=${err.target ?? '(なし)'} Code=${err.code ?? '(なし)'} ${oneLine(err.message)}`);

    // ★ループを進めてよいか = 「足りない項目」を名指ししているか。
    //   「戻しを飛ばしてよいか」(isDefiniteRejection) とは別の判断にしてある (Codex R5)
    const target = missingFieldTarget(err);
    if (!target) {
      // 何が起きたか言い切れない = 書き込まれたかもしれない
      uncertain = !isDefiniteRejection(err);
      return { applied: false, fields, required, uncertain,
        stopReason: `どの項目が足りないのか分かりません (HTTP ${err.status} / ${oneLine(err.message)})` };
    }
    if (!KNOWN_REJECT_CODES.has(String(err.code || ''))) {
      // 見たことのないコード。項目を足して進むのはよいが、
      // 「書き込みは起きていない」とは言い切らない (最後に戻しへ行く)
      console.log(`    ⚠️ 見たことのないエラーコードです (${err.code ?? 'なし'})。念のため、最後に戻しを送ります`);
      uncertain = true;
    }
    if (Object.prototype.hasOwnProperty.call(fields, target)) {
      // 既に送っているのに同じ項目を指されている = 値の中身が悪い。当てずっぽうで直さない
      return { applied: false, fields, required, uncertain,
        stopReason: `${target} は既に送っていますが、まだ受け付けられません (${oneLine(err.message)})` };
    }
    if (required.length >= MAX_FIELDS_TO_ADD) {
      return { applied: false, fields, required, uncertain,
        stopReason: `必須項目を ${MAX_FIELDS_TO_ADD} 個足しても通りませんでした (次に求められたのは「${target}」)` };
    }
    if (!FIELD_SOURCES[target]) {
      return { applied: false, fields, required, uncertain,
        stopReason: `知らない項目「${target}」を求められました。値を作れないのでここで止めます (${oneLine(err.message)})` };
    }
    const value = fieldValueFrom(before, itemBase, target);
    if (value === null) {
      return { applied: false, fields, required, uncertain,
        stopReason: `「${target}」の値を「前」の応答から取れませんでした。当てずっぽうでは送りません` };
    }
    console.log(`    → 「${target}」を足します (前の値: ${oneLine(value)})`);
    fields[target] = value;
    required.push(target);
  }
  return { applied: false, fields, required, uncertain,
    stopReason: `${MAX_SENDS} 回送っても通りませんでした` };
}

/**
 * 「前」の応答から **この道具が復元できる** 項目を集める。
 * ★戻しは必須項目だけでは足りない (Codex R1)。もし editItem が「送らなかった項目を消す」なら、
 *   必須項目だけで戻すと、消えた商品名・説明などが消えたまま固定されてしまう。
 * ⚠️これは「全項目」ではない。FIELD_SOURCES に載っている項目だけ。
 *   画像のように別の API でしか戻せないものは含まれない (だから最後に「前」との差分を必ず見る)。
 */
function fullRestoreFields(before, itemBase, price) {
  const fields = { item_code: itemCode, price: String(price) };
  for (const name of Object.keys(FIELD_SOURCES)) {
    if (name === 'item_code' || name === 'price') continue;
    const v = fieldValueFrom(before, itemBase, name);
    if (v !== null) fields[name] = v;
  }
  return fields;
}

async function main() {
  const guard = guardTestCode(itemCode);
  if (guard) throw new Error(guard);

  console.log(`商品コード: ${itemCode} / ${live ? '★実際に書き込みます (--live)' : '見るだけ (--live なし)'}`);
  const beforeXml = await getRawXml(itemCode);
  const before = await flattenXml(beforeXml);
  const itemBase = itemBaseOf(before, itemCode);
  console.log(`「前」の項目数: ${before.size} (XML ${beforeXml.length} バイト)`);

  const currentPrice = itemPriceOf(before, itemCode);
  console.log(`いまの価格: ${currentPrice ?? '(読めません)'}`);
  const sample = [...before.entries()].filter(([, v]) => v && v.length < 60).slice(0, 15);
  console.log('項目の例:');
  for (const [k, v] of sample) console.log(`  ${k} = ${v}`);

  const itemGuard = guardTestItem(before, itemCode);
  if (itemGuard) {
    console.log(`\n⚠️ ${itemGuard}`);
    if (live) throw new Error(`書き込みできません。商品名に「${TEST_NAME_MARKER}」を入れてください`);
  }
  if (!live) {
    console.log('\n--live を付けると、必須項目を足しながら送って前後を突き合わせます。');
    return;
  }
  // ★+1 した後の値まで妥当か見る。上限ぎりぎりだと足した瞬間に扱えない数になり、
  //   「変わっていないのに変わった」と読める (Codex R5)
  if (!Number.isSafeInteger(currentPrice) || currentPrice <= 0 || currentPrice >= MAX_PRICE) {
    throw new Error(`いまの価格 (${currentPrice}) が 1〜${(MAX_PRICE - 1).toLocaleString()} の整数ではないため、書き込みは行いません`);
  }

  const probePrice = currentPrice + 1;
  console.log(`\n★ 価格を ${currentPrice} → ${probePrice} にしながら、必須と言われた項目だけを足していきます。`);
  console.log('  足す値はすべて「前」の応答から取ったそのままの値です (当てずっぽうの値は作りません)。');

  const sent = await sendAddingRequiredFields(before, itemBase, probePrice);
  console.log(`\n必須だった項目: ${sent.required.length > 0 ? sent.required.join(' → ') : '(なし)'}`);

  // ★受け付けられずに返ってきた回は、**戻しの送信もしない** (Codex R4)。
  //   いま確かめようとしているのは「送らなかった項目が消えるか」。もし消えるなら、
  //   戻しのつもりの書き込み自体が、復元できない項目 (画像など) を消しかねない。
  //   書き込みが起きていない以上、読み直して「前」と同じであることを確かめれば足りる。
  const definiteReject = !sent.applied && !sent.uncertain;
  if (definiteReject) {
    console.error(`\n⚠️ 送信は通りませんでした: ${sent.stopReason}`);
    console.error('   Yahoo が受け付けずに返しているので、商品は書き換わっていないはずです。');
    if (sent.required.length > 0) console.error(`   ここまでで分かった必須項目: ${sent.required.join(' → ')}`);
    process.exitCode = 1;
  }

  try {
    if (!sent.applied) throw new Error(`送信の結果が分かりません: ${sent.stopReason}`);

    const after = await flattenXml(await getRawXml(itemCode));
    console.log(`「後」の項目数: ${after.size}`);
    const d1 = diff(before, after);
    report('送ったあとの差分', d1);

    const afterPrice = itemPriceOf(after, itemCode);
    if (afterPrice !== probePrice) {
      console.error(`\n⚠️ 価格が ${probePrice} になっていません (実際: ${afterPrice})。`
        + '送信が効いていないので、何も判定できません');
      process.exitCode = 1;
      return;
    }
    const collateral = collateralOf(d1, itemBase);
    const notSent = Object.keys(FIELD_SOURCES).filter((f) => !(f in sent.fields));
    console.log(`\n送らなかった項目 (この商品にあるもの): ${
      notSent.filter((f) => fieldValueFrom(before, itemBase, f) !== null).join(', ') || '(なし)'}`);
    console.log(`\n${collateral.length === 0
      ? '✅ 価格は変わり、それ以外は変わっていません → **必須項目さえ送れば、送らなかった項目は消えない**'
      : `🚨 価格以外が ${collateral.length} 項目 変わった/消えた → **送らなかった項目は消える**。`
        + '価格更新でも全項目を送り直す設計が要る'}`);
  } finally {
    try {
      let restoreErr = null;
      let r2 = null;
      if (definiteReject) {
        console.log('\n受け付けられずに返ってきたので、戻しの送信はしません。いまの状態だけ確かめます。');
      } else {
        console.log(`\n元の状態に戻します (価格 ${currentPrice} + 「前」から復元できる項目)`);
        const restoreFields = fullRestoreFields(before, itemBase, currentPrice);
        console.log(`  戻しに送る項目: ${Object.keys(restoreFields).join(', ')}`);
        r2 = await callEditItem(restoreFields);
        restoreErr = editItemError(r2);
        if (restoreErr) {
          // 復元できる項目一式が弾かれたら、通った時と同じ項目一式でもう一度 (せめて価格だけでも戻す)
          console.error(`  復元できる項目一式が弾かれました (${oneLine(restoreErr.message || r2.body)})。必須項目だけで戻します`);
          r2 = await callEditItem({ ...sent.fields, price: String(currentPrice) });
          restoreErr = editItemError(r2);
        }
        console.log(`editItem: HTTP ${r2.status} / ${oneLine(r2.body)}`);
      }
      // ★最後は「送れたか」ではなく **いまの状態が「前」と同じか** で判断する
      const restored = await flattenXml(await getRawXml(itemCode));
      const back = diff(before, restored);
      report('いまの状態と、最初との差分', back);
      const stillOff = back.changed.length + back.removed.length + back.added.length;
      if (stillOff === 0) {
        console.log(`\n✅ 商品は最初の状態に戻っています${definiteReject ? ' (そもそも書き換わっていません)' : ''}`);
      } else {
        console.error(`\n🚨 最初の状態と ${stillOff} 項目 違います。Yahoo の管理画面で直してください`);
        if (restoreErr) console.error(`   (戻しの送信も通っていません: ${oneLine(restoreErr.message || r2.body)})`);
        process.exitCode = 1;
      }
      if (sent.uncertain) await settleAfterUncertainSend(itemCode, before, itemBase, currentPrice);
    } catch (e) {
      console.error(`🚨 戻す処理でエラー: ${e.message}`);
      console.error(`   Yahoo の管理画面で ${itemCode} を確かめてください (価格は ${currentPrice} 円)`);
      process.exitCode = 1;
    }
  }
}

/**
 * 応答が返らなかった送信が遅れて効いてくる場合に備え、時間を置いて確かめ直す。
 * 違っていたらもう一度戻す。
 *
 * ★この関数は「大丈夫でした」とは言わない。応答が返らなかった送信について
 *   「この後もう効かない」ことは、どれだけ待っても証明できないため (Codex R4)。
 *   最後は必ず「あとで管理画面で確かめてください」と人に引き継いで終了コード 1 にする。
 */
async function settleAfterUncertainSend(code, before, itemBase, wantPrice) {
  for (let i = 1; i <= SETTLE_ROUNDS; i++) {
    console.log(`\n送信の結果が不明だったので ${SETTLE_WAIT_MS / 1000} 秒待って確かめ直します (${i}/${SETTLE_ROUNDS})`);
    await sleep(SETTLE_WAIT_MS);
    let now;
    try {
      now = await flattenXml(await getRawXml(code));
    } catch (e) {
      console.error(`  確かめられませんでした (${e.message})`);
      continue;
    }
    // ★価格だけでなく全項目を見る。遅れて効いた送信で項目が消えることもある (Codex R2)
    const d = diff(before, now);
    const off = d.changed.length + d.removed.length + d.added.length;
    if (off === 0) { console.log('  最初の状態のままです'); continue; }
    console.error(`  🚨 ${off} 項目が最初と違います (遅れて効いた送信)。もう一度戻します`);
    report('  いまの差分', d);
    try {
      const r = await callEditItem(fullRestoreFields(before, itemBase, wantPrice));
      const f = editItemError(r);
      if (f) throw new Error(oneLine(r.body));
    } catch (e) {
      console.error(`  🚨 戻せませんでした (${e.message})`);
      process.exitCode = 1;
      return;
    }
  }
  // ★ここで「もう大丈夫」とは言えない。応答が返らなかった送信は、待ち時間を何倍にしても
  //   「この後もう効かない」ことを証明できない (Codex R4)。確かめるのを人に引き継ぐ。
  console.error('\n⚠️ この回は送信の結果が分かりませんでした。');
  console.error('   いまの状態は確認しましたが、この後さらに遅れて効く可能性が残ります。');
  console.error(`   あとで Yahoo の管理画面で ${code} を確かめてください (価格 ${wantPrice} 円・商品名や説明が消えていないか)。`);
  process.exitCode = 1;
}

main().catch((e) => {
  console.error('エラー:', e.message);
  process.exitCode = 1;
});
