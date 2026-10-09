/**
 * copy-ne-order-lot.mjs — NE の発注ロット (goods_lot =「発注ロット単位」) を発注アプリ (po_product_attrs.order_lot) に一度だけ写す
 *   (10/9 中原さん Q5 b: 一度全部写し、そのあと NE の値は使わない)
 *
 * 🚨 本番 (Render の DATA_DIR/warehouse-mirror.db) で流すのは中原さんの OK の後だけ。まず dry-run (既定) の数を見せる。
 *
 * 使い方 (Render の Shell・DATA_DIR は Render の環境変数のまま):
 *   node apps/purchase-orders/scripts/copy-ne-order-lot.mjs            dry-run (何も書かない・数と例・止まる商品を出す)
 *   node apps/purchase-orders/scripts/copy-ne-order-lot.mjs --apply    写す + 発注のすすめる数を発注アプリの値に切り替える (order_lot_source = app)。
 *                                                                      今が ne のときだけ (もう app = 何も変えずに終わる)
 *   node apps/purchase-orders/scripts/copy-ne-order-lot.mjs --diff     写した後の見張り (読むだけ): NE と発注アプリの値が違う・発注アプリが空
 *   node apps/purchase-orders/scripts/copy-ne-order-lot.mjs --back-to-ne  戻す (order_lot_source = ne = NE の値に戻す。写した値は消さない)
 *   node apps/purchase-orders/scripts/copy-ne-order-lot.mjs --reapply  写した後 (app) にもう一度写す (写した後に増えた商品など)。
 *       --confirm=<数> が無ければ書かずに「書く件数」を出す・あれば計画し直した件数がその数と同じときだけ書く (app のまま)
 *   --json  結果を JSON で出す
 *
 * 写す元 = 商品管理リストの公開の回 (mirror_pml_snapshot_rows・朝の NE 同期 = raw_ne_products の goods_lot)。その日の NE CSV の上書きは使わない (朝の正の値)。
 * 決まり:
 *   - セット商品 (商品区分 = セット) は写さない (発注アプリは扱わない)
 *   - 行ごとに NE のロットを「正の整数 / 空・0 / おかしい (負・小数・数でない)」に分けてから、同じ商品の全部の行を比べる (Codex #1674 R1 High 3)
 *     - おかしい行がある = 写さない (ne_bad)
 *     - 同じ商品 (大文字小文字・前後の空白だけ違う) の行で分け方か値が違う (12 と 0・12 と空・12 と 20) = 写さない (dup_conflict・人が決める)
 *     - 全部 空・0 = 写さない (ne_empty)。発注アプリの値も空なら、写した後も発注のすすめる数は出ない = 今と同じ (NE が 0 / 空でも今は出ていない)
 *   - 発注アプリにもう発注ロットがある (マスタの入力・マスタ管理で入れた): 同じ = same / 違う = 発注アプリの値を残す (conflict_keep_app・数と例を出す)
 *   - 発注アプリに行がある (グループなど) が発注ロットが空 = その行の発注ロットだけ入れる (fill)
 *   - 行が無い = 発注ロットだけの行を作る (insert・created_via = ne-lot-copy。「紐付け」には数えない)
 *   - 止まる商品 (blockers・Codex #1674 R1 High 2) = 写さない商品 (ne_bad・dup_conflict) のうち、取扱中・発注アプリが空・今は NE の値ですすめる数が出ている
 *     (どれかの行の NE のロットが正)。1 件でもあれば --apply / --reapply は書く前に全体を止める (何も書かない・app にしない)。
 *     解き方 = 発注アプリにその商品の発注ロットを入れる (マスタ管理の商品紐付け・マスタの入力の商品の画面)。人が入れた商品は解いた扱い
 *   - --apply は 1 つの即時の取引: 計画し直す → 止まる商品が無いか → 写す + 1 件ずつ po_audit_log (actor_type = migration) + 最後に order_lot_source = app。
 *     途中で落ちたら何も変わらない
 */
import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL, fileURLToPath } from 'node:url';

const argv = process.argv.slice(2);
const args = new Set(argv);
const MODE = args.has('--apply') ? 'apply' : args.has('--reapply') ? 'reapply' : args.has('--diff') ? 'diff' : args.has('--back-to-ne') ? 'back' : 'dry';
const CONFIRM = (() => { const a = argv.find((x) => x.startsWith('--confirm=')); return a == null ? null : a.slice('--confirm='.length); })();
const JSON_OUT = args.has('--json');
const ACTOR = 'copy-ne-order-lot';
const SAMPLE = 20;

/** 1 行の NE のロット → { s: 'pos', n } 正の整数 / { s: 'empty' } 空・0 / { s: 'bad', raw } 負・小数・数でない */
export function lotState(v) {
  if (v == null || (typeof v === 'string' && v.trim() === '')) return { s: 'empty' };
  const n = Number(v);
  if (!Number.isFinite(n)) return { s: 'bad', raw: v };
  if (n === 0) return { s: 'empty' };
  if (n > 0 && Number.isInteger(n)) return { s: 'pos', n };
  return { s: 'bad', raw: v };
}
const stateWord = (st) => (st.s === 'pos' ? String(st.n) : st.s === 'empty' ? '空' : `おかしい(${st.raw})`);

/** 写す計画 (読むだけ)。db = warehouse-mirror.db・deps = { normProductCode, pmlRows: 公開の回の行 } */
export function planCopy(db, { normProductCode, pmlRows }) {
  const attrs = new Map(db.prepare('SELECT * FROM po_product_attrs').all().map((a) => [a.product_key, a]));
  const byKey = new Map();
  for (const r of pmlRows) {
    if (String(r['商品区分'] || '').trim() === 'セット') continue;
    const key = normProductCode(r['商品コード']);
    if (!key) continue;
    if (!byKey.has(key)) byKey.set(key, []);
    byKey.get(key).push(r);
  }
  const out = { insert: [], fill: [], same: [], conflict_keep_app: [], ne_empty: [], ne_bad: [], dup_conflict: [], blockers: [] };
  let neEmptyActive = 0;
  for (const [key, rows] of byKey) {
    const states = rows.map((r) => lotState(r['発注ロット単位']));
    const active = rows.some((r) => String(r['取扱区分'] || '') === '取扱中');
    const code = String((rows.find((r) => String(r['取扱区分'] || '') === '取扱中') || rows[0])['商品コード']).trim();
    const a = attrs.get(key) || null;
    const app = a && a.order_lot != null ? Number(a.order_lot) : null;
    // 今の発注の計算 (logic.js computeProduct は行ごとに Number(発注ロット単位) > 0 を使う) ですすめる数が出ている行があるか
    const usableNow = rows.some((r) => Number(r['発注ロット単位']) > 0);
    const item = { key, code, active, app, ne: null };
    const block = (x) => { if (active && app == null && usableNow) out.blockers.push(x); };
    if (states.some((st) => st.s === 'bad')) {
      const x = { ...item, ne_raw: states.map(stateWord) };
      out.ne_bad.push(x); block(x);
      continue;
    }
    const kinds = [...new Set(states.map(stateWord))];
    if (kinds.length > 1) {
      const x = { ...item, ne_values: kinds };
      out.dup_conflict.push(x); block(x);
      continue;
    }
    if (states[0].s === 'empty') { out.ne_empty.push(item); if (active && app == null) neEmptyActive++; continue; }
    item.ne = states[0].n;
    if (app != null) { (app === item.ne ? out.same : out.conflict_keep_app).push(item); continue; }
    (a ? out.fill : out.insert).push(item);
  }
  return { ...out, neEmptyActive, products: byKey.size };
}

/** 計画どおりに写す (呼び手の取引の中で)。戻り値 = 書いた件数 */
export function applyCopy(db, plan, { audit, now = new Date().toISOString() }) {
  const ins = db.prepare(`INSERT INTO po_product_attrs (product_key, product_code, order_lot, created_via, created_at, updated_at)
                          VALUES (?,?,?,'ne-lot-copy',?,?)`);
  const fill = db.prepare('UPDATE po_product_attrs SET order_lot=?, updated_at=? WHERE product_key=? AND order_lot IS NULL');
  let n = 0;
  for (const x of plan.insert) {
    ins.run(x.key, x.code, x.ne, now, now);
    audit(db, { actorType: 'migration', actor: ACTOR, action: 'po_order_lot_copy', resource: `attrs:${x.key}`, detail: { via: 'ne-lot-copy', code: x.code, before: null, after: { order_lot: x.ne } } });
    n++;
  }
  for (const x of plan.fill) {
    if (fill.run(x.ne, now, x.key).changes !== 1) throw new Error(`写す途中で ${x.code} の発注ロットが入った (ほかの人が直した)。何も変えていません。もう一度 dry-run から`);
    audit(db, { actorType: 'migration', actor: ACTOR, action: 'po_order_lot_copy', resource: `attrs:${x.key}`, detail: { via: 'ne-lot-copy', code: x.code, before: { order_lot: null }, after: { order_lot: x.ne } } });
    n++;
  }
  return n;
}

/** 写した後の見張り (読むだけ): 取扱中で発注アプリが空 / NE と違う */
export function diffAfter(db, { normProductCode, pmlRows }) {
  const attrs = new Map(db.prepare('SELECT product_key, order_lot FROM po_product_attrs').all().map((a) => [a.product_key, a.order_lot]));
  const missing = []; const diff = []; const seen = new Set();
  for (const r of pmlRows) {
    if (String(r['商品区分'] || '').trim() === 'セット') continue;
    const key = normProductCode(r['商品コード']);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    const app = attrs.get(key) == null ? null : Number(attrs.get(key));
    const ne = Number(r['発注ロット単位']) > 0 ? Number(r['発注ロット単位']) : null;
    const item = { code: String(r['商品コード']).trim(), app, ne };
    if (app == null && String(r['取扱区分'] || '') === '取扱中') missing.push(item);
    else if (app != null && ne != null && app !== ne) diff.push(item);
  }
  return { missing, diff };
}

function summary(plan) {
  return {
    products: plan.products,
    insert: plan.insert.length, fill: plan.fill.length, same: plan.same.length,
    conflict_keep_app: plan.conflict_keep_app.length, ne_empty: plan.ne_empty.length, ne_empty_active_and_app_empty: plan.neEmptyActive,
    ne_bad: plan.ne_bad.length, dup_conflict: plan.dup_conflict.length, blockers: plan.blockers.length,
  };
}
const neWords = (x) => (x.ne_values || x.ne_raw || []).join(' / ');

async function main() {
  // 🚨 DATA_DIR が無い = 作業の場所の data/ に新しい空の DB を作ってしまう = 断る (本番は Render の DATA_DIR)
  if (!process.env.DATA_DIR) { console.error('DATA_DIR が設定されていません (Render の Shell では設定済み)。どの warehouse-mirror.db か分からないので止めます'); process.exit(2); }
  const file = path.join(process.env.DATA_DIR, 'warehouse-mirror.db');
  if (!fs.existsSync(file)) { console.error(`${file} がありません。止めます (新しい DB は作らない)`); process.exit(2); }
  if (MODE === 'reapply' && CONFIRM != null && !/^\d+$/.test(CONFIRM)) { console.error('--confirm= には書く件数 (数) を入れてください'); process.exit(2); }
  const WORK = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../..');
  const imp = (p) => import(pathToFileURL(path.join(WORK, p)).href);
  const { initMirrorDB } = await imp('apps/warehouse-mirror/db.js');
  initMirrorDB();
  const { getDB, normProductCode } = await imp('apps/purchase-orders/db.js');
  const { loadPml } = await imp('apps/purchase-orders/logic.js');
  const { audit, getSetting, setSetting } = await imp('apps/purchase-orders/ledger.js');
  const db = getDB();
  const print = (o, lines) => { if (JSON_OUT) console.log(JSON.stringify(o, null, 2)); else console.log(lines.join('\n')); };
  const sourceNow = () => (getSetting('order_lot_source') === 'app' ? 'app' : 'ne');
  const source = sourceNow();

  if (MODE === 'back') {
    setSetting('order_lot_source', 'ne', { actorType: 'migration', actor: ACTOR, reason: '発注ロットを NE の値に戻す (copy-ne-order-lot --back-to-ne)' });
    print({ ok: true, before: source, after: 'ne' }, [`order_lot_source: ${source} → ne (発注のすすめる数は NE の値に戻りました。写した値は残っています)`]);
    return;
  }
  const { pub, rows } = loadPml();
  if (!pub) { console.error('商品管理リストの公開の回がありません。止めます'); process.exit(2); }
  const head = [`DB: ${file}`, `商品管理リストの公開の回: ${pub.run_id} (as_of ${pub.as_of_date || '?'}・NE 同期 ${pub.src_ne_products_synced_at || '?'})`, `いまの出どころ order_lot_source: ${source}`];
  const blockerLines = (list) => list.slice(0, SAMPLE).map((x) => `  ${x.code}  NE=${neWords(x)}`);

  if (MODE === 'diff') {
    const d = diffAfter(db, { normProductCode, pmlRows: rows });
    print({ source, missing: d.missing.length, diff: d.diff.length, samples: { missing: d.missing.slice(0, SAMPLE), diff: d.diff.slice(0, SAMPLE) } }, [
      ...head,
      `取扱中で発注アプリの発注ロットが空 (すすめる数が出ない): ${d.missing.length} 件`, ...d.missing.slice(0, SAMPLE).map((x) => `  ${x.code}  NE=${x.ne ?? '空'}`),
      `NE と発注アプリが違う: ${d.diff.length} 件`, ...d.diff.slice(0, SAMPLE).map((x) => `  ${x.code}  発注アプリ=${x.app}  NE=${x.ne}`),
    ]);
    return;
  }

  if (MODE === 'dry') {
    const plan = planCopy(db, { normProductCode, pmlRows: rows });
    const s = summary(plan);
    const ex = (k, f) => plan[k].slice(0, SAMPLE).map(f);
    print({ mode: 'dry-run', source, ...s, samples: Object.fromEntries(['insert', 'fill', 'conflict_keep_app', 'ne_bad', 'dup_conflict', 'blockers'].map((k) => [k, plan[k].slice(0, SAMPLE)])) }, [
      ...head, '── dry-run (何も書いていません) ──',
      ...(source === 'app' ? ['⚠ もう写してあります (order_lot_source = app)。--apply は何もしません。もう一度写すときは --reapply'] : []),
      `商品 (セットを除く・同じコードは 1 つ): ${s.products}`,
      `写す (行を作る・発注ロットだけ): ${s.insert}`, `写す (今ある行の空の発注ロットを埋める): ${s.fill}`,
      `もう同じ値: ${s.same}`,
      `発注アプリの値を残す (NE と違う): ${s.conflict_keep_app}`, ...ex('conflict_keep_app', (x) => `  ${x.code}  発注アプリ=${x.app}  NE=${x.ne}`),
      `NE が空 / 0 (写さない): ${s.ne_empty} (うち取扱中で発注アプリも空 = 写した後もすすめる数が出ない (今と同じ): ${s.ne_empty_active_and_app_empty})`,
      `NE の値がおかしい (負・小数・数でない。写さない): ${s.ne_bad}`, ...ex('ne_bad', (x) => `  ${x.code}  NE=${neWords(x)}`),
      `同じ商品が 2 行以上でロットが違う (12 と 0・12 と空も。写さない): ${s.dup_conflict}`, ...ex('dup_conflict', (x) => `  ${x.code}  NE=${neWords(x)}`),
      `止まる商品 (取扱中・発注アプリが空・今は NE の値ですすめる数が出ている・上の 2 つのうち): ${s.blockers} 件`, ...blockerLines(plan.blockers),
      ...(s.blockers ? ['  → 1 件でもあると --apply は止まります。発注アプリにその商品の発注ロットを入れてから (マスタ管理の商品紐付け・マスタの入力の商品の画面)'] : []),
      '', '中原さんの OK の後に --apply で写します (写した最後に発注のすすめる数を発注アプリの値に切り替えます)',
    ]);
    return;
  }

  // --apply / --reapply: 計画を取引の中で作り直してから書く (dry-run の後に変わった分も入る)
  const STOP = 'copy-ne-order-lot-stop';
  const stop = (code, body) => { const e = new Error(STOP); e.stop = { code, body }; return e; };
  let result;
  try {
    result = db.transaction(() => {
      const src = sourceNow();
      // 一度だけ (Codex #1674 R1 Medium 2): --apply は ne のときだけ。もう app = 何も変えない (写した後に増えた商品を NE から埋めない)
      if (MODE === 'apply' && src === 'app') return { skipped: 'already_app' };
      if (MODE === 'reapply' && src !== 'app') throw stop(2, { ok: false, reason: 'not_app', error: 'まだ写していません (order_lot_source = ne)。最初は --apply' });
      const plan = planCopy(db, { normProductCode, pmlRows: loadPml().rows });
      // 止まる商品があれば書く前に全体を止める (Codex #1674 R1 High 2)
      if (plan.blockers.length) throw stop(3, { ok: false, reason: 'blocked', blocked: plan.blockers, ...summary(plan) });
      const toWrite = plan.insert.length + plan.fill.length;
      if (MODE === 'reapply') {
        if (CONFIRM == null) return { preview: true, to_write: toWrite, ...summary(plan) };
        if (Number(CONFIRM) !== toWrite) throw stop(2, { ok: false, reason: 'confirm_mismatch', to_write: toWrite, confirm: Number(CONFIRM) });
      }
      const written = applyCopy(db, plan, { audit });
      if (MODE === 'apply') {
        setSetting('order_lot_source', 'app', { actorType: 'migration', actor: ACTOR, reason: `NE の発注ロットを発注アプリに写した (${written} 件)。以後は発注アプリの値だけを使う` });
      }
      audit(db, { actorType: 'migration', actor: ACTOR, action: MODE === 'apply' ? 'po_order_lot_copy_done' : 'po_order_lot_recopy_done', resource: 'setting:order_lot_source', detail: summary(plan) });
      return { written, ...summary(plan) };
    }).immediate();
  } catch (e) {
    if (!(e && e.message === STOP && e.stop)) throw e;
    const { code, body } = e.stop;
    const lines = body.reason === 'blocked'
      ? [...head, `── 止めました (何も書いていません・order_lot_source は ${source} のまま) ──`, `止まる商品 ${body.blocked.length} 件 (取扱中・発注アプリが空・NE の値が重なり / おかしい):`, ...blockerLines(body.blocked),
        '発注アプリにその商品の発注ロットを入れてから、もう一度 dry-run → --apply']
      : body.reason === 'confirm_mismatch' ? [...head, `止めました: 書く件数は ${body.to_write} 件です (--confirm=${body.confirm} と違う)。何も書いていません`]
        : [...head, `止めました: ${body.error}`];
    print(body, lines);
    process.exitCode = code;
    return;
  }
  if (result.skipped === 'already_app') {
    print({ mode: MODE, ok: true, skipped: 'already_app' }, [...head, 'もう写してあります (order_lot_source = app)。何も変えていません。写した後に増えた商品を写すときは --reapply (書く件数の確かめつき)']);
    return;
  }
  if (result.preview) {
    print({ mode: MODE, ok: true, ...result }, [...head, `── 写し直しの確かめ (何も書いていません) ──`, `書く件数: ${result.to_write} 件 (行を作る ${result.insert}・埋める ${result.fill})`,
      `書くときは: --reapply --confirm=${result.to_write}`]);
    return;
  }
  print({ mode: MODE, ok: true, ...result, source_after: 'app' }, [...head, `── ${MODE === 'apply' ? '写しました' : '写し直しました'} ──`, `書いた: ${result.written} 件 (行を作る ${result.insert}・埋める ${result.fill})`,
    `発注アプリの値を残した: ${result.conflict_keep_app}・NE が空: ${result.ne_empty}・おかしい: ${result.ne_bad}・重なり: ${result.dup_conflict}`,
    'order_lot_source: app (発注のすすめる数は発注アプリの発注ロットだけを使います。戻す = --back-to-ne)']);
}

const isMain = process.argv[1] && path.resolve(process.argv[1]) === path.resolve(fileURLToPath(import.meta.url));
if (isMain) main().catch((e) => { console.error(e && e.stack || e); process.exit(1); });
