/**
 * 入荷受付チェック (apps/inbound-check) — iPad 画面のレンダリング検証
 *
 * 実行: node scripts/test-inbound-check-render.mjs
 *
 * views/index.html のインラインJSを vm + スタブDOM で実行し、`GET /api/state` の応答を差し替えて
 * **行の状態ごとの見え方と主ボタン**を確かめる。ブラウザを立ち上げずに
 * 「未着手 / 一部 / 数量一致 / 超過 / 確定済み」の出し分けと数量パネルを固定する。
 *
 * 検証項目 (要件定義 v1.3 §11.7 の状態表):
 *   found=0            → [全部あり] + [＋1箱 (N個)]
 *   0<found<planned    → [残りも全部あり] ・ 行が partial ・ 「82 / 106 個 あと24個」
 *   found=planned      → [確認]
 *   found>planned      → [超過で確定] ・「4個 多い」
 *   checked            → [確認済み] (やり直す) ・ ＋1箱 は出さない
 *   入数未登録         → [入数を設定] (無効ボタンにしない)
 *   数量パネル         → 入数・箱数ステッパー・バラ・数えた記録・不足で確定
 */
import fs from 'fs';
import path from 'path';
import vm from 'vm';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const HTML = fs.readFileSync(path.join(__dirname, '..', 'apps', 'inbound-check', 'views', 'index.html'), 'utf8');

let pass = 0, fail = 0;
function ok(cond, label) {
  if (cond) { pass++; console.log(`  ✓ ${label}`); } else { fail++; console.log(`  ✗ ${label}`); }
}

// ── スタブDOM ──────────────────────────────────────────────
// 本物の DOM は使わない。innerHTML に入った文字列を検査できれば、状態ごとの出し分けは確かめられる
function makeEl(sel) {
  const el = {
    _sel: sel, innerHTML: '', textContent: '', value: '', disabled: false,
    style: {}, _classes: new Set(), _handlers: {},
    classList: {
      add: (...c) => c.forEach(x => el._classes.add(x)),
      remove: (...c) => c.forEach(x => el._classes.delete(x)),
      contains: c => el._classes.has(c),
      toggle: (c, on) => { if (on === undefined) { el._classes.has(c) ? el._classes.delete(c) : el._classes.add(c); } else if (on) el._classes.add(c); else el._classes.delete(c); },
    },
    get className() { return [...el._classes].join(' '); },
    set className(v) { el._classes = new Set(String(v).split(/\s+/).filter(Boolean)); },
    addEventListener: (t, fn) => { el._handlers[t] = fn; },
    querySelector: () => null,
    querySelectorAll: () => [],
    closest: () => null,
    contains: () => false,
    getAttribute: () => null,
    focus() {},
  };
  return el;
}

function makeContext(stateJson) {
  const els = new Map();
  const q = sel => {
    if (!els.has(sel)) els.set(sel, makeEl(sel));
    return els.get(sel);
  };
  const store = new Map();
  const ctx = {
    console, Math, Number, String, Array, Object, JSON, Date, Set, Map, Intl, Boolean, Error,
    parseInt, parseFloat, isNaN, Promise, setTimeout, clearTimeout, setInterval: () => 1, clearInterval: () => {},
    encodeURIComponent, decodeURIComponent, AbortController,
    crypto: { randomUUID: () => 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee' },
    localStorage: { getItem: k => (store.has(k) ? store.get(k) : null), setItem: (k, v) => store.set(k, String(v)) },
    location: { href: '/apps/inbound-check/' },
    document: {
      querySelector: q,
      querySelectorAll: () => [],
      addEventListener: () => {},
      get activeElement() { return null; },
      visibilityState: 'visible',
    },
    fetch: async (url) => {
      if (String(url).includes('/api/state')) return { status: 200, json: async () => stateJson };
      return { status: 200, json: async () => ({ ok: true, events: [] }) };
    },
  };
  ctx.window = ctx;
  return { ctx, q };
}

const m = /<script>([\s\S]*?)<\/script>/.exec(HTML);
if (!m) { console.log('  ✗ インラインJSが見つかりません'); process.exit(1); }
const SCRIPT = m[1] + String.fromCharCode(10) + "globalThis.__t = { openLines, render, state, qhist, setFilter: v => { filter = v; } };";

const LINE = (over = {}) => ({
  line_key: 'AR1|1|1', ar_no: 'AR1', product_id: 'asahilabo15g', code_key: 'asahilabo15g',
  product_name: '旭研究所 業務用ハイドロキノン 5% クリーム15g', barcode: '4500000000000',
  planned_qty: 106, received_qty: 106, seq: 1, version: 1, quantity_version: 1,
  check_status: 'unchecked', found_qty: 0, remaining_qty: 106, quantity_relation: 'shortage',
  finalized_result: null, pack_qty: 10, current_pack_qty: null, checked_by: null, checked_at: null,
  info: { code: 'asahilabo15g', irisu: 10, bc_seal: '不要', direct_pick: '無', storage_form: 'そのまま', iroha: '無し', memo: null, version: 1 },
  pick_locs: [{ loc: 'P3FD-005-010-05', qty: 3 }], other_locs: [], loc_source: 'pick',
  image_url: null, expiry_managed: false, expiry_source: 'none',
  dest: { destination: 'bfaith', missing: [], writeBack: true }, prev_checked: null,
  ...over,
});

const STATE = lines => ({
  ok: true,
  batch: { id: 7, csv_generated_at: '2026-09-02T00:28:00+09:00', imported_at: '2026-09-02T00:31:00.000Z', work_date: '2026-09-02', row_count: lines.length, slip_count: 1 },
  slips: [{ batch_id: 7, ar_no: 'AR1', planned_date: '2026-09-01', received_date: '2026-09-01', status: '受付済', line_count: lines.length, seq: 1, checked_count: lines.filter(l => l.check_status === 'checked').length, partial_count: lines.filter(l => l.check_status !== 'checked' && l.found_qty > 0).length }],
  lines,
  day_stale: false,
  totals: {
    lines: lines.length,
    checked: lines.filter(l => l.check_status === 'checked').length,
    partial: lines.filter(l => l.check_status !== 'checked' && l.found_qty > 0).length,
    undecided: 0, toIroha: 0, toIrohaQty: 0,
  },
  workers: [{ code: '0001', name: '山田', staff_id: 1 }],
  me: { session: null, device: { id: 1, label: 'iPad1' }, admin: false },
});

/** 画面を描いて #list の HTML を返す */
async function renderWith(lines, { open = null, worker = true, filter = null } = {}) {
  const { ctx, q } = makeContext(STATE(lines));
  vm.createContext(ctx);
  vm.runInContext(SCRIPT, ctx, { timeout: 5000 });
  if (worker) { ctx.localStorage.setItem('ic_worker_code', '0001'); ctx.localStorage.setItem('ic_worker_name', '山田'); }
  // load() は script 末尾で走っている。マイクロタスクを回して描画完了を待つ
  for (let i = 0; i < 10; i++) await new Promise(r => setTimeout(r, 0));
  if (filter) ctx.__t.setFilter(filter);
  if (open) { ctx.__t.qhist.set(open, []); ctx.__t.openLines.add(open); }
  if (filter || open) ctx.__t.render(true);
  return { html: q('#list').innerHTML, stat: q('#stat').innerHTML, ctx, q };
}

console.log('[1] 行の状態と主ボタン');
{
  let r = await renderWith([LINE()]);
  ok(/data-act="all"[^>]*>全部あり</.test(r.html), '未着手 → 主ボタンは [全部あり]');
  ok(/data-act="plus"[^>]*>＋1箱 \(10個\)</.test(r.html), '未着手 → ＋1箱 (10個) が出る');
  ok(!/class="row[^"]*partial/.test(r.html), '未着手の行に partial は付かない');
  ok(/予定 <b>106<\/b> 個 ・ 1箱 10個/.test(r.html), '予定と1箱の入数が出る');

  r = await renderWith([LINE({ found_qty: 82, remaining_qty: 24, quantity_relation: 'shortage' })]);
  ok(/class="row[^"]* partial/.test(r.html), '一部 → 行に partial (黄色)');
  ok(/82 \/ 106 個 <small>あと 24個<\/small>/.test(r.html), '一部 → 「82 / 106 個 あと 24個」');
  ok(/data-act="all"[^>]*>残りも<br>全部あり</.test(r.html), '一部 → 主ボタンは [残りも全部あり]');
  ok(/data-act="plus"/.test(r.html), '一部 → ＋1箱 も出る');

  r = await renderWith([LINE({ found_qty: 106, remaining_qty: 0, quantity_relation: 'exact' })]);
  ok(/data-act="exact"[^>]*>確認</.test(r.html), '数量一致 → 主ボタンは [確認]');
  ok(/揃いました/.test(r.html) && /qbar full/.test(r.html), '数量一致 → 「揃いました」+ 緑のバー');

  r = await renderWith([LINE({ found_qty: 110, remaining_qty: -4, quantity_relation: 'excess' })]);
  ok(/data-act="excess"[^>]*>超過で確定</.test(r.html), '超過 → 主ボタンは [超過で確定]');
  ok(/110 \/ 106 個 <small>4個 多い<\/small>/.test(r.html), '超過 → 「4個 多い」');

  r = await renderWith([LINE({ check_status: 'checked', found_qty: 106, remaining_qty: 0, quantity_relation: 'exact', finalized_result: 'exact', version: 2, checked_by: '山田', checked_at: '2026-09-02T05:30:00.000Z' })], { filter: 'all' });
  ok(/data-act="reopen"[^>]*>✅ 確認済み</.test(r.html), '確定済み → [確認済み] (押すとやり直す)');
  ok(!/data-act="plus"/.test(r.html), '確定済み → ＋1箱 は出さない');

  r = await renderWith([LINE({ check_status: 'checked', found_qty: 82, remaining_qty: 24, quantity_relation: 'shortage', finalized_result: 'shortage', version: 2, checked_by: '山田', checked_at: '2026-09-02T05:30:00.000Z' })], { filter: 'all' });
  ok(/✅ 不足で<br>確認済み/.test(r.html), '不足で確定済み → 「不足で確認済み」と出る');

  r = await renderWith([LINE({ pack_qty: null, info: { ...LINE().info, irisu: null } })]);
  ok(/data-act="setpack"[^>]*>入数を設定</.test(r.html), '入数未登録 → ＋1箱ではなく [入数を設定] (無効ボタンにしない)');
  ok(/入数 未登録/.test(r.html), '入数未登録 と表示される');
}

console.log('\n[2] 上部の集計');
{
  const r = await renderWith([
    LINE(),
    LINE({ line_key: 'AR1|2|1', seq: 2, found_qty: 5, planned_qty: 12, remaining_qty: 7 }),
    LINE({ line_key: 'AR1|3|1', seq: 3, check_status: 'checked', found_qty: 6, planned_qty: 6, quantity_relation: 'exact', finalized_result: 'exact' }),
  ]);
  ok(/完了 1/.test(r.stat) && /途中 1/.test(r.stat) && /未着手 1/.test(r.stat), '「完了 1 ・途中 1 ・未着手 1」が出る');
  ok(/途中 1/.test(r.html), '伝票カードにも途中件数が出る');
}

console.log('\n[3] 数量パネル (行を開く)');
{
  const r = await renderWith([LINE({ found_qty: 82, remaining_qty: 24 })], { open: 'AR1|1|1' });
  ok(/今から数える箱の入数/.test(r.html), 'パネル: 「今から数える箱の入数」');
  ok(/すでに数えた箱の入数は変わりません/.test(r.html), 'パネル: 過去の箱は変わらないと明記 (訂正は履歴側)');
  ok(/data-q="dec"[^>]*>−</.test(r.html) && /data-q="inc"[^>]*>＋</.test(r.html), 'パネル: 箱数の ＋/− ステッパー');
  ok(/data-act="addboxes"[^>]*>この箱数を足す</.test(r.html), 'パネル: [この箱数を足す]');
  ok(/data-act="addloose"[^>]*>バラを足す</.test(r.html), 'パネル: バラの入力');
  ok(/data-act="shortage"[^>]*>これ以上来ない — 不足 24個 で確認済みにする</.test(r.html),
    'パネル: 不足で確定は**主ボタンではなくパネル内**に置き、結果を文言に書く');
  ok(/数えた記録/.test(r.html), 'パネル: 数えた記録 (訂正・取り消しの入口)');

  const r2 = await renderWith([LINE()], { open: 'AR1|1|1' });
  ok(!/data-act="shortage"/.test(r2.html), '0個のときは不足確定を出さない (押し間違いの余地を作らない)');

  const r3 = await renderWith([LINE({ check_status: 'checked', found_qty: 106, quantity_relation: 'exact', finalized_result: 'exact', version: 2 })], { open: 'AR1|1|1', filter: 'all' });
  ok(/やり直す<\/b>と編集できる/.test(r3.html), '確定済みのパネルは編集させず「やり直す」に誘導する');
  ok(!/data-act="addboxes"/.test(r3.html), '確定済みでは数量を足せない');
}

console.log(`\n${pass} PASS / ${fail} FAIL`);
process.exitCode = fail ? 1 : 0;
