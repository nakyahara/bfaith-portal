#!/usr/bin/env node
/**
 * once-variation-group-from-ne.mjs — 一回きり: NE で直接作った色違いのまとまり (例 fuwear = fuwear-bk / -nd / -wh) を、
 * Company DB の「色違いのまとまり」の記録 (予約・軸・選択肢・子の選択肢・revision・product-hub のカードの知らせ) にする。
 * (中原さん 10/12「fuwear-bk/-nd/-wh は自社 DB でバリエーション登録できなかった日の新商品で、仕方なく NE に登録した。データ直して・ボードにカードも」)
 *
 * 🚨 一般の機能ではない (台帳 config/jobs-registry.mjs の temporary_asset 'once-variation-group-from-ne' = 期限の後に消す)。
 *    代表 (親) は変えない: 夜間ロードが NE の代表商品コードから付けた親 (札 fuwear・帰属 load) をそのまま使う = 照合 ② の親の数え・widen・夜間ロードは何も変わらない。
 *    書くのは「まとまりの表」(0067・company だけの値・夜間ロードは触らない) と知らせ (0066) だけ:
 *      予約 ops.variation_group_codes (source = load = NE の代表から)・revision の行 (ops._variation_group_resolve)・
 *      軸 1 (core.variation_axes)・選択肢 (core.variation_options = NE のコードの末尾 -BK・名前 = 商品名の最後の【】)・子の選択肢 (core.sku_variation_choices)・
 *      revision 1 と完全なスナップショット (ops._variation_bump = ops.product_hub_outbox の group_snapshot) → product-hub のボードを開くと今の取り込み (cdb-group-intake.js) が 1 枚のカードにする
 *    = 色違いの登録の画面で作ったまとまりと同じ形 (中原さんの決定 3B「NE で作った古いまとまりは最初の 1 回だけ前からの子の色名を人が確かめる」= この dry-run の表を中原さんが見て OK する)
 *
 * 使い方 (miniPC の PowerShell・リポジトリ直下):
 *   # 1. 読むだけ (既定)。env COMPANY_DB_WATCH_URL (watcher) か --url。今の姿・作るもの・計画のハッシュを出す。足りない / 違う所があれば「直さない」理由を出して exit 1
 *   node -r dotenv/config scripts/company-db/once-variation-group-from-ne.mjs --group fuwear [--axis-name カラー] [--names "-BK=ブラック,-ND=ヌード,-WH=ホワイト"] [--json out.json]
 *   # 2. 書く (🚨 dry-run の表を中原さんが見て OK の後だけ)。env COMPANY_DB_URL (DB の持ち主 = まとまりの表は関数か持ち主しか書けない) か --url。
 *   #    1 つの取引で鍵 → 計画を読み直す → ハッシュが --expect-hash と同じときだけ書く → 書いた後を確かめる → commit。1 つでも違えば巻き戻す (何も変えない)
 *   node -r dotenv/config scripts/company-db/once-variation-group-from-ne.mjs --group fuwear --apply --expect-hash <64 桁> --actor <人のメール> --yes [--reason "<理由>"]
 * 終了コード: 0 = dry-run を出した (直せる) / apply で commit・1 = 直せない (前提が違う) / apply を巻き戻した / つながらない・2 = 引数が違う
 */
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { openPgClient, pgAdapter } from './migrate.mjs';

const fail = (message) => Object.assign(new Error(message), { code: 'ONCE_ARGS' });
const OPTION_CODE_RE = /^-[A-Za-z0-9]{1,10}$/;
const CODE_RE = /^[A-Za-z0-9_-]{1,30}$/;
const CTRL_RE = new RegExp(`[\\x00-\\x1f\\x7f${String.fromCharCode(0x2028, 0x2029)}]`);
const DEFAULT_REASON = 'NE で直接作った色違いのまとまりを Company DB のまとまりの記録にする (一回きり・10/12 中原さん)';
const SOURCE_SYSTEM = 'once_variation_group';
/** 選択肢名の一意の鍵 (DB の name_key = btrim(normalize(name, NFKC)) と同じ) */
const labelKey = (s) => String(s ?? '').normalize('NFKC').replace(/^ +| +$/g, '');
/** 名前の形 (DB の ops.variation_label_problem と同じ: 前後の空白なし・制御文字なし・1〜100 字) */
const labelProblem = (s) => (typeof s !== 'string' || !s.length || [...s].length > 100 || s !== s.trim() || CTRL_RE.test(s) || !labelKey(s) ? 'label' : null);
/** 商品名の最後の【】の中 (NE の「共通の商品名【選択肢名】」の形) */
export function lastBracket(name) {
  const m = String(name ?? '').match(/【([^【】]+)】\s*$/);
  return m ? m[1].trim() : null;
}

/** 引数を読む。誤り = ONCE_ARGS */
export function parseOnceArgs(argv) {
  const o = { group: null, axisName: 'カラー', names: {}, apply: false, yes: false, expectHash: null, actor: null, reason: DEFAULT_REASON, json: null, url: null };
  const need = (i, name) => { const v = argv[i + 1]; if (v == null || v.startsWith('--')) throw fail(`${name} の後に値が要る`); return v; };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--group') { o.group = need(i, a); i++; }
    else if (a === '--axis-name') { o.axisName = need(i, a); i++; }
    else if (a === '--names') {
      for (const part of need(i, a).split(',').map((s) => s.trim()).filter(Boolean)) {
        const k = part.indexOf('=');
        if (k < 1) throw fail(`--names は "-BK=ブラック,-ND=ヌード" の形 (${part})`);
        const code = part.slice(0, k).trim(); const name = part.slice(k + 1).trim();
        if (!OPTION_CODE_RE.test(code)) throw fail(`--names の選択肢番号 ${code} は「-」+ 英数字 1〜10 字`);
        if (labelProblem(name)) throw fail(`--names の ${code} の名前は 1〜100 字 (制御文字なし)`);
        o.names[code] = name;
      }
      i++;
    }
    else if (a === '--apply') o.apply = true;
    else if (a === '--yes') o.yes = true;
    else if (a === '--expect-hash') { const v = need(i, a); if (!/^[0-9a-f]{64}$/.test(v)) throw fail('--expect-hash は dry-run が出した 64 桁の 16 進'); o.expectHash = v; i++; }
    else if (a === '--actor') { o.actor = need(i, a); i++; }
    else if (a === '--reason') { o.reason = need(i, a).trim(); i++; }
    else if (a === '--json') { o.json = need(i, a); i++; }
    else if (a === '--url') { o.url = need(i, a); i++; }
    else throw fail(`知らない引数: ${a}`);
  }
  if (!o.group || !CODE_RE.test(o.group)) throw fail('--group <まとまりのコード (例 fuwear)> が要る (英数字と _- の 1〜30 字)');
  if (labelProblem(o.axisName)) throw fail('--axis-name は 1〜100 字 (前後の空白・制御文字なし)');
  if (!o.reason || o.reason.length > 200 || CTRL_RE.test(o.reason)) throw fail('--reason は 1〜200 字 (制御文字なし)');
  if (o.apply) {
    if (o.expectHash == null) throw fail('--apply は --expect-hash <dry-run が出した計画のハッシュ> が要る');
    if (!o.actor || !/^[^\s@]+@[^\s@]+$/.test(o.actor)) throw fail('--apply は --actor <人のメール> が要る');
    if (!o.yes) throw fail('--apply は --yes が要る (dry-run の表を中原さんが見て OK の後だけ)');
  }
  return o;
}

const stable = (v) => (Array.isArray(v) ? `[${v.map(stable).join(',')}]`
  : v && typeof v === 'object' ? `{${Object.keys(v).sort().map((k) => `${JSON.stringify(k)}:${stable(v[k])}`).join(',')}}` : JSON.stringify(v));

/**
 * 今の Company DB を読んで、作るもの (計画) と「直さない」理由を決める (読むだけ)。
 * 戻り値 { plan: { group, axis, options, children } | null, problems: [文], hash }。problems が 1 つでもあれば直さない
 */
export async function readOncePlan(db, { group, axisName = 'カラー', names = {} }) {
  const problems = [];
  const norm = group.toLowerCase();
  const tags = (await db.query(`select p.product_id::text as id, p.display_code, p.name, p.parent_product_id::text as parent,
       (select count(*)::int from core.skus k where k.product_id = p.product_id) as n_sku
     from core.products p where p.company_id = 1 and core.norm_code(p.display_code) = $1 order by p.product_id`, [norm])).rows;
  const skuSame = (await db.query('select code from core.skus where company_id = 1 and code_norm = $1', [norm])).rows;
  if (skuSame.length) problems.push(`まとまりのコード ${group} が SKU のコード (${skuSame.map((r) => r.code).join(', ')}) = 単品が代表のまとまり (この道の外)`);
  if (tags.length !== 1) {
    problems.push(`表示のコード ${group} の商品 (札) が ${tags.length} 件 (ちょうど 1 件の札だけ直す。0 件 = 夜間ロードが札を作っていない = 子に親が無い)`);
    return { plan: null, problems, hash: null };
  }
  const P = tags[0];
  if (P.n_sku !== 0) problems.push(`商品 ${P.id} (${P.display_code}) に SKU がある = 札でない`);
  if (P.parent !== null) problems.push(`札 ${P.display_code} に親がある (親か子のどちらか一方)`);
  // NE の元のコード (0041 = 最新の照合の回)。まとまり = rep・子 = product
  const neRep = (await db.query(`select ne_code, state from ops.master_ne_codes where code_norm = $1 and kind = 'rep'`, [norm])).rows[0] || null;
  if (!neRep || neRep.state !== 'ok') problems.push(`NE の代表商品コード ${group} が NE のコードの台帳 (0041) に無い / ok でない (${neRep ? neRep.state : 'なし'})`);
  else if (neRep.ne_code !== P.display_code) problems.push(`NE の代表の書き方 ${neRep.ne_code} と札の表示のコード ${P.display_code} が違う`);
  // 子 = 親がこの札の商品 (全部)
  const kids = (await db.query(`select c.product_id::text as product_id, c.parent_set_by, s.sku_id::text as sku_id, s.code, s.code_norm, s.sku_kind, s.name,
       r.state as reg, (select count(*)::int from core.skus k2 where k2.product_id = c.product_id) as n_sku,
       exists (select 1 from core.products g where g.parent_product_id = c.product_id) as has_kids,
       n.ne_code, n.state as ne_state
     from core.products c left join core.skus s on s.product_id = c.product_id
     left join ops.master_registrations r on r.sku_id = s.sku_id
     left join ops.master_ne_codes n on n.code_norm = s.code_norm and n.kind = 'product'
     where c.company_id = 1 and c.parent_product_id = $1 order by s.code_norm`, [P.id])).rows;
  if (!kids.length) problems.push(`札 ${P.display_code} に子が無い (夜間ロードが親を付けていない)`);
  const children = [];
  for (const k of kids) {
    if (k.sku_id == null || k.n_sku !== 1 || k.sku_kind !== 'single') { problems.push(`子の商品 ${k.product_id} が単品の SKU 1 つでない (${k.code ?? 'SKU なし'})`); continue; }
    if (k.reg === 'cancelled') problems.push(`子 ${k.code} は登録をやめた (cancelled)`);
    if (k.has_kids) problems.push(`子 ${k.code} が子を持つ (2 段)`);
    if (k.ne_state !== 'ok' || !k.ne_code) { problems.push(`子 ${k.code} の NE のコード (0041) が ok でない (${k.ne_state ?? 'なし'})`); continue; }
    const tail = k.ne_code.slice(P.display_code.length);
    if (k.ne_code.slice(0, P.display_code.length).toLowerCase() !== norm || !OPTION_CODE_RE.test(tail)) {
      problems.push(`子 ${k.ne_code} が「まとまりのコード + 選択肢番号 (-英数字 1〜10)」でない`);
      continue;
    }
    const name = Object.hasOwn(names, tail) ? names[tail] : lastBracket(k.name);
    if (!name || labelProblem(name)) { problems.push(`子 ${k.ne_code} の選択肢名を商品名の最後の【】から取れない (${k.name}) = --names ${tail}=名前 で渡す`); continue; }
    children.push({ sku_id: k.sku_id, code: k.code, ne_code: k.ne_code, product_id: k.product_id, option: tail, option_name: name, reg: k.reg ?? null, parent_set_by: k.parent_set_by ?? null });
  }
  for (const code of Object.keys(names)) if (!children.some((c) => c.option === code)) problems.push(`--names の ${code} に当たる子が無い`);
  const oc = new Map(); const on = new Map();
  for (const c of children) {
    const a = c.option.toLowerCase(); const b = labelKey(c.option_name);
    if (oc.has(a)) problems.push(`選択肢番号 ${c.option} が 2 つの子 (${oc.get(a)}・${c.code}) で同じ`); else oc.set(a, c.code);
    if (on.has(b)) problems.push(`選択肢名 ${c.option_name} が 2 つの子 (${on.get(b)}・${c.code}) で同じ = --names で分ける`); else on.set(b, c.code);
  }
  // まとまりの表 (0067): 予約・軸・選択肢・子の選択肢・revision・知らせ = まだ無いこと (予約だけは「この札を指す」なら可 = 0067 の事前の予約)
  const res = (await db.query(`select code_norm, code, group_product_id::text as gid, source from ops.variation_group_codes where company_id = 1 and (code_norm = $1 or group_product_id = $2)`, [norm, P.id])).rows;
  if (res.some((r) => r.gid !== P.id || r.code_norm !== norm)) problems.push(`予約 (ops.variation_group_codes) が別のまとまり・コードを指す (${res.map((r) => `${r.code}→${r.gid}`).join(', ')})`);
  const has = (await db.query(`select (select count(*)::int from core.variation_axes where group_product_id = $1) as axes,
       (select count(*)::int from core.variation_options where group_product_id = $1) as options,
       (select count(*)::int from core.sku_variation_choices where group_product_id = $1 or sku_id = any ($2::bigint[])) as choices,
       (select revision from ops.variation_group_revisions where group_product_id = $1) as revision,
       (select count(*)::int from ops.product_hub_outbox where group_product_id = $1) as events`, [P.id, children.map((c) => c.sku_id)])).rows[0];
  if (has.axes || has.options || has.choices) problems.push(`このまとまりにはもう軸 ${has.axes}・選択肢 ${has.options}・子の選択肢 ${has.choices} がある (もう直してある = 何もしない)`);
  if ((has.revision ?? 0) !== 0 || has.events) problems.push(`このまとまりにはもう revision ${has.revision} / カードの知らせ ${has.events} 件がある (もう直してある = 何もしない)`);
  const plan = {
    group: { product_id: P.id, code: P.display_code, name: P.name, reserved: res.length ? res[0].source : null },
    axis: { axis: 1, name: axisName },
    options: children.map((c, i) => ({ code: c.option, name: c.option_name, sort: i })),
    children: children.map((c) => ({ sku_id: c.sku_id, code: c.code, ne_code: c.ne_code, product_id: c.product_id, option: c.option, reg: c.reg, parent_set_by: c.parent_set_by })),
  };
  const hash = crypto.createHash('sha256').update(stable({ v: 1, plan, problems })).digest('hex');
  return { plan, problems, hash };
}

/** 人が読む表 */
export function planLines(r) {
  const out = [];
  if (r.plan) {
    const g = r.plan.group;
    out.push(`まとまり: ${g.code} (商品 ${g.product_id}・名前「${g.name}」・予約 ${g.reserved ?? 'まだ (この取引で source = load)'}) ← 代表 (親) は今のまま (夜間ロードが NE の代表から付けた親)`);
    out.push(`軸 1 (横): ${r.plan.axis.name}`);
    for (const c of r.plan.children) {
      const o = r.plan.options.find((x) => x.code === c.option);
      out.push(`  子 ${c.code} (NE ${c.ne_code}・SKU ${c.sku_id}・登録 ${c.reg ?? '行なし'}・親の帰属 ${c.parent_set_by ?? 'null'}) → 選択肢 ${c.option}「${o.name}」`);
    }
    out.push('書いた後: revision 1・product-hub のカードの知らせ (group_snapshot) 1 件 → ボードを開くとまとまりで 1 枚のカード');
  }
  for (const p of r.problems) out.push(`⛔ ${p}`);
  return out;
}

/**
 * 書く (1 つの取引)。db = DB の持ち主の接続。鍵 → 読み直す → ハッシュ → 書く → 確かめる → commit。違う / 断られた = 巻き戻す
 * 戻り値 { ok, plan, hash, revision?, event_id?, error? }
 */
export async function applyOnce(db, opts, { log = console.log } = {}) {
  const requestId = opts.requestId || crypto.randomUUID();
  const norm = opts.group.toLowerCase();
  await db.query('begin');
  try {
    // 鍵の順 = まとまりの関数と同じ: 段階 (共有) → マスタの書き込み (共有) → 親子 → まとまりのコード → まとまり
    await db.query(`select pg_advisory_xact_lock_shared(hashtext('ops.master_cutover'))`);
    await db.query('select pg_advisory_xact_lock_shared(core.master_write_lock_key())');
    await db.query('select pg_advisory_xact_lock(core.parent_lock_key())');
    await db.query('select ops.variation_lock_code($1)', [norm]);
    const first = await readOncePlan(db, opts);
    if (!first.plan) throw Object.assign(new Error(first.problems.join(' / ')), { reason: 'not_fixable' });
    await db.query('select ops.variation_lock_group($1::bigint)', [first.plan.group.product_id]);
    const r = await readOncePlan(db, opts);   // 鍵の後に読み直す
    if (r.problems.length) throw Object.assign(new Error(r.problems.join(' / ')), { reason: 'not_fixable' });
    if (r.hash !== opts.expectHash) throw Object.assign(new Error(`計画のハッシュが dry-run と違う (今 ${r.hash}・渡された ${opts.expectHash}) = 対象が変わった。dry-run をもう一度見てから`), { reason: 'plan_mismatch' });
    const gid = r.plan.group.product_id;
    // 変更の記録 (0026 の trigger) の誰が・理由
    for (const [k, v] of [['core.actor_type', 'human'], ['core.actor_id', opts.actor], ['core.source_system', SOURCE_SYSTEM], ['core.request_id', requestId], ['core.reason', opts.reason]]) {
      await db.query('select set_config($1, $2, true)', [k, v]);
    }
    await db.query(`select ops._variation_group_resolve($1::bigint, $2, 'load', false)`, [gid, opts.actor]);   // 予約 (無ければ source = load)・revision の行
    await db.query(`select set_config('ops.variation_protocol', '1', true)`);
    await db.query(`insert into core.variation_axes (company_id, group_product_id, axis, name, created_by_type, created_by_id) values (1, $1, 1, $2, 'human', $3)`,
      [gid, r.plan.axis.name, opts.actor]);
    for (const o of r.plan.options) {
      await db.query(`insert into core.variation_options (company_id, group_product_id, axis, code, name, sort, created_by_type, created_by_id) values (1, $1, 1, $2, $3, $4, 'human', $5)`,
        [gid, o.code, o.name, o.sort, opts.actor]);
    }
    for (const c of r.plan.children) {
      await db.query(`insert into core.sku_variation_choices (sku_id, company_id, group_product_id, option1_id, created_by_type, created_by_id)
         select $1, 1, $2, o.option_id, 'human', $4 from core.variation_options o where o.group_product_id = $2 and o.axis = 1 and o.code = $3`,
      [c.sku_id, gid, c.option, opts.actor]);
    }
    const bump = (await db.query('select ops._variation_bump($1::bigint, $2::uuid, $3) as r', [gid, requestId, opts.actor])).rows[0].r;   // revision 1・スナップショット・知らせ
    await db.query(`select set_config('ops.variation_protocol', '', true)`);
    // 書いた後を確かめる (子の数・選択肢・知らせ)
    const after = (await db.query(`select (select revision from ops.variation_group_revisions where group_product_id = $1) as revision,
         (select count(*)::int from core.sku_variation_choices where group_product_id = $1) as choices,
         (select payload from ops.product_hub_outbox where event_id = $2::uuid) as payload`, [gid, bump.event_id])).rows[0];
    const snapKids = (after.payload?.children || []).map((c) => [c.code, c.choices?.['1'] ?? null]).sort();
    const wantKids = r.plan.children.map((c) => [c.code, c.option]).sort();
    if (after.revision !== 1 || after.choices !== r.plan.children.length || JSON.stringify(snapKids) !== JSON.stringify(wantKids)) {
      throw Object.assign(new Error(`書いた後が計画と違う (revision ${after.revision}・子の選択肢 ${after.choices}・知らせの子 ${JSON.stringify(snapKids)})`), { reason: 'verify_failed' });
    }
    await db.query('commit');
    log(`✅ commit: まとまり ${r.plan.group.code} (商品 ${gid})・revision 1・知らせ ${bump.event_id}・request_id ${requestId}`);
    return { ok: true, plan: r.plan, hash: r.hash, revision: 1, event_id: bump.event_id, request_id: requestId };
  } catch (e) {
    try { await db.query('rollback'); } catch { /* */ }
    log(`⛔ 巻き戻した = 何も変えていない: ${e.message}`);
    return { ok: false, error: { reason: e.reason || e.code || 'error', message: String(e.message || e) } };
  }
}

/** 本体 (試験は db を渡す) */
export async function runOnce(db, opts, { log = console.log } = {}) {
  if (!opts.apply) {
    await db.query('begin read only');
    try {
      const r = await readOncePlan(db, opts);
      log(`dry-run (読むだけ・何も書かない): まとまり ${opts.group}`);
      for (const l of planLines(r)) log(l);
      if (r.problems.length) { log('⛔ 直さない (上の理由)'); return { mode: 'dry-run', ...r, exitCode: 1 }; }
      log(`計画のハッシュ (--apply に --expect-hash で渡す): ${r.hash}`);
      return { mode: 'dry-run', ...r, exitCode: 0 };
    } finally { try { await db.query('rollback'); } catch { /* */ } }
  }
  const r = await applyOnce(db, opts, { log });
  return { mode: 'apply', ...r, exitCode: r.ok ? 0 : 1 };
}

async function main() {
  let opts;
  try { opts = parseOnceArgs(process.argv.slice(2)); } catch (e) { console.error(`❌ ${e.message}`); process.exitCode = 2; return; }
  const url = opts.url || (opts.apply ? process.env.COMPANY_DB_URL : process.env.COMPANY_DB_WATCH_URL);
  if (!url) { console.error(`❌ ${opts.apply ? 'COMPANY_DB_URL (DB の持ち主)' : 'COMPANY_DB_WATCH_URL (watcher = 読むだけ)'} か --url が要る`); process.exitCode = 2; return; }
  let client = null;
  try {
    client = await openPgClient(url, { application_name: 'once-variation-group-from-ne' });
    await client.query(`set statement_timeout = '20s'`);
    await client.query(`set lock_timeout = '10s'`);
    const r = await runOnce(pgAdapter(client), opts);
    if (opts.json) fs.writeFileSync(opts.json, JSON.stringify({ mode: r.mode, hash: r.hash ?? null, plan: r.plan ?? null, problems: r.problems ?? [], event_id: r.event_id ?? null, error: r.error ?? null }, null, 2));
    process.exitCode = r.exitCode;
  } catch (e) {
    console.error(`❌ ${String(e && e.message).replace(/postgres(ql)?:\/\/\S+/gi, '[接続文字列]')}`);
    process.exitCode = 1;
  } finally {
    if (client) { try { await client.end(); } catch { /* */ } }
  }
}

const isMain = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) main();
