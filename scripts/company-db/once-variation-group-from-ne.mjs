#!/usr/bin/env node
/**
 * once-variation-group-from-ne.mjs — 一回きり: NE で直接作った色違いのまとまり (fuwear = fuwear-bk / -nd / -wh ほか許可の一覧の 3 つだけ) を、
 * Company DB の「色違いのまとまり」の記録 (予約・軸・選択肢・子の選択肢・revision・product-hub のカードの知らせ) にする。
 * (中原さん 10/12「fuwear-bk/-nd/-wh は自社 DB でバリエーション登録できなかった日の新商品で、仕方なく NE に登録した。データ直して・ボードにカードも」)
 *
 * 🚨 一般の機能ではない (台帳 config/jobs-registry.mjs の temporary_asset 'once-variation-group-from-ne' = 期限の後に消す)。
 * 🚨 **代表 (products.parent) の持ち主が company になった後 (切替 = widen の後) だけ書く** (#1693 Codex R1 High 2)。load の間は「切替の後に流す」で止まる
 *    (0067 のまとまりの関数と同じ門 ops._variation_gate = 段階 new_open・持ち主表が切替の記録と同じ・products.parent / products.name が company)。
 *    代表 (親) は変えない: 夜間ロードが NE の代表商品コードから付けた親 (札・帰属 load) をそのまま使う。
 * 確かめ (読むだけ・dry-run と apply の両方・#1693 Codex R1 High 1 / Medium 1 / Medium 2):
 *   - 対象 = 許可の一覧 (ALLOWED_GROUPS) のまとまりだけ・子は NE で直接作られた商品 (quarantined)・親の帰属 load・子の数は 0067 の上限 (120) まで
 *   - NE の代表 = miniPC の warehouse.db の NE の完全な取得から、照合 ② / drift-list と同じ読み方 (neParentObservations) で作った観測。
 *     **最新の封をした照合 ② の回 (ops.master_parent_gate_results) の観測と同じもの (obs_hash が一致)・36 時間以内** のときだけ使う
 *     (Company DB の親から観測を作らない)。子それぞれの NE の代表 = --group・ops.parent_raw_gate (親の数え) で子にずれが無い
 *   - まとまりの表 (予約・軸・選択肢・子の選択肢・revision・知らせ) がまだ無い (予約だけは「この札を指す」なら可)
 *   - 計画のハッシュ = 人・理由・段階・持ち主・封をした照合の回・NE の観測・作るもの・**知らせ (スナップショット) の中身の全部** (子の名前・売価・JAN・共通の欄)
 * 書く (apply・1 つの取引): 鍵 (段階 → マスタの書き込み → 門 → SKU → 親子 → NE のコード (共有) → まとまりのコード → まとまり) → 読み直す → ハッシュ → 約束
 *   (ops._open_variation_write の variation_label_edit = 保存の記録 ops.master_edit_requests に request_id) → 予約・revision の行 (ops._variation_group_resolve) →
 *   軸 1・選択肢・子の選択肢 → revision 1 とスナップショット (ops._variation_bump) → 知らせの中身を計画と照らす → 保存の記録 (ops.close_reg_write) → commit。
 *   commit の答えが分からない (接続が切れた) = 新しい接続で request_id の保存の記録を読み「書けた / 書けていない / 分からない」を言う (#1693 Codex R1 Medium 3)
 * その後: product-hub のボードを開くと今の取り込み (cdb-group-intake.js) がまとまりで 1 枚のカードにする (done = created / linked・conflict = 人が片付ける = 台帳の runbook)
 *
 * 使い方 (miniPC の PowerShell・リポジトリ直下。NE の取得 = env DATA_DIR / WAREHOUSE_DATA_DIR か --data-dir):
 *   # 1. 読むだけ (既定)。env COMPANY_DB_WATCH_URL (watcher) か --url。今の姿・作るもの・計画のハッシュ。直さない理由があれば exit 1 (表は出す)
 *   node -r dotenv/config scripts/company-db/once-variation-group-from-ne.mjs --group fuwear --actor <人のメール> [--axis-name カラー] [--names "-BK=ブラック,-ND=ヌード"] [--json out.json]
 *   # 2. 書く (🚨 切替の後・dry-run の表を中原さんが見て OK の後だけ)。env COMPANY_DB_URL (DB の持ち主) か --url
 *   node -r dotenv/config scripts/company-db/once-variation-group-from-ne.mjs --group fuwear --actor <人のメール> --apply --expect-hash <64 桁> --yes
 * 終了コード: 0 = dry-run で直せる / apply で commit・1 = 直さない / 巻き戻した / 書けたか分からない・つながらない・2 = 引数が違う
 */
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { openPgClient, pgAdapter } from './migrate.mjs';

/** 直してよいまとまり (中原さん 10/12 = NE で直接作った 3 つだけ)。ほかは断る */
export const ALLOWED_GROUPS = Object.freeze(['fuwear', 'sanrio3soupk', 'smhc']);
/** 0067 の ops.variation_max_children() と同じ (apply は DB の関数とも照らす) */
export const MAX_CHILDREN = 120;
/** 封をした照合の回の新しさ (0070 の ops.ne_gone_fresh_hours と同じ 36 時間) */
export const FRESH_HOURS = 36;
const PLAN_VERSION = 2;
const fail = (message) => Object.assign(new Error(message), { code: 'ONCE_ARGS' });
const OPTION_CODE_RE = /^-[A-Za-z0-9]{1,10}$/;
const CTRL_RE = new RegExp(`[\\x00-\\x1f\\x7f${String.fromCharCode(0x2028, 0x2029)}]`);
const DEFAULT_REASON = 'NE で直接作った色違いのまとまりを Company DB のまとまりの記録にする (一回きり・10/12 中原さん)';
const SOURCE_SYSTEM = 'once_variation_group';
const DEFAULT_COMMON = Object.freeze({ shipping: null, amazon_url: null, asin: null, official_url: null, reference_urls: [], yahoo: null });
/** 選択肢名の一意の鍵 (DB の name_key = btrim(normalize(name, NFKC)) と同じ) */
const labelKey = (s) => String(s ?? '').normalize('NFKC').replace(/^ +| +$/g, '');
/** 名前の形 (DB の ops.variation_label_problem と同じ: 前後の空白なし・制御文字なし・1〜100 字) */
const labelProblem = (s) => (typeof s !== 'string' || !s.length || [...s].length > 100 || s !== s.trim() || CTRL_RE.test(s) || !labelKey(s) ? 'label' : null);
/** 商品名の最後の【】の中 (NE の「共通の商品名【選択肢名】」の形) */
export function lastBracket(name) {
  const m = String(name ?? '').match(/【([^【】]+)】\s*$/);
  return m ? m[1].trim() : null;
}
export const stable = (v) => (Array.isArray(v) ? `[${v.map(stable).join(',')}]`
  : v && typeof v === 'object' ? `{${Object.keys(v).sort().map((k) => `${JSON.stringify(k)}:${stable(v[k])}`).join(',')}}` : JSON.stringify(v));
const sha = (s) => crypto.createHash('sha256').update(s).digest('hex');

/** 引数を読む。誤り = ONCE_ARGS */
export function parseOnceArgs(argv) {
  const o = { group: null, axisName: 'カラー', names: {}, apply: false, yes: false, expectHash: null, actor: null, reason: DEFAULT_REASON, json: null, url: null, dataDir: null };
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
    else if (a === '--data-dir') { o.dataDir = need(i, a); i++; }
    else throw fail(`知らない引数: ${a}`);
  }
  if (!o.group || !ALLOWED_GROUPS.includes(o.group)) throw fail(`--group は許可の一覧 (${ALLOWED_GROUPS.join(' / ')}) のどれか (打ったとおり)`);
  if (labelProblem(o.axisName)) throw fail('--axis-name は 1〜100 字 (前後の空白・制御文字なし)');
  if (!o.reason || o.reason.length > 200 || CTRL_RE.test(o.reason)) throw fail('--reason は 1〜200 字 (制御文字なし)');
  // 人は計画 (知らせの created_by・保存の記録) に入る = dry-run も同じ人で
  if (!o.actor || !/^[^\s@]+@[^\s@]+$/.test(o.actor) || o.actor.length > 320) throw fail('--actor <人のメール> が要る (dry-run も apply も同じ人)');
  if (o.apply) {
    if (o.expectHash == null) throw fail('--apply は --expect-hash <dry-run が出した計画のハッシュ> が要る');
    if (!o.yes) throw fail('--apply は --yes が要る (切替の後・dry-run の表を中原さんが見て OK の後だけ)');
  }
  return o;
}

/**
 * 今の Company DB と NE の観測を読んで、作るもの (計画) と「直さない」理由を決める (読むだけ)。
 * ne = { obs (parent-obs-v1・neParentObservations の答え) }・now = 今 (新しさの確かめ)。
 * 戻り値 { plan, expected (知らせのスナップショット), context, problems: [文], hash }。plan が null = 札が決まらない (表も出せない)
 */
export async function readOncePlan(db, opts, { ne, now = new Date() } = {}) {
  const { group, axisName = 'カラー', names = {}, actor, reason = DEFAULT_REASON } = opts;
  const problems = [];
  if (!ALLOWED_GROUPS.includes(group)) return { plan: null, expected: null, context: null, problems: [`まとまり ${group} は許可の一覧 (${ALLOWED_GROUPS.join(' / ')}) に無い`], hash: null };
  const norm = group.toLowerCase();
  // 段階・持ち主 (DB の active)。products.parent が company でない = 切替の後に流す (#1693 Codex R1 High 2)
  const cut = (await db.query('select phase, owner_hash from ops.master_cutover_state where id = 1')).rows[0] || {};
  const own = (await db.query('select active_hash, active_map from ops.master_ownership_state where id = 1')).rows[0] || {};
  const ownMap = own.active_map || {};
  const context = { phase: cut.phase ?? null, owner_hash: cut.owner_hash ?? null, active_hash: own.active_hash ?? null,
    parent_owner: ownMap['products.parent'] ?? 'load', name_owner: ownMap['products.name'] ?? 'load' };
  if (context.parent_owner !== 'company') problems.push('parent_not_company: 代表 (products.parent) の持ち主がまだ load (夜間ロード) = 切替 (widen) の後に流す (今は何も書かない)');
  if (context.name_owner !== 'company') problems.push('products.name の持ち主が company でない');
  if (context.phase !== 'new_open') problems.push(`切替の段階が ${context.phase} (new_open でない)`);
  if (context.owner_hash !== context.active_hash) problems.push('持ち主表が切替の記録と違う');

  const tags = (await db.query(`select p.product_id::text as id, p.display_code, p.name, p.parent_product_id::text as parent,
       (select count(*)::int from core.skus k where k.product_id = p.product_id) as n_sku
     from core.products p where p.company_id = 1 and core.norm_code(p.display_code) = $1 order by p.product_id`, [norm])).rows;
  const skuSame = (await db.query('select code from core.skus where company_id = 1 and code_norm = $1', [norm])).rows;
  if (skuSame.length) problems.push(`まとまりのコード ${group} が SKU のコード (${skuSame.map((r) => r.code).join(', ')}) = 単品が代表のまとまり (この道の外)`);
  if (tags.length !== 1) {
    problems.push(`表示のコード ${group} の商品 (札) が ${tags.length} 件 (ちょうど 1 件の札だけ直す。0 件 = 夜間ロードが札を作っていない = 子に親が無い)`);
    return { plan: null, expected: null, context, problems, hash: null };
  }
  const P = tags[0];
  if (P.n_sku !== 0) problems.push(`商品 ${P.id} (${P.display_code}) に SKU がある = 札でない`);
  if (P.parent !== null) problems.push(`札 ${P.display_code} に親がある (親か子のどちらか一方)`);
  // NE の元のコード (0041 = 最新の照合の回)。まとまり = rep・子 = product
  const neRep = (await db.query(`select ne_code, state from ops.master_ne_codes where code_norm = $1 and kind = 'rep'`, [norm])).rows[0] || null;
  if (!neRep || neRep.state !== 'ok') problems.push(`NE の代表商品コード ${group} が NE のコードの台帳 (0041) に無い / ok でない (${neRep ? neRep.state : 'なし'})`);
  else if (neRep.ne_code !== P.display_code) problems.push(`NE の代表の書き方 ${neRep.ne_code} と札の表示のコード ${P.display_code} が違う`);
  // 子 = 親がこの札の商品 (全部・sku_id の順 = スナップショットと同じ順)
  const kids = (await db.query(`select c.product_id::text as product_id, c.parent_set_by, s.sku_id::text as sku_id, s.code, s.code_norm, s.sku_kind, s.name,
       pg_catalog.to_jsonb(s.standard_price_jpy) as price, r.state as reg, (select count(*)::int from core.skus k2 where k2.product_id = c.product_id) as n_sku,
       exists (select 1 from core.products g where g.parent_product_id = c.product_id) as has_kids,
       coalesce((select pg_catalog.jsonb_agg(e.external_value order by e.external_value) from core.external_ids e
                  where e.entity_type = 'product' and e.entity_id = s.product_id and e.system = 'jan' and e.id_kind = 'jan' and e.valid_to is null), '[]'::jsonb) as jans,
       n.ne_code, n.state as ne_state
     from core.products c left join core.skus s on s.product_id = c.product_id
     left join ops.master_registrations r on r.sku_id = s.sku_id
     left join ops.master_ne_codes n on n.code_norm = s.code_norm and n.kind = 'product'
     where c.company_id = 1 and c.parent_product_id = $1 order by s.sku_id`, [P.id])).rows;
  if (!kids.length) problems.push(`札 ${P.display_code} に子が無い (夜間ロードが親を付けていない)`);
  if (kids.length > MAX_CHILDREN) problems.push(`子が ${kids.length} 件 (0067 の上限 ${MAX_CHILDREN} を超える)`);
  const children = [];
  for (const k of kids) {
    if (k.sku_id == null || k.n_sku !== 1 || k.sku_kind !== 'single') { problems.push(`子の商品 ${k.product_id} が単品の SKU 1 つでない (${k.code ?? 'SKU なし'})`); continue; }
    // NE で直接作られた商品 (quarantined)・親は夜間ロードが NE の代表から付けた (帰属 load) だけ (#1693 Codex R1 Medium 2)
    if (k.reg !== 'quarantined') problems.push(`子 ${k.code} の登録の状態が ${k.reg ?? '行なし'} (NE で直接作られた商品 = quarantined だけ直す)`);
    if (k.parent_set_by !== 'load') problems.push(`子 ${k.code} の親の帰属が ${k.parent_set_by ?? 'null'} (夜間ロードが NE の代表から付けた親 = load だけ)`);
    if (k.has_kids) problems.push(`子 ${k.code} が子を持つ (2 段)`);
    if (k.ne_state !== 'ok' || !k.ne_code) { problems.push(`子 ${k.code} の NE のコード (0041) が ok でない (${k.ne_state ?? 'なし'})`); continue; }
    const tail = k.ne_code.slice(P.display_code.length);
    if (k.ne_code.slice(0, P.display_code.length).toLowerCase() !== norm || !OPTION_CODE_RE.test(tail)) {
      problems.push(`子 ${k.ne_code} が「まとまりのコード + 選択肢番号 (-英数字 1〜10)」でない`);
      continue;
    }
    const name = Object.hasOwn(names, tail) ? names[tail] : lastBracket(k.name);
    if (!name || labelProblem(name)) { problems.push(`子 ${k.ne_code} の選択肢名を商品名の最後の【】から取れない (${k.name}) = --names ${tail}=名前 で渡す`); continue; }
    children.push({ sku_id: k.sku_id, code: k.code, code_norm: k.code_norm, ne_code: k.ne_code, product_id: k.product_id, option: tail, option_name: name,
      reg: k.reg ?? null, parent_set_by: k.parent_set_by ?? null, name: k.name, price: k.price ?? null, jans: k.jans || [] });
  }
  for (const code of Object.keys(names)) if (!children.some((c) => c.option === code)) problems.push(`--names の ${code} に当たる子が無い`);
  const oc = new Map(); const on = new Map();
  for (const c of children) {
    const a = c.option.toLowerCase(); const b = labelKey(c.option_name);
    if (oc.has(a)) problems.push(`選択肢番号 ${c.option} が 2 つの子 (${oc.get(a)}・${c.code}) で同じ`); else oc.set(a, c.code);
    if (on.has(b)) problems.push(`選択肢名 ${c.option_name} が 2 つの子 (${on.get(b)}・${c.code}) で同じ = --names で分ける`); else on.set(b, c.code);
  }

  // NE の代表 (#1693 Codex R1 High 1): 最新の封をした照合 ② の回の観測と同じ NE の観測で、子それぞれの代表が --group
  const neInfo = { compare_run_id: null, result_id: null, evidence_sha256: null, obs_hash: null, products_complete_at: null, children: [] };
  const obs = ne && ne.obs;
  if (!obs || obs.format !== 'parent-obs-v1') problems.push('NE の観測 (warehouse.db の NE の完全な取得) を読めない');
  else {
    const { trustProblems } = await import('../../apps/company-db/master-compare/parent-gate.mjs');
    const tp = trustProblems(obs);
    if (tp.length) problems.push(`NE の取得を確かめられない (${tp.join(', ')})`);
    const seal = (await db.query(`select result_id::text as result_id, compare_run_id, evidence_sha256, obs_hash, products_complete_at, created_at
       from ops.master_parent_gate_results order by result_id desc limit 1`)).rows[0] || null;
    const myHash = (await db.query(`select pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to($1::jsonb::text, 'UTF8')), 'hex') as h`, [JSON.stringify(obs)])).rows[0].h;
    neInfo.obs_hash = myHash;
    if (!seal) problems.push('封をした照合 ② の回 (代表の数えの記録) が無い = 翌朝の照合の後に');
    else {
      Object.assign(neInfo, { compare_run_id: seal.compare_run_id, result_id: seal.result_id, evidence_sha256: seal.evidence_sha256,
        products_complete_at: new Date(seal.products_complete_at).toISOString() });
      if (seal.obs_hash !== myHash) problems.push(`NE の観測が最新の封をした照合の回 (${seal.compare_run_id}) の観測と違う (NE の取得が新しくなった / 古い) = 照合 ② の後に流し直す`);
      if (now.getTime() - new Date(seal.created_at).getTime() > FRESH_HOURS * 3600e3) problems.push(`最新の封をした照合の回 (${seal.compare_run_id}) が ${FRESH_HOURS} 時間より古い`);
    }
    const rowOf = new Map((obs.rows || []).map((r) => [r[0], r]));
    for (const c of children) {
      const r = rowOf.get(c.code_norm);
      neInfo.children.push(r ? [...r] : [c.code_norm, null]);
      if (!r) problems.push(`子 ${c.code} が NE の取得に無い`);
      else if (r[1] !== 'single' || r[2] !== 'ok') problems.push(`子 ${c.code} の NE の代表が読めない (${r[1]}・${r[2]})`);
      else if (r[3] !== norm || r[4] !== P.display_code) problems.push(`子 ${c.code} の NE の代表が ${r[4] ?? '(なし)'} (--group ${group} と違う)`);
    }
    if ((obs.untrusted || []).some((u) => children.some((c) => c.code_norm === u))) problems.push('子が NE の取得で保持 (正規化の衝突・取込の整合) されている');
    // 照合 ② と同じ親の数え (ops.parent_raw_gate) で、子にずれ (6 つの数えのどれか) が無い
    if (!tp.length) {
      const g = (await db.query('select ops.parent_raw_gate(1, $1::jsonb, true) as r', [JSON.stringify(obs)])).rows[0].r;
      for (const it of (g.items || []).filter((x) => children.some((c) => c.code === x.code))) problems.push(`子 ${it.code} の代表のずれ: ${it.class}${it.reason ? ` (${it.reason})` : ''}`);
    }
  }

  // まとまりの表 (0067): 予約・軸・選択肢・子の選択肢・revision・知らせ = まだ無いこと (予約だけは「この札を指す」なら可 = 0067 の事前の予約)
  const res = (await db.query(`select code_norm, code, group_product_id::text as gid, source from ops.variation_group_codes where company_id = 1 and (code_norm = $1 or group_product_id = $2)`, [norm, P.id])).rows;
  if (res.some((r) => r.gid !== P.id || r.code_norm !== norm)) problems.push(`予約 (ops.variation_group_codes) が別のまとまり・コードを指す (${res.map((r) => `${r.code}→${r.gid}`).join(', ')})`);
  const has = (await db.query(`select (select count(*)::int from core.variation_axes where group_product_id = $1) as axes,
       (select count(*)::int from core.variation_options where group_product_id = $1) as options,
       (select count(*)::int from core.sku_variation_choices where group_product_id = $1 or sku_id = any ($2::bigint[])) as choices,
       (select revision from ops.variation_group_revisions where group_product_id = $1) as revision,
       (select common from ops.variation_group_revisions where group_product_id = $1) as common,
       (select count(*)::int from ops.product_hub_outbox where group_product_id = $1) as events`, [P.id, children.map((c) => c.sku_id)])).rows[0];
  if (has.axes || has.options || has.choices) problems.push(`このまとまりにはもう軸 ${has.axes}・選択肢 ${has.options}・子の選択肢 ${has.choices} がある (もう直してある = 何もしない)`);
  if ((has.revision ?? 0) !== 0 || has.events) problems.push(`このまとまりにはもう revision ${has.revision} / カードの知らせ ${has.events} 件がある (もう直してある = 何もしない)`);

  const plan = {
    group: { product_id: P.id, code: P.display_code, name: P.name, reserved: res.length ? res[0].source : null },
    axis: { axis: 1, name: axisName },
    options: children.map((c, i) => ({ code: c.option, name: c.option_name, sort: i })),
    children: children.map((c) => ({ sku_id: c.sku_id, code: c.code, ne_code: c.ne_code, product_id: c.product_id, option: c.option, reg: c.reg, parent_set_by: c.parent_set_by })),
  };
  // 知らせ (ph-group-v1) の中身の全部 = ops.variation_group_snapshot(まとまり, 1, 人) と同じ形 (書いた後に DB の知らせと照らす)
  const expected = {
    schema: 'ph-group-v1', revision: 1, created_by: actor,
    group: { product_id: P.id, sku_id: null, code: P.display_code, name: P.name, kind: 'tag' },
    axes: [{ axis: 1, name: axisName }],
    options: plan.options.map((o) => ({ axis: 1, code: o.code, name: o.name, sort: o.sort })),
    children: children.map((c) => ({ sku_id: c.sku_id, code: c.code, name: c.name, price: c.price, choices: { 1: c.option }, jans: c.jans })),
    cancelled_children: [],
    common: has.common || { ...DEFAULT_COMMON },
  };
  const hash = sha(stable({ v: PLAN_VERSION, actor, reason, context, ne: neInfo, plan, expected, problems }));
  return { plan, expected, context, ne: neInfo, problems, hash };
}

/** 人が読む表 */
export function planLines(r) {
  const out = [];
  if (r.context) out.push(`段階 ${r.context.phase}・代表 (products.parent) の持ち主 ${r.context.parent_owner}`);
  if (r.ne && r.ne.compare_run_id) out.push(`NE の代表の根拠: 封をした照合 ② の回 ${r.ne.compare_run_id} (NE の取得 ${r.ne.products_complete_at})`);
  if (r.plan) {
    const g = r.plan.group;
    out.push(`まとまり: ${g.code} (商品 ${g.product_id}・名前「${g.name}」・予約 ${g.reserved ?? 'まだ (書くときに source = load)'}) ← 代表 (親) は今のまま (夜間ロードが NE の代表から付けた親)`);
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

/** commit の答えが分からないとき: 新しい接続で request_id の保存の記録を読む。'written' / 'not_written' / 'unknown' */
export async function outcomeOf(reconnect, requestId) {
  if (!reconnect) return { state: 'unknown', why: '読み直す接続が無い' };
  let c = null;
  try {
    c = await reconnect();
    const r = (await c.db.query(`select m.status, (select count(*)::int from ops.product_hub_outbox o where o.request_id = $1::uuid) as events
       from (select 1) x left join ops.master_edit_requests m on m.request_id = $1::uuid`, [requestId])).rows[0];
    if (r.status === 'done' && r.events === 1) return { state: 'written' };
    if (r.status == null && r.events === 0) return { state: 'not_written' };
    return { state: 'unknown', why: `保存の記録 ${r.status ?? 'なし'}・知らせ ${r.events} 件` };
  } catch (e) {
    return { state: 'unknown', why: String((e && e.message) || e).replace(/postgres(ql)?:\/\/\S+/gi, '[接続文字列]').slice(0, 200) };
  } finally { if (c && c.close) { try { await c.close(); } catch { /* */ } } }
}

/**
 * 書く (1 つの取引)。db = DB の持ち主の接続。戻り値 { ok, outcome: committed / rolled_back / written / not_written / unknown, plan, hash, revision?, event_id?, request_id, error? }
 */
export async function applyOnce(db, opts, { ne, now = new Date(), reconnect = null, log = console.log } = {}) {
  const requestId = opts.requestId || crypto.randomUUID();
  const norm = opts.group.toLowerCase();
  let r;
  let bump;
  try {
    await db.query('begin');
    // 鍵の順 = まとまりの関数 (0067) と同じ: 段階 (共有) → マスタの書き込み (共有) → 門 → SKU → 親子 → NE のコード (共有) → まとまりのコード → まとまり
    await db.query(`select pg_advisory_xact_lock_shared(hashtext('ops.master_cutover'))`);
    await db.query('select pg_advisory_xact_lock_shared(core.master_write_lock_key())');
    const first = await readOncePlan(db, opts, { ne, now });
    if (!first.plan) throw Object.assign(new Error(first.problems.join(' / ')), { reason: 'not_fixable' });
    const ownership = (await db.query('select active_map from ops.master_ownership_state where id = 1')).rows[0]?.active_map;
    // 0067 のまとまりの関数と同じ門 (段階 new_open・持ち主表が切替の記録と同じ・products.parent / products.name が company。でなければ parent_not_company / before_cutover)
    await db.query(`select ops._variation_gate($1::jsonb, array['products.parent', 'products.name'])`, [JSON.stringify(ownership ?? null)]);
    const ids = first.plan.children.map((c) => BigInt(c.sku_id)).sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
    for (const id of ids) await db.query(`select pg_advisory_xact_lock(hashtextextended('core.sku:' || $1::text, 0))`, [String(id)]);
    await db.query('select pg_advisory_xact_lock(core.parent_lock_key())');
    await db.query(`select pg_advisory_xact_lock_shared(hashtext('ops.ne_codes'))`);
    await db.query('select ops.variation_lock_code($1)', [norm]);
    await db.query('select ops.variation_lock_group($1::bigint)', [first.plan.group.product_id]);
    r = await readOncePlan(db, opts, { ne, now });   // 鍵の後に読み直す
    if (r.problems.length) throw Object.assign(new Error(r.problems.join(' / ')), { reason: 'not_fixable' });
    if (r.hash !== opts.expectHash) throw Object.assign(new Error(`計画のハッシュが dry-run と違う (今 ${r.hash}・渡された ${opts.expectHash}) = 対象が変わった。dry-run をもう一度見てから`), { reason: 'plan_mismatch' });
    if (r.plan.children.length > Number((await db.query('select ops.variation_max_children() as n')).rows[0].n)) throw Object.assign(new Error('子が 0067 の上限を超える'), { reason: 'not_fixable' });
    const gid = r.plan.group.product_id;
    // 変更の記録 (0026 の trigger) の誰が・理由
    for (const [k, v] of [['core.actor_type', 'human'], ['core.actor_id', opts.actor], ['core.source_system', SOURCE_SYSTEM], ['core.request_id', requestId], ['core.reason', opts.reason]]) {
      await db.query('select set_config($1, $2, true)', [k, v]);
    }
    // 約束 (0067 のまとまりの約束と同じ = 保存の記録に request_id・人・理由・計画のハッシュ)
    await db.query(`select ops._open_variation_write('variation_label_edit', $1::uuid, $2, $3, $4::jsonb, null, '{}'::bigint[], $5, $6::jsonb)`,
      [requestId, opts.actor, opts.reason, JSON.stringify(ownership), r.hash, JSON.stringify({ group_product_id: gid })]);
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
    bump = (await db.query('select ops._variation_bump($1::bigint, $2::uuid, $3) as r', [gid, requestId, opts.actor])).rows[0].r;   // revision 1・スナップショット・知らせ
    // 書いた後を確かめる = 知らせの中身の全部が計画 (expected) と同じ (#1693 Codex R1 Medium 1)
    const ev = (await db.query('select payload, revision, request_id::text as rid, created_by from ops.product_hub_outbox where event_id = $1::uuid', [bump.event_id])).rows[0];
    if (!ev || ev.revision !== 1 || ev.rid !== requestId || ev.created_by !== opts.actor || stable(ev.payload) !== stable(r.expected)) {
      throw Object.assign(new Error(`書いた後の知らせが計画と違う (${ev ? `revision ${ev.revision}` : '知らせなし'})`), { reason: 'verify_failed' });
    }
    const result = { ok: true, request_id: requestId, group_product_id: gid, group_code: r.plan.group.code, revision: 1, event_id: bump.event_id, plan_hash: r.hash };
    await db.query(`select set_config('ops.variation_protocol', '', true)`);
    await db.query('select ops.close_reg_write($1::jsonb, $2)', [JSON.stringify(result), r.plan.group.code]);   // 保存の記録 (request_id)
  } catch (e) {
    try { await db.query('rollback'); } catch { /* */ }
    log(`⛔ 巻き戻した (commit の前) = 何も変えていない: ${e.message}`);
    return { ok: false, outcome: 'rolled_back', request_id: requestId, error: { reason: e.reason || e.code || 'error', message: String(e.message || e) } };
  }
  // commit (#1693 Codex R1 Medium 3: 答えが分からない = 新しい接続で確かめる。「巻き戻した」とは言わない)
  try {
    await db.query('commit');
  } catch (e) {
    const o = await outcomeOf(reconnect, requestId);
    const words = { written: '書けていた (commit は通っていた)', not_written: '書けていない (何も変わっていない)', unknown: `書けたか分からない (${o.why})` };
    log(`⚠️ commit の答えが分からない (${String(e.message || e).slice(0, 200)}) → 新しい接続で request_id ${requestId} を確かめた: ${words[o.state]}`);
    return { ok: o.state === 'written', outcome: o.state, request_id: requestId, plan: r.plan, hash: r.hash, event_id: bump?.event_id ?? null,
      error: o.state === 'written' ? null : { reason: `commit_${o.state}`, message: String(e.message || e) } };
  }
  log(`✅ commit: まとまり ${r.plan.group.code} (商品 ${r.plan.group.product_id})・revision 1・知らせ ${bump.event_id}・request_id ${requestId}`);
  return { ok: true, outcome: 'committed', plan: r.plan, hash: r.hash, revision: 1, event_id: bump.event_id, request_id: requestId };
}

/** 本体 (試験は db と ne を渡す) */
export async function runOnce(db, opts, { ne, now = new Date(), reconnect = null, log = console.log } = {}) {
  if (!opts.apply) {
    await db.query('begin read only');
    try {
      const r = await readOncePlan(db, opts, { ne, now });
      log(`dry-run (読むだけ・何も書かない): まとまり ${opts.group}`);
      for (const l of planLines(r)) log(l);
      if (r.problems.length) { log('⛔ 直さない (上の理由)'); return { mode: 'dry-run', ...r, exitCode: 1 }; }
      log(`計画のハッシュ (--apply に --expect-hash で渡す・同じ --actor で): ${r.hash}`);
      return { mode: 'dry-run', ...r, exitCode: 0 };
    } finally { try { await db.query('rollback'); } catch { /* */ } }
  }
  const r = await applyOnce(db, opts, { ne, now, reconnect, log });
  return { mode: 'apply', ...r, exitCode: r.ok ? 0 : 1 };
}

/** miniPC の warehouse.db の NE の完全な取得 → 代表の観測 (drift-list と同じ部品) */
async function readNe(dataDir) {
  const { readNeSide } = await import('../../apps/company-db/master-compare/compare-ne.mjs');
  const { neParentObservations } = await import('../../apps/company-db/master-compare/drift-list.mjs');
  return { obs: neParentObservations(readNeSide(dataDir)).obs };
}

async function main() {
  let opts;
  try { opts = parseOnceArgs(process.argv.slice(2)); } catch (e) { console.error(`❌ ${e.message}`); process.exitCode = 2; return; }
  const url = opts.url || (opts.apply ? process.env.COMPANY_DB_URL : process.env.COMPANY_DB_WATCH_URL);
  if (!url) { console.error(`❌ ${opts.apply ? 'COMPANY_DB_URL (DB の持ち主)' : 'COMPANY_DB_WATCH_URL (watcher = 読むだけ)'} か --url が要る`); process.exitCode = 2; return; }
  const dataDir = (opts.dataDir || process.env.DATA_DIR || process.env.WAREHOUSE_DATA_DIR || '').trim();
  if (!dataDir) { console.error('❌ DATA_DIR / WAREHOUSE_DATA_DIR (warehouse.db の場所) か --data-dir が要る'); process.exitCode = 2; return; }
  const clean = (e) => String(e && e.message).replace(/postgres(ql)?:\/\/\S+/gi, '[接続文字列]');
  let ne;
  try { ne = await readNe(dataDir); } catch (e) { console.error(`❌ NE の取得を読めない: ${clean(e)}`); process.exitCode = 1; return; }
  const connect = async () => {
    const c = await openPgClient(url, { application_name: 'once-variation-group-from-ne' });
    c.on('error', () => {});
    await c.query(`set statement_timeout = '20s'`);
    await c.query(`set lock_timeout = '10s'`);
    return c;
  };
  let client = null;
  try {
    client = await connect();
    const reconnect = async () => { const c = await connect(); await c.query('set default_transaction_read_only = on'); return { db: pgAdapter(c), close: () => c.end() }; };
    const r = await runOnce(pgAdapter(client), opts, { ne, reconnect });
    if (opts.json) fs.writeFileSync(opts.json, JSON.stringify({ mode: r.mode, hash: r.hash ?? null, plan: r.plan ?? null, expected: r.expected ?? null, problems: r.problems ?? [],
      outcome: r.outcome ?? null, request_id: r.request_id ?? null, event_id: r.event_id ?? null, error: r.error ?? null }, null, 2));
    process.exitCode = r.exitCode;
  } catch (e) {
    console.error(`❌ ${clean(e)}`);
    process.exitCode = 1;
  } finally {
    if (client) { try { await client.end(); } catch { /* */ } }
  }
}

const isMain = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) main();
