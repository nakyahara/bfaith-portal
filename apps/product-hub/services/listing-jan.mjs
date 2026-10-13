/**
 * listing-jan.mjs — 楽天に送る JAN を「商品マスタ (Company DB) から選んだ物」だけにする (Company DB構想 21 §5 B2 の段 3〜5)
 *
 * なぜ: JAN の正本は Company DB (中原さんの決定 3 = a: product-hub の JAN は Company DB の JAN から選ぶだけ)。
 *   段 1・2 (#1707) で product-hub の SQLite に「結んだ印」(jan_cdb_row / jan_cdb_value / jan_link_state) を足し、今ある値を結んだ。
 *   この部品は段 3〜5 の決まりを 1 か所に持つ:
 *   段 3 (選ぶ)   = 単位ごとの今の状態と、Company DB のその商品の有効な JAN (選べる物) を詳細画面に出す (pickViewOf)
 *   段 4 (止める) = 結ばれていない / 要確認 / JAN と「なしの理由」の両方 / どちらも無い 単位は出品を止める (janListingBlock・SQLite だけ)
 *   段 5 (送る直前の 1 か所) = 結んだ JAN を Company DB で確かめ直し (resolveListingJans)、送る値は確かめた Company DB の値。
 *          payload の articleNumber は「有効な JAN 1 つ」か「カタログ ID なしの理由」のどちらか 1 つだけ (checkArticleNumbers)
 * 🚨 止めるのは設定 (環境変数 PH_JAN_ENFORCE=1) が入っているときだけ。既定 (入っていない) は今までどおり:
 *   送る値は今の jan_code・止めない・Company DB も読まない (登録の道)。プレビューと詳細画面に「止める設定を入れたら止まる」印を出すだけ。
 *   本番ではまだ「移す」(jan-link) を流していない = 今ある JAN は結ばれていない。設定を入れるのは、中原さんが jan-link で移した結果を見てから
 *   (推定で現場を止めない)。
 * 単位 (楽天に送る値と同じ決め方 = cdb-jan-compare.mjs の janUnitsForDraft = buildItemPayload と同じ):
 *   単品 = 出品の商品コード ↔ product_drafts.jan_code (なしの理由 = draft_rakuten.catalog_id_exemption_reason)
 *   まとまり = 子の SKU ごと ↔ draft_sku_jans (なしの理由 = draft_sku_catalog_exemptions → 無ければページ共通の理由)
 */
import crypto from 'node:crypto';
import { janUnitsForDraft, janUnitsForDrafts, readCdbJans, janPairKey, JAN_READ_DEADLINE_MS } from './cdb-jan-compare.mjs';
import { JAN_LINK_REASONS, JAN_LINK_FIX } from './jan-link.mjs';

/** 止める設定の名前 (環境変数)。'1' のときだけ止める。ほかの値・無し = 止めない (今までどおり) */
export const JAN_ENFORCE_ENV = 'PH_JAN_ENFORCE';
export function janEnforceOn(env = process.env) {
  return String((env && env[JAN_ENFORCE_ENV]) ?? '').trim() === '1';
}

/** 止める理由 (画面・プレビュー・出品の答えで同じ言葉を使う) */
export const JAN_GATE_PROBLEMS = Object.freeze({
  not_linked: {
    label: '商品マスタに結ばれていない',
    text: 'JAN が商品マスタ (Company DB) の JAN に結ばれていない (手で打った・結んだ後に値が変わった)',
    fix: 'カテゴリ・属性タブの「カタログID」で、商品マスタの JAN から選ぶ',
  },
  needs_review: {
    label: '要確認',
    text: 'JAN が要確認 (商品マスタの JAN に 1 つに決まらなかった)',
    fix: 'カテゴリ・属性タブの「カタログID」で、商品マスタの JAN から選ぶ (商品マスタに無ければ、マスタの入力の商品の画面で足してから)',
  },
  changed: {
    label: '商品マスタで変わった',
    text: '結んだ JAN が商品マスタで外された・値が変わった・別の商品に移った',
    fix: 'カテゴリ・属性タブの「カタログID」で、商品マスタの JAN から選び直す',
  },
  both: {
    label: 'JAN と「なしの理由」の両方',
    text: 'JAN とカタログ ID なしの理由の両方が入っている (楽天に送るのはどちらか 1 つだけ)',
    fix: 'JAN を使うなら「IDあり」で商品マスタの JAN を選び直す (理由は外れる)。使わないなら「IDなし」にする',
  },
  neither: {
    label: 'JAN も「なしの理由」も無い',
    text: 'JAN もカタログ ID なしの理由も無い',
    fix: '「IDあり」で商品マスタの JAN を選ぶか、「IDなし」で理由を選ぶ (商品マスタに JAN が無ければ、マスタの入力の商品の画面で足す)',
  },
  dup: {
    label: 'SKU の JAN の行が 2 つ',
    text: '大文字小文字だけ違う同じ SKU の JAN の行が 2 つある (どちらを送るか決まらない)',
    fix: 'カテゴリ・属性タブの「カタログID」で、商品マスタの JAN を選び直す (行が 1 つになる)',
  },
  no_code: {
    label: '出品の商品コードが無い',
    text: '出品の商品コードが無いので JAN を確かめられない',
    fix: '基本情報タブで商品コードを入れる',
  },
  unreadable: {
    label: '確かめられない',
    text: '商品マスタ (Company DB) を読めないので、結んだ JAN を確かめられない',
    fix: '少し待ってからもう一度',
  },
});

const str = (v) => String(v ?? '').trim();
const normSku = (v) => String(v ?? '').trim().toLowerCase();
const validReason = (v) => {
  if (v == null || String(v).trim() === '') return null;
  const n = Number(v);
  return Number.isInteger(n) && n >= 1 && n <= 6 ? n : null;
};
/** JAN の大まかな比べ方 (core.norm_code に近い: 全角 → 半角・空白を除く・小文字)。最後の判定は Company DB の jan_norm で */
export function looseJan(v) {
  return String(v ?? '').normalize('NFKC').replace(/[\s 　﻿​]/g, '').toLowerCase();
}
const EMPTY_LINK = Object.freeze({ state: null, row: null, value: null, reason: null });
const linkOf = (r) => (r ? {
  state: r.jan_link_state ?? null,
  row: r.jan_cdb_row == null ? null : Number(r.jan_cdb_row),
  value: r.jan_cdb_value == null ? null : String(r.jan_cdb_value),
  reason: r.jan_link_reason ?? null,
} : EMPTY_LINK);

/** master-edit の商品の画面 (JAN を足す・直すところ) */
export const masterEditSkuUrl = (code) => `/apps/master-edit/sku/${encodeURIComponent(str(code))}`;

/**
 * 出品の単位と、今の SQLite の状態 (同期・読むだけ)。
 * 戻り値 { draftId, neCode, mode, units: [{ code, phJan, link, dup, ownReason, fallbackReason }] } / null (ドラフトが無い)
 *   ownReason = その単位に入っている「なしの理由」(単品 = ページの理由・まとまり = その SKU の理由)
 *   fallbackReason = まとまりのページ共通の理由 (JAN の無い SKU で、SKU の理由が無いときに使う。JAN のある SKU には効かない)
 */
export function listingJanUnits(db, draftId) {
  const d = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(draftId);
  if (!d) return null;
  const { mode, units } = janUnitsForDraft(db, d);   // buildItemPayload と同じ決め方 (resolveVariationGroup)
  const rk = db.prepare('SELECT catalog_id_exemption_reason FROM draft_rakuten WHERE draft_id = ?').get(d.id) || {};
  return assembleUnits(d, { mode, units }, {
    pageReason: validReason(rk.catalog_id_exemption_reason),
    skuRows: mode === 'variation' ? db.prepare('SELECT * FROM draft_sku_jans WHERE draft_id = ? ORDER BY sku_code').all(d.id) : [],
    exRows: mode === 'variation' ? db.prepare('SELECT sku_code, reason FROM draft_sku_catalog_exemptions WHERE draft_id = ?').all(d.id) : [],
  });
}

/** 単位と SQLite の行から、出品の単位の形を組む (1 件ずつ = listingJanUnits と、一括 = listingJanUnitsBulk の両方がこれを通る) */
function assembleUnits(d, { mode, units }, { pageReason, skuRows, exRows }) {
  const base = { draftId: d.id, neCode: str(d.ne_code) };
  if (mode !== 'variation') {
    return { ...base, mode: 'single', units: units.map((u) => ({ code: u.code, phJan: u.phJan, link: linkOf(d), dup: false, ownReason: pageReason, fallbackReason: null })) };
  }
  const byNorm = new Map();
  for (const r of [...skuRows].sort((a, b) => String(a.sku_code).localeCompare(String(b.sku_code)))) {
    const k = normSku(r.sku_code);
    if (!byNorm.has(k)) byNorm.set(k, []);
    byNorm.get(k).push(r);
  }
  // buildItemPayload と同じ引き方 (SKU コードを小文字にした鍵でそのまま引く)
  const exOf = new Map(exRows.map((r) => [String(r.sku_code), validReason(r.reason)]));
  return {
    ...base,
    mode: 'variation',
    units: units.map((u) => {
      const key = u.code.toLowerCase();
      const same = byNorm.get(normSku(u.code)) || [];
      const own = same.find((r) => String(r.sku_code) === key) || null;
      return { code: u.code, phJan: u.phJan, link: own ? linkOf(own) : EMPTY_LINK, dup: same.length > 1, ownReason: exOf.get(key) ?? null, fallbackReason: pageReason };
    }),
  };
}

/**
 * 一括版 (管理画面の「今止めたら止まる」の数): ドラフトの数によらず SQLite を数回だけ読む (Codex #1713 R1 L1)。
 * 単位の決め方は B1 の一括の関数 janUnitsForDrafts (= 1 件ずつの janUnitsForDraft と同じ答え・試験で突き合わせる)。戻り値 Map(draftId → 単位の形)
 */
export function listingJanUnitsBulk(db, drafts) {
  const out = new Map();
  if (!drafts.length) return out;
  const unitsOf = janUnitsForDrafts(db, drafts);
  const reasonOf = new Map(db.prepare('SELECT draft_id, catalog_id_exemption_reason AS r FROM draft_rakuten').all().map((x) => [x.draft_id, validReason(x.r)]));
  const group = (rows) => { const m = new Map(); for (const r of rows) { if (!m.has(r.draft_id)) m.set(r.draft_id, []); m.get(r.draft_id).push(r); } return m; };
  const skuOf = group(db.prepare('SELECT * FROM draft_sku_jans').all());
  const exOf = group(db.prepare('SELECT draft_id, sku_code, reason FROM draft_sku_catalog_exemptions').all());
  for (const d of drafts) {
    out.set(d.id, assembleUnits(d, unitsOf.get(d.id), { pageReason: reasonOf.get(d.id) ?? null, skuRows: skuOf.get(d.id) || [], exRows: exOf.get(d.id) || [] }));
  }
  return out;
}

/**
 * 段 4 の判定の本体 (止める単位の一覧)。門 (janListingBlock) と数え (enforceForecast) は必ずこれを通す = 同じ答え (Codex #1713 R1 M1)。
 * 戻り値 [{ u (単位・商品コードが無いときは null), d (stop) }] (空 = 止めない)
 */
export function gateProblemsOf(lj) {
  if (!lj) return [];
  if (!lj.units.length) return [{ u: null, d: stop('no_code') }];
  return lj.units.map((u) => ({ u, d: decideLocal(u) })).filter((x) => !x.d.ok);
}

const stop = (problem, detail = '') => ({ ok: false, problem, detail: detail || '' });

/**
 * 段 4: SQLite だけで決まる判定 (Company DB を読まない・同期)。
 * 戻り値 { ok: true, verdict: 'verify' } (結んである = 段 5 で Company DB に確かめる) / { ok: true, verdict: 'reason', send: { reason } } / stop
 */
export function decideLocal(u) {
  if (u.dup) return stop('dup');
  if (u.phJan) {
    if (u.ownReason != null) return stop('both', `JAN ${u.phJan}・なしの理由 ${u.ownReason}`);
    if (u.link.state === 'needs_review') return stop('needs_review', JAN_LINK_REASONS[u.link.reason] || u.link.reason || '');
    if (u.link.state !== 'linked' || u.link.row == null || !u.link.value || looseJan(u.link.value) !== looseJan(u.phJan)) return stop('not_linked', `JAN ${u.phJan}`);
    return { ok: true, verdict: 'verify' };
  }
  const reason = u.ownReason ?? u.fallbackReason;
  if (reason != null) return { ok: true, verdict: 'reason', send: { reason } };
  return stop('neither');
}

/** 段 5: 結んである単位を、読んだ Company DB の行で確かめ直す。送る値は Company DB の値 */
export function verifyLinked(u, read) {
  if (!read || !read.ok) return stop('unreadable', (read && read.reason) || '');
  const row = read.byPair.get(janPairKey(u.code, u.phJan));
  if (!row) return stop('unreadable', 'Company DB の答えにこの商品コードが無い');
  let jans = row.jans;
  if (typeof jans === 'string') { try { jans = JSON.parse(jans); } catch { jans = null; } }
  if (!Array.isArray(jans)) return stop('unreadable', 'Company DB の JAN の形が違う');
  if (row.sku_id == null) return stop('changed', '商品マスタにこの商品コードが無い');
  if (row.product_id == null) return stop('changed', '商品マスタではセット・例外の商品 (JAN を持たない)');
  const hit = jans.find((j) => String(j.row) === String(u.link.row));
  if (!hit) return stop('changed', `結んだ JAN (${u.link.value}) は商品マスタのこの商品の有効な JAN に無い`);
  if (String(hit.value) !== String(u.link.value)) return stop('changed', `商品マスタの値が ${u.link.value} から ${hit.value} に変わった`);
  if (row.jan_norm == null || String(hit.norm) !== String(row.jan_norm)) return stop('changed', `product-hub の JAN ${u.phJan} が結んだ JAN ${hit.value} と違う`);
  return { ok: true, verdict: 'jan', send: { value: String(hit.value) } };
}

/** 単位の判定を人の言葉に (止める理由・プレビューの注意) */
export function problemText(u, d, { mode = 'single' } = {}) {
  const p = JAN_GATE_PROBLEMS[d.problem] || { label: d.problem, text: d.problem, fix: '' };
  if (!u) return `カタログID (JAN) ${p.label}: ${p.text}。直し方: ${p.fix}`;
  const who = mode === 'variation' ? `SKU「${u.code}」` : `商品コード ${u.code}`;
  const fix = d.problem === 'needs_review' && u.link.reason && JAN_LINK_FIX[u.link.reason] ? `${p.fix} (${JAN_LINK_FIX[u.link.reason]})` : p.fix;
  return `カタログID (JAN) ${p.label}: ${who} — ${p.text}${d.detail ? ` (${d.detail})` : ''}。直し方: ${fix}`;
}

function snapshotOf(lj) {
  return crypto.createHash('sha256').update(JSON.stringify([lj.draftId, lj.neCode, lj.mode,
    lj.units.map((u) => [u.code, u.phJan, u.link.state, u.link.row, u.link.value, u.link.reason, u.dup, u.ownReason, u.fallbackReason])])).digest('hex');
}

/** 確かめた後に SQLite の JAN の状態が変わっていないか (送る直前に同期で照らす) */
export function janGateFresh(db, draftId, gate) {
  const lj = listingJanUnits(db, draftId);
  return !!(lj && gate && gate.snapshot && snapshotOf(lj) === gate.snapshot);
}

/**
 * 段 4 (出品の前に止める・SQLite だけ・同期): 止める設定が入っているときに、止める単位があれば理由の文 (無ければ null)。
 * 詳細画面の「公開で登録」とボードからの出品が、画像の転送やロックの前に通る (assertRakutenListable)
 */
export function janListingBlock(db, draftId, { env = process.env } = {}) {
  if (!janEnforceOn(env)) return null;
  const lj = listingJanUnits(db, draftId);
  const bad = gateProblemsOf(lj);
  if (!bad.length) return null;
  return bad.map(({ u, d }) => problemText(u, d, { mode: lj.mode })).join(' / ');
}

/** 止める設定の前に出す知らせ (プレビュー): 「設定を入れたら止まる」単位 (SQLite だけで決まる物) */
export function janLinkNotices(db, draftId) {
  const lj = listingJanUnits(db, draftId);
  return gateProblemsOf(lj)
    .map(({ u, d }) => `${problemText(u, d, { mode: lj.mode })} (今は止めません。止める設定 ${JAN_ENFORCE_ENV}=1 を入れると出品が止まります)`);
}

/**
 * 段 5: 送る前に JAN を決める。止める設定が無ければ Company DB を読まずに { enforce: false } (今までどおり jan_code を送る)。
 * 設定があれば: 段 4 の判定 → 結んである単位を Company DB で確かめ直す (読むだけの取引・期限つき・読めない = 止める)。
 * 戻り値 { enforce, ok, mode, units: [{ ...unit, decision }], reasons, snapshot }
 *   decision = { ok: true, send: { value } | { reason } } / { ok: false, problem, detail }
 *   snapshot = 確かめたときの SQLite の状態の印 (送る直前に janGateFresh で照らす)
 */
export async function resolveListingJans(db, draftId, { env = process.env, enforce = janEnforceOn(env), deadlineMs = JAN_READ_DEADLINE_MS, deadlineAt = null, read: given = null, lj: givenLj = null } = {}) {
  // lj / read = 呼ぶ側がもう組んだ単位と、その単位の全部の組で読んだ答え (プレビュー = 1 回の読みで payload と B1 の印の両方・Codex #1713 R1 M2)
  const lj = givenLj || listingJanUnits(db, draftId);
  if (!lj) return { enforce, ok: false, mode: null, units: [], reasons: ['ドラフトが見つかりません'], snapshot: null };
  const snapshot = snapshotOf(lj);   // 🚨 読む (await) より前の状態の印
  const units = lj.units.map((u) => ({ ...u, local: decideLocal(u) }));
  if (!enforce) return { enforce: false, ok: true, mode: lj.mode, units, reasons: [], snapshot };
  const toVerify = units.filter((u) => u.local.verdict === 'verify');
  let read = given;
  if (toVerify.length && !read) {
    read = await readCdbJans(toVerify.map((u) => ({ code: u.code, jan: u.phJan })), { env, deadlineMs, deadlineAt });
  }
  for (const u of units) u.decision = u.local.verdict === 'verify' ? verifyLinked(u, read) : u.local;
  const reasons = units.filter((u) => !u.decision.ok).map((u) => problemText(u, u.decision, { mode: lj.mode }));
  if (!units.length) reasons.push(problemText(null, stop('no_code')));
  return { enforce: true, ok: reasons.length === 0, mode: lj.mode, units, reasons, snapshot };
}

/** buildItemPayload が使う: SKU コード (小文字) → 確かめた決定 */
export function gateDecisionFor(gate, skuCode) {
  if (!gate || !gate.enforce) return null;
  const k = normSku(skuCode);
  const u = gate.units.find((x) => normSku(x.code) === k);
  return u ? u.decision : null;
}

/**
 * 🚨 送る直前の 1 か所 (段 5): payload の articleNumber を照らす。戻り値 = 止める理由の配列 (空 = 送ってよい)。
 *   いつも: どの SKU も「JAN (value) 1 つ」か「なしの理由 (exemptionReason 1〜6)」のどちらか 1 つだけ。
 *   止める設定のとき: 確かめた SKU と送る SKU が同じ・送る値 = 確かめた Company DB の値 (カタログID の属性も)・
 *     確かめた後に SQLite の JAN の状態が変わっていない (db を渡したとき)
 */
export function checkArticleNumbers(payload, gate, { db = null, draftId = null } = {}) {
  const out = [];
  const variants = payload && payload.variants;
  const entries = variants && typeof variants === 'object' ? Object.entries(variants) : [];
  if (!entries.length) return ['楽天に送る SKU がありません'];
  for (const [sku, v] of entries) {
    const an = v && v.articleNumber;
    const keys = an && typeof an === 'object' ? Object.keys(an) : [];
    const hasValue = !!an && typeof an.value === 'string' && an.value.trim() !== '';
    const hasReason = !!an && Number.isInteger(an.exemptionReason) && an.exemptionReason >= 1 && an.exemptionReason <= 6;
    if (keys.length !== 1 || hasValue === hasReason) {
      out.push(`SKU「${sku}」のカタログID: JAN 1 つか「カタログIDなしの理由」のどちらか 1 つだけにしてください`);
    }
  }
  if (!gate || !gate.enforce) return out;
  if (!gate.ok) out.push(...gate.reasons);
  if (db && draftId != null && !janGateFresh(db, draftId, gate)) {
    out.push('カタログID (JAN) を確かめている間に JAN が変わりました。もう一度押してください (何も送っていません)');
  }
  const want = new Map(gate.units.map((u) => [normSku(u.code), u.decision]));
  if (want.size !== entries.length || entries.some(([sku]) => !want.has(normSku(sku)))) {
    out.push('楽天に送る SKU と、JAN を確かめた SKU が合いません (何も送っていません)');
  }
  for (const [sku, v] of entries) {
    const d = want.get(normSku(sku));
    if (!d || !d.ok) continue;   // 理由は上で積んだ
    const an = (v && v.articleNumber) || {};
    const same = d.send.value != null ? an.value === d.send.value && an.exemptionReason === undefined
      : an.exemptionReason === d.send.reason && an.value === undefined;
    if (!same) out.push(`SKU「${sku}」: 送るカタログID が、確かめた商品マスタの値と違います (何も送っていません)`);
    const cat = ((v && v.attributes) || []).filter((a) => a && a.name === 'カタログID');
    if (cat.length && !(d.send.value != null && cat.length === 1 && Array.isArray(cat[0].values) && cat[0].values.length === 1 && cat[0].values[0] === d.send.value)) {
      out.push(`SKU「${sku}」: 属性「カタログID」が、確かめた商品マスタの値と違います (何も送っていません)`);
    }
  }
  return [...new Set(out)];
}

/**
 * 段 3 (選ぶ画面): 単位ごとの今の状態と、Company DB のその商品の有効な JAN (選べる物)。read = readCdbJans の戻り値 (同じ組で読んだ物)
 * 戻り値 { enforce, readable, reason, mode, units: [{ code, phJan, linkedRow, state, label, text, fix, detail, cdb: { skuMissing, noJanKind, jans: [{ row, value }] }, masterEditUrl }] }
 *   state = 'ok_jan' (結んであり商品マスタで確かめた) / 'ok_reason' (なしの理由) / 止める理由 (JAN_GATE_PROBLEMS の鍵)
 */
export function pickViewOf(lj, read, { env = process.env } = {}) {
  const enforce = janEnforceOn(env);
  const readable = !!(read && read.ok);
  const units = lj.units.map((u) => {
    const local = decideLocal(u);
    const d = local.verdict === 'verify' ? verifyLinked(u, read) : local;
    const row = readable ? read.byPair.get(janPairKey(u.code, u.phJan)) : null;
    let jans = row ? row.jans : [];
    if (typeof jans === 'string') { try { jans = JSON.parse(jans); } catch { jans = []; } }
    if (!Array.isArray(jans)) jans = [];
    const state = d.ok ? (d.send.value != null ? 'ok_jan' : 'ok_reason') : d.problem;
    const p = d.ok ? null : JAN_GATE_PROBLEMS[d.problem];
    return {
      code: u.code,
      phJan: u.phJan,
      // 選んである行 (結んであって、段 4 で止まらない物だけ。確かめられない / 変わった は選び直してもらう)
      linkedRow: local.verdict === 'verify' && (d.ok || d.problem === 'unreadable') ? u.link.row : null,
      reason: u.ownReason ?? (u.phJan ? null : u.fallbackReason),
      state,
      label: d.ok ? (d.send.value != null ? '商品マスタの JAN を選んである' : `カタログIDなし (理由 ${d.send.reason})`) : p.label,
      text: d.ok ? '' : p.text,
      fix: d.ok ? '' : (d.problem === 'needs_review' && u.link.reason && JAN_LINK_FIX[u.link.reason] ? `${p.fix} (${JAN_LINK_FIX[u.link.reason]})` : p.fix),
      detail: d.ok ? '' : d.detail,
      cdb: {
        skuMissing: !!row && row.sku_id == null,
        noJanKind: !!row && row.sku_id != null && row.product_id == null ? (row.sku_kind === 'set' ? 'set' : 'other') : null,
        jans: jans.map((j) => ({ row: Number(j.row), value: String(j.value) })),
      },
      masterEditUrl: masterEditSkuUrl(u.code),
    };
  });
  return { enforce, readable, reason: readable ? null : ((read && read.reason) || '読めない'), mode: lj.mode, units };
}

/**
 * 選んだ JAN を Company DB で確かめる (選ぶ画面の保存の前・読むだけ)。read = readCdbJans([{ code, jan: value }]) の戻り値
 * 戻り値 { ok: true, row, value } / { ok: false, status, error }
 */
export function verifySelection({ code, row, value }, read) {
  if (!read || !read.ok) return { ok: false, status: 503, error: `商品マスタ (Company DB) を読めないので選べません (${(read && read.reason) || '読めない'})。少し待ってからもう一度` };
  const r = read.byPair.get(janPairKey(code, value));
  if (!r) return { ok: false, status: 503, error: '商品マスタ (Company DB) の答えにこの商品コードがありません。少し待ってからもう一度' };
  if (r.sku_id == null) return { ok: false, status: 409, error: `${code} は商品マスタにありません (新商品の登録の後に選べます)` };
  if (r.product_id == null) return { ok: false, status: 409, error: `${code} は商品マスタではセット・例外の商品なので JAN を持てません (「IDなし」で理由を選んでください)` };
  let jans = r.jans;
  if (typeof jans === 'string') { try { jans = JSON.parse(jans); } catch { jans = null; } }
  if (!Array.isArray(jans)) return { ok: false, status: 503, error: '商品マスタの JAN の形が違います' };
  const hit = jans.find((j) => String(j.row) === String(row));
  if (!hit || String(hit.value) !== String(value) || r.jan_norm == null || String(hit.norm) !== String(r.jan_norm)) {
    return { ok: false, status: 409, error: `選んだ JAN (${value}) は今の商品マスタの ${code} の有効な JAN にありません (外された・変わった)。画面を開き直して選び直してください` };
  }
  return { ok: true, row: Number(hit.row), value: String(hit.value) };
}

/**
 * 止める設定を入れたら、今いくつ止まるか (jan-link の管理画面に出す・SQLite だけ)。
 * 🚨 中原さんの答え 5a: この数が 0 (か止まってよい物だけ) になってから設定を入れる = 少なく出てはいけない (Codex #1713 R1 M1):
 *   - 判定は門と同じ関数 (gateProblemsOf)。商品コードが無い (単位が無い) ドラフトも止まる商品として数える
 *   - 1 件でも読めない・組めない = 投げる (黙って外さない)。呼ぶ側は「数えきれていない (この数は信用しない)」と出す
 *   - 数えるのは楽天に未登録で、色追加のカード (既存ページに足す = 出品しない) でない物。保留・除外も数える (詳細画面からは出せるので)
 * 戻り値 { drafts, units, stopDrafts, heldStopDrafts, byProblem: { problem: 単位の数 } }
 */
export function enforceForecast(db) {
  const drafts = db.prepare(`SELECT d.* FROM product_drafts d LEFT JOIN draft_rakuten r ON r.draft_id = d.id
    WHERE r.registered_at IS NULL AND d.added_to_draft_id IS NULL ORDER BY d.id`).all();
  const all = listingJanUnitsBulk(db, drafts);
  const out = { drafts: 0, units: 0, stopDrafts: 0, heldStopDrafts: 0, byProblem: {} };
  for (const d of drafts) {
    const lj = all.get(d.id);
    if (!lj) throw new Error(`ドラフト ${d.id} の単位を組めない`);
    out.drafts++;
    out.units += lj.units.length;
    const bad = gateProblemsOf(lj);
    if (!bad.length) continue;
    out.stopDrafts++;
    if (['on_hold', 'excluded'].includes(String(d.status || ''))) out.heldStopDrafts++;
    for (const { d: x } of bad) out.byProblem[x.problem] = (out.byProblem[x.problem] || 0) + 1;
  }
  return out;
}
