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
import { janUnitsForDraft, readCdbJans, janPairKey, JAN_READ_DEADLINE_MS } from './cdb-jan-compare.mjs';
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
  const { mode, units } = janUnitsForDraft(db, d);
  const rk = db.prepare('SELECT catalog_id_exemption_reason FROM draft_rakuten WHERE draft_id = ?').get(d.id) || {};
  const pageReason = validReason(rk.catalog_id_exemption_reason);
  const base = { draftId: d.id, neCode: str(d.ne_code) };
  if (mode !== 'variation') {
    return { ...base, mode: 'single', units: units.map((u) => ({ code: u.code, phJan: u.phJan, link: linkOf(d), dup: false, ownReason: pageReason, fallbackReason: null })) };
  }
  const byNorm = new Map();
  for (const r of db.prepare('SELECT * FROM draft_sku_jans WHERE draft_id = ? ORDER BY sku_code').all(d.id)) {
    const k = normSku(r.sku_code);
    if (!byNorm.has(k)) byNorm.set(k, []);
    byNorm.get(k).push(r);
  }
  // buildItemPayload と同じ引き方 (SKU コードを小文字にした鍵でそのまま引く)
  const exOf = new Map(db.prepare('SELECT sku_code, reason FROM draft_sku_catalog_exemptions WHERE draft_id = ?').all(d.id)
    .map((r) => [String(r.sku_code), validReason(r.reason)]));
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
  if (!lj) return null;
  if (!lj.units.length) return 'カタログID (JAN): 出品の商品コードが無いので JAN を確かめられません';
  const bad = lj.units.map((u) => ({ u, d: decideLocal(u) })).filter((x) => !x.d.ok);
  if (!bad.length) return null;
  return bad.map(({ u, d }) => problemText(u, d, { mode: lj.mode })).join(' / ');
}

/** 止める設定の前に出す知らせ (プレビュー): 「設定を入れたら止まる」単位 (SQLite だけで決まる物) */
export function janLinkNotices(db, draftId) {
  const lj = listingJanUnits(db, draftId);
  if (!lj) return [];
  return lj.units.map((u) => ({ u, d: decideLocal(u) })).filter((x) => !x.d.ok)
    .map(({ u, d }) => `${problemText(u, d, { mode: lj.mode })} (今は止めません。止める設定 ${JAN_ENFORCE_ENV}=1 を入れると出品が止まります)`);
}

/**
 * 段 5: 送る前に JAN を決める。止める設定が無ければ Company DB を読まずに { enforce: false } (今までどおり jan_code を送る)。
 * 設定があれば: 段 4 の判定 → 結んである単位を Company DB で確かめ直す (読むだけの取引・期限つき・読めない = 止める)。
 * 戻り値 { enforce, ok, mode, units: [{ ...unit, decision }], reasons, snapshot }
 *   decision = { ok: true, send: { value } | { reason } } / { ok: false, problem, detail }
 *   snapshot = 確かめたときの SQLite の状態の印 (送る直前に janGateFresh で照らす)
 */
export async function resolveListingJans(db, draftId, { env = process.env, enforce = janEnforceOn(env), deadlineMs = JAN_READ_DEADLINE_MS, deadlineAt = null, read: given = null } = {}) {
  const lj = listingJanUnits(db, draftId);
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
  if (!units.length) reasons.push('カタログID (JAN): 出品の商品コードが無いので JAN を確かめられません');
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
 * 止める設定を入れたら、今いくつ止まるか (jan-link の管理画面に出す・SQLite だけ・楽天に登録済みの商品は数えない)
 * 戻り値 { drafts, units, stopDrafts, byProblem: { problem: 単位の数 } }
 */
export function enforceForecast(db) {
  const ids = db.prepare(`SELECT d.id FROM product_drafts d LEFT JOIN draft_rakuten r ON r.draft_id = d.id
    WHERE r.registered_at IS NULL AND COALESCE(d.status, '') NOT IN ('excluded') AND d.added_to_draft_id IS NULL ORDER BY d.id`).all().map((r) => r.id);
  const out = { drafts: 0, units: 0, stopDrafts: 0, byProblem: {} };
  for (const id of ids) {
    let lj;
    try { lj = listingJanUnits(db, id); } catch { continue; }
    if (!lj || !lj.units.length) continue;
    out.drafts++;
    let stopped = false;
    for (const u of lj.units) {
      out.units++;
      const d = decideLocal(u);
      if (d.ok) continue;
      stopped = true;
      out.byProblem[d.problem] = (out.byProblem[d.problem] || 0) + 1;
    }
    if (stopped) out.stopDrafts++;
  }
  return out;
}
