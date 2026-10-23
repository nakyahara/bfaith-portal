#!/usr/bin/env node
/**
 * strip-material-tags.mjs — 一回きり: 商品名の末尾の資材の印 (_白ビ袋 / _梱機プ / _長3封 / _白プチ / _ネコ段 / _K-44 / _K-50 / _K-60 / _厚紙封 / _パフ箱 / _その他
 * = lib/master-write.mjs の MATERIAL_TAG_RE) を外す (中原さん 10/23 承認・1 回だけ・画面のボタンは作らない。D-47 = 資材は梱包アプリで登録し、名前には付けない)。
 *
 * 🚨 一般の機能ではない (台帳 config/jobs-registry.mjs の temporary_asset 'strip-material-tags' = 済んだら消す)。
 *
 * 外すのは「資材がほかの所に登録してある」単品だけ (名前の印が最後の手がかりになっている商品は外さない = 一覧にして返す):
 *   - 郵便料金 (apps/postage・Render の postage.db の pm_skus.default_material_code): 印から資材を引く道 (apps/postage/import.js の materialFromName =
 *     _長3封 / _梱機プ / _白プチ / _白ビ袋) がある印は、その商品の資材の登録があり、印から引ける資材と同じときだけ。
 *     無い = 資材の列が空の表を取り込むと、名前の印から資材を引く (smoke.mjs「資材列が空 → 商品名の末尾 _梱機プ から白プチを引く」) = 外すと引けなくなる
 *   - 梱包アプリ (apps/packing・miniPC の C:\tools\bfaith-picking\data\picking.db の pk_pack_material_rules): この商品だけの伝票 (構成 = この SKU × 数量) の
 *     資材の登録 (status active) が 1 つ以上ある (--gate postage = 郵便料金が印から資材を引く印だけ梱包アプリを見ない・ほかの印は見る = どこにも登録が無いまま外さない)。配送の種類ごとの登録なので「印と同じ資材か」は見ない (dry-run に数だけ出す)
 *   - そのほか外さない: セット・例外の SKU・登録の状態が available でない (登録をやめた cancelled は数えるだけ)・印が 2 つ重なる・外すと 2 字未満・
 *     外した名前が保存の決まりに合わない (empty・制御文字・コードと同じ)・外すとほかの商品と同じ名前 (NFKC・空白を除いて比べる)
 *
 * 書き方 (apply): 1 件 = 1 回の商品の画面の保存 (lib/master-write.mjs の saveSku = 1 取引) を、画面と同じロール master_edit で呼ぶ
 *   = 持ち主の門・段階・鍵・編集の印・変更の記録 (source_system portal_master_edit・人 = --actor・理由 = --reason)・単品の商品の行の名前 (products.name) も同じ値にそろえる。
 *   → 「NEへ送る」(0094 の ops.ne_upd_units) は「その欄の今の値を最後に入れたのがポータル (portal_master_edit)」だけを出す = この保存は人の直しとして出る。
 *   🚨 DB の持ち主のロール (COMPANY_DB_URL) では書かない (画面のロールの守りを通らない) = apply は master_edit のロールでつないだときだけ。
 *   🚨 画面と同じく保存を開くスイッチ env MASTER_EDIT_OPEN = 1 のときだけ書く (Render の値が 1 のときだけ、流す PowerShell で 1 にする)。
 *   request_id = SKU・前の名前・後の名前・やり直しの回から決まる (同じ直しを 2 回書かない・前に失敗した回は次の回で)。
 *
 * 計画のハッシュ = 人・理由・条件 (--gate)・書くもの全部 (SKU・コード・前の名前・後の名前)。この道具で済んだもの (保存の記録 ops.master_edit_requests の done・
 *   今の名前 = 後の名前) も同じ形で計画に入れる = 途中で止まっても、同じハッシュでもう一度 apply すれば残りだけを書く (済んだものは書かない)。全部済んだ後の apply = 何もしない。
 *   印のある商品・資材の登録が増えた / 変わった = ハッシュが変わる = もう一度 dry-run から。
 *
 * 使い方 (miniPC の PowerShell・リポジトリ直下・値は一重引用符):
 *   準備: $env:DOTENV_CONFIG_PATH = 'C:\Users\bfaith\bfaith-portal\.env'
 *         郵便料金の DB = Render の毎晩のバックアップの写し (gdrive:bfaith-backup/render/daily/postage-<日付>-*.db.gz を rclone で取って gunzip)
 *   # 1. 読むだけ (既定・読み取りの取引)。env COMPANY_DB_MASTER_EDIT_URL か COMPANY_DB_WATCH_URL か --url
 *   node -r dotenv/config scripts/company-db/strip-material-tags.mjs --actor '<人のメール>' --postage-db '<postage.db>' --packing-db 'C:\tools\bfaith-picking\data\picking.db' [--csv skipped.csv] [--json plan.json]
 *   # 2. 書く (dry-run の表を中原さんが見て OK の後だけ・同じ --actor / --reason / --gate / 同じ資材の DB)。env COMPANY_DB_MASTER_EDIT_URL (画面のロール master_edit) か --url・
 *   #    env MASTER_EDIT_OPEN = 1 (どちらもその PowerShell だけで $env: に入れる・.env には書かない)
 *   node -r dotenv/config scripts/company-db/strip-material-tags.mjs --actor '<人のメール>' --postage-db ... --packing-db ... --apply --expect-hash <64 桁> --yes [--limit 20] [--chunk 50]
 * 終了コード: 0 = dry-run で計画を出した / apply で書き終えた (失敗なし)・1 = 書かない (ハッシュが違う) / 途中で止めた / 失敗があった・2 = 引数・接続・ロールが違う
 */
import fs from 'node:fs';
import path from 'node:path';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import { openPgClient, pgAdapter } from './migrate.mjs';
import { normSku } from '../../lib/sku-norm.js';
import { saveSku, readCurrent, editTokenOf, jstDate, stable, sha256, MATERIAL_TAG_RE, MasterWriteError, REASON_MAX } from '../../lib/master-write.mjs';
import { materialFromName, normalizeSku as postageSkuKey } from '../../apps/postage/import.js';

export const TOOL = 'strip-material-tags';
const PLAN_VERSION = 1;
export const DEFAULT_REASON = '商品名の末尾の資材の印を外す (資材は梱包アプリで登録・D-47・一回きり 10/23 中原さん)';
export const CHUNK_DEFAULT = 50;
export const CHUNK_MAX = 200;
/** 同じ直しの request_id のやり直しの回 (失敗の記録が残った回は使わない) */
export const MAX_ATTEMPTS = 5;
/** NE へ送る CSV の 1 ファイルの行の上限 (0094 の ops.ne_upd_file_limit) */
export const NE_FILE_ROWS = 1000;
/** 毎朝の自動で送る上限 (0094 の ops.ne_upd_auto_limit。超える朝は held) */
export const NE_AUTO_LIMIT = 100;
export const GATES = Object.freeze(['both', 'postage', 'none']);
const CTRL_RE = new RegExp(`[\\x00-\\x1f\\x7f${String.fromCharCode(0x2028, 0x2029)}]`);
/** 誤りの文 (接続文字列を出さない) */
const cleanMsg = (e) => String(e && e.message).replace(/postgres(ql)?:\/\/\S+/gi, '[接続文字列]');
const fail = (message) => Object.assign(new Error(message), { code: 'STRIP_ARGS' });

/** 外さない理由 (dry-run の表・CSV の言葉) */
export const SKIP_WORDS = Object.freeze({
  kind_set: 'セット (外さない・構成品の名前で見る)',
  kind_exception: '例外の SKU (NE に無い)',
  reg_state: '登録の状態が available でない (quarantined など)',
  double_tag: '資材の印が 2 つ重なっている (人が見て直す)',
  name_short: '外すと名前が 2 字未満',
  name_invalid: '外した名前が保存の決まりに合わない (empty・制御文字・コードと同じ)',
  duplicate_name: '外すとほかの商品と同じ名前になる',
  no_postage_material: '郵便料金 (postage) に資材の登録が無い (名前の印から資材を引く道が残っている)',
  postage_mismatch: '郵便料金の資材の登録が名前の印と違う',
  no_packing_rule: '梱包アプリにこの商品だけの伝票の資材の登録が無い',
});
/** 書くときに飛ばす / だめにする理由のうち、その商品だけのもの (ほかは止める = 夜間の取り込み中・切替前・つながらない など) */
const ITEM_REASONS = new Set(['version_conflict', 'retry', 'invalid_input', 'not_found', 'cancelled_sku', 'exception_sku', 'no_product', 'request_id_reused', 'cost_overlap']);

/** 名前の末尾の資材の印。無い = null。{ tag, to } (to = 印を外して前後の空白を除いた名前 = 保存 (textIn) と同じ形) */
export function stripTag(name) {
  const s = String(name ?? '');
  const m = MATERIAL_TAG_RE.exec(s);
  if (!m) return null;
  return { tag: m[1], to: s.slice(0, m.index).trim() };
}
/**
 * 🆕 --gate none (10/23 中原さん「資材の登録の確かめを外して、印の付いた単品の名前を全部外す」): 末尾の印を重なりも全部外す。
 *   無い = null。{ tag (いちばん外側の印), tags, to }
 */
export function stripAllTags(name) {
  let s = String(name ?? '');
  const tags = [];
  for (let m = MATERIAL_TAG_RE.exec(s); m; m = MATERIAL_TAG_RE.exec(s)) { tags.push(m[1]); s = s.slice(0, m.index).trimEnd(); }
  if (!tags.length) return null;
  return { tag: tags[0], tags, to: s.trim() };
}
/** 同じ名前かの比べ方 (NFKC・空白を除く) */
const nameKey = (s) => String(s ?? '').normalize('NFKC').replace(/\s+/g, '');
/** request_id (uuid の形) = SKU・前の名前・後の名前・やり直しの回から決まる */
export function stripRequestId(skuId, from, to, attempt = 0) {
  const h = sha256(`${TOOL}:v${PLAN_VERSION}:${skuId}:${from}:${to}:${attempt}`);
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20, 32)}`;
}

/** 引数を読む。誤り = STRIP_ARGS */
export function parseStripArgs(argv) {
  const o = { apply: false, yes: false, expectHash: null, actor: null, reason: DEFAULT_REASON, gate: 'both', postageDb: null, packingDb: null,
    limit: null, chunk: CHUNK_DEFAULT, json: null, csv: null, url: null, show: 20 };
  const need = (i, name) => { const v = argv[i + 1]; if (v == null || v.startsWith('--')) throw fail(`${name} の後に値が要る`); return v; };
  const posInt = (v, name, max) => { if (!/^\d+$/.test(v) || Number(v) < 1 || Number(v) > max) throw fail(`${name} は 1〜${max} の整数`); return Number(v); };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--apply') o.apply = true;
    else if (a === '--yes') o.yes = true;
    else if (a === '--expect-hash') { const v = need(i, a); if (!/^[0-9a-f]{64}$/.test(v)) throw fail('--expect-hash は dry-run が出した 64 桁の 16 進'); o.expectHash = v; i++; }
    else if (a === '--actor') { o.actor = need(i, a).trim().toLowerCase(); i++; }
    else if (a === '--reason') { o.reason = need(i, a).trim(); i++; }
    else if (a === '--gate') { o.gate = need(i, a); if (!GATES.includes(o.gate)) throw fail('--gate は both (郵便料金 + 梱包アプリ・既定) か postage (郵便料金だけ) か none (資材の登録を見ずに全部外す・10/23 中原さん)'); i++; }
    else if (a === '--postage-db') { o.postageDb = need(i, a); i++; }
    else if (a === '--packing-db') { o.packingDb = need(i, a); i++; }
    else if (a === '--limit') { o.limit = posInt(need(i, a), a, 100000); i++; }
    else if (a === '--chunk') { o.chunk = posInt(need(i, a), a, CHUNK_MAX); i++; }
    else if (a === '--show') { o.show = posInt(need(i, a), a, 10000); i++; }
    else if (a === '--json') { o.json = need(i, a); i++; }
    else if (a === '--csv') { o.csv = need(i, a); i++; }
    else if (a === '--url') { o.url = need(i, a); i++; }
    else throw fail(`知らない引数: ${a}`);
  }
  if (!o.actor || !/^[^\s@]+@[^\s@]+$/.test(o.actor) || o.actor.length > 320) throw fail('--actor <人のメール> が要る (dry-run も apply も同じ人)');
  if (!o.reason || [...o.reason].length > REASON_MAX || CTRL_RE.test(o.reason)) throw fail(`--reason は 1〜${REASON_MAX} 字・制御文字なし (dry-run も apply も同じ理由)`);
  if (o.gate !== 'none' && !o.postageDb) throw fail('--postage-db <郵便料金の postage.db (Render のバックアップの写し)> が要る');
  if (o.gate === 'both' && !o.packingDb) throw fail('--packing-db <梱包アプリの picking.db (miniPC の C:\\tools\\bfaith-picking\\data\\picking.db)> が要る (--gate both)');
  if (o.apply) {
    if (o.expectHash == null) throw fail('--apply は --expect-hash <dry-run が出した計画のハッシュ> が要る');
    if (!o.yes) throw fail('--apply は --yes が要る (dry-run の表を中原さんが見て OK の後だけ)');
  } else if (o.limit != null) throw fail('--limit は --apply のときだけ');
  return o;
}

/**
 * 資材の登録を読む (読むだけ・SQLite を readonly で開く)。
 * 戻り値 = { postage: Map(郵便料金のキー → { material, source }), packing: Map(梱包アプリのキー → [{ delivery, material, qty }]) | null, meta }
 * キーはそれぞれのアプリが引く形と同じ (郵便料金 = apps/postage/import.js の normalizeSku = NFKC・前後の空白・小文字 /
 * 梱包アプリ = apps/packing/materials.js の encodeSku の前 = 前後の空白・小文字)。同じキーになる郵便料金の行が 2 つ以上 = どちらか分からない = 登録なしと見る
 */
export function readMaterialEvidence({ postageDb, packingDb = null }) {
  const require = createRequire(import.meta.url);
  const Database = require('better-sqlite3');
  const openRo = (file, what) => {
    if (!file || !fs.existsSync(file)) throw fail(`${what} のファイルが無い: ${file}`);
    return new Database(file, { readonly: true, fileMustExist: true });
  };
  const hasTable = (db, t) => !!db.prepare("select 1 from sqlite_master where type = 'table' and name = ?").get(t);
  const meta = {};
  const postage = new Map();
  {
    const db = openRo(postageDb, '郵便料金の DB');
    try {
      if (!hasTable(db, 'pm_skus')) throw fail(`郵便料金の DB に pm_skus が無い (${postageDb})`);
      for (const r of db.prepare('select sku_code, default_material_code, material_source from pm_skus').all()) {
        const k = postageSkuKey(r.sku_code);
        if (!k) continue;
        postage.set(k, postage.has(k) ? { material: null, source: null, ambiguous: true } : { material: r.default_material_code || null, source: r.material_source || null });
      }
      const u = db.prepare('select count(*) as n, sum(default_material_code is not null) as with_material, max(updated_at) as updated_at from pm_skus').get();
      meta.postage = { file: postageDb, mtime: fs.statSync(postageDb).mtime.toISOString(), rows: u.n, with_material: Number(u.with_material || 0), updated_at: u.updated_at };
    } finally { db.close(); }
  }
  let packing = null;
  if (packingDb) {
    packing = new Map();
    const db = openRo(packingDb, '梱包アプリの DB');
    try {
      if (!hasTable(db, 'pk_pack_material_rules')) throw fail(`梱包アプリの DB に pk_pack_material_rules が無い (${packingDb})`);
      let n = 0;
      for (const r of db.prepare("select combo_key, delivery_code, material_code from pk_pack_material_rules where status = 'active'").all()) {
        n++;
        const parts = String(r.combo_key).split('|').slice(1);
        if (parts.length !== 1) continue;   // この商品だけの伝票 (構成が 1 種類) の登録だけ
        const m = /^(.+)\*(\d+)$/.exec(parts[0]);
        if (!m) continue;
        let sku;
        try { sku = decodeURIComponent(m[1]); } catch { continue; }
        const k = packingSkuKey(sku);
        if (!k) continue;
        if (!packing.has(k)) packing.set(k, []);
        packing.get(k).push({ delivery: r.delivery_code, material: r.material_code, qty: Number(m[2]) });
      }
      const u = db.prepare('select max(updated_at) as updated_at from pk_pack_material_rules').get();
      meta.packing = { file: packingDb, mtime: fs.statSync(packingDb).mtime.toISOString(), active_rules: n, single_sku: packing.size, updated_at: u.updated_at };
    } finally { db.close(); }
  }
  return { postage, packing, meta };
}

/** 梱包アプリのキー (apps/packing/materials.js の encodeSku は前後の空白を除いて小文字にしてから %XX にする) */
export const packingSkuKey = (v) => String(v ?? '').trim().toLowerCase();

/** 1 つの SKU を見る = { ok: true, tag, to } か { ok: false, why, tag } (印が無い = null) */
function judge(row, { evidence, gate }) {
  const t = gate === 'none' ? stripAllTags(row.name) : stripTag(row.name);
  if (!t) return null;
  const base = { tag: t.tag, to: t.to };
  if (row.sku_kind === 'set') return { ...base, ok: false, why: 'kind_set' };
  if (row.sku_kind !== 'single') return { ...base, ok: false, why: 'kind_exception' };
  // 🆕 --gate none (中原さん 10/23): 資材の登録を見ない・印の重なりも全部外す・登録をやめた商品 (cancelled) のほかは外す (要確認も)
  if (gate === 'none') {
    if (row.reg_state === 'cancelled') return { ...base, ok: false, why: 'reg_state' };
    if ([...t.to].length < 2) return { ...base, ok: false, why: 'name_short' };
    if (/^empty$/i.test(t.to) || CTRL_RE.test(t.to) || normSku(t.to) === row.code_norm) return { ...base, ok: false, why: 'name_invalid' };
    return { ...base, ok: true };
  }
  if (row.reg_state !== 'available') return { ...base, ok: false, why: 'reg_state' };
  if (MATERIAL_TAG_RE.test(t.to)) return { ...base, ok: false, why: 'double_tag' };
  if ([...t.to].length < 2) return { ...base, ok: false, why: 'name_short' };
  if (/^empty$/i.test(t.to) || CTRL_RE.test(t.to) || normSku(t.to) === row.code_norm) return { ...base, ok: false, why: 'name_invalid' };
  // 郵便料金: 名前の印から資材を引く道がある印 (materialFromName) は、登録があり、引ける資材と同じときだけ
  const fromName = materialFromName(row.name);
  const pm = evidence.postage.get(postageSkuKey(row.code)) || null;
  const pk = evidence.packing ? (evidence.packing.get(packingSkuKey(row.code)) || []) : [];
  const ev = { postage_from_name: fromName, postage: pm ? pm.material : null, postage_source: pm ? pm.source : null, packing: pk.map((x) => `${x.delivery}:${x.material}`).sort() };
  if (fromName) {
    if (!pm || !pm.material) return { ...base, ...ev, ok: false, why: 'no_postage_material' };
    if (pm.material !== fromName) return { ...base, ...ev, ok: false, why: 'postage_mismatch' };
  }
  // 梱包アプリ: --gate both は全部・--gate postage でも郵便料金が印を見ない印 (_K-44 など) は梱包アプリの登録が要る (どこにも登録が無いまま外さない)
  if ((gate === 'both' || !fromName) && !pk.length) return { ...base, ...ev, ok: false, why: 'no_packing_rule' };
  return { ...base, ...ev, ok: true };
}

/**
 * 計画を作る (読むだけ・呼び手の取引の中で)。opts = { actor, reason, gate, evidence }
 * 戻り値 = { hash, writes (書くもの全部 = pending + done), pending, done, skipped, counts, ne }
 */
export async function buildStripPlan(db, { actor, reason, gate = 'both', evidence }) {
  const rows = (await db.query(`select s.sku_id::text as sku_id, s.code, s.code_norm, s.sku_kind, s.name, s.handling, r.state as reg_state
      from core.skus s left join ops.master_registrations r on r.sku_id = s.sku_id
     where s.company_id = 1 order by s.sku_id`)).rows;
  const byId = new Map(rows.map((r) => [r.sku_id, r]));
  // 済んだもの = この道具の request_id の保存の記録 (done・名前だけの直し) で、今の名前 = 後の名前。
  //   SKU ごとに新しい記録から見る = 印を付け直されてもう一度外した商品は、最後に外したときの前の名前 (Codex R2 Medium)
  const doneRows = (await db.query(`select r.request_id::text as request_id, r.sku_id::text as sku_id,
        r.result -> 'changed' -> 0 ->> 'from' as from_name, r.result -> 'changed' -> 0 ->> 'to' as to_name
      from ops.master_edit_requests r
     where r.company_id = 1 and r.operation = 'sku_edit' and r.status = 'done' and r.sku_id is not null
       and jsonb_typeof(r.result -> 'changed') = 'array' and jsonb_array_length(r.result -> 'changed') = 1
       and r.result -> 'changed' -> 0 ->> 'field' = 'name'
     order by r.sku_id, r.started_at desc, r.request_id desc`)).rows;
  const done = [];
  const doneIds = new Set();
  const seenSku = new Set();
  for (const d of doneRows) {
    const t1 = stripTag(d.from_name);
    const t2 = stripAllTags(d.from_name);
    if (!((t1 && t1.to === d.to_name) || (t2 && t2.to === d.to_name))) continue;
    let ours = false;
    for (let a = 0; a < MAX_ATTEMPTS && !ours; a++) ours = stripRequestId(d.sku_id, d.from_name, d.to_name, a) === d.request_id;
    if (!ours || seenSku.has(d.sku_id)) continue;
    seenSku.add(d.sku_id);   // この道具の最後の記録だけを見る
    const cur = byId.get(d.sku_id);
    if (!cur || cur.name !== d.to_name) continue;   // 済んだ後にまた直された = この道具の外 (計画に入れない)
    doneIds.add(d.sku_id);
    done.push({ sku_id: d.sku_id, code: cur.code, from: d.from_name, to: d.to_name, request_id: d.request_id });
  }
  // 印のある商品を見る
  const cancelled = [];
  const cands = [];
  const skipped = [];
  for (const r of rows) {
    if (doneIds.has(r.sku_id)) continue;
    const j = judge(r, { evidence, gate });
    if (!j) continue;
    if (r.reg_state === 'cancelled') { cancelled.push({ code: r.code, name: r.name, tag: j.tag }); continue; }
    if (j.ok) cands.push({ row: r, j });
    else skipped.push({ sku_id: r.sku_id, code: r.code, name: r.name, tag: j.tag, why: j.why, to: j.to, handling: r.handling,
      postage: j.postage ?? null, postage_from_name: j.postage_from_name ?? null, packing: j.packing ?? [] });
  }
  // 同じ名前: ほかの商品の今の名前 (登録をやめたものも) か、ほかの外した後の名前と同じ = 外さない
  //   外さないことにした商品は今の名前のまま = 数え直す (増えなくなるまで)
  const dup = new Set();
  for (;;) {
    if (gate === 'none') break;   // 🆕 --gate none: 同じ名前になっても外す (中原さん 10/23・名前に一意の決まりは無い)
    const keyCount = new Map();
    const bump = (k) => keyCount.set(k, (keyCount.get(k) || 0) + 1);
    const changing = new Set(cands.filter((c) => !dup.has(c.row.sku_id)).map((c) => c.row.sku_id));
    for (const r of rows) if (!changing.has(r.sku_id)) bump(nameKey(r.name));
    for (const c of cands) if (changing.has(c.row.sku_id)) bump(nameKey(c.j.to));
    const more = cands.filter((c) => changing.has(c.row.sku_id) && keyCount.get(nameKey(c.j.to)) > 1);
    if (!more.length) break;
    for (const c of more) dup.add(c.row.sku_id);
  }
  const pending = [];
  for (const c of cands) {
    if (dup.has(c.row.sku_id)) {
      skipped.push({ sku_id: c.row.sku_id, code: c.row.code, name: c.row.name, tag: c.j.tag, why: 'duplicate_name', to: c.j.to, handling: c.row.handling,
        postage: c.j.postage ?? null, postage_from_name: c.j.postage_from_name ?? null, packing: c.j.packing ?? [] });
      continue;
    }
    pending.push({ sku_id: c.row.sku_id, code: c.row.code, from: c.row.name, to: c.j.to, tag: c.j.tag, handling: c.row.handling,
      postage: c.j.postage ?? null, packing: c.j.packing ?? [] });
  }
  const bySku = (a, b) => (BigInt(a.sku_id) < BigInt(b.sku_id) ? -1 : BigInt(a.sku_id) > BigInt(b.sku_id) ? 1 : 0);
  pending.sort(bySku); done.sort(bySku); skipped.sort(bySku);
  const writes = [...pending.map((x) => ({ ...x, state: 'pending' })), ...done.map((x) => ({ ...x, state: 'done' }))].sort(bySku);
  const hash = sha256(stable({ v: PLAN_VERSION, tool: TOOL, actor, reason, gate, writes: writes.map((x) => [x.sku_id, x.code, x.from, x.to]) }));
  // 数
  const tagged = pending.length + skipped.length;
  const byReason = {};
  const byTag = {};
  for (const s of skipped) {
    byReason[s.why] = (byReason[s.why] || 0) + 1;
    const t = byTag[s.tag] ||= { tagged: 0, strip: 0, skipped: 0 };
    t.tagged++; t.skipped++;
  }
  for (const p of pending) { const t = byTag[p.tag] ||= { tagged: 0, strip: 0, skipped: 0 }; t.tagged++; t.strip++; }
  // (参考) --gate postage なら外せる数 = 郵便料金が印から資材を引く印で、梱包アプリの登録だけが無いもの (同じ名前の確かめの前)
  const postageOnly = pending.length + (gate === 'both' ? skipped.filter((s) => s.why === 'no_packing_rule' && s.postage_from_name).length : 0);
  const counts = {
    tagged_non_cancelled: tagged, cancelled: cancelled.length, strippable: pending.length, done: done.length, skipped: skipped.length,
    skipped_no_material: (byReason.no_postage_material || 0) + (byReason.no_packing_rule || 0), skipped_mismatch: byReason.postage_mismatch || 0,
    skipped_other: skipped.length - (byReason.no_postage_material || 0) - (byReason.no_packing_rule || 0) - (byReason.postage_mismatch || 0),
    by_reason: byReason, by_tag: byTag, if_gate_postage: postageOnly,
  };
  const neRows = pending.length + done.length;   // 単品だけ (セットは外さない)
  return { hash, writes, pending, done, skipped, cancelled, counts,
    ne: { product_rows: neRows, files: Math.ceil(neRows / NE_FILE_ROWS), auto_limit: NE_AUTO_LIMIT, file_rows: NE_FILE_ROWS } };
}

/** 計画の表 (dry-run と apply の初めに出す) */
export function planLines(plan, { show = 20, evidence = null, gate = 'both' } = {}) {
  const c = plan.counts;
  const L = [];
  if (evidence?.meta?.postage) {
    const m = evidence.meta.postage;
    L.push(`郵便料金 (postage.db): 商品 ${m.rows} 件・資材の登録あり ${m.with_material} 件・最後の更新 ${m.updated_at ?? '-'}・ファイル ${m.file} (${m.mtime})`);
  }
  if (evidence?.meta?.packing) {
    const m = evidence.meta.packing;
    L.push(`梱包アプリ (picking.db): 有効な登録 ${m.active_rules} 件・この商品だけの伝票の登録がある SKU ${m.single_sku} 件・最後の更新 ${m.updated_at ?? '-'}・ファイル ${m.file} (${m.mtime})`);
  }
  L.push(`条件 (--gate): ${gate === 'both' ? '郵便料金 + 梱包アプリ' : gate === 'none' ? '資材の登録を見ない (全部外す・印の重なりも・同じ名前になっても)' : '郵便料金だけ'}`);
  L.push(`資材の印がある商品 (登録をやめたものを除く): ${c.tagged_non_cancelled} 件 (登録をやめた商品 ${c.cancelled} 件は数えるだけ)`);
  L.push(`  外す: ${c.strippable} 件${c.done ? ` (この道具で済んだもの ${c.done} 件は別)` : ''}`);
  L.push(`  外さない: ${c.skipped} 件 = 資材の登録が無い ${c.skipped_no_material}・登録と印が違う ${c.skipped_mismatch}・そのほか ${c.skipped_other}`);
  for (const [why, n] of Object.entries(c.by_reason).sort((a, b) => b[1] - a[1])) L.push(`    ${why} ${n} 件: ${SKIP_WORDS[why] || why}`);
  if (gate === 'both') L.push(`  (参考・おおよそ) 郵便料金だけを条件にすると (--gate postage) 外すのは ${c.if_gate_postage} 件 (同じ名前の確かめの前)`);
  L.push('  印ごと (印がある / 外す / 外さない):');
  for (const [tag, t] of Object.entries(c.by_tag).sort((a, b) => b[1].tagged - a[1].tagged)) L.push(`    _${tag}: ${t.tagged} / ${t.strip} / ${t.skipped}`);
  if (plan.pending.length) {
    L.push(`  外す例 (最初の ${Math.min(show, plan.pending.length)} 件):`);
    for (const p of plan.pending.slice(0, show)) L.push(`    ${p.code}: 「${p.from}」→「${p.to}」`);
  }
  const groups = {};
  for (const s of plan.skipped) (groups[s.why] ||= []).push(s);
  for (const [why, list] of Object.entries(groups)) {
    L.push(`  外さない ${why} (最初の ${Math.min(show, list.length)} 件 / ${list.length}):`);
    for (const s of list.slice(0, show)) L.push(`    ${s.code} _${s.tag}: ${s.name}${s.postage ? ` [郵便料金 ${s.postage}]` : ''}${s.packing && s.packing.length ? ` [梱包 ${s.packing.length}]` : ''}`);
  }
  L.push(`NE へ送る (0094 / 0096): 単品の名前 ${plan.ne.product_rows} 行 = CSV ${plan.ne.files} ファイル (1 ファイル ${plan.ne.file_rows} 行まで・ほかの直しも同じファイルに入る)。` +
    `毎朝の自動は ${plan.ne.auto_limit} 欄を超えると止まる (held) = 画面「NEへ送る」の「CSV で受け取る」か「今すぐ送る」で`);
  return L;
}

/** 外さない一覧の CSV (UTF-8 BOM・CRLF) */
export function skippedCsv(plan) {
  const q = (v) => { const s = String(v ?? ''); return /[",\r\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s; };
  const head = ['コード', '名前', '印', '外さない理由', '理由の説明', '取扱', '郵便料金の資材の登録', '名前の印から引ける郵便料金の資材', '梱包アプリの登録 (配送:資材)'];
  const lines = [head.join(',')];
  for (const s of plan.skipped) lines.push([s.code, s.name, s.tag, s.why, SKIP_WORDS[s.why] || '', s.handling, s.postage, s.postage_from_name, (s.packing || []).join(' ')].map(q).join(','));
  return `\uFEFF${lines.join('\r\n')}\r\n`;
}

/** 今つないでいるロール (変更の記録の db_user と同じ決め方 = SET ROLE の役 か ログインした役) */
export async function currentDbUser(db) {
  return (await db.query(`select case when coalesce(current_setting('role', true), 'none') <> 'none' then current_setting('role', true) else session_user::text end as u`)).rows[0].u;
}

/**
 * 書く。opts = { actor, reason, gate, evidence, expectHash, limit, chunk, now (試験), log, save (試験で差し替え) }
 * 戻り値 = { ok, stopped, hash, wrote, already, skipped: [...], failed: [...], plan }
 */
export async function applyStrip(db, opts) {
  const log = opts.log || console.log;
  const save = opts.save || saveSku;
  if (opts.open !== true) {
    return { ok: false, refused: 'not_open', message: '保存を開くスイッチ (MASTER_EDIT_OPEN = 1) が入っていない。画面と同じく、Render の MASTER_EDIT_OPEN が 1 のときだけ、その PowerShell で $env:MASTER_EDIT_OPEN を 1 にして流す' };
  }
  const role = await currentDbUser(db);
  if (role !== 'master_edit') {
    return { ok: false, refused: 'role', message: `画面のロール master_edit でつないでいない (今 = ${role})。DB の持ち主のロールでは書かない (画面の守りを通らない)` };
  }
  await db.query('begin read only');
  let plan;
  try { plan = await buildStripPlan(db, opts); } finally { try { await db.query('rollback'); } catch { /* */ } }
  if (plan.hash !== opts.expectHash) {
    return { ok: false, refused: 'hash', plan, message: `計画のハッシュが dry-run と違う (今 ${plan.hash})。印のある商品・資材の登録・人・理由・条件のどれかが変わった = もう一度 dry-run から` };
  }
  let todo = plan.pending;
  if (opts.limit != null) todo = todo.slice(0, opts.limit);
  const chunk = opts.chunk || CHUNK_DEFAULT;
  const out = { ok: true, stopped: null, hash: plan.hash, wrote: [], already: [], skipped: [], failed: [], plan };
  const today = () => jstDate(opts.now || new Date());
  const nChunks = Math.ceil(todo.length / chunk);
  log(`書く: ${todo.length} 件 (計画の外す ${plan.pending.length} 件・済んだもの ${plan.done.length} 件)・${chunk} 件ずつ ${nChunks} 回`);
  for (let ci = 0; ci < nChunks && !out.stopped; ci++) {
    const part = todo.slice(ci * chunk, (ci + 1) * chunk);
    for (const it of part) {
      const r = await writeOne(db, it, { ...opts, save, today: today() });
      if (r.state === 'wrote') out.wrote.push({ code: it.code, request_id: r.request_id });
      else if (r.state === 'already') out.already.push({ code: it.code });
      else if (r.state === 'skipped') out.skipped.push({ code: it.code, why: r.why, name: r.name });
      else if (r.state === 'failed') out.failed.push({ code: it.code, reason: r.reason, message: r.message, request_id: r.request_id });
      else { out.stopped = { code: it.code, reason: r.reason, message: r.message, request_id: r.request_id }; out.ok = false; break; }
    }
    log(`  [${ci + 1}/${nChunks}] 書いた 累計 ${out.wrote.length}・済んでいた ${out.already.length}・飛ばした ${out.skipped.length}・だめ ${out.failed.length}`);
  }
  if (out.failed.length || out.skipped.length) out.ok = false;
  return out;
}

/** 1 件書く。state = wrote / already / skipped / failed (その商品だけ) / stop (止める) */
async function writeOne(db, it, opts) {
  let cur;
  try { cur = await readCurrent(db, it.sku_id, opts.today); } catch (e) { return { state: 'stop', reason: 'read_failed', message: cleanMsg(e) }; }
  if (!cur) return { state: 'skipped', why: 'not_found' };
  if (cur.name === it.to) return { state: 'already' };
  if (cur.name !== it.from || cur.sku_kind !== 'single'
      || (opts.gate === 'none' ? cur.registration?.state === 'cancelled' : cur.registration?.state !== 'available')) return { state: 'skipped', why: 'changed_since_plan', name: cur.name };
  // やり直しの回 = 記録の無い最初の回 (前の回の失敗・印を付け直される前に済んだ回の番号は使わない。済んだかは今の名前で見る)
  const ids = Array.from({ length: MAX_ATTEMPTS }, (_, a) => stripRequestId(it.sku_id, it.from, it.to, a));
  const prev = (await db.query('select request_id::text as request_id, status from ops.master_edit_requests where request_id = any($1::uuid[])', [ids])).rows;
  const used = new Set(prev.map((p) => p.request_id));
  const nextAttempt = () => ids.findIndex((id) => !used.has(id));
  let attempt = nextAttempt();
  for (let tries = 0; tries < 2; tries++) {
    if (attempt < 0 || attempt >= MAX_ATTEMPTS) return { state: 'failed', reason: 'too_many_attempts', message: `この直しの記録がもう ${MAX_ATTEMPTS} 回ある (人が見る)` };
    const requestId = ids[attempt];
    if (tries > 0) {
      try { cur = await readCurrent(db, it.sku_id, opts.today); } catch (e) { return { state: 'stop', reason: 'read_failed', message: cleanMsg(e) }; }
      if (cur && cur.name === it.to) return { state: 'already' };
      if (!cur || cur.name !== it.from) return { state: 'skipped', why: 'changed_since_plan', name: cur ? cur.name : null };
    }
    try {
      const res = await opts.save(db, { actor: opts.actor, requestId, code: cur.code, reason: opts.reason, seen: { token: editTokenOf(cur), event_id: null }, values: { name: it.to } },
        { open: opts.open === true, now: opts.now });
      const ch = res.changed || [];
      if (!(res.ok && (res.no_change || (ch.length === 1 && ch[0].field === 'name' && ch[0].to === it.to)))) {
        return { state: 'stop', reason: 'unexpected_result', message: JSON.stringify(res).slice(0, 300), request_id: requestId };
      }
      return { state: res.no_change ? 'already' : 'wrote', request_id: requestId };
    } catch (e) {
      if (e instanceof MasterWriteError) {
        if (e.reason === 'version_conflict' && tries === 0) { used.add(requestId); attempt = nextAttempt(); continue; }   // 読み直して次の回で 1 回だけ
        if (ITEM_REASONS.has(e.reason)) return { state: 'failed', reason: e.reason, message: e.message, request_id: requestId };
        return { state: 'stop', reason: e.reason, message: e.message, request_id: requestId };
      }
      return { state: 'stop', reason: e && e.code ? String(e.code) : 'error', message: cleanMsg(e), request_id: requestId };
    }
  }
  return { state: 'failed', reason: 'version_conflict', message: '2 回とも画面を開いた後に変わった' };
}

/** dry-run か apply を流す (main と試験の入口)。戻り値 = { mode, exitCode, plan?, result? } */
export async function runStrip(db, opts, { log = console.log, now = null, save = null } = {}) {
  const evidence = opts.evidence || (opts.gate === 'none'
    ? { postage: new Map(), packing: null, meta: { gate: 'none' } }   // 🆕 --gate none = 資材の DB を読まない
    : readMaterialEvidence({ postageDb: opts.postageDb, packingDb: opts.gate === 'both' ? opts.packingDb : null }));
  const base = { actor: opts.actor, reason: opts.reason, gate: opts.gate, evidence };
  if (!opts.apply) {
    await db.query('begin read only');
    let plan;
    try { plan = await buildStripPlan(db, base); } finally { try { await db.query('rollback'); } catch { /* */ } }
    log('dry-run (読むだけ・何も書かない): 商品名の末尾の資材の印を外す');
    for (const l of planLines(plan, { show: opts.show, evidence, gate: opts.gate })) log(l);
    log(`計画のハッシュ (--apply に --expect-hash で渡す・同じ --actor / --reason / --gate・同じ資材の DB で): ${plan.hash}`);
    return { mode: 'dry-run', exitCode: 0, plan };
  }
  const open = opts.open ?? process.env.MASTER_EDIT_OPEN === '1';
  const r = await applyStrip(db, { ...base, expectHash: opts.expectHash, limit: opts.limit, chunk: opts.chunk, open, now, log, save });
  if (r.refused) { log(`⛔ 書かない: ${r.message}`); return { mode: 'apply', exitCode: r.refused === 'hash' ? 1 : 2, result: r, plan: r.plan }; }
  log(`結果: 書いた ${r.wrote.length}・済んでいた ${r.already.length}・飛ばした ${r.skipped.length}・だめ ${r.failed.length}`);
  for (const s of r.skipped) log(`  飛ばした ${s.code}: ${s.why}${s.name ? ` (今の名前「${s.name}」)` : ''}`);
  for (const f of r.failed) log(`  だめ ${f.code}: ${f.reason} ${f.message}`);
  if (r.stopped) log(`⛔ 止めた (${r.stopped.code}: ${r.stopped.reason} ${r.stopped.message})。同じ --expect-hash でもう一度流せば残りから続ける (書けたものは書かない)`);
  else if (r.wrote.length + r.already.length < r.plan.pending.length) log(`残り ${r.plan.pending.length - r.wrote.length - r.already.length - r.skipped.length - r.failed.length} 件 (同じ --expect-hash でもう一度流せば続き)`);
  return { mode: 'apply', exitCode: r.ok ? 0 : 1, result: r, plan: r.plan };
}

async function main() {
  let opts;
  try { opts = parseStripArgs(process.argv.slice(2)); } catch (e) { console.error(`❌ ${e.message}`); process.exitCode = 2; return; }
  const url = opts.url || process.env.COMPANY_DB_MASTER_EDIT_URL || (opts.apply ? null : process.env.COMPANY_DB_WATCH_URL);
  if (!url) { console.error(`❌ ${opts.apply ? 'COMPANY_DB_MASTER_EDIT_URL (画面のロール master_edit)' : 'COMPANY_DB_MASTER_EDIT_URL か COMPANY_DB_WATCH_URL'} か --url が要る`); process.exitCode = 2; return; }
  let client = null;
  try {
    client = await openPgClient(url, { application_name: TOOL });
    client.on('error', () => {});
    await client.query(`set statement_timeout = '60s'`);
    await client.query(`set lock_timeout = '10s'`);
    await client.query(`set idle_in_transaction_session_timeout = '120s'`);
    const r = await runStrip(pgAdapter(client), opts);
    if (opts.json && r.plan) {
      fs.writeFileSync(opts.json, JSON.stringify({ mode: r.mode, hash: r.plan.hash, counts: r.plan.counts, ne: r.plan.ne, pending: r.plan.pending, done: r.plan.done,
        skipped: r.plan.skipped, cancelled: r.plan.cancelled, result: r.result ? { wrote: r.result.wrote, already: r.result.already, skipped: r.result.skipped, failed: r.result.failed, stopped: r.result.stopped } : null }, null, 2));
    }
    if (opts.csv && r.plan) fs.writeFileSync(opts.csv, skippedCsv(r.plan));
    process.exitCode = r.exitCode;
  } catch (e) {
    console.error(`❌ ${cleanMsg(e)}`);
    process.exitCode = e && e.code === 'STRIP_ARGS' ? 2 : 1;
  } finally {
    if (client) { try { await client.end(); } catch { /* */ } }
  }
}

const isMain = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) main();
