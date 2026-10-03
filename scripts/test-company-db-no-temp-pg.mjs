#!/usr/bin/env node
/**
 * test-company-db-no-temp-pg.mjs — 0057 (D-60 PR 1b-0 の一部 = 一時の表を使わない relink_shipments_bulk / merge_duplicate_suppliers) を本物の PostgreSQL で比べる
 *
 * 固定する契約:
 *   1 旧い関数 (0017 / 0027 の版 = 0056 まで流した DB) と新しい関数 (0057 の後) に同じ fixture を入れ、同じ順で呼ぶと、
 *     戻り値 (例外なら SQLSTATE と文言) と、全部の表の中身 (ops.schema_migrations を除く)・全部の通し番号の値が 1 行も違わない。
 *     境目 = 候補 0 件・p_limit ちょうど / ±1・p_limit の上限 100,000 (+1 は例外)・候補 100,001 件・p_after = null / 先の番号・同じ取引で続けて呼ぶ・
 *     仕入先の二重の連鎖 ('0001' / '1' / '01')・会社をまたぐ同じコード・NULL の列・代表の印・文書の紐付けの重なり・2 回目は何もしない。乱数の fixture を数回
 *     (now() は同じ取引の中で 1 つの値 = '<now>' に置き換える。uuid は表の中で初めて出た順の番号に置き換える)
 *     🚨 1 つの文の中で行を処理する順は契約ではない (旧い関数でも実行計画しだい) = 順で決まる値だけは順に依らない形で比べる:
 *        version の列 = fixture の後に上げた値は 'bumped' にして行ごと (どの行を上げたか。値そのものは行の順で変わる) + 通し番号の最後の値 / 監査 (events.master_change_events) = change_id のまとまり (中の出来事の順はそのまま) の集まり +
 *        event_id の順の「操作|対象」(同じ対象の UPDATE と INSERT が続く所だけは並べ替えて比べる = 新は直すと足すが 1 つの MERGE = 行の順に混ざりうる) + event_id の集まり
 *   2 新しい関数は一時の表を作らない = 呼んだ取引の中で pg_class の relpersistence = 't' が 0 (旧い関数は 0 より多い = 確かめ方が効いている)・
 *     TEMP の権限を外した DB で呼んで通る (旧い関数は 42501)・pg_get_functiondef に一時の表を作る文が無い・tempPrivilegeAudit の一時の表を作る関数は reresolve_order_lines だけ
 *   3 新しい merge_duplicate_suppliers は仕入先 5,000 行までは旧いのと同じ・5,001 行で 54000 (何も変えない)。最大のまとまりの幅 (仕入先 約 5,000 が同じ商品・同じ文書) も旧と同じ
 *   4 0057 は 2 回流しても同じ・署名・戻り値の型・持ち主・EXECUTE の権限の表は変わらない・search_path は pg_catalog, pg_temp・hash join を強く選ばせる設定
 *     (enable_nestloop = off・enable_mergejoin = off。off は完全な禁止ではない)・work_mem = 4MB・hash_mem_multiplier = 2・関数の中で発火する trigger の一覧 (設定が効く範囲) が決まった形・
 *     本文に恒真の条件 ((select count(*) from …) >= 0) と array_agg(…)[1] が無い
 *   5 同時に動くとき (2 接続。待つ側が lock を待っていることを pg_blocking_pids で確かめてから相手を commit): 戻り値・SQLSTATE (lock_timeout・23503)・最後の表が旧と同じ。
 *     deadlock は何回か回して、どちらが 40P01 になったかの数と最後の表を旧と新で比べる。旧と違うのは 4 つだけ (説明つきで固定):
 *     relink ① 文 1 (lock) の後に commit されて範囲に入った未結合の伝票も結ぶ ② 店舗 (ne_shops) は文 2 (結ぶ文) の版 /
 *     merge ③ 待った相手が直した寄せる行の値を (直す前でなく) 直した後の値で移す ④ 文の snapshot の後に相手が残す行に同じ商品 / 文書の行を足して commit → 23505 を
 *     その文だけ巻き戻して新しい snapshot でやり直す (相手の行を残す行として まとめる。旧は相手の値を寄せる行の値で上書き)
 * 使い方: node scripts/test-company-db-no-temp-pg.mjs   (npm run test:company-db にも入っている)
 *   🚨 試験が自分で使い捨てのクラスタを起動する (test-company-db-profit-fn-revoke-pg.mjs と同じ = embedded-postgres・OS の一時フォルダ・ランダムのポート・最後に止めて消す)。
 *      外の PostgreSQL には一切つながない。見つからない・版が違う・起動できない・フォルダが消えない = 失敗 (exit 1)
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { createRequire } from 'node:module';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { openPgClient, pgAdapter, applyMigrations, DEFAULT_DIR } from './company-db/migrate.mjs';
import { tempPrivilegeAudit } from './company-db/heavy-entry-manifest.mjs';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const PINNED_EMBEDDED_PG = JSON.parse(fs.readFileSync(path.join(ROOT, 'package.json'), 'utf8')).devDependencies['embedded-postgres'];
async function loadEmbeddedPostgres() {
  const bases = [path.join(ROOT, 'package.json'), ...(process.env.EMBEDDED_PG_DIR ? [path.join(process.env.EMBEDDED_PG_DIR, 'package.json')] : []), 'C:/tmp/pg-embed/package.json'];
  const seen = [];
  for (const b of bases) {
    let main, ver;
    try { const req = createRequire(b); main = req.resolve('embedded-postgres'); let d = path.dirname(main); while (path.basename(d) !== 'embedded-postgres' && path.dirname(d) !== d) d = path.dirname(d); ver = JSON.parse(fs.readFileSync(path.join(d, 'package.json'), 'utf8')).version; } catch { continue; }
    if (ver !== PINNED_EMBEDDED_PG) { seen.push(path.dirname(b) + ' = ' + ver); continue; }
    return { EmbeddedPostgres: (await import(pathToFileURL(main).href)).default, from: path.dirname(b) };
  }
  return { why: seen.length ? '版が ' + PINNED_EMBEDDED_PG + ' でない (' + seen.join(' / ') + ')' : '見つからない' };
}
const loaded = await loadEmbeddedPostgres();
if (!loaded.EmbeddedPostgres) {
  console.error('❌ embedded-postgres ' + PINNED_EMBEDDED_PG + ' が' + loaded.why + ' = 本物の PostgreSQL の比較の試験を流せない (飛ばさない)。リポジトリで npm ci');
  process.exit(1);
}
const clusterDir = path.join(os.tmpdir(), `cdb-notemp-pg-${crypto.randomBytes(4).toString('hex')}`);
const SU_PW = `su_${crypto.randomBytes(12).toString('hex')}`;
const port = 55000 + crypto.randomInt(4000);
const cluster = new loaded.EmbeddedPostgres({ databaseDir: clusterDir, user: 'postgres', password: SU_PW, port, persistent: false, onLog: () => {}, onError: () => {} });
let cleanupFailed = false;
const stopCluster = async () => {
  try { await cluster.stop(); } catch (e) { console.error('使い捨てのクラスタを止めるときの誤り: ' + e.message); }
  for (let i = 0; i < 10 && fs.existsSync(clusterDir); i++) { try { fs.rmSync(clusterDir, { recursive: true, force: true }); } catch { await new Promise((r) => setTimeout(r, 500)); } }
  if (fs.existsSync(clusterDir)) { cleanupFailed = true; console.error('❌ 使い捨てのクラスタのフォルダが消えない: ' + clusterDir); }
};
const url = `postgres://postgres:${SU_PW}@127.0.0.1:${port}/postgres`;
console.log('使い捨てのクラスタ: embedded-postgres ' + PINNED_EMBEDDED_PG + ' (' + loaded.from + ')');

let ok = 0, ng = 0;
const ONLY = process.env.D60_ONLY ? new RegExp(process.env.D60_ONLY) : null;   // 開発用の絞り込み (npm の試験では使わない)
const t = async (name, fn) => { if (ONLY && !ONLY.test(name)) return; try { await fn(); ok++; console.log('  ok  ' + name); } catch (e) { ng++; console.log('  NG  ' + name + '\n      ' + (e.stack || e.message || e)); } };
const quiet = () => {};
const hex = crypto.randomBytes(4).toString('hex');
const OWNER = `cdb_nt_o_${hex}`, PW = `t_${crypto.randomBytes(12).toString('hex')}`;
const T_OLD = `cdb_nt_old_${hex}`, T_NEW = `cdb_nt_new_${hex}`;
const SQL_0057 = fs.readFileSync(path.join(DEFAULT_DIR, '0057_no_temp_relink_merge.sql'), 'utf8');
const SIG_RELINK = 'core.relink_shipments_bulk(smallint, bigint, integer)';
const SIG_MERGE = 'core.merge_duplicate_suppliers()';
const TEMP_DDL = /create\s+(local\s+|global\s+)?temp(orary)?\s+table/i;

// ─── 決まった乱数 (mulberry32) ───
const rng = (seed) => { let a = seed >>> 0; return () => { a = (a + 0x6d2b79f5) >>> 0; let x = a; x = Math.imul(x ^ (x >>> 15), x | 1); x ^= x + Math.imul(x ^ (x >>> 7), x | 61); return ((x ^ (x >>> 14)) >>> 0) / 4294967296; }; };
const lit = (v) => (v == null ? 'null' : typeof v === 'number' || typeof v === 'boolean' ? String(v) : `'${String(v).replace(/'/g, "''")}'`);

const clients = [];
let setupError = null;
let su = null;
try {
  await cluster.initialise();
  await cluster.start();
  su = await openPgClient(url);
  su.on('error', () => {});
  await su.query(`create role ${OWNER} login createrole password '${PW}'`);   // Render の default user と同じ: superuser でない・CREATEROLE
  await su.query(`create role watcher login password '${PW}'`);
  await su.query(`create database ${T_OLD} owner ${OWNER}`);
  const open = async (dbName, role = OWNER) => { const x = new URL(url); x.username = role; x.password = PW; x.pathname = `/${dbName}`; const c = await openPgClient(x.toString()); c.on('error', () => {}); clients.push(c); return c; };
  const close = async (c) => { const i = clients.indexOf(c); if (i >= 0) clients.splice(i, 1); try { await c.end(); } catch { /* */ } };

  // 雛形 = 0056 まで (旧い関数) / 0056 まで + 0057 (新しい関数)。比べる回ごとに雛形から DB を作る = 通し番号の値も同じところから始まる
  {
    const c = await open(T_OLD);
    const r = await applyMigrations(pgAdapter(c), { log: quiet, to: '0056' });
    assert.ok(r.applied.includes('0056') && !r.applied.includes('0057'));
    await close(c);
  }
  await su.query(`create database ${T_NEW} template ${T_OLD} owner ${OWNER}`);
  let aclBefore;
  {
    const c = await open(T_NEW);
    aclBefore = (await c.query(`select p.oid::regprocedure::text as sig, p.proacl::text as acl, pg_get_userbyid(p.proowner) as owner, pg_get_function_result(p.oid) as res
      from pg_proc p where p.oid in ($1::regprocedure, $2::regprocedure) order by 1`, [SIG_RELINK, SIG_MERGE])).rows;
    const r = await applyMigrations(pgAdapter(c), { log: quiet });
    assert.deepEqual(r.applied, ['0057']);
    await close(c);
  }

  let seq = 0;
  /** 雛形から DB を作り、fixture → 呼ぶ (1 つの取引・最後は rollback) → 戻り値・表の中身・一時の表の数を返す */
  const runCase = async (tmpl, fixtureSql, calls, { noTemp = false } = {}) => {
    const name = `cdb_nt_case_${hex}_${++seq}`;
    await su.query(`create database ${name} template ${tmpl} owner ${OWNER}`);
    if (noTemp) await su.query(`revoke temporary on database ${name} from public, ${OWNER}`);
    const c = await open(name);
    try {
      if (noTemp) assert.equal((await c.query(`select has_database_privilege(current_user, current_database(), 'TEMP') as x`)).rows[0].x, false);
      await c.query('begin');
      await c.query(`set local statement_timeout = '300s'`);
      if (fixtureSql) await c.query(fixtureSql);
      const versionSince = Number((await c.query(`select case when is_called then last_value else 0 end as v from core.master_version_seq`)).rows[0].v);
      const results = [];
      const ms = [];
      let tempRels = 0;
      for (const sql of calls) {
        await c.query('savepoint s');
        try {
          const t0 = performance.now();
          const rows = (await c.query(`select to_jsonb(r)::text as j from (${sql}) r`)).rows.map((x) => x.j);
          ms.push(Math.round(performance.now() - t0));
          tempRels = Math.max(tempRels, Number((await c.query(`select count(*) as n from pg_class where relpersistence = 't'`)).rows[0].n));
          await c.query('release savepoint s');
          results.push({ ok: rows });
        } catch (e) {
          await c.query('rollback to savepoint s');
          results.push({ err: `${e.code} ${e.message}` });
        }
      }
      const snap = await snapshot(c, { versionSince });
      await c.query('rollback');
      return { results, snap, tempRels, ms };
    } finally {
      await close(c);
      await su.query(`drop database ${name}`);
    }
  };
  /** 全部の表 (分割の子は親で読む・ops.schema_migrations を除く) と通し番号。now() は '<now>'、uuid は表の中で初めて出た順の番号に */
  const snapshot = async (c, { recentSince = null, versionSince = null } = {}) => {
    const now = (await c.query(`select to_jsonb(now())::text as n`)).rows[0].n;
    // 同時の試験 = 取引が複数 (fixture・A・B) で now() が違う → この回が始まった後の時刻は全部 '<now>' (決まった時刻の fixture (2026-03-04) はそのまま)
    const recent = (j) => (recentSince == null ? j : j.replace(/"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?[+-]\d\d:\d\d)"/g, (m, iso) => (Date.parse(iso) >= recentSince ? '"<now>"' : m)));
    const tables = (await c.query(`select c.oid::regclass::text as t,
        exists (select 1 from pg_attribute a where a.attrelid = c.oid and a.attname = 'version' and not a.attisdropped) as has_version,
        (select string_agg(quote_ident(a.attname), ', ' order by k.i) from pg_index x cross join unnest(x.indkey) with ordinality k(attnum, i)
           join pg_attribute a on a.attrelid = x.indrelid and a.attnum = k.attnum where x.indrelid = c.oid and x.indisprimary) as pk
      from pg_class c join pg_namespace n on n.oid = c.relnamespace
     where c.relkind in ('r', 'p') and not c.relispartition and n.nspname not in ('pg_catalog', 'information_schema') and n.nspname !~ '^pg_'
       and c.oid <> 'ops.schema_migrations'::regclass order by 1`)).rows;
    const out = {};
    for (const { t: tbl, pk, has_version: hasVersion } of tables) {
      const raw = (await c.query(`select to_jsonb(x)::text as j from ${tbl} x order by ${pk || 'to_jsonb(x)::text'}`)).rows;
      const uuids = new Map();
      let rows = raw.map((r) => recent(r.j.split(now).join('"<now>"'))
        .replace(/"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"/g, (u) => { if (!uuids.has(u)) uuids.set(u, `"uuid#${uuids.size}"`); return uuids.get(u); }));
      // 🚨 1 つの文の中で行を処理する順は契約ではない (旧い関数でも実行計画しだい = 一時の表の統計・結合の形で変わる)。順に依る値だけを順に依らない形にして比べる:
      //   version (trigger が行ごとに通し番号を振る) = fixture の後に上げた値は 'bumped' に置き換えて行ごとに比べる (= どの行を上げたかは同じ)。
      //     上げた値そのものは文の中の行の順で変わる (足す行は version の通し番号を 2 つ進める = 直す行と足す行の順で値の集まりも変わる) = 比べない。
      //     通し番号の最後の値 ((sequences)) は比べる (= 進めた数は同じ)
      //   監査 (master_change_events) = event_id・change_id を除いた出来事を change_id のまとまりごとに比べ、event_id の順の「操作|対象」の並び (= 文の順) と event_id の集まりを別に比べる
      if (tbl === 'events.master_change_events') {
        const objs = rows.map((j) => JSON.parse(j));
        // event_id の順の「操作|対象」= 文の順 (消す → 直す / 足す …)。同じ対象の UPDATE と INSERT が続く所 (= 新の 1 つの MERGE。旧は直す文 → 足す文) だけは
        //   並べ替える (MERGE の中で直す行と足す行の順は計画しだい = 契約ではない)。DELETE とそれ以外の境・対象の変わり目はそのまま比べる
        const seqOps = objs.map((o) => `${o.operation}|${o.entity_type}`);
        const norm = [];
        for (let i = 0; i < seqOps.length;) {
          const [op, ent] = seqOps[i].split('|');
          if (op !== 'UPDATE' && op !== 'INSERT') { norm.push(seqOps[i]); i++; continue; }
          let j = i; while (j < seqOps.length && /^(UPDATE|INSERT)\|/.test(seqOps[j]) && seqOps[j].split('|')[1] === ent) j++;
          norm.push(...seqOps.slice(i, j).sort()); i = j;
        }
        out[`${tbl} (event_id の順の 操作|対象)`] = norm;
        out[`${tbl} (event_id)`] = objs.map((o) => String(o.event_id));
        // まとまり (= 1 行の変更) の中の出来事の順 (event_id の順 = 列の名前の順) はそのまま比べる。順に依らない形にするのはまとまりどうしの並びだけ (Codex R1 Low)
        const groups = new Map();
        for (const { event_id: _e, change_id: cid, ...rest } of objs) { if (!groups.has(cid)) groups.set(cid, []); groups.get(cid).push(JSON.stringify(rest)); }
        rows = [...groups.values()].map((g) => JSON.stringify(g)).sort();
      } else if (hasVersion) {
        assert.ok(versionSince != null, 'versionSince が無い');
        rows = rows.map((j) => { const o = JSON.parse(j); return JSON.stringify({ ...o, version: Number(o.version) > versionSince ? 'bumped' : Number(o.version) }); });
      }
      out[tbl] = rows;
    }
    const seqs = (await c.query(`select schemaname || '.' || sequencename as s, last_value::text as v from pg_sequences where schemaname !~ '^pg_' and schemaname <> 'information_schema' order by 1`)).rows;
    out['(sequences)'] = seqs.map((r) => `${r.s}=${r.v}`);
    return out;
  };
  /** 旧い (0017 / 0027) と新しい (0057) で同じ結果か。違えば最初の違いを出す */
  const compare = async (label, fixtureSql, calls, opts = {}) => {
    const a = await runCase(T_OLD, fixtureSql, calls);
    const b = await runCase(T_NEW, fixtureSql, calls);
    assert.deepEqual(b.results, a.results, `${label}: 戻り値が違う`);
    const names = [...new Set([...Object.keys(a.snap), ...Object.keys(b.snap)])];
    for (const n of names) {
      const x = a.snap[n] || [], y = b.snap[n] || [];
      if (x.length !== y.length || x.some((v, i) => v !== y[i])) {
        if (process.env.D60_DEBUG) { for (let k = 0; k < Math.max(x.length, y.length); k++) if (x[k] !== y[k]) console.log(`DIFF ${n} [${label}]\n  old ${x[k]}\n  new ${y[k]}`); continue; }
        const i = x.findIndex((v, k) => v !== y[k]);
        assert.fail(`${label}: 表 ${n} が違う (旧 ${x.length} 行 / 新 ${y.length} 行・最初の違い ${i}: 旧 ${x[i]} / 新 ${y[i]})`);
      }
    }
    assert.equal(b.tempRels, 0, `${label}: 新しい関数の取引で一時の表がある`);
    if (opts.oldUsesTemp) assert.ok(a.tempRels > 0, `${label}: 旧い関数で一時の表が見えない (確かめ方が効いていない)`);
    return { a, b };
  };

  // ─── fixture: relink ───
  const SHOPS = `insert into core.ne_shops (company_id, shop_code, shop_name, platform, mall, scope_key, order_no_prefix) values
    (1, 'S1', '楽天', 'rakuten', 'rakuten', 'rk', ''), (1, 'S2', 'Yahoo', 'yahoo', 'yahoo', 'yh', 'b-faith01-'), (1, 'S3', 'FBA 納品', '_ignore', null, null, ''),
    (1, 'S4', 'Amazon', 'amazon_fbm', 'amazon', 'jp', ''), (2, 'S1', 'いろは 楽天', 'rakuten', 'rakuten', 'rk', '');\n`;
  const order = (co, mall, scope, no) => `insert into core.orders (company_id, mall, scope_key, mall_order_no, source_system, ordered_at, order_date_jst, status, received_batch_seq, source_updated_at, transform_version, content_hash)
    values (${co}, ${lit(mall)}, ${lit(scope)}, ${lit(no)}, 'ne', now(), date '2026-09-01', 'new', 1, now(), 't', 'h');\n`;
  const ship = (co, slip, neNo, shop, linkTo = null) => `insert into core.shipments (company_id, ne_slip_no, ne_order_no, shop_code, status, received_batch_seq, source_updated_at, transform_version, content_hash, order_id)
    values (${co}, ${lit(slip)}, ${lit(neNo)}, ${lit(shop)}, 'new', 1, now(), 't', 'h', ${linkTo == null ? 'null' : `(select order_id from core.orders where company_id = ${co} and mall_order_no = ${lit(linkTo)} limit 1)`});\n`;
  const relinkFixed = () => {
    let s = SHOPS;
    s += order(1, 'rakuten', 'rk', '1001') + order(1, 'rakuten', 'rk', '1002') + order(1, 'yahoo', 'yh', 'b-faith01-2001') + order(1, 'amazon', 'jp', '3001')
      + order(1, 'rakuten', 'other', '1003') + order(2, 'rakuten', 'rk', '1001') + order(1, 'yahoo', 'yh', '2002');
    s += ship(1, 'A01', '1001', 'S1') + ship(1, 'A02', '1002', 'S1') + ship(1, 'A03', '2001', 'S2') + ship(1, 'A04', '2002', 'S2')   // 2002 は接頭辞が無い注文 = 結ばれない
      + ship(1, 'A05', '3001', 'S4') + ship(1, 'A06', '1001', 'S3') + ship(1, 'A07', null, 'S1') + ship(1, 'A08', '1001', null)        // 対象外の店・番号なし・店なし
      + ship(1, 'A09', '9999', 'S1') + ship(1, 'A10', '1003', 'S1') + ship(1, 'A11', '1002', 'S1', '1002') + ship(2, 'A12', '1001', 'S1') // 注文なし・別の scope・結び済み・別の会社
      + ship(1, 'A13', '1001', 'S1') + ship(1, 'A14', '3001', 'S4');                                                                      // 同じ注文に 2 つ目の伝票
    return s;
  };
  const relinkCall = (co, after, limit) => `select * from core.relink_shipments_bulk(${co}::smallint, ${after == null ? 'null' : after}::bigint, ${limit == null ? 'null' : limit}::integer)`;
  const relinkRandom = (seed) => {
    const r = rng(seed); const pick = (a) => a[Math.floor(r() * a.length)];
    let s = SHOPS;
    const shops = [['S1', 'rakuten', 'rk', ''], ['S2', 'yahoo', 'yh', 'b-faith01-'], ['S4', 'amazon', 'jp', '']];
    const nos = Array.from({ length: 40 }, (_, i) => String(5000 + i));
    const made = [];
    for (let i = 0; i < 1 + Math.floor(r() * 60); i++) {
      const co = r() < 0.9 ? 1 : 2; const [, mall, scope, prefix] = co === 2 ? ['S1', 'rakuten', 'rk', ''] : pick(shops); const no = prefix + pick(nos);
      if (made.some((m) => m.co === co && m.mall === mall && m.scope === scope && m.no === no)) continue;
      made.push({ co, mall, scope, no }); s += order(co, mall, scope, no);
    }
    for (let i = 0; i < Math.floor(r() * 90); i++) {
      const co = r() < 0.9 ? 1 : 2;
      const shop = co === 2 ? (r() < 0.8 ? 'S1' : null) : r() < 0.08 ? null : pick(['S1', 'S2', 'S3', 'S4']);
      const neNo = r() < 0.1 ? null : pick(nos);
      const mine = made.filter((m) => m.co === co);
      const link = mine.length && r() < 0.15 ? pick(mine).no : null;
      s += ship(co, `R${seed}_${i}`, neNo, shop, link);
    }
    const calls = [];
    for (let k = 0; k < 4; k++) calls.push(relinkCall(r() < 0.9 ? 1 : 2, r() < 0.15 ? null : Math.floor(r() * 40), pick([1, 2, 3, 5, 8, 13, 20000, 100000])));
    return { s, calls };
  };

  // ─── fixture: merge ───
  const sup = (co, code, name, o = {}) => `insert into core.suppliers (company_id, code, name, order_method, lead_time_days, active, email_to, email_cc, contact_name, fax_number, relay_to, order_memo)
    values (${co}, ${lit(code)}, ${lit(name)}, ${lit(o.om)}, ${lit(o.lt)}, ${o.active === false ? 'false' : 'true'}, ${lit(o.email_to)}, ${lit(o.email_cc)}, ${lit(o.contact_name)}, ${lit(o.fax)}, ${lit(o.relay_to)}, ${lit(o.memo)});\n`;
  const sid = (co, code) => `(select supplier_id from core.suppliers where company_id = ${co} and code = ${lit(code)})`;
  const sku = (co, code) => `insert into core.skus (company_id, sku_kind, code, name) values (${co}, 'set', ${lit(code)}, ${lit(code)});\n`;
  const kid = (co, code) => `(select sku_id from core.skus where company_id = ${co} and code = ${lit(code)})`;
  /** 時刻: null = now() / 数 = 決まった時刻 + その秒数 + マイクロ秒 (時差 +09 つき = 消した行の RETURNING・CTE を通っても 1 マイクロ秒も変わらないかを見る) */
  const ts = (sec) => (sec == null ? 'now()' : `timestamptz '2026-03-04 05:06:07.123456+09' + interval '${sec} seconds' + interval '${(sec * 7919) % 1000000} microseconds'`);
  const ss = (co, scode, kcode, o = {}) => `insert into core.supplier_skus (company_id, supplier_id, sku_id, vendor_code, order_unit, stock_units_per_order_unit, min_order_qty, order_multiple, unit_cost_jpy, lead_time_days, active, is_primary, created_by_type, created_by_id, created_at)
    values (${co}, ${sid(co, scode)}, ${kid(co, kcode)}, ${lit(o.vendor)}, ${lit(o.unit)}, ${lit(o.units)}, ${lit(o.moq)}, ${lit(o.mult)}, ${lit(o.cost)}, ${lit(o.lt)}, ${o.active === false ? 'false' : 'true'}, ${o.primary ? 'true' : 'false'}, ${lit(o.by || 'system')}, ${lit(o.byId)}, ${ts(o.at)});\n`;
  const doc = (ref) => `insert into docs.documents (company_id, document_type, storage, external_ref, title) values (1, 'contract', 'url', ${lit(ref)}, ${lit(ref)});\n`;
  const link = (ref, co, scode, role, when = null) => `insert into docs.document_links (document_id, entity_type, entity_id, link_role, created_at) values ((select document_id from docs.documents where external_ref = ${lit(ref)}), 'supplier', ${sid(co, scode)}, ${lit(role)}, ${ts(when)});\n`;
  const po = (ref, co, scode) => `insert into core.purchase_orders (company_id, source_ref, supplier_id, supplier_code, supplier_name, status, source_updated_at) values (${co}, ${lit(ref)}, ${sid(co, scode)}, ${lit(scode)}, 'x', 'draft', now());\n`;
  const ext = (co, scode, value) => `insert into core.external_ids (company_id, entity_type, entity_id, system, id_kind, external_value, resolution, resolved_by_type, resolved_by_id) values (${co}, 'supplier', ${sid(co, scode)}, 'purchase_orders', 'supplier_code', ${lit(value)}, 'imported', 'system', 'test');\n`;
  const MERGE = 'select * from core.merge_duplicate_suppliers()';
  const mergeFixed = () => {
    let s = '';
    s += sup(1, '0001', '0001', { lt: 10 }) + sup(1, '1', 'アメージングクラフト様', { om: 'email', lt: 7, email_to: 'a@example.invalid', fax: '06-0000' }) + sup(1, '01', '01', { email_cc: 'c@example.invalid', contact_name: '山田', active: false })
      + sup(1, '0099', '0099', { active: false }) + sup(1, '99', '千年前の食品舎様', { om: 'email', relay_to: 'r@example.invalid', memo: 'メモ' })
      + sup(1, '900', 'トイズファン様', { om: 'web' }) + sup(1, '0900', '0900') + sup(1, '9999', 'B-Faith株式会社') + sup(1, ' 7 ', '単独の短いコード')
      + sup(1, 'abc', '数字以外') + sup(1, '12345', '長い数字') + sup(1, '012345', '長い数字 (0 つき)')
      + sup(2, '1', 'いろは側の 1') + sup(2, '0001', 'いろは側の 0001');                 // 会社をまたぐ同じコード = 会社の中だけでまとめる
    for (const k of ['ska', 'skb', 'skb2', 'skc', 'skd', 'ske', 'skp', 'skq']) s += sku(1, k);
    s += sku(2, 'skz');
    s += ss(1, '0001', 'ska') + ss(1, '0001', 'skb', { vendor: 'K-B', cost: 50, lt: 3 }) + ss(1, '1', 'skb', { vendor: 'D-B1', units: 7, cost: 60 }) + ss(1, '01', 'skb', { vendor: 'D-B2', lt: 9, mult: 2 })   // 残す行と寄せる行の両方に値 = 残す行が先
      + ss(1, '1', 'ska', { vendor: 'V-A', units: 12, cost: 100, by: 'human', byId: 'u1' }) + ss(1, '1', 'skc', { vendor: 'V-C', moq: 2 })
      + ss(1, '01', 'skc', { vendor: 'V-C2', mult: 3, lt: 5 }) + ss(1, '01', 'skd', { vendor: 'V-D', unit: 'case', active: false }) + ss(1, '900', 'ska', { vendor: 'T-A' })
      + ss(1, '0001', 'skb2') + ss(1, '1', 'skb2', { vendor: 'V-B1' }) + ss(1, '01', 'skb2', { units: 24 }) + ss(1, '1', 'ske') + ss(1, '01', 'ske', { vendor: 'V-E2', units: 6 })
      + ss(1, '0001', 'skp') + ss(1, '01', 'skp', { primary: true, vendor: 'P' })        // 代表の印は寄せる行にだけ = 消してから残す行に付く
      + ss(1, '1', 'skq', { primary: true }) + ss(1, '99', 'skq')
      + ss(2, '1', 'skz', { vendor: 'Z1' }) + ss(2, '0001', 'skz', { units: 4, primary: true });
    s += doc('d1') + doc('d2') + link('d1', 1, '1', null) + link('d1', 1, '01', 'evidence') + link('d2', 1, '0001', null) + link('d2', 1, '1', 'attachment') + link('d2', 2, '1', 'x');
    s += po('po:1', 1, '1') + po('po:2', 1, '01') + po('po:3', 1, '0001') + po('po:4', 2, '1');
    s += ext(1, '99', '99') + ext(1, '01', '01') + ext(2, '1', 'co2-1');
    return s;
  };
  const mergeRandom = (seed) => {
    const r = rng(seed); const pick = (a) => a[Math.floor(r() * a.length)]; const maybe = (p, v) => (r() < p ? v : null);
    let s = '';
    const codes = { 1: new Set(), 2: new Set() };
    const forms = (n) => [String(n), String(n).padStart(2, '0'), String(n).padStart(4, '0'), String(n).padStart(6, '0'), `A-${String(n).padStart(2, '0')}`];
    for (let i = 0; i < 3 + Math.floor(r() * 40); i++) {
      const co = r() < 0.85 ? 1 : 2; const code = pick(forms(1 + Math.floor(r() * 6)));   // 6 つの番号に 5 つの形 = 二重の連鎖が多い
      if (codes[co].has(code)) continue; codes[co].add(code);
      s += sup(co, code, r() < 0.35 ? code : `仕入先${seed}_${i}`, { om: maybe(0.4, pick(['email', 'fax', 'web'])), lt: maybe(0.4, Math.floor(r() * 30)), active: r() < 0.85,
        email_to: maybe(0.3, `to${i}@example.invalid`), email_cc: maybe(0.2, `cc${i}@example.invalid`), contact_name: maybe(0.3, `担当${i}`), fax: maybe(0.2, `06-${i}`), relay_to: maybe(0.1, `relay${i}`), memo: maybe(0.2, `memo${i}`) });
    }
    const skus = { 1: Array.from({ length: 10 }, (_, i) => `k${i}`), 2: ['k0', 'k1', 'k2'] };
    for (const co of [1, 2]) for (const k of skus[co]) s += sku(co, k);
    const primaryTaken = new Set();
    for (const co of [1, 2]) {
      const list = [...codes[co]];
      for (const sc of list) for (const k of skus[co]) {
        if (r() > 0.5) continue;
        const primary = !primaryTaken.has(`${co}/${k}`) && r() < 0.3; if (primary) primaryTaken.add(`${co}/${k}`);
        s += ss(co, sc, k, { vendor: maybe(0.5, `V${Math.floor(r() * 99)}`), unit: maybe(0.3, pick(['case', 'inner', 'each'])), units: maybe(0.4, 1 + Math.floor(r() * 48)),
          moq: maybe(0.3, 1 + Math.floor(r() * 10)), mult: maybe(0.3, 1 + Math.floor(r() * 6)), cost: maybe(0.5, Math.floor(r() * 5000)), lt: maybe(0.3, Math.floor(r() * 20)),
          active: r() < 0.85, primary, by: pick(['system', 'human', 'ai']), byId: maybe(0.5, `by${Math.floor(r() * 5)}`), at: maybe(0.6, Math.floor(r() * 9e6)) });
      }
      for (let d = 0; d < 3; d++) {
        const ref = `doc${seed}_${co}_${d}`; s += doc(ref);
        for (const sc of list) if (r() < 0.3) s += link(ref, co, sc, maybe(0.5, pick(['evidence', 'attachment', 'main_image'])), maybe(0.6, Math.floor(r() * 9e6)));
      }
      list.forEach((sc, i) => { if (r() < 0.3) s += po(`po${seed}_${co}_${i}`, co, sc); if (r() < 0.3) s += ext(co, sc, `x${seed}_${co}_${i}`); });
    }
    return s;
  };

  console.log('relink_shipments_bulk: 旧い (0017) と新しい (0057) を比べる');
  await t('候補 0 件 (表が空・p_after が先・別の会社) = examined 0・last_id null', async () => {
    await compare('空', SHOPS, [relinkCall(1, 0, 20000), relinkCall(1, 999999, 5), relinkCall(2, null, 1)]);
  });
  await t('決まった fixture: p_limit = 候補の数ちょうど / −1 / +1・1 件ずつ続けて (同じ取引の中で 2 回目以降)・p_after = null', async () => {
    // 会社 1 の候補 = A01〜A06・A09・A10・A13・A14 の 10 件 (A07 番号なし・A08 店なし・A11 結び済み・A12 会社 2)。shipment_id は雛形から 1 で始まる
    const { b } = await compare('ちょうど', relinkFixed(), [relinkCall(1, 0, 10), relinkCall(1, 0, 10)], { oldUsesTemp: true });
    assert.deepEqual(JSON.parse(b.results[0].ok[0]), { linked: 6, examined: 10, last_id: 14 });   // A01・A02・A03・A05・A13・A14
    assert.deepEqual(JSON.parse(b.results[1].ok[0]), { linked: 0, examined: 4, last_id: 10 });    // 結べなかった 4 件 (A04・A06・A09・A10) は残る
    await compare('−1 → 続き', relinkFixed(), [relinkCall(1, 0, 9), relinkCall(1, 13, 9), relinkCall(1, null, 9)], { oldUsesTemp: true });
    await compare('+1', relinkFixed(), [relinkCall(1, 0, 11)]);
    await compare('1 件ずつ', relinkFixed(), Array.from({ length: 14 }, (_, i) => relinkCall(1, i, 1)));
    await compare('別の会社・p_after = null', relinkFixed(), [relinkCall(2, null, 100000), relinkCall(1, null, 100000)]);
  });
  await t('p_limit の誤り (0・負・null・100,001) は同じ例外 (P0001 p_limit must be 1..100000)・100,000 は通る', async () => {
    const { b } = await compare('誤り', relinkFixed(), [relinkCall(1, 0, 0), relinkCall(1, 0, -1), relinkCall(1, 0, null), relinkCall(1, 0, 100001), relinkCall(1, 0, 100000)]);
    for (const x of b.results.slice(0, 4)) assert.match(x.err, /^P0001 p_limit must be 1\.\.100000$/);
    assert.ok(b.results[4].ok);
  });
  for (const seed of [11, 12, 13, 14, 15, 16]) {
    await t(`乱数の fixture (seed ${seed})`, async () => { const { s, calls } = relinkRandom(seed); await compare(`relink seed ${seed}`, s, calls); });
  }
  await t('候補 100,001 件: p_limit 100,000 で 100,000 件・続きで 1 件 (戻り値・表の中身が同じ)', async () => {
    const big = `${SHOPS}
      insert into core.orders (company_id, mall, scope_key, mall_order_no, source_system, ordered_at, order_date_jst, status, received_batch_seq, source_updated_at, transform_version, content_hash)
        select 1, 'rakuten', 'rk', 'B' || g, 'ne', now(), date '2026-09-01', 'new', 1, now(), 't', 'h' from generate_series(1, 100000) g;
      insert into core.shipments (company_id, ne_slip_no, ne_order_no, shop_code, status, received_batch_seq, source_updated_at, transform_version, content_hash)
        select 1, 'BS' || g, 'B' || g, 'S1', 'new', 1, now(), 't', 'h' from generate_series(1, 100001) g;`;
    const t0 = Date.now();
    const { a, b } = await compare('100,001', big, [relinkCall(1, 0, 100000), 'select * from core.relink_shipments_bulk(1::smallint, (select max(shipment_id) - 1 from core.shipments), 100000)']);
    const r0 = JSON.parse(b.results[0].ok[0]);
    assert.deepEqual([r0.linked, r0.examined], [100000, 100000]);
    const r1 = JSON.parse(b.results[1].ok[0]);
    assert.deepEqual([r1.linked, r1.examined], [0, 1]);   // 100,001 件目は注文が無い (注文は 100,000 件)
    console.log(`      (呼ぶ時間 = 旧 ${a.ms.join(' / ')} ms・新 ${b.ms.join(' / ')} ms。fixture と比べるのも含めて ${((Date.now() - t0) / 1000).toFixed(1)} 秒)`);
  });

  console.log('merge_duplicate_suppliers: 旧い (0027) と新しい (0057) を比べる');
  await t('仕入先 0 件 = (0, 0, 0)', async () => {
    const { b } = await compare('空', '', [MERGE, MERGE]);
    assert.deepEqual(JSON.parse(b.results[0].ok[0]), { merged_suppliers: 0, supplier_skus_after: 0, renamed_codes: 0 });
  });
  await t('決まった fixture: 3 つの連鎖・会社をまたぐ同じコード・連絡先・有効・代表の印・文書の重なり・発注・外部 ID・コードの書き換え / 2 回目は何もしない', async () => {
    const { b } = await compare('決まった', mergeFixed(), [MERGE, MERGE], { oldUsesTemp: true });
    assert.equal(JSON.parse(b.results[0].ok[0]).merged_suppliers, 6);
    // 残す行と寄せる行の両方に値がある列は残す行の値・残す行が空の列は寄せる行 (supplier_id の順) の値 (0025 / 0027 の契約)
    const skb = b.snap['core.supplier_skus'].map((j) => JSON.parse(j)).filter((x) => x.vendor_code === 'K-B');
    assert.deepEqual(skb.map((x) => [x.stock_units_per_order_unit, x.unit_cost_jpy, x.lead_time_days, x.order_multiple]), [[7, 50, 3, 2]]);
    assert.deepEqual(JSON.parse(b.results[1].ok[0]).merged_suppliers, 0);
  });
  await t('二重の無い仕入先だけ (コードの書き換えだけ)', async () => {
    await compare('書き換えだけ', sup(1, '7', 'x') + sup(1, 'abc', 'y') + sup(2, '08', 'z'), [MERGE]);
  });
  for (const seed of [21, 22, 23, 24, 25, 26]) {
    await t(`乱数の fixture (seed ${seed})`, async () => { await compare(`merge seed ${seed}`, mergeRandom(seed), [MERGE, MERGE]); });
  }
  await t('仕入先 5,000 行 (上限ちょうど) は旧いのと同じ・5,001 行は新しい方だけ 54000 で止まり何も変えない', async () => {
    const at = (n) => `insert into core.suppliers (company_id, code, name) select 1, 'B' || g, 'B' || g from generate_series(1, ${n - 2}) g;\n` + sup(1, '1', 'いち') + sup(1, '0001', '0001');
    const { b } = await compare('5,000', at(5000), [MERGE]);
    assert.equal(JSON.parse(b.results[0].ok[0]).merged_suppliers, 1);
    const fx = at(5001);
    const plain = await runCase(T_NEW, fx, []);
    const n = await runCase(T_NEW, fx, [MERGE]);
    assert.match(n.results[0].err, /^54000 merge_duplicate_suppliers: 仕入先が 5001 行 \(上限 5000 行\)/);
    assert.deepEqual(n.snap, plain.snap);
    const o = await runCase(T_OLD, fx, [MERGE]);
    assert.equal(JSON.parse(o.results[0].ok[0]).merged_suppliers, 1);   // 旧い関数は上限なし (= この PR で足した上限)
  });

  await t('最大のまとまりの幅 (Codex R2 High): 仕入先 5,000 が同じ形のコード = 1 つの残す行に 4,999 を寄せる・同じ商品 2 つ・同じ文書 2 つ・空でない最初の値がまとまりの奥にある = 旧と同じ', async () => {
    const fx = `insert into core.suppliers (company_id, code, name, order_memo, email_to)
        select 1, repeat('0', g) || '1', case when g = 4000 then '本当の名前' else repeat('0', g) || '1' end,
               case when g >= 3000 and g % 11 = 0 then rpad('memo' || g || '-', 300, 'm') end, case when g = 4998 then 'last@example.invalid' end
          from generate_series(0, 4999) g;
      insert into core.skus (company_id, sku_kind, code, name) values (1, 'set', 'W1', 'W1'), (1, 'set', 'W2', 'W2');
      insert into core.supplier_skus (company_id, supplier_id, sku_id, vendor_code, order_unit, unit_cost_jpy, lead_time_days, is_primary, created_by_type, created_by_id, created_at)
        select 1, s.supplier_id, k.sku_id,
               case when s.supplier_id % 97 = 0 and s.supplier_id > 2500 then rpad('v' || s.supplier_id || '-' || k.code || '-', 300, 'x') end,
               case when s.supplier_id = 4321 then 'case' end, case when s.supplier_id > 4990 then s.supplier_id end, null,
               s.supplier_id = 3333 and k.code = 'W1', case when s.supplier_id % 2 = 0 then 'human' else 'system' end, 'id' || s.supplier_id,
               timestamptz '2026-03-04 05:06:07.123456+09' + make_interval(secs => 10000 - s.supplier_id)
          from core.suppliers s cross join core.skus k where k.code <> 'W2' or s.supplier_id % 3 <> 0;
      insert into docs.documents (company_id, document_type, storage, external_ref, title) values (1, 'contract', 'url', 'w1', 'w1'), (1, 'contract', 'url', 'w2', 'w2');
      insert into docs.document_links (document_id, entity_type, entity_id, link_role, created_at)
        select d.document_id, 'supplier', s.supplier_id, case when s.supplier_id % 89 = 0 and s.supplier_id > 1000 then rpad('r' || s.supplier_id || '-', 300, 'r') end,
               timestamptz '2026-03-04 05:06:07.123456+09' + make_interval(secs => s.supplier_id)
          from docs.documents d cross join core.suppliers s;`;
    const { b } = await compare('最大の幅', fx, [MERGE, MERGE]);
    assert.equal(JSON.parse(b.results[0].ok[0]).merged_suppliers, 4999);
    const kept = b.snap['core.supplier_skus'].map((j) => JSON.parse(j));
    assert.equal(kept.length, 2);
    assert.ok(kept.every((x) => x.vendor_code && x.vendor_code.startsWith('v2522-')), JSON.stringify(kept.map((x) => x.vendor_code && x.vendor_code.slice(0, 12))));   // 2,500 より大きい最初の 97 の倍数
  });

  // ─── 2 接続の同時の試験 (Codex R1 Medium): 接続を lock で止め (pg_blocking_pids で待っていることを確かめてから) 相手を commit → 戻り値・SQLSTATE・最後の表を旧と新で比べる ───
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  // 数は数に (bigint は文字で返る)
  const wrapQ = (p) => p.then((r) => ({ ok: r.rows.map((x) => JSON.stringify(Object.fromEntries(Object.entries(x).map(([k, v]) => [k, typeof v === 'string' && /^-?\d+$/.test(v) ? Number(v) : v])))) }), (e) => { if (process.env.D60_DEBUG && e.code === "40P01") console.log("      [debug] " + e.detail); return { err: `${e.code} ${e.message}` }; });
  /** pid の backend が誰かの lock を待つまで待つ。待たずに終わった = 同時の試験が成り立っていない = 失敗 */
  const waitBlocked = async (pid, pending) => {
    let done = false; pending.then(() => { done = true; });
    for (let i = 0; i < 500; i++) {
      if ((await su.query('select cardinality(pg_blocking_pids($1)) > 0 as b', [pid])).rows[0].b) {
        // 何を待っているか (lock の種類・mode・表) = 旧と新で同じ所で待つかを比べる
        const w = (await su.query(`select string_agg(l.locktype || ' ' || l.mode || coalesce(' ' || l.relation::regclass::text, ''), ', ' order by 1) as w from pg_locks l where l.pid = $1 and not l.granted`, [pid])).rows[0].w;
        if (process.env.D60_DEBUG) console.log(`      [debug] 待っている: ${w} / ${(await su.query('select query from pg_stat_activity where pid = $1', [pid])).rows[0].query.slice(0, 120).replace(/\s+/g, ' ')}`);
        return w;
      }
      if (done) throw new Error(`pid ${pid} が lock を待たずに終わった (同時の試験が成り立っていない)`);
      await sleep(20);
    }
    throw new Error(`pid ${pid} が 10 秒たっても lock を待たない`);
  };
  /** 雛形から DB を作り fixture を commit → 接続 A・B で scenario → 最後の表 (この回の後の時刻は '<now>' = 取引ごとの now() の違いを消す) */
  const concRun = async (tmpl, fixtureSql, scenario) => {
    const name = `cdb_nt_cc_${hex}_${++seq}`;
    await su.query(`create database ${name} template ${tmpl} owner ${OWNER}`);
    // deadlock の検査は待ち始めて deadlock_timeout の後に 1 回だけ = 後から待ち始めた側が deadlock_timeout より遅れて待ち始めると、先の側の検査が空振りし
    // 負ける側が入れ替わる (機械が重いとき。旧でも新でも起きた)。3 秒にしてぶれにくくする (それでも数は必須にせず、負けた側ごとの最後の表を比べる)
    await su.query(`alter database ${name} set deadlock_timeout = '3s'`);
    const since = Date.now() - 1000;
    const mine = [];
    try {
      let versionSince;
      { const l = await open(name); try { await l.query(fixtureSql); versionSince = Number((await l.query(`select case when is_called then last_value else 0 end as v from core.master_version_seq`)).rows[0].v); } finally { await close(l); } }
      const mk = async () => { const c = await open(name); mine.push(c); return { c, pid: (await c.query('select pg_backend_pid() as p')).rows[0].p, q: (sql) => wrapQ(c.query(sql)) }; };
      const A = await mk(), B = await mk();
      const one = async (sql) => { const s = await open(name); try { return (await s.query(sql)).rows; } finally { await close(s); } };
      const out = await scenario({ A, B, one });
      const s = await open(name);
      let snap; try { snap = await snapshot(s, { recentSince: since, versionSince }); } finally { await close(s); }
      return { out, snap };
    } finally {
      for (const c of mine) await close(c);
      await su.query(`drop database ${name}`);
    }
  };
  const diffTables = (x, y) => [...new Set([...Object.keys(x), ...Object.keys(y)])].filter((n) => JSON.stringify(x[n] || []) !== JSON.stringify(y[n] || [])).sort();
  const concCompare = async (fixtureSql, scenario) => {
    const a = await concRun(T_OLD, fixtureSql, scenario);
    const b = await concRun(T_NEW, fixtureSql, scenario);
    const diff = diffTables(a.snap, b.snap);
    if (process.env.D60_DEBUG) for (const n of diff) { const x = a.snap[n] || [], y = b.snap[n] || []; console.log(`DIFF ${n}\n  旧だけ ${x.filter((r) => !y.includes(r)).join('\n         ')}\n  新だけ ${y.filter((r) => !x.includes(r)).join('\n         ')}`); }
    return { a, b, diff };
  };
  const RL = relinkCall(1, 0, 100000);
  const endTx = async (x, r) => { await x.c.query(r && r.err ? 'rollback' : 'commit'); };
  /** deadlock: 何回か回して、どちらが 40P01 になったか・最後の表・A が待つ lock を旧と新で数える。
   *  必須 = 毎回ちょうど 1 本が 40P01 (ほかの誤りなし)・負けた側が同じ回の最後の表が旧と新で同じ・A が待つ lock が旧と新で同じ。
   *  どちらが負けるかの数は表示だけ (deadlock の検査の時刻で入れ替わる = 版に依らない。上の concRun の説明) */
  const deadlockRounds = async (fixtureSql, scenario, rounds = 3) => {
    const tally = {};
    for (const [label, tmpl] of [['旧', T_OLD], ['新', T_NEW]]) {
      tally[label] = { A: 0, B: 0, none: 0, both: 0, snaps: new Map(), waits: new Set() };
      for (let i = 0; i < rounds; i++) {
        const { out, snap } = await concRun(tmpl, fixtureSql, scenario);
        tally[label].waits.add(out.waitA);
        const a = out.ra.err ? out.ra.err.slice(0, 5) : 'ok', b = out.rb.err ? out.rb.err.slice(0, 5) : 'ok';
        const who = a === '40P01' && b === '40P01' ? 'both' : a === '40P01' ? 'A' : b === '40P01' ? 'B' : 'none';
        tally[label][who]++;
        tally[label].snaps.set(who, JSON.stringify(snap));
        assert.ok(['ok', '40P01'].includes(a) && ['ok', '40P01'].includes(b), `${label}: deadlock 以外の誤り ${out.ra.err || ''} / ${out.rb.err || ''}`);
      }
    }
    for (const who of ['A', 'B']) if (tally['旧'].snaps.has(who) && tally['新'].snaps.has(who)) assert.equal(tally['新'].snaps.get(who), tally['旧'].snaps.get(who), `${who} が負けた回の最後の表が旧と新で違う`);
    for (const label of ['旧', '新']) assert.equal(tally[label].none + tally[label].both, 0, `${label}: deadlock が 1 本だけでない`);
    assert.deepEqual([...tally['新'].waits], [...tally['旧'].waits], 'A が待つ lock が旧と新で違う');
    const fmt = (t) => `A が 40P01 ${t.A} 回・B が 40P01 ${t.B} 回・deadlock なし ${t.none} 回`;
    console.log(`      (deadlock ${rounds} 回ずつ: 旧 = ${fmt(tally['旧'])} / 新 = ${fmt(tally['新'])}・A が待つ lock = ${[...tally['新'].waits].join(' / ')})`);
    return tally;
  };

  console.log('同時に動くとき (2 接続を lock で止めて比べる)');
  await t('relink を同じ会社・同じ cursor で 2 本: B は A の lock を待ち、A の commit の後に残りを見る (戻り値・最後の表が同じ)', async () => {
    const { a, b, diff } = await concCompare(relinkFixed(), async ({ A, B }) => {
      await A.c.query('begin'); const a1 = await A.q(RL);
      await B.c.query('begin'); const pb = B.q(RL); await waitBlocked(B.pid, pb);
      await A.c.query('commit'); const b1 = await pb; await endTx(B, b1);
      return { a1, b1 };
    });
    assert.deepEqual(b.out, a.out); assert.deepEqual(diff, []);
    assert.deepEqual(JSON.parse(b.out.a1.ok[0]), { linked: 6, examined: 10, last_id: 14 });
    assert.deepEqual(JSON.parse(b.out.b1.ok[0]), { linked: 0, examined: 4, last_id: 10 });
  });
  await t('relink の 2 本目が lock_timeout (55P03) で止まる = 旧と同じ誤り・何も変えない', async () => {
    const { a, b, diff } = await concCompare(relinkFixed(), async ({ A, B }) => {
      await A.c.query('begin'); const a1 = await A.q(RL);
      await B.c.query('begin'); await B.c.query(`set local lock_timeout = '300ms'`); const b1 = await B.q(RL); await endTx(B, b1);
      await A.c.query('commit');
      return { a1, b1 };
    });
    assert.deepEqual(b.out, a.out); assert.deepEqual(diff, []);
    assert.match(b.out.b1.err, /^55P03 /);
  });
  await t('relink 中に伝票が直される (待った相手が 注文番号を直す・手で結ぶ): lock の後の版で数え・結ぶ (旧と同じ)', async () => {
    const { a, b, diff } = await concCompare(relinkFixed(), async ({ A, B }) => {
      await B.c.query('begin');
      await B.c.query(`update core.shipments set ne_order_no = '1002' where ne_slip_no = 'A09'`);   // 注文なし → ある注文番号
      await B.c.query(`update core.shipments set order_id = (select order_id from core.orders where company_id = 1 and mall_order_no = '1003') where ne_slip_no = 'A01'`);   // 手で結ぶ = 候補から外れる
      await A.c.query('begin'); const pa = A.q(RL); await waitBlocked(A.pid, pa);
      await B.c.query('commit'); const a1 = await pa; await endTx(A, a1);
      return { a1 };
    });
    assert.deepEqual(b.out, a.out); assert.deepEqual(diff, []);
    assert.deepEqual(JSON.parse(b.out.a1.ok[0]), { linked: 6, examined: 9, last_id: 14 });   // A01 が外れ (9 件)・A09 が 1002 に結ばれる
  });
  await t('relink 中に注文 (鍵でない列)・伝票が同じ取引で直される: 旧と同じ (注文は lock しない)', async () => {
    const { a, b, diff } = await concCompare(relinkFixed(), async ({ A, B }) => {
      await B.c.query('begin');
      await B.c.query(`update core.orders set status = 'shipped' where company_id = 1 and mall_order_no = '1001'`);
      await B.c.query(`update core.shipments set status = 'confirmed' where ne_slip_no in ('A01', 'A03')`);
      await A.c.query('begin'); const pa = A.q(RL); await waitBlocked(A.pid, pa);
      await B.c.query('commit'); const a1 = await pa; await endTx(A, a1);
      return { a1 };
    });
    assert.deepEqual(b.out, a.out); assert.deepEqual(diff, []);
    assert.deepEqual(JSON.parse(b.out.a1.ok[0]), { linked: 6, examined: 10, last_id: 14 });
  });
  await t('relink 中に待った相手が同じ取引で伝票を直し 注文を足した: 旧と同じ (結ぶ文は lock の後の新しい snapshot = 足された注文も見て結ぶ。R1 の版は 1 つの snapshot で結ばなかった)', async () => {
    const { a, b, diff } = await concCompare(relinkFixed(), async ({ A, B, one }) => {
      await B.c.query('begin');
      await B.c.query(`update core.shipments set ne_order_no = '7777' where ne_slip_no = 'A09'`);
      await B.c.query(order(1, 'rakuten', 'rk', '7777'));
      await A.c.query('begin'); const pa = A.q(RL); await waitBlocked(A.pid, pa);
      await B.c.query('commit'); const a1 = await pa; await endTx(A, a1);
      const linkedA09 = (await one(`select order_id is not null as x from core.shipments where ne_slip_no = 'A09'`))[0].x;
      return { a1, linkedA09 };
    });
    assert.deepEqual(b.out, a.out); assert.deepEqual(diff, []);
    assert.deepEqual(JSON.parse(b.out.a1.ok[0]), { linked: 7, examined: 10, last_id: 14 });
    assert.equal(b.out.linkedA09, true);
  });
  await t('relink 中に待った相手が注文の鍵を変え・注文を消した (Codex R2 Medium): 旧と同じ (変える前の鍵・消した注文には結ばない・23503 にならない)', async () => {
    const { a, b, diff } = await concCompare(relinkFixed(), async ({ A, B }) => {
      await B.c.query('begin');
      await B.c.query(`update core.orders set mall_order_no = '1002x' where company_id = 1 and mall = 'rakuten' and scope_key = 'rk' and mall_order_no = '1002'`);   // A02 の注文 (A11 は結び済み)
      await B.c.query(`delete from core.orders where company_id = 1 and mall = 'amazon' and mall_order_no = '3001'`);                                           // A05・A14 の注文 (まだ誰も結んでいない)
      await B.c.query(`update core.shipments set status = 'confirmed' where ne_slip_no = 'A01'`);                                                                 // A を候補の lock で待たせる
      await A.c.query('begin'); const pa = A.q(RL); await waitBlocked(A.pid, pa);
      await B.c.query('commit'); const a1 = await pa; await endTx(A, a1);
      return { a1 };
    });
    assert.deepEqual(b.out, a.out); assert.deepEqual(diff, []);
    assert.deepEqual(JSON.parse(b.out.a1.ok[0]), { linked: 3, examined: 10, last_id: 14 });   // A01・A03・A13 だけ (A02 は鍵が変わり、A05・A14 は注文が消えた)
  });
  await t('🚨 違い ① (説明つき): lock の文の後に commit されて (p_after, 最後の番号] に入った未結合の伝票 → 旧は結ばずに cursor を越える (次の先頭からの走査で結ぶ)・新はその場で結ぶ', async () => {
    const { a, b, diff } = await concCompare(relinkFixed(), async ({ A, B }) => {
      await B.c.query('begin');
      await B.c.query(`update core.shipments set status = 'confirmed' where ne_slip_no = 'A05'`);   // A を候補の lock で待たせる
      await B.c.query(`update core.shipments set order_id = null where ne_slip_no = 'A11'`);        // 結び済み (= 候補でない) の A11 (shipment_id 11 ≦ 最後の番号 14) を未結合に
      await A.c.query('begin'); const pa = A.q(RL); await waitBlocked(A.pid, pa);
      await B.c.query('commit'); const a1 = await pa;
      const a2 = await A.q(RL);   // 次の先頭からの走査 (relink_rescan)
      await endTx(A, a2);
      return { a1, a2 };
    });
    assert.deepEqual(JSON.parse(a.out.a1.ok[0]), { linked: 6, examined: 10, last_id: 14 });
    assert.deepEqual(JSON.parse(b.out.a1.ok[0]), { linked: 7, examined: 10, last_id: 14 });   // A11 も結んだ (examined には入らない)
    assert.deepEqual(JSON.parse(a.out.a2.ok[0]), { linked: 1, examined: 5, last_id: 11 });
    assert.deepEqual(JSON.parse(b.out.a2.ok[0]), { linked: 0, examined: 4, last_id: 10 });
    assert.deepEqual(diff, []);   // 次の走査の後は同じ
  });
  await t('🚨 違い ② (説明つき): relink 中に待った相手が店舗 (ne_shops) の接頭辞を直した → 旧は lock の文の版 (直す前)・新は結ぶ文の版 (直した後) で結ぶ', async () => {
    const { a, b, diff } = await concCompare(relinkFixed(), async ({ A, B, one }) => {
      await B.c.query('begin');
      await B.c.query(`update core.ne_shops set order_no_prefix = 'zz-' where company_id = 1 and shop_code = 'S2'`);
      await B.c.query(`update core.shipments set status = 'confirmed' where ne_slip_no = 'A01'`);
      await A.c.query('begin'); const pa = A.q(RL); await waitBlocked(A.pid, pa);
      await B.c.query('commit'); const a1 = await pa; await endTx(A, a1);
      return { a1, linkedA03: (await one(`select order_id is not null as x from core.shipments where ne_slip_no = 'A03'`))[0].x };
    });
    assert.deepEqual(JSON.parse(a.out.a1.ok[0]), { linked: 6, examined: 10, last_id: 14 });
    assert.deepEqual(JSON.parse(b.out.a1.ok[0]), { linked: 5, examined: 10, last_id: 14 });   // A03 (Yahoo 2001) は接頭辞 'zz-' では注文が無い
    assert.deepEqual([a.out.linkedA03, b.out.linkedA03], [true, false]);
    assert.deepEqual(diff, ['core.shipments']);
  });
  await t('relink と伝票の更新の deadlock (A が候補 1〜4 を lock して 5 を待つ・B が 5 を持って 2 を待つ): 毎回 1 本だけ 40P01・負けた側ごとの最後の表と A が待つ lock が旧と新で同じ', async () => {
    await deadlockRounds(relinkFixed(), async ({ A, B }) => {
      await B.c.query('begin'); await B.c.query(`update core.shipments set status = 'confirmed' where ne_slip_no = 'A05'`);
      await A.c.query('begin'); const pa = A.q(RL); const waitA = await waitBlocked(A.pid, pa);
      const pb = B.q(`update core.shipments set status = 'confirmed' where ne_slip_no = 'A02'`);
      const [ra, rb] = await Promise.all([pa, pb]);
      await endTx(A, ra); await endTx(B, rb);
      return { ra, rb, waitA };
    });
  });

  const SID = (code) => sid(1, code), KID = (code) => kid(1, code);
  await t('merge を 2 本: B は A の lock (残す仕入先) を待ち、A の commit の後は何もしない (戻り値・最後の表が同じ)', async () => {
    const { a, b, diff } = await concCompare(mergeFixed(), async ({ A, B }) => {
      await A.c.query('begin'); const a1 = await A.q(MERGE);
      await B.c.query('begin'); const pb = B.q(MERGE); await waitBlocked(B.pid, pb);
      await A.c.query('commit'); const b1 = await pb; await endTx(B, b1);
      return { a1, b1 };
    });
    assert.deepEqual(b.out, a.out); assert.deepEqual(diff, []);
    assert.equal(JSON.parse(b.out.a1.ok[0]).merged_suppliers, 6);
  });
  await t('merge 中に残す行の商品の行が直される (待った相手 = 残す行): 旧と同じ (文の始めの値で直し戻す)', async () => {
    const { a, b, diff } = await concCompare(mergeFixed(), async ({ A, B }) => {
      await B.c.query('begin'); await B.c.query(`update core.supplier_skus set vendor_code = 'X-K' where supplier_id = ${SID('0001')} and sku_id = ${KID('ska')}`);
      await A.c.query('begin'); const pa = A.q(MERGE); await waitBlocked(A.pid, pa);
      await B.c.query('commit'); const a1 = await pa; await endTx(A, a1);
      return { a1 };
    });
    assert.deepEqual(b.out, a.out); assert.deepEqual(diff, []);
  });
  await t('merge 中に寄せる行へ商品の行が足される (待った相手が FK の lock): 旧と同じ 23503 で何も変えない', async () => {
    const { a, b, diff } = await concCompare(mergeFixed(), async ({ A, B }) => {
      await B.c.query('begin'); await B.c.query(ss(1, '01', 'ska', { vendor: 'X-INS' }));
      await A.c.query('begin'); const pa = A.q(MERGE); await waitBlocked(A.pid, pa);
      await B.c.query('commit'); const a1 = await pa; await endTx(A, a1);
      return { a1 };
    });
    assert.deepEqual(b.out, a.out); assert.deepEqual(diff, []);
    assert.match(b.out.a1.err, /^23503 /);
  });
  await t('merge 中に寄せる行へ文書の紐付けが足され・残す仕入先が直される: 旧と同じ (足された紐付けも寄せる)', async () => {
    const { a, b, diff } = await concCompare(mergeFixed(), async ({ A, B }) => {
      await B.c.query('begin'); await B.c.query(link('d2', 1, '01', 'X-INS')); await B.c.query(`update core.suppliers set order_memo = 'B のメモ' where supplier_id = ${SID('0001')}`);
      await A.c.query('begin'); const pa = A.q(MERGE); await waitBlocked(A.pid, pa);
      await B.c.query('commit'); const a1 = await pa; await endTx(A, a1);
      return { a1 };
    });
    assert.deepEqual(b.out, a.out); assert.deepEqual(diff, []);
  });
  for (const [what, upd, check, oldV, newV] of [
    ['商品の行 (寄せる行 01 の skd の発注先コード)', `update core.supplier_skus set vendor_code = 'X-NEW' where supplier_id = ${SID('01')} and sku_id = ${KID('skd')}`,
      `select vendor_code as v from core.supplier_skus where supplier_id = ${SID('0001')} and sku_id = ${KID('skd')}`, 'V-D', 'X-NEW', ['core.supplier_skus', 'events.master_change_events']],
    ['文書の紐付け (寄せる行 01 の d1 の役割)', `update docs.document_links set link_role = 'X-ROLE' where entity_type = 'supplier' and entity_id = ${SID('01')} and document_id = (select document_id from docs.documents where external_ref = 'd1')`,
      `select link_role as v from docs.document_links where entity_type = 'supplier' and entity_id = ${SID('0001')} and document_id = (select document_id from docs.documents where external_ref = 'd1')`, 'evidence', 'X-ROLE', ['docs.document_links']],
  ]) {
    await t(`🚨 違い (説明つき): merge 中に${what}が直され commit → 旧は直す前の値・新は直した後の値を残す行に移す (ほかは同じ)`, async () => {
      const { a, b, diff } = await concCompare(mergeFixed(), async ({ A, B, one }) => {
        await B.c.query('begin'); await B.c.query(upd);
        await A.c.query('begin'); const pa = A.q(MERGE); await waitBlocked(A.pid, pa);
        await B.c.query('commit'); const a1 = await pa; await endTx(A, a1);
        return { a1, v: (await one(check))[0].v };
      });
      assert.deepEqual(b.out.a1, a.out.a1);
      assert.deepEqual([a.out.v, b.out.v], [oldV, newV]);
      const expectDiff = what.startsWith('商品') ? ['core.supplier_skus', 'events.master_change_events'] : ['docs.document_links'];
      assert.deepEqual(diff, expectDiff);
      for (const n of expectDiff) {   // 違う行は「旧だけの行の値 (oldV) を newV に置き換えると新だけの行」= 違うのはその値だけ (監査は残す行に足した行の INSERT の new_value)
        const onlyA = a.snap[n].filter((r) => !b.snap[n].includes(r)), onlyB = b.snap[n].filter((r) => !a.snap[n].includes(r));
        assert.ok(onlyA.length > 0 && onlyA.length === onlyB.length, `${n}: 違う行の数 旧 ${onlyA.length} / 新 ${onlyB.length}`);
        assert.deepEqual(onlyA.map((r) => r.split(oldV).join(newV)).sort(), [...onlyB].sort(), `${n} が値のほかでも違う`);
      }
    });
  }
  await t('merge 中に待った相手が残す行の商品の行を消した (Codex R2 Medium): 旧と同じ (MERGE は消えた行を WHEN NOT MATCHED に回し、まとめた値で足し直す)', async () => {
    const { a, b, diff } = await concCompare(mergeFixed(), async ({ A, B, one }) => {
      await B.c.query('begin'); await B.c.query(`delete from core.supplier_skus where supplier_id = ${SID('0001')} and sku_id = ${KID('skb')}`);
      await A.c.query('begin'); const pa = A.q(MERGE); await waitBlocked(A.pid, pa);
      await B.c.query('commit'); const a1 = await pa; await endTx(A, a1);
      return { a1, v: (await one(`select vendor_code, stock_units_per_order_unit, unit_cost_jpy, lead_time_days, order_multiple from core.supplier_skus where supplier_id = ${SID('0001')} and sku_id = ${KID('skb')}`))[0] };
    });
    assert.deepEqual(b.out, a.out); assert.deepEqual(diff, []);
    assert.deepEqual(b.out.v, { vendor_code: 'K-B', stock_units_per_order_unit: 7, unit_cost_jpy: '50', lead_time_days: 3, order_multiple: 2 });   // 文の始めの残す行の値 + 寄せる行
  });
  await t('merge の後に別の取引が残す仕入先に商品の行を足そうとする (Codex R2 Medium の窓): 足す側は merge の最初の文 (残す仕入先を直す) の lock と FK の KEY SHARE がぶつかって commit まで待つ = merge の文の snapshot の後に足されることは起きない (旧と同じ・足す側は 23505)', async () => {
    const { a, b, diff } = await concCompare(mergeFixed(), async ({ A, B }) => {
      await A.c.query('begin'); const a1 = await A.q(MERGE);
      await B.c.query('begin'); const pb = B.q(ss(1, '0001', 'skd', { vendor: 'B-D', cost: 77 })); const waitB = await waitBlocked(B.pid, pb);
      await A.c.query('commit'); const b1 = await pb; await endTx(B, b1);
      return { a1, b1: b1.err ? b1.err.slice(0, 5) : 'ok', waitB };
    });
    assert.deepEqual(b.out, a.out); assert.deepEqual(diff, []);
    assert.equal(b.out.b1, '23505');
  });
  // 文書の紐付けは仕入先への FK が無い (entity_id は汎用の番号) = 上の lock で止まらない → merge の文の snapshot の後に足されうる
  await t('🚨 違い ④ (説明つき・Codex R2 Medium): merge の文書の紐付けの文の snapshot の後に、待った相手が残す行 0001 に同じ文書 d1 の紐付けを足して commit → 旧は相手の役割を寄せる行の値で上書き・新は MERGE の 23505 をその文だけ巻き戻して新しい snapshot でやり直す (相手の行を残す行としてまとめる・誤りにならない)', async () => {
    const D1 = `(select document_id from docs.documents where external_ref = 'd1')`;
    const { a, b, diff } = await concCompare(mergeFixed(), async ({ A, B, one }) => {
      // B が寄せる行 01 の d1 の紐付けを lock → A の merge がそれを消す所で待つ (= A の文の snapshot は取った後) → B が残す行 0001 に d1 を足して commit
      await B.c.query('begin'); await B.c.query(`update docs.document_links set created_at = created_at where entity_type = 'supplier' and entity_id = ${SID('01')} and document_id = ${D1}`);
      await A.c.query('begin'); const pa = A.q(MERGE); await waitBlocked(A.pid, pa);
      await B.c.query(link('d1', 1, '0001', 'B-ROLE'));
      await B.c.query('commit'); const a1 = await pa; await endTx(A, a1);
      return { a1, v: (await one(`select link_role as v from docs.document_links where entity_type = 'supplier' and entity_id = ${SID('0001')} and document_id = ${D1}`))[0].v };
    });
    assert.deepEqual(b.out.a1, a.out.a1);
    assert.ok(b.out.a1.ok, JSON.stringify(b.out.a1));
    assert.deepEqual([a.out.v, b.out.v], ['evidence', 'B-ROLE']);
    assert.deepEqual(diff, ['docs.document_links']);   // 違うのはその 1 行の役割だけ
  });
  await t('merge と商品の行の更新の deadlock (A が残す仕入先を lock して寄せる行の商品を待つ・B が商品の行を持って残す仕入先を待つ): 毎回 1 本だけ 40P01・負けた側ごとの最後の表と A が待つ lock が旧と新で同じ', async () => {
    await deadlockRounds(mergeFixed(), async ({ A, B }) => {
      await B.c.query('begin'); await B.c.query(`update core.supplier_skus set order_unit = 'Y' where supplier_id = ${SID('01')} and sku_id = ${KID('skc')}`);
      await A.c.query('begin'); const pa = A.q(MERGE); const waitA = await waitBlocked(A.pid, pa);
      const pb = B.q(`update core.suppliers set order_memo = 'Y' where supplier_id = ${SID('0001')}`);
      const [ra, rb] = await Promise.all([pa, pb]);
      await endTx(A, ra); await endTx(B, rb);
      return { ra, rb, waitA };
    });
  });

  console.log('一時の表を使わない');
  await t('🚨 TEMP の権限を外した DB (PUBLIC と持ち主から): 新しい 2 つの関数は通る・旧い関数は 42501 (permission denied to create temporary tables)', async () => {
    const fx = relinkFixed() + mergeFixed();
    const calls = [relinkCall(1, 0, 100000), MERGE];
    const n = await runCase(T_NEW, fx, calls, { noTemp: true });
    assert.ok(n.results.every((x) => x.ok), JSON.stringify(n.results));
    assert.equal(n.tempRels, 0);
    const o = await runCase(T_OLD, fx, calls, { noTemp: true });
    for (const x of o.results) assert.match(x.err || '', /^42501 permission denied to create temporary tables/);
    // TEMP がある DB の新しい関数と同じ結果
    const w = await runCase(T_NEW, fx, calls);
    assert.deepEqual(n.results, w.results);
    assert.deepEqual(n.snap, w.snap);
  });
  await t('pg_get_functiondef に一時の表を作る文が無い・一時の表を作る関数 (監査) は reresolve_order_lines だけ・旧い DB では 3 つ', async () => {
    const c = await open(T_NEW);
    try {
      for (const s of [SIG_RELINK, SIG_MERGE]) assert.doesNotMatch((await c.query(`select pg_get_functiondef($1::regprocedure) as d`, [s])).rows[0].d, TEMP_DDL, s);
      assert.deepEqual((await tempPrivilegeAudit(pgAdapter(c))).tempFunctions, ['core.reresolve_order_lines(p_company smallint, p_mall text, p_since date)']);
    } finally { await close(c); }
    const o = await open(T_OLD);
    try {
      assert.deepEqual((await tempPrivilegeAudit(pgAdapter(o))).tempFunctions.map((x) => x.slice(0, x.indexOf('('))).sort(),
        ['core.merge_duplicate_suppliers', 'core.relink_shipments_bulk', 'core.reresolve_order_lines']);
    } finally { await close(o); }
  });
  await t('0057 は署名・戻り値の型・持ち主・EXECUTE の権限の表を変えない・search_path は pg_catalog, pg_temp・hash join を強く選ばせる設定・恒真の条件と array_agg(…)[1] が無い・もう一度流しても同じ', async () => {
    const c = await open(T_NEW);
    try {
      const q = `select p.oid::regprocedure::text as sig, p.proacl::text as acl, pg_get_userbyid(p.proowner) as owner, pg_get_function_result(p.oid) as res
        from pg_proc p where p.oid in ($1::regprocedure, $2::regprocedure) order by 1`;
      assert.deepEqual((await c.query(q, [SIG_RELINK, SIG_MERGE])).rows, aclBefore);
      const cfg = (await c.query(`select p.oid::regprocedure::text as sig, p.proconfig as cfg, p.prosecdef as secdef from pg_proc p where p.oid in ($1::regprocedure, $2::regprocedure) order by 1`, [SIG_RELINK, SIG_MERGE])).rows;
      // どちらも hash join を強く選ばせる (enable_nestloop = off・enable_mergejoin = off = CTE の件数・列の分布を planner に渡せない = 一部の鍵の結合で件数の 2 乗にさせない。
      //   off は完全な禁止ではない = ほかの方法が無ければ入れ子のループも選ばれる)・work_mem 4MB (heap の枠を呼び手に依らず決める)。設定は中で発火する trigger にも効く (下の一覧)
      for (const r of cfg) { assert.deepEqual(r.cfg, ['search_path=pg_catalog, pg_temp', 'enable_nestloop=off', 'enable_mergejoin=off', 'work_mem=4MB', 'hash_mem_multiplier=2'], r.sig); assert.equal(r.secdef, false, r.sig); }
      // Codex R2: 恒真の条件で文の中の順を作らない・array_agg でまとまりの値を全部配列にしない (merge の最初の文の対応の配列 = bigint 5,000 未満 だけは残す)
      for (const s of [SIG_RELINK, SIG_MERGE]) {
        const d = (await c.query(`select pg_get_functiondef($1::regprocedure) as d`, [s])).rows[0].d;
        assert.doesNotMatch(d, /\(select count\(\*\) from \w+\) >= 0/i, s);
        assert.doesNotMatch(d, /\(array_agg\(/i, s);
        assert.equal((d.match(/array_agg\(/gi) || []).length, s === SIG_MERGE ? 2 : 0, s);
      }
      // 関数の SET が効く trigger の一覧 (関数が書く表に発火するもの)。増えた・変わった = 設定 (planner・work_mem) がその SQL にも効く = 見直す (README の 0057 の節)
      const trg = (await c.query(`select t.tgrelid::regclass::text || ' ' || case when t.tgisinternal then 'RI ' || coalesce(con.conname, '?') else t.tgname end || ' ' || t.tgfoid::regproc::text as x
          from pg_trigger t left join pg_constraint con on con.oid = t.tgconstraint
         where t.tgrelid in ('core.shipments'::regclass, 'core.suppliers'::regclass, 'core.supplier_skus'::regclass, 'docs.document_links'::regclass, 'core.purchase_orders'::regclass, 'core.external_ids'::regclass)
         order by 1`)).rows.map((r) => r.x);
      const userTrg = trg.filter((x) => !/ RI /.test(x));
      assert.deepEqual(userTrg, [
        'core.external_ids trg_external_ids_jan_audit core.audit_master_change', 'core.external_ids trg_external_ids_jan_bump core.bump_jan_owner_version',
        'core.external_ids trg_external_ids_jan_guard core.guard_jan_external_ids', 'core.external_ids trg_external_ids_jan_no_delete core.guard_jan_external_ids',
        'core.external_ids trg_external_ids_writer core.guard_external_ids_writer', 'core.external_ids trg_master_edit_jan core.guard_master_edit_jan',
        'core.purchase_orders trg_po_closed_guard core.check_po_closed_guard', 'core.purchase_orders trg_po_issued_immutable core.check_po_issued_immutable',
        'core.purchase_orders trg_po_parent_check core.check_po_parent', 'core.purchase_orders trg_purchase_orders_touch core.touch_updated_at',
        'core.shipments trg_shipments_touch core.touch_updated_at_unless_seq_only',
        'core.supplier_skus trg_master_edit_guard ops.guard_master_edit_write', 'core.supplier_skus trg_reg_csv_live ops.guard_reg_csv_live',
        'core.supplier_skus trg_supplier_skus_audit core.audit_master_change', 'core.supplier_skus trg_supplier_skus_primary_registered core.guard_primary_supplier_registered',
        'core.supplier_skus trg_supplier_skus_touch core.touch_updated_at', 'core.supplier_skus trg_supplier_skus_version core.bump_master_version',
        'core.suppliers trg_suppliers_audit core.audit_master_change', 'core.suppliers trg_suppliers_lifecycle core.guard_suppliers_lifecycle',
        'core.suppliers trg_suppliers_touch core.touch_updated_at', 'core.suppliers trg_suppliers_version core.bump_master_version',
      ]);
      assert.ok(trg.length - userTrg.length > 0 && trg.filter((x) => / RI /.test(x)).every((x) => / "?RI_FKey_\w+"?$/.test(x)), trg.join('\n'));   // あとは FK の検査 (PostgreSQL の内部の trigger) だけ
      // その trigger の関数の本文で結合があるのは ops.guard_master_edit_write (core.products の親の輪の確かめ) と ops.guard_reg_csv_live (SKU 数個の引き) だけ =
      //   どちらも画面のロール master_edit のときだけ (ほかの呼び手は最初の行で返る)。
      //   ほかは 1 つの表を引くか早く返すだけ = 関数の planner の設定で計画が変わらない
      const withJoin = [];
      for (const fn of [...new Set(userTrg.map((x) => x.split(' ')[2]))].sort()) {
        const body = (await c.query(`select prosrc from pg_proc where oid = $1::regproc`, [fn])).rows[0].prosrc;
        if (/\bjoin\b/i.test(body)) withJoin.push(fn);
      }
      assert.deepEqual(withJoin, ['ops.guard_master_edit_write', 'ops.guard_reg_csv_live']);
      const defBefore = (await c.query(`select pg_get_functiondef($1::regprocedure) || pg_get_functiondef($2::regprocedure) as d`, [SIG_RELINK, SIG_MERGE])).rows[0].d;
      await c.query('begin'); await c.query(SQL_0057); await c.query('commit');
      assert.equal((await c.query(`select pg_get_functiondef($1::regprocedure) || pg_get_functiondef($2::regprocedure) as d`, [SIG_RELINK, SIG_MERGE])).rows[0].d, defBefore);
      assert.deepEqual((await c.query(q, [SIG_RELINK, SIG_MERGE])).rows, aclBefore);
      assert.equal((await applyMigrations(pgAdapter(c), { log: quiet })).applied.length, 0);
      // 同じ名前の関数がほかに無い (create or replace で同じ署名を置き換えた = 重ねて足していない)
      assert.deepEqual((await c.query(`select count(*)::int as n from pg_proc p join pg_namespace n on n.oid = p.pronamespace where n.nspname = 'core' and p.proname in ('relink_shipments_bulk', 'merge_duplicate_suppliers')`)).rows[0].n, 2);
    } finally { await close(c); }
  });
} catch (e) {
  setupError = e;
  console.error('❌ 準備か試験の外で落ちた (飛ばさない): ' + (e.stack || e.message));
} finally {
  for (const c of clients) { try { await c.end(); } catch { /* */ } }
  if (su) { try { await su.end(); } catch { /* */ } }
  await stopCluster();
}
console.log(`\n${ok} ok / ${ng} NG`);
// 🚨 embedded-postgres の async-exit-hook が beforeExit で process.exit(0) を呼ぶ = exitCode では足りない (profit-fn-revoke-pg と同じ) → 明示で process.exit
process.exit(ng || setupError || cleanupFailed ? 1 : 0);
