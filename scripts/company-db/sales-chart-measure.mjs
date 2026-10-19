/**
 * sales-chart-measure.mjs — 販売数のグラフ (0086 の ops.sku_sales_daily) の速さを本番で **読むだけ** で測る (0092 の前と後・書かない)
 *
 * 出すこと:
 *   1. 注文の 2 表 (core.orders / core.order_lines) の大きさ・行数・「全部の行が見える」頁の割合 (visibility map = 索引だけで読める割合)・最後の vacuum・vacuum の後に足した / 変わった行
 *   2. 0092 の索引 3 本があるか・valid か・大きさ (無ければ 予想の大きさ = 当てる前の容量の見積もり)
 *   3. --code の SKU ごとに、関数の本文 (pg_proc の今の版) を explain (analyze, buffers) = 時間・読んだ頁 (hit / read)・使った索引・表の頁を読んだ数 (heap fetch)
 *      (本文に引数を埋めた診断。画面と同じ「関数の呼び出し」の時間は --call で別に測る = SQL の関数の計画の作り方が違いうる・Codex 0092 R1 Medium)
 *   4. (--call・本文の EXPLAIN より先に測る) 画面と同じ形 select … from ops.sku_sales_daily($1, $2) (引数は bind) の時間。持ち主の接続 (COMPANY_DB_URL) で begin read only の中 (watcher は関数の実行権が無い)
 *   5. (--vacuum だけ) vacuum (analyze, truncate false) core.orders / core.order_lines を流す = 0092 の後に「全部見える」印を付ける (書く操作は これだけ・中原さんの OK の後に)。
 *      vacuum は読み書きを止めない (SHARE UPDATE EXCLUSIVE・truncate false = 末尾の空き頁を切り詰めない = ACCESS EXCLUSIVE を取らない・Codex 0092 R2)・取引の外。持ち主の接続 (COMPANY_DB_URL)
 *
 * 安全:
 *   - 照会用のロール (COMPANY_DB_WATCH_URL = watcher・select だけ) で、begin read only の取引の中で読む。最後は rollback
 *   - statement_timeout = 取引の中だけ 60 秒
 *   - 出すのは 件数・頁の数・時間・索引の名前だけ (注文番号・金額は出さない)
 *
 * 使い方 (miniPC):
 *   set DOTENV_CONFIG_PATH=C:\Users\bfaith\bfaith-portal\.env&& node -r dotenv/config scripts/company-db/sales-chart-measure.mjs --code 0726-001931 --code 0726-001389
 *   オプション: --code <商品コード> (何回でも・無ければ 1 と 2 だけ) / --call (4 も・COMPANY_DB_URL) / --json (JSON だけを出す)
 *   当てた後の印 (中原さんの OK の後): node -r dotenv/config scripts/company-db/sales-chart-measure.mjs --vacuum (これだけを流して終わる)
 */
import { fileURLToPath } from 'node:url';
import { openPgClient } from './migrate.mjs';

export const CHART_INDEXES = Object.freeze(['ix_order_lines_listing_only_chart', 'ix_order_lines_sku_chart', 'ix_orders_chart']);
const FROM = '2024-10-01';   // 画面が頼む期間の始まりと同じ形 (24 か月前の月初・関数の中で 2025-01-01 に切る)

/** explain の JSON の木 → 1 行ずつの要約 (純粋な関数) */
export function planLines(node, depth = 0, out = []) {
  const n = node || {};
  const parts = [`${'  '.repeat(depth)}${n['Node Type']}`];
  if (n['Index Name']) parts.push(`using ${n['Index Name']}`);
  if (n['Relation Name']) parts.push(`on ${n['Relation Name']}`);
  parts.push(`rows=${n['Actual Rows']} loops=${n['Actual Loops']} hit=${n['Shared Hit Blocks']} read=${n['Shared Read Blocks']}`);
  if (n['Heap Fetches'] != null) parts.push(`heap_fetch=${n['Heap Fetches']}`);
  if (n['Actual Total Time'] != null) parts.push(`${Math.round(n['Actual Total Time'] * (n['Actual Loops'] || 1))}ms`);
  out.push(parts.join(' '));
  for (const k of n.Plans || []) planLines(k, depth + 1, out);
  return out;
}

/** 関数の本文の引数を値で埋める (sku_id は数・日付は決まった形だけ = 文字列を SQL に入れない) */
export function bodyWith(prosrc, skuId, from = FROM) {
  if (!/^\d+$/.test(String(skuId)) || !/^\d{4}-\d{2}-\d{2}$/.test(from)) throw new Error('sku_id / from の形が違う');
  return prosrc.replace(/\bp_sku_id\b/g, `${skuId}::bigint`).replace(/\bp_from\b/g, `date '${from}'`);
}

async function main() {
  const args = process.argv.slice(2);
  const codes = [];
  for (let i = 0; i < args.length; i++) if (args[i] === '--code' && args[i + 1]) codes.push(args[++i]);
  const jsonOnly = args.includes('--json');
  if (args.includes('--vacuum')) {
    if (!process.env.COMPANY_DB_URL) { console.error('COMPANY_DB_URL が無い (vacuum は表の持ち主の接続で)'); process.exit(2); }
    const o = await openPgClient(process.env.COMPANY_DB_URL);
    try {
      for (const t of ['core.orders', 'core.order_lines']) { const t0 = Date.now(); await o.query(`vacuum (analyze, truncate false) ${t}`); console.log(`vacuum (analyze, truncate false) ${t}: ${Math.round((Date.now() - t0) / 1000)} 秒`); }
    } finally { await o.end(); }
    return;
  }
  // 🆕 Codex 0092 R2: 画面と同じ呼び出しを先に測る (下の本文の EXPLAIN が頁を温めた後に測ると、初めて開く画面の時間より速く出る)
  const calls = new Map();
  if (args.includes('--call') && codes.length) {
    if (!process.env.COMPANY_DB_URL) { console.error('COMPANY_DB_URL が無い (--call は関数を実行できる接続で)'); process.exit(2); }
    const o = await openPgClient(process.env.COMPANY_DB_URL);
    try {
      await o.query('begin read only');
      await o.query(`set local statement_timeout = '60s'`);
      for (const code of codes) {
        const k = (await o.query(`select sku_id from core.skus where company_id = 1 and code_norm = core.norm_code($1)`, [code])).rows[0];
        if (!k) continue;
        const t0 = Date.now();
        const n = (await o.query(`select count(*)::int as n from ops.sku_sales_daily($1::bigint, $2::date)`, [k.sku_id, FROM])).rows[0].n;
        calls.set(code, { ms: Date.now() - t0, rows: n });
      }
    } finally { try { await o.query('rollback'); } catch { /* 読むだけ */ } await o.end(); }
  }
  const url = process.env.COMPANY_DB_WATCH_URL;
  if (!url) { console.error('COMPANY_DB_WATCH_URL が無い (照会用のロール watcher の接続)'); process.exit(2); }
  const c = await openPgClient(url);
  const out = { tables: [], indexes: [], skus: [] };
  try {
    await c.query('begin read only');
    await c.query(`set local statement_timeout = '60s'`);
    out.tables = (await c.query(`
      select c.relname as table, c.reltuples::bigint as rows, pg_catalog.pg_relation_size(c.oid) as heap_bytes, pg_catalog.pg_total_relation_size(c.oid) as total_bytes,
             c.relpages, c.relallvisible, round(100.0 * c.relallvisible / greatest(c.relpages, 1), 1) as all_visible_pct,
             round(100.0 * c.relallfrozen / greatest(c.relpages, 1), 1) as all_frozen_pct,
             array_to_string(c.reloptions, ',') as reloptions,
             s.last_vacuum, s.last_autovacuum, s.last_analyze, s.last_autoanalyze, s.n_dead_tup, s.n_ins_since_vacuum, s.n_mod_since_analyze
        from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid = c.relnamespace
        left join pg_catalog.pg_stat_user_tables s on s.relid = c.oid
       where n.nspname = 'core' and c.relname in ('orders', 'order_lines') order by 1`)).rows;
    const have = (await c.query(`select c.relname as name, i.indisvalid as valid, pg_catalog.pg_relation_size(c.oid) as bytes
        from pg_catalog.pg_index i join pg_catalog.pg_class c on c.oid = i.indexrelid join pg_catalog.pg_namespace n on n.oid = c.relnamespace
       where n.nspname = 'core' and c.relname = any($1::text[])`, [CHART_INDEXES])).rows;
    // 予想 = 行数 × 1 行の大きさ (見出し 8 + 列 + 行の指し 4・頁の 90% まで詰める)。部分索引も全部の行で数える (大きい向き)
    const per = { ix_order_lines_listing_only_chart: ['order_lines', 44], ix_order_lines_sku_chart: ['order_lines', 44], ix_orders_chart: ['orders', 44] };
    for (const name of CHART_INDEXES) {
      const h = have.find((x) => x.name === name);
      const t = out.tables.find((x) => x.table === per[name][0]);
      out.indexes.push({ name, exists: !!h, valid: h ? h.valid : null, bytes: h ? Number(h.bytes) : null,
        estimateBytes: t ? Math.round((Number(t.rows) * per[name][1]) / 0.9) : null });
    }
    const src = (await c.query(`select prosrc from pg_catalog.pg_proc where oid = pg_catalog.to_regprocedure('ops.sku_sales_daily(bigint, date)')`)).rows[0];
    if (!src) throw new Error('ops.sku_sales_daily が無い (0086 の前)');
    for (const code of codes) {
      const k = (await c.query(`select sku_id, sku_kind from core.skus where company_id = 1 and code_norm = core.norm_code($1)`, [code])).rows[0];
      if (!k) { out.skus.push({ code, error: '無いコード' }); continue; }
      const r = await c.query(`explain (analyze, buffers, format json) ${bodyWith(src.prosrc, k.sku_id)}`);
      const p = r.rows[0]['QUERY PLAN'][0];
      out.skus.push({ code, kind: k.sku_kind, ms: Math.round(p['Execution Time']), rows: p.Plan['Actual Rows'],
        hit: p.Plan['Shared Hit Blocks'], read: p.Plan['Shared Read Blocks'], plan: planLines(p.Plan),
        callMs: calls.get(code)?.ms ?? null, callRows: calls.get(code)?.rows ?? null });
    }
  } finally {
    try { await c.query('rollback'); } catch { /* 読むだけ */ }
    await c.end();
  }
  if (jsonOnly) { console.log(JSON.stringify(out, null, 2)); return; }
  const mb = (b) => (b == null ? '-' : `${(Number(b) / 1048576).toFixed(0)} MB`);
  console.log('1 注文の 2 表');
  for (const t of out.tables) {
    console.log(`  ${t.table}: 約 ${Number(t.rows).toLocaleString()} 行・表 ${mb(t.heap_bytes)} (索引を含めて ${mb(t.total_bytes)})・全部見える頁 ${t.all_visible_pct}% (凍った頁 ${t.all_frozen_pct}%)・設定 ${t.reloptions || '(既定)'}`);
    console.log(`    最後の vacuum ${t.last_vacuum || '-'} / autovacuum ${t.last_autovacuum || '-'}・その後に足した行 ${t.n_ins_since_vacuum}・死んだ行 ${t.n_dead_tup}`);
  }
  console.log('2 0092 の索引');
  for (const i of out.indexes) console.log(`  ${i.name}: ${i.exists ? `ある (${i.valid ? 'valid' : '🚨 invalid'}・${mb(i.bytes)})` : `まだ無い (予想 ${mb(i.estimateBytes)} まで)`}`);
  console.log('3 関数の速さ (explain analyze・読むだけ)');
  for (const s of out.skus) {
    if (s.error) { console.log(`  ${s.code}: ${s.error}`); continue; }
    console.log(`  ${s.code} (${s.kind}): ${s.ms} ms・${s.rows} 行・頁 hit ${s.hit} / read ${s.read}${s.callMs != null ? `・関数の呼び出し ${s.callMs} ms (${s.callRows} 行)` : ''}`);
    for (const l of s.plan) console.log(`      ${l}`);
  }
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  main().catch((e) => { console.error(`❌ ${e.message}`); process.exit(1); });
}
