/**
 * test-master-publish.mjs — Company DB の写し (マスタ正本切替 ④a。設計 = AI_reference CompanyDB構想/15 §4「必ず書く試験」の miniPC 側 + Codex ④ 設計 R0)
 *
 * 固定する契約:
 *   1 持ち主が全部 load なら PR の前と同じ: 業務の表 7 つ (m_products・m_set_components・exception_genka・product_shipping・product_tax_rate・
 *     product_sales_class・m_reorder_setting) の行を全部 (時刻の列も・時計を止めて) ハッシュした値と、作り直しが書き換えた行の数 (total_changes) が
 *     前のコード (origin/master c894ca64) と同じ。世代が無い朝も、値 0 行の世代がある朝も。写しだけの道 (読む・重ねる・そろえる・確かめる) は 1 行も書かない。
 *     わざと壊す: 値を変えない UPDATE でも行の数で見つかる
 *   2 NE の原価が 0 より大きくても C が勝つ・原価の出どころの逆向き (manual / override_zero (0 円) / imported → 例外)・セットは C の構成品から導く・理由は company_owned
 *   3 C の原価が空 = MISSING・原価 NULL で作り直しは止まらない (例外原価の行は触らない)
 *   4 上書き表: exception_genka / product_shipping は既にある行だけ・m_reorder_setting はこの作り直しの SKU だけ入れる / 直す / 消す
 *   5 Company DB にしか無い SKU は m_products にも m_reorder_setting にも足さない (not_in_ne)
 *   6 世代が古い・後退 (変更の記録の番号)・途中で落ちた・検証で落ちた・商品名が SKU と違う = 印は動かない = 作り直しは前の世代
 *   7 止める: 持ち主の設定が違う (別の epoch)・世代が無い・値が欠ける・m_products の 2 つのコードが同じ C の SKU に当たる = 入れ替えない
 *   8 由来が消えない (記録と同じ中身・写しの世代が由来・Render 到達の証跡に載る)・mirror に送ると材料は matched・C の値はロードの後も残る・2 回目も同じ
 *   9 取扱区分の語 (NE の語が同じ区分なら残す・持ち主を替えただけでは語を変えない)
 *  10 わざと壊す: 世代の値の書き換え (ハッシュ)・入れた後に m_products が世代と違う = 巻き戻す (取引の中の確かめ)・次の工程の確かめも見つける
 *  11 世代は 14 個残す (今の世代は消さない)・印は前にしか進まない
 *  12 入口 (cli): 取れた回は ping なし・受け入れない = fail / 入れた後の確かめが通った回だけ ok・今朝の作り直しでない = fail / 未設定 = ⏭️ + fail / --dry-run は書かない
 *  13 台帳・daily-sync (再構築の直前と直後)・照合 ② が company_owned を知っている
 *  17 持ち主の epoch (0055・Codex #1564 R1 H1): config (configured) を書き換えただけでは何も変わらない / prepare → 明示のロード → 写し → 作り直し → 確かめ → activate の順だけ /
 *     証拠 (今の作り直しの世代・今朝の確かめ・読み直し) が欠ければ active にしない / 証拠の世代の後に夜間ロードが入った = active にしない (R3 High 1・R4 = commit の番号) /
 *     明示のロードは本番と同じ HTTP の道 (router の startLoad・host = render) = 写しは場所ではなく最後に commit したロードを使う (R4 High) /
 *     行が無い = 全部 load / cancel / 記録は足すだけ
 *  18 NE にしか無いセット (Company DB に無い) = 構成品が C にあっても全部 NE の道 (導いた原価・税・売上分類・取扱区分・構成品の名前と原価)・確かめも NE の値 (Codex #1564 R1 H2)
 *  19 ②b 古い表 (NE に欄が無い列 = 税区分・売上分類・送料・推奨保有月数。Codex #1564 R1 H3): 全部 load = 比べない / 作り直しの直後 = 差 0 (由来 = 作り直し・世代) /
 *     写しの後に C を直した = 反映待ち (台帳・始まりは動かない)・翌朝の作り直しで入る / 世代にあった値が無い = breach / 期限の後も違う = overdue /
 *     台帳が使えない = blocked / 作り直しが今朝でない = blocked / 朝の要約の先頭と証跡 master-compare に出る
 *  20 写しの反映が世代と違う朝 (exit 4) = 後の m_products・上書き表を読む工程を全部止める (Codex #1564 R1 H4): 工程ごとの判断・daily-sync の工程が全部どちらかの一覧に載る・
 *     止めない工程は直接読まない・自分で ping を打つ工程は fail の ping・配線 (確かめの直後に立てる・runScript が最初に判断・通知の前に ping)
 *  21 同じ NE の取得・同じ世代・同じ中身の作り直し = 何も書かない (業務の表 7 つの全部の行・時刻・作り直しの記録・世代の表が同じ)・
 *     NE の取得が新しい = 今までどおり入れ替える (毎朝の daily-sync)・古い表が書き換えられていた・中身が違う = 入れ替えて直す (#1564 Codex R2 Medium 5)
 *  22 影運転の数: C が空で古い表に行がある推奨保有月数も「変わる」に数える (null も値。Codex #1564 R1 L7)
 *  23 切替の後に作り直しを飛ばした・止まった朝 (写しだけ新しい世代) = 遅れ (exit 1・fail の ping)。broken にしない = 後の工程は止めない
 *     (A C の原価を直した朝 / B 何も直さず NE の新しい商品が夜間ロードで C に入った朝。#1564 の見直し H-A)
 *  24 止めるかどうかの正 = warehouse.db の門 (cdb_publish_gate。safe / broken / unknown): 違う = broken (証跡が書けなくても exit 4)・遅れ・証跡が無い・古い = 前の値のまま・
 *     safe に戻せるのは通った確かめだけ・行が無く持ち主が C = unknown・読めない = unknown・全部 load で行が無い = 流す (今と同じ)・
 *     自動再試行 (RERUN_AFTER も)・商品管理リストの手の更新も同じ門で止まる (M-2・#1564 Codex R2 High 2) /
 *     (R3 High 2) safe は確かめた作り直し・世代・ハッシュを持つ = 後に作り直した・broken を書けなかった (前の safe が残った) = 使わない (unknown・再試行と手の更新も止まる) /
 *     門の表が読めない = unknown (行が無いと同じにしない) / 行が無く全部 load = 今の世代・作り直し・作り直しの世代がそろうときだけ流す /
 *     (R4 Medium 1) 作り直しが今の世代を使っていない (写しの後に作り直しが失敗した) = 全部 load でも unknown・確かめが通れば全部 load でも safe を書く /
 *     (R5 Medium) 確かめが通らなかった (落ちた・exit 1) 朝 = 確かめた safe の行が無ければ unknown を残し (再試行・手の更新も止まる)・daily-sync もこの回を止める /
 *     確かめた safe の行が今も合う遅れの朝 = 流す・broken は unknown に替えない /
 *     (R6 Medium) 写しが取れない・NE と作り直しは通った朝 (前の世代で新しい作り直し) = 古い表が作り直しの世代と同じなら safe を書く (exit 1 のまま)・daily-sync・再試行・手の更新は流す /
 *     (R6 Low) warehouse.db を開けない・門を書けない (SQLITE_BUSY) 回 = 止めの印 (DATA_DIR のファイル) を残す = 故障が直っても通った確かめまで止まる
 *  25 記録の後に足した列 (記録した持ち主に無い) = load として足す・知らない列 = 壊れ (M-4)
 *  27 持ち主の正を 1 つに (0055・Codex #1564 R2 High 3): 0001〜0055 がそろって入る / 切替の段階を company_owner に進める前提 = epoch が active (差し込み口) /
 *     段階の owner_hash = active (⑤-1 の段階の行の trigger) / 画面の保存の門 (⑤-1 の ops.begin_master_write) = active (記録に無い列 = load)
 *  28 持ち主表のハッシュは 1 つの式 (load の列は数えない。#1564 Codex R3 Medium): JS = DB / 前の列の組で切り替えた後に列を足しても画面の保存・登録の門は通る・
 *     夜間ロードは足した列を load で動かす
 *  30 セットの導いた値・構成品の行 (コード・数量・C の名前・原価) も入れた後の確かめで比べる (同じ決め方で導き直す・ハッシュに入る) = 書き換え = broken (R7 High)・印を消せない確かめは通らない (R7 Medium 1)
 *  31 JAN だけ company の prepare は通る (写さない列)・Amazon の構成は断る (R7 Medium 2)
 *  32 区分 (skus.sku_kind) の持ち主が C: prepare → 明示のロード → 写し → 作り直し → 確かめ → activate / C = 単品・NE = セットは C の区分・C の値で写す (構成の行なし) / C = セット・NE = 単品はその SKU だけ前の行のまま /
 *     夜間ロードは区分を NE に戻さない / f_sales のセット展開・商品管理リストが壊れない / load に戻すと NE の区分
 *  33 区分の持ち主が C の 9 升 (C 3 × NE 3) の期待値・前の行が無い = 載せない・確かめ・②b・下流 3 系統 (f_sales・商品管理リスト・router)
 *  29 最後に commit したロード = DB が振る番号の順 (0055 の ops.master_load_commits。送り手の時計・場所では決めない・数で並べる)・dry-run は番号なし・
 *     0055 の前の毎晩のロード (番号の行が無い) = 最初の朝はそれを使う (commit_seq = null。R5 Low)・
 *     写しは場所を問わず最後のロード・番号の表は足すだけ (#1564 Codex R4 Medium 2・High)
 *  26 NE と C で種類が違う SKU (NE でセットを単品に) = その SKU だけ NE の値 (⚠️・証跡)・作り直し全部は止めない (M-5) /
 *     C のセットの構成品に NE にしか無い単品 = 値が混ざる = ⚠️・証跡 (L-2)
 * 使い方: node scripts/test-master-publish.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cdb-publish-'));
const mirrorDir = fs.mkdtempSync(path.join(os.tmpdir(), 'cdb-publish-mirror-'));
process.env.DATA_DIR = tmp;
process.env.DAILY_SYNC_RUN_ID = 'ds_test_publish';
delete process.env.COMPANY_DB_WATCH_URL;

const { default: Database } = await import('better-sqlite3');
const { PGlite } = await import('@electric-sql/pglite');
const { applyMigrations, pgliteAdapter } = await import('./company-db/migrate.mjs');
const { initDB, getDB, readNeRawRev } = await import('../apps/warehouse/db.js');
const { rebuildMProducts, TAX_RATES } = await import('../apps/warehouse/rebuild-m-products.js');
const { readMasterMaterial, readMaterialWithLineage, latestBuild, MASTER_BUILD_RULE_VERSION } = await import('../apps/warehouse/master-material.js');
const { materialDigest, buildMaterialGeneration, projectMaterialRows, MATERIAL_COLUMNS } = await import('../apps/warehouse/material-lineage.js');
const { MIRROR_PRODUCTS_DDL, MIRROR_SET_COMPONENTS_DDL } = await import('../apps/warehouse-mirror/material-tables.js');
const { masterReceiptEvidence } = await import('../apps/warehouse/sync-to-render.js');
const { buildPlanFromRender } = await import('../apps/company-db/load/sources.mjs');
const { runInitialLoad } = await import('../apps/company-db/load/engine.mjs');
// 試験の基準 = 全部 load の持ち主表 (⑤-3b の PR から configured = config/master-ownership.mjs は 10/5 の 13 キーが company。写し・作り直しは epoch を読むので configured は基準にしない)
const { OWNED_COLUMNS: OWNED_COLS_ALL } = await import('../config/master-ownership.mjs');
const MASTER_OWNERSHIP = Object.freeze(Object.fromEntries(OWNED_COLS_ALL.map((k) => [k, 'load'])));
const { readEvidence } = await import('../apps/company-db/push/evidence.mjs');
const { jstDateStr } = await import('../lib/jst-date.js');
const MP = await import('../apps/warehouse/master-publish.js');
const F = await import('../apps/company-db/publish/fetch.mjs');
const OS = await import('../apps/company-db/load/ownership-state.mjs');
const EP = await import('./company-db/master-ownership-epoch.mjs');
const FX = await import('./fixtures/master-publish/test-fixture.mjs');
const { SEMANTIC_VERSIONS, decisionPrint } = await import('../apps/company-db/master-compare/compare-ne.mjs');
const { JOBS_REGISTRY, validateRegistry } = await import('../config/jobs-registry.mjs');

let passed = 0;
async function ta(name, fn) { try { await fn(); passed++; console.log(`  ok  ${name}`); } catch (e) { console.error(`  NG  ${name}\n      ${e.stack || e.message}`); process.exitCode = 1; } }
const quiet = () => {};
const quietly = async (fn) => { const l = console.log, w = console.warn, er = console.error; console.log = quiet; console.warn = quiet; console.error = quiet; try { return await fn(); } finally { console.log = l; console.warn = w; console.error = er; } };
const sha = (s) => crypto.createHash('sha256').update(s).digest('hex');

/**
 * PR の前のコード (origin/master c894ca64) で、fixtures/master-publish/test-fixture.mjs の seedNe() の材料を作り直した値。
 *   first / second = 空の m_products から 1 回目・続けて 2 回目 (時計を FROZEN_AT に止めて。changes = total_changes() の増え方・tables_all = 業務の表 7 つの全部の行のハッシュ)
 * 🚨 材料を変えたら前のコードで取り直す (今のコードで取り直さない = 変わっていない証明にならない)。取り直し方:
 *   git archive c894ca64 apps/warehouse lib config | tar -x -C .golden-tmp → .golden-tmp に置いた小さな script で FX.seedNe → FX.noopProbe(rebuildMProducts) を 2 回 (その後 .golden-tmp は消す)
 */
const GOLDEN = Object.freeze({
  products: { row_count: 3125, content_hash: 'bc46aa8d60541791b41d08a9c5bb57344ec16f3ee53b5594cc1b4f54d7e3e76b' },
  set_components: { row_count: 14, content_hash: '323d672b9ccbf79e7eef6388c8ab4cd1c988f5dffbaa9a8cb522d10955917061' },
  reasons_sha: 'f780e4c1d3ef35ba924ff5877cd78c832c82d1c7edd415a0fe11d80a5cca8147',
  reasons_n: 13,
  first: { changes: 6281, tables_all: 'a29b93c4477658981d52083cb184fe2562a39803ec1e2e6da14297a1275c18c5' },
  second: { changes: 12561, tables_all: 'a29b93c4477658981d52083cb184fe2562a39803ec1e2e6da14297a1275c18c5' },
});

await quietly(() => initDB());
const db = getDB();
FX.seedNe(db, readNeRawRev);

const rebuild = (ownership) => quietly(() => rebuildMProducts(ownership ? { ownership } : undefined));
const mp = (code) => db.prepare('SELECT * FROM m_products WHERE 商品コード = ?').get(code);
function snap() {
  const m = readMasterMaterial(db);
  const b = db.prepare('SELECT * FROM m_products_builds ORDER BY rowid DESC LIMIT 1').get();   // 入れた順 (時計を止めた回は published_at が同じ)
  return { products: materialDigest('products', m.products), set_components: materialDigest('set_components', m.set_components), reasons: JSON.parse(b.reasons), reasonsText: b.reasons, build: b };
}
const reasonsOf = (s, code) => s.reasons.filter((r) => r.code === code);
const OWN = (...keys) => ({ ...MASTER_OWNERSHIP, ...Object.fromEntries(keys.map((k) => [k, 'company'])) });
const pointer = () => MP.currentGenerationNo(db);
const genRow = (no) => db.prepare('SELECT * FROM cdb_publish_generations WHERE generation_no = ?').get(no);
const count = (t, where = '1 = 1', ...p) => db.prepare(`SELECT COUNT(*) AS n FROM ${t} WHERE ${where}`).get(...p).n;

// ── Company DB (PGlite) と Render の mirror (一時) ──
const pg = new PGlite(); const pdb = pgliteAdapter(pg);
await applyMigrations(pdb, { log: quiet });
await pdb.query("select set_config('ops.widen_protocol', '1', false)");   // 0058 (G5): この試験は持ち主の epoch を直接置く = 同じ印 (G5 そのものは scripts/test-master-widen.mjs が見る)
const q = async (sql, p) => (await pdb.query(sql, p)).rows;
/** m_products を Render の mirror に送った状態にする (送り手と受け手と同じ: 中身と、中身から出し直したハッシュの世代) */
function publishMirror() {
  const { products, set_components } = readMasterMaterial(db);
  const m = new Database(path.join(mirrorDir, 'warehouse-mirror.db'));
  try {
    m.exec(MIRROR_PRODUCTS_DDL); m.exec(MIRROR_SET_COMPONENTS_DDL);
    m.exec(`CREATE TABLE IF NOT EXISTS mirror_material_generations (entity TEXT PRIMARY KEY, generation_id TEXT NOT NULL, content_hash TEXT NOT NULL, row_count INTEGER NOT NULL,
      source_complete_at TEXT, created_at TEXT, received_at TEXT NOT NULL, semantics TEXT)`);
    const g = buildMaterialGeneration({ products, set_components });
    m.transaction(() => {
      m.exec('DELETE FROM mirror_products; DELETE FROM mirror_set_components; DELETE FROM mirror_material_generations');
      const put = (table, cols, rows) => { const st = m.prepare(`INSERT INTO ${table} (${[...cols, 'updated_at'].map((c) => `"${c}"`).join(', ')}) VALUES (${[...cols, 'updated_at'].map(() => '?').join(', ')})`); for (const r of rows) st.run(...cols.map((c) => r[c]), 'x'); };
      put('mirror_products', MATERIAL_COLUMNS.products, projectMaterialRows('products', products));
      put('mirror_set_components', MATERIAL_COLUMNS.set_components, projectMaterialRows('set_components', set_components));
      for (const e of ['products', 'set_components']) {
        const d = materialDigest(e, m.prepare(`SELECT * FROM mirror_${e}`).all());
        m.prepare('INSERT INTO mirror_material_generations (entity, generation_id, content_hash, row_count, source_complete_at, created_at, received_at) VALUES (?,?,?,?,?,?,?)')
          .run(e, g.generation_id, d.content_hash, d.row_count, null, g.created_at, 'x');
      }
    })();
    return g;
  } finally { m.close(); }
}
let loadN = 0;
/** 夜間ロード (持ち主を渡さない = 本番と同じく Company DB の epoch で決まる)。usePrepared = 切替の日に明示して頼んだロード */
async function loadNow({ usePrepared = false } = {}) {
  const r = await runInitialLoad(pdb, buildPlanFromRender({ dataDir: mirrorDir, log: quiet }), { log: quiet, runId: `load_pub_${++loadN}`, host: 'render-nightly', usePrepared });
  assert.equal(r.ok, true, r.error);
  return r;
}
/**
 * 切替の日の明示のロードを本番と同じ道で流す: HTTP の POST /apps/company-db/sync/load?apply=1&use_prepared=1 (scripts/company-db/remote-load.mjs load --apply --use-prepared)
 *   → router の startLoad (host = 'render') → runLoadOnce → runInitialLoad。接続は router の差し替え口で PGlite に (#1564 Codex R4 High)。
 *   終わるまで待ち、失敗なら投げる。戻り値 = { run_id, last (router の状態), commit (0055 の commit の番号の行) }
 */
let httpLoad = null;
async function loadViaHttp({ usePrepared = true } = {}) {
  if (!httpLoad) {
    const CR = await import('../apps/company-db/router.mjs');
    const express = (await import('express')).default;
    CR.__setPgClientFactory(async () => ({
      query: async (text, params) => {
        if (params && params.length) return pg.query(text, params);
        if (text.includes(';')) { await pg.exec(text); return { rows: [] }; }
        return pg.query(text);
      },
      end: async () => {}, on: () => {},
    }));
    const app = express();
    app.use('/apps/company-db/sync', CR.default);
    const server = await new Promise((resolve) => { const sv = app.listen(0, '127.0.0.1', () => resolve(sv)); });
    server.unref();
    httpLoad = { CR, base: `http://127.0.0.1:${server.address().port}/apps/company-db/sync` };
  }
  const saved = { DATA_DIR: process.env.DATA_DIR, COMPANY_DB_URL: process.env.COMPANY_DB_URL, MIRROR_SYNC_KEY: process.env.MIRROR_SYNC_KEY };
  Object.assign(process.env, { DATA_DIR: mirrorDir, COMPANY_DB_URL: 'postgres://test', MIRROR_SYNC_KEY: 'k-publish' });
  let res;
  try {
    const r = await fetch(`${httpLoad.base}/load?apply=1${usePrepared ? '&use_prepared=1' : ''}`, { method: 'POST', headers: { 'x-sync-key': 'k-publish' } });
    res = { status: r.status, body: await r.json() };
  } finally { for (const [k, v] of Object.entries(saved)) { if (v === undefined) delete process.env[k]; else process.env[k] = v; } }
  assert.equal(res.status, 202, JSON.stringify(res.body));
  for (let i = 0; i < 600 && httpLoad.CR.getLoadState().current; i++) await new Promise((r) => setTimeout(r, 50));
  const last = httpLoad.CR.getLoadState().last;
  assert.equal(last && last.run_id, res.body.run_id);
  if (last.status !== 'done') throw new Error(`明示のロードが失敗: ${last.error}`);
  return { run_id: last.run_id, last, commit: await OS.latestLoadCommit(pdb) };
}
/** 試験の近道: active を直接書く (本番は master-ownership-epoch.mjs の prepare → activate だけ。[17] で確かめる) */
async function setActive(ownership) {
  const m = OS.sortedOwnership(ownership), h = OS.ownershipHashOf(m);
  await q(`insert into ops.master_ownership_state (id, active_hash, active_map, activated_by) values (1, $1, $2::jsonb, 'test')
    on conflict (id) do update set active_hash = excluded.active_hash, active_map = excluded.active_map, prepared_hash = null, prepared_map = null, prepared_at = null, prepared_by = null, updated_at = now()`,
  [h, JSON.stringify(m)]);
}
/**
 * 試験の近道: 切替の段階 (⑤-1 の 0051 の ops.master_cutover_state) を直接置く。段階の守り (⑤-1・⑤-2a・0055 の trigger) を通さない
 *   (session_replication_role = replica = この取引だけ trigger を止める)。本番は ops.set_master_cutover_phase だけ。守りそのものは [27] で確かめる
 */
async function setCutoverPhase(phase, ownerHash = 'a'.repeat(64)) {
  const oh = ['company_owner', 'new_open'].includes(phase) ? ownerHash : null;
  await pdb.query('begin');
  try {
    await q('set local session_replication_role = replica');
    await q(`update ops.master_cutover_state set phase = $1, owner_hash = $2 where id = 1`, [phase, oh]);
    await pdb.query('commit');
  } catch (e) { await pdb.query('rollback'); throw e; }
}
/** active を ownership にして夜間ロード (切替が済んだ後の毎晩と同じ) */
async function nightly(ownership = MASTER_OWNERSHIP) {
  await setActive(ownership);
  return loadNow();
}
const fetchGen = (ownership, extra = {}) => quietly(() => F.runPublish({ db: pdb, sqlite: db, dataDir: tmp, ownership, ...extra }));
const skuId = async (code) => (await q('select sku_id from core.skus where code = $1', [code]))[0].sku_id;
const setCost = async (code, jpy, source = 'manual', status = 'OVERRIDDEN') => q('update core.sku_costs set cost_jpy = $2, cost_source = $3, cost_status = $4 where valid_to is null and sku_id = $1', [await skuId(code), jpy, source, status]);
/** 有効な原価の行があれば直し、無ければ足す (ロードが load の回に作った行・閉じた行のどちらでも) */
async function putCost(code, jpy, source = 'manual', status = 'OVERRIDDEN') {
  if ((await q('select 1 from core.sku_costs where valid_to is null and sku_id = $1', [await skuId(code)])).length) return setCost(code, jpy, source, status);
  return q(`insert into core.sku_costs (company_id, sku_id, cost_jpy, cost_source, cost_status, valid_from) values (1, $1, $2, $3, $4, current_date)`, [await skuId(code), jpy, source, status]);
}
/** 単品の名前は SKU と商品の両方 (Company DB の入力画面と同じ。片方だけ = 写しは受け入れない) */
const setName = async (code, name, { productToo = true } = {}) => {
  await q('update core.skus set name = $2 where code = $1', [code, name]);
  if (productToo) await q('update core.products set name = $2 where product_id = (select product_id from core.skus where code = $1)', [code, name]);
};
const setHandling = async (code, h) => {
  await q('update core.skus set handling = $2 where code = $1', [code, h]);
  await q('update core.products set status = $2 where product_id = (select product_id from core.skus where code = $1)', [code, h === 'discontinued' ? 'discontinued' : 'active']);
};
/** NE にだけ単品を足す (取込が最後まで終わった状態にする) */
const addNe = (code, name, cost) => {
  db.prepare(`INSERT OR REPLACE INTO raw_ne_products (商品コード, 商品名, 仕入先コード, 原価, 売価, 取扱区分, 代表商品コード, 在庫数, 引当数, 消費税率, 作成日, synced_at)
    VALUES (?, ?, '0001', ?, 20, '取扱中', '', 0, 0, 10, '2026-09-29', ?)`).run(code, name, cost, FX.T1);
  FX.markComplete(db, readNeRawRev);
};
const evidence = () => readEvidence(tmp, jstDateStr(new Date()))[F.EVIDENCE_NAME];
/** 正規化で full と重なる 'ｆｕｌｌ' を NE から外す / 戻す (C の列がある作り直しは重なりで止まる = [7] で確かめる) */
const dropFullWidth = () => { db.prepare("DELETE FROM raw_ne_products WHERE 商品コード = 'ｆｕｌｌ'").run(); FX.markComplete(db, readNeRawRev); };
const addFullWidth = () => {
  db.prepare(`INSERT INTO raw_ne_products (商品コード, 商品名, 仕入先コード, 原価, 売価, 取扱区分, 代表商品コード, 在庫数, 引当数, 消費税率, 作成日, synced_at)
    VALUES ('ｆｕｌｌ', '全角の full', '0001', 12, 22, '取扱中', '', 3, 1, 10, '2026-01-01', ?)`).run(FX.T1);
  FX.markComplete(db, readNeRawRev);
};

await ta('[1] 持ち主が全部 load = PR の前と同じ (業務の表 7 つの全部の行・書き換えた行の数・中身・理由)。写しだけの道は 1 行も書かない・わざと壊すと見つかる', async () => {
  // (a) 世代が無い朝
  assert.equal(pointer(), null);
  const p1 = await quietly(() => FX.noopProbe(db, () => rebuildMProducts()));
  assert.equal(p1.ok, true);
  assert.deepEqual([p1.changes, p1.tables.all], [GOLDEN.first.changes, GOLDEN.first.tables_all]);
  const s1 = snap();
  assert.deepEqual([s1.products, s1.set_components, sha(s1.reasonsText), s1.reasons.length], [GOLDEN.products, GOLDEN.set_components, GOLDEN.reasons_sha, GOLDEN.reasons_n]);
  assert.equal(s1.build.cdb_publish_generation_no, null);
  assert.equal(s1.build.rule_version, MASTER_BUILD_RULE_VERSION);   // mpb-v2 (値は v1 と同じ)
  // (b) 夜間ロード (持ち主は全部 load) → 写し = 値 0 行の世代 (verified) → 作り直し = 同じ
  publishMirror();
  await nightly();
  const g = await fetchGen(MASTER_OWNERSHIP);
  assert.equal(g.state, 'verified', JSON.stringify(g.problems));
  assert.equal(g.evidence.row_count, 0); assert.equal(g.evidence.sku_count, 3124);   // ｆｕｌｌ (正規化の重なり) は Company DB に無い
  assert.equal(pointer(), g.generation_no);
  const gr = genRow(g.generation_no);
  assert.deepEqual([gr.state, gr.row_count, gr.ownership_hash], ['verified', 0, MP.ownershipHash(MASTER_OWNERSHIP)]);
  assert.match(gr.generation_id, MP.GENERATION_ID_RE);
  assert.ok(Number.isSafeInteger(gr.version_watermark) && gr.version_watermark > 0);   // 変更の記録 (events.master_change_events) の最大の番号
  const ev = evidence();
  assert.deepEqual([ev.state, ev.verdict, ev.generation_no, ev.company_owned], ['complete', 'verified', g.generation_no, []]);
  assert.equal(ev.shadow.not_in_ne, 0); assert.equal(ev.shadow.compared, 3125);   // ｆｕｌｌ は C に無いが norm が同じ full に当たる
  assert.ok(ev.shadow.by_col.name >= 2, JSON.stringify(ev.shadow));   // 名前の空欄 → コード (s-blank・set-e) = 全部 C にしたら変わる
  const p2 = await quietly(() => FX.noopProbe(db, () => rebuildMProducts()));
  assert.deepEqual([p2.ok, p2.changes, p2.tables.all], [true, GOLDEN.second.changes, GOLDEN.second.tables_all]);
  const s2 = snap();
  assert.equal(sha(s2.reasonsText), GOLDEN.reasons_sha);
  assert.equal(s2.build.cdb_publish_generation_no, g.generation_no);   // 使った (持ち主が同じ) 世代
  assert.equal(s2.build.cdb_publish_generation_id, gr.generation_id);
  // 写しだけの道 (読む・重ねる・そろえる・入れた後の確かめ・次の工程の確かめ) は、持ち主が全部 load なら 1 行も書かない
  assert.equal((await rebuild()).ok, true);   // 時計を止めない作り直し = 最新の記録 (published_at) がこの回
  const c0 = FX.totalChanges(db);
  const pubR = MP.makePublishResolver({ ownership: MASTER_OWNERSHIP, publication: MP.readCurrentPublish(db, { values: false }), staged: new Map([['s-ne', '単品']]), taxRates: TAX_RATES });
  assert.equal(pubR.active, false);
  assert.deepEqual(MP.applySideTables(db, pubR), { exception_genka: { updated: 0, deleted: 0 }, product_shipping: { updated: 0, deleted: 0 }, m_reorder_setting: { updated: 0, inserted: 0, deleted: 0 } });
  assert.equal(MP.verifyApplied(db, { publication: MP.readCurrentPublish(db), ownership: MASTER_OWNERSHIP, taxRates: TAX_RATES }).ok, true);
  const va = await F.runVerifyApply({ sqlite: db, dataDir: tmp, ownership: MASTER_OWNERSHIP, write: () => true, taxRates: TAX_RATES });
  assert.equal(va.state, 'verified', JSON.stringify(va.problems));
  // 書くのは門の 1 行だけ (確かめた作り直しの safe。全部 load でも書く = #1564 Codex R4 Medium 1)。業務の表は 1 行も書かない
  assert.equal(FX.totalChanges(db) - c0, 1);
  assert.equal(db.prepare('SELECT state FROM cdb_publish_gate WHERE id = 1').get().state, 'safe');
  db.prepare('DELETE FROM cdb_publish_gate').run();
  // わざと壊す: 値を変えない UPDATE (無条件の書き換え) でも行の数で見つかる。ハッシュだけでは見えない (だから両方を見る)
  const mut = await quietly(() => FX.noopProbe(db, async () => { const r = await rebuildMProducts(); db.prepare('UPDATE product_tax_rate SET tax_rate = tax_rate').run(); return r; }));
  assert.notEqual(mut.changes, GOLDEN.second.changes);
  assert.equal(mut.tables.all, GOLDEN.second.tables_all);
  const mut2 = await quietly(() => FX.noopProbe(db, async () => { const r = await rebuildMProducts(); db.prepare("UPDATE m_reorder_setting SET synced_at = 'y' WHERE sku = 's-ne'").run(); return r; }));
  assert.notEqual(mut2.tables.all, GOLDEN.second.tables_all);
  db.prepare("UPDATE m_reorder_setting SET synced_at = 'x' WHERE sku = 's-ne'").run();
  // (c) ④a で写さない列 (products.parent) を company にした・一緒に切り替える組の片方だけ = 扱えない = 作り直しを止める (何も書かない)
  assert.deepEqual(MP.checkPublishOwnership(OWN('products.parent')), ['not_copied:products.parent']);
  assert.deepEqual(MP.checkPublishOwnership(OWN('skus.name')), ['co_switch:products.name+skus.name']);
  assert.deepEqual(MP.checkPublishOwnership(OWN('skus.tax_rate', 'products.status')), ['co_switch:products.status+skus.handling', 'co_switch:skus.tax_rate+skus.tax_class']);
  assert.deepEqual(MP.checkPublishOwnership(OWN('skus.name', 'products.name', 'skus.tax_rate', 'skus.tax_class')), []);
  assert.throws(() => MP.assertPublishOwnership(OWN('products.parent')), /④a で扱えない/);
  const t0 = FX.tablesDigest(db).all, nBuilds = count('m_products_builds');
  for (const bad of [OWN('products.parent'), OWN('skus.name')]) {
    const r = await rebuild(bad);
    assert.deepEqual([r.ok, r.error, r.problem], [false, 'CDB_PUBLISH_UNAVAILABLE', 'ownership_not_supported']);
  }
  assert.deepEqual([FX.tablesDigest(db).all, count('m_products_builds')], [t0, nBuilds]);   // 業務の表も記録も触らない
  // この後の試験 (持ち主が C の列がある) のために、正規化で full と重なる ｆｕｌｌ を NE から外して作り直す (重なりは [7] で)
  dropFullWidth();
  assert.equal((await rebuild()).ok, true);
});

await ta('[2] NE の原価が 0 より大きくても C が勝つ・原価の出どころの逆向き (manual・0 円の override_zero・imported → 例外)・セットは C の構成品から導く', async () => {
  const own = OWN('sku_costs');
  await nightly(own);
  await setCost('s-ne', 150);                                         // NE は 100
  await setCost('s-tax8', 0, 'override_zero', 'OVERRIDDEN');          // 0 円に決めた
  await setCost('s-sc3', 45, 'imported', 'COMPLETE');                 // 外から入れた (金額は NE と同じ)
  const g = await fetchGen(own);
  assert.equal(g.state, 'verified', JSON.stringify(g.problems));
  assert.deepEqual(g.evidence.company_owned, ['cost']);
  assert.equal(g.evidence.row_count, 3124 * 2);   // 原価の欄 + C にある SKU の印 (_sku)
  const r = await rebuild(own);
  assert.equal(r.ok, true, JSON.stringify(r.checks));
  assert.equal(r.publish.generation_no, g.generation_no);
  assert.equal(r.publish.applied.ok, true);
  assert.ok(r.publish.applied.counts.derived > 0);   // セットの set_calc の原価は構成品から導く (比べない)
  const s = snap();
  assert.equal(s.build.cdb_publish_generation_no, g.generation_no);
  assert.match(s.build.cdb_publish_applied_hash, /^[0-9a-f]{64}$/);
  const cost = (c) => [mp(c).原価, mp(c).原価ソース, mp(c).原価状態];
  assert.deepEqual(cost('s-ne'), [150, '例外', 'OVERRIDDEN']);
  assert.deepEqual(cost('s-tax8'), [0, '例外', 'OVERRIDDEN']);        // 0 円は空ではない
  assert.deepEqual(cost('s-sc3'), [45, '例外', 'COMPLETE']);
  const co = (code) => reasonsOf(s, code).filter((x) => x.reason === 'company_owned').map((x) => [x.col, x.value, x.ne_value, x.cdb_cost_source]);
  assert.deepEqual(co('s-ne'), [['cost', { 原価: 150, 原価ソース: '例外', 原価状態: 'OVERRIDDEN' }, { 原価: 100, 原価ソース: 'NE', 原価状態: 'COMPLETE' }, 'manual']]);
  assert.deepEqual(co('s-tax8').map((x) => x[3]), ['override_zero']);
  assert.deepEqual(co('s-sc3').map((x) => x[3]), ['imported']);
  // セット: 構成品の入力を C の値に替えて今の決め方 (set-a = s-ne 150 × 2 + s-exc 555 (C の例外原価) = 855。NE の道は s-exc の NE 原価 0 で PARTIAL)
  assert.deepEqual(cost('set-a'), [855, 'セット計算', 'COMPLETE']);
  assert.deepEqual(reasonsOf(s, 'set-a').map((x) => [x.col, x.reason]), [['cost', 'company_owned']]);
  assert.deepEqual(cost('set-b'), [null, '不明', 'PARTIAL']);        // 構成品の 0 円は「無い」と同じ (今の決め方)
  assert.deepEqual(cost('set-g'), [90, 'セット計算', 'COMPLETE']);   // NE の道と同じ = 理由を変えない
  assert.equal(reasonsOf(s, 'set-g').length, 0);
  // セット自身の人の決めた原価 (C の manual = 例外原価 777) は C の値のまま。理由は今までの exception_cost
  assert.deepEqual(cost('set-d'), [777, '例外', 'OVERRIDDEN']);
  assert.deepEqual(reasonsOf(s, 'set-d').map((x) => x.reason), ['exception_cost']);
  // 構成品の行の原価も C の値 (0 円は NULL = 今の決め方)
  const comp = db.prepare('SELECT 構成商品原価 AS c FROM m_set_components WHERE セット商品コード = ? AND 構成商品コード = ?');
  assert.deepEqual([comp.get('set-a', 's-ne').c, comp.get('set-a', 's-exc').c, comp.get('set-b', 's-tax8').c], [150, 555, null]);
  // 持ち主が load の列は今までどおり
  assert.deepEqual([mp('s-ne').商品名, mp('s-ne').消費税率, mp('s-ne').税区分], ['NE の単品', 0.1, 'STANDARD_10']);
  // 例外原価の行は値が同じ = 触らない。行は作らない
  assert.equal(count('exception_genka', "sku = 's-ne'"), 0);
  assert.equal(db.prepare("SELECT synced_at FROM exception_genka WHERE sku = 's-exc'").get().synced_at, 'x');
});

await ta('[3] C の原価が空 = 原価状態 MISSING・原価 NULL で作り直しは止まらない (例外原価の行は触らない)', async () => {
  const own = OWN('sku_costs');
  await q('update core.sku_costs set valid_to = valid_from where valid_to is null and sku_id = any($1::bigint[])', [[await skuId('s-ne'), await skuId('s-exc')]]);
  const g = await fetchGen(own);
  assert.equal(g.state, 'verified', JSON.stringify(g.problems));
  const r = await rebuild(own);
  assert.equal(r.ok, true, JSON.stringify(r.checks));   // B6 (OVERRIDDEN なのに原価 NULL) で止まらない
  assert.deepEqual(['s-ne', 's-exc'].map((c) => [mp(c).原価, mp(c).原価ソース, mp(c).原価状態]), [[null, '不明', 'MISSING'], [null, '不明', 'MISSING']]);
  assert.equal(mp('set-a').原価状態, 'MISSING');   // 構成品の原価が全部無い
  // 例外原価の行は「空」を行の無さで表す (古い値を残さない = 財務の SQL が古い原価を使わない・持ち主を load に戻しても B6 で止まらない。Codex R1 H3)
  assert.equal(count('exception_genka', "sku = 's-exc'"), 0);
  assert.deepEqual(r.publish.side_tables.exception_genka, { updated: 0, deleted: 1 });
  const s = snap();
  assert.deepEqual(reasonsOf(s, 's-exc').map((x) => [x.reason, x.value.原価, x.ne_value.原価, x.cdb_cost_source, x.owner_key, x.cdb_value, x.generation_no]),
    [['company_owned', null, 555, null, 'sku_costs', { cost: null }, g.generation_no]]);
  assert.equal(r.checks.some((c) => c.startsWith('❌')), false);
  // 同じ世代をもう一度入れる = 上書き表は何も変わらない (時刻も)
  const side0 = FX.tablesDigest(db, ['exception_genka', 'product_shipping', 'm_reorder_setting']).all;
  const r2 = await rebuild(own);
  assert.deepEqual(r2.publish.side_tables, { exception_genka: { updated: 0, deleted: 0 }, product_shipping: { updated: 0, deleted: 0 }, m_reorder_setting: { updated: 0, inserted: 0, deleted: 0 } });
  assert.equal(FX.tablesDigest(db, ['exception_genka', 'product_shipping', 'm_reorder_setting']).all, side0);
});

await ta('[4] 上書き表: exception_genka / product_shipping は既にある行だけ・m_reorder_setting はこの作り直しの SKU を入れる / 直す / 消す', async () => {
  const own = OWN('sku_costs', 'skus.shipping', 'skus.reorder_months');
  await nightly(own);
  db.prepare("INSERT OR REPLACE INTO exception_genka (sku, genka, 商品名, synced_at) VALUES ('s-exc', 555, NULL, 'x')").run();   // [3] で消えた行を戻す (直す行)
  await putCost('s-exc', 600);
  await q(`update core.skus set shipping_code = 'S9', shipping_method = 'ゆうパケット', shipping_cost_jpy = 250 where code = 's-ship'`);
  await q(`update core.skus set shipping_code = null, shipping_method = null, shipping_cost_jpy = null where code = 'ex-only'`);   // C で送料を全部空 = 行を消す
  await q(`update core.skus set reorder_months = 5 where code = 's-ne'`);    // 行あり = 直す
  await q(`update core.skus set reorder_months = 4 where code = 's-tax8'`);  // 行なし = 入れる (商品管理リストの snapshot が直接読む)
  await q(`update core.skus set reorder_months = null where code = 'set-a'`); // C で未登録 = 行を消す
  const g = await fetchGen(own);
  assert.equal(g.state, 'verified', JSON.stringify(g.problems));
  const before = { eg: count('exception_genka'), ps: count('product_shipping') };
  const r = await rebuild(own);
  assert.equal(r.ok, true, JSON.stringify(r.checks));
  assert.deepEqual(r.publish.side_tables, { exception_genka: { updated: 1, deleted: 0 }, product_shipping: { updated: 1, deleted: 1 }, m_reorder_setting: { updated: 1, inserted: 1, deleted: 1 } });
  assert.equal(count('product_shipping', "sku = 'ex-only'"), 0);
  assert.deepEqual([mp('ex-only').送料, mp('ex-only').送料コード, mp('ex-only').配送方法], [null, null, null]);
  assert.equal(mp('s-exc').原価, 600);
  assert.equal(db.prepare("SELECT genka FROM exception_genka WHERE sku = 's-exc'").get().genka, 600);
  assert.deepEqual(db.prepare("SELECT shipping_code, ship_method, ship_cost FROM product_shipping WHERE sku = 's-ship'").get(), { shipping_code: 'S9', ship_method: 'ゆうパケット', ship_cost: 250 });
  assert.deepEqual([mp('s-ship').送料コード, mp('s-ship').配送方法, mp('s-ship').送料], ['S9', 'ゆうパケット', 250]);
  const rs = (sku) => db.prepare('SELECT 推奨保有月数 AS m, updated_by AS by, 商品名 AS name FROM m_reorder_setting WHERE sku = ?').get(sku);
  assert.deepEqual(rs('s-ne'), { m: 5, by: 'company_db', name: 'NE の単品' });
  assert.deepEqual(rs('s-tax8'), { m: 4, by: 'company_db', name: '軽減税率の単品' });
  assert.equal(rs('set-a'), undefined);
  assert.deepEqual({ eg: count('exception_genka'), ps: count('product_shipping') }, { eg: before.eg, ps: before.ps - 1 });   // 例外原価・送料の行は作らない (空 = 消す)
  // 代表の行 (rep-x = 名札。SKU ではない) は触らない。代表から継いだ s-inh は C の値 (= 継いだ値) のまま
  assert.deepEqual(db.prepare("SELECT shipping_code, ship_cost FROM product_shipping WHERE sku = 'rep-x'").get(), { shipping_code: 'S2', ship_cost: 800 });
  assert.deepEqual([mp('s-inh').送料コード, mp('s-inh').送料], ['S2', 800]);
  assert.equal(count('exception_genka', "sku = 's-ne'"), 0);   // C の原価が空の s-ne は例外原価の行を作らない
  assert.equal(r.publish.applied.ok, true);
});

await ta('[5] Company DB にしか無い SKU は m_products にも m_reorder_setting にも足さない (not_in_ne)', async () => {
  const own = OWN('skus.name', 'products.name', 'skus.reorder_months');
  await nightly(own);
  const pid = (await q(`insert into core.products (company_id, display_code, name) values (1, 'cdb-only', 'C だけの商品') returning product_id`))[0].product_id;
  await q(`insert into core.skus (company_id, product_id, sku_kind, code, name, reorder_months) values (1, $1, 'single', 'cdb-only', 'C だけの商品', 3)`, [pid]);
  const g = await fetchGen(own);
  assert.equal(g.state, 'verified', JSON.stringify(g.problems));
  assert.equal(g.evidence.shadow.not_in_ne, 1);
  const nMp = count('m_products');
  const r = await rebuild(own);
  assert.equal(r.ok, true, JSON.stringify(r.checks));
  assert.equal(mp('cdb-only'), undefined);
  assert.equal(count('m_reorder_setting', "sku = 'cdb-only'"), 0);
  assert.equal(r.publish.stats.not_in_ne, 1);
  assert.equal(r.publish.applied.counts.not_in_ne, 1);
  assert.equal(count('m_products'), nMp);
});

await ta('[6] 世代が古い・後退・途中で落ちた・検証で落ちた・商品名が SKU と違う = 印は動かない = 作り直しは前の世代', async () => {
  const own = OWN('skus.name', 'products.name');
  await nightly(own);
  await setName('s-ne', 'C で直した名前');
  const good = await fetchGen(own);
  assert.equal(good.state, 'verified', JSON.stringify(good.problems));
  const cur = pointer();
  assert.equal(cur, good.generation_no);
  // (a) 途中で落ちた (値を入れた後・印を進める前) = 全部巻き戻る・理由つきの行だけ残る
  await setName('s-ne', '落ちた回の名前');
  await assert.rejects(fetchGen(own, { beforeCommit: () => { throw new Error('途中で落ちた'); } }), /途中で落ちた/);
  assert.equal(pointer(), cur);
  const failedRow = db.prepare('SELECT * FROM cdb_publish_generations ORDER BY generation_no DESC LIMIT 1').get();
  assert.deepEqual([failedRow.state, failedRow.reason], ['rejected', 'stage_failed:error']);
  assert.equal(count('cdb_publish_values', 'generation_no = ?', failedRow.generation_no), 0);
  assert.equal(evidence().state, 'failed');
  // (b) 検証で落ちた (値の範囲: C の名前が空白だけ) / 単品の商品名が SKU と違う (片方だけ直した)
  await setName('s-ne', '   ');
  assert.deepEqual((await fetchGen(own)).problems, ['value_out_of_range']);
  await setName('s-ne', 'SKU だけ直した名前', { productToo: false });
  const pn = await fetchGen(own);
  assert.deepEqual([pn.state, pn.problems, pn.evidence.detail.products.name_mismatch], ['rejected', ['product_name_mismatch'], 1]);
  assert.equal(pointer(), cur);
  // (c) 古い (今の世代の時刻が先) / (d) 後退 (変更の記録の番号が下がった = 復元を疑う)
  await setName('s-ne', '古い世代の後の名前');
  const saved = genRow(cur);
  db.prepare("UPDATE cdb_publish_generations SET cdb_read_at = '2999-01-01T00:00:00.000000Z' WHERE generation_no = ?").run(cur);
  assert.deepEqual((await fetchGen(own)).problems, ['not_newer']);
  db.prepare('UPDATE cdb_publish_generations SET cdb_read_at = ?, version_watermark = ? WHERE generation_no = ?').run(saved.cdb_read_at, 1e12, cur);
  assert.deepEqual((await fetchGen(own)).problems, ['watermark_backward', 'watermark_fork']);   // 前の水位の出来事 (1e12) も無い
  db.prepare('UPDATE cdb_publish_generations SET version_watermark = ? WHERE generation_no = ?').run(saved.version_watermark, cur);
  // (e) 歴史の分かれ: 前の水位の出来事 (event_id) の中身が違う = 復元・別の DB を疑う (番号が下がらなくても見つける)
  assert.match(saved.watermark_fingerprint, /^[0-9a-f]{64}$/);
  db.prepare('UPDATE cdb_publish_generations SET watermark_fingerprint = ? WHERE generation_no = ?').run('f'.repeat(64), cur);
  const fork = await fetchGen(own);
  assert.deepEqual([fork.problems, fork.evidence.detail.watermark_fork.found], [['watermark_fork'], 'different']);
  db.prepare('UPDATE cdb_publish_generations SET watermark_fingerprint = ? WHERE generation_no = ?').run(saved.watermark_fingerprint, cur);
  assert.equal(pointer(), cur);
  // 作り直しは前の (受け入れた) 世代 = 「C で直した名前」
  const r = await rebuild(own);
  assert.equal(r.ok, true, JSON.stringify(r.checks));
  assert.equal(mp('s-ne').商品名, 'C で直した名前');
  assert.equal(latestBuild(db).cdb_publish_generation_no, cur);
  // 数の減り (単位の確かめ): SKU の数 / 同じ持ち主 (epoch) のときだけ値の行数
  const src = { load: { ingest_run_id: 'x' }, loadOwnership: null, skus: [] };
  const base = { ownership_hash: 'h', rows: [], problems: [], cols: [], version_watermark: 10, cdb_read_at: '2026-09-30T00:00:01.000000Z' };
  const prev = { ownership_hash: 'h', sku_count: 100, row_count: 100, version_watermark: 10, cdb_read_at: '2026-09-30T00:00:00.000000Z' };
  assert.ok(F.verifyGeneration({ prev, next: { ...base, sku_count: 89, row_count: 100 }, source: src }).problems.includes('shrunk'));
  assert.ok(F.verifyGeneration({ prev, next: { ...base, sku_count: 100, row_count: 89 }, source: src }).problems.includes('shrunk'));
  assert.ok(!F.verifyGeneration({ prev, next: { ...base, ownership_hash: 'other', sku_count: 100, row_count: 0 }, source: src }).problems.includes('shrunk'));
  assert.ok(F.verifyGeneration({ prev, next: { ...base, cols: ['name'], sku_count: 100, row_count: 100, version_watermark: null }, source: src }).problems.includes('no_version'));
  // 持ち主が全部 load (値 0 行) = 値の前提が欠けても受け入れる (止めない・detail に残す = 毎朝の fail の ping にしない。#1564 の見直し L-5)
  const soft = F.verifyGeneration({ prev, next: { ...base, sku_count: 100, row_count: 100, version_watermark: null },
    source: { ...src, skus: [{ sku_kind: 'single', name: 'a', product_name: 'a', product_status: 'active', handling: 'active', product_id: null },
      { sku_kind: 'single', name: 'b', product_name: 'b', product_status: 'active', handling: 'active', product_id: '9' }, { sku_kind: 'single', name: 'c', product_name: 'c', product_status: 'active', handling: 'active', product_id: '9' }] } });
  assert.deepEqual([soft.problems, soft.detail.ignored_all_load], [[], ['no_load_ownership', 'no_version', 'single_without_product', 'product_shared']]);
  const hard = F.verifyGeneration({ prev, next: { ...base, cols: ['cost'], sku_count: 100, row_count: 100, version_watermark: null }, source: src });
  assert.ok(['no_load_ownership', 'no_version'].every((p) => hard.problems.includes(p)), JSON.stringify(hard.problems));   // 持ち主が C の列がある = 止める
  await setName('s-ne', 'C で直した名前');
});

await ta('[7] 止める: 持ち主の設定が違う・世代が無い・値が欠ける・m_products の 2 つのコードが同じ C の SKU に当たる = 入れ替えない (前の m_products のまま)', async () => {
  // 写し: 夜間ロードが記録した持ち主 (skus.name・products.name) と Company DB の active (sku_costs も) が違う = 受け入れない (config は関係ない)
  const was = (await OS.readOwnershipState(pdb)).active.map;
  await setActive(OWN('skus.name', 'products.name', 'sku_costs'));
  const g = await fetchGen(MASTER_OWNERSHIP);
  assert.deepEqual([g.state, g.problems], ['rejected', ['ownership_mismatch']]);
  assert.ok(g.evidence.detail.ownership.local !== g.evidence.detail.ownership.products);
  await setActive(was);
  const stopped = async (ownership, problem) => {
    const before = snap();
    const r = await rebuild(ownership);
    assert.deepEqual([r.ok, r.error, r.problem], [false, 'CDB_PUBLISH_UNAVAILABLE', problem], JSON.stringify(r.checks));
    const after = snap();
    assert.deepEqual([after.products, after.build.build_id], [before.products, before.build.build_id]);   // 入れ替えない・記録も書かない
  };
  // 作り直し: 手元に C の列があるのに、今の世代の持ち主が違う (別の epoch の世代は使わない)
  await stopped(OWN('skus.handling', 'products.status'), 'ownership_mismatch');
  // 世代が 1 つも無い (印が無い)
  const saved = pointer();
  db.prepare('DELETE FROM sync_meta WHERE key = ?').run(MP.PUBLISH_CURRENT_KEY);
  await stopped(OWN('skus.name', 'products.name'), 'no_generation');
  // 本番 (持ち主を渡さない): 前の作り直しが持ち主が C の列を使った = 世代が無くても NE の値に黙って戻さない (config では決めない。Codex #1564 R1 H1)
  assert.notEqual(snap().build.cdb_publish_ownership_hash, MP.ownershipHash(MASTER_OWNERSHIP));
  await stopped(null, 'no_generation');
  db.prepare("INSERT INTO sync_meta (key, value, updated_at) VALUES (?, ?, '')").run(MP.PUBLISH_CURRENT_KEY, String(saved));
  // 正規化の重なり: NE に full と ｆｕｌｌ (同じ norm)・C は full だけ = 1 つの C の値を 2 つのコードに配らない
  addFullWidth();
  await stopped(OWN('skus.name', 'products.name'), 'target_norm_collision');
  dropFullWidth();
  // 一緒に切り替える組の片方だけ・写さない列が company = 扱えない
  await stopped(OWN('skus.name'), 'ownership_not_supported');
  // 種類が違う (写しの時): C で単品 s-sc3 の種類がセット (Company DB に売上分類の置き場所が無い) = その SKU だけ写さない (受け入れる・証跡に出す)。
  //   前は incomplete で全部を止めた = 1 行の食い違いで全部を止めない (#1564 の見直し M-5。食い違いそのものは照合 ② の kind で出る)
  const ownSc = OWN('products.sales_class');
  await nightly(ownSc);
  await q(`update core.skus set sku_kind = 'set' where code = 's-sc3'`);
  const inc = await fetchGen(ownSc);
  assert.deepEqual([inc.state, inc.problems, inc.evidence.kind_mismatch], ['verified', [], { count: 1, codes: ['s-sc3'] }]);
  // 種類が違う (作り直しの時): C ではセットの s-late が今朝 NE に単品で出た = その SKU だけ NE の値 (前は value_missing で全部を止めた)
  await nightly(ownSc);   // s-sc3 の種類は材料どおり (単品) に戻る
  await q(`insert into core.skus (company_id, sku_kind, code, name) values (1, 'set', 's-late', 'C ではセット')`);
  assert.equal((await fetchGen(ownSc)).state, 'verified');
  addNe('s-late', 'NE では単品', 10);
  const late = await rebuild(ownSc);
  assert.deepEqual([late.ok, late.publish.stats.kind_mismatch_codes, mp('s-late').商品区分], [true, ['s-late'], '単品'], JSON.stringify(late.checks));
  // 種類は同じなのに要る欄が無い (世代が壊れた) = 止める (value_missing。NE の値で黙って埋めない)
  const pubX = MP.makePublishResolver({ ownership: ownSc, publication: { generation: genRow(pointer()), entries: new Map([['s-sc', { code: 's-sc', kind: 'single', v: {} }]]), problem: null },
    staged: new Map([['s-sc', '単品']]), taxRates: TAX_RATES });
  assert.deepEqual([pubX.problem, pubX.problemDetail.samples], ['value_missing', [['s-sc', 'sales_class']]]);
  db.prepare("DELETE FROM raw_ne_products WHERE 商品コード = 's-late'").run(); FX.markComplete(db, readNeRawRev);
  assert.equal((await rebuild(ownSc)).ok, true);
  // 持ち主が全部 load に戻れば (世代の持ち主が違っても) 今までどおり作り直す
  const r3 = await rebuild();
  assert.equal(r3.ok, true);
  assert.equal(mp('s-ne').商品名, 'NE の単品');
});

await ta('[8] 由来が消えない・写しの世代が由来に載る・Render に送ると材料は matched・C の値はロードの後も残る・2 回目も同じ', async () => {
  const own = OWN('sku_costs', 'skus.name', 'products.name');
  await nightly(own);   // s-sc3 の種類は材料どおり (単品) に戻る
  await putCost('s-ne', 150);
  await setName('s-ne', 'C の名前');
  const g = await fetchGen(own);
  assert.equal(g.state, 'verified', JSON.stringify(g.problems));
  const r = await rebuild(own);
  assert.equal(r.ok, true, JSON.stringify(r.checks));
  assert.deepEqual([mp('s-ne').商品名, mp('s-ne').原価], ['C の名前', 150]);
  const first = snap();
  // 作り直しの記録と送る中身が同じ = 由来が付く (後から UPDATE していない)。由来に写しの世代
  const lin = readMaterialWithLineage(db).lineage;
  assert.equal(lin.build_id, first.build.build_id, JSON.stringify(lin));
  assert.deepEqual([lin.cdb_publish.generation_no, lin.cdb_publish.generation_id, lin.cdb_publish.applied_hash],
    [g.generation_no, genRow(g.generation_no).generation_id, first.build.cdb_publish_applied_hash]);
  const mg = buildMaterialGeneration({ products: [], set_components: [], build: lin });
  assert.equal(mg.build.cdb_publish.generation_no, g.generation_no);   // 材料の世代 (Render へ) にも
  assert.equal(masterReceiptEvidence({ generation: mg, lineage: lin, masterPart: {}, response: { ok: true } }).cdb_publish.generation_no, g.generation_no);   // Render 到達の証跡にも
  // Render の mirror に送る → 夜間ロード (持ち主は同じ) → 材料は matched・C の値はそのまま
  publishMirror();
  const lr = await nightly(own);
  assert.deepEqual([lr.material.products.status, lr.material.set_components.status], ['matched', 'matched']);
  const c = (await q(`select s.name, p.name as pname, c.cost_jpy::int as cost, c.cost_source from core.skus s join core.products p on p.product_id = s.product_id
    join core.sku_costs c on c.sku_id = s.sku_id and c.valid_to is null where s.code = 's-ne'`))[0];
  assert.deepEqual(c, { name: 'C の名前', pname: 'C の名前', cost: 150, cost_source: 'manual' });
  // もう一度 写し → 作り直し = 同じ中身
  assert.equal((await fetchGen(own)).state, 'verified');
  assert.equal((await rebuild(own)).ok, true);
  const second = snap();
  assert.deepEqual([second.products, second.set_components], [first.products, first.set_components]);
});

await ta('[9] 取扱区分の語 (NE の語が同じ区分なら残す・持ち主を替えただけでは語を変えない)', async () => {
  assert.deepEqual([
    MP.handlingFromCdb('active', '取扱中'), MP.handlingFromCdb('active', '取扱中止'), MP.handlingFromCdb('discontinued', 'ﾒｰｶｰ取扱中止'),
    MP.handlingFromCdb('discontinued', '取扱中'), MP.handlingFromCdb('discontinued', null), MP.handlingFromCdb('unknown', '取扱中'), MP.handlingFromCdb('unknown', ''),
  ], ['取扱中', '取扱中', 'ﾒｰｶｰ取扱中止', '取扱中止', '取扱中止', null, '']);
  assert.deepEqual([MP.supplierFromCdb('0001', '1'), MP.supplierFromCdb('0002', '1'), MP.supplierFromCdb(null, '1')], ['1', '0002', null]);
  assert.deepEqual(Object.entries(MP.COST_SOURCE_TO_M), [['ne', 'NE'], ['set_calc', 'セット計算'], ['manual', '例外'], ['override_zero', '例外'], ['imported', '例外']]);
  const own = OWN('skus.handling', 'products.status');
  await nightly(own);
  // 持ち主を替えただけ (C の値 = 材料の値) なら取扱区分は今までと同じ語 (単品の ﾒｰｶｰ取扱中止・セットの導いた ﾒｰｶｰ取扱中止)
  const g9 = await fetchGen(own);
  assert.equal(g9.state, 'verified', JSON.stringify([g9.problems, g9.evidence && g9.evidence.detail]));
  assert.equal((await rebuild(own)).ok, true);
  assert.deepEqual(['s-maker', 's-stop', 'set-h', 'set-c'].map((c) => mp(c).取扱区分), ['ﾒｰｶｰ取扱中止', '取扱中止', 'ﾒｰｶｰ取扱中止', '取扱中止']);
  // C で s-ne を止める・s-stop を戻す → 単品はその語・構成品に s-ne を持つ set-a は構成品から止まる
  await setHandling('s-ne', 'discontinued');
  await setHandling('s-stop', 'active');
  assert.equal((await fetchGen(own)).state, 'verified');
  const r = await rebuild(own);
  assert.equal(r.ok, true, JSON.stringify(r.checks));
  assert.deepEqual(['s-ne', 's-stop', 's-maker', 'set-a'].map((c) => mp(c).取扱区分), ['取扱中止', '取扱中', 'ﾒｰｶｰ取扱中止', '取扱中止']);
  const s = snap();
  assert.deepEqual(reasonsOf(s, 'set-a').filter((x) => x.col === 'handling').map((x) => [x.value, x.ne_value]), [['取扱中止', '取扱中']]);
  // 商品の状態を片方だけ変えた = 受け入れない
  await q(`update core.skus set handling = 'active' where code = 's-ne'`);
  assert.deepEqual((await fetchGen(own)).problems, ['product_status_mismatch']);
  await setHandling('s-ne', 'active');
  // 正規化の重なり (世代の中): 同じ norm に 2 つの書き方・norm がコードと合わない = 受け入れない
  const row = (code, norm, col = 'name') => ({ code, code_norm: norm, col, sku_kind: 'single', value: JSON.stringify('x') });
  const src = { load: { ingest_run_id: 'x' }, loadOwnership: null, skus: [] };
  const next = (rows) => ({ rows, problems: [], cols: [], ownership_hash: 'h', version_watermark: 1, sku_count: 1, row_count: rows.length, cdb_read_at: 'z' });
  assert.ok(F.verifyGeneration({ prev: null, next: next([row('abc', 'abc'), row('ABC', 'abc')]), source: src }).problems.includes('norm_collision'));
  assert.ok(F.verifyGeneration({ prev: null, next: next([row('abc', 'abd')]), source: src }).problems.includes('norm_mismatch'));
  assert.ok(!F.verifyGeneration({ prev: null, next: next([row('ＡＢＣ', 'abc')]), source: src }).problems.includes('norm_mismatch'));
});

await ta('[10] わざと壊す: 世代の値の書き換え (ハッシュ)・入れた後に m_products が世代と違う = 巻き戻す・次の工程の確かめも見つける・値の範囲', async () => {
  const own = OWN('skus.handling', 'products.status');
  assert.equal((await fetchGen(own)).state, 'verified');
  assert.equal((await rebuild(own)).ok, true);
  const cur = pointer();
  const before = snap();
  db.prepare("UPDATE cdb_publish_values SET value = '\"active\"' WHERE generation_no = ? AND code_norm = 's-maker' AND col = 'handling'").run(cur);
  assert.equal(MP.readCurrentPublish(db).problem, 'hash_mismatch');
  const r = await rebuild(own);
  assert.deepEqual([r.ok, r.error, r.problem], [false, 'CDB_PUBLISH_UNAVAILABLE', 'hash_mismatch']);
  assert.deepEqual(snap().products, before.products);
  assert.equal((await fetchGen(own)).state, 'verified');   // 新しい世代を取り直せば直る
  // 入れた後の確かめ (取引の中): 入れた後に m_products が変わる (ここでは TEMP のトリガーで) = 世代と違う = 巻き戻す
  db.exec(`CREATE TEMP TRIGGER t_break AFTER INSERT ON main.m_products WHEN NEW.商品コード = 's-maker' BEGIN
    UPDATE m_products SET 取扱区分 = '取扱中' WHERE 商品コード = 's-maker'; END`);
  let r2;
  try { r2 = await rebuild(own); } finally { db.exec('DROP TRIGGER IF EXISTS temp.t_break'); }
  assert.deepEqual([r2.ok, r2.error], [false, 'CDB_PUBLISH_VERIFY'], JSON.stringify(r2.checks));
  assert.deepEqual(r2.publish.applied.problems.map((x) => [x.code, x.col]), [['s-maker', 'handling']]);
  assert.deepEqual(snap().products, before.products);   // 巻き戻った
  assert.equal((await rebuild(own)).ok, true);
  // 次の工程の確かめ: 作り直しの後に m_products が書き換えられた = 見つける (fail)
  const fetched = () => ({ [F.EVIDENCE_NAME]: { state: 'complete', verdict: 'verified', generation_no: pointer(), sync_run_id: 'ds_test_publish' } });
  const ok = await F.runVerifyApply({ sqlite: db, dataDir: tmp, ownership: own, write: () => true, read: fetched, taxRates: TAX_RATES });
  assert.equal(ok.state, 'verified', JSON.stringify(ok.problems));
  db.prepare("UPDATE m_products SET 取扱区分 = '取扱中' WHERE 商品コード = 's-maker'").run();
  const ng = await F.runVerifyApply({ sqlite: db, dataDir: tmp, ownership: own, write: () => true, read: fetched, taxRates: TAX_RATES });
  assert.deepEqual([ng.problems, ng.broken], [['applied_mismatch', 'applied_hash_changed'], true]);   // broken = daily-sync は snapshot・Render同期 を止める (exit 4)
  const x4 = await quietly(() => F.cli(['--verify-apply', '--daily'], { env: { DATA_DIR: tmp }, log: quiet, ping: async () => {}, openSqlite: async () => db, ownership: own,
    verify: async () => ng }));
  assert.equal(x4.code, F.EXIT.applied_broken);
  assert.equal((await rebuild(own)).ok, true);
  // 値の範囲
  const V = MP.validPublishValue;
  assert.deepEqual([V('cost', { jpy: 1, source: 'manual', status: 'COMPLETE' }), V('cost', { jpy: 0, source: 'override_zero', status: 'OVERRIDDEN' }), V('cost', { jpy: -1, source: 'manual', status: 'COMPLETE' }),
    V('cost', { jpy: 1.5, source: 'ne', status: 'COMPLETE' }), V('cost', { jpy: 1, source: 'x', status: 'COMPLETE' }), V('cost', null)], [true, true, false, false, false, true]);
  assert.deepEqual([V('tax_rate', 0.1), V('tax_rate', 0.12), V('sales_class', 4), V('sales_class', 5), V('reorder_months', 60), V('reorder_months', 61), V('handling', 'active'), V('handling', null),
    V('shipping', { code: null, method: null, cost_jpy: null }), V('shipping', null), V('name', ''), V('primary_supplier', { multiple: ['a', 'b'] }), V('nope', 1)],
  [true, false, true, false, true, false, true, false, true, false, false, false, false]);
  assert.throws(() => MP.costFromCdb({ jpy: 1, source: 'nope', status: 'COMPLETE' }), /知らない原価の出どころ/);
});

await ta('[11] 世代は 14 個残す (今の世代は消さない)・印は前にしか進まない・rejected は印を動かさない', async () => {
  const mk = (i) => ({ generation_id: `cpg_x_${i}`, cdb_read_at: `2026-09-30T00:00:${String(i).padStart(2, '0')}.000000Z`,
    version_watermark: 1, load_run_id: null, ownership: {}, ownership_hash: 'h', rows: [], row_count: 0, sku_count: 0, content_hash: MP.publishContentHash([]) });
  // 印を大きな番号にしておく = 新しい世代の番号が小さければ印は動かない (前にしか進まない)
  const saved = pointer();
  db.prepare('UPDATE sync_meta SET value = ? WHERE key = ?').run('999999', MP.PUBLISH_CURRENT_KEY);
  assert.equal(F.stageGeneration(db, mk(1)).moved, false);
  assert.equal(pointer(), 999999);
  db.prepare('UPDATE sync_meta SET value = ? WHERE key = ?').run(String(saved), MP.PUBLISH_CURRENT_KEY);
  // verified を 1 つ → その後 rejected を 16 = 今の世代は新しい 14 個より古いが消さない
  const v = F.stageGeneration(db, mk(2));
  assert.equal(v.moved, true);
  for (let i = 3; i <= 18; i++) F.stageGeneration(db, mk(i), { state: 'rejected', reason: 'test' });
  assert.equal(pointer(), v.generation_no);
  const all = db.prepare('SELECT generation_no FROM cdb_publish_generations ORDER BY generation_no').all().map((x) => x.generation_no);
  assert.equal(all.length, 15, JSON.stringify(all));   // 新しい 14 個 + 今の世代
  assert.ok(all.includes(v.generation_no));
  assert.equal(count('cdb_publish_values v', 'NOT EXISTS (SELECT 1 FROM cdb_publish_generations g WHERE g.generation_no = v.generation_no)'), 0);   // 値の行も一緒に消える
  // 次の verified で印が進めば、古い今の世代も 14 個の外 = 消える
  const v2 = F.stageGeneration(db, mk(19));
  assert.equal(pointer(), v2.generation_no);
  assert.equal(count('cdb_publish_generations'), 14);
  assert.equal(genRow(v.generation_no), undefined);
});

await ta('[12] 入口 (cli): 取れた回は ping なし・受け入れない = fail / 入れた後の確かめが通った回だけ ok・今朝の作り直しでない = fail / 未設定 = ⏭️ + fail / --dry-run は書かない', async () => {
  const pings = [];
  const deps = (extra = {}) => ({ now: new Date(), log: quiet, ping: async (id, p) => { pings.push([id, p.status]); }, openSqlite: async () => db,
    connectFor: () => async () => ({ db: pdb, close: async () => {} }), ...extra });
  const env = { DATA_DIR: tmp, COMPANY_DB_WATCH_URL: 'postgres://test', DAILY_SYNC_RUN_ID: 'ds_test_publish' };
  let x = await quietly(() => F.cli(['--daily'], deps({ env: { DATA_DIR: tmp, DAILY_SYNC_RUN_ID: 'ds_test_publish' } })));
  assert.equal(x.code, 0); assert.match(x.last, /^⏭️ .*未設定/);
  assert.equal(evidence().state, 'skipped');
  await nightly();
  x = await quietly(() => F.cli(['--daily'], deps({ env })));
  assert.equal(x.code, 0, x.last); assert.match(x.last, /^✅ Company DB の写し: 世代 \d+ \(値 0 行/);
  const genNo = pointer();
  // 今朝の作り直し → 入れた後の確かめ = ok の ping (これだけが ok)
  assert.equal((await rebuild()).ok, true);
  x = await quietly(() => F.cli(['--verify-apply', '--daily'], deps({ env })));
  assert.equal(x.code, 0, x.last); assert.match(x.last, /^✅ Company DB の写しの反映: 世代 \d+ を m_products に入れた \(持ち主が C の列なし/);
  const ev = evidence();
  assert.deepEqual([ev.state, ev.generation_no, ev.apply.state, ev.apply.generation_no], ['complete', genNo, 'verified', genNo]);
  // 別の daily-sync の回 (NE の失敗で作り直しを飛ばした朝など) = 確かめられない = fail
  x = await quietly(() => F.cli(['--verify-apply', '--daily'], deps({ env: { ...env, DAILY_SYNC_RUN_ID: 'ds_other' } })));
  assert.equal(x.code, 1); assert.match(x.last, /build_not_this_run/);
  // 受け入れない = ❌ + fail
  await setActive(OWN('skus.name', 'products.name'));   // Company DB の active が夜間ロードの持ち主と違う
  x = await quietly(() => F.cli(['--daily'], deps({ env })));
  assert.equal(x.code, 1); assert.match(x.last, /^❌ .*ownership_mismatch/);
  await setActive(MASTER_OWNERSHIP);
  const n0 = count('cdb_publish_generations'), p0 = pointer();
  x = await quietly(() => F.cli(['--dry-run'], deps({ env })));
  assert.equal(x.code, 0); assert.match(x.last, /試し・書かない/);
  assert.deepEqual([count('cdb_publish_generations'), pointer()], [n0, p0]);
  x = await quietly(() => F.cli(['--daily', '--nope'], deps({ env })));
  assert.equal(x.code, 1);   // 知らない引数 = 失敗 (ping の印も読めない = 打たない)
  assert.deepEqual(pings, [[F.JOB_ID, 'fail'], [F.JOB_ID, 'ok'], [F.JOB_ID, 'fail'], [F.JOB_ID, 'fail']]);   // 取れた回と dry-run は打たない
});

await ta('[13] 台帳・daily-sync (再構築の直前と直後)・照合 ② が知っている (cdb-master-publish・company_owned)', async () => {
  const e = JOBS_REGISTRY.find((j) => j.id === F.JOB_ID);
  assert.ok(e, '台帳に cdb-master-publish が無い');
  const lz = JOBS_REGISTRY.find((j) => j.id === 'lz-daily-build');
  assert.deepEqual(Object.keys(e).sort(), Object.keys(lz).sort());   // lz-daily-build と同じ形
  assert.deepEqual([e.type, e.anchor_hour_jst, e.anchor_minute_jst, e.lifecycle], ['scheduled_job', 7, 0, 'permanent']);
  assert.deepEqual(validateRegistry(), []);
  assert.match(JOBS_REGISTRY.find((j) => j.id === 'warehouse-daily-sync').purpose, /Company DB の写しの反映/);
  const ds = fs.readFileSync(path.join(ROOT, 'apps/warehouse/daily-sync.js'), 'utf8');
  const iPub = ds.indexOf("runScript('apps/company-db/publish/fetch.mjs --daily'"), iRebuild = ds.indexOf("runScript('apps/warehouse/rebuild-m-products.js'");
  const iApply = ds.indexOf("runScript('apps/company-db/publish/fetch.mjs --verify-apply --daily'"), iHistory = ds.indexOf("runScript('apps/warehouse/record-m-products-history.js'");
  assert.ok(iPub > 0 && iPub < iRebuild && iRebuild < iApply && iApply < iHistory, '写しは m_products 再構築の直前・反映の確かめは直後');
  // 反映が世代と違う (exit 4) = 後の工程を止める (詳しくは [20])
  assert.match(ds, /cdbPublishBroken = !cdbPublishApplyResult\.success && cdbPublishApplyResult\.exitCode === 4/);
  assert.equal(SEMANTIC_VERSIONS.company_owned, 1);
  const print = decisionPrint({ norm: 's-ne', kind: 'single', col: 'cost', problem: 'cost', owner: 'company', reasonKind: 'company_owned',
    reason: { reason: 'company_owned', owner_key: 'sku_costs', cdb_value: { cost: { jpy: 150, source: 'manual', status: 'OVERRIDDEN' } }, value: { 原価: 150 }, ne_value: 100, cdb_cost_source: 'manual', generation_no: 7, code: 's-ne' },
    n_state: 'value', n: 100, c: 150, proposal: { op: 'set_ne_value', value: 150 } });
  // 指紋に入る = 持ち主のキー・変換の前の C の値・古い表の値・元の出どころ (世代の番号は毎朝変わるので入れない)
  assert.deepEqual(print.reason, { reason: 'company_owned', owner_key: 'sku_costs', cdb_value: { cost: { jpy: 150, source: 'manual', status: 'OVERRIDDEN' } }, value: { 原価: 150 }, cdb_cost_source: 'manual' });
  assert.equal(print.semantic, 'company_owned@1');
  // 理由の差し替え: 変わった列だけ今までの理由を外して company_owned (変わらなければ今までの理由のまま = 同じ配列)
  const R = [{ col: 'cost', reason: 'exception_cost' }, { col: '*', reason: 'not_in_latest_fetch' }];
  const neV = { name: 'a', genka: 1, genkaSource: '例外', genkaStatus: 'OVERRIDDEN' };
  assert.equal(MP.mergeReasons('x', '単品', neV, { ...neV }, R), R);
  assert.deepEqual(MP.mergeReasons('x', '単品', neV, { ...neV, genka: 2, cdbCostSource: 'manual' }, R).map((x) => [x.reason, x.cdb_cost_source]), [['not_in_latest_fetch', undefined], ['company_owned', 'manual']]);
});

await ta('[14] 本番と同じ watcher ロール (select だけ・読むだけ) で写しが取れる (変更の記録の表の最大の番号と前の水位の出来事も読める)・書けない', async () => {
  const { createRoles } = await import('./company-db/create-watch-roles.mjs');
  await createRoles(pg, { watcherPw: 'w', writerPw: 'x' });
  await nightly();
  const prev = F.currentGeneration(db);
  await pg.query('set role watcher');
  try {
    await pg.query('set default_transaction_read_only = on');
    // 読む口そのもの (取引の中の最初の文 = 時刻・水位の出来事・前の水位の出来事)
    await pdb.query('begin transaction isolation level repeatable read read only');
    let src;
    try { src = await F.readPublishSource(pdb, { prevWatermark: prev.version_watermark }); } finally { await pdb.query('rollback'); }
    assert.ok(Number.isSafeInteger(src.watermark) && src.watermark >= prev.version_watermark);
    assert.match(src.watermarkFingerprint, /^[0-9a-f]{64}$/);
    assert.equal(src.prevEventFingerprint, prev.watermark_fingerprint);   // 前の水位の出来事が同じ中身で読める
    assert.ok(src.skus.length > 3000 && src.loadOwnership?.products);
    const g = await fetchGen(MASTER_OWNERSHIP);   // 1 回分を watcher で
    assert.equal(g.state, 'verified', JSON.stringify(g.problems));
    await assert.rejects(pdb.query("update core.skus set name = 'x' where code = 's-ne'"), /read-only|permission denied/);
    await assert.rejects(pdb.query("insert into events.master_change_events (company_id, change_id, operation, entity_type, entity_key, actor_type, source_system) values (1, gen_random_uuid(), 'INSERT', 'sku', '{}', 'system', 'x')"), /read-only|permission denied/);
  } finally { await pg.query('reset role'); await pg.query('set default_transaction_read_only = off'); }
});

await ta('[15] 作り直しの入力 (product_tax_rate / product_sales_class) は、持ち主が C の列では m_products を上書きできない (セットの手動の売上分類も)・例外の商品の売上分類は今までどおり・扱えない持ち主は入口で止める', async () => {
  const own = OWN('products.sales_class', 'skus.tax_rate', 'skus.tax_class');
  await nightly(own);
  // 古い画面が入力の表に書いた (切替の後は閉じる入口。書かれても持ち主が C の列には効かない)
  db.prepare("INSERT OR REPLACE INTO product_tax_rate (sku, tax_rate, synced_at) VALUES ('s-taxfb', 0.1, 'x')").run();
  db.prepare("INSERT OR REPLACE INTO product_sales_class (sku, sales_class, synced_at) VALUES ('s-sc', 3, 'x')").run();
  assert.equal((await fetchGen(own)).state, 'verified');
  const r = await rebuild(own);
  assert.equal(r.ok, true, JSON.stringify(r.checks));
  assert.deepEqual([mp('s-taxfb').消費税率, mp('s-taxfb').税区分], [0.08, 'REDUCED_8']);   // C の値 (入力の表の 0.1 は効かない)
  assert.equal(mp('s-sc').売上分類, 1);                                                      // C の値 (入力の表の 3 は効かない)
  // セットの手動の売上分類 (set-g = 2) は使わない = 構成品 (s-sc3 = 3) の C の値から導く (15 §5 の 2 の推奨)
  assert.equal(mp('set-g').売上分類, 3);
  assert.deepEqual(reasonsOf(snap(), 'set-g').filter((x) => x.col === 'sales_class').map((x) => [x.value, x.ne_value, x.owner_key]), [[3, 2, 'products.sales_class']]);
  // 例外の商品 (NE に無い) は Company DB に売上分類の置き場所が無い = product_sales_class のまま (切替の前に決めること)
  assert.equal(mp('ex-only').売上分類, 2);
  db.prepare("INSERT OR REPLACE INTO product_tax_rate (sku, tax_rate, synced_at) VALUES ('s-taxfb', 0.08, 'x')").run();
  db.prepare("INSERT OR REPLACE INTO product_sales_class (sku, sales_class, synced_at) VALUES ('s-sc', 1, 'x')").run();
  // 扱えない持ち主 (一緒に切り替える組の片方だけ) が active に入った (prepare は通さない = [17]) = 写しは受け入れない (fail の ping)。
  //   config (configured) が扱えないだけなら止めない (config だけでは何も変わらない。prepare が断る)
  const pings = [];
  const run = () => quietly(() => F.cli(['--daily'], { env: { DATA_DIR: tmp, COMPANY_DB_WATCH_URL: 'postgres://test' }, log: quiet, ownership: OWN('skus.tax_rate'),
    ping: async (id, p) => { pings.push(p.status); }, openSqlite: async () => db, connectFor: () => async () => ({ db: pdb, close: async () => {} }) }));
  let x = await run();
  assert.equal(x.code, 0, x.last);
  await setActive(OWN('skus.tax_rate'));
  x = await run();
  assert.equal(x.code, 1); assert.match(x.last, /^❌ .*ownership_not_supported/);
  assert.deepEqual(pings, ['fail']);
  await setActive(own);
});

await ta('[16] 切替の後に NE にだけある SKU (Company DB に無い) = 止めない: NE の値で作る (写さない)・証跡とログの ⚠️ に出す・確かめも通る (ok の ping)・夜間ロードが入れた翌朝から C の値', async () => {
  const own = OWN('sku_costs', 'skus.name', 'products.name');
  await nightly(own);
  assert.equal((await fetchGen(own)).state, 'verified');
  addNe('s-new', 'NE にだけある新しい商品', 77);
  const r = await rebuild(own);
  assert.equal(r.ok, true, JSON.stringify(r.checks));
  const x = mp('s-new');
  assert.deepEqual([x.商品名, x.原価, x.原価ソース, x.原価状態], ['NE にだけある新しい商品', 77, 'NE', 'COMPLETE']);   // NE の値のまま
  assert.deepEqual([r.publish.stats.not_in_cdb, r.publish.stats.not_in_cdb_codes], [1, ['s-new']]);
  assert.ok(r.warn.some((w) => /Company DB に無い SKU 1 件は NE の値のまま.*s-new/.test(w)), JSON.stringify(r.warn));
  assert.deepEqual([r.publish.applied.ok, r.publish.applied.counts.not_in_cdb, r.publish.applied.not_in_cdb_codes], [true, 1, ['s-new']]);
  assert.equal(reasonsOf(snap(), 's-new').filter((z) => z.reason === 'company_owned').length, 0);
  // 次の工程の確かめ = 通る (ok の ping)・⚠️ と証跡に出す (朝の要約に出る)
  const pings = [];
  const deps = { now: new Date(), log: quiet, ping: async (id, p) => { pings.push(p.status); }, openSqlite: async () => db, ownership: own,
    env: { DATA_DIR: tmp, COMPANY_DB_WATCH_URL: 'postgres://test', DAILY_SYNC_RUN_ID: 'ds_test_publish' }, connectFor: () => async () => ({ db: pdb, close: async () => {} }) };
  const v = await quietly(() => F.cli(['--verify-apply', '--daily'], deps));
  assert.equal(v.code, 0, v.last);
  assert.match(v.last, /^⚠️ Company DB の写しの反映: .*Company DB に無い SKU 1 件は NE の値のまま \(s-new\)/);
  assert.deepEqual(pings, ['ok']);
  assert.deepEqual(evidence().apply.not_in_cdb, { count: 1, codes: ['s-new'] });
  // 次の朝の写し: きのうの m_products に s-new がある = 受け入れる (止めない)・証跡と ⚠️ に出す
  const f2 = await quietly(() => F.cli(['--daily'], deps));
  assert.equal(f2.code, 0, f2.last);
  assert.match(f2.last, /^⚠️ Company DB の写し: .*Company DB に無い SKU 1 件は NE の値のまま \(s-new\)/);
  assert.deepEqual(evidence().not_in_cdb, { count: 1, codes: ['s-new'] });
  assert.deepEqual(pings, ['ok']);   // 取れた回は打たない
  // Render に送って夜間ロードが C に入れた翌朝 = C の値で作る (NE にしか無い SKU は 0)
  publishMirror();
  await nightly(own);
  await putCost('s-new', 80);
  assert.equal((await fetchGen(own)).state, 'verified');
  const r2 = await rebuild(own);
  assert.equal(r2.ok, true, JSON.stringify(r2.checks));
  assert.deepEqual([mp('s-new').原価, mp('s-new').原価ソース, r2.publish.stats.not_in_cdb], [80, '例外', 0]);
  db.prepare("DELETE FROM raw_ne_products WHERE 商品コード = 's-new'").run(); FX.markComplete(db, readNeRawRev);
});

await ta('[17] 持ち主の epoch (0055): config を書き換えただけでは何も変わらない・prepare → 明示のロード → 写し → 作り直し → 確かめ → activate の順だけ・証拠が欠ければ active にしない・行が無い = 全部 load', async () => {
  const own = OWN('sku_costs');
  const logs = [];
  const epochCli = (argv, extra = {}) => quietly(() => EP.cli(argv, { env: { DATA_DIR: tmp }, connect: async () => ({ db: pdb, close: async () => {} }), openSqlite: async () => db,
    log: (m) => logs.push(m), ...extra }));
  const allHash = OS.ownershipHashOf(MASTER_OWNERSHIP), ownHash = MP.ownershipHash(own);
  assert.equal(ownHash, OS.ownershipHashOf(own));   // 夜間ロード・写し・作り直しで同じハッシュの式
  // (a) 行が無い (0055 の前・まだ誰も prepare していない) = 全部 load
  await q('delete from ops.master_ownership_state');
  let lr = await loadNow();
  assert.deepEqual([lr.ownership_epoch.epoch, lr.company_owned], ['default', []]);
  // (b) config (configured) を書き換えただけ = 写し・作り直しは active (全部 load) のまま
  let g = await fetchGen(own);
  assert.equal(g.state, 'verified', JSON.stringify(g.problems));
  assert.deepEqual([g.evidence.company_owned, g.evidence.epochs.generation.kind, g.evidence.epochs.configured], [[], 'default', ownHash]);
  assert.equal((await rebuild()).ok, true);
  assert.equal(snap().build.cdb_publish_ownership_hash, allHash);
  // (c) prepare: 扱えない設定・active と同じ = 記録しない
  assert.equal(await epochCli(['prepare'], { ownership: OWN('skus.tax_rate') }), 1);
  assert.match(logs.at(-1), /④a で扱えない/);
  assert.equal(await epochCli(['prepare'], { ownership: MASTER_OWNERSHIP }), 1);
  assert.match(logs.at(-1), /今の active と同じ/);
  assert.equal((await q('select count(*)::int as n from ops.master_ownership_state'))[0].n, 0);   // 断った回は何も残さない
  assert.equal(await epochCli(['prepare'], { ownership: own }), 0, logs.at(-1));
  let st = await OS.readOwnershipState(pdb);
  assert.deepEqual([st.state, st.active.hash, st.prepared.hash], ['ok', allHash, ownHash]);
  // prepare しても毎晩のロード (明示なし)・写しは active のまま
  lr = await loadNow();
  assert.deepEqual([lr.ownership_epoch.epoch, lr.company_owned], ['active', []]);
  // 最後に commit したロードが active の持ち主 (番号は合っていても) = activate しない (LOAD_EPOCH_MISMATCH。#1564 Codex R4)
  await setCutoverPhase('frozen');
  try {
    await assert.rejects(OS.activateOwnership(pdb, { expectHash: ownHash, expectPreparedAt: (await OS.readOwnershipState(pdb)).prepared.prepared_at, expectLoadCommitSeq: lr.load_commit_seq, actor: 't', evidence: {} }),
      (e) => e.code === 'LOAD_EPOCH_MISMATCH' && e.last_load === lr.run_id);
  } finally { await setCutoverPhase('legacy_open'); }
  assert.equal((await OS.readOwnershipState(pdb)).active.hash, allHash);
  g = await fetchGen(own);
  assert.deepEqual([g.state, g.evidence.company_owned, g.evidence.epochs.generation.kind, g.evidence.epochs.prepared], ['verified', [], 'active', ownHash]);
  // 証拠が無い = active にしない
  assert.equal(await epochCli(['activate']), 1);
  assert.match(logs.at(-1), /build_not_prepared_epoch/);
  // (d) 切替の日: 明示のロード → 写し = prepared の世代 → 作り直し (持ち主を渡さない = 世代の持ち主) → 確かめ → activate
  //   明示のロードは本番と同じ HTTP の道 (host = render。毎晩の cron = render-nightly ではない) = 写しは場所ではなく commit の順で選ぶ (#1564 Codex R4 High)
  const hl = await loadViaHttp({ usePrepared: true });
  assert.deepEqual([hl.commit.ingest_run_id, hl.commit.epoch, hl.commit.host, hl.commit.ownership_hash], [hl.run_id, 'prepared', 'render', ownHash]);
  assert.equal((await q("select host from ops.ingest_runs where ingest_run_id = $1", [hl.run_id]))[0].host, 'render');
  assert.equal((await F.selectPublishLoad(pdb)).ingest_run_id, hl.run_id);   // 写しが使うロード = 最後に commit したロード (毎晩の cron の前の回ではない)
  await putCost('s-ne', 155);
  g = await fetchGen(MASTER_OWNERSHIP);   // config は関係ない
  assert.deepEqual([g.state, g.evidence.company_owned, g.evidence.epochs.generation.kind], ['verified', ['cost'], 'prepared'], JSON.stringify(g.problems));
  const r = await rebuild();
  assert.equal(r.ok, true, JSON.stringify(r.checks));
  assert.deepEqual([r.publish.epoch, mp('s-ne').原価], [`generation ${g.generation_no}`, 155]);
  assert.equal(snap().build.cdb_publish_ownership_hash, ownHash);
  assert.equal(await epochCli(['activate']), 1);   // 入れた後の確かめの前
  assert.match(logs.at(-1), /no_verified_apply_evidence/);
  const va = await F.runVerifyApply({ sqlite: db, dataDir: tmp, taxRates: TAX_RATES });
  assert.deepEqual([va.state, va.evidence.apply.epoch], ['verified', 'prepared'], JSON.stringify(va.problems));
  assert.match(va.line, /activate で active にできる/);
  // 確かめの後に m_products が書き換えられた = active にしない
  db.prepare("UPDATE m_products SET 原価 = 1 WHERE 商品コード = 's-ne'").run();
  assert.equal(await epochCli(['activate']), 1);
  assert.match(logs.at(-1), /applied_mismatch_now/);
  db.prepare("UPDATE m_products SET 原価 = 155 WHERE 商品コード = 's-ne'").run();
  // 切替の段階 (⑤-1): 表が無い・legacy_open (古い入口がまだ正)・company_owner・new_open (切り替えた後) = active にしない。frozen だけ (#1564 の見直し M-1)
  //   表が無い (⑤-1 の前の DB) = 段階の表を読む問い合わせが「無い」と答える接続で確かめる (この積み方では ⑤-1 の 0051 がいつもある)
  const noCutover = { query: async (sql, p) => (/to_regclass\('ops\.master_cutover_state'\)/.test(sql) ? { rows: [{ ok: false }] } : pdb.query(sql, p)) };
  const lastCommit = await OS.latestLoadCommit(pdb);   // 証拠の世代が読んだ夜間ロード (= 今の最後のロード = HTTP の明示のロード)
  const lastLoad = lastCommit.commit_seq;
  assert.deepEqual(db.prepare('SELECT load_run_id, load_commit_seq FROM cdb_publish_generations WHERE generation_no = ?').get(g.generation_no),
    { load_run_id: hl.run_id, load_commit_seq: Number(lastLoad) });   // 世代に commit の番号
  assert.deepEqual([evidence().load_run_id, evidence().load_commit_seq], [hl.run_id, lastLoad]);   // 証跡にも
  await assert.rejects(OS.activateOwnership(noCutover, { expectHash: ownHash, expectPreparedAt: (await OS.readOwnershipState(pdb)).prepared.prepared_at, expectLoadCommitSeq: lastLoad, actor: 't', evidence: {} }),
    (e) => e.code === 'CUTOVER_STATE_MISSING');
  assert.equal((await q("select phase from ops.master_cutover_state where id = 1"))[0].phase, 'legacy_open');   // ⑤-1 の 0051 の最初の段階
  for (const ph of ['legacy_open', 'company_owner', 'new_open']) {
    await setCutoverPhase(ph);
    assert.equal(await epochCli(['activate']), 1, ph);
    assert.match(logs.at(-1), new RegExp(`切替の段階が ${ph} = active にしない`));
  }
  await setCutoverPhase('frozen');
  // 証拠を集めた後に (同じ持ち主で) prepare がやり直された = 前の証拠の時刻では active にしない (行の鍵の後に比べる。#1564 Codex R2 Medium 4)
  const pAt = (await OS.readOwnershipState(pdb)).prepared.prepared_at;
  await q("update ops.master_ownership_state set prepared_at = prepared_at + interval '1 millisecond' where id = 1");
  await assert.rejects(OS.activateOwnership(pdb, { expectHash: ownHash, expectPreparedAt: pAt, expectLoadCommitSeq: lastLoad, actor: 't', evidence: {} }), (e) => e.code === 'PREPARED_CHANGED');
  await assert.rejects(OS.activateOwnership(pdb, { expectHash: ownHash, expectLoadCommitSeq: lastLoad, actor: 't', evidence: {} }), (e) => e.code === 'PREPARED_AT_REQUIRED');
  await assert.rejects(OS.activateOwnership(pdb, { expectHash: ownHash, expectPreparedAt: pAt, actor: 't', evidence: {} }), (e) => e.code === 'LOAD_COMMIT_REQUIRED');
  await q("update ops.master_ownership_state set prepared_at = prepared_at - interval '1 millisecond' where id = 1");
  assert.equal((await OS.readOwnershipState(pdb)).prepared.prepared_at, pAt);
  // 証拠の世代の後に夜間ロードが入った (最後のロードが証拠のロードでない) = active にしない (#1564 Codex R3 High 1。並んだときの順は本物の PostgreSQL の試験 [21])
  await assert.rejects(OS.activateOwnership(pdb, { expectHash: ownHash, expectPreparedAt: pAt, expectLoadCommitSeq: String(BigInt(lastLoad) - 1n), actor: 't', evidence: {} }),
    (e) => e.code === 'LOAD_AFTER_EVIDENCE' && e.last_load === hl.run_id && e.last_commit_seq === lastLoad);
  assert.equal((await OS.readOwnershipState(pdb)).active.hash, allHash);
  // prepare より前に Company DB を読んだ世代 = active にしない (#1564 の見直し L-1)
  await q("update ops.master_ownership_state set prepared_at = now() + interval '1 hour' where id = 1");
  assert.equal(await epochCli(['activate']), 1);
  assert.match(logs.at(-1), /generation_before_prepare/);
  await q("update ops.master_ownership_state set prepared_at = prepared_at - interval '2 hours' where id = 1");
  assert.equal((await OS.readOwnershipState(pdb)).active.hash, allHash);   // 断った回は何も変えない
  // 世代に commit の番号が無い (0055 の前のロードの世代) = active にしない (証拠で断る。#1564 Codex R4 Medium 2)
  const gNo = snap().build.cdb_publish_generation_no, keepSeq = db.prepare('SELECT load_commit_seq FROM cdb_publish_generations WHERE generation_no = ?').get(gNo).load_commit_seq;
  db.prepare('UPDATE cdb_publish_generations SET load_commit_seq = NULL WHERE generation_no = ?').run(gNo);
  assert.equal(await epochCli(['activate']), 1);
  assert.match(logs.at(-1), /generation_without_load_commit/);
  db.prepare('UPDATE cdb_publish_generations SET load_commit_seq = ? WHERE generation_no = ?').run(keepSeq, gNo);
  assert.equal(await epochCli(['activate']), 0, logs.at(-1));
  st = await OS.readOwnershipState(pdb);
  assert.deepEqual([st.active.hash, st.prepared], [ownHash, null]);
  const [act] = await q("select evidence from ops.master_ownership_events where action = 'activate'");
  assert.deepEqual([act.evidence.build_id, act.evidence.generation_no], [snap().build.build_id, g.generation_no]);
  // (e) 次の夜から毎晩のロード・写しは新しい active
  lr = await loadNow();
  assert.deepEqual([lr.ownership_epoch.epoch, lr.company_owned], ['active', ['sku_costs']]);
  g = await fetchGen(MASTER_OWNERSHIP);
  assert.deepEqual([g.state, g.evidence.company_owned, g.evidence.epochs.generation.kind], ['verified', ['cost'], 'active']);
  // (f) cancel = prepared だけ取り消す (active はそのまま)・prepared が無ければ明示のロードは動かない
  assert.equal(await epochCli(['prepare'], { ownership: MASTER_OWNERSHIP }), 0);
  assert.equal(await epochCli(['cancel']), 0);
  st = await OS.readOwnershipState(pdb);
  assert.deepEqual([st.active.hash, st.prepared], [ownHash, null]);
  await assert.rejects(loadViaHttp({ usePrepared: true }), /prepared の持ち主が無い/);
  // 記録は足すだけ
  assert.deepEqual((await q('select action from ops.master_ownership_events order by event_id')).map((e) => e.action), ['init', 'prepare', 'activate', 'prepare', 'cancel_prepare']);
  await assert.rejects(q('delete from ops.master_ownership_events'), /足すだけ/);
  // 記録が壊れている (ハッシュが中身と違う) = 推測で持ち主を決めない (ロードも写しも止まる)
  await q("update ops.master_ownership_state set active_map = active_map || '{\"sku_costs\": \"load\"}'::jsonb");
  await assert.rejects(OS.readOwnershipState(pdb), /ハッシュが中身と違う/);
  await setActive(MASTER_OWNERSHIP);
  await nightly();
});

await ta('[18] NE にしか無いセット (Company DB に無い) = 構成品が C にあっても全部 NE の道で作る (導いた原価・税・売上分類・取扱区分・構成品の名前と原価)・確かめも NE の値を待つ', async () => {
  const own = OWN('sku_costs', 'skus.name', 'products.name', 'skus.tax_rate', 'skus.tax_class', 'products.sales_class', 'skus.handling', 'products.status');
  const insSet = db.prepare('INSERT OR REPLACE INTO raw_ne_set_products (セット商品コード, セット商品名, セット販売価格, 商品コード, 数量, synced_at) VALUES (?, ?, ?, ?, ?, ?)');
  insSet.run('set-new', 'NE にだけある新しいセット', 900, 's-sc', 2, FX.T1); insSet.run('set-new', 'NE にだけある新しいセット', 900, 's-sc3', 1, FX.T1);
  FX.markComplete(db, readNeRawRev);
  const COLS = ['商品名', '原価', '原価ソース', '原価状態', '消費税率', '税区分', '売上分類', '取扱区分'];
  const pick = (code) => COLS.map((c) => mp(code)[c]);
  const comps = (code) => db.prepare('SELECT 構成商品コード, 数量, 構成商品名, 構成商品原価 FROM m_set_components WHERE セット商品コード = ? ORDER BY 構成商品コード').all(code);
  // 今までの決め方 (全部 load) の値
  assert.equal((await rebuild(MASTER_OWNERSHIP)).ok, true);
  const base = { set: pick('set-new'), comps: comps('set-new'), setC: pick('set-c') };
  assert.deepEqual(base.set.slice(1), [40 * 2 + 45, 'セット計算', 'COMPLETE', 0.08, 'MIXED', 1, '取扱中'], JSON.stringify(base.set));
  // 構成品の C の値を NE と違えておく (C の値で導けば、原価・税・売上分類・取扱区分・構成品の名前と原価が全部変わる)
  await nightly(own);
  await putCost('s-sc', 400); await putCost('s-sc3', 450);
  await setName('s-sc', 'C の名前 sc'); await setName('s-sc3', 'C の名前 sc3');
  await q("update core.skus set tax_rate = 0.08, tax_class = 'REDUCED_8' where code in ('s-sc', 's-sc3')");   // NE は 10 と 8 = MIXED
  await q("update core.products set sales_class = 4 where product_id in (select product_id from core.skus where code in ('s-sc', 's-sc3'))");   // NE は 1 と 3
  await setHandling('s-sc3', 'discontinued');
  const g = await fetchGen(own);
  assert.equal(g.state, 'verified', JSON.stringify(g.problems));
  const r = await rebuild(own);
  assert.equal(r.ok, true, JSON.stringify(r.checks));
  // NE にしか無いセット = 今までの決め方の値のまま (構成品の C の値は使わない)
  assert.deepEqual(pick('set-new'), base.set);
  assert.deepEqual(comps('set-new'), base.comps);
  assert.deepEqual([r.publish.stats.not_in_cdb, r.publish.stats.not_in_cdb_codes], [1, ['set-new']]);
  assert.equal(reasonsOf(snap(), 'set-new').filter((z) => z.reason === 'company_owned').length, 0);
  // C にあるセット (同じ構成品を持つ set-c) は C の値で導く = C の値が本当に NE と違うことの確かめ
  assert.notEqual(mp('set-c').原価, base.setC[1]);
  assert.deepEqual(comps('set-c').filter((x) => x.構成商品コード === 's-sc').map((x) => [x.構成商品名, x.構成商品原価]), [['C の名前 sc', 400]]);
  // 次の工程の確かめも通る (NE にしか無いセットは not_in_cdb)
  const va = await F.runVerifyApply({ sqlite: db, dataDir: tmp, write: () => true, taxRates: TAX_RATES });
  assert.equal(va.state, 'verified', JSON.stringify(va.problems));
  assert.deepEqual(va.evidence.apply.not_in_cdb, { count: 1, codes: ['set-new'] });
  // 取引の中の確かめは NE の値を待つ: NE にしか無いセットの構成品の行が C の値になっていたら見つける (C の値を使った構成品は持ち主が C の列を待たない)
  const neSc = base.comps.find((x) => x.構成商品コード === 's-sc');
  db.prepare("UPDATE m_set_components SET 構成商品名 = 'C の名前 sc', 構成商品原価 = 400 WHERE セット商品コード = 'set-new' AND 構成商品コード = 's-sc'").run();
  const pubR = MP.makePublishResolver({ ownership: own, publication: MP.readCurrentPublish(db), staged: new Map([['set-new', 'セット'], ['s-sc', '単品']]), taxRates: TAX_RATES });
  pubR.expectComponent('set-new', 's-sc', { name: neSc.構成商品名, cost: neSc.構成商品原価 }, { fromCdb: false });
  pubR.expectComponent('set-c', 's-sc', { name: neSc.構成商品名, cost: neSc.構成商品原価 }, { fromCdb: true });
  const vx = MP.verifyApplied(db, { publication: MP.readCurrentPublish(db), ownership: own, taxRates: TAX_RATES, expected: pubR.expected });
  assert.deepEqual(vx.problems.filter((p) => /構成/.test(p.col)).map((p) => [p.code, p.col]), [['set-new/s-sc', 'unchanged:構成商品名'], ['set-new/s-sc', 'unchanged:構成商品原価']]);
  db.prepare('UPDATE m_set_components SET 構成商品名 = ?, 構成商品原価 = ? WHERE セット商品コード = ? AND 構成商品コード = ?').run(neSc.構成商品名, neSc.構成商品原価, 'set-new', 's-sc');
  db.prepare("DELETE FROM raw_ne_set_products WHERE セット商品コード = 'set-new'").run(); FX.markComplete(db, readNeRawRev);
  await nightly();
});

await ta('[19] ②b 古い表 (税区分・売上分類・送料・推奨保有月数 = NE に欄が無い): 全部 load = 比べない・差 0・反映待ち (台帳)・breach・overdue・blocked・朝の要約と証跡', async () => {
  const { compareOldTables, oldTablesSummary, OLD_RESULT_DIR } = await import('../apps/company-db/master-compare/compare-old-tables.mjs');
  const { runCompare, makeCompareRunId } = await import('../apps/company-db/master-compare/run.mjs');
  const { pendingDir, WRITE_FAILED } = await import('../apps/company-db/master-compare/pending.mjs');
  const today = jstDateStr(new Date());
  const old = async (extra = {}) => {
    await pdb.query('begin transaction isolation level repeatable read read only');
    try {
      const t = (await q(`select to_char(clock_timestamp() at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') as t`))[0].t;
      return await compareOldTables({ db: pdb, dataDir: tmp, asOfJst: today, syncRunId: 'ds_test_publish', cdbReadAt: t, compareRunId: makeCompareRunId(), taxRates: TAX_RATES, ...extra });
    } finally { await pdb.query('rollback'); }
  };
  const items = (o) => o.items.map((x) => [x.code, x.col, x.class]).sort((a, b) => `${a}`.localeCompare(`${b}`));
  const morning = async (own) => { assert.equal((await fetchGen(own)).state, 'verified'); const r = await rebuild(); assert.equal(r.ok, true, JSON.stringify(r.checks)); return r; };
  // (a) 全部 load = 比べない (C を読まない・台帳も書かない・要約に出さない)
  await nightly();
  await morning(MASTER_OWNERSHIP);
  let o = await old();
  assert.deepEqual([o.verdict, o.reason, o.cols], ['not_applied', 'load_owned', []]);
  assert.equal(oldTablesSummary(o), null);
  assert.equal(fs.existsSync(path.join(tmp, OLD_RESULT_DIR)), false);
  // (b) 持ち主が C: 作り直しの直後 = 差 0 (由来 = 今朝の作り直し・写しの世代)
  const own = OWN('skus.tax_rate', 'skus.tax_class', 'products.sales_class', 'skus.shipping', 'skus.reorder_months');
  await nightly(own);
  await morning(own);
  o = await old();
  assert.deepEqual([o.verdict, o.cols], ['pass', ['tax_class', 'sales_class', 'shipping', 'reorder_months']], JSON.stringify(o.items));
  assert.deepEqual([o.build.build_id, o.publish.generation_no, o.publish.generation_id], [snap().build.build_id, pointer(), genRow(pointer()).generation_id]);
  assert.ok(o.counts.checked > 3000 && o.counts.derived > 0);
  assert.equal(o.to_ne.state, 'not_applicable');
  assert.match(oldTablesSummary(o), /^✅ ②b 古い表: 差 0 \(列 tax_class・sales_class・shipping・reorder_months/);
  // (c) 写しの後に C を直した = 反映待ち (始まり = 最初に見た時刻・次の回も動かない)。翌朝の写し → 作り直しで入る = 差 0・台帳から外れる
  await q("update core.skus set tax_rate = 0.08, tax_class = 'REDUCED_8' where code = 's-ne'");
  await q("update core.products set sales_class = 2 where product_id = (select product_id from core.skus where code = 's-sc')");
  await q("update core.skus set shipping_code = 'S7', shipping_method = 'ネコポス', shipping_cost_jpy = 199 where code = 's-ship'");
  await q("update core.skus set reorder_months = null where code = 's-ne'");   // 空も値 (行を消す)
  o = await old();
  assert.equal(o.verdict, 'lag');
  assert.deepEqual(items(o), [['s-ne', 'reorder_months', 'lag'], ['s-ne', 'tax_class', 'lag'], ['s-sc', 'sales_class', 'lag'], ['s-ship', 'shipping', 'lag']]);   // 送料は m_products と product_shipping で 1 件
  assert.equal(o.pending.written.entries, 4);
  assert.match(oldTablesSummary(o), /^ℹ️ ②b 古い表: 反映待ち 4 件/);
  const o2 = await old();
  assert.deepEqual(o2.items.map((x) => x.start_at), o.items.map((x) => x.start_at));
  await morning(own);
  o = await old();
  assert.deepEqual([o.verdict, o.pending.written.entries], ['pass', 0]);
  // (d) 世代にあった値が古い表に無い (作り直しの後に古い画面が書いた) = breach (反映待ちにしない)
  db.prepare("INSERT OR REPLACE INTO m_reorder_setting (sku, 推奨保有月数, 商品名, updated_by, synced_at) VALUES ('s-ne', 3, 'NE の単品', 'x', 'x')").run();
  db.prepare("UPDATE m_products SET 税区分 = 'STANDARD_10' WHERE 商品コード = 's-ne'").run();
  o = await old();
  assert.equal(o.verdict, 'breach');
  assert.deepEqual(o.items.map((x) => [x.code, x.col, x.class, x.why]).sort(), [['s-ne', 'reorder_months', 'breach', 'generation_had_value'], ['s-ne', 'tax_class', 'breach', 'generation_had_value']]);
  assert.match(oldTablesSummary(o), /^⚠️ ②b 古い表: C の値が入っていない 2 件 \(tax_class 1 \/ reorder_months 1・世代 \d+・作り直し /);
  // 朝の照合 (run.mjs) = 要約の先頭に ⚠️・証跡 master-compare に由来つきで残る
  const rc = await quietly(() => runCompare({ db: pdb, dataDir: tmp, asOf: today, syncRunId: 'ds_test_publish', neCompare: null,
    compare: async () => ({ format: 'x', verdict: 'pass', counts: { compared: {} }, load: { ingest_run_id: 'L1', started_at: 'x' } }) }));
  assert.match(rc.line, /^⚠️ ②b 古い表: C の値が入っていない 2 件 .* \/ ✅ マスタ照合 ①/);
  const mc = readEvidence(tmp, today)['master-compare'];
  assert.deepEqual([mc.old_tables.verdict, mc.old_tables.counts.breach, mc.old_tables.build.build_id, mc.old_tables.publish.generation_no], ['breach', 2, snap().build.build_id, pointer()]);
  await morning(own);   // 作り直しで C の値に戻る
  assert.equal((await old()).verdict, 'pass');
  // (e) 期限の後も違う = overdue (反映待ちの始まりより後に C を読んだ世代での作り直しでも違う)
  await q("update core.products set sales_class = 4 where product_id = (select product_id from core.skus where code = 's-sc')");
  assert.deepEqual(items(await old()), [['s-sc', 'sales_class', 'lag']]);
  await q("update core.products set sales_class = 1 where product_id = (select product_id from core.skus where code = 's-sc')");
  await morning(own);   // 世代は 1 を読んだ
  await q("update core.products set sales_class = 4 where product_id = (select product_id from core.skus where code = 's-sc')");
  o = await old();
  assert.deepEqual([o.verdict, items(o)], ['breach', [['s-sc', 'sales_class', 'overdue']]]);
  // (f) 台帳が使えない (前の回の保存に失敗した印) = 反映待ちを判定しない = blocked
  fs.writeFileSync(path.join(pendingDir(tmp, OLD_RESULT_DIR), WRITE_FAILED), '{}');
  o = await old();
  assert.deepEqual([o.verdict, o.reason, items(o)], ['blocked', 'pending_untrusted', [['s-sc', 'sales_class', 'held']]]);
  assert.match(oldTablesSummary(o), /^⚠️ ②b 古い表: 判定できない \(pending_untrusted\)/);
  fs.rmSync(path.join(pendingDir(tmp, OLD_RESULT_DIR), WRITE_FAILED));
  // (g) 作り直しが今朝の daily-sync の回でない = blocked
  assert.deepEqual([(await old({ syncRunId: 'ds_other' })).verdict, (await old({ syncRunId: 'ds_other' })).reason], ['blocked', 'stale_build']);
  // 後片付け
  await q("update core.products set sales_class = 1 where product_id = (select product_id from core.skus where code = 's-sc')");
  await nightly();
  await morning(MASTER_OWNERSHIP);
});

await ta('[20] 写しの反映が世代と違う朝 (exit 4) = 後の m_products・上書き表を読む工程を全部止める (⚠️ 見送り・再試行に載せない・自分で ping を打つ工程は fail の ping)・工程の一覧は daily-sync と同じ', async () => {
  const G = await import('../apps/warehouse/publish-gate.js');
  // 工程ごとの判断
  assert.deepEqual(G.publishGateDecision('apps/warehouse/rebuild-f-sales.js', { broken: false }), { skip: false });
  const d = G.publishGateDecision('apps/warehouse/rebuild-f-sales.js', { broken: true });
  assert.deepEqual([d.skip, d.pingJobId], [true, null]);
  assert.match(d.summary, /^⚠️ 見送り: Company DB の写しの反映が世代と違う/);
  assert.equal(G.publishGateDecision('scripts/company-db/lz-daily.mjs --daily', { broken: true }).pingJobId, 'lz-daily-build');
  assert.equal(G.publishGateDecision('scripts/yahoo-finance/build-yahoo-daily-fact.js --data-dir X --month 2026-10', { broken: true }).skip, true);
  for (const f of ['apps/company-db/master-compare/run.mjs --daily', 'apps/warehouse/backup-warehouse.js', 'apps/company-db/watch/run.mjs']) assert.equal(G.publishGateDecision(f, { broken: true }).skip, false, f);
  // daily-sync の写しの反映より後の工程は、全部どちらかの一覧に載っている (足した工程は止めるか決めてから載せる)。一覧に古い工程も残さない
  const ds = fs.readFileSync(path.join(ROOT, 'apps/warehouse/daily-sync.js'), 'utf8');
  const iApply = ds.indexOf("runScript('apps/company-db/publish/fetch.mjs --verify-apply --daily'");
  const files = new Set([...ds.slice(iApply).matchAll(/runScript\(\s*(?:'([^']+)'|`([^`]+)`)/g)].map((m) => G.scriptFileOf(m[1] || m[2])));
  assert.ok(files.size > 40, String(files.size));
  assert.deepEqual([...files].filter((f) => !Object.hasOwn(G.PUBLISH_GATED_SCRIPTS, f) && !Object.hasOwn(G.PUBLISH_UNGATED_SCRIPTS, f)), []);
  for (const f of [...Object.keys(G.PUBLISH_GATED_SCRIPTS), ...Object.keys(G.PUBLISH_UNGATED_SCRIPTS)]) {
    assert.ok(files.has(f), `daily-sync の写しの反映より後に無い: ${f}`);
    assert.ok(fs.existsSync(path.join(ROOT, f)), f);
  }
  assert.deepEqual(Object.keys(G.PUBLISH_GATED_SCRIPTS).filter((f) => Object.hasOwn(G.PUBLISH_UNGATED_SCRIPTS, f)), []);
  // 止めない工程は m_products・上書き表・それを読む view を直接読まない (読むなら止める一覧へ)。確かめる・残す・比べる側だけは別
  const T = /\b(m_products(_history)?|m_set_components|exception_genka|product_shipping|m_reorder_setting|product_tax_rate|product_sales_class|v_product_master|v_missing_data|v_sku_costed|v_amazon_sku_profit_actual_v4)\b/;
  const EXEMPT = new Set(['apps/company-db/publish/fetch.mjs', 'apps/warehouse/backup-warehouse.js', 'apps/company-db/master-compare/run.mjs']);
  assert.deepEqual(Object.keys(G.PUBLISH_UNGATED_SCRIPTS).filter((f) => !EXEMPT.has(f) && T.test(fs.readFileSync(path.join(ROOT, f), 'utf8'))), []);
  // 自分で ping を打つ工程の台帳の項目がある
  for (const id of Object.values(G.GATED_OWN_PING)) assert.ok(JOBS_REGISTRY.find((j) => j.id === id), id);
  // daily-sync の配線: 反映の確かめの直後 (履歴の記録より前) に立てる・runScript が最初に判断する・通知の前に fail の ping・前の個別の見送りは残っていない
  // exit 4 と門 (cdb_publish_gate) の両方・確かめが通らなかった朝は「確かめた safe の行」だけ流す (#1564 の見直し L-4・Codex R2 High 2・R5 Medium)
  assert.match(ds, /const cdbPublishGateDecision = gateAfterVerify\(\{ apply: cdbPublishApplyResult, gate: cdbPublishGateNow \}\);/);
  const iGate = ds.indexOf('publishGate.broken = cdbPublishBroken || cdbPublishGateDecision.broken;');
  assert.ok(iGate > iApply && iGate < ds.indexOf("runScript('apps/warehouse/record-m-products-history.js'"));
  assert.match(ds, /function runScript\([^)]*\) \{\s*\/\/[^\n]*\n\s*const gate = publishGateDecision\(scriptPath, publishGate\);\s*if \(gate\.skip\) \{/);
  assert.match(ds, /return \{ success: false, blocked: true, gated: true, summary: gate\.summary \};/);
  const iFlush = ds.indexOf('await flushPublishGatePings();');
  assert.ok(iFlush > 0 && iFlush < ds.indexOf('const notifyOk = await notify(notifyMsg);'));
  assert.doesNotMatch(ds, /publishBrokenSkip/);
  // 履歴の記録を止めた朝も、観測の原価は runScript に渡す (= 止める一覧で ⚠️ 見送り。「履歴が失敗」と言わない)
  assert.match(ds, /: historyResult\.gated \? runScript\('apps\/company-db\/push\/sku-cost-observed\.mjs --send'/);
});

await ta('[21] 同じ NE の取得・同じ世代・同じ中身の作り直し = 何も書かない (時刻も記録も)・NE の取得が新しい = 今までどおり入れ替える・古い表が書き換えられた = 入れ替えて直す', async () => {
  const own = OWN('sku_costs', 'skus.shipping', 'skus.reorder_months', 'skus.name', 'products.name');
  await nightly(own);
  assert.equal((await fetchGen(own)).state, 'verified');
  const r1 = await rebuild();
  assert.equal(r1.ok, true, JSON.stringify(r1.checks));
  assert.equal(r1.skipped, undefined);   // 世代が新しい = 入れ替える
  const ALL = [...FX.BUSINESS_TABLES, 'm_products_builds', 'cdb_publish_generations', 'cdb_publish_values'];
  const meta = () => db.prepare('SELECT key, value, updated_at FROM sync_meta WHERE key = ?').get(MP.PUBLISH_CURRENT_KEY);
  const before = { all: FX.tablesDigest(db, ALL).all, meta: meta(), build: snap().build.build_id };
  // 同じ NE の取得・同じ世代 = 何も書かない (業務の表 7 つの全部の行 (updated_at も)・作り直しの記録・世代の表・印が同じ)
  await new Promise((res) => setTimeout(res, 5));   // 時刻が進んでも
  const c0 = FX.totalChanges(db);
  const r2 = await rebuild();
  const skipDelta = FX.totalChanges(db) - c0;
  assert.deepEqual([r2.ok, r2.skipped, r2.build_id], [true, 'unchanged', before.build]);
  assert.equal(FX.tablesDigest(db, ALL).all, before.all);
  assert.deepEqual(meta(), before.meta);
  // 書いたのは作業用の TEMP 表と作り直しの札だけ (業務の表を入れ替えると m_products・m_set_components の行の 2 倍以上増える)
  const rows = count('m_products') + count('m_set_components');
  const c1 = FX.totalChanges(db);
  db.prepare("UPDATE sync_meta SET value = value WHERE key = 'ne_api_products_complete_at'").run();   // (比べる物差し: 1 行の書き換え = 1)
  assert.equal(FX.totalChanges(db) - c1, 1);
  // NE の取得が新しい (毎朝の daily-sync = 取込の完了の印の時刻が新しい) = 今までどおり入れ替える (中身が同じでも updated_at が新しい・記録も足す)
  const newMark = () => db.prepare("UPDATE sync_meta SET value = ? WHERE key IN ('ne_api_products_complete_at', 'ne_api_setproducts_complete_at')").run(new Date().toISOString().replace('T', ' ').slice(0, 19));
  newMark();
  const c2 = FX.totalChanges(db);
  const r3 = await rebuild();
  const fullDelta = FX.totalChanges(db) - c2;
  assert.deepEqual([r3.ok, r3.skipped], [true, undefined]);
  assert.notEqual(snap().build.build_id, before.build);
  assert.ok(skipDelta + 2 * rows <= fullDelta, JSON.stringify({ skipDelta, fullDelta, rows }));
  // 作り直しの後に古い表 (持ち主が C の列) が書き換えられた = 同じ取得・同じ世代でも入れ替えて直す (何もしないにしない)
  const mid = snap().build.build_id;
  db.prepare("INSERT OR REPLACE INTO m_reorder_setting (sku, 推奨保有月数, 商品名, updated_by, synced_at) VALUES ('s-ne', 9, 'NE の単品', 'x', 'x')").run();   // C と違う行 (古い画面が書いた)
  const r4 = await rebuild();
  assert.deepEqual([r4.ok, r4.skipped], [true, undefined]);
  assert.notEqual(snap().build.build_id, mid);
  // 中身が違う (今までの決め方の入力 = 例外原価を足した) = 入れ替える
  const mid2 = snap().build.build_id;
  db.prepare("INSERT OR REPLACE INTO exception_genka (sku, genka, 商品名, synced_at) VALUES ('s-tax8', 66, NULL, 'x')").run();
  const r5 = await rebuild();
  assert.deepEqual([r5.ok, r5.skipped], [true, undefined]);
  assert.notEqual(snap().build.build_id, mid2);
  db.prepare("DELETE FROM exception_genka WHERE sku = 's-tax8'").run();
  assert.equal((await rebuild()).ok, true);
  FX.markComplete(db, readNeRawRev);   // 印を元の時刻に戻す (ほかの試験と同じ材料)
  await nightly();
});

await ta('[22] 影運転の数: C が空で古い表に行がある推奨保有月数も「変わる」に数える (null も値)', async () => {
  db.prepare("INSERT OR REPLACE INTO m_reorder_setting (sku, 推奨保有月数, 商品名, updated_by, synced_at) VALUES ('s-ne', 3, 'NE の単品', 'x', 'x')").run();
  const one = (x) => ({ skus: [{ code: 's-ne', code_norm: 's-ne', sku_kind: 'single', name: mp('s-ne').商品名, handling: 'active', reorder_months: null, ...x }], costs: new Map(), primary: new Map(), has0027: true });
  assert.equal(F.shadowCounts(db, one({})).by_col.reorder_months, 1);                  // C が空・古い表は 3 = 全部 C にしたら行を消す = 変わる
  assert.equal(F.shadowCounts(db, one({ reorder_months: 3 })).by_col.reorder_months, 0);
  assert.equal(F.shadowCounts(db, one({ reorder_months: 4 })).by_col.reorder_months, 1);
  db.prepare("DELETE FROM m_reorder_setting WHERE sku = 's-ne'").run();
  assert.equal(F.shadowCounts(db, one({})).by_col.reorder_months, 0);                  // どちらも空 = 同じ
  assert.equal(F.shadowCounts(db, one({ reorder_months: 2 })).by_col.reorder_months, 1);   // C に値・古い表に行が無い = 変わる
});

await ta('[23] 作り直しを飛ばした・止まった朝 (写しだけ新しい世代) = 遅れ (exit 1)・broken にしない (A C を直した朝 / B NE の新しい商品が C に入っただけの朝)', async () => {
  const own = OWN('sku_costs');
  const pings = [];
  const deps = (runId) => ({ now: new Date(), log: quiet, ping: async (id, p) => { pings.push(p.status); }, openSqlite: async () => db,
    env: { DATA_DIR: tmp, COMPANY_DB_WATCH_URL: 'postgres://test', DAILY_SYNC_RUN_ID: runId }, connectFor: () => async () => ({ db: pdb, close: async () => {} }) });
  // 前の朝: 写し → 作り直し → 確かめ = verified (世代 G1)
  await nightly(own);
  assert.equal((await fetchGen(own)).state, 'verified');
  assert.equal((await rebuild()).ok, true);
  assert.equal((await quietly(() => F.cli(['--verify-apply', '--daily'], deps('ds_test_publish')))).code, 0);
  const g1 = snap().build.cdb_publish_generation_no;
  /** 次の朝 (別の daily-sync の回): 夜間ロード → 写し (新しい世代) → 作り直しを飛ばす or 止まる → 確かめ */
  const nextMorning = async (runId, { stopRebuild = false } = {}) => {
    await nightly(own);
    process.env.DAILY_SYNC_RUN_ID = runId;
    try {
      const f = await quietly(() => F.cli(['--daily'], deps(runId)));
      assert.equal(f.code, 0, f.last);   // 写しは取れた (新しい世代・印も進む)
      assert.ok(pointer() > g1);
      if (stopRebuild) {   // 作り直しが止まった (入れ替えない・記録も書かない)
        const r = await rebuild(OWN('skus.handling', 'products.status'));
        assert.deepEqual([r.ok, r.problem], [false, 'ownership_mismatch']);
      }
      const v = await quietly(() => F.cli(['--verify-apply', '--daily'], deps(runId)));
      const ev = evidence();
      return { v, ev };
    } finally { process.env.DAILY_SYNC_RUN_ID = 'ds_test_publish'; }
  };
  // (A) C の原価を直した朝に NE の取得が失敗 = 作り直しを飛ばした
  await putCost('s-ne', 177);
  let { v, ev } = await nextMorning('ds_next_a');
  assert.equal(v.code, 1, v.last);   // exit 4 ではない = 後の工程は止めない
  assert.deepEqual([ev.apply.broken, ev.apply.lag, ev.apply.generation_no, ev.apply.current_generation_no], [false, true, g1, pointer()]);
  assert.ok(['build_not_this_run', 'build_generation_not_today', 'generation_moved'].every((p) => ev.apply.problems.includes(p)), JSON.stringify(ev.apply.problems));
  assert.ok(!ev.apply.problems.some((p) => /^applied_/.test(p)), JSON.stringify(ev.apply.problems));   // m_products は作り直しが使った世代 G1 のまま = 違わない
  assert.match(v.last, /遅れ \(m_products は世代 \d+ のまま/);
  assert.equal(mp('s-ne').原価 === 177, false);   // C の新しい原価は翌朝の作り直しで入る
  // (A2) 作り直しが止まった朝も同じ (前の m_products のまま = 遅れ)
  ({ v, ev } = await nextMorning('ds_next_a2', { stopRebuild: true }));
  assert.deepEqual([v.code, ev.apply.broken, ev.apply.lag], [1, false, true]);
  // (B) 何も直さず、NE の新しい商品が夜間ロードで C に入っただけの朝 (作り直しを飛ばした)
  addNe('s-new2', 'NE の新しい商品 2', 66);
  publishMirror();
  ({ v, ev } = await nextMorning('ds_next_b'));
  assert.deepEqual([v.code, ev.apply.broken, ev.apply.lag], [1, false, true], JSON.stringify(ev.apply.problems));
  assert.ok(!ev.apply.problems.some((p) => /^applied_/.test(p)), JSON.stringify(ev.apply.problems));
  assert.deepEqual(pings.slice(-3), ['fail', 'fail', 'fail']);   // 確かめられない = fail の ping (ok にしない)
  // 翌朝 (写し → 作り直し) で届く (C の原価・新しい商品)
  assert.equal((await fetchGen(own)).state, 'verified');
  assert.equal((await rebuild()).ok, true);
  assert.equal((await quietly(() => F.cli(['--verify-apply', '--daily'], deps('ds_test_publish')))).code, 0);
  assert.equal(mp('s-ne').原価, 177);
  db.prepare("DELETE FROM raw_ne_products WHERE 商品コード = 's-new2'").run(); FX.markComplete(db, readNeRawRev);
  await nightly();
});

/** 試験だけ: 今の作り直しを確かめた safe の行を直接置く */
function writeGateForTest(G, sqlite, build) {
  G.writePublishGate(sqlite, { state: 'safe', reason: 'test', buildId: build.build_id, generationNo: build.cdb_publish_generation_no, appliedHash: build.cdb_publish_applied_hash,
    ownershipHash: build.cdb_publish_ownership_hash, checkedAt: new Date().toISOString() });
}

await ta('[24] 止めるかどうかの正 = warehouse.db の門 (safe / broken / unknown): 違う = broken (証跡が書けなくても exit 4)・遅れ・証跡が無い = 前の値のまま・safe に戻すのは通った確かめだけ・再試行と手の更新も同じ門', async () => {
  const G = await import('../apps/warehouse/publish-gate.js');
  const R = await import('../apps/warehouse/retry-failed-jobs.js');
  const P = await import('../apps/warehouse/pml-fba-refresh.js');
  const gate = () => G.readPublishGate({ db });
  const row = () => G.readGateRow(db);
  const pings = [];
  const deps = (extra = {}) => ({ now: new Date(), log: quiet, ping: async (id, p) => { pings.push(p.status); }, openSqlite: async () => db,
    env: { DATA_DIR: tmp, COMPANY_DB_WATCH_URL: 'postgres://test', DAILY_SYNC_RUN_ID: 'ds_test_publish' }, connectFor: () => async () => ({ db: pdb, close: async () => {} }), ...extra });
  const verify = (extra) => quietly(() => F.cli(['--verify-apply', '--daily'], deps(extra)));
  db.prepare('DELETE FROM cdb_publish_gate').run();
  // (a) 持ち主が全部 load・行が無い = 流す (今の世代を使った作り直しがある)・確かめが通ったら全部 load でも safe を書く (#1564 Codex R4 Medium 1 =
  //     作り直しを飛ばした朝も、確かめた作り直しのままなら行で流せる)
  await nightly();
  assert.equal((await fetchGen(MASTER_OWNERSHIP)).state, 'verified');
  assert.equal((await rebuild()).ok, true);
  assert.deepEqual([gate().state, gate().open, gate().source], ['safe', true, 'implicit']);
  assert.equal((await verify()).code, 0);
  assert.deepEqual([row().state, row().build_id, gate().source], ['safe', snap().build.build_id, 'row']);
  db.prepare('DELETE FROM cdb_publish_gate').run();
  // (b) 持ち主が C: 確かめる前 (行が無い) = unknown = 止める
  const own = OWN('sku_costs');
  await nightly(own);
  assert.equal((await fetchGen(own)).state, 'verified');
  assert.equal((await rebuild()).ok, true);
  assert.deepEqual([gate().state, gate().open, gate().reason], ['unknown', false, 'no_gate_row_with_company_owner']);
  assert.equal((await verify()).code, 0);   // 通った = safe
  assert.deepEqual([row().state, gate().open], ['safe', true]);
  // (c) 作り直しの後に古い表が書き換えられた = broken (exit 4)
  const origCost = mp('s-ne').原価;
  db.prepare("UPDATE m_products SET 原価 = 1 WHERE 商品コード = 's-ne'").run();
  let v = await verify();
  assert.deepEqual([v.code, row().state, gate().open], [4, 'broken', false]);
  // (d) 証跡が書けなくても exit 4 (門は先に書いた)
  db.prepare('DELETE FROM cdb_publish_gate').run();
  v = await verify({ write: () => { throw new Error('ディスクがいっぱい'); } });
  assert.equal(v.code, 4, v.last);
  assert.match(v.last, /書けない: 証跡 ディスクがいっぱい/);
  assert.equal(row().state, 'broken');
  // (e) 古い表を手で戻した後の遅れの朝 (写しだけ新しい世代・作り直しを飛ばした = 確かめは通らない) = broken のまま (遅れで消さない)
  db.prepare('UPDATE m_products SET 原価 = ? WHERE 商品コード = ?').run(origCost, 's-ne');
  await nightly(own);
  process.env.DAILY_SYNC_RUN_ID = 'ds_gate_next';
  try {
    assert.equal((await quietly(() => F.cli(['--daily'], deps({ env: { DATA_DIR: tmp, COMPANY_DB_WATCH_URL: 'postgres://test', DAILY_SYNC_RUN_ID: 'ds_gate_next' } })))).code, 0);
    v = await quietly(() => F.cli(['--verify-apply', '--daily'], deps({ env: { DATA_DIR: tmp, COMPANY_DB_WATCH_URL: 'postgres://test', DAILY_SYNC_RUN_ID: 'ds_gate_next' } })));
  } finally { process.env.DAILY_SYNC_RUN_ID = 'ds_test_publish'; }
  assert.equal(v.code, 1, v.last);
  assert.ok(!/applied_/.test(v.last), v.last);   // 作り直しの世代とは同じ (遅れだけ)
  assert.deepEqual([row().state, gate().open], ['broken', false]);
  // (f) 証跡が消えた・10 日前の broken = 門は broken のまま (日付で消えない・証跡を正にしない)
  fs.rmSync(path.join(tmp, 'company-db-evidence'), { recursive: true, force: true });
  db.prepare("UPDATE cdb_publish_gate SET checked_at = '2026-09-01T00:00:00.000Z' WHERE id = 1").run();
  assert.deepEqual([gate().state, gate().open], ['broken', false]);
  // 再試行・商品管理リストの手の更新も同じ門で止まる
  const ran = [];
  const run = (script, name) => { ran.push(name); return { success: true, summary: 'ok' }; };
  const res = R.runRetryRound(['f_sales', 'Render同期', 'マスタ照合'], { run, log: quiet, publishGate: gate() });
  assert.deepEqual(ran, ['マスタ照合', '新商品の許可', 'CompanyDB見張り']);
  assert.deepEqual(res.filter((x) => x.gated).map((x) => [x.name, x.blocked, x.pingJobId]),
    [['f_sales', true, null], ['Render同期', true, null], ['ロジザード毎日の商品マスタ(影)', true, 'lz-daily-build']]);
  assert.ok(res.filter((x) => x.gated).every((x) => /^⚠️ 見送り: Company DB の写しの反映が世代と違う .*門 cdb_publish_gate = broken/.test(x.summary)), JSON.stringify(res));
  const calls = [];
  const pml = (g) => ({ refresh: async () => { calls.push('refresh'); return { row_count: 1, fetched_at: 'x' }; }, build: async () => { calls.push('build'); return { ok: true, run_id: 'r' }; },
    sync: async () => { calls.push('sync'); return { state: 'sent', count: 1 }; }, gate: g });
  await assert.rejects(P.runPmlFbaRefresh(pml(gate)), (e) => e.code === 'PUBLISH_BROKEN' && /門 = broken/.test(e.message));
  assert.deepEqual(calls, []);
  let n = 0;
  await assert.rejects(P.runPmlFbaRefresh(pml(() => (n++ >= 1 ? gate() : { broken: false, open: true }))), (e) => e.code === 'PUBLISH_BROKEN');   // 取った後に閉じた = 作らない
  assert.deepEqual(calls, ['refresh']);
  // (g) 直した (作り直し) → 通った確かめ = safe に戻る (戻せるのはこれだけ)
  assert.equal((await fetchGen(own)).state, 'verified');
  assert.equal((await rebuild()).ok, true);
  assert.equal((await verify()).code, 0);
  assert.deepEqual([row().state, gate().open], ['safe', true]);
  ran.length = 0; calls.length = 0;
  R.runRetryRound(['f_sales', 'Render同期', 'マスタ照合'], { run, log: quiet, publishGate: gate() });
  assert.deepEqual(ran, ['f_sales', 'Render同期', 'マスタ照合', '新商品の許可', 'ロジザード毎日の商品マスタ(影)', 'CompanyDB見張り']);
  assert.equal((await P.runPmlFbaRefresh(pml(gate))).pml_run_id, 'r');
  assert.deepEqual([row().build_id, row().generation_no, row().applied_hash, row().ownership_hash],
    [snap().build.build_id, snap().build.cdb_publish_generation_no, snap().build.cdb_publish_applied_hash, snap().build.cdb_publish_ownership_hash]);   // safe は確かめたものを持つ
  // ── #1564 Codex R3 High 2 ──
  // (i) safe の行の後に作り直した (まだ確かめていない) = その safe は使わない・持ち主が C = unknown → 確かめが通れば safe
  db.prepare("UPDATE raw_ne_products SET 商品名 = 商品名 WHERE 商品コード = 's-ne'").run(); FX.markComplete(db, readNeRawRev);
  assert.equal((await rebuild()).ok, true);
  assert.notEqual(snap().build.build_id, row().build_id);
  assert.deepEqual([row().state, gate().state, gate().open, gate().reason], ['safe', 'unknown', false, 'safe_row_stale:build_changed']);
  assert.equal((await verify()).code, 0);
  assert.deepEqual([gate().state, gate().open, gate().source], ['safe', true, 'row']);
  // (j) 違うと分かったのに broken を書けなかった (門の UPDATE が落ちた) = 前の safe が残る → 読み手は今の古い表から作り直したハッシュと比べて使わない =
  //     再試行・商品管理リストの手の更新も止まる (exit 4 は daily-sync が止める)
  const gateWriteFails = new Proxy(db, { get(t, k) {
    if (k === 'prepare') return (sql) => { if (/INSERT INTO cdb_publish_gate/.test(sql)) throw new Error('門を書けない (試験)'); return t.prepare(sql); };
    const v = t[k]; return typeof v === 'function' ? v.bind(t) : v;
  } });
  const origCost2 = mp('s-ne').原価;
  db.prepare("UPDATE m_products SET 原価 = 2 WHERE 商品コード = 's-ne'").run();
  v = await verify({ openSqlite: async () => gateWriteFails });
  assert.equal(v.code, 4, v.last);
  assert.match(v.last, /書けない: 門 門を書けない/);
  assert.equal(row().state, 'safe');   // 前の safe が残った
  //   止めの印 (DATA_DIR のファイル。#1564 Codex R6 Low) が先に止める (broken)・印が無くても、行は今の古い表から作り直したハッシュで使わない
  assert.deepEqual([gate().state, gate().open, gate().source], ['broken', false, 'stop_mark']);
  assert.match(gate().reason, /^stop_mark: applied_mismatch.*門を書けない \(試験\)/);
  assert.ok(fs.existsSync(path.join(tmp, G.GATE_STOP_MARK)));
  assert.deepEqual([G.gateOfDb(db).state, G.gateOfDb(db).reason], ['unknown', 'safe_row_stale:applied_changed_now']);
  ran.length = 0; calls.length = 0;
  R.runRetryRound(['f_sales', 'Render同期', 'マスタ照合'], { run, log: quiet, publishGate: gate() });
  assert.deepEqual(ran, ['マスタ照合', '新商品の許可', 'CompanyDB見張り']);
  await assert.rejects(P.runPmlFbaRefresh(pml(gate)), (e) => e.code === 'PUBLISH_BROKEN');
  assert.deepEqual(calls, []);
  db.prepare('UPDATE m_products SET 原価 = ? WHERE 商品コード = ?').run(origCost2, 's-ne');
  assert.equal(G.gateOfDb(db).open, true);   // 行は確かめたときと同じに戻った
  assert.equal(gate().open, false);          // 止めの印は残る = 通った確かめまで止まったまま
  assert.equal((await verify()).code, 0);
  assert.deepEqual([gate().state, gate().open, gate().source, fs.existsSync(path.join(tmp, G.GATE_STOP_MARK))], ['safe', true, 'row', false]);   // safe を書けた = 印を消した
  // (k) 門の表はあるが読めない (SELECT が落ちる) = unknown (「行が無い」= 全部 load なら流す、と同じにしない)。確かめは通っても unreadable を記録
  const gateReadFails = new Proxy(db, { get(t, k) {
    if (k === 'prepare') return (sql) => { if (/FROM cdb_publish_gate WHERE id = 1/.test(sql)) throw new Error('門を読めない (試験)'); return t.prepare(sql); };
    const v = t[k]; return typeof v === 'function' ? v.bind(t) : v;
  } });
  const gr = G.readPublishGate({ db: gateReadFails });
  assert.deepEqual([gr.state, gr.open], ['unknown', false]);
  assert.match(gr.reason, /^gate_unreadable: 門を読めない/);
  v = await verify({ openSqlite: async () => gateReadFails });
  assert.equal(v.code, 0, v.last);   // 確かめは通った = safe を書き直す
  assert.deepEqual([evidence().apply.gate.before, evidence().apply.gate.after, row().state], ['unreadable', 'safe', 'safe']);
  // (l) 行が無い = 全部 load と分かるときだけ流す: 今の世代が無い・作り直しが無い・作り直しが世代を使っていない = unknown
  db.prepare('DELETE FROM cdb_publish_gate').run();
  await nightly();
  assert.equal((await fetchGen(MASTER_OWNERSHIP)).state, 'verified');
  assert.equal((await rebuild()).ok, true);
  assert.deepEqual([gate().state, gate().open, gate().source], ['safe', true, 'implicit']);
  const hide = (re, val) => new Proxy(db, { get(t, k) {
    if (k === 'prepare') return (sql) => (re.test(sql) ? { get: () => val, all: () => [] } : t.prepare(sql));
    const vv = t[k]; return typeof vv === 'function' ? vv.bind(t) : vv;
  } });
  const noGen = G.readPublishGate({ db: hide(/SELECT value FROM sync_meta WHERE key = \?/, undefined) });
  assert.deepEqual([noGen.state, noGen.open, noGen.reason], ['unknown', false, 'no_gate_row_unverified:no_current_generation:no_generation']);
  const noBuild = G.readPublishGate({ db: hide(/FROM m_products_builds ORDER BY/, undefined) });
  assert.deepEqual([noBuild.state, noBuild.open, noBuild.reason], ['unknown', false, 'no_gate_row_unverified:no_build']);
  const genless = { ...snap().build, cdb_publish_generation_no: null };
  const noPub = G.readPublishGate({ db: hide(/FROM m_products_builds ORDER BY/, genless) });
  assert.deepEqual([noPub.state, noPub.open, noPub.reason], ['unknown', false, 'no_gate_row_unverified:build_without_generation']);
  // 古い safe の行でも、全部 load と分かれば流す (写す値が無い)
  writeGateForTest(G, db, snap().build);
  db.prepare("UPDATE cdb_publish_gate SET build_id = 'old_build' WHERE id = 1").run();
  assert.deepEqual([gate().state, gate().open, gate().reason], ['safe', true, 'all_load_safe_row_stale:build_changed']);
  db.prepare('DELETE FROM cdb_publish_gate').run();
  // (m) 写しが今の世代 N を作った後に作り直しが失敗した・飛ばした (最新の作り直しは前の世代 M のまま) = 全部 load でも「分かる」にしない (#1564 Codex R4 Medium 1)
  const buildM = snap().build;
  const gN = await fetchGen(MASTER_OWNERSHIP);
  assert.equal(gN.state, 'verified');
  assert.notEqual(gN.generation_no, buildM.cdb_publish_generation_no);
  const noRowN = gate();
  assert.deepEqual([noRowN.state, noRowN.open, noRowN.reason], ['unknown', false, 'no_gate_row_unverified:build_not_current_generation']);
  writeGateForTest(G, db, buildM);
  db.prepare("UPDATE cdb_publish_gate SET build_id = 'older_build' WHERE id = 1").run();   // 前の safe の行 (今の作り直しを確かめていない)
  assert.deepEqual([gate().state, gate().open, gate().reason], ['unknown', false, 'safe_row_stale:build_changed']);
  writeGateForTest(G, db, buildM);   // 今の作り直し M を確かめた safe (作り直しを飛ばした朝 = 遅れ) = 行で流す (m_products は確かめた M のまま)
  assert.deepEqual([gate().state, gate().open, gate().source], ['safe', true, 'row']);
  db.prepare('DELETE FROM cdb_publish_gate').run();
  assert.equal((await rebuild()).ok, true);   // 作り直しが N を使った = 全部 load と分かる
  assert.deepEqual([gate().state, gate().open, gate().source], ['safe', true, 'implicit']);
  // (n) 確かめが通らなかった朝 (#1564 Codex R5 Medium): 行が無く全部 load (暗黙の safe = 最初の朝・warehouse.db を戻した朝) でも
  //     決める前に落ちた = 門を unknown に残す (自動再試行・商品管理リストの手の更新も止まる)・daily-sync はこの回の 28 工程を止める
  let vc = await verify({ verify: async () => { throw new Error('落ちた (試験)'); } });
  assert.equal(vc.code, 1, vc.last);
  assert.match(vc.last, /落ちた \(試験\) \(門 = unknown\)/);
  assert.deepEqual([row().state, gate().state, gate().open], ['unknown', 'unknown', false]);
  assert.match(row().reason, /^verify_apply_crashed: 落ちた/);
  ran.length = 0; calls.length = 0;
  R.runRetryRound(['f_sales', 'Render同期', 'マスタ照合'], { run, log: quiet, publishGate: gate() });
  assert.deepEqual(ran, ['マスタ照合', '新商品の許可', 'CompanyDB見張り']);
  await assert.rejects(P.runPmlFbaRefresh(pml(gate)), (e) => e.code === 'PUBLISH_BROKEN');
  assert.deepEqual(calls, []);
  //     daily-sync の判断: 確かめが exit 1 で門が暗黙の safe = 止める / 門が確かめた safe の行 = 流す / exit 4 = broken
  const implicitSafe = { open: true, state: 'safe', source: 'implicit', reason: 'all_load_no_gate_row' };
  assert.deepEqual([G.gateAfterVerify({ apply: { success: false, exitCode: 1 }, gate: implicitSafe }).broken, G.gateAfterVerify({ apply: { success: false, exitCode: 1 }, gate: implicitSafe }).state],
    [true, 'unknown']);
  assert.equal(G.gateAfterVerify({ apply: { success: true, exitCode: 0 }, gate: implicitSafe }).broken, false);
  assert.equal(G.gateAfterVerify({ apply: { success: false, exitCode: 1 }, gate: { open: true, state: 'safe', source: 'row', reason: 'verified' } }).broken, false);
  assert.deepEqual(G.gateAfterVerify({ apply: { success: false, exitCode: 4 }, gate: { open: true, state: 'safe', source: 'row', reason: 'verified' } }).state, 'broken');
  //     遅れだけの決めた失敗 (今朝の写し・作り直しが別の回) で古い表が作り直しの世代と同じ = その作り直しの safe を書く (exit 1 のまま。#1564 Codex R6 Medium)
  db.prepare('DELETE FROM cdb_publish_gate').run();
  vc = await verify({ env: { DATA_DIR: tmp, COMPANY_DB_WATCH_URL: 'postgres://test', DAILY_SYNC_RUN_ID: 'ds_other_run' } });
  assert.equal(vc.code, 1, vc.last);
  assert.deepEqual([row().state, row().build_id, gate().open, gate().source], ['safe', snap().build.build_id, true, 'row']);
  assert.match(row().reason, /^lag_verified: /);
  //     遅れでない決めた失敗 (作り直しが使った世代の持ち主が読めない) = 確かめた safe の行が無ければ unknown を残す
  db.prepare('DELETE FROM cdb_publish_gate').run();
  const bpNo = snap().build.cdb_publish_generation_no;
  const ownSaved = db.prepare('SELECT ownership FROM cdb_publish_generations WHERE generation_no = ?').get(bpNo).ownership;
  db.prepare("UPDATE cdb_publish_generations SET ownership = '{壊れた' WHERE generation_no = ?").run(bpNo);
  try {
    vc = await verify();
    assert.equal(vc.code, 1, vc.last);
    assert.match(vc.last, /generation_ownership_unreadable/);
    assert.equal(row().state, 'unknown');
  } finally { db.prepare('UPDATE cdb_publish_generations SET ownership = ? WHERE generation_no = ?').run(ownSaved, bpNo); }
  assert.equal(gate().open, false);
  //     broken は unknown に替えない (戻せるのは通った確かめだけ)
  db.prepare('DELETE FROM cdb_publish_gate').run();
  G.writePublishGate(db, { state: 'broken', reason: 'test', checkedAt: new Date().toISOString() });
  vc = await verify({ verify: async () => { throw new Error('落ちた (試験 2)'); } });
  assert.match(vc.last, /門 = broken のまま/);
  assert.equal(row().state, 'broken');
  // (o) 前の確かめた safe の行が今も合う遅れの朝 (写しだけ新しい世代・作り直しは前のまま) = 流す (行のまま・daily-sync も再試行も止めない)
  db.prepare('DELETE FROM cdb_publish_gate').run();
  assert.equal((await verify()).code, 0);
  assert.deepEqual([row().state, gate().source], ['safe', 'row']);
  assert.equal((await fetchGen(MASTER_OWNERSHIP)).state, 'verified');   // 写しだけ新しい世代 (作り直しは飛ばした)
  vc = await verify();
  assert.equal(vc.code, 1, vc.last);   // 遅れ
  assert.deepEqual([row().state, gate().state, gate().open, gate().source], ['safe', 'safe', true, 'row']);
  assert.equal(G.gateAfterVerify({ apply: { success: false, exitCode: 1 }, gate: gate() }).broken, false);
  ran.length = 0;
  R.runRetryRound(['f_sales', 'Render同期', 'マスタ照合'], { run, log: quiet, publishGate: gate() });
  assert.deepEqual(ran, ['f_sales', 'Render同期', 'マスタ照合', '新商品の許可', 'ロジザード毎日の商品マスタ(影)', 'CompanyDB見張り']);
  db.prepare('DELETE FROM cdb_publish_gate').run();
  assert.equal((await rebuild()).ok, true);
  // (p) 写しが取れない朝 (Company DB に届かない)・NE と作り直しは通った (前の世代で新しい作り直し B1)・確かめは exit 1 (fetch_not_verified) =
  //     古い表は B1 の世代と同じ = B1 の safe を書く = daily-sync・再試行・商品管理リストの手の更新は流す (今の全部 load の本番のふつうの朝。#1564 Codex R6 Medium)
  assert.equal((await verify()).code, 0);   // 前の朝 = B0 の safe
  const b0 = row().build_id;
  process.env.DAILY_SYNC_RUN_ID = 'ds_day2';
  try {
    const env2 = { DATA_DIR: tmp, COMPANY_DB_WATCH_URL: 'postgres://test', DAILY_SYNC_RUN_ID: 'ds_day2' };
    const genBefore = pointer();
    const f2 = await quietly(() => F.cli(['--daily'], deps({ env: env2, connectFor: () => async () => { throw new Error('Company DB に届かない (試験)'); } })));
    assert.equal(f2.code, 1, f2.last);
    assert.equal(pointer(), genBefore);   // 写しの印は動かない
    db.prepare("UPDATE raw_ne_products SET 商品名 = 商品名 WHERE 商品コード = 's-ne'").run(); FX.markComplete(db, readNeRawRev);   // NE は通った (取得の印が新しい)
    assert.equal((await rebuild()).ok, true);
    const b1 = snap().build;
    assert.deepEqual([b1.build_id !== b0, b1.cdb_publish_generation_no, b1.daily_sync_run_id], [true, genBefore, 'ds_day2']);   // 前の世代で新しい作り直し
    assert.deepEqual([G.readPublishGate({ dataDir: tmp }).open, G.readPublishGate({ dataDir: tmp }).reason], [true, 'all_load_safe_row_stale:build_changed']);
    pings.length = 0;
    const v2 = await quietly(() => F.cli(['--verify-apply', '--daily'], deps({ env: env2 })));
    assert.equal(v2.code, 1, v2.last);   // 確かめは通っていない (今朝の写しが無い) = exit 1・fail の ping のまま
    assert.deepEqual(pings, ['fail']);
    assert.match(v2.last, /fetch_not_verified.*門 safe \(後の工程は流す\)/);
    assert.deepEqual([row().state, row().build_id, row().reason], ['safe', b1.build_id, 'lag_verified: fetch_not_verified']);
    const g2 = G.readPublishGate({ dataDir: tmp });
    assert.deepEqual([g2.state, g2.open, g2.source], ['safe', true, 'row']);
    assert.equal(G.gateAfterVerify({ apply: { success: false, exitCode: v2.code }, gate: g2 }).broken, false);   // daily-sync は流す
    ran.length = 0; calls.length = 0;
    R.runRetryRound(['f_sales', 'Render同期', 'マスタ照合'], { run, log: quiet, publishGate: G.readPublishGate({ dataDir: tmp }) });
    assert.deepEqual(ran, ['f_sales', 'Render同期', 'マスタ照合', '新商品の許可', 'ロジザード毎日の商品マスタ(影)', 'CompanyDB見張り']);
    assert.equal((await P.runPmlFbaRefresh(pml(gate))).pml_run_id, 'r');
  } finally { process.env.DAILY_SYNC_RUN_ID = 'ds_test_publish'; }
  // (q) 止めの印 (#1564 Codex R6 Low): warehouse.db を開けない最初の朝 = 印を残す → 開けるようになっても (行が無く全部 load の暗黙の safe でも) 止まったまま →
  //     通った確かめで消える / 門の書き込みが SQLITE_BUSY (落ちた回の unknown も書けない) → 故障が直っても止まったまま → 通った確かめで消える
  const stopFile = path.join(tmp, G.GATE_STOP_MARK);
  const busyErr = () => Object.assign(new Error('database is locked (試験)'), { code: 'SQLITE_BUSY' });
  const stopped = async () => {
    assert.deepEqual([gate().state, gate().open, gate().source], ['unknown', false, 'stop_mark']);
    assert.equal(G.readPublishGate({ dataDir: tmp }).open, false);
    assert.equal(G.gateAfterVerify({ apply: { success: true, exitCode: 0 }, gate: G.readPublishGate({ dataDir: tmp }) }).broken, true);
    ran.length = 0; calls.length = 0;
    R.runRetryRound(['f_sales', 'Render同期', 'マスタ照合'], { run, log: quiet, publishGate: G.readPublishGate({ dataDir: tmp }) });
    assert.deepEqual(ran, ['マスタ照合', '新商品の許可', 'CompanyDB見張り']);
    await assert.rejects(P.runPmlFbaRefresh(pml(gate)), (e) => e.code === 'PUBLISH_BROKEN');
    assert.deepEqual(calls, []);
  };
  db.prepare('DELETE FROM cdb_publish_gate').run();
  assert.equal((await fetchGen(MASTER_OWNERSHIP)).state, 'verified');   // 今朝の写し (この回) → 作り直し = 後で確かめが通る朝
  assert.equal((await rebuild()).ok, true);
  assert.deepEqual([gate().source, gate().open, fs.existsSync(stopFile)], ['implicit', true, false]);   // 前提: 行が無く全部 load と分かる・印なし
  vc = await verify({ openSqlite: async () => { throw busyErr(); } });
  assert.equal(vc.code, 1, vc.last);
  assert.match(vc.last, /database is locked \(試験\) \(止めの印を残した\)/);
  assert.ok(fs.existsSync(stopFile));
  await stopped();   // 開けるようになった後も止まったまま
  assert.equal((await verify()).code, 0);
  assert.deepEqual([gate().open, gate().source, fs.existsSync(stopFile)], [true, 'row', false]);   // 通った確かめ = 印を消した
  db.prepare('DELETE FROM cdb_publish_gate').run();
  const busy = new Proxy(db, { get(t, k) {
    if (k === 'prepare') return (sql) => { if (/INSERT INTO cdb_publish_gate/.test(sql)) throw busyErr(); return t.prepare(sql); };
    const v = t[k]; return typeof v === 'function' ? v.bind(t) : v;
  } });
  vc = await verify({ openSqlite: async () => busy, verify: async () => { throw new Error('落ちた (試験 3)'); } });
  assert.match(vc.last, /落ちた \(試験 3\) \(門を書けない: database is locked \(試験\)・止めの印を残した\)/);
  assert.equal(row(), null);   // 門には何も書けていない
  await stopped();             // 故障が直った (db を普通に読む) 後も止まったまま
  assert.equal((await verify()).code, 0);
  assert.deepEqual([gate().open, gate().source, fs.existsSync(stopFile)], [true, 'row', false]);
  db.prepare('DELETE FROM cdb_publish_gate').run();
  // (h) 読めない (warehouse.db が無い・壊れた) = unknown = 止める
  assert.deepEqual([G.readPublishGate({ dataDir: path.join(tmp, 'no-such-dir') }).state, G.readPublishGate({ dataDir: path.join(tmp, 'no-such-dir') }).open], ['unknown', false]);
  assert.equal(G.readPublishGate({ dataDir: null }).open, false);
  assert.deepEqual([G.readPublishGate({ dataDir: tmp }).state, G.readPublishGate({ dataDir: tmp }).open], ['safe', true]);   // ファイルから読み取り専用で開いても同じ
  // 配線: daily-sync (exit 4 と門)・再試行・手の更新が同じ読み手
  assert.match(fs.readFileSync(path.join(ROOT, 'apps/warehouse/daily-sync.js'), 'utf8'), /const cdbPublishGateNow = readPublishGate\(\{ dataDir: process\.env\.DATA_DIR \|\| path\.join\(PROJECT_DIR, 'data'\) \}\);/);
  const rsrc = fs.readFileSync(path.join(ROOT, 'apps/warehouse/retry-failed-jobs.js'), 'utf8');
  assert.match(rsrc, /const publishGate = readPublishGate\(\{ dataDir: process\.env\.DATA_DIR \|\| path\.join\(PROJECT_DIR, 'data'\) \}\);/);
  assert.match(rsrc, /const results = runRetryRound\(state\.remaining_jobs, \{ publishGate \}\);/);
  const fsrc = fs.readFileSync(path.join(ROOT, 'apps/warehouse/fba-service.js'), 'utf8');
  assert.match(fsrc, /runPmlFbaRefresh\(\{ updateProgress, refresh: refreshFbaLive, build: buildProductManagementSnapshot, sync: syncPmlSnapshotOnly,\s*gate: \(\) => readPublishGate\(\{ db: wdb \}\) \}\)/);
  assert.doesNotMatch(fsrc, /await buildProductManagementSnapshot\(\{ fbaSource: 'live' \}\)/);
  db.prepare('DELETE FROM cdb_publish_gate').run();
  await nightly();
});

await ta('[25] 記録の後に足した列 (記録した持ち主に無い) = load として足す (壊れにしない)・知らない列・値 = 壊れ', async () => {
  const partial = OS.sortedOwnership(OWN('sku_costs'));
  delete partial['skus.reorder_months'];   // この列を足す前に記録した active
  const put = async (map) => q(`insert into ops.master_ownership_state (id, active_hash, active_map, activated_by) values (1, $1, $2::jsonb, 'test')
    on conflict (id) do update set active_hash = excluded.active_hash, active_map = excluded.active_map, prepared_hash = null, prepared_map = null, prepared_at = null, prepared_by = null`,
  [OS.ownershipHashOf(map), JSON.stringify(map)]);
  await put(partial);
  const st = await OS.readOwnershipState(pdb);
  assert.deepEqual([st.active.map['skus.reorder_months'], st.active.filled, st.active.stored_hash, st.active.hash],
    ['load', ['skus.reorder_months'], OS.ownershipHashOf(partial), OS.ownershipHashOf(OWN('sku_costs'))]);
  const lr = await loadNow();
  assert.deepEqual(lr.company_owned, ['sku_costs']);                 // 夜間ロードは止まらない
  assert.equal((await fetchGen(MASTER_OWNERSHIP)).state, 'verified');   // 写しも通る (夜間ロードの記録 = 足した後の持ち主)
  assert.equal(await quietly(() => EP.cli(['prepare'], { env: {}, connect: async () => ({ db: pdb, close: async () => {} }), log: quiet, ownership: OWN('sku_costs') })), 1);   // 実際に効く持ち主と同じ = 用意しない
  // 知らない列・知らない値 = 壊れ (推測で持ち主を決めない)
  await put({ ...partial, 'skus.no_such_col': 'load' });
  await assert.rejects(OS.readOwnershipState(pdb), (e) => e.code === 'OWNERSHIP_STATE_BROKEN' && /知らない列/.test(e.message));
  await put({ ...partial, sku_costs: 'maybe' });
  await assert.rejects(OS.readOwnershipState(pdb), (e) => e.code === 'OWNERSHIP_STATE_BROKEN');
  await nightly();
});

await ta('[26] NE と C で種類が違う SKU = その SKU だけ NE の値 (⚠️・証跡・止めない) / C のセットに NE にしか無い構成品 = 値が混ざる (⚠️・証跡)', async () => {
  // (M-5) NE で set-g をセットから単品にした (C はまだセット)・売上分類の持ち主は C (C のセットには売上分類の欄が無い)
  const own = OWN('products.sales_class');
  await nightly(own);
  db.prepare("DELETE FROM raw_ne_set_products WHERE セット商品コード = 'set-g'").run();
  addNe('set-g', 'セット G を単品に', 90);
  assert.equal((await fetchGen(own)).state, 'verified');
  const r = await rebuild();
  assert.equal(r.ok, true, JSON.stringify(r.checks));   // 1 件の食い違いで作り直し全部を止めない (前は value_missing)
  assert.deepEqual([r.publish.stats.kind_mismatch, r.publish.stats.kind_mismatch_codes], [1, ['set-g']]);
  assert.ok(r.warn.some((w) => /NE と Company DB で種類が違う SKU 1 件は NE の値のまま.*set-g/.test(w)), JSON.stringify(r.warn));
  assert.deepEqual([mp('set-g').商品区分, mp('set-g').売上分類], ['単品', 2]);   // NE の道 (product_sales_class の 2)
  const va = await F.runVerifyApply({ sqlite: db, dataDir: tmp, write: () => true, taxRates: TAX_RATES });
  assert.equal(va.state, 'verified', JSON.stringify(va.problems));
  assert.deepEqual(va.evidence.apply.kind_mismatch, { count: 1, codes: ['set-g'] });
  assert.match(va.line, /^⚠️ .*種類が違う SKU 1 件は NE の値のまま \(set-g\)/);
  const g2 = await fetchGen(own);   // 翌朝の写し (m_products は単品・C はセット) も受け入れる
  assert.deepEqual([g2.state, g2.evidence.kind_mismatch], ['verified', { count: 1, codes: ['set-g'] }]);
  db.prepare("DELETE FROM raw_ne_products WHERE 商品コード = 'set-g'").run();
  db.prepare("INSERT OR REPLACE INTO raw_ne_set_products (セット商品コード, セット商品名, セット販売価格, 商品コード, 数量, synced_at) VALUES ('set-g', 'セット G', 200, 's-sc3', 2, ?)").run(FX.T1);
  FX.markComplete(db, readNeRawRev);
  // (L-2) C にある set-a の構成品に NE にしか無い単品 s-only を足した (C は構成品を知らない) = 導いた値に C と NE が混ざる = ⚠️
  const own2 = OWN('sku_costs');
  await nightly(own2);
  addNe('s-only', 'NE にだけある構成品', 30);
  db.prepare("INSERT OR REPLACE INTO raw_ne_set_products (セット商品コード, セット商品名, セット販売価格, 商品コード, 数量, synced_at) VALUES ('set-a', 'セット A', 800, 's-only', 1, ?)").run(FX.T1);
  FX.markComplete(db, readNeRawRev);
  assert.equal((await fetchGen(own2)).state, 'verified');
  const r2 = await rebuild();
  assert.equal(r2.ok, true, JSON.stringify(r2.checks));
  assert.deepEqual([r2.publish.applied.counts.mixed_sets, r2.publish.applied.mixed_set_codes], [1, ['set-a']]);
  assert.ok(r2.warn.some((w) => /セットの構成品に NE にしか無い単品がある 1 件 .*set-a/.test(w)), JSON.stringify(r2.warn));
  const va2 = await F.runVerifyApply({ sqlite: db, dataDir: tmp, write: () => true, taxRates: TAX_RATES });
  assert.deepEqual([va2.state, va2.evidence.apply.mixed_sets], ['verified', { count: 1, codes: ['set-a'] }]);
  db.prepare("DELETE FROM raw_ne_set_products WHERE セット商品コード = 'set-a' AND 商品コード = 's-only'").run();
  db.prepare("DELETE FROM raw_ne_products WHERE 商品コード = 's-only'").run();
  FX.markComplete(db, readNeRawRev);
  assert.equal((await rebuild()).ok, true);
  await nightly();
});

await ta('[27] 持ち主の正を 1 つに: 0001〜0055 がそろう・company_owner に進む前提 = epoch が active・段階の owner_hash = active・画面の保存の門 = active', async () => {
  // (a) 積み方 (master 0050 → ⑤-1 0051 → ⑤-2a 0052 → ⑤-2b 0053 → ⑦-1 0054 → ④a 0055) がそろって入る・前提の差し込み口に 1 行
  assert.deepEqual((await q("select version from ops.schema_migrations where version in ('0051', '0052', '0053', '0054', '0055') order by 1")).map((r) => r.version), ['0051', '0052', '0053', '0054', '0055']);
  assert.deepEqual((await q('select name from ops.master_cutover_prereq_checks order by 1')).map((r) => r.name), ['0052_registrations', '0054_amazon_map', '0055_ownership_epoch']);
  // epoch の鍵 = 4705310055 (番号にそろえる)・ほかの鍵 (0036 の親子・0051 のマスタの書き込み) と重ならない
  assert.deepEqual(Object.values((await q('select ops.master_ownership_lock_key()::text as e, core.master_write_lock_key()::text as w, core.parent_lock_key()::text as p'))[0]), ['4705310055', '4705310051', '4705310036']);
  const own = OWN('sku_costs'), ownHash = OS.ownershipHashOf(own);
  const put = async (map) => q(`insert into ops.master_ownership_state (id, active_hash, active_map, activated_by) values (1, $1, $2::jsonb, 'test')
    on conflict (id) do update set active_hash = excluded.active_hash, active_map = excluded.active_map, prepared_hash = null, prepared_map = null, prepared_at = null, prepared_by = null`,
  [OS.ownershipHashOf(map), JSON.stringify(map)]);
  // (b) 前提 (⑤-1 の ops.set_master_cutover_phase が集める): company_owner・new_open に進むのは epoch が active のときだけ
  const probs = async (from = 'frozen', to = 'company_owner') => (await q('select ops.master_cutover_prereq_problems($1, $2) as p', [from, to]))[0].p.filter((x) => x.startsWith('0055_ownership_epoch'));
  await q('delete from ops.master_ownership_state');
  assert.match((await probs()).join(), /epoch_missing/);
  await put(OS.sortedOwnership(MASTER_OWNERSHIP));
  assert.match((await probs()).join(), /epoch_all_load/);
  await put(own);
  assert.deepEqual(await probs(), []);
  assert.deepEqual(await probs('company_owner', 'new_open'), []);
  await OS.prepareOwnership(pdb, { map: OWN('sku_costs', 'skus.shipping'), actor: 't' });
  assert.match((await probs()).join(), /epoch_prepared_pending/);
  await OS.cancelPrepared(pdb, { actor: 't' });
  assert.deepEqual(await probs('legacy_open', 'frozen'), []);   // frozen に進むのは前提なし (activate は frozen のときだけ = 先に止める)
  // (c) 段階の行 (⑤-1): company_owner に入る owner_hash = active の記録のハッシュだけ (証拠の owner_hash と epoch が同じ)
  await setCutoverPhase('frozen');
  const toOwner = async (oh) => {
    await pdb.query('begin');
    try { await q("select set_config('ops.cutover_protocol', '1', true)"); await q("update ops.master_cutover_state set phase = 'company_owner', owner_hash = $1 where id = 1", [oh]); await pdb.query('commit'); }
    catch (e) { await pdb.query('rollback'); throw e; }
  };
  await assert.rejects(toOwner('b'.repeat(64)), /cutover_epoch: .*owner_hash_not_active/);
  await put(OS.sortedOwnership(MASTER_OWNERSHIP));
  await assert.rejects(toOwner(OS.ownershipHashOf(MASTER_OWNERSHIP)), /cutover_epoch: .*epoch_all_load/);
  await put(own);
  await toOwner(ownHash);
  assert.deepEqual((await q('select phase, owner_hash from ops.master_cutover_state where id = 1'))[0], { phase: 'company_owner', owner_hash: ownHash });
  // (d) 画面の保存の門 (⑤-1 の ops.begin_master_write・本物の 9 つの引数 = 操作 sku_edit・SKU・編集の印・保存の中身のハッシュ・版):
  //   段階の記録と同じ持ち主表でも、epoch (active) と違えば始めない。取引は巻き戻す (保存の記録 done を書かないので commit はしない)
  await setCutoverPhase('new_open', ownHash);
  const skuNe = Number(await skuId('s-ne'));
  const begin = async () => {
    await pdb.query('begin');
    try {
      const v = (await q('select ops.master_edit_versions($1) as v', [skuNe]))[0].v;
      return (await q('select ops.begin_master_write($1::uuid, $2, null, $3::jsonb, $4, $5, $6, $7, $8::jsonb) as r',
        [crypto.randomUUID(), 'tester', JSON.stringify(own), 'sku_edit', skuNe, 'a'.repeat(64), 'b'.repeat(64), JSON.stringify(v)]))[0].r;
    } finally { await pdb.query('rollback'); }
  };
  // ⑤-2a の新商品の登録 (ops.register_new_sku) も同じ約束の表に sku_create の行を書く = 同じ確かめ (操作で分けない)。行の形は登録の関数と同じ
  const beginCreate = async () => {
    await pdb.query('begin');
    try {
      await q(`insert into ops.master_write_sessions (session_id, txid, request_id, operation, sku_id, derived_sku_ids, target_product_ids, edit_token, payload_hash, versions,
          actor_id, reason, source_system, db_user, phase, owner_hash, ownership)
        values (gen_random_uuid(), txid_current(), gen_random_uuid(), 'sku_create', $1, '{}'::bigint[], '{}'::bigint[], repeat('0', 64), $2, '{}'::jsonb,
          'tester', null, 'portal_master_edit', 'master_edit', 'new_open', $3, $4::jsonb)`, [skuNe, 'c'.repeat(64), ownHash, JSON.stringify(own)]);
      return true;
    } finally { await pdb.query('rollback'); }
  };
  // ⑤-2b (0053) の約束 (仕入先・ファイル = SKU なし) と ⑦-1 (0054) の Amazon の約束 (出品だけ = SKU なし) も同じ確かめ (操作で分けない・SKU の無い約束も)
  const beginOther = async (operation) => {
    const amazon = operation.startsWith('amazon_map_');
    await pdb.query('begin');
    try {
      // Amazon の約束の相手 = 出品 (試験の DB には無い = 取引の中だけ作る・巻き戻す)
      const listingId = amazon ? (await q("insert into core.listings (company_id, mall, listing_code) values (1, 'amazon', 'test-0055-map') returning listing_id::text as id"))[0].id : null;
      await q(`insert into ops.master_write_sessions (session_id, txid, request_id, operation, sku_id, listing_id, derived_sku_ids, target_product_ids, edit_token, payload_hash, versions,
          actor_id, reason, source_system, db_user, phase, owner_hash, ownership)
        values (gen_random_uuid(), txid_current(), gen_random_uuid(), $1, null, $2::bigint, '{}'::bigint[], '{}'::bigint[], repeat('0', 64), $3, '{}'::jsonb,
          'tester', null, $4, 'master_edit', 'new_open', $5, $6::jsonb)`, [operation, listingId, 'c'.repeat(64), amazon ? 'portal_amazon_map' : 'portal_master_edit', ownHash, JSON.stringify(own)]);
      return true;
    } finally { await pdb.query('rollback'); }
  };
  const OTHER_OPS = ['supplier_create', 'reg_csv_build', 'jan_edit', 'amazon_map_save', 'amazon_map_delete'];
  assert.equal((await begin()).phase, 'new_open');   // active = 画面の持ち主表 = 始められる
  assert.equal(await beginCreate(), true);
  for (const op of OTHER_OPS) assert.equal(await beginOther(op), true, op);
  await put(OWN('sku_costs', 'skus.shipping'));
  await assert.rejects(begin(), /before_cutover: 持ち主表が持ち主の epoch \(active\) と違う/);
  await assert.rejects(beginCreate(), /before_cutover: 持ち主表が持ち主の epoch \(active\) と違う/);   // 登録も同じ
  for (const op of OTHER_OPS) await assert.rejects(beginOther(op), /before_cutover: 持ち主表が持ち主の epoch \(active\) と違う/, op);
  const partial = { ...OS.sortedOwnership(own) }; delete partial['skus.reorder_months'];   // 記録の後に足した列 (画面では load) = load として同じ
  await put(partial);
  assert.equal((await begin()).phase, 'new_open');
  const noCost = { ...OS.sortedOwnership(own) }; delete noCost.sku_costs;   // 画面は company・記録に無い = load = 違う
  await put(noCost);
  await assert.rejects(begin(), /before_cutover: 持ち主表が持ち主の epoch/);
  await q('delete from ops.master_ownership_state');   // epoch の記録が無い = 全部 load = 違う
  await assert.rejects(begin(), /before_cutover: 持ち主表が持ち主の epoch/);
  await assert.rejects(beginCreate(), /before_cutover: 持ち主表が持ち主の epoch/);
  await setCutoverPhase('legacy_open');
  await nightly();
});

await ta('[28] 持ち主表のハッシュは 1 つの式 (load の列は数えない): 前の列の組で切り替えた後に列を足しても、画面の保存・登録は止まらない・夜間ロードは足した列を load で動かす (#1564 Codex R3 Medium)', async () => {
  const C = await import('../lib/master-cutover.mjs');
  const own = OS.sortedOwnership(OWN('sku_costs'));
  const older = { ...own }; delete older['skus.reorder_months'];   // 切替のとき (この列を OWNED_COLUMNS に足す前) の持ち主表
  const put = async (map) => q(`insert into ops.master_ownership_state (id, active_hash, active_map, activated_by) values (1, $1, $2::jsonb, 'test')
    on conflict (id) do update set active_hash = excluded.active_hash, active_map = excluded.active_map, prepared_hash = null, prepared_map = null, prepared_at = null, prepared_by = null`,
  [OS.ownershipHashOf(map), JSON.stringify(map)]);
  // (a) 式は 1 つ: JS (lib/master-cutover.mjs・ownership-state.mjs・master-publish.js) = DB (ops.ownership_hash)。load の列は数えない
  const sqlHash = async (m) => (await q('select ops.ownership_hash($1::jsonb) as h', [JSON.stringify(m)]))[0].h;
  assert.equal(C.ownershipHash(own), C.ownershipHash(older));
  assert.deepEqual([OS.ownershipHashOf(own), MP.ownershipHash(own), await sqlHash(own), await sqlHash(older)], Array(4).fill(C.ownershipHash(own)));
  assert.notEqual(C.ownershipHash({ ...own, 'skus.reorder_months': 'company' }), C.ownershipHash(own));   // company の列は数える
  assert.equal(await sqlHash({ ...own, 'skus.reorder_months': 'company' }), C.ownershipHash({ ...own, 'skus.reorder_months': 'company' }));
  // (b) 前の列の組で切り替える (epoch の active・段階の owner_hash = その時の画面の持ち主表 = older)
  await put(older);
  await setCutoverPhase('frozen');
  await pdb.query('begin');
  try { await q("select set_config('ops.cutover_protocol', '1', true)"); await q("update ops.master_cutover_state set phase = 'company_owner', owner_hash = $1 where id = 1", [C.ownershipHash(older)]); await pdb.query('commit'); }
  catch (e) { await pdb.query('rollback'); throw e; }
  await setCutoverPhase('new_open', C.ownershipHash(older));
  // (c) 列を足した後の画面 (持ち主表 = own。足した列は load): 段階の記録と同じ = 保存できる (JS の門・DB の ⑤-1 の begin・0055 の約束の門・登録の行)
  const cut = await C.readCutoverPhase(pdb);
  assert.equal(C.newEntryWritable(cut, own), true);
  assert.equal(C.newEntryWritable(cut, { ...own, 'skus.reorder_months': 'company' }), false);   // 足した列を company にした = 違う
  const skuNe = Number(await skuId('s-ne'));
  const begin = async (ownership) => {
    await pdb.query('begin');
    try {
      const v = (await q('select ops.master_edit_versions($1) as v', [skuNe]))[0].v;
      return (await q('select ops.begin_master_write($1::uuid, $2, null, $3::jsonb, $4, $5, $6, $7, $8::jsonb) as r',
        [crypto.randomUUID(), 'tester', JSON.stringify(ownership), 'sku_edit', skuNe, 'a'.repeat(64), 'b'.repeat(64), JSON.stringify(v)]))[0].r;
    } finally { await pdb.query('rollback'); }
  };
  assert.equal((await begin(own)).phase, 'new_open');
  assert.equal((await begin(older)).phase, 'new_open');
  await assert.rejects(begin({ ...own, 'skus.reorder_months': 'company' }), /before_cutover: 持ち主表が切替のときの記録と違う/);
  assert.equal((await q('select ops.master_ownership_matches_active($1::jsonb) as ok', [JSON.stringify(own)]))[0].ok, true);   // 登録 (sku_create) の約束の門も同じ式
  // (d) 夜間ロード・写し: 記録に無い列 = load (止まらない)
  const st = await OS.readOwnershipState(pdb);
  assert.deepEqual([st.active.map['skus.reorder_months'], st.active.filled, st.active.hash], ['load', ['skus.reorder_months'], C.ownershipHash(own)]);
  const lr = await loadNow();
  assert.deepEqual([lr.ownership_epoch.epoch, lr.company_owned], ['active', ['sku_costs']]);
  // (e) 写し: 夜間ロードが前の式で記録したハッシュ (式を変えた日) でも、記録した持ち主表から今の式で比べる = 偽の食い違いにしない
  const oldFormat = 'f'.repeat(64);
  const allMap = OS.sortedOwnership(OS.ALL_LOAD);
  const v1 = F.verifyGeneration({ prev: null, next: { ownership: allMap, ownership_hash: C.ownershipHash(allMap), cols: [], rows: [], problems: [], version_watermark: 1 },
    source: { load: { ingest_run_id: 'old_load' }, loadOwnership: { products: { ownership: allMap, ownership_hash: oldFormat }, set_components: { ownership: allMap, ownership_hash: oldFormat } }, skus: [] } });
  assert.ok(!v1.problems.includes('ownership_mismatch'), JSON.stringify(v1.problems));
  const v2 = F.verifyGeneration({ prev: null, next: { ownership: own, ownership_hash: C.ownershipHash(own), cols: [], rows: [], problems: [], version_watermark: 1 },
    source: { load: { ingest_run_id: 'old_load' }, loadOwnership: { products: { ownership: allMap, ownership_hash: C.ownershipHash(own) }, set_components: { ownership: allMap, ownership_hash: C.ownershipHash(own) } }, skus: [] } });
  assert.ok(v2.problems.includes('ownership_mismatch'), JSON.stringify(v2.problems));   // 記録した持ち主表が違う = 食い違い (ハッシュの列だけでは決めない)
  const ep = F.generationEpoch({ ownershipState: { state: 'ok', active: { map: allMap, hash: C.ownershipHash(allMap) }, prepared: { map: own, hash: C.ownershipHash(own) } },
    loadOwnership: { products: { ownership: own, ownership_hash: oldFormat } } });
  assert.equal(ep.kind, 'prepared');
  await setCutoverPhase('legacy_open');
  await q('delete from ops.master_ownership_state');
  await nightly();
  // (f) 段階が company_owner / new_open (前の式で owner_hash を記録した後) の DB では 0055 は止まる (式を黙って変えない)
  const pg2 = new PGlite(); const pdb2 = pgliteAdapter(pg2);
  try {
    await applyMigrations(pdb2, { log: quiet, to: '0052' });
    await pdb2.query('begin');
    await pdb2.query('set local session_replication_role = replica');
    await pdb2.query("update ops.master_cutover_state set phase = 'company_owner', owner_hash = $1 where id = 1", ['e'.repeat(64)]);
    await pdb2.query('commit');
    await assert.rejects(applyMigrations(pdb2, { log: quiet }), /0055: 切替の段階が company_owner \/ new_open/);
    assert.deepEqual((await pdb2.query("select version from ops.schema_migrations where version = '0055'")).rows, []);
  } finally { await pg2.close(); }
});

await ta('[29] 最後に commit したロード = DB が振る番号の順 (送り手の時計・場所では決めない)・番号の表は足すだけ・写しは場所を問わず最後のロード (#1564 Codex R4 Medium 2・High)', async () => {
  const a = await loadNow();
  const b = await loadNow();
  assert.equal(b.load_commit_seq, String(BigInt(a.load_commit_seq) + 1n));   // 番号は文字 (bigint を Number にしない・広げる道 PR-1)
  // b の送り手の時計が遅れていた (時刻は a より前) = 時刻では a が後に見えるが、commit は b が後 = 番号で b
  await q("update ops.ingest_runs set started_at = started_at - interval '1 day', finished_at = finished_at - interval '1 day' where ingest_run_id = $1", [b.run_id]);
  const last = await OS.latestLoadCommit(pdb);
  assert.deepEqual([last.ingest_run_id, last.commit_seq, last.epoch], [b.run_id, b.load_commit_seq, b.ownership_epoch.epoch]);
  // 番号は数で並べる (文字で並べると '9' が '10' より後になる。この試験では番号が 2 桁を越えている)
  assert.ok(BigInt(b.load_commit_seq) >= 10n, String(b.load_commit_seq));
  assert.equal(last.commit_seq, (await q('select max(commit_seq)::text as m from ops.master_load_commits'))[0].m);
  assert.equal((await F.selectPublishLoad(pdb)).ingest_run_id, b.run_id);
  // 場所: 毎晩の cron (render-nightly) より後に別の場所で commit したロード = 写しはそれを使う (照合 ① の毎晩の回は cron のまま)
  const other = await runInitialLoad(pdb, buildPlanFromRender({ dataDir: mirrorDir, log: quiet }), { log: quiet, runId: `load_pub_${++loadN}`, host: 'render' });
  assert.equal(other.ok, true, other.error);
  assert.deepEqual([(await F.selectPublishLoad(pdb)).ingest_run_id, (await F.selectPublishLoad(pdb)).commit_seq], [other.run_id, String(BigInt(b.load_commit_seq) + 1n)]);
  // dry-run は番号を取らない (巻き戻す)
  const dry = await runInitialLoad(pdb, buildPlanFromRender({ dataDir: mirrorDir, log: quiet }), { log: quiet, runId: `load_pub_${++loadN}`, host: 'render-nightly', dryRun: true });
  assert.equal(dry.load_commit_seq, undefined);
  assert.equal((await OS.latestLoadCommit(pdb)).ingest_run_id, other.run_id);
  // 0055 の前の毎晩の cron のロード (番号の行が無い) = 0055 の後の最初の朝の写しはそのロードを使う (commit_seq = null。#1564 Codex R5 Low)
  const pg3 = new PGlite(); const pdb3 = pgliteAdapter(pg3);
  try {
    await applyMigrations(pdb3, { log: quiet, to: '0052' });
    const old = await runInitialLoad(pdb3, buildPlanFromRender({ dataDir: mirrorDir, log: quiet }), { log: quiet, runId: 'load_before_0055', host: 'render-nightly' });
    assert.equal(old.ok, true, old.error);
    assert.equal(old.load_commit_seq, undefined);   // 番号の表が無い = 番号を取らない (ロードは止めない)
    await applyMigrations(pdb3, { log: quiet });
    assert.equal((await pdb3.query('select count(*)::int as n from ops.master_load_commits')).rows[0].n, 0);
    const first = await F.selectPublishLoad(pdb3);
    assert.deepEqual([first.ingest_run_id, first.commit_seq, first.host], ['load_before_0055', null, 'render-nightly']);
    // 0055 の後の最初の本適用のロード = 番号 1 = それを使う
    const next = await runInitialLoad(pdb3, buildPlanFromRender({ dataDir: mirrorDir, log: quiet }), { log: quiet, runId: 'load_after_0055', host: 'render-nightly' });
    assert.equal(next.load_commit_seq, '1');
    assert.deepEqual([(await F.selectPublishLoad(pdb3)).ingest_run_id, (await F.selectPublishLoad(pdb3)).commit_seq], ['load_after_0055', '1']);
  } finally { await pg3.close(); }
  // 足すだけ
  await assert.rejects(q("update ops.master_load_commits set host = 'x'"), /足すだけ/);   // 番号そのものは identity (always) = 書き換えられない
  await assert.rejects(q('delete from ops.master_load_commits'), /足すだけ/);
  await nightly();
});

await ta('[30] セットの導いた値 (原価・税率・税区分・売上分類・取扱区分) と構成品の行 (コード・数量・C の名前・C の原価) も入れた後の確かめで比べる = 作り直しの後に書き換えられた = broken (exit 4)・ハッシュも変わる / 印を消せない確かめは通らない (#1564 Codex R7 High・Medium 1)', async () => {
  const G = await import('../apps/warehouse/publish-gate.js');
  const own = OWN('sku_costs', 'skus.tax_rate', 'skus.tax_class', 'products.sales_class', 'skus.handling', 'products.status', 'skus.name', 'products.name');
  const deps = (extra = {}) => ({ now: new Date(), log: quiet, ping: async () => {}, openSqlite: async () => db,
    env: { DATA_DIR: tmp, COMPANY_DB_WATCH_URL: 'postgres://test', DAILY_SYNC_RUN_ID: 'ds_test_publish' }, connectFor: () => async () => ({ db: pdb, close: async () => {} }), ...extra });
  const verify = (extra) => quietly(() => F.cli(['--verify-apply', '--daily'], deps(extra)));
  db.prepare('DELETE FROM cdb_publish_gate').run();
  await nightly(own);
  assert.equal((await fetchGen(own)).state, 'verified');
  const r = await rebuild();
  assert.equal(r.ok, true, JSON.stringify(r.checks));
  // 作り直しが C にあるセットの導き方の入力・構成品の行を残した
  const ex = db.prepare("SELECT args_json, components_json FROM m_set_publish_expect WHERE set_code = 'set-a'").get();
  assert.ok(ex, 'set-a の導き方の入力が無い');
  assert.deepEqual(JSON.parse(ex.components_json).map((x) => [x.c, x.from_cdb]).sort(), [['s-exc', true], ['s-ne', true]]);
  let v = await verify();
  assert.equal(v.code, 0, v.last);
  const base = snap().build.cdb_publish_applied_hash;
  const row0 = { mp: { ...mp('set-a') }, comps: db.prepare("SELECT * FROM m_set_components WHERE セット商品コード = 'set-a' ORDER BY 構成商品コード").all() };
  const restore = () => {
    db.prepare('UPDATE m_products SET 原価 = ?, 原価ソース = ?, 原価状態 = ?, 消費税率 = ?, 税区分 = ?, 売上分類 = ?, 取扱区分 = ? WHERE 商品コード = ?')
      .run(row0.mp.原価, row0.mp.原価ソース, row0.mp.原価状態, row0.mp.消費税率, row0.mp.税区分, row0.mp.売上分類, row0.mp.取扱区分, 'set-a');
    for (const c of row0.comps) db.prepare('UPDATE m_set_components SET 数量 = ?, 構成商品名 = ?, 構成商品原価 = ? WHERE セット商品コード = ? AND 構成商品コード = ?').run(c.数量, c.構成商品名, c.構成商品原価, 'set-a', c.構成商品コード);
  };
  const cases = [
    ['原価', "UPDATE m_products SET 原価 = 999 WHERE 商品コード = 'set-a'", /set_derived:cost/],
    ['原価状態', "UPDATE m_products SET 原価状態 = 'PARTIAL' WHERE 商品コード = 'set-a'", /set_derived:cost/],
    ['税率・税区分', "UPDATE m_products SET 消費税率 = 0.08, 税区分 = 'REDUCED_8' WHERE 商品コード = 'set-a'", /set_derived:tax/],
    ['売上分類', "UPDATE m_products SET 売上分類 = 3 WHERE 商品コード = 'set-a'", /set_derived:sales_class/],
    ['取扱区分', "UPDATE m_products SET 取扱区分 = '取扱中止' WHERE 商品コード = 'set-a'", /set_derived:handling/],
    ['構成品の名前 (C)', "UPDATE m_set_components SET 構成商品名 = 'WRONG NAME' WHERE セット商品コード = 'set-a' AND 構成商品コード = 's-ne'", /set_components/],
    ['構成品の原価 (C)', "UPDATE m_set_components SET 構成商品原価 = 999 WHERE セット商品コード = 'set-a' AND 構成商品コード = 's-ne'", /set_components/],
    ['構成品の数量', "UPDATE m_set_components SET 数量 = 5 WHERE セット商品コード = 'set-a' AND 構成商品コード = 's-ne'", /set_components/],
  ];
  for (const [label, sql, re] of cases) {
    db.prepare(sql).run();
    const a = MP.verifyApplied(db, { publication: MP.readCurrentPublish(db), ownership: OS.sortedOwnership(own), taxRates: TAX_RATES });
    assert.equal(a.ok, false, label);
    assert.ok(a.problems.some((p) => re.test(p.col)), `${label}: ${JSON.stringify(a.problems)}`);
    assert.notEqual(a.applied_hash, base, label);   // ハッシュも変わる
    v = await verify();
    assert.equal(v.code, 4, `${label}: ${v.last}`);
    assert.deepEqual([G.readGateRow(db).state, G.readPublishGate({ db }).open], ['broken', false], label);
    restore();
    v = await verify();
    assert.equal(v.code, 0, `${label} (戻した): ${v.last}`);
    assert.equal(G.readGateRow(db).state, 'safe', label);
  }
  // 導き方の入力が無い (別の作り直しの名残・消えた) = 確かめられない = broken
  const exAll = db.prepare('SELECT * FROM m_set_publish_expect').all();
  db.prepare("DELETE FROM m_set_publish_expect WHERE set_code = 'set-a'").run();
  v = await verify();
  assert.equal(v.code, 4, v.last);
  assert.match(v.last, /applied_mismatch/);
  const insEx = db.prepare('INSERT OR REPLACE INTO m_set_publish_expect (set_code, args_json, components_json) VALUES (?, ?, ?)');
  for (const x of exAll) insEx.run(x.set_code, x.args_json, x.components_json);
  assert.equal((await verify()).code, 0);
  // 印を消せない確かめ = 通っていない (exit 1・safe の行にしない。#1564 Codex R7 Medium 1)
  const stopFile = path.join(tmp, G.GATE_STOP_MARK);
  G.writeStopMark({ dataDir: tmp }, { state: 'unknown', reason: '試験', now: new Date() });
  const va = await F.runVerifyApply({ sqlite: db, dataDir: tmp, taxRates: TAX_RATES, clearMark: () => ({ ok: false, removed: false, error: '消せない (試験)' }) });
  assert.equal(va.state, 'failed', JSON.stringify(va.problems));
  assert.ok(va.problems.includes('stop_mark_clear_failed'));
  assert.deepEqual([G.readGateRow(db).state, fs.existsSync(stopFile), G.readPublishGate({ db }).open], ['unknown', true, false]);
  const okv = await verify();   // 消せるようになった = 通る・印も消える
  assert.equal(okv.code, 0, okv.last);
  assert.deepEqual([G.readGateRow(db).state, fs.existsSync(stopFile)], ['safe', false]);
  db.prepare('DELETE FROM cdb_publish_gate').run();
  await nightly();
});

await ta('[31] JAN (external_ids.jan・⑤-2b) だけ company の prepare は通る (古い表に置き場所が無い = 写さない列)・Amazon の構成 (listing_components.amazon) は ⑦-2 まで断る (#1564 Codex R7 Medium 2)', async () => {
  assert.deepEqual(MP.checkPublishOwnership(OWN('external_ids.jan')), []);
  assert.deepEqual(MP.publishCols(OWN('external_ids.jan')), []);   // 写す列は無い
  assert.deepEqual(MP.checkPublishOwnership(OWN('listing_components.amazon')), ['not_copied:listing_components.amazon']);
  const logs = [];
  const epochCli = (argv, extra = {}) => quietly(() => EP.cli(argv, { env: {}, connect: async () => ({ db: pdb, close: async () => {} }), log: (m) => logs.push(m), ...extra }));
  await q('delete from ops.master_ownership_state');
  assert.equal(await epochCli(['prepare'], { ownership: OWN('external_ids.jan') }), 0, logs.at(-1));
  assert.equal((await OS.readOwnershipState(pdb)).prepared.map['external_ids.jan'], 'company');
  assert.equal(await epochCli(['prepare'], { ownership: OWN('listing_components.amazon') }), 1);
  assert.match(logs.at(-1), /not_copied:listing_components\.amazon/);
  assert.equal(await epochCli(['cancel']), 0);
  await q('delete from ops.master_ownership_state');
});

await ta('[32] 区分 (skus.sku_kind) の持ち主が C: prepare → 明示のロード → 写し → 作り直し → 確かめ → activate が通る / C = 単品・NE = セット = C の区分・C の値 (構成の行を作らない) / C = セット・NE = 単品 = その SKU は前の行のまま (fail-closed・前の行が無い = 載せない・NE を直すと写る) / 夜間ロードは区分を NE に戻さない / 商品区分を読む業務 (f_sales のセット展開・商品管理リスト) が壊れない / load に戻すと NE の区分', async () => {
  const own = OWN('skus.sku_kind', 'skus.name', 'products.name');
  assert.deepEqual(MP.checkPublishOwnership(own), []);   // 区分は写す列 = prepare できる
  assert.deepEqual(MP.publishCols(own), ['name', 'kind']);
  assert.deepEqual(MP.publishCols(MASTER_OWNERSHIP), []);   // load の今は写さない
  const logs = [];
  const epochCli = (argv, extra = {}) => quietly(() => EP.cli(argv, { env: { DATA_DIR: tmp }, connect: async () => ({ db: pdb, close: async () => {} }), openSqlite: async () => db,
    log: (m) => logs.push(m), ...extra }));
  // 準備: 全部 load で作り直し → mirror → 夜間ロード (C も NE と同じ区分)
  await setActive(MASTER_OWNERSHIP);
  addNe('s-kind1', 'NE の単品 K', 40);
  assert.equal((await fetchGen(MASTER_OWNERSHIP)).state, 'verified');
  assert.equal((await rebuild()).ok, true);
  publishMirror();
  await loadNow();
  // C で区分を変える (ポータルの区分の編集の代わり): s-kind1 を単品 → セット (前の区分の product_id は残る = Codex の指摘の形)・
  //   set-g をセット → 単品 (商品を作って付け、構成は外す)
  await q("update core.skus set sku_kind = 'set', name = 'C のセット K' where code = 's-kind1'");
  const pid = (await q("insert into core.products (company_id, display_code, name, status, created_by_type, created_by_id) values (1, 'set-g', 'C の単品 G', 'active', 'human', 'test') returning product_id"))[0].product_id;
  await q("delete from core.sku_components where parent_sku_id = (select sku_id from core.skus where code = 'set-g')");
  await q("update core.skus set sku_kind = 'single', product_id = $1, name = 'C の単品 G', handling = 'active' where code = 'set-g'", [pid]);
  const kindOf = async (code) => (await q('select sku_kind, product_id::text as product_id from core.skus where code = $1', [code]))[0];
  const before = { k1: await kindOf('s-kind1'), g: await kindOf('set-g') };
  assert.ok(before.k1.product_id != null);   // セットに前の区分の product_id が残った形
  // 切替の日の順: prepare → 明示のロード (prepared の持ち主) = 区分は社内のまま・食い違いは判断の記録に
  assert.equal(await epochCli(['prepare'], { ownership: own }), 0, logs.at(-1));
  const hl = await loadViaHttp({ usePrepared: true });
  // 区分は社内のまま・セットの名残の product_id は外す (正規化)・単品 set-g の商品はそのまま
  assert.deepEqual([await kindOf('s-kind1'), await kindOf('set-g')], [{ sku_kind: 'set', product_id: null }, before.g]);
  assert.equal((await q("select count(*)::int as n from core.sku_components where parent_sku_id = (select sku_id from core.skus where code = 'set-g')"))[0].n, 0);   // NE のセットの構成を C の単品に入れない
  const dk = (await q("select payload from ops.load_decisions where ingest_run_id = $1 and section = 'skus'", [hl.run_id]))[0].payload.kind_held;
  assert.deepEqual([...dk].sort(), [['s-kind1', 'single', 'set'], ['set-g', 'set', 'single']]);
  // C = セット・NE = 単品 で前の m_products の行が無い SKU (C だけでセットとして作った・NE は単品で新しく登録) も作る
  await q("insert into core.skus (company_id, sku_kind, code, name, handling) values (1, 'set', 's-kind2', 'C のセット 2', 'active')");
  addNe('s-kind2', 'NE の単品 2', 30);
  const prevK1 = db.prepare('SELECT 商品名, 商品区分, 原価, 標準売価 FROM m_products WHERE 商品コード = ?').get('s-kind1');
  // 写し = prepared の世代 (区分の列あり)。区分の違う SKU は止めない・知らせる
  const g = await fetchGen(MASTER_OWNERSHIP);
  assert.deepEqual([g.state, g.evidence.company_owned, g.evidence.epochs.generation.kind, g.evidence.kind_c_single_ne_set, g.evidence.kind_c_set_ne_single_frozen, g.evidence.kind_mismatch],
    ['verified', ['name', 'kind'], 'prepared', { count: 1, codes: ['set-g'] }, { count: 1, codes: ['s-kind1'] }, { count: 0, codes: [] }], JSON.stringify(g.problems));
  assert.match(g.line, /^⚠️ .*社内は単品・NE はセットの SKU 1 件は単品として写す .*set-g.*社内と NE で区分が違う SKU 1 件は前の行のまま .*s-kind1/);
  // 作り直し: C = 単品・NE = セット (set-g) = 単品の行・C の値・構成の行を作らない / C = セット・NE = 単品 (s-kind1) = 前の行のまま (fail-closed) /
  //   前の行が無い (s-kind2) = 載せない
  const r = await rebuild();
  assert.equal(r.ok, true, JSON.stringify(r.checks));
  assert.deepEqual([r.publish.stats.kind_c_single_ne_set_codes, r.publish.stats.kind_c_set_ne_single_frozen_codes, r.publish.stats.kind_mismatch], [['set-g'], ['s-kind1', 's-kind2'], 0]);
  assert.ok(r.warn.some((w) => /社内は単品・NE はセットの SKU 1 件は単品として写した.*set-g/.test(w)), JSON.stringify(r.warn));
  assert.ok(r.warn.some((w) => /区分が違う SKU 2 件は前の行のまま.*前の行が無い 1 件は載せない \(s-kind2\).*s-kind1, s-kind2/.test(w)), JSON.stringify(r.warn));
  assert.deepEqual([mp('set-g').商品区分, mp('set-g').商品名, mp('set-g').セット構成品数], ['単品', 'C の単品 G', null]);
  assert.equal(count('m_set_components', "セット商品コード = 'set-g'"), 0);
  assert.deepEqual(db.prepare('SELECT 商品名, 商品区分, 原価, 標準売価 FROM m_products WHERE 商品コード = ?').get('s-kind1'), prevK1);   // 前の行のまま (C の名前も入れない)
  assert.equal(mp('s-kind2'), undefined);   // 前の行が無い = 載せない (NE の値の行も残さない)
  assert.deepEqual(db.prepare('SELECT code, prev_row FROM m_publish_kind_frozen ORDER BY code').all(), [{ code: 's-kind1', prev_row: 1 }, { code: 's-kind2', prev_row: 0 }]);
  const kr0 = snap().reasons.filter((x) => x.col === 'kind').map((x) => [x.code, x.reason, x.value]).sort();
  assert.deepEqual(kr0, [['s-kind1', 'kind_c_set_ne_single_frozen', 'previous_row'], ['s-kind2', 'kind_c_set_ne_single_frozen', 'omitted'], ['set-g', 'company_owned', '単品']]);
  // 入れた後の確かめも通る (前の行のまま の SKU は比べない)
  const va0 = await F.runVerifyApply({ sqlite: db, dataDir: tmp, taxRates: TAX_RATES });
  assert.deepEqual([va0.state, va0.evidence.apply.counts.kind_frozen], ['verified', 1], JSON.stringify(va0.problems));
  // NE を直す (NE の画面で s-kind1 をセットに = セットの表に行)・s-kind2 は NE から消す = 翌朝の写し・作り直しで C の区分・C の値
  db.prepare("INSERT OR REPLACE INTO raw_ne_set_products (セット商品コード, セット商品名, セット販売価格, 商品コード, 数量, synced_at) VALUES ('s-kind1', 'NE のセット K', 300, 's-ne', 2, ?)").run(FX.T1);
  db.prepare("DELETE FROM raw_ne_products WHERE 商品コード = 's-kind2'").run();
  FX.markComplete(db, readNeRawRev);
  await q("delete from core.skus where code = 's-kind2'");
  const g2 = await fetchGen(MASTER_OWNERSHIP);
  assert.deepEqual([g2.state, g2.evidence.kind_c_single_ne_set, g2.evidence.kind_c_set_ne_single_frozen], ['verified', { count: 1, codes: ['set-g'] }, { count: 0, codes: [] }], JSON.stringify(g2.problems));
  const r2 = await rebuild();
  assert.equal(r2.ok, true, JSON.stringify(r2.checks));
  assert.deepEqual([r2.publish.stats.kind_c_single_ne_set, r2.publish.stats.kind_c_set_ne_single_frozen], [1, 0]);
  assert.deepEqual([mp('s-kind1').商品区分, mp('s-kind1').商品名, count('m_set_components', "セット商品コード = 's-kind1'")], ['セット', 'C のセット K', 1]);   // 区分が同じセット = 今までどおり (構成の行・導いた値)
  assert.equal(count('m_publish_kind_frozen'), 0);
  const kr = snap().reasons.filter((x) => x.col === 'kind');
  assert.deepEqual(kr.map((x) => [x.code, x.reason, x.owner_key, x.cdb_value, x.ne_value, x.value]), [['set-g', 'company_owned', 'skus.sku_kind', { kind: 'single' }, 'セット', '単品']]);
  // 入れた後の確かめ (次の工程) も通る・activate できる
  const va = await F.runVerifyApply({ sqlite: db, dataDir: tmp, taxRates: TAX_RATES });
  assert.deepEqual([va.state, va.evidence.apply.epoch], ['verified', 'prepared'], JSON.stringify(va.problems));
  // わざと壊す: 作り直しの後に区分・構成の行が書き換えられた = 確かめで見つかる (戻す)
  db.prepare("UPDATE m_products SET 商品区分 = 'セット' WHERE 商品コード = 'set-g'").run();
  const bad = MP.verifyApplied(db, { publication: MP.readCurrentPublish(db), ownership: own, taxRates: TAX_RATES });
  assert.ok(bad.problems.some((p) => p.code === 'set-g' && p.col === 'kind'), JSON.stringify(bad.problems));
  db.prepare("UPDATE m_products SET 商品区分 = '単品' WHERE 商品コード = 'set-g'").run();
  db.prepare("INSERT INTO m_set_components (セット商品コード, 構成商品コード, 数量, 構成商品名, 構成商品原価, updated_at) VALUES ('set-g', 's-sc3', 2, 'x', 45, 'x')").run();
  const bad2 = MP.verifyApplied(db, { publication: MP.readCurrentPublish(db), ownership: own, taxRates: TAX_RATES });
  assert.ok(bad2.problems.some((p) => p.code === 'set-g' && p.col === 'kind_components'), JSON.stringify(bad2.problems));
  db.prepare("DELETE FROM m_set_components WHERE セット商品コード = 'set-g'").run();
  await setCutoverPhase('frozen');
  try { assert.equal(await epochCli(['activate']), 0, logs.at(-1)); } finally { await setCutoverPhase('legacy_open'); }
  // 次の夜 (active = 区分も C): mirror は C の区分の m_products = ロードの材料も C の区分 = 食い違いの記録は空・区分は社内のまま
  publishMirror();
  const lr = await loadNow();
  assert.deepEqual([lr.company_owned.includes('skus.sku_kind'), (await kindOf('s-kind1')).sku_kind, (await kindOf('set-g')).sku_kind], [true, 'set', 'single']);
  assert.ok(!lr.conflicts.some((c) => c.kind === 'sku_kind_held'), JSON.stringify(lr.conflicts.filter((c) => c.kind === 'sku_kind_held')));
  // 商品区分を読む業務: f_sales (Amazon の注文のセット展開は 商品区分 = セット かつ 構成の行があるときだけ) = C の単品は展開しない (直接の販売)・ほかのセットは今までどおり展開
  db.exec('CREATE TABLE IF NOT EXISTS raw_rakuten_orders (order_number TEXT, order_date TEXT, order_status INTEGER, item_number TEXT, item_name TEXT, price_tax_incl REAL, units INTEGER, delete_item_flag INTEGER)');
  const { rebuildFSales } = await import('../apps/warehouse/rebuild-f-sales.js');
  const insOrder = db.prepare("INSERT INTO raw_sp_orders (amazon_order_id, purchase_date, order_status, seller_sku, title, quantity, item_price, synced_at) VALUES (?, '2026-10-01T10:00:00Z', 'Shipped', ?, 't', ?, 1000, 'x')");
  insOrder.run('k-order-1', 'set-g', 3); insOrder.run('k-order-2', 's-kind1', 2); insOrder.run('k-order-3', 'set-a', 1);
  await quietly(() => rebuildFSales());
  const fs1 = (code) => db.prepare("SELECT SUM(数量) AS q, SUM(直接販売数) AS d, SUM(セット経由数) AS s FROM f_sales_by_product WHERE 日付 = '2026-10-01' AND 商品コード = ?").get(code);
  assert.deepEqual([fs1('set-g'), fs1('s-sc3').q], [{ q: 3, d: 3, s: 0 }, null]);   // C の単品 = そのまま (NE の構成品 s-sc3 に展開しない)
  assert.equal(fs1('s-ne').s, 6);   // set-a (s-ne × 2) と s-kind1 (s-ne × 2 × 2) は展開
  db.prepare("DELETE FROM raw_sp_orders WHERE amazon_order_id LIKE 'k-order-%'").run();
  await quietly(() => rebuildFSales());
  // 商品管理リスト (登録日に 商品区分 = セット を使う) も作れる・区分は C
  const { buildProductManagementSnapshot } = await import('../apps/warehouse/build-product-management-snapshot.js');
  const pml = await quietly(() => buildProductManagementSnapshot());
  const pr = (code) => db.prepare('SELECT 商品区分 FROM product_management_snapshot_rows WHERE run_id = ? AND 商品コード = ?').get(pml.run_id, code);
  assert.deepEqual([pr('set-g').商品区分, pr('s-kind1').商品区分], ['単品', 'セット']);
  // warehouse の画面の API (商品区分で絞る「未登録」の一覧) も C の区分で動く: 原価の未登録は単品だけ = C の単品 set-g も対象・セット s-kind1 は対象外
  const wr = await import('../apps/warehouse/router.js'); const router = wr.default || wr.router;
  const callGet = (routePath, query) => new Promise((resolve, reject) => {
    const layer = router.stack.find((l) => l.route && l.route.path === routePath && l.route.methods.get);
    const res = { statusCode: 200, status(c) { this.statusCode = c; return this; }, json(b) { resolve({ status: this.statusCode, body: b }); }, setHeader() {}, set() { return this; } };
    try { const x = layer.route.stack[0].handle({ query, body: {}, params: {}, headers: {}, session: {} }, res, reject); if (x && typeof x.catch === 'function') x.catch(reject); } catch (e) { reject(e); }
  });
  db.prepare("UPDATE m_products SET 原価 = NULL, 原価状態 = 'MISSING' WHERE 商品コード IN ('set-g', 's-kind1')").run();
  const miss = await callGet('/api/missing/prioritized', { type: 'genka' });
  assert.equal(miss.status, 200);
  const missCodes = miss.body.rows.map((x) => x.商品コード);
  assert.deepEqual([missCodes.includes('set-g'), missCodes.includes('s-kind1')], [true, false]);
  assert.equal(miss.body.rows.find((x) => x.商品コード === 'set-g').商品区分, '単品');
  // 持ち主を load に戻す = 写しも作り直しも NE の区分・夜間ロードも NE の区分に合わせる (今までどおり)
  await nightly();   // 持ち主を load に (夜間ロードの記録した持ち主 = 写しの持ち主)
  assert.equal((await fetchGen(MASTER_OWNERSHIP)).state, 'verified');
  assert.equal((await rebuild()).ok, true);
  assert.deepEqual([mp('set-g').商品区分, count('m_set_components', "セット商品コード = 'set-g'")], ['セット', 1]);
  publishMirror();
  await loadNow();
  assert.deepEqual([(await kindOf('s-kind1')).sku_kind, (await kindOf('set-g')).sku_kind], ['set', 'set']);
  db.prepare("DELETE FROM raw_ne_set_products WHERE セット商品コード = 's-kind1'").run();
  db.prepare("DELETE FROM raw_ne_products WHERE 商品コード = 's-kind1'").run();
  FX.markComplete(db, readNeRawRev);
  assert.equal((await rebuild()).ok, true);
});

await ta('[33] 区分の持ち主が C の 9 升 (C の区分 3 × NE の区分 3): 同じ = 写す / C 単品・NE セット = 単品として写す (構成なし) / ほか 5 升 (C セット・NE 単品・例外を含む 4 升) = 前の行のまま (構成も)・前の行が無い = 載せない / 確かめ・下流 3 系統 (f_sales・商品管理リスト・router)', async () => {
  const own = OWN('skus.sku_kind', 'skus.name', 'products.name');
  // NE の区分 (単品 = 商品の表 / セット = セットの表 / 例外 = exception_genka だけ) を 9 升 + 前の行の無い 1 件に
  const CELLS = [   // [コード, C の区分, NE の区分]
    ['q-ss', 'single', 'single'], ['q-st', 'single', 'set'], ['q-sx', 'single', 'exception'],
    ['q-ts', 'set', 'single'], ['q-tt', 'set', 'set'], ['q-tx', 'set', 'exception'],
    ['q-xs', 'exception', 'single'], ['q-xt', 'exception', 'set'], ['q-xx', 'exception', 'exception'],
  ];
  const putNe = (code, k) => {
    db.prepare('DELETE FROM raw_ne_products WHERE 商品コード = ?').run(code);
    db.prepare('DELETE FROM raw_ne_set_products WHERE セット商品コード = ?').run(code);
    db.prepare('DELETE FROM exception_genka WHERE sku = ?').run(code);
    if (k === 'single') addNe(code, `NE ${code}`, 50);
    if (k === 'set') db.prepare("INSERT INTO raw_ne_set_products (セット商品コード, セット商品名, セット販売価格, 商品コード, 数量, synced_at) VALUES (?, ?, 500, 's-ne', 3, ?)").run(code, `NE ${code}`, FX.T1);
    if (k === 'exception') db.prepare("INSERT INTO exception_genka (sku, genka, 商品名, synced_at) VALUES (?, 77, ?, 'x')").run(code, `NE ${code}`);
    FX.markComplete(db, readNeRawRev);
  };
  // 1 日目 (全部 load): NE を C の区分にして作り直し・夜間ロード = C に C の区分の SKU ができる (前の m_products の行も C の区分)
  for (const [code, ck] of CELLS) putNe(code, ck);
  await nightly();
  assert.equal((await fetchGen(MASTER_OWNERSHIP)).state, 'verified');
  assert.equal((await rebuild()).ok, true);
  publishMirror();
  await nightly();
  const kinds = async () => Object.fromEntries((await q("select code, sku_kind from core.skus where code like 'q-%' order by code")).map((r) => [r.code, r.sku_kind]));
  assert.deepEqual(await kinds(), Object.fromEntries(CELLS.map(([c, ck]) => [c, ck])));
  for (const [code] of CELLS) await setName(code, `C ${code}`, { productToo: CELLS.find((x) => x[0] === code)[1] === 'single' });
  // 前の行が無い C のセット (NE は今朝から単品)
  await q("insert into core.skus (company_id, sku_kind, code, name, handling) values (1, 'set', 'q-new', 'C q-new', 'active')");
  const prev = Object.fromEntries(CELLS.map(([c]) => [c, db.prepare('SELECT 商品名, 商品区分, 原価, 原価ソース, 消費税率, 取扱区分, セット構成品数, updated_at FROM m_products WHERE 商品コード = ?').get(c)]));
  const prevComps = (c) => db.prepare('SELECT 構成商品コード, 数量 FROM m_set_components WHERE セット商品コード = ? ORDER BY 構成商品コード').all(c);
  const prevC = Object.fromEntries(CELLS.map(([c]) => [c, prevComps(c)]));
  // 2 日目: NE の区分を変える (NE の画面で)・持ち主 = 区分も C
  for (const [code, , nk] of CELLS) putNe(code, nk);
  putNe('q-new', 'single');
  await nightly(own);   // 材料 (前の m_products) は C の区分 = 区分は社内のまま
  assert.deepEqual(await kinds(), { ...Object.fromEntries(CELLS.map(([c, ck]) => [c, ck])), 'q-new': 'set' });
  const g = await fetchGen(MASTER_OWNERSHIP);
  assert.equal(g.state, 'verified', JSON.stringify(g.problems));
  assert.deepEqual([g.evidence.kind_c_single_ne_set.codes, g.evidence.kind_c_set_ne_single_frozen.codes], [['q-st'], ['q-sx', 'q-ts', 'q-tx', 'q-xs', 'q-xt']]);
  const r = await rebuild();
  assert.equal(r.ok, true, JSON.stringify(r.checks));
  assert.deepEqual([r.publish.stats.kind_c_single_ne_set_codes, r.publish.stats.kind_c_set_ne_single_frozen_codes, r.publish.stats.kind_mismatch],
    [['q-st'], ['q-new', 'q-sx', 'q-ts', 'q-tx', 'q-xs', 'q-xt'], 0]);
  const row = (c) => db.prepare('SELECT 商品名, 商品区分, 原価, 原価ソース, 消費税率, 取扱区分, セット構成品数, updated_at FROM m_products WHERE 商品コード = ?').get(c);
  // 同じ 3 升 = C の区分・C の名前
  assert.deepEqual([row('q-ss').商品区分, row('q-ss').商品名, row('q-tt').商品区分, row('q-tt').商品名, row('q-xx').商品区分, row('q-xx').商品名], ['単品', 'C q-ss', 'セット', 'C q-tt', '例外', 'C q-xx']);
  assert.deepEqual(prevComps('q-tt'), [{ 構成商品コード: 's-ne', 数量: 3 }]);
  // C 単品・NE セット = 単品・C の名前・構成なし
  assert.deepEqual([row('q-st').商品区分, row('q-st').商品名, row('q-st').セット構成品数, prevComps('q-st')], ['単品', 'C q-st', null, []]);
  // ほか 5 升 = 前の行のまま (時刻も)・前の構成のまま
  for (const c of ['q-sx', 'q-ts', 'q-tx', 'q-xs', 'q-xt']) { assert.deepEqual(row(c), prev[c], c); assert.deepEqual(prevComps(c), prevC[c], c); }
  assert.equal(row('q-new'), undefined);   // 前の行が無い = 載せない
  assert.deepEqual(db.prepare("SELECT code, prev_row FROM m_publish_kind_frozen ORDER BY code").all().map((x) => [x.code, x.prev_row]),
    [['q-new', 0], ['q-sx', 1], ['q-ts', 1], ['q-tx', 1], ['q-xs', 1], ['q-xt', 1]]);
  const reasons = snap().reasons.filter((x) => x.col === 'kind' && x.code.startsWith('q-')).map((x) => [x.code, x.reason, x.cdb_value?.kind, x.ne_value, x.value]).sort();
  assert.deepEqual(reasons, [['q-new', 'kind_c_set_ne_single_frozen', 'set', '単品', 'omitted'], ['q-st', 'company_owned', 'single', 'セット', '単品'],
    ['q-sx', 'kind_c_set_ne_single_frozen', 'single', '例外', 'previous_row'], ['q-ts', 'kind_c_set_ne_single_frozen', 'set', '単品', 'previous_row'],
    ['q-tx', 'kind_c_set_ne_single_frozen', 'set', '例外', 'previous_row'], ['q-xs', 'kind_c_set_ne_single_frozen', 'exception', '単品', 'previous_row'],
    ['q-xt', 'kind_c_set_ne_single_frozen', 'exception', 'セット', 'previous_row']]);
  // 入れた後の確かめ = 通る (前の行のまま の SKU は比べない・数える)・②b も前の行のままを差にしない
  const va = await F.runVerifyApply({ sqlite: db, dataDir: tmp, taxRates: TAX_RATES });
  assert.deepEqual([va.state, va.evidence.apply.counts.kind_frozen], ['verified', 5], JSON.stringify(va.problems));
  const v2 = MP.verifyApplied(db, { publication: MP.readCurrentPublish(db), ownership: OWN('skus.name', 'products.name'), taxRates: TAX_RATES, maxProblems: Infinity, kindCopied: true });
  assert.ok(!v2.problems.some((x) => String(x.code).startsWith('q-')), JSON.stringify(v2.problems.filter((x) => String(x.code).startsWith('q-'))));
  // わざと壊す: 前の行のまま の印が消えた = 前の行と C の名前が違う = 確かめで見つかる
  const keepFrozen = db.prepare('SELECT * FROM m_publish_kind_frozen').all();
  db.exec('DELETE FROM m_publish_kind_frozen');
  const v3 = MP.verifyApplied(db, { publication: MP.readCurrentPublish(db), ownership: own, taxRates: TAX_RATES, maxProblems: Infinity });
  assert.ok(v3.problems.some((x) => x.code === 'q-ts' || x.code === 'q-xs'), JSON.stringify(v3.problems.slice(0, 5)));
  for (const f of keepFrozen) db.prepare('INSERT INTO m_publish_kind_frozen (code, prev_row, snapshot) VALUES (?, ?, ?)').run(f.code, f.prev_row, f.snapshot);
  // わざと壊す (#1641 Codex R1 High): 印を残したまま、前の行のまま の商品の行 (名前・原価)・構成の行を書き換える / 載せない SKU の行を足す = 確かめで見つかり・ハッシュも変わる
  const vOk = MP.verifyApplied(db, { publication: MP.readCurrentPublish(db), ownership: own, taxRates: TAX_RATES, maxProblems: Infinity });
  assert.equal(vOk.ok, true, JSON.stringify(vOk.problems.slice(0, 3)));
  const tamper = [
    ["UPDATE m_products SET 商品名 = '書き換え' WHERE 商品コード = 'q-ts'", "UPDATE m_products SET 商品名 = ? WHERE 商品コード = 'q-ts'", prev['q-ts'].商品名, 'q-ts'],
    ["UPDATE m_products SET 原価 = 1 WHERE 商品コード = 'q-xs'", "UPDATE m_products SET 原価 = ? WHERE 商品コード = 'q-xs'", prev['q-xs'].原価, 'q-xs'],
    ["UPDATE m_set_components SET 数量 = 9 WHERE セット商品コード = 'q-ts'", "UPDATE m_set_components SET 数量 = ? WHERE セット商品コード = 'q-ts'", prevC['q-ts'][0].数量, 'q-ts'],
    ["INSERT INTO m_products (商品コード, 商品名, 商品区分, 原価状態, updated_at) VALUES ('q-new', '足した', 'セット', 'MISSING', 'x')", "DELETE FROM m_products WHERE 商品コード = 'q-new'", undefined, 'q-new'],
  ];
  assert.ok(prevC['q-ts'].length > 0);   // q-ts の前の行はセット (構成つき)
  for (const [bad, undo, val, code] of tamper) {
    db.prepare(bad).run();
    const vb = MP.verifyApplied(db, { publication: MP.readCurrentPublish(db), ownership: own, taxRates: TAX_RATES, maxProblems: Infinity });
    assert.ok(vb.problems.some((x) => x.code === code && x.col === 'kind_frozen'), `${bad}: ${JSON.stringify(vb.problems.slice(0, 3))}`);
    assert.notEqual(vb.applied_hash, vOk.applied_hash, bad);
    if (val === undefined) db.prepare(undo).run(); else db.prepare(undo).run(val);
  }
  assert.equal(MP.verifyApplied(db, { publication: MP.readCurrentPublish(db), ownership: own, taxRates: TAX_RATES, maxProblems: Infinity }).applied_hash, vOk.applied_hash);
  // 下流 3 系統: f_sales (Amazon の注文のセット展開) / 商品管理リスト / router の未登録一覧
  db.exec('CREATE TABLE IF NOT EXISTS raw_rakuten_orders (order_number TEXT, order_date TEXT, order_status INTEGER, item_number TEXT, item_name TEXT, price_tax_incl REAL, units INTEGER, delete_item_flag INTEGER)');
  const { rebuildFSales } = await import('../apps/warehouse/rebuild-f-sales.js');
  const ins = db.prepare("INSERT INTO raw_sp_orders (amazon_order_id, purchase_date, order_status, seller_sku, title, quantity, item_price, synced_at) VALUES (?, '2026-10-02T10:00:00Z', 'Shipped', ?, 't', 1, 100, 'x')");
  for (const [code] of [...CELLS, ['q-new']]) ins.run(`q-order-${code}`, code);
  await quietly(() => rebuildFSales());
  const fsq = (code) => db.prepare("SELECT SUM(直接販売数) AS d, SUM(セット経由数) AS s FROM f_sales_by_product WHERE 日付 = '2026-10-02' AND 商品コード = ?").get(code);
  assert.deepEqual([fsq('q-st'), fsq('q-ss'), fsq('q-tt'), fsq('q-new')], [{ d: 1, s: 0 }, { d: 1, s: 0 }, { d: null, s: null }, { d: null, s: null }]);   // C 単品は展開しない・C セットは展開・載せない SKU は売上に出ない (Amazon の対応なし)
  assert.ok(fsq('s-ne').s >= 3);   // q-tt (s-ne × 3) は展開
  for (const c of ['q-ts', 'q-xt']) assert.deepEqual(fsq(c), prev[c].商品区分 === 'セット' ? { d: null, s: null } : { d: 1, s: 0 }, c);   // 前の行のまま = 前の区分で展開
  db.prepare("DELETE FROM raw_sp_orders WHERE amazon_order_id LIKE 'q-order-%'").run();
  await quietly(() => rebuildFSales());
  const { buildProductManagementSnapshot } = await import('../apps/warehouse/build-product-management-snapshot.js');
  const pml = await quietly(() => buildProductManagementSnapshot());
  const pr = (code) => db.prepare('SELECT 商品区分 FROM product_management_snapshot_rows WHERE run_id = ? AND 商品コード = ?').get(pml.run_id, code)?.商品区分 ?? null;
  assert.deepEqual(['q-ss', 'q-st', 'q-tt', 'q-xx', 'q-ts', 'q-xs', 'q-new'].map(pr), ['単品', '単品', 'セット', '例外', prev['q-ts'].商品区分, prev['q-xs'].商品区分, null]);
  db.prepare("UPDATE m_products SET 原価 = NULL, 原価状態 = 'MISSING' WHERE 商品コード IN ('q-st', 'q-tt')").run();   // 原価の未登録は単品だけ
  const wr = await import('../apps/warehouse/router.js'); const router = wr.default || wr.router;
  const layer = router.stack.find((l) => l.route && l.route.path === '/api/missing/prioritized' && l.route.methods.get);
  const miss = await new Promise((resolve, reject) => { const res = { statusCode: 200, status(c) { this.statusCode = c; return this; }, json(b) { resolve({ status: this.statusCode, body: b }); }, setHeader() {}, set() { return this; } };
    try { const x = layer.route.stack[0].handle({ query: { type: 'genka' }, body: {}, params: {}, headers: {}, session: {} }, res, reject); if (x && typeof x.catch === 'function') x.catch(reject); } catch (e) { reject(e); } });
  assert.equal(miss.status, 200);
  const mk = Object.fromEntries(miss.body.rows.filter((x) => x.商品コード.startsWith('q-')).map((x) => [x.商品コード, x.商品区分]));
  assert.deepEqual([mk['q-st'], mk['q-tt'], mk['q-new']], ['単品', undefined, undefined]);   // C 単品 (NE セット) は単品の一覧に・C セットは出ない
  // 後片付け: 全部 load に戻して NE からも消す
  await nightly();
  for (const [code] of [...CELLS, ['q-new']]) { db.prepare('DELETE FROM raw_ne_products WHERE 商品コード = ?').run(code); db.prepare('DELETE FROM raw_ne_set_products WHERE セット商品コード = ?').run(code); db.prepare('DELETE FROM exception_genka WHERE sku = ?').run(code); }
  FX.markComplete(db, readNeRawRev);
  assert.equal((await fetchGen(MASTER_OWNERSHIP)).state, 'verified');
  assert.equal((await rebuild()).ok, true);
});

await pg.close();
try { fs.rmSync(tmp, { recursive: true, force: true }); fs.rmSync(mirrorDir, { recursive: true, force: true }); } catch { /* Windows は OS に任せる */ }
console.log(`\n${passed} 件 PASS`);
process.exit(process.exitCode || 0);
