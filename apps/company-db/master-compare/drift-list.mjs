/**
 * drift-list.mjs — 代表 (親) のずれの一覧 (読むだけ・0068・AI_reference CompanyDB構想/20_代表の正本を自社DBへ_設計 v7 §② の「数える (当日の前)」・§⑩ PR-6)
 *
 * 何をするか: miniPC の warehouse.db の NE の完全な取得 (照合 ② と同じ読み方 = readNeSide・nModelOf・取込の整合) から単品の代表の観測を作り、
 *   Company DB の ops.parent_raw_gate (watcher・読むだけ) に数えさせて、6 つの数えと一覧を出す。
 *   数え = parent_mismatch (NE と違う) / parent_incomparable (比べられない) / parent_ambiguous (当たる親が 2 つ以上) / parent_missing (社内に親が無い) /
 *          parent_two_level (2 段) / parent_loop (循環)。単品だけ・登録の状態 × 品目の状態の判定表 (設計 v7 §⑥) で対象を分ける (下書きは数えない など)
 *   1 件ずつ「NE と社内のどちらが正しいか」を人が決めて直す (NE を正 = 持ち主 load の間に夜間ロードが直す / 社内を正 = NE の画面で人が直す・翌朝の照合で確かめる)。
 *   全部 0 になってから widen (products.parent) の当日の段へ。
 * 🚨 何も書かない (読み取りだけの取引・watcher のロール)。本番のランナー (daily-sync・照合) を流さない。毎朝の数えは照合 ② (run.mjs) が残す
 *
 * 使い方 (miniPC):
 *   node apps/company-db/master-compare/drift-list.mjs [--data-dir D] [--class parent_mismatch] [--limit 200] [--json]
 * env: COMPANY_DB_WATCH_URL (watcher)・DATA_DIR (warehouse.db)
 * 終わり方: 数えた = exit 0 (ずれがあっても)・数えられない (warehouse.db が無い・取得の印が無い・整合が読めない・DB に届かない・0068 の前) = exit 1
 */
import 'dotenv/config';
import fs from 'node:fs';
import { fileURLToPath } from 'node:url';
import { readNeSide, nModelOf, neIntegrity, neIntegrityRows, resolveNeCodes } from './compare-ne.mjs';
import { parentObservations, parentObsTrust, readParentCounts, repSpellingsOf, PARENT_COUNT_KEYS, PARENT_COUNT_JA } from './parent-gate.mjs';

/**
 * 一覧を作る (読むだけ)。ne = readNeSide の結果・db = watcher の接続 ({ query })。
 * @returns {Promise<{ ne_fetch: object, counts, counted, excluded, obs, items, gate }>}
 */
export async function driftList({ ne, db }) {
  if (!ne || ne.error) throw new Error(`NE の取得を読めない (${ne ? ne.error : 'なし'})`);
  if (!ne.hasSrc) throw new Error('warehouse.db が古い (元の値 *_src の列が無い)');
  const M = ne.meta || {};
  if (!M.ne_api_products_complete_at || !M.ne_api_setproducts_complete_at) throw new Error('NE の取得の完了の印が無い (取得の途中・失敗) = 数えない');
  const integ = neIntegrity(M);
  if (!integ) throw new Error('取込の整合 (ne_api_*_integrity) を読めない = 数えない');
  if ((await db.query("select to_regprocedure('ops.parent_raw_gate(integer, jsonb, boolean)') is not null as ok")).rows[0].ok !== true) {
    throw new Error('Company DB に ops.parent_raw_gate が無い (0068 の前)');
  }
  const { m: nm, collided } = nModelOf(ne);
  // 🆕 #1676 Codex R2 High: 代表の名前空間の書き方の台帳を読めない = 数えない (空 = 「衝突なし」と読まない)
  const sp = repSpellingsOf(resolveNeCodes(ne.spellings));
  if (sp.state !== 'ok') throw new Error(`NE のコードの元の書き方 (代表) を読めない (${sp.reason}) = 数えない (書き方の衝突を見落とす。NE の取得の後に集め終えた印を確かめる)`);
  // 🆕 #1676 Codex R3 High 2・R4: 照合 ② と同じ許可の一覧 (取得の件数・取込の整合・区分のゲートの integrity_untrusted・台帳) の全部が ok のときだけ数える
  const trust = parentObsTrust({ fetch_counts: ne.fetchCounts, integrity: integ, kind_gate: { integrity_untrusted: neIntegrityRows(ne, integ.intBlocked).integrityRows }, rep_spellings: sp });
  if (!trust.complete) throw new Error(`NE の取得を確かめられない (${trust.reasons.join(', ')}) = 数えない (どのコードか特定できない。取得の件数・取込の整合を確かめて取り直す)`);
  const obs = parentObservations(nm, { untrusted: [...collided, ...integ.intBlocked.keys()], trust, repSpellings: sp });
  const r = await readParentCounts(db, obs, { detail: true });
  return { ne_fetch: { products_complete_at: M.ne_api_products_complete_at, setproducts_complete_at: M.ne_api_setproducts_complete_at,
    rows_dropped: integ.absenceUntrusted }, ...r };
}

const CLASS_RE = new RegExp(`^(${PARENT_COUNT_KEYS.join('|')})$`);
export function parseArgs(argv) {
  const out = { dataDir: null, json: false, cls: null, limit: 200 };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--data-dir') out.dataDir = argv[++i];
    else if (a === '--json') out.json = true;
    else if (a === '--class') { out.cls = argv[++i]; if (!CLASS_RE.test(out.cls || '')) throw new Error(`--class は ${PARENT_COUNT_KEYS.join(' / ')}`); }
    else if (a === '--limit') { out.limit = Number(argv[++i]); if (!Number.isInteger(out.limit) || out.limit < 1) throw new Error('--limit は 1 以上の整数'); }
    else throw new Error(`知らない引数: ${a}`);
  }
  return out;
}

/** 人が読む形 (行の配列) */
export function formatDriftList(r, { cls = null, limit = 200 } = {}) {
  const lines = [];
  const total = PARENT_COUNT_KEYS.reduce((a, k) => a + (Number(r.counts?.[k]) || 0), 0);
  lines.push(`代表 (親) のずれ ${total} 件 (数えた単品 ${r.counted}・NE の取得 ${r.ne_fetch.products_complete_at} UTC${r.ne_fetch.rows_dropped ? '・🚨 取得で行が落ちた' : ''})`);
  lines.push(`  ${PARENT_COUNT_KEYS.map((k) => `${PARENT_COUNT_JA[k]} ${r.counts?.[k] ?? 0}`).join(' / ')}`);
  const ex = r.excluded || {};
  lines.push(`  数えない: セット ${ex.set ?? 0}・例外 ${ex.exception ?? 0}・下書き ${ex.draft ?? 0}・配った確かめ待ち ${ex.issued ?? 0}・取り込めなかった ${ex.failed ?? 0}・やめた ${ex.cancelled ?? 0}`);
  const g = r.gate || {};
  lines.push(`  門: 持ち主 ${g.enforced ? 'company (0 でないと新しい NE 登録の CSV が閉じる)' : 'load (知らせだけ)'}・${g.open ? '開いている' : `閉じている (${(g.problems || []).join(' / ')})`}`);
  const items = (r.items || []).filter((x) => !cls || x.class === cls);
  for (const x of items.slice(0, limit)) {
    lines.push(`  ${x.class.padEnd(20)} ${String(x.code).padEnd(24)} NE の代表 ${x.ne_parent ?? '(なし)'} / 社内の親 ${x.cdb_parent ?? '(なし)'}${x.reason ? ` [${x.reason}]` : ''}`
      + ` (登録 ${x.reg_state ?? '行なし'}${x.item_state ? `・CSV ${x.item_state}` : ''})`);
  }
  if (items.length > limit) lines.push(`  … ほか ${items.length - limit} 件 (--limit で増やす・--json で全部)`);
  return lines;
}

const fold = (x) => (process.platform === 'win32' ? x.toLowerCase() : x);
const isMain = (() => { try { return !!process.argv[1] && fold(fs.realpathSync.native(process.argv[1])) === fold(fs.realpathSync.native(fileURLToPath(import.meta.url))); } catch { return false; } })();
if (isMain) {
  let code = 1, close = null;
  try {
    const a = parseArgs(process.argv.slice(2));
    const dataDir = (a.dataDir || process.env.DATA_DIR || '').trim();
    if (!dataDir) throw new Error('DATA_DIR が無い (--data-dir でも可)');
    const url = (process.env.COMPANY_DB_WATCH_URL || '').trim();
    if (!url) throw new Error('COMPANY_DB_WATCH_URL が無い (watcher = 読むだけ)');
    const ne = readNeSide(dataDir);
    const { connectWatcher } = await import('./run.mjs');
    const c = await connectWatcher(url);
    close = c.close;
    const r = await driftList({ ne, db: c.db });
    if (a.json) console.log(JSON.stringify(r, null, 1));
    else for (const l of formatDriftList(r, { cls: a.cls, limit: a.limit })) console.log(l);
    code = 0;
  } catch (e) {
    console.error(`❌ drift-list: ${String(e && e.message).replace(/\s+/g, ' ').slice(0, 400)}`);
    code = 1;
  } finally { if (close) { try { await close(); } catch { /* */ } } }
  // pg の直後に process.exit() しない (Windows の Node は libuv の assertion で 127 になる。#1386)
  process.exitCode = code;
  setTimeout(() => process.exit(code), 10000).unref();
}
