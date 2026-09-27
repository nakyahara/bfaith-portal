/**
 * lz-shadow.mjs — ロジザード用 CSV の影運転の突き合わせ (③b-2a。設計 = AI_reference CompanyDB構想/10 §6.2「③b-2」契約 v1〜v3)
 *   GAS の出力 (2 つの CSV) と、NE の取得の値 (miniPC の lz-shadow-snapshot.mjs の材料) から作った CSV を突き合わせ、記録を残す。
 *   G ドライブが見える PC で Claude が手で流す (定期実行ではない。GAS の出力が新しくなった日だけ = 中原さん L-1)
 *
 * 使い方:
 *   node scripts/company-db/lz-shadow.mjs --snapshot <材料の JSON> [--gas-dir D] [--out-root D] [--lz-list <バーコードマスタ.csv>] [--gas-input <logi_hinban.csv>] [--force]
 *   --gas-dir  GAS の出力のフォルダ (既定 = G:\共有ドライブ\入荷バーコード発行\ロジザードアップロード。D-45 で許されたフォルダ。読むだけ)
 *   --out-root 記録の置き場所 (既定 = AI_reference の CompanyDB構想\_raw\LZ影運転)。実行ごとに <実行 ID> のフォルダを新しく作る
 *   --lz-list  ロジザードにある商品の一覧 (GAS が読んだバーコードマスタ.csv)。無ければ新商品の「どれが載るか」は判定できない。
 *              あっても合格が言うのは「GAS と同じ一覧から同じものが作れた」まで (一覧は直近 30 日の書き出し。正しい集合かは ③c で全件の一覧で確かめる)
 *   --gas-input GAS が読んだ NE の品番マスタ。写しを残すだけ (GAS の入力で再現する道は実物を見てから作る = 今は時刻のずれと言えない)
 *   --force    GAS の出力が前の回と同じでも流す
 * 🚨 写しの名前は必ず shadow_<実行 ID>_<元の名前> (GAS は Drive 全体からファイル名で探す = 元の名前の写しを置くと本番の GAS が読むおそれ。契約 v3 H3)
 * 🚨 GAS の出力のフォルダには何も書かない。記録のフォルダが GAS の出力のフォルダの親 (入荷バーコード発行) の中・または GAS のフォルダを含むなら止める (リンクも実体で見る)。
 *    記録のフォルダのファイルは新しく作るだけ (上書きしない)
 * 合格 (両方のファイルが pass) したら、miniPC で台帳の完了の ping を打つ (手順 = db/company/README.md「ロジザード用 CSV の影運転」)
 */
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { buildLzCsv, DAILY, NEW, LZ_CONVERTER_VERSION, ICONV_EXPECTED, iconvVersion } from '../../apps/master-decisions/lz-csv.mjs';
import { compareLz, parseCsvBytes, lzIdsFromBarcodeMaster, LZ_COMPARE_VERSION } from '../../apps/master-decisions/lz-compare.mjs';
import { LZ_SNAPSHOT_FORMAT } from '../../apps/master-decisions/lz-snapshot.mjs';

export const DEFAULT_GAS_DIR = 'G:\\共有ドライブ\\入荷バーコード発行\\ロジザードアップロード';
export const DEFAULT_OUT_ROOT = 'G:\\共有ドライブ\\AI_reference\\システム設計\\CompanyDB構想\\_raw\\LZ影運転';
/** GAS が名前で探す・作るファイル (写しにこの名前を使わない) */
export const GAS_NAMES = Object.freeze(['logi_hinban.csv', 'バーコードマスタ.csv', DAILY.file, NEW.file]);
export const RUN_ID_RE = /^lzs_\d{8}T\d{9}Z_[0-9a-f]{6}$/;
export const RULES_REF = 'AI_reference CompanyDB構想/10 §6.2 ③b-2 契約 v1〜v3・実測 (2026-09-27)';

const sha256 = (buf) => crypto.createHash('sha256').update(buf).digest('hex');
export const makeRunId = (now = new Date()) => `lzs_${now.toISOString().replace(/[-:.]/g, '')}_${crypto.randomBytes(3).toString('hex')}`;
export const shadowName = (runId, name) => {
  const n = `shadow_${runId}_${name}`;
  if (GAS_NAMES.includes(n) || !n.startsWith('shadow_lzs_')) throw new Error(`写しの名前が GAS の名前と重なる: ${n}`);
  return n;
};

export function parseArgs(argv) {
  const out = { snapshot: null, gasDir: DEFAULT_GAS_DIR, outRoot: DEFAULT_OUT_ROOT, lzList: null, gasInput: null, force: false };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--snapshot') out.snapshot = argv[++i];
    else if (a === '--gas-dir') out.gasDir = argv[++i];
    else if (a === '--out-root') out.outRoot = argv[++i];
    else if (a === '--lz-list') out.lzList = argv[++i];
    else if (a === '--gas-input') out.gasInput = argv[++i];
    else if (a === '--force') out.force = true;
    else throw new Error(`知らない引数: ${a}`);
  }
  if (!out.snapshot) throw new Error('--snapshot が要る (miniPC の lz-shadow-snapshot.mjs の出力)');
  return out;
}

/** ファイルを読んで、記録に残す情報 (元の場所・大きさ・時刻・sha256) を付ける。無ければ null */
function readSource(p) {
  if (!fs.existsSync(p)) return null;
  const buf = fs.readFileSync(p);
  const st = fs.statSync(p);
  return { buf, info: { source: p, bytes: buf.length, mtime: st.mtime.toISOString(), sha256: sha256(buf) } };
}

/** 実体のパス (まだ無い部分は、ある所まで実体にしてから足す = リンク・ジャンクションも実体で比べる) */
function realOrNearest(p) {
  let cur = path.resolve(p); const rest = [];
  while (!fs.existsSync(cur)) { const up = path.dirname(cur); if (up === cur) break; rest.unshift(path.basename(cur)); cur = up; }
  return path.join(fs.existsSync(cur) ? fs.realpathSync.native(cur) : cur, ...rest);
}
const foldPath = (x) => (process.platform === 'win32' ? x.toLowerCase() : x);
const inside = (a, b) => { const r = path.relative(foldPath(b), foldPath(a)); return r === '' || (!r.startsWith('..') && !path.isAbsolute(r)); };
/**
 * 記録の置き場所が本番を壊さないか (Codex #1498 R1 M6)。GAS の出力のフォルダの親 (= 入荷バーコード発行。GAS の入力もここ) の中、
 * または GAS のフォルダを含む場所 (G:\共有ドライブ など) には書かない
 */
export function assertOutRootSafe(outRoot, gasDir) {
  const o = realOrNearest(outRoot), g = realOrNearest(gasDir);
  if (inside(o, path.dirname(g)) || inside(g, o)) throw new Error(`記録の置き場所 ${outRoot} は GAS のフォルダ (${gasDir}) の近く = 書かない`);
}

/** 前の回の manifest (実行 ID の順で最後のもの) */
export function lastManifest(outRoot) {
  let names = [];
  try { names = fs.readdirSync(outRoot).filter((n) => RUN_ID_RE.test(n)).sort(); } catch { return null; }
  for (const n of names.reverse()) {
    const f = path.join(outRoot, n, shadowName(n, 'manifest.json'));
    try { return JSON.parse(fs.readFileSync(f, 'utf8')); } catch { /* 途中で止まった回は飛ばす */ }
  }
  return null;
}

/**
 * 新商品の CSV に載る商品 (こちらの側)。
 *   ロジザードにある商品の一覧があれば: 元のコードがその一覧に無い商品 (GAS と同じく文字の完全一致)。
 *   無ければ: GAS の新商品の CSV にある商品ID と同じ元のコードの商品だけ (値だけ比べる。どれが載るかは判定できない)
 */
export function newItemsFor(items, { lzIds, gasNewKeys }) {
  if (lzIds) {
    const lower = new Set([...lzIds].map((x) => x.toLowerCase()));
    const out = [];
    for (const it of items) {
      if (!it.ne_code) { out.push(it); continue; }   // 元のコードが無い = 載るか分からない = 作れない行として数える
      if (lzIds.has(it.ne_code)) continue;   // ロジザードにある (文字の完全一致 = GAS と同じ)
      // 大文字・小文字だけ違う商品ID がロジザードにある = 別の商品として新規登録になるおそれ = 作らずに止める (契約 v2 H1・Codex #1498 R1 H2)
      if (lower.has(it.ne_code.toLowerCase())) { out.push({ ...it, ne_code: null, code_reason: 'lz_case_collision' }); continue; }
      out.push(it);
    }
    return out;
  }
  const keys = new Set(gasNewKeys);
  const lower = new Set(gasNewKeys.map((k) => k.toLowerCase()));
  return items.filter((it) => (it.ne_code ? keys.has(it.ne_code) : lower.has(it.code_norm)));
}

/**
 * 1 回の影運転 (ファイルの読み書きつき)。戻り値 = { skipped?, runId, dir, manifest }
 */
export function runLzShadow({ snapshotPath, gasDir, outRoot, lzListPath = null, gasInputPath = null, force = false, now = new Date() }) {
  assertOutRootSafe(outRoot, gasDir);
  const iv = iconvVersion();
  if (iv !== ICONV_EXPECTED) throw new Error(`iconv-lite の版が ${iv} (実測した表は ${ICONV_EXPECTED})。表を実測し直してから版を上げる`);
  const snapSrc = readSource(snapshotPath);
  if (!snapSrc) throw new Error(`材料が無い: ${snapshotPath}`);
  const snap = JSON.parse(snapSrc.buf.toString('utf8'));
  if (snap.format !== LZ_SNAPSHOT_FORMAT) throw new Error(`材料の形が違う: ${snap.format}`);
  if (!snap.ne || !snap.ne.ok) throw new Error(`材料の NE の取得を使えない: ${snap.ne && snap.ne.reason}`);
  const gasDaily = readSource(path.join(gasDir, '商品マスタ', DAILY.file));
  if (!gasDaily) throw new Error(`GAS の毎日の商品マスタが無い: ${path.join(gasDir, '商品マスタ', DAILY.file)}`);
  const gasNew = readSource(path.join(gasDir, NEW.file));
  const lz = lzListPath ? readSource(lzListPath) : null;
  if (lzListPath && !lz) throw new Error(`ロジザードの商品の一覧が無い: ${lzListPath}`);
  const gi = gasInputPath ? readSource(gasInputPath) : null;
  if (gasInputPath && !gi) throw new Error(`GAS の入力が無い: ${gasInputPath}`);
  // L-1: GAS の出力が前の回から変わっていなければ流さない
  const prev = lastManifest(outRoot);
  if (!force && prev && prev.files && prev.files.gas_daily && prev.files.gas_daily.sha256 === gasDaily.info.sha256
      && ((prev.files.gas_new && prev.files.gas_new.sha256) || null) === (gasNew ? gasNew.info.sha256 : null)) {
    return { skipped: 'gas_unchanged', prev_run: prev.run_id };
  }
  const runId = makeRunId(now);
  const dir = path.join(outRoot, runId);
  fs.mkdirSync(outRoot, { recursive: true });
  fs.mkdirSync(dir);   // 同じ実行 ID のフォルダがあれば止まる
  const write = (name, buf) => { const f = path.join(dir, shadowName(runId, name)); fs.writeFileSync(f, buf, { flag: 'wx' }); return { file: path.basename(f), bytes: buf.length, sha256: sha256(buf) }; };

  // ── 毎日の商品マスタ ──
  const ours = buildLzCsv(snap.items, 'daily');
  const daily = compareLz({ gas: gasDaily.buf, ours, compareCols: [0, 1, 2, 3, 4], header: DAILY.header });
  // ── 新商品 ──
  let newer = null, oursNew = null, lzInfo = null;
  if (gasNew) {
    const gasNewKeys = parseCsvBytes(gasNew.buf).records.slice(1).map((r) => r.cells[0].toString('latin1')).filter((k) => k !== '');
    let lzIds = null, setCheck = null;
    if (lz) {
      const x = lzIdsFromBarcodeMaster(lz.buf);
      lzInfo = { rows: x.rows, reason: x.reason, ids: x.ids ? x.ids.size : 0,
        case_variants: x.ids ? x.ids.size - new Set([...x.ids].map((v) => v.toLowerCase())).size : 0 };   // 大文字・小文字だけ違う ID の組 (情報)
      if (x.ids) lzIds = x.ids; else setCheck = `ロジザードの商品の一覧を読めない (${x.reason})`;
    } else setCheck = 'ロジザードにある商品の一覧 (GAS が読んだバーコードマスタ.csv) が無い';
    oursNew = buildLzCsv(newItemsFor(snap.items, { lzIds, gasNewKeys }), 'new');
    newer = compareLz({ gas: gasNew.buf, ours: oursNew, compareCols: [0, 1, 2, 3, 6], setCheck, header: NEW.header });
  }
  const verdict = daily.verdict === 'pass' && newer && newer.verdict === 'pass' ? 'pass' : 'fail';
  // 新商品の「どれが載るか」: 合格でも GAS と同じ一覧 (直近 30 日の書き出し) から同じものが作れた、まで。正しい集合かは ③c で全件の一覧で確かめる (Codex #1498 R1 H1)
  const newSetBasis = !gasNew ? null : lz && lzInfo && !lzInfo.reason ? 'gas_list_reproduction' : 'not_checked';
  const files = {
    gas_daily: { ...gasDaily.info, copy: write(DAILY.file, gasDaily.buf) },
    gas_new: gasNew ? { ...gasNew.info, copy: write(NEW.file, gasNew.buf) } : null,
    lz_list: lz ? { ...lz.info, copy: write('バーコードマスタ.csv', lz.buf), ...lzInfo } : null,
    gas_input: gi ? { ...gi.info, copy: write('logi_hinban.csv', gi.buf) } : null,
    snapshot: { ...snapSrc.info, copy: write('ne_snapshot.json', snapSrc.buf) },
    ours_daily: write(`cdb_${DAILY.file}`, ours.bytes),
    ours_new: oursNew ? write(`cdb_${NEW.file}`, oursNew.bytes) : null,
  };
  // 「?」にした文字の位置・元の文字・理由 (NE の側で化けていた / こちらの変換) と推測の印も行ごとに残す (契約 v2 M6・Codex #1498 R1 M7)
  const details = (b) => ({ counts: b.counts, unmade: b.unmade, file_unverified: b.file_unverified,
    subs: b.rows.flatMap((r) => r.subs.map((x) => ({ code: r.key, ...x }))), unverified: b.rows.flatMap((r) => r.unverified.map((x) => ({ code: r.key, ...x }))) });
  const report = { run_id: runId, daily: { build: details(ours), compare: daily },
    new: newer ? { build: details(oursNew), set_basis: newSetBasis, compare: newer } : { skipped: 'GAS の新商品の CSV が無い' } };
  files.report = write('report.json', Buffer.from(JSON.stringify(report, null, 1), 'utf8'));
  const manifest = {
    run_id: runId, at: now.toISOString(), verdict,
    versions: { converter: LZ_CONVERTER_VERSION, compare: LZ_COMPARE_VERSION, iconv_lite: iv, snapshot: snap.format, rules: RULES_REF },
    snapshot: { taken_at: snap.taken_at, ne_marks: snap.ne.marks, code_mark: snap.cdb ? snap.cdb.mark : null, counts: snap.counts },
    // GAS が読んだ NE の品番マスタ (logi_hinban.csv) で再現する道はまだ無い = 時刻のずれとは言えない。渡されたら写しだけ残す (あとで再現に使う)
    gas_input: gi ? { saved: true, reproduced: false } : 'none',
    new_set: { basis: newSetBasis, certified: false, note: '正しい集合かは ③c で全件の一覧で確かめる' },
    summary: { daily: { verdict: daily.verdict, ...daily.summary, same_rows: daily.counts.same_rows, gas_rows: daily.counts.gas_rows },
      new: newer ? { verdict: newer.verdict, ...newer.summary, same_rows: newer.counts.same_rows, gas_rows: newer.counts.gas_rows, compared_cols: newer.compared_cols, not_compared_cols: newer.not_compared_cols } : null },
    files,
  };
  write('manifest.json', Buffer.from(JSON.stringify(manifest, null, 1), 'utf8'));
  return { runId, dir, manifest, report };
}

export function summaryLines(r) {
  if (r.skipped) return [`⏭️ ロジザード用 CSV の影運転: GAS の出力が前の回 (${r.prev_run}) から変わっていない (流すなら --force)`];
  const m = r.manifest, d = m.summary.daily, n = m.summary.new;
  const part = (x) => `${x.verdict === 'pass' ? '✅ 合格' : '❌ 不合格'} (同じ行 ${x.same_rows}/${x.gas_rows}・説明できない差 ${x.unexplained}・判定できない ${x.undeterminable}・形の差 ${x.shape}・許す差 ${x.allowed}・初めて確かめた形 ${x.rules_first_seen})`;
  const lines = [
    `${m.verdict === 'pass' ? '✅' : '⚠️'} ロジザード用 CSV の影運転 ${m.run_id}: ${m.verdict === 'pass' ? '合格' : '不合格'}`,
    `  毎日の商品マスタ: ${part(d)}`,
    `  新商品: ${n ? part(n) : 'GAS の新商品の CSV が無い'}`,
    ...(n ? [`    比べた列 = ${n.compared_cols.join('・')} / 比べない (人が入れる) = ${n.not_compared_cols.join('・')}`,
      `    どれが載るか = ${m.new_set.basis === 'gas_list_reproduction' ? 'GAS が読んだ一覧 (直近 30 日の書き出し) での再現だけ (正しい集合かは ③c で全件の一覧で確かめる)' : '確かめていない (GAS が読んだバーコードマスタ.csv が無い)'}`] : []),
    ...(m.gas_input !== 'none' ? ['  GAS の入力 (logi_hinban.csv) = 写しを残した (再現の道はまだ無い)'] : []),
    `  記録: ${r.dir}`,
  ];
  if (m.verdict === 'pass') lines.push('  → 合格。miniPC で台帳の完了の ping (lz-shadow-compare) を打つ (db/company/README.md「ロジザード用 CSV の影運転」)');
  return lines;
}

const fold = (x) => (process.platform === 'win32' ? x.toLowerCase() : x);
const isMain = (() => { try { return !!process.argv[1] && fold(fs.realpathSync.native(process.argv[1])) === fold(fs.realpathSync.native(fileURLToPath(import.meta.url))); } catch { return false; } })();
if (isMain) {
  try {
    const a = parseArgs(process.argv.slice(2));
    const r = runLzShadow({ snapshotPath: a.snapshot, gasDir: a.gasDir, outRoot: a.outRoot, lzListPath: a.lzList, gasInputPath: a.gasInput, force: a.force });
    for (const l of summaryLines(r)) console.log(l);
  } catch (e) {
    console.log(`❌ ロジザード用 CSV の影運転: ${String(e && e.message).replace(/\s+/g, ' ').slice(0, 400)}`);
    process.exitCode = 1;
  }
}
