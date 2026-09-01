/**
 * 取込 + カバー率の結合テスト。
 *   node apps/postage/smoke.mjs
 *
 * 一時ディレクトリに postage.db と warehouse.db (fixture) を作って動かすので、本番には触れない。
 * 実データ (2026-08-30 の『定形外の重さ.xlsx』800行) に混ざっていた壊れ方を、そのまま再現して検証する。
 */
import ExcelJS from 'exceljs';
import Database from 'better-sqlite3';
import fs from 'fs';
import os from 'os';
import path from 'path';

const TMP = fs.mkdtempSync(path.join(os.tmpdir(), 'postage-smoke-'));
process.env.DATA_DIR = TMP;
process.env.POSTAGE_WAREHOUSE_DB = path.join(TMP, 'warehouse.db');

let pass = 0, fail = 0;
const t = (name, fn) => {
  try { fn(); pass++; console.log(`  ok   ${name}`); }
  catch (e) { fail++; console.log(`  FAIL ${name}\n       ${e.message}`); }
};
const eq = (a, b, what) => {
  if (JSON.stringify(a) !== JSON.stringify(b)) throw new Error(`${what || ''} expected ${JSON.stringify(b)}, got ${JSON.stringify(a)}`);
};
const throws = (fn, re, what) => {
  try { fn(); } catch (e) { if (re.test(e.message)) return; throw new Error(`${what || ''} 別のエラー: ${e.message}`); }
  throw new Error(`${what || ''} エラーになりませんでした`);
};

const HEADERS = ['商品コード', '商品名', '商品の重さ', '資材', '資材の重さ', '送り状の重さ', '資材の厚み', '商品の厚み'];

// ── 重量表 fixture (実データの壊れ方を再現) ──────────────────
async function makeXlsx(file) {
  const wb = new ExcelJS.Workbook();
  const ws = wb.addWorksheet('定形外商品コード');
  ws.addRow(HEADERS);
  // 正常 — 資材列あり
  ws.addRow(['sku-a', 'テスト商品A_長3封', 15, '茶封筒', '5g', '', '', '0.1cm']);
  // 資材列が空 → 商品名の末尾 `_梱機プ` から白プチを引く
  ws.addRow(['sku-b', 'テスト商品B_梱機プ', 30, '', '', '', '', '2cm']);
  // 列ズレ — 商品の厚みが空で、送り状の重さ / 資材の厚み に cm (実データ8行と同じ形)
  ws.addRow(['sku-c', 'テスト商品C_長3封', 12, '茶封筒', '5g', '0.1cm', '0.1cm', '']);
  // 重複 (内容が同じ) — 取り込みは通し、注意だけ出す
  ws.addRow(['sku-a', 'テスト商品A_長3封', 15, '茶封筒', '5g', '', '', '0.1cm']);
  // 重さの列に長さの単位 → この商品は丸ごと見送り
  ws.addRow(['sku-d', 'テスト商品D_長3封', '0.5cm', '茶封筒', '5g', '', '', '0.2cm']);
  // 資材列と商品名サフィックスの食い違い → 注意 (資材列を採用)
  ws.addRow(['sku-e', 'テスト商品E_長3封', 20, '白ビ袋', '11g', '', '', '1.5cm']);
  // 重さ未登録 (空欄は許す)
  ws.addRow(['sku-f', 'テスト商品F_長3封', '', '茶封筒', '5g', '', '', '0.3cm']);
  // 重複 (内容が違う) — どちらが正か決まるまで **先頭行も含めて** 取り込まない
  ws.addRow(['sku-g', 'テスト商品G_長3封', 20, '茶封筒', '5g', '', '', '0.4cm']);
  ws.addRow(['sku-g', 'テスト商品G_長3封', 99, '茶封筒', '5g', '', '', '0.4cm']);
  // 重さ 0 → 入力ミスとして丸ごと見送り
  ws.addRow(['sku-h', 'テスト商品H_長3封', 0, '茶封筒', '5g', '', '', '0.1cm']);
  await wb.xlsx.writeFile(file);
}

// ── 出荷実績 fixture ────────────────────────────────────────
function makeWarehouse(file) {
  const d = new Database(file);
  d.exec('CREATE TABLE raw_ne_order_base (伝票番号 TEXT, 出荷確定日 TEXT, 配送方法名 TEXT)');
  d.exec('CREATE TABLE raw_ne_orders (伝票番号 TEXT, 商品コード TEXT, 商品名 TEXT, 受注数 INTEGER, キャンセル区分 TEXT)');
  const ib = d.prepare('INSERT INTO raw_ne_order_base VALUES (?,?,?)');
  const io = d.prepare('INSERT INTO raw_ne_orders VALUES (?,?,?,?,?)');
  const slip = (no, date, lines, method = '定形外郵便') => {
    ib.run(no, `${date} 10:00:00`, method);
    for (const [sku, qty] of lines) io.run(no, sku, sku, qty, '有効');
  };
  d.transaction(() => {
    slip('1', '2026-06-01', [['sku-a', 1]]);               // 茶封筒・薄い・軽い → 定形
    slip('2', '2026-06-01', [['sku-a', 1]]);
    slip('3', '2026-06-02', [['sku-b', 1]]);               // 白プチ = 外寸未測定 → missing_dims
    slip('4', '2026-06-02', [['sku-f', 1]]);               // 重さ未登録 → missing_weight
    slip('5', '2026-06-03', [['sku-c', 1]]);               // 取り込まれていない → missing_sku
    slip('6', '2026-06-03', [['sku-zzz', 1]]);             // マスタに無い → missing_sku
    slip('7', '2026-06-04', [['sku-a', 1]], 'ヤマト(ネコポス)'); // 定形外以外は対象外
    slip('8', '2026-06-04', [['sku-a', 1], ['sku-e', 1]]); // 資材が食い違う → material_conflict
    slip('9', '2026-06-05', [['sku-g', 1]]);               // 食い違う重複 → 取り込まれない
  })();
  d.close();
}

console.log(`一時ディレクトリ: ${TMP}\n`);
makeWarehouse(process.env.POSTAGE_WAREHOUSE_DB);
const xlsx = path.join(TMP, 'weights.xlsx');
await makeXlsx(xlsx);

const { initPostageDB, getDB, closePostageDB, getTariffVersionFor } = await import('./db.js');
const { importWeightFile } = await import('./import.js');
const { coverageReport } = await import('./coverage.js');
initPostageDB();

console.log('■ 種データ');
t('料金表が入っている (定形110 / 規格内140 / 規格外260)', () => {
  const db = getDB();
  eq(db.prepare("SELECT amount_yen a FROM pm_tariff_bands WHERE band_code='teikei_50'").get().a, 110);
  eq(db.prepare("SELECT amount_yen a FROM pm_tariff_bands WHERE band_code='kikakunai_50'").get().a, 140);
  eq(db.prepare("SELECT amount_yen a FROM pm_tariff_bands WHERE band_code='kikakugai_50'").get().a, 260);
});
t('送り状シール 0.5g が入っている', () => {
  eq(getDB().prepare("SELECT weight_g g FROM pm_overheads WHERE code='okurijo_seal'").get().g, 0.5);
});
t('資材の外寸は 3 種とも未実測のまま (推測で確定させない)', () => {
  const rows = getDB().prepare('SELECT dims_verified FROM pm_materials').all();
  eq(rows.every((r) => r.dims_verified === 0), true);
});
t('有効期間が重なる料金表があったら判定を止める', () => {
  const db = getDB();
  db.prepare("INSERT INTO pm_tariff_versions (name, valid_from) VALUES ('重複版','2025-01-01')").run();
  eq(getTariffVersionFor('2026-06-01'), null, '新しいほうを黙って採用しない');
  db.prepare("DELETE FROM pm_tariff_versions WHERE name='重複版'").run();
  eq(getTariffVersionFor('2026-06-01') !== null, true);
});

console.log('\n■ 取込 (dry-run)');
const dry = await importWeightFile(xlsx, { dryRun: true, actor: 'smoke' });
t('dry-run では pm_skus に入らない', () => {
  eq(getDB().prepare('SELECT COUNT(*) n FROM pm_skus').get().n, 0);
});
t('列ズレを検出する (送り状の重さ / 資材の厚み)', () => {
  const shifts = dry.issues.filter((i) => i.kind === 'column_shift');
  eq(shifts.length >= 2, true, `column_shift が ${shifts.length} 件`);
  eq(shifts.some((i) => i.column_name === '送り状の重さ'), true);
});
t('重さの列に cm → 要修正', () => {
  eq(dry.issues.some((i) => i.sku_code === 'sku-d' && i.severity === 'error'), true);
});
t('重さ 0 は要修正 (欠測は空欄)', () => {
  eq(dry.issues.some((i) => i.sku_code === 'sku-h' && i.kind === 'zero_weight'), true);
});
t('要修正を含む商品は「取り込みません」と明示される', () => {
  const rejected = dry.issues.filter((i) => i.kind === 'row_rejected').map((i) => i.sku_code).sort();
  eq(rejected, ['sku-c', 'sku-d', 'sku-h']);
});
t('内容が同じ重複は「注意」、違う重複は「要修正」', () => {
  const dup = dry.issues.filter((i) => i.kind === 'duplicate_sku');
  eq(dup.some((i) => i.sku_code === 'sku-a' && i.severity === 'warn'), true, '同内容');
  eq(dup.some((i) => i.sku_code === 'sku-g' && i.severity === 'error'), true, '内容違い');
});
t('資材列と商品名サフィックスの食い違いを拾う', () => {
  eq(dry.issues.some((i) => i.kind === 'material_mismatch' && i.sku_code === 'sku-e'), true);
});

console.log('\n■ 取込 (反映)');
const applied = await importWeightFile(xlsx, { dryRun: false, actor: 'smoke' });
t('資材列が空でも商品名の末尾から資材が入る', () => {
  eq(getDB().prepare("SELECT default_material_code m FROM pm_skus WHERE sku_code='sku-b'").get().m, 'shiropuchi');
});
t('食い違う重複は先頭行も含めて取り込まない (先勝ちしない)', () => {
  eq(getDB().prepare("SELECT COUNT(*) n FROM pm_skus WHERE sku_code='sku-g'").get().n, 0);
});
t('列ズレの行を含む商品は丸ごと取り込まない (部分反映しない)', () => {
  eq(getDB().prepare("SELECT COUNT(*) n FROM pm_skus WHERE sku_code='sku-c'").get().n, 0);
});
t('重さが読めなかった商品も丸ごと見送り', () => {
  eq(getDB().prepare("SELECT COUNT(*) n FROM pm_skus WHERE sku_code IN ('sku-d','sku-h')").get().n, 0);
});
t('空欄の重さは NULL のまま入る (0 で埋めない)', () => {
  const r = getDB().prepare("SELECT unit_weight_g w, thickness_mm t FROM pm_skus WHERE sku_code='sku-f'").get();
  eq(r.w, null); eq(r.t, 3);
});
t('厚みは cm → mm に変換される (0.1cm → 1mm)', () => {
  eq(getDB().prepare("SELECT thickness_mm t FROM pm_skus WHERE sku_code='sku-a'").get().t, 1);
});
t('2回目の取込で件数が二重にならない (upsert)', () => {
  eq(applied.applied, getDB().prepare('SELECT COUNT(*) n FROM pm_skus').get().n);
});

console.log('\n■ 取込の入口を守る');
await (async () => {
  const bad = path.join(TMP, 'bad.xlsx');
  const wb = new ExcelJS.Workbook();
  const ws = wb.addWorksheet('別物');
  ws.addRow(['商品コード', 'なにか']);
  ws.addRow(['x', 'y']);
  await wb.xlsx.writeFile(bad);
  let msg = '';
  try { await importWeightFile(bad, { dryRun: true }); } catch (e) { msg = e.message; }
  t('必須列が無いファイルは弾く', () => { eq(/必須の列/.test(msg), true, msg); });

  const dupHead = path.join(TMP, 'duphead.xlsx');
  const wb2 = new ExcelJS.Workbook();
  const ws2 = wb2.addWorksheet('s');
  ws2.addRow([...HEADERS, '商品の重さ']);
  ws2.addRow(['x', 'x_長3封', 1, '茶封筒', '5g', '', '', '0.1cm', 2]);
  await wb2.xlsx.writeFile(dupHead);
  let msg2 = '';
  try { await importWeightFile(dupHead, { dryRun: true }); } catch (e) { msg2 = e.message; }
  t('同じ意味の列が2つあるファイルは弾く', () => { eq(/同じ意味の列/.test(msg2), true, msg2); });
})();

console.log('\n■ カバー率');
t('資材が未実測のうちは 1 通も確定しない', () => {
  const r0 = coverageReport({ since: '2026-06-01' });
  eq(r0.confirmed, 0, '推定寸法で定形110円を出さない');
  eq(r0.blockedMaterials.some((m) => m.material_code === 'chabuto'), true);
});

// 茶封筒を実測した、という状態にする
getDB().prepare('UPDATE pm_materials SET outer_length_mm=235, outer_width_mm=120, dims_verified=1 WHERE material_code=?').run('chabuto');
const rep = coverageReport({ since: '2026-06-01' });
t('定形外だけを対象にする (ネコポスは数えない)', () => { eq(rep.total, 8); });
t('茶封筒を実測したら 定形110円 で確定する', () => {
  eq(rep.confirmed, 2);
  eq(rep.bands.find((b) => b.band_code === 'teikei_50')?.count, 2);
  eq(rep.totalAmount, 220);
});
t('外寸未測定の資材は「不明」に落ちる', () => {
  eq(rep.reasons.some((r) => r.reason === 'missing_dims'), true);
});
t('「この資材を測ると何通動くか」が出る', () => {
  eq(rep.blockedMaterials.find((m) => m.material_code === 'shiropuchi')?.count, 1);
});
t('取り込まれなかった商品は「マスタに無い」として不足に出る', () => {
  const codes = rep.missingSkus.map((m) => m.sku_code);
  eq(codes.includes('sku-c'), true, '列ズレで見送った商品');
  eq(codes.includes('sku-g'), true, '重複で見送った商品');
  eq(codes.includes('sku-zzz'), true, '表に無い商品');
});
t('不足SKUが出現回数の多い順に並ぶ', () => {
  const c = rep.missingSkus.map((m) => m.count);
  eq(c.every((v, i) => i === 0 || c[i - 1] >= v), true, '降順');
});

console.log('\n■ 資材の外寸を入れると確定が増える');
getDB().prepare('UPDATE pm_materials SET outer_length_mm=250, outer_width_mm=180, dims_verified=1 WHERE material_code=?').run('shiropuchi');
const rep2 = coverageReport({ since: '2026-06-01' });
t('白プチの外寸を入れたら 1 通ぶん確定が増える', () => { eq(rep2.confirmed, rep.confirmed + 1); });
t('増えた分は規格内140円 (30 + 10 + 0.5 = 40.5g・厚さ20mm)', () => {
  eq(rep2.bands.find((b) => b.band_code === 'kikakunai_50')?.count, 1);
});

// 接続を閉じてから消す (Windows は開いたままだと WAL ファイルを消せない)。
// 消せなくてもテスト結果は変えない
closePostageDB();
try { fs.rmSync(TMP, { recursive: true, force: true }); }
catch { console.log(`  (一時ディレクトリを消せませんでした: ${TMP})`); }
console.log(`\n${fail === 0 ? '✅' : '❌'} ${pass} passed, ${fail} failed`);
process.exitCode = fail === 0 ? 0 : 1;
