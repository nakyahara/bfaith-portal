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
  // 重複 (内容が違う) — どちらが正か決まるまで **先頭行も含めて** 取り込まない。
  // この行の「資材の厚み 0.9cm」は見送る SKU の行なので、茶封筒の厚みの根拠に使ってはいけない
  ws.addRow(['sku-g', 'テスト商品G_長3封', 20, '茶封筒', '5g', '', '0.9cm', '0.4cm']);
  ws.addRow(['sku-g', 'テスト商品G_長3封', 99, '茶封筒', '5g', '', '0.9cm', '0.4cm']);
  // 重さ 0 → 入力ミスとして丸ごと見送り
  ws.addRow(['sku-h', 'テスト商品H_長3封', 0, '茶封筒', '5g', '', '', '0.1cm']);
  // 資材の厚みだけ入っていて商品の厚みが空 (2026-09-05 の表で 26 件) → 列ズレではない。商品の厚みは未登録のまま取り込む
  ws.addRow(['sku-i', 'テスト商品I_長3封', 23, '茶封筒', '5g', '2g', '0.1cm', '']);
  // 資材の厚みが資材ごとに揃っている → 茶封筒はマスタが空なら 0.1cm (=1mm) で埋まる
  ws.addRow(['sku-j', 'テスト商品J_長3封', 8, '茶封筒', '5g', '2g', '0.1cm', '0.5cm']);
  // 白プチは表の中で 0.2cm と 0.1cm が混ざる → 揃っていないので埋めない (人に返す)
  ws.addRow(['sku-k', 'テスト商品K_梱機プ', 28, '白プチ', '10g', '2g', '0.2cm', '1cm']);
  ws.addRow(['sku-l', 'テスト商品L_梱機プ', 29, '白プチ', '10g', '2g', '0.1cm', '1cm']);
  await wb.xlsx.writeFile(file);
}

// ── packing-dispatch の出力履歴 fixture (Render で warehouse.db が無いときの実績) ──
function makeMirror(file) {
  const d = new Database(file);
  d.exec(`CREATE TABLE pd_shipment_tracking (
    ne_uketsuke_no TEXT PRIMARY KEY, shipping_method_code TEXT NOT NULL, shop_name TEXT, order_no TEXT,
    product_items_json TEXT, exported_at TEXT)`);
  const ins = d.prepare('INSERT INTO pd_shipment_tracking VALUES (?,?,?,?,?,?)');
  const items = (arr) => JSON.stringify(arr.map(([product_code, qty, product_name]) => ({ product_code, product_name: product_name || product_code, qty })));
  d.transaction(() => {
    // JST 6/1 10:00 に出力 (UTC 01:00)。商品コードは大文字混じりでも小文字に寄せる
    ins.run('M1', 'teikeigai', '楽天', 'r-1', items([['SKU-A', 1, 'ミラー商品A']]), '2026-06-01T01:00:00.000Z');
    ins.run('M2', 'letterpack', '楽天', 'r-2', items([['sku-a', 1]]), '2026-06-01T01:00:00.000Z');   // 定形外ではない
    ins.run('M3', 'teikeigai', 'Yahoo', 'y-3', items([['sku-zzz', 1, '表に無い商品']]), '2026-06-02T01:00:00.000Z');
    ins.run('M4', 'teikeigai', 'Yahoo', 'y-4', '{broken', '2026-06-02T01:00:00.000Z');              // 壊れた JSON
    ins.run('M5', 'teikeigai', 'Yahoo', 'y-5', items([['sku-a', 1]]), '2026-05-31T14:59:59.000Z');  // JST 5/31 23:59 → 期間外
    ins.run('M6', ' Teikeigai ', 'Yahoo', 'y-6', items([['sku-a', 1, 'ミラー商品A']]), '2026-06-02T01:00:00.000Z'); // 表記ゆれでも定形外
  })();
  d.close();
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
makeMirror(path.join(TMP, 'warehouse-mirror.db'));
const xlsx = path.join(TMP, 'weights.xlsx');
await makeXlsx(xlsx);

const { initPostageDB, getDB, closePostageDB, getTariffVersionFor } = await import('./db.js');
const { importWeightFile } = await import('./import.js');
const { coverageReport, lookupProductName, availableSources } = await import('./coverage.js');
const { lookupProductNameFromMirror, readCompositions } = await import('./composition.js');
initPostageDB();

console.log('■ 既存DBの移行');
t('pm_materials に thickness_mm 列がある (ALTER で足される)', () => {
  const cols = getDB().prepare('PRAGMA table_info(pm_materials)').all().map((c) => c.name);
  eq(cols.includes('thickness_mm'), true, cols.join(','));
});
t('古い pm_materials (列なし) を開き直しても壊れない', () => {
  const db = getDB();
  db.exec('ALTER TABLE pm_materials DROP COLUMN thickness_mm');
  initPostageDB();
  const cols = getDB().prepare('PRAGMA table_info(pm_materials)').all().map((c) => c.name);
  eq(cols.includes('thickness_mm'), true);
  throws(() => getDB().prepare("UPDATE pm_materials SET thickness_mm=0 WHERE material_code='chabuto'").run(), /CHECK/, '0 は入らない');
});
t('判定ログの表がある', () => {
  eq(getDB().prepare("SELECT COUNT(*) n FROM sqlite_master WHERE name='pm_print_decisions'").get().n, 1);
});

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
// fixture は数行しか無いので、資材の厚みの自動登録に要る行数を 2 に下げる (本番既定は 10)
const dry = await importWeightFile(xlsx, { dryRun: true, actor: 'smoke', materialThicknessMinRows: 2 });
t('dry-run では pm_skus に入らない', () => {
  eq(getDB().prepare('SELECT COUNT(*) n FROM pm_skus').get().n, 0);
});
t('列ズレを検出する (送り状の重さに cm)', () => {
  const shifts = dry.issues.filter((i) => i.kind === 'column_shift');
  eq(shifts.length >= 1, true, `column_shift が ${shifts.length} 件`);
  eq(shifts.some((i) => i.column_name === '送り状の重さ' && i.sku_code === 'sku-c'), true);
});
t('資材の厚みだけ入っている行は列ズレにしない (sku-i は取り込む)', () => {
  eq(dry.issues.some((i) => i.sku_code === 'sku-i' && i.severity === 'error'), false,
    JSON.stringify(dry.issues.filter((i) => i.sku_code === 'sku-i')));
  eq(dry.issues.some((i) => i.kind === 'column_shift' && i.column_name === '資材の厚み'), false);
});
t('資材の厚みが表の中で揃っていれば「入れます」、混ざっていれば「手で入れて」と出る', () => {
  const fill = dry.issues.find((i) => i.kind === 'material_thickness_fill');
  eq(!!fill && /茶封筒/.test(fill.message) && /1mm/.test(fill.message), true, fill?.message);
  const amb = dry.issues.find((i) => i.kind === 'material_thickness_ambiguous');
  eq(!!amb && /白プチ/.test(amb.message), true, amb?.message);
});
t('dry-run では資材の厚みも入らない', () => {
  eq(getDB().prepare("SELECT thickness_mm t FROM pm_materials WHERE material_code='chabuto'").get().t, null);
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

t('行数が足りない資材は自動で入れず「手で入れて」と出る (本番既定 10 行)', async () => {
  const d = await importWeightFile(xlsx, { dryRun: true, actor: 'smoke' });
  eq(d.issues.some((i) => i.kind === 'material_thickness_fill'), false);
  const c = d.issues.find((i) => i.kind === 'material_thickness_candidate' && /茶封筒/.test(i.message));
  eq(!!c && /10行以上/.test(c.message), true, c?.message);
});

console.log('\n■ 取込 (反映)');
const applied = await importWeightFile(xlsx, { dryRun: false, actor: 'smoke', materialThicknessMinRows: 2 });
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
t('資材の厚みだけの行は商品の厚み NULL で入る', () => {
  const r = getDB().prepare("SELECT unit_weight_g w, thickness_mm t FROM pm_skus WHERE sku_code='sku-i'").get();
  eq(r.w, 23); eq(r.t, null);
});
t('資材マスタの厚みが空なら、表で揃っている値を入れる (茶封筒 0.1cm → 1mm)', () => {
  eq(getDB().prepare("SELECT thickness_mm t FROM pm_materials WHERE material_code='chabuto'").get().t, 1);
  eq(applied.summary.materialThicknessFilled, 1);
});
t('表の中で揃っていない資材は入れない (白プチ)', () => {
  eq(getDB().prepare("SELECT thickness_mm t FROM pm_materials WHERE material_code='shiropuchi'").get().t, null);
});
t('見送った SKU の行 (茶封筒 0.9cm) は厚みの根拠に使われない', () => {
  // 使われていたら 1mm と 9mm で「揃っていない」になり、1mm は入らなかったはず
  eq(applied.issues.some((i) => i.kind === 'material_thickness_ambiguous' && /茶封筒/.test(i.message)), false);
});
t('人が入れた資材の厚みは表で上書きしない', async () => {
  getDB().prepare("UPDATE pm_materials SET thickness_mm=1.5 WHERE material_code='chabuto'").run();
  const again = await importWeightFile(xlsx, { dryRun: false, actor: 'smoke', materialThicknessMinRows: 2 });
  eq(getDB().prepare("SELECT thickness_mm t FROM pm_materials WHERE material_code='chabuto'").get().t, 1.5);
  eq(again.issues.some((i) => i.kind === 'material_thickness_mismatch' && /茶封筒/.test(i.message)), true, '食い違いは注意として出る');
  getDB().prepare("UPDATE pm_materials SET thickness_mm=1 WHERE material_code='chabuto'").run();
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
t('資材の厚みが無い資材は「不明」に落ちる (白プチは表の中で揃わず未登録のまま)', () => {
  eq(rep.reasons.some((r) => r.reason === 'missing_material_thickness'), true, JSON.stringify(rep.reasons));
});
t('「この資材の厚みを入れると何通動くか」が出る', () => {
  const m = rep.blockedMaterials.find((m) => m.material_code === 'shiropuchi');
  eq(m?.count, 1); eq(m?.need, '厚み');
});
t('厚みを入れたら次は外寸で止まる (missing_dims・「外寸を入れると」)', () => {
  getDB().prepare('UPDATE pm_materials SET thickness_mm=2 WHERE material_code=?').run('shiropuchi');
  const r = coverageReport({ since: '2026-06-01' });
  eq(r.reasons.some((x) => x.reason === 'missing_dims'), true, JSON.stringify(r.reasons));
  eq(r.blockedMaterials.find((m) => m.material_code === 'shiropuchi')?.need, '外寸');
  getDB().prepare('UPDATE pm_materials SET thickness_mm=NULL WHERE material_code=?').run('shiropuchi');
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

console.log('\n■ 資材の外寸・厚みを入れると確定が増える');
getDB().prepare('UPDATE pm_materials SET outer_length_mm=250, outer_width_mm=180, dims_verified=1 WHERE material_code=?').run('shiropuchi');
const repNoTh = coverageReport({ since: '2026-06-01' });
t('外寸を入れても資材の厚みが空なら確定しない (「厚みを入れると N 通」と出る)', () => {
  eq(repNoTh.confirmed, rep.confirmed);
  eq(repNoTh.blockedMaterials.find((m) => m.material_code === 'shiropuchi')?.need, '厚み');
});
getDB().prepare('UPDATE pm_materials SET thickness_mm=2 WHERE material_code=?').run('shiropuchi');
const rep2 = coverageReport({ since: '2026-06-01' });
t('白プチの外寸と厚みを入れたら 1 通ぶん確定が増える', () => { eq(rep2.confirmed, rep.confirmed + 1); });
t('増えた分は規格内140円 (30 + 10 + 0.5 = 40.5g・厚さ 20+2 = 22mm)', () => {
  eq(rep2.bands.find((b) => b.band_code === 'kikakunai_50')?.count, 1);
});

console.log('\n■ packing-dispatch の出力履歴を実績にする (Render)');
t('読める出どころが両方並ぶ (warehouse が先)', () => { eq(availableSources(), ['warehouse', 'packing-dispatch']); });
const repM = coverageReport({ since: '2026-06-01', source: 'packing-dispatch' });
t('定形外 (teikeigai・表記ゆれ含む) だけ数える・期間外 (JST 5/31) は入らない', () => {
  eq(repM.source, 'packing-dispatch'); eq(repM.total, 4, JSON.stringify(repM.byDate));
});
t('商品コードは小文字に寄せて突き合わせる (SKU-A → sku-a で定形110円)', () => {
  eq(repM.confirmed, 2); eq(repM.bands[0].band_code, 'teikei_50');
});
t('壊れた JSON は「構成が壊れている」→ 不明 (黙って確定しない)', () => {
  eq(repM.reasons.some((r) => r.reason === 'broken_composition'), true, JSON.stringify(repM.reasons));
});
t('出荷日は出力日の JST', () => { eq(repM.byDate.map((d) => d.date).sort(), ['2026-06-01', '2026-06-02']); });
t('商品名は出力履歴からも引ける', () => {
  eq(lookupProductNameFromMirror('SKU-A'), 'ミラー商品A');
  eq(lookupProductName('sku-a'), 'sku-a', 'warehouse があればそちらが先');
});
t('伝票番号で構成を引ける (無い伝票は Map に入らない)', () => {
  const m = readCompositions(['M1', ' M2 ', 'nope', 'M4', 'M6']);
  eq([...m.keys()].sort(), ['M1', 'M2', 'M4', 'M6']);
  eq(m.get('M1').lines, [{ sku_code: 'sku-a', qty: 1, product_name: 'ミラー商品A' }]);
  eq(m.get('M1').broken, false);
  eq(m.get('M2').method_code, 'letterpack');
  eq(m.get('M4').broken, true); eq(m.get('M4').lines, []);
  eq(m.get('M6').method_code, 'teikeigai', '配送方法コードは trim + 小文字に寄せる');
});
t('1 明細でも読めなければ構成全体を壊れている扱い (読めた分だけで判定しない)', async () => {
  const { parseItems } = await import('./composition.js');
  eq(parseItems(JSON.stringify([{ product_code: 'sku-a', qty: 1 }, { qty: 1 }])).broken, true, '商品コード欠け');
  eq(parseItems(JSON.stringify([{ product_code: 'sku-a', qty: true }])).broken, true, '数量が真偽値');
  eq(parseItems(JSON.stringify([{ product_code: 'sku-a', qty: '2' }])).broken, true, '数量が文字列');
  eq(parseItems(JSON.stringify([{ product_code: 'sku-a', qty: 2 }, 'x'])).broken, true, '要素が文字列');
  eq(parseItems('{"a":1}').broken, true, '配列でない');
  eq(parseItems(JSON.stringify([{ product_code: ' SKU-A ', qty: 2 }])), { lines: [{ sku_code: 'sku-a', qty: 2, product_name: null }], broken: false });
  eq(parseItems('[]'), { lines: [], broken: false }, '空配列は壊れていない (明細なし → no_lines)');
});
t('存在しない出どころを指定したら auto 扱い', () => {
  eq(coverageReport({ since: '2026-06-01', source: 'nope' }).available, true);
});
t('warehouse.db が壊れていれば (ファイルはある) packing-dispatch に自動で切り替わる', () => {
  const whPath = process.env.POSTAGE_WAREHOUSE_DB;
  const backup = fs.readFileSync(whPath);
  fs.writeFileSync(whPath, 'this is not a sqlite file');
  try {
    eq(availableSources(), ['packing-dispatch']);
    const r = coverageReport({ since: '2026-06-01' });
    eq(r.available, true); eq(r.source, 'packing-dispatch');
    eq(lookupProductName('sku-a'), 'ミラー商品A', '商品名も packing-dispatch から');
  } finally {
    fs.writeFileSync(whPath, backup);
  }
  eq(availableSources()[0], 'warehouse', '戻せば warehouse が先に戻る');
});

// 接続を閉じてから消す (Windows は開いたままだと WAL ファイルを消せない)。
// 消せなくてもテスト結果は変えない
closePostageDB();
try { fs.rmSync(TMP, { recursive: true, force: true }); }
catch { console.log(`  (一時ディレクトリを消せませんでした: ${TMP})`); }
console.log(`\n${fail === 0 ? '✅' : '❌'} ${pass} passed, ${fail} failed`);
process.exitCode = fail === 0 ? 0 : 1;
