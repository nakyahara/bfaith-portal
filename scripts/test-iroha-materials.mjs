import { temporaryTestRoot } from './test-temp-dir.mjs';
await temporaryTestRoot(import.meta.url);
/**
 * 資材を複数持てるようにした分のテスト (2026-10-01 中原さん: 10 個ずつ小分けする袋も記録したい)
 *
 * 実行: node scripts/test-iroha-materials.mjs
 *
 * 検証項目:
 *   1. 正規化と検証 (重複・表記ゆれ・使い道・小分けの数・件数の上限・壊れた入力)
 *   2. 保存の形 (キー順を固定・中身が同じなら同じ文字列 = version を無駄に進めない)
 *   3. 読み出しのフォールバック (未移行 = material_code / '[]' = 資材なし / 壊れた JSON は古い値へ)
 *   4. 取込で現場の 2 件目を消さない (空欄は触らない・値があれば 1 件目だけ差し替え)
 *   5. 古い画面からの material_code だけの更新で 2 件目を消さない
 *   6. 候補の集計は「その資材を使う商品の数」(同じ商品の中の重複は 1 回)
 *   7. 読み出し (service): materials が出る・未登録の数え方
 *   8. 権限: 資材なし→登録は誰でも / 入っているものを変える・足すのは職員
 */
import fs from 'fs';
import os from 'os';
import path from 'path';

if (!process.env.DATA_DIR) {
  process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'iroha-materials-test-'));
}

let pass = 0, fail = 0;
function ok(cond, label) {
  if (cond) { pass++; console.log(`  ✓ ${label}`); } else { fail++; console.log(`  ✗ ${label}`); }
}

const M = await import('../lib/iroha-materials.js');

console.log('[1] 正規化と検証');
{
  const r1 = M.canonicalizeMaterials(['D-8', 'ｄ－８', '313ビニール袋']);
  ok(r1.ok && r1.materials.length === 2 && r1.materials[0].code === 'D-8' && r1.materials[1].code === '313ビニール袋',
    '同じ資材 (全角・小文字違い) は 1 つにまとまる。表記は先に書いた方を残す');
  ok(r1.materials[0].usage === 'normal', '使い道の既定は normal');

  const r2 = M.canonicalizeMaterials([{ code: 'D-8' }, { code: '313ビニール袋', usage: 'inner_pack', units_per_pack: 10 }]);
  ok(r2.ok && r2.materials[1].usage === 'inner_pack' && r2.materials[1].units_per_pack === 10,
    '小分け袋は「何個ずつ」を持てる');
  const r3 = M.canonicalizeMaterials([{ code: 'D-8', usage: 'normal', units_per_pack: 10 }]);
  ok(r3.ok && r3.materials[0].units_per_pack === undefined,
    '🚨小分け袋でない資材に数は残さない (使い道を戻したのに「10個ずつ」が出たままにならない)');

  ok(M.canonicalizeMaterials([{ code: 'D-8', usage: 'xx' }]).error === 'bad_usage', '知らない使い道は拒否');
  for (const bad of [0, -1, 1.5, 'x', 100001]) {
    const r = M.canonicalizeMaterials([{ code: 'D-8', usage: 'inner_pack', units_per_pack: bad }]);
    ok(r.ok === false && r.error === 'bad_units_per_pack', `小分けの数が ${JSON.stringify(bad)} は拒否`);
  }
  ok(M.canonicalizeMaterials(['a', 'b', 'c', 'd']).error === 'too_many_materials', `資材は ${M.MAX_MATERIALS} つまで`);
  ok(M.canonicalizeMaterials(['x'.repeat(101)]).error === 'too_long', '長すぎる名前は拒否');
  ok(M.canonicalizeMaterials([{ code: { code: 'D-8' } }]).materials.length === 0,
    '🚨入れ子のオブジェクトは資材として受けない ([object Object] を画面に出さない)');
  ok(M.canonicalizeMaterials([]).materials.length === 0 && M.canonicalizeMaterials(null).materials.length === 0,
    '空は空 (エラーにしない)');
  ok(M.canonicalizeMaterials(['  D-8   x ']).materials[0].code === 'D-8 x', '前後の空白は落とし、連続空白は 1 つに');
  ok(M.canonicalizeMaterials('D-8').materials[0].code === 'D-8', '文字列 1 つでも受ける');
}

console.log('\n[2] 保存の形');
{
  const list = M.canonicalizeMaterials([{ code: 'D-8' }, { code: '袋', usage: 'inner_pack', units_per_pack: 10 }]).materials;
  const json = M.serializeMaterials(list);
  ok(json === '[{"code":"D-8"},{"code":"袋","usage":"inner_pack","units_per_pack":10}]',
    `キー順を固定し normal の使い道は書かない (${json})`);
  ok(M.serializeMaterials(M.parseMaterialsJson(json)) === json, '読み書きで形が変わらない');
  ok(M.sameMaterials(list, M.parseMaterialsJson(json)) === true, '中身が同じなら同じ扱い (version を無駄に進めない)');
  ok(M.sameMaterials([{ code: 'D-8', usage: 'normal' }], [{ code: 'D-8' }]) === true, 'normal の有無は同じ扱い');
  ok(M.sameMaterials([{ code: 'D-8' }], [{ code: 'D-9' }]) === false, '違う資材は違う扱い');
  ok(M.serializeMaterials([]) === '[]', '資材なしは [] (null にしない = 未移行と区別する)');
}

console.log('\n[3] 読み出しのフォールバック');
{
  ok(M.materialsOf({ materials_json: null, material_code: 'D-8' })[0].code === 'D-8',
    '未移行の行は material_code から 1 件にする');
  ok(M.materialsOf({ materials_json: '[]', material_code: 'D-8' }).length === 0,
    "🚨'[]' = 資材なし。material_code へ落とさない (人が消したのに戻らない)");
  ok(M.materialsOf({ materials_json: '{壊れた', material_code: 'D-8' })[0].code === 'D-8',
    '🚨壊れた JSON は古い material_code へ落とす (壊れた値で現場の資材を消さない)');
  ok(M.materialsOf({ materials_json: null, material_code: null }).length === 0, 'どちらも無ければ 0 件');
  ok(M.materialsOf(null).length === 0, '行が無くても落ちない');
  ok(M.materialsOf({ materials: [{ code: 'D-8' }] })[0].code === 'D-8', '配列で持っている形 (スナップショット) も読める');
  ok(M.materialsOf({ material_code: { code: 'D-8' } }).length === 0, '🚨入れ子の material_code は捨てる');
  ok(M.primaryMaterialCode(M.materialsOf({ materials_json: '[{"code":"A"},{"code":"B"}]' })) === 'A',
    '1 件目が主資材 (material_code に写る値)');
}

console.log('\n[4] 1 件目だけの差し替え (withPrimaryCode)');
{
  const cur = M.canonicalizeMaterials([{ code: 'D-8' }, { code: '袋', usage: 'inner_pack', units_per_pack: 10 }]).materials;
  const next = M.withPrimaryCode(cur, 'D-9');
  ok(next.length === 2 && next[0].code === 'D-9' && next[1].code === '袋' && next[1].units_per_pack === 10,
    '🚨1 件目を差し替えても 2 件目 (小分け袋) は残る');
  ok(M.withPrimaryCode(cur, '袋')[0].code === '袋' && M.withPrimaryCode(cur, '袋').length === 1,
    '2 件目と同じ資材を 1 件目にすると重複を作らずまとまる');
  ok(M.withPrimaryCode(cur, '')[0].code === '袋', '1 件目を空にすると 2 件目が繰り上がる');
  ok(M.withPrimaryCode([], 'D-8')[0].code === 'D-8' && M.withPrimaryCode([], 'D-8').length === 1, '0 件から 1 件');
}

console.log('\n[5] 未登録の数え方');
{
  ok(M.materialsMissing([]).join() === '資材', '資材が 0 件なら「資材」が足りない');
  ok(M.materialsMissing([{ code: 'D-8' }]).length === 0, '1 件あれば足りている (2 件目は任意なので数えない)');
  ok(M.materialsMissing([{ code: 'D-8' }, { code: '袋', usage: 'inner_pack' }]).join() === '小分け数',
    '小分け袋なのに「何個ずつ」が無ければ「小分け数」が足りない');
  ok(M.materialsMissing([{ code: '袋', usage: 'inner_pack', units_per_pack: 10 }]).length === 0, '数があれば足りている');
}

console.log('\n[6] 画面・通知の文字列');
{
  ok(M.materialsText([{ code: 'D-8' }, { code: '袋', usage: 'inner_pack', units_per_pack: 10 }]) === 'D-8 ＋ 袋 (10個ずつ)',
    '資材を並べて書く (10個ずつ も分かる)');
  ok(M.materialsText([{ code: '袋', usage: 'inner_pack' }]) === '袋 (小分け袋)', '数が未登録でも「小分け袋」と分かる');
  ok(M.materialsText([]) === '', '資材なしは空');
}

// ─── ここから DB を使う (取込・保存・候補・読み出し) ───
const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const { getDB } = await import('../apps/inbound-check/db.js');
const { applyWorkMaster, updateWorkMasterRow } = await import('../apps/inbound-check/work-master.js');
const db = getDB();
const insProd = db.prepare(`INSERT INTO mirror_products (product_id, 商品コード, 商品名, 商品区分, 取扱区分, 原価状態, updated_at)
  VALUES (?, ?, ?, '単品', '取扱中', '確定', '2026-10-01T00:00:00Z')`);
for (let i = 1; i <= 3; i++) insProd.run(i, `MAT-${i}`, `資材テスト商品${i}`);
const rowOf = (code) => db.prepare('SELECT * FROM f_iroha_work_master WHERE code_key = ?').get(code.toLowerCase());
const xlsxRow = (code, material, note = null) => ({
  code, codeKey: code.toLowerCase(), material, container: '20Lコンテナ', units: 180, processCount: 3, note,
});

console.log('\n[7] 保存 (materials が正本・material_code は写し)');
{
  applyWorkMaster([xlsxRow('MAT-1', 'D-8')], { user: 'test' });
  const v1 = rowOf('MAT-1').version;
  const r = updateWorkMasterRow('MAT-1', {
    materials: [{ code: 'D-8' }, { code: '313ビニール袋', usage: 'inner_pack', units_per_pack: 10 }],
  }, 'test', v1);
  const row = rowOf('MAT-1');
  ok(r.ok && M.materialsOf(row).length === 2, '2 件目 (小分け袋) を足せる');
  ok(row.material_code === 'D-8', '🚨material_code は 1 件目の写し (古い画面・外部委託がこれを読む)');
  ok(M.materialsOf(row)[1].units_per_pack === 10, '何個ずつ かも残る');

  const same = updateWorkMasterRow('MAT-1', {
    materials: [{ code: 'D-8' }, { code: '313ビニール袋', usage: 'inner_pack', units_per_pack: 10 }],
  }, 'test', row.version);
  ok(same.ok && same.unchanged === true && rowOf('MAT-1').version === row.version,
    '🚨同じ資材を選び直しただけなら version を進めない (エラーにもしない)');

  const bad = updateWorkMasterRow('MAT-1', { materials: [{ code: 'D-8', usage: 'inner_pack', units_per_pack: 0 }] }, 'test', row.version);
  ok(bad.ok === false && bad.error === 'bad_units_per_pack', '数がおかしければ保存しない');

  const old = updateWorkMasterRow('MAT-1', { material_code: 'D-9' }, 'test', rowOf('MAT-1').version);
  const row2 = rowOf('MAT-1');
  ok(old.ok && M.materialsOf(row2).length === 2 && M.materialsOf(row2)[0].code === 'D-9' && M.materialsOf(row2)[1].code === '313ビニール袋',
    '🚨古い画面が material_code だけ送っても 2 件目は消えない');
  ok(row2.material_code === 'D-9', '写しも一緒に変わる');

  const cleared = updateWorkMasterRow('MAT-1', { materials: [] }, 'test', row2.version);
  ok(cleared.ok && rowOf('MAT-1').materials_json === '[]' && rowOf('MAT-1').material_code === null,
    '資材なしにできる (materials_json は [] ・写しは空)');
}

console.log('\n[8] 取込で現場の登録を消さない');
{
  // 🚨取込は全置換 (xlsx に無い行は消える) なので、毎回ぜんぶの行を渡す。ここで MAT-1 は消える
  const sheet = (m2, note2 = null) => [xlsxRow('MAT-2', m2, note2), xlsxRow('MAT-3', 'D-9')];
  applyWorkMaster(sheet('D-8'), { user: 'test' });
  updateWorkMasterRow('MAT-2', {
    materials: [{ code: 'D-8' }, { code: '313ビニール袋', usage: 'inner_pack', units_per_pack: 10 }],
  }, 'test', rowOf('MAT-2').version);

  const c1 = applyWorkMaster(sheet('D-8'), { user: 'test' });
  ok(c1.unchanged === 2 && M.materialsOf(rowOf('MAT-2')).length === 2, '同じ資材の再取込では何も変わらない');

  const c2 = applyWorkMaster(sheet('D-9'), { user: 'test' });
  const after = M.materialsOf(rowOf('MAT-2'));
  ok(c2.updated === 1 && after.length === 2 && after[0].code === 'D-9' && after[1].units_per_pack === 10,
    '🚨xlsx の「資材」は 1 件目だけを差し替える (iPad で足した小分け袋は残る)');

  const c3 = applyWorkMaster(sheet(null, 'メモ'), { user: 'test' });
  const after3 = M.materialsOf(rowOf('MAT-2'));
  ok(c3.updated === 1 && after3.length === 2 && after3[0].code === 'D-9',
    '🚨xlsx の資材が空欄なら資材は触らない (空欄で現場の登録を消さない)');
  ok(rowOf('MAT-2').note === 'メモ', '資材以外はこれまでどおり取込で更新される');
  ok(c3.materials_kept >= 1, '2 件以上の資材を守った件数を返す');
}

console.log('\n[9] 候補の集計 (その資材を使う商品の数)');
{
  const { seedWorkOptionsFromMaster, listWorkOptions, _resetSeedFingerprint } = await import('../apps/iroha-work/db.js');
  // MAT-3 は [8] の取込で D-9 が入っている。同じ資材を 2 回 (表記ゆれ込みで) 選んでも 1 件にまとまる
  updateWorkMasterRow('MAT-3', { materials: [{ code: 'D-9' }, { code: 'ｄ－９' }] }, 'test', rowOf('MAT-3').version);
  ok(M.materialsOf(rowOf('MAT-3')).length === 1, '同じ資材を 2 回選んでも 1 件にまとまる');
  _resetSeedFingerprint();
  seedWorkOptionsFromMaster({ force: true });
  const d9 = listWorkOptions('material', true).filter((o) => M.normalizeOptionCode(o.code) === 'D-9');
  ok(d9.length === 1, 'D-9 の候補は 1 つ');
  // MAT-2 (1 件目が D-9) と MAT-3 (D-9) の 2 商品 → sort_order = -2 (使用回数の負数)
  ok(d9[0].sort_order === -2, `使う商品の数で並ぶ (同じ商品の中の重複は 1 回 — 実際 ${d9[0].sort_order})`);
  const fukuro = listWorkOptions('material', true).filter((o) => o.code === '313ビニール袋');
  ok(fukuro.length === 1, '🚨2 件目の資材 (小分け袋) も候補に出る');
}

console.log('\n[10] 読み出し (service) と権限');
{
  const SV = await import('../apps/iroha-work/service.js');
  const wm = rowOf('MAT-2');
  const m = SV.masterOfTask(wm, null);
  ok(m.materials.length === 2 && m.material_code === m.materials[0].code, 'カードの作業仕様に materials が出る');
  ok(m.missing.includes('資材') === false, '資材があれば足りない扱いにしない');

  const noMat = SV.masterOfTask({ ...wm, materials_json: '[]', material_code: null }, null);
  ok(noMat.missing.includes('資材'), '資材なしは「資材」が足りない');
  const noPer = SV.masterOfTask({ ...wm, materials_json: '[{"code":"袋","usage":"inner_pack"}]' }, null);
  ok(noPer.missing.includes('小分け数'), '小分け袋で数が無ければ「小分け数」が足りない');

  // カード (作成時スナップショット) 側の資材へのフォールバック
  const fromCard = SV.masterOfTask({ version: 3, material_code: null, materials_json: null },
    { materials: [{ code: 'D-8' }], units_per_container: 10 });
  ok(fromCard.materials[0].code === 'D-8', 'マスタが空ならカードの資材を出す (表示を消さない)');

  // 権限: 空欄を埋めるのは誰でも / 入っているものを変える・足すのは職員
  const fill = SV.classifyMasterEdit({ materials_json: '[]' }, { materials: [{ code: 'D-8' }] });
  ok(fill.fills.includes('materials') && fill.overwrites.length === 0, '資材なし → 登録するのは空欄埋め (誰でも)');
  const add = SV.classifyMasterEdit({ materials_json: '[{"code":"D-8"}]' }, { materials: [{ code: 'D-8' }, { code: '袋' }] });
  ok(add.overwrites.includes('materials'), '🚨入っている資材に 2 件目を足すのは「変更」(職員のみ)');
  const nochange = SV.classifyMasterEdit({ materials_json: '[{"code":"D-8"}]' }, { materials: ['D-8'] });
  ok(nochange.fills.length === 0 && nochange.overwrites.length === 0, '同じ資材なら変更なし');
}

console.log(`\n結果: ${pass} PASS / ${fail} FAIL`);
process.exit(fail === 0 ? 0 : 1);
