import { temporaryTestDataDir } from './test-temp-dir.mjs';
import { compositionFor } from './fixtures/lp-compose/index.mjs';
await temporaryTestDataDir(import.meta.url, 'test-designer-sheet-');
/**
 * デザイナー修正依頼書 (スプレッドシート) — 画像制作の新フロー PR-F (2026-10-09・スタッフ要望 ⑥)
 * 実行: node scripts/test-ph-designer-sheet.mjs
 *
 *   ① lib/designer-sheet.js … 中身を組む・修正指示を読み戻す (純粋関数。入力 → 行)
 *   ② 本番の経路 (router を express に載せて POST /api/drafts/:id/designer-sheet と詳細画面) — Google は偽物
 *      (Drive のファイル・権限 / Sheets のタブ・セル・印を持つ)。権限は管理者ではなく実際に押す役割 (画像登録者) でも
 *   ③ 画面の JS (detail.ejs の @designer-sheet の間を切り出し、偽の document で連続操作。送り先は本番の router)
 * 実際の Google には繋がない
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

process.env.PH_LP_COMPOSE_ENABLED = '1';
process.env.PH_LP_COMPOSE_DAILY_CAP = '200';
const ENV = { PH_LP_IMAGE_ENABLED: '1', OPENAI_LP_IMAGE_API_KEY: 'sk-test-lp-image', PH_LP_MONTHLY_BUDGET_JPY: '3000' };
for (const [k, v] of Object.entries(ENV)) process.env[k] = v;
delete process.env.GOOGLE_SERVICE_ACCOUNT_KEY;

const { initMirrorDB } = await import('../apps/warehouse-mirror/db.js');
initMirrorDB();
const dbmod = await import('../apps/product-hub/db.js');
const db = dbmod.initProductHubDB();
const lp = await import('../apps/product-hub/lib/lp-compose.js');
const li = await import('../apps/product-hub/lib/lp-image.js');
const ds = await import('../apps/product-hub/lib/designer-sheet.js');
const svc = await import('../apps/product-hub/services/designer-sheet-service.js');
const sw = await import('../apps/product-hub/services/sheets-writer.js');

const HERE = path.dirname(fileURLToPath(import.meta.url));
let pass = 0; let fail = 0;
const ok = (c, l, d = '') => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}${d ? ' — ' + String(d).slice(0, 600) : ''}`); } };
const eq = (a, b, l) => ok(JSON.stringify(a) === JSON.stringify(b), l, `期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)}`);
const throws = (l, fn, want) => { try { fn(); fail++; console.log(`  ✗ ${l} — 例外が出なかった`); } catch (e) { ok(!want || String(e.message).includes(want), l, e.message); } };

const FID = '1MtcKdnRZPf1iqKiNxMJ1ODDPJE3vX9JR';
const FOLDER = `https://drive.google.com/drive/folders/${FID}`;
const isFormula = (c) => !!c && typeof c === 'object' && typeof c.__formula === 'string';

// ════════════════════════════════════════════════════════════════
console.log('① 中身を組む (lib/designer-sheet.js)');
// ════════════════════════════════════════════════════════════════
const IMG = (root, cur, no, ver = 1, extra = {}) => ({ root_id: root, current_id: cur, no, seq: no + 1, role: `役割${no}`, title: `見出し${no}`, version: ver, drive_file_id: `DRVFILE${String(cur).padStart(6, '0')}`, ...extra });
{
  eq(ds.designerSheetTitle('maitakep50'), 'デザイナー修正依頼書_maitakep50', 'ファイル名 = デザイナー修正依頼書_<商品コード>');
  eq(ds.designerSheetTitle('a/b:c\n'), 'デザイナー修正依頼書_a／b_c', 'ファイル名に使えない文字は寄せる (撮影指示書と同じ)');
  eq(ds.designerSheetTitle(''), 'デザイナー修正依頼書_(商品コードなし)', '商品コードが無いとき');
  eq(ds.imageFormulaUrl('DRVFILE000001'), 'https://lh3.googleusercontent.com/d/DRVFILE000001', '=IMAGE の URL は drive.google.com 以外 (IMAGE 関数のヘルプの決まり) の lh3.googleusercontent.com/d/<ID>');
  ok(!/drive\.google\.com/.test(ds.imageFormulaUrl('DRVFILE000001')), '🚨 =IMAGE の URL に drive.google.com を使わない (uc?export=view は 403・ヘルプでも不可)');
  throws('🚨 ファイル ID の形が違えば式を組まない (引用符で式を壊させない)', () => ds.imageFormulaUrl('abc")&IMPORTXML("x'), '不正');
  throws('閲覧画面の URL も同じ検査', () => ds.driveViewUrl('short'), '不正');
  eq([ds.imageLabel({ no: 0 }), ds.imageLabel({ no: 3 }), ds.imageLabel({ no: null, seq: 2 })], ['0枚目 (TOP)', '3枚目', '2番目'], '画像番号 (画面の画像カードと同じ)');

  // 作れない理由
  const card = (o = {}) => ({ root_id: 1, seq: 1, no: 0, pending: false, current: { id: 1, drive_file_id: 'DRVFILE000001', version: 1 }, ...o });
  ok(/GOOGLE_SERVICE_ACCOUNT_KEY/.test(ds.designerSheetBlockReason({ configured: false, folderId: 'F', job: { status: 'done' }, cards: [card()] })), 'サービスアカウントが無い');
  ok(/画像フォルダ/.test(ds.designerSheetBlockReason({ configured: true, folderId: null, job: { status: 'done' }, cards: [card()] })), '画像フォルダが無い');
  ok(/先に「AI画像生成」/.test(ds.designerSheetBlockReason({ configured: true, folderId: 'F', job: null, cards: [] })), '画像がまだ無い');
  ok(/いま画像を作っています/.test(ds.designerSheetBlockReason({ configured: true, folderId: 'F', job: { status: 'running' }, cards: [card()] })), '🚨 全部作っている途中は作れない (全部の画像ができてから)');
  ok(/作り直している/.test(ds.designerSheetBlockReason({ configured: true, folderId: 'F', job: { status: 'done' }, cards: [card(), card({ root_id: 2, pending: true })] })), '🚨 1 枚作り直している途中も作れない');
  const why = ds.designerSheetBlockReason({ configured: true, folderId: 'F', job: { status: 'partial' }, cards: [card(), card({ root_id: 2, no: 2, current: null })] });
  ok(/作れなかった画像があります \(2枚目\)/.test(why || ''), `🚨 できていない画像があれば作れない・どれかを出す (${why})`);
  eq(ds.designerSheetBlockReason({ configured: true, folderId: 'F', job: { status: 'partial' }, cards: [card(), card({ root_id: 2, no: 2 })] }), null, '全部作るが一部失敗でも、作り直して全部できていれば作れる');

  // 作ったばかり
  const b = ds.buildDesignerSheet({ productCode: 'CODE1', productName: '商品', images: [IMG(10, 10, 0), IMG(11, 11, 1), IMG(12, 12, 2)] });
  const t = b.tabs[0];
  ok(b.title === 'デザイナー修正依頼書_CODE1' && b.tabs.length === 1 && t.name === ds.DESIGNER_TAB && t.name !== '撮影依頼書' && t.name !== '依頼文', 'タブは 1 枚 (撮影指示書のタブ名と別)');
  eq(t.rows[2], ds.HEADERS, '3 行目が見出し (画像番号・役割 / AI生成画像 / 修正指示 / 版 / 画像を開く / 管理番号)');
  ok(t.rows[2][0] === '画像番号・役割' && t.rows[2][1] === 'AI生成画像' && t.rows[2][2] === '修正指示' && t.rows[2][3] === '版', '列の並びはラフどおり (画像番号・役割 / AI生成画像 / 修正指示) + 版');
  const imgRows = t.rows.slice(3);
  eq(imgRows.map((r) => r[0]), ['0枚目 (TOP)｜役割0\n見出し0', '1枚目｜役割1\n見出し1', '2枚目｜役割2\n見出し2'], 'TOP から順に 1 画像 1 行・番号と役割・見出し');
  ok(imgRows.every((r, i) => isFormula(r[1]) && r[1].__formula === `=IMAGE("https://lh3.googleusercontent.com/d/DRVFILE${String(10 + i).padStart(6, '0')}")`), '画像の列は =IMAGE("…") の式 (最新の版のファイル)');
  ok(imgRows.every((r) => r[2] === ''), '修正指示は空欄');
  eq(imgRows.map((r) => r[3]), ['v1', 'v1', 'v1'], '版');
  ok(imgRows.every((r, i) => isFormula(r[4]) && r[4].__formula === `=HYPERLINK("https://drive.google.com/file/d/DRVFILE${String(10 + i).padStart(6, '0')}/view", "画像を開く")`), '画像が出ないときの逃げ道 (Drive の閲覧画面へのリンク)');
  eq(imgRows.map((r) => r[5]), ['img-10', 'img-11', 'img-12'], '管理番号 = 画像の元の行の ID (作り直しても変わらない)');
  ok(t.rows.flat().filter((c) => !isFormula(c)).every((c) => typeof c === 'string'), '式のセル以外は全部文字 (stringValue で書く)');
  eq(t.format.rowHeights, [{ start: 3, end: 6, px: ds.IMAGE_ROW_PX }], '画像の行だけ高くする');
  ok(t.format.frozenRows === 3 && t.format.boldRows.includes(2) && t.format.columnWidths.length === 6, '見出しまでを固定・太字・6 列の幅');
  eq([b.carried, b.orphaned], [0, 0], '作ったばかりは戻す修正指示なし');
  const evil = ds.buildDesignerSheet({ productCode: 'C', productName: 'n', images: [IMG(1, 1, 1, 1, { role: '=IMPORTXML("http://x","//a")', title: '=HYPERLINK("http://evil")' })] });
  ok(!isFormula(evil.tabs[0].rows[3][0]) && evil.tabs[0].rows[3][0].includes('=IMPORTXML'), '🚨 AI の構成の文字 (= で始まっても) は文字のセル (式にしない)');

  // 読み戻し: シートに見えている値 (式の結果: IMAGE は空・HYPERLINK は「画像を開く」)
  const shown = (rows) => rows.map((r) => r.map((c) => (isFormula(c) ? (c.__formula.startsWith('=HYPERLINK') ? '画像を開く' : '') : c)));
  const vals = shown(t.rows);
  vals[3][2] = 'TOP の文字を大きく';
  vals[5][2] = '背景を白に\n影を薄く';
  let back = ds.readBackNotes(vals);
  ok(back.ok && back.byKey.size === 2 && back.byKey.get(10).note === 'TOP の文字を大きく' && back.byKey.get(12).note === '背景を白に\n影を薄く' && back.orphans.length === 0,
    '人が書いた修正指示を管理番号ごとに読み戻す (空欄は読まない・改行もそのまま)');
  // 人が列を足した (修正指示の左に「担当」)・管理番号の列を動かした → 見出しの文字で列を探す
  const moved = vals.map((r, i) => (i < 2 ? r : [r[0], r[1], i === 2 ? '担当' : '田中', ...r.slice(2)]));
  back = ds.readBackNotes(moved);
  ok(back.ok && back.byKey.get(10)?.note === 'TOP の文字を大きく' && back.byKey.get(12)?.note.startsWith('背景を白に'), '人が列を足しても、見出しの文字で修正指示・管理番号の列を探す');
  // 同じ管理番号の行が 2 つ (人が行をコピー)・管理番号を消した行 → 行き先なし (消さない)
  const dup = vals.map((r) => r.slice());
  dup.push(['1枚目の写し', '', 'コピーした行の指示', 'v1', '', 'img-10']);
  dup.push(['人が足した行', '', '全体に明るく', '', '', '']);
  back = ds.readBackNotes(dup);
  ok(back.byKey.get(10).note === 'TOP の文字を大きく' && back.orphans.map((o) => o.note).join('|') === 'コピーした行の指示|全体に明るく', '🚨 同じ管理番号の 2 行目・管理番号の無い行の修正指示は「行き先なし」で残す (黙って捨てない)', JSON.stringify(back.orphans));
  // 見出しが消えた
  const noHead = vals.filter((_, i) => i !== 2);
  back = ds.readBackNotes(noHead);
  ok(!back.ok && /見出しの行が見つかりません/.test(back.error), '🚨 見出しの行が無い (どこが修正指示か分からない) シートは読めない = 上書きしない');
  const above = [vals[0], vals[1], vals[3], vals[2], vals[4], vals[5]];
  back = ds.readBackNotes(above);
  ok(back.ok && back.byKey.get(10)?.note === 'TOP の文字を大きく', '🚨 画像の行を見出しより上へ動かしても、修正指示を読み落とさない (Codex 名指し5 高)', JSON.stringify([...back.byKey]));
  const fakeHead = vals.map((r) => r.slice());
  fakeHead[2][2] = 'コメント';           // 見出しの「修正指示」を書き換えた
  fakeHead[4][2] = '修正指示';           // 本文にちょうど「修正指示」と書いた
  back = ds.readBackNotes(fakeHead);
  ok(!back.ok && /見出しの行が見つかりません/.test(back.error), '🚨 本文に「修正指示」と書いた行を見出しと取り違えない (見出しは 修正指示・管理番号・AI生成画像 がそろった行 — Codex 名指し4 中)');
  const twoHeads = [vals[0], ['', ...vals[2]], ...vals.slice(1)];
  back = ds.readBackNotes(twoHeads);
  ok(!back.ok && /見出しの行 .* が 2 つ/.test(back.error), '🚨 見出しの行が 2 つ (コピーして 1 列ずらした) → どちらの列が本物か分からないので上書きしない (Codex 名指し7 高)', JSON.stringify(back));
  const withOrphan = [...vals, [], [ds.ORPHAN_HEADING, '', '全体をもっと明るく', '', '', '']];
  back = ds.readBackNotes(withOrphan);
  ok(back.ok && back.orphans.some((o) => o.note === '全体をもっと明るく'), '🚨 「前の依頼書の修正指示」の見出しの行に書いた修正指示も消さずに残す (Codex 名指し7 中)', JSON.stringify(back.orphans));
  const dupHead = vals.map((r, i) => (i === 2 ? [r[0], r[1], '修正指示', ...r.slice(2)] : [r[0], r[1], '', ...r.slice(2)]));
  back = ds.readBackNotes(dupHead);
  ok(!back.ok && /同じ名前の列が 2 つ/.test(back.error), '🚨 「修正指示」の見出しが 2 つ (人が同じ名前の列を足した) なら、どちらが本物か分からないので上書きしない (Codex 名指し2 高)');
  ok(ds.readBackNotes([]).ok && ds.readBackNotes(null).ok && ds.readBackNotes([['', '']]).ok, '空のタブ・タブなしは読める (戻すものなし)');

  // 管理番号と画像の照らし合わせ (読み戻しは FORMULA = 画像の列は式そのもの)
  {
    const fv = vals.map((r, i) => (i >= 3 ? [r[0], t.rows[i][1].__formula, r[2], r[3], r[4], r[5]] : r.slice()));
    const files = new Map([['DRVFILE000010', 10], ['DRVFILE000011', 11], ['DRVFILE000012', 12]]);
    const rof = (fid) => files.get(fid) ?? null;
    let bk = ds.readBackNotes(fv, { rootOfFile: rof });
    ok(bk.byKey.get(10)?.note === 'TOP の文字を大きく' && bk.byKey.get(12)?.note.startsWith('背景を白に') && bk.layout.rowOfKey.get(11) === 4 && bk.layout.headerIdx === 2 && bk.layout.noteCol === 2,
      '照らし合わせ: 管理番号とその行の画像が合えば同じ画像として読む・行の場所も覚える');
    const swapped = fv.map((r) => r.slice());
    [swapped[3][5], swapped[5][5]] = [swapped[5][5], swapped[3][5]];
    bk = ds.readBackNotes(swapped, { rootOfFile: rof });
    ok(bk.byKey.size === 0 && bk.orphans.map((o) => o.note).sort().join('|') === ['TOP の文字を大きく', '背景を白に\n影を薄く'].sort().join('|'),
      '🚨 管理番号のセルだけを入れ替えた (別の有効な番号) → 別の画像の行に付けず「行き先なし」に残す (Codex 名指し1 高)', JSON.stringify([...bk.byKey]));
    const moved2 = [fv[0], fv[1], fv[2], fv[5], fv[4], fv[3]];
    bk = ds.readBackNotes(moved2, { rootOfFile: rof });
    ok(bk.byKey.get(10)?.note === 'TOP の文字を大きく' && bk.byKey.get(10)?.row === 5 && bk.byKey.get(12)?.row === 3, '行ごと並べ替えた (管理番号と画像が一緒に動いた) ときは同じ画像として読む');
    const noImg = fv.map((r) => r.slice()); noImg[3][1] = '';
    bk = ds.readBackNotes(noImg, { rootOfFile: rof });
    ok(!bk.byKey.has(10) && bk.orphans.some((o) => o.note === 'TOP の文字を大きく'), '画像のセルを消された行は照らせないので「行き先なし」');
    const below = [...fv.slice(0, 3), fv[4], fv[5], [], [ds.ORPHAN_HEADING], ['前の', '', '前からの指示', 'v1', '', ''], fv[3]];
    bk = ds.readBackNotes(below, { rootOfFile: rof });
    ok(bk.byKey.get(10)?.note === 'TOP の文字を大きく' && bk.orphans.map((o) => o.note).join() === '前からの指示',
      '🚨 「前の依頼書の修正指示」より下へ動かした画像の行も、管理番号と画像が合えば同じ画像の行として読む (Codex 名指し6 中)', JSON.stringify([[...bk.byKey], bk.orphans]));
    // KEEP: 同じ行・同じ列なら修正指示のセルを書かない (読み戻した後に書いた分を消さない)
    bk = ds.readBackNotes(fv, { rootOfFile: rof });
    const kb = ds.buildDesignerSheet({ productCode: 'C', productName: 'n', previous: bk, images: [IMG(10, 10, 0), IMG(11, 21, 1, 2), IMG(12, 12, 2)] });
    const kr = kb.tabs[0].rows;
    ok(kb.kept === 3 && [3, 4, 5].every((i) => kr[i][2] && kr[i][2].__keep === true) && kb.tabs[0].clearRows === fv.length && kb.tabs[0].clearCols === 6 && kr[4][3] === 'v2',
      '🚨 前のシートと同じ行・同じ列の画像は、修正指示のセルを書かない (KEEP = いまシートにある値のまま。1 枚作り直しのよくある場合)', JSON.stringify(kr.slice(3).map((r) => r[2])));
    const kb2 = ds.buildDesignerSheet({ productCode: 'C', productName: 'n', previous: bk, images: [IMG(11, 21, 1, 2), IMG(10, 10, 0), IMG(12, 12, 2)] });
    ok(kb2.kept === 1 && kb2.tabs[0].rows[3][2] === '' && kb2.tabs[0].rows[4][2] === 'TOP の文字を大きく' && kb2.tabs[0].rows[5][2].__keep === true, '並びが変わった行は読み戻した値で書く (同じ行のままの画像だけ KEEP)');
    const shifted = fv.map((r, i) => (i < 2 ? r : [r[0], r[1], i === 2 ? '担当' : '', ...r.slice(2)]));
    const kb3 = ds.buildDesignerSheet({ productCode: 'C', productName: 'n', previous: ds.readBackNotes(shifted, { rootOfFile: rof }), images: [IMG(10, 10, 0)] });
    ok(kb3.kept === 0 && kb3.tabs[0].rows[3][2] === 'TOP の文字を大きく' && kb3.tabs[0].clearRows === undefined, '人が列を足した (修正指示の列が動いた) ときは KEEP にしない (読み戻した値で書き直す)');
  }
  // 作り直す: 画像 11 を作り直した (v2)・画像 12 は無くなった・画像 13 が増えた・並びが変わった
  back = ds.readBackNotes(vals.map((r, i) => (i === 4 ? [r[0], r[1], 'FV のロゴを右に', r[3], r[4], r[5]] : r)));
  const b2 = ds.buildDesignerSheet({ productCode: 'CODE1', productName: '商品', previous: back,
    images: [IMG(13, 13, 0), IMG(10, 10, 1), IMG(11, 21, 2, 2)] });
  const r2 = b2.tabs[0].rows;
  eq(r2.slice(3, 6).map((r) => [r[5], r[2], r[3]]), [['img-13', '', 'v1'], ['img-10', 'TOP の文字を大きく', 'v1'], ['img-11', 'FV のロゴを右に', 'v2 (修正指示は v1 の画像に書いたもの)']],
    '🚨 修正指示は同じ画像 (管理番号) の行に戻る (並びが変わっても)・増えた画像は空欄・作り直した画像は「v1 の画像に書いたもの」と添える');
  ok(isFormula(r2[5][1]) && r2[5][1].__formula.includes('DRVFILE000021'), '作り直した画像は最新の版のファイルを出す');
  const oh = r2.findIndex((r) => String(r[0]).startsWith('前の依頼書の修正指示'));
  ok(oh === 7 && r2[6].length === 0 && r2[8][2] === '背景を白に\n影を薄く' && r2[8][0].startsWith('2枚目') && r2[8][5] === '' && b2.orphaned === 1 && b2.carried === 2,
    '🚨 無くなった画像への修正指示はシートの下「前の依頼書の修正指示」に残す (管理番号は付けない = どの画像にも付け替えない)', JSON.stringify(r2.slice(6)));
  ok(b2.tabs[0].format.boldRows.includes(oh) && b2.tabs[0].format.shadedRows.includes(oh), '「前の依頼書の修正指示」の見出しは太字・網かけ');
  // もう一度作り直す (同じ版のまま): 注記は引き継ぐ・前に残した行き先なしも残る・人が空にした行き先なしは消える
  const v2 = shown(r2);
  let back3 = ds.readBackNotes(v2);
  const b3 = ds.buildDesignerSheet({ productCode: 'CODE1', productName: '商品', previous: back3, images: [IMG(13, 13, 0), IMG(10, 10, 1), IMG(11, 21, 2, 2)] });
  ok(b3.tabs[0].rows[5][3] === 'v2 (修正指示は v1 の画像に書いたもの)' && b3.tabs[0].rows.some((r) => r[2] === '背景を白に\n影を薄く') && b3.orphaned === 1,
    '同じ版のまま作り直しても注記と「前の依頼書の修正指示」は残る');
  const v2b = v2.map((r) => r.slice());
  v2b[5][3] = 'v2';            // 人が注記を消した (新しい画像にも当てはまると確かめた)
  v2b[8][2] = '';              // 行き先なしの修正指示を人が空にした (片付けた)
  back3 = ds.readBackNotes(v2b);
  const b4 = ds.buildDesignerSheet({ productCode: 'CODE1', productName: '商品', previous: back3, images: [IMG(13, 13, 0), IMG(10, 10, 1), IMG(11, 21, 2, 2)] });
  ok(b4.tabs[0].rows[5][3] === 'v2' && b4.orphaned === 0 && !b4.tabs[0].rows.some((r) => String(r[0]).startsWith('前の依頼書')), '人が消した注記・空にした行き先なしは戻さない');
  // さらに作り直した (v3): 指示は v1 のまま
  const b5 = ds.buildDesignerSheet({ productCode: 'CODE1', productName: '商品', previous: ds.readBackNotes(v2), images: [IMG(11, 31, 2, 3)] });
  eq(b5.tabs[0].rows[3][3], 'v3 (修正指示は v1 の画像に書いたもの)', '何度作り直しても、修正指示を書いた版を添え続ける');
  eq(ds.versionCell(2, { note: '指示', version: 'v2' }), 'v2', '指示を書いた版と今の版が同じなら添えない');

  // hash
  const h1 = ds.designerImagesHash({ jobId: 5, images: [IMG(10, 10, 0), IMG(11, 11, 1)] });
  ok(h1 === ds.designerImagesHash({ jobId: 5, images: [IMG(10, 10, 0, 1, { role: '別の役割' }), IMG(11, 11, 1)] }) && /^[0-9a-f]{64}$/.test(h1), 'hash は画像 (どの版のどのファイル) で決まる (役割の文字では変わらない)');
  ok(h1 !== ds.designerImagesHash({ jobId: 5, images: [IMG(10, 10, 0), IMG(11, 21, 1, 2)] }) && h1 !== ds.designerImagesHash({ jobId: 6, images: [IMG(10, 10, 0), IMG(11, 11, 1)] }), '1 枚作り直す・全部作り直すと hash が変わる');
}

// ════════════════════════════════════════════════════════════════
{
  const calls = [];
  const fake = { sheets: { spreadsheets: {
    get: async () => ({ data: { sheets: [{ properties: { sheetId: 5, title: 'T', gridProperties: { rowCount: 6000, columnCount: 26 } }, developerMetadata: [{ metadataKey: 'phOwnedTab', metadataValue: 'T' }] }] } }),
    batchUpdate: async (q) => { calls.push(q); return { data: {} }; },
  } } };
  const rows = [['見出し'], ['a', sw.KEEP_CELL, 'b'], ['c', sw.KEEP_CELL, 'd'], ['e']];
  await sw.writeSpreadsheet(fake, { spreadsheetId: 'S', tabs: [{ name: 'T', rows, clearRows: 5000, clearCols: 6 }] });
  const rq = calls[0].requestBody.requests;
  const vals = rq.filter((r) => r.updateCells && !/userEnteredFormat/.test(r.updateCells.fields));
  ok(vals.length <= 8 && vals.some((r) => r.updateCells.range && r.updateCells.range.startRowIndex === 3 && r.updateCells.range.endRowIndex === 5000),
    '🚨 KEEP のあるタブで前の版が 5,000 行あっても、KEEP の無い行はまとめて消す (1 行ずつの要求にしない — Codex base R7 P1)', String(vals.length));
  const touches = (r, row, col) => (r.updateCells.start ? (row >= r.updateCells.start.rowIndex && row < r.updateCells.start.rowIndex + r.updateCells.rows.length && col >= r.updateCells.start.columnIndex && col < r.updateCells.start.columnIndex + r.updateCells.rows[row - r.updateCells.start.rowIndex].values.length) : (row >= r.updateCells.range.startRowIndex && row < r.updateCells.range.endRowIndex && col >= r.updateCells.range.startColumnIndex && col < r.updateCells.range.endColumnIndex));
  ok(!vals.some((r) => touches(r, 1, 1) || touches(r, 2, 1)) && vals.some((r) => touches(r, 1, 2)) && vals.some((r) => touches(r, 1, 5))
    && !vals.some((r) => r.updateCells.range && r.updateCells.range.startRowIndex <= 1 && r.updateCells.range.endRowIndex > 1), 'KEEP のセルは消しも書きもしない');
}

console.log('② 本番の経路 (router・偽の Google)');
// ════════════════════════════════════════════════════════════════
const spec = lp.importSpec(db, { kind: 'product_analysis', title: 'LP制作システム', body: '本文 V2.2', sheetTitles: ['出力形式'], actor: 't' }).spec;
let seqNo = 0;
const T0 = Date.now();
/** 構成ができた (done・実モデル一致) 商品を 1 つ作る (test-ph-lp-image.mjs と同じ作り方) */
function makeComposed({ code } = {}) {
  seqNo++;
  const name = 'ハッカ油スプレー';
  const id = Number(db.prepare(`INSERT INTO product_drafts (ne_code, name, drive_folder_url, image_priority, created_by) VALUES (?, ?, ?, '自社商品（重要度：高）', 't')`)
    .run(code || 'DSHEET' + seqNo, name, FOLDER).lastInsertRowid);
  const draft = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(id);
  const images = [{ file_id: 'REFWHITE' + String(seqNo).padStart(6, '0'), role: 'white_bg', modified_time: '2026-10-01T00:00:00.000Z' }];
  const r = lp.requestJob(db, { draft, spec, idempotencyKey: 'key-ds-' + seqNo, actor: 't', productInfo: 'ハッカ油', colorVariations: '', images, now: T0 });
  const c = lp.claimJob(db, { runnerRunId: 'run-ds-' + seqNo, maxImages: 16, now: T0 });
  const g = lp.reserveGeneration(db, c.job.job_id, { leaseToken: c.job.lease_token, model: lp.DEFAULT_MODEL, promptVersion: lp.PROMPT_VERSION, now: T0 });
  for (const im of JSON.parse(r.job.packet_json).images) lp.recordImageServed(db, c.job.job_id, { leaseToken: c.job.lease_token, fileId: im.file_id, sha256: 'a'.repeat(64), bytes: 10, now: T0 });
  const s = lp.submitResult(db, g.generation_id, { packetHash: c.job.packet_hash, verdict: 'accepted', output: compositionFor(name), lint: { ok: true }, reviewRounds: 1, now: T0 });
  if (s.status !== 'done') throw new Error('submit: ' + JSON.stringify(s));
  lp.recordModelCheck(db, { runnerRunId: 'run-ds-' + seqNo, actualModels: ['claude-opus-5-5'], now: T0 });
  dbmod.setShootMode(db, id, 'none', { actor: 't' });
  return db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(id);
}
let fileSeq = 0;
const newDriveId = () => 'DRVIMG' + String(++fileSeq).padStart(8, '0');
/** 作る係の代わりに、依頼の画像を全部できたことにする (Drive のファイルも偽物に置く) */
function finishJob(jobId) {
  const nowS = new Date().toISOString();
  for (const im of db.prepare("SELECT id FROM ph_lp_images WHERE image_job_id = ? AND status IN ('queued','running')").all(jobId)) {
    const fid = newDriveId();
    g.files.set(fid, { id: fid, name: 'img.png', mimeType: 'image/png', parents: ['AIFOLDER'], perms: [] });
    db.prepare("UPDATE ph_lp_images SET status = 'done', drive_file_id = ?, completed_at = ?, cost_jpy = 5 WHERE id = ?").run(fid, nowS, im.id);
  }
  db.prepare("UPDATE ph_lp_image_jobs SET status = 'done', completed_at = ?, ai_folder_id = COALESCE(ai_folder_id, 'AIFOLDER') WHERE id = ?").run(nowS, jobId);
}
let keySeq = 0;
const fullJob = (draft) => {
  const r = li.requestImageJob(db, { draft, folderId: FID, idempotencyKey: 'ds-full-' + String(++keySeq).padStart(4, '0'), actor: 't', env: ENV });
  if (!r.ok) throw new Error('requestImageJob: ' + JSON.stringify(r));
  return r.job.id;
};
const regen = (draft, imageId) => {
  const r = li.requestImageRegen(db, { draft, imageId, folderId: FID, idempotencyKey: 'ds-regen-' + String(++keySeq).padStart(4, '0'), actor: 't', env: ENV });
  if (!r.ok) throw new Error('requestImageRegen: ' + JSON.stringify(r));
  return r.job.id;
};
const cardsOf = (draft) => li.imageStateFor(db, { draft, folderId: FID, env: ENV }).cards;

// ── 偽の Google (Drive のファイル・権限 / Sheets のタブ・セル・印) ──
const g = { files: new Map(), seq: 0, permSeq: 0, fail: {}, log: [], batches: 0, delay: 0 };
const gErr = (code, msg, reason) => Object.assign(new Error(msg), { code, ...(reason ? { errors: [{ reason }] } : {}) });
const hit = async (op, p) => {
  g.log.push(op);
  if (g.delay) await new Promise((res) => setTimeout(res, g.delay));
  const f = g.fail[op];
  if (f && (!f.when || f.when(p))) { if (f.once) delete g.fail[op]; throw f.err; }
};
const fileOrThrow = (id) => { const f = g.files.get(id); if (!f) throw gErr(404, 'File not found: ' + id); return f; };
const fakeDrive = {
  files: {
    list: async (p) => {
      await hit('list', p);
      const folder = /'([^']+)' in parents/.exec(p.q)?.[1];
      const ap = /appProperties has \{ key='([^']+)' and value='([^']+)' \}/.exec(p.q);
      const mime = /mimeType = '([^']+)'/.exec(p.q)?.[1];
      const files = [...g.files.values()].filter((f) => !f.trashed && f.parents.includes(folder) && (!mime || f.mimeType === mime)
        && (!ap || (f.appProperties || {})[ap[1]] === ap[2]));
      return { data: { files: files.map((f) => ({ id: f.id, name: f.name })) } };
    },
    get: async (p) => { await hit('get', p); const f = fileOrThrow(p.fileId); return { data: { id: f.id, name: f.name, trashed: !!f.trashed, parents: f.parents, mimeType: f.mimeType } }; },
    create: async (p) => {
      await hit('create', p);
      const id = `SS${String(++g.seq).padStart(6, '0')}`;
      g.files.set(id, { id, name: p.requestBody.name, mimeType: p.requestBody.mimeType, parents: p.requestBody.parents, appProperties: p.requestBody.appProperties || {},
        tabs: [{ sheetId: 0, title: 'シート1', values: [], formulas: [], meta: [] }], perms: [] });
      return { data: { id } };
    },
    update: async (p) => { await hit('update', p); const f = fileOrThrow(p.fileId); if ('name' in p.requestBody) f.name = p.requestBody.name; return { data: {} }; },
  },
  permissions: {
    list: async (p) => {
      await hit('plist', p);
      if (g.onPlist) { const fn = g.onPlist; g.onPlist = null; fn(p); }
      const f = fileOrThrow(p.fileId);
      // 本物と同じく 1 ページ 100 件まで (pageSize を省くとさらに少ないこともある)。続きは nextPageToken
      const size = Math.min(100, Number(p.pageSize) || 20);
      const from = Number(p.pageToken || 0);
      if (from) g.log.push('plist-page' + (from / size + 1));
      const all = f.perms.map((x) => ({ id: x.id, type: x.type, role: x.role }));
      return { data: { permissions: all.slice(from, from + size), ...(from + size < all.length ? { nextPageToken: String(from + size) } : {}) } };
    },
    create: async (p) => {
      await hit('pcreate', p);
      const f = fileOrThrow(p.fileId);
      if (!p.supportsAllDrives) throw gErr(404, 'File not found (共有ドライブ)');
      const rb = p.requestBody;
      const id = rb.type === 'anyone' ? 'anyoneWithLink' : 'perm' + (++g.permSeq);
      f.perms.push({ id, type: rb.type, role: rb.role, discover: rb.allowFileDiscovery });
      if (g.failAfterCreate) { g.failAfterCreate = false; throw gErr(undefined, 'socket hang up'); }
      return { data: { id } };
    },
    delete: async (p) => {
      await hit('pdelete', p);
      const f = fileOrThrow(p.fileId);
      const i = f.perms.findIndex((x) => x.id === p.permissionId);
      if (i < 0) throw gErr(404, 'Permission not found');
      f.perms.splice(i, 1);
      return { data: {} };
    },
  },
};
// 式のセルは画面に見える値 (IMAGE = 空・HYPERLINK = 表示名) で持つ。式そのものは formulas に
const shownOf = (c) => {
  const fv = c?.userEnteredValue?.formulaValue;
  if (fv != null) return fv.startsWith('=HYPERLINK') ? (/,\s*"([^"]*)"\)$/.exec(fv)?.[1] ?? '') : '';
  return c?.userEnteredValue?.stringValue ?? '';
};
const fakeSheets = { spreadsheets: {
  get: async (p) => {
    await hit('sget', p);
    if (g.onSget) { const fn = g.onSget; g.onSget = null; fn(p); }
    // n 回目の読み (書き込み係が送る直前に読む回) で割り込ませる口
    if (g.onSgetNth && --g.onSgetNth.n === 0) { const fn = g.onSgetNth.fn; g.onSgetNth = null; fn(p); }
    const f = fileOrThrow(p.spreadsheetId);
    return { data: { sheets: f.tabs.map((t) => ({ properties: { sheetId: t.sheetId, title: t.title, gridProperties: { rowCount: 1000, columnCount: 26 } },
      developerMetadata: t.meta.map((m) => ({ metadataKey: m.key, metadataValue: m.value })) })) } };
  },
  values: {
    get: async (p) => {
      await hit('vget', p);
      if (g.onVgetNth && --g.onVgetNth.n === 0) { const fn = g.onVgetNth.fn; g.onVgetNth = null; fn(p); }
      const f = fileOrThrow(p.spreadsheetId);
      const title = /^'(.+?)'(?:!|$)/.exec(p.range)?.[1];
      g.lastRange = p.range;
      const box = /!([A-Z]+)(\d+):([A-Z]+)(\d+)$/.exec(p.range);
      const colNo = (a) => [...a].reduce((n, ch) => n * 26 + ch.charCodeAt(0) - 64, 0);
      const lim = box ? { r0: Number(box[2]) - 1, r1: Number(box[4]), c0: colNo(box[1]) - 1, c1: colNo(box[3]) } : null;
      const t = f.tabs.find((x) => x.title === title);
      if (!t) throw gErr(400, 'Unable to parse range: ' + p.range);
      // 本物と同じく、後ろの空のセル・空の行は返さない
      // FORMULA なら式のセルは式そのもの (本物と同じ)
      const asFormula = p.valueRenderOption === 'FORMULA';
      const rows = t.values.map((r, ri) => { const a = r.map((v, ci) => (asFormula && t.formulas[ri]?.[ci] != null ? t.formulas[ri][ci] : v)); while (a.length && a[a.length - 1] === '') a.pop(); return a; })
        .map((a, ri) => (lim ? (ri >= lim.r0 && ri < lim.r1 ? a.slice(lim.c0, lim.c1) : []) : a));
      while (rows.length && rows[rows.length - 1].length === 0) rows.pop();
      return { data: { values: rows } };
    },
  },
  batchUpdate: async (p) => {
    await hit('sbatch', p);
    const f = fileOrThrow(p.spreadsheetId);
    const tabs = f.tabs.map((t) => ({ ...t, values: t.values.map((r) => r.slice()), formulas: t.formulas.map((r) => r.slice()), meta: t.meta.slice() }));
    const find = (id) => { const t = tabs.find((x) => x.sheetId === id); if (!t) throw gErr(400, `No grid with id: ${id}`); return t; };
    for (const r of p.requestBody.requests) {
      if (r.addSheet) {
        const pr = r.addSheet.properties;
        if (tabs.some((t) => t.title === pr.title || t.sheetId === pr.sheetId)) throw gErr(400, `A sheet with the name "${pr.title}" already exists`);
        tabs.push({ sheetId: pr.sheetId, title: pr.title, values: [], formulas: [], meta: [] });
      } else if (r.createDeveloperMetadata) {
        const m = r.createDeveloperMetadata.developerMetadata;
        find(m.location.sheetId).meta.push({ key: m.metadataKey, value: m.metadataValue });
      } else if (r.updateCells) {
        const u = r.updateCells;
        if (u.range) {
          const t = find(u.range.sheetId);
          if (/userEnteredValue/.test(u.fields)) {
            const rg = u.range;
            if (rg.startRowIndex == null) { t.values = []; t.formulas = []; }
            else {
              // 範囲 (四角) の中だけ消す (外のセル = KEEP の修正指示は残る)
              g.rectClears = (g.rectClears || 0) + 1;
              for (let ri = rg.startRowIndex; ri < Math.min(rg.endRowIndex, t.values.length); ri++) {
                for (let ci = rg.startColumnIndex; ci < rg.endColumnIndex; ci++) {
                  if (t.values[ri] && ci < t.values[ri].length) t.values[ri][ci] = '';
                  if (t.formulas[ri] && ci < t.formulas[ri].length) t.formulas[ri][ci] = null;
                }
              }
            }
          }
        }
        else {
          const t = find(u.start.sheetId);
          // start の位置から書く (送らなかったセル = KEEP はいまの値のまま)
          u.rows.forEach((row, dr) => (row.values || []).forEach((c, dc) => {
            const ri = u.start.rowIndex + dr; const ci = u.start.columnIndex + dc;
            while (t.values.length <= ri) t.values.push([]);
            while (t.formulas.length <= ri) t.formulas.push([]);
            while (t.values[ri].length < ci) t.values[ri].push('');
            t.values[ri][ci] = shownOf(c);
            t.formulas[ri][ci] = c?.userEnteredValue?.formulaValue ?? null;
          }));
          g.cellWrites = (g.cellWrites || 0) + 1;
        }
      } else if (r.updateSheetProperties && /title/.test(r.updateSheetProperties.fields)) {
        const t = find(r.updateSheetProperties.properties.sheetId);
        if (tabs.some((x) => x !== t && x.title === r.updateSheetProperties.properties.title)) throw gErr(400, 'duplicate title');
        t.title = r.updateSheetProperties.properties.title;
      } else if (r.updateSheetProperties || r.repeatCell || r.updateDimensionProperties || r.appendDimension) {
        find((r.updateSheetProperties?.properties || r.repeatCell?.range || r.updateDimensionProperties?.range || r.appendDimension).sheetId);
        if (r.updateDimensionProperties && r.updateDimensionProperties.range.dimension === 'ROWS') {
          const t = find(r.updateDimensionProperties.range.sheetId);
          t.heights = t.heights || {};
          for (let i = r.updateDimensionProperties.range.startIndex; i < r.updateDimensionProperties.range.endIndex; i++) t.heights[i] = r.updateDimensionProperties.properties.pixelSize;
        }
      } else if (r.deleteSheet) {
        find(r.deleteSheet.sheetId);
        tabs.splice(tabs.findIndex((t) => t.sheetId === r.deleteSheet.sheetId), 1);
      } else throw gErr(400, 'unknown request ' + Object.keys(r).join());
    }
    if (!tabs.length) throw gErr(400, 'You can\'t remove all the sheets in a document.');
    f.tabs = tabs;
    g.batches += 1;
    if (g.failAfterBatch) { g.failAfterBatch = false; throw gErr(undefined, 'socket hang up (書けた後)'); }
    // 書けた直後 (記録する前) に、ほかの人の操作を割り込ませる口
    if (g.onBatch) { const fn = g.onBatch; g.onBatch = null; fn(p); }
    return { data: { replies: [] } };
  },
} };
let clientsOn = true;
svc.__setDesignerSheetClientsForTest(() => (clientsOn ? { drive: fakeDrive, sheets: fakeSheets } : null));

const wf = await import('../apps/product-hub/lib/workflow.js');
const express = (await import('express')).default;
const { default: router } = await import('../apps/product-hub/router.js');
const app = express();
const ADMIN = { email: 'nakahara@b-faith.biz', role: 'admin' };
let session = ADMIN;
app.use((req, _res, next) => { req.session = session; next(); });
app.use('/apps/product-hub', router);
const server = app.listen(0);
await new Promise((r) => server.once('listening', r));
const base = `http://127.0.0.1:${server.address().port}/apps/product-hub`;
const post = async (url, body) => { const res = await fetch(base + url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }); return { status: res.status, json: await res.json() }; };
// 作っている途中の GET /lp-images は本物の作る係を起こす (この試験では Drive が無いので失敗にされる) — 途中の状態は直接見る
const stateNow = (draft) => svc.designerSheetStateFor(db, db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(draft.id), { canEdit: true });
const stateOf = async (draft) => (await (await fetch(`${base}/api/drafts/${draft.id}/lp-images`)).json()).designer_sheet;
/** 画面と同じ送り方 (画面が見ている依頼書と画像の並び) */
const press = async (draft, over = {}) => { const st = await stateOf(draft); return post(`/api/drafts/${draft.id}/designer-sheet`, { seen_file_id: st.file_id || '', seen_images_hash: st.images_hash || '', ...over }); };
const rowOf = (draft) => db.prepare('SELECT * FROM ph_designer_sheets WHERE draft_id = ?').get(draft.id) || {};
const sharesOf = (draft) => db.prepare('SELECT * FROM ph_designer_sheet_shares WHERE draft_id = ? ORDER BY id').all(draft.id);
const sheetFiles = () => [...g.files.values()].filter((f) => f.mimeType === 'application/vnd.google-apps.spreadsheet');
const tabOf = (fileId, title = ds.DESIGNER_TAB) => g.files.get(fileId)?.tabs.find((t) => t.title === title);
const publicFiles = () => [...g.files.values()].filter((f) => (f.perms || []).some((x) => x.type === 'anyone')).map((f) => f.id).sort();
const evOf = (draft, ev) => db.prepare('SELECT detail FROM draft_events WHERE draft_id = ? AND event = ? ORDER BY id').all(draft.id, ev).map((e) => e.detail);
/** 人がシートの修正指示の欄に書く (管理番号の行) */
const humanWrites = (fileId, key, note) => {
  const t = tabOf(fileId);
  const head = t.values.findIndex((r) => r.includes('修正指示'));
  const col = t.values[head].indexOf('修正指示');
  const keyCol = t.values[head].findIndex((c) => c.startsWith('管理番号'));
  const row = t.values.find((r) => r[keyCol] === key);
  row[col] = note;
};
const noteOfKey = (fileId, key) => { const t = tabOf(fileId); const head = t.values.findIndex((r) => r.includes('修正指示')); const keyCol = t.values[head].findIndex((c) => c.startsWith('管理番号')); const row = t.values.find((r) => r[keyCol] === key); return row ? { note: row[t.values[head].indexOf('修正指示')], ver: row[t.values[head].indexOf('版')], row: t.values.indexOf(row) } : null; };

const A = makeComposed({ code: 'maitakep50' });
{
  // 画像がまだ無い
  let st = await stateOf(A);
  ok(st && st.exists === false && /先に「AI画像生成」/.test(st.blocked || '') && st.can_edit === true, 'GET (画像の状態): 依頼書の状態も載る・画像がまだ無ければ作れない理由');
  let r = await post(`/api/drafts/${A.id}/designer-sheet`, { seen_file_id: '', seen_images_hash: 'a'.repeat(64) });
  ok(r.status === 409 && r.json.code === 'not_ready' && g.log.length === 0 && sheetFiles().length === 0, '🚨 画像ができていなければ 409・Google に触らない');

  // 全部作っている途中
  const j1 = fullJob(A);
  st = stateNow(A);
  ok(/いま画像を作っています/.test(st.blocked || ''), '全部作っている途中は作れない (全部できてから)');
  // 2 枚できて 1 枚失敗 → 作れない
  finishJob(j1);
  const ims = db.prepare('SELECT * FROM ph_lp_images WHERE image_job_id = ? ORDER BY seq').all(j1);
  db.prepare("UPDATE ph_lp_images SET status = 'failed', drive_file_id = NULL WHERE id = ?").run(ims[2].id);
  st = await stateOf(A);
  ok(/作れなかった画像があります \(2枚目\)/.test(st.blocked || '') && st.total === 3 && st.done === 2, '🚨 作れなかった画像があれば作れない (どれかを出す)');
  // 失敗した 1 枚を作り直してできた → 作れる
  const rj = regen(A, ims[2].id);
  st = stateNow(A);
  ok(/作り直している/.test(st.blocked || ''), '作り直している途中は作れない');
  finishJob(rj);
  st = await stateOf(A);
  ok(st.blocked === null && st.done === 3 && /^[0-9a-f]{64}$/.test(st.images_hash), '全部できたら作れる (作り直してできた 1 枚も含む)');

  // 入力の形
  r = await post(`/api/drafts/${A.id}/designer-sheet`, { seen_images_hash: st.images_hash });
  const r2 = await post(`/api/drafts/${A.id}/designer-sheet`, { seen_file_id: '', seen_images_hash: 'x' });
  ok(r.status === 400 && r2.status === 400 && g.log.length === 0, 'seen_file_id / seen_images_hash が無い・形が違えば 400');

  // サービスアカウントが無い
  clientsOn = false;
  st = await stateOf(A);
  r = await post(`/api/drafts/${A.id}/designer-sheet`, { seen_file_id: '', seen_images_hash: st.images_hash });
  ok(r.status === 503 && /GOOGLE_SERVICE_ACCOUNT_KEY/.test(r.json.error) && /GOOGLE_SERVICE_ACCOUNT_KEY/.test(st.blocked || '') && g.log.length === 0, '🚨 サービスアカウントが無ければ作らない・画面にも理由 (fail-closed)');
  clientsOn = true;

  // 権限: 役割なしは 403・画像登録者 (管理者でない) は作れる
  const noRole = wf.createStaff({ name: '依頼書 役割なし', kind: 'internal', portal_email: 'ds-norole@b-faith.biz' });
  const imgStaff = wf.createStaff({ name: '依頼書 画像登録者', kind: 'internal', portal_email: 'ds-img@b-faith.biz' });
  db.prepare(`INSERT INTO ph_staff_roles (staff_id, role_code) VALUES (?, 'image')`).run(imgStaff);
  session = { email: 'ds-norole@b-faith.biz', displayName: '役割なし', role: 'user' };
  st = await stateOf(A);
  r = await post(`/api/drafts/${A.id}/designer-sheet`, { seen_file_id: '', seen_images_hash: st.images_hash });
  ok(r.status === 403 && /画像登録者/.test(r.json.error) && st.can_edit === false && g.log.length === 0, '🚨 画像の役割が無い担当者は 403 (画面にも押せない人と渡す)');

  const stepsBefore = JSON.stringify(db.prepare('SELECT step_code, state FROM draft_step_progress WHERE draft_id = ? ORDER BY step_code').all(A.id));
  const statusBefore = db.prepare('SELECT status FROM product_drafts WHERE id = ?').get(A.id).status;
  session = { email: 'ds-img@b-faith.biz', displayName: '画像登録者', role: 'user' };
  r = await press(A);
  session = ADMIN;
  const cards = cardsOf(A);
  const curFiles = cards.map((c) => c.current.drive_file_id);
  const ss = sheetFiles();
  ok(r.status === 200 && r.json.ok && r.json.created === true && r.json.count === 3 && ss.length === 1, '画像登録者 (管理者でない) が押すと依頼書ができる', JSON.stringify(r));
  const F = ss[0];
  ok(F.parents.join() === FID && F.name === 'デザイナー修正依頼書_maitakep50' && F.appProperties.phDesignerSheetDraft === String(A.id) && !('phShootSheetDraft' in F.appProperties),
    '置き場 = 商品の画像フォルダ・名前 = デザイナー修正依頼書_<商品コード>・印 (appProperties) は撮影指示書と別のキー');
  const t = tabOf(F.id);
  ok(t && F.tabs.length === 1 && t.meta.some((m) => m.key === 'phOwnedTab'), '最初の「シート1」を消して、印つきのタブ 1 枚');
  eq(t.values.slice(3).map((r0) => r0[0]), ['0枚目 (TOP)｜楽天検索結果用TOP画像／商品認識', '1枚目｜FV／商品理解\n夏のベタつく空気に、ひと吹き。', '2枚目｜使用シーン／使い方の理解\n玄関にも、網戸にも。'],
    'TOP から順に、画像番号・役割・見出し (画像を作ったときの構成から)');
  eq(t.formulas.slice(3).map((r0) => r0[1]), curFiles.map((id) => `=IMAGE("https://lh3.googleusercontent.com/d/${id}")`), '画像の列 = 各画像の最新のできた版を =IMAGE() で');
  eq(t.values.slice(3).map((r0) => [r0[2], r0[3], r0[5]]), [['', 'v1', `img-${cards[0].root_id}`], ['', 'v1', `img-${cards[1].root_id}`], ['', 'v2', `img-${cards[2].root_id}`]], '修正指示は空欄・版・管理番号');
  ok(t.heights && t.heights[3] === 300 && t.heights[5] === 300 && t.heights[2] === undefined, '画像の行を高くする');
  eq(publicFiles(), curFiles.slice().sort(), '🚨 公開 (リンクを知っている人は閲覧可) は依頼書に載せる 3 枚だけ');
  const v1of3rd = db.prepare('SELECT drive_file_id FROM ph_lp_images WHERE id = ?').get(ims[2].id).drive_file_id;
  ok(v1of3rd == null && !publicFiles().includes('REFWHITE' + String(seqNo).padStart(6, '0')) && !g.files.get(FID), '🚨 古い版・素材 (参考の白抜き)・フォルダは公開しない');
  const p0 = g.files.get(curFiles[0]).perms[0];
  ok(p0.type === 'anyone' && p0.role === 'reader' && p0.discover === false, '公開は anyone・reader・検索に出さない (リンクを知っている人だけ)');
  ok(g.log.filter((x) => x === 'pcreate').length === 3, '権限は共有ドライブ対応 (supportsAllDrives) で付ける (偽物は無いと 404)');
  eq(sharesOf(A).map((s) => [s.drive_file_id, s.permission_id, s.revoked_at, s.shared_by]), curFiles.map((id) => [id, 'anyoneWithLink', null, 'ds-img@b-faith.biz']), '付けた公開を記録する (後で外せるように・誰が)');
  const row = rowOf(A);
  ok(row.file_id === F.id && row.url === `https://docs.google.com/spreadsheets/d/${F.id}/edit` && row.image_count === 3 && row.images_hash === st.images_hash && row.writing_at == null && row.lease_token == null,
    '記録: URL・ファイル ID・作ったときの画像の hash (印は返す)');
  ok(evOf(A, 'designer_sheet_created').length === 1, '履歴に残す');
  st = await stateOf(A);
  ok(st.exists && st.url === row.url && st.stale === false && st.count === 3 && st.by === 'ds-img@b-faith.biz', '画面: 作成済み (開く)・最新');
  ok(JSON.stringify(db.prepare('SELECT step_code, state FROM draft_step_progress WHERE draft_id = ? ORDER BY step_code').all(A.id)) === stepsBefore
    && db.prepare('SELECT status FROM product_drafts WHERE id = ?').get(A.id).status === statusBefore, '工程・ボード (⑤デザイン修正) は動かさない');

  // 古い画面: 作る前の画面 (seen_file_id = '') から押す
  const nFiles = sheetFiles().length;
  r = await post(`/api/drafts/${A.id}/designer-sheet`, { seen_file_id: '', seen_images_hash: st.images_hash });
  ok(r.status === 409 && r.json.code === 'stale_screen' && sheetFiles().length === nFiles && r.json.designer_sheet?.exists === true, '🚨 古いタブ (作る前の画面) からの 2 回目は 409・2 つ目を作らない・最新の状態を返す');

  // 人が修正指示を書く・人のタブを足す → 1 枚作り直す → 最新の画像で作り直す
  humanWrites(F.id, `img-${cards[0].root_id}`, 'TOP の文字を大きく');
  humanWrites(F.id, `img-${cards[1].root_id}`, 'ロゴを右上に\n背景をもう少し明るく');
  F.tabs.push({ sheetId: 77, title: '人のメモ', values: [['メモ']], formulas: [], meta: [] });
  const oldFv = cards[1].current.drive_file_id;
  const rj2 = regen(A, cards[1].head_id);
  st = stateNow(A);
  ok(st.exists && /作り直している/.test(st.blocked || ''), '作り直している間は「作り直す」も押せない');
  finishJob(rj2);
  st = await stateOf(A);
  ok(st.stale === true && st.blocked === null, '🚨 作成後に作り直した画像があれば「最新の画像で作り直す」を出す');
  r = await post(`/api/drafts/${A.id}/designer-sheet`, { seen_file_id: F.id, seen_images_hash: row.images_hash });
  ok(r.status === 409 && r.json.code === 'images_changed', '🚨 画面が見ていた画像の並びと違えば 409 (見ていない画像で作らない)');
  const batches0 = g.batches;
  r = await press(A);
  const cards2 = cardsOf(A);
  ok(r.status === 200 && r.json.created === false && r.json.carried === 2 && r.json.revoked === 1 && sheetFiles().length === 1 && rowOf(A).file_id === F.id && g.batches === batches0 + 1,
    '作り直し: 同じファイルを上書き (URL は変わらない)・書いた修正指示 2 件を残した・古い版の公開を 1 つ外した', JSON.stringify(r.json));
  eq(noteOfKey(F.id, `img-${cards[0].root_id}`), { note: 'TOP の文字を大きく', ver: 'v1', row: 3 }, '🚨 人が書いた修正指示は同じ画像の行に戻る');
  eq(noteOfKey(F.id, `img-${cards[1].root_id}`), { note: 'ロゴを右上に\n背景をもう少し明るく', ver: 'v2 (修正指示は v1 の画像に書いたもの)', row: 4 }, '🚨 作り直した画像にも修正指示を残し、v1 に書いたものと添える');
  ok(tabOf(F.id).formulas[4][1] === `=IMAGE("https://lh3.googleusercontent.com/d/${cards2[1].current.drive_file_id}")` && cards2[1].current.drive_file_id !== oldFv, '画像は最新の版 (v2) に替わる');
  ok(F.tabs.some((x) => x.title === '人のメモ' && x.values[0][0] === 'メモ'), '人が足したタブは触らない');
  eq(publicFiles(), cards2.map((c) => c.current.drive_file_id).sort(), '🚨 依頼書から外れた古い版 (v1) の公開は外す (公開は載せている最新版だけ)');
  ok(sharesOf(A).find((s) => s.drive_file_id === oldFv).revoked_at != null, '外したことを記録する');
  st = await stateOf(A);
  ok(st.stale === false && evOf(A, 'designer_sheet_updated').length === 1, '作り直した後は最新・履歴');

  // 読み戻した後・送る前に人が修正指示を書いた → 同じ行の画像は KEEP なので消えない
  {
    const rjK = regen(A, cards2[2].head_id); finishJob(rjK);
    g.onSgetNth = { n: 3, fn: () => humanWrites(F.id, `img-${cards2[2].root_id}`, '作り直しの最中に書いた') };
    const rK = await press(A);
    ok(rK.status === 200 && rK.json.kept === 3 && noteOfKey(F.id, `img-${cards2[2].root_id}`).note === '作り直しの最中に書いた' && noteOfKey(F.id, `img-${cards2[0].root_id}`).note === 'TOP の文字を大きく',
      '🚨 書く直前に読み直した後・Google に送る前に人が書いた修正指示も消えない (同じ行の画像は修正指示のセルを書かない — Codex 名指し1 高)', JSON.stringify(rK.json));
    ok(noteOfKey(F.id, `img-${cards2[2].root_id}`).ver === 'v' + cardsOf(A)[2].current.version && cardsOf(A)[2].current.version === 3 && tabOf(F.id).formulas[5][1].includes(cardsOf(A)[2].current.drive_file_id), 'KEEP でも画像・版は最新に書き直す');
    // 管理番号のセルだけ入れ替えた → 別の画像に付けない
    const t0 = tabOf(F.id); const kc = 5;
    [t0.values[3][kc], t0.values[4][kc]] = [t0.values[4][kc], t0.values[3][kc]];
    const rjS = regen(A, cardsOf(A)[2].head_id); finishJob(rjS);
    const rS = await press(A);
    const tS = tabOf(F.id);
    const ohS = tS.values.findIndex((x) => String(x[0]).startsWith('前の依頼書の修正指示'));
    ok(rS.status === 200 && tS.values[3][2] === '' && tS.values[4][2] === '' && ohS > 0 && tS.values.slice(ohS + 1).map((x) => x[2]).sort().join('|') === ['TOP の文字を大きく', 'ロゴを右上に\n背景をもう少し明るく'].sort().join('|'),
      '🚨 (本番の経路) 管理番号のセルを入れ替えられたら、その修正指示は別の画像の行に付けず下に残す', JSON.stringify(tS.values.map((x) => x[2])));
    // 片付け: 下に残った修正指示を空にし、元の 2 件を書き直して次の試験へ
    for (const row of tS.values.slice(ohS + 1)) row[2] = '';
    humanWrites(F.id, `img-${cards2[0].root_id}`, 'TOP の文字を大きく');
    humanWrites(F.id, `img-${cards2[1].root_id}`, 'ロゴを右上に\n背景をもう少し明るく');
    humanWrites(F.id, `img-${cards2[2].root_id}`, '');
  }
  // 送ったが Google が断った (403 = 書けていないと分かる) → 送らなかったのと同じ (今回の画像の公開は外す・作り直しが要る、は出さない)
  {
    const rjD = regen(A, cardsOf(A)[2].head_id); finishJob(rjD);
    const newD = cardsOf(A)[2].current.drive_file_id;
    g.fail.sbatch = { err: gErr(403, 'The caller does not have permission'), once: true };
    const rD = await press(A);
    ok(rD.status === 502 && !publicFiles().includes(newD) && rowOf(A).writing_at == null && !tabOf(F.id).formulas[5][1].includes(newD),
      '🚨 Google が書き込みを断った (403) ときは、シートは前のまま = 今回載せようとした画像の公開を外し、書いている印も残さない (Codex 名指し6 高)', JSON.stringify([rD.json, rowOf(A).writing_at]));
    const rD2 = await press(A);
    ok(rD2.status === 200 && publicFiles().includes(newD), '押し直せば作れる');
  }
  // 送った後に返事が来なかった (Google 側では書けている) → 今回載せた画像の公開は外さない・作り直しが要る、を出す
  {
    const rjB = regen(A, cardsOf(A)[2].head_id); finishJob(rjB);
    const newFile = cardsOf(A)[2].current.drive_file_id;
    g.failAfterBatch = true;
    const rB = await press(A);
    ok(rB.status === 502 && publicFiles().includes(newFile) && tabOf(F.id).formulas[5][1].includes(newFile) && rowOf(A).writing_at != null && stateNow(A).stale === true,
      '🚨 送った後の失敗は「書けていない」と決めつけない — シートが出している新しい画像の公開は外さない (Codex 名指し5 中)', JSON.stringify(rB.json));
    // 返事が来なかった後で、同じ画像をさらに作り直した (v 次) → 片付けは「今の画像」ではなく、シートに送った画像の公開を残す
    const rjB3 = regen(A, cardsOf(A)[2].head_id); finishJob(rjB3);
    await svc.sweepDesignerShares({ db });
    ok(publicFiles().includes(newFile) && JSON.parse(rowOf(A).writing_images_json || '[]').includes(newFile),
      '🚨 書いている途中で止まった商品の片付けは、送った画像 (記録した一覧) の公開を残す (シートが出している — Codex 名指し7 高)');
    const rB2 = await press(A);
    ok(rB2.status === 200 && rowOf(A).writing_at == null && rowOf(A).writing_images_json == null && stateNow(A).stale === false && !publicFiles().includes(newFile),
      '押し直せば記録でき、外れた画像の公開も外れる');
  }
  // 読み戻した後 (書く直前の読み直しの前) に、人が行を入れ替えた・列を足した → 書かない (修正指示を消さない・付け違えない)
  {
    const rjX = regen(A, cards2[1].head_id); finishJob(rjX);
    const tX = tabOf(F.id);
    const before = JSON.stringify(tX.values);
    const bX = g.batches;
    g.onVgetNth = { n: 2, fn: () => { [tX.values[3], tX.values[4]] = [tX.values[4], tX.values[3]]; [tX.formulas[3], tX.formulas[4]] = [tX.formulas[4], tX.formulas[3]]; } };
    const rX = await press(A);
    ok(rX.status === 409 && rX.json.code === 'sheet_changed' && g.batches === bX && /書いた内容は消していません/.test(rX.json.error),
      '🚨 読み戻した後に行を入れ替えられたら書かない (KEEP の座標がずれて修正指示が別の画像に付くのを防ぐ — Codex 名指し3 高)', JSON.stringify(rX.json));
    g.onVgetNth = { n: 2, fn: () => { tX.values = tX.values.map((r) => ['', ...r]); tX.formulas = tX.formulas.map((r) => [null, ...r]); } };
    const rX2 = await press(A);
    ok(rX2.status === 409 && rX2.json.code === 'sheet_changed' && g.batches === bX, '🚨 読み戻した後に列を足されても書かない');
    tX.values = tX.values.map((r) => r.slice(1)); tX.formulas = tX.formulas.map((r) => r.slice(1));
    [tX.values[3], tX.values[4]] = [tX.values[4], tX.values[3]]; [tX.formulas[3], tX.formulas[4]] = [tX.formulas[4], tX.formulas[3]];
    ok(JSON.stringify(tX.values) === before, '(片付け) 元に戻した');
    const rX3 = await press(A);
    ok(rX3.status === 200 && noteOfKey(F.id, `img-${cards2[1].root_id}`).note === 'ロゴを右上に\n背景をもう少し明るく', 'シートを触り終えてから押せば作り直せる (修正指示は残る)');
  }
  // 人が「デザイナー修正依頼」のタブの名前を変えた → 印で自分のタブと分かる。名前を元に戻して同じタブに書く (コピーを作らない)
  {
    const tR = tabOf(F.id);
    tR.title = '旧 依頼 (田中)';
    const nTabs = F.tabs.length;
    const rjR = regen(A, cardsOf(A)[0].head_id); finishJob(rjR);
    const rR = await press(A);
    ok(rR.status === 200 && rR.json.carried === 2 && !!tabOf(F.id) && !F.tabs.some((x) => x.title === '旧 依頼 (田中)') && F.tabs.length === nTabs && noteOfKey(F.id, `img-${cards2[0].root_id}`)?.note === 'TOP の文字を大きく',
      '🚨 タブの名前を変えられても、印で自分のタブと分かる — 名前を元に戻して同じタブに書く (修正指示は残る・コピーを作らないので古いタブへの書き込みを落とさない — Codex 名指し3・6 中)', JSON.stringify(rR.json));
    // 人がタブをコピーした (印も写る) → どれが今の依頼書か分からない → 止める
    const tNow = tabOf(F.id);
    F.tabs.push({ ...tNow, sheetId: 99, title: 'デザイナー修正依頼 のコピー', values: tNow.values.map((x) => x.slice()), formulas: tNow.formulas.map((x) => x.slice()), meta: tNow.meta.slice() });
    const bR = g.batches;
    const rR2 = await press(A);
    ok(rR2.status === 409 && rR2.json.code === 'unreadable' && /2 枚あります/.test(rR2.json.error) && g.batches === bR, '🚨 印の付いたタブが 2 枚 (コピーした) → どれが今のか分からないので作り直さない (片方の修正指示を黙って落とさない)', JSON.stringify(rR2.json));
    F.tabs = F.tabs.filter((x) => x.sheetId !== 99);
  }
  // 人が左に 26 列を足して、全部が AA 列より右へ動いた → タブ全体を読むので、修正指示は残る (A1:Z2000 で読むと空に見えて消していた)
  {
    const tW = tabOf(F.id);
    tW.values = tW.values.map((r) => [...Array(26).fill(''), ...r]);
    tW.formulas = tW.formulas.map((r) => [...Array(26).fill(null), ...r]);
    const rjW = regen(A, cardsOf(A)[1].head_id); finishJob(rjW);
    const rW = await press(A);
    ok(rW.status === 200 && g.lastRange === `'${ds.DESIGNER_TAB}'` && noteOfKey(F.id, `img-${cards2[0].root_id}`)?.note === 'TOP の文字を大きく' && noteOfKey(F.id, `img-${cards2[1].root_id}`)?.note === 'ロゴを右上に\n背景をもう少し明るく',
      '🚨 読み戻しはタブ全体 (範囲で区切らない) — 列を足して右へ動いた修正指示も読んで残す (Codex 名指し2 高)', JSON.stringify([rW.json, g.lastRange]));
  }
  // prompt が上限で切れて「この画像の指示」が無い → 受付のときの構成から役割を引く
  {
    const rootTop = cardsOf(A)[0].root_id;
    const keepPrompt = db.prepare('SELECT prompt FROM ph_lp_images WHERE id = ?').get(rootTop).prompt;
    db.prepare('UPDATE ph_lp_images SET prompt = ? WHERE id = ?').run(keepPrompt.slice(0, keepPrompt.indexOf('【この画像の指示')), rootTop);
    const rjP = regen(A, cardsOf(A)[0].head_id); finishJob(rjP);
    const rP = await press(A);
    ok(rP.status === 200 && tabOf(F.id).values[3][0] === '0枚目 (TOP)｜楽天検索結果用TOP画像／商品認識', 'prompt が切れていても、受付のときの構成から役割を引く (Codex 名指し2 中)', tabOf(F.id).values[3][0]);
    // 共通の決まりの中に「【この画像の指示: 例】」と例が書かれていても、その画像の見出しと同じものを探すので取り違えない
    db.prepare('UPDATE ph_lp_images SET prompt = ? WHERE id = ?').run('【この画像の指示: 例】\n## 画像の役割\n偽の役割\n\n' + keepPrompt, rootTop);
    const rjP3 = regen(A, cardsOf(A)[0].head_id); finishJob(rjP3);
    const rP3 = await press(A);
    ok(rP3.status === 200 && tabOf(F.id).values[3][0] === '0枚目 (TOP)｜楽天検索結果用TOP画像／商品認識', '🚨 prompt の中の例の「この画像の指示」と取り違えない (Codex 名指し7 低)', tabOf(F.id).values[3][0]);
    // 「この画像の指示」はあるが、上限 (30,000 文字) まであって途中で切れている → 受付の構成から
    const mi = keepPrompt.indexOf('【この画像の指示');
    const partial = keepPrompt.slice(mi, keepPrompt.indexOf('## 画像の役割', mi) + '## 画像の役割\n楽天検'.length);
    db.prepare('UPDATE ph_lp_images SET prompt = ? WHERE id = ?').run('x'.repeat(30_000 - partial.length) + partial, rootTop);
    const rjP2 = regen(A, cardsOf(A)[0].head_id); finishJob(rjP2);
    const rP2 = await press(A);
    ok(rP2.status === 200 && tabOf(F.id).values[3][0] === '0枚目 (TOP)｜楽天検索結果用TOP画像／商品認識', 'prompt が上限まであって「この画像の指示」の途中で切れていても、受付の構成から引く (Codex 名指し4 低)', tabOf(F.id).values[3][0]);
    db.prepare('UPDATE ph_lp_images SET prompt = ? WHERE id = ?').run(keepPrompt, rootTop);
  }
  // 全部作り直す (別の画像になる) → 修正指示は下に残す
  const j2 = fullJob(A);
  st = stateNow(A);
  ok(/いま画像を作っています/.test(st.blocked || '') && st.stale === true, '全部作り直している途中は押せない (作り直しが要るのは出す)');
  finishJob(j2);
  r = await press(A);
  const cards3 = cardsOf(A);
  const tt = tabOf(F.id);
  const oh = tt.values.findIndex((x) => String(x[0]).startsWith('前の依頼書の修正指示'));
  ok(r.status === 200 && r.json.carried === 0 && r.json.orphaned === 2 && oh > 5 && tt.values.slice(oh + 1).map((x) => x[2]).join('|') === 'TOP の文字を大きく|ロゴを右上に\n背景をもう少し明るく',
    '🚨 全部作り直した (別の画像) ときは、前の修正指示を別の画像の行に付け替えず、下の「前の依頼書の修正指示」に残す', JSON.stringify(tt.values));
  ok(tt.values.slice(3, 6).every((x) => x[2] === '') && tt.values.slice(3, 6).map((x) => x[5]).join() === cards3.map((c) => `img-${c.root_id}`).join(), '新しい画像の行は空欄・新しい管理番号');
  eq(publicFiles(), cards3.map((c) => c.current.drive_file_id).sort(), '前の依頼の画像の公開は全部外す');

  // 見出しを消された → 上書きしない
  const keepVals = JSON.stringify(tabOf(F.id).values);
  tabOf(F.id).values.splice(2, 1);
  const b0 = g.batches;
  const jr = regen(A, cards3[0].head_id); finishJob(jr);
  r = await press(A);
  ok(r.status === 409 && r.json.code === 'unreadable' && g.batches === b0 && /見出しの行が見つかりません/.test(r.json.error), '🚨 見出しの行が消されたシートは上書きしない (書いた内容が分からないので消さない)');
  const newV2 = cardsOf(A)[0].current.drive_file_id;
  ok(!publicFiles().includes(newV2), '🚨 書かなかったときは、この回で付けた公開を外す (依頼書に載らない画像を公開したままにしない)');
  tabOf(F.id).values = JSON.parse(keepVals);

  // 人が「修正指示」の列をもう 1 つ足した → 上書きしない
  {
    const tD = tabOf(F.id);
    const hi = tD.values.findIndex((x) => x.includes('修正指示'));
    const keepD = JSON.stringify([tD.values, tD.formulas]);
    tD.values = tD.values.map((x, i) => [...x.slice(0, 2), i === hi ? '修正指示' : '', ...x.slice(2)]);
    tD.formulas = tD.formulas.map((x) => [...x.slice(0, 2), null, ...x.slice(2)]);
    const bD = g.batches;
    const rD = await press(A);
    ok(rD.status === 409 && rD.json.code === 'unreadable' && g.batches === bD, '🚨 (本番の経路) 「修正指示」の列が 2 つあれば上書きしない');
    [tD.values, tD.formulas] = JSON.parse(keepD);
  }
  // 人が同じ名前のタブを作った (印なし) → 上書きしない
  const ownTab = tabOf(F.id);
  ownTab.meta = [];
  r = await press(A);
  ok(r.status === 409 && r.json.code === 'tab_conflict' && /人が作った/.test(r.json.error), '🚨 印の無い同じ名前のタブ (人が作った) は上書きしない');
  ownTab.meta = [{ key: 'phOwnedTab', value: ds.DESIGNER_TAB }];

  // 画像フォルダを変えた → 新しいフォルダに作り、前のフォルダの依頼書から修正指示を引き継ぐ (前の依頼書は消さない)
  {
    const FID2 = '1MovedFolderAbCdEfGhIjKlMnOp';
    humanWrites(F.id, `img-${cardsOf(A)[1].root_id}`, 'フォルダを変えても残る');
    db.prepare('UPDATE product_drafts SET drive_folder_url = ? WHERE id = ?').run(`https://drive.google.com/drive/folders/${FID2}`, A.id);
    const rM = await press(A);
    const newF = rowOf(A).file_id;
    ok(rM.status === 200 && rM.json.created === true && newF !== F.id && g.files.get(newF).parents.includes(FID2) && noteOfKey(newF, `img-${cardsOf(A)[1].root_id}`)?.note === 'フォルダを変えても残る' && !g.files.get(F.id).trashed,
      '🚨 画像フォルダを変えたら、新しいフォルダに作って前の依頼書の修正指示を引き継ぐ (Codex 名指し6 高)', JSON.stringify(rM.json));
    // 元に戻す (以降の試験は前の依頼書で続ける)
    db.prepare('UPDATE product_drafts SET drive_folder_url = ? WHERE id = ?').run(FOLDER, A.id);
    g.files.delete(newF);
    humanWrites(F.id, `img-${cardsOf(A)[1].root_id}`, '');
    db.prepare('UPDATE ph_designer_sheets SET file_id = ?, url = ? WHERE draft_id = ?').run(F.id, `https://docs.google.com/spreadsheets/d/${F.id}/edit`, A.id);
  }
  // 書いた後に記録できなかった (writing_at が立ったまま) → 作り直しが要る
  r = await press(A);
  ok(r.status === 200 && r.json.carried === 0, '(前提) 作り直せる', JSON.stringify(r.json));
  db.prepare("UPDATE ph_designer_sheets SET writing_at = '2026-10-09T00:00:00Z' WHERE draft_id = ?").run(A.id);
  st = await stateOf(A);
  ok(st.stale === true && st.interrupted === true, '書いた後に記録できなかった (writing_at が立ったまま) なら「作り直しが要る」を出す');
  r = await press(A);
  ok(r.status === 200 && rowOf(A).writing_at == null, '作り直して記録できたら下ろす');

  // DB に書く前に止まった (記録が無い) → 画像フォルダの印から拾い直す (2 つ作らない・書いた修正指示も残る)
  humanWrites(F.id, `img-${cards3[1].root_id}`, '拾い直しても残る');
  db.prepare('UPDATE ph_designer_sheets SET file_id = NULL, url = NULL WHERE draft_id = ?').run(A.id);
  r = await press(A);
  ok(r.status === 200 && sheetFiles().length === 1 && rowOf(A).file_id === F.id && noteOfKey(F.id, `img-${cards3[1].root_id}`).note === '拾い直しても残る',
    '🚨 記録の無いファイルは印 (appProperties) から拾い直す (2 つ作らない・修正指示も残る)');
  // 人が同じ名前で手作りしたシート (印なし) は拾わない / ごみ箱に入れた依頼書は作り直す
  g.files.set('SSHUMAN0001', { id: 'SSHUMAN0001', name: 'デザイナー修正依頼書_maitakep50', mimeType: 'application/vnd.google-apps.spreadsheet', parents: [FID], appProperties: {}, tabs: [{ sheetId: 0, title: ds.DESIGNER_TAB, values: [['人の']], formulas: [], meta: [] }], perms: [] });
  F.trashed = true;
  r = await press(A);
  const live = sheetFiles().filter((f) => !f.trashed && f.appProperties.phDesignerSheetDraft === String(A.id));
  ok(r.status === 200 && r.json.created === true && live.length === 1 && live[0].id !== F.id && g.files.get('SSHUMAN0001').tabs[0].values[0][0] === '人の',
    'ごみ箱に入れた依頼書は新しく作る・人が手作りした同じ名前のシートは触らない');
}

console.log('③ 共有ドライブの設定でリンク共有ができない');
{
  const B = makeComposed();
  finishJob(fullJob(B));
  const files = cardsOf(B).map((c) => c.current.drive_file_id);
  // 1 枚目は公開できて、2 枚目で断られる
  g.fail.pcreate = { err: gErr(403, 'The user does not have permission to share this item outside the shared drive.', 'teamDriveDomainUsersOnlyRestriction'), when: (p) => p.fileId === files[1], once: true };
  const nCreate = g.log.filter((x) => x === 'create').length;
  let r = await press(B);
  ok(r.status === 409 && r.json.code === 'share_blocked' && r.json.error.startsWith('共有ドライブの設定でリンク共有ができません') && /依頼書は作っていません/.test(r.json.error),
    '🚨 共有ドライブの設定で断られたら、理由 (共有ドライブの設定でリンク共有ができません) を返す', JSON.stringify(r.json));
  ok(g.log.filter((x) => x === 'create').length === nCreate && !rowOf(B).file_id, '🚨 依頼書を中途半端に作らない (ファイルを作らない・記録しない)');
  ok(!publicFiles().some((id) => files.includes(id)) && sharesOf(B).every((s) => s.revoked_at), '🚨 この回で付けた公開 (1 枚目) は外す');
  ok(evOf(B, 'designer_sheet_failed').length === 1, '失敗を履歴に残す');
  const st = await stateOf(B);
  ok(st.exists === false && st.blocked === null, '画面は作成前のまま (設定を直せばもう一度押せる)');
  g.fail.pcreate = { err: gErr(400, 'ACL change not allowed.', 'invalidSharingRequest'), once: true };
  r = await press(B);
  ok(r.status === 409 && r.json.code === 'share_blocked', 'Drive の文書の「ACL change not allowed」(400 invalidSharingRequest) も共有の設定とみなす');
  g.fail.pcreate = { err: gErr(403, 'Insufficient permissions for this file', 'insufficientFilePermissions'), once: true };
  r = await press(B);
  ok(r.status === 502 && r.json.code === 'google' && /コンテンツ管理者/.test(r.json.error), 'サービスアカウントの権限不足は別の理由 (コンテンツ管理者に)');
  g.fail.pcreate = { err: gErr(403, 'Rate limit exceeded', 'userRateLimitExceeded'), once: true };
  r = await press(B);
  ok(r.status === 502 && r.json.code === 'google' && /少し待って/.test(r.json.error) && !/共有ドライブの設定/.test(r.json.error), '403 でも回数の上限 (userRateLimitExceeded) は「少し待って」(共有ドライブの設定と言わない — Codex 名指し7 低)');
  g.fail.pcreate = { err: gErr(429, 'Rate Limit Exceeded'), once: true };
  r = await press(B);
  ok(r.status === 502 && /少し待って/.test(r.json.error), '回数の上限は「少し待って」');

  // 前から公開されていた画像 (人が付けた) は記録しない = 外さない
  g.files.get(files[0]).perms.push({ id: 'anyoneWithLink', type: 'anyone', role: 'reader' });
  r = await press(B);
  ok(r.status === 200 && !sharesOf(B).some((s) => s.drive_file_id === files[0] && !s.revoked_at), '前から公開されていたファイルはポータルが付けたものとして記録しない');
  const rj = regen(B, cardsOf(B)[0].head_id); finishJob(rj);
  r = await press(B);
  ok(r.status === 200 && g.files.get(files[0]).perms.some((x) => x.type === 'anyone'), '🚨 人が付けた公開は、依頼書から外れても外さない');
  // 外せなかった公開は記録に理由を残し、次に外し直す
  const cur0 = cardsOf(B)[0].current.drive_file_id;
  const rj2 = regen(B, cardsOf(B)[0].head_id); finishJob(rj2);
  g.fail.pdelete = { err: gErr(500, 'Backend Error'), once: true };
  r = await press(B);
  const s0 = sharesOf(B).find((s) => s.drive_file_id === cur0);
  ok(r.status === 200 && r.json.revoked === 0 && s0.revoked_at == null && /失敗/.test(s0.revoke_error || ''), '外せなかった公開は記録に理由を残す (依頼書はできている)');
  let stB = stateNow(B);
  ok(stB.stale === true && stB.unrevoked === 1 && stB.blocked === null, '🚨 外せなかった公開があれば、画像が同じでも「作り直す」を出す (次の作り直しが来なくても気づける — Codex 名指し1 高)');
  r = await press(B);
  stB = stateNow(B);
  ok(r.status === 200 && sharesOf(B).find((s) => s.drive_file_id === cur0).revoked_at != null && !publicFiles().includes(cur0) && stB.stale === false && stB.unrevoked === 0, '押せば外し直す (画像を作り直さなくても)');
  // 公開は付いたのに返事が来なかった (付けた直後に止まった) → この回で付けようとした公開は外す
  const rj4 = regen(B, cardsOf(B)[2].head_id); finishJob(rj4);
  const f4 = cardsOf(B)[2].current.drive_file_id;
  g.failAfterCreate = true;
  r = await press(B);
  ok(r.status === 502 && publicFiles().includes(f4) && sharesOf(B).filter((x) => x.drive_file_id === f4).every((x) => x.revoked_at && x.revoke_error === 'unknown_owner')
    && evOf(B, 'designer_sheet_share_unknown').length === 1,
    '🚨 公開が付いたのに返事が来なかった → 付ける前に記録していたので気づけるが、その間に人が付けたのかもしれないので外さず、履歴に残す (Codex 名指し6 中)', JSON.stringify([r, sharesOf(B).filter((x) => x.drive_file_id === f4)]));
  g.files.get(f4).perms = g.files.get(f4).perms.filter((x) => x.type !== 'anyone');   // (片付け) 人が Drive で外した
  // 前の回が「付けようとしていた行」(ID なし) のまま止まり、いま公開が付いている (止まった後に人が付けたのかもしれない)
  // → ポータルのものと決めつけない: 記録は閉じて履歴に残し、公開は外さない (Codex 名指し4 / base P2)
  g.files.get(f4).perms.push({ id: 'anyoneWithLink', type: 'anyone', role: 'reader' });
  db.prepare('INSERT INTO ph_designer_sheet_shares (draft_id, drive_file_id, permission_id, image_id, shared_by) VALUES (?, ?, NULL, NULL, ?)').run(B.id, f4, 't');
  r = await press(B);
  ok(r.status === 200 && sharesOf(B).filter((x) => x.drive_file_id === f4).every((x) => x.revoked_at && (x.permission_id == null)) && sharesOf(B).some((x) => x.drive_file_id === f4 && x.revoke_error === 'unknown_owner')
    && evOf(B, 'designer_sheet_share_unknown').length === 2, '🚨 前の回の「付けようとしていた行」と、いまある公開を結び付けない (持ち主の分からない公開として閉じ、履歴に残す)', JSON.stringify(sharesOf(B).filter((x) => x.drive_file_id === f4)));
  const rj5 = regen(B, cardsOf(B)[2].head_id); finishJob(rj5);
  r = await press(B);
  ok(r.status === 200 && publicFiles().includes(f4), '🚨 持ち主の分からない公開は、依頼書から外れても外さない (人が付けたものかもしれない)');
  // 片付けの側でも同じ: 前の回の ID なしの行は外さない
  const rjU = regen(B, cardsOf(B)[2].head_id); finishJob(rjU);
  const fU = cardsOf(B)[2].current.drive_file_id;
  g.files.get(fU).perms.push({ id: 'anyoneWithLink', type: 'anyone', role: 'reader' });
  db.prepare('INSERT INTO ph_designer_sheet_shares (draft_id, drive_file_id, permission_id, image_id, shared_by) VALUES (?, ?, NULL, NULL, ?)').run(B.id, fU, 't');
  const rjU2 = regen(B, cardsOf(B)[2].head_id); finishJob(rjU2);
  await svc.sweepDesignerShares({ db });
  ok(publicFiles().includes(fU) && sharesOf(B).some((x) => x.drive_file_id === fU && x.revoke_error === 'unknown_owner'), '起動のあとの片付けも、前の回の ID なしの行の公開は外さない');
  r = await press(B);
  ok(r.status === 200, '(前提) 作り直せる');
  // 権限の一覧の 2 ページ目に公開がある (共有ドライブは 1 ページ 100 件) → 見落とさない
  {
    // 依頼書に載っている (公開中の) 画像の権限が 150 件あり、公開 (anyone) は 2 ページ目
    const fp = cardsOf(B)[0].current.drive_file_id;
    const f = g.files.get(fp);
    f.perms = [...Array.from({ length: 150 }, (_, k) => ({ id: 'u' + k, type: 'user', role: 'reader' })), ...f.perms];
    const rowsBefore = sharesOf(B).filter((x) => x.drive_file_id === fp && !x.revoked_at).length;
    const nCreate = g.log.filter((x) => x === 'pcreate').length;
    r = await press(B);
    ok(r.status === 200 && g.log.includes('plist-page2') && g.log.filter((x) => x === 'pcreate').length === nCreate
      && sharesOf(B).filter((x) => x.drive_file_id === fp && !x.revoked_at).length === rowsBefore && rowsBefore === 1,
      '🚨 権限の一覧は全部のページを読む (2 ページ目の公開を見落として「公開が無い」と記録を閉じ、付け直さない — Codex 名指し4 高)', JSON.stringify(r.json));
    f.perms = f.perms.filter((x) => x.type !== 'user');
  }
  // 公開を外すと 404: その権限が無い (もう外れている) なら閉じる / 公開が残っているなら閉じない
  {
    const rjH = regen(B, cardsOf(B)[1].head_id); finishJob(rjH);
    const fh = db.prepare('SELECT drive_file_id FROM ph_designer_sheet_shares WHERE draft_id = ? AND revoked_at IS NULL AND drive_file_id NOT IN (SELECT ? ) ORDER BY id').all(B.id, cardsOf(B)[1].current.drive_file_id)
      .map((x) => x.drive_file_id).find((id) => !cardsOf(B).some((c) => c.current.drive_file_id === id));
    g.fail.pdelete = { err: gErr(404, 'File not found'), once: true };
    r = await press(B);
    const rowH = sharesOf(B).filter((x) => x.drive_file_id === fh).at(-1);
    ok(r.status === 200 && fh && rowH.revoked_at == null && /404/.test(rowH.revoke_error || '') && stateNow(B).unrevoked === 1,
      '🚨 削除が 404 でも公開が残っているなら閉じない (404 はファイルが見えないときにも返る — Codex 名指し4 中)', JSON.stringify(rowH));
    r = await press(B);
    ok(r.status === 200 && !publicFiles().includes(fh) && stateNow(B).unrevoked === 0, '押せば外し直す');
    // ファイルごと見えない (削除も一覧も 404) → 公開が残っているかもしれないので閉じない
    const rjV = regen(B, cardsOf(B)[1].head_id); finishJob(rjV);
    const fv = sharesOf(B).filter((x) => !x.revoked_at).map((x) => x.drive_file_id).find((id) => !cardsOf(B).some((c) => c.current.drive_file_id === id));
    g.fail.pdelete = { err: gErr(404, 'File not found'), when: (q) => q.fileId === fv, once: true };
    g.fail.plist = { err: gErr(404, 'File not found'), when: (q) => q.fileId === fv, once: true };
    r = await press(B);
    const rowV = sharesOf(B).filter((x) => x.drive_file_id === fv).at(-1);
    ok(r.status === 200 && fv && rowV.revoked_at == null && /見えない/.test(rowV.revoke_error || '') && stateNow(B).unrevoked === 1,
      '🚨 ファイルごと見えない 404 では記録を閉じない (外せていないまま。削除も止まり、片付けが外し直す — Codex base R5 P1)', JSON.stringify(rowV));
    const swV = await svc.sweepDesignerShares({ db });
    ok(swV.revoked >= 1 && !publicFiles().includes(fv) && stateNow(B).unrevoked === 0, '見えるようになれば片付けが外す');
  }
  // 1 枚ごとに印の期限を延ばす = 公開の途中で印を取られたら止める
  const rj6 = regen(B, cardsOf(B)[0].head_id); finishJob(rj6);
  const f6 = cardsOf(B)[0].current.drive_file_id;
  g.onPlist = () => { db.prepare("UPDATE ph_designer_sheets SET lease_token = 'other', lease_until = '2999-01-01T00:00:00Z' WHERE draft_id = ?").run(B.id); };
  const bP = g.batches;
  const nPC = g.log.filter((x) => x === 'plist').length;
  r = await press(B);
  const pcDuring = g.log.filter((x) => x === 'plist').length - nPC;
  db.prepare('UPDATE ph_designer_sheets SET lease_token = NULL, lease_until = NULL WHERE draft_id = ?').run(B.id);
  ok(r.status === 409 && /時間がかかりすぎた/.test(r.json.error) && g.batches === bP && !publicFiles().includes(f6) && pcDuring === 1, '🚨 公開の途中で印を別の処理に取られたら、残りの画像は公開せずに止める (1 枚ごとに印を確かめる・付けた公開は外す・書かない)', JSON.stringify(r.json));
}

{
  const E = makeComposed();
  finishJob(fullJob(E));
  const fe = cardsOf(E).map((c) => c.current.drive_file_id);
  // 前の回: 1 枚目を公開して記録した直後にプロセスが止まった (依頼書は無い)
  g.files.get(fe[0]).perms.push({ id: 'anyoneWithLink', type: 'anyone', role: 'reader' });
  db.prepare('INSERT INTO ph_designer_sheet_shares (draft_id, drive_file_id, permission_id, image_id, shared_by) VALUES (?, ?, ?, NULL, ?)').run(E.id, fe[0], 'anyoneWithLink', 't');
  const stE = stateNow(E);
  ok(stE.exists === false && stE.unrevoked === 1, '🚨 作成前でも、公開したままの画像 (作成が途中で止まった) を数える (Codex 名指し2 高)');
  // 今回は 2 枚目で断られる → 前の回の 1 枚目も含めて片付ける
  g.fail.pcreate = { err: gErr(403, 'Sharing outside the shared drive is not allowed', 'teamDriveDomainUsersOnlyRestriction'), when: (q) => q.fileId === fe[1], once: true };
  let rE = await press(E);
  ok(rE.status === 409 && rE.json.code === 'share_blocked' && !fe.some((f) => publicFiles().includes(f)) && stateNow(E).unrevoked === 0,
    '🚨 作れなかったときは、記録の依頼書に載っていない公開を全部外す (前の回が残した分も)', JSON.stringify(publicFiles().filter((f) => fe.includes(f))));
  // 前の回が公開した直後にプロセスごと止まり、だれも押さない → 起動のあとの片付けで外す
  {
    const E2 = makeComposed();
    finishJob(fullJob(E2));
    const f2 = cardsOf(E2)[0].current.drive_file_id;
    g.files.get(f2).perms.push({ id: 'anyoneWithLink', type: 'anyone', role: 'reader' });
    db.prepare('INSERT INTO ph_designer_sheet_shares (draft_id, drive_file_id, permission_id, image_id, shared_by) VALUES (?, ?, ?, NULL, ?)').run(E2.id, f2, 'anyoneWithLink', 't');
    // 作っている最中 (印が期限内) の商品は触らない
    db.prepare("INSERT OR IGNORE INTO ph_designer_sheets (draft_id) VALUES (?)").run(E2.id);
    db.prepare("UPDATE ph_designer_sheets SET lease_token = 'busy', lease_until = '2999-01-01T00:00:00Z' WHERE draft_id = ?").run(E2.id);
    let sw = await svc.sweepDesignerShares({ db });
    ok(publicFiles().includes(f2), '作っている最中の商品の公開は片付けない');
    db.prepare('UPDATE ph_designer_sheets SET lease_token = NULL, lease_until = NULL WHERE draft_id = ?').run(E2.id);
    // 書いている途中で止まった (writing_at) 商品も触らない (シートが新しい画像を出しているかもしれない)
    // 送った画像 (writing_images_json) = f2。シートはこれを出しているかもしれない
    db.prepare("UPDATE ph_designer_sheets SET writing_at = '2026-10-09T00:00:00Z', writing_images_json = ? WHERE draft_id = ?").run(JSON.stringify([f2]), E2.id);
    sw = await svc.sweepDesignerShares({ db });
    ok(publicFiles().includes(f2), '書いている途中で止まった商品でも、送った画像の公開は片付けない (シートが出しているかもしれない)');
    // 同じ商品の古い版 (記録の依頼書にも今の画像にも無い) の公開は外す
    const oldV = 'DRVOLDVER0001';
    g.files.set(oldV, { id: oldV, name: 'old.png', mimeType: 'image/png', parents: ['AIFOLDER'], perms: [{ id: 'anyoneWithLink', type: 'anyone', role: 'reader' }] });
    db.prepare('INSERT INTO ph_designer_sheet_shares (draft_id, drive_file_id, permission_id, image_id, shared_by) VALUES (?, ?, ?, NULL, ?)').run(E2.id, oldV, 'anyoneWithLink', 't');
    sw = await svc.sweepDesignerShares({ db });
    ok(!publicFiles().includes(oldV) && publicFiles().includes(f2), '🚨 書いている途中で止まった商品も、どちらの版のシートにも載っていない古い版の公開は外す (Codex 名指し5 高)');
    db.prepare('UPDATE ph_designer_sheets SET writing_at = NULL, writing_images_json = NULL WHERE draft_id = ?').run(E2.id);
    sw = await svc.sweepDesignerShares({ db });
    ok(!publicFiles().includes(f2) && sw.revoked >= 1 && stateNow(E2).unrevoked === 0 && rowOf(E2).lease_token == null,
      '🚨 起動のあとの片付けで、依頼書に載っていない公開を外す (だれも押さなくても — Codex 名指し3 高)', JSON.stringify(sw));
  }
  // 商品を消す: 公開を外してから消す。外せなければ消さない
  rE = await press(E);
  ok(rE.status === 200 && fe.every((f) => publicFiles().includes(f)), '(前提) 依頼書ができて 3 枚公開');
  db.prepare('UPDATE product_drafts SET source = ? WHERE id = ?').run(dbmod.SOURCE_NOTION_IMPORT, E.id);
  g.fail.pdelete = { err: gErr(500, 'Backend Error'), once: true };
  let del = await post(`/api/drafts/${E.id}/delete`, {});
  ok(del.status === 409 && /公開を外せませんでした/.test(del.json.error) && db.prepare('SELECT id FROM product_drafts WHERE id = ?').get(E.id), '🚨 商品を消す前に公開を外す — 外せなければ消さない (Codex 名指し2 高)', JSON.stringify(del));
  del = await post(`/api/drafts/${E.id}/delete`, {});
  ok(del.status === 200 && !fe.some((f) => publicFiles().includes(f)) && !db.prepare('SELECT id FROM product_drafts WHERE id = ?').get(E.id) && sharesOf(E).every((x) => x.revoked_at),
    '公開を外せたら商品を消す (公開の記録は残す)');
  const noShare = db.prepare(`INSERT INTO product_drafts (ne_code, name, source, created_by) VALUES ('DS-DEL', 'x', ?, 't')`).run(dbmod.SOURCE_NOTION_IMPORT).lastInsertRowid;
  const nLog = g.log.length;
  del = await post(`/api/drafts/${noShare}/delete`, {});
  ok(del.status === 200 && g.log.length === nLog, '公開が無い商品は Google に触らずに消す (今までどおり)');
}

console.log('④ 二重押し・2 人同時・待っている間の変化');
{
  const C = makeComposed();
  finishJob(fullJob(C));
  g.delay = 30;
  const st = await stateOf(C);
  const body = { seen_file_id: '', seen_images_hash: st.images_hash };
  const [r1, r2] = await Promise.all([post(`/api/drafts/${C.id}/designer-sheet`, body), post(`/api/drafts/${C.id}/designer-sheet`, body)]);
  g.delay = 0;
  const codes = [r1, r2].map((x) => x.status).sort();
  ok(codes.join() === '200,409' && [r1, r2].some((x) => x.json.code === 'busy') && sheetFiles().filter((f) => f.appProperties.phDesignerSheetDraft === String(C.id)).length === 1,
    '🚨 二重押し・2 人同時は 1 本だけ (2 本目は 409 busy・ファイルは 1 つ)', JSON.stringify([r1, r2].map((x) => x.json.code || x.status)));
  // 待っている間 (シートを読んだ直後) にほかの人が画像を作り直した → 書かない・記録しない
  const fileC = rowOf(C).file_id;
  const hashBefore = rowOf(C).images_hash;
  const rj = regen(C, cardsOf(C)[0].head_id); finishJob(rj);
  const b0 = g.batches;
  g.onSget = () => { const r = regen(C, cardsOf(C)[1].head_id); finishJob(r); };
  const r3 = await press(C);
  ok(r3.status === 409 && r3.json.code === 'conflict' && g.batches === b0 && rowOf(C).images_hash === hashBefore && rowOf(C).file_id === fileC,
    '🚨 待っている間に画像が変わったら書かない・記録しない (古い材料で上書きしない)', JSON.stringify(r3.json));
  ok(!publicFiles().includes(cardsOf(C)[0].current.drive_file_id), '書かなかった回で付けた公開は外す');
  const r4 = await press(C);
  ok(r4.status === 200 && rowOf(C).images_hash !== hashBefore, '読み直して押せば作り直せる');
  // シートを読んだ後・送る直前 (書き込み係がタブを読んだ回) にほかの人が作り直した → 送らない
  const rjA = regen(C, cardsOf(C)[0].head_id); finishJob(rjA);
  const hB = rowOf(C).images_hash; const bB = g.batches;
  g.onSgetNth = { n: 3, fn: () => { const r = regen(C, cardsOf(C)[1].head_id); finishJob(r); } };
  const rB = await press(C);
  ok(rB.status === 409 && rB.json.code === 'conflict' && g.batches === bB && rowOf(C).images_hash === hB && rowOf(C).writing_at == null,
    '🚨 送る直前 (書き込み係がタブを読んだ後) に画像が変わっても送らない', JSON.stringify([rB.json, g.onSgetNth]));
  // 書けた後・記録する前にほかの人が作り直した → 記録しない (作り直しが要るまま)
  g.onBatch = () => { const r = regen(C, cardsOf(C)[2].head_id); finishJob(r); };
  const rC = await press(C);
  ok(rC.status === 409 && rC.json.code === 'conflict' && rowOf(C).images_hash === hB && rowOf(C).writing_at != null && rC.json.designer_sheet?.stale === true,
    '🚨 書いた後・記録する前に画像が変わったら記録しない (書いている印は立てたまま = 作り直しが要る)', JSON.stringify(rC.json));
  ok(await press(C).then((x) => x.status === 200) && rowOf(C).writing_at == null, '読み直して押せば記録できる');
  // 印 (lease) を別の処理に取られた (時間切れ) → 書かない
  const rj5 = regen(C, cardsOf(C)[2].head_id); finishJob(rj5);
  const b1 = g.batches;
  g.onSget = () => { db.prepare("UPDATE ph_designer_sheets SET lease_token = 'other', lease_until = '2999-01-01T00:00:00Z' WHERE draft_id = ?").run(C.id); };
  const r5 = await press(C);
  ok(r5.status === 409 && /時間がかかりすぎた/.test(r5.json.error) && g.batches === b1, '🚨 時間切れで印を別の処理に取られたら書かない');
  db.prepare('UPDATE ph_designer_sheets SET lease_token = NULL, lease_until = NULL WHERE draft_id = ?').run(C.id);
  // Google の失敗 (書き込み) → 前のシートのまま・記録は変えない
  const hashNow6 = rowOf(C).images_hash;
  g.fail.sbatch = { err: gErr(500, 'Backend Error'), once: true };
  const r6 = await press(C);
  ok(r6.status === 502 && /作れませんでした/.test(r6.json.error) && rowOf(C).images_hash === hashNow6 && r6.json.designer_sheet?.stale === true, 'Google の失敗は 502 (記録の hash は変えない・作り直しが要るまま)', JSON.stringify(r6));
}

console.log('⑤ 詳細画面 (本番の描画)');
{
  const html = await (await fetch(`${base}/detail/${A.id}`)).text();
  ok(['id="dsheet"', 'id="dsheet-create"', 'id="dsheet-open"', 'id="dsheet-stale"', 'id="dsheet-update"', 'id="dsheet-msg"'].every((s) => html.includes(s)), '詳細画面に依頼書の箱 (作成・開く・作り直す)');
  ok(html.indexOf('id="dsheet"') > html.indexOf('id="lpi-grid"') && html.indexOf('id="dsheet"') < html.indexOf('id="ipf-step-manage"'), '置き場は生成した画像の箱 (#lpi) の下 (AI画像生成の段の中)');
  const lpiJson = JSON.parse((html.match(/<script type="application\/json" id="lpi-json">([\s\S]*?)<\/script>/) || [])[1] || 'null');
  ok(lpiJson && lpiJson.designer_sheet && lpiJson.designer_sheet.exists === true && lpiJson.designer_sheet.url, '最初の表示の状態 (lpi-json の designer_sheet)');
  const stale = /<div id="dsheet-stale" hidden style="([^"]*)"/.exec(html);
  ok(stale && !/display\s*:/.test(stale[1]), '「作り直す」の箱は hidden で消せる (自分に display を書かない)');
}

// ════════════════════════════════════════════════════════════════
console.log('⑥ 画面の JS (印の間を切り出して偽の document で動かす・送り先は本番の router)');
// ════════════════════════════════════════════════════════════════
{
  const src = fs.readFileSync(path.join(HERE, '..', 'apps', 'product-hub', 'views', 'detail.ejs'), 'utf8');
  const s0 = src.indexOf('/* @designer-sheet:start');
  const chunk = src.slice(s0, src.indexOf('/* @designer-sheet:end */'));
  ok(s0 > 0 && chunk.length > 1000 && !chunk.includes('<%'), '画面の JS を切り出せる (EJS の値を含まない)');
  const ui = new Function(chunk + '\nreturn { dsheetView, initDesignerSheet };')();
  const lpiChunk = src.slice(src.indexOf('/* @lp-image-ui:start'), src.indexOf('/* @lp-image-ui:end */'));
  const lpiUi = new Function(lpiChunk + '\nreturn { initLpImages };')();

  // 純粋関数
  const stBase = { can_edit: true, exists: false, url: null, file_id: null, title: 'デザイナー修正依頼書_X', at: null, by: null, count: null, stale: false, interrupted: false, blocked: null, images_hash: 'h'.repeat(64), total: 3, done: 3 };
  let v = ui.dsheetView({ ...stBase, blocked: '作れなかった画像があります (2枚目)。…' }, false);
  ok(v.showCreate && v.createDisabled && v.reason.startsWith('作れなかった画像') && v.meta.includes('全3枚'), '作成前・全部できていない: 押せない・理由を出す');
  v = ui.dsheetView(stBase, false);
  ok(v.showCreate && !v.createDisabled && v.createLabel === 'デザイナー修正依頼書を作成' && !v.openUrl && !v.showStale, '作成前・全部できた: 「デザイナー修正依頼書を作成」を押せる');
  v = ui.dsheetView(stBase, true);
  ok(v.createDisabled && v.createLabel === '作成中…', '送っている途中は押せない');
  v = ui.dsheetView({ ...stBase, exists: true, url: 'https://docs.google.com/spreadsheets/d/S/edit', at: '2026-10-09T01:02:03Z', by: 'a@x', count: 3 }, false);
  ok(!v.showCreate && v.openUrl.endsWith('/edit') && v.showStale && !v.staleWarn && v.updateLabel === '作り直す' && v.meta.startsWith('2026-10-09 01:02 作成 (a@x) ・ 3枚') && v.title === 'デザイナー修正依頼書_X' && v.reason === '', '作成済み・最新: 「開く ↗」と、目立たない「作り直す」(Drive で消した・移したときに直せる)');
  v = ui.dsheetView({ ...stBase, exists: true, url: 'u', stale: true, unrevoked: 2 }, false);
  ok(v.staleWarn && v.staleText.includes('2 枚の公開') && v.updateLabel === '最新の画像で作り直す', '依頼書から外れた画像の公開を外せていなければ出す');
  v = ui.dsheetView({ ...stBase, exists: true, url: 'u', stale: true }, false);
  ok(v.showStale && !v.updateDisabled && v.staleText === '作成後に作り直した画像があります。' && v.updateLabel === '最新の画像で作り直す', '作成後に作り直した画像があれば「最新の画像で作り直す」');
  v = ui.dsheetView({ ...stBase, exists: true, url: 'u', stale: true, blocked: '作り直している画像があります。…' }, false);
  ok(v.updateDisabled && v.reason.startsWith('作り直している'), '作り直している途中は「作り直す」を押せない・理由');
  v = ui.dsheetView({ ...stBase, can_edit: false }, false);
  ok(v.createDisabled && /画像登録者/.test(v.reason), '押せない人には理由');
  ok(ui.dsheetView(null, false).hidden === true, '状態が無ければ出さない');

  // 偽の document
  const mk = (tag) => ({ tagName: String(tag).toUpperCase(), children: [], style: {}, dataset: {}, hidden: false, disabled: false, listeners: {}, _text: '', href: '', title: '',
    get textContent() { return this._text + this.children.map((c) => c.textContent).join(''); },
    set textContent(x) { this._text = String(x); this.children = []; },
    appendChild(c) { this.children.push(c); return c; },
    addEventListener(tp, fn) { (this.listeners[tp] = this.listeners[tp] || []).push(fn); },
    async click() { for (const fn of this.listeners.click || []) await fn(); },
    count(tp) { return (this.listeners[tp] || []).length; } });
  const ids = ['dsheet', 'lpi-json', 'dsheet-title', 'dsheet-meta', 'dsheet-create', 'dsheet-open', 'dsheet-stale', 'dsheet-stale-text', 'dsheet-update', 'dsheet-msg',
    'lpi', 'lpi-btn', 'lpi-status', 'lpi-meta', 'lpi-grid', 'lpi-checks', 'lpi-head', 'lpi-count', 'lpi-checked', 'lpi-folder', 'lpi-stale'];
  const D = makeComposed();
  const els = Object.fromEntries(ids.map((id) => [id, mk('div')]));
  els.dsheet.dataset.draftId = String(D.id);
  els.lpi.dataset.draftId = String(D.id);
  const doc = { getElementById: (id) => els[id] || null, createElement: mk };
  const sent = []; let refreshes = 0; let net = 'ok';
  const fetchJson = async (url, opt) => {
    sent.push([url, opt ? opt.body : null]);
    if (net === 'throw') throw new Error('net');
    const res = await fetch(`http://127.0.0.1:${server.address().port}` + url, opt && opt.method === 'POST' ? { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(opt.body) } : {});
    return { status: res.status, json: await res.json() };
  };
  const loadJson = async () => { els['lpi-json']._text = JSON.stringify(await (await fetch(`${base}/api/drafts/${D.id}/lp-images`)).json()); };
  // 画像がまだ: 押せない
  const j = fullJob(D);
  els['lpi-json']._text = JSON.stringify({ designer_sheet: stateNow(D) });
  let ctl = ui.initDesignerSheet(doc, { fetchJson, refresh: () => { refreshes++; } });
  ok(!els.dsheet.hidden && els['dsheet-create'].disabled && !els['dsheet-create'].hidden && /いま画像を作っています/.test(els['dsheet-msg'].textContent) && els['dsheet-open'].hidden,
    '画面: 画像を作っている間は「作成」を押せない・理由を出す');
  await els['dsheet-create'].click();
  ok(sent.length === 0, '押せないボタンは送らない');
  // 画像ができた → 画像の箱のポーリング (onState) で入れ直されて押せる
  finishJob(j);
  const lpiCtl = lpiUi.initLpImages(doc, { fetchJson, confirm: () => false, setTimer: () => 0, clearTimer: () => {}, newKey: () => 'k-0001', isHidden: () => false,
    onState: (s) => ctl.update(s.designer_sheet) });
  await lpiCtl.poll();
  ok(!els['dsheet-create'].disabled && els['dsheet-msg'].textContent === '' && els['dsheet-meta'].textContent.includes('全3枚'), '🚨 画像の箱が取り直した状態で入れ直す (全部できたら押せる)');
  // 作る (二重押しは 1 回)
  const p1 = els['dsheet-create'].click();
  const p2 = els['dsheet-create'].click();
  ok(els['dsheet-create'].disabled && els['dsheet-create'].textContent === '作成中…', '送っている間は 作成中… で押せない');
  await Promise.all([p1, p2]);
  const posts = sent.filter((x) => x[1]);
  ok(posts.length === 1 && posts[0][0] === `/apps/product-hub/api/drafts/${D.id}/designer-sheet` && posts[0][1].seen_file_id === '' && /^[0-9a-f]{64}$/.test(posts[0][1].seen_images_hash),
    '🚨 二重押しでも送るのは 1 回・画面が見ていた依頼書 (無し) と画像の並びを添える', JSON.stringify(posts));
  const fileD = rowOf(D).file_id;
  ok(fileD && els['dsheet-create'].hidden && !els['dsheet-open'].hidden && els['dsheet-open'].href === `https://docs.google.com/spreadsheets/d/${fileD}/edit`
    && els['dsheet-title'].textContent === `デザイナー修正依頼書_${D.ne_code}` && els['dsheet-msg'].textContent.startsWith('デザイナー修正依頼書を作りました') && els['dsheet-stale'].dataset.warn === '0'
    && !els['dsheet-stale'].hidden && els['dsheet-update'].textContent === '作り直す' && els['dsheet-stale-text'].textContent.includes('Drive で消した・移した'),
    '作れたら「開く ↗」とファイル名・作成日時を出す (読み直さずに)');
  // 1 枚作り直す → ポーリングで「最新の画像で作り直す」
  humanWrites(fileD, `img-${cardsOf(D)[0].root_id}`, 'もっと明るく');
  const rj = regen(D, cardsOf(D)[0].head_id); finishJob(rj);
  await lpiCtl.poll();
  ok(els['dsheet-stale'].dataset.warn === '1' && els['dsheet-update'].textContent === '最新の画像で作り直す' && els['dsheet-stale-text'].textContent === '作成後に作り直した画像があります。' && !els['dsheet-update'].disabled, '🚨 作成後に作り直した画像があれば「最新の画像で作り直す」を出す (ポーリングで)');
  await els['dsheet-update'].click();
  const last = sent.filter((x) => x[1]).at(-1);
  ok(last[1].seen_file_id === fileD && els['dsheet-stale'].dataset.warn === '0' && /最新の画像で作り直しました \(書いてあった修正指示 1 件は同じ画像の行に残しました\)/.test(els['dsheet-msg'].textContent),
    '作り直す: 見ていた依頼書を添え、残した修正指示の数を出す', els['dsheet-msg'].textContent);
  ok(noteOfKey(fileD, `img-${cardsOf(D)[0].root_id}`).note === 'もっと明るく', '(本番の経路) 修正指示は残っている');
  // 古い画面: 別のタブで作り直された後に押す → 409 の理由・画像の箱を取り直す
  const old = ctl.state();
  const rj2 = regen(D, cardsOf(D)[1].head_id); finishJob(rj2);
  ctl.update({ ...old, stale: true });
  await els['dsheet-update'].click();
  ok(/作れませんでした: 画面を開いた後で画像が変わりました/.test(els['dsheet-msg'].textContent) && refreshes === 1, '🚨 古い画面から押すと 409 の理由を出し、画像の箱を取り直す');
  ok(ctl.state().images_hash !== old.images_hash, '断られた応答の最新の状態で入れ直す (次は今の画像で押せる)');
  // 通信断
  net = 'throw';
  await els['dsheet-update'].click();
  net = 'ok';
  ok(/通信できませんでした/.test(els['dsheet-msg'].textContent) && !els['dsheet-update'].disabled, '通信断は理由を出して押せる状態に戻す');
  ok(els['dsheet-create'].count('click') === 1 && els['dsheet-update'].count('click') === 1, 'ボタンの処理は 1 つずつ');
  ctl.update(undefined);
  ok(ctl.state() && ctl.state().exists, '古い応答 (designer_sheet が無い) では何もしない');
  // 押せない人の画面
  els['lpi-json']._text = JSON.stringify({ designer_sheet: { ...stBase, can_edit: false } });
  ctl = ui.initDesignerSheet(doc, { fetchJson: async () => { throw new Error('送らない'); } });
  ok(els['dsheet-create'].disabled && /画像登録者/.test(els['dsheet-msg'].textContent), '役割の無い人の画面: 押せない形で理由を出す');
  els['lpi-json']._text = JSON.stringify({ enabled: true });
  ctl = ui.initDesignerSheet(doc, { fetchJson });
  ok(els.dsheet.hidden === true, '状態が無い (古いサーバ) なら箱を出さない');
}

console.log('⑦ DB の移行 (PR-F の途中の版で作った表)');
{
  db.exec(`ALTER TABLE ph_designer_sheet_shares RENAME TO tmp_shares;
    CREATE TABLE ph_designer_sheet_shares (id INTEGER PRIMARY KEY AUTOINCREMENT, draft_id INTEGER NOT NULL, drive_file_id TEXT NOT NULL, permission_id TEXT NOT NULL,
      image_id INTEGER, shared_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')), shared_by TEXT, revoked_at TEXT, revoke_error TEXT);
    INSERT INTO ph_designer_sheet_shares SELECT * FROM tmp_shares WHERE permission_id IS NOT NULL;
    DROP TABLE tmp_shares;
    ALTER TABLE ph_designer_sheets DROP COLUMN writing_images_json;`);
  const nBefore = db.prepare('SELECT COUNT(*) AS n FROM ph_designer_sheet_shares').get().n;
  dbmod.migrateDesignerSheetTables(db);
  dbmod.migrateDesignerSheetTables(db);
  const pid = db.prepare('PRAGMA table_info(ph_designer_sheet_shares)').all().find((c) => c.name === 'permission_id');
  const cols = db.prepare('PRAGMA table_info(ph_designer_sheets)').all().map((c) => c.name);
  ok(pid && pid.notnull === 0 && cols.includes('writing_images_json') && db.prepare('SELECT COUNT(*) AS n FROM ph_designer_sheet_shares').get().n === nBefore && nBefore > 0,
    '🚨 途中の版の表 (permission_id NOT NULL・writing_images_json なし) を今の形にそろえる・記録は残す・何度呼んでもよい (Codex 名指し7 中)');
  db.prepare('INSERT INTO ph_designer_sheet_shares (draft_id, drive_file_id, permission_id) VALUES (1, ?, NULL)').run('DRVMIGRATE001');
  ok(true, '移行の後は ID なしの行を入れられる');
}

server.close();
svc.__setDesignerSheetClientsForTest(null);
console.log(`\n${fail === 0 ? '✅' : '❌'} ${pass} 件成功 / ${fail} 件失敗`);
process.exit(fail === 0 ? 0 : 1);
