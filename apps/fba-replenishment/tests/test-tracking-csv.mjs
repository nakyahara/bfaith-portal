/**
 * tracking-csv.js のテスト (外部依存なし・本番データに触らない)
 *   node apps/fba-replenishment/tests/test-tracking-csv.mjs
 */
import assert from 'node:assert/strict';
import iconv from 'iconv-lite';
import {
  parseTrackingCsv, buildAssignments, checkShipDate,
  normTrackingId, normKanriNo, normCell,
} from '../tracking-csv.js';

let pass = 0;
const t = (name, fn) => { fn(); pass++; console.log(`  ok  ${name}`); };

// 実物と同じ34列・引用符混在 (個数と重量だけ裸) のCSVを組み立てる
const HEADER = ['登録日','出荷日','送り状番号','荷受人コード','荷受人郵便番号','荷受人電話番号','荷受人住所１','荷受人住所２','荷受人住所３','荷受人名前１','荷受人名前２','特殊計','着店コード','着店名','荷送人コード','荷送人郵便番号','荷送人電話番号','荷送人住所１','荷送人住所２','荷送人名前１','荷送人名前２','個数','重量','指定日','輸送商品１','輸送商品２','品名記事１','品名記事２','品名記事３','元着区分','保険金額','お客様管理番号','請求先コード','請求先部課所コード'];

function row({ ymd = '20260807', tracking, fc, qty = 1, kanri = '', name = 'ａｍａｚｏｎ．ｃｏ．ｊｐ' }) {
  const c = new Array(34).fill('');
  c[0] = ymd; c[1] = ymd; c[2] = tracking; c[3] = `'${fc}`; c[9] = name; c[31] = kanri ? `'${kanri}` : "'";
  // 個数(21)・重量(22) は引用符なしで出る
  return c.map((v, i) => (i === 21 ? String(qty) : i === 22 ? '0' : `"${v}"`)).join(',');
}
const csv = (rows) => iconv.encode([HEADER.map((h) => `"${h}"`).join(','), ...rows].join('\r\n'), 'CP932');

console.log('tracking-csv');

t('正規化: 送り状番号はハイフンを除いた数字になる', () => {
  assert.equal(normTrackingId('663-7913-4975').value, '66379134975');
  assert.equal(normTrackingId('６６３-７９１３-４９７５').value, '66379134975'); // 全角
  assert.equal(normTrackingId("'663-9387-3022").value, '66393873022');        // 先頭アポストロフィ
  assert.equal(normTrackingId('663–7913–4975').value, '66379134975');          // Unicodeハイフン
});

t('正規化: 数字とハイフン以外は削らずに弾く', () => {
  assert.equal(normTrackingId('AB-123-456').ok, false);
  assert.equal(normTrackingId('12345').ok, false); // 桁不足
  assert.equal(normTrackingId('').ok, false);
});

t('正規化: お客様管理番号は半角英数16文字まで・空欄は許容', () => {
  assert.equal(normKanriNo("'FBA15GG2MM9B").value, 'FBA15GG2MM9B');
  assert.equal(normKanriNo('').value, '');
  assert.equal(normKanriNo("'").value, '');
  assert.equal(normKanriNo('A'.repeat(17)).ok, false);
  assert.equal(normKanriNo('FBA-15GG').ok, false);
});

t('先頭ゼロを落とさない (文字列のまま扱う)', () => {
  assert.equal(normCell("'0648607868"), '0648607868');
  assert.equal(normTrackingId('006-3913-4975').value, '00639134975');
});

t('パース: 引用符ありなし混在でも列がずれない', () => {
  const { rows, problems } = parseTrackingCsv(csv([
    row({ tracking: '663-9387-3162', fc: 'HND2', kanri: 'FBA15GGL5J2X' }),
    row({ tracking: '663-9387-3173', fc: 'HND2', kanri: 'FBA15GGL5J2X' }),
  ]));
  assert.equal(problems.length, 0);
  assert.equal(rows.length, 2);
  assert.equal(rows[0].trackingId, '66393873162');
  assert.equal(rows[0].fcCode, 'HND2');
  assert.equal(rows[0].納品番号, 'FBA15GGL5J2X');
  assert.equal(rows[0].個数, 1);
});

t('パース: 列数が違うCSVは受け付けない', () => {
  assert.throws(() => parseTrackingCsv(iconv.encode('"a","b"\r\n"1","2"', 'CP932')), /列数が想定と違います/);
});

t('出荷日チェック: 前日のCSVが残っていたら弾く', () => {
  const { rows } = parseTrackingCsv(csv([row({ ymd: '20260806', tracking: '663-7913-4975', fc: 'HND2' })]));
  const r = checkShipDate(rows, '20260807');
  assert.equal(r.ok, false);
  assert.match(r.problem, /出荷日が期待/);
  assert.equal(checkShipDate(rows, '20260806').ok, true);
});

// ── 割り当て ──────────────────────────────────────────────
const SHIPMENTS = [
  { shipmentConfirmationId: 'FBA15GGL5J2X', shipmentId: 'sh-A', fcCode: 'HND2', boxIds: ['B1', 'B2'], hasTracking: false },
  { shipmentConfirmationId: 'FBA15GGLDVMG', shipmentId: 'sh-B', fcCode: 'XHD4', boxIds: ['C1', 'C2', 'C3'], hasTracking: false },
];

t('割り当て: 納品番号で一意に決まる (CSVの並び順=箱の順)', () => {
  const { rows } = parseTrackingCsv(csv([
    row({ tracking: '663-9387-3162', fc: 'HND2', kanri: 'FBA15GGL5J2X' }),
    row({ tracking: '663-9387-3173', fc: 'HND2', kanri: 'FBA15GGL5J2X' }),
    row({ tracking: '663-9387-3210', fc: 'XHD4', kanri: 'FBA15GGLDVMG' }),
    row({ tracking: '663-9387-3221', fc: 'XHD4', kanri: 'FBA15GGLDVMG' }),
    row({ tracking: '663-9387-3232', fc: 'XHD4', kanri: 'FBA15GGLDVMG' }),
  ]));
  const { assignments, problems } = buildAssignments(rows, SHIPMENTS);
  assert.equal(problems.length, 0);
  assert.equal(assignments.length, 2);
  const a = assignments.find((x) => x.shipmentConfirmationId === 'FBA15GGL5J2X');
  assert.equal(a.matchedBy, '納品番号');
  assert.deepEqual(a.items, [
    { boxId: 'B1', trackingId: '66393873162', 送り状番号: '663-9387-3162' },
    { boxId: 'B2', trackingId: '66393873173', 送り状番号: '663-9387-3173' },
  ]);
});

t('🚨箱数と伝票枚数が合わなければ problem を立てる (いろはの数え間違いを検知)', () => {
  const { rows } = parseTrackingCsv(csv([
    row({ tracking: '663-9387-3162', fc: 'HND2', kanri: 'FBA15GGL5J2X' }),
  ]));
  const { problems } = buildAssignments(rows, SHIPMENTS);
  assert.ok(problems.some((p) => /個口合計 1 と輸送箱 2 が一致しません/.test(p)), problems.join(' / '));
});

t('🚨管理番号が空なら既定では割り当てない (FCコードで推測しない)', () => {
  const ships = [{ shipmentConfirmationId: 'FBA-X1', shipmentId: 's1', fcCode: 'HND2', boxIds: ['B1'], hasTracking: false }];
  const { rows } = parseTrackingCsv(csv([row({ tracking: '663-9387-3162', fc: 'HND2' })]));
  const { assignments, problems } = buildAssignments(rows, ships); // 既定 = フォールバックOFF
  assert.equal(assignments.length, 0);
  assert.ok(problems.some((p) => /納品番号を入れてください/.test(p)), problems.join(' / '));
});

t('🚨同一FC宛の納品が複数あるときは、明示的に許可しても中断する', () => {
  const ships = [
    { shipmentConfirmationId: 'FBA-X1', shipmentId: 's1', fcCode: 'HND2', boxIds: ['B1'], hasTracking: false },
    { shipmentConfirmationId: 'FBA-X2', shipmentId: 's2', fcCode: 'HND2', boxIds: ['B2'], hasTracking: false },
  ];
  const { rows } = parseTrackingCsv(csv([row({ tracking: '663-9387-3162', fc: 'HND2' })]));
  const { assignments, problems } = buildAssignments(rows, ships, { allowFcFallback: true });
  assert.equal(assignments.length, 0);
  assert.ok(problems.some((p) => /どれに割り当てるか決まりません/.test(p)), problems.join(' / '));
});

t('明示的に許可し、FC宛の納品が1つだけなら割り当てる (移行期の手動実行用)', () => {
  const ships = [{ shipmentConfirmationId: 'FBA-X1', shipmentId: 's1', fcCode: 'HND2', boxIds: ['B1'], hasTracking: false }];
  const { rows } = parseTrackingCsv(csv([row({ tracking: '663-9387-3162', fc: 'HND2' })]));
  const { assignments } = buildAssignments(rows, ships, { allowFcFallback: true });
  assert.equal(assignments.length, 1);
  assert.equal(assignments[0].matchedBy, 'FCコード');
});

t('FBA以外の便 (amrc等) は除外として件数を残す', () => {
  const { rows } = parseTrackingCsv(csv([
    row({ tracking: '663-9387-3162', fc: 'HND2', kanri: 'FBA15GGL5J2X' }),
    row({ tracking: '663-9387-3173', fc: 'HND2', kanri: 'FBA15GGL5J2X' }),
    row({ tracking: '663-1111-2222', fc: 'AMRC', qty: 4, name: 'センコー株式会社' }),
  ]));
  const { excluded, problems } = buildAssignments(rows, [SHIPMENTS[0]]);
  assert.equal(problems.filter((p) => /AMRC/.test(p)).length, 0); // エラーにはしない
  const ex = excluded.find((e) => e.fcCode === 'AMRC');
  assert.ok(ex, JSON.stringify(excluded));
  assert.equal(ex.件数, 4); // 個数分に展開して数える
});

t('登録済みの納品はスキップする (二重投入しない)', () => {
  const ships = [{ ...SHIPMENTS[0], hasTracking: true }];
  const { rows } = parseTrackingCsv(csv([row({ tracking: '663-9387-3162', fc: 'HND2', kanri: 'FBA15GGL5J2X' })]));
  const { assignments, skipped } = buildAssignments(rows, ships);
  assert.equal(assignments.length, 0);
  assert.equal(skipped[0].reason, '登録済み');
});

t('🚨同じ送り状番号が別の宛先に現れたら取り違えを疑う', () => {
  const { rows } = parseTrackingCsv(csv([
    row({ tracking: '663-9387-3162', fc: 'HND2', kanri: 'FBA15GGL5J2X' }),
    row({ tracking: '663-9387-3162', fc: 'XHD4', kanri: 'FBA15GGLDVMG' }),
  ]));
  const { problems } = buildAssignments(rows, SHIPMENTS);
  assert.ok(problems.some((p) => /複数の宛先に現れます/.test(p)), problems.join(' / '));
});

t('1送り状=複数個口は個数分に展開される', () => {
  const ships = [{ shipmentConfirmationId: 'FBAY00000001', shipmentId: 'sY', fcCode: 'HND2', boxIds: ['B1', 'B2', 'B3'], hasTracking: false }];
  const { rows } = parseTrackingCsv(csv([row({ tracking: '663-9387-3162', fc: 'HND2', qty: 3, kanri: 'FBAY00000001' })]));
  const { assignments, problems } = buildAssignments(rows, ships);
  assert.equal(problems.length, 0);
  assert.deepEqual(assignments[0].items.map((i) => i.trackingId), ['66393873162', '66393873162', '66393873162']);
});

t('🚨投入待ちの納品がCSVに1行も無ければ中断する (納品ごと数え漏らし)', () => {
  // HND2 の行しか無いのに、XHD4 の納品も投入待ち
  const { rows } = parseTrackingCsv(csv([
    row({ tracking: '663-9387-3162', fc: 'HND2', kanri: 'FBA15GGL5J2X' }),
    row({ tracking: '663-9387-3173', fc: 'HND2', kanri: 'FBA15GGL5J2X' }),
  ]));
  const { problems } = buildAssignments(rows, SHIPMENTS);
  assert.ok(
    problems.some((p) => p.includes('FBA15GGLDVMG') && p.includes('送り状がCSVにありません')),
    problems.join(' / '),
  );
});

t('🚨CSVの管理番号に対応する納品が無ければ中断する (転記違い・別プランの混入)', () => {
  const { rows } = parseTrackingCsv(csv([
    row({ tracking: '663-9387-3162', fc: 'HND2', kanri: 'FBA15GGL5J2X' }),
    row({ tracking: '663-9387-3173', fc: 'HND2', kanri: 'FBA15GGL5J2X' }),
    row({ tracking: '663-9999-0001', fc: 'HND2', kanri: 'FBA15WRONG01' }),
  ]));
  const { problems } = buildAssignments(rows, [SHIPMENTS[0]]);
  assert.ok(
    problems.some((p) => p.includes('FBA15WRONG01') && p.includes('対応する納品が見つかりません')),
    problems.join(' / '),
  );
});

console.log(`\n${pass} 件すべて通過`);
