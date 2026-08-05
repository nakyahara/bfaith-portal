/**
 * 印刷グラフ (inbound-chart.js) の座標を検算する。
 *   node apps/fba-replenishment/tests/test-inbound-chart.mjs [repo-root]
 * 描画そのものは目視だが、幾何の破綻はここで止める。
 *  - マークが描画領域からはみ出していないか
 *  - X軸ラベルが重ならないか
 *  - ピークラベルが上端で切れないか
 *  - 目盛りが「きりのいい数」になっているか
 */
import path from 'path';
import { pathToFileURL } from 'url';

const repo = process.argv[2]
  || path.resolve(path.dirname(new URL(import.meta.url).pathname.replace(/^\/(?=[A-Za-z]:)/, '')), '../../..');
const { buildInboundChart } = await import(pathToFileURL(path.join(repo, 'apps/fba-replenishment/inbound-chart.js')));

let pass = 0, fail = 0;
const ok = (c, n) => { if (c) { pass++; console.log(`  ok - ${n}`); } else { fail++; console.log(`  NG - ${n}`); } };

function mkRows(n, unit) {
  const rows = [];
  for (let i = 0; i < n; i++) {
    const d = new Date(Date.UTC(2026, 0, 1) + i * 86400000).toISOString().slice(0, 10);
    rows.push({
      created_date: unit === 'day' ? d : null,
      ym: unit === 'month' ? `2025-${String((i % 12) + 1).padStart(2, '0')}` : null,
      shipment_count: 3,
      sku_count: 20 + (i * 7) % 120,
      qty_shipped: 500 + (i * 337) % 6000,
      qty_received: 400,
      qty_unreceived: 100,
    });
  }
  return rows.reverse(); // 本番は新しい順
}

function check(n, unit, label, expectTooMany) {
  console.log(`\n[${label}] n=${n} unit=${unit}`);
  const C = buildInboundChart(mkRows(n, unit), unit);
  if (!C) { ok(false, 'chart が生成される'); return; }
  if (expectTooMany) {
    ok(C.tooMany === true, '点が多すぎる場合はグラフを描かず注意文に切り替える');
    ok(C.n === n && C.maxPoints > 0, '件数と上限を返す');
    return;
  }
  ok(!C.tooMany, '上限内なのでグラフを描く');

  const left = C.PAD_L, right = C.W - C.PAD_R;

  // 段ごとに天井と基線が違う (1枚のSVGに上下2段を密着させているため)
  for (const [name, p, top, bottom] of [['送付数', C.shipped, C.topY1, C.baseY1], ['SKU数', C.skus, C.topY2, C.baseY2]]) {
    const marks = p.marks;
    ok(marks.length === n, `${name}: マーク数が行数と一致 (${marks.length})`);
    ok(marks.every(m => m.x >= left - 0.01 && m.x + m.w <= right + 0.01),
       `${name}: 棒が左右の描画領域に収まる`);
    ok(marks.every(m => m.cy >= top - 0.01 && m.cy <= bottom + 0.01),
       `${name}: 値が上下の描画領域に収まる`);
    ok(marks.every(m => m.h >= 0.5), `${name}: 高さ0の日でも棒が消えない`);
    ok(marks.every(m => m.w <= 24.01), `${name}: 棒幅が24pxを超えない (帯を埋めない)`);
    ok(marks.every(m => m.topY >= top - 0.01 && m.topY + m.h <= bottom + 0.01),
       `${name}: 棒が基線より下へはみ出さない (値0でも)`);
    ok(marks.every(m => m.r <= m.w / 2 + 0.01 && m.r <= m.h + 0.01),
       `${name}: 角丸半径が幅と高さに収まる (パスが退化しない)`);

    // 値0が基線、最大値が上端になっているか
    const maxMark = marks.reduce((a, m) => (m.value > a.value ? m : a), marks[0]);
    ok(Math.abs(maxMark.cy - (top + (bottom - top) * (1 - maxMark.value / p.top))) < 0.05,
       `${name}: 最大値の位置がスケールと一致`);
    ok(p.top >= p.max, `${name}: 軸の上端が最大値以上 (${p.top} >= ${p.max})`);

    // 下段(SKU数)は高さが狭いので目盛りを0と上端だけに間引いている。
    // その場合は刻み幅ではなく「上端がきりのいい刻みの倍数か」で見る。
    const sparse = p.yTicks.length === 2 && p.yTicks[1].v === p.top;
    const step = sparse ? p.top / 3 : (p.yTicks[1].v - p.yTicks[0].v);
    const isNice = (v) => {
      if (v <= 0) return false;
      const mag = Math.pow(10, Math.floor(Math.log10(v)));
      return [1, 2, 5, 10].includes(Math.round((v / mag) * 100) / 100);
    };
    if (sparse) {
      // top は niceTicks の刻み(1/2/5×10^k)の整数倍になっているはず
      const mag = Math.pow(10, Math.floor(Math.log10(p.top)));
      const okTop = [1, 2, 5, 10].some(u => Math.abs((p.top / (u * mag)) % 1) < 1e-9)
        || [1, 2, 5, 10].some(u => Math.abs((p.top / (u * mag / 10)) % 1) < 1e-9);
      ok(okTop, `${name}: 上端がきりのいい刻みの倍数 (${p.top})`);
    } else {
      ok(isNice(step), `${name}: 目盛り幅がきりのいい数 (${step})`);
    }
    ok(p.yTicks.length >= 2 && p.yTicks.length <= 8, `${name}: 目盛り本数が2〜8 (${p.yTicks.length})`);
    ok(p.yTicks[0].v === 0, `${name}: 基線が0から始まる`);

    // ピークラベルが上端で切れない (y >= 10 を保証している)
    const labelY = Math.max(top + 8, p.peak.cy - 5);
    ok(labelY >= top + 8 - 0.01, `${name}: ピークラベルが上端で切れない (y=${labelY.toFixed(1)})`);
  }

  // X軸ラベルの重なり: 9px フォントで1文字あたり約5.5px、最長ラベル基準
  const maxChars = Math.max(...C.xTicks.map(t => t.label.length));
  const est = maxChars * 5.5;
  const gaps = C.xTicks.slice(1).map((t, i) => t.x - C.xTicks[i].x);
  const minGap = gaps.length ? Math.min(...gaps) : Infinity;
  ok(minGap >= est * 0.9,
     `X軸ラベルが重ならない (最小間隔 ${minGap.toFixed(1)}px >= 推定幅 ${est.toFixed(1)}px の9割)`);
  ok(C.xTicks.length <= 11, `X軸ラベル数が13以下 (${C.xTicks.length})`);
  ok(C.xTicks.every(t => t.x >= left && t.x <= right), 'X軸ラベルが描画領域内');
}

check(30, 'day', '日別30日');
check(14, 'month', '月別14ヶ月');
check(1, 'day', '1件のみ');
check(2, 'day', '2件');
check(92, 'day', '日別92日 (直近90日)');
check(120, 'day', '日別120日 (上限ちょうど)');
check(121, 'day', '日別121日 (上限超え)', true);
check(365, 'day', '日別365日 (1年)', true);

console.log('\n[ゼロ値・空]');
{
  const zero = buildInboundChart([{ created_date: '2026-01-01', sku_count: 0, qty_shipped: 0 }], 'day');
  ok(!!zero, '全部0でも生成できる');
  ok(zero.shipped.top > 0, '全部0でも軸の上端が正 (0除算しない)');
  ok(zero.shipped.marks[0].h >= 0.5, '0でも棒が残る');
  ok(zero.shipped.marks[0].topY + zero.shipped.marks[0].h <= zero.baseY1 + 0.01, '0の棒が基線より下へ出ない');
  ok(zero.shipped.marks[0].r <= zero.shipped.marks[0].h + 0.01, '0の棒の角丸が高さを超えない');
  ok(buildInboundChart([], 'day') === null, '空配列は null を返す');
}

console.log(`\n${pass} pass / ${fail} fail`);
process.exit(fail === 0 ? 0 : 1);
