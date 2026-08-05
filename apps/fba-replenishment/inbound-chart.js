/**
 * FBA納品実績の印刷用グラフ — 座標計算だけを持つ純粋モジュール。
 * router から切り出してあるのは、幾何をテストから直接叩けるようにするため。
 */

/**
 * 印刷用グラフの座標を組む。
 *
 * 送付数(数千)とSKU数(数十〜百)はスケールが50倍違うので、1つの図に重ねると
 * 折れ線が底に張り付いて読めない。かといって2軸にすると「折れ線が棒を超えた」
 * のような実在しない関係が見えてしまう (89 SKU と 3,396個 を比べても意味がない)。
 *
 * そこで **1枚のSVGの中に上下2段** を置き、
 *   - 段の間を詰める (離すと同じ日が繋がって見えない)
 *   - X軸ラベルの位置に縦グリッドを上段の天井から下段の基線まで通す
 *   - 段名は見出し行を作らず、軸の内側に小さく置く
 * ことで「1つの図」として読めるようにする。
 *
 * @param {Array} rows - サマリ行 (新しい順で渡ってくる)
 * @param {string} unit - 'day' | 'month'
 */
export function buildInboundChart(rows, unit) {
  const W = 720;          // A4縦の印刷可能幅に収まる論理幅
  const PAD_L = 50;       // Y軸ラベルの幅
  const PAD_R = 12;
  const PAD_T = 14;
  const H1 = 108;         // 送付数の段 (主役なので広く取る)
  const GAP = 10;         // 段の隙間。詰めることで同じ日の対応が見える
  const H2 = 64;          // SKU数の段
  const XAXIS = 20;       // X軸ラベルの高さ
  const H = PAD_T + H1 + GAP + H2 + XAXIS;
  const MAX_BAR = 24;     // 太らせない (帯の余りは余白として残す)

  const data = [...rows].reverse(); // 左→右で時系列順に読ませる
  const n = data.length;
  if (n === 0) return null;

  const plotW = W - PAD_L - PAD_R;
  // 点が多すぎると棒が1px未満になり、紙の上では判別できない絵になる。
  // 無理に描くより「月別で見てください」と言う方が正しい。
  const MAX_POINTS = 120;
  if (n > MAX_POINTS) return { tooMany: true, n, maxPoints: MAX_POINTS };

  const band = plotW / n;
  // 帯を超えて太らせない (超えると隣とくっつき、両端は描画領域からはみ出す)
  const barW = Math.min(MAX_BAR, band, band * 0.62);

  const key = (r) => r.created_date || r.ym;
  const xCenter = (i) => PAD_L + band * i + band / 2;

  // 目盛りは「きりのいい数」に丸める (1/2/5 × 10^k)
  function niceTicks(max) {
    if (max <= 0) return { top: 1, ticks: [0, 1] };
    const rough = max / 4;
    const mag = Math.pow(10, Math.floor(Math.log10(rough)));
    const norm = rough / mag;
    const step = (norm <= 1 ? 1 : norm <= 2 ? 2 : norm <= 5 ? 5 : 10) * mag;
    const top = Math.ceil(max / step) * step;
    const ticks = [];
    for (let v = 0; v <= top + 1e-9; v += step) ticks.push(Math.round(v));
    return { top, ticks };
  }

  const topY1 = PAD_T;
  const baseY1 = PAD_T + H1;
  const topY2 = baseY1 + GAP;
  const baseY2 = topY2 + H2;

  /**
   * @param {Function} valueOf
   * @param {'bar'|'line'} kind
   * @param {number} plotTop  段の天井
   * @param {number} plotBase 段の基線
   * @param {boolean} sparseTicks 目盛りを0と上端だけにするか (下段は狭いので間引く)
   */
  function panel(valueOf, kind, plotTop, plotBase, sparseTicks) {
    const h = plotBase - plotTop;
    // スプレッドで渡すと件数次第で引数上限に当たるので reduce で求める
    const max = data.reduce((a, r) => Math.max(a, valueOf(r) || 0), 0);
    const { top, ticks } = niceTicks(max);
    const y = (v) => plotBase - (top === 0 ? 0 : (v / top) * h);

    const marks = data.map((r, i) => {
      const v = valueOf(r) || 0;
      const cy = y(v);
      // 棒: 高さ0でも消えないよう最低0.5px残す (その日は0だった、と読めるように)。
      // 上端 barTop は基線から高さを引いて出す。cy から作ると 0 のときに
      // 基線より下へ角丸がはみ出して自己交差する。
      const bh = Math.max(0.5, plotBase - cy);
      return {
        key: key(r),
        value: v,
        cx: +xCenter(i).toFixed(2),
        cy: +cy.toFixed(2),
        x: +(xCenter(i) - barW / 2).toFixed(2),
        w: +barW.toFixed(2),
        h: +bh.toFixed(2),
        topY: +(plotBase - bh).toFixed(2),
        // 角丸は幅と高さの両方に収める (細い棒・低い棒で破綻させない)
        r: +Math.min(3, barW / 2, bh).toFixed(2),
      };
    });

    // 最大値だけ直接ラベルを置く (全点に置くと読まれない)
    let peak = null;
    for (const m of marks) if (!peak || m.value > peak.value) peak = m;

    // 下段は高さが狭いので目盛りは0と上端だけ。線が詰まって読めなくなるため
    const shown = sparseTicks ? [0, top] : ticks;
    return {
      kind, max, top, marks, peak,
      base: plotBase,
      polyline: marks.map(m => `${m.cx},${m.cy}`).join(' '),
      yTicks: shown.map(v => ({ v, y: +y(v).toFixed(2), label: v.toLocaleString('ja-JP') })),
    };
  }

  // X軸ラベルは重ならない数だけ。日別で件数が多いと全部は置けない。
  // 末尾(最新)は必ず出したいが、等間隔の最後と近すぎると重なるのでその1つ前を落とす。
  const maxLabels = 10;
  const step = Math.max(1, Math.ceil(n / maxLabels));
  const idxs = [];
  for (let i = 0; i < n; i += step) idxs.push(i);
  if (idxs[idxs.length - 1] !== n - 1) {
    if (n - 1 - idxs[idxs.length - 1] < step * 0.7) idxs.pop();
    idxs.push(n - 1);
  }
  const xTicks = idxs.map(i => ({
    x: +xCenter(i).toFixed(2),
    // 日別は月日だけにして幅を稼ぐ (年は見出しに出ている)
    label: unit === 'month' ? key(data[i]) : String(key(data[i])).slice(5).replace('-', '/'),
  }));

  return {
    W, H, PAD_L, PAD_R, PAD_T,
    plotW, n,
    topY1, baseY1, topY2, baseY2,
    // 縦グリッドは上段の天井から下段の基線まで通す。これが無いと同じ日が縦に揃って見えない
    gridTop: topY1,
    gridBottom: baseY2,
    xTicks,
    shipped: panel(r => r.qty_shipped || 0, 'bar', topY1, baseY1, false),
    skus: panel(r => r.sku_count || 0, 'line', topY2, baseY2, true),
  };
}
