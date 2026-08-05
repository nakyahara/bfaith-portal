/**
 * FBA納品実績の印刷用グラフ — 座標計算だけを持つ純粋モジュール。
 * router から切り出してあるのは、幾何をテストから直接叩けるようにするため。
 */
/**
 * 印刷用グラフの座標を組む。
 *
 * 送付数(数千)とSKU数(数十〜百)はスケールが50倍違うので、1つの図に重ねると
 * 折れ線が底に張り付いて読めない。X軸を共有した上下2段にする
 * (2軸チャートは値の大小を誤読させるので採らない)。
 *
 * @param {Array} rows - サマリ行 (新しい順で渡ってくる)
 * @param {string} unit - 'day' | 'month'
 */
export function buildInboundChart(rows, unit) {
  const W = 720;          // A4縦の印刷可能幅に収まる論理幅
  const H = 170;          // 1段あたりの高さ
  const PAD_L = 54;       // Y軸ラベルの幅
  const PAD_R = 12;
  const PAD_T = 12;
  const PAD_B = 26;       // X軸ラベルの高さ
  const MAX_BAR = 24;     // 太らせない (帯の余りは余白として残す)

  const data = [...rows].reverse(); // 左→右で時系列順に読ませる
  const n = data.length;
  if (n === 0) return null;

  const plotW = W - PAD_L - PAD_R;
  const plotH = H - PAD_T - PAD_B;
  // 点が多すぎると棒が1px未満になり、紙の上では判別できない絵になる。
  // 無理に描くより「月別で見てください」と言う方が正しい。
  const MAX_POINTS = 120;
  if (n > MAX_POINTS) return { tooMany: true, n, maxPoints: MAX_POINTS };

  const band = plotW / n;
  // 帯を超えて太らせない (超えると隣とくっつき、両端は描画領域からはみ出す)
  const barW = Math.min(MAX_BAR, band, band * 0.7);

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

  function panel(valueOf, kind) {
    // スプレッドで渡すと件数次第で引数上限に当たるので reduce で求める
    const max = data.reduce((a, r) => Math.max(a, valueOf(r) || 0), 0);
    const { top, ticks } = niceTicks(max);
    const y = (v) => PAD_T + plotH - (top === 0 ? 0 : (v / top) * plotH);
    const baseY = PAD_T + plotH;

    const marks = data.map((r, i) => {
      const v = valueOf(r) || 0;
      const cy = y(v);
      // 棒: 高さ0でも消えないよう最低0.5px残す (その日は0だった、と読めるように)。
      // 上端 topY は基線から h を引いて出す。cy から作ると 0 のときに
      // 基線より下へ角丸がはみ出して自己交差する。
      const h = Math.max(0.5, baseY - cy);
      const topY = baseY - h;
      return {
        key: key(r),
        value: v,
        cx: +xCenter(i).toFixed(2),
        cy: +cy.toFixed(2),
        x: +(xCenter(i) - barW / 2).toFixed(2),
        w: +barW.toFixed(2),
        h: +h.toFixed(2),
        topY: +topY.toFixed(2),
        // 角丸は幅と高さの両方に収める (細い棒・低い棒で破綻させない)
        r: +Math.min(4, barW / 2, h).toFixed(2),
      };
    });

    // 最大値だけ直接ラベルを置く (全点に置くと読まれない)
    let peak = null;
    for (const m of marks) if (!peak || m.value > peak.value) peak = m;

    return {
      kind,
      max,
      top,
      marks,
      peak,
      polyline: marks.map(m => `${m.cx},${m.cy}`).join(' '),
      yTicks: ticks.map(v => ({ v, y: +y(v).toFixed(2), label: v.toLocaleString('ja-JP') })),
    };
  }

  // X軸ラベルは重ならない数だけ。日別で件数が多いと全部は置けない。
  // 末尾(最新)は必ず出したいが、等間隔の最後と近すぎると重なるのでその1つ前を落とす。
  const maxLabels = 12;
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
    W, H, PAD_L, PAD_R, PAD_T, PAD_B,
    plotW, plotH, n,
    baselineY: PAD_T + plotH,
    xTicks,
    shipped: panel(r => r.qty_shipped || 0, 'bar'),
    skus: panel(r => r.sku_count || 0, 'line'),
  };
}

