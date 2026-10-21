/*
 * me-sales-chart.js — 1 つの商品の画面の「販売数のグラフ」(10/17 中原さん。読むだけ)
 *   - GET /api/sku/:code/sales-chart (週 52・月 24・日 90 の数をまとめて 1 回で読む) → 切り替えは読み直さない
 *   - 時間切れ (8 秒) = 言葉と「もう一度読む (時間がかかることがあります)」= ?wait=long (30 秒まで待つ・10/19)
 *   - 上 = モール別の積み上げの棒 (楽天・Amazon・Yahoo!・その他)。下 = ロジザードの在庫の線 (単品だけ・別のグラフ = 軸を 2 つ重ねない)
 *   - 在庫 0 の日 (欠品) がある期間は両方のグラフで帯を付ける (凡例に「欠品」の言葉)。いちばん新しい期間 (途中) は薄い棒
 *   - 値は棒にかざす / グラフを選んで ← → で見る (同じ中身を読み上げにも)。表 (表で見る) にも全部ある = ツールチップだけに頼らない
 *   - 名前・数は textContent で入れる (innerHTML に値を混ぜない)。幅はグラフの箱に合わせて描き直す (スマホ 390 でもはみ出さない)
 *   - 🆕 10/19: 画面のタブ (me-tabs.js)「販売数のグラフ」を初めて開いたときに読む (隠れたタブのまま読まない = 画面を開くのが速い)。
 *     読んでいる間に別のタブへ移った = 隠れたまま描き、開き直したら箱の幅で描き直す (ResizeObserver)
 */
(function () {
  'use strict';
  var box = document.getElementById('p-sales');
  if (!box) return;
  var BASE = box.getAttribute('data-base') || '';
  var CODE = box.getAttribute('data-code') || '';
  var SVGNS = 'http://www.w3.org/2000/svg';
  var $ = function (id) { return document.getElementById(id); };
  var sumEl = $('sc-sum'), legendEl = $('sc-legend'), chartEl = $('sc-chart'), salesEl = $('sc-sales'), stockEl = $('sc-stock'), stockHead = $('sc-stock-h');
  var tip = $('sc-tip'), live = $('sc-live'), tableBox = $('sc-table-box'), thead = $('sc-thead'), rowsEl = $('sc-rows'), unitSeg = $('sc-unit');
  var retryBox = $('sc-retry'), retryBtn = $('sc-retry-btn'), waitEl = $('sc-wait');
  var data = null, unit = 'week', active = -1, lastW = 0;
  var SER = [{ key: 'rakuten', label: '楽天' }, { key: 'amazon', label: 'Amazon' }, { key: 'yahoo', label: 'Yahoo!' }, { key: 'other', label: 'その他' }];
  var UNIT = { week: { n: '52 週', per: '週', avg: '週平均' }, month: { n: '24 か月', per: '月', avg: '月平均' }, day: { n: '90 日', per: '日', avg: '1 日平均' } };
  var WD = ['日', '月', '火', '水', '木', '金', '土'];

  function num(v) { return v == null ? '' : Number(v).toLocaleString('ja-JP'); }
  function avg(v) { return (Math.round(v * 10) / 10).toLocaleString('ja-JP'); }
  var compact = (typeof Intl !== 'undefined' && Intl.NumberFormat) ? new Intl.NumberFormat('ja-JP', { notation: 'compact', maximumFractionDigits: 1 }) : null;
  function tickText(v) { return v >= 10000 && compact ? compact.format(v) : Number(v).toLocaleString('ja-JP'); }
  function ymd(s) { var p = String(s).split('-'); return { y: +p[0], m: +p[1], d: +p[2] }; }
  function md(s) { var x = ymd(s); return x.m + '/' + x.d; }
  function full(s) { var x = ymd(s); return x.y + '/' + x.m + '/' + x.d; }
  function wd(s) { var x = ymd(s); return WD[new Date(Date.UTC(x.y, x.m - 1, x.d)).getUTCDay()]; }
  /** 期間の名前 (ツールチップ・表) */
  function periodLabel(b) {
    if (unit === 'day') return full(b.from) + ' (' + wd(b.from) + ')';
    if (unit === 'month') { var x = ymd(b.from); return x.y + '年' + x.m + '月'; }
    var f = ymd(b.from), t = ymd(b.to);
    return full(b.from) + '〜' + (f.y === t.y ? md(b.to) : full(b.to));
  }
  function el(name, attrs, parent) {
    var e = document.createElementNS(SVGNS, name);
    for (var k in attrs) if (Object.prototype.hasOwnProperty.call(attrs, k)) e.setAttribute(k, attrs[k]);
    if (parent) parent.appendChild(e);
    return e;
  }
  function html(tag, cls, text, parent) {
    var e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text != null) e.textContent = text;
    if (parent) parent.appendChild(e);
    return e;
  }
  /** 目盛りのきりのいい上限と間隔 (目盛りは 4 本まで) */
  function niceScale(max) {
    if (!(max > 0)) return { max: 4, step: 1 };
    var raw = max / 4, p = Math.pow(10, Math.floor(Math.log(raw) / Math.LN10)), step = p;
    [1, 2, 2.5, 5, 10].some(function (m) { step = m * p; return step >= raw; });
    step = Math.max(1, Math.ceil(step));
    return { max: Math.ceil(max / step) * step, step: step };
  }
  function api(path) {
    return fetch(BASE + path, { headers: { Accept: 'application/json' }, credentials: 'same-origin' })
      .then(function (r) {
        return r.json().catch(function () { return { ok: false, error: '読めませんでした (' + r.status + ')' }; }).then(function (j) {
          if (j && !j.ok && !j.reason && r.status === 504) j.reason = 'timeout';   // 手前の中継の時間切れ (JSON でも JSON でなくても) = サーバーの時間切れと同じ扱い
          return j;
        });
      })
      .catch(function () { return { ok: false, reason: 'network', error: 'つながりませんでした。少し待ってから画面を開き直してください' }; });
  }

  function view() { return data.views[unit]; }
  function hasStock(v) { return data.kind === 'single' && v.buckets.some(function (b) { return b.stock; }); }

  // ── 見出しのまとめ (読み上げの説明にも使う)
  function summary(v) {
    var whole = v.buckets.filter(function (b) { return !b.noData && !b.partial && !b.partialData; });
    var withData = v.buckets.filter(function (b) { return !b.noData; });
    var t = [];
    if (!withData.length) return 'この期間は注文の記録の前です (注文は 2025-01 から)';
    if (!v.totals.total) t.push('直近 ' + UNIT[unit].n + 'に売れた記録はありません');
    else {
      var s = '直近 ' + UNIT[unit].n + 'の合計 ' + num(v.totals.total) + ' 個';
      if (whole.length) s += ' (' + UNIT[unit].avg + ' ' + avg(whole.reduce(function (a, b) { return a + b.total; }, 0) / whole.length) + " 個" + (whole.length < withData.length ? '・途中の' + UNIT[unit].per + 'を除く' : '') + ')';
      t.push(s);
      var top = withData.reduce(function (a, b) { return b.total > a.total ? b : a; });
      t.push('いちばん多い' + UNIT[unit].per + ' ' + periodLabel(top) + ' の ' + num(top.total) + ' 個');
      if (v.totals.viaSet) t.push('うちセット経由 ' + num(v.totals.viaSet) + ' 個');
    }
    if (data.kind === 'single') {
      var st = v.buckets.filter(function (b) { return b.stock; });
      if (st.length) {
        var last = st[st.length - 1].stock, zero = st.reduce(function (a, b) { return a + b.stock.zeroDays; }, 0);
        t.push('在庫 ' + full(last.endDay) + ' の締め ' + num(last.end) + ' 個' + (zero ? ' · 欠品 (在庫 0) の日 ' + zero + ' 日' : ''));
      } else t.push('在庫の線はロジザードの毎日の在庫がある日だけ (' + (data.stockFrom ? full(data.stockFrom) + ' から' : 'この商品はまだ記録がありません') + ')');
    }
    return t.join(' · ');
  }

  // ── 凡例 (棒の色 + その期間の合計 = 色だけに頼らない)
  function legend(v) {
    legendEl.textContent = '';
    SER.forEach(function (s, i) {
      var li = html('li', null, null, legendEl);
      html('span', 'sw sc-c' + (i + 1), null, li).setAttribute('aria-hidden', 'true');
      html('span', null, s.label + (s.key === 'other' ? ' (' + (data.otherMalls || '') + ')' : ''), li);
      html('b', null, num(v.totals[s.key]), li);
    });
    if (hasStock(v)) { var a = html('li', null, null, legendEl); html('span', 'ln', null, a).setAttribute('aria-hidden', 'true'); html('span', null, '在庫 (下のグラフ)', a); }
    if (v.buckets.some(function (b) { return b.stock && b.stock.zeroDays; })) { var o = html('li', null, null, legendEl); html('span', 'sw out', null, o).setAttribute('aria-hidden', 'true'); html('span', null, '欠品 (在庫 0) の日がある' + UNIT[unit].per, o); }
    if (v.buckets.some(function (b) { return b.partial; })) { var p = html('li', null, null, legendEl); html('span', 'sw part', null, p).setAttribute('aria-hidden', 'true'); html('span', null, '薄い棒 = 途中の' + UNIT[unit].per + ' (前日まで)', p); }
    legendEl.hidden = false;
  }

  // ── x の目盛り: 週・日 = 月の変わり目 (日は 15 日も)・月 = 1 月にそろえて間引く。重なる分は出さない
  function xLabels(v, slot) {
    var out = [], minGap = 44, lastX = -1e9, prevM = null;
    v.buckets.forEach(function (b, i) {
      var f = ymd(b.from), t = ymd(b.to), text = null;
      if (unit === 'month') {
        var k = Math.max(1, Math.ceil(minGap / slot)), step = k <= 1 ? 1 : k <= 2 ? 2 : k <= 3 ? 3 : k <= 4 ? 4 : k <= 6 ? 6 : 12;
        if ((f.m - 1) % step === 0) text = f.m === 1 || !out.length ? (String(f.y).slice(2) + '年' + f.m + '月') : f.m + '月';
      } else if (unit === 'week') {
        var m = t.m;   // その週の終わりの月
        if (prevM !== null && m !== prevM) text = m === 1 ? (String(t.y).slice(2) + '年1月') : m + '月';
        prevM = m;
      } else if (f.d === 1 || (f.d === 15 && slot * 14 >= minGap * 1.6)) text = f.d === 1 ? (f.m === 1 ? String(f.y).slice(2) + '年1/1' : f.m + '/1') : f.m + '/15';
      if (!text) return;
      var x = (i + 0.5) * slot;
      if (x - lastX < minGap) return;
      out.push({ i: i, text: text }); lastX = x;
    });
    return out;
  }

  /** 角の丸い上の端 (4px・根元は四角) の棒の path */
  function barPath(x, y, w, h, r) {
    r = Math.max(0, Math.min(r, w / 2, h));
    return 'M' + x + ',' + (y + h) + 'V' + (y + r) + 'Q' + x + ',' + y + ' ' + (x + r) + ',' + y + 'H' + (x + w - r) + 'Q' + (x + w) + ',' + y + ' ' + (x + w) + ',' + (y + r) + 'V' + (y + h) + 'Z';
  }

  var geo = null;   // いまの描画の位置 (ツールチップ用)
  function draw() {
    var v = view();
    var W = Math.max(280, Math.floor(chartEl.clientWidth || box.clientWidth || 600));
    lastW = W;
    var narrow = W < 520;
    var M = { l: 40, r: 6, t: 10, b: 24 };
    var pw = W - M.l - M.r, n = v.buckets.length, slot = pw / n;
    var bw = slot >= 4 ? Math.min(24, slot - 2) : Math.max(1, slot - 1);
    var H = narrow ? 170 : 210, ph = H - M.t - M.b;
    var maxT = 0; v.buckets.forEach(function (b) { if (b.total > maxT) maxT = b.total; });
    var sc = niceScale(maxT), y = function (q) { return M.t + ph - (q / sc.max) * ph; };
    salesEl.textContent = '';
    var svg = el('svg', { width: W, height: H, viewBox: '0 0 ' + W + ' ' + H, 'aria-hidden': 'true', focusable: 'false' }, salesEl);
    // 欠品の帯 (いちばん後ろ)
    v.buckets.forEach(function (b, i) { if (b.stock && b.stock.zeroDays) el('rect', { class: 'sc-out', x: M.l + i * slot, y: M.t, width: Math.max(1, slot), height: ph }, svg); });
    // データの前 (注文の記録の前) = 何も描かない・幅があれば言葉
    var nd = 0; while (nd < n && v.buckets[nd].noData) nd++;
    if (nd && nd * slot >= 56) { var tx = el('text', { x: M.l + nd * slot / 2, y: M.t + ph / 2, 'text-anchor': 'middle', class: 'sc-nd' }, svg); tx.textContent = '記録の前'; }
    // 目盛りの線 (細く・実線)
    for (var q = 0; q <= sc.max; q += sc.step) {
      el('line', { class: q === 0 ? 'sc-axis' : 'sc-grid', x1: M.l, x2: W - M.r, y1: Math.round(y(q)) + 0.5, y2: Math.round(y(q)) + 0.5 }, svg);
      var tt = el('text', { class: 'tick', x: M.l - 6, y: y(q) + 4, 'text-anchor': 'end' }, svg); tt.textContent = tickText(q);
    }
    // 選んだ期間の柱 (棒の後ろ)
    var hl = el('rect', { class: 'sc-hl', x: 0, y: M.t, width: Math.max(1, slot), height: ph, visibility: 'hidden' }, svg);
    // 棒 (下から 楽天・Amazon・Yahoo!・その他。間は 2px のすき間)
    v.buckets.forEach(function (b, i) {
      if (b.noData || !b.total) return;
      var g = el('g', b.partial ? { class: 'sc-partial' } : {}, svg);
      var x = M.l + i * slot + (slot - bw) / 2, base = 0;
      var segs = SER.map(function (s, k) { return { k: k, q: b.qty[s.key] }; }).filter(function (s) { return s.q > 0; });
      segs.forEach(function (s, j) {
        var y0 = y(base), y1 = y(base + s.q), h = y0 - y1;
        var gap = j > 0 && bw >= 3 && h > 3 ? 2 : 0;   // 下の段とのすき間
        var top = j === segs.length - 1;
        var hh = Math.max(0.5, h - gap);
        if (top) el('path', { class: 'sc-c' + (s.k + 1), d: barPath(x, y1, bw, hh, 4) }, g);
        else el('rect', { class: 'sc-c' + (s.k + 1), x: x, y: y1, width: bw, height: hh }, g);
        base += s.q;
      });
    });
    // x の目盛り
    xLabels(v, slot).forEach(function (l) { var t = el('text', { class: 'tick', x: M.l + (l.i + 0.5) * slot, y: H - 6, 'text-anchor': 'middle' }, svg); t.textContent = l.text; });
    // 当たり (柱ぜんぶ = 棒より大きい)
    var hits = el('g', {}, svg);
    v.buckets.forEach(function (b, i) { var r = el('rect', { class: 'sc-hit', x: M.l + i * slot, y: M.t, width: Math.max(1, slot), height: ph + 6, 'data-i': i }, hits); r.style.fill = 'transparent'; });
    geo = { M: M, slot: slot, W: W, hl: [hl], salesH: H };

    // ── 在庫の線 (単品・記録のある期間だけ)
    stockEl.textContent = '';
    if (hasStock(v)) {
      stockHead.hidden = false;
      stockHead.textContent = '在庫 (ロジザードの倉庫・' + (unit === 'day' ? 'その日の締め' : 'その' + UNIT[unit].per + 'の最後の締め') + ') · ' + full(data.stockFrom) + ' から';
      var SH = narrow ? 76 : 90, SM = { t: 8, b: 8 }, sph = SH - SM.t - SM.b;
      var maxS = 0; v.buckets.forEach(function (b) { if (b.stock && b.stock.end > maxS) maxS = b.stock.end; });
      var ss = niceScale(maxS), sy = function (q) { return SM.t + sph - (q / ss.max) * sph; };
      var s2 = el('svg', { width: W, height: SH, viewBox: '0 0 ' + W + ' ' + SH, 'aria-hidden': 'true', focusable: 'false' }, stockEl);
      v.buckets.forEach(function (b, i) { if (b.stock && b.stock.zeroDays) el('rect', { class: 'sc-out', x: M.l + i * slot, y: SM.t, width: Math.max(1, slot), height: sph }, s2); });
      [0, ss.max].forEach(function (q) {
        el('line', { class: q === 0 ? 'sc-axis' : 'sc-grid', x1: M.l, x2: W - M.r, y1: Math.round(sy(q)) + 0.5, y2: Math.round(sy(q)) + 0.5 }, s2);
        var t = el('text', { class: 'tick', x: M.l - 6, y: sy(q) + 4, 'text-anchor': 'end' }, s2); t.textContent = tickText(q);
      });
      var hl2 = el('rect', { class: 'sc-hl', x: 0, y: SM.t, width: Math.max(1, slot), height: sph, visibility: 'hidden' }, s2);
      var d = '', prev = false, pts = [];
      v.buckets.forEach(function (b, i) {
        if (!b.stock) { prev = false; return; }
        var px = M.l + (i + 0.5) * slot, py = sy(b.stock.end);
        d += (prev ? 'L' : 'M') + px.toFixed(1) + ',' + py.toFixed(1);
        pts.push({ i: i, x: px, y: py, alone: !prev && !(v.buckets[i + 1] && v.buckets[i + 1].stock) });
        prev = true;
      });
      el('path', { class: 'sc-line', d: d }, s2);
      // 点 = つながる相手の無い点と、いちばん新しい点だけ (全部に点を打たない)
      pts.forEach(function (p, k) { if (p.alone || k === pts.length - 1) el('circle', { class: 'sc-dot', cx: p.x, cy: p.y, r: 4 }, s2); });
      var hits2 = el('g', {}, s2);
      v.buckets.forEach(function (b, i) { var r = el('rect', { class: 'sc-hit', x: M.l + i * slot, y: 0, width: Math.max(1, slot), height: SH, 'data-i': i }, hits2); r.style.fill = 'transparent'; });
      geo.hl.push(hl2);
    } else {
      stockHead.hidden = true;
    }
    if (active >= n) active = n - 1;
    if (active >= 0) show(active, false);
  }

  // ── ツールチップ (値が先・名前が後。色は線の印だけ・文字は文字の色)
  function tipRow(label, value, keyCls, strong) {
    var r = html('div', 'row', null, tip);
    var l = html('span', null, null, r);
    if (keyCls) html('span', 'k ' + keyCls, null, l);
    l.appendChild(document.createTextNode(label));
    html('span', strong ? 'v' : 'n', value, r);
  }
  function describe(b) {
    var t = [periodLabel(b) + (b.partial ? ' (途中・前日まで)' : '')];
    if (b.noData) t.push('注文の記録の前');
    else {
      t.push('合計 ' + num(b.total) + ' 個');
      SER.forEach(function (s) { t.push(s.label + ' ' + num(b.qty[s.key])); });
      if (b.viaSet) t.push('うちセット経由 ' + num(b.viaSet));
      if (b.partialData) t.push('期間の途中から (注文は 2025-01 から)');
    }
    if (data.kind === 'single') t.push(b.stock ? '在庫 ' + num(b.stock.end) + ' 個 (' + md(b.stock.endDay) + ' の締め)' + (b.stock.zeroDays ? '・欠品の日 ' + b.stock.zeroDays + ' 日' : '') : '在庫の記録なし');
    return t.join('、');
  }
  function show(i, announce) {
    var v = view(), b = v.buckets[i];
    if (!b || !geo) return;
    active = i;
    geo.hl.forEach(function (r) { r.setAttribute('x', geo.M.l + i * geo.slot); r.setAttribute('visibility', 'visible'); });
    tip.textContent = '';
    html('div', 'h', periodLabel(b) + (b.partial ? ' (途中)' : ''), tip);
    if (b.noData) html('div', 'n', '注文の記録の前 (2025-01 から)', tip);
    else {
      tipRow('合計', num(b.total) + ' 個', null, true);
      for (var k = SER.length - 1; k >= 0; k--) tipRow(SER[k].label, num(b.qty[SER[k].key]), 'sc-k' + (k + 1));   // 積み上げと同じ上から
      if (b.viaSet) tipRow('うちセット経由', num(b.viaSet));
      if (b.partialData) html('div', 'n', '期間の途中から (注文は 2025-01 から)', tip);
    }
    if (data.kind === 'single') {
      if (b.stock) { tipRow('在庫 (' + md(b.stock.endDay) + ' の締め)', num(b.stock.end), 'ln'); if (b.stock.zeroDays) tipRow('欠品 (在庫 0) の日', b.stock.zeroDays + ' 日', 'out'); }
      else html('div', 'n', '在庫の記録なし', tip);
    }
    tip.hidden = false;
    var cx = geo.M.l + (i + 0.5) * geo.slot, tw = tip.offsetWidth || 180;
    var left = cx + 12 + tw > geo.W ? cx - 12 - tw : cx + 12;
    tip.style.left = Math.max(0, Math.min(geo.W - tw, left)) + 'px';
    tip.style.top = '4px';
    if (announce) live.textContent = describe(b);
  }
  function hide() {
    tip.hidden = true;
    if (geo) geo.hl.forEach(function (r) { r.setAttribute('visibility', 'hidden'); });
  }
  chartEl.addEventListener('pointermove', function (e) {
    var t = e.target && e.target.getAttribute && e.target.getAttribute('data-i');
    if (t != null && Number(t) !== active) show(Number(t), false); else if (t != null && tip.hidden) show(Number(t), false);
  });
  chartEl.addEventListener('pointerleave', function () { if (document.activeElement !== chartEl) { hide(); active = -1; } });
  chartEl.addEventListener('focus', function () { var n = view().buckets.length; show(active >= 0 ? active : n - 1, true); });
  chartEl.addEventListener('blur', function () { hide(); });
  chartEl.addEventListener('keydown', function (e) {
    var n = view().buckets.length, i = active < 0 ? n - 1 : active;
    if (e.key === 'ArrowLeft') i = Math.max(0, i - 1);
    else if (e.key === 'ArrowRight') i = Math.min(n - 1, i + 1);
    else if (e.key === 'Home') i = 0;
    else if (e.key === 'End') i = n - 1;
    else if (e.key === 'Escape') { hide(); return; }
    else return;
    e.preventDefault();
    show(i, true);
  });

  // ── 表 (新しい順・グラフと同じ数。空欄 = 記録の前)
  function table(v) {
    var single = data.kind === 'single';
    thead.textContent = '';
    var tr = html('tr', null, null, thead);
    ['期間'].concat(SER.map(function (s) { return s.label; }), ['合計', 'うちセット経由'], single ? ['在庫 (最後の締め)', '欠品の日'] : []).forEach(function (h, k) {
      var th = html('th', k ? 'n' : null, h, tr); th.setAttribute('scope', 'col');
    });
    rowsEl.textContent = '';
    v.buckets.slice().reverse().forEach(function (b) {
      var r = html('tr', null, null, rowsEl);
      var c0 = html('td', 'mv-when', periodLabel(b), r);
      if (b.partial) html('span', 'sub', '途中 (前日まで)', c0);
      if (b.noData) html('span', 'sub', '注文の記録の前', c0);
      else if (b.partialData) html('span', 'sub', '途中から (2025-01 から)', c0);
      SER.forEach(function (s) { html('td', 'n', b.noData ? '' : num(b.qty[s.key]), r); });
      html('td', 'n', b.noData ? '' : num(b.total), r);
      html('td', 'n', b.noData ? '' : num(b.viaSet), r);
      if (single) {
        // 在庫はどの日の締めの数かも書く (週・月の途中に取れなかった日があっても読み違えない。Codex R1 Low)
        var cs = html('td', 'n', b.stock ? num(b.stock.end) : '', r);
        if (b.stock && unit !== 'day') html('span', 'sub', md(b.stock.endDay) + ' の締め', cs);
        html('td', 'n', b.stock ? String(b.stock.zeroDays) : '', r);
      }
    });
    tableBox.hidden = false;
  }

  function render() {
    var v = view();
    sumEl.textContent = summary(v);
    legend(v);
    chartEl.hidden = false;
    draw();
    table(v);
  }
  function setUnit(u) {
    if (!UNIT[u] || u === unit) return;
    unit = u; active = -1; hide();
    Array.prototype.forEach.call(unitSeg.querySelectorAll('button'), function (b) { var on = b.getAttribute('data-v') === u; b.classList.toggle('on', on); b.setAttribute('aria-pressed', on ? 'true' : 'false'); });
    if (data) render();
  }
  unitSeg.addEventListener('click', function (e) { var b = e.target.closest && e.target.closest('button[data-v]'); if (b) setUnit(b.getAttribute('data-v')); });

  if (typeof ResizeObserver !== 'undefined') {
    new ResizeObserver(function () { if (data && !chartEl.hidden && Math.abs((chartEl.clientWidth || 0) - lastW) >= 2) draw(); }).observe(chartEl);
  } else window.addEventListener('resize', function () { if (data) draw(); });

  // 🆕 10/19 中原さん「表示できなかったものはボタンを押したら表示されるように」: 画面を開いたときはサーバーの 8 秒で読む。
  //   時間切れ (よく売れる商品) = 言葉と「もう一度読む」→ ?wait=long (サーバーで 30 秒まで待つ)。読む間はボタンを止めて「読み込み中…」(2 度押しで 2 本読まない)
  //   ボタンを出す = 時間切れ。押した後 (長い読み) は、混んでいる (busy)・つながらない (network) もボタンを残す (もう一度押せる)
  var RETRY_AFTER_LONG = { timeout: 1, busy: 1, network: 1 };
  var loading = false;
  function setLoading(on) {
    loading = on;
    retryBtn.disabled = on;
    waitEl.hidden = !on;
    box.setAttribute('aria-busy', on ? 'true' : 'false');
  }
  function failText(j, long) {
    if (j.reason === 'timeout') return long ? '長く待っても読み切れませんでした (DB が混んでいるかもしれません)。少し待ってからもう一度押してください'
      : '販売数の読み出しに時間がかかりました (よく売れる商品は時間がかかることがあります)。下のボタンで、長く待って読み直せます';
    if (j.reason === 'network' && long) return 'つながりませんでした。少し待ってからもう一度押してください';
    return j.error || '読めませんでした';
  }
  function load(long) {
    if (loading) return;
    setLoading(true);
    if (long) sumEl.textContent = '読み込み中… (30 秒ほどかかることがあります)';
    api('/api/sku/' + encodeURIComponent(CODE) + '/sales-chart' + (long ? '?wait=long' : '')).then(function (j) {
      setLoading(false);
      if (!j || !j.ok) {
        j = j || {};
        sumEl.textContent = failText(j, long);
        retryBox.hidden = !(j.reason === 'timeout' || (long && RETRY_AFTER_LONG[j.reason]));
        return;
      }
      retryBox.hidden = true;
      data = j;
      render();
    });
  }
  retryBtn.addEventListener('click', function () { load(true); });
  // 🆕 タブ (me-tabs.js): 隠れたタブの中 = そのタブを初めて開いたときに 1 回だけ読む (開いている = すぐ読む)
  var tp = box.closest ? box.closest('[role="tabpanel"]') : null;
  var started = false;
  function start() { if (started) return; started = true; load(false); }
  if (tp) tp.addEventListener('me:tabshown', function () { if (!started) start(); else if (data && Math.abs((chartEl.clientWidth || 0) - lastW) >= 2) draw(); });
  if (!tp || !tp.hidden) start();
})();
