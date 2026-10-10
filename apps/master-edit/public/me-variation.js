/*
 * me-variation.js — 新商品の登録「色違い・サイズ違いのまとまり」の画面の動き (CompanyDB構想/20 v7 §④・§⑩ の PR-7)
 *   正本 = 見本 v2 (色違いの登録_見本_20261009.html・中原さんと Codex の UI レビューで決めた形)。見本の動きをそのまま、ダミーのデータを本物の API に:
 *     GET  /api/variation/groups?q=     今あるまとまりを探す
 *     GET  /api/variation/groups/:id    まとまりを 1 つ (軸・選択肢・今ある子・子の値 = 共通の欄に写す)
 *     POST /api/variation/check         打った値を DB で確かめる (まとまりのコード・子のコード・JAN の重なり)
 *     POST /api/new/variation           まとめての登録 (1 つの取引で全部か何も無いか)
 *   - 上から 1 → 7 の順。打つたびに全部を確かめ直す (model)。右の「すすみぐあい」= あと N つ・直すところ・作る N / 120
 *   - 子 = 横 × 縦 の全部の組み合わせ (マス目で 作る ⇄ 作らない)・今ある子は出さない・前回作らなかった組み合わせは最初は「作らない」(押すと作る)
 *   - 子の名前 = 共通の商品名【横の選択肢名】【縦の選択肢名】【JAN】(JAN 無しは省く)。人が直した名前は自動では変えない
 *   - 上限 120 を超えたら保存を止めて、色そのものを分ける案 (残りの色は「次に足す色」に残す → 保存の後に「残りを足す」)
 *   - NE で作った古いまとまり = 最初の 1 回だけ、前からある子の「コードにつける文字」(コードの末尾から仮に入れる) と色の名前 (空 = 人が打つ) を確かめる
 *   - 今あるまとまりに足す = 今ある子の値を写す (どの子から・値が違う項目は人が選ぶ = 自動では選ばない)
 *   - スマホの幅: 手順の帯は「4/7 選択肢 ← 前へ / 次へ →」・キーボードの間は下の帯を小さく・子が 30 件をこえたら「パソコンがおすすめ」
 */
(function () {
  'use strict';
  var dataEl = document.getElementById('me-variation');
  if (!dataEl) return;
  var P = JSON.parse(dataEl.textContent || 'null');
  if (!P) return;
  var ME = window.MasterEdit = window.MasterEdit || {};
  var BASE = P.base;
  var $ = function (s, r) { return (r || document).querySelector(s); };
  var $$ = function (s, r) { return Array.prototype.slice.call((r || document).querySelectorAll(s)); };
  var esc = function (x) { return String(x == null ? '' : x).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; }); };
  var norm = function (s) { return String(s == null ? '' : s).toLowerCase(); };
  var nkey = function (s) { return String(s == null ? '' : s).normalize('NFKC').replace(/\s+/g, ' ').trim().toLowerCase(); };
  var yen = function (n) { return Number(n).toLocaleString('ja-JP'); };
  var MAX_KIDS = P.maxKids || 120;
  var NAME_MAX = 255;
  var CODE_RE = /^[A-Za-z0-9_-]{1,30}$/;
  var OPT_RE = /^-[A-Za-z0-9]{1,10}$/;
  var SEP_RE = /^(.*\S)\s*[\t\/／,，、]\s*(\S+)$/;
  var janCd = function (b) { var s = 0; for (var i = 0; i < b.length; i++) s += Number(b[i]) * (((b.length - 1 - i) % 2 === 0) ? 3 : 1); return String((10 - (s % 10)) % 10); };
  var isPhone = function () { return window.matchMedia('(max-width: 760px)').matches; };
  var canSave = P.canSave === true;

  var SUPPLIERS = P.suppliers || [];
  var SHIPPING = P.shipping || [];
  var SALES = { 1: '1 自社', 2: '2 取引先限定', 3: '3 仕入', 4: '4 輸出' };
  var FIELDS = ['price', 'cost', 'tax', 'sales', 'supplier', 'months', 'expiry', 'inbound', 'ship'];
  var FL = { price: '売価', cost: '原価', tax: '税率', sales: '売上分類', supplier: '代表の仕入先', months: '推奨保有月数', expiry: '有効期限の管理', inbound: '入荷日の管理', ship: '発送方法' };
  /** 共通の欄の画面の値 → 入れ物の id (seg は data-field) */
  var INPUT_ID = { price: 'c-price', cost: 'c-cost', supplier: 'f-primary_supplier', ship: 'c-ship', months: 'c-months' };
  var SEG_ID = { tax: 'c-tax', sales: 'c-sales', expiry: 'c-expiry', inbound: 'c-inbound' };
  var fv = function (f, v) {
    if (v === '' || v == null) return '(空)';
    if (f === 'price' || f === 'cost') return yen(v) + ' 円';
    if (f === 'tax') return v === '0.08' ? '8%' : '10%';
    if (f === 'sales') return SALES[v] || v;
    if (f === 'supplier') { var s = SUPPLIERS.filter(function (x) { return x[0] === v; })[0]; return s ? s[0] + ' ' + s[1] : v; }
    if (f === 'ship') { var t = SHIPPING.filter(function (x) { return x[0] === v; })[0]; return t ? t[0] + ' ' + t[1] : v; }
    if (f === 'months') return v + ' か月';
    return v === '1' ? 'あり' : 'なし';
  };
  var COMMON0 = { price: '', cost: '', tax: '', sales: '', supplier: '', months: '', expiry: '', inbound: '0', ship: '' };

  // ---------- 画面の状態 ----------
  var fresh = function () {
    return { mode: 'new', gcode: '', gname: '', pk: null, q: '', results: null, axisName: ['', ''], vOn: false, optText: ['', ''], nextOpts: [],
      common: Object.assign({}, COMMON0), copied: null, conflicts: {}, picks: {}, oldKid: {},
      excluded: new Set(), optIn: new Set(), edits: {}, filter: 'all', open: null, janPaste: false, janMsg: '', flash: null, done: null };
  };
  var S = fresh();
  var R = { key: '', busy: false, group: null, codes: {}, jans: {}, fresh: true, error: null };   // DB の確かめの答え (打った値ごと)

  var money = function (s) { var t = String(s == null ? '' : s).normalize('NFKC').replace(/[,\s円¥]/g, ''); if (t === '') return null; return /^\d{1,9}$/.test(t) ? Number(t) : NaN; };
  function janProblem(j) {
    if (!/^\d+$/.test(j)) return { c: 'shape', m: '数字だけで入れます' };
    if (j.length !== 8 && j.length !== 13) return { c: 'shape', m: '8 けたか 13 けた (いま ' + j.length + ' けた)' };
    if (janCd(j.slice(0, -1)) !== j.slice(-1)) return { c: 'shape', m: '最後の数字 (チェック数字) が合いません。打ちまちがいかも' };
    if (R.jans[j]) return { c: 'dup', m: 'ほかの商品 (' + R.jans[j] + ') が使っています' };
    return null;
  }

  // 「名前 / コードにつける文字」の行を読む
  function parseOpts(text, old) {
    var rows = [];
    String(text || '').split(/\r?\n/).forEach(function (raw, idx) {
      var t = raw.replace(/　/g, ' ').trim();
      if (!t) return;
      var name = '', num = '';
      var m = t.match(SEP_RE);
      if (!m) m = t.match(/^(.*\S)\s+([-－‐−]\S*)$/);
      if (m) { name = m[1].trim(); num = m[2]; } else if (/^[-－‐−]/.test(t)) num = t; else name = t;
      num = num.normalize('NFKC').replace(/^[‐−]/, '-');
      var r = { ln: idx + 1, raw: raw, name: name, num: num, errs: [], fix: false };
      if (!name) r.errs.push({ c: 'miss', m: '名前がありません' });
      else if (name.length > 100) r.errs.push({ c: 'shape', m: '名前は 100 字まで' });
      if (!num) r.errs.push({ c: 'miss', m: 'コードにつける文字がありません (例: ブラウン / -BR)' });
      else if (!OPT_RE.test(num)) {
        if (/^[A-Za-z0-9]{1,10}$/.test(num)) { r.errs.push({ c: 'shape', m: '「-」から入れます (→ -' + num + ')' }); r.fix = true; }
        else if (/^-.*[-_]/.test(num)) r.errs.push({ c: 'shape', m: '「-」は先頭の 1 つだけ。あとは英字と数字だけ' });
        else if (/^-[A-Za-z0-9]{11,}$/.test(num)) r.errs.push({ c: 'shape', m: '「-」のあと 10 字まで' });
        else r.errs.push({ c: 'shape', m: '使えるのは「-」と英字・数字だけ' });
      }
      rows.push(r);
    });
    var seenNum = new Map(old.map(function (o) { return [norm(o.num), '前からある「' + (o.name || o.num) + '」']; }));
    var seenName = new Map(old.filter(function (o) { return o.name; }).map(function (o) { return [nkey(o.name), '前からある選択肢']; }));
    rows.forEach(function (r) {
      if (r.num && OPT_RE.test(r.num)) { var k = norm(r.num); if (seenNum.has(k)) r.errs.push({ c: 'dup', m: 'コードにつける文字が ' + seenNum.get(k) + ' と同じ (大文字小文字は同じと見ます)' }); else seenNum.set(k, r.ln + ' 行目'); }
      if (r.name) { var k2 = nkey(r.name); if (seenName.has(k2)) r.errs.push({ c: 'dup', m: '名前が ' + seenName.get(k2) + ' と同じ' }); else seenName.set(k2, r.ln + ' 行目'); }
    });
    S.nextOpts.forEach(function (nx) { var nm = String(nx).match(SEP_RE); if (!nm) return; rows.forEach(function (r) { if (r.num && norm(r.num) === norm(nm[2]) && !r.errs.length) r.errs.push({ c: 'dup', m: '「次に足す色」にも同じ文字があります' }); }); });
    return rows;
  }

  // 選んだまとまり (API の答え) → 画面の形
  function groupView(g) {
    var recorded = g.axes.length > 0;
    var axes = recorded ? g.axes.map(function (a) {
      return { name: a.name, options: g.options.filter(function (o) { return o.axis === a.axis; }).map(function (o) { return { name: o.name, num: o.code }; }) };
    }) : null;
    var active = g.children.filter(function (k) { return !k.cancelled; });
    var combos = new Set();
    g.children.forEach(function (k) { if (k.choices) combos.add(norm(k.choices['1']) + '|' + norm(k.choices['2'] || '')); });
    return { id: g.product_id, code: g.code, name: g.name, kind: g.kind, source: g.source, revision: g.revision, axes: axes,
      kids: active.map(function (k) { return k.code; }), kidRows: active, allCodes: new Set(g.children.map(function (k) { return norm(k.code); })), combos: combos,
      cancelled: g.children.filter(function (k) { return k.cancelled; }).map(function (k) { return k.code; }) };
  }

  // NE で作ったまとまり: 前からある子のコードの末尾から、コードにつける文字を仮に入れる (名前は空 = 人が打つ)
  function initOldKids(g) {
    var two = S.vOn;
    var prev = S.oldKid;
    S.oldKid = {};
    g.kids.forEach(function (c) {
      var tail = norm(c).indexOf(norm(g.code)) === 0 ? c.slice(g.code.length) : '';
      var toks = tail.match(/-[A-Za-z0-9]+/g) || [];
      var p = prev[c] || {};
      S.oldKid[c] = { hnum: two ? (toks[0] || '') : (OPT_RE.test(tail) ? tail : (toks[0] || tail)), vnum: two ? (toks.slice(1).join('') || '') : '', hname: p.hname || '', vname: p.vname || '' };
    });
  }

  // 子 = 横 × 縦 の組み合わせ (今ある子は出さない・前に作らなかった組み合わせは「前回は作らなかった」)
  function buildKids(axes, gcode, pk, hLimit) {
    var H = axes[0].old.map(function (o) { return { name: o.name, num: o.num, old: true }; })
      .concat(axes[0].good.slice(0, hLimit == null ? undefined : hLimit).map(function (o) { return { name: o.name, num: o.num, old: false }; }));
    var V = axes.length === 2 ? axes[1].old.map(function (o) { return { name: o.name, num: o.num, old: true }; }).concat(axes[1].good.map(function (o) { return { name: o.name, num: o.num, old: false }; })) : [null];
    var out = [];
    H.forEach(function (h) {
      V.forEach(function (v) {
        var code = gcode + h.num + (v ? v.num : '');
        var both = h.old && (!v || v.old);
        var key = norm(h.num) + '|' + (v ? norm(v.num) : '');
        if (both && pk && (pk.combos.has(key) || pk.allCodes.has(norm(code)))) return;   // 今ある子 (やめた子も = 使い回さない)
        out.push({ key: key, code: code, h: h, v: v, prev: both, inc: both ? S.optIn.has(key) : !S.excluded.has(key) });
      });
    });
    return out;
  }

  // ---------- 入れた値から全部を出す (画面の確かめ = サーバーと同じ決まり + DB の答え) ----------
  function model() {
    var add = S.mode === 'add';
    var pk = add ? S.pk : null;
    var issues = [];
    var Pi = function (step, c, m, go) { issues.push({ step: step, c: c, m: m, go: go }); };
    var gcode = '';
    var baseName = add ? (pk ? pk.name : '') : S.gname.trim();
    if (!add) {
      var t = S.gcode; gcode = t;
      if (!t) Pi(2, 'miss', 'まとまりのコード', '#g-code');
      else if (t !== t.trim()) Pi(2, 'shape', 'まとまりのコード: 前後に空白があります', '#g-code');
      else if (!CODE_RE.test(t)) Pi(2, 'shape', 'まとまりのコード: 英字・数字・- _ だけ (30 字まで)', '#g-code');
      else if (/^set-/i.test(t)) Pi(2, 'shape', 'まとまりのコード: 「set-」で始めない (セットの印)', '#g-code');
      else if (R.group && R.group.code === t && R.group.problem) Pi(2, 'dup', 'まとまりのコード: ' + R.group.message, '#g-code');
      if (!baseName) Pi(2, 'miss', '商品名', '#g-name');
      else if (baseName.length > NAME_MAX) Pi(2, 'shape', '商品名: 255 字まで', '#g-name');
    } else if (!pk) Pi(2, 'miss', '足すまとまりを選ぶ', '#g-q');
    else gcode = pk.code;

    var recorded = !!(pk && pk.axes);
    var firstTime = !!(pk && !pk.axes);
    var ac = recorded ? pk.axes.length : (S.vOn ? 2 : 1);
    var axes = [];
    var oldRows = [];
    if (!add || pk) {
      for (var i = 0; i < ac; i++) {
        var name = recorded ? pk.axes[i].name : S.axisName[i].trim();
        if (!recorded && !name) Pi(3, 'miss', (i === 0 ? '横軸' : '縦軸') + 'の名前', '#ax-' + i);
        else if (!recorded && name.length > 100) Pi(3, 'shape', (i === 0 ? '横軸' : '縦軸') + 'の名前は 100 字まで', '#ax-' + i);
        axes.push({ i: i, name: name, old: recorded ? pk.axes[i].options : [] });
      }
      if (ac === 2 && axes[0].name && axes[1].name && nkey(axes[0].name) === nkey(axes[1].name)) Pi(3, 'dup', '横軸と縦軸が同じ名前です', '#ax-1');
      // NE で作ったまとまり: 最初の 1 回だけ、前からある子の選択肢を確かめる
      if (firstTime) {
        var maps = [new Map(), new Map()];
        pk.kids.forEach(function (c, idx) {
          var r = S.oldKid[c] || {};
          var row = { code: c, idx: idx, r: r, errs: [] };
          var E = function (cc, m) { row.errs.push({ c: cc, m: m }); Pi(4, cc, '前からある ' + c + ': ' + m, '#old-' + idx); };
          for (var j = 0; j < ac; j++) {
            var num = j === 0 ? r.hnum : r.vnum, nm = String(j === 0 ? r.hname || '' : r.vname || '').trim();
            var lab = axes[j].name || (j === 0 ? '横' : '縦');
            if (!OPT_RE.test(num || '')) E('shape', lab + 'のコードにつける文字の形');
            if (!nm) E('miss', lab + 'の名前');
            else if (nm.length > 100) E('shape', lab + 'の名前は 100 字まで');
            if (OPT_RE.test(num || '') && nm) {
              var k = norm(num);
              var cur = maps[j].get(k);
              if (cur && nkey(cur.name) !== nkey(nm)) E('dup', '「' + num + '」に名前が 2 つ (' + cur.name + ' と ' + nm + ')');
              else if (!cur) {
                var same = Array.from(maps[j].values()).filter(function (x) { return nkey(x.name) === nkey(nm); })[0];
                if (same) E('dup', '名前「' + nm + '」の文字が 2 つ (' + same.num + ' と ' + num + ')');
                else maps[j].set(k, { name: nm, num: num });
              }
            }
          }
          if (norm(gcode + (r.hnum || '') + (ac === 2 ? (r.vnum || '') : '')) !== norm(c)) E('shape', 'まとまりのコード + 文字 が この子のコードと合いません');
          oldRows.push(row);
        });
        for (var a2 = 0; a2 < ac; a2++) axes[a2].old = Array.from(maps[a2].values());
      }
      axes.forEach(function (ax) {
        ax.rows = parseOpts(S.optText[ax.i], ax.old);
        var label = (ax.i === 0 ? '横' : '縦') + '「' + (ax.name || (ax.i === 0 ? '横軸' : '縦軸')) + '」';
        ax.rows.forEach(function (r) { r.errs.forEach(function (e) { Pi(4, e.c, label + ' ' + r.ln + ' 行目' + (r.name ? '「' + r.name + '」' : '') + ': ' + e.m, '#opt-' + ax.i); }); });
        ax.good = ax.rows.filter(function (r) { return !r.errs.length; });
        if (!ax.good.length && !ax.old.length) Pi(4, 'miss', label + 'の選択肢 (1 つ以上)', '#opt-' + ax.i);
      });
    }

    var C = S.common;
    Object.keys(S.conflicts).forEach(function (f) { if (!S.picks[f]) Pi(5, 'miss', FL[f] + ' (今ある子で ' + S.conflicts[f].length + ' 種類・選ぶ)', '#cf-' + f); });
    var cp = money(C.price);
    if (cp === null) { if (!S.conflicts.price || S.picks.price) Pi(5, 'miss', '売価', '#c-price'); } else if (Number.isNaN(cp) || cp < 1) Pi(5, 'shape', '売価: 1 円以上の整数で', '#c-price');
    var cc = money(C.cost);
    if (cc !== null && Number.isNaN(cc)) Pi(5, 'shape', '原価: 整数で', '#c-cost');
    if (!C.tax && !S.conflicts.tax) Pi(5, 'miss', '税率', '#c-tax');
    if (!C.sales && !S.conflicts.sales) Pi(5, 'miss', '売上分類', '#c-sales');
    if (!C.supplier && !S.conflicts.supplier) Pi(5, 'miss', '代表の仕入先', '#f-primary_supplier');
    var mo = String(C.months).normalize('NFKC').trim();
    if (mo === '') { if (!S.conflicts.months) Pi(5, 'miss', '推奨保有月数', '#c-months'); }
    else if (!/^\d{1,2}(\.\d)?$/.test(mo) || Number(mo) > 60) Pi(5, 'shape', '推奨保有月数: 0〜60 (小数は 1 けたまで)', '#c-months');
    if (C.expiry === '' && !S.conflicts.expiry) Pi(5, 'miss', 'ロジザードの有効期限の管理', '#c-expiry');

    var kids = axes.length ? buildKids(axes, gcode, pk) : [];
    var seenCode = new Map(), seenJan = new Map();
    kids.forEach(function (k, i2) {
      k.idx = i2;
      var e = S.edits[k.key] || {};
      k.jan = String(e.jan == null ? '' : e.jan).normalize('NFKC').trim();
      k.auto = baseName + '【' + k.h.name + '】' + (k.v ? '【' + k.v.name + '】' : '') + (k.jan ? '【' + k.jan + '】' : '');
      k.manual = !!e.manual; k.name = k.manual ? (e.name == null ? '' : e.name) : k.auto;
      k.price = e.price == null ? '' : e.price; k.cost = e.cost == null ? '' : e.cost;
      k.pv = money(k.price); k.cv = money(k.cost);
      k.ownPrice = k.pv !== null && !Number.isNaN(k.pv) && k.pv !== cp;
      k.ownCost = k.cv !== null && !Number.isNaN(k.cv) && k.cv !== cc;
      k.errs = [];
      if (!k.inc) return;
      var E = function (c, m) { k.errs.push({ c: c, m: m }); };
      if (gcode) {
        var nk = norm(k.code);
        if (k.code.length > 30) E('shape', 'コードが 30 字をこえます (' + k.code.length + ' 字)');
        else if (R.codes[k.code]) E('dup', R.codes[k.code].message);
        if (seenCode.has(nk)) E('dup', 'コードが ' + seenCode.get(nk) + ' と同じ'); else seenCode.set(nk, k.code);
      }
      if (!k.name.trim()) E('miss', '名前がありません');
      else if (k.name.length > NAME_MAX) E('shape', '名前が長すぎます (' + k.name.length + ' 字 · 255 字まで)');
      else if (/^empty$/i.test(k.name.trim())) E('shape', '名前に「empty」だけは使えません');
      if (k.pv !== null && (Number.isNaN(k.pv) || k.pv < 1)) E('shape', '売価は 1 円以上の整数で');
      if (k.cv !== null && Number.isNaN(k.cv)) E('shape', '原価は整数で');
      if (k.jan) { var jp = janProblem(k.jan); if (jp) E(jp.c, 'JAN: ' + jp.m); else if (seenJan.has(k.jan)) E('dup', 'JAN が ' + seenJan.get(k.jan) + ' と同じ'); else seenJan.set(k.jan, k.code); }
      k.errs.forEach(function (x) { Pi(6, x.c, k.code + ': ' + x.m, '#kid-' + i2); });
    });
    var live = kids.filter(function (k) { return k.inc; });
    var n = live.length;
    if (kids.length && !n) Pi(6, 'miss', '作る子 (1 つ以上)', '#sec-kids');
    // 上限をこえたら: 外すのではなく、横の選択肢そのものを分ける案を出す
    var plan = null;
    if (n > MAX_KIDS) {
      var g0 = axes[0].good.length;
      for (var kk = g0 - 1; kk >= 1; kk--) {
        var cnt = buildKids(axes, gcode, pk, kk).filter(function (x) { return x.inc; }).length;
        if (cnt <= MAX_KIDS) { plan = { k: kk, rest: g0 - kk, cnt: cnt }; break; }
      }
      Pi(6, 'count', '子が ' + n + ' 件。1 回に作れるのは ' + MAX_KIDS + ' 件まで' + (plan ? ' → 「この分け方にする」で ' + plan.cnt + ' 件と残り ' + plan.rest + ' ' + (axes[0].name === 'カラー' ? '色' : 'つ') + 'に分ける' : ''), '#sec-kids');
    }
    return { add: add, pk: pk, gcode: gcode, baseName: baseName, recorded: recorded, firstTime: firstTime, ac: ac, axes: axes, oldRows: oldRows, kids: kids, live: live, n: n, issues: issues, cp: cp, cc: cc, plan: plan };
  }

  // ---------- 描く ----------
  var icon = function (n, cls) { return '<svg class="ic' + (cls ? ' ' + cls : '') + '" aria-hidden="true"><use href="#i-' + n + '"/></svg>'; };
  var errLine = function (e) { return '<div class="' + (e.c === 'miss' ? 'why-wrn' : 'why-blk') + '">' + icon(e.c === 'miss' ? 'warn' : 'x') + '<span>' + esc(e.m) + '</span></div>'; };
  var unitOf = function (m) { return m.axes[0] && m.axes[0].name === 'カラー' ? '色' : 'つ'; };

  function renderGroup() {
    var add = S.mode === 'add';
    $$('[data-mode]').forEach(function (b) { var on = b.getAttribute('data-mode') === S.mode; b.classList.toggle('on', on); b.setAttribute('aria-checked', on ? 'true' : 'false'); });
    $('#grp-new').hidden = add;
    $('#grp-add').hidden = !add;
    renderGroupSearch();
  }
  var srcBadge = function (g) {
    if (g.recorded || g.axes) return '<span class="b info">' + (g.source === 'portal' ? 'ポータルで作った' : '記録あり') + ' · ' + esc((g.axes || []).map(function (a) { return a.name || a; }).join(' × ')) + '</span>';
    return g.kind === 'single' ? '<span class="b mute">単品がまとまりの元 · 記録なし</span>' : '<span class="b warn">NE で作った · 色の記録なし (最初の 1 回だけ確かめる)</span>';
  };
  function renderGroupSearch() {
    var pk = S.pk;
    $('#g-searchbox').hidden = !!pk;
    if (pk) {
      $('#g-results').innerHTML = '';
      $('#g-picked').innerHTML = '<div class="picked"><div class="ph2"><span class="pc">' + esc(pk.code) + '</span><span class="pn">' + esc(pk.name) + '</span>' + srcBadge(pk)
        + '<span class="acts"><button type="button" class="btn sm" data-act="repick"' + (canSave ? '' : ' disabled') + '>' + icon('undo', 's') + 'えらび直す</button></span></div>'
        + '<div class="hint" style="margin-top:6px">まとまりのコードと今ある子は変わりません。新しい子の名前は「' + esc(pk.name) + '【…】」で作ります。今ある子 ' + pk.kids.length + ' 件:</div>'
        + '<div class="kidchips">' + pk.kids.map(function (c) { return '<span class="compchip"><span class="mono">' + esc(c) + '</span></span>'; }).join('') + '</div></div>';
      return;
    }
    $('#g-picked').innerHTML = '';
    var res = S.results;
    if (!res) { $('#g-results').innerHTML = '<div class="empty">探しています…</div>'; return; }
    if (res.error) { $('#g-results').innerHTML = '<div class="empty">' + esc(res.error) + '</div>'; return; }
    $('#g-results').innerHTML = res.items.length ? res.items.map(function (g) {
      return '<button type="button" class="gitem" data-pick="' + esc(g.product_id) + '"' + (canSave ? '' : ' disabled') + '><span class="code">' + esc(g.code) + '</span><span>' + esc(g.name) + '</span>' + srcBadge(g) + '<span class="kn">子 ' + g.children + ' 件</span></button>';
    }).join('') : '<div class="empty">見つかりません。新しい楽天のページなら「新しいまとまりを作る」へ</div>';
  }
  var searchSeq = 0, searchTimer = 0;
  function searchGroups() {
    clearTimeout(searchTimer);
    var my = ++searchSeq;
    searchTimer = setTimeout(function () {
      fetch(BASE + '/api/variation/groups?q=' + encodeURIComponent(S.q), { headers: { Accept: 'application/json' } })
        .then(function (r) { return r.json().catch(function () { return {}; }).then(function (j) { return { r: r, j: j }; }); })
        .then(function (x) { if (my !== searchSeq) return; S.results = x.r.ok && x.j.ok ? { items: x.j.items } : { error: x.j.error || '探せませんでした (HTTP ' + x.r.status + ')', items: [] }; renderGroupSearch(); })
        .catch(function () { if (my !== searchSeq) return; S.results = { error: 'つながりません。少し待ってから', items: [] }; renderGroupSearch(); });
    }, 250);
  }

  function renderAxes() {
    var pk = S.mode === 'add' ? S.pk : null;
    var dis = canSave ? '' : ' disabled';
    var h = '';
    if (S.mode === 'add' && !pk) h = '<div class="empty">先に 2 でまとまりを選んでください</div>';
    else if (pk && pk.axes) {
      h += pk.axes.map(function (a, i) { return '<div class="f"><div class="lab">' + (i === 0 ? '横軸' : '縦軸') + '</div><div class="ctl"><span class="lockval">' + icon('lock') + esc(a.name) + '</span><span class="hint">コードの ' + (i === 0 ? '1' : '2') + ' つ目につける文字</span></div></div>'; }).join('');
      h += '<div class="hint" style="margin-top:4px">今あるまとまりは軸の数を変えられません (このまとまりは ' + pk.axes.length + ' 軸)。</div>';
    } else {
      if (pk) h += '<div class="callout warn" style="margin-bottom:6px">' + icon('info') + '<div class="grow"><div class="t">このまとまりは軸と選択肢の記録がありません</div><div class="hint">ここで初めて軸の名前を付けます。4 で、前からある子 ' + pk.kids.length + ' 件の色の名前も 1 回だけ確かめます。</div></div></div>';
      var chips = function (i, list) { return '<span class="sugg"><span class="t">よく使う:</span>' + list.map(function (v) { return '<button type="button" class="chip xs" data-axv="' + i + '" data-v="' + esc(v) + '"' + dis + '>' + esc(v) + '</button>'; }).join('') + '</span>'; };
      h += '<div class="f"><label class="lab" for="ax-0">横軸の名前<span class="req">必須</span></label><div class="ctl"><input class="in w-s" id="ax-0" maxlength="40" value="' + esc(S.axisName[0]) + '" placeholder="例: カラー"' + dis + '>' + chips(0, ['カラー', '柄', '香り', '味']) + '<span class="hint" style="flex-basis:100%">コードの 1 つ目につける文字 (色・柄など)</span></div></div>';
      h += '<div class="f"><div class="lab" id="lab-ax-v">縦軸<span class="opt">要るときだけ</span></div><div class="ctl"><div class="seg" role="group" id="ax-v" aria-labelledby="lab-ax-v" data-value="' + (S.vOn ? '1' : '0') + '">'
        + '<button type="button" data-v="0" aria-pressed="' + (!S.vOn) + '"' + (!S.vOn ? ' class="on"' : '') + dis + '>使わない (1 軸)</button><button type="button" data-v="1" aria-pressed="' + S.vOn + '"' + (S.vOn ? ' class="on"' : '') + dis + '>使う (2 軸)</button></div>'
        + (S.vOn ? '<input class="in w-s" id="ax-1" maxlength="40" value="' + esc(S.axisName[1]) + '" placeholder="例: サイズ" aria-label="縦軸の名前"' + dis + '>' + chips(1, ['サイズ', '容量', '入数']) : '')
        + '<span class="hint" style="flex-basis:100%">軸は 2 つまで · 縦軸を使うと 横 × 縦 の全部の組み合わせが子になります (作らない組み合わせは 6 のマス目で外す)</span></div></div>';
    }
    $('#axes-box').innerHTML = h;
  }

  var dashed = function (s) { return esc(s).replace(/^-/, '<span class="dash">-</span>'); };
  function renderAnat(m) {
    var el = $('#anat');
    if (!m.axes.length) { el.hidden = true; return; }
    el.hidden = false;
    var h0 = m.axes[0].old[0] || m.axes[0].good[0];
    var v0 = m.ac === 2 ? (m.axes[1].old[0] || m.axes[1].good[0]) : null;
    var g = m.gcode || 'まとまり';
    var hv = h0 ? h0.num : '-??', vv = v0 ? v0.num : '-??';
    var part = function (cls, val, lab, ph, d) { return '<span class="pt ' + cls + (ph ? ' ph' : '') + '"><code>' + (d ? dashed(val) : esc(val)) + '</code><small>' + esc(lab) + '</small></span>'; };
    el.innerHTML = '<div class="at">商品のコードのでき方 (入れるとすぐ変わります)</div><div class="parts">'
      + part('g', g, 'まとまりのコード', !m.gcode) + '<span class="op">+</span>'
      + part('h', hv, (m.axes[0].name || '横軸') + 'につける文字' + (h0 ? ' (' + h0.name + ')' : ''), !h0, true)
      + (m.ac === 2 ? '<span class="op">+</span>' + part('v', vv, (m.axes[1].name || '縦軸') + 'につける文字' + (v0 ? ' (' + v0.name + ')' : ''), !v0, true) : '')
      + '<span class="op">=</span>' + part('full', g + hv + (m.ac === 2 ? vv : ''), '商品のコード', !(m.gcode && h0 && (m.ac < 2 || v0))) + '</div>'
      + '<div class="anote">' + icon('warn', 's') + '<span>黄色の「<b>-</b>」も、コードにつける文字の一部です。<code>' + esc(hv) + '</code> のように「-」から入れます (「+」は打ちません)。</span></div>';
  }

  function renderOpts(m) {
    var wait = S.mode === 'add' && !m.pk;
    $('#opt-wait').hidden = !wait;
    $('#opt-grid').hidden = wait;
    $('#opt-grid').classList.toggle('one', m.ac < 2);
    $('#optc-1').hidden = m.ac < 2;
    var eq = '';
    if (m.axes.length && m.kids.length) {
      var over = m.n > MAX_KIDS;
      var offN = m.kids.length - m.n;
      if (!m.add) {
        var a = m.axes.map(function (ax) { return ax.good.length; });
        eq = '<div class="eqbar' + (over ? ' over' : '') + '"><span>' + esc(m.axes[0].name || '横') + ' <b>' + a[0] + '</b></span>' + (m.ac === 2 ? '<span class="x">×</span><span>' + esc(m.axes[1].name || '縦') + ' <b>' + a[1] + '</b></span>' : '')
          + '<span class="x">=</span><span><b class="tot">' + (a[0] * (a[1] || 1)) + '</b> 商品</span>' + (offN ? '<span class="hint">(作らない ' + offN + ' → 作る ' + m.n + ')</span>' : '') + '<span class="hint">· 1 回に ' + MAX_KIDS + ' 件まで</span></div>';
      } else {
        var prevN = m.kids.filter(function (k) { return k.prev; }).length;
        eq = '<div class="eqbar' + (over ? ' over' : '') + '"><span>新しい組み合わせ <b>' + (m.kids.length - prevN) + '</b></span>' + (prevN ? '<span class="x">+</span><span>前回は作らなかった <b>' + prevN + '</b></span>' : '')
          + '<span class="x">→</span><span>作る <b class="tot">' + m.n + '</b> 商品</span><span class="hint">· 今ある ' + m.pk.kids.length + ' 件は変えません</span></div>';
      }
    }
    $('#opt-eq').innerHTML = eq;
    // NE で作ったまとまり: 前からある子の確かめ (最初の 1 回だけ)
    var ob = '';
    if (m.firstTime) {
      var dis = canSave ? '' : ' disabled';
      ob = '<div class="oldbox"><div class="ot">' + icon('warn') + '前からある子 ' + m.pk.kids.length + ' 件の選択肢を確かめる (このまとまりで最初の 1 回だけ)</div>'
        + '<div class="hint" style="margin-top:4px">コードの終わりから「コードにつける文字」を仮に入れました。合っているか見て、色の名前を入れてください。同じ色を別の文字で足してしまうのを防ぎます。</div>'
        + '<div class="scrollx"><table class="opttbl"><thead><tr><th>今ある子のコード</th>' + m.axes.map(function (ax) { return '<th>' + esc(ax.name || (ax.i ? '縦' : '横')) + 'につける文字 <span class="autotag">仮</span></th><th>' + esc(ax.name || (ax.i ? '縦' : '横')) + 'の名前</th>'; }).join('') + '<th>確かめ</th></tr></thead><tbody>'
        + m.oldRows.map(function (row) {
          return '<tr id="old-' + row.idx + '"' + (row.errs.some(function (e) { return e.c !== 'miss'; }) ? ' class="bad"' : '') + '><td class="num">' + esc(row.code) + '</td>'
            + m.axes.map(function (ax) { var p = ax.i === 0 ? 'h' : 'v'; return '<td><input class="in mono oldin" data-oc="' + esc(row.code) + '" data-of="' + p + 'num" value="' + esc(row.r[p + 'num']) + '" aria-label="' + esc(row.code) + ' のコードにつける文字"' + dis + '></td><td><input class="in oldin" data-oc="' + esc(row.code) + '" data-of="' + p + 'name" value="' + esc(row.r[p + 'name']) + '" placeholder="例: ブラウン" aria-label="' + esc(row.code) + ' の名前"' + dis + '></td>'; }).join('')
            + '<td class="st">' + (row.errs.length ? row.errs.map(errLine).join('') : '<span class="why-ok">' + icon('check', 's') + 'OK</span>') + '</td></tr>';
        }).join('') + '</tbody></table></div></div>';
    }
    var obEl = $('#old-box');
    if (!obEl.contains(document.activeElement)) obEl.innerHTML = ob;
    else refreshOld(m);
    for (var i = 0; i < 2; i++) {
      var ax = m.axes[i];
      if (!ax) continue;
      var lab = i === 0 ? '横' : '縦';
      $('#opt-t-' + i).textContent = lab + '軸「' + (ax.name || lab + '軸') + '」の選択肢';
      $('#opt-l-' + i).textContent = ax.old.length ? '足すものだけ 1 行に 1 つ: 名前 / コードにつける文字' : '1 行に 1 つ: 名前 / コードにつける文字';
      $('#opt-n-' + i).textContent = (ax.old.length ? '前から ' + ax.old.length + ' + 足す ' : '') + ax.good.length + ' つ';
      $('#opt-ex-' + i).innerHTML = '例: ' + (i === 0 ? 'ブラウン / <code>-BR</code>' : (ax.name === 'サイズ' ? 'M / <code>-M</code>' : '90cm / <code>-90</code>')) + ' <span class="hint aux">← 名前 / コードにつける文字 (「-」から)</span>';
      var firstOther = m.ac === 2 ? (m.axes[1 - i].old[0] || m.axes[1 - i].good[0]) : null;
      var exCode = (function (ii, fo) { return function (num) { return (m.gcode || 'まとまり') + (ii === 0 ? num + (m.ac === 2 ? (fo ? fo.num : '-??') : '') : (fo ? fo.num : '-??') + num) + (m.ac === 2 ? ' …' : ''); }; })(i, firstOther);
      var t = '';
      if (ax.old.length || ax.rows.length) {
        t += '<div class="scrollx"><table class="opttbl"><thead><tr><th>#</th><th>名前</th><th>コードにつける文字</th><th>できるコード例</th><th>確かめ</th></tr></thead><tbody>';
        t += ax.old.map(function (o) { return '<tr class="old"><td class="ln">' + icon('lock', 's') + '</td><td>' + esc(o.name) + '</td><td class="num">' + esc(o.num) + '</td><td class="ex">' + esc(exCode(o.num)) + '</td><td class="st">前からある (変えません)</td></tr>'; }).join('');
        t += ax.rows.map(function (r) {
          return '<tr data-ln="' + r.ln + '"' + (r.errs.length ? ' class="bad"' : '') + '><td class="ln">' + r.ln + '</td><td>' + (esc(r.name) || '<span class="muted">—</span>') + '</td><td class="num">' + (esc(r.num) || '<span class="muted">—</span>') + '</td><td class="ex">' + (r.errs.length ? '—' : esc(exCode(r.num))) + '</td><td class="st">'
            + (r.errs.length ? r.errs.map(errLine).join('') : '<span class="why-ok">' + icon('check', 's') + (ax.old.length ? '足す' : 'OK') + '</span>') + '</td></tr>';
        }).join('');
        t += '</tbody></table></div>';
      }
      var nfix = ax.rows.filter(function (r) { return r.fix; }).length;
      if (nfix) t += '<div class="fixrow"><button type="button" class="btn sm soft" data-fix="' + i + '"' + (canSave ? '' : ' disabled') + '>「-」を付ける (' + nfix + ' 行)</button><span class="hint">押すと直った行が光ります。黙っては直しません</span></div>';
      $('#opt-p-' + i).innerHTML = t;
    }
    if (S.flash) { var f = S.flash; S.flash = null; f.lines.forEach(function (ln) { var tr = $('#opt-p-' + f.i + ' tr[data-ln="' + ln + '"]'); if (tr) tr.classList.add('flash'); }); }
    var nb = '';
    if (S.nextOpts.length) {
      nb = '<div class="nextbox"><div class="nt">' + icon('layers') + '次に足す' + unitOf(m) + ' (' + S.nextOpts.length + ') — 今回は保存しません<span class="right"><button type="button" class="btn sm" data-act="unsplit">' + icon('undo', 's') + '分けるのをやめる</button></span></div>'
        + '<div class="hint" style="margin-top:4px">保存の後に「残りを足す」を押すと、このまとまりに続けて入れられます (ここに残しておきます)。</div><div>'
        + S.nextOpts.map(function (l) { var mm = String(l).match(SEP_RE); return '<span class="optchip">' + esc(mm ? mm[1] : l) + (mm ? ' <code>' + esc(mm[2]) + '</code>' : '') + '</span>'; }).join('') + '</div></div>';
    }
    $('#next-box').innerHTML = nb;
  }
  function refreshOld(m) {
    m.oldRows.forEach(function (row) {
      var tr = $('#old-' + row.idx); if (!tr) return;
      tr.classList.toggle('bad', row.errs.some(function (e) { return e.c !== 'miss'; }));
      tr.querySelector('.st').innerHTML = row.errs.length ? row.errs.map(errLine).join('') : '<span class="why-ok">' + icon('check', 's') + 'OK</span>';
    });
  }

  function renderCommon(m) {
    var h = '';
    if (m.add && m.pk && S.copied) {
      var same = FIELDS.filter(function (f) { return !S.conflicts[f]; }).map(function (f) { return FL[f]; });
      h = '<div class="callout info copied">' + icon('info') + '<div class="grow"><div class="t"><span class="mono">' + esc(S.copied.from) + '</span> から写しました (今ある子 ' + S.copied.n + ' 件を見ました)</div><div class="hint">全部の子で同じだった値: ' + esc(same.join('・')) + '。そのままでよければ何もしなくて大丈夫です。'
        + (S.copied.dropped.length ? ' <b style="color:var(--warn)">' + esc(S.copied.dropped.join('・')) + ' は今は選べない値だったので写していません (選び直してください)。</b>' : '')
        + (Object.keys(S.conflicts).length ? ' <b style="color:var(--warn)">値が違う項目は自動では選びません (下で選んでください)。</b>' : '') + '</div></div></div>';
    }
    $('#c-copied').innerHTML = h;
    FIELDS.forEach(function (f) {
      var el = $('#cf-' + f); if (!el) return;
      var cf = S.conflicts[f];
      if (!cf) { el.innerHTML = ''; return; }
      var done = !!S.picks[f];
      el.innerHTML = '<div class="cfbox' + (done ? ' done' : '') + '"><div class="ct">' + icon(done ? 'check' : 'warn') + (done ? FL[f] + 'を選びました: ' + esc(fv(f, S.common[f])) : '今ある子で' + FL[f] + 'が ' + cf.length + ' 種類あります。新しい子に使う値を選んでください') + '</div>'
        + '<div class="opts">' + cf.map(function (o) { return '<button type="button" class="btn' + (done && S.common[f] === o.v ? ' on' : '') + '" data-cf="' + f + '" data-v="' + esc(o.v) + '"' + (canSave ? '' : ' disabled') + '>' + esc(fv(f, o.v)) + '<small>' + esc(o.codes.slice(0, 6).join('・') + (o.codes.length > 6 ? ' ほか ' + (o.codes.length - 6) : '')) + '</small></button>'; }).join('') + '</div></div>';
    });
  }

  var ochip = function (o, c) { return '<span class="ochip ' + c + '">' + esc(o.name) + '</span>'; };
  function kidTags(k) {
    var t = [];
    if (!k.inc) t.push(k.prev ? '<span class="b vio">前回は作らなかった · 今回も作らない</span>' : '<span class="b mute">作らない</span>');
    else if (k.prev) t.push('<span class="b vio">前回は作らなかった → 今回作る</span>');
    t.push(k.manual ? '<span class="mantag">名前を手で変更</span>' : '<span class="autotag">名前は自動</span>');
    var bad = k.errs.filter(function (e) { return e.c !== 'miss'; }).length;
    if (bad) t.push('<span class="b err">' + icon('x') + '直す ' + bad + '</span>');
    return t.join('');
  }
  var moneyChips = function (k, m) {
    return '<span class="mchip' + (k.ownPrice ? ' own' : '') + '">' + (k.ownPrice ? 'この商品だけ ' + yen(k.pv) + ' 円' : '共通 ' + (m.cp == null || Number.isNaN(m.cp) ? '—' : yen(m.cp) + ' 円')) + '</span>'
      + '<span class="mchip' + (k.ownCost ? ' own' : '') + '">原価 ' + (k.ownCost ? 'この商品だけ ' + yen(k.cv) + ' 円' : '共通 ' + (m.cc == null || Number.isNaN(m.cc) ? 'あとで' : yen(m.cc) + ' 円')) + '</span>';
  };
  var kidErrs = function (k) { return k.inc && k.errs.length ? k.errs.map(errLine).join('') : ''; };
  function kedit(k, m) {
    return '<div class="kedit"><div><label for="ke-name-' + k.idx + '">名前</label><input class="in kin" id="ke-name-' + k.idx + '" data-k="name" data-fid="name:' + esc(k.key) + '" maxlength="300" value="' + esc(k.name) + '">'
      + '<div class="row2">' + (k.manual ? '<span class="mantag">手で変更</span><button type="button" class="linkbtn" data-reset="name" data-key="' + esc(k.key) + '">自動の名前に戻す</button>' : '<span class="autotag">自動</span>') + '<span>' + k.name.length + ' / 255 字</span></div></div>'
      + '<div><label for="ke-price-' + k.idx + '">この商品だけの売価</label><span class="yen"><input class="in kin" id="ke-price-' + k.idx + '" data-k="price" data-fid="price:' + esc(k.key) + '" inputmode="numeric" value="' + esc(k.price) + '" placeholder="共通 ' + (m.cp ? yen(m.cp) : '—') + '"></span>'
      + '<div class="row2">' + (k.price !== '' ? '<button type="button" class="linkbtn" data-reset="price" data-key="' + esc(k.key) + '">共通に戻す</button>' : '空 = 共通の値') + '</div></div>'
      + '<div><label for="ke-cost-' + k.idx + '">この商品だけの原価</label><span class="yen"><input class="in kin" id="ke-cost-' + k.idx + '" data-k="cost" data-fid="cost:' + esc(k.key) + '" inputmode="numeric" value="' + esc(k.cost) + '" placeholder="共通 ' + (m.cc ? yen(m.cc) : 'あとで') + '"></span>'
      + '<div class="row2">' + (k.cost !== '' ? '<button type="button" class="linkbtn" data-reset="cost" data-key="' + esc(k.key) + '">共通に戻す</button>' : '空 = 共通の値') + '</div></div></div>';
  }
  var FILTERS = [['all', '全部'], ['err', '誤りだけ'], ['diff', '共通と違うだけ'], ['nojan', 'JAN なし'], ['off', '作らない']];
  var passF = function (k, f) { return f === 'all' ? true : f === 'err' ? k.errs.length > 0 : f === 'diff' ? (k.ownPrice || k.ownCost || k.manual) : f === 'nojan' ? (k.inc && !k.jan) : !k.inc; };
  function filterChips(m) {
    return '<span class="lbl0">絞る:</span>' + FILTERS.map(function (x) { var f = x[0]; var c = m.kids.filter(function (k) { return passF(k, f); }).length; return '<button type="button" class="chip' + (S.filter === f ? ' on' : '') + (f === 'err' && c ? ' t-err' : '') + '" data-filter="' + f + '" aria-pressed="' + (S.filter === f) + '">' + x[1] + ' <span class="n">' + c + '</span></button>'; }).join('')
      + '<button type="button" class="btn sm" data-act="janpaste" aria-expanded="' + S.janPaste + '" style="margin-left:auto"' + (canSave ? '' : ' disabled') + '>' + icon('paste', 's') + 'JAN をまとめて貼る</button>';
  }

  function renderKids(m) {
    var box = $('#kids-box');
    var a = document.activeElement;
    var fid = a && box.contains(a) ? a.getAttribute('data-fid') : null;
    var sel = fid && a.setSelectionRange ? [a.selectionStart, a.selectionEnd] : null;
    if (S.mode === 'add' && !m.pk) { box.innerHTML = '<div class="empty">先に 2 でまとまりを選んでください</div>'; return; }
    if (!m.kids.length) { box.innerHTML = '<div class="empty">4 で選択肢を入れると、ここに子が並びます</div>'; return; }
    var two = m.ac === 2;
    var over = m.n > MAX_KIDS;
    var offN = m.kids.length - m.n;
    var dis = canSave ? '' : ' disabled';
    var h = '<div class="kidtop"><span class="cnt2">作る <b>' + m.n + '</b> 件 · 作らない <b class="off">' + offN + '</b> 件</span>'
      + (offN ? '<button type="button" class="btn sm" data-filter="off">作らない ' + offN + ' 件を見る</button>' : '')
      + '<span class="meter' + (over ? ' over' : '') + '"><span class="mv"><b>' + m.n + '</b> / ' + MAX_KIDS + ' 件まで</span><span class="bar"><i style="width:' + Math.min(100, m.n / MAX_KIDS * 100) + '%"></i></span></span></div>';
    if (over) {
      var u = unitOf(m);
      h += '<div class="plan"><div class="pt1">' + icon('warn') + '子が ' + m.n + ' 件。1 回に作れるのは ' + MAX_KIDS + ' 件までなので、このままでは保存できません</div>'
        + (m.plan ? '<div class="pt2">安全な分け方: 今回は <b>' + esc(m.axes[0].name) + ' ' + m.plan.k + ' ' + u + (two ? ' × ' + esc(m.axes[1].name) + ' ' + (m.axes[1].old.length + m.axes[1].good.length) : '') + ' = ' + m.plan.cnt + ' 件</b>、残りの <b>' + m.plan.rest + ' ' + u + '</b>は次に足します。</div>'
          + '<div class="hint" style="margin-top:4px">組み合わせを「外して」分けると、外した商品があとで作れなくなることがあるので、' + esc(m.axes[0].name) + 'そのものを分けます。残りは 4 の「次に足す' + u + '」に残り、保存の後に続けて足せます。</div>'
          + '<div class="acts"><button type="button" class="btn pri" data-act="split"' + dis + '>この分け方にする (' + m.plan.cnt + ' 件 + 残り ' + m.plan.rest + ' ' + u + ')</button></div>'
          : '<div class="pt2">' + esc(m.axes[1] ? m.axes[1].name : '') + 'が多すぎて分けられません。選択肢を減らしてください。</div>') + '</div>';
    }
    if (isPhone() && m.kids.length > 30) h += '<div class="callout info pcwarn">' + icon('pc') + '<div class="grow"><div class="t">' + m.kids.length + ' 件あります。パソコンがおすすめです</div><div class="hint">小さい画面だと見落としやすいので、確かめはパソコンの大きい画面で。</div></div></div>';
    if (m.pk) h += '<details class="more" style="margin:0 0 12px"><summary>' + icon('chevr', 's') + '今ある子 ' + m.pk.kids.length + ' 件 (変えません)</summary><div class="body kidchips">' + m.pk.kids.map(function (c) { return '<span class="compchip"><span class="mono">' + esc(c) + '</span></span>'; }).join('') + '</div></details>';
    if (two) {
      var H = m.axes[0].old.map(function (o) { return { name: o.name, num: o.num, old: true }; }).concat(m.axes[0].good.map(function (o) { return { name: o.name, num: o.num, old: false }; }));
      var V = m.axes[1].old.map(function (o) { return { name: o.name, num: o.num, old: true }; }).concat(m.axes[1].good.map(function (o) { return { name: o.name, num: o.num, old: false }; }));
      var byKey = new Map(m.kids.map(function (k) { return [k.key, k]; }));
      h += '<div class="mxwrap"><div class="mt"><span>組み合わせ: <b>行 = ' + esc(m.axes[0].name || '横') + '</b> / <b class="v">列 = ' + esc(m.axes[1].name || '縦') + '</b></span><span class="hint">押すと 作る ⇄ 作らない · 作るものはここで選びます</span></div>'
        + '<div class="mxscroll"><table class="matrix"><thead><tr><th class="corner">' + esc(m.axes[0].name || '横') + ' ↓<br>' + esc(m.axes[1].name || '縦') + ' →</th>'
        + V.map(function (v) { return '<th class="vn">' + esc(v.name) + '</th>'; }).join('') + '</tr></thead><tbody>'
        + H.map(function (hh) {
          return '<tr><th class="hn">' + esc(hh.name) + '</th>' + V.map(function (v) {
            var key = norm(hh.num) + '|' + norm(v.num);
            var k = byKey.get(key);
            if (!k) return '<td><button type="button" class="cell" disabled title="今ある子">今ある</button></td>';
            return '<td><button type="button" class="cell' + (k.prev ? ' prev' : '') + '" data-mx="' + esc(key) + '" data-fid="mx:' + esc(key) + '" aria-pressed="' + k.inc + '" aria-label="' + esc(hh.name + ' × ' + v.name) + (k.inc ? ' (作る)' : ' (作らない)') + '"' + dis + '>'
              + (k.inc ? icon('check', 's') + '作る' : (k.prev ? '前回なし' : '作らない')) + '</button></td>';
          }).join('') + '</tr>';
        }).join('') + '</tbody></table></div>'
        + '<div class="legend"><span><i class="mk"></i>作る</span><span><i class="of"></i>作らない</span>' + (m.add ? '<span><i class="ex"></i>今ある (変えません)</span><span><i class="pv"></i>前回は作らなかった (押すと今回作る)</span>' : '') + '</div></div>';
    }
    h += '<div class="kfilter" id="kfilter">' + filterChips(m) + '</div>';
    var first = m.live[0] || m.kids[0];
    if (S.janPaste) h += '<div class="janpaste"><label class="lab3" for="jan-paste">Excel から「商品のコード / JAN」の 2 列を貼る (1 行に 1 つ)</label>'
      + '<div class="example">例: <code>' + esc(first.code) + '</code><span class="hint aux">(タブ)</span><code>4580123450013</code></div>'
      + '<textarea class="in" id="jan-paste" spellcheck="false" placeholder="' + esc(first.code) + '\t4580123450013"></textarea>'
      + '<div class="savefoot"><button type="button" class="btn soft" data-act="janapply">' + icon('check', 's') + 'JAN を入れる</button><button type="button" class="btn ghost" data-act="janpaste">閉じる</button><span class="hint" id="jan-msg">' + esc(S.janMsg) + '</span></div></div>';
    var rows = m.kids.filter(function (k) { return passF(k, S.filter); });
    h += '<div class="klist" id="klist"><div class="khead" aria-hidden="true"><span>商品のコード · ' + esc(m.axes.map(function (x) { return x.name || '?'; }).join(' · ')) + '</span><span>名前</span><span>売価 · 原価</span><span>JAN</span><span></span></div>';
    h += rows.length ? rows.map(function (k) {
      var cls = ['krow', k.inc ? '' : 'off', k.prev ? 'prev' : '', k.inc && k.errs.some(function (e) { return e.c !== 'miss'; }) ? 'row-err' : ''].filter(Boolean).join(' ');
      var d2 = k.inc && canSave ? '' : ' disabled';
      var open = S.open === k.key && k.inc;
      return '<div class="' + cls + '" id="kid-' + k.idx + '" data-key="' + esc(k.key) + '" data-code="' + esc(k.code) + '">'
        + '<div class="c-code"><span class="kc"><span class="sg">' + esc(m.gcode) + '</span><span class="sh">' + esc(k.h.num) + '</span>' + (k.v ? '<span class="sv">' + esc(k.v.num) + '</span>' : '') + '</span><div class="ochips">' + ochip(k.h, 'h') + (k.v ? ochip(k.v, 'v') : '') + '</div></div>'
        + '<div class="c-name"><div class="nm" title="' + esc(k.name) + '">' + esc(k.name) + '</div><div class="tags">' + kidTags(k) + '</div></div>'
        + '<div class="c-money">' + moneyChips(k, m) + '</div>'
        + '<div class="c-jan"><input class="in kin mono" data-k="jan" data-fid="jan:' + esc(k.key) + '" inputmode="numeric" maxlength="13" value="' + esc(k.jan) + '" placeholder="JAN なし"' + d2 + ' aria-label="' + esc(k.code) + ' の JAN"></div>'
        + '<div class="c-btn"><button type="button" class="btn sm' + (open ? ' soft' : '') + '" data-open="' + esc(k.key) + '" data-fid="op:' + esc(k.key) + '" aria-expanded="' + open + '" title="この商品だけ変える (名前・売価・原価)"' + d2 + '>' + (open ? '閉じる' : '変える') + '</button></div>'
        + '<div class="k-err">' + kidErrs(k) + '</div>' + (open ? kedit(k, m) : '') + '</div>';
    }).join('') : '<div class="kempty">この絞り込みに当てはまる子はありません</div>';
    h += '</div>';
    h += '<div class="hint" style="margin-top:8px">「変える」で、この商品だけの名前・売価・原価を入れられます (空なら共通の値) · 手で変えた名前は、あとで選択肢名や JAN を変えても自動では変えません · JAN は NE には送りません (自社 DB と商品名にだけ入ります)</div>';
    box.innerHTML = h;
    if (fid) { var el = box.querySelector('[data-fid="' + CSS.escape(fid) + '"]'); if (el) { el.focus(); if (sel && el.setSelectionRange) try { el.setSelectionRange(sel[0], sel[1]); } catch (e) { /* 数の欄 */ } } }
  }

  // 子の欄で打ったとき: 打っている欄は描き直さない (日本語の入力が切れないように)
  function refreshKids(m) {
    m.kids.forEach(function (k) {
      var row = $('#kids-box .krow[data-key="' + CSS.escape(k.key) + '"]');
      if (!row) return;
      row.querySelector('.nm').textContent = k.name; row.querySelector('.nm').title = k.name;
      row.querySelector('.tags').innerHTML = kidTags(k);
      row.querySelector('.c-money').innerHTML = moneyChips(k, m);
      row.querySelector('.k-err').innerHTML = kidErrs(k);
      row.classList.toggle('row-err', k.inc && k.errs.some(function (e) { return e.c !== 'miss'; }));
      var ke = row.querySelector('.kedit');
      if (ke) {
        var ni = ke.querySelector('[data-k="name"]');
        if (document.activeElement !== ni && ni.value !== k.name) ni.value = k.name;
        var r2 = ke.querySelectorAll('.row2');
        r2[0].innerHTML = (k.manual ? '<span class="mantag">手で変更</span><button type="button" class="linkbtn" data-reset="name" data-key="' + esc(k.key) + '">自動の名前に戻す</button>' : '<span class="autotag">自動</span>') + '<span>' + k.name.length + ' / 255 字</span>';
        r2[1].innerHTML = k.price !== '' ? '<button type="button" class="linkbtn" data-reset="price" data-key="' + esc(k.key) + '">共通に戻す</button>' : '空 = 共通の値';
        r2[2].innerHTML = k.cost !== '' ? '<button type="button" class="linkbtn" data-reset="cost" data-key="' + esc(k.key) + '">共通に戻す</button>' : '空 = 共通の値';
      }
    });
    var kf = $('#kfilter'); if (kf) kf.innerHTML = filterChips(m);
  }

  // 保存の後は直せない 3 つ (確かめの画面のいちばん上・黄色)
  function fixed3(m, withCheck) {
    var codes = m.live.slice(0, 3).map(function (k) { return '<span class="mono">' + esc(k.code) + '</span>'; }).join('・') + (m.n > 3 ? ' … 全 ' + m.n + ' 件' : '');
    return '<div class="fixed3"><div class="ft">' + icon('lock') + '保存の後は直せない 3 つ</div><ol>'
      + '<li>まとまりのコード: <b class="mono">' + esc(m.gcode || '—') + '</b>' + (m.add ? ' (今あるまとまり)' : ' (新しく作る · 楽天の商品管理番号と同じ)') + '</li>'
      + '<li>各商品のコード: ' + (m.n ? codes : '—') + '</li>'
      + '<li>どのまとまりに入るか: ' + (m.n ? '作る ' + m.n + ' 件は全部 <b class="mono">' + esc(m.gcode) + '</b> に入ります' + (m.add ? ' · 今ある ' + m.pk.kids.length + ' 件は変更なし・新しい ' + m.n + ' 件だけ作る' : '') : '—') + '</li></ol>'
      + (withCheck ? '<label class="chk"><input type="checkbox" id="fix-ok"><span>この 3 つを確かめました。保存の後は直せません</span></label>' : '') + '</div>';
  }
  function summaryRows(m) {
    var offs = m.kids.filter(function (k) { return !k.inc; });
    var ex = [];
    m.live.forEach(function (k) {
      if (k.ownPrice) ex.push('<span class="mono">' + esc(k.code) + '</span>: 売価 ' + yen(k.pv) + ' 円 (共通 ' + yen(m.cp) + ' 円)');
      if (k.ownCost) ex.push('<span class="mono">' + esc(k.code) + '</span>: 原価 ' + yen(k.cv) + ' 円 (共通 ' + (m.cc != null ? yen(m.cc) + ' 円' : 'なし') + ')');
      if (k.manual) ex.push('<span class="mono">' + esc(k.code) + '</span>: 名前を手で変更 →「' + esc(k.name) + '」');
      if (k.prev) ex.push('<span class="mono">' + esc(k.code) + '</span>: 前回は作らなかった組み合わせを今回作る');
    });
    var jans = m.live.filter(function (k) { return k.jan; }).length;
    var list = function (arr, max) { return '<ul>' + arr.slice(0, max).map(function (x) { return '<li>' + x + '</li>'; }).join('') + (arr.length > max ? '<li>… ほか ' + (arr.length - max) + ' 件</li>' : '') + '</ul>'; };
    var po = ME.orderSettings ? ME.orderSettings.collect() : null;
    return [
      ['まとまり', '<span class="mono">' + esc(m.gcode) + '</span> 「' + esc(m.baseName) + '」 ' + (m.add ? '<span class="b mute">今あるまとまりに足す</span>' : '<span class="b info">新しく作る</span>')],
      ['軸と選択肢', m.axes.map(function (a) { return esc(a.name) + ' ' + (a.old.length ? '前から ' + a.old.length + ' + ' : '') + '足す ' + a.good.length; }).join('<br>') + (m.firstTime ? '<br><span class="hint aux">前からある子 ' + m.pk.kids.length + ' 件の選択肢も記録します</span>' : '')],
      ['作る商品', '<b>' + m.n + ' 件</b> · JAN あり ' + jans + ' 件 · ほかは共通の値どおり'],
      ['共通と違う所', ex.length ? list(ex, 12) : 'なし'],
      ['作らない商品', offs.length ? list(offs.map(function (k) { return '<span class="mono">' + esc(k.code) + '</span> ' + esc(k.h.name + (k.v ? ' × ' + k.v.name : '')) + (k.prev ? ' (前回も作らなかった)' : ''); }), 12) : 'なし'],
      ['共通の欄', '売価 ' + (m.cp ? yen(m.cp) + ' 円' : '—') + (m.cc != null && !Number.isNaN(m.cc) ? ' · 原価 ' + yen(m.cc) + ' 円' : ' · 原価はあとで') + ' · 税 ' + fv('tax', S.common.tax) + ' · 仕入先 ' + esc(fv('supplier', S.common.supplier))],
      ['発注の設定', po ? '発注アプリへ (全部の子に同じ値)' : 'なし (あとで商品の画面で)'],
      ['出品カード', 'まとまりで 1 枚' + (m.ac === 2 ? ' (2 軸なので出品は止めたまま)' : '')],
    ].concat(S.nextOpts.length ? [['次に足す' + unitOf(m), S.nextOpts.length + ' ' + unitOf(m) + ' (今回は保存しない · 保存の後に続けて足す)']] : []);
  }

  var CAT = { count: '子の数', dup: '重なり', shape: '形の誤り', miss: 'まだ入れていない' };
  function renderChecks(m) {
    var by = function (c) { return m.issues.filter(function (x) { return x.c === c; }); };
    var item = function (x) { return '<li><button type="button" data-go="' + esc(x.go) + '"><span>' + esc(x.m) + '</span><span class="go">ここへ →</span></button></li>'; };
    var grp = function (c, okText, sub, wide) {
      var xs = by(c);
      var cls = xs.length ? (c === 'miss' ? 'miss' : 'bad') : 'ok';
      return '<div class="chkgrp ' + cls + (wide ? ' wide' : '') + '"><div class="cg-h">' + icon(xs.length ? (c === 'miss' ? 'warn' : 'x') : 'check') + CAT[c] + '<span class="n">' + (xs.length ? xs.length + ' つ' : okText) + '</span></div>'
        + (sub ? '<div class="cg-s">' + sub + '</div>' : '') + (xs.length ? '<ul>' + xs.slice(0, 15).map(item).join('') + (xs.length > 15 ? '<li class="hint aux" style="padding:4px 8px">… ほか ' + (xs.length - 15) + ' つ (6 の「誤りだけ」で見られます)</li>' : '') + '</ul>' : '') + '</div>';
    };
    var countSub = m.kids.length ? '作る ' + m.n + ' 件 / ' + MAX_KIDS + ' 件まで' : 'まだ子がありません';
    var dupSub = 'コード (今ある・消した・NE・この中どうし)・選択肢・JAN' + (R.busy || !R.fresh ? ' · <b style="color:var(--warn)">DB で確かめています…</b>' : R.error ? ' · <b style="color:var(--err)">' + esc(R.error) + '</b>' : '');
    var h = fixed3(m, false);
    h += '<div class="chkgrid">' + grp('count', m.n && m.n <= MAX_KIDS ? 'OK' : '—', countSub)
      + grp('dup', 'なし', dupSub)
      + grp('shape', 'なし', 'コードと文字の形・名前の長さ・JAN のけた') + '</div>';
    if (by('miss').length) h += '<div class="chkgrid">' + grp('miss', '', '', true) + '</div>';
    if (m.kids.length) h += '<details class="more"><summary>' + icon('chevr', 's') + '中身を見る (共通と違う所・作らない商品)</summary><div class="body"><table class="sumtbl">' + summaryRows(m).map(function (r) { return '<tr><th>' + r[0] + '</th><td>' + r[1] + '</td></tr>'; }).join('') + '</table></div></details>';
    $('#checks').innerHTML = h;
  }

  var STEPS = [[1, '種類', '#sec-kind'], [2, 'まとまり', '#sec-group'], [3, '軸', '#sec-axes'], [4, '選択肢', '#sec-opts'], [5, '共通の欄', '#sec-common'], [6, '子の一覧', '#sec-kids'], [7, '確かめて保存', '#sec-save']];
  function stepState(m, s) {
    if (s === 1) return 'done';
    if (m.add && !m.pk && (s === 3 || s === 4 || s === 6)) return 'todo';
    if (s === 7) return m.issues.length || !ready() ? 'todo' : 'done';
    var xs = m.issues.filter(function (x) { return x.step === s; });
    if (xs.some(function (x) { return x.c !== 'miss'; })) return 'bad';
    if (xs.length) return 'todo';
    if (s === 6 && !m.kids.length) return 'todo';
    return 'done';
  }
  function stepSub(m, s) {
    var xs = m.issues.filter(function (x) { return x.step === s; });
    if (s === 1) return '色違い・サイズ違い';
    if (s === 2) return m.gcode ? m.gcode + (m.add ? ' (今ある)' : ' (新しく)') : (m.add ? 'まとまりを選ぶ' : 'コードと商品名');
    if (s === 3) return m.axes.length ? m.axes.map(function (a) { return a.name || '?'; }).join(' × ') : '—';
    if (s === 4) return m.axes.length ? m.axes.map(function (a) { return (a.name || '?') + ' ' + (a.old.length ? a.old.length + '+' : '') + a.good.length; }).join(' · ') + (S.nextOpts.length ? ' (次に ' + S.nextOpts.length + ')' : '') : '—';
    if (s === 5) return xs.length ? 'あと ' + xs.length + ' つ' : 'そろった';
    if (s === 6) return '作る ' + m.n + ' 件' + (m.n > MAX_KIDS ? ' (多すぎ)' : '');
    if (s === 7) return m.issues.length ? '上がそろうと押せます' : !ready() ? 'DB で確かめています' : '押せます';
    return '';
  }
  /** DB の確かめが今の値に追いついているか (追いつくまで保存は押せない) */
  function ready() { return R.fresh && !R.busy && !R.error; }
  function renderHud(m) {
    var miss = m.issues.filter(function (x) { return x.c === 'miss'; }).length;
    var bad = m.issues.length - miss;
    var r = $('#remain');
    var ok = !m.issues.length && ready();
    r.classList.toggle('ok', ok);
    r.innerHTML = m.issues.length ? (miss ? '下書き保存まで あと<b>' + miss + '</b>つ' : '入れるところは そろいました') + (bad ? '<span class="badn">直すところ ' + bad + ' つ</span>' : '')
      : ready() ? '下書きにできます<b>' + icon('check') + '</b>' : 'DB で確かめています…';
    // 先に共通の「未保存 N 件」を出してから、下の帯 (.mb-n) は すすみぐあい の言葉で上書きする (見本と同じ)
    if (ME.setUnsaved) ME.setUnsaved(ME.dirty().n);
    $('#mb-n').style.color = '';
    $('#mb-n').textContent = S.done ? '保存しました' : m.issues.length ? (miss ? 'あと ' + miss + ' つ' : '') + (bad ? (miss ? ' · ' : '') + '直す ' + bad + ' つ' : '') + ' · 作る ' + m.n + ' 件' : '下書きにできます · 作る ' + m.n + ' 件';
    $('#hud-steps').innerHTML = STEPS.map(function (x) {
      var s = x[0], st = stepState(m, s);
      return '<li class="' + (st === 'done' ? 'done' : st === 'bad' ? 'bad' : 'todo need') + '"><button type="button" data-go="' + x[2] + '"><span class="mark">' + (st === 'done' ? icon('check') : st === 'bad' ? icon('x') : s) + '</span><span class="lbl">' + s + ' ' + x[1] + '<span class="sub2">' + esc(stepSub(m, s)) + '</span></span></button></li>';
    }).join('');
    var over = m.n > MAX_KIDS;
    $('#hud-meter').className = 'meter' + (over ? ' over' : '');
    $('#hud-meter').innerHTML = '<span class="mv">作る <b>' + m.n + '</b> / ' + MAX_KIDS + '</span><span class="bar" style="width:110px"><i style="width:' + Math.min(100, m.n / MAX_KIDS * 100) + '%"></i></span>';
    $('#save').disabled = $('#save2').disabled = !canSave || !ok || !!S.done || busy;
    $$('#jump a[data-step]').forEach(function (a) { var st = stepState(m, +a.getAttribute('data-step')); a.classList.toggle('done', st === 'done'); a.classList.toggle('bad', st === 'bad'); a.classList.toggle('todo', st === 'todo'); });
    $$('.panel-h .stepno[data-step]').forEach(function (el) { var n0 = el.getAttribute('data-step'); if (!/^[1-7]$/.test(n0)) return; var st = stepState(m, +n0); el.classList.toggle('done', st === 'done'); el.classList.toggle('bad', st === 'bad'); el.innerHTML = st === 'done' ? icon('check', 's') : st === 'bad' ? '!' : n0; });
    $('#after-n').textContent = '子 ' + m.n + ' 行';
  }

  function renderMisc(m) {
    var gi = m.issues.filter(function (x) { return x.go === '#g-code' && x.c !== 'miss'; });
    var msg = $('#g-code-msg');
    if (S.mode === 'new' && S.gcode) {
      if (gi.length) {
        var pid = R.group && R.group.code === S.gcode ? R.group.product_id : null;
        msg.className = 'fmsg err';
        msg.innerHTML = icon('x', 's') + esc(gi[0].m.replace(/^まとまりのコード: /, '')) + (pid ? ' <button type="button" class="btn sm soft" data-gotopick="' + esc(pid) + '">このまとまりに足す</button>' : '');
      } else if (!R.group || R.group.code !== S.gcode || R.busy) { msg.className = 'fmsg'; msg.innerHTML = '確かめています…'; }
      else { msg.className = 'fmsg ok'; msg.innerHTML = icon('check', 's') + '使えます (まだだれも使っていない)'; }
    } else { msg.className = 'fmsg'; msg.innerHTML = ''; }
    $('#g-code').closest('.f').classList.toggle('err', S.mode === 'new' && gi.length > 0);
    var k0 = m.live[0] || m.kids[0];
    $('#g-name-ex').textContent = k0 ? k0.auto : (S.gname ? S.gname + '【ブラウン】' : '—');
    var cn = '<div class="hint" style="margin-bottom:6px">作る ' + m.n + ' 件を 1 枚のカードにまとめます · NE に入る (翌朝の確かめ) までは出品を止めます (「NE の写し待ち」)' + (m.add && m.pk ? ' · 今あるカードがあれば、そのカードに足して「要確認」を付けます' : '') + '</div>';
    if (m.ac === 2) cn += '<div class="callout warn" style="margin-bottom:8px">' + icon('warn') + '<div class="grow"><div class="t">2 軸 (' + esc(m.axes.map(function (a) { return a.name || '?'; }).join(' × ')) + ') は、カードは作りますが出品は止めたままです</div><div class="hint">product-hub が 2 軸の出品に対応するまで (後の PR) の間だけです。</div></div></div>';
    $('#card-note').innerHTML = cn;
  }

  // ---------- DB の確かめ (打った値ごと・少し待ってからまとめて) ----------
  var checkTimer = 0, checkSeq = 0;
  function remoteKeyOf(m) {
    var codes = m.live.map(function (k) { return k.code; }).filter(function (c) { return CODE_RE.test(c); });
    var jans = m.live.map(function (k) { return k.jan; }).filter(function (j) { return /^(\d{8}|\d{13})$/.test(j); });
    var gc = S.mode === 'new' && CODE_RE.test(S.gcode) && !/^set-/i.test(S.gcode) ? S.gcode : '';
    return { key: JSON.stringify([gc, codes, jans]), gc: gc, codes: codes, jans: jans };
  }
  /** 確かめが今の値に追いついたか (試験と画面の印 = フォームの data-check) */
  function markCheck() { form.setAttribute('data-check', ready() ? 'done' : 'busy'); }
  function scheduleCheck(m) {
    var q = remoteKeyOf(m);
    if (q.key === R.key && (R.fresh || R.busy)) { markCheck(); return; }
    R.key = q.key; R.fresh = false; R.error = null;
    clearTimeout(checkTimer);
    var my = ++checkSeq;
    checkTimer = setTimeout(function () {
      if (!q.gc && !q.codes.length && !q.jans.length) { R.group = null; R.codes = {}; R.jans = {}; R.fresh = true; R.busy = false; light(); return; }
      R.busy = true;
      fetch(BASE + '/api/variation/check', { method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json' }, body: JSON.stringify({ group_code: q.gc || null, codes: q.codes, jans: q.jans }) })
        .then(function (r) { return r.json().catch(function () { return {}; }).then(function (j) { return { r: r, j: j }; }); })
        .then(function (x) {
          if (my !== checkSeq) return;
          R.busy = false;
          if (!x.r.ok || !x.j.ok) { R.error = 'DB で確かめられません (' + (x.j.error || 'HTTP ' + x.r.status) + ')。少し待つともう一度確かめます'; R.fresh = false; retryCheck(); light(); return; }
          R.group = x.j.group ? Object.assign({ code: q.gc }, x.j.group) : null;
          R.codes = x.j.codes || {}; R.jans = x.j.jans || {}; R.fresh = true; R.error = null;
          if (M && M.issues.some(function (i) { return i.go === '#g-code'; }) !== !!(R.group && R.group.problem)) update(); else light();
        })
        .catch(function () { if (my !== checkSeq) return; R.busy = false; R.error = 'つながりません。少し待つともう一度確かめます'; retryCheck(); light(); });
    }, 350);
    markCheck();
  }
  var retryT = 0;
  function retryCheck() { clearTimeout(retryT); retryT = setTimeout(function () { R.key = ''; scheduleCheck(model()); }, 4000); }

  var M = null;
  var busy = false;
  function update() { M = model(); renderAnat(M); renderOpts(M); renderCommon(M); renderKids(M); renderChecks(M); renderHud(M); renderMisc(M); scheduleCheck(M); }
  function light() { M = model(); refreshKids(M); renderChecks(M); renderHud(M); renderMisc(M); scheduleCheck(M); }
  function lightOld() { M = model(); renderAnat(M); renderOpts(M); renderKids(M); renderChecks(M); renderHud(M); renderMisc(M); scheduleCheck(M); }

  // ---------- 入れた値を画面へ ----------
  function setSeg(seg, v) { if (!seg) return; seg.setAttribute('data-value', v || ''); $$('button', seg).forEach(function (b) { var on = b.getAttribute('data-v') === v; b.classList.toggle('on', on); b.setAttribute('aria-pressed', on ? 'true' : 'false'); }); }
  function syncInputs() {
    $('#g-code').value = S.gcode; $('#g-name').value = S.gname; $('#g-q').value = S.q;
    $('#opt-0').value = S.optText[0]; $('#opt-1').value = S.optText[1];
    Object.keys(INPUT_ID).forEach(function (f) { var el = $('#' + INPUT_ID[f]); if (el) el.value = S.common[f]; });
    Object.keys(SEG_ID).forEach(function (f) { setSeg($('#' + SEG_ID[f]), S.common[f]); });
  }
  function full() { syncInputs(); renderGroup(); renderAxes(); update(); }

  // 今あるまとまりを選ぶ = 今ある子の値を写す (違う値は自動で選ばない・今は選べない値は写さない)
  function applyGroup(g) {
    var pk = groupView(g);
    var keepReason = S.reason;
    S = fresh();
    S.mode = 'add'; S.pk = pk; S.reason = keepReason;
    S.axisName = pk.axes ? pk.axes.map(function (a) { return a.name; }).concat(['']).slice(0, 2) : ['', ''];
    S.vOn = !!(pk.axes && pk.axes.length === 2);
    S.common = Object.assign({}, COMMON0); S.conflicts = {}; S.picks = {};
    var dropped = [];
    var okSup = new Set(SUPPLIERS.map(function (x) { return x[0]; })), okShip = new Set(SHIPPING.map(function (x) { return x[0]; }));
    var rows = pk.kidRows;
    FIELDS.forEach(function (f) {
      if (!rows.length) return;
      var mp = new Map();
      rows.forEach(function (k) {
        var v = k.values[f] == null ? '' : String(k.values[f]);
        if (f === 'supplier' && v && !okSup.has(v)) { if (dropped.indexOf(FL[f]) < 0) dropped.push(FL[f]); v = ''; }
        if (f === 'ship' && v && !okShip.has(v)) { if (dropped.indexOf(FL[f]) < 0) dropped.push(FL[f]); v = ''; }
        if (!mp.has(v)) mp.set(v, []);
        mp.get(v).push(k.code);
      });
      if (mp.size === 1) S.common[f] = Array.from(mp.keys())[0];
      else S.conflicts[f] = Array.from(mp).map(function (x) { return { v: x[0], codes: x[1] }; });
    });
    if (S.common.inbound === '' && !S.conflicts.inbound) S.common.inbound = '0';
    S.copied = rows.length ? { from: rows[0].code, n: rows.length, dropped: dropped } : null;
    if (!pk.axes) initOldKids(pk);
    full();
  }
  var pickSeq = 0;
  function pickGroup(id, then) {
    var my = ++pickSeq;
    $('#g-picked').innerHTML = '<div class="empty">読んでいます…</div>';
    $('#g-searchbox').hidden = true;
    fetch(BASE + '/api/variation/groups/' + encodeURIComponent(id), { headers: { Accept: 'application/json' } })
      .then(function (r) { return r.json().catch(function () { return {}; }).then(function (j) { return { r: r, j: j }; }); })
      .then(function (x) {
        if (my !== pickSeq) return;
        if (!x.r.ok || !x.j.ok) { S.mode = 'add'; S.pk = null; S.results = { error: x.j.error || ('読めませんでした (HTTP ' + x.r.status + ')'), items: [] }; full(); return; }
        applyGroup(x.j.group);
        if (then) then();
      })
      .catch(function () { if (my !== pickSeq) return; S.pk = null; S.results = { error: 'つながりません。少し待ってから', items: [] }; full(); });
  }

  // ---------- 動き ----------
  function toast(t) { if (ME.toast) ME.toast(t); }
  function go(sel) {
    var el = $(sel); if (!el) return;
    var tgt = el.closest('.f') || el.closest('.krow') || el.closest('tr') || el.closest('.optcard') || el;
    tgt.scrollIntoView({ behavior: 'smooth', block: 'center' });
    tgt.classList.remove('flash'); void tgt.offsetWidth; tgt.classList.add('flash');
    if (/^(INPUT|TEXTAREA|SELECT)$/.test(el.tagName)) setTimeout(function () { el.focus({ preventScroll: true }); }, 350);
    $('#savebox').classList.remove('open');
  }

  function onInput(t) {
    if (S.done) return;
    var id = t.id;
    if (id === 'g-code') S.gcode = t.value;
    else if (id === 'g-name') S.gname = t.value;
    else if (id === 'g-q') { S.q = t.value; S.results = S.results || null; searchGroups(); return; }
    else if (id === 'ax-0' || id === 'ax-1') S.axisName[+id.slice(-1)] = t.value;
    else if (id === 'opt-0' || id === 'opt-1') S.optText[+id.slice(-1)] = t.value;
    else if (id === 'jan-paste' || id === 'reason') return;
    else if (Object.keys(INPUT_ID).some(function (f) { return INPUT_ID[f] === id; })) {
      var f = Object.keys(INPUT_ID).filter(function (x) { return INPUT_ID[x] === id; })[0];
      S.common[f] = t.value; if (S.conflicts[f]) S.picks[f] = true;
      if (f === 'supplier' || f === 'ship') { renderCommon(model()); }
    } else if (t.classList.contains('oldin')) { var r = S.oldKid[t.getAttribute('data-oc')]; if (!r) return; r[t.getAttribute('data-of')] = t.value; lightOld(); return; }
    else if (t.classList.contains('kin')) {
      var key = t.closest('.krow').getAttribute('data-key'), k = t.getAttribute('data-k');
      var e = S.edits[key] || (S.edits[key] = {});
      if (k === 'name') { e.manual = true; e.name = t.value; } else e[k] = t.value;
      light(); return;
    } else if (t.closest && t.closest('#sec-order, #sec-card')) { renderHud(M || model()); return; }
    else return;
    update();
  }
  var form = $('#f');
  document.addEventListener('input', function (e) { if (!e.isComposing && form.contains(e.target)) onInput(e.target); });
  document.addEventListener('compositionend', function (e) { if (form.contains(e.target)) onInput(e.target); });
  document.addEventListener('change', function (e) {
    var t = e.target;
    if (t.tagName === 'SELECT' && form.contains(t) && !t.closest('#sec-order')) onInput(t);
    if (t.id === 'fix-ok') $('#confirm-yes').disabled = !t.checked || busy;
    // 切り替えボタン (me-shell.js が押された状態と data-value を付けて change を出す)
    if (t.classList && t.classList.contains('seg') && form.contains(t)) {
      if (S.done) return;
      var fld = t.getAttribute('data-field');
      var v = t.getAttribute('data-value') || '';
      if (fld && Object.prototype.hasOwnProperty.call(S.common, fld)) { S.common[fld] = v; if (S.conflicts[fld]) S.picks[fld] = true; update(); return; }
      if (t.id === 'ax-v') {
        S.vOn = v === '1';
        if (S.mode === 'add' && S.pk && !S.pk.axes) initOldKids(S.pk);
        renderAxes(); update();
        if (S.vOn) setTimeout(function () { var a1 = $('#ax-1'); if (a1) a1.focus(); }, 0);
        return;
      }
      if (t.closest('#sec-order')) renderHud(M || model());
    }
  });
  // スマホでキーボードが出ている間は、下の固定の帯を小さく
  document.addEventListener('focusin', function (e) { if (isPhone() && e.target.matches && e.target.matches('input:not([type=checkbox]), textarea, select')) document.body.classList.add('kb'); });
  document.addEventListener('focusout', function () { setTimeout(function () { var a = document.activeElement; if (!a || !a.matches || !a.matches('input:not([type=checkbox]), textarea, select')) document.body.classList.remove('kb'); }, 50); });

  function doSplit() {
    var m = model(); if (!m.plan) return;
    var moved = m.axes[0].good.slice(m.plan.k);
    var lines = new Set(moved.map(function (r) { return r.ln; }));
    var kept = [];
    S.optText[0].split(/\r?\n/).forEach(function (l, i) { if (lines.has(i + 1)) S.nextOpts.push(l.trim()); else kept.push(l); });
    S.optText[0] = kept.join('\n'); $('#opt-0').value = S.optText[0];
    update();
    toast('今回は ' + m.plan.cnt + ' 件にしました。残りの ' + m.plan.rest + ' ' + unitOf(m) + 'は 4 の「次に足す」に残しています');
    go('#next-box');
  }
  function unSplit() { S.optText[0] = (S.optText[0].replace(/\s+$/, '') + '\n' + S.nextOpts.join('\n')).replace(/^\n/, ''); S.nextOpts = []; $('#opt-0').value = S.optText[0]; update(); }
  function janApply() {
    var m = model();
    var byCode = new Map(m.kids.map(function (k) { return [norm(k.code), k]; }));
    var ok = 0; var miss = [];
    String($('#jan-paste').value || '').split(/\r?\n/).forEach(function (l) {
      var t = l.trim(); if (!t) return;
      var mm = t.match(/^(\S+)[\s,，\/]+(\S+)$/);
      if (!mm) { miss.push(t); return; }
      var k = byCode.get(norm(mm[1]));
      if (!k) { miss.push(mm[1]); return; }
      (S.edits[k.key] || (S.edits[k.key] = {})).jan = mm[2].normalize('NFKC'); ok++;
    });
    S.janMsg = ok + ' 件入れました' + (miss.length ? ' · 見つからないコード ' + miss.length + ' 件 (' + miss.slice(0, 3).join('・') + (miss.length > 3 ? ' …' : '') + ')' : '');
    update();
  }
  /** 保存の後: 保存したまとまりを「今ある」として読み直し、残りの色を入れる (残りを足す) */
  function continueNext() {
    var rest = S.nextOpts.slice();
    var gid = S.done && S.done.group_product_id;
    if (!gid) return;
    unlockAfterSave();
    requestId = uuid();
    pickGroup(gid, function () {
      S.optText[0] = rest.join('\n');
      $('#opt-0').value = S.optText[0];
      $('#result').innerHTML = '';
      update();
      toast('残りの ' + rest.length + ' つを入れました。マス目と一覧を見て、もう一度保存してください');
      go('#sec-opts');
    });
  }

  var stepNavCur = function () { return curStep(); };
  document.addEventListener('click', function (e) {
    var b = e.target.closest && e.target.closest('button, a[href^="#"]');
    if (!b) return;
    var d = b.dataset || {};
    if (b.closest('#confirm-bg')) {
      if (b.id === 'confirm-no') return closeConfirm();
      if (b.id === 'confirm-yes') return doSave();
      return;
    }
    if (!form.contains(b)) return;
    if (d.act === 'next') return continueNext();
    if (d.act === 'again') { if (ME.replacePage) ME.replacePage(BASE + '/new?kind=variation'); else location.replace(BASE + '/new?kind=variation'); return; }
    if (S.done && !d.go && !d.stepnav && !d.filter && !d.open) return;
    if (d.mode) { if (S.mode !== d.mode) { var keep = { gcode: S.gcode, gname: S.gname }; S = fresh(); S.mode = d.mode; if (d.mode === 'new') Object.assign(S, keep); else searchGroups(); full(); } return; }
    if (d.pick) return pickGroup(d.pick);
    if (d.gotopick) { S.mode = 'add'; return pickGroup(d.gotopick); }
    if (d.act === 'repick') { S = fresh(); S.mode = 'add'; full(); searchGroups(); setTimeout(function () { $('#g-q').focus(); }, 0); return; }
    if (d.act === 'split') return doSplit();
    if (d.act === 'unsplit') return unSplit();
    if (d.act === 'janpaste') { S.janPaste = !S.janPaste; S.janMsg = ''; update(); if (S.janPaste) setTimeout(function () { var jp = $('#jan-paste'); if (jp) jp.focus(); }, 0); return; }
    if (d.act === 'janapply') return janApply();
    if (d.axv) { S.axisName[+d.axv] = d.v; renderAxes(); update(); return; }
    if (d.fix != null) {
      var i = +d.fix; var changed = [];
      S.optText[i] = S.optText[i].split('\n').map(function (ln, j) { var nl = ln.replace(/^(.*\S)(\s*[\t\/／,，、]\s*)([A-Za-z0-9]{1,10})\s*$/, '$1$2-$3'); if (nl !== ln) changed.push(j + 1); return nl; }).join('\n');
      $('#opt-' + i).value = S.optText[i]; S.flash = { i: i, lines: changed }; update(); return;
    }
    if (d.mx) { var k = M.kids.filter(function (x) { return x.key === d.mx; })[0]; if (k && k.prev) { if (S.optIn.has(d.mx)) S.optIn.delete(d.mx); else S.optIn.add(d.mx); } else if (S.excluded.has(d.mx)) S.excluded.delete(d.mx); else S.excluded.add(d.mx); update(); return; }
    if (d.filter) { S.filter = d.filter; update(); if (b.closest('.kidtop')) go('#kfilter'); return; }
    if (d.open) { S.open = S.open === d.open ? null : d.open; update(); if (S.open) { var r = $('#kids-box .krow[data-key="' + CSS.escape(d.open) + '"] [data-k="price"]'); if (r) r.focus(); } return; }
    if (d.reset) { var e2 = S.edits[d.key]; if (e2) { if (d.reset === 'name') { delete e2.manual; delete e2.name; } else delete e2[d.reset]; } update(); return; }
    if (d.cf) { S.common[d.cf] = d.v; S.picks[d.cf] = true; syncInputs(); update(); return; }
    if (d.stepnav) { var cur = stepNavCur(); var nx = Math.max(1, Math.min(7, cur + Number(d.stepnav))); go(STEPS[nx - 1][2]); return; }
    if (d.go) { go(d.go); return; }
    if (b.id === 'savebox-toggle') { var o = $('#savebox').classList.toggle('open'); b.setAttribute('aria-expanded', o); return; }
    if (b.id === 'save' || b.id === 'save2') return openConfirm();
    if (b.matches('#jump a')) { e.preventDefault(); go(b.getAttribute('href')); }
  });
  document.addEventListener('keydown', function (e) { if (e.key === 'Escape' && $('#confirm-bg').classList.contains('on')) closeConfirm(); });

  // ---------- 保存 ----------
  function uuid() {
    if (window.crypto && crypto.randomUUID) return crypto.randomUUID();
    var b = new Uint8Array(16); crypto.getRandomValues(b); b[6] = (b[6] & 15) | 64; b[8] = (b[8] & 63) | 128;
    var h = Array.prototype.map.call(b, function (x) { return x.toString(16).padStart(2, '0'); }).join('');
    return h.slice(0, 8) + '-' + h.slice(8, 12) + '-' + h.slice(12, 16) + '-' + h.slice(16, 20) + '-' + h.slice(20);
  }
  // 登録 1 回に 1 つ (通信が切れて押し直しても 2 回入らない)。返事が来た後にもう一度押すときは新しい番号
  var requestId = uuid();
  var val = function (id) { var e = document.getElementById(id); return e ? e.value.trim() : ''; };
  function collect(m) {
    var C = S.common;
    var values = { standard_price: C.price, tax_rate: C.tax, sales_class: C.sales, primary_supplier: C.supplier, reorder_months: C.months, expiry_managed: C.expiry };
    if (C.inbound !== '') values.inbound_date_managed = C.inbound;
    if (C.ship) values.shipping_code = C.ship;
    if (String(C.cost).trim() !== '') values.cost = { jpy: C.cost };
    var opts = [];
    m.axes.forEach(function (ax) {
      if (m.firstTime) ax.old.forEach(function (o) { opts.push({ axis: ax.i + 1, code: o.num, name: o.name }); });
      ax.good.forEach(function (o) { opts.push({ axis: ax.i + 1, code: o.num, name: o.name }); });
    });
    var amz = val('amazon-url');
    var asin = (amz.match(/\/(?:dp|gp\/product)\/([A-Za-z0-9]{10})(?:[/?]|$)/) || [])[1] || '';
    var out = {
      request_id: requestId, reason: val('reason'),
      group: m.add ? { mode: 'add', product_id: m.pk.id } : { mode: 'new', code: S.gcode, name: S.gname.trim() },
      options: opts,
      children: m.live.map(function (k) {
        var ch = { 1: k.h.num }; if (k.v) ch[2] = k.v.num;
        return { code: k.code, choices: ch, name: k.name, price: k.ownPrice ? k.price : '', cost: k.ownCost ? k.cost : '', jan: k.jan };
      }),
      values: values,
      card: { official_url: val('official-url'), amazon_url: amz, asin: asin ? asin.toUpperCase() : '' },
    };
    if (!m.add || m.firstTime) out.axes = m.axes.map(function (ax) { return { axis: ax.i + 1, name: ax.name }; });
    var po = ME.orderSettings ? ME.orderSettings.collect() : null;
    if (po) out.order_settings = po;
    return out;
  }
  function openConfirm() {
    if (!canSave || S.done || busy) return;
    var m = model();
    if (m.issues.length) { go('#sec-save'); return; }
    if (!ready()) { toast('DB で確かめています。少し待ってからもう一度'); return; }
    $('#confirm-fixed').innerHTML = fixed3(m, true);
    $('#confirm-body').innerHTML = summaryRows(m).map(function (r) { return '<tr><th>' + r[0] + '</th><td>' + r[1] + '</td></tr>'; }).join('');
    $('#confirm-yes').disabled = true;
    $('#confirm-bg').classList.add('on');
    setTimeout(function () { $('#fix-ok').focus(); }, 0);
  }
  function closeConfirm() { $('#confirm-bg').classList.remove('on'); }
  /** サーバーの誤りの項目 → 直す欄 */
  function fieldTarget(j) {
    var f = String(j.field || '');
    if (f === 'group.code') return '#g-code';
    if (f === 'group') return S.mode === 'add' ? '#sec-group' : '#g-code';
    if (/^axes\.\d$/.test(f)) return '#ax-' + (Number(f.slice(5)) - 1);
    if (f === 'axes') return '#sec-axes';
    if (/^options\.\d$/.test(f)) return '#opt-' + (Number(f.slice(8)) - 1);
    if (f === 'options') return '#sec-opts';
    if (f === 'children' && j.code && M) { var k = M.kids.filter(function (x) { return norm(x.code) === norm(j.code); })[0]; return k ? '#kid-' + k.idx : '#sec-kids'; }
    if (f === 'children') return '#sec-kids';
    var MAP = { standard_price: '#c-price', cost: '#c-cost', tax_rate: '#c-tax', sales_class: '#c-sales', primary_supplier: '#f-primary_supplier', reorder_months: '#c-months', expiry_managed: '#c-expiry', inbound_date_managed: '#c-inbound', shipping_code: '#c-ship' };
    if (MAP[f]) return MAP[f];
    if (/^card/.test(f)) return '#official-url';
    if (/^order_settings/.test(f)) { var row = document.querySelector('[data-row="' + f.replace(/"/g, '') + '"]'); return row ? '[data-row="' + f.replace(/"/g, '') + '"]' : '#sec-order'; }
    return '#sec-save';
  }
  function lockAfterSave() { $$('input, select, textarea', form).forEach(function (x) { if (!x.closest('#result')) { x.dataset.wasDisabled = x.disabled ? '1' : ''; x.disabled = true; } }); }
  function unlockAfterSave() { S.done = null; $$('input, select, textarea', form).forEach(function (x) { if (!x.closest('#result') && x.dataset.wasDisabled !== '1') x.disabled = false; }); }
  function doSave() {
    if (!$('#fix-ok').checked || busy) return;
    var m = model();
    var body = collect(m);
    var yes = $('#confirm-yes'); yes.textContent = '保存しています…'; yes.classList.add('busy'); yes.disabled = true;
    busy = true; renderHud(m);
    fetch(BASE + '/api/new/variation', { method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json' }, body: JSON.stringify(body) })
      .then(function (r) { return r.json().catch(function () { return {}; }).then(function (j) { return { r: r, j: j }; }); })
      .then(function (x) {
        busy = false;
        yes.textContent = 'コードを確定して下書き保存'; yes.classList.remove('busy');
        closeConfirm();
        requestId = uuid();   // 返事が来た = この番号の登録は終わった
        if (x.r.ok && x.j.ok) {
          S.done = x.j;
          lockAfterSave();
          var u = unitOf(m);
          var kids = x.j.children || [];
          var po = x.j.order_settings;
          $('#result').innerHTML = '<div class="result ok"><div class="rt">' + icon('check', 's') + ' ' + kids.length + ' 件を下書きにしました' + (x.j.replayed ? ' (前の同じ保存の結果)' : '') + '</div><ul>'
            + '<li>まとまり ' + esc(x.j.group_code) + (x.j.group_created ? ' を作りました' : ' に足しました (今ある ' + (m.pk ? m.pk.kids.length : 0) + ' 件は変更なし)') + ' (軸・選択肢も一緒に記録)</li>'
            + '<li>次は「NE 登録の CSV」で、この回の ' + kids.length + ' 件を 1 ファイルにして NE に取り込みます' + (S.nextOpts.length ? ' (回ごとに 1 ファイル = 残りを足す前に、この回のファイルを作っておくと分けられます)' : '') + '</li>'
            + '<li>出品カードはまとまりで 1 枚 (NE の写し待ち)</li>'
            + (po ? (po.ok ? '<li>発注の設定を ' + po.written + ' 件に入れました</li>' : '<li style="color:var(--warn)">発注の設定だけ ' + po.failed.length + ' 件入れられませんでした (' + esc(po.failed.slice(0, 3).map(function (f) { return f.code; }).join('・')) + ')。商品の画面の「発注の設定」で入れてください</li>') : '')
            + '</ul><div class="kidchips" style="margin:6px 0 10px">' + kids.slice(0, 12).map(function (k) { return '<a class="compchip" href="' + BASE + '/sku/' + encodeURIComponent(k.code) + '"><span class="mono">' + esc(k.code) + '</span></a>'; }).join('') + (kids.length > 12 ? '<span class="hint">… ほか ' + (kids.length - 12) + ' 件</span>' : '') + '</div>'
            + (S.nextOpts.length ? '<button type="button" class="btn pri" data-act="next">' + icon('plus', 's') + '残りの ' + S.nextOpts.length + ' ' + u + 'を足す (このまとまりに続けて)</button> ' : '')
            + '<a class="btn soft sm" href="' + BASE + '/reg-csv">' + icon('send', 's') + 'NE 登録の CSV へ</a> <button type="button" class="btn ghost sm" data-act="again">' + icon('plus', 's') + '別のまとまりを登録する</button></div>';
          update();
          go('#result');
          return;
        }
        var j = x.j || {};
        $('#result').innerHTML = '<div class="result err" role="alert"><div class="rt">' + esc(j.error || ('HTTP ' + x.r.status)) + '</div></div>';
        update();
        var tgt = fieldTarget(j);
        if (tgt && tgt !== '#sec-save') go(tgt); else go('#result');
      })
      .catch(function () {
        busy = false; yes.textContent = 'コードを確定して下書き保存'; yes.classList.remove('busy'); yes.disabled = !$('#fix-ok').checked;
        renderHud(model());
        $('#result').innerHTML = '<div class="result err" role="alert"><div class="rt">通信できませんでした。もう一度「コードを確定して下書き保存」を押してください (同じ登録は 2 回入りません)</div></div>';
      });
  }
  ME.onSave = canSave ? function () { openConfirm(); } : null;
  ME.review = function () { go('#sec-save'); };
  /** 未保存 = 何か入れた (保存した後は 0) */
  ME.dirty = function () {
    if (S.done) return { n: 0, items: [], impacts: [] };
    var items = [];
    if (S.mode === 'new' ? (S.gcode || S.gname) : S.pk) items.push('まとまり (' + (S.mode === 'new' ? S.gcode || S.gname : S.pk.code) + ')');
    if (S.optText[0].trim() || S.optText[1].trim()) items.push('選択肢');
    if (FIELDS.some(function (f) { return S.common[f] !== COMMON0[f]; }) && S.mode === 'new') items.push('共通の欄');
    if (Object.keys(S.edits).length) items.push('子の一覧の変更');
    if (ME.orderSettings && ME.orderSettings.collect()) items.push('発注の設定');
    return { n: items.length, items: items, impacts: [] };
  };

  // いまどこか (飛び先の帯・スマホの「4/7 選択肢」)
  var secIds = ['sec-kind', 'sec-group', 'sec-axes', 'sec-opts', 'sec-common', 'sec-kids', 'sec-save'];
  var extra = { 'sec-order': 'sec-common', 'sec-card': 'sec-common' };
  var curId = 'sec-kind';
  var curStep = function () { return secIds.indexOf(curId) + 1; };
  function markCur() {
    $$('#jump a').forEach(function (a) { a.classList.toggle('cur', a.getAttribute('href') === '#' + curId); });
    var s = curStep();
    $('#jumpm-now').innerHTML = s + '/7 ' + STEPS[s - 1][1] + '<small>' + esc(M ? stepSub(M, s) : '') + '</small>';
    $('[data-stepnav="-1"]').disabled = s <= 1; $('[data-stepnav="1"]').disabled = s >= 7;
  }
  if ('IntersectionObserver' in window) {
    var vis = new Map();
    var io = new IntersectionObserver(function (ents) {
      ents.forEach(function (en) { vis.set(en.target.id, en.isIntersecting ? en.intersectionRatio : 0); });
      var best = null, br = 0;
      vis.forEach(function (r, id) { if (r > br) { br = r; best = extra[id] || id; } });
      if (best) { curId = best; markCur(); }
    }, { threshold: [0, 0.1, 0.25, 0.5, 0.75], rootMargin: '-120px 0px -35% 0px' });
    secIds.concat(['sec-order', 'sec-card']).forEach(function (id) { var el = document.getElementById(id); if (el) io.observe(el); });
  }

  full();
  markCur();
})();
