/*
 * me-new.js — 新商品の登録の画面 (単品・セット) の動き (第 2 段 10/5)
 *   - 未保存の数 = data-dirty-field を付けた欄だけ (保存の理由・種類の札・全体から探す は数えない)。セットの構成は「構成の中身どうし」をくらべる
 *   - 右の「下書き保存まで あと N つ」= ① 下書きに要る ② NE 登録の CSV までに要る ③ 出品カードに要る ④ そのほか (🆕 2026-10-13: ②〜④ も「値か入力なし」になるまで保存は押せない)
 *   - 上の飛び先の帯 = 区切りごとに全部の項目がそろっていれば緑・足りなければ黄色
 *   - 保存の送り方は前と同じ (POST /api/new { request_id, kind, code, reason, values, card })。
 *     🆕 2026-10-08 中原さん: 単品の代表の仕入先は ① (必須)・発送方法は ③ (あとでも可・届いてサイズを見てから)。サーバー (lib/master-register.mjs) と DB (0060) も同じ決まり
 *     🆕 2026-10-08 中原さん「下書き保存時にわかる内容だから必須にする」: 単品の売上分類 (「あとで」は無い)・推奨保有月数・単品のロジザードの有効期限の管理 (初めは選んでいない) も ①。
 *     サーバー (lib/master-register.mjs) と DB (0061) も同じ決まり。セットの売上分類は構成品から (今までどおり)
 *   - 保存が通ったら、できた商品の画面へ移る (今の画面の履歴を置き換える = 戻る 1 回で前の画面)。商品の画面の上に結果を 1 回だけ出す
 *   🆕 2026-10-13 中原さん「全部そろわないと入力できない」: 必須でない項目は どれも「値」か「入力なし」(public/me-none.js) のどちらか。
 *     どちらでもない項目があるうちは保存を押せない (あと N つに数える)。並び = ① 必須 → ② NE 登録の CSV までに要る → ③ 出品カードに要る → ④ そのほか。
 *     入力なしにした項目は「なし」の印。送る本文の blanks = 入力なしにした項目 (サーバー = lib/master-register-blanks.mjs が同じ決まりでもう一度確かめる)。
 *     カードを作らない = カードの欄 (Yahoo! も) は数えない・送らない。発注の設定は欄が出る (書ける) ときだけ (public/me-order.js の items)
 *   🆕 2026-10-17 中原さん: 「セット商品を作るか」= 作る のときは「作るセット」(個数・セット商品コード・セット商品名) を 1 つ以上 (③ に数える)。
 *     コード = 単品のコード-個数・名前 = 単品の名前 個数個セット を初めに入れる (直すまでは単品のコード・名前に付いていく)。決まり = lib/set-plan.mjs (サーバーも同じ)。
 *     コードは打つとその場で確かめる (/api/set-code-check = Company DB・NE・product-hub)。送る = card.sets (作る かつ カードを作るときだけ)
 */
(function () {
  'use strict';
  var $ = function (s, r) { return (r || document).querySelector(s); };
  var $$ = function (s, r) { return Array.prototype.slice.call((r || document).querySelectorAll(s)); };
  function esc(s) { return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; }); }
  var ME = window.MasterEdit = window.MasterEdit || {};
  var dataEl = document.getElementById('me-new');
  var form = document.getElementById('f');
  var scope = document.getElementById('new-page');
  if (!dataEl || !form || !scope) return;
  var P = JSON.parse(dataEl.textContent);
  var BASE = P.base;
  var isSet = P.kind === 'set';
  var canSave = form.getAttribute('data-can-save') === '1';
  var half = function (s) { return String(s == null ? '' : s).replace(/[０-９．，]/g, function (c) { return c === '，' ? ',' : String.fromCharCode(c.charCodeAt(0) - 0xFEE0); }); };
  var yen = function (v) { return v == null || v === '' || isNaN(Number(v)) ? '' : Number(v).toLocaleString('ja-JP'); };
  var HANDLING = { active: '取扱中', discontinued: '中止', unknown: '不明' };
  var SALES = { 1: '自社', 2: '取引先限定', 3: '仕入', 4: '輸出' };
  var val = function (id) { var e = document.getElementById(id); return e ? e.value.trim() : ''; };

  /* ---------- 欄の値・未保存 ---------- */
  function components() {
    return $$('#comp-rows tr.comp-row').map(function (tr) { return { code: $('.c-code', tr).value.trim(), qty: $('.c-qty', tr).value.trim() }; })
      .filter(function (r) { return r.code || (r.qty && r.qty !== '1'); });
  }
  function refUrls() { return $$('#ref-rows .ref-url').map(function (x) { return x.value.trim(); }).filter(Boolean); }
  function valueOf(el) {
    if (el.id === 'comp') return JSON.stringify(components());
    if (el.id === 'refs') return JSON.stringify(refUrls());
    if (el.id === 'set-plans') return JSON.stringify(setPlanRows().map(function (r) { return [r.qty, r.code, r.name]; }));
    if (el.classList.contains('seg')) return el.getAttribute('data-value') || '';
    if (el.type === 'checkbox') return el.checked ? '1' : '';
    return el.value;
  }
  function tracked() { return $$('[data-dirty-field]', scope).filter(function (el) { return !el.disabled; }); }
  var initial = new Map();
  tracked().forEach(function (el) { initial.set(el, valueOf(el)); });
  var saved = false;
  function dirtyEls() { return saved ? [] : tracked().filter(function (el) { return initial.has(el) && valueOf(el) !== initial.get(el); }); }
  function segText(seg, v) { var b = $$('button', seg).filter(function (x) { return x.getAttribute('data-v') === v; })[0]; return b ? b.textContent.replace(/\s+/g, ' ').trim() : v; }
  function shown(el) {
    var v = valueOf(el);
    if (el.id === 'comp') { var rows = components(); return rows.length ? rows.map(function (r) { return r.code + '×' + r.qty; }).join(', ') : 'なし'; }
    if (el.id === 'refs') return refUrls().length + ' 個';
    if (el.id === 'set-plans') { var sp = setPlanRows(); return sp.length ? sp.map(function (r) { return r.code + ' (' + r.qty + '個)'; }).join(', ') : 'なし'; }
    if (el.classList.contains('seg')) return segText(el, v);
    if (el.tagName === 'SELECT') { var o = $$('option', el).filter(function (x) { return x.value === v; })[0]; return o ? o.textContent.trim() : v; }
    if (el.id === 'f-maker' && ME.maker) return ME.maker.label(v);   // 🆕 0084: 名簿の番号 → 名前
    return v === '' ? '(空)' : v;
  }

  /* ---------- ① ② ③ ---------- */
  // 🆕 2026-10-08 夜 (Company DB構想 20 §③・0064): 大文字も使える。サーバーの validateNewSkuCode と同じ決まり (重なりはサーバーが小文字にそろえて確かめる)
  var CODE_RE = /^[A-Za-z0-9_-]{1,30}$/;
  var codeState = { code: null, ok: null, message: '' };   // サーバーの確かめ (打った文字ごと)
  function codeShape(s) {
    if (!s) return '商品コードを入れてください';
    if (s !== s.trim()) return '前後に空白があります';
    if (!CODE_RE.test(s)) return '使える文字は英字 (大文字・小文字)・数字・- と _ だけ (30 字まで)';
    if (/^set-/i.test(s)) return 'set- で始まるコードは使えません';
    return '';
  }
  /**
   * 🆕 0080: ロジザードのバーコードの欄 → 送る形 (body) か 形の一言 (problem)。サーバーの lib/logizard-barcode.mjs lzValueShape と同じ決まり
   *   FNSKU = X00 + 英数字 7 字 (大文字にそろえる)・JAN = 8 / 13 けた + チェック数字・「空欄」の印 = { kind: 'lz_assign' }
   */
  function lzbc() {
    var inEl = $('#f-lzbc-in'), bl = $('#f-lzbc-blank');
    if (!inEl) return { body: null, problem: '' };
    if (bl && bl.checked) return { body: { kind: 'lz_assign' }, problem: '' };
    var v = String(inEl.value || '').normalize('NFKC').trim().toUpperCase();
    if (!v) return { body: null, problem: '' };
    if (/^X00[0-9A-Z]{7}$/.test(v)) return { body: { kind: 'value', value: v }, kind: 'fnsku' };
    if (/^\d+$/.test(v)) {
      if (v.length !== 8 && v.length !== 13) return { body: null, problem: 'JAN は 8 けたか 13 けた (いま ' + v.length + ' けた)' };
      var d = v.split('').map(Number), ck = d.pop(), sum = 0;
      for (var i = d.length - 1, w = 3; i >= 0; i--, w = w === 3 ? 1 : 3) sum += d[i] * w;
      return (10 - (sum % 10)) % 10 === ck ? { body: { kind: 'value', value: v }, kind: 'jan' } : { body: null, problem: 'JAN の最後の数字 (チェック数字) が合いません' };
    }
    return { body: null, problem: /^X0/.test(v) ? 'FNSKU は X00 + 英数字 7 字の 10 文字' : 'FNSKU (X00…) か JAN (数字) を入れてください' };
  }
  /** 形の一言を出す。typed = 値を打った (「空欄」の印を外す = 両方は持たない) */
  function lzbcShow(typed) {
    var inEl = $('#f-lzbc-in'), bl = $('#f-lzbc-blank'), m = $('#f-lzbc-msg');
    if (!inEl) return;
    if (typed && bl && bl.checked && inEl.value.trim()) bl.checked = false;
    var r = lzbc();
    if (m) m.textContent = bl && bl.checked ? 'ロジザードが番号を付けます' : r.kind === 'fnsku' ? 'FNSKU の形' : r.kind === 'jan' ? 'JAN の形 (チェック数字 OK)' : r.problem || '';
  }
  function segVal(id) { var s = document.getElementById(id); return s ? s.getAttribute('data-value') || '' : ''; }
  function cardOn() { return segVal('card-create') !== '0'; }
  function priceOk(id) { var v = half(val(id)).replace(/,/g, ''); return /^\d+$/.test(v) && Number(v) >= 1; }
  /** 推奨保有月数の形 (サーバーの PARSERS.reorder_months と同じ: 0〜60・小数は 1 桁まで) */
  function monthsOk() { var v = half(val('f-reorder_months')); return /^\d{1,2}(\.\d)?$/.test(v) && Number(v) <= 60; }
  /** 項目 = { id, sec, t (名前), ok, focus (セレクタ), level 1|2|3 } */
  function items() {
    var code = $('#code').value;
    var list = [
      { id: 'code', sec: 'sec-code', t: '商品コード', ok: !codeShape(code) && !(codeState.code === code && codeState.ok === false), focus: '#code', level: 1 },
      { id: 'name', sec: 'sec-code', t: '名前', ok: !!val('f-name'), focus: '#f-name', level: 1 },
    ];
    if (isSet) list.push({ id: 'components', sec: 'sec-comp', t: '構成 (1 品以上)', ok: components().some(function (r) { return r.code; }), focus: '#comp-rows .c-code', level: 1 });
    if (isSet) {
      var cc = compCheck();
      if (cc.pending.length) list.push({ id: 'comp_pending', sec: 'sec-comp', t: '構成品を確かめています (' + cc.pending.length + ' 行)', ok: false, focus: '#comp-rows tr.comp-row:nth-child(' + (cc.pending[0].sectionRowIndex + 1) + ') .c-code', level: 1 });
      else if (cc.bad.length) list.push({ id: 'comp_bad', sec: 'sec-comp', t: '構成品にできないコードを直す (' + cc.bad.length + ' 行)', ok: false, focus: '#comp-rows tr.comp-row:nth-child(' + (cc.bad[0].sectionRowIndex + 1) + ') .c-code', level: 1 });
    }
    if (isSet && salesFromComp != null && val('f-set_sales_class_override')) list.push({ id: 'override', sec: 'sec-comp', t: '売上分類の上書きを空にする (構成品から導けます)', ok: false, focus: '#f-set_sales_class_override', level: 1 });
    list.push({ id: 'standard_price', sec: 'sec-money', t: '売価', ok: priceOk('f-standard_price'), focus: '#f-standard_price', level: 1 });
    if (!isSet) list.push({ id: 'tax_rate', sec: 'sec-tax', t: '税率', ok: !!segVal('f-tax_rate'), focus: '#f-tax_rate button', level: 1 });
    if (!isSet) list.push({ id: 'primary_supplier', sec: 'sec-tax', t: '代表の仕入先', ok: !!val('f-primary_supplier'), focus: '#f-primary_supplier', level: 1 });
    // 🆕 2026-10-08 中原さん「下書き保存時にわかる内容だから必須にする」: 単品の売上分類・推奨保有月数 (単品もセットも)・単品のロジザードの有効期限の管理は ①。
    //   セットの売上分類は構成品から導く (導けないときだけ「例外のとき」の上書き = 保存でサーバーが確かめる) = ここには足さない
    if (!isSet) list.push({ id: 'sales_class', sec: 'sec-tax', t: '売上分類', ok: !!segVal('f-sales_class'), focus: '#f-sales_class button', level: 1 });
    list.push({ id: 'reorder_months', sec: 'sec-ship', t: '推奨保有月数 (0〜60)', ok: monthsOk(), focus: '#f-reorder_months', level: 1 });
    if (!isSet) list.push({ id: 'expiry_managed', sec: 'sec-ship', t: 'ロジザードの有効期限の管理', ok: !!segVal('f-expiry'), focus: '#f-expiry button', level: 1 });
    // 🆕 0080: ロジザードのバーコード = 値か「空欄」のどちらかが要る (欄がある = 0080 がある DB だけ)
    if (!isSet && $('#f-lzbc-in')) list.push({ id: 'logizard_barcode', sec: 'sec-ship', t: 'ロジザードのバーコード (値か「空欄」)', ok: !!lzbc().body, focus: '#f-lzbc-in', level: 1 });
    // 🆕 2026-10-13: 必須でない項目 = 値か「入力なし」。ok = どちらか / none = 入力なし (印「なし」)。原価は 1 円以上の形も見る (0 = 値でない)
    var N = ME.none;
    var opt = function (key, sec, t, focus, level, okValue) {
      if (!N || !N.present(key)) return;
      var none = N.on(key) && !N.has(key);
      list.push({ id: key, sec: sec, t: t, ok: none || (okValue == null ? N.has(key) : okValue), none: none, focus: focus, level: level, opt: true });
    };
    var card = cardOn();
    if (!isSet) opt('cost', 'sec-money', '原価 (1 円以上)', '#cost-jpy', 2, priceOk('cost-jpy'));
    if (card) {
      opt('card.amazon', 'sec-card', 'Amazon URL か ASIN', '#amazon-url', 3);
      opt('card.official_url', 'sec-card', '公式ページ URL', '#official-url', 3);
    }
    // 発送方法 = 出品カードの楽天の配送方法にも使う (届いてサイズを見てから = 入力なし・商品の画面で入れる)。カードを作らないときは ④
    opt('shipping_code', 'sec-ship', '発送方法 (届いてからでも可)', document.getElementById('shipping') ? '#shipping' : '[data-row="shipping_code"] .nonechk input', card ? 3 : 4);
    if (!isSet && card) opt('card.set_decision', 'sec-card', 'セット商品を作るか', '#set-plan button', 3);
    // 🆕 2026-10-17: 「作る」= 作るセットが 1 つ以上・全部の行の形が正しい (サーバーの確かめで「使えない」と分かった行も直すまで止める。確かめ中・確かめられない = 保存のときにサーバーがもう一度)
    if (!isSet && card && segVal('set-plan') === 'create') {
      var spc = setPlanCheck();
      list.push({ id: 'card.sets', sec: 'sec-card', t: spc.rows === 0 ? '作るセット (個数・コード・名前を 1 つ以上)' : spc.bad ? '作るセットを直す (' + spc.bad + ' 行)' : '作るセット (' + spc.rows + ' 個)',
        ok: spc.rows > 0 && spc.bad === 0, focus: spc.firstBad || '#set-plan-add', level: 3 });
    }
    // ④ そのほか (画面の上からの順)
    if (isSet) opt('exception_cost', 'sec-comp', '例外原価 (入力なし = 構成品の合計)', '#xcost-jpy', 4);
    // 🆕 0084: メーカー (代表の仕入先のすぐ下・値 = 名簿から選んだ = 打っただけでは値でない)。欄がある (0084 がある DB) ときだけ
    if (!isSet) opt('maker', 'sec-tax', 'メーカー (一覧から選ぶ)', '#f-maker-in', 4);
    if (!isSet && ME.orderSettings && ME.orderSettings.items) {
      ME.orderSettings.items().forEach(function (x) { list.push({ id: x.key, sec: 'sec-order', t: x.t, ok: x.ok, none: x.none, focus: x.focus, level: 4, opt: true }); });
    }
    if (!isSet) opt('inbound_date_managed', 'sec-ship', 'ロジザードの入荷日の管理', '#f-inbound button', 4);
    if (card) {
      opt('card.reference_urls', 'sec-card', '参考 URL', '#ref-rows .ref-url', 4);
      [['card.yahoo.price', 'Yahoo!売価', '#y-price'], ['card.yahoo.price_sagawa', 'Yahoo!売価 (佐川)', '#y-price-sagawa'], ['card.yahoo.delivery_label', 'Yahoo!の配送方法', '#y-delivery'],
        ['card.yahoo.category_id', 'Yahoo!カテゴリID', '#y-category'], ['card.yahoo.path', 'Yahoo!path', '#y-path']].forEach(function (y) { opt(y[0], 'sec-yahoo', y[1], y[2], 4); });
    }
    return list;
  }
  /** 送る「入力なし」の印 = 数えている項目のうち入力なしにしたもの (カードを作らない = カードの欄は送らない) */
  function blanks() {
    var order = P.blankKeys || [];   // サーバーと同じ並び (lib/master-register-blanks.mjs の BLANK_KEYS)
    return items().filter(function (x) { return x.opt && x.none; }).map(function (x) { return x.id; }).sort(function (a, b) { return order.indexOf(a) - order.indexOf(b); });
  }

  /* ---------- 画面を描き直す ---------- */
  var saveBtn = $('#save'), busy = false;
  var lastState = { n: 0, items: [], impacts: [] };
  var drawn = {};
  /** 中身が同じなら描き直さない (押している最中のボタンを消すと押しても効かない = 欄を離れたときの change で描き直していた) */
  function paint(sel, html) { if (drawn[sel] === html) return; drawn[sel] = html; $(sel).innerHTML = html; }
  var mark = function (ok, none) { return '<span class="mark" aria-hidden="true">' + (none ? 'なし' : ok ? '<svg class="ic"><use href="#i-check"/></svg>' : '') + '</span>'; };
  function update() {
    if (ME.none) ME.none.refresh();
    var it = items();
    // 🆕 2026-10-13: 数えるのは全部の項目 (① = 値がない / ②〜④ = 値も入力なしもない)
    var need = it.filter(function (x) { return !x.ok; });
    var h = '';
    [[1, '① 下書きに要る (必須)', ''], [2, '② NE 登録の CSV までに要る', '値か入力なし'], [3, '③ 出品カードに要る', '値か入力なし'], [4, '④ そのほかの項目', '値か入力なし']].forEach(function (g) {
      var xs = it.filter(function (x) { return x.level === g[0]; });
      if (g[0] === 2 && isSet) { h += '<li class="grp2">' + g[1] + '<span>構成品から</span></li><li class="hint" style="padding:2px 10px 4px">構成品が全部 NE 確認済みなら作れます (NE 登録の CSV の画面で確かめます)</li>'; return; }
      if (g[0] === 3 && !cardOn()) { h += '<li class="grp2">' + g[1] + '<span>カードを作らない</span></li>'; }
      if (!xs.length) return;
      if (!(g[0] === 3 && !cardOn())) h += '<li class="grp2">' + g[1] + '<span>' + xs.filter(function (x) { return x.ok; }).length + ' / ' + xs.length + '</span></li>';
      xs.forEach(function (x) {
        h += '<li class="' + (x.none ? 'done none' : x.ok ? 'done' : 'todo need') + (x.opt ? ' opt' : '') + '"><button type="button" data-focus="' + esc(x.focus) + '">'
          + mark(x.ok, x.none) + '<span class="lbl">' + esc(x.t) + '</span>'
          + (x.none ? '<span class="nonetag">入力なし</span>' : x.ok ? '<span class="sr">入っています</span>' : '<span class="go">' + (x.opt ? '値か入力なし' : '入れる') + ' →</span>') + '</button></li>';
      });
    });
    paint('#checklist', h);
    var rm = $('#remain');
    rm.classList.toggle('ok', need.length === 0);
    var rh = need.length ? '下書き保存まで あと<b>' + need.length + '</b>つ' : '<b><svg class="ic" aria-hidden="true" style="width:20px;height:20px;vertical-align:-3px"><use href="#i-check"/></svg></b>下書きを保存できます';
    paint('#remain', rh);
    // 飛び先の帯・区切りの番号: 足りない項目 (値も入力なしもない) がある区切り = 黄色・全部そろった = 緑
    $$('#jump a[data-sec]').forEach(function (a) {
      var sec = a.getAttribute('data-sec');
      var mine = it.filter(function (x) { return x.sec === sec; });
      var todo = mine.some(function (x) { return !x.ok; });
      a.classList.toggle('todo', todo); a.classList.toggle('done', mine.length > 0 && !todo);
      var st = $('[data-step="' + sec + '"]'); if (st) st.classList.toggle('done', mine.length > 0 && !todo);
    });
    // 選ぶまで「まだ選んでいません」を出す切り替え (税率・売上分類・ロジザードの有効期限の管理)
    [['tax-miss', 'f-tax_rate', 'tax_rate'], ['sales-miss', 'f-sales_class', 'sales_class'], ['expiry-miss', 'f-expiry', 'expiry_managed']].forEach(function (m) {
      var on = !!segVal(m[1]);
      var tm = document.getElementById(m[0]); if (tm) tm.hidden = on;
      var row = $('[data-row="' + m[2] + '"]'); if (row && document.getElementById(m[1])) row.classList.toggle('missing', !on);
    });
    // 未保存
    var d = dirtyEls();
    var names = [];
    d.forEach(function (el) { var n = el.getAttribute('data-label') || el.getAttribute('data-dirty-field'); if (names.indexOf(n) < 0) names.push(n); });
    $$('.f[data-row]', scope).forEach(function (f) { f.classList.toggle('dirty', d.some(function (el) { return f.contains(el); })); });
    if (saveBtn) {
      saveBtn.disabled = !canSave || saved || busy || need.length > 0;
      var sh = need.length ? 'あと ' + need.length + ' つ: ' + esc(need[0].t) : '下書きを保存する <span class="kbd" aria-hidden="true">Ctrl</span><span class="kbd" aria-hidden="true">S</span>';
      paint('#save', sh);
    }
    ME.setUnsaved(names.length);
    lastState = { n: names.length, items: d.map(function (el) { return (el.getAttribute('data-label') || '') + ': ' + shown(el); }).filter(function (x, i, a) { return a.indexOf(x) === i; }), impacts: need.length ? ['保存していないので、下書きはまだできていません'] : [], need: need };
    yahooSummary();
  }
  ME.dirty = function () { return { n: lastState.n, items: lastState.items, impacts: lastState.impacts }; };
  ME.recount = function () { update(); };   // 発注の設定の部品 (後から読む) が読み終わったとき
  scope.addEventListener('input', function (e) { if (e.target.id === 'code') codeLater(); if (e.target.id === 'f-lzbc-in') lzbcShow(true); update(); });
  var lzBlankEl = $('#f-lzbc-blank');
  if (lzBlankEl) lzBlankEl.addEventListener('change', function () { if (lzBlankEl.checked) $('#f-lzbc-in').value = ''; lzbcShow(false); });
  scope.addEventListener('change', update);

  /* ---------- ① ② ③ を押すと その欄へ ---------- */
  function focusTo(sel) {
    var el = $(sel); if (!el) return;
    var det = el.closest('details'); if (det) det.open = true;
    el.scrollIntoView({ behavior: 'smooth', block: 'center' });
    try { el.focus({ preventScroll: true }); } catch (e) { el.focus(); }
  }
  $('#checklist').addEventListener('click', function (e) { var b = e.target.closest('button[data-focus]'); if (b) focusTo(b.getAttribute('data-focus')); });
  // 飛び先の帯で Yahoo! へ飛んだら開く
  $$('#jump a[data-sec="sec-yahoo"]').forEach(function (a) { a.addEventListener('click', function () { var d = $('#sec-yahoo'); if (d) d.open = true; }); });

  /* ---------- 商品コードを確かめる (打つとその場で・確かめるのボタン) ---------- */
  var codeMsg = $('#code-msg'), codeTimer = null, codeSeq = 0;
  function sayCode(t, cls) { codeMsg.className = 'fmsg ' + (cls || ''); codeMsg.innerHTML = t ? (cls === 'ok' ? '<svg class="ic s" aria-hidden="true"><use href="#i-check"/></svg>' : cls === 'err' ? '<svg class="ic s" aria-hidden="true"><use href="#i-warn"/></svg>' : '') + esc(t) : ''; }
  function checkCode() {
    var code = $('#code').value;
    var shape = codeShape(code);
    if (shape) { codeState = { code: code, ok: false, message: shape }; sayCode(code ? shape : '', 'err'); update(); return; }
    var my = ++codeSeq;
    sayCode('確かめています…', 'info');
    fetch(BASE + '/api/code-check?code=' + encodeURIComponent(code), { headers: { Accept: 'application/json' } })
      .then(function (r) { return r.json().catch(function () { return {}; }).then(function (j) { return { r: r, j: j }; }); })
      .then(function (x) {
        if (my !== codeSeq || $('#code').value !== code) return;
        if (x.r.ok && x.j.ok) { codeState = { code: code, ok: true }; sayCode('使えます', 'ok'); }
        else if (x.r.status === 200 || x.r.status === 400) { codeState = { code: code, ok: false }; sayCode(x.j.message || x.j.error || '使えません', 'err'); }
        else { codeState = { code: code, ok: null }; sayCode('いまは確かめられません (保存のときにもう一度確かめます)', 'warn'); }
        update();
      })
      .catch(function () { if (my === codeSeq) { codeState = { code: code, ok: null }; sayCode('確かめられません (通信)。保存のときにもう一度確かめます', 'warn'); update(); } });
  }
  function codeLater() { clearTimeout(codeTimer); codeState = { code: null, ok: null }; sayCode(''); codeTimer = setTimeout(checkCode, 450); }
  var cc = $('#code-check'); if (cc) cc.addEventListener('click', function () { clearTimeout(codeTimer); checkCode(); });

  /* ---------- セットの構成 ---------- */
  var rowsEl = $('#comp-rows');
  /**
   * 行 → 引き当てた答え { code (どのコードの答えか), item (見つからない = null), error }。
   * 今のコードの答えが無い行 = 照合中 (打ち直した直後・返事待ち) = ① で保存を止める (#1628 Codex R2 M1: 古い答えの見込みのまま保存して断られる)
   */
  var facts = new Map();
  function factOf(tr) { var f = facts.get(tr); return f && f.code === $('.c-code', tr).value.trim() ? f : null; }
  function compCheck() {
    var rows = $$('#comp-rows tr.comp-row').filter(function (tr) { return $('.c-code', tr).value.trim(); });
    var pending = rows.filter(function (tr) { return !factOf(tr); });
    var bad = rows.filter(function (tr) { var f = factOf(tr); return f && (!f.item || f.item.kind !== 'single'); });
    return { pending: pending, bad: bad };
  }
  function relabel(tr, i) {
    var code = $('.c-code', tr).value.trim(), who = (i + 1) + ' 行目' + (code ? ' (' + code + ')' : '');
    $('.no', tr).textContent = String(i + 1);
    $('.c-code', tr).setAttribute('aria-label', (i + 1) + ' 行目の構成品のコード');
    $('.c-qty', tr).setAttribute('aria-label', who + ' の数');
    var acts = { up: 'を上へ', down: 'を下へ', del: 'を外す' };
    $$('button[data-act]', tr).forEach(function (b) { b.setAttribute('aria-label', who + ' ' + acts[b.getAttribute('data-act')]); });
  }
  function renumber() { $$('#comp-rows tr.comp-row').forEach(relabel); tiles(); update(); }
  /** 確かめ直しの間 (3 秒・10 秒・30 秒の 3 回)。試験だけ window.__meRetryWaits で短くする */
  var RETRY_WAITS = Array.isArray(window.__meRetryWaits) ? window.__meRetryWaits : [3000, 10000, 30000];
  function lookup(tr) {
    var code = $('.c-code', tr).value.trim();
    var set = function (cls, v, bad) { var el = $(cls, tr); el.textContent = v; if (cls === '.c-name') el.classList.toggle('bad', !!bad); };
    clearTimeout(tr.__lookupTimer);
    var f0 = facts.get(tr);
    if (f0 && f0.code === code) return;   // この世代 (打った後) の答えはもう持っている (打つたびに捨てる = 入力の処理)
    facts.delete(tr);
    if (!code) { ['.c-name', '.c-tax', '.c-sales', '.c-cost', '.c-handling'].forEach(function (c) { set(c, ''); }); tiles(); return; }
    var seq = tr.__lookupSeq || 0;   // 世代は打った瞬間・行を消したときに進む。この世代の返事だけ使う
    if (tr.__inflight === seq + '|' + code) return;   // 同じ世代の同じコードをもう聞いている
    tr.__inflight = seq + '|' + code;
    // この世代の返事か (行が画面にある・世代とコードが同じ)
    var current = function () { return tr.isConnected && (tr.__lookupSeq || 0) === seq && $('.c-code', tr).value.trim() === code; };
    /**
     * 確かめられなかった (5xx・通信・401/403)。自動の確かめ直しは 3 秒・10 秒・30 秒の 3 回まで (#1628 Codex R4 M1)。
     * その後と 401/403 (自動で繰り返しても直らない) は「もう一度確かめる」のボタン。どちらも答えにしない = 保存は止めたまま
     */
    var retry = function (msg, auto) {
      tr.__inflight = null;
      if (!current()) return;
      var n = tr.__retryN || 0;
      var wait = RETRY_WAITS[n];
      var el = $('.c-name', tr);
      el.classList.add('bad');
      if (auto && wait) {
        tr.__retryN = n + 1;
        el.textContent = msg + '。' + (wait >= 1000 ? wait / 1000 + ' 秒後' : '少し後') + 'にもう一度確かめます';
        tr.__lookupTimer = setTimeout(function () { if (current()) lookup(tr); }, wait);
      } else {
        el.textContent = msg + '。 ';
        var btn = document.createElement('button');
        btn.type = 'button'; btn.className = 'btn sm'; btn.setAttribute('data-act', 'relookup'); btn.textContent = 'もう一度確かめる';
        el.appendChild(btn);
      }
      tiles(); update();
    };
    var ctl = window.AbortController ? new AbortController() : null;
    if (tr.__abort) tr.__abort.abort();
    tr.__abort = ctl;
    set('.c-name', '引き当てています…');
    fetch(BASE + '/api/lookup?code=' + encodeURIComponent(code), { headers: { Accept: 'application/json' }, signal: ctl ? ctl.signal : undefined })
      .then(function (r) { return r.json().catch(function () { return {}; }).then(function (j) { return { r: r, j: j }; }); })
      .then(function (x) {
        if (!current()) return;
        tr.__inflight = null;
        // 答えにするのは 400 (形が違う)・404 (無い) だけ。ほか (5xx = DB につながらない など・401/403) は答えにしない (#1628 Codex R3 M1・R4 M1)
        if (x.r.status === 401 || x.r.status === 403) { retry('確かめられません (' + (x.j.error || 'HTTP ' + x.r.status) + ')。ログインし直すか、画面を開き直してください', false); return; }
        if (!x.r.ok && x.r.status !== 400 && x.r.status !== 404) { retry('いまは確かめられません (' + (x.j.error || 'HTTP ' + x.r.status) + ')', true); return; }
        tr.__retryN = 0;
        if (!x.r.ok || !x.j.ok) { facts.set(tr, { code: code, item: null }); set('.c-name', x.j.error || '見つかりません', true); ['.c-tax', '.c-sales', '.c-cost', '.c-handling'].forEach(function (c) { set(c, ''); }); tiles(); update(); return; }
        var it = x.j.item;
        facts.set(tr, { code: code, item: it });
        set('.c-name', it.name + (it.kind !== 'single' ? ' (単品でない = 構成品にできません)' : ''), it.kind !== 'single');
        set('.c-tax', it.tax_rate == null ? '未' : Math.round(it.tax_rate * 100) + '%');
        set('.c-sales', it.sales_class == null ? '未' : String(it.sales_class));
        set('.c-cost', it.cost_jpy == null ? '未' : yen(it.cost_jpy) + ' 円');
        set('.c-handling', HANDLING[it.handling] || it.handling);
        tiles(); update();
      })
      .catch(function (err) { if (err && err.name === 'AbortError') return; retry('引き当てできません (通信)', true); });
  }
  /** 構成品から導いた売上分類 (導けない・分からない = null) */
  var salesFromComp = null;
  /** 売上分類の上書きの欄: 構成品から導ける間は押せない (入れてあれば空にできるよう開けておく = ① で止める) */
  function overrideState() {
    var sel = document.getElementById('f-set_sales_class_override'); if (!sel || !canSave || saved) return;
    sel.disabled = salesFromComp != null && !sel.value;
    var hint = document.getElementById('override-hint');
    if (hint) hint.textContent = salesFromComp != null ? '構成品から ' + salesFromComp + ' (' + (SALES[salesFromComp] || '') + ') を導けるので、上書きはできません' : '構成品から導けないときだけ (未入力・輸出 4 と 1〜3 の混在)';
  }
  /** 計算で決まる値の見込み (lib/master-set-rules.js deriveSetCdb と同じ決まり: 税率 = 全部同じならそれ・混ざれば低い 8%・未入力があれば決まらない / 売上分類 = 1〜3 の小さい番号 (4 だけなら 4・4 と 1〜3 の混在は決まらない) / 原価 = 原価 × 数の合計 (0・未入力があれば決まらない)) */
  function tiles() {
    if (!isSet) return;
    var rows = $$('#comp-rows tr.comp-row').map(function (tr) { var c = $('.c-code', tr).value.trim(); var f = factOf(tr); return c ? { f: f ? f.item : null, qty: Number(half($('.c-qty', tr).value.trim())) } : null; }).filter(Boolean);
    var put = function (id, tv, tr, bad) { var t = $('#' + id); if (!t) return; t.classList.toggle('bad', !!bad); $('.tv', t).innerHTML = tv; $('.tr', t).textContent = tr; };
    salesFromComp = null;
    if (!rows.length) { overrideState(); put('t-tax', '—', '構成品を入れると出ます'); put('t-sales', '—', 'いちばん小さい番号'); put('t-cost', '—', '構成品の原価 × 数'); put('t-price', '—', 'セットの売価とくらべる'); return; }
    var known = rows.every(function (r) { return !!r.f; });
    if (!known) { overrideState(); ['t-tax', 't-sales', 't-cost', 't-price'].forEach(function (id) { put(id, '…', '構成品を引き当て中か、見つからない行があります'); }); return; }
    var taxes = rows.map(function (r) { return r.f.tax_rate; });
    if (taxes.some(function (t) { return t == null; })) put('t-tax', '決まりません', '税率が未入力の構成品があります (先に単品の税率を)', true);
    else { var uniq = taxes.filter(function (t, i) { return taxes.indexOf(t) === i; }); put('t-tax', (uniq.length > 1 ? 8 : Math.round(uniq[0] * 100)) + '<small>%</small>', uniq.length > 1 ? '8% と 10% が混ざっています (低い方の 8%)' : '構成品が全部 ' + Math.round(uniq[0] * 100) + '%', uniq.length > 1); }
    // 売上分類: 構成品から導ける間は上書きできない (サーバーが断る = lib/master-register.mjs)。導けない (未入力・輸出 4 と 1〜3 の混在) ときだけ上書きで決める (#1628 Codex R1 M2)
    var ov = val('f-set_sales_class_override');
    var sc = rows.map(function (r) { return r.f.sales_class; });
    var four = sc.filter(function (x) { return Number(x) === 4; }).length;
    var missing = sc.some(function (x) { return x == null; });
    var mixed = !missing && four > 0 && four < sc.length;
    salesFromComp = missing || mixed ? null : Math.min.apply(null, sc.map(Number));
    if (salesFromComp != null && ov) put('t-sales', '上書きできません', '構成品から ' + salesFromComp + ' (' + (SALES[salesFromComp] || '') + ') を導けます。「売上分類の上書き」を空にしてください', true);
    else if (ov) put('t-sales', esc(ov) + '<small> ' + esc(SALES[ov] || '') + '</small>', '上書き (構成品から導けないので)');
    else if (missing) put('t-sales', '決まりません', '売上分類が未入力の構成品があります (「例外のとき」の「売上分類の上書き」で決める)', true);
    else if (mixed) put('t-sales', '決まりません', '輸出 4 と 1〜3 が混ざっています (「例外のとき」の「売上分類の上書き」で決める)', true);
    else put('t-sales', salesFromComp + '<small> ' + esc(SALES[salesFromComp] || '') + '</small>', four ? '全部 輸出' : 'いちばん小さい番号');
    overrideState();
    var xc = val('xcost-jpy');
    if (xc) put('t-cost', esc(yen(half(xc).replace(/,/g, ''))) + '<small> 円</small>', '例外原価 (構成品の合計の代わり)');
    else if (rows.some(function (r) { return !(r.f.cost_jpy > 0); })) put('t-cost', '決まりません', '原価が無い構成品があります (先に単品の原価か、例外原価を)', true);
    else put('t-cost', yen(rows.reduce(function (a, r) { return a + r.f.cost_jpy * (r.qty || 0); }, 0)) + '<small> 円</small>', '構成品の原価 × 数の合計');
    if (rows.some(function (r) { return r.f.standard_price == null; })) put('t-price', '—', '売価の無い構成品があります');
    else {
      var sum = rows.reduce(function (a, r) { return a + r.f.standard_price * (r.qty || 0); }, 0);
      var sp = half(val('f-standard_price')).replace(/,/g, '');
      put('t-price', yen(sum) + '<small> 円</small>', /^\d+$/.test(sp) ? 'セットの売価との差 ' + (Number(sp) - sum >= 0 ? '+' : '−') + yen(Math.abs(Number(sp) - sum)) + ' 円' : 'セットの売価とくらべる');
    }
  }
  if (rowsEl && canSave) {
    rowsEl.addEventListener('click', function (e) {
      var b = e.target.closest('button[data-act]'); if (!b || b.disabled) return;
      var tr = b.closest('tr'), act = b.getAttribute('data-act');
      if (act === 'relookup') { tr.__retryN = 0; tr.__inflight = null; lookup(tr); return; }
      if (act === 'del') { var next = tr.nextElementSibling || tr.previousElementSibling; clearTimeout(tr.__lookupTimer); tr.__lookupSeq = (tr.__lookupSeq || 0) + 1; tr.__inflight = null; if (tr.__abort) tr.__abort.abort(); facts.delete(tr); tr.remove(); if (next) $('.c-code', next).focus(); else $('#comp-add').focus(); }
      if (act === 'up' && tr.previousElementSibling) { tr.parentNode.insertBefore(tr, tr.previousElementSibling); b.focus(); }
      if (act === 'down' && tr.nextElementSibling) { tr.parentNode.insertBefore(tr.nextElementSibling, tr); b.focus(); }
      renumber();
    });
    rowsEl.addEventListener('change', function (e) { if (e.target.classList.contains('c-code')) { var tr = e.target.closest('tr'); relabel(tr, $$('#comp-rows tr.comp-row').indexOf(tr)); lookup(tr); } });
    rowsEl.addEventListener('input', function (e) {
      if (e.target.classList.contains('c-qty')) tiles();
      if (e.target.classList.contains('c-code')) {
        var tr = e.target.closest('tr');
        // 打った瞬間に古い答えを捨てて世代を進める (A → B → A と打ち直しても、前の A の答え・遅れて来る返事は使わない = #1628 Codex R3 L3)
        facts.delete(tr); tr.__lookupSeq = (tr.__lookupSeq || 0) + 1; tr.__inflight = null; tr.__retryN = 0; if (tr.__abort) tr.__abort.abort();
        ['.c-tax', '.c-sales', '.c-cost', '.c-handling'].forEach(function (c) { $(c, tr).textContent = ''; }); $('.c-name', tr).textContent = e.target.value.trim() ? '確かめています…' : ''; $('.c-name', tr).classList.remove('bad');
        tiles();
        clearTimeout(tr.__lookupTimer);
        tr.__lookupTimer = setTimeout(function () { lookup(tr); }, 500);
      }
    });
    // 構成品のコードで Enter = 引き当てて数の欄へ (保存はしない)
    rowsEl.addEventListener('keydown', function (e) {
      if (e.key !== 'Enter' || e.isComposing || e.keyCode === 229) return;
      if (e.target.classList.contains('c-code')) { e.preventDefault(); var q = $('.c-qty', e.target.closest('tr')); if (q) { q.focus(); q.select(); } }
    });
    $('#comp-add').addEventListener('click', function () {
      if ($$('#comp-rows tr.comp-row').length >= (P.maxComponents || 20)) { ME.toast('構成品は ' + (P.maxComponents || 20) + ' 品までです'); return; }
      rowsEl.appendChild($('#comp-tpl').content.firstElementChild.cloneNode(true));
      renumber();
      $('.c-code', rowsEl.lastElementChild).focus();
    });
  }
  ['f-standard_price', 'xcost-jpy', 'f-set_sales_class_override'].forEach(function (id) { var e = document.getElementById(id); if (e) { e.addEventListener('input', tiles); e.addEventListener('change', tiles); } });

  /* ---------- 出品カード: 作らない・参考 URL・Amazon URL から ASIN・セット商品を作るか ---------- */
  /** カードの欄に値が残っているか (カードを作らないときも送る = 形が違えば保存で断られる) */
  function filled(boxSel) {
    var box = $(boxSel); if (!box) return false;
    return $$('input, select, textarea', box).some(function (el) { return el.type !== 'checkbox' && el.type !== 'radio' && el.type !== 'file' && el.value.trim() !== '' && !el.closest('#row-set-plans'); })
      || $$('.seg', box).some(function (sg) { return !!sg.getAttribute('data-value'); });
  }
  /** カードの欄・Yahoo! の欄に値が残っているか (DOM から全部見る = 漏れが無い・#1628 Codex R4 M2)。カードを作らないときも送る = 形が違えば保存で断られる */
  function cardHasValues() { return filled('#card-fields') || yahooHasValues(); }
  function yahooHasValues() { return filled('#sec-yahoo'); }
  function cardShow() {
    var on = cardOn();
    var keepCard = !on && filled('#card-fields'), keepYahoo = !on && yahooHasValues();
    var keep = keepCard || keepYahoo;
    var cf = $('#card-fields'); if (cf) { cf.hidden = !on && !keepCard; }
    var note = $('#card-off-note');
    if (note) {
      note.hidden = on;
      var where = [keepCard ? '下の欄' : '', keepYahoo ? '「Yahoo! を楽天と変えるときだけ」の欄' : ''].filter(Boolean).join('と');
      note.textContent = keep ? 'カードは作りません。ただし' + where + 'に入れた値は保存のときに確かめます (形が違うと保存できません)。要らなければ空にしてください。' : 'カードは作りません (保存しても product-hub のボードにカードはできません)。';
      note.classList.toggle('wrn', keep);
    }
    var plan = segVal('set-plan');
    // 作らない理由・メモは「作らない / 保留」なら出す (カードを作らないときも、値は送って確かめる = 直せるように・#1628 Codex R3 M2)
    var rr = $('#row-set-reason'); if (rr) rr.hidden = !(plan === 'none' || plan === 'hold');
    var sr = $('#set-reason'); if (sr) sr.disabled = !canSave || plan !== 'none';
    // 🆕 作るセット: 「作る」かつカードを作るときだけ出す (行を足すのは「作る」を押したとき = 全部外したら 0 行のまま ③ で止める)
    var sp = $('#row-set-plans');
    if (sp) sp.hidden = !(plan === 'create' && on);
  }
  /* ---------- 🆕 作るセット (「作る」のときの個数・セット商品コード・セット商品名・lib/set-plan.mjs と同じ決まり) ---------- */
  var spRowEl = $('#row-set-plans');
  var SP_MAX = spRowEl ? Number(spRowEl.getAttribute('data-max')) || 10 : 10;
  var SP_MIN = spRowEl ? Number(spRowEl.getAttribute('data-qty-min')) || 2 : 2;
  var SP_QMAX = spRowEl ? Number(spRowEl.getAttribute('data-qty-max')) || 99 : 99;
  var SP_NMAX = spRowEl ? Number(spRowEl.getAttribute('data-name-max')) || 255 : 255;
  /** 初めのコード・名前 (lib/set-plan.mjs の defaultSetCode / defaultSetName と同じ = 名前は半角の空白 1 つ・2026-10-17 中原さん) */
  function spDefCode(q) { return $('#code').value.trim() + '-' + q; }
  function spDefName(q) { return (val('f-name') + ' ' + q + '個セット').trim(); }
  function spQty(v) { var t = half(v).trim(); if (!/^\d{1,2}$/.test(t)) return null; var n = Number(t); return n >= SP_MIN && n <= SP_QMAX ? n : null; }
  /** 行 → { tr, qty (数か null), qtyRaw, code, name } (入れた順) */
  function setPlanRows() {
    return $$('#set-plan-rows tr.sp-row').map(function (tr) { var raw = $('.sp-qty', tr).value; return { tr: tr, qty: spQty(raw), qtyRaw: raw.trim(), code: $('.sp-code', tr).value, name: $('.sp-name', tr).value.trim() }; });
  }
  var spChecks = new Map();   // 行 → サーバーの確かめ { key (セットのコード + 単品のコード), ok (true / false / null = 確かめられない), message }
  /** 確かめの答えの鍵 = セットのコードと単品のコードの組 (単品のコードで答えが変わる = 単品と同じコード・Codex R3 Medium 4) */
  function spKey(code) { return code + '|' + $('#code').value.trim(); }
  /** 行ごとの形の一言 (問題が無ければ '')。重なり = 上の行と同じ個数・同じコード (大文字小文字は同じと見る・後の行に印 = サーバーと同じ)・単品のコードと同じ */
  function spProblem(r, rows) {
    if (r.qty == null) return '個数は ' + SP_MIN + '〜' + SP_QMAX + ' の整数';
    if (rows.slice(0, rows.indexOf(r)).some(function (x) { return x.qty === r.qty; })) return r.qty + '個セットが 2 行あります';
    var shape = codeShape(r.code);
    if (shape) return shape.replace(/^商品コード/, 'セット商品コード');
    if (r.code.toLowerCase() === $('#code').value.trim().toLowerCase()) return 'セット商品コードが単品のコードと同じです';
    if (rows.slice(0, rows.indexOf(r)).some(function (x) { return x.code.toLowerCase() === r.code.toLowerCase(); })) return 'セット商品コードが 2 行あります';
    if (!r.name) return 'セット商品名を入れてください';
    if (r.name.length > SP_NMAX) return 'セット商品名は ' + SP_NMAX + ' 字まで';
    var c = spChecks.get(r.tr);
    if (c && c.key === spKey(r.code) && c.ok === false) return c.message || '使えないコードです';
    return '';
  }
  /** 数える答え { rows, bad, firstBad (直す欄のセレクタ) } と、行ごとの一言を出す */
  function setPlanCheck() {
    var rows = setPlanRows(); var bad = 0; var firstBad = null;
    rows.forEach(function (r, i) {
      var prob = spProblem(r, rows);
      var m = $('.sp-msg', r.tr);
      var c = spChecks.get(r.tr);
      var t = prob || (c && c.key === spKey(r.code) ? (c.ok ? '使えます' : c.ok === null ? (c.message || 'いまは確かめられません (保存のときにもう一度)') : '') : r.code ? '確かめています…' : '');
      var cls = prob ? 'err' : c && c.key === spKey(r.code) && c.ok ? 'ok' : c && c.key === spKey(r.code) && c.ok === null ? 'warn' : 'info';
      if (m && (m.textContent !== t || m.className !== 'fmsg sp-msg ' + cls)) { m.textContent = t; m.className = 'fmsg sp-msg ' + cls; }
      if (prob) { bad++; if (!firstBad) firstBad = '#set-plan-rows tr.sp-row:nth-child(' + (i + 1) + ') ' + (/個数|個セット/.test(prob) ? '.sp-qty' : /名/.test(prob) ? '.sp-name' : '.sp-code'); }
    });
    return { rows: rows.length, bad: bad, firstBad: firstBad };
  }
  var spSeq = 0;
  /** サーバーで確かめる (打った後・少し待って)。答えはそのコードのときだけ使う */
  function spCheckLater(tr) {
    clearTimeout(tr.__spTimer);
    var code = $('.sp-code', tr).value;
    if (codeShape(code)) { spChecks.delete(tr); return; }
    var key = spKey(code);
    tr.__spTimer = setTimeout(function () {
      var my = ++spSeq; tr.__spSeq = my;
      fetch(BASE + '/api/set-code-check?code=' + encodeURIComponent(code) + '&parent=' + encodeURIComponent($('#code').value.trim()) + '&request_id=' + encodeURIComponent(requestId), { headers: { Accept: 'application/json' } })
        .then(function (r) { return r.json().catch(function () { return {}; }).then(function (j) { return { r: r, j: j }; }); })
        .then(function (x) {
          if (tr.__spSeq !== my || !tr.isConnected || spKey($('.sp-code', tr).value) !== key) return;   // 打ち直した (単品のコードも) = 古い答えは使わない
          if (x.r.ok && x.j.ok) spChecks.set(tr, { key: key, ok: true });
          else if (x.r.status === 200 || x.r.status === 400) spChecks.set(tr, { key: key, ok: false, message: x.j.message || x.j.error || '使えません' });
          else spChecks.set(tr, { key: key, ok: null, message: 'いまは確かめられません (保存のときにもう一度確かめます)' });
          update();
        })
        .catch(function () { if (tr.__spSeq === my) { spChecks.set(tr, { key: key, ok: null, message: '確かめられません (通信)。保存のときにもう一度確かめます' }); update(); } });
    }, 450);
  }
  /** 行の初めの値 (直していない欄 = data-auto) を単品のコード・名前・個数に合わせる */
  function spFollow(tr) {
    var q = spQty($('.sp-qty', tr).value);
    if (q == null) return;
    var c = $('.sp-code', tr), n = $('.sp-name', tr);
    if (c.getAttribute('data-auto') === '1' && c.value !== spDefCode(q)) { c.value = spDefCode(q); spCheckLater(tr); }
    if (n.getAttribute('data-auto') === '1') n.value = spDefName(q);
  }
  function spRelabel() {
    $$('#set-plan-rows tr.sp-row').forEach(function (tr, i) {
      var who = (i + 1) + ' 行目の';
      $('.sp-qty', tr).setAttribute('aria-label', who + '個数'); $('.sp-code', tr).setAttribute('aria-label', who + 'セット商品コード');
      $('.sp-name', tr).setAttribute('aria-label', who + 'セット商品名'); $('[data-sp-del]', tr).setAttribute('aria-label', (i + 1) + ' 行目のセットを外す');
    });
  }
  /** 行を足す (個数 = 次の空いている数)。初めのコード・名前は直すまで単品に付いていく */
  function addSetPlanRow(qty) {
    var tpl = $('#set-plan-tpl'); if (!tpl) return null;
    var used = setPlanRows().map(function (r) { return r.qty; });
    // 個数 = 決まりの中 (2〜99) で使っていない数 (望みの数から上へ・無ければ下から = Codex R2 Low)
    var free = function (n) { return n >= SP_MIN && n <= SP_QMAX && used.indexOf(n) < 0; };
    var q = qty; while (!free(q) && q < SP_QMAX) q++;
    if (!free(q)) { q = SP_MIN; while (!free(q) && q < SP_QMAX) q++; }
    var tr = tpl.content.firstElementChild.cloneNode(true);
    $('.sp-qty', tr).value = String(q);
    $('.sp-code', tr).setAttribute('data-auto', '1'); $('.sp-name', tr).setAttribute('data-auto', '1');
    $('#set-plan-rows').appendChild(tr);
    spFollow(tr); spRelabel();
    if (!$('.sp-code', tr).value.trim() || codeShape($('.sp-code', tr).value)) spChecks.delete(tr);
    return tr;
  }
  var spRowsEl = $('#set-plan-rows');
  if (spRowsEl && canSave) {
    spRowsEl.addEventListener('input', function (e) {
      var tr = e.target.closest('tr.sp-row'); if (!tr) return;
      if (e.target.classList.contains('sp-qty')) spFollow(tr);
      if (e.target.classList.contains('sp-code')) { e.target.setAttribute('data-auto', e.target.value === '' ? '1' : '0'); spChecks.delete(tr); spCheckLater(tr); }
      if (e.target.classList.contains('sp-name')) e.target.setAttribute('data-auto', e.target.value === '' ? '1' : '0');
    });
    spRowsEl.addEventListener('click', function (e) {
      var b = e.target.closest('[data-sp-del]'); if (!b || b.disabled) return;
      var tr = b.closest('tr.sp-row'); var next = tr.nextElementSibling || tr.previousElementSibling;
      clearTimeout(tr.__spTimer); spChecks.delete(tr); tr.remove(); spRelabel();
      if (next) $('.sp-qty', next).focus(); else { var add = $('#set-plan-add'); if (add) add.focus(); }
      update();
    });
  }
  var spAdd = $('#set-plan-add');
  if (spAdd && canSave) spAdd.addEventListener('click', function () {
    if ($$('#set-plan-rows tr.sp-row').length >= SP_MAX) { ME.toast('作るセットは ' + SP_MAX + ' 個までです'); return; }
    var used = setPlanRows().map(function (r) { return r.qty || 0; });
    var tr = addSetPlanRow(Math.max(SP_MIN, (used.length ? Math.max.apply(null, used) : SP_MIN - 1) + 1));
    if (tr) $('.sp-qty', tr).focus();
    update();
  });
  // 単品のコード・名前を打ったら、直していない行の初めの値も付いていく
  // 単品のコードを打ち直したら、直したコードの行も確かめ直す (答えは単品のコードとの組)
  ['code', 'f-name'].forEach(function (id) {
    var e = document.getElementById(id);
    if (e) e.addEventListener('input', function () {
      $$('#set-plan-rows tr.sp-row').forEach(function (tr) { var before = $('.sp-code', tr).value; spFollow(tr); if (id === 'code' && $('.sp-code', tr).value === before) spCheckLater(tr); });
    });
  });

  ['card-create', 'set-plan'].forEach(function (id) {
    var e = document.getElementById(id);
    if (e) e.addEventListener('change', function () {
      // 「作る」にした (かカードを作るに戻した) とき、作るセットが 1 行も無ければ 2個セットを 1 行 (初めの値)
      if (segVal('set-plan') === 'create' && cardOn() && canSave && !saved && !$$('#set-plan-rows tr.sp-row').length) addSetPlanRow(SP_MIN);
      cardShow(); update();
    });
  });
  // カードを作らないときに欄を空にしていったら、全部空になった時点で畳む (打っている途中では畳まない = 欄を離れたとき)
  var cardFieldsEl = $('#card-fields'); if (cardFieldsEl) cardFieldsEl.addEventListener('focusout', function () { setTimeout(cardShow, 0); });
  var yahooEl = $('#sec-yahoo'); if (yahooEl) { yahooEl.addEventListener('focusout', function () { setTimeout(cardShow, 0); }); yahooEl.addEventListener('change', cardShow); }
  var refRows = $('#ref-rows');
  function refRenumber() { $$('#ref-rows .urlrow').forEach(function (r, i) { $('.idx', r).textContent = String(i + 1); $('.ref-url', r).setAttribute('aria-label', '参考 URL ' + (i + 1)); $('[data-ref-del]', r).setAttribute('aria-label', '参考 URL ' + (i + 1) + ' を外す'); }); }
  var refAdd = $('#ref-add');
  if (refAdd && canSave) refAdd.addEventListener('click', function () {
    if ($$('#ref-rows .urlrow').length >= (P.maxRefs || 20)) { ME.toast('参考 URL は ' + (P.maxRefs || 20) + ' 個までです'); return; }
    var row = $('#ref-rows .urlrow').cloneNode(true); $('.ref-url', row).value = '';
    refRows.appendChild(row); refRenumber(); $('.ref-url', row).focus(); update();
  });
  if (refRows) refRows.addEventListener('click', function (e) {
    var b = e.target.closest('[data-ref-del]'); if (!b || b.disabled) return;
    var rows = $$('#ref-rows .urlrow'), row = b.closest('.urlrow');
    if (rows.length === 1) $('.ref-url', row).value = ''; else row.remove();
    refRenumber(); var first = $('#ref-rows .ref-url'); if (first) first.focus(); update();
  });
  var amz = $('#amazon-url');
  if (amz) amz.addEventListener('input', function () {
    var m = /(?:\/dp\/|\/gp\/product\/|\/product\/)([A-Z0-9]{10})(?:[/?#]|$)/i.exec(amz.value);
    var asin = $('#asin');
    if (m && asin && !asin.value.trim()) { asin.value = m[1].toUpperCase(); $('#asin-msg').hidden = false; update(); }
  });
  var asinEl = $('#asin'); if (asinEl) asinEl.addEventListener('input', function () { $('#asin-msg').hidden = true; });
  function yahooSummary() {
    var s = $('#yahoo-sum'); if (!s) return;
    var parts = [];
    if (val('y-price')) parts.push('Yahoo!売価 ' + yen(half(val('y-price')).replace(/,/g, '')) + ' 円');
    if (val('y-price-sagawa')) parts.push('佐川 ' + yen(half(val('y-price-sagawa')).replace(/,/g, '')) + ' 円');
    if (val('y-delivery')) parts.push('配送 ' + val('y-delivery'));
    if (val('y-category')) parts.push('カテゴリ ' + val('y-category'));
    if (val('y-path')) parts.push('path あり');
    s.textContent = parts.length ? parts.join(' · ') + ' を入れてあります' : 'ふだんは全部「入力なし」';
    s.className = 'b ' + (parts.length ? 'info' : 'mute');
  }

  /* ---------- 集める (送り方は前と同じ) ---------- */
  function uuid() {
    if (window.crypto && crypto.randomUUID) return crypto.randomUUID();
    var b = new Uint8Array(16); crypto.getRandomValues(b); b[6] = (b[6] & 15) | 64; b[8] = (b[8] & 63) | 128;
    var h = Array.prototype.map.call(b, function (x) { return x.toString(16).padStart(2, '0'); }).join('');
    return h.slice(0, 8) + '-' + h.slice(8, 12) + '-' + h.slice(12, 16) + '-' + h.slice(16, 20) + '-' + h.slice(20);
  }
  // 登録 1 回に 1 つ (通信が切れて押し直しても 2 回入らない)。返事が来た後にもう一度押すときは新しい番号
  var requestId = uuid();
  function fieldValue(name) {
    var el = $('[data-field="' + name + '"]', form);
    if (!el) return undefined;
    return el.classList.contains('seg') ? el.getAttribute('data-value') || '' : el.value;
  }
  function collect() {
    var values = {};
    var keys = isSet ? ['name', 'standard_price', 'shipping_code', 'reorder_months', 'handling_own', 'set_sales_class_override']
      : ['name', 'standard_price', 'shipping_code', 'tax_rate', 'sales_class', 'primary_supplier', 'reorder_months', 'expiry_managed', 'inbound_date_managed', 'maker'];   // maker = 🆕 0084 (隠した欄 = 名簿の番号・欄が無い = 送らない)
    keys.forEach(function (k) { var v = fieldValue(k); if (v !== undefined && v !== '') values[k] = v; });
    if (isSet) {
      values.components = components();
      // 原価の理由は送らない (新規登録では要らない・サーバーが「新商品の登録」を入れる = 2026-10-08 中原さん)
      if (val('xcost-jpy')) values.exception_cost = { jpy: val('xcost-jpy') };
    } else if (val('cost-jpy')) values.cost = { jpy: val('cost-jpy') };
    var card = {
      create: cardOn(),
      official_url: val('official-url'), amazon_url: val('amazon-url'), asin: val('asin'),
      reference_urls: refUrls(),
      yahoo: { price: val('y-price'), price_sagawa: val('y-price-sagawa'), delivery_label: val('y-delivery'), category_id: val('y-category'), path: val('y-path') },
    };
    if (!isSet) {
      var plan = segVal('set-plan');
      if (plan) card.set_decision = { decision: plan, reason_code: plan === 'none' ? val('set-reason') : '', reason_text: val('set-text') };
      // 🆕 作るセット (作る かつ カードを作るときだけ・1 つ以上 = 新しい画面は必ず送る)。個数は数・コードは打ったとおり・名前は前後の空白を落とす
      if (plan === 'create' && cardOn()) card.sets = setPlanRows().map(function (r) { return { qty: r.qty == null ? r.qtyRaw : r.qty, code: r.code, name: r.name }; });
    }
    // 🆕 2026-10-13: 入力なしにした項目 (サーバーが「値か入力なし」をもう一度確かめる・登録の記録 (理由) に残す)
    var out = { request_id: requestId, kind: P.kind, code: $('#code').value, reason: val('reason'), values: values, card: card, blanks: blanks() };
    // 発注の設定 (発注アプリ・単品だけ・public/me-order.js)。何も入れていない = 送らない (発注アプリに何も書かない)
    var po = !isSet && ME.orderSettings ? ME.orderSettings.collect() : null;
    if (po) out.order_settings = po;
    // 🆕 0080 (CompanyDB構想 23 v4): ロジザードに送るバーコード (単品だけ・欄が無い = 0080 の前 = 送らない)
    if (!isSet && $('#f-lzbc-in')) out.logizard_barcode = lzbc().body;
    // 🆕 0087: 梱包の大きさ・重さ (単品だけ・入れなくてよい・欄が無い / 全部空 = 送らない)
    if (!isSet) { var pk = pkgBody(); if (pk) out.package = pk; }
    return out;
  }

  /** 🆕 0087: 梱包の 縦・横・高さ (cm)・重さ (g) の欄 (views/_package-new.ejs) → { length, width, height, weight } (全部空・欄が無い = null = 送らない。形はサーバーが確かめる) */
  function pkgBody() {
    var ids = { length: 'np-length', width: 'np-width', height: 'np-height', weight: 'np-weight' }, o = {}, any = false;
    Object.keys(ids).forEach(function (k) { var e = document.getElementById(ids[k]); var v = e && !e.disabled ? String(e.value || '').trim() : ''; o[k] = v; if (v) any = true; });
    return any ? o : null;
  }
  /* ---------- 登録 ---------- */
  function msg(t, cls) { var m = $('#msg'); m.className = 'msgline ' + (cls || ''); m.textContent = t || ''; }
  /**
   * サーバーの誤りの項目 → 直す欄 (1 つの行に欄が 2 つある Yahoo! など。#1628 Codex R5 L1)。
   * ここに無い項目は data-row の行の最初の欄
   */
  var FIELD_INPUT = { logizard_barcode: '#f-lzbc-in', maker: '#f-maker-in', 'card.yahoo': '#y-price', 'card.yahoo.price': '#y-price', 'card.yahoo.price_sagawa': '#y-price-sagawa', 'card.yahoo.delivery_label': '#y-delivery', 'card.yahoo.category_id': '#y-category', 'card.yahoo.path': '#y-path', reason: '#reason' };
  function showError(j, status) {
    var h = '<div class="result err" role="alert"><div class="rt">' + esc(j.error || ('HTTP ' + status)) + '</div>';
    if (j.reason === 'set_underivable' && Array.isArray(j.blockers)) h += '<ul>' + j.blockers.map(function (b) { return '<li>' + esc(b) + '</li>'; }).join('') + '</ul>';
    $('#result').innerHTML = h + '</div>';
    if (j.field) {
      if (/^card\./.test(String(j.field))) { var cf = $('#card-fields'); if (cf) cf.hidden = false; }   // 隠れた欄の誤りでも直せるように開く
      var row = $('[data-row="' + String(j.field).replace(/"/g, '') + '"]', scope);
      var target = Object.prototype.hasOwnProperty.call(FIELD_INPUT, j.field) ? $(FIELD_INPUT[j.field]) : null;
      if (target) row = target.closest('.f') || row;
      // セット判断の誤りで「作らない / 保留」を選んでいる = 直すのは理由・メモの欄
      var plan0 = segVal('set-plan');
      if (j.field === 'card.set_decision' && (plan0 === 'none' || plan0 === 'hold') && $('#row-set-reason')) { row = $('#row-set-reason'); row.hidden = false; }
      // 作るセットの誤り = その行のコードの欄 (何行目か = j.index)
      if (j.field === 'card.sets' && $('#row-set-plans')) {
        row = $('#row-set-plans'); row.hidden = false;
        var spTr = typeof j.index === 'number' ? $$('#set-plan-rows tr.sp-row')[j.index] : null;
        target = spTr ? $(/個数|個セット/.test(String(j.error || '')) ? '.sp-qty' : /名/.test(String(j.error || '')) && !/コード/.test(String(j.error || '')) ? '.sp-name' : '.sp-code', spTr) : $('#set-plan-add');
        if (spTr && /^(code_|ph_|set_code)/.test(String(j.reason || ''))) { spChecks.set(spTr, { key: spKey($('.sp-code', spTr).value), ok: false, message: j.error }); }
      }
      if (!row && target) { try { target.focus(); } catch (e) { /* */ } }
      if (row) {
        row.classList.add('err');
        var det = row.closest('details'); if (det) det.open = true;
        row.scrollIntoView({ behavior: 'smooth', block: 'center' });
        var c = target || $('input:not([disabled]), select:not([disabled]), button:not([disabled])', row); if (c) try { c.focus({ preventScroll: true }); } catch (e) { c.focus(); }
      }
    }
  }
  /** 保存が通った後は入力の場所を閉じる (できた商品の画面へ移るまでの間に打った値が黙って消えないように) */
  function lockAfterSave() {
    var main = form.firstElementChild;
    main.setAttribute('inert', '');
    $$('input, select, textarea, button', main).forEach(function (x) { x.disabled = true; });
    var r = $('#reason'); if (r) r.disabled = true;
  }
  var NOTE_KEY = 'master-edit:saved-note';   // 商品の画面 (me-sku.js) が上に 1 回だけ出す知らせ (同じ形)
  function doSave() {
    if (!saveBtn || saveBtn.disabled || busy) return;
    var zero = ['y-price', 'y-price-sagawa'].filter(function (id) { return /^[0０]+$/.test(val(id)); });
    if (zero.length) { msg('Yahoo!売価は 1 円以上で入れてください (決めていなければ空のまま)', 'err'); focusTo('#' + zero[0]); return; }
    var body = collect();
    busy = true; update(); msg('登録しています…'); $('#result').innerHTML = '';
    $$('.f.err', scope).forEach(function (f) { f.classList.remove('err'); });
    fetch(BASE + '/api/new', { method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json' }, body: JSON.stringify(body) })
      .then(function (r) { return r.json().catch(function () { return {}; }).then(function (j) { return { r: r, j: j }; }); })
      .then(function (x) {
        busy = false;
        if (x.r.ok && x.j.ok) {
          var code = x.j.code || body.code;
          saved = true; lockAfterSave(); update();
          msg('登録しました (下書き)。' + code + ' の画面を開いています…', 'ok');
          var warns = (x.j.warnings || []).slice();
          // 🆕 #1719 R2 High: ロジザードの写しと照らせていない値 = 手順の一文 (GAS の H 列に入れる前にロジザードの商品マスタで確かめる)。商品の画面にも同じ印が出る
          if (x.j.lz_unchecked) warns.push('ロジザードのバーコード: ' + (x.j.lz_unchecked.note || 'ロジザードの写しと照らせていません (' + x.j.lz_unchecked.words + ')'));
          if (x.j.card && x.j.card.status !== 'done') warns.push('product-hub の出品カード: ' + (x.j.card_label || x.j.card.status) + ' (商品の画面の「カードをもう一度作る」か、product-hub のボードを開くと作り直します)');
          // 🆕 作るセット: 下書きを作った = NE でやること (セット商品を NE に登録する)・作れなかった行 = 気をつけること
          var nes = (x.j.ne_steps || []).slice();
          var sent = body.card && body.card.sets ? body.card.sets : [];
          if (sent.length) {
            var made = x.j.card && Array.isArray(x.j.card.set_drafts) ? x.j.card.set_drafts : null;
            var skipped = x.j.card && Array.isArray(x.j.card.set_skipped) ? x.j.card.set_skipped : [];
            nes.push('セット商品 ' + sent.map(function (s) { return s.code + ' (' + s.qty + '個)'; }).join('・') + ' も NE に登録してください。product-hub の「セットの下書き」は NE に取り込まれると本コード確定になり、それまで出品しません'
              + (made ? '' : ' (下書きはカードができたときに作ります)'));
            skipped.forEach(function (k) { warns.push('セットの下書き ' + k.code + ' は作っていません: ' + (k.why || '')); });
          }
          // 登録は成功・発注アプリへの書き込み (②) だけまだ = 🆕 0075: 知らせは Company DB に残る (自動でもう一度届ける) / 要確認 = 商品の画面で確かめる。
          //   0075 の前の DB = 商品の画面の「発注の設定」で入れ直す (知らせが無い)
          var po = x.j.order_settings;
          if (po && !po.ok) {
            if (po.pending) warns.push('発注の設定はまだ発注アプリに届いていません (登録と一緒に残してあるので、自動でもう一度届けます。急ぐときは「発注の設定の届け待ち」の画面で「もう一度届ける」)');
            else if (po.status) warns.push('発注の設定は要確認です (' + (po.error || '発注アプリにもう設定がある') + ')。商品の画面の「発注の設定」で確かめて、「発注の設定の届け待ち」の画面で閉じてください');
            else warns.push('発注の設定だけ保存できませんでした (' + (po.error || '発注アプリに書けない') + ')。商品の画面の「発注の設定」で入れてください');
          }
          try { sessionStorage.setItem(NOTE_KEY, JSON.stringify({ code: String(code), at: Date.now(), j: { replayed: !!x.j.replayed, changed: [], derived: [], ne_steps: nes, warnings: warns } })); } catch (e) { /* 置けない = 知らせを出さないだけ */ }
          var url = BASE + '/sku/' + encodeURIComponent(code);
          if (ME.replacePage) ME.replacePage(url); else location.replace(url);
          return;
        }
        msg('', 'err');
        showError(x.j, x.r.status);
        // 返事が来た = この番号の登録は終わった。直してもう一度押すときは新しい番号。
        //   🆕 成否が分からない (keep_request_id・作るセットの予約を持ったまま) = 同じ番号のまま押し直す (Codex R6 Medium 1)
        if (!x.j.keep_request_id) requestId = uuid();
        update();
      })
      .catch(function () { busy = false; update(); msg('通信できませんでした。もう一度「下書きを保存する」を押してください (同じ登録は 2 回入りません)', 'err'); });
  }
  if (saveBtn && canSave) saveBtn.addEventListener('click', doSave);
  form.addEventListener('submit', function (e) { e.preventDefault(); });

  /* ---------- 保存の箱へ (未保存の札・離れるときの「変更内容を確認する」・狭い画面の帯) ---------- */
  var box = $('#savebox'), boxToggle = $('#savebox-toggle');
  function review() {
    if (!box) return;
    box.classList.add('open'); if (boxToggle) boxToggle.setAttribute('aria-expanded', 'true');
    box.scrollIntoView({ behavior: 'smooth', block: 'center' });
    var target = saveBtn && !saveBtn.disabled ? saveBtn : box;
    target.focus({ preventScroll: true });
  }
  ME.review = review;
  if (boxToggle) boxToggle.addEventListener('click', function () { var on = !box.classList.contains('open'); box.classList.toggle('open', on); boxToggle.setAttribute('aria-expanded', on ? 'true' : 'false'); });
  ME.onSave = canSave ? function () {
    if (saveBtn && !saveBtn.disabled) { doSave(); return; }
    var need = lastState.need || [];
    if (need.length) { ME.toast('下書き保存まで あと ' + need.length + ' つ: ' + need[0].t); focusTo(need[0].focus); return; }
    ME.toast(saved ? '登録しました。商品の画面を開いています' : '登録しています…');
  } : null;

  cardShow();
  tiles();
  update();
})();
