/*
 * me-profit.js — 1 つの商品の画面の「利益 (1 個あたり)」を、欄を変えたその場で計算し直す (10/6 中原さん「利益の計算も入れてほしい」)
 *   - 計算はサーバーと同じファイル (lib/profit-estimate.js の masterProfit = price-update の estimateGross)。router が /public/profit-estimate.js で配る
 *   - 使う欄: 標準売価 (#f-standard_price)・原価を変える (#cost-jpy・単品)・例外原価 (#xcost-jpy / #xcost-clear・セット)・税率 (単品の切り替え)・送料 (#f-shipping_code)
 *     直せない欄 (🔒) = 保存している値。原価を入れていない = 今日の原価のまま
 *   - 保存の送り方・未保存の数には関わらない (読むだけ。data-dirty-field も付けない)
 */
(function () {
'use strict';
const $ = (s) => document.querySelector(s);
const dataEl = document.getElementById('me-page');
const box = document.getElementById('profit-box');
if (!dataEl || !box) return;
// 計算のファイルはこの JS と同じ所 (/public/) から同じ版 (?v=) で読む (配り直した日に古い式を使わない)。動的な import = ふつうの script のまま (試験の構文の確かめも通る)
const me = document.querySelector('script[src*="/me-profit.js"]');
const libUrl = new URL('profit-estimate.js' + (me ? new URL(me.src, location.href).search : ''), me ? me.src : location.href).href;
import(libUrl).then(start).catch((e) => { console.error('[me-profit] 利益の計算を読めない', e); });

function start(PF) {
  const P = JSON.parse(dataEl.textContent);
  const S = P.profit || {};
  const isSet = P.kind === 'set';
  /**
   * 欄の円 = サーバー (lib/master-write.mjs の intIn)・me-sku.js の intOf と同じ読み: 全角の数字・．，－ を半角に → カンマと空白を除く → 整数だけ。
   * 空 = null / 整数でない・マイナス = bad (保存もサーバーが断る)
   */
  const yenIn = (raw) => {
    const t = String(raw == null ? '' : raw).replace(/[０-９．，－]/g, (c) => String.fromCharCode(c.charCodeAt(0) - 0xFEE0)).replace(/[,\s]/g, '');
    if (t === '') return { v: null };
    return /^-?\d+$/.test(t) && Number(t) >= 0 ? { v: Number(t) } : { bad: true };
  };
  const same = (a, b) => (a == null && b == null) || (a != null && b != null && Number(a) === Number(b));

  /** 画面の値 (変えていない・直せない欄 = 保存している値) */
  function screenValues() {
    const bad = [];
    let price = S.price;
    const pEl = $('#f-standard_price');
    if (pEl && !pEl.disabled) { const r = yenIn(pEl.value); if (r.bad) bad.push('標準売価'); else price = r.v; }
    let cost = S.cost;
    if (isSet) {
      const clear = $('#xcost-clear'); const xj = $('#xcost-jpy');
      if (clear && clear.checked) cost = S.setCostSum;
      else if (xj && xj.value.trim() !== '') { const r = yenIn(xj.value); if (r.bad) bad.push('例外原価'); else cost = r.v; }
    } else {
      const cj = $('#cost-jpy');
      if (cj && cj.value.trim() !== '') { const r = yenIn(cj.value); if (r.bad) bad.push('原価'); else cost = r.v; }
    }
    let taxRate = S.taxRate;
    const seg = document.querySelector('.seg[data-field="tax_rate"]');
    if (seg) { const t = seg.getAttribute('data-value') || ''; taxRate = t === '' ? null : Number(t); }
    let shipping = S.shipping;
    const sh = $('#f-shipping_code');
    if (sh && !sh.disabled && sh.value !== (S.shipCode || '')) {
      // 送料コードを変えた = 送料の表の金額 (表に無い・空 = 未入力)
      shipping = sh.value && S.shipCosts && Object.prototype.hasOwnProperty.call(S.shipCosts, sh.value) ? S.shipCosts[sh.value] : null;
    }
    return { price, cost, taxRate, shipping, bad };
  }

  const words = (r) => (r.ok ? `${PF.fmtProfitYen(r.profit)} 円 (${PF.fmtProfitRate(r.rate)})` : '計算できません');
  const saved = PF.masterProfit({ price: S.price, cost: S.cost, taxRate: S.taxRate, shipping: S.shipping, feeRate: S.feeRate });

  function setBig(el, text, unit, neg) {
    el.textContent = text;
    if (unit) { const sm = document.createElement('small'); sm.textContent = unit; el.appendChild(sm); }
    el.classList.toggle('neg', !!neg);
  }
  function paint() {
    const sv = screenValues();
    const changed = sv.bad.length > 0 || !same(sv.price, S.price) || !same(sv.cost, S.cost) || !same(sv.taxRate, S.taxRate) || !same(sv.shipping, S.shipping);
    const r = sv.bad.length ? { ok: false, reason: `入れた${sv.bad.join('・')}が数ではないので計算できません`, notes: [] }
      : PF.masterProfit({ price: sv.price, cost: sv.cost, taxRate: sv.taxRate, shipping: sv.shipping, feeRate: S.feeRate });
    const neg = r.ok && r.profit < 0;
    setBig($('#pf-val'), r.ok ? PF.fmtProfitYen(r.profit) : '—', r.ok ? '円' : '', neg);
    setBig($('#pf-rate'), r.ok ? PF.fmtProfitRate(r.rate) : '—', '', neg);
    $('#pf-when').textContent = changed ? '画面の値で (未保存)' : 'いまの値で';
    const calc = $('#pf-calc'); calc.hidden = !r.ok; calc.textContent = r.ok ? PF.profitLine(r) : '';
    const why = $('#pf-why'); why.hidden = r.ok; why.firstElementChild.textContent = r.ok ? '' : r.reason;
    const notes = PF.profitNoteWords(r.notes); const nt = $('#pf-notes'); nt.hidden = !notes.length; nt.textContent = notes.join(' · ');
    const next = $('#pf-next');
    next.hidden = !changed;
    if (changed) {
      next.textContent = '';
      const t = document.createElement('span'); t.className = 'lbl'; t.textContent = '保存すると'; next.appendChild(t);
      const b = document.createElement('div'); b.className = 'pf-arrow';
      b.textContent = `利益 ${words(saved)} → ${words(r)}`;
      next.appendChild(b);
      next.classList.toggle('down', r.ok && saved.ok && r.profit < saved.profit);
      next.classList.toggle('up', r.ok && saved.ok && r.profit > saved.profit);
    }
    box.setAttribute('data-profit', r.ok ? String(r.profit) : '');
  }
  // 欄の入力・切り替えボタン (me-shell.js が data-value を変えて change を出す)・元に戻す・やめる = どれも document まで上がる。
  // その場で計算し直す + 後から値を変える処理 (同じ document の後ろの受け手) のためにもう 1 回
  let queued = false;
  const now = () => { paint(); if (queued) return; queued = true; setTimeout(() => { queued = false; paint(); }, 0); };
  ['input', 'change', 'click'].forEach((ev) => document.addEventListener(ev, now));
  paint();
  document.documentElement.setAttribute('data-me-profit', 'ready');
}
})();
