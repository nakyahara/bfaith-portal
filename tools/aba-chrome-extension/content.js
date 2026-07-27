// content script — Amazon.co.jp のページ種別を判定して描画する。
// データ取得は全て sw.js (service worker) 経由。ページのDOMは読むだけで、
// Amazonへの追加リクエストは一切発生させない (自動化検知リスクを作らない)。

const ASIN_RE = /^[A-Z0-9]{10}$/;

function sendMessage(msg) {
  return new Promise((resolve) => {
    try {
      chrome.runtime.sendMessage(msg, (res) => {
        if (chrome.runtime.lastError) resolve({ ok: false, error: chrome.runtime.lastError.message });
        else resolve(res || { ok: false, error: '応答なし' });
      });
    } catch (e) {
      resolve({ ok: false, error: e.message });
    }
  });
}

function el(tag, cls, text) {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text != null) e.textContent = text;
  return e;
}

function fmtNum(n) { return n == null ? '-' : Number(n).toLocaleString('ja-JP'); }
function fmtPct(v) { return v == null ? '-' : `${Math.round(v * 10000) / 100}%`; }
function fmtWeek(w) { return w ? w.slice(5).replace('-', '/') : '-'; }

// ============================================================
// 商品ページ: 注文ワード (ABA) パネル
// ============================================================
function detectProductAsin() {
  const m = location.pathname.match(/\/(?:dp|gp\/product)\/([A-Z0-9]{10})(?:[/?]|$)/i);
  if (m) return m[1].toUpperCase();
  const input = document.querySelector('#ASIN, input[name="ASIN"]');
  if (input && ASIN_RE.test(input.value)) return input.value;
  return null;
}

async function renderProductPanel(asin) {
  if (document.getElementById('bfaba-panel')) return;
  const panel = el('div', 'bfaba-panel');
  panel.id = 'bfaba-panel';

  const header = el('div', 'bfaba-header');
  const title = el('span', 'bfaba-title', `注文ワード (ABA) — ${asin}`);
  const toggle = el('button', 'bfaba-toggle', '−');
  header.appendChild(title);
  header.appendChild(toggle);
  panel.appendChild(header);

  const body = el('div', 'bfaba-body');
  body.appendChild(el('div', 'bfaba-note', '読み込み中...'));
  panel.appendChild(body);

  toggle.addEventListener('click', () => {
    const hidden = body.style.display === 'none';
    body.style.display = hidden ? '' : 'none';
    toggle.textContent = hidden ? '−' : '+';
  });

  // 差し込み位置: 商品メイン領域の直後 → 見つからなければ body 先頭
  const anchor = document.querySelector('#ppd') || document.querySelector('#dp-container');
  if (anchor && anchor.parentNode) anchor.parentNode.insertBefore(panel, anchor.nextSibling);
  else document.body.prepend(panel);

  const res = await sendMessage({ type: 'reverse', asin });
  body.textContent = '';
  if (!res.ok) {
    body.appendChild(el('div', 'bfaba-error', `取得エラー: ${res.error}`));
    return;
  }
  const { terms, latestWeek, note } = res.data;
  if (note) { body.appendChild(el('div', 'bfaba-note', note)); return; }
  if (!terms || terms.length === 0) {
    body.appendChild(el('div', 'bfaba-note',
      '注文ワードなし (保有データの全週で、このASINはどの検索語でもクリック上位3に入っていません)'));
    return;
  }

  body.appendChild(el('div', 'bfaba-note', `最新データ週: 〜${fmtWeek(latestWeek)} / ${terms.length}ワード (クリック時コピー)`));

  const table = el('table', 'bfaba-table');
  const thead = el('thead');
  const hr = el('tr');
  for (const h of ['キーワード', 'ABA順位', '週間変化', 'クリック共有', '転換共有', 'Top3クリック/転換', '出現週']) {
    hr.appendChild(el('th', null, h));
  }
  thead.appendChild(hr);
  table.appendChild(thead);

  const tbody = el('tbody');
  for (const t of terms) {
    const tr = el('tr', t.isCurrent ? null : 'bfaba-stale');
    const kwTd = el('td', 'bfaba-kw', t.searchTerm);
    kwTd.title = 'クリックでコピー';
    kwTd.addEventListener('click', () => {
      navigator.clipboard.writeText(t.searchTerm);
      kwTd.classList.add('bfaba-copied');
      setTimeout(() => kwTd.classList.remove('bfaba-copied'), 600);
    });
    tr.appendChild(kwTd);

    const rankTd = el('td', null, fmtNum(t.abaRank));
    if (!t.isCurrent) rankTd.textContent += ` (〜${fmtWeek(t.lastSeen)})`;
    tr.appendChild(rankTd);

    let changeText = '-', changeCls = '';
    if (t.weeklyChange != null) {
      // weeklyChange 正 = 順位改善 (数字が小さくなった)
      changeText = `${t.weeklyChange > 0 ? '▲' : t.weeklyChange < 0 ? '▼' : '±'}${fmtNum(Math.abs(t.weeklyChange))} (${t.weeklyChangePct > 0 ? '+' : ''}${t.weeklyChangePct}%)`;
      changeCls = t.weeklyChange > 0 ? 'bfaba-up' : t.weeklyChange < 0 ? 'bfaba-down' : '';
    }
    tr.appendChild(el('td', changeCls, changeText));

    tr.appendChild(el('td', null, fmtPct(t.clickShare)));
    tr.appendChild(el('td', null, fmtPct(t.conversionShare)));
    tr.appendChild(el('td', null, `${fmtPct(t.top3ClickShare)} / ${fmtPct(t.top3ConversionShare)}`));
    tr.appendChild(el('td', null, `${t.weeksSeen}週`));
    tbody.appendChild(tr);
  }
  table.appendChild(tbody);
  const wrap = el('div', 'bfaba-scroll');
  wrap.appendChild(table);
  body.appendChild(wrap);
}

// ============================================================
// 検索結果ページ: BSR / 梱包 / ブランド バッジ
// ============================================================
function collectSearchItems() {
  const items = [];
  for (const node of document.querySelectorAll('div.s-main-slot div[data-asin]')) {
    const asin = (node.dataset.asin || '').toUpperCase();
    if (!ASIN_RE.test(asin)) continue;
    if (node.dataset.bfabaDone) continue;
    if (!node.querySelector('h2')) continue; // 広告枠・区切り等を除外
    items.push({ node, asin });
  }
  return items;
}

function badgeFor(info) {
  const badge = el('div', 'bfaba-badge');
  if (!info || info.error) { badge.appendChild(el('span', 'bfaba-b-muted', 'ABA: 取得エラー')); return badge; }
  if (info.notFound) { badge.appendChild(el('span', 'bfaba-b-muted', 'カタログ情報なし')); return badge; }

  // BSR: 小カテゴリ (classification) → 大カテゴリ (display) の順
  const cls = (info.ranks || []).filter(r => r.type === 'classification').sort((a, b) => a.rank - b.rank);
  const disp = (info.ranks || []).filter(r => r.type === 'display').sort((a, b) => a.rank - b.rank);
  if (cls[0]) badge.appendChild(el('span', 'bfaba-b-rank', `#${fmtNum(cls[0].rank)} ${cls[0].title}`));
  if (disp[0]) badge.appendChild(el('span', 'bfaba-b-rank2', `#${fmtNum(disp[0].rank)} ${disp[0].title}`));
  if (!cls[0] && !disp[0]) badge.appendChild(el('span', 'bfaba-b-muted', 'BSRなし'));

  const phys = [];
  if (info.packageWeightG != null) phys.push(`${fmtNum(info.packageWeightG)}g`);
  if (info.packageDimsCm) {
    const d = info.packageDimsCm;
    phys.push(`${d.l ?? '?'}×${d.w ?? '?'}×${d.h ?? '?'}cm`);
  }
  if (phys.length) badge.appendChild(el('span', 'bfaba-b-phys', `梱包 ${phys.join('・')}`));
  if (info.brand) badge.appendChild(el('span', 'bfaba-b-brand', info.brand));

  const asinSpan = el('span', 'bfaba-b-asin', info.asin);
  asinSpan.title = 'クリックでコピー';
  asinSpan.addEventListener('click', (e) => {
    e.preventDefault(); e.stopPropagation();
    navigator.clipboard.writeText(info.asin);
    asinSpan.classList.add('bfaba-copied');
    setTimeout(() => asinSpan.classList.remove('bfaba-copied'), 600);
  });
  badge.appendChild(asinSpan);
  return badge;
}

let serpBusy = false;
async function annotateSearchResults() {
  if (serpBusy) return;
  const items = collectSearchItems();
  if (items.length === 0) return;
  serpBusy = true;
  try {
    for (const it of items) it.node.dataset.bfabaDone = '1';
    const asins = [...new Set(items.map(i => i.asin))];
    const res = await sendMessage({ type: 'catalog', asins });
    if (!res.ok) {
      console.warn('[bfaba] catalog取得失敗:', res.error);
      // 再試行できるようにマークを外す (トークン未設定等は次回ページでも静かに失敗)
      for (const it of items) delete it.node.dataset.bfabaDone;
      return;
    }
    for (const it of items) {
      const info = res.data.items[it.asin];
      const h2 = it.node.querySelector('h2');
      const container = h2 ? (h2.closest('div[data-cy="title-recipe"]') || h2.parentElement) : it.node;
      container.appendChild(badgeFor(info));
    }
  } finally {
    serpBusy = false;
  }
}

// ============================================================
// 起動
// ============================================================
function main() {
  const asin = detectProductAsin();
  if (asin) {
    renderProductPanel(asin);
    return;
  }
  if (location.pathname === '/s' || location.pathname.startsWith('/s?') || location.search.includes('k=')) {
    annotateSearchResults();
    // ページ内更新 (絞り込み・ページ送りの一部はSPA的に差し替わる) を拾う
    let timer = null;
    const observer = new MutationObserver(() => {
      clearTimeout(timer);
      timer = setTimeout(annotateSearchResults, 800);
    });
    const slot = document.querySelector('div.s-main-slot');
    if (slot) observer.observe(slot, { childList: true, subtree: false });
  }
}

main();
