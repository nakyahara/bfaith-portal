/* 検索KW案の判定 — 判断の送信と、押した結果の見た目の更新。
   ⚠️見送りの理由必須はサーバ側 (keyword-store.js) でも確かめている。ここは早く気づかせるためだけ。 */
const DECIDED = { adopt: 'いい', hold: '保留', reject: '見送り' };

// 理由チップ: input の状態を親ラベルに写す (:has を待たずに色を付けるため)
const paint = (input) => input.closest('.kw-chip')?.classList.toggle('is-on', input.checked);
document.querySelectorAll('.kw-chip input').forEach(paint);
document.addEventListener('change', (event) => {
  if (event.target.matches('.kw-chip input')) paint(event.target);
});

// 判定済みの印。サーバが出していない (=未判定だった) カードには作って差し込む
function markCard(card, decision) {
  card.classList.remove('is-adopt', 'is-hold', 'is-reject');
  card.classList.add('is-' + decision);
  const side = card.querySelector('.kw-card__side');
  if (!side) return;
  let stamp = side.querySelector('.kw-stamp');
  if (!stamp) {
    stamp = document.createElement('p');
    side.prepend(stamp);
  }
  stamp.className = 'kw-stamp kw-stamp--' + decision;
  stamp.textContent = DECIDED[decision] + 'で記録済み';
}

document.addEventListener('click', async (event) => {
  const button = event.target.closest('button[data-decision]');
  if (!button) return;
  const card = button.closest('article');
  const status = card.querySelector('.result');
  const textarea = card.querySelector('textarea');
  const comment = textarea.value;
  const reason_codes = [...card.querySelectorAll('.kw-chips input:checked')].map((i) => i.value);
  const decision = button.dataset.decision;
  status.classList.remove('is-error');
  if (decision === 'reject' && !comment.trim() && !reason_codes.length) {
    status.textContent = '見送りの理由を選ぶか、補足に記入してください。';
    status.classList.add('is-error');
    textarea.focus();
    return;
  }
  const buttons = card.querySelectorAll('button[data-decision]');
  buttons.forEach((b) => { b.disabled = true; });
  status.textContent = '記録しています…';
  try {
    const r = await fetch('/apps/product-scout/keywords/' + encodeURIComponent(card.dataset.id) + '/decision', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ run_id: card.dataset.run, decision, comment, reason_codes }),
    });
    const value = await r.json();
    if (!r.ok) throw new Error(value.error || '保存できませんでした');
    buttons.forEach((b) => b.setAttribute('aria-pressed', String(b === button)));
    markCard(card, decision);
    status.textContent = DECIDED[decision] + 'で記録しました。';
  } catch (e) {
    status.textContent = e.message || '保存できませんでした。';
    status.classList.add('is-error');
  } finally {
    buttons.forEach((b) => { b.disabled = false; });
  }
});
