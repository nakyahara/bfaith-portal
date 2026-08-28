'use strict';
// 新商品企画スカウト — 採否の記録だけを行う小さなスクリプト。
// 画面の描画はサーバ側 (EJS) に寄せてあるので、ここは送信と表示切替だけ。

document.addEventListener('change', function (ev) {
  const sel = ev.target;
  if (sel.name !== 'decision') return;
  const form = sel.closest('.ps-decide');
  if (!form) return;
  // 不採用のときだけ理由コードを出す。常時出すと採用時に選ばせる意味のない欄が残る
  const reason = form.querySelector('.ps-reason-select');
  const isReject = sel.value === 'reject';
  reason.hidden = !isReject;
  if (!isReject) reason.value = '';
});

document.addEventListener('submit', async function (ev) {
  const form = ev.target;
  if (!form.classList || !form.classList.contains('ps-decide')) return;
  ev.preventDefault();

  const article = form.closest('.ps-concept');
  const msg = form.querySelector('.ps-msg');
  const decision = form.querySelector('[name=decision]').value;
  if (!decision) { msg.textContent = '判断を選んでください'; return; }

  const reasonCode = form.querySelector('[name=reasonCode]').value;
  const comment = form.querySelector('[name=comment]').value;
  const recheckCondition = form.querySelector('[name=recheckCondition]').value;

  if (decision === 'reject' && !reasonCode) { msg.textContent = '不採用の理由を選んでください'; return; }
  if (decision === 'reject' && reasonCode === 'other' && !comment.trim()) {
    msg.textContent = '「その他」のときはコメントを書いてください'; return;
  }

  const btn = form.querySelector('button');
  btn.disabled = true;
  msg.textContent = '記録中…';
  try {
    const res = await fetch('/apps/product-scout/concepts/' + article.dataset.id + '/decision', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ decision: decision, reasonCode: reasonCode, comment: comment, recheckCondition: recheckCondition }),
    });
    const json = await res.json().catch(function () { return {}; });
    if (!res.ok) throw new Error(json.error || ('HTTP ' + res.status));
    msg.textContent = '記録しました';
    article.classList.add('ps-just-decided');
    btn.textContent = '記録済み';
  } catch (e) {
    msg.textContent = '失敗: ' + e.message;
    btn.disabled = false;
  }
});
