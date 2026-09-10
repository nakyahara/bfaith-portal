document.addEventListener('click',async event=>{
  const button=event.target.closest('button[data-decision]');if(!button)return;
  const card=button.closest('article');const status=card.querySelector('.result');const comment=card.querySelector('textarea').value;
  const reason_codes=[...card.querySelectorAll('.reasons input:checked')].map(i=>i.value);
  if(button.dataset.decision==='reject'&&!comment.trim()&&!reason_codes.length){status.textContent='見送りの理由を選ぶか記入してください。';return;}
  const buttons=card.querySelectorAll('button');buttons.forEach(b=>b.disabled=true);
  try{
    const r=await fetch('/apps/product-scout/keywords/'+encodeURIComponent(card.dataset.id)+'/decision',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({run_id:card.dataset.run,decision:button.dataset.decision,comment,reason_codes})});
    const value=await r.json();if(!r.ok)throw new Error(value.error||'保存できませんでした');status.textContent=button.textContent+'で記録しました。';
  }catch(e){status.textContent=e.message||'保存できませんでした。';}finally{buttons.forEach(b=>b.disabled=false);}
});
