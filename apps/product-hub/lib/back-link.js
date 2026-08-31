/**
 * 詳細画面の「← 戻る」の戻り先 (2026-08-28 中原さん要望)。
 *
 * 工程ボードのカードから開いたら、直前に見ていたボード (本流 / 画像・絞り込み・種別) に戻す。
 * 一覧から開いたときは今まで通り一覧へ。
 *
 * ?back= の中身は**素通ししない**。既知のキーだけを拾って URL を組み直すので、
 * 外部サイトへ飛ばされる余地がない (オープンリダイレクト対策)。
 * ボード側 (board.ejs) が付ける印は v=board + view/assignee/filter/kind。
 */

export const LIST_BACK_LINK = { url: '/apps/product-hub/list', label: '← 一覧に戻る' };

export function backLinkOf(query) {
  const raw = String(query?.back ?? '');
  // 長すぎる値は組み立て直す価値がない (壊れたリンク・いたずら)
  if (!raw || raw.length > 200) return LIST_BACK_LINK;
  let src;
  try { src = new URLSearchParams(raw); } catch { return LIST_BACK_LINK; }
  if (src.get('v') !== 'board') return LIST_BACK_LINK;
  const p = new URLSearchParams();
  const isImage = src.get('view') === 'image';
  if (isImage) p.set('view', 'image');
  const assignee = String(src.get('assignee') ?? '');
  if (assignee === 'me' || /^\d{1,9}$/.test(assignee)) p.set('assignee', assignee);
  const filter = src.get('filter');
  // 確認中 (2026-08-31) も戻り先として通す。ここに足さないと、確認中で絞ったボードから
  // カードを開いて解除 → 戻ると絞り込みが外れ、続きを処理する列を見失う
  if (filter === 'unassigned' || filter === 'checking') p.set('filter', filter);
  const kind = src.get('kind');
  // 種別は画像ビューだけの条件 (本流に付けても board 側が無視する)
  if (isImage && (kind === 'top' || kind === 'detail')) p.set('kind', kind);
  const qs = p.toString();
  return {
    url: '/apps/product-hub/board' + (qs ? '?' + qs : ''),
    label: isImage ? '← 工程ボード (画像) に戻る' : '← 工程ボードに戻る',
  };
}
