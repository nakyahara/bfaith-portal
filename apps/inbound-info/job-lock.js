/**
 * 入庫情報管理の重い処理を1本に直列化するプロセス内 mutex。
 *
 * 対象: 入荷予定 (nefuda.csv) の取得と、入荷予定リストPDF の生成→Drive保存。
 * それぞれ別々に排他していると次の順序が成立してしまうため、同じロックに入れる
 * (Codex pdf-R1 Medium):
 *   PDF が古いDBを読む → 別経路が CSV を更新 → PDF が古い内容で Drive を上書き
 * PDF生成は Python 子プロセスを含み数秒〜十数秒かかるので、この窓は現実に開く。
 *
 * 単一プロセス前提 (Render 1インスタンス)。DB側にも file_modified_time の鮮度ガードがあり二重防御。
 */
let chain = Promise.resolve();

/**
 * fn を直列に実行する。fn が失敗しても次の実行は塞がない。
 * ⚠ 入れ子で呼ぶとデッドロックするので、ロック内から別のロック対象を呼ばないこと。
 */
export function runExclusive(fn) {
  const p = chain.then(fn, fn);
  chain = p.catch(() => {});
  return p;
}
