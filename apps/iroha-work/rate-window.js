/**
 * 時間の窓で回数を数える、小さな道具 (要件 §AB-11 の 6)。
 *
 * 外部施設の専用 URL は**ログインがいらない口**なので、外から繰り返し叩かれても
 * 同じプロセスで動いている社内の画面を巻き込まないように、回数の上限を置く。
 *
 * ⭐ここまでに 3 回作り直している。作り直した理由を残しておく (同じ間違いを繰り返さないため):
 *
 *   ① 上限が無かった → 外から好きなだけ読める
 *   ② トークンを鍵にして数えた → **鍵を外から自由に作れる**ので、毎回ちがうトークンを送るだけで
 *      記録が無限に増え、掃除のため毎回全部を見にいく = 上限そのものが攻撃の口になった (Codex R3 重大)
 *   ③ あふれたら古いものから捨てた → **別の相手をたくさん作って自分の記録を消し、数え直せた** (Codex R4 中)
 *   ④ あふれたら断るようにした → その上限を**正しい URL にも掛けていた**ので、
 *      外から相手を大量に作るだけで先方を締め出せた (自己レビュー)
 *
 * いまの決まり:
 *   - 鍵は**外から自由に作れないもの**にする (確かめたあとのリンク id / 信用できる接続元)
 *   - あふれたら**期限切れだけ**捨て、それでも空かなければ**断る** (生きている記録は消さない)
 *   - 掃除で全部を見にいかない (入れた順に並ぶので、前から数個で足りる)
 */

/** 掃除で見る数の上限。入れた順 = 期限の順なので、これだけ見れば足りる */
const SWEEP_MAX = 50;

/**
 * 1 回ぶん数える。
 *
 * @param {Map} map      key → { count, until }。**入れた順**を保つので Map であること
 * @param {*} key        数える相手 (外から自由に作れないもの)
 * @param {number} max   窓の中で許す回数
 * @param {number} cap   覚える相手の上限 (0 = 上限なし。増えないと分かっている相手だけ)
 * @param {number} windowMs 窓の長さ (ミリ秒)
 * @param {number} now   いま (試験で時計を動かすため)
 * @returns {boolean} 通してよいか
 */
export function bumpWindow(map, key, max, cap, windowMs, now = Date.now()) {
  const cur = map.get(key);
  if (cur && cur.until > now) {
    if (++cur.count > max) return false;
    return true;
  }
  if (cur) map.delete(key);              // 期限切れは入れ直す (入れた順を新しくする)
  if (cap && map.size >= cap) {
    // ⭐**期限切れのものだけ**捨てる。前から数個で足りる (全部は見にいかない)
    let looked = 0;
    for (const [k, v] of map) {
      if (v.until > now) break;          // ここから先はまだ生きている (入れた順なので)
      map.delete(k);
      if (++looked >= SWEEP_MAX || map.size < cap) break;
    }
    // ⭐それでも空かないなら、**生きている記録は捨てずに断る**。
    //   捨てると、別の相手をたくさん作って自分の記録を消せる = 数え直しができてしまう
    if (map.size >= cap) return false;
  }
  map.set(key, { count: 1, until: now + windowMs });
  return true;
}
