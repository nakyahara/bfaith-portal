/**
 * R16 (2026-07-04 中原さん指示・重大): Yahoo に送る HTML/テキストから楽天リンクを根絶する。
 *
 * 背景: 楽天 salesDescription 内の「セット商品」「関連商品」テーブルに
 * `<a href="https://item.rakuten.co.jp/b-faith/AA0203/">` 形式の楽天内部リンクが入っており、
 * これがそのまま Yahoo に登録されていた (Yahoo 側で bouncer 経由の外部リンクとして表示される)。
 * Yahoo!ショッピングの規約上、他モールへの誘導リンクは違反 → 必ず Yahoo 内部リンクに変換する。
 *
 * ポリシー (fail-closed):
 *   1. 自店商品リンク item.rakuten.co.jp/{RAKUTEN_SHOP_SLUG}/{code}/
 *        → https://store.shopping.yahoo.co.jp/{YAHOO_SELLER_ID}/{code 小文字}.html に書換
 *          (Yahoo 移行後の item_code = 楽天商品管理番号なので URL は決定的に対応する)
 *   2. その他の楽天系リンク (rakuten.co.jp / rakuten.ne.jp / r10s.jp / rakuten.com /
 *      rakuten-static.com とその全 subdomain) → <a> を unwrap (リンク除去、中身のみ残す)
 *   3. YAHOO_SELLER_ID 未設定でも楽天リンクは絶対に残さない (書換不能なら unwrap)
 *   4. 最終 backstop: findRakutenResidueInFields で送信直前の全 string field を走査し、
 *      楽天ドメイン文字列 (percent-encode された bouncer 形式含む) が残っていれば検知
 */

const RAKUTEN_FAMILY_SUFFIXES = [
  'rakuten.co.jp',
  'rakuten.ne.jp',
  'r10s.jp',
  'rakuten.com',
  'rakuten-static.com',
];

// 送信直前 backstop 用: URL らしき文字列中に楽天ドメインが含まれるか。
//   - href に残った生 URL も、 bouncer 等で percent-encode された `%2F...rakuten.co.jp` も、
//     `dest_path=https%3A%2F%2Fitem.rakuten.co.jp...` のような形も「rakuten.co.jp」等の
//     ドメイン文字列そのもので検知する (エンコードでドメイン名の文字自体は変わらない)。
export const RAKUTEN_RESIDUE_RE = /(?:rakuten\.(?:co\.jp|ne\.jp|com)|r10s\.jp|rakuten-static\.com)/i;

/**
 * Codex R16 R2 High: 文字列全体への decodeURIComponent は、 どこか 1 箇所でも不正な `%`
 * (例: `%ZZ`) があると全体が throw して decode が中断し、 別の場所にある `%2E` 等の
 * エンコード済み楽天ドメインを見逃す。 有効な `%xx` トークンだけを部分的に復号する
 * tolerant decode で、 不正 `%` が混ざっていてもドメイン部分は必ず復号されるようにする。
 * (multibyte UTF-8 の %E3 等は単独 decode 不能でそのまま残るが、 ドメイン検知に必要なのは
 *  ASCII の `%2E`/`%2F`/`%3A` だけなので問題ない)
 */
function tolerantDecodeOnce(s) {
  return String(s).replace(/%[0-9A-Fa-f]{2}/g, (m) => {
    try {
      return decodeURIComponent(m);
    } catch (_) {
      return m;
    }
  });
}

/**
 * Codex R16 R1 High: `.` が `%2E` 等に percent-encode されていると raw 文字列への
 * regex が素通りする。 最大 3 回まで反復 decode して安定形を得る (二重エンコード対策)。
 * 通常は decodeURIComponent (multibyte も正しく復号)、 throw したら tolerant decode に
 * フォールバック (Codex R2 High)。
 */
export function deepDecode(s) {
  let cur = String(s);
  for (let i = 0; i < 3; i += 1) {
    let next;
    try {
      next = decodeURIComponent(cur);
    } catch (_) {
      next = tolerantDecodeOnce(cur);
    }
    if (next === cur) break;
    cur = next;
  }
  return cur;
}

export function isRakutenFamilyHost(hostname) {
  if (!hostname) return false;
  const h = String(hostname).toLowerCase();
  return RAKUTEN_FAMILY_SUFFIXES.some((suf) => h === suf || h.endsWith(`.${suf}`));
}

function getShopSlug(shopSlug) {
  return String(shopSlug || process.env.RAKUTEN_SHOP_SLUG || 'b-faith').trim().toLowerCase();
}

function getSellerId(sellerId) {
  const v = String(sellerId || process.env.YAHOO_SELLER_ID || '').trim();
  return v || null;
}

function escapeRegExp(s) {
  return String(s).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/**
 * R18 (2026-07-04 中原さん指示): 楽天が商品説明に自動挿入するカテゴリパンくず
 * `<!--ECMPAN--> ... <!--/ECMPAN-->` ブロックは Yahoo への移行対象外として丸ごと除去する。
 * (実例: GONESH-no4-2 の SP用フリースペースに楽天カテゴリページへのリンク列が入っていた)
 * 閉じ忘れ等で対にならない stray マーカーも除去する (中身は通常の link sanitize に任せる)。
 * Yahoo の全項目に適用するため、 sanitizeProductHtml の入口 + explanation/executor の
 * 生 HTML 読み取り箇所で呼ぶ。
 */
const ECMPAN_BLOCK_RE = /<!--\s*ECMPAN\s*-->[\s\S]*?<!--\s*\/\s*ECMPAN\s*-->/gi;
const ECMPAN_MARKER_RE = /<!--\s*\/?\s*ECMPAN\s*-->/gi;
export function stripEcmpanBlocks(html) {
  if (html == null || html === '') return html == null ? '' : html;
  return String(html).replace(ECMPAN_BLOCK_RE, '').replace(ECMPAN_MARKER_RE, '');
}

/**
 * 楽天 URL 1 本を判定して Yahoo 側の扱いを返す。
 *
 * 対応する形:
 *   - 直接リンク: https://item.rakuten.co.jp/b-faith/AA0203/
 *   - redirect/bouncer 形式: https://shopping.yahoo.co.jp/.../bouncer.html?dest_path=https%3A%2F%2Fitem.rakuten.co.jp%2Fb-faith%2FAA0203%2F
 *     (楽天ドメインが query に percent-encode で埋まっているケースも decode して判定)
 *
 * @returns {{ kind: 'not_rakuten' } | { kind: 'rewrite', yahooUrl: string } | { kind: 'remove' }}
 *   - not_rakuten: 楽天系ドメインを一切含まない (触らない)
 *   - rewrite:     自店商品リンク → yahooUrl に書換
 *   - remove:      楽天系だが書換不能 (他店/検索/イベント/CDN 等、 または seller 未設定) → リンク除去
 */
export function classifyRakutenHref(href, { shopSlug = null, sellerId = null } = {}) {
  if (!href) return { kind: 'not_rakuten' };
  const raw = String(href).trim();

  let isRakuten = false;
  try {
    const u = new URL(raw);
    isRakuten = isRakutenFamilyHost(u.hostname);
  } catch (_) {
    // parse 不能 (相対 URL 等) は isSafeHref (http/https のみ) 側で落ちるので host 判定は skip
  }
  // host が楽天系でなくても、 URL 文字列中に楽天ドメインが埋まっていれば楽天系として扱う
  // (bouncer / redirect / dest_path percent-encode 形式)。
  // Codex R16 R1 High: `%2E` 等で `.` までエンコードされた形をすり抜けさせないため、
  // raw と反復 decode 済みの両方を検査する。
  const decoded = deepDecode(raw);
  if (!isRakuten && !RAKUTEN_RESIDUE_RE.test(raw) && !RAKUTEN_RESIDUE_RE.test(decoded)) {
    return { kind: 'not_rakuten' };
  }

  // 自店商品リンク抽出: percent-encode されていても decode して item.rakuten.co.jp/{slug}/{code} を探す。
  //   R18 (GONESH-no4-2 実例で発覚): /b-faith/c/0000000220/ はカテゴリページ (c/ prefix) であって
  //   商品ではない。 終端 lookahead (次が更なる path segment なら不一致) + code 'c' 除外で、
  //   カテゴリ URL を誤って /c.html に書換せず remove に落とす。
  const seller = getSellerId(sellerId);
  if (seller) {
    const m = decoded.match(
      new RegExp(
        `item\\.rakuten\\.co\\.jp/${escapeRegExp(getShopSlug(shopSlug))}/([A-Za-z0-9._-]+)/?(?=[?#"'<>&|\\s]|$)`,
        'i'
      )
    );
    if (m && m[1].toLowerCase() !== 'c') {
      const code = m[1].toLowerCase();
      return { kind: 'rewrite', yahooUrl: `https://store.shopping.yahoo.co.jp/${seller}/${code}.html` };
    }
  }
  return { kind: 'remove' };
}

// テキスト中の URL 抽出 (プレーンテキスト explanation 用)。
//   区切り: 空白 / 引用符 / タグ括弧 / 全角括弧・句読点
const TEXT_URL_RE = /https?:\/\/[^\s"'<>()（）「」【】、。]+/g;

/**
 * プレーンテキスト (explanation 等) から楽天 URL を除去/書換する。
 *   - 自店商品 URL → Yahoo URL 文字列に書換
 *   - その他の楽天系 URL → 削除 (空文字置換)
 */
export function scrubRakutenUrlsFromText(text, { shopSlug = null, sellerId = null } = {}) {
  if (text == null || text === '') return text == null ? '' : text;
  return String(text).replace(TEXT_URL_RE, (url) => {
    const c = classifyRakutenHref(url, { shopSlug, sellerId });
    if (c.kind === 'rewrite') return c.yahooUrl;
    if (c.kind === 'remove') return '';
    return url;
  });
}

/**
 * 送信直前 backstop: fields の全 string 値に楽天ドメイン文字列が残っていないか走査。
 *
 * @param {object} fields Yahoo editItem に送る fields
 * @returns {Array<{field: string, sample: string}>} 残存箇所 (空配列 = clean)
 */
export function findRakutenResidueInFields(fields) {
  const residues = [];
  if (!fields || typeof fields !== 'object') return residues;
  for (const [k, v] of Object.entries(fields)) {
    if (typeof v !== 'string' || v === '') continue;
    // Codex R16 R1 High: raw と反復 decode 済みの両方を検査 (%2E エンコード形も検知)
    const target = RAKUTEN_RESIDUE_RE.test(v) ? v : deepDecode(v);
    const m = target.match(RAKUTEN_RESIDUE_RE);
    if (m) {
      const idx = Math.max(0, target.toLowerCase().indexOf(m[0].toLowerCase()) - 60);
      residues.push({ field: k, sample: target.slice(idx, idx + 160) });
    }
  }
  return residues;
}
