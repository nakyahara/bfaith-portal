/**
 * 宛先ドメインがメールを受け取れるかの事前確認 (2026-08-27 中原さん要望)
 *
 * ■ 作った理由
 *   宛先を打ち間違えたまま送ると、相手のメールサーバーに届かず、数時間〜2日かけて
 *   「配信できませんでした」の通知だけが返ってくる。その間お客様には何も届いていないのに
 *   画面上は送信済みに見える。送る前にドメインの存在を確かめれば、打ち間違いの大半は
 *   その場で止められる (2026-08-27 メルカリShops no-reply 宛の配信失敗通知が繰り返し届いた件の議論)。
 *
 * ■ 方針: 止めるのは「確実に受け取れない」と分かったときだけ (fail-open)
 *   - ドメインが存在しない (NXDOMAIN) / メールサーバーの登録がまったく無い → 止める
 *   - RFC 7505 の null MX ('.') = ドメイン所有者が「メールは受け取らない」と宣言 → 止める
 *   - DNSが引けない・タイムアウト・その他のエラー → 止めない (正常な送信を邪魔しない)
 *   MX が無くても A/AAAA があれば配送される (implicit MX) ので、A/AAAA も見てから判断する。
 *   この関数は例外を投げない (呼び元が送信を止めてしまわないよう、内部で握って fail-open にする)。
 *
 * ■ 待たせない工夫
 *   A/AAAA は並列に引き、全体にもタイムアウトを置く。DNSが詰まっているときに送信ボタンが
 *   何秒も固まると二度押しを誘発するため (Codexレビュー Medium)。
 *
 * ■ できないこと
 *   MX は登録されているのに実際には受け取らないサーバー (例: mercari-shops.com) は
 *   ここでは見抜けない。そちらは通知専用ドメインの表 (no-reply.js) で塞ぐ。
 */
import dns from 'dns';

/** 同じドメインを何度も引かないための短期キャッシュ (画面操作のたびのDNSを減らす)。
 * 「受け取れる」は長め、「受け取れない」は短め — DNS設定の直しがすぐ反映されるように */
const CACHE_TTL_OK_MS = 10 * 60 * 1000;
const CACHE_TTL_NG_MS = 60 * 1000;
const DEFAULT_TIMEOUT_MS = 3000;      // 1クエリあたり
const DEFAULT_OVERALL_MS = 5000;      // MX + A/AAAA を含む全体
const cache = new Map();

/** メールアドレスからドメイン部を取り出す (取れなければ null) */
export function domainOf(address) {
  const addr = String(address || '').trim();
  const at = addr.lastIndexOf('@');
  if (at <= 0 || at === addr.length - 1) return null;
  const domain = addr.slice(at + 1).toLowerCase();
  return /^[a-z0-9.-]+\.[a-z0-9-]+$/.test(domain) ? domain : null;
}

/** テスト用の差し替え口 (本番コードからは呼ばない)。null で解除。
 * 画面・APIを通した検証で実際のDNSを引かせないために使う */
let testResolver = null;
export function setResolverForTest(r) { testResolver = r; }

/** 実DNS用のリゾルバ (タイムアウト付き)。テストでは resolver を差し替える */
function defaultResolver(timeoutMs) {
  const r = new dns.promises.Resolver({ timeout: timeoutMs, tries: 1 });
  return {
    resolveMx: d => r.resolveMx(d),
    resolve4: d => r.resolve4(d),
    resolve6: d => r.resolve6(d),
  };
}

/** DNSの「答えは返ってきたが、そのレコードは無い」= 判断材料として有効なエラー */
const NO_RECORD_CODES = new Set(['ENOTFOUND', 'ENODATA', 'NXDOMAIN']);

const skip = (why) => ({ ok: true, skipped: true, why });

/**
 * 宛先がメールを受け取れるドメインかを確かめる。例外は投げない。
 * @param {string} address 宛先メールアドレス
 * @param {{resolver?: object, timeoutMs?: number, overallTimeoutMs?: number, now?: number, useCache?: boolean}} opts
 * @returns {Promise<{ok: boolean, reason?: string, skipped?: boolean, why?: string, domain?: string}>}
 *   ok=false のとき reason はそのまま画面に出せる日本語。
 *   skipped=true は「確認できなかったので通した」(fail-open) の意味
 */
export async function checkRecipientDomain(address, opts = {}) {
  const domain = domainOf(address);
  // 形式がおかしいものはここでは判定しない (アドレスの形の検証は compose.js の役目)
  if (!domain) return skip('ドメインを取り出せませんでした');
  // DNSが使えない環境や検証時に丸ごと止められる逃げ道 (既定は有効)。
  // resolver を渡されているとき (テスト) は注入されたものを使う
  if (!opts.resolver && !testResolver && String(process.env.INQUIRY_HUB_MX_CHECK || '').toLowerCase() === 'off') {
    return { ...skip('ドメインの事前確認は無効 (INQUIRY_HUB_MX_CHECK=off)'), domain };
  }

  const now = opts.now ?? Date.now();
  const useCache = opts.useCache !== false;
  if (useCache) {
    const hit = cache.get(domain);
    if (hit && now - hit.at < hit.ttl) return { ...hit.result, domain, cached: true };
  }

  const overall = opts.overallTimeoutMs ?? DEFAULT_OVERALL_MS;
  let timer = null;
  let result;
  try {
    // リゾルバの生成自体が失敗しても止めない (Codexレビュー: try の外に置かない)
    const resolver = opts.resolver || testResolver || defaultResolver(opts.timeoutMs ?? DEFAULT_TIMEOUT_MS);
    const guard = new Promise((resolve) => {
      timer = setTimeout(() => resolve(skip(`確認が${overall}msで終わりませんでした`)), overall);
    });
    result = await Promise.race([lookup(resolver, domain), guard]);
  } catch (e) {
    result = skip(`DNSを確認できませんでした (${e?.code || e?.message || e})`);
  } finally {
    if (timer) clearTimeout(timer);
  }

  // fail-open で通したもの (skipped) はキャッシュしない — 次回はもう一度確かめる
  if (useCache && !result.skipped) {
    cache.set(domain, { at: now, ttl: result.ok ? CACHE_TTL_OK_MS : CACHE_TTL_NG_MS, result });
  }
  return { ...result, domain };
}

/** MX → (無ければ) A/AAAA の順に見て判断する。例外は投げない */
async function lookup(resolver, domain) {
  let mx;
  try {
    mx = await resolver.resolveMx(domain);
  } catch (e) {
    if (!NO_RECORD_CODES.has(e?.code)) return skip(`DNSを確認できませんでした (${e?.code || e?.message || e})`);
    // MX が無いだけなら A/AAAA でも配送され得る (implicit MX)。ドメインごと無い場合も含めて次で確定させる
    return addressRecords(resolver, domain, e.code === 'ENOTFOUND');
  }
  const records = (mx || []).filter(r => r && typeof r.exchange === 'string');
  // RFC 7505: exchange が '.' だけの MX = 「このドメインはメールを受け取らない」の明示
  if (records.length && records.every(r => r.exchange.trim() === '.' || r.exchange.trim() === '')) {
    return { ok: false, reason: `${domain} はメールを受け取らない設定です (宛先を確認してください)` };
  }
  if (records.length) return { ok: true };
  return addressRecords(resolver, domain, false);
}

/** MX が無いドメインの最終判断: A/AAAA があれば受け取れる可能性がある (implicit MX)。
 * A と AAAA は並列に引く (直列だとタイムアウトが積み上がって送信ボタンが固まる) */
async function addressRecords(resolver, domain, domainMissing) {
  const probe = async (fn) => {
    try {
      const rows = await fn(domain);
      return Array.isArray(rows) && rows.length > 0 ? 'found' : 'none';
    } catch (e) {
      return NO_RECORD_CODES.has(e?.code) ? 'none' : 'unknown';
    }
  };
  const [v4, v6] = await Promise.all([probe(resolver.resolve4), probe(resolver.resolve6)]);
  if (v4 === 'found' || v6 === 'found') return { ok: true };
  // 片方でも「確かめられなかった」なら止めない (確実に受け取れないと言い切れない)
  if (v4 === 'unknown' || v6 === 'unknown') return skip('A/AAAAレコードを確認できませんでした');
  return {
    ok: false,
    reason: domainMissing
      ? `送信先のドメイン ${domain} が見つかりません (メールアドレスの打ち間違いがないか確認してください)`
      : `${domain} にメールサーバーが登録されていません (メールアドレスの打ち間違いがないか確認してください)`,
  };
}

/** テスト用: キャッシュを空にする */
export function clearMxCache() {
  cache.clear();
}
