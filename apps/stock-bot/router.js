/**
 * stock-bot — Google Chat 在庫検索ボット (Render専用)
 *
 * 専用スペースに商品名 (または商品ID/バーコード) を送ると mirror_logizard_stock (毎時更新) を
 * 検索し、候補をボタンで返す。タップで該当SKUのロケーション別フリー在庫を返す。
 *
 * 費用ゼロ設計: Google Chat API は無料・検索はローカルSQLite・外部従量APIなし。
 *
 * mount: server.js で env STOCK_BOT_PROJECT_NUMBER 設定時のみ (miniPCは未設定=非mount。
 *        feedback_minipc_shares_portal_server_js の教訓で二重稼働を作らない)
 * 認証: Google Chat が付与する Bearer IDトークンを検証 (fail-closed)。
 *       issuer=chat@system.gserviceaccount.com / audience=GCPプロジェクト番号。
 *       加えて発言者のドメインを STOCK_BOT_ALLOWED_DOMAIN (既定 b-faith.biz) で制限。
 * セットアップ手順 = 同ディレクトリ README.md
 */
import { Router } from 'express';
import { OAuth2Client } from 'google-auth-library';
import { getMirrorDB } from '../warehouse-mirror/db.js';
import { buildStockLocationsText } from '../picking/stock-locations.js';

const CHAT_ISSUER = 'chat@system.gserviceaccount.com';
const CERTS_URL = `https://www.googleapis.com/service_accounts/v1/metadata/x509/${CHAT_ISSUER}`;
const MAX_CANDIDATES = 10;   // ボタンで並べる候補の上限

// ─── Google Chat リクエスト検証 ───

let _client = null;
const getClient = () => (_client ??= new OAuth2Client());
// in-flight Promise を共有 (同時リクエストで certs 取得が重複しないように)。1時間で失効
let _certsPromise = null;
let _certsAt = 0;

function getChatCerts(fetchFn = fetch) {
  if (!_certsPromise || Date.now() - _certsAt > 3600_000) {
    _certsAt = Date.now();
    _certsPromise = fetchFn(CERTS_URL).then(async (r) => {
      if (!r.ok) throw new Error(`certs取得失敗: HTTP ${r.status}`);
      return r.json();
    }).catch((e) => {
      _certsPromise = null;   // 失敗をキャッシュしない
      throw e;
    });
  }
  return _certsPromise;
}

function jwtKid(token) {
  try {
    const header = JSON.parse(Buffer.from(String(token).split('.')[0], 'base64url').toString('utf8'));
    return typeof header?.kid === 'string' && header.kid ? header.kid : null;
  } catch {
    return null;
  }
}

// 鍵ローテーション対応の強制再取得は全体で1分に1回まで (でたらめな kid の連打で
// Google への certs 取得を毎リクエスト強制させない)
let _lastForcedRefetch = 0;

async function verifyChatBearer(authorization) {
  const projectNumber = process.env.STOCK_BOT_PROJECT_NUMBER;
  if (!projectNumber) return false;   // mount gate があるので通常来ないが fail-closed
  const m = String(authorization || '').match(/^Bearer (.+)$/);
  if (!m) return false;
  const kid = jwtKid(m[1]);
  if (!kid) return false;
  try {
    let certs = await getChatCerts();
    if (!certs[kid]) {
      // 未知の kid = 鍵ローテーション直後の可能性のみ再取得。署名不正・audience不一致等の
      // 検証失敗ではキャッシュを捨てない (即401)
      if (Date.now() - _lastForcedRefetch < 60_000) return false;
      _lastForcedRefetch = Date.now();
      _certsPromise = null;
      certs = await getChatCerts();
      if (!certs[kid]) return false;
    }
    await getClient().verifySignedJwtWithCertsAsync(m[1], certs, projectNumber, [CHAT_ISSUER]);
    return true;
  } catch (e) {
    console.warn(`[stock-bot] トークン検証失敗: ${e.message}`);
    return false;
  }
}

/**
 * Bearer 検証ミドルウェア。**body parser より前に mount する** (server.js 参照) —
 * 未認可リクエストに body を読ませない (グローバル parser からも除外済み)。
 * verifyFn 注入はテスト用 (実署名なしで 401/通過を検証できるように)。
 */
export function makeStockBotAuth(verifyFn = verifyChatBearer) {
  return async (req, res, next) => {
    if (await verifyFn(req.headers.authorization)) return next();
    return res.status(401).json({ error: 'unauthorized' });
  };
}
export const stockBotAuth = makeStockBotAuth();

// ─── 検索 (mirror_logizard_stock) ───

const escapeLike = (s) => String(s).replace(/[\\%_]/g, (c) => `\\${c}`);

/** 商品名/商品ID 部分一致 + バーコード完全一致で SKU 候補を返す (良品フリー在庫の多い順)。 */
export function searchProducts(db, query, limit = MAX_CANDIDATES + 1) {
  const q = String(query || '').trim();
  if (!q) return [];
  const like = `%${escapeLike(q)}%`;
  return db.prepare(`
    SELECT 商品ID AS sku, MIN(商品名) AS name,
           SUM(CASE WHEN 品質区分名 = '良品' THEN 在庫数 - 引当数 ELSE 0 END) AS free
    FROM mirror_logizard_stock
    WHERE 商品名 LIKE ? ESCAPE '\\' OR 商品ID LIKE ? ESCAPE '\\' OR バーコード = ?
    GROUP BY 商品ID
    ORDER BY free DESC, 商品ID
    LIMIT ?
  `).all(like, like, q, limit);
}

/** SKU のロケーション別在庫を picking と同じ整形 (良品のみ・フリー降順・仮想ロケ合算) で返す。 */
export function buildStockReply(db, sku, now = new Date()) {
  const code = String(sku || '').trim().toLowerCase();   // mirror の商品ID は取込時に trim+小文字済み
  const locations = db.prepare(`
    SELECT ブロック略称 AS block, ロケ AS location, 品質区分名 AS quality,
           SUM(在庫数) AS qty, SUM(引当数) AS allocated, SUM(在庫数 - 引当数) AS free
    FROM mirror_logizard_stock
    WHERE 商品ID = ?
    GROUP BY ブロック略称, ロケ, 品質区分名
  `).all(code);
  if (locations.length === 0) return null;
  const meta = db.prepare(`
    SELECT MAX(captured_at) AS captured_at, MAX(在庫日) AS stock_date, MIN(商品名) AS name
    FROM mirror_logizard_stock WHERE 商品ID = ?
  `).get(code);
  const body = buildStockLocationsText(
    { ok: true, importedAt: meta?.captured_at || null, stockDate: meta?.stock_date || null, locations },
    { now, title: '在庫ロケーション', maxLines: 10 },
  );
  return `${meta?.name || ''}\n(${code})\n${body}`;
}

// ─── Chat イベント処理 ───

const USAGE = '商品名の一部 (例: ティーツリー)、商品ID、またはバーコードを送ると在庫ロケーションを探します。';

/** カード形式の候補リスト (ボタンタップで showStock を発火)。 */
function buildCandidatesCard(candidates, hasMore) {
  const buttons = candidates.map((c) => ({
    text: `${String(c.name || c.sku).slice(0, 40)} (フリー${c.free})`,
    onClick: { action: { function: 'showStock', parameters: [{ key: 'sku', value: c.sku }] } },
  }));
  const widgets = [{ buttonList: { buttons } }];
  if (hasMore) {
    widgets.push({ textParagraph: { text: `他にも候補があります (${MAX_CANDIDATES}件まで表示)。キーワードを絞ってください。` } });
  }
  return {
    text: `🔍 ${candidates.length}件見つかりました。タップで在庫を表示します。`,
    cardsV2: [{ cardId: 'stock-candidates', card: { sections: [{ widgets }] } }],
  };
}

/**
 * Chat イベント → 応答オブジェクト (同期応答)。DBエラー等は throw せず必ず応答を返す。
 * @param event Google Chat のイベント payload
 * @param db better-sqlite3 (mirror)
 */
export function handleChatEvent(event, db, now = new Date()) {
  const type = event?.type;
  if (type === 'ADDED_TO_SPACE') {
    return { text: `📦 在庫検索ボットです。${USAGE}` };
  }
  if (type === 'CARD_CLICKED') {
    // 形式ゆれ対応: 新形式 = common.invokedFunction + common.parameters (map)、
    // 旧形式 = action.actionMethodName + action.parameters ([{key,value}])
    const fn = event.common?.invokedFunction || event.action?.actionMethodName;
    const params = event.common?.parameters
      || Object.fromEntries((event.action?.parameters || []).map((p) => [p.key, p.value]));
    if (fn !== 'showStock' || !params.sku) {
      return { actionResponse: { type: 'NEW_MESSAGE' }, text: 'このボタンは処理できませんでした。もう一度検索してください。' };
    }
    const reply = buildStockReply(db, params.sku, now);
    return {
      actionResponse: { type: 'NEW_MESSAGE' },
      text: reply || `該当データが見つかりませんでした (${params.sku})。在庫が動いた可能性があります。もう一度検索してください。`,
    };
  }
  if (type === 'MESSAGE') {
    const q = String(event.message?.argumentText ?? event.message?.text ?? '').trim();
    if (!q) return { text: USAGE };
    // 1文字検索は候補が広すぎ+全表LIKE走査の無駄撃ちなので案内を返す
    if (q.length < 2) return { text: `キーワードは2文字以上で送ってください。${USAGE}` };
    const found = searchProducts(db, q);
    if (found.length === 0) return { text: `「${q}」に一致する商品が見つかりませんでした。別のキーワードで試してください。` };
    if (found.length === 1) return { text: buildStockReply(db, found[0].sku, now) };
    const hasMore = found.length > MAX_CANDIDATES;
    return buildCandidatesCard(found.slice(0, MAX_CANDIDATES), hasMore);
  }
  return {};   // REMOVED_FROM_SPACE 等は黙って 200
}

// ─── レート制限 (スペース単位・in-memory) ───
// 全表LIKE走査の連打対策。認証済み社内ユーザーしか来ない前提の軽いガード

const RATE_LIMIT_PER_MIN = 20;
const _rate = new Map();   // key → { windowStart, count }

export function checkRateLimit(key, nowMs = Date.now()) {
  if (_rate.size > 1000) _rate.clear();   // 異常な多スペースは丸ごとリセット (正常運用では数個)
  const cur = _rate.get(key);
  if (!cur || nowMs - cur.windowStart >= 60_000) {
    _rate.set(key, { windowStart: nowMs, count: 1 });
    return true;
  }
  cur.count++;
  return cur.count <= RATE_LIMIT_PER_MIN;
}

// ─── ルーター ───

const router = Router();

// 認証 (stockBotAuth) は server.js で body parser より前に掛けている
router.post('/chat-events', (req, res) => {
  const event = req.body || {};
  // 社内ドメイン限定 (Chat アプリの公開設定と二重の防御)。検索・ボタン操作は発言者メール必須
  // (fail-closed)。ADDED_TO_SPACE 等の管理イベントのみメール無しを許容
  const allowedDomain = (process.env.STOCK_BOT_ALLOWED_DOMAIN || 'b-faith.biz').toLowerCase();
  const email = String(event.user?.email || '').toLowerCase();
  const needsUser = event.type === 'MESSAGE' || event.type === 'CARD_CLICKED';
  if (needsUser && !email) {
    return res.json({ text: '利用者を確認できなかったため処理できません。' });
  }
  if (email && !email.endsWith(`@${allowedDomain}`)) {
    return res.json({ text: '社内ユーザーのみ利用できます。' });
  }
  if (!checkRateLimit(event.space?.name || event.user?.name || 'unknown')) {
    return res.json({ text: '検索が集中しています。1分ほどおいて再送してください。' });
  }
  try {
    return res.json(handleChatEvent(event, getMirrorDB()));
  } catch (e) {
    // mirror 未初期化 (boot直後) や想定外の payload でも 500 にしない (Chat側の再送・エラー表示を避ける)
    console.error('[stock-bot] イベント処理失敗:', e);
    return res.json({ text: '在庫データの準備中か、一時的なエラーです。少し待ってもう一度送ってください。' });
  }
});

export default router;
