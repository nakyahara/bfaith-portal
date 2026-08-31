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
import { parseYahooReauthCommand, handleYahooReauth } from './yahoo-reauth.js';
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

function jwtClaims(token) {
  try {
    return JSON.parse(Buffer.from(String(token).split('.')[1], 'base64url').toString('utf8'));
  } catch {
    return null;
  }
}

// Workspaceアドオン形式の呼び出し元: accounts.google.com 発行の OIDC IDトークンで、
// email = アドオン実行基盤のサービスアカウント (プロジェクト番号で一意)
const ADDON_ISSUERS = ['https://accounts.google.com', 'accounts.google.com'];
const addonCallerEmail = (projectNumber) =>
  `service-${projectNumber}@gcp-sa-gsuiteaddons.iam.gserviceaccount.com`;
// このボットの公開エンドポイントURL。アドオン形式のカード内ボタン action.function は
// エンドポイントURL必須 (関数名だとクライアント側で「リクエストを処理できません」になり、
// サーバーに届かない)。上書き env は https URL のみ受け付ける (非URLだと全ボタンが壊れるため)
const DEFAULT_CHAT_EVENTS_URL = 'https://bfaith-portal.onrender.com/apps/stock-bot/chat-events';
export const CHAT_EVENTS_URL = /^https:\/\//.test(process.env.STOCK_BOT_CHAT_EVENTS_URL || '')
  ? process.env.STOCK_BOT_CHAT_EVENTS_URL : DEFAULT_CHAT_EVENTS_URL;
// audience は構成世代により「プロジェクト番号」か「エンドポイントURL」のどちらかで届く
// (Codexレビュー high: 固定にすると構成次第で正規リクエストが全401)。両方+任意の上書き値を許可
const addonAudiences = (projectNumber) => {
  const list = [projectNumber, CHAT_EVENTS_URL];
  if (process.env.STOCK_BOT_ADDON_AUDIENCE) list.push(process.env.STOCK_BOT_ADDON_AUDIENCE);
  return list;
};

// 鍵ローテーション対応の強制再取得は全体で1分に1回まで (でたらめな kid の連打で
// Google への certs 取得を毎リクエスト強制させない)
let _lastForcedRefetch = 0;

async function verifyChatBearer(authorization) {
  const projectNumber = process.env.STOCK_BOT_PROJECT_NUMBER;
  if (!projectNumber) return false;   // mount gate があるので通常来ないが fail-closed
  const m = String(authorization || '').match(/^Bearer (.+)$/);
  if (!m) return false;
  // アドオン形式 (2025年以降の新規Chatアプリ): accounts.google.com 発行の IDトークン。
  // audience 許可リスト + 呼び出し元SAメールの完全一致で検証 (どちらも fail-closed)
  const iss = jwtClaims(m[1])?.iss;
  if (ADDON_ISSUERS.includes(iss)) {
    try {
      const ticket = await getClient().verifyIdToken({ idToken: m[1], audience: addonAudiences(projectNumber) });
      const p = ticket.getPayload();
      return p?.email_verified === true && p?.email === addonCallerEmail(projectNumber);
    } catch (e) {
      console.warn(`[stock-bot] アドオン形式トークン検証失敗: ${e.message}`);
      return false;
    }
  }
  // 旧形式: chat@system.gserviceaccount.com 発行 (x509証明書で自前検証)
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
    SELECT ブロック略称 AS block, ロケ AS location, 品質区分名 AS quality, 有効期限 AS expiry,
           SUM(在庫数) AS qty, SUM(引当数) AS allocated, SUM(在庫数 - 引当数) AS free
    FROM mirror_logizard_stock
    WHERE 商品ID = ?
    GROUP BY ブロック略称, ロケ, 品質区分名, 有効期限
  `).all(code);
  if (locations.length === 0) return null;
  const meta = db.prepare(`
    SELECT MAX(captured_at) AS captured_at, MAX(在庫日) AS stock_date, MIN(商品名) AS name
    FROM mirror_logizard_stock WHERE 商品ID = ?
  `).get(code);
  const body = buildStockLocationsText(
    { ok: true, importedAt: meta?.captured_at || null, stockDate: meta?.stock_date || null, locations },
    { now, title: '在庫ロケーション' },
  );
  return `${meta?.name || ''}\n(${code})\n${body}`;
}

// ─── Chat イベント処理 ───

const USAGE = '商品名の一部 (例: ティーツリー)、商品ID、またはバーコードを送ると在庫ロケーションを探します。';

/**
 * MESSAGE から検索語を取り出す。旧形式の argumentText はメンション除去済みだが、
 * アドオン形式では argumentText 自体が無い、または**メンションを含んだまま**届く
 * (実機確認 2026-08-18) → どちらを採っても必ずメンション除去を通す。
 * 除去は annotations の USER_MENTION 表示名ベース (startIndex/length は文字数の
 * 数え方が環境依存のため使わない — 日本語名で中途切断の危険)
 */
function messageQuery(message) {
  const arg = message?.argumentText;
  let text = (typeof arg === 'string' && arg.trim()) ? arg : String(message?.text ?? '');
  for (const a of (Array.isArray(message?.annotations) ? message.annotations : [])) {
    const dn = a?.type === 'USER_MENTION' ? a.userMention?.user?.displayName : null;
    // 1注釈 = 1除去 (最初の一致のみ)。全置換だと検索語側の同名文字列まで消える
    if (dn) text = text.replace(`@${dn}`, ' ');
  }
  // annotations が無い/表示名が取れない場合の保険: 先頭の@トークンのみ除去。
  // 「@ボット名␣検索語」の空白区切り入力だけを想定した割り切り (メンションと検索語が
  // 密着/句読点区切りの場合は除去しきれず0件応答になるが、本文が見えるので自己解決可能)。
  // 旧形式で検索語自体が「@xxx 」で始まる場合も削られるが、商品検索に@始まりは実在しない前提
  return text.replace(/^\s*@\S+\s+/, '').trim();
}

/** カード形式の候補リスト (ボタンタップで showStock を発火)。 */
function buildCandidatesCard(candidates, hasMore) {
  const buttons = candidates.map((c) => ({
    text: `${String(c.name || c.sku).slice(0, 40)} (フリー${c.free})`,
    // method 併記: アドオン形式では function 名がクリックイベントに入らないことがある
    onClick: { action: { function: 'showStock', parameters: [{ key: 'method', value: 'showStock' }, { key: 'sku', value: c.sku }] } },
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
    const params = event.common?.parameters
      || Object.fromEntries((event.action?.parameters || []).map((p) => [p.key, p.value]));
    // 関数名の解決: アドオン形式では invokedFunction に function 名が入らないことがある
    // (URL だったり空だったり) ため、カード側で併記した method パラメータを最優先。
    // __action_method_name__ = Google のアドオン移行時の旧 function 名の受け皿
    const fn = params.method || params.__action_method_name__
      || event.common?.invokedFunction || event.action?.actionMethodName;
    // 配送ルール変更の承認カード (packing-dispatch/rule-change.js が投稿・同じChatアプリで受ける)
    if ((fn === 'pdRuleApprove' || fn === 'pdRuleReject') && params.id) {
      return handleRuleDecision(fn, params, event);
    }
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
    const q = messageQuery(event.message);
    if (!q) return { text: USAGE };
    // Yahoo OAuth 再認可 (yahoo-reauth.js)。「yahoo再認可」/ リダイレクト URL (code=…) の貼り付けを商品検索より先に拾う。
    // 権限は env (YAHOO_REAUTH_USERS) で fail-closed。非同期 = Promise を返す (router 側は Promise.resolve で await 済み)
    const reauthCmd = parseYahooReauthCommand(q);
    if (reauthCmd) return handleYahooReauth(reauthCmd, { email: event.user?.email });
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

// ─── 配送ルール変更の承認 (packing-dispatch連携・遅延import) ───

// 承認者の制限 — fail-closed (Codexレビュー high: 本番の配送ルールを書き換える操作のため、
// PD_RULE_APPROVERS 未設定なら誰も承認できない)。カンマ区切りメールで指定
function ruleApproverAllowed(email) {
  const raw = String(process.env.PD_RULE_APPROVERS || '').trim();
  if (!raw) return false;
  return raw.toLowerCase().split(',').map((s) => s.trim()).includes(String(email || '').toLowerCase());
}

async function handleRuleDecision(fn, params, event) {
  const email = String(event.user?.email || '');
  // 承認カードを流すスペース以外からのボタンは受けない (別スペースへの転送・偽カード対策)
  const expectSpace = String(process.env.PD_RULE_CHANGE_SPACE || '');
  if (!expectSpace || event.space?.name !== expectSpace) {
    return { actionResponse: { type: 'NEW_MESSAGE' }, text: 'このスペースでは承認操作を受け付けていません。' };
  }
  if (!ruleApproverAllowed(email)) {
    return { actionResponse: { type: 'NEW_MESSAGE' }, text: `承認権限がありません (${email})。管理者が PD_RULE_APPROVERS に追加すると使えます。` };
  }
  try {
    const { applyRuleChangeDecision } = await import('../packing-dispatch/rule-change.js');
    const { decisionReplyText } = await import('../packing-dispatch/rule-change.js');
    const row = applyRuleChangeDecision(Number(params.id), fn === 'pdRuleApprove' ? 'approve' : 'reject', email);
    // 元カードを結果テキストへ差し替え (ボタンを消して二重押しを防ぐ。冪等ガードはDB側にもある)
    return { actionResponse: { type: 'UPDATE_MESSAGE' }, text: decisionReplyText(row) };
  } catch (e) {
    console.error('[stock-bot] ルール変更の承認処理失敗:', e);
    return { actionResponse: { type: 'NEW_MESSAGE' }, text: `処理に失敗しました: ${e.message}` };
  }
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

// ─── Workspaceアドオン形式の互換 ───
// 2025年以降に GCP コンソールで新規作成した Chat アプリは「Google Workspace アドオン」として
// ビルドされ、イベントが event.chat.* に包まれて届き、応答も hostAppDataAction で包む必要がある
// (旧形式 = event.type / 素の {text})。既存ロジックは旧形式のまま、入口と出口で変換する。

/**
 * アドオン形式の userIdToken (発言者本人のIDトークン) から署名検証済みの email を返す。
 * 検証不能・email_verified でない場合は '' (fail-closed → ルート側で「利用者を確認
 * できなかった」応答になる)。audience はアドオン構成の OAuth クライアントIDで不定のため
 * 署名・発行者・期限のみ検証 (メールの真正性はGoogle署名で担保。権限判定はこの email +
 * PD_RULE_APPROVERS の突合で行う)
 */
export async function userEmailFromIdToken(token, client = getClient()) {
  if (!token) return '';
  try {
    const ticket = await client.verifyIdToken({ idToken: token });
    const p = ticket.getPayload();
    return p?.email_verified === true && typeof p?.email === 'string' ? p.email : '';
  } catch (e) {
    console.warn(`[stock-bot] userIdToken検証失敗: ${e.message}`);
    return '';
  }
}

/** アドオン形式イベント → 旧形式 (handleChatEvent が読める形)。旧形式はそのまま素通し。 */
export function adaptChatEvent(raw) {
  const chat = raw?.chat;
  if (!chat || typeof chat !== 'object') return { event: raw || {}, addon: false };
  const common = raw.commonEventObject || {};
  const user = { ...(chat.user || {}) };
  const p = chat.messagePayload || chat.addedToSpacePayload || chat.removedFromSpacePayload
    || chat.buttonClickedPayload || chat.appCommandPayload || {};
  const base = { user, space: p.space || p.message?.space || chat.space, message: p.message };
  const asClick = () => ({
    event: {
      ...base,
      type: 'CARD_CLICKED',
      common: { invokedFunction: common.invokedFunction, parameters: common.parameters || {} },
    },
    addon: true,
  });
  if (chat.messagePayload) return { event: { ...base, type: 'MESSAGE' }, addon: true };
  if (chat.addedToSpacePayload) return { event: { ...base, type: 'ADDED_TO_SPACE' }, addon: true };
  if (chat.removedFromSpacePayload) return { event: { ...base, type: 'REMOVED_FROM_SPACE' }, addon: true };
  if (chat.buttonClickedPayload) return asClick();
  // スラッシュコマンドは本文つきの MESSAGE として扱う (invokedFunction が同居しても
  // CARD_CLICKED に誤分類しない)。コマンド未定義のアプリでは通常来ない
  if (chat.appCommandPayload) {
    return p.message ? { event: { ...base, type: 'MESSAGE' }, addon: true } : { event: base, addon: true };
  }
  if (common.invokedFunction) return asClick();   // payload 無しでクリック情報のみの防御
  // invokedFunction すら無く parameters だけ届く形もある (function=URL化した場合など)
  if (common.parameters?.method || common.parameters?.__action_method_name__) return asClick();
  return { event: base, addon: true };   // 未知のpayloadは type 無し → handleChatEvent が空応答
}

/**
 * アドオン形式ではカード内ボタンの onClick.action.function にエンドポイントURLが必須
 * (関数名のままだとGoogleクライアント側で「リクエストを処理できません」になり、
 * サーバーに届かない — 実機で確認)。関数名は method パラメータへ退避して URL に差し替える
 */
export function rewriteCardActionsForAddon(node) {
  if (Array.isArray(node)) { node.forEach(rewriteCardActionsForAddon); return node; }
  if (!node || typeof node !== 'object') return node;
  const a = node.onClick?.action;
  if (a && typeof a.function === 'string' && !/^https?:\/\//.test(a.function)) {
    const params = Array.isArray(a.parameters) ? a.parameters : (a.parameters = []);
    // function 名が処理名の正 — 既存 method が食い違っていても上書き (取り違え防止)
    const existing = params.find((p) => p?.key === 'method');
    if (existing) existing.value = a.function;
    else params.push({ key: 'method', value: a.function });
    a.function = CHAT_EVENTS_URL;
  }
  for (const v of Object.values(node)) rewriteCardActionsForAddon(v);
  return node;
}

/** 旧形式の応答 → アドオン形式 (hostAppDataAction) に包む。空応答は空のまま。 */
export function wrapAddonResponse(out) {
  if (!out || (!out.text && !out.cardsV2)) return {};
  const message = {};
  if (out.text) message.text = out.text;
  if (out.cardsV2) message.cardsV2 = rewriteCardActionsForAddon(out.cardsV2);
  const action = out.actionResponse?.type === 'UPDATE_MESSAGE'
    ? { updateMessageAction: { message } }
    : { createMessageAction: { message } };
  return { hostAppDataAction: { chatDataAction: action } };
}

// ─── ルーター ───

const router = Router();

// 認証 (stockBotAuth) は server.js で body parser より前に掛けている
router.post('/chat-events', async (req, res) => {
  const raw = req.body || {};
  const { event, addon } = adaptChatEvent(raw);
  // アドオン形式は chat.user に email が無いことがある → 署名検証済みの userIdToken からのみ補完
  if (addon && !event.user?.email && raw.authorizationEventObject?.userIdToken) {
    const email = await userEmailFromIdToken(raw.authorizationEventObject.userIdToken);
    if (email) event.user = { ...event.user, email };
  }
  const send = (out) => res.json(addon ? wrapAddonResponse(out) : out);
  if (addon) {
    // 新形式は形式差異を後から追えるように1行だけ残す (本文・メール値は出さない)。
    // type 不明時は payload のキー構成も出す (形式差異の一次情報)
    const shape = event.type ? '' : ` chatKeys=${Object.keys(raw.chat || {}).join('+') || '-'} commonKeys=${Object.keys(raw.commonEventObject || {}).join('+') || '-'}`;
    console.log(`[stock-bot] addonイベント type=${event.type || '不明'} email=${event.user?.email ? 'あり' : '無し'} space=${event.space?.name || '不明'}${shape}`);
  }
  // 社内ドメイン限定 (Chat アプリの公開設定と二重の防御)。検索・ボタン操作は発言者メール必須
  // (fail-closed)。ADDED_TO_SPACE 等の管理イベントのみメール無しを許容
  const allowedDomain = (process.env.STOCK_BOT_ALLOWED_DOMAIN || 'b-faith.biz').toLowerCase();
  const email = String(event.user?.email || '').toLowerCase();
  const needsUser = event.type === 'MESSAGE' || event.type === 'CARD_CLICKED';
  if (needsUser && !email) {
    return send({ text: '利用者を確認できなかったため処理できません。' });
  }
  if (email && !email.endsWith(`@${allowedDomain}`)) {
    return send({ text: '社内ユーザーのみ利用できます。' });
  }
  if (!checkRateLimit(event.space?.name || event.user?.name || 'unknown')) {
    return send({ text: '検索が集中しています。1分ほどおいて再送してください。' });
  }
  try {
    return Promise.resolve(handleChatEvent(event, getMirrorDB()))
      .then((out) => send(out))
      .catch((e) => {
        console.error('[stock-bot] 応答生成失敗:', e);
        send({ text: '内部エラーが発生しました。時間をおいて再送してください。' });
      });
  } catch (e) {
    // mirror 未初期化 (boot直後) や想定外の payload でも 500 にしない (Chat側の再送・エラー表示を避ける)
    console.error('[stock-bot] イベント処理失敗:', e);
    return send({ text: '在庫データの準備中か、一時的なエラーです。少し待ってもう一度送ってください。' });
  }
});

export default router;
