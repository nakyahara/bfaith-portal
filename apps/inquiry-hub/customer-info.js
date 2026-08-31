/**
 * inquiry-hub 顧客情報の手入力 (2026-08-31 中原さん要望)
 *
 * 位置づけ:
 *   詳細画面 右上「顧客情報」パネルの 注文番号・商品コード・商品名 を画面から入力して保存する。
 *   メール問い合わせは注文番号が付いて来ないので、スタッフが NE 等で調べた結果を
 *   その問い合わせに残す (次に開いた人が調べ直さなくて済む)。保存ボタンは無く、
 *   入力欄から出た時点で自動保存する (router.js の /customer-info API)。
 *
 * 確定情報ロック (最重要):
 *   楽天/Yahoo! の購入者問い合わせのように、モール同期 (sync/engine.js) が注文番号等を
 *   返して来たものは「確定情報」なので画面から変更できない。
 *   判定は inquiries.manual_fields (JSON配列。手入力した項目名) で行う:
 *     - 値があり、かつ manual_fields に無い項目 = 同期が入れた確定情報 → ロック
 *     - 値が無い項目、または manual_fields にある項目 = 手入力可 (空にも戻せる)
 *   既存データは manual_fields=NULL なので、値が入っている項目はすべてロック扱い
 *   (この機能より前に手入力の経路は無く、値は必ず同期由来のため)。
 *   同期がその項目に値を返した時点で manual_fields から外れ、以後ロックされる
 *   (engine.js の COALESCE 上書きと同じタイミング。manualFieldsAfterSync)。
 *
 * モール選択 (2026-08-31 追加要望「どのモールかも選べて、その注文詳細へ飛べるように」):
 *   手入力の注文番号には「どのモールの注文か」(inquiries.order_mall) を一緒に持ち、
 *   モール+注文番号からそのモールの注文詳細への直リンクを出す (ORDER_MALLS)。
 *   注文番号の形式からモールが分かるときは自動で入れる (楽天/Amazon は形式が固有、Yahoo! は店舗接頭辞)。
 *   同期が注文番号を入れた (ロック) 問い合わせではモール = チャネルそのもので order_mall は使わない。
 *   直リンクの形式が分かっていないモールは管理画面トップを返し、画面側で「番号をコピーして開く」。
 */
import { getDB, logActivity } from './db.js';

/** 手入力できる項目 (キー = inquiries のカラム名) */
export const CUSTOMER_INFO_FIELDS = Object.freeze({
  order_number: { label: '注文番号', max: 100 },
  product_code: { label: '商品コード', max: 100 },
  product_name: { label: '商品名', max: 300 },
});
export const CUSTOMER_INFO_KEYS = Object.freeze(Object.keys(CUSTOMER_INFO_FIELDS));

/**
 * 注文番号のモール (会社概要の出店チャネル順)。
 *   orderUrl : 注文詳細への直リンク。楽天/Yahoo! は中原さん実証済み (2026-08-16)、Amazon は Seller Central の
 *              注文詳細 (orders-v3/order/<注文番号>)。初回クリックで確認して違えばここを直す
 *   adminUrl : 直リンク形式が未確認のモールは管理画面トップ。画面は「注文番号をコピーして開く」導線にする
 *   pattern  : 注文番号の形式からモールを推定するための正規表現 (楽天 6-8-8桁以上 / Amazon 3-7-7桁)
 */
export const ORDER_MALLS = Object.freeze({
  rakuten:  { label: '楽天市場', short: '楽天', pattern: /^\d{6}-\d{8}-\d+$/,
    orderUrl: no => `https://order-rp.rms.rakuten.co.jp/order-rb/individual-order-detail/init?orderNumber=${encodeURIComponent(no)}` },
  yahoo:    { label: 'Yahoo!ショッピング', short: 'Yahoo!',
    orderUrl: (no, ctx) => ctx.yahooAccount
      ? `https://pro.store.yahoo.co.jp/pro.${encodeURIComponent(ctx.yahooAccount)}/order/manage/detail/${encodeURIComponent(no)}` : null },
  amazon:   { label: 'Amazon', short: 'Amazon', pattern: /^\d{3}-\d{7}-\d{7}$/,
    orderUrl: no => `https://sellercentral.amazon.co.jp/orders-v3/order/${encodeURIComponent(no)}` },
  aupay:    { label: 'au PAY マーケット', short: 'au PAY', adminUrl: 'https://manager.wowma.jp/' },
  qoo10:    { label: 'Qoo10', short: 'Qoo10', adminUrl: 'https://qsm.qoo10.jp/' },
  mercari:  { label: 'メルカリShops', short: 'メルカリ', adminUrl: 'https://mercari-shops.com/seller' },
  linegift: { label: 'LINEギフト', short: 'LINEギフト' },
});
export const ORDER_MALL_KEYS = Object.freeze(Object.keys(ORDER_MALLS));

/** アダプター契約 (engine.js の item) 側のキー名との対応 */
const ITEM_KEY_OF = Object.freeze({ order_number: 'orderNumber', product_code: 'productCode', product_name: 'productName' });

/** inquiries.manual_fields (JSON配列 or NULL) → 既知の項目名だけの配列。壊れた値は「手入力なし」扱い */
export function parseManualFields(raw) {
  if (!raw) return [];
  try {
    const arr = JSON.parse(raw);
    return Array.isArray(arr) ? arr.filter(k => CUSTOMER_INFO_KEYS.includes(k)) : [];
  } catch { return []; }
}

/** 配列 → 保存形式 (空なら NULL) */
export function serializeManualFields(keys) {
  const uniq = CUSTOMER_INFO_KEYS.filter(k => keys.includes(k));
  return uniq.length ? JSON.stringify(uniq) : null;
}

/**
 * 同期がアダプター値で上書きする項目を手入力集合から外す (engine.js の既存チケット更新時に呼ぶ)。
 * item が null を返した項目は手入力値が残る (COALESCE で消えないのと対応)
 */
export function manualFieldsAfterSync(raw, item) {
  const keep = parseManualFields(raw).filter(k => item[ITEM_KEY_OF[k]] == null);
  return serializeManualFields(keep);
}

/** 画面/API用: 項目ごとの { value, manual, locked } */
export function customerInfoState(inq) {
  const manual = new Set(parseManualFields(inq.manual_fields));
  const out = {};
  for (const k of CUSTOMER_INFO_KEYS) {
    const value = inq[k] == null || String(inq[k]) === '' ? null : String(inq[k]);
    out[k] = { value, manual: manual.has(k), locked: value != null && !manual.has(k) };
  }
  return out;
}

/** 入力値の正規化。空は NULL。改行・制御文字は不可 (1行の識別子なので)。長すぎは throw */
export function normalizeCustomerInfoValue(key, raw) {
  const meta = CUSTOMER_INFO_FIELDS[key];
  if (!meta) throw new Error('不正な項目です');
  if (raw == null) return null;
  if (typeof raw !== 'string' && typeof raw !== 'number') throw new Error(`${meta.label}は文字列で指定してください`);
  const v = String(raw).replace(/\s+/g, ' ').trim();
  if (!v) return null;
  // eslint-disable-next-line no-control-regex
  if (/[\u0000-\u001f\u007f]/.test(v)) throw new Error(`${meta.label}に使えない文字が含まれています`);
  if (v.length > meta.max) throw new Error(`${meta.label}は${meta.max}文字以内で入力してください`);
  return v;
}

/** モール指定の正規化。空 = 未選択 (NULL)。既知のキー以外は throw */
export function normalizeOrderMall(raw) {
  if (raw == null) return null;
  const v = String(raw).trim();
  if (!v) return null;
  if (!ORDER_MALLS[v]) throw new Error('モールの指定が不正です');
  return v;
}

/** 有効な Yahoo! 店舗のアカウント (ストアクリエイターProのURLと注文番号の接頭辞に使う。通常1件) */
export function listYahooAccounts() {
  return getDB().prepare("SELECT account_identifier FROM shops WHERE channel_type = 'yahoo' AND is_active = 1 ORDER BY id").all()
    .map(r => String(r.account_identifier || '').trim()).filter(Boolean);
}

/** 注文番号の形式からモールを推定 (楽天/Amazon は形式が固有。Yahoo! は店舗アカウントの接頭辞)。分からなければ null */
export function guessOrderMall(orderNumber, { yahooAccounts = [] } = {}) {
  const no = String(orderNumber || '').trim();
  if (!no) return null;
  for (const k of ORDER_MALL_KEYS) if (ORDER_MALLS[k].pattern && ORDER_MALLS[k].pattern.test(no)) return k;
  if (yahooAccounts.some(a => a && no.startsWith(`${a}-`))) return 'yahoo';
  return null;
}

/** チャネルがそのままモールになるもの (同期が注文番号を入れる = 確定情報) */
const channelMallOf = inq => (inq.channel_type === 'rakuten' || inq.channel_type === 'yahoo') ? inq.channel_type : null;

/**
 * 注文リンクの計算 (詳細画面と /customer-info API の両方で使う)。
 *   neOrderNo/neOrderUrl : NE 個別受注明細 (kensaku_denpyo_no)。Yahoo! は NE 側に店舗接頭辞が無いので剥がす
 *   mall/mallLabel       : ロック (同期由来) ならチャネル、手入力なら order_mall (未選択ならチャネルが楽天/Yahoo!のときだけそれ)
 *   mallOrderUrl         : そのモールの注文詳細。Yahoo! の URL は接頭辞付き注文番号が必要なので無ければ付ける
 *   mallAdminUrl         : 直リンク形式が未確認のモールの管理画面トップ (画面側で番号コピー+開く)
 * yahooAccounts はテスト用に注入可 (省略時は shops から引く)
 */
export function orderLinksOf(inq, { yahooAccounts } = {}) {
  const raw = String(inq.order_number || '').trim();
  const locked = customerInfoState(inq).order_number.locked;
  const chMall = channelMallOf(inq);
  const mall = locked ? chMall : (ORDER_MALLS[inq.order_mall] ? inq.order_mall : chMall);
  const meta = mall ? ORDER_MALLS[mall] : null;
  const yahooAccount = mall !== 'yahoo' ? null
    : (inq.channel_type === 'yahoo' && inq.account_identifier ? String(inq.account_identifier).trim()
      : ((yahooAccounts ?? listYahooAccounts())[0] || null));
  const prefix = yahooAccount ? `${yahooAccount}-` : null;
  const neOrderNo = !raw ? null : (prefix && raw.startsWith(prefix) ? raw.slice(prefix.length) : raw);
  const mallNo = !raw ? null : (prefix && !raw.startsWith(prefix) ? prefix + raw : raw);
  const neOrderUrl = neOrderNo
    ? `https://main.next-engine.com/Userjyuchu/jyuchuInp?kensaku_denpyo_no=${encodeURIComponent(neOrderNo)}&jyuchu_meisai_order=jyuchu_meisai_gyo`
    : null;
  const mallOrderUrl = meta && meta.orderUrl && mallNo ? meta.orderUrl(mallNo, { yahooAccount }) : null;
  const mallAdminUrl = meta && !meta.orderUrl && meta.adminUrl && mallNo ? meta.adminUrl : null;
  return {
    orderNumber: raw || null, neOrderNo, neOrderUrl,
    mall, mallLabel: meta ? meta.label : null, mallShort: meta ? meta.short : null,
    mallOrderUrl, mallAdminUrl, mallOrderNo: mallNo,
  };
}

/**
 * 顧客情報を保存する (項目単位。patch に含まれる既知の項目だけ更新)。
 *   - ロック項目に別の値を入れようとしたら throw (e.code = 'LOCKED')。同じ値なら無視
 *   - 値を入れた項目は manual_fields に加え、空にした項目は外す
 *   - order_mall (注文番号のモール): 注文番号がロックなら変更不可。'' = 未選択。
 *     注文番号を入れてモール未選択なら形式から推定して自動で入れる (guessedMall に返す)
 *   - 変更があれば操作ログ customer_info_edit (before/after は変更した項目のみ)
 * 戻り値: { ok, unchanged?, changed: [項目名], guessedMall, order_mall, state }
 */
export function setCustomerInfo(inquiryId, patch, actorId = null) {
  const db = getDB();
  const has = k => Object.prototype.hasOwnProperty.call(patch || {}, k);
  return db.transaction(() => {
    const inq = db.prepare('SELECT id, channel_type, order_number, product_code, product_name, manual_fields, order_mall FROM inquiries WHERE id = ?').get(inquiryId);
    if (!inq) throw new Error('問い合わせが見つかりません');
    const state = customerInfoState(inq);
    const lockedError = msg => { const e = new Error(msg); e.code = 'LOCKED'; return e; };
    const before = {}, after = {};
    const changed = [];
    for (const k of CUSTOMER_INFO_KEYS) {
      if (!has(k)) continue;
      const v = normalizeCustomerInfoValue(k, patch[k]);
      if (v === state[k].value) continue;
      if (state[k].locked) throw lockedError(`${CUSTOMER_INFO_FIELDS[k].label}はモールから取得した確定情報のため変更できません`);
      before[k] = state[k].value;
      after[k] = v;
      changed.push(k);
    }

    // モール選択
    const mallBefore = ORDER_MALLS[inq.order_mall] ? inq.order_mall : null;
    let mallAfter = mallBefore;
    let mallChanged = false;
    if (has('order_mall')) {
      const m = normalizeOrderMall(patch.order_mall);
      if (m !== mallBefore) {
        if (state.order_number.locked) throw lockedError('注文番号のモールは確定情報のため変更できません');
        mallAfter = m; mallChanged = true;
      }
    }
    // 注文番号を入れてモール未選択なら形式から推定
    let guessedMall = null;
    const orderAfter = changed.includes('order_number') ? after.order_number : state.order_number.value;
    // (楽天/Amazon の形式は固有なので、選んであるモールと違う形式の番号が来たら判定し直す。
    //  同じ patch でモールを明示指定していればそちらを優先)
    if (changed.includes('order_number') && orderAfter && !has('order_mall')) {
      const g = guessOrderMall(orderAfter, { yahooAccounts: listYahooAccounts() });
      if (g && g !== mallAfter) { guessedMall = g; mallAfter = g; mallChanged = true; }
    }
    if (mallChanged) { before.order_mall = mallBefore; after.order_mall = mallAfter; changed.push('order_mall'); }
    if (!changed.length) return { ok: true, unchanged: true, changed: [], guessedMall: null, order_mall: mallBefore, state };

    const manual = new Set(parseManualFields(inq.manual_fields));
    for (const k of changed) { if (k === 'order_mall') continue; if (after[k] == null) manual.delete(k); else manual.add(k); }
    const sets = changed.map(k => `${k} = ?`).concat(['manual_fields = ?', "updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now')"]);
    db.prepare(`UPDATE inquiries SET ${sets.join(', ')} WHERE id = ?`)
      .run(...changed.map(k => after[k]), serializeManualFields([...manual]), inquiryId);
    logActivity(inquiryId, { actorType: 'user', userId: actorId, actionType: 'customer_info_edit', before, after });
    const now = db.prepare('SELECT order_number, product_code, product_name, manual_fields, order_mall FROM inquiries WHERE id = ?').get(inquiryId);
    return { ok: true, changed, guessedMall, order_mall: now.order_mall || null, state: customerInfoState(now) };
  }).immediate();
}
