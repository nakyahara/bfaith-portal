/**
 * 楽天 問い合わせ管理API (inquirymng-api / R-Messe) 受信同期アダプター (設計書§2、Step 2)
 * sync/engine.js のアダプター契約 (fetchNew) を実装する。
 *
 * 実測契約 (2026-07-17 Step 0契約テスト + 窓プローブ。契約テストノート参照):
 * - GET /inquiries?fromDate&toDate&limit&page → { totalCount, totalPageCount, page, list: [...] }
 * - fromDate/toDate は 'yyyy-MM-ddTHH:mm:ss' (タイムゾーン表記なし) の **JST解釈**。
 *   レスポンス日時は '+09:00' オフセット付きで返る (Date.parse可)
 * - 窓のフィルタ対象は **regDate (問い合わせ作成日)**。lastUpdateDate では絞れない
 *   → 窓を滑らせるだけでは「古い問い合わせへの顧客の追い返信」を取りこぼす。
 *   一覧の各行に replies[] が全量埋め込まれているため、常に直近 lookbackDays (既定60日) の
 *   regDate 窓を再照合して差分を拾う (UPSERTは外部IDで冪等なので再照合は安全)。
 *   ⚠️ 制約: lookbackDays より古い問い合わせへの追い返信は通常同期では拾えない。
 *   日次で lookbackDays を広げた深掘り実行 (runner の --deep) で補完する
 * - replies[].id は問い合わせ内連番 (0始まり)・再取得で安定 → external_message_id 'r:<id>'
 * - replyFrom: 'merchant' | 'user'
 * - limit=50 は実測OK・101 は 400 (上限未特定のため50固定)
 * - **fromDate〜toDate の範囲は31日以内** (超過は 400 IE001 'date time range should be
 *   within 31 days'。2026-07-17 実測) → 窓を30日スライスに分割して順に取得する
 * - レート制限 1リクエスト/秒 → 1.1秒間隔で呼ぶ
 * - 認証: Authorization: ESA Base64(serviceSecret:licenseKey)
 */

import { SendRejectedError } from '../../outbox.js';

const BASE = 'https://api.rms.rakuten.co.jp/es/1.0/inquirymng-api';
const DAY_MS = 86400000;

export const DEFAULT_LOOKBACK_DAYS = 60;   // 通常同期の再照合幅 (追い返信の取りこぼし対策)
export const DEEP_LOOKBACK_DAYS = 365;     // 深掘り同期 (日次) の再照合幅
const PAGE_LIMIT = 50;
const MAX_RANGE_DAYS = 30;                 // 1クエリの最大期間 (API上限31日の安全側)
const DEFAULT_MAX_PAGES = 60;              // スライスあたり60ページ=3,000件。超過は契約想定外として全体失敗させる

/** UTCのISO/エポックms → 楽天APIが解釈するJST naive文字列 'yyyy-MM-ddTHH:mm:ss' */
export function toJstNaive(input) {
  const ms = typeof input === 'number' ? input : Date.parse(input);
  if (!Number.isFinite(ms)) throw new Error(`toJstNaive: 不正な日時です: ${input}`);
  const d = new Date(ms + 9 * 3600000); // JSTへシフトしてUTCゲッターで読む (ホストTZ非依存)
  const p = n => String(n).padStart(2, '0');
  return `${d.getUTCFullYear()}-${p(d.getUTCMonth() + 1)}-${p(d.getUTCDate())}` +
    `T${p(d.getUTCHours())}:${p(d.getUTCMinutes())}:${p(d.getUTCSeconds())}`;
}

/** 一覧1行 → エンジン契約の inquiry 1件 (エクスポートはテスト用)
 * expectedShopId を渡すと row.shopId と照合し、不一致は contract_violation で全体失敗させる
 * (複数店舗運用や認証設定ミスで他店舗のデータが混入するのを防ぐ) */
export function mapInquiry(row, expectedShopId = null) {
  if (!row || typeof row.inquiryNumber !== 'string' || !row.inquiryNumber) {
    const e = new Error('楽天一覧レスポンス契約違反: inquiryNumber がありません');
    e.errorType = 'contract_violation';
    throw e;
  }
  if (expectedShopId != null && String(row.shopId) !== String(expectedShopId)) {
    const e = new Error(`楽天一覧レスポンス契約違反: shopId不一致 (期待=${expectedShopId} 実際=${row.shopId}, ${row.inquiryNumber})`);
    e.errorType = 'contract_violation';
    throw e;
  }
  const mapAttachment = a => ({
    // path はファイル取得キーで一意 (契約テスト§2.4)。無い場合はエンジンの synthetic 採番に委ねる
    externalAttachmentId: a?.path || undefined,
    fileName: a?.label ?? null,
  });
  const messages = [{
    // 最初の質問 (問い合わせ作成メッセージ)。問い合わせ内で一意な固定ID
    externalMessageId: 'q',
    senderType: 'customer',
    senderName: row.userName ?? null,
    bodyText: row.message ?? null,
    isIncoming: 1,
    sentAt: row.regDate, receivedAt: row.regDate,
    attachments: (row.attachments || []).map(mapAttachment),
  }];
  for (const rep of row.replies || []) {
    // 実測契約は 'merchant' | 'user' の2値。未知値を顧客扱いすると誤って未読化・再オープンさせるため
    // 契約違反として全体失敗させる (Codex R1 medium)
    const replyFrom = String(rep.replyFrom || '').toLowerCase();
    if (replyFrom !== 'merchant' && replyFrom !== 'user') {
      const e = new Error(`楽天一覧レスポンス契約違反: 未知のreplyFrom '${rep.replyFrom}' (${row.inquiryNumber} reply id=${rep.id})`);
      e.errorType = 'contract_violation';
      throw e;
    }
    const fromMerchant = replyFrom === 'merchant';
    messages.push({
      // id は問い合わせ内連番 (0始まり)・安定 (実測)。欠落時はエンジンの synthetic 採番へ
      externalMessageId: rep.id == null ? undefined : `r:${rep.id}`,
      senderType: fromMerchant ? 'shop' : 'customer',
      senderName: fromMerchant ? null : (row.userName ?? null),
      bodyText: rep.message ?? null,
      isIncoming: fromMerchant ? 0 : 1,
      sentAt: rep.regDate, receivedAt: rep.regDate,
      attachments: (rep.attachments || []).map(mapAttachment),
    });
  }
  return {
    externalInquiryId: row.inquiryNumber,
    customerName: row.userName ?? null,
    customerIdentifier: row.userMaskEmail ?? null,
    subject: row.category
      ? (row.type ? `${row.category}（${row.type}）` : row.category)
      : (row.type ?? null),
    orderNumber: row.orderNumber ?? null,
    productCode: row.itemNumber ?? null,
    productName: row.itemName ?? null,
    externalStatus: row.isCompleted == null ? null : (row.isCompleted ? 'completed' : 'open'),
    externalIsRead: row.readByMerchant == null ? null : !!row.readByMerchant,
    receivedAt: row.regDate,
    messages,
  };
}

/**
 * env から transport 設定を解決する (ランナー/cron 共用)。
 * 楽天キーがあれば direct、なければ miniPC passthrough (warehouse)、どちらも無ければ null。
 */
export function resolveRakutenTransportFromEnv(env = process.env) {
  if (env.RAKUTEN_SERVICE_SECRET && env.RAKUTEN_LICENSE_KEY) {
    return { transport: 'direct', serviceSecret: env.RAKUTEN_SERVICE_SECRET, licenseKey: env.RAKUTEN_LICENSE_KEY };
  }
  if (env.WAREHOUSE_URL && env.WAREHOUSE_SERVICE_TOKEN && env.CF_ACCESS_CLIENT_ID && env.CF_ACCESS_CLIENT_SECRET) {
    return {
      transport: 'warehouse', warehouseUrl: env.WAREHOUSE_URL, serviceToken: env.WAREHOUSE_SERVICE_TOKEN,
      cfClientId: env.CF_ACCESS_CLIENT_ID, cfClientSecret: env.CF_ACCESS_CLIENT_SECRET,
    };
  }
  return null;
}

/**
 * @param {object} cfg
 *   transport?: 'direct' (既定) | 'warehouse'
 *     direct    — RMS を直接叩く。serviceSecret / licenseKey 必須 (miniPC上での実行・契約テスト用)
 *     warehouse — miniPC bfaith-portal の /service-api/rakuten-rms/inquiries passthrough を
 *                 Cloudflare Tunnel 経由で叩く (Render用。設計原則: Renderに楽天キーを置かない)。
 *                 warehouseUrl / serviceToken / cfClientId / cfClientSecret 必須
 *   expectedShopId?: レスポンス row.shopId との照合値 (店舗混入防止。運用では必須推奨)
 *   lookbackDays?: regDate 再照合幅 (既定60。--deep 時は365等を渡す)
 *   requestTimeoutMs?: リクエスト単位のタイムアウト (既定90秒。miniPC側の直列キュー待ちを見込む)
 *   fetchImpl?: fetch 差し替え (テスト用)
 *   sleepMs?: リクエスト間隔 (既定1100。テストでは0)
 *   maxPages?: ページ数上限 (超過は throw。既定60)
 *   sendMode?: 'dryrun' (既定) | 'live' — 返信送信 (sendReply)。dryrunは検証とログのみで実送信しない
 */
export function createRakutenAdapter(cfg = {}) {
  const { serviceSecret, licenseKey, expectedShopId = null } = cfg;
  const transport = cfg.transport ?? 'direct';
  let listUrlBase, buildHeaders;
  if (transport === 'direct') {
    if (!serviceSecret || !licenseKey) {
      throw new Error('createRakutenAdapter: transport=direct には serviceSecret / licenseKey が必須です (env RAKUTEN_SERVICE_SECRET / RAKUTEN_LICENSE_KEY)');
    }
    const auth = 'ESA ' + Buffer.from(`${serviceSecret}:${licenseKey}`).toString('base64');
    listUrlBase = `${BASE}/inquiries`;
    buildHeaders = () => ({ Authorization: auth });
  } else if (transport === 'warehouse') {
    const { warehouseUrl, serviceToken, cfClientId, cfClientSecret } = cfg;
    if (!warehouseUrl || !serviceToken || !cfClientId || !cfClientSecret) {
      throw new Error('createRakutenAdapter: transport=warehouse には warehouseUrl / serviceToken / cfClientId / cfClientSecret が必須です (env WAREHOUSE_URL / WAREHOUSE_SERVICE_TOKEN / CF_ACCESS_CLIENT_ID / CF_ACCESS_CLIENT_SECRET)');
    }
    listUrlBase = `${warehouseUrl.replace(/\/+$/, '')}/service-api/rakuten-rms/inquiries`;
    buildHeaders = () => ({
      'CF-Access-Client-Id': cfClientId,
      'CF-Access-Client-Secret': cfClientSecret,
      Authorization: `Bearer ${serviceToken}`,
    });
  } else {
    throw new Error(`createRakutenAdapter: 未知のtransport '${transport}'`);
  }
  const lookbackDays = cfg.lookbackDays ?? DEFAULT_LOOKBACK_DAYS;
  const fetchImpl = cfg.fetchImpl ?? fetch;
  const sleepMs = cfg.sleepMs ?? 1100;
  const maxPages = cfg.maxPages ?? DEFAULT_MAX_PAGES;
  // warehouse 経由は miniPC 側 rakutenRequest の直列キュー+リトライ (最大4試行) を挟むため長めに取る
  const requestTimeoutMs = cfg.requestTimeoutMs ?? 90000;
  const sleep = ms => new Promise(r => setTimeout(r, ms));

  // エラー本文は構造化フィールドだけをログに残す (問い合わせ本文等の露出防止。Codex R1)。
  // RMS直: {error:{code,message,targets}} / warehouse passthrough失敗: {ok,error,message} の両形に対応
  const errorHead = async res => {
    const text = await res.text().catch(() => '');
    try {
      const j = JSON.parse(text);
      return JSON.stringify({
        code: j?.error?.code ?? (typeof j?.error === 'string' ? j.error : undefined),
        message: j?.error?.message ?? j?.message,
        targets: j?.error?.targets,
      }).slice(0, 300);
    } catch { return text.slice(0, 120); }
  };

  async function getListPage(fromDate, toDate, page) {
    const url = `${listUrlBase}?fromDate=${encodeURIComponent(fromDate)}&toDate=${encodeURIComponent(toDate)}&limit=${PAGE_LIMIT}&page=${page}`;
    let res;
    try {
      res = await fetchImpl(url, { headers: buildHeaders(), signal: AbortSignal.timeout(requestTimeoutMs) });
    } catch (err) {
      const timedOut = err?.name === 'TimeoutError' || err?.name === 'AbortError';
      const e = new Error(timedOut
        ? `楽天API タイムアウト (${requestTimeoutMs}ms, page=${page})`
        : `楽天API接続失敗: ${err?.message || err}`);
      e.errorType = 'fetch_failed';
      throw e;
    }
    if (res.status !== 200) {
      const e = new Error(`楽天API HTTP ${res.status} (page=${page}): ${await errorHead(res)}`);
      e.errorType = res.status === 401 || res.status === 403 ? 'auth'
        : res.status === 429 ? 'rate_limited' : 'fetch_failed';
      throw e;
    }
    const j = await res.json();
    if (!Array.isArray(j.list) || !Number.isFinite(j.totalPageCount)) {
      const e = new Error(`楽天一覧レスポンス契約違反 (page=${page}): list/totalPageCount が想定形でありません`);
      e.errorType = 'contract_violation';
      throw e;
    }
    return j;
  }

  const sendMode = cfg.sendMode ?? 'dryrun';
  // 送信URL: warehouse=miniPC passthrough (自動リトライ無効化済み) / direct=RMS直
  const replyUrl = transport === 'warehouse'
    ? `${cfg.warehouseUrl.replace(/\/+$/, '')}/service-api/rakuten-rms/inquiry-reply`
    : `${BASE}/inquiry/reply`;

  return {
    channelType: 'rakuten',
    isLive: sendMode === 'live',

    /**
     * 返信送信 (outbox.js §8.3 契約)。未送信確定の失敗のみ SendRejectedError。
     * 送信要求後の失敗 (5xx/timeout/network) は汎用エラー → unknown (自動再送しない。
     * miniPC passthrough 側も maxAttempts:1 でリトライ無効化済み)。
     * 引数 dryRun: true はアダプター設定に関係なく実送信を禁止 (プレビュー経路用)
     */
    async sendReply({ inquiry, bodyText, dryRun = false }) {
      const inquiryNumber = String(inquiry?.external_inquiry_id || '').trim();
      if (!/^\d{1,10}-\d{8}-\d{1,12}[a-z]?$/i.test(inquiryNumber)) {
        throw new SendRejectedError(`楽天問い合わせ番号が不正です (external_inquiry_id='${inquiryNumber}')`);
      }
      const message = String(bodyText || '');
      if (!message.trim()) throw new SendRejectedError('本文が空です');
      if (message.length > 10000) throw new SendRejectedError('本文が長すぎます (10000文字まで)');

      if (dryRun || sendMode !== 'live') {
        console.log(`[rakuten-send DRYRUN] inquiry=${inquiryNumber} bytes=${message.length} (実送信していません)`);
        return { dryRun: true, externalReplyId: `dryrun:${inquiryNumber}` };
      }

      let res;
      try {
        res = await fetchImpl(replyUrl, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', ...buildHeaders() },
          body: JSON.stringify({ inquiryNumber, message }),
          signal: AbortSignal.timeout(requestTimeoutMs),
        });
      } catch (err) {
        const timedOut = err?.name === 'TimeoutError' || err?.name === 'AbortError';
        throw new Error(timedOut
          ? `楽天返信送信タイムアウト (${requestTimeoutMs}ms) — 結果不明`
          : `楽天返信送信の接続失敗: ${err?.message || err}`);
      }
      const text = await res.text().catch(() => '');
      let data = null;
      try { data = JSON.parse(text); } catch { /* 非JSONはdata=nullのまま */ }
      if (res.status >= 400 && res.status < 500) {
        // 4xx = 受理されなかった (未送信確定)。エラー本文は構造化フィールドのみ (本文露出防止)
        throw new SendRejectedError(`楽天返信が拒否されました (HTTP ${res.status}): ${JSON.stringify({
          code: data?.error?.code ?? (typeof data?.error === 'string' ? data.error : undefined),
          message: data?.error?.message ?? data?.message,
        }).slice(0, 300)}`);
      }
      if (res.status >= 500) throw new Error(`楽天返信送信 HTTP ${res.status} — 結果不明`);
      // 成功。ReplyResult の返信IDの形は実返信テストで確定させる (無ければ outbox が op:<operation_id> を使う)
      const replyId = data?.result?.id ?? data?.result?.replyId ?? data?.replyId ?? data?.id;
      console.log(`[rakuten-send] 送信完了 inquiry=${inquiryNumber} replyId=${replyId ?? '(レスポンスに無し)'}`);
      return { externalReplyId: replyId != null ? `r:${replyId}` : undefined };
    },

    /** sync/engine.js 契約。部分成功は返さない (途中失敗は全体 throw) */
    async fetchNew({ sinceIso, untilIso }) {
      const untilMs = Date.parse(untilIso);
      const sinceMs = Date.parse(sinceIso);
      if (!Number.isFinite(untilMs) || !Number.isFinite(sinceMs)) {
        throw new Error(`fetchNew: 不正なウィンドウです since=${sinceIso} until=${untilIso}`);
      }
      // regDate フィルタ制約への対応: エンジンの since より広い lookback 窓で常に再照合する
      const effectiveSinceMs = Math.min(sinceMs, untilMs - lookbackDays * DAY_MS);

      // API上限 (31日以内) に合わせて窓を30日スライスに分割 (境界の同時刻は両スライスに載り得るが冪等)
      const slices = [];
      for (let cur = effectiveSinceMs; cur < untilMs;) {
        const end = Math.min(cur + MAX_RANGE_DAYS * DAY_MS, untilMs);
        slices.push([cur, end]);
        cur = end;
      }

      const byId = new Map(); // スライス跨ぎ・ページ跨ぎの重複排除 (後着を採用。UPSERTなのでどちらでも安全)
      let requests = 0;
      for (const [fromMs, toMs] of slices) {
        const fromDate = toJstNaive(fromMs);
        const toDate = toJstNaive(toMs);
        let page = 1;
        for (;;) {
          if (requests > 0) await sleep(sleepMs);
          requests++;
          const j = await getListPage(fromDate, toDate, page);
          for (const row of j.list) byId.set(row.inquiryNumber, row);
          if (page >= j.totalPageCount) break;
          page++;
          if (page > maxPages) {
            const e = new Error(`楽天一覧が ${maxPages} ページを超過 (total=${j.totalCount}, ${fromDate}〜${toDate})。バックフィル幅を分割してください`);
            e.errorType = 'window_too_large';
            throw e;
          }
        }
      }
      return {
        inquiries: [...byId.values()].map(row => mapInquiry(row, expectedShopId)),
        // observedUntil 省略 = untilIso まで完全列挙 (regDate基準の全スライス・全ページ取得済)。
        // lookback より古い問い合わせへの追い返信はこの完全性保証の対象外 (--deep で補完。
        // ⚠️ --deep の幅 (既定365日) より古い問い合わせへの追い返信は拾えない。楽天側の保存期間が
        // それを超えるなら --lookback-days で広げるか、巡回バックフィルを別途設ける)
      };
    },
  };
}
