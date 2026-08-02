/**
 * Yahoo!ショッピング 問い合わせ管理API (externalTalk*) 受信同期アダプター (設計書§3、Step 2)
 * sync/engine.js のアダプター契約 (fetchNew) を実装する。
 *
 * 経路: さくらVPSプロキシ (vps-proxy/aupay-proxy.js) の passthrough を経由する。
 *   - Yahoo!システムAPIは登録済み固定IPからしか呼べない (px-04306。Step 0実測: オフィスIP=403、VPS=200)
 *   - Bearer token + 公開鍵署名 (px-04102対策) はVPS側で付与される
 *   - 認証は X-Proxy-Secret のみ (env YAHOO_PROXY_URL / YAHOO_PROXY_SECRET)
 *
 * 実測契約 (2026-07-17 Step 0契約テスト + 保存レスポンス解析):
 * - externalTalkList: {summary:{filter, unansweredCount, topic:{start,end,count}}, headlines:[...]}
 *   1ページ最大20件。**userPostTime (顧客の最終投稿日時・UNIX秒) の降順ソート**
 *   → 顧客の追い返信は先頭に浮上する。ただし店舗側だけの返信 (Yahoo!管理画面から) は浮上しない
 *   → 一覧を lookback 幅 (既定14日) までスキャンし、その中で
 *     max(userPostTime, sellerPostTime) が since 以降のトピックだけ詳細取得する
 *   ⚠️ 制約: lookback より古い userPostTime のトピックへの店舗側更新は通常同期では拾えない
 *     (deep 日次=365日で補完。顧客側の新着は必ず浮上するため取りこぼさない)
 * - externalTalkDetail: {topic:{...}, messages:[{messageId(1始まり連番・安定), postUserType,
 *   postdate(UNIX秒の文字列), body, fileList}]}
 * - postUserType: 'user' | 'seller' (未知値は contract_violation で全体失敗)
 * - レート制限 1リクエスト/秒 → 1.1秒間隔
 */

import { SendRejectedError } from '../../outbox.js';
import { contentTypeFromExt } from '../../mime.js';
import { readBodyWithLimit } from '../../read-limited.js';

const DAY_MS = 86400000;
const SEND_BODY_MAX = 2000; // externalTalkAdd の本文上限 (公式仕様)

export const DEFAULT_LIST_LOOKBACK_DAYS = 14;  // 通常同期の一覧スキャン幅 (店舗側更新の取りこぼし対策)
export const DEEP_LIST_LOOKBACK_DAYS = 365;    // 深掘り同期 (日次) のスキャン幅
const PAGE_SIZE = 20;                          // externalTalkList の1ページ上限 (実測・仕様)
const DEFAULT_MAX_PAGES = 150;                 // 150ページ=3,000件 (全2,432件を上回る)。超過は全体失敗

/** UNIX秒 (number|string) → エポックms。不正は null */
export function unixSecToMs(v) {
  const n = Number(v);
  return Number.isFinite(n) && n > 0 ? n * 1000 : null;
}

/** 詳細レスポンス → エンジン契約の inquiry 1件 (エクスポートはテスト用)
 * headline は一覧行 (isUnread 等の補完に使う。null可) */
export function mapTopicDetail(topicId, detail, headline = null) {
  const topic = detail?.topic;
  if (!topic || !Array.isArray(detail.messages)) {
    const e = new Error(`Yahoo詳細レスポンス契約違反: topic/messages が想定形でありません (${topicId})`);
    e.errorType = 'contract_violation';
    throw e;
  }
  const messages = detail.messages.map(m => {
    const t = String(m.postUserType || '').toLowerCase();
    if (t !== 'user' && t !== 'seller') {
      const e = new Error(`Yahoo詳細レスポンス契約違反: 未知のpostUserType '${m.postUserType}' (${topicId} messageId=${m.messageId})`);
      e.errorType = 'contract_violation';
      throw e;
    }
    const fromUser = t === 'user';
    const atMs = unixSecToMs(m.postdate);
    return {
      // messageId はトピック内連番 (1始まり)・再取得安定 (Step 0実測)。欠落時はエンジンのsynthetic採番へ
      externalMessageId: m.messageId == null ? undefined : `m:${m.messageId}`,
      senderType: fromUser ? 'customer' : 'shop',
      senderName: null,
      bodyText: m.body ?? null,
      isIncoming: fromUser ? 1 : 0,
      sentAt: atMs, receivedAt: atMs,
      // 公式仕様 (質問詳細API): fileList[] = { fileName, objectKey, fileExt, thumbnailUrl, fileSize }。
      // objectKey がファイル取得API (externalTalkFileDownload) のキーなので外部IDに使う
      // (旧実装は fileId/id を見ていて objectKey を捨てていた → 添付を取得できなかった)
      attachments: (m.fileList || []).map(f => ({
        externalAttachmentId: f?.objectKey ?? f?.fileId ?? f?.id ?? undefined,
        fileName: f?.fileName ?? f?.name ?? null,
        contentType: contentTypeFromExt(f?.fileExt),
        fileSize: Number.isFinite(Number(f?.fileSize)) ? Number(f.fileSize) : null,
      })),
    };
  });
  // 受付日時 = 最初のメッセージの投稿日時
  const firstAt = messages.map(m => m.sentAt).filter(Boolean).sort((a, b) => a - b)[0] ?? null;
  if (!firstAt) {
    const e = new Error(`Yahoo詳細レスポンス契約違反: postdate を1件も解釈できません (${topicId})`);
    e.errorType = 'contract_violation';
    throw e;
  }
  return {
    externalInquiryId: topicId,
    customerName: null,
    customerIdentifier: topic.userMaskedIdx ?? headline?.userMaskedId ?? null,
    subject: topic.title ?? headline?.title ?? null,
    orderNumber: topic.orderid ?? headline?.orderId ?? null,
    productCode: topic.itemcode ?? headline?.itemCode ?? null,
    productName: null,
    externalStatus: topic.isComplete == null ? null : (topic.isComplete ? 'completed' : 'open'),
    // isSellerUnRead = 店舗側の未読 (モール側の既読状態。表示専用)
    externalIsRead: topic.isSellerUnRead == null ? null : !topic.isSellerUnRead,
    receivedAt: firstAt,
    messages,
  };
}

/**
 * @param {object} cfg
 *   proxyUrl, proxySecret: VPSプロキシ (必須。env YAHOO_PROXY_URL / YAHOO_PROXY_SECRET)
 *   listLookbackDays?: 一覧スキャン幅 (既定14。deep時は365等)
 *   requestTimeoutMs?: リクエスト単位タイムアウト (既定45秒。VPS側の上流30秒+余裕)
 *   fetchImpl?: fetch 差し替え (テスト用)
 *   sleepMs?: リクエスト間隔 (既定1100。テストでは0)
 *   maxPages?: 一覧ページ数上限 (既定150)
 */
export function createYahooAdapter(cfg = {}) {
  const { proxyUrl, proxySecret } = cfg;
  if (!proxyUrl || !proxySecret) {
    throw new Error('createYahooAdapter: proxyUrl / proxySecret は必須です (env YAHOO_PROXY_URL / YAHOO_PROXY_SECRET)');
  }
  const base = proxyUrl.replace(/\/+$/, '');
  const listLookbackDays = cfg.listLookbackDays ?? DEFAULT_LIST_LOOKBACK_DAYS;
  const fetchImpl = cfg.fetchImpl ?? fetch;
  const sleepMs = cfg.sleepMs ?? 1100;
  const maxPages = cfg.maxPages ?? DEFAULT_MAX_PAGES;
  const requestTimeoutMs = cfg.requestTimeoutMs ?? 45000;
  const sendMode = cfg.sendMode ?? 'dryrun';
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  let requests = 0;

  async function proxyGet(path, params, label) {
    if (requests > 0) await sleep(sleepMs);
    requests++;
    const u = new URL(`${base}${path}`);
    for (const [k, v] of Object.entries(params)) u.searchParams.set(k, String(v));
    let res;
    try {
      res = await fetchImpl(u.toString(), {
        headers: { 'X-Proxy-Secret': proxySecret },
        signal: AbortSignal.timeout(requestTimeoutMs),
      });
    } catch (err) {
      const timedOut = err?.name === 'TimeoutError' || err?.name === 'AbortError';
      const e = new Error(timedOut
        ? `Yahooプロキシ タイムアウト (${requestTimeoutMs}ms, ${label})`
        : `Yahooプロキシ接続失敗 (${label}): ${err?.message || err}`);
      e.errorType = 'fetch_failed';
      throw e;
    }
    const text = await res.text();
    if (res.status !== 200) {
      // Yahoo!のエラーはXML {<Error><Message/><Code/>}。コード部分だけ抜く (本文露出防止)
      const code = (text.match(/<Code>([^<]{1,40})<\/Code>/) || [])[1];
      const e = new Error(`Yahoo API HTTP ${res.status} (${label})${code ? ` code=${code}` : ''}`);
      e.errorType = res.status === 401 || res.status === 403 ? 'auth'
        : res.status === 429 ? 'rate_limited' : 'fetch_failed';
      throw e;
    }
    try { return JSON.parse(text); }
    catch {
      // 本文は含めない (問い合わせ内容等の露出防止。長さと先頭1文字の種別だけ)
      const e = new Error(`Yahoo APIレスポンスがJSONでありません (${label}): ${text.length} bytes, startsWith='${text.slice(0, 1)}'`);
      e.errorType = 'contract_violation';
      throw e;
    }
  }

  return {
    channelType: 'yahoo',
    isLive: sendMode === 'live',

    /**
     * 返信送信 (outbox.js §8.3 契約)。VPSプロキシの /yahoo/externalTalkAdd passthrough を叩く
     * (Bearer+公開鍵署名はVPS側付与、プロキシ側もリトライしない=二重投稿防止)。
     * 未送信確定 (topicId不正/本文空・2000字超/4xx) のみ SendRejectedError、
     * 5xx/timeout/network は汎用エラー → unknown (自動再送しない)。
     * 成功時は externalReplyId='m:<messageid>' — 受信同期のメッセージID体系と揃えて
     * 後続の同期で同じメッセージが来ても二重表示にならない。
     * 引数 dryRun: true はアダプター設定に関係なく実送信を禁止 (プレビュー経路用)
     */
    async sendReply({ inquiry, bodyText, dryRun = false }) {
      const topicId = String(inquiry?.external_inquiry_id || '').trim();
      if (!/^[A-Za-z0-9_-]{1,128}$/.test(topicId)) {
        throw new SendRejectedError(`Yahoo!トピックIDが不正です (external_inquiry_id='${topicId}')`);
      }
      const message = String(bodyText || '');
      if (!message.trim()) throw new SendRejectedError('本文が空です');
      if (message.length > SEND_BODY_MAX) {
        throw new SendRejectedError(`本文が長すぎます (Yahoo!の上限${SEND_BODY_MAX}文字。現在${message.length}文字 — 短くしてください)`);
      }

      if (dryRun || sendMode !== 'live') {
        console.log(`[yahoo-send DRYRUN] topic=${topicId.slice(0, 12)}… bytes=${message.length} (実送信していません)`);
        return { dryRun: true, externalReplyId: `dryrun:${topicId}` };
      }

      let res;
      try {
        res = await fetchImpl(`${base}/yahoo/externalTalkAdd`, {
          method: 'POST',
          headers: { 'X-Proxy-Secret': proxySecret, 'Content-Type': 'application/json' },
          body: JSON.stringify({ topicId, message }),
          signal: AbortSignal.timeout(requestTimeoutMs),
        });
      } catch (err) {
        const timedOut = err?.name === 'TimeoutError' || err?.name === 'AbortError';
        throw new Error(timedOut
          ? `Yahoo!返信送信タイムアウト (${requestTimeoutMs}ms) — 結果不明`
          : `Yahoo!返信送信の接続失敗: ${err?.message || err}`);
      }
      const text = await res.text().catch(() => '');
      if (res.status >= 400 && res.status < 500) {
        // 4xx = 受理されなかった (未送信確定)。エラーコードのみ抽出 (本文露出防止)
        const code = (text.match(/<Code>([^<]{1,40})<\/Code>/) || [])[1];
        let reason;
        try { reason = JSON.parse(text)?.error?.reason ?? JSON.parse(text)?.error; } catch { /* XML等 */ }
        throw new SendRejectedError(`Yahoo!返信が拒否されました (HTTP ${res.status})${code ? ` code=${code}` : ''}${reason ? ` reason=${String(reason).slice(0, 120)}` : ''}`);
      }
      if (res.status >= 500) throw new Error(`Yahoo!返信送信 HTTP ${res.status} — 結果不明`);
      // 2xx でも messageid が取れなければ「結果不明」扱い (unknown→人手照合)。
      // externalReplyId 無しで sent 確定させると outbox が op:<操作ID> で記録し、次回同期の
      // m:<messageid> と二重表示になる (Codexレビュー反映)。投稿された可能性は残るため
      // SendRejectedError にはしない
      let data = null;
      try { data = JSON.parse(text); } catch { /* 下でエラー化 */ }
      const messageId = data?.messageid ?? data?.messageId;
      if (messageId == null || messageId === '') {
        throw new Error(`Yahoo!返信送信はHTTP ${res.status}だが messageid を取得できず — 結果不明 (投稿済みの可能性あり。Yahoo!管理画面で確認して解決してください)`);
      }
      console.log(`[yahoo-send] 送信完了 topic=${topicId.slice(0, 12)}… messageid=${messageId}`);
      return { externalReplyId: `m:${messageId}` };
    },

    /**
     * 添付の実体取得 (画面の添付表示用。attachments.js から呼ばれる)。
     * VPSプロキシの /yahoo/externalTalkFile passthrough (公式: ファイル取得API
     * externalTalkFileDownload?key=<objectKey>) を叩き、投稿時の Content-Type のまま受け取る。
     * @returns {{ buffer: Buffer, contentType: string|null, fileName: string|null }}
     */
    async fetchAttachment({ externalAttachmentId, fileName, maxBytes = null }) {
      const key = String(externalAttachmentId || '');
      // syn: = 同期時に objectKey が取れず決定的IDで代替したもの (取得キーが無い)。
      // objectKey 対応前に取り込んだ添付が該当し、再同期 (deep) で実キーに更新される
      if (!key || key.startsWith('syn:')) {
        throw new Error('この添付にはYahoo!側の取得キー (objectKey) がありません (再同期が必要です)');
      }
      const u = new URL(`${base}/yahoo/externalTalkFile`);
      u.searchParams.set('key', key);
      let res;
      try {
        res = await fetchImpl(u.toString(), {
          headers: { 'X-Proxy-Secret': proxySecret },
          signal: AbortSignal.timeout(requestTimeoutMs),
        });
      } catch (err) {
        const timedOut = err?.name === 'TimeoutError' || err?.name === 'AbortError';
        throw new Error(timedOut ? `Yahoo添付取得 タイムアウト (${requestTimeoutMs}ms)` : `Yahoo添付取得の接続失敗: ${err?.message || err}`);
      }
      if (res.status !== 200) throw new Error(`Yahoo添付取得 HTTP ${res.status}`);
      // Content-Length は信用せず、読みながら実バイト数で打ち切る (Codexレビュー High-1)
      const buffer = await readBodyWithLimit(res, maxBytes, '添付');
      return { buffer, contentType: res.headers.get('content-type') || null, fileName: fileName || null };
    },

    /**
     * 認証状態の取得 (任意メソッド。cron/手動同期が shops.auth_expires_at の自動更新に使う)。
     * VPSプロキシの /yahoo/health が refresh token の失効日時を返す
     * (YConnect refresh token は28日で失効し月次再認可が必要 → 期限を画面で見える化する)
     */
    async getAuthStatus() {
      const j = await proxyGet('/yahoo/health', {}, 'health');
      return {
        ok: !!j?.hasTokens,
        authExpiresAt: j?.refreshTokenExpiresAt || null,
      };
    },

    /** sync/engine.js 契約。部分成功は返さない (途中失敗は全体 throw) */
    async fetchNew({ sinceIso, untilIso }) {
      requests = 0;
      const untilMs = Date.parse(untilIso);
      const sinceMs = Date.parse(sinceIso);
      if (!Number.isFinite(untilMs) || !Number.isFinite(sinceMs)) {
        throw new Error(`fetchNew: 不正なウィンドウです since=${sinceIso} until=${untilIso}`);
      }
      const floorSec = Math.floor((untilMs - listLookbackDays * DAY_MS) / 1000);
      const changedSinceSec = Math.floor(Math.min(sinceMs, untilMs) / 1000);

      // 1. 一覧スキャン: userPostTime 降順を前提に、floorSec より古くなったら打ち切る
      const candidates = [];
      let start = 1;
      for (let page = 1; ; page++) {
        if (page > maxPages) {
          const e = new Error(`Yahoo一覧が ${maxPages} ページを超過しました。listLookbackDays を見直してください`);
          e.errorType = 'window_too_large';
          throw e;
        }
        const j = await proxyGet('/yahoo/externalTalkList', { start, result: PAGE_SIZE }, `list start=${start}`);
        const headlines = j?.headlines;
        const total = j?.summary?.topic?.count;
        if (!Array.isArray(headlines) || !Number.isFinite(total)) {
          const e = new Error(`Yahoo一覧レスポンス契約違反 (start=${start}): headlines/summary.topic.count が想定形でありません`);
          e.errorType = 'contract_violation';
          throw e;
        }
        let reachedFloor = false;
        for (const h of headlines) {
          if (!h?.topicId) {
            const e = new Error(`Yahoo一覧レスポンス契約違反 (start=${start}): topicId がありません`);
            e.errorType = 'contract_violation';
            throw e;
          }
          // userPostTime は走査打ち切りの基準になるため厳格に検証する
          // (不正値を0扱いにすると即打ち切り→取りこぼしを「成功」としてコミットしてしまう。Codex R1 high)
          const userSec = Number(h.userPostTime);
          if (!Number.isFinite(userSec) || userSec <= 0) {
            const e = new Error(`Yahoo一覧レスポンス契約違反 (start=${start}, ${h.topicId}): userPostTime が不正です (${h.userPostTime})`);
            e.errorType = 'contract_violation';
            throw e;
          }
          let sellerSec = 0;
          if (h.sellerPostTime != null) {
            sellerSec = Number(h.sellerPostTime);
            if (!Number.isFinite(sellerSec) || sellerSec <= 0) {
              const e = new Error(`Yahoo一覧レスポンス契約違反 (start=${start}, ${h.topicId}): sellerPostTime が不正です (${h.sellerPostTime})`);
              e.errorType = 'contract_violation';
              throw e;
            }
          }
          if (userSec < floorSec) { reachedFloor = true; break; }
          if (Math.max(userSec, sellerSec) >= changedSinceSec) candidates.push(h);
        }
        if (reachedFloor || start + headlines.length > total || headlines.length === 0) break;
        start += headlines.length;
      }

      // 2. 変化があったトピックだけ詳細取得 (メッセージ全量はここでしか取れない)
      const inquiries = [];
      for (const h of candidates) {
        const d = await proxyGet('/yahoo/externalTalkDetail', { topicId: h.topicId }, `detail ${String(h.topicId).slice(0, 12)}…`);
        inquiries.push(mapTopicDetail(h.topicId, d, h));
      }
      return {
        inquiries,
        // observedUntil 省略 = untilIso まで完全列挙。ただし lookback より古い userPostTime の
        // トピックへの店舗側更新はこの保証の対象外 (deep で補完。顧客新着は必ず浮上するため対象内)
      };
    },
  };
}

/** env から Yahoo transport 設定を解決 (ランナー/cron共用)。揃っていなければ null */
export function resolveYahooTransportFromEnv(env = process.env) {
  if (env.YAHOO_PROXY_URL && env.YAHOO_PROXY_SECRET) {
    return { proxyUrl: env.YAHOO_PROXY_URL, proxySecret: env.YAHOO_PROXY_SECRET };
  }
  return null;
}
