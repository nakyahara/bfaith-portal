/**
 * inquiry-hub 添付ファイルの実体取得 (2026-08-02 スタッフ要望「添付された写真や画像を確認したい」)
 *
 * 方針:
 *   - DBには保存しない (オンデマンドで外部から取り、その場で返す)。
 *     Render の永続ディスクを添付で埋めない・退避や保存期間の運用を増やさないため。
 *     同期時に保存しているのはメタデータ (ファイル名・サイズ・取得キー) だけ
 *   - チャネル別の取り方はアダプター (sync/adapters/*.js の fetchAttachment) に閉じる。
 *     ここは「どのアダプターに渡すか」と安全な返し方 (MIME・サイズ上限) だけを担う
 *   - 取得できない添付 (取得キーが無い旧データ・削除済み) はエラー文言で返し、画面側で
 *     「取得できませんでした」と出す (画面全体は壊さない)
 */
import { getDB } from './db.js';
import { buildAdapterForShop } from './sync/cron.js';
import { contentTypeForServing } from './mime.js';

/** 1ファイルの上限 (これを超えるものはモール/メールの画面で見てもらう)。
 * 取得はメモリ上に全量を載せる (Gmailはbase64 JSONで返るためストリームできない) ので、
 * 同時アクセス時のメモリを見込んで控えめにする */
export const MAX_ATTACHMENT_BYTES = 20 * 1024 * 1024;
/** 取得結果の短期キャッシュ (画像を並べたスレッドを複数人が開いても外部APIを叩き直さない) */
const CACHE_TTL_MS = 5 * 60 * 1000;
const CACHE_MAX_BYTES = 48 * 1024 * 1024;
/** 外部APIへの同時取得数 (Gmail/RMS/Yahoo!のレート制限とRenderのメモリを守る) */
const MAX_CONCURRENT_FETCH = 3;

const cache = new Map();      // attachmentId -> { value, bytes, expiresAt }
const inflight = new Map();   // attachmentId -> Promise (同一添付の同時リクエストを1回に集約)
let cacheBytes = 0;
let running = 0;
const waiters = [];

function cacheGet(id) {
  const hit = cache.get(id);
  if (!hit) return null;
  if (hit.expiresAt <= Date.now()) { cache.delete(id); cacheBytes -= hit.bytes; return null; }
  return hit.value;
}

function cachePut(id, value) {
  const bytes = value.buffer.length;
  if (bytes > CACHE_MAX_BYTES) return;              // 単体で枠を超えるものは載せない
  const old = cache.get(id);
  if (old) { cache.delete(id); cacheBytes -= old.bytes; }
  // 枠を超えたら古い順 (挿入順) に捨てる
  while (cacheBytes + bytes > CACHE_MAX_BYTES && cache.size > 0) {
    const [k, v] = cache.entries().next().value;
    cache.delete(k);
    cacheBytes -= v.bytes;
  }
  cache.set(id, { value, bytes, expiresAt: Date.now() + CACHE_TTL_MS });
  cacheBytes += bytes;
}

/** 同時取得数の制限 (超過分は順番待ち) */
async function withSlot(fn) {
  if (running >= MAX_CONCURRENT_FETCH) await new Promise(resolve => waiters.push(resolve));
  running++;
  try { return await fn(); }
  finally {
    running--;
    const next = waiters.shift();
    if (next) next();
  }
}

/** 添付1件 + それが属するメッセージ・問い合わせ・店舗をまとめて引く */
export function getAttachmentContext(attachmentId) {
  const db = getDB();
  if (!Number.isInteger(attachmentId)) return null;
  return db.prepare(`SELECT a.*, m.external_message_id, m.inquiry_id,
      i.channel_type, i.shop_id, i.external_inquiry_id
    FROM inquiry_attachments a
    JOIN inquiry_messages m ON m.id = a.inquiry_message_id
    JOIN inquiries i ON i.id = m.inquiry_id
    WHERE a.id = ?`).get(attachmentId) || null;
}

/**
 * 添付の実体を取得する (短期キャッシュ + 同一添付の同時リクエスト集約 + 同時実行制限つき)。
 * @returns {{ buffer: Buffer, contentType: string, fileName: string }}
 * @throws チャネル未対応・取得キー無し・外部エラー時 (メッセージは画面に出せる日本語)
 */
export async function fetchAttachmentBody(ctx) {
  const cached = cacheGet(ctx.id);
  if (cached) return cached;
  const pending = inflight.get(ctx.id);
  if (pending) return pending;                       // 同じ画像を複数タブ/複数人が同時に開いても取得は1回
  const p = withSlot(() => fetchFromChannel(ctx))
    .then(v => { cachePut(ctx.id, v); return v; })
    .finally(() => inflight.delete(ctx.id));
  inflight.set(ctx.id, p);
  return p;
}

/** 実際にチャネル (Gmail/楽天/Yahoo!) から取る部分 */
async function fetchFromChannel(ctx) {
  const db = getDB();
  const shop = db.prepare('SELECT * FROM shops WHERE id = ?').get(ctx.shop_id);
  if (!shop) throw new Error('店舗設定が見つかりません');
  const adapter = buildAdapterForShop(shop);
  if (!adapter) {
    throw new Error(`${ctx.channel_type} の接続設定 (env) が未設定のため添付を取得できません`);
  }
  if (typeof adapter.fetchAttachment !== 'function') {
    throw new Error(`${ctx.channel_type} は添付の取得に対応していません`);
  }
  const got = await adapter.fetchAttachment({
    externalAttachmentId: ctx.external_attachment_id,
    externalMessageId: ctx.external_message_id,
    externalInquiryId: ctx.external_inquiry_id,
    fileName: ctx.file_name,
    fileSize: ctx.file_size,
    maxBytes: MAX_ATTACHMENT_BYTES,
  });
  if (!got?.buffer?.length) throw new Error('添付の中身が空でした');
  if (got.buffer.length > MAX_ATTACHMENT_BYTES) {
    throw new Error(`添付が大きすぎます (${Math.round(got.buffer.length / 1048576)}MB。上限${Math.round(MAX_ATTACHMENT_BYTES / 1048576)}MB)`);
  }
  const fileName = got.fileName || ctx.file_name || 'attachment';
  // 申告 Content-Type も拡張子もそのままは信じない。inline (画像・PDF) で返すのは
  // 中身の先頭バイトが一致したものだけ。食い違えばダウンロード扱いに落とす (mime.js)
  return { buffer: got.buffer, contentType: contentTypeForServing(fileName, got.contentType || ctx.content_type, got.buffer), fileName };
}

/** ダウンロード名のヘッダー値 (RFC5987。日本語ファイル名を壊さない + ヘッダーインジェクション防止) */
export function contentDispositionValue(fileName, inline) {
  const safeAscii = String(fileName || 'attachment').replace(/[^\x20-\x7e]/g, '_').replace(/["\\]/g, '_');
  const encoded = encodeURIComponent(String(fileName || 'attachment')).replace(/['()*]/g, c => '%' + c.charCodeAt(0).toString(16).toUpperCase());
  return `${inline ? 'inline' : 'attachment'}; filename="${safeAscii}"; filename*=UTF-8''${encoded}`;
}
