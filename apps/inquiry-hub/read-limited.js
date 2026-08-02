/**
 * 上限つきレスポンス読み取り (添付ファイル取得の防御。2026-08-02 Codexレビュー High-1)
 *
 * Content-Length は申告値でしかない (欠落・過小申告があり得る)。読み終えてからサイズを
 * 判定すると、その時点で既にメモリを食っている → 読みながら実バイト数で打ち切る。
 *
 * adapters から使うため独立モジュールにしている (attachments.js は cron.js 経由で
 * adapters を読むので、そこに置くと循環 import になる)。
 */

/**
 * @param {Response} res fetch のレスポンス
 * @param {number|null} maxBytes 上限 (null なら無制限)
 * @param {string} label エラー文言に出す対象名
 * @returns {Promise<Buffer>}
 */
export async function readBodyWithLimit(res, maxBytes, label = '添付') {
  const limitMb = maxBytes ? Math.round(maxBytes / 1048576) : 0;
  // 申告値で弾けるものは読む前に弾く (無駄な転送をしない)
  const declared = Number(res.headers.get('content-length'));
  if (maxBytes && Number.isFinite(declared) && declared > maxBytes) {
    throw new Error(`${label}が大きすぎます (${Math.round(declared / 1048576)}MB。上限${limitMb}MB)`);
  }
  const reader = res.body?.getReader?.();
  if (!reader) {
    // body ストリームを持たない実装 (テストのモック等) はまとめて読んでから判定する
    const buf = Buffer.from(await res.arrayBuffer());
    if (maxBytes && buf.length > maxBytes) {
      throw new Error(`${label}が大きすぎます (${Math.round(buf.length / 1048576)}MB。上限${limitMb}MB)`);
    }
    return buf;
  }
  const chunks = [];
  let total = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    total += value.length;
    if (maxBytes && total > maxBytes) {
      // 途中で打ち切る (残りは受け取らない)
      await reader.cancel().catch(() => { /* 既に切れている場合は無視 */ });
      throw new Error(`${label}が大きすぎます (上限${limitMb}MB)`);
    }
    chunks.push(Buffer.from(value));
  }
  return Buffer.concat(chunks, total);
}
