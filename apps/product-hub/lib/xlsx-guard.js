/**
 * .xlsx を展開する**前**に、展開後の大きさを見る (2026-10-01・Codex API R1 #2 / R3 #1)。
 *
 * xlsx は ZIP。5MB のファイルでも中身が高圧縮なら展開で GB になりうる (zip bomb)。
 * exceljs の `wb.xlsx.load(buf)` は ZIP 全体を展開してからメモリに載せるので、
 * **load を呼んだ時点で手遅れ**。セル数や文字数の上限は load の後にしか効かない。
 *
 * なのでここで ZIP の**中央ディレクトリだけ**を読む。中央ディレクトリには各エントリの
 * 「展開後サイズ」が書いてあり、**1 バイトも展開せずに**合計が分かる。
 * 解凍しないので、どんなに悪い入力でもここでの費用は数十 KB の走査だけ。
 *
 * 嘘のサイズが書かれている可能性はある (中央ディレクトリは自己申告)。
 * それでも「申告が大きい」ものは確実に弾けるので、素朴な爆弾には十分効く。
 * 申告を偽って実際に巨大なものは防げない — それは load 後のセル数・文字数の上限で受ける。
 */

const EOCD_SIG = 0x06054b50;      // End of Central Directory
const CDH_SIG = 0x02014b50;       // Central Directory Header
const ZIP64_MARK = 0xffffffff;

export class XlsxTooLargeError extends Error {
  constructor(message) {
    super(message);
    this.name = 'XlsxTooLargeError';
    this.tooLarge = true;
  }
}

/**
 * 展開せずに ZIP の中央ディレクトリから展開後サイズの合計を求める。
 * @param {Buffer} buf
 * @returns {{entries: number, expandedBytes: number}}
 * @throws {Error} ZIP として読めない / zip64
 */
export function zipExpandedSize(buf) {
  if (!Buffer.isBuffer(buf) || buf.length < 22) throw new Error('ZIP として短すぎます');
  // EOCD は末尾にある (コメントが付くと最大 64KB 手前)。後ろから探す
  const from = Math.max(0, buf.length - 22 - 0xffff);
  let eocd = -1;
  for (let i = buf.length - 22; i >= from; i--) {
    if (buf.readUInt32LE(i) === EOCD_SIG) { eocd = i; break; }
  }
  if (eocd < 0) throw new Error('ZIP の末尾 (EOCD) が見つかりません');

  const entries = buf.readUInt16LE(eocd + 10);
  const cdSize = buf.readUInt32LE(eocd + 12);
  const cdOffset = buf.readUInt32LE(eocd + 16);
  if (entries === 0xffff || cdSize === ZIP64_MARK || cdOffset === ZIP64_MARK) {
    throw new Error('zip64 の .xlsx は受け付けません');
  }
  if (cdOffset + cdSize > buf.length) throw new Error('ZIP の中央ディレクトリが壊れています');

  let p = cdOffset;
  let expanded = 0;
  let n = 0;
  while (n < entries) {
    if (p + 46 > buf.length || buf.readUInt32LE(p) !== CDH_SIG) {
      throw new Error('ZIP の中央ディレクトリが壊れています');
    }
    const uncompressed = buf.readUInt32LE(p + 24);
    if (uncompressed === ZIP64_MARK) throw new Error('zip64 の .xlsx は受け付けません');
    expanded += uncompressed;
    const nameLen = buf.readUInt16LE(p + 28);
    const extraLen = buf.readUInt16LE(p + 30);
    const commentLen = buf.readUInt16LE(p + 32);
    p += 46 + nameLen + extraLen + commentLen;
    n += 1;
  }
  return { entries, expandedBytes: expanded };
}

/**
 * 展開後の大きさが上限に収まるか。収まらなければ XlsxTooLargeError を投げる
 * (呼び手は 413 にする。「xlsx として読めない」400 と区別する — Codex API R3 #3)。
 * ZIP として読めない入力は普通の Error (= 400)。
 *
 * @param {Buffer} buf
 * @param {{maxEntries?: number, maxExpandedBytes?: number, maxRatio?: number}} limits
 */
export function assertXlsxExpandsSafely(buf, {
  maxEntries = 500,
  maxExpandedBytes = 80 * 1024 * 1024,
  maxRatio = 200,
} = {}) {
  const { entries, expandedBytes } = zipExpandedSize(buf);
  if (entries > maxEntries) {
    throw new XlsxTooLargeError(`.xlsx の中のファイルが多すぎます (${entries} / ${maxEntries} まで)`);
  }
  if (expandedBytes > maxExpandedBytes) {
    throw new XlsxTooLargeError(
      `.xlsx の展開後が大きすぎます (${Math.round(expandedBytes / 1024 / 1024)}MB / ${Math.round(maxExpandedBytes / 1024 / 1024)}MB まで)`);
  }
  // 圧縮比。普通の .xlsx は 10〜20 倍。200 倍を超えるのは中身がほぼ同じ文字の繰り返し = 爆弾の形
  if (buf.length > 0 && expandedBytes / buf.length > maxRatio) {
    throw new XlsxTooLargeError(`.xlsx の圧縮比が高すぎます (${Math.round(expandedBytes / buf.length)} 倍 / ${maxRatio} 倍まで)`);
  }
  return { entries, expandedBytes };
}
