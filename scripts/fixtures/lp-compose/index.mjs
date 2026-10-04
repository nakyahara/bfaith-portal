/**
 * fixture の読み出し (テスト用)。
 *
 * PR1-c から **lint はサーバが実行して、それが正本**になった。
 * なので「構成を accepted で受け取る」テストは、**本当に lint を通る本文**でないと通らない。
 * 短いダミー文字列は使えない — そこがこのファイルの存在理由。
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));

export const FIXTURES = {
  /** 仕様書「出力例」+「AIプロンプトV2.2」から組んだ 2 枚もの */
  specExample: 'v22-2images-spec-example.md',
  /** 3 枚もの (0〜2枚目) */
  hakka3: 'v22-3images-hakka.md',
  /** 旧版。lint が落とすことを固定する */
  legacyV21: 'v21-legacy.md',
};

// 改行コードは正規化する。.gitattributes で LF に固定しているが、
// それを外した PC でもテストが意味不明に落ちないように
export const fixture = (name) =>
  fs.readFileSync(path.join(HERE, name), 'utf8').replace(/\r\n?/g, '\n');

/**
 * 商品名だけ差し替えた構成を返す。
 * lint の検査 17 は「構成の `## 商品` が draft の商品名と一致すること」を見るので、
 * テストの draft 名に合わせる必要がある。
 */
export function compositionFor(productName, name = FIXTURES.hakka3) {
  const body = fixture(name);
  // 🚨 「置き換わったか」で見ない — fixture の商品名と同じ名前を渡すと中身が変わらず、
  //    見出しがあるのに「無い」と言ってしまう
  const re = /^## 商品\n.*$/m;
  if (!re.test(body)) throw new Error(`fixture ${name} に「## 商品」の行がありません`);
  return body.replace(re, `## 商品\n${productName}`);
}
