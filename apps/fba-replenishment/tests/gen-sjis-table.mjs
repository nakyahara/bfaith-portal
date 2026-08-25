/**
 * tools/fba-fukutsu-helper/sjis.js (Unicode→CP932 変換表) を iconv-lite から生成する。
 * ブラウザ (content script) には Shift_JIS エンコーダが無いので、拡張に表を同梱する。
 *   node apps/fba-replenishment/tests/gen-sjis-table.mjs
 * 生成物はコミットする。iconv-lite を上げたときに再生成して差分を見る。
 */
import fs from 'node:fs';
import path from 'node:path';
import iconv from 'iconv-lite';

const here = path.dirname(new URL(import.meta.url).pathname.replace(/^\/(?=[A-Za-z]:)/, ''));
const out = path.resolve(here, '../../../tools/fba-fukutsu-helper/sjis.js');

const runs = [];
let cur = null;
let count = 0;
for (let u = 0x80; u <= 0xffff; u++) {
  if (u >= 0xd800 && u <= 0xdfff) continue;
  const ch = String.fromCharCode(u);
  const b = iconv.encode(ch, 'cp932');
  if (b.length === 1 && b[0] === 0x3f) continue; // 変換不能 ('?')
  if (iconv.decode(b, 'cp932') !== ch) continue; // 往復一致する文字だけ
  const s = b.length === 1 ? b[0] : (b[0] << 8) | b[1];
  count++;
  if (cur && u === cur.u + cur.n && s === cur.s + cur.n) { cur.n++; continue; }
  cur = { u, s, n: 1 };
  runs.push(cur);
}
const flat = runs.flatMap((r) => [r.u, r.s, r.n]);

const body = `// 自動生成: Unicode→CP932 (Shift_JIS) の変換表。[unicode開始, sjis開始, 連続数] の繰り返し。
// 生成元 = iconv-lite cp932 (往復一致する文字のみ)。手で編集しない (再生成 = apps/fba-replenishment/tests/gen-sjis-table.mjs)。
(function (root) {
  'use strict';
  var RUNS = [${flat.join(',')}];
  var MAP = {};
  for (var i = 0; i < RUNS.length; i += 3) { for (var k = 0; k < RUNS[i + 2]; k++) MAP[RUNS[i] + k] = RUNS[i + 1] + k; }
  /** 文字列→Shift_JIS(CP932)バイト列。変換できない文字があれば投げる (化けた伝票を出さない) */
  function encodeSjis(str) {
    var s = String(str == null ? '' : str);
    var out = [], bad = [];
    for (var i = 0; i < s.length; i++) {
      var u = s.charCodeAt(i);
      if (u < 0x80) { out.push(u); continue; }
      var c = MAP[u];
      if (c === undefined) { if (bad.indexOf(s[i]) < 0) bad.push(s[i]); continue; }
      if (c > 0xFF) out.push(c >> 8, c & 0xFF); else out.push(c);
    }
    if (bad.length) { var e = new Error('Shift_JISにできない文字があります: ' + bad.join(' ')); e.code = 'SJIS'; throw e; }
    return new Uint8Array(out);
  }
  root.BF_SJIS = { encodeSjis: encodeSjis };
})(typeof globalThis !== 'undefined' ? globalThis : this);
`;
fs.writeFileSync(out, body);
console.log(`${out}: ${count}文字 / ${runs.length}区間 / ${body.length}bytes`);
