/**
 * canonical-hash.mjs — manifest の checksum の共通の部品 (Company DB構想 13 §3.1・§3.4・R8 Low4)
 *   = **正規の JSON の SHA-256 (UTF-8)**。観測の原価 (D7b-2) が最初の使い手。決済のそろった日 (coverage) の manifest・原価の cost_input_hash も同じ部品を使う (後の PR)。
 *
 * 正規の JSON の決まり (どの使い手も同じ):
 *   - object の鍵は **文字列の UTF-16 の順に並べ直す** (作った順に依らない = 鍵の順が固定)
 *   - null は JSON の null。undefined・関数・symbol・bigint は例外 (黙って落とさない・丸めない)
 *   - 数は **安全な整数だけ** (円・件数・ID)。小数・NaN・Infinity は例外 (浮動小数の表し方の違いを指紋に入れない)
 *   - 日付・日時は呼ぶ側が文字列にしてから渡す (日付 = YYYY-MM-DD・日時 = UTC の YYYY-MM-DDTHH:MM:SSZ)。Date は例外
 *   - 配列の並びは呼ぶ側が決める (観測の原価 = 商品コード → valid_from)
 * 🚨 保存済みの checksum と同じ式のまま変えない (変えるときは使い手の版を上げる)
 */
import crypto from 'node:crypto';

/** 正規の JSON の文字列。決まりの外の値は例外 */
export function canonicalJsonStrict(v, where = '$') {
  if (v === null) return 'null';
  switch (typeof v) {
    case 'string': return JSON.stringify(v);
    case 'boolean': return v ? 'true' : 'false';
    case 'number':
      if (!Number.isSafeInteger(v)) throw new Error(`${where}: 数は安全な整数だけ (${v})`);
      return String(v);
    case 'object': {
      if (Object.getOwnPropertySymbols(v).length) throw new Error(`${where}: symbol の鍵は使えない (黙って落とさない)`);   // 配列にも (Codex #1549 R4 Low1)
      if (Array.isArray(v)) {
        // 🚨 疎な配列 (穴) は例外 (map は穴を飛ばす = Array(1) と [] が同じ指紋になる。Codex #1549 R3 Low1)
        const parts = [];
        for (let i = 0; i < v.length; i++) { if (!Object.hasOwn(v, i)) throw new Error(`${where}[${i}]: 配列に穴がある`); parts.push(canonicalJsonStrict(v[i], `${where}[${i}]`)); }
        return `[${parts.join(',')}]`;
      }
      if (Object.getPrototypeOf(v) !== Object.prototype && Object.getPrototypeOf(v) !== null) throw new Error(`${where}: 素の object でない (Date などは文字列にしてから)`);
      if (Object.getOwnPropertySymbols(v).length) throw new Error(`${where}: symbol の鍵は使えない (黙って落とさない)`);
      const keys = Object.keys(v).sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
      return `{${keys.map((k) => `${JSON.stringify(k)}:${canonicalJsonStrict(v[k], `${where}.${k}`)}`).join(',')}}`;
    }
    default: throw new Error(`${where}: JSON にできない値 (${typeof v})`);
  }
}

/** 正規の JSON の SHA-256 (UTF-8・16 進 64 桁) */
export function canonicalSha256(v) {
  return crypto.createHash('sha256').update(canonicalJsonStrict(v), 'utf8').digest('hex');
}
