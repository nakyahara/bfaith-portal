/**
 * ABA Top Search Terms レポート (JSON) のストリーミングパーサ
 *
 * レポートは単一の巨大JSON:
 *   { "reportSpecification": {...}, "dataByDepartmentAndSearchTerm": [ {...}, {...}, ... ] }
 * 数百MB級になり得るため JSON.parse は使えない (miniPC はメモリ制約あり)。
 *
 * 実装方針 (Codex R1〜R4 の指摘を反映した決定版):
 *   文書全体を「厳密なJSON文法の streaming FSM」で検証しながら読む。
 *   キー/コロン/値/カンマの順序・括弧の種類・リテラルの正当性をすべて検証し、
 *   途中切断・ゴミ・構文崩れは必ずエラーにする (fail-closed —
 *   壊れたレポートを「取込完了」として台帳確定させない)。
 *   対象配列 (トップレベルの dataByDepartmentAndSearchTerm) の要素オブジェクトだけは
 *   生テキストをキャプチャして要素単位で JSON.parse する (値の解釈は本物に任せる)。
 *   文字列値にキー名が現れても誤検知しない (キー文字列だけを照合)。
 *
 * 既知の限界 (Codex R5 medium、いずれも安全側なので許容):
 *   - 非キャプチャ部の文字列内の不正エスケープ (\q 等) は検証しない (要素本体は
 *     JSON.parse が検証する。メタデータの緩い受理はデータ破損に繋がらない)
 *   - キー照合は生文字列比較のため "dataBy...Term" のような等価エスケープ表記は
 *     不一致 → 文書拒否 = fail-closed でジョブが失敗して顕在化する (サイレント欠落しない)
 */
import { StringDecoder } from 'string_decoder';

const ARRAY_KEY = 'dataByDepartmentAndSearchTerm';
const SCALAR_RE = /^(true|false|null|-?(0|[1-9]\d*)(\.\d+)?([eE][+-]?\d+)?)$/;

/**
 * ストリーミングで配列要素を1件ずつ callback に渡す。
 * @param {AsyncIterable<Buffer|string>} source - 解凍済みJSONテキストのチャンク列
 * @param {(item: object) => void} onItem
 * @returns {Promise<{itemCount: number}>}
 */
export async function parseAbaReportStream(source, onItem) {
  let itemCount = 0;

  // ---- 文法FSM ----
  // frame: { type: 'obj', expect: 'key-or-end'|'key'|'colon'|'value'|'comma-or-end' }
  //        { type: 'arr', expect: 'value-or-end'|'value'|'comma-or-end', isTarget }
  const stack = [];
  let rootSeen = false;           // トップレベル値を読み始めたか
  let docDone = false;            // トップレベル値が完結したか
  let targetSeen = false;         // 対象配列が現れたか
  let pendingTargetValue = false; // 直前のトップレベルキーが ARRAY_KEY → 次の値が対象配列
  let inString = false;
  let escaped = false;
  let strIsKey = false;
  let strBuf = null;              // トップレベルのキー文字列の中身 (キー照合用)。それ以外は null
  let literal = '';               // スカラーリテラル蓄積 (true/false/null/数値の検証用)

  // ---- 対象配列の要素キャプチャ ----
  let collecting = false;
  let elDepth = 0;
  let elInString = false;
  let elEscaped = false;
  let buf = '';

  const isWs = (c) => c === ' ' || c === '\t' || c === '\n' || c === '\r';
  const err = (msg) => { throw new Error(`ABAレポート解析エラー: ${msg} (itemCount=${itemCount})`); };
  const top = () => stack[stack.length - 1];

  // 1つの値が完結した → 親フレームの期待を進める (親なし = 文書完結)
  const valueDone = () => {
    const t = top();
    if (!t) { docDone = true; return; }
    t.expect = 'comma-or-end';
  };
  const flushLiteral = () => {
    if (literal === '') return;
    if (!SCALAR_RE.test(literal)) err(`不正なリテラル '${literal.slice(0, 30)}'`);
    literal = '';
    valueDone();
  };
  // いま「値の開始」が許される位置か
  const expectingValue = () => {
    if (docDone) return false;
    if (stack.length === 0) return !rootSeen;
    const t = top();
    if (t.type === 'obj') return t.expect === 'value';
    return t.expect === 'value' || t.expect === 'value-or-end';
  };
  // 値開始の共通処理。対象配列フラグは「次の1値」だけに効く
  const beginValue = () => {
    if (stack.length === 0) rootSeen = true;
    const wasTarget = pendingTargetValue;
    pendingTargetValue = false;
    return wasTarget;
  };

  // マルチバイト文字がチャンク境界で割れても壊れないよう StringDecoder で持ち越す
  const decoder = new StringDecoder('utf8');

  for await (const chunk of source) {
    const text = typeof chunk === 'string' ? chunk : decoder.write(chunk);

    for (let i = 0; i < text.length; i++) {
      const ch = text[i];

      // ================= 対象配列の要素キャプチャ =================
      if (collecting) {
        buf += ch;
        if (elInString) {
          if (elEscaped) { elEscaped = false; }
          else if (ch === '\\') { elEscaped = true; }
          else if (ch === '"') { elInString = false; }
          continue;
        }
        if (ch === '"') { elInString = true; continue; }
        if (ch === '{' || ch === '[') { elDepth++; continue; }
        if (ch === '}' || ch === ']') {
          elDepth--;
          if (elDepth === 0) {
            onItem(JSON.parse(buf));
            itemCount++;
            buf = '';
            collecting = false;
            top().expect = 'comma-or-end';
          }
        }
        continue;
      }

      // ================= 文字列 (キー / 値) =================
      if (inString) {
        if (escaped) { escaped = false; }
        else if (ch === '\\') { escaped = true; }
        else if (ch === '"') {
          inString = false;
          if (strIsKey) {
            top().expect = 'colon';
            pendingTargetValue = stack.length === 1 && strBuf === ARRAY_KEY;
          } else {
            valueDone();
          }
          strBuf = null;
        } else if (strBuf !== null && strBuf.length <= ARRAY_KEY.length) {
          strBuf += ch;
        }
        continue;
      }

      if (isWs(ch)) { flushLiteral(); continue; }
      // リテラルの直後に構造文字が来たら先に確定させる (例: `123,` / `true}`)
      if (ch === ',' || ch === ':' || ch === '}' || ch === ']') flushLiteral();
      if (docDone) err(`文書終端後に余分な文字 '${ch}'`);

      switch (ch) {
        case '{': {
          if (!expectingValue()) err(`'{' の位置が不正`);
          const t = top();
          if (t && t.type === 'arr' && t.isTarget) {
            // 対象配列の要素 → キャプチャ開始 (FSMを一時バイパス、閉じで JSON.parse)
            collecting = true;
            elDepth = 1;
            elInString = false;
            elEscaped = false;
            buf = '{';
            break;
          }
          beginValue();
          stack.push({ type: 'obj', expect: 'key-or-end' });
          break;
        }
        case '[': {
          if (!expectingValue()) err(`'[' の位置が不正`);
          const isTarget = beginValue();
          if (isTarget && targetSeen) err('対象配列が複数回出現');
          if (isTarget) targetSeen = true;
          stack.push({ type: 'arr', expect: 'value-or-end', isTarget });
          break;
        }
        case '}': {
          const t = top();
          if (!t || t.type !== 'obj' || (t.expect !== 'key-or-end' && t.expect !== 'comma-or-end')) {
            err(`'}' の位置が不正`);
          }
          stack.pop();
          valueDone();
          break;
        }
        case ']': {
          const t = top();
          if (!t || t.type !== 'arr' || (t.expect !== 'value-or-end' && t.expect !== 'comma-or-end')) {
            err(`']' の位置が不正`);
          }
          stack.pop();
          valueDone();
          break;
        }
        case ',': {
          const t = top();
          if (!t || t.expect !== 'comma-or-end') err(`',' の位置が不正`);
          t.expect = t.type === 'obj' ? 'key' : 'value';
          break;
        }
        case ':': {
          const t = top();
          if (!t || t.type !== 'obj' || t.expect !== 'colon') err(`':' の位置が不正`);
          t.expect = 'value';
          break;
        }
        case '"': {
          const t = top();
          if (t && t.type === 'obj' && (t.expect === 'key-or-end' || t.expect === 'key')) {
            inString = true;
            strIsKey = true;
            // キー照合はトップレベル (= root オブジェクト直下) だけ必要
            strBuf = stack.length === 1 ? '' : null;
            break;
          }
          if (!expectingValue()) err(`'"' の位置が不正`);
          if (t && t.type === 'arr' && t.isTarget) err('対象配列に文字列要素 (オブジェクトのみ想定)');
          beginValue();
          inString = true;
          strIsKey = false;
          strBuf = null;
          break;
        }
        default: {
          // スカラーリテラル (true/false/null/数値) の開始または継続
          if (literal === '') {
            if (!expectingValue()) err(`不正な文字 '${ch}'`);
            const t = top();
            if (t && t.type === 'arr' && t.isTarget) err('対象配列に非オブジェクト要素');
            beginValue();
          }
          literal += ch;
        }
      }
    }
  }

  flushLiteral(); // トップレベルがスカラーだけの文書 (異常系) の確定用
  if (collecting || inString || stack.length > 0 || !docDone) {
    err('文書が完結する前にストリームが終了 (ダウンロード途中切断の疑い)');
  }
  if (!targetSeen) err('dataByDepartmentAndSearchTerm 配列が見つからない');
  return { itemCount };
}

/**
 * レポート要素 → aba_search_terms 行への正規化。
 * フィールド名はAmazonの仕様変更に備えて候補を複数見る (実レポートで要確認)。
 * @returns {object|null} 必須フィールド欠落時は null (呼び出し側でカウント)
 */
export function normalizeAbaItem(item) {
  const searchTerm = item.searchTerm ?? item.search_term;
  const rank = item.searchFrequencyRank ?? item.search_frequency_rank;
  const asin = item.clickedAsin ?? item.clicked_asin;
  if (!searchTerm || rank == null || !asin) return null;

  const position = item.clickShareRank ?? item.click_share_rank ?? null;
  return {
    department: item.departmentName ?? item.department_name ?? '',
    search_term: String(searchTerm),
    search_frequency_rank: Number(rank),
    click_position: position != null ? Number(position) : null,
    asin: String(asin).toUpperCase(),
    product_title: item.clickedItemName ?? item.clicked_item_name ?? item.productTitle ?? null,
    click_share: item.clickShare ?? item.click_share ?? null,
    conversion_share: item.conversionShare ?? item.conversion_share ?? null,
  };
}
