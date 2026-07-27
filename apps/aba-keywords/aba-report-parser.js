/**
 * ABA Top Search Terms レポート (JSON) のストリーミングパーサ
 *
 * レポートは単一の巨大JSON:
 *   { "reportSpecification": {...}, "dataByDepartmentAndSearchTerm": [ {...}, {...}, ... ] }
 * 数百MB級になり得るため JSON.parse は使えない (miniPC はメモリ制約あり)。
 *
 * 依存を増やさないため専用の逐次スキャナで実装する。方針は「省メモリだが厳格」:
 *   - キー探索は JSON 構文 (括弧スタック・文字列・エスケープ) を追跡し、
 *     トップレベル (スタック深さ1) のプロパティ名の直後の ':' → '[' だけを配列開始と認める
 *     (文字列値にキー名が現れても誤検知しない — Codex R1 medium)
 *   - 配列要素はオブジェクトのみ許容。非オブジェクト要素・要素間のゴミは即エラー
 *     (黙って読み飛ばすと 0件取込が「完了」として台帳確定する — Codex R2 high)
 *   - 括弧はスタックで種類まで照合 ('{' を ']' で閉じる破損を検出 — Codex R3 high)。
 *     配列後の残り (tail) も文字列/括弧/スカラーリテラルを検証し、末尾ゴミはエラー
 *   - 要素単位で JSON.parse するので値の解釈は本物の JSON パーサに任せる
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

  // phase: 'seek' キー探索 → 'expect-colon' → 'expect-bracket' → 'in-array' 要素収集
  //        → 'tail' 外側JSONの閉じ待ち → 'complete' 文書終端 (以降は空白のみ許容)
  let phase = 'seek';
  const stack = [];       // 要素外の開き括弧 ('{' | '[')。対象配列の '[' は積まない
  let inString = false;
  let escaped = false;
  let strBuf = null;      // スタック深さ1の文字列の中身 (キー候補)。それ以外は null
  let literal = '';       // tail のスカラーリテラル蓄積 (true/false/null/数値の検証用)

  // in-array の状態
  let arrayExpect = 'value-or-end'; // 'value-or-end' | 'value' | 'comma-or-end'
  let elDepth = 0;
  let elInString = false;
  let elEscaped = false;
  let buf = '';
  let collecting = false;

  const isWs = (c) => c === ' ' || c === '\t' || c === '\n' || c === '\r';
  const popBracket = (ch) => {
    const open = stack.pop();
    if (!open || (ch === '}' && open !== '{') || (ch === ']' && open !== '[')) {
      throw new Error(`ABAレポート解析エラー: 括弧の対応不一致 ('${open || 'なし'}' に対して '${ch}')`);
    }
  };
  const flushLiteral = () => {
    if (literal === '') return;
    if (!SCALAR_RE.test(literal)) {
      throw new Error(`ABAレポート解析エラー: 不正なリテラル '${literal.slice(0, 30)}'`);
    }
    literal = '';
  };

  // マルチバイト文字がチャンク境界で割れても壊れないよう StringDecoder で持ち越す
  const decoder = new StringDecoder('utf8');

  for await (const chunk of source) {
    const text = typeof chunk === 'string' ? chunk : decoder.write(chunk);

    for (let i = 0; i < text.length; i++) {
      const ch = text[i];

      // ================= 要素収集 =================
      if (phase === 'in-array') {
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
            }
          }
          continue;
        }
        // 要素間: 期待トークンを厳格に検証 (ゴミ・非オブジェクト要素は即エラー)
        if (isWs(ch)) continue;
        if (ch === ']' && arrayExpect !== 'value') { phase = 'tail'; continue; }
        if (arrayExpect === 'comma-or-end') {
          if (ch === ',') { arrayExpect = 'value'; continue; }
          throw new Error(`ABAレポート解析エラー: 要素の後に不正な文字 '${ch}' (itemCount=${itemCount})`);
        }
        // 'value' / 'value-or-end': 要素はオブジェクトのみ許容
        if (ch === '{') {
          collecting = true;
          elDepth = 1;
          elInString = false;
          elEscaped = false;
          buf = '{';
          arrayExpect = 'comma-or-end';
          continue;
        }
        throw new Error(`ABAレポート解析エラー: オブジェクト以外の配列要素 (先頭文字 '${ch}', itemCount=${itemCount})`);
      }

      // ================= 配列後: 外側JSONの閉じを検証 =================
      if (phase === 'tail' || phase === 'complete') {
        if (inString) {
          if (escaped) { escaped = false; }
          else if (ch === '\\') { escaped = true; }
          else if (ch === '"') { inString = false; }
          continue;
        }
        if (isWs(ch)) { flushLiteral(); continue; }
        if (phase === 'complete') {
          throw new Error(`ABAレポート解析エラー: 文書終端後に余分な文字 '${ch}'`);
        }
        if (ch === '"') { flushLiteral(); inString = true; continue; }
        if (ch === ',' || ch === ':') { flushLiteral(); continue; }
        if (ch === '{' || ch === '[') { flushLiteral(); stack.push(ch); continue; }
        if (ch === '}' || ch === ']') {
          flushLiteral();
          popBracket(ch);
          if (stack.length === 0) phase = 'complete';
          continue;
        }
        literal += ch; // スカラーリテラル候補 (区切りで検証)
        continue;
      }

      // ================= キー探索 (JSON構文追跡) =================
      if (inString) {
        if (escaped) { escaped = false; }
        else if (ch === '\\') { escaped = true; }
        else if (ch === '"') {
          inString = false;
          if (stack.length === 1 && strBuf === ARRAY_KEY) phase = 'expect-colon';
          strBuf = null;
        } else if (strBuf !== null && strBuf.length <= ARRAY_KEY.length) {
          strBuf += ch;
        }
        continue;
      }
      if (ch === '"') {
        // 文字列開始。expect-colon/expect-bracket 中なら誤検知だったのでリセット
        if (phase !== 'seek') phase = 'seek';
        inString = true;
        strBuf = stack.length === 1 ? '' : null;
        continue;
      }
      if (isWs(ch)) continue;

      if (phase === 'expect-colon') {
        if (ch === ':') { phase = 'expect-bracket'; continue; }
        // ':' 以外 = キーではなく「キー名と同じ文字列値」だった → 探索継続 (構造文字は下で処理)
        phase = 'seek';
      }
      if (phase === 'expect-bracket') {
        if (ch === '[') { phase = 'in-array'; arrayExpect = 'value-or-end'; continue; }
        // 値が配列でない → 探索継続
        phase = 'seek';
      }
      if (ch === '{' || ch === '[') stack.push(ch);
      else if (ch === '}' || ch === ']') popBracket(ch);
    }
  }

  if (phase !== 'complete') {
    if (phase === 'seek' || phase === 'expect-colon' || phase === 'expect-bracket') {
      throw new Error('ABAレポート解析エラー: dataByDepartmentAndSearchTerm 配列が見つからない');
    }
    if (phase === 'in-array') {
      // 配列の閉じ ']' 未到達 = ダウンロード途中切断。成功扱いにすると欠損データで
      // 台帳が確定してしまうため必ず失敗させる
      throw new Error(`ABAレポート解析エラー: 配列が閉じる前にストリームが終了 (itemCount=${itemCount}, collecting=${collecting})`);
    }
    throw new Error(`ABAレポート解析エラー: 外側JSONが閉じる前にストリームが終了 (itemCount=${itemCount})`);
  }
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
