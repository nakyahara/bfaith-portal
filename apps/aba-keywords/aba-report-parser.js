/**
 * ABA Top Search Terms レポート (JSON) のストリーミングパーサ
 *
 * レポートは単一の巨大JSON:
 *   { "reportSpecification": {...}, "dataByDepartmentAndSearchTerm": [ {...}, {...}, ... ] }
 * 数百MB級になり得るため JSON.parse は使えない (miniPC はメモリ制約あり)。
 *
 * 依存を増やさないため専用の逐次スキャナで実装する。キー探索は生テキストの
 * indexOf ではなく JSON 構文 (トップレベル深さ・文字列・エスケープ) を追跡し、
 * 「depth=1 のプロパティ名 dataByDepartmentAndSearchTerm の直後の ':' → '['」
 * だけを配列開始と認める (文字列値にキー名が現れても誤検知しない — Codex R1 medium)。
 * 要素は {} / [] ネストとエスケープを追跡して切り出し、要素単位で JSON.parse する。
 */
import { StringDecoder } from 'string_decoder';

const ARRAY_KEY = 'dataByDepartmentAndSearchTerm';

/**
 * ストリーミングで配列要素を1件ずつ callback に渡す。
 * 配列の閉じ ']' まで読めなければ必ず throw する (途中切断を成功扱いすると
 * 台帳が「取込完了」になりサイレント欠落するため — Codex R1 high)。
 * @param {AsyncIterable<Buffer|string>} source - 解凍済みJSONテキストのチャンク列
 * @param {(item: object) => void} onItem
 * @returns {Promise<{itemCount: number}>}
 */
export async function parseAbaReportStream(source, onItem) {
  let itemCount = 0;

  // ---- 探索フェーズの状態 (in-array 前) ----
  // phase: 'seek' → キー探索 / 'expect-colon' → キー直後の ':' 待ち /
  //        'expect-bracket' → '[' 待ち / 'in-array' → 要素収集
  let phase = 'seek';
  let depth = 0;          // ドキュメントの {} / [] 深さ
  let inString = false;
  let escaped = false;
  let strBuf = null;      // depth===1 の文字列の中身 (キー候補)。それ以外は null

  // ---- in-array フェーズの状態 ----
  let elDepth = 0;
  let elInString = false;
  let elEscaped = false;
  let buf = '';
  let collecting = false;
  let done = false;

  // マルチバイト文字がチャンク境界で割れても壊れないよう StringDecoder で持ち越す
  const decoder = new StringDecoder('utf8');

  for await (const chunk of source) {
    if (done) break;
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
        // 要素間: '{' で次要素開始、']' で配列終端
        if (ch === '{') {
          collecting = true;
          elDepth = 1;
          elInString = false;
          elEscaped = false;
          buf = '{';
        } else if (ch === ']') {
          done = true;
          break;
        }
        continue; // カンマ・空白は読み飛ばす
      }

      // ================= キー探索 (JSON構文追跡) =================
      if (inString) {
        if (escaped) { escaped = false; }
        else if (ch === '\\') { escaped = true; }
        else if (ch === '"') {
          inString = false;
          if (depth === 1 && strBuf === ARRAY_KEY) phase = 'expect-colon';
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
        strBuf = depth === 1 ? '' : null;
        continue;
      }
      if (ch === ' ' || ch === '\t' || ch === '\n' || ch === '\r') continue;

      if (phase === 'expect-colon') {
        if (ch === ':') { phase = 'expect-bracket'; continue; }
        // ':' 以外 = キーではなく「キー名と同じ文字列値」だった → 探索継続 (構造文字は下で処理)
        phase = 'seek';
      }
      if (phase === 'expect-bracket') {
        if (ch === '[') { phase = 'in-array'; continue; }
        // 値が配列でない → 探索継続
        phase = 'seek';
      }
      if (ch === '{' || ch === '[') depth++;
      else if (ch === '}' || ch === ']') depth--;
    }
  }

  if (!done) {
    if (phase !== 'in-array') {
      throw new Error('ABAレポート解析エラー: dataByDepartmentAndSearchTerm 配列が見つからない');
    }
    // 配列の閉じ ']' 未到達 = ダウンロード途中切断。成功扱いにすると欠損データで
    // 台帳が確定してしまうため必ず失敗させる
    throw new Error(`ABAレポート解析エラー: 配列が閉じる前にストリームが終了 (itemCount=${itemCount}, collecting=${collecting})`);
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
