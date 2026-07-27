/**
 * ABA Top Search Terms レポート (JSON) のストリーミングパーサ
 *
 * レポートは単一の巨大JSON:
 *   { "reportSpecification": {...}, "dataByDepartmentAndSearchTerm": [ {...}, {...}, ... ] }
 * 数百MB級になり得るため JSON.parse は使えない (miniPC はメモリ制約あり)。
 *
 * 依存を増やさないため、専用の逐次スキャナで実装する。前提は
 * 「dataByDepartmentAndSearchTerm 配列の要素はネストなしのフラットなオブジェクト」
 * だが、安全側に倒して要素内の {} / [] ネストと文字列エスケープは正しく追跡する。
 * 要素単位で JSON.parse するので値の解釈は本物の JSON パーサに任せる。
 */

import { StringDecoder } from 'string_decoder';

const ARRAY_KEY = '"dataByDepartmentAndSearchTerm"';

/**
 * ストリーミングで配列要素を1件ずつ callback に渡す。
 * @param {AsyncIterable<Buffer|string>} source - 解凍済みJSONテキストのチャンク列
 * @param {(item: object) => void} onItem
 * @returns {Promise<{itemCount: number}>}
 */
export async function parseAbaReportStream(source, onItem) {
  let itemCount = 0;

  // 状態機械
  // phase: 'seek-key' → ARRAY_KEY を探す / 'seek-array' → '[' を待つ / 'in-array' → 要素収集
  let phase = 'seek-key';
  let tail = '';            // seek-key 用: チャンク境界でキーが割れた場合に備えた持ち越し
  let depth = 0;            // 要素内の {} / [] ネスト深さ
  let inString = false;
  let escaped = false;
  let buf = '';             // 現在の要素の生JSON
  let collecting = false;   // 要素の '{' を見つけて収集中か
  let done = false;

  // マルチバイト文字がチャンク境界で割れても壊れないよう StringDecoder で持ち越す
  const decoder = new StringDecoder('utf8');

  for await (const chunk of source) {
    if (done) break;
    let text = typeof chunk === 'string' ? chunk : decoder.write(chunk);
    if (text.length === 0) continue;

    if (phase === 'seek-key') {
      const hay = tail + text;
      const idx = hay.indexOf(ARRAY_KEY);
      if (idx === -1) {
        // キーがチャンク境界で割れる可能性に備え、キー長-1 だけ持ち越す
        tail = hay.slice(-(ARRAY_KEY.length - 1));
        continue;
      }
      phase = 'seek-array';
      text = hay.slice(idx + ARRAY_KEY.length);
      tail = '';
    }

    let i = 0;
    if (phase === 'seek-array') {
      const idx = text.indexOf('[');
      if (idx === -1) continue;
      phase = 'in-array';
      i = idx + 1;
    }

    // phase === 'in-array'
    for (; i < text.length; i++) {
      const ch = text[i];

      if (collecting) {
        buf += ch;
        if (inString) {
          if (escaped) { escaped = false; }
          else if (ch === '\\') { escaped = true; }
          else if (ch === '"') { inString = false; }
          continue;
        }
        if (ch === '"') { inString = true; continue; }
        if (ch === '{' || ch === '[') { depth++; continue; }
        if (ch === '}' || ch === ']') {
          depth--;
          if (depth === 0) {
            onItem(JSON.parse(buf));
            itemCount++;
            buf = '';
            collecting = false;
          }
          continue;
        }
        continue;
      }

      // 要素と要素の間: '{' で次要素開始、']' で配列終端
      if (ch === '{') {
        collecting = true;
        depth = 1;
        inString = false;
        escaped = false;
        buf = '{';
      } else if (ch === ']') {
        done = true;
        break;
      }
      // それ以外 (カンマ・空白・改行) は読み飛ばす
    }
  }

  if (collecting) {
    throw new Error(`ABAレポート解析エラー: 配列要素が途中で終わっている (itemCount=${itemCount})`);
  }
  if (phase !== 'in-array') {
    throw new Error('ABAレポート解析エラー: dataByDepartmentAndSearchTerm 配列が見つからない');
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
