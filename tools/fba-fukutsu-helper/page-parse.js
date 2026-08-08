// Seller Central「ステップ3 – 印刷された輸送箱ラベル」の画面から、
// 福山通運の伝票CSVに必要な情報を読み取る。
//
// ⭐CSSセレクタではなく**表示テキスト**で拾う。Amazonの画面はReactで組まれていて
//   クラス名や階層は簡単に変わるが、「納品番号：」「納品先：」「輸送箱：」といった
//   文言は業務上の意味を持つので比較的安定している。
// ⭐読めなかったら**必ずエラーにする**。黙って欠けたCSVを作らない
//   (伝票発行は運賃が発生する不可逆な操作)。
// ⭐正規表現は必ず**リテラル**で書く。new RegExp('…\\s…') は生成や転記のたびに
//   エスケープが1段落ちて静かに壊れる (2026-08-08 実際に踏んだ)。

(function (root) {
  'use strict';

  // 全角の英数字を半角へ (コードや数字のゆれを吸収)
  function toHalf(s) {
    return String(s == null ? '' : s).replace(/[Ａ-Ｚａ-ｚ０-９]/g, function (c) {
      return String.fromCharCode(c.charCodeAt(0) - 0xfee0);
    });
  }

  var pad2 = function (n) { return String(n).length < 2 ? '0' + n : String(n); };

  /** 「出荷日：2026年8月7日金曜日」→ '20260807' */
  function parseShipDate(text) {
    var m = toHalf(String(text == null ? '' : text)).match(/出荷日\s*[：:]\s*(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日/);
    return m ? m[1] + pad2(m[2]) + pad2(m[3]) : null;
  }

  /**
   * 画面テキストから納品の一覧を作る。
   * @returns {{ok: boolean, ymd: string|null, shipments: Array, problems: string[]}}
   */
  function parseShipments(text) {
    var problems = [];
    // ⭐入口で英数字だけ半角化する。画面によって FCコード・数字・郵便番号が全角で出ることがあり、
    //   個別のパターンに全角を足していくと漏れる。日本語はそのまま残る。
    var t = toHalf(String(text == null ? '' : text));
    var ymd = parseShipDate(t);
    if (!ymd) problems.push('出荷日が読み取れません');

    // 「納品番号：FBA…」を起点に、次の納品番号までを1ブロックとして扱う
    var re = /納品番号\s*[：:]\s*([A-Z0-9]{6,20})/g;
    var marks = [];
    var m;
    while ((m = re.exec(t)) !== null) marks.push({ id: m[1], at: m.index });

    if (!marks.length) {
      return {
        ok: false,
        ymd: ymd,
        shipments: [],
        problems: ['この画面に納品情報が見つかりません。Seller Central の「ステップ3 – 印刷された輸送箱ラベル」で押してください'],
      };
    }

    var shipments = [];
    for (var i = 0; i < marks.length; i++) {
      var chunk = t.slice(marks[i].at, i + 1 < marks.length ? marks[i + 1].at : t.length);
      var id = marks[i].id;

      // 「納品先：HND2 – 350-1301 Japan 埼玉県狭山市 青柳 915」
      var dst = chunk.match(/納品先\s*[：:]\s*([A-Za-z0-9]{2,8})\s*[–—-]\s*(\d{3}-?\d{4})\s*Japan\s*([^\n]*)/);
      // 「出荷商品: 輸送箱：5, SKU：9, ユニット：387」
      var box = chunk.match(/輸送箱\s*[：:]\s*(\d+)/);

      if (!dst) { problems.push(id + ': 納品先 (FCコード・郵便番号・住所) が読み取れません'); continue; }
      if (!box) { problems.push(id + ': 輸送箱の数が読み取れません'); continue; }

      var n = parseInt(toHalf(box[1]), 10);
      if (!(n >= 1)) { problems.push(id + ': 輸送箱の数が不正です (' + box[1] + ')'); continue; }

      shipments.push({
        shipmentConfirmationId: toHalf(id).toUpperCase(),
        fcCode: toHalf(dst[1]).toUpperCase(),
        boxCount: n,
        // 都道府県・市はこの1本の文字列に含まれているので addressLine1 にまとめて渡す
        address: { postalCode: toHalf(dst[2]), addressLine1: dst[3].replace(/\s+/g, ' ').trim() },
      });
    }

    // 🚨同じ納品番号が2回読めた = 画面の重複表示か読み取りのズレ。
    //   そのままCSVを作ると伝票を二重に発行してしまう (運賃が二重にかかる)
    var seen = {};
    for (var k = 0; k < shipments.length; k++) {
      var id = shipments[k].shipmentConfirmationId;
      if (seen[id]) problems.push(id + ' がこの画面に複数回出ています。伝票を二重に出さないよう確認してください');
      seen[id] = true;
    }

    if (!shipments.length && !problems.length) problems.push('納品が1件も読み取れませんでした');
    return { ok: problems.length === 0 && shipments.length > 0, ymd: ymd, shipments: shipments, problems: problems };
  }

  root.BF_PAGE = { parseShipments: parseShipments, parseShipDate: parseShipDate, toHalf: toHalf };
})(typeof globalThis !== 'undefined' ? globalThis : this);
