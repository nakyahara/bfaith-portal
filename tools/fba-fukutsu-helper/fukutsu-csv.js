/**
 * 福山通運 26列CSV の組み立て — 拡張とサーバで共通の実装。
 *
 * ⭐ここはブラウザ (content script) と Node の両方で読める素のJSにしてある。
 *   `apps/fba-replenishment/fukutsu-csv.js` (サーバ側) と**同じ結果**を出さなければならず、
 *   `apps/fba-replenishment/tests/test-fukutsu-csv-parity.mjs` が両者の一致を検証する。
 *   片方だけ直すとテストが落ちる。
 *
 * ⭐宛先は **常に画面 (Seller Central) に出ている住所をそのまま書く**。
 *   iS-2 のお届け先マスタには頼らない (2026-08-25 中原さんの判断)。
 *   理由: Amazonは新しいFCを開くので、マスタ側のコード一覧を保守し続けると必ずズレる。
 *   Amazonが「ここへ送れ」と表示している住所が正。
 */
(function (root) {
  'use strict';

  /** 付録1-1-1「CSVエントリフォーマット (記事3行)」26列 */
  var HEADER = [
    '荷受人ｺｰﾄﾞ（お届け先ｺｰﾄﾞ）', '電話番号', '住所', '住所２ ', '住所３ ', '名前 ', '名前２ ', '郵便番号 ',
    '特殊計 ', '着店コード ', '荷送人ｺｰﾄﾞ(ご依頼主ｺｰﾄﾞ)', '個数', '才数', '重量',
    '輸送商品１', '輸送商品２', '品名記事１', '品名記事２', '品名記事３', '配達指定日 ',
    'お客様管理番号', '予備', '元払区分', '保険金額', '出荷日付', '登録日付'
  ];

  var COL = {
    荷受人コード: 0, 電話番号: 1, 住所1: 2, 住所2: 3, 住所3: 4, 名前1: 5, 名前2: 6, 郵便番号: 7,
    荷送人コード: 10, 個数: 11, 品名記事1: 16, お客様管理番号: 20, 元払区分: 22, 出荷日付: 24
  };

  var NL = String.fromCharCode(10);
  var SENDER_CODE = '0648607868';   // 荷送人コード (ご依頼主) — 現行GASと同じ固定値
  var AMAZON_TEL = '(06)6333-4858'; // Amazon FC宛の電話番号 (画面に無い唯一の項目。iS-2マスタ74件中70件がこの番号)

  /** YYYYMMDD が実在する日付か (2026年13月40日 のような値を弾く) */
  function isRealYmd(v) {
    var t = String(v == null ? '' : v);
    if (!/^\d{8}$/.test(t)) return false;
    var y = Number(t.slice(0, 4)), m = Number(t.slice(4, 6)), d = Number(t.slice(6, 8));
    var dt = new Date(Date.UTC(y, m - 1, d));
    return dt.getUTCFullYear() === y && dt.getUTCMonth() === m - 1 && dt.getUTCDate() === d;
  }

  function cErr(message) {
    var e = new Error(message);
    e.code = 'FUKUTSU_CSV';
    return e;
  }

  /** 住所を20文字ずつ3つに割る (マニュアル: 各20文字・住所は合計60文字まで) */
  function splitAddress(s) {
    var t = String(s == null ? '' : s).replace(/\s+/g, ' ').trim();
    return [t.slice(0, 20), t.slice(20, 40), t.slice(40, 60)];
  }

  /** 郵便番号は「123-4567」か「1234567」。それ以外は空にして人に気づかせる */
  function normPostal(s) {
    var t = String(s == null ? '' : s).replace(/[^0-9-]/g, '');
    return /^\d{3}-?\d{4}$/.test(t) ? t : '';
  }

  function buildRowsForShipment(shipment, ymd) {
    var fc = String(shipment.fcCode == null ? '' : shipment.fcCode).trim().toUpperCase();
    var kanri = String(shipment.shipmentConfirmationId == null ? '' : shipment.shipmentConfirmationId).trim().toUpperCase();
    var n = Number(shipment.boxCount);

    if (!fc) throw cErr('宛先FCコードが取れませんでした');
    if (!/^[A-Z0-9]{1,16}$/.test(kanri)) throw cErr('納品番号が不正です: ' + kanri);
    if (!(n === Math.floor(n)) || !isFinite(n) || n < 1) throw cErr(kanri + ': 輸送箱の数が取れませんでした');

    // 宛先 = 画面の住所。欠けたまま伝票を出すと届かないので必ず止める
    var a = shipment.address || {};
    var parts = [a.stateOrProvinceCode, a.city, a.addressLine1].filter(Boolean).join('');
    var post = normPostal(a.postalCode);
    if (!post) throw cErr(kanri + ' (' + fc + '): 郵便番号が読み取れません');
    if (parts.replace(/\s/g, '').length < 6) throw cErr(kanri + ' (' + fc + '): 住所が読み取れません');
    if (parts.length > 60) throw cErr(kanri + ' (' + fc + '): 住所が60文字を超えています (' + parts.length + '文字)。切り捨てると届きません');
    var sp = splitAddress(parts);

    var rows = [];
    for (var i = 0; i < n; i++) {
      var c = [];
      for (var j = 0; j < 26; j++) c.push('');
      c[COL.荷受人コード] = fc;
      c[COL.電話番号] = AMAZON_TEL;
      c[COL.住所1] = sp[0]; c[COL.住所2] = sp[1]; c[COL.住所3] = sp[2];
      c[COL.名前1] = ('Amazon.co.jp ' + fc).slice(0, 20);
      c[COL.郵便番号] = post;
      c[COL.荷送人コード] = SENDER_CODE;
      c[COL.個数] = '1';      // 1箱1伝票
      c[COL.元払区分] = '1';   // 元払
      c[COL.出荷日付] = ymd;
      c[COL.お客様管理番号] = kanri; // ⭐追跡番号の突合の鍵
      rows.push(c);
    }
    return { rows: rows, address: parts, postalCode: post };
  }

  function q(v) {
    var s2 = String(v == null ? '' : v);
    var needsQuote = s2.indexOf(',') >= 0 || s2.indexOf('"') >= 0 || s2.indexOf(String.fromCharCode(13)) >= 0 || s2.indexOf(String.fromCharCode(10)) >= 0;
    return needsQuote ? '"' + s2.split('"').join('""') + '"' : s2;
  }

  function buildFukutsuCsv(shipments, ymd) {
    if (!isRealYmd(ymd)) throw cErr('出荷日が不正です: ' + ymd);
    if (!shipments || !shipments.length) throw cErr('対象の納品がありません');

    var all = [], detail = [];
    for (var i = 0; i < shipments.length; i++) {
      var r = buildRowsForShipment(shipments[i], ymd);
      all = all.concat(r.rows);
      detail.push({
        納品番号: shipments[i].shipmentConfirmationId, fcCode: shipments[i].fcCode, 箱数: r.rows.length,
        宛先: '〒' + r.postalCode + ' ' + r.address
      });
    }

    // 現行GASと同じ形: ヘッダー + 空行 + データ (この形で取込に通っている実績がある)
    var lines = [HEADER.map(q).join(','), new Array(26).fill('').join(',')];
    for (var k = 0; k < all.length; k++) lines.push(all[k].map(q).join(','));

    return {
      csv: lines.join(NL) + NL,
      summary: { 出荷日: ymd, 伝票枚数: all.length, 納品数: shipments.length, detail: detail }
    };
  }

  root.BF_FUKUTSU = {
    HEADER: HEADER, COL: COL,
    splitAddress: splitAddress,
    normPostal: normPostal,
    buildRowsForShipment: buildRowsForShipment,
    buildFukutsuCsv: buildFukutsuCsv
  };
})(typeof globalThis !== 'undefined' ? globalThis : this);
