/**
 * csv-export.js — 月末棚卸し履歴を CSV(UTF-8 BOM付き) でダウンロード
 *
 * Excel(Windows版/Mac版)で文字化けしないよう UTF-8 BOM を先頭に付ける。
 * 1ファイルに サマリー → 区分別明細 → 発注後未着 を縦に並べる。
 */

const CATEGORY_LABEL = {
  fba_warehouse: 'FBA倉庫内在庫',
  fba_inbound: 'FBA輸送中在庫',
  own_warehouse: '自社倉庫在庫',
  fba_us: '米国FBA在庫',
};

/**
 * RFC 4180 準拠で 1 セルをエスケープ + CSV formula injection 対策。
 *   - `,` `"` 改行 を含む場合は `"..."` で囲み、内部の `"` は `""` に
 *   - `=` `+` `-` `@` 0x09(TAB) 0x0D(CR) で始まる文字列は Excel/Sheets が
 *     数式として実行してしまうため `'` (アポストロフィ) を先頭に付けて
 *     リテラル化する（OWASP 推奨）
 *
 * 注意: 識別子(SKU/商品コード)を Excel で開くと "00123" が 123 に
 * 自動変換される問題は CSV では本質的に防げない。正確な値が必要な場合は
 * 同画面の Excel ダウンロード(xlsx)を使うこと。
 */
const FORMULA_TRIGGER = /^[=+\-@\t\r]/;

function csvCell(v) {
  if (v == null) return '';
  let s = String(v);
  if (FORMULA_TRIGGER.test(s)) {
    s = "'" + s;
  }
  if (/[",\r\n]/.test(s)) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

function row(values) {
  return values.map(csvCell).join(',') + '\r\n';
}

export function buildSnapshotCsv(snap) {
  const s = snap.summary;
  let out = '';

  // ── サマリー ──
  out += row(['区分', '金額（税抜）']);
  out += row(['FBA倉庫内在庫', s.fba_warehouse]);
  out += row(['FBA輸送中在庫', s.fba_inbound]);
  out += row(['自社倉庫在庫', s.own_warehouse]);
  out += row(['米国FBA在庫', s.fba_us]);
  out += row(['発注後未着商品', s.pending_orders]);
  out += row(['合計', s.total]);
  out += row([]);
  out += row(['基準日', s.snapshot_date]);
  out += row(['作成日時', s.created_at]);
  out += row([]);

  // ── 明細（区分毎にブロック） ──
  const grouped = new Map();
  for (const d of snap.details) {
    if (!grouped.has(d.category)) grouped.set(d.category, []);
    grouped.get(d.category).push(d);
  }
  for (const [cat, rows] of grouped) {
    out += row(['【' + (CATEGORY_LABEL[cat] || cat) + '】 明細']);
    out += row(['Amazon SKU', '商品コード', '商品名', '数量', '原価', '金額', '原価状態']);
    for (const d of rows) {
      out += row([
        d.seller_sku || '',
        d.商品コード || '',
        d.商品名 || '',
        d.数量,
        d.原価,
        d.金額,
        d.原価状態 || '',
      ]);
    }
    out += row([]);
  }

  // ── 発注後未着 ──
  if (snap.pending && snap.pending.length > 0) {
    out += row(['【発注後未着商品】']);
    out += row(['仕入先', '金額（税抜）', 'メモ']);
    for (const p of snap.pending) {
      out += row([p.supplier_name, p.amount, p.note || '']);
    }
  }

  // UTF-8 BOM(0xEF 0xBB 0xBF) を先頭に付けて Excel で文字化けを防ぐ
  return Buffer.concat([Buffer.from([0xEF, 0xBB, 0xBF]), Buffer.from(out, 'utf-8')]);
}
