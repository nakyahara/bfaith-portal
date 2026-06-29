/**
 * 商品管理リスト 表示アプリ (⑤b)
 *
 * Render mirror の published snapshot (mirror_pml_published / mirror_pml_snapshot_rows) を
 * 閲覧 + CSV ダウンロードする。データ生成は warehouse (④) → sync (⑤) 済み。
 * 本アプリは read-only・サーバ側で mirror を読む (MIRROR_READ_TOKEN はブラウザに渡さない)。
 *
 *   GET /              … HTML テーブル表示 (status/as_of/鮮度 + 検索)
 *   GET /export.csv    … Shift-JIS CSV (現スプレッドシート互換の列順)
 */
import { Router } from 'express';
import iconv from 'iconv-lite';
import { getMirrorDB } from '../warehouse-mirror/db.js';

const router = Router();

// 出力列順 (現スプレッドシート互換 / mirror_pml_snapshot_rows と一致)
const COLS = [
  '商品コード', '商品名', '仕入先', '取扱区分', '商品区分', '売上分類', '最終仕入日', '在庫保管日数',
  '総在庫数', 'FBA在庫数', 'フリー在庫', '注残数', '引当数', '総在庫数_引当なし',
  '販売数7日_FBA', '販売数7日_FBA以外', '販売数7日_合計',
  '販売数30日_FBA', '販売数30日_FBA以外', '販売数30日_合計',
  '発注ロット単位', '推奨保有月数', '売価', '原価', '想定見込み利益', '概算利益率',
  '代表商品コード', 'ロケーションコード', '商品分類タグ', '登録日',
];

// 表示見出しの差し替え (DB列名・checksum・同期契約は COLS のまま据え置き、見出しテキストのみ別名)
const LABELS = { 'FBA在庫数': 'FBA在庫数(倉庫+入荷待ち)' };
const label = c => LABELS[c] || c;

function loadPublished() {
  const db = getMirrorDB();
  // pub + rows を 1 read transaction で読む。sync 側 atomic swap (DELETE→INSERT→published upsert) と
  // 競合しても「旧 run_id の published metadata + 空/別 run の rows」を返さないようにする (Codex⑤b R1 High)。
  return db.transaction(() => {
    const pub = db.prepare('SELECT * FROM mirror_pml_published WHERE id=1').get();
    if (!pub) return { pub: null, rows: [] };
    const rows = db.prepare(`SELECT ${COLS.join(', ')} FROM mirror_pml_snapshot_rows WHERE run_id=? ORDER BY 商品コード`).all(pub.run_id);
    return { pub, rows };
  })();
}

const he = s => String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
function csvCell(v) {
  const s = v == null ? '' : String(v);
  return /[",\r\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

// ─── CSV ダウンロード (Shift-JIS) ───
router.get('/export.csv', (req, res) => {
  let pub, rows;
  try { ({ pub, rows } = loadPublished()); }
  catch (e) { return res.status(500).send('error: ' + e.message); }
  if (!pub) return res.status(404).send('published snapshot なし');
  const lines = [COLS.map(c => csvCell(label(c))).join(',')];
  for (const r of rows) lines.push(COLS.map(c => csvCell(r[c])).join(','));
  const buf = iconv.encode(lines.join('\r\n') + '\r\n', 'Shift_JIS');
  const fname = `product_management_${pub.as_of_date || pub.run_id}.csv`;
  res.set('Content-Type', 'text/csv; charset=Shift_JIS');
  res.set('Content-Disposition', `attachment; filename="${fname}"`);
  res.set('Cache-Control', 'no-store');
  res.send(buf);
});

// ─── HTML 表示 ───
router.get('/', (req, res) => {
  let pub, rows;
  try { ({ pub, rows } = loadPublished()); }
  catch (e) { return res.status(500).send('error: ' + e.message); }

  const statusBadge = !pub ? '<span class="b b-f">未生成</span>'
    : pub.status === 'ok' ? '<span class="b b-ok">ok</span>'
    : pub.status === 'partial' ? '<span class="b b-p">partial</span>'
    : '<span class="b b-f">failed</span>';

  const wm = pub ? `NE: ${he(pub.src_ne_products_synced_at)} / 販売: ${he(pub.src_velocity_as_of)} / FBA在庫: ${he(pub.src_fba_business_date)} / 発注設定: ${he(pub.src_reorder_updated_at)}` : '';
  const numCols = new Set(['売上分類','在庫保管日数','総在庫数','FBA在庫数','フリー在庫','注残数','引当数','総在庫数_引当なし','販売数7日_FBA','販売数7日_FBA以外','販売数7日_合計','販売数30日_FBA','販売数30日_FBA以外','販売数30日_合計','発注ロット単位','推奨保有月数','売価','原価','想定見込み利益','概算利益率']);

  const head = '<tr>' + COLS.map(c => `<th${numCols.has(c) ? ' class="r"' : ''}>${he(label(c))}</th>`).join('') + '</tr>';
  const body = rows.map(r => '<tr>' + COLS.map(c => `<td${numCols.has(c) ? ' class="r"' : ''}>${he(r[c])}</td>`).join('') + '</tr>').join('');

  res.send(`<!DOCTYPE html><html lang="ja"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>商品管理リスト</title>
<style>
  *{margin:0;padding:0;box-sizing:border-box}
  body{font-family:-apple-system,'Segoe UI',sans-serif;background:#f5f5f5;color:#333;font-size:13px}
  .hd{background:#1a5276;color:#fff;padding:12px 20px;display:flex;align-items:center;gap:16px;flex-wrap:wrap}
  .hd h1{font-size:18px}
  .hd a{color:#aed6f1;text-decoration:none;font-size:12px}
  .bar{padding:10px 20px;background:#fff;border-bottom:1px solid #ddd;display:flex;gap:12px;align-items:center;flex-wrap:wrap}
  .b{padding:2px 10px;border-radius:10px;font-weight:700;font-size:12px}
  .b-ok{background:#d5f5e3;color:#1e8449}.b-p{background:#fdebd0;color:#b9770e}.b-f{background:#fadbd8;color:#c0392b}
  .meta{font-size:11px;color:#777}
  input{padding:6px 10px;border:1px solid #ccc;border-radius:4px;font-size:13px;width:260px}
  .btn{padding:6px 14px;background:#2980b9;color:#fff;border:none;border-radius:4px;cursor:pointer;font-size:13px;text-decoration:none}
  .wrap{margin:12px 20px;overflow:auto;max-height:calc(100vh - 150px);background:#fff;border:1px solid #ddd;border-radius:6px}
  table{border-collapse:collapse;width:100%;font-size:12px;white-space:nowrap}
  th,td{padding:5px 8px;border-bottom:1px solid #eee;text-align:left}
  th{background:#eef3f7;position:sticky;top:0;z-index:1}
  td.r,th.r{text-align:right}
  tr:hover td{background:#f8fbff}
</style></head><body>
<div class="hd"><h1>商品管理リスト</h1><nav><a href="/">← ポータル</a></nav></div>
<div class="bar">
  状態: ${statusBadge}
  <span class="meta">as_of: <b>${he(pub?.as_of_date)}</b> ・ 行数: <b id="cnt">${rows.length}</b> ・ 生成: ${he(pub?.generated_at)} ・ 同期: ${he(pub?.synced_at)}</span>
  <span class="meta">鮮度 [${wm}]${pub && pub.ne_fba_overlap ? ' ・ <b style="color:#c0392b">overlap=' + pub.ne_fba_overlap + '</b>' : ''}</span>
  <input id="q" placeholder="商品コード / 商品名 で絞り込み" oninput="filt()">
  <a class="btn" href="export.csv">CSVダウンロード (Shift-JIS)</a>
</div>
<div class="wrap"><table><thead>${head}</thead><tbody id="tb">${body}</tbody></table></div>
<script>
  function filt(){
    var q=document.getElementById('q').value.trim().toLowerCase();
    var n=0;document.querySelectorAll('#tb tr').forEach(function(tr){
      var c0=tr.cells[0]?tr.cells[0].textContent.toLowerCase():'';
      var c1=tr.cells[1]?tr.cells[1].textContent.toLowerCase():'';
      var show=!q||c0.indexOf(q)>=0||c1.indexOf(q)>=0;
      tr.style.display=show?'':'none';if(show)n++;
    });
    document.getElementById('cnt').textContent=n;
  }
</script>
</body></html>`);
});

export default router;
