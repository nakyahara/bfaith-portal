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

// ─── miniPC (WarehouseServer) service-api 呼び出し (オンデマンドFBA更新 Part2) ───
const WAREHOUSE_URL = process.env.WAREHOUSE_URL || 'https://wh.bfaith-wh.uk';
function getServiceHeaders() {
  return {
    'CF-Access-Client-Id': process.env.CF_ACCESS_CLIENT_ID || '',
    'CF-Access-Client-Secret': process.env.CF_ACCESS_CLIENT_SECRET || '',
    'Authorization': `Bearer ${process.env.WAREHOUSE_SERVICE_TOKEN || ''}`,
    'Content-Type': 'application/json',
  };
}
async function callWarehouse(fullPath, { method = 'GET', timeout = 30000 } = {}) {
  const requestId = `pml-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
  const res = await fetch(`${WAREHOUSE_URL}${fullPath}`, {
    method, headers: { ...getServiceHeaders(), 'x-request-id': requestId },
    redirect: 'manual', signal: AbortSignal.timeout(timeout),
  });
  if (res.status === 302 || res.status === 303) throw new Error(`CF Access認証構成異常 (${res.status})`);
  if (res.status === 401 || res.status === 403) throw new Error(`認証失敗 HTTP ${res.status}`);
  if (!res.ok) { const t = await res.text().catch(() => ''); throw new Error(`ミニPC HTTP ${res.status}: ${t.slice(0, 200)}`); }
  const ct = res.headers.get('content-type') || '';
  if (!ct.includes('application/json')) throw new Error(`レスポンス形式異常 (ct=${ct || 'none'})`);
  return res.json();
}

function loadPublished() {
  const db = getMirrorDB();
  // pub + rows を 1 read transaction で読む。sync 側 atomic swap (DELETE→INSERT→published upsert) と
  // 競合しても「旧 run_id の published metadata + 空/別 run の rows」を返さないようにする (Codex⑤b R1 High)。
  // 注残数はアプリ発注台帳が正本 (SSoT化 2026-07-13): v_pml_rows_authoritative (=published run + 台帳注残差替。
  // purchase-orders の初期化で作られる正本ビュー) を読む。NE由来の mirror_pml_snapshot_rows.注残数 は legacy
  return db.transaction(() => {
    const pub = db.prepare('SELECT * FROM mirror_pml_published WHERE id=1').get();
    if (!pub) return { pub: null, rows: [], backorderSource: null };
    // 緊急ロールバック (po_settings backorder_source='ne') 時のみNE由来値に戻す
    let src = 'app';
    try { src = db.prepare("SELECT value FROM po_settings WHERE key='backorder_source'").get()?.value || 'app'; } catch { src = 'app'; }
    const rows = src === 'ne'
      ? db.prepare(`SELECT ${COLS.join(', ')} FROM mirror_pml_snapshot_rows WHERE run_id=? ORDER BY 商品コード`).all(pub.run_id)
      : db.prepare(`SELECT ${COLS.join(', ')} FROM v_pml_rows_authoritative ORDER BY 商品コード`).all();
    return { pub, rows, backorderSource: src === 'ne' ? 'ne_legacy' : 'app_ledger' };
  })();
}

const he = s => String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
// ISO(UTC) → JST 'YYYY-MM-DD HH:MM' 表示
function fmtJst(iso) {
  if (!iso) return '—';
  const t = Date.parse(iso);
  if (Number.isNaN(t)) return he(iso);
  const j = new Date(t + 9 * 3600 * 1000);
  const p = n => String(n).padStart(2, '0');
  return `${j.getUTCFullYear()}-${p(j.getUTCMonth() + 1)}-${p(j.getUTCDate())} ${p(j.getUTCHours())}:${p(j.getUTCMinutes())}`;
}
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
  let pub, rows, backorderSource;
  try { ({ pub, rows, backorderSource } = loadPublished()); }
  catch (e) { return res.status(500).send('error: ' + e.message); }

  const statusBadge = !pub ? '<span class="b b-f">未生成</span>'
    : pub.status === 'ok' ? '<span class="b b-ok">ok</span>'
    : pub.status === 'partial' ? '<span class="b b-p">partial</span>'
    : '<span class="b b-f">failed</span>';

  const wm = pub ? `NE: ${he(pub.src_ne_products_synced_at)} / 販売: ${he(pub.src_velocity_as_of)} / FBA在庫: ${he(pub.src_fba_business_date)} / 発注設定: ${he(pub.src_reorder_updated_at)}` : '';
  const boNote = !pub ? ''
    : (backorderSource === 'ne_legacy'
      ? '⚠️ 注残数: NE由来 (緊急ロールバック中 backorder_source=ne)'
      : '注残数: <b>発注アプリの台帳</b> (リアルタイム。NE由来の注残は2026-07-13で更新停止=legacy)');
  // FBA在庫の鮮度 (live=オンデマンド最新 / daily=朝の日次)。「総在庫数」は自社(朝)+FBAの混在である旨も明示。
  const fbaFresh = !pub ? ''
    : pub.fba_source_kind === 'live'
      ? `🟢 FBA在庫: <b>${fmtJst(pub.fba_fetched_at)} 取得(最新)</b> ・ 自社在庫・販売数: 朝同期時点 → <b>総在庫数=自社(朝)+FBA(最新)</b>`
      : `FBA在庫: ${he(pub.src_fba_business_date)}(朝同期) ・ 自社在庫・販売数: 朝同期時点`;
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
  <button class="btn" id="refreshBtn" onclick="refreshFba()" style="background:#16a085">FBA在庫を今すぐ更新</button>
  <span class="meta" id="refreshStat"></span>
</div>
<div class="bar" style="padding-top:0;border-bottom:1px solid #ddd">
  <span class="meta">${fbaFresh}${boNote ? ' ・ ' + boNote : ''}</span>
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
  var pollTimer=null;
  function setStat(msg,color){var el=document.getElementById('refreshStat');el.textContent=msg;el.style.color=color||'#777';}
  function refreshFba(){
    var btn=document.getElementById('refreshBtn');
    if(btn.disabled)return;
    btn.disabled=true;setStat('起動中…','#b9770e');
    fetch('api/refresh-fba',{method:'POST'}).then(function(x){return x.json();}).then(function(r){
      if(r.error){setStat('失敗: '+r.error,'#c0392b');btn.disabled=false;return;}
      if(!r.jobId){setStat(r.message||'実行中です','#b9770e');btn.disabled=false;return;}
      setStat('更新中… AmazonからRESTOCK取得中(数分かかります)','#b9770e');
      poll(r.jobId);
    }).catch(function(e){setStat('失敗: '+e,'#c0392b');btn.disabled=false;});
  }
  function poll(jobId){
    if(pollTimer)clearTimeout(pollTimer);
    pollTimer=setTimeout(function(){
      fetch('api/refresh-fba/jobs/'+encodeURIComponent(jobId)).then(function(x){return x.json();}).then(function(j){
        var st=j.status;
        var prog=(j.progress&&j.progress.message)||'';
        if(st==='completed'){setStat('✅ 更新完了。再読み込みします…','#1e8449');setTimeout(function(){location.reload();},1500);return;}
        if(st==='failed'){setStat('❌ 失敗: '+(j.error||prog||'unknown'),'#c0392b');document.getElementById('refreshBtn').disabled=false;return;}
        setStat('更新中… '+(prog||'処理中')+' (数分かかります)','#b9770e');
        poll(jobId);
      }).catch(function(e){setStat('状態取得エラー: '+e,'#c0392b');document.getElementById('refreshBtn').disabled=false;});
    },5000);
  }
</script>
</body></html>`);
});

// ─── オンデマンドFBA更新 (Part2) 起動 + ジョブ状態プロキシ ───
// 「FBA在庫を今すぐ更新」ボタン → miniPC で RESTOCK取得→build(live)→pml-only同期 を1ジョブ実行。
// SP-API のレポート生成が非同期で数分かかるため、jobId を返し画面側でポーリングする。
router.post('/api/refresh-fba', async (req, res) => {
  try {
    const r = await callWarehouse('/service-api/fba/pml/fba-refresh', { method: 'POST', timeout: 30000 });
    res.json(r);
  } catch (e) {
    res.status(502).json({ error: 'ミニPCへの接続に失敗: ' + e.message });
  }
});
router.get('/api/refresh-fba/jobs/:jobId', async (req, res) => {
  try {
    const r = await callWarehouse(`/service-api/jobs/${encodeURIComponent(req.params.jobId)}`, { timeout: 15000 });
    res.json(r);
  } catch (e) {
    res.status(502).json({ error: 'ジョブ状態の取得に失敗: ' + e.message });
  }
});

export default router;
