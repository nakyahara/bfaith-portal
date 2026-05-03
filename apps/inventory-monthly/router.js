/**
 * inventory-monthly/router.js — 月末棚卸しツール
 *
 * 画面:
 *   GET  /                - 入力画面（CSV3種アップロード + 米国FBA金額 + 発注後未着）
 *   POST /aggregate       - 集計実行（保存はしない）
 *   POST /save            - 集計結果を inv_snapshot に保存
 *   GET  /history         - 履歴一覧
 *   GET  /history/:id     - 履歴詳細
 *   GET  /export/:id.xlsx - Excelダウンロード
 *
 * 仕様: g:\共有ドライブ\AI_reference\システム設計\月末棚卸しツール_仕様書.md
 */
import { Router } from 'express';
import multer from 'multer';
import fs from 'fs';
import { initInventoryMonthly, getDB } from './db.js';
import { parseRestockReport, parseOwnWarehouse } from './csv-parser.js';
import { aggregateInventory, saveSnapshot, listSnapshots, getSnapshot } from './aggregator.js';
import { exportSnapshotToXlsx } from './excel-export.js';
import { buildSnapshotCsv } from './csv-export.js';

const router = Router();
const UPLOAD_DIR = process.env.DATA_DIR ? process.env.DATA_DIR + '/import' : 'data/import';
if (!fs.existsSync(UPLOAD_DIR)) { try { fs.mkdirSync(UPLOAD_DIR, { recursive: true }); } catch {} }
const upload = multer({ dest: UPLOAD_DIR, limits: { fileSize: 50 * 1024 * 1024 } });

/** ミラーDB依存の処理は遅延初期化する。失敗時はそのリクエストだけ 503 を返す。 */
function ensureDbOrFail(res) {
  try {
    initInventoryMonthly();
    return true;
  } catch (e) {
    res.status(503).json({ error: 'mirror DB が利用できません: ' + e.message });
    return false;
  }
}
function ensureDbOrFailHtml(res) {
  try {
    initInventoryMonthly();
    return true;
  } catch (e) {
    res.status(503).send(renderLayout('mirror DB エラー', `<div class="err">mirror DB が利用できません: ${esc(e.message)}</div>`));
    return false;
  }
}

// ───────── HTML helpers ─────────

const yen = n => '¥' + (Math.round(Number(n) || 0)).toLocaleString('ja-JP');
const esc = s => String(s ?? '').replace(/[&<>"']/g, c => ({
  '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'
}[c]));

function renderLayout(title, body) {
  // <base> を絶対パスで固定することで `/apps/inventory-monthly`（末尾スラッシュなし）
  // でアクセスされても、fetch('aggregate') や href="history" が
  // 期待通り `/apps/inventory-monthly/...` に解決される。
  return `<!doctype html>
<html lang="ja">
<head>
<meta charset="utf-8">
<base href="/apps/inventory-monthly/">
<title>${esc(title)}</title>
<style>
  body { font-family: -apple-system, "Hiragino Sans", "Meiryo", sans-serif; max-width: 1100px; margin: 24px auto; padding: 0 16px; color: #222; }
  h1 { font-size: 22px; border-bottom: 2px solid #2c5aa0; padding-bottom: 8px; }
  h2 { font-size: 16px; margin-top: 28px; color: #2c5aa0; }
  .nav a { margin-right: 12px; color: #2c5aa0; text-decoration: none; }
  .card { background: #f8f9fa; border: 1px solid #dee2e6; border-radius: 6px; padding: 14px 16px; margin: 10px 0; }
  label { display: block; font-weight: bold; margin-bottom: 4px; font-size: 13px; }
  input[type=file], input[type=date], input[type=number], input[type=text] { padding: 6px 8px; border: 1px solid #ccc; border-radius: 4px; font-size: 14px; }
  button { background: #2c5aa0; color: white; border: 0; padding: 8px 18px; border-radius: 4px; font-size: 14px; cursor: pointer; }
  button.secondary { background: #6c757d; }
  table { border-collapse: collapse; width: 100%; margin: 8px 0; font-size: 13px; }
  th, td { border: 1px solid #dee2e6; padding: 6px 10px; text-align: left; }
  th { background: #e9ecef; }
  td.num { text-align: right; font-variant-numeric: tabular-nums; }
  .total-row td { background: #fff3cd; font-weight: bold; }
  .warn { background: #fff3cd; border: 1px solid #ffc107; padding: 10px; border-radius: 4px; margin: 12px 0; }
  .err { background: #f8d7da; border: 1px solid #f5c2c7; padding: 10px; border-radius: 4px; margin: 12px 0; }
  .pending-row { display: flex; gap: 8px; margin: 4px 0; align-items: center; }
  .pending-row input[type=text] { flex: 2; }
  .pending-row input[type=number] { flex: 1; }
  small.hint { color: #666; font-weight: normal; }
</style>
</head>
<body>
<h1>📦 月末棚卸しツール</h1>
<div class="nav"><a href=".">入力</a> <a href="history">履歴</a> <a href="daily">日次推移</a> <a href="/">ポータルへ戻る</a></div>
${body}
</body>
</html>`;
}

function renderInputPage() {
  // 当月末をJST(Asia/Tokyo)で計算する。
  // 本番コンテナ(Render等)は TZ 未設定だと UTC で動くため、`new Date()` の
  // ローカル取得だと JST 月初 0:00〜9:00 の間に「先月」と誤判定して
  // 月末日が1ヶ月ズレる。Intl で JST の年月日を直接取り出して回避する。
  const fmt = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Tokyo',
    year: 'numeric', month: '2-digit', day: '2-digit',
  });
  const parts = Object.fromEntries(fmt.formatToParts(new Date()).map(p => [p.type, p.value]));
  const jstYear = Number(parts.year);
  const jstMonth = Number(parts.month); // 1-12
  // JSTでの当月末日: その月の翌月0日 = 月末
  const lastDay = new Date(Date.UTC(jstYear, jstMonth, 0)).getUTCDate();
  const pad2 = n => String(n).padStart(2, '0');
  const monthEnd = `${jstYear}-${pad2(jstMonth)}-${pad2(lastDay)}`;

  return renderLayout('月末棚卸し - 入力', `
<form id="frm" enctype="multipart/form-data">
  <div class="card">
    <label>棚卸し基準日</label>
    <input type="date" name="snapshot_date" value="${monthEnd}" required>
  </div>

  <div class="card">
    <label>① 国内FBA在庫（発注推奨レポートCSV / Shift_JIS）</label>
    <input type="file" name="fba_csv" accept=".csv" required>
    <small class="hint">Seller Central → 在庫レポート → 「補充」→ 発注推奨レポート の CSV をそのままアップロード<br>
    集計内訳: <b>FBA倉庫内</b> = 在庫あり + FC移管中 + FC処理中 + 出荷待ち / <b>FBA輸送中</b> = 進行中 + 出荷済み + 受領中（販売不可は除外）</small>
  </div>

  <div class="card">
    <label>② 自社倉庫CSV（NextEngine: jishazaikotanaorosi.csv）</label>
    <input type="file" name="own_csv" accept=".csv" required>
    <small class="hint">A列=商品コード、E列=在庫数。原価はマスタから引くため、CSVの原価は使いません。</small>
  </div>

  <div class="card">
    <label>③ 米国FBA在庫</label>
    <div style="margin-bottom:6px">
      <label style="display:inline; font-weight:normal">
        <input type="radio" name="us_mode" value="amount" checked onchange="toggleUsMode()"> 直接入力（円・税抜）
      </label>
      <input type="number" name="us_fba_amount" id="us_amount_input" min="0" step="1" value="0" style="margin-left:8px">
    </div>
    <div>
      <label style="display:inline; font-weight:normal">
        <input type="radio" name="us_mode" value="csv" onchange="toggleUsMode()"> CSVから自動算出
      </label>
      <input type="file" name="us_fba_csv" id="us_csv_input" accept=".csv" disabled style="margin-left:8px">
      <small class="hint">米国版「発注推奨レポート」CSV (Restock Inventory Recommendations Report)。<br>
      JP原価マスタで円換算します（米国独自の原価マスタは現状なし）。</small>
    </div>
  </div>

  <div class="card">
    <label>④ 発注後未着商品 <small class="hint">仕入先名+税抜金額。同月内に請求書計上済みだが未着の在庫</small></label>
    <div id="pendingRows">
      <div class="pending-row">
        <input type="text" name="pending_supplier[]" placeholder="仕入先名">
        <input type="number" name="pending_amount[]" placeholder="金額（税抜）" min="0" step="1">
        <button type="button" class="secondary" onclick="this.parentElement.remove()">削除</button>
      </div>
    </div>
    <button type="button" class="secondary" onclick="addPending()">+ 行を追加</button>
  </div>

  <div style="display:flex;gap:12px;align-items:center;margin-top:10px">
    <button type="submit" data-save="0">プレビュー集計</button>
    <button type="submit" data-save="1" style="background:#198754">集計して履歴に保存</button>
    <small class="hint">保存ボタンは同じCSVをサーバー側で再集計してから保存します（クライアント結果は信頼しません）</small>
  </div>
</form>

<div id="result"></div>

<script>
// ③ 米国FBA: 「直接入力」と「CSV取込」の入力欄を排他で活性化する。
// 非アクティブ側を disabled にしておくことで FormData に値が乗らず、
// サーバー側で「どっちが選ばれたか」を req.body / req.files の有無だけで判定できる。
function toggleUsMode() {
  const mode = document.querySelector('input[name="us_mode"]:checked').value;
  document.getElementById('us_amount_input').disabled = (mode !== 'amount');
  document.getElementById('us_csv_input').disabled = (mode !== 'csv');
}

function addPending() {
  const div = document.createElement('div');
  div.className = 'pending-row';
  div.innerHTML = '<input type="text" name="pending_supplier[]" placeholder="仕入先名"><input type="number" name="pending_amount[]" placeholder="金額（税抜）" min="0" step="1"><button type="button" class="secondary" onclick="this.parentElement.remove()">削除</button>';
  document.getElementById('pendingRows').appendChild(div);
}

let saveFlag = '0';
let lastResult = null; // 直前の集計結果。CSVダウンロードボタンから参照する。
document.querySelectorAll('#frm button[type=submit]').forEach(btn => {
  btn.addEventListener('click', () => { saveFlag = btn.dataset.save || '0'; });
});

document.getElementById('frm').addEventListener('submit', async (e) => {
  e.preventDefault();
  const result = document.getElementById('result');
  result.innerHTML = '<div class="card">' + (saveFlag === '1' ? '集計+保存中...' : '集計中...') + '</div>';
  const fd = new FormData(e.target);
  fd.set('save', saveFlag);
  try {
    const res = await fetch('aggregate', {
      method: 'POST',
      body: fd,
      // requireAppAccess に「これは API 呼び出しなのでログイン画面 HTML を返さず
      // 401 JSON にしてほしい」と伝える。Accept がないと旧来通り /login への
      // リダイレクト HTML が返ってきて res.json() が JSON parse error になる。
      headers: { 'Accept': 'application/json' },
    });
    if (res.status === 401 || res.status === 403) {
      result.innerHTML = '<div class="err">認証が切れました。<a href="/login">再ログイン</a>してから再実行してください。</div>';
      return;
    }
    const data = await res.json();
    if (!res.ok) {
      result.innerHTML = '<div class="err">' + (data.error || 'エラー') + '</div>';
      return;
    }
    lastResult = data;
    result.innerHTML = renderResult(data);
  } catch (err) {
    result.innerHTML = '<div class="err">' + err.message + '</div>';
  }
});

function yen(n) { return '¥' + Math.round(Number(n)||0).toLocaleString('ja-JP'); }
// クライアント側 HTML エスケープ。CSV由来の SKU/商品コード文字列は全てここを通す。
function esc(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g, c => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
  }[c]));
}

// ─── クライアント側 CSV 生成 (formula injection 対策付き) ───
//
// サーバーの csv-export.js と同じ規則で生成する:
//   - カンマ・ダブルクオート・改行 を含む値はダブルクオートで囲み、
//     内部のダブルクオートは2つに重ねる
//   - = + - @ TAB CR で始まる値は Excel/Sheets が数式として
//     実行してしまうためアポストロフィを前置してリテラル化
function csvCell(v) {
  if (v == null) return '';
  let s = String(v);
  if (/^[=+\\-@\\t\\r]/.test(s)) s = "'" + s;
  if (/[",\\r\\n]/.test(s)) return '"' + s.replace(/"/g, '""') + '"';
  return s;
}
function rowsToCsv(rows) {
  return rows.map(r => r.map(csvCell).join(',')).join('\\r\\n') + '\\r\\n';
}
function downloadCsv(filename, content) {
  const blob = new Blob(['\\uFEFF' + content], { type: 'text/csv;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

const CATEGORY_LABEL = {
  fba_warehouse: 'FBA倉庫内在庫',
  fba_inbound: 'FBA輸送中在庫',
  own_warehouse: '自社倉庫在庫',
  fba_us: '米国FBA在庫',
};
const INCOMPLETE_COST_STATUSES = ['MISSING','PARTIAL','PARTIAL_SET','NOT_IN_MASTER'];

function dlSummary() {
  if (!lastResult) return;
  const t = lastResult.totals;
  const rows = [
    ['区分', '金額（税抜）'],
    ['FBA倉庫内在庫', t.fba_warehouse],
    ['FBA輸送中在庫', t.fba_inbound],
    ['自社倉庫在庫', t.own_warehouse],
    ['米国FBA在庫', t.fba_us],
    ['発注後未着商品', t.pending],
    ['合計', t.total],
  ];
  downloadCsv('月末棚卸し_サマリー.csv', rowsToCsv(rows));
}
function dlDetails() {
  if (!lastResult) return;
  const rows = [['区分','Amazon SKU','商品コード','商品名','数量','原価','金額','原価状態']];
  for (const x of (lastResult.details || [])) {
    rows.push([CATEGORY_LABEL[x.category] || x.category, x.seller_sku || '', x.商品コード || '', x.商品名 || '', x.数量, x.原価, x.金額, x.原価状態 || '']);
  }
  downloadCsv('月末棚卸し_明細.csv', rowsToCsv(rows));
}
function dlUnmapped() {
  if (!lastResult) return;
  // details から UNMAPPED_SKU 行を抽出して文脈付きで出す
  const rows = [['区分','Amazon SKU','商品名','数量']];
  const seen = new Set();
  for (const x of (lastResult.details || [])) {
    if (x.原価状態 !== 'UNMAPPED_SKU') continue;
    const key = (x.category || '') + '|' + (x.seller_sku || '');
    if (seen.has(key)) continue;
    seen.add(key);
    rows.push([CATEGORY_LABEL[x.category] || x.category, x.seller_sku || '', x.商品名 || '', x.数量]);
  }
  downloadCsv('未マップSKU.csv', rowsToCsv(rows));
}
function dlMissingCost() {
  if (!lastResult) return;
  const rows = [['区分','Amazon SKU','商品コード','商品名','数量','原価状態']];
  const seen = new Set();
  for (const x of (lastResult.details || [])) {
    if (!INCOMPLETE_COST_STATUSES.includes(x.原価状態)) continue;
    const key = (x.category || '') + '|' + (x.seller_sku || '') + '|' + (x.商品コード || '');
    if (seen.has(key)) continue;
    seen.add(key);
    rows.push([CATEGORY_LABEL[x.category] || x.category, x.seller_sku || '', x.商品コード || '', x.商品名 || '', x.数量, x.原価状態 || '']);
  }
  downloadCsv('原価未登録.csv', rowsToCsv(rows));
}

function renderResult(d) {
  const t = d.totals;
  const rows = [
    ['FBA倉庫内在庫', t.fba_warehouse],
    ['FBA輸送中在庫', t.fba_inbound],
    ['自社倉庫在庫',   t.own_warehouse],
    ['米国FBA在庫',    t.fba_us],
    ['発注後未着商品', t.pending],
  ].map(([k,v]) => '<tr><td>'+esc(k)+'</td><td class="num">'+yen(v)+'</td></tr>').join('');

  // ダウンロードボタン群（集計結果直下）
  const summaryDl = '<p style="margin:8px 0">'
    + '<button type="button" class="secondary" onclick="dlSummary()">📥 サマリーCSV</button> '
    + '<button type="button" class="secondary" onclick="dlDetails()">📥 明細CSV (商品ごと)</button>'
    + '</p>';

  let warnHtml = '';
  const w = d.warnings;
  const showList = (label, arr, btnLabel, onclick) => {
    if (!arr.length) return '';
    const dlBtn = btnLabel ? ' <button type="button" class="secondary" onclick="'+onclick+'">📥 '+esc(btnLabel)+'</button>' : '';
    return '<div class="warn"><b>'+esc(label)+': '+arr.length+'件</b>'+dlBtn
      + '<br><small>'+arr.slice(0,30).map(esc).join(', ')+(arr.length>30?' ...':'')+'</small></div>';
  };
  warnHtml += showList('Amazon SKU → NE商品コード未マップ', w.unmappedSkus, '未マップSKU CSV', 'dlUnmapped()');
  warnHtml += showList('原価未登録の商品コード', w.missingCost, '原価未登録CSV', 'dlMissingCost()');

  const savedHtml = d.snapshot_id ? '<div class="card" style="background:#d1e7dd;border-color:#a3cfbb"><b>履歴に保存しました</b> → <a href="history/'+encodeURIComponent(d.snapshot_id)+'">詳細を表示</a></div>' : '';

  return '<h2>集計結果</h2>'
    + savedHtml
    + '<table><thead><tr><th>区分</th><th>金額（税抜）</th></tr></thead><tbody>'
    + rows
    + '<tr class="total-row"><td>合計</td><td class="num">'+yen(t.total)+'</td></tr>'
    + '</tbody></table>'
    + summaryDl
    + warnHtml;
}
</script>
`);
}

// ───────── routes ─────────

router.get('/', (req, res) => {
  res.send(renderInputPage());
});

/**
 * /aggregate
 *   常に CSV を再パース→サーバー側集計する。クライアントから集計結果JSONを
 *   受け取って保存することは絶対にしない（改ざん防止）。
 *   `save=1` が付いていれば同一トランザクションで履歴に保存し snapshot_id を返す。
 */
router.post('/aggregate', upload.fields([
  { name: 'fba_csv', maxCount: 1 },
  { name: 'own_csv', maxCount: 1 },
  { name: 'us_fba_csv', maxCount: 1 },
]), (req, res) => {
  // multer が書き出した一時ファイルはどの分岐を通っても必ず削除する。
  // 早期 return / 例外 / DB エラーいずれの場合も orphan を残さないため、
  // 関数の入口で cleanup を定義し、return/throw 直前に呼び出す。
  const cleanup = () => {
    try { if (req.files?.fba_csv?.[0]?.path) fs.unlinkSync(req.files.fba_csv[0].path); } catch {}
    try { if (req.files?.own_csv?.[0]?.path) fs.unlinkSync(req.files.own_csv[0].path); } catch {}
    try { if (req.files?.us_fba_csv?.[0]?.path) fs.unlinkSync(req.files.us_fba_csv[0].path); } catch {}
  };

  try {
    const wantSave = req.body.save === '1' || req.body.save === 'true';

    const snapshot_date = (req.body.snapshot_date || '').slice(0, 10);
    if (!/^\d{4}-\d{2}-\d{2}$/.test(snapshot_date)) {
      cleanup();
      return res.status(400).json({ error: '棚卸し基準日が不正です' });
    }

    const fbaFile = req.files?.fba_csv?.[0];
    const ownFile = req.files?.own_csv?.[0];
    if (!fbaFile || !ownFile) {
      cleanup();
      return res.status(400).json({ error: '①FBA CSV と ②自社倉庫CSV は必須です' });
    }

    // パースは DB 不要。ここまでで CSV を読み切って一時ファイルを消す。
    const fbaBuf = fs.readFileSync(fbaFile.path);
    const ownBuf = fs.readFileSync(ownFile.path);
    // 米国FBA CSV は任意（直接入力モードなら無し）
    const usFbaFile = req.files?.us_fba_csv?.[0];
    const usFbaBuf = usFbaFile ? fs.readFileSync(usFbaFile.path) : null;
    cleanup();

    const fbaRows = parseRestockReport(fbaBuf);
    const ownRows = parseOwnWarehouse(ownBuf);
    // ③ 米国FBA: CSVが来ていればパースしてaggregatorに渡す（金額直入力より優先）
    const usFbaRows = usFbaBuf ? parseRestockReport(usFbaBuf) : null;
    const usFbaAmount = usFbaRows ? 0 : (Number(req.body.us_fba_amount) || 0);

    // multer (append-field) は HTML name="pending_supplier[]" を見ると
    // 末尾の `[]` を「配列appendマーカー」として解釈し、req.body には
    // 角括弧なしの `pending_supplier` キーで配列として格納する。
    // 旧実装は `req.body['pending_supplier[]']` を読んでいて常にundefinedとなり、
    // 発注後未着の入力が無言で 0 円扱いされる事故を起こしていた。
    const suppliers = [].concat(req.body.pending_supplier || []);
    const amounts = [].concat(req.body.pending_amount || []);
    const pendingRows = suppliers
      .map((s, i) => ({ supplier_name: (s || '').trim(), amount: Number(amounts[i]) || 0 }))
      .filter(p => p.supplier_name && p.amount > 0);

    // 集計と保存はどちらも mirror DB が必要。
    if (!ensureDbOrFail(res)) return;
    const result = aggregateInventory({ fbaRows, ownRows, usFbaRows, usFbaAmount, pendingRows });

    let snapshot_id = null;
    if (wantSave) {
      snapshot_id = saveSnapshot({ snapshot_date, result, pendingRows });
    }
    res.json({ ...result, snapshot_id });
  } catch (e) {
    cleanup();
    res.status(500).json({ error: e.message });
  }
});

router.get('/history', (req, res) => {
  if (!ensureDbOrFailHtml(res)) return;
  const list = listSnapshots(); // snapshot_date DESC
  const rows = list.map(s => `
    <tr>
      <td><a href="history/${s.id}">${esc(s.snapshot_date)}</a></td>
      <td class="num">${yen(s.fba_warehouse)}</td>
      <td class="num">${yen(s.fba_inbound)}</td>
      <td class="num">${yen(s.own_warehouse)}</td>
      <td class="num">${yen(s.fba_us)}</td>
      <td class="num">${yen(s.pending_orders)}</td>
      <td class="num"><b>${yen(s.total)}</b></td>
      <td>${esc(s.created_at)}</td>
    </tr>`).join('');

  // チャート用データ (古い順、Chart.js が期待する order)
  const chartData = [...list].reverse().map(s => ({
    date: s.snapshot_date,
    fba_warehouse: s.fba_warehouse || 0,
    fba_inbound: s.fba_inbound || 0,
    own_warehouse: s.own_warehouse || 0,
    fba_us: s.fba_us || 0,
    pending_orders: s.pending_orders || 0,
    total: s.total || 0,
  }));
  const chartJson = JSON.stringify(chartData);

  res.send(renderLayout('月末棚卸し - 履歴', `
<h2>月末在庫推移 (積み上げ面グラフ)</h2>
${list.length === 0 ? '<p>まだ履歴がありません。</p>' : `
<div class="card">
  <div style="position:relative;height:380px"><canvas id="histChart"></canvas></div>
</div>

<h2>履歴一覧</h2>
<table>
  <thead><tr><th>基準日</th><th>FBA倉庫</th><th>FBA輸送中</th><th>自社倉庫</th><th>米国FBA</th><th>発注後未着</th><th>合計</th><th>作成日時</th></tr></thead>
  <tbody>${rows}</tbody>
</table>

<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<script>
  const data = ${chartJson};
  const labels = data.map(d => d.date);
  const palette = {
    fba_warehouse: '#2c5aa0',  // 紺
    fba_inbound:   '#5b9bd5',  // 水色
    own_warehouse: '#70ad47',  // 緑 (一番大きいので分かりやすい色)
    fba_us:        '#ed7d31',  // オレンジ
    pending_orders:'#a5a5a5',  // グレー
  };
  const labelMap = {
    fba_warehouse: 'FBA倉庫',
    fba_inbound:   'FBA輸送中',
    own_warehouse: '自社倉庫',
    fba_us:        '米国FBA',
    pending_orders:'発注後未着',
  };
  const datasets = Object.keys(palette).map(key => ({
    label: labelMap[key],
    data: data.map(d => d[key]),
    backgroundColor: palette[key],
    borderColor: palette[key],
    fill: true,
    tension: 0.2,
    pointRadius: 2,
  }));
  new Chart(document.getElementById('histChart').getContext('2d'), {
    type: 'line',
    data: { labels, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      scales: {
        y: {
          stacked: true,
          ticks: { callback: v => '¥' + (v/1000000).toFixed(0) + 'M' }
        },
        x: { ticks: { maxRotation: 60, minRotation: 0 } }
      },
      plugins: {
        tooltip: {
          callbacks: {
            label: ctx => {
              const v = ctx.parsed.y;
              return ctx.dataset.label + ': ¥' + Math.round(v).toLocaleString();
            },
            footer: items => {
              const total = items.reduce((s,i) => s + i.parsed.y, 0);
              return '合計: ¥' + Math.round(total).toLocaleString();
            }
          }
        },
        legend: { position: 'bottom' }
      }
    }
  });
</script>
`}
`));
});

// 過去月末データの一括投入 (CSV 由来、22ヶ月分、ハードコード)
// 既に同 snapshot_date 行があれば skip (再実行安全)
// マッピング:
//   fba_warehouse = FBA倉庫_10% + 8%
//   fba_inbound   = FBA輸送中_10 + 8 + FBA輸送準備(NE伝票)_10 + 8
//   own_warehouse = 自社倉庫_10 + 8
//   fba_us        = 米国FBA_10 + 8 + 米国輸送中FBA_10 + 8
//   pending_orders = 注文後未着_10 + 8
// 1回叩いたら不要 (idempotent なので残しても害なし)
const HISTORICAL_MONTHLY_TOTALS = [
  // [snapshot_date, fba_warehouse, fba_inbound, own_warehouse, fba_us, pending_orders]
  ['2024-06-30', 27941026, 1617028,  107875351, 1230210, 1022768],
  ['2024-07-31', 29860090, 2698085,  127544915, 590700,  948980],
  ['2024-08-31', 28798532, 410000,   110980186, 906670,  5577920],
  ['2024-09-30', 25758563, 1328114,  135212880, 1180450, 2129360],
  ['2024-10-31', 28278927, 3224697,  138298610, 565450,  1450240],
  ['2024-11-30', 31504594, 1896085,  141845968, 1887290, 595824],
  ['2024-12-31', 30107046, 3583283,  151336505, 1887290, 694806],
  ['2025-01-31', 30631119, 3168067,  155347944, 1388290, 763514],
  ['2025-02-28', 31161585, 2485995,  148618022, 1087160, 6876942],
  ['2025-03-31', 35441316, 3821868,  152969657, 571130,  1699590],
  ['2025-04-30', 33203896, 4136514,  149258238, 1417910, 2455944],
  ['2025-05-31', 33928211, 4201766,  152211457, 345040,  892906],
  ['2025-06-30', 31353128, 1970022,  160807890, 241370,  754374],
  ['2025-07-31', 30856674, 3535892,  169226038, 241370,  2557036],
  ['2025-08-31', 30990773, 3969203,  165937904, 241370,  999810],
  ['2025-09-30', 29231979, 2828446,  166014821, 36250,   2401460],
  ['2025-10-31', 29085016, 1399703,  172743169, 25200,   2400911],
  ['2025-11-30', 26629950, 3223740,  173146430, 1280800, 251680],
  ['2025-12-31', 27865104, 1205080,  173279939, 1200200, 211680],
  ['2026-01-31', 31612880, 15941,    171713024, 1106000, 1998510],
  ['2026-02-28', 28068812, 367960,   179659945, 918750,  534220],
  ['2026-03-31', 27665214, 5575553,  175396635, 699100,  5251184],
];

// GET: 確認ページ (ボタン付き)。ブラウザで直接開いて実行できる。
router.get('/admin/import-historical', (req, res) => {
  if (!ensureDbOrFailHtml(res)) return;
  const db = getDB();
  const placeholders = HISTORICAL_MONTHLY_TOTALS.map(() => '?').join(',');
  const existing = db.prepare(
    `SELECT snapshot_date FROM inv_snapshot WHERE snapshot_date IN (${placeholders})`
  ).all(...HISTORICAL_MONTHLY_TOTALS.map(([d]) => d));
  const existingDates = new Set(existing.map(r => r.snapshot_date));
  const pendingList = HISTORICAL_MONTHLY_TOTALS.filter(([d]) => !existingDates.has(d));
  res.send(renderLayout('過去履歴インポート', `
<h2>過去22ヶ月分の月末在庫データを inv_snapshot に投入</h2>
<div class="card">
  <p><b>対象</b>: ${HISTORICAL_MONTHLY_TOTALS.length}ヶ月 (2024-06 〜 2026-03)</p>
  <p><b>既存</b>: ${existing.length}件 (skip)</p>
  <p><b>投入予定</b>: ${pendingList.length}件</p>
</div>
${pendingList.length === 0 ? '<div class="warn">既に全データ投入済み。実行しても skip されるだけ。</div>' : `
<form method="POST" action="admin/import-historical">
  <button type="submit">${pendingList.length}件を投入する</button>
</form>
`}
<p><a href="history">← 履歴ページへ</a></p>
  `));
});

router.post('/admin/import-historical', (req, res) => {
  if (!ensureDbOrFail(res)) return;
  const db = getDB();
  const now = new Date().toISOString().replace('T', ' ').slice(0, 19);
  const note = 'historical-import (CSV取込、税抜10%+8%合算、輸送準備含む)';

  const checkStmt = db.prepare('SELECT id FROM inv_snapshot WHERE snapshot_date = ?');
  const insStmt = db.prepare(`
    INSERT INTO inv_snapshot
      (snapshot_date, fba_warehouse, fba_inbound, own_warehouse, fba_us, pending_orders, total, note, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  let inserted = 0;
  const skipped = [];
  const tx = db.transaction(() => {
    for (const [date, fw, fi, ow, fu, po] of HISTORICAL_MONTHLY_TOTALS) {
      if (checkStmt.get(date)) {
        skipped.push(date);
        continue;
      }
      const total = fw + fi + ow + fu + po;
      insStmt.run(date, fw, fi, ow, fu, po, total, note, now);
      inserted++;
    }
  });
  tx();

  // ブラウザ form submit (HTML 期待) なら結果ページを描画。API 呼び出し (JSON Accept) なら JSON。
  if (req.accepts(['html', 'json']) === 'html') {
    return res.send(renderLayout('過去履歴インポート - 完了', `
<div class="card" style="background:#d1e7dd;border-color:#a3cfbb">
  <h2>✅ 完了</h2>
  <p><b>新規投入</b>: ${inserted}件</p>
  <p><b>スキップ</b> (既存): ${skipped.length}件</p>
  <p><b>合計対象</b>: ${HISTORICAL_MONTHLY_TOTALS.length}件</p>
</div>
<p><a href="history">→ 履歴ページで確認</a></p>
    `));
  }
  res.json({ ok: true, inserted, skipped, total: HISTORICAL_MONTHLY_TOTALS.length });
});

// 日次在庫スナップショット推移 (mirror_inv_daily_summary を読む)
// PR-A/0/B でミニPC側が毎朝 inv_daily_summary を生成 → sync-to-render で mirror に複製
router.get('/daily', (req, res) => {
  if (!ensureDbOrFailHtml(res)) return;
  const db = getDB();
  let rows = [];
  try {
    rows = db.prepare(`
      SELECT business_date, market, category, total_qty, total_value,
             resolved_count, unresolved_count, cost_missing_count,
             source_status, source_row_count
      FROM mirror_inv_daily_summary
      WHERE market = 'jp'
      ORDER BY business_date DESC, category
    `).all();
  } catch (e) {
    return res.status(503).send(renderLayout('日次推移', `<p>mirror_inv_daily_summary が未作成です: ${esc(e.message)}</p><p>ミニPC からの初回 sync を待ってください。</p>`));
  }

  // pivot: business_date 単位に行集約
  const byDate = new Map();
  for (const r of rows) {
    if (!byDate.has(r.business_date)) byDate.set(r.business_date, { date: r.business_date });
    byDate.get(r.business_date)[r.category] = r;
  }
  const dates = [...byDate.values()];

  function fmtVal(cell) {
    if (!cell) return '<td style="color:#999">-</td>';
    if (cell.source_status === 'no_source') return '<td style="color:#c0392b" title="SP-API 未取得">no data</td>';
    const v = cell.total_value;
    const warn = cell.unresolved_count + cell.cost_missing_count;
    const valueText = v != null ? '¥' + Math.round(v).toLocaleString() : '-';
    const warnSpan = warn > 0 ? ` <small style="color:#e67e22" title="未解決:${cell.unresolved_count} 原価不明:${cell.cost_missing_count}">⚠️${warn}</small>` : '';
    return `<td>${valueText}${warnSpan}</td>`;
  }

  function fmtTotal(d) {
    const cats = ['fba_warehouse', 'fba_inbound', 'own_warehouse'];
    let total = 0, anyData = false, anyMissing = false;
    for (const c of cats) {
      const cell = d[c];
      if (cell && cell.source_status !== 'no_source' && cell.total_value != null) {
        total += cell.total_value;
        anyData = true;
      } else {
        anyMissing = true;
      }
    }
    if (!anyData) return '<td style="color:#999">-</td>';
    return `<td style="font-weight:600">¥${Math.round(total).toLocaleString()}${anyMissing ? ' <small style="color:#e67e22">部分</small>' : ''}</td>`;
  }

  const tableRows = dates.map(d => `
    <tr>
      <td>${esc(d.date)}</td>
      ${fmtVal(d.fba_warehouse)}
      ${fmtVal(d.fba_inbound)}
      ${fmtVal(d.own_warehouse)}
      ${fmtTotal(d)}
    </tr>
  `).join('');

  // チャート用データ (古い順)
  const chartData = [...dates].reverse().map(d => ({
    date: d.date,
    fba_warehouse: (d.fba_warehouse && d.fba_warehouse.source_status !== 'no_source') ? d.fba_warehouse.total_value || 0 : null,
    fba_inbound:   (d.fba_inbound   && d.fba_inbound.source_status   !== 'no_source') ? d.fba_inbound.total_value   || 0 : null,
    own_warehouse: (d.own_warehouse && d.own_warehouse.source_status !== 'no_source') ? d.own_warehouse.total_value || 0 : null,
  }));
  const chartJson = JSON.stringify(chartData);

  res.send(renderLayout('月末棚卸し - 日次推移', `
<h2>日次在庫推移 (国内)</h2>
<p style="color:#666;font-size:13px">
  毎朝 自動で記録された在庫金額の推移。
  「FBA倉庫」=月末ツールと同じ4カラム合算 (在庫あり+FC移管中+FC処理中+出荷待ち)。
  「自社倉庫」=NextEngine 在庫数。
  ⚠️ 表示は警告件数 (未解決SKU + 原価未登録)。
  「no data」 = SP-API 未取得 (cron 失敗 or 当日未稼働)。
</p>
${dates.length === 0 ? '<p>まだデータがありません。ミニPC で daily-sync が走るのを待ってください。</p>' : `

<div class="card">
  <div style="position:relative;height:340px"><canvas id="dailyChart"></canvas></div>
</div>

<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<script>
  const data = ${chartJson};
  const labels = data.map(d => d.date);
  const palette = {
    fba_warehouse: '#2c5aa0',
    fba_inbound:   '#5b9bd5',
    own_warehouse: '#70ad47',
  };
  const labelMap = {
    fba_warehouse: 'FBA倉庫',
    fba_inbound:   'FBA輸送中',
    own_warehouse: '自社倉庫',
  };
  const datasets = Object.keys(palette).map(key => ({
    label: labelMap[key],
    data: data.map(d => d[key]),
    backgroundColor: palette[key],
    borderColor: palette[key],
    fill: true,
    tension: 0.2,
    pointRadius: 2,
    spanGaps: false, // null の日 (no_source) は線をつなげない
  }));
  new Chart(document.getElementById('dailyChart').getContext('2d'), {
    type: 'line',
    data: { labels, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      scales: {
        y: {
          stacked: true,
          ticks: { callback: v => '¥' + (v/1000000).toFixed(0) + 'M' }
        }
      },
      plugins: {
        tooltip: {
          callbacks: {
            label: ctx => {
              const v = ctx.parsed.y;
              if (v == null) return ctx.dataset.label + ': データなし';
              return ctx.dataset.label + ': ¥' + Math.round(v).toLocaleString();
            },
            footer: items => {
              const total = items.filter(i => i.parsed.y != null).reduce((s,i) => s + i.parsed.y, 0);
              return total > 0 ? '合計: ¥' + Math.round(total).toLocaleString() : '';
            }
          }
        },
        legend: { position: 'bottom' }
      }
    }
  });
</script>
`}
${dates.length === 0 ? '' : `
<table>
  <thead>
    <tr>
      <th>業務日 (JST)</th>
      <th>FBA倉庫</th>
      <th>FBA輸送中</th>
      <th>自社倉庫</th>
      <th>合計</th>
    </tr>
  </thead>
  <tbody>${tableRows}</tbody>
</table>
<p style="color:#888;font-size:11px;margin-top:8px">合計に「部分」と出る日: いずれかのカテゴリが no_source / cost_missing。月末確定値とは異なる可能性あり (月末ツールで確定値を出してください)。</p>
`}
`));
});

router.get('/history/:id', (req, res) => {
  if (!ensureDbOrFailHtml(res)) return;
  const id = Number(req.params.id);
  const snap = getSnapshot(id);
  if (!snap) return res.status(404).send('Not found');
  const s = snap.summary;
  const totalRow = (label, val) => `<tr><td>${label}</td><td class="num">${yen(val)}</td></tr>`;
  const pendingHtml = snap.pending.length ? `
<h2>発注後未着商品</h2>
<table><thead><tr><th>仕入先</th><th>金額</th></tr></thead><tbody>
${snap.pending.map(p => `<tr><td>${esc(p.supplier_name)}</td><td class="num">${yen(p.amount)}</td></tr>`).join('')}
</tbody></table>` : '';
  res.send(renderLayout('月末棚卸し ' + s.snapshot_date, `
<h2>${esc(s.snapshot_date)}</h2>
<table>
  <tbody>
    ${totalRow('FBA倉庫内在庫', s.fba_warehouse)}
    ${totalRow('FBA輸送中在庫', s.fba_inbound)}
    ${totalRow('自社倉庫在庫', s.own_warehouse)}
    ${totalRow('米国FBA在庫', s.fba_us)}
    ${totalRow('発注後未着商品', s.pending_orders)}
    <tr class="total-row"><td>合計</td><td class="num">${yen(s.total)}</td></tr>
  </tbody>
</table>
<p>
  <a href="export/${id}.xlsx">📥 Excelダウンロード</a>
  &nbsp;|&nbsp;
  <a href="export/${id}.csv">📥 CSVダウンロード</a>
  &nbsp;/&nbsp; 明細件数: ${snap.details.length}
</p>
${pendingHtml}
`));
});

router.get('/export/:id.xlsx', async (req, res) => {
  if (!ensureDbOrFailHtml(res)) return;
  const id = Number(req.params.id);
  const snap = getSnapshot(id);
  if (!snap) return res.status(404).send('Not found');
  try {
    const buf = await exportSnapshotToXlsx(snap);
    const filename = `月末棚卸し_${snap.summary.snapshot_date}.xlsx`;
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename*=UTF-8''${encodeURIComponent(filename)}`);
    res.send(buf);
  } catch (e) {
    res.status(500).send('<pre>' + esc(e.message) + '</pre>');
  }
});

router.get('/export/:id.csv', (req, res) => {
  if (!ensureDbOrFailHtml(res)) return;
  const id = Number(req.params.id);
  const snap = getSnapshot(id);
  if (!snap) return res.status(404).send('Not found');
  try {
    const buf = buildSnapshotCsv(snap);
    const filename = `月末棚卸し_${snap.summary.snapshot_date}.csv`;
    // text/csv は UTF-8 BOM 付きで送る。filename* で日本語ファイル名対応。
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename*=UTF-8''${encodeURIComponent(filename)}`);
    res.send(buf);
  } catch (e) {
    res.status(500).send('<pre>' + esc(e.message) + '</pre>');
  }
});

export default router;
