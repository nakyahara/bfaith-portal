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
import { initInventoryMonthly } from './db.js';
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
<div class="nav"><a href=".">入力</a> <a href="history">履歴</a> <a href="/">ポータルへ戻る</a></div>
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
    <label>③ 米国FBA在庫金額（円） <small class="hint">直接入力（Phase 1）</small></label>
    <input type="number" name="us_fba_amount" min="0" step="1" value="0">
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
function addPending() {
  const div = document.createElement('div');
  div.className = 'pending-row';
  div.innerHTML = '<input type="text" name="pending_supplier[]" placeholder="仕入先名"><input type="number" name="pending_amount[]" placeholder="金額（税抜）" min="0" step="1"><button type="button" class="secondary" onclick="this.parentElement.remove()">削除</button>';
  document.getElementById('pendingRows').appendChild(div);
}

let saveFlag = '0';
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

function renderResult(d) {
  const t = d.totals;
  const rows = [
    ['FBA倉庫内在庫', t.fba_warehouse],
    ['FBA輸送中在庫', t.fba_inbound],
    ['自社倉庫在庫',   t.own_warehouse],
    ['米国FBA在庫',    t.fba_us],
    ['発注後未着商品', t.pending],
  ].map(([k,v]) => '<tr><td>'+esc(k)+'</td><td class="num">'+yen(v)+'</td></tr>').join('');

  let warnHtml = '';
  const w = d.warnings;
  const showList = (label, arr) => arr.length ? '<div class="warn"><b>'+esc(label)+': '+arr.length+'件</b><br><small>'+arr.slice(0,30).map(esc).join(', ')+(arr.length>30?' ...':'')+'</small></div>' : '';
  warnHtml += showList('Amazon SKU → NE商品コード未マップ', w.unmappedSkus);
  warnHtml += showList('原価未登録の商品コード', w.missingCost);

  const savedHtml = d.snapshot_id ? '<div class="card" style="background:#d1e7dd;border-color:#a3cfbb"><b>履歴に保存しました</b> → <a href="history/'+encodeURIComponent(d.snapshot_id)+'">詳細を表示</a></div>' : '';

  return '<h2>集計結果</h2>'
    + savedHtml
    + '<table><thead><tr><th>区分</th><th>金額（税抜）</th></tr></thead><tbody>'
    + rows
    + '<tr class="total-row"><td>合計</td><td class="num">'+yen(t.total)+'</td></tr>'
    + '</tbody></table>'
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
]), (req, res) => {
  // multer が書き出した一時ファイルはどの分岐を通っても必ず削除する。
  // 早期 return / 例外 / DB エラーいずれの場合も orphan を残さないため、
  // 関数の入口で cleanup を定義し、return/throw 直前に呼び出す。
  const cleanup = () => {
    try { if (req.files?.fba_csv?.[0]?.path) fs.unlinkSync(req.files.fba_csv[0].path); } catch {}
    try { if (req.files?.own_csv?.[0]?.path) fs.unlinkSync(req.files.own_csv[0].path); } catch {}
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
    cleanup();

    const fbaRows = parseRestockReport(fbaBuf);
    const ownRows = parseOwnWarehouse(ownBuf);
    const usFbaAmount = Number(req.body.us_fba_amount) || 0;

    const suppliers = [].concat(req.body['pending_supplier[]'] || []);
    const amounts = [].concat(req.body['pending_amount[]'] || []);
    const pendingRows = suppliers
      .map((s, i) => ({ supplier_name: (s || '').trim(), amount: Number(amounts[i]) || 0 }))
      .filter(p => p.supplier_name && p.amount > 0);

    // 集計と保存はどちらも mirror DB が必要。
    if (!ensureDbOrFail(res)) return;
    const result = aggregateInventory({ fbaRows, ownRows, usFbaAmount, pendingRows });

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
  const list = listSnapshots();
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
  res.send(renderLayout('月末棚卸し - 履歴', `
<h2>履歴一覧</h2>
${list.length === 0 ? '<p>まだ履歴がありません。</p>' : `
<table>
  <thead><tr><th>基準日</th><th>FBA倉庫</th><th>FBA輸送中</th><th>自社倉庫</th><th>米国FBA</th><th>発注後未着</th><th>合計</th><th>作成日時</th></tr></thead>
  <tbody>${rows}</tbody>
</table>`}
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
