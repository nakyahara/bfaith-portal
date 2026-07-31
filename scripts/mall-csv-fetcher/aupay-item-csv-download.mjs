/**
 * au PAYマーケット 一括商品CSVダウンロード 自動化 (mall-csv-fetcher)
 *
 * Wow!manager「商品・画像・デザイン > 一括商品CSVダウンロード」の
 *   ①条件設定 → ②「CSVデータ作成」 → ③処理状況が「完了」になるまで待つ → ④ダウンロード
 * を1コマンドで通す。既定はオリジナルテンプレート「ヤフー在庫アップ後確認」/ 販売ステータス=販売中。
 *
 * 実測 (2026-07-31):
 *   - 画面の「CSVデータ作成」は <a id=btnSave> で、JSが hidden `insertproc=insertproc` を
 *     足してから form#_main_frm を submit する。**この hidden が無いと POST は HTTP 400**
 *   - POST 先は画面と同じ productCsvDl/index、成功時 302 Location=/productCsvDl/index/<jobId>
 *   - 1テンプレートで item.csv と stock.csv (選択肢在庫) の2本が同時に生成される
 *   - 生成は約25秒。ダウンロードリンクは /productCsvDl/download/<id> の素のhref (GETで直取り可)
 *   - CSV本体は Shift_JIS (BOMなし)、ヘッダ行はASCII、改行コード CRLF
 *
 * 実行: node scripts/mall-csv-fetcher/aupay-item-csv-download.mjs [--out <dir>] [--template <名前>]
 *   env: HEADLESS=1
 *        AITEM_TEMPLATE      テンプレート名 (既定「ヤフー在庫アップ後確認」)。--template が優先
 *        AITEM_SELL_STATUS   all | selling | ended (既定 selling)
 *        AITEM_OUT_DIR       保存先ディレクトリ (既定 downloads/aupay-item-csv)
 *        AITEM_LINEFEED_DEL  1=各項目内の改行を除去 (既定1、画面の既定と同じ)
 *        AITEM_NOTIFY        1=失敗時にGChat通知 (既定0。定期実行に載せるときだけ1)
 */

import { mkdir, writeFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { openAupayContext, ensureWowmaLogin, gotoWowmaPage, WOWMA_BASE } from './lib-aupay-login.mjs';
import { initRunLog, sendGChat, buildErrorReport } from './lib-notify.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const IDX_URL = `${WOWMA_BASE}productCsvDl/index`;
const OUT_DIR_DEFAULT = join(__dirname, 'downloads', 'aupay-item-csv');

// 販売ステータス radio csvDlSellStsKbn: 9=全て 1=販売中 2=販売終了
const SELL_STATUS = { all: '9', selling: '1', ended: '2' };
const JOB_TIMEOUT_MS = 10 * 60 * 1000;

function parseArgs(argv) {
  const a = { out: null, template: null, sellStatus: null, help: false };
  for (let i = 0; i < argv.length; i++) {
    const k = argv[i];
    if (k === '--out') a.out = argv[++i];
    else if (k === '--template') a.template = argv[++i];
    else if (k === '--sell-status') a.sellStatus = argv[++i];
    else if (k === '--help' || k === '-h') a.help = true;
    else throw new Error(`不明な引数: ${k} (--out / --template / --sell-status)`);
  }
  return a;
}

function jstStamp() {
  const d = new Date(Date.now() + 9 * 3600 * 1000).toISOString();
  return d.slice(0, 19).replace(/[-:]/g, '').replace('T', '-'); // YYYYMMDD-HHMMSS (JST)
}

/** 処理状況テーブルを読む。[{fileName, acceptedAt, template, statusText, downloadId, href}] */
async function readStatusRows(page) {
  return page.evaluate(() => {
    const t = [...document.querySelectorAll('table')].find((tb) => /CSVファイル名/.test(tb.innerText));
    if (!t) return null;
    const out = [];
    for (const tr of t.querySelectorAll('tr')) {
      const cells = [...tr.querySelectorAll('td')].map((td) => (td.innerText || '').trim());
      if (cells.length < 4) continue;
      const a = [...tr.querySelectorAll('a')].find((x) => /ダウンロード/.test(x.innerText || ''));
      const href = a ? (a.getAttribute('href') || '') : '';
      const m = href.match(/\/productCsvDl\/download\/(\d+)/);
      out.push({
        fileName: cells[0], acceptedAt: cells[1], template: cells[2], statusText: cells[3],
        downloadId: m ? m[1] : null, href,
      });
    }
    return out;
  });
}

const rowKey = (r) => `${r.fileName}|${r.acceptedAt}|${r.template}`;

function classify(statusText) {
  if (/完了/.test(statusText) && /データなし/.test(statusText)) return 'no_data';
  if (/完了/.test(statusText)) return 'done';
  if (/処理待ち|処理中|受付/.test(statusText)) return 'pending';
  if (/エラー|失敗/.test(statusText)) return 'error';
  return 'unknown';
}

/** select#csvDlUserTmpltName の option からテンプレート名 → ID を解決 */
async function resolveTemplate(page, wantName) {
  const opts = await page.evaluate(() => {
    const s = document.querySelector('select[name=csvDlUserTmpltName]');
    if (!s) return null;
    return [...s.options].map((o) => ({ id: o.value, name: (o.text || '').trim() }));
  });
  if (!opts) {
    throw new Error('FORM_VERIFY: オリジナルテンプレートのselect (csvDlUserTmpltName) が見つからず。画面仕様変更の疑い');
  }
  const hit = opts.filter((o) => o.name === wantName);
  if (hit.length === 1) return hit[0];
  if (hit.length > 1) {
    throw new Error(`TEMPLATE: 同名テンプレートが複数あります (${wantName})。Wow!manager側で改名してください`);
  }
  throw new Error(
    `TEMPLATE: テンプレート「${wantName}」が見つかりません。登録済み: ${opts.map((o) => o.name).join(' / ') || '(なし)'}`
  );
}

/**
 * フォームに条件を反映し、画面JSと同じ hidden (insertproc) を足して serialize → POST。
 * DOMのFormDataをそのまま送るので hidden/Struts の `_name=on` 群も画面と完全一致する。
 */
async function submitCreate(page, { templateId, sellStatusValue, linefeedDel }) {
  const body = await page.evaluate((cfg) => {
    const f = document.querySelector('form#_main_frm');
    if (!f) return { error: 'form#_main_frm が無い' };
    const sel = (n) => f.querySelector(`select[name=${n}]`);
    if (!sel('productCsvDlType') || !sel('csvDlUserTmpltName')) return { error: 'ダウンロードタイプ/テンプレートのselectが無い' };
    sel('productCsvDlType').value = '2';                 // オリジナルテンプレートでダウンロード
    sel('csvDlUserTmpltName').value = cfg.templateId;
    const lf = f.querySelector('input[name=csvLinefeedDelFlg]');
    if (lf) lf.checked = cfg.linefeedDel;
    let sellHit = false;
    for (const r of f.querySelectorAll('input[name=csvDlSellStsKbn]')) {
      r.checked = (r.value === cfg.sellStatusValue);
      if (r.checked) sellHit = true;
    }
    if (!sellHit) return { error: `販売ステータス ${cfg.sellStatusValue} のradioが無い` };
    for (const r of f.querySelectorAll('input[name=csvDLCondType]')) r.checked = (r.value === '0'); // 対象商品=指定なし
    for (const n of ['csvDLCondLotNo', 'csvDLCondProductCd', 'csvDLCondMgtId']) {
      const el = f.querySelector(`textarea[name=${n}]`);
      if (el) el.value = '';
    }
    // 画面の <a id=btnSave> のJSと同じ hidden。これが無いと HTTP 400
    const hid = document.createElement('input');
    hid.type = 'hidden'; hid.name = 'insertproc'; hid.value = 'insertproc';
    f.appendChild(hid);
    const p = new URLSearchParams();
    for (const [k, v] of new FormData(f).entries()) if (typeof v === 'string') p.append(k, v);
    hid.remove();
    // 送信直前の読み戻し検証 (画面が別の値に矯正していないか)
    return {
      body: p.toString(),
      echo: {
        type: p.get('productCsvDlType'), tmpl: p.get('csvDlUserTmpltName'),
        sell: p.get('csvDlSellStsKbn'), cond: p.get('csvDLCondType'),
        lf: p.get('csvLinefeedDelFlg'), insertproc: p.get('insertproc'),
      },
    };
  }, { templateId, sellStatusValue, linefeedDel });

  if (body.error) throw new Error(`FORM_VERIFY: ${body.error} — 画面仕様変更の疑い`);
  const e = body.echo;
  if (e.type !== '2' || e.tmpl !== templateId || e.sell !== sellStatusValue || e.cond !== '0' || e.insertproc !== 'insertproc') {
    throw new Error(`FORM_VERIFY: 送信値の読み戻しが不一致 ${JSON.stringify(e)}`);
  }
  console.log(`  [form] type=オリジナル tmpl=${e.tmpl} 販売ステータス=${e.sell} 対象商品=指定なし 改行除去=${e.lf || 'off'}`);

  const res = await page.request.post(IDX_URL, {
    headers: { 'content-type': 'application/x-www-form-urlencoded', referer: IDX_URL },
    data: body.body, maxRedirects: 0, timeout: 60000,
  });
  if (res.status() !== 302) {
    throw new Error(`CREATE_FAILED: 「CSVデータ作成」POST が HTTP ${res.status()} (302=受理 が来ない)`);
  }
  const loc = res.headers()['location'] || '';
  const jobId = (loc.match(/\/productCsvDl\/index\/(\d+)/) || [])[1] || null;
  console.log(`  [create] 受理 (302) jobId=${jobId || '(不明)'}`);
  return jobId;
}

/** 自ジョブの行が全部終端になるまでポーリング */
async function waitForJob(page, beforeKeys, jobId) {
  const deadline = Date.now() + JOB_TIMEOUT_MS;
  let mine = [];
  for (let i = 0; ; i++) {
    await page.waitForTimeout(i === 0 ? 5000 : 10000);
    await gotoWowmaPage(page, IDX_URL, '一括商品CSVダウンロード');
    const rows = await readStatusRows(page);
    if (rows === null) throw new Error('POLL_FAILED: 処理状況テーブルが見つからず (画面仕様変更の疑い)');
    mine = rows.filter((r) => !beforeKeys.has(rowKey(r)));
    if (mine.length === 0) {
      if (Date.now() > deadline) throw new Error('JOB_BIND: 作成を受理されたのに処理状況へ行が現れない');
      console.log('  [poll] 行の出現待ち ...');
      continue;
    }
    const st = mine.map((r) => classify(r.statusText));
    const errored = mine.filter((r, k) => st[k] === 'error');
    if (errored.length) throw new Error(`JOB_FAILED: 処理エラー (${errored.map((r) => `${r.fileName}:${r.statusText}`).join(', ')})`);
    if (st.every((s) => s === 'done' || s === 'no_data')) break;
    if (Date.now() > deadline) {
      throw new Error(`JOB_TIMEOUT: ${Math.round(JOB_TIMEOUT_MS / 60000)}分待っても完了せず (${mine.map((r) => `${r.fileName}:${r.statusText}`).join(', ')})`);
    }
    console.log(`  [poll] ${mine.map((r) => `${r.fileName}=${r.statusText}`).join(' / ')}`);
  }
  // 302 Location の jobId が自ジョブ行に含まれるか (bind取り違えの検算)
  if (jobId && mine.some((r) => r.downloadId) && !mine.some((r) => r.downloadId === jobId)) {
    console.warn(`  ⚠ 受理jobId ${jobId} が自ジョブ行 (${mine.map((r) => r.downloadId).join(',')}) に無い — 並行操作の疑い`);
  }
  console.log(`  [poll] 完了 (${mine.length}件: ${mine.map((r) => r.fileName).join(', ')})`);
  return mine;
}

/** ダウンロードリンクを直GET。CSVであることを検証して Buffer を返す */
async function downloadRow(page, row) {
  const url = new URL(row.href, IDX_URL).toString();
  const res = await page.request.get(url, { timeout: 120000 });
  if (!res.ok()) throw new Error(`DL_FAILED: ${row.fileName} が HTTP ${res.status()}`);
  const ct = res.headers()['content-type'] || '';
  const buf = Buffer.from(await res.body());
  if (/text\/html/i.test(ct) || buf.slice(0, 200).toString('ascii').toLowerCase().includes('<html')) {
    throw new Error(`DL_VERIFY: ${row.fileName} がCSVでなくHTMLを返した (ct=${ct}) — セッション切れ/仕様変更の疑い`);
  }
  if (buf.length === 0) throw new Error(`DL_VERIFY: ${row.fileName} が0バイト`);
  // ヘッダ行はASCII。CSV仕様の主キー列がある事を肯定確認 (エラーページ等の取り違え防止)
  const head = buf.slice(0, 4096).toString('latin1').split(/\r?\n/)[0];
  if (!/(^|,)"?(lotNumber|itemCode|ctrlCol)"?(,|$)/.test(head)) {
    throw new Error(`DL_VERIFY: ${row.fileName} のヘッダに lotNumber/itemCode/ctrlCol が無い (head="${head.slice(0, 120)}")`);
  }
  const lines = buf.toString('latin1').split(/\r?\n/).filter((l) => l.length > 0).length;
  return { buf, header: head, dataRows: Math.max(0, lines - 1) };
}

async function main() {
  let args;
  try {
    args = parseArgs(process.argv.slice(2));
  } catch (e) {
    console.error(`FATAL: ${e.message}`);
    process.exitCode = 2;
    return;
  }
  if (args.help) {
    console.log('使い方: node scripts/mall-csv-fetcher/aupay-item-csv-download.mjs [--out <dir>] [--template <名前>] [--sell-status all|selling|ended]');
    return;
  }
  const runLog = initRunLog('aupay-item-csv');
  const templateName = args.template || process.env.AITEM_TEMPLATE || 'ヤフー在庫アップ後確認';
  const sellKey = (args.sellStatus || process.env.AITEM_SELL_STATUS || 'selling').trim();
  if (!SELL_STATUS[sellKey]) {
    console.error(`FATAL: 販売ステータスは all / selling / ended のいずれか (got: ${sellKey})`);
    process.exitCode = 2;
    return;
  }
  const outDir = args.out || process.env.AITEM_OUT_DIR || OUT_DIR_DEFAULT;
  const linefeedDel = (process.env.AITEM_LINEFEED_DEL ?? '1') === '1';
  const notify = process.env.AITEM_NOTIFY === '1';

  await mkdir(outDir, { recursive: true });
  console.log('=== au PAY 一括商品CSVダウンロード ===');
  console.log(`テンプレート: ${templateName} / 販売ステータス: ${sellKey} / 改行除去: ${linefeedDel ? 'ON' : 'OFF'}`);
  console.log(`保存先: ${outDir}`);

  let context;
  const saved = [];
  try {
    context = await openAupayContext();
  } catch (e) {
    console.error(`FATAL: ブラウザ起動失敗: ${e.message}`);
    process.exitCode = 1;
    return;
  }
  const page = context.pages()[0] || await context.newPage();
  page.on('dialog', (d) => { console.log(`  [dialog] ${d.message()}`); d.accept().catch(() => {}); });

  try {
    await ensureWowmaLogin(page);
    await gotoWowmaPage(page, IDX_URL, '一括商品CSVダウンロード');

    const tmpl = await resolveTemplate(page, templateName);
    console.log(`  [template] ${tmpl.name} (id=${tmpl.id})`);

    const before = await readStatusRows(page);
    if (before === null) throw new Error('FORM_VERIFY: 処理状況テーブルが見つからず (画面仕様変更の疑い)');
    const beforeKeys = new Set(before.map(rowKey));
    console.log(`  [status] 既存 ${before.length} 行`);

    const jobId = await submitCreate(page, { templateId: tmpl.id, sellStatusValue: SELL_STATUS[sellKey], linefeedDel });
    const mine = await waitForJob(page, beforeKeys, jobId);

    const stamp = jstStamp();
    for (const row of mine) {
      if (classify(row.statusText) === 'no_data') {
        console.log(`  [empty] ${row.fileName}: ${row.statusText} — 保存スキップ`);
        continue;
      }
      if (!row.href) throw new Error(`DL_FAILED: ${row.fileName} は完了だがダウンロードリンクなし`);
      const { buf, header, dataRows } = await downloadRow(page, row);
      const base = row.fileName.replace(/\.csv$/i, '');
      const dest = join(outDir, `aupay_${base}_${stamp}.csv`);
      await writeFile(dest, buf);
      const latest = join(outDir, `${base}.csv`); // 固定名 (下流ツールが参照しやすいように毎回上書き)
      await writeFile(latest, buf);
      const sha = createHash('sha256').update(buf).digest('hex').slice(0, 12);
      console.log(`  ✅ ${row.fileName}: ${dataRows}行 ${buf.length}bytes sha=${sha}`);
      console.log(`     ${dest}`);
      console.log(`     ${latest} (固定名)`);
      console.log(`     列: ${header.slice(0, 200)}${header.length > 200 ? ' …' : ''}`);
      saved.push({ file: row.fileName, rows: dataRows, bytes: buf.length, path: dest });
    }

    if (saved.length === 0) throw new Error('RESULT_EMPTY: 保存できたファイルが1本もない (全てデータなし)');
    console.log(`\n=== 完了: ${saved.map((s) => `${s.file}(${s.rows}行)`).join(' / ')} ===`);
    console.log('※ CSVは Shift_JIS / CRLF。Excelはそのまま開けます');
  } catch (err) {
    console.error(`\n⚠️ ${err.message}`);
    const shot = join(__dirname, 'spike-output', 'aupay_item_csv_error.png');
    await mkdir(dirname(shot), { recursive: true }).catch(() => {});
    await page.screenshot({ path: shot, fullPage: true }).catch(() => {});
    if (notify) {
      await sendGChat(buildErrorReport({
        mall: 'aupay', logPath: runLog.logPath,
        failures: [{ reportType: `一括商品CSV (${templateName})`, error: err.message, url: page.url(), screenshot: shot }],
        repro: 'node scripts/mall-csv-fetcher/aupay-item-csv-download.mjs',
      }), 'aupay-item-csv').catch(() => {});
    }
    process.exitCode = String(err.message).startsWith('AUTH_') || String(err.message).startsWith('2FA_') ? 3 : 1;
  } finally {
    await context.close().catch(() => {});
  }
}

main();
