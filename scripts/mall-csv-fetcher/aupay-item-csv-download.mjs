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

import { mkdir, writeFile, rename, unlink, open } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { dirname, join, basename } from 'node:path';
import { openAupayContext, ensureWowmaLogin, gotoWowmaPage, WOWMA_BASE } from './lib-aupay-login.mjs';
import { initRunLog, sendGChat, buildErrorReport } from './lib-notify.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const IDX_URL = `${WOWMA_BASE}productCsvDl/index`;
const OUT_DIR_DEFAULT = join(__dirname, 'downloads', 'aupay-item-csv');
const LOCK_PATH = join(__dirname, 'logs', 'aupay-item-csv.lock');

// 販売ステータス radio csvDlSellStsKbn: 9=全て 1=販売中 2=販売終了
const SELL_STATUS = { all: '9', selling: '1', ended: '2' };
const JOB_TIMEOUT_MS = 10 * 60 * 1000;
const LOCK_STALE_MS = 30 * 60 * 1000;
// 処理状況テーブルの1ページ表示件数 (画面既定50)。これに近づいたらページングで
// 自ジョブ行が押し出される恐れがあるため警告する
const STATUS_PAGE_SIZE = 50;
// サーバー由来のファイル名をそのままパスに使わないための許可パターン
const SAFE_FILE_NAME = /^[A-Za-z0-9][A-Za-z0-9_-]*\.csv$/;

function parseArgs(argv) {
  const a = { out: null, template: null, sellStatus: null, help: false };
  const need = (i, k) => {
    const v = argv[i];
    if (v === undefined || v.startsWith('--')) throw new Error(`${k} には値が必要です`);
    return v;
  };
  for (let i = 0; i < argv.length; i++) {
    const k = argv[i];
    if (k === '--out') a.out = need(++i, '--out');
    else if (k === '--template') a.template = need(++i, '--template');
    else if (k === '--sell-status') a.sellStatus = need(++i, '--sell-status');
    else if (k === '--help' || k === '-h') a.help = true;
    else throw new Error(`不明な引数: ${k} (--out / --template / --sell-status)`);
  }
  return a;
}

function jstStamp() {
  const d = new Date(Date.now() + 9 * 3600 * 1000).toISOString();
  return d.slice(0, 23).replace(/[-:.]/g, '').replace('T', '-'); // YYYYMMDD-HHMMSSmmm (JST)
}

/** 多重起動ロック (自分自身の同時実行が固定名ファイルを相互に上書きするのを防ぐ) */
async function acquireLock() {
  await mkdir(dirname(LOCK_PATH), { recursive: true });
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const fh = await open(LOCK_PATH, 'wx');
      await fh.writeFile(JSON.stringify({ pid: process.pid, startedAt: new Date().toISOString() }));
      await fh.close();
      return;
    } catch (e) {
      if (e.code !== 'EEXIST') throw e;
      const { statSync } = await import('node:fs');
      let age = Infinity;
      try { age = Date.now() - statSync(LOCK_PATH).mtimeMs; } catch { /* 直前に消えた */ }
      if (age > LOCK_STALE_MS) {
        console.warn(`  ⚠ 古いロック (${Math.round(age / 60000)}分前) を破棄します`);
        await unlink(LOCK_PATH).catch(() => {});
        continue;
      }
      throw new Error(`LOCKED: 別の実行が進行中です (${LOCK_PATH})。終わってから再実行してください`);
    }
  }
  throw new Error(`LOCKED: ロックを取得できません (${LOCK_PATH})`);
}
const releaseLock = () => unlink(LOCK_PATH).catch(() => {});

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
 * 画面submitとの等価性が崩れる条件 (ファイル項目・非文字列値・insertprocの既存) は明示的に弾く。
 */
async function submitCreate(page, { templateId, sellStatusValue, linefeedDel }) {
  const built = await page.evaluate((cfg) => {
    const f = document.querySelector('form#_main_frm');
    if (!f) return { error: 'form#_main_frm が無い' };
    const enc = f.getAttribute('enctype');
    if (enc && !/urlencoded/i.test(enc)) return { error: `enctype が想定外 (${enc})` };
    if (f.querySelector('input[type=file]')) {
      return { error: 'フォームにファイル項目がある — urlencodedシリアライズでは等価にならない' };
    }
    if (f.querySelector('[name=insertproc]')) {
      return { error: '画面側に既に insertproc がある — 二重付与になるため中断' };
    }
    const sel = (n) => f.querySelector(`select[name=${n}]`);
    if (!sel('productCsvDlType') || !sel('csvDlUserTmpltName')) return { error: 'ダウンロードタイプ/テンプレートのselectが無い' };
    sel('productCsvDlType').value = '2';                 // オリジナルテンプレートでダウンロード
    sel('csvDlUserTmpltName').value = cfg.templateId;
    if (sel('csvDlUserTmpltName').value !== cfg.templateId) {
      return { error: `テンプレートID ${cfg.templateId} がselectに無い` };
    }
    const lf = f.querySelector('input[name=csvLinefeedDelFlg]');
    if (lf) lf.checked = cfg.linefeedDel;
    let sellHit = false;
    for (const r of f.querySelectorAll('input[name=csvDlSellStsKbn]')) {
      r.checked = (r.value === cfg.sellStatusValue);
      if (r.checked) sellHit = true;
    }
    if (!sellHit) return { error: `販売ステータス ${cfg.sellStatusValue} のradioが無い` };
    let condHit = false;
    for (const r of f.querySelectorAll('input[name=csvDLCondType]')) {
      r.checked = (r.value === '0');                     // 対象商品=指定なし
      if (r.checked) condHit = true;
    }
    if (!condHit) return { error: '対象商品「指定なし」のradioが無い' };
    for (const n of ['csvDLCondLotNo', 'csvDLCondProductCd', 'csvDLCondMgtId']) {
      const el = f.querySelector(`textarea[name=${n}]`);
      if (el) el.value = '';
    }
    // 画面の <a id=btnSave> のJSと同じ hidden。これが無いと HTTP 400
    const hid = document.createElement('input');
    hid.type = 'hidden'; hid.name = 'insertproc'; hid.value = 'insertproc';
    f.appendChild(hid);
    const p = new URLSearchParams();
    const nonString = [];
    for (const [k, v] of new FormData(f).entries()) {
      if (typeof v === 'string') p.append(k, v);
      else nonString.push(k);
    }
    hid.remove();
    if (nonString.length) return { error: `非文字列のフォーム項目がある (${nonString.join(', ')})` };
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

  if (built.error) throw new Error(`FORM_VERIFY: ${built.error} — 画面仕様変更の疑い`);
  const e = built.echo;
  if (e.type !== '2' || e.tmpl !== templateId || e.sell !== sellStatusValue || e.cond !== '0' || e.insertproc !== 'insertproc') {
    throw new Error(`FORM_VERIFY: 送信値の読み戻しが不一致 ${JSON.stringify(e)}`);
  }
  console.log(`  [form] type=オリジナル tmpl=${e.tmpl} 販売ステータス=${e.sell} 対象商品=指定なし 改行除去=${e.lf || 'off'}`);

  const res = await page.request.post(IDX_URL, {
    headers: { 'content-type': 'application/x-www-form-urlencoded', referer: IDX_URL },
    data: built.body, maxRedirects: 0, timeout: 60000,
  });
  if (res.status() !== 302) {
    const peek = (await res.text().catch(() => '')).replace(/\s+/g, ' ').slice(0, 200);
    throw new Error(`CREATE_FAILED: 「CSVデータ作成」POST が HTTP ${res.status()} (302=受理 が来ない)${peek ? ` body="${peek}"` : ''}`);
  }
  // jobId は自ジョブbindの上限として必須。取れないなら誤ダウンロードを避けて即中断。
  // 部分一致だと別オリジンや余計なパスを含むLocationも通ってしまうので、
  // origin と pathname 全体をアンカーして検証する
  const loc = res.headers()['location'] || '';
  const expected = new URL(IDX_URL);
  let jobId = null;
  try {
    const u = new URL(loc, IDX_URL);
    if (u.origin === expected.origin) {
      const m = u.pathname.match(new RegExp(`^${expected.pathname.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}/(\\d+)$`));
      if (m) jobId = m[1];
    }
  } catch { /* 不正URL → jobId=null で下の検証に落ちる */ }
  if (!jobId) {
    throw new Error(`CREATE_VERIFY: 302のLocationからjobIdを取得できず (location="${loc}") — 画面仕様変更の疑い`);
  }
  console.log(`  [create] 受理 (302) jobId=${jobId}`);
  return jobId;
}

/**
 * 自ジョブの行が全部終端になるまでポーリングし、自ジョブ行だけを返す。
 *
 * 行の同定が難しい理由: ファイル名は item.csv/stock.csv 固定で論理日付を持たず、
 * 画面はジョブ単位のIDを行に出さない。使える手掛かりは downloadId と受付日時だけ。
 *
 * 待ち合わせ (ポーリング中): 「submit前に無かった」×「テンプレート名が自分のもの」で絞る。
 *   完了までdownloadIdが振られないため、この2つがポーリング中に使える識別子。
 *   同一テンプレートの並行ジョブは混じり得るが、他人の失敗でこちらも止まる方向
 *   (fail-closed) なので危険はない。
 *
 * 取得対象の確定 (完了後): **受付日時をアンカーにして厳密に絞る**。
 *   downloadId === jobId の行は、サーバーが自分のPOSTに返したIDそのものなので確実に自ジョブ。
 *   その行の受付日時と一致する行だけを自ジョブとする (1ジョブの複数ファイルは受付日時が同一)。
 *   これで「事前取得後・自ジョブ受付前」に割り込んだ同一テンプレートの並行ジョブ
 *   (id範囲だけでは弾けない) も、受付日時が違うので除外される。
 *   同一秒に別ジョブが受け付けられた場合だけは原理的に分離できない。
 */
async function waitForJob(page, { beforeKeys, maxIdBefore, jobId, templateName }) {
  const deadline = Date.now() + JOB_TIMEOUT_MS;
  const jobIdNum = Number(jobId);
  let fresh = [];
  let unknownStreak = 0;
  for (let i = 0; ; i++) {
    await page.waitForTimeout(i === 0 ? 5000 : 10000);
    await gotoWowmaPage(page, IDX_URL, '一括商品CSVダウンロード');
    const rows = await readStatusRows(page);
    if (rows === null) throw new Error('POLL_FAILED: 処理状況テーブルが見つからず (画面仕様変更の疑い)');
    if (rows.length >= STATUS_PAGE_SIZE) {
      console.warn(`  ⚠ 処理状況が ${rows.length} 行 — ページングで自ジョブ行が押し出される恐れ (表示件数を増やすか日を置いて再実行)`);
    }
    const newRows = rows.filter((r) => !beforeKeys.has(rowKey(r)));
    fresh = newRows.filter((r) => r.template === templateName);
    const otherTmpl = newRows.filter((r) => r.template !== templateName);
    if (otherTmpl.length) {
      console.warn(`  ⚠ 別テンプレートの新規行を無視: ${otherTmpl.map((r) => `${r.fileName}(${r.template})`).join(', ')}`);
    }
    if (fresh.length === 0) {
      if (Date.now() > deadline) throw new Error('JOB_BIND: 作成を受理されたのに処理状況へ行が現れない');
      console.log('  [poll] 行の出現待ち ...');
      continue;
    }
    const st = fresh.map((r) => classify(r.statusText));
    const errored = fresh.filter((r, k) => st[k] === 'error');
    if (errored.length) throw new Error(`JOB_FAILED: 処理エラー (${errored.map((r) => `${r.fileName}:${r.statusText}`).join(', ')})`);
    if (st.includes('unknown')) {
      // 新しい失敗状態やログイン画面の混入をタイムアウトまで放置しない
      if (++unknownStreak >= 3) {
        const u = fresh.filter((r, k) => st[k] === 'unknown').map((r) => `${r.fileName}:"${r.statusText}"`).join(', ');
        throw new Error(`POLL_UNKNOWN_STATUS: 解釈できないステータスが続いています (${u}) url=${page.url()} — 画面仕様変更の疑い`);
      }
    } else {
      unknownStreak = 0;
    }
    if (st.every((s) => s === 'done' || s === 'no_data')) break;
    if (Date.now() > deadline) {
      throw new Error(`JOB_TIMEOUT: ${Math.round(JOB_TIMEOUT_MS / 60000)}分待っても完了せず (${fresh.map((r) => `${r.fileName}:${r.statusText}`).join(', ')})`);
    }
    console.log(`  [poll] ${fresh.map((r) => `${r.fileName}=${r.statusText}`).join(' / ')}`);
  }

  // 完了行は必ずidを持つ。まずid範囲で粗く絞る
  const inRange = [];
  const outOfRange = [];
  for (const r of fresh) {
    if (classify(r.statusText) === 'no_data') continue; // id無し。アンカー確定後に受付日時で拾う
    const id = Number(r.downloadId);
    if (!Number.isFinite(id)) {
      throw new Error(`JOB_BIND: 完了行 ${r.fileName} からダウンロードIDを取得できず (href="${r.href}")`);
    }
    if (id <= jobIdNum && (maxIdBefore === null || id > maxIdBefore)) inRange.push(r);
    else outOfRange.push(r);
  }
  if (outOfRange.length) {
    console.warn(`  ⚠ 自ジョブ範囲 (${maxIdBefore ?? '-'} < id <= ${jobIdNum}) 外の新規行を除外: ${outOfRange.map((r) => `${r.fileName}#${r.downloadId}`).join(', ')} — 並行して別の作成が走った模様`);
  }

  // アンカー = downloadId が jobId そのものの行 (サーバーが自分に返したID = 確実に自ジョブ)。
  // その受付日時と一致する行だけを自ジョブとする
  const anchor = inRange.find((r) => r.downloadId === jobId);
  if (!anchor) {
    // 自ジョブのファイルが全部「データなし」なら起こり得る。DL対象が無いだけなので
    // 空で返し、呼び元の RESULT_EMPTY に委ねる (他ジョブのCSVは絶対に掴まない)
    console.warn(`  ⚠ 受理jobId ${jobId} の行が見つからず — DL対象なしとして扱います (id: ${inRange.map((r) => r.downloadId).join(',') || 'なし'})`);
    return [];
  }
  const candidates = [...inRange, ...fresh.filter((r) => classify(r.statusText) === 'no_data')];
  const mine = candidates.filter((r) => r.acceptedAt === anchor.acceptedAt);
  const dropped = candidates.filter((r) => r.acceptedAt !== anchor.acceptedAt);
  if (dropped.length) {
    console.warn(`  ⚠ 受付日時が自ジョブ (${anchor.acceptedAt}) と違う新規行を除外: ${dropped.map((r) => `${r.fileName}@${r.acceptedAt}`).join(', ')}`);
  }
  console.log(`  [poll] 完了 (${mine.length}件: ${mine.map((r) => r.fileName).join(', ')} / 受付 ${anchor.acceptedAt})`);
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
  // 引用符内の改行は AITEM_LINEFEED_DEL=1 (既定) で除去済み。0 のときは行数が概算になる
  const lines = buf.toString('latin1').split(/\r?\n/).filter((l) => l.length > 0).length;
  return { buf, header: head, dataRows: Math.max(0, lines - 1) };
}

async function run(page, { templateName, sellKey, linefeedDel, outDir }) {
  await ensureWowmaLogin(page);
  await gotoWowmaPage(page, IDX_URL, '一括商品CSVダウンロード');

  const tmpl = await resolveTemplate(page, templateName);
  console.log(`  [template] ${tmpl.name} (id=${tmpl.id})`);

  const before = await readStatusRows(page);
  if (before === null) throw new Error('FORM_VERIFY: 処理状況テーブルが見つからず (画面仕様変更の疑い)');
  const beforeKeys = new Set(before.map(rowKey));
  const beforeIds = before.map((r) => Number(r.downloadId)).filter(Number.isFinite);
  const maxIdBefore = beforeIds.length ? Math.max(...beforeIds) : null;
  console.log(`  [status] 既存 ${before.length} 行 (最大id=${maxIdBefore ?? '-'})`);

  const jobId = await submitCreate(page, { templateId: tmpl.id, sellStatusValue: SELL_STATUS[sellKey], linefeedDel });
  const mine = await waitForJob(page, { beforeKeys, maxIdBefore, jobId, templateName: tmpl.name });

  // ── 全ファイルをDL+検証してから公開する (item だけ新版 / stock は旧版、を作らない) ──
  const staged = [];
  for (const row of mine) {
    if (classify(row.statusText) === 'no_data') {
      console.log(`  [empty] ${row.fileName}: ${row.statusText} — 保存スキップ`);
      continue;
    }
    if (!row.href) throw new Error(`DL_FAILED: ${row.fileName} は完了だがダウンロードリンクなし`);
    // サーバー由来のファイル名をそのままパスに使わない
    const safeName = basename(row.fileName);
    if (!SAFE_FILE_NAME.test(safeName)) {
      throw new Error(`DL_VERIFY: 想定外のファイル名 "${row.fileName}" — 保存先逸脱を避けて中断`);
    }
    const { buf, header, dataRows } = await downloadRow(page, row);
    staged.push({ name: safeName, base: safeName.replace(/\.csv$/i, ''), buf, header, dataRows });
  }
  if (staged.length === 0) throw new Error('RESULT_EMPTY: 保存できたファイルが1本もない (全てデータなし)');

  const stamp = jstStamp();
  const saved = [];
  for (const f of staged) {
    const dest = join(outDir, `aupay_${f.base}_${stamp}.csv`);
    await writeFile(dest, f.buf);                       // 履歴 (一意名なので上書き衝突しない)
    const latest = join(outDir, `${f.base}.csv`);        // 固定名 (下流ツールが参照しやすいように)
    const tmp = `${latest}.tmp-${process.pid}`;
    await writeFile(tmp, f.buf);
    await rename(tmp, latest);                          // 差し替えは atomic に
    const sha = createHash('sha256').update(f.buf).digest('hex').slice(0, 12);
    console.log(`  ✅ ${f.name}: ${f.dataRows}行 ${f.buf.length}bytes sha=${sha}`);
    console.log(`     ${dest}`);
    console.log(`     ${latest} (固定名)`);
    console.log(`     列: ${f.header.slice(0, 200)}${f.header.length > 200 ? ' …' : ''}`);
    saved.push({ file: f.name, rows: f.dataRows });
  }
  return saved;
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

  let runLog = { logPath: null };
  let locked = false;
  let context = null;
  let page = null;

  const fail = async (err, phase) => {
    console.error(`\n⚠️ ${err.message}`);
    let shot = null;
    if (page) {
      shot = join(__dirname, 'spike-output', `aupay_item_csv_error_${process.pid}.png`);
      await mkdir(dirname(shot), { recursive: true }).catch(() => {});
      const ok = await page.screenshot({ path: shot, fullPage: true }).then(() => true).catch(() => false);
      if (!ok) shot = null;
    }
    if (notify) {
      await sendGChat(buildErrorReport({
        mall: 'aupay', logPath: runLog.logPath,
        failures: [{ reportType: `一括商品CSV (${templateName}/${phase})`, error: err.message, url: page ? page.url() : '', screenshot: shot }],
        repro: 'node scripts/mall-csv-fetcher/aupay-item-csv-download.mjs',
      }), 'aupay-item-csv').catch(() => {});
    }
    process.exitCode = String(err.message).startsWith('AUTH_') || String(err.message).startsWith('2FA_') ? 3 : 1;
  };

  try {
    runLog = initRunLog('aupay-item-csv');
    console.log('=== au PAY 一括商品CSVダウンロード ===');
    console.log(`テンプレート: ${templateName} / 販売ステータス: ${sellKey} / 改行除去: ${linefeedDel ? 'ON' : 'OFF'}`);
    console.log(`保存先: ${outDir}`);
    await acquireLock();
    locked = true;
    await mkdir(outDir, { recursive: true });
    context = await openAupayContext();
    page = context.pages()[0] || await context.newPage();
    page.on('dialog', (d) => { console.log(`  [dialog] ${d.message()}`); d.accept().catch(() => {}); });

    const saved = await run(page, { templateName, sellKey, linefeedDel, outDir });
    console.log(`\n=== 完了: ${saved.map((s) => `${s.file}(${s.rows}行)`).join(' / ')} ===`);
    console.log('※ CSVは Shift_JIS / CRLF。Excelはそのまま開けます');
  } catch (err) {
    await fail(err, context ? '実行' : '起動');
  } finally {
    if (context) await context.close().catch(() => {});
    if (locked) await releaseLock();
  }
}

main().catch((err) => {
  // 最終防衛線 (通知やスクショの経路自体が壊れた場合でも無音終了させない)
  console.error(`FATAL(uncaught): ${err && err.stack ? err.stack : err}`);
  process.exitCode = 1;
});
