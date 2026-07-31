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
 * DL後は Google ドライブの共有フォルダ「AUpayダウンロード」へ **固定名で毎回上書き** する
 * (miniPCにGドライブは無いのでrclone経由。[[feedback_minipc_no_gdrive_use_rclone]])。
 *
 * 実行: node scripts/mall-csv-fetcher/aupay-item-csv-download.mjs [--out <dir>] [--template <名前>]
 *   env: HEADLESS=1
 *        AITEM_TEMPLATE      テンプレート名 (既定「ヤフー在庫アップ後確認」)。--template が優先
 *        AITEM_SELL_STATUS   all | selling | ended (既定 selling)
 *        AITEM_OUT_DIR       保存先ディレクトリ (既定 downloads/aupay-item-csv)
 *        AITEM_LINEFEED_DEL  1=各項目内の改行を除去 (既定1、画面の既定と同じ)
 *        AITEM_NOTIFY        1=失敗時にGChat通知 (既定0。定期実行に載せるときだけ1)
 *        AITEM_DRIVE_FOLDER  アップロード先のDriveフォルダID (既定=AUpayダウンロード)
 *        AITEM_DRIVE_REMOTE  rcloneリモート名 (既定 gdrive:)
 *        --no-drive          Driveアップロードを行わない (ローカル保存だけ)
 */

import { mkdir, writeFile, rename, unlink, open } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { dirname, join, basename } from 'node:path';
import { openAupayContext, ensureWowmaLogin, gotoWowmaPage, WOWMA_BASE } from './lib-aupay-login.mjs';
import { initRunLog, sendGChat, buildErrorReport } from './lib-notify.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const IDX_URL = `${WOWMA_BASE}productCsvDl/index`;
const OUT_DIR_DEFAULT = join(__dirname, 'downloads', 'aupay-item-csv');
const LOCK_PATH = join(__dirname, 'logs', 'aupay-item-csv.lock');

// Google ドライブ 共有フォルダ「AUpayダウンロード」(2026-07-31 中原さん作成、実在をAPIで確認済)。
// 秘密ではないので既定値として持つ。別フォルダに出すなら AITEM_DRIVE_FOLDER で上書き
const TEMPLATE_DEFAULT = 'ヤフー在庫アップ後確認';
// このフォルダは上のテンプレート専用 (別テンプレートの結果は入れない)
const DRIVE_FOLDER_DEFAULT = '1Stxagr7RybClMrt_W1x7izeD8cCadPmq';
const DRIVE_REMOTE_DEFAULT = 'gdrive:';
const DRIVE_ID_RE = /^[A-Za-z0-9_-]{20,60}$/;   // フォルダIDの形 (パス/オプション混入を弾く)
const DRIVE_REMOTE_RE = /^[A-Za-z0-9_-]+:$/;

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
  const a = { out: null, template: null, sellStatus: null, driveFolder: null, noDrive: false, help: false };
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
    else if (k === '--drive-folder') a.driveFolder = need(++i, '--drive-folder');
    else if (k === '--no-drive') a.noDrive = true;
    else if (k === '--help' || k === '-h') a.help = true;
    else throw new Error(`不明な引数: ${k} (--out / --template / --sell-status / --drive-folder / --no-drive)`);
  }
  return a;
}

/**
 * rclone で Google ドライブへ固定名アップロード (毎回上書き)。
 *
 * copyto は同名ファイルを中身だけ差し替えるので **DriveのファイルIDは変わらない**
 * (共有リンクやシートの参照が生きたまま)。2026-07-31に実測で確認:
 * `Copied (replaced existing)` → id/createdTime そのまま・modifiedTime だけ更新。
 *
 * ⭐--ignore-times で **中身が前回と同一でも必ず転送する**。
 * 既定 (サイズ+時刻) や --checksum だと同一内容の回は転送がスキップされ、
 * Drive上の更新日時が前回のまま残る。商品マスタは在庫と違って何日も変わらないので、
 * 中原さんが更新日時を見て「item.csv だけ更新されていない?」と毎回不安になる。
 * 550KB程度の転送で「更新日時 = 最後に実行した時刻」が保証される方が価値が高い。
 *
 * 引数は spawnSync + shell:false で配列渡し ([[feedback_execfile_vs_execsync]])。
 */
function uploadToDrive(localPath, destName, { remote, folderId }) {
  if (!DRIVE_REMOTE_RE.test(remote)) throw new Error(`DRIVE_CONFIG: rcloneリモート名が不正 (${remote})`);
  if (!DRIVE_ID_RE.test(folderId)) throw new Error(`DRIVE_CONFIG: DriveフォルダIDが不正 (${folderId})`);
  if (!SAFE_FILE_NAME.test(destName)) throw new Error(`DRIVE_CONFIG: アップロード名が不正 (${destName})`);
  const args = [
    'copyto', localPath, `${remote}${destName}`,
    '--drive-root-folder-id', folderId,   // ここで指定したフォルダの外には絶対に書かない
    '--ignore-times',                     // 同一内容でも必ず転送 = 更新日時が実行時刻になる
    '--stats', '0', '--log-level', 'INFO',
  ];
  // rclone の INFO ログは stderr に出るので spawnSync で両方受け取る
  const r = spawnSync('rclone', args, {
    shell: false, encoding: 'utf8', timeout: 5 * 60 * 1000,
    maxBuffer: 8 * 1024 * 1024,   // 既定1MBだとログ量が増えたとき成功しても失敗扱いになる
  });
  const log = `${r.stdout || ''}${r.stderr || ''}`;
  if (r.error || r.status !== 0) {
    const why = r.error ? r.error.message : `exit ${r.status}${r.signal ? ` signal ${r.signal}` : ''}`;
    throw new Error(`DRIVE_UPLOAD: ${destName} のアップロードに失敗 (${why})${log.trim() ? ` :: ${log.trim().slice(0, 400)}` : ''}`);
  }
  // --ignore-times なので必ず転送されるはず。されていなければ想定外なので気づけるようにする
  if (!/Copied|Updated|Transferred:\s*[1-9]/i.test(log)) {
    console.warn(`  ⚠ ${destName}: rcloneが転送した形跡なし (更新日時が古いままの可能性) :: ${log.trim().slice(0, 200)}`);
  }
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

/** 受付日時「YYYY/MM/DD HH:MM:SS」(JST) を epoch ms に。読めなければ null */
function parseAcceptedAt(s) {
  const m = String(s).match(/^(\d{4})\/(\d{2})\/(\d{2})\s+(\d{2}):(\d{2}):(\d{2})$/);
  if (!m) return null;
  const [, y, mo, d, h, mi, se] = m.map(Number);
  return Date.UTC(y, mo - 1, d, h - 9, mi, se); // JST → UTC
}

/**
 * 自ジョブの行が全部終端になるまでポーリングし、自ジョブ行だけを返す。
 *
 * 行の同定が難しい理由: ファイル名は item.csv/stock.csv 固定で論理日付を持たず、
 * 画面はジョブ単位のIDを行に出さない。使える手掛かりは downloadId と受付日時だけ。
 * 実測では 1ジョブ = 同一の受付日時 + 連番の downloadId (末尾が302で返る jobId)。
 *
 * ①待ち合わせ対象の絞り込み (ポーリング中):
 *   「submit前に無かった」×「テンプレート名が自分のもの」で候補を作り、受付日時でグループ化して
 *   自ジョブのグループだけを待つ。グループの選び方は
 *     (a) downloadId === jobId の行を含むグループ → 確定 (サーバーが自分に返したID)
 *     (b) まだ (a) が出ていなければ、受付日時が自分のPOST時刻に最も近いグループ (許容±5分)
 *     (c) それも決まらなければ候補全部 (fail-closed)
 *   これで、同一テンプレートの並行ジョブのエラーや未完了に巻き込まれて
 *   誤終了・タイムアウトすることがなくなる。
 *
 * ②取得対象の確定 (完了後): downloadId === jobId の行をアンカーにし、
 *   その受付日時と一致する行だけを自ジョブとする。これが最終的な正。
 *   「事前取得後・自ジョブ受付前」に割り込んだ同一テンプレートの並行ジョブ
 *   (id範囲だけでは弾けない) も受付日時が違うので除外される。
 *   同一秒に別ジョブが受け付けられた場合だけは原理的に分離できない。
 */
async function waitForJob(page, { beforeKeys, maxIdBefore, jobId, templateName, submitAtMs }) {
  const deadline = Date.now() + JOB_TIMEOUT_MS;
  const jobIdNum = Number(jobId);
  const NEAR_MS = 120 * 1000; // 自ジョブの受付日時はPOST直後の数秒以内。狭くして他ジョブとの取り違えを減らす
  let group = [];
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
    const fresh = newRows.filter((r) => r.template === templateName);
    const otherTmpl = newRows.filter((r) => r.template !== templateName);
    if (otherTmpl.length) {
      console.warn(`  ⚠ 別テンプレートの新規行を無視: ${otherTmpl.map((r) => `${r.fileName}(${r.template})`).join(', ')}`);
    }
    if (fresh.length === 0) {
      if (Date.now() > deadline) throw new Error('JOB_BIND: 作成を受理されたのに処理状況へ行が現れない');
      console.log('  [poll] 行の出現待ち ...');
      continue;
    }

    // 受付日時でグループ化 → 自ジョブのグループを選ぶ
    const byAccepted = new Map();
    for (const r of fresh) {
      if (!byAccepted.has(r.acceptedAt)) byAccepted.set(r.acceptedAt, []);
      byAccepted.get(r.acceptedAt).push(r);
    }
    const anchorRow = fresh.find((r) => r.downloadId === jobId);
    let pickedAt = null;
    if (anchorRow) {
      pickedAt = anchorRow.acceptedAt;                                 // (a) 確定
    } else {
      // (b) POST時刻に最も近いグループ。グループが1つでも近接判定は必ず通す
      // (自ジョブの行がまだ出ておらず、並行ジョブの行だけが見えている場合があるため)
      let best = null;
      for (const at of byAccepted.keys()) {
        const ms = parseAcceptedAt(at);
        if (ms === null) continue;
        const diff = Math.abs(ms - submitAtMs);
        if (diff <= NEAR_MS && (best === null || diff < best.diff)) best = { at, diff };
      }
      pickedAt = best ? best.at : null;
    }
    const certain = Boolean(anchorRow);
    group = pickedAt === null ? fresh : byAccepted.get(pickedAt);      // (c) 決まらなければ全部
    if (pickedAt !== null && byAccepted.size > 1) {
      const others = [...byAccepted.keys()].filter((a) => a !== pickedAt);
      console.warn(`  ⚠ 同一テンプレートの別ジョブ (受付 ${others.join(', ')}) は待ち合わせ対象から除外`);
    }

    const st = group.map((r) => classify(r.statusText));
    const errored = group.filter((r, k) => st[k] === 'error');
    const unknowns = group.filter((r, k) => st[k] === 'unknown');
    // 異常の扱い: アンカー確定 or POST時刻の近傍グループなら自ジョブとして即中断する。
    // 近接判定すら通らない (pickedAt=null) 場合は自ジョブと断定できないので、
    // 警告に留めて待ち続ける — 他人のジョブのエラーでこちらを落とさない (fail-closed)
    if (errored.length) {
      const msg = `処理エラー (${errored.map((r) => `${r.fileName}:${r.statusText}`).join(', ')})`;
      if (pickedAt !== null) throw new Error(`JOB_FAILED: ${msg}`);
      console.warn(`  ⚠ 自ジョブと断定できない行でエラー: ${msg} — 待機継続`);
    }
    if (unknowns.length) {
      // 新しい失敗状態やログイン画面の混入をタイムアウトまで放置しない
      if (++unknownStreak >= 3 && pickedAt !== null) {
        const u = unknowns.map((r) => `${r.fileName}:"${r.statusText}"`).join(', ');
        throw new Error(`POLL_UNKNOWN_STATUS: 解釈できないステータスが続いています (${u}) url=${page.url()} — 画面仕様変更の疑い`);
      }
    } else {
      unknownStreak = 0;
    }
    // 終端条件: 選んだグループが全部終端。ただしアンカー未確定なら、
    // 「自ジョブがまだ未完了なだけ」の可能性を潰すためアンカーが出るまで待つ。
    // 例外は「近傍グループが全部データなし」= 自ジョブにDL対象が無いケース
    if (st.every((s) => s === 'done' || s === 'no_data')) {
      if (certain) break;
      if (pickedAt !== null && st.every((s) => s === 'no_data')) break;
      console.log('  [poll] 受理jobIdの行が未出現 — 自ジョブの完了を待機中 ...');
    }
    if (Date.now() > deadline) {
      throw new Error(`JOB_TIMEOUT: ${Math.round(JOB_TIMEOUT_MS / 60000)}分待っても完了せず (${group.map((r) => `${r.fileName}:${r.statusText}`).join(', ') || '自ジョブの行を特定できず'})`);
    }
    console.log(`  [poll] ${group.map((r) => `${r.fileName}=${r.statusText}`).join(' / ')}`);
  }

  // ── 取得対象の確定 ──
  const anchor = group.find((r) => r.downloadId === jobId);
  if (!anchor) {
    // 自ジョブのファイルが全部「データなし」の場合。DL対象が無いだけなので
    // 空で返し、呼び元の RESULT_EMPTY に委ねる (他ジョブのCSVは絶対に掴まない)
    console.warn(`  ⚠ 受理jobId ${jobId} の行が無い — DL対象なしとして扱います`);
    return [];
  }
  const mine = [];
  const dropped = [];
  for (const r of group) {
    if (r.acceptedAt !== anchor.acceptedAt) { dropped.push(`${r.fileName}@${r.acceptedAt}`); continue; }
    if (classify(r.statusText) === 'no_data') { mine.push(r); continue; }  // id無しが正常
    const id = Number(r.downloadId);
    if (!Number.isFinite(id)) {
      throw new Error(`JOB_BIND: 完了行 ${r.fileName} からダウンロードIDを取得できず (href="${r.href}")`);
    }
    if (id <= jobIdNum && (maxIdBefore === null || id > maxIdBefore)) mine.push(r);
    else dropped.push(`${r.fileName}#${id}`);
  }
  if (dropped.length) {
    console.warn(`  ⚠ 自ジョブ (受付 ${anchor.acceptedAt} / ${maxIdBefore ?? '-'} < id <= ${jobIdNum}) に合わない行を除外: ${dropped.join(', ')}`);
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

async function run(page, { templateName, sellKey, linefeedDel, outDir, drive }) {
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

  const submitAtMs = Date.now();
  const jobId = await submitCreate(page, { templateId: tmpl.id, sellStatusValue: SELL_STATUS[sellKey], linefeedDel });
  const mine = await waitForJob(page, { beforeKeys, maxIdBefore, jobId, templateName: tmpl.name, submitAtMs });

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
    saved.push({ file: f.name, rows: f.dataRows, latest });
  }

  // ── Google ドライブへ固定名で上書き (全ファイル揃ってから) ──
  if (drive) {
    console.log(`\n  [drive] アップロード先=${drive.remote} folderId=${drive.folderId}`);
    // 2本を1回のAPI呼び出しで差し替える手段は無いので、途中で失敗すると
    // Drive上で item.csv と stock.csv の世代が食い違う。黙って終わらせず、
    // どれが新版でどれが旧版のままかを名指しで報告する
    const done = [];
    try {
      for (const s of saved) {
        uploadToDrive(s.latest, s.file, drive);
        done.push(s.file);
        console.log(`  ☁ ${s.file} を上書き (${s.rows}行)`);
      }
    } catch (e) {
      const rest = saved.map((s) => s.file).filter((f) => !done.includes(f));
      throw new Error(`${e.message}\n  ⚠ Driveの世代が食い違っています: 更新済=${done.join(', ') || 'なし'} / 旧版のまま=${rest.join(', ')}。再実行して揃えてください`);
    }
    console.log(`  https://drive.google.com/drive/folders/${drive.folderId}`);
  } else {
    console.log('\n  [drive] Driveアップロードなし (ローカル保存のみ)');
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
    console.log('使い方: node scripts/mall-csv-fetcher/aupay-item-csv-download.mjs');
    console.log('  [--out <dir>] [--template <名前>] [--sell-status all|selling|ended]');
    console.log('  [--drive-folder <DriveフォルダID>] [--no-drive]');
    return;
  }
  const templateName = args.template || process.env.AITEM_TEMPLATE || TEMPLATE_DEFAULT;
  const sellKey = (args.sellStatus || process.env.AITEM_SELL_STATUS || 'selling').trim();
  if (!SELL_STATUS[sellKey]) {
    console.error(`FATAL: 販売ステータスは all / selling / ended のいずれか (got: ${sellKey})`);
    process.exitCode = 2;
    return;
  }
  const outDir = args.out || process.env.AITEM_OUT_DIR || OUT_DIR_DEFAULT;
  const linefeedDel = (process.env.AITEM_LINEFEED_DEL ?? '1') === '1';
  const notify = process.env.AITEM_NOTIFY === '1';
  let drive = null;
  if (!args.noDrive) {
    const folderId = (args.driveFolder || process.env.AITEM_DRIVE_FOLDER || DRIVE_FOLDER_DEFAULT).trim();
    const remote = (process.env.AITEM_DRIVE_REMOTE || DRIVE_REMOTE_DEFAULT).trim();
    // 既定のDriveフォルダは既定テンプレート専用。別テンプレートの結果を同じ
    // item.csv / stock.csv に上書きすると、中身の違うCSVが黙って入れ替わる。
    // 別テンプレートを使うなら出力先も明示させる
    const usingDefaultFolder = !args.driveFolder && !process.env.AITEM_DRIVE_FOLDER;
    if (usingDefaultFolder && templateName !== TEMPLATE_DEFAULT) {
      console.error(`FATAL: テンプレート「${templateName}」の結果を既定のDriveフォルダへは出せません`);
      console.error(`  既定フォルダは「${TEMPLATE_DEFAULT}」専用です (同じ item.csv/stock.csv を別の中身で上書きしてしまうため)`);
      console.error('  --drive-folder <別フォルダID> で出力先を指定するか、--no-drive を付けてください');
      process.exitCode = 2;
      return;
    }
    if (!DRIVE_ID_RE.test(folderId)) {
      console.error(`FATAL: DriveフォルダIDが不正です (${folderId})`);
      process.exitCode = 2;
      return;
    }
    if (!DRIVE_REMOTE_RE.test(remote)) {
      console.error(`FATAL: rcloneリモート名が不正です (${remote})。末尾のコロンまで含めて指定`);
      process.exitCode = 2;
      return;
    }
    drive = { folderId, remote };
  }

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
    console.log(`Drive: ${drive ? `${drive.remote} folderId=${drive.folderId} (固定名で上書き)` : '(なし)'}`);
    await acquireLock();
    locked = true;
    await mkdir(outDir, { recursive: true });
    context = await openAupayContext();
    page = context.pages()[0] || await context.newPage();
    page.on('dialog', (d) => { console.log(`  [dialog] ${d.message()}`); d.accept().catch(() => {}); });

    const saved = await run(page, { templateName, sellKey, linefeedDel, outDir, drive });
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
