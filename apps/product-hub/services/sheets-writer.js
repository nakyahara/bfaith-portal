/**
 * Google スプレッドシートを作って書く (汎用。撮影指示書 PR-D・デザイナー修正依頼書 PR-F で使う)。2026-10-09
 *
 * - 置き場 = Drive の指定フォルダ (共有ドライブ)。files.create で mimeType = スプレッドシートを作る
 * - 中身の差し替え = 指定したタブ名ごとに clear → values.batchUpdate。**ファイルは作り直さない = URL は変わらない**
 * - 書式 = 太字の行・網かけの行・列幅・折り返し・固定行 (spreadsheets.batchUpdate)
 *
 * 🚨 値は必ず valueInputOption = 'RAW' で書く (呼び手が選べないようにしてある)。
 *    材料は AI の出力と人の入力なので、`=IMPORTXML(...)` `=HYPERLINK(...)` のような値を数式として評価させない
 *
 * 認証: 既存のサービスアカウント (env GOOGLE_SERVICE_ACCOUNT_KEY・base64 JSON)。スコープは drive
 *   (Sheets API は drive スコープでも呼べる。フォルダへの作成に drive が要るので 1 つにまとめる)。
 *   SA が置き場の共有ドライブに「コンテンツ管理者」以上で入っていないと 403 になる。
 * クライアント ({ drive, sheets }) は差し込み式。試験は偽物を渡す (実際の Google には繋がない)
 */
import { google } from 'googleapis';

const SPREADSHEET_MIME = 'application/vnd.google-apps.spreadsheet';
// 1 呼び出しの上限。画面のボタンから同期で待つので長くしすぎない
const GOOGLE_TIMEOUT_MS = 20_000;
const ERROR_MAX_LEN = 300;
// Drive の ID として妥当な文字だけ (クエリ文字列に埋めるので検査する)
const DRIVE_ID_RE = /^[A-Za-z0-9_-]{5,200}$/;
// appProperties のキー・値に使える文字 (クエリ文字列に埋めるので検査する)
const APP_PROP_RE = /^[A-Za-z0-9_.-]{1,100}$/;

/**
 * { drive, sheets } を返す。env が無ければ null (fail-closed: 呼び手は「未設定なので作れません」を出す)。
 * ⚠️ 鍵が壊れていると JSON.parse が throw する — 呼び出し側の try の中で呼ぶこと
 */
export function getSheetsWriteClients(env = process.env) {
  const keyBase64 = env.GOOGLE_SERVICE_ACCOUNT_KEY;
  if (!keyBase64) return null;
  const credentials = JSON.parse(Buffer.from(keyBase64, 'base64').toString('utf-8'));
  const auth = new google.auth.GoogleAuth({ credentials, scopes: ['https://www.googleapis.com/auth/drive'] });
  return { drive: google.drive({ version: 'v3', auth }), sheets: google.sheets({ version: 'v4', auth }) };
}

/** Google の失敗を、画面に出せる日本語の理由にする (どこを直せばいいかが分かる文) */
export function explainGoogleError(e) {
  const status = Number(e?.code || e?.status || e?.response?.status) || null;
  const raw = String(e?.message || e || '');
  const msg = raw.length > ERROR_MAX_LEN ? raw.slice(0, ERROR_MAX_LEN) + '…' : raw;
  if (/has not been used|SERVICE_DISABLED|is disabled|accessNotConfigured/i.test(raw)) {
    return `Google Sheets API (または Drive API) がサービスアカウントのプロジェクトで有効になっていません。管理者が Google Cloud で有効にしてください (${msg})`;
  }
  if (status === 401) return `Google のサービスアカウントで認証できませんでした (鍵を確認してください: ${msg})`;
  if (status === 403 || /insufficient|permission/i.test(raw)) {
    return `書き込みの権限がありません。サービスアカウントを商品フォルダのある共有ドライブに「コンテンツ管理者」以上で追加してください (${msg})`;
  }
  if (status === 404) return `フォルダまたはファイルが見つかりません。画像フォルダの URL と、サービスアカウントから見える場所かを確認してください (${msg})`;
  if (status === 429) return `Google の呼び出し回数の上限に当たりました。少し待ってからもう一度押してください (${msg})`;
  if (/timeout|ETIMEDOUT|ECONNRESET|ENOTFOUND|EAI_AGAIN|socket hang up/i.test(raw)) {
    return `Google に繋がりませんでした (時間切れ・通信の失敗)。少し待ってからもう一度押してください (${msg})`;
  }
  return `Google の処理で失敗しました: ${msg}`;
}

/**
 * フォルダの中に、appProperties (key=value) の付いたスプレッドシートがあれば返す。
 * 「前に作ったけれど DB に書く前に止まった」ファイルを拾い直して、二重に作らないため。
 * 名前では探さない — 人が同じ名前で手作りしたシートを上書きしないように
 * @returns {Promise<{id: string}|null>}
 */
export async function findSpreadsheetByAppProperty({ drive }, { folderId, key, value }) {
  if (!DRIVE_ID_RE.test(String(folderId || ''))) throw new Error('フォルダの ID が不正です');
  if (!APP_PROP_RE.test(String(key || '')) || !APP_PROP_RE.test(String(value || ''))) throw new Error('appProperties の形が不正です');
  const r = await drive.files.list({
    q: `'${folderId}' in parents and trashed = false and mimeType = '${SPREADSHEET_MIME}' and appProperties has { key='${key}' and value='${value}' }`,
    fields: 'files(id, name)',
    pageSize: 5,
    supportsAllDrives: true,
    includeItemsFromAllDrives: true,
    orderBy: 'createdTime',
  }, { timeout: GOOGLE_TIMEOUT_MS });
  const hit = (r?.data?.files || [])[0];
  return hit && hit.id ? { id: hit.id } : null;
}

/**
 * 前に作ったファイルがまだ使えるか (ごみ箱に入っていない・同じフォルダにある)。
 * 消された (404) なら usable=false。それ以外の失敗は throw (権限・通信の失敗で作り直さない)
 */
export async function spreadsheetUsable({ drive }, { fileId, folderId }) {
  if (!DRIVE_ID_RE.test(String(fileId || ''))) return { usable: false, reason: 'invalid_id' };
  let meta;
  try {
    meta = await drive.files.get({ fileId, fields: 'id, trashed, parents, mimeType', supportsAllDrives: true }, { timeout: GOOGLE_TIMEOUT_MS });
  } catch (e) {
    if (Number(e?.code || e?.status || e?.response?.status) === 404) return { usable: false, reason: 'not_found' };
    throw e;
  }
  const d = meta?.data || {};
  if (d.trashed) return { usable: false, reason: 'trashed' };
  if (d.mimeType && d.mimeType !== SPREADSHEET_MIME) return { usable: false, reason: 'not_spreadsheet' };
  if (folderId && Array.isArray(d.parents) && !d.parents.includes(folderId)) return { usable: false, reason: 'moved' };
  return { usable: true, reason: null };
}

/**
 * フォルダの中に空のスプレッドシートを作る。権限はフォルダから継承 (リンク共有は付けない)
 * @returns {Promise<{id: string}>}
 */
export async function createSpreadsheetInFolder({ drive }, { folderId, title, appProperties = {} }) {
  if (!DRIVE_ID_RE.test(String(folderId || ''))) throw new Error('フォルダの ID が不正です');
  const r = await drive.files.create({
    requestBody: { name: String(title), mimeType: SPREADSHEET_MIME, parents: [folderId], appProperties },
    fields: 'id',
    supportsAllDrives: true,
  }, { timeout: GOOGLE_TIMEOUT_MS });
  const id = r?.data?.id;
  if (!id) throw new Error('作ったスプレッドシートの ID が返ってきませんでした');
  return { id };
}

/** シート名を A1 記法の範囲に (`'` は `''` に) */
const a1Sheet = (name) => `'${String(name).replace(/'/g, "''")}'`;

/**
 * スプレッドシートの中身を差し替える。
 * @param {{sheets, drive}} clients
 * @param {object} o
 * @param {string} o.spreadsheetId
 * @param {string} [o.title]  ファイル名 (違っていれば付け直す。撮影の種類を変えたとき)
 * @param {Array<{name: string, rows: string[][], format?: object}>} o.tabs  書くタブ (この順に並べる)
 * @param {string[]} [o.removeTabs]  あれば消すタブ名 (自分が作るタブのうち、今回は要らないもの)。人が足したタブは消さない
 * @param {boolean} [o.fresh]  作ったばかり (最初からある「シート1」を消す)
 */
export async function writeSpreadsheet({ sheets, drive }, { spreadsheetId, title, tabs, removeTabs = [], fresh = false }) {
  const opt = { timeout: GOOGLE_TIMEOUT_MS };
  const got = await sheets.spreadsheets.get({ spreadsheetId, fields: 'sheets.properties(sheetId,title,index)' }, opt);
  const existing = (got?.data?.sheets || []).map((s) => s.properties || {});
  const byTitle = new Map(existing.map((p) => [p.title, p]));
  const wanted = new Set(tabs.map((t) => t.name));

  // 1. 足りないタブを足し、要らないタブを消す (足してから消す = 最後の 1 枚を消して失敗しない)
  const structure = [];
  for (const t of tabs) if (!byTitle.has(t.name)) structure.push({ addSheet: { properties: { title: t.name } } });
  for (const p of existing) {
    if (wanted.has(p.title)) continue;
    if (fresh || removeTabs.includes(p.title)) structure.push({ deleteSheet: { sheetId: p.sheetId } });
  }
  if (structure.length) {
    const r = await sheets.spreadsheets.batchUpdate({ spreadsheetId, requestBody: { requests: structure } }, opt);
    for (const rep of r?.data?.replies || []) {
      const p = rep?.addSheet?.properties;
      if (p) byTitle.set(p.title, p);
    }
  }
  const sheetIdOf = (name) => {
    const p = byTitle.get(name);
    if (!p || p.sheetId == null) throw new Error(`タブ「${name}」を用意できませんでした`);
    return p.sheetId;
  };

  // 2. 値: タブごとに消してから書く (前の版の行が残らない)。RAW = 数式として評価させない
  await sheets.spreadsheets.values.batchClear({ spreadsheetId, requestBody: { ranges: tabs.map((t) => a1Sheet(t.name)) } }, opt);
  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId,
    requestBody: {
      valueInputOption: 'RAW',
      data: tabs.map((t) => ({ range: `${a1Sheet(t.name)}!A1`, majorDimension: 'ROWS', values: t.rows.map((row) => row.map((c) => (c == null ? '' : String(c)))) })),
    },
  }, opt);

  // 3. 書式: 一度まっさらにしてから付け直す (前の版の太字が別の行に残らない)
  const fmt = [];
  tabs.forEach((t, index) => {
    const sheetId = sheetIdOf(t.name);
    const f = t.format || {};
    const width = Math.max(1, ...t.rows.map((r) => r.length));
    fmt.push({ updateSheetProperties: { properties: { sheetId, index, gridProperties: { frozenRowCount: Number(f.frozenRows) || 0 } }, fields: 'index,gridProperties.frozenRowCount' } });
    fmt.push({ repeatCell: {
      range: { sheetId },
      cell: { userEnteredFormat: { wrapStrategy: f.wrap ? 'WRAP' : 'OVERFLOW_CELL', verticalAlignment: 'TOP' } },
      fields: 'userEnteredFormat',
    } });
    for (const row of f.boldRows || []) {
      fmt.push({ repeatCell: {
        range: { sheetId, startRowIndex: row, endRowIndex: row + 1, startColumnIndex: 0, endColumnIndex: width },
        cell: { userEnteredFormat: { textFormat: { bold: true } } },
        fields: 'userEnteredFormat.textFormat.bold',
      } });
    }
    for (const row of f.shadedRows || []) {
      fmt.push({ repeatCell: {
        range: { sheetId, startRowIndex: row, endRowIndex: row + 1, startColumnIndex: 0, endColumnIndex: width },
        cell: { userEnteredFormat: { backgroundColor: { red: 0.93, green: 0.95, blue: 0.97 } } },
        fields: 'userEnteredFormat.backgroundColor',
      } });
    }
    (f.columnWidths || []).forEach((px, i) => {
      fmt.push({ updateDimensionProperties: {
        range: { sheetId, dimension: 'COLUMNS', startIndex: i, endIndex: i + 1 },
        properties: { pixelSize: Number(px) || 100 },
        fields: 'pixelSize',
      } });
    });
  });
  if (fmt.length) await sheets.spreadsheets.batchUpdate({ spreadsheetId, requestBody: { requests: fmt } }, opt);

  // 4. ファイル名 (違うときだけ)
  if (title && drive) {
    const meta = await drive.files.get({ fileId: spreadsheetId, fields: 'name', supportsAllDrives: true }, opt);
    if (meta?.data?.name !== title) {
      await drive.files.update({ fileId: spreadsheetId, requestBody: { name: title }, fields: 'id', supportsAllDrives: true }, opt);
    }
  }
}
