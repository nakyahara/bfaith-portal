/**
 * 白抜き画像の受信箱 (2026-09-14 中原さん要望)。
 *
 * 仕入先 0001 (AM Craft・大畑さん) からメールで届く白抜き商品画像は、Drive の受信箱フォルダ
 * 「大畑さんから送られてきた添付ファイル」へ自動保存されている (メールの添付を Drive へ保存する
 * 仕組みはこのアプリの外)。画像タブの「📥 受信箱から選ぶ」で受信箱の画像を 1 枚選ぶと:
 *   ① 「商品コード_00.<拡張子>」に改名して商品の画像フォルダへ移動する (= 受信箱の一覧から消える)
 *   ② 白抜き背景 (draft_rakuten.white_bg_*) として登録する
 *
 * 設計:
 *   - 受信箱にある画像しか受け付けない (fail-closed: Drive に親フォルダを聞いて確かめる)。
 *     別の商品で先に登録されて移動済みなら 409 = 二重登録・横取りを防ぐ
 *   - 移動先 = カードの画像フォルダ (drive_folder_url)。単品でフォルダが無ければ
 *     drive-image-folder の自動作成でその場で作る。セット派生でフォルダが無ければ移動しない (登録だけ)
 *   - 🚨 移動先があるのに移動できなければ、登録もしない (2026-09-18 中原さん判断)。当初は「移動は best-effort・
 *     登録が主目的」で、失敗しても警告つきで登録していた。ところが SA が受信箱で「閲覧者」のままだったため
 *     9/14〜9/18 の登録が全件この経路に落ち、画面には白抜きが入っているのに Drive には来ない、という
 *     食い違いだけが黙って積み上がった (警告は alert 1 回きりで、画面を読み直すと消えるので誰も気づけない)。
 *     いまは「画面と Drive が食い違うぐらいなら、何も起きなかったことにして人にやり直させる」を選ぶ:
 *       登録の前に  capabilities (canEdit / canMoveItemWithinDrive) を聞き、動かせないと分かっていれば 403 で止める
 *       移動したあと Drive に所在を聞き直して分岐する (Codex R2):
 *         移動先に新しい名前である = 移動は届いて応答だけ落ちた → 移動済みとして登録
 *         受信箱にまだある        = 移動できていない → 登録しない (502)
 *         別の場所 / 消えた       = 誰かが動かした → 登録しない (409)
 *         所在が分からない        = Drive が続けて失敗 → 登録しない (502。受信箱に無い画像を登録しないため)
 *     移動先がそもそも無いとき (セット派生・フォルダを作れなかった) は従来どおり登録だけする = 仕様
 *   - 移動先に同じ枠 (商品コード_00.*) のファイルが既にあれば「_旧<日時>」へ改名して退ける。消さない (人が戻せる)。
 *     退けないと同じ番号が 2 枚になり「フォルダから自動セット」が止まる。退けるのは **移動が成功したあと**:
 *     先に退けると、移動に失敗したときだけ「旧ファイルの名前は変わったのに新しい画像は来ていない」状態が残る
 *   - 受信箱の一覧では、既にどれかの商品の白抜きになっているファイルに「登録済み: 商品コード」を付ける
 *     (9/14〜9/18 に受信箱へ残ったまま登録された画像を、別の商品で黙って使ってしまわないように)。
 *     一覧は受信箱に書き込めるか (canAddChildren) も返す = 画像を選ぶ前に権限不足に気づける
 *   - 登録は 1 本ずつ (直列化・Codex R1 high): 親フォルダの確認 → 移動 → 退避 → 登録 の間に別の登録が割り込むと、
 *     同じ画像が 2 商品に登録されたり同じ商品に _00 が 2 枚できたりする。Render は 1 プロセスなのでプロセス内の
 *     直列化で足りる (人がボタンを押す頻度の処理)。移動に失敗したときの所在確認は上の分岐
 *   - 登録せずに終わるとき (403 / 409 / 502) は draft_events (white_bg_inbox_failed) に残す (Codex R2 low)
 *   - SA = GOOGLE_SERVICE_ACCOUNT_KEY (drive scope・drive-image-folder と同じ client)。
 *     受信箱と商品フォルダの両方で SA に「コンテンツ管理者」以上の権限が要る (共有ドライブ側で付与する)
 *
 * 環境変数:
 *   PH_WHITE_BG_INBOX_FOLDER_ID … 受信箱フォルダの Drive ID。既定は DEFAULT_INBOX
 */
import { getDB, logEvent } from '../db.js';
import { parseDriveLink, fileViewUrl, DRIVE_FILE_ID_PATTERN } from '../lib/drive-link.js';
import { parseImageFileName } from '../lib/folder-import.js';
import { attemptImageFolderCreation, getDriveWriteClient, isSingleProductDraft } from './drive-image-folder.js';

// 中原さん指定の受信箱 (2026-09-14)。共有ドライブ直下の「大畑さんから送られてきた添付ファイル」
const DEFAULT_INBOX = '1rPzsWJaFqo4pW0JCm77ZjkXhpyG7fF4N';

// Drive の ID として妥当な文字だけ許す (drive-image-folder と同じ理由: env の誤設定でクエリを壊さない)
const DRIVE_ID_RE = /^[A-Za-z0-9_-]{5,}$/;
// Drive API 1 呼び出しの上限 (人がボタンを押して待っている)
const DRIVE_TIMEOUT_MS = 15_000;
// 受信箱の一覧に出す上限。メールの添付置き場なので溜まりうるが、選ぶ画面で数百枚以上は見ない
export const INBOX_MAX_FILES = 500;
const ERROR_MAX_LEN = 300;

// 受信箱の一覧で見せた画像 (サムネイルのプロキシが許可するため)。
// /api/thumb は「product-hub に登録済みの画像」しか返さない (SA は Drive を広く読めるので任意の ID を覗かせない)。
// 受信箱の画像はまだ登録されていないので、そのままでは一覧のサムネイルが全部 404 になる
// (2026-09-14 中原さん指摘「画像が見えない」)。一覧を返したときに SA が受信箱で実際に見た ID だけを、期限つきで許可する。
// Render は 1 プロセスなのでメモリで足りる (再起動で消えたら一覧を開き直せば戻る)
export const INBOX_THUMB_TTL_MS = 30 * 60 * 1000;
const inboxSeen = new Map(); // fileId → { modifiedTime, at }

function rememberInboxFiles(files, now = Date.now()) {
  for (const [id, v] of inboxSeen) if (now - v.at > INBOX_THUMB_TTL_MS) inboxSeen.delete(id);
  for (const f of files) {
    inboxSeen.delete(f.id);
    inboxSeen.set(f.id, { modifiedTime: f.modifiedTime || null, at: now });
  }
  // 古い順に捨てる (一覧を何度開いても際限なく増えない)
  while (inboxSeen.size > INBOX_MAX_FILES * 2) inboxSeen.delete(inboxSeen.keys().next().value);
}

/** 受信箱の一覧で最近見せた画像なら { modifiedTime } (サムネイルの版数の期待値)。見せていなければ null */
export function inboxThumbRef(fileId, now = Date.now()) {
  const key = String(fileId || '');
  const v = inboxSeen.get(key);
  if (!v) return null;
  if (now - v.at > INBOX_THUMB_TTL_MS) { inboxSeen.delete(key); return null; }
  return { modifiedTime: v.modifiedTime };
}

export function whiteBgInboxFolderId() {
  const env = String(process.env.PH_WHITE_BG_INBOX_FOLDER_ID || '').trim();
  if (!env) return DEFAULT_INBOX;
  if (!DRIVE_ID_RE.test(env)) {
    console.warn(`[product-hub] PH_WHITE_BG_INBOX_FOLDER_ID が Drive ID の形式ではないため既定を使います: ${env}`);
    return DEFAULT_INBOX;
  }
  return env;
}

export function whiteBgInboxFolderUrl() {
  return folderUrl(whiteBgInboxFolderId());
}

function folderUrl(id) {
  return `https://drive.google.com/drive/folders/${id}`;
}

function truncateError(e) {
  const msg = e && e.message ? String(e.message) : String(e);
  return msg.length > ERROR_MAX_LEN ? `${msg.slice(0, ERROR_MAX_LEN)}…` : msg;
}

const EXT_BY_MIME = { 'image/jpeg': 'jpg', 'image/png': 'png', 'image/webp': 'webp', 'image/gif': 'gif' };

/**
 * 登録後のファイル名「商品コード_00.<拡張子>」。拡張子は元ファイルから (無ければ mimeType から。
 * それも分からなければ jpg)。lib/folder-import.js の parseImageFileName がこの名前を _00 (白抜き) と読む
 */
export function whiteBgFileName(neCode, originalName, mimeType) {
  const m = String(originalName || '').match(/\.([a-zA-Z0-9]{1,5})$/);
  let ext = m ? m[1].toLowerCase() : '';
  if (ext === 'jpeg') ext = 'jpg';
  if (!ext) ext = EXT_BY_MIME[String(mimeType || '').toLowerCase()] || 'jpg';
  return `${String(neCode || '').trim()}_00.${ext}`;
}

/** JST の「YYYYMMDD-HHmm」 (退けたファイルの名前に付ける) */
function jstStamp(date) {
  const t = new Date(date.getTime() + 9 * 60 * 60 * 1000);
  const p = (n) => String(n).padStart(2, '0');
  return `${t.getUTCFullYear()}${p(t.getUTCMonth() + 1)}${p(t.getUTCDate())}-${p(t.getUTCHours())}${p(t.getUTCMinutes())}`;
}

/** JST の「YYYY/MM/DD HH:mm」 (受信箱の一覧の表示用)。読めない日時は空文字 */
export function jstDisplay(iso) {
  const ms = Date.parse(iso || '');
  if (!Number.isFinite(ms)) return '';
  const t = new Date(ms + 9 * 60 * 60 * 1000);
  const p = (n) => String(n).padStart(2, '0');
  return `${t.getUTCFullYear()}/${p(t.getUTCMonth() + 1)}/${p(t.getUTCDate())} ${p(t.getUTCHours())}:${p(t.getUTCMinutes())}`;
}

/**
 * 退けるファイルの名前。「abc_00.jpg」→「abc_00_旧20260914-1030.jpg」。
 * 🚨 末尾が「_数字.拡張子」にならないこと: parseImageFileName が枠として読んでしまうと、
 *    退けたはずのファイルが「フォルダから自動セット」で別の枠に入る
 */
export function parkedName(originalName, at = new Date()) {
  const name = String(originalName || '');
  const m = name.match(/^(.*)\.([a-zA-Z0-9]+)$/);
  const stamp = jstStamp(at);
  return m ? `${m[1]}_旧${stamp}.${m[2]}` : `${name}_旧${stamp}`;
}

/**
 * メール添付の自動保存が Drive の説明欄に書く「From: / Subject: / Date:」から差出人と件名を取り出す。
 * 形式が違えば null のまま (表示に使うだけ)
 */
export function parseMailDescription(description) {
  const out = { from: null, subject: null };
  for (const line of String(description || '').split(/\r?\n/)) {
    const m = line.match(/^(From|Subject):\s*(.*)$/i);
    if (!m) continue;
    const key = m[1].toLowerCase();
    if (key === 'from' && out.from == null) out.from = m[2].trim();
    if (key === 'subject' && out.subject == null) out.subject = m[2].trim();
  }
  return out;
}

/** 自動保存が付ける「日時_ハッシュ_」の頭を落とした表示名 (元の添付ファイル名に近づける) */
export function inboxDisplayName(name) {
  return String(name || '').replace(/^\d{8}_\d{6}_[0-9a-f]{8,}_/i, '');
}

/**
 * 受信箱にある画像のうち、既にどれかの商品の白抜き背景になっているものを引く。
 * @returns {Map<string, string>} drive_file_id → 商品コード
 */
export function registeredWhiteBgCodes(db, fileIds) {
  const map = new Map();
  const ids = [...new Set((fileIds || []).filter(Boolean))];
  for (let i = 0; i < ids.length; i += 500) {
    const chunk = ids.slice(i, i + 500);
    const rows = db.prepare(`
      SELECT r.white_bg_drive_file_id AS fid, d.ne_code
        FROM draft_rakuten r JOIN product_drafts d ON d.id = r.draft_id
       WHERE r.white_bg_drive_file_id IN (${chunk.map(() => '?').join(',')})
       ORDER BY r.draft_id
    `).all(...chunk);
    for (const r of rows) if (!map.has(r.fid)) map.set(r.fid, r.ne_code);
  }
  return map;
}

function driveClientOrThrow(driveClient) {
  const drive = driveClient || getDriveWriteClient();
  if (!drive) {
    const e = new Error('Drive の設定 (GOOGLE_SERVICE_ACCOUNT_KEY) が無いため受信箱を読めません');
    e.statusCode = 503;
    throw e;
  }
  return drive;
}

/**
 * 受信箱の画像一覧 (新しい順)。
 * @param {{driveClient?: object, db?: object}} [opts] driveClient は smoke 用の注入口
 * @returns {Promise<{files: Array<{id, name, displayName, mimeType, createdTime, modifiedTime, receivedAt, mailFrom, mailSubject, registeredFor}>,
 *   truncated: boolean, folderUrl: string, writable: boolean|null}>} writable=false なら SA が受信箱を読めるだけ = 選んでも移動できない
 */
export async function listWhiteBgInbox({ driveClient = null, db = null } = {}) {
  const drive = driveClientOrThrow(driveClient);
  const inboxId = whiteBgInboxFolderId();
  const raw = [];
  let pageToken;
  let truncated = false;
  do {
    const res = await drive.files.list({
      q: `'${inboxId}' in parents and trashed = false and mimeType contains 'image/'`,
      fields: 'nextPageToken, files(id, name, mimeType, createdTime, modifiedTime, description)',
      pageSize: 200,
      orderBy: 'createdTime desc',
      supportsAllDrives: true,
      includeItemsFromAllDrives: true,
      pageToken,
    }, { timeout: DRIVE_TIMEOUT_MS });
    for (const f of res.data?.files || []) {
      if (!String(f.mimeType || '').startsWith('image/')) continue;
      raw.push(f);
    }
    pageToken = res.data?.nextPageToken;
    if (raw.length >= INBOX_MAX_FILES) {
      truncated = raw.length > INBOX_MAX_FILES || !!pageToken;
      break;
    }
  } while (pageToken);
  // Drive の orderBy に頼り切らず、こちらでも新しい順に揃える (受信箱は「最近届いたもの」を探す場所)
  raw.sort((a, b) => (Date.parse(b.createdTime || '') || 0) - (Date.parse(a.createdTime || '') || 0));
  const picked = raw.slice(0, INBOX_MAX_FILES);
  const registered = registeredWhiteBgCodes(db || getDB(), picked.map((f) => f.id));
  const files = picked.map((f) => {
    const mail = parseMailDescription(f.description);
    return {
      id: f.id,
      name: f.name,
      displayName: inboxDisplayName(f.name),
      mimeType: f.mimeType,
      createdTime: f.createdTime || null,
      modifiedTime: f.modifiedTime || null,
      receivedAt: jstDisplay(f.createdTime),
      mailFrom: mail.from,
      mailSubject: mail.subject,
      registeredFor: registered.get(f.id) || null,
    };
  });
  rememberInboxFiles(files);
  return { files, truncated, folderUrl: folderUrl(inboxId), writable: await inboxWritable(drive, inboxId) };
}

/**
 * 受信箱フォルダに SA が書き込めるか (2026-09-18)。読めるだけだと画像を選んでも移動できないので、
 * 選ぶ前に画面で知らせる。確かめられなければ null (一覧は出す = 一覧が権限確認で落ちるほうが困る)
 */
async function inboxWritable(drive, inboxId) {
  try {
    const r = await drive.files.get({
      fileId: inboxId, fields: 'id, capabilities(canEdit, canAddChildren)', supportsAllDrives: true,
    }, { timeout: DRIVE_TIMEOUT_MS });
    const cap = r.data?.capabilities;
    if (!cap) return null;
    return cap.canEdit !== false && cap.canAddChildren !== false;
  } catch (_) {
    return null;
  }
}

function folderIdOf(url) {
  const p = parseDriveLink(url);
  return p && p.type === 'folder' && DRIVE_ID_RE.test(p.id) ? p.id : null;
}

/** 移動先フォルダ直下の画像一覧 (同じ枠のファイルを探すため)。商品フォルダは小さいので 1000 件で打ち切る */
async function listFolderImages(drive, folderId) {
  const files = [];
  let pageToken;
  do {
    const res = await drive.files.list({
      q: `'${folderId}' in parents and trashed = false and mimeType contains 'image/'`,
      fields: 'nextPageToken, files(id, name, mimeType)',
      pageSize: 200,
      supportsAllDrives: true,
      includeItemsFromAllDrives: true,
      pageToken,
    }, { timeout: DRIVE_TIMEOUT_MS });
    files.push(...(res.data?.files || []));
    pageToken = res.data?.nextPageToken;
  } while (pageToken && files.length < 1000);
  return files;
}

/**
 * 移動先に「商品コード_00.*」が既にあれば「_旧<日時>」へ改名して退ける (消さない)。
 * 拡張子違い (png と jpg) も同じ枠なので、名前の完全一致ではなく parseImageFileName で判定する
 * @param {string[]} opts.parked 退けたあとの名前を逐次 push する配列。途中で throw しても成功分が呼び出し側に残る (Codex R1 low)
 */
async function parkExistingWhiteBg(drive, { folderId, neCode, exceptFileId, now, parked }) {
  const code = String(neCode || '').trim().toLowerCase();
  for (const f of await listFolderImages(drive, folderId)) {
    if (f.id === exceptFileId) continue;
    const p = parseImageFileName(f.name);
    if (!p || p.kind !== 'white' || String(p.base).trim().toLowerCase() !== code) continue;
    const name = parkedName(f.name, now());
    await drive.files.update({
      fileId: f.id, requestBody: { name }, fields: 'id, name', supportsAllDrives: true,
    }, { timeout: DRIVE_TIMEOUT_MS });
    parked.push(name);
  }
}

/**
 * 移動に失敗したあと、ファイルがいまどこにあるかを聞き直す (Codex R2)。
 * @returns {Promise<{status: 'ok', parents: string[], name: string, trashed: boolean, modifiedTime: string|null}
 *   | {status: 'gone'} | {status: 'unknown', error: string}>}  gone = 404 (消えた) / unknown = 確認できない
 */
async function locateFile(drive, fileId) {
  try {
    const r = await drive.files.get({
      fileId, fields: 'id, name, parents, trashed, modifiedTime', supportsAllDrives: true,
    }, { timeout: DRIVE_TIMEOUT_MS });
    const d = r.data || {};
    return { status: 'ok', parents: d.parents || [], name: d.name || '', trashed: !!d.trashed, modifiedTime: d.modifiedTime || null };
  } catch (e) {
    if (Number(e?.code || e?.response?.status || 0) === 404) return { status: 'gone' };
    return { status: 'unknown', error: truncateError(e) };
  }
}

/**
 * 登録せずに終わるときの記録 (画面を閉じても操作履歴で追える — Codex R2 low)。
 * 退避は移動が成功したあとにしか行わないので、ここまで来た時点で Drive は何も変わっていない
 */
function logFailure(db, draftId, reason, actor) {
  try {
    logEvent(db, draftId, 'white_bg_inbox_failed', reason, actor);
  } catch (_) { /* fail-soft */ }
}

const fail = (status, error) => ({ ok: false, status, error });

/**
 * 受信箱の画像 1 枚を、この商品の白抜き背景 (_00) として登録し、商品の画像フォルダへ移動する。
 * **throw しない** (結果は ok/status/error で返す。router はそのまま JSON にする)。
 * @param {number} draftId
 * @param {string} fileId 受信箱にある Drive ファイル ID
 * @param {{actor?: string|null, driveClient?: object, now?: () => Date}} [opts]
 * @returns {Promise<{ok: true, fileId: string, name: string, originalName: string, moved: boolean,
 *   folderUrl: string|null, parked: string[], warnings: string[]} | {ok: false, status: number, error: string}>}
 */
let registerChain = Promise.resolve();
export function registerWhiteBgFromInbox(draftId, fileId, opts = {}) {
  // 1 本ずつ (Codex R1 high)。前の登録が失敗していても次は動く (chain は常に resolve させる)
  const run = () => doRegister(draftId, fileId, opts)
    .catch((e) => fail(500, `白抜き背景の登録に失敗しました: ${truncateError(e)}`));
  const p = registerChain.then(run, run);
  registerChain = p.catch(() => {});
  return p;
}

async function doRegister(draftId, fileId, { actor = null, driveClient = null, now = () => new Date() } = {}) {
  const db = getDB();
  const draft = db.prepare(
    'SELECT id, ne_code, name, drive_folder_url, parent_draft_id, provisional_code FROM product_drafts WHERE id = ?',
  ).get(draftId);
  if (!draft) return fail(404, '商品が見つかりません');
  const id = String(fileId || '').trim();
  if (!DRIVE_FILE_ID_PATTERN.test(id)) return fail(400, '画像の ID が不正です');

  let drive;
  try {
    drive = getDriveWriteClientSafe(driveClient);
  } catch (e) {
    return fail(503, `Drive の鍵 (GOOGLE_SERVICE_ACCOUNT_KEY) を読めません: ${truncateError(e)}`);
  }
  if (!drive) return fail(503, 'Drive の設定 (GOOGLE_SERVICE_ACCOUNT_KEY) が無いため登録できません');
  const inboxId = whiteBgInboxFolderId();

  // 受信箱にあるファイルだけ (fail-closed)。別の商品で先に登録されていれば親が変わっているので 409
  let meta;
  try {
    const r = await drive.files.get({
      fileId: id,
      // capabilities = 移動できるかの事前確認 (2026-09-18)。fields で頼まないと返らない
      fields: 'id, name, mimeType, parents, trashed, modifiedTime, capabilities(canEdit, canMoveItemWithinDrive)',
      supportsAllDrives: true,
    }, { timeout: DRIVE_TIMEOUT_MS });
    meta = r.data || {};
  } catch (e) {
    const status = Number(e?.code || e?.response?.status || 0);
    if (status === 404) return fail(404, '受信箱にその画像がありません (一覧を読み直してください)');
    return fail(502, `画像の情報を取得できませんでした: ${truncateError(e)}`);
  }
  if (meta.trashed || !(meta.parents || []).includes(inboxId)) {
    return fail(409, 'その画像はもう受信箱にありません (別の商品で登録済みかもしれません)。一覧を読み直してください');
  }
  if (!String(meta.mimeType || '').startsWith('image/')) return fail(400, '画像ファイルではありません');

  const newName = whiteBgFileName(draft.ne_code, meta.name, meta.mimeType);
  const warnings = [];

  // 移動先 = 商品の画像フォルダ。単品でまだ無ければその場で作る (カード作成時と同じ関数・冪等)
  let destId = folderIdOf(draft.drive_folder_url);
  if (!destId && isSingleProductDraft(draft)) {
    const made = await attemptImageFolderCreation(draft.id, { actor, driveClient: drive });
    destId = folderIdOf(made.url);
    if (!destId) {
      warnings.push(`商品の画像フォルダを作れなかったため、画像は受信箱に残っています${made.error ? ` (${made.error})` : ''}。`
        + '基本情報に画像フォルダのリンクを貼ってから、もう一度「受信箱から選ぶ」を押すと移動できます');
    }
  } else if (!destId) {
    warnings.push('セット商品には画像フォルダが無いため、画像は受信箱に残しました。'
      + '移動したいときは画像タブの「画像フォルダから自動セット」の欄にフォルダのリンクを入れて保存してから、もう一度「受信箱から選ぶ」を押してください');
  }

  let moved = false;
  const parked = [];
  let modifiedTime = meta.modifiedTime || null;
  if (destId) {
    // 🚨 移動できるかを先に Drive に聞く (2026-09-18)。SA が受信箱で「閲覧者」のままだと移動だけが必ず失敗し、
    // 画面には白抜きが入っているのに Drive には来ない食い違いが黙って積み上がる (9/14〜9/18 に実際に起きた)。
    // capabilities は fields で頼んだときだけ返る。返らない Drive では判定せず、実際に移動して確かめる
    const cap = meta.capabilities || {};
    if (cap.canEdit === false || cap.canMoveItemWithinDrive === false) {
      logFailure(db, draft.id,
        `受信箱から動かす権限が無いため登録を中止 (canEdit=${cap.canEdit} / canMoveItemWithinDrive=${cap.canMoveItemWithinDrive})`, actor);
      return fail(403, 'この画像を受信箱から動かす権限がないため、白抜き背景は登録していません。'
        + 'Drive で受信箱フォルダのサービスアカウント (bfaith-portal@…) を「コンテンツ管理者」にしてから、もう一度お試しください');
    }
    try {
      const r = await drive.files.update({
        fileId: id,
        addParents: destId,
        removeParents: inboxId,
        requestBody: { name: newName },
        fields: 'id, name, modifiedTime, parents',
        supportsAllDrives: true,
      }, { timeout: DRIVE_TIMEOUT_MS });
      moved = true;
      if (r.data?.modifiedTime) modifiedTime = r.data.modifiedTime;
    } catch (e) {
      // 移動に失敗。ファイルの所在を聞き直して分岐する (ヘッダーの「移動できなければ登録しない」参照)
      const err = truncateError(e);
      const loc = await locateFile(drive, id);
      const live = loc.status === 'ok' && !loc.trashed;
      // 移動済み = 移動先にあり **受信箱には無く** 新しい名前 (両方の親を含む中途半端な状態は「受信箱にある」側で扱う — Codex R3 low)
      if (live && loc.parents.includes(destId) && !loc.parents.includes(inboxId) && loc.name === newName) {
        // 移動は届いていて応答だけ落ちた (Codex R2 medium) → 移動済みとして登録へ
        moved = true;
        if (loc.modifiedTime) modifiedTime = loc.modifiedTime;
        warnings.push(`移動の応答が確認できませんでしたが、商品フォルダに ${newName} があるので移動済みとして登録しました (${err})`);
      } else if (live && loc.parents.includes(inboxId)) {
        // 受信箱に残ったまま = 移動できていない → 登録もしない (2026-09-18 中原さん判断: やり直させる)
        logFailure(db, draft.id, `商品フォルダへ移動できず登録を中止 (${err})`, actor);
        return fail(502, `商品フォルダへ移動できなかったため、白抜き背景は登録していません (${err})。`
          + 'サービスアカウントに受信箱と商品フォルダの編集権限 (コンテンツ管理者) があるか確認してから、もう一度お試しください');
      } else if (loc.status === 'unknown') {
        // 所在が分からない = Drive が続けて失敗 (Codex R2 high)。受信箱に無いかもしれない画像は登録しない
        logFailure(db, draft.id, `移動に失敗し所在も確認できず登録を中止 (${err} / ${loc.error})`, actor);
        return fail(502, `商品フォルダへの移動に失敗し、画像が受信箱に残っているかも確認できませんでした (${err})。`
          + 'しばらくしてからやり直してください');
      } else {
        // 受信箱にも移動先にも無い (別の場所・ゴミ箱・消えた) = 誰かが動かした → 登録しない (Codex R1 high)
        logFailure(db, draft.id, `移動に失敗し、画像はもう受信箱に無いため登録を中止 (${err})`, actor);
        return fail(409, 'その画像はもう受信箱にありません (別の商品で登録済みかもしれません)。一覧を読み直してください');
      }
    }
    // 前の _00 を退けるのは移動のあと (2026-09-18)。先に退けると、移動が失敗したときだけ
    // 「旧ファイルの名前は変わったのに新しい画像は来ていない」状態が商品フォルダに残る。
    // この順なら移動が失敗した時点で Drive は一切変わっていない (やり直せば済む)。
    // 退ける前は同じ名前が一瞬 2 枚あるが、退けたあとは 1 枚に戻る
    try {
      await parkExistingWhiteBg(drive, { folderId: destId, neCode: draft.ne_code, exceptFileId: id, now, parked });
    } catch (e) {
      warnings.push(`前の白抜き背景を「_旧」に退けられませんでした (${truncateError(e)})。`
        + `商品フォルダに ${newName} が 2 枚あると「フォルダから自動セット」が止まるので、古いほうの名前を変えてください`);
    }
  }

  // 登録。ここへ来るのは「移動できた」か「移動先がそもそも無い (セット派生・フォルダ未作成)」のどちらか。
  // 移動先があるのに移動できなかったときは上で return 済み。フォルダ取込 (applyFolderImport) と同じ書き方
  db.transaction(() => {
    db.prepare(`
      INSERT INTO draft_rakuten (draft_id, white_bg_drive_file_id, white_bg_drive_url, white_bg_modified_time) VALUES (?, ?, ?, ?)
      ON CONFLICT(draft_id) DO UPDATE SET
        white_bg_drive_file_id = excluded.white_bg_drive_file_id,
        white_bg_drive_url = excluded.white_bg_drive_url,
        white_bg_modified_time = excluded.white_bg_modified_time,
        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
    `).run(draft.id, id, fileViewUrl(id), modifiedTime);
    logEvent(db, draft.id, 'white_bg_set_from_inbox',
      `${meta.name} → ${moved ? `${newName} (商品フォルダへ移動)` : '受信箱に残したまま登録'}`
        + (parked.length ? ` / 退けた旧ファイル: ${parked.join(', ')}` : ''),
      actor);
  })();

  return {
    ok: true,
    fileId: id,
    name: moved ? newName : meta.name,
    originalName: meta.name,
    moved,
    folderUrl: destId ? folderUrl(destId) : null,
    parked,
    warnings,
  };
}

/** 鍵が壊れていると getDriveWriteClient は throw する。呼び出し側で 503 にするため分けておく */
function getDriveWriteClientSafe(driveClient) {
  return driveClient || getDriveWriteClient();
}
