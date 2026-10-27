/**
 * 単品カード作成時に Google Drive へ画像フォルダを自動作成する (2026-08-27 中原さん指示)。
 *
 * 運用: 新しい単品商品のカードを作ったら、画像置き場の親フォルダ直下に
 *       「商品コード_商品名」のフォルダを作り、カードの drive_folder_url に貼る。
 *       (これまで人手でやっていた作業の自動化)
 *
 * 🆕 2026-10-27 中原さん「全部の商品にフォルダを」: 入ってくる商品と入ってこない商品があった。原因は
 *   ① Company DB の新商品の登録から来るカード (cdb-card-intake / cdb-group-intake = 今の主な入口) が作っていなかった
 *   ② セット (派生・登録) を対象外にしていた
 *   ③ 失敗の回収が NE の自動取込 (切替で閉じた) の中でしか動いていなかった
 *   → 対象を「商品コードが確定したカード全部」(セットも・仮コードのあいだは待つ) にし、見回り (sweepMissingImageFolders) が
 *     URL の空いたカードを拾って作る (起動 1 分後・その後 10 分おき・ボードを開いたとき・新商品の登録の直後)。
 *   出品済みページへの色追加のカード (added_to_draft_id) は、足し先のページのフォルダの URL を写す (新しく作らない)。
 *   Notion から取り込んだ既存カード・取り込み前 (CUTOFF より前) のカード・対象外 (excluded) は触らない
 *
 * 設計:
 *   - 対象は**商品コードが確定したカード** (仮コード provisional_code=1 のセットは、本コードが決まってから作る)。
 *     バリエーション代表も対象 (楽天1ページ=1フォルダ)
 *   - drive_folder_url が既に入っているカードには触らない (Notion取込・人の手入力を上書きしない)
 *   - 冪等: 親フォルダ直下に同名フォルダがあれば再利用する (二重作成しない)。
 *     さらに同一 draft への同時呼び出しは in-flight Map で1本にまとめる
 *     (list→create の隙間で二重作成しない。単一プロセス前提 = Render は1プロセス) (Codex R1 high-1)
 *   - fail-soft: この関数は**絶対に reject しない** (SA 鍵の解析失敗も含めて outcome で返す)。
 *     Drive 側の失敗でカード作成は止めない。draft_events に記録して返すだけ (Codex R1 high-2)
 *   - 再試行: 失敗した draft (drive_folder_failed イベントあり・URL 空のまま) は
 *     retryFailedImageFolders() が回収する (intake cron から毎回呼ばれる) (Codex R1 high-3)
 *   - レース安全: URL の書き込みは「まだ空のときだけ」の条件付き UPDATE。
 *     作成中に人が別フォルダを貼ったらそちらを正とする (#657 フォルダ取込と同じ思想)
 *   - SA は rakuten-listing と同じ GOOGLE_SERVICE_ACCOUNT_KEY。ただしフォルダ作成には
 *     書き込みが要るので scope は drive (readonly ではない)。SA に親フォルダの
 *     「コンテンツ管理者」以上の権限が無いと failed になる (共有ドライブ側で付与する)
 *
 * 環境変数:
 *   GOOGLE_SERVICE_ACCOUNT_KEY    … SA 鍵 (base64)。無ければ disabled (既存 env をそのまま使う)
 *   PH_IMAGE_FOLDER_PARENT_ID     … 画像親フォルダの Drive ID。既定は下の DEFAULT_PARENT
 *   PH_AUTO_DRIVE_FOLDER_DISABLED … '1' で機能ごと止める (緊急停止用)
 */
import { google } from 'googleapis';

import { getDB, logEvent } from '../db.js';

// 中原さん指定の画像置き場 (2026-08-27)。共有ドライブ内のフォルダ
const DEFAULT_PARENT = '1wMgG-MvVdun7-y89gvGovv8Y7x8ICAd4';

const ERROR_MAX_LEN = 500;
// Drive API 1呼び出しの上限。カード作成レスポンスを道連れにしない (notion-card と同じ思想)
const DRIVE_TIMEOUT_MS = 10_000;
// 再試行1回あたりの上限 (異常時に Drive を叩きすぎない)
export const MAX_RETRY_PER_RUN = 50;
// 1 draft あたりの総試行上限 (Codex R2 high: 恒久失敗の無制限 retry とイベント無限増加を防ぐ)。
// drive_folder_failed イベント数 = 試行回数として数える (append-only なので信頼できるカウンタ)
export const MAX_RETRY_ATTEMPTS = 10;
// 前回失敗からの最短間隔 (時間)。同一 intake 実行内での即時再試行も自然に防ぐ
export const RETRY_BACKOFF_HOURS = 2;

// Drive の ID として妥当な文字だけ許す。env の誤設定でクエリ文字列を壊さない (Codex R1 medium-4)
const DRIVE_ID_RE = /^[A-Za-z0-9_-]{5,}$/;

export function imageFolderParentId() {
  const env = String(process.env.PH_IMAGE_FOLDER_PARENT_ID || '').trim();
  if (!env) return DEFAULT_PARENT;
  if (!DRIVE_ID_RE.test(env)) {
    console.warn(`[product-hub] PH_IMAGE_FOLDER_PARENT_ID が Drive ID の形式ではないため既定を使います: ${env}`);
    return DEFAULT_PARENT;
  }
  return env;
}

function isDisabled() {
  return ['1', 'true', 'on', 'yes'].includes(String(process.env.PH_AUTO_DRIVE_FOLDER_DISABLED ?? '').trim().toLowerCase());
}

/**
 * フォルダ名「商品コード_商品名」。親フォルダは Google Drive for desktop で
 * Windows (G:) に同期されるため、Windows で使えない文字は全角に寄せる/落とす。
 * 末尾のドット・空白も Windows では不可。長すぎる商品名はパス長制限に当たるので切る。
 */
export function buildImageFolderName(neCode, name) {
  const clean = (s) => String(s == null ? '' : s)
    .replace(/[\u0000-\u001f\u007f]/g, ' ')
    .replace(/[\\/]/g, '／')
    .replace(/:/g, '：')
    .replace(/\*/g, '＊')
    .replace(/\?/g, '？')
    .replace(/"/g, '”')
    .replace(/</g, '＜')
    .replace(/>/g, '＞')
    .replace(/\|/g, '｜')
    .replace(/\s+/g, ' ')
    .trim();
  const code = clean(neCode);
  const productName = clean(name);
  let folder = productName ? `${code}_${productName}` : code;
  if (folder.length > 100) folder = folder.slice(0, 100).trim();
  return folder.replace(/[. ]+$/g, '');
}

/** 画像フォルダを自動で作るカードか (2026-10-27): 商品コードが確定している (仮コードのセットは待つ)。セットも対象 */
export function isFolderEligibleDraft(draft) {
  if (!draft) return false;
  return !Number(draft.provisional_code || 0);
}

/** セット商品でない (白抜きの受信箱が使う。画像フォルダの自動作成の対象かは isFolderEligibleDraft) */
export function isSingleProductDraft(draft) {
  if (!draft) return false;
  return draft.parent_draft_id == null && !Number(draft.provisional_code || 0);
}

export function getDriveWriteClient() {
  const keyBase64 = process.env.GOOGLE_SERVICE_ACCOUNT_KEY;
  if (!keyBase64) return null;
  // ⚠️ 鍵が壊れていると JSON.parse が throw する — 呼び出し側の try の中で呼ぶこと
  const credentials = JSON.parse(Buffer.from(keyBase64, 'base64').toString('utf-8'));
  const auth = new google.auth.GoogleAuth({
    credentials,
    // フォルダ作成のため readonly ではなく drive (rakuten-listing の読み取り client とは別)
    scopes: ['https://www.googleapis.com/auth/drive'],
  });
  return google.drive({ version: 'v3', auth });
}

/**
 * 親フォルダ直下に同名フォルダがあれば返し、無ければ作る (冪等)。
 * @returns {Promise<{id: string, url: string, reused: boolean}>}
 */
export async function ensureImageFolder(drive, { name, parentId }) {
  // Drive クエリの文字列リテラルは \ と ' をエスケープする (parentId は DRIVE_ID_RE 検証済み)
  const escaped = name.replace(/\\/g, '\\\\').replace(/'/g, "\\'");
  const found = await drive.files.list({
    q: `'${parentId}' in parents and trashed = false and mimeType = 'application/vnd.google-apps.folder' and name = '${escaped}'`,
    fields: 'files(id, name)',
    pageSize: 2,
    supportsAllDrives: true,
    includeItemsFromAllDrives: true,
  }, { timeout: DRIVE_TIMEOUT_MS });
  const hit = (found.data.files || [])[0];
  if (hit) return { id: hit.id, url: folderUrl(hit.id), reused: true };

  const created = await drive.files.create({
    requestBody: { name, mimeType: 'application/vnd.google-apps.folder', parents: [parentId] },
    fields: 'id',
    supportsAllDrives: true,
  }, { timeout: DRIVE_TIMEOUT_MS });
  return { id: created.data.id, url: folderUrl(created.data.id), reused: false };
}

function folderUrl(id) {
  return `https://drive.google.com/drive/folders/${id}`;
}

function truncateError(e) {
  const msg = e && e.message ? String(e.message) : String(e);
  return msg.length > ERROR_MAX_LEN ? `${msg.slice(0, ERROR_MAX_LEN)}…` : msg;
}

// 同一 draft への同時呼び出しを1本にまとめる (list→create の隙間の二重作成防止)
const inFlight = new Map();

/**
 * 1 ドラフトぶんの画像フォルダ作成を試みる。**絶対に reject しない** (呼び出し側 fire-safe)。
 * @param {number} draftId
 * @param {{actor?: string|null, driveClient?: object}} [opts] driveClient は smoke 用の注入口
 * @returns {Promise<{outcome: string, url?: string, unusedFolderUrl?: string, folderName?: string, error?: string}>}
 */
export function attemptImageFolderCreation(draftId, opts = {}) {
  const key = Number(draftId);
  const running = inFlight.get(key);
  if (running) return running;
  const p = doAttempt(key, opts)
    .catch((e) => ({ outcome: 'failed', error: truncateError(e) }))
    .finally(() => inFlight.delete(key));
  inFlight.set(key, p);
  return p;
}

async function doAttempt(draftId, { actor = null, driveClient = null, sweep = false } = {}) {
  const db = getDB();
  if (isDisabled()) return { outcome: 'disabled' };

  const draft = db.prepare(
    'SELECT * FROM product_drafts WHERE id = ?'
  ).get(draftId);
  if (!draft) return { outcome: 'not_found' };
  if (!isFolderEligibleDraft(draft)) return { outcome: 'skipped_provisional' };
  // 対象外にしたカードは作らない (見回りが選んだ後に人が対象外にした・Codex)
  if (draft.status === 'excluded') return { outcome: 'skipped_excluded' };
  // 見回りは、選んだ後にカードが変わっていても守るカードを触らない (Notion から取り込んだ・自動作成を始める前)
  if (sweep && (String(draft.source || 'portal') === 'notion_import' || String(draft.created_at || '') < FOLDER_SWEEP_CUTOFF)) return { outcome: 'skipped_protected' };
  if (draft.drive_folder_url && String(draft.drive_folder_url).trim() !== '') return { outcome: 'skipped_has_url' };
  // 出品済みページへの色追加のカード: 足し先のページのフォルダを使う (取込のたびに色ごとのフォルダを増やさない)。
  // 足し先にまだ無ければ待つ (足し先のフォルダができた後の見回りで写す)
  if (draft.added_to_draft_id != null) {
    const target = db.prepare('SELECT drive_folder_url FROM product_drafts WHERE id = ?').get(draft.added_to_draft_id);
    const url = target && target.drive_folder_url && String(target.drive_folder_url).trim();
    if (!url) return { outcome: 'skipped_waiting_target' };
    const r = db.prepare(`UPDATE product_drafts SET drive_folder_url = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
       WHERE id = ? AND (drive_folder_url IS NULL OR TRIM(drive_folder_url) = '')`).run(url, draftId);
    if (r.changes) logEvent(db, draftId, 'drive_folder_created', `足し先のページ (#${draft.added_to_draft_id}) のフォルダを使う`, actor);
    return { outcome: r.changes ? 'linked_target' : 'skipped_has_url', url };
  }

  const folderName = buildImageFolderName(draft.ne_code, draft.name);
  try {
    // 鍵の解析も try の中: 壊れた鍵で throw させず failed にする (Codex R1 high-2)
    const drive = driveClient || getDriveWriteClient();
    if (!drive) return { outcome: 'disabled' };

    const folder = await ensureImageFolder(drive, { name: folderName, parentId: imageFolderParentId() });
    // 空のときだけ書く: 作成中に人が貼った URL を上書きしない
    const updated = db.prepare(`
      UPDATE product_drafts SET drive_folder_url = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
       WHERE id = ? AND (drive_folder_url IS NULL OR TRIM(drive_folder_url) = '') AND status != 'excluded'
    `).run(folder.url, draftId);
    if (updated.changes === 0) {
      // フォルダ自体は Drive にできている (無害)。カードは人の入力を正とする。
      // url には実際に残った (人が貼った) URL を返す (Codex R1 low-6)
      const kept = db.prepare('SELECT drive_folder_url FROM product_drafts WHERE id = ?').get(draftId)?.drive_folder_url || null;
      logEvent(db, draftId, 'drive_folder_kept_manual', `${folderName} を作ったが手入力の URL を優先`, actor);
      return { outcome: 'kept_manual_url', url: kept, unusedFolderUrl: folder.url, folderName };
    }
    logEvent(db, draftId, 'drive_folder_created', `${folderName}${folder.reused ? ' (既存を再利用)' : ''}`, actor);
    return { outcome: folder.reused ? 'reused' : 'created', url: folder.url, folderName };
  } catch (e) {
    const error = truncateError(e);
    try { logEvent(db, draftId, 'drive_folder_failed', error, actor); } catch (_) { /* fail-soft */ }
    return { outcome: 'failed', error, folderName };
  }
}

/**
 * 複数ドラフトへ直列に適用する (一括登録・NE自動取込用)。
 * 逐次実行 = Drive API を叩きすぎない。1件の失敗で残りを止めない (Codex R1 high-2)。
 * 呼び出し側は await せず fire-and-forget でよい (reject しない)。
 * @param {number[]} draftIds
 * @returns {Promise<{created: number, reused: number, skipped: number, failed: number}>}
 */
export async function attemptImageFolderCreationBatch(draftIds, { actor = null, driveClient = null, sweep = false } = {}) {
  const summary = { created: 0, reused: 0, skipped: 0, failed: 0 };
  for (const id of draftIds || []) {
    // 出品済みページへの色追加のカードは doAttempt が足し先のフォルダの URL を写す (新しく作らない・2026-10-27)
    const r = await attemptImageFolderCreation(id, { actor, driveClient, sweep });
    if (r.outcome === 'created') summary.created += 1;
    else if (r.outcome === 'reused') summary.reused += 1;
    else if (r.outcome === 'failed') summary.failed += 1;
    else summary.skipped += 1;
  }
  return summary;
}

/**
 * 失敗の回収 (Codex R1 high-3): 過去に drive_folder_failed になったまま URL が空の単品を
 * 再試行する。intake cron から毎回呼ばれる = Drive の一時障害・権限付与前の失敗が
 * 権限付与後に自然回復する。対象は「一度作ろうとして失敗した draft」だけ
 * (既存カードへの一括バックフィルはしない)。reject しない。
 *
 * retry の制御 (Codex R2 high 対応):
 *   - 総試行 MAX_RETRY_ATTEMPTS 回まで (failed イベント数で数える = イベント増加も同数で頭打ち)。
 *     上限に達した draft は対象外 → 人が詳細画面で URL を貼る (`drive_folder_failed` イベントで追える)
 *   - 前回失敗から RETRY_BACKOFF_HOURS 時間以内は再試行しない (同一実行内の即時 retry も防ぐ)
 *   - 古い失敗から順に処理 (LIMIT で先頭固定にならない = 飢餓防止)
 * @returns {Promise<{retried: number, created: number, reused: number, skipped: number, failed: number}>}
 */
export async function retryFailedImageFolders({ limit = MAX_RETRY_PER_RUN, actor = null, driveClient = null } = {}) {
  try {
    const db = getDB();
    if (isDisabled()) return { retried: 0, created: 0, reused: 0, skipped: 0, failed: 0 };
    const rows = db.prepare(`
      SELECT d.id FROM product_drafts d
        JOIN draft_events e ON e.draft_id = d.id AND e.event = 'drive_folder_failed'
       WHERE (d.drive_folder_url IS NULL OR TRIM(d.drive_folder_url) = '')
         AND COALESCE(d.provisional_code, 0) = 0
       GROUP BY d.id
      HAVING COUNT(e.id) < ?
         AND MAX(e.created_at) <= strftime('%Y-%m-%dT%H:%M:%fZ','now','-${Number(RETRY_BACKOFF_HOURS)} hours')
       ORDER BY MAX(e.created_at) ASC
       LIMIT ?
    `).all(MAX_RETRY_ATTEMPTS, limit);
    const summary = await attemptImageFolderCreationBatch(rows.map((r) => r.id), { actor, driveClient });
    return { retried: rows.length, ...summary };
  } catch (e) {
    console.error('[product-hub] 画像フォルダ再試行の走査に失敗:', e && e.message);
    return { retried: 0, created: 0, reused: 0, skipped: 0, failed: 0 };
  }
}

/**
 * 🆕 2026-10-27 中原さん: URL の空いたカードを見回って画像フォルダを作る (全部の商品にフォルダを)。
 * 新商品の登録 (Company DB) から来るカード・セット・色追加のカードも、作り損ねた分 (失敗・入口の作り漏れ) もここで拾う。
 * 対象: 商品コードが確定・URL が空・対象外 (excluded) でない・Notion から取り込んだカードでない・CUTOFF 以降に作ったカード。
 *   失敗の多い (MAX_RETRY_ATTEMPTS 回) カードと、前の失敗から RETRY_BACKOFF_HOURS 時間たっていないカードは飛ばす (今までの再試行と同じ決まり)。
 * 1 回 limit 件まで・直列 (Drive を叩きすぎない)。重なって呼ばれたら後の呼び出しは何もしない。reject しない
 */
export const FOLDER_SWEEP_CUTOFF = '2026-08-27';   // 画像フォルダの自動作成を始めた日。これより前のカードは人の運用のまま
export const FOLDER_SWEEP_LIMIT = 30;
let sweeping = null;
export function sweepMissingImageFolders({ limit = FOLDER_SWEEP_LIMIT, actor = 'system:image-folder-sweep', driveClient = null } = {}) {
  if (sweeping) return sweeping;
  const run = (async () => {
    try {
      const db = getDB();
      if (isDisabled()) return { found: 0, created: 0, reused: 0, skipped: 0, failed: 0 };
      if (!driveClient && !process.env.GOOGLE_SERVICE_ACCOUNT_KEY) return { found: 0, created: 0, reused: 0, skipped: 0, failed: 0 };
      const rows = db.prepare(`
        SELECT d.id FROM product_drafts d
          LEFT JOIN (SELECT draft_id, COUNT(*) AS n, MAX(created_at) AS last FROM draft_events WHERE event = 'drive_folder_failed' GROUP BY draft_id) f ON f.draft_id = d.id
         WHERE (d.drive_folder_url IS NULL OR TRIM(d.drive_folder_url) = '')
           AND COALESCE(d.provisional_code, 0) = 0
           AND d.status != 'excluded'
           AND COALESCE(d.source, 'portal') != 'notion_import'
           AND d.created_at >= ?
           -- 色追加のカードは、足し先にフォルダがあるときだけ (無いカードで毎回 LIMIT を食って、ほかのカードが回らなくならないように・Codex)
           AND (d.added_to_draft_id IS NULL OR EXISTS (SELECT 1 FROM product_drafts t WHERE t.id = d.added_to_draft_id AND TRIM(COALESCE(t.drive_folder_url, '')) <> ''))
           AND (f.n IS NULL OR (f.n < ? AND f.last <= strftime('%Y-%m-%dT%H:%M:%fZ','now','-${Number(RETRY_BACKOFF_HOURS)} hours')))
         ORDER BY (d.added_to_draft_id IS NOT NULL), d.id
         LIMIT ?
      `).all(FOLDER_SWEEP_CUTOFF, MAX_RETRY_ATTEMPTS, limit);
      const summary = await attemptImageFolderCreationBatch(rows.map((r) => r.id), { actor, driveClient, sweep: true });
      if (rows.length) console.log(`[product-hub] 画像フォルダの見回り: 対象 ${rows.length} 件 created=${summary.created} reused=${summary.reused} skipped=${summary.skipped} failed=${summary.failed}`);
      return { found: rows.length, ...summary };
    } catch (e) {
      console.error('[product-hub] 画像フォルダの見回りに失敗:', e && e.message);
      return { found: 0, created: 0, reused: 0, skipped: 0, failed: 0 };
    }
  })();
  // 🚨 片付けは「覚えた後」に (中で await せずに終わる呼び出し = 鍵が無いなど は、覚える前に終わる。
  //    中の finally で消すと、終わった約束を覚えたままになり、以後の見回りが全部その答えを返していた)
  sweeping = run;
  run.finally(() => { if (sweeping === run) sweeping = null; });
  return run;
}
