/**
 * Google Drive の固定フォルダから固定ファイル名の CSV を取得する共通処理。
 *
 * 「ローカルにDLして手動アップロード」の手間を省くため、Render サーバが Drive API (読み取り専用) で
 * 直接ダウンロードする経路。packing-dispatch のキャリアCSV取込 (2026-07-18) で実運用に入り、
 * purchase-orders のロジザード在庫CSV取込 (2026-07-20) でも使うため lib へ切り出した。
 *
 * 認証は既存の GOOGLE_SERVICE_ACCOUNT_KEY (base64 JSON) を流用 (fba-replenishment/drive-upload.js と同じ)。
 * 対象フォルダをサービスアカウントが閲覧できる必要がある (共有ドライブのメンバー、または
 * フォルダをサービスアカウントのメールアドレスに閲覧者共有)。
 */
import { google } from 'googleapis';

const TIMEOUT = 20000; // Drive応答が遅くても処理全体を待たせない (drive-upload.js と同じ方針)
// 手動アップロード経路の multer 上限と揃える。Drive経由だけ無制限にメモリへ読み込まないための防御。
export const DEFAULT_MAX_CSV_BYTES = 20 * 1024 * 1024;
const INFO_CACHE_TTL_MS = 60 * 1000; // 更新日時表示は 60 秒キャッシュ (表示用ポーリングで Drive API を叩き過ぎない)

export function vErr(message, detail) {
  const e = new Error(message);
  e.code = 'VALIDATION';
  if (detail) e.detail = detail;
  return e;
}

// Drive クライアントはプロセス内で使い回す (毎リクエストの認証クライアント再生成を避ける)。
// GOOGLE_SERVICE_ACCOUNT_KEY は起動時のみ読む運用 (env は再起動で反映) なのでキャッシュ無効化は不要。
let _cached = null;
function getDriveClient() {
  if (_cached) return _cached;
  const keyBase64 = process.env.GOOGLE_SERVICE_ACCOUNT_KEY;
  if (!keyBase64) throw new Error('GOOGLE_SERVICE_ACCOUNT_KEY が未設定です (Render の env を確認してください)');
  const keyJson = JSON.parse(Buffer.from(keyBase64, 'base64').toString('utf-8'));
  const auth = new google.auth.GoogleAuth({
    credentials: keyJson,
    scopes: ['https://www.googleapis.com/auth/drive.readonly'],
  });
  _cached = { drive: google.drive({ version: 'v3', auth }), serviceAccountEmail: keyJson.client_email || null };
  return _cached;
}

// Drive の RFC3339 UTC を「YYYY-MM-DD HH:mm」JST 表記へ (toISOString は UTC のままなので +9h を自前計算)
export function toJstDisplay(iso) {
  if (!iso) return null;
  const t = Date.parse(iso);
  if (Number.isNaN(t)) return null;
  const d = new Date(t + 9 * 3600 * 1000);
  const p = (n) => String(n).padStart(2, '0');
  return `${d.getUTCFullYear()}-${p(d.getUTCMonth() + 1)}-${p(d.getUTCDate())} ${p(d.getUTCHours())}:${p(d.getUTCMinutes())}`;
}

function assertConfig(cfg) {
  if (!cfg || !cfg.folderId || !cfg.filename) {
    throw new Error('Drive取得の設定が不正です (folderId / filename が必要)');
  }
}

/**
 * フォルダ内から対象ファイルを検索して metadata を返す。
 * 同名が複数あれば modifiedTime 最新を採用。見つからなければ VALIDATION エラー。
 */
export async function findDriveFile(cfg) {
  assertConfig(cfg);
  const { drive, serviceAccountEmail } = getDriveClient();
  const esc = (s) => String(s).replace(/\\/g, '\\\\').replace(/'/g, "\\'"); // 検索クエリ用エスケープ
  const label = cfg.label || cfg.filename;

  // フォルダの属する共有ドライブIDを特定して検索範囲を限定する (corpora=allDrives の
  // incompleteSearch による見落としを避ける。drive-upload.js と同じ方針)。
  let driveId = null;
  try {
    const meta = await drive.files.get(
      { fileId: cfg.folderId, fields: 'id, driveId', supportsAllDrives: true },
      { timeout: TIMEOUT }
    );
    driveId = meta.data.driveId || null;
  } catch (e) {
    throw new Error(
      `Drive フォルダにアクセスできません (${label})。フォルダをサービスアカウント` +
      `${serviceAccountEmail ? ` 「${serviceAccountEmail}」` : ''} に閲覧者以上で共有してください。(詳細: ${e.message})`
    );
  }

  const list = await drive.files.list({
    q: `name = '${esc(cfg.filename)}' and '${esc(cfg.folderId)}' in parents and trashed = false`,
    fields: 'files(id, name, modifiedTime, size), incompleteSearch',
    orderBy: 'modifiedTime desc',
    pageSize: 10,
    supportsAllDrives: true,
    includeItemsFromAllDrives: true,
    ...(driveId ? { corpora: 'drive', driveId } : { corpora: 'user' }),
  }, { timeout: TIMEOUT });

  const file = list.data.files && list.data.files[0];
  if (!file) {
    throw vErr(`Drive フォルダに「${cfg.filename}」が見つかりません (${label})。${cfg.notFoundHint || ''}`.trim());
  }
  return {
    label,
    filename: file.name,
    file_id: file.id,
    size: file.size != null ? Number(file.size) : null,
    modified_time: file.modifiedTime || null,           // RFC3339 UTC (機械用)
    modified_time_jst: toJstDisplay(file.modifiedTime), // 表示用 JST
  };
}

/**
 * フォルダ内のファイル一覧 (名前の部分一致・更新日時降順)。
 * picking の「Driveから取り込む」のように、固定ファイル名ではなく
 * 「フォルダに溜まっていくCSVから選ぶ」用途向け (2026-08-12 追加)。
 */
export async function listDriveFiles({ folderId, nameContains, pageSize = 50 }) {
  if (!folderId) throw new Error('listDriveFiles: folderId が必要です');
  const { drive, serviceAccountEmail } = getDriveClient();
  const esc = (s) => String(s).replace(/\\/g, '\\\\').replace(/'/g, "\\'");
  let driveId = null;
  try {
    const meta = await drive.files.get(
      { fileId: folderId, fields: 'id, driveId', supportsAllDrives: true },
      { timeout: TIMEOUT }
    );
    driveId = meta.data.driveId || null;
  } catch (e) {
    throw new Error(
      `Drive フォルダにアクセスできません。フォルダをサービスアカウント` +
      `${serviceAccountEmail ? ` 「${serviceAccountEmail}」` : ''} に閲覧者以上で共有してください。(詳細: ${e.message})`
    );
  }
  const q = [`'${esc(folderId)}' in parents`, 'trashed = false']
    .concat(nameContains ? [`name contains '${esc(nameContains)}'`] : [])
    .join(' and ');
  const files = await listAllPages(drive, {
    q,
    fields: 'nextPageToken, incompleteSearch, files(id, name, modifiedTime, size)',
    orderBy: 'modifiedTime desc',
    pageSize,
    supportsAllDrives: true,
    includeItemsFromAllDrives: true,
    ...(driveId ? { corpora: 'drive', driveId } : { corpora: 'user' }),
  });
  return files.map((f) => ({
    file_id: f.id,
    filename: f.name,
    size: f.size != null ? Number(f.size) : null,
    modified_time: f.modifiedTime || null,
    modified_time_jst: toJstDisplay(f.modifiedTime),
  }));
}

/**
 * files.list を nextPageToken が尽きるまで辿る (途中で黙って欠落させない)。
 *
 * corpora='drive' (共有ドライブ限定検索) は、サービスアカウントが**共有ドライブ自体の
 * メンバー**でないと "shared drive membership" エラーになる (フォルダ単位の閲覧者共有では
 * 不足)。その場合は corpora='allDrives' (共有された個々のアイテムも検索対象) へ自動で
 * 切り替えて再試行する。allDrives は incompleteSearch がありうるため、立ったら中断する
 * (欠けた一覧を成功として返さない — 下記参照)。
 */
async function listAllPages(drive, params, maxPages = 20) {
  const out = [];
  let pageToken;
  let effective = params;
  for (let i = 0; i < maxPages; i++) {
    let list;
    try {
      list = await drive.files.list({ ...effective, pageToken }, { timeout: TIMEOUT });
    } catch (e) {
      const membership = /shared drive membership|teamDriveMembershipRequired/i.test(String(e?.message || ''));
      if (membership && effective.corpora === 'drive' && out.length === 0 && !pageToken) {
        const { driveId: _omit, ...rest } = effective;
        effective = { ...rest, corpora: 'allDrives' };
        i--;   // 同じページを allDrives で取り直す
        continue;
      }
      throw e;
    }
    // 🚨 不完全な一覧を「成功」として返さない。欠けたまま返すと、取込側は
    // 「そのファイルは無かった」と解釈して黙って処理を飛ばす (2026-09-04 の障害と同じ形)。
    // 中断すれば呼び出し側 (ポーラー) がエラーとして記録し、次の周期で取り直せる
    if (list.data.incompleteSearch) {
      throw new Error(
        'Drive の検索結果が不完全です (incompleteSearch=true)。一覧が欠けたまま成功扱いになるのを' +
        '避けるため中断しました。サービスアカウントを共有ドライブのメンバーにすると' +
        ' corpora=drive で検索でき、この状態を避けられます。'
      );
    }
    out.push(...(list.data.files || []));
    pageToken = list.data.nextPageToken;
    if (!pageToken) return out;
  }
  throw new Error(`Drive の一覧が ${maxPages} ページを超えました (対象フォルダが想定外に大きい)`);
}

/**
 * フォルダ直下のサブフォルダ一覧 (出荷_no → 出荷_01〜45 のような構成向け)。
 */
export async function listDriveSubfolders({ folderId }) {
  if (!folderId) throw new Error('listDriveSubfolders: folderId が必要です');
  const { drive } = getDriveClient();
  const esc = (s) => String(s).replace(/\\/g, '\\\\').replace(/'/g, "\\'");
  let driveId = null;
  try {
    const meta = await drive.files.get(
      { fileId: folderId, fields: 'id, driveId', supportsAllDrives: true },
      { timeout: TIMEOUT }
    );
    driveId = meta.data.driveId || null;
  } catch (e) {
    throw new Error(`Drive フォルダにアクセスできません (詳細: ${e.message})`);
  }
  const folders = await listAllPages(drive, {
    q: `'${esc(folderId)}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false`,
    fields: 'nextPageToken, incompleteSearch, files(id, name)',
    pageSize: 200,
    supportsAllDrives: true,
    includeItemsFromAllDrives: true,
    ...(driveId ? { corpora: 'drive', driveId } : { corpora: 'user' }),
  });
  return folders.map((f) => ({ folder_id: f.id, name: f.name }));
}

/**
 * 配列を同時実行数を抑えて並列処理する (Drive API を一気に叩かないための整流)。
 * 1件でも失敗したら全体を throw する = 呼び出し側は「一部だけ欠けた一覧」を受け取らない。
 *
 * ⭐失敗したら**新しいタスクを始めない**が、走り出したものは終わるまで待ってから throw する。
 *   Promise.all は最初の reject で即座に返るので、そのまま抜けると残りの worker が裏で
 *   API を叩き続け、呼び出し側の再実行と重なって同時実行数を超える (Codexレビュー P2)。
 * ⭐停止フラグは throw された値と**分けて**持つ。兼用にすると `reject(undefined)` のように
 *   falsy な値で失敗したときに停止せず、穴あきの配列を成功として返してしまう (Codexレビュー2巡目)。
 */
export async function mapWithConcurrency(items, limit, fn) {
  const out = new Array(items.length);
  const n = Number(limit);
  const width = Math.min(Number.isFinite(n) ? Math.max(1, Math.floor(n)) : 1, items.length);
  let next = 0;
  let stopped = false;
  let firstError;
  const worker = async () => {
    while (!stopped) {
      const i = next++;
      if (i >= items.length) return;
      try {
        out[i] = await fn(items[i]);
      } catch (e) {
        if (!stopped) { stopped = true; firstError = e; }
      }
    }
  };
  await Promise.all(Array.from({ length: width }, worker));
  if (stopped) throw firstError;
  return out;
}

// フォルダ単位クエリの同時実行数。出荷_no は 65 フォルダあり、直列だと 50 秒かかって
// ポーリング間隔 (120秒) を圧迫する。読み取り専用なので 8 並列でもレート制限に余裕がある
export const ACROSS_CONCURRENCY = 8;

/**
 * 複数フォルダ (親+サブフォルダ群) を横断してファイルを一覧する。
 * Drive API は再帰検索ができないため、フォルダごとに1クエリ投げて束ねる。
 * 返り値の parent_name はどのサブフォルダにあったか (出荷_XX) の表示・導出用。
 *
 * 🚨 **フォルダを `'A' in parents or 'B' in parents` で束ねてはいけない** (2026-09-04 障害)。
 * OR で束ねたクエリは Drive の**検索インデックス依存**になり、インデックスに載り損ねた
 * ファイルが実在するのに永久に返らない。しかも `incompleteSearch` は false のまま
 * (=「結果は完全」と言ってくる) なので、欠落が黙って起きる:
 *   - 梱包の納品書CSV 4本が9時間拾われず、144伝票が梱包アプリに出てこなかった
 *   - `name = '納品書_出荷_07.csv'` の全体検索も 0件 (実体はフォルダ直下に存在)
 *   - 束ね方を変えると拾える組み合わせが毎回変わる = 再現性が低く気づけない
 * 単一の `'X' in parents` は子リストの直接列挙なのでインデックスを介さず取りこぼさない
 * (同じ条件で 18/18 件を安定して取得できることを実測で確認)。
 * `name contains` も単一フォルダなら列挙後のフィルタとして正しく効く (実測で確認済み)。
 */
export async function listDriveFilesAcross({ folders, nameContains, pageSize = 100 }, deps = {}) {
  if (!Array.isArray(folders) || folders.length === 0) return [];
  const drive = deps.drive || getDriveClient().drive;
  const esc = (s) => String(s).replace(/\\/g, '\\\\').replace(/'/g, "\\'");
  // 同じフォルダを2回引かない。束ねる実装では OR の重複が自然に潰れていたが、
  // フォルダ単位クエリでは同じファイルが2件返ってしまう (Codexレビュー)
  const uniqFolders = [];
  const seen = new Set();
  for (const f of folders) {
    if (f?.folder_id && !seen.has(f.folder_id)) { seen.add(f.folder_id); uniqFolders.push(f); }
  }
  if (uniqFolders.length === 0) return [];

  let driveId = null;
  try {
    const meta = await drive.files.get(
      { fileId: uniqFolders[0].folder_id, fields: 'id, driveId', supportsAllDrives: true },
      { timeout: TIMEOUT }
    );
    driveId = meta.data.driveId || null;
  } catch { /* driveId が取れなくても corpora: user で続行 */ }

  const perFolder = await mapWithConcurrency(uniqFolders, ACROSS_CONCURRENCY, async (folder) => {
    const q = [`'${esc(folder.folder_id)}' in parents`, 'trashed = false']
      .concat(nameContains ? [`name contains '${esc(nameContains)}'`] : [])
      .join(' and ');
    const files = await listAllPages(drive, {
      q,
      fields: 'nextPageToken, incompleteSearch, files(id, name, modifiedTime, size)',
      orderBy: 'modifiedTime desc',
      pageSize,
      supportsAllDrives: true,
      includeItemsFromAllDrives: true,
      ...(driveId ? { corpora: 'drive', driveId } : { corpora: 'user' }),
    });
    // 単一フォルダのクエリなので親は自明 (Drive に parents を返させる必要がない)
    return files.map((f) => ({
      file_id: f.id,
      filename: f.name,
      parent_id: folder.folder_id,
      parent_name: folder.name ?? null,
      size: f.size != null ? Number(f.size) : null,
      modified_time: f.modifiedTime || null,
      modified_time_jst: toJstDisplay(f.modifiedTime),
    }));
  });

  const out = perFolder.flat();
  out.sort((a, b) => String(b.modified_time || '').localeCompare(String(a.modified_time || '')));
  return out;
}

/**
 * file_id 指定でCSV本体をダウンロードする。
 * confused-deputy 防止のため、必ず「指定フォルダ直下のファイルであること」を検証する
 * (サービスアカウントは他フォルダも閲覧できるため、任意IDのDLを許すと権限の横断になる)。
 */
export async function downloadDriveFileById({ fileId, folderId, folderIds, maxBytes = DEFAULT_MAX_CSV_BYTES }) {
  const allowed = new Set(folderIds || (folderId ? [folderId] : []));
  if (!fileId || allowed.size === 0) throw new Error('downloadDriveFileById: fileId / folderId(s) が必要です');
  const { drive } = getDriveClient();
  const getMeta = async () => {
    const meta = await drive.files.get(
      { fileId, fields: 'id, name, parents, size, modifiedTime, trashed', supportsAllDrives: true },
      { timeout: TIMEOUT }
    );
    return meta.data;
  };
  const inFolder = (f) => !f.trashed && Array.isArray(f.parents) && f.parents.some((p) => allowed.has(p));
  // 検証→DL→再検証 (downloadDriveCsv と同方針)。検証とDLの間にファイルが対象外フォルダへ
  // 移動・更新された場合に古い/権限外の内容を採用しないため、DL後に metadata を取り直して
  // 親と更新時刻の一致を確認する。差し替わっていたら1回だけ取り直す
  for (let attempt = 0; attempt < 2; attempt++) {
    const before = await getMeta();
    if (!inFolder(before)) {
      throw vErr('指定されたファイルは対象フォルダにありません。一覧を更新してやり直してください。');
    }
    if (before.size != null && Number(before.size) > maxBytes) {
      throw vErr(`ファイルが大きすぎます (${Math.round(Number(before.size) / 1024 / 1024)}MB > 上限${Math.round(maxBytes / 1024 / 1024)}MB)。`);
    }
    const res = await drive.files.get(
      { fileId, alt: 'media', supportsAllDrives: true },
      { responseType: 'arraybuffer', timeout: TIMEOUT }
    );
    const buffer = Buffer.from(res.data);
    if (buffer.length > maxBytes) {
      throw vErr(`ファイルが大きすぎます (${Math.round(buffer.length / 1024 / 1024)}MB > 上限${Math.round(maxBytes / 1024 / 1024)}MB)。`);
    }
    const after = await getMeta();
    if (!inFolder(after)) {
      throw vErr('指定されたファイルは対象フォルダにありません。一覧を更新してやり直してください。');
    }
    if (after.modifiedTime === before.modifiedTime) {
      return {
        buffer,
        filename: after.name,
        modified_time: after.modifiedTime || null,
        modified_time_jst: toJstDisplay(after.modifiedTime),
      };
    }
    // DL中に更新された → 取り直し
  }
  throw new Error('Drive ファイルが更新中のようです。少し待ってからもう一度お試しください。');
}

// ── 更新日時表示用 metadata (60 秒キャッシュ) ──
// キーは対象ファイルそのもの (フォルダ+ファイル名) にする。アプリ側の source 名に依存させない。
const _infoCache = new Map(); // `${folderId}/${filename}` → { at: epoch_ms, info }

const cacheKey = (cfg) => `${cfg.folderId}/${cfg.filename}`;

/**
 * @param {object} cfg
 * @param {{force?: boolean}} [opts] force=true でキャッシュを読まずに Drive へ聞き直す。
 *   「いま取り直させたので、更新日時が動いたかを確かめたい」用 (60秒キャッシュだと自分の更新が見えない)。
 *   取得結果はキャッシュへ入れ直すので、続く表示用の呼び出しは新しい値を見る。
 */
export async function getDriveCsvInfo(cfg, { force = false } = {}) {
  assertConfig(cfg);
  const key = cacheKey(cfg);
  const hit = _infoCache.get(key);
  if (!force && hit && Date.now() - hit.at < INFO_CACHE_TTL_MS) return hit.info;
  const info = await findDriveFile(cfg);
  _infoCache.set(key, { at: Date.now(), info });
  return info;
}

/**
 * CSV 本体をダウンロードして Buffer で返す (エンコーディング判定は呼び出し側の parser に任せる)。
 * DL 後に modifiedTime を再確認し、DL 中にファイルが差し替わっていたら 1 回だけ取り直す
 * (表示する更新日時と取り込んだ中身の世代を一致させる)。
 */
export async function downloadDriveCsv(cfg) {
  assertConfig(cfg);
  const maxBytes = cfg.maxBytes || DEFAULT_MAX_CSV_BYTES;
  const { drive } = getDriveClient();
  for (let attempt = 0; attempt < 2; attempt++) {
    const info = await findDriveFile(cfg);
    if (info.size != null && info.size > maxBytes) {
      throw vErr(`ファイルが大きすぎます (${Math.round(info.size / 1024 / 1024)}MB > 上限${Math.round(maxBytes / 1024 / 1024)}MB)。対象ファイルが正しいか確認してください。`);
    }
    const res = await drive.files.get(
      { fileId: info.file_id, alt: 'media', supportsAllDrives: true },
      { responseType: 'arraybuffer', timeout: TIMEOUT }
    );
    const buffer = Buffer.from(res.data);
    if (buffer.length > maxBytes) {
      throw vErr(`ファイルが大きすぎます (${Math.round(buffer.length / 1024 / 1024)}MB > 上限${Math.round(maxBytes / 1024 / 1024)}MB)。対象ファイルが正しいか確認してください。`);
    }
    // DL 中の差し替え検知: 同名検索をやり直し、file_id と modifiedTime の両方が一致した時だけ採用。
    // 「削除→同名再アップロード」は file_id が変わるため、旧IDの modifiedTime 確認では検知できない。
    // 再確認の検索自体が失敗したら throw されエラー扱い (成功扱いにしない、fail-closed)。
    const after = await findDriveFile(cfg);
    if (after.file_id === info.file_id && after.modified_time === info.modified_time) {
      _infoCache.set(cacheKey(cfg), { at: Date.now(), info }); // 表示キャッシュも取込時点に更新
      return { ...info, buffer };
    }
    // 差し替わっていた → ループ先頭から取り直し (次周は新しい方を取る)
  }
  throw new Error('Drive ファイルが更新中のようです。少し待ってからもう一度お試しください。');
}
