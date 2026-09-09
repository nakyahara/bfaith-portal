#!/usr/bin/env node
/**
 * archive-items.mjs — モールの商品一覧 (夜間に取った応答) を日付つきで圧縮保存し、商品の「履歴」を残す
 *
 * なぜ (Company DB構想 06 §7 Step 0。中原さん決定 2026-09-09 D-17「Postgres を待たずに始める」):
 *   Amazon 出品レポートと楽天 items/search は想定利益の夜間処理 (23:30) が毎晩全件取っているのに、
 *   価格と公開状態しか保存せず、タイトル・説明・画像・JAN・属性は捨てていた。商品の「昨日」がどこにも無い。
 *   本命は Company DB の raw 層 (Phase 1) だが、それまで 0 円で履歴を貯め始めるのがこのスクリプト。
 *   Postgres ができたら gz を raw.<src>_contents / _observations へ backfill する。
 *
 * 何をするか (fetch-listings.js の取得直後に呼ばれる。ロジザード在庫の archive-snapshot.mjs と同じ型):
 *   1. 応答 (TSV の原文 / 商品オブジェクトの配列) を
 *      <dest>/<mall>/YYYY/MM/items_<mall>_YYYYMMDD_HHMMSS_<run_id>.<tsv|ndjson>.gz に保存
 *      (時刻 = 取得時刻 JST。work ファイルに固定 → gz.tmp → gunzip して sha256 検証 → rename)
 *   2. <dest>/<mall>/manifest.jsonl に 1 行追記 = 毎回の「観測の記録」
 *      (complete / pages / truncated / 件数 / sha256 / content_hash / same_as_previous)
 *      🚨 変化が無くても毎晩保存する。「変化なしの日」と「取れなかった日」を区別するため (06 §11-1)。
 *         content_hash (行を並べ替えたハッシュ) が前回と同じなら same_as_previous=true を書く
 *   3. 🚨 0 件は保存しない (code=empty)。「取れなかった」を「無くなった」と読み替えない (空の入荷CSV事故 2026-09-08)。
 *      部分取得 (ページ打ち切り・期限) は証拠として保存するが manifest に complete=false を書く。
 *      削除・停止の判定の根拠には使わない (06 §11-2)
 *   4. 同名 gz が既にあり中身も同一なら成功扱い (再実行で冪等)。別内容なら衝突エラー (上書きしない)
 *   5. 世代管理はしない (1 日 1 本 × 数 MB、永久)。Company DB へ backfill したあとに整理する
 *   6. offsite (任意): MALL_ITEMS_RCLONE_REMOTE (または BACKUP_RCLONE_REMOTE の最終要素を mall-items-history に
 *      置き換えた先) が決まるときだけ、履歴フォルダ全体を rclone copy (既存同一は飛ばす。削除はしない)。
 *      失敗しても保存自体は成功なので offsite='failed' で知らせるだけ
 *
 * 呼び出し側の約束 (fetch-listings.js):
 *   - 保存の失敗で取得結果 (listing_enum_status) を変えない。結果は戻り値 archive に載せ、ping の note に写す
 *   - 取得が 0 件 / 形式不正のときは呼ばない (呼んでも empty で skip する)
 *
 * 使い方 (手動での保存。auPAY CSV など):
 *   node scripts/mall-items/archive-items.mjs --mall aupay --file item.csv --format tsv --source wowma_item_csv --shop-id 54318092
 *   オプション: --run-id <id> / --dest <dir> / --dry-run / --no-offsite / --incomplete (complete=false で記録)
 *
 * 終了コード: 0 = 保存した / 同名同内容が既にあった、3 = 0 件で保存しなかった、4 = 保存したが offsite 失敗、
 *             1 = 保存失敗 (gzip・検証・衝突)、2 = 引数・ファイル不正
 * サイズ感: Amazon TSV 4,300 行 ≈ 2MB → gz 0.3MB / 楽天 6,500 SKU の JSON ≈ 20MB → gz 2MB。1 日 3MB 弱、年 1GB 弱
 */
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import {
  jstStamp, readLastManifest, sha256Gunzip, gzipVerified, manifestHas, appendManifest,
} from '../logizard-stock/archive-snapshot.mjs';

export const DEFAULTS = {
  dest: path.join(process.env.DATA_DIR || path.join(process.cwd(), 'data'), 'mall-items-history'),
};

/** 保存を許すモール (ディレクトリ名になる。知らない名前で勝手にフォルダを増やさない) */
export const MALLS = ['amazon', 'rakuten', 'yahoo', 'aupay', 'qoo10', 'linegift'];
export const FORMATS = ['tsv', 'ndjson'];

const FILE_RE = /^items_([a-z0-9]+)_(\d{8})_(\d{6})_([A-Za-z0-9_-]+)\.(tsv|ndjson)\.gz$/;

function badArgs(msg) {
  return Object.assign(new Error(msg), { code: 'BAD_ARGS' });
}

function sha256Hex(buf) {
  return crypto.createHash('sha256').update(buf).digest('hex');
}

/** 文字列比較 (コードユニット順。ロケールに依存しない) */
function byCodeUnit(a, b) {
  return a < b ? -1 : (a > b ? 1 : 0);
}

/**
 * 応答を保存用テキストに整える。
 *   tsv    : 文字列をそのまま (原文保持。末尾に改行が無ければ足す)
 *   ndjson : 配列なら 1 要素 1 行に JSON 化。sortKey があればその順、無ければ行の文字列順に並べる
 *            (順序に情報は無いので、同じ内容なら同じバイト列になるようにする = 冪等・再実行の検証が楽)
 * 戻り値 { text, rows }。rows = データ行数 (tsv はヘッダを除く)
 */
export function buildText(format, payload, { sortKey } = {}) {
  if (format === 'tsv') {
    if (typeof payload !== 'string') throw badArgs('tsv の payload は文字列');
    const text = payload.endsWith('\n') ? payload : payload + '\n';
    const lines = text.split(/\r?\n/).filter((l) => l.length > 0);
    return { text, rows: Math.max(0, lines.length - 1) };
  }
  if (format === 'ndjson') {
    let lines;
    if (Array.isArray(payload)) {
      const keyed = payload.map((o) => ({ k: sortKey ? String(sortKey(o) ?? '') : '', l: JSON.stringify(o) }));
      keyed.sort((a, b) => byCodeUnit(a.k, b.k) || byCodeUnit(a.l, b.l));
      lines = keyed.map((x) => x.l);
    } else if (typeof payload === 'string') {
      lines = payload.split(/\r?\n/).filter((l) => l.length > 0);
    } else {
      throw badArgs('ndjson の payload は配列か文字列');
    }
    return { text: lines.length ? lines.join('\n') + '\n' : '', rows: lines.length };
  }
  throw badArgs(`format は ${FORMATS.join(' / ')} のどれか: ${format}`);
}

/**
 * 内容ハッシュ = 行を並べ替えて取る sha256 (取得順が違うだけの応答を「同じ内容」と見なす)。
 * tsv はヘッダ行を先頭に固定し、データ行だけ並べ替える。改行コードの違い (CRLF/LF) も吸収する
 */
export function contentHashOf(format, text) {
  const lines = text.split(/\r?\n/).filter((l) => l.length > 0);
  let ordered;
  if (format === 'tsv') {
    const [head, ...body] = lines;
    body.sort(byCodeUnit);
    ordered = head === undefined ? [] : [head, ...body];
  } else {
    ordered = [...lines].sort(byCodeUnit);
  }
  return sha256Hex(ordered.join('\n') + '\n');
}

/** dest/<mall> 配下の items_*.gz を列挙 (日付順) */
export function listItemSnapshots(dest, mall) {
  const out = [];
  const root = path.join(dest, mall);
  if (!fs.existsSync(root)) return out;
  for (const y of fs.readdirSync(root)) {
    const yd = path.join(root, y);
    if (!/^\d{4}$/.test(y) || !fs.statSync(yd).isDirectory()) continue;
    for (const m of fs.readdirSync(yd)) {
      const md = path.join(yd, m);
      if (!/^\d{2}$/.test(m) || !fs.statSync(md).isDirectory()) continue;
      for (const f of fs.readdirSync(md)) {
        const mm = FILE_RE.exec(f);
        if (!mm || mm[1] !== mall) continue;
        out.push({ file: path.join(md, f), day: mm[2], time: mm[3], runId: mm[4], format: mm[5] });
      }
    }
  }
  return out.sort((a, b) => byCodeUnit(a.day + a.time + a.runId, b.day + b.time + b.runId));
}

/**
 * offsite 先: 明示 env > BACKUP_RCLONE_REMOTE の最終パス要素を mall-items-history に置換 > なし
 *   gdrive:bfaith-backup/warehouse → gdrive:bfaith-backup/mall-items-history
 *   gdrive:bfaith-backup (サブディレクトリ無し) → null (勝手にバケット直下へ置かない)
 */
export function resolveOffsiteRemote(env = process.env) {
  const explicit = (env.MALL_ITEMS_RCLONE_REMOTE || '').trim();
  if (explicit) return explicit;
  const base = (env.BACKUP_RCLONE_REMOTE || '').trim().replace(/\/+$/, '');
  const m = /^([^:]+:)(.*)\/[^/]+$/.exec(base);
  if (!m) return null;
  return `${m[1]}${m[2]}/mall-items-history`;
}

/** 履歴フォルダ全体 (全モール) を rclone copy。戻り値 'ok' | 'failed' */
function offsiteCopy(dest, remote, { rcloneConfig, log }) {
  const args = [];
  if (rcloneConfig) args.push('--config', rcloneConfig);
  args.push('copy', dest, remote, '--include', '/*/*/*/items_*.gz', '--include', '/*/manifest.jsonl',
    '--transfers', '4', '--timeout', '60s', '--retries', '1', '--max-duration', '3m', '--cutoff-mode', 'hard');
  try {
    execFileSync('rclone', args, { stdio: ['ignore', 'pipe', 'pipe'], timeout: 200000, windowsHide: true });
    log(`offsite ok: ${remote}`);
    return 'ok';
  } catch (e) {
    const tail = (e.stderr ? e.stderr.toString() : e.message).trim().split('\n').slice(-2).join(' | ');
    log(`offsite FAILED (保存は完了、次回に追いつく): ${tail}`);
    return 'failed';
  }
}

/** 直前の manifest 行 (同じ店舗) の content_hash。無ければ null */
function previousContentHash(mallDir, shopId) {
  const mf = path.join(mallDir, 'manifest.jsonl');
  if (!fs.existsSync(mf)) return null;
  const lines = fs.readFileSync(mf, 'utf-8').split(/\r?\n/).filter(Boolean);
  for (let i = lines.length - 1; i >= 0; i--) {
    try {
      const rec = JSON.parse(lines[i]);
      if (rec.shop_id === shopId && rec.content_hash) return rec.content_hash;
    } catch { /* 壊れた行は飛ばす */ }
  }
  return null;
}

/**
 * 本体。
 * @param {object} opts
 *   mall       'amazon' | 'rakuten' | ...        (必須)
 *   shopId     店舗・アカウントの識別 (必須。Amazon = sellerId@marketplace、楽天 = 店舗コード)
 *   source     取得元の名前 (必須。'merchant_listings_all_data' / 'rms_items_search' ...)
 *   runId      取得の run_id (必須。ファイル名と manifest に載る。price_fetch_run と突き合わせる鍵)
 *   fetchedAt  取得時刻 ISO (必須。JST に直してファイル名にする)
 *   format     'tsv' | 'ndjson' (必須)
 *   payload    tsv = 文字列 / ndjson = オブジェクト配列 or 文字列 (必須)
 *   sortKey    ndjson の並べ替えキー関数 (任意)
 *   meta       { complete, enum_status, pages, truncated, deadline_hit, api_version, note } (任意。manifest に写す)
 *   dest / env / log / now / dryRun / noOffsite (任意)
 * 戻り値:
 *   { action: 'archived'|'skipped', code: 'archived'|'exists_same'|'empty', file?, relFile?, items, rows?, bytes?,
 *     gzBytes?, sha256?, contentHash?, sameAsPrevious?, complete, offsite: 'ok'|'failed'|'skipped'|null }
 * 例外: BAD_ARGS / COLLISION (同名 gz が別内容) / gzip・検証失敗
 */
export async function archiveItems(opts = {}) {
  const { mall, shopId, source, runId, fetchedAt, format, payload } = opts;
  if (!MALLS.includes(mall)) throw badArgs(`mall は ${MALLS.join(' / ')} のどれか: ${mall}`);
  if (!FORMATS.includes(format)) throw badArgs(`format は ${FORMATS.join(' / ')} のどれか: ${format}`);
  if (!shopId || typeof shopId !== 'string') throw badArgs('shopId が無い');
  if (!source || typeof source !== 'string') throw badArgs('source が無い');
  if (!runId || typeof runId !== 'string') throw badArgs('runId が無い');
  const fetchedMs = new Date(fetchedAt || NaN).getTime();
  if (!Number.isFinite(fetchedMs)) throw badArgs(`fetchedAt が日時として読めない: ${fetchedAt}`);
  const dest = opts.dest || DEFAULTS.dest;
  const now = opts.now || new Date();
  const env = opts.env || process.env;
  const dryRun = !!opts.dryRun;
  const meta = opts.meta || {};
  const complete = meta.complete !== false;   // 明示的に false のときだけ部分取得
  const log = opts.log || ((m) => console.log(`[mall-items] ${m}`));

  const built = buildText(format, payload, { sortKey: opts.sortKey });
  const items = Number.isInteger(opts.items) ? opts.items : built.rows;
  if (items === 0 || built.rows === 0 || built.text.length === 0) {
    // 🚨 0 件は「取れなかった」の可能性が高い。保存すると「全部消えた」を履歴に残してしまう
    return { action: 'skipped', code: 'empty', reason: '0 件のため保存しない (取れなかったを無くなったと読み替えない)', items, complete, offsite: null };
  }

  const buf = Buffer.from(built.text, 'utf-8');
  const sha = sha256Hex(buf);
  const contentHash = contentHashOf(format, built.text);
  const stamp = jstStamp(new Date(fetchedMs));
  const hms = stamp.iso.slice(11, 19).replace(/:/g, '');
  const runIdSafe = String(runId).replace(/[^A-Za-z0-9_-]/g, '');
  if (!runIdSafe) throw badArgs('runId に使える文字が無い');
  const mallDir = path.join(dest, mall);
  const dir = path.join(mallDir, stamp.year, stamp.month);
  const name = `items_${mall}_${stamp.ymd}_${hms}_${runIdSafe}.${format}.gz`;
  const outFile = path.join(dir, name);
  const relFile = `${mall}/${stamp.year}/${stamp.month}/${name}`;

  if (dryRun) {
    log(`dry-run: ${relFile} (items=${items}, rows=${built.rows}, bytes=${buf.length}, complete=${complete})`);
    return { action: 'archived', code: 'archived', dryRun: true, file: outFile, relFile, items, rows: built.rows, bytes: buf.length, gzBytes: 0, sha256: sha, contentHash, complete, offsite: null };
  }

  fs.mkdirSync(dir, { recursive: true });
  const work = path.join(mallDir, `.work-${process.pid}-${runIdSafe}.txt`);
  let result;
  try {
    fs.writeFileSync(work, buf);
    let gzBytes; let code = 'archived';
    if (fs.existsSync(outFile)) {
      // 同名が既にある = クラッシュ後の再実行。中身が同じなら成功扱い、違えば衝突 (上書きしない)
      const v = await sha256Gunzip(outFile);
      if (v.sha !== sha) throw Object.assign(new Error(`同名の履歴 ${relFile} が別内容で存在 (既存 ${v.bytes} bytes)。手で確認して退避してから再実行`), { code: 'COLLISION' });
      gzBytes = fs.statSync(outFile).size; code = 'exists_same';
      log(`already archived (同名同内容): ${relFile}`);
    } else {
      try {
        gzBytes = await gzipVerified(work, outFile, sha);
        log(`archived: ${relFile} (items=${items}, ${(buf.length / 1048576).toFixed(2)}MB → ${(gzBytes / 1024).toFixed(0)}KB, complete=${complete})`);
      } catch (e) {
        // 存在確認と rename の間に別プロセスが同名を作ったとき (Windows の rename は失敗する)
        if (!fs.existsSync(outFile)) throw e;
        const v = await sha256Gunzip(outFile);
        if (v.sha !== sha) throw Object.assign(new Error(`同名の履歴 ${relFile} が別内容で存在 (既存 ${v.bytes} bytes)。手で確認して退避してから再実行`), { code: 'COLLISION' });
        gzBytes = fs.statSync(outFile).size; code = 'exists_same';
        log(`already archived by another run (同名同内容): ${relFile}`);
      }
    }

    const sameAsPrevious = previousContentHash(mallDir, shopId) === contentHash;
    if (!manifestHas(mallDir, relFile)) {
      appendManifest(mallDir, {
        archived_at: now.toISOString(),
        snapshot_at: stamp.iso,
        mall, shop_id: shopId, source, run_id: runId,
        file: relFile, format, encoding: 'utf-8',
        items, rows: built.rows, bytes: buf.length, gz_bytes: gzBytes,
        sha256: sha, content_hash: contentHash, same_as_previous: sameAsPrevious,
        complete,
        enum_status: meta.enum_status ?? null,
        pages: meta.pages ?? null,
        truncated: meta.truncated ?? null,
        deadline_hit: meta.deadline_hit ?? null,
        api_version: meta.api_version ?? null,
        note: meta.note ?? null,
      });
    }

    let offsite = 'skipped';
    if (!opts.noOffsite) {
      const remote = resolveOffsiteRemote(env);
      if (remote) offsite = offsiteCopy(dest, remote, { rcloneConfig: (env.BACKUP_RCLONE_CONFIG || '').trim(), log });
      else log('offsite: remote 未設定のためローカル保存のみ (MALL_ITEMS_RCLONE_REMOTE か BACKUP_RCLONE_REMOTE)');
    }
    result = { action: 'archived', code, file: outFile, relFile, items, rows: built.rows, bytes: buf.length, gzBytes, sha256: sha, contentHash, sameAsPrevious, complete, offsite };
  } finally {
    try { fs.unlinkSync(work); } catch { /* 無ければよい */ }
  }
  return result;
}

// ─── CLI (手動保存) ───
const isMain = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) {
  const args = process.argv.slice(2);
  const getArg = (f) => { const i = args.indexOf(f); return i >= 0 && i < args.length - 1 ? args[i + 1] : null; };
  const file = getArg('--file');
  if (!file || !fs.existsSync(file)) { console.error('--file <path> が必要 (存在するファイル)'); process.exit(2); }
  const format = getArg('--format') || 'tsv';
  const payload = fs.readFileSync(file, 'utf-8');
  const opts = {
    mall: getArg('--mall'),
    shopId: getArg('--shop-id'),
    source: getArg('--source'),
    runId: getArg('--run-id') || `manual_${Date.now().toString(36)}_${crypto.randomBytes(3).toString('hex')}`,
    fetchedAt: getArg('--fetched-at') || fs.statSync(file).mtime.toISOString(),
    format, payload,
    dest: getArg('--dest') || undefined,
    dryRun: args.includes('--dry-run'),
    noOffsite: args.includes('--no-offsite'),
    meta: { complete: !args.includes('--incomplete'), note: `manual archive of ${path.basename(file)}` },
  };
  archiveItems(opts).then((r) => {
    if (r.action === 'skipped') console.log(`[mall-items] skipped: ${r.reason}`);
    console.log(`RESULT action=${r.action} code=${r.code} offsite=${r.offsite ?? 'none'}${r.relFile ? ` file=${r.relFile}` : ''}`);
    if (r.code === 'empty') process.exit(3);
    if (r.offsite === 'failed') process.exit(4);
    process.exit(0);
  }).catch((e) => {
    console.error(`[mall-items] FAILED: ${e.message}`);
    console.log(`RESULT action=error code=${e.code || 'error'}`);
    process.exit(e.code === 'BAD_ARGS' ? 2 : 1);
  });
}
