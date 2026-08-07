/**
 * FBA納品 追跡番号の自動投入 — 実行本体 (毎日22:00 JST に走らせる想定)
 *
 * 流れ:
 *   1. 福山通運の出荷実績CSVを取る (Drive の固定フォルダ / またはローカルファイル)
 *   2. 解析して「納品ごとの boxId ← 送り状番号」を作る
 *   3. fail-closed の判定を通ったものだけ SP-API で投入する
 *   4. 何をしたかを自前記録へ残し、結果を GChat へ通知する
 *
 * ⭐22:00 に走らせる理由は2つ:
 *   - 追跡番号を入れると納品の修正が極端に面倒になるため、日中に箱数のズレが
 *     表面化する時間を作る (子会社いろはの数え間違いがまれに発生する)
 *   - APIの投入は **当日中でないと期限切れで拒否される** (2026-08-07 実測)。
 *     翌朝に回すと通らない。逆に Seller Central 画面は期限後でも入るので、
 *     取りこぼした日は人が画面で入れればリカバリできる
 */
import crypto from 'node:crypto';
import fs from 'node:fs';
import { parseTrackingCsv, buildAssignments, checkShipDate, tErr } from './tracking-csv.js';
import { findOpenShipments, putTrackingDetails, checkDeadline, missingEnv } from './tracking-service.js';
import * as store from './tracking-store.js';
import { sendGChatMessage } from '../profit-analysis/gchat-client.js';

const DEFAULT_FILENAME = 'fukutsu_tuiseki.csv';

/** JSTの YYYYMMDD (toISOString はUTCなので +9h を自前で足す) */
export function jstYmd(date = new Date()) {
  const d = new Date(date.getTime() + 9 * 3600 * 1000);
  const p = (n) => String(n).padStart(2, '0');
  return `${d.getUTCFullYear()}${p(d.getUTCMonth() + 1)}${p(d.getUTCDate())}`;
}

const sha256 = (buf) => crypto.createHash('sha256').update(buf).digest('hex').slice(0, 16);

/**
 * CSVを取得する。ローカル指定が最優先 (手元での通しテスト用)。
 * Drive経路は lib/drive-csv.js (サービスアカウント・読み取り専用) をそのまま使う。
 */
async function fetchCsv({ file, folderId, filename }) {
  if (file) {
    if (!fs.existsSync(file)) throw tErr(`CSVが見つかりません: ${file}`);
    return { buf: fs.readFileSync(file), source: `local:${file}`, modifiedAt: fs.statSync(file).mtime.toISOString() };
  }
  if (!folderId) {
    throw tErr('CSVの取得先が未設定です (--file を指定するか FBA_TRACKING_DRIVE_FOLDER_ID を設定してください)');
  }
  const { downloadDriveCsv } = await import('../../lib/drive-csv.js');
  const cfg = { folderId, filename: filename || DEFAULT_FILENAME, label: '福山通運 出荷実績CSV' };
  const buf = await downloadDriveCsv(cfg);
  return { buf: Buffer.isBuffer(buf) ? buf : buf?.buffer ?? buf, source: `drive:${cfg.filename}`, modifiedAt: buf?.modified_time ?? null };
}

/**
 * @param {object} opts
 * @param {string}  [opts.file]        ローカルCSVのパス (指定するとDriveを使わない)
 * @param {string}  [opts.folderId]    Drive フォルダID
 * @param {string}  [opts.filename]    Drive 上のファイル名 (既定 fukutsu_tuiseki.csv)
 * @param {boolean} [opts.commit]      true で実際に投入する (既定 false = プレビューのみ)
 * @param {string}  [opts.expectYmd]   期待する出荷日 (既定 = 実行時のJST当日)
 * @param {Date}    [opts.now]
 * @returns {Promise<object>} サマリ
 */
export async function runTrackingJob(opts = {}) {
  const now = opts.now ?? new Date();
  const commit = Boolean(opts.commit);
  const expectYmd = opts.expectYmd ?? jstYmd(now);
  const runId = `${jstYmd(now)}-${now.getTime().toString(36)}`;
  const summary = {
    runId, commit, expectYmd, source: null, sourceHash: null,
    blocked: [], excluded: [], skipped: [], registered: [], failed: [], note: [],
  };

  const miss = missingEnv();
  if (miss.length) { summary.blocked.push(`SP-APIの環境変数が不足: ${miss.join(', ')}`); return summary; }

  // 1. CSV取得
  let csv;
  try {
    csv = await fetchCsv({ file: opts.file, folderId: opts.folderId ?? process.env.FBA_TRACKING_DRIVE_FOLDER_ID, filename: opts.filename });
  } catch (e) {
    summary.blocked.push(`CSVを取得できません: ${e.message}`);
    return summary;
  }
  summary.source = csv.source;
  summary.sourceHash = sha256(csv.buf);

  // 2. 解析
  let rows;
  try {
    const parsed = parseTrackingCsv(csv.buf);
    rows = parsed.rows;
    if (parsed.problems.length) summary.blocked.push(...parsed.problems);
  } catch (e) {
    summary.blocked.push(e.message);
    return summary;
  }
  if (!rows.length) { summary.blocked.push('CSVにデータ行がありません'); return summary; }

  // 3. 🚨古いCSVの誤処理を防ぐ (固定ファイル名運用のため中身の出荷日で見る)
  const d = checkShipDate(rows, expectYmd);
  if (!d.ok) { summary.blocked.push(d.problem); return summary; }

  // 4. 同じCSVを二度処理しない (出荷日チェックと二重の防御)
  const dup = store.findBySourceHash(summary.sourceHash);
  if (dup) summary.note.push(`このCSVは ${dup.at} に処理済みです (同一内容の納品はスキップされます)`);

  if (summary.blocked.length) return summary;

  // 5. 対象の納品を探す
  let shipments;
  try {
    shipments = await findOpenShipments();
  } catch (e) {
    summary.blocked.push(`納品の取得に失敗: ${e.message}`);
    return summary;
  }
  if (!shipments.length) {
    summary.blocked.push('追跡番号の未登録な納品が見つかりません (すでに入力済み、またはラベル未発行の可能性)');
    return summary;
  }

  // 6. 割り当て
  const { assignments, excluded, problems, skipped } = buildAssignments(rows, shipments);
  summary.excluded = excluded;
  summary.skipped = skipped;
  if (problems.length) { summary.blocked.push(...problems); return summary; } // 1つでも怪しければ全部止める

  // 7. 納品ごとに投入
  for (const a of assignments) {
    const ship = shipments.find((s) => s.shipmentConfirmationId === a.shipmentConfirmationId);
    const items = a.items.map((i) => ({ boxId: i.boxId, trackingId: i.trackingId }));

    // 自前記録が第一 (APIの trackingDetails は反映が数時間遅れるため信用しない)
    if (store.isSameAsRecorded(a.shipmentConfirmationId, items)) {
      summary.skipped.push({ shipmentConfirmationId: a.shipmentConfirmationId, reason: '投入済み (同一内容)' });
      continue;
    }
    const already = store.findSuccess(a.shipmentConfirmationId);
    if (already) {
      summary.failed.push({
        shipmentConfirmationId: a.shipmentConfirmationId,
        error: `過去に別の内容で投入済みです (${already.at})。取り違えの恐れがあるため自動では上書きしません`,
      });
      continue;
    }

    const dl = checkDeadline(ship, now);
    if (!dl.ok) {
      summary.failed.push({ shipmentConfirmationId: a.shipmentConfirmationId, error: dl.note, expired: dl.expired, needsManual: true });
      continue;
    }

    if (!commit) {
      summary.registered.push({ ...a, dryRun: true });
      continue;
    }

    const res = await putTrackingDetails({ inboundPlanId: ship.inboundPlanId, shipmentId: ship.shipmentId }, items);
    const rec = {
      runId, shipmentConfirmationId: a.shipmentConfirmationId, shipmentId: ship.shipmentId,
      inboundPlanId: ship.inboundPlanId, fcCode: a.fcCode, matchedBy: a.matchedBy,
      items: a.items, sourceFile: summary.source, sourceHash: summary.sourceHash,
      result: res.ok ? 'success' : 'failed', operationId: res.operationId ?? null, error: res.error ?? null,
    };
    store.append(rec);
    if (res.ok) summary.registered.push(a);
    else summary.failed.push({ shipmentConfirmationId: a.shipmentConfirmationId, error: res.error, retryable: res.retryable });
  }

  return summary;
}

/** GChat 用の本文。読み手が「何をすればいいか」だけ分かるように書く。 */
export function formatSummary(s) {
  const L = [];
  const head = s.blocked.length ? '🚨' : s.failed.length ? '⚠️' : s.registered.length ? '✅' : 'ℹ️';
  L.push(`${head} *FBA納品 追跡番号${s.commit ? '' : '(プレビュー)'}*  ${s.expectYmd}`);

  if (s.blocked.length) {
    L.push('', '*中断しました。登録は一切していません*');
    s.blocked.forEach((b) => L.push(`・${b}`));
    L.push('', '→ 内容を直してから再実行してください。当日中に入らなければ、翌日 Seller Central 画面から手入力できます');
  }
  if (s.registered.length) {
    L.push('', `*登録${s.commit ? '' : '予定'} ${s.registered.length}件*`);
    s.registered.forEach((a) => L.push(`・${a.shipmentConfirmationId} (${a.fcCode}) ${a.countBoxes}箱  照合=${a.matchedBy}`));
  }
  if (s.failed.length) {
    L.push('', `*失敗 ${s.failed.length}件*`);
    s.failed.forEach((f) => L.push(`・${f.shipmentConfirmationId}: ${f.error}${f.needsManual ? ' → 画面から手入力してください' : ''}`));
  }
  if (s.skipped.length) L.push('', `スキップ ${s.skipped.length}件 (${s.skipped.map((x) => x.reason).join('/')})`);
  if (s.excluded.length) {
    // 黙って捨てない。FBA以外の便が混ざるのは正常だが、件数は必ず見せる
    const n = s.excluded.reduce((a, e) => a + (e.件数 ?? 1), 0);
    L.push('', `除外 ${n}件 (FBA以外の便など): ${s.excluded.map((e) => e.fcCode ?? e.納品番号).join(', ')}`);
  }
  s.note.forEach((n) => L.push('', `※ ${n}`));
  return L.join('\n');
}

/** 通知先は要対応スペース。未設定でもジョブ自体は失敗させない。 */
export async function notifySummary(s) {
  const webhook = process.env.GCHAT_WEBHOOK_JOBS || process.env.GCHAT_WEBHOOK;
  const text = formatSummary(s);
  if (!webhook) { console.warn('[fba-tracking] GChat webhook 未設定のため通知しません'); return { sent: false, text }; }
  try {
    await sendGChatMessage(webhook, text);
    return { sent: true, text };
  } catch (e) {
    console.error('[fba-tracking] GChat通知に失敗:', e.message);
    return { sent: false, text, error: e.message };
  }
}
