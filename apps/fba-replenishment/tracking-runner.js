/**
 * FBA納品 追跡番号の自動投入 — 実行本体 (毎日22:00 JST に走らせる想定)
 *
 * 流れ:
 *   1. **先に Amazon 側を見て「今日投入すべき納品があるか」を確かめる**
 *   2. あれば福山通運の出荷実績CSVを取り、解析して boxId ← 送り状番号 を作る
 *   3. fail-closed の判定を通ったものだけ SP-API で投入する
 *   4. 何をしたかを自前記録へ残し、結果を GChat へ通知する
 *
 * ⭐なぜ「納品の確認」が先なのか (2026-08-08 に実運用で判明):
 *   CSVを先に見ると、**FBA納品が無い日に前日のCSVが置きっぱなし**なだけで
 *   「古いCSVがある」と赤で鳴ってしまう。納品が無い日は正常なので、
 *   同じ「CSVが古い/無い」でも
 *     - 投入待ちの納品がある → 🚨 人が今日の分を出力すべき
 *     - 投入待ちの納品が無い → ℹ️ 何もすることが無い日
 *   と結論が真逆になる。判定材料の順番がそのまま誤警報の有無を決める。
 *
 * ⭐22:00 に走らせる理由:
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
    return { buf: fs.readFileSync(file), source: `local:${file}` };
  }
  if (!folderId) {
    throw tErr('CSVの取得先が未設定です (--file を指定するか FBA_TRACKING_DRIVE_FOLDER_ID を設定してください)');
  }
  const { downloadDriveCsv } = await import('../../lib/drive-csv.js');
  const cfg = { folderId, filename: filename || DEFAULT_FILENAME, label: '福山通運 出荷実績CSV' };
  const buf = await downloadDriveCsv(cfg);
  return { buf: Buffer.isBuffer(buf) ? buf : buf?.buffer ?? buf, source: `drive:${cfg.filename}` };
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
    // ⭐severity は「人を呼ぶべきか」の段階。納品が無い日は普通にあるので、
    //   それを blocked にすると毎晩ニセの警報が鳴って本当の異常が埋もれる。
    //   ok      = 登録した
    //   info    = 今日はすることが無い (投入待ちの納品が無い)
    //   warn    = 気に留めてほしいが自動対応は不要 (期限切れ・過去に別内容で投入済み等)
    //   blocked = 人が直すまで登録できない (箱数不一致・CSVが古い/無い・権限エラー)
    severity: 'ok',
    openShipments: 0,
    blocked: [], excluded: [], skipped: [], registered: [], failed: [], note: [],
  };

  const miss = missingEnv();
  if (miss.length) {
    summary.severity = 'blocked';
    summary.blocked.push(`SP-APIの環境変数が不足: ${miss.join(', ')}`);
    return summary;
  }

  // 🚨多重起動を止める。定期実行と手動実行が重なると、記録を確認してからPUTするまでの
  //   隙間で同じ納品へ二重投入しうる
  const lock = store.acquireLock(runId);
  if (!lock.ok) {
    summary.severity = 'blocked';
    summary.blocked.push(lock.reason);
    return summary;
  }
  try {
    return await runInner(summary, { opts, now, commit, expectYmd, runId });
  } finally {
    store.releaseLock();
  }
}

async function runInner(summary, ctx) {
  const { opts, now, commit, expectYmd, runId } = ctx;

  // ── 1. まず Amazon 側を見る ────────────────────────────────
  // ここを先にやることで「納品が無い日の置きっぱなしCSV」を異常扱いしないで済む。
  let shipments;
  try {
    const r = await findOpenShipments();
    shipments = r.shipments;
    // 🚨取得に失敗したプランがあると「今日は納品が無い」に化ける。書き込みジョブなので
    //   一部でも見えていない状態では進めない (全体を止めて人に知らせる)
    if (r.errors.length) {
      summary.severity = 'blocked';
      summary.blocked.push(
        'Amazonの納品情報を一部取得できませんでした。見えていない納品がある可能性があるため中断します',
        ...r.errors,
      );
      return summary;
    }
  } catch (e) {
    summary.severity = 'blocked';
    summary.blocked.push(`納品の取得に失敗: ${e.message}`);
    return summary;
  }
  summary.openShipments = shipments.length;

  if (!shipments.length) {
    // 追跡番号の投入を待っている納品が1つも無い = 今日はすることが無い。
    // (納品が無い日 / すでに人が画面で入れた日 / まだラベル未発行)
    summary.severity = 'info';
    summary.note.push('追跡番号の投入を待っている納品はありません (納品が無い日、すでに画面で入力済み、またはラベル未発行)');
    return summary;
  }

  // ── 2. ここから先は「投入すべき納品がある」= CSVが必要 ──────
  const waiting = shipments.map((s) => `${s.shipmentConfirmationId}(${s.fcCode}) ${s.boxIds.length}箱`).join(' / ');

  let csv;
  try {
    csv = await fetchCsv({ file: opts.file, folderId: opts.folderId ?? process.env.FBA_TRACKING_DRIVE_FOLDER_ID, filename: opts.filename });
  } catch (e) {
    summary.severity = 'blocked';
    summary.blocked.push(
      `追跡番号待ちの納品が ${shipments.length}件あるのに、出荷実績CSVが取得できません: ${e.message}`,
      `対象: ${waiting}`,
      'iS-2の「出荷実績印刷・CSV出力」から本日分を fukutsu_tuiseki.csv として保存してください',
    );
    return summary;
  }
  summary.source = csv.source;
  summary.sourceHash = sha256(csv.buf);

  // ── 3. 解析 ────────────────────────────────────────────
  let rows;
  try {
    const parsed = parseTrackingCsv(csv.buf);
    rows = parsed.rows;
    if (parsed.problems.length) summary.blocked.push(...parsed.problems);
  } catch (e) {
    summary.severity = 'blocked';
    summary.blocked.push(e.message);
    return summary;
  }
  if (!rows.length) { summary.severity = 'blocked'; summary.blocked.push('CSVにデータ行がありません'); return summary; }

  // 🚨古いCSVの誤処理を防ぐ (固定ファイル名運用のため中身の出荷日で見る)。
  // ここに来ている時点で投入待ちの納品があるので、古いCSV = 人が本日分を出し忘れている
  const d = checkShipDate(rows, expectYmd);
  if (!d.ok) {
    summary.severity = 'blocked';
    summary.blocked.push(d.problem, `追跡番号待ちの納品: ${waiting}`);
    return summary;
  }

  // 同じCSVを二度処理しない (出荷日チェックと二重の防御)
  const dup = store.findBySourceHash(summary.sourceHash);
  if (dup) summary.note.push(`このCSVは ${dup.at} に処理済みです (同一内容の納品はスキップされます)`);

  if (summary.blocked.length) { summary.severity = 'blocked'; return summary; }

  // ── 4. 割り当て ────────────────────────────────────────
  const { assignments, excluded, problems, skipped } = buildAssignments(rows, shipments, {
    allowFcFallback: opts.allowFcFallback === true, // 既定OFF。移行期に手動実行するときだけ明示的に許す
  });
  summary.excluded = excluded;
  summary.skipped = skipped;
  if (problems.length) { // 1つでも怪しければ全部止める
    summary.severity = 'blocked';
    summary.blocked.push(...problems);
    return summary;
  }

  // ── 5. 納品ごとに投入 ──────────────────────────────────
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
        needsManual: true,
      });
      continue;
    }

    // 🚨前回「送ったが結果を書けずに落ちた」記録が残っている。APIの反映は数時間遅れるので
    //   自動で送り直すと二重投入になる。人が画面で確認するまで触らない
    const latest = store.findLatest(a.shipmentConfirmationId);
    if (latest && (latest.result === 'pending' || latest.indeterminate)) {
      summary.failed.push({
        shipmentConfirmationId: a.shipmentConfirmationId,
        error: `前回の投入が結果不明のまま残っています (${latest.at})。Seller Central画面で反映を確認し、` +
          '未登録なら画面から入力してください。自動では送り直しません',
        needsManual: true,
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

    // ⭐**送る前に** pending を残す。ここを書いた直後に落ちても、次回は自動で送り直さない
    const base = {
      runId, shipmentConfirmationId: a.shipmentConfirmationId, shipmentId: ship.shipmentId,
      inboundPlanId: ship.inboundPlanId, fcCode: a.fcCode, matchedBy: a.matchedBy,
      items: a.items, sourceFile: summary.source, sourceHash: summary.sourceHash,
    };
    store.append({ ...base, result: 'pending' });

    const res = await putTrackingDetails({ inboundPlanId: ship.inboundPlanId, shipmentId: ship.shipmentId }, items);
    const rec = {
      runId, shipmentConfirmationId: a.shipmentConfirmationId, shipmentId: ship.shipmentId,
      inboundPlanId: ship.inboundPlanId, fcCode: a.fcCode, matchedBy: a.matchedBy,
      items: a.items, sourceFile: summary.source, sourceHash: summary.sourceHash,
      result: res.ok ? 'success' : 'failed', operationId: res.operationId ?? null, error: res.error ?? null,
      indeterminate: res.indeterminate === true,
    };
    store.append(rec);
    if (res.ok) summary.registered.push(a);
    else summary.failed.push({
      shipmentConfirmationId: a.shipmentConfirmationId, error: res.error,
      retryable: res.retryable, needsManual: res.indeterminate === true,
    });
  }

  if (summary.failed.length) summary.severity = 'warn';
  return summary;
}

/** GChat 用の本文。読み手が「何をすればいいか」だけ分かるように書く。 */
export function formatSummary(s) {
  const L = [];
  const sev = s.severity ?? (s.blocked.length ? 'blocked' : s.failed.length ? 'warn' : 'ok');
  const head = { blocked: '🚨', warn: '⚠️', info: 'ℹ️', ok: s.registered.length ? '✅' : 'ℹ️' }[sev] ?? 'ℹ️';
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
