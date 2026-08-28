/**
 * shohyo-links — 証憑受け箱の突合 + 自動添付ジョブ (Render 常駐 node-cron・毎時)
 *
 * 台帳 = jobs-registry 'shohyo-voucher-attach'。
 * 1周期の仕事:
 *   1. 受け箱の未処理 (new/proposed/waiting_registration/ambiguous/no_match/error) を集める
 *   2. それらの日付を覆う期間のMF明細 + 仕訳を取る (1回のAPI往復で全件分)
 *   3. matchBatch でルール突合 (証憑→明細・明細→証憑の両方向で一意のときだけ strong) → status を更新
 *   4. strong かつ 明細が仕訳登録済み かつ 仕訳に証憑なし かつ 自動添付ON のときだけ、
 *      claim (リース) を取ってから POST /vouchers で貼る。提案モード (OFF) では proposed に置く
 *
 * 二重添付防止: POST の前に必ず claimForAttach()。cron と手動 (「今すぐ照合」「承認」) が重なっても
 * 確保できた1者だけが貼る。プロセス内でも同時に1周期しか走らせない。
 *
 * env: SHOHYO_ATTACH_ENABLED=false で停止 / SHOHYO_ATTACH_CRON (既定 "7 * * * *" JST)
 */
import cron from 'node-cron';
import { pingJob } from '../jobs-monitor/ping-local.js';
import { listLinks } from './db.js';
import { loadTokens, getTransactions, getJournalsByTransactionIds, postVoucher } from './mf-api.js';
import { matchBatch, isValidDate } from './matcher.js';
import {
  listOpenInbox, setMatch, readFile, autoAttachEnabled, transactionOwners,
  claimForAttach, releaseClaim, markAttached, recoverStaleClaims,
} from './inbox.js';

const JOB_ID = 'shohyo-voucher-attach';
const OFF = new Set(['0', 'false', 'off', 'no']);
const DAY_MS = 24 * 60 * 60 * 1000;
const iso = (d) => new Date(d).toISOString().slice(0, 10);

/** 受け箱の日付を覆う期間 (前後7日の余裕。日付が無効/無しは登録日)。MFの制約 = 366日以内 */
export function periodFor(items, now = Date.now()) {
  const dates = items
    .map(i => (isValidDate(i.doc_date) ? i.doc_date : String(i.created_at || '').slice(0, 10)))
    .map(Date.parse).filter(Number.isFinite);
  const max = Math.min((dates.length ? Math.max(...dates) : now) + 7 * DAY_MS, now + DAY_MS);
  const min = (dates.length ? Math.min(...dates) : now) - 7 * DAY_MS;
  return [iso(Math.max(min, max - 360 * DAY_MS)), iso(max)];
}

/**
 * 確保 → POST → 確定 を1か所にまとめる (cron と手動承認で同じ経路)。
 * @returns {{ ok: true, file_id } | { ok: false, error }}
 */
export async function attachWithClaim(item, journal, { tx_id, mode, actor, reason }) {
  const token = claimForAttach(item.id);
  if (!token) return { ok: false, error: 'claim_failed' }; // 他の実行者が先に確保した (または添付済み)
  try {
    const res = await postVoucher(journal.id, item.file_name, readFile(item).toString('base64'));
    const fileId = res?.voucher_file_ids?.[0]?.file_id || '';
    if (!fileId) console.warn(`[shohyo-attach] inbox#${item.id}: MFが file_id を返しませんでした`, JSON.stringify(res).slice(0, 200));
    const ok = markAttached(item.id, token, { journal_id: journal.id, journal_number: journal.number, tx_id, mf_file_id: fileId, mode, actor, reason });
    if (!ok) console.error(`[shohyo-attach] inbox#${item.id}: 確保トークンが一致せず attached にできませんでした (MF側には貼れています file_id=${fileId})`);
    return { ok: true, file_id: fileId };
  } catch (e) {
    releaseClaim(item.id, token, `${e.message}${e.detail ? ' ' + JSON.stringify(e.detail).slice(0, 200) : ''}`);
    return { ok: false, error: e.message };
  }
}

let running = null;

/**
 * 突合を1周期分実行する。同時に呼ばれたら進行中の結果を返す (二重実行しない)。
 * @param {{ attach?: boolean, actor?: string }} opts
 */
export function runInboxMatch(opts = {}) {
  if (running) return running;
  running = runOnce(opts).finally(() => { running = null; });
  return running;
}

async function runOnce({ attach = true, actor = 'cron' } = {}) {
  const items = listOpenInbox();
  const summary = { checked: items.length, proposed: 0, waiting: 0, ambiguous: 0, none: 0, attached: 0, errors: 0 };
  if (!items.length) return { ok: true, ...summary };
  if (!loadTokens()) return { ok: false, error: 'mf_not_connected', ...summary };

  const [start, end] = periodFor(items);
  const txs = await getTransactions(start, end);
  const registeredIds = txs.filter(t => t.journalizing_status !== 'none' && t.journalizing_status !== 'excluded').map(t => t.id);
  const journals = registeredIds.length ? await getJournalsByTransactionIds(registeredIds, start, end) : [];
  const journalByTx = new Map(journals.filter(j => j.transaction_id).map(j => [j.transaction_id, j]));
  const vendors = listLinks();
  const auto = autoAttachEnabled();

  // 判定フェーズ: 全証憑をまとめて (先着順で明細を奪わない)
  const results = matchBatch(items, txs, vendors, { owners: transactionOwners() });

  // 反映フェーズ
  for (const item of items) {
    const m = results.get(item.id);
    try {
      if (m.kind === 'none') { summary.none++; setMatch(item.id, { status: 'no_match', reason: m.reason, candidates: [] }); continue; }
      if (m.kind === 'ambiguous') { summary.ambiguous++; setMatch(item.id, { status: 'ambiguous', reason: m.reason, candidates: m.candidates }); continue; }

      const c = m.candidates[0];
      const j = journalByTx.get(c.tx_id) || null;
      if (c.status === 'none' || !j) {
        summary.waiting++;
        setMatch(item.id, { status: 'waiting_registration', tx_id: c.tx_id, strength: m.strength, reason: m.reason, candidates: m.candidates });
        continue;
      }
      // 相手の仕訳に既に証憑が付いている (人が貼った・MFが自動取得した) なら貼らずに提案に留める
      const alreadyHas = (j.voucher_file_ids || []).length > 0;
      if (attach && auto && m.strength === 'strong' && !alreadyHas) {
        const r = await attachWithClaim(item, j, { tx_id: c.tx_id, mode: 'auto', actor, reason: m.reason });
        if (r.ok) summary.attached++;
        else if (r.error !== 'claim_failed') summary.errors++;
      } else {
        summary.proposed++;
        setMatch(item.id, {
          status: 'proposed', tx_id: c.tx_id, journal_id: j.id, journal_number: j.number, strength: m.strength,
          reason: alreadyHas ? `${m.reason} (仕訳に証憑あり)` : m.reason, candidates: m.candidates,
        });
      }
    } catch (e) {
      summary.errors++;
      console.error(`[shohyo-attach] inbox#${item.id} failed:`, e.message, e.detail || '');
      setMatch(item.id, { status: 'error', reason: String(e.message).slice(0, 200), candidates: [] });
    }
  }
  return { ok: summary.errors === 0, ...summary, period: [start, end], auto };
}

async function tick() {
  try {
    const r = await runInboxMatch({ attach: true, actor: 'cron' });
    const note = r.error ? r.error : `checked=${r.checked} attached=${r.attached} proposed=${r.proposed} waiting=${r.waiting} ambiguous=${r.ambiguous} none=${r.none} err=${r.errors}`;
    console.log(`[shohyo-attach] ${note}`);
    // 未接続は「動いているが仕事ができない」なので ok にしない
    pingJob(JOB_ID, r.error === 'mf_not_connected' ? 'partial' : (r.ok ? 'ok' : 'partial'), note);
  } catch (e) {
    console.error('[shohyo-attach] tick failed:', e.message);
    pingJob(JOB_ID, 'fail', String(e.message).slice(0, 180));
  }
}

export function startShohyoAttachCron() {
  try {
    const n = recoverStaleClaims();
    if (n) console.warn(`[shohyo-attach] 前回途中で止まった添付 ${n}件を error に戻しました (受け箱で確認)`);
  } catch (e) { console.error('[shohyo-attach] init failed:', e.message); }
  if (OFF.has(String(process.env.SHOHYO_ATTACH_ENABLED ?? '').trim().toLowerCase())) {
    console.log('[shohyo-attach] cron: disabled');
    return;
  }
  const expr = (process.env.SHOHYO_ATTACH_CRON || '7 * * * *').trim();
  if (!cron.validate(expr)) {
    console.error(`[shohyo-attach] 不正な cron 式 "${expr}" — 起動しません`);
    return;
  }
  cron.schedule(expr, tick, { timezone: 'Asia/Tokyo' });
  console.log(`[shohyo-attach] cron: enabled (${expr} JST)`);
}
