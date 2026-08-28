/**
 * shohyo-links — 証憑受け箱の突合 + 自動添付ジョブ (Render 常駐 node-cron・毎時)
 *
 * 台帳 = jobs-registry 'shohyo-voucher-attach'。
 * 1周期の仕事:
 *   1. 受け箱の未処理 (new/proposed/waiting_registration/ambiguous/no_match/error) を集める
 *   2. それらの日付を覆う期間のMF明細 + 仕訳を取る (1回のAPI往復で全件分)
 *   3. matcher でルール突合 → status を更新
 *   4. strong一致 かつ 明細が仕訳登録済み かつ 自動添付ON のときだけ POST /vouchers で貼る
 *      (提案モード=OFF のときは proposed に置いて人の承認を待つ)
 *
 * env: SHOHYO_ATTACH_ENABLED=false で停止 / SHOHYO_ATTACH_CRON (既定 "7 * * * *" JST)
 */
import cron from 'node-cron';
import { pingJob } from '../jobs-monitor/ping-local.js';
import { listLinks } from './db.js';
import { loadTokens, getTransactions, getJournalsByTransactionIds, postVoucher } from './mf-api.js';
import { matchVoucher } from './matcher.js';
import {
  listOpenInbox, setMatch, markAttached, markError, readFile, autoAttachEnabled, takenTransactionIds,
} from './inbox.js';

const JOB_ID = 'shohyo-voucher-attach';
const OFF = new Set(['0', 'false', 'off', 'no']);
const DAY_MS = 24 * 60 * 60 * 1000;
const iso = (d) => new Date(d).toISOString().slice(0, 10);

/** 受け箱の日付を覆う期間 (前後7日の余裕。日付なしは登録日を使う)。MFの制約 = 366日以内 */
function periodFor(items) {
  const dates = items.map(i => i.doc_date || i.created_at.slice(0, 10)).map(Date.parse).filter(Number.isFinite);
  if (!dates.length) return null;
  const min = Math.min(...dates) - 7 * DAY_MS;
  const max = Math.min(Math.max(...dates) + 7 * DAY_MS, Date.now() + DAY_MS);
  return [iso(Math.max(min, max - 360 * DAY_MS)), iso(max)];
}

/**
 * 突合を1周期分実行する。
 * @param {{ attach?: boolean, actor?: string }} opts  attach=false なら status 更新だけ (画面の「今すぐ照合」用)
 */
export async function runInboxMatch({ attach = true, actor = 'cron' } = {}) {
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
  const taken = takenTransactionIds();

  for (const item of items) {
    try {
      // 自分が既に確定している相手は「他の証憑に取られている」扱いにしない
      const takenExceptMe = new Set([...taken].filter(id => id !== item.match_tx_id));
      const m = matchVoucher(item, txs, vendors, { taken: takenExceptMe });
      if (m.kind === 'none') { summary.none++; setMatch(item.id, { status: 'no_match', reason: m.reason, candidates: [] }); continue; }
      if (m.kind === 'ambiguous') { summary.ambiguous++; setMatch(item.id, { status: 'ambiguous', reason: m.reason, candidates: m.candidates }); continue; }

      const c = m.candidates[0];
      const j = journalByTx.get(c.tx_id) || null;
      if (c.status === 'none' || !j) {
        summary.waiting++;
        setMatch(item.id, { status: 'waiting_registration', tx_id: c.tx_id, strength: m.strength, reason: m.reason, candidates: m.candidates });
        taken.add(c.tx_id);
        continue;
      }
      // 相手の仕訳に既に証憑が付いている (人が貼った・MFが自動取得した) なら貼らずに提案に留める
      const alreadyHas = (j.voucher_file_ids || []).length > 0;
      if (attach && auto && m.strength === 'strong' && !alreadyHas) {
        const res = await postVoucher(j.id, item.file_name, readFile(item).toString('base64'));
        const fileId = res?.voucher_file_ids?.[0]?.file_id || '';
        markAttached(item.id, { journal_id: j.id, journal_number: j.number, tx_id: c.tx_id, mf_file_id: fileId, mode: 'auto', actor, reason: m.reason });
        summary.attached++;
      } else {
        summary.proposed++;
        setMatch(item.id, {
          status: 'proposed', tx_id: c.tx_id, journal_id: j.id, journal_number: j.number, strength: m.strength,
          reason: alreadyHas ? `${m.reason} (仕訳に証憑あり)` : m.reason, candidates: m.candidates,
        });
      }
      taken.add(c.tx_id);
    } catch (e) {
      summary.errors++;
      console.error(`[shohyo-attach] inbox#${item.id} failed:`, e.message, e.detail || '');
      markError(item.id, `${e.message}${e.detail ? ' ' + JSON.stringify(e.detail).slice(0, 200) : ''}`);
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
