/**
 * notify-jobs.mjs — ロジザードの取込の知らせ (GChat の要対応スペース・GCHAT_WEBHOOK_JOBS)
 *
 * Render の画面の即時の知らせ (admin-router.js) と同じスペース = 止め・要確認・止まった取込・再適用待ちが 1 か所に届く (③c-1b-2b-2 契約 v3 H)。
 * miniPC の設定はリポジトリ直下の .env の 1 つだけ。送り先が無い = 毎晩の本番はログインの前に止める (呼び手)。
 */

/** 送り先 (https の URL として読めて、ホスト名があるものだけ)。無い・壊れている = null (呼び手はログインの前に止める。Codex #1547 R2 Medium) */
export function jobsHook(env = process.env) {
  const h = String(env.GCHAT_WEBHOOK_JOBS || '').trim();
  if (!h || /\s/.test(h)) return null;
  let u;
  try { u = new URL(h); } catch { return null; }
  return u.protocol === 'https:' && /^[a-z0-9.-]+$/i.test(u.hostname) && u.hostname.includes('.') ? h : null;
}

/** 送る (送れた = true・送り先が無い / 失敗 = false。例外は投げない) */
export async function sendJobsChat(text, { env = process.env, fetchImpl = fetch, timeoutMs = 15000 } = {}) {
  const hook = jobsHook(env);
  if (!hook) return false;
  try {
    const res = await fetchImpl(hook, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ text: String(text).slice(0, 4000) }), signal: AbortSignal.timeout(timeoutMs) });
    return !!res.ok;
  } catch { return false; }
}
