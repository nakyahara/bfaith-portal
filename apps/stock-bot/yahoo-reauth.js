/**
 * yahoo-reauth.js — Google Chat から Yahoo OAuth の再認可を完結させる (在庫検索ボットに相乗り)
 *
 * 背景: Yahoo!ショッピング API の refresh token は 28 日で失効し、月 1 回「認可 URL を開く → Yahoo! JAPAN ID で
 * ログイン → https://b-faith.biz/?code=XXXX に戻る → code をトークン交換」の人手作業が要る
 * (『Yahoo OAuth トークン再認可運用』)。ログインと同意はブラウザでしかできないが、それ以外を GChat に寄せる:
 *   1. 「yahoo再認可」と送る → 認可 URL と手順を返す (VPS /yahoo/auth-url)
 *   2. リダイレクト先の URL (…?code=XXXX) をそのまま貼る (or「yahoo code XXXX」) → VPS /yahoo/token/init → 期限を返す
 * 制限: 実行者は YAHOO_REAUTH_USERS (カンマ区切りメール、未設定なら PD_RULE_APPROVERS) に限定 = fail-closed。
 * PII/秘密: code はログに出さない。VPS secret は env からだけ読む。
 */

const AUTH_RE = /^\s*yahoo\s*(再認可|再認証|reauth|auth|認可)\s*$/i;
const MANUAL_CODE_RE = /^\s*yahoo\s+code\s+([A-Za-z0-9_.-]{6,200})\s*$/i;
const CODE_VALUE_RE = /^[A-Za-z0-9_.-]{6,200}$/;
const REDIRECT_HOSTS = new Set(['b-faith.biz', 'www.b-faith.biz']);

/** 貼り付けられた URL が Yahoo のコールバック先 (b-faith.biz) で code を持つときだけ code を返す (他サイトの ?code= は拾わない — Codex R1 Medium) */
function codeFromRedirectUrl(text) {
  const m = String(text).match(/https?:\/\/\S+/);
  if (!m) return null;
  let u;
  try { u = new URL(m[0]); } catch { return null; }
  if (!REDIRECT_HOSTS.has(u.hostname.toLowerCase()) || (u.pathname !== '/' && u.pathname !== '')) return null;
  const code = u.searchParams.get('code');
  return code && CODE_VALUE_RE.test(code) ? code : null;
}

export function parseYahooReauthCommand(text) {
  const t = String(text || '').trim();
  if (AUTH_RE.test(t)) return { kind: 'auth-url' };
  const manual = t.match(MANUAL_CODE_RE);
  if (manual) return { kind: 'code', code: manual[1] };
  const fromUrl = codeFromRedirectUrl(t);
  if (fromUrl) return { kind: 'code', code: fromUrl };
  return null;
}

export function reauthUserAllowed(email, env = process.env) {
  const raw = String(env.YAHOO_REAUTH_USERS || env.PD_RULE_APPROVERS || '').trim();
  if (!raw) return false;
  return raw.toLowerCase().split(',').map((s) => s.trim()).filter(Boolean).includes(String(email || '').toLowerCase());
}

function proxyConfig(env) {
  const url = String(env.YAHOO_PROXY_URL || '').trim().replace(/\/$/, '');
  const secret = String(env.YAHOO_PROXY_SECRET || '').trim(); // Yahoo 用 secret のみ (fail-closed、Codex R1 Low)
  return { url, secret };
}

async function getHealth(url, secret, fetchImpl) {
  try {
    const r = await fetchImpl(`${url}/yahoo/health`, { headers: { 'X-Proxy-Secret': secret }, signal: AbortSignal.timeout(10000) });
    return r.ok ? await r.json() : null;
  } catch { return null; }
}
const fmtExpiry = (iso) => {
  if (!iso) return '不明';
  const d = new Date(iso);
  const days = Math.floor((d - Date.now()) / 86400000);
  return `${d.toISOString().slice(0, 10)} (残り ${days} 日)`;
};

/**
 * @returns Chat 応答オブジェクト { text }
 */
export async function handleYahooReauth(cmd, { email, env = process.env, fetchImpl = fetch } = {}) {
  if (!reauthUserAllowed(email, env)) {
    return { text: `Yahoo 再認可の権限がありません (${email || '不明'})。Render の YAHOO_REAUTH_USERS に追加すると使えます。` };
  }
  const { url, secret } = proxyConfig(env);
  if (!url || !secret) return { text: 'Yahoo プロキシの設定 (YAHOO_PROXY_URL / YAHOO_PROXY_SECRET) が Render にありません。' };

  if (cmd.kind === 'auth-url') {
    let auth;
    try {
      const r = await fetchImpl(`${url}/yahoo/auth-url`, { headers: { 'X-Proxy-Secret': secret }, signal: AbortSignal.timeout(10000) });
      if (!r.ok) return { text: `認可 URL を取得できませんでした (HTTP ${r.status})。VPS プロキシを確認してください。` };
      auth = await r.json();
    } catch (e) {
      return { text: `認可 URL を取得できませんでした (${e?.name === 'TimeoutError' ? 'timeout' : 'network'})。VPS プロキシを確認してください。` };
    }
    const health = await getHealth(url, secret, fetchImpl);
    return {
      text: [
        `🔑 *Yahoo API 再認可* (現在の refresh token 期限: ${fmtExpiry(health?.refreshTokenExpiresAt)})`,
        '',
        '1. 下のリンクを開き、Yahoo! JAPAN ID (b-faith01 のストア用) でログイン → 「同意」',
        auth.url,
        '',
        '2. 「https://b-faith.biz/?code=…」に戻ったら、その *URL をそのままこのチャットに貼る* だけで完了します',
        '   (URL が長くて貼りにくければ「yahoo code XXXX」でも可)',
        '',
        '※ 認可コードは数分で失効し 1 回しか使えません。戻ったらすぐ貼ってください',
        '※ 貼った URL はチャット履歴に残るので、このボットとの DM で行い、完了したらそのメッセージは削除して構いません',
      ].join('\n'),
    };
  }

  if (cmd.kind === 'code') {
    let r, body = null;
    try {
      r = await fetchImpl(`${url}/yahoo/token/init`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Proxy-Secret': secret },
        body: JSON.stringify({ code: cmd.code }),
        signal: AbortSignal.timeout(20000),
      });
      try { body = await r.json(); } catch { body = null; }
    } catch (e) {
      return { text: `トークン交換に失敗しました (${e?.name === 'TimeoutError' ? 'timeout' : 'network'})。もう一度「yahoo再認可」からやり直してください。` };
    }
    if (!r.ok || !body?.ok) {
      // 認可コード期限切れ/使い回しが典型。詳細文字列は返さない (上流の文言を持ち回らない)
      return { text: `❌ トークン交換に失敗しました (HTTP ${r.status})。認可コードが古い可能性があります。「yahoo再認可」からやり直してください。` };
    }
    const health = await getHealth(url, secret, fetchImpl);
    return {
      text: `✅ Yahoo API の再認可が完了しました。refresh token の新しい期限: *${fmtExpiry(health?.refreshTokenExpiresAt)}*\n次回もこのチャットで「yahoo再認可」と送ればOKです。`,
    };
  }
  return { text: 'Yahoo 再認可コマンドを解釈できませんでした。' };
}
