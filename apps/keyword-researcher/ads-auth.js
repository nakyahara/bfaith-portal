/**
 * Amazon Ads API OAuth2 認証フロー
 * リフレッシュトークンを取得するためのワンタイム認証用ルーター
 */
import { Router } from 'express';
import crypto from 'crypto';

const router = Router();

const ADS_CLIENT_ID = process.env.ADS_CLIENT_ID;
const ADS_CLIENT_SECRET = process.env.ADS_CLIENT_SECRET;
const TOKEN_URL = 'https://api.amazon.co.jp/auth/o2/token';
const AUTH_URL = 'https://www.amazon.co.jp/ap/oa';

// ── Step 1: 認証開始 → Amazonの認可画面にリダイレクト ──
router.get('/start', (req, res) => {
  if (!ADS_CLIENT_ID) {
    return res.status(500).send('ADS_CLIENT_ID が .env に設定されていません');
  }

  const state = crypto.randomBytes(16).toString('hex');
  req.session.adsAuthState = state;

  const redirectUri = `${req.protocol}://${req.get('host')}/ads-auth/callback`;
  const params = new URLSearchParams({
    client_id: ADS_CLIENT_ID,
    scope: 'advertising::campaign_management',
    response_type: 'code',
    redirect_uri: redirectUri,
    state,
  });

  res.redirect(`${AUTH_URL}?${params.toString()}`);
});

// ── Step 2: コールバック → 認可コードをトークンに交換 ──
router.get('/callback', async (req, res) => {
  const { code, state, error } = req.query;

  if (error) {
    return res.status(400).send(`認証エラー: ${error}`);
  }

  if (!code) {
    return res.status(400).send('認可コードが取得できませんでした');
  }

  // CSRF チェック
  if (state !== req.session.adsAuthState) {
    return res.status(403).send('state パラメータが一致しません（CSRF対策）');
  }

  const redirectUri = `${req.protocol}://${req.get('host')}/ads-auth/callback`;

  try {
    const tokenRes = await fetch(TOKEN_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8' },
      body: new URLSearchParams({
        grant_type: 'authorization_code',
        code,
        client_id: ADS_CLIENT_ID,
        client_secret: ADS_CLIENT_SECRET,
        redirect_uri: redirectUri,
      }).toString(),
    });

    const data = await tokenRes.json();

    if (data.error) {
      return res.status(400).send(`
        <h2>トークン取得エラー</h2>
        <p>${data.error}: ${data.error_description || ''}</p>
      `);
    }

    // Profile ID を取得
    let profiles = [];
    try {
      const profileRes = await fetch('https://advertising-api-fe.amazon.com/v2/profiles', {
        headers: {
          'Authorization': `Bearer ${data.access_token}`,
          'Amazon-Advertising-API-ClientId': ADS_CLIENT_ID,
          'Content-Type': 'application/json',
        },
      });
      profiles = await profileRes.json();
    } catch (e) {
      console.error('[AdsAuth] Profile取得エラー:', e.message);
    }

    // 結果を表示（ユーザーが.envにコピペする）
    const jpProfile = Array.isArray(profiles)
      ? profiles.find(p => p.countryCode === 'JP')
      : null;

    res.send(`
      <!DOCTYPE html>
      <html><head><meta charset="utf-8"><title>Amazon Ads API 認証完了</title>
      <style>
        body { font-family: sans-serif; max-width: 800px; margin: 40px auto; padding: 0 20px; }
        .success { background: #d4edda; border: 1px solid #c3e6cb; padding: 15px; border-radius: 4px; margin-bottom: 20px; }
        .env-block { background: #f8f9fa; border: 1px solid #dee2e6; padding: 15px; border-radius: 4px; font-family: monospace; white-space: pre-wrap; word-break: break-all; }
        .warning { background: #fff3cd; border: 1px solid #ffc107; padding: 15px; border-radius: 4px; margin-top: 20px; }
        h1 { color: #28a745; }
      </style></head>
      <body>
        <h1>認証完了！</h1>
        <div class="success">Amazon Ads API の認証に成功しました。</div>

        <h2>.env に以下を設定してください：</h2>
        <div class="env-block">ADS_REFRESH_TOKEN=${data.refresh_token}
${jpProfile ? `ADS_PROFILE_ID=${jpProfile.profileId}` : '# Profile IDが取得できませんでした'}</div>

        ${jpProfile ? `
        <h2>Profile 情報</h2>
        <table border="1" cellpadding="8" cellspacing="0">
          <tr><td>Profile ID</td><td>${jpProfile.profileId}</td></tr>
          <tr><td>国</td><td>${jpProfile.countryCode}</td></tr>
          <tr><td>通貨</td><td>${jpProfile.currencyCode}</td></tr>
          <tr><td>タイムゾーン</td><td>${jpProfile.timezone}</td></tr>
          <tr><td>アカウントタイプ</td><td>${jpProfile.accountInfo?.type || '-'}</td></tr>
          <tr><td>マーケットプレイスID</td><td>${jpProfile.accountInfo?.marketplaceStringId || '-'}</td></tr>
        </table>` : ''}

        ${profiles.length > 1 ? `
        <h2>全Profile一覧</h2>
        <pre>${JSON.stringify(profiles, null, 2)}</pre>` : ''}

        <div class="warning">
          <strong>重要:</strong> この画面に表示されているリフレッシュトークンは秘密情報です。<br>
          .env にコピペしたら、このページは閉じてください。
        </div>
      </body></html>
    `);
  } catch (err) {
    console.error('[AdsAuth] トークン交換エラー:', err);
    res.status(500).send(`トークン交換に失敗: ${err.message}`);
  }
});

// ── 認証ステータス確認 ──
router.get('/status', async (req, res) => {
  const refreshToken = process.env.ADS_REFRESH_TOKEN;
  const profileId = process.env.ADS_PROFILE_ID;

  if (!refreshToken) {
    return res.json({ authenticated: false, message: 'リフレッシュトークン未設定' });
  }

  try {
    // トークンをリフレッシュしてみる
    const tokenRes = await fetch(TOKEN_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8' },
      body: new URLSearchParams({
        grant_type: 'refresh_token',
        refresh_token: refreshToken,
        client_id: ADS_CLIENT_ID,
        client_secret: ADS_CLIENT_SECRET,
      }).toString(),
    });

    const data = await tokenRes.json();

    if (data.error) {
      return res.json({ authenticated: false, message: `トークンエラー: ${data.error}` });
    }

    res.json({
      authenticated: true,
      profileId: profileId || '未設定',
      message: 'Ads API 認証OK',
    });
  } catch (err) {
    res.json({ authenticated: false, message: err.message });
  }
});

export default router;
