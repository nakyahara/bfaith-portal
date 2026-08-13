/**
 * picking — 欠品通知 (LINE/GChat) のテスト。fetch をグローバル差し替えで検証し、実APIは叩かない。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'picking-test-'));
delete process.env.PICKING_LINE_CHANNEL_TOKEN;
delete process.env.PICKING_LINE_TO;
delete process.env.PICKING_ALERT_WEBHOOK;

const { buildShortageText, notifyShortage } = await import('../notify.js');

let passed = 0;
function t(name, fn) { fn(); passed++; console.log(`  ok: ${name}`); }

const INFO = {
  batch: { folder_name: '出荷_03', hikiate_class: 'ネコポス手動単品' },
  line: { locationLabel: 'P3FB-001-003-03', location: '00100303', sku: 'teatree10', product_name: 'ティーツリーオイル', qty: 3 },
  worker: '星',
  shortageQty: 2,
};

t('buildShortageText: バッチ/ロケ/商品/数量/作業者が入る (一部欠品は確保数も)', () => {
  const text = buildShortageText(INFO);
  for (const s of ['出荷_03', 'P3FB-001-003-03', 'teatree10', '欠品 2個 / 指示 3個', '(1個は確保済み)', '星']) {
    assert.ok(text.includes(s), `"${s}" を含むはず:\n${text}`);
  }
  const full = buildShortageText({ ...INFO, shortageQty: 3 });
  assert.ok(!full.includes('確保済み'), '全量欠品では確保数を出さない');
});

// fetch を記録用に差し替え
const calls = [];
let failUrls = new Set();
globalThis.fetch = async (url, opts) => {
  calls.push({ url, body: JSON.parse(opts.body), auth: opts.headers?.Authorization || null });
  if (failUrls.has(url)) return { ok: false, status: 500, text: async () => 'boom' };
  return { ok: true, status: 200, text: async () => '' };
};

{
  // 未設定なら disabled (fetchも呼ばれない)
  const r = await notifyShortage(INFO);
  assert.equal(r, 'disabled');
  assert.equal(calls.length, 0);
  console.log('  ok: 両方未設定なら disabled (async)');
}

{
  // LINE broadcast (PICKING_LINE_TO 無し)
  process.env.PICKING_LINE_CHANNEL_TOKEN = 'test-token';
  calls.length = 0;
  const r = await notifyShortage(INFO);
  assert.equal(r, 'sent');
  assert.equal(calls.length, 1);
  assert.ok(calls[0].url.endsWith('/broadcast'));
  assert.equal(calls[0].auth, 'Bearer test-token');
  assert.equal(calls[0].body.messages[0].type, 'text');
  console.log('  ok: LINE broadcast (宛先未指定) (async)');
}

{
  // LINE push (宛先2件) + GChat 併用
  process.env.PICKING_LINE_TO = 'Uaaa, Cbbb';
  process.env.PICKING_ALERT_WEBHOOK = 'https://chat.example/webhook';
  calls.length = 0;
  const r = await notifyShortage(INFO);
  assert.equal(r, 'sent');
  const pushes = calls.filter((c) => c.url.endsWith('/push'));
  assert.equal(pushes.length, 2);
  assert.deepEqual(pushes.map((c) => c.body.to), ['Uaaa', 'Cbbb']);
  assert.ok(calls.some((c) => c.url === 'https://chat.example/webhook'));
  console.log('  ok: LINE push (複数宛先) + GChat 併用 (async)');
}

{
  // 片方失敗でも sent (両方失敗なら throw)
  failUrls = new Set(['https://chat.example/webhook']);
  calls.length = 0;
  const r = await notifyShortage(INFO);
  assert.equal(r, 'sent');
  failUrls = new Set(['https://api.line.me/v2/bot/message/push', 'https://chat.example/webhook']);
  let threw = false;
  try { await notifyShortage(INFO); } catch { threw = true; }
  assert.ok(threw, '全経路失敗は throw (呼び出し側が warn)');
  console.log('  ok: 片方失敗はsent・全滅はthrow (async)');
}

// ─── webhook 署名検証 (groupId取得用) ───
{
  const { handleLineWebhook } = await import('../notify.js');
  const cryptoMod = await import('node:crypto');
  const body = Buffer.from(JSON.stringify({ events: [{ type: 'join', source: { type: 'group', groupId: 'Cxxxx' } }] }));
  // secret未設定 → fail-closed
  delete process.env.PICKING_LINE_CHANNEL_SECRET;
  assert.equal(handleLineWebhook(body, 'sig'), false);
  // 正しい署名 → true / 改ざん → false
  process.env.PICKING_LINE_CHANNEL_SECRET = 'test-secret';
  const sig = cryptoMod.createHmac('sha256', 'test-secret').update(body).digest('base64');
  assert.equal(handleLineWebhook(body, sig), true);
  assert.equal(handleLineWebhook(body, sig.slice(0, -2) + 'xx'), false);
  assert.equal(handleLineWebhook(Buffer.from('tampered'), sig), false);
  console.log('  ok: LINE webhook 署名検証 (secret必須・改ざん拒否) (async)');
}

console.log(`\ntest-notify: ${passed + 5} 件 pass`);
