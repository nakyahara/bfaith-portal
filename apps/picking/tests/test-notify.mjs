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

// ─── 祝日判定 (jp-holiday.js) ───
{
  const { isJpHolidayJst, isJstWeekendOrHoliday } = await import('../jp-holiday.js');
  const jst = (s) => new Date(`${s}T10:00:00+09:00`);
  t('jp-holiday: 2026年の固定祝日・ハッピーマンデー・春秋分', () => {
    assert.equal(isJpHolidayJst(jst('2026-01-01')), true, '元日');
    assert.equal(isJpHolidayJst(jst('2026-01-12')), true, '成人の日 (1月第2月曜)');
    assert.equal(isJpHolidayJst(jst('2026-03-20')), true, '春分');
    assert.equal(isJpHolidayJst(jst('2026-08-11')), true, '山の日');
    assert.equal(isJpHolidayJst(jst('2026-09-21')), true, '敬老の日 (9月第3月曜)');
    assert.equal(isJpHolidayJst(jst('2026-09-23')), true, '秋分');
    assert.equal(isJpHolidayJst(jst('2025-03-20')), true, '2025春分 (別年の式検証)');
  });
  t('jp-holiday: 振替休日と国民の休日', () => {
    assert.equal(isJpHolidayJst(jst('2026-05-06')), true, '5/3憲法記念日が日曜→5/4,5/5も祝日→振替は5/6');
    assert.equal(isJpHolidayJst(jst('2026-09-22')), true, '敬老の日と秋分に挟まれた国民の休日');
    assert.equal(isJpHolidayJst(jst('2026-05-07')), false, '振替の翌日は平日');
  });
  t('jp-holiday: 土日と平日・JST境界', () => {
    assert.equal(isJstWeekendOrHoliday(jst('2026-08-15')), true, '土曜');
    assert.equal(isJpHolidayJst(jst('2026-08-15')), false, '土曜は祝日ではない');
    assert.equal(isJstWeekendOrHoliday(jst('2026-08-17')), false, '平日の月曜');
    // UTCではまだ日曜でも、JSTで月曜朝なら平日 (2026-08-16 23:00Z = JST 8/17 08:00)
    assert.equal(isJstWeekendOrHoliday(new Date('2026-08-16T23:00:00Z')), false, 'JST基準で判定');
    assert.equal(isJstWeekendOrHoliday(new Date('2026-08-14T16:00:00Z')), true, 'JST 8/15(土) 01:00');
  });
  t('jp-holiday: サポート範囲 (2022〜) 外は平日扱い (五輪特例等の過去年を誤判定しない)', () => {
    assert.equal(isJpHolidayJst(jst('2021-07-22')), false, '2021五輪特例 (海の日移動) は範囲外=判定しない');
    assert.equal(isJpHolidayJst(jst('2020-08-10')), false, '2020特例も範囲外');
    assert.equal(isJpHolidayJst(jst('2022-01-01')), true, '範囲の下限2022年は判定する');
  });
}

// ─── 土日祝の送り分け (PICKING_LINE_TO_HOLIDAY) ───
{
  const { resolveLineTo } = await import('../notify.js');
  const sat = new Date('2026-08-15T10:00:00+09:00');
  const mon = new Date('2026-08-17T10:00:00+09:00');
  const holiday = new Date('2026-09-22T10:00:00+09:00');   // 国民の休日 (火曜)
  process.env.PICKING_LINE_TO = 'Cnormal';
  process.env.PICKING_LINE_TO_HOLIDAY = 'Choliday';
  t('resolveLineTo: 土日祝は休日宛先・平日は通常宛先', () => {
    assert.deepEqual(resolveLineTo(sat).to, ['Choliday'], '土曜');
    assert.deepEqual(resolveLineTo(holiday).to, ['Choliday'], '平日の祝日');
    assert.deepEqual(resolveLineTo(mon).to, ['Cnormal'], '平日');
  });
  t('resolveLineTo: 休日宛先が未設定なら土日祝も通常宛先へフォールバック', () => {
    delete process.env.PICKING_LINE_TO_HOLIDAY;
    assert.deepEqual(resolveLineTo(sat).to, ['Cnormal']);
  });
  // エンドツーエンド: notifyShortage が土曜に休日グループへpushする
  process.env.PICKING_LINE_TO_HOLIDAY = 'Choliday';
  failUrls = new Set();
  calls.length = 0;
  await notifyShortage(INFO, sat);
  const pushTos = calls.filter((c) => c.url.endsWith('/push')).map((c) => c.body.to);
  assert.deepEqual(pushTos, ['Choliday'], '土曜の欠品は休日ラインのみへ');
  calls.length = 0;
  await notifyShortage(INFO, mon);
  assert.deepEqual(calls.filter((c) => c.url.endsWith('/push')).map((c) => c.body.to), ['Cnormal']);
  console.log('  ok: notifyShortage: 土曜=休日ライン・平日=通常ライン (async)');
  // 後続テストへの影響を戻す
  delete process.env.PICKING_LINE_TO_HOLIDAY;
  process.env.PICKING_LINE_TO = 'Uaaa, Cbbb';
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

// ─── Notionカードのproperties組み立て (担当者連携) ───
{
  const { buildCardProperties } = await import('../notion.js');
  const schema = {
    titleProp: '名前',
    statusProp: { name: 'ステータス', type: 'select' },
    workerProp: { name: 'ピッキング担当者', type: 'select' },
  };
  const p1 = buildCardProperties(schema, 'ピッキング中', '星');
  assert.deepEqual(p1['ステータス'], { select: { name: 'ピッキング中' } });
  assert.deepEqual(p1['ピッキング担当者'], { select: { name: '星' } });
  // email (セッションログイン) は選択肢を増殖させないため設定しない
  const p2 = buildCardProperties(schema, 'ピッキング完了', 'd.nakahara@b-faith.biz');
  assert.equal(p2['ピッキング担当者'], undefined);
  // workerProp が無いDBでも動く / status型のDBにも対応
  const p3 = buildCardProperties({ titleProp: '名前', statusProp: { name: 'ステータス', type: 'status' }, workerProp: null }, '完了', '星');
  assert.deepEqual(p3['ステータス'], { status: { name: '完了' } });
  assert.equal(Object.keys(p3).length, 1);
  console.log('  ok: buildCardProperties (担当者select・email除外・status型対応) (async)');
}

// ─── カードタイトルの部分一致判定 ───
{
  const { titleMatchesFolder } = await import('../notion.js');
  assert.equal(titleMatchesFolder('出荷_18', '出荷_18'), true, '完全一致');
  assert.equal(titleMatchesFolder('8/13 出荷_18 ネコポス', '出荷_18'), true, 'タイトルに含む');
  assert.equal(titleMatchesFolder('出荷_18', '出荷_1'), false, '出荷_1は出荷_18に誤マッチしない');
  assert.equal(titleMatchesFolder('出荷_1 (AES)', '出荷_01'), true, 'ゼロ埋め表記ゆれを同一視');
  assert.equal(titleMatchesFolder('出荷_01', '出荷_1'), true, '逆方向のゆれも同一視');
  assert.equal(titleMatchesFolder('別のカード', '出荷_18'), false);
  console.log('  ok: titleMatchesFolder (含む判定・番号境界・ゼロ埋めゆれ) (async)');
}

// ─── 時間系プロパティ ───
{
  const { buildCardProperties, toJstDateValue } = await import('../notion.js');
  assert.equal(toJstDateValue('2026-08-13T00:30:00Z'), '2026-08-13T09:30:00+09:00', 'JST変換');
  assert.equal(toJstDateValue('invalid'), null);

  const schema = {
    titleProp: '名前',
    statusProp: { name: 'ステータス', type: 'select' },
    workerProp: { name: 'ピッキング担当者', type: 'select' },
    startProp: 'ピッキング開始', endProp: 'ピッキング終了',
    minutesProp: 'ピッキング時間(分)', secPerLineProp: '秒/明細',
  };
  // 完了時: 開始/終了/時間(分)/秒明細 が入る (中断5分は除外済みのactiveSecが渡る)
  const done = buildCardProperties(schema, 'ピッキング完了', '星', {
    startedAt: '2026-08-13T00:00:00Z', finishedAt: '2026-08-13T00:20:00Z',
    activeSec: 900, lineCount: 30,
  });
  assert.equal(done['ピッキング開始'].date.start, '2026-08-13T09:00:00+09:00');
  assert.equal(done['ピッキング終了'].date.start, '2026-08-13T09:20:00+09:00');
  assert.equal(done['ピッキング時間(分)'].number, 15);
  assert.equal(done['秒/明細'].number, 30);
  // 作業中 (完了取消後): 終了・時間はクリア、開始は維持
  const picking = buildCardProperties(schema, 'ピッキング中', '星', {
    startedAt: '2026-08-13T00:00:00Z', finishedAt: null, activeSec: null, lineCount: 30,
  });
  assert.equal(picking['ピッキング終了'].date, null);
  assert.equal(picking['ピッキング時間(分)'].number, null);
  assert.ok(picking['ピッキング開始'].date.start);
  // プロパティが無いDBでは時間系を一切書かない
  const minimal = buildCardProperties(
    { titleProp: '名前', statusProp: { name: 'ステータス', type: 'select' }, workerProp: null },
    'ピッキング完了', '星',
    { startedAt: '2026-08-13T00:00:00Z', finishedAt: '2026-08-13T00:20:00Z', activeSec: 900, lineCount: 30 });
  assert.deepEqual(Object.keys(minimal), ['ステータス']);
  console.log('  ok: 時間系プロパティ (完了で記入・再作業でクリア・未作成はスキップ) (async)');
}

// ─── プロパティ名の全角/半角ゆれ (実運用 8/13: （分） が検出されなかった) ───
{
  const { getSchema } = await import('../notion.js');
  const dbMeta = {
    properties: {
      '名前': { type: 'title' },
      'ステータス': { type: 'select' },
      'ピッキング担当者': { type: 'select' },
      'ピッキング時間（分）': { type: 'number' },   // 全角括弧
      '秒／明細': { type: 'number' },               // 全角スラッシュ
      'ピッキング開始': { type: 'date' },
    },
  };
  globalThis.fetch = async () => ({ ok: true, status: 200, json: async () => dbMeta, text: async () => '' });
  process.env.PICKING_NOTION_TOKEN = 'test';
  const schema = await getSchema();
  assert.equal(schema.minutesProp, 'ピッキング時間（分）', '全角括弧でも検出し、書込には実名を使う');
  assert.equal(schema.secPerLineProp, '秒／明細');
  assert.equal(schema.startProp, 'ピッキング開始');
  assert.equal(schema.endProp, undefined, '未作成のものは書かない');
  console.log('  ok: プロパティ名の全角/半角ゆれを同一視 (async)');
}

console.log(`\ntest-notify: ${passed + 10} 件 pass`);
