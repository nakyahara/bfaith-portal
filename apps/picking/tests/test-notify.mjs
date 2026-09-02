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
// 他ロケ在庫の warehouse 連携は未設定状態を既定にする (既存テストの通知本文・fetch回数を変えない)
delete process.env.WAREHOUSE_URL;
delete process.env.WAREHOUSE_SERVICE_TOKEN;

const { buildShortageText, notifyShortage } = await import('../notify.js');
const { listStockCandidates } = await import('../stock-locations.js');

let passed = 0;
function t(name, fn) { fn(); passed++; console.log(`  ok: ${name}`); }

const INFO = {
  batch: { folder_name: '出荷_03', hikiate_class: 'ネコポス手動単品' },
  line: { locationLabel: 'P3FB-001-003-03', location: '00100303', sku: 'teatree10', product_name: 'ティーツリーオイル', qty: 3 },
  worker: '星',
  shortageQty: 2,
};

t('listStockCandidates: 画面用はロケ単位にまとめ (free合計・期限は近い方)・現ロケ除外・上限', () => {
  const data = { importedAt: new Date().toISOString(), locations: [
    { block: 'P3FB', location: '001-003-03', free: 5, quality: '良品' },                       // 現ロケ (除外)
    { block: 'P4FA', location: '001-003-02', free: 2, quality: '良品', expiry: '20280101' },
    { block: 'P4FA', location: '001-003-02', free: 3, quality: '良品', expiry: '20270601' },  // 同ロケ別ロット
    { block: 'P4FA', location: '001-003-09', free: 1, quality: '不良品' },                      // 良品以外 (除外)
    { block: 'ZZZ', location: 'ZZZ-ZZZ-ZZ', free: 40, quality: '良品' },
  ] };
  const g = listStockCandidates(data, { excludeBlock: 'P3FB', excludeLocation: '00100303', groupByLocation: true, maxRows: 1 });
  assert.equal(g.fetched, true);
  assert.deepEqual(g.rows.map((r) => [r.label, r.free, r.expiry]), [['P4FA-001-003-02', 5, '2027/06/01']]);
  assert.equal(g.truncated, 1, 'ZZZ が上限で落ちた件数');
  const raw = listStockCandidates(data, { excludeBlock: 'P3FB', excludeLocation: '00100303' });
  assert.equal(raw.rows.length, 3, '通知用はロットごと');
  assert.equal(listStockCandidates(null).fetched, false, '取得失敗は fetched=false (候補ゼロと区別)');
});

t('buildShortageText v2: 判断結果で見出しが変わる (他ロケ全量確保/後で取りに行く/どこにもない)', () => {
  const alt = buildShortageText({ ...INFO, shortageQty: 2, altFree: 1,
    line: { ...INFO.line, alt_block: 'P4FA', alt_location: '001-003-02', alt_qty: 2, remaining_qty: 0, remaining: null } });
  assert.ok(alt.startsWith('📦 他ロケからピッキングしました — ロジザードで数量を減らしてください'), alt);
  assert.ok(alt.includes('→ P4FA-001-003-02 から 2個 確保しました (表示在庫1より多い・現物優先)'), alt);
  assert.ok(alt.includes('ロジザード: P4FA-001-003-02 の在庫を 2個 減らしてください'), alt);
  assert.ok(!alt.includes('他ロケ在庫'), '全量確保のときは他ロケ一覧を付けない');
  const later = buildShortageText({ ...INFO, shortageQty: 3, stockText: '📍 他ロケ在庫: なし',
    line: { ...INFO.line, alt_block: 'P4FA', alt_location: '001-003-02', alt_qty: 1, remaining_qty: 2, remaining: 'later' } });
  assert.ok(later.startsWith('🕒 ピッキング欠品 — 後で取りに行きます'), later);
  assert.ok(later.includes('残り 2個 → 後で取りに行きます') && later.includes('📍 他ロケ在庫: なし'), later);
  const none = buildShortageText({ ...INFO, shortageQty: 1, line: { ...INFO.line, remaining_qty: 1, remaining: 'none' } });
  assert.ok(none.startsWith('❌ ピッキング欠品 — どのロケにも在庫がありません'), none);
  assert.ok(none.includes('指定ロケで不足 1個 / 指示 3個 (2個は確保済み)'), none);
});

t('buildShortageText: バッチ/ロケ/商品/商品コード/数量/作業者が入る (一部欠品は確保数も)', () => {
  const text = buildShortageText(INFO);
  for (const s of ['出荷_03', 'P3FB-001-003-03', '商品コード: teatree10', '欠品 2個 / 指示 3個', '(1個は確保済み)', '星']) {
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
  // 🚨 回帰テスト (2026-09-02): LINEトークンがあっても、スイッチ off なら LINE には送らない。
  // トークンは在庫検索ボット (line-search.js) と共用のため env から消せない。
  // 「トークンがあるから送る」に戻すと従量課金が再開する
  process.env.PICKING_LINE_CHANNEL_TOKEN = 'test-token';
  process.env.PICKING_LINE_TO = 'Uaaa, Cbbb';
  delete process.env.PICKING_SHORTAGE_LINE;
  calls.length = 0;
  const r = await notifyShortage(INFO);
  assert.equal(r, 'disabled', 'LINEトークンだけでは通知経路にならない');
  assert.equal(calls.length, 0, 'LINE APIを一度も叩かない');
  console.log('  ok: LINEトークンがあってもスイッチoffなら送らない (async)');
}

{
  // 通常運用: GChat のみ (LINEトークンは残っているがスイッチ off)
  process.env.PICKING_ALERT_WEBHOOK = 'https://chat.example/webhook';
  calls.length = 0;
  const r = await notifyShortage(INFO);
  assert.equal(r, 'sent');
  assert.equal(calls.length, 1);
  assert.equal(calls[0].url, 'https://chat.example/webhook');
  assert.ok(!calls.some((c) => c.url.includes('api.line.me')), 'LINEには送らない');
  console.log('  ok: 通常運用はGChatのみ (async)');
}

{
  // LINE broadcast (スイッチ on・PICKING_LINE_TO 無し)
  process.env.PICKING_SHORTAGE_LINE = 'on';
  delete process.env.PICKING_LINE_TO;
  delete process.env.PICKING_ALERT_WEBHOOK;
  calls.length = 0;
  const r = await notifyShortage(INFO);
  assert.equal(r, 'sent');
  assert.equal(calls.length, 1);
  assert.ok(calls[0].url.endsWith('/broadcast'));
  assert.equal(calls[0].auth, 'Bearer test-token');
  assert.equal(calls[0].body.messages[0].type, 'text');
  console.log('  ok: LINE broadcast (スイッチon・宛先未指定) (async)');
}

{
  // LINE push (宛先2件) + GChat 併用 (スイッチ on のとき)
  process.env.PICKING_LINE_TO = 'Uaaa, Cbbb';
  process.env.PICKING_ALERT_WEBHOOK = 'https://chat.example/webhook';
  calls.length = 0;
  const r = await notifyShortage(INFO);
  assert.equal(r, 'sent');
  const pushes = calls.filter((c) => c.url.endsWith('/push'));
  assert.equal(pushes.length, 2);
  assert.deepEqual(pushes.map((c) => c.body.to), ['Uaaa', 'Cbbb']);
  assert.ok(calls.some((c) => c.url === 'https://chat.example/webhook'));
  console.log('  ok: LINE push (複数宛先) + GChat 併用 (スイッチon) (async)');
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
  failUrls = new Set();
  delete process.env.PICKING_SHORTAGE_LINE;   // 以降のテストは既定 (LINE off) に戻す
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
  // エンドツーエンド: notifyShortage が土曜に休日グループへpushする (LINE再開時の挙動)
  process.env.PICKING_LINE_TO_HOLIDAY = 'Choliday';
  process.env.PICKING_SHORTAGE_LINE = 'on';
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
  delete process.env.PICKING_SHORTAGE_LINE;
  process.env.PICKING_LINE_TO = 'Uaaa, Cbbb';
}

// ─── webhook 署名検証 (groupId取得+在庫検索イベント供給) ───
{
  const { handleLineWebhook } = await import('../notify.js');
  const cryptoMod = await import('node:crypto');
  const body = Buffer.from(JSON.stringify({ events: [{ type: 'join', source: { type: 'group', groupId: 'Cxxxx' } }] }));
  // secret未設定 → fail-closed (null = 403)
  delete process.env.PICKING_LINE_CHANNEL_SECRET;
  assert.equal(handleLineWebhook(body, 'sig'), null);
  // 正しい署名 → イベント配列 / 改ざん → null
  process.env.PICKING_LINE_CHANNEL_SECRET = 'test-secret';
  const sig = cryptoMod.createHmac('sha256', 'test-secret').update(body).digest('base64');
  const events = handleLineWebhook(body, sig);
  assert.ok(Array.isArray(events) && events.length === 1 && events[0].type === 'join', '検証OKはイベント配列を返す');
  assert.equal(handleLineWebhook(body, sig.slice(0, -2) + 'xx'), null);
  assert.equal(handleLineWebhook(Buffer.from('tampered'), sig), null);
  // 署名一致だがbodyが壊れている → 空配列 (200は返すが何もしない)
  const broken = Buffer.from('not-json');
  const brokenSig = cryptoMod.createHmac('sha256', 'test-secret').update(broken).digest('base64');
  assert.deepEqual(handleLineWebhook(broken, brokenSig), []);
  console.log('  ok: LINE webhook 署名検証 (secret必須・改ざん拒否・イベント配列返却) (async)');
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

// ─── 他ロケ在庫 (stock-locations.js) ───
{
  const { buildStockLocationsText, normalizeLocationDigits, fetchStockLocations, stockLookupConfigured } = await import('../stock-locations.js');
  t('normalizeLocationDigits: 区切りあり/なし8桁を同一視', () => {
    assert.equal(normalizeLocationDigits('001-003-03'), '00100303');
    assert.equal(normalizeLocationDigits('00100303'), '00100303');
    assert.equal(normalizeLocationDigits(''), '');
  });

  const data = {
    ok: true,
    importedAt: '2026-08-16T03:03:00.000Z',   // JST 12:03
    stockDate: '20260816',
    locations: [
      { block: 'P3FB', location: '001-003-03', quality: '良品', qty: 3, allocated: 0, free: 3 },      // 報告ロケ → 除外
      { block: 'R1FA', location: '001-001-01', quality: '良品', qty: 200, allocated: 0, free: 200 },
      { block: 'R1FA', location: '002-002-01', quality: '良品', qty: 160, allocated: 10, free: 150, expiry: '20280115' },
      { block: 'P1FB', location: '001-004-02', quality: '不良品', qty: 50, allocated: 0, free: 50 },   // 良品以外 → 出さない
      { block: 'P1FB', location: '001-009-01', quality: '良品', qty: 5, allocated: 5, free: 0 },       // フリー0 → 出さない
      { block: 'ZZZ', location: 'ZZZ-ZZZ-ZZ', quality: '良品', qty: 1000, allocated: 0, free: 1000 },  // 仮想ロケ → 合算行のみ
    ],
  };
  const now = new Date('2026-08-16T03:30:00Z');   // JST 12:30 (取込27分後)

  t('buildStockLocationsText: 良品のみ・棚ロケ先頭フリー降順・報告ロケ除外・特殊ロケは実名で末尾', () => {
    const text = buildStockLocationsText(data, { excludeBlock: 'P3FB', excludeLocation: '00100303', now });
    const lines = text.split('\n');
    // 「12:03」形式はLINEが時刻リンク化するので「12時03分」表記 (区切りも「→」でコロン不使用)
    assert.ok(lines[0].startsWith('📍 他ロケ在庫 (12時03分時点)'), text);
    assert.equal(lines[1], '・R1FA-001-001-01 → 200個', '期限なしロットは補足なし');
    assert.equal(lines[2], '・R1FA-002-002-01 → 150個 (期限2028/01/15・別途引当10)', '期限は補足括弧にまとめる');
    assert.equal(lines[3], '・ZZZ-ZZZ-ZZ → 1000個', '特殊ロケは実際のロケコードのまま末尾に表示');
    assert.equal(lines.length, 4, `不良品/フリー0/報告ロケは出ないはず:\n${text}`);
    assert.ok(!text.includes('⚠'), '新鮮なら警告なし');
    assert.ok(!/\d: ?\d/.test(text), '「数字:数字」を含まない (LINE時刻リンク化対策)');
  });

  t('buildStockLocationsText: ロケは丸めず全部表示 (「…他Nロケ」を出さない)', () => {
    const many = {
      ok: true, importedAt: data.importedAt, stockDate: '20260816',
      locations: Array.from({ length: 15 }, (_, i) => ({
        block: 'R1FA', location: `00${String(i + 1).padStart(2, '0')}-001-01`, quality: '良品',
        qty: 10 + i, allocated: 0, free: 10 + i,
      })),
    };
    const text = buildStockLocationsText(many, { now });
    assert.equal(text.split('\n').length, 16, `見出し+15ロケ全部:\n${text}`);
    assert.ok(!text.includes('…他'), '丸め表示を出さない');
  });

  t('buildStockLocationsText: 同ロケの期限違いロットは隣接して期限の近い順 (先入先出)', () => {
    const lots = {
      ok: true, importedAt: data.importedAt, stockDate: '20260816',
      locations: [
        // ロケA (合計150) のロットは数量がバラバラでも隣接し、期限昇順。ロケB (70) はその後
        { block: 'R1FA', location: '001-001-01', quality: '良品', qty: 100, allocated: 0, free: 100, expiry: '20280115' },
        { block: 'R1FA', location: '002-002-02', quality: '良品', qty: 70, allocated: 0, free: 70, expiry: '20270601' },
        { block: 'R1FA', location: '001-001-01', quality: '良品', qty: 50, allocated: 0, free: 50, expiry: '20271210' },
      ],
    };
    const lines = buildStockLocationsText(lots, { now }).split('\n');
    assert.equal(lines[1], '・R1FA-001-001-01 → 50個 (期限2027/12/10)');
    assert.equal(lines[2], '・R1FA-001-001-01 → 100個 (期限2028/01/15)');
    assert.equal(lines[3], '・R1FA-002-002-02 → 70個 (期限2027/06/01)');
  });

  t('buildStockLocationsText: 5,000字を超えそうなときだけ末尾を間引く (安全弁)', () => {
    const huge = {
      ok: true, importedAt: data.importedAt, stockDate: '20260816',
      locations: Array.from({ length: 60 }, (_, i) => ({
        block: 'X', location: `特殊ロケの長い名前テスト-${'あ'.repeat(80)}-${i}`, quality: '良品',
        qty: 1, allocated: 0, free: 1,
      })),
    };
    const text = buildStockLocationsText(huge, { now });
    assert.ok(text.length <= 5000, `LINE上限内: ${text.length}`);
    assert.ok(text.includes('文字数上限'), '間引いたことを明示');
  });

  t('buildStockLocationsText: 180分超のスナップショットは古い旨の警告', () => {
    const later = new Date('2026-08-16T07:00:00Z');   // 取込から約4時間後
    const text = buildStockLocationsText(data, { now: later });
    assert.ok(text.includes('⚠古い可能性'), text);
  });

  t('buildStockLocationsText: 「なし」と「取得できず」は別表示 (0件と取得不能を混ぜない)', () => {
    const none = buildStockLocationsText({ ok: true, importedAt: data.importedAt, locations: [] }, { now });
    assert.ok(none.includes('時点): なし'), none);
    assert.equal(buildStockLocationsText(null, { now }), '📍 他ロケ在庫: 取得できず');
  });

  t('stockLookupConfigured: env未設定なら false (通知に在庫行を出さない)', () => {
    assert.equal(stockLookupConfigured(), false);
  });

  // fetchStockLocations: fail-soft と SKU 正規化
  process.env.WAREHOUSE_URL = 'http://wh.local';
  process.env.WAREHOUSE_SERVICE_TOKEN = 'tkn';
  assert.equal(await fetchStockLocations('abc', async () => ({ ok: false, status: 500 })), null, 'HTTPエラーは null');
  assert.equal(await fetchStockLocations('abc', async () => { throw new Error('timeout'); }), null, '例外も null');
  const got = await fetchStockLocations(' ABC ', async (url, opts) => {
    assert.ok(String(url).startsWith('http://wh.local/service-api/logizard-stock/locations?code=abc'), url);
    assert.equal(opts.headers.Authorization, 'Bearer tkn');
    return { ok: true, status: 200, json: async () => ({ ok: true, importedAt: null, stockDate: null, locations: [] }) };
  });
  assert.ok(got && Array.isArray(got.locations), 'trim+小文字で照会して結果を返す');
  delete process.env.WAREHOUSE_URL;
  delete process.env.WAREHOUSE_SERVICE_TOKEN;
  console.log('  ok: fetchStockLocations fail-soft + SKU正規化 (async)');
}

// ─── e2e: 欠品通知の本文に他ロケ在庫が載る ───
{
  process.env.WAREHOUSE_URL = 'http://wh.local';
  process.env.WAREHOUSE_SERVICE_TOKEN = 'tkn';
  // 実運用と同じ経路 (GChatのみ) で本文を検証する
  process.env.PICKING_ALERT_WEBHOOK = 'https://chat.example/webhook';
  delete process.env.PICKING_SHORTAGE_LINE;
  const sent = [];
  globalThis.fetch = async (url, opts) => {
    if (String(url).startsWith('http://wh.local/service-api/logizard-stock/locations')) {
      return {
        ok: true, status: 200,
        json: async () => ({
          ok: true, importedAt: new Date().toISOString(), stockDate: '20260816',
          locations: [{ block: 'R1FA', location: '001-001-01', quality: '良品', qty: 200, allocated: 0, free: 200 }],
        }),
      };
    }
    sent.push(JSON.parse(opts.body));
    return { ok: true, status: 200, text: async () => '' };
  };
  await notifyShortage(INFO);
  const text = sent[0].text;
  assert.ok(text.includes('📍 他ロケ在庫'), text);
  assert.ok(text.includes('・R1FA-001-001-01 → 200個'), text);
  // warehouse 側が落ちていても通知は出る (取得できず表示)
  sent.length = 0;
  globalThis.fetch = async (url, opts) => {
    if (String(url).startsWith('http://wh.local/')) return { ok: false, status: 503, text: async () => '' };
    sent.push(JSON.parse(opts.body));
    return { ok: true, status: 200, text: async () => '' };
  };
  await notifyShortage(INFO);
  assert.ok(sent[0].text.includes('他ロケ在庫: 取得できず'), sent[0].text);
  delete process.env.WAREHOUSE_URL;
  delete process.env.WAREHOUSE_SERVICE_TOKEN;
  console.log('  ok: 欠品通知本文に他ロケ在庫が載る / warehouse停止でも通知は出る (async)');
}

console.log(`\ntest-notify: ${passed + 14} 件 pass`);
