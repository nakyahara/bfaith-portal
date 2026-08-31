/**
 * mf-api.js の突合ロジック+トークン保存のスモークテスト
 * 実行: node apps/shohyo-links/tests/test-mf-match.mjs
 */
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'shohyo-mf-test-'));

const { normalizeText, vendorKeys, journalTexts, matchVendors, journalDigest, loadTokens, saveTokens, clearTokens } =
  await import('../mf-api.js');

let failed = 0;
function check(label, cond) {
  console.log(`${cond ? 'OK ' : 'NG '} ${label}`);
  if (!cond) failed++;
}

// normalizeText: 全角→半角・大文字化・空白除去
check('全角英数の正規化', normalizeText('ＤＰ４＊ＲＡＫＳＵＬ') === 'DP4*RAKSUL');
check('半角カナはNFKCで全角カナへ寄る', normalizeText('ﾗｸｽﾙ') === normalizeText('ラクスル'));
check('空白・中黒の除去', normalizeText('ラクスル ・カブシキ') === normalizeText('ラクスルカブシキ'));

// vendorKeys: 括弧の内外がキーになる
const keys = vendorKeys('ﾗｸｽﾙ（DP4*RAKSUL）');
check('括弧内 (明細コード) がキーに含まれる', keys.includes(normalizeText('DP4*RAKSUL')));
check('括弧外 (社名) がキーに含まれる', keys.includes(normalizeText('ﾗｸｽﾙ')));
check('丸数字プレフィックスの除去', vendorKeys('⑪Amazon【梱包資材】5422').some(k => k.startsWith(normalizeText('AMAZON'))));

// journalTexts: name系フィールドを再帰収集し日付・IDは拾わない
const journal = {
  id: 'xxx', transaction_date: '2026-08-15', memo: 'ラクスル印刷代',
  details: [{ debitor: { account_name: '消耗品費', trade_partner_name: 'ﾗｸｽﾙ ﾃﾞｨ-ﾋﾟ-4*RAKSUL', value: 11000 } }],
};
const texts = journalTexts(journal);
check('memoを収集する', texts.includes('ラクスル印刷代'));
check('trade_partner_nameを収集する', texts.some(t => t.includes('RAKSUL')));
check('日付は収集しない', !texts.includes('2026-08-15'));

// matchVendors
const vendors = [
  { id: 1, name: 'ﾗｸｽﾙ（DP4*RAKSUL）' },
  { id: 2, name: 'ｱｽｸﾙ（DP11 ASKUL B2B）' },
  { id: 3, name: 'FONDESK MONTHLY CHARGE' },
];
const m1 = matchVendors(journal, vendors);
check('明細コードで一致する', m1.length === 1 && m1[0].id === 1);
const m2 = matchVendors({ memo: 'FONDESK 8月分' }, vendors);
check('英字社名の部分一致', m2.length === 1 && m2[0].id === 3);
check('一致なしは空配列', matchVendors({ memo: '西濃運輸' }, vendors).length === 0);

// トークン保存
// journalDigest (MF公式サンプルの branches 形状)
const j2 = {
  branches: [
    { remark: '8月分', debitor: { value: 11000, account_name: '消耗品費', trade_partner_name: 'ラクスル株式会社' },
      creditor: { value: 11000, account_name: '未払金', sub_account_name: 'UPSIDER' } },
    { remark: '', debitor: { value: 3300, account_name: '消耗品費' },
      creditor: { value: 3300, account_name: '未払金' } },
  ],
};
const d2 = journalDigest(j2);
check('借方の合計を金額にする', d2.amount === 14300);
check('勘定科目を重複なく集める', d2.accounts.join('/') === '消耗品費/未払金/UPSIDER');
check('取引先を分けて返す', d2.partners.join('/') === 'ラクスル株式会社');
check('空の摘要は入れない', d2.remarks.join('/') === '8月分');
const d3 = journalDigest({ memo: 'x', value: 500, account_name: '通信費' });
check('branchesが無い形でも拾う (保険)', d3.accounts.includes('通信費') && d3.amount === 500);

check('初期状態は未接続', loadTokens() === null);
saveTokens({ access_token: 'a', refresh_token: 'r', expires_at: 123 });
check('保存→復元', loadTokens()?.refresh_token === 'r');
saveTokens({ access_token: 'b', refresh_token: 'r2', expires_at: 456 });
check('上書き保存 (1行維持)', loadTokens()?.access_token === 'b');
clearTokens();
check('クリアで未接続に戻る', loadTokens() === null);

// ---- 会計期間またぎ (2026-08-30 実機の mf_api_400 を再現) ----
const { splitByAccountingPeriods, getJournals, getJournalsByTransactionIds, resetAccountingPeriodsCache, mfErrorText } =
  await import('../mf-api.js');
const FY = [ // MFは開始日の降順で返す (決算期 7/1〜6/30)
  { start_date: '2026-07-01', end_date: '2027-06-30', fiscal_year: 2026 },
  { start_date: '2025-07-01', end_date: '2026-06-30', fiscal_year: 2025 },
];
const eq = (a, b) => JSON.stringify(a) === JSON.stringify(b);
check('split: 会計期間が無ければ分割しない (従来どおり1本)', eq(splitByAccountingPeriods('2026-05-14', '2026-08-31', []), [['2026-05-14', '2026-08-31']]));
check('split: 期の中に収まる期間はそのまま', eq(splitByAccountingPeriods('2026-08-01', '2026-08-31', FY), [['2026-08-01', '2026-08-31']]));
check('split: 6/30-7/1 をまたぐと期ごとに2本 (昇順)', eq(splitByAccountingPeriods('2026-05-14', '2026-08-31', FY), [['2026-05-14', '2026-06-30'], ['2026-07-01', '2026-08-31']]));
check('split: 期の境界1日だけ (7/1〜7/1)', eq(splitByAccountingPeriods('2026-07-01', '2026-07-01', FY), [['2026-07-01', '2026-07-01']]));
check('split: 最新期より先の日は落とす', eq(splitByAccountingPeriods('2027-06-01', '2027-07-15', FY), [['2027-06-01', '2027-06-30']]));
check('split: どの期にも無い期間は空', eq(splitByAccountingPeriods('2020-01-01', '2020-01-31', FY), []));
check('split: 3期またぎ', eq(
  splitByAccountingPeriods('2025-06-01', '2026-07-31', [...FY, { start_date: '2024-07-01', end_date: '2025-06-30', fiscal_year: 2024 }]),
  [['2025-06-01', '2025-06-30'], ['2025-07-01', '2026-06-30'], ['2026-07-01', '2026-07-31']]));

// fetch を差し替えて「期をまたぐと400」な MF を再現し、getJournals が期ごとに叩いて結合することを確認 (MF本体は叩かない)
saveTokens({ access_token: 't', refresh_token: 'r', expires_at: Date.now() + 3600_000 });
resetAccountingPeriodsCache();
const calls = [];
const realFetch = globalThis.fetch;
const inPeriod = (d) => FY.find(p => d >= p.start_date && d <= p.end_date) || null;
const MF_400 = { errors: [{ code: 'invalid_query_parameter_value', message: "Accounting period doesn't exist for the fiscal year." }] };
globalThis.fetch = async (url) => {
  const u = new URL(url);
  calls.push(u.pathname + u.search);
  const json = (status, body) => new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } });
  if (u.pathname === '/api/v3/offices') return json(200, { name: 'テスト事業者', accounting_periods: FY });
  if (u.pathname === '/api/v3/journals') {
    const s = u.searchParams.get('start_date'), e = u.searchParams.get('end_date');
    if (!inPeriod(s) || inPeriod(s) !== inPeriod(e)) return json(400, MF_400);
    const txIds = u.searchParams.getAll('transaction_ids');
    const all = [
      { id: 'j-may', transaction_date: '2026-05-21', transaction_id: 'tx-may' },
      { id: 'j-aug', transaction_date: '2026-08-14', transaction_id: 'tx-aug' },
    ];
    return json(200, { journals: all.filter(j => j.transaction_date >= s && j.transaction_date <= e && (!txIds.length || txIds.includes(j.transaction_id))) });
  }
  return json(404, {});
};
try {
  const r400 = await globalThis.fetch('https://mf.test/api/v3/journals?start_date=2026-05-14&end_date=2026-08-31');
  check('(再現) 期をまたぐ1本叩きは MF が 400 を返す', r400.status === 400);
  const errText = mfErrorText(Object.assign(new Error('mf_api_400'), { detail: await r400.json() }));
  check('mfErrorText: MFのエラー本文を1行にする', errText.includes('invalid_query_parameter_value') && errText.includes('Accounting period'));
  check('mfErrorText: detail が無ければ空', mfErrorText(new Error('x')) === '');
  calls.length = 0; // ここまでは再現用の直叩き (getJournals の呼び出し回数に混ぜない)

  const js = await getJournals('2026-05-14', '2026-08-31');
  check('getJournals: 期をまたぐ期間でも 400 にならず両期の仕訳が揃う', js.map(j => j.id).sort().join(',') === 'j-aug,j-may');
  const jr = calls.filter(c => c.startsWith('/api/v3/journals'));
  check('getJournals: 仕訳APIは期ごとに叩く (6/30まで と 7/1から)', jr.length === 2 && jr.some(c => c.includes('end_date=2026-06-30')) && jr.some(c => c.includes('start_date=2026-07-01')));
  check('会計期間は offices から1回だけ取る', calls.filter(c => c.startsWith('/api/v3/offices')).length === 1);
  const byTx = await getJournalsByTransactionIds(['tx-may', 'tx-aug'], '2026-05-14', '2026-08-31');
  check('getJournalsByTransactionIds: 期ごとに引いて結合 (重複なし)', byTx.map(j => j.id).sort().join(',') === 'j-aug,j-may');
  check('会計期間のキャッシュが効く (offices は増えない)', calls.filter(c => c.startsWith('/api/v3/offices')).length === 1);
  const inOne = await getJournals('2026-08-01', '2026-08-31');
  check('getJournals: 期内なら従来どおり1本', inOne.length === 1 && inOne[0].id === 'j-aug');
} finally {
  globalThis.fetch = realFetch;
  clearTokens();
}

console.log(failed ? `\n${failed} 件失敗` : '\n全件パス');
process.exit(failed ? 1 : 0);
