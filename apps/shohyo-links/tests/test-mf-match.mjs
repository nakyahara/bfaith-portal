/**
 * mf-api.js の突合ロジック+トークン保存のスモークテスト
 * 実行: node apps/shohyo-links/tests/test-mf-match.mjs
 */
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'shohyo-mf-test-'));

const { normalizeText, vendorKeys, journalTexts, matchVendors, loadTokens, saveTokens, clearTokens } =
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
check('初期状態は未接続', loadTokens() === null);
saveTokens({ access_token: 'a', refresh_token: 'r', expires_at: 123 });
check('保存→復元', loadTokens()?.refresh_token === 'r');
saveTokens({ access_token: 'b', refresh_token: 'r2', expires_at: 456 });
check('上書き保存 (1行維持)', loadTokens()?.access_token === 'b');
clearTokens();
check('クリアで未接続に戻る', loadTokens() === null);

console.log(failed ? `\n${failed} 件失敗` : '\n全件パス');
process.exit(failed ? 1 : 0);
