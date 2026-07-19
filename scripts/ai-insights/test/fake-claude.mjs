// smoke 用の偽 claude CLI。stdin を読み切ってから FAKE_MODE に応じた出力を返す
// FAKE_MODE: ok (正常JSON) / bad (壊れた出力) / fail (exit 1)
//            prohibited (禁止モールに言及) / invented (factsに無い数字を創作)
let stdin = '';
process.stdin.setEncoding('utf8');
for await (const chunk of process.stdin) stdin += chunk;

const mode = process.env.FAKE_MODE || 'ok';
if (mode === 'fail') {
  process.stderr.write('fake claude: simulated failure\n');
  process.exit(1);
}
if (mode === 'bad') {
  process.stdout.write('ここにJSONはありません\n');
  process.exit(0);
}
if (mode === 'prohibited' || mode === 'invented') {
  const badTopic = mode === 'prohibited'
    ? { title: 'amazonの売上が急減', category: 'sales', evidence: '今週0円', action: '確認', confidence: 'med' }
    : { title: '楽天が好調', category: 'sales', evidence: '今週98,765円と大幅増', action: '確認', confidence: 'med' };
  process.stdout.write(JSON.stringify({
    type: 'result', subtype: 'success', is_error: false,
    result: JSON.stringify({ summary: 'テスト', topics: [badTopic], topic_updates: [], data_notes: '' }),
  }));
  process.exit(0);
}

// プロンプトに前回論点の topic_id が含まれていれば topic_updates に使う (フィルタ検証用)
if (mode === 'badsummary') {
  // topics は正常だが summary が禁止された全社集計に言及 → summary 検査で棄却されるべき
  process.stdout.write(JSON.stringify({
    type: 'result', subtype: 'success', is_error: false,
    result: JSON.stringify({
      summary: '全社売上は15,500円と好調でした。',
      topics: [], topic_updates: [], data_notes: '',
    }),
  }));
  process.exit(0);
}

const topicIdMatch = stdin.match(/topic_id=(tp_[a-zA-Z0-9_]+)/);
const result = {
  summary: '楽天が前週比+50%と好調でした。詳細は論点参照。',
  topics: [
    {
      title: '楽天売上が前週比+50%',
      category: 'sales',
      evidence: '今週1.5万円（前週1.0万円）',
      hypothesis: 'セール参加の影響の可能性 (要確認)',
      next_check: '楽天分析ダッシュボードで商品別内訳',
      action: '好調SKUの在庫日数を確認し補充判断',
      owner: '中原さん',
      deadline: '金曜まで',
      confidence: 'med',
    },
  ],
  topic_updates: [
    ...(topicIdMatch ? [{ topic_id: topicIdMatch[1], status: 'carried' }] : []),
    { topic_id: 'tp_unknown_should_be_filtered', status: 'resolved' },
  ],
  data_notes: 'amazon 売上は取込欠損疑いのため今週は評価していません。',
};
process.stdout.write(JSON.stringify({
  type: 'result', subtype: 'success', is_error: false,
  result: `${JSON.stringify(result)}`,
}));
