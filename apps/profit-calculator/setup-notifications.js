/**
 * SP-API通知セットアップスクリプト（1回だけ実行）
 *
 * 実行手順:
 *   1. 環境変数を設定（.envまたは直接export）
 *   2. node apps/profit-calculator/setup-notifications.js
 *
 * 必要な環境変数:
 *   AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY（SP-API用と同じ）
 *   SP_API_CLIENT_ID, SP_API_CLIENT_SECRET, SP_API_REFRESH_TOKEN
 *   AWS_SQS_REGION (デフォルト: ap-northeast-1)
 */
import { SQSClient, CreateQueueCommand, GetQueueAttributesCommand, SetQueueAttributesCommand, GetQueueUrlCommand } from '@aws-sdk/client-sqs';
import { STSClient, GetCallerIdentityCommand } from '@aws-sdk/client-sts';
import SellingPartner from 'amazon-sp-api';

const REGION = process.env.AWS_SQS_REGION || 'ap-northeast-1';
const QUEUE_NAME = 'bfaith-sp-api-notifications';
const MARKETPLACE_ID = 'A1VC38T7YXB528'; // Amazon.co.jp
const SP_API_AWS_ACCOUNT = '437568002678'; // AmazonのSP-API用AWSアカウント

async function setup() {
  console.log('=== SP-API通知セットアップ開始 ===\n');

  // ── Step 1: AWSアカウントID取得 ──
  console.log('[Step 1] AWSアカウントID確認...');
  const sts = new STSClient({ region: REGION });
  const identity = await sts.send(new GetCallerIdentityCommand({}));
  const accountId = identity.Account;
  console.log(`  アカウントID: ${accountId}\n`);

  // ── Step 2: SQSキュー作成 ──
  console.log('[Step 2] SQSキュー作成...');
  const sqs = new SQSClient({ region: REGION });

  let queueUrl;
  try {
    // 既存キュー確認
    const existing = await sqs.send(new GetQueueUrlCommand({ QueueName: QUEUE_NAME }));
    queueUrl = existing.QueueUrl;
    console.log(`  既存キュー検出: ${queueUrl}`);
  } catch {
    // 新規作成
    const createResult = await sqs.send(new CreateQueueCommand({
      QueueName: QUEUE_NAME,
      Attributes: {
        ReceiveMessageWaitTimeSeconds: '20',    // ロングポーリング
        MessageRetentionPeriod: '345600',       // 4日間保持
        VisibilityTimeout: '60',                // 処理中は60秒非表示
      },
    }));
    queueUrl = createResult.QueueUrl;
    console.log(`  キュー作成完了: ${queueUrl}`);
  }

  // キューARN取得
  const attrs = await sqs.send(new GetQueueAttributesCommand({
    QueueUrl: queueUrl,
    AttributeNames: ['QueueArn'],
  }));
  const queueArn = attrs.Attributes.QueueArn;
  console.log(`  ARN: ${queueArn}\n`);

  // ── Step 3: アクセスポリシー設定 ──
  console.log('[Step 3] SQSアクセスポリシー設定...');
  const policy = {
    Version: '2012-10-17',
    Statement: [
      {
        Effect: 'Allow',
        Principal: { AWS: `arn:aws:iam::${SP_API_AWS_ACCOUNT}:root` },
        Action: ['sqs:SendMessage', 'sqs:GetQueueAttributes'],
        Resource: queueArn,
      },
    ],
  };

  await sqs.send(new SetQueueAttributesCommand({
    QueueUrl: queueUrl,
    Attributes: { Policy: JSON.stringify(policy) },
  }));
  console.log('  ポリシー設定完了（Amazon SP-APIからの書き込みを許可）\n');

  // ── Step 4: SP-API Destination作成 ──
  console.log('[Step 4] SP-API Destination作成...');
  const spGrantless = new SellingPartner({
    region: 'fe',
    options: { only_grantless_operations: true },
    credentials: {
      SELLING_PARTNER_APP_CLIENT_ID: process.env.SP_API_CLIENT_ID,
      SELLING_PARTNER_APP_CLIENT_SECRET: process.env.SP_API_CLIENT_SECRET,
      AWS_ACCESS_KEY_ID: process.env.AWS_ACCESS_KEY_ID,
      AWS_SECRET_ACCESS_KEY: process.env.AWS_SECRET_ACCESS_KEY,
    },
  });

  let destinationId;
  try {
    // 既存Destination確認
    const destinations = await spGrantless.callAPI({
      operation: 'getDestinations',
      endpoint: 'notifications',
    });
    const existing = destinations.find(d => d.resource?.sqs?.arn === queueArn);
    if (existing) {
      destinationId = existing.destinationId;
      console.log(`  既存Destination検出: ${destinationId}`);
    }
  } catch (e) {
    console.log('  既存Destination確認スキップ:', e.message);
  }

  if (!destinationId) {
    const destResult = await spGrantless.callAPI({
      operation: 'createDestination',
      endpoint: 'notifications',
      body: {
        name: 'BFaithPriceRevision',
        resourceSpecification: {
          sqs: { arn: queueArn },
        },
      },
    });
    destinationId = destResult.destinationId;
    console.log(`  Destination作成完了: ${destinationId}`);
  }
  console.log();

  // ── Step 5: Subscription作成 ──
  console.log('[Step 5] ANY_OFFER_CHANGED Subscription作成...');
  const sp = new SellingPartner({
    region: 'fe',
    refresh_token: process.env.SP_API_REFRESH_TOKEN,
    credentials: {
      SELLING_PARTNER_APP_CLIENT_ID: process.env.SP_API_CLIENT_ID,
      SELLING_PARTNER_APP_CLIENT_SECRET: process.env.SP_API_CLIENT_SECRET,
      AWS_ACCESS_KEY_ID: process.env.AWS_ACCESS_KEY_ID,
      AWS_SECRET_ACCESS_KEY: process.env.AWS_SECRET_ACCESS_KEY,
    },
  });

  try {
    // 既存Subscription確認
    const existingSub = await sp.callAPI({
      operation: 'getSubscription',
      endpoint: 'notifications',
      path: { notificationType: 'ANY_OFFER_CHANGED' },
    });
    console.log(`  既存Subscription検出: ${existingSub.subscriptionId}`);
    console.log(`  Destination: ${existingSub.destinationId}`);
  } catch {
    // 新規作成
    const subResult = await sp.callAPI({
      operation: 'createSubscription',
      endpoint: 'notifications',
      path: { notificationType: 'ANY_OFFER_CHANGED' },
      body: {
        payloadVersion: '1.0',
        destinationId,
      },
    });
    console.log(`  Subscription作成完了: ${subResult.subscriptionId}`);
  }

  // ── 完了 ──
  console.log('\n=== セットアップ完了 ===\n');
  console.log('Renderに以下の環境変数を追加してください:');
  console.log(`  AWS_SQS_QUEUE_URL = ${queueUrl}`);
  console.log(`  AWS_SQS_REGION    = ${REGION}`);
  console.log('\n通知が届くまで数分かかる場合があります。');
}

setup().catch(err => {
  console.error('\nセットアップ失敗:', err.message);
  if (err.code) console.error('  エラーコード:', err.code);
  process.exit(1);
});
