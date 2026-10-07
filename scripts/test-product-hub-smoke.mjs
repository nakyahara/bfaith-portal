/**
 * product-hub の smoke を `npm run test:product-hub` から回すためのラッパー。
 *
 * 🚨 なぜ要るか: smoke は EJS を**実際に描画**するので、router が渡す locals を
 *    テンプレートに足して smoke 側に足し忘れると、そこで初めて分かる。
 *    ところが smoke は test:product-hub に入っていなかったため、
 *    **PR1-d / PR1-e で `lpCompose` / `lpSpec` を足したときに 123 件落ちたまま master に乗った**
 *    (本番は router が必ず渡すので画面は動く。壊れていたのはテストの方)。
 *    PR の流れで必ず回るようにして、同じ落とし方を塞ぐ。
 *
 * smoke は本番DBを触らないよう DATA_DIR を必須にしているので、使い捨ての置き場を作って渡す。
 */
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const SMOKE = path.join(HERE, '..', 'apps', 'product-hub', 'scripts', 'smoke.mjs');

const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'test-ph-smoke-'));
let code = 1;
try {
  code = await new Promise((resolve) => {
    const child = spawn(process.execPath, [SMOKE], {
      stdio: 'inherit',
      env: { ...process.env, DATA_DIR: dir },
    });
    child.on('close', (c) => resolve(c ?? 1));
  });
} finally {
  // 失敗しても置き場は消す (Windows はハンドルが残ることがあるので握りつぶす)
  await fs.rm(dir, { recursive: true, force: true }).catch(() => {});
}
process.exit(code);
