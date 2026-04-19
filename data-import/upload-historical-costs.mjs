/**
 * 過去の運賃・資材費データを Render にアップロード
 *
 * 使い方:
 *   MIRROR_SYNC_KEY=xxx node data-import/upload-historical-costs.mjs [--url=...]
 *
 * 既定のURL: https://bfaith-portal.onrender.com
 */
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const urlArg = process.argv.find(a => a.startsWith('--url='));
const BASE = urlArg ? urlArg.slice(6) : 'https://bfaith-portal.onrender.com';
const SYNC_KEY = process.env.MIRROR_SYNC_KEY;

if (!SYNC_KEY) {
  console.error('ERROR: MIRROR_SYNC_KEY環境変数を設定してください');
  process.exit(1);
}

const data = JSON.parse(fs.readFileSync(path.join(__dirname, 'historical-costs.json'), 'utf-8'));
console.log(`Freight: ${data.freight.length}件, Material: ${data.material.length}件`);
console.log(`POST ${BASE}/apps/mgmt-accounting/import-historical`);

const res = await fetch(`${BASE}/apps/mgmt-accounting/import-historical`, {
  method: 'POST',
  headers: { 'Content-Type': 'application/json', 'X-Sync-Key': SYNC_KEY },
  body: JSON.stringify(data),
});

const body = await res.text();
console.log(`HTTP ${res.status}`);
console.log(body);
