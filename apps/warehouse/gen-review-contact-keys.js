#!/usr/bin/env node
/**
 * gen-review-contact-keys.js — contacts暗号鍵の初期生成 (mall-csv-fetcher P2 PR-B)
 *
 * リポジトリ直下 .env に CONTACTS_ENC_KEY / CONTACTS_HMAC_KEY (各32byte hex) を追記する。
 * - 既に存在する場合は何もしない (鍵ローテはこのツールの対象外 — 既存暗号文が読めなくなるため)
 * - ★鍵の値は画面・ログに一切出力しない (feedback_secret_hygiene)
 *
 * 実行 (miniPC、リポジトリ直下で): node apps/warehouse/gen-review-contact-keys.js
 */
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const envPath = path.resolve(__dirname, '..', '..', '.env');

if (!fs.existsSync(envPath)) {
  console.error(`FATAL: .env が見つかりません (${envPath})。リポジトリ直下に .env がある環境 (miniPC) で実行してください`);
  process.exit(2);
}

const txt = fs.readFileSync(envPath, 'utf8');
const has = (k) => new RegExp(`^\\s*${k}\\s*=`, 'm').test(txt);

const missing = ['CONTACTS_ENC_KEY', 'CONTACTS_HMAC_KEY'].filter((k) => !has(k));
if (missing.length === 0) {
  console.log('CONTACTS_ENC_KEY / CONTACTS_HMAC_KEY は設定済み。何もしません (ローテはこのツールの対象外)');
  process.exit(0);
}

const lines = missing.map((k) => `${k}=${crypto.randomBytes(32).toString('hex')}`);
const sep = txt.endsWith('\n') ? '' : '\n';
fs.appendFileSync(envPath, `${sep}# 楽天レビューcontacts暗号鍵 (gen-review-contact-keys.js が自動生成、値は表示しない)\n${lines.join('\n')}\n`);
console.log(`生成完了: ${missing.join(', ')} を .env に追記しました (値は表示しません)`);
