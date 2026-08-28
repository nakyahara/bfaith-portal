/**
 * gdrive-inbox.js のテスト。Drive クライアントを偽物に差し替えて、一覧→DL→受け箱→移動 の流れと
 * 対応外・重複・失敗時の挙動を検証する (実Driveは触らない)
 * 実行: node apps/shohyo-links/tests/test-gdrive-inbox.mjs
 */
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'shohyo-gdrive-'));
const { runGdriveInbox, gdriveInboxFolderId, gdriveInboxEnabled } = await import('../gdrive-inbox.js');
const { listInbox } = await import('../inbox.js');

let failed = 0;
const check = (label, cond) => { console.log(`${cond ? 'OK ' : 'NG '} ${label}`); if (!cond) failed++; };

check('フォルダIDの形式チェック', gdriveInboxFolderId({ SHOHYO_GDRIVE_INBOX_FOLDER_ID: '1wMgG-MvVdun7' }) === '1wMgG-MvVdun7' && gdriveInboxFolderId({ SHOHYO_GDRIVE_INBOX_FOLDER_ID: "x' or 1" }) === '');
check('鍵とフォルダIDが揃って有効', gdriveInboxEnabled({ SHOHYO_GDRIVE_INBOX_FOLDER_ID: '1wMgG-MvVdun7', GOOGLE_SERVICE_ACCOUNT_KEY: 'k' }) && !gdriveInboxEnabled({ SHOHYO_GDRIVE_INBOX_FOLDER_ID: '1wMgG-MvVdun7' }));

// 偽 Drive
const pdf = Buffer.from('%PDF-1.4 receipt');
const files = [
  { id: 'f1', name: '2026-08-08_21092_ロジマート.pdf', mimeType: 'application/pdf', size: String(pdf.length) },
  { id: 'f2', name: 'big.pdf', mimeType: 'application/pdf', size: String(6 * 1024 * 1024) },
  { id: 'f3', name: 'memo.txt', mimeType: 'text/plain', size: '10' },
  { id: 'f4', name: 'same-content.pdf', mimeType: 'application/pdf', size: String(pdf.length) },
  { id: 'f5', name: 'fake.pdf', mimeType: 'application/pdf', size: '6' },
];
const moved = [];
const created = [];
const drive = {
  files: {
    list: async ({ q }) => {
      if (q.includes("mimeType = 'application/vnd.google-apps.folder'")) return { data: { files: q.includes('取込済み') ? [{ id: 'sub-ingested' }] : [] } };
      return { data: { files } };
    },
    create: async ({ requestBody }) => { created.push(requestBody.name); return { data: { id: 'sub-' + requestBody.name } }; },
    get: async ({ fileId }) => ({ data: fileId === 'f5' ? Buffer.from('<html>') : pdf }),
    update: async ({ fileId, addParents, removeParents }) => { moved.push([fileId, addParents, removeParents]); return { data: { id: fileId } }; },
  },
};

const r = await runGdriveInbox({ drive, folderId: 'root1', env: {} });
check('一覧5件', r.listed === 5);
check('サブフォルダ: 取込済みは既存を使い、対応外は作る', !created.includes('取込済み') && created.includes('対応外'));
check('受け箱に1件入る (規約ファイル名から日付・金額)', r.ingested === 1 && listInbox().length === 1 && listInbox()[0].amount === 21092 && listInbox()[0].source === 'gdrive');
check('同じ内容は duplicate (受け箱は増えない)', r.duplicate === 1 && listInbox().length === 1);
check('5MB超・text/plain・中身がPDFでない は対応外', r.unsupported === 3);
const to = Object.fromEntries(moved.map(([id, add]) => [id, add]));
check('取込済みへ移動: f1, f4', to.f1 === 'sub-ingested' && to.f4 === 'sub-ingested');
check('対応外へ移動: f2, f3, f5', to.f2 === 'sub-対応外' && to.f3 === 'sub-対応外' && to.f5 === 'sub-対応外');
check('失敗なし ok', r.ok && r.failed === 0);

// DL失敗は失敗として数え、他は続行
const drive2 = { files: { ...drive.files, get: async ({ fileId }) => { if (fileId === 'f1') throw new Error('boom'); return { data: pdf }; } } };
const r2 = await runGdriveInbox({ drive: drive2, folderId: 'root1', env: {} });
check('DL失敗は failed (ok=false) で他は続行', !r2.ok && r2.failed === 1 && r2.errors[0].startsWith('2026-08-08') && r2.duplicate === 2);

// 未設定はスキップ
const r3 = await runGdriveInbox({ env: {} });
check('フォルダID未設定は skipped', r3.skipped === 'not_configured');

console.log(failed ? `\n${failed}件NG` : '\n全件パス');
process.exit(failed ? 1 : 0);
