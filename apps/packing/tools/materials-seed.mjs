/**
 * 梱包資材 seed 投入 CLI (要件定義 v1.7 §6)。
 *
 * 使い方 (miniPC は C:\tools\bfaith-picking で実行 — DATA_DIR は .env / 既定 ./data):
 *   node apps/packing/tools/materials-seed.mjs --file apps/packing/seed/materials.json --images apps/packing/seed/materials
 *   node apps/packing/tools/materials-seed.mjs --file ... --dry-run   (投入内容の確認のみ)
 *
 * 画像は DATA_DIR/materials/<code>-<rand>.<ext> へコピーして immutable 配信する
 * (差し替えはファイル名が変わる)。既に同じ code の画像がある場合も新しいファイルを作り、
 * DB の image_file を差し替える (旧ファイルは復旧猶予として残す — 管理画面/cleanup で整理)。
 */
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { initPackingDB, DATA_DIR } from '../db.js';
import { seedMaterialsData, normalizeKeyText } from '../materials.js';

function arg(name, fallback = null) {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 ? (process.argv[i + 1] || '') : fallback;
}
const file = arg('file');
const imagesDir = arg('images');
const dryRun = process.argv.includes('--dry-run');

if (!file) {
  console.error('usage: node materials-seed.mjs --file <seed.json> [--images <dir>] [--dry-run]');
  process.exit(1);
}
if (path.basename(file).includes('placeholder')) {
  console.error('placeholder seed は本番投入できません (テスト専用)');
  process.exit(1);
}
const seed = JSON.parse(fs.readFileSync(file, 'utf8'));

// 概要表示 (dry-run / 実行前の確認)
console.log(`資材 ${seed.materials?.length ?? 0} 件 / 分類 ${seed.classes?.length ?? 0} 件 / `
  + `候補 ${seed.class_materials?.length ?? 0} 分類 / 伝票指定辞書 ${seed.header_map?.length ?? 0} 件`);
for (const h of seed.header_map || []) {
  console.log(`  header: "${normalizeKeyText(h.header_value)}" → ${h.base_delivery_code}${h.material_code ? ` (指定資材 ${h.material_code})` : ''}`);
}
if (dryRun) { console.log('dry-run: DB へは書き込みません'); process.exit(0); }

initPackingDB();

// 画像コピー: seed 内の image_file (ファイル名) を imagesDir から DATA_DIR/materials へ
const matDir = path.join(DATA_DIR, 'materials');
fs.mkdirSync(matDir, { recursive: true });
for (const m of seed.materials || []) {
  if (!m.image_file) continue;
  if (!imagesDir) { console.warn(`--images 未指定のため画像スキップ: ${m.code}`); m.image_file = null; continue; }
  const src = path.join(imagesDir, m.image_file);
  if (!fs.existsSync(src)) { console.warn(`画像なし (スキップ): ${src}`); m.image_file = null; continue; }
  const ext = path.extname(src).toLowerCase();
  if (!['.jpg', '.jpeg', '.png', '.webp'].includes(ext)) { console.warn(`未対応形式 (スキップ): ${src}`); m.image_file = null; continue; }
  const name = `${m.code}-${crypto.randomBytes(6).toString('hex')}${ext}`;
  fs.copyFileSync(src, path.join(matDir, name));
  m.image_file = name;
  console.log(`画像: ${m.code} → materials/${name}`);
}

seedMaterialsData(seed, 'seed-cli');
console.log('seed 投入完了');
