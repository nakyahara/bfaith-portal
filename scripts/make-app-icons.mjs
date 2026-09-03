/**
 * ホーム画面用アプリアイコンの生成 (倉庫系 iPad アプリを一目で見分けるため)
 *
 * 背景: ピッキング・梱包・入荷受付チェックが全部 /favicon.png を使っていたため、
 *       ホーム画面に3つ並べると同じアイコンになって現場が押し間違える (中原さん 2026-09-01)。
 *
 * 実行: node scripts/make-app-icons.mjs
 * 出力: public/app-icons/<slug>-180.png (Apple touch icon) と -192/-512.png (PWA manifest)
 *
 * 依存は sharp のみ (既に package.json にある)。SVG を組み立ててラスタライズする。
 * ⭐フォントに依存しない図形 + 大きな文字1〜2字。iPad のホーム画面 (角丸で切られる) を前提に
 *   端 12% は余白として空ける。色はアプリごとに変えて、暗い背景で白抜き = 小さくても判別できる。
 */
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import sharp from 'sharp';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT_DIR = path.join(__dirname, '..', 'public', 'app-icons');

// 各アプリ: 色 (濃い地色) と中の図。図は「見た瞬間に分かる」ことを優先し、文字は最小限にする
const APPS = [
  {
    slug: 'inbound-check',
    label: '入荷受付チェック',
    bg: ['#1c7ed6', '#1864ab'],     // 青 = 入荷
    // 箱 + チェックマーク
    art: `
      <rect x="118" y="170" width="276" height="200" rx="18" fill="rgba(255,255,255,0.18)"/>
      <path d="M118 232 L394 232" stroke="#fff" stroke-width="14" stroke-linecap="round" opacity="0.55"/>
      <path d="M256 170 L256 232" stroke="#fff" stroke-width="14" stroke-linecap="round" opacity="0.55"/>
      <path d="M170 300 L228 356 L350 234" fill="none" stroke="#fff" stroke-width="42"
            stroke-linecap="round" stroke-linejoin="round"/>`,
  },
  {
    slug: 'picking',
    label: 'ピッキング支援',
    bg: ['#f76707', '#d9480f'],     // オレンジ = ピッキング
    // 棚 + 取り出す矢印
    art: `
      <rect x="120" y="150" width="180" height="230" rx="14" fill="rgba(255,255,255,0.18)"/>
      <path d="M120 226 L300 226 M120 302 L300 302" stroke="#fff" stroke-width="12" opacity="0.55"/>
      <path d="M300 265 L392 265" stroke="#fff" stroke-width="34" stroke-linecap="round"/>
      <path d="M348 218 L400 265 L348 312" fill="none" stroke="#fff" stroke-width="34"
            stroke-linecap="round" stroke-linejoin="round"/>`,
  },
  {
    slug: 'packing',
    label: '梱包支援',
    bg: ['#2f9e44', '#2b8a3e'],     // 緑 = 梱包
    // 箱 + テープ (封をする)
    art: `
      <rect x="110" y="176" width="292" height="196" rx="18" fill="rgba(255,255,255,0.18)"/>
      <path d="M110 240 L402 240" stroke="#fff" stroke-width="14" opacity="0.55"/>
      <rect x="228" y="150" width="56" height="248" rx="10" fill="#fff" opacity="0.92"/>
      <path d="M256 118 C 210 140, 210 176, 256 176 C 302 176, 302 140, 256 118 Z" fill="#fff" opacity="0.92"/>`,
  },
  {
    slug: 'iroha-work',
    label: 'いろは在庫化',
    bg: ['#0ca678', '#087f5b'],     // ティール = いろは (梱包の緑と並ばないが、念のため色味を変える)
    // 棚 (2段・商品が並ぶ) + 上から商品を入れる矢印 = 「在庫化 (棚入れ)」。
    // 単色プレースホルダだったものを置き換え (中原さん 2026-09-03「分かりやすい在庫化みたいな形に」)
    art: `
      <rect x="104" y="196" width="304" height="216" rx="18" fill="rgba(255,255,255,0.18)"/>
      <path d="M104 304 L408 304" stroke="#fff" stroke-width="14" opacity="0.55"/>
      <rect x="128" y="232" width="56" height="56" rx="10" fill="#fff" opacity="0.92"/>
      <rect x="200" y="232" width="56" height="56" rx="10" fill="#fff" opacity="0.92"/>
      <rect x="128" y="340" width="56" height="56" rx="10" fill="#fff" opacity="0.92"/>
      <rect x="200" y="340" width="56" height="56" rx="10" fill="#fff" opacity="0.92"/>
      <rect x="272" y="340" width="56" height="56" rx="10" fill="#fff" opacity="0.92"/>
      <rect x="300" y="80" width="72" height="72" rx="12" fill="#fff" opacity="0.92"/>
      <path d="M336 168 L336 244" stroke="#fff" stroke-width="30" stroke-linecap="round"/>
      <path d="M298 212 L336 254 L374 212" fill="none" stroke="#fff" stroke-width="30"
            stroke-linecap="round" stroke-linejoin="round"/>`,
  },
];

const SIZES = [180, 192, 512];

function svg({ bg, art }) {
  return `<svg xmlns="http://www.w3.org/2000/svg" width="512" height="512" viewBox="0 0 512 512">
  <defs>
    <linearGradient id="g" x1="0" y1="0" x2="0" y2="1">
      <stop offset="0" stop-color="${bg[0]}"/><stop offset="1" stop-color="${bg[1]}"/>
    </linearGradient>
  </defs>
  <rect width="512" height="512" fill="url(#g)"/>
  ${art}
</svg>`;
}

fs.mkdirSync(OUT_DIR, { recursive: true });
for (const app of APPS) {
  const buf = Buffer.from(svg(app));
  for (const size of SIZES) {
    const out = path.join(OUT_DIR, `${app.slug}-${size}.png`);
    await sharp(buf).resize(size, size).png({ compressionLevel: 9 }).toFile(out);
    console.log(`${app.label.padEnd(16)} → ${path.relative(process.cwd(), out)} (${fs.statSync(out).size} B)`);
  }
}
console.log('\n完了。iPad はホーム画面に追加した時点のアイコンを保持するので、');
console.log('既に追加済みのものは一度削除してから追加し直してください。');
