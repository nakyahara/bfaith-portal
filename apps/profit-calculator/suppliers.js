/**
 * 仕入れ先マスタ管理 — JSON file in DATA_DIR
 */
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, '..', '..', 'data');
const SUPPLIERS_FILE = path.join(DATA_DIR, 'suppliers.json');

function ensureDataDir() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
}

// 初期データ（スプレッドシート「仕入先コード」シートから）
const DEFAULT_SUPPLIERS = [
  {"code":"0001","name":"アメージングクラフト様"},
  {"code":"0002","name":"ビーフリー様"},
  {"code":"0003","name":"Rok様"},
  {"code":"0004","name":"アカシアスタイル様"},
  {"code":"0005","name":"サンスター技研様"},
  {"code":"0099","name":"千年前の食品舎様"},
  {"code":"0100","name":"ジェラルドジャパン様"},
  {"code":"0101","name":"H&J様"},
  {"code":"0102","name":"アクアデザイン様"},
  {"code":"0103","name":"東京粉末【WEBサイト発注】下代20,000円以上送料無料 様"},
  {"code":"0104","name":"ウイスクイー様"},
  {"code":"0105","name":"クサノハ化粧品様"},
  {"code":"0106","name":"イーライフ株式会社様"},
  {"code":"0107","name":"株式会社サロンジェ様"},
  {"code":"0108","name":"尾上萬様"},
  {"code":"0109","name":"プラスティーチャー様"},
  {"code":"0110","name":"吉岡商店様"},
  {"code":"0111","name":"オーアールエス様"},
  {"code":"0112","name":"犬飼タオル様"},
  {"code":"0113","name":"株式会社FER様"},
  {"code":"0114","name":"株式会社フォーユー様"},
  {"code":"0115","name":"有限会社ビジネス・ベンチャーズ・ジャパン様"},
  {"code":"0116","name":"株式会社ニダフジャパン様【FAX発注】"},
  {"code":"0117","name":"株式会社マルコ様"},
  {"code":"0118","name":"大武ルート工業様【発注書PDF必要】"},
  {"code":"0119","name":"バイワールド株式会社様"},
  {"code":"0120","name":"株式会社ジャパン・ゼネラル貿易様"},
  {"code":"0121","name":"朝光テープ有限会社様【FAX発注】"},
  {"code":"0122","name":"株式会社日本パール加工様"},
  {"code":"0123","name":"株式会社二光社様"},
  {"code":"0124","name":"ジャパンソルト株式会社様"},
  {"code":"0125","name":"株式会社トルネ（スーパーデリバリー経由）様"},
  {"code":"0126","name":"株式会社プラスワン【WEB発注】様"},
  {"code":"0127","name":"まるは油脂化学株式会社様"},
  {"code":"0128","name":"ゼダーフード様"},
  {"code":"0129","name":"株式会社ユニエンタープライズ【WEBサイト発注】様"},
  {"code":"0130","name":"丸宗株式会社様"},
  {"code":"0131","name":"アリサン有限会社【WEBサイト発注】下代合計25,000円以上送料無料 様"},
  {"code":"0132","name":"エンド商事様"},
  {"code":"0133","name":"クラスアップ様"},
  {"code":"0134","name":"株式会社SAWA（スーパーデリバリー経由）様"},
  {"code":"0900","name":"トイズファン様"},
  {"code":"0997","name":"アスクル"},
  {"code":"0998","name":"シモジマ"},
  {"code":"0999","name":"ヤマト運輸（資材発注部署）"},
  {"code":"9998","name":"資材発注"},
  {"code":"9999","name":"B-Faith株式会社"},
];

export function loadSuppliers() {
  try {
    if (fs.existsSync(SUPPLIERS_FILE)) {
      const data = JSON.parse(fs.readFileSync(SUPPLIERS_FILE, 'utf-8'));
      if (data.length > 0) return data;
    }
  } catch (e) {
    console.warn('[Suppliers] 読み込み失敗:', e.message);
  }
  // ファイルが無い or 空 → 初期データを書き込んで返す
  saveSuppliers(DEFAULT_SUPPLIERS);
  console.log(`[Suppliers] 初期データ ${DEFAULT_SUPPLIERS.length}件 を作成しました`);
  return [...DEFAULT_SUPPLIERS];
}

export function saveSuppliers(suppliers) {
  ensureDataDir();
  fs.writeFileSync(SUPPLIERS_FILE, JSON.stringify(suppliers, null, 2), 'utf-8');
}

export function addSupplier(supplier) {
  const suppliers = loadSuppliers();
  const existing = suppliers.findIndex(s => s.code === supplier.code);
  if (existing >= 0) {
    suppliers[existing] = { ...suppliers[existing], ...supplier };
  } else {
    suppliers.push(supplier);
  }
  suppliers.sort((a, b) => a.code.localeCompare(b.code));
  saveSuppliers(suppliers);
  return suppliers;
}

export function deleteSupplier(code) {
  const suppliers = loadSuppliers();
  const filtered = suppliers.filter(s => s.code !== code);
  saveSuppliers(filtered);
  return filtered;
}
