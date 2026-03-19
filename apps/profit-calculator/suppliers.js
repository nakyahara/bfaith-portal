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

export function loadSuppliers() {
  try {
    if (fs.existsSync(SUPPLIERS_FILE)) {
      return JSON.parse(fs.readFileSync(SUPPLIERS_FILE, 'utf-8'));
    }
  } catch (e) {
    console.warn('[Suppliers] 読み込み失敗:', e.message);
  }
  return [];
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
  saveSuppliers(suppliers);
  return suppliers;
}
