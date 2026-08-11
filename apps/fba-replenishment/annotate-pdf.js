/**
 * TMP1 PDF の Node→Python ブリッジ (python/annotate_picking_pdf.py)。
 * (aes-pdf-sorter と同じく Render では /app/venv の python を使う)
 *
 * 2段階で使う (1 spawn に抽出と注番をまとめると、注番に必要な突合結果が
 * 抽出後の Node 側処理でしか作れず循環するため):
 *  1. extractPickingPdf()  — 構造化抽出のみ (残数の取得元)
 *  2. annotatePickingPdf() — 検証済みの page/item 単位 プランNo 割り当てで注番
 */
import { spawn } from 'node:child_process';
import { promises as fsp } from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const SCRIPT = path.join(__dirname, 'python', 'annotate_picking_pdf.py');

function pythonCmd() {
  if (process.env.RENDER) return path.join(__dirname, '..', '..', 'venv', 'bin', 'python3');
  return process.platform === 'win32' ? 'python' : 'python3';
}

function runPython(args, { timeoutMs, timeoutLabel }) {
  return new Promise((resolve, reject) => {
    const cp = spawn(pythonCmd(), [SCRIPT, ...args], { stdio: ['ignore', 'pipe', 'pipe'] });
    let out = '', err = '';
    const timer = setTimeout(() => { cp.kill('SIGKILL'); reject(new Error(`${timeoutLabel}がタイムアウトしました`)); }, timeoutMs);
    cp.stdout.on('data', (d) => { out += d; });
    cp.stderr.on('data', (d) => { err += d; });
    cp.on('error', (e) => { clearTimeout(timer); reject(new Error(`${timeoutLabel}プロセス起動失敗: ${e.message}`)); });
    cp.on('close', (code) => {
      clearTimeout(timer);
      if (code !== 0) return reject(new Error(`${timeoutLabel}に失敗 (exit ${code}): ${(err || out).slice(0, 500)}`));
      // 成功時の stdout JSON は完全性検証に使うため、解析不能は成功扱いにしない (fail-closed)
      try { resolve(JSON.parse(out)); }
      catch { reject(new Error(`${timeoutLabel}の結果JSONを解析できません: ${(out || '(空)').slice(0, 200)}`)); }
    });
  });
}

/**
 * TMP1 PDF の構造化抽出 (ブロック/ロケ/商品ID/数量/残数)。
 * 構造検証 NG は Python 側が非0 exit → ここで throw (fail-closed)。
 * @param {Buffer} tmp1Buffer
 * @returns {Promise<{ pages:number, total:number, items:Array<{page,item,block,location,productId,qty,zansu}> }>}
 */
export async function extractPickingPdf(tmp1Buffer, { timeoutMs = 60000 } = {}) {
  const dir = await fsp.mkdtemp(path.join(os.tmpdir(), 'pp-extract-'));
  const pdfIn = path.join(dir, 'in.pdf');
  const jsonOut = path.join(dir, 'extract.json');
  try {
    await fsp.writeFile(pdfIn, tmp1Buffer);
    const stats = await runPython(['--pdf', pdfIn, '--extract-json', jsonOut], { timeoutMs, timeoutLabel: 'TMP1 PDF抽出' });
    if (stats.mode !== 'extract') throw new Error(`TMP1 PDF抽出の結果が不正です (mode=${stats.mode})`);
    const payload = JSON.parse(await fsp.readFile(jsonOut, 'utf8'));
    const items = (payload.items || []).map(it => ({
      page: it.page, item: it.item,
      block: it.block, location: it.location,
      productId: it.product_id,
      qty: it.qty, zansu: it.zansu,
    }));
    return { pages: payload.pages || 0, total: payload.total || items.length, items };
  } finally {
    await fsp.rm(dir, { recursive: true, force: true }).catch(() => {});
  }
}

/**
 * 納品プランNo を注番した PDF を生成。
 * @param {Buffer} tmp1Buffer
 * @param {Array<{page:number,item:number,productId:string,planNo:string,status:string}>} planItems
 * @returns {Promise<{ pdfBuffer: Buffer, matched: number, total: number }>}
 */
export async function annotatePickingPdf(tmp1Buffer, planItems, { timeoutMs = 60000 } = {}) {
  const dir = await fsp.mkdtemp(path.join(os.tmpdir(), 'pp-annot-'));
  const pdfIn = path.join(dir, 'in.pdf');
  const planJson = path.join(dir, 'plans.json');
  const pdfOut = path.join(dir, 'out.pdf');
  try {
    await fsp.writeFile(pdfIn, tmp1Buffer);
    await fsp.writeFile(planJson, JSON.stringify({ items: planItems }));
    const stats = await runPython(['--pdf', pdfIn, '--plan-json', planJson, '--out', pdfOut], { timeoutMs, timeoutLabel: 'PDF注番' });
    // 完全性検証: 注番件数が入力と一致しなければ成功扱いにしない (fail-closed)
    if (stats.mode !== 'annotate') throw new Error(`PDF注番の結果が不正です (mode=${stats.mode})`);
    if (stats.total !== planItems.length) {
      throw new Error(`PDF注番の対象件数(${stats.total})が入力件数(${planItems.length})と一致しません`);
    }
    const expectedMatched = planItems.filter(it => it.status === 'matched').length;
    if (stats.matched !== expectedMatched) {
      throw new Error(`PDF注番の一致件数(${stats.matched})が期待値(${expectedMatched})と一致しません`);
    }
    const pdfBuffer = await fsp.readFile(pdfOut);
    return { pdfBuffer, matched: stats.matched, total: stats.total };
  } finally {
    await fsp.rm(dir, { recursive: true, force: true }).catch(() => {});
  }
}
