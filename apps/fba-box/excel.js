/**
 * FBA納品 箱詰め記録 — STA パックリストExcel の取込 (解析は Python/openpyxl)
 *
 * 要件 §10 (Codex R1 C6): アップロードファイルを信用しない。
 *   .xlsx (ZIPシグネチャ) のみ / サイズ上限 / 原本はランダム名で隔離保存 / SHA-256 記録。
 * 解析は apps/fba-box/python/parse_packlist.py (構造検出・未知形式は ok:false)。
 */
import { spawn } from 'node:child_process';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const SCRIPT = path.join(__dirname, 'python', 'parse_packlist.py');
const WRITE_SCRIPT = path.join(__dirname, 'python', 'write_packlist.py');

const DATA_DIR = process.env.DATA_DIR || path.join(process.cwd(), 'data');
export const EXCEL_DIR = path.join(DATA_DIR, 'fba-box-excel');
/** Excel 出力の保存先 (版ごとに別ファイル。原本は EXCEL_DIR 直下のまま非破壊) */
export const EXPORT_DIR = path.join(EXCEL_DIR, 'exports');

export const MAX_XLSX_BYTES = 8 * 1024 * 1024;

/** 既知テンプレの fingerprint (実物2件 = 2026-09-02 で確定。新形式が来たら要確認の上で追加) */
export const KNOWN_FINGERPRINTS = new Set(['d337e046bbf029c1']);

function pythonCmd() {
  if (process.env.RENDER) return path.join(__dirname, '..', '..', 'venv', 'bin', 'python3');
  return process.platform === 'win32' ? 'python' : 'python3';
}

/**
 * Python スクリプトを起動して stdout の JSON を返す。
 * input があれば stdin に流す (書き込み指示のような大きな JSON は引数でなく stdin で渡す —
 * [[feedback_external_cli_pass_data_via_stdin]])
 */
function runPython(script, args, { timeoutMs = 30_000, input = null, label = 'Excel解析' } = {}) {
  return new Promise((resolve, reject) => {
    // PYTHONUTF8: Windows ローカルでは stdout が cp932 になり JSON が壊れる (Render/Linux は元々 UTF-8)
    const cp = spawn(pythonCmd(), [script, ...args], {
      stdio: [input == null ? 'ignore' : 'pipe', 'pipe', 'pipe'],
      env: { ...process.env, PYTHONUTF8: '1', PYTHONIOENCODING: 'utf-8' },
    });
    let out = '', err = '';
    const timer = setTimeout(() => {
      cp.kill('SIGKILL');
      reject(new Error(`${label}がタイムアウトしました`));
    }, timeoutMs);
    cp.stdout.on('data', (c) => { out += c; if (out.length > 20 * 1024 * 1024) { cp.kill('SIGKILL'); } });
    cp.stderr.on('data', (c) => { err += c; });
    cp.on('error', (e) => { clearTimeout(timer); reject(e); });
    cp.on('close', (code) => {
      clearTimeout(timer);
      try {
        resolve(JSON.parse(out));
      } catch {
        reject(new Error(`${label}に失敗しました (exit=${code}): ${String(err).slice(0, 300)}`));
      }
    });
    if (input != null) {
      cp.stdin.on('error', () => { /* 子が先に落ちた場合の EPIPE。close 側で失敗として扱う */ });
      cp.stdin.end(input);
    }
  });
}

/**
 * パックリストへの書き込み (PR2, 要件 F-7)。python/write_packlist.py が原本 zip の該当セルだけを
 * 差し替え、再読込・byte 一致・fingerprint で独自検算した結果を返す。
 * @param sheets [{ sheetName, cells: [{row, col, value, kind}] }]
 * @returns { ok:true, outputPath, sha256, written, verify } | { ok:false, error, message, ... }
 */
export async function writePacklist({ templatePath, sheets, fileTag = 'export' }) {
  if (!templatePath || !fs.existsSync(templatePath)) {
    return { ok: false, error: 'template_missing', message: 'アップロードされた原本Excelが見つかりません (永続ディスクから消えた可能性)。テンプレを再アップロードしてください' };
  }
  fs.mkdirSync(EXPORT_DIR, { recursive: true });
  const outputPath = path.join(EXPORT_DIR, `${String(fileTag).replace(/[^a-zA-Z0-9_-]/g, '')}-${Date.now()}-${crypto.randomBytes(6).toString('hex')}.xlsx`);
  let r;
  try {
    r = await runPython(WRITE_SCRIPT, [], {
      timeoutMs: 60_000, label: 'Excel書き込み',
      input: JSON.stringify({ template: templatePath, output: outputPath, sheets }),
    });
  } catch (e) {
    try { fs.unlinkSync(outputPath); } catch { /* noop */ }
    return { ok: false, error: 'write_failed', message: e.message };
  }
  if (!r.ok) {
    try { fs.unlinkSync(outputPath); } catch { /* noop */ }
    return { ok: false, error: r.error || 'write_failed', message: r.message || 'Excelに書き込めませんでした', detail: r };
  }
  // Python が報告した sha256 と Node 側で計算した値の一致 (別実装で二重に確認)
  const sha256 = crypto.createHash('sha256').update(fs.readFileSync(outputPath)).digest('hex');
  if (sha256 !== r.sha256) {
    try { fs.unlinkSync(outputPath); } catch { /* noop */ }
    return { ok: false, error: 'verify_failed', message: '出力ファイルのハッシュが一致しません' };
  }
  return { ok: true, outputPath, sha256, written: r.written, verify: r.verify };
}

/**
 * アップロードされたバッファを検証 → 隔離保存 → 構造解析。
 * @returns { ok:false, error, message } | { ok:true, storedPath, sha256, parsed, fingerprintKnown }
 */
export async function ingestPacklist(buffer, originalName) {
  if (!buffer || buffer.length === 0) return { ok: false, error: 'no_file', message: 'ファイルがありません' };
  if (buffer.length > MAX_XLSX_BYTES) return { ok: false, error: 'too_large', message: 'ファイルが大きすぎます (8MBまで)' };
  // ZIP シグネチャ (xlsx は ZIP)。PK\x03\x04 以外は受けない
  if (!(buffer[0] === 0x50 && buffer[1] === 0x4b && buffer[2] === 0x03 && buffer[3] === 0x04)) {
    return { ok: false, error: 'not_xlsx', message: '.xlsx ファイルではありません (STAからDLしたパックリストをそのままアップしてください)' };
  }
  if (!/\.xlsx$/i.test(String(originalName || ''))) {
    return { ok: false, error: 'not_xlsx', message: '拡張子が .xlsx ではありません' };
  }
  fs.mkdirSync(EXCEL_DIR, { recursive: true });
  const sha256 = crypto.createHash('sha256').update(buffer).digest('hex');
  const storedPath = path.join(EXCEL_DIR, `${Date.now()}-${crypto.randomBytes(8).toString('hex')}.xlsx`);
  fs.writeFileSync(storedPath, buffer);
  let parsed;
  try {
    parsed = await runPython(SCRIPT, [storedPath]);
  } catch (e) {
    try { fs.unlinkSync(storedPath); } catch { /* noop */ }
    return { ok: false, error: 'parse_failed', message: e.message };
  }
  if (!parsed.ok) {
    try { fs.unlinkSync(storedPath); } catch { /* noop */ }
    return { ok: false, error: parsed.error || 'parse_failed', message: parsed.message || 'Excelの形式を認識できませんでした', detail: parsed };
  }
  // 書き込み対象セルが locked のテンプレは PR2 (出力) で書けない → 取込時点で止める (fail-closed)
  const lockedCount = parsed.sheets.reduce((a, s) => a + (s.lockedTargets?.length || 0), 0);
  if (lockedCount > 0) {
    try { fs.unlinkSync(storedPath); } catch { /* noop */ }
    return { ok: false, error: 'locked_cells', message: `書き込み対象のセルが保護されています (${lockedCount}箇所)。テンプレの形式が変わった可能性があります — 手動転記に切り替えて中原さんに連絡してください` };
  }
  // 未知の fingerprint は受理しない (Codex PR1 #1: 列構造の意味が変わった新テンプレで
  // 作業→誤ったExcel出力まで進む事故を、警告ではなく拒否で止める)。
  // 新形式が来たら実物を確認して KNOWN_FINGERPRINTS に追加する
  if (!KNOWN_FINGERPRINTS.has(parsed.fingerprint)) {
    try { fs.unlinkSync(storedPath); } catch { /* noop */ }
    return { ok: false, error: 'unknown_template', fingerprint: parsed.fingerprint,
      message: `テンプレの形式が既知のものと違います (fingerprint=${parsed.fingerprint})。今回は手動転記で進め、このコードを中原さんに伝えてください (実物確認のうえ対応形式に追加します)` };
  }
  return { ok: true, storedPath, sha256, parsed, fingerprintKnown: true };
}
