/**
 * select-set の管理画面 (ポータルセッション認証。mount側で requireAppAccess('select-set'))。
 * 選べるセットの登録・手動マッピング・おまけ優先順位の編集と、商品OPの動作確認。
 *
 * マスタの「正」は Render 側 (中原さんが普段開くポータル)。
 * miniPC 側は Render から取得したコピーで動くため、この画面での編集は読み取り専用にする
 * (miniPCで編集しても次の取得で上書きされるため、書けてしまう方が事故になる)。
 */
import { Router } from 'express';
import archiver from 'archiver';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import {
  deleteMapping, deleteSet, initSelectSetDB, listMappings, listOmake, listSets,
  replaceOmake, upsertMapping, upsertSet,
} from './db.js';
import { masterMode, masterStatus, pullMaster } from './master-sync.js';
import {
  SsError, canValidateProducts, diagnose, environment, expandForOrder, inspectSet, stockOfSoft,
} from './service.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const router = Router();

/** 拡張機能のzipに入れない開発用ファイル (Googleに提出するパッケージに社内メモを混ぜないため) */
const DEV_ONLY_FILES = new Set(['make-icons.mjs', 'STORE_LISTING.md']);

// import 時 (= server.js boot 時) に DB を初期化する。migration 失敗時は起動を止める
initSelectSetDB();

function api(handler) {
  return async (req, res) => {
    try {
      res.json(await handler(req));
    } catch (e) {
      if (e instanceof SsError) return res.status(e.status).json({ error: e.message, code: e.code });
      console.error('[select-set] API error', e);
      res.status(500).json({ error: 'サーバーエラーが発生しました' });
    }
  };
}

/** マスタを編集してよい環境か。miniPC (replica) では書かせない */
function assertEditable() {
  if (masterMode() === 'replica') {
    throw new SsError(
      'この画面はコピーです。セット・マッピング・おまけの編集は普段のポータル (Render側) で行ってください。'
      + ' ここで編集しても次回の取得で上書きされます。',
      { status: 409, code: 'READ_ONLY_REPLICA' },
    );
  }
}

router.get('/', (req, res) => {
  // 末尾スラッシュなしで開くと相対URLの解決基準がずれるため正規URLへ寄せる (easy-ship と同パターン)
  const qIdx = req.originalUrl.indexOf('?');
  const pathname = qIdx === -1 ? req.originalUrl : req.originalUrl.slice(0, qIdx);
  const query = qIdx === -1 ? '' : req.originalUrl.slice(qIdx);
  if (!pathname.endsWith('/')) return res.redirect(308, pathname + '/' + query);
  res.render(path.join(__dirname, 'views/index'), {
    title: '選べるセットの明細展開',
    username: req.session.email,
    displayName: req.session.displayName,
  });
});

// ---- 環境・診断 ----

/** 画面が最初に叩く。マスタの正がどちらか・商品マスタが引けるかで表示を出し分ける */
router.get('/api/env', api(() => environment()));

/**
 * 診断。「画面では解決できないのに手元のスクリプトでは解決できる」ときに、
 * サーバープロセスが実際に何を見ているか (cwd / DATA_DIR / 商品マスタの件数 / 認証情報の有無) を出す。
 * 値そのものは返さず、有無と件数だけ。
 */
router.get('/api/diag', api(() => diagnose()));

/** miniPC側で「今すぐRenderからマスタを取り直す」 */
router.post('/api/master/pull', api(async () => {
  if (masterMode() !== 'replica') {
    throw new SsError('この環境はマスタの取得元ではありません', { status: 409, code: 'NOT_REPLICA' });
  }
  const r = await pullMaster();
  return { ...r, status: masterStatus() };
}));

// ---- セット ----
router.get('/api/sets', api(() => ({ sets: listSets({ includeInactive: true }) })));

router.post('/api/sets', api((req) => {
  assertEditable();
  const setCode = String(req.body?.setCode || '').trim();
  if (!setCode) throw new SsError('セット商品コードを入力してください');
  upsertSet({
    setCode,
    label: req.body?.label || '',
    isActive: req.body?.isActive === false ? 0 : 1,
    note: req.body?.note || '',
  });
  return { ok: true };
}));

router.delete('/api/sets/:code', api((req) => {
  assertEditable();
  deleteSet(String(req.params.code));
  return { ok: true };
}));

/** RMSから選択肢定義を引いて状態を見る。?force=1 でキャッシュを無視して取り直す */
router.get('/api/sets/:code/inspect', api((req) => inspectSet(String(req.params.code), {
  force: req.query.force === '1',
})));

// ---- 手動マッピング ----
router.get('/api/mappings', api((req) => ({ mappings: listMappings(req.query.setCode || null) })));

router.post('/api/mappings', api((req) => {
  assertEditable();
  const setCode = String(req.body?.setCode || '').trim();
  const optionText = String(req.body?.optionText || '').trim();
  const productCode = String(req.body?.productCode || '').trim();
  if (!setCode || !optionText || !productCode) throw new SsError('セット・選択肢・商品コードは必須です');
  // 商品マスタが引ける環境でだけ実在チェックする。
  // Render には warehouse.db が無いので、ここで弾くと登録そのものができなくなる
  let warning = null;
  if (canValidateProducts()) {
    if (!stockOfSoft(productCode)) {
      throw new SsError(`商品コード「${productCode}」がNE商品マスタに見つかりません`, { code: 'UNKNOWN_PRODUCT' });
    }
  } else {
    warning = '商品コードの実在チェックはこの環境では行えません (登録は保存しました)';
  }
  const id = upsertMapping({ id: req.body?.id, setCode, optionText, productCode, note: req.body?.note || '' });
  return { ok: true, id, warning };
}));

router.delete('/api/mappings/:id', api((req) => {
  assertEditable();
  deleteMapping(Number(req.params.id));
  return { ok: true };
}));

// ---- おまけ優先順位 ----
router.get('/api/omake', api(() => ({
  omake: listOmake().map((r) => ({ ...r, ...(stockOfSoft(r.product_code) || { name: '', available: null }) })),
})));

router.post('/api/omake', api((req) => {
  assertEditable();
  const codes = Array.isArray(req.body?.codes) ? req.body.codes : [];
  if (!codes.length) throw new SsError('おまけ候補が空です');
  let warning = null;
  if (canValidateProducts()) {
    const unknown = codes.filter((c) => !stockOfSoft(c));
    if (unknown.length) {
      throw new SsError(`NE商品マスタに無い商品コード: ${unknown.join(', ')}`, { code: 'UNKNOWN_PRODUCT' });
    }
  } else {
    warning = '商品コードの実在チェックはこの環境では行えません (並びは保存しました)';
  }
  const n = replaceOmake(codes);
  return { ok: true, count: n, warning };
}));

/**
 * Chrome拡張の配布。
 * ⚠ Chrome はストア外の拡張を .crx で直接インストールさせない (ドラッグ&ドロップも不可) ため、
 *   「zipを落とす → 解凍 → デベロッパーモードで『パッケージ化されていない拡張機能を読み込む』」
 *   という手順はどうしても残る。ここはその配布を楽にするだけ
 *   (各PCにリポジトリを置かなくてよくする / 更新版を配れるようにする)。
 */
router.get('/download/extension.zip', (req, res) => {
  const dir = path.resolve(__dirname, '../../tools/ne-select-set-helper');
  let version = '';
  try {
    version = JSON.parse(fs.readFileSync(path.join(dir, 'manifest.json'), 'utf8')).version || '';
  } catch { /* バージョンが読めなくても配布は続ける */ }
  if (!fs.existsSync(dir)) {
    return res.status(404).json({ error: '拡張機能のファイルが見つかりません' });
  }
  res.set({
    'Content-Type': 'application/zip',
    'Content-Disposition': `attachment; filename=ne-select-set-helper${version ? '-' + version : ''}.zip`,
  });
  const archive = archiver('zip', { zlib: { level: 9 } });
  archive.on('error', (e) => {
    console.error('[select-set] 拡張機能のzip作成に失敗', e);
    res.destroy();
  });
  archive.pipe(res);
  // 開発用ファイルは同梱しない。特に STORE_LISTING.md は提出手順の社内メモなので、
  // Google に出すパッケージに入れたくない
  archive.directory(dir, false, (entry) => (DEV_ONLY_FILES.has(entry.name) ? false : entry));
  archive.finalize();
});

// ---- 動作確認 ----
router.post('/api/try', api((req) => expandForOrder({
  setCode: req.body?.setCode,
  op: req.body?.op,
  quantity: req.body?.quantity,
})));

export default router;
