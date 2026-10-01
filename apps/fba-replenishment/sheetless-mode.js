/**
 * sheetless-mode.js — FBA 補充の「Sheet なし (fail-closed)」モードの決まり (マスタ正本切替 ⑦-F・2026-10-01)
 *
 * 正本 = AI_reference システム設計/CompanyDB構想/16_AmazonSKUの対応の編集_設計_20261001.md §7 v2 H3・§8 契約 v3 High 3。
 *
 * env FBA_SHEETLESS_MODE:
 *   なし / '' / '0' = モードを使わない (今までどおり。本番の既定)
 *   '1'             = モードを使う。そろっていないと計算しない (Sheet には戻らない)
 *   それ以外の値     = 「使うつもり」とみなし、設定の誤りとして計算しない (黙ってモードを外さない)
 *
 * モードを使うとき:
 *   - FBA_SKU_MAPPING_SOURCE = 'mirror' (SKU の対応は warehouse-mirror の mirror_sku_resolved だけ)
 *   - FBA_NONFBA_SOURCE      = 'pml'    (他 CH の販売は商品管理リストの snapshot だけ)
 *   - fba.db の sku_mapping (Google Sheet「商品コード変換テーブル」の写し) の値は、計算・FNSKU・起動時の移行のどれにも使わない
 *   - 06:00 の定期同期は Sheet の同期だけ外す (土台・納品実績は続ける)。手の Sheet 同期の口は 410
 *   - FNSKU の更新は fba_sku_attrs だけに書く (sku_mapping には書かない)
 *
 * env FBA_SHEETLESS_IO (miniPC 用・Codex PR R1 Medium 1):
 *   計算をしない miniPC は Sheet の「入出力」だけを止める = Sheet 同期の口は 410・sku_mapping に書かない・起動時の backfill を流さない。
 *   計算の読み方 (sku_mapping を読む・Sheet に戻る) は変えない。FBA_SHEETLESS_MODE を入れた所は入出力も止まる (MODE ⊃ IO)
 *   なし / '' / '0' = 止めない。それ以外の値 = 止める (止める側に倒す)
 *
 * このファイルは env を読むだけ (DB に触らない)。db.js・router.js・sheets-sync.js・miniPC の fba-service.js が使う。
 */

export const SHEETLESS_ENV = 'FBA_SHEETLESS_MODE';
export const SHEETLESS_IO_ENV = 'FBA_SHEETLESS_IO';

/** 起動時の sku_mapping → fba_sku_attrs の backfill を「済んだ」と記録する印のキー (fba.db の fba_migration_marks) */
export const BACKFILL_MARK_KEY = 'sku_mapping_to_fba_sku_attrs';

/** 手の Sheet 同期の口が返す 410 の文言 */
export const SHEET_SYNC_GONE_MESSAGE = 'Sheet なしのモード (FBA_SHEETLESS_MODE=1 / miniPC は FBA_SHEETLESS_IO=1) なので、Google Sheet「商品コード変換テーブル」の同期は止めています。'
  + 'SKU の対応はマスタ (Company DB → 写し) から、他 CH の販売は商品管理リストから読みます。';

const isOn = (v) => v !== undefined && v !== null && String(v).trim() !== '' && String(v).trim() !== '0';

/** モードを使うつもりか (値が入っていて '0' でない)。🚨 '1' 以外の値も「使うつもり」= 設定の誤りとして止める側に倒す */
export function isSheetlessRequested(env = process.env) {
  return isOn(env[SHEETLESS_ENV]);
}

/**
 * Sheet の入出力を止めるか (Sheet 同期の口は 410・sku_mapping に書かない・FNSKU は fba_sku_attrs だけ・起動時の backfill を流さない)。
 * Render = FBA_SHEETLESS_MODE (計算も Sheet なし) / miniPC = FBA_SHEETLESS_IO (入出力だけ)
 */
export function isSheetlessIoRequested(env = process.env) {
  return isSheetlessRequested(env) || isOn(env[SHEETLESS_IO_ENV]);
}

/**
 * モードを使うつもりのときに、そろっていない設定の一覧 (日本語)。使わないときは []。
 * 🚨 比べ方は実際の読み手と同じにする: 対応は getSkuMappingSource() が大小文字をそのまま見る ('mirror' だけ)、
 *    他 CH は getNonFbaSource() が小文字にしてから見る
 */
export function sheetlessProblems(env = process.env) {
  if (!isSheetlessRequested(env)) return [];
  const out = [];
  if (String(env[SHEETLESS_ENV]).trim() !== '1') out.push(`${SHEETLESS_ENV} の値が 1 ではない (${JSON.stringify(String(env[SHEETLESS_ENV]))})`);
  const mapping = env.FBA_SKU_MAPPING_SOURCE || 'sheet';
  if (mapping !== 'mirror') out.push(`FBA_SKU_MAPPING_SOURCE が mirror ではない (${mapping})`);
  const nonFba = (env.FBA_NONFBA_SOURCE || 'sheet').toLowerCase();
  if (nonFba !== 'pml') out.push(`FBA_NONFBA_SOURCE が pml ではない (${nonFba})`);
  return out;
}

/** 設定の誤りの例外 (code = FBA_SHEETLESS_MISCONFIG) */
export function sheetlessMisconfigError(problems) {
  return Object.assign(
    new Error(`Sheet なしのモードの設定がそろっていない: ${problems.join(' / ')}。Sheet には戻らずに止める`),
    { code: 'FBA_SHEETLESS_MISCONFIG', problems },
  );
}
