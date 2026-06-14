/**
 * readiness blocked reason を業務担当者向け日本語文に翻訳する辞書 (Phase E-6 UI 再設計)。
 *
 * 内部 reason コード (notion_title_missing 等) を 中原さん / 業務担当者向けの
 * 「何を / どこで直すか」 文に変換する。 内部用語を UI に出さないための単一窓口。
 *
 * 設計原則 (Codex E-6 UX 議論 確定):
 *   - 「何が悪い」 より 「どこを直す」 を先に
 *   - Notion で直す項目を明示
 *   - 厳しい色 (赤) より「直してね」 トーン
 */

/**
 * 単一 reason 翻訳。
 * @returns {{ message: string, notionField: string|null, severity: 'must'|'check' }}
 */
export function translateReason(rawReason) {
  if (!rawReason || typeof rawReason !== 'string') {
    return { message: '原因不明のエラーです', notionField: null, severity: 'must' };
  }
  // reason の本体 (先頭の : 前まで)
  const head = rawReason.split(':')[0];
  switch (head) {
    case 'notion_title_missing':
      return { message: 'Yahoo!タイトルが Notion に入っていません', notionField: 'Yahoo!タイトル', severity: 'must' };
    case 'notion_title_too_long':
      return { message: 'Yahoo!タイトルが 65 字を超えています', notionField: 'Yahoo!タイトル', severity: 'must' };
    case 'product_category_unresolved':
      return { message: 'Yahoo!カテゴリが解決できません (楽天 genre が学習済 130 件に無い、 または学習辞書未構築)。 Notion に 「Yahoo!カテゴリID」 列を作って入力してください。 または既存出品から学習し直してください', notionField: 'Yahoo!カテゴリID', severity: 'must' };
    case 'path_unresolved':
      return { message: 'Yahoo!path が解決できません (同上、 楽天 genre が学習済 130 件に無い)。 Notion に 「Yahoo!path」 列を作って入力してください', notionField: 'Yahoo!path', severity: 'must' };
    case 'price_invalid_or_zero':
      return { message: '売価が未入力、 または 0 円です', notionField: '売価', severity: 'must' };
    case 'delivery_mapping_unresolved':
      return { message: '配送方法が Yahoo 用に変換できません (Notion で配送方法を選んでください)', notionField: '配送方法', severity: 'must' };
    case 'delivery_value_invalid':
      return { message: '配送方法の値が想定外です', notionField: '配送方法', severity: 'must' };
    case 'postage_set_out_of_range':
      return { message: '配送方法の設定が Yahoo の範囲外です', notionField: '配送方法', severity: 'must' };
    case 'notion_tax_rate_missing':
      return { message: '税率が Notion に入っていません', notionField: '税率', severity: 'must' };
    case 'notion_tax_rate_ref_error':
      return { message: '税率の Notion 計算式がエラーです', notionField: '税率', severity: 'must' };
    case 'notion_tax_rate_unknown_value':
      return { message: '税率の値が想定外です', notionField: '税率', severity: 'must' };
    case 'rakuten_tax_rate_missing':
      return { message: '楽天側の税率が取得できません', notionField: null, severity: 'check' };
    case 'rakuten_tax_rate_invalid':
      return { message: '楽天側の税率が不正です', notionField: null, severity: 'check' };
    case 'tax_rate_mismatch': {
      // tax_rate_mismatch:notion=0.1,rakuten=0.08 → 「Notion: 10%、 楽天: 8%」
      const detail = rawReason.slice(head.length + 1);
      const notion = detail.match(/notion=([\d.]+)/)?.[1];
      const rakuten = detail.match(/rakuten=([\d.]+)/)?.[1];
      const fmt = (v) => (v ? `${Math.round(parseFloat(v) * 100)}%` : '？');
      return {
        message: `税率が Notion と楽天で食い違っています (Notion: ${fmt(notion)} / 楽天: ${fmt(rakuten)})`,
        notionField: '税率',
        severity: 'must',
      };
    }
    case 'image_preflight_not_run':
      return { message: '商品画像の確認が終わっていません', notionField: null, severity: 'check' };
    case 'image_upload_failed':
      return { message: 'Yahoo 用の商品画像アップロードに失敗しました', notionField: '画像', severity: 'must' };
    case 'variation_result_missing':
      return { message: 'バリエーション情報の解析に失敗しました', notionField: null, severity: 'must' };
    case 'variation_conflict':
      return { message: 'バリエーション情報に矛盾があります (Notion の「バリエーション有無」 と楽天が食い違ってます)', notionField: 'バリエーション有無', severity: 'must' };
    case 'variation_subcode_errors':
      return { message: 'バリエーションの商品コード形式に問題があります', notionField: 'バリエーション', severity: 'must' };
    case 'lead_time_preflight_not_run':
      return { message: '発送日設定の確認が終わっていません', notionField: null, severity: 'check' };
    case 'lead_time_invalid':
      return { message: '発送日設定が Yahoo に登録できない値です', notionField: null, severity: 'must' };
    case 'variation_spec_category_unknown':
      return { message: 'バリエーション項目を判定するための Yahoo カテゴリが未設定です', notionField: 'カテゴリ', severity: 'must' };
    case 'variation_spec_table_missing':
      return { message: 'バリエーション項目マスタが未設定です (管理者作業必要)', notionField: null, severity: 'must' };
    case 'variation_spec_db_missing':
      return { message: 'バリエーション項目マスタ DB に接続できません', notionField: null, severity: 'must' };
    case 'variation_spec_unresolved': {
      const axes = rawReason.slice(head.length + 1);
      return {
        message: `Yahoo カテゴリに対応するバリエーション項目が未設定です: ${axes}`,
        notionField: 'バリエーション項目',
        severity: 'must',
      };
    }
    case 'variation_axes_missing':
      return { message: 'バリエーション項目が楽天側で取得できません', notionField: null, severity: 'check' };
    case 'rakuten_fetch_failed':
      return { message: '楽天から商品データを取得できませんでした', notionField: null, severity: 'check' };
    case 'rakuten_item_not_found':
      return { message: '楽天に この商品コードが存在しません', notionField: null, severity: 'must' };
    case 'persist_failed':
      return { message: '内部状態の保存に失敗しました', notionField: null, severity: 'must' };
    default:
      // Codex E-6 R1 Medium-2: 未知 reason は内部用語をそのまま出さない
      //   業務担当者に文脈不明な英語 reason を見せない、 管理者向け詳細は別途 (details モーダル) で出す。
      return {
        message: '確認が必要な項目があります (管理者に連絡してください)',
        notionField: null,
        severity: 'check',
        _rawForAdmin: rawReason,
      };
  }
}

/**
 * reasons[] → 表示用にまとめる。
 * 重複した notionField はまとめて一覧化。
 */
export function summarizeReasons(rawReasons) {
  if (!Array.isArray(rawReasons) || rawReasons.length === 0) {
    return { items: [], notionFields: [], primaryMessage: '' };
  }
  const items = rawReasons.map(translateReason);
  const notionFieldSet = new Set();
  for (const r of items) if (r.notionField) notionFieldSet.add(r.notionField);
  return {
    items,
    notionFields: [...notionFieldSet],
    primaryMessage: items[0]?.message || '',
  };
}

/**
 * カテゴリー分類: 「売価未入力」 「タイトル修正」 等で集計するための bucket。
 * 不備種類別入口で 「売価未入力 45 件」 と表示するための材料。
 */
const CATEGORY_LABELS = {
  'Yahoo!タイトル':    'タイトル未入力',
  '売価':              '売価未入力',
  '配送方法':          '配送方法未設定',
  '税率':              '税率の問題',
  'カテゴリ':          'カテゴリ未設定',
  '画像':              '画像の問題',
  'バリエーション':    'バリエーションの問題',
  'バリエーション有無': 'バリエーションの問題',
  'バリエーション項目': 'バリエーションの問題',
};

export function categorizeReason(rawReason) {
  const r = translateReason(rawReason);
  if (!r.notionField) return 'その他';
  return CATEGORY_LABELS[r.notionField] || 'その他';
}
