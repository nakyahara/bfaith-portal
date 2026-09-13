/**
 * カードの内容を別のカードへコピーする (2026-09-13 スタッフ要望・中原さん決定「別のカードへ内容をコピー」)。
 *
 * 容量違いの商品は情報がほぼ同じなので、入力し終えたカードの内容を、自動で入った別の商品コードの
 * カードへ**上書き**する。自動で入ったカードをそのまま使うので、削除は要らない
 * (同じ商品コードのカードは 1 枚しか作れないので「複製して元を消す」より事故が少ない)。
 *
 * コピーする: 公式ページURL / 参考URL / 仕様表 (数量で変わる行は除く) / 商品ページ表記 (許可リスト) /
 *   楽天のジャンル・属性 (数量で変わる属性は除く)・カタログIDなしの理由 / 店舗内カテゴリ /
 *   Yahoo! のカテゴリ / タイトル・説明文 (AI の出力。人が直した扱いにして AI に上書きさせない) /
 *   自社商品・画像の重要度 (対で動く値なので両方)
 * コピーしない: 商品名・商品コード・JAN・メーカー型番・売価・ASIN / Amazon URL・画像・画像制作・
 *   配送方法・税率・SKU の表・工程・モール別状況 (商品ごとに違う / 実物で決まる)
 * 数量で変わる値 (内容量・サイズ・食品表示・仕様表や属性の「容量」行) はコピー先の値を残す。
 * 考え方はセット商品の派生 (services/set-derive.js の copy) と同じ。あちらは新しいカードへ、こちらは既存のカードへ書く
 */
import { logEvent, upsertDraftYahoo } from '../db.js';
import { isQuantityDependentSpec } from './set-derive.js';

function httpError(status, message) {
  const e = new Error(message);
  e.status = status;
  return e;
}

/** 商品ページ表記で数量に依存しない列 (set-derive の許可リストと同じ) */
const PAGE_INFO_COPY_COLS = [
  'product_type', 'brand_name', 'ingredients', 'usage_notes', 'other_notes', 'origin_type', 'origin_country',
  'category_label', 'seller_name', 'importer_name',
];

/**
 * 人の確認が入る工程。コピー先でこれらが済んで (完了・対象外) いたら上書きしない (Codex R1 P1):
 * 確認済みのカードの内容を差し替えても工程は戻らないので、容量などを直す前に「準備完了」として
 * ボードから出品できてしまう。コピー先は、内容の確認に入る前のカード (自動で入ったばかり / AI が書いた直後) に限る
 */
const REVIEWED_STEPS = ['desc_review', 'title_approve', 'set_review', 'listing'];

/** 出品済み (楽天に登録済み / 出品中・結果不明 / 他モールで展開済み) か。上書き先・削除の可否に使う */
export function listedReason(db, draftId) {
  const rk = db.prepare('SELECT registered_at, listing_outcome FROM draft_rakuten WHERE draft_id = ?').get(draftId);
  if (rk?.registered_at) return '楽天に出品済みです';
  if (rk?.listing_outcome === 'running') return '楽天への出品の最中です';
  if (rk?.listing_outcome === 'unknown') return '楽天への出品の結果が分からない状態です (RMS で確認してください)';
  const mall = db.prepare("SELECT mall FROM draft_mall_status WHERE draft_id = ? AND state = 'done' LIMIT 1").get(draftId);
  if (mall) return `モール (${mall.mall}) に展開済みです`;
  return null;
}

/**
 * @returns {{ targetId: number, targetNeCode: string, refs: number, specs: number, droppedSpecs: number,
 *   droppedAttrs: number, aiOutputs: number, pageInfo: boolean, rakuten: boolean, shopCategories: number }}
 */
export function copyDraftContent(db, sourceId, targetNeCode, actor) {
  const code = String(targetNeCode ?? '').trim();
  if (!code) throw httpError(400, 'コピー先の商品コードを入れてください');
  const run = db.transaction(() => {
    const src = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(sourceId);
    if (!src) throw httpError(404, 'コピー元のカードが見つかりません');
    const dst = db.prepare('SELECT * FROM product_drafts WHERE LOWER(TRIM(ne_code)) = ?').get(code.toLowerCase());
    if (!dst) throw httpError(404, `商品コード「${code}」のカードがありません。先に「新規登録」で作ってください`);
    if (dst.id === src.id) throw httpError(400, 'コピー元と同じカードです。コピー先には別の商品コードを入れてください');
    // 出品済みの商品の内容は上書きしない (出品後の説明文・属性を黙って差し替えると、次の更新で楽天まで変わる)
    const listed = listedReason(db, dst.id);
    if (listed) throw httpError(400, `コピー先「${dst.ne_code}」は${listed}。出品済みのカードの内容は上書きしません`);
    // セット商品は工程も説明文の作り方も別 (構成・仮コード) なので、単品の内容で上書きしない
    if (dst.parent_draft_id != null) throw httpError(400, `コピー先「${dst.ne_code}」はセット商品です。セット商品へはコピーできません`);
    const reviewed = db.prepare(`
      SELECT s.label FROM draft_step_progress p JOIN ph_steps s ON s.code = p.step_code
      WHERE p.draft_id = ? AND p.step_code IN (${REVIEWED_STEPS.map(() => '?').join(', ')}) AND p.state IN ('done', 'skip')
      ORDER BY s.sort LIMIT 1
    `).get(dst.id, ...REVIEWED_STEPS);
    if (reviewed) {
      throw httpError(400, `コピー先「${dst.ne_code}」は工程「${reviewed.label}」が済んでいます。確認済みのカードの内容は上書きしません`
        + ' (コピー先は、内容の確認に入る前のカードにしてください)');
    }
    const now = new Date().toISOString();

    // 公式ページURL と、対で動く 自社商品・画像の重要度 (own_brand=1 ⟺ 重要度=自社商品 の不変条件を崩さない)
    db.prepare(`
      UPDATE product_drafts SET official_url = ?, own_brand = ?, image_priority = ?,
        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
      WHERE id = ?
    `).run(src.official_url, src.own_brand, src.image_priority, dst.id);

    // 参考URL: 丸ごと入れ替え
    db.prepare('DELETE FROM draft_reference_urls WHERE draft_id = ?').run(dst.id);
    const refs = db.prepare(`
      INSERT INTO draft_reference_urls (draft_id, url, sort) SELECT ?, url, sort FROM draft_reference_urls WHERE draft_id = ?
    `).run(dst.id, src.id).changes;

    // 仕様表: 数量で変わる行 (サイズ・容量・入数…) はコピー先の行を残し、それ以外を入れ替える
    db.prepare('SELECT id, spec_key FROM draft_specs WHERE draft_id = ?').all(dst.id)
      .filter((r) => !isQuantityDependentSpec(r.spec_key))
      .forEach((r) => db.prepare('DELETE FROM draft_specs WHERE id = ?').run(r.id));
    let specs = 0; let droppedSpecs = 0;
    const insSpec = db.prepare('INSERT INTO draft_specs (draft_id, spec_key, spec_value, sort) VALUES (?, ?, ?, ?)');
    for (const sp of db.prepare('SELECT spec_key, spec_value, sort FROM draft_specs WHERE draft_id = ? ORDER BY sort, id').all(src.id)) {
      if (isQuantityDependentSpec(sp.spec_key)) { droppedSpecs++; continue; }
      insSpec.run(dst.id, sp.spec_key, sp.spec_value, sp.sort);
      specs++;
    }

    // 🚨 以下の「上書き」は、コピー元に行が無い項目もコピー先を空にする (Codex R1 P2: 飛ばすとコピー先の古い値が
    // 残り、コピー元とコピー先の中身が混ざる)。残すのは数量で変わる値 (内容量・サイズ・食品表示・容量の行) だけ

    // 商品ページ表記: 数量に依存しない列だけ上書き (内容量・サイズ・食品表示はコピー先の値を残す)。
    // コピー元に行が無ければ、その列を空にする (商品タイプは NOT NULL なので既定の general)
    const pinfo = db.prepare('SELECT * FROM draft_page_info WHERE draft_id = ?').get(src.id);
    const dstHasPinfo = !!db.prepare('SELECT 1 FROM draft_page_info WHERE draft_id = ?').get(dst.id);
    if (pinfo || dstHasPinfo) {
      const vals = PAGE_INFO_COPY_COLS.map((c) => (pinfo ? pinfo[c] : (c === 'product_type' ? 'general' : null)) ?? (c === 'product_type' ? 'general' : null));
      db.prepare(`
        INSERT INTO draft_page_info (draft_id, ${PAGE_INFO_COPY_COLS.join(', ')})
        VALUES (?, ${PAGE_INFO_COPY_COLS.map(() => '?').join(', ')})
        ON CONFLICT(draft_id) DO UPDATE SET
          ${PAGE_INFO_COPY_COLS.map((c) => `${c} = excluded.${c}`).join(', ')},
          updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
      `).run(dst.id, ...vals);
    }

    // 楽天: ジャンル・属性・カタログIDなしの理由。属性も数量で変わるもの (容量 等) はコピー先の値を残す。
    // メーカー型番・配送方法・登録状態は商品ごとに違うので触らない。コピー元に行が無ければ空にする
    const srk = db.prepare('SELECT genre_id, attributes_json, catalog_id_exemption_reason FROM draft_rakuten WHERE draft_id = ?').get(src.id);
    const drk = db.prepare('SELECT attributes_json FROM draft_rakuten WHERE draft_id = ?').get(dst.id);
    let droppedAttrs = 0;
    if (srk || drk) {
      let srcAttrs = [];
      try { srcAttrs = JSON.parse(srk?.attributes_json || '[]'); } catch (_) { srcAttrs = []; }
      let dstAttrs = [];
      try { dstAttrs = JSON.parse(drk?.attributes_json || '[]'); } catch (_) { dstAttrs = []; }
      const keepDst = (Array.isArray(dstAttrs) ? dstAttrs : []).filter((a) => isQuantityDependentSpec(a?.name));
      const fromSrc = (Array.isArray(srcAttrs) ? srcAttrs : []).filter((a) => {
        if (isQuantityDependentSpec(a?.name)) { droppedAttrs++; return false; }
        return true;
      });
      const merged = [...fromSrc, ...keepDst.filter((k) => !fromSrc.some((s) => s?.name === k?.name))];
      db.prepare(`
        INSERT INTO draft_rakuten (draft_id, genre_id, attributes_json, catalog_id_exemption_reason)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(draft_id) DO UPDATE SET
          genre_id = excluded.genre_id, attributes_json = excluded.attributes_json,
          catalog_id_exemption_reason = excluded.catalog_id_exemption_reason
      `).run(dst.id, srk?.genre_id ?? null, JSON.stringify(merged), srk?.catalog_id_exemption_reason ?? null);
    }

    // 店舗内カテゴリ: 丸ごと入れ替え
    db.prepare('DELETE FROM draft_shop_categories WHERE draft_id = ?').run(dst.id);
    const shopCategories = db.prepare(`
      INSERT INTO draft_shop_categories (draft_id, shop_category_id, slot)
      SELECT ?, shop_category_id, slot FROM draft_shop_categories WHERE draft_id = ?
    `).run(dst.id, src.id).changes;

    // Yahoo!: カテゴリだけ (売価・配送・税率は商品ごとに違う)。コピー元に無ければ空にする
    const sy = db.prepare('SELECT yahoo_category_id, yahoo_path FROM draft_yahoo WHERE draft_id = ?').get(src.id);
    if (sy || db.prepare('SELECT 1 FROM draft_yahoo WHERE draft_id = ?').get(dst.id)) {
      upsertDraftYahoo(db, dst.id, { yahoo_category_id: sy?.yahoo_category_id ?? null, yahoo_path: sy?.yahoo_path ?? null });
    }

    // タイトル・説明文 (AI の出力)。人が直した扱い (edited_by_human=1) にして、コピー先で後から AI が
    // 動いても上書きさせない (コピーした文章を直して使うのが目的)。コピー元に無い種類はコピー先からも消す
    db.prepare(`
      DELETE FROM draft_ai_outputs WHERE draft_id = ? AND kind NOT IN (SELECT kind FROM draft_ai_outputs WHERE draft_id = ?)
    `).run(dst.id, src.id);
    const aiOutputs = db.prepare(`
      INSERT INTO draft_ai_outputs (draft_id, kind, content, generated_at, model_note, edited_by_human)
      SELECT ?, kind, content, ?, ?, 1 FROM draft_ai_outputs WHERE draft_id = ?
      ON CONFLICT(draft_id, kind) DO UPDATE SET
        content = excluded.content, generated_at = excluded.generated_at,
        model_note = excluded.model_note, edited_by_human = 1
    `).run(dst.id, now, `${src.ne_code} からコピー`, src.id).changes;

    const summary = {
      targetId: dst.id, targetNeCode: dst.ne_code,
      refs, specs, droppedSpecs, droppedAttrs, aiOutputs, pageInfo: !!pinfo, rakuten: !!srk, shopCategories,
    };
    const detail = `タイトル・説明文 ${aiOutputs} 件 / 参考URL ${refs} 件 / 仕様表 ${specs} 行`
      + (droppedSpecs ? ` (数量で変わる ${droppedSpecs} 行は除外)` : '')
      + (droppedAttrs ? ` / 数量で変わる属性 ${droppedAttrs} 件は除外` : '');
    logEvent(db, dst.id, 'content_copied_from', `${src.ne_code} から内容をコピー: ${detail}`, actor);
    logEvent(db, src.id, 'content_copied_to', `${dst.ne_code} へ内容をコピー: ${detail}`, actor);
    return summary;
  });
  return run();
}
