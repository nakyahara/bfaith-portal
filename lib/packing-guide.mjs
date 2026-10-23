/**
 * packing-guide.mjs — 商品 (単品) の梱包指示 = 入れ方の写真 (3 枚まで・順番と一言) と注意点の文 (中原さん 2026-10-23・Company DB の 0101)
 *
 * 中原さん「梱包が複雑になる商品が増えてきたので、どう袋や箱に入れたかの写真を 3 枚ぐらいと、梱包時の注意点を入れたい。
 *           商品マスター (master-edit) からも、梱包システムからカメラでそのまま登録したい」
 *   - 正 = Company DB (core.sku_packing_guides・core.sku_packing_photos・記録 ops.sku_packing_guide_events)。書くのは DB の関数だけ
 *     (ops.add_sku_packing_photo = 写真を 1 枚足す / ops.set_sku_packing_guide = 注意点の文と写真の並び・一言を保存する)
 *   - 写真は「入れ方の例」(使う資材は梱包の資材カードが正)。現場で撮ったものも確認なしですぐ使う (記録は残す)
 *   - 写真はここで JPEG (長辺 1280px・EXIF の向きを直して位置情報などは捨てる) に作り直してから DB に渡す (画面が縮めて送っても、もう一度)
 *   - 梱包アプリ (miniPC) は Render の配りの口 (apps/packing-guide/api-router.mjs) から読むだけの写しを取る
 * 0101 の前の DB・ロールの流し直しの前 (master_edit に実行・読む権限が無い) = packingGuideReady が false = 画面は欄を出さない・書き込みは 503
 */
import crypto from 'node:crypto';

export const MAX_PHOTOS = 3;
export const NOTE_MAX = 2000;
export const CAPTION_MAX = 100;
/** 作り直した写真の長辺 (px) と、DB に入れる 1 枚の上限 (DB の check と同じ) */
export const PHOTO_LONG_EDGE = 1280;
export const PHOTO_MAX_BYTES = 1_500_000;
/** 受け取る写真 (作り直す前) の上限。画面は縮めて送る (長辺 1600px の JPEG = 1 MB 前後) ので、縮められなかったときの余裕 */
export const UPLOAD_MAX_BYTES = 12 * 1024 * 1024;
/** 作り直す前の画素の上限 (巨大な画像で Render のメモリを使い切らない) */
const INPUT_MAX_PIXELS = 50_000_000;
export const VIAS = Object.freeze(['sku_page', 'packing']);
export const GUIDE_FUNCTIONS = Object.freeze({
  add: 'ops.add_sku_packing_photo(uuid, text, text, bigint, bytea, integer, integer, text)',
  set: 'ops.set_sku_packing_guide(uuid, text, text, bigint, bigint, text, jsonb)',
});
export const GUIDE_TABLES = Object.freeze(['core.sku_packing_guides', 'core.sku_packing_photos', 'ops.sku_packing_guide_events']);

export class PackingGuideError extends Error {
  constructor(status, reason, message, extra = {}) {
    super(message);
    this.code = 'PACKING_GUIDE';
    this.status = status;
    this.reason = reason;
    this.extra = extra;
  }
}
const bad = (message, reason = 'invalid_input') => new PackingGuideError(400, reason, message);

/** 0101 があって、この接続のロールが表を読めて関数を実行できるか (ロールの流し直しの前 = false) */
export async function packingGuideReady(db) {
  try {
    const r = (await db.query(`select (select bool_and(to_regclass(t) is not null and has_table_privilege(current_user, to_regclass(t), 'select')) from unnest($1::text[]) as t)
        and (select bool_and(to_regprocedure(f) is not null and has_function_privilege(current_user, to_regprocedure(f), 'execute')) from unnest($2::text[]) as f) as ok`,
    [GUIDE_TABLES, Object.values(GUIDE_FUNCTIONS)])).rows[0];
    return r?.ok === true;
  } catch {
    return false;
  }
}

/** 商品コード → SKU (会社 1)。無い = null */
export async function skuForGuide(db, code) {
  const r = (await db.query(`select k.sku_id::text as sku_id, k.code, k.name, k.sku_kind, r.state as reg_state
      from core.skus k left join ops.master_registrations r on r.sku_id = k.sku_id
     where k.company_id = 1 and k.code_norm = core.norm_code($1)`, [String(code || '')])).rows[0];
  return r || null;
}

/** 今の指示 (写真の中身は読まない)。何も無い = { note: '', photos: [], event_id: null, ... } */
export async function readPackingGuide(db, skuId) {
  const [g, photos, last] = await Promise.all([
    db.query(`select note, updated_by, updated_via, updated_at from core.sku_packing_guides where sku_id = $1`, [skuId]),
    db.query(`select photo_id::text as photo_id, position, caption, sha256, width, height, added_by, added_via, added_at
        from core.sku_packing_photos where sku_id = $1 and position is not null order by position`, [skuId]),
    db.query(`select max(event_id)::text as event_id from ops.sku_packing_guide_events where sku_id = $1`, [skuId]),
  ]);
  const row = g.rows[0] || null;
  return {
    note: row?.note || '',
    photos: photos.rows.map((p) => ({ ...p, position: Number(p.position), width: Number(p.width), height: Number(p.height) })),
    event_id: last.rows[0]?.event_id || null,
    updated_by: row?.updated_by || null,
    updated_via: row?.updated_via || null,
    updated_at: row?.updated_at || null,
  };
}
/** 指示があるか (注意点の文か写真) */
export const hasGuide = (g) => !!g && (String(g.note || '').trim() !== '' || (Array.isArray(g.photos) && g.photos.length > 0));

/** 写真の中身 (外した写真も読める = 前の版)。skuId を渡すとその商品の写真だけ。無い = null */
export async function readPackingPhoto(db, photoId, { skuId = null } = {}) {
  if (!/^[1-9]\d{0,17}$/.test(String(photoId))) return null;
  const r = (await db.query(`select image, sha256, sku_id::text as sku_id from core.sku_packing_photos where photo_id = $1${skuId != null ? ' and sku_id = $2' : ''}`,
    skuId != null ? [String(photoId), String(skuId)] : [String(photoId)])).rows[0];
  if (!r) return null;
  return { image: Buffer.from(r.image), sha256: r.sha256, sku_id: r.sku_id };
}

/**
 * 写真を作り直す: EXIF の向きを直す → 長辺 1280px まで縮める (大きくはしない) → JPEG (メタデータは付けない = 位置情報などは捨てる)。
 * 画像として読めない・大きすぎる = 400
 */
export async function normalizePhoto(buf) {
  if (!Buffer.isBuffer(buf) || buf.length === 0) throw bad('写真が空です');
  if (buf.length > UPLOAD_MAX_BYTES) throw bad(`写真が大きすぎます (${Math.floor(UPLOAD_MAX_BYTES / 1024 / 1024)} MB まで)`, 'too_large');
  const { default: sharp } = await import('sharp');
  let out;
  try {
    out = await sharp(buf, { limitInputPixels: INPUT_MAX_PIXELS, failOn: 'error' })
      .rotate()
      .resize({ width: PHOTO_LONG_EDGE, height: PHOTO_LONG_EDGE, fit: 'inside', withoutEnlargement: true })
      .jpeg({ quality: 80, mozjpeg: true })
      .toBuffer({ resolveWithObject: true });
  } catch (e) {
    throw bad(`写真として読めませんでした (JPEG・PNG・WebP で撮り直してください)`, 'bad_image');
  }
  if (out.data.length > PHOTO_MAX_BYTES) throw bad('写真を小さくできませんでした。撮り直してください', 'too_large');
  return { image: out.data, width: out.info.width, height: out.info.height, sha256: crypto.createHash('sha256').update(out.data).digest('hex') };
}

/** 画面が送る base64 (data URL の頭があってもよい) → Buffer */
export function decodeImageBase64(s) {
  if (typeof s !== 'string' || !s) throw bad('写真がありません');
  const body = s.replace(/^data:image\/[a-z0-9.+-]+;base64,/i, '');
  if (body.length > Math.ceil(UPLOAD_MAX_BYTES / 3) * 4 + 4) throw bad(`写真が大きすぎます (${Math.floor(UPLOAD_MAX_BYTES / 1024 / 1024)} MB まで)`, 'too_large');
  if (!/^[A-Za-z0-9+/]+={0,2}$/.test(body)) throw bad('写真の形が違います');
  return Buffer.from(body, 'base64');
}

/** 一言 (改行なし・100 字まで)・注意点 (2000 字まで) の形 */
export function captionIn(v) {
  const s = String(v ?? '').replace(/[\r\n\t]+/g, ' ').trim();
  if (s.length > CAPTION_MAX) throw bad(`写真の一言は ${CAPTION_MAX} 字までにしてください`);
  if (/[\u0000-\u001F\u007F]/.test(s)) throw bad('写真の一言に使えない文字があります');
  return s;
}
export function noteIn(v) {
  const s = String(v ?? '').replace(/\r\n?/g, '\n');
  if (s.length > NOTE_MAX) throw bad(`注意点は ${NOTE_MAX} 字までにしてください`);
  if (/[\u0001-\u0008\u000B\u000C\u000E-\u001F\u007F]/.test(s)) throw bad('注意点に使えない文字があります');
  return s.trim() === '' ? '' : s;
}
export function requestIdIn(v) {
  const s = String(v || '');
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(s)) throw bad('request_id の形が違います');
  return s.toLowerCase();
}
/** 保存の本文 { request_id, seen_event_id, note, photos: [{ photo_id, caption }] } の形 */
export function guideSaveIn(body) {
  if (!body || typeof body !== 'object' || Array.isArray(body)) throw bad('本文の形が違います');
  const seen = body.seen_event_id == null || body.seen_event_id === '' ? null : String(body.seen_event_id);
  if (seen != null && !/^[1-9]\d{0,17}$/.test(seen)) throw bad('seen_event_id の形が違います');
  if (!Array.isArray(body.photos) || body.photos.length > MAX_PHOTOS) throw bad(`写真の並びは ${MAX_PHOTOS} つまでです`);
  const photos = body.photos.map((p) => {
    if (!p || typeof p !== 'object' || !/^[1-9]\d{0,17}$/.test(String(p.photo_id ?? ''))) throw bad('写真の並びの形が違います');
    return { photo_id: String(p.photo_id), caption: captionIn(p.caption) };
  });
  if (new Set(photos.map((p) => p.photo_id)).size !== photos.length) throw bad('同じ写真が 2 回あります');
  return { requestId: requestIdIn(body.request_id), seenEventId: seen, note: noteIn(body.note), photos };
}

/** DB の関数の断り → PackingGuideError (知らない誤りはそのまま投げる) */
function fromDb(e) {
  const msg = String(e?.message || '');
  const m = /^(invalid_input|not_single|cancelled|photos_full|request_reused|unknown_photo|version_conflict): ?([\s\S]*)$/.exec(msg);
  if (!m) return e;
  const words = {
    invalid_input: [400, `入れた値の形が違います (${m[2]})`],
    not_single: [400, '梱包指示はセットではなく単品の商品に入れてください (梱包のときにセットは構成品に分かれています)'],
    cancelled: [409, '登録をやめた商品には入れられません'],
    photos_full: [409, `写真はもう ${MAX_PHOTOS} 枚あります。どれかを外してから足してください`],
    request_reused: [409, '同じ操作の番号がほかで使われました。画面を開き直してください'],
    unknown_photo: [409, '画面を開いた後に写真が変わりました。何も保存していません。画面を開き直してください'],
    version_conflict: [409, '画面を開いた後に、ほかの人がこの商品の梱包指示を変えました。何も保存していません。画面を開き直してください'],
  }[m[1]];
  return new PackingGuideError(words[0], m[1], words[1]);
}

/** 写真を 1 枚足す (作り直した写真を渡す)。戻り値 = DB の関数の答え */
export async function addPackingPhoto(db, { requestId, actor, via, skuId, photo, caption = '' }) {
  if (!VIAS.includes(via)) throw bad('via の形が違います');
  try {
    const r = (await db.query(`select ops.add_sku_packing_photo($1::uuid, $2, $3, $4::bigint, $5::bytea, $6::integer, $7::integer, $8) as r`,
      [requestIdIn(requestId), String(actor || ''), via, String(skuId), photo.image, photo.width, photo.height, captionIn(caption)])).rows[0].r;
    return typeof r === 'string' ? JSON.parse(r) : r;
  } catch (e) { throw fromDb(e); }
}

/** 注意点の文と写真の並び・一言を保存する。戻り値 = DB の関数の答え */
export async function savePackingGuide(db, { requestId, actor, via, skuId, seenEventId, note, photos }) {
  if (!VIAS.includes(via)) throw bad('via の形が違います');
  try {
    const r = (await db.query(`select ops.set_sku_packing_guide($1::uuid, $2, $3, $4::bigint, $5::bigint, $6, $7::jsonb) as r`,
      [requestIdIn(requestId), String(actor || ''), via, String(skuId), seenEventId, note, JSON.stringify(photos)])).rows[0].r;
    return typeof r === 'string' ? JSON.parse(r) : r;
  } catch (e) { throw fromDb(e); }
}

/**
 * 配り (梱包アプリの写し): 指示がある単品の全部 (注意点の文か今の写真がある・登録をやめた商品は出さない)。写真の中身は入れない (番号と sha256 で別に取る)
 * 戻り値 = [{ code, sku_id, name, note, event_id, updated_at, updated_by, photos: [{ photo_id, sha256, position, caption, width, height }] }] (コードの順)
 */
export async function listGuidesForExport(db) {
  const rows = (await db.query(`
    with g as (
      select k.sku_id, k.code, k.name, coalesce(x.note, '') as note, x.updated_at, x.updated_by
        from core.skus k
        left join core.sku_packing_guides x on x.sku_id = k.sku_id
        left join ops.master_registrations r on r.sku_id = k.sku_id
       where k.company_id = 1 and k.sku_kind = 'single' and r.state is distinct from 'cancelled'
         and (x.note <> '' or exists (select 1 from core.sku_packing_photos p where p.sku_id = k.sku_id and p.position is not null))
    )
    select g.sku_id::text as sku_id, g.code, g.name, g.note, g.updated_at, g.updated_by,
           (select max(e.event_id)::text from ops.sku_packing_guide_events e where e.sku_id = g.sku_id) as event_id,
           coalesce((select json_agg(json_build_object('photo_id', p.photo_id::text, 'sha256', p.sha256, 'position', p.position, 'caption', p.caption,
                                                       'width', p.width, 'height', p.height) order by p.position)
                       from core.sku_packing_photos p where p.sku_id = g.sku_id and p.position is not null), '[]'::json) as photos
      from g order by g.code`)).rows;
  return rows.map((r) => ({ ...r, photos: typeof r.photos === 'string' ? JSON.parse(r.photos) : r.photos }));
}
