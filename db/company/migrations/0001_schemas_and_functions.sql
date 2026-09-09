-- 0001 スキーマと共通関数 (Company DB構想 03 §1 P-11 / 06 §5.1)
--
-- 層 = raw (取ったまま) / core (解決済みの今) / snapshots (日次) / events (変化、append-only)
--      / ai (判断・所見) / docs (文書台帳) / ops (運用) / mart (AI と画面が読む層)
-- ops.schema_migrations は scripts/company-db/migrate.mjs が bootstrap で作る (ここでは作らない)。
--
-- 規約 (03 §1):
--   P-1  金額は bigint (円)。列名は *_jpy
--   P-2  時刻は timestamptz (UTC 保存)。業務日は JST の date (core.jst_date)
--   P-3  外部コードの比較は正規化列 (code_norm)。正規化は lib/sku-norm.js normSku() と同じ (core.norm_code)
--   P-4  raw は append-only。content_hash に volatile 列を入れない
--   P-6  イベントは append-only + idempotency_key unique
--   P-9  Canonical 表は監査列 created_at / created_by_type / created_by_id / updated_at

create schema if not exists raw;
create schema if not exists core;
create schema if not exists snapshots;
create schema if not exists events;
create schema if not exists ai;
create schema if not exists docs;
create schema if not exists ops;
create schema if not exists mart;

-- ─────────────────────────────────────────────────────────────
-- core.norm_code(text) = lib/sku-norm.js normSku() の SQL 版
--   1. 全角英数記号 (U+FF01〜U+FF5E) → 半角 / 全角スペース (U+3000) → 半角スペース
--   2. 各種ダッシュ (U+2212, U+2010〜U+2015, U+FE58, U+FE63) → '-'   (U+FF0D は 1 で '-' になる)
--   3. 空白 (Unicode の空白類を含む) を全部除去
--   4. 小文字化
--   🚨 NFKC 正規化は使わない (半角カナ→全角カナ など JS がしない変換が入り、JS と食い違う)。
--      JS と SQL の一致は scripts/test-company-db-ddl.mjs が固定する (03 §10)
--   ※ null は null (JS は '' を返す。null を比較に使わないので差は許容)
-- ─────────────────────────────────────────────────────────────
create or replace function core.fullwidth_ascii_src() returns text language sql immutable as $$
  select string_agg(chr(c), '' order by c) from generate_series(65281, 65374) as c
$$;
create or replace function core.fullwidth_ascii_dst() returns text language sql immutable as $$
  select string_agg(chr(c - 65248), '' order by c) from generate_series(65281, 65374) as c
$$;
create or replace function core.norm_code(p text) returns text language sql immutable strict as $$
  select lower(
    regexp_replace(
      translate(
        translate(replace(p, chr(12288), ' '), core.fullwidth_ascii_src(), core.fullwidth_ascii_dst()),
        chr(8722) || chr(8208) || chr(8209) || chr(8210) || chr(8211) || chr(8212) || chr(8213) || chr(65112) || chr(65123),
        '---------'
      ),
      '[\s\u00a0\u2000-\u200a\u202f\u205f\u3000\ufeff]', '', 'g'
    )
  )
$$;

-- JST の業務日 (D-13)。UTC で保存した時刻を「日本の何日か」に直す
create or replace function core.jst_date(p timestamptz) returns date language sql immutable strict as $$
  select (p at time zone 'Asia/Tokyo')::date
$$;

-- updated_at を自動で進める (更新のたびに now())
create or replace function core.touch_updated_at() returns trigger language plpgsql as $$
begin
  new.updated_at := now();
  return new;
end
$$;
