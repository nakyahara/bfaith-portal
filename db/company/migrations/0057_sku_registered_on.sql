-- 0057: 商品の登録日 (core.skus.registered_on。2026-10-05 中原さんの要望「商品登録日を持たせてほしい。既存の商品は NE のデータを取得して」)
--
-- 何を持つか: SKU (NE の商品コードの粒度) ごとに「いつ登録した商品か」= JST の日付 1 つと、その出どころ 1 つ。
--   registered_on        date  = 登録日 (JST の暦日。時刻は持たない = 一覧で並べる・絞るのは日付で足りる。NE の作成日の時刻は NE の画面で見る)
--   registered_on_source text  = 出どころ
--     'ne'         = NE の商品マスタの作成日 (goods_creation_date → raw_ne_products.作成日 → 商品管理リストの公開 snapshot の 登録日。夜間ロードが入れる)
--     'portal'     = ポータルの新商品の登録 (apps/master-edit/new・ops.register_new_sku) で作った日 (この列の既定値 = 作った取引の JST の今日)
--     'first_seen' = 夜間ロードが NE で初めて見て SKU を作った日 (NE の作成日が取れないセット・例外の SKU だけ。NE のセットの API の作成日は今は取っていない)
-- 決まり:
--   ・一度入った値は変えない (下の trigger)。夜間ロードは registered_on が空の行だけを埋める (NE の値が後から変わっても上書きしない)。
--     ポータルで登録した商品は、あとで NE に登録しても (NE の作成日 = CSV を取り込んだ日) ポータルの日のまま
--   ・既にある行は空のまま (この migration では埋めない)。適用の後の最初の夜間ロードが、商品管理リストの公開 snapshot の 登録日 から単品を埋める
--     (2026-10-05 の本番: 単品 5,059 件は全部 NE の作成日あり・セット 2,229 件と例外 89 件は無い = 空のまま = 画面は「—」)
--   ・持ち主表 (config/master-ownership.mjs) のキーにはしない = 'load' / 'company' のどちらでも同じ「空なら 1 回だけ入れる」(切替の epoch の対象の外)
--   ・画面のロール master_edit はこの列を UPDATE できない (create-master-edit-roles.mjs の列の権限に入れない)。読むのは表の SELECT で読める
--   ・変更の記録 (0026 の events.master_change_events) と version は、ほかの列と同じに付く (最初の夜間ロードで単品 ~5,000 件 × 2 列の UPDATE の記録が 1 回だけ増える)
-- 直すとき (間違った値を直す保守): 持ち主が trg_skus_registered_on_fixed を disable → 直す → enable (同じ取引で)。アプリからは直さない

alter table core.skus
  add column registered_on date,
  add column registered_on_source text;

alter table core.skus
  add constraint ck_skus_registered_on_source check (registered_on_source in ('ne', 'portal', 'first_seen')),
  add constraint ck_skus_registered_on_pair check ((registered_on is null) = (registered_on_source is null)),
  add constraint ck_skus_registered_on_range check (registered_on is null or registered_on >= date '2000-01-01');

-- 既にある行は空のまま (上の add column)。これから足す行の既定 = 作った取引の JST の今日・ポータルで登録 (列を書かない INSERT = ops.register_new_sku)。
--   夜間ロード (apps/company-db/load/engine.mjs) は 2 つの列を必ず明示して入れる (NE の作成日 / 初めて見た日 / 空)
alter table core.skus
  alter column registered_on set default core.jst_date(now()),
  alter column registered_on_source set default 'portal';

comment on column core.skus.registered_on is '登録日 (JST の日付)。一度入ったら変えない。出どころは registered_on_source (ne = NE の作成日 / portal = ポータルで登録した日 / first_seen = 夜間ロードが初めて見た日)。空 = 分からない (0057 の前からあるセット・例外など)';
comment on column core.skus.registered_on_source is '登録日の出どころ: ne / portal / first_seen (registered_on と同時に空 / 値あり)';

-- 一度入った登録日は変えない (空 → 値 だけ通す)。夜間ロードの「空の行だけ」の決まりを DB でも守る
create function core.guard_sku_registered_on() returns trigger language plpgsql set search_path = pg_catalog, core, pg_temp as $$
begin
  if old.registered_on is not null
     and (new.registered_on is distinct from old.registered_on or new.registered_on_source is distinct from old.registered_on_source) then
    raise exception 'registered_on_fixed: SKU % の登録日 (% · %) は一度入ったら変えない', old.code, old.registered_on, old.registered_on_source
      using errcode = '23514';
  end if;
  return new;
end $$;
create trigger trg_skus_registered_on_fixed before update of registered_on, registered_on_source on core.skus
  for each row execute function core.guard_sku_registered_on();

