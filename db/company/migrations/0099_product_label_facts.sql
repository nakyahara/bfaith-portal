-- 0099 (仮の番号・マージの時に「すぐ次の番号」に付け替える。0084〜0088 は先に入る見込みのブランチが使う。migrate は欠番を断る):
--   商品 (単品) の表示の項目 = 原産国・内容量 (数 + 単位)・メーカー型番・消費期限の目安 (日数) (Company DB構想 27 ④-1・中原さんの決定 Q1〜Q9 = 全部 a・2026-10-18)
--   中原さん「法定表示・出品の項目 (原産国・内容量・メーカー型番・消費期限の目安) をマスタの入力で入れたい」= 正本は Company DB・product-hub は読んで見せる・人が「マスタの値を入れる」を押す
--
-- 考え方 (梱包の大きさ・重さ (0086 仮番号) と同じ形・設計 27 §3.4 = v2):
--   - 人が入れた値 = 🆕 core.product_label_facts (商品ごとに 1 行・項目ごとに空でよい)。夜間ロードは触らない
--     🚨 0003 の観測 + 解決 (source = master_edit の観測 + 規則の版 v2) にしない:
--        夜間ロードは規則の版を v1 に固定して読む (apps/company-db/load/engine.mjs の RULE_VERSION)・触った商品の内容量を毎晩 v1 で選び直して
--        core.products.net_content を書く = 人の観測を足しても次の夜に product-hub の値に戻る。v1 に規則は足せない (0003 の trg_rules_used_version)。
--        v2 に切り替えると JAN・ブランド… 全部の属性の解決の版が変わる (夜間ロードの動きが広く変わる)。観測は追記だけ = 人の値を「外す」も表せない
--   - まとめた値 = 🆕 core.v_product_label_facts (項目ごとに 人の値 → 無ければ出どころ)。いま使う値を読む人はここを読む (正式な読み口)
--       原産国・メーカー型番・消費期限の目安 = 人の値だけ (Company DB にほかの出どころは無い。product-hub の今の値は画面が product-hub から直に読んで見せる)
--       内容量 = 人の値 → 無ければ core.products.net_content (夜間ロードが product-hub などの観測から解決した値・出どころ = 解決に使った観測の source_system)
--   - core.products.country_of_origin / model_number・core.product_compliance.shelf_life_days (0002・0003 で作った列・書く人も読む人もいない) は書かない (空のまま)。
--     core.products.net_content と mart の内容量 (0007・0009・0077 の mart のビュー) = 今までどおり夜間ロードの値 (観測から解決した参考の値)。
--     人の値を含めた値は core.v_product_label_facts (列の注釈にも書く)。いま mart の内容量を読むコードは無い (repo の grep・2026-10-18)
--   - 空にして保存 = その項目の人の値を外す (まとめた値は出どころに戻る = 内容量は夜間ロードの値・ほかは空)。4 つとも空 = 行を消す (記録は残る)
--   - 単品だけ (セット・例外は商品の行を持たない = 構成品から・設計 27 Q8)。登録で必須にしない (「値か入力なし」にも数えない)。NE には送らない
--   - 消費期限の目安 = 製造 (入荷) からの日数 (Q9)。食品・化粧品など期限のある商品に入れる目安。Company DB に食品・化粧品の区分が無いので DB では絞らない
--     (画面の説明で分ける・ロジザードの期限管理 (core.products.expiry_managed)・ページに出す文 (product-hub の food_expiry) とは別の項目)
-- 単位: 内容量の単位 = ml / g / 枚 / 個 (lib/master-label.mjs の NET_UOMS と同じ)。数 = 0 より大きく、小数 2 けたまで (core.products.net_content と同じ numeric(10,2))
--
-- なにを:
--   1. core.product_label_facts = 人が入れた今の値 (1 行 = 1 商品・無い = 人の値なし・4 つとも空の行は持たない)
--   2. ops.product_label_fact_events = 入れた・変えた・外したの記録 (追記だけ・request_id と商品で一意)
--   3. ops.set_product_label_facts(...) = 人の値を入れる / 変える / 外す (新商品の登録・まとまりの登録 = 登録と同じ取引 / 商品の画面 = 保存と同じ取引)。
--      守りは梱包の大きさ・重さ (0086 ops.set_product_package)・メーカー (0084 ops.set_product_maker) と同じ: 単品・会社 1・やめていない・切替の段階 new_open・
--      門を通った保存 / 登録の取引の中・記録の request_id・人 = 約束と同じ・今の値が画面の見ていたもの (最後の記録の番号) と違う = version_conflict
--   4. core.v_product_label_facts (まとめた値)
-- 🚨 既にある表・関数・ビューは変えない (表・関数・ビューを足すだけ・既にある列には注釈だけ)。security definer = search_path = pg_catalog, pg_temp・public の実行権を外す。
--    画面のロール (master_edit) の権限 (表・ビューの select・書く関数の実行) は scripts/company-db/create-master-edit-roles.mjs を流し直して付ける
--    (流すまで画面は欄を出さない・保存に値が来たら 503 db_not_ready)

-- ═══════════ 1. 人の値・記録 ═══════════
create table core.product_label_facts (
  product_id        bigint primary key references core.products,
  company_id        smallint not null default 1 references core.companies,
  country_of_origin text check (country_of_origin is null or (pg_catalog.char_length(country_of_origin) between 1 and 40
                                 and country_of_origin = pg_catalog.btrim(country_of_origin) and country_of_origin !~ '[[:cntrl:]]' and country_of_origin !~ '製$')),
  net_content       numeric(10,2) check (net_content is null or net_content > 0),
  net_content_uom   text check (net_content_uom is null or net_content_uom in ('ml', 'g', '枚', '個')),
  model_number      text check (model_number is null or (pg_catalog.char_length(model_number) between 1 and 60
                                 and model_number = pg_catalog.btrim(model_number) and model_number !~ '[[:cntrl:]]')),
  shelf_life_days   integer check (shelf_life_days is null or shelf_life_days between 1 and 3650),
  set_by            text not null,
  set_via           text not null check (set_via in ('register', 'variation', 'sku_page')),
  request_id        uuid not null,
  set_at            timestamptz not null default now(),
  constraint ck_product_label_facts_net_pair check ((net_content is null) = (net_content_uom is null)),
  constraint ck_product_label_facts_any check (country_of_origin is not null or net_content is not null or model_number is not null or shelf_life_days is not null),
  foreign key (company_id, product_id) references core.products (company_id, product_id)
);
comment on table core.product_label_facts is '🆕 0099: 商品 (単品の商品の行) の表示の項目 (原産国・内容量・メーカー型番・消費期限の目安の日数) の人の値 (行が無い = 人の値なし)。夜間ロードは触らない。書くのは ops.set_product_label_facts だけ。いま使う値は core.v_product_label_facts';

create table ops.product_label_fact_events (
  event_id   bigint generated always as identity primary key,
  company_id smallint not null default 1 references core.companies,
  product_id bigint not null references core.products,
  sku_id     bigint not null references core.skus,
  from_value jsonb,   -- 前の人の値 { country_of_origin, net_content, net_content_uom, model_number, shelf_life_days } (無かった = null)
  to_value   jsonb,   -- 後の人の値 (外した = null)
  via        text not null check (via in ('register', 'variation', 'sku_page')),
  actor_id   text not null,
  request_id uuid not null,
  reason     text,
  db_user    text not null,
  created_at timestamptz not null default now(),
  constraint ck_plfe_changed check (from_value is distinct from to_value),
  unique (request_id, product_id)
);
create index ix_product_label_fact_events_product on ops.product_label_fact_events (product_id, event_id desc);
comment on table ops.product_label_fact_events is '🆕 0099: 表示の項目の人の値を入れた・変えた・外した記録 (追記だけ・書くのは ops.set_product_label_facts だけ)。商品の最後の記録の番号 = 画面が見ていた版';
revoke all on core.product_label_facts, ops.product_label_fact_events from public;

-- 守り: 書くのは関数の中だけ (印 ops.label_protocol)・記録は追記だけ・TRUNCATE できない
create function ops.guard_label_tables() returns trigger language plpgsql set search_path = pg_catalog, pg_temp as $$
begin
  if tg_table_name = 'product_label_fact_events' and tg_op <> 'INSERT' then
    raise exception 'product_label_fact_events: 記録は変えない・消さない (追記だけ)' using errcode = '42501';
  end if;
  if coalesce(pg_catalog.current_setting('ops.label_protocol', true), '') is distinct from '1' then
    raise exception '%: 表示の項目の表は ops.set_product_label_facts の中でだけ書く', tg_table_name using errcode = '42501';
  end if;
  return case when tg_op = 'DELETE' then old else new end;
end $$;
revoke all on function ops.guard_label_tables() from public;
create trigger trg_product_label_facts_guard before insert or update or delete on core.product_label_facts for each row execute function ops.guard_label_tables();
create trigger trg_product_label_fact_events_guard before insert or update or delete on ops.product_label_fact_events for each row execute function ops.guard_label_tables();
create trigger trg_product_label_facts_no_truncate before truncate on core.product_label_facts for each statement execute function core.reject_mutation();
create trigger trg_product_label_fact_events_no_truncate before truncate on ops.product_label_fact_events for each statement execute function core.reject_mutation();

-- ═══════════ 2. 人の値を入れる ═══════════
/**
 * p_country / p_net_content + p_net_content_uom / p_model_number / p_shelf_life_days = 入れる人の値 (項目ごとに null = その項目は人の値なし・全部 null = 人の値を外す)・
 * p_seen_event_id = 画面が見ていた最後の記録の番号 (無い = null。新商品の登録 = いつも null)・p_via = register / variation / sku_page・p_reason = 記録に残す理由
 * 戻り値 = { ok, sku_id, product_id, from, to, changed, event_id }
 * 断る: invalid_input (形・範囲・内容量の数と単位の片方だけ) / before_cutover / not_single (セット・例外・商品の行の無い SKU) / cancelled (登録をやめた) /
 *       master_write_session_required (門を通った保存・登録の取引の外) / version_conflict (今の人の値が画面の見ていたものと違う)
 * 押し直し: 画面の保存・登録の押し直しは呼び手 (ops.master_edit_requests の前の答え) が返す = ここには来ない (ふつうの再送は呼び手で扱う)。
 *   ここに 2 回目が来たときは、先に version_conflict か「変わりなし」になる。記録を 2 回書こうとしたら記録の一意 (request_id, 商品) で断る (unique_violation)
 * 🚨 読む側: 画面は人の値と最後の記録の番号を 1 つの SELECT で読む (lib/master-label.mjs の readHumanLabel・人の値の行が無くても番号は返す・Codex 設計 R2 M)
 */
create function ops.set_product_label_facts(p_request_id uuid, p_actor_id text, p_sku_id bigint, p_country text, p_net_content numeric, p_net_content_uom text,
    p_model_number text, p_shelf_life_days integer, p_seen_event_id bigint, p_via text, p_reason text) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_db_user text := case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;
  s         record;
  v_cur     core.product_label_facts;
  v_last    bigint;
  v_from    jsonb;
  v_to      jsonb;
  v_event   bigint;
  v_sess    ops.master_write_sessions;
begin
  if p_request_id is null or p_sku_id is null then raise exception 'invalid_input: request_id と SKU が要る' using errcode = '22023'; end if;
  if ops.reg_actor_problem(p_actor_id, p_reason) is not null or p_actor_id !~ '^[^[:space:]@]+@[^[:space:]@]+$' then
    raise exception 'invalid_input: 人 (メール)・理由 (200 字まで) の形が違う' using errcode = '22023';
  end if;
  if p_via is null or p_via not in ('register', 'variation', 'sku_page') then raise exception 'invalid_input: どこから は register / variation / sku_page' using errcode = '22023'; end if;
  if p_via in ('register', 'variation') and p_seen_event_id is not null then raise exception 'invalid_input: 新商品の登録に「見ていた記録」は無い' using errcode = '22023'; end if;
  if p_country is not null and (pg_catalog.char_length(p_country) not between 1 and 40 or p_country <> pg_catalog.btrim(p_country) or p_country ~ '[[:cntrl:]]' or p_country ~ '製$') then
    raise exception 'invalid_input: 原産国は 40 字までの国名 (前後の空白・改行なし・「製」を付けない)' using errcode = '22023';
  end if;
  if (p_net_content is null) <> (p_net_content_uom is null) then raise exception 'invalid_input: 内容量は数と単位をそろえて入れる (片方だけは入れない)' using errcode = '22023'; end if;
  if p_net_content is not null and (p_net_content <= 0 or p_net_content >= 100000000 or p_net_content <> pg_catalog.round(p_net_content, 2)) then
    raise exception 'invalid_input: 内容量は 0 より大きく、小数 2 けたまで' using errcode = '22023';
  end if;
  if p_net_content_uom is not null and p_net_content_uom not in ('ml', 'g', '枚', '個') then raise exception 'invalid_input: 内容量の単位は ml / g / 枚 / 個' using errcode = '22023'; end if;
  if p_model_number is not null and (pg_catalog.char_length(p_model_number) not between 1 and 60 or p_model_number <> pg_catalog.btrim(p_model_number) or p_model_number ~ '[[:cntrl:]]') then
    raise exception 'invalid_input: メーカー型番は 60 字まで (前後の空白・改行なし)' using errcode = '22023';
  end if;
  if p_shelf_life_days is not null and p_shelf_life_days not between 1 and 3650 then raise exception 'invalid_input: 消費期限の目安は 1〜3650 日' using errcode = '22023'; end if;
  if (select c.phase from ops.master_cutover_state c where c.id = 1) is distinct from 'new_open' then
    raise exception 'before_cutover: 切替の段階が new_open でない' using errcode = 'P0001';
  end if;
  select k.sku_id, k.company_id, k.sku_kind, k.product_id, r.state as reg into s
    from core.skus k left join ops.master_registrations r on r.sku_id = k.sku_id where k.sku_id = p_sku_id;
  if s.sku_id is null or s.company_id <> 1 then raise exception 'invalid_input: SKU % が Company DB に無い', p_sku_id using errcode = '22023'; end if;
  if s.sku_kind is distinct from 'single' or s.product_id is null then
    raise exception 'not_single: 表示の項目は単品の商品だけ (SKU %・セットは構成品から)', p_sku_id using errcode = '22023';
  end if;
  if s.reg = 'cancelled' then raise exception 'cancelled: 登録をやめた商品 (SKU %) には書かない', p_sku_id using errcode = '22023'; end if;
  -- 守り: 門を通った保存・登録の取引の中だけ (0084 のメーカー・0086 の梱包と同じ)。
  --   sku_page = この取引の今の約束 (ops.begin_master_write = sku_edit・この SKU・同じ request_id・同じ人) /
  --   register・variation = この取引でこの SKU を登録した約束 (sku_create) の行 (created_at ≧ この取引の始まり = バックアップから戻した DB の古い約束を今のものと見ない)
  if p_via = 'sku_page' then
    v_sess := ops.current_master_write_session();
    if v_sess.session_id is null or v_sess.operation is distinct from 'sku_edit' or v_sess.sku_id is distinct from p_sku_id
       or v_sess.request_id is distinct from p_request_id or v_sess.actor_id is distinct from p_actor_id then
      raise exception 'master_write_session_required: 商品の画面の表示の項目は、同じ取引でこの SKU の保存 (ops.begin_master_write) を始めた後だけ' using errcode = '42501';
    end if;
  elsif not exists (select 1 from ops.master_write_sessions w where w.txid = pg_catalog.txid_current() and w.created_at >= pg_catalog.now() and w.operation = 'sku_create' and w.sku_id = p_sku_id
                      and w.request_id = p_request_id and w.actor_id = p_actor_id) then
    raise exception 'master_write_session_required: 新商品の表示の項目は、その SKU を登録した同じ取引の中だけ (ops.register_new_sku の後)' using errcode = '42501';
  end if;
  -- 鍵: SKU の鍵 → 商品の行 (商品の画面の保存・登録の取引はもう持っている = 同じ取引で取り直しても待たない)
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('core.sku:' || p_sku_id::text, 0));
  perform 1 from core.products p where p.product_id = s.product_id for update;
  v_last := (select max(e.event_id) from ops.product_label_fact_events e where e.product_id = s.product_id);
  if v_last is distinct from p_seen_event_id then
    raise exception 'version_conflict: 画面を開いた後にこの商品の表示の項目が変わった (今の記録 %・開いたとき %)', coalesce(v_last::text, 'なし'), coalesce(p_seen_event_id::text, 'なし') using errcode = '40001';
  end if;
  select * into v_cur from core.product_label_facts x where x.product_id = s.product_id;
  v_from := case when v_cur.product_id is null then null else pg_catalog.jsonb_build_object('country_of_origin', v_cur.country_of_origin, 'net_content', v_cur.net_content,
    'net_content_uom', v_cur.net_content_uom, 'model_number', v_cur.model_number, 'shelf_life_days', v_cur.shelf_life_days) end;
  v_to := case when p_country is null and p_net_content is null and p_model_number is null and p_shelf_life_days is null then null
               else pg_catalog.jsonb_build_object('country_of_origin', p_country, 'net_content', p_net_content::numeric(10,2), 'net_content_uom', p_net_content_uom,
                 'model_number', p_model_number, 'shelf_life_days', p_shelf_life_days) end;
  if v_from is not distinct from v_to then
    return pg_catalog.jsonb_build_object('ok', true, 'sku_id', p_sku_id::text, 'product_id', s.product_id::text, 'from', v_from, 'to', v_to, 'changed', false, 'event_id', v_last::text);
  end if;
  perform pg_catalog.set_config('ops.label_protocol', '1', true);
  if v_to is null then
    delete from core.product_label_facts x where x.product_id = s.product_id;
  else
    insert into core.product_label_facts (product_id, company_id, country_of_origin, net_content, net_content_uom, model_number, shelf_life_days, set_by, set_via, request_id)
      values (s.product_id, 1, p_country, p_net_content, p_net_content_uom, p_model_number, p_shelf_life_days, p_actor_id, p_via, p_request_id)
      on conflict (product_id) do update set country_of_origin = excluded.country_of_origin, net_content = excluded.net_content, net_content_uom = excluded.net_content_uom,
        model_number = excluded.model_number, shelf_life_days = excluded.shelf_life_days,
        set_by = excluded.set_by, set_via = excluded.set_via, request_id = excluded.request_id, set_at = pg_catalog.now();
  end if;
  insert into ops.product_label_fact_events (company_id, product_id, sku_id, from_value, to_value, via, actor_id, request_id, reason, db_user)
    values (1, s.product_id, p_sku_id, v_from, v_to, p_via, p_actor_id, p_request_id, p_reason, v_db_user) returning event_id into v_event;
  perform pg_catalog.set_config('ops.label_protocol', '', true);
  return pg_catalog.jsonb_build_object('ok', true, 'sku_id', p_sku_id::text, 'product_id', s.product_id::text, 'from', v_from, 'to', v_to, 'changed', true, 'event_id', v_event::text);
end $$;
revoke all on function ops.set_product_label_facts(uuid, text, bigint, text, numeric, text, text, integer, bigint, text, text) from public;

-- ═══════════ 3. まとめた値 ═══════════
/**
 * 商品ごとのいま使う表示の項目。項目ごとに 人の値 → 無ければ出どころ。*_source = human / 出どころ (内容量だけ = 夜間ロードの解決に使った観測の source_system・
 *   分からない = nightly_load)・human_set_at = 人の値を入れた時刻。行 = 人の値があるか、夜間ロードの内容量がある商品だけ
 *   🚨 security_invoker にしない = 読む人に core.products・観測の表の権限が無くてもよい (このビューの select だけ渡す)
 *   原産国・メーカー型番・消費期限の目安の出どころは今は人だけ (product-hub の今の値は product-hub / マスタの入力の画面が直に読んで見せる・観測にはしない =
 *   product-hub で消した値が Company DB に残る道を作らない・設計 27 §3.4 v2 の Codex R1 High 1)
 */
create view core.v_product_label_facts as
select p.product_id,
       h.country_of_origin, case when h.country_of_origin is not null then 'human' end as country_source,
       case when h.net_content is not null then h.net_content else p.net_content end as net_content,
       case when h.net_content is not null then h.net_content_uom else p.net_content_uom end as net_content_uom,
       case when h.net_content is not null then 'human'
            when p.net_content is not null then coalesce((select o.source_system from core.attribute_resolutions r
                   join core.product_attribute_observations o on o.observation_id = r.resolved_observation_id
                  where r.entity_type = 'product' and r.entity_id = p.product_id and r.attribute = 'net_content' and r.packaging_scope = 'item'), 'nightly_load') end as net_source,
       h.model_number, case when h.model_number is not null then 'human' end as model_source,
       h.shelf_life_days, case when h.shelf_life_days is not null then 'human' end as shelf_source,
       h.set_at as human_set_at
  from core.products p
  left join core.product_label_facts h on h.product_id = p.product_id
 where h.product_id is not null or p.net_content is not null;
comment on view core.v_product_label_facts is '🆕 0099: 商品ごとのいま使う表示の項目 (原産国・内容量・メーカー型番・消費期限の目安の日数)。項目ごとに 人 (core.product_label_facts) → 出どころ (内容量だけ = 夜間ロードの core.products.net_content)。人の値を含めた値を読むときはここ (正式な読み口)';
revoke all on core.v_product_label_facts from public;

-- 既にある列の注釈 (読む人が迷わないように。列・値は変えない)
comment on column core.products.net_content is '内容量の数 (夜間ロードが product-hub などの観測から解決した参考の値)。🆕 0099: マスタの入力で人が入れた値を含めたいま使う値は core.v_product_label_facts';
comment on column core.products.country_of_origin is '🆕 0099 の注釈: 使っていない列 (書く人なし)。原産国は core.v_product_label_facts (人が入れた値 = core.product_label_facts)';
comment on column core.products.model_number is '🆕 0099 の注釈: 使っていない列 (書く人なし)。メーカー型番は core.v_product_label_facts (人が入れた値 = core.product_label_facts)';
comment on column core.product_compliance.shelf_life_days is '🆕 0099 の注釈: 使っていない列 (夜間ロードは書かない)。消費期限の目安の日数は core.v_product_label_facts (人が入れた値 = core.product_label_facts)';
