-- 0080 (仮の番号・マージの時に付け替える): ロジザードに送るバーコードの「値」と、ロジザード登録の CSV の帳面 (AI_reference CompanyDB構想/23 v4)
--   中原さん 10/13 夜「FNSKU ってどこに入力するの? 新商品登録の画面に作ってくれよ。ここで入れてすぐにロジザードと NE に登録するからさ」
--   「逆に Amazon に出品するにはいらない」= 値はロジザードに送るためだけ (Amazon の出品・対応とは切り離す)
--   Q1 = a: 入荷日の管理が「入力なし」(不明) の商品は、ロジザードの CSV の入荷日管理フラグに 0 (なし) を入れて送る
--
-- なにを (1 本目 = 値を入れる。ロジザード登録の CSV と出した記録の帳面は 2 本目の migration):
--   1. ops.sku_logizard_barcode_rules (0078) に値の列 value を足す。🆕 0080 から書く行 = rule fnsku + value (X00 + 英数字 7 字) /
--      rule jan + value (8 か 13 桁・チェック数字) / rule lz_assign (空欄 = ロジザードが番号を付ける・value なし)。
--      0078 の行 (auto・値の無い fnsku / jan) は残す (= 値が無い = もう一度入れる)。SKU ごとに一番新しい行が今の値
--   2. ops.lzbc_value_holder(sku_id, 値) = その値を持っているほかの商品のコード (無い = null)。ほかの SKU の今の値・
--      Company DB の JAN (ほかの商品)・FNSKU (ほかの商品を含む出品)。🚨 ロジザードのバーコードマスタの写しは Render の SQLite = アプリが見る
--   3. ops.set_sku_logizard_barcode(request_id, 人, sku_id, 種類, 値, どこから, 開いたときの行番号) = 値を書く (master_edit だけ)。
--      単品・会社 1・やめていないときだけ。SKU の鍵 → 値の鍵 → 押し直し → stale → 重なり
--   4. 0078 の ops.set_sku_logizard_barcode_rule (決め方だけ) は画面から使わない = ロールの script で master_edit の実行を外す (持ち主だけ)
-- 🚨 既にある行は変えない (列を足すだけ)。security definer = 一時の表を使わない・search_path = pg_catalog, pg_temp・public の実行権を外す

-- ═══════════ 1. 値の列 ═══════════
alter table ops.sku_logizard_barcode_rules add column value text;
alter table ops.sku_logizard_barcode_rules add constraint ck_slbr_value check (
  value is null
  or (rule = 'fnsku' and value ~ '^X00[0-9A-Z]{7}$')
  or (rule = 'jan' and value ~ '^([0-9]{8}|[0-9]{13})$'));
create index ix_slbr_value on ops.sku_logizard_barcode_rules (value) where value is not null;
-- 🆕 #1719 Codex R1 High 2 (中原さん「推定で現場を止めない」): ロジザードのバーコードマスタの写しと照らせたか (照らせない = 保存は通す・印を残す)。
--   checked = 写しと照らした / unavailable = 写しを読めない / empty = 写しが空 / stale = 写しが古い (36 時間より前)。lz_copy_as_of = 照らした写しの時刻 (読めない = null)。
--   値の無い行 (空欄・0078 の行) は照らさない = null。照らせなかった値は、ロジザード登録の CSV (2 本目) を出すときに新しい写しでもう一度照らす (照らせるまで出さない)
alter table ops.sku_logizard_barcode_rules add column lz_copy_state text check (lz_copy_state in ('checked', 'unavailable', 'empty', 'stale'));
alter table ops.sku_logizard_barcode_rules add column lz_copy_as_of timestamptz;
alter table ops.sku_logizard_barcode_rules add constraint ck_slbr_copy check ((value is null) = (lz_copy_state is null));
-- 🆕 #1719 Codex R2 Medium 1: 結果と写しの時刻の組み合わせ = checked / stale は時刻が要る・unavailable / empty / 値の無い行は時刻なし (写しの中に行が無い = 時刻も無い)。
--   新しい・古い (36 時間) は書く関数が DB の時計で確かめる (行の CHECK は時計を見られない)
alter table ops.sku_logizard_barcode_rules add constraint ck_slbr_copy_as_of check (
  case when lz_copy_state in ('checked', 'stale') then lz_copy_as_of is not null else lz_copy_as_of is null end);
comment on column ops.sku_logizard_barcode_rules.lz_copy_state is '🆕 0080: 値をロジザードのバーコードマスタの写しと照らせたか (checked / unavailable / empty / stale・値の無い行 = null)';
comment on column ops.sku_logizard_barcode_rules.value is '🆕 0080: ロジザードに送るバーコードの値 (rule fnsku = X00 + 英数字 7 字 / jan = 8 か 13 桁)。rule lz_assign = null (空欄 = ロジザードが番号を付ける)。0078 の行は null (値なし = もう一度入れる)';

-- ═══════════ 2. 値の持ち主 ═══════════
/**
 * p_value を持っているほかの商品のコード (無い = null)。読むだけ (書く関数の中と、画面のその場の確かめ = master_edit に実行だけ)。
 *   ① ほかの SKU の今の値 (一番新しい行・やめた商品を除く) ② Company DB の有効な JAN (ほかの商品)
 *   ③ Company DB の有効な FNSKU で、出品の構成にほかの SKU がある (同じ会社)。構成の無い出品の FNSKU は持ち主が分からない = 数えない (参考)
 */
create function ops.lzbc_value_holder(p_sku_id bigint, p_value text) returns text
  language sql stable security definer set search_path = pg_catalog, pg_temp as $$
  select coalesce(
    (select k.code from (select distinct on (r.sku_id) r.sku_id, r.value from ops.sku_logizard_barcode_rules r
                          where r.company_id = 1 and r.sku_id <> p_sku_id order by r.sku_id, r.rule_row desc) x
       join core.skus k on k.sku_id = x.sku_id left join ops.master_registrations g on g.sku_id = x.sku_id
      where x.value = p_value and g.state is distinct from 'cancelled' order by k.code_norm limit 1),
    (select coalesce(k.code, 'product ' || e.entity_id::text) from core.external_ids e
       left join core.skus k on k.company_id = 1 and k.product_id = e.entity_id
      where e.company_id = 1 and e.entity_type = 'product' and e.system = 'jan' and e.id_kind = 'jan' and e.valid_to is null
        and e.external_norm = core.norm_code(p_value)
        and e.entity_id is distinct from (select s.product_id from core.skus s where s.sku_id = p_sku_id)
      order by k.code_norm limit 1),
    (select k.code from core.external_ids e
       join core.listings l on l.company_id = e.company_id and l.listing_id = e.entity_id
       join core.listing_components c on c.company_id = l.company_id and c.listing_id = l.listing_id and c.sku_id <> p_sku_id
       join core.skus k on k.sku_id = c.sku_id
      where e.company_id = 1 and e.entity_type = 'listing' and e.system = 'amazon' and e.id_kind = 'fnsku' and e.valid_to is null
        and e.external_norm = core.norm_code(p_value)
      order by k.code_norm limit 1))
$$;
revoke all on function ops.lzbc_value_holder(bigint, text) from public;

/** JAN-8 / JAN-13 のチェック数字 (lib/master-write.mjs janValid と同じ) */
create function ops.lzbc_jan_ok(p text) returns boolean language plpgsql immutable set search_path = pg_catalog, pg_temp as $$
declare
  v_sum integer := 0;
  v_i   integer;
begin
  if p is null or p !~ '^([0-9]{8}|[0-9]{13})$' then return false; end if;
  for v_i in 1 .. pg_catalog.length(p) - 1 loop
    v_sum := v_sum + pg_catalog.substr(p, pg_catalog.length(p) - v_i, 1)::integer * (case when v_i % 2 = 1 then 3 else 1 end);
  end loop;
  return (10 - v_sum % 10) % 10 = pg_catalog.right(p, 1)::integer;
end $$;
revoke all on function ops.lzbc_jan_ok(text) from public;

-- ═══════════ 2b. 書く関数の答えの控え (押し直しを見分ける・#1719 Codex R5 Medium) ═══════════
--   書く関数を呼んで通った回 (書いた・同じなので書かなかった の両方) の request_id と答え。値の行 (rules) は書かなかった回を残さない = 書かなかった応答が失われた後の再送を見分けるのはこの表。
--   (request_id, sku_id) ごとに 1 行 (まとまりの登録は子ごとに同じ request_id)。書くのは関数の中だけ (印 ops.logizard_barcode_protocol)・追記だけ
create table ops.sku_logizard_barcode_requests (
  request_id uuid        not null,
  sku_id     bigint      not null references core.skus (sku_id),
  actor_id   text        not null,
  rule       text        not null,
  value      text,
  via        text        not null,
  seen_row   bigint,                 -- 画面を開いたときの行番号 (#1719 Codex R6 Low: 押し直しの比べに入れる = 違う行番号の同じ request_id は request_id_reused)
  result     jsonb       not null,
  created_at timestamptz not null default pg_catalog.now(),
  primary key (request_id, sku_id)
);
revoke all on ops.sku_logizard_barcode_requests from public;
comment on table ops.sku_logizard_barcode_requests is '🆕 0080 (#1719 Codex R5): ops.set_sku_logizard_barcode の答えの控え (書いた・書かなかった の両方)。同じ request_id の押し直し = この答えを返す';
create function ops.guard_sku_logizard_barcode_requests() returns trigger language plpgsql set search_path = pg_catalog, pg_temp as $$
begin
  if tg_op <> 'INSERT' then raise exception 'logizard_barcode_request: 答えの控えは変えない・消さない (追記だけ)' using errcode = '42501'; end if;
  if coalesce(pg_catalog.current_setting('ops.logizard_barcode_protocol', true), '') is distinct from '1' then
    raise exception 'logizard_barcode_request: 答えの控えは ops.set_sku_logizard_barcode の中でだけ書く' using errcode = '42501';
  end if;
  return new;
end $$;
revoke all on function ops.guard_sku_logizard_barcode_requests() from public;
create trigger trg_sku_logizard_barcode_requests_guard before insert or update or delete on ops.sku_logizard_barcode_requests
  for each row execute function ops.guard_sku_logizard_barcode_requests();
create trigger trg_sku_logizard_barcode_requests_no_truncate before truncate on ops.sku_logizard_barcode_requests
  for each statement execute function core.reject_mutation();

-- ═══════════ 3. 値を書く ═══════════
/**
 * p_kind = fnsku / jan / lz_assign (空欄)・p_value = 値 (lz_assign = null)・p_via = register (新商品の登録の取引の中) / sku_page (商品の画面)・
 * p_seen_row = 画面を開いたときの一番新しい行番号 (行が無かった = null・新商品の登録 = null)。人 (メール形) が要る。
 * p_lz_state / p_lz_as_of = 呼び手 (アプリ) がロジザードのバーコードマスタの写しと照らした結果 (値があるときは要る・空欄 = null)。
 *   🆕 #1719 Codex R2 Medium 1 = 結果と時刻が合わないと断る (直に呼んでも「照らした」と偽れない)。DB の時計で:
 *     checked = 時刻が 36 時間より新しい (未来は 5 分まで)・stale = 時刻が 36 時間より前・unavailable / empty / 空欄 = 時刻なし。
 *     36 時間のちょうど境目の前後 5 分は、アプリと DB の時計のずれ・待ち時間で断らないよう、どちらでも通す
 * 戻り値 = { ok, sku_id, kind, value, lz_copy_state, lz_copy_as_of, changed, rule_row, replayed? }
 * 断る: invalid_input (形・チェック数字) / not_single / cancelled / stale / lzbc_taken (ほかの商品の値) / request_id_reused
 */
create function ops.set_sku_logizard_barcode(p_request_id uuid, p_actor_id text, p_sku_id bigint, p_kind text, p_value text, p_via text, p_seen_row bigint,
                                               p_lz_state text, p_lz_as_of timestamptz) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_db_user text := case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;
  s         record;
  v_prev    ops.sku_logizard_barcode_rules;
  v_cur     ops.sku_logizard_barcode_rules;
  v_holder  text;
  v_row     bigint;
  v_req     ops.sku_logizard_barcode_requests;
  v_res     jsonb;
begin
  if p_request_id is null or p_sku_id is null then raise exception 'invalid_input: request_id と SKU が要る' using errcode = '22023'; end if;
  if ops.reg_actor_problem(p_actor_id, null) is not null or p_actor_id !~ '^[^[:space:]@]+@[^[:space:]@]+$' then
    raise exception 'invalid_input: 人 (メール) が要る' using errcode = '22023';
  end if;
  if p_via is null or p_via not in ('register', 'sku_page') then raise exception 'invalid_input: どこから は register / sku_page' using errcode = '22023'; end if;
  if p_kind = 'lz_assign' then
    if p_value is not null then raise exception 'invalid_input: 空欄 (ロジザードが番号を付ける) に値は付けない' using errcode = '22023'; end if;
  elsif p_kind = 'fnsku' then
    if p_value is null or p_value !~ '^X00[0-9A-Z]{7}$' then raise exception 'invalid_input: FNSKU は X00 + 英数字 7 字 (大文字・10 文字)' using errcode = '22023'; end if;
  elsif p_kind = 'jan' then
    if not ops.lzbc_jan_ok(p_value) then raise exception 'invalid_input: JAN は 8 か 13 桁の数字で、チェック数字が合うもの' using errcode = '22023'; end if;
  else
    raise exception 'invalid_input: 種類は fnsku / jan / lz_assign' using errcode = '22023';
  end if;
  if (p_value is null) <> (p_lz_state is null) or (p_lz_state is not null and p_lz_state not in ('checked', 'unavailable', 'empty', 'stale'))
     or (p_lz_state is null and p_lz_as_of is not null) then
    raise exception 'invalid_input: 値があるときはロジザードの写しと照らした結果 (checked / unavailable / empty / stale) が要る・空欄には要らない' using errcode = '22023';
  end if;
  if p_lz_state in ('unavailable', 'empty') and p_lz_as_of is not null then
    raise exception 'invalid_input: 写しを読めない・空 (%) に写しの時刻は付けない', p_lz_state using errcode = '22023';
  end if;
  if p_lz_state = 'checked' and (p_lz_as_of is null or p_lz_as_of <= pg_catalog.clock_timestamp() - interval '36 hours 5 minutes'
                                 or p_lz_as_of > pg_catalog.clock_timestamp() + interval '5 minutes') then
    raise exception 'invalid_input: 照らした (checked) は 36 時間より新しい写しの時刻が要る (時刻 %)', coalesce(p_lz_as_of::text, 'なし') using errcode = '22023';
  end if;
  if p_lz_state = 'stale' and (p_lz_as_of is null or p_lz_as_of > pg_catalog.clock_timestamp() - interval '35 hours 55 minutes') then
    raise exception 'invalid_input: 古い (stale) は 36 時間より前の写しの時刻が要る (時刻 %)', coalesce(p_lz_as_of::text, 'なし') using errcode = '22023';
  end if;
  -- SKU の鍵 → 値の鍵 (同じ値を 2 つの商品に同時に付けない)。この後の文は鍵の後の姿を見る (read committed)
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('core.sku:' || p_sku_id::text, 0));
  if p_value is not null then perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('ops.lzbc:' || p_value, 0)); end if;
  -- 押し直し (同じ request_id・同じ SKU) = 前の答え。🆕 #1719 R5: 答えの控え (書かなかった回も) を先に見る
  select * into v_req from ops.sku_logizard_barcode_requests x where x.request_id = p_request_id and x.sku_id = p_sku_id;
  if v_req.request_id is not null then
    if v_req.actor_id is distinct from p_actor_id or v_req.rule is distinct from p_kind or v_req.value is distinct from p_value or v_req.via is distinct from p_via
       or v_req.seen_row is distinct from p_seen_row then
      raise exception 'request_id_reused: request_id % はほかの人・中身で使われている', p_request_id using errcode = '23505';
    end if;
    return v_req.result || pg_catalog.jsonb_build_object('replayed', true);
  end if;
  select * into v_prev from ops.sku_logizard_barcode_rules r where r.request_id = p_request_id and r.sku_id = p_sku_id;
  if v_prev.rule_row is not null then
    if v_prev.actor_id is distinct from p_actor_id or v_prev.rule is distinct from p_kind or v_prev.value is distinct from p_value or v_prev.via is distinct from p_via then
      raise exception 'request_id_reused: request_id % はほかの人・中身で使われている', p_request_id using errcode = '23505';
    end if;
    -- 答えの印は前に残した結果と時刻から (#1719 R2 Low: 押し直しの今の写しで作らない)
    return pg_catalog.jsonb_build_object('ok', true, 'sku_id', p_sku_id::text, 'kind', v_prev.rule, 'value', v_prev.value, 'lz_copy_state', v_prev.lz_copy_state,
      'lz_copy_as_of', v_prev.lz_copy_as_of, 'changed', true, 'rule_row', v_prev.rule_row::text, 'replayed', true);
  end if;
  select k.sku_id, k.company_id, k.sku_kind, r.state as reg into s from core.skus k left join ops.master_registrations r on r.sku_id = k.sku_id where k.sku_id = p_sku_id;
  if s.sku_id is null or s.company_id <> 1 then raise exception 'invalid_input: SKU % が Company DB に無い', p_sku_id using errcode = '22023'; end if;
  if s.sku_kind is distinct from 'single' then raise exception 'not_single: ロジザードのバーコードは単品だけ (SKU %)', p_sku_id using errcode = '22023'; end if;
  if s.reg = 'cancelled' then raise exception 'cancelled: 登録をやめた商品 (SKU %) には書かない', p_sku_id using errcode = '22023'; end if;
  select * into v_cur from ops.sku_logizard_barcode_rules r where r.sku_id = p_sku_id order by r.rule_row desc limit 1;
  if v_cur.rule_row is distinct from p_seen_row then
    raise exception 'stale: 画面を開いた後にほかの人がバーコードを直した (今の行 %・開いたときの行 %)', coalesce(v_cur.rule_row::text, 'なし'), coalesce(p_seen_row::text, 'なし') using errcode = '40001';
  end if;
  if p_value is not null then
    v_holder := ops.lzbc_value_holder(p_sku_id, p_value);
    if v_holder is not null then raise exception 'lzbc_taken: バーコード % はほかの商品 (%) が使っている', p_value, v_holder using errcode = '23505'; end if;
  end if;
  -- 今の一番新しい行と同じ (種類・値・写しと照らした結果) = 書かない (照らせなかった値を照らせた = 書く)
  if v_cur.rule_row is not null and v_cur.rule = p_kind and v_cur.value is not distinct from p_value and v_cur.lz_copy_state is not distinct from p_lz_state then
    -- 書かない = 残っている行の時刻 (#1719 R3 Low: 今回渡された時刻を返すと、画面と DB の時刻が食い違う)。🆕 #1719 R5: 書かなかった回も答えの控えに残す (再送を見分ける)
    v_res := pg_catalog.jsonb_build_object('ok', true, 'sku_id', p_sku_id::text, 'kind', p_kind, 'value', p_value, 'lz_copy_state', v_cur.lz_copy_state, 'lz_copy_as_of', v_cur.lz_copy_as_of,
      'changed', false, 'rule_row', v_cur.rule_row::text);
  else
    perform pg_catalog.set_config('ops.logizard_barcode_protocol', '1', true);
    insert into ops.sku_logizard_barcode_rules (company_id, sku_id, rule, value, via, actor_id, request_id, db_user, lz_copy_state, lz_copy_as_of)
      values (1, p_sku_id, p_kind, p_value, p_via, p_actor_id, p_request_id, v_db_user, p_lz_state, p_lz_as_of) returning rule_row into v_row;
    v_res := pg_catalog.jsonb_build_object('ok', true, 'sku_id', p_sku_id::text, 'kind', p_kind, 'value', p_value, 'lz_copy_state', p_lz_state, 'lz_copy_as_of', p_lz_as_of,
      'changed', true, 'rule_row', v_row::text);
  end if;
  perform pg_catalog.set_config('ops.logizard_barcode_protocol', '1', true);
  insert into ops.sku_logizard_barcode_requests (request_id, sku_id, actor_id, rule, value, via, seen_row, result) values (p_request_id, p_sku_id, p_actor_id, p_kind, p_value, p_via, p_seen_row, v_res);
  perform pg_catalog.set_config('ops.logizard_barcode_protocol', '', true);
  return v_res;
end $$;
revoke all on function ops.set_sku_logizard_barcode(uuid, text, bigint, text, text, text, bigint, text, timestamp with time zone) from public;

-- ═══════════ 4. 0078 の「決め方だけ」の関数は画面から使わない (#1719 Codex R1 Medium 2) ═══════════
--   ロールの script の流し直しを待たずに、この migration で master_edit の実行を外す (value = null の行を足して値を消せてしまうため)。ロールが無い DB (試験の最初) は何もしない
do $$ begin
  if exists (select 1 from pg_catalog.pg_roles where rolname = 'master_edit') then
    execute 'revoke execute on function ops.set_sku_logizard_barcode_rule(uuid, text, bigint, text, text, text, bigint) from master_edit';
  end if;
end $$;
