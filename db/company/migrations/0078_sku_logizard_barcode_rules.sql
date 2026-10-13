-- 0078 (仮の番号・マージの時に付け替える): ロジザードに送るバーコードの「決め方」(AI_reference CompanyDB構想/23・中原さんの決定 2026-10-13 夜)
--   「JAN コードとロジザードに送るバーコード情報は別。商品のバーコードには Amazon の FNSKU を記載している場合もある」
--   値の決め方 = FNSKU があれば FNSKU・無ければ JAN・JAN も無ければ空欄 (ロジザードが番号を付ける)
--
-- なにを:
--   1. ops.sku_logizard_barcode_rules = SKU ごとの決め方 (auto / fnsku / jan / lz_assign) と、候補が複数のときに人が選んだ 1 本 (pinned) の記録。
--      追記だけ (変えない・消さない・TRUNCATE しない)・書くのは下の関数だけ (取引の中の印)。SKU ごとに一番新しい行 (rule_row の大きい方) が今の決め方。
--      **行が無い = 決め方なし** (この機能の前の商品・NE で直接作った商品。画面は auto で計算した参考・③c-2 では要確認)
--      🚨 送る値は持たない = CSV を作るときに Company DB の今の FNSKU・JAN から決める (lib/logizard-barcode.mjs)。pinned は「候補が複数のときにどれか」の印だけ。
--         core.external_ids (識別子) には混ぜない・core.products の列にしない (ops.register_new_sku を作り直さない・持ち主表にキーを足さない)
--   2. ops.set_sku_logizard_barcode_rule(request_id, 人, sku_id, 決め方, 選んだ 1 本, どこから, 開いたときの行番号) = 決め方を書く
--      (security definer・画面のロール master_edit だけに実行)。書けるのは 単品・会社 1・登録の状態が「やめた」でない商品 (いつでも = 使うのはロジザードに初めて載せる時だけ)。
--      SKU の鍵 (core.sku:<id>) の後に、開いたときの行番号と今の一番新しい行番号を比べる (違う = stale = 画面を開き直す)。
--      選んだ 1 本は fnsku / jan のときだけ・今の候補 (FNSKU = Amazon 日本・この SKU 1 個だけの出品・消えていない / JAN = この商品の有効な JAN) にあるときだけ。
--      同じ request_id の押し直し = 前の答え (replayed)・違う中身 = request_id_reused。今の一番新しい行と同じ (決め方・選んだ 1 本) = 書かない (changed = false)
-- 🚨 この migration は既にある行を何も変えない (表は空)。security definer = 一時の表を使わない・search_path = pg_catalog, pg_temp・public の実行権を外す。
--    ロール (scripts/company-db/create-master-edit-roles.mjs) の流し直しで master_edit に 表の select と 2. の実行を足す (watcher の select は create-watch-roles.mjs)

-- ═══════════ 1. 決め方の表 ═══════════
create table ops.sku_logizard_barcode_rules (
  rule_row    bigint generated always as identity primary key,
  company_id  smallint not null default 1 references core.companies,
  sku_id      bigint not null,
  rule        text not null check (rule in ('auto', 'fnsku', 'jan', 'lz_assign')),
  pinned      text check (pinned is null or (rule = 'fnsku' and pinned ~ '^X0[0-9A-Z]{8}$') or (rule = 'jan' and pinned ~ '^([0-9]{8}|[0-9]{13})$')),
  via         text not null check (via in ('register', 'sku_page')),
  actor_id    text not null check (pg_catalog.length(actor_id) between 1 and 320),
  request_id  uuid not null,
  db_user     text not null,
  created_at  timestamptz not null default pg_catalog.clock_timestamp(),
  foreign key (company_id, sku_id) references core.skus (company_id, sku_id),
  unique (request_id, sku_id)
);
create index ix_slbr_sku on ops.sku_logizard_barcode_rules (sku_id, rule_row desc);
comment on table ops.sku_logizard_barcode_rules is 'ロジザードに送るバーコードの決め方 (0078・CompanyDB構想 23)。SKU ごとに一番新しい行が今の決め方・行が無い = 決め方なし。送る値は持たない (CSV を作るときに FNSKU・JAN から決める)。追記だけ・書くのは ops.set_sku_logizard_barcode_rule だけ';
comment on column ops.sku_logizard_barcode_rules.rule is 'auto = FNSKU → JAN → 空欄 / fnsku = FNSKU だけ (無ければ待つ) / jan = JAN だけ (無ければ待つ) / lz_assign = 空欄 (ロジザードが番号を付ける)';
comment on column ops.sku_logizard_barcode_rules.pinned is '候補が複数のときに人が選んだ 1 本 (fnsku / jan のときだけ)。今の候補に無くなったら答えは要確認 (黙ってほかの値にしない)';
comment on column ops.sku_logizard_barcode_rules.via is 'register = 新商品の登録 (同じ取引) / sku_page = 商品の画面';

create function ops.guard_sku_logizard_barcode_rules() returns trigger language plpgsql set search_path = pg_catalog, pg_temp as $$
begin
  if tg_op <> 'INSERT' then raise exception 'logizard_barcode_rule: 決め方の行は変えない・消さない (追記だけ)' using errcode = '42501'; end if;
  if coalesce(pg_catalog.current_setting('ops.logizard_barcode_protocol', true), '') is distinct from '1' then
    raise exception 'logizard_barcode_rule: 決め方は ops.set_sku_logizard_barcode_rule の中でだけ書く' using errcode = '42501';
  end if;
  return new;
end $$;
revoke all on function ops.guard_sku_logizard_barcode_rules() from public;
create trigger trg_sku_logizard_barcode_rules_guard before insert or update or delete on ops.sku_logizard_barcode_rules
  for each row execute function ops.guard_sku_logizard_barcode_rules();
create trigger trg_sku_logizard_barcode_rules_no_truncate before truncate on ops.sku_logizard_barcode_rules
  for each statement execute function core.reject_mutation();

-- ═══════════ 2. 決め方を書く ═══════════
/**
 * p_rule = auto / fnsku / jan / lz_assign・p_pinned = 選んだ 1 本 (fnsku / jan のときだけ・null = 選ばない)・p_via = register (新商品の登録の取引の中) / sku_page (商品の画面)・
 * p_seen_row = 画面を開いたときの一番新しい行番号 (行が無かった = null)。人 (メール形) が要る。
 * 戻り値 = { ok, sku_id, rule, pinned, changed, rule_row?, replayed? }
 * 呼べるロール = master_edit (画面) だけ。🚨 人 (actor_id) は呼び手が言う値 (DB では確かめられない)。DB が残すのは db_user
 */
create function ops.set_sku_logizard_barcode_rule(p_request_id uuid, p_actor_id text, p_sku_id bigint, p_rule text, p_pinned text, p_via text, p_seen_row bigint) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_db_user text := case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;
  s         record;
  v_prev    ops.sku_logizard_barcode_rules;
  v_cur     ops.sku_logizard_barcode_rules;
  v_row     bigint;
  v_sum     integer := 0;
  v_i       integer;
begin
  if p_request_id is null or p_sku_id is null then raise exception 'invalid_input: request_id と SKU が要る' using errcode = '22023'; end if;
  if ops.reg_actor_problem(p_actor_id, null) is not null or p_actor_id !~ '^[^[:space:]@]+@[^[:space:]@]+$' then
    raise exception 'invalid_input: 人 (メール) が要る' using errcode = '22023';
  end if;
  if p_rule is null or p_rule not in ('auto', 'fnsku', 'jan', 'lz_assign') then raise exception 'invalid_input: 決め方は auto / fnsku / jan / lz_assign' using errcode = '22023'; end if;
  if p_via is null or p_via not in ('register', 'sku_page') then raise exception 'invalid_input: どこから は register / sku_page' using errcode = '22023'; end if;
  if p_pinned is not null and not ((p_rule = 'fnsku' and p_pinned ~ '^X0[0-9A-Z]{8}$') or (p_rule = 'jan' and p_pinned ~ '^([0-9]{8}|[0-9]{13})$')) then
    raise exception 'invalid_input: 選んだ 1 本は 決め方 FNSKU なら FNSKU (X0…)・JAN なら JAN (8 か 13 桁) のときだけ' using errcode = '22023';
  end if;
  -- SKU の鍵 (NE 登録の CSV・JAN の保存と同じ鍵)。この後の文は鍵の後の姿を見る (read committed)
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('core.sku:' || p_sku_id::text, 0));
  -- 押し直し (同じ request_id・同じ SKU) = 前の答え。人・決め方・選んだ 1 本・どこから が違えば request_id_reused
  select * into v_prev from ops.sku_logizard_barcode_rules r where r.request_id = p_request_id and r.sku_id = p_sku_id;
  if v_prev.rule_row is not null then
    if v_prev.actor_id is distinct from p_actor_id or v_prev.rule is distinct from p_rule or v_prev.pinned is distinct from p_pinned or v_prev.via is distinct from p_via then
      raise exception 'request_id_reused: request_id % はほかの人・中身で使われている', p_request_id using errcode = '23505';
    end if;
    return pg_catalog.jsonb_build_object('ok', true, 'sku_id', p_sku_id::text, 'rule', v_prev.rule, 'pinned', v_prev.pinned, 'changed', true, 'rule_row', v_prev.rule_row::text, 'replayed', true);
  end if;
  select k.sku_id, k.company_id, k.sku_kind, k.product_id, r.state as reg into s
    from core.skus k left join ops.master_registrations r on r.sku_id = k.sku_id where k.sku_id = p_sku_id;
  if s.sku_id is null or s.company_id <> 1 then raise exception 'invalid_input: SKU % が Company DB に無い', p_sku_id using errcode = '22023'; end if;
  if s.sku_kind is distinct from 'single' then raise exception 'not_single: ロジザードのバーコードの決め方は単品だけ (SKU %)', p_sku_id using errcode = '22023'; end if;
  if s.reg = 'cancelled' then raise exception 'cancelled: 登録をやめた商品 (SKU %) の決め方は書かない', p_sku_id using errcode = '22023'; end if;
  -- 開いたときの行番号と今の一番新しい行番号 (違う = ほかの人がちょうど直した = 画面を開き直す)
  select * into v_cur from ops.sku_logizard_barcode_rules r where r.sku_id = p_sku_id order by r.rule_row desc limit 1;
  if v_cur.rule_row is distinct from p_seen_row then
    raise exception 'stale: 画面を開いた後にほかの人が決め方を直した (今の行 %・開いたときの行 %)', coalesce(v_cur.rule_row::text, 'なし'), coalesce(p_seen_row::text, 'なし') using errcode = '40001';
  end if;
  -- 選んだ 1 本は今の候補にあるときだけ (lib/logizard-barcode.mjs の候補と同じ読み方)。
  -- JAN はチェック数字まで (lzJanOk と同じ・Codex PR R1 M2 = 形の合わない JAN が external_ids にあっても選ばせない)
  if p_pinned is not null and p_rule = 'jan' then
    for v_i in 1 .. pg_catalog.length(p_pinned) - 1 loop
      v_sum := v_sum + pg_catalog.substr(p_pinned, pg_catalog.length(p_pinned) - v_i, 1)::integer * (case when v_i % 2 = 1 then 3 else 1 end);
    end loop;
    if (10 - v_sum % 10) % 10 <> pg_catalog.right(p_pinned, 1)::integer then
      raise exception 'pinned_not_candidate: JAN % はチェック数字が合わない', p_pinned using errcode = '22023';
    end if;
  end if;
  if p_pinned is not null and p_rule = 'jan' and not exists (
       select 1 from core.external_ids e where e.company_id = 1 and e.entity_type = 'product' and e.entity_id = s.product_id
          and e.system = 'jan' and e.id_kind = 'jan' and e.valid_to is null and e.external_norm = core.norm_code(p_pinned)) then
    raise exception 'pinned_not_candidate: JAN % はこの商品の今の JAN ではない', p_pinned using errcode = '22023';
  end if;
  if p_pinned is not null and p_rule = 'fnsku' and not exists (
       select 1 from core.listing_components lc
         join core.listings l on l.company_id = lc.company_id and l.listing_id = lc.listing_id and l.mall = 'amazon' and l.status is distinct from 'deleted'
         join core.external_ids e on e.company_id = l.company_id and e.entity_type = 'listing' and e.entity_id = l.listing_id
                                 and e.system = 'amazon' and e.id_kind = 'fnsku' and e.valid_to is null and e.external_norm = core.norm_code(p_pinned)
        where lc.company_id = 1 and lc.sku_id = p_sku_id and lc.qty = 1
          -- 出品の構成がこの SKU 1 個だけ (同じ FNSKU が 2 つの出品に付くことは無い = 0002 の ux_external_ids_active)
          and (select pg_catalog.count(*) from core.listing_components x where x.listing_id = l.listing_id) = 1) then
    raise exception 'pinned_not_candidate: FNSKU % はこの商品だけの Amazon 日本の出品の今の FNSKU ではない', p_pinned using errcode = '22023';
  end if;
  -- 今の一番新しい行と同じ = 書かない
  if v_cur.rule_row is not null and v_cur.rule = p_rule and v_cur.pinned is not distinct from p_pinned then
    return pg_catalog.jsonb_build_object('ok', true, 'sku_id', p_sku_id::text, 'rule', p_rule, 'pinned', p_pinned, 'changed', false, 'rule_row', v_cur.rule_row::text);
  end if;
  perform pg_catalog.set_config('ops.logizard_barcode_protocol', '1', true);
  insert into ops.sku_logizard_barcode_rules (company_id, sku_id, rule, pinned, via, actor_id, request_id, db_user)
    values (1, p_sku_id, p_rule, p_pinned, p_via, p_actor_id, p_request_id, v_db_user) returning rule_row into v_row;
  perform pg_catalog.set_config('ops.logizard_barcode_protocol', '', true);
  return pg_catalog.jsonb_build_object('ok', true, 'sku_id', p_sku_id::text, 'rule', p_rule, 'pinned', p_pinned, 'changed', true, 'rule_row', v_row::text);
end $$;
revoke all on function ops.set_sku_logizard_barcode_rule(uuid, text, bigint, text, text, text, bigint) from public;
