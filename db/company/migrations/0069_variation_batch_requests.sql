-- 0069: まとめての登録の「要求の全部」のハッシュ (2026-10-10。AI_reference CompanyDB構想/20 v7 §⑤・§⑩ の PR-7・#1679 Codex R1 Medium 1)
-- 🚨 前提 = 0067 (まとまりの DB・本番に適用済み = 0067 は変えない)。0067 の関数・表の本文は触らない (新しい表と関数を 1 つずつ足すだけ)
--
-- なぜ: 0067 の ops.variation_batches.spec_hash は「まとまり・軸・選択肢・子のコードと選択肢」だけのハッシュ。
--   画面 (lib/master-variation.mjs) は子の名前・売価・原価・JAN・出品カードの欄・理由も送る = 同じ request_id でそれらだけを変えて送ると、
--   0067 だけでは前の答え (replayed) が返る (画面は「保存できた」に見えるが DB は前の値のまま)。
--   → 要求の全部のハッシュ (lib の variationPayloadHashOf) を、まとめての登録と同じ取引で残す。押し直しは lib が request の鍵の直後にこれと照らし、
--     同じ = 残した答えをすぐ返す (門・product-hub の下書き・仕入先などは見ない = 自分の保存で作られたカードや、後で閉じた門で押し直しが失敗しない)・
--     違う = request_id_reused
-- なにを:
--   1. ops.variation_batch_requests (request_id = まとめての登録・人・要求のハッシュ・追記だけ)
--   2. ops.variation_batch_record_request(request_id, actor, hash) = この取引で開いた (閉じる前の) まとめての登録にだけ 1 回書く (security definer・画面のロールに実行だけ)
-- 🚨 今は何も変わらない: まとめての登録は products.parent の持ち主が company のときだけ動く (今は load = 0067 の関数が断る = この表にも何も入らない)
-- 🚨 ロール (scripts/company-db/create-master-edit-roles.mjs) の流し直しで master_edit に関数の実行と表の読み取り (流さなくても今の動きは変わらない)

create table ops.variation_batch_requests (
  request_id    uuid primary key references ops.variation_batches (request_id),
  company_id    smallint not null default 1 references core.companies,
  actor_id      text not null check (pg_catalog.length(actor_id) between 1 and 320),
  request_hash  text not null check (request_hash ~ '^[0-9a-f]{64}$'),
  recorded_at   timestamptz not null default pg_catalog.clock_timestamp()
);
select core.make_append_only('ops', 'variation_batch_requests');
comment on table ops.variation_batch_requests is 'まとめての登録の要求の全部のハッシュ (0069)。まとめての登録を開いた取引で 1 回だけ書く = 押し直しの照合 (lib/master-variation.mjs)。追記だけ';

/**
 * まとめての登録の要求のハッシュを残す。この取引で開いた (まだ閉じていない) まとめての登録・開いた人だけ・1 回だけ (2 回目は主キーで断る)。
 * 書くのはこの関数だけ (画面のロールに表の insert は渡さない)
 */
create function ops.variation_batch_record_request(p_request_id uuid, p_actor_id text, p_request_hash text) returns void
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_b ops.variation_batches;
begin
  if p_request_id is null or coalesce(p_request_hash, '') !~ '^[0-9a-f]{64}$' then
    raise exception 'invalid_input: request_id と要求のハッシュ (64 桁) が要る' using errcode = '22023';
  end if;
  select * into v_b from ops.variation_batches b where b.request_id = p_request_id for update;
  if not found or v_b.status <> 'open' or v_b.txid <> pg_catalog.txid_current() then
    raise exception 'no_open_batch: この取引で開いたまとめての登録 (request_id %) が無い', p_request_id using errcode = 'P0001';
  end if;
  if v_b.actor_id is distinct from p_actor_id then
    raise exception 'master_write_session_mismatch: まとめての登録を開いた人と違う' using errcode = '42501';
  end if;
  insert into ops.variation_batch_requests (request_id, company_id, actor_id, request_hash) values (p_request_id, 1, p_actor_id, p_request_hash);
end $$;
revoke all on function ops.variation_batch_record_request(uuid, text, text) from public;
revoke all on ops.variation_batch_requests from public;

do $$ begin
  if exists (select 1 from pg_roles where rolname = 'watcher') then
    execute 'grant select on ops.variation_batch_requests to watcher';
  end if;
end $$;
