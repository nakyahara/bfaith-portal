-- 0062: 商品の画面で、新商品の下書きの間だけ ロジザードの有効期限の管理・入荷日の管理 (core.products の最初の値) を直せる (2026-10-08 中原さん
--   「下書きの状態なら、ロジザードの期限管理の情報とかも編集できるようにする機能は絶対必要」)
-- 何をするか:
--   1. 画面のロール master_edit が core.products の expiry_managed / inbound_date_managed を変えるときの守り (BEFORE UPDATE の trigger)。
--      通すのは、その商品の SKU が全部 登録の状態 = 下書き (draft) で、NE 登録の CSV を配った品目 (issued / import_declared / partial / verified) が無いときだけ。
--      それ以外 = 42501 logizard_locked (NE からロジザードに載ったら ロジザードが正)。入荷日の管理を空 (不明) に戻すのも断る
--      アプリ (lib/master-write.mjs の logizardLockOf) と同じ決まり。ほかのロール (夜間ロード・持ち主) は今までどおり (夜間ロードはこの 2 列を書かない = 0027)
--   2. 列の update の権限は scripts/company-db/create-master-edit-roles.mjs (MASTER_EDIT_WRITE) で渡す (権限の正はロールの script = 流し直しで消えない)
--   0051 の守り (trg_master_edit_guard = 約束・段階・操作・相手の行) はそのまま効く (この 2 列は持ち主のキーが無い = 持ち主表では止めない)
-- 既にある行は変えない

create function core.guard_master_edit_logizard_flags() returns trigger
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_db_user text := case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;
begin
  if v_db_user is distinct from 'master_edit' then return new; end if;
  if new.expiry_managed is not distinct from old.expiry_managed and new.inbound_date_managed is not distinct from old.inbound_date_managed then return new; end if;
  if new.inbound_date_managed is null and old.inbound_date_managed is not null then
    raise exception 'logizard_locked: 入荷日の管理は空 (不明) に戻さない (商品 %)', new.product_id using errcode = '42501';
  end if;
  if not exists (select 1 from core.skus s where s.product_id = new.product_id)
     or exists (select 1 from core.skus s left join ops.master_registrations r on r.sku_id = s.sku_id
                 where s.product_id = new.product_id and r.state is distinct from 'draft')
     or exists (select 1 from core.skus s join ops.ne_reg_export_items i on i.sku_id = s.sku_id
                 where s.product_id = new.product_id and i.state in ('issued', 'import_declared', 'partial', 'verified')) then
    raise exception 'logizard_locked: ロジザードの有効期限・入荷日の管理は、新商品の下書きの間 (NE 登録の CSV を配る前) だけ画面で直せる (商品 %)', new.product_id using errcode = '42501';
  end if;
  return new;
end $$;
revoke all on function core.guard_master_edit_logizard_flags() from public;
create trigger trg_master_edit_logizard_flags before update of expiry_managed, inbound_date_managed on core.products
  for each row execute function core.guard_master_edit_logizard_flags();
