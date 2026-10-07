-- 0048 0047 の CHECK を既存の行で確かめる (D7b-1a・#1554 Codex R1 Medium。2026-09-30)
--
-- 0047 は core.order_finance_daily の「分けられない部品」の 4 列の CHECK と、今の形の版の行の等式の CHECK を NOT VALID で足した
--   (新しく入る・変わる行には効く。既存の 59 万行の検査を 0047 の ACCESS EXCLUSIVE の lock の中で migration の commit まで続けない)。
-- ここで既存の行を確かめる。VALIDATE CONSTRAINT の lock は SHARE UPDATE EXCLUSIVE = 読み書き (受け口の apply・画面の読み取り) を止めない。
-- 既存の行は 4 列とも 0 (0047 の既定)・版は amazon_finance_v1 (等式の対象の外) = 必ず通る。通らなければ migration が失敗して止まる (誰かが壊れた値を入れた)
alter table core.order_finance_daily validate constraint ck_order_finance_daily_unclassified;
alter table core.order_finance_daily validate constraint ck_order_finance_daily_unmapped_count;
alter table core.order_finance_daily validate constraint ck_order_finance_daily_class_form;
