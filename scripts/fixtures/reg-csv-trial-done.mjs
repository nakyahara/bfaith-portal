/**
 * reg-csv-trial-done.mjs — 試験だけ: 「試し用・閉じた (全部の商品が終わった)・商品は全部 NE で確かめた」NE 登録の CSV のファイルを 1 つ置く (10/12 中原さんの画面の写真のファイル #2 の形)
 *
 * なぜ: 画面の「この形を実機で確かめた」のボタンを出すか (lib の regVerifyTargetOf) を、版ごと (引退した v1・門のあるセット・門の無い v2 / まとまり) に描いて確かめたい。
 *   本物の道 (作る → 配る → 翌朝の照合) で作れるのは今の作れる版だけ (v1 は 0065 の前の DB でしか作れない) = 行を直接置く。
 *   本番は ops.ne_reg_* の関数だけが書く (表の守りの trigger) = 試験の接続のログイン (superuser) で session_replication_role = replica にして守りを外し、終わったら戻す
 * 使い方: const id = await seedTrialDoneExport(pg, { sessionUser, schema: 'ne-reg-single-v1', code: 's003' }) → 番号 (文字)
 */
import crypto from 'node:crypto';
import { REG_SCHEMA_RULES } from '../../lib/master-reg-csv.mjs';

const hex = (s) => crypto.createHash('sha256').update(String(s)).digest('hex');

/** pg = PGlite (または query を持つもの)・sessionUser = 試験の接続のログイン (superuser)・role = 戻すロール (既定 deploy)。code = 置く商品 (版の sku_kind と同じ種類の商品) */
export async function seedTrialDoneExport(pg, { sessionUser, schema, code, role = 'deploy', run = 'mc_seed_trial_done' }) {
  const rule = Object.prototype.hasOwnProperty.call(REG_SCHEMA_RULES, schema) ? REG_SCHEMA_RULES[schema] : null;
  if (!rule) throw new Error(`知らない版: ${schema}`);
  const tag = `${schema}|${code}|${Date.now()}|${Math.random()}`;
  await pg.query(`set session authorization ${sessionUser}`);
  try {
    await pg.query('set session_replication_role = replica');
    const s = (await pg.query('select sku_id::text as id, code, code_norm, sku_kind from core.skus where code = $1', [code])).rows[0];
    if (!s) throw new Error(`商品が無い: ${code}`);
    if (s.sku_kind !== rule.skuKind) throw new Error(`${code} は ${s.sku_kind} (${schema} = ${rule.skuKind})`);
    const e = (await pg.query(`insert into ops.ne_reg_exports (kind, schema_version, header, encoding, trial, item_count, row_count, aggregate_token, payload_hash, sha256, file_bytes, request_id, ne_codes_run, cost_day,
        created_by, state, issued_at, issued_by, closed_at, closed_by, close_reason)
      values ($1, $2, $3, 'utf8', true, 1, 1, $4, $5, $6, '\\x00'::bytea, $7::uuid, $8, current_date, 'seed@test', 'closed', now(), 'seed@test', now(), 'system', 'finished') returning export_id::text as id`,
    [rule.kind, schema, rule.header.join(','), hex('agg' + tag), hex('payload' + tag), hex('sha' + tag), crypto.randomUUID(), run])).rows[0].id;
    await pg.query(`insert into ops.ne_reg_export_items (export_id, sku_id, code_norm, ne_code, sku_kind, item_token, expected, snapshot_hash, row_from, row_to, state, state_changed_by, verified_run, verified_at)
      values ($1, $2, $3, $4, $5, $6, '{}'::jsonb, $7, 1, 1, 'verified', 'system', $8, now())`,
    [e, s.id, s.code_norm, s.code, s.sku_kind, hex('item' + tag), hex('snap' + tag), run]);
    return e;
  } finally {
    await pg.query('set session_replication_role = origin');
    await pg.query(`set session authorization ${sessionUser}`);
    await pg.query(`set role ${role}`);
  }
}
