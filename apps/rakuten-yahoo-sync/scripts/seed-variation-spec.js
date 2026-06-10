#!/usr/bin/env node
/**
 * variation_spec_mapping seed CLI (Phase E-4)。
 * バリ品の axis_label → Yahoo spec_id を手動入力する。
 *
 * 使い方:
 *   node apps/rakuten-yahoo-sync/scripts/seed-variation-spec.js --axis サイズ --category 1933 --spec-id 78
 *   node apps/rakuten-yahoo-sync/scripts/seed-variation-spec.js --list
 *   node apps/rakuten-yahoo-sync/scripts/seed-variation-spec.js --delete --axis サイズ --category 1933
 */

import { getDB } from '../db.js';

function parseArgs(argv) {
  const out = { action: 'add' };
  for (let i = 2; i < argv.length; i += 1) {
    const a = argv[i];
    if (a === '--list') out.action = 'list';
    else if (a === '--delete') out.action = 'delete';
    else if (a === '--axis') out.axis = argv[++i];
    else if (a === '--category') {
      const raw = argv[++i];
      if (!/^[1-9]\d*$/.test(raw || '')) {
        console.error(`--category must be a positive integer >= 1 (got: ${JSON.stringify(raw)})`);
        process.exit(2);
      }
      out.category = parseInt(raw, 10);
    }
    else if (a === '--spec-id') out.specId = argv[++i];
    else if (a === '--help' || a === '-h') {
      console.log('Usage: seed-variation-spec.js [--list | --delete] --axis <name> --category <int> [--spec-id <id>]');
      process.exit(0);
    } else {
      console.error(`unknown arg: ${a}`);
      process.exit(2);
    }
  }
  return out;
}

function main() {
  const args = parseArgs(process.argv);
  const db = getDB();
  if (args.action === 'list') {
    const rows = db.prepare(`
      SELECT axis_label, yahoo_product_category, spec_id, updated_at
        FROM variation_spec_mapping
       ORDER BY axis_label, yahoo_product_category
    `).all();
    if (rows.length === 0) console.log('(no rows)');
    else console.table(rows);
    return;
  }
  if (!args.axis || !Number.isFinite(args.category)) {
    console.error('--axis and --category are required');
    process.exit(2);
  }
  if (args.action === 'delete') {
    const r = db.prepare(`
      DELETE FROM variation_spec_mapping WHERE axis_label = ? AND yahoo_product_category = ?
    `).run(args.axis, args.category);
    console.log(JSON.stringify({ deleted: r.changes }));
    return;
  }
  if (!args.specId) {
    console.error('--spec-id is required for add');
    process.exit(2);
  }
  const r = db.prepare(`
    INSERT INTO variation_spec_mapping (axis_label, yahoo_product_category, spec_id)
    VALUES (?, ?, ?)
    ON CONFLICT(axis_label, yahoo_product_category) DO UPDATE SET
      spec_id    = excluded.spec_id,
      updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
  `).run(args.axis, args.category, args.specId);
  console.log(JSON.stringify({ axis: args.axis, category: args.category, spec_id: args.specId, changes: r.changes }));
}

try { main(); } catch (e) {
  console.error(`[seed-variation-spec] FATAL: ${e.message}`);
  process.exit(1);
}
