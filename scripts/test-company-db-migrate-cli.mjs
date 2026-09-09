/**
 * test-company-db-migrate-cli.mjs — migrate.mjs の接続オプション (TLS の扱い) の試験。pg も PGlite も要らない
 * 使い方: node scripts/test-company-db-migrate-cli.mjs
 */
import assert from 'node:assert/strict';
import { pgClientOptions } from './company-db/migrate.mjs';

let passed = 0;
function t(name, fn) {
  try { fn(); passed++; console.log(`  ok  ${name}`); }
  catch (e) { console.error(`  NG  ${name}\n      ${e.message}`); process.exitCode = 1; }
}

t('[!] Render の External URL (ホスト名にドット) は TLS + 証明書検証あり。検証を切る手段は無い', () => {
  const o = pgClientOptions('postgres://u:p@dpg-abc123-a.singapore-postgres.render.com:5432/company_db');
  assert.deepEqual(o.ssl, { rejectUnauthorized: true });
  assert.equal(o.application_name, 'company-db-migrate');
  assert.match(o.connectionString, /^postgres:\/\//);
  assert.equal(pgClientOptions.length, 1);   // env で挙動を変える引数を持たない
});

t('Render の Internal URL (ホスト名にドット無し) と localhost は TLS 無し', () => {
  assert.equal(pgClientOptions('postgres://u:p@dpg-abc123-a:5432/company_db').ssl, undefined);
  assert.equal(pgClientOptions('postgres://u:p@localhost:5432/x').ssl, undefined);
  assert.equal(pgClientOptions('postgresql://u:p@127.0.0.1/x').ssl, undefined);
});

t('接続文字列が壊れていれば例外 (黙って localhost に繋がない)', () => {
  assert.throws(() => pgClientOptions('not a url'));
});

console.log(`\n${passed} 件 PASS`);
