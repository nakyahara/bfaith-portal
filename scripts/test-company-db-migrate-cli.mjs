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
const strict = { rejectUnauthorized: true };

t('[!] Render の External URL は TLS + 証明書検証あり', () => {
  const o = pgClientOptions('postgres://u:p@dpg-abc123-a.singapore-postgres.render.com:5432/company_db');
  assert.deepEqual(o.ssl, strict);
  assert.equal(o.application_name, 'company-db-migrate');
  assert.match(o.connectionString, /^postgres:\/\//);
});

t('[!] TLS 無しは loopback と Render の内部ホスト名だけ (明示的に ssl:false = PGSSLMODE を継承しない)', () => {
  assert.equal(pgClientOptions('postgres://u:p@dpg-abc123-a:5432/company_db').ssl, false);
  assert.equal(pgClientOptions('postgres://u:p@dpg-abc123:5432/company_db').ssl, false);
  assert.equal(pgClientOptions('postgres://u:p@localhost:5432/x').ssl, false);
  assert.equal(pgClientOptions('postgresql://u:p@127.0.0.1/x').ssl, false);
  assert.equal(pgClientOptions('postgres://u:p@[::1]:5432/x').ssl, false);
});

t('[!] IPv6 直指定・私有 IP・短い名前は「内部」扱いにしない (TLS 検証あり)', () => {
  assert.deepEqual(pgClientOptions('postgres://u:p@[2001:db8::1]:5432/x').ssl, strict);
  assert.deepEqual(pgClientOptions('postgres://u:p@10.0.0.1/x').ssl, strict);
  assert.deepEqual(pgClientOptions('postgres://u:p@db/x').ssl, strict);
  assert.deepEqual(pgClientOptions('postgres://u:p@127.0.0.2/x').ssl, strict);
  assert.deepEqual(pgClientOptions('postgres://u:p@DPG-abc/x').ssl, strict);     // 大文字は Render の内部名ではない
});

t('[!] URL のクエリで TLS や接続先を差し替えられない (?ssl= / ?sslmode= / ?host= などは拒む)', () => {
  for (const q of ['?ssl=no-verify', '?sslmode=disable', '?sslmode=require', '?ssl=false', '?sslrootcert=/x', '?uselibpqcompat=true', '?host=external.example.com', '?hostaddr=1.2.3.4', '?port=1']) {
    assert.throws(() => pgClientOptions('postgres://u:p@h.example.com/x' + q), /クエリ .* を付けない/, q);
  }
  // 🚨 pg はクエリの host を URL 本体より優先する。localhost に見せかけて外部へ TLS 無しで繋ぐ抜け道 (Codex R2)
  assert.throws(() => pgClientOptions('postgres://u:p@localhost/db?host=external.example.com'), /クエリ host/);
  assert.doesNotThrow(() => pgClientOptions('postgres://u:p@h.example.com/x?application_name=x'));
});

t('接続文字列が壊れている・postgres 以外は例外 (黙って localhost に繋がない)', () => {
  assert.throws(() => pgClientOptions('not a url'));
  assert.throws(() => pgClientOptions('mysql://u:p@h.example.com/x'), /postgres/);
});

console.log(`\n${passed} 件 PASS`);
