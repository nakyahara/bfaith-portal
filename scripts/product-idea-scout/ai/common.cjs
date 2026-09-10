'use strict';
const crypto = require('node:crypto');
function canonical(value) {
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return '[' + value.map(canonical).join(',') + ']';
  return '{' + Object.keys(value).sort().map(k => JSON.stringify(k) + ':' + canonical(value[k])).join(',') + '}';
}
const hash = value => crypto.createHash('sha256').update(canonical(value)).digest('hex');
function requireValue(ok, code, detail = '') {
  if (!ok) { const e = new Error(code + (detail ? ': ' + detail : '')); e.code = code; throw e; }
}
function dateMs(value) {
  return typeof value === 'string' && /^\d{4}-\d{2}-\d{2}(?:T.*(?:Z|[+-]\d{2}:\d{2}))?$/.test(value)
    ? Date.parse(value) : NaN;
}
module.exports = { canonical, hash, requireValue, dateMs };
