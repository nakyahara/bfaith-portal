#!/usr/bin/env node
/** test-yahoo-order-contact.cjs — PR-Y-B 純粋ロジックのスモーク。実行: node vps-proxy/test-yahoo-order-contact.cjs */
'use strict';
const { isValidOrderId, parseOrderContactXml, authorizeStatusOk, CONTACT_FIELDS } = require('./yahoo-order-contact.js');
let passed = 0, failed = 0;
const check = (name, cond, detail = '') => { if (cond) { console.log(`  ✅ ${name}`); passed++; } else { console.log(`  ❌ ${name} ${detail}`); failed++; } };

check('orderId 形式: 正常', isValidOrderId('b-faith01-10288658'));
check('orderId 形式: 不正 (空/空白/長すぎ/記号)', !isValidOrderId('') && !isValidOrderId('a b') && !isValidOrderId('x'.repeat(70)) && !isValidOrderId('a;b') && !isValidOrderId(123));
check('CONTACT_FIELDS に PII は BillMailAddress だけ (氏名・電話は取らない)', CONTACT_FIELDS === 'OrderId,OrderStatus,ShipStatus,ShipDate,SocialGiftType,BillMailAddress');

const ok = `<?xml version='1.0' encoding='UTF-8'?><ResultSet totalResultsAvailable="1"><Result><Status>OK</Status><OrderInfo><OrderId>b-faith01-1</OrderId><OrderStatus>2</OrderStatus><Pay><BillMailAddress><![CDATA[user+tag@Example.co.jp]]></BillMailAddress></Pay><Ship><ShipDate>2026-08-27</ShipDate><ShipStatus>3</ShipStatus></Ship><SocialGiftType>0</SocialGiftType></OrderInfo></Result></ResultSet>`;
const r = parseOrderContactXml(ok);
check('正常 XML: CDATA のメール・発送日・ステータスを抽出', r.ok && r.contact.email === 'user+tag@Example.co.jp' && r.contact.shipDate === '2026-08-27' && r.contact.shipStatus === '3' && r.contact.socialGiftType === '0' && r.contact.orderId === 'b-faith01-1', JSON.stringify(r));
const unshipped = ok.replace('<ShipDate>2026-08-27</ShipDate>', '<ShipDate></ShipDate>').replace('<ShipStatus>3</ShipStatus>', '<ShipStatus>1</ShipStatus>');
check('未発送: ShipDate 空', parseOrderContactXml(unshipped).contact.shipDate === '');
const err = `<?xml version='1.0'?><Error><Message>Request Parameter Error : IsGift</Message><Code>od90101</Code></Error>`;
const e = parseOrderContactXml(err);
check('<Error> は ok=false + code', !e.ok && e.error === 'yahoo_error' && e.code === 'od90101');
const notFound = `<ResultSet><Result><Status>NG</Status><Message>not found</Message></Result></ResultSet>`;
check('Status NG は ok=false', !parseOrderContactXml(notFound).ok && parseOrderContactXml(notFound).error === 'yahoo_status');
check('OrderInfo 無しは ok=false', !parseOrderContactXml('<ResultSet><Result><Status>OK</Status></Result></ResultSet>').ok);
check('authorizeStatusOk: authorized だけ true', authorizeStatusOk('authorized') && !authorizeStatusOk('none') && !authorizeStatusOk('expired-key-version') && !authorizeStatusOk(undefined));

console.log(`\n結果: ${passed} PASS / ${failed} FAIL`);
process.exit(failed > 0 ? 1 : 0);
