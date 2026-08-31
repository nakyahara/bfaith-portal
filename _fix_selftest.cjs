const fs = require('fs');
const p = 'vps-proxy/aupay-proxy.js';
let s = fs.readFileSync(p, 'utf8');

const oldCase2 = `  // 2) バリエーションあり。商品価格が SubCodeInfo の価格に引っ張られないこと
  const withSub = \`<ResultSet><Result><ItemCode>v-001</ItemCode><Name>バリ商品</Name>
    <Price>2000</Price>
    <SubCodeInfo><SubCode>v-001-a</SubCode><Price>2100</Price></SubCodeInfo>
    <SubCodeInfo><SubCode>v-001-b</SubCode><Price>2200</Price></SubCodeInfo>
    </Result></ResultSet>\`;`;

const newCase2 = `  // 2) バリエーションあり (SKU別に価格を持つ形)。商品価格がサブの価格に引っ張られないこと。
  //    ※旧テストは M0 時点で想像した <SubCodeInfo> 構造だった。実測の形 (code 属性 + <SubCodes> 内) に直した
  const withSub = \`<ResultSet><Result><ItemCode>v-001</ItemCode><Name>バリ商品</Name>
    <Price>2000</Price>
    <SubCodes>
      <SubCode code="v-001-a" quantity="1"><Price>2100</Price></SubCode>
      <SubCode code="v-001-b" quantity="2"><Price>2200</Price></SubCode>
    </SubCodes>
    </Result></ResultSet>\`;`;

const oldCase3 = `  // 3) SubCodeInfo が Price より前にあっても商品価格を取り違えない
  const subFirst = \`<ResultSet><Result><ItemCode>v-002</ItemCode>
    <SubCodeInfo><SubCode>v-002-a</SubCode><Price>500</Price></SubCodeInfo>
    <Price>900</Price><Name>順序違い</Name></Result></ResultSet>\`;`;

const newCase3 = `  // 3) サブの塊が商品Price より前にあっても取り違えない
  const subFirst = \`<ResultSet><Result><ItemCode>v-002</ItemCode>
    <SubCodes><SubCode code="v-002-a"><Price>500</Price></SubCode></SubCodes>
    <Price>900</Price><Name>順序違い</Name></Result></ResultSet>\`;`;

let n = 0;
for (const [from, to] of [[oldCase2, newCase2], [oldCase3, newCase3]]) {
  if (!s.includes(from)) { console.error('見つからない:', from.slice(0, 40)); process.exit(1); }
  s = s.replace(from, to); n++;
}
fs.writeFileSync(p, s);
console.log('置換', n, '件');
