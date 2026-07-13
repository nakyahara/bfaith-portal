/**
 * 出荷明細メールの自動取得・解析 (ロジザード入荷予定の作成の入力を自動化する)
 *
 *   AMC (アメージングクラフト): @am-craft.jp から xlsx 添付「出荷明細表」(見出し行=商品コード/商品名/出荷数量)
 *   ビーフリー:                @be-free.biz から件名「出荷明細」、本文HTMLの表 (商品ID/商品名/出荷数)
 *
 * Gmail readonly (既存の PO_GMAIL_* 環境変数) で取得し、解析済み明細を po_shipment_mails に保存する。
 * gmail_id UNIQUE で同じメールは二重登録されない。変換 (対応表逆引き+原価) は router 側の共通ロジック。
 * テスト: PO_SHIPMENT_FAKE_DATA (JSON) で Gmail API を偽装できる。
 */
import ExcelJS from 'exceljs';
import { getDB, normSupplierCode } from './db.js';
import { gmailApiGet } from './email.js';
import { audit } from './ledger.js';

const nowIso = () => new Date().toISOString();

// 仕入先とメールの対応 (増えたら設定へ昇格を検討。domain は From アドレスのドメイン一致)
export const SHIPMENT_MAIL_RULES = [
  { supplierCode: '1', domain: 'am-craft.jp', kind: 'xlsx' },
  { supplierCode: '2', domain: 'be-free.biz', kind: 'body', subjectFilter: '出荷明細' },
];

const b64urlToBuf = s => Buffer.from(String(s || '').replace(/-/g, '+').replace(/_/g, '/'), 'base64');

function ruleFor(fromAddr, subject) {
  const from = String(fromAddr || '').toLowerCase();
  for (const r of SHIPMENT_MAIL_RULES) {
    if (!from.includes('@' + r.domain) && !from.endsWith(r.domain + '>')) continue;
    if (r.subjectFilter && !String(subject || '').includes(r.subjectFilter)) continue;
    return r;
  }
  return null;
}

/** xlsx添付 (AMC出荷明細表) → [{vendorCode, vendorName, qty}]。見出し行 (商品コード+出荷数量) を探して以降を読む */
export async function parseShipmentXlsx(buf) {
  const wb = new ExcelJS.Workbook();
  await wb.xlsx.load(buf);
  const ws = wb.worksheets[0];
  if (!ws) throw new Error('シートがありません');
  const items = [];
  let headerRow = 0, colCode = 0, colName = 0, colQty = 0;
  ws.eachRow((row, rowNo) => {
    if (!headerRow) {
      row.eachCell((cell, colNo) => {
        const v = String(cell.value == null ? '' : cell.value).trim();
        if (v === '商品コード' || v === '商品ID') colCode = colNo;
        if (v === '商品名') colName = colNo;
        if (v === '出荷数量' || v === '出荷数' || v === '数量') colQty = colNo;
      });
      if (colCode && colQty) headerRow = rowNo;
      return;
    }
    const code = String(row.getCell(colCode).value == null ? '' : row.getCell(colCode).value).trim();
    const qtyRaw = row.getCell(colQty).value;
    const qty = Number(String(qtyRaw == null ? '' : qtyRaw).replace(/,/g, ''));
    if (!code || !Number.isInteger(qty) || qty <= 0) return;
    const name = colName ? String(row.getCell(colName).value == null ? '' : row.getCell(colName).value).trim() : '';
    items.push({ vendorCode: code, vendorName: name || null, qty });
  });
  if (!headerRow) throw new Error('見出し行 (商品コード/出荷数量) が見つかりません');
  return items;
}

/** 本文HTML (ビーフリーの表) → [{vendorCode, vendorName, qty}]。<tr>/<td> を素朴に読む */
export function parseShipmentHtml(html) {
  const stripTags = s => s.replace(/<[^>]*>/g, '').replace(/&nbsp;/g, ' ').replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"').replace(/&#39;/g, "'").trim();
  const items = [];
  const trRe = /<tr[^>]*>([\s\S]*?)<\/tr>/gi;
  let tr;
  while ((tr = trRe.exec(String(html || ''))) !== null) {
    const cells = [];
    const tdRe = /<t[dh][^>]*>([\s\S]*?)<\/t[dh]>/gi;
    let td;
    while ((td = tdRe.exec(tr[1])) !== null) cells.push(stripTags(td[1]));
    if (cells.length < 2) continue;
    const code = cells[0];
    if (!code || /商品ID|商品コード/.test(code)) continue; // 見出し行
    // 末尾側から最初に正の整数になるセル = 数量 (商品名内の数字を拾わない)
    let qty = null, qtyIdx = -1;
    for (let i = cells.length - 1; i >= 1; i--) {
      const n = Number(cells[i].replace(/,/g, ''));
      if (cells[i] !== '' && Number.isInteger(n) && n > 0) { qty = n; qtyIdx = i; break; }
    }
    if (qty == null) continue;
    items.push({ vendorCode: code, vendorName: cells.slice(1, qtyIdx).filter(Boolean).join(' ') || null, qty });
  }
  return items;
}

/** Gmail message payload から text/html と xlsx添付 ({filename, attachmentId}) を集める */
function walkParts(payload, out) {
  if (!payload) return out;
  const mime = payload.mimeType || '';
  const fname = payload.filename || '';
  if (fname && /\.xlsx$/i.test(fname) && payload.body && payload.body.attachmentId) {
    out.attachments.push({ filename: fname, attachmentId: payload.body.attachmentId });
  }
  if (mime === 'text/html' && payload.body && payload.body.data) out.html += b64urlToBuf(payload.body.data).toString('utf8');
  for (const p of payload.parts || []) walkParts(p, out);
  return out;
}

function headerOf(payload, name) {
  const h = (payload.headers || []).find(x => String(x.name).toLowerCase() === name.toLowerCase());
  return h ? h.value : '';
}

/** 偽装データ (テスト用): [{id, from, subject, internalDate, bodyHtml?, attachments?: [{filename, dataBase64}]}] */
function fakeMails() {
  try { return JSON.parse(process.env.PO_SHIPMENT_FAKE_DATA || '[]'); } catch { return []; }
}

/**
 * 出荷明細メールを取得して po_shipment_mails に登録する (既存gmail_idはスキップ=冪等)。
 * @returns {checked, added, errors: [{gmailId, error}]}
 */
export async function fetchShipmentMails(actor, { newerThanDays = 30 } = {}) {
  const db = getDB();
  const exists = db.prepare('SELECT 1 FROM po_shipment_mails WHERE gmail_id=?');
  const ins = db.prepare(`INSERT INTO po_shipment_mails
    (gmail_id, supplier_code, from_addr, subject, received_at, parsed_json, parse_note, status, error, created_at, updated_at)
    VALUES (?,?,?,?,?,?,?,?,?,?,?)`);
  const result = { checked: 0, added: 0, errors: [] };

  const useFake = !!process.env.PO_SHIPMENT_FAKE_DATA;
  let candidates; // [{id, from, subject, internalDateIso, getHtml(), getXlsxBuffers()}]
  if (useFake) {
    candidates = fakeMails().map(m => ({
      id: m.id, from: m.from, subject: m.subject, internalDateIso: m.internalDate || nowIso(),
      getHtml: async () => m.bodyHtml || '',
      getXlsx: async () => (m.attachments || []).filter(a => /\.xlsx$/i.test(a.filename))
        .map(a => ({ filename: a.filename, buf: Buffer.from(a.dataBase64, 'base64') })),
    }));
  } else {
    const domains = SHIPMENT_MAIL_RULES.map(r => `from:${r.domain}`).join(' OR ');
    const q = `(${domains}) newer_than:${newerThanDays}d`;
    const list = await gmailApiGet('messages?maxResults=50&q=' + encodeURIComponent(q));
    const metas = list.messages || [];
    candidates = [];
    for (const meta of metas) {
      if (exists.get(meta.id)) continue; // 取得済みは本文をダウンロードしない
      const msg = await gmailApiGet(`messages/${encodeURIComponent(meta.id)}?format=full`);
      const collected = walkParts(msg.payload, { html: '', attachments: [] });
      candidates.push({
        id: msg.id,
        from: headerOf(msg.payload, 'From'), subject: headerOf(msg.payload, 'Subject'),
        internalDateIso: msg.internalDate ? new Date(Number(msg.internalDate)).toISOString() : nowIso(),
        getHtml: async () => collected.html,
        getXlsx: async () => {
          const out = [];
          for (const a of collected.attachments) {
            const att = await gmailApiGet(`messages/${encodeURIComponent(msg.id)}/attachments/${encodeURIComponent(a.attachmentId)}`, 60000);
            out.push({ filename: a.filename, buf: b64urlToBuf(att.data) });
          }
          return out;
        },
      });
    }
  }

  for (const c of candidates) {
    result.checked++;
    if (exists.get(c.id)) continue;
    const rule = ruleFor(c.from, c.subject);
    if (!rule) continue; // 対象外の差出人/件名 (請求書等) は登録しない
    let items = [], note = null, status = 'new', error = null;
    try {
      if (rule.kind === 'xlsx') {
        const files = await c.getXlsx();
        if (!files.length) throw new Error('xlsx添付がありません');
        items = await parseShipmentXlsx(files[0].buf);
        note = files[0].filename + (files.length > 1 ? ` (他${files.length - 1}添付は未解析)` : '');
      } else {
        items = parseShipmentHtml(await c.getHtml());
        if (!items.length) throw new Error('本文から明細の表を読み取れません');
      }
      if (!items.length) throw new Error('明細が0行です');
    } catch (e) {
      status = 'error';
      error = String(e.message || e);
    }
    ins.run(c.id, rule.supplierCode, String(c.from || '').slice(0, 200), String(c.subject || '').slice(0, 200),
      c.internalDateIso, JSON.stringify(items), note, status, error, nowIso(), nowIso());
    result.added++;
    if (error) result.errors.push({ gmailId: c.id, error });
  }
  if (result.added > 0) {
    audit(db, { actorType: 'user', actor, action: 'shipment_mail_fetch', resource: 'shipment_mails',
      detail: { checked: result.checked, added: result.added, errors: result.errors.length } });
  }
  return result;
}

export function listShipmentMails() {
  const db = getDB();
  // new と error を表示 (done/ignored は直近5件だけ参考表示)
  const open = db.prepare("SELECT * FROM po_shipment_mails WHERE status IN ('new','error') ORDER BY received_at DESC").all();
  const recent = db.prepare("SELECT * FROM po_shipment_mails WHERE status IN ('done','ignored') ORDER BY updated_at DESC LIMIT 5").all();
  return { open, recent };
}

export function setShipmentMailStatus(id, status, actor) {
  if (!['done', 'ignored', 'new'].includes(status)) throw new Error(`不正なstatus: ${status}`);
  const db = getDB();
  const row = db.prepare('SELECT * FROM po_shipment_mails WHERE id=?').get(id);
  if (!row) throw new Error('メールが見つかりません');
  db.transaction(() => {
    db.prepare('UPDATE po_shipment_mails SET status=?, updated_at=? WHERE id=?').run(status, nowIso(), id);
    audit(db, { actorType: 'user', actor, action: 'shipment_mail_status', resource: `shipment_mail:${id}`,
      detail: { from: row.status, to: status, gmailId: row.gmail_id } });
  })();
}
