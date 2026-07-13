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

/** Fromヘッダから実メールボックスを抽出 ("表示名 <a@b>" → a@b。表示名にドメインを書く偽装を通さない、Codex mail-R1 High) */
export function mailboxOf(fromHeader) {
  const s = String(fromHeader || '').trim();
  const m = s.match(/<([^<>]+)>\s*$/);
  const box = (m ? m[1] : s).trim().toLowerCase();
  return /^[^\s@]+@[^\s@]+$/.test(box) ? box : null;
}

function ruleFor(fromHeader, subject) {
  const box = mailboxOf(fromHeader);
  if (!box) return null;
  const domain = box.slice(box.indexOf('@') + 1);
  for (const r of SHIPMENT_MAIL_RULES) {
    if (domain !== r.domain) continue; // ドメイン完全一致のみ (部分一致・表示名は不可)
    if (r.subjectFilter && !String(subject || '').includes(r.subjectFilter)) continue;
    return r;
  }
  return null;
}

/**
 * Gmailが付与する Authentication-Results で送信元の真正性を確認する (From詐称対策、Codex mail-R1 High)。
 * Fromドメインとの整合まで検証する (Codex mail-R2 High):
 *  - dmarc=pass (dmarcは定義上From整合。header.from があればFromドメインと一致することも確認)
 *  - または spf=pass の smtp.mailfrom と dkim=pass の header.d の両方がFromドメインに整合
 * 節 (;区切り) 単位で解析し、dmarc=passive 等の部分一致誤認を防ぐ。検証不能は false (自動変換させない)
 */
/**
 * Authentication-Results を ';' 節に分割する状態機械。quoted-string (quoted-pair \x 対応) と
 * 入れ子コメント () の中身を捨て、その内側の ';' を節区切りにしない (正規表現置換では
 * `reason="x\"; dmarc=pass"` のようなエスケープで迂回できる、Codex mail-R4 High / RFC 8601・5322)
 */
export function splitAuthClauses(header) {
  const s = String(header || '');
  const clauses = [];
  let cur = '', depth = 0, inQ = false, esc = false;
  for (let i = 0; i < s.length; i++) {
    const ch = s[i];
    if (esc) { esc = false; continue; }
    if (inQ) {
      if (ch === '\\') { esc = true; continue; }
      if (ch === '"') inQ = false;
      continue;
    }
    if (depth > 0) {
      if (ch === '\\') { esc = true; continue; }
      if (ch === '(') depth++;
      else if (ch === ')') depth--;
      continue;
    }
    if (ch === '"') { inQ = true; continue; }
    if (ch === '(') { depth = 1; continue; }
    if (ch === ';') { clauses.push(cur.trim()); cur = ''; continue; }
    cur += ch;
  }
  clauses.push(cur.trim());
  return clauses;
}

export function authPassed(authResultsHeaders, fromDomain) {
  const headers = Array.isArray(authResultsHeaders) ? authResultsHeaders : [authResultsHeaders];
  const d = String(fromDomain || '').toLowerCase();
  if (!d) return false;
  const align = v => !!v && (v === d || v.endsWith('.' + d) || d.endsWith('.' + v));
  for (const raw of headers) {
    const clauses = splitAuthClauses(String(raw || '').toLowerCase());
    // 先頭節 = authserv-id。Gmail自身 (mx.google.com) が付与したヘッダだけを信頼する
    // (外部から持ち込まれた同名ヘッダを判定に使わない、Codex mail-R4 High)
    const authserv = (clauses[0] || '').split(/\s+/)[0];
    if (authserv !== 'mx.google.com') continue;
    let dmarcFromDom = null, spfDom = null;
    const dkimDoms = [];
    for (const c of clauses.slice(1)) {
      // method=result は「節の先頭」のみ有効 (RFC 8601 resinfo)
      const m = c.match(/^(dmarc|spf|dkim)=([a-z0-9]+)/);
      if (!m || m[2] !== 'pass') continue;
      if (m[1] === 'dmarc') {
        const hf = c.match(/header\.from=([^\s]+)/);
        if (hf) dmarcFromDom = hf[1]; // header.from の明示整合のみ許可 (無記載passは採用しない)
      } else if (m[1] === 'spf') {
        const mf = c.match(/smtp\.mailfrom=(?:[^@\s]*@)?([^\s]+)/);
        if (mf) spfDom = mf[1];
      } else {
        const hd = c.match(/header\.d=([^\s]+)/) || c.match(/header\.i=@?(?:[^@\s]*@)?([^\s]+)/);
        if (hd) dkimDoms.push(hd[1]);
      }
    }
    if (align(dmarcFromDom)) return true;
    if (align(spfDom) && dkimDoms.some(align)) return true;
  }
  return false;
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
      // 見出しは「同一行」に商品コード系+出荷数系が揃った行だけ (行またぎで蓄積しない、Codex mail-R2 Medium)
      let c = 0, n = 0, q = 0;
      row.eachCell((cell, colNo) => {
        const v = String(cell.value == null ? '' : cell.value).trim();
        if (v === '商品コード' || v === '商品ID') c = colNo;
        if (v === '商品名') n = colNo;
        if (v === '出荷数量' || v === '出荷数' || v === '数量') q = colNo;
      });
      if (c && q) { headerRow = rowNo; colCode = c; colName = n; colQty = q; }
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

/**
 * 本文HTML (ビーフリーの表) → [{vendorCode, vendorName, qty}]。
 * 「商品ID/商品コード」と「出荷数/出荷数量/数量」の見出しを持つ表だけを対象にし、見出しの列位置で読む
 * (署名・レイアウト・問い合わせ番号などの無関係な表を明細として誤解釈しない、Codex mail-R1 Medium)
 */
export function parseShipmentHtml(html) {
  const stripTags = s => s.replace(/<[^>]*>/g, '').replace(/&nbsp;/g, ' ').replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"').replace(/&#39;/g, "'").trim();
  const rowsOf = tableHtml => {
    const rows = [];
    const trRe = /<tr[^>]*>([\s\S]*?)<\/tr>/gi;
    let tr;
    while ((tr = trRe.exec(tableHtml)) !== null) {
      const cells = [];
      const tdRe = /<t[dh][^>]*>([\s\S]*?)<\/t[dh]>/gi;
      let td;
      while ((td = tdRe.exec(tr[1])) !== null) cells.push(stripTags(td[1]));
      rows.push(cells);
    }
    return rows;
  };
  // 返信引用 (blockquote/gmail_quote) 内の過去の出荷明細を拾わない。さらに明細表が複数あれば
  // 合算せずエラーにする (今回分と過去分の二重入荷防止、Codex mail-R2 Medium)。
  // blockquoteの入れ子は「最内側から繰り返し除去」で対応 (非貪欲1回では外側が残る、Codex mail-R3 Medium)
  let src = String(html || '');
  let prevSrc;
  do {
    prevSrc = src;
    src = src.replace(/<blockquote[^>]*>(?:(?!<blockquote)[\s\S])*?<\/blockquote>/gi, '');
  } while (src !== prevSrc);
  src = src.replace(/<div[^>]*class=["']?[^"'>]*gmail_quote[^"'>]*["']?[\s\S]*$/gi, '');
  let matchedTables = 0;
  const items = [];
  const tableRe = /<table[^>]*>([\s\S]*?)<\/table>/gi;
  let tb;
  while ((tb = tableRe.exec(src)) !== null) {
    const rows = rowsOf(tb[1]);
    // 見出し行を探す (商品ID/商品コード + 出荷数系 が同じ行にある表だけが明細)
    let head = -1, colCode = -1, colName = -1, colQty = -1;
    for (let i = 0; i < rows.length; i++) {
      const cells = rows[i];
      const ci = cells.findIndex(c => c === '商品ID' || c === '商品コード');
      const qi = cells.findIndex(c => c === '出荷数' || c === '出荷数量' || c === '数量');
      if (ci >= 0 && qi >= 0) {
        head = i; colCode = ci; colQty = qi;
        colName = cells.findIndex(c => c === '商品名');
        break;
      }
    }
    if (head < 0) continue; // 見出しの無い表 (署名等) は無視
    matchedTables++;
    if (matchedTables > 1) throw new Error('明細の表が複数あります (返信引用に過去の明細が残っている可能性)。手動貼り付けで今回分だけ変換してください');
    for (let i = head + 1; i < rows.length; i++) {
      const cells = rows[i];
      const code = (cells[colCode] || '').trim();
      const qty = Number(String(cells[colQty] || '').replace(/,/g, ''));
      if (!code || !Number.isInteger(qty) || qty <= 0) continue;
      items.push({ vendorCode: code, vendorName: (colName >= 0 && cells[colName]) ? cells[colName].trim() : null, qty });
    }
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
/** 同名ヘッダを全部返す (Authentication-Resultsは複数あり得る。先頭1つだけ見る順序依存を避ける) */
function headersOf(payload, name) {
  return (payload.headers || []).filter(x => String(x.name).toLowerCase() === name.toLowerCase()).map(x => x.value);
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
  // 一時的な通信障害 (添付ダウンロード等) は行を確定保存せず次回の取得でやり直す。
  // 恒久的な内容エラー (添付なし/形式不正) だけ status='error' で保存する (Codex mail-R1 Medium)
  const transientErr = e => { e.transient = true; return e; };
  let candidates; // [{id, from, subject, authResults, internalDateIso, getHtml(), getXlsx()}]
  if (useFake) {
    candidates = fakeMails().map(m => {
      const dom = (mailboxOf(m.from) || 'x@invalid').split('@')[1];
      return {
      id: m.id, from: m.from, subject: m.subject,
      authResults: m.authResults !== undefined ? m.authResults
        : `mx.google.com; spf=pass smtp.mailfrom=${dom}; dkim=pass header.d=${dom}; dmarc=pass header.from=${dom}`,
      internalDateIso: m.internalDate || nowIso(),
      getHtml: async () => m.bodyHtml || '',
      getXlsx: async () => (m.attachments || []).filter(a => /\.xlsx$/i.test(a.filename))
        .map(a => ({ filename: a.filename, buf: Buffer.from(a.dataBase64, 'base64') })),
      };
    });
  } else {
    const domains = SHIPMENT_MAIL_RULES.map(r => `from:${r.domain}`).join(' OR ');
    const q = `(${domains}) newer_than:${newerThanDays}d`;
    // nextPageToken を最後まで辿る (1ページ50件で頭打ちにならないように、Codex mail-R1 Medium)。10ページ=500通で打ち切り
    const metas = [];
    let pageToken = null;
    for (let page = 0; page < 10; page++) {
      const list = await gmailApiGet('messages?maxResults=50&q=' + encodeURIComponent(q) + (pageToken ? '&pageToken=' + encodeURIComponent(pageToken) : ''));
      metas.push(...(list.messages || []));
      pageToken = list.nextPageToken || null;
      if (!pageToken) break;
    }
    candidates = [];
    for (const meta of metas) {
      if (exists.get(meta.id)) continue; // 取得済みは本文をダウンロードしない
      const msg = await gmailApiGet(`messages/${encodeURIComponent(meta.id)}?format=full`);
      const collected = walkParts(msg.payload, { html: '', attachments: [] });
      candidates.push({
        id: msg.id,
        from: headerOf(msg.payload, 'From'), subject: headerOf(msg.payload, 'Subject'),
        authResults: headersOf(msg.payload, 'Authentication-Results'), // 全ヘッダを渡し、authserv-id=mx.google.comのみ採用

        internalDateIso: msg.internalDate ? new Date(Number(msg.internalDate)).toISOString() : nowIso(),
        getHtml: async () => collected.html,
        getXlsx: async () => {
          const out = [];
          for (const a of collected.attachments) {
            let att;
            try { att = await gmailApiGet(`messages/${encodeURIComponent(msg.id)}/attachments/${encodeURIComponent(a.attachmentId)}`, 60000); }
            catch (e) { throw transientErr(e); } // 添付DL失敗=一時障害扱い (次回取得でやり直し)
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
      // 送信元の真正性 (SPF/DKIM/DMARC のFromドメイン整合)。検証できないメールは自動変換対象にしない (From詐称対策)
      if (!authPassed(c.authResults, rule.domain)) throw new Error('送信元の認証 (SPF/DKIM/DMARC) を確認できません — なりすましの可能性があるため自動変換しません。本物なら手動貼り付けを使ってください');
      if (rule.kind === 'xlsx') {
        const files = await c.getXlsx();
        if (!files.length) throw new Error('xlsx添付がありません');
        items = await parseShipmentXlsx(files[0].buf);
        note = files[0].filename + (files.length > 1 ? ` (他${files.length - 1}添付は未解析)` : '');
      } else {
        items = parseShipmentHtml(await c.getHtml());
        if (!items.length) throw new Error('本文から明細の表 (商品ID/出荷数の見出し) を読み取れません');
      }
      if (!items.length) throw new Error('明細が0行です');
    } catch (e) {
      if (e.transient) { result.errors.push({ gmailId: c.id, error: String(e.message || e), transient: true }); continue; }
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
