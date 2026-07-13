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

// 仕入先とメールの対応 (増えたら設定へ昇格を検討。domain は From アドレスのドメイン一致)。
// extraAuthDomains: SPF/DKIM単独fallbackで追加許可する完全一致ドメイン (配送用サブドメイン等。
// 解析エラーの [判定: ...] に出た実ドメインを確認して足す)
export const SHIPMENT_MAIL_RULES = [
  { supplierCode: '1', domain: 'am-craft.jp', kind: 'xlsx', extraAuthDomains: [] },
  { supplierCode: '2', domain: 'be-free.biz', kind: 'body', subjectFilter: '出荷明細', extraAuthDomains: [] },
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

export function authPassed(authResultsHeaders, fromDomain, extraExactDomains = []) {
  const headers = Array.isArray(authResultsHeaders) ? authResultsHeaders : [authResultsHeaders];
  const d = String(fromDomain || '').toLowerCase();
  if (!d) return false;
  const align = v => !!v && (v === d || v.endsWith('.' + d) || d.endsWith('.' + v));
  const exactSet = new Set([d, ...extraExactDomains.map(x => String(x || '').toLowerCase()).filter(Boolean)]);
  const exact = v => !!v && exactSet.has(v);
  // 信頼対象 (mx.google.com) の全ヘッダをまず集約してから一度だけ判定する
  // (別ヘッダの dmarc=fail をSPF単独passで迂回させない、Codex 入荷予定R2 Medium)
  let dmarcFail = false;
  let dmarcPassAligned = false;
  const spfDoms = [], dkimDoms = [];
  for (const raw of headers) {
    const clauses = splitAuthClauses(String(raw || '').toLowerCase());
    // 先頭節 = authserv-id。Gmail自身 (mx.google.com) が付与したヘッダだけを信頼する
    // (外部から持ち込まれた同名ヘッダを判定に使わない、Codex mail-R4 High)
    const authserv = (clauses[0] || '').split(/\s+/)[0];
    if (authserv !== 'mx.google.com') continue;
    for (const c of clauses.slice(1)) {
      // method=result は「節の先頭」のみ有効 (RFC 8601 resinfo)
      const m = c.match(/^(dmarc|spf|dkim)=([a-z0-9]+)/);
      if (!m) continue;
      if (m[1] === 'dmarc' && m[2] === 'fail') { dmarcFail = true; continue; }
      if (m[2] !== 'pass') continue;
      if (m[1] === 'dmarc') {
        const hf = c.match(/header\.from=([^\s]+)/);
        if (hf && align(hf[1])) dmarcPassAligned = true; // header.from の明示整合のみ許可 (無記載passは採用しない)
      } else if (m[1] === 'spf') {
        const mf = c.match(/smtp\.mailfrom=(?:[^@\s]*@)?([^\s]+)/);
        if (mf) spfDoms.push(mf[1]);
      } else {
        const hd = c.match(/header\.d=([^\s]+)/) || c.match(/header\.i=@?(?:[^@\s]*@)?([^\s]+)/);
        if (hd) dkimDoms.push(hd[1]);
      }
    }
  }
  if (dmarcPassAligned) return true;
  // SPF単独 / DKIM単独でも「完全一致ドメインのpass」なら許可する。
  // 仕入先 (中小事業者) はDMARC未導入・DKIM未設定が普通で、両方要求だと実メールが全滅した (2026-07-13 本番72件全エラー)。
  // 単独fallbackの制限 (Codex 入荷予定R1/R2 Medium):
  //  - どこかのヘッダに明示的な dmarc=fail があればfallbackしない (DMARCポリシー違反をSPF/DKIM単独で上書きしない。none/無記載のみ許容)
  //  - 整合は完全一致のみ (サブドメイン委譲先のESP等による親ドメインFrom詐称を通さない)。
  //    配送用サブドメイン (bounce.〜等) が必要な仕入先は SHIPMENT_MAIL_RULES.extraAuthDomains に明示追加する
  if (dmarcFail) return false;
  if (spfDoms.some(exact)) return true;
  if (dkimDoms.some(exact)) return true;
  return false;
}

/** Authentication-Results の判定内訳を人間向けに要約 (解析エラーの原因調査用) */
export function summarizeAuth(authResultsHeaders) {
  const headers = Array.isArray(authResultsHeaders) ? authResultsHeaders : [authResultsHeaders];
  const out = [];
  for (const raw of headers) {
    const clauses = splitAuthClauses(String(raw || '').toLowerCase());
    const authserv = (clauses[0] || '').split(/\s+/)[0];
    if (authserv !== 'mx.google.com') continue;
    for (const c of clauses.slice(1)) {
      const m = c.match(/^(dmarc|spf|dkim)=([a-z0-9]+)/);
      if (!m) continue;
      const dom = c.match(/(?:header\.from|smtp\.mailfrom|header\.d|header\.i)=(?:[^@\s]*@)?([^\s]+)/);
      out.push(`${m[1]}=${m[2]}${dom ? ` (${dom[1]})` : ''}`);
    }
  }
  return out.length ? out.join(' / ') : 'Gmail付与のAuthentication-Resultsなし';
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

function fakeCandidates() {
  return fakeMails().map(m => {
    const dom = (mailboxOf(m.from) || 'x@invalid').split('@')[1];
    const xlsx = (m.attachments || []).filter(a => /\.xlsx$/i.test(a.filename));
    return {
      id: m.id, from: m.from, subject: m.subject,
      authResults: m.authResults !== undefined ? m.authResults
        : `mx.google.com; spf=pass smtp.mailfrom=${dom}; dkim=pass header.d=${dom}; dmarc=pass header.from=${dom}`,
      internalDateIso: m.internalDate || nowIso(),
      xlsxNames: xlsx.map(a => a.filename), // 添付メタデータ (中身のDLなしで判定できる)
      getHtml: async () => m.bodyHtml || '',
      getXlsxAt: async i => ({ filename: xlsx[i].filename, buf: Buffer.from(xlsx[i].dataBase64, 'base64') }),
    };
  });
}

const transientErr = e => { e.transient = true; return e; };

/** Gmailのメッセージ (format=full) → 解析候補。添付は「メタデータのみ」保持し、中身のDLは getFirstXlsx まで遅延 */
function candidateFromGmailMsg(msg) {
  const collected = walkParts(msg.payload, { html: '', attachments: [] });
  return {
    id: msg.id,
    from: headerOf(msg.payload, 'From'), subject: headerOf(msg.payload, 'Subject'),
    authResults: headersOf(msg.payload, 'Authentication-Results'), // 全ヘッダを渡し、authserv-id=mx.google.comのみ採用
    internalDateIso: msg.internalDate ? new Date(Number(msg.internalDate)).toISOString() : nowIso(),
    xlsxNames: collected.attachments.map(a => a.filename),
    getHtml: async () => collected.html,
    // 添付は必要になった1件ずつDLする — 認証を通ったメールについてのみ呼ばれる
    // (未認証メールの大量添付でAPI/メモリを浪費させない、Codex 入荷予定R1 High)
    getXlsxAt: async i => {
      const a = collected.attachments[i];
      let att;
      try { att = await gmailApiGet(`messages/${encodeURIComponent(msg.id)}/attachments/${encodeURIComponent(a.attachmentId)}`, 60000); }
      catch (e) { throw transientErr(e); } // 添付DL失敗=一時障害扱い (次回取得でやり直し)
      return { filename: a.filename, buf: b64urlToBuf(att.data) };
    },
  };
}

/**
 * ルールに従って1通を解析する (取得/再解析の共通コア。exportはテスト用)。
 * @throws e.notCandidate=true: 出荷明細ではない (AMCのxlsx添付なし=請求書等) — 一覧に登録しない
 * @throws e.transient=true: 一時的な取得障害 — 保存せず次回やり直し
 */
export async function parseByRule(c, rule) {
  const authError = () => new Error('送信元の認証を確認できません — なりすましの可能性があるため自動変換しません。' +
    `本物なら手動貼り付けを使ってください [判定: ${summarizeAuth(c.authResults)}]`);
  if (rule.kind === 'xlsx') {
    // 判定順: 添付メタデータなし=対象外 → 認証 → 認証済みだけ添付をDL (Codex 入荷予定R1 High)
    const names = c.xlsxNames || [];
    if (!names.length) { const e = new Error('xlsx添付がありません'); e.notCandidate = true; throw e; }
    if (!authPassed(c.authResults, rule.domain, rule.extraAuthDomains)) throw authError();
    // 添付が複数のメール (請求書.xlsx+出荷明細.xlsx 等) 対策: ファイル名に「出荷明細」を含む添付を優先し、
    // 解析できるまで順に試す (最大3添付。認証済みメールのみ)
    const order = names.map((n, i) => ({ n, i }))
      .sort((a, b) => (a.n.includes('出荷明細') ? 0 : 1) - (b.n.includes('出荷明細') ? 0 : 1) || a.i - b.i)
      .slice(0, 3);
    let lastErr = null;
    for (const { i } of order) {
      const file = await c.getXlsxAt(i);
      try {
        const items = await parseShipmentXlsx(file.buf);
        if (items.length) return { items, note: file.filename + (names.length > 1 ? ` (全${names.length}添付中これを解析)` : '') };
        lastErr = new Error(`明細が0行です (${file.filename})`);
      } catch (e) {
        if (e.transient) throw e;
        lastErr = new Error(`${String(e.message || e)} (${file.filename})`);
      }
    }
    throw lastErr || new Error('明細が0行です');
  }
  if (!authPassed(c.authResults, rule.domain, rule.extraAuthDomains)) throw authError();
  const items = parseShipmentHtml(await c.getHtml());
  if (!items.length) throw new Error('本文から明細の表 (商品ID/出荷数の見出し) を読み取れません');
  return { items, note: null };
}

/**
 * 出荷明細メールを取得して po_shipment_mails に登録する (既存gmail_idはスキップ=冪等)。
 * @returns {checked, added, errors: [{gmailId, error}]}
 */
export async function fetchShipmentMails(actor, { newerThanDays = 30 } = {}) {
  const db = getDB();
  const exists = db.prepare('SELECT 1 FROM po_shipment_mails WHERE gmail_id=?');
  // INSERT OR IGNORE: 同時実行で両方が未登録と判断してもUNIQUE違反で落ちない (後着は無視、Codex mail-R5 Low)
  const ins = db.prepare(`INSERT OR IGNORE INTO po_shipment_mails
    (gmail_id, supplier_code, from_addr, subject, received_at, parsed_json, parse_note, status, error, created_at, updated_at)
    VALUES (?,?,?,?,?,?,?,?,?,?,?)`);
  const result = { checked: 0, added: 0, errors: [] };

  const useFake = !!process.env.PO_SHIPMENT_FAKE_DATA;
  // 一時的な通信障害 (添付ダウンロード等) は行を確定保存せず次回の取得でやり直す。
  // 恒久的な内容エラー (形式不正等) だけ status='error' で保存する (Codex mail-R1 Medium)
  let candidates; // [{id, from, subject, authResults, internalDateIso, getHtml(), getXlsx()}]
  if (useFake) {
    candidates = fakeCandidates();
  } else {
    const domains = SHIPMENT_MAIL_RULES.map(r => `from:${r.domain}`).join(' OR ');
    const q = `(${domains}) newer_than:${newerThanDays}d`;
    // nextPageToken を最後まで辿る (1ページ50件で頭打ちにならないように、Codex mail-R1 Medium)。10ページ=500通で打ち切り
    const metas = [];
    let pageToken = null;
    let capped = false;
    for (let page = 0; page < 10; page++) {
      const list = await gmailApiGet('messages?maxResults=50&q=' + encodeURIComponent(q) + (pageToken ? '&pageToken=' + encodeURIComponent(pageToken) : ''));
      metas.push(...(list.messages || []));
      pageToken = list.nextPageToken || null;
      if (!pageToken) break;
      if (page === 9) capped = true;
    }
    // 上限 (500通) 到達は黙って打ち切らず警告として返す (取り残しの可視化、Codex mail-R5 Medium)
    if (capped) result.errors.push({ gmailId: null, error: `検索結果が${metas.length}通を超えたため打ち切りました。期間を分けて取得してください`, transient: true });
    candidates = [];
    for (const meta of metas) {
      if (exists.get(meta.id)) continue; // 取得済みは本文をダウンロードしない
      const msg = await gmailApiGet(`messages/${encodeURIComponent(meta.id)}?format=full`);
      candidates.push(candidateFromGmailMsg(msg));
    }
  }

  for (const c of candidates) {
    result.checked++;
    if (exists.get(c.id)) continue;
    const rule = ruleFor(c.from, c.subject);
    if (!rule) continue; // 対象外の差出人/件名 (請求書等) は登録しない
    let items = [], note = null, status = 'new', error = null;
    try {
      ({ items, note } = await parseByRule(c, rule));
    } catch (e) {
      if (e.transient) { result.errors.push({ gmailId: c.id, error: String(e.message || e), transient: true }); continue; }
      // 出荷明細ではないメール (AMCのxlsx添付なし=請求書等) は一覧に載せないが、
      // tombstone (not_candidate) として保存し、次回以降の再取得・再DLを防ぐ (Codex 入荷予定R1 Medium)
      status = e.notCandidate ? 'not_candidate' : 'error';
      error = String(e.message || e);
    }
    ins.run(c.id, rule.supplierCode, String(c.from || '').slice(0, 200), String(c.subject || '').slice(0, 200),
      c.internalDateIso, JSON.stringify(items), note, status, error, nowIso(), nowIso());
    if (status === 'not_candidate') { result.nonCandidates = (result.nonCandidates || 0) + 1; continue; }
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
  // new と error を表示 (done/ignored は直近10件を「↩ 戻す」付きで参考表示)
  const open = db.prepare("SELECT * FROM po_shipment_mails WHERE status IN ('new','error') ORDER BY received_at DESC").all();
  const recent = db.prepare("SELECT * FROM po_shipment_mails WHERE status IN ('done','ignored') ORDER BY updated_at DESC LIMIT 10").all();
  // 「出荷明細ではない」と判定して一覧に出していないメール (tombstone)。誤判定の確認・救出用に直近20件+総件数を返す
  const notCandidates = db.prepare("SELECT * FROM po_shipment_mails WHERE status='not_candidate' ORDER BY received_at DESC LIMIT 20").all();
  const notCandidateTotal = db.prepare("SELECT COUNT(*) AS n FROM po_shipment_mails WHERE status='not_candidate'").get().n;
  return { open, recent, notCandidates, notCandidateTotal };
}

/**
 * 1通を再解析する (Gmailから取り直して現行の解析ロジックにかけ直す)。
 * 解析コード修正後に、既存のerror行をボタン一つでやり直すため。
 * @returns {status:'new'|'error', itemCount?, error?} または {removed:true, reason} (出荷明細の対象外だった行は削除)
 */
export async function reparseShipmentMail(id, actor) {
  const db = getDB();
  const row = db.prepare('SELECT * FROM po_shipment_mails WHERE id=?').get(id);
  if (!row) throw new Error('メールが見つかりません');
  if (row.status === 'done') throw new Error('登録済みのメールは再解析できません (必要なら「↩ 戻す」で未処理に戻してから)');
  // new/error に加えて not_candidate も再解析可 (「出荷明細ではない」の誤判定をユーザーが救出できるように)
  if (!['new', 'error', 'not_candidate'].includes(row.status)) throw new Error(`未処理 (new/error/対象外) のメールだけ再解析できます (現在: ${row.status})`);
  let c;
  if (process.env.PO_SHIPMENT_FAKE_DATA) {
    c = fakeCandidates().find(x => x.id === row.gmail_id) || null;
  } else {
    let msg;
    try { msg = await gmailApiGet(`messages/${encodeURIComponent(row.gmail_id)}?format=full`); }
    catch (e) { throw new Error(`Gmailからメールを再取得できません: ${String(e.message || e)}`); }
    c = candidateFromGmailMsg(msg);
  }
  if (!c) throw new Error('Gmailからメールを再取得できません');
  // 状態遷移は「開始時に読んだ版のままの行」だけに適用するCAS (解析中に別タブで状態が変わった行を
  // 上書きしない。status IN だけでは ignored→new 等の変更をすり抜ける、Codex 入荷予定R1 High/JST-R1 Medium)
  const applyIfStillOpen = (fields, params, detail) => db.transaction(() => {
    const upd = db.prepare(`UPDATE po_shipment_mails SET ${fields}, updated_at=? WHERE id=? AND status=? AND updated_at=?`)
      .run(...params, nowIso(), id, row.status, row.updated_at);
    if (upd.changes === 0) throw new Error('再解析中にこのメールの状態が変わりました (登録済み/無視など)。一覧を更新して確認してください');
    audit(db, { actorType: 'user', actor, action: 'shipment_mail_reparse', resource: `shipment_mail:${id}`,
      detail: { gmailId: row.gmail_id, ...detail } });
  }).immediate();
  const toNotCandidate = reason => {
    // 物理削除しない: gmail_id を残して次回取得の再DLを防ぎ、監査で追える (Codex 入荷予定R1 Medium)
    applyIfStillOpen("status='not_candidate', error=?", [reason], { removed: true, reason });
    return { removed: true, reason };
  };
  const rule = ruleFor(c.from, c.subject);
  if (!rule) return toNotCandidate('出荷明細の対象外です (差出人/件名がルールに一致しません)');
  try {
    const { items, note } = await parseByRule(c, rule);
    applyIfStillOpen("status='new', parsed_json=?, parse_note=?, error=NULL", [JSON.stringify(items), note], { status: 'new', items: items.length });
    return { status: 'new', itemCount: items.length };
  } catch (e) {
    if (e.transient) throw new Error(`一時的な取得エラー: ${String(e.message || e)} — もう一度試してください`);
    if (e.notCandidate) return toNotCandidate(`${String(e.message || e)} — 出荷明細ではないため一覧から外しました`);
    const error = String(e.message || e);
    applyIfStillOpen("status='error', error=?", [error], { status: 'error', error });
    return { status: 'error', error };
  }
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
