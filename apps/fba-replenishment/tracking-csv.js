/**
 * 福山通運 iSTAR-2 の「出荷実績CSV」を読み、FBA納品の輸送箱へ割り当てる対応表を作る。
 *
 * 入力 = iS-2 の [出荷実績印刷・CSV出力] が吐く 34列・Shift-JIS(CP932) の CSV。
 * 出力 = 納品(shipment)ごとの「boxId ← 送り状番号」の並び。
 *
 * ⭐設計の要 = 「お客様管理番号」(上り26列のU列 / 下り34列のAF列・半角英数16文字)。
 *   送り状を発行するときにここへ Amazon の納品番号 (FBA15GG2MM9B) を入れておけば、
 *   同じFC宛の納品が同時に複数あっても取り違えない。
 *   まだ入っていない過渡期のために FCコードでの割り当ても持つが、
 *   **一意に決まらなければ必ず中断する** (黙って推測しない)。
 *
 * 2026-08-07 実測で確定した事実:
 *   - 追跡番号は Amazon 側では **ハイフン無し** で保持される (663-7913-4975 → 66379134975)
 *   - **CSVの並び順 = 箱の連番順** (人が画面入力した実データと完全一致)
 *   - 個数・重量の列だけ引用符が無い (混在CSVなので素朴な split(',') では壊れる)
 */
import iconv from 'iconv-lite';

// 付録1-2-1「出荷実績CSVフォーマット (標準形式:記事3行)」の列位置 (0始まり)
export const COL = {
  登録日: 0,
  出荷日: 1,
  送り状番号: 2,
  荷受人コード: 3,
  荷受人名前1: 9,
  個数: 21,
  お客様管理番号: 31,
};
const EXPECTED_COLUMNS = 34;

/** 業務エラー (利用者にそのまま見せてよい文言) */
export function tErr(message, detail) {
  const e = new Error(message);
  e.code = 'TRACKING_CSV';
  if (detail) e.detail = detail;
  return e;
}

/**
 * 引用符ありとなしが混在する1行をパースする。
 * iS-2 の出力は大半が "..." だが 個数・重量 は裸の数値で来るため、素朴な分割は使えない。
 */
function parseCsvLine(line) {
  const out = [];
  let cur = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (inQuotes) {
      if (c === '"') {
        if (line[i + 1] === '"') { cur += '"'; i++; } // "" = エスケープされた "
        else inQuotes = false;
      } else cur += c;
    } else if (c === '"') {
      inQuotes = true;
    } else if (c === ',') {
      out.push(cur); cur = '';
    } else cur += c;
  }
  out.push(cur);
  return out;
}

/** 全角英数 → 半角。Excel が付ける先頭のアポストロフィと前後の空白を落とす。 */
export function normCell(v) {
  return String(v ?? '')
    .replace(/[Ａ-Ｚａ-ｚ０-９]/g, (c) => String.fromCharCode(c.charCodeAt(0) - 0xfee0))
    .replace(/^['`\s]+|\s+$/g, '')
    .trim();
}

/**
 * 送り状番号 → Amazon へ送る形 (数字のみ)。
 * 福山の書式は「数字とハイフン」だけなので、それ以外が混ざっていたら弾く
 * (英数を無条件に削ると別キャリアの番号を黙って壊すため、削らずに検出する)。
 */
export function normTrackingId(raw) {
  const s = normCell(raw).replace(/[‐-‒–—―－]/g, '-'); // Unicode のハイフン類を ASCII に寄せる
  if (!/^[0-9-]+$/.test(s)) return { ok: false, value: null, reason: `数字とハイフン以外を含む: ${s}` };
  const digits = s.replace(/-/g, '');
  if (!/^[0-9]{8,20}$/.test(digits)) return { ok: false, value: null, reason: `桁数が想定外 (${digits.length}桁): ${s}` };
  return { ok: true, value: digits, reason: null };
}

/** お客様管理番号 = 納品番号。半角英数16文字まで (マニュアル付録1)。 */
export function normKanriNo(raw) {
  const s = normCell(raw).toUpperCase();
  if (!s) return { ok: true, value: '', reason: null }; // 空欄は「未設定」であってエラーではない
  if (!/^[A-Z0-9]{1,16}$/.test(s)) return { ok: false, value: null, reason: `半角英数16文字以内ではない: ${s}` };
  return { ok: true, value: s, reason: null };
}

/**
 * CSV(Buffer) を行の配列にする。
 * @param {Buffer} buf CP932 の CSV
 * @returns {{rows: Array, problems: string[]}}
 */
export function parseTrackingCsv(buf) {
  if (!buf || !buf.length) throw tErr('CSVが空です');
  const text = iconv.decode(buf, 'CP932');
  const lines = text.split(/\r?\n/).filter((l) => l.trim() !== '');
  if (lines.length < 2) throw tErr('CSVにデータ行がありません (ヘッダーのみ)');

  const header = parseCsvLine(lines[0]);
  if (header.length !== EXPECTED_COLUMNS) {
    throw tErr(
      `CSVの列数が想定と違います (期待 ${EXPECTED_COLUMNS} / 実際 ${header.length})。` +
        'iS-2 の「出荷実績印刷・CSV出力」以外のCSVではありませんか',
      { header: header.slice(0, 5) },
    );
  }
  if (normCell(header[COL.送り状番号]) !== '送り状番号') {
    throw tErr(`3列目が「送り状番号」ではありません: ${normCell(header[COL.送り状番号])}`);
  }

  const rows = [];
  const problems = [];
  for (let i = 1; i < lines.length; i++) {
    const c = parseCsvLine(lines[i]);
    const lineNo = i + 1;
    if (c.length !== EXPECTED_COLUMNS) {
      problems.push(`${lineNo}行目: 列数が ${c.length} (期待 ${EXPECTED_COLUMNS})`);
      continue;
    }
    const tracking = normTrackingId(c[COL.送り状番号]);
    const kanri = normKanriNo(c[COL.お客様管理番号]);
    const qtyRaw = normCell(c[COL.個数]);
    const qty = /^[0-9]+$/.test(qtyRaw) ? Number(qtyRaw) : NaN;

    if (!tracking.ok) problems.push(`${lineNo}行目: 送り状番号が不正 (${tracking.reason})`);
    if (!kanri.ok) problems.push(`${lineNo}行目: お客様管理番号が不正 (${kanri.reason})`);
    if (!Number.isInteger(qty) || qty < 1) problems.push(`${lineNo}行目: 個数が不正 (${qtyRaw})`);

    rows.push({
      lineNo,
      出荷日: normCell(c[COL.出荷日]),
      送り状番号: normCell(c[COL.送り状番号]),
      trackingId: tracking.value,
      fcCode: normCell(c[COL.荷受人コード]).toUpperCase(),
      荷受人名前: normCell(c[COL.荷受人名前1]),
      個数: Number.isInteger(qty) ? qty : null,
      納品番号: kanri.value ?? '',
    });
  }
  return { rows, problems };
}

/**
 * 出荷日が全行そろって期待日と一致するかを見る。
 * ⭐固定ファイル名運用では「前日のCSVが残っていて、それを当日分として処理する」事故が起きうる。
 *   ファイル名では防げないので中身で防ぐ。
 * @param {Array} rows
 * @param {string} expectedYmd 'YYYYMMDD' (JST)
 */
export function checkShipDate(rows, expectedYmd) {
  const seen = new Map();
  for (const r of rows) seen.set(r.出荷日, (seen.get(r.出荷日) ?? 0) + 1);
  const dates = [...seen.keys()];
  if (dates.length === 1 && dates[0] === expectedYmd) return { ok: true, dates, problem: null };
  const detail = [...seen.entries()].map(([d, n]) => `${d}:${n}件`).join(' / ');
  return {
    ok: false,
    dates,
    problem: `CSVの出荷日が期待 (${expectedYmd}) と一致しません → ${detail}。` +
      '古いCSVが残っていないか、出力時の日付指定を確認してください',
  };
}

/**
 * 行を納品(shipment)へ割り当てる。
 *
 * @param {Array} rows                parseTrackingCsv の rows
 * @param {Array} shipments           [{ shipmentConfirmationId, fcCode, boxIds: string[], hasTracking: boolean }]
 * @param {{allowFcFallback?: boolean}} [opts]
 * @returns {{assignments: Array, excluded: Array, problems: string[], skipped: Array}}
 */
export function buildAssignments(rows, shipments, opts = {}) {
  // ⭐既定OFF。拡張が お客様管理番号 に納品番号を入れるようになったので、
  //   FCコードでの推測は「拡張導入前の移行期に手動実行するとき」だけに限る。
  //   一意に見えても『その送り状がその納品のもの』である保証はない (Codex指摘)。
  const allowFcFallback = opts.allowFcFallback === true;
  const problems = [];
  const excluded = [];
  const skipped = [];

  // 個数分だけ展開する (1送り状=複数個口があり得るため。FBA分は実績上すべて個数1)
  const units = [];
  for (const r of rows) {
    if (!r.trackingId || !r.個数) continue; // 不正行は parseTrackingCsv 側で problems 済み
    for (let i = 0; i < r.個数; i++) units.push(r);
  }

  // 同じ送り状番号が別の納品番号/FCに現れる = 取り違えの疑い
  const byTracking = new Map();
  for (const u of units) {
    const key = u.trackingId;
    const tag = u.納品番号 || `FC:${u.fcCode}`;
    if (!byTracking.has(key)) byTracking.set(key, new Set());
    byTracking.get(key).add(tag);
  }
  for (const [t, tags] of byTracking) {
    if (tags.size > 1) problems.push(`送り状番号 ${t} が複数の宛先に現れます (${[...tags].join(' / ')})`);
  }

  const targets = shipments.filter((s) => !s.hasTracking);
  for (const s of shipments) {
    if (s.hasTracking) skipped.push({ shipmentConfirmationId: s.shipmentConfirmationId, reason: '登録済み' });
  }

  const assignments = [];
  const usedUnits = new Set();

  // ① 納品番号 (お客様管理番号) での割り当て — 本命。同一FC宛が複数でも一意に決まる
  for (const s of targets) {
    const mine = units.filter((u, i) => u.納品番号 === s.shipmentConfirmationId && !usedUnits.has(i));
    if (!mine.length) continue;
    const idx = [];
    units.forEach((u, i) => { if (u.納品番号 === s.shipmentConfirmationId && !usedUnits.has(i)) idx.push(i); });
    idx.forEach((i) => usedUnits.add(i));
    assignments.push(makeAssignment(s, idx.map((i) => units[i]), '納品番号', problems));
  }

  // ② FCコードでの割り当て — 納品番号が未設定の過渡期のみ。一意でなければ中断
  const rest = units.map((u, i) => ({ u, i })).filter(({ u, i }) => !usedUnits.has(i) && !u.納品番号);
  if (rest.length) {
    const byFc = new Map();
    for (const { u, i } of rest) {
      if (!byFc.has(u.fcCode)) byFc.set(u.fcCode, []);
      byFc.get(u.fcCode).push(i);
    }
    for (const [fc, idx] of byFc) {
      const cands = targets.filter((s) => s.fcCode === fc && !assignments.some((a) => a.shipmentConfirmationId === s.shipmentConfirmationId));
      if (!cands.length) {
        excluded.push({ fcCode: fc, 件数: idx.length, reason: '対象の納品が見つからない (FBA以外の便の可能性)' });
        continue;
      }
      if (!allowFcFallback) {
        problems.push(`${fc}: お客様管理番号が空です。送り状発行時に納品番号を入れてください`);
        continue;
      }
      if (cands.length > 1) {
        problems.push(
          `${fc} 宛の未登録の納品が ${cands.length} 件あり、どれに割り当てるか決まりません ` +
            `(${cands.map((c) => c.shipmentConfirmationId).join(' / ')})。` +
            '送り状発行時に「お客様管理番号」へ納品番号を入れてください',
        );
        continue;
      }
      idx.forEach((i) => usedUnits.add(i));
      assignments.push(makeAssignment(cands[0], idx.map((i) => units[i]), 'FCコード', problems));
    }
  }

  // どの納品にも割り当てられなかった行 (納品番号が入っているのに該当shipmentが無い等)
  // 🚨お客様管理番号に値があるのに該当する納品が無い = 転記違い・プラン取得漏れ・古い行の混入。
  //   「FBA以外の便」と同じ扱い (黙って除外) にすると、取り違えたまま他の納品だけ登録してしまう。
  const orphans = new Map();
  units.forEach((u, i) => {
    if (usedUnits.has(i) || !u.納品番号) return; // 管理番号が空の行は ② で excluded 済み
    orphans.set(u.納品番号, (orphans.get(u.納品番号) ?? 0) + 1);
  });
  for (const [no, n] of orphans) {
    problems.push(
      `CSVの「お客様管理番号」${no} (${n}件) に対応する納品が見つかりません。` +
        '納品番号の打ち間違い、別のプランの伝票が混ざっている、などが考えられます',
    );
  }

  // 🚨投入待ちの納品なのにCSVに1行も無い = 納品ごと数え漏らした可能性。
  //   箱数の照合は「行があった納品」しか通らないので、ここで拾わないと素通りする。
  for (const s of targets) {
    if (assignments.some((a) => a.shipmentConfirmationId === s.shipmentConfirmationId)) continue;
    problems.push(
      `${s.shipmentConfirmationId} (${s.fcCode}, ${(s.boxIds ?? []).length}箱) の送り状がCSVにありません。` +
        'この納品ぶんの伝票を出し忘れていないか確認してください',
    );
  }

  return { assignments, excluded, problems, skipped };
}

function makeAssignment(shipment, myUnits, matchedBy, problems) {
  const boxIds = shipment.boxIds ?? [];
  if (myUnits.length !== boxIds.length) {
    problems.push(
      `${shipment.shipmentConfirmationId} (${shipment.fcCode}): ` +
        `伝票の個口合計 ${myUnits.length} と輸送箱 ${boxIds.length} が一致しません。` +
        '箱数の数え間違いか、送り状の出し漏れ・出し過ぎの可能性があります',
    );
  }
  return {
    shipmentConfirmationId: shipment.shipmentConfirmationId,
    shipmentId: shipment.shipmentId,
    fcCode: shipment.fcCode,
    matchedBy,
    // ⭐並びは「CSVの上から順に箱1,2,3…」。人が画面入力した実データと一致することを確認済み
    items: boxIds.map((boxId, i) => ({
      boxId,
      trackingId: myUnits[i]?.trackingId ?? null,
      送り状番号: myUnits[i]?.送り状番号 ?? null,
    })),
    countCsv: myUnits.length,
    countBoxes: boxIds.length,
  };
}
