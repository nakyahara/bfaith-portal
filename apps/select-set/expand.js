/**
 * expand.js — 「選べる〇種セット」の商品OPを実SKUの明細行に展開する純粋ロジック
 *
 * 何を解いているか:
 *   モールで売れた「選べる〇種セット」は、組み合わせが無限で商品マスタに登録できないため
 *   架空の商品コード (例 selectae10-5) で受注が入り、NEでは引当待ちに落ちる。
 *   お客さんが選んだ実SKUは受注明細の「商品OP」に埋まっているので、そこから明細行を組み立てる。
 *
 * なぜ文字列から商品コードを推測しないか:
 *   同じ選択肢名でもセットが違えば別SKU (「プルメリア」= selectae10-5 なら ae-plumeria10、
 *   selectwa10-5 なら wae10ml-pl)。必ず「そのセットの選択肢定義」の中だけで解決する。
 *
 * モールごとに商品OPの書式が違う (2026-08-07 実データ調査):
 *   楽天   1本目:プルメリア_ae-plumeria10|2本目:...        (区切り = |)
 *   Yahoo! 1本目:ココナッツ_ae-coconut10 2本目:...          (区切り = 半角スペース)
 *   Qoo10  1本目:【ホワイトムスク】ae-whitemusk10,2本目:... (区切り = , / 選択肢が【】付き)
 *   auPAY  1本目=リゾートアンドスパ_ae-resort10&2本目=...   (区切り = & / ラベル区切りが =)
 *   → 区切り文字で割ると壊れる (ganesh の「スティック型_BLACK CHERRY」のように値に空白が入る)。
 *     RMS から取れる displayName (「1本目」「シークレットプレゼントについては」等) を
 *     アンカーにして切るので、区切り文字が何であっても影響を受けない。
 */

/** 「N本目」「N種類目」など、お客さんが商品を選ぶ枠かどうか */
export function isPickLabel(displayName) {
  return /^\s*\d+\s*(?:本|種類|個|点|品|色)?\s*目\s*$/.test(String(displayName || ''));
}

/**
 * 表記ゆれを吸収する正規化。
 * モールによって同じ商品が違う書き方で来る (auPAY は ＆ が使えず「アンド」表記になる等):
 *   リゾート＆スパ / リゾートアンドスパ / 【リゾート＆スパ】 → 同じものとして扱う
 */
export function normalizeValue(s) {
  return String(s || '')
    .normalize('NFKC')
    .toLowerCase()
    .replace(/[＆&]/g, '')
    .replace(/アンド/g, '')
    .replace(/[【】\[\]（）()「」『』〈〉・､,、。\s_\-–—~〜]/g, '')
    .trim();
}

/**
 * 選択肢の表示文字列から商品コードの候補を取り出す。
 *   「ココナッツ_ae-coconut10」    → ae-coconut10   (最後の _ の後ろ)
 *   「【カモミール】wae10-ca」      → wae10-ca       (】の後ろ)
 *   「16.スイートオレンジsweetorange10」→ sweetorange10 (末尾の英数字塊)
 * どれが正しいかは呼び出し側が「実在する商品コードか」で判定する。
 */
export function extractCodeCandidates(displayValue) {
  const v = String(displayValue || '').trim();
  const out = [];
  const afterBracket = v.match(/】\s*(.+)$/);
  if (afterBracket) out.push(afterBracket[1].trim());
  const us = v.lastIndexOf('_');
  if (us >= 0) out.push(v.slice(us + 1).trim());
  const tail = v.match(/([A-Za-z][A-Za-z0-9-]{2,})$/);
  if (tail) out.push(tail[1]);
  return [...new Set(out.filter(Boolean))];
}

/**
 * セットの選択肢定義を組み立てる。
 *
 * @param {object} p
 * @param {string} p.setCode          セット商品コード (例 selectae10-5)
 * @param {Array}  p.rmsOptions       RMS customizationOptions ([{displayName, selections:[{displayValue}]}])
 * @param {Array}  p.manualRows       手動補完 ([{ option, code }]) — RMSで解けないセット/別名用
 * @param {Set}    p.knownProductCodes NE商品マスタの商品コード集合 (小文字)
 * @returns {{setCode, labels, options, skus, unresolved}}
 *   options … 正規化した選択肢文字列 → 商品コード
 *   skus    … このセットで許可された商品コード (長い順。値への埋め込み一致に使う)
 */
export function buildCatalog({ setCode, rmsOptions = [], manualRows = [], knownProductCodes = new Set() }) {
  const options = new Map();
  const source = new Map();          // 正規化キー → 'rms' | 'manual' (どちらが登録したか)
  const ambiguousKeys = new Set();   // 別SKUがぶつかったキー。二度と登録しない
  const skus = new Set();
  const unresolved = [];
  const labels = [];
  const conflicts = [];

  /**
   * 🚨 曖昧なキーは「登録しない」だけでは足りない。
   *   先に入っていた方を消さないと、最初に出たSKUへ黙って展開されてしまう
   *   (Codexレビュー #1 / 2026-08-08)。消したうえで、以後の再登録も禁止する。
   * 優先順位: 手動 > RMS。同じ種類同士でぶつかったら曖昧として捨てる。
   */
  const put = (rawOption, code, { from = 'rms' } = {}) => {
    const key = normalizeValue(rawOption);
    if (!key || !code) return;
    if (ambiguousKeys.has(key)) return;
    const prev = options.get(key);
    if (prev && prev !== code) {
      const prevFrom = source.get(key);
      if (prevFrom === 'rms' && from === 'manual') {
        options.set(key, code);      // 手動でRMSを上書きするのは意図した動き
        source.set(key, 'manual');
        skus.add(code);
        return;
      }
      if (prevFrom === 'manual' && from === 'rms') return; // 手動を守る
      conflicts.push({ option: rawOption, a: prev, b: code, from });
      options.delete(key);
      ambiguousKeys.add(key);
      return;
    }
    options.set(key, code);
    source.set(key, from);
    skus.add(code);
  };

  /**
   * 1つの選択肢を、モールが送ってきうる全ての書き方で引けるように登録する。
   * 「ココナッツ_ae-coconut10」に対し、モールによっては「ココナッツ」だけ、
   * 「【ココナッツ】ae-coconut10」に対しては「ココナッツ」だけを送ってくることがあるため、
   * 表示名だけのキーも必ず入れる (これが無いと辞書引きが外れる。
   *  2026-08-07: 手動行に入れ忘れて ae5ml-select5 の解決率が 27% に落ちた)。
   */
  const register = (rawOption, code, opts) => {
    put(rawOption, code, opts);
    const m = String(rawOption).match(/^(.*?)_([A-Za-z0-9-]+)$/);
    if (m && m[2].toLowerCase() === String(code).toLowerCase()) put(m[1], code, opts);
    const b = String(rawOption).match(/^【(.+?)】/);
    if (b) put(b[1], code, opts);
  };

  for (const opt of rmsOptions) {
    const name = String(opt?.displayName || '');
    labels.push({ name, isPick: isPickLabel(name) });
    for (const sel of opt?.selections || []) {
      const dv = String(sel?.displayValue || '').trim();
      if (!dv) continue;
      const code = extractCodeCandidates(dv).find((c) => knownProductCodes.has(c.toLowerCase()));
      if (code) register(dv, code, { from: 'rms' });
      else unresolved.push(dv);
    }
  }

  // 手動補完は RMS より優先度が高い (RMSの文字数制限で切れたコードの別名などを直せるように)
  for (const row of manualRows) {
    if (!row?.option || !row?.code) continue;
    register(row.option, row.code, { from: 'manual' });
  }

  const skuList = [...skus];
  return {
    setCode,
    labels,
    /** 期待される選択枠の数。ここと実際に解決できた行数が一致しないと展開しない */
    expectedPicks: labels.filter((l) => l.isPick).length,
    /** RMSが返したラベル名の集合。未知のラベルが出てきたら止めるために使う */
    labelNames: new Set(labels.map((l) => normalizeValue(l.name))),
    options,
    ambiguousKeys,
    skus: skuList,
    skuSet: new Set(skuList.map((c) => c.toLowerCase())),
    knownProductCodes,
    unresolved,
    conflicts,
  };
}

/**
 * 商品OPを「ラベル → 値」に割る。
 * RMS の displayName が分かっていればそれをアンカーに切る (確実)。
 * 分からない場合だけ「N本目」パターンで拾う (手動セット用のフォールバック)。
 */
export function splitOp(op, labels = []) {
  const text = String(op || '');
  if (!text.trim()) return [];
  const SEP = '[:：=＝]';
  const anchors = [];

  for (const l of labels) {
    const name = String(l?.name || '').trim();
    if (!name) continue;
    const re = new RegExp(escapeRe(name) + '\\s*' + SEP + '\\s*', 'g');
    let m;
    while ((m = re.exec(text)) !== null) {
      anchors.push({ label: name, isPick: !!l.isPick, start: m.index, valueStart: m.index + m[0].length });
    }
  }

  // RMSの displayName に無いラベルを拾う。
  // 例: RMSは「シークレットプレゼントについては」だが、過去の受注には
  //     「おまけについては」で入っているものがある。これを拾えないと、
  //     直前の選択枠の値に「真正ラベンダー|おまけについては:当店が…」と丸ごと吸われて解決に失敗する。
  //
  // 🚨 以前は「区切り文字と : を含まない40文字以内の塊」を全部ラベル候補にしていたが、
  //   値の中に `xxx=yyy` が入ると xxx をラベルと誤認して値を途中で切る危険があった
  //   (Codexレビュー #4 / 2026-08-08)。そこで拾う対象を次の2つだけに絞る:
  //     ・「〜については」「〜について」で終わる補足ラベル (おまけ・同意確認の類)
  //     ・「N本目」「N種類目」などの選択枠ラベル
  //   どちらも業務上の定型で、商品名の途中に現れることはまずない。
  const AUX = new RegExp('(?:^|[|,&\\s])([^|,&:：=＝\\s]{1,30}については?)\\s*' + SEP + '\\s*', 'g');
  const PICK = new RegExp('(?:^|[|,&\\s])(\\d+\\s*(?:本|種類|個|点|品|色)?\\s*目)\\s*' + SEP + '\\s*', 'g');
  for (const re of [AUX, PICK]) {
    let g;
    while ((g = re.exec(text)) !== null) {
      const lead = g[0].length - g[0].replace(/^[|,&\s]+/, '').length;
      const start = g.index + lead;
      const valueStart = g.index + g[0].length;
      const covered = anchors.some((a) => (start >= a.start && start < a.valueStart) || a.valueStart === valueStart);
      if (covered) continue;
      anchors.push({ label: g[1], isPick: isPickLabel(g[1]), start, valueStart, viaGuess: true });
    }
  }

  anchors.sort((a, b) => a.start - b.start);
  return anchors.map((a, i) => {
    const end = i + 1 < anchors.length ? anchors[i + 1].start : text.length;
    return {
      label: a.label,
      isPick: a.isPick,
      viaGuess: !!a.viaGuess,
      value: text.slice(a.valueStart, end).replace(/[|,&\s、]+$/g, '').trim(),
    };
  });
}

function escapeRe(s) {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/**
 * 1つの選択肢の値 → 商品コード。次の順で解決する。
 *   1. label    … 選択肢定義との完全一致 (正規化して突き合わせ)。一番確実なのでこれを先に見る
 *   2. embedded … 「_ の後ろ」「】の後ろ」「末尾の英数字塊」から取り出した候補が、
 *                 そのセットで許可されたSKUと完全一致し、かつ候補が1つに定まるとき
 *   3. master   … 選択肢定義には無いが、取り出した候補がNE商品マスタに実在するとき
 *
 * 3 が要る理由 (2026-08-07 実測): RMSは「楽天の・今の」選択肢しか持たないため、
 * 楽天で廃止した香りや他モール限定の選択肢が解決できない
 * (例: selecteo10ml3 の「7.ロ-ズマリ-シネオ-ル_cineole10」は cineole10 が在庫934で実在するのに
 *  楽天の現行ページには無い)。ただし 3 は選択肢定義の裏付けが無い推定なので、
 * 呼び出し側で「マスタ照合で拾った」と明示して人に見せること。
 *
 * 🚨 以前は「許可SKUが値に含まれていれば採用」(部分文字列一致) を最優先にしていたが、
 *   `abc10` が `abc100` や `xabc10-old` にも一致してしまい、別SKUへ誤展開する危険があった
 *   (Codexレビュー #2 / 2026-08-08)。境界のない包含判定は廃止し、
 *   構造化された位置から取り出した候補の完全一致だけを使う。
 *   候補が複数のSKUに割れたときは、選ばずに null を返して止める。
 */
export function resolveValue(catalog, value) {
  const v = String(value || '');
  if (!v.trim()) return null;

  const key = normalizeValue(v);
  if (catalog.ambiguousKeys?.has(key)) return null; // 曖昧と分かっているキーは触らない
  const hit = catalog.options.get(key);
  if (hit) return { code: hit, via: 'label' };

  const candidates = [...new Set(extractCodeCandidates(v).map((c) => c.toLowerCase()))];
  const pick = (pool) => {
    const found = [...new Set(candidates.filter((c) => pool.has(c)))];
    return found.length === 1 ? found[0] : null;
  };

  if (catalog.skuSet?.size) {
    const c = pick(catalog.skuSet);
    if (c) {
      // 大文字小文字を元の登録どおりに戻す
      const canonical = catalog.skus.find((s) => s.toLowerCase() === c) || c;
      return { code: canonical, via: 'embedded' };
    }
  }
  if (catalog.knownProductCodes?.size) {
    const c = pick(catalog.knownProductCodes);
    if (c) return { code: c, via: 'master' };
  }
  return null;
}

/**
 * おまけを決める。優先順位の上から見て「利用可能在庫がある最初の1つ」。
 * @param {Array<string>} priority   商品コードの配列 (順位順)
 * @param {(code:string)=>({available:number,name?:string}|null)} stockOf
 */
export function pickOmake(priority = [], stockOf = () => null) {
  const tried = [];
  for (const code of priority) {
    const s = stockOf(code) || {};
    const available = Number.isFinite(s.available) ? s.available : 0;
    tried.push({ code, name: s.name || '', available, chosen: false });
    if (available > 0) {
      tried[tried.length - 1].chosen = true;
      return { code, candidates: tried };
    }
  }
  return { code: null, candidates: tried };
}

/**
 * 展開の本体。
 * @returns {{ok, lines, omake, warnings, parts}}
 *   ok=false のときは拡張側で自動投入させない (fail-closed)。
 */
export function expandOp({ catalog, op, quantity = 1, omakePriority = [], stockOf = () => null }) {
  const warnings = [];
  const notices = [];
  const bail = (msg) => ({ ok: false, lines: [], omake: null, parts: [], warnings: [msg], notices });

  // 🚨 数量は補正せず、おかしければ止める。
  //   以前は 0・負数・非数値をすべて 1 に丸めていたため、DOMの読み取り失敗が
  //   「受注数1」として通ってしまった (Codexレビュー #10 / 2026-08-08)。
  const qty = Number(quantity);
  if (!Number.isInteger(qty) || qty < 1 || qty > 999) {
    return bail(`受注数「${quantity}」が不正です (1〜999の整数のみ)`);
  }

  const parts = splitOp(op, catalog?.labels || []);
  if (!parts.length) return bail('商品OPを選択肢に分割できませんでした');

  // 🚨 選択枠の数が分からない状態では展開しない。
  //   以前は「見つかった枠が全部解けたらOK」だったので、5本セットで4枠しか
  //   認識できなくても ok=true になっていた (Codexレビュー #3 / 2026-08-08)。
  const expected = catalog?.expectedPicks || 0;
  if (!expected) {
    return bail('このセットの選択枠の数を確認できません (楽天の選択肢定義が取得できていない可能性があります)');
  }

  const seen = new Set();
  for (const p of parts) {
    if (!p.isPick) continue;
    const k = normalizeValue(p.label);
    if (seen.has(k)) return bail(`「${p.label}」が商品OPに2回以上あります`);
    seen.add(k);
  }
  const foundPicks = parts.filter((p) => p.isPick).length;
  if (foundPicks !== expected) {
    return bail(`選択枠が${expected}個のはずですが、商品OPからは${foundPicks}個しか読み取れませんでした`);
  }

  // RMSにも「〜については」形にも当てはまらないラベルが出てきたら、読み違えている可能性が高い
  const unknownLabels = parts
    .filter((p) => !p.isPick && p.viaGuess && !catalog.labelNames?.has(normalizeValue(p.label)))
    .map((p) => p.label);
  if (unknownLabels.length) {
    notices.push(`見慣れない項目がありました: ${[...new Set(unknownLabels)].join(' / ')}`);
  }

  const lines = [];
  let omake = null;
  let failed = false;

  for (const p of parts) {
    const hit = resolveValue(catalog, p.value);
    if (p.isPick) {
      // 選ぶ枠は必ず解決できなければならない。1つでも駄目なら止める
      if (!hit) {
        failed = true;
        warnings.push(`「${p.label}」の選択肢「${p.value}」が商品コードに変換できません`);
        continue;
      }
      lines.push({ label: p.label, value: p.value, code: hit.code, quantity: qty, via: hit.via });
      if (hit.via === 'master') {
        notices.push(`「${p.value}」は選択肢定義に無く、商品マスタ照合で ${hit.code} と判断しました`);
      }
    } else if (hit) {
      // 「シークレットプレゼントについては」等。商品に解決できた = おまけ枠とみなし、
      // RMS上のSKUではなく優先順位リストから在庫のあるものを選ぶ (中原さん指示 2026-08-07)
      if (omake) {
        warnings.push(`おまけ枠が複数あります (「${p.label}」)`);
        continue;
      }
      const picked = pickOmake(omakePriority.length ? omakePriority : [hit.code], stockOf);
      omake = { label: p.label, value: p.value, rmsCode: hit.code, ...picked, quantity: qty };
      if (!picked.code) {
        warnings.push('おまけの候補が全て在庫切れです。おまけ行は追加しません');
      }
    }
    // 解決できない非選択枠 (「文字数の関係で〜:確認しました。」等の同意確認) は無視する
  }

  // 最終確認: 期待した枠の数だけ明細行ができているか
  if (!failed && lines.length !== expected) {
    failed = true;
    warnings.push(`明細行が${expected}行になるはずですが${lines.length}行しか作れませんでした`);
  }

  return { ok: !failed, lines, omake, parts, warnings, notices, expectedPicks: expected };
}

/** NEの「明細入出力補助」に貼る形 (商品コードと受注数を改行区切り) に整える */
export function toPasteBlocks(result) {
  const rows = [...result.lines];
  if (result.omake?.code) rows.push({ code: result.omake.code, quantity: result.omake.quantity });
  return {
    codes: rows.map((r) => r.code).join('\n'),
    quantities: rows.map((r) => String(r.quantity)).join('\n'),
    rowCount: rows.length,
  };
}
