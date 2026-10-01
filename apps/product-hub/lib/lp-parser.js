/**
 * LP 構成書パーサー — bfaith-flow (lp-tool) の本番パーサーの移植。
 *
 * 移植元: https://bfaith-flow.onrender.com/assets/parser.js
 * 取得:   2026-10-01
 * sha256: 1a2f22856ee134938a0f7ce1c79a9d58023bb23ce18778c369da3855413f2855
 *
 * 🚨 **下の「写し ここから 〜 ここまで」は配信元と 1 バイトも違わない。**
 *    lint が「人が lp-tool に貼ったときに本当に読めるか」を見るためのものなので、
 *    こちらで直すと**本番と違うものを検査することになる** (設計 §6-B / §6-C)。
 *    直したいところがあれば bfaith-flow 側を直して、ここは取り直す。
 *
 *    ドリフトの照合 (写しの範囲は行頭の //>>> と //<<< で挿んである):
 *      curl -s https://bfaith-flow.onrender.com/assets/parser.js > /tmp/parser.js
 *      sed -n '/^\/\/>>>/,/^\/\/<<</p' lp-parser.js | sed '1d;$d' | diff - /tmp/parser.js
 *    fixture の契約テスト (scripts/test-ph-lp-compose-lint.mjs) が壊れれば、
 *    配信元が変わったことに気づける (設計 §6-C: 週次の自動監視は段階2)。
 *
 * ⚠️ 既知のズレ (設計 §6 の検査 19 がこれを見張る):
 *    仕様書 V2.2 の固定見出しのうち **「バッジ・補足」と「使用素材」が構造見出しに入っておらず**、
 *    `badgeSupplement` (その他項目のバケツ) に落ちている。V2.2 では
 *    `individualPrompt` = 画像ブロック全文なので画像生成への実害は無いが、
 *    **仕様書とパーサーがズレる現象は現実に起きている**。
 */

// 配信元は本物の `window` に付ける。移植ではその受け皿だけ用意して、写しには手を触れない。
const window = {};

//>>> 写し ここから — assets/parser.js と 1 バイトも違わない (行頭が //>>> の行はここだけ)
// 商品制作支援システム - 構成書パーサー（ルールベース／AI API不使用）
//
// 「LP制作システム ⑦ AI画像生成プロンプト」（V2.1 / V2.2）の出力全文を受け取り、
// Markdown見出しと固定ラベルを基準に情報を抽出する。
//
// 想定フォーマット（見出し・ラベルの表記ゆれにはある程度対応する）:
//
//   # 共通設定                      … 任意の見出し文（内容が「N枚目｜」でなければ全て共通部分とみなす）
//   ## 共通生成条件
//   ...
//   ## 商品情報
//   商品名：〇〇
//   画像サイズ：1200×1500px
//   種類・香り：〇〇／〇〇
//   ## 全体トーン
//   ...
//   ## 共通カラー
//   ベースカラー：#111111
//   アクセントカラー（無香料）：#aaaaaa
//   ## 商品再現ルール
//   ...
//   ## 共通NG
//   ...
//   ## 生成後の確認項目
//   ...
//
//   # 1枚目｜FV
//   生成目的：...
//   メインコピー：...
//   サブコピー：...
//   本文：...
//   画像構成：...
//   使用カラー：...
//   個別生成プロンプト：
//   ...
//
//   # 2枚目｜使用価値
//   ...
//
// 取得できない項目は null のまま返す（呼び出し側で「未取得」表示にする）。
// 元の文章は書き換えない・存在しない情報を補完しない。

(function (global) {
  "use strict";

  const IMAGE_HEADING_RE = /^#{1,6}\s*([0-9０-９]+)\s*枚目\s*[｜\|]\s*(.+?)\s*$/;
  const MARKDOWN_HEADING_RE = /^#{1,6}\s*(.+?)\s*$/;

  function toHalfWidthNumber(str) {
    return String(str).replace(/[０-９]/g, (c) => String.fromCharCode(c.charCodeAt(0) - 0xfee0));
  }

  function normalizeLineEndings(text) {
    return String(text || "").replace(/\r\n/g, "\n").replace(/\r/g, "\n");
  }

  function isBlank(val) {
    return val === null || val === undefined || String(val).trim() === "";
  }

  // **項目名** の次行以降を値として読む。実運用のV2.1出力で使われる形式。
  function extractBoldFields(lines) {
    const result = {};
    let label = null;
    let buffer = [];
    function flush() {
      if (label) result[label] = buffer.join("\n").trim() || null;
      buffer = [];
    }
    for (const line of lines) {
      const m = line.match(/^\s*\*\*\s*(.+?)\s*\*\*\s*(.*)$/);
      if (m) {
        flush();
        label = m[1].trim();
        buffer = m[2].trim() ? [m[2].trim()] : [];
      } else if (MARKDOWN_HEADING_RE.test(line)) {
        flush();
        label = null;
      } else if (label) {
        buffer.push(line);
      }
    }
    flush();
    return result;
  }

  // V2.1の生成プロンプト内に文章として埋め込まれているコピーを補助的に取得する。
  // 明示ラベルで取得できた値を優先し、ここでは「」で囲まれた原文だけを使う。
  function extractQuotedAfterCue(text, cuePatterns) {
    const lines = normalizeLineEndings(text).split("\n");
    for (const cue of cuePatterns) {
      for (let i = 0; i < lines.length; i++) {
        if (!cue.test(lines[i].trim())) continue;
        const nearby = lines.slice(i, Math.min(lines.length, i + 12)).join("\n");
        const quoted = nearby.match(/「([\s\S]*?)」/);
        if (quoted && quoted[1].trim()) return quoted[1].trim();
      }
    }
    return null;
  }

  // プロンプト本文に実際に登場する色名だけを、重複を除いて一覧化する。
  // AIによる推測は行わず、原文にない色は追加しない。
  function extractColorSummary(text) {
    const colorPattern =
      /白〜アイボリー〜淡いグレージュ|淡いイエロー〜オレンジ|白〜淡いブルー|白〜淡いグレージュ|白〜グレージュ|白〜アイボリー|白〜グレー|淡いグレージュ|淡いブルー|淡いイエロー|アイボリー|グレージュ|オレンジ|イエロー|ブルー|ベージュ|グレー|ホワイト|白/g;
    const matches = normalizeLineEndings(text).match(colorPattern) || [];
    const unique = [];
    matches.forEach(function (color) {
      if (unique.indexOf(color) === -1) unique.push(color);
    });
    return unique.length ? unique.join("／") : null;
  }

  // Markdown見出しの階層を保ったまま、指定見出しの内容を取得する。
  // 同じ見出しが複数ある場合（香り別の「本文」「使用カラー」など）にも対応する。
  function findMarkdownSections(text, headingPattern) {
    const lines = normalizeLineEndings(text).split("\n");
    const headings = [];
    const stack = [];

    lines.forEach(function (line, index) {
      const m = line.match(/^(#{1,6})\s*(.+?)\s*$/);
      if (!m) return;
      const level = m[1].length;
      const heading = m[2].trim();
      while (stack.length && stack[stack.length - 1].level >= level) stack.pop();
      const parentHeading = stack.length ? stack[stack.length - 1].heading : null;
      const item = { index: index, level: level, heading: heading, parentHeading: parentHeading };
      headings.push(item);
      stack.push(item);
    });

    return headings
      .filter(function (item) {
        return headingPattern.test(item.heading);
      })
      .map(function (item) {
        let end = lines.length;
        for (let i = 0; i < headings.length; i++) {
          if (headings[i].index > item.index && headings[i].level <= item.level) {
            end = headings[i].index;
            break;
          }
        }
        return {
          heading: item.heading,
          level: item.level,
          parentHeading: item.parentHeading,
          content: lines.slice(item.index + 1, end).join("\n").trim() || null,
        };
      });
  }

  function firstSectionText(text, headingPattern) {
    const sections = findMarkdownSections(text, headingPattern);
    return sections.length ? sections[0].content : null;
  }

  function allSectionText(text, headingPattern) {
    const sections = findMarkdownSections(text, headingPattern).filter(function (section) {
      return !isBlank(section.content);
    });
    if (!sections.length) return null;
    if (sections.length === 1) return sections[0].content;
    return sections
      .map(function (section) {
        return section.parentHeading
          ? "【" + section.parentHeading + "】\n" + section.content
          : section.content;
      })
      .join("\n\n");
  }

  function stripSimpleMarkdown(text) {
    return isBlank(text)
      ? null
      : String(text)
          .replace(/^\s*[-*]\s+/gm, "")
          .replace(/\*\*/g, "")
          .replace(/`/g, "")
          .trim();
  }

  function extractV22AdditionalText(blockText) {
    const structural = /^(画像の役割|目的|メイン見出し|サブ見出し|本文|商品配置|背景[・･]シーン|装飾[・･]演出|使用カラー|詳細レイアウト|レイアウト|生成指示|NG事項|生成後チェック)$/;
    const sections = findMarkdownSections(blockText, /.+/).filter(function (section) {
      return section.level === 2 && !structural.test(section.heading) && !isBlank(section.content);
    });
    return sections.length
      ? sections
          .map(function (section) {
            return "【" + section.heading + "】\n" + section.content;
          })
          .join("\n\n")
      : null;
  }

  // ラベル行 (「ラベル：値」 or 「ラベル:値」) を検出し、次のラベル行までを値として束ねる
  function extractLabeledFields(lines, aliasGroups) {
    const result = {};
    let currentKey = null;
    let buffer = [];
    const unlabeled = [];

    function flush() {
      const text = buffer.join("\n").trim();
      if (currentKey) {
        result[currentKey] = result[currentKey] ? result[currentKey] + "\n" + text : text;
      } else if (text) {
        unlabeled.push(text);
      }
      buffer = [];
    }

    for (const rawLine of lines) {
      const line = rawLine;
      const m = line.match(/^\s*[-・*]?\s*([^\s：:#][^：:]{0,24})[：:]\s?(.*)$/);
      let matchedKey = null;
      if (m) {
        const labelText = m[1].trim();
        for (const group of aliasGroups) {
          if (group.patterns.some((p) => p.test(labelText))) {
            matchedKey = group.key;
            break;
          }
        }
      }
      if (m) {
        // ラベル行の形をした行が来たら、既知/未知にかかわらずここで区切る。
        // 未知のラベルの値を直前の既知フィールドに紛れ込ませない。
        flush();
        currentKey = matchedKey;
        buffer = [matchedKey ? m[2] : line];
      } else {
        buffer.push(line);
      }
    }
    flush();

    for (const key of Object.keys(result)) {
      result[key] = result[key].trim() || null;
    }
    return { fields: result, unlabeled: unlabeled.join("\n").trim() };
  }

  const COMMON_HEADING_GROUPS = [
    { key: "commonConditions", patterns: [/生成条件/] },
    { key: "productInfo", patterns: [/商品情報|商品概要|商品[・･]種類|種類[・･]香り/] },
    { key: "overallTone", patterns: [/トーン/] },
    { key: "color", patterns: [/カラー|配色/] },
    { key: "reproductionRules", patterns: [/再現ルール|再現条件/] },
    { key: "commonNG", patterns: [/共通NG|禁止事項|^NG$/] },
    { key: "confirmationItems", patterns: [/確認項目|チェック項目/] },
  ];

  const PRODUCT_INFO_ALIASES = [
    { key: "productName", patterns: [/^商品名$/, /^品名$/] },
    { key: "imageSize", patterns: [/^画像サイズ$/, /^サイズ$/, /^画像規格$/] },
  ];

  const COLOR_ALIASES = [
    { key: "baseColor", patterns: [/^ベースカラー$/, /^共通ベースカラー$/, /^共通カラー$/] },
  ];

  const IMAGE_FIELD_ALIASES = [
    { key: "purpose", patterns: [/^生成目的$/, /^目的$/] },
    { key: "mainCopy", patterns: [/^メインコピー$/] },
    { key: "subCopy", patterns: [/^サブコピー([・･]表示情報)?$/, /^表示情報$/] },
    { key: "body", patterns: [/^本文$/] },
    { key: "composition", patterns: [/^画像構成$/, /^構成$/] },
    { key: "usedColor", patterns: [/^使用カラー$/, /^カラー$/] },
    { key: "individualPrompt", patterns: [/^個別生成プロンプト$/, /^生成プロンプト$/, /^プロンプト$/] },
  ];

  function parseCommonBlock(text) {
    const common = {
      templateVersion: null,
      commonConditions: null,
      productName: null,
      imageSize: null,
      productInfo: null,
      overallTone: null,
      baseColor: null,
      commonColors: null,
      accentColors: [], // [{ name, value }]
      reproductionRules: null,
      commonNG: null,
      confirmationItems: null,
    };
    const unclassified = [];

    // ## 見出しでサブセクションに分割
    const lines = normalizeLineEndings(text).split("\n");
    const sections = [];
    let cur = { heading: null, lines: [] };
    for (const line of lines) {
      const hm = line.match(MARKDOWN_HEADING_RE);
      if (hm) {
        if (cur.heading !== null || cur.lines.some((l) => l.trim())) sections.push(cur);
        cur = { heading: hm[1], lines: [] };
      } else {
        cur.lines.push(line);
      }
    }

    // V2.1の実出力は「**商品**」「**画像サイズ**」形式のため、共通部全体から補完する。
    const bold = extractBoldFields(lines);
    common.productName = common.productName || bold["商品名"] || bold["商品"] || null;
    common.imageSize = common.imageSize || bold["画像サイズ"] || null;
    common.productInfo = common.productInfo || bold["香り"] || bold["種類"] || null;
    common.overallTone = common.overallTone || bold["全体トーン"] || null;
    if (!common.baseColor && bold["共通ベース"] ) common.baseColor = bold["共通ベース"];
    if (!common.baseColor && common.overallTone) {
      const baseMatch = common.overallTone.match(/(?:^|\n)\s*[-・*]?\s*([^\n。]+?)を共通ベース(?:にする)?/);
      if (baseMatch) common.baseColor = baseMatch[1].trim();
    }
    common.reproductionRules = common.reproductionRules || bold["商品再現の最重要ルール"] || bold["商品再現ルール"] || null;

    const accentText = bold["香り別アクセント"] || bold["種類別アクセント"];
    if (accentText && common.accentColors.length === 0) {
      accentText.split("\n").forEach(function (line) {
        const m = line.match(/^\s*[-・*]?\s*([^：:]+)[：:]\s*(.+)$/);
        if (m) common.accentColors.push({ name: m[1].trim(), value: m[2].trim() });
      });
    }
    sections.push(cur);

    for (const section of sections) {
      const bodyLines = section.lines;
      const bodyText = bodyLines.join("\n").trim();
      if (section.heading === null) {
        if (bodyText) unclassified.push(bodyText);
        continue;
      }

      let matchedKey = null;
      for (const group of COMMON_HEADING_GROUPS) {
        if (group.patterns.some((p) => p.test(section.heading))) {
          matchedKey = group.key;
          break;
        }
      }

      if (matchedKey === "commonConditions") {
        common.commonConditions = bodyText || null;
      } else if (matchedKey === "overallTone") {
        common.overallTone = bodyText || null;
      } else if (matchedKey === "reproductionRules") {
        common.reproductionRules = bodyText || null;
      } else if (matchedKey === "commonNG") {
        common.commonNG = bodyText || null;
      } else if (matchedKey === "confirmationItems") {
        common.confirmationItems = bodyText || null;
      } else if (matchedKey === "productInfo") {
        const { fields, unlabeled } = extractLabeledFields(bodyLines, PRODUCT_INFO_ALIASES);
        common.productName = fields.productName || null;
        common.imageSize = fields.imageSize || null;
        common.productInfo = unlabeled || null;
      } else if (matchedKey === "color") {
        const { fields, unlabeled } = extractLabeledFields(bodyLines, COLOR_ALIASES);
        common.baseColor = fields.baseColor || null;
        // アクセントカラー（〇〇）: 値 の行を個別に拾う
        const accentRe = /^\s*[-・*]?\s*アクセントカラー[（(]([^）)]+)[）)]\s*[：:]\s*(.+)$/;
        const accentPlainRe = /^\s*[-・*]?\s*アクセントカラー\s*[：:]\s*(.+)$/;
        for (const l of bodyLines) {
          const am = l.match(accentRe);
          if (am) {
            common.accentColors.push({ name: am[1].trim(), value: am[2].trim() });
            continue;
          }
          const pm = l.match(accentPlainRe);
          if (pm) {
            common.accentColors.push({ name: null, value: pm[1].trim() });
          }
        }
        if (unlabeled && common.accentColors.length === 0 && !common.baseColor) {
          // ラベルが全く見つからない場合のみ、未分類として保持
          unclassified.push(unlabeled);
        }
      } else {
        // 見出し名から判定できない場合は、商品名などが直接ラベル行として
        // 混在している可能性があるので、全体をラベル抽出にもかける
        const { fields, unlabeled } = extractLabeledFields(bodyLines, PRODUCT_INFO_ALIASES);
        if (fields.productName) common.productName = common.productName || fields.productName;
        if (fields.imageSize) common.imageSize = common.imageSize || fields.imageSize;
        if (unlabeled) unclassified.push(unlabeled);
      }
    }

    // 商品名がどこにも見出しラベルとして無かった場合、共通ブロック全体から直接探す
    if (!common.productName) {
      const direct = text.match(/商品名\s*[：:]\s*(.+)/);
      if (direct) common.productName = direct[1].trim();
    }

    // V2.2は各共通項目をMarkdown見出しで構造化している。
    // 従来ロジックで取れた値を残しつつ、V2.2の見出しを正として上書きする。
    if (/AI画像生成プロンプト\s*出力テンプレート\s*V2\.2/i.test(text)) {
      common.templateVersion = "V2.2";
      common.commonConditions = firstSectionText(text, /^共通生成条件$/) || common.commonConditions;
      common.imageSize = stripSimpleMarkdown(firstSectionText(text, /^画像サイズ$/)) || common.imageSize;
      common.productName = stripSimpleMarkdown(firstSectionText(text, /^商品$/)) || common.productName;
      common.productInfo = firstSectionText(text, /^香り$/) || common.productInfo;
      common.overallTone = firstSectionText(text, /^全体トーン$/) || common.overallTone;
      common.commonColors = firstSectionText(text, /^共通使用カラー$/) || common.commonColors;
      common.reproductionRules = firstSectionText(text, /^商品再現ルール$/) || common.reproductionRules;
      common.commonNG = firstSectionText(text, /^共通NG事項$/) || common.commonNG;
      common.confirmationItems = firstSectionText(text, /^共通生成後チェック$/) || common.confirmationItems;

      if (common.commonColors) {
        const base = common.commonColors.match(/ベース背景\s*[：:]\s*`?(#[0-9A-Fa-f]{6})`?/);
        if (base) common.baseColor = base[1].toUpperCase();
        common.accentColors = [];
        let colorGroup = null;
        common.commonColors.split("\n").forEach(function (line) {
          const heading = line.match(/^###\s+(.+?)\s*$/);
          if (heading) {
            colorGroup = stripSimpleMarkdown(heading[1]);
            return;
          }
          const m = line.match(/^\s*[-*]?\s*([^：:]+)[：:]\s*`?(#[0-9A-Fa-f]{6})`?\s*$/);
          if (m && (/アクセント|濃色|淡色/.test(m[1]) || (colorGroup && /SAVON|MUSK|OSMANTHUS|サボン|ムスク|キンモクセイ/i.test(colorGroup)))) {
            const colorName = m[1].trim();
            common.accentColors.push({
              name: colorGroup && /^(淡色|濃色)$/.test(colorName) ? colorGroup + " " + colorName : colorName,
              value: m[2].toUpperCase(),
            });
          }
        });
      }
    } else {
      common.templateVersion = "V2.1";
    }

    return { common, unclassified: unclassified.filter(Boolean) };
  }

  function parseImageBlock(no, name, blockText, templateVersion) {
    const lines = normalizeLineEndings(blockText).split("\n");
    const { fields, unlabeled } = extractLabeledFields(lines, IMAGE_FIELD_ALIASES);

    // 「## 生成目的」「## 生成プロンプト」の見出し形式にも対応する。
    const headingFields = {};
    let current = null;
    let buffer = [];
    function flushHeading() {
      if (current) headingFields[current] = buffer.join("\n").trim() || null;
      buffer = [];
    }
    lines.forEach(function (line) {
      const hm = line.match(MARKDOWN_HEADING_RE);
      if (hm) {
        flushHeading();
        const heading = hm[1].trim();
        if (/生成目的|^目的$/.test(heading)) current = "purpose";
        else if (/生成プロンプト|個別プロンプト/.test(heading)) current = "individualPrompt";
        else if (/メインコピー/.test(heading)) current = "mainCopy";
        else if (/サブコピー|表示情報/.test(heading)) current = "subCopy";
        else if (/^本文$/.test(heading)) current = "body";
        else if (/画像構成|^構成$/.test(heading)) current = "composition";
        else if (/使用カラー|配色/.test(heading)) current = "usedColor";
        else current = null;
      } else if (current) {
        buffer.push(line);
      }
    });
    flushHeading();

    Object.keys(headingFields).forEach(function (key) {
      if (isBlank(fields[key])) fields[key] = headingFields[key];
    });

    // 実運用フォーマットではコピー類が「生成プロンプト」の文章内にある。
    // 明示見出しがない場合に限り、近くの「」から原文をそのまま補完する。
    if (isBlank(fields.mainCopy)) {
      fields.mainCopy = extractQuotedAfterCue(blockText, [
        /メインコピー/,
        /その下に大きく/,
        /^上部に/,
        /見出しとして/,
      ]);
    }
    if (isBlank(fields.subCopy)) {
      fields.subCopy = extractQuotedAfterCue(blockText, [/サブコピー/]);
    }
    if (isBlank(fields.body)) {
      fields.body = extractQuotedAfterCue(blockText, [/本文として/, /^本文$/]);
    }
    if (isBlank(fields.usedColor)) {
      fields.usedColor = extractColorSummary(fields.individualPrompt || blockText);
    }

    const isV22 = templateVersion === "V2.2";
    const imageRole = isV22 ? firstSectionText(blockText, /^画像の役割$/) : null;
    const v22Purpose = isV22 ? firstSectionText(blockText, /^目的$/) : null;
    const v22Main = isV22 ? allSectionText(blockText, /^メイン見出し$/) : null;
    const v22Sub = isV22 ? allSectionText(blockText, /^サブ見出し$/) : null;
    const v22Body = isV22 ? allSectionText(blockText, /^本文$/) : null;
    const badgeSupplement = isV22 ? extractV22AdditionalText(blockText) : null;
    const productPlacement = isV22 ? firstSectionText(blockText, /^商品配置$/) : null;
    const backgroundScene = isV22 ? firstSectionText(blockText, /^背景[・･]シーン$/) : null;
    const decorationEffect = isV22 ? firstSectionText(blockText, /^装飾[・･]演出$/) : null;
    const v22Colors = isV22 ? allSectionText(blockText, /^使用カラー$/) : null;
    const detailedLayout = isV22 ? firstSectionText(blockText, /^(詳細レイアウト|レイアウト)$/) : null;
    const generationInstruction = isV22 ? firstSectionText(blockText, /^生成指示$/) : null;
    const ngItems = isV22 ? firstSectionText(blockText, /^NG事項$/) : null;
    const postCheck = isV22 ? firstSectionText(blockText, /^生成後チェック$/) : null;

    if (isV22) {
      fields.purpose = v22Purpose || fields.purpose || imageRole;
      fields.mainCopy = v22Main || fields.mainCopy;
      fields.subCopy = v22Sub || fields.subCopy;
      fields.body = v22Body || fields.body;
      fields.usedColor = v22Colors || fields.usedColor;
    }

    const structuredKeys = ["mainCopy", "subCopy", "body", "composition"];
    const hasStructured = structuredKeys.some((k) => !isBlank(fields[k]));

    let individualPrompt = isV22 ? blockText.trim() : fields.individualPrompt || (unlabeled || null);
    let composition = fields.composition || null;

    if (!hasStructured && !composition) {
      // 分解できない場合：元の個別生成プロンプトを画像構成に残す
      composition = individualPrompt || null;
    }

    return {
      no: Number(no),
      name: name || null,
      imageRole: imageRole,
      purpose: fields.purpose || null,
      mainCopy: fields.mainCopy || null,
      subCopy: fields.subCopy || null,
      body: fields.body || null,
      badgeSupplement: badgeSupplement,
      productPlacement: productPlacement,
      backgroundScene: backgroundScene,
      decorationEffect: decorationEffect,
      composition: composition,
      usedColor: fields.usedColor || null,
      detailedLayout: detailedLayout,
      generationInstruction: generationInstruction,
      ngItems: ngItems,
      postCheck: postCheck,
      individualPrompt: individualPrompt,
      rawBlockText: blockText.trim(),
    };
  }

  function parseConstructionDoc(rawInput) {
    const text = normalizeLineEndings(rawInput || "");
    const lines = text.split("\n");
    const templateVersion = /AI画像生成プロンプト\s*出力テンプレート\s*V2\.2/i.test(text) ? "V2.2" : "V2.1";

    // 画像見出しの行インデックスを収集
    const headingIdx = [];
    lines.forEach((line, i) => {
      const m = line.match(IMAGE_HEADING_RE);
      if (m) {
        headingIdx.push({ index: i, no: toHalfWidthNumber(m[1]), name: m[2] });
      }
    });

    const warnings = [];

    // 最終画像の後ろに置かれる「共通NG」「生成後確認項目」は共通部として扱う。
    let trailingCommonIndex = -1;
    if (headingIdx.length) {
      const lastImageIndex = headingIdx[headingIdx.length - 1].index;
      for (let i = lastImageIndex + 1; i < lines.length; i++) {
        const hm = lines[i].match(MARKDOWN_HEADING_RE);
        if (hm && /共通NG|AI生成時の共通NG|生成後の確認項目|AI生成後の確認項目/.test(hm[1])) {
          trailingCommonIndex = i;
          break;
        }
      }
    }
    const commonParts = [headingIdx.length ? lines.slice(0, headingIdx[0].index).join("\n") : text];
    if (trailingCommonIndex >= 0) commonParts.push(lines.slice(trailingCommonIndex).join("\n"));
    const commonText = commonParts.join("\n");
    const { common, unclassified } = parseCommonBlock(commonText);

    const images = [];
    if (headingIdx.length === 0) {
      warnings.push("「# N枚目｜画像名」の形式の見出しが見つかりませんでした。画像構成を取得できません。");
    } else {
      for (let i = 0; i < headingIdx.length; i++) {
        const start = headingIdx[i].index + 1;
        let end = i + 1 < headingIdx.length ? headingIdx[i + 1].index : lines.length;
        if (i === headingIdx.length - 1 && trailingCommonIndex >= 0) end = trailingCommonIndex;
        const blockText = lines.slice(start, end).join("\n");
        images.push(parseImageBlock(headingIdx[i].no, headingIdx[i].name, blockText, templateVersion));
      }
    }

    // 欠落項目の警告
    const commonFieldLabels = {
      commonConditions: "共通生成条件",
      productName: "商品名",
      imageSize: "画像サイズ",
      productInfo: "商品・種類・香りなどの情報",
      overallTone: "全体トーン",
      baseColor: "共通ベースカラー",
      reproductionRules: "商品再現ルール",
      commonNG: "共通NG",
      confirmationItems: "生成後の確認項目",
    };
    for (const [key, label] of Object.entries(commonFieldLabels)) {
      if (isBlank(common[key])) warnings.push(`「${label}」を取得できませんでした（未取得）。`);
    }
    if (common.accentColors.length === 0) {
      warnings.push("「香り・種類別アクセントカラー」を取得できませんでした（未取得）。");
    }
    images.forEach((img) => {
      if (templateVersion === "V2.2") {
        const required = [
          ["imageRole", "画像の役割"],
          ["mainCopy", "メイン見出し"],
          ["usedColor", "使用カラー"],
          ["detailedLayout", "詳細レイアウト"],
          ["generationInstruction", "生成指示"],
          ["ngItems", "NG事項"],
          ["postCheck", "生成後チェック"],
        ];
        required.forEach(function (item) {
          if (isBlank(img[item[0]])) {
            warnings.push(`${img.no}枚目「${img.name || "?"}」の${item[1]}を取得できませんでした。`);
          }
        });
      } else if (isBlank(img.purpose)) {
        warnings.push(`${img.no}枚目「${img.name || "?"}」の生成目的を取得できませんでした。`);
      }
    });

    return {
      common,
      images,
      templateVersion,
      unclassified,
      warnings,
      isEmpty: !text.trim(),
    };
  }

  global.LPParser = { parseConstructionDoc, isBlank };
})(window);
//<<< 写し ここまで

export const { parseConstructionDoc, isBlank } = window.LPParser;
/** 移植元の取得時ハッシュ。ドリフトを見るときの照合先 (設計 §6-C) */
export const PARSER_SOURCE_SHA256 = '1a2f22856ee134938a0f7ce1c79a9d58023bb23ce18778c369da3855413f2855';
export const PARSER_SOURCE_URL = 'https://bfaith-flow.onrender.com/assets/parser.js';
export const PARSER_FETCHED_AT = '2026-10-01';
