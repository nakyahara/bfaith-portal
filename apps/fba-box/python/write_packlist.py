# -*- coding: utf-8 -*-
"""STA パックリストExcel への書き込み (箱別数量・箱数・重量寸法)。

方式 = 原本 .xlsx (zip) の該当シート XML だけをセル単位で差し替える。
openpyxl で load→save すると sharedStrings が消え・styles が書き換わり・数式のキャッシュ値が
落ちる (2026-09-03 実物で確認) ので、STA が受け付ける保証のある「原本と同じバイト列 + 対象セル」
に寄せる。書かないセル・他エントリは原本と byte 一致 (検証で保証)。

要件 F-7: unlocked な入力セルのみ (取込時に locked 検査済み) / 数式セル不触 /
サーバ側独自検算 (書いた値を再読込して突合・構造 fingerprint 不変)。

入力 (stdin JSON):
  { "template": "<原本パス>", "output": "<出力パス>",
    "sheets": [ { "sheetName": "...", "cells": [ {"row": 6, "col": 13, "value": 3, "kind": "qty"}, ... ] } ] }
出力 (stdout JSON 1個): { ok, sha256, written, verify: {...} } / { ok:false, error, message }
"""
import hashlib
import json
import os
import re
import sys
import zipfile
from xml.sax.saxutils import unescape

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import parse_packlist  # noqa: E402

MAX_CELLS = 50000


class WriteError(Exception):
    def __init__(self, error, message, **extra):
        super().__init__(message)
        self.error = error
        self.message = message
        self.extra = extra


def col_letter(n):
    s = ''
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def col_index(letters):
    n = 0
    for ch in letters:
        n = n * 26 + (ord(ch) - 64)
    return n


def number_text(v):
    """Excel の <v> 用の数値表記 (整数は '3'、小数は '12.4')"""
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        raise WriteError('bad_value', f'数値以外は書けません: {v!r}')
    f = float(v)
    if f != f or f in (float('inf'), float('-inf')):
        raise WriteError('bad_value', f'不正な数値: {v!r}')
    if f.is_integer():
        return str(int(f))
    return repr(round(f, 6))


def sheet_paths(zf):
    """シート名 → zip 内パス (xl/workbook.xml + rels から)"""
    wb = zf.read('xl/workbook.xml').decode('utf-8')
    rels = zf.read('xl/_rels/workbook.xml.rels').decode('utf-8')
    rid_to_target = {}
    for m in re.finditer(r'<Relationship\b([^>]*)/?>', rels):
        attrs = dict(re.findall(r'(\w+)="([^"]*)"', m.group(1)))
        if attrs.get('Type', '').endswith('/worksheet'):
            target = attrs.get('Target', '')
            if target.startswith('/'):
                target = target[1:]
            elif not target.startswith('xl/'):
                target = 'xl/' + target
            rid_to_target[attrs.get('Id')] = target
    out = {}
    for m in re.finditer(r'<sheet\b([^>]*)/?>', wb):
        attrs = dict(re.findall(r'([\w:]+)="([^"]*)"', m.group(1)))
        name = unescape(attrs.get('name', ''), {'&quot;': '"', '&apos;': "'"})
        rid = attrs.get('r:id') or attrs.get('id')
        if name and rid in rid_to_target:
            out[name] = rid_to_target[rid]
    return out


def col_styles(xml):
    """<cols> の列スタイル (min..max → style)。新規セルに列スタイルを継承させる"""
    styles = []
    m = re.search(r'<cols>(.*?)</cols>', xml, re.S)
    if not m:
        return styles
    for cm in re.finditer(r'<col\b([^>]*)/?>', m.group(1)):
        attrs = dict(re.findall(r'(\w+)="([^"]*)"', cm.group(1)))
        if 'style' in attrs:
            styles.append((int(attrs['min']), int(attrs['max']), attrs['style']))
    return styles


def style_for(styles, c):
    for lo, hi, st in styles:
        if lo <= c <= hi:
            return st
    return None


ROW_OPEN_RE = r'<row\b(?=[^>]*\br="{r}")([^>]*?)(/?)>'
CELL_RE = re.compile(r'<c\b(?=[^>]*\br="([A-Z]+)(\d+)")([^>]*?)(?:/>|>(.*?)</c>)', re.S)


def patch_row(xml, row_no, cells_to_write, styles):
    """1行分のセルを差し替え/挿入した XML を返す"""
    m = re.search(ROW_OPEN_RE.format(r=row_no), xml)
    if not m:
        raise WriteError('row_missing', f'{row_no}行目が原本に存在しません (未知の形式)')
    attrs, selfclose = m.group(1), m.group(2)
    if selfclose:
        row_start, row_end = m.start(), m.end()
        inner = ''
        open_tag = f'<row{attrs}>'
        close_tag = '</row>'
    else:
        end = xml.find('</row>', m.end())
        if end < 0:
            raise WriteError('bad_xml', f'{row_no}行目の終端が見つかりません')
        row_start, row_end = m.start(), end + len('</row>')
        inner = xml[m.end():end]
        open_tag = xml[m.start():m.end()]
        close_tag = '</row>'

    # 既存セルを列順に分解
    parts = []   # (col_idx, text)
    pos = 0
    for cm in CELL_RE.finditer(inner):
        if cm.start() != pos and inner[pos:cm.start()].strip():
            raise WriteError('bad_xml', f'{row_no}行目にセル以外の要素があります')
        if int(cm.group(2)) != row_no:
            raise WriteError('bad_xml', f'{row_no}行目に他の行のセル参照があります')
        parts.append([col_index(cm.group(1)), cm.group(0), cm.group(3)])
        pos = cm.end()
    if inner[pos:].strip():
        raise WriteError('bad_xml', f'{row_no}行目にセル以外の要素があります')
    for i in range(1, len(parts)):
        if parts[i][0] <= parts[i - 1][0]:
            raise WriteError('bad_xml', f'{row_no}行目のセル順が崩れています')

    for c, value in cells_to_write:
        ref = f'{col_letter(c)}{row_no}'
        existing = next((p for p in parts if p[0] == c), None)
        if existing is not None:
            old_attrs = existing[2]
            if '<f>' in existing[1] or '<f ' in existing[1]:
                raise WriteError('formula_cell', f'{ref} は数式セルのため書きません')
            sm = re.search(r'\bs="([^"]*)"', old_attrs)
            s_attr = f' s="{sm.group(1)}"' if sm else ''
            existing[1] = f'<c r="{ref}"{s_attr} t="n"><v>{number_text(value)}</v></c>'
        else:
            st = style_for(styles, c)
            s_attr = f' s="{st}"' if st else ''
            new = [c, f'<c r="{ref}"{s_attr} t="n"><v>{number_text(value)}</v></c>', '']
            idx = next((i for i, p in enumerate(parts) if p[0] > c), len(parts))
            parts.insert(idx, new)
    new_inner = ''.join(p[1] for p in parts)
    return xml[:row_start] + open_tag + new_inner + close_tag + xml[row_end:]


def patch_sheet_xml(xml, cells):
    styles = col_styles(xml)
    by_row = {}
    seen = set()
    for cell in cells:
        r, c = int(cell['row']), int(cell['col'])
        if r <= 0 or c <= 0 or r > 1048576 or c > 16384:
            raise WriteError('bad_cell', f'セル位置が不正です: row={r} col={c}')
        if (r, c) in seen:
            raise WriteError('bad_cell', f'同じセルへの二重書き込み: {col_letter(c)}{r}')
        seen.add((r, c))
        by_row.setdefault(r, []).append((c, cell['value']))
    for r in sorted(by_row):
        xml = patch_row(xml, r, sorted(by_row[r]), styles)
    return xml


def sha256_of(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


def main():
    try:
        req = json.load(sys.stdin)
    except Exception as e:
        return fail('bad_input', f'入力JSONを読めません: {e}')
    try:
        result = write(req)
    except WriteError as e:
        return fail(e.error, e.message, **e.extra)
    print(json.dumps(result, ensure_ascii=False))


def fail(error, message, **extra):
    out = {'ok': False, 'error': error, 'message': message}
    out.update(extra)
    print(json.dumps(out, ensure_ascii=False))
    sys.exit(0)


def write(req):
    template, output = req.get('template'), req.get('output')
    sheets = req.get('sheets') or []
    if not template or not output or not os.path.isfile(template):
        raise WriteError('bad_input', '原本 (template) / 出力先 (output) が不正です')
    total_cells = sum(len(s.get('cells') or []) for s in sheets)
    if total_cells == 0:
        raise WriteError('nothing_to_write', '書き込むセルがありません')
    if total_cells > MAX_CELLS:
        raise WriteError('too_many_cells', 'セル数が多すぎます')

    with zipfile.ZipFile(template) as zin:
        paths = sheet_paths(zin)
        patched = {}
        for s in sheets:
            name = s.get('sheetName')
            if name not in paths:
                raise WriteError('sheet_missing', f'シート「{name}」が原本にありません')
            p = paths[name]
            if p in patched:
                raise WriteError('bad_input', f'シート「{name}」が二重に指定されています')
            xml = zin.read(p).decode('utf-8')
            patched[p] = patch_sheet_xml(xml, s.get('cells') or []).encode('utf-8')
        # 出力: 原本のエントリ順・圧縮属性をそのまま、差し替えシートだけ新しい内容
        tmp = output + '.part'
        with zipfile.ZipFile(tmp, 'w') as zout:
            for info in zin.infolist():
                data = patched.get(info.filename)
                if data is None:
                    data = zin.read(info.filename)
                zi = zipfile.ZipInfo(info.filename, date_time=info.date_time)
                zi.compress_type = info.compress_type
                zi.external_attr = info.external_attr
                zout.writestr(zi, data)
        os.replace(tmp, output)

    verify = verify_output(template, output, sheets, set(patched))
    return {'ok': True, 'sha256': sha256_of(output), 'written': total_cells, 'verify': verify}


def verify_output(template, output, sheets, patched_paths):
    """独自検算: ①書いたセルの再読込一致 ②他エントリの byte 一致 ③構造 fingerprint 不変"""
    import openpyxl
    wb = openpyxl.load_workbook(output, data_only=False)
    mismatches = []
    checked = 0
    for s in sheets:
        ws = wb[s['sheetName']]
        for cell in s.get('cells') or []:
            got = ws.cell(row=int(cell['row']), column=int(cell['col'])).value
            want = cell['value']
            checked += 1
            if not isinstance(got, (int, float)) or abs(float(got) - float(want)) > 1e-9:
                mismatches.append({'row': cell['row'], 'col': cell['col'], 'want': want, 'got': got})
    if mismatches:
        raise WriteError('verify_failed', '書いた値を再読込すると一致しません', mismatches=mismatches[:20])

    with zipfile.ZipFile(template) as za, zipfile.ZipFile(output) as zb:
        na, nb = [i.filename for i in za.infolist()], [i.filename for i in zb.infolist()]
        if na != nb:
            raise WriteError('verify_failed', 'zip のエントリ構成が原本と違います')
        changed = [n for n in na if za.read(n) != zb.read(n)]
        unexpected = [n for n in changed if n not in patched_paths]
        if unexpected:
            raise WriteError('verify_failed', '書き換え対象外のエントリが変わっています', entries=unexpected)

    try:
        a = parse_packlist.analyze(template)
        b = parse_packlist.analyze(output)
    except parse_packlist.ParseError as e:
        raise WriteError('verify_failed', f'出力の構造解析に失敗: {e.message}')
    if a['fingerprint'] != b['fingerprint']:
        raise WriteError('verify_failed', '出力の構造 fingerprint が原本と違います')
    for sa, sb in zip(a['sheets'], b['sheets']):
        for key in ('headerRow', 'boxNameRow', 'dimRows', 'boxColumns', 'packingGroupId', 'protected'):
            if sa.get(key) != sb.get(key):
                raise WriteError('verify_failed', f'出力の構造 ({key}) が原本と違います')
        if [r['row'] for r in sa['skuRows']] != [r['row'] for r in sb['skuRows']]:
            raise WriteError('verify_failed', '出力のSKU行構成が原本と違います')
    return {'cellsChecked': checked, 'changedEntries': sorted(patched_paths), 'fingerprint': b['fingerprint']}


if __name__ == '__main__':
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:
        print(json.dumps({'ok': False, 'error': 'internal', 'message': str(e)}, ensure_ascii=False))
        sys.exit(2)
