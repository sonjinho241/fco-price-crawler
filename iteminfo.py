# -*- coding: utf-8 -*-
"""넥슨 확률표(iteminfo.nexon.com/probability/fco) 해석. 네트워크·DB 를 모르는 순수 함수만 둔다.

페이지 하나 = 표 하나. 표는 두 모양이다(2026-10-13 전량 301개 실측).
- 3칸 [아이템, 개수, 확률]: 상자. 아이템칸은 다른 표의 **제목**(하위 상자) 또는 물건 이름.
- 4칸 [등급, 아이템, 개수, 확률]: 등급칸이 rowspan 으로 묶인다.
  - 등급 'BP'      → 아이템칸이 BP 금액
  - 아이템 '개봉일 0~1시 사이 기준가 TOP k' → Top Price 순위. 등급칸 = 대상 클래스 목록,
    제목 = 강화 범위·최종 OVR 하한('(11강, 117+)')
  - 아이템 '선수명 (N강)' → 카드 확정. 등급칸 = 시즌 코드(예 DCB)
  - 그 밖(CP·코치 등급) → BP 로 못 바꾸는 물건

확률은 넥슨 표기 문자열 그대로 둔다(float 금지 — 근사 금지 원칙).
"""
from __future__ import annotations

import hashlib
import html as _html
import json
import re
from dataclasses import asdict, dataclass, field
from decimal import Decimal
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

_TAG = re.compile(r"<[^>]+>")
_WS = re.compile(r"\s+")
_TOP_RE = re.compile(r"기준가\s*TOP\s*(\d+)$")
_CARD_RE = re.compile(r"^(.+?)\s*\((\d{1,2})강\)$")
# 'Top Price 105 (11강, 117+)' / '(8~9강, 109+)' / '(10강)' / '(1강)'
_TP_TITLE_RE = re.compile(r"Top Price\s*(\d+)[^()]*\((\d{1,2})(?:\s*~\s*(\d{1,2}))?강(?:\s*,\s*(\d{2,3})\+)?\)")
_PERIOD_RE = re.compile(r"판매 or 획득 가능 기간\s*:\s*(.+)$")


def _text(fragment: str) -> str:
    return _WS.sub(" ", _html.unescape(_TAG.sub("", fragment))).strip()


def norm_title(title: str) -> str:
    """제목 비교용 키. 넥슨은 같은 상자를 부모 표에서 공백만 다르게 적기도 한다."""
    return _WS.sub("", title)


def norm_code(code: str) -> str:
    """시즌 코드 비교용 키: '25 UCL'='25UCL', 'NO7'='NO.7'."""
    return re.sub(r"[\s.]", "", code).upper()


@dataclass
class RawRow:
    grade: str
    item: str
    quantity: int
    probability: str  # 넥슨 표기 그대로('47.4800000000000000%')


@dataclass
class RawTable:
    sn: int
    title: str
    period: Optional[str]
    four_columns: bool
    rows: List[RawRow]


def parse_index(page: str) -> List[Tuple[int, str]]:
    """목록 페이지 → [(sn, 제목)]. 순서는 넥슨 목록 순서(대체로 sn 내림차순)."""
    out, seen = [], set()
    for sn, title in re.findall(r'href="/probability/fco\?sn=(\d+)"[^>]*>(.*?)</a>', page, re.S):
        if int(sn) not in seen:
            seen.add(int(sn))
            out.append((int(sn), _text(title)))
    return out


def parse_page(sn: int, page: str) -> RawTable:
    block = re.search(r'<div class="table_layout">(.*?)</table>', page, re.S)
    if block is None:
        raise ValueError(f"sn={sn}: 확률표가 없습니다")
    b = block.group(1)
    title_m = re.search(r"<h3[^>]*>(.*?)</h3>", b, re.S)
    title = _text(title_m.group(1)) if title_m else ""
    period = None
    for remark in re.findall(r'<div class="new_remark">(.*?)</div>', b, re.S):
        for line in re.split(r"<br\s*/?>", remark):
            m = _PERIOD_RE.search(_text(line).lstrip("* "))
            if m:
                period = m.group(1).strip()
    heads = [_text(h) for h in re.findall(r"<th[^>]*>(.*?)</th>", b, re.S)]
    four = len(heads) == 4
    body = re.search(r"<tbody>(.*?)</tbody>", b, re.S)
    rows: List[RawRow] = []
    grade, left = "", 0
    for tr in re.findall(r"<tr>(.*?)</tr>", body.group(1) if body else "", re.S):
        cells = re.findall(r"<td([^>]*)>(.*?)</td>", tr, re.S)
        if not cells:
            continue
        if four and (left <= 0 or len(cells) == 4):
            attrs, value = cells[0]
            span = re.search(r'rowspan="(\d+)"', attrs)
            grade, left = _text(value), int(span.group(1)) if span else 1
            cells = cells[1:]
        if four:
            left -= 1
        if len(cells) != 3:
            raise ValueError(f"sn={sn}: 칸 수가 맞지 않는 행 {[_text(c) for _, c in cells]}")
        item, qty, prob = (_text(c) for _, c in cells)
        rows.append(RawRow(grade if four else "", item, int(qty.replace(",", "")), prob))
    return RawTable(sn, title, period, four, rows)


def probability_value(text: str) -> Decimal:
    """'47.48%' → Decimal('0.4748'). 소수 자릿수를 잃지 않는다."""
    return Decimal(text.strip().rstrip("%").strip()) / 100


# ---- 해석 -----------------------------------------------------------------


@dataclass
class PoolSpec:
    """Top Price 표의 후보 조건. 카드 목록은 크롤러가 이 조건으로 DB 에서 만든다."""

    class_codes: List[str]
    strongs: List[int]
    min_ovr: Optional[int]
    top_n: int


@dataclass
class ParsedRow:
    row_no: int
    raw_grade: str
    raw_item: str
    raw_probability: str
    probability: str  # Decimal 문자열(0~1)
    quantity: int
    row_kind: str  # card / top_rank / sub_pack / bp / item / unparsed
    season_code: Optional[str] = None
    player_name: Optional[str] = None
    strong: Optional[int] = None
    pool_key: Optional[str] = None
    top_rank: Optional[int] = None
    sub_sn: Optional[int] = None
    bp_amount: Optional[int] = None


@dataclass
class ParsedTable:
    sn: int
    title: str
    period: Optional[str]
    table_kind: str  # top_price / final_ovr / box / mixed
    draw_mode: str  # one / all
    rows: List[ParsedRow]
    pool: Optional[PoolSpec] = None
    ambiguous_refs: List[str] = field(default_factory=list)

    @property
    def content_hash(self) -> str:
        payload = json.dumps(
            [self.title, self.period, self.draw_mode, [asdict(r) for r in self.rows]],
            ensure_ascii=False, sort_keys=True,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def pool_spec(title: str, grades: Iterable[str], row_count: int) -> Optional[PoolSpec]:
    m = _TP_TITLE_RE.search(title)
    if m is None:
        return None
    top_n, lo, hi, ovr = m.groups()
    lo_i, hi_i = int(lo), int(hi or lo)
    codes: List[str] = []
    for grade in grades:
        for code in grade.split(","):
            code = code.strip()
            if code and code not in codes:
                codes.append(code)
    return PoolSpec(
        class_codes=codes,
        strongs=list(range(lo_i, hi_i + 1)),
        min_ovr=int(ovr) if ovr else None,
        top_n=int(top_n) if int(top_n) else row_count,
    )


class TitleIndex:
    """제목 → sn. 같은 제목이 여러 sn 에 있으면(2026-10 실측 2건) 형제 행들의 sn 과
    가장 가까운 쪽을 고른다 — 넥슨은 한 상자의 하위 표를 연속 번호로 만든다."""

    def __init__(self, titles: Sequence[Tuple[int, str]]):
        self._by: Dict[str, List[int]] = {}
        for sn, title in titles:
            self._by.setdefault(norm_title(title), []).append(sn)

    def candidates(self, title: str) -> List[int]:
        return self._by.get(norm_title(title), [])


def _median(values: List[int]) -> float:
    ordered = sorted(values)
    mid = len(ordered) // 2
    return float(ordered[mid]) if len(ordered) % 2 else (ordered[mid - 1] + ordered[mid]) / 2


def classify(table: RawTable, titles: TitleIndex) -> ParsedTable:
    rows: List[ParsedRow] = []
    ambiguous: List[str] = []
    pending: List[Tuple[ParsedRow, List[int]]] = []
    for no, raw in enumerate(table.rows, start=1):
        row = ParsedRow(
            row_no=no, raw_grade=raw.grade, raw_item=raw.item, raw_probability=raw.probability,
            probability=str(probability_value(raw.probability)), quantity=raw.quantity,
            row_kind="unparsed",
        )
        top = _TOP_RE.search(raw.item)
        card = _CARD_RE.match(raw.item)
        if table.four_columns and raw.grade == "BP" and raw.item.replace(",", "").isdigit():
            row.row_kind, row.bp_amount = "bp", int(raw.item.replace(",", ""))
        elif table.four_columns and top:
            row.row_kind, row.top_rank, row.pool_key = "top_rank", int(top.group(1)), ""
        elif table.four_columns and card and raw.grade:
            row.row_kind = "card"
            row.season_code, row.player_name, row.strong = raw.grade, card.group(1).strip(), int(card.group(2))
        elif table.four_columns:
            row.row_kind = "item"
        else:
            found = titles.candidates(raw.item)
            if len(found) == 1:
                row.row_kind, row.sub_sn = "sub_pack", found[0]
            elif found:
                row.row_kind = "sub_pack"
                pending.append((row, found))
            else:
                row.row_kind = "item"
        rows.append(row)

    siblings = [r.sub_sn for r in rows if r.sub_sn is not None]
    for row, found in pending:
        if siblings:
            center = _median(siblings)
            row.sub_sn = min(found, key=lambda sn: (abs(sn - center), sn))
        else:
            row.sub_sn = min(found)
        ambiguous.append(f"{row.raw_item} → {row.sub_sn} (후보 {found})")

    kinds = {r.row_kind for r in rows}
    if kinds == {"top_rank"}:
        table_kind = "top_price"
    elif kinds <= {"card"}:
        table_kind = "final_ovr"
    elif "top_rank" in kinds or "card" in kinds:
        table_kind = "mixed"
    else:
        table_kind = "box"

    # '모두 획득' 상자는 확률칸이 전부 100% 다(합이 100% 를 넘는다).
    probs = [Decimal(r.probability) for r in rows]
    draw_mode = "all" if len(rows) > 1 and all(p == 1 for p in probs) else "one"

    pool = None
    if table_kind == "top_price":
        pool = pool_spec(table.title, (r.raw_grade for r in rows), len(rows))
    return ParsedTable(table.sn, table.title, table.period, table_kind, draw_mode, rows, pool, ambiguous)

