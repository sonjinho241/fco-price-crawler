# -*- coding: utf-8 -*-
"""넥슨 확률표 해석. fixtures/iteminfo/*.html 은 2026-10-13 실제 페이지 원본이다.

    python -m unittest discover -s tests
"""
import pathlib
import sys
import unittest
from decimal import Decimal

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

import iteminfo as ii  # noqa: E402

FIX = pathlib.Path(__file__).resolve().parent / "fixtures" / "iteminfo"

# 부모 상자가 참조하는 하위 표 제목들(실측 목록 일부). 25211/25215 는 제목이 같다.
TITLES = [
    (25210, "PTG 포함 Top Price 800 (11강, 113+)"),
    (25211, "UC Top Price 105 (11강, 117+)"),
    (25212, "26TOTS Top Price 140 (11강, 118+)"),
    (25213, "SPT Top Price 75 (11강, 117+)"),
    (25214, "26FSL Top Price 32 (11강, 117+)"),
    (25215, "UC Top Price 105 (11강, 117+)"),
    (25232, "행운의 BP 카드 (350만~700만)"),
    (25276, "[진짜 귀한 가을전어] UC, SPT, 26FSL 클래스 ALL 선수팩 (8강)"),
    (25277, "[진짜 귀한 가을전어][UC 8강 포함] 멀티 클래스 최종 OVR 129+ 프리미엄팩 (8~11강)"),
    (25283, "[가을전어] BP 카드 1850만 BP"),
]


def load(sn):
    return ii.classify(ii.parse_page(sn, (FIX / f"s{sn}.html").read_text(encoding="utf-8")), ii.TitleIndex(TITLES))


FINAL_OVR = """<div class="table_layout"><h3 class="name">테스트 최종 OVR 팩</h3>
<div class="new_remark">* 판매 or 획득 가능 기간 : 상시 운영</div>
<table><thead><tr><th class="rating">등급</th><th class="name">아이템</th><th class="amount">개수</th><th class="percent">확률</th></tr></thead>
<tbody>
<tr><td class="rating" rowspan="2">DCB</td><td class="name">크리스티아누 호날두 (8강)</td><td class="amount">1</td><td class="percent">40.0000000000000000%</td></tr>
<tr><td class="name">T. 알렉산더-아놀드 (7강)</td><td class="amount">1</td><td class="percent">35.0000000000000000%</td></tr>
<tr><td class="rating" rowspan="1">25 UCL</td><td class="name">에덴 아자르 (10강)</td><td class="amount">1</td><td class="percent">25.0000000000000000%</td></tr>
</tbody></table></div>"""


class ParsePageTest(unittest.TestCase):
    def test_box_with_ambiguous_title_picks_sibling_block(self):
        t = load(25235)
        self.assertEqual(t.title, "[11강 20%] 11강팩 or BP 상자")
        self.assertEqual(t.period, "2026-09-30 점검 후 ~ 2026-10-29 점검 전")
        self.assertEqual((t.table_kind, t.draw_mode), ("box", "one"))
        self.assertEqual([r.sub_sn for r in t.rows], [25211, 25212, 25213, 25214, 25210, 25232])
        self.assertEqual(t.rows[-1].quantity, 50)
        self.assertEqual(len(t.ambiguous_refs), 1)

    def test_top_price_pool_spec(self):
        t = load(25099)
        self.assertEqual(t.table_kind, "top_price")
        self.assertEqual([r.top_rank for r in t.rows], list(range(1, 26)))
        self.assertEqual(t.pool, ii.PoolSpec(["UC"], [11], 121, 25))
        self.assertEqual(t.rows[0].raw_probability, "0.1432664756446990%")
        self.assertEqual(Decimal(t.rows[0].probability), Decimal("0.001432664756446990"))

    def test_bp_card(self):
        t = load(25232)
        self.assertEqual([(r.row_kind, r.bp_amount) for r in t.rows],
                         [("bp", 7_000_000), ("bp", 4_500_000), ("bp", 3_500_000)])
        self.assertEqual(sum(Decimal(r.probability) for r in t.rows), 1)

    def test_take_all_box(self):
        t = load(25293)
        self.assertEqual(t.draw_mode, "all")
        self.assertEqual([r.sub_sn for r in t.rows], [25276, 25277, 25283])

    def test_key_box_rows_are_items(self):
        t = load(25198)
        self.assertEqual({r.row_kind for r in t.rows}, {"item"})
        self.assertEqual(sorted(r.quantity for r in t.rows), [1, 3, 20, 800])

    def test_final_ovr_rowspan_groups(self):
        t = ii.classify(ii.parse_page(1, FINAL_OVR), ii.TitleIndex([]))
        self.assertEqual(t.table_kind, "final_ovr")
        self.assertEqual(
            [(r.season_code, r.player_name, r.strong) for r in t.rows],
            [("DCB", "크리스티아누 호날두", 8), ("DCB", "T. 알렉산더-아놀드", 7), ("25 UCL", "에덴 아자르", 10)],
        )
        self.assertEqual(t.period, "상시 운영")

    def test_content_hash_is_stable_and_sensitive(self):
        a, b = load(25099), load(25099)
        self.assertEqual(a.content_hash, b.content_hash)
        b.rows[0].raw_probability = "0.15%"
        self.assertNotEqual(a.content_hash, b.content_hash)


class HelpersTest(unittest.TestCase):
    def test_pool_spec_variants(self):
        self.assertEqual(ii.pool_spec("UC Top Price 42 (8~9강, 117+)", ["UC"], 42), ii.PoolSpec(["UC"], [8, 9], 117, 42))
        self.assertEqual(ii.pool_spec("UC Top Price 105 (10강)", ["UC"], 105), ii.PoolSpec(["UC"], [10], None, 105))
        spec = ii.pool_spec("[동일확률] LIVE Top Price 17000 (1강)", ["LIVE"], 17000)
        self.assertEqual((spec.strongs, spec.min_ovr), ([1], None))
        self.assertEqual(ii.pool_spec("[AG] 행운의 BP 카드", [], 3), None)

    def test_codes(self):
        self.assertEqual(ii.norm_code("25 UCL"), ii.norm_code("25UCL"))
        self.assertEqual(ii.norm_code("NO7"), ii.norm_code("NO.7"))

    def test_index(self):
        page = '<a data-x="1" href="/probability/fco?sn=25343">[9개 9%] FC PRO 트리플 반반 상자</a>' \
               '<a href="/probability/fco?sn=25343">dup</a>'
        self.assertEqual(ii.parse_index(page), [(25343, "[9개 9%] FC PRO 트리플 반반 상자")])


if __name__ == "__main__":
    unittest.main()
