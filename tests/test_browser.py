# -*- coding: utf-8 -*-
"""브라우저로 받아 오는 비상 경로(iteminfo_browser.py 스크립트 · iteminfo_job 묶음 읽기).

    python -m unittest discover -s tests
"""
import json
import pathlib
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

import iteminfo as ii  # noqa: E402
import iteminfo_browser as browser  # noqa: E402
import iteminfo_job as job  # noqa: E402

FIX = pathlib.Path(__file__).resolve().parent / "fixtures" / "iteminfo"
INDEX = ('<a href="/probability/fco?sn=25232">행운의 BP 카드 (350만~700만)</a>'
         '<a href="/probability/fco?sn=25099">옛 표</a>')


class SnippetTest(unittest.TestCase):
    def test_known_and_also_are_filled_in(self):
        js = browser.build_snippet([3, 1, 2, 2], also=[2])
        self.assertIn("new Set([1, 2, 3])", js)
        self.assertIn("const ALSO = [2];", js)
        self.assertNotIn("__KNOWN__", js)
        self.assertNotIn("__ALSO__", js)

    def test_runs_only_on_iteminfo_and_waits_between_tables(self):
        js = browser.build_snippet([])
        self.assertIn("location.host !== 'iteminfo.nexon.com'", js)
        self.assertIn("GAP_MS = [3000, 5000]", js)
        self.assertIn("res.status !== 429", js)


class BundleTest(unittest.TestCase):
    def write(self, data):
        f = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8")
        json.dump(data, f, ensure_ascii=False)
        f.close()
        self.addCleanup(pathlib.Path(f.name).unlink)
        return f.name

    def test_reads_index_pages_and_failed(self):
        page = (FIX / "s25232.html").read_text(encoding="utf-8")
        path = self.write({"version": 1, "index": INDEX, "pages": {"25232": page}, "failed": [25300]})
        index, pages, failed = job.read_local(path, 0)
        self.assertEqual([sn for sn, _ in index], [25232, 25099])
        self.assertEqual(list(pages), [25232])
        self.assertEqual(failed, [25300])
        self.assertTrue(ii.parse_page(25232, pages[25232]).title)  # 저장한 HTML 을 그대로 해석한다

    def test_rejects_other_json(self):
        with self.assertRaises(SystemExit):
            job.read_local(self.write({"hello": 1}), 0)

    def test_folder_mode_counts_only_saved_pages(self):
        with tempfile.TemporaryDirectory() as d:
            root = pathlib.Path(d)
            (root / "index.html").write_text(INDEX, encoding="utf-8")
            (root / "s25232.html").write_text((FIX / "s25232.html").read_text(encoding="utf-8"), encoding="utf-8")
            index, pages, failed = job.read_local(d, 0)
        self.assertEqual(len(index), 2)
        self.assertEqual(list(pages), [25232])
        self.assertEqual(failed, [])  # 폴더에 없는 25099 는 실패가 아니다


if __name__ == "__main__":
    unittest.main()
