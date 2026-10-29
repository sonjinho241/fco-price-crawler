# -*- coding: utf-8 -*-
"""넥슨 확률표를 사람의 브라우저로 받아 오는 비상 경로.

2026-10-29 부터 넥슨(Cloudflare)이 GitHub 러너의 자동 수집을 20회 안팎에서 429 로 막는다. 사람이 보는
브라우저는 막히지 않으므로, 목요일에 새 표를 못 받았으면 이 경로로 넣는다.

1. 스크립트 만들기 — DB 에 이미 있는 sn 을 넣은 콘솔 스크립트를 만든다(받을 것 = DB 에 없는 새 표만):

       DATABASE_URL=... python iteminfo_browser.py snippet            # → iteminfo_snippet.js
       DATABASE_URL=... python iteminfo_browser.py snippet --also 25394,25395   # 이미 있는 표도 다시 받기

2. 크롬에서 https://iteminfo.nexon.com/probability/fco 를 열고 F12 → Console 에 iteminfo_snippet.js 내용을
   붙여 넣는다. 표마다 3~5초 쉬며 받고(429 면 1분 쉬고 다시), 끝나면 iteminfo-<날짜>.json 하나를 내려받는다.
   진행은 탭 제목과 콘솔에 나온다. 도중에 탭을 닫으면 처음부터 다시 한다.

3. 넣기 — 받은 파일을 크롤러에 준다(저장한 표만 비교해 바뀐 것만 쓰고 후보 풀을 다시 만든다):

       DATABASE_URL=... ITEMINFO_LOCAL_DIR=C:/Users/HP/Downloads/iteminfo-20261105.json python iteminfo_job.py

DATABASE_URL 은 자동 배치와 같은 크롤러용 주소(GitHub Secret, 세션 모드 :5432)를 쓴다. 로그·파일에 남기지 말 것.
"""
from __future__ import annotations

import argparse
import json
import pathlib
import sys
from typing import Iterable, List

from sqlalchemy import text

from crawler_job import make_engine

# 콘솔 스크립트 본문. __KNOWN__·__ALSO__ 만 바꿔 끼운다. 같은 출처(iteminfo.nexon.com)에서 fetch 하므로
# 브라우저의 쿠키·헤더 그대로 사람 한 명의 조회로 나간다. 간격은 사람이 하나씩 여는 속도에 맞춘다.
SNIPPET = r"""// FCO 매니저 확률표 받기 — iteminfo.nexon.com/probability/fco 페이지의 콘솔에서 실행한다.
(async () => {
  const KNOWN = new Set(__KNOWN__);
  const ALSO = __ALSO__;
  const GAP_MS = [3000, 5000];
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  if (location.host !== 'iteminfo.nexon.com') { console.error('iteminfo.nexon.com 확률표 페이지에서 실행해주세요.'); return; }
  async function get(url) {
    for (let i = 0; i < 4; i++) {
      const res = await fetch(url, { credentials: 'include' });
      if (res.ok) return res.text();
      if (res.status !== 429) throw new Error('HTTP ' + res.status);
      console.warn('429 — 1분 쉬고 다시 받습니다', url);
      await sleep(60000);
    }
    throw new Error('HTTP 429 가 계속됩니다');
  }
  const index = await get('/probability/fco');
  const listed = [...new Set([...index.matchAll(/href="\/probability\/fco\?sn=(\d+)"/g)].map((m) => +m[1]))];
  const want = listed.filter((sn) => !KNOWN.has(sn) || ALSO.includes(sn));
  console.log('목록 ' + listed.length + '개 중 받을 표 ' + want.length + '개', want);
  const pages = {};
  const failed = [];
  for (let i = 0; i < want.length; i++) {
    const sn = want[i];
    document.title = '받는 중 ' + (i + 1) + '/' + want.length;
    if (i) await sleep(GAP_MS[0] + Math.random() * (GAP_MS[1] - GAP_MS[0]));
    try { pages[sn] = await get('/probability/fco?sn=' + sn); console.log('받음', sn); }
    catch (e) { failed.push(sn); console.warn('못 받음', sn, e.message); }
  }
  const day = new Date().toISOString().slice(0, 10).replace(/-/g, '');
  const blob = new Blob([JSON.stringify({ version: 1, saved_at: new Date().toISOString(), index, pages, failed })],
    { type: 'application/json' });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = 'iteminfo-' + day + '.json';
  document.body.appendChild(a);
  a.click();
  a.remove();
  document.title = '끝 ' + Object.keys(pages).length + '/' + want.length;
  console.log('끝: 받음 ' + Object.keys(pages).length + ' · 못 받음 ' + failed.length, failed);
})();
"""


def build_snippet(known: Iterable[int], also: Iterable[int] = ()) -> str:
    return (SNIPPET
            .replace("__KNOWN__", json.dumps(sorted({int(s) for s in known})))
            .replace("__ALSO__", json.dumps(sorted({int(s) for s in also}))))


def known_sns(engine) -> List[int]:
    with engine.connect() as conn:
        return [int(sn) for (sn,) in conn.execute(text("SELECT sn FROM spend.prob_table"))]


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)
    snip = sub.add_parser("snippet", help="브라우저 콘솔 스크립트를 만든다")
    snip.add_argument("--also", default="", help="이미 있어도 다시 받을 sn(쉼표로)")
    snip.add_argument("--out", default="iteminfo_snippet.js")
    args = parser.parse_args(argv)

    also = [int(s) for s in args.also.split(",") if s.strip()]
    known = known_sns(make_engine())
    pathlib.Path(args.out).write_text(build_snippet(known, also), encoding="utf-8")
    print(f"{args.out}: DB 에 있는 표 {len(known)}개는 건너뛰고 새 표만 받습니다"
          + (f"(다시 받기 {len(also)}개)" if also else "") + ".")


if __name__ == "__main__":
    sys.exit(main())
