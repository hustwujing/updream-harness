from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import httpx

from .state import capability_token_file


def main(argv: list[str] | None = None) -> int:
    parser=argparse.ArgumentParser(prog="sandbox-review")
    parser.add_argument("--request",required=True,help="JSON object or path")
    args=parser.parse_args(argv);raw=args.request
    path=Path(raw);value=json.loads(path.read_text() if path.is_file() else raw)
    if not isinstance(value,dict):parser.error("Review request must be an object")
    if os.environ.get("HARNESS_CHILD_MODE")=="1":
        print(json.dumps({"error":"A Child Harness cannot create a Reviewer"}));return 2
    value.setdefault("harness",os.environ.get("HARNESS_KIND",""))
    value.setdefault("model",os.environ.get("HARNESS_MODEL",""))
    base=os.environ.get("HARNESS_GATEWAY_URL","").rstrip("/")
    token=capability_token_file()
    if not base or not token.is_file():
        print(json.dumps({"error":"Harness Gateway capability is not configured"}));return 2
    response=httpx.post(base+"/v1/reviews",headers={"Authorization":"Bearer "+token.read_text().strip()},
        json=value,timeout=30)
    print(response.text);return 0 if response.is_success else 1


if __name__=="__main__":raise SystemExit(main())
