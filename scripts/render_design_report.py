#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import re


ROOT=Path(__file__).resolve().parents[1]


def main():
    parser=argparse.ArgumentParser();parser.add_argument("--trace",type=Path,
        default=ROOT/"docs/design-traceability.yaml");parser.add_argument("--output",type=Path,
        default=ROOT/"docs/reports/initial-implementation-report.md");args=parser.parse_args()
    value=json.loads(args.trace.read_text());spec=ROOT/value["spec"];source=spec.read_text()
    sections=re.findall(r"^## (.+)$",source,re.M);rows=value["requirements"]
    mapped=[row["section"] for row in rows]
    if len(mapped)!=len(set(mapped)):raise ValueError("Duplicate traceability section")
    if set(mapped)!=set(sections):
        raise ValueError("Unmapped design sections: "+repr(sorted(set(sections)-set(mapped))))
    for row in rows:
        if not row.get("implementation") or not row.get("tests"):raise ValueError("Empty mapping: "+row["section"])
        for relative in (*row["implementation"],*row["tests"]):
            path=ROOT/relative.split("#",1)[0]
            if not path.exists():raise FileNotFoundError(str(path))
    lines=["# Initial Design Implementation Report","",f"Spec: `{value['spec']}`","",
        f"Mapped sections: **{len(rows)}/{len(sections)}**","",
        "| Design section | Implementation evidence | Test evidence | Current evidence |","| --- | --- | --- | --- |"]
    for row in rows:
        lines.append("| "+row["section"]+" | "+"<br>".join(f"`{item}`" for item in row["implementation"])
            +" | "+"<br>".join(f"`{item}`" for item in row["tests"])+" | "+row["evidence"]+" |")
    lines.extend(["","Pending live evidence is explicit and must be replaced by Task 15 receipts before final release.",""])
    args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text("\n".join(lines))
    print(json.dumps({"output":str(args.output),"mapped":len(rows)},sort_keys=True))


if __name__=="__main__":main()
