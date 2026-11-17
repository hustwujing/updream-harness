"""Read structured telemetry without database or provider credentials."""
import argparse
from datetime import datetime,timezone
import hashlib
import json
import os
from pathlib import Path


def query(root,filters,*,since=None,limit=10000,related=False):
    archive=[];seen=set();malformed=0;scan_limit=max(10000,min(200000,limit*20))
    for path in sorted(Path(root).glob("*.telemetry.jsonl")):
        try:
            with path.open() as stream:
                for line_number,line in enumerate(stream,1):
                    try:row=json.loads(line)
                    except (ValueError,UnicodeError):malformed+=1;continue
                    if not isinstance(row,dict):malformed+=1;continue
                    attrs=row.get("attributes",{})
                    if not isinstance(attrs,dict):malformed+=1;continue
                    if since and row.get("timestamp","")<since:continue
                    fallback=hashlib.sha256((str(path)+":"+str(line_number)+":"+line).encode()).hexdigest()
                    identity=(("span",row.get("trace_id"),row.get("span_id"))
                        if row.get("signal")=="span" and row.get("trace_id") and row.get("span_id")
                        else (row.get("signal") or "log",attrs.get("updream.source_event_id")
                            or row.get("event_id") or fallback))
                    if identity in seen:continue
                    seen.add(identity);archive.append(row)
                    if len(archive)>scan_limit*2:
                        archive=sorted(archive,key=lambda value:value.get("timestamp",""))[-scan_limit:]
        except (FileNotFoundError,PermissionError):continue
    def matches(row):
        attrs=row.get("attributes",{})
        return not any((row.get(key) if key in {"trace_id","span_id"} else
            attrs.get("updream."+key))!=value for key,value in filters.items())
    rows=[row for row in archive if matches(row)]
    if related:
        turns={row.get("attributes",{}).get("updream.turn_id") for row in rows}-{None,""}
        rows=[row for row in archive if matches(row) or
            row.get("attributes",{}).get("updream.turn_id") in turns]
    return sorted(rows,key=lambda value:value.get("timestamp",""))[-limit:],malformed


def main():
    parser=argparse.ArgumentParser(description="updream-harness correlated log timeline")
    parser.add_argument("--log-dir",default=str(Path(os.getenv("SANDBOX_HARNESS_DATA_ROOT",".data"))/"logs"))
    for key in ("thread","turn","trace","request","sandbox"):parser.add_argument("--"+key)
    parser.add_argument("--since",help="UTC ISO-8601 timestamp")
    parser.add_argument("--limit",type=int,default=1000)
    parser.add_argument("--json",action="store_true")
    parser.add_argument("--related",action="store_true",help="Include related Turns across traces (automatic for --request)")
    args=parser.parse_args()
    filters={key+"_id":getattr(args,key) for key in ("thread","turn","trace","request","sandbox")
        if getattr(args,key)}
    if not filters:parser.error("Supply at least one correlation ID")
    if args.since:
        try:
            parsed=datetime.fromisoformat(args.since)
            args.since=parsed.replace(tzinfo=parsed.tzinfo or timezone.utc).astimezone(timezone.utc).isoformat()
        except ValueError:parser.error("Invalid --since timestamp")
    rows,malformed=query(args.log_dir,filters,since=args.since,
        limit=max(1,min(args.limit,10000)),related=args.related or bool(args.request))
    if args.json:
        print(json.dumps({"records":rows,"malformed_lines":malformed},ensure_ascii=False))
    else:
        for row in rows:
            attrs=row.get("attributes",{})
            print(" | ".join(str(x) for x in (row.get("timestamp",""),
                row.get("resource",{}).get("service.name",""),
                row.get("severity_text",row.get("status","")),row.get("event_name",""),
                attrs.get("updream.duration_ms",row.get("duration_ms","")),
                attrs.get("updream.state",attrs.get("updream.outcome","")),
                row.get("trace_id",""),row.get("span_id",""))))
        print(f"{len(rows)} records; {malformed} malformed lines skipped")


if __name__=="__main__":main()
