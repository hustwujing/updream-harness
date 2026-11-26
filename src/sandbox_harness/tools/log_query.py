"""Read structured telemetry without database or provider credentials."""
import argparse
from datetime import datetime,timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
from tempfile import NamedTemporaryFile


def query(root,filters,*,since=None,limit=10000,related=False):
    root=Path(root);root.parent.mkdir(parents=True,exist_ok=True);malformed=0
    with NamedTemporaryFile(dir=root.parent,prefix=".telemetry-query-",suffix=".sqlite",delete=False) as tmp:
        index_path=Path(tmp.name)
    try:
        db=sqlite3.connect(index_path)
        db.execute("CREATE TABLE records (identity TEXT PRIMARY KEY, timestamp TEXT, thread_id TEXT, "
            "turn_id TEXT, request_id TEXT, sandbox_id TEXT, job_id TEXT, execution_id TEXT, "
            "allocation_id TEXT, provider_job_id TEXT, trace_id TEXT, span_id TEXT, payload TEXT)")
        for path in sorted(root.glob("*.telemetry.jsonl")):
            try:
                with path.open() as stream:
                    for line_number,line in enumerate(stream,1):
                        try:row=json.loads(line)
                        except (ValueError,UnicodeError):malformed+=1;continue
                        if not isinstance(row,dict):malformed+=1;continue
                        attrs=row.get("attributes",{})
                        if not isinstance(attrs,dict):malformed+=1;continue
                        timestamp=row.get("timestamp","")
                        if since and timestamp<since:continue
                        fallback=hashlib.sha256((str(path)+":"+str(line_number)+":"+line).encode()).hexdigest()
                        identity=(("span:"+str(row.get("trace_id"))+":"+str(row.get("span_id")))
                            if row.get("signal")=="span" and row.get("trace_id") and row.get("span_id")
                            else str(row.get("signal") or "log")+":"+str(
                                attrs.get("updream.source_event_id") or row.get("event_id") or fallback))
                        db.execute("INSERT OR IGNORE INTO records VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",(
                            identity,timestamp,attrs.get("updream.thread_id"),attrs.get("updream.turn_id"),
                            attrs.get("updream.request_id"),attrs.get("updream.sandbox_id"),
                            attrs.get("updream.job_id"),attrs.get("updream.execution_id"),
                            attrs.get("updream.allocation_id"),attrs.get("updream.provider_job_id"),
                            row.get("trace_id"),row.get("span_id"),json.dumps(row,ensure_ascii=False)))
            except (FileNotFoundError,PermissionError):continue
        db.commit();columns={"thread_id":"thread_id","turn_id":"turn_id","request_id":"request_id",
            "sandbox_id":"sandbox_id","job_id":"job_id","execution_id":"execution_id",
            "allocation_id":"allocation_id","provider_job_id":"provider_job_id",
            "trace_id":"trace_id","span_id":"span_id"}
        clauses=[];parameters=[]
        for key,value in filters.items():clauses.append(columns[key]+"=?");parameters.append(value)
        direct=" AND ".join(clauses) or "1=1";where=direct;query_parameters=list(parameters)
        if related:
            turns=[row[0] for row in db.execute("SELECT DISTINCT turn_id FROM records WHERE "+direct+
                " AND turn_id IS NOT NULL AND turn_id!='' LIMIT 100",parameters)]
            if turns:
                where="("+direct+") OR turn_id IN ("+",".join("?" for _ in turns)+")"
                query_parameters.extend(turns)
        encoded=[row[0] for row in db.execute("SELECT payload FROM records WHERE "+where+
            " ORDER BY timestamp DESC, identity DESC LIMIT ?",query_parameters+[limit])]
        return [json.loads(value) for value in reversed(encoded)],malformed
    finally:
        try:db.close()
        except Exception:pass
        index_path.unlink(missing_ok=True)


def main():
    parser=argparse.ArgumentParser(description="updream-harness correlated log timeline")
    parser.add_argument("--log-dir",default=str(Path(os.getenv("SANDBOX_HARNESS_DATA_ROOT",".data"))/"logs"))
    for key in ("thread","turn","trace","request","sandbox","job","execution","allocation","provider-job"):
        parser.add_argument("--"+key)
    parser.add_argument("--since",help="UTC ISO-8601 timestamp")
    parser.add_argument("--limit",type=int,default=1000)
    parser.add_argument("--json",action="store_true")
    parser.add_argument("--related",action="store_true",help="Include related Turns across traces (automatic for --request)")
    args=parser.parse_args()
    filters={key.replace('-','_')+"_id":getattr(args,key.replace('-','_'))
        for key in ("thread","turn","trace","request","sandbox","job","execution","allocation","provider-job")
        if getattr(args,key.replace('-','_'))}
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
