"""Read structured telemetry without database or provider credentials."""
import argparse
from datetime import datetime,timezone
import json
import os
from pathlib import Path


def query(root, filters, *, since=None, limit=10000):
    rows=[];seen=set();malformed=0
    for path in sorted(Path(root).glob('*.telemetry.jsonl')):
        try:
            with path.open() as stream:
                for line in stream:
                    try:row=json.loads(line)
                    except (ValueError,UnicodeError):malformed+=1;continue
                    if not isinstance(row,dict):malformed+=1;continue
                    attrs=row.get('attributes',{})
                    if not isinstance(attrs,dict):malformed+=1;continue
                    if any((row.get(key) if key in {'trace_id','span_id'} else attrs.get('updream.'+key))!=value for key,value in filters.items()):continue
                    if since and row.get('timestamp','')<since:continue
                    identity=('span',row.get('trace_id'),row.get('span_id')) if row.get('signal')=='span' else ('log',attrs.get('updream.source_event_id') or row.get('event_id'))
                    if identity[-1] and identity in seen:continue
                    seen.add(identity);rows.append(row)
                    if len(rows)>limit*2:
                        rows=sorted(rows,key=lambda value:value.get('timestamp',''))[-limit:]
        except (FileNotFoundError,PermissionError):continue
    return sorted(rows,key=lambda value:value.get('timestamp',''))[-limit:],malformed


def main():
    parser=argparse.ArgumentParser(description='updream-harness correlated log timeline')
    parser.add_argument('--log-dir',default=str(Path(os.getenv('SANDBOX_HARNESS_DATA_ROOT','.data'))/'logs'))
    for key in ('thread','turn','trace','request','sandbox'):parser.add_argument('--'+key)
    parser.add_argument('--since',help='UTC ISO-8601 timestamp')
    parser.add_argument('--limit',type=int,default=1000)
    parser.add_argument('--json',action='store_true')
    parser.add_argument('--related',action='store_true',help='Include related Turns across traces (automatic for --request)')
    args=parser.parse_args()
    filters={key+'_id':getattr(args,key) for key in ('thread','turn','trace','request','sandbox') if getattr(args,key)}
    if not filters:parser.error('Supply at least one correlation ID')
    if args.since:
        try:
            parsed=datetime.fromisoformat(args.since)
            args.since=parsed.replace(tzinfo=parsed.tzinfo or timezone.utc).astimezone(timezone.utc).isoformat()
        except ValueError:parser.error('Invalid --since timestamp')
    rows,malformed=query(args.log_dir,filters,since=args.since,limit=max(1,min(args.limit,10000)))
    if args.related or args.request:
        turns={row.get('attributes',{}).get('updream.turn_id') for row in rows}-{None,''}
        expanded=list(rows)
        for turn_id in sorted(turns)[:100]:
            related,_=query(args.log_dir,{'turn_id':turn_id},since=args.since,limit=max(1,min(args.limit,10000)))
            expanded.extend(related)
        unique={}
        for row in expanded:
            attrs=row.get('attributes',{})
            key=('span',row.get('trace_id'),row.get('span_id')) if row.get('signal')=='span' else ('log',attrs.get('updream.source_event_id') or row.get('event_id'))
            unique[key]=row
        rows=sorted(unique.values(),key=lambda row:row.get('timestamp',''))[-max(1,min(args.limit,10000)):]
    if args.json:
        print(json.dumps({'records':rows,'malformed_lines':malformed},ensure_ascii=False))
    else:
        for row in rows:
            attrs=row.get('attributes',{})
            print(' | '.join(str(x) for x in (row.get('timestamp',''),row.get('resource',{}).get('service.name',''),
                row.get('severity_text',row.get('status','')),row.get('event_name',''),
                attrs.get('updream.duration_ms',row.get('duration_ms','')),
                attrs.get('updream.state',attrs.get('updream.outcome','')),row.get('trace_id',''),row.get('span_id',''))))
        print(f'{len(rows)} records; {malformed} malformed lines skipped')

if __name__=='__main__':main()
