#!/usr/bin/env python3
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime,timezone
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.request
import uuid


ROOT=Path(__file__).resolve().parents[1]
DATA=ROOT/".data"
BROWSER_SCRIPTS=sorted((ROOT/"tests").glob("browser_*.cjs"))
LIVE_BROWSER={"browser_live_background.cjs","browser_live_interaction.cjs","browser_project_files.cjs"}


def run_stage(name,argv,receipt,*,env=None,cwd=ROOT):
    started=time.monotonic();print(json.dumps({"gate_stage":name,"state":"started"}),flush=True)
    result=subprocess.run(argv,cwd=cwd,env=env,check=False)
    row={"name":name,"command":argv,"exit_code":result.returncode,
        "elapsed_seconds":round(time.monotonic()-started,3)}
    receipt["stages"].append(row);print(json.dumps({"gate_stage":name,"state":"passed" if not result.returncode else "failed",
        "elapsed_seconds":row["elapsed_seconds"]}),flush=True)
    if result.returncode:raise subprocess.CalledProcessError(result.returncode,argv)


def port_free(port):
    with socket.socket() as probe:
        probe.settimeout(.2)
        return probe.connect_ex(("127.0.0.1",port))!=0


@contextmanager
def postgres():
    root=DATA/"gate-postgresql";data=root/"data";run=root/"run";log=root/"postgresql.log"
    database="gate_"+uuid.uuid4().hex
    root.mkdir(parents=True,exist_ok=True);run.mkdir(exist_ok=True)
    database_user="sandbox-harness-db";uid=int(subprocess.check_output(["id","-u",database_user],text=True))
    gid=int(subprocess.check_output(["id","-g",database_user],text=True))
    for path in (root,run):os.chown(path,uid,gid);path.chmod(0o700)
    if not (data/"PG_VERSION").is_file():
        subprocess.run(["runuser","-u",database_user,"--","/usr/lib/postgresql/16/bin/initdb",
            "-D",str(data),"--auth-local=trust","--auth-host=trust","--encoding=UTF8","--locale=C.UTF-8"],check=True)
    if not port_free(28433):raise RuntimeError("gate_postgres_port_in_use")
    options=f"-h 127.0.0.1 -p 28433 -k {run}"
    subprocess.run(["runuser","-u",database_user,"--","/usr/lib/postgresql/16/bin/pg_ctl",
        "-D",str(data),"-l",str(log),"-o",options,"-w","start"],check=True)
    created=False
    try:
        subprocess.run(["runuser","-u",database_user,"--","/usr/lib/postgresql/16/bin/createdb",
            "-h","127.0.0.1","-p","28433",database],check=True)
        created=True
        yield f"postgresql+asyncpg://sandbox-harness-db@127.0.0.1:28433/{database}"
    finally:
        if created:
            subprocess.run(["runuser","-u",database_user,"--","/usr/lib/postgresql/16/bin/dropdb",
                "-h","127.0.0.1","-p","28433","--force",database],check=False)
        subprocess.run(["runuser","-u",database_user,"--","/usr/lib/postgresql/16/bin/pg_ctl",
            "-D",str(data),"-m","fast","-w","stop"],check=False)


@contextmanager
def browser_server(port:int):
    if not port_free(port):raise RuntimeError("gate_browser_port_in_use")
    root=DATA/"gate-browser"/uuid.uuid4().hex;root.mkdir(parents=True)
    environment={**os.environ,"SANDBOX_HARNESS_BACKEND":"fake","SANDBOX_HARNESS_DATA_ROOT":str(root),
        "SANDBOX_HARNESS_DATABASE_URL":"sqlite+aiosqlite:///"+str(root/"platform.db"),
        "SANDBOX_HARNESS_REDIS_URL":"","SANDBOX_HARNESS_OBJECT_BACKEND":"local",
        "SANDBOX_HARNESS_MODEL_CATALOG_DISCOVERY":"false","SANDBOX_HARNESS_API_BIND_PORT":str(port)}
    log=(root/"api.log").open("wb")
    process=subprocess.Popen([str(ROOT/".venv/bin/sandbox-harness-parallel-api")],cwd=ROOT,env=environment,
        stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
    try:
        deadline=time.monotonic()+float(os.environ.get("GATE_BROWSER_STARTUP_TIMEOUT_SECONDS","60"))
        while time.monotonic()<deadline:
            if process.poll() is not None:raise RuntimeError("gate_browser_server_exited")
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/health",timeout=.2) as response:
                    if response.status==200:break
            except Exception:time.sleep(.05)
        else:raise TimeoutError("gate_browser_server_not_ready")
        yield environment
    finally:
        if process.poll() is None:
            os.killpg(process.pid,signal.SIGTERM)
            try:process.wait(15)
            except subprocess.TimeoutExpired:os.killpg(process.pid,signal.SIGKILL);process.wait()
        log.close()


def main():
    parser=argparse.ArgumentParser();parser.add_argument("--skip-browser",action="store_true")
    parser.add_argument("--skip-python",action="store_true",
        help="Resume non-Python stages after a separately evidenced focused rerun")
    parser.add_argument("--include-live-browser",action="store_true",
        help="Run deployed cases requiring UI_SESSION_TOKEN and a prepared real Thread")
    parser.add_argument("--browser-script",action="append",default=[],
        help="Run only named browser script stems for a focused rerun")
    parser.add_argument("--browser-only",action="store_true",
        help="Run focused browser scripts after separately evidenced build stages")
    parser.add_argument("--allow-dirty",action="store_true");args=parser.parse_args()
    if not args.allow_dirty:
        subprocess.run(["git","diff","--quiet"],cwd=ROOT,check=True)
        subprocess.run(["git","diff","--cached","--quiet"],cwd=ROOT,check=True)
    revision=subprocess.check_output(["git","rev-parse","HEAD"],cwd=ROOT,text=True).strip()
    started=datetime.now(timezone.utc);receipt={"revision":revision,"started_at":started.isoformat(),"stages":[]}
    target=DATA/"evidence/gate"/(started.strftime("%Y%m%dT%H%M%SZ")+".json");target.parent.mkdir(parents=True,exist_ok=True)
    browser_cache=(DATA/"playwright-browsers" if (DATA/"playwright-browsers").is_dir() else
        Path("/data2/wujing03/sandbox-harness-platform/.data/playwright-browsers"))
    if not browser_cache.is_dir():raise FileNotFoundError("PLAYWRIGHT_BROWSERS_PATH under /data2 is unavailable")
    environment={**os.environ,"UV_CACHE_DIR":str(DATA/"uv-cache"),"npm_config_cache":str(DATA/"npm-cache"),
        "PLAYWRIGHT_BROWSERS_PATH":str(browser_cache)}
    browser_port=int(os.environ.get("GATE_BROWSER_PORT","24767"))
    if not 1024<=browser_port<=65535:raise ValueError("invalid_gate_browser_port")
    try:
        if not args.browser_only:
            run_stage("python_compile",[sys.executable,"-m","compileall","-q","src","runtime","deploy","scripts","tests"],receipt,env=environment)
            run_stage("skill_bundle",[sys.executable,"runtime/skill_bundle.py","verify"],receipt,env=environment)
            shell_files=[str(path.relative_to(ROOT)) for path in sorted((ROOT/"deploy").glob("*.sh"))]
            run_stage("shell_syntax",["bash","-n",*shell_files],receipt,env=environment)
            if not args.skip_python:
                with postgres() as url:
                    test_env={**environment,"PARALLEL_TEST_POSTGRES_URL":url}
                    run_stage("python_tests",[str(ROOT/".venv/bin/pytest"),"-q"],receipt,env=test_env)
            run_stage("frontend_tests",["npm","--prefix","web","test"],receipt,env=environment)
            run_stage("frontend_build",["npm","--prefix","web","run","build"],receipt,env=environment)
            run_stage("systemd_verify",["systemd-analyze","verify",*[str(path) for path in sorted(
                (ROOT/"deploy/systemd").glob("sandbox-harness-parallel-*.service"))]],receipt,env=environment)
        if not args.skip_browser:
            with browser_server(browser_port) as browser_env:
                browser_env={**browser_env,**environment,"NODE_PATH":str(ROOT/"web/node_modules"),
                    "TEST_BASE_URL":f"http://127.0.0.1:{browser_port}/"}
                scripts=[script for script in BROWSER_SCRIPTS
                    if args.include_live_browser or script.name not in LIVE_BROWSER]
                if args.browser_script:
                    requested=set(args.browser_script);scripts=[script for script in scripts if script.stem in requested]
                    if {script.stem for script in scripts}!=requested:raise ValueError("unknown_browser_script")
                for skipped in sorted(LIVE_BROWSER) if not args.include_live_browser else ():
                    receipt["stages"].append({"name":"browser:"+Path(skipped).stem,"state":"deferred_live"})
                for script in scripts:
                    run_stage("browser:"+script.stem,["node",str(script)],receipt,env=browser_env)
        receipt["status"]="passed"
    except BaseException as exc:
        receipt["status"]="failed";receipt["error_type"]=type(exc).__name__;raise
    finally:
        receipt["finished_at"]=datetime.now(timezone.utc).isoformat()
        receipt["elapsed_seconds"]=round((datetime.now(timezone.utc)-started).total_seconds(),3)
        target.write_text(json.dumps(receipt,ensure_ascii=False,indent=2)+"\n")
        latest=target.parent/"latest.json";shutil.copyfile(target,latest)
        print(json.dumps({"gate_receipt":str(target),"status":receipt.get("status"),
            "elapsed_seconds":receipt["elapsed_seconds"]}),flush=True)


if __name__=="__main__":main()
