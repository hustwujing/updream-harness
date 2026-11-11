#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
from zipfile import ZipFile

from PIL import Image
from openpyxl import Workbook
from reportlab.pdfgen import canvas

from sandbox_runtime.skills import materialize_skills


ROOT = Path("/opt/platform/runtime/skills")
WORK = Path("/workspace/tmp/skill-smoke")


def run(*command: str, cwd: Path | None = None) -> str:
    value = subprocess.run(command, cwd=cwd, check=True, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=180)
    return value.stdout


def main() -> None:
    if os.getuid() == 0:
        raise SystemExit("skill smoke must run as Runtime UID 10001")
    shutil.rmtree(WORK, ignore_errors=True);WORK.mkdir(parents=True)
    results = {"harnesses": {}, "artifacts": {}}
    for harness in ("codex", "claude", "deepseek"):
        installed = materialize_skills(harness, Path("/workspace"))
        root = Path(installed["root"])
        assert len(list(root.glob("*/SKILL.md"))) == 25
        results["harnesses"][harness] = installed["skill_count"]

    graph = WORK / "graph.json"
    graph.write_text(json.dumps({"direction":"LR","nodes":[{"id":"api","label":"API"},
        {"id":"worker","label":"Worker"}],"edges":[{"source":"api","target":"worker","label":"job"}]}))
    drawio = WORK / "architecture.drawio";drawio_png = WORK / "architecture.png"
    run("python3",str(ROOT/"drawio-skill/scripts/autolayout.py"),str(graph),"-o",str(drawio))
    run("python3",str(ROOT/"drawio-skill/scripts/validate.py"),str(drawio),"--score")
    run("drawio","-x","-f","png","--width","800","-o",str(drawio_png),str(drawio))
    with Image.open(drawio_png) as image:assert image.width>100 and image.height>50
    results["artifacts"]["drawio_png"] = drawio_png.stat().st_size

    diagram_html = ROOT / "diagram-design/assets/example-flowchart.html"
    diagram_png = WORK / "diagram-design.png"
    run("python3",str(ROOT/"diagram-design/scripts/self_check.py"),str(diagram_html))
    run("python3",str(ROOT/"diagram-design/scripts/export_png.py"),str(diagram_html),str(diagram_png),"1")
    with Image.open(diagram_png) as image:assert image.width>=900 and image.height>=600
    results["artifacts"]["diagram_design_png"] = diagram_png.stat().st_size

    anatomy = WORK / "anatomy";shutil.copytree(ROOT/"anatomy",anatomy)
    run("chmod", "-R", "u+w", str(anatomy))
    run("node","examples/dial-indicator/build.mjs",cwd=anatomy)
    anatomy_html = anatomy / "examples/dial-indicator/dial-indicator.html"
    anatomy_png = WORK / "anatomy.png"
    run("node",str(anatomy/"scripts/capture.mjs"),str(anatomy_html),"--out",str(anatomy_png),"--width","390",cwd=anatomy)
    with Image.open(anatomy_png) as image:assert image.width>=390 and image.height>300
    results["artifacts"]["anatomy_png"] = anatomy_png.stat().st_size

    docx = WORK / "document.docx"
    node_docx = WORK / "document.cjs"
    node_docx.write_text("""const fs=require('fs');const {Document,Packer,Paragraph}=require('docx');
const d=new Document({sections:[{children:[new Paragraph('updream-harness skill smoke')]}]});
Packer.toBuffer(d).then(b=>fs.writeFileSync(process.argv[2],b));\n""")
    run("node",str(node_docx),str(docx))
    run("python3",str(ROOT/"docx/scripts/office/soffice.py"),"--headless","--convert-to","pdf","--outdir",str(WORK),str(docx))
    assert (WORK/"document.pdf").is_file()
    results["artifacts"]["docx"] = docx.stat().st_size

    deck_dir=WORK/"deck"/"SmokeDeck"/"origin_image";deck_dir.mkdir(parents=True)
    Image.new("RGB",(1280,720),(35,91,78)).save(deck_dir/"slide_01.png")
    run("python3",str(ROOT/"codex-ppt/scripts/assemble_ppt.py"),str(WORK/"deck"),"SmokeDeck.pptx")
    pptx=WORK/"deck"/"SmokeDeck"/"SmokeDeck.pptx"
    with ZipFile(pptx) as archive:assert "ppt/slides/slide1.xml" in archive.namelist()
    run("python3",str(ROOT/"pptx/scripts/office/soffice.py"),"--headless","--convert-to","pdf","--outdir",str(WORK),str(pptx))
    assert (WORK/"SmokeDeck.pdf").is_file()
    results["artifacts"]["pptx"] = pptx.stat().st_size

    workbook=Workbook();sheet=workbook.active;sheet["A1"]=2;sheet["A2"]=3;sheet["A3"]="=SUM(A1:A2)"
    xlsx=WORK/"formula.xlsx";workbook.save(xlsx)
    recalc=json.loads(run("python3",str(ROOT/"xlsx/scripts/recalc.py"),str(xlsx)))
    assert recalc["status"]=="success" and recalc["total_errors"]==0
    results["artifacts"]["xlsx"] = xlsx.stat().st_size

    pdf=WORK/"report.pdf";writer=canvas.Canvas(str(pdf));writer.drawString(72,720,"updream-harness skill smoke");writer.save()
    run("qpdf","--check",str(pdf));run("pdftoppm","-png","-singlefile",str(pdf),str(WORK/"report"))
    assert (WORK/"report.png").is_file()
    results["artifacts"]["pdf"] = pdf.stat().st_size

    source=WORK/"source.png";Image.new("RGB",(640,360),(245,245,240)).save(source)
    doctor=json.loads(run("editppt","doctor","--json"))
    assert all(doctor["dependencies"].values())
    edit_run=WORK/"editppt-run"
    run("editppt","prepare",str(source),"--job-dir",str(edit_run),"--no-text-hints")
    assert (edit_run/"deck_manifest.json").is_file() and (edit_run/"pages/page_001/source.png").is_file()
    results["artifacts"]["editppt_manifest"] = (edit_run/"deck_manifest.json").stat().st_size

    print(json.dumps({"passed":True,**results},sort_keys=True))


if __name__ == "__main__":
    main()
