#!/usr/bin/env python3
from __future__ import annotations

import importlib
import json
import shutil
import subprocess


COMMANDS = (
    'apply_patch','chromium','cmake','convert','curl','dot','drawio','dvisvgm','ffmpeg','ffprobe',
    'file','g++','gcc','git','identify','jq','latex','libreoffice','make','manim','node','npm',
    'pandoc','pdftoppm','pkg-config','playwright','python','qpdf','rg','sandbox-browser-render',
    'sandbox-browser-screenshot','sandbox-transcribe','tesseract','unzip','xz','zip',
)
MODULES = {
    'anthropic':'anthropic','av':'av','beautifulsoup4':'bs4','cairosvg':'cairosvg',
    'ctranslate2':'ctranslate2','opencv-python-headless':'cv2','faster-whisper':'faster_whisper',
    'ffmpeg-python':'ffmpeg','ImageHash':'imagehash','imageio':'imageio','librosa':'librosa',
    'manim':'manim','moviepy':'moviepy','mutagen':'mutagen','numpy':'numpy','onnxruntime':'onnxruntime',
    'openai':'openai','openpyxl':'openpyxl','pandas':'pandas','Pillow':'PIL','playwright':'playwright',
    'PyMuPDF':'pymupdf','pypdf':'pypdf','rembg':'rembg','reportlab':'reportlab','scipy':'scipy',
    'Shapely':'shapely','scikit-image':'skimage','scikit-learn':'sklearn','SoundFile':'soundfile',
    'SVGWrite':'svgwrite','trimesh':'trimesh',
}
PACKAGES = (
    'fonts-liberation2','fonts-noto-color-emoji','fonts-wqy-zenhei','libegl-mesa0','libegl1',
    'libgles2','lmodern','mesa-utils','xauth',
)
FONTS = {
    'emoji':('Noto Color Emoji','Noto Color Emoji'),
    'latin':('Liberation Sans','Liberation Sans'),
    'zh-cn':(':lang=zh-cn','WenQuanYi Zen Hei'),
}


def main() -> None:
    failures=[]
    for command in COMMANDS:
        if not shutil.which(command):failures.append('command:'+command)
    for package,module in MODULES.items():
        try:importlib.import_module(module)
        except Exception as exc:failures.append('module:'+package+':'+type(exc).__name__)
    for package in PACKAGES:
        result=subprocess.run(['dpkg-query','-W','-f=${db:Status-Status}',package],
            capture_output=True,text=True)
        if result.returncode or result.stdout.strip()!='installed':failures.append('package:'+package)
    for language,(query,family) in FONTS.items():
        result=subprocess.run(['fc-match','-f=%{family}',query],capture_output=True,text=True)
        if result.returncode or family.lower() not in result.stdout.lower():
            failures.append('font:'+language+':'+family)
    value={'commands':len(COMMANDS),'modules':len(MODULES),'packages':len(PACKAGES),
        'fonts':len(FONTS),'failures':failures}
    print(json.dumps(value,sort_keys=True))
    if failures:raise SystemExit('original image capability parity failed: '+','.join(failures))


if __name__=='__main__':main()
