#!/usr/bin/env python3
"""Create a bounded, deterministic montage of rendered slide images."""

from __future__ import annotations

import argparse
import math
import os
from pathlib import Path
import re
import tempfile
import warnings

from PIL import Image, ImageDraw, ImageFont, UnidentifiedImageError


MAX_FILES = 64
MAX_ENCODED_BYTES = 512 * 1024 * 1024
MAX_DECODED_PIXELS = 200_000_000
MAX_OUTPUT_EDGE = 16_384
THUMB_WIDTH = 480
PADDING = 16
LABEL_HEIGHT = 28


def natural_key(path: Path) -> tuple[object, ...]:
    return tuple(int(part) if part.isdigit() else part.casefold()
        for part in re.split(r"(\d+)", path.name))


def image_paths(directory: Path) -> list[Path]:
    if not directory.is_dir():
        raise ValueError("input_dir must be a directory")
    entries=sorted((entry for entry in directory.iterdir() if not entry.name.startswith('.')),
        key=natural_key)
    if not entries:
        raise ValueError("input_dir contains no images")
    if len(entries)>MAX_FILES:
        raise ValueError(f"image count exceeds limit ({MAX_FILES})")
    total=0
    for path in entries:
        if path.is_symlink():
            raise ValueError(f"symlink image is not allowed: {path.name}")
        if not path.is_file():
            raise ValueError(f"image entry is not a regular file: {path.name}")
        total+=path.stat().st_size
        if total>MAX_ENCODED_BYTES:
            raise ValueError("encoded image bytes exceed limit")
    return entries


def load_images(paths: list[Path]) -> list[tuple[Path, Image.Image]]:
    loaded=[];pixels=0
    for path in paths:
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("error",Image.DecompressionBombWarning)
                with Image.open(path) as probe:
                    probe.verify()
                with Image.open(path) as source:
                    source.load()
                    pixels+=source.width*source.height
                    if pixels>MAX_DECODED_PIXELS:
                        raise ValueError("decoded image pixels exceed limit")
                    loaded.append((path,source.convert("RGB")))
        except (UnidentifiedImageError,OSError,Image.DecompressionBombError,
                Image.DecompressionBombWarning) as exc:
            raise ValueError(f"invalid or unsafe image: {path.name}") from exc
    return loaded


def build_montage(images: list[tuple[Path, Image.Image]], label_mode: str) -> Image.Image:
    columns=max(1,math.ceil(math.sqrt(len(images))))
    rows=math.ceil(len(images)/columns)
    label_height=LABEL_HEIGHT if label_mode=='filename' else 0
    ratios=[image.height/image.width for _,image in images]
    cell_height=max(1,math.ceil(THUMB_WIDTH*max(ratios)))+label_height
    width=columns*THUMB_WIDTH+(columns+1)*PADDING
    height=rows*cell_height+(rows+1)*PADDING
    if width>MAX_OUTPUT_EDGE or height>MAX_OUTPUT_EDGE:
        raise ValueError("montage output edge exceeds limit")
    canvas=Image.new("RGB",(width,height),"white")
    draw=ImageDraw.Draw(canvas);font=ImageFont.load_default()
    for index,(path,image) in enumerate(images):
        row,column=divmod(index,columns)
        scale=min(THUMB_WIDTH/image.width,(cell_height-label_height)/image.height)
        size=(max(1,round(image.width*scale)),max(1,round(image.height*scale)))
        thumb=image.resize(size,Image.Resampling.LANCZOS)
        x=PADDING+column*THUMB_WIDTH+(THUMB_WIDTH-size[0])//2
        y=PADDING+row*cell_height
        canvas.paste(thumb,(x,y))
        if label_mode=='filename':
            label=path.name[:120]
            draw.text((PADDING+column*THUMB_WIDTH,y+cell_height-LABEL_HEIGHT+6),label,
                fill="black",font=font)
    return canvas


def save_atomic(image: Image.Image, output: Path) -> None:
    suffix=output.suffix.lower()
    if suffix not in {'.png','.jpg','.jpeg'}:
        raise ValueError("output_file must end in .png, .jpg, or .jpeg")
    output.parent.mkdir(parents=True,exist_ok=True)
    descriptor,temp_name=tempfile.mkstemp(prefix='.'+output.name+'.',suffix=suffix,dir=output.parent)
    os.close(descriptor);temp=Path(temp_name)
    try:
        format_name='PNG' if suffix=='.png' else 'JPEG'
        image.save(temp,format=format_name,optimize=False,quality=90)
        with temp.open('rb') as stream:os.fsync(stream.fileno())
        os.replace(temp,output)
    finally:
        temp.unlink(missing_ok=True)


def main() -> int:
    parser=argparse.ArgumentParser()
    parser.add_argument('--input_dir',required=True,type=Path)
    parser.add_argument('--output_file',required=True,type=Path)
    parser.add_argument('--label_mode',choices=('filename','none'),default='filename')
    args=parser.parse_args()
    try:
        images=load_images(image_paths(args.input_dir))
        save_atomic(build_montage(images,args.label_mode),args.output_file)
    except ValueError as exc:
        parser.error(str(exc))
    return 0


if __name__=='__main__':
    raise SystemExit(main())
