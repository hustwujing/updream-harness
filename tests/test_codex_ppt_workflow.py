import importlib.util
from pathlib import Path
import sys
from types import ModuleType


ROOT=Path(__file__).resolve().parents[1]


def load(name,path):
    spec=importlib.util.spec_from_file_location(name,path);module=importlib.util.module_from_spec(spec)
    sys.path.insert(0,str(path.parent))
    try:spec.loader.exec_module(module)
    finally:sys.path.pop(0)
    return module


def test_speaker_notes_accept_level_one_and_canonical_level_two_headings(tmp_path):
    assemble=load("codex_ppt_assemble",ROOT/"runtime/skills/codex-ppt/scripts/assemble_ppt.py")
    (tmp_path/"speech.md").write_text("# Slide 1\nFirst note\n\n## 第 2 页：标题\n第二页备注\n")
    assert assemble.load_speaker_notes(str(tmp_path))=={1:"First note",2:"第二页备注"}


def test_generated_slide_prompt_defaults_to_sparse_provider_rendered_text(tmp_path):
    state=ModuleType("slide_run_state");state.DEFAULT_MAX_CONCURRENT_SLIDES=8
    state.now_iso=lambda:"now";state.rel_to_deck=lambda *args:"";state.save_jobs=lambda *args:None
    state.set_run_status=lambda *args:None;sys.modules["slide_run_state"]=state
    try:prompts=load("codex_ppt_prompts",ROOT/"runtime/skills/codex-ppt/scripts/prepare_slide_prompts.py")
    finally:sys.modules.pop("slide_run_state",None)
    value=prompts._build_prompt(deck={"language":"Chinese","goal":"Explain","style":{}},
        slide={"title":"总体架构","key_points":["one","two","three","four","five","six"]},
        number=2,global_style_reference=None,base_dir=tmp_path)
    assert "at most five short labels" in value
    assert "Detailed prose belongs in speaker notes" in value


def test_codex_ppt_forbids_duplicate_provider_authorization_channel():
    skill=(ROOT/"runtime/skills/codex-ppt/SKILL.md").read_text()
    assert "Do not use `sandbox-interact`" in skill
    assert "media confirmation card" in skill
