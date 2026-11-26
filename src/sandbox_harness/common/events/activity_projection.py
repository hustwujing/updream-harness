from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any


MAX_DISPLAY_VALUE_CHARS=16_384
MAX_OUTPUT_TAIL_CHARS=8_192
HIDDEN_STEP_NAMES={
    'Codex session','Claude session','DeepSeek session',
    'Codex response','Claude response','DeepSeek response',
    'Input completed','Interaction applied',
}


@dataclass(frozen=True)
class ProjectionInstruction:
    key:str
    kind:str
    status:str
    summary:str
    payload:dict[str,Any]
    output_text:str=''


def _bounded(value:Any)->Any:
    try:encoded=json.dumps(value,ensure_ascii=False,sort_keys=True,default=str)
    except (TypeError,ValueError):encoded=str(value)
    if len(encoded)<=MAX_DISPLAY_VALUE_CHARS:return value
    return {'preview':encoded[:MAX_DISPLAY_VALUE_CHARS],'truncated':True}


def _payload(value:dict[str,Any])->dict[str,Any]:
    result={}
    for key in ('name','tool_use_id','activity_id','step_id','model_request_id','interaction_id',
                'code','is_error','detail','result','plan','phase'):
        if key in value:result[key]=_bounded(value[key])
    if value.get('text') and not result.get('detail'):
        result['text']=str(value['text'])[:MAX_DISPLAY_VALUE_CHARS]
    return result


def projection_instruction(kind:str,payload:dict[str,Any],status:str,
                           sequence:int)->ProjectionInstruction|None:
    if kind=='text_delta':return None
    name=str(payload.get('name') or '').strip()
    if kind in {'step_started','step_progress','step_finished'} and name in HIDDEN_STEP_NAMES:
        return None
    if kind in {'tool_started','tool_output','tool_finished'}:
        identity=str(payload.get('tool_use_id') or '').strip()
        if not identity:
            if kind=='tool_output':return None
            identity='event:'+str(sequence)
        state=('failed' if payload.get('is_error') else 'completed'
            if kind=='tool_finished' else 'in_progress')
        return ProjectionInstruction('tool:'+identity,'tool',state,name or 'tool',_payload(payload),
            str(payload.get('text') or '') if kind=='tool_output' else '')
    if kind in {'step_started','step_progress','step_finished'}:
        identity=next((str(payload[key]).strip() for key in
            ('activity_id','step_id','model_request_id','interaction_id') if payload.get(key)), '')
        projected_kind='plan' if name.lower()=='plan' else 'step'
        key=('plan' if projected_kind=='plan' else 'step:'+identity
            if identity else 'event:'+str(sequence))
        state=('failed' if payload.get('is_error') else 'completed'
            if kind=='step_finished' else 'in_progress')
        return ProjectionInstruction(key,projected_kind,state,name or '执行阶段',_payload(payload))
    if kind in {'warning','error','artifact_created','waiting_user'}:
        state=('failed' if kind=='error' else 'in_progress' if kind=='waiting_user' else 'completed')
        return ProjectionInstruction('event:'+str(sequence),kind,state,
            name or str(payload.get('text') or kind),_payload(payload))
    return None


def merge_projection_payload(current:dict[str,Any],instruction:ProjectionInstruction)->dict[str,Any]:
    result={**current,**instruction.payload}
    if instruction.output_text:
        result['output_tail']=(str(current.get('output_tail') or '')+instruction.output_text)[
            -MAX_OUTPUT_TAIL_CHARS:]
    return result
