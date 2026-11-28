import React,{useEffect,useRef,useState} from 'react';


export function useAnimatedNumber(identity,target,duration=350){
 const value=Math.max(0,Number(target||0)),[display,setDisplay]=useState(value),current=useRef(value),key=useRef(identity);
 useEffect(()=>{const reduced=globalThis.matchMedia?.('(prefers-reduced-motion: reduce)').matches;
  if(key.current!==identity||reduced){key.current=identity;current.current=value;setDisplay(value);return}
  const start=current.current,change=value-start;if(!change)return;const began=performance.now();let frame=0;
  const step=now=>{const progress=Math.min(1,(now-began)/duration),eased=1-Math.pow(1-progress,3),next=start+change*eased;current.current=next;setDisplay(next);if(progress<1)frame=requestAnimationFrame(step)};
  frame=requestAnimationFrame(step);return()=>cancelAnimationFrame(frame)
 },[duration,identity,value]);return display
}


export function unicodeCharacterCount(text){return [...String(text||'')].length}

export function UnicodeCharacterCount({identity,text,count:providedCount,className='',label='正在生成'}){
 const count=providedCount==null?unicodeCharacterCount(text):Math.max(0,Number(providedCount||0));
 const display=useAnimatedNumber(identity,count,500);
 return <small className={className} data-character-count={Math.round(display)}>
  {label} · <strong>{Math.round(display).toLocaleString()}</strong> 字符
 </small>
}
