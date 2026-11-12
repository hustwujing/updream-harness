import React from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import {Download} from 'lucide-react';
import {apiUrl} from './api';
import {previewable} from './file-preview';

export function workspaceFile(href,artifacts,files){
 let path;try{path=decodeURIComponent(href||'')}catch{return null}
 if(!path.startsWith('/workspace/'))return null;
 if(path.split('/').some(part=>part==='..'))return null;
 const output=path.replace(/^\/workspace\/(?:project\/)?outputs\//,'');
 if(output!==path){
  const artifact=artifacts.find(item=>item.summary===output&&item.payload?.artifact_id);
  if(artifact)return {id:artifact.payload.artifact_id,source:'artifact',name:output,mime:artifact.payload.mime||'application/octet-stream',download:apiUrl(`artifacts/${artifact.payload.artifact_id}`),previewUrl:apiUrl(`previews/artifact/${artifact.payload.artifact_id}`)};
 }
 const managed=path.replace(/^\/workspace\/project\//,'');
 const file=files.find(item=>item.path===managed || (output!==path&&item.path===`artifacts/${output}`));
 return file?{id:file.id,source:'project-file',name:file.path,mime:file.mime,download:apiUrl(`project-files/${file.id}`),previewUrl:apiUrl(`previews/project-file/${file.id}`)}:null;
}

export default function AgentMarkdown({text,artifacts,files,onPreview}){
 const components={
  a:({href,children})=>{
   const file=workspaceFile(href,artifacts,files);
   if(file){
    const preview=previewable(file);
    const open=event=>{if(preview){event.preventDefault();onPreview({...file,url:`${file.download}?inline=true`})}};
    return <span className="message-file"><a href={file.download} onClick={open} download={!preview}>{children}</a>{preview&&<a className="message-download" href={file.download} download title="下载文件" aria-label={`下载 ${file.name}`}><Download size={13}/></a>}</span>;
   }
   if(href?.startsWith('/workspace/'))return <span className="message-file-pending" title="文件保存完成后即可打开">{children} <small>等待文件保存</small></span>;
   if(!href)return <span>{children}</span>;
   return <a href={href} target="_blank" rel="noopener noreferrer">{children}</a>;
  },
  img:({src,alt})=>{
   const file=workspaceFile(src,artifacts,files);
   if(!file)return <span>{alt||'图片尚未保存'}</span>;
   return <button className="message-image" onClick={()=>onPreview({...file,url:`${file.download}?inline=true`})}><img src={`${file.download}?inline=true`} alt={alt||file.name}/></button>;
  },
 };
 return <ReactMarkdown remarkPlugins={[remarkGfm]} skipHtml components={components}>{text}</ReactMarkdown>;
}
