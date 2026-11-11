import React,{useEffect,useRef,useState} from 'react';
import {ChevronLeft,ChevronRight} from 'lucide-react';
import {Document,Page,pdfjs} from 'react-pdf';
import 'react-pdf/dist/Page/AnnotationLayer.css';
import 'react-pdf/dist/Page/TextLayer.css';

pdfjs.GlobalWorkerOptions.workerSrc=new URL('pdfjs-dist/build/pdf.worker.min.mjs',import.meta.url).toString();

function useWidth(ref){
 const [width,setWidth]=useState(800);
 useEffect(()=>{if(!ref.current)return;const resize=()=>setWidth(Math.max(280,Math.min(1000,ref.current.clientWidth-28))),observer=new ResizeObserver(resize);resize();observer.observe(ref.current);return()=>observer.disconnect()},[ref]);
 return width;
}

function PdfPreview({url,paged=false}){
 const root=useRef(null),width=useWidth(root),[pages,setPages]=useState(0),[page,setPage]=useState(1),[error,setError]=useState('');
 useEffect(()=>{setPages(0);setPage(1);setError('')},[url]);
 return <div className={`document-pdf ${paged?'paged':''}`} ref={root}>
  {paged&&<nav><button disabled={page<=1} onClick={()=>setPage(value=>value-1)} aria-label="上一页"><ChevronLeft size={16}/></button><span>{pages?`${page} / ${pages}`:'正在加载…'}</span><button disabled={!pages||page>=pages} onClick={()=>setPage(value=>value+1)} aria-label="下一页"><ChevronRight size={16}/></button></nav>}
  {error?<div className="preview-error">{error}</div>:<Document file={url} loading={<div className="preview-loading">正在加载文档…</div>} onLoadSuccess={value=>setPages(value.numPages)} onLoadError={()=>setError('文档预览加载失败，可以下载后查看。')}>
   {paged?(pages>0&&<Page pageNumber={page} width={width}/>):Array.from({length:pages},(_,index)=><Page key={index+1} pageNumber={index+1} width={width}/>) }
  </Document>}
 </div>;
}

function DocxPreview({url}){
 const root=useRef(null),[state,setState]=useState('loading');
 useEffect(()=>{let cancelled=false;setState('loading');if(root.current)root.current.replaceChildren();(async()=>{try{const [response,module]=await Promise.all([fetch(url),import('docx-preview')]);if(!response.ok)throw new Error();const data=await response.arrayBuffer();if(cancelled)return;await module.renderAsync(data,root.current,undefined,{className:'docx-page',inWrapper:true,breakPages:true,renderHeaders:true,renderFooters:true});if(!cancelled)setState('ready')}catch{if(!cancelled)setState('error')}})();return()=>{cancelled=true}},[url]);
 return <div className="document-scroll">{state==='loading'&&<div className="preview-loading">正在加载 Word 文档…</div>}{state==='error'&&<div className="preview-error">Word 文档预览失败，可以下载后查看。</div>}<div className="docx-preview" ref={root}/></div>;
}

function SpreadsheetPreview({url}){
 const [book,setBook]=useState(null),[sheet,setSheet]=useState(''),[error,setError]=useState('');
 useEffect(()=>{let cancelled=false;setBook(null);setError('');(async()=>{try{const [response,module]=await Promise.all([fetch(url),import('xlsx')]);if(!response.ok)throw new Error();const XLSX=module.read?module:module.default,workbook=XLSX.read(await response.arrayBuffer(),{type:'array',cellDates:true});if(cancelled)return;const sheets=Object.fromEntries(workbook.SheetNames.map(name=>[name,XLSX.utils.sheet_to_json(workbook.Sheets[name],{header:1,raw:false,defval:''})]));setBook({names:workbook.SheetNames,sheets});setSheet(workbook.SheetNames[0]||'')}catch{if(!cancelled)setError('Excel 文档预览失败，可以下载后查看。')}})();return()=>{cancelled=true}},[url]);
 if(error)return <div className="preview-error">{error}</div>;
 if(!book)return <div className="preview-loading">正在加载 Excel 文档…</div>;
 const rows=book.sheets[sheet]||[],visible=rows.slice(0,1000),columns=Math.min(100,Math.max(0,...visible.map(row=>row.length)));
 return <div className="spreadsheet-preview"><nav>{book.names.map(name=><button className={name===sheet?'active':''} onClick={()=>setSheet(name)} key={name}>{name}</button>)}</nav><div className="spreadsheet-scroll"><table><tbody>{visible.map((row,rowIndex)=><tr key={rowIndex}>{Array.from({length:columns},(_,column)=><td key={column}>{String(row[column]??'')}</td>)}</tr>)}</tbody></table>{rows.length>visible.length&&<p>仅预览前 1000 行，完整内容请下载查看。</p>}</div></div>;
}

export default function DocumentPreview({value,kind}){
 if(kind==='pdf')return <PdfPreview url={value.url}/>;
 if(kind==='presentation')return <PdfPreview url={value.previewUrl} paged/>;
 if(kind==='document')return <DocxPreview url={value.download}/>;
 if(kind==='spreadsheet')return <SpreadsheetPreview url={value.download}/>;
 return <div className="preview-error">该文件暂不支持在线预览。</div>;
}

function PdfThumbnail({url}){
 return <Document file={url} loading={<span className="document-thumbnail-loading">正在生成预览</span>} error={<span className="document-thumbnail-loading">无法预览</span>}><Page pageNumber={1} width={230} renderTextLayer={false} renderAnnotationLayer={false}/></Document>;
}

function DocxThumbnail({url}){
 const root=useRef(null);
 useEffect(()=>{let cancelled=false;if(root.current)root.current.replaceChildren();(async()=>{try{const [response,module]=await Promise.all([fetch(url),import('docx-preview')]);if(!response.ok)throw new Error();const data=await response.arrayBuffer();if(!cancelled)await module.renderAsync(data,root.current,undefined,{className:'docx-page',inWrapper:true,breakPages:true,renderHeaders:false,renderFooters:false})}catch{if(root.current)root.current.textContent='无法预览'}})();return()=>{cancelled=true}},[url]);
 return <div className="document-thumbnail-docx" ref={root}/>;
}

function SpreadsheetThumbnail({url}){
 const [rows,setRows]=useState([]);
 useEffect(()=>{let cancelled=false;(async()=>{try{const [response,module]=await Promise.all([fetch(url),import('xlsx')]);const XLSX=module.read?module:module.default,book=XLSX.read(await response.arrayBuffer(),{type:'array'}),values=XLSX.utils.sheet_to_json(book.Sheets[book.SheetNames[0]],{header:1,raw:false,defval:''});if(!cancelled)setRows(values.slice(0,7).map(row=>row.slice(0,5)))}catch{}})();return()=>{cancelled=true}},[url]);
 return <table className="document-thumbnail-sheet"><tbody>{rows.map((row,index)=><tr key={index}>{row.map((cell,column)=><td key={column}>{String(cell)}</td>)}</tr>)}</tbody></table>;
}

export function DocumentThumbnail({value,kind}){
 if(kind==='pdf')return <PdfThumbnail url={value.url}/>;
 if(kind==='presentation')return <PdfThumbnail url={value.previewUrl}/>;
 if(kind==='document')return <DocxThumbnail url={value.download}/>;
 if(kind==='spreadsheet')return <SpreadsheetThumbnail url={value.download}/>;
 return null;
}
