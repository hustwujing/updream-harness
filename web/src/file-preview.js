const extension=value=>String(value?.name||'').split('.').at(-1)?.toLowerCase()||'';

export function previewKind(value){
 const mime=String(value?.mime||'').toLowerCase(),ext=extension(value);
 if(mime==='application/pdf'||ext==='pdf')return 'pdf';
 if(mime.includes('presentation')||mime==='application/vnd.ms-powerpoint'||['ppt','pptx'].includes(ext))return 'presentation';
 if(mime.includes('wordprocessingml')||mime==='application/msword'||['doc','docx'].includes(ext))return ext==='doc'?'unsupported':'document';
 if(mime.includes('spreadsheetml')||mime==='application/vnd.ms-excel'||['xls','xlsx'].includes(ext))return 'spreadsheet';
 if(mime.startsWith('image/'))return 'image';
 if(mime.startsWith('video/'))return 'video';
 if(mime.startsWith('text/'))return 'text';
 return '';
}

export const previewable=value=>Boolean(previewKind(value));
