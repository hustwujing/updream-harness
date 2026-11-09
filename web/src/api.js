export function apiUrl(path){return new URL(`api/${path}`,document.baseURI).toString();}

export async function api(path,options={}){
  const response=await fetch(apiUrl(path),{...options,headers:{...(options.body instanceof Blob?{}:{'Content-Type':'application/json'}),...(options.headers||{})}});
  if(!response.ok){const error=new Error((await response.text())||response.statusText);error.status=response.status;throw error;}
  return response.status===204?null:response.json();
}

export const listThreads=()=>api('threads');
export const ensureSession=()=>api('session');
export const getHarnessCatalog=()=>api('catalog/harnesses');
export const createThread=value=>api('threads',{method:'POST',body:JSON.stringify(value)});
export const listTurns=id=>api(`threads/${id}/turns`);
export const listItems=(id,after=0)=>api(`threads/${id}/items?after=${after}&limit=500`);
export const listProjectFiles=id=>api(`threads/${id}/project-files`);
export const getTurn=id=>api(`turns/${id}`);
export const createTurn=(id,value)=>api(`threads/${id}/turns`,{method:'POST',body:JSON.stringify(value)});
export const appendTurnInput=(id,value)=>api(`turns/${id}/inputs`,{method:'POST',body:JSON.stringify(value)});
export const interruptTurn=id=>api(`turns/${id}/cancel`,{method:'POST'});
export async function upload(file){return api(`uploads?name=${encodeURIComponent(file.name)}&mime=${encodeURIComponent(file.type||'application/octet-stream')}`,{method:'POST',body:file,headers:{'Content-Type':file.type||'application/octet-stream'}});}
