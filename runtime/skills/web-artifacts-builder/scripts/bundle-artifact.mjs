import {execFileSync,spawnSync} from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import {pathToFileURL} from 'node:url';

const project=path.resolve(process.argv[2]||'');
if(!project||!fs.existsSync(path.join(project,'package.json')))throw new Error('project is required');
const globalRoot=execFileSync('npm',['root','-g'],{encoding:'utf8'}).trim();
const esbuild=await import(pathToFileURL(path.join(globalRoot,'esbuild/lib/main.js')));
const buildRoot=path.join(project,'.artifact-build');
fs.rmSync(buildRoot,{recursive:true,force:true});fs.mkdirSync(buildRoot,{recursive:true});
const cssPath=path.join(buildRoot,'styles.css');
const tailwind=spawnSync(process.execPath,[path.join(globalRoot,'tailwindcss/lib/cli.js'),
  '-c',path.join(project,'tailwind.config.cjs'),'-i',path.join(project,'src/index.css'),
  '-o',cssPath,'--minify'],{cwd:project,encoding:'utf8'});
if(tailwind.status!==0)throw new Error(`tailwind failed: ${tailwind.stderr||tailwind.stdout}`);
const alias={name:'workspace-alias',setup(build){build.onResolve({filter:/^@\//},args=>({
  path:path.join(project,'src',args.path.slice(2))}));}};
const result=await esbuild.build({absWorkingDir:project,entryPoints:['src/main.tsx'],bundle:true,
  write:false,format:'iife',platform:'browser',target:['es2022'],jsx:'automatic',minify:true,
  plugins:[alias],loader:{'.png':'dataurl','.jpg':'dataurl','.jpeg':'dataurl','.gif':'dataurl',
    '.webp':'dataurl','.svg':'dataurl','.css':'empty'}});
const javascript=result.outputFiles.find(file=>file.path.endsWith('.js'));
if(!javascript)throw new Error('esbuild produced no JavaScript');
const source=fs.readFileSync(path.join(project,'index.html'),'utf8');
if(!source.includes('<div id="root"></div>'))throw new Error('index.html has no root element');
const css=fs.readFileSync(cssPath,'utf8');
const html=source.replace('</head>',`<style>${css}</style></head>`)
  .replace('</body>',`<script>${javascript.text}</script></body>`);
fs.writeFileSync(path.join(project,'bundle.html'),html);
fs.rmSync(buildRoot,{recursive:true,force:true});
console.log(`offline bundle complete: bundle.html (${Buffer.byteLength(html)} bytes)`);
