import React,{useEffect,useRef,useState} from 'react';
import * as THREE from 'three';
import {GLTFLoader} from 'three/examples/jsm/loaders/GLTFLoader.js';
import {OrbitControls} from 'three/examples/jsm/controls/OrbitControls.js';

export default function Model3DPreview({value}){
 const host=useRef(null),[error,setError]=useState('');
 useEffect(()=>{
  const node=host.current;if(!node)return;
  let frame=0,disposed=false,model=null;
  const scene=new THREE.Scene();scene.background=new THREE.Color(0x20211f);
  const camera=new THREE.PerspectiveCamera(38,1,.01,1000);camera.position.set(2.6,1.8,3.2);
  const renderer=new THREE.WebGLRenderer({antialias:true,alpha:false});renderer.setPixelRatio(Math.min(devicePixelRatio||1,2));renderer.outputColorSpace=THREE.SRGBColorSpace;renderer.shadowMap.enabled=true;node.appendChild(renderer.domElement);
  const controls=new OrbitControls(camera,renderer.domElement);controls.enableDamping=true;controls.dampingFactor=.07;
  scene.add(new THREE.HemisphereLight(0xffffff,0x5a625d,2.4));
  const key=new THREE.DirectionalLight(0xffffff,3);key.position.set(4,6,5);scene.add(key);
  const rim=new THREE.DirectionalLight(0x9fc7ff,1.5);rim.position.set(-4,2,-3);scene.add(rim);
  const grid=new THREE.GridHelper(10,20,0x62645e,0x383a37);grid.position.y=-1;scene.add(grid);
  const resize=()=>{const width=Math.max(1,node.clientWidth),height=Math.max(1,node.clientHeight);renderer.setSize(width,height,false);camera.aspect=width/height;camera.updateProjectionMatrix()};
  const observer=new ResizeObserver(resize);observer.observe(node);resize();
  new GLTFLoader().load(value.url,gltf=>{
   if(disposed)return;model=gltf.scene;scene.add(model);
   const box=new THREE.Box3().setFromObject(model),size=box.getSize(new THREE.Vector3()),center=box.getCenter(new THREE.Vector3()),radius=Math.max(size.x,size.y,size.z,.01);
   model.position.sub(center);grid.position.y=-size.y/2;
   camera.near=Math.max(.001,radius/100);camera.far=Math.max(100,radius*100);camera.position.set(radius*1.8,radius*1.2,radius*2.2);camera.updateProjectionMatrix();controls.target.set(0,0,0);controls.update();
  },undefined,()=>!disposed&&setError('3D 模型加载失败'));
  const render=()=>{controls.update();renderer.render(scene,camera);frame=requestAnimationFrame(render)};render();
  return()=>{disposed=true;cancelAnimationFrame(frame);observer.disconnect();controls.dispose();renderer.dispose();scene.traverse(object=>{object.geometry?.dispose?.();if(object.material){const values=Array.isArray(object.material)?object.material:[object.material];for(const material of values){for(const entry of Object.values(material))entry?.isTexture&&entry.dispose();material.dispose?.()}}});renderer.domElement.remove()};
 },[value.url]);
 return <div className="model3d-preview" ref={host}>{error&&<div className="model3d-error" role="alert">{error}</div>}<div className="model3d-controls">拖动旋转 · 滚轮缩放 · 右键平移</div></div>;
}
