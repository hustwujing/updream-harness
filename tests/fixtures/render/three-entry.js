import * as THREE from 'three';

const scene = new THREE.Scene();
scene.background = new THREE.Color('#11161a');
const camera = new THREE.PerspectiveCamera(42, 16 / 9, 0.1, 100);
camera.position.set(3.2, 2.2, 4.8);
camera.lookAt(0, 0.4, 0);
const renderer = new THREE.WebGLRenderer({antialias: true, preserveDrawingBuffer: true});
renderer.setPixelRatio(1);
renderer.setSize(640, 360, false);
renderer.shadowMap.enabled = true;
document.body.append(renderer.domElement);

const cube = new THREE.Mesh(
  new THREE.BoxGeometry(1.4, 1.4, 1.4),
  new THREE.MeshStandardMaterial({color: '#35c2b4', roughness: 0.28, metalness: 0.15}),
);
cube.position.y = 0.85;
cube.castShadow = true;
scene.add(cube);
const floor = new THREE.Mesh(new THREE.PlaneGeometry(12, 12), new THREE.MeshStandardMaterial({color: '#d8ddd5', roughness: 0.9}));
floor.rotation.x = -Math.PI / 2;
floor.receiveShadow = true;
scene.add(floor);
scene.add(new THREE.HemisphereLight('#ffffff', '#38434a', 2.1));
const key = new THREE.DirectionalLight('#fff3dc', 4.2);
key.position.set(3, 6, 4); key.castShadow = true; scene.add(key);

window.renderFrame = time => {
  cube.rotation.x = time * 0.9;
  cube.rotation.y = time * 1.4;
  renderer.render(scene, camera);
};
window.renderFrame(0);
window.__RENDER_READY__ = true;
