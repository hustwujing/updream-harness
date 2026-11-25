import dagre from '@dagrejs/dagre';

export const GRAPH_NODE_WIDTH=170;
export const GRAPH_NODE_HEIGHT=82;

const terminal=new Set(['COMPLETED','FAILED','CANCELLED','PAUSED','WAITING_USER','SUCCEEDED','BLOCKED','UNKNOWN','TERMINAL','CONTINUED','CONFIRMED','REJECTED','EXPIRED','PUBLISHED','REVIEW_BLOCKED','OPTIONAL_WARNING','REQUIRED_UNAVAILABLE','responded','expired']);

function graphTurns(nodes){
 return nodes.filter(node=>node.type==='primary_harness').sort((left,right)=>String(left.started_at).localeCompare(String(right.started_at))).map((node,index)=>({id:node.turn_id,label:`第 ${index+1} 轮`,state:node.state,startedAt:node.started_at}));
}

function selectedTurn(turns,requested){
 if(requested&&turns.some(turn=>turn.id===requested))return requested;
 return [...turns].reverse().find(turn=>!terminal.has(turn.state))?.id||turns.at(-1)?.id||'';
}

function turnComponent(graph,turnId){
 const direct=new Set(graph.nodes.filter(node=>node.turn_id===turnId).map(node=>node.id)),outgoing=new Map();
 for(const edge of graph.edges||[]){if(!outgoing.has(edge.from))outgoing.set(edge.from,[]);if(!outgoing.has(edge.to))outgoing.set(edge.to,[]);outgoing.get(edge.from).push(edge.to);outgoing.get(edge.to).push(edge.from)}
 const visible=new Set(direct),queue=[...direct];
 while(queue.length){for(const target of outgoing.get(queue.shift())||[])if(!visible.has(target)){visible.add(target);queue.push(target)}}
 return visible;
}

export function defaultCollapsedGroups(graph,turnId){
 const component=turnComponent(graph,turnId),counts=new Map();
 for(const edge of graph.edges||[])if(component.has(edge.from)&&component.has(edge.to)&&edge.kind==='contains')counts.set(edge.from,(counts.get(edge.from)||0)+1);
 return new Set([...counts].filter(([,count])=>count>6).map(([id])=>id));
}

function hiddenByCollapsedGroups(graph,component,collapsedGroups){
 const outgoing=new Map();
 for(const edge of graph.edges||[]){if(['continued_by','reported_to','waits_on'].includes(edge.kind))continue;if(!outgoing.has(edge.from))outgoing.set(edge.from,[]);outgoing.get(edge.from).push(edge.to)}
 const hidden=new Set(),counts=new Map();
 for(const group of collapsedGroups||[]){const queue=[...(outgoing.get(group)||[])];counts.set(group,queue.length);while(queue.length){const id=queue.shift();if(hidden.has(id)||!component.has(id))continue;hidden.add(id);queue.push(...(outgoing.get(id)||[]))}}
 return {hidden,counts};
}

function wrapRanks(nodes,maxWidth){
 if(!Number.isFinite(maxWidth))return null;
 const gap=52,margin=24,columns=Math.max(1,Math.floor((maxWidth-margin*2+gap)/(GRAPH_NODE_WIDTH+gap)));
 const ranks=[...new Set(nodes.map(node=>node.x))].sort((a,b)=>a-b);
 if(ranks.length<=columns)return null;
 const byRank=new Map(ranks.map(value=>[value,nodes.filter(node=>node.x===value)]));
 const bands=[];
 for(let index=0;index<ranks.length;index+=columns)bands.push(ranks.slice(index,index+columns));
 let top=margin;const positioned=[];
 bands.forEach((band,bandIndex)=>{
  const spans=band.map(rank=>{const values=byRank.get(rank),minimum=Math.min(...values.map(node=>node.y)),maximum=Math.max(...values.map(node=>node.y+node.height));return {rank,values,minimum,span:maximum-minimum}}),height=Math.max(...spans.map(value=>value.span));
  spans.forEach((entry,index)=>{const slot=bandIndex%2?columns-1-index:index,x=margin+slot*(GRAPH_NODE_WIDTH+gap);for(const node of entry.values)positioned.push({...node,x,y:top+node.y-entry.minimum,band:bandIndex})});
  top+=height+64;
 });
 return {nodes:positioned,width:Math.min(maxWidth,margin*2+columns*GRAPH_NODE_WIDTH+(columns-1)*gap),height:Math.max(360,top-80+margin)};
}

export function executionGraphView(graph,{turnId='',collapsedGroups=new Set(),maxWidth=Infinity}={}){
 const allNodes=graph.nodes||[],allEdges=graph.edges||[],turns=graphTurns(allNodes),selectedTurnId=selectedTurn(turns,turnId);
 if(!selectedTurnId)return {nodes:[],edges:[],turns,selectedTurnId:'',width:640,height:360};
 const component=turnComponent({nodes:allNodes,edges:allEdges},selectedTurnId),{hidden,counts}=hiddenByCollapsedGroups({nodes:allNodes,edges:allEdges},component,collapsedGroups);
 const selectedNodes=allNodes.filter(node=>component.has(node.id)&&!hidden.has(node.id)),ids=new Set(selectedNodes.map(node=>node.id));
 const selectedEdges=allEdges.filter(edge=>ids.has(edge.from)&&ids.has(edge.to));
 const layoutGraph=new dagre.graphlib.Graph().setDefaultEdgeLabel(()=>({}));
 layoutGraph.setGraph({rankdir:'LR',ranksep:52,nodesep:22,edgesep:14,marginx:24,marginy:24});
 for(const node of selectedNodes)layoutGraph.setNode(node.id,{width:GRAPH_NODE_WIDTH,height:GRAPH_NODE_HEIGHT});
 for(const edge of selectedEdges)layoutGraph.setEdge(edge.from,edge.to);
 dagre.layout(layoutGraph);
 let positioned=selectedNodes.map(node=>{const position=layoutGraph.node(node.id);return {...node,x:position.x-GRAPH_NODE_WIDTH/2,y:position.y-GRAPH_NODE_HEIGHT/2,width:GRAPH_NODE_WIDTH,height:GRAPH_NODE_HEIGHT,collapsedCount:counts.get(node.id)||0,band:0}});
 const dimensions=layoutGraph.graph();
 const wrapped=wrapRanks(positioned,maxWidth);if(wrapped)positioned=wrapped.nodes;
 return {nodes:positioned,edges:selectedEdges.map((edge,index)=>({...edge,id:`${edge.from}:${edge.to}:${edge.kind}:${index}`})),turns,selectedTurnId,width:wrapped?.width||Math.max(640,Math.ceil(dimensions.width||0)),height:wrapped?.height||Math.max(360,Math.ceil(dimensions.height||0))};
}
