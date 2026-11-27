export function visibleNetworkAccess(activeTurn,session){
 return activeTurn?.network_access||(session?.authenticated?'public':'restricted');
}
