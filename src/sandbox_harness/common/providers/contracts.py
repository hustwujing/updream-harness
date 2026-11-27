from __future__ import annotations

from typing import Any,Protocol


class MediaProviderContract(Protocol):
    adapter:str
    kind:str
    def validate_spec(self,spec:object)->None:...
    def public(self,spec:object)->tuple[dict[str,Any],dict[str,Any]]:...
    def public_metadata(self,spec:object)->dict[str,Any]:...
    def availability(self,spec:object)->tuple[bool,str]:...
    def validate_shape(self,spec:object,parameters:dict[str,Any],mode:str)->None:...
    def validate(self,spec:object,parameters:dict[str,Any],mode:str,
                 references:list[dict[str,Any]])->None:...
    def provider_parameters(self,spec:object,parameters:dict[str,Any],mode:str)->dict[str,Any]:...


class MediaProviderContractRegistry:
    def __init__(self,contracts:list[MediaProviderContract]|tuple[MediaProviderContract,...]):
        values={contract.adapter:contract for contract in contracts}
        if len(values)!=len(contracts):raise ValueError("duplicate_media_provider_contract")
        self._values=values

    def resolve(self,adapter:str)->MediaProviderContract:
        try:return self._values[adapter]
        except KeyError:raise ValueError("media_provider_contract_not_registered:"+adapter) from None

    @property
    def adapters(self)->frozenset[str]:return frozenset(self._values)


def default_media_provider_contracts(*,ark_assets_configured:bool=True,
                                     tripo_configured:bool=True)->MediaProviderContractRegistry:
    from .ark import (ArkImageContract,ArkVideoContract)
    from .dashscope import (HappyhorseVideoContract,WanVideoContract)
    from .minimax import (MiniMaxAudioContract)
    from .tripo import (TripoContract)
    return MediaProviderContractRegistry((ArkImageContract(),ArkVideoContract(ark_assets_configured),
        HappyhorseVideoContract(),WanVideoContract(),MiniMaxAudioContract(),
        TripoContract(tripo_configured)))
