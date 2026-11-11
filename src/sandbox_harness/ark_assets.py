from __future__ import annotations

import datetime
import hashlib
import hmac
import json
from typing import Any
from urllib.parse import urlencode, urlsplit

import httpx


class ArkAssetError(RuntimeError):
    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


def _sign(key: bytes, value: str) -> bytes:
    return hmac.new(key, value.encode(), hashlib.sha256).digest()


class ArkAssets:
    def __init__(self, settings, *, transport: httpx.AsyncBaseTransport | None = None):
        self.endpoint = settings.ark_control_endpoint.rstrip("/")
        parsed = urlsplit(self.endpoint)
        if parsed.scheme != "https" or not parsed.netloc or parsed.path:
            raise ValueError("Ark control endpoint must be an HTTPS origin")
        self.host = parsed.netloc
        self.access_key = settings.ark_access_key
        self.secret_key = settings.ark_secret_key
        self.project_name = settings.ark_project_name
        self.group_id = settings.ark_asset_group_id
        self.group_type = settings.ark_asset_group_type
        self.region = "cn-beijing"
        self.service = "ark"
        self.transport = transport

    @property
    def configured(self) -> bool:
        return bool(self.credentials_configured and self.group_id)

    @property
    def credentials_configured(self) -> bool:
        return bool(self.access_key and self.secret_key and self.project_name)

    def _headers(self, action: str, body: bytes, now: datetime.datetime) -> tuple[str, dict[str, str]]:
        xdate = now.astimezone(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        date = xdate[:8]
        payload_hash = hashlib.sha256(body).hexdigest()
        query = urlencode({"Action": action, "Version": "2024-01-01"})
        canonical_headers = (f"content-type:application/json\nhost:{self.host}\n"
            f"x-content-sha256:{payload_hash}\nx-date:{xdate}\n")
        signed_headers = "content-type;host;x-content-sha256;x-date"
        canonical_request = f"POST\n/\n{query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
        scope = f"{date}/{self.region}/{self.service}/request"
        string_to_sign = (f"HMAC-SHA256\n{xdate}\n{scope}\n"
            f"{hashlib.sha256(canonical_request.encode()).hexdigest()}")
        signing = _sign(_sign(_sign(_sign(self.secret_key.encode(), date), self.region), self.service), "request")
        signature = hmac.new(signing, string_to_sign.encode(), hashlib.sha256).hexdigest()
        authorization = (f"HMAC-SHA256 Credential={self.access_key}/{scope}, "
            f"SignedHeaders={signed_headers}, Signature={signature}")
        return query, {"Content-Type": "application/json", "Host": self.host,
            "X-Content-Sha256": payload_hash, "X-Date": xdate, "Authorization": authorization}

    async def _call(self, action: str, payload: dict[str, Any]) -> dict[str, Any]:
        if not self.credentials_configured:
            raise ArkAssetError("ark_asset_credentials_unavailable")
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
        query, headers = self._headers(action, body, datetime.datetime.now(datetime.timezone.utc))
        try:
            async with httpx.AsyncClient(timeout=30, trust_env=False, transport=self.transport) as client:
                response = await client.post(self.endpoint + "/?" + query, content=body, headers=headers)
        except httpx.HTTPError as exc:
            raise ArkAssetError("ark_asset_transport_error") from exc
        try:
            value = response.json()
        except ValueError as exc:
            raise ArkAssetError("ark_asset_invalid_response") from exc
        metadata = value.get("ResponseMetadata", {}) if isinstance(value, dict) else {}
        error = metadata.get("Error", {}) if isinstance(metadata, dict) else {}
        if not response.is_success or error:
            code = str(error.get("Code") or f"http_{response.status_code}")
            raise ArkAssetError("ark_asset_" + "".join(character if character.isalnum() else "_" for character in code)[:80])
        result = value.get("Result", {}) if isinstance(value, dict) else {}
        if not isinstance(result, dict):
            raise ArkAssetError("ark_asset_invalid_result")
        return result

    @staticmethod
    def asset_name(digest: str) -> str:
        return "aniforge-" + digest[:40]

    async def find(self, digest: str) -> dict[str, Any] | None:
        if not self.group_id:
            raise ArkAssetError("ark_asset_group_unavailable")
        name = self.asset_name(digest)
        result = await self._call("ListAssets", {"Filter": {"GroupType": self.group_type,
            "GroupIds": [self.group_id], "Name": name}, "MaxResults": 20,
            "SortBy": "CreateTime", "SortOrder": "Desc", "ProjectName": self.project_name})
        for item in result.get("Items", []):
            if isinstance(item, dict) and item.get("Name") == name and item.get("GroupId") == self.group_id:
                return item
        return None

    async def create(self, reference: dict[str, Any]) -> str:
        if not self.group_id:
            raise ArkAssetError("ark_asset_group_unavailable")
        result = await self._call("CreateAsset", {"GroupId": self.group_id,
            "URL": reference["public_url"], "Name": self.asset_name(reference["sha256"]),
            "AssetType": reference["kind"].title(), "ProjectName": self.project_name})
        asset_id = str(result.get("Id") or "")
        if not asset_id.startswith("asset-"):
            raise ArkAssetError("ark_asset_id_missing")
        return asset_id

    async def get(self, asset_id: str) -> dict[str, Any]:
        return await self._call("GetAsset", {"Id": asset_id, "ProjectName": self.project_name})

    async def list_groups(self, name: str = "") -> list[dict[str, Any]]:
        filter_value: dict[str, Any] = {"GroupType": self.group_type}
        if name:
            filter_value["Name"] = name
        result = await self._call("ListAssetGroups", {"Filter": filter_value,
            "MaxResults": 100, "SortBy": "CreateTime", "SortOrder": "Desc",
            "ProjectName": self.project_name})
        return [item for item in result.get("Items", []) if isinstance(item, dict)]

    async def create_group(self, name: str, description: str = "") -> str:
        result = await self._call("CreateAssetGroup", {"Name": name,
            "Description": description, "GroupType": self.group_type,
            "ProjectName": self.project_name})
        group_id = str(result.get("Id") or "")
        if not group_id.startswith("group-"):
            raise ArkAssetError("ark_asset_group_id_missing")
        return group_id
