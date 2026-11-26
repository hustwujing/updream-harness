"""Media References persistence; transaction boundaries are preserved."""
from __future__ import annotations
import hashlib
from datetime import (datetime, timedelta)
from sqlalchemy import (or_, select, update)
from sqlalchemy.exc import (IntegrityError)
from sqlalchemy.dialects.postgresql import (insert as postgresql_insert)
from sqlalchemy.dialects.sqlite import (insert as sqlite_insert)
from sandbox_harness.common.contracts.domain import (identifier, now, MediaReferenceUploadView)
from .models import (MediaReferenceAssetRow, MediaReferenceUploadRow, MediaVoiceCloneRow, ThreadRow, utc_datetime)


class MediaReferencesTransactions:
    @staticmethod
    def _media_reference_upload_view(row: MediaReferenceUploadRow) -> MediaReferenceUploadView:
        status = "expired" if row.status == "pending" and utc_datetime(row.expires_at) <= now() else row.status
        return MediaReferenceUploadView(id=row.id, thread_id=row.thread_id, name=row.name,
            kind=row.kind, mime=row.mime, size=row.size, sha256=row.sha256,
            object_key=row.object_key, public_url=row.public_url, status=status,
            project_file_id=row.project_file_id or "", expires_at=utc_datetime(row.expires_at))


    async def create_media_reference_upload(self, owner_id: str, thread_id: str, value, *,
                                            object_key: str, public_url: str,
                                            expires_at: datetime) -> MediaReferenceUploadView:
        async with self.connections.begin() as db:
            thread = (await db.execute(select(ThreadRow).where(
                ThreadRow.id == thread_id, ThreadRow.owner_id == owner_id)
                .with_for_update())).scalar_one_or_none()
            if not thread:
                raise KeyError("thread_not_found")
            if thread.archived_at is not None:
                raise RuntimeError("media_reference_thread_archived")
            row = MediaReferenceUploadRow(id=identifier(), owner_id=owner_id, thread_id=thread_id,
                name=value.name, kind=value.kind, mime=value.mime, size=value.size,
                sha256=value.sha256, object_key=object_key, public_url=public_url,
                status="pending", expires_at=expires_at, created_at=now(), updated_at=now())
            db.add(row)
        return self._media_reference_upload_view(row)


    async def get_media_reference_upload(self, upload_id: str, owner_id: str) -> MediaReferenceUploadView | None:
        async with self.connections() as db:
            row = (await db.execute(select(MediaReferenceUploadRow).where(
                MediaReferenceUploadRow.id == upload_id,
                MediaReferenceUploadRow.owner_id == owner_id))).scalar_one_or_none()
        return self._media_reference_upload_view(row) if row else None


    async def finish_media_reference_upload(self, upload_id: str, owner_id: str,
                                            project_file_id: str) -> MediaReferenceUploadView:
        async with self.connections.begin() as db:
            candidate = (await db.execute(select(MediaReferenceUploadRow).where(
                MediaReferenceUploadRow.id == upload_id,
                MediaReferenceUploadRow.owner_id == owner_id))).scalar_one_or_none()
            if not candidate:
                raise KeyError("media_reference_upload_not_found")
            thread = (await db.execute(select(ThreadRow).where(
                ThreadRow.id == candidate.thread_id,
                ThreadRow.owner_id == owner_id).with_for_update())).scalar_one_or_none()
            if not thread:
                raise KeyError("thread_not_found")
            row = (await db.execute(select(MediaReferenceUploadRow).where(
                MediaReferenceUploadRow.id == upload_id,
                MediaReferenceUploadRow.owner_id == owner_id).with_for_update())).scalar_one_or_none()
            if not row:
                raise KeyError("media_reference_upload_not_found")
            if row.status == "completed":
                return self._media_reference_upload_view(row)
            if thread.archived_at is not None:
                raise RuntimeError("media_reference_thread_archived")
            if row.status != "pending" or utc_datetime(row.expires_at) <= now():
                raise RuntimeError("media_reference_upload_expired")
            row.status = "completed"; row.project_file_id = project_file_id; row.updated_at = now()
        return self._media_reference_upload_view(row)


    @staticmethod
    def _media_reference_asset(row: MediaReferenceAssetRow) -> dict:
        return {"sha256": row.sha256, "kind": row.kind, "mime": row.mime, "size": row.size,
            "object_ref": row.object_ref, "object_key": row.object_key, "public_url": row.public_url,
            "boss_status": row.boss_status, "ark_asset_id": row.ark_asset_id,
            "ark_status": row.ark_status, "ark_error": row.ark_error,
            "lease_owner": row.lease_owner, "lease_expires_at": row.lease_expires_at}


    async def ensure_media_reference_asset(self, reference: dict, *, object_key: str,
                                           public_url: str, boss_status: str = "pending") -> dict:
        async with self.connections.begin() as db:
            values={"sha256":reference["sha256"],"kind":reference["kind"],
                "mime":reference["mime"],"size":reference["size"],
                "object_ref":reference["object_ref"],"object_key":object_key,
                "public_url":public_url,"boss_status":boss_status,"ark_status":"pending",
                "created_at":now(),"updated_at":now()}
            dialect=db.bind.dialect.name
            if dialect=="postgresql":
                statement=postgresql_insert(MediaReferenceAssetRow).values(**values).on_conflict_do_nothing(
                    index_elements=[MediaReferenceAssetRow.sha256])
            elif dialect=="sqlite":
                statement=sqlite_insert(MediaReferenceAssetRow).values(**values).on_conflict_do_nothing(
                    index_elements=[MediaReferenceAssetRow.sha256])
            else:raise RuntimeError("media_reference_asset_idempotency_requires_supported_database")
            await db.execute(statement)
            row=await db.get(MediaReferenceAssetRow,reference["sha256"],with_for_update=True)
            if not row:raise RuntimeError("media_reference_asset_insert_missing")
            expected=(reference["kind"],reference["mime"],reference["size"],
                reference["object_ref"],object_key,public_url)
            actual=(row.kind,row.mime,row.size,row.object_ref,row.object_key,row.public_url)
            if actual!=expected:raise RuntimeError("media_reference_asset_identity_conflict")
        return self._media_reference_asset(row)


    async def get_media_reference_asset(self, digest: str) -> dict | None:
        async with self.connections() as db:
            row = await db.get(MediaReferenceAssetRow, digest)
        return self._media_reference_asset(row) if row else None


    async def claim_media_reference_asset(self, digest: str, owner: str,
                                          lease_seconds: int = 60) -> tuple[dict, bool]:
        async with self.connections.begin() as db:
            timestamp = now()
            row=(await db.execute(update(MediaReferenceAssetRow).where(
                MediaReferenceAssetRow.sha256==digest,
                or_(MediaReferenceAssetRow.lease_owner=="",
                    MediaReferenceAssetRow.lease_expires_at.is_(None),
                    MediaReferenceAssetRow.lease_expires_at<timestamp,
                    MediaReferenceAssetRow.lease_owner==owner),
            ).values(lease_owner=owner,
                lease_expires_at=timestamp+timedelta(seconds=lease_seconds),updated_at=timestamp)
            .returning(MediaReferenceAssetRow))).scalar_one_or_none()
            if row:return self._media_reference_asset(row),True
            row=await db.get(MediaReferenceAssetRow,digest)
            if not row:raise KeyError("media_reference_asset_not_found")
            return self._media_reference_asset(row),False


    async def update_media_reference_asset(self, digest: str, owner: str, *,
                                           boss_status: str | None = None,
                                           ark_asset_id: str | None = None,
                                           ark_status: str | None = None,
                                           ark_error: str | None = None,
                                           release: bool = False) -> dict:
        async with self.connections.begin() as db:
            row = await db.get(MediaReferenceAssetRow, digest, with_for_update=True)
            if not row or row.lease_owner != owner:
                raise RuntimeError("media_reference_asset_lease_lost")
            if boss_status is not None: row.boss_status = boss_status
            if ark_asset_id is not None: row.ark_asset_id = ark_asset_id
            if ark_status is not None: row.ark_status = ark_status
            if ark_error is not None: row.ark_error = ark_error[:200]
            row.updated_at = now()
            if release:
                row.lease_owner = ""; row.lease_expires_at = None
            else:
                row.lease_expires_at = now() + timedelta(seconds=60)
        return self._media_reference_asset(row)


    @staticmethod
    def _media_voice_clone(row: MediaVoiceCloneRow) -> dict:
        return {"id": row.id, "owner_id": row.owner_id, "sha256": row.sha256,
            "mime": row.mime, "size": row.size, "object_ref": row.object_ref,
            "voice_id": row.voice_id, "provider_file_id": row.provider_file_id,
            "revision": row.revision, "status": row.status, "error_code": row.error_code,
            "lease_owner": row.lease_owner, "lease_expires_at": row.lease_expires_at,
            "last_used_at": utc_datetime(row.last_used_at) if row.last_used_at else None}


    async def ensure_media_voice_clone(self, owner_id: str, reference: dict) -> dict:
        try:
            async with self.connections.begin() as db:
                row = (await db.execute(select(MediaVoiceCloneRow).where(
                    MediaVoiceCloneRow.owner_id == owner_id,
                    MediaVoiceCloneRow.sha256 == reference["sha256"]).with_for_update())).scalar_one_or_none()
                if row:
                    expected = (reference["mime"], reference["size"], reference["object_ref"])
                    if (row.mime, row.size, row.object_ref) != expected:
                        raise RuntimeError("media_voice_clone_identity_conflict")
                else:
                    identity = hashlib.sha256((owner_id + ":" + reference["sha256"] + ":1").encode()).hexdigest()
                    row = MediaVoiceCloneRow(id=identifier(), owner_id=owner_id,
                        sha256=reference["sha256"], mime=reference["mime"], size=reference["size"],
                        object_ref=reference["object_ref"], voice_id="updream_" + identity[:32],
                        revision=1, status="pending", created_at=now(), updated_at=now())
                    db.add(row)
        except IntegrityError:
            row = None
        if row is None:
            async with self.connections() as db:
                row = (await db.execute(select(MediaVoiceCloneRow).where(
                    MediaVoiceCloneRow.owner_id == owner_id,
                    MediaVoiceCloneRow.sha256 == reference["sha256"]))).scalar_one_or_none()
            if not row:
                raise RuntimeError("media_voice_clone_create_conflict")
            expected = (reference["mime"], reference["size"], reference["object_ref"])
            if (row.mime, row.size, row.object_ref) != expected:
                raise RuntimeError("media_voice_clone_identity_conflict")
        return self._media_voice_clone(row)


    async def get_media_voice_clone(self, owner_id: str, digest: str) -> dict | None:
        async with self.connections() as db:
            row = (await db.execute(select(MediaVoiceCloneRow).where(
                MediaVoiceCloneRow.owner_id == owner_id,
                MediaVoiceCloneRow.sha256 == digest))).scalar_one_or_none()
        return self._media_voice_clone(row) if row else None


    async def claim_media_voice_clone(self, owner_id: str, digest: str, lease_owner: str,
                                      lease_seconds: int = 120) -> tuple[dict, bool]:
        async with self.connections.begin() as db:
            row = (await db.execute(select(MediaVoiceCloneRow).where(
                MediaVoiceCloneRow.owner_id == owner_id,
                MediaVoiceCloneRow.sha256 == digest).with_for_update())).scalar_one_or_none()
            if not row:
                raise KeyError("media_voice_clone_not_found")
            timestamp = now()
            if (row.lease_owner and row.lease_owner != lease_owner and row.lease_expires_at
                    and utc_datetime(row.lease_expires_at) > timestamp):
                return self._media_voice_clone(row), False
            row.lease_owner = lease_owner
            row.lease_expires_at = timestamp + timedelta(seconds=lease_seconds)
            row.updated_at = timestamp
        return self._media_voice_clone(row), True


    async def update_media_voice_clone(self, owner_id: str, digest: str, lease_owner: str, *,
                                       status: str | None = None,
                                       provider_file_id: str | None = None,
                                       error_code: str | None = None,
                                       release: bool = False) -> dict:
        async with self.connections.begin() as db:
            row = (await db.execute(select(MediaVoiceCloneRow).where(
                MediaVoiceCloneRow.owner_id == owner_id,
                MediaVoiceCloneRow.sha256 == digest).with_for_update())).scalar_one_or_none()
            if not row or row.lease_owner != lease_owner:
                raise RuntimeError("media_voice_clone_lease_lost")
            if status is not None: row.status = status
            if provider_file_id is not None: row.provider_file_id = provider_file_id
            if error_code is not None: row.error_code = error_code[:100]
            row.updated_at = now()
            if release:
                row.lease_owner = ""; row.lease_expires_at = None
            else:
                row.lease_expires_at = now() + timedelta(seconds=120)
        return self._media_voice_clone(row)


    async def touch_media_voice_clone(self, owner_id: str, digest: str, voice_id: str) -> bool:
        async with self.connections.begin() as db:
            row = (await db.execute(select(MediaVoiceCloneRow).where(
                MediaVoiceCloneRow.owner_id == owner_id, MediaVoiceCloneRow.sha256 == digest,
                MediaVoiceCloneRow.voice_id == voice_id).with_for_update())).scalar_one_or_none()
            if not row:
                return False
            row.status = "active"; row.error_code = ""; row.last_used_at = now(); row.updated_at = now()
            return True


    async def rotate_media_voice_clone(self, owner_id: str, digest: str,
                                       lease_owner: str) -> dict:
        async with self.connections.begin() as db:
            row = (await db.execute(select(MediaVoiceCloneRow).where(
                MediaVoiceCloneRow.owner_id == owner_id,
                MediaVoiceCloneRow.sha256 == digest).with_for_update())).scalar_one_or_none()
            if not row or row.lease_owner != lease_owner:
                raise RuntimeError("media_voice_clone_lease_lost")
            row.revision += 1
            identity = hashlib.sha256((owner_id + ":" + digest + ":" + str(row.revision)).encode()).hexdigest()
            row.voice_id = "updream_" + identity[:32]
            row.provider_file_id = ""; row.status = "pending"; row.error_code = ""
            row.last_used_at = None; row.updated_at = now()
            row.lease_expires_at = now() + timedelta(seconds=120)
        return self._media_voice_clone(row)


    async def invalidate_media_voice_clone(self, owner_id: str, digest: str, voice_id: str) -> bool:
        async with self.connections.begin() as db:
            row = (await db.execute(select(MediaVoiceCloneRow).where(
                MediaVoiceCloneRow.owner_id == owner_id, MediaVoiceCloneRow.sha256 == digest,
                MediaVoiceCloneRow.voice_id == voice_id).with_for_update())).scalar_one_or_none()
            if not row:
                return False
            row.status = "pending"; row.provider_file_id = ""; row.error_code = "voice_not_available"
            row.last_used_at = None; row.lease_owner = ""; row.lease_expires_at = None; row.updated_at = now()
            return True

