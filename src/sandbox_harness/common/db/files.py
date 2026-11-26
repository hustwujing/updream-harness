"""Files persistence; transaction boundaries are preserved."""
from __future__ import annotations
import json
from sqlalchemy import (select, update)
from sqlalchemy.ext.asyncio import (AsyncSession)
from sqlalchemy.dialects.postgresql import (insert as postgresql_insert)
from sqlalchemy.dialects.sqlite import (insert as sqlite_insert)
from sandbox_harness.common.observability.observability import (observed)
from sandbox_harness.common.contracts.domain import (Artifact, ItemType, ProjectFile, TurnProjectFileSnapshot, identifier, now)
from sandbox_harness.common.contracts.job_contracts import (JobRequest, OutputDisposition)
from .models import (ArtifactRow, ItemRow, JobArtifactRow, JobRow, ProjectFileRow, ThreadRow, TurnInputRow, TurnProjectFileRow, TurnRow, UploadRow, valid_project_file_path)


class FilesTransactions:
    @observed("db.artifact.commit")
    async def add_artifact(self, turn_id: str, *, path: str, sha256: str, size: int, mime: str, object_ref: str,
                           lease_owner: str | None = None, recovery_generation: int | None = None) -> Artifact:
        value = Artifact(id=identifier(), turn_id=turn_id, path=path, sha256=sha256, size=size, mime=mime, object_ref=object_ref, created_at=now())
        async with self.connections.begin() as db:
            if lease_owner is not None:
                await self.require_lease(db, turn_id, lease_owner, recovery_generation)
            db.add(ArtifactRow(**value.model_dump()))
        return value


    @observed("db.artifact.publish")
    async def publish_artifact(self, thread_id: str, turn_id: str, *, path: str, sha256: str,
                               size: int, mime: str, object_ref: str, partial: bool,
                               lease_owner: str, recovery_generation: int) -> tuple[Artifact, ProjectFile, bool]:
        async with self.connections.begin() as db:
            await self.require_lease(db, turn_id, lease_owner, recovery_generation)
            turn = await db.get(TurnRow, turn_id, with_for_update=True)
            if not turn or turn.thread_id != thread_id:
                raise KeyError("turn_not_found")
            artifact_row = (await db.execute(select(ArtifactRow).where(
                ArtifactRow.turn_id == turn_id,
                ArtifactRow.path == path,ArtifactRow.sha256 == sha256).order_by(
                ArtifactRow.created_at,ArtifactRow.id).limit(1).with_for_update())).scalar_one_or_none()
            authoritative = None
            if artifact_row is None and not partial:
                base = select(JobArtifactRow).join(JobRow,JobRow.id==JobArtifactRow.job_id).where(
                    JobRow.thread_id==thread_id,JobArtifactRow.filename==path,
                    JobArtifactRow.output_disposition==OutputDisposition.DELIVERY.value,
                    JobArtifactRow.state=='reviewed')
                authoritative=(await db.execute(base.where(JobArtifactRow.sha256==sha256,
                    JobArtifactRow.size==size,JobArtifactRow.mime==mime).order_by(
                    JobArtifactRow.created_at,JobArtifactRow.id).limit(1).with_for_update()
                )).scalar_one_or_none()
                if authoritative is None:
                    query=base if turn.internal else base.where(JobRow.origin_turn_id==turn_id)
                    candidates=list((await db.execute(query.order_by(
                        JobArtifactRow.created_at,JobArtifactRow.id).limit(2).with_for_update()
                    )).scalars().all())
                    if len(candidates)==1:authoritative=candidates[0]
            if authoritative is not None:
                artifact_row=await db.get(ArtifactRow,authoritative.artifact_id,with_for_update=True)
                project_row,_=await self._register_project_file_locked(db,thread_id,
                    path='artifacts/'+path,source_type='artifact',source_turn_id=turn_id,
                    sha256=authoritative.sha256,size=authoritative.size,mime=authoritative.mime,
                    object_ref=authoritative.object_ref)
            else:
                project_row, project_created = await self._register_project_file_locked(db, thread_id,
                    path="artifacts/" + path, source_type="artifact", source_turn_id=turn_id,
                    sha256=sha256, size=size, mime=mime, object_ref=object_ref)
                if artifact_row is None and not project_created and project_row.source_turn_id != turn_id:
                    artifact_row = (await db.execute(select(ArtifactRow).where(
                        ArtifactRow.turn_id == project_row.source_turn_id,
                        ArtifactRow.path == path, ArtifactRow.sha256 == sha256,
                        ArtifactRow.partial == False).order_by(
                        ArtifactRow.created_at, ArtifactRow.id).limit(1).with_for_update()
                    )).scalar_one_or_none()
            created = artifact_row is None
            if created:
                artifact_row = ArtifactRow(id=identifier(), turn_id=turn_id, path=path,
                    sha256=sha256, size=size, mime=mime, object_ref=object_ref,
                    partial=partial, created_at=now())
                db.add(artifact_row)
            elif partial and not artifact_row.partial:
                artifact_row.partial = True
            artifact_items = (await db.execute(select(ItemRow).where(
                ItemRow.thread_id == thread_id, ItemRow.type == ItemType.ARTIFACT.value
            ).with_for_update())).scalars().all()
            item = next((row for row in artifact_items
                if json.loads(row.payload_json).get("artifact_id") == artifact_row.id), None)
            if item is None:
                timestamp = now(); sequence = await self.next_item_sequence(db, thread_id)
                db.add(ItemRow(id=identifier(), thread_id=thread_id, turn_id=turn_id,
                    sequence=sequence, type=ItemType.ARTIFACT.value, status="completed",
                    summary=path, payload_json=json.dumps({"artifact_id":artifact_row.id,
                        "mime":artifact_row.mime,"size":artifact_row.size,
                        "partial":artifact_row.partial}),
                    created_at=timestamp, updated_at=timestamp))
            else:
                payload = json.loads(item.payload_json)
                if payload.get("partial") is not artifact_row.partial:
                    payload["partial"] = artifact_row.partial
                    item.payload_json = json.dumps(payload); item.updated_at = now()
        artifact = Artifact.model_validate({key:getattr(artifact_row,key) for key in Artifact.model_fields})
        return artifact, self.project_file_view(project_row), created


    async def mark_turn_artifacts_partial(self, turn_id: str, *, lease_owner: str,
                                          recovery_generation: int) -> int:
        async with self.connections.begin() as db:
            await self.require_lease(db, turn_id, lease_owner, recovery_generation)
            rows = list((await db.execute(select(ArtifactRow).where(
                ArtifactRow.turn_id == turn_id,
                ArtifactRow.path.not_like("input/%")).with_for_update())).scalars().all())
            identities = {row.id for row in rows}
            for row in rows:
                row.partial = True
            if identities:
                items = list((await db.execute(select(ItemRow).where(
                    ItemRow.turn_id == turn_id,
                    ItemRow.type == ItemType.ARTIFACT.value).with_for_update())).scalars().all())
                for item in items:
                    payload = json.loads(item.payload_json)
                    if payload.get("artifact_id") in identities and payload.get("partial") is not True:
                        payload["partial"] = True
                        item.payload_json = json.dumps(payload); item.updated_at = now()
            return len(rows)


    async def artifact_by_identity(self, turn_id: str, path: str, sha256: str) -> Artifact | None:
        async with self.connections() as db:
            row = (await db.execute(select(ArtifactRow).where(
                ArtifactRow.turn_id == turn_id, ArtifactRow.path == path, ArtifactRow.sha256 == sha256,
            ).limit(1))).scalar_one_or_none()
        return Artifact.model_validate({key: getattr(row, key) for key in Artifact.model_fields}) if row else None


    async def artifacts(self, turn_id: str) -> list[Artifact]:
        async with self.connections() as db:
            rows = (await db.execute(select(ArtifactRow).where(ArtifactRow.turn_id == turn_id,
                ArtifactRow.path.not_like("input/%")).order_by(ArtifactRow.created_at))).scalars().all()
        return [Artifact.model_validate({key: getattr(row, key) for key in Artifact.model_fields}) for row in rows]


    @staticmethod
    def project_file_view(row: ProjectFileRow) -> ProjectFile:
        return ProjectFile.model_validate({key: getattr(row, key) for key in ProjectFile.model_fields})


    @staticmethod
    def turn_project_file_view(row: TurnProjectFileRow) -> TurnProjectFileSnapshot:
        return TurnProjectFileSnapshot.model_validate({
            key: getattr(row, key) for key in TurnProjectFileSnapshot.model_fields})


    @staticmethod
    async def _register_project_file_locked(db: AsyncSession, thread_id: str, *, path: str,
                                            source_type: str, source_turn_id: str, sha256: str,
                                            size: int, mime: str, object_ref: str) -> tuple[ProjectFileRow, bool]:
        await db.execute(update(ThreadRow).where(ThreadRow.id == thread_id).values(updated_at=ThreadRow.updated_at))
        latest = (await db.execute(select(ProjectFileRow).where(
            ProjectFileRow.thread_id == thread_id, ProjectFileRow.path == path,
        ).order_by(ProjectFileRow.version.desc()).limit(1).with_for_update())).scalar_one_or_none()
        if latest and latest.sha256 == sha256 and not latest.deleted:
            return latest, False
        row = ProjectFileRow(id=identifier(), thread_id=thread_id, path=path,
            version=(latest.version + 1 if latest else 1), source_type=source_type,
            source_turn_id=source_turn_id, sha256=sha256, size=size, mime=mime,
            object_ref=object_ref, created_at=now())
        db.add(row)
        return row, True


    async def register_project_file(self, thread_id: str, *, path: str, source_type: str,
                                    source_turn_id: str, sha256: str, size: int, mime: str,
                                    object_ref: str, lease_owner: str | None = None,
                                    recovery_generation: int | None = None) -> tuple[ProjectFile, bool]:
        if not valid_project_file_path(path, source_type):
            raise ValueError("Invalid managed project path")
        async with self.connections.begin() as db:
            if lease_owner is not None:
                await self.require_lease(db, source_turn_id, lease_owner, recovery_generation)
            row, created = await self._register_project_file_locked(db, thread_id, path=path,
                source_type=source_type, source_turn_id=source_turn_id, sha256=sha256,
                size=size, mime=mime, object_ref=object_ref)
        return self.project_file_view(row), created


    async def current_project_files(self, thread_id: str, owner_id: str | None = None) -> list[ProjectFile]:
        async with self.connections() as db:
            query = select(ProjectFileRow).join(ThreadRow, ThreadRow.id == ProjectFileRow.thread_id).where(
                ProjectFileRow.thread_id == thread_id)
            if owner_id is not None:
                query = query.where(ThreadRow.owner_id == owner_id)
            rows = (await db.execute(query.order_by(ProjectFileRow.path, ProjectFileRow.version.desc()))).scalars().all()
        current = {}
        for row in rows:
            current.setdefault(row.path, row)
        return [self.project_file_view(current[path]) for path in sorted(current)
            if not current[path].deleted and valid_project_file_path(current[path].path, current[path].source_type)]


    async def freeze_turn_project_files(self, turn_id: str, thread_id: str, *,
                                        lease_owner: str, recovery_generation: int) -> list[TurnProjectFileSnapshot]:
        async with self.connections.begin() as db:
            await self.require_lease(db, turn_id, lease_owner, recovery_generation)
            turn = await db.get(TurnRow, turn_id, with_for_update=True)
            if not turn or turn.thread_id != thread_id:
                raise KeyError("turn_not_found")
            existing = (await db.execute(select(TurnProjectFileRow).where(
                TurnProjectFileRow.turn_id == turn_id).order_by(TurnProjectFileRow.path))).scalars().all()
            if turn.project_files_frozen:
                return [self.turn_project_file_view(row) for row in existing]
            rows = (await db.execute(select(ProjectFileRow).where(
                ProjectFileRow.thread_id == thread_id).order_by(
                ProjectFileRow.path, ProjectFileRow.version.desc()).with_for_update())).scalars().all()
            current: dict[str, ProjectFileRow] = {}
            for row in rows:
                current.setdefault(row.path, row)
            timestamp = now()
            snapshots = []
            for path in sorted(current):
                source = current[path]
                if source.deleted or not valid_project_file_path(source.path, source.source_type):
                    continue
                snapshot = TurnProjectFileRow(id=identifier(), turn_id=turn_id,
                    project_file_id=source.id, path=source.path, version=source.version,
                    source_type=source.source_type, source_turn_id=source.source_turn_id,
                    sha256=source.sha256, size=source.size, mime=source.mime,
                    object_ref=source.object_ref, created_at=timestamp)
                db.add(snapshot); snapshots.append(snapshot)
                db.add(ArtifactRow(id=identifier(),turn_id=turn_id,path="input/"+source.path,
                    sha256=source.sha256,size=source.size,mime=source.mime,
                    object_ref=source.object_ref,created_at=timestamp))
            turn.project_files_frozen = True
        return [self.turn_project_file_view(row) for row in snapshots]


    async def turn_project_files(self, turn_id: str, owner_id: str | None = None) -> list[TurnProjectFileSnapshot]:
        async with self.connections() as db:
            query = select(TurnProjectFileRow).join(TurnRow,
                TurnRow.id == TurnProjectFileRow.turn_id).join(ThreadRow,
                ThreadRow.id == TurnRow.thread_id).where(TurnProjectFileRow.turn_id == turn_id)
            if owner_id is not None:
                query = query.where(ThreadRow.owner_id == owner_id)
            rows = (await db.execute(query.order_by(TurnProjectFileRow.path))).scalars().all()
        return [self.turn_project_file_view(row) for row in rows]


    async def turn_input_artifacts(self,turn_id:str,owner_id:str|None=None)->list[dict]:
        async with self.connections() as db:
            if owner_id is not None and not (await db.execute(select(TurnRow.id).join(
                    ThreadRow,ThreadRow.id==TurnRow.thread_id).where(TurnRow.id==turn_id,
                    ThreadRow.owner_id==owner_id))).scalar_one_or_none():return []
            query=select(TurnProjectFileRow).join(TurnRow,
                TurnRow.id==TurnProjectFileRow.turn_id).join(ThreadRow,
                ThreadRow.id==TurnRow.thread_id).where(TurnProjectFileRow.turn_id==turn_id)
            if owner_id is not None:query=query.where(ThreadRow.owner_id==owner_id)
            snapshots=(await db.execute(query.order_by(TurnProjectFileRow.path))).scalars().all()
            artifacts=(await db.execute(select(ArtifactRow).where(ArtifactRow.turn_id==turn_id,
                ArtifactRow.path.like("input/%")).order_by(
                    ArtifactRow.created_at,ArtifactRow.id))).scalars().all()
            inputs=(await db.scalars(select(TurnInputRow).where(TurnInputRow.turn_id==turn_id)
                .order_by(TurnInputRow.sequence))).all()
        by_id={row.id:row for row in artifacts}
        by_identity={}
        for artifact in artifacts:
            by_identity.setdefault((artifact.path,artifact.sha256),[]).append(artifact)
        sequence_by_id={}
        for pending in inputs:
            for attachment in json.loads(pending.attachments_json):
                artifact=by_id.get(self.input_artifact_id(pending.id,attachment['name']))
                if artifact is None and pending.status=='applied':
                    legacy=by_identity.get(('input/inbox/'+attachment['name'],attachment['sha256']),[])
                    artifact=legacy[-1] if legacy else None
                if artifact:sequence_by_id[artifact.id]=pending.sequence
        snapshots_by_path={row.path:row for row in snapshots}
        # Keep every immutable ID for existing Child grants. Path lookup in the
        # Runtime chooses the last row, so aliases follow input sequence, even
        # when an earlier input is prepared again after worker replacement.
        ordered=sorted(artifacts,key=lambda row:(row.path,sequence_by_id.get(row.id,0),
            row.created_at,row.id))
        values=[]
        for artifact in ordered:
            path=artifact.path.removeprefix('input/')
            snapshot=snapshots_by_path.get(path)
            values.append({'artifact':Artifact.model_validate({key:getattr(artifact,key)
                    for key in Artifact.model_fields}),
                'project_file_id':snapshot.project_file_id if snapshot else '',
                'project_path':path,'source_type':snapshot.source_type if snapshot else 'attachment'})
        return values


    async def project_file_history(self, thread_id: str, path: str, owner_id: str) -> list[ProjectFile]:
        async with self.connections() as db:
            rows = (await db.execute(select(ProjectFileRow).join(ThreadRow,
                ThreadRow.id == ProjectFileRow.thread_id).where(
                    ProjectFileRow.thread_id == thread_id, ProjectFileRow.path == path,
                    ThreadRow.owner_id == owner_id,
                ).order_by(ProjectFileRow.version.desc()))).scalars().all()
        return [self.project_file_view(row) for row in rows]


    async def restore_project_file_version(self, file_id: str, owner_id: str) -> tuple[ProjectFile, bool]:
        async with self.connections.begin() as db:
            source = (await db.execute(select(ProjectFileRow).join(ThreadRow,
                ThreadRow.id == ProjectFileRow.thread_id).where(
                    ProjectFileRow.id == file_id, ThreadRow.owner_id == owner_id,
                ).with_for_update())).scalar_one_or_none()
            if not source:
                raise KeyError("project_file_not_found")
            latest = (await db.execute(select(ProjectFileRow).where(
                ProjectFileRow.thread_id == source.thread_id,
                ProjectFileRow.path == source.path,
            ).order_by(ProjectFileRow.version.desc()).limit(1).with_for_update())).scalar_one()
            if not latest.deleted and latest.sha256 == source.sha256:
                return self.project_file_view(latest), False
            row = ProjectFileRow(id=identifier(), thread_id=source.thread_id, path=source.path,
                version=latest.version + 1, source_type=source.source_type,
                source_turn_id=source.source_turn_id, sha256=source.sha256, size=source.size,
                mime=source.mime, object_ref=source.object_ref, deleted=False, created_at=now())
            db.add(row)
        return self.project_file_view(row), True


    async def apply_managed_file_versions(self, thread_id: str, owner_id: str,
                                          changes: list[dict]) -> list[ProjectFile]:
        async with self.connections.begin() as db:
            thread = (await db.execute(select(ThreadRow).where(
                ThreadRow.id == thread_id, ThreadRow.owner_id == owner_id).with_for_update()
            )).scalar_one_or_none()
            if not thread:
                raise KeyError("thread_not_found")
            latest_by_path = {}
            for change in changes:
                path = change["path"]
                latest = (await db.execute(select(ProjectFileRow).where(
                    ProjectFileRow.thread_id == thread_id, ProjectFileRow.path == path,
                ).order_by(ProjectFileRow.version.desc()).limit(1).with_for_update())).scalar_one_or_none()
                expected = change.get("expected_version")
                if expected is None:
                    if latest and not latest.deleted:
                        raise RuntimeError("managed_project_version_conflict")
                elif not latest or latest.deleted or latest.version != expected:
                    raise RuntimeError("managed_project_version_conflict")
                latest_by_path[path] = latest
            result = []
            for change in changes:
                latest = latest_by_path[change["path"]]
                if change["kind"] == "missing":
                    if latest:
                        row = ProjectFileRow(id=identifier(), thread_id=thread_id,
                            path=latest.path, version=latest.version + 1,
                            source_type=latest.source_type, source_turn_id=latest.source_turn_id,
                            sha256=latest.sha256, size=latest.size, mime=latest.mime,
                            object_ref=latest.object_ref, deleted=True, created_at=now())
                        db.add(row); result.append(row)
                    continue
                row, _ = await self._register_project_file_locked(db, thread_id,
                    path=change["path"], source_type=change["source_type"], source_turn_id="",
                    sha256=change["sha256"], size=change["size"], mime=change["mime"],
                    object_ref=change["object_ref"])
                result.append(row)
        return [self.project_file_view(row) for row in result]


    async def rename_project_file(self, file_id: str, owner_id: str, new_path: str) -> ProjectFile:
        async with self.connections.begin() as db:
            source = (await db.execute(select(ProjectFileRow).join(ThreadRow,
                ThreadRow.id == ProjectFileRow.thread_id).where(
                    ProjectFileRow.id == file_id, ThreadRow.owner_id == owner_id,
                ).with_for_update())).scalar_one_or_none()
            if not source or source.deleted:
                raise KeyError("project_file_not_found")
            latest_source = (await db.execute(select(ProjectFileRow).where(
                ProjectFileRow.thread_id == source.thread_id, ProjectFileRow.path == source.path,
            ).order_by(ProjectFileRow.version.desc()).limit(1).with_for_update())).scalar_one()
            if latest_source.id != source.id:
                raise RuntimeError("project_file_version_stale")
            target = (await db.execute(select(ProjectFileRow).where(
                ProjectFileRow.thread_id == source.thread_id, ProjectFileRow.path == new_path,
            ).order_by(ProjectFileRow.version.desc()).limit(1).with_for_update())).scalar_one_or_none()
            if target and not target.deleted:
                raise RuntimeError("project_file_path_exists")
            timestamp = now()
            tombstone = ProjectFileRow(id=identifier(), thread_id=source.thread_id, path=source.path,
                version=source.version + 1, source_type=source.source_type, source_turn_id=source.source_turn_id,
                sha256=source.sha256, size=source.size, mime=source.mime, object_ref=source.object_ref,
                deleted=True, created_at=timestamp)
            row = ProjectFileRow(id=identifier(), thread_id=source.thread_id, path=new_path,
                version=(target.version + 1 if target else 1), source_type=source.source_type,
                source_turn_id=source.source_turn_id, sha256=source.sha256, size=source.size,
                mime=source.mime, object_ref=source.object_ref, deleted=False, created_at=timestamp)
            db.add_all([tombstone, row])
        return self.project_file_view(row)


    async def delete_project_file(self, file_id: str, owner_id: str) -> ProjectFile:
        async with self.connections.begin() as db:
            source = (await db.execute(select(ProjectFileRow).join(ThreadRow,
                ThreadRow.id == ProjectFileRow.thread_id).where(
                    ProjectFileRow.id == file_id, ThreadRow.owner_id == owner_id,
                ).with_for_update())).scalar_one_or_none()
            if not source or source.deleted:
                raise KeyError("project_file_not_found")
            latest = (await db.execute(select(ProjectFileRow).where(
                ProjectFileRow.thread_id == source.thread_id, ProjectFileRow.path == source.path,
            ).order_by(ProjectFileRow.version.desc()).limit(1).with_for_update())).scalar_one()
            if latest.id != source.id:
                raise RuntimeError("project_file_version_stale")
            row = ProjectFileRow(id=identifier(), thread_id=source.thread_id, path=source.path,
                version=source.version + 1, source_type=source.source_type,
                source_turn_id=source.source_turn_id, sha256=source.sha256, size=source.size,
                mime=source.mime, object_ref=source.object_ref, deleted=True, created_at=now())
            db.add(row)
        return self.project_file_view(row)


    async def get_project_file_for_owner(self, file_id: str, owner_id: str) -> ProjectFile | None:
        async with self.connections() as db:
            row = (await db.execute(select(ProjectFileRow).join(ThreadRow,
                ThreadRow.id == ProjectFileRow.thread_id).where(
                    ProjectFileRow.id == file_id, ThreadRow.owner_id == owner_id,
                ))).scalar_one_or_none()
        return self.project_file_view(row) if row else None


    async def resolve_project_file_reference(self, thread_id: str, owner_id: str, *,
                                             file_id: str = "", path: str = "") -> ProjectFile | None:
        async with self.connections() as db:
            if file_id:
                row = (await db.execute(select(ProjectFileRow).join(ThreadRow,
                    ThreadRow.id == ProjectFileRow.thread_id).where(
                        ProjectFileRow.id == file_id, ProjectFileRow.thread_id == thread_id,
                        ThreadRow.owner_id == owner_id,
                    ))).scalar_one_or_none()
                if not row:
                    return None
                latest = (await db.execute(select(ProjectFileRow).where(
                    ProjectFileRow.thread_id == thread_id, ProjectFileRow.path == row.path,
                ).order_by(ProjectFileRow.version.desc()).limit(1))).scalar_one()
                if latest.id != row.id or latest.deleted:
                    return None
            else:
                row = (await db.execute(select(ProjectFileRow).join(ThreadRow,
                    ThreadRow.id == ProjectFileRow.thread_id).where(
                        ProjectFileRow.thread_id == thread_id, ProjectFileRow.path == path,
                        ThreadRow.owner_id == owner_id,
                    ).order_by(ProjectFileRow.version.desc()).limit(1))).scalar_one_or_none()
                if not row or row.deleted:
                    return None
        return self.project_file_view(row)


    async def get_artifact(self, artifact_id: str) -> Artifact | None:
        async with self.connections() as db:
            row = await db.get(ArtifactRow, artifact_id)
        return Artifact.model_validate({key: getattr(row, key) for key in Artifact.model_fields}) if row else None


    async def get_artifact_for_owner(self, artifact_id: str, owner_id: str) -> Artifact | None:
        async with self.connections() as db:
            row = (await db.execute(select(ArtifactRow).join(TurnRow, TurnRow.id == ArtifactRow.turn_id)
                .join(ThreadRow, ThreadRow.id == TurnRow.thread_id).where(
                    ArtifactRow.id == artifact_id, ThreadRow.owner_id == owner_id))).scalar_one_or_none()
        return Artifact.model_validate({key: getattr(row, key) for key in Artifact.model_fields}) if row else None


    async def validate_job_input_artifacts(self, owner_id: str, thread_id: str,
                                           jobs: tuple[JobRequest, ...]) -> None:
        identities={item.artifact_id for job in jobs for item in job.input_artifacts}
        if not identities:return
        async with self.connections() as db:
            rows=(await db.execute(select(ArtifactRow.id).join(TurnRow,
                TurnRow.id==ArtifactRow.turn_id).join(ThreadRow,
                ThreadRow.id==TurnRow.thread_id).where(ArtifactRow.id.in_(identities),
                TurnRow.thread_id==thread_id,ThreadRow.owner_id==owner_id))).scalars().all()
        if set(rows)!=identities:raise RuntimeError('job_input_artifact_outside_scope')


    async def record_upload(self, owner_id: str, attachment) -> None:
        async with self.connections.begin() as db:
            values={"owner_id":owner_id,"object_ref":attachment.object_ref,
                "sha256":attachment.sha256,"size":attachment.size,"name":attachment.name,
                "mime":attachment.mime,"created_at":now()}
            dialect=db.bind.dialect.name
            if dialect=="postgresql":
                statement=postgresql_insert(UploadRow).values(**values).on_conflict_do_nothing(
                    index_elements=[UploadRow.owner_id,UploadRow.object_ref])
            elif dialect=="sqlite":
                statement=sqlite_insert(UploadRow).values(**values).on_conflict_do_nothing(
                    index_elements=[UploadRow.owner_id,UploadRow.object_ref])
            else:
                raise RuntimeError("upload_idempotency_requires_supported_database")
            await db.execute(statement)


    async def attachments_owned(self, owner_id: str, attachments: list) -> bool:
        if not attachments:
            return True
        async with self.connections() as db:
            for attachment in attachments:
                row = await db.get(UploadRow, (owner_id, attachment.object_ref))
                if not row or row.sha256 != attachment.sha256 or row.size != attachment.size:
                    return False
        return True

