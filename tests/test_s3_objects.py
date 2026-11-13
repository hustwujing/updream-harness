from io import BytesIO

import pytest

from sandbox_harness.objects import S3ObjectStore


class S3:
    def __init__(self):
        self.values = {}

    def put_object(self, *, Bucket, Key, Body, IfNoneMatch):
        assert Bucket == "shared-bucket"
        assert Key.startswith("sandbox-harness/objects/")
        assert IfNoneMatch == "*"
        self.values[Key] = bytes(Body)

    def head_object(self, *, Bucket, Key):
        return {"ContentLength":len(self.values[Key])}

    def get_object(self, *, Bucket, Key, Range=None):
        data = self.values[Key]
        if Range:
            first,last=Range.removeprefix("bytes=").split("-");data=data[int(first):int(last)+1]
        return {"ContentLength": len(data), "Body": BytesIO(data)}


def test_s3_objects_use_prefix_and_verify_content():
    client = S3()
    store = S3ObjectStore(bucket="shared-bucket", prefix="sandbox-harness/", client=client)
    reference, digest = store.put(b"artifact")

    assert reference == "sha256:" + digest
    assert list(client.values) == [f"sandbox-harness/objects/{digest[:2]}/{digest[2:]}"]
    assert store.get(reference, maximum=100) == b"artifact"
    assert store.size(reference,maximum=100)==8
    assert b"".join(store.iter_bytes(reference,maximum=100,start=1,end=3,chunk_size=2))==b"rti"


def test_s3_objects_enforce_read_limit_and_prefix():
    client = S3()
    store = S3ObjectStore(bucket="shared-bucket", prefix="sandbox-harness", client=client)
    reference, _ = store.put(b"artifact")

    with pytest.raises(ValueError, match="byte limit"):
        store.get(reference, maximum=2)
    with pytest.raises(ValueError, match="prefix"):
        S3ObjectStore(bucket="shared-bucket", prefix="../other", client=client)
