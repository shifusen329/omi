"""
MinIO-backed drop-in for `google.cloud.storage.Client`.

Duck-types just enough of the google-cloud-storage surface actually used by
utils/other/storage.py so the swap is a single line at the top of that module
(``storage_client = MinioStorageClient()``). No call-site edits required.

Bucket auto-creation: on first construction, any bucket name present in the
set of `BUCKET_*` env vars is `head_bucket`'d and created if missing. This
matches the "create on boot" behaviour approved for self-host.
"""
import datetime
import io
import logging
import os
from typing import IO, Iterable, List, Optional

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from google.cloud.exceptions import NotFound

logger = logging.getLogger(__name__)


def _s3_client():
    endpoint = os.environ.get('MINIO_ENDPOINT') or os.environ.get('S3_ENDPOINT_URL')
    access_key = os.environ.get('MINIO_ACCESS_KEY') or os.environ.get('AWS_ACCESS_KEY_ID')
    secret_key = os.environ.get('MINIO_SECRET_KEY') or os.environ.get('AWS_SECRET_ACCESS_KEY')
    region = os.environ.get('MINIO_REGION') or os.environ.get('AWS_REGION') or 'us-east-1'
    if not endpoint or not access_key or not secret_key:
        raise RuntimeError('MinIO storage backend requested but MINIO_ENDPOINT / MINIO_ACCESS_KEY / MINIO_SECRET_KEY are not set.')
    return boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=Config(signature_version='s3v4', s3={'addressing_style': 'path'}),
    )


_BUCKET_ENV_VARS = (
    'BUCKET_SPEECH_PROFILES',
    'BUCKET_BACKUPS',
    'BUCKET_PLUGINS_LOGOS',
    'BUCKET_POSTPROCESSING',
    'BUCKET_MEMORIES_RECORDINGS',
    'BUCKET_PRIVATE_CLOUD_SYNC',
    'BUCKET_TEMPORAL_SYNC_LOCAL',
    'BUCKET_APP_THUMBNAILS',
    'BUCKET_CHAT_FILES',
    'BUCKET_DESKTOP_UPDATES',
)


def ensure_configured_buckets(client) -> None:
    """Head each configured BUCKET_* env var; create any that don't exist."""
    seen: set[str] = set()
    for var in _BUCKET_ENV_VARS:
        name = os.environ.get(var)
        if not name or name in seen:
            continue
        seen.add(name)
        try:
            client.head_bucket(Bucket=name)
        except ClientError as e:
            code = e.response.get('Error', {}).get('Code', '')
            if code in ('NoSuchBucket', '404', 'NotFound'):
                try:
                    client.create_bucket(Bucket=name)
                    logger.info('minio: created bucket %s', name)
                except ClientError as e2:
                    logger.warning('minio: failed to create bucket %s: %s', name, e2)
            else:
                logger.debug('minio: head_bucket %s -> %s', name, code)


class _MinioBlob:
    """Duck-types google.cloud.storage.Blob for the handful of methods used by storage.py."""

    __slots__ = ('_s3', 'bucket_name', 'name', '_size', '_metadata', '_time_created', 'cache_control')

    def __init__(self, s3, bucket_name: str, name: str, size: Optional[int] = None):
        self._s3 = s3
        self.bucket_name = bucket_name
        self.name = name
        self._size = size
        self._metadata: Optional[dict] = None
        self._time_created: Optional[datetime.datetime] = None
        self.cache_control: Optional[str] = None

    # ------------------------------------------------------------------
    # Read / query
    # ------------------------------------------------------------------

    @property
    def size(self) -> Optional[int]:
        return self._size

    @property
    def metadata(self):
        return self._metadata

    @metadata.setter
    def metadata(self, value):
        self._metadata = dict(value) if value else None

    @property
    def time_created(self) -> Optional[datetime.datetime]:
        return self._time_created

    def exists(self) -> bool:
        try:
            self._s3.head_object(Bucket=self.bucket_name, Key=self.name)
            return True
        except ClientError as e:
            code = e.response.get('Error', {}).get('Code', '')
            if code in ('404', 'NoSuchKey', 'NotFound'):
                return False
            raise

    def reload(self) -> None:
        try:
            r = self._s3.head_object(Bucket=self.bucket_name, Key=self.name)
        except ClientError as e:
            if e.response.get('Error', {}).get('Code', '') in ('404', 'NoSuchKey', 'NotFound'):
                raise NotFound(self.name) from e
            raise
        self._size = r.get('ContentLength')
        self._metadata = dict(r.get('Metadata') or {})
        lm = r.get('LastModified')
        if lm:
            # boto3 returns a datetime already aware
            self._time_created = lm

    # ------------------------------------------------------------------
    # Upload
    # ------------------------------------------------------------------

    def _extra_args(self, content_type: Optional[str]) -> dict:
        extra: dict = {}
        if content_type:
            extra['ContentType'] = content_type
        if self.cache_control:
            extra['CacheControl'] = self.cache_control
        if self._metadata:
            # S3 metadata values must be strings
            extra['Metadata'] = {k: str(v) for k, v in self._metadata.items()}
        return extra

    def upload_from_filename(self, file_path: str, content_type: Optional[str] = None) -> None:
        try:
            self._s3.upload_file(file_path, self.bucket_name, self.name, ExtraArgs=self._extra_args(content_type))
        except ClientError as e:
            raise RuntimeError(f'MinIO upload_from_filename {self.name}: {e}') from e

    def upload_from_string(self, data, content_type: Optional[str] = None) -> None:
        if isinstance(data, str):
            data = data.encode('utf-8')
        kwargs = {
            'Bucket': self.bucket_name,
            'Key': self.name,
            'Body': data,
        }
        extra = self._extra_args(content_type)
        kwargs.update(extra)
        self._s3.put_object(**kwargs)

    # ------------------------------------------------------------------
    # Download
    # ------------------------------------------------------------------

    def download_to_filename(self, file_path: str) -> None:
        try:
            self._s3.download_file(self.bucket_name, self.name, file_path)
        except ClientError as e:
            if e.response.get('Error', {}).get('Code', '') in ('404', 'NoSuchKey'):
                raise NotFound(self.name) from e
            raise

    def download_as_bytes(self) -> bytes:
        try:
            r = self._s3.get_object(Bucket=self.bucket_name, Key=self.name)
        except ClientError as e:
            if e.response.get('Error', {}).get('Code', '') in ('404', 'NoSuchKey'):
                raise NotFound(self.name) from e
            raise
        return r['Body'].read()

    # ------------------------------------------------------------------
    # Delete / mutate
    # ------------------------------------------------------------------

    def delete(self) -> None:
        try:
            self._s3.delete_object(Bucket=self.bucket_name, Key=self.name)
        except ClientError as e:
            if e.response.get('Error', {}).get('Code', '') in ('404', 'NoSuchKey'):
                raise NotFound(self.name) from e
            raise

    def make_public(self) -> None:
        # MinIO supports public buckets via policy, but we keep things simple:
        # callers that need public access should use signed URLs via
        # generate_signed_url(). No-op here; storage.py already logs if this fails.
        return None

    def generate_signed_url(
        self,
        version: str = 'v4',
        expiration: Optional[datetime.timedelta] = None,
        method: str = 'GET',
    ) -> str:
        secs = int(expiration.total_seconds()) if expiration else 3600
        operation = 'get_object' if method.upper() == 'GET' else 'put_object'
        return self._s3.generate_presigned_url(
            operation,
            Params={'Bucket': self.bucket_name, 'Key': self.name},
            ExpiresIn=secs,
        )

    # ------------------------------------------------------------------
    # Streaming write — used by upload_audio_chunks_batch
    # ------------------------------------------------------------------

    def open(self, mode: str = 'wb', content_type: Optional[str] = None):
        if mode != 'wb':
            raise NotImplementedError(f'_MinioBlob.open mode {mode!r} not supported')
        return _MinioWriteStream(self, content_type=content_type)


class _MinioWriteStream(io.RawIOBase):
    """Buffering write stream that uploads on close. Mirrors blob.open('wb')."""

    def __init__(self, blob: _MinioBlob, content_type: Optional[str] = None):
        super().__init__()
        self._blob = blob
        self._content_type = content_type
        self._buffer = io.BytesIO()
        self._closed = False

    def writable(self) -> bool:  # type: ignore[override]
        return True

    def write(self, data) -> int:  # type: ignore[override]
        if isinstance(data, (bytes, bytearray, memoryview)):
            return self._buffer.write(data)
        raise TypeError(f'_MinioWriteStream.write expects bytes, got {type(data).__name__}')

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._buffer.seek(0)
        payload = self._buffer.getvalue()
        self._blob.upload_from_string(payload, content_type=self._content_type)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


class _MinioBucket:
    __slots__ = ('_s3', 'name')

    def __init__(self, s3, name: str):
        self._s3 = s3
        self.name = name

    def blob(self, name: str) -> _MinioBlob:
        return _MinioBlob(self._s3, self.name, name)

    def list_blobs(self, prefix: Optional[str] = None) -> Iterable[_MinioBlob]:
        paginator = self._s3.get_paginator('list_objects_v2')
        kwargs = {'Bucket': self.name}
        if prefix:
            kwargs['Prefix'] = prefix
        for page in paginator.paginate(**kwargs):
            for obj in page.get('Contents') or []:
                blob = _MinioBlob(self._s3, self.name, obj['Key'], size=obj.get('Size'))
                blob._time_created = obj.get('LastModified')
                yield blob


class MinioStorageClient:
    """Replaces google.cloud.storage.Client. Only exposes `.bucket(name)`."""

    def __init__(self):
        self._s3 = _s3_client()
        try:
            ensure_configured_buckets(self._s3)
        except Exception as e:
            logger.warning('minio bucket bootstrap skipped: %s', e)

    def bucket(self, name: str) -> _MinioBucket:
        return _MinioBucket(self._s3, name)
