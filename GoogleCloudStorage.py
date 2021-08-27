import logging
import os

from google.cloud import storage

logger = logging.getLogger(__name__)


class GoogleCloudStorageBucket:
    def __init__(self, bucket: str):
        self.client = storage.Client()
        self.bucket = self.client.get_bucket(bucket)
        self.bucket_name = bucket

    def download(self, remote: str, local: str = None, overwrite: bool = False):
        if not local:
            local = os.path.basename(remote)

        if local.endswith("/"):
            local += os.path.basename(remote)

        try:
            os.makedirs(os.path.dirname(local))
        except FileExistsError:
            pass

        if os.path.exists(local) and not overwrite:
            raise FileExistsError(f"{local} already exists")

        blob = self.bucket.get_blob(remote)
        if not blob:
            raise FileNotFoundError(f"{remote} not found")

        logger.info(f"Downloading gs://{self.bucket_name}/{remote} to {local}")
        blob.download_to_filename(local)
        return local

    def upload(self, local: str, remote: str = None):
        if not os.path.exists(local):
            raise FileNotFoundError(f"{local} not found")

        if not remote:
            remote = os.path.basename(local)

        if remote.endswith("/"):
            remote += os.path.basename(local)

        logger.info(f"Uploading {local} to gs://{self.bucket_name}/{remote}")
        blob = self.bucket.blob(remote)
        blob.upload_from_filename(local)

    def ls(self, prefix: str = None):
        blobs = self.client.list_blobs(self.bucket, prefix=prefix)
        return [blob for blob in blobs]

    def exists(self, filename: str):
        blob = self.bucket.blob(filename)
        return blob.exists()
