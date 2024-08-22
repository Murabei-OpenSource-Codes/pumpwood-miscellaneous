"""Google Storage Cloud."""
import io
import pandas as pd
from typing import Callable, List
from google.cloud import storage
from google.cloud.storage.blob import Blob
from google.resumable_media import requests
from google.resumable_media.requests import ChunkedDownload
from google.auth.transport.requests import AuthorizedSession
from ._general import (
    FlaskStreamUploadWrapper, FlaskStreamDownloadWrapper)
from pumpwood_communication import exceptions


class PumpWoodGoogleBucket():
    """Class to make comunication with Google Cloud Storage."""

    def __init__(self, bucket_name):
        """
        __init__.

        Args:
            bucket_name (str): Name of the bucket.
        """
        self._client = storage.Client()
        self._bucket_name = bucket_name
        self._google_bucket = self._client.bucket(bucket_name)

    def check_file_exists(self, file_path: str) -> bool:
        """
        Check if file exists.

        Args:
            file_path [str]:
                Path to file in storage.
        Returns:
            Return a boolean value checking if the file exists on storage.
        """
        blob = self._google_bucket.blob(file_path)
        return blob.exists()

    def list_files(self, path: str = "") -> List[str]:
        """
        List file at storage path.

        Args:
            path [str]:
                Path of the storage to list files.
        Returns [List[str]]:
            List of all files under path (sub-folders).
        """
        blobs = self._google_bucket.list_blobs(prefix=path)
        return [b.name for b in blobs]

    def write_file(self, file_path: str, data: bytes, if_exists: str = 'fail',
                   content_type='text/plain') -> str:
        """
        Write file on Google Bucket.

        Args:
            file_path [str]:
                Path to save the file.
            data [str]:
                File content in bytes.
            if_exists [str]:
                if_exists must be in 'overwrite',
                'overwrite_streaming' (stream file to overwrite if exists),
                'append_breakline' (append content with a breakline between),
                'append' (append content without break line),
                'fail' (fail if file exists)]
            content_type [str]:
                Mime-type of the content.
        Returns:
            A string with bucket path.
        Raises:
            PumpWoodForbidden:
                'There is a file with same name on bucket'. If
                `if_exists='fail'`, it will raise error if bucket has a
                file with same name.
            PumpWoodNotImplementedError:
                'if_exists must be in {if_exists}'. If `if_exists` is not
                implemented.
        """
        if_exists_opt = [
            'overwrite', 'overwrite_streaming', 'append_breakline',
            'append', 'fail']
        if if_exists in if_exists_opt:
            msg = "if_exists must be in {}".format(if_exists_opt)
            raise exceptions.PumpWoodNotImplementedError(msg)

        blob = self._google_bucket.blob(file_path)
        if blob.exists() and if_exists == 'fail':
            msg = 'There is a file with same name on bucket'
            raise exceptions.PumpWoodForbidden(msg)
        elif if_exists in ['append_breakline', 'append']:
            old_text = self.read_file(file_path)
            old_text = old_text + b'\n' \
                if if_exists == 'append_breakline' else old_text
            data = old_text + data

        blob.upload_from_string(
            data, content_type=content_type)
        return file_path

    def write_file_stream(self, file_path: str, data_stream: io.BytesIO,
                          chunk_size: int = 1024 * 1024):
        """
        Write file as stream to google cloud.

        Args:
            file_path (str): Path to save the stream in Google Storage Bucket.
            data_stream (io.BytesIO): Data stream.
        Kwargs:
            chunk_size(int): Size of the chuck to be transmited, default for
                1024 * 1024 (1Mb).
        Return (dict):
            Return the file path used to save data ("file_path" key) and the
            total of bytes that were transmited.
        Raises:
            No particular raises at this function.
        """
        blob = self._google_bucket.blob(file_path)
        file_stream_obj = GoogleStorageUploadFileStream(
            client=self._client, blob=blob, bucket_name=self._bucket_name,
            chunk_size=chunk_size, data_stream=data_stream)
        while True:
            finished = file_stream_obj.write()
            if finished:
                break

        bytes_uploaded = file_stream_obj.get_bytes_uploaded()
        return {
            "file_path": file_path, "bytes_uploaded": bytes_uploaded}

    def get_read_file_iterator(self,  file_path: str,
                               chunk_size: int = 1024 * 1024) -> Callable:
        """
        Return an iterator to stream download data in flask.

        Args:
            file_path (str): Storage path.
        Kwargs:
            chunk_size (int): Chunk size in bytes, default to 1Mb.
        Raises:
            No specific raises.
        """
        blob = self._google_bucket.blob(file_path)
        file_stream_obj = GoogleStorageDownloadFileStream(
            client=self._client, blob=blob, bucket_name=self._bucket_name,
            chunk_size=chunk_size)
        return file_stream_obj.read_iterator()

    def delete_file(self, file_path: str) -> bool:
        """
        Delete file from storage.

        Args:
            file_path [str]:
                Path of the file that will be deleted
        Returns:
            Return True if file is deleted.
        Raises:
            PumpWoodObjectDoesNotExist:
                'file_path %s does not exist' % file_path. Indicates that
                file does not exists on storage.
        """
        blob = self._google_bucket.blob(file_path)
        if not blob.exists():
            msg = 'file_path %s does not exist' % file_path
            raise exceptions.PumpWoodObjectDoesNotExist(msg)
        blob.delete()
        return True

    def read_file(self, file_path: str) -> dict:
        """
        Read file content from storage.

        Args:
            file_path [str]:
                Path of the file at the storage.
        Returns:
            A dictionary with keys.
            - **data:** Binary data from the file.
            - **content_type:** Content type at the storage, usually not
                correct.
        Raises:
            PumpWoodObjectDoesNotExist:
                'file_path {file_path} does not exist'. Raise error when file
                is not found at the storage.
        """
        blob = self._google_bucket.blob(file_path)
        if not blob.exists():
            msg = 'file_path %s does not exist' % file_path
            raise exceptions.PumpWoodObjectDoesNotExist(msg)

        data = blob.download_as_string()
        content_type = blob.content_type
        return {'data': data, 'content_type': content_type}

    def download_to_file(self, file_path: str, file_obj):
        """
        Download file from storage and save it in a local path.

        Args:
            file_obj (any): A file like object.
            local_path (str): Local path to save file.
        Kwargs:
            No Kwargs
        Raises:
            PumpWoodObjectDoesNotExist:
                'file_path {file_path} does not exist'. Raise error when file
                is not found at the storage.
        """
        blob = self._google_bucket.blob(file_path)
        if not blob.exists():
            msg = 'file_path %s does not exist' % file_path
            raise exceptions.PumpWoodObjectDoesNotExist(msg)

        blob = self._google_bucket.blob(file_path, chunk_size=262144*5)
        blob.download_to_file(file_obj)
        file_obj.close()

    def get_file_hash(self, file_path: str):
        """
        Return file hash calculated at cloud storage provider.

        Args:
            file_path (str): File path.
        Kwargs:
            No Kwargs.
        Returns:
            str: Hash of the file.
        Raises:
            PumpWoodObjectDoesNotExist:
                "file_path {file_path} does not exist", If file is not found
                on storage.
        """
        blob = self._google_bucket.blob(file_path)
        if not blob.exists():
            msg = 'file_path %s does not exist' % file_path
            raise exceptions.PumpWoodObjectDoesNotExist(msg)

        blob.reload()
        return blob.md5_hash


class GoogleStorageUploadFileStream:
    """Create a upload file stream for Google Storage."""

    def __init__(self, client: storage.Client, blob: Blob,
                 bucket_name: str, chunk_size: int, data_stream: io.BytesIO):
        self._client = client
        self._transport = AuthorizedSession(
            credentials=self._client._credentials)
        self._chunk_size = chunk_size
        self._blob = blob

        url_template = 'https://www.googleapis.com/upload/storage/v1/b/' + \
            '{bucket_name}/o?uploadType=resumable'
        url = url_template.format(bucket_name=bucket_name)

        self._request = requests.ResumableUpload(
            upload_url=url, chunk_size=self._chunk_size)

        stream = FlaskStreamUploadWrapper(data_stream)
        self._request.initiate(
            transport=self._transport,
            content_type='application/octet-stream',
            stream=stream, stream_final=False,
            metadata={'name': self._blob.name})

    def write(self):
        self._request.transmit_next_chunk(self._transport)
        return self._request.finished

    def get_bytes_uploaded(self):
        return self._request.bytes_uploaded


class GoogleStorageDownloadFileStream:
    """Create a download file stream for Google Storage."""

    def __init__(self, client: storage.Client, blob: Blob,
                 bucket_name: str, chunk_size: int):
        self._client = client
        self._transport = AuthorizedSession(
            credentials=self._client._credentials)
        self._chunk_size = chunk_size
        self._blob = blob
        self.bytes_position = 0
        url = self._blob._get_download_url(client=client)

        self._stream = FlaskStreamDownloadWrapper()
        self._request = ChunkedDownload(
            url, chunk_size, self._stream)

    def read_iterator(self):
        """Create an interator to download data from Google Cloud."""
        while True:
            self._request.consume_next_chunk(self._transport)
            last_chunck = self._stream.get_last_chunk()
            yield last_chunck
            if self._request.finished:
                break
