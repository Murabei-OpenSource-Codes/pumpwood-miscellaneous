"""Google Storage Cloud."""
import os
import io
from typing import Callable
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from ._general import (
    FlaskStreamUploadWrapper, FlaskStreamDownloadWrapper)


class PumpWoodAzureBlob():
    """Class to make comunication with Azure Blob Storage."""

    def __init__(self, bucket_name: str):
        """
        __init__.

        Args:
            bucket_name (str): Name of the bucket.
        """
        # Collection AZURE_STORAGE_CONNECTION_STRING from environment
        # variables
        connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        blob_service = BlobServiceClient.from_connection_string(
            connect_str)
        self._client = blob_service.get_container_client(container=bucket_name)
        if not self._client.exists():
            Exception("Container [%s] does not exists" % bucket_name)
        self._bucket_name = bucket_name

    def check_file_exists(self, file_path: str) -> bool:
        """
        Check if file exists.

        Args:
            file_path [str]: Path to file in storage.

        Return:
            Return a boolean value checking if the file exists on storage.
        """
        blob = self._client.get_blob_client(blob=file_path)
        return blob.exists()

    def write_file(self, file_path: str, data: bytes, if_exists: str = 'fail',
                   content_type='text/plain') -> str:
        """
        Write file on Azure.

        Args:
            file_path (str): Path to save the file.
            data (str): File content in bytes.
            if_exists (str): if_exists must be in 'overide',
                'overide_streaming' (stream file to overide if exists),
                'append_breakline' (append content with a breakline between),
                'append' (append content without break line),
                'fail' (fail if file exists)]
            content_type (str): Mime-type of the content.
        """
        if_exists_opt = ['overide', 'overide_streaming', 'append_breakline',
                         'append', 'fail']
        if if_exists in if_exists_opt:
            Exception("if_exists must be in {}".format(if_exists_opt))

        blob = self._client.get_blob_client(blob=file_path)
        if blob.exists() and if_exists == 'fail':
            raise Exception('There is a file with same name on bucket')

        elif if_exists in ['append_breakline', 'append']:
            old_text = blob.download_blob().readall()
            old_text = old_text + b'\n' \
                if if_exists == 'append_breakline' else old_text
            data = old_text + data

        blob.upload_blob(data)
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
        blob = self._client.get_blob_client(blob=file_path)
        blob.upload_blob(data_stream)
        properties = blob.get_blob_properties()

        return {
            "file_path": file_path, "bytes_uploaded": properties['size']}

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
        iterator = blob.download_blob()

        return file_stream_obj.read_iterator()

    def delete_file(self, file_path: str):
        blob = self._google_bucket.blob(file_path)
        if not blob.exists():
            Exception('file_path %s does not exist' % file_path)
        blob.delete()
        return True

    def read_file(self, file_path: str):
        blob = self._google_bucket.blob(file_path)
        if not blob.exists():
            Exception('file_path %s does not exist' % file_path)
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
            No specific raises.
        """
        blob = self._google_bucket.blob(file_path, chunk_size=262144*5)
        blob.download_to_file(file_obj)
        file_obj.close()


class AzureStorageUploadFileStream:
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


class AzureStorageDownloadFileStream:
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
