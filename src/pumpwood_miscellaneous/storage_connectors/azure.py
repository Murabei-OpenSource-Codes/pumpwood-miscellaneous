"""Google Storage Cloud."""
import os
import io
from typing import Callable
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from ._general import (
    FlaskStreamUploadWrapper, FlaskStreamDownloadWrapper)


class PumpWoodAzureStorage():
    """Class to make communication with Azure Blob Storage."""

    def __init__(self, bucket_name: str):
        """
        __init__.

        Args:
            bucket_name (str): Name of the bucket.
        """
        # Collection AZURE_STORAGE_CONNECTION_STRING from environment
        # variables
        connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        if connect_str is None:
            raise Exception("AZURE_STORAGE_CONNECTION_STRING not set")
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

    def list_files(self, path: str = "") -> list:
        """
        List file at storage path.

        Args:
            path [str]: Path of the storage to list files.
        Return [list]:
            List of all files under path (sub-folders).
        """
        return [
            x['name']
            for x in self._client.list_blobs(name_starts_with=path)]

    def write_file(self, file_path: str, data: bytes, if_exists: str = 'fail',
                   content_type='text/plain') -> str:
        """
        Write file on Azure.

        Args:
            file_path (str): Path to save the file.
            data (str): File content in bytes.
            if_exists (str): if_exists must be in 'overwrite',
                'overwrite_streaming' (stream file to overwrite if exists),
                'append_breakline' (append content with a breakline between),
                'append' (append content without break line),
                'fail' (fail if file exists)]
            content_type (str): Mime-type of the content.
        """
        if_exists_opt = [
            'overwrite', 'overwrite_streaming', 'append_breakline',
            'append', 'fail']
        if if_exists in if_exists_opt:
            Exception("if_exists must be in {}".format(if_exists_opt))

        blob = self._client.get_blob_client(blob=file_path)
        blob_exists = blob.exists()
        if blob_exists and if_exists == 'fail':
            raise Exception('There is a file with same name on bucket')
        elif if_exists in ['append_breakline', 'append']:
            old_text = blob.download_blob().readall()
            old_text = old_text + b'\n' \
                if if_exists == 'append_breakline' else old_text
            data = old_text + data

        # Removendo o blob caso tenha mesmo nome
        if blob_exists:
            blob.delete_blob()

        blob.upload_blob(data)
        return file_path

    def write_file_stream(self, file_path: str, data_stream: io.BytesIO):
        """
        Write file as stream to google cloud.

        Args:
            file_path (str): Path to save the stream in Google Storage Bucket.
            data_stream (io.BytesIO): Data stream.
        Return (dict):
            Return the file path used to save data ("file_path" key) and the
            total of bytes that were transmited.
        Raises:
            No particular raises at this function.
        """
        blob = self._client.get_blob_client(blob=file_path)
        blob_exists = blob.exists()

        # Removendo o blob caso tenha mesmo nome
        if blob_exists:
            blob.delete_blob()

        file_stream_obj = AzureStorageUploadFileStream(
            client=blob, data_stream=data_stream)
        file_stream_obj.write()

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
        download_blob = self._client.get_blob_client(
            blob=file_path).download_blob()
        return download_blob.chunks()

    def delete_file(self, file_path: str):
        blob = self._client.get_blob_client(blob=file_path)
        if not blob.exists():
            Exception('file_path %s does not exist' % file_path)
        blob.delete_blob()

        if blob.exists():
            raise Exception("Blob was not deleted from Azure")
        return True

    def read_file(self, file_path: str):
        blob = self._client.get_blob_client(blob=file_path)
        if not blob.exists():
            Exception('file_path %s does not exist' % file_path)
        data = blob.download_blob().readall()

        properties = blob.get_blob_properties()
        content_type = properties["content_settings"]["content_type"]
        return {'data': data, 'content_type': content_type}

    def download_to_file(self, file_path: str, file_obj: any):
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
        blob = self._client.get_blob_client(blob=file_path)
        download_blob = blob.download_blob()
        download_blob.download_to_stream(file_obj)
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
            Exception("file_path {file_path} does not exist")
                If file is not found on storage.
        """
        return "not implemented"


class AzureStorageUploadFileStream:
    """Create a upload file stream for Google Storage."""

    def __init__(self, blob: BlobClient, data_stream: io.BytesIO, **kwargs):
        """
        __init__.

        Args:
            blob [BlobClient]:
            data_stream [io.BytesIO]:
        Return:
            Azure Storage Upload FileStream
        """
        self._blob = blob
        self._stream = FlaskStreamUploadWrapper(data_stream)

    def write(self):
        """Write stream to cloud."""
        self._blob.upload_blob(self._stream)
        return True

    def get_bytes_uploaded(self):
        return self._stream.bytes_position


# class AzureStorageDownloadFileStream:
#     """Create a download file stream for Google Storage."""
#
#     def __init__(self, blob: Blob, bucket_name: str, chunk_size: int):
#         self._chunk_size = chunk_size
#         self._blob = blob
#         self.bytes_position = 0
#
#         self._stream = FlaskStreamDownloadWrapper()
#
#     def read_iterator(self):
#         """Create an interator to download data from Google Cloud."""
#         while True:
#             self._request.consume_next_chunk(self._transport)
#             last_chunck = self._stream.get_last_chunk()
#             yield last_chunck
#             if self._request.finished:
#                 break
