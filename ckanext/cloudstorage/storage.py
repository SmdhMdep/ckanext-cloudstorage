#!/usr/bin/env python
# -*- coding: utf-8 -*-
import cgi
import mimetypes
from urllib.parse import urljoin
from datetime import datetime, timedelta
from time import time
from tempfile import SpooledTemporaryFile

from ckan.plugins import toolkit
from ckan.lib.uploader import ResourceUpload as DefaultResourceUpload
from ckan import model
from ckan.lib import munge

from libcloud.storage.types import Provider, ObjectDoesNotExistError
from libcloud.storage.providers import get_driver

from .config import config

from werkzeug.datastructures import FileStorage as FlaskFileStorage


c = toolkit.c

ALLOWED_UPLOAD_TYPES = (cgi.FieldStorage, FlaskFileStorage)


def _get_underlying_file(wrapper):
    if isinstance(wrapper, FlaskFileStorage):
        return wrapper.stream
    return wrapper.file


class CloudStorage(object):
    def __init__(self):
        self.driver_name = config.driver_name
        self._driver_options = config.driver_options
        self.container_name = config.container_name

        if 'S3' in self.driver_name and 'key' not in self.driver_options:
            self._authenticate_with_aws()

        self.driver = get_driver(
            getattr(
                Provider,
                self.driver_name
            )
        )(**self.driver_options)
        self._container = None

    def _authenticate_with_aws(self):
        """
        TTL max 900 seconds for IAM role session
        https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use.html#id_roles_use_view-role-max-session
        """
        import boto3
        session = boto3.Session(
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
        )
        credentials = session.get_credentials()
        current_credentials = credentials.get_frozen_credentials()
        self.driver_options = {
            **self.driver_options,
            'key': current_credentials.access_key,
            'secret': current_credentials.secret_key,
            'token': current_credentials.token,
            'expires': datetime.fromtimestamp(time() + 900).strftime('%Y-%m-%dT%H:%M:%SZ')
        }

        self.driver = get_driver(
            getattr(
                Provider,
                self.driver_name
            )
        )(**self.driver_options)
        self._container = None

    @property
    def container(self):
        """
        Return the currently configured libcloud container.
        """
        if self.driver_options.get('expires'):
            expires = datetime.strptime(self.driver_options['expires'], "%Y-%m-%dT%H:%M:%SZ")
            if expires < datetime.utcnow():
                self._authenticate_with_aws()

        if self._container is None:
            self._container = self.driver.get_container(
                container_name=self.container_name
            )

        return self._container

    @property
    def driver_options(self):
        return self._driver_options

    @driver_options.setter
    def driver_options(self, value):
        self._driver_options = value

    @property
    def can_use_advanced_azure(self) -> bool:
        """
        `True` if the `azure-storage` module is installed and
        ckanext-cloudstorage has been configured to use Azure, otherwise
        `False`.
        """
        if self.driver_name == 'AZURE_BLOBS':
            try:
                from azure import storage as _
                return True
            except ImportError:
                pass

        return False

    @property
    def can_use_advanced_aws(self) -> bool:
        """
        `True` if the `boto3` module is installed and ckanext-cloudstorage has
        been configured to use Amazon S3, otherwise `False`.
        """
        if 'S3' in self.driver_name:
            try:
                import boto3 as _
                return True
            except ImportError:
                pass

        return False


STORAGE_PATH_FIELD_NAME = "cloud_storage_key"


class ResourceCloudStorage(CloudStorage):
    def __init__(self, resource):
        """
        Support for uploading resources to any storage provider
        implemented by the apache-libcloud library.

        :param resource: The resource dict.
        """
        # NOTE: The resource parameter can be either:
        # 1. an actual resource dict (when instantiated by the ckan framework)
        # 2. a dict with one key, `multipart_name`. Used to call `get_path`.
        # 3. an empty dict. Used to get access to the underlying driver instance.
        super().__init__()

        self.use_secure_urls = config.use_secure_urls
        self.leave_files = config.leave_files
        self.guess_mimetype = config.guess_mimetype

        self.filename = None
        self.old_filename = None
        self.file = None
        self.resource = resource

        upload_field_storage = resource.pop('upload', None)
        self._clear = resource.pop('clear_upload', None)
        multipart_name = resource.pop('multipart_name', None)

        # Check to see if a file has been provided
        if bool(upload_field_storage) and isinstance(upload_field_storage, ALLOWED_UPLOAD_TYPES):
            self.filename = munge.munge_filename(upload_field_storage.filename)
            self.file_upload = _get_underlying_file(upload_field_storage)
            resource['url'] = self.filename
            resource['url_type'] = 'upload'
        elif multipart_name and self.can_use_advanced_aws:
            # This means that file was successfully uploaded and stored
            # at cloud.
            # Currently implemented just AWS version
            resource['url'] = munge.munge_filename(multipart_name)
            resource['url_type'] = 'upload'
        elif self._clear and resource.get('id'):
            # Apparently, this is a created-but-not-commited resource whose
            # file upload has been canceled. We're copying the behaviour of
            # ckaenxt-s3filestore here.
            old_resource = model.Session.query(model.Resource).get(resource['id'])
            self.old_filename = old_resource.url
            resource['url_type'] = ''

    @property
    def _fallback_uploader(self):
        if getattr(self, '_fallback_uploader_instance', None) is None:
            self._fallback_uploader_instance = DefaultResourceUpload(self.resource)
        return self._fallback_uploader_instance

    def _get_cloud_storage_path(self, resource_id):
        return toolkit.get_action('resource_show')(
            {'model': model, 'ignore_auth': True},
            {'id': resource_id},
        ).get(STORAGE_PATH_FIELD_NAME)

    def get_path(self, resource_id):
        path = self._get_cloud_storage_path(resource_id)
        return path or self._fallback_uploader.get_path(resource_id)

    def upload(self, id, max_size=10):
        """
        Complete the file upload, or clear an existing upload.

        :param id: The resource_id.
        :param max_size: Ignored.
        """
        storage_path = self._get_cloud_storage_path(id)
        if storage_path is None:
            return self._fallback_uploader.upload(id, max_size)
        elif self.filename:
            if self.can_use_advanced_azure:
                return self._upload_using_azure(storage_path)
            else:
                return self._upload_using_libcloud(storage_path)
        elif self._clear and self.old_filename and not self.leave_files:
            # This is only set when a previously-uploaded file is replaced
            # by a link. We want to delete the previously-uploaded file.
            if self.can_use_advanced_aws:
                return self._delete_using_aws(storage_path, id, max_size)
            else:
                return self._delete_using_libcloud(storage_path, id, max_size)

    def get_url_from_filename(self, rid, filename, content_type=None, expires_in=None):
        """
        Retrieve a publicly accessible URL for the given resource_id
        and filename.

        .. note::

            Works for Azure and any libcloud driver that implements
            support for get_object_cdn_url (ex: AWS S3).

        :param rid: The resource ID.
        :param filename: The resource filename.
        :param content_type: Optionally a Content-Type header.
        :param expires_in: Optional number of seconds until the pre-signed url expires
                        only used with AWS.

        :returns: Externally accessible URL or None.
        """
        # Find the key the file *should* be stored at.
        path = self._get_cloud_storage_path(rid)
        if path is None:
            return f'file://{self._fallback_uploader.get_path(rid)}'
        # If advanced azure features are enabled, generate a temporary
        # shared access link instead of simply redirecting to the file.
        elif self.can_use_advanced_azure and self.use_secure_urls:
            return self._get_url_from_filename_using_azure(path)
        elif self.can_use_advanced_aws and self.use_secure_urls:
            expiry = expires_in or 3600
            return self._get_url_from_filename_using_aws(content_type, path, expiry)
        else:
            return self._get_url_from_filename_using_libcloud(path)

    def _upload_using_azure(self, storage_path: str):
        from azure.storage import blob as azure_blob
        from azure.storage.blob.models import ContentSettings

        blob_service = azure_blob.BlockBlobService(
            self.driver_options['key'],
            self.driver_options['secret']
        )
        content_settings = None
        if self.guess_mimetype:
            content_type, _ = mimetypes.guess_type(self.filename)
            if content_type:
                content_settings = ContentSettings(
                    content_type=content_type
                )

        return blob_service.create_blob_from_stream(
            container_name=self.container_name,
            blob_name=storage_path,
            stream=self.file_upload,
            content_settings=content_settings
        )

    def _upload_using_libcloud(self, storage_path: str):
        # If it's temporary file, we'd better convert it
        # into FileIO. Otherwise libcloud will iterate
        # over lines, not over chunks and it will really
        # slow down the process for files that consist of
        # millions of short line
        if isinstance(self.file_upload, SpooledTemporaryFile):
            self.file_upload.rollover()
            try:
                # extract underlying file
                file_upload_iter = self.file_upload._file.detach()
            except AttributeError:
                # It's python2
                file_upload_iter = self.file_upload._file
        else:
            file_upload_iter = iter(self.file_upload)

        extra = None
        if self.guess_mimetype:
            content_type, _ = mimetypes.guess_type(self.filename)
            extra = {'content_type': content_type} if content_type else None

        self.container.upload_object_via_stream(
            iterator=file_upload_iter,
            object_name=storage_path,
            extra=extra,
        )

    def _delete_using_aws(self, storage_path: str, id: str, max_size: int):
        import boto3
        client = boto3.client(
            's3',
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
            region_name=config.aws_bucket_region,
        )
        client.delete_object(Bucket=self.container_name, Key=storage_path)

    def _delete_using_libcloud(self, storage_path: str, id: str, max_size: int):
        try:
            cloud_object = self.container.get_object(storage_path)
            self.container.delete_object(cloud_object)
        except ObjectDoesNotExistError:
            try:
                self._fallback_uploader.upload(id, max_size)
            except:
                # It's possible for the object to have already been deleted, or
                # for it to not yet exist in a committed state due to an
                # outstanding lease.
                return

    def _get_url_from_filename_using_azure(self, path: str):
        from azure.storage import blob as azure_blob

        blob_service = azure_blob.BlockBlobService(
            self.driver_options['key'],
            self.driver_options['secret']
        )

        return blob_service.make_blob_url(
            container_name=self.container_name,
            blob_name=path,
            sas_token=blob_service.generate_blob_shared_access_signature(
                container_name=self.container_name,
                blob_name=path,
                expiry=datetime.utcnow() + timedelta(hours=1),
                permission=azure_blob.BlobPermissions.READ
            )
        )

    def _get_url_from_filename_using_aws(self, content_type: str, path: str, expires_in: int):
        import boto3
        client = boto3.client(
            's3',
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
            region_name=config.aws_bucket_region,
        )
        params = {'Bucket': self.container_name, 'Key': path}
        if content_type:
            params['ResponseContentType'] = content_type

        return client.generate_presigned_url(
            ClientMethod='get_object',
            Params=params,
            ExpiresIn=expires_in,
        )

    def _get_url_from_filename_using_libcloud(self, path: str):
        try:
            obj = self.container.get_object(path)
        except ObjectDoesNotExistError:
            return None

        try:
            return self.driver.get_object_cdn_url(obj)
        except NotImplementedError:
            if 'S3' in self.driver_name:
                base_url = f'https://{self.driver.connection.host}'
                return urljoin(base_url, f'{self.container_name}/{path}')
            # This extra 'url' property isn't documented anywhere, sadly. See azure_blobs.py:_xml_to_object for more.
            elif 'url' in obj.extra:
                return obj.extra['url']
            raise
