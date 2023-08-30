from ast import literal_eval
from typing import Optional

from ckan.plugins import toolkit


class Config:
    @property
    def driver_name(self) -> str:
        """
        The name of the driver (ex: AZURE_BLOBS, S3) that ckanext-cloudstorage
        is configured to use.


        .. note::

            This value is used to lookup the apache-libcloud driver to use
            based on the Provider enum.
        """
        return toolkit.config['ckanext.cloudstorage.driver']

    @property
    def driver_options(self) -> dict:
        return literal_eval(toolkit.config.get('ckanext.cloudstorage.driver_options', '{}'))

    @property
    def container_name(self) -> str:
        """
        The name of the container (also called buckets on some providers)
        ckanext-cloudstorage is configured to use.
        """
        return toolkit.config["ckanext.cloudstorage.container_name"]

    @property
    def aws_access_key_id(self) -> Optional[str]:
        return self.driver_options.get('key')

    @property
    def aws_secret_access_key(self) -> Optional[str]:
        return self.driver_options.get('secret')

    @property
    def aws_bucket_region(self) -> Optional[str]:
        return self.driver_options.get('region')

    @property
    def use_secure_urls(self) -> bool:
        """
        `True` if ckanext-cloudstroage is configured to generate secure
        one-time URLs to resources, `False` otherwise.
        """
        return toolkit.asbool(toolkit.config.get('ckanext.cloudstorage.use_secure_urls', True))

    @property
    def leave_files(self) -> bool:
        """
        `True` if ckanext-cloudstorage is configured to leave files on the
        provider instead of removing them when a resource/package is deleted,
        otherwise `False`.
        """
        return toolkit.asbool(toolkit.config.get('ckanext.cloudstorage.leave_files', False))

    @property
    def queue_region(self) -> str:
        return toolkit.config["ckanext.cloudstorage.sync.queue_region"]

    @property
    def queue_url(self) -> str:
        return toolkit.config["ckanext.cloudstorage.sync.queue_url"]

    @property
    def use_fake_events(self) -> bool:
        return toolkit.asbool(toolkit.config.get("ckanext.cloudstorage.sync.use_fake_events", False))

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

    @property
    def guess_mimetype(self) -> bool:
        """
        `True` if ckanext-cloudstorage is configured to guess mime types,
        `False` otherwise.
        """
        return toolkit.asbool(toolkit.config.get('ckanext.cloudstorage.guess_mimetype', False))


config = Config()
