#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os

from ckanext.cloudstorage.config import config


def use_secure_urls():
    return all([
        config.use_secure_urls,
        # Currently implemented just AWS version
        'S3' in config.driver_name
    ])


STREAM_RESOURCE_TYPE = 'stream'


def is_stream_resource(resource):
    return resource.get('resource_type') == STREAM_RESOURCE_TYPE


def get_package_cloud_storage_key(package: dict) -> str:
    return os.path.join(
        '1',
        package['organization']['name'],
        package['cloud_storage_key_segment'],
    )
