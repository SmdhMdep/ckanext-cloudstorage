#!/usr/bin/env python
# -*- coding: utf-8 -*-
from ckanext.cloudstorage.config import config


def use_secure_urls():
    return all([
        config.use_secure_urls,
        # Currently implemented just AWS version
        'S3' in config.driver_name
    ])
