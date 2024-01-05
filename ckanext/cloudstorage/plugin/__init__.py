#!/usr/bin/env python
# -*- coding: utf-8 -*-
from ckan import plugins
from ckanext.cloudstorage import storage
from ckanext.cloudstorage import helpers
import ckanext.cloudstorage.logic.action.presigned_url as presigned_url_action
import ckanext.cloudstorage.logic.action.multipart as m_action
import ckanext.cloudstorage.logic.auth.multipart as m_auth

if plugins.toolkit.check_ckan_version(min_version='2.9.0'):
    from ckanext.cloudstorage.plugin.flask_plugin import MixinPlugin
else:
    from ckanext.cloudstorage.plugin.pylons_plugin import MixinPlugin

from ..resource_object_key import ResourceObjectKey
from ..storage import STORAGE_PATH_FIELD_NAME
from .. import model
from ..validators import (
    valid_resource_name,
    default_cloud_storage_key_package_segment,
    no_update_to_package_cloud_storage_key_segment,
)


class CloudStoragePlugin(MixinPlugin, plugins.SingletonPlugin, plugins.toolkit.DefaultDatasetForm):
    plugins.implements(plugins.IUploader)
    plugins.implements(plugins.IRoutes, inherit=True)
    plugins.implements(plugins.IConfigurable)
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IActions)
    plugins.implements(plugins.ITemplateHelpers)
    plugins.implements(plugins.IAuthFunctions)
    plugins.implements(plugins.IResourceController, inherit=True)
    plugins.implements(plugins.IValidators)
    plugins.implements(plugins.IDatasetForm)

    # IConfigurer

    def update_config(self, config):
        plugins.toolkit.add_template_directory(config, '../templates')
        plugins.toolkit.add_public_directory(config, '../public')
        plugins.toolkit.add_resource('../fanstatic/scripts', 'cloudstorage-js')

    # ITemplateHelpers

    def get_helpers(self):
        return dict(
            cloudstorage_use_secure_urls=helpers.use_secure_urls,
            is_stream_resource=helpers.is_stream_resource,
            get_package_cloud_storage_key=helpers.get_package_cloud_storage_key,
            can_generate_presigned_url=helpers.can_generate_presigned_url,
        )

    def configure(self, config):
        required_keys = (
            'ckanext.cloudstorage.driver',
            'ckanext.cloudstorage.driver_options',
            'ckanext.cloudstorage.container_name'
        )

        for rk in required_keys:
            if config.get(rk) is None:
                raise RuntimeError(
                    'Required configuration option {0} not found.'.format(
                        rk
                    )
                )

        model.create_tables()

    def get_resource_uploader(self, data_dict):
        # We provide a custom Resource uploader.
        return storage.ResourceCloudStorage(data_dict)

    def get_uploader(self, upload_to, old_filename=None):
        # We don't provide misc-file storage (group images for example)
        # Returning None here will use the default Uploader.
        return None

    # IActions

    def get_actions(self):
        return {
            'cloudstorage_initiate_multipart': m_action.initiate_multipart,
            'cloudstorage_upload_multipart': m_action.upload_multipart,
            'cloudstorage_finish_multipart': m_action.finish_multipart,
            'cloudstorage_abort_multipart': m_action.abort_multipart,
            'cloudstorage_check_multipart': m_action.check_multipart,
            'cloudstorage_clean_multipart': m_action.clean_multipart,
            'resource_create_presigned_url': presigned_url_action.create_presigned_url,
        }

    # IAuthFunctions

    def get_auth_functions(self):
        return {
            'cloudstorage_initiate_multipart': m_auth.initiate_multipart,
            'cloudstorage_upload_multipart': m_auth.upload_multipart,
            'cloudstorage_finish_multipart': m_auth.finish_multipart,
            'cloudstorage_abort_multipart': m_auth.abort_multipart,
            'cloudstorage_check_multipart': m_auth.check_multipart,
            'cloudstorage_clean_multipart': m_auth.clean_multipart,
        }

    # IResourceController

    def _get_storage_path(self, context, resource):
        context = dict(context, ignore_auth=True)
        package = plugins.toolkit.get_action('package_show')(context, {
            'id': resource['package_id']
        })
        return ResourceObjectKey.from_resource(package, resource).raw

    def before_create(self, context, resource):
        if resource.get('url_type') == 'upload' and resource.get(STORAGE_PATH_FIELD_NAME) is None:
            resource[STORAGE_PATH_FIELD_NAME] = self._get_storage_path(context, resource)

    def before_update(self, context, current, resource):
        if STORAGE_PATH_FIELD_NAME not in resource and STORAGE_PATH_FIELD_NAME in current:
            resource[STORAGE_PATH_FIELD_NAME] = current[STORAGE_PATH_FIELD_NAME]
        elif resource.get('upload') and resource.get('url_type') == 'upload' or resource.get('multipart_name'):
            # new upload initiated, use the current resource as the updated resource object might be missing some fields
            resource[STORAGE_PATH_FIELD_NAME] = self._get_storage_path(context, current)

    def before_delete(self, context, id_dict, resources):
        # let's get all info about our resource. It somewhere in resources
        # but if there is some possibility that it isn't(magic?) we skip
        resource = next((r for r in resources if r['id'] == id_dict['id']), None)
        if resource is not None and resource['url_type'] == 'upload':
            resource = dict(resource, clear_upload=True)
            uploader = self.get_resource_uploader(resource)
            uploader.upload(resource['id'])

    # IValidators

    def get_validators(self):
        return {
            valid_resource_name.__name__: valid_resource_name
        }

    # IDatasetForm

    def package_types(self):
        return []

    def is_fallback(self):
        return True

    def show_package_schema(self):
        schema = super().show_package_schema()
        schema.update({
            'cloud_storage_key_segment': [
                plugins.toolkit.get_converter('convert_from_extras'),
                default_cloud_storage_key_package_segment,
            ],
        })
        return schema

    def create_package_schema(self):
        schema = super().create_package_schema()
        schema['resources']['name'].append(valid_resource_name)
        schema.update({
            'cloud_storage_key_segment': [
                plugins.toolkit.get_validator('ignore_missing'),
                plugins.toolkit.get_converter('convert_to_extras'),
            ]
        })
        return schema

    def update_package_schema(self):
        schema = super().update_package_schema()
        schema['resources']['name'].append(valid_resource_name)
        schema.update({
            'cloud_storage_key_segment': [
                plugins.toolkit.get_converter('ignore_missing'),
                no_update_to_package_cloud_storage_key_segment,
                plugins.toolkit.get_converter('convert_to_extras'),
            ]
        })
        return schema
