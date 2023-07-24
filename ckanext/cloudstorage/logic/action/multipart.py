#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import datetime

from ckan.plugins.toolkit import config
from ckan.plugins import plugin_loaded
from sqlalchemy.orm.exc import NoResultFound
import ckan.model as model
import ckan.lib.helpers as h
import ckan.plugins.toolkit as toolkit

from ckanext.cloudstorage.storage import ResourceCloudStorage
from ckanext.cloudstorage.model import MultipartUpload, MultipartPart
from werkzeug.datastructures import FileStorage as FlaskFileStorage

log = logging.getLogger(__name__)


def _get_underlying_file(wrapper):
    if isinstance(wrapper, FlaskFileStorage):
        return wrapper.stream
    return wrapper.file


def _get_max_multipart_lifetime():
    value = float(config.get('ckanext.cloudstorage.max_multipart_lifetime', 7))
    return datetime.timedelta(value)


def _get_object_url(uploader, name):
    return '/' + uploader.container_name + '/' + name


def _delete_multipart(upload, uploader):
    resp = uploader.driver.connection.request(
        _get_object_url(uploader, upload.name) + '?uploadId=' + upload.id,
        method='DELETE'
    )
    if not resp.success():
        raise toolkit.ValidationError(resp.error)

    upload.delete()
    upload.commit()
    return resp


def _save_part_info(n, etag, upload):
    try:
        part = model.Session.query(MultipartPart).filter(
            MultipartPart.n == n,
            MultipartPart.upload == upload).one()
    except NoResultFound:
        part = MultipartPart(n, etag, upload)
    else:
        part.etag = etag
    part.save()
    return part


def check_multipart(context, data_dict):
    """Check whether unfinished multipart upload already exists.

    :param context:
    :param data_dict: dict with required `id`
    :returns: None or dict with `upload` - existing multipart upload info
    :rtype: NoneType or dict

    """

    h.check_access('cloudstorage_check_multipart', data_dict)
    id = toolkit.get_or_bust(data_dict, 'id')
    try:
        upload = model.Session.query(MultipartUpload).filter_by(
            resource_id=id).one()
    except NoResultFound:
        return
    upload_dict = upload.as_dict()
    upload_dict['parts'] = model.Session.query(MultipartPart).filter(
        MultipartPart.upload == upload).count()
    return {'upload': upload_dict}


def initiate_multipart(context, data_dict):
    """Initiate new Multipart Upload.

    :param context:
    :param data_dict: dict with required keys:
        id: resource's id
        name: filename
        size: filesize

    :returns: MultipartUpload info
    :rtype: dict

    """

    h.check_access('cloudstorage_initiate_multipart', data_dict)
    id, name, size = toolkit.get_or_bust(data_dict, ['id', 'name', 'size'])
    user_id = None
    if context['auth_user_obj']:
        user_id = context['auth_user_obj'].id

    res_dict = toolkit.get_action('resource_show')(
        context.copy(), {'id': data_dict.get('id')})

    # Delay handling in archiver
    res_dict['upload_in_progress'] = True
    toolkit.get_action('resource_patch')(context.copy(), res_dict)

    uploader = ResourceCloudStorage({'multipart_name': name})
    res_name = uploader.path_from_filename(id, name)

    old_upload = MultipartUpload.by_name(res_name)
    if old_upload is not None:
        _delete_multipart(old_upload, uploader)

    for old_upload in model.Session.query(MultipartUpload).filter_by(resource_id=id):
        _delete_multipart(old_upload, uploader)

    upload_object = MultipartUpload(
        uploader.driver._initiate_multipart(
            container=uploader.container,
            object_name=res_name
        ),
        id,
        res_name,
        size,
        name,
        user_id
    )
    upload_object.save()
    return upload_object.as_dict()


def upload_multipart(context, data_dict):
    h.check_access('cloudstorage_upload_multipart', data_dict)
    upload_id, part_number, part_content = toolkit.get_or_bust(
        data_dict,
        ['uploadId', 'partNumber', 'upload']
    )

    uploader = ResourceCloudStorage({})
    upload = model.Session.query(MultipartUpload).get(upload_id)
    data = _get_underlying_file(part_content).read()

    resp = uploader.driver.connection.request(
        _get_object_url(
            uploader,
            upload.name
        ),
        params={
            'uploadId': upload_id,
            'partNumber': part_number
        },
        method='PUT',
        data=data,
        headers={
            'Content-Length': len(data)
        }
    )

    if resp.status != 200:
        raise toolkit.ValidationError('Upload failed: part %s' % part_number)

    _save_part_info(part_number, resp.headers['etag'], upload)

    return {
        'partNumber': part_number,
        'ETag': resp.headers['etag']
    }


def finish_multipart(context, data_dict):
    """Called after all parts had been uploaded.

    Triggers call to `_commit_multipart` which will convert separate uploaded
    parts into single file

    :param context:
    :param data_dict: dict with required key `uploadId` - id of Multipart Upload that should be finished
    :param keepDraft: true if dataset should be kept as a draft after the file is uploaded
    :returns: None
    :rtype: NoneType

    """

    h.check_access('cloudstorage_finish_multipart', data_dict)
    upload_id = toolkit.get_or_bust(data_dict, 'uploadId')
    save_action = data_dict.get('save_action', False)
    upload = model.Session.query(MultipartUpload).get(upload_id)
    chunks = [
        (part.n, part.etag)
        for part in model.Session.query(MultipartPart).filter_by(
            upload_id=upload_id).order_by(MultipartPart.n)
    ]
    uploader = ResourceCloudStorage({})

    try:
        obj = uploader.container.get_object(upload.name)
        obj.delete()
    except Exception:
        pass

    uploader.driver._commit_multipart(
        container=uploader.container,
        object_name=upload.name,
        upload_id=upload_id,
        chunks=chunks
    )

    upload.delete()
    upload.commit()

    res_dict = toolkit.get_action('resource_show')(
        context.copy(), {'id': data_dict.get('id')})

    # Change draft state of package to active
    if save_action and save_action == "go-metadata":
        try:
            pkg_dict = toolkit.get_action('package_show')(
                context.copy(), {'id': res_dict['package_id']})

            if pkg_dict['state'] == 'draft' and not toolkit.asbool(data_dict.get('keepDraft')):
                toolkit.get_action('package_patch')(
                    dict(context.copy(), allow_state_change=True),
                    dict(id=pkg_dict['id'], state='active')
                )

        except Exception as e:
            log.error(e)

    # Trigger handling in archiver
    res_dict.pop('upload_in_progress', None)
    toolkit.get_action('resource_update')(context.copy(), res_dict)

    # Submit to datapusher, uses custom config variable which is not triggered automatically in ckan
    if plugin_loaded('datapusher'):
        resource_format = res_dict.get('format')
        supported_formats = toolkit.config.get(
            'ckanext.cloudstorage.datapusher.formats'
        )

        submit = (
            resource_format
            and resource_format.lower() in supported_formats
            and res_dict.get('url_type') != u'datapusher'
        )

        if submit:
            toolkit.get_action(u'datapusher_submit')(
                context, {
                    u'resource_id': res_dict['id']
                }
            )

    return {'commited': True}


def abort_multipart(context, data_dict):
    h.check_access('cloudstorage_abort_multipart', data_dict)
    id = toolkit.get_or_bust(data_dict, ['id'])
    uploader = ResourceCloudStorage({})

    resource_uploads = MultipartUpload.resource_uploads(id)

    aborted = []
    for upload in resource_uploads:
        _delete_multipart(upload, uploader)

        aborted.append(upload.id)

    model.Session.commit()

    return aborted


def clean_multipart(context, data_dict):
    """Clean old multipart uploads.

    :param context:
    :param data_dict:
    :returns: dict with:
        removed - amount of removed uploads.
        total - total amount of expired uploads.
        errors - list of errors raised during deletion. Appears when
        `total` and `removed` are different.
    :rtype: dict

    """

    h.check_access('cloudstorage_clean_multipart', data_dict)
    uploader = ResourceCloudStorage({})
    delta = _get_max_multipart_lifetime()
    oldest_allowed = datetime.datetime.utcnow() - delta

    uploads_to_remove = model.Session.query(MultipartUpload).filter(
        MultipartUpload.initiated < oldest_allowed
    )

    result = {
        'removed': 0,
        'total': uploads_to_remove.count(),
        'errors': []
    }

    for upload in uploads_to_remove:
        try:
            _delete_multipart(upload, uploader)
        except toolkit.ValidationError as e:
            result['errors'].append(e.error_summary)
        else:
            result['removed'] += 1

    return result
