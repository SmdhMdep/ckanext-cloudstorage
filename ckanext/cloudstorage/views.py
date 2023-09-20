# -*- coding: utf-8 -*-
import logging

from flask import Blueprint
from ckan.views import resource
from ckan import model
from ckan.plugins import toolkit

import ckanext.cloudstorage.utils as utils


logger = logging.getLogger(__name__)
cloudstorage = Blueprint('cloudstorage', __name__, url_defaults={u'package_type': u'dataset'})


@cloudstorage.route('/dataset/<id>/resource/<resource_id>/download')
@cloudstorage.route('/dataset/<id>/resource/<resource_id>/download/<filename>')
def download(id, resource_id, filename=None, package_type='dataset'):
    return utils.resource_download(id, resource_id, filename)

@cloudstorage.route('/dataset/<id>/resource/new')
def resource_new(package_type, id):
    context = {
        u'model': model,
        u'session': model.Session,
        u'user': toolkit.g.user,
        u'auth_user_obj': toolkit.g.userobj,
    }

    try:
        pkg_dict = toolkit.get_action(u'package_show')(context, {u'id': id})
    except toolkit.ObjectNotFound:
        return resource.CreateView().get(package_type, id)

    if pkg_dict['state'].startswith('draft') and len(pkg_dict['resources']) == 1:
        return resource.EditView().get(package_type, id, pkg_dict['resources'][0]['id'])
    else:
        return resource.CreateView().get(package_type, id)


def get_blueprints():
    return [cloudstorage]
