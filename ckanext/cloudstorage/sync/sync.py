import logging
import re
from typing import Optional

import ckan.model as model
from ckan.plugins import toolkit

from ..config import config
from ..distributed_lock import distributed_lock
from ..resource_object_key import ResourceObjectKeyType
from ..utils import convert_global_package_name_to_local
from ..helpers import STREAM_RESOURCE_TYPE
from .s3_event_message import S3EventMessage, receive_s3_events, fake_receive_s3_events


logger = logging.getLogger(__name__)


def schedule_s3_sync_job():
    with distributed_lock('s3-sync-job'):
        jobs = toolkit.get_action("job_list")({"ignore_auth": True, "model": model}, {})
        sync_jobs = [job for job in jobs if job['title'] == sync_s3.__name__]
        # don't overwhelm the work queue
        if len(sync_jobs) < 10:
            logger.info("enqueuing a new sync job with %i existing jobs", len(sync_jobs))
            toolkit.enqueue_job(sync_s3, title=sync_s3.__name__)

def sync_s3():
    context = {"model": model, "session": model.Session, "ignore_auth": True, "defer_commit": True, "user": None}

    for event in _events():
        try:
            _do_sync(context, event)
        except ValueError as e:
            logger.exception("invalid s3 sync event")
            event.mark_invalid(*e.args)
        except Exception as e:
            logger.exception("unknown error while handling s3 sync event")
            event.mark_error(e)
        else:
            event.mark_received()

def _events():
    return (
        receive_s3_events(config.container_name, config.queue_region, config.queue_url, config.driver_options)
        if not config.use_fake_events else
        fake_receive_s3_events()
    )

def _do_sync(context, event: S3EventMessage):
    organization = _get_organization(dict(context), event.resource_key.organization_name)
    admin = _get_organization_admin(organization)
    event_context = dict(context, user=admin.get("id"))

    package_context = dict(event_context, for_update=True)
    package = _find_package(package_context, event.resource_key.package_name)
    current_resource = (
        _get_resource_by_name_or_none(package, event.resource_key.name)
        if package is not None else None
    )

    if event.can_apply_to(current_resource):
        logger.debug("handling key %s for %s event", event.resource_key.raw, event.type)
        _apply_event(dict(event_context), event, organization, package, current_resource)
    else:
        logger.debug("ignoring key %s for %s event", event.resource_key.raw, event.type)

def _get_organization(context, org_name: str) -> dict:
    try:
        return toolkit.get_action("organization_show")(context, dict(id=org_name))
    except toolkit.ObjectNotFound:
        raise ValueError(f"organization {org_name} does not exist")

def _get_organization_admin(org: dict) -> dict:
    for user in org["users"]:
        if user["capacity"] == "admin":
            return user
    raise ValueError(f"missing organization admin for {org['name']}")

def _find_package(context, package_name: str) -> Optional[dict]:
    try:
        return toolkit.get_action("package_show")(context, dict(id=package_name))
    except toolkit.ObjectNotFound:
        return None

def _get_resource_by_name_or_none(package: dict, name: str) -> Optional[dict]:
    for resource in package["resources"]:
        if resource["name"] == name:
            return resource
    return None

def _apply_event(
    context,
    event: S3EventMessage,
    organization: dict,
    package: Optional[dict],
    resource: Optional[dict],
):
    updated_resource = _updated_resource(event, package, resource)
    logger.debug("updated resource: %s", updated_resource)

    if package is not None and package["owner_org"] != organization["id"]:
        raise ValueError("package does not belong to the same organization.")

    if event.is_created_event():
        if package is None:
            logger.debug("creating new package %s under %s", event.resource_key.package_name, organization["id"])
            package = _new_package(event.resource_key.package_name, event.resource_key.package_segment, organization, updated_resource)
            _call_action("package_create", context, package)
        elif resource is None:
            logger.debug("creating resource %s under %s", event.resource_key.name, package["name"])
            _call_action("resource_create", context, updated_resource)
        else:
            logger.debug("updating resource %s under %s", event.resource_key.name, package["name"])
            _call_action("resource_update", context, updated_resource)
    elif package is None:
        return
    elif resource is not None:
        logger.debug("deleting resource %s under %s", event.resource_key.name, package["name"])
        delete_data = dict(id=updated_resource["id"])
        # FIXME: can't delete and update in the same transaction
        # _call_action("resource_patch", context, updated_resource)
        _call_action("resource_delete", context, delete_data)

    context["model"].repo.commit()

def _new_package(package_name: str, package_segment: str, organization: dict, resource: Optional[dict]) -> dict:
    return dict(
        name=package_name,
        title=_format_package_title(package_name),
        owner_org=organization["id"],
        private=True,
        resources=[resource] if resource is not None else [],
        cloud_storage_key_segment=package_segment,
    )

def _format_package_title(name: str):
    local_name = convert_global_package_name_to_local(name)
    readable_name = re.sub(r"[^a-zA-Z0-9]+", " ", local_name)
    return readable_name.strip().title()

def _updated_resource(
    event: S3EventMessage,
    package: Optional[dict],
    resource: Optional[dict],
) -> dict:
    package, resource = package or {}, resource or {}
    if event.is_created_event():
        extra = (
            dict(resource_type=STREAM_RESOURCE_TYPE)
            if event.resource_key.type == ResourceObjectKeyType.STREAMING
            else dict()
        )

        return dict(
            resource,
            package_id=package.get("id"),
            name=resource.get('name', event.resource_key.name),
            url=event.resource_key.name,
            url_type='upload',
            size=event.object_size,
            aws_s3_sequencer=event.object_sequencer,
            cloud_storage_key=event.resource_key.raw,
            last_modified=event.time,
            **extra,
        )
    else:
        updated_resource = dict(
            resource,
            url_type='upload',
            aws_s3_sequencer=event.object_sequencer,
            cloud_storage_key=None,
            last_modified=event.time,
        )
        updated_resource.pop('url', None)
        return updated_resource

def _call_action(action, context, data):
    toolkit.check_access(action, context, data)
    toolkit.get_action(action)(context, data)
