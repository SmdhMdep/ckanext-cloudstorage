# -*- coding: utf-8 -*-
import logging
import sys

import click
from ckan.lib.jobs import Worker

import ckanext.cloudstorage.utils as utils
from .sync import schedule_s3_sync_job


logger = logging.getLogger(__name__)


@click.group()
def cloudstorage():
    """CloudStorage management commands.
    """
    pass


@cloudstorage.command()
def sync():
    logger.debug("triggering s3 sync job from command")
    schedule_s3_sync_job()


@cloudstorage.command()
def initdb():
    utils.initdb()


@cloudstorage.command('fix-cors')
@click.argument('domains', nargs=-1)
def fix_cors(domains):
    """Update CORS rules where possible.
    """
    msg, ok = utils.fix_cors(domains)
    click.secho(msg, fg='green' if ok else 'red')


@cloudstorage.command()
@click.argument('path')
@click.argument('resource', required=False)
def migrate(path, resource):
    """Upload local storage to the remote.
    """
    utils.migrate(path, resource)


def get_commands():
    return [cloudstorage]
