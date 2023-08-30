from ast import literal_eval

from ckan.plugins import toolkit


_sentinel = object()

def _get_config(name, default=None):
    value = toolkit.config.get(name, _sentinel)
    return value if value is not _sentinel and value else default


class Config:
    @property
    def bucket_name(self) -> str:
        return toolkit.config["ckanext.cloudstorage.container_name"]

    @property
    def driver_options(self) -> dict:
        return literal_eval(_get_config("ckanext.cloudstorage.driver_options", "{}"))

    @property
    def sync_reschedule_delay(self) -> str:
        return toolkit.asint(_get_config("ckanext.cloudstorage.sync.reschedule_delay", '10'))

    @property
    def queue_region(self) -> str:
        return toolkit.config["ckanext.cloudstorage.sync.queue_region"]

    @property
    def queue_url(self) -> str:
        return toolkit.config["ckanext.cloudstorage.sync.queue_url"]

    @property
    def use_fake_events(self) -> bool:
        return toolkit.asbool(_get_config("ckanext.cloudstorage.sync.use_fake_events", False))


config = Config()
