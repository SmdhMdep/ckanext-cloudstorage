import re
import itertools

from ckan.plugins import toolkit as tk

from .utils import convert_global_package_name_to_local


ALLOWED_SPECIAL_CHARS = r"!-_.*'()&,$:@?+ "
# Matches the S3 object name restrictions
_name_regex = re.compile(rf"^(?:[0-9]|[A-Z]|[a-z]|[{re.escape(ALLOWED_SPECIAL_CHARS)}])+$")


def iter_other_resource_name_keys(skip_key, data):
    keys_generator = (('resources', index, 'name') for index in itertools.count())
    data_keys = itertools.takewhile(lambda k: k in data, keys_generator)
    return (key for key in data_keys if key != skip_key)


def valid_resource_name(key, data, errors, context):
    """Validate resource name to be unique and a valid filename."""
    current_name = data[key]
    if _name_regex.fullmatch(current_name) is None:
        symbols = ', '.join(map(lambda s: f'"{s}"', ALLOWED_SPECIAL_CHARS))
        errors[key].append(
            'Invalid resource name. Name can only contain '
            f'alphanumeric characters and the characters {symbols}.'
        )

    for name_key in iter_other_resource_name_keys(key, data):
        if current_name == data[name_key]:
            errors[key].append('That name is already in use.')
            break


def default_cloud_storage_key_package_segment(key, data, errors, context):
    '''When key is missing or value is an empty string or None, replace it with
    a default value'''
    value = data.get(key)
    if value is None or value == '' or value is tk.missing:
        data[key] = convert_global_package_name_to_local(data[('name',)])


def no_update_to_package_cloud_storage_key_segment(value, context):
    model = context.get('package')
    key_segment = model and (
        model.extras.get('cloud_storage_key_segment')
        or convert_global_package_name_to_local(model.name)
    )

    if key_segment and key_segment != value:
        raise tk.Invalid(
            f'Cannot change the cloud storage key segment for dataset'
        )
    return value
