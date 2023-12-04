import re
import itertools


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
