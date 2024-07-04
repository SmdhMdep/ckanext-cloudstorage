from ckan.plugins import toolkit as tk
from ...utils import canonicalize_package_name, convert_local_package_name_to_global


@tk.side_effect_free
def cloudstorage_package_show(context, data):
    org_name, name = tk.get_or_bust(data, ['org_name', 'name'])
    package_name = canonicalize_package_name(name)
    package_name = convert_local_package_name_to_global(org_name, package_name)

    data['id'] = package_name
    return tk.get_action('package_show')(context, data)
