import os


from ckan.lib import uploader
from ckan.plugins import toolkit as tk


def create_presigned_url(context, data):
    resource_id = tk.get_or_bust(data, 'id')

    resource = tk.get_action('resource_show')(context, {'id': resource_id})
    if resource.get('url_type') != 'upload':
        # This isn't a file upload, so error out.
        raise tk.Invalid('resource is a url')

    filename = os.path.basename(resource['url'])
    upload = uploader.get_resource_uploader(resource)
    url = upload.get_url_from_filename(resource['id'], filename)

    if url is None:
        raise Exception('resource not available')

    if url.startswith('file://'):
        raise tk.Invalid('signed url cannot be generated')

    return {'url': url}
