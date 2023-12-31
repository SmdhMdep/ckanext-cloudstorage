from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional, Type
from abc import abstractmethod, ABC

from .utils import convert_local_package_name_to_global, canonicalize_package_name


_FACTORIES = []


class ResourceObjectKeyType(Enum):
    UPLOAD = 'upload'
    STREAMING = 'streaming'


@dataclass
class ResourceObjectKey(ABC):
    raw: str
    """The raw object key name."""

    version: int
    """The version of this key format. Must be defined as a static attribute on the class."""

    organization_name: str
    """The id of the organization for this resource."""

    package_name: str
    """The id of the package for this resource."""

    package_segment: str
    """Actual key segment."""

    name: str
    """The name of the resource. For uploaded data, this matches the filename. For streamed data, 
    this is the folder name (prefix part) right after the package name.
    """

    type: ResourceObjectKeyType
    """The key type. Determines the underlying resource type this key references."""

    filename: str
    """Matches the object name in S3"""

    ingestion_datetime: Optional[datetime]
    """For streamed data, the date this data was ingested. None for uploaded data."""

    @classmethod
    def _factories(cls):
        # can't use `__subclasses__` directly here as it doesn't account for deeper hierarchies.
        return sorted(_FACTORIES, key=lambda f: f.version, reverse=True)

    @classmethod
    def from_raw_key(cls, key: str) -> 'ResourceObjectKey':
        for factory in cls._factories():
            instance = factory.try_parse(key)
            if instance is not None:
                return instance
        raise ValueError(f'Unsupported key format: {key}')

    @classmethod
    def from_resource(cls, package: dict, resource: dict) -> 'ResourceObjectKey':
        key = resource.get('cloud_storage_key')
        if key is not None:
            return cls.from_raw_key(key)
        for factory in cls._factories():
            instance = factory.try_create(package, resource)
            if instance is not None:
                return instance
        raise ValueError(f'Cannot create key for resource: {resource.get("id")}')

    @classmethod
    @abstractmethod
    def try_parse(cls, key: str) -> 'ResourceObjectKey':
        pass

    @classmethod
    @abstractmethod
    def try_create(cls, package: dict, resource: dict) -> 'ResourceObjectKey':
        pass

    def __init_subclass__(cls) -> None:
        _FACTORIES.append(cls)
        return super().__init_subclass__()


class _ResourceObjectKeyV0(ResourceObjectKey):
    """
    Version 0 of the resource object key format.

    Upload resource format:
      <org_name> / <package_name> / <resource_name = resource_filename>
    Streaming resource format:
      <org_name> / <package_name> / <resource_name> / ... / <resource_filename>

    Distinguishing between the two types is solely based on the path length
    """
    version = 0

    @classmethod
    def try_parse(cls, key):
        return _parse_from_path_v0_v1(cls, key, key)

    @classmethod
    def try_create(cls, package, resource):
        return _create_from_resource_v0_v1(cls, None, package, resource)


class _ResourceObjectKeyV1(ResourceObjectKey):
    """
    Version 1 of the resource object key format. This is similar to version 0 but
    encodes the version number in the key, and enforces the file name format for streamed data.

    Upload resource format:
      <version> / <org_name> / <package_name> / <resource_name = resource_filename>
    Streaming resource format:
      <version> / <org_name> / <package_name> / <resource_name> / ... / <resource_filename>

    Distinguishing between the two types is solely based on the path length
    """
    version = 1

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.type == ResourceObjectKeyType.STREAMING and not self.ingestion_datetime:
            raise ValueError("expected ingestion time to be included in the filename for streamed data")

    @classmethod
    def try_parse(cls, key):
        version, _, path = key.partition('/')
        if int(version) == cls.version:
            return _parse_from_path_v0_v1(cls, path, key)
        return None

    @classmethod
    def try_create(cls, package: dict, resource: dict):
        return _create_from_resource_v0_v1(cls, str(cls.version), package, resource)


def _parse_from_path_v0_v1(cls: Type[ResourceObjectKey], path: str, key: str) -> ResourceObjectKey:
    try:
        organization_name, package_segment, *resource_path = path.split("/")
        if not resource_path:
            raise ValueError("key does not match the expected schema")
    except ValueError:
        raise ValueError("key does not match the expected schema")

    key_type = ResourceObjectKeyType.STREAMING if len(resource_path) > 1 else ResourceObjectKeyType.UPLOAD

    local_package_name = canonicalize_package_name(package_segment)
    package_name = convert_local_package_name_to_global(organization_name, local_package_name)
    name = resource_path[0]
    filename = (
        name if key_type == ResourceObjectKeyType.UPLOAD
        else resource_path[-1]
    )
    ingestion_datetime = (
        _try_parse_streaming_datetime(filename)
        if key_type == ResourceObjectKeyType.STREAMING
        else None
    )

    return cls(
        raw=key,
        version=cls.version,
        organization_name=organization_name,
        package_name=package_name,
        package_segment=package_segment,
        name=name,
        type=key_type,
        filename=filename,
        ingestion_datetime=ingestion_datetime,
    )

def _create_from_resource_v0_v1(cls: Type[ResourceObjectKey], prefix: Optional[str], package: dict, resource: dict):
    organization_name = package['organization']['name']
    package_segment = package['cloud_storage_key_segment']
    name = resource['name']
    path = f'{organization_name}/{package_segment}/{name}'

    return cls(
        raw=f'{prefix}/{path}' if prefix else path,
        version=cls.version,
        organization_name=organization_name,
        package_name=package['name'],
        package_segment=package_segment,
        name=name,
        type=ResourceObjectKeyType.UPLOAD,
        filename=name,
        ingestion_datetime=None,
    )

def _try_parse_streaming_datetime(name) -> Optional[datetime]:
    """Returns the datetime encoded within the object name for streamed data."""
    # streamed objects name follows the pattern:
    #   DeliveryStreamName-DeliveryStreamVersion-YYYY-MM-dd-HH-MM-SS-RandomString
    # Since the stream name may contain dashes (-) itself, we're relying on the random
    # string always having 5 parts and parsing from the end.
    # example names:
    #   - S301-1-2023-09-07-10-00-39-85dc0d38-176c-369f-b4f2-d8ecb6d95dfe
    #   - PUT-S3-Qj0zi-3-2023-06-26-18-42-52-3d8d51f5-0fc4-3a21-8d2e-ff614b8e9a30
    try:
        ingestion_datetime = name.rsplit('-', 5)[0].rsplit('-', 6)[1:]
        return datetime(*map(int, ingestion_datetime)) # type: ignore
    except:
        return None
