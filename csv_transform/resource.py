import enum
import importlib
import os
import types
import typing
import typing_extensions
from csv_transform.exceptions import InvalidResourceError


class ResourceInjestor(typing_extensions.Protocol):

    def __init__(self, name: str, options: typing.Dict) -> None:
        pass

    def injest(self) -> typing.Any:
        pass


class FileResourceInjestor:

    def __init__(self, filename: str, extra: typing.Dict) -> None:
        location = extra.pop('location', '.')
        self.filename = os.path.join(location, filename)
        self.extras = extra

    def injest(self) -> str:
        return open(self.filename, 'r', **self.extras)


class ModuleResourceInjestor:

    def __init__(self, modulename: str, extra: typing.Dict) -> None:
        self.module_name = modulename
        self.module = importlib.import_module(modulename)

    def injest(self) -> types.ModuleType:
        return self.module


@enum.unique
class ResourceType(enum.Enum):
    FILE = enum.auto()
    MODULE = enum.auto()


resource_map: typing.Dict[ResourceType, typing.Type[ResourceInjestor]] = {
    ResourceType.FILE: FileResourceInjestor,
    ResourceType.MODULE: ModuleResourceInjestor,
}


class Resource:

    def __init__(self, id: int, type: ResourceType, content: typing.Any) -> None:
        self.id = id
        self.type = type
        self.content = content

    @staticmethod
    def from_spec(id: int, spec: typing.Dict) -> 'Resource':
        try:
            resource_type = ResourceType[spec.get('type', 'file').strip().upper()]
        except KeyError as exc:
            raise InvalidResourceError(f"{spec.get('type')} is not a valid resource type") from exc

        name = spec.get('name', 'default')
        extras: typing.Dict = spec.get('extra', {})

        injestor_type = resource_map.get(resource_type, FileResourceInjestor)
        injestor = injestor_type(name, extras)
        content = injestor.injest()

        return Resource(id, resource_type, content)


