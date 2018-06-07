#!/usr/bin/env python
import csv
import enum
import Levenshtein
import logging
import itertools
import os
import re
import types
import typing
import typing_extensions
import click
import importlib


class Map:

    @staticmethod
    def from_spec(spec: typing.Dict) -> 'Map':
        pass


class ResourceInjestor(typing.Protocol):

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
        with open(self.filename, 'r', **self.extras) as file:
            contents = file.read()
        return contents

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
        resource_type = ResourceType[spec.get('type', 'file').strip().upper()]
        name = spec.get('name', 'default')
        extras: typing.Dict = spec.get('extra', {})

        injestor_type = resource_map.get(resource_type, FileResourceInjestor)
        injestor = injestor_type(name, extras)
        content = injestor.injest()

        return Resource(id, resource_type, content)


class MapRequest():

    def __init__(self, mappings: typing.List[Map], resources: typing.List[Resource]) -> None:
        self.mappings = mappings
        self.resources = resources

    def apply(self, csv: typing.Iterable[typing.Dict]) -> typing.Iterator[typing.List[str]]:
        pass

    @staticmethod
    def from_mapfile(self, mapfile: typing.Dict) -> 'MapRequest':
        maps = [Map.from_spec(spec) for spec in mapfile.get('maps', [])]
        resources = mapfile.get('resources', {})
        resources = [Resource.from_spec(id, resources[id]) for id in resources]
        return MapRequest(maps, resources)


@click.group()
def main() -> None:
    pass

@main.command()
@click.argument('mapfile', type=click.File('r', encoding='utf-8-sig'), nargs=-1)
@click.argument('reducefile', type=click.File('r', encoding='utf-8-sig'))
def mapreduce(mapfile: typing.List[typing.IO[str]], reducefile: typing.IO[str]) -> None:
    pass


if __name__ == '__main__':
    main()
