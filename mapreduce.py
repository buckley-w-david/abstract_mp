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


class MapReduceError(Exception):
    pass


class InvalidResourceError(MapReduceError):
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
            raise InvalidResourceError(f"{spec.get("type")} is not a valid resource type") from exc

        name = spec.get('name', 'default')
        extras: typing.Dict = spec.get('extra', {})

        injestor_type = resource_map.get(resource_type, FileResourceInjestor)
        injestor = injestor_type(name, extras)
        content = injestor.injest()

        return Resource(id, resource_type, content)


# I guess these are effectivly just closures
class Invoker(typing.Protocol):

    def __init__(self, target, resource, apply) -> None:
        pass

    def __call__(self, row: typing.List[str]) -> str:
        pass


class BuiltinFunctionInvoker:

    def __init__(self, target, resource, apply) -> None:
        pass

    def __call__(self, row: typing.List[str]) -> str:
        pass


class BuiltinMethodInvoker:

    def __init__(self, target, resource, apply) -> None:
        pass

    def __call__(self, row: typing.List[str]) -> str:
        pass


class ResourceMethodInvoker:

    def __init__(self, target, resource, apply) -> None:
        pass

    def __call__(self, row: typing.List[str]) -> str:
        pass


@enum.unique
class ApplyType(enum.Enum):
    BUILTIN_FUNCTION = enum.auto()
    BUILTIN_METHOD = enum.auto()
    RESOURCE_METHOD = enum.auto()


invoke_map: typing.Dict[ApplyType, typing.Type[Invoker]] = {
    ApplyType.BUILTIN_FUNCTION: BuiltinFunctionInvoker,
    ApplyType.BUILTIN_METHOD: BuiltingMethodInvoker,
}

class ValueMap:

    def __init__(self, invoke: typing.Callable) -> None:
        self.invoke = invoke

    def __call__(row: typing.List[str]) -> str:
        return self.invoke(row)

    @staticmethod
    def from_spec(spec: typing.Dict, resources: typing.Dict[int, Resource]) -> 'Map':
        required_resources = spec.get('uses', [])
        resources = [resources.get(resource) for resource in required_resources]
        resources = resources
        target = spec.get('target', 0)
        apply = spec.get('apply')
        if not apply:
            self.invoke = lambda *args: *args

        try:
            type = ApplyType[apply.get('type', 'builtin-function').replace('-', '_').upper()]
        except KeyError as exc:
            raise ApplyTypeError(f"{apply.get('type')} is not a valid apply type")

        invoker = invoke_map[type]
        invoke = invoker(target, resource, apply)
        return ValueMap(invoke)


class CSVMapper():

    def __init__(self, mappings: typing.List[Map]) -> None:
        self.mappings = mappings

    def apply(self, csv: typing.Iterable[typing.List[str]]) -> typing.Iterator[typing.List[str]]:
        for row in csv:
            yield [mapping.apply(row) for mapping in self.mappings]

    @staticmethod
    def from_mapfile(self, mapfile: typing.Dict) -> 'MapRequest':
        resources = mapfile.get('resources', {})
        resources = [Resource.from_spec(id, resources[id]) for id in resources]
        maps = [Map.from_spec(spec, resources) for spec in mapfile.get('maps', [])]
        return MapRequest(maps)


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
