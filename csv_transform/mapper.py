import enum
import typing
import typing_extensions
from csv_transform.exceptions import ApplyTypeError
from csv_transform import resource
from csv_transform import invoker


@enum.unique
class ApplyType(enum.Enum):
    BUILTIN_FUNCTION = enum.auto()
    BUILTIN_METHOD = enum.auto()
    RESOURCE_METHOD = enum.auto()


invoke_map: typing.Dict[ApplyType, typing.Type[invoker.Invoker]] = {
    ApplyType.BUILTIN_FUNCTION: invoker.BuiltinFunctionInvoker,
    ApplyType.BUILTIN_METHOD: invoker.BuiltinMethodInvoker,
}

class ValueMap:

    def __init__(self, invoke: typing.Callable) -> None:
        self.invoke = invoke

    def __call__(self, row: typing.List[str]) -> str:
        return self.invoke(row)

    @staticmethod
    def from_spec(spec: typing.Dict, resources: typing.Dict[int, Resource]) -> 'Map':
        required_resource_ids = spec.get('uses', [])
        required_resources = [resources.get(resource) for resource in required_resource_ids]
        target = spec.get('target', 0)
        apply = spec.get('apply')
        if not apply:
            return ValueMap(lambda *args: args)

        try:
            type = ApplyType[apply.get('type', 'builtin-function').replace('-', '_').upper()]
        except KeyError as exc:
            raise ApplyTypeError(f"{apply.get('type')} is not a valid apply type")

        invoker = invoke_map[type]
        invoke = invoker(target, required_resources, apply)
        return ValueMap(invoke)


class CSVMapper():

    def __init__(self, mappings: typing.List[typing.Callable]) -> None:
        self.mappings = mappings

    def apply(self, csv: typing.Iterable[typing.List[str]]) -> typing.Iterator[typing.List[str]]:
        for row in csv:
            yield [mapping(row) for mapping in self.mappings]

    @staticmethod
    def from_mapfile(mapfile: typing.Dict) -> 'MapRequest':
        resources = mapfile.get('resources', {})
        resources = [resource.Resource.from_spec(id, resources[id]) for id in resources]
        maps = [ValueMap.from_spec(spec, resources) for spec in mapfile.get('maps', [])]
        return CSVMapper(maps)
