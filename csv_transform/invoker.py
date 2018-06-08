import typing
import typing_extensions

# I guess these are effectivly just closures
class Invoker(typing_extensions.Protocol):

    def __init__(self, target, resource, apply) -> None:
        pass

    def __call__(self, row: typing.List[str]) -> str:
        pass


class BuiltinFunctionInvoker:

    def __init__(self, target, resource, apply) -> None:
        name = apply.get('name')
        self.func = getattr(globals()['__builtins__'], name)
        self.target = target
        self.args = apply.get('args')

    def __call__(self, row: typing.List[str]) -> str:
        self.func(row[self.target], **self.args)


class BuiltinMethodInvoker:

    def __init__(self, target, resource, apply) -> None:
        source = apply.get('source')
        name = apply.get('name')
        self.func = getattr(getattr(globals()['__builtins__'], source), name)
        self.args = apply.get('args')
        self.target = target

    def __call__(self, row: typing.List[str]) -> typing.Any:
        return self.func(row[self.target], **self.args)


class ResourceMethodInvoker:

    def __init__(self, target, resource, apply) -> None:
        pass

    def __call__(self, row: typing.List[str]) -> str:
        pass


# I guess these are effectivly just closures
class Invoker(typing_extensions.Protocol):

    def __init__(self, target, resource, apply) -> None:
        pass

    def __call__(self, row: typing.List[str]) -> str:
        pass


class BuiltinFunctionInvoker:

    def __init__(self, target, resource, apply) -> None:
        name = apply.get('name')
        self.func = getattr(globals()['__builtins__'], name)
        self.target = target
        self.args = apply.get('args')

    def __call__(self, row: typing.List[str]) -> str:
        self.func(row[self.target], **self.args)


class BuiltinMethodInvoker:

    def __init__(self, target, resource, apply) -> None:
        source = apply.get('source')
        name = apply.get('name')
        self.func = getattr(getattr(globals()['__builtins__'], source), name)
        self.args = apply.get('args')
        self.target = target

    def __call__(self, row: typing.List[str]) -> typing.Any:
        return self.func(row[self.target], **self.args)


class ResourceMethodInvoker:

    def __init__(self, target, resource, apply) -> None:
        pass

    def __call__(self, row: typing.List[str]) -> str:
        pass


